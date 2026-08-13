"""ECS daily: KRX·DART·FMP Bronze 증분을 수집해 Silver에 인증 반영."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3

from pipeline import dart_silver_backfill_ecs
from pipeline.bronze import (
    corporate_actions,
    dividends,
    financials,
    fmp as fmp_bronze,
    index,
    stock_krxapi,
)
from pipeline.common.paths import base_uri, ymd_to_dash
from pipeline.silver import load
from pipeline.silver import fmp_load
from pipeline.silver.dart_action_snapshot import DEFAULT_COVERAGE_START
from pipeline.silver_quality import freshness
from pipeline.silver_quality import migrate


KST = ZoneInfo("Asia/Seoul")
EXPECTED_STOCK_FILES = {"kospi.parquet", "kosdaq.parquet"}
EXPECTED_INDEX_FILES = {"krx.parquet", "kospi.parquet", "kosdaq.parquet"}
CORPORATE_ACTION_DISCOVERY_LOOKBACK_DAYS = 14
CORPORATE_ACTION_EVIDENCE_LOOKBACK_DAYS = 180


def _target_day() -> str:
    """Return explicit PIPELINE_DATE or the previous KST calendar day."""
    override = os.environ.get("PIPELINE_DATE")
    if override:
        return override
    return (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y%m%d")


def _fmp_target_day(krx_day: str) -> str:
    """Use a completed US session at the existing 08:30 KST schedule.

    FMP documents bulk refreshes as taking several hours.  At 08:30 KST the
    same-calendar US session has only just closed, so use the prior weekday.
    """
    candidate = datetime.strptime(krx_day, "%Y%m%d").date() - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def _key_from_s3_uri(uri: str) -> str:
    return uri.removeprefix(base_uri("s3") + "/")


def _list_prefix(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3")
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return keys


def _download_keys(bucket: str, keys: list[str], root: Path) -> list[str]:
    s3 = boto3.client("s3")
    keys = sorted(set(keys))
    if not keys:
        return []

    print(f"[sync] downloading {len(keys)} changed/needed objects", flush=True)
    start = time.time()
    done = 0

    def download(key: str) -> str:
        dest = root / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dest))
        return str(dest)

    paths: list[str] = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(download, key) for key in keys]
        for fut in as_completed(futures):
            paths.append(fut.result())
            done += 1
            if done % 100 == 0 or done == len(keys):
                print(f"[sync] downloaded {done}/{len(keys)} elapsed={time.time() - start:.1f}s", flush=True)
    return paths


def _assert_complete_daily_market(stock_keys: list[str], index_keys: list[str], ds: str) -> None:
    """Prevent partial trading-day loads from deleting/replacing silver rows."""
    stock_files = {Path(key).name for key in stock_keys}
    if not index_keys and not stock_files:
        return
    missing = EXPECTED_STOCK_FILES - stock_files
    index_files = {Path(key).name for key in index_keys}
    missing_index = EXPECTED_INDEX_FILES - index_files
    if missing or missing_index:
        raise RuntimeError(
            f"incomplete market bronze for {ds}: "
            f"missing_stock={sorted(missing)}, missing_index={sorted(missing_index)} "
            f"(stock={sorted(stock_files)}, index={sorted(index_files)})"
        )


def main() -> None:
    # No API/S3/RDS mutation may happen before this common session lock.  It
    # serializes the complete certification epoch with one-off dart-extras,
    # preventing an older local snapshot from certifying after another task
    # has observed a newer Bronze action generation.
    certification_lock = (
        dart_silver_backfill_ecs.acquire_daily_certification_lock()
    )
    try:
        _main_locked(certification_lock)
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(
            certification_lock,
        )


def _main_locked(certification_lock) -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    day = _target_day()
    ds = ymd_to_dash(day)
    coverage_end = datetime.strptime(day, "%Y%m%d").date()
    root = Path("/app/data")

    def assert_epoch() -> None:
        dart_silver_backfill_ecs.assert_daily_certification_lock(
            certification_lock,
        )

    print(f"[daily] start day={day}", flush=True)
    migrate.assert_current()
    stock_krxapi.run(day, day, "s3")
    index.run(day, day, "s3")
    changed_financial_uris = financials.run(int(day[:4]), int(day[:4]), "s3", refresh_existing=True)
    if os.environ.get("DART_DIVIDENDS_ENABLED", "true").lower() in {
        "1", "true", "yes", "on",
    }:
        changed_dividend_uris = dividends.run_for_financial_paths(
            changed_financial_uris,
            "s3",
        )
    else:
        changed_dividend_uris = []
        print("[daily] DART regular-report dividends disabled", flush=True)
    changed_financial_keys = [_key_from_s3_uri(uri) for uri in changed_financial_uris]
    changed_dividend_keys = [
        _key_from_s3_uri(uri) for uri in changed_dividend_uris
    ]
    print(f"[daily] changed financial files={len(changed_financial_keys)}", flush=True)
    print(
        f"[daily] changed dividend files={len(changed_dividend_uris)}",
        flush=True,
    )
    action_from = (
        datetime.strptime(day, "%Y%m%d").date()
        - timedelta(days=CORPORATE_ACTION_DISCOVERY_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    action_evidence_from = (
        datetime.strptime(day, "%Y%m%d").date()
        - timedelta(days=CORPORATE_ACTION_EVIDENCE_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    # Record whether this task inherited an already-unusable return contract.
    # A new Bronze action is allowed to demote a previously healthy contract,
    # but it must not hide an older BUILDING failure by extending raw coverage.
    contract_ready_before_action_collection = (
        dart_silver_backfill_ecs.total_return_contract_ready(
            conn=certification_lock,
        )
    )
    action_contract_invalidated = False

    def invalidate_before_first_action_write(_path: str) -> None:
        nonlocal action_contract_invalidated
        # The collector invokes this boundary for every new object.  Keep
        # proving lock ownership even after the one-time DB invalidation so a
        # long S3 publication cannot continue after its PostgreSQL session
        # (and therefore its cross-task exclusion) was lost.
        assert_epoch()
        if action_contract_invalidated:
            return
        # This callback completes before _BronzeWriter schedules the first S3
        # PUT.  A new action observation can therefore never coexist with a
        # stale CERTIFIED return label, even when later preflight fails.
        dart_silver_backfill_ecs.invalidate_total_return_for_observed_action(
            coverage_end,
            conn=certification_lock,
        )
        action_contract_invalidated = True

    genuine_action_changes: list[str] = []
    changed_action_uris = corporate_actions.run(
        action_from,
        day,
        "s3",
        include_dependencies=True,
        dependency_fromdate=action_evidence_from,
        changed_sink=genuine_action_changes,
        before_change=invalidate_before_first_action_write,
    )
    has_action_change = bool(genuine_action_changes)
    changed_action_keys = [
        _key_from_s3_uri(uri) for uri in changed_action_uris
    ]
    action_disclosure_manifest = (
        "corporate_actions/dart/manifests/"
        f"from={action_from}/to={day}/disclosures.json"
    )
    if action_disclosure_manifest not in changed_action_keys:
        changed_action_keys.append(action_disclosure_manifest)
    print(
        f"[daily] changed corporate-action files={len(changed_action_keys)}, "
        f"genuine changes={len(genuine_action_changes)}",
        flush=True,
    )

    stock_keys = _list_prefix(bucket, f"stock/krxapi/date={ds}/")
    index_keys = _list_prefix(bucket, f"index/krxapi/date={ds}/")
    _assert_complete_daily_market(stock_keys, index_keys, ds)
    market_closed = not stock_keys and not index_keys
    source_will_invalidate_return = bool(stock_keys) or has_action_change
    # Every non-skipped Silver load currently evaluates/publishes the bounded
    # action candidate frame. A financial or regular-dividend refresh can
    # therefore cause a genuine issuer-action upsert even when the market is
    # closed and the Bronze action collector wrote no new object. Prepare the
    # complete action evidence before that transaction; the post-commit DB
    # state below remains the authority on whether recertification is needed.
    source_may_publish_actions = bool(
        changed_financial_keys or changed_dividend_keys
    )
    requires_total_return_closure = (
        source_will_invalidate_return
        or source_may_publish_actions
        or not contract_ready_before_action_collection
    )

    keys = ["financials/dart/corpCode.xml"]
    keys += stock_keys
    keys += index_keys
    keys += changed_financial_keys
    keys += changed_dividend_keys
    keys += changed_action_keys

    local_paths = _download_keys(bucket, keys, root)
    financial_files = [
        p for p in local_paths
        if (
            "/financials/dart/year=" in p
            or "/financials/dart_full/year=" in p
        )
    ]
    dividend_files = [
        p for p in local_paths if "/dividends/dart/alot-matter/" in p
    ]

    if requires_total_return_closure:
        # Complete all network/filesystem evidence first.  A missing viewer,
        # correction family, frozen cash-scale body or v5 coverage interval
        # therefore stops the task before its KRX Silver transaction.
        dart_silver_backfill_ecs.prepare_total_return_snapshot(
            coverage_end,
            bucket=bucket,
            root=root,
            certification_lock=certification_lock,
        )
        dart_silver_backfill_ecs.preview_total_return_actions(
            coverage_end,
            root=root,
            conn=certification_lock,
        )
        assert_epoch()
        # A prior task may have failed after its atomic raw-price publish and
        # intentionally left the contract BUILDING.  Repair that exact existing
        # coverage before admitting another price day; otherwise repeated
        # scheduled failures would keep extending uncertified raw coverage.
        if not contract_ready_before_action_collection:
            print(
                "[total-return] closing pre-existing uncertified coverage "
                "before daily price publication",
                flush=True,
            )
            dart_silver_backfill_ecs.close_total_return_contract(
                coverage_end,
                root=root,
                certification_lock=certification_lock,
            )

    print(
        f"[silver] incremental start day={day}, "
        f"financial_files={len(financial_files)}, "
        f"dividend_files={len(dividend_files)}",
        flush=True,
    )
    assert_epoch()
    load.incremental(
        day,
        "local",
        financial_files=financial_files,
        dividend_files=dividend_files,
        market_closed=market_closed,
        has_action_change=has_action_change,
        action_coverage_start=DEFAULT_COVERAGE_START,
        action_coverage_end=coverage_end,
        conn=certification_lock,
    )
    print(f"[silver] incremental complete day={day}", flush=True)

    # The price/action prediction above decides whether expensive evidence can
    # be prepared before the Silver transaction.  It is not the authority on
    # what that transaction actually changed: even on a market holiday, a
    # financial/dividend refresh can make the action publisher upsert an
    # issuer event and atomically demote the return contract.  Re-read the
    # contract through the same K2 certification session after the commit.
    assert_epoch()
    contract_ready_after_incremental = (
        dart_silver_backfill_ecs.total_return_contract_ready(
            conn=certification_lock,
        )
    )
    if not contract_ready_after_incremental:
        # The source transaction has deliberately demoted the label contract to
        # BUILDING.  The same ECS invocation must now validate the new raw-price
        # day against local actions, publish the exact action snapshot, rebuild
        # the entire v3 label and pass an independent audit.  Any exception is
        # fatal and leaves the contract visibly BUILDING.
        assert_epoch()
        dart_silver_backfill_ecs.close_total_return_contract(
            coverage_end,
            root=root,
            certification_lock=certification_lock,
        )

    # FMP is a separate source transaction. KRX/DART remains committed if FMP
    # later fails, and a task retry safely reuses the immutable raw objects.
    fmp_day = _fmp_target_day(day)
    assert_epoch()
    print(f"[fmp] daily start day={fmp_day}", flush=True)
    fmp_uris = fmp_bronze.run_daily(fmp_day, "s3")
    fmp_keys = [_key_from_s3_uri(uri) for uri in fmp_uris]
    _download_keys(bucket, fmp_keys, root)
    fmp_load.run(src="local", day=fmp_day)
    print(f"[fmp] daily complete day={fmp_day}", flush=True)

    # A BUILDING/drifted total-return contract or stale source is a task
    # failure.  Do not emit a false-green ECS exit after source publication.
    fr = freshness.assert_fresh()
    print(f"[freshness] ok {fr['sources']}", flush=True)


if __name__ == "__main__":
    main()
