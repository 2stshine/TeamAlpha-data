"""ECS daily: KRX·DART·FMP Bronze 증분을 수집해 Silver에 인증 반영."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3

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
from pipeline.silver_quality import freshness
from pipeline.silver_quality import migrate


KST = ZoneInfo("Asia/Seoul")
EXPECTED_STOCK_FILES = {"kospi.parquet", "kosdaq.parquet"}
EXPECTED_INDEX_FILES = {"krx.parquet", "kospi.parquet", "kosdaq.parquet"}
CORPORATE_ACTION_DISCOVERY_LOOKBACK_DAYS = 14
CORPORATE_ACTION_EVIDENCE_LOOKBACK_DAYS = 180
ACTION_DISCLOSURE_MANIFEST_NAME = "disclosures_v3.json"
LEGACY_ACTION_DISCLOSURE_MANIFEST_NAME = "disclosures.json"


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


def _action_disclosure_manifest_key(action_from: str, day: str) -> str:
    return (
        "corporate_actions/dart/manifests/"
        f"from={action_from}/to={day}/{ACTION_DISCLOSURE_MANIFEST_NAME}"
    )


def _reject_legacy_action_manifests(keys: list[str]) -> None:
    legacy = [
        key for key in keys
        if key.endswith(f"/{LEGACY_ACTION_DISCLOSURE_MANIFEST_NAME}")
        and "/corporate_actions/dart/manifests/" in f"/{key}"
    ]
    if legacy:
        raise RuntimeError(
            "legacy corporate-action disclosure manifests cannot authenticate "
            f"the v3 collector universe: {sorted(legacy)}"
        )


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
    bucket = os.environ["S3_BRONZE_BUCKET"]
    day = _target_day()
    ds = ymd_to_dash(day)
    root = Path("/app/data")

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
    genuine_action_changes: list[str] = []
    changed_action_uris = corporate_actions.run(
        action_from,
        day,
        "s3",
        include_dependencies=True,
        dependency_fromdate=action_evidence_from,
        changed_sink=genuine_action_changes,
    )
    has_action_change = bool(genuine_action_changes)
    changed_action_keys = [
        _key_from_s3_uri(uri) for uri in changed_action_uris
    ]
    _reject_legacy_action_manifests(changed_action_keys)
    action_disclosure_manifest = _action_disclosure_manifest_key(
        action_from,
        day,
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

    print(
        f"[silver] incremental start day={day}, "
        f"financial_files={len(financial_files)}, "
        f"dividend_files={len(dividend_files)}",
        flush=True,
    )
    load.incremental(
        day,
        "local",
        financial_files=financial_files,
        dividend_files=dividend_files,
        market_closed=market_closed,
        has_action_change=has_action_change,
    )
    print(f"[silver] incremental complete day={day}", flush=True)

    # KRX 가격 또는 배당 입력을 게시한 트랜잭션은 total-return 계약을
    # BUILDING으로 내린다. 일일 증분만으로는 최신 정정 DART 패밀리, 공식
    # 권리변동 증거와 전체 가격 스케일을 인증할 수 없으므로 여기서 부분
    # 재계산하거나 다시 CERTIFIED로 올리지 않는다. 연구 재개 전에는 반드시
    # action-only snapshot -> full rebuild -> independent audit 폐쇄 흐름을
    # 실행해야 한다.
    print(
        "[total-return] source publication complete; certified full rebuild "
        "and audit are required before research",
        flush=True,
    )

    # FMP is a separate source transaction. KRX/DART remains committed if FMP
    # later fails, and a task retry safely reuses the immutable raw objects.
    fmp_day = _fmp_target_day(day)
    print(f"[fmp] daily start day={fmp_day}", flush=True)
    fmp_uris = fmp_bronze.run_daily(fmp_day, "s3")
    fmp_keys = [_key_from_s3_uri(uri) for uri in fmp_uris]
    _download_keys(bucket, fmp_keys, root)
    fmp_load.run(src="local", day=fmp_day)
    print(f"[fmp] daily complete day={fmp_day}", flush=True)

    # 신선도 자기점검(비치명적). 파이프라인이 아예 멈춘 경우까지 잡으려면 별도
    # 스케줄로 `python -m pipeline.silver_quality.freshness` 를 돌린다.
    try:
        fr = freshness.assert_fresh()
        print(f"[freshness] ok {fr['sources']}", flush=True)
    except RuntimeError as exc:
        print(f"[freshness] WARNING {exc}", flush=True)


if __name__ == "__main__":
    main()
