"""ECS daily: bronze 증분 수집 후 변경분만 내려받아 silver incremental 반영."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3

from pipeline.bronze import corporate_actions, financials, index, stock_krxapi
from pipeline.common.paths import base_uri, ymd_to_dash
from pipeline.silver import load


KST = ZoneInfo("Asia/Seoul")
EXPECTED_STOCK_FILES = {"kospi.parquet", "kosdaq.parquet"}
EXPECTED_INDEX_FILES = {"krx.parquet", "kospi.parquet", "kosdaq.parquet"}


def _target_day() -> str:
    """Return explicit PIPELINE_DATE or the previous KST calendar day."""
    override = os.environ.get("PIPELINE_DATE")
    if override:
        return override
    return (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y%m%d")


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
    bucket = os.environ["S3_BRONZE_BUCKET"]
    day = _target_day()
    ds = ymd_to_dash(day)
    root = Path("/app/data")

    print(f"[daily] start day={day}", flush=True)
    stock_krxapi.run(day, day, "s3")
    index.run(day, day, "s3")
    changed_financial_uris = financials.run(int(day[:4]), int(day[:4]), "s3", refresh_existing=True)
    changed_financial_keys = [_key_from_s3_uri(uri) for uri in changed_financial_uris]
    print(f"[daily] changed financial files={len(changed_financial_keys)}", flush=True)
    action_from = (
        datetime.strptime(day, "%Y%m%d").date() - timedelta(days=14)
    ).strftime("%Y%m%d")
    changed_action_uris = corporate_actions.run(action_from, day, "s3")
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
        f"[daily] changed corporate-action files={len(changed_action_keys)}",
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
    keys += changed_action_keys

    local_paths = _download_keys(bucket, keys, root)
    financial_files = [p for p in local_paths if "/financials/dart/year=" in p]

    print(f"[silver] incremental start day={day}, financial_files={len(financial_files)}", flush=True)
    load.incremental(
        day,
        "local",
        financial_files=financial_files,
        market_closed=market_closed,
    )
    print(f"[silver] incremental complete day={day}", flush=True)


if __name__ == "__main__":
    main()
