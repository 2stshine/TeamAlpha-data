"""ECS에서 DART 배당·기업행사 및 누락 KRX 날짜를 Silver에 반영한다."""
from __future__ import annotations

import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import boto3

from pipeline.common import db
from pipeline.silver import (
    cash_adjustment_scale_evidence,
    dart_extra_load,
    load,
    total_return_audit,
    total_return_rebuild,
)


DATA_ROOT = Path("/app/data")
_PRICE_EVIDENCE_KEY = re.compile(
    r"^stock/(?:marcap|krxapi)/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/"
    r"[^/]+\.parquet$"
)


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        keys.extend(
            item["Key"] for item in page.get("Contents", [])
            if not item["Key"].endswith("/")
        )
    return keys


def _download(bucket: str, keys: list[str], root: Path) -> int:
    client = boto3.client("s3")
    unique = sorted(set(keys))
    def one(key: str) -> None:
        destination = root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(destination))
    done = 0
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(one, key) for key in unique]
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 500 == 0 or done == len(unique):
                print(f"[dart-silver-ecs] downloaded={done}/{len(unique)}", flush=True)
    return done


def _cash_scale_price_keys(root: Path) -> list[str]:
    """Read only the exact KRX price objects named by the frozen manifest."""
    path = root / cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("cash-scale evidence manifest is invalid JSON") from exc
    if (
        payload.get("schema_version")
        != cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        or payload.get("complete") is not True
        or not isinstance(payload.get("evidence"), list)
    ):
        raise RuntimeError("cash-scale evidence manifest is not complete/frozen")
    keys: set[str] = set()
    for index, row in enumerate(payload["evidence"]):
        if not isinstance(row, dict):
            raise RuntimeError(
                f"cash-scale evidence parent {index} is not an object"
            )
        for field in (
            "previous_price_source_object_key",
            "adjustment_price_source_object_key",
        ):
            key = str(row.get(field) or "").strip()
            candidate = Path(key)
            if (
                not key
                or candidate.is_absolute()
                or ".." in candidate.parts
                or _PRICE_EVIDENCE_KEY.fullmatch(key) is None
            ):
                raise RuntimeError(
                    "cash-scale manifest has an invalid KRX price object key: "
                    f"parent={index} field={field} key={key!r}"
                )
            keys.add(key)
    return sorted(keys)


def run_dart_extras() -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    s3 = boto3.client("s3")
    prefixes = (
        "dividends/dart/",
        "corporate_actions/dart/",
        # The certified v5 action snapshot binds cash-scale evidence and its
        # content-addressed KIND/KRX bodies below this prefix.  Downloading
        # only DART objects would make local manifest verification incomplete.
        "corporate_actions/krx/",
    )
    keys = [key for prefix in prefixes for key in _list_keys(s3, bucket, prefix)]
    count = _download(bucket, keys, DATA_ROOT)
    price_keys = _cash_scale_price_keys(DATA_ROOT)
    if price_keys:
        count += _download(bucket, price_keys, DATA_ROOT)
    if count == 0:
        raise RuntimeError("no DART dividend/corporate-action Bronze objects")
    expected_end = os.environ.get("DART_SNAPSHOT_EXPECTED_END")
    if not expected_end:
        raise RuntimeError(
            "DART_SNAPSHOT_EXPECTED_END=YYYY-MM-DD is required for apply"
        )
    coverage_end = date.fromisoformat(expected_end)
    # Publish the exact TR action/source-receipt snapshot, then close the
    # derived-label workflow in the same ECS invocation.  Any failure after
    # source publication leaves the contract BUILDING (fail closed).
    dart_extra_load.run(
        src="local",
        apply=True,
        total_return_actions_only=True,
        expected_coverage_end=coverage_end,
    )
    total_return_rebuild.run(apply=True)
    report = total_return_audit.audit()
    if not report.get("safe_for_research"):
        failed = sorted(
            key for key, passed in (report.get("checks") or {}).items()
            if not passed
        )
        raise RuntimeError(
            "DART ECS total-return audit failed after rebuild: "
            f"{failed}"
        )
    print(
        "[dart-silver-ecs] DART TR actions/rebuild certified",
        flush=True,
    )


def _silver_krx_dates() -> set[str]:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM price_daily WHERE source='KRX'"
            )
            return {row[0].isoformat() for row in cur.fetchall()}
    finally:
        conn.close()


def run_krx_gap() -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    s3 = boto3.client("s3")
    stock_keys = _list_keys(s3, bucket, "stock/krxapi/date=")
    dates = sorted({
        key.split("date=", 1)[1].split("/", 1)[0]
        for key in stock_keys
        if "/" in key.split("date=", 1)[1]
    } - _silver_krx_dates())
    root = DATA_ROOT
    for day in dates:
        keys = [key for key in stock_keys if f"date={day}/" in key]
        keys += _list_keys(s3, bucket, f"index/krxapi/date={day}/")
        keys += ["financials/dart/corpCode.xml"]
        _download(bucket, keys, root)
        load.incremental(
            datetime.strptime(day, "%Y-%m-%d").strftime("%Y%m%d"),
            "local",
            financial_files=[],
            dividend_files=[],
            market_closed=False,
        )
    print(f"[dart-silver-ecs] KRX gap dates completed={len(dates)}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("dart-extras", "krx-gap"), required=True)
    args = parser.parse_args()
    if args.phase == "dart-extras":
        run_dart_extras()
    else:
        run_krx_gap()


if __name__ == "__main__":
    main()
