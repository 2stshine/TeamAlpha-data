"""ECS에서 DART 배당·기업행사 및 누락 KRX 날짜를 Silver에 반영한다."""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3

from pipeline.common import db
from pipeline.silver import dart_extra_load, load


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


def run_dart_extras() -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    s3 = boto3.client("s3")
    prefixes = (
        "dividends/dart/",
        "corporate_actions/dart/manifests/",
        "corporate_actions/dart/structured/",
        "corporate_actions/dart/documents/",
    )
    keys = [key for prefix in prefixes for key in _list_keys(s3, bucket, prefix)]
    count = _download(bucket, keys, Path("/app/data"))
    if count == 0:
        raise RuntimeError("no DART dividend/corporate-action Bronze objects")
    dart_extra_load.run(src="local")


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
    root = Path("/app/data")
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
