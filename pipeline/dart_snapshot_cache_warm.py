"""Warm the persistent DART evidence cache without touching Silver state."""
from __future__ import annotations

import os
from pathlib import Path

import boto3

from pipeline import dart_silver_backfill_ecs


DATA_ROOT = Path("/app/data")
PREFIXES = (
    "dividends/dart/",
    "corporate_actions/dart/",
    "corporate_actions/krx/",
)


def run(*, bucket: str | None = None, root: Path | None = None) -> tuple[int, int]:
    bucket = bucket or os.environ["S3_BRONZE_BUCKET"]
    root = (root or DATA_ROOT).resolve()
    s3 = boto3.client("s3")
    objects = [
        item for prefix in PREFIXES
        for item in dart_silver_backfill_ecs._list_objects(s3, bucket, prefix)
    ]
    if not objects:
        raise RuntimeError("no DART evidence objects found for cache warm")
    result = dart_silver_backfill_ecs._download_changed(bucket, objects, root)
    print(
        "[dart-cache] warm complete "
        f"downloaded={result[0]} reused={result[1]} total={len(objects)}",
        flush=True,
    )
    return result


def main() -> None:
    run()


if __name__ == "__main__":
    main()
