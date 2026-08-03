"""ECS helpers for bounded FMP Bronze/Silver historical backfills."""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import boto3

from pipeline.bronze import fmp as bronze_fmp
from pipeline.silver import fmp_load


def _silver_prefixes(year: int) -> tuple[str, ...]:
    return (
        "stock/fmp/universe/",
        f"stock/fmp/eod-bulk/date={year}-",
        f"financials/fmp/income/year={year}/",
        f"financials/fmp/balance/year={year}/",
        f"financials/fmp/cashflow/year={year}/",
        "corporate_actions/fmp/splits/year=",
        f"corporate_actions/fmp/dividends/year={year}/",
        "fx/fmp/pair=USDKRW/from=",
    )


def _download_prefixes(bucket: str, root: Path, prefixes: tuple[str, ...]) -> int:
    s3 = boto3.client("s3")
    downloaded = 0
    for prefix in prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        prefix_count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                destination = root / key
                destination.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(destination))
                downloaded += 1
                prefix_count += 1
                if downloaded % 100 == 0:
                    print(
                        f"[fmp-backfill-ecs] downloaded={downloaded}",
                        flush=True,
                    )
        print(
            f"[fmp-backfill-ecs] prefix={prefix} objects={prefix_count}",
            flush=True,
        )
    return downloaded


def run_silver_year(year: int, *, resume: str | None = None) -> None:
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise SystemExit("S3_BRONZE_BUCKET is required")
    root = Path("/app/data")
    count = _download_prefixes(bucket, root, _silver_prefixes(year))
    if count == 0:
        raise RuntimeError(f"no FMP Bronze objects downloaded for year={year}")
    print(
        f"[fmp-backfill-ecs] silver start year={year} objects={count}",
        flush=True,
    )
    fmp_load.run(
        src="local",
        fromyear=year,
        toyear=year,
        resume=resume,
    )
    print(f"[fmp-backfill-ecs] silver complete year={year}", flush=True)


def run_full(fromyear: int, toyear: int) -> None:
    bronze_fmp.run_backfill(fromyear, toyear, dest="s3")
    root = Path("/app/data")
    for year in range(fromyear, toyear + 1):
        # Fargate ephemeral storage is bounded. Each certified year is already
        # durable in RDS, so discard only this task's downloaded cache before
        # materializing the next year.
        if root.exists():
            shutil.rmtree(root)
        run_silver_year(year)
    print(
        f"[fmp-backfill-ecs] full complete years={fromyear}-{toyear}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase", choices=("bronze", "silver-year", "full"), required=True,
    )
    parser.add_argument("--from", dest="fromyear", type=int)
    parser.add_argument("--to", dest="toyear", type=int)
    parser.add_argument("--year", type=int)
    parser.add_argument("--resume")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.phase in {"bronze", "full"}:
        if args.fromyear is None or args.toyear is None:
            raise SystemExit(f"{args.phase} phase requires --from and --to")
        if args.phase == "full":
            run_full(args.fromyear, args.toyear)
        else:
            bronze_fmp.run_backfill(args.fromyear, args.toyear, dest="s3")
        return
    if args.year is None:
        raise SystemExit("silver-year phase requires --year")
    run_silver_year(args.year, resume=args.resume)


if __name__ == "__main__":
    main()
