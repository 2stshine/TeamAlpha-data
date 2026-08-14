"""Fail-closed facade for the retired combined backfill/daily entrypoint.

Both legacy modes can publish KRX price/action inputs without completing the
v3 total-return rebuild and independent audit.  Production daily work belongs
to :mod:`pipeline.daily_full`; destructive history rebuild remains disabled
until every source-scoped dataset has an authenticated reload contract.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

from pipeline.bronze import (
    corporate_actions,
    dividends,
    financials,
    fmp as fmp_bronze,
    index,
    stock_krxapi,
    stock_marcap,
)
from pipeline.silver import load
from pipeline.silver import fmp_load

# silver 는 현재 로컬 bronze(./data)를 읽는다 → dest='local' 일 때 end-to-end.
# dest='s3' 는 bronze 만 S3 적재(silver S3 직접읽기는 후속).


def run_backfill(fromyear: int, toyear: int, dest: str) -> None:
    """초기 1회: bronze 전 구간 적재 → silver 전체 반영."""
    raise RuntimeError(
        "legacy jobs backfill is disabled: it publishes KRX inputs without "
        "the closed total-return certification workflow"
    )


def run_daily(day: str, dest: str) -> None:
    """수동 증분: 지정 날짜 bronze → silver 증분 반영. 재개(exists)로 중복 방지."""
    raise RuntimeError(
        "legacy jobs daily is disabled; use pipeline.daily_full so price and "
        "DART action writes close with full rebuild and independent audit"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["backfill", "daily"], required=True)
    p.add_argument("--from", dest="fromyear", type=int, help="backfill 시작 연도")
    p.add_argument("--to", dest="toyear", type=int, help="backfill 종료 연도")
    p.add_argument("--date", help="daily 대상일 YYYYMMDD (기본: 오늘)")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "backfill":
        if not (args.fromyear and args.toyear):
            raise SystemExit("backfill 은 --from 과 --to (연도) 가 필요합니다.")
        run_backfill(args.fromyear, args.toyear, args.dest)
    else:
        day = args.date or date.today().strftime("%Y%m%d")
        run_daily(day, args.dest)


if __name__ == "__main__":
    main()
