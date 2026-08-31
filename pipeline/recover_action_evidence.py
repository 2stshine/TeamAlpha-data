"""Recover a complete immutable DART action-evidence interval in S3."""
from __future__ import annotations

import argparse
from datetime import datetime

from pipeline import dart_silver_backfill_ecs
from pipeline.bronze import corporate_actions


def run(fromdate: str, todate: str) -> None:
    start = datetime.strptime(fromdate, "%Y%m%d").date()
    end = datetime.strptime(todate, "%Y%m%d").date()
    if start > end:
        raise ValueError("--from must not be after --to")

    certification_lock = (
        dart_silver_backfill_ecs.acquire_daily_certification_lock()
    )
    invalidated = False

    def invalidate_before_first_write(_path: str) -> None:
        nonlocal invalidated
        dart_silver_backfill_ecs.assert_daily_certification_lock(
            certification_lock,
        )
        if invalidated:
            return
        dart_silver_backfill_ecs.invalidate_total_return_for_observed_action(
            end,
            conn=certification_lock,
        )
        invalidated = True

    try:
        changed = corporate_actions.run(
            fromdate,
            todate,
            "s3",
            include_dependencies=False,
            before_change=invalidate_before_first_write,
        )
        dart_silver_backfill_ecs.assert_daily_certification_lock(
            certification_lock,
        )
        print(
            "[action-evidence-recovery] complete "
            f"interval={fromdate}~{todate} changed={len(changed)}",
            flush=True,
        )
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(
            certification_lock,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="fromdate", required=True)
    parser.add_argument("--to", dest="todate", required=True)
    args = parser.parse_args()
    run(args.fromdate, args.todate)


if __name__ == "__main__":
    main()
