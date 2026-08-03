"""FMP Silver source-scoped load: raw Bronze -> DQ -> atomic RDS publish."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import fmp
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    CandidateBundle,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality.rules.fmp import check_fmp
from pipeline.silver_quality.runner import assert_publishable, print_summary


def _parse_day(day: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(day, fmt).date()
        except ValueError:
            pass
    raise SystemExit("날짜 형식은 YYYYMMDD 또는 YYYY-MM-DD 여야 합니다.")


def _fingerprint(base: str, target_date: date | None) -> str:
    digest = hashlib.sha256()
    root = Path(base)
    for path in sorted(root.rglob("manifest.json")):
        rendered = str(path).replace("\\", "/")
        if "/fmp/" not in rendered:
            continue
        try:
            metadata = json.loads(path.read_bytes())
        except (OSError, ValueError, TypeError):
            continue
        if target_date is not None:
            markers = re.findall(r"(?:date|snapshot_date)=(\d{4}-\d{2}-\d{2})", rendered)
            if markers and target_date.isoformat() not in markers:
                continue
        digest.update(str(path.relative_to(root)).encode())
        digest.update(str(metadata.get("sha256") or "").encode())
    return digest.hexdigest()


def _publish(conn, bundle, context, target_date: date | None) -> None:
    identifier_map = fmp.publish_assets(
        conn, bundle.assets, bundle.identifiers, context.run_id,
    )
    if target_date is not None:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM price_daily "
                "WHERE source IN ('FMP','FMP_FX') AND trade_date=%s",
                (target_date,),
            )
    fmp.publish_prices(conn, bundle.prices, identifier_map, context.run_id)
    fmp.publish_fundamentals(
        conn, bundle.fundamentals, identifier_map, context.run_id,
    )
    fmp.publish_actions(conn, bundle.actions, identifier_map, context.run_id)


def _daily(*, src: str, day: str) -> None:
    target_date = _parse_day(day)
    base = base_uri(src)
    conn = db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn,
            mode="fmp_daily",
            target_date=target_date,
            input_fingerprint=_fingerprint(base, target_date),
        )
        try:
            bundle = fmp.build_candidates(base, target_date)
        except Exception as exc:
            failure = CheckResult(
                rule_code="FMP_CANDIDATE_TRANSFORMATION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="FMP Bronze parses into Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", [failure], error_message=str(exc),
            )
            raise
        results = check_fmp(bundle)
        print_summary(results)
        try:
            assert_publishable(results)
        except QualityGateError as exc:
            repository.finish_run(
                conn, context, "FAILED", results, error_message=str(exc),
            )
            raise
        try:
            with conn.transaction():
                _publish(conn, bundle, context, target_date)
                repository.save_metrics(conn, context.run_id, bundle)
                repository.finish_run(
                    conn, context, "CERTIFIED", results, commit=False,
                )
        except Exception as exc:
            conn.rollback()
            failure = CheckResult(
                rule_code="FMP_PUBLISH_TRANSACTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="source-scoped atomic FMP publish",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", results + [failure],
                error_message=f"publish failed: {exc}",
            )
            raise
        print(
            f"[silver-fmp] certified run={context.run_id} date={target_date}",
            flush=True,
        )
    finally:
        conn.close()


def _discovered_years(base: str) -> list[int]:
    years: set[int] = set()
    root = Path(base)
    patterns = (
        "stock/fmp/eod-bulk/date=*/response.*",
        "financials/fmp/*/year=*/period=*/response.*",
        "corporate_actions/fmp/*/year=*/response.*",
        "corporate_actions/fmp/*/year=*/*/response.*",
    )
    for pattern in patterns:
        for path in root.glob(pattern):
            match = re.search(r"(?:date|year)=(\d{4})", str(path))
            if match:
                years.add(int(match.group(1)))
    return sorted(years)


def _completed_partitions(conn, parent_run_id: UUID) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partition_key FROM dq_run "
            "WHERE parent_run_id=%s AND status='CERTIFIED'",
            (parent_run_id,),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _certify_partition(conn, parent, key: str, bundle: CandidateBundle) -> None:
    child = repository.start_run(
        conn,
        mode="fmp_backfill_partition",
        parent_run_id=parent.run_id,
        partition_key=key,
    )
    results = check_fmp(bundle)
    print_summary(results)
    try:
        assert_publishable(results)
        with conn.transaction():
            _publish(conn, bundle, child, None)
            repository.save_metrics(conn, child.run_id, bundle)
            repository.finish_run(
                conn, child, "CERTIFIED", results, commit=False,
            )
    except Exception as exc:
        conn.rollback()
        repository.finish_run(
            conn, child, "FAILED", results, error_message=str(exc),
        )
        raise
    print(f"[silver-fmp] certified partition={key}", flush=True)


def _backfill(
    *,
    src: str,
    fromyear: int | None,
    toyear: int | None,
    resume: str | None,
) -> None:
    base = base_uri(src)
    fingerprint = _fingerprint(base, None)
    conn = db.connect()
    parent = None
    try:
        repository.assert_schema(conn)
        if resume:
            parent = repository.get_run(conn, UUID(resume))
            if parent.mode != "fmp_backfill":
                raise ValueError(f"not an FMP backfill run: {resume}")
            if parent.input_fingerprint != fingerprint:
                raise RuntimeError("FMP Bronze fingerprint changed; resume refused")
            repository.update_status(conn, parent.run_id, "BUILDING")
        else:
            parent = repository.start_run(
                conn,
                mode="fmp_backfill",
                status="BUILDING",
                input_fingerprint=fingerprint,
            )
        discovered = _discovered_years(base)
        if not discovered and (fromyear is None or toyear is None):
            raise RuntimeError("no FMP backfill years discovered")
        first = fromyear if fromyear is not None else min(discovered)
        last = toyear if toyear is not None else max(discovered)
        if first > last:
            raise ValueError("fromyear must be <= toyear")

        assets, identifiers, universe_stats = fmp.prepare_universe(base)
        fx_assets, fx_identifiers, _, _ = fmp.prepare_fx(base)
        assets = pd.concat([assets, fx_assets], ignore_index=True)
        identifiers = pd.concat([identifiers, fx_identifiers], ignore_index=True)
        completed = _completed_partitions(conn, parent.run_id)

        key = "asset:all"
        if key not in completed:
            _certify_partition(
                conn,
                parent,
                key,
                CandidateBundle(
                    assets=assets,
                    identifiers=identifiers,
                    stats={"asset": universe_stats, "_source": "FMP"},
                ),
            )

        for year in range(first, last + 1):
            price_key = f"price:year={year}"
            if price_key not in completed:
                stock_prices, price_stats = fmp.prepare_prices(
                    base, assets, identifiers, year=year,
                )
                _, _, fx_prices, fx_stats = fmp.prepare_fx(base, year=year)
                price_frame = pd.concat(
                    [stock_prices, fx_prices], ignore_index=True,
                )
                if not price_frame.empty:
                    _certify_partition(
                        conn,
                        parent,
                        price_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            prices=price_frame,
                            stats={
                                "asset": universe_stats,
                                "price_daily": price_stats,
                                "fx": fx_stats,
                                "_source": "FMP",
                            },
                        ),
                    )

            fundamental_key = f"fundamental:year={year}"
            if fundamental_key not in completed:
                fundamentals, stats = fmp.prepare_fundamentals(
                    base, identifiers, year=year,
                )
                if not fundamentals.empty:
                    _certify_partition(
                        conn,
                        parent,
                        fundamental_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            fundamentals=fundamentals,
                            stats={
                                "asset": universe_stats,
                                "fundamental": stats,
                                "_source": "FMP",
                            },
                        ),
                    )

            action_key = f"corporate_action:year={year}"
            if action_key not in completed:
                actions, stats = fmp.prepare_actions(
                    base, identifiers, year=year,
                )
                if not actions.empty:
                    _certify_partition(
                        conn,
                        parent,
                        action_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            actions=actions,
                            stats={
                                "asset": universe_stats,
                                "corporate_action": stats,
                                "_source": "FMP",
                            },
                        ),
                    )

        repository.finish_run(conn, parent, "CERTIFIED", [])
        print(f"[silver-fmp] certified backfill run={parent.run_id}", flush=True)
    except Exception as exc:
        if parent is not None:
            conn.rollback()
            repository.finish_run(
                conn, parent, "FAILED", [], error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def run(
    *,
    src: str = "local",
    day: str | None = None,
    fromyear: int | None = None,
    toyear: int | None = None,
    resume: str | None = None,
) -> None:
    if day is not None:
        _daily(src=src, day=day)
        return
    _backfill(
        src=src,
        fromyear=fromyear,
        toyear=toyear,
        resume=resume,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("daily", "backfill"), required=True)
    parser.add_argument("--date")
    parser.add_argument("--from", dest="fromyear", type=int)
    parser.add_argument("--to", dest="toyear", type=int)
    parser.add_argument("--resume")
    parser.add_argument("--src", choices=("local",), default="local")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "daily" and not args.date:
        raise SystemExit("daily mode requires --date")
    run(
        src=args.src,
        day=args.date if args.mode == "daily" else None,
        fromyear=args.fromyear,
        toyear=args.toyear,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
