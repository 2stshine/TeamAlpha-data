"""Silver 오케스트레이터: 후보 생성 → 자동 품질 검사 → 원자적 publish."""
from __future__ import annotations

import argparse
from datetime import date, datetime

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import assets, corporate_actions, financials, prices
from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary


def _parse_day(day: str | None) -> date:
    if not day:
        raise SystemExit("incremental 은 --date YYYYMMDD 가 필요합니다.")
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(day, fmt).date()
        except ValueError:
            pass
    raise SystemExit("날짜 형식은 YYYYMMDD 또는 YYYY-MM-DD 여야 합니다.")


def _build_candidates(
    base: str,
    *,
    target_date: date | None,
    financial_files: list[str] | None,
) -> CandidateBundle:
    asset_df, identifier_df = assets.prepare(base)
    preferred_to_common = assets.preferred_share_issuer_map(asset_df)
    price_df, price_stats = prices.prepare(base, target_date=target_date)
    all_price_identifiers = set(
        asset_df.loc[
            asset_df["asset_type"].eq("stock"), "natural_key"
        ].astype(str)
    )
    supported_price_identifiers = set(
        price_df.loc[
            price_df["asset_type"].eq("stock"), "identifier"
        ].astype(str)
    )
    asset_df, identifier_df = assets.restrict_to_price_universe(
        asset_df,
        identifier_df,
        supported_price_identifiers,
    )
    if financial_files is not None:
        fundamental_df, fundamental_stats = financials.prepare(
            base, files=financial_files,
        )
    else:
        years = {target_date.year} if target_date else None
        fundamental_df, fundamental_stats = financials.prepare(base, years=years)
    action_df, action_stats = corporate_actions.prepare(
        base,
        target_date=target_date,
    )
    action_df, inherited_action_stats = corporate_actions.inherit_issuer_events(
        action_df,
        preferred_to_common,
    )
    action_stats["issuer_inheritance"] = inherited_action_stats
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        prices=price_df,
        fundamentals=fundamental_df,
        stats={
            "price_daily": price_stats,
            "fundamental": fundamental_stats,
            "corporate_action": action_stats,
            "_corporate_actions": action_df,
            "_unsupported_market_identifiers": (
                all_price_identifiers - supported_price_identifiers
            ),
        },
    )


def incremental(
    day: str | None = None,
    src: str = "local",
    financial_files: list[str] | None = None,
    market_closed: bool = False,
) -> None:
    target_date = _parse_day(day)
    base = base_uri(src)
    conn = db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn, mode="daily", target_date=target_date, status="RUNNING",
        )
        if market_closed and not financial_files:
            repository.finish_run(conn, context, "SKIPPED", [])
            print(
                f"[silver-quality] skipped market holiday date={target_date}",
                flush=True,
            )
            return
        try:
            bundle = _build_candidates(
                base,
                target_date=target_date,
                financial_files=financial_files,
            )
        except Exception as exc:
            transform_failure = CheckResult(
                rule_code="CANDIDATE_TRANSFORMATION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="Bronze parses into Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", [transform_failure],
                error_message=str(exc),
            )
            raise
        bundle.stats["_existing_krx_identifiers"] = (
            repository.existing_krx_identifiers(conn)
        )
        candidate_krx_identifiers = set(
            bundle.identifiers.loc[
                bundle.identifiers["source"].eq("KRX"), "identifier"
            ].astype(str)
        )
        full_price_universe = (
            candidate_krx_identifiers
            | bundle.stats["_existing_krx_identifiers"]
        )
        (
            bundle.fundamentals,
            bundle.stats["fundamental"],
        ) = financials.exclude_nontradable(
            bundle.fundamentals,
            bundle.stats["fundamental"],
            full_price_universe,
            bundle.stats["_unsupported_market_identifiers"],
        )
        history = repository.recent_price_history(
            conn,
            bundle.prices["identifier"].astype(str).unique().tolist()
            if not bundle.prices.empty else [],
            target_date,
        )
        conn.commit()
        results = evaluate(bundle, target_date=target_date, history=history)
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
                krx_map = assets.publish(
                    conn, bundle.assets, bundle.identifiers, context.run_id,
                )
                if not bundle.prices.empty:
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM price_daily "
                            "WHERE source='KRX' AND trade_date=%s",
                            (target_date,),
                        )
                        print(
                            f"[prices] 기존 price_daily {target_date} "
                            f"{cur.rowcount}행 교체",
                            flush=True,
                        )
                    prices.publish(
                        conn, bundle.prices, krx_map, context.run_id, target_date,
                    )
                financials.publish(
                    conn, bundle.fundamentals, krx_map, context.run_id,
                )
                repository.save_metrics(conn, context.run_id, bundle)
                repository.finish_run(
                    conn, context, "CERTIFIED", results, commit=False,
                )
        except Exception as exc:
            conn.rollback()
            publish_failure = CheckResult(
                rule_code="PUBLISH_TRANSACTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic publish commit",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn,
                context,
                "FAILED",
                results + [publish_failure],
                error_message=f"publish failed: {exc}",
            )
            raise
        print(
            f"[silver-quality] certified daily run={context.run_id} "
            f"date={target_date}",
            flush=True,
        )
    finally:
        conn.close()


def backfill(src: str = "local", resume: str | None = None) -> None:
    """영구 quality_stage를 사용하는 최초 backfill."""
    from pipeline.silver_quality.backfill import run

    run(src=src, resume=resume)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["backfill", "incremental"], default="backfill")
    p.add_argument("--src", choices=["local"], default="local")
    p.add_argument("--date", help="incremental 대상일 YYYYMMDD")
    p.add_argument("--resume", help="backfill dq_run UUID")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "backfill":
        backfill(args.src, args.resume)
    else:
        incremental(args.date, args.src)


if __name__ == "__main__":
    main()
