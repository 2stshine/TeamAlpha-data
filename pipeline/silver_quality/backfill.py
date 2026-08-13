"""Legacy staged Silver backfill implementation (direct CLI disabled).

The retained helpers are useful for tests and a future closed orchestrator,
but the direct entrypoint can publish KRX/action inputs without completing the
v3 total-return contract.  ``main`` therefore fails before base or DB access.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import assets, corporate_actions, financials, prices
from pipeline.silver.dart_action_snapshot import (
    DEFAULT_COVERAGE_START,
    verify_snapshot_manifest,
)
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality import repository, staging
from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary

PUBLISH_LOCK_ID = 7_226_494_898
DIRECT_CLI_DISABLED_MESSAGE = (
    "direct Silver quality backfill CLI is disabled: it can leave the "
    "KRX total-return contract BUILDING without closed recertification"
)


def _fingerprint(base: str) -> str:
    manifest_fingerprint = Path(base) / ".bronze-input-fingerprint"
    if manifest_fingerprint.exists():
        value = manifest_fingerprint.read_text(encoding="utf-8").strip()
        if not value:
            raise RuntimeError("empty Bronze manifest fingerprint")
        return value

    digest = hashlib.sha256()
    root = Path(base)
    for path in sorted(
        p for p in root.rglob("*")
        if p.is_file() and p.suffix in {".parquet", ".json", ".xml"}
    ):
        stat = path.stat()
        digest.update(str(path.relative_to(root)).encode())
        digest.update(str(stat.st_size).encode())
        digest.update(str(stat.st_mtime_ns).encode())
    return digest.hexdigest()


def _candidate_bundle(base: str) -> CandidateBundle:
    action_snapshot = verify_snapshot_manifest(
        base, required_start=DEFAULT_COVERAGE_START,
    )
    asset_df, identifier_df = assets.prepare(base)
    price_df, price_stats = prices.prepare(base)
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
    fundamental_df, fundamental_stats = financials.prepare(base)
    fundamental_df, fundamental_stats = financials.exclude_nontradable(
        fundamental_df,
        fundamental_stats,
        supported_price_identifiers,
        all_price_identifiers - supported_price_identifiers,
    )
    action_df, action_stats = corporate_actions.prepare(
        base,
        coverage_start=action_snapshot.coverage_start,
        coverage_end=action_snapshot.coverage_end,
    )
    action_df, inherited_action_stats = corporate_actions.inherit_issuer_events(
        action_df,
        assets.preferred_share_issuer_map(asset_df),
    )
    action_stats["issuer_inheritance"] = inherited_action_stats
    # Mirror the daily path: DART actions for issuers outside the tradable KRX
    # price universe (KONEX-only, delisted, pre-coverage) are surfaced as an
    # explicit Silver exclusion rather than an identifier-mapping failure.
    action_df, action_stats = corporate_actions.exclude_nontradable(
        action_df,
        action_stats,
        supported_price_identifiers,
        all_price_identifiers - supported_price_identifiers,
    )
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        prices=price_df,
        fundamentals=fundamental_df,
        actions=action_df,
        stats={
            "price_daily": price_stats,
            "fundamental": fundamental_stats,
            "corporate_action": action_stats,
        },
    )


def _required_backfill_results(bundle: CandidateBundle) -> list[CheckResult]:
    required = {
        "asset": len(bundle.assets),
        "price_daily": len(bundle.prices),
        "fundamental": len(bundle.fundamentals),
    }
    return [
        CheckResult(
            rule_code="BACKFILL_REQUIRED_DATASET",
            dataset=dataset,
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS if count else CheckStatus.FAIL,
            expected="non-empty first-backfill candidate",
            actual=f"row_count={count}",
            failed_count=0 if count else 1,
        )
        for dataset, count in required.items()
    ]


def _asset_stage_frames(bundle: CandidateBundle) -> tuple[pd.DataFrame, pd.DataFrame]:
    return (
        bundle.assets[[
            "natural_key", "name", "asset_type", "instrument_type", "exchange",
            "currency", "country_code", "base_currency", "listed_from",
            "listed_to", "source_file",
        ]],
        bundle.identifiers[[
            "natural_key", "source", "identifier", "identifier_type",
            "valid_from", "valid_to", "source_file",
        ]],
    )


def _price_stage_frame(frame: pd.DataFrame) -> pd.DataFrame:
    staged = frame.copy()
    if "total_return_close" not in staged:
        staged["total_return_close"] = staged["adj_close"]
    if "currency" not in staged:
        staged["currency"] = "KRW"
    if "vwap" not in staged:
        staged["vwap"] = None
    if "available_at" not in staged:
        staged["available_at"] = (
            pd.to_datetime(staged["trade_date"])
            .dt.tz_localize("Asia/Seoul")
            + pd.Timedelta(days=1, hours=8, minutes=30)
        )
    return staged[[
        "identifier", "source", "trade_date", "open", "high", "low", "close",
        "adj_close", "total_return_close", "currency", "vwap", "available_at",
        "volume", "trading_value", "shares", "market_cap", "market",
        "asset_type", "prev_diff", "fluc_rate", "source_file",
    ]]


def _fundamental_stage_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[[
        "identifier", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "filing_id", "filed", "accepted_at",
        "available_date", "available_at", "metric", "value", "currency",
        "unit_type", "revision_key", "source_file",
    ]]


def _action_stage_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = corporate_actions.normalize_for_publish(frame)
    if normalized.empty:
        return normalized
    normalized["source_file"] = (
        frame["source_file"].reset_index(drop=True)
        if "source_file" in frame else None
    )
    return normalized


def _stage_partition(
    conn,
    parent,
    *,
    table: str,
    partition_key: str,
    frame: pd.DataFrame,
    bundle: CandidateBundle,
    target_date=None,
) -> None:
    child = repository.start_run(
        conn,
        mode="backfill_partition",
        status="RUNNING",
        target_date=target_date,
        parent_run_id=parent.run_id,
        partition_key=partition_key,
    )
    results = evaluate(
        bundle, target_date=target_date, partition_key=partition_key,
    )
    print_summary(results)
    try:
        assert_publishable(results)
        staging.replace_backfill_partition(
            conn, table, parent.run_id, partition_key, frame,
        )
        repository.save_metrics(conn, child.run_id, bundle)
        repository.finish_run(conn, child, "CERTIFIED", results)
        print(
            f"[backfill] staged partition={partition_key} "
            f"table={table} rows={len(frame)}",
            flush=True,
        )
    except QualityGateError as exc:
        conn.rollback()
        repository.finish_run(
            conn, child, "FAILED", results, error_message=str(exc),
        )
        raise


def _assert_final_empty(conn) -> None:
    with conn.cursor() as cur:
        counts = {}
        for table in (
            "corporate_action", "asset_identifier", "price_daily", "fundamental", "asset",
        ):
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
    if any(counts.values()):
        raise RuntimeError(
            f"first backfill refuses non-empty Silver tables: {counts}. "
            "명시적인 rebuild 절차가 필요합니다."
        )


def _publish(conn, context, bundle: CandidateBundle, results) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISH_LOCK_ID,))
        acquire_return_writer_transaction_lock(conn)
        _assert_final_empty(conn)
        krx_map = assets.publish(
            conn, bundle.assets, bundle.identifiers, context.run_id,
        )
        prices.publish(conn, bundle.prices, krx_map, context.run_id)
        financials.publish(conn, bundle.fundamentals, krx_map, context.run_id)
        corporate_actions.publish(conn, bundle.actions, krx_map, context.run_id)
        expected = {
            "price_daily": len(bundle.prices),
            "fundamental": len(bundle.fundamentals),
        }
        with conn.cursor() as cur:
            for table, count in expected.items():
                cur.execute(f"SELECT count(*) FROM {table}")
                actual = cur.fetchone()[0]
                if actual != count:
                    raise RuntimeError(
                        f"publish reconciliation failed {table}: "
                        f"expected={count}, actual={actual}"
                    )
        repository.save_metrics(conn, context.run_id, bundle)
        repository.finish_run(
            conn, context, "CERTIFIED", results, commit=False,
        )


def run(src: str = "local", resume: str | None = None) -> None:
    base = base_uri(src)
    conn = db.connect()
    context = None
    try:
        repository.assert_schema(conn)
        fingerprint = _fingerprint(base)
        if resume:
            context = repository.get_run(conn, UUID(resume))
            if context.mode != "backfill":
                raise ValueError(f"not a backfill run: {resume}")
            if context.input_fingerprint != fingerprint:
                raise RuntimeError(
                    "Bronze fingerprint가 최초 실행과 다릅니다. "
                    "변경된 입력은 새 backfill run으로 시작하세요."
                )
            with conn.cursor() as cur:
                cur.execute("DELETE FROM dq_result WHERE run_id=%s", (context.run_id,))
                cur.execute("DELETE FROM dq_metric WHERE run_id=%s", (context.run_id,))
            repository.update_status(conn, context.run_id, "BUILDING")
        else:
            context = repository.start_run(
                conn,
                mode="backfill",
                status="BUILDING",
                input_fingerprint=fingerprint,
            )

        print(f"[backfill] preparing candidates run={context.run_id}", flush=True)
        try:
            bundle = _candidate_bundle(base)
        except Exception as exc:
            transform_failure = CheckResult(
                rule_code="CANDIDATE_TRANSFORMATION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="all Bronze files parse into Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", [transform_failure],
                error_message=str(exc),
            )
            raise

        # Silver write 전 전체 preflight. 통계 경고도 이때 함께 계산한다.
        preflight = evaluate(bundle) + _required_backfill_results(bundle)
        print_summary(preflight)
        try:
            assert_publishable(preflight)
        except QualityGateError as exc:
            repository.finish_run(
                conn, context, "FAILED", preflight, error_message=str(exc),
            )
            raise

        done_asset = staging.staged_partitions(conn, context.run_id, "asset")
        done_identifier = staging.staged_partitions(
            conn, context.run_id, "asset_identifier",
        )
        if "asset:all" not in done_asset or "asset:all" not in done_identifier:
            asset_stage, identifier_stage = _asset_stage_frames(bundle)
            asset_bundle = CandidateBundle(
                assets=bundle.assets, identifiers=bundle.identifiers,
            )
            _stage_partition(
                conn, context, table="asset", partition_key="asset:all",
                frame=asset_stage, bundle=asset_bundle,
            )
            staging.replace_backfill_partition(
                conn, "asset_identifier", context.run_id,
                "asset:all", identifier_stage,
            )
            conn.commit()

        done_prices = staging.staged_partitions(conn, context.run_id, "price_daily")
        for trade_date, frame in bundle.prices.groupby("trade_date", sort=True):
            key = f"price:{trade_date.isoformat()}"
            if key in done_prices:
                continue
            price_bundle = CandidateBundle(
                assets=bundle.assets,
                identifiers=bundle.identifiers,
                prices=frame.reset_index(drop=True),
                stats={
                    "price_daily": {
                        "input_rows": len(frame),
                        "transformed_rows": len(frame),
                        "excluded_rows": 0,
                        "rejected_rows": 0,
                    }
                },
            )
            _stage_partition(
                conn, context, table="price_daily", partition_key=key,
                frame=_price_stage_frame(frame), bundle=price_bundle,
                target_date=trade_date,
            )

        done_fundamental = staging.staged_partitions(
            conn, context.run_id, "fundamental",
        )
        if not bundle.fundamentals.empty:
            groups = bundle.fundamentals.groupby(
                [bundle.fundamentals["period_end"].map(lambda d: d.year), "fiscal_period"],
                sort=True,
            )
            for (year, fiscal_period), frame in groups:
                key = f"fundamental:{year}:{fiscal_period}"
                if key in done_fundamental:
                    continue
                fundamental_bundle = CandidateBundle(
                    assets=bundle.assets,
                    identifiers=bundle.identifiers,
                    fundamentals=frame.reset_index(drop=True),
                )
                _stage_partition(
                    conn, context, table="fundamental", partition_key=key,
                    frame=_fundamental_stage_frame(frame),
                    bundle=fundamental_bundle,
                )

        done_actions = staging.staged_partitions(
            conn, context.run_id, "corporate_action",
        )
        if not bundle.actions.empty and "corporate_action:all" not in done_actions:
            action_bundle = CandidateBundle(
                assets=bundle.assets,
                identifiers=bundle.identifiers,
                actions=bundle.actions,
                stats={"corporate_action": bundle.stats.get("corporate_action", {})},
            )
            _stage_partition(
                conn,
                context,
                table="corporate_action",
                partition_key="corporate_action:all",
                frame=_action_stage_frame(bundle.actions),
                bundle=action_bundle,
            )

        repository.update_status(conn, context.run_id, "VALIDATING")
        global_results = evaluate(bundle) + _required_backfill_results(bundle)
        print_summary(global_results)
        try:
            assert_publishable(global_results)
            _publish(conn, context, bundle, global_results)
        except QualityGateError as exc:
            conn.rollback()
            repository.finish_run(
                conn, context, "FAILED", global_results,
                error_message=str(exc),
            )
            raise
        except Exception as exc:
            conn.rollback()
            publish_failure = CheckResult(
                rule_code="PUBLISH_TRANSACTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic first-backfill publish",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", global_results + [publish_failure],
                error_message=str(exc),
            )
            raise

        try:
            staging.cleanup_backfill(conn, context.run_id)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(
                f"[backfill] certified but staging cleanup failed: {exc}",
                flush=True,
            )
        print(f"[backfill] CERTIFIED run={context.run_id}", flush=True)
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", choices=["local"], default="local")
    parser.add_argument("--resume", help="기존 backfill parent dq_run UUID")
    return parser.parse_args()


def main() -> None:
    """Reject the unsafe direct write path before parsing or external access."""
    raise RuntimeError(DIRECT_CLI_DISABLED_MESSAGE)


def _unsafe_legacy_main() -> None:
    """Retained for a future closed orchestrator; never dispatch directly."""
    args = parse_args()
    run(args.src, args.resume)


if __name__ == "__main__":
    main()
