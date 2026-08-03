"""불변 S3 cutoff를 가격·재무 도메인으로 분리해 감사하고 종합 인증한다.

각 domain 명령은 별도 프로세스에서 실행되므로 가격과 재무 전체 DataFrame을
동시에 메모리에 보유하지 않는다. finalize는 후보 데이터를 다시 읽지 않고
동일 fingerprint/ruleset의 자식 DQ 결과만 합쳐 부모 run을 CERTIFIED로 만든다.
"""
from __future__ import annotations

import argparse
import gc
import json
import os
from datetime import date
from pathlib import Path
from uuid import UUID

import boto3
import pandas as pd

from pipeline.common import db
from pipeline.silver import assets, corporate_actions, financials, prices
from pipeline.silver_quality import QUALITY_RULESET_VERSION, repository
from pipeline.silver_quality.ecs_backfill import _load_manifests, _sync_cutoff
from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality.runner import (
    assert_publishable,
    evaluate,
    print_summary,
)

PARENT_MODE = "s3_quality_audit_split"
DOMAIN_MODES = {
    "prices": "s3_quality_audit_prices",
    "fundamentals": "s3_quality_audit_fundamentals",
}
DOMAIN_PREFIXES = {
    "prices": (
        "stock/",
        "index/",
        "corporate_actions/",
        "financials/dart/corpCode.xml",
    ),
    "fundamentals": ("stock/", "financials/"),
}


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def _warning_totals(results: list[CheckResult]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for item in results:
        if (
            item.severity == Severity.WARNING
            and item.status == CheckStatus.FAIL
        ):
            totals[item.rule_code] = (
                totals.get(item.rule_code, 0) + item.failed_count
            )
    return totals


def _manifest_fingerprint() -> str:
    _, fingerprint = _load_manifests(
        boto3.client("s3"),
        _required("S3_BRONZE_BUCKET"),
        _required("BACKFILL_MANIFEST_PREFIX"),
    )
    return fingerprint


def _parent_fingerprint(conn, parent_run_id: UUID) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT mode, status, input_fingerprint, ruleset_version
            FROM dq_run WHERE run_id=%s
            """,
            (parent_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"parent DQ run not found: {parent_run_id}")
    mode, status, fingerprint, ruleset = row
    if (
        mode != PARENT_MODE
        or status != "VALIDATING"
        or ruleset != QUALITY_RULESET_VERSION
    ):
        raise RuntimeError(
            "invalid split-audit parent: "
            f"mode={mode}, status={status}, ruleset={ruleset}"
        )
    return fingerprint


def _price_universes(base: str) -> tuple[set[str], set[str]]:
    """가격 파일을 한 개씩 읽어 전체/지원시장 ticker 집합만 유지한다."""
    all_identifiers: set[str] = set()
    supported_identifiers: set[str] = set()
    for path in sorted(Path(base).glob("stock/marcap/date=*/all.parquet")):
        frame = pd.read_parquet(path, columns=["Code", "Market"])
        identifiers = frame["Code"].astype(str)
        markets = frame["Market"].astype(str).replace(prices.MARKET_NORM)
        all_identifiers.update(identifiers)
        supported_identifiers.update(
            identifiers[~markets.isin(prices.UNSUPPORTED_MARKETS)]
        )
    for path in sorted(Path(base).glob("stock/krxapi/date=*/*.parquet")):
        frame = pd.read_parquet(path, columns=["ISU_CD", "MKT_NM"])
        identifiers = frame["ISU_CD"].astype(str)
        markets = frame["MKT_NM"].astype(str).replace(prices.MARKET_NORM)
        all_identifiers.update(identifiers)
        supported_identifiers.update(
            identifiers[~markets.isin(prices.UNSUPPORTED_MARKETS)]
        )
    return all_identifiers, supported_identifiers


def _price_bundle(base: str) -> CandidateBundle:
    asset_df, identifier_df = assets.prepare(base)
    price_df, price_stats = prices.prepare(base)
    supported = set(
        price_df.loc[
            price_df["asset_type"].eq("stock"), "identifier"
        ].astype(str)
    )
    asset_df, identifier_df = assets.restrict_to_price_universe(
        asset_df,
        identifier_df,
        supported,
    )
    action_df, action_stats = corporate_actions.prepare(base)
    action_df, inheritance = corporate_actions.inherit_issuer_events(
        action_df,
        assets.preferred_share_issuer_map(asset_df),
    )
    action_stats["issuer_inheritance"] = inheritance
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        prices=price_df,
        actions=action_df,
        stats={
            "price_daily": price_stats,
            "corporate_action": action_stats,
        },
    )


def _price_static_bundle(base: str) -> CandidateBundle:
    """Prepare only bounded price-domain inputs shared by annual partitions."""
    asset_df, identifier_df = assets.prepare(base)
    _, supported = _price_universes(base)
    asset_df, identifier_df = assets.restrict_to_price_universe(
        asset_df,
        identifier_df,
        supported,
    )
    action_df, action_stats = corporate_actions.prepare(base)
    action_df, inheritance = corporate_actions.inherit_issuer_events(
        action_df,
        assets.preferred_share_issuer_map(asset_df),
    )
    action_stats["issuer_inheritance"] = inheritance
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        actions=action_df,
        stats={
            "corporate_action": action_stats,
        },
    )


def _price_history_tail(
    previous: pd.DataFrame,
    current: pd.DataFrame,
    trading_days: int = 20,
) -> pd.DataFrame:
    """Keep only the rows needed for cross-year lags and 20-day baselines."""
    combined = pd.concat([previous, current], ignore_index=True)
    if combined.empty:
        return combined
    dates = sorted(combined["trade_date"].dropna().unique())
    keep_dates = set(dates[-trading_days:])
    return combined[combined["trade_date"].isin(keep_dates)].copy()


def _align_history_adj_close(
    history: pd.DataFrame,
    current: pd.DataFrame,
) -> pd.DataFrame:
    """Align prior-year tail scale to the independently normalized year.

    Annual preparation intentionally normalizes adj_close at each year end.
    Only the last prior observation is used to verify the first current
    observation's adjusted-return recurrence, so rescale that anchor from the
    KRX reference return. Close-based spike and drift rules are unaffected.
    """
    if history.empty or current.empty:
        return history
    aligned = history.copy()
    first = (
        current.sort_values(["identifier", "trade_date"])
        .groupby("identifier", sort=False)
        .head(1)
        .set_index("identifier")
    )
    last_indices = (
        aligned.sort_values(["identifier", "trade_date"])
        .groupby("identifier", sort=False)
        .tail(1)
        .index
    )
    for index in last_indices:
        identifier = str(aligned.at[index, "identifier"])
        if identifier not in first.index:
            continue
        row = first.loc[identifier]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        previous_close = pd.to_numeric(
            pd.Series([aligned.at[index, "close"]]),
            errors="coerce",
        ).iloc[0]
        close = pd.to_numeric(pd.Series([row["close"]]), errors="coerce").iloc[0]
        prev_diff = pd.to_numeric(
            pd.Series([row["prev_diff"]]),
            errors="coerce",
        ).iloc[0]
        current_adj = pd.to_numeric(
            pd.Series([row["adj_close"]]),
            errors="coerce",
        ).iloc[0]
        reference = close - prev_diff
        if (
            pd.isna(previous_close)
            or pd.isna(reference)
            or pd.isna(current_adj)
            or previous_close <= 0
            or reference <= 0
        ):
            continue
        economic_return = close / reference - 1
        aligned.at[index, "adj_close"] = current_adj / (1 + economic_return)
    return aligned


def _run_price_partitions(
    base: str,
) -> tuple[list[CheckResult], int, set[str], dict[int, int]]:
    """Evaluate the complete cutoff one calendar year at a time."""
    static = _price_static_bundle(base)
    years = prices.available_years(base)
    if not years:
        return [], 0, set(), {}
    results: list[CheckResult] = []
    history = pd.DataFrame()
    row_count = 0
    identifiers: set[str] = set()
    year_row_counts: dict[int, int] = {}
    final_year = years[-1]
    for year in years:
        frame, stats = prices.prepare(
            base,
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31),
        )
        aligned_history = _align_history_adj_close(history, frame)
        lookahead = pd.DataFrame()
        if year < final_year:
            # Corporate actions at year end may be reflected on the first
            # trading day of the next year. Keep a small forward window so
            # event-centric checks have the same evidence as a full scan.
            lookahead, _ = prices.prepare(
                base,
                start_date=date(year + 1, 1, 1),
                end_date=date(year + 1, 1, 31),
            )
        evaluation_context = pd.concat(
            [aligned_history, lookahead],
            ignore_index=True,
        )
        bundle = CandidateBundle(
            assets=static.assets,
            identifiers=static.identifiers,
            prices=frame,
            actions=static.actions,
            stats={
                "price_daily": stats,
                "corporate_action": static.stats["corporate_action"],
            },
        )
        partition_key = f"year:{year}"
        partition_results = evaluate(
            bundle,
            history=evaluation_context,
            partition_key=partition_key,
        )
        results.extend(partition_results)
        try:
            assert_publishable(partition_results)
        except QualityGateError:
            raise QualityGateError(results)
        rows = len(frame)
        row_count += rows
        year_row_counts[year] = rows
        identifiers.update(frame["identifier"].astype(str))
        history = _price_history_tail(history, frame)
        print(
            json.dumps({
                "domain": "prices",
                "partition_key": partition_key,
                "rows": rows,
                "cumulative_rows": row_count,
            }, sort_keys=True),
            flush=True,
        )
        del (
            aligned_history,
            evaluation_context,
            lookahead,
            bundle,
            frame,
            partition_results,
        )
        gc.collect()
    results.append(CheckResult(
        rule_code="PARTITIONED_PRICE_AUDIT_COMPLETE",
        dataset="price_daily",
        severity=Severity.CRITICAL,
        status=CheckStatus.PASS,
        expected="every discovered price year is evaluated exactly once",
        actual=f"years={years}, row_count={row_count}",
        partition_key="domain:prices",
    ))
    return results, row_count, identifiers, year_row_counts


def _fundamental_bundle(base: str) -> CandidateBundle:
    asset_df, identifier_df = assets.prepare(base)
    all_identifiers, supported = _price_universes(base)
    asset_df, identifier_df = assets.restrict_to_price_universe(
        asset_df,
        identifier_df,
        supported,
    )
    fundamental_df, fundamental_stats = financials.prepare(base)
    fundamental_df, fundamental_stats = financials.exclude_nontradable(
        fundamental_df,
        fundamental_stats,
        supported,
        all_identifiers - supported,
    )
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        fundamentals=fundamental_df,
        stats={"fundamental": fundamental_stats},
    )


def _required_domain_result(
    domain: str,
    bundle: CandidateBundle,
) -> CheckResult:
    rows = len(
        bundle.prices if domain == "prices" else bundle.fundamentals
    )
    return CheckResult(
        rule_code="SPLIT_AUDIT_REQUIRED_DATASET",
        dataset="price_daily" if domain == "prices" else "fundamental",
        severity=Severity.CRITICAL,
        status=CheckStatus.PASS if rows else CheckStatus.FAIL,
        expected=f"non-empty {domain} candidate for split audit",
        actual=f"row_count={rows}",
        failed_count=0 if rows else 1,
        partition_key=f"domain:{domain}",
    )


def init_parent() -> UUID:
    fingerprint = _manifest_fingerprint()
    conn = db.connect()
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn,
            mode=PARENT_MODE,
            status="VALIDATING",
            input_fingerprint=fingerprint,
        )
        print(json.dumps({
            "parent_run_id": str(context.run_id),
            "fingerprint": fingerprint,
            "ruleset_version": QUALITY_RULESET_VERSION,
        }, sort_keys=True), flush=True)
        return context.run_id
    finally:
        conn.close()


def run_domain(domain: str, parent_run_id: UUID) -> UUID:
    root = Path(os.environ.get("BACKFILL_DATA_ROOT", "/app/data"))
    fingerprint = _sync_cutoff(
        root,
        include_prefixes=DOMAIN_PREFIXES[domain],
    )
    conn = db.connect()
    context = None
    results: list[CheckResult] = []
    try:
        repository.assert_schema(conn)
        expected_fingerprint = _parent_fingerprint(conn, parent_run_id)
        if fingerprint != expected_fingerprint:
            raise RuntimeError(
                "domain cutoff fingerprint differs from parent: "
                f"expected={expected_fingerprint}, actual={fingerprint}"
            )
        context = repository.start_run(
            conn,
            mode=DOMAIN_MODES[domain],
            status="VALIDATING",
            parent_run_id=parent_run_id,
            partition_key=f"domain:{domain}",
            input_fingerprint=fingerprint,
        )
        if domain == "prices":
            (
                results,
                row_count,
                price_identifiers,
                year_row_counts,
            ) = _run_price_partitions(str(root))
            required = CheckResult(
                rule_code="SPLIT_AUDIT_REQUIRED_DATASET",
                dataset="price_daily",
                severity=Severity.CRITICAL,
                status=CheckStatus.PASS if row_count else CheckStatus.FAIL,
                expected="non-empty prices candidate for split audit",
                actual=f"row_count={row_count}",
                failed_count=0 if row_count else 1,
                partition_key="domain:prices",
            )
            results.append(required)
            bundle = None
        else:
            bundle = _fundamental_bundle(str(root))
            results = evaluate(
                bundle,
                partition_key=f"domain:{domain}",
            )
            results.append(_required_domain_result(domain, bundle))
            row_count = len(bundle.fundamentals)
        print_summary(results)
        assert_publishable(results)
        gc.collect()
        if domain == "prices":
            repository.save_price_partition_metrics(
                conn,
                context.run_id,
                row_count=row_count,
                instrument_count=len(price_identifiers),
                year_row_counts=year_row_counts,
            )
        else:
            repository.save_metrics(conn, context.run_id, bundle)
        repository.finish_run(conn, context, "CERTIFIED", results)
        print(json.dumps({
            "domain": domain,
            "run_id": str(context.run_id),
            "parent_run_id": str(parent_run_id),
            "fingerprint": fingerprint,
            "rows": row_count,
            "warnings": _warning_totals(results),
        }, ensure_ascii=False, sort_keys=True), flush=True)
        return context.run_id
    except Exception as exc:
        if isinstance(exc, QualityGateError):
            results = exc.results
        try:
            conn.rollback()
            if context is not None:
                repository.finish_run(
                    conn,
                    context,
                    "FAILED",
                    results,
                    error_message=str(exc),
                )
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _latest_children(conn, parent_run_id: UUID) -> dict[str, tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, mode, status, input_fingerprint, ruleset_version
            FROM dq_run
            WHERE parent_run_id=%s AND mode = ANY(%s)
            ORDER BY started_at DESC
            """,
            (parent_run_id, list(DOMAIN_MODES.values())),
        )
        rows = cur.fetchall()
    latest = {}
    for row in rows:
        latest.setdefault(row[1], row)
    return latest


def _combined_child_results(
    conn,
    child_by_domain: dict[str, tuple],
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for domain, child in child_by_domain.items():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dataset_name, rule_code, severity, status,
                       expected_value, actual_value, failed_count,
                       sample_records
                FROM dq_result
                WHERE run_id=%s
                ORDER BY result_id
                """,
                (child[0],),
            )
            rows = cur.fetchall()
        results.extend(
            CheckResult(
                rule_code=row[1],
                dataset=row[0],
                severity=Severity(row[2]),
                status=CheckStatus(row[3]),
                expected=row[4],
                actual=row[5],
                failed_count=row[6],
                samples=row[7] or [],
                partition_key=f"domain:{domain}",
            )
            for row in rows
        )
        results.append(CheckResult(
            rule_code="SPLIT_AUDIT_DOMAIN_CERTIFIED",
            dataset=domain,
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected=(
                "domain child run is CERTIFIED for parent "
                "fingerprint/ruleset"
            ),
            actual=f"child_run_id={child[0]}",
            partition_key=f"domain:{domain}",
        ))
    return results


def finalize(parent_run_id: UUID) -> None:
    conn = db.connect()
    try:
        repository.assert_schema(conn)
        fingerprint = _parent_fingerprint(conn, parent_run_id)
        children = _latest_children(conn, parent_run_id)
        resolved: dict[str, tuple] = {}
        problems = []
        for domain, mode in DOMAIN_MODES.items():
            child = children.get(mode)
            if child is None:
                problems.append(f"{domain}:missing")
                continue
            _, _, status, child_fingerprint, ruleset = child
            if (
                status != "CERTIFIED"
                or child_fingerprint != fingerprint
                or ruleset != QUALITY_RULESET_VERSION
            ):
                problems.append(
                    f"{domain}:status={status},"
                    f"fingerprint={child_fingerprint},ruleset={ruleset}"
                )
                continue
            resolved[domain] = child
        if problems:
            raise RuntimeError(
                "split audit cannot be certified: " + "; ".join(problems)
            )
        results = _combined_child_results(conn, resolved)
        assert_publishable(results)
        context = repository.get_run(conn, parent_run_id)
        repository.finish_run(conn, context, "CERTIFIED", results)
        print_summary(results)
        print(json.dumps({
            "parent_run_id": str(parent_run_id),
            "status": "CERTIFIED",
            "fingerprint": fingerprint,
            "domain_runs": {
                domain: str(child[0])
                for domain, child in resolved.items()
            },
            "warnings": _warning_totals(results),
        }, ensure_ascii=False, sort_keys=True), flush=True)
    except Exception as exc:
        conn.rollback()
        try:
            context = repository.get_run(conn, parent_run_id)
            repository.finish_run(
                conn,
                context,
                "FAILED",
                [],
                error_message=str(exc),
            )
        except Exception:
            pass
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--action",
        choices=["init", "domain", "finalize"],
        required=True,
    )
    parser.add_argument("--domain", choices=sorted(DOMAIN_MODES))
    parser.add_argument("--parent-run-id")
    args = parser.parse_args()
    if args.action == "domain" and not args.domain:
        parser.error("--domain is required for --action domain")
    if args.action in {"domain", "finalize"} and not args.parent_run_id:
        parser.error("--parent-run-id is required")
    return args


def main() -> None:
    args = parse_args()
    if args.action == "init":
        init_parent()
    elif args.action == "domain":
        run_domain(args.domain, UUID(args.parent_run_id))
    else:
        finalize(UUID(args.parent_run_id))


if __name__ == "__main__":
    main()
