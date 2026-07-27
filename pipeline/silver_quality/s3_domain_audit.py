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
        stats={
            "price_daily": price_stats,
            "corporate_action": action_stats,
            "_corporate_actions": action_df,
        },
    )


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
        bundle = (
            _price_bundle(str(root))
            if domain == "prices"
            else _fundamental_bundle(str(root))
        )
        results = evaluate(
            bundle,
            partition_key=f"domain:{domain}",
        )
        results.append(_required_domain_result(domain, bundle))
        print_summary(results)
        assert_publishable(results)
        gc.collect()
        repository.save_metrics(conn, context.run_id, bundle)
        repository.finish_run(conn, context, "CERTIFIED", results)
        print(json.dumps({
            "domain": domain,
            "run_id": str(context.run_id),
            "parent_run_id": str(parent_run_id),
            "fingerprint": fingerprint,
            "rows": (
                len(bundle.prices)
                if domain == "prices"
                else len(bundle.fundamentals)
            ),
            "warnings": {
                item.rule_code: item.failed_count
                for item in results
                if item.severity == Severity.WARNING
                and item.status == CheckStatus.FAIL
            },
        }, ensure_ascii=False, sort_keys=True), flush=True)
        return context.run_id
    except Exception as exc:
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
            "warnings": {
                item.rule_code: item.failed_count
                for item in results
                if item.severity == Severity.WARNING
                and item.status == CheckStatus.FAIL
            },
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
