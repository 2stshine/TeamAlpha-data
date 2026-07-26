"""기존 Silver read-only 검사 후 결과만 dq_* 테이블에 기록."""
from __future__ import annotations

import argparse

import pandas as pd

from pipeline.common import db
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.runner import evaluate, print_summary


def _read(conn, query: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def run(scope: str = "all") -> None:
    conn = db.connect()
    context = None
    try:
        repository.assert_schema(conn)
        context = repository.start_run(conn, mode="audit", status="VALIDATING")
        assets = _read(conn, """
            SELECT ai.identifier AS natural_key, a.name, a.asset_type,
                   a.exchange, a.currency
            FROM asset a
            JOIN asset_identifier ai ON ai.asset_id=a.asset_id AND ai.source='KRX'
        """)
        identifiers = _read(conn, """
            SELECT krx.identifier AS natural_key, ai.source, ai.identifier
            FROM asset_identifier ai
            JOIN asset_identifier krx
              ON krx.asset_id=ai.asset_id AND krx.source='KRX'
        """)
        prices = pd.DataFrame()
        fundamentals = pd.DataFrame()
        if scope in {"all", "price"}:
            prices = _read(conn, """
                SELECT ai.identifier, p.source, p.trade_date, p.open, p.high, p.low,
                       p.close, p.adj_close, p.volume, p.trading_value, p.shares,
                       p.market_cap, p.market, a.asset_type,
                       NULL::numeric AS prev_diff, NULL::numeric AS fluc_rate
                FROM price_daily p
                JOIN asset a ON a.asset_id=p.asset_id
                JOIN asset_identifier ai
                  ON ai.asset_id=p.asset_id AND ai.source='KRX'
            """)
            for column in [
                "open", "high", "low", "close", "adj_close", "volume",
                "trading_value", "shares", "market_cap", "prev_diff", "fluc_rate",
            ]:
                prices[column] = pd.to_numeric(prices[column], errors="coerce")
        if scope in {"all", "fundamental"}:
            fundamentals = _read(conn, """
                SELECT ai.identifier, f.source, f.period_end, f.fiscal_period,
                       f.fs_type, f.filing_id, f.filed, f.available_date,
                       f.metric, f.value, f.currency, f.revision_key
                FROM fundamental f
                JOIN asset_identifier ai
                  ON ai.asset_id=f.asset_id AND ai.source='KRX'
            """)
            fundamentals["value"] = pd.to_numeric(
                fundamentals["value"], errors="coerce",
            )
        bundle = CandidateBundle(assets, identifiers, prices, fundamentals)
        results = evaluate(bundle)
        print_summary(results)
        repository.save_metrics(conn, context.run_id, bundle)
        status = "FAILED" if any(r.blocks_publish for r in results) else "CERTIFIED"
        repository.finish_run(conn, context, status, results)
        print(f"[silver-quality] audit {status} run={context.run_id}", flush=True)
        if status == "FAILED":
            raise SystemExit(1)
    except Exception as exc:
        conn.rollback()
        if context is not None:
            failure = CheckResult(
                rule_code="AUDIT_EXECUTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="audit completes",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", [failure], error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope", choices=["all", "price", "fundamental"], default="all",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.scope)
