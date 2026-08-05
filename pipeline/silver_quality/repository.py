"""DQ 실행 결과와 baseline metric 저장."""
from __future__ import annotations

import json
import math
from datetime import date
from uuid import UUID, uuid4

import pandas as pd

from pipeline.silver_quality import QUALITY_RULESET_VERSION
from pipeline.silver_quality.models import (
    BatchContext,
    CheckResult,
    CheckStatus,
    Severity,
)


INCREMENTAL_WARNING_MODES = frozenset({"daily", "fmp_daily"})


def _json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def assert_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.dq_run')")
        if cur.fetchone()[0] is None:
            raise RuntimeError(
                "Silver quality schema가 없습니다. "
                "`python -m pipeline.silver_quality.migrate`를 먼저 실행하세요."
            )


def start_run(
    conn,
    *,
    mode: str,
    status: str = "RUNNING",
    target_date: date | None = None,
    parent_run_id: UUID | None = None,
    partition_key: str | None = None,
    input_fingerprint: str | None = None,
    run_id: UUID | None = None,
) -> BatchContext:
    run_id = run_id or uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq_run (
                run_id, parent_run_id, mode, target_date, partition_key,
                input_fingerprint, ruleset_version, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_id, parent_run_id, mode, target_date, partition_key,
                input_fingerprint, QUALITY_RULESET_VERSION, status,
            ),
        )
    conn.commit()
    return BatchContext(
        run_id=run_id,
        mode=mode,
        target_date=target_date,
        parent_run_id=parent_run_id,
        partition_key=partition_key,
        input_fingerprint=input_fingerprint,
    )


def get_run(conn, run_id: UUID) -> BatchContext:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT mode, target_date, parent_run_id, partition_key, input_fingerprint
            FROM dq_run WHERE run_id=%s
            """,
            (run_id,),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"dq_run not found: {run_id}")
    return BatchContext(run_id, row[0], row[1], row[2], row[3], row[4])


def save_results(conn, run_id: UUID, results: list[CheckResult]) -> None:
    if not results:
        return
    with conn.cursor() as cur:
        for r in results:
            cur.execute(
                """
                INSERT INTO dq_result (
                    run_id, partition_key, dataset_name, rule_code, severity,
                    status, expected_value, actual_value, failed_count, sample_records
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """,
                (
                    run_id, r.partition_key, r.dataset, r.rule_code,
                    r.severity.value, r.status.value, r.expected, r.actual,
                    r.failed_count,
                    json.dumps(
                        _json_safe(r.samples), ensure_ascii=False,
                        default=str, allow_nan=False,
                    ),
                ),
            )


def _warning_scope_key(
    context: BatchContext,
    result: CheckResult,
) -> tuple[str, str | None]:
    partition_key = result.partition_key or context.partition_key
    if partition_key:
        return f"partition={partition_key}", partition_key
    if context.target_date:
        return f"date={context.target_date.isoformat()}", None
    return f"run={context.run_id}", None


def _incremental_warning_entries(
    context: BatchContext,
    results: list[CheckResult],
) -> list[dict]:
    """Consolidate warning observations to one row per evaluated scope/rule."""
    entries: dict[tuple[str, str, str], dict] = {}
    for result in results:
        if result.severity != Severity.WARNING:
            continue
        scope_key, partition_key = _warning_scope_key(context, result)
        key = (scope_key, result.dataset, result.rule_code)
        entry = entries.setdefault(
            key,
            {
                "scope_key": scope_key,
                "partition_key": partition_key,
                "dataset_name": result.dataset,
                "rule_code": result.rule_code,
                "status": CheckStatus.PASS,
                "failed_count": 0,
                "expected_value": result.expected,
                "actual_values": [],
                "sample_records": [],
            },
        )
        if result.status == CheckStatus.FAIL:
            entry["status"] = CheckStatus.FAIL
        entry["failed_count"] += int(result.failed_count)
        entry["actual_values"].append(result.actual)
        remaining = 20 - len(entry["sample_records"])
        if remaining > 0:
            entry["sample_records"].extend(result.samples[:remaining])
    return list(entries.values())


def sync_incremental_warning_state(
    conn,
    context: BatchContext,
    results: list[CheckResult],
) -> None:
    """Project certified incremental warning observations into OPEN/RESOLVED state.

    The immutable observation history remains in dq_result. A warning is only
    resolved by a PASS for the same mode, changed partition/date, dataset and
    rule. Missing rules do not implicitly resolve anything.
    """
    if context.mode not in INCREMENTAL_WARNING_MODES:
        return
    entries = _incremental_warning_entries(context, results)
    if not entries:
        return
    with conn.cursor() as cur:
        for entry in entries:
            actual_value = " | ".join(entry["actual_values"])
            identity = (
                context.mode,
                entry["scope_key"],
                entry["dataset_name"],
                entry["rule_code"],
            )
            if entry["status"] == CheckStatus.FAIL:
                cur.execute(
                    """
                    INSERT INTO dq_warning_state (
                        mode, scope_key, target_date, partition_key,
                        dataset_name, rule_code, status,
                        first_seen_run_id, last_failed_run_id,
                        last_evaluated_run_id, observation_count,
                        latest_failed_count, expected_value, actual_value,
                        sample_records
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,1,%s,%s,%s,%s::jsonb
                    )
                    ON CONFLICT (mode, scope_key, dataset_name, rule_code)
                    DO UPDATE SET
                        status='OPEN',
                        last_failed_run_id=EXCLUDED.last_failed_run_id,
                        last_evaluated_run_id=EXCLUDED.last_evaluated_run_id,
                        resolved_run_id=NULL,
                        last_failed_at=now(),
                        last_evaluated_at=now(),
                        resolved_at=NULL,
                        observation_count=dq_warning_state.observation_count + 1,
                        reopen_count=dq_warning_state.reopen_count +
                            CASE WHEN dq_warning_state.status='RESOLVED' THEN 1 ELSE 0 END,
                        latest_failed_count=EXCLUDED.latest_failed_count,
                        expected_value=EXCLUDED.expected_value,
                        actual_value=EXCLUDED.actual_value,
                        sample_records=EXCLUDED.sample_records
                    """,
                    (
                        context.mode,
                        entry["scope_key"],
                        context.target_date,
                        entry["partition_key"],
                        entry["dataset_name"],
                        entry["rule_code"],
                        context.run_id,
                        context.run_id,
                        context.run_id,
                        entry["failed_count"],
                        entry["expected_value"],
                        actual_value,
                        json.dumps(
                            _json_safe(entry["sample_records"]),
                            ensure_ascii=False,
                            default=str,
                            allow_nan=False,
                        ),
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE dq_warning_state
                    SET status='RESOLVED',
                        last_evaluated_run_id=%s,
                        resolved_run_id=%s,
                        last_evaluated_at=now(),
                        resolved_at=now(),
                        latest_failed_count=0,
                        expected_value=%s,
                        actual_value=%s,
                        sample_records=%s::jsonb
                    WHERE mode=%s AND scope_key=%s
                      AND dataset_name=%s AND rule_code=%s
                      AND status='OPEN'
                    """,
                    (
                        context.run_id,
                        context.run_id,
                        entry["expected_value"],
                        actual_value,
                        json.dumps(
                            _json_safe(entry["sample_records"]),
                            ensure_ascii=False,
                            default=str,
                            allow_nan=False,
                        ),
                        *identity,
                    ),
                )


def open_warning_counts(conn, mode: str | None = None) -> tuple[int, int]:
    where = "WHERE mode=%s" if mode else ""
    params = (mode,) if mode else ()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*), COALESCE(sum(latest_failed_count), 0)
            FROM dq_open_warning {where}
            """,
            params,
        )
        row = cur.fetchone()
    return int(row[0]), int(row[1])


def save_metrics(conn, run_id: UUID, bundle) -> None:
    frames = {
        "asset": bundle.assets,
        "asset_identifier": bundle.identifiers,
        "price_daily": bundle.prices,
        "fundamental": bundle.fundamentals,
        "corporate_action": bundle.actions,
    }
    with conn.cursor() as cur:
        def insert_metric(dataset: str, name: str, value, dimension=None):
            cur.execute(
                """
                INSERT INTO dq_metric(
                    run_id,dataset_name,metric_name,dimension,metric_value
                ) VALUES (%s,%s,%s,%s::jsonb,%s)
                """,
                (
                    run_id, dataset, name,
                    json.dumps(dimension or {}, ensure_ascii=False, default=str),
                    None if pd.isna(value) else float(value),
                ),
            )

        for dataset, frame in frames.items():
            insert_metric(dataset, "row_count", len(frame))
        if not bundle.prices.empty:
            p = bundle.prices
            insert_metric(
                "price_daily", "distinct_instrument_count",
                p["identifier"].nunique(),
            )
            insert_metric(
                "price_daily", "null_close_ratio",
                p["close"].isna().mean(),
            )
            duplicate_ratio = p.duplicated(
                ["identifier", "source", "trade_date"], keep=False,
            ).mean()
            insert_metric("price_daily", "duplicate_ratio", duplicate_ratio)
            close = pd.to_numeric(p["close"], errors="coerce")
            for quantile, name in ((0.01, "close_p01"), (0.5, "close_p50"), (0.99, "close_p99")):
                insert_metric("price_daily", name, close.quantile(quantile))
            ordered = p.sort_values(["identifier", "trade_date"]).copy()
            ordered["return"] = (
                ordered.groupby("identifier")["close"].pct_change(fill_method=None)
            )
            returns = ordered["return"].dropna()
            if not returns.empty:
                for quantile, name in (
                    (0.01, "return_p01"),
                    (0.5, "return_p50"),
                    (0.99, "return_p99"),
                ):
                    insert_metric("price_daily", name, returns.quantile(quantile))
            for market, count in bundle.prices.groupby("market", dropna=False).size().items():
                insert_metric(
                    "price_daily", "row_count_by_market", count,
                    {"market": None if pd.isna(market) else market},
                )
        if not bundle.fundamentals.empty:
            f = bundle.fundamentals
            insert_metric(
                "fundamental", "distinct_instrument_count",
                f["identifier"].nunique(),
            )
            insert_metric(
                "fundamental", "null_value_ratio",
                f["value"].isna().mean(),
            )
        corporate_actions = bundle.actions
        if (
            isinstance(corporate_actions, pd.DataFrame)
            and not corporate_actions.empty
        ):
            effective_column = (
                "effective_date"
                if "effective_date" in corporate_actions
                else "ex_date"
            )
            factor_column = (
                "expected_factor"
                if "expected_factor" in corporate_actions
                else "expected_price_factor"
            )
            insert_metric(
                "corporate_action",
                "row_count",
                len(corporate_actions),
            )
            insert_metric(
                "corporate_action",
                "effective_date_count",
                corporate_actions[effective_column].notna().sum(),
            )
            insert_metric(
                "corporate_action",
                "expected_factor_count",
                corporate_actions[factor_column].notna().sum(),
            )
            if "expects_price_adjustment" in corporate_actions:
                price_adjusting = corporate_actions[
                    "expects_price_adjustment"
                ].fillna(False).sum()
            else:
                price_adjusting = corporate_actions[factor_column].notna().sum()
            insert_metric(
                "corporate_action", "price_adjusting_event_count", price_adjusting,
            )


def save_price_partition_metrics(
    conn,
    run_id: UUID,
    *,
    row_count: int,
    instrument_count: int,
    year_row_counts: dict[int, int],
) -> None:
    """Persist bounded-memory price audit metrics without a full DataFrame."""
    rows = [
        ("row_count", {}, row_count),
        ("distinct_instrument_count", {}, instrument_count),
    ]
    rows.extend(
        ("row_count_by_year", {"year": year}, count)
        for year, count in sorted(year_row_counts.items())
    )
    with conn.cursor() as cur:
        for metric_name, dimension, value in rows:
            cur.execute(
                """
                INSERT INTO dq_metric(
                    run_id,dataset_name,metric_name,dimension,metric_value
                ) VALUES (%s,'price_daily',%s,%s::jsonb,%s)
                """,
                (
                    run_id,
                    metric_name,
                    json.dumps(dimension, ensure_ascii=False),
                    float(value),
                ),
            )


def finish_run(
    conn,
    context: BatchContext,
    status: str,
    results: list[CheckResult],
    *,
    error_message: str | None = None,
    commit: bool = True,
) -> None:
    save_results(conn, context.run_id, results)
    if status == "CERTIFIED":
        sync_incremental_warning_state(conn, context, results)
    failed = sum(1 for r in results if r.status.value == "FAIL")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dq_run
            SET status=%s, finished_at=now(), total_rule_count=%s,
                failed_rule_count=%s, error_message=%s
            WHERE run_id=%s
            """,
            (status, len(results), failed, error_message, context.run_id),
        )
    if commit:
        conn.commit()


def update_status(conn, run_id: UUID, status: str, *, commit: bool = True) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dq_run
            SET status=%s,
                finished_at=CASE WHEN %s IN ('RUNNING','BUILDING','VALIDATING')
                                 THEN NULL ELSE finished_at END,
                error_message=CASE WHEN %s IN ('RUNNING','BUILDING','VALIDATING')
                                   THEN NULL ELSE error_message END
            WHERE run_id=%s
            """,
            (status, status, status, run_id),
        )
    if commit:
        conn.commit()


def recent_price_history(conn, identifiers: list[str], before_date, days: int = 20) -> pd.DataFrame:
    if not identifiers:
        return pd.DataFrame(columns=[
            "identifier", "trade_date", "close", "adj_close", "market",
            "asset_type", "shares", "market_cap",
        ])
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT ai.identifier, p.trade_date, p.close, p.adj_close,
                       p.market, a.asset_type, p.shares, p.market_cap,
                       row_number() OVER (
                           PARTITION BY ai.identifier ORDER BY p.trade_date DESC
                       ) AS rn
                FROM price_daily p
                JOIN asset a ON a.asset_id=p.asset_id
                JOIN asset_identifier ai
                  ON ai.asset_id=p.asset_id AND ai.source='KRX'
                WHERE p.source='KRX' AND p.trade_date < %s
                  AND ai.identifier = ANY(%s)
            )
            SELECT identifier, trade_date, close, adj_close, market, asset_type,
                   shares, market_cap
            FROM ranked WHERE rn <= %s
            """,
            (before_date, identifiers, days),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "identifier", "trade_date", "close", "adj_close", "market", "asset_type",
        "shares", "market_cap",
    ])


def existing_krx_identifiers(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier FROM asset_identifier WHERE source='KRX'"
        )
        return {str(row[0]) for row in cur.fetchall()}
