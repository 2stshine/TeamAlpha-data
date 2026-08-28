"""Bounded, fail-closed KRX gross-total-return rebuild.

The command is deliberately a dry run unless ``--apply`` is supplied.  It
only reads certified KRX stock prices and certified issuer-scope DART actions,
reconstructs each asset in bounded batches, and promotes the return contract
only after every batch has passed the same transaction-wide validation.

Examples
--------
Preview without persistent writes::

    uv run python -m pipeline.silver.total_return_rebuild

Preview local complete Bronze actions against certified RDS prices without
publishing actions, DQ state, or the return contract::

    uv run python -m pipeline.silver.total_return_rebuild \
        --actions-base /complete/dart/snapshot

Apply migration 008 first, then explicitly publish the certified rebuild::

    uv run python -m pipeline.silver_quality.migrate
    uv run python -m pipeline.silver.dart_extra_load --base /complete/dart/snapshot
    uv run python -m pipeline.silver.total_return_rebuild --apply

For the apply path, the complete DART snapshot re-upsert is a prerequisite:
it populates ``action_scope`` for old rows.  The runner selects the latest
certified ``dart_dividend_action_backfill`` run as one immutable action
snapshot; it never mixes pre-migration UNKNOWN-scope history into that
snapshot.  ``--actions-base`` is preview-only and never performs that upsert.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from uuid import UUID

import numpy as np
import pandas as pd

from pipeline.common import db
from pipeline.silver import corporate_actions
from pipeline.silver.total_returns import (
    apply_dividends_to_prices,
    classify_cash_dividend_revisions,
    resolve_dividend_ex_dates,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)


METHODOLOGY_VERSION = "krx_gross_dividend_reinvested_v1"
RESOLUTION_VERSION = "krx_dividend_resolution_v1"
DIVIDEND_TREATMENT = "gross_cash_dividend_reinvested_on_ex_date"

_PRICE_STAGE = "_stg_krx_total_return_rebuild"
_AUDIT_STAGE = "_stg_dividend_event_resolution"

_BLOCKING_APPLICATION_STATUSES = frozenset({
    "unresolved_ex_date",
    "no_price_series",
    "invalid_cash_amount",
})
_EXPLICIT_EXCLUSIONS = {
    "before_market_coverage": "BEFORE_MARKET_COVERAGE",
    "pending_future_trade": "PENDING_FUTURE_TRADE",
    "before_listing_or_episode_start": "BEFORE_LISTING_OR_EPISODE_START",
    "listing_episode_gap": "LISTING_EPISODE_GAP",
}

_AUDIT_COLUMNS = [
    "asset_id",
    "source",
    "action_key",
    "resolution_version",
    "is_canonical",
    "excluded_reason",
    "resolved_ex_date",
    "ex_date_basis",
    "applied_trade_date",
    "raw_cash_amount",
    "adjusted_cash_amount",
    "quality_run_id",
]


@dataclass
class RebuildSummary:
    """Compact audit summary returned by dry-run and apply modes."""

    apply: bool
    asset_count: int = 0
    price_row_count: int = 0
    cash_action_count: int = 0
    canonical_event_count: int = 0
    applied_event_count: int = 0
    excluded_event_count: int = 0
    coverage_start: str | None = None
    coverage_end: str | None = None
    run_id: str | None = None
    action_snapshot_run_id: str | None = None
    action_source: str = "rds_certified_snapshot"
    local_actions_base: str | None = None
    local_actions_fingerprint: str | None = None
    unmapped_action_count: int = 0

    def absorb(self, batch: "BatchRebuild") -> None:
        self.asset_count += int(batch.prices["asset_id"].nunique())
        self.price_row_count += len(batch.prices)
        self.cash_action_count += len(batch.audit)
        self.canonical_event_count += batch.canonical_event_count
        self.applied_event_count += batch.applied_event_count
        self.excluded_event_count += batch.excluded_event_count
        if not batch.prices.empty:
            start = batch.prices["trade_date"].min().date().isoformat()
            end = batch.prices["trade_date"].max().date().isoformat()
            self.coverage_start = min(
                value for value in (self.coverage_start, start) if value
            )
            self.coverage_end = max(
                value for value in (self.coverage_end, end) if value
            )


@dataclass
class BatchRebuild:
    prices: pd.DataFrame
    audit: pd.DataFrame
    canonical_event_count: int
    applied_event_count: int
    excluded_event_count: int


@dataclass(frozen=True)
class LocalActionSnapshot:
    actions: pd.DataFrame
    base: str
    fingerprint: str
    unmapped_count: int


def _chunks(values: Iterable[int], size: int) -> Iterator[list[int]]:
    if size <= 0:
        raise ValueError("batch_size must be positive")
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def _assert_contract_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('public.dividend_event_resolution'),
                   to_regclass('public.price_return_contract')
            """
        )
        row = cur.fetchone()
    if not row or any(value is None for value in row):
        raise RuntimeError(
            "KRX total-return schema가 없습니다. "
            "migration 008_krx_total_return.sql을 먼저 적용하세요."
        )


def _certified_asset_ids(conn) -> list[int]:
    """Return only assets with at least one certified KRX stock-price row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.asset_id
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.exchange='KRX'
              AND q.status='CERTIFIED'
            ORDER BY p.asset_id
            """
        )
        return [int(row[0]) for row in cur.fetchall()]


def _certified_action_snapshot_run(conn) -> UUID:
    """Resolve the latest complete, certified DART snapshot re-upsert."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM dq_run
            WHERE mode='dart_dividend_action_backfill'
              AND status='CERTIFIED'
            ORDER BY finished_at DESC NULLS LAST, started_at DESC, run_id DESC
            LIMIT 1
            """
        )
        snapshot = cur.fetchone()
    if not snapshot:
        raise RuntimeError(
            "certified dart_dividend_action_backfill이 없습니다. complete "
            "snapshot을 pipeline.silver.dart_extra_load로 재업서트하세요."
        )
    snapshot_run_id = snapshot[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE ca.action_scope IS NULL),
                   count(*) FILTER (
                       WHERE ca.action_scope='ISSUER'
                         AND ca.action_type='cash_dividend'
                   )
            FROM corporate_action ca
            WHERE ca.quality_run_id=%s
              AND ca.source='DART_DISCLOSURE'
              AND ca.action_type IN ('cash_dividend', 'ex_dividend')
            """,
            (snapshot_run_id,),
        )
        null_scope_count, issuer_cash_count = cur.fetchone()
    if int(null_scope_count) > 0:
        raise RuntimeError(
            "latest certified DART snapshot에 NULL action_scope가 "
            f"{null_scope_count}건 있습니다"
        )
    if int(issuer_cash_count) == 0:
        raise RuntimeError(
            "latest certified DART snapshot에 ISSUER cash-dividend가 없습니다"
        )
    return snapshot_run_id


def _local_bronze_fingerprint(base: str) -> tuple[str, str]:
    """Fingerprint the local DART evidence set without touching RDS."""
    root = Path(base).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"actions-base is not a directory: {root}")
    paths: set[Path] = set()
    for pattern in (
        "corporate_actions/dart/**/*.json",
        "corporate_actions/dart/**/*.zip",
    ):
        paths.update(path for path in root.glob(pattern) if path.is_file())
    if not paths:
        raise ValueError(
            f"actions-base has no DART corporate-action evidence: {root}"
        )
    digest = hashlib.sha256()
    for path in sorted(paths):
        stat = path.stat()
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(f"{stat.st_size}:{stat.st_mtime_ns}".encode("ascii"))
    return str(root), digest.hexdigest()


def _current_krx_asset_map(
    conn,
    identifiers: Sequence[str],
) -> dict[str, int]:
    if not identifiers:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier, asset_id
            FROM asset_identifier
            WHERE source='KRX'
              AND identifier_type='ticker'
              AND valid_to IS NULL
              AND identifier = ANY(%s)
            """,
            (list(identifiers),),
        )
        return {
            str(identifier): int(asset_id)
            for identifier, asset_id in cur.fetchall()
        }


def _prepare_local_action_snapshot(
    conn,
    base: str,
) -> LocalActionSnapshot:
    """Prepare a complete local Bronze action snapshot for read-only preview."""
    resolved_base, fingerprint = _local_bronze_fingerprint(base)
    candidates, _ = corporate_actions.prepare(resolved_base)
    required = {"identifier", "source", "event_type", "action_scope"}
    missing = required - set(candidates.columns)
    if missing:
        raise RuntimeError(
            "local corporate-action candidates missing columns: "
            f"{sorted(missing)}"
        )
    scoped = candidates[
        candidates["source"].eq("DART_DISCLOSURE")
        & candidates["action_scope"].eq("ISSUER")
        & candidates["event_type"].isin(("cash_dividend", "ex_dividend"))
    ].copy()
    if scoped.empty or not scoped["event_type"].eq("cash_dividend").any():
        raise RuntimeError(
            "local complete Bronze has no ISSUER DART cash-dividend"
        )

    normalized = corporate_actions.normalize_for_publish(scoped)
    normalized["identifier"] = normalized["identifier"].astype(str)
    identifier_map = _current_krx_asset_map(
        conn,
        sorted(normalized["identifier"].unique()),
    )
    normalized["asset_id"] = normalized["identifier"].map(identifier_map)
    unmapped_count = int(normalized["asset_id"].isna().sum())
    mapped = normalized[normalized["asset_id"].notna()].copy()
    if mapped.empty or not mapped["action_type"].eq("cash_dividend").any():
        raise RuntimeError(
            "local ISSUER cash-dividend does not map to any current KRX asset"
        )
    mapped["asset_id"] = mapped["asset_id"].astype("int64")
    # total_returns groups on `identifier`; asset_id is stable across ticker
    # history and matches the RDS price query's grouping key.
    mapped["identifier"] = mapped["asset_id"].astype(str)
    mapped = mapped.rename(columns={
        "action_type": "event_type",
        "ex_date": "effective_date",
    })
    actions = mapped[[
        "asset_id",
        "identifier",
        "source",
        "action_key",
        "event_type",
        "announcement_date",
        "effective_date",
        "record_date",
        "cash_amount",
        "filing_id",
    ]].sort_values(
        ["asset_id", "announcement_date", "action_key"],
        kind="mergesort",
    ).reset_index(drop=True)
    return LocalActionSnapshot(
        actions=actions,
        base=resolved_base,
        fingerprint=fingerprint,
        unmapped_count=unmapped_count,
    )


def _global_krx_sessions(conn) -> pd.Series:
    """Load one global KRX session calendar, never an asset-local calendar."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.trade_date
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.exchange='KRX'
              AND q.status='CERTIFIED'
            ORDER BY p.trade_date
            """
        )
        rows = cur.fetchall()
    sessions = pd.Series([row[0] for row in rows], dtype="datetime64[ns]")
    if sessions.empty:
        raise RuntimeError("certified KRX stock session이 없습니다")
    return sessions


def _certified_prices(conn, asset_ids: Sequence[int]) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.asset_id, p.asset_id::text AS identifier,
                   p.trade_date, p.close, p.adj_close
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.asset_id = ANY(%s)
              AND p.source='KRX'
              AND a.asset_type='stock'
              AND a.exchange='KRX'
              AND q.status='CERTIFIED'
            ORDER BY p.asset_id, p.trade_date
            """,
            (list(asset_ids),),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "asset_id", "identifier", "trade_date", "close", "adj_close",
    ])


def _issuer_dart_actions(
    conn,
    asset_ids: Sequence[int],
    action_snapshot_run_id: UUID,
) -> pd.DataFrame:
    """Read no inherited/related-company or uncertified action evidence."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ca.asset_id, ca.asset_id::text AS identifier,
                   ca.source, ca.action_key,
                   ca.action_type AS event_type,
                   ca.announcement_date, ca.ex_date AS effective_date,
                   ca.record_date, ca.cash_amount, ca.filing_id
            FROM corporate_action ca
            JOIN asset a ON a.asset_id=ca.asset_id
            JOIN dq_run q ON q.run_id=ca.quality_run_id
            WHERE ca.asset_id = ANY(%s)
              AND ca.source='DART_DISCLOSURE'
              AND ca.action_scope='ISSUER'
              AND ca.action_type IN ('cash_dividend', 'ex_dividend')
              AND a.asset_type='stock'
              AND a.exchange='KRX'
              AND q.status='CERTIFIED'
              AND ca.quality_run_id=%s
            ORDER BY ca.asset_id, ca.announcement_date, ca.action_key
            """,
            (list(asset_ids), action_snapshot_run_id),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "asset_id",
        "identifier",
        "source",
        "action_key",
        "event_type",
        "announcement_date",
        "effective_date",
        "record_date",
        "cash_amount",
        "filing_id",
    ])


def _audit_frame(
    classified: pd.DataFrame,
    resolved_events: pd.DataFrame,
    *,
    run_id: UUID | None,
) -> pd.DataFrame:
    """Map every cash source row to an applied event or explicit exclusion."""
    resolved_by_key: dict[tuple[str, str, str], pd.Series] = {}
    for _, event in resolved_events.iterrows():
        key = (
            str(event["identifier"]),
            str(event["source"]),
            str(event["dividend_key"]),
        )
        resolved_by_key[key] = event

    records: list[dict] = []
    for _, action in classified.iterrows():
        key = (
            str(action["identifier"]),
            str(action["source"]),
            str(action["dividend_key"]),
        )
        source_canonical = bool(action["is_canonical"])
        event = resolved_by_key.get(key) if source_canonical else None
        excluded_reason = action["excluded_reason"]
        audit_canonical = source_canonical

        if source_canonical:
            if event is None:
                raise RuntimeError(
                    f"canonical dividend has no resolution: {key}"
                )
            application_status = str(event["application_status"])
            if application_status in _BLOCKING_APPLICATION_STATUSES:
                raise RuntimeError(
                    "unresolved canonical dividend "
                    f"{key}: {application_status}"
                )
            if application_status == "applied":
                excluded_reason = None
            elif application_status in _EXPLICIT_EXCLUSIONS:
                audit_canonical = False
                excluded_reason = _EXPLICIT_EXCLUSIONS[application_status]
            else:
                raise RuntimeError(
                    f"unknown dividend application status {application_status!r}"
                )

        records.append({
            "asset_id": int(action["asset_id"]),
            "source": str(action["source"]),
            "action_key": str(action["dividend_key"]),
            "resolution_version": RESOLUTION_VERSION,
            "is_canonical": audit_canonical,
            "excluded_reason": excluded_reason,
            "resolved_ex_date": (
                event["resolved_ex_date"] if event is not None else None
            ),
            "ex_date_basis": (
                event["ex_date_basis"] if event is not None else None
            ),
            "applied_trade_date": (
                event["applied_trade_date"] if event is not None else None
            ),
            "raw_cash_amount": action["cash_amount"],
            "adjusted_cash_amount": (
                event["adjusted_cash_amount"] if event is not None else None
            ),
            "quality_run_id": run_id,
        })
    return pd.DataFrame(records, columns=_AUDIT_COLUMNS)


def _assert_dividend_yields(
    rebuilt: pd.DataFrame,
    events: pd.DataFrame,
    *,
    max_dividend_yield: float,
) -> None:
    """Reject suspicious parsed amounts instead of silently compounding them."""
    if not 0 < max_dividend_yield <= 1:
        raise ValueError("max_dividend_yield must be in (0, 1]")
    applied = events[events["application_status"].eq("applied")].copy()
    if applied.empty:
        return
    ordered = rebuilt.sort_values(["identifier", "trade_date"]).copy()
    ordered["previous_adj_close"] = ordered.groupby(
        "identifier", sort=False,
    )["adj_close"].shift(1)
    reference = ordered[[
        "identifier", "trade_date", "previous_adj_close",
    ]].rename(columns={"trade_date": "applied_trade_date"})
    applied = applied.merge(
        reference,
        on=["identifier", "applied_trade_date"],
        how="left",
        validate="many_to_one",
    )
    applied["cash_yield"] = (
        pd.to_numeric(applied["adjusted_cash_amount"], errors="coerce")
        / pd.to_numeric(applied["previous_adj_close"], errors="coerce")
    )
    invalid = applied[
        applied["cash_yield"].isna()
        | ~np.isfinite(applied["cash_yield"])
        | applied["cash_yield"].le(0)
        | applied["cash_yield"].gt(max_dividend_yield)
    ]
    if not invalid.empty:
        sample = invalid[[
            "identifier", "dividend_key", "cash_yield",
        ]].head(5).to_dict("records")
        raise RuntimeError(
            "dividend cash yield is outside the fail-closed bound "
            f"(0, {max_dividend_yield}]: {sample}"
        )


def _build_batch(
    prices: pd.DataFrame,
    actions: pd.DataFrame,
    sessions: pd.Series,
    *,
    run_id: UUID | None,
    max_dividend_yield: float,
) -> BatchRebuild:
    if prices.empty:
        raise RuntimeError("asset batch has no certified KRX stock prices")
    expected_keys = prices[["asset_id", "trade_date"]].copy()
    expected_keys["trade_date"] = pd.to_datetime(
        expected_keys["trade_date"], errors="coerce",
    ).dt.normalize()
    if expected_keys.duplicated().any():
        raise RuntimeError("duplicate certified KRX asset/date price rows")

    classified = classify_cash_dividend_revisions(actions)
    canonical = classified[classified["is_canonical"]].copy().reset_index(
        drop=True,
    )
    market_coverage_start = pd.to_datetime(sessions, errors="coerce").min()
    market_coverage_end = pd.to_datetime(sessions, errors="coerce").max()
    resolved = resolve_dividend_ex_dates(canonical, actions, sessions)
    if not resolved.empty:
        record_dates = pd.to_datetime(resolved["record_date"], errors="coerce")
        resolved["_runner_resolution_status"] = None
        before_coverage = record_dates.lt(market_coverage_start)
        future_inference = (
            resolved["ex_date_basis"].eq("KRX_T2_INFERRED")
            & record_dates.gt(market_coverage_end)
        )
        resolved.loc[
            before_coverage, "_runner_resolution_status",
        ] = "before_market_coverage"
        resolved.loc[
            future_inference, "_runner_resolution_status",
        ] = "pending_future_trade"
        unresolved_by_boundary = before_coverage | future_inference
        resolved.loc[unresolved_by_boundary, "resolved_ex_date"] = pd.NaT
        resolved.loc[unresolved_by_boundary, "ex_date_basis"] = None
    rebuilt, events = apply_dividends_to_prices(prices, resolved)
    if not events.empty and "_runner_resolution_status" in events:
        boundary_status = events["_runner_resolution_status"].notna()
        events.loc[boundary_status, "application_status"] = events.loc[
            boundary_status, "_runner_resolution_status"
        ]
    if len(rebuilt) != len(prices):
        raise RuntimeError("total-return rebuild changed price row count")
    actual_keys = rebuilt[["asset_id", "trade_date"]]
    if set(map(tuple, actual_keys.to_numpy())) != set(
        map(tuple, expected_keys.to_numpy())
    ):
        raise RuntimeError("total-return rebuild changed price keys")

    total_return = pd.to_numeric(
        rebuilt["total_return_close"], errors="coerce",
    )
    if (
        total_return.isna().any()
        or (~np.isfinite(total_return)).any()
        or total_return.le(0).any()
    ):
        raise RuntimeError("rebuilt total_return_close must be finite and positive")
    _assert_dividend_yields(
        rebuilt,
        events,
        max_dividend_yield=max_dividend_yield,
    )
    audit = _audit_frame(classified, events, run_id=run_id)
    if len(audit) != len(classified):
        raise RuntimeError("not every DART cash action received an audit decision")

    output = rebuilt[[
        "asset_id", "trade_date", "total_return_close",
    ]].copy()
    output["quality_run_id"] = run_id
    return BatchRebuild(
        prices=output,
        audit=audit,
        canonical_event_count=int(classified["is_canonical"].sum()),
        applied_event_count=int(
            events["application_status"].eq("applied").sum()
        ),
        excluded_event_count=int((~audit["is_canonical"]).sum()),
    )


def _create_temp_stages(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TEMP TABLE {_PRICE_STAGE} (
                asset_id BIGINT NOT NULL,
                trade_date DATE NOT NULL,
                total_return_close NUMERIC(28,8) NOT NULL,
                quality_run_id UUID NOT NULL,
                PRIMARY KEY(asset_id, trade_date)
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            f"""
            CREATE TEMP TABLE {_AUDIT_STAGE}
            (LIKE dividend_event_resolution INCLUDING DEFAULTS)
            ON COMMIT DROP
            """
        )


def _publish_batch(conn, batch: BatchRebuild) -> tuple[int, int]:
    """COPY one bounded batch and atomically update price plus event audit."""
    price_rows = list(
        batch.prices.astype(object).where(
            pd.notna(batch.prices), None,
        ).itertuples(index=False, name=None)
    )
    audit_rows = list(
        batch.audit.astype(object).where(
            pd.notna(batch.audit), None,
        ).itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_PRICE_STAGE}")
        with cur.copy(
            f"COPY {_PRICE_STAGE} "
            "(asset_id,trade_date,total_return_close,quality_run_id) FROM STDIN"
        ) as copy:
            for row in price_rows:
                copy.write_row(row)
        cur.execute(
            f"""
            UPDATE price_daily p
            SET total_return_close=s.total_return_close,
                quality_run_id=s.quality_run_id,
                loaded_at=now()
            FROM {_PRICE_STAGE} s
            WHERE p.asset_id=s.asset_id
              AND p.trade_date=s.trade_date
              AND p.source='KRX'
              AND EXISTS (
                  SELECT 1 FROM dq_run q
                  WHERE q.run_id=p.quality_run_id
                    AND q.status='CERTIFIED'
              )
            """
        )
        updated = int(cur.rowcount)
        if updated != len(price_rows):
            raise RuntimeError(
                "certified price changed during rebuild: "
                f"expected={len(price_rows)} updated={updated}"
            )

        cur.execute(f"TRUNCATE {_AUDIT_STAGE}")
        if audit_rows:
            columns = ",".join(_AUDIT_COLUMNS)
            with cur.copy(
                f"COPY {_AUDIT_STAGE} ({columns}) FROM STDIN"
            ) as copy:
                for row in audit_rows:
                    copy.write_row(row)
            cur.execute(
                f"""
                INSERT INTO dividend_event_resolution ({columns})
                SELECT {columns} FROM {_AUDIT_STAGE}
                ON CONFLICT (
                    asset_id,source,action_key,resolution_version
                ) DO UPDATE SET
                    is_canonical=EXCLUDED.is_canonical,
                    excluded_reason=EXCLUDED.excluded_reason,
                    resolved_ex_date=EXCLUDED.resolved_ex_date,
                    ex_date_basis=EXCLUDED.ex_date_basis,
                    applied_trade_date=EXCLUDED.applied_trade_date,
                    raw_cash_amount=EXCLUDED.raw_cash_amount,
                    adjusted_cash_amount=EXCLUDED.adjusted_cash_amount,
                    quality_run_id=EXCLUDED.quality_run_id,
                    resolved_at=now()
                """
            )
            audited = int(cur.rowcount)
            if audited != len(audit_rows):
                raise RuntimeError(
                    "dividend audit upsert row parity failed: "
                    f"expected={len(audit_rows)} actual={audited}"
                )
        else:
            audited = 0
    return updated, audited


def _pass_results(summary: RebuildSummary) -> list[CheckResult]:
    return [
        CheckResult(
            rule_code="KRX_TOTAL_RETURN_INPUT_SCOPE",
            dataset="price_daily",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="certified KRX stocks and certified ISSUER DART only",
            actual=(
                f"assets={summary.asset_count}, "
                f"cash_actions={summary.cash_action_count}"
            ),
        ),
        CheckResult(
            rule_code="KRX_TOTAL_RETURN_ROW_PARITY",
            dataset="price_daily",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="every input price key updated exactly once",
            actual=f"rows={summary.price_row_count}",
        ),
        CheckResult(
            rule_code="KRX_DIVIDEND_EVENT_RESOLUTION",
            dataset="dividend_event_resolution",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="every cash action applied or explicitly excluded",
            actual=(
                f"canonical={summary.canonical_event_count}, "
                f"applied={summary.applied_event_count}, "
                f"excluded={summary.excluded_event_count}"
            ),
        ),
    ]


def _mark_contract_building(conn, run_id: UUID) -> None:
    """Invalidate any older certification before an apply run starts.

    This small state change is committed before the long rebuild transaction.
    A crash or failed batch therefore leaves research fail-closed at BUILDING,
    while all price/audit changes from the failed run are rolled back.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_return_contract (
                source,asset_type,field_name,methodology_version,
                dividend_treatment,status,quality_run_id,metadata,
                certified_at,updated_at
            ) VALUES (
                'KRX','stock','total_return_close',%s,%s,'BUILDING',%s,
                '{"reason":"certified rebuild in progress"}'::jsonb,
                NULL,now()
            )
            ON CONFLICT (source,asset_type,field_name) DO UPDATE SET
                methodology_version=EXCLUDED.methodology_version,
                dividend_treatment=EXCLUDED.dividend_treatment,
                status='BUILDING',
                coverage_start=NULL,
                coverage_end=NULL,
                quality_run_id=EXCLUDED.quality_run_id,
                metadata=EXCLUDED.metadata,
                certified_at=NULL,
                updated_at=now()
            """,
            (METHODOLOGY_VERSION, DIVIDEND_TREATMENT, run_id),
        )
    conn.commit()


def _certify_contract(
    conn,
    summary: RebuildSummary,
    run_id: UUID,
) -> None:
    metadata = {
        "asset_count": summary.asset_count,
        "price_row_count": summary.price_row_count,
        "cash_action_count": summary.cash_action_count,
        "canonical_event_count": summary.canonical_event_count,
        "applied_event_count": summary.applied_event_count,
        "excluded_event_count": summary.excluded_event_count,
        "resolution_version": RESOLUTION_VERSION,
        "action_snapshot_run_id": summary.action_snapshot_run_id,
        "input_scope": {
            "prices": "CERTIFIED KRX stock",
            "actions": "CERTIFIED DART_DISCLOSURE ISSUER",
        },
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*), count(DISTINCT p.asset_id),
                   min(p.trade_date), max(p.trade_date)
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.exchange='KRX'
              AND p.quality_run_id=%s
            """,
            (run_id,),
        )
        row_count, asset_count, coverage_start, coverage_end = cur.fetchone()
        if int(row_count) != summary.price_row_count:
            raise RuntimeError(
                "final price-row parity failed: "
                f"expected={summary.price_row_count} actual={row_count}"
            )
        if int(asset_count) != summary.asset_count:
            raise RuntimeError(
                "final asset parity failed: "
                f"expected={summary.asset_count} actual={asset_count}"
            )
        if (
            coverage_start.isoformat() != summary.coverage_start
            or coverage_end.isoformat() != summary.coverage_end
        ):
            raise RuntimeError("final total-return coverage bounds changed")

        # This is intentionally the last data operation.  No batch can make
        # the contract visible as CERTIFIED before this point.
        cur.execute(
            """
            INSERT INTO price_return_contract (
                source,asset_type,field_name,methodology_version,
                dividend_treatment,status,coverage_start,coverage_end,
                quality_run_id,metadata,certified_at,updated_at
            ) VALUES (
                'KRX','stock','total_return_close',%s,%s,'CERTIFIED',
                %s,%s,%s,%s::jsonb,clock_timestamp(),clock_timestamp()
            )
            ON CONFLICT (source,asset_type,field_name) DO UPDATE SET
                methodology_version=EXCLUDED.methodology_version,
                dividend_treatment=EXCLUDED.dividend_treatment,
                status='CERTIFIED',
                coverage_start=EXCLUDED.coverage_start,
                coverage_end=EXCLUDED.coverage_end,
                quality_run_id=EXCLUDED.quality_run_id,
                metadata=EXCLUDED.metadata,
                certified_at=clock_timestamp(),
                updated_at=clock_timestamp()
            """,
            (
                METHODOLOGY_VERSION,
                DIVIDEND_TREATMENT,
                coverage_start,
                coverage_end,
                run_id,
                json.dumps(metadata, ensure_ascii=False),
            ),
        )


def _rebuild(
    conn,
    *,
    apply: bool,
    batch_size: int,
    max_dividend_yield: float,
    run_id: UUID | None,
    actions_base: str | None = None,
) -> RebuildSummary:
    if apply and actions_base is not None:
        raise ValueError("local actions cannot be used by apply rebuilds")
    local_snapshot = (
        _prepare_local_action_snapshot(conn, actions_base)
        if actions_base is not None
        else None
    )
    action_snapshot_run_id = (
        None
        if local_snapshot is not None
        else _certified_action_snapshot_run(conn)
    )
    asset_ids = _certified_asset_ids(conn)
    if not asset_ids:
        raise RuntimeError("certified KRX stock prices가 없습니다")
    sessions = _global_krx_sessions(conn)
    summary = RebuildSummary(
        apply=apply,
        run_id=str(run_id) if run_id is not None else None,
        action_snapshot_run_id=(
            str(action_snapshot_run_id)
            if action_snapshot_run_id is not None
            else None
        ),
        action_source=(
            "local_complete_bronze"
            if local_snapshot is not None
            else "rds_certified_snapshot"
        ),
        local_actions_base=(
            local_snapshot.base if local_snapshot is not None else None
        ),
        local_actions_fingerprint=(
            local_snapshot.fingerprint
            if local_snapshot is not None
            else None
        ),
        unmapped_action_count=(
            local_snapshot.unmapped_count
            if local_snapshot is not None
            else 0
        ),
    )
    if apply:
        _create_temp_stages(conn)

    for batch_number, asset_batch in enumerate(
        _chunks(asset_ids, batch_size), start=1,
    ):
        prices = _certified_prices(conn, asset_batch)
        if local_snapshot is not None:
            actions = local_snapshot.actions[
                local_snapshot.actions["asset_id"].isin(asset_batch)
            ].copy()
        else:
            actions = _issuer_dart_actions(
                conn,
                asset_batch,
                action_snapshot_run_id,
            )
        batch = _build_batch(
            prices,
            actions,
            sessions,
            run_id=run_id,
            max_dividend_yield=max_dividend_yield,
        )
        if apply:
            _publish_batch(conn, batch)
        summary.absorb(batch)
        print(
            "[total-return] "
            f"batch={batch_number} assets={len(asset_batch)} "
            f"prices={len(batch.prices)} actions={len(batch.audit)}",
            flush=True,
        )

    if summary.cash_action_count == 0:
        raise RuntimeError(
            "selected action source의 mapped ISSUER cash-dividend가 0건입니다; "
            "계약을 가격수익률로 잘못 인증하지 않습니다"
        )
    return summary


def run(
    *,
    apply: bool = False,
    batch_size: int = 100,
    max_dividend_yield: float = 1.0,
    actions_base: str | None = None,
    conn=None,
) -> RebuildSummary:
    """Run a read-only preview or an explicit transaction-wide rebuild."""
    if apply and actions_base is not None:
        raise ValueError(
            "--actions-base is read-only preview evidence and cannot be "
            "combined with --apply"
        )
    owns_connection = conn is None
    connection = conn or db.connect()
    context = None
    try:
        if not apply:
            # Put every dry-run query, including schema preflight, under a
            # database-enforced read-only transaction.  Local action preview
            # therefore cannot mutate RDS even if a future helper regresses.
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, "
                        "READ ONLY"
                    )
                repository.assert_schema(connection)
                _assert_contract_schema(connection)
                summary = _rebuild(
                    connection,
                    apply=False,
                    batch_size=batch_size,
                    max_dividend_yield=max_dividend_yield,
                    run_id=None,
                    actions_base=actions_base,
                )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
            return summary

        repository.assert_schema(connection)
        _assert_contract_schema(connection)
        connection.commit()
        context = repository.start_run(
            connection,
            mode="krx_total_return_rebuild",
            status="RUNNING",
            partition_key=METHODOLOGY_VERSION,
        )
        try:
            _mark_contract_building(connection, context.run_id)
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                    )
                summary = _rebuild(
                    connection,
                    apply=True,
                    batch_size=batch_size,
                    max_dividend_yield=max_dividend_yield,
                    run_id=context.run_id,
                )
                _certify_contract(connection, summary, context.run_id)
                repository.finish_run(
                    connection,
                    context,
                    "CERTIFIED",
                    _pass_results(summary),
                    commit=False,
                )
        except Exception as exc:
            connection.rollback()
            failure = CheckResult(
                rule_code="KRX_TOTAL_RETURN_REBUILD",
                dataset="price_daily",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic certified KRX gross-total-return rebuild",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                connection,
                context,
                "FAILED",
                [failure],
                error_message=str(exc),
            )
            raise
        print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
        return summary
    finally:
        if owns_connection:
            connection.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument(
        "--apply",
        action="store_true",
        help="검증된 전체 rebuild를 원자적으로 Silver에 반영",
    )
    write_mode.add_argument(
        "--actions-base",
        help=(
            "로컬 complete Bronze root의 DART actions로 read-only preview; "
            "--apply와 함께 사용할 수 없음"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="메모리에 올릴 asset 수 (기본 100)",
    )
    parser.add_argument(
        "--max-dividend-yield",
        type=float,
        default=1.0,
        help="이 값을 초과한 개별 현금배당/직전 조정종가 비율은 차단",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    run(
        apply=args.apply,
        batch_size=args.batch_size,
        max_dividend_yield=args.max_dividend_yield,
        actions_base=args.actions_base,
    )


if __name__ == "__main__":
    main()
