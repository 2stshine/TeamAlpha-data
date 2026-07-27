"""Bounded-memory full-series adjusted-close reconciliation.

The first pass derives the final cumulative KRX reference factor for every
listing episode. The second pass independently recomputes every expected
adjusted close and compares it with the published Silver row one year at a
time. No full-period price DataFrame is materialized.
"""
from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator

import pandas as pd

from pipeline.silver import prices
from pipeline.common import db
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)


@dataclass
class _SeriesState:
    last_date: date
    last_close: float
    episode: int


def _annual_frames(base: str) -> Iterator[tuple[int, pd.DataFrame]]:
    for year in prices.available_years(base):
        frame, _ = prices.prepare(
            base,
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31),
        )
        yield year, frame.sort_values(
            ["identifier", "trade_date"],
        ).reset_index(drop=True)


def _factor_for_row(
    trade_date: date,
    close: float,
    prev_diff: float | None,
    previous: _SeriesState | None,
) -> tuple[int, float]:
    if (
        previous is None
        or (trade_date - previous.last_date).days
        > prices.LISTING_EPISODE_GAP_DAYS
    ):
        episode = 0 if previous is None else previous.episode + 1
        return episode, 1.0
    reference = close - (0.0 if pd.isna(prev_diff) else float(prev_diff))
    if previous.last_close <= 0 or reference <= 0:
        return previous.episode, 1.0
    factor = reference / previous.last_close
    if abs(factor - 1.0) < 1e-9:
        factor = 1.0
    return previous.episode, factor


def _episode_totals(base: str) -> dict[tuple[str, int], float]:
    state: dict[str, _SeriesState] = {}
    totals: dict[tuple[str, int], float] = {}
    for _, frame in _annual_frames(base):
        for row in frame[
            ["identifier", "trade_date", "close", "prev_diff"]
        ].itertuples(index=False):
            identifier = str(row.identifier)
            close = float(row.close)
            previous = state.get(identifier)
            episode, factor = _factor_for_row(
                row.trade_date,
                close,
                row.prev_diff,
                previous,
            )
            key = (identifier, episode)
            totals[key] = totals.get(key, 1.0) * factor
            state[identifier] = _SeriesState(
                row.trade_date,
                close,
                episode,
            )
    return totals


def _published_year(conn, year: int) -> dict[tuple[str, date], tuple[float, float]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ai.identifier, p.trade_date, p.close, p.adj_close
            FROM price_daily p
            JOIN asset_identifier ai
              ON ai.asset_id=p.asset_id AND ai.source='KRX'
            WHERE p.source='KRX'
              AND p.trade_date >= %s
              AND p.trade_date < %s
            """,
            (date(year, 1, 1), date(year + 1, 1, 1)),
        )
        return {
            (str(identifier), trade_date): (
                float(close),
                float(adj_close),
            )
            for identifier, trade_date, close, adj_close in cur.fetchall()
        }


def check_published_adj_close(
    conn,
    base: str,
    *,
    partition_key: str = "full-series",
) -> CheckResult:
    """Compare the local/published Silver values with a two-pass recomputation."""
    totals = _episode_totals(base)
    state: dict[str, _SeriesState] = {}
    cumulative: dict[tuple[str, int], float] = {}
    expected_rows = 0
    published_rows = 0
    missing_rows = 0
    extra_rows = 0
    close_mismatches = 0
    adj_close_mismatches = 0
    samples: list[dict] = []

    for year, frame in _annual_frames(base):
        published = _published_year(conn, year)
        published_rows += len(published)
        for row in frame[
            ["identifier", "trade_date", "close", "prev_diff"]
        ].itertuples(index=False):
            expected_rows += 1
            identifier = str(row.identifier)
            close = float(row.close)
            previous = state.get(identifier)
            episode, factor = _factor_for_row(
                row.trade_date,
                close,
                row.prev_diff,
                previous,
            )
            episode_key = (identifier, episode)
            cumulative[episode_key] = (
                cumulative.get(episode_key, 1.0) * factor
            )
            expected_adj_close = round(
                close
                * totals[episode_key]
                / cumulative[episode_key],
                4,
            )
            actual = published.pop(
                (identifier, row.trade_date),
                None,
            )
            if actual is None:
                missing_rows += 1
                if len(samples) < 20:
                    samples.append({
                        "identifier": identifier,
                        "trade_date": row.trade_date,
                        "reason": "missing published Silver row",
                        "expected_adj_close": expected_adj_close,
                    })
            else:
                actual_close, actual_adj_close = actual
                if abs(actual_close - close) > 0.00011:
                    close_mismatches += 1
                    if len(samples) < 20:
                        samples.append({
                            "identifier": identifier,
                            "trade_date": row.trade_date,
                            "reason": "published close differs from Bronze candidate",
                            "expected_close": close,
                            "actual_close": actual_close,
                        })
                if abs(actual_adj_close - expected_adj_close) > 0.00011:
                    adj_close_mismatches += 1
                    if len(samples) < 20:
                        samples.append({
                            "identifier": identifier,
                            "trade_date": row.trade_date,
                            "reason": "full-series adjusted close mismatch",
                            "expected_adj_close": expected_adj_close,
                            "actual_adj_close": actual_adj_close,
                        })
            state[identifier] = _SeriesState(
                row.trade_date,
                close,
                episode,
            )
        extra_rows += len(published)
        for (identifier, trade_date), values in list(published.items())[
            : max(0, 20 - len(samples))
        ]:
            samples.append({
                "identifier": identifier,
                "trade_date": trade_date,
                "reason": "extra published Silver row",
                "actual_close": values[0],
                "actual_adj_close": values[1],
            })

    failures = (
        missing_rows
        + extra_rows
        + close_mismatches
        + adj_close_mismatches
    )
    return CheckResult(
        rule_code="ADJ_CLOSE_FULL_SERIES_STREAMING_RECONCILIATION",
        dataset="price_daily",
        severity=Severity.ERROR,
        status=CheckStatus.FAIL if failures else CheckStatus.PASS,
        expected=(
            "published close and adj_close equal an independent two-pass "
            "full-series recomputation for every Bronze candidate key"
        ),
        actual=(
            f"expected_rows={expected_rows}, "
            f"published_rows={published_rows}, "
            f"missing_rows={missing_rows}, extra_rows={extra_rows}, "
            f"close_mismatches={close_mismatches}, "
            f"adj_close_mismatches={adj_close_mismatches}"
        ),
        failed_count=failures,
        samples=samples,
        partition_key=partition_key,
    )


def _input_fingerprint(base: str) -> str:
    marker = Path(base) / ".bronze-input-fingerprint"
    if marker.exists():
        return marker.read_text(encoding="utf-8").strip()
    return hashlib.sha256(str(Path(base).resolve()).encode()).hexdigest()


def run(base: str) -> CheckResult:
    conn = db.connect()
    context = None
    result = None
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn,
            mode="published_adj_close_streaming_audit",
            status="VALIDATING",
            input_fingerprint=_input_fingerprint(base),
        )
        result = check_published_adj_close(conn, base)
        status = "FAILED" if result.blocks_publish else "CERTIFIED"
        repository.finish_run(conn, context, status, [result])
        print(
            f"[silver-quality] {result.rule_code} "
            f"status={result.status.value} {result.actual}",
            flush=True,
        )
        if status == "FAILED":
            raise SystemExit(1)
        return result
    except Exception as exc:
        conn.rollback()
        if context is not None and result is None:
            failure = CheckResult(
                rule_code="ADJ_CLOSE_STREAMING_AUDIT_EXECUTION",
                dataset="price_daily",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="two-pass adjusted-close audit completes",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn,
                context,
                "FAILED",
                [failure],
                error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    args = parser.parse_args()
    run(args.base)


if __name__ == "__main__":
    main()
