"""Repair legacy FMP Silver rows without resetting the complete US history."""
from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from pipeline.common import db
from pipeline.fmp_backfill_ecs import _download_prefixes
from pipeline.silver import fmp


@dataclass(frozen=True)
class ExistingAsset:
    asset_id: int
    name: str
    tickers: tuple[str, ...]


def _stale_assets(
    existing: list[ExistingAsset],
    admitted_tickers: set[str],
) -> list[ExistingAsset]:
    return [
        asset for asset in existing
        if not set(asset.tickers).intersection(admitted_tickers)
    ]


def _chunks(values: list[int], size: int):
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def _load_current_universe(root: Path) -> tuple[set[str], dict]:
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BRONZE_BUCKET is required")
    if root.exists():
        shutil.rmtree(root)
    count = _download_prefixes(
        bucket, root, ("stock/fmp/universe/",),
    )
    if count == 0:
        raise RuntimeError("no FMP universe objects downloaded")
    _, identifiers, stats = fmp.prepare_universe(str(root))
    admitted = set(
        identifiers.loc[
            identifiers["identifier_type"].eq("ticker"), "identifier",
        ].astype(str)
    )
    if not admitted:
        raise RuntimeError("FMP admitted ticker universe is empty")
    return admitted, stats


def _existing_assets(conn) -> list[ExistingAsset]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.asset_id, a.name,
                   array_agg(ai.identifier ORDER BY ai.identifier)
            FROM asset a
            JOIN asset_identifier ai USING(asset_id)
            WHERE ai.source='FMP' AND ai.identifier_type='ticker'
            GROUP BY a.asset_id, a.name
            ORDER BY a.asset_id
        """)
        return [
            ExistingAsset(int(asset_id), str(name), tuple(tickers))
            for asset_id, name, tickers in cur.fetchall()
        ]


def _assert_exclusive_assets(conn, asset_ids: list[int]) -> None:
    if not asset_ids:
        return
    checks = {
        "foreign_identifier": """
            SELECT count(*) FROM asset_identifier
            WHERE asset_id=ANY(%s) AND source<>'FMP'
        """,
        "foreign_price": """
            SELECT count(*) FROM price_daily
            WHERE asset_id=ANY(%s) AND source NOT IN ('FMP','FMP_FX')
        """,
        "foreign_fundamental": """
            SELECT count(*) FROM fundamental
            WHERE asset_id=ANY(%s) AND source<>'FMP'
        """,
        "foreign_action": """
            SELECT count(*) FROM corporate_action
            WHERE asset_id=ANY(%s)
              AND source NOT IN ('FMP_DIVIDEND','FMP_SPLIT')
        """,
        "gold_factor": """
            SELECT count(*) FROM gold.factor_value WHERE asset_id=ANY(%s)
        """,
    }
    failures = {}
    with conn.cursor() as cur:
        for name, query in checks.items():
            cur.execute(query, (asset_ids,))
            count = int(cur.fetchone()[0])
            if count:
                failures[name] = count
    if failures:
        raise RuntimeError(
            f"targeted FMP repair refused cross-source references: {failures}"
        )


def _delete_stale_assets(
    conn,
    assets: list[ExistingAsset],
    *,
    batch_size: int,
) -> dict[str, int]:
    counts = {
        "asset": 0,
        "price_daily": 0,
        "fundamental": 0,
        "corporate_action": 0,
        "asset_identifier": 0,
    }
    ids = [asset.asset_id for asset in assets]
    _assert_exclusive_assets(conn, ids)
    conn.rollback()
    for batch in _chunks(ids, batch_size):
        with conn.transaction():
            with conn.cursor() as cur:
                for table in (
                    "price_daily", "fundamental", "corporate_action",
                    "asset_identifier",
                ):
                    cur.execute(
                        f"SELECT count(*) FROM {table} WHERE asset_id=ANY(%s)",
                        (batch,),
                    )
                    counts[table] += int(cur.fetchone()[0])
                cur.execute("DELETE FROM asset WHERE asset_id=ANY(%s)", (batch,))
                counts["asset"] += cur.rowcount
        print(
            f"[silver-fmp-repair] stale assets "
            f"deleted={counts['asset']}/{len(ids)}",
            flush=True,
        )
    return counts


def _delete_invalid_identifiers(
    conn,
    admitted_tickers: set[str],
) -> tuple[int, list[dict]]:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ai.asset_id, ai.identifier, a.name
                FROM asset_identifier ai
                JOIN asset a USING(asset_id)
                WHERE ai.source='FMP' AND ai.identifier_type='ticker'
                  AND NOT (ai.identifier=ANY(%s))
                ORDER BY ai.asset_id, ai.identifier
            """, (sorted(admitted_tickers),))
            rows = cur.fetchall()
            samples = [
                {"asset_id": int(asset_id), "identifier": identifier, "name": name}
                for asset_id, identifier, name in rows[:20]
            ]
            cur.execute("""
                DELETE FROM asset_identifier
                WHERE source='FMP' AND identifier_type='ticker'
                  AND NOT (identifier=ANY(%s))
            """, (sorted(admitted_tickers),))
            deleted = cur.rowcount
            if deleted != len(rows):
                raise RuntimeError(
                    "FMP identifier set changed during repair: "
                    f"selected={len(rows)}, deleted={deleted}"
                )
    return deleted, samples


def _delete_non_session_prices(conn) -> tuple[int, list[str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT trade_date FROM price_daily
            WHERE source='FMP' ORDER BY trade_date
        """)
        bad_dates = [
            trade_date for (trade_date,) in cur.fetchall()
            if not fmp._is_xnys_session(trade_date)
        ]
    conn.rollback()
    if not bad_dates:
        return 0, []
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM price_daily
                WHERE source='FMP' AND trade_date=ANY(%s)
            """, (bad_dates,))
            deleted = cur.rowcount
    return deleted, [value.isoformat() for value in bad_dates]


def _delete_invalid_ohlc(conn) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM price_daily
                WHERE source='FMP' AND (
                    open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
                    OR high < greatest(open, close)
                    OR low > least(open, close)
                )
            """)
            return cur.rowcount


def run(*, apply: bool, batch_size: int = 25) -> None:
    admitted_tickers, universe_stats = _load_current_universe(
        Path("/app/data"),
    )
    conn = db.connect()
    try:
        existing = _existing_assets(conn)
        conn.rollback()
        stale = _stale_assets(existing, admitted_tickers)
        print(
            "[silver-fmp-repair] plan "
            f"existing_assets={len(existing)} "
            f"admitted_tickers={len(admitted_tickers)} "
            f"stale_assets={len(stale)} "
            f"universe_exclusions={universe_stats.get('excluded_by_reason', {})} "
            f"samples={[asset.__dict__ for asset in stale[:20]]}",
            flush=True,
        )
        if not apply:
            return
        stale_counts = _delete_stale_assets(
            conn, stale, batch_size=batch_size,
        )
        invalid_identifiers, identifier_samples = _delete_invalid_identifiers(
            conn, admitted_tickers,
        )
        non_session_prices, bad_dates = _delete_non_session_prices(conn)
        invalid_ohlc = _delete_invalid_ohlc(conn)
        print(
            "[silver-fmp-repair] complete "
            f"stale_counts={stale_counts} "
            f"invalid_identifiers={invalid_identifiers} "
            f"identifier_samples={identifier_samples} "
            f"non_session_prices={non_session_prices} "
            f"non_session_dates={bad_dates} "
            f"invalid_ohlc={invalid_ohlc}",
            flush=True,
        )
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--batch-size", type=int, default=25)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be positive")
    run(apply=args.apply, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
