"""일별 temporary staging과 최초 backfill 영구 staging."""
from __future__ import annotations

from uuid import UUID

import pandas as pd


def _copy_frame(cur, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    clean = frame[columns].astype(object).where(pd.notna(frame[columns]), None)
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as cp:
        for row in clean.itertuples(index=False, name=None):
            cp.write_row(row)


def replace_backfill_partition(
    conn,
    table: str,
    run_id: UUID,
    partition_key: str,
    frame: pd.DataFrame,
) -> None:
    allowed = {"asset", "asset_identifier", "price_daily", "fundamental"}
    if table not in allowed:
        raise ValueError(f"unsupported stage table: {table}")
    target = f"quality_stage.{table}"
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {target} WHERE backfill_run_id=%s AND partition_key=%s",
            (run_id, partition_key),
        )
        if frame.empty:
            return
        staged = frame.copy()
        staged.insert(0, "partition_key", partition_key)
        staged.insert(0, "backfill_run_id", run_id)
        columns = list(staged.columns)
        _copy_frame(cur, target, columns, staged)


def staged_partitions(conn, run_id: UUID, table: str) -> set[str]:
    if table not in {"asset", "asset_identifier", "price_daily", "fundamental"}:
        raise ValueError(table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT partition_key FROM quality_stage.{table} "
            "WHERE backfill_run_id=%s",
            (run_id,),
        )
        return {row[0] for row in cur.fetchall()}


def cleanup_backfill(conn, run_id: UUID) -> None:
    with conn.cursor() as cur:
        for table in ("fundamental", "price_daily", "asset_identifier", "asset"):
            cur.execute(
                f"DELETE FROM quality_stage.{table} WHERE backfill_run_id=%s",
                (run_id,),
            )
