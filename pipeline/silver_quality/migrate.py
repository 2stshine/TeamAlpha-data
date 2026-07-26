"""Silver quality DB migration 실행.

사용: uv run python -m pipeline.silver_quality.migrate
"""
from __future__ import annotations

from pathlib import Path

from pipeline.common import db

MIGRATION = Path(__file__).parent / "migrations" / "001_quality.sql"
ADVISORY_LOCK_ID = 7_226_494_897


def run(conn=None) -> None:
    owns = conn is None
    conn = conn or db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            try:
                cur.execute(MIGRATION.read_text(encoding="utf-8"))
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        conn.commit()
        print("[silver-quality] migration complete", flush=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        if owns:
            conn.close()


if __name__ == "__main__":
    run()
