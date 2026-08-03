"""Silver quality DB migration 실행.

사용: uv run python -m pipeline.silver_quality.migrate
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from pipeline.common import db

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
ADVISORY_LOCK_ID = 7_226_494_897


def _expected() -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))
    }


def assert_current(conn=None) -> None:
    """Read-only guard used by scheduled jobs; never runs production DDL."""
    owns = conn is None
    conn = conn or db.connect()
    try:
        expected = _expected()
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.silver_schema_migration')")
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    "Silver v2 migration is not applied; run "
                    "`python -m pipeline.silver_quality.migrate` in a maintenance window"
                )
            cur.execute("SELECT migration_name,sha256 FROM silver_schema_migration")
            actual = dict(cur.fetchall())
        missing = sorted(set(expected) - set(actual))
        changed = sorted(
            name for name, checksum in expected.items()
            if name in actual and actual[name] != checksum
        )
        if missing or changed:
            raise RuntimeError(
                f"Silver migrations are not current: missing={missing}, changed={changed}"
            )
        conn.rollback()
    finally:
        if owns:
            conn.close()


def run(conn=None) -> None:
    owns = conn is None
    conn = conn or db.connect()
    try:
        with conn.cursor() as cur:
            # Transaction-scoped lock is released even when a migration fails.
            # A session-scoped lock can leak on an aborted transaction and block
            # every later deploy until that connection is closed.
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_ID,))
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_schema_migration (
                    migration_name TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            migrations = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))
            if not migrations:
                raise RuntimeError(f"no Silver migrations found: {MIGRATIONS_DIR}")
            for migration in migrations:
                sql = migration.read_text(encoding="utf-8")
                checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                cur.execute(
                    "SELECT sha256 FROM silver_schema_migration "
                    "WHERE migration_name=%s",
                    (migration.name,),
                )
                recorded = cur.fetchone()
                if recorded:
                    if recorded[0] != checksum:
                        raise RuntimeError(
                            "applied migration checksum changed: "
                            f"{migration.name}"
                        )
                    continue
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO silver_schema_migration(migration_name,sha256) "
                    "VALUES (%s,%s)",
                    (migration.name, checksum),
                )
                print(
                    f"[silver-quality] applied migration={migration.name}",
                    flush=True,
                )
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
