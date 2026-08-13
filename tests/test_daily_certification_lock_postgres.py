from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import psycopg
import pytest

from pipeline import dart_silver_backfill_ecs as ecs


def _run(command: list[str]) -> None:
    subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.skipif(
    any(shutil.which(binary) is None for binary in ("initdb", "pg_ctl")),
    reason="local PostgreSQL binaries are unavailable",
)
def test_real_postgres_epoch_lock_health_exclusion_and_release(
    tmp_path, monkeypatch,
):
    """Exercise the exact bigint pg_locks mapping on PostgreSQL itself."""
    initdb = str(shutil.which("initdb"))
    pg_ctl = str(shutil.which("pg_ctl"))
    data_dir = tmp_path / "postgres-lock"
    log_path = tmp_path / "postgres-lock.log"
    socket_dir = Path(tempfile.mkdtemp(prefix="teamalpha-lock-", dir="/tmp"))
    port = 55434
    started = False
    try:
        try:
            _run([
                initdb,
                "-D",
                str(data_dir),
                "-A",
                "trust",
                "-U",
                "postgres",
                "--no-locale",
                "--encoding=UTF8",
            ])
        except subprocess.CalledProcessError as exc:
            diagnostics = f"{exc.stdout}\n{exc.stderr}"
            if (
                "could not create shared memory segment" in diagnostics
                and "Operation not permitted" in diagnostics
            ):
                pytest.skip("test sandbox blocks PostgreSQL shared memory")
            raise
        _run([
            pg_ctl,
            "-D",
            str(data_dir),
            "-l",
            str(log_path),
            "-o",
            f"-F -k {socket_dir} -h '' -p {port}",
            "-w",
            "start",
        ])
        started = True

        def connect():
            return psycopg.connect(
                host=str(socket_dir),
                port=port,
                user="postgres",
                dbname="postgres",
            )

        monkeypatch.setattr(ecs.db, "connect", connect)
        owner = ecs.acquire_daily_certification_lock()
        ecs.assert_daily_certification_lock(owner)

        contender = connect()
        contender.autocommit = True
        try:
            with contender.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (ecs.DAILY_CERTIFICATION_LOCK_KEY,),
                )
                assert cursor.fetchone() == (False,)

            ecs.release_daily_certification_lock(owner)

            with contender.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (ecs.DAILY_CERTIFICATION_LOCK_KEY,),
                )
                assert cursor.fetchone() == (True,)
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (ecs.DAILY_CERTIFICATION_LOCK_KEY,),
                )
                assert cursor.fetchone() == (True,)
        finally:
            contender.close()
    finally:
        if started:
            subprocess.run(
                [
                    pg_ctl,
                    "-D",
                    str(data_dir),
                    "-m",
                    "immediate",
                    "-w",
                    "stop",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        shutil.rmtree(socket_dir, ignore_errors=True)
