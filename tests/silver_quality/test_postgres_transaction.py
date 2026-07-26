import os
from pathlib import Path

import psycopg
import pytest

from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity
from pipeline.silver import load


pytestmark = pytest.mark.postgres


@pytest.fixture()
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    connection = psycopg.connect(url)
    schema = Path(__file__).parents[2] / "sql" / "schema.sql"
    migration = (
        Path(__file__).parents[2]
        / "pipeline" / "silver_quality" / "migrations" / "001_quality.sql"
    )
    with connection.cursor() as cur:
        cur.execute(schema.read_text(encoding="utf-8"))
        cur.execute(migration.read_text(encoding="utf-8"))
        cur.execute(
            "TRUNCATE fundamental, price_daily, asset_identifier, asset, "
            "dq_metric, dq_result, dq_run CASCADE"
        )
    connection.commit()
    yield connection
    connection.close()


def test_failed_publish_rolls_back_silver_but_keeps_audit(conn):
    context = repository.start_run(conn, mode="daily", status="RUNNING")
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO asset(
                        name,asset_type,exchange,currency,quality_run_id
                    ) VALUES ('bad','stock','KRX','KRW',%s)
                    """,
                    (context.run_id,),
                )
            raise RuntimeError("forced publish failure")
    except RuntimeError:
        conn.rollback()

    failed = CheckResult(
        "TEST_FORCED_ROLLBACK", "asset", Severity.ERROR, CheckStatus.FAIL,
        "commit", "rollback", 1,
    )
    repository.finish_run(
        conn, context, "FAILED", [failed], error_message="forced",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM asset")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT status FROM dq_run WHERE run_id=%s", (context.run_id,))
        assert cur.fetchone()[0] == "FAILED"


def test_market_holiday_is_recorded_as_skipped(conn, monkeypatch):
    monkeypatch.delenv("SILVER_DB_SECRET_ID", raising=False)
    monkeypatch.setenv("SILVER_DB_URL", os.environ["TEST_DATABASE_URL"])
    load.incremental(
        "20260707",
        financial_files=[],
        market_closed=True,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM dq_run WHERE mode='daily' ORDER BY started_at DESC LIMIT 1"
        )
        assert cur.fetchone()[0] == "SKIPPED"
