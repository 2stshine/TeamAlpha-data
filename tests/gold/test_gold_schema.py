import os
from pathlib import Path

import psycopg
import pytest


pytestmark = pytest.mark.postgres

ROOT = Path(__file__).parents[2]


@pytest.fixture()
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    connection = psycopg.connect(url)
    with connection.cursor() as cur:
        cur.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
        cur.execute((ROOT / "sql" / "gold_schema.sql").read_text(encoding="utf-8"))
        cur.execute(
            "TRUNCATE gold.factor, public.asset RESTART IDENTITY CASCADE"
        )
    connection.commit()
    yield connection
    connection.close()


def _seed_factor(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.asset(name, asset_type, exchange, currency)
            VALUES ('삼성전자', 'stock', 'KRX', 'KRW')
            RETURNING asset_id
            """
        )
        asset_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO gold.factor(
                factor_key, version, description, implementation_uri,
                implementation_hash, config
            )
            VALUES (
                'mom_12m', 1, '최근 12개월 수익률',
                'pipeline/gold/factors/mom_12m.py', 'test-hash',
                '{"lookback_days":252,"higher_is_better":true}'::jsonb
            )
            RETURNING factor_id
            """
        )
        factor_id = cur.fetchone()[0]
    conn.commit()
    return factor_id, asset_id


def _evaluate(conn, factor_id: int, passed: bool) -> None:
    status = "APPROVED" if passed else "REJECTED"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE gold.factor
               SET evaluation = %s::jsonb,
                   status = %s
             WHERE factor_id = %s
            """,
            (
                (
                    '{"metrics":{"ic_mean":0.03},'
                    f'"input_fingerprint":"fixture","passed":{str(passed).lower()}}}'
                ),
                status,
                factor_id,
            ),
        )
    conn.commit()


def test_approval_requires_passed_evaluation(conn):
    factor_id, _ = _seed_factor(conn)

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gold.factor
                       SET evaluation = '{"passed":false}'::jsonb,
                           status = 'APPROVED'
                     WHERE factor_id = %s
                    """,
                    (factor_id,),
                )
    conn.rollback()


def test_only_approved_factor_accepts_values(conn):
    factor_id, asset_id = _seed_factor(conn)

    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gold.factor_value(
                        factor_id, asset_id, as_of_date, value, rank
                    )
                    VALUES (%s, %s, DATE '2026-07-27', 0.25, 1)
                    """,
                    (factor_id, asset_id),
                )
    conn.rollback()

    _evaluate(conn, factor_id, True)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.factor_value(
                factor_id, asset_id, as_of_date, value, rank
            )
            VALUES (%s, %s, DATE '2026-07-27', 0.25, 1)
            """,
            (factor_id, asset_id),
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT value, rank FROM gold.factor_value")
        assert cur.fetchone() == (0.25, 1)


def test_failed_evaluation_marks_factor_rejected(conn):
    factor_id, _ = _seed_factor(conn)
    _evaluate(conn, factor_id, False)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, evaluation->>'passed' FROM gold.factor"
        )
        assert cur.fetchone() == ("REJECTED", "false")


def test_gold_schema_is_three_tables_and_twenty_columns(conn):
    expected = {
        "factor": {
            "factor_id", "factor_key", "version", "description",
            "implementation_uri", "implementation_hash", "config",
            "evaluation", "status",
        },
        "factor_value": {
            "factor_id", "asset_id", "as_of_date", "value", "rank",
        },
        "factor_correlation": {
            "left_factor_id", "right_factor_id", "period_start", "period_end",
            "correlation", "observation_count",
        },
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'gold'
              AND table_name IN (
                  'factor', 'factor_value', 'factor_correlation'
              )
            """
        )
        actual = {name: set() for name in expected}
        for table_name, column_name in cur.fetchall():
            actual[table_name].add(column_name)

    assert actual == expected
    assert sum(map(len, actual.values())) == 20
