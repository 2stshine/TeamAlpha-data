import os
from pathlib import Path

import psycopg
import pytest

from pipeline.silver_quality import migrate


pytestmark = pytest.mark.postgres
ROOT = Path(__file__).parents[2]


def test_legacy_schema_upgrade_preserves_rows():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    connection = psycopg.connect(url)
    legacy = """
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE asset (
            asset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL CHECK(asset_type IN ('stock','index')),
            exchange TEXT NOT NULL,
            currency TEXT NOT NULL
        );
        CREATE TABLE asset_identifier (
            asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            identifier TEXT NOT NULL,
            PRIMARY KEY(asset_id,source,identifier)
        );
        CREATE UNIQUE INDEX uq_asset_identifier_source_identifier
            ON asset_identifier(source,identifier);
        CREATE TABLE price_daily (
            asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open NUMERIC(18,4), high NUMERIC(18,4), low NUMERIC(18,4),
            close NUMERIC(18,4), adj_close NUMERIC(18,4), volume BIGINT,
            trading_value NUMERIC(20,2), shares BIGINT,
            market_cap NUMERIC(24,2), market TEXT,
            PRIMARY KEY(asset_id,source,trade_date)
        );
        CREATE TABLE fundamental (
            asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            period_end DATE NOT NULL,
            fiscal_period TEXT NOT NULL,
            fs_type TEXT NOT NULL CHECK(fs_type IN ('CFS','OFS')),
            filing_id TEXT,
            filed DATE,
            available_date DATE,
            metric TEXT NOT NULL,
            value NUMERIC(20,2),
            PRIMARY KEY(asset_id,source,period_end,fiscal_period,fs_type,metric)
        );
        INSERT INTO asset(name,asset_type,exchange,currency)
        VALUES ('삼성전자','stock','KRX','KRW');
        INSERT INTO asset_identifier VALUES (1,'KRX','005930');
        INSERT INTO price_daily(
            asset_id,source,trade_date,open,high,low,close,adj_close,volume,
            trading_value,shares,market_cap,market
        ) VALUES (
            1,'KRX','2026-07-31',70000,71000,69000,70500,70500,100,
            7050000,1000,70500000,'KOSPI'
        );
        INSERT INTO fundamental(
            asset_id,source,period_end,fiscal_period,fs_type,filing_id,filed,
            available_date,metric,value
        ) VALUES (
            1,'DART','2025-12-31','FY','CFS','20260301000001','2026-03-01',
            '2026-03-02','total_assets',100
        );
    """
    with connection.cursor() as cur:
        cur.execute(legacy)
    connection.commit()

    migrate.run(connection)

    with connection.cursor() as cur:
        for table in ("asset", "asset_identifier", "price_daily", "fundamental"):
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == 1
        cur.execute("SELECT instrument_type,base_currency FROM asset")
        assert cur.fetchone() == ("common_stock", "KRW")
        cur.execute("SELECT statement_type,data_basis,unit_type FROM fundamental")
        assert cur.fetchone() == ("BS", "STANDARDIZED", "currency")
        cur.execute("SELECT currency,total_return_close FROM price_daily")
        assert cur.fetchone() == ("KRW", 70500)
    connection.close()


@pytest.fixture()
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    connection = psycopg.connect(url)
    with connection.cursor() as cur:
        cur.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    connection.commit()
    migrate.run(connection)
    migrate.run(connection)
    migrate.assert_current(connection)
    yield connection
    connection.close()


def test_v2_migrations_are_ordered_idempotent_and_complete(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT migration_name FROM silver_schema_migration ORDER BY migration_name"
        )
        assert cur.fetchall() == [
            ("001_quality.sql",),
            ("002_silver_v2_fmp.sql",),
        ]
        cur.execute("SELECT to_regclass('public.corporate_action')")
        assert cur.fetchone()[0] == "corporate_action"
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='price_daily'
            """
        )
        columns = {row[0] for row in cur.fetchall()}
        assert {"currency", "total_return_close", "vwap", "available_at"} <= columns


def test_cik_can_be_shared_but_current_ticker_cannot(conn):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE corporate_action, fundamental, price_daily, "
                "asset_identifier, asset RESTART IDENTITY CASCADE"
            )
            cur.execute(
                """
                INSERT INTO asset(name,asset_type,exchange,currency,base_currency)
                VALUES ('Class A','stock','NYSE','USD','USD'),
                       ('Class B','stock','NYSE','USD','USD')
                RETURNING asset_id
                """
            )
            first, second = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                INSERT INTO asset_identifier(
                    asset_id,source,identifier,identifier_type
                ) VALUES (%s,'FMP','0000001','cik'),(%s,'FMP','0000001','cik')
                """,
                (first, second),
            )
            cur.execute(
                "INSERT INTO asset_identifier(asset_id,source,identifier) "
                "VALUES (%s,'FMP','TEST')",
                (first,),
            )
            with pytest.raises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    cur.execute(
                        "INSERT INTO asset_identifier(asset_id,source,identifier) "
                        "VALUES (%s,'FMP','TEST')",
                        (second,),
                    )
