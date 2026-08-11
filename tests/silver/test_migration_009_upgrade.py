from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from datetime import date
from pathlib import Path

import psycopg
import pytest

from pipeline.silver.cash_adjustment_scale_evidence import (
    SOURCE_EVIDENCE_COLUMNS,
    SUPPORT_ACTION_COLUMNS,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "pipeline/silver_quality/migrations/009_krx_total_return.sql"
)
MIGRATION_010 = (
    ROOT
    / "pipeline/silver_quality/migrations/010_cash_adjustment_scale_evidence.sql"
)

V1_DIVIDEND_COLUMNS = (
    "asset_id", "source", "action_key", "announcement_date", "ex_date",
    "record_date", "payment_date", "cash_amount", "adjusted_cash_amount",
    "currency", "frequency", "status", "confidence", "filing_id",
    "quality_run_id", "loaded_at", "report_name", "action_scope",
)
V3_APPENDED_COLUMNS = (
    "dart_rm", "corp_cls", "cash_amount_status", "source_evidence_status",
    "correction_of_action_key", "revision_root_action_key", "revision_kind",
    "viewer_evidence_sha256", "economic_evidence_sha256",
    "reviewed_correction_id", "payment_date_quality_status",
)

_OLD_SCHEMA = """
CREATE SCHEMA quality_stage;
CREATE TABLE dq_run (run_id UUID PRIMARY KEY);
CREATE TABLE asset (asset_id BIGINT PRIMARY KEY);
CREATE TABLE corporate_action (
    asset_id BIGINT NOT NULL,
    source TEXT NOT NULL,
    action_key TEXT NOT NULL,
    action_type TEXT NOT NULL,
    announcement_date DATE,
    ex_date DATE,
    record_date DATE,
    payment_date DATE,
    cash_amount NUMERIC(28,8),
    adjusted_cash_amount NUMERIC(28,8),
    currency TEXT,
    frequency TEXT,
    status TEXT,
    confidence TEXT,
    filing_id TEXT,
    quality_run_id UUID,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    report_name TEXT,
    action_scope TEXT
);
CREATE TABLE quality_stage.corporate_action
    (LIKE corporate_action INCLUDING ALL);
CREATE TABLE price_daily (
    asset_id BIGINT,
    source TEXT,
    trade_date DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adj_close NUMERIC,
    currency TEXT,
    vwap NUMERIC,
    available_at TIMESTAMPTZ,
    volume NUMERIC,
    trading_value NUMERIC,
    shares NUMERIC,
    market_cap NUMERIC,
    market TEXT,
    total_return_close NUMERIC,
    quality_run_id UUID,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE quality_stage.price_daily (LIKE price_daily INCLUDING ALL);
CREATE VIEW dividend_history AS
SELECT asset_id,source,action_key,announcement_date,ex_date,record_date,
       payment_date,cash_amount,adjusted_cash_amount,currency,frequency,
       status,confidence,filing_id,quality_run_id,loaded_at,
       report_name,action_scope
FROM corporate_action
WHERE action_type='cash_dividend';
COMMENT ON VIEW dividend_history IS 'deployed-v1-comment';
CREATE ROLE research_reader NOLOGIN;
GRANT SELECT ON dividend_history TO research_reader;
CREATE VIEW dividend_history_consumer AS
SELECT asset_id,action_scope FROM dividend_history;
"""


def _created_table_body(sql: str, table: str, next_table: str | None) -> str:
    body = sql.split(f"CREATE TABLE IF NOT EXISTS {table} (", 1)[1]
    if next_table is not None:
        return body.split(f"CREATE TABLE IF NOT EXISTS {next_table} (", 1)[0]
    ends = [
        position for token in ("ALTER TABLE dividend_event_resolution", "DO $$")
        if (position := body.find(token)) >= 0
    ]
    return body[:min(ends)]


def test_schema_and_migration_010_preserve_canonical_evidence_column_order():
    definitions = (
        (ROOT / "sql/schema.sql").read_text(encoding="utf-8"),
        MIGRATION_010.read_text(encoding="utf-8"),
    )
    for sql in definitions:
        parent = _created_table_body(
            sql,
            "cash_adjustment_scale_source_evidence",
            "cash_adjustment_scale_support_action",
        )
        child = _created_table_body(
            sql, "cash_adjustment_scale_support_action", None,
        )
        for body, columns in (
            (parent, SOURCE_EVIDENCE_COLUMNS),
            (child, SUPPORT_ACTION_COLUMNS),
        ):
            positions = []
            for column in columns:
                match = re.search(rf"(?m)^    {re.escape(column)}\s", body)
                assert match is not None, column
                positions.append(match.start())
            assert positions == sorted(positions)


def _run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        **kwargs,
    )


@pytest.mark.skipif(
    any(shutil.which(binary) is None for binary in ("initdb", "pg_ctl", "psql")),
    reason="local PostgreSQL binaries are unavailable",
)
def test_migration_009_upgrades_deployed_v1_view_twice_without_drop(tmp_path):
    """Exercise the deployed 18-column view on a real PostgreSQL server."""
    initdb = str(shutil.which("initdb"))
    pg_ctl = str(shutil.which("pg_ctl"))
    psql = str(shutil.which("psql"))
    data_dir = tmp_path / "postgres"
    log_path = tmp_path / "postgres.log"
    socket_dir = Path(tempfile.mkdtemp(prefix="teamalpha-009-", dir="/tmp"))
    # The unique Unix-socket directory isolates this cluster.  Disable TCP so
    # the integration test remains network-free inside the test sandbox.
    port = 5432
    started = False
    try:
        try:
            _run([
                initdb, "-D", str(data_dir), "-A", "trust", "-U", "postgres",
                "--no-locale", "--encoding=UTF8",
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
            pg_ctl, "-D", str(data_dir), "-l", str(log_path),
            "-o", f"-F -k {socket_dir} -h '' -p {port}", "-w", "start",
        ])
        started = True
        psql_base = [
            psql, "-X", "-v", "ON_ERROR_STOP=1", "-h", str(socket_dir),
            "-p", str(port), "-U", "postgres", "-d", "postgres",
        ]
        _run(psql_base, input=_OLD_SCHEMA)
        _run([*psql_base, "-f", str(MIGRATION)])

        with psycopg.connect(
            host=str(socket_dir), port=port, user="postgres", dbname="postgres",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name='dividend_history'
                    ORDER BY ordinal_position
                    """
                )
                first_columns = tuple(row[0] for row in cur.fetchall())
                cur.execute(
                    "SELECT has_table_privilege("
                    "'research_reader','dividend_history','SELECT')"
                )
                grant_preserved = bool(cur.fetchone()[0])
                cur.execute(
                    "SELECT obj_description('dividend_history'::regclass)"
                )
                comment = cur.fetchone()[0]
                cur.execute("SELECT * FROM dividend_history_consumer LIMIT 0")

        _run([*psql_base, "-f", str(MIGRATION)])

        with psycopg.connect(
            host=str(socket_dir), port=port, user="postgres", dbname="postgres",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name='dividend_history'
                    ORDER BY ordinal_position
                    """
                )
                repeated_columns = tuple(row[0] for row in cur.fetchall())

        expected = (*V1_DIVIDEND_COLUMNS, *V3_APPENDED_COLUMNS)
        assert first_columns == expected
        assert repeated_columns == expected
        assert grant_preserved is True
        assert comment == "deployed-v1-comment"
    finally:
        if started:
            subprocess.run(
                [pg_ctl, "-D", str(data_dir), "-m", "immediate", "-w", "stop"],
                check=False,
                capture_output=True,
                text=True,
            )
        shutil.rmtree(socket_dir, ignore_errors=True)


@pytest.mark.skipif(
    any(shutil.which(binary) is None for binary in ("initdb", "pg_ctl", "psql")),
    reason="local PostgreSQL binaries are unavailable",
)
def test_migration_010_is_idempotent_and_freezes_parent_child_contract(tmp_path):
    """Apply frozen 009 then run 010 twice on a real PostgreSQL server."""
    initdb = str(shutil.which("initdb"))
    pg_ctl = str(shutil.which("pg_ctl"))
    psql = str(shutil.which("psql"))
    data_dir = tmp_path / "postgres-010"
    log_path = tmp_path / "postgres-010.log"
    socket_dir = Path(tempfile.mkdtemp(prefix="teamalpha-010-", dir="/tmp"))
    port = 5433
    started = False
    try:
        try:
            _run([
                initdb, "-D", str(data_dir), "-A", "trust", "-U", "postgres",
                "--no-locale", "--encoding=UTF8",
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
            pg_ctl, "-D", str(data_dir), "-l", str(log_path),
            "-o", f"-F -k {socket_dir} -h '' -p {port}", "-w", "start",
        ])
        started = True
        psql_base = [
            psql, "-X", "-v", "ON_ERROR_STOP=1", "-h", str(socket_dir),
            "-p", str(port), "-U", "postgres", "-d", "postgres",
        ]
        _run(psql_base, input=_OLD_SCHEMA)
        _run([*psql_base, "-f", str(MIGRATION)])
        _run(psql_base, input="""
            UPDATE price_return_contract
            SET methodology_version='krx_gross_dividend_reinvested_v2',
                status='CERTIFIED',
                coverage_start=DATE '2015-01-01',
                coverage_end=DATE '2026-08-10',
                metadata='{"contract_release":"legacy-v2"}'::jsonb,
                certified_at=now();
        """)
        _run([*psql_base, "-f", str(MIGRATION_010)])

        with psycopg.connect(
            host=str(socket_dir), port=port, user="postgres", dbname="postgres",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT methodology_version,status,coverage_start,"
                    "coverage_end,quality_run_id,certified_at "
                    "FROM price_return_contract"
                )
                invalidated_legacy_contract = cur.fetchone()
                cur.execute(
                    """
                    UPDATE price_return_contract
                    SET status='CERTIFIED',
                        coverage_start=DATE '2015-01-01',
                        coverage_end=DATE '2026-08-10',
                        metadata=jsonb_build_object(
                            'contract_release',
                            'krx_total_return_v3_cash_scale_evidence_2026_08'
                        ),
                        certified_at=now()
                    """
                )

        _run([*psql_base, "-f", str(MIGRATION_010)])

        with psycopg.connect(
            host=str(socket_dir), port=port, user="postgres", dbname="postgres",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass('cash_adjustment_scale_source_evidence'),"
                    "to_regclass('cash_adjustment_scale_support_action')"
                )
                tables = cur.fetchone()
                cur.execute(
                    """
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid IN (
                        'cash_adjustment_scale_support_action'::regclass,
                        'dividend_event_resolution'::regclass
                    )
                    """
                )
                constraints = {row[0] for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name='dividend_event_resolution'
                    """
                )
                resolution_columns = {row[0] for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT methodology_version,status,coverage_start,
                           coverage_end,quality_run_id,
                           certified_at IS NOT NULL,
                           metadata->>'contract_release'
                    FROM price_return_contract
                    WHERE source='KRX' AND asset_type='stock'
                      AND field_name='total_return_close'
                    """
                )
                contract = cur.fetchone()
                cur.execute(
                    "SELECT has_table_privilege("
                    "'research_reader','dividend_history','SELECT')"
                )
                grant_preserved = bool(cur.fetchone()[0])
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' "
                    "AND table_name='dividend_history' "
                    "ORDER BY ordinal_position DESC LIMIT 1"
                )
                final_view_column = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO dq_run(run_id) VALUES
                        ('00000000-0000-0000-0000-000000000010');
                    INSERT INTO asset(asset_id) VALUES (1001),(1002);
                    INSERT INTO dart_action_snapshot_contract(
                        quality_run_id,schema_version,manifest_sha256,
                        body_digest,body_count,coverage_start,coverage_end,
                        action_count
                    ) VALUES (
                        '00000000-0000-0000-0000-000000000010',
                        'fixture',repeat('a',64),repeat('b',64),1,
                        DATE '2015-01-01',DATE '2026-08-10',1
                    );
                    INSERT INTO dividend_source_receipt(
                        quality_run_id,receipt_no,asset_id,ticker,report_name,
                        announcement_date,revision_kind,
                        revision_root_receipt_no,terminal_receipt_no,
                        terminal_announcement_date,
                        is_terminal_economic_revision,source_evidence_status,
                        cash_amount_status,record_date,cash_amount,pit_event_date,
                        mapping_status
                    ) VALUES
                    (
                        '00000000-0000-0000-0000-000000000010',
                        '20260101000001',1001,'000001','현금배당결정',
                        DATE '2026-01-01','ORIGINAL_DECISION',
                        '20260101000001','20260101000001',DATE '2026-01-01',
                        true,'VERIFIED_OPENDART_DOCUMENT','POSITIVE',
                        DATE '2026-01-02',100,DATE '2026-01-01','INCLUDED'
                    ),
                    (
                        '00000000-0000-0000-0000-000000000010',
                        '20260101000002',1002,'000002','현금배당결정',
                        DATE '2026-01-02','ORIGINAL_DECISION',
                        '20260101000002','20260101000002',DATE '2026-01-02',
                        true,'VERIFIED_OPENDART_DOCUMENT','POSITIVE',
                        DATE '2026-01-03',200,DATE '2026-01-02','INCLUDED'
                    );
                    INSERT INTO cash_adjustment_scale_source_evidence(
                        action_snapshot_run_id,evidence_key,asset_id,ticker,
                        cash_receipt_no,cash_source_evidence_status,
                        cash_action_body_path,cash_action_body_sha256,
                        cash_economic_body_path,cash_economic_body_schema,
                        cash_economic_sha256,support_action_count,
                        support_action_digest,support_semantic_group_count,
                        price_source,previous_price_source_object_key,
                        previous_price_source_content_sha256,
                        previous_price_source_etag,previous_price_source_schema,
                        adjustment_price_source_object_key,
                        adjustment_price_source_content_sha256,
                        adjustment_price_source_etag,
                        adjustment_price_source_schema,previous_trade_date,
                        adjustment_trade_date,raw_previous_close,
                        raw_applied_close,raw_reference_price,
                        expected_price_factor,cash_scale_basis,
                        manifest_row_sha256
                    ) VALUES
                    (
                        '00000000-0000-0000-0000-000000000010','parent-one',
                        1001,'000001','20260101000001',
                        'VERIFIED_OPENDART_DOCUMENT','cash-1.zip',repeat('c',64),
                        'cash-1.zip','OPENDART_DOCUMENT_ZIP_V1',repeat('c',64),
                        1,repeat('d',64),1,'KRX','previous-1',repeat('e',64),
                        repeat('f',32),'marcap_parquet_v1','applied-1',
                        repeat('1',64),repeat('2',32),'marcap_parquet_v1',
                        DATE '2026-01-01',DATE '2026-01-02',100,101,99,
                        0.99,'PRE_EVENT_PRICE_SCALE',repeat('3',64)
                    ),
                    (
                        '00000000-0000-0000-0000-000000000010','parent-two',
                        1002,'000002','20260101000002',
                        'VERIFIED_OPENDART_DOCUMENT','cash-2.zip',repeat('4',64),
                        'cash-2.zip','OPENDART_DOCUMENT_ZIP_V1',repeat('4',64),
                        1,repeat('5',64),1,'KRX','previous-2',repeat('6',64),
                        repeat('7',32),'marcap_parquet_v1','applied-2',
                        repeat('8',64),repeat('9',32),'marcap_parquet_v1',
                        DATE '2026-01-02',DATE '2026-01-03',200,201,198,
                        0.99,'PRE_EVENT_PRICE_SCALE',repeat('a',64)
                    );
                """)
                child_insert = """
                    INSERT INTO cash_adjustment_scale_support_action(
                        action_snapshot_run_id,evidence_key,
                        support_action_source,support_action_key,
                        support_action_type,target_cash_receipt_no,
                        target_adjustment_date,support_action_body_path,
                        support_action_body_sha256,support_action_quality_run_id,
                        support_announcement_date,support_record_date,
                        support_ratio_numerator,support_ratio_denominator,
                        support_entitlement_security_class,
                        support_distributed_security_class,
                        support_report_name,support_action_scope,
                        support_semantic_group_keys,support_semantic_role,
                        manifest_support_row_sha256
                    ) VALUES (
                        '00000000-0000-0000-0000-000000000010','parent-one',
                        'DART_DISCLOSURE','20251201000001','stock_dividend',
                        %s,%s,'support.zip',repeat('b',64),
                        '00000000-0000-0000-0000-000000000010',
                        DATE '2025-12-01',DATE '2026-01-02',0.1,1,
                        'COMMON','COMMON','주식배당결정','ISSUER','["g"]',
                        'ADJUSTMENT_COMPONENT',repeat('c',64)
                    )
                """
                cur.execute("SAVEPOINT cross_parent_target")
                with pytest.raises(psycopg.errors.ForeignKeyViolation):
                    cur.execute(child_insert, (
                        "20260101000002", date(2026, 1, 3),
                    ))
                cur.execute("ROLLBACK TO SAVEPOINT cross_parent_target")
                cur.execute(child_insert, (
                    "20260101000001", date(2026, 1, 2),
                ))
                cur.execute(
                    "SELECT count(*) FROM cash_adjustment_scale_support_action"
                )
                exact_parent_child_count = cur.fetchone()[0]

        assert tables == (
            "cash_adjustment_scale_source_evidence",
            "cash_adjustment_scale_support_action",
        )
        assert {
            "cash_scale_support_source_type_check",
            "cash_scale_support_role_semantics_check",
            "cash_scale_support_parent_identity_fk",
            "dividend_resolution_scale_evidence_fk",
            "dividend_resolution_v2_scale_contract_check",
        }.issubset(constraints)
        assert {
            "previous_trade_date", "previous_close", "previous_adj_close",
            "applied_close", "applied_adj_close", "previous_price_scale",
            "applied_price_scale", "selected_cash_scale",
            "cash_adjustment_scale_basis", "scale_change_detected",
            "scale_evidence_action_snapshot_run_id", "scale_evidence_key",
            "scale_price_factor_observed", "scale_price_factor_reference",
            "scale_price_factor_parity",
        }.issubset(resolution_columns)
        assert invalidated_legacy_contract == (
            "krx_gross_dividend_reinvested_v3", "BUILDING",
            None, None, None, None,
        )
        assert contract == (
            "krx_gross_dividend_reinvested_v3", "CERTIFIED",
            date(2015, 1, 1), date(2026, 8, 10), None, True,
            "krx_total_return_v3_cash_scale_evidence_2026_08",
        )
        assert grant_preserved is True
        assert final_view_column == "source_body_sha256"
        assert exact_parent_child_count == 1
    finally:
        if started:
            subprocess.run(
                [pg_ctl, "-D", str(data_dir), "-m", "immediate", "-w", "stop"],
                check=False,
                capture_output=True,
                text=True,
            )
        shutil.rmtree(socket_dir, ignore_errors=True)
