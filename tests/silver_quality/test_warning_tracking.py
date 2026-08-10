from datetime import date
from uuid import uuid4

from pipeline.silver_quality.models import (
    BatchContext,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.repository import (
    WARNING_TRACKED_MODES,
    _incremental_warning_entries,
    acknowledge_warning,
    sync_incremental_warning_state,
)


def _result(
    status: CheckStatus,
    *,
    failed_count: int = 0,
    partition_key: str | None = None,
) -> CheckResult:
    return CheckResult(
        rule_code="PRICE_RETURN_SPIKE",
        dataset="price_daily",
        severity=Severity.WARNING,
        status=status,
        expected="no unexplained spike",
        actual=f"failed={failed_count}",
        failed_count=failed_count,
        samples=[{"identifier": "000001", "trade_date": "2026-08-04"}],
        partition_key=partition_key,
    )


def test_warning_entries_use_incremental_date_as_stable_scope():
    context = BatchContext(
        run_id=uuid4(), mode="daily", target_date=date(2026, 8, 4),
    )

    entries = _incremental_warning_entries(
        context,
        [_result(CheckStatus.FAIL, failed_count=2)],
    )

    assert entries == [
        {
            "scope_key": "date=2026-08-04",
            "partition_key": None,
            "dataset_name": "price_daily",
            "rule_code": "PRICE_RETURN_SPIKE",
            "status": CheckStatus.FAIL,
            "failed_count": 2,
            "expected_value": "no unexplained spike",
            "actual_values": ["failed=2"],
            "sample_records": [
                {"identifier": "000001", "trade_date": "2026-08-04"},
            ],
        }
    ]


def test_warning_entries_prefer_explicit_changed_partition():
    context = BatchContext(
        run_id=uuid4(), mode="daily", target_date=date(2026, 8, 4),
    )

    entry = _incremental_warning_entries(
        context,
        [_result(CheckStatus.PASS, partition_key="fundamental:2025:FY")],
    )[0]

    assert entry["scope_key"] == "partition=fundamental:2025:FY"
    assert entry["partition_key"] == "fundamental:2025:FY"
    assert entry["status"] == CheckStatus.PASS


class _Cursor:
    def __init__(self, statements, rowcount=1):
        self.statements = statements
        self.rowcount = rowcount

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))


class _Connection:
    def __init__(self, rowcount=1):
        self.statements = []
        self._rowcount = rowcount

    def cursor(self):
        return _Cursor(self.statements, self._rowcount)


def test_whole_dataset_backfill_warning_uses_dataset_scope():
    # No partition and no target_date (a full-range backfill global check):
    # scope must be the dataset, not the run id, so re-runs are idempotent.
    context = BatchContext(run_id=uuid4(), mode="dart_dividend_action_backfill")
    entry = _incremental_warning_entries(
        context, [_result(CheckStatus.FAIL, failed_count=730)],
    )[0]
    assert entry["scope_key"] == "dataset=price_daily"
    assert entry["partition_key"] is None


def test_backfill_modes_are_tracked():
    assert "backfill_candidate" in WARNING_TRACKED_MODES
    assert "fmp_backfill_partition" in WARNING_TRACKED_MODES
    assert "dart_dividend_action_backfill" in WARNING_TRACKED_MODES
    # read-only re-check modes must NOT be tracked (would duplicate the worklist)
    assert "audit" not in WARNING_TRACKED_MODES

    context = BatchContext(
        run_id=uuid4(), mode="backfill_candidate",
        partition_key="price:year=1998",
    )
    conn = _Connection()
    sync_incremental_warning_state(
        conn, context, [_result(CheckStatus.FAIL, failed_count=4)],
    )
    assert len(conn.statements) == 1
    assert "INSERT INTO dq_warning_state" in conn.statements[0][0]


def test_fail_upsert_preserves_acknowledgement_when_value_unchanged():
    context = BatchContext(
        run_id=uuid4(), mode="backfill_candidate",
        partition_key="price:year=1998",
    )
    conn = _Connection()
    sync_incremental_warning_state(
        conn, context, [_result(CheckStatus.FAIL, failed_count=4)],
    )
    sql = conn.statements[0][0]
    # ACKNOWLEDGED rows with an unchanged fingerprint stay acknowledged.
    assert "WHEN dq_warning_state.status='ACKNOWLEDGED'" in sql
    assert "acknowledged_fingerprint IS NOT DISTINCT FROM EXCLUDED.actual_value" in sql


def test_pass_resolves_open_or_acknowledged():
    context = BatchContext(
        run_id=uuid4(), mode="backfill_candidate",
        partition_key="price:year=1998",
    )
    conn = _Connection()
    sync_incremental_warning_state(
        conn, context, [_result(CheckStatus.PASS, partition_key="price:year=1998")],
    )
    sql = conn.statements[0][0]
    assert "SET status='RESOLVED'" in sql
    assert "status IN ('OPEN', 'ACKNOWLEDGED')" in sql


def test_acknowledge_warning_targets_only_open_rows():
    conn = _Connection(rowcount=1)
    assert acknowledge_warning(conn, 42, note="benign", by="daniel") is True
    sql, params = conn.statements[0]
    assert "SET status='ACKNOWLEDGED'" in sql
    assert "acknowledged_fingerprint=actual_value" in sql
    assert "WHERE warning_state_id=%s AND status='OPEN'" in sql
    assert params == ("daniel", "benign", 42)


def test_acknowledge_warning_returns_false_when_no_open_row():
    conn = _Connection(rowcount=0)
    assert acknowledge_warning(conn, 999) is False


def test_certified_incremental_failure_opens_and_pass_resolves_same_scope():
    context = BatchContext(
        run_id=uuid4(), mode="daily", target_date=date(2026, 8, 4),
    )
    conn = _Connection()

    sync_incremental_warning_state(
        conn,
        context,
        [_result(CheckStatus.FAIL, failed_count=3)],
    )
    sync_incremental_warning_state(
        conn,
        context,
        [_result(CheckStatus.PASS)],
    )

    assert len(conn.statements) == 2
    assert "INSERT INTO dq_warning_state" in conn.statements[0][0]
    assert "ON CONFLICT" in conn.statements[0][0]
    assert "SET status='RESOLVED'" in conn.statements[1][0]
    assert conn.statements[1][1][-4:] == (
        "daily",
        "date=2026-08-04",
        "price_daily",
        "PRICE_RETURN_SPIKE",
    )


def test_non_incremental_runs_do_not_change_warning_state():
    context = BatchContext(run_id=uuid4(), mode="audit")
    conn = _Connection()

    sync_incremental_warning_state(
        conn,
        context,
        [_result(CheckStatus.FAIL, failed_count=1)],
    )

    assert conn.statements == []
