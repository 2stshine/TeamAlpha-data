from datetime import date
from uuid import uuid4

from pipeline.silver_quality.models import (
    BatchContext,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.repository import (
    _incremental_warning_entries,
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
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))


class _Connection:
    def __init__(self):
        self.statements = []

    def cursor(self):
        return _Cursor(self.statements)


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
