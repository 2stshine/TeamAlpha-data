from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pipeline.silver import load
from pipeline.silver_quality.models import CandidateBundle


class _PassedSkipGuard(Exception):
    """Sentinel raised past the SKIPPED guard to prove it did not short-circuit."""


def _stub_run_lifecycle(monkeypatch) -> list[str]:
    """Stub the DB/run-record layer and capture finish_run terminal statuses."""
    monkeypatch.setattr(load.db, "connect", lambda: MagicMock())
    monkeypatch.setattr(load.repository, "assert_schema", lambda conn: None)
    monkeypatch.setattr(
        load.repository,
        "start_run",
        lambda conn, **kwargs: MagicMock(run_id="run-1", mode="daily"),
    )
    finish_statuses: list[str] = []
    monkeypatch.setattr(
        load.repository,
        "finish_run",
        lambda conn, context, status, results, **kwargs: finish_statuses.append(
            status
        ),
    )
    return finish_statuses


def test_incremental_skips_market_holiday_without_any_change(monkeypatch):
    finish_statuses = _stub_run_lifecycle(monkeypatch)
    build_calls: list[bool] = []
    monkeypatch.setattr(
        load, "_build_candidates", lambda *a, **k: build_calls.append(True),
    )

    load.incremental(
        "20260815",
        "local",
        financial_files=[],
        dividend_files=[],
        market_closed=True,
        has_action_change=False,
        action_coverage_start=date(2015, 1, 1),
        action_coverage_end=date(2026, 8, 15),
    )

    assert finish_statuses == ["SKIPPED"]
    assert build_calls == []  # candidate build must not be reached


def test_incremental_does_not_skip_when_only_action_changed(monkeypatch):
    finish_statuses = _stub_run_lifecycle(monkeypatch)

    def _boom(*a, **k):
        raise _PassedSkipGuard()

    monkeypatch.setattr(load, "_build_candidates", _boom)

    with pytest.raises(_PassedSkipGuard):
        load.incremental(
            "20260815",
            "local",
            financial_files=[],
            dividend_files=[],
            market_closed=True,
            has_action_change=True,
            action_coverage_start=date(2015, 1, 1),
            action_coverage_end=date(2026, 8, 15),
        )

    # Guard was passed: the run proceeds and the transform error marks it FAILED,
    # never SKIPPED.
    assert finish_statuses == ["FAILED"]


def test_incremental_reuses_external_epoch_connection_without_closing(
    monkeypatch,
):
    external = MagicMock()
    monkeypatch.setattr(
        load.db,
        "connect",
        lambda: pytest.fail("must not open an unfenced DB session"),
    )
    monkeypatch.setattr(load.repository, "assert_schema", lambda conn: None)
    monkeypatch.setattr(
        load.repository,
        "start_run",
        lambda conn, **kwargs: MagicMock(run_id="run-1", mode="daily"),
    )
    monkeypatch.setattr(load.repository, "finish_run", lambda *a, **k: None)

    load.incremental(
        "20260815",
        "local",
        financial_files=[],
        dividend_files=[],
        market_closed=True,
        has_action_change=False,
        action_coverage_start=date(2015, 1, 1),
        action_coverage_end=date(2026, 8, 15),
        conn=external,
    )

    external.close.assert_not_called()


def test_direct_price_load_cli_is_disabled_before_writer_dispatch(monkeypatch):
    monkeypatch.setattr(
        load,
        "parse_args",
        lambda: SimpleNamespace(
            mode="incremental", src="local", date="20260810", resume=None,
        ),
    )
    monkeypatch.setattr(
        load,
        "incremental",
        lambda *args, **kwargs: pytest.fail("unsafe incremental reached"),
    )

    with pytest.raises(RuntimeError, match="direct Silver price load"):
        load.main()


@pytest.mark.parametrize(
    ("coverage_start", "coverage_end", "message"),
    [
        (None, date(2026, 8, 15), "coverage start"),
        (date(2015, 1, 2), date(2026, 8, 15), "coverage start"),
        (date(2015, 1, 1), None, "must equal target_date"),
        (date(2015, 1, 1), date(2026, 8, 16), "must equal target_date"),
    ],
)
def test_incremental_fails_before_db_when_action_coverage_is_not_exact(
    monkeypatch, coverage_start, coverage_end, message,
):
    monkeypatch.setattr(
        load.db, "connect", lambda: pytest.fail("DB opened before scope check"),
    )

    with pytest.raises(ValueError, match=message):
        load.incremental(
            "20260815",
            "local",
            action_coverage_start=coverage_start,
            action_coverage_end=coverage_end,
        )


def test_incremental_accepts_explicit_bounded_action_scope(monkeypatch):
    finish_statuses = _stub_run_lifecycle(monkeypatch)

    def _boom(*args, **kwargs):
        raise _PassedSkipGuard()

    monkeypatch.setattr(load, "_build_candidates", _boom)

    with pytest.raises(_PassedSkipGuard):
        load.incremental(
            "20260815",
            "local",
            financial_files=[],
            dividend_files=[],
            action_coverage_start=date(2026, 2, 16),
            action_coverage_end=date(2026, 8, 15),
            allow_bounded_action_scope=True,
        )

    assert finish_statuses == ["FAILED"]


def test_daily_candidate_filter_excludes_unmapped_actions_explicitly():
    bundle = CandidateBundle(
        actions=pd.DataFrame([
            {
                "identifier": "005930",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
            {
                "identifier": "999999",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
            {
                "identifier": "250030",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
        ]),
        stats={
            "fundamental": {},
            "corporate_action": {
                "transformed_rows": 3,
                "excluded_rows": 0,
            },
        },
    )

    load._exclude_nontradable_candidates(
        bundle,
        {"005930"},
        {"250030"},
    )

    assert bundle.actions["identifier"].tolist() == ["005930"]
    stats = bundle.stats["corporate_action"]
    assert stats["transformed_rows"] == 1
    assert stats["excluded_rows"] == 2
    assert stats["no_tradable_price_action"]["row_count"] == 1
    assert stats["unsupported_market_action"]["row_count"] == 1
