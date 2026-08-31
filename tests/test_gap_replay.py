import os

import pytest

from pipeline import gap_replay


def test_weekdays_excludes_weekend_and_validates_range():
    assert gap_replay._weekdays("20260814", "20260818") == [
        "20260814", "20260817", "20260818",
    ]
    with pytest.raises(ValueError, match="must not precede"):
        gap_replay._weekdays("20260818", "20260814")


def test_gap_replay_holds_one_epoch_and_only_certifies_final_day(monkeypatch):
    events: list[object] = []
    lock = object()
    monkeypatch.setenv("PIPELINE_DATE", "original")
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: events.append("acquire") or lock,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: events.append(("release", connection)),
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda *, conn: conn is lock,
    )

    def run_day(connection, **kwargs):
        events.append((os.environ["PIPELINE_DATE"], connection, kwargs))

    monkeypatch.setattr(gap_replay.daily_full, "_main_locked", run_day)

    gap_replay.run("20260814", "20260818")

    assert events == [
        "acquire",
        (
            "20260814", lock,
            {
                "allow_deferred_total_return": False,
                "close_total_return": False,
                "assert_final_freshness": False,
                "collect_financials": False,
                "full_year_financial_snapshot": False,
            },
        ),
        (
            "20260817", lock,
            {
                "allow_deferred_total_return": True,
                "close_total_return": False,
                "assert_final_freshness": False,
                "collect_financials": False,
                "full_year_financial_snapshot": False,
            },
        ),
        (
            "20260818", lock,
            {
                "allow_deferred_total_return": True,
                "close_total_return": True,
                "assert_final_freshness": True,
                "collect_financials": True,
                "full_year_financial_snapshot": True,
            },
        ),
        ("release", lock),
    ]
    assert os.environ["PIPELINE_DATE"] == "original"


def test_gap_replay_refuses_unhealthy_entry_contract(monkeypatch):
    lock = object()
    released: list[object] = []
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: lock,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        released.append,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda *, conn: False,
    )

    with pytest.raises(RuntimeError, match="CERTIFIED"):
        gap_replay.run("20260817", "20260818")
    assert released == [lock]
