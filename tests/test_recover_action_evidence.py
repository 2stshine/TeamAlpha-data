from unittest.mock import Mock

import pytest

from pipeline import recover_action_evidence


def test_recovery_holds_lock_and_invalidates_before_publication(monkeypatch):
    lock = object()
    acquire = Mock(return_value=lock)
    release = Mock()
    assert_lock = Mock()
    invalidate = Mock()

    monkeypatch.setattr(
        recover_action_evidence.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        acquire,
    )
    monkeypatch.setattr(
        recover_action_evidence.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        release,
    )
    monkeypatch.setattr(
        recover_action_evidence.dart_silver_backfill_ecs,
        "assert_daily_certification_lock",
        assert_lock,
    )
    monkeypatch.setattr(
        recover_action_evidence.dart_silver_backfill_ecs,
        "invalidate_total_return_for_observed_action",
        invalidate,
    )

    def collect(*args, **kwargs):
        callback = kwargs["before_change"]
        callback("first")
        callback("second")
        return ["first", "second"]

    monkeypatch.setattr(
        recover_action_evidence.corporate_actions, "run", collect,
    )

    recover_action_evidence.run("20141201", "20260811")

    acquire.assert_called_once_with()
    invalidate.assert_called_once()
    assert invalidate.call_args.kwargs["conn"] is lock
    assert assert_lock.call_count == 3
    release.assert_called_once_with(lock)


def test_recovery_rejects_reversed_interval_before_lock(monkeypatch):
    acquire = Mock()
    monkeypatch.setattr(
        recover_action_evidence.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        acquire,
    )
    with pytest.raises(ValueError, match="must not be after"):
        recover_action_evidence.run("20260812", "20260811")
    acquire.assert_not_called()
