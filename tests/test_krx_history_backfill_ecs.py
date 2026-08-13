import pytest

from pipeline import krx_history_backfill_ecs as rebuild


def test_rebuild_refuses_without_confirmation(monkeypatch):
    monkeypatch.delenv("KRX_HISTORY_REBUILD_CONFIRM", raising=False)
    called = []
    monkeypatch.setattr(rebuild, "_download_prefixes", lambda *a, **k: called.append("dl"))
    monkeypatch.setattr(rebuild, "_truncate_silver", lambda: called.append("truncate"))
    with pytest.raises(SystemExit):
        rebuild.run(confirm=None)
    # Nothing destructive (or even a download) runs without confirmation.
    assert called == []


def test_rebuild_refuses_on_wrong_token(monkeypatch):
    monkeypatch.delenv("KRX_HISTORY_REBUILD_CONFIRM", raising=False)
    with pytest.raises(SystemExit):
        rebuild.run(confirm="yes")


def test_confirm_accepts_token_from_env(monkeypatch):
    monkeypatch.setenv("KRX_HISTORY_REBUILD_CONFIRM", "REBUILD")
    assert rebuild._confirmed(None) is True
    monkeypatch.setenv("KRX_HISTORY_REBUILD_CONFIRM", "nope")
    assert rebuild._confirmed(None) is False
    assert rebuild._confirmed("REBUILD") is True


def test_dry_run_skips_confirm_and_truncate(monkeypatch):
    monkeypatch.delenv("KRX_HISTORY_REBUILD_CONFIRM", raising=False)
    monkeypatch.setenv("S3_BRONZE_BUCKET", "b")
    calls = []
    monkeypatch.setattr(rebuild, "_download_prefixes", lambda *a, **k: 5)
    monkeypatch.setattr(rebuild, "_truncate_silver", lambda: calls.append("truncate"))
    monkeypatch.setattr(rebuild, "_dry_run", lambda: calls.append("dry_run"))
    # no confirm needed for dry-run; must not truncate
    rebuild.run(confirm=None, dry_run=True)
    assert calls == ["dry_run"]


def test_destructive_rebuild_is_disabled_before_lock_download_or_truncate(
    monkeypatch,
):
    monkeypatch.setenv("S3_BRONZE_BUCKET", "b")
    calls = []
    monkeypatch.setattr(
        rebuild.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: calls.append("lock"),
    )
    monkeypatch.setattr(rebuild, "_download_prefixes", lambda *a, **k: 10)
    monkeypatch.setattr(rebuild, "_truncate_silver", lambda: calls.append("truncate"))

    with pytest.raises(RuntimeError, match="DIVIDEND/REPORTED"):
        rebuild.run(confirm="REBUILD")
    assert calls == []
