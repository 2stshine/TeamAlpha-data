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
