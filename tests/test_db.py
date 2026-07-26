from pipeline.common import db


def test_connect_uses_failure_detection_timeouts(monkeypatch):
    captured = {}

    def fake_connect(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(db, "database_url", lambda: "postgresql://example/quant")
    monkeypatch.setattr(db.psycopg, "connect", fake_connect)

    db.connect()

    assert captured == {
        "url": "postgresql://example/quant",
        "connect_timeout": 15,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 3,
        "tcp_user_timeout": 60_000,
    }
