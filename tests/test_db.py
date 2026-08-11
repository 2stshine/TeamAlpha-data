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


def test_connect_applies_local_tunnel_override(monkeypatch):
    captured = {}

    def fake_connect(url, **kwargs):
        captured["url"] = url
        return object()

    monkeypatch.setattr(
        db,
        "database_url",
        lambda: "postgresql://reader:secret@rds.example:5432/quant",
    )
    monkeypatch.setattr(db.psycopg, "connect", fake_connect)
    monkeypatch.setenv("SILVER_DB_HOST_OVERRIDE", "127.0.0.1")
    monkeypatch.setenv("SILVER_DB_PORT_OVERRIDE", "55432")

    db.connect()

    params = db.conninfo_to_dict(captured["url"])
    assert params["host"] == "127.0.0.1"
    assert params["port"] == "55432"
    assert params["dbname"] == "quant"
    assert params["user"] == "reader"
