from datetime import date

from pipeline.silver_quality.freshness import evaluate


class _Cur:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=()):
        pass

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return _Cur(self.rows)

    def rollback(self):
        pass


def test_freshness_flags_stale_source():
    conn = _Conn([
        ("KRX", date(2026, 8, 1)),          # lag 10 > 5 -> stale
        ("FMP", date(2026, 8, 10)),         # lag 1 -> fresh
        ("FMP_COMMODITY", date(2026, 8, 10)),
        ("FMP_FX", date(2026, 8, 10)),
    ])
    r = evaluate(conn, as_of=date(2026, 8, 11))
    assert r["sources"]["KRX"]["stale"] is True
    assert r["sources"]["KRX"]["lag_days"] == 10
    assert r["sources"]["FMP"]["stale"] is False
    assert r["stale_sources"] == ["KRX"]


def test_freshness_all_fresh():
    conn = _Conn([
        ("KRX", date(2026, 8, 10)),
        ("FMP", date(2026, 8, 7)),
        ("FMP_COMMODITY", date(2026, 8, 7)),
        ("FMP_FX", date(2026, 8, 7)),
    ])
    r = evaluate(conn, as_of=date(2026, 8, 11))
    assert r["stale_sources"] == []


def test_freshness_missing_source_is_stale():
    conn = _Conn([("KRX", date(2026, 8, 10))])   # FMP* absent
    r = evaluate(conn, as_of=date(2026, 8, 11))
    assert "FMP" in r["stale_sources"]
    assert r["sources"]["FMP"]["max_trade_date"] is None
