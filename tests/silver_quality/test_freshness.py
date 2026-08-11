from datetime import date
from uuid import UUID

from pipeline.silver.total_return_audit import (
    CONTRACT_START as AUDIT_CONTRACT_START,
    METHODOLOGY_VERSION as AUDIT_METHODOLOGY_VERSION,
)
from pipeline.silver.total_return_rebuild import (
    CONTRACT_COVERAGE_START as REBUILD_CONTRACT_START,
    METHODOLOGY_VERSION as REBUILD_METHODOLOGY_VERSION,
)
from pipeline.silver_quality import freshness
from pipeline.silver_quality.freshness import evaluate
from pipeline.silver.return_contract import CONTRACT_RELEASE


class _Cur:
    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=()):
        self.sql = sql

    def fetchall(self):
        return self.conn.rows

    def fetchone(self):
        return self.conn.contract_row


class _Conn:
    def __init__(self, rows, contract_row):
        self.rows = rows
        self.contract_row = contract_row

    def cursor(self):
        return _Cur(self)

    def rollback(self):
        pass


def _contract(
    coverage_end=date(2026, 8, 10),
    *,
    status="CERTIFIED",
    methodology="krx_gross_dividend_reinvested_v3",
    coverage_start=date(2015, 1, 2),
    quality_run_id=UUID("00000000-0000-0000-0000-000000000001"),
    contract_release=CONTRACT_RELEASE,
    dq_status="CERTIFIED",
    dq_mode="krx_total_return_rebuild",
    first_certified_trade=date(2015, 1, 2),
    last_certified_trade=None,
):
    last_certified_trade = last_certified_trade or coverage_end
    return (
        status,
        methodology,
        coverage_start,
        coverage_end,
        quality_run_id,
        contract_release,
        dq_status,
        dq_mode,
        first_certified_trade,
        last_certified_trade,
    )


def test_freshness_contract_constants_match_certifier_and_auditor():
    assert (
        freshness.KRX_TOTAL_RETURN_METHODOLOGY
        == AUDIT_METHODOLOGY_VERSION
        == REBUILD_METHODOLOGY_VERSION
    )
    assert (
        freshness.KRX_TOTAL_RETURN_COVERAGE_START
        == AUDIT_CONTRACT_START
        == REBUILD_CONTRACT_START
    )


def test_freshness_flags_stale_source():
    conn = _Conn([
        ("KRX", date(2026, 8, 1)),          # lag 10 > 5 -> stale
        ("FMP", date(2026, 8, 10)),         # lag 1 -> fresh
        ("FMP_COMMODITY", date(2026, 8, 10)),
        ("FMP_FX", date(2026, 8, 10)),
    ], _contract(date(2026, 8, 1)))
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
    ], _contract())
    r = evaluate(conn, as_of=date(2026, 8, 11))
    assert r["stale_sources"] == []
    assert r["sources"]["KRX_TOTAL_RETURN"]["ready"] is True


def test_freshness_missing_source_is_stale():
    conn = _Conn(
        [("KRX", date(2026, 8, 10))],
        _contract(),
    )  # FMP* absent
    r = evaluate(conn, as_of=date(2026, 8, 11))
    assert "FMP" in r["stale_sources"]
    assert r["sources"]["FMP"]["max_trade_date"] is None


def test_freshness_rejects_building_total_return_with_current_raw_prices():
    conn = _Conn([
        ("KRX", date(2026, 8, 10)),
        ("FMP", date(2026, 8, 10)),
        ("FMP_COMMODITY", date(2026, 8, 10)),
        ("FMP_FX", date(2026, 8, 10)),
    ], _contract(
        status="BUILDING",
        quality_run_id=None,
        dq_status=None,
        dq_mode=None,
    ))

    result = evaluate(conn, as_of=date(2026, 8, 11))

    assert result["stale_sources"] == ["KRX_TOTAL_RETURN"]
    contract = result["sources"]["KRX_TOTAL_RETURN"]
    assert contract["ready"] is False
    assert any("BUILDING" in issue for issue in contract["issues"])


def test_freshness_rejects_missing_or_drifted_total_return_contract():
    rows = [
        ("KRX", date(2026, 8, 10)),
        ("FMP", date(2026, 8, 10)),
        ("FMP_COMMODITY", date(2026, 8, 10)),
        ("FMP_FX", date(2026, 8, 10)),
    ]
    missing = evaluate(_Conn(rows, None), as_of=date(2026, 8, 11))
    assert "KRX_TOTAL_RETURN" in missing["stale_sources"]

    drifted = evaluate(
        _Conn(
            rows,
                _contract(
                    date(2026, 8, 8),
                    contract_release="stale-release",
                    dq_status="FAILED",
                    last_certified_trade=date(2026, 8, 10),
                ),
        ),
        as_of=date(2026, 8, 11),
    )
    contract = drifted["sources"]["KRX_TOTAL_RETURN"]
    assert contract["ready"] is False
    assert any("contract_release" in issue for issue in contract["issues"])
    assert any("coverage_end" in issue for issue in contract["issues"])
    assert any("dq_status" in issue for issue in contract["issues"])


def test_freshness_rejects_empty_certified_price_coverage():
    rows = [
        ("KRX", date(2026, 8, 10)),
        ("FMP", date(2026, 8, 10)),
        ("FMP_COMMODITY", date(2026, 8, 10)),
        ("FMP_FX", date(2026, 8, 10)),
    ]
    result = evaluate(
        _Conn(
            rows,
            _contract(
                coverage_start=None,
                coverage_end=None,
                first_certified_trade=None,
                last_certified_trade=None,
            ),
        ),
        as_of=date(2026, 8, 11),
    )

    contract = result["sources"]["KRX_TOTAL_RETURN"]
    assert contract["ready"] is False
    assert "KRX_TOTAL_RETURN" in result["stale_sources"]
    assert "certified KRX common-stock price coverage is empty" in contract["issues"]
    assert "contract coverage bounds are missing" in contract["issues"]
