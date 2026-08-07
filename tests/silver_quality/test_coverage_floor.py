"""Daily coverage-floor gates for KRX and FMP (partial-day protection)."""
from datetime import date

import pandas as pd

from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CandidateBundle, CheckStatus, Severity
from pipeline.silver_quality.rules.fmp import check_fmp
from pipeline.silver_quality.rules.prices import check_market_coverage_floor

TARGET = date(2026, 8, 5)
KRX_BASELINE = {"price_daily": {"coverage_baseline": {"KOSPI": 900, "KOSDAQ": 1600}}}


def _only(results, code):
    matches = [r for r in results if r.rule_code == code]
    assert len(matches) == 1, f"expected exactly one {code}, got {len(matches)}"
    return matches[0]


def _krx_stock_prices(counts_by_market, target_date=TARGET):
    rows = []
    for market, n in counts_by_market.items():
        for i in range(n):
            rows.append({
                "asset_type": "stock",
                "market": market,
                "identifier": f"{market}{i:05d}",
                "trade_date": target_date,
            })
    return pd.DataFrame(
        rows, columns=["asset_type", "market", "identifier", "trade_date"],
    )


# --- KRX rule -------------------------------------------------------------

def test_krx_floor_passes_on_full_day():
    prices = _krx_stock_prices({"KOSPI": 900, "KOSDAQ": 1600})
    res = _only(
        check_market_coverage_floor(prices, KRX_BASELINE, TARGET, False),
        "PRICE_MARKET_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.PASS
    assert res.severity is Severity.ERROR


def test_krx_floor_passes_exactly_at_boundary():
    # 450 == 50% of 900 -> not below floor -> PASS.
    prices = _krx_stock_prices({"KOSPI": 450, "KOSDAQ": 1600})
    res = _only(
        check_market_coverage_floor(prices, KRX_BASELINE, TARGET, False),
        "PRICE_MARKET_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.PASS


def test_krx_floor_blocks_partial_market():
    # KOSPI 400 < 50% of 900 -> shortfall -> blocking FAIL.
    prices = _krx_stock_prices({"KOSPI": 400, "KOSDAQ": 1600})
    res = _only(
        check_market_coverage_floor(prices, KRX_BASELINE, TARGET, False),
        "PRICE_MARKET_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.FAIL
    assert res.blocks_publish
    assert res.failed_count == 1


def test_krx_floor_blocks_index_only_day_with_no_stock_rows():
    # The vacuous-pass hole: stock rows entirely missing but index present.
    prices = pd.DataFrame([
        {"asset_type": "index", "market": None, "identifier": "1028",
         "trade_date": TARGET},
    ])
    res = _only(
        check_market_coverage_floor(prices, KRX_BASELINE, TARGET, False),
        "PRICE_MARKET_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.FAIL
    assert res.failed_count == 2  # both markets missing


def test_krx_floor_noop_without_baseline():
    prices = _krx_stock_prices({"KOSPI": 1, "KOSDAQ": 1})
    assert check_market_coverage_floor(
        prices, {"price_daily": {}}, TARGET, False,
    ) == []


def test_krx_floor_noop_on_market_closed():
    assert check_market_coverage_floor(
        pd.DataFrame(), KRX_BASELINE, TARGET, True,
    ) == []


def test_krx_floor_noop_without_target_date():
    prices = _krx_stock_prices({"KOSPI": 900, "KOSDAQ": 1600})
    assert check_market_coverage_floor(prices, KRX_BASELINE, None, False) == []


# --- FMP rule -------------------------------------------------------------

def _fmp_bundle(*, transformed_rows, baseline, market_closed=False,
                target_date=date(2026, 8, 4)):
    return CandidateBundle(
        assets=pd.DataFrame(),
        identifiers=pd.DataFrame(),
        prices=pd.DataFrame(),
        fundamentals=pd.DataFrame(),
        actions=pd.DataFrame(),
        stats={
            "_target_date": target_date,
            "_market_closed": market_closed,
            "price_daily": {
                "transformed_rows": transformed_rows,
                "coverage_baseline": baseline,
            },
        },
    )


def test_fmp_floor_passes_on_full_session():
    res = _only(
        check_fmp(_fmp_bundle(transformed_rows=4800, baseline=5000)),
        "FMP_DAILY_PRICE_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.PASS
    assert res.severity is Severity.ERROR


def test_fmp_floor_blocks_partial_session():
    res = _only(
        check_fmp(_fmp_bundle(transformed_rows=1000, baseline=5000)),
        "FMP_DAILY_PRICE_COVERAGE_FLOOR",
    )
    assert res.status is CheckStatus.FAIL
    assert res.blocks_publish


def test_fmp_floor_absent_without_baseline():
    results = check_fmp(_fmp_bundle(transformed_rows=1000, baseline=None))
    assert not [
        r for r in results if r.rule_code == "FMP_DAILY_PRICE_COVERAGE_FLOOR"
    ]


def test_fmp_floor_absent_on_market_closed():
    results = check_fmp(
        _fmp_bundle(transformed_rows=0, baseline=5000, market_closed=True),
    )
    assert not [
        r for r in results if r.rule_code == "FMP_DAILY_PRICE_COVERAGE_FLOOR"
    ]


# --- baseline repository helpers -----------------------------------------

class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, *_):
        return None

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


def test_market_baseline_is_per_market_median():
    rows = (
        [("KOSPI", n) for n in (900, 902, 898, 901, 899, 900)]
        + [("KOSDAQ", n) for n in (1600, 1601, 1599, 1600, 1602, 1598)]
    )
    base = repository.recent_market_coverage_baseline(
        _Connection(rows), "KRX", ["KOSPI", "KOSDAQ"], TARGET,
    )
    assert base == {"KOSPI": 900, "KOSDAQ": 1600}


def test_market_baseline_skips_market_below_min_days():
    rows = [("KOSPI", 900)] * 3  # fewer than min_days observations
    assert repository.recent_market_coverage_baseline(
        _Connection(rows), "KRX", ["KOSPI"], TARGET,
    ) == {}


def test_source_daily_baseline_is_median():
    rows = [(5000,), (5010,), (4990,), (5000,), (5001,)]
    assert repository.recent_source_daily_count_baseline(
        _Connection(rows), "FMP", date(2026, 8, 4),
    ) == 5000


def test_source_daily_baseline_none_when_too_few_sessions():
    rows = [(5000,), (5010,)]
    assert repository.recent_source_daily_count_baseline(
        _Connection(rows), "FMP", date(2026, 8, 4),
    ) is None
