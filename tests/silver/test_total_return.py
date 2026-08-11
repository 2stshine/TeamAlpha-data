from datetime import date

import pandas as pd

from pipeline.silver.total_return import compute_total_return_close


def _prices(rows):
    return pd.DataFrame(
        rows, columns=["asset_id", "trade_date", "close", "adj_close"],
    )


def test_no_dividends_equals_adj_close():
    p = _prices([
        (1, date(2020, 1, 2), 1000.0, 1000.0),
        (1, date(2020, 1, 3), 1100.0, 1100.0),
    ])
    tr = compute_total_return_close(p, pd.DataFrame(
        columns=["asset_id", "ex_date", "cash_amount"]))
    assert list(tr) == [1000.0, 1100.0]


def test_single_dividend_reinvestment_zeroes_ex_date_total_return():
    # price 1000 -> 1000 -> 950 (drop == 50 dividend) -> 950
    p = _prices([
        (1, date(2020, 1, 2), 1000.0, 1000.0),
        (1, date(2020, 1, 3), 1000.0, 1000.0),
        (1, date(2020, 1, 6), 950.0, 950.0),   # ex-date
        (1, date(2020, 1, 7), 950.0, 950.0),
    ])
    div = pd.DataFrame([(1, date(2020, 1, 6), 50.0)],
                       columns=["asset_id", "ex_date", "cash_amount"])
    tr = compute_total_return_close(p, div)
    # latest anchored to adj_close; total return flat across the ex-date drop
    assert round(tr.iloc[-1], 4) == 950.0                 # latest == adj_close
    # daily total return on ex-date is ~0 (price -5% offset by +5% dividend)
    r = tr.iloc[2] / tr.iloc[1] - 1
    assert abs(r) < 1e-9
    # historical TR price is below price-only adj_close (reinvestment)
    assert tr.iloc[0] < 1000.0
    assert round(tr.iloc[0], 4) == 950.0


def test_null_cash_amount_is_skipped():
    p = _prices([
        (1, date(2020, 1, 2), 1000.0, 1000.0),
        (1, date(2020, 1, 3), 950.0, 950.0),
    ])
    div = pd.DataFrame([(1, date(2020, 1, 3), None)],
                       columns=["asset_id", "ex_date", "cash_amount"])
    tr = compute_total_return_close(p, div)
    assert list(tr) == [1000.0, 950.0]  # unchanged (no usable amount)


def test_assets_are_isolated():
    p = _prices([
        (1, date(2020, 1, 2), 1000.0, 1000.0),
        (1, date(2020, 1, 3), 950.0, 950.0),   # asset 1 ex-date
        (2, date(2020, 1, 2), 500.0, 500.0),
        (2, date(2020, 1, 3), 500.0, 500.0),   # asset 2 no dividend
    ])
    div = pd.DataFrame([(1, date(2020, 1, 3), 50.0)],
                       columns=["asset_id", "ex_date", "cash_amount"])
    tr = compute_total_return_close(p, div)
    s = pd.Series(tr.values, index=p.index)
    # asset 2 untouched
    assert list(s[p["asset_id"] == 2]) == [500.0, 500.0]
    # asset 1 latest == adj_close
    a1 = s[p["asset_id"] == 1]
    assert round(a1.iloc[-1], 4) == 950.0


def test_absurd_yield_is_ignored():
    # a garbage dividend larger than price must not poison the series
    p = _prices([
        (1, date(2020, 1, 2), 100.0, 100.0),
        (1, date(2020, 1, 3), 100.0, 100.0),
    ])
    div = pd.DataFrame([(1, date(2020, 1, 3), 5000.0)],  # 50x price -> ignored
                       columns=["asset_id", "ex_date", "cash_amount"])
    tr = compute_total_return_close(p, div)
    assert list(tr) == [100.0, 100.0]
