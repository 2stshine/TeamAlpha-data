from datetime import date

import pandas as pd
import pytest

from pipeline.silver.total_returns import (
    apply_dividends_to_prices,
    build_total_return_close,
    canonicalize_cash_dividends,
    classify_cash_dividend_revisions,
    resolve_dividend_ex_dates,
)


def _actions(rows):
    defaults = {
        "source": "DART_DISCLOSURE",
        "event_type": "cash_dividend",
        "announcement_date": date(2026, 1, 1),
        "effective_date": None,
        "record_date": date(2026, 1, 6),
        "cash_amount": 10.0,
        "rcept_no": "20260101000001",
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def _prices(rows):
    return pd.DataFrame(rows, columns=[
        "identifier", "trade_date", "close", "adj_close",
    ])


def test_canonical_dividend_uses_latest_revision_and_rejects_incomplete_rows():
    actions = _actions([
        {
            "identifier": "005930",
            "announcement_date": date(2026, 1, 2),
            "cash_amount": 300,
            "rcept_no": "20260102000001",
        },
        {
            "identifier": "005930",
            "announcement_date": date(2026, 1, 3),
            "cash_amount": 500,
            "rcept_no": "20260103000001",
        },
        {
            "identifier": "000660",
            "cash_amount": None,
            "rcept_no": "20260104000001",
        },
    ])

    canonical = canonicalize_cash_dividends(actions)

    assert len(canonical) == 1
    assert canonical.iloc[0]["cash_amount"] == pytest.approx(500)
    assert canonical.iloc[0]["dividend_key"] == "20260103000001"
    assert canonical.attrs["canonicalization"] == {
        "input_rows": 3,
        "eligible_rows": 2,
        "canonical_rows": 1,
        "superseded_rows": 1,
        "rejected_rows": 1,
    }

    audit = classify_cash_dividend_revisions(actions)
    assert set(audit["dividend_key"]) == {
        "20260102000001", "20260103000001", "20260104000001",
    }
    assert audit.set_index("dividend_key").loc[
        "20260102000001", "excluded_reason"
    ] == "SUPERSEDED_REVISION"
    assert audit.set_index("dividend_key").loc[
        "20260103000001", "is_canonical"
    ]
    assert audit.set_index("dividend_key").loc[
        "20260104000001", "excluded_reason"
    ] == "INVALID_CASH_AMOUNT"


def test_ex_date_uses_explicit_notice_before_market_session_inference():
    cash = canonicalize_cash_dividends(_actions([{
        "identifier": "005930",
        "record_date": date(2026, 1, 6),
    }]))
    all_actions = _actions([
        {
            "identifier": "005930",
            "record_date": date(2026, 1, 6),
        },
        {
            "identifier": "005930",
            "source": "DART_DISCLOSURE",
            "event_type": "ex_dividend",
            "announcement_date": date(2026, 1, 2),
            "effective_date": date(2026, 1, 2),
            "record_date": None,
            "cash_amount": None,
            "rcept_no": "20260102000009",
        },
    ])

    resolved = resolve_dividend_ex_dates(
        cash,
        all_actions,
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
        ])),
    )

    assert resolved.iloc[0]["resolved_ex_date"] == pd.Timestamp("2026-01-02")
    assert resolved.iloc[0]["ex_date_basis"] == "KRX_NOTICE"


def test_ex_date_falls_back_to_second_session_on_or_before_record_date():
    cash = canonicalize_cash_dividends(_actions([{
        "identifier": "005930",
        "record_date": date(2026, 1, 6),
    }]))

    resolved = resolve_dividend_ex_dates(
        cash,
        _actions([]),
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
        ])),
    )

    assert resolved.iloc[0]["resolved_ex_date"] == pd.Timestamp("2026-01-05")
    assert (
        resolved.iloc[0]["ex_date_basis"]
        == "KRX_T2_INFERRED"
    )


def test_gross_total_return_scales_dividend_for_later_split():
    prices = _prices([
        ("005930", date(2026, 1, 1), 100.0, 50.0),
        ("005930", date(2026, 1, 2), 100.0, 50.0),
        ("005930", date(2026, 1, 5), 50.0, 50.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2026, 1, 2),
    }])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["adjusted_cash_amount"] == pytest.approx(5.0)
    assert events.iloc[0]["application_status"] == "applied"
    assert result["total_return_close"].tolist() == pytest.approx([
        50.0, 55.0, 55.0,
    ])


def test_nontrading_ex_date_applies_on_first_following_trade_and_sums_cash():
    prices = _prices([
        ("005930", date(2026, 1, 2), 100.0, 100.0),
        ("005930", date(2026, 1, 5), 98.0, 98.0),
    ])
    dividends = pd.DataFrame([
        {
            "identifier": "005930", "cash_amount": 1.0,
            "resolved_ex_date": date(2026, 1, 3),
        },
        {
            "identifier": "005930", "cash_amount": 2.0,
            "resolved_ex_date": date(2026, 1, 3),
        },
    ])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert set(events["application_status"]) == {"applied"}
    assert set(events["applied_trade_date"]) == {pd.Timestamp("2026-01-05")}
    assert result.iloc[1]["adjusted_cash_dividend"] == pytest.approx(3.0)
    assert result.iloc[1]["total_return_close"] == pytest.approx(101.0)


def test_dividend_does_not_cross_a_new_listing_episode():
    prices = _prices([
        ("005930", date(2024, 1, 2), 100.0, 100.0),
        ("005930", date(2026, 1, 5), 50.0, 50.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2025, 1, 2),
    }])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["application_status"] == "listing_episode_gap"
    assert result["adjusted_cash_dividend"].sum() == 0
    assert result["total_return_close"].tolist() == pytest.approx([100.0, 50.0])


def test_end_to_end_without_cash_events_equals_adjusted_close():
    prices = _prices([
        ("005930", date(2026, 1, 2), 100.0, 90.0),
        ("005930", date(2026, 1, 5), 110.0, 99.0),
    ])

    result, events = build_total_return_close(
        prices,
        _actions([]),
        prices["trade_date"],
    )

    assert events.empty
    assert result["total_return_close"].tolist() == pytest.approx([90.0, 99.0])
