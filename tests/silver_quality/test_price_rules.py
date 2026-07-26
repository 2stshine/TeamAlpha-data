from datetime import date

import pandas as pd

from pipeline.silver import assets
from pipeline.silver.prices import (
    _exclude_unsupported_markets,
    _normalize_incomplete_ohlc,
    _rescale_history_for_events,
    _verify_adj_close_post_publish,
)
from pipeline.silver_quality.rules.prices import check_prices


DAY = date(2026, 7, 8)


def _row(identifier, asset_type, **overrides):
    row = {
        "identifier": identifier,
        "source": "KRX",
        "trade_date": DAY,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "adj_close": 105.0,
        "volume": 10,
        "trading_value": 1_000,
        "shares": 1_000 if asset_type == "stock" else None,
        "market_cap": 105_000.0,
        "market": "KOSPI" if asset_type == "stock" else None,
        "asset_type": asset_type,
        "prev_diff": 5.0,
        "fluc_rate": 5.0,
    }
    row.update(overrides)
    return row


def _valid_prices(stock_overrides=None):
    return pd.DataFrame([
        _row("005930", "stock", **(stock_overrides or {})),
        _row("035720", "stock", market="KOSDAQ"),
        _row("1028", "index", shares=None, market=None),
        _row("2203", "index", shares=None, market=None),
    ])


def _failed(results, code):
    return next(r for r in results if r.rule_code == code).failed_count


def _action(**overrides):
    row = {
        "identifier": "005930",
        "event_type": "stock_split",
        "announcement_date": DAY,
        "effective_date": DAY,
        "match_window_days": 3,
        "expected_factor": None,
        "expects_price_adjustment": True,
        "confidence": "EXCHANGE_NOTICE",
        "rcept_no": "20260708000001",
        "report_name": "변경상장(액면분할)",
        "source": "DART_DISCLOSURE",
        "source_file": "fixture.json",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_zero_ohl_is_normalized_to_null_without_changing_other_values():
    frame = pd.DataFrame([{
        "open": 0,
        "high": 0.0,
        "low": 0,
        "close": 123.0,
        "volume": 10,
        "trading_value": 1_230,
        "market_cap": 123_000,
    }])
    normalized = _normalize_incomplete_ohlc(frame.copy())
    assert normalized[["open", "high", "low"]].isna().all(axis=None)
    assert normalized.loc[0, "close"] == 123.0
    assert normalized.loc[0, "volume"] == 10
    assert normalized.loc[0, "trading_value"] == 1_230
    assert normalized.loc[0, "market_cap"] == 123_000


def test_konex_is_explicitly_excluded():
    frame = pd.DataFrame([
        _row("005930", "stock"),
        _row("123456", "stock", market="KONEX"),
        _row("1028", "index", market=None),
    ])
    retained, detail = _exclude_unsupported_markets(frame)
    assert set(retained["identifier"]) == {"005930", "1028"}
    assert detail["row_count"] == 1
    assert detail["ticker_count"] == 1
    assert detail["markets"] == {"KONEX": 1}


def test_assets_without_supported_price_history_are_excluded():
    asset_frame = pd.DataFrame([
        {"natural_key": "005930", "asset_type": "stock"},
        {"natural_key": "123456", "asset_type": "stock"},
        {"natural_key": "1028", "asset_type": "index"},
    ])
    identifier_frame = pd.DataFrame([
        {"natural_key": "005930", "source": "KRX", "identifier": "005930"},
        {"natural_key": "005930", "source": "DART", "identifier": "00126380"},
        {"natural_key": "123456", "source": "KRX", "identifier": "123456"},
        {"natural_key": "1028", "source": "KRX", "identifier": "1028"},
    ])
    retained_assets, retained_identifiers = assets.restrict_to_price_universe(
        asset_frame,
        identifier_frame,
        {"005930"},
    )
    assert set(retained_assets["natural_key"]) == {"005930", "1028"}
    assert set(retained_identifiers["natural_key"]) == {"005930", "1028"}


def test_duplicate_price_blocks():
    frame = _valid_prices()
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    results = check_prices(frame, target_date=DAY)
    duplicate = next(r for r in results if r.rule_code == "COMMON_DUPLICATE_KEY")
    assert duplicate.blocks_publish
    assert duplicate.failed_count == 2


def test_suspended_stock_shape_is_allowed():
    frame = _valid_prices({
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "close": 100.0,
        "adj_close": 100.0,
        "volume": 0,
        "trading_value": 0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    results = check_prices(frame, target_date=DAY)
    assert _failed(results, "PRICE_OHLC_LOGIC") == 0
    warning = next(r for r in results if r.rule_code == "SOURCE_INCOMPLETE_OHLC")
    assert warning.failed_count == 0
    assert not warning.blocks_publish
    no_trade = next(r for r in results if r.rule_code == "SOURCE_NO_TRADE_OHLC")
    assert no_trade.status.value == "PASS"
    assert "observed_rows=1" in no_trade.actual


def test_partial_zero_ohlc_blocks():
    frame = _valid_prices({"open": 0.0})
    result = next(
        r for r in check_prices(frame, target_date=DAY)
        if r.rule_code == "PRICE_OHLC_LOGIC"
    )
    assert result.blocks_publish


def test_active_close_only_ohlc_is_warning_only():
    frame = _valid_prices({
        "open": None,
        "high": None,
        "low": None,
        "close": 100.0,
        "adj_close": 100.0,
        "volume": 123,
        "trading_value": 12_300,
        "market_cap": 100_000.0,
    })
    results = check_prices(frame, target_date=DAY)
    assert _failed(results, "PRICE_OHLC_LOGIC") == 0
    warning = next(r for r in results if r.rule_code == "SOURCE_INCOMPLETE_OHLC")
    assert warning.failed_count == 1
    assert not warning.blocks_publish


def test_index_market_cap_is_not_required():
    frame = _valid_prices()
    frame.loc[frame["asset_type"].eq("index"), "market_cap"] = None
    assert _failed(check_prices(frame, target_date=DAY), "PRICE_REQUIRED_POSITIVE") == 0


def test_stock_market_cap_is_required():
    frame = _valid_prices({"market_cap": None})
    result = next(
        r for r in check_prices(frame, target_date=DAY)
        if r.rule_code == "PRICE_REQUIRED_POSITIVE"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_corrupted_adj_close_blocks_full_series():
    frame = _valid_prices()
    frame.loc[frame["identifier"].eq("005930"), "adj_close"] = 999.0
    result = next(
        r for r in check_prices(frame)
        if r.rule_code == "ADJ_CLOSE_RECONCILIATION"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_missing_adjustment_source_fields_blocks():
    frame = _valid_prices()
    frame.loc[frame["identifier"].eq("005930"), "prev_diff"] = None
    result = next(
        r for r in check_prices(frame)
        if r.rule_code == "ADJ_CLOSE_SOURCE_FIELDS"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_price_spike_is_warning_only():
    frame = _valid_prices({
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 100.0,
        "adj_close": 100.0,
        "market_cap": 100_000.0,
        "prev_diff": 90.0,
        "fluc_rate": 900.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 10.0,
    }])
    result = next(
        r for r in check_prices(frame, target_date=DAY, history=history)
        if r.rule_code == "PRICE_RETURN_SPIKE"
    )
    assert result.failed_count == 1
    assert not result.blocks_publish


def test_corporate_action_is_not_a_return_or_scale_warning():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(),
    )
    assert _failed(results, "PRICE_RETURN_SPIKE") == 0
    assert _failed(results, "PRICE_SCALE_JUMP") == 0
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 0
    event = next(
        r for r in results
        if r.rule_code == "PRICE_ADJUSTMENT_FACTOR_CHANGE"
    )
    assert event.status.value == "PASS"
    assert "observed_events=1" in event.actual
    assert "dart_confirmed=1" in event.actual


def test_unconfirmed_krx_adjustment_is_warning():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 1
    assert _failed(results, "PRICE_SCALE_JUMP") == 1


def test_dart_factor_mismatch_is_warning():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="capital_reduction",
            expected_factor=8.0,
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "CORPORATE_ACTION_FACTOR_MISMATCH") == 1


def test_dart_action_without_krx_adjustment_is_warning():
    frame = _valid_prices()
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="bonus_issue",
            expected_factor=0.5,
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_ACTION_WITHOUT_KRX_ADJUSTMENT") == 1


def test_dart_action_accepts_krx_adjustment_anywhere_in_holiday_window():
    frame = _valid_prices()
    history = pd.DataFrame([
        {
            "identifier": "005930",
            "trade_date": date(2026, 7, 6),
            "close": 100.0,
            "adj_close": 50.0,
            "market": "KOSPI",
            "asset_type": "stock",
            "prev_diff": 0.0,
        },
        {
            "identifier": "005930",
            "trade_date": date(2026, 7, 7),
            "close": 50.0,
            "adj_close": 50.0,
            "market": "KOSPI",
            "asset_type": "stock",
            "prev_diff": 0.0,
        },
    ])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="bonus_issue",
            effective_date=date(2026, 7, 5),
            expected_factor=0.5,
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_ACTION_WITHOUT_KRX_ADJUSTMENT") == 0


def test_wrong_daily_adj_close_continuity_blocks():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 900.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    result = next(
        r for r in check_prices(
            frame,
            target_date=DAY,
            history=history,
        )
        if r.rule_code == "ADJ_CLOSE_RETURN_CONTINUITY"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_small_source_adjustment_is_applied_to_daily_continuity():
    frame = _valid_prices({
        "open": 101.0,
        "high": 101.0,
        "low": 101.0,
        "close": 101.0,
        "adj_close": 101.0,
        "market_cap": 101_000.0,
        "prev_diff": 0.9,
        "fluc_rate": 0.8991,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "ADJ_CLOSE_RETURN_CONTINUITY") == 0
    event = next(
        r for r in results
        if r.rule_code == "PRICE_ADJUSTMENT_FACTOR_CHANGE"
    )
    assert "observed_events=0" in event.actual


class _RescaleCursor:
    def __init__(self):
        self.query = ""
        self.rowcount = 0
        self.update_params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.query = query
        if query.startswith("UPDATE"):
            self.update_params = params
            self.rowcount = 1

    def fetchone(self):
        return (100.0, 100.0)


class _RescaleConnection:
    def __init__(self):
        self._cursor = _RescaleCursor()

    def cursor(self):
        return self._cursor


def test_small_source_adjustment_rescales_published_history():
    connection = _RescaleConnection()
    stock = pd.DataFrame([{
        "asset_id": 1,
        "close": 101.0,
        "prev_diff": 0.9,
    }])
    fixed = _rescale_history_for_events(connection, stock, DAY)
    assert fixed == 1
    factor, asset_id, target_date = connection._cursor.update_params
    assert abs(factor - 1.001) < 1e-12
    assert asset_id == 1
    assert target_date == DAY


class _FakeCursor:
    def __init__(self, current_adj_close):
        self.current_adj_close = current_adj_close
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _params):
        self.query = query

    def fetchall(self):
        if "trade_date=%s" in self.query:
            return [(1, 1_000.0, self.current_adj_close)]
        if "trade_date < %s" in self.query:
            return [(1, 100.0, 1_000.0)]
        return []


class _FakeConnection:
    def __init__(self, current_adj_close):
        self.current_adj_close = current_adj_close

    def cursor(self):
        return _FakeCursor(self.current_adj_close)


def test_post_publish_adj_close_verification():
    candidates = pd.DataFrame([{
        "asset_id": 1,
        "asset_type": "stock",
        "prev_diff": 0.0,
    }])
    _verify_adj_close_post_publish(
        _FakeConnection(1_000.0),
        candidates,
        DAY,
    )


def test_post_publish_adj_close_verification_rejects_mismatch():
    candidates = pd.DataFrame([{
        "asset_id": 1,
        "asset_type": "stock",
        "prev_diff": 0.0,
    }])
    try:
        _verify_adj_close_post_publish(
            _FakeConnection(900.0),
            candidates,
            DAY,
        )
    except RuntimeError as exc:
        assert "ADJ_CLOSE_POST_PUBLISH failed" in str(exc)
    else:
        raise AssertionError("expected post-publish verification failure")
