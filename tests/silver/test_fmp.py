import csv
import io
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline.silver import fmp
from pipeline.fmp_commodities import COMMODITY_SPECS
from pipeline.silver_quality.models import CheckStatus
from pipeline.silver_quality.rules.fmp import check_fmp


class _ReusedTickerCursor:
    def __init__(self):
        self._row = None
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self.queries.append((normalized, params))
        if normalized.startswith("SELECT asset_id FROM asset_identifier"):
            # The database contains only a closed historical COIN episode.
            # The fixed lookup must not accept it for a current candidate.
            self._row = None if "valid_to IS NULL" in normalized else (45925,)
        elif normalized.startswith("INSERT INTO asset("):
            self._row = (90001,)
        else:
            self._row = None

    def fetchone(self):
        return self._row


class _ReusedTickerConnection:
    def __init__(self):
        self.cur = _ReusedTickerCursor()

    def cursor(self):
        return self.cur


class _TickerChangeCursor:
    def __init__(self):
        self._row = None
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self.queries.append((normalized, params))
        self._row = None
        if normalized.startswith(
            "SELECT asset_id,valid_from FROM asset_identifier"
        ):
            if params[1] == "JAB":
                self._row = (77, date(2026, 8, 5))
        elif normalized.startswith("SELECT asset_id FROM asset_identifier"):
            if "valid_from=%s AND valid_to=%s" in normalized:
                self._row = None
            else:
                identifier = params[-1]
                if identifier in {"JAB", "KYG500041036"}:
                    self._row = (77,)
        elif normalized.startswith("INSERT INTO asset("):
            raise AssertionError("validated ticker change created a new asset")

    def fetchone(self):
        return self._row


class _TickerChangeConnection:
    def __init__(self):
        self.cur = _TickerChangeCursor()

    def cursor(self):
        return self.cur


def _write(path: Path, payload: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _csv(rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode()


def _manifest(path: Path):
    _write(
        path.with_name("manifest.json"),
        json.dumps({"received_at": "2026-08-04T01:00:00+00:00"}).encode(),
    )


def _universe(root: Path):
    rows = [
        {
            "symbol": "AAPL", "companyName": "Apple Inc.",
            "exchange": "NASDAQ", "currency": "USD", "country": "US",
            "isEtf": False, "isFund": False, "isAdr": False,
            "industry": "Consumer Electronics", "ipoDate": "1980-12-12",
            "cik": "0000320193", "cusip": "037833100", "isin": "US0378331005",
        },
        {
            "symbol": "SPY", "companyName": "SPDR S&P 500 ETF Trust",
            "exchange": "NYSE", "currency": "USD", "country": "US",
            "isEtf": True, "isFund": False, "isAdr": False,
            "industry": "ETF", "ipoDate": "1993-01-22", "cik": "",
            "cusip": "", "isin": "",
        },
        {
            "symbol": "FUNDX", "companyName": "Example Fund",
            "exchange": "NASDAQ", "currency": "USD", "country": "US",
            "isEtf": False, "isFund": True, "isAdr": False,
            "industry": "Fund", "ipoDate": "2020-01-01", "cik": "",
            "cusip": "", "isin": "",
        },
        {
            "symbol": "ACMEW", "companyName": "Acme Warrants",
            "exchange": "NASDAQ", "currency": "USD", "country": "US",
            "isEtf": False, "isFund": False, "isAdr": False,
            "industry": "Shell Companies", "ipoDate": "2024-01-01", "cik": "",
            "cusip": "", "isin": "",
        },
    ]
    _write(
        root / "stock/fmp/universe/profile-bulk/snapshot_date=2020-08-28/part=0/response.csv",
        _csv(rows),
    )
    _commodity_list(root, "2020-08-28")


def _commodity_list(root: Path, snapshot: str):
    _write(
        root
        / f"commodities/fmp/list/snapshot_date={snapshot}/response.json",
        json.dumps([
            {
                "symbol": spec.symbol,
                "name": f"{spec.name} Futures",
                "currency": spec.raw_currency,
                "tradeMonth": "Dec",
            }
            for spec in COMMODITY_SPECS
        ]).encode(),
    )


def test_current_ticker_reuse_does_not_merge_with_closed_historical_asset():
    assets = pd.DataFrame([{
        "natural_key": "FMP:COIN",
        "name": "Coinbase Global, Inc.",
        "asset_type": "stock",
        "instrument_type": "common_stock",
        "exchange": "NASDAQ",
        "currency": "USD",
        "country_code": "US",
        "base_currency": "USD",
        "listed_from": date(2021, 4, 14),
        "listed_to": None,
    }])
    identifiers = pd.DataFrame([{
        "natural_key": "FMP:COIN",
        "source": "FMP",
        "identifier": "COIN",
        "identifier_type": "ticker",
        "valid_from": date(2021, 4, 14),
        "valid_to": None,
    }])
    conn = _ReusedTickerConnection()

    mapping = fmp.publish_assets(conn, assets, identifiers, "quality-run")

    assert mapping["FMP:COIN"] == 90001
    assert mapping["COIN"] == 90001
    lookup = next(
        query for query, _ in conn.cur.queries
        if query.startswith("SELECT asset_id FROM asset_identifier")
    )
    assert "valid_to IS NULL" in lookup


def test_declared_ticker_change_reuses_and_closes_current_old_ticker():
    assets = pd.DataFrame([{
        "natural_key": "FMP:ATLQ",
        "name": "JAB Acquisition Corp. I Class A",
        "asset_type": "stock",
        "instrument_type": "common_stock",
        "exchange": "NASDAQ",
        "currency": "USD",
        "country_code": "US",
        "base_currency": "USD",
        "listed_from": date(2026, 8, 31),
        "listed_to": None,
    }])
    identifiers = pd.DataFrame([
        {
            "natural_key": "FMP:ATLQ", "source": "FMP",
            "identifier": "JAB", "identifier_type": "ticker",
            "valid_from": date.min, "valid_to": date(2026, 8, 30),
        },
        {
            "natural_key": "FMP:ATLQ", "source": "FMP",
            "identifier": "ATLQ", "identifier_type": "ticker",
            "valid_from": date(2026, 8, 31), "valid_to": None,
        },
        {
            "natural_key": "FMP:ATLQ", "source": "FMP",
            "identifier": "KYG500041036", "identifier_type": "isin",
            "valid_from": date(2026, 8, 31), "valid_to": None,
        },
    ])
    conn = _TickerChangeConnection()

    mapping = fmp.publish_assets(conn, assets, identifiers, "quality-run")

    assert mapping["FMP:ATLQ"] == 77
    assert mapping["JAB"] == 77
    assert mapping["ATLQ"] == 77
    assert any(
        query.startswith("UPDATE asset_identifier SET valid_to=%s")
        and params[0] == date(2026, 8, 30)
        and params[3] == "JAB"
        for query, params in conn.cur.queries
    )


def test_symbol_change_noop_does_not_bound_current_ticker(tmp_path):
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-08-10/part=0/response.csv",
        _csv([{
            "symbol": "GOAI", "companyName": "Eva Live Inc.",
            "exchange": "NASDAQ", "currency": "USD", "country": "US",
            "isEtf": False, "isFund": False, "isAdr": False,
            "isActivelyTrading": True, "industry": "Technology",
            "ipoDate": "2023-01-01", "cik": "", "cusip": "", "isin": "",
        }]),
    )
    _write(
        tmp_path
        / "stock/fmp/universe/symbol-change/snapshot_date=2026-08-10/response.json",
        json.dumps([{
            "date": "1969-12-31", "companyName": "Eva Live Inc.",
            "oldSymbol": "GOAI", "newSymbol": "GOAI",
        }]).encode(),
    )

    _, identifiers, _ = fmp.prepare_universe(
        str(tmp_path), target_date=date(2026, 8, 10),
    )

    ticker = identifiers[
        identifiers["identifier_type"].eq("ticker")
        & identifiers["identifier"].eq("GOAI")
    ]
    assert len(ticker) == 1
    assert pd.isna(ticker.iloc[0]["valid_to"])
    assert ticker.iloc[0]["valid_from"] == date(2023, 1, 1)


def test_silver_filters_instruments_but_keeps_bronze_and_maps_price_semantics(tmp_path):
    _universe(tmp_path)
    universe_path = (
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2020-08-28/part=0/response.csv"
    )
    original_universe = universe_path.read_bytes()
    price_path = tmp_path / "stock/fmp/eod-bulk/date=2020-08-28/response.csv"
    _write(price_path, _csv([
        {
            "symbol": "AAPL", "date": "2020-08-28", "open": 126.0125,
            "low": 124.725, "high": 126.4425, "close": 124.81,
            "adjClose": 121.05, "volume": 187630000,
        },
        {
            "symbol": "SPY", "date": "2020-08-28", "open": 350,
            "low": 349, "high": 351, "close": 350.5,
            "adjClose": 330, "volume": 100,
        },
    ]))
    _manifest(price_path)
    _write(
        tmp_path / "corporate_actions/fmp/splits/year=2020/from=2020-01-01/to=2020-12-31/response.json",
        json.dumps([
            {"symbol": "AAPL", "date": "2020-08-31", "numerator": 4, "denominator": 1}
        ]).encode(),
    )
    _write(
        tmp_path / "corporate_actions/fmp/splits/snapshot_date=2020-08-28/response.json",
        json.dumps([
            {"symbol": "AAPL", "date": "2020-08-31", "numerator": 4, "denominator": 1}
        ]).encode(),
    )

    bundle = fmp.build_candidates(str(tmp_path), date(2020, 8, 28))

    assert set(
        bundle.assets.loc[
            bundle.assets["asset_type"].eq("stock"), "natural_key"
        ]
    ) == {"FMP:AAPL"}
    assert bundle.assets["asset_type"].eq("commodity").sum() == 28
    assert bundle.stats["asset"]["excluded_by_reason"] == {
        "NON_EQUITY_NAME": 1,
        "FUND": 1,
        "ETF": 1,
    }
    assert universe_path.read_bytes() == original_universe
    assert len(bundle.prices) == 1
    row = bundle.prices.iloc[0]
    assert row["close"] == pytest.approx(499.24)
    assert row["adj_close"] == pytest.approx(124.81)
    assert row["total_return_close"] == pytest.approx(121.05)
    assert row["available_at"].isoformat() == "2026-08-04T01:00:00+00:00"
    assert not any(result.status == CheckStatus.FAIL for result in check_fmp(bundle))


def test_financials_and_actions_are_long_pit_candidates(tmp_path):
    _universe(tmp_path)
    _write(
        tmp_path / "financials/fmp/income/year=2025/period=FY/response.csv",
        _csv([{
            "date": "2025-09-27", "symbol": "AAPL", "reportedCurrency": "USD",
            "filingDate": "2025-10-31", "acceptedDate": "2025-10-31T18:00:00Z",
            "fiscalYear": "2025", "period": "FY", "revenue": 100,
            "netIncome": 20, "eps": 2.5, "weightedAverageShsOut": 10,
        }]),
    )
    _write(
        tmp_path / "financials/fmp/balance/year=2025/period=FY/response.csv",
        _csv([{
            "date": "2025-09-27", "symbol": "AAPL", "reportedCurrency": "USD",
            "filingDate": "2025-10-31", "acceptedDate": "2025-10-31T18:00:00Z",
            "fiscalYear": "2025", "period": "FY", "totalAssets": 200,
            "totalLiabilities": 120, "totalStockholdersEquity": 80,
        }]),
    )
    _write(
        tmp_path / "corporate_actions/fmp/dividends/year=2025/from=2025-01-01/to=2025-12-31/response.json",
        json.dumps([{
            "symbol": "AAPL", "date": "2025-08-11", "dividend": 0.25,
            "adjDividend": 0.24, "frequency": "Quarterly",
            "declarationDate": "2025-07-31", "recordDate": "2025-08-11",
            "paymentDate": "2025-08-14",
        }]).encode(),
    )

    assets, identifiers, _ = fmp.prepare_universe(str(tmp_path))
    fundamentals, _ = fmp.prepare_fundamentals(str(tmp_path), identifiers)
    actions, _ = fmp.prepare_actions(str(tmp_path), identifiers)

    assert set(fundamentals["statement_type"]) == {"IS", "BS"}
    assert set(fundamentals["data_basis"]) == {"STANDARDIZED"}
    eps = fundamentals[fundamentals["metric"].eq("eps")].iloc[0]
    assert eps["unit_type"] == "per_share"
    assert eps["available_at"].isoformat() == "2025-10-31T18:00:00+00:00"
    dividend = actions.iloc[0]
    assert dividend["action_type"] == "cash_dividend"
    assert dividend["cash_amount"] == pytest.approx(0.25)
    assert dividend["adjusted_cash_amount"] == pytest.approx(0.24)
    assert dividend["frequency"] == "Quarterly"
    assert dividend["currency"] == "USD"


def test_prices_exclude_invalid_ohlc_and_reduce_duplicate_keys(tmp_path):
    _universe(tmp_path)
    price_path = tmp_path / "stock/fmp/eod-bulk/date=2020-08-28/response.csv"
    _write(price_path, _csv([
        {
            "symbol": "AAPL", "date": "2020-08-28", "open": 100,
            "low": 90, "high": 110, "close": 105, "adjClose": 104,
            "volume": 100,
        },
        {
            "symbol": "AAPL", "date": "2020-08-28", "open": 100,
            "low": 90, "high": 110, "close": 105, "adjClose": 104,
            "volume": 200,
        },
        {
            "symbol": "AAPL", "date": "2020-08-28", "open": 100,
            "low": 90, "high": 99, "close": 105, "adjClose": 104,
            "volume": 300,
        },
    ]))
    _manifest(price_path)
    _commodity_list(tmp_path, "2026-07-31")
    assets, identifiers, _ = fmp.prepare_universe(str(tmp_path))
    frame, stats = fmp.prepare_prices(
        str(tmp_path), assets, identifiers, year=2020,
    )
    assert len(frame) == 1
    assert frame.iloc[0]["volume"] == 200
    assert stats["invalid_ohlc_excluded"]["row_count"] == 1
    assert stats["duplicate_price_rows_removed"]["row_count"] == 1


def test_prices_exclude_global_bulk_rows_outside_xnys_sessions(tmp_path):
    _universe(tmp_path)
    holiday_path = tmp_path / "stock/fmp/eod-bulk/date=2020-09-07/response.csv"
    _write(holiday_path, _csv([{
        "symbol": "AAPL", "date": "2020-09-07", "open": 120,
        "low": 119, "high": 121, "close": 120, "adjClose": 119,
        "volume": 100,
    }]))
    _manifest(holiday_path)

    assets, identifiers, _ = fmp.prepare_universe(str(tmp_path))
    frame, stats = fmp.prepare_prices(
        str(tmp_path), assets, identifiers, year=2020,
    )

    assert frame.empty
    assert stats["non_session_rows_excluded"]["row_count"] == 1
    assert stats["non_session_rows_excluded"]["samples"][0]["symbol"] == "AAPL"


def test_commodities_normalize_usx_and_preserve_negative_futures_ohlc(tmp_path):
    _commodity_list(tmp_path, "2020-04-20")
    corn = (
        tmp_path
        / "commodities/fmp/eod/symbol=ZCUSX/"
        "from=2020-01-01/to=2020-12-31/response.json"
    )
    crude = (
        tmp_path
        / "commodities/fmp/eod/symbol=CLUSD/"
        "from=2020-01-01/to=2020-12-31/response.json"
    )
    _write(corn, json.dumps([{
        "symbol": "ZCUSX", "date": "2020-04-20",
        "open": 462.5, "high": 470, "low": 460, "close": 465,
        "vwap": 464, "volume": 100,
    }]).encode())
    _write(crude, json.dumps([{
        "symbol": "CLUSD", "date": "2020-04-20",
        "open": 17.73, "high": 17.85, "low": -40.32, "close": 17.73,
        "vwap": 1.0, "volume": 200,
    }]).encode())
    _manifest(corn)
    _manifest(crude)

    assets, identifiers, prices, stats = fmp.prepare_commodities(
        str(tmp_path), year=2020,
    )

    assert len(assets) == 28
    assert set(identifiers["identifier_type"]) == {"commodity_symbol"}
    corn_row = prices[prices["identifier"].eq("ZCUSX")].iloc[0]
    assert corn_row["close"] == pytest.approx(4.65)
    assert corn_row["vwap"] == pytest.approx(4.64)
    assert corn_row["currency"] == "USD"
    assert corn_row["price_unit"] == "USD/bushel"
    crude_row = prices[prices["identifier"].eq("CLUSD")].iloc[0]
    assert crude_row["low"] == pytest.approx(-40.32)
    assert crude_row["adj_close"] == crude_row["close"]
    assert pd.isna(crude_row["total_return_close"])
    bundle = fmp.CandidateBundle(
        assets=assets,
        identifiers=identifiers,
        prices=prices,
        stats={
            "commodity": stats,
            "_source_scope": "commodity",
        },
    )
    assert not any(result.blocks_publish for result in check_fmp(bundle))


def test_commodities_keep_sunday_sessions_and_exclude_saturday_rows(tmp_path):
    _commodity_list(tmp_path, "2026-01-05")
    path = (
        tmp_path
        / "commodities/fmp/eod/symbol=ZRUSD/"
        "from=2021-01-01/to=2026-12-31/response.json"
    )
    _write(path, json.dumps([
        {
            "symbol": "ZRUSD", "date": "2021-11-27",
            "open": 14.45, "high": 14.45, "low": 14.09,
            "close": 14.29, "volume": 10,
        },
        {
            "symbol": "ZRUSD", "date": "2026-01-04",
            "open": 10.0, "high": 10.2, "low": 9.9,
            "close": 10.1, "volume": 20,
        },
    ]).encode())
    _manifest(path)

    assets, identifiers, prices, stats = fmp.prepare_commodities(
        str(tmp_path),
    )

    assert prices["trade_date"].tolist() == [date(2026, 1, 4)]
    excluded = stats["non_session_rows_excluded"]
    assert excluded["row_count"] == 1
    assert excluded["samples"][0]["trade_date"] == date(2021, 11, 27)
    bundle = fmp.CandidateBundle(
        assets=assets,
        identifiers=identifiers,
        prices=prices,
        stats={"commodity": stats, "_source_scope": "commodity"},
    )
    results = check_fmp(bundle)
    assert not any(result.blocks_publish for result in results)
    modified = next(
        result for result in results
        if result.rule_code == "FMP_COMMODITY_NON_SESSION_EXCLUDED"
    )
    assert modified.actual == "affected_rows=1"


def test_monday_daily_commodity_candidate_includes_sunday_session(tmp_path):
    _commodity_list(tmp_path, "2026-08-03")
    path = (
        tmp_path
        / "commodities/fmp/eod/symbol=ZRUSD/"
        "from=2026-08-02/to=2026-08-03/response.json"
    )
    _write(path, json.dumps([
        {
            "symbol": "ZRUSD", "date": "2026-08-02",
            "open": 10.0, "high": 10.2, "low": 9.9,
            "close": 10.1, "volume": 20,
        },
        {
            "symbol": "ZRUSD", "date": "2026-08-03",
            "open": 10.1, "high": 10.3, "low": 10.0,
            "close": 10.2, "volume": 30,
        },
    ]).encode())
    _manifest(path)

    _, _, prices, _ = fmp.prepare_commodities(
        str(tmp_path), target_date=date(2026, 8, 3),
    )

    assert prices["trade_date"].tolist() == [
        date(2026, 8, 2), date(2026, 8, 3),
    ]


def test_universe_excludes_same_issuer_nasdaq_suffix_instruments(tmp_path):
    rows = []
    for symbol, name in (
        ("BBLG", "Bone Biologics Corporation"),
        ("BBLGW", "Bone Biologics Corp"),
        ("GMBL", "Esports Entertainment Group Inc"),
        ("GMBLW", "Esports Entertainment Group, Inc. Common Stock"),
        ("GMBLZ", "Esports Entertainment Group Inc"),
        ("ACME", "Acme Holdings"),
        ("ACMEU", "Acme Holdings Unit"),
        # A suffix-looking ticker without a same-issuer base must survive.
        ("AROW", "Arrow Financial Corporation"),
    ):
        rows.append({
            "symbol": symbol, "companyName": name, "exchange": "NASDAQ",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Financial Services", "ipoDate": "2020-01-01",
            "cik": "", "cusip": "", "isin": "",
        })
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-07-31/part=0/response.csv",
        _csv(rows),
    )

    assets, identifiers, stats = fmp.prepare_universe(str(tmp_path))

    assert set(assets["natural_key"]) == {
        "FMP:BBLG", "FMP:GMBL", "FMP:ACME", "FMP:AROW",
    }
    assert stats["excluded_by_reason"] == {
        "WARRANT_SUFFIX": 2,
        "DERIVATIVE_SUFFIX": 1,
        "NON_EQUITY_NAME": 1,
    }
    assert set(identifiers["identifier"]) >= {"BBLG", "GMBL", "ACME", "AROW"}


def test_symbol_change_does_not_admit_unit_ticker_episode(tmp_path):
    rows = [{
        "symbol": "FG", "companyName": "F&G Annuities & Life, Inc.",
        "exchange": "NYSE", "currency": "USD", "country": "US",
        "isEtf": False, "isFund": False, "isAdr": False,
        "isActivelyTrading": True, "industry": "Insurance",
        "ipoDate": "2022-12-01", "cik": "", "cusip": "", "isin": "",
    }]
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-07-31/part=0/response.csv",
        _csv(rows),
    )
    _write(
        tmp_path
        / "stock/fmp/universe/symbol-change/snapshot_date=2026-07-31/response.json",
        json.dumps([
            {
                "companyName": "Fgl Holdings", "oldSymbol": "CFCO",
                "newSymbol": "FG", "date": "2018-02-14",
            },
            {
                "companyName": "Fgl Holdings", "oldSymbol": "CFCOU",
                "newSymbol": "FG", "date": "2016-07-25",
            },
        ]).encode(),
    )

    _, identifiers, stats = fmp.prepare_universe(str(tmp_path))

    tickers = set(
        identifiers.loc[identifiers["identifier_type"].eq("ticker"), "identifier"]
    )
    assert tickers == {"FG", "CFCO"}
    assert stats["excluded_symbol_change_identifier_count"] == 1


def test_universe_excludes_stale_profiles_and_ambiguous_security_ids(tmp_path):
    rows = [
        {
            "symbol": "NEW", "companyName": "New Corp", "exchange": "NASDAQ",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Software", "ipoDate": "2020-01-01",
            "cik": "1", "cusip": "111111111", "isin": "US1111111111",
        },
        {
            "symbol": "OLD", "companyName": "Old Corp", "exchange": "NASDAQ",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": False,
            "industry": "Software", "ipoDate": "2010-01-01",
            "cik": "1", "cusip": "111111111", "isin": "US1111111111",
        },
        {
            "symbol": "DUPEA", "companyName": "Dupe A", "exchange": "NYSE",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Industrials", "ipoDate": "2021-01-01",
            "cik": "2", "cusip": "222222222", "isin": "US2222222222",
        },
        {
            "symbol": "DUPEB", "companyName": "Dupe B", "exchange": "NYSE",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Industrials", "ipoDate": "2022-01-01",
            "cik": "3", "cusip": "222222222", "isin": "US2222222222",
        },
    ]
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-07-31/part=0/response.csv",
        _csv(rows),
    )

    assets, identifiers, stats = fmp.prepare_universe(
        str(tmp_path), date(2026, 7, 31),
    )

    assert set(assets["natural_key"]) == {"FMP:NEW", "FMP:DUPEA", "FMP:DUPEB"}
    assert stats["excluded_by_reason"]["INACTIVE_UNDATED"] == 1
    assert stats["ambiguous_identifier_rows_removed"] == 4
    assert set(
        identifiers.loc[
            identifiers["identifier_type"].eq("ticker"), "identifier"
        ]
    ) == {"NEW", "DUPEA", "DUPEB"}
    assert not identifiers.duplicated(
        ["source", "identifier_type", "identifier"], keep=False,
    ).any()


def test_prices_respect_ticker_validity_after_symbol_change(tmp_path):
    rows = [
        {
            "symbol": "NEW", "companyName": "Renamed Corp", "exchange": "NASDAQ",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Software", "ipoDate": "2020-01-01",
            "cik": "1", "cusip": "111111111", "isin": "US1111111111",
        },
        {
            "symbol": "OLD", "companyName": "Renamed Corp", "exchange": "NASDAQ",
            "currency": "USD", "country": "US", "isEtf": False,
            "isFund": False, "isAdr": False, "isActivelyTrading": True,
            "industry": "Software", "ipoDate": "2020-01-01",
            "cik": "1", "cusip": "111111111", "isin": "US1111111111",
        },
    ]
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-07-31/part=0/response.csv",
        _csv(rows),
    )
    _write(
        tmp_path
        / "stock/fmp/universe/symbol-change/snapshot_date=2026-07-31/response.json",
        json.dumps([{
            "oldSymbol": "OLD", "newSymbol": "NEW", "date": "2026-06-01",
        }]).encode(),
    )
    price_path = tmp_path / "stock/fmp/eod-bulk/date=2026-07-31/response.csv"
    _write(price_path, _csv([
        {
            "symbol": "OLD", "date": "2026-07-31", "open": 10,
            "high": 11, "low": 9, "close": 10, "adjClose": 10,
            "volume": 100,
        },
        {
            "symbol": "NEW", "date": "2026-07-31", "open": 20,
            "high": 21, "low": 19, "close": 20, "adjClose": 20,
            "volume": 200,
        },
    ]))
    _manifest(price_path)
    _commodity_list(tmp_path, "2026-07-31")

    bundle = fmp.build_candidates(str(tmp_path), date(2026, 7, 31))

    assert list(bundle.prices["identifier"]) == ["NEW"]
    assert list(bundle.prices["natural_key"]) == ["FMP:NEW"]
    assert not any(result.status == CheckStatus.FAIL for result in check_fmp(bundle))


def test_fundamentals_deduplicate_old_and_new_tickers_by_asset(tmp_path):
    rows = [
        {
            "symbol": symbol, "companyName": "Renamed Corp",
            "exchange": "NASDAQ", "currency": "USD", "country": "US",
            "isEtf": False, "isFund": False, "isAdr": False,
            "isActivelyTrading": True, "industry": "Software",
            "ipoDate": "2010-01-01", "cik": "1",
            "cusip": "111111111", "isin": "US1111111111",
        }
        for symbol in ("OLD", "NEW")
    ]
    _write(
        tmp_path
        / "stock/fmp/universe/profile-bulk/snapshot_date=2026-07-31/part=0/response.csv",
        _csv(rows),
    )
    _write(
        tmp_path
        / "stock/fmp/universe/symbol-change/snapshot_date=2026-07-31/response.json",
        json.dumps([{
            "oldSymbol": "OLD", "newSymbol": "NEW", "date": "2026-01-01",
        }]).encode(),
    )
    statement = {
        "date": "2015-12-31", "reportedCurrency": "USD",
        "filingDate": "2016-03-01", "acceptedDate": "2016-03-01T12:00:00Z",
        "period": "FY", "revenue": 100,
    }
    _write(
        tmp_path / "financials/fmp/income/year=2015/period=FY/response.csv",
        _csv([{**statement, "symbol": "OLD"}, {**statement, "symbol": "NEW"}]),
    )

    _, identifiers, _ = fmp.prepare_universe(str(tmp_path))
    fundamentals, stats = fmp.prepare_fundamentals(
        str(tmp_path), identifiers, year=2015,
    )

    assert len(fundamentals) == 1
    assert fundamentals.iloc[0]["natural_key"] == "FMP:NEW"
    assert stats["duplicate_rows_removed"] == 1


def test_fundamentals_exclude_values_without_pit_availability(tmp_path):
    _universe(tmp_path)
    _write(
        tmp_path / "financials/fmp/income/year=2018/period=FY/response.csv",
        _csv([{
            "date": "2018-12-31", "symbol": "AAPL",
            "reportedCurrency": "USD", "filingDate": "",
            "acceptedDate": "", "period": "FY", "revenue": 100,
            "netIncome": 20,
        }]),
    )

    _, identifiers, _ = fmp.prepare_universe(str(tmp_path))
    fundamentals, stats = fmp.prepare_fundamentals(
        str(tmp_path), identifiers, year=2018,
    )

    assert fundamentals.empty
    assert stats["missing_available_at_values_excluded"] == 2


def test_usdkrw_is_an_fx_price_asset(tmp_path):
    path = tmp_path / "fx/fmp/pair=USDKRW/date=2026-08-03/response.json"
    _write(path, json.dumps([{
        "symbol": "USDKRW", "date": "2026-08-03", "open": 1380.1,
        "high": 1390.2, "low": 1378.5, "close": 1388.4,
        "vwap": 1386.3, "volume": 0,
    }]).encode())
    _manifest(path)
    duplicate_path = (
        tmp_path / "fx/fmp/pair=USDKRW/from=2015/to=2026/response.json"
    )
    _write(duplicate_path, json.dumps([{
        "symbol": "USDKRW", "date": "2026-08-03", "open": 1380.1,
        "high": 1390.2, "low": 1378.5, "close": 1388.4,
        "vwap": 1386.3, "volume": 0,
    }]).encode())
    _manifest(duplicate_path)

    assets, identifiers, prices, _ = fmp.prepare_fx(
        str(tmp_path), target_date=date(2026, 8, 3),
    )

    assert assets.iloc[0]["asset_type"] == "fx"
    assert len(prices) == 1
    assert identifiers.iloc[0]["identifier_type"] == "fx_pair"
    assert prices.iloc[0]["close"] == pytest.approx(1388.4)
    assert prices.iloc[0]["currency"] == "KRW"


def test_usdkrw_excludes_invalid_ohlc_rows(tmp_path):
    path = tmp_path / "fx/fmp/pair=USDKRW/from=2015/to=2026/response.json"
    _write(path, json.dumps([{
        "symbol": "USDKRW", "date": "2025-09-14", "open": 1393.69995,
        "high": 1392.88, "low": 1384.18994, "close": 1393.69995,
        "volume": 0,
    }]).encode())
    _manifest(path)

    assets, identifiers, prices, stats = fmp.prepare_fx(
        str(tmp_path), year=2025,
    )

    assert assets.empty
    assert identifiers.empty
    assert prices.empty
    assert stats["invalid_ohlc_excluded"]["row_count"] == 1


def test_delisted_union_across_snapshots_not_shadowed_by_partial(tmp_path):
    # A partial recent delisted snapshot (1 page) must not hide an earlier
    # complete one (many pages). _all_snapshot_files unions across snapshots;
    # _latest_snapshot_files would return only the latest (partial) snapshot.
    root = tmp_path / "stock" / "fmp" / "universe" / "delisted"
    full = root / "snapshot_date=2026-08-04"
    for page in range(3):
        d = full / f"page={page}"
        d.mkdir(parents=True)
        (d / "response.json").write_text("[]", encoding="utf-8")
    partial = root / "snapshot_date=2026-08-05" / "page=0"
    partial.mkdir(parents=True)
    (partial / "response.json").write_text("[]", encoding="utf-8")

    pattern = str(root / "snapshot_date=*/page=*/response.*")
    union = fmp._all_snapshot_files(pattern, None)
    latest = fmp._latest_snapshot_files(pattern, None)
    assert len(union) == 4      # 3 full pages + 1 partial page
    assert len(latest) == 1     # old behavior: only the partial latest snapshot
    # target_date filtering still applies
    assert fmp._all_snapshot_files(pattern, date(2026, 8, 4)) == sorted(
        str(full / f"page={p}" / "response.json") for p in range(3)
    )
