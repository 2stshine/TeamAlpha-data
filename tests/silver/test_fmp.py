import csv
import io
import json
from datetime import date
from pathlib import Path

import pytest

from pipeline.silver import fmp
from pipeline.silver_quality.models import CheckStatus
from pipeline.silver_quality.rules.fmp import check_fmp


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

    bundle = fmp.build_candidates(str(tmp_path), date(2020, 8, 28))

    assert set(bundle.assets["natural_key"]) == {"FMP:AAPL"}
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
    assert dividend["currency"] == "USD"


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

    bundle = fmp.build_candidates(str(tmp_path), date(2026, 7, 31))

    assert list(bundle.prices["identifier"]) == ["NEW"]
    assert list(bundle.prices["natural_key"]) == ["FMP:NEW"]
    assert not any(result.status == CheckStatus.FAIL for result in check_fmp(bundle))


def test_usdkrw_is_an_fx_price_asset(tmp_path):
    path = tmp_path / "fx/fmp/pair=USDKRW/date=2026-08-03/response.json"
    _write(path, json.dumps([{
        "symbol": "USDKRW", "date": "2026-08-03", "open": 1380.1,
        "high": 1390.2, "low": 1378.5, "close": 1388.4,
        "vwap": 1386.3, "volume": 0,
    }]).encode())
    _manifest(path)

    assets, identifiers, prices, _ = fmp.prepare_fx(
        str(tmp_path), target_date=date(2026, 8, 3),
    )

    assert assets.iloc[0]["asset_type"] == "fx"
    assert identifiers.iloc[0]["identifier_type"] == "fx_pair"
    assert prices.iloc[0]["close"] == pytest.approx(1388.4)
    assert prices.iloc[0]["currency"] == "KRW"
