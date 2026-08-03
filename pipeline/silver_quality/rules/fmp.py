"""Source-aware quality gates for FMP Silver candidates."""
from __future__ import annotations

import pandas as pd

from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.rules.common import (
    duplicate_keys,
    finite_numbers,
    null_keys,
    result,
)


def check_fmp(bundle: CandidateBundle) -> list[CheckResult]:
    checks: list[CheckResult] = []
    assets = bundle.assets
    identifiers = bundle.identifiers
    prices = bundle.prices
    fundamentals = bundle.fundamentals
    actions = bundle.actions

    stock_assets = (
        assets[assets["asset_type"].eq("stock")]
        if "asset_type" in assets else assets.iloc[0:0]
    )
    checks.append(result(
        "FMP_REQUIRED_UNIVERSE", "asset", Severity.CRITICAL,
        int(stock_assets.empty), "at least one admitted FMP equity",
        "empty" if stock_assets.empty else f"rows={len(stock_assets)}",
    ))

    checks.extend([
        null_keys(
            assets, "asset",
            ["natural_key", "name", "asset_type", "instrument_type", "exchange", "base_currency"],
        ),
        duplicate_keys(assets, "asset", ["natural_key"]),
        null_keys(
            identifiers, "asset_identifier",
            ["natural_key", "source", "identifier", "identifier_type", "valid_from"],
        ),
        duplicate_keys(
            identifiers, "asset_identifier",
            ["natural_key", "source", "identifier_type", "identifier", "valid_from"],
        ),
    ])
    if not assets.empty:
        stocks = assets[assets["asset_type"].eq("stock")]
        invalid = stocks[
            ~stocks["exchange"].isin(["NASDAQ", "NYSE", "AMEX"])
            | ~stocks["instrument_type"].isin(
                ["common_stock", "preferred_stock", "adr", "reit"]
            )
            | stocks.get("is_etf", False).astype(bool)
            | stocks.get("is_fund", False).astype(bool)
        ]
        checks.append(result(
            "FMP_SILVER_UNIVERSE", "asset", Severity.CRITICAL, invalid,
            "only non-ETF/non-fund US-exchange equities are admitted",
        ))
    if not identifiers.empty:
        orphans = identifiers[
            ~identifiers["natural_key"].isin(set(assets["natural_key"]))
        ]
        checks.append(result(
            "FMP_IDENTIFIER_ORPHAN", "asset_identifier", Severity.ERROR,
            orphans, "every identifier belongs to an admitted FMP asset",
        ))
        current_unique = identifiers[
            identifiers["valid_to"].isna() & ~identifiers["identifier_type"].eq("cik")
        ]
        checks.append(duplicate_keys(
            current_unique, "asset_identifier",
            ["source", "identifier_type", "identifier"],
        ))
    symbols = set(
        identifiers.loc[
            identifiers["identifier_type"].isin(["ticker", "fx_pair"]),
            "identifier",
        ].astype(str)
    ) if not identifiers.empty else set()

    if not prices.empty:
        checks.extend([
            null_keys(prices, "price_daily", ["identifier", "source", "trade_date", "close", "adj_close"]),
            duplicate_keys(prices, "price_daily", ["identifier", "source", "trade_date"]),
            finite_numbers(
                prices, "price_daily",
                ["open", "high", "low", "close", "adj_close", "total_return_close", "volume", "vwap"],
            ),
        ])
        if "natural_key" in prices:
            checks.append(duplicate_keys(
                prices, "price_daily",
                ["natural_key", "source", "trade_date"],
            ))
        unmapped = prices[~prices["identifier"].astype(str).isin(symbols)]
        checks.append(result(
            "FMP_PRICE_IDENTIFIER_MAPPING", "price_daily", Severity.CRITICAL,
            unmapped, "every FMP price maps to an admitted equity",
        ))
        numeric = prices[["open", "high", "low", "close"]].apply(
            pd.to_numeric, errors="coerce",
        )
        bad_ohlc = prices[
            numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
            | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
            | numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
        ]
        checks.append(result(
            "FMP_PRICE_OHLC", "price_daily", Severity.ERROR, bad_ohlc,
            "positive OHLC with low <= open/close <= high",
        ))

    target_date = bundle.stats.get("_target_date")
    if target_date is not None and not bundle.stats.get("_market_closed", False):
        stock_price_count = int(
            bundle.stats.get("price_daily", {}).get("transformed_rows", 0)
        )
        checks.append(result(
            "FMP_EXPECTED_DAILY_PRICE", "price_daily", Severity.CRITICAL,
            int(stock_price_count == 0),
            "at least one admitted US equity price on an open market day",
            f"date={target_date}, stock_price_rows={stock_price_count}",
        ))

    if not fundamentals.empty:
        fundamental_key = [
            "identifier", "source", "statement_type", "data_basis", "period_end",
            "fiscal_period", "fs_type", "revision_key", "metric",
        ]
        checks.extend([
            null_keys(fundamentals, "fundamental", fundamental_key + ["available_at"]),
            duplicate_keys(fundamentals, "fundamental", fundamental_key),
            finite_numbers(fundamentals, "fundamental", ["value"]),
        ])
        checks.append(result(
            "FMP_FUNDAMENTAL_IDENTIFIER_MAPPING", "fundamental", Severity.CRITICAL,
            fundamentals[~fundamentals["identifier"].astype(str).isin(symbols)],
            "every FMP statement maps to an admitted equity",
        ))

    if not actions.empty:
        checks.extend([
            null_keys(actions, "corporate_action", ["identifier", "source", "action_key", "action_type", "ex_date"]),
            duplicate_keys(actions, "corporate_action", ["identifier", "source", "action_key"]),
        ])
        checks.append(result(
            "FMP_ACTION_IDENTIFIER_MAPPING", "corporate_action", Severity.CRITICAL,
            actions[~actions["identifier"].astype(str).isin(symbols)],
            "every FMP action maps to an admitted equity",
        ))

    universe = bundle.stats.get("asset", {})
    excluded = int(universe.get("excluded_symbol_count", 0))
    checks.append(CheckResult(
        rule_code="FMP_SILVER_UNIVERSE_EXCLUDED",
        dataset="asset",
        severity=Severity.MODIFIED,
        status=CheckStatus.PASS,
        expected="out-of-scope instruments remain in Bronze and are excluded only in Silver",
        actual=(
            f"excluded_symbols={excluded}, "
            f"reasons={universe.get('excluded_by_reason', {})}"
        ),
        failed_count=0,
        samples=list(universe.get("excluded_samples", []))[:20],
    ))
    ambiguous_identifiers = int(
        universe.get("ambiguous_identifier_rows_removed", 0)
    )
    checks.append(CheckResult(
        rule_code="FMP_AMBIGUOUS_IDENTIFIER_EXCLUDED",
        dataset="asset_identifier",
        severity=Severity.MODIFIED,
        status=CheckStatus.PASS,
        expected=(
            "ambiguous current CUSIP/ISIN remain in Bronze and are omitted "
            "from Silver"
        ),
        actual=f"excluded_identifier_rows={ambiguous_identifiers}",
        failed_count=0,
        samples=list(
            universe.get("ambiguous_identifier_samples", [])
        )[:20],
    ))
    return checks
