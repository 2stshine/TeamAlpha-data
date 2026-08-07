"""Source-aware quality gates for FMP Silver candidates."""
from __future__ import annotations

import exchange_calendars as xcals
import pandas as pd

from pipeline.fmp_commodities import COMMODITY_BY_SYMBOL, COMMODITY_SYMBOLS
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

# A completed US session returns close to the full admitted universe. A daily
# admitted-equity count below this fraction of the recent baseline means the
# eod-bulk fetch was still partial, not a genuine universe change.
FMP_COVERAGE_FLOOR_FRACTION = 0.5


def check_fmp(bundle: CandidateBundle) -> list[CheckResult]:
    checks: list[CheckResult] = []
    assets = bundle.assets
    identifiers = bundle.identifiers
    prices = bundle.prices
    fundamentals = bundle.fundamentals
    actions = bundle.actions
    commodity_enabled = (
        bundle.stats.get("_source_scope") == "commodity"
        or "commodity" in bundle.stats
        or (
            "asset_type" in assets
            and assets["asset_type"].eq("commodity").any()
        )
    )

    stock_assets = (
        assets[assets["asset_type"].eq("stock")]
        if "asset_type" in assets else assets.iloc[0:0]
    )
    commodity_scope = bundle.stats.get("_source_scope") == "commodity"
    if not commodity_scope:
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
        commodities = assets[assets["asset_type"].eq("commodity")]
        expected_units = commodities["natural_key"].astype(str).map(
            lambda key: (
                COMMODITY_BY_SYMBOL.get(
                    key.removeprefix("FMP:COMMODITY:")
                ).price_unit
                if key.removeprefix("FMP:COMMODITY:") in COMMODITY_BY_SYMBOL
                else None
            )
        )
        invalid_commodities = commodities[
            ~commodities["natural_key"].astype(str).str.removeprefix(
                "FMP:COMMODITY:"
            ).isin(COMMODITY_SYMBOLS)
            | ~commodities["instrument_type"].eq(
                "commodity_future_continuous"
            )
            | ~commodities["exchange"].eq("COMMODITY")
            | ~commodities["currency"].eq("USD")
            | ~commodities["base_currency"].eq("USD")
            | commodities.get("price_unit", pd.Series(index=commodities.index)).isna()
            | ~commodities.get(
                "price_unit", pd.Series(index=commodities.index)
            ).eq(expected_units)
        ]
        checks.append(result(
            "FMP_COMMODITY_UNIVERSE", "asset", Severity.CRITICAL,
            invalid_commodities,
            "only the 28 allowlisted normalized continuous-futures assets",
        ))
        if commodity_enabled:
            commodity_symbols = set(
                commodities["natural_key"].astype(str).str.removeprefix(
                    "FMP:COMMODITY:"
                )
            )
            checks.append(result(
                "FMP_COMMODITY_UNIVERSE_COMPLETE", "asset", Severity.CRITICAL,
                int(commodity_symbols != set(COMMODITY_SYMBOLS)),
                "exactly 28 physical non-micro commodity series",
                actual=(
                    f"count={len(commodity_symbols)} "
                    f"missing={sorted(set(COMMODITY_SYMBOLS)-commodity_symbols)} "
                    f"extra={sorted(commodity_symbols-set(COMMODITY_SYMBOLS))}"
                ),
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
            identifiers["identifier_type"].isin(
                ["ticker", "fx_pair", "commodity_symbol"]
            ),
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
        commodity_price = prices["source"].eq("FMP_COMMODITY")
        bad_ohlc = prices[
            numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
            | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
            | numeric.isna().any(axis=1)
            | (
                ~commodity_price
                & numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
            )
        ]
        checks.append(result(
            "FMP_PRICE_OHLC", "price_daily", Severity.ERROR, bad_ohlc,
            "finite OHLC with low <= open/close <= high; non-futures positive",
        ))
        commodities = prices[commodity_price]
        if not commodities.empty:
            semantic_bad = commodities[
                ~commodities["currency"].eq("USD")
                | commodities["total_return_close"].notna()
                | commodities["shares"].notna()
                | commodities["market_cap"].notna()
                | commodities["trading_value"].notna()
                | commodities["market"].notna()
                | ~commodities["adj_close"].eq(commodities["close"])
                | commodities.get(
                    "price_unit", pd.Series(index=commodities.index)
                ).isna()
            ]
            checks.append(result(
                "FMP_COMMODITY_PRICE_SEMANTICS", "price_daily",
                Severity.ERROR, semantic_bad,
                "USD normalized continuous-future price; adj_close=close; "
                "equity-only fields null",
            ))
            dates = pd.to_datetime(
                commodities["trade_date"], errors="coerce",
            )
            checks.append(result(
                "FMP_COMMODITY_WEEKDAY", "price_daily", Severity.ERROR,
                commodities[dates.dt.weekday.eq(5).to_numpy()],
                "commodity EOD observations may use Sunday evening through "
                "Friday session dates, but never Saturday",
            ))
        equity_prices = prices[prices["source"].eq("FMP")]
        if not equity_prices.empty:
            parsed_dates = pd.to_datetime(
                equity_prices["trade_date"], errors="coerce",
            )
            start = parsed_dates.min()
            end = parsed_dates.max()
            sessions = set(
                xcals.get_calendar("XNYS")
                .sessions_in_range(start, end)
                .tz_localize(None)
                .date
            )
            non_session = equity_prices[
                ~parsed_dates.dt.date.isin(sessions).to_numpy()
            ]
            checks.append(result(
                "FMP_PRICE_TRADING_SESSION", "price_daily", Severity.ERROR,
                non_session, "every FMP equity price is on an XNYS session",
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
        baseline = bundle.stats.get("price_daily", {}).get("coverage_baseline")
        if baseline:
            baseline = int(baseline)
            floor = FMP_COVERAGE_FLOOR_FRACTION * baseline
            checks.append(CheckResult(
                rule_code="FMP_DAILY_PRICE_COVERAGE_FLOOR",
                dataset="price_daily",
                severity=Severity.ERROR,
                status=(
                    CheckStatus.FAIL
                    if stock_price_count < floor
                    else CheckStatus.PASS
                ),
                expected=(
                    f"admitted US equity rows >= {FMP_COVERAGE_FLOOR_FRACTION:.0%} "
                    f"of recent baseline {baseline}"
                ),
                actual=(
                    f"date={target_date}, stock_price_rows={stock_price_count}, "
                    f"baseline={baseline}"
                ),
                failed_count=int(stock_price_count < floor),
            ))

    if not fundamentals.empty:
        fundamental_key = [
            (
                "natural_key"
                if "natural_key" in fundamentals
                else "identifier"
            ),
            "source", "statement_type", "data_basis", "period_end",
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
        action_identity = (
            "natural_key" if "natural_key" in actions else "identifier"
        )
        checks.extend([
            null_keys(actions, "corporate_action", [action_identity, "source", "action_key", "action_type", "ex_date"]),
            duplicate_keys(actions, "corporate_action", [action_identity, "source", "action_key"]),
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
    price_stats = bundle.stats.get("price_daily", {})
    for code, detail_key, expected in (
        (
            "FMP_NON_SESSION_PRICE_EXCLUDED", "non_session_rows_excluded",
            "global-bulk rows outside XNYS sessions remain in Bronze and are excluded from Silver",
        ),
        (
            "FMP_INVALID_OHLC_EXCLUDED", "invalid_ohlc_excluded",
            "invalid source OHLC rows remain in Bronze and are excluded from Silver",
        ),
        (
            "FMP_DUPLICATE_PRICE_REMOVED", "duplicate_price_rows_removed",
            "duplicate source price keys are deterministically reduced to one Silver row",
        ),
    ):
        detail = price_stats.get(detail_key, {})
        count = int(detail.get("row_count", 0))
        checks.append(CheckResult(
            rule_code=code,
            dataset="price_daily",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=expected,
            actual=f"affected_rows={count}",
            failed_count=0,
            samples=list(detail.get("samples", []))[:20],
        ))
    if commodity_enabled:
        commodity_stats = bundle.stats.get("commodity", {})
        non_session = commodity_stats.get("non_session_rows_excluded", {})
        non_session_count = int(non_session.get("row_count", 0))
        checks.append(CheckResult(
            rule_code="FMP_COMMODITY_NON_SESSION_EXCLUDED",
            dataset="price_daily",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "Saturday provider rows remain in Bronze and are excluded "
                "from Silver; Sunday evening futures sessions are retained"
            ),
            actual=f"affected_rows={non_session_count}",
            failed_count=0,
            samples=list(non_session.get("samples", []))[:20],
        ))
        missing_provider = list(commodity_stats.get("missing_from_provider", []))
        currency_mismatches = list(
            commodity_stats.get("provider_currency_mismatches", [])
        )
        checks.append(result(
            "FMP_COMMODITY_PROVIDER_LIST", "asset", Severity.CRITICAL,
            len(missing_provider) + int(
                not commodity_stats.get("provider_list_present", False)
            ) + len(currency_mismatches),
            "the current FMP commodities-list contains all 28 allowlisted symbols",
            actual=(
                f"list_present={commodity_stats.get('provider_list_present', False)} "
                f"missing={missing_provider} "
                f"currency_mismatches={currency_mismatches}"
            ),
        ))
        roll = commodity_stats.get("possible_roll", {})
        roll_count = int(roll.get("row_count", 0))
        checks.append(CheckResult(
            rule_code="FMP_COMMODITY_POSSIBLE_ROLL",
            dataset="price_daily",
            severity=Severity.WARNING,
            status=CheckStatus.FAIL if roll_count else CheckStatus.PASS,
            expected=(
                "no unreviewed absolute one-day move above 20%; "
                "source values preserved"
            ),
            actual=f"possible_roll_rows={roll_count}",
            failed_count=roll_count,
            samples=list(roll.get("samples", []))[:20],
        ))
    return checks
