"""규칙 registry와 실행 순서."""
from __future__ import annotations

from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.rules.common import result
from pipeline.silver_quality.rules.assets import check_assets
from pipeline.silver_quality.rules.financials import check_financials
from pipeline.silver_quality.rules.prices import check_prices
from pipeline.silver_quality.rules.reconciliation import check_reconciliation


def run_registered_rules(
    bundle: CandidateBundle,
    *,
    target_date=None,
    history=None,
    partition_key: str | None = None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    results.extend(check_assets(bundle.assets, bundle.identifiers, partition_key))
    krx = set(bundle.stats.get("_existing_krx_identifiers", set()))
    if not bundle.identifiers.empty:
        krx.update(
            bundle.identifiers.loc[
                bundle.identifiers["source"].eq("KRX"), "identifier"
            ].astype(str)
        )
    if not bundle.prices.empty:
        missing = bundle.prices[
            ~bundle.prices["identifier"].astype(str).isin(krx)
        ]
        results.append(result(
            "PRICE_IDENTIFIER_MAPPING", "price_daily", Severity.ERROR,
            missing, "every price identifier maps to a KRX asset",
            partition_key=partition_key,
        ))
    if not bundle.fundamentals.empty:
        missing = bundle.fundamentals[
            ~bundle.fundamentals["identifier"].astype(str).isin(krx)
        ]
        results.append(result(
            "FUNDAMENTAL_IDENTIFIER_MAPPING", "fundamental", Severity.ERROR,
            missing, "every fundamental ticker maps to a KRX asset",
            partition_key=partition_key,
        ))
    unsupported_market = bundle.stats.get("price_daily", {}).get(
        "unsupported_market",
        {"row_count": 0, "ticker_count": 0, "markets": {}, "samples": []},
    )
    unsupported_rows = int(unsupported_market.get("row_count", 0))
    unsupported_tickers = int(unsupported_market.get("ticker_count", 0))
    if unsupported_rows > 0:
        results.append(CheckResult(
            rule_code="UNSUPPORTED_MARKET_EXCLUDED",
            dataset="price_daily",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected="KONEX rows are explicitly excluded from the Silver universe",
            actual=(
                f"excluded_rows={unsupported_rows}, "
                f"excluded_tickers={unsupported_tickers}, "
                f"markets={unsupported_market.get('markets', {})}"
            ),
            failed_count=0,
            samples=list(unsupported_market.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    unsupported_asset = bundle.stats.get("fundamental", {}).get(
        "unsupported_market_asset",
        {"row_count": 0, "ticker_count": 0, "samples": []},
    )
    unsupported_asset_rows = int(unsupported_asset.get("row_count", 0))
    unsupported_asset_tickers = int(
        unsupported_asset.get("ticker_count", 0)
    )
    if unsupported_asset_rows > 0:
        results.append(CheckResult(
            rule_code="UNSUPPORTED_MARKET_ASSET_EXCLUDED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "fundamentals for assets with only KONEX price history "
                "are explicitly excluded"
            ),
            actual=(
                f"excluded_rows={unsupported_asset_rows}, "
                f"excluded_tickers={unsupported_asset_tickers}"
            ),
            failed_count=0,
            samples=list(unsupported_asset.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    nontradable = bundle.stats.get("fundamental", {}).get(
        "no_tradable_price_asset",
        {"row_count": 0, "ticker_count": 0, "samples": []},
    )
    nontradable_rows = int(nontradable.get("row_count", 0))
    nontradable_tickers = int(nontradable.get("ticker_count", 0))
    if nontradable_rows > 0:
        results.append(CheckResult(
            rule_code="NO_TRADABLE_PRICE_ASSET",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "DART ticker occurs at least once in the complete KRX price universe; "
                "otherwise exclude explicitly"
            ),
            actual=(
                f"excluded_rows={nontradable_rows}, "
                f"excluded_tickers={nontradable_tickers}"
            ),
            failed_count=0,
            samples=list(nontradable.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    full_statement = bundle.stats.get("fundamental", {}).get(
        "full_statement_supplement",
        {"row_count": 0, "file_count": 0},
    )
    full_statement_rows = int(full_statement.get("row_count", 0))
    if full_statement_rows > 0:
        results.append(CheckResult(
            rule_code="DART_FULL_STATEMENT_SUPPLEMENT",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "full-statement source fills only business keys absent from "
                "the DART major-account source"
            ),
            actual=(
                f"supplemented_rows={full_statement_rows}, "
                f"source_files={int(full_statement.get('file_count', 0))}"
            ),
            failed_count=0,
            partition_key=partition_key,
        ))
    replacement = bundle.stats.get("fundamental", {}).get(
        "accounting_equation_supplement_replacement",
        {"row_count": 0, "scope_count": 0, "samples": []},
    )
    replacement_rows = int(replacement.get("row_count", 0))
    replacement_scopes = int(replacement.get("scope_count", 0))
    if replacement_rows > 0:
        results.append(CheckResult(
            rule_code="DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "replace assets/liabilities/equity atomically only when the "
                "same-revision DART full statement balances within 1%"
            ),
            actual=(
                f"replaced_scopes={replacement_scopes}, "
                f"replaced_rows={replacement_rows}"
            ),
            failed_count=0,
            samples=list(replacement.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    if not bundle.prices.empty:
        results.extend(check_prices(
            bundle.prices,
            target_date=target_date,
            history=history,
            corporate_actions=bundle.stats.get("_corporate_actions"),
            partition_key=partition_key,
        ))
    if not bundle.fundamentals.empty:
        results.extend(check_financials(
            bundle.fundamentals,
            partition_key,
            source_inconsistency=bundle.stats.get(
                "fundamental",
                {},
            ).get("source_accounting_inconsistency"),
        ))
    results.extend(check_reconciliation(bundle.stats, partition_key))
    return results
