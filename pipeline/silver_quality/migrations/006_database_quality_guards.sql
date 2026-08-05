-- Database-level last line of defense for deterministic Critical/Error rules.
-- Statistical, cross-row, cross-source and warning rules intentionally remain
-- in the Python quality gate.

ALTER TABLE asset
    ADD CONSTRAINT asset_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(name) <> ''
        AND btrim(asset_type) <> ''
        AND btrim(instrument_type) <> ''
        AND btrim(exchange) <> ''
        AND btrim(currency) <> ''
        AND base_currency IS NOT NULL
        AND btrim(base_currency) <> ''
        AND currency ~ '^[A-Z]{3}$'
        AND base_currency ~ '^[A-Z]{3}$'
        AND (
            (
                asset_type = 'stock'
                AND exchange IN ('KRX', 'NASDAQ', 'NYSE', 'AMEX')
                AND instrument_type IN (
                    'common_stock', 'preferred_stock', 'adr', 'reit'
                )
            )
            OR (
                asset_type = 'index'
                AND exchange = 'KRX'
                AND instrument_type = 'index'
            )
            OR (
                asset_type = 'fx'
                AND exchange = 'FX'
                AND instrument_type = 'fx'
            )
        )
    ) NOT VALID;

ALTER TABLE asset_identifier
    ADD CONSTRAINT asset_identifier_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(source) <> ''
        AND btrim(identifier) <> ''
        AND btrim(identifier_type) <> ''
    ) NOT VALID;

ALTER TABLE price_daily
    ADD CONSTRAINT price_daily_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(source) <> ''
        AND close IS NOT NULL
        AND close > 0
        AND close::text NOT IN ('NaN', 'Infinity', '-Infinity')
        AND adj_close IS NOT NULL
        AND adj_close > 0
        AND adj_close::text NOT IN ('NaN', 'Infinity', '-Infinity')
        AND (
            (
                open IS NULL AND high IS NULL AND low IS NULL
                AND source NOT IN ('FMP', 'FMP_FX')
            )
            OR (
                open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                AND open > 0 AND high > 0 AND low > 0
                AND open::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND high::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND low::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND high >= GREATEST(open, close)
                AND low <= LEAST(open, close)
            )
        )
        AND (volume IS NULL OR volume >= 0)
        AND (trading_value IS NULL OR trading_value >= 0)
        AND (shares IS NULL OR shares > 0)
        AND (market_cap IS NULL OR market_cap >= 0)
        AND (
            total_return_close IS NULL
            OR total_return_close::text NOT IN ('NaN', 'Infinity', '-Infinity')
        )
        AND (
            vwap IS NULL
            OR vwap::text NOT IN ('NaN', 'Infinity', '-Infinity')
        )
        AND (currency IS NULL OR currency ~ '^[A-Z]{3}$')
        AND (
            source <> 'KRX'
            OR market IS NULL
            OR (market_cap IS NOT NULL AND market_cap > 0)
        )
        AND (
            shares IS NULL
            OR market_cap IS NULL
            OR abs(market_cap - close * shares)
               <= abs(close * shares) * 0.01
        )
    ) NOT VALID;

ALTER TABLE fundamental
    ADD CONSTRAINT fundamental_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(source) <> ''
        AND btrim(statement_type) <> ''
        AND btrim(data_basis) <> ''
        AND btrim(fiscal_period) <> ''
        AND btrim(fs_type) <> ''
        AND btrim(revision_key) <> ''
        AND btrim(metric) <> ''
        AND btrim(unit_type) <> ''
        AND statement_type IN ('BS', 'IS', 'CF', 'DIVIDEND')
        AND value IS NOT NULL
        AND value::text NOT IN ('NaN', 'Infinity', '-Infinity')
        AND available_date IS NOT NULL
        AND available_at IS NOT NULL
        AND (
            (unit_type = 'shares' AND (
                currency IS NULL OR currency ~ '^[A-Z]{3}$'
            ))
            OR (
                unit_type <> 'shares'
                AND currency IS NOT NULL
                AND currency ~ '^[A-Z]{3}$'
            )
        )
        AND (
            source <> 'DART'
            OR (
                available_date > period_end
                AND (
                    filed IS NULL
                    OR (
                        filed >= period_end
                        AND available_date = filed + 1
                    )
                )
                AND (
                    fs_type IN ('CFS', 'OFS')
                    OR (
                        statement_type = 'DIVIDEND'
                        AND fs_type = 'UNKNOWN'
                    )
                )
            )
        )
        AND (
            statement_type <> 'DIVIDEND'
            OR (metric = 'total_cash_dividend' AND unit_type = 'currency')
            OR (metric IN ('payout_ratio', 'dividend_yield') AND unit_type = 'percent')
            OR (metric = 'cash_dividend_per_share' AND unit_type = 'per_share')
            OR (metric = 'stock_dividend_per_share' AND unit_type = 'shares')
        )
    ) NOT VALID;

ALTER TABLE corporate_action
    ADD CONSTRAINT corporate_action_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(source) <> ''
        AND btrim(action_key) <> ''
        AND btrim(action_type) <> ''
        AND btrim(status) <> ''
        AND (source NOT LIKE 'FMP_%' OR ex_date IS NOT NULL)
    ) NOT VALID;

-- NOT VALID keeps the brief ADD CONSTRAINT lock independent from the table
-- scan. Validation checks every existing row and then protects future writes.
ALTER TABLE asset VALIDATE CONSTRAINT asset_critical_error_guard;
ALTER TABLE asset_identifier
    VALIDATE CONSTRAINT asset_identifier_critical_error_guard;
ALTER TABLE price_daily
    VALIDATE CONSTRAINT price_daily_critical_error_guard;
ALTER TABLE fundamental
    VALIDATE CONSTRAINT fundamental_critical_error_guard;
ALTER TABLE corporate_action
    VALIDATE CONSTRAINT corporate_action_critical_error_guard;
