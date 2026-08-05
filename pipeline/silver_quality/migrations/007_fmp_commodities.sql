-- FMP physical commodity continuous-futures support.

ALTER TABLE asset ADD COLUMN IF NOT EXISTS price_unit TEXT;

ALTER TABLE asset DROP CONSTRAINT IF EXISTS asset_asset_type_v2_check;
ALTER TABLE asset DROP CONSTRAINT IF EXISTS asset_asset_type_v3_check;
ALTER TABLE asset ADD CONSTRAINT asset_asset_type_v3_check
    CHECK (asset_type IN ('stock','index','fx','commodity')) NOT VALID;

-- Replace the v2 guard so commodity assets have an explicit, non-ambiguous
-- shape while every existing stock/index/FX invariant remains unchanged.
ALTER TABLE asset DROP CONSTRAINT IF EXISTS asset_critical_error_guard;
ALTER TABLE asset ADD CONSTRAINT asset_critical_error_guard CHECK (
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
            AND price_unit IS NULL
        )
        OR (
            asset_type = 'index'
            AND exchange = 'KRX'
            AND instrument_type = 'index'
            AND price_unit IS NULL
        )
        OR (
            asset_type = 'fx'
            AND exchange = 'FX'
            AND instrument_type = 'fx'
            AND price_unit IS NULL
        )
        OR (
            asset_type = 'commodity'
            AND exchange = 'COMMODITY'
            AND instrument_type = 'commodity_future_continuous'
            AND currency = 'USD'
            AND base_currency = 'USD'
            AND price_unit IS NOT NULL
            AND btrim(price_unit) <> ''
        )
    )
) NOT VALID;

-- Futures can trade through zero (WTI did in April 2020). Preserve those raw
-- prices while still enforcing finite values and valid OHLC ordering.
ALTER TABLE price_daily DROP CONSTRAINT IF EXISTS price_daily_critical_error_guard;
ALTER TABLE price_daily ADD CONSTRAINT price_daily_critical_error_guard CHECK (
    quality_run_id IS NOT NULL
    AND btrim(source) <> ''
    AND close IS NOT NULL
    AND close::text NOT IN ('NaN', 'Infinity', '-Infinity')
    AND adj_close IS NOT NULL
    AND adj_close::text NOT IN ('NaN', 'Infinity', '-Infinity')
    AND (
        source = 'FMP_COMMODITY'
        OR (close > 0 AND adj_close > 0)
    )
    AND (
        (
            open IS NULL AND high IS NULL AND low IS NULL
            AND source NOT IN ('FMP', 'FMP_FX', 'FMP_COMMODITY')
        )
        OR (
            open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
            AND open::text NOT IN ('NaN', 'Infinity', '-Infinity')
            AND high::text NOT IN ('NaN', 'Infinity', '-Infinity')
            AND low::text NOT IN ('NaN', 'Infinity', '-Infinity')
            AND (
                source = 'FMP_COMMODITY'
                OR (open > 0 AND high > 0 AND low > 0)
            )
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

ALTER TABLE asset VALIDATE CONSTRAINT asset_asset_type_v3_check;
ALTER TABLE asset VALIDATE CONSTRAINT asset_critical_error_guard;
ALTER TABLE price_daily VALIDATE CONSTRAINT price_daily_critical_error_guard;

