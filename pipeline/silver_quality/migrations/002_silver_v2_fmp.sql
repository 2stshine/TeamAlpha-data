-- Silver v2: FMP 미국시장 지원, 시점 정확 식별자, 기업행사 영속화.
-- 기존 KRX/DART 행을 보존하면서 재실행할 수 있어야 한다.

ALTER TABLE asset ADD COLUMN IF NOT EXISTS instrument_type TEXT;
ALTER TABLE asset ADD COLUMN IF NOT EXISTS country_code TEXT;
ALTER TABLE asset ADD COLUMN IF NOT EXISTS base_currency TEXT;
ALTER TABLE asset ADD COLUMN IF NOT EXISTS listed_from DATE;
ALTER TABLE asset ADD COLUMN IF NOT EXISTS listed_to DATE;

UPDATE asset
SET instrument_type = CASE asset_type
        WHEN 'index' THEN 'index'
        WHEN 'fx' THEN 'fx'
        ELSE 'common_stock'
    END
WHERE instrument_type IS NULL;
UPDATE asset SET country_code = 'KR' WHERE country_code IS NULL AND exchange = 'KRX';
UPDATE asset SET base_currency = currency WHERE base_currency IS NULL;
ALTER TABLE asset ALTER COLUMN instrument_type SET NOT NULL;
ALTER TABLE asset ALTER COLUMN instrument_type SET DEFAULT 'unknown';

ALTER TABLE asset DROP CONSTRAINT IF EXISTS asset_asset_type_check;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='asset'::regclass AND conname='asset_asset_type_v2_check'
    ) THEN
        ALTER TABLE asset ADD CONSTRAINT asset_asset_type_v2_check
            CHECK (asset_type IN ('stock','index','fx'));
    END IF;
END $$;

ALTER TABLE asset_identifier ADD COLUMN IF NOT EXISTS identifier_type TEXT;
ALTER TABLE asset_identifier ADD COLUMN IF NOT EXISTS valid_from DATE;
ALTER TABLE asset_identifier ADD COLUMN IF NOT EXISTS valid_to DATE;
UPDATE asset_identifier
SET identifier_type=CASE WHEN source='DART' THEN 'corp_code' ELSE 'ticker' END
WHERE identifier_type IS NULL;
UPDATE asset_identifier SET valid_from=DATE '0001-01-01' WHERE valid_from IS NULL;
ALTER TABLE asset_identifier ALTER COLUMN identifier_type SET NOT NULL;
ALTER TABLE asset_identifier ALTER COLUMN valid_from SET NOT NULL;
ALTER TABLE asset_identifier ALTER COLUMN identifier_type SET DEFAULT 'ticker';
ALTER TABLE asset_identifier ALTER COLUMN valid_from SET DEFAULT DATE '0001-01-01';
ALTER TABLE asset_identifier DROP CONSTRAINT IF EXISTS uq_asset_identifier_source_identifier;
DROP INDEX IF EXISTS uq_asset_identifier_source_identifier;
ALTER TABLE asset_identifier DROP CONSTRAINT IF EXISTS asset_identifier_pkey;
ALTER TABLE asset_identifier ADD CONSTRAINT asset_identifier_pkey PRIMARY KEY (
    asset_id, source, identifier_type, identifier, valid_from
);
CREATE INDEX IF NOT EXISTS ix_asset_identifier_lookup_v2
    ON asset_identifier(source, identifier_type, identifier, valid_from, valid_to);
CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_identifier_current
    ON asset_identifier(source, identifier_type, identifier)
    WHERE valid_to IS NULL AND identifier_type <> 'cik';

ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS total_return_close NUMERIC(28,8);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS vwap NUMERIC(28,8);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
UPDATE price_daily p
SET currency = a.base_currency
FROM asset a
WHERE p.asset_id=a.asset_id AND p.currency IS NULL;
UPDATE price_daily SET total_return_close=adj_close
WHERE total_return_close IS NULL AND source='KRX';
ALTER TABLE price_daily ALTER COLUMN open TYPE NUMERIC(28,8);
ALTER TABLE price_daily ALTER COLUMN high TYPE NUMERIC(28,8);
ALTER TABLE price_daily ALTER COLUMN low TYPE NUMERIC(28,8);
ALTER TABLE price_daily ALTER COLUMN close TYPE NUMERIC(28,8);
ALTER TABLE price_daily ALTER COLUMN adj_close TYPE NUMERIC(28,8);
ALTER TABLE price_daily ALTER COLUMN trading_value TYPE NUMERIC(30,4);
ALTER TABLE price_daily ALTER COLUMN market_cap TYPE NUMERIC(30,4);

DROP VIEW IF EXISTS fundamental_current;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS statement_type TEXT;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS data_basis TEXT;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS unit_type TEXT;
UPDATE fundamental
SET statement_type = CASE
        WHEN metric IN (
            'total_assets','current_assets','noncurrent_assets',
            'total_liabilities','current_liabilities','noncurrent_liabilities',
            'total_equity','capital_stock','retained_earnings'
        ) THEN 'BS'
        ELSE 'IS'
    END
WHERE statement_type IS NULL;
UPDATE fundamental SET data_basis='STANDARDIZED' WHERE data_basis IS NULL;
UPDATE fundamental SET available_at=available_date::timestamp AT TIME ZONE 'UTC'
WHERE available_at IS NULL AND available_date IS NOT NULL;
UPDATE fundamental SET unit_type='currency' WHERE unit_type IS NULL;
ALTER TABLE fundamental ALTER COLUMN statement_type SET NOT NULL;
ALTER TABLE fundamental ALTER COLUMN data_basis SET NOT NULL;
ALTER TABLE fundamental ALTER COLUMN unit_type SET NOT NULL;
ALTER TABLE fundamental ALTER COLUMN statement_type SET DEFAULT 'UNKNOWN';
ALTER TABLE fundamental ALTER COLUMN data_basis SET DEFAULT 'STANDARDIZED';
ALTER TABLE fundamental ALTER COLUMN unit_type SET DEFAULT 'currency';
ALTER TABLE fundamental ALTER COLUMN currency DROP NOT NULL;
ALTER TABLE fundamental ALTER COLUMN value TYPE NUMERIC(30,6);
ALTER TABLE fundamental DROP CONSTRAINT IF EXISTS fundamental_fs_type_check;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='fundamental'::regclass AND conname='fundamental_fs_type_v2_check'
    ) THEN
        ALTER TABLE fundamental ADD CONSTRAINT fundamental_fs_type_v2_check
            CHECK (fs_type IN ('CFS','OFS','UNKNOWN'));
    END IF;
END $$;
ALTER TABLE fundamental DROP CONSTRAINT IF EXISTS fundamental_pkey;
ALTER TABLE fundamental ADD CONSTRAINT fundamental_pkey PRIMARY KEY (
    asset_id, source, statement_type, data_basis, period_end,
    fiscal_period, fs_type, revision_key, metric
);
CREATE INDEX IF NOT EXISTS ix_fundamental_pit_v2
    ON fundamental(asset_id, metric, available_at, revision_key);

CREATE OR REPLACE VIEW fundamental_current AS
SELECT asset_id, source, statement_type, data_basis, period_end, fiscal_period,
       fs_type, filing_id, filed, accepted_at, available_date, available_at,
       metric, value, currency, unit_type, revision_key, quality_run_id, loaded_at
FROM (
    SELECT f.*, row_number() OVER (
        PARTITION BY asset_id, source, statement_type, data_basis,
                     period_end, fiscal_period, fs_type, metric
        ORDER BY available_at DESC NULLS LAST, revision_key DESC
    ) AS rn
    FROM fundamental f
) ranked
WHERE rn=1;

CREATE TABLE IF NOT EXISTS corporate_action (
    asset_id             BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source               TEXT NOT NULL,
    action_key           TEXT NOT NULL,
    action_type          TEXT NOT NULL,
    announcement_date    DATE,
    ex_date              DATE,
    record_date          DATE,
    payment_date         DATE,
    cash_amount          NUMERIC(28,8),
    currency             TEXT,
    ratio_numerator      NUMERIC(28,8),
    ratio_denominator    NUMERIC(28,8),
    expected_price_factor NUMERIC(28,12),
    share_count_factor   NUMERIC(28,12),
    status               TEXT NOT NULL DEFAULT 'confirmed',
    confidence           TEXT,
    filing_id            TEXT,
    quality_run_id       UUID REFERENCES dq_run(run_id),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, source, action_key)
);
CREATE INDEX IF NOT EXISTS ix_corporate_action_event
    ON corporate_action(asset_id, ex_date, action_type);

ALTER TABLE quality_stage.asset ADD COLUMN IF NOT EXISTS instrument_type TEXT;
ALTER TABLE quality_stage.asset ADD COLUMN IF NOT EXISTS country_code TEXT;
ALTER TABLE quality_stage.asset ADD COLUMN IF NOT EXISTS base_currency TEXT;
ALTER TABLE quality_stage.asset ADD COLUMN IF NOT EXISTS listed_from DATE;
ALTER TABLE quality_stage.asset ADD COLUMN IF NOT EXISTS listed_to DATE;
ALTER TABLE quality_stage.asset_identifier ADD COLUMN IF NOT EXISTS identifier_type TEXT;
ALTER TABLE quality_stage.asset_identifier ADD COLUMN IF NOT EXISTS valid_from DATE;
ALTER TABLE quality_stage.asset_identifier ADD COLUMN IF NOT EXISTS valid_to DATE;
ALTER TABLE quality_stage.price_daily ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE quality_stage.price_daily ADD COLUMN IF NOT EXISTS total_return_close NUMERIC(28,8);
ALTER TABLE quality_stage.price_daily ADD COLUMN IF NOT EXISTS vwap NUMERIC(28,8);
ALTER TABLE quality_stage.price_daily ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE quality_stage.fundamental ADD COLUMN IF NOT EXISTS statement_type TEXT;
ALTER TABLE quality_stage.fundamental ADD COLUMN IF NOT EXISTS data_basis TEXT;
ALTER TABLE quality_stage.fundamental ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE quality_stage.fundamental ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE quality_stage.fundamental ADD COLUMN IF NOT EXISTS unit_type TEXT;

CREATE TABLE IF NOT EXISTS quality_stage.corporate_action (
    backfill_run_id       UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key         TEXT NOT NULL,
    identifier            TEXT NOT NULL,
    source                TEXT NOT NULL,
    action_key            TEXT NOT NULL,
    action_type           TEXT NOT NULL,
    announcement_date     DATE,
    ex_date               DATE,
    record_date           DATE,
    payment_date          DATE,
    cash_amount           NUMERIC(28,8),
    currency              TEXT,
    ratio_numerator       NUMERIC(28,8),
    ratio_denominator     NUMERIC(28,8),
    expected_price_factor NUMERIC(28,12),
    share_count_factor    NUMERIC(28,12),
    status                TEXT,
    confidence            TEXT,
    filing_id             TEXT,
    source_file           TEXT,
    PRIMARY KEY(backfill_run_id, identifier, source, action_key)
);
