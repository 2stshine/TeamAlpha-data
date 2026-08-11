-- Auditable KRX gross-total-return support.
-- Applying this migration deliberately leaves the contract in BUILDING state;
-- research must not treat total_return_close as dividend-inclusive until a
-- certified rebuild promotes it to CERTIFIED.

ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS corp_cls TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS dart_rm TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS cash_amount_status TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS source_evidence_status TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS correction_of_action_key TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS revision_root_action_key TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS revision_kind TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS viewer_evidence_sha256 TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS economic_evidence_sha256 TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS reviewed_correction_id TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS payment_date_quality_status TEXT;

ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS corp_cls TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS dart_rm TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS cash_amount_status TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS source_evidence_status TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS correction_of_action_key TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS revision_root_action_key TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS revision_kind TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS viewer_evidence_sha256 TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS economic_evidence_sha256 TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS reviewed_correction_id TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS payment_date_quality_status TEXT;

-- The raw price observation keeps its original quality lineage.  A derived
-- total return has a separate run lineage and must never overwrite it.
ALTER TABLE price_daily
    ADD COLUMN IF NOT EXISTS total_return_quality_run_id UUID
        REFERENCES dq_run(run_id);
ALTER TABLE price_daily
    ADD COLUMN IF NOT EXISTS total_return_loaded_at TIMESTAMPTZ;
ALTER TABLE quality_stage.price_daily
    ADD COLUMN IF NOT EXISTS total_return_quality_run_id UUID
        REFERENCES dq_run(run_id);
ALTER TABLE quality_stage.price_daily
    ADD COLUMN IF NOT EXISTS total_return_loaded_at TIMESTAMPTZ;

CREATE OR REPLACE VIEW factor_price_feature_daily AS
SELECT asset_id,source,trade_date,open,high,low,close,adj_close,currency,
       vwap,available_at,volume,trading_value,shares,market_cap,market,
       quality_run_id,loaded_at
FROM price_daily;
COMMENT ON VIEW factor_price_feature_daily IS
    'Feature-safe price-only projection. Excludes ex-post total_return_close '
    'and total-return lineage; use total_return_close only as a forward label.';
-- Role grants are intentionally not changed by this migration.  After an
-- operator audits production role ownership, grant research/Gold readers this
-- view and withhold direct price_daily access to total_return_close.

UPDATE corporate_action
SET action_scope = 'UNKNOWN'
WHERE action_scope IS NULL;
UPDATE quality_stage.corporate_action
SET action_scope = 'UNKNOWN'
WHERE action_scope IS NULL;
ALTER TABLE corporate_action
    ALTER COLUMN action_scope SET DEFAULT 'UNKNOWN';
ALTER TABLE corporate_action
    ALTER COLUMN action_scope SET NOT NULL;
ALTER TABLE quality_stage.corporate_action
    ALTER COLUMN action_scope SET DEFAULT 'UNKNOWN';
ALTER TABLE quality_stage.corporate_action
    ALTER COLUMN action_scope SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='corporate_action'::regclass
          AND conname='corporate_action_scope_check'
    ) THEN
        ALTER TABLE corporate_action
            ADD CONSTRAINT corporate_action_scope_check
            CHECK (
                action_scope IN ('ISSUER', 'RELATED_COMPANY', 'UNKNOWN')
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='corporate_action'::regclass
          AND conname='corporate_action_corp_cls_check'
    ) THEN
        ALTER TABLE corporate_action
            ADD CONSTRAINT corporate_action_corp_cls_check
            CHECK (corp_cls IS NULL OR corp_cls IN ('Y', 'K', 'N', 'E'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS dividend_event_resolution (
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    action_key TEXT NOT NULL,
    resolution_version TEXT NOT NULL,
    is_canonical BOOLEAN NOT NULL,
    excluded_reason TEXT,
    resolved_ex_date DATE,
    ex_date_basis TEXT,
    applied_trade_date DATE,
    raw_cash_amount NUMERIC(28,8),
    adjusted_cash_amount NUMERIC(28,8),
    source_announcement_date DATE,
    revision_group_key TEXT,
    source_evidence_status TEXT,
    cash_amount_status TEXT,
    correction_of_action_key TEXT,
    revision_root_action_key TEXT,
    revision_kind TEXT,
    viewer_evidence_sha256 TEXT,
    economic_evidence_sha256 TEXT,
    reviewed_correction_id TEXT,
    payment_date_quality_status TEXT,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A new rebuild appends a new audit decision.  Prior decisions are never
    -- mutated or hidden by an upsert.
    PRIMARY KEY (
        quality_run_id, asset_id, source, action_key, resolution_version
    ),
    CHECK (
        (is_canonical AND excluded_reason IS NULL)
        OR (NOT is_canonical AND excluded_reason IS NOT NULL)
    ),
    CHECK (
        ex_date_basis IS NULL
        OR ex_date_basis IN ('KRX_NOTICE', 'KRX_T2_INFERRED')
    )
);

ALTER TABLE dividend_event_resolution
    ALTER COLUMN quality_run_id SET NOT NULL;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS revision_group_key TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS source_evidence_status TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS cash_amount_status TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS correction_of_action_key TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS source_announcement_date DATE;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS revision_root_action_key TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS revision_kind TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS viewer_evidence_sha256 TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS economic_evidence_sha256 TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS reviewed_correction_id TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS payment_date_quality_status TEXT;

DO $$
DECLARE
    existing_pk_name TEXT;
    existing_pk_columns TEXT[];
BEGIN
    SELECT c.conname,
           array_agg(a.attname ORDER BY keys.ordinality)
    INTO existing_pk_name, existing_pk_columns
    FROM pg_constraint c
    CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
    JOIN pg_attribute a
      ON a.attrelid=c.conrelid AND a.attnum=keys.attnum
    WHERE c.conrelid='dividend_event_resolution'::regclass
      AND c.contype='p'
    GROUP BY c.conname;
    IF existing_pk_name IS NULL
       OR existing_pk_columns <> ARRAY[
           'quality_run_id','asset_id','source','action_key',
           'resolution_version'
       ]::TEXT[] THEN
        IF existing_pk_name IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE dividend_event_resolution DROP CONSTRAINT %I',
                existing_pk_name
            );
        END IF;
        ALTER TABLE dividend_event_resolution
            ADD CONSTRAINT dividend_event_resolution_pkey PRIMARY KEY (
                quality_run_id,asset_id,source,action_key,resolution_version
            );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS dividend_source_receipt (
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    receipt_no TEXT NOT NULL,
    asset_id BIGINT REFERENCES asset(asset_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    corp_cls TEXT,
    report_name TEXT NOT NULL,
    dart_rm TEXT,
    announcement_date DATE NOT NULL,
    revision_kind TEXT,
    revision_root_receipt_no TEXT,
    previous_receipt_no TEXT,
    terminal_receipt_no TEXT NOT NULL,
    terminal_announcement_date DATE NOT NULL,
    is_terminal_economic_revision BOOLEAN NOT NULL,
    source_evidence_status TEXT NOT NULL,
    cash_amount_status TEXT NOT NULL,
    record_date DATE,
    payment_date DATE,
    cash_amount NUMERIC(28,8),
    viewer_evidence_sha256 TEXT,
    economic_evidence_sha256 TEXT,
    reviewed_correction_id TEXT,
    payment_date_quality_status TEXT,
    pit_event_date DATE NOT NULL,
    mapping_status TEXT NOT NULL,
    excluded_reason TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (quality_run_id, receipt_no),
    CHECK (receipt_no ~ '^[0-9]{14}$'),
    CONSTRAINT dividend_source_receipt_ticker_check
        CHECK (ticker ~ '^[0-9A-Z]{6}$'),
    CHECK (revision_root_receipt_no ~ '^[0-9]{14}$'),
    CHECK (terminal_receipt_no ~ '^[0-9]{14}$'),
    CHECK (
        previous_receipt_no IS NULL
        OR previous_receipt_no ~ '^[0-9]{14}$'
    ),
    CHECK (
        viewer_evidence_sha256 IS NULL
        OR viewer_evidence_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        economic_evidence_sha256 IS NULL
        OR economic_evidence_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT dividend_source_receipt_mapping_status_check
        CHECK (mapping_status IN ('INCLUDED','EXCLUDED')),
    CONSTRAINT dividend_source_receipt_terminal_check CHECK (
        is_terminal_economic_revision = (receipt_no=terminal_receipt_no)
    ),
    CONSTRAINT dividend_source_receipt_mapping_partition_check CHECK (
        (mapping_status='INCLUDED' AND asset_id IS NOT NULL
         AND excluded_reason IS NULL)
        OR
        (mapping_status='EXCLUDED' AND excluded_reason IS NOT NULL)
    )
);

-- Idempotent alignment for an earlier pre-release 009 table definition.
ALTER TABLE dividend_source_receipt
    ALTER COLUMN asset_id DROP NOT NULL;
ALTER TABLE dividend_source_receipt
    ALTER COLUMN corp_cls DROP NOT NULL;
ALTER TABLE dividend_source_receipt
    DROP CONSTRAINT IF EXISTS dividend_source_receipt_corp_cls_check;
-- Replace the pre-release numeric-only ticker guard.  KRX short codes are
-- six uppercase alphanumeric characters as of 2025/2026 (for example
-- 0008Z0); PIT identity and certified market episodes remain the scope gate.
ALTER TABLE dividend_source_receipt
    DROP CONSTRAINT IF EXISTS dividend_source_receipt_ticker_check;
ALTER TABLE dividend_source_receipt
    ADD CONSTRAINT dividend_source_receipt_ticker_check
    CHECK (ticker ~ '^[0-9A-Z]{6}$');
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS payment_date DATE;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS announcement_date DATE;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS reviewed_correction_id TEXT;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS payment_date_quality_status TEXT;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS pit_event_date DATE;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS mapping_status TEXT;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS excluded_reason TEXT;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS terminal_receipt_no TEXT;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS terminal_announcement_date DATE;
ALTER TABLE dividend_source_receipt
    ADD COLUMN IF NOT EXISTS is_terminal_economic_revision BOOLEAN;

WITH ranked AS (
    SELECT quality_run_id,ticker,revision_root_receipt_no,receipt_no,
           announcement_date,
           row_number() OVER (
               PARTITION BY quality_run_id,ticker,revision_root_receipt_no
               ORDER BY announcement_date DESC,receipt_no DESC
           ) AS terminal_rank
    FROM dividend_source_receipt
    WHERE coalesce(revision_kind,'') <> 'ATTACHMENT_ONLY'
      AND cash_amount_status <> 'ATTACHMENT_ONLY'
), terminal AS (
    SELECT quality_run_id,ticker,revision_root_receipt_no,
           receipt_no AS terminal_receipt_no,
           announcement_date AS terminal_announcement_date
    FROM ranked WHERE terminal_rank=1
)
UPDATE dividend_source_receipt d
SET terminal_receipt_no=t.terminal_receipt_no,
    terminal_announcement_date=t.terminal_announcement_date,
    is_terminal_economic_revision=(d.receipt_no=t.terminal_receipt_no)
FROM terminal t
WHERE d.quality_run_id=t.quality_run_id
  AND d.ticker=t.ticker
  AND d.revision_root_receipt_no=t.revision_root_receipt_no
  AND (
      d.terminal_receipt_no IS DISTINCT FROM t.terminal_receipt_no
      OR d.terminal_announcement_date IS DISTINCT FROM
         t.terminal_announcement_date
      OR d.is_terminal_economic_revision IS DISTINCT FROM
         (d.receipt_no=t.terminal_receipt_no)
  );

ALTER TABLE dividend_source_receipt
    ALTER COLUMN terminal_receipt_no SET NOT NULL;
ALTER TABLE dividend_source_receipt
    ALTER COLUMN terminal_announcement_date SET NOT NULL;
ALTER TABLE dividend_source_receipt
    ALTER COLUMN is_terminal_economic_revision SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_source_receipt'::regclass
          AND conname='dividend_source_receipt_mapping_status_check'
    ) THEN
        ALTER TABLE dividend_source_receipt
            ADD CONSTRAINT dividend_source_receipt_mapping_status_check
            CHECK (mapping_status IN ('INCLUDED','EXCLUDED'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_source_receipt'::regclass
          AND conname='dividend_source_receipt_mapping_partition_check'
    ) THEN
        ALTER TABLE dividend_source_receipt
            ADD CONSTRAINT dividend_source_receipt_mapping_partition_check
            CHECK (
                (mapping_status='INCLUDED' AND asset_id IS NOT NULL
                 AND excluded_reason IS NULL)
                OR
                (mapping_status='EXCLUDED' AND excluded_reason IS NOT NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_source_receipt'::regclass
          AND conname='dividend_source_receipt_terminal_check'
    ) THEN
        ALTER TABLE dividend_source_receipt
            ADD CONSTRAINT dividend_source_receipt_terminal_check CHECK (
                is_terminal_economic_revision =
                    (receipt_no=terminal_receipt_no)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_dividend_resolution_run_applied
    ON dividend_event_resolution(quality_run_id, asset_id, applied_trade_date)
    WHERE is_canonical;

CREATE TABLE IF NOT EXISTS dart_action_snapshot_contract (
    quality_run_id UUID PRIMARY KEY REFERENCES dq_run(run_id),
    schema_version TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    body_digest TEXT NOT NULL CHECK (body_digest ~ '^[0-9a-f]{64}$'),
    body_count BIGINT NOT NULL CHECK (body_count > 0),
    coverage_start DATE NOT NULL,
    coverage_end DATE NOT NULL,
    action_count BIGINT NOT NULL CHECK (action_count > 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (coverage_start = DATE '2015-01-01'),
    CHECK (coverage_end >= coverage_start)
);

CREATE TABLE IF NOT EXISTS price_return_contract (
    source TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    methodology_version TEXT NOT NULL,
    dividend_treatment TEXT NOT NULL,
    status TEXT NOT NULL,
    coverage_start DATE,
    coverage_end DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    certified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, asset_type, field_name),
    CHECK (status IN ('BUILDING', 'CERTIFIED', 'FAILED')),
    CHECK (
        (status = 'CERTIFIED' AND certified_at IS NOT NULL)
        OR status <> 'CERTIFIED'
    ),
    CHECK (
        status <> 'CERTIFIED'
        OR (
            coverage_start >= DATE '2015-01-01'
            AND coverage_end >= coverage_start
        )
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='price_return_contract'::regclass
          AND conname='price_return_contract_certified_scope_check'
    ) THEN
        ALTER TABLE price_return_contract
            ADD CONSTRAINT price_return_contract_certified_scope_check
            CHECK (
                status <> 'CERTIFIED'
                OR (
                    coverage_start >= DATE '2015-01-01'
                    AND coverage_end >= coverage_start
                )
            );
    END IF;
END $$;

INSERT INTO price_return_contract (
    source,
    asset_type,
    field_name,
    methodology_version,
    dividend_treatment,
    status,
    metadata
) VALUES (
    'KRX',
    'stock',
    'total_return_close',
    'krx_gross_dividend_reinvested_v2',
    'gross_cash_dividend_reinvested_on_ex_date',
    'BUILDING',
    '{"reason":"awaiting certified dividend rebuild"}'::jsonb
)
ON CONFLICT (source, asset_type, field_name) DO NOTHING;

-- Any certification visible while 009 is being installed predates the final
-- content-addressed receipt/PIT contract, even if a pre-release process used
-- the same v2 methodology string.  Force one fresh audited rebuild.
UPDATE price_return_contract
SET status='BUILDING',
    metadata=coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
        'invalidated_reason', 'MIGRATION_009_REBUILD_REQUIRED',
        'invalidated_at', now()
    ),
    certified_at=NULL,
    updated_at=now()
WHERE source='KRX'
  AND asset_type='stock'
  AND field_name='total_return_close'
  AND status='CERTIFIED'
  AND metadata->>'contract_release' IS DISTINCT FROM
      'krx_total_return_v2_receipt_pit_parsed_digest_2026_08';

CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at,
       -- Preserve the deployed v1 ordinal contract through action_scope.
       -- PostgreSQL only permits CREATE OR REPLACE VIEW to retain existing
       -- column names/order and append new columns; this also preserves view
       -- dependencies, grants, ownership and comments without DROP CASCADE.
       report_name, action_scope, dart_rm, corp_cls,
       cash_amount_status, source_evidence_status,
       correction_of_action_key, revision_root_action_key, revision_kind,
       viewer_evidence_sha256, economic_evidence_sha256,
       reviewed_correction_id, payment_date_quality_status
FROM corporate_action
WHERE action_type = 'cash_dividend';
