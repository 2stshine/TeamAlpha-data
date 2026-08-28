-- Auditable KRX gross-total-return support.
-- Applying this migration deliberately leaves the contract in BUILDING state;
-- research must not treat total_return_close as dividend-inclusive until a
-- certified rebuild promotes it to CERTIFIED.

ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT;

ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT;

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

CREATE TABLE IF NOT EXISTS dividend_event_resolution (
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
    quality_run_id UUID REFERENCES dq_run(run_id),
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, action_key, resolution_version),
    CHECK (
        (is_canonical AND excluded_reason IS NULL)
        OR (NOT is_canonical AND excluded_reason IS NOT NULL)
    ),
    CHECK (
        ex_date_basis IS NULL
        OR ex_date_basis IN ('KRX_NOTICE', 'KRX_T2_INFERRED')
    )
);

CREATE INDEX IF NOT EXISTS ix_dividend_resolution_applied
    ON dividend_event_resolution(asset_id, applied_trade_date)
    WHERE is_canonical;

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
    )
);

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
    'krx_gross_dividend_reinvested_v1',
    'gross_cash_dividend_reinvested_on_ex_date',
    'BUILDING',
    '{"reason":"awaiting certified dividend rebuild"}'::jsonb
)
ON CONFLICT (source, asset_type, field_name) DO NOTHING;

CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at,
       report_name, action_scope
FROM corporate_action
WHERE action_type = 'cash_dividend';
