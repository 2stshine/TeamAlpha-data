-- Silver quality audit schema and persistent first-backfill staging.

CREATE TABLE IF NOT EXISTS dq_run (
    run_id            UUID PRIMARY KEY,
    parent_run_id     UUID REFERENCES dq_run(run_id),
    mode              TEXT NOT NULL,
    target_date       DATE,
    partition_key     TEXT,
    input_fingerprint TEXT,
    ruleset_version   TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (
        status IN ('RUNNING', 'BUILDING', 'VALIDATING', 'CERTIFIED', 'FAILED', 'SKIPPED')
    ),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    total_rule_count  INTEGER NOT NULL DEFAULT 0,
    failed_rule_count INTEGER NOT NULL DEFAULT 0,
    error_message     TEXT
);
CREATE INDEX IF NOT EXISTS ix_dq_run_parent ON dq_run(parent_run_id);
CREATE INDEX IF NOT EXISTS ix_dq_run_target ON dq_run(mode, target_date, started_at DESC);

CREATE TABLE IF NOT EXISTS dq_result (
    result_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key  TEXT,
    dataset_name   TEXT NOT NULL,
    rule_code      TEXT NOT NULL,
    severity       TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'ERROR', 'WARNING', 'MODIFIED', 'INFO')),
    status         TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    expected_value TEXT,
    actual_value   TEXT,
    failed_count   BIGINT NOT NULL DEFAULT 0,
    sample_records JSONB NOT NULL DEFAULT '[]'::jsonb,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_result_run ON dq_result(run_id, severity, status);

-- 기존 DB의 severity CHECK 제약을 MODIFIED 등급까지 허용하도록 갱신한다.
-- (과거 INFO 행 검증을 위해 INFO도 유지한다. DROP IF EXISTS로 재실행 안전.)
ALTER TABLE dq_result DROP CONSTRAINT IF EXISTS dq_result_severity_check;
ALTER TABLE dq_result
    ADD CONSTRAINT dq_result_severity_check
    CHECK (severity IN ('CRITICAL', 'ERROR', 'WARNING', 'MODIFIED', 'INFO'));

CREATE TABLE IF NOT EXISTS dq_metric (
    metric_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    dataset_name  TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    dimension     JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_value  DOUBLE PRECISION,
    measured_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_metric_baseline
    ON dq_metric(dataset_name, metric_name, measured_at DESC);

ALTER TABLE asset ADD COLUMN IF NOT EXISTS quality_run_id UUID REFERENCES dq_run(run_id);
ALTER TABLE asset ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE asset_identifier ADD COLUMN IF NOT EXISTS quality_run_id UUID REFERENCES dq_run(run_id);
ALTER TABLE asset_identifier ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS quality_run_id UUID REFERENCES dq_run(run_id);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS quality_run_id UUID REFERENCES dq_run(run_id);
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE fundamental ADD COLUMN IF NOT EXISTS revision_key TEXT;

UPDATE fundamental
SET revision_key = COALESCE(
    NULLIF(filing_id, ''),
    'legacy:' || period_end::text || ':' || fiscal_period || ':' || fs_type
)
WHERE revision_key IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'fundamental'::regclass
          AND conname = 'fundamental_pkey'
          AND pg_get_constraintdef(oid) NOT LIKE '%revision_key%'
    ) THEN
        ALTER TABLE fundamental DROP CONSTRAINT fundamental_pkey;
        ALTER TABLE fundamental ALTER COLUMN revision_key SET NOT NULL;
        ALTER TABLE fundamental ADD CONSTRAINT fundamental_pkey PRIMARY KEY (
            asset_id, source, period_end, fiscal_period, fs_type, revision_key, metric
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'asset_identifier'::regclass
          AND conname = 'uq_asset_identifier_source_identifier'
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = 'uq_asset_identifier_source_identifier'
          AND c.relkind = 'i'
    ) THEN
        ALTER TABLE asset_identifier
        ADD CONSTRAINT uq_asset_identifier_source_identifier UNIQUE (source, identifier);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_fundamental_pit_revision
    ON fundamental(asset_id, metric, available_date, revision_key);

CREATE OR REPLACE VIEW fundamental_current AS
SELECT asset_id, source, period_end, fiscal_period, fs_type, filing_id, filed,
       available_date, metric, value, currency, revision_key, quality_run_id, loaded_at
FROM (
    SELECT f.*,
           row_number() OVER (
               PARTITION BY asset_id, source, period_end, fiscal_period, fs_type, metric
               ORDER BY available_date DESC NULLS LAST, revision_key DESC
           ) AS rn
    FROM fundamental f
) ranked
WHERE rn = 1;

CREATE SCHEMA IF NOT EXISTS quality_stage;

CREATE TABLE IF NOT EXISTS quality_stage.asset (
    backfill_run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key   TEXT NOT NULL,
    natural_key     TEXT NOT NULL,
    name            TEXT NOT NULL,
    asset_type      TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    currency        TEXT NOT NULL,
    source_file     TEXT,
    PRIMARY KEY(backfill_run_id, natural_key)
);

CREATE TABLE IF NOT EXISTS quality_stage.asset_identifier (
    backfill_run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key   TEXT NOT NULL,
    natural_key     TEXT NOT NULL,
    source          TEXT NOT NULL,
    identifier      TEXT NOT NULL,
    source_file     TEXT,
    PRIMARY KEY(backfill_run_id, source, identifier)
);

CREATE TABLE IF NOT EXISTS quality_stage.price_daily (
    backfill_run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key   TEXT NOT NULL,
    identifier      TEXT NOT NULL,
    source          TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open            NUMERIC(18,4),
    high            NUMERIC(18,4),
    low             NUMERIC(18,4),
    close           NUMERIC(18,4),
    adj_close       NUMERIC(18,4),
    volume          BIGINT,
    trading_value   NUMERIC(20,2),
    shares          BIGINT,
    market_cap      NUMERIC(24,2),
    market          TEXT,
    asset_type      TEXT NOT NULL,
    prev_diff       NUMERIC(18,4),
    fluc_rate       NUMERIC(12,6),
    source_file     TEXT,
    PRIMARY KEY(backfill_run_id, identifier, source, trade_date)
);

CREATE TABLE IF NOT EXISTS quality_stage.fundamental (
    backfill_run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key   TEXT NOT NULL,
    identifier      TEXT NOT NULL,
    source          TEXT NOT NULL,
    period_end      DATE NOT NULL,
    fiscal_period   TEXT NOT NULL,
    fs_type         TEXT NOT NULL,
    filing_id       TEXT,
    filed           DATE,
    available_date  DATE NOT NULL,
    metric          TEXT NOT NULL,
    value           NUMERIC(20,2),
    currency        TEXT NOT NULL,
    revision_key    TEXT NOT NULL,
    source_file     TEXT,
    PRIMARY KEY (
        backfill_run_id, identifier, source, period_end,
        fiscal_period, fs_type, revision_key, metric
    )
);
