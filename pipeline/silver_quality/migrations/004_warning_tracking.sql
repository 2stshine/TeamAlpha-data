-- Incremental warning lifecycle tracking.
-- dq_result remains the immutable observation log; this table is the compact
-- projection used to query only warnings that have not passed a recheck of the
-- same incremental scope.

CREATE TABLE IF NOT EXISTS dq_warning_state (
    warning_state_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode                   TEXT NOT NULL,
    scope_key              TEXT NOT NULL,
    target_date            DATE,
    partition_key          TEXT,
    dataset_name           TEXT NOT NULL,
    rule_code              TEXT NOT NULL,
    status                 TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    first_seen_run_id      UUID NOT NULL REFERENCES dq_run(run_id),
    last_failed_run_id     UUID NOT NULL REFERENCES dq_run(run_id),
    last_evaluated_run_id  UUID NOT NULL REFERENCES dq_run(run_id),
    resolved_run_id        UUID REFERENCES dq_run(run_id),
    first_seen_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_evaluated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at            TIMESTAMPTZ,
    observation_count      BIGINT NOT NULL DEFAULT 1,
    reopen_count           BIGINT NOT NULL DEFAULT 0,
    latest_failed_count    BIGINT NOT NULL DEFAULT 0,
    expected_value         TEXT,
    actual_value           TEXT,
    sample_records         JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE (mode, scope_key, dataset_name, rule_code)
);

CREATE INDEX IF NOT EXISTS ix_dq_warning_state_open
    ON dq_warning_state(mode, last_failed_at DESC)
    WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS ix_dq_warning_state_rule
    ON dq_warning_state(rule_code, status, last_failed_at DESC);

CREATE OR REPLACE VIEW dq_open_warning AS
SELECT warning_state_id, mode, scope_key, target_date, partition_key,
       dataset_name, rule_code, first_seen_run_id, last_failed_run_id,
       last_evaluated_run_id, first_seen_at, last_failed_at,
       last_evaluated_at, observation_count, reopen_count,
       latest_failed_count, expected_value, actual_value, sample_records
FROM dq_warning_state
WHERE status = 'OPEN';
