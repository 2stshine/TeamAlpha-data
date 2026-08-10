-- Manual acknowledgement for the warning worklist.
--
-- Warnings are now tracked for every silver-loading mode (daily + all backfill/
-- rebuild passes), not just daily. Most of the accumulated warnings are
-- structural and benign (e.g. pre-2015 KRX resets with no DART basis), so the
-- reviewer needs a way to mark one "reviewed / accepted" and drop it from the
-- open worklist without it being a PASS. That is the ACKNOWLEDGED state:
--
--   OPEN         -> currently failing, needs review
--   ACKNOWLEDGED -> reviewed & accepted; stays down across re-runs as long as
--                   the observed value is unchanged (reopens if it changes)
--   RESOLVED     -> a later PASS of the same scope cleared it automatically
--
-- acknowledged_fingerprint stores the actual_value at ack time; a re-run whose
-- actual_value still matches keeps the row ACKNOWLEDGED, a changed value reopens
-- it to OPEN so a materially different failure is not silently suppressed.

ALTER TABLE dq_warning_state
    ADD COLUMN IF NOT EXISTS acknowledged_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS acknowledged_by          TEXT,
    ADD COLUMN IF NOT EXISTS review_note              TEXT,
    ADD COLUMN IF NOT EXISTS acknowledged_fingerprint TEXT;

ALTER TABLE dq_warning_state DROP CONSTRAINT IF EXISTS dq_warning_state_status_check;
ALTER TABLE dq_warning_state
    ADD CONSTRAINT dq_warning_state_status_check
    CHECK (status IN ('OPEN', 'RESOLVED', 'ACKNOWLEDGED'));

-- The open worklist excludes ACKNOWLEDGED automatically (status <> 'OPEN').
-- Recreate the view so it also surfaces the review columns for OPEN rows.
CREATE OR REPLACE VIEW dq_open_warning AS
SELECT warning_state_id, mode, scope_key, target_date, partition_key,
       dataset_name, rule_code, first_seen_run_id, last_failed_run_id,
       last_evaluated_run_id, first_seen_at, last_failed_at,
       last_evaluated_at, observation_count, reopen_count,
       latest_failed_count, expected_value, actual_value, sample_records
FROM dq_warning_state
WHERE status = 'OPEN';

-- Companion view: everything a reviewer has already signed off on.
CREATE OR REPLACE VIEW dq_acknowledged_warning AS
SELECT warning_state_id, mode, scope_key, target_date, partition_key,
       dataset_name, rule_code, last_failed_run_id, last_failed_at,
       observation_count, reopen_count, latest_failed_count,
       expected_value, actual_value, acknowledged_at, acknowledged_by,
       review_note, sample_records
FROM dq_warning_state
WHERE status = 'ACKNOWLEDGED';

CREATE INDEX IF NOT EXISTS ix_dq_warning_state_acknowledged
    ON dq_warning_state(rule_code, acknowledged_at DESC)
    WHERE status = 'ACKNOWLEDGED';
