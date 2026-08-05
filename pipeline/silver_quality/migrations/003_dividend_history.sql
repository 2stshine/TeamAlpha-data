-- Silver dividend support without adding another persistent table.
-- Raw/source amount is retained separately from a split-adjusted comparable amount.

ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS adjusted_cash_amount NUMERIC(28,8);
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS frequency TEXT;

ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS adjusted_cash_amount NUMERIC(28,8);
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS frequency TEXT;

CREATE INDEX IF NOT EXISTS ix_corporate_action_dividend
    ON corporate_action(asset_id, ex_date DESC)
    WHERE action_type = 'cash_dividend';

CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at
FROM corporate_action
WHERE action_type = 'cash_dividend';
