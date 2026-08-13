-- Keep published corporate actions byte-for-byte comparable with the reviewed
-- cash-scale support rows widened by migration 011.
ALTER TABLE corporate_action
    ALTER COLUMN ratio_numerator TYPE NUMERIC(28,12),
    ALTER COLUMN ratio_denominator TYPE NUMERIC(28,12);

ALTER TABLE quality_stage.corporate_action
    ALTER COLUMN ratio_numerator TYPE NUMERIC(28,12),
    ALTER COLUMN ratio_denominator TYPE NUMERIC(28,12);
