-- Preserve reviewed share ratios used by the exact cash-scale support contract.
-- Migration 010 declared values such as 0.1456981704 in its CHECK contract but
-- stored the underlying columns at only eight decimal places.
ALTER TABLE cash_adjustment_scale_support_action
    ALTER COLUMN support_ratio_numerator TYPE NUMERIC(28,12),
    ALTER COLUMN support_ratio_denominator TYPE NUMERIC(28,12);
