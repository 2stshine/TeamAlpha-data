-- Exact source and runtime lineage for cash dividends that coincide with a
-- KRX reference-price adjustment.  Migration 009 remains checksum-frozen.

ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS source_body_sha256 TEXT;
ALTER TABLE quality_stage.corporate_action
    ADD COLUMN IF NOT EXISTS source_body_sha256 TEXT;

CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at,
       report_name, action_scope, dart_rm, corp_cls,
       cash_amount_status, source_evidence_status,
       correction_of_action_key, revision_root_action_key, revision_kind,
       viewer_evidence_sha256, economic_evidence_sha256,
       reviewed_correction_id, payment_date_quality_status,
       source_body_sha256
FROM corporate_action
WHERE action_type='cash_dividend';

CREATE TABLE IF NOT EXISTS cash_adjustment_scale_source_evidence (
    action_snapshot_run_id UUID NOT NULL
        REFERENCES dart_action_snapshot_contract(quality_run_id),
    evidence_key TEXT NOT NULL,
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    cash_receipt_no TEXT NOT NULL,
    cash_source_evidence_status TEXT NOT NULL,
    cash_action_body_path TEXT NOT NULL,
    cash_action_body_sha256 TEXT NOT NULL,
    cash_economic_body_path TEXT NOT NULL,
    cash_economic_body_schema TEXT NOT NULL,
    cash_economic_sha256 TEXT NOT NULL,
    support_action_count INTEGER NOT NULL,
    support_action_digest TEXT NOT NULL,
    support_semantic_group_count INTEGER NOT NULL,
    price_source TEXT NOT NULL,
    previous_price_source_object_key TEXT NOT NULL,
    previous_price_source_content_sha256 TEXT NOT NULL,
    previous_price_source_etag TEXT NOT NULL,
    previous_price_source_schema TEXT NOT NULL,
    adjustment_price_source_object_key TEXT NOT NULL,
    adjustment_price_source_content_sha256 TEXT NOT NULL,
    adjustment_price_source_etag TEXT NOT NULL,
    adjustment_price_source_schema TEXT NOT NULL,
    previous_trade_date DATE NOT NULL,
    adjustment_trade_date DATE NOT NULL,
    raw_previous_close NUMERIC(28,8) NOT NULL,
    raw_applied_close NUMERIC(28,8) NOT NULL,
    raw_reference_price NUMERIC(28,8) NOT NULL,
    expected_price_factor NUMERIC(28,12) NOT NULL,
    cash_scale_basis TEXT NOT NULL,
    manifest_row_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(action_snapshot_run_id, evidence_key),
    UNIQUE(
        action_snapshot_run_id, asset_id, cash_receipt_no,
        adjustment_trade_date
    ),
    UNIQUE(
        action_snapshot_run_id, evidence_key, cash_receipt_no,
        adjustment_trade_date
    ),
    FOREIGN KEY(action_snapshot_run_id, cash_receipt_no)
        REFERENCES dividend_source_receipt(quality_run_id, receipt_no),
    CHECK (length(btrim(evidence_key)) BETWEEN 1 AND 300),
    CHECK (ticker ~ '^[0-9A-Z]{6}$'),
    CHECK (cash_receipt_no ~ '^[0-9]{14}$'),
    CHECK (cash_source_evidence_status IN (
        'VERIFIED_OPENDART_DOCUMENT',
        'VERIFIED_DART_VIEWER_BODY',
        'VERIFIED_REVIEWED_SOURCE_ERRATUM'
    )),
    CHECK (length(btrim(cash_action_body_path)) > 0),
    CHECK (cash_action_body_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (length(btrim(cash_economic_body_path)) > 0),
    CHECK (cash_economic_body_schema IN (
        'OPENDART_DOCUMENT_ZIP_V1',
        'DART_VIEWER_HTML_V1',
        'REVIEWED_PERIODIC_JSON_V1'
    )),
    CHECK (cash_economic_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (support_action_count > 0),
    CHECK (support_action_digest ~ '^[0-9a-f]{64}$'),
    CHECK (support_semantic_group_count > 0),
    CHECK (support_semantic_group_count <= support_action_count),
    CHECK (price_source='KRX'),
    CHECK (length(btrim(previous_price_source_object_key)) > 0),
    CHECK (previous_price_source_content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (previous_price_source_etag ~ '^[0-9a-f]{32}(-[0-9]+)?$'),
    CHECK (previous_price_source_schema IN (
        'marcap_parquet_v1','krxapi_stock_parquet_v1'
    )),
    CHECK (length(btrim(adjustment_price_source_object_key)) > 0),
    CHECK (adjustment_price_source_content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (adjustment_price_source_etag ~ '^[0-9a-f]{32}(-[0-9]+)?$'),
    CHECK (adjustment_price_source_schema IN (
        'marcap_parquet_v1','krxapi_stock_parquet_v1'
    )),
    CHECK (previous_trade_date < adjustment_trade_date),
    CHECK (raw_previous_close > 0),
    CHECK (raw_applied_close > 0),
    CHECK (raw_reference_price > 0),
    CHECK (expected_price_factor > 0),
    CHECK (cash_scale_basis='PRE_EVENT_PRICE_SCALE'),
    CHECK (manifest_row_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS cash_adjustment_scale_support_action (
    action_snapshot_run_id UUID NOT NULL,
    evidence_key TEXT NOT NULL,
    support_action_source TEXT NOT NULL,
    support_action_key TEXT NOT NULL,
    support_action_type TEXT NOT NULL,
    target_cash_receipt_no TEXT NOT NULL,
    target_adjustment_date DATE NOT NULL,
    support_action_body_path TEXT NOT NULL,
    support_action_body_sha256 TEXT NOT NULL,
    support_action_quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    support_announcement_date DATE,
    support_ex_date DATE,
    support_record_date DATE,
    support_ratio_numerator NUMERIC(28,8),
    support_ratio_denominator NUMERIC(28,8),
    support_entitlement_security_class TEXT,
    support_distributed_security_class TEXT,
    support_expected_price_factor NUMERIC(28,12),
    support_reference_price NUMERIC(28,8),
    support_reason TEXT,
    support_report_name TEXT NOT NULL,
    support_action_scope TEXT NOT NULL,
    support_semantic_group_keys TEXT NOT NULL,
    support_semantic_role TEXT NOT NULL,
    manifest_support_row_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(
        action_snapshot_run_id, evidence_key,
        support_action_source, support_action_key, support_action_type
    ),
    FOREIGN KEY(action_snapshot_run_id, evidence_key)
        REFERENCES cash_adjustment_scale_source_evidence(
            action_snapshot_run_id, evidence_key
        ),
    CONSTRAINT cash_scale_support_parent_identity_fk FOREIGN KEY(
        action_snapshot_run_id, evidence_key,
        target_cash_receipt_no, target_adjustment_date
    ) REFERENCES cash_adjustment_scale_source_evidence(
        action_snapshot_run_id, evidence_key,
        cash_receipt_no, adjustment_trade_date
    ),
    CHECK (action_snapshot_run_id=support_action_quality_run_id),
    CHECK (length(btrim(support_action_source)) > 0),
    CHECK (length(btrim(support_action_key)) > 0),
    CHECK (length(btrim(support_action_type)) > 0),
    CHECK (target_cash_receipt_no ~ '^[0-9]{14}$'),
    CHECK (length(btrim(support_action_body_path)) > 0),
    CHECK (support_action_body_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        support_ratio_numerator IS NULL
        OR support_ratio_numerator > 0
    ),
    CHECK (
        support_ratio_denominator IS NULL
        OR support_ratio_denominator > 0
    ),
    CHECK (
        (support_ratio_numerator IS NULL) =
        (support_ratio_denominator IS NULL)
    ),
    CHECK (
        support_entitlement_security_class IS NULL
        OR support_entitlement_security_class IN (
            'COMMON','PREFERRED','COMMON_AND_PREFERRED'
        )
    ),
    CHECK (
        support_distributed_security_class IS NULL
        OR support_distributed_security_class IN (
            'COMMON','PREFERRED','NEW_PREFERRED'
        )
    ),
    CHECK (
        support_semantic_role <> 'ADJUSTMENT_COMPONENT'
        OR (
            support_entitlement_security_class IS NOT NULL
            AND support_distributed_security_class IS NOT NULL
        )
    ),
    CHECK (
        support_expected_price_factor IS NULL
        OR support_expected_price_factor > 0
    ),
    CHECK (support_reference_price IS NULL OR support_reference_price > 0),
    CHECK (
        support_action_source<>'DART_VIEWER'
        OR (
            support_action_key ~ '^[0-9]{14}$'
            AND support_action_body_path ~
                '^corporate_actions/dart/support_action_families/objects/'
                'sha256=[0-9a-f]{64}\.html$'
            AND support_action_body_path=
                'corporate_actions/dart/support_action_families/objects/'
                'sha256=' || support_action_body_sha256 || '.html'
            AND (
                (support_action_type='bonus_issue'
                 AND support_ex_date IS NOT NULL
                 AND support_record_date IS NULL
                 AND support_expected_price_factor IS NOT NULL)
                OR
                (support_action_type='stock_dividend'
                 AND support_ex_date IS NULL
                 AND support_record_date IS NOT NULL
                 AND support_expected_price_factor IS NULL
                 AND support_ratio_numerator IS NOT NULL
                 AND support_entitlement_security_class='COMMON'
                 AND support_distributed_security_class='COMMON')
            )
        )
    ),
    CHECK (length(btrim(support_report_name)) > 0),
    CHECK (support_action_scope='ISSUER'),
    CONSTRAINT cash_scale_support_source_type_check CHECK (
        (support_action_source='DART_STRUCTURED'
         AND support_action_type='bonus_issue')
        OR (support_action_source='DART_VIEWER'
            AND support_action_type IN ('bonus_issue','stock_dividend'))
        OR (support_action_source='DART_DISCLOSURE'
            AND support_action_type IN (
                'stock_dividend','ex_dividend','rights_detachment',
                'combined_detachment'
            ))
        OR (support_action_source='KRX_KIND'
            AND support_action_type IN (
                'stock_dividend','ex_dividend','rights_detachment',
                'combined_detachment'
            ))
        OR (support_action_source='KRX_KIND'
            AND support_action_type='paid_increase'
            AND support_action_key='20180201000086'
            AND support_action_body_sha256=
                'cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9')
    ),
    CONSTRAINT cash_scale_support_role_semantics_check CHECK (
        (
            support_semantic_role='ADJUSTMENT_COMPONENT'
            AND (
                (support_action_source IN (
                    'DART_STRUCTURED','DART_VIEWER'
                 )
                 AND support_action_type='bonus_issue'
                 AND support_ratio_numerator IS NOT NULL
                 AND support_entitlement_security_class='COMMON'
                 AND support_distributed_security_class='COMMON'
                 AND support_expected_price_factor IS NOT NULL
                 AND support_expected_price_factor=round(
                     1::numeric / (
                         1::numeric + support_ratio_numerator /
                         support_ratio_denominator
                     ), 12
                 ))
                OR (
                    support_action_source IN (
                        'DART_DISCLOSURE','DART_VIEWER','KRX_KIND'
                    )
                    AND support_action_type='stock_dividend'
                    AND support_ratio_numerator IS NOT NULL
                    AND (
                        (support_entitlement_security_class='COMMON'
                         AND support_distributed_security_class='COMMON')
                        OR
                        (support_entitlement_security_class=
                            'COMMON_AND_PREFERRED'
                         AND support_distributed_security_class=
                            'NEW_PREFERRED')
                    )
                )
                OR (
                    support_action_source='KRX_KIND'
                    AND support_action_type='paid_increase'
                    AND support_action_key='20180201000086'
                    AND support_action_body_sha256=
                        'cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9'
                    AND support_ratio_numerator=0.1456981704
                    AND support_ratio_denominator=1
                    AND support_entitlement_security_class='COMMON'
                    AND support_distributed_security_class='COMMON'
                    AND support_expected_price_factor IS NULL
                    AND support_record_date=DATE '2017-12-31'
                )
            )
        ) OR (
            support_semantic_role='CORROBORATION'
            AND support_action_source IN ('DART_DISCLOSURE','KRX_KIND')
            AND support_action_type IN (
                'ex_dividend','rights_detachment','combined_detachment'
            )
            AND (
                support_action_source<>'KRX_KIND'
                OR (
                    support_entitlement_security_class IN (
                        'COMMON','PREFERRED'
                    )
                    AND support_distributed_security_class IS NULL
                    AND support_ratio_numerator IS NULL
                    AND support_ratio_denominator IS NULL
                    AND support_reference_price IS NOT NULL
                )
            )
        )
    ),
    CHECK (
        jsonb_typeof(support_semantic_group_keys::jsonb)='array'
        AND jsonb_array_length(support_semantic_group_keys::jsonb) > 0
    ),
    CHECK (support_semantic_role IN (
        'ADJUSTMENT_COMPONENT','CORROBORATION'
    )),
    CHECK (manifest_support_row_sha256 ~ '^[0-9a-f]{64}$')
);

COMMENT ON COLUMN cash_adjustment_scale_support_action.support_action_source IS
    'DART_VIEWER is an official content-addressed viewer-body bonus or stock-dividend component; it is distinct from an OpenDART structured API/disclosure row.';

ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS previous_trade_date DATE;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS previous_close NUMERIC(28,8);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS previous_adj_close NUMERIC(28,8);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS applied_close NUMERIC(28,8);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS applied_adj_close NUMERIC(28,8);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS previous_price_scale NUMERIC(28,12);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS applied_price_scale NUMERIC(28,12);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS selected_cash_scale NUMERIC(28,12);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS cash_adjustment_scale_basis TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_change_detected BOOLEAN;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_evidence_action_snapshot_run_id UUID;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_evidence_key TEXT;
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_price_factor_observed NUMERIC(28,12);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_price_factor_reference NUMERIC(28,12);
ALTER TABLE dividend_event_resolution
    ADD COLUMN IF NOT EXISTS scale_price_factor_parity BOOLEAN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_event_resolution'::regclass
          AND conname='dividend_resolution_scale_evidence_fk'
    ) THEN
        ALTER TABLE dividend_event_resolution
            ADD CONSTRAINT dividend_resolution_scale_evidence_fk
            FOREIGN KEY(
                scale_evidence_action_snapshot_run_id, scale_evidence_key
            ) REFERENCES cash_adjustment_scale_source_evidence(
                action_snapshot_run_id, evidence_key
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_event_resolution'::regclass
          AND conname='dividend_resolution_v2_scale_contract_check'
    ) THEN
        ALTER TABLE dividend_event_resolution
            ADD CONSTRAINT dividend_resolution_v2_scale_contract_check CHECK (
                resolution_version <> 'krx_dividend_resolution_v2'
                OR (
                    (
                        is_canonical
                        AND excluded_reason IS NULL
                        AND applied_trade_date IS NOT NULL
                        AND previous_trade_date IS NOT NULL
                        AND previous_close > 0
                        AND previous_adj_close > 0
                        AND applied_close > 0
                        AND applied_adj_close > 0
                        AND previous_price_scale > 0
                        AND applied_price_scale > 0
                        AND selected_cash_scale > 0
                        AND scale_change_detected IS NOT NULL
                        AND scale_price_factor_observed > 0
                        AND scale_price_factor_reference > 0
                        AND scale_price_factor_parity
                        AND (
                            (
                                NOT scale_change_detected
                                AND cash_adjustment_scale_basis=
                                    'STABLE_PRICE_SCALE'
                                AND scale_evidence_action_snapshot_run_id
                                    IS NULL
                                AND scale_evidence_key IS NULL
                            ) OR (
                                scale_change_detected
                                AND cash_adjustment_scale_basis=
                                    'PRE_EVENT_PRICE_SCALE'
                                AND scale_evidence_action_snapshot_run_id
                                    IS NOT NULL
                                AND scale_evidence_key IS NOT NULL
                            )
                        )
                    ) OR (
                        NOT is_canonical
                        AND excluded_reason IS NOT NULL
                        AND applied_trade_date IS NULL
                        AND previous_trade_date IS NULL
                        AND previous_close IS NULL
                        AND previous_adj_close IS NULL
                        AND applied_close IS NULL
                        AND applied_adj_close IS NULL
                        AND previous_price_scale IS NULL
                        AND applied_price_scale IS NULL
                        AND selected_cash_scale IS NULL
                        AND cash_adjustment_scale_basis IS NULL
                        AND scale_change_detected IS NULL
                        AND scale_evidence_action_snapshot_run_id IS NULL
                        AND scale_evidence_key IS NULL
                        AND scale_price_factor_observed IS NULL
                        AND scale_price_factor_reference IS NULL
                        AND scale_price_factor_parity IS NULL
                    )
                )
            );
    END IF;
END $$;

-- Any visible v2 certification predates row-level collision evidence.  Leave
-- the contract BUILDING until a full resolution-v2 rebuild certifies it.
UPDATE price_return_contract
SET methodology_version='krx_gross_dividend_reinvested_v3',
    status='BUILDING',
    coverage_start=NULL,
    coverage_end=NULL,
    quality_run_id=NULL,
    metadata=coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
        'invalidated_reason', 'MIGRATION_010_SCALE_EVIDENCE_REQUIRED',
        'invalidated_at', now()
    ),
    certified_at=NULL,
    updated_at=now()
WHERE source='KRX'
  AND asset_type='stock'
  AND field_name='total_return_close'
  AND (
      methodology_version IS DISTINCT FROM
          'krx_gross_dividend_reinvested_v3'
      OR (
          status='CERTIFIED'
          AND metadata->>'contract_release' IS DISTINCT FROM
              'krx_total_return_v3_cash_scale_evidence_2026_08'
      )
  );
