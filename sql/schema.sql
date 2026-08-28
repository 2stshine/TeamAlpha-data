-- TeamAlpha silver 스키마 (PostgreSQL/RDS) — schema_tables.md 와 1:1
-- asset_id 를 중심으로 가격·재무·기업행사를 연결. source 컬럼·asset_identifier 로 소스 추가에 열려 있음.

-- 품질 실행 이력. Silver 행은 통과한 quality_run_id와 연결된다.
CREATE TABLE IF NOT EXISTS dq_run (
    run_id UUID PRIMARY KEY,
    parent_run_id UUID REFERENCES dq_run(run_id),
    mode TEXT NOT NULL,
    target_date DATE,
    partition_key TEXT,
    input_fingerprint TEXT,
    ruleset_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('RUNNING','BUILDING','VALIDATING','CERTIFIED','FAILED','SKIPPED')
    ),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    total_rule_count INTEGER NOT NULL DEFAULT 0,
    failed_rule_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dq_result (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key TEXT,
    dataset_name TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'ERROR', 'WARNING', 'MODIFIED', 'INFO')),
    status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    expected_value TEXT,
    actual_value TEXT,
    failed_count BIGINT NOT NULL DEFAULT 0,
    sample_records JSONB NOT NULL DEFAULT '[]'::jsonb,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dq_metric (
    metric_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    dataset_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    dimension JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_value DOUBLE PRECISION,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 일별 증분 warning의 현재 상태. 전체 관측 이력은 dq_result에 보존한다.
CREATE TABLE IF NOT EXISTS dq_warning_state (
    warning_state_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    target_date DATE,
    partition_key TEXT,
    dataset_name TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    first_seen_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    last_failed_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    last_evaluated_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    resolved_run_id UUID REFERENCES dq_run(run_id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    observation_count BIGINT NOT NULL DEFAULT 1,
    reopen_count BIGINT NOT NULL DEFAULT 0,
    latest_failed_count BIGINT NOT NULL DEFAULT 0,
    expected_value TEXT,
    actual_value TEXT,
    sample_records JSONB NOT NULL DEFAULT '[]'::jsonb,
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

-- 1. asset — 종목 마스터 (소스 독립 정체성)
CREATE TABLE IF NOT EXISTS asset (
    asset_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('stock', 'index', 'fx', 'commodity')),
    instrument_type TEXT NOT NULL DEFAULT 'unknown',
    exchange   TEXT NOT NULL,          -- 예: 'KRX'
    currency   TEXT NOT NULL,          -- 예: 'KRW'
    country_code TEXT,
    base_currency TEXT,
    price_unit TEXT,
    listed_from DATE,
    listed_to DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE asset ADD COLUMN IF NOT EXISTS price_unit TEXT;

-- 2. asset_identifier — 소스별 종목코드 매핑 (소스 추가 확장점)
CREATE TABLE IF NOT EXISTS asset_identifier (
    asset_id   BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source     TEXT NOT NULL,          -- 'KRX' | 'DART' | 'FMP'
    identifier TEXT NOT NULL,          -- ticker/corp code/CIK/CUSIP/ISIN/FX pair/원자재 심볼
    identifier_type TEXT NOT NULL DEFAULT 'ticker',
    valid_from DATE NOT NULL DEFAULT DATE '0001-01-01',
    valid_to DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, identifier_type, identifier, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_asset_identifier_lookup
    ON asset_identifier(source, identifier_type, identifier, valid_from, valid_to);
CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_identifier_current
    ON asset_identifier(source, identifier_type, identifier)
    WHERE valid_to IS NULL AND identifier_type <> 'cik';

-- 3. price_daily — 주식·지수·FX·원자재 일봉. shares/market_cap 흡수.
CREATE TABLE IF NOT EXISTS price_daily (
    asset_id      BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source        TEXT NOT NULL,       -- 가격 출처 (예: 'KRX')
    trade_date    DATE NOT NULL,
    open          NUMERIC(28,8),
    high          NUMERIC(28,8),
    low           NUMERIC(28,8),
    close         NUMERIC(28,8),
    adj_close     NUMERIC(28,8),       -- 분할 등 가격 조정 종가
    total_return_close NUMERIC(28,8),  -- 배당까지 반영한 총수익 지수형 종가
    currency      TEXT,
    vwap          NUMERIC(28,8),
    available_at  TIMESTAMPTZ,
    volume        BIGINT,
    trading_value NUMERIC(30,4),
    shares        BIGINT,              -- 상장주식수 (index는 NULL)
    market_cap    NUMERIC(30,4),       -- 시가총액. FMP는 원천에 없으면 NULL
    market        TEXT,                -- 주식시장 또는 'FX'; 지수·원자재는 NULL. 날짜별 값 — 아래 참고
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, trade_date)
);
-- market 은 asset 이 아니라 여기 있다: 종목이 시장을 옮기기 때문(KONEX→KOSDAQ 71건, KOSDAQ→KOSPI 16건).
-- 종목당 하나만 저장하면 승격 전 이력이 승격 후 시장으로 잘못 분류돼 유니버스가 오염된다.
-- 날짜별로 두면 `WHERE market IN ('KOSPI','KOSDAQ')` 이 자동으로 시점 정확(PIT)해진다.

-- 4. fundamental — 재무 (long, DART). 한 행 = 종목×회계기간×공시×지표.
CREATE TABLE IF NOT EXISTS fundamental (
    asset_id       BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source         TEXT NOT NULL,      -- 'DART' …
    period_end     DATE NOT NULL,      -- 회계기간 종료일
    fiscal_period  TEXT NOT NULL CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    fs_type        TEXT NOT NULL CHECK (fs_type IN ('CFS', 'OFS', 'UNKNOWN')),
    statement_type TEXT NOT NULL DEFAULT 'UNKNOWN', -- BS | IS | CF
    data_basis     TEXT NOT NULL DEFAULT 'STANDARDIZED',
    filing_id      TEXT,               -- 접수번호(rcept_no)
    filed          DATE,               -- 접수일
    available_date DATE,               -- PIT 사용가능일 (filed+1 or 법정기한+1)
    accepted_at    TIMESTAMPTZ,
    available_at   TIMESTAMPTZ,
    metric         TEXT NOT NULL,      -- 표준지표: revenue, net_income, total_equity…
    value          NUMERIC(30,6),
    currency       TEXT,
    unit_type      TEXT NOT NULL DEFAULT 'currency',
    revision_key   TEXT NOT NULL,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        asset_id, source, statement_type, data_basis, period_end,
        fiscal_period, fs_type, revision_key, metric
    )
);
-- PIT 조회용 (available_date <= 기준일 필터)
CREATE INDEX IF NOT EXISTS ix_fundamental_pit ON fundamental (asset_id, metric, available_date);

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

-- 5. corporate_action — 가격·주식수 변화를 설명하는 원천 기업행사.
CREATE TABLE IF NOT EXISTS corporate_action (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    action_key TEXT NOT NULL,
    action_type TEXT NOT NULL,
    announcement_date DATE,
    ex_date DATE,
    record_date DATE,
    payment_date DATE,
    cash_amount NUMERIC(28,8),
    adjusted_cash_amount NUMERIC(28,8),
    currency TEXT,
    frequency TEXT,
    ratio_numerator NUMERIC(28,8),
    ratio_denominator NUMERIC(28,8),
    expected_price_factor NUMERIC(28,12),
    share_count_factor NUMERIC(28,12),
    status TEXT NOT NULL DEFAULT 'confirmed',
    confidence TEXT,
    filing_id TEXT,
    report_name TEXT,
    action_scope TEXT NOT NULL DEFAULT 'UNKNOWN',
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, source, action_key)
);
-- 기존 DB에 schema.sql을 재적용해도 신규 배당 컬럼이 보강되도록 한다.
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS adjusted_cash_amount NUMERIC(28,8);
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS frequency TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT DEFAULT 'UNKNOWN';
CREATE INDEX IF NOT EXISTS ix_corporate_action_event
    ON corporate_action(asset_id, ex_date, action_type);
CREATE INDEX IF NOT EXISTS ix_corporate_action_dividend
    ON corporate_action(asset_id, ex_date DESC)
    WHERE action_type = 'cash_dividend';

-- 배당 연구용 최소 조회 인터페이스. 원천 행은 corporate_action에만 보관한다.
CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at,
       report_name, action_scope
FROM corporate_action
WHERE action_type = 'cash_dividend';

-- KRX gross total-return derivation audit and certification contract.
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
    PRIMARY KEY(asset_id, source, action_key, resolution_version)
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
    PRIMARY KEY(source, asset_type, field_name)
);

-- Deterministic Critical/Error invariants are also enforced by RDS. The
-- canonical deployed expressions live in migration 006_database_quality_guards.sql.
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='asset'::regclass AND conname='asset_critical_error_guard') THEN
ALTER TABLE asset ADD CONSTRAINT asset_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(name) <> '' AND btrim(asset_type) <> ''
        AND btrim(instrument_type) <> '' AND btrim(exchange) <> ''
        AND btrim(currency) <> '' AND base_currency IS NOT NULL
        AND btrim(base_currency) <> ''
        AND currency ~ '^[A-Z]{3}$' AND base_currency ~ '^[A-Z]{3}$'
        AND (
            (asset_type='stock' AND exchange IN ('KRX','NASDAQ','NYSE','AMEX')
             AND instrument_type IN ('common_stock','preferred_stock','adr','reit')
             AND price_unit IS NULL)
            OR (asset_type='index' AND exchange='KRX' AND instrument_type='index'
                AND price_unit IS NULL)
            OR (asset_type='fx' AND exchange='FX' AND instrument_type='fx'
                AND price_unit IS NULL)
            OR (asset_type='commodity' AND exchange='COMMODITY'
                AND instrument_type='commodity_future_continuous'
                AND currency='USD' AND base_currency='USD'
                AND price_unit IS NOT NULL AND btrim(price_unit)<>'')
        )
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='asset_identifier'::regclass AND conname='asset_identifier_critical_error_guard') THEN
ALTER TABLE asset_identifier ADD CONSTRAINT asset_identifier_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(identifier) <> '' AND btrim(identifier_type) <> ''
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='price_daily'::regclass AND conname='price_daily_critical_error_guard') THEN
ALTER TABLE price_daily ADD CONSTRAINT price_daily_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND close IS NOT NULL
        AND close::text NOT IN ('NaN','Infinity','-Infinity')
        AND adj_close IS NOT NULL
        AND adj_close::text NOT IN ('NaN','Infinity','-Infinity')
        AND (source='FMP_COMMODITY' OR (close>0 AND adj_close>0))
        AND (
            (open IS NULL AND high IS NULL AND low IS NULL
             AND source NOT IN ('FMP','FMP_FX','FMP_COMMODITY'))
            OR (open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                AND open::text NOT IN ('NaN','Infinity','-Infinity')
                AND high::text NOT IN ('NaN','Infinity','-Infinity')
                AND low::text NOT IN ('NaN','Infinity','-Infinity')
                AND (source='FMP_COMMODITY'
                     OR (open>0 AND high>0 AND low>0))
                AND high >= GREATEST(open,close)
                AND low <= LEAST(open,close))
        )
        AND (volume IS NULL OR volume >= 0)
        AND (trading_value IS NULL OR trading_value >= 0)
        AND (shares IS NULL OR shares > 0)
        AND (market_cap IS NULL OR market_cap >= 0)
        AND (total_return_close IS NULL OR total_return_close::text NOT IN ('NaN','Infinity','-Infinity'))
        AND (vwap IS NULL OR vwap::text NOT IN ('NaN','Infinity','-Infinity'))
        AND (currency IS NULL OR currency ~ '^[A-Z]{3}$')
        AND (source <> 'KRX' OR market IS NULL OR (market_cap IS NOT NULL AND market_cap > 0))
        AND (shares IS NULL OR market_cap IS NULL
             OR abs(market_cap-close*shares) <= abs(close*shares)*0.01)
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fundamental'::regclass AND conname='fundamental_critical_error_guard') THEN
ALTER TABLE fundamental ADD CONSTRAINT fundamental_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(statement_type) <> '' AND btrim(data_basis) <> ''
        AND btrim(fiscal_period) <> '' AND btrim(fs_type) <> ''
        AND btrim(revision_key) <> '' AND btrim(metric) <> ''
        AND btrim(unit_type) <> ''
        AND statement_type IN ('BS','IS','CF','DIVIDEND')
        AND value IS NOT NULL AND value::text NOT IN ('NaN','Infinity','-Infinity')
        AND available_date IS NOT NULL AND available_at IS NOT NULL
        AND ((unit_type='shares' AND (currency IS NULL OR currency ~ '^[A-Z]{3}$'))
             OR (unit_type<>'shares' AND currency IS NOT NULL
                 AND currency ~ '^[A-Z]{3}$'))
        AND (source<>'DART' OR (
            available_date>period_end
            AND (filed IS NULL OR (filed>=period_end AND available_date=filed+1))
            AND (fs_type IN ('CFS','OFS')
                 OR (statement_type='DIVIDEND' AND fs_type='UNKNOWN'))
        ))
        AND (statement_type<>'DIVIDEND'
             OR (metric='total_cash_dividend' AND unit_type='currency')
             OR (metric IN ('payout_ratio','dividend_yield') AND unit_type='percent')
             OR (metric='cash_dividend_per_share' AND unit_type='per_share')
             OR (metric='stock_dividend_per_share' AND unit_type='shares'))
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='corporate_action'::regclass AND conname='corporate_action_critical_error_guard') THEN
ALTER TABLE corporate_action ADD CONSTRAINT corporate_action_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(action_key) <> '' AND btrim(action_type) <> ''
        AND btrim(status) <> ''
        AND (source NOT LIKE 'FMP_%' OR ex_date IS NOT NULL)
    );
END IF; END $$;
