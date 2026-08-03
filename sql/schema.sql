-- TeamAlpha silver 스키마 (PostgreSQL/RDS) — schema_tables.md 와 1:1
-- asset_id 를 중심으로 가격·재무를 연결. source 컬럼·asset_identifier 로 소스 추가에 열려 있음.

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

-- 1. asset — 종목 마스터 (소스 독립 정체성)
CREATE TABLE IF NOT EXISTS asset (
    asset_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('stock', 'index', 'fx')),
    instrument_type TEXT NOT NULL DEFAULT 'unknown',
    exchange   TEXT NOT NULL,          -- 예: 'KRX'
    currency   TEXT NOT NULL,          -- 예: 'KRW'
    country_code TEXT,
    base_currency TEXT,
    listed_from DATE,
    listed_to DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. asset_identifier — 소스별 종목코드 매핑 (소스 추가 확장점)
CREATE TABLE IF NOT EXISTS asset_identifier (
    asset_id   BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source     TEXT NOT NULL,          -- 'KRX' | 'DART' | (향후 'YAHOO'·'SEC'…)
    identifier TEXT NOT NULL,          -- KRX='005930', DART='00126380'
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

-- 3. price_daily — 일봉 (주식 + 지수 공용). shares/market_cap 흡수.
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
    market        TEXT,                -- 'KOSPI' | 'KOSDAQ' | 'KONEX' (index는 NULL). 날짜별 값 — 아래 참고
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
    currency TEXT,
    ratio_numerator NUMERIC(28,8),
    ratio_denominator NUMERIC(28,8),
    expected_price_factor NUMERIC(28,12),
    share_count_factor NUMERIC(28,12),
    status TEXT NOT NULL DEFAULT 'confirmed',
    confidence TEXT,
    filing_id TEXT,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, source, action_key)
);
CREATE INDEX IF NOT EXISTS ix_corporate_action_event
    ON corporate_action(asset_id, ex_date, action_type);
