-- TeamAlpha Gold v1 — 확정 최소 스키마
-- 기존 Silver RDS의 같은 database 안에 gold schema로 생성한다.

CREATE SCHEMA IF NOT EXISTS gold;

-- 1. 팩터 정의·버전·설정·최신 평가.
CREATE TABLE IF NOT EXISTS gold.factor (
    factor_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    factor_key          TEXT NOT NULL,
    version             INTEGER NOT NULL CHECK (version > 0),
    description         TEXT NOT NULL,
    implementation_uri  TEXT NOT NULL,
    implementation_hash TEXT NOT NULL,
    config              JSONB NOT NULL,
    evaluation          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'CANDIDATE'
                            CHECK (
                                status IN (
                                    'CANDIDATE', 'APPROVED',
                                    'REJECTED', 'RETIRED'
                                )
                            ),
    CONSTRAINT uq_gold_factor_version
        UNIQUE (factor_key, version),
    CONSTRAINT ck_gold_factor_key
        CHECK (factor_key ~ '^[a-z][a-z0-9_]*$'),
    CONSTRAINT ck_gold_factor_implementation
        CHECK (
            NULLIF(btrim(implementation_uri), '') IS NOT NULL
            AND NULLIF(btrim(implementation_hash), '') IS NOT NULL
        ),
    CONSTRAINT ck_gold_factor_json_objects
        CHECK (
            jsonb_typeof(config) = 'object'
            AND jsonb_typeof(evaluation) = 'object'
        ),
    CONSTRAINT ck_gold_factor_decision
        CHECK (
            status = 'CANDIDATE'
            OR (
                status = 'APPROVED'
                AND evaluation @> '{"passed": true}'::jsonb
            )
            OR (
                status = 'REJECTED'
                AND evaluation @> '{"passed": false}'::jsonb
            )
            OR (
                status = 'RETIRED'
                AND evaluation @> '{"passed": true}'::jsonb
            )
        )
);
CREATE INDEX IF NOT EXISTS ix_gold_factor_key
    ON gold.factor(factor_key, version DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_factor_active
    ON gold.factor(factor_key)
    WHERE status = 'APPROVED';

-- 2. 승인 팩터의 종목×PIT 날짜별 값과 랭크.
CREATE TABLE IF NOT EXISTS gold.factor_value (
    factor_id  BIGINT NOT NULL
                   REFERENCES gold.factor(factor_id),
    asset_id   BIGINT NOT NULL
                   REFERENCES public.asset(asset_id),
    as_of_date DATE NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    rank       INTEGER NOT NULL CHECK (rank > 0),
    PRIMARY KEY (factor_id, asset_id, as_of_date)
);
CREATE INDEX IF NOT EXISTS ix_gold_factor_value_date_rank
    ON gold.factor_value(factor_id, as_of_date, rank);
CREATE INDEX IF NOT EXISTS ix_gold_factor_value_asset
    ON gold.factor_value(asset_id, as_of_date);

CREATE OR REPLACE FUNCTION gold.require_approved_factor_value()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM gold.factor
         WHERE factor_id = NEW.factor_id
           AND status = 'APPROVED'
    ) THEN
        RAISE EXCEPTION 'factor % is not active and APPROVED', NEW.factor_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tr_require_approved_factor_value
    ON gold.factor_value;
CREATE TRIGGER tr_require_approved_factor_value
BEFORE INSERT OR UPDATE ON gold.factor_value
FOR EACH ROW
EXECUTE FUNCTION gold.require_approved_factor_value();

-- 3. 승인 팩터 간 일별 rank Spearman correlation.
CREATE TABLE IF NOT EXISTS gold.factor_correlation (
    left_factor_id    BIGINT NOT NULL
                          REFERENCES gold.factor(factor_id),
    right_factor_id   BIGINT NOT NULL
                          REFERENCES gold.factor(factor_id),
    period_start      DATE NOT NULL,
    period_end        DATE NOT NULL,
    correlation       DOUBLE PRECISION NOT NULL
                          CHECK (correlation >= -1.0 AND correlation <= 1.0),
    observation_count BIGINT NOT NULL CHECK (observation_count > 1),
    PRIMARY KEY (
        left_factor_id, right_factor_id, period_start, period_end
    ),
    CONSTRAINT ck_gold_correlation_order
        CHECK (left_factor_id < right_factor_id),
    CONSTRAINT ck_gold_correlation_period
        CHECK (period_start <= period_end)
);
CREATE INDEX IF NOT EXISTS ix_gold_factor_correlation_right
    ON gold.factor_correlation(right_factor_id, period_end DESC);

CREATE OR REPLACE FUNCTION gold.require_approved_correlation_factors()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    approved_count INTEGER;
BEGIN
    SELECT count(*)
      INTO approved_count
      FROM gold.factor
     WHERE factor_id IN (NEW.left_factor_id, NEW.right_factor_id)
       AND status = 'APPROVED';

    IF approved_count <> 2 THEN
        RAISE EXCEPTION 'correlation requires two active APPROVED factors';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tr_require_approved_correlation_factors
    ON gold.factor_correlation;
CREATE TRIGGER tr_require_approved_correlation_factors
BEFORE INSERT OR UPDATE ON gold.factor_correlation
FOR EACH ROW
EXECUTE FUNCTION gold.require_approved_correlation_factors();

CREATE OR REPLACE VIEW gold.active_factor_catalog AS
SELECT
    factor_id,
    factor_key,
    version,
    description,
    implementation_uri,
    config,
    evaluation
FROM gold.factor
WHERE status = 'APPROVED';
