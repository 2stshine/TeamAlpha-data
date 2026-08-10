-- realized_volatility_252d Gold implementation.
-- value = 최근 252거래행 일별 총수익률의 표본 표준편차, 유효 관측 최소 126개.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
-- %(start_month)s와 %(end_month)s는 YYYY-MM-01 형태의 닫힌 signal month 범위다.
WITH certified AS (
    SELECT
        p.asset_id,
        a.name,
        a.instrument_type,
        p.trade_date,
        p.total_return_close,
        p.market_cap,
        p.market,
        lag(p.total_return_close) OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS prior_total_return_close,
        row_number() OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS age_days,
        min(p.trade_date) OVER (PARTITION BY p.asset_id) AS first_seen
    FROM public.price_daily p
    JOIN public.asset a
      ON a.asset_id = p.asset_id
     AND a.exchange = 'KRX'
     AND a.asset_type = 'stock'
    JOIN public.dq_run q
      ON q.run_id = p.quality_run_id
     AND q.status = 'CERTIFIED'
    JOIN LATERAL (
        SELECT 1
        FROM public.asset_identifier ai
        WHERE ai.asset_id = p.asset_id
          AND ai.source = 'KRX'
          AND ai.identifier_type = 'ticker'
          AND ai.valid_from <= p.trade_date
          AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
        ORDER BY ai.valid_from DESC
        LIMIT 1
    ) identifier ON true
    WHERE p.source = 'KRX'
      AND p.market IN ('KOSPI', 'KOSDAQ')
), daily_returns AS (
    SELECT
        certified.*,
        CASE
            WHEN prior_total_return_close > 0
             AND total_return_close > 0
            THEN total_return_close / prior_total_return_close - 1
        END AS daily_total_return
    FROM certified
), daily_features AS (
    SELECT
        daily_returns.*,
        stddev_samp(daily_total_return) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS daily_volatility_252d,
        count(daily_total_return) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS daily_return_observations_252d
    FROM daily_returns
), monthly AS (
    SELECT
        daily_features.*,
        min(trade_date) OVER () AS dataset_start,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM daily_features
), raw_values AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
        daily_volatility_252d::double precision AS value,
        date_trunc('month', trade_date) AS signal_month
    FROM monthly
    WHERE month_rank = 1
      AND date_trunc('month', trade_date)
          BETWEEN %(start_month)s::date AND %(end_month)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND (age_days >= 250 OR first_seen = dataset_start)
      AND market_cap > 0
      AND total_return_close > 0
      AND daily_return_observations_252d >= 126
      AND daily_volatility_252d IS NOT NULL
), ranked AS (
    SELECT
        asset_id,
        as_of_date,
        value,
        rank() OVER (
            PARTITION BY signal_month ORDER BY value ASC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
