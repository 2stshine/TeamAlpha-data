-- return_kurtosis_24m Gold implementation.
-- value = 최근 24개월 total_return_close 월수익률의 pandas-compatible
-- unbiased Fisher excess kurtosis. 연속 24개 월말 가격행과 최소 18개
-- 유효 월수익률을 요구한다.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.total_return_close, p.market_cap, p.market,
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
), monthly AS (
    SELECT
        certified.*,
        min(trade_date) OVER () AS dataset_start,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM certified
), monthly_prices AS (
    SELECT
        asset_id, name, instrument_type, trade_date, total_return_close,
        market_cap, market, age_days, first_seen, dataset_start,
        date_trunc('month', trade_date) AS signal_month
    FROM monthly
    WHERE month_rank = 1
), monthly_returns AS (
    SELECT
        monthly_prices.*,
        total_return_close::double precision
            / lag(total_return_close::double precision) OVER (
                PARTITION BY asset_id ORDER BY signal_month
              ) - 1.0 AS monthly_return
    FROM monthly_prices
), targets AS (
    SELECT *
    FROM monthly_returns
    WHERE signal_month BETWEEN %(start_month)s::date AND %(end_month)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND (age_days >= 250 OR first_seen = dataset_start)
      AND market_cap > 0
      AND total_return_close > 0
), window_stats AS (
    SELECT
        t.asset_id, t.trade_date AS as_of_date, t.signal_month,
        count(*) AS window_rows,
        min(r.signal_month) AS first_signal_month,
        count(r.monthly_return) AS n,
        avg(r.monthly_return) AS mean_return,
        var_samp(r.monthly_return) AS sample_variance
    FROM targets t
    JOIN monthly_returns r
      ON r.asset_id = t.asset_id
     AND r.signal_month BETWEEN
         t.signal_month - interval '23 months' AND t.signal_month
    GROUP BY t.asset_id, t.trade_date, t.signal_month
), fourth_moment AS (
    SELECT
        s.asset_id, s.as_of_date, s.signal_month,
        s.window_rows, s.first_signal_month, s.n, s.sample_variance,
        sum(power(r.monthly_return - s.mean_return, 4)) AS fourth_sum
    FROM window_stats s
    JOIN monthly_returns r
      ON r.asset_id = s.asset_id
     AND r.signal_month BETWEEN
         s.signal_month - interval '23 months' AND s.signal_month
    WHERE r.monthly_return IS NOT NULL
    GROUP BY
        s.asset_id, s.as_of_date, s.signal_month, s.window_rows,
        s.first_signal_month, s.n, s.sample_variance
), raw_values AS (
    SELECT
        asset_id, as_of_date, signal_month,
        CASE WHEN sample_variance = 0 THEN -3.0 ELSE (
            n::double precision * (n + 1)::double precision * fourth_sum
            / (
                (n - 1)::double precision
                * (n - 2)::double precision
                * (n - 3)::double precision
                * power(sample_variance, 2)
              )
            - 3.0 * power((n - 1)::double precision, 2)
              / ((n - 2)::double precision * (n - 3)::double precision)
        ) END AS value
    FROM fourth_moment
    WHERE window_rows = 24
      AND first_signal_month = signal_month - interval '23 months'
      AND n >= 18
      AND sample_variance >= 0
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_month ORDER BY value ASC) AS rank
    FROM raw_values
    WHERE value IS NOT NULL
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
