-- 최신 KRX 거래일 기준 12-1 모멘텀 한 스냅샷.
--
-- signal = adj_close[t-21 trading days] / adj_close[t-252 trading days] - 1
-- universe = 최신 as_of_date에 KOSPI/KOSDAQ인 stock
-- as_of_date = 최신 적재 거래일
--
-- 호출자는 %(factor_id)s를 전달하고 transaction을 소유한다.
WITH market_calendar AS (
    SELECT DISTINCT trade_date
    FROM public.factor_price_feature_daily
    WHERE source = 'KRX'
      AND market IN ('KOSPI', 'KOSDAQ')
),
anchors AS (
    SELECT
        dates[1] AS as_of_date,
        dates[22] AS signal_end_date,
        dates[253] AS signal_start_date
    FROM (
        SELECT array_agg(trade_date ORDER BY trade_date DESC) AS dates
        FROM market_calendar
    ) calendar
),
raw_values AS (
    SELECT
        p_end.asset_id,
        anchors.as_of_date,
        (
            p_end.adj_close::double precision
            / p_start.adj_close::double precision
        ) - 1.0 AS value
    FROM anchors
    JOIN public.factor_price_feature_daily p_asof
      ON p_asof.source = 'KRX'
     AND p_asof.trade_date = anchors.as_of_date
     AND p_asof.market IN ('KOSPI', 'KOSDAQ')
    JOIN public.factor_price_feature_daily p_end
      ON p_end.asset_id = p_asof.asset_id
     AND p_end.source = 'KRX'
     AND p_end.trade_date = anchors.signal_end_date
    JOIN public.factor_price_feature_daily p_start
      ON p_start.asset_id = p_end.asset_id
     AND p_start.source = 'KRX'
     AND p_start.trade_date = anchors.signal_start_date
    JOIN public.asset a
      ON a.asset_id = p_end.asset_id
     AND a.asset_type = 'stock'
    WHERE anchors.as_of_date IS NOT NULL
      AND anchors.signal_end_date IS NOT NULL
      AND anchors.signal_start_date IS NOT NULL
      AND p_end.adj_close > 0
      AND p_start.adj_close > 0
),
ranked AS (
    SELECT
        asset_id,
        as_of_date,
        value,
        rank() OVER (ORDER BY value DESC) AS rank
    FROM raw_values
)
INSERT INTO gold.factor_value (
    factor_id,
    asset_id,
    as_of_date,
    value,
    rank
)
SELECT
    %(factor_id)s,
    asset_id,
    as_of_date,
    value,
    rank
FROM ranked
ON CONFLICT (factor_id, asset_id, as_of_date)
DO UPDATE SET
    value = EXCLUDED.value,
    rank = EXCLUDED.rank;
