-- dividend_yield_ttm Gold implementation.
-- value = signal date에 알려지고 실제 적용된 직전 12개월 조정 현금배당 / adj_close
-- predicted_sign = +1, 따라서 rank 1은 raw value가 가장 높은 종목이다.
-- 현재 CERTIFIED total-return 계약이 고정한 resolution/action snapshot만 사용한다.
WITH current_contract AS (
    SELECT
        coverage_start,
        coverage_end,
        quality_run_id,
        metadata->>'resolution_version' AS resolution_version,
        (metadata->>'action_snapshot_run_id')::uuid AS action_snapshot_run_id
    FROM public.price_return_contract
    WHERE source = 'KRX'
      AND asset_type = 'stock'
      AND field_name = 'total_return_close'
      AND methodology_version = 'krx_gross_dividend_reinvested_v1'
      AND status = 'CERTIFIED'
      AND certified_at IS NOT NULL
      AND coverage_start IS NOT NULL
      AND coverage_end IS NOT NULL
      AND coverage_start <= coverage_end
      AND metadata->>'resolution_version' IS NOT NULL
      AND metadata->>'action_snapshot_run_id' IS NOT NULL
), certified_prices AS (
    SELECT
        p.asset_id,
        a.name,
        a.instrument_type,
        p.trade_date,
        p.adj_close,
        p.total_return_close,
        p.market_cap,
        p.market,
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
        certified_prices.*,
        min(trade_date) OVER () AS dataset_start,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM certified_prices
), universe AS (
    SELECT
        m.asset_id,
        m.trade_date AS as_of_date,
        date_trunc('month', m.trade_date) AS signal_month,
        m.adj_close::double precision AS adj_close
    FROM monthly m
    CROSS JOIN current_contract c
    WHERE m.month_rank = 1
      AND date_trunc('month', m.trade_date)
          BETWEEN %(start_month)s::date AND %(end_month)s::date
      AND m.trade_date BETWEEN c.coverage_start AND c.coverage_end
      AND m.instrument_type = 'common_stock'
      AND m.name !~* '(스팩|SPAC)'
      AND position('리츠' in m.name) = 0
      AND (m.age_days >= 250 OR m.first_seen = m.dataset_start)
      AND m.market_cap > 0
      AND m.total_return_close > 0
      AND m.adj_close > 0
), canonical_events AS (
    SELECT
        r.asset_id,
        ca.announcement_date + 1 AS known_date,
        r.applied_trade_date,
        r.adjusted_cash_amount::double precision AS adjusted_cash_amount
    FROM current_contract c
    JOIN public.dividend_event_resolution r
      ON r.quality_run_id = c.quality_run_id
     AND r.resolution_version = c.resolution_version
    JOIN public.corporate_action ca
      ON ca.asset_id = r.asset_id
     AND ca.source = r.source
     AND ca.action_key = r.action_key
     AND ca.quality_run_id = c.action_snapshot_run_id
    JOIN public.asset a
      ON a.asset_id = r.asset_id
    JOIN public.dq_run resolution_q
      ON resolution_q.run_id = r.quality_run_id
     AND resolution_q.status = 'CERTIFIED'
    JOIN public.dq_run action_q
      ON action_q.run_id = ca.quality_run_id
     AND action_q.status = 'CERTIFIED'
    WHERE r.is_canonical IS TRUE
      AND r.excluded_reason IS NULL
      AND r.applied_trade_date IS NOT NULL
      AND r.adjusted_cash_amount > 0
      AND ca.announcement_date IS NOT NULL
      AND ca.source = 'DART_DISCLOSURE'
      AND ca.action_type = 'cash_dividend'
      AND ca.action_scope = 'ISSUER'
      AND a.exchange = 'KRX'
      AND a.asset_type = 'stock'
), raw_values AS (
    SELECT
        u.asset_id,
        u.as_of_date,
        u.signal_month,
        coalesce(sum(e.adjusted_cash_amount), 0.0::double precision)
            / u.adj_close AS value
    FROM universe u
    LEFT JOIN canonical_events e
      ON e.asset_id = u.asset_id
     AND e.applied_trade_date > (u.as_of_date - interval '12 months')
     AND e.applied_trade_date <= u.as_of_date
     AND e.known_date <= u.as_of_date
    GROUP BY u.asset_id, u.as_of_date, u.signal_month, u.adj_close
), ranked AS (
    SELECT
        asset_id,
        as_of_date,
        value,
        rank() OVER (
            PARTITION BY signal_month ORDER BY value DESC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
