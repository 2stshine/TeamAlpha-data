-- Seed OPEN warning state from the immutable history that predates migration
-- 004. For each incremental scope/rule, only the most recent certified
-- observation determines whether it is still open.

WITH warning_observation AS (
    SELECT r.run_id,
           q.mode,
           q.target_date,
           COALESCE(r.partition_key, q.partition_key) AS partition_key,
           CASE
               WHEN COALESCE(r.partition_key, q.partition_key) IS NOT NULL
                   THEN 'partition=' || COALESCE(r.partition_key, q.partition_key)
               ELSE 'date=' || q.target_date::text
           END AS scope_key,
           r.dataset_name,
           r.rule_code,
           r.status,
           r.expected_value,
           r.actual_value,
           r.failed_count,
           r.sample_records,
           r.checked_at
    FROM dq_result r
    JOIN dq_run q ON q.run_id = r.run_id
    WHERE r.severity = 'WARNING'
      AND q.status = 'CERTIFIED'
      AND q.mode IN ('daily', 'fmp_daily')
      AND (q.target_date IS NOT NULL
           OR COALESCE(r.partition_key, q.partition_key) IS NOT NULL)
),
latest AS (
    SELECT DISTINCT ON (mode, scope_key, dataset_name, rule_code)
           *
    FROM warning_observation
    ORDER BY mode, scope_key, dataset_name, rule_code,
             checked_at DESC, run_id DESC
),
failure_stats AS (
    SELECT mode, scope_key, dataset_name, rule_code,
           (array_agg(run_id ORDER BY checked_at, run_id))[1]
               AS first_seen_run_id,
           (array_agg(run_id ORDER BY checked_at DESC, run_id DESC))[1]
               AS last_failed_run_id,
           min(checked_at) AS first_seen_at,
           max(checked_at) AS last_failed_at,
           count(*) AS observation_count
    FROM warning_observation
    WHERE status = 'FAIL'
    GROUP BY mode, scope_key, dataset_name, rule_code
)
INSERT INTO dq_warning_state (
    mode, scope_key, target_date, partition_key,
    dataset_name, rule_code, status,
    first_seen_run_id, last_failed_run_id, last_evaluated_run_id,
    first_seen_at, last_failed_at, last_evaluated_at,
    observation_count, latest_failed_count,
    expected_value, actual_value, sample_records
)
SELECT l.mode, l.scope_key, l.target_date, l.partition_key,
       l.dataset_name, l.rule_code, 'OPEN',
       s.first_seen_run_id, s.last_failed_run_id, l.run_id,
       s.first_seen_at, s.last_failed_at, l.checked_at,
       s.observation_count, l.failed_count,
       l.expected_value, l.actual_value, l.sample_records
FROM latest l
JOIN failure_stats s
  ON s.mode = l.mode
 AND s.scope_key = l.scope_key
 AND s.dataset_name = l.dataset_name
 AND s.rule_code = l.rule_code
WHERE l.status = 'FAIL'
ON CONFLICT (mode, scope_key, dataset_name, rule_code) DO NOTHING;
