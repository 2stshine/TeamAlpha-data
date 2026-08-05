"""운영 Silver의 source별 적재 범위와 핵심 무결성을 읽기 전용 점검한다."""
from __future__ import annotations

import json

from pipeline.common import db


QUERIES = {
    "migrations": """
        SELECT migration_name, applied_at
        FROM silver_schema_migration ORDER BY migration_name
    """,
    "assets_by_exchange": """
        SELECT exchange, asset_type, count(*)
        FROM asset GROUP BY exchange, asset_type ORDER BY exchange, asset_type
    """,
    "identifiers_by_source": """
        SELECT source, identifier_type, count(*)
        FROM asset_identifier GROUP BY source, identifier_type
        ORDER BY source, identifier_type
    """,
    "prices_by_source": """
        SELECT source, count(*), count(DISTINCT asset_id),
               min(trade_date), max(trade_date)
        FROM price_daily GROUP BY source ORDER BY source
    """,
    "fundamentals_by_source": """
        SELECT source, statement_type, data_basis, count(*),
               count(DISTINCT asset_id), min(period_end), max(period_end)
        FROM fundamental
        GROUP BY source, statement_type, data_basis
        ORDER BY source, statement_type, data_basis
    """,
    "actions_by_source": """
        SELECT source, action_type, count(*), count(DISTINCT asset_id),
               min(ex_date), max(ex_date)
        FROM corporate_action GROUP BY source, action_type
        ORDER BY source, action_type
    """,
    "dividend_coverage": """
        SELECT source, count(*), count(cash_amount),
               count(adjusted_cash_amount), count(frequency),
               min(ex_date), max(ex_date)
        FROM dividend_history GROUP BY source ORDER BY source
    """,
    "integrity": """
        SELECT
          (SELECT count(*) FROM price_daily WHERE close IS NULL) AS null_close,
          (SELECT count(*) FROM fundamental
           WHERE value IS NULL OR available_date IS NULL
              OR (unit_type <> 'shares' AND currency IS NULL))
            AS invalid_fundamental,
          (SELECT count(*) FROM corporate_action
           WHERE action_type IS NULL OR action_key IS NULL) AS invalid_action,
          (SELECT count(*) FROM asset_identifier WHERE valid_from IS NULL)
            AS invalid_identifier
    """,
    "recent_runs": """
        SELECT mode, status, ruleset_version, target_date, partition_key,
               started_at, finished_at, left(coalesce(error_message,''), 240)
        FROM dq_run ORDER BY started_at DESC LIMIT 20
    """,
    "open_warning_summary": """
        SELECT mode, dataset_name, rule_code, count(*) AS open_scopes,
               sum(latest_failed_count) AS latest_failed_rows,
               min(target_date) AS oldest_target_date,
               max(target_date) AS newest_target_date,
               max(last_failed_at) AS last_failed_at
        FROM dq_open_warning
        GROUP BY mode, dataset_name, rule_code
        ORDER BY mode, latest_failed_rows DESC, rule_code
    """,
    "open_warning_recent": """
        SELECT mode, scope_key, target_date, dataset_name, rule_code,
               latest_failed_count, last_failed_at, actual_value
        FROM dq_open_warning
        ORDER BY last_failed_at DESC, warning_state_id DESC
        LIMIT 50
    """,
}


def main() -> None:
    conn = db.connect()
    report = {}
    try:
        with conn.cursor() as cur:
            for name, query in QUERIES.items():
                cur.execute(query)
                report[name] = cur.fetchall()
        conn.rollback()
    finally:
        conn.close()
    print(json.dumps(report, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
