from pathlib import Path

from pipeline.gold.run import validate_query_sql


ROOT = Path(__file__).parents[2]
SQL_PATH = (
    ROOT / "pipeline/gold/factors/operating_income_to_liabilities.sql"
)


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def test_operating_income_to_liabilities_is_query_only_pit_silver_sql():
    sql = _sql()

    validate_query_sql(sql)
    assert "public.fundamental" in sql
    assert "q.status = 'CERTIFIED'" in sql
    assert "f.available_date <= u.as_of_date" in sql
    assert "f.available_date <= s.state_date" in sql
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "fundamental_current" not in body
    assert "gold." not in body.lower()


def test_operating_income_to_liabilities_reconstructs_the_research_definition():
    sql = _sql()

    assert "f.metric IN ('operating_income', 'total_liabilities')" in sql
    assert "f.metric = 'operating_income'" in sql
    assert "f.metric = 'total_liabilities'" in sql
    assert "max(q.fy_value) - sum(q.quarter_value)" in sql
    assert "HAVING count(DISTINCT q.fiscal_period) = 3" in sql
    assert "WHERE recent_rank <= 4" in sql
    assert "HAVING count(*) = 4" in sql
    assert "max(period_end) - min(period_end) <= 370" in sql
    assert "WHERE liabilities.total_liabilities > 0" in sql
    assert "oi.operating_income_ttm / liabilities.total_liabilities AS value" in sql


def test_operating_income_to_liabilities_ranks_high_values_first():
    sql = _sql()

    assert "PARTITION BY signal_month ORDER BY value DESC" in sql
    assert "SELECT asset_id, as_of_date, value, rank" in sql
