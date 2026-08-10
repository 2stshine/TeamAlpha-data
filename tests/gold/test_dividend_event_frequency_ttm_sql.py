from pathlib import Path

from pipeline.gold.run import validate_query_sql


ROOT = Path(__file__).parents[2]
SQL_PATH = ROOT / "pipeline/gold/factors/dividend_event_frequency_ttm.sql"


def test_dividend_event_frequency_is_query_only_canonical_pit_count():
    sql = SQL_PATH.read_text(encoding="utf-8")
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )

    validate_query_sql(sql)
    assert "public.price_return_contract" in body
    assert "public.dividend_event_resolution" in body
    assert "r.is_canonical IS TRUE" in body
    assert "ca.announcement_date + 1" in body
    assert "e.applied_trade_date > (u.as_of_date - interval '12 months')" in body
    assert "count(e.action_key)::double precision AS value" in body
    assert "ORDER BY value DESC" in body
    assert "sum(" not in body.lower()
    assert "/ u.adj_close" not in body
    assert "INSERT INTO" not in body.upper()


def test_dividend_event_frequency_preserves_zero_event_rows():
    sql = SQL_PATH.read_text(encoding="utf-8")

    assert "LEFT JOIN canonical_events e" in sql
    assert "count(e.action_key)::double precision AS value" in sql
