import hashlib
import json
from pathlib import Path

from pipeline.gold.run import build_upsert_sql, validate_contract, validate_query_sql


ROOT = Path(__file__).parents[2]
MANIFEST = json.loads(
    (ROOT / "pipeline/gold/factors/manifest.json").read_text(encoding="utf-8")
)
EXPECTED_FACTORS = {
    "amihud_illiquidity_1m",
    "dividend_event_frequency_ttm",
    "dividend_yield_ttm",
    "max_daily_return_1m",
    "net_equity_issuance_price_adjusted_12m",
    "operating_income_to_liabilities",
    "paid_in_capital_ratio",
    "realized_volatility_252d",
    "trading_turnover_20d",
}


def test_allowlisted_factor_sql_files_exist_and_have_stable_hashes():
    assert set(MANIFEST) == EXPECTED_FACTORS
    for spec in MANIFEST.values():
        path = ROOT / spec["sql"]
        assert path.is_file()
        assert len(hashlib.sha256(path.read_bytes()).hexdigest()) == 64
        assert spec["value_contract"] == "raw_value_direction_adjusted_rank_v1"
        assert len(spec["research_definition_hash"]) == 16


def test_factors_return_raw_values_and_rank_in_predicted_direction():
    for spec in MANIFEST.values():
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        direction = "DESC" if spec["predicted_sign"] == 1 else "ASC"
        assert f"ORDER BY value {direction}" in sql
        assert "%(start_month)s" in sql
        assert "%(end_month)s" in sql
        assert "INSERT INTO" not in sql
        validate_query_sql(sql)


def test_manifest_contains_both_positive_and_negative_factor_directions():
    positive = {name for name, spec in MANIFEST.items() if spec["predicted_sign"] == 1}
    negative = {name for name, spec in MANIFEST.items() if spec["predicted_sign"] == -1}
    assert positive == {
        "amihud_illiquidity_1m",
        "dividend_event_frequency_ttm",
        "dividend_yield_ttm",
        "operating_income_to_liabilities",
    }
    assert negative == EXPECTED_FACTORS - positive


def test_factor_sql_rejects_gold_or_current_state_relations():
    template = (
        "SELECT asset_id, as_of_date, value, rank FROM {relation} "
        "WHERE as_of_date BETWEEN %(start_month)s AND %(end_month)s"
    )
    for relation in ("gold.factor_value", "public.fundamental_current"):
        try:
            validate_query_sql(template.format(relation=relation))
        except ValueError as exc:
            assert "Silver relation" in str(exc)
        else:
            raise AssertionError(f"forbidden relation accepted: {relation}")


def test_runner_wraps_the_same_read_only_query_for_gold_upsert():
    spec = MANIFEST["trading_turnover_20d"]
    query = (ROOT / spec["sql"]).read_text(encoding="utf-8")
    wrapped = build_upsert_sql(query)

    assert query.strip().removesuffix(";") in wrapped
    assert "INSERT INTO gold.factor_value" in wrapped
    assert "ON CONFLICT (factor_id, asset_id, as_of_date)" in wrapped


def test_paid_in_capital_is_point_in_time_and_not_current_state():
    sql = (ROOT / MANIFEST["paid_in_capital_ratio"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "f.available_date <= u.as_of_date" in sql
    assert "q.status = 'CERTIFIED'" in sql
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "fundamental_current" not in body


def test_turnover_uses_current_plus_previous_nineteen_rows():
    sql = (ROOT / MANIFEST["trading_turnover_20d"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW" in sql
    assert "adv20 > 0" not in sql


def test_daily_return_factors_use_certified_total_return_history():
    for factor in (
        "amihud_illiquidity_1m",
        "max_daily_return_1m",
        "realized_volatility_252d",
    ):
        sql = (ROOT / MANIFEST[factor]["sql"]).read_text(encoding="utf-8")
        assert "total_return_close" in sql
        assert "q.status = 'CERTIFIED'" in sql


def test_dividend_yield_replays_the_certified_canonical_pit_contract():
    sql = (ROOT / MANIFEST["dividend_yield_ttm"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "public.price_return_contract" in sql
    assert "public.dividend_event_resolution" in sql
    assert "r.is_canonical IS TRUE" in sql
    assert "ca.announcement_date + 1" in sql
    assert "interval '12 months'" in sql


def test_price_adjusted_issuance_uses_split_adjusted_price_and_exact_month_lag():
    sql = (
        ROOT / MANIFEST["net_equity_issuance_price_adjusted_12m"]["sql"]
    ).read_text(encoding="utf-8")
    assert "market_cap::double precision / adj_close::double precision" in sql
    assert "lag(adjusted_share_base, 12)" in sql
    assert "lag(signal_month, 12)" in sql


def test_runner_accepts_structured_publisher_contract():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": -1,
            "research_definition_hash": spec["research_definition_hash"],
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    validate_contract(metadata, spec, path)


def test_runner_rejects_a_different_research_definition():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": spec["predicted_sign"],
            "research_definition_hash": "different",
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    try:
        validate_contract(metadata, spec, path)
    except ValueError as exc:
        assert "research_definition_hash" in str(exc)
    else:
        raise AssertionError("definition mismatch must fail")


def test_runner_metadata_query_selects_only_approved_versions():
    source = (ROOT / "pipeline/gold/run.py").read_text(encoding="utf-8")
    assert "AND status = 'APPROVED'" in source
