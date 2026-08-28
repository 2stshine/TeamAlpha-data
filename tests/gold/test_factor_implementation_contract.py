import hashlib
import json
from pathlib import Path

from pipeline.gold.run import build_upsert_sql, validate_contract, validate_query_sql


ROOT = Path(__file__).parents[2]
MANIFEST = json.loads(
    (ROOT / "pipeline/gold/factors/manifest.json").read_text(encoding="utf-8")
)


def test_allowlisted_factor_sql_files_exist_and_have_stable_hashes():
    assert set(MANIFEST) == {
        "market_leverage",
        "operating_return_on_capital_employed",
        "paid_in_capital_ratio",
        "return_kurtosis_24m",
        "trading_turnover_20d",
        "turnover_volatility_12m",
    }
    for spec in MANIFEST.values():
        path = ROOT / spec["sql"]
        assert path.is_file()
        assert len(hashlib.sha256(path.read_bytes()).hexdigest()) == 64
        assert spec["value_contract"] == "raw_value_direction_adjusted_rank_v1"
        assert len(spec["research_definition_hash"]) == 16


def test_negative_sign_factors_return_raw_values_and_rank_low_raw_first():
    for name in (
        "paid_in_capital_ratio",
        "return_kurtosis_24m",
        "trading_turnover_20d",
        "turnover_volatility_12m",
    ):
        spec = MANIFEST[name]
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert spec["predicted_sign"] == -1
        assert "ORDER BY value ASC" in sql
        assert "%(start_month)s" in sql
        assert "%(end_month)s" in sql
        assert "INSERT INTO" not in sql
        validate_query_sql(sql)


def test_positive_sign_factors_rank_high_raw_first():
    for name in ("market_leverage", "operating_return_on_capital_employed"):
        spec = MANIFEST[name]
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert spec["predicted_sign"] == 1
        assert "ORDER BY value DESC" in sql
        validate_query_sql(sql)


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


def test_market_leverage_replays_pit_total_liabilities():
    sql = (ROOT / MANIFEST["market_leverage"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "f.available_date <= u.as_of_date" in sql
    assert "f.metric = 'total_liabilities'" in sql
    assert "value::double precision / market_cap AS value" in sql
    assert "value >= 0" in sql
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


def test_new_factors_preserve_pit_and_rolling_contracts():
    roce = (ROOT / MANIFEST["operating_return_on_capital_employed"]["sql"]).read_text(
        encoding="utf-8"
    )
    kurtosis = (ROOT / MANIFEST["return_kurtosis_24m"]["sql"]).read_text(
        encoding="utf-8"
    )
    turnover_volatility = (
        ROOT / MANIFEST["turnover_volatility_12m"]["sql"]
    ).read_text(encoding="utf-8")

    assert "f.available_date <= u.as_of_date" in roce
    assert "fy.fy_end - interval '370 days'" in roce
    assert "ROWS BETWEEN 11 PRECEDING AND CURRENT ROW" in turnover_volatility
    assert "stddev_samp(log_turnover)" in turnover_volatility
    assert "interval '23 months'" in kurtosis
    assert "sample_variance" in kurtosis
    assert "sample_variance = 0 THEN -3.0" in kurtosis
    assert MANIFEST["return_kurtosis_24m"]["parity_atol"] == 5e-6
    assert MANIFEST["return_kurtosis_24m"]["parity_rtol"] == 2e-7
    assert MANIFEST["return_kurtosis_24m"]["allow_tolerance_equivalent_ranks"] is True


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
