import json
from pathlib import Path

from pipeline.gold.publish import (
    DEFAULT_APPROVAL_PATH,
    _desired_metadata,
    _month_count,
    load_approval,
    validate_approval,
)


SELECTED = {
    "dividend_yield_ttm",
    "max_daily_return_1m",
    "net_equity_issuance_price_adjusted_12m",
    "realized_volatility_252d",
    "operating_income_to_liabilities",
}


def test_approval_selects_only_nonduplicate_promoted_factors():
    document = load_approval()
    assert set(document["factors"]) == SELECTED
    assert "dividend_event_frequency_ttm" not in document["factors"]
    assert document["selection"]["selected_pair_max_oos"] < 0.70


def test_approval_is_bound_to_exact_verified_sql_hashes():
    document = load_approval()
    validated = validate_approval(document)
    assert set(validated) == SELECTED
    for factor_key, item in validated.items():
        assert (
            item["implementation_hash"]
            == document["factors"][factor_key]["implementation_sha256"]
        )


def test_approval_requires_36_month_promote_evidence():
    document = load_approval()
    for approval in document["factors"].values():
        evaluation = approval["evaluation"]
        assert evaluation["passed"] is True
        assert evaluation["verdict"] == "PROMOTE"
        assert evaluation["oos_signal_months"] == 36
        assert evaluation["oos_rank_ic"] >= evaluation["oos_required_rank_ic"]


def test_backfill_window_has_one_hundred_signal_months():
    document = load_approval()
    assert _month_count(
        document["backfill_start_month"],
        document["backfill_end_month"],
    ) == 100


def test_gold_metadata_keeps_raw_value_and_directional_rank_contract():
    document = load_approval()
    validated = validate_approval(document)
    for factor_key, item in validated.items():
        config, evaluation = _desired_metadata(
            document, factor_key, item,
        )
        assert config["value_contract"]["id"] == (
            "raw_value_direction_adjusted_rank_v1"
        )
        assert config["predicted_sign"] in {-1, 1}
        assert evaluation["passed"] is True


def test_approval_document_is_plain_json_for_auditability():
    raw = DEFAULT_APPROVAL_PATH.read_text(encoding="utf-8")
    assert json.loads(raw)["approval_id"] == "promoted-20260810-nonduplicate-v1"
    assert Path(DEFAULT_APPROVAL_PATH).suffix == ".json"
