from copy import deepcopy
import json
from pathlib import Path

import pytest

from pipeline.gold.run import build_upsert_sql
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


def test_approval_is_bound_to_an_immutable_research_commit():
    document = load_approval()
    source = document["research_source"]
    assert source["repository"].startswith("https://github.com/")
    assert len(source["commit"]) == 40
    validate_approval(document)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("oos_rank_ic", 0.0),
        ("oos_rank_ic_retention", 0.1),
        ("oos_by_qvalue", 0.11),
        ("oos_signal_months", 35),
        ("oos_end", "2026-04"),
    ],
)
def test_tampered_confirmation_metrics_are_rejected(field, value):
    document = deepcopy(load_approval())
    evaluation = document["factors"]["dividend_yield_ttm"]["evaluation"]
    evaluation[field] = value
    with pytest.raises(ValueError):
        validate_approval(document)


def test_nonfinite_correlation_threshold_is_rejected():
    document = deepcopy(load_approval())
    document["selection"][
        "maximum_allowed_median_absolute_correlation"
    ] = float("nan")
    with pytest.raises(ValueError):
        validate_approval(document)


def test_upsert_wrapper_cannot_write_outside_approved_month_range():
    sql = build_upsert_sql(
        """
        SELECT asset_id, as_of_date, value, rank
        FROM public.price_daily
        WHERE trade_date BETWEEN %(start_month)s AND %(end_month)s
        """
    )
    assert "as_of_date >= %(start_month)s::date" in sql
    assert "as_of_date < (%(end_month)s::date + interval '1 month')" in sql
