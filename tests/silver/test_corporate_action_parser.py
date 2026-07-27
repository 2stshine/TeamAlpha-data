import json
from datetime import date

import pandas as pd
import pytest

from pipeline.silver import corporate_actions


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_prepare_normalizes_structured_factor_and_exchange_notice(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260601000001.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000001",
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000002",
        "rcept_dt": "20260707",
        "report_nm": "권리락(무상증자)",
    }])

    events, stats = corporate_actions.prepare(str(tmp_path))

    assert len(events) == 2
    bonus = events[events["source"].eq("DART_STRUCTURED")].iloc[0]
    assert bonus["event_type"] == "bonus_issue"
    assert bonus["effective_date"] == date(2026, 7, 8)
    assert bonus["expected_factor"] == pytest.approx(0.5)
    notice = events[events["source"].eq("DART_DISCLOSURE")].iloc[0]
    assert notice["event_type"] == "rights_detachment"
    assert notice["effective_date"] == date(2026, 7, 7)
    assert stats["effective_date_count"] == 2
    assert stats["expected_factor_count"] == 1


def test_capital_reduction_share_factor_is_not_a_price_factor(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000003.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000003",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "capital_reduction"
    assert pd.isna(events.iloc[0]["expected_factor"])
    assert events.iloc[0]["share_count_factor"] == pytest.approx(8.0)
    assert events.iloc[0]["share_count_before"] == pytest.approx(8_000)
    assert events.iloc[0]["share_count_after"] == pytest.approx(1_000)
    assert events.iloc[0]["share_count_factor_comparable"]
    assert (
        events.iloc[0]["share_count_comparison_reason"]
        == "UNIFORM_REDUCTION"
    )
    assert events.iloc[0]["action_method"] == "보통주식 8대 1 무상감자"


def test_non_uniform_reduction_is_not_share_factor_comparable(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000005.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000005",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "최대주주 보유주식만 8대 1 병합",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert not events.iloc[0]["share_count_factor_comparable"]


def test_combined_offering_does_not_infer_factor_from_bonus_leg_only(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=combined_offering/year=2026"
        / "corp=005930/rcept=20260601000004.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000004",
        "fric_nstk_asstd": "2026년 07월 08일",
        "fric_nstk_ascnt_ps_ostk": "1.0",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "combined_offering"
    assert pd.isna(events.iloc[0]["expected_factor"])


def test_simultaneous_financing_makes_reduction_ratio_not_comparable(tmp_path):
    reduction = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000010.json"
    )
    financing = (
        tmp_path
        / "corporate_actions/dart/structured/event=paid_increase/year=2026"
        / "corp=005930/rcept=20260601000011.json"
    )
    _write_json(reduction, {
        "rcept_no": "20260601000010",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })
    _write_json(financing, {
        "rcept_no": "20260601000011",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    reduction_event = events[
        events["event_type"].eq("capital_reduction")
    ].iloc[0]
    assert not reduction_event["share_count_factor_comparable"]
    assert (
        reduction_event["share_count_comparison_reason"]
        == "SIMULTANEOUS_FINANCING_DISCLOSURE"
    )


def test_ex_dividend_notice_is_evidence_not_required_price_adjustment(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000005",
        "rcept_dt": "20260707",
        "report_nm": "배당락",
    }])
    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    assert event["event_type"] == "ex_dividend"
    assert event["confirms_price_adjustment"]
    assert not event["expects_price_adjustment"]
    assert event["effective_date"] == date(2026, 7, 7)


def test_common_issuer_events_are_inherited_by_preferred_share():
    event = pd.DataFrame([{
        "identifier": "001520",
        "event_type": "delisting",
        "rcept_no": "20260707000006",
    }])
    expanded, stats = corporate_actions.inherit_issuer_events(
        event,
        {"001529": "001520"},
    )
    inherited = expanded[expanded["identifier"].eq("001529")].iloc[0]
    assert inherited["issuer_event_inherited"]
    assert inherited["issuer_parent_identifier"] == "001520"
    assert stats["inherited_event_count"] == 1
