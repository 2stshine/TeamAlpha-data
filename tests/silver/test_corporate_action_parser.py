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


def test_capital_reduction_factor_uses_before_over_after_shares(tmp_path):
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
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "capital_reduction"
    assert events.iloc[0]["expected_factor"] == pytest.approx(8.0)


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
