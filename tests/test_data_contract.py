from pathlib import Path

from pipeline.silver.dart_action_snapshot import SCHEMA_VERSION
from pipeline.silver.total_return_rebuild import METHODOLOGY_VERSION
from pipeline.silver_quality import QUALITY_RULESET_VERSION


ROOT = Path(__file__).resolve().parents[1]


def test_research_contract_tracks_certified_total_return_constants():
    human = (ROOT / "DATA_CONTRACT.md").read_text(encoding="utf-8")
    machine = (ROOT / "field_reliability.yaml").read_text(encoding="utf-8")

    assert f"ruleset {QUALITY_RULESET_VERSION}" in human
    assert f'ruleset_version: "{QUALITY_RULESET_VERSION}"' in machine
    assert METHODOLOGY_VERSION in human
    assert METHODOLOGY_VERSION in machine
    assert SCHEMA_VERSION in human
    assert SCHEMA_VERSION in machine
    assert "feature_pit_safe=false" in human
    assert "feature_pit_safe: false" in machine


def test_research_contract_declares_forward_compound_not_back_adjustment():
    human = (ROOT / "DATA_CONTRACT.md").read_text(encoding="utf-8")
    machine = (ROOT / "field_reliability.yaml").read_text(encoding="utf-8")
    combined = human + machine

    assert "(adj_close[t]+adjusted_cash[t])/adj_close[t-1]" in human
    assert "(adj_close[t]+adjusted_cash[t])/adj_close[t-1]" in machine
    for stale_claim in (
        "최신일==adj_close",
        "latest == adj_close",
        "back-adjusted",
        "검증(KRX 근사)",
        "±1~2거래일",
        "approx +/-1-2",
        "배당 X",
        "NO dividends",
        "배당 미반영",
    ):
        assert stale_claim not in combined


def test_direct_dividend_features_are_not_pit_certified():
    human = (ROOT / "DATA_CONTRACT.md").read_text(encoding="utf-8")
    machine = (ROOT / "field_reliability.yaml").read_text(encoding="utf-8")

    assert "직접 배당 피처로는 PIT 인증되지 않았다" in human
    assert machine.count("feature_pit_certified: false") >= 2
    assert "latest_corrected_action_snapshot" in machine
    assert "historical_as_of: unavailable" in machine

    # The total-return clarification must not erase the independent DART
    # fundamental restatement/as-of contract.
    assert "FUNDAMENTAL_PIT_ORDER" in human
    assert "available_date <= :as_of_date" in human
    assert "NEVER just the latest revision" in machine
