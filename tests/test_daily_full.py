import inspect

import pytest

from pipeline import daily_full
from pipeline.daily_full import _fmp_target_day


def test_fmp_target_uses_completed_prior_weekday():
    assert _fmp_target_day("20260804") == "20260803"
    assert _fmp_target_day("20260803") == "20260731"


def test_daily_never_runs_legacy_partial_total_return_writer():
    source = inspect.getsource(daily_full)
    assert "from pipeline.silver import total_return" not in source
    assert "total_return.run_daily" not in source
    assert "certified full rebuild" in source


def test_daily_requires_v3_disclosure_manifest_and_rejects_v1():
    key = daily_full._action_disclosure_manifest_key("20260721", "20260804")
    assert key == (
        "corporate_actions/dart/manifests/from=20260721/to=20260804/"
        "disclosures_v3.json"
    )
    daily_full._reject_legacy_action_manifests([key])

    legacy = (
        "corporate_actions/dart/manifests/from=20260721/to=20260804/"
        "disclosures.json"
    )
    with pytest.raises(RuntimeError, match="cannot authenticate the v3"):
        daily_full._reject_legacy_action_manifests([legacy])
