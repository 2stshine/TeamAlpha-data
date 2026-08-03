from pipeline.daily_full import _fmp_target_day


def test_fmp_target_uses_completed_prior_weekday():
    assert _fmp_target_day("20260804") == "20260803"
    assert _fmp_target_day("20260803") == "20260731"
