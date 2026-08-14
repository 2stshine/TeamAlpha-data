import pytest

from pipeline import jobs


@pytest.mark.parametrize(
    ("function", "args", "message"),
    [
        (jobs.run_daily, ("20260810", "s3"), "legacy jobs daily"),
        (jobs.run_backfill, (2015, 2026, "s3"), "legacy jobs backfill"),
    ],
)
def test_legacy_price_entrypoints_are_disabled_before_source_mutation(
    monkeypatch, function, args, message,
):
    monkeypatch.setattr(
        jobs.stock_krxapi,
        "run",
        lambda *a, **k: pytest.fail("unsafe source mutation reached"),
    )
    monkeypatch.setattr(
        jobs.stock_marcap,
        "run",
        lambda *a, **k: pytest.fail("unsafe source mutation reached"),
    )

    with pytest.raises(RuntimeError, match=message):
        function(*args)
