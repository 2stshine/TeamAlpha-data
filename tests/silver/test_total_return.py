import pytest

from pipeline.silver import total_return


@pytest.mark.parametrize(
    "entrypoint",
    [
        total_return.derive_ex_dates,
        total_return.compute_total_return_close,
        total_return.assets_with_recent_dividend_changes,
        total_return.run,
        total_return.run_daily,
    ],
)
def test_legacy_total_return_entrypoints_fail_closed(entrypoint):
    with pytest.raises(RuntimeError, match="legacy partial total-return"):
        entrypoint()
