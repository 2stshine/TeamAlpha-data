import pytest

from pipeline.silver_quality import backfill, s3_backfill


@pytest.mark.parametrize("module", [backfill, s3_backfill])
def test_direct_backfill_cli_is_disabled_before_base_db_or_dispatch(
    monkeypatch, module,
):
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: pytest.fail("CLI argument parsing reached"),
    )
    monkeypatch.setattr(
        module,
        "run",
        lambda *args, **kwargs: pytest.fail("legacy write dispatch reached"),
    )
    monkeypatch.setattr(
        module.db,
        "connect",
        lambda: pytest.fail("database access reached"),
    )

    with pytest.raises(RuntimeError, match="backfill CLI is disabled"):
        module.main()
