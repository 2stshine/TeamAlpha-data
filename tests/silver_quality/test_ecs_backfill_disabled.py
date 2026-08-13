import pytest

from pipeline.silver_quality import ecs_backfill


def test_destructive_ecs_backfill_is_disabled_before_rds_or_s3_access(
    monkeypatch,
):
    monkeypatch.setattr(
        ecs_backfill,
        "_prepare_rds",
        lambda **kwargs: pytest.fail("destructive RDS preparation reached"),
    )
    monkeypatch.setattr(
        ecs_backfill,
        "_sync_cutoff",
        lambda *args: pytest.fail("S3 sync reached"),
    )

    with pytest.raises(RuntimeError, match="destructive ECS Silver backfill"):
        ecs_backfill.main()
