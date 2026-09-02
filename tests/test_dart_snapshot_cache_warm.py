from pipeline import dart_snapshot_cache_warm as warm


def test_cache_warm_lists_all_evidence_prefixes_and_downloads_delta(
    monkeypatch, tmp_path,
):
    listed = []
    downloaded = []
    monkeypatch.setattr(warm.boto3, "client", lambda service: object())

    def list_objects(s3, bucket, prefix):
        listed.append((bucket, prefix))
        return [warm.dart_silver_backfill_ecs._S3Object(prefix + "one", '"e"', 1)]

    monkeypatch.setattr(
        warm.dart_silver_backfill_ecs, "_list_objects", list_objects,
    )
    monkeypatch.setattr(
        warm.dart_silver_backfill_ecs,
        "_download_changed",
        lambda bucket, objects, root: (
            downloaded.append((bucket, list(objects), root)) or (2, 1)
        ),
    )

    assert warm.run(bucket="bronze", root=tmp_path) == (2, 1)
    assert listed == [("bronze", prefix) for prefix in warm.PREFIXES]
    assert downloaded[0][0] == "bronze"
    assert len(downloaded[0][1]) == 3
    assert downloaded[0][2] == tmp_path.resolve()
