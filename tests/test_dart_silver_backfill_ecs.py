from datetime import date
import json

import pytest

from pipeline import dart_silver_backfill_ecs as ecs


def test_dart_ecs_closes_tr_action_rebuild_audit_flow(
    monkeypatch, tmp_path,
):
    calls = []
    listed_prefixes = []
    downloads = []
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")
    monkeypatch.setattr(ecs.boto3, "client", lambda service: object())
    def list_keys(s3, bucket, prefix):
        listed_prefixes.append(prefix)
        return [prefix + "object"]

    monkeypatch.setattr(ecs, "_list_keys", list_keys)
    monkeypatch.setattr(ecs, "DATA_ROOT", tmp_path)
    manifest = (
        tmp_path
        / ecs.cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({
        "schema_version": (
            ecs.cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        ),
        "complete": True,
        "evidence": [{
            "previous_price_source_object_key": (
                "stock/marcap/date=2018-12-26/all.parquet"
            ),
            "adjustment_price_source_object_key": (
                "stock/marcap/date=2018-12-27/all.parquet"
            ),
        }],
    }), encoding="utf-8")

    def download(bucket, keys, root):
        downloads.append((bucket, list(keys), root))
        return len(keys)

    monkeypatch.setattr(ecs, "_download", download)
    monkeypatch.setattr(
        ecs.dart_extra_load,
        "run",
        lambda **kwargs: calls.append(("dart", kwargs)),
    )
    monkeypatch.setattr(
        ecs.total_return_rebuild,
        "run",
        lambda **kwargs: calls.append(("rebuild", kwargs)),
    )
    monkeypatch.setattr(
        ecs.total_return_audit,
        "audit",
        lambda: calls.append(("audit", {})) or {
            "safe_for_research": True, "checks": {},
        },
    )

    ecs.run_dart_extras()

    assert listed_prefixes == [
        "dividends/dart/",
        "corporate_actions/dart/",
        "corporate_actions/krx/",
    ]
    assert downloads[1] == (
        "bronze",
        [
            "stock/marcap/date=2018-12-26/all.parquet",
            "stock/marcap/date=2018-12-27/all.parquet",
        ],
        tmp_path,
    )
    assert calls == [
        ("dart", {
            "src": "local", "apply": True,
            "total_return_actions_only": True,
            "expected_coverage_end": date(2026, 8, 10),
        }),
        ("rebuild", {"apply": True}),
        ("audit", {}),
    ]


def test_dart_ecs_refuses_success_when_final_audit_fails(monkeypatch):
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")
    monkeypatch.setattr(ecs.boto3, "client", lambda service: object())
    monkeypatch.setattr(ecs, "_list_keys", lambda *args: ["object"])
    monkeypatch.setattr(ecs, "_download", lambda *args: 1)
    monkeypatch.setattr(ecs.dart_extra_load, "run", lambda **kwargs: None)
    monkeypatch.setattr(ecs.total_return_rebuild, "run", lambda **kwargs: None)
    monkeypatch.setattr(
        ecs.total_return_audit,
        "audit",
        lambda: {"safe_for_research": False, "checks": {"digest": False}},
    )

    with pytest.raises(RuntimeError, match="digest"):
        ecs.run_dart_extras()


def test_cash_scale_price_keys_reject_manifest_path_escape(tmp_path):
    path = tmp_path / ecs.cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "schema_version": (
            ecs.cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        ),
        "complete": True,
        "evidence": [{
            "previous_price_source_object_key": "../secret.parquet",
            "adjustment_price_source_object_key": (
                "stock/marcap/date=2018-12-27/all.parquet"
            ),
        }],
    }), encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid KRX price object key"):
        ecs._cash_scale_price_keys(tmp_path)
