import hashlib
import json

import pytest

from pipeline.silver import cash_adjustment_scale_evidence as evidence
from pipeline.silver.migrate_cash_scale_manifest import migrate_payload


OLD = "corporate_actions/dart/manifests/from=old/to=old/disclosures_v3.json"
NEW = "corporate_actions/dart/manifests/from=new/to=new/disclosures_v3.json"


def _payload(status="VERIFIED_DART_VIEWER_BODY", economic="economic.html"):
    row = {column: None for column in evidence.MANIFEST_ROW_COLUMNS}
    row.update({
        "evidence_key": "asset:1:2026-08-11",
        "cash_receipt_no": "20260811900001",
        "cash_source_evidence_status": status,
        "cash_action_body_path": OLD,
        "cash_action_body_sha256": "a" * 64,
        "cash_economic_body_path": economic,
    })
    row["manifest_row_sha256"] = evidence.manifest_parent_row_sha256(row)
    return {
        "schema_version": evidence.SOURCE_EVIDENCE_CONTRACT,
        "complete": True,
        "row_count": 1,
        "row_digest": "old",
        "support_action_count": 0,
        "support_action_digest": "support",
        "support_semantic_group_count": 0,
        "evidence": [{**row, "support_actions": []}],
    }


def _replacement(receipt="20260811900001"):
    return json.dumps([{"rcept_no": receipt}], separators=(",", ":")).encode()


def test_migration_rebinds_only_display_list_and_rehashes_parent():
    replacement = _replacement()
    migrated, count = migrate_payload(
        _payload(),
        unavailable_path=OLD,
        replacement_path=NEW,
        replacement_body=replacement,
    )
    row = migrated["evidence"][0]
    assert count == 1
    assert row["cash_action_body_path"] == NEW
    assert row["cash_action_body_sha256"] == hashlib.sha256(replacement).hexdigest()
    assert row["cash_economic_body_path"] == "economic.html"
    assert row["manifest_row_sha256"] == evidence.manifest_parent_row_sha256(row)
    assert migrated["row_digest"] == evidence.source_manifest_digest(
        __import__("pandas").DataFrame(migrated["evidence"])
    )


def test_migration_rejects_economic_body_rebinding():
    with pytest.raises(RuntimeError, match="economic cash evidence"):
        migrate_payload(
            _payload(economic=OLD),
            unavailable_path=OLD,
            replacement_path=NEW,
            replacement_body=_replacement(),
        )


def test_migration_requires_receipt_in_authenticated_replacement():
    with pytest.raises(RuntimeError, match="missing cash receipt"):
        migrate_payload(
            _payload(),
            unavailable_path=OLD,
            replacement_path=NEW,
            replacement_body=_replacement("20260811999999"),
        )
