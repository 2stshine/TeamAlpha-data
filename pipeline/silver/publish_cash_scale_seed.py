"""Publish the missing immutable cash-scale seed from certified RDS evidence.

This is a recovery-only operation.  It does not derive new economic meaning:
the canonical manifest is reconstructed from the latest persisted, certified
331-parent action snapshot and must match its recorded manifest hash exactly.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from psycopg.rows import dict_row

from pipeline import dart_silver_backfill_ecs
from pipeline.silver import (
    cash_adjustment_scale_builder as builder,
    cash_adjustment_scale_evidence as evidence,
    krx_kind_reference,
)


PAID_COMPONENT = {
    "adjustment_date": "2017-12-27",
    "announcement_date": "2017-12-27",
    "asset_name": "아세아시멘트",
    "body_content_length": 42460,
    "body_sha256": "cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9",
    "body_url": "https://kind.krx.co.kr/external/2018/02/01/000047/20180201000086/11306.htm",
    "component_action_key": "20180201000086",
    "component_action_source": "KRX_KIND",
    "component_action_type": "paid_increase",
    "contents_content_length": 1046,
    "contents_sha256": "d9fdaacae60f43ac42a6c551c6a8559de4c2ec3edf050f764893be57ef8b5e28",
    "contents_url": "https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&docNo=20180201000086",
    "distributed_security_class": "COMMON",
    "entitlement_security_class": "COMMON",
    "main_content_length": 24808,
    "main_sha256": "6472611a5b11e9036922960a43a891d10e27cd5aa8659857ebdbdc6a12938814",
    "main_url": "https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=20180201000047&docno=&viewerhost=&viewerport=",
    "ratio_denominator": 1.0,
    "ratio_numerator": 0.1456981704,
    "record_date": "2017-12-31",
    "report_name": "유상증자 결정",
    "semantic_role": "ADJUSTMENT_COMPONENT",
    "source_form_code": "11306",
    "target_cash_receipt_no": "20180226800579",
    "terminal_acceptance_no": "20180201000047",
    "terminal_announcement_date": "2018-02-01",
    "ticker": "183190",
}


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _manifest_payload(
    parents: pd.DataFrame,
    supports: pd.DataFrame,
) -> dict[str, object]:
    # Reproduce the builder's serialized representation, not merely its
    # database-neutral digest representation.  The original overlap frame was
    # sorted by asset/date and its numeric values were JSON numbers; persisted
    # NUMERIC values otherwise arrive from psycopg as Decimal strings.
    def original_row(row, columns) -> dict[str, object]:
        rendered: dict[str, object] = {}
        for column in columns:
            value = getattr(row, column)
            if value is None or pd.isna(value):
                rendered[column] = None
            elif column in evidence._DATE_COLUMNS:
                rendered[column] = value.isoformat()[:10]
            elif column in evidence._INTEGER_COLUMNS:
                rendered[column] = int(value)
            elif column in evidence._DECIMAL_PLACES:
                rendered[column] = float(value)
            else:
                rendered[column] = str(value)
        return rendered

    parent_rows = [
        original_row(row, evidence.MANIFEST_ROW_COLUMNS)
        for row in parents.sort_values(
            ["asset_id", "adjustment_trade_date"], kind="stable",
        ).itertuples(index=False)
    ]
    support_rows = [
        original_row(row, evidence.MANIFEST_SUPPORT_ACTION_COLUMNS)
        for row in supports.sort_values(
            [
                "evidence_key", "support_action_source",
                "support_action_key", "support_action_type",
            ],
            kind="stable",
        ).itertuples(index=False)
    ]
    parent_hashes = {
        str(row.evidence_key): str(row.manifest_row_sha256)
        for row in parents.itertuples(index=False)
    }
    support_hashes = {
        (
            str(row.evidence_key), str(row.support_action_source),
            str(row.support_action_key), str(row.support_action_type),
        ): str(row.manifest_support_row_sha256)
        for row in supports.itertuples(index=False)
    }
    by_parent: dict[str, list[dict[str, object]]] = {}
    groups: set[str] = set()
    for row in support_rows:
        identity = (
            str(row["evidence_key"]), str(row["support_action_source"]),
            str(row["support_action_key"]), str(row["support_action_type"]),
        )
        if evidence.manifest_support_row_sha256(row) != support_hashes[identity]:
            raise RuntimeError(f"persisted support self-digest drifted: {identity}")
        rendered = {**row, "manifest_support_row_sha256": support_hashes[identity]}
        by_parent.setdefault(identity[0], []).append(rendered)
        groups.update(json.loads(str(row["support_semantic_group_keys"])))
    rendered_parents: list[dict[str, object]] = []
    for row in parent_rows:
        key = str(row["evidence_key"])
        if evidence.manifest_parent_row_sha256(row) != parent_hashes[key]:
            raise RuntimeError(f"persisted parent self-digest drifted: {key}")
        rendered_parents.append({
            **row,
            "manifest_row_sha256": parent_hashes[key],
            "support_actions": by_parent.get(key, []),
        })
    return {
        "schema_version": evidence.SOURCE_EVIDENCE_CONTRACT,
        "complete": True,
        "row_count": len(parent_rows),
        "row_digest": evidence.source_manifest_digest(parents),
        "support_action_count": len(support_rows),
        "support_action_digest": evidence.support_manifest_digest(supports),
        "support_semantic_group_count": len(groups),
        "evidence": rendered_parents,
    }


def _component_request(root: Path) -> tuple[Path, Path]:
    resources = Path(__file__).resolve().parents[1] / "resources" / "kind"
    reference = resources / "reference-requests-v2.json"
    component_v1 = json.loads(
        (resources / "component-requests-v1.json").read_text(encoding="utf-8")
    )
    components = sorted(
        [*component_v1["components"], PAID_COMPONENT],
        key=lambda row: (row["ticker"], row["component_action_key"]),
    )
    component = {
        "schema_version": krx_kind_reference.KIND_COMPONENT_REQUEST_SCHEMA,
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_TERMINAL_COMPONENT",
        "complete": True,
        "component_count": len(components),
        "component_digest": hashlib.sha256(_canonical(components)).hexdigest(),
        "components": components,
    }
    destination = root / "component-requests-v2.json"
    destination.write_bytes(_canonical(component))
    return reference, destination


def _price_payload(
    s3,
    bucket: str,
    parents: pd.DataFrame,
) -> dict[str, object]:
    declared: dict[str, builder.PriceObject] = {}
    for row in parents.itertuples(index=False):
        for prefix in ("previous", "adjustment"):
            key = str(getattr(row, f"{prefix}_price_source_object_key"))
            trade_date = getattr(
                row,
                "previous_trade_date" if prefix == "previous"
                else "adjustment_trade_date",
            )
            digest = str(getattr(row, f"{prefix}_price_source_content_sha256"))
            etag = str(getattr(row, f"{prefix}_price_source_etag"))
            schema = str(getattr(row, f"{prefix}_price_source_schema"))
            head = s3.head_object(Bucket=bucket, Key=key)
            actual_etag = str(head["ETag"]).strip('"').lower()
            if actual_etag != etag:
                raise RuntimeError(f"persisted price ETag drifted: {key}")
            candidate = builder.PriceObject(
                trade_date=trade_date,
                source_object_key=key,
                local_path=key,
                etag=etag,
                content_length=int(head["ContentLength"]),
                content_sha256=digest,
                version_id=head.get("VersionId"),
                server_side_encryption=head.get("ServerSideEncryption"),
                source_schema=schema,
            )
            existing = declared.get(key)
            if existing is not None and existing != candidate:
                raise RuntimeError(f"conflicting persisted price receipt: {key}")
            declared[key] = candidate
    if len(declared) != builder.EXPECTED_PRICE_OBJECT_COUNT:
        raise RuntimeError(
            f"persisted price object count changed: {len(declared)}"
        )
    return builder._price_object_payload(
        declared.values(), bucket=bucket, prefix="",
    )


def _put_once(s3, bucket: str, root: Path, path: Path) -> None:
    key = path.relative_to(root).as_posix()
    body = path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    try:
        s3.put_object(
            Bucket=bucket, Key=key, Body=body,
            ContentType="application/json" if path.suffix == ".json" else "application/octet-stream",
            IfNoneMatch="*",
        )
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if code not in {"PreconditionFailed", "ConditionalRequestConflict"} and status not in {409, 412}:
            raise
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if hashlib.sha256(existing).hexdigest() != digest:
            raise RuntimeError(f"immutable seed object conflicts: {key}") from exc
    print(f"[cash-scale-seed] published {key} sha256={digest}", flush=True)


def run() -> None:
    lock = dart_silver_backfill_ecs.acquire_daily_certification_lock()
    try:
        bucket = __import__("os").environ["S3_BRONZE_BUCKET"]
        with lock.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT action_snapshot_run_id FROM "
                "cash_adjustment_scale_source_evidence "
                "GROUP BY action_snapshot_run_id "
                "ORDER BY max(recorded_at) DESC LIMIT 1"
            )
            selected = cursor.fetchone()
            if selected is None:
                raise RuntimeError("no persisted cash-scale evidence")
            run_id = selected["action_snapshot_run_id"]
            cursor.execute(
                "SELECT metadata FROM dart_action_snapshot_contract "
                "WHERE quality_run_id=%s", (run_id,),
            )
            metadata = cursor.fetchone()["metadata"][
                "cash_adjustment_scale_evidence"
            ]
            cursor.execute(
                f"SELECT {','.join(evidence.SOURCE_EVIDENCE_COLUMNS)} "
                "FROM cash_adjustment_scale_source_evidence "
                "WHERE action_snapshot_run_id=%s ORDER BY evidence_key",
                (run_id,),
            )
            parents = pd.DataFrame(cursor.fetchall())
            cursor.execute(
                f"SELECT {','.join(evidence.SUPPORT_ACTION_COLUMNS)} "
                "FROM cash_adjustment_scale_support_action "
                "WHERE action_snapshot_run_id=%s ORDER BY evidence_key, "
                "support_action_source, support_action_key, support_action_type",
                (run_id,),
            )
            supports = pd.DataFrame(cursor.fetchall())

        payload = _manifest_payload(parents, supports)
        raw_manifest = _canonical(payload)
        if payload["row_digest"] != metadata["manifest_parent_row_digest"]:
            raise RuntimeError("persisted parent manifest digest mismatch")
        if payload["support_action_digest"] != metadata["manifest_support_action_digest"]:
            raise RuntimeError("persisted support manifest digest mismatch")
        if hashlib.sha256(raw_manifest).hexdigest() != metadata["manifest_sha256"]:
            raise RuntimeError("reconstructed source manifest hash mismatch")

        s3 = boto3.client("s3")
        with tempfile.TemporaryDirectory(prefix="cash-scale-seed-") as name:
            root = Path(name)
            manifest = root / evidence.MANIFEST_RELATIVE_PATH
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_bytes(raw_manifest)
            price_manifest = root / evidence.PRICE_OBJECT_MANIFEST_RELATIVE_PATH
            price_manifest.write_bytes(_canonical(_price_payload(s3, bucket, parents)))
            reference, component = _component_request(root)
            builder.download_kind_evidence(root, reference, component)
            kind_paths = krx_kind_reference.external_evidence_paths(root)
            for path in (*kind_paths, manifest, price_manifest):
                _put_once(s3, bucket, root, path)
        print(
            f"[cash-scale-seed] complete parents={len(parents)} "
            f"supports={len(supports)}",
            flush=True,
        )
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(lock)


if __name__ == "__main__":
    run()
