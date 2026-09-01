"""Rebuild the complete KIND support manifest from certified cash-scale evidence."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from pipeline.silver import cash_adjustment_scale_builder as builder
from pipeline.silver import cash_adjustment_scale_evidence as evidence
from pipeline.silver import krx_kind_reference as kind


def _put_immutable(s3, bucket: str, root: Path, path: Path) -> None:
    key = path.relative_to(root).as_posix()
    body = path.read_bytes()
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
    except ClientError as exc:
        status = int(
            exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        )
        if status not in {409, 412}:
            raise
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != body:
            raise RuntimeError(f"immutable KIND object conflicts: {key}") from exc


def _get(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def run() -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    s3 = boto3.client("s3")
    source_key = evidence.MANIFEST_RELATIVE_PATH.as_posix()
    support_key = kind.KIND_SUPPORT_MANIFEST_RELATIVE_PATH.as_posix()
    source = json.loads(_get(s3, bucket, source_key))
    current_response = s3.get_object(Bucket=bucket, Key=support_key)
    current_raw = current_response["Body"].read()
    current = json.loads(current_raw)
    current_etag = str(current_response["ETag"])
    component_path = str(current["component_request_path"])
    component_raw = _get(s3, bucket, component_path)

    references: list[tuple[str, dict[str, object]]] = []
    components: list[tuple[str, dict[str, object]]] = []
    identities: set[tuple[str, str, str]] = set()
    for parent in source["evidence"]:
        ticker = str(parent["ticker"]).zfill(6)
        for child in parent["support_actions"]:
            if child["support_action_source"] != "KRX_KIND":
                continue
            identity = (
                ticker,
                str(child["support_action_key"]),
                str(child["support_action_type"]),
            )
            if identity in identities:
                raise RuntimeError(f"duplicate cash-scale KIND child: {identity}")
            identities.add(identity)
            target = (
                components
                if child["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
                else references
            )
            target.append((ticker, child))
    if len(references) != 104 or len(components) != 2:
        raise RuntimeError(
            f"unexpected KIND child counts: references={len(references)} "
            f"components={len(components)}"
        )

    cache: dict[str, bytes] = {}
    rows: list[dict[str, object]] = []
    for index, (ticker, child) in enumerate(references, start=1):
        acceptance = str(child["support_action_key"])
        identity_url = (
            "https://kind.krx.co.kr/common/disclsviewer.do?method=search&"
            f"acptno={acceptance}&docno=&viewerhost=&viewerport="
        )
        identity_body = builder._http_body(identity_url)
        receipt = kind.parse_kind_identity_receipt(identity_body)
        if receipt.acceptance_no != acceptance or receipt.ticker != ticker:
            raise RuntimeError(f"KIND identity mismatch: {ticker}/{acceptance}")
        contents_url = (
            "https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&"
            f"docNo={receipt.selected_document_no}"
        )
        contents_body = builder._http_body(contents_url)
        body_url = kind.parse_kind_contents_body_url(contents_body)
        action_key, _, form_code = kind.kind_url_identity(body_url)
        if action_key != acceptance:
            raise RuntimeError(f"KIND selected body mismatch: {ticker}/{acceptance}")
        body_key = str(child["support_action_body_path"])
        body = _get(s3, bucket, body_key)
        body_sha = hashlib.sha256(body).hexdigest()
        if body_sha != child["support_action_body_sha256"]:
            raise RuntimeError(f"KIND body SHA mismatch: {body_key}")
        cache[identity_url] = identity_body
        cache[body_url] = body
        rows.append({
            "ticker": ticker,
            "asset_name": receipt.issuer_name,
            "security_class": child["support_entitlement_security_class"],
            "source_url": body_url,
            "identity_source_url": identity_url,
            "support_semantic_role": child["support_semantic_role"],
            "source_form_code": form_code,
            "target_adjustment_date": child["target_adjustment_date"],
            "target_cash_receipt_no": child["target_cash_receipt_no"],
            "identity_content_length": len(identity_body),
            "identity_sha256": hashlib.sha256(identity_body).hexdigest(),
            "body_content_length": len(body),
            "body_sha256": body_sha,
        })
        print(f"[kind-rebuild] reviewed {index}/{len(references)}", flush=True)

    rows.sort(key=lambda row: (str(row["ticker"]), str(row["source_url"])))
    reference_payload = {
        "schema_version": kind.KIND_REQUEST_SCHEMA,
        "complete": True,
        "request_count": len(rows),
        "request_digest": hashlib.sha256(kind.canonical_bytes(rows)).hexdigest(),
        "requests": rows,
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_MAIN_AND_SELECTED_BODY",
    }
    reference_raw = kind.canonical_bytes(reference_payload)

    with tempfile.TemporaryDirectory(prefix="kind-support-rebuild-") as name:
        root = Path(name).resolve()
        reference_file = root / "reference-requests-v2.json"
        component_file = root / "component-requests-v2.json"
        reference_file.write_bytes(reference_raw)
        component_file.write_bytes(component_raw)

        def fetch(url: str) -> bytes:
            body = cache.get(url)
            if body is None:
                body = builder._http_body(url)
                cache[url] = body
            return body

        supports = builder.download_kind_evidence(
            root, reference_file, component_file, fetcher=fetch,
        )
        if len(supports) != len(identities):
            raise RuntimeError(
                f"rebuilt KIND support count mismatch: {len(supports)}"
            )
        paths = kind.external_evidence_paths(root)
        manifest = root / kind.KIND_SUPPORT_MANIFEST_RELATIVE_PATH
        for path in paths:
            if path != manifest:
                _put_immutable(s3, bucket, root, path)
        manifest_raw = manifest.read_bytes()
        s3.put_object(
            Bucket=bucket,
            Key=support_key,
            Body=manifest_raw,
            IfMatch=current_etag,
            ContentType="application/json",
        )
        print(
            f"[kind-rebuild] complete supports={len(supports)} "
            f"manifest_sha256={hashlib.sha256(manifest_raw).hexdigest()}",
            flush=True,
        )


if __name__ == "__main__":
    run()
