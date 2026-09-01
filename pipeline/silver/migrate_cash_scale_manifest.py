"""Rebind unavailable display-only DART list evidence to an authenticated list.

This recovery operation changes no economic body, price body, receipt, amount,
or support action.  It only replaces an unavailable ``cash_action_body_path``
for ``VERIFIED_DART_VIEWER_BODY`` parents, then recomputes the parent and
aggregate manifest digests.  The replacement list must contain every affected
cash receipt and already exist in the Bronze bucket.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from copy import deepcopy

import boto3
import pandas as pd

from pipeline import dart_silver_backfill_ecs
from pipeline.silver import cash_adjustment_scale_evidence as evidence


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def migrate_payload(
    payload: dict[str, object],
    *,
    unavailable_path: str,
    replacement_path: str,
    replacement_body: bytes,
) -> tuple[dict[str, object], int]:
    if not unavailable_path or unavailable_path == replacement_path:
        raise ValueError("cash-scale evidence paths must be distinct")
    replacement_sha = hashlib.sha256(replacement_body).hexdigest()
    try:
        disclosures = json.loads(replacement_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("replacement DART disclosure list is invalid") from exc
    if not isinstance(disclosures, list):
        raise RuntimeError("replacement DART disclosure body must be a list")
    receipts = {
        str(row.get("rcept_no") or "")
        for row in disclosures if isinstance(row, dict)
    }

    migrated = deepcopy(payload)
    entries = migrated.get("evidence")
    if not isinstance(entries, list):
        raise RuntimeError("cash-scale evidence payload has no evidence list")
    changed = 0
    for row in entries:
        if not isinstance(row, dict):
            raise RuntimeError("cash-scale evidence parent is not an object")
        if row.get("cash_action_body_path") != unavailable_path:
            continue
        if row.get("cash_source_evidence_status") != "VERIFIED_DART_VIEWER_BODY":
            raise RuntimeError(
                "only DART viewer parents may rebind their display list"
            )
        if row.get("cash_economic_body_path") == unavailable_path:
            raise RuntimeError("economic cash evidence cannot be rebound")
        receipt = str(row.get("cash_receipt_no") or "")
        if receipt not in receipts:
            raise RuntimeError(
                f"replacement DART list is missing cash receipt: {receipt}"
            )
        row["cash_action_body_path"] = replacement_path
        row["cash_action_body_sha256"] = replacement_sha
        row["manifest_row_sha256"] = evidence.manifest_parent_row_sha256(row)
        changed += 1
    if changed == 0:
        raise RuntimeError("no cash-scale parents reference the unavailable path")

    parent_frame = pd.DataFrame(entries)
    migrated["row_count"] = len(parent_frame)
    migrated["row_digest"] = evidence.source_manifest_digest(parent_frame)
    return migrated, changed


def run(*, unavailable_path: str, replacement_path: str) -> None:
    lock = dart_silver_backfill_ecs.acquire_daily_certification_lock()
    try:
        bucket = os.environ["S3_BRONZE_BUCKET"]
        s3 = boto3.client("s3")
        key = evidence.MANIFEST_RELATIVE_PATH.as_posix()
        current = s3.get_object(Bucket=bucket, Key=key)
        current_body = current["Body"].read()
        current_etag = str(current["ETag"])
        try:
            payload = json.loads(current_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("cash-scale evidence manifest is invalid") from exc
        replacement = s3.get_object(
            Bucket=bucket, Key=replacement_path,
        )["Body"].read()
        migrated, changed = migrate_payload(
            payload,
            unavailable_path=unavailable_path,
            replacement_path=replacement_path,
            replacement_body=replacement,
        )
        rendered = _canonical(migrated)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=rendered,
            ContentType="application/json",
            IfMatch=current_etag,
        )
        print(
            "[cash-scale-manifest-migration] "
            f"parents={changed} sha256={hashlib.sha256(rendered).hexdigest()}",
            flush=True,
        )
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(lock)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unavailable-path", required=True)
    parser.add_argument("--replacement-path", required=True)
    args = parser.parse_args()
    run(
        unavailable_path=args.unavailable_path,
        replacement_path=args.replacement_path,
    )


if __name__ == "__main__":
    main()
