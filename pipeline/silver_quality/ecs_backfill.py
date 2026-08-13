"""Disabled legacy ECS one-off Silver rebuild implementation.

The task downloads only objects captured in the cutoff manifest, verifies that
their S3 ETag and size have not changed, atomically clears legacy Silver while
applying the DQ migration, then runs the normal staged backfill and final audit.
That sequence does not restore all FMP/DART source-scoped data or close the v3
return contract, so :func:`main` fails before S3/RDS access.
"""
from __future__ import annotations

import gc
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import boto3

from pipeline.common import db
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality import audit, backfill, s3_backfill
from pipeline.silver_quality.migrate import MIGRATIONS_DIR


MANIFEST_NAMES = ("stock", "index", "financials", "corporate_actions")
REBUILD_LOCK_ID = 7_226_494_896


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def _load_manifests(
    s3,
    bucket: str,
    prefix: str,
) -> tuple[list[dict], str]:
    digest = hashlib.sha256()
    objects: list[dict] = []
    for name in MANIFEST_NAMES:
        key = f"{prefix.rstrip('/')}/{name}.json"
        response = s3.get_object(Bucket=bucket, Key=key)
        raw = response["Body"].read()
        digest.update(name.encode("utf-8"))
        digest.update(raw)
        part = json.loads(raw)
        if not isinstance(part, list):
            raise RuntimeError(f"invalid manifest payload: {key}")
        objects.extend(part)

    keys = [item.get("key") for item in objects]
    if any(not isinstance(key, str) for key in keys):
        raise RuntimeError("manifest contains a non-string key")
    if len(keys) != len(set(keys)):
        raise RuntimeError("manifest contains duplicate S3 keys")
    allowed = ("stock/", "index/", "financials/", "corporate_actions/")
    invalid = [key for key in keys if not key.startswith(allowed)]
    if invalid:
        raise RuntimeError(f"manifest contains out-of-scope keys: {invalid[:10]}")
    return objects, digest.hexdigest()


def _download_one(s3, bucket: str, root: Path, item: dict) -> int:
    key = item["key"]
    expected_size = int(item["size"])
    expected_etag = str(item["etag"]).strip('"')
    response = s3.get_object(Bucket=bucket, Key=key)
    actual_etag = str(response["ETag"]).strip('"')
    actual_size = int(response["ContentLength"])
    if actual_etag != expected_etag or actual_size != expected_size:
        raise RuntimeError(
            "Bronze object changed after cutoff: "
            f"key={key}, expected=({expected_etag},{expected_size}), "
            f"actual=({actual_etag},{actual_size})"
        )

    destination = root / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    md5 = hashlib.md5(usedforsecurity=False)
    written = 0
    with destination.open("wb") as output:
        while chunk := response["Body"].read(1024 * 1024):
            output.write(chunk)
            md5.update(chunk)
            written += len(chunk)
    if written != expected_size:
        raise RuntimeError(
            f"short Bronze download: key={key}, expected={expected_size}, actual={written}"
        )
    if "-" not in expected_etag and md5.hexdigest() != expected_etag:
        raise RuntimeError(f"Bronze content checksum mismatch: key={key}")

    modified = item.get("last_modified")
    if modified:
        timestamp = datetime.fromisoformat(str(modified).replace("Z", "+00:00")).timestamp()
        os.utime(destination, (timestamp, timestamp))
    return written


def _sync_cutoff(
    root: Path,
    *,
    include_prefixes: tuple[str, ...] | None = None,
) -> str:
    bucket = _required("S3_BRONZE_BUCKET")
    prefix = _required("BACKFILL_MANIFEST_PREFIX")
    if root.exists() and any(root.iterdir()):
        raise RuntimeError(f"Bronze download root must be empty: {root}")
    root.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")
    all_objects, fingerprint = _load_manifests(s3, bucket, prefix)
    objects = (
        [
            item
            for item in all_objects
            if any(
                item["key"].startswith(allowed)
                for allowed in include_prefixes
            )
        ]
        if include_prefixes is not None
        else all_objects
    )
    total_bytes = 0
    completed = 0
    print(
        f"[ecs-backfill] syncing manifest objects={len(objects)}/"
        f"{len(all_objects)} "
        f"fingerprint={fingerprint}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [
            executor.submit(_download_one, s3, bucket, root, item)
            for item in objects
        ]
        for future in as_completed(futures):
            total_bytes += future.result()
            completed += 1
            if completed % 1000 == 0 or completed == len(objects):
                print(
                    f"[ecs-backfill] downloaded {completed}/{len(objects)} "
                    f"bytes={total_bytes}",
                    flush=True,
                )
    (root / ".bronze-input-fingerprint").write_text(
        fingerprint + "\n",
        encoding="utf-8",
    )
    return fingerprint


def _prepare_rds(*, s3_candidate_mode: bool, resume: bool) -> None:
    expected_database = _required("EXPECTED_DB_NAME")
    expected_host = _required("EXPECTED_DB_HOST")
    confirmation = _required("SILVER_REBUILD_CONFIRM")
    expected_confirmation = f"{expected_database}:2026-07-24"
    if confirmation != expected_confirmation:
        raise RuntimeError(
            "invalid rebuild confirmation: "
            f"expected={expected_confirmation!r}, actual={confirmation!r}"
        )

    parsed = urlparse(db.database_url())
    if parsed.hostname != expected_host or parsed.path.lstrip("/") != expected_database:
        raise RuntimeError(
            "refusing unexpected database target: "
            f"host={parsed.hostname}, database={parsed.path.lstrip('/')}"
        )

    conn = db.connect()
    try:
        with conn.transaction():
            acquire_return_writer_transaction_lock(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                actual_database = cur.fetchone()[0]
                if actual_database != expected_database:
                    raise RuntimeError(
                        f"connected to unexpected database: {actual_database}"
                    )
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (REBUILD_LOCK_ID,))
                counts = {}
                for table in (
                    "asset",
                    "asset_identifier",
                    "price_daily",
                    "fundamental",
                ):
                    cur.execute(f"SELECT count(*) FROM {table}")
                    counts[table] = cur.fetchone()[0]
                print(f"[ecs-backfill] legacy Silver counts={counts}", flush=True)
                for migration in sorted(
                    MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql")
                ):
                    cur.execute(migration.read_text(encoding="utf-8"))
                if not resume:
                    cur.execute(
                        "TRUNCATE fundamental, price_daily, "
                        "asset_identifier, asset RESTART IDENTITY CASCADE"
                    )
                if s3_candidate_mode and not resume:
                    cur.execute(
                        "TRUNCATE quality_stage.fundamental, "
                        "quality_stage.price_daily, "
                        "quality_stage.asset_identifier, quality_stage.asset"
                    )
                    cur.execute(
                        """
                        UPDATE dq_run
                        SET status='FAILED', finished_at=now(),
                            error_message=COALESCE(
                                error_message,
                                'superseded by S3 candidate backfill'
                            )
                        WHERE status IN ('RUNNING','BUILDING','VALIDATING')
                        """
                    )
        print("[ecs-backfill] Silver cleared and DQ migration committed", flush=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    raise RuntimeError(
        "destructive ECS Silver backfill is disabled: it truncates shared "
        "KRX/FMP/DART data without the authenticated fundamental reload and "
        "closed total-return recertification workflow"
    )


def _unsafe_legacy_main() -> None:
    """Retained for forensic reference; never dispatch from an entrypoint."""
    root = Path(os.environ.get("BACKFILL_DATA_ROOT", "/app/data"))
    fingerprint = _sync_cutoff(root)
    print(f"[ecs-backfill] cutoff verified fingerprint={fingerprint}", flush=True)
    resume = os.environ.get("BACKFILL_RESUME_RUN_ID")
    s3_candidate_mode = os.environ.get("S3_CANDIDATE_BACKFILL", "1") == "1"
    _prepare_rds(
        s3_candidate_mode=s3_candidate_mode,
        resume=bool(resume),
    )
    if s3_candidate_mode:
        s3_backfill.run("local", resume=resume)
    else:
        backfill.run("local", resume=resume)
    gc.collect()
    if not s3_candidate_mode:
        audit.run("all")
    print("[ecs-backfill] rebuild and audit complete", flush=True)


if __name__ == "__main__":
    main()
