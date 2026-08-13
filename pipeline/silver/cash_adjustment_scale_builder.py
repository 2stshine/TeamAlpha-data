"""Build content-addressed evidence for cash events on KRX scale changes.

This recovery tool is intentionally read-only with respect to AWS and RDS.
It has three local-output phases:

* ``download-prices`` uses AWS CLI ``head-object`` followed by a conditional
  ``get-object --if-match`` for the exact KRX Bronze objects required by the
  reviewed 331-event overlap receipt;
* ``download-kind`` verifies separate externally reviewed reference/component
  request objects, then retains all official identity/contents/body responses
  as content-addressed objects;
* ``build`` re-parses a freshly completed DART v3/v5 snapshot, resolves exact
  cash/support bodies, and atomically writes the v1 source-evidence manifest.

No previous support-classification CSV is accepted as input.  Every support
relationship is derived again from the verified official snapshot.  A plain
cash ex-dividend notice is never an adjustment component.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable, Sequence
from urllib.parse import urlparse

import pandas as pd
import requests

from pipeline.bronze.dart_viewer_corrections import (
    verify_viewer_corrections,
)
from pipeline.bronze.dart_support_action_families import (
    bonus_issue_common_terms_from_body,
    stock_dividend_common_terms_from_body,
    SupportActionFamilyEntry,
    verify_support_action_families,
)
from pipeline.silver import corporate_actions
from pipeline.silver.cash_adjustment_scale_evidence import (
    MANIFEST_RELATIVE_PATH,
    PRE_EVENT_PRICE_SCALE,
    PRICE_OBJECT_MANIFEST_CONTRACT,
    PRICE_OBJECT_MANIFEST_RELATIVE_PATH,
    SOURCE_EVIDENCE_CONTRACT,
    manifest_parent_row_sha256,
    manifest_support_row_sha256,
    source_manifest_digest,
    support_manifest_digest,
    verify_source_evidence_manifest,
)
from pipeline.silver.dart_action_snapshot import (
    DEFAULT_COVERAGE_START,
    _assert_continuous,
    _native_complete_intervals,
)
from pipeline.silver.krx_kind_reference import (
    DartDetachmentNoticeNotFound,
    KIND_BODY_OBJECT_ROOT,
    KIND_COMPONENT_REPORT_NAME_61474,
    KIND_COMPONENT_REPORT_NAME_11306,
    KIND_COMPONENT_REQUEST_SCHEMA,
    KIND_CONTENTS_OBJECT_ROOT,
    KIND_IDENTITY_OBJECT_ROOT,
    KIND_REQUEST_OBJECT_ROOT,
    KIND_REQUEST_SCHEMA,
    KIND_REFERENCE_REPORT_NAME_70767,
    KIND_REFERENCE_REPORT_NAME_99311,
    KIND_REFERENCE_REPORT_NAME_99302,
    KIND_SUPPORT_MANIFEST_RELATIVE_PATH,
    KIND_SUPPORT_SCHEMA,
    kind_component_url_identity,
    kind_contents_url_document_no,
    kind_identity_url_acceptance,
    kind_url_identity as _kind_url_identity,
    kind_url_document_no,
    parse_kind_contents_body_url,
    parse_dart_detachment_notice,
    parse_kind_identity_receipt,
    parse_kind_reference_notice as _kind_reference_notice,
    parse_kind_stock_dividend_component,
    parse_kind_paid_increase_component,
    verify_kind_component_request_object,
    verify_kind_request_object,
    verify_kind_support_manifest,
)
from pipeline.silver.reviewed_dividend_corrections import (
    active_corrections as active_reviewed_corrections,
)
from pipeline.silver.total_returns import stored_price_factor_interval
from pipeline.silver.reviewed_cash_scale_exceptions import (
    NO_NOTICE_STOCK_DIVIDEND,
    REVIEWED_COMBINED_NOTICE_GROUP_KINDS,
)


RECOVERY_OVERLAP_SHA256 = (
    "86c154409c2380c6823ed18a7275fde37419cd8a845e4561d83a1dc4f3cdcfbc"
)
RECOVERY_EXPECTATIONS_SHA256 = (
    "7e7346a8d115c851d8d85ac457334df4485ecb02f223e004f272df4f289d0867"
)
CLASSIFICATION_RELATIVE_PATH = Path(
    "corporate_actions/krx/cash_adjustment_scale_classification.json"
)
BUILD_SUMMARY_RELATIVE_PATH = Path(
    "corporate_actions/krx/cash_adjustment_scale_source_evidence.summary.json"
)
PRICE_OBJECT_SCHEMA = PRICE_OBJECT_MANIFEST_CONTRACT
CLASSIFICATION_SCHEMA = "krx_cash_adjustment_scale_classification_v1"
BUILD_SUMMARY_SCHEMA = "krx_cash_adjustment_scale_build_summary_v1"
PRICE_SCHEMA = "marcap_parquet_v1"
EXPECTED_PARENT_COUNT = 331
EXPECTED_PRICE_OBJECT_COUNT = 49
_SHA = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT = re.compile(r"^[0-9]{14}$")


@dataclass(frozen=True)
class RecoveryInputs:
    overlap: pd.DataFrame
    missing_pairs: pd.DataFrame
    resolved_cash: pd.DataFrame
    expectations: dict[str, object]
    overlap_path: Path
    expectations_path: Path
    missing_pairs_path: Path
    resolved_cash_path: Path

    @property
    def price_dates(self) -> tuple[date, ...]:
        values = set(self.overlap["previous_date"])
        values.update(self.overlap["applied_date"])
        return tuple(sorted(values))


@dataclass(frozen=True)
class PriceObject:
    trade_date: date
    source_object_key: str
    local_path: str
    etag: str
    content_length: int
    content_sha256: str
    version_id: str | None
    server_side_encryption: str | None
    source_schema: str = PRICE_SCHEMA


@dataclass(frozen=True)
class BuildResult:
    manifest_path: str
    manifest_sha256: str
    parent_count: int
    parent_digest: str
    support_action_count: int
    support_action_digest: str
    semantic_group_count: int
    summary_path: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory_descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _json_list(value: object, *, field: str) -> list[object]:
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid recovery JSON-list field: {field}") from exc
    if not isinstance(decoded, list):
        raise RuntimeError(f"recovery field is not a JSON list: {field}")
    return decoded


def _date(value: object, *, field: str) -> date:
    try:
        return pd.Timestamp(value).date()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid {field}: {value!r}") from exc


def _verify_named_input(
    directory: Path,
    name: str,
    declared: dict[str, object],
) -> Path:
    path = directory / name
    expected = str(declared.get("sha256") or "")
    if not path.is_file() or _SHA.fullmatch(expected) is None:
        raise RuntimeError(f"missing/invalid recovery input declaration: {name}")
    if _sha256(path) != expected:
        raise RuntimeError(f"recovery input SHA mismatch: {name}")
    return path


def load_recovery_inputs(
    overlap_path: str | Path,
    expectations_path: str | Path,
    *,
    expected_overlap_sha256: str = RECOVERY_OVERLAP_SHA256,
    expected_expectations_sha256: str = RECOVERY_EXPECTATIONS_SHA256,
) -> RecoveryInputs:
    """Verify the frozen receipt and recompute its basic set invariants."""
    overlap_file = Path(overlap_path).expanduser().resolve()
    expectations_file = Path(expectations_path).expanduser().resolve()
    if _sha256(overlap_file) != expected_overlap_sha256:
        raise RuntimeError("recovery overlap SHA mismatch")
    if _sha256(expectations_file) != expected_expectations_sha256:
        raise RuntimeError("recovery expectations SHA mismatch")
    try:
        expectations = json.loads(expectations_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid recovery expectations JSON") from exc
    if expectations.get("schema_version") != (
        "teamalpha_dividend_final_gate_expectations_v1"
    ):
        raise RuntimeError("unsupported recovery expectations schema")
    declared_inputs = expectations.get("inputs")
    if not isinstance(declared_inputs, dict):
        raise RuntimeError("recovery expectations have no input receipts")
    expected_overlap = declared_inputs.get(overlap_file.name)
    if not isinstance(expected_overlap, dict) or (
        expected_overlap.get("sha256") != expected_overlap_sha256
    ):
        raise RuntimeError("expectations/overlap receipt mismatch")
    directory = expectations_file.parent
    missing_file = _verify_named_input(
        directory,
        "teamalpha-dividend-price-pair-missing-20260812.csv",
        declared_inputs.get(
            "teamalpha-dividend-price-pair-missing-20260812.csv", {}
        ),
    )
    resolved_file = _verify_named_input(
        directory,
        "teamalpha-dividend-resolved-cache-20260812.parquet",
        declared_inputs.get(
            "teamalpha-dividend-resolved-cache-20260812.parquet", {}
        ),
    )

    overlap = pd.read_csv(overlap_file, dtype={"ticker": "string"})
    required_overlap = {
        "asset_id", "ticker", "ex_date", "previous_date", "applied_date",
        "cash_receipts", "cash_amounts", "record_dates", "cash_event_count",
        "previous_close", "previous_adj_close", "applied_close",
        "applied_adj_close", "source_adjustment_factor",
    }
    if not required_overlap.issubset(overlap.columns):
        raise RuntimeError("recovery overlap columns are incomplete")
    overlap["ticker"] = overlap["ticker"].astype(str).str.zfill(6)
    for column in ("ex_date", "previous_date", "applied_date"):
        overlap[column] = overlap[column].map(
            lambda value, name=column: _date(value, field=name)
        )
    counts = expectations.get("counts") or {}
    expected_parents = int(counts.get("expected_external_evidence_parent_rows", -1))
    if expected_parents != EXPECTED_PARENT_COUNT or len(overlap) != expected_parents:
        raise RuntimeError("recovery overlap parent-count mismatch")
    pair_columns = ["asset_id", "ex_date"]
    if overlap.duplicated(pair_columns).any():
        raise RuntimeError("recovery changed pairs are not unique")
    if not overlap["ex_date"].eq(overlap["applied_date"]).all():
        raise RuntimeError("recovery applied/ex dates differ")
    if not overlap["cash_event_count"].eq(1).all():
        raise RuntimeError("recovery changed pair is not one cash event")
    for row in overlap.itertuples(index=False):
        receipts = _json_list(row.cash_receipts, field="cash_receipts")
        amounts = _json_list(row.cash_amounts, field="cash_amounts")
        records = _json_list(row.record_dates, field="record_dates")
        if (
            len(receipts) != 1 or _RECEIPT.fullmatch(str(receipts[0])) is None
            or len(amounts) != 1 or float(amounts[0]) <= 0
            or len(records) != 1
        ):
            raise RuntimeError("recovery changed pair cash identity is invalid")
        if not math.isclose(
            float(row.source_adjustment_factor),
            (
                float(row.previous_adj_close) / float(row.previous_close)
            ) / (
                float(row.applied_adj_close) / float(row.applied_close)
            ),
            rel_tol=0,
            abs_tol=5e-13,
        ):
            raise RuntimeError("recovery source adjustment factor changed")

    missing = pd.read_csv(missing_file, dtype={"ticker": "string"})
    if not {"asset_id", "ex_date", "cash_event_count"}.issubset(missing.columns):
        raise RuntimeError("first-listing exclusion columns are incomplete")
    missing["ticker"] = missing["ticker"].astype(str).str.zfill(6)
    missing["ex_date"] = missing["ex_date"].map(
        lambda value: _date(value, field="first-listing ex_date")
    )
    expected_missing = int(counts.get("first_listing_exclusion_pairs", -1))
    if len(missing) != expected_missing or expected_missing != 2:
        raise RuntimeError("first-listing exclusion count changed")
    if not missing["cash_event_count"].eq(1).all():
        raise RuntimeError("first-listing exclusion event count changed")
    changed_pairs = set(overlap[pair_columns].itertuples(index=False, name=None))
    first_pairs = set(missing[pair_columns].itertuples(index=False, name=None))
    if changed_pairs.intersection(first_pairs):
        raise RuntimeError("first-listing pair leaked into changed-scale evidence")

    resolved = pd.read_parquet(resolved_file)
    required_resolved = {
        "asset_id", "source", "dividend_key", "action_key", "is_canonical",
        "excluded_reason", "resolved_ex_date",
    }
    if not required_resolved.issubset(resolved.columns):
        raise RuntimeError("resolved cash receipt columns are incomplete")
    if len(resolved) != int(counts.get("canonical_cash_events", -1)):
        raise RuntimeError("canonical cash-event count changed")
    if (
        not resolved["is_canonical"].fillna(False).all()
        or resolved["excluded_reason"].notna().any()
        or resolved["resolved_ex_date"].isna().any()
    ):
        raise RuntimeError("resolved cash cache is no longer canonical")
    if resolved.duplicated(["asset_id", "source", "dividend_key"]).any():
        raise RuntimeError("resolved cash event keys are not unique")
    resolved["resolved_ex_date"] = resolved["resolved_ex_date"].map(
        lambda value: _date(value, field="resolved_ex_date")
    )
    canonical_pairs = set(
        resolved[["asset_id", "resolved_ex_date"]]
        .itertuples(index=False, name=None)
    )
    if len(canonical_pairs) != int(counts.get("canonical_asset_date_pairs", -1)):
        raise RuntimeError("canonical cash pair count changed")
    if not changed_pairs.issubset(canonical_pairs) or not first_pairs.issubset(
        canonical_pairs
    ):
        raise RuntimeError("changed/first-listing pairs escaped canonical cash set")
    if len(set(overlap["previous_date"]).union(overlap["applied_date"])) != (
        EXPECTED_PRICE_OBJECT_COUNT
    ):
        raise RuntimeError("recovery price-object date count changed")
    return RecoveryInputs(
        overlap=overlap,
        missing_pairs=missing,
        resolved_cash=resolved,
        expectations=expectations,
        overlap_path=overlap_file,
        expectations_path=expectations_file,
        missing_pairs_path=missing_file,
        resolved_cash_path=resolved_file,
    )


def verify_fresh_dart_snapshot(
    base: str | Path,
    *,
    coverage_start: date,
    coverage_end: date,
) -> tuple[tuple[date, date], ...]:
    """Require native disclosures_v3/structured_v3/documents_v5 coverage."""
    root = Path(base).expanduser().resolve()
    intervals = _native_complete_intervals(
        root,
        required_start=coverage_start,
        required_end=coverage_end,
    )
    return _assert_continuous(
        intervals,
        required_start=coverage_start,
        required_end=coverage_end,
    )


def _parse_s3_root(value: str) -> tuple[str, str]:
    error = "price source root is not a canonical safe S3 URI"
    if (
        not isinstance(value, str)
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or "%" in value
    ):
        raise ValueError(error)
    parsed = urlparse(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or "@" in parsed.netloc
        or ":" in parsed.netloc
    ):
        raise ValueError(error)
    bucket = parsed.netloc
    if (
        len(bucket) < 3
        or len(bucket) > 63
        or re.fullmatch(r"[a-z0-9][a-z0-9.-]*[a-z0-9]", bucket) is None
        or ".." in bucket
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+){3}", bucket) is not None
    ):
        raise ValueError(error)
    if not parsed.path:
        return bucket, ""
    if not parsed.path.startswith("/") or parsed.path == "/":
        raise ValueError(error)
    prefix = parsed.path[1:]
    segments = prefix.split("/")
    if any(
        not segment
        or segment in {".", ".."}
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._=-]*", segment) is None
        for segment in segments
    ):
        raise ValueError(error)
    return bucket, prefix


def _aws_json(
    arguments: Sequence[str],
    *,
    profile: str | None,
    region: str | None,
) -> dict[str, object]:
    command = ["aws"]
    if profile:
        command.extend(["--profile", profile])
    if region:
        command.extend(["--region", region])
    command.extend([*arguments, "--output", "json", "--no-cli-pager"])
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError("AWS CLI returned invalid JSON") from exc


def _price_object_payload(objects: Iterable[PriceObject], *, bucket: str, prefix: str) -> dict:
    rows = [
        {
            "trade_date": item.trade_date.isoformat(),
            "source_object_key": item.source_object_key,
            "local_path": item.local_path,
            "etag": item.etag,
            "content_length": item.content_length,
            "content_sha256": item.content_sha256,
            "version_id": item.version_id,
            "server_side_encryption": item.server_side_encryption,
            "source_schema": item.source_schema,
        }
        for item in sorted(objects, key=lambda value: value.trade_date)
    ]
    digest = hashlib.sha256(_canonical_bytes(rows)).hexdigest()
    return {
        "schema_version": PRICE_OBJECT_SCHEMA,
        "complete": True,
        "source_bucket": bucket,
        "source_prefix": prefix,
        "object_count": len(rows),
        "object_digest": digest,
        "objects": rows,
    }


def download_price_objects(
    base: str | Path,
    inputs: RecoveryInputs,
    *,
    s3_root: str,
    profile: str | None,
    region: str | None,
    aws_runner: Callable[..., dict[str, object]] = _aws_json,
) -> tuple[PriceObject, ...]:
    """HEAD then conditionally GET the exact 49 immutable Bronze objects.

    Every body is validated in a staging directory before any canonical path
    changes.  Publishing then has an exact rollback journal for both the 49
    bodies and the canonical receipt manifest, so a failed verifier cannot
    leave a half-updated local snapshot behind.
    """
    root = Path(base).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    bucket, prefix = _parse_s3_root(s3_root)
    manifest_path = root / PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    previous_manifest = manifest_path.read_bytes() if manifest_path.is_file() else None
    with tempfile.TemporaryDirectory(
        prefix=".cash-scale-price-download.", dir=root,
    ) as staging_name:
        staging = Path(staging_name)
        objects: list[PriceObject] = []
        staged: list[tuple[Path, Path]] = []
        for trade_date in inputs.price_dates:
            relative = f"stock/marcap/date={trade_date.isoformat()}/all.parquet"
            source_key = "/".join(part for part in (prefix, relative) if part)
            common = [
                "s3api", "head-object", "--bucket", bucket, "--key", source_key,
            ]
            head = aws_runner(common, profile=profile, region=region)
            etag = str(head.get("ETag") or "").strip('"').lower()
            if re.fullmatch(r"[0-9a-f]{32}", etag) is None:
                raise RuntimeError(
                    f"price object does not have a single-part ETag: {source_key}"
                )
            if head.get("VersionId"):
                raise RuntimeError(f"unexpected versioned price object: {source_key}")
            try:
                content_length = int(head["ContentLength"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"missing S3 content length for {source_key}"
                ) from exc
            if content_length <= 0:
                raise RuntimeError(f"empty S3 price object: {source_key}")
            destination = root / relative
            temporary = staging / f"{trade_date.isoformat()}.parquet"
            get_arguments = [
                "s3api", "get-object", "--bucket", bucket, "--key", source_key,
                "--if-match", etag,
            ]
            version_id = None
            get_arguments.append(str(temporary))
            received = aws_runner(
                get_arguments,
                profile=profile,
                region=region,
            )
            received_etag = str(received.get("ETag") or "").strip('"').lower()
            if received_etag != etag:
                raise RuntimeError(f"S3 object changed during GET: {source_key}")
            if received.get("VersionId"):
                raise RuntimeError(
                    f"unexpected versioned price object during GET: {source_key}"
                )
            if temporary.stat().st_size != content_length:
                raise RuntimeError(f"S3 content length/body mismatch: {source_key}")
            if _md5(temporary) != etag:
                raise RuntimeError(f"S3 single-part ETag/body mismatch: {source_key}")
            content_sha = _sha256(temporary)
            try:
                schema = pd.read_parquet(temporary, columns=[]).columns
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"invalid KRX marcap parquet: {source_key}") from exc
            del schema
            staged.append((temporary, destination))
            objects.append(PriceObject(
                trade_date=trade_date,
                source_object_key=source_key,
                local_path=relative,
                etag=etag,
                content_length=content_length,
                content_sha256=content_sha,
                version_id=(str(version_id) if version_id else None),
                server_side_encryption=(
                    str(head.get("ServerSideEncryption"))
                    if head.get("ServerSideEncryption") else None
                ),
            ))
        if len(objects) != EXPECTED_PRICE_OBJECT_COUNT:
            raise RuntimeError("price download did not produce exactly 49 objects")

        published: list[tuple[Path, Path | None]] = []
        try:
            for index, (temporary, destination) in enumerate(staged):
                destination.parent.mkdir(parents=True, exist_ok=True)
                backup = staging / f"previous-{index}.parquet"
                previous = backup if destination.is_file() else None
                if previous is not None:
                    os.replace(destination, previous)
                # Journal before replace so even an exceptional replace
                # restores the previous canonical body.
                published.append((destination, previous))
                os.replace(temporary, destination)
            _atomic_write(
                manifest_path,
                _canonical_bytes(
                    _price_object_payload(objects, bucket=bucket, prefix=prefix)
                ),
            )
            verified = verify_price_object_manifest(
                root, inputs, expected_s3_root=s3_root,
            )
            verified_again = verify_price_object_manifest(
                root, inputs, expected_s3_root=s3_root,
            )
            if verified != verified_again:
                raise RuntimeError("price-object verifier roundtrip changed")
        except Exception:
            if previous_manifest is None:
                manifest_path.unlink(missing_ok=True)
            else:
                _atomic_write(manifest_path, previous_manifest)
            for destination, previous in reversed(published):
                destination.unlink(missing_ok=True)
                if previous is not None:
                    os.replace(previous, destination)
            raise
        return verified


def verify_price_object_manifest(
    base: str | Path,
    inputs: RecoveryInputs,
    *,
    expected_s3_root: str,
) -> tuple[PriceObject, ...]:
    root = Path(base).expanduser().resolve()
    path = root / PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    try:
        raw_manifest = path.read_bytes()
        payload = json.loads(raw_manifest)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("missing/invalid price-object manifest") from exc
    if raw_manifest != _canonical_bytes(payload):
        raise RuntimeError("price-object manifest is not canonical JSON bytes")
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version", "complete", "source_bucket", "source_prefix",
        "object_count", "object_digest", "objects",
    }:
        raise RuntimeError("price-object manifest fields changed")
    if payload.get("schema_version") != PRICE_OBJECT_SCHEMA or payload.get(
        "complete"
    ) is not True:
        raise RuntimeError("price-object manifest is not complete")
    expected_bucket, expected_prefix = _parse_s3_root(expected_s3_root)
    if (
        payload.get("source_bucket") != expected_bucket
        or payload.get("source_prefix") != expected_prefix
    ):
        raise RuntimeError("price-object source root mismatch")
    rows = payload.get("objects")
    if not isinstance(rows, list) or len(rows) != EXPECTED_PRICE_OBJECT_COUNT:
        raise RuntimeError("price-object manifest count mismatch")
    if int(payload.get("object_count", -1)) != len(rows):
        raise RuntimeError("price-object declared count mismatch")
    if payload.get("object_digest") != hashlib.sha256(
        _canonical_bytes(rows)
    ).hexdigest():
        raise RuntimeError("price-object aggregate digest mismatch")
    objects: list[PriceObject] = []
    row_fields = {
        "trade_date", "source_object_key", "local_path", "etag",
        "content_length", "content_sha256", "version_id",
        "server_side_encryption", "source_schema",
    }
    for row in rows:
        if not isinstance(row, dict) or set(row) != row_fields:
            raise RuntimeError("price-object entry fields changed")
        item = PriceObject(
            trade_date=_date(row.get("trade_date"), field="price trade_date"),
            source_object_key=str(row.get("source_object_key") or ""),
            local_path=str(row.get("local_path") or ""),
            etag=str(row.get("etag") or "").strip('"').lower(),
            content_length=int(row.get("content_length", -1)),
            content_sha256=str(row.get("content_sha256") or ""),
            version_id=(str(row["version_id"]) if row.get("version_id") else None),
            server_side_encryption=(
                str(row["server_side_encryption"])
                if row.get("server_side_encryption") else None
            ),
            source_schema=str(row.get("source_schema") or ""),
        )
        relative = Path(item.local_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("price-object local path escapes snapshot")
        expected_relative = Path(
            f"stock/marcap/date={item.trade_date.isoformat()}/all.parquet"
        )
        if relative != expected_relative or item.source_schema != PRICE_SCHEMA:
            raise RuntimeError("price-object path/schema mismatch")
        expected_source_key = "/".join(
            part for part in (expected_prefix, relative.as_posix()) if part
        )
        if item.source_object_key != expected_source_key:
            raise RuntimeError("price-object source key mismatch")
        if re.fullmatch(r"[0-9a-f]{32}", item.etag) is None:
            raise RuntimeError("price-object must have a single-part ETag")
        if item.version_id is not None:
            raise RuntimeError("unexpected versioned price-object receipt")
        local = root / relative
        if (
            not local.is_file()
            or local.stat().st_size != item.content_length
            or _sha256(local) != item.content_sha256
        ):
            raise RuntimeError("price-object local content mismatch")
        if _md5(local) != item.etag:
            raise RuntimeError("price-object ETag/body mismatch")
        objects.append(item)
    if tuple(sorted(item.trade_date for item in objects)) != inputs.price_dates:
        raise RuntimeError("price-object date set has missing/unused objects")
    if len({item.trade_date for item in objects}) != len(objects):
        raise RuntimeError("duplicate price-object trade date")
    return tuple(sorted(objects, key=lambda item: item.trade_date))


def _relative(root: Path, path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise RuntimeError(f"evidence body escapes snapshot: {path}") from exc


def _matching_body(root: Path, event: dict[str, object]) -> tuple[str, str]:
    expected_sha = str(event.get("source_body_sha256") or "")
    receipt = str(event.get("rcept_no") or "")
    ticker = str(event.get("identifier") or "").zfill(6)
    candidates: list[Path] = []
    source_file = Path(str(event.get("source_file") or ""))
    if source_file.is_file():
        candidates.append(source_file)
    candidates.extend((root / "corporate_actions" / "dart" / "documents").glob(
        f"year=*/corp={ticker}/rcept={receipt}.zip"
    ))
    matched = sorted({
        path.resolve() for path in candidates
        if path.is_file() and _sha256(path) == expected_sha
    })
    if len(matched) != 1:
        raise RuntimeError(
            "official action body is missing/ambiguous: "
            f"ticker={ticker} receipt={receipt} matches={len(matched)}"
        )
    return _relative(root, matched[0]), expected_sha


def _cash_body(
    root: Path,
    event: dict[str, object],
    viewer_by_receipt: dict[str, object],
) -> dict[str, str]:
    action_path, action_sha = _matching_body(root, event)
    receipt = str(event["rcept_no"])
    status = str(event.get("source_evidence_status") or "")
    economic_sha = str(event.get("economic_evidence_sha256") or "")
    if status == "VERIFIED_OPENDART_DOCUMENT":
        if action_sha != economic_sha or not zipfile.is_zipfile(root / action_path):
            raise RuntimeError("OpenDART cash action/economic body mismatch")
        economic_path = action_path
        schema = "OPENDART_DOCUMENT_ZIP_V1"
    elif status == "VERIFIED_DART_VIEWER_BODY":
        viewer = viewer_by_receipt.get(receipt)
        if viewer is None:
            raise RuntimeError(f"missing verified viewer cash body: {receipt}")
        candidate = root / str(viewer.economic_viewer_path)
        if not candidate.is_file() or _sha256(candidate) != economic_sha:
            raise RuntimeError(f"viewer cash body SHA mismatch: {receipt}")
        economic_path = _relative(root, candidate)
        schema = "DART_VIEWER_HTML_V1"
    elif status == "VERIFIED_REVIEWED_SOURCE_ERRATUM":
        correction = next(
            (
                item for item in active_reviewed_corrections(root)
                if item["receipt_no"] == receipt
            ),
            None,
        )
        if correction is None or not correction.get("evidence_path"):
            raise RuntimeError(f"missing reviewed cash evidence: {receipt}")
        candidate = root / str(correction["evidence_path"])
        if not candidate.is_file() or _sha256(candidate) != economic_sha:
            raise RuntimeError(f"reviewed cash evidence SHA mismatch: {receipt}")
        economic_path = _relative(root, candidate)
        schema = "REVIEWED_PERIODIC_JSON_V1"
    else:
        raise RuntimeError(f"cash receipt is not terminally verified: {receipt}")
    return {
        "cash_source_evidence_status": status,
        "cash_action_body_path": action_path,
        "cash_action_body_sha256": action_sha,
        "cash_economic_body_path": economic_path,
        "cash_economic_body_schema": schema,
        "cash_economic_sha256": economic_sha,
    }


def _zip_payloads(path: Path) -> list[bytes]:
    try:
        with zipfile.ZipFile(path) as archive:
            return [archive.read(name) for name in archive.namelist()]
    except (OSError, zipfile.BadZipFile) as exc:
        raise RuntimeError(f"invalid official DART ZIP: {path}") from exc


def _labelled_notice(path: Path):
    labelled = corporate_actions._combined_detachment_details  # API drift sentinel
    del labelled
    notices = []
    for payload in _zip_payloads(path):
        try:
            notices.append(parse_dart_detachment_notice(payload))
        except DartDetachmentNoticeNotFound:
            # A DART ZIP may contain unrelated/correction attachments.  Only
            # exact complete notice tables are candidates; malformed partial
            # payloads neither supply fields nor poison one valid body.
            continue
        except RuntimeError as exc:
            # A correction attachment can contain only the labelled date
            # marker and therefore match no supported complete table schema.
            # Isolate only that exact incomplete-candidate condition.  A
            # malformed complete or ambiguous table still fails closed.
            if str(exc) != "DART detachment notice schema is unsupported":
                raise
            continue
    unique = list(dict.fromkeys(notices))
    if not unique:
        raise DartDetachmentNoticeNotFound(
            "official detachment notice table is absent"
        )
    if len(unique) != 1:
        raise RuntimeError("official detachment reference/reason is ambiguous")
    return unique[0]


def _read_price_row(path: Path, *, ticker: str, trade_date: date) -> tuple[float, float]:
    try:
        frame = pd.read_parquet(path, columns=["Code", "Date", "Close", "Changes"])
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"invalid marcap evidence parquet: {path}") from exc
    codes = frame["Code"].astype(str).str.zfill(6)
    dates = pd.to_datetime(frame["Date"], errors="coerce").dt.date
    matching = frame[codes.eq(ticker) & dates.eq(trade_date)]
    if len(matching) != 1:
        raise RuntimeError(
            f"marcap row missing/ambiguous: ticker={ticker} date={trade_date}"
        )
    row = matching.iloc[0]
    close = float(row["Close"])
    reference = close - float(row["Changes"])
    if not all(math.isfinite(value) and value > 0 for value in (close, reference)):
        raise RuntimeError("marcap close/reference is invalid")
    return close, reference


def _http_body(url: str) -> bytes:
    response = requests.get(
        url,
        timeout=(10, 60),
        allow_redirects=False,
        headers={"User-Agent": "TeamAlphaDataRecovery/1.0"},
    )
    if (
        response.status_code != 200
        or response.url != url
        or not response.content
    ):
        raise RuntimeError("KIND official body response is not exact HTTP 200 bytes")
    return response.content


def _kind_support_payload(
    supports: Sequence[dict[str, object]],
    *,
    reference_request_path: str,
    reference_request_sha256: str,
    component_request_path: str,
    component_request_sha256: str,
) -> dict[str, object]:
    rows = sorted(
        (dict(item) for item in supports),
        key=lambda item: (
            str(item["ticker"]),
            str(item["support_action_key"]),
            str(item["support_action_type"]),
            str(item["support_semantic_role"]),
        ),
    )
    return {
        "schema_version": KIND_SUPPORT_SCHEMA,
        "complete": True,
        "reference_request_path": reference_request_path,
        "reference_request_sha256": reference_request_sha256,
        "component_request_path": component_request_path,
        "component_request_sha256": component_request_sha256,
        "support_count": len(rows),
        "support_digest": hashlib.sha256(_canonical_bytes(rows)).hexdigest(),
        "supports": rows,
    }


def _retain_kind_request(
    root: Path,
    source: str | Path,
    *,
    verifier: Callable[..., object],
) -> tuple[bytes, str, Path]:
    source_path = Path(source).expanduser().resolve()
    raw = source_path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid KIND reviewed request JSON") from exc
    if raw != _canonical_bytes(payload):
        raise RuntimeError("KIND reviewed request is not canonical JSON bytes")
    digest = hashlib.sha256(raw).hexdigest()
    relative = KIND_REQUEST_OBJECT_ROOT / f"sha256={digest}.json"
    _atomic_write(root / relative, raw)
    verifier(
        root,
        relative_path=relative.as_posix(),
        expected_sha256=digest,
    )
    return raw, digest, relative


def _checked_download(
    fetcher: Callable[[str], bytes],
    *,
    url: str,
    expected_length: object,
    expected_sha256: object,
    label: str,
) -> bytes:
    body = fetcher(url)
    digest = hashlib.sha256(body).hexdigest()
    if (
        not isinstance(expected_length, int)
        or len(body) != expected_length
        or digest != expected_sha256
    ):
        raise RuntimeError(f"KIND {label} reviewed length/SHA mismatch")
    return body


def _store_kind_html(root: Path, object_root: Path, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    relative = object_root / f"sha256={digest}.html"
    _atomic_write(root / relative, body)
    return relative.as_posix(), digest


def download_kind_evidence(
    base: str | Path,
    reference_request_manifest: str | Path,
    component_request_manifest: str | Path,
    *,
    fetcher: Callable[[str], bytes] = _http_body,
) -> list[dict[str, object]]:
    """Download both reviewed KIND request sets into immutable local objects."""
    root = Path(base).expanduser().resolve()
    raw_reference, reference_sha, retained_reference = _retain_kind_request(
        root,
        reference_request_manifest,
        verifier=verify_kind_request_object,
    )
    raw_component, component_sha, retained_component = _retain_kind_request(
        root,
        component_request_manifest,
        verifier=verify_kind_component_request_object,
    )
    reference_payload = json.loads(raw_reference)
    component_payload = json.loads(raw_component)
    _, request_by_identity = verify_kind_request_object(
        root,
        relative_path=retained_reference.as_posix(),
        expected_sha256=reference_sha,
    )
    _, component_by_identity = verify_kind_component_request_object(
        root,
        relative_path=retained_component.as_posix(),
        expected_sha256=component_sha,
    )

    destination = root / KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    previous = destination.read_bytes() if destination.is_file() else None
    supports: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for raw in reference_payload["requests"]:
        ticker = str(raw["ticker"] or "").zfill(6)
        security_class = str(raw["security_class"] or "")
        source_url = str(raw["source_url"] or "")
        action_key, announcement, form_id = _kind_url_identity(source_url)
        identity = (ticker, action_key)
        if identity not in request_by_identity:
            raise RuntimeError("KIND request object identity changed after retention")
        if identity in seen:
            raise RuntimeError("duplicate KIND request identity")
        seen.add(identity)
        identity_url = str(raw["identity_source_url"])
        identity_body = _checked_download(
            fetcher,
            url=identity_url,
            expected_length=raw["identity_content_length"],
            expected_sha256=raw["identity_sha256"],
            label="reference identity",
        )
        body = _checked_download(
            fetcher,
            url=source_url,
            expected_length=raw["body_content_length"],
            expected_sha256=raw["body_sha256"],
            label="reference body",
        )
        identity_receipt = parse_kind_identity_receipt(identity_body)
        notice = _kind_reference_notice(body, expected_form_id=form_id)
        if (
            notice.security_class != security_class
            or identity_receipt.acceptance_no != action_key
            or identity_receipt.ticker != ticker
            or identity_receipt.selected_document_no
            != kind_url_document_no(source_url)
            or (notice.ticker is not None and notice.ticker != ticker)
            or notice.effective_date.isoformat() != raw["target_adjustment_date"]
        ):
            raise RuntimeError(
                "KIND reference identity/body semantics mismatch: "
                f"ticker={ticker} action={action_key}"
            )
        body_relative, body_sha = _store_kind_html(
            root, KIND_BODY_OBJECT_ROOT, body,
        )
        identity_relative, identity_sha = _store_kind_html(
            root, KIND_IDENTITY_OBJECT_ROOT, identity_body,
        )
        supports.append({
            "ticker": ticker,
            "issuer_name": notice.issuer_name,
            "source_form_id": form_id,
            "source_url": source_url,
            "target_cash_receipt_no": raw["target_cash_receipt_no"],
            "target_adjustment_date": raw["target_adjustment_date"],
            "identity_source_url": identity_url,
            "identity_body_path": identity_relative,
            "identity_body_content_length": len(identity_body),
            "identity_body_sha256": identity_sha,
            "contents_source_url": None,
            "contents_body_path": None,
            "contents_body_content_length": None,
            "contents_body_sha256": None,
            "terminal_acceptance_no": action_key,
            "support_action_source": "KRX_KIND",
            "support_action_key": action_key,
            "support_action_type": notice.action_type,
            "support_semantic_role": "CORROBORATION",
            "support_action_body_path": body_relative,
            "support_action_body_content_length": len(body),
            "support_action_body_sha256": body_sha,
            "support_announcement_date": announcement.isoformat(),
            "support_ex_date": notice.effective_date.isoformat(),
            "support_record_date": None,
            "support_ratio_numerator": None,
            "support_ratio_denominator": None,
            "support_entitlement_security_class": security_class,
            "support_distributed_security_class": None,
            "support_expected_price_factor": None,
            "support_reference_price": notice.reference_price,
            "support_reason": notice.reason,
            "support_report_name": (
                KIND_REFERENCE_REPORT_NAME_99311
                if form_id == "99311"
                else KIND_REFERENCE_REPORT_NAME_99302
                if form_id == "99302"
                else KIND_REFERENCE_REPORT_NAME_70767
            ),
            "support_action_scope": "ISSUER",
        })

    component_seen: set[tuple[str, str]] = set()
    for raw in component_payload["components"]:
        ticker = str(raw["ticker"] or "").zfill(6)
        key = str(raw["component_action_key"])
        request_identity = (ticker, key)
        if request_identity not in component_by_identity:
            raise RuntimeError("KIND component identity changed after retention")
        if request_identity in component_seen:
            raise RuntimeError("duplicate KIND component identity")
        component_seen.add(request_identity)
        main_url = str(raw["main_url"])
        contents_url = str(raw["contents_url"])
        body_url = str(raw["body_url"])
        main = _checked_download(
            fetcher,
            url=main_url,
            expected_length=raw["main_content_length"],
            expected_sha256=raw["main_sha256"],
            label="component identity",
        )
        contents = _checked_download(
            fetcher,
            url=contents_url,
            expected_length=raw["contents_content_length"],
            expected_sha256=raw["contents_sha256"],
            label="component contents",
        )
        body = _checked_download(
            fetcher,
            url=body_url,
            expected_length=raw["body_content_length"],
            expected_sha256=raw["body_sha256"],
            label="component body",
        )
        identity_receipt = parse_kind_identity_receipt(main)
        component = (
            parse_kind_stock_dividend_component(body)
            if raw["component_action_type"] == "stock_dividend"
            else parse_kind_paid_increase_component(body)
        )
        terminal = str(raw["terminal_acceptance_no"])
        if (
            kind_identity_url_acceptance(main_url) != terminal
            or identity_receipt.acceptance_no != terminal
            or identity_receipt.ticker != ticker
            or corporate_actions._compact(identity_receipt.issuer_name)
            != corporate_actions._compact(raw["asset_name"])
            or identity_receipt.selected_document_no != key
            or kind_contents_url_document_no(contents_url) != key
            or parse_kind_contents_body_url(contents) != body_url
            or kind_component_url_identity(body_url)[2] != key
            or component.decision_date.isoformat() != raw["announcement_date"]
            or component.record_date.isoformat() != raw["record_date"]
            or not math.isclose(
                component.ratio_numerator,
                float(raw["ratio_numerator"]),
                rel_tol=0,
                abs_tol=1e-12,
            )
            or not math.isclose(
                component.ratio_denominator,
                float(raw["ratio_denominator"]),
                rel_tol=0,
                abs_tol=1e-12,
            )
            or component.entitlement_security_class
            != raw["entitlement_security_class"]
            or component.distributed_security_class
            != raw["distributed_security_class"]
            or component.report_name != raw["report_name"]
        ):
            raise RuntimeError("KIND component official evidence chain mismatch")
        main_relative, main_sha = _store_kind_html(
            root, KIND_IDENTITY_OBJECT_ROOT, main,
        )
        contents_relative, contents_sha = _store_kind_html(
            root, KIND_CONTENTS_OBJECT_ROOT, contents,
        )
        body_relative, body_sha = _store_kind_html(
            root, KIND_BODY_OBJECT_ROOT, body,
        )
        supports.append({
            "ticker": ticker,
            "issuer_name": identity_receipt.issuer_name,
            "source_form_id": raw["source_form_code"],
            "source_url": body_url,
            "target_cash_receipt_no": raw["target_cash_receipt_no"],
            "target_adjustment_date": raw["adjustment_date"],
            "identity_source_url": main_url,
            "identity_body_path": main_relative,
            "identity_body_content_length": len(main),
            "identity_body_sha256": main_sha,
            "contents_source_url": contents_url,
            "contents_body_path": contents_relative,
            "contents_body_content_length": len(contents),
            "contents_body_sha256": contents_sha,
            "terminal_acceptance_no": terminal,
            "support_action_source": "KRX_KIND",
            "support_action_key": key,
            "support_action_type": raw["component_action_type"],
            "support_semantic_role": "ADJUSTMENT_COMPONENT",
            "support_action_body_path": body_relative,
            "support_action_body_content_length": len(body),
            "support_action_body_sha256": body_sha,
            "support_announcement_date": raw["terminal_announcement_date"],
            "support_ex_date": None,
            "support_record_date": raw["record_date"],
            "support_ratio_numerator": raw["ratio_numerator"],
            "support_ratio_denominator": raw["ratio_denominator"],
            "support_entitlement_security_class": raw[
                "entitlement_security_class"
            ],
            "support_distributed_security_class": raw[
                "distributed_security_class"
            ],
            "support_expected_price_factor": None,
            "support_reference_price": None,
            "support_reason": None,
            "support_report_name": raw["report_name"],
            "support_action_scope": "ISSUER",
        })
    payload = _kind_support_payload(
        supports,
        reference_request_path=retained_reference.as_posix(),
        reference_request_sha256=reference_sha,
        component_request_path=retained_component.as_posix(),
        component_request_sha256=component_sha,
    )
    try:
        _atomic_write(destination, _canonical_bytes(payload))
        verified = _kind_supports(root)
        verified_again = _kind_supports(root)
        if verified != verified_again:
            raise RuntimeError("KIND support verifier roundtrip changed")
        return verified_again
    except Exception:
        if previous is None:
            destination.unlink(missing_ok=True)
        else:
            _atomic_write(destination, previous)
        raise


def _kind_supports(root: Path) -> list[dict[str, object]]:
    return verify_kind_support_manifest(root)


def _semantic_group(
    *, ticker: str, record_date: date, kind: str, ratio: float,
) -> str:
    rendered_ratio = format(float(ratio), ".12g")
    return f"{ticker}|{record_date.isoformat()}|{kind}|{rendered_ratio}"


def _support_row(
    event: dict[str, object],
    *,
    evidence_key: str,
    target_cash_receipt_no: str,
    target_adjustment_date: date,
    body_path: str,
    body_sha: str,
    groups: Sequence[str],
    role: str,
    entitlement: str | None = None,
    distributed: str | None = None,
    reference: float | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    row = {
        "evidence_key": evidence_key,
        "support_action_source": str(event["source"]),
        "support_action_key": str(event["rcept_no"]),
        "support_action_type": str(event["event_type"]),
        "target_cash_receipt_no": target_cash_receipt_no,
        "target_adjustment_date": target_adjustment_date,
        "support_action_body_path": body_path,
        "support_action_body_sha256": body_sha,
        "support_announcement_date": event.get("announcement_date"),
        "support_ex_date": event.get("effective_date"),
        "support_record_date": event.get("record_date"),
        "support_ratio_numerator": event.get("ratio_numerator"),
        "support_ratio_denominator": event.get("ratio_denominator"),
        "support_entitlement_security_class": entitlement,
        "support_distributed_security_class": distributed,
        "support_expected_price_factor": event.get("expected_factor"),
        "support_reference_price": reference,
        "support_reason": reason,
        "support_report_name": event.get("report_name"),
        "support_action_scope": event.get("action_scope"),
        "support_semantic_group_keys": json.dumps(
            sorted(groups), ensure_ascii=False, separators=(",", ":"),
        ),
        "support_semantic_role": role,
    }
    row["manifest_support_row_sha256"] = manifest_support_row_sha256(row)
    return row


def _compact_report(value: object) -> str:
    return corporate_actions._compact(value)


def _is_related_company_report(value: object) -> bool:
    compact = _compact_report(value)
    return "자회사의주요경영사항" in compact or "종속회사의주요경영사항" in compact


def _verified_family_terminal_event(
    root: Path,
    events: Sequence[dict[str, object]],
    family: SupportActionFamilyEntry,
) -> dict[str, object]:
    """Bind one verified official family to its exact prepared event/body."""
    receipt = family.terminal_economic_receipt_no
    source_rows = [
        source for source in family.sources if source.receipt_no == receipt
    ]
    if len(source_rows) != 1:
        raise RuntimeError("verified support family terminal source is ambiguous")
    source = source_rows[0]
    if family.action_type == "stock_dividend":
        disclosure_candidates = [
            event for event in events
            if str(event.get("identifier") or "").zfill(6) == family.ticker
            and event.get("event_type") == family.action_type
            and event.get("source") == "DART_DISCLOSURE"
            and str(event.get("rcept_no") or "") == receipt
        ]
        if (
            len(disclosure_candidates) == 1
            and disclosure_candidates[0].get("record_date") is not None
            and disclosure_candidates[0].get("ratio_numerator") is not None
        ):
            expected_source = "DART_DISCLOSURE"
            candidates = disclosure_candidates
        else:
            expected_source = "DART_VIEWER"
            candidates = [_viewer_stock_dividend_family_event(root, family)]
    elif source.structured_path is not None:
        expected_source = "DART_STRUCTURED"
        candidates = list(events)
    else:
        expected_source = "DART_VIEWER"
        candidates = [_viewer_bonus_family_event(root, family)]
    matches = [
        event for event in candidates
        if str(event.get("identifier") or "").zfill(6) == family.ticker
        and event.get("event_type") == family.action_type
        and event.get("source") == expected_source
        and str(event.get("rcept_no") or "") == receipt
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "verified support family lacks one exact prepared terminal event: "
            f"ticker={family.ticker} action={family.action_type} "
            f"receipt={receipt} matches={len(matches)}"
        )
    event = matches[0]
    if (
        event.get("action_scope") != "ISSUER"
        or str(event.get("report_name") or "") != source.report_name
        or event.get("announcement_date")
        != _date(source.receipt_date, field="family receipt date")
    ):
        raise RuntimeError(
            "prepared support event/fresh family row parity failed: "
            f"receipt={receipt}"
        )
    numerator = event.get("ratio_numerator")
    denominator = event.get("ratio_denominator")
    if (
        family.terminal_ratio is None
        or numerator is None
        or denominator is None
        or float(denominator) <= 0
        or not math.isclose(
            float(numerator) / float(denominator),
            float(family.terminal_ratio),
            rel_tol=0,
            abs_tol=5e-13,
        )
    ):
        raise RuntimeError(
            "prepared support event/family terminal ratio parity failed: "
            f"receipt={receipt}"
        )
    body_path, body_sha = _matching_body(root, event)
    if family.action_type == "bonus_issue":
        if source.structured_path is not None:
            if (
                source.structured_sha256 is None
                or body_path != source.structured_path
                or body_sha != source.structured_sha256
            ):
                raise RuntimeError(
                    "prepared bonus event/family structured-body parity failed: "
                    f"receipt={receipt}"
                )
        elif (
            event.get("source") != "DART_VIEWER"
            or body_path != source.body_path
            or body_sha != source.body_sha256
        ):
            raise RuntimeError(
                "prepared bonus event/family viewer-body parity failed: "
                f"receipt={receipt}"
            )
    elif event.get("source") == "DART_DISCLOSURE":
        expected_path = (
            f"corporate_actions/dart/documents/year={receipt[:4]}/"
            f"corp={family.ticker}/rcept={receipt}.zip"
        )
        if body_path != expected_path:
            raise RuntimeError(
                "prepared stock-dividend event lacks its exact official ZIP: "
                f"receipt={receipt} path={body_path}"
            )
    elif (
        event.get("source") != "DART_VIEWER"
        or body_path != source.body_path
        or body_sha != source.body_sha256
    ):
        raise RuntimeError(
            "prepared stock-dividend event/family viewer-body parity failed: "
            f"receipt={receipt}"
        )
    return event


def _viewer_bonus_family_event(
    root: Path,
    family: SupportActionFamilyEntry,
) -> dict[str, object]:
    """Materialize an exact viewer-backed bonus event when OpenDART omitted it.

    This is not a synthetic structured API row.  The distinct ``DART_VIEWER``
    source preserves the official transport identity, while the verified
    support-family manifest binds the issuer, selector order, terminal body,
    ratio and record-date semantics to content-addressed bytes.
    """
    if family.action_type != "bonus_issue":
        raise RuntimeError("viewer fallback is only valid for bonus issues")
    receipt = family.terminal_economic_receipt_no
    sources = [
        source for source in family.sources if source.receipt_no == receipt
    ]
    if len(sources) != 1:
        raise RuntimeError("viewer bonus family terminal source is ambiguous")
    source = sources[0]
    if source.structured_path is not None:
        raise RuntimeError("structured bonus family cannot use viewer fallback")
    relative = Path(source.body_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError("viewer bonus family body path is unsafe")
    body_path = (root / relative).resolve()
    try:
        body_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("viewer bonus family body escaped snapshot") from exc
    if (
        not body_path.is_file()
        or body_path.stat().st_size != source.body_content_length
        or _sha256(body_path) != source.body_sha256
    ):
        raise RuntimeError("viewer bonus family body changed")
    terms = bonus_issue_common_terms_from_body(body_path.read_bytes())
    if terms is None:
        raise RuntimeError("viewer bonus family lacks exact common-share terms")
    ratio = float(terms.common_ratio)
    if ratio <= 0:
        raise RuntimeError("viewer bonus family ratio is not positive")
    if family.terminal_ratio is None or not math.isclose(
        ratio, float(family.terminal_ratio), rel_tol=0, abs_tol=5e-13,
    ):
        raise RuntimeError("viewer bonus family terminal ratio parity failed")
    return {
        "identifier": family.ticker,
        "event_type": "bonus_issue",
        "announcement_date": _date(
            source.receipt_date, field="viewer bonus receipt date",
        ),
        "effective_date": _date(
            terms.record_date, field="viewer bonus record date",
        ),
        "match_window_days": 7,
        "expected_factor": 1.0 / (1.0 + ratio),
        "record_date": None,
        "ratio_numerator": ratio,
        "ratio_denominator": 1.0,
        "rcept_no": receipt,
        "report_name": source.report_name,
        "action_scope": "ISSUER",
        "source_evidence_status": "VERIFIED_DART_VIEWER_BODY",
        "source_body_sha256": source.body_sha256,
        "source": "DART_VIEWER",
        "source_file": str(body_path),
    }


def _viewer_stock_dividend_family_event(
    root: Path,
    family: SupportActionFamilyEntry,
) -> dict[str, object]:
    """Materialize exact viewer-backed common stock-dividend economics."""
    if family.action_type != "stock_dividend":
        raise RuntimeError("viewer stock fallback requires a stock-dividend family")
    if (
        family.terminal_status != "ACTIVE"
        or not family.terminal_admissible
        or family.terminal_ratio is None
    ):
        raise RuntimeError("viewer stock-dividend family is not active/admissible")
    receipt = family.terminal_economic_receipt_no
    sources = [
        source for source in family.sources if source.receipt_no == receipt
    ]
    if len(sources) != 1:
        raise RuntimeError("viewer stock-dividend terminal source is ambiguous")
    source = sources[0]
    if source.structured_path is not None:
        raise RuntimeError("viewer stock-dividend source cannot be structured")
    if corporate_actions._compact(source.report_name) not in {
        "주식배당결정", "기재정정주식배당결정",
    }:
        raise RuntimeError("viewer stock-dividend report-name contract changed")
    relative = Path(source.body_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError("viewer stock-dividend family body path is unsafe")
    body_path = (root / relative).resolve()
    try:
        body_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("viewer stock-dividend family body escaped snapshot") from exc
    if (
        not body_path.is_file()
        or body_path.stat().st_size != source.body_content_length
        or _sha256(body_path) != source.body_sha256
    ):
        raise RuntimeError("viewer stock-dividend family body changed")
    terms = stock_dividend_common_terms_from_body(body_path.read_bytes())
    if terms is None or float(terms.common_ratio) <= 0:
        raise RuntimeError("viewer stock-dividend family lacks positive common terms")
    ratio = float(terms.common_ratio)
    if not math.isclose(
        ratio, float(family.terminal_ratio), rel_tol=0, abs_tol=5e-13,
    ):
        raise RuntimeError("viewer stock-dividend family terminal ratio parity failed")
    record_date = _date(terms.record_date, field="viewer stock-dividend record date")
    return {
        "identifier": family.ticker,
        "event_type": "stock_dividend",
        "announcement_date": _date(
            source.receipt_date, field="viewer stock-dividend receipt date",
        ),
        "effective_date": None,
        "match_window_days": 0,
        "expected_factor": None,
        "record_date": record_date,
        "ratio_numerator": ratio,
        "ratio_denominator": 1.0,
        "rcept_no": receipt,
        "report_name": source.report_name,
        "action_scope": "ISSUER",
        "source_evidence_status": "VERIFIED_DART_VIEWER_BODY",
        "source_body_sha256": source.body_sha256,
        "source": "DART_VIEWER",
        "source_file": str(body_path),
    }


def _prepared_family_terminal_candidates(
    root: Path,
    events: Sequence[dict[str, object]],
    family: SupportActionFamilyEntry,
) -> list[dict[str, object]]:
    """Locate a family's economic receipt without validating unrelated history.

    The official family manifest is authoritative for the economic receipt.
    This first pass deliberately performs only identity lookup: strict body,
    status and ratio checks belong after the family has been shown to overlap
    the parent event.  Otherwise one withdrawn or incomplete historical family
    for the same issuer could block an unrelated current adjustment.
    """
    source_rows = [
        source for source in family.sources
        if source.receipt_no == family.terminal_economic_receipt_no
    ]
    if len(source_rows) != 1:
        raise RuntimeError("support family terminal source is ambiguous")
    if source_rows[0].structured_path is None:
        if family.action_type == "bonus_issue":
            return [_viewer_bonus_family_event(root, family)]
        if family.action_type == "stock_dividend":
            disclosure = [
                event for event in events
                if str(event.get("identifier") or "").zfill(6)
                == family.ticker
                and event.get("event_type") == "stock_dividend"
                and event.get("source") == "DART_DISCLOSURE"
                and str(event.get("rcept_no") or "")
                == family.terminal_economic_receipt_no
                and not _is_related_company_report(event.get("report_name"))
            ]
            if not family.terminal_admissible or any(
                event.get("record_date") is not None
                and event.get("ratio_numerator") is not None
                and event.get("ratio_denominator") is not None
                for event in disclosure
            ):
                return disclosure
            return [_viewer_stock_dividend_family_event(root, family)]
    expected_source = (
        "DART_DISCLOSURE"
        if family.action_type == "stock_dividend"
        else "DART_STRUCTURED"
    )
    return [
        event for event in events
        if str(event.get("identifier") or "").zfill(6) == family.ticker
        and event.get("event_type") == family.action_type
        and event.get("source") == expected_source
        and str(event.get("rcept_no") or "")
        == family.terminal_economic_receipt_no
        and not _is_related_company_report(event.get("report_name"))
    ]


def _component_supports(
    root: Path,
    events: list[dict[str, object]],
    kind_supports: list[dict[str, object]],
    support_families: Sequence[SupportActionFamilyEntry],
    *,
    ticker: str,
    record_date: date,
    adjustment_date: date,
    evidence_key: str,
    cash_receipt_no: str,
) -> tuple[list[dict[str, object]], dict[str, list[str]], dict[str, object]]:
    issuer = [
        row for row in events
        if str(row.get("identifier") or "").zfill(6) == ticker
        and not _is_related_company_report(row.get("report_name"))
    ]
    stock_events = [
        row for row in issuer
        if row.get("source") == "DART_DISCLOSURE"
        and row.get("event_type") == "stock_dividend"
        and row.get("record_date") == record_date
    ]
    stock_entries = [
        family for family in support_families
        if family.ticker == ticker and family.action_type == "stock_dividend"
    ]
    covered_stock_receipts = {
        receipt for family in stock_entries
        for receipt in family.ordered_family_receipts
    }
    uncovered_stock = sorted({
        str(event.get("rcept_no") or "") for event in stock_events
        if str(event.get("rcept_no") or "") not in covered_stock_receipts
    })
    if uncovered_stock:
        raise RuntimeError(
            "prepared stock-dividend rows are absent from verified official "
            f"families: {uncovered_stock}"
        )
    matching_kind = [
        item for item in kind_supports
        if item["ticker"] == ticker
        and item["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
        and item["support_action_type"] == "stock_dividend"
        and item["target_cash_receipt_no"] == cash_receipt_no
        and _date(
            item.get("target_adjustment_date"),
            field="KIND target adjustment date",
        ) == adjustment_date
        and _date(item.get("support_record_date"), field="KIND record date")
        == record_date
    ]
    if len(matching_kind) > 1:
        raise RuntimeError("KIND stock support is ambiguous")
    matching_paid = [
        item for item in kind_supports
        if item["ticker"] == ticker
        and item["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
        and item["support_action_type"] == "paid_increase"
        and item["target_cash_receipt_no"] == cash_receipt_no
        and _date(
            item.get("target_adjustment_date"),
            field="KIND paid-rights target date",
        ) == adjustment_date
    ]
    if len(matching_paid) > 1:
        raise RuntimeError("KIND paid-rights support is ambiguous")
    matching_stock_families: list[
        tuple[SupportActionFamilyEntry, dict[str, object]]
    ] = []
    for family in stock_entries:
        preliminary = [
            event for event in _prepared_family_terminal_candidates(
                root, events, family,
            )
            if (
                event.get("record_date") == record_date
                if event.get("source") != "DART_VIEWER"
                else event.get("record_date") is not None
                and adjustment_date < event["record_date"]
                <= adjustment_date + timedelta(days=7)
            )
        ]
        if not preliminary:
            continue
        if len(preliminary) != 1:
            raise RuntimeError("ambiguous prepared stock-dividend family terminal")
        if not family.terminal_admissible:
            # A cross-class DART decision is deliberately not an ordinary
            # same-class component.  It may coexist with one independently
            # reviewed KIND component that binds COMMON_AND_PREFERRED holders
            # to NEW_PREFERRED shares (the CJ case).  Withdrawal, cancellation,
            # denial and zero remain hard failures even when a notice exists.
            if (
                family.terminal_status == "CROSS_CLASS_DISTRIBUTION"
                and len(matching_kind) == 1
            ):
                continue
            raise RuntimeError(
                "terminal stock-dividend family is inadmissible: "
                f"root={family.root_receipt_no} status={family.terminal_status}"
            )
        terminal = (
            preliminary[0]
            if preliminary[0].get("source") == "DART_VIEWER"
            else _verified_family_terminal_event(root, events, family)
        )
        if str(terminal.get("rcept_no") or "") != str(
            preliminary[0].get("rcept_no") or ""
        ):
            raise RuntimeError("stock-dividend family terminal identity changed")
        matching_stock_families.append((family, terminal))
    if len(matching_stock_families) > 1:
        raise RuntimeError("ambiguous verified stock-dividend families")
    components: list[dict[str, object]] = []
    groups_by_kind: dict[str, list[str]] = {
        "stock": [], "bonus": [], "paid": [],
    }
    diagnostic: dict[str, object] = {
        "terminal_stock_receipts": [],
        "component_receipts": [],
        # DART's stock-dividend ratio is an entitlement per eligible share.
        # It is not the KRX price-dilution ratio: treasury shares are excluded
        # from eligibility but remain in the issued-share denominator used by
        # the exchange.  Keep the entitlement as provenance and never turn it
        # into a naive ``1 / (1 + ratio)`` price factor.
        "component_entitlement_ratios": [],
    }
    if matching_stock_families:
        family, terminal = matching_stock_families[0]
        ratio = float(family.terminal_ratio)
        stock_record_date = _date(
            terminal["record_date"], field="stock-dividend record date",
        )
        group = _semantic_group(
            ticker=ticker,
            record_date=stock_record_date,
            kind="STOCK_DIVIDEND",
            ratio=ratio,
        )
        body_path, body_sha = _matching_body(root, terminal)
        components.append(_support_row(
            terminal,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=body_path,
            body_sha=body_sha,
            groups=[group],
            role="ADJUSTMENT_COMPONENT",
            entitlement="COMMON",
            distributed="COMMON",
        ))
        groups_by_kind["stock"].append(group)
        diagnostic["terminal_stock_receipts"] = [str(terminal["rcept_no"])]
        diagnostic["stock_family_receipts"] = list(
            family.ordered_family_receipts
        )
        diagnostic["stock_family_root_receipt"] = family.root_receipt_no
        diagnostic["stock_family_terminal_receipt"] = (
            family.terminal_receipt_no
        )
        diagnostic["stock_family_bind_digest"] = family.fresh_row_bind_digest
        diagnostic["component_entitlement_ratios"].append({
            "receipt": str(terminal["rcept_no"]),
            "action_type": "stock_dividend",
            "ratio": ratio,
            "semantics": "PER_ELIGIBLE_SHARE_ENTITLEMENT",
        })

    if matching_kind:
        if matching_stock_families:
            raise RuntimeError("KIND/DART stock support is ambiguous")
        item = matching_kind[0]
        if item["support_report_name"] != KIND_COMPONENT_REPORT_NAME_61474:
            raise RuntimeError("KIND component report-name contract changed")
        ratio = float(item["support_ratio_numerator"])
        denominator = float(item["support_ratio_denominator"])
        group = _semantic_group(
            ticker=ticker,
            record_date=record_date,
            kind="STOCK_DIVIDEND",
            ratio=ratio / denominator,
        )
        event = {
            "source": "KRX_KIND",
            "rcept_no": item["support_action_key"],
            "event_type": "stock_dividend",
            "announcement_date": _date(
                item["support_announcement_date"], field="KIND announcement"
            ),
            "effective_date": None,
            "record_date": record_date,
            "ratio_numerator": ratio,
            "ratio_denominator": denominator,
            "expected_factor": item.get("support_expected_price_factor"),
            "report_name": item["support_report_name"],
            "action_scope": "ISSUER",
        }
        components.append(_support_row(
            event,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=str(item["support_action_body_path"]),
            body_sha=str(item["support_action_body_sha256"]),
            groups=[group],
            role="ADJUSTMENT_COMPONENT",
            entitlement=str(item["support_entitlement_security_class"]),
            distributed=str(item["support_distributed_security_class"]),
        ))
        groups_by_kind["stock"].append(group)
        diagnostic["terminal_stock_receipts"] = [str(item["support_action_key"])]
        diagnostic["component_entitlement_ratios"].append({
            "receipt": str(item["support_action_key"]),
            "action_type": "stock_dividend",
            "ratio": ratio / denominator,
            "semantics": "CROSS_CLASS_ENTITLEMENT_NOT_PRICE_DILUTION",
        })

    if matching_paid:
        if matching_stock_families or matching_kind:
            raise RuntimeError("paid-rights/stock support is ambiguous")
        item = matching_paid[0]
        if (
            ticker != "183190"
            or cash_receipt_no != "20180226800579"
            or adjustment_date != date(2017, 12, 27)
            or item["support_report_name"] != KIND_COMPONENT_REPORT_NAME_11306
        ):
            raise RuntimeError("paid-rights component is outside the closed set")
        ratio = float(item["support_ratio_numerator"])
        denominator = float(item["support_ratio_denominator"])
        paid_record_date = _date(
            item["support_record_date"], field="paid-rights record date",
        )
        group = _semantic_group(
            ticker=ticker,
            record_date=paid_record_date,
            kind="PAID_INCREASE",
            ratio=ratio / denominator,
        )
        event = {
            "source": "KRX_KIND",
            "rcept_no": item["support_action_key"],
            "event_type": "paid_increase",
            "announcement_date": _date(
                item["support_announcement_date"], field="paid-rights announcement",
            ),
            "effective_date": None,
            "record_date": paid_record_date,
            "ratio_numerator": ratio,
            "ratio_denominator": denominator,
            "expected_factor": None,
            "report_name": item["support_report_name"],
            "action_scope": "ISSUER",
        }
        components.append(_support_row(
            event,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=str(item["support_action_body_path"]),
            body_sha=str(item["support_action_body_sha256"]),
            groups=[group],
            role="ADJUSTMENT_COMPONENT",
            entitlement="COMMON",
            distributed="COMMON",
        ))
        groups_by_kind["paid"].append(group)
        diagnostic["component_entitlement_ratios"].append({
            "receipt": str(item["support_action_key"]),
            "action_type": "paid_increase",
            "ratio": ratio / denominator,
            "semantics": "PAID_RIGHTS_ENTITLEMENT_NOT_PRICE_FACTOR",
        })

    bonus_events = [
        row for row in issuer
        if row.get("source") == "DART_STRUCTURED"
        and row.get("event_type") == "bonus_issue"
        and row.get("effective_date") is not None
        and abs((row["effective_date"] - adjustment_date).days)
        <= int(row.get("match_window_days") or 0)
    ]
    bonus_entries = [
        family for family in support_families
        if family.ticker == ticker and family.action_type == "bonus_issue"
    ]
    covered_bonus_receipts = {
        receipt for family in bonus_entries
        for receipt in family.ordered_family_receipts
    }
    uncovered_bonus = sorted({
        str(event.get("rcept_no") or "") for event in bonus_events
        if str(event.get("rcept_no") or "") not in covered_bonus_receipts
    })
    if uncovered_bonus:
        raise RuntimeError(
            "prepared bonus rows are absent from verified official families: "
            f"{uncovered_bonus}"
        )
    matching_bonus_families: list[
        tuple[SupportActionFamilyEntry, dict[str, object]]
    ] = []
    for family in bonus_entries:
        preliminary = []
        for event in _prepared_family_terminal_candidates(root, events, family):
            effective = event.get("effective_date")
            if effective is not None and abs(
                (effective - adjustment_date).days
            ) <= int(event.get("match_window_days") or 0):
                preliminary.append(event)
        if not preliminary:
            continue
        if len(preliminary) != 1:
            raise RuntimeError("ambiguous prepared bonus-issue family terminal")
        if not family.terminal_admissible:
            raise RuntimeError(
                "terminal bonus-issue family is inadmissible: "
                f"root={family.root_receipt_no} status={family.terminal_status}"
            )
        terminal = _verified_family_terminal_event(root, events, family)
        if (
            terminal.get("source") != "DART_VIEWER"
            and terminal is not preliminary[0]
        ) or (
            terminal.get("source") == "DART_VIEWER"
            and terminal != preliminary[0]
        ):
            raise RuntimeError("bonus-issue family terminal identity changed")
        matching_bonus_families.append((family, terminal))
    if len(matching_bonus_families) > 1:
        raise RuntimeError("ambiguous verified bonus-issue families")
    if matching_bonus_families:
        family, terminal = matching_bonus_families[0]
        ratio = float(family.terminal_ratio)
        group = _semantic_group(
            ticker=ticker,
            record_date=terminal["effective_date"],
            kind="BONUS_ISSUE",
            ratio=ratio,
        )
        body_path, body_sha = _matching_body(root, terminal)
        components.append(_support_row(
            terminal,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=body_path,
            body_sha=body_sha,
            groups=[group],
            role="ADJUSTMENT_COMPONENT",
            entitlement="COMMON",
            distributed="COMMON",
        ))
        groups_by_kind["bonus"].append(group)
        diagnostic["bonus_family_receipts"] = list(
            family.ordered_family_receipts
        )
        diagnostic["bonus_family_root_receipt"] = family.root_receipt_no
        diagnostic["bonus_family_terminal_receipt"] = (
            family.terminal_receipt_no
        )
        diagnostic["bonus_family_bind_digest"] = family.fresh_row_bind_digest
        diagnostic["component_entitlement_ratios"].append({
            "receipt": str(terminal["rcept_no"]),
            "action_type": "bonus_issue",
            "ratio": ratio,
            "semantics": "PER_ELIGIBLE_SHARE_ENTITLEMENT",
        })
    if not components:
        raise RuntimeError("no official non-cash adjustment component")
    diagnostic["component_receipts"] = [
        str(item["support_action_key"]) for item in components
    ]
    return components, groups_by_kind, diagnostic


def _corroborations(
    root: Path,
    events: list[dict[str, object]],
    kind_supports: Sequence[dict[str, object]] = (),
    *,
    ticker: str,
    adjustment_date: date,
    raw_reference: float,
    evidence_key: str,
    cash_receipt_no: str,
    groups_by_kind: dict[str, list[str]],
    asset_name: str | None = None,
) -> tuple[list[dict[str, object]], set[str]]:
    candidates = [
        row for row in events
        if str(row.get("identifier") or "").zfill(6) == ticker
        and row.get("source") == "DART_DISCLOSURE"
        and row.get("action_scope") == "ISSUER"
        and row.get("event_type") in {
            "ex_dividend", "rights_detachment", "combined_detachment",
        }
        and row.get("effective_date") == adjustment_date
    ]
    selected: list[dict[str, object]] = []
    corroborated: set[str] = set()
    compact_asset = corporate_actions._compact(asset_name)
    expected_security_class = (
        "PREFERRED"
        if re.search(r"(?:[0-9]+우|우B|우선주|우)$", compact_asset)
        else "COMMON"
    )
    for event in sorted(candidates, key=lambda item: str(item["rcept_no"])):
        body_path, body_sha = _matching_body(root, event)
        try:
            notice = _labelled_notice(root / body_path)
        except DartDetachmentNoticeNotFound:
            # Correction and attachment disclosures can share the target date
            # without containing the exchange notice table.  They are not
            # candidates unless one complete exact table parses.
            continue
        if (
            notice.ticker != ticker
            or notice.security_class != expected_security_class
            or notice.effective_date != adjustment_date
            or notice.action_type != event["event_type"]
        ):
            raise RuntimeError("official detachment notice identity mismatch")
        reference = notice.reference_price
        reason = notice.reason
        if not math.isclose(reference, raw_reference, rel_tol=0, abs_tol=1e-8):
            raise RuntimeError("official notice/KRX reference price mismatch")
        compact = corporate_actions._compact(reason)
        groups: list[str] = []
        event_type = event["event_type"]
        if event_type == "ex_dividend" and "주식배당" in compact:
            groups.extend(groups_by_kind["stock"])
        elif event_type == "rights_detachment" and "무상증자" in compact:
            groups.extend(groups_by_kind["bonus"])
        elif event_type == "rights_detachment" and "유상증자" in compact:
            groups.extend(groups_by_kind["paid"])
        elif event_type == "combined_detachment":
            if "배당" in compact:
                groups.extend(groups_by_kind["stock"])
            if "무상증자" in compact:
                groups.extend(groups_by_kind["bonus"])
        if not groups:
            # Plain/cash-only notices are retained by Bronze but can never be
            # promoted into this non-cash scale evidence graph.
            continue
        unique_groups = sorted(set(groups))
        if event_type == "combined_detachment":
            group_kinds = frozenset(group.split("|")[2] for group in unique_groups)
            if REVIEWED_COMBINED_NOTICE_GROUP_KINDS.get(
                str(event["rcept_no"])
            ) != group_kinds:
                raise RuntimeError(
                    "combined detachment notice is outside the reviewed "
                    "receipt/group contract"
                )
        selected.append(_support_row(
            event,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=body_path,
            body_sha=body_sha,
            groups=unique_groups,
            role="CORROBORATION",
            reference=reference,
            reason=reason,
        ))
        corroborated.update(unique_groups)

    dart_group_counts: dict[str, int] = {}
    for row in selected:
        for group in json.loads(row["support_semantic_group_keys"]):
            dart_group_counts[group] = dart_group_counts.get(group, 0) + 1
    if any(count != 1 for count in dart_group_counts.values()):
        raise RuntimeError("official detachment notice is ambiguous for a component")

    kind_candidates = [
        item for item in kind_supports
        if item["ticker"] == ticker
        and item["support_semantic_role"] == "CORROBORATION"
        and item["target_cash_receipt_no"] == cash_receipt_no
        and _date(
            item.get("target_adjustment_date"), field="KIND target adjustment date",
        ) == adjustment_date
        and _date(item.get("support_ex_date"), field="KIND ex date")
        == adjustment_date
    ]
    if kind_candidates and not compact_asset:
        raise RuntimeError("KIND corroboration requires exact parent asset name")
    exact_kind: list[dict[str, object]] = []
    for item in kind_candidates:
        if item["support_report_name"] not in {
            KIND_REFERENCE_REPORT_NAME_99311,
            KIND_REFERENCE_REPORT_NAME_99302,
            KIND_REFERENCE_REPORT_NAME_70767,
        }:
            raise RuntimeError("KIND reference report-name contract changed")
        if (
            item.get("support_entitlement_security_class")
            != expected_security_class
        ):
            raise RuntimeError(
                "KIND corroboration security class differs from parent"
            )
        if not math.isclose(
            float(item["support_reference_price"]), raw_reference,
            rel_tol=0, abs_tol=1e-8,
        ):
            raise RuntimeError("official KIND/KRX reference price mismatch")
        compact = corporate_actions._compact(item.get("support_reason"))
        groups: list[str] = []
        event_type = str(item["support_action_type"])
        if event_type == "ex_dividend" and "주식배당" in compact:
            groups.extend(groups_by_kind["stock"])
        elif event_type == "rights_detachment" and "무상증자" in compact:
            groups.extend(groups_by_kind["bonus"])
        elif event_type == "rights_detachment" and "유상증자" in compact:
            groups.extend(groups_by_kind["paid"])
        elif event_type == "combined_detachment":
            if "주식배당" in compact:
                groups.extend(groups_by_kind["stock"])
            if "무상증자" in compact:
                groups.extend(groups_by_kind["bonus"])
        if not groups:
            raise RuntimeError("KIND notice does not corroborate a component group")
        event = {
            "source": "KRX_KIND",
            "rcept_no": item["support_action_key"],
            "event_type": event_type,
            "announcement_date": _date(
                item["support_announcement_date"], field="KIND announcement",
            ),
            "effective_date": adjustment_date,
            "record_date": None,
            "ratio_numerator": None,
            "ratio_denominator": None,
            "expected_factor": None,
            "report_name": item["support_report_name"],
            "action_scope": "ISSUER",
        }
        exact_kind.append(_support_row(
            event,
            evidence_key=evidence_key,
            target_cash_receipt_no=cash_receipt_no,
            target_adjustment_date=adjustment_date,
            body_path=str(item["support_action_body_path"]),
            body_sha=str(item["support_action_body_sha256"]),
            groups=sorted(set(groups)),
            role="CORROBORATION",
            entitlement=expected_security_class,
            reference=float(item["support_reference_price"]),
            reason=str(item["support_reason"]),
        ))
    if len(exact_kind) > 1:
        raise RuntimeError("duplicate KIND corroboration for one security/date")
    selected.extend(exact_kind)
    for row in exact_kind:
        corroborated.update(json.loads(row["support_semantic_group_keys"]))
    return selected, corroborated


def _cash_event(
    events: list[dict[str, object]],
    viewer_by_receipt: dict[str, object],
    *,
    ticker: str,
    receipt: str,
    cash_amount: float,
    record_date: date,
) -> dict[str, object]:
    cash_rows = [
        row for row in events
        if str(row.get("identifier") or "").zfill(6) == ticker
        and row.get("source") == "DART_DISCLOSURE"
        and row.get("event_type") == "cash_dividend"
        and not _is_related_company_report(row.get("report_name"))
    ]
    matching = [
        row for row in cash_rows
        if str(row.get("rcept_no") or "") == receipt
    ]
    if len(matching) != 1:
        raise RuntimeError(f"cash receipt is missing/ambiguous: {receipt}")
    requested_event = matching[0]
    root = str(requested_event.get("revision_root_action_key") or receipt)
    verified_viewers = tuple({
        id(item): item for item in viewer_by_receipt.values()
    }.values())

    def official_terminal(candidate_root: str) -> dict[str, object]:
        family = [
            row for row in cash_rows
            if str(
                row.get("revision_root_action_key")
                or row.get("rcept_no") or ""
            ) == candidate_root
        ]
        official = [
            item for item in verified_viewers
            if str(item.revision_root_receipt_no) == candidate_root
        ]
        if official:
            orders = {tuple(item.official_family_order) for item in official}
            if len(orders) != 1:
                raise RuntimeError(
                    "verified cash family official order is ambiguous: "
                    f"root={candidate_root}"
                )
            order = next(iter(orders))
            if not order or order[-1] != candidate_root:
                raise RuntimeError(
                    f"verified cash family root/order mismatch: {candidate_root}"
                )
            terminal_receipt = order[0]
            terminal_evidence = [
                item for item in official
                if item.economic_body_receipt_no == terminal_receipt
            ]
            if not terminal_evidence:
                raise RuntimeError(
                    "verified cash family has no economic terminal evidence: "
                    f"root={candidate_root} terminal={terminal_receipt}"
                )
        else:
            if (
                len(family) != 1
                or str(family[0].get("rcept_no") or "") != candidate_root
                or family[0].get("revision_kind") != "ORIGINAL_DECISION"
            ):
                raise RuntimeError(
                    "cash revision family lacks verified official selector order: "
                    f"root={candidate_root}"
                )
            terminal_receipt = candidate_root
        terminal_events = [
            row for row in family
            if str(row.get("rcept_no") or "") == terminal_receipt
            and row.get("revision_kind") != "ATTACHMENT_ONLY"
            and row.get("cash_amount_status") != "ATTACHMENT_ONLY"
        ]
        if len(terminal_events) != 1:
            raise RuntimeError(
                "cash official economic terminal is missing/ambiguous: "
                f"root={candidate_root} terminal={terminal_receipt}"
            )
        return terminal_events[0]

    event = official_terminal(root)
    if str(event.get("rcept_no") or "") != receipt:
        raise RuntimeError(
            "recovery cash receipt is not the fresh official family terminal: "
            f"receipt={receipt} terminal={event.get('rcept_no')}"
        )
    # A newly discovered independent decision on the same issuer/record date
    # would invalidate the frozen one-event overlap receipt.  Do not silently
    # retain only the previously known root.
    positive_terminal_roots: set[str] = set()
    roots = {
        str(row.get("revision_root_action_key") or row.get("rcept_no") or "")
        for row in cash_rows
    }
    for candidate_root in roots:
        candidate_terminal = official_terminal(candidate_root)
        if (
            candidate_terminal.get("record_date") == record_date
            and candidate_terminal.get("cash_amount_status") == "POSITIVE"
            and candidate_terminal.get("cash_amount") is not None
            and float(candidate_terminal["cash_amount"]) > 0
        ):
            positive_terminal_roots.add(candidate_root)
    if positive_terminal_roots != {root}:
        raise RuntimeError(
            "fresh DART cash roots no longer match the frozen one-event pair: "
            f"roots={sorted(positive_terminal_roots)} expected={root}"
        )
    if (
        event.get("action_scope") != "ISSUER"
        or event.get("record_date") != record_date
        or event.get("cash_amount") is None
        or not math.isclose(
            float(event["cash_amount"]), cash_amount, rel_tol=0, abs_tol=1e-8
        )
    ):
        raise RuntimeError(f"cash receipt economics changed: {receipt}")
    return event


def _build_one(
    root: Path,
    row: object,
    *,
    events: list[dict[str, object]],
    viewer_by_receipt: dict[str, object],
    kind_supports: list[dict[str, object]],
    support_families: Sequence[SupportActionFamilyEntry],
    prices: dict[date, PriceObject],
) -> tuple[dict[str, object], dict[str, object]]:
    ticker = str(row.ticker).zfill(6)
    adjustment_date = row.applied_date
    previous_date = row.previous_date
    receipts = _json_list(row.cash_receipts, field="cash_receipts")
    amounts = _json_list(row.cash_amounts, field="cash_amounts")
    records = _json_list(row.record_dates, field="record_dates")
    receipt = str(receipts[0])
    cash_amount = float(amounts[0])
    record_date = _date(records[0], field="cash record date")
    evidence_key = f"{ticker}:{receipt}:{adjustment_date.isoformat()}"
    cash = _cash_event(
        events,
        viewer_by_receipt,
        ticker=ticker,
        receipt=receipt,
        cash_amount=cash_amount,
        record_date=record_date,
    )
    cash_bodies = _cash_body(root, cash, viewer_by_receipt)
    previous_object = prices[previous_date]
    applied_object = prices[adjustment_date]
    previous_close, _ = _read_price_row(
        root / previous_object.local_path,
        ticker=ticker,
        trade_date=previous_date,
    )
    applied_close, reference = _read_price_row(
        root / applied_object.local_path,
        ticker=ticker,
        trade_date=adjustment_date,
    )
    if (
        not math.isclose(previous_close, float(row.previous_close), rel_tol=0, abs_tol=1e-8)
        or not math.isclose(applied_close, float(row.applied_close), rel_tol=0, abs_tol=1e-8)
    ):
        raise RuntimeError("overlap/KRX raw price parity failed")
    observed_factor = reference / previous_close
    factor_low, factor_high = stored_price_factor_interval(
        previous_close=float(row.previous_close),
        previous_adj_close=float(row.previous_adj_close),
        applied_close=float(row.applied_close),
        applied_adj_close=float(row.applied_adj_close),
    )
    if not factor_low <= observed_factor <= factor_high:
        raise RuntimeError("overlap/KRX reference factor parity failed")
    components, groups_by_kind, diagnostic = _component_supports(
        root,
        events,
        kind_supports,
        support_families,
        ticker=ticker,
        record_date=record_date,
        adjustment_date=adjustment_date,
        evidence_key=evidence_key,
        cash_receipt_no=receipt,
    )
    corroborations, corroborated = _corroborations(
        root,
        events,
        kind_supports,
        ticker=ticker,
        asset_name=str(row.asset_name),
        adjustment_date=adjustment_date,
        raw_reference=reference,
        evidence_key=evidence_key,
        cash_receipt_no=receipt,
        groups_by_kind=groups_by_kind,
    )
    # A DART stock-dividend decision identifies its record date, not its ex
    # date.  It therefore requires an exact official detachment notice.
    dart_stock_groups = {
        group for component in components
        if component["support_action_source"] in {
            "DART_DISCLOSURE", "DART_VIEWER",
        }
        and component["support_action_type"] == "stock_dividend"
        for group in json.loads(component["support_semantic_group_keys"])
    }
    if not dart_stock_groups.issubset(corroborated):
        viewer_stock = [
            component for component in components
            if component["support_action_source"] == "DART_VIEWER"
            and component["support_action_type"] == "stock_dividend"
        ]
        reviewed_exception = False
        if len(viewer_stock) == 1 and not (
            dart_stock_groups & corroborated
        ):
            component = viewer_stock[0]
            reviewed_identity = (
                ticker,
                receipt,
                adjustment_date.isoformat(),
                str(component["support_action_key"]),
                _date(
                    component["support_record_date"],
                    field="reviewed stock record date",
                ).isoformat(),
                format(
                    float(component["support_ratio_numerator"])
                    / float(component["support_ratio_denominator"]),
                    ".12g",
                ),
                previous_object.content_sha256,
                applied_object.content_sha256,
                format(reference, ".12g"),
            )
            reviewed_exception = reviewed_identity in NO_NOTICE_STOCK_DIVIDEND
        if not reviewed_exception:
            raise RuntimeError(
                "stock-dividend family lacks exact ex/reference notice: "
                f"reviewed_identity={locals().get('reviewed_identity')}"
            )
        diagnostic["corroboration_mode"] = "NO_NOTICE_MARKET_REFERENCE"
    supports = components + corroborations
    support_frame = pd.DataFrame(supports)
    all_groups = {
        group for support in supports
        for group in json.loads(str(support["support_semantic_group_keys"]))
    }
    parent = {
        "evidence_key": evidence_key,
        "ticker": ticker,
        "cash_receipt_no": receipt,
        **cash_bodies,
        "support_action_count": len(supports),
        "support_action_digest": support_manifest_digest(support_frame),
        "support_semantic_group_count": len(all_groups),
        "price_source": "KRX",
        "previous_price_source_object_key": previous_object.local_path,
        "previous_price_source_content_sha256": previous_object.content_sha256,
        "previous_price_source_etag": previous_object.etag,
        "previous_price_source_schema": previous_object.source_schema,
        "adjustment_price_source_object_key": applied_object.local_path,
        "adjustment_price_source_content_sha256": applied_object.content_sha256,
        "adjustment_price_source_etag": applied_object.etag,
        "adjustment_price_source_schema": applied_object.source_schema,
        "previous_trade_date": previous_date,
        "adjustment_trade_date": adjustment_date,
        "raw_previous_close": previous_close,
        "raw_applied_close": applied_close,
        "raw_reference_price": reference,
        "expected_price_factor": observed_factor,
        "cash_scale_basis": PRE_EVENT_PRICE_SCALE,
    }
    parent["manifest_row_sha256"] = manifest_parent_row_sha256(parent)
    parent["support_actions"] = sorted(
        supports,
        key=lambda item: (
            str(item["support_action_source"]),
            str(item["support_action_key"]),
            str(item["support_action_type"]),
        ),
    )
    cash_only_coincidence_factor = (previous_close - cash_amount) / previous_close
    diagnostic.update({
        "asset_id": int(row.asset_id),
        "ticker": ticker,
        "cash_receipt": receipt,
        "cash_amount": cash_amount,
        "record_date": record_date.isoformat(),
        "adjustment_date": adjustment_date.isoformat(),
        "observed_price_factor": observed_factor,
        "official_reference_price": reference,
        "corroboration_receipts": [
            str(item["support_action_key"]) for item in corroborations
        ],
        # Diagnostic only.  This value is never consulted by classification.
        "cash_dps_coincidence_factor": cash_only_coincidence_factor,
        "cash_dps_numeric_coincidence": math.isclose(
            cash_only_coincidence_factor, observed_factor,
            rel_tol=0,
            abs_tol=5e-13,
        ),
        "classification_basis": "OFFICIAL_NON_CASH_COMPONENT_AND_KRX_REFERENCE",
    })
    return parent, diagnostic


def _manifest_payload(parents: list[dict[str, object]]) -> dict[str, object]:
    parent_rows = [
        {key: value for key, value in parent.items() if key != "support_actions"}
        for parent in parents
    ]
    support_rows = [
        support for parent in parents for support in parent["support_actions"]
    ]
    all_groups = {
        group for support in support_rows
        for group in json.loads(str(support["support_semantic_group_keys"]))
    }
    return {
        "schema_version": SOURCE_EVIDENCE_CONTRACT,
        "complete": True,
        "row_count": len(parent_rows),
        "row_digest": source_manifest_digest(pd.DataFrame(parent_rows)),
        "support_action_count": len(support_rows),
        "support_action_digest": support_manifest_digest(
            pd.DataFrame(support_rows)
        ),
        "support_semantic_group_count": len(all_groups),
        "evidence": parents,
    }


def _assert_kind_support_consumed(
    kind_supports: Sequence[dict[str, object]],
    parents: Sequence[dict[str, object]],
) -> None:
    """Reject reviewed KIND rows that do not bind any rebuilt parent."""
    declared = {
        (
            str(item["ticker"]), str(item["support_action_key"]),
            str(item["support_action_type"]),
            str(item["support_semantic_role"]),
            str(item["target_cash_receipt_no"]),
            _date(
                item["target_adjustment_date"], field="KIND target adjustment date",
            ).isoformat(),
        )
        for item in kind_supports
    }
    consumed: dict[tuple[str, str, str, str, str, str], int] = {}
    for parent in parents:
        ticker = str(parent["ticker"])
        for support in parent["support_actions"]:
            if support["support_action_source"] != "KRX_KIND":
                continue
            identity = (
                ticker,
                str(support["support_action_key"]),
                str(support["support_action_type"]),
                str(support["support_semantic_role"]),
                str(support["target_cash_receipt_no"]),
                _date(
                    support["target_adjustment_date"],
                    field="support target adjustment date",
                ).isoformat(),
            )
            consumed[identity] = consumed.get(identity, 0) + 1
    if set(consumed) != declared or any(count != 1 for count in consumed.values()):
        raise RuntimeError(
            "KIND support manifest has unused/orphan identities: "
            f"declared={sorted(declared)} consumed={sorted(consumed)}"
        )


def _kind_summary(
    root: Path, supports: Sequence[dict[str, object]],
) -> dict[str, object]:
    if not supports:
        return {
            "kind_support_manifest_sha256": None,
            "kind_support_count": 0,
            "kind_support_digest": hashlib.sha256(b"[]").hexdigest(),
            "kind_reference_request_sha256": None,
            "kind_reference_request_provenance": None,
            "kind_reference_request_count": 0,
            "kind_reference_request_digest": hashlib.sha256(b"[]").hexdigest(),
            "kind_component_request_sha256": None,
            "kind_component_request_provenance": None,
            "kind_component_request_count": 0,
            "kind_component_request_digest": hashlib.sha256(b"[]").hexdigest(),
        }
    path = root / KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    payload = json.loads(path.read_bytes())
    reference = json.loads(
        (root / str(payload["reference_request_path"])).read_bytes()
    )
    component = json.loads(
        (root / str(payload["component_request_path"])).read_bytes()
    )
    return {
        "kind_support_manifest_sha256": _sha256(path),
        "kind_support_count": len(supports),
        "kind_support_digest": payload["support_digest"],
        "kind_reference_request_sha256": payload["reference_request_sha256"],
        "kind_reference_request_provenance": reference["provenance"],
        "kind_reference_request_count": reference["request_count"],
        "kind_reference_request_digest": reference["request_digest"],
        "kind_component_request_sha256": payload["component_request_sha256"],
        "kind_component_request_provenance": component["provenance"],
        "kind_component_request_count": component["component_count"],
        "kind_component_request_digest": component["component_digest"],
    }


def _publish_source_manifest(
    root: Path,
    payload: dict[str, object],
    *,
    coverage_start: date,
    coverage_end: date,
    expected_parent_count: int = EXPECTED_PARENT_COUNT,
):
    """Atomically publish, verify twice, and restore the prior manifest on failure."""
    destination = root / MANIFEST_RELATIVE_PATH
    previous = destination.read_bytes() if destination.is_file() else None
    try:
        _atomic_write(destination, _canonical_bytes(payload))
        verified = verify_source_evidence_manifest(
            str(root),
            required_start=coverage_start,
            required_end=coverage_end,
        )
        if verified.row_count != expected_parent_count:
            raise RuntimeError("source-evidence verifier parent count changed")
        # A second filesystem read catches accidental dependence on mutable
        # in-memory rows or on a verifier result cached across the write.
        verified_again = verify_source_evidence_manifest(
            str(root),
            required_start=coverage_start,
            required_end=coverage_end,
        )
        if verified.metadata != verified_again.metadata:
            raise RuntimeError("source-evidence verifier roundtrip changed")
    except Exception:
        if previous is None:
            destination.unlink(missing_ok=True)
        else:
            _atomic_write(destination, previous)
        raise
    return verified


def build_source_evidence(
    base: str | Path,
    inputs: RecoveryInputs,
    *,
    coverage_start: date,
    coverage_end: date,
    expected_s3_root: str,
) -> BuildResult:
    root = Path(base).expanduser().resolve()
    intervals = verify_fresh_dart_snapshot(
        root,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    price_objects = verify_price_object_manifest(
        root, inputs, expected_s3_root=expected_s3_root,
    )
    prices = {item.trade_date: item for item in price_objects}
    verified_families = verify_support_action_families(
        root, required_start=coverage_start, required_end=coverage_end,
    )
    families_by_ticker: dict[str, list[SupportActionFamilyEntry]] = {}
    for family in verified_families.entries:
        families_by_ticker.setdefault(family.ticker, []).append(family)
    prepared, prepare_stats = corporate_actions.prepare(
        str(root),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    events = prepared.to_dict("records")
    # Calling prepare only after v3/v5 marker verification is intentional: no
    # old classification artifact or legacy disclosure manifest can be used.
    viewer = verify_viewer_corrections(
        str(root), required_start=coverage_start, required_end=coverage_end,
    )
    viewer_by_receipt = {item.receipt_no: item for item in viewer.receipts}
    kind = _kind_supports(root)
    parents: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    for row in inputs.overlap.sort_values(
        ["asset_id", "applied_date"], kind="stable"
    ).itertuples(index=False):
        try:
            parent, diagnostic = _build_one(
                root,
                row,
                events=events,
                viewer_by_receipt=viewer_by_receipt,
                kind_supports=kind,
                support_families=families_by_ticker.get(
                    str(row.ticker).zfill(6), ()
                ),
                prices=prices,
            )
        except Exception as exc:  # noqa: BLE001 - preserve all HOLD reasons
            failures.append({
                "asset_id": int(row.asset_id),
                "ticker": str(row.ticker).zfill(6),
                "adjustment_date": row.applied_date.isoformat(),
                "cash_receipts": _json_list(row.cash_receipts, field="cash_receipts"),
                "error": str(exc),
            })
            continue
        parents.append(parent)
        diagnostics.append(diagnostic)
    try:
        _assert_kind_support_consumed(kind, parents)
    except RuntimeError as exc:
        failures.append({
            "asset_id": None,
            "ticker": None,
            "adjustment_date": None,
            "cash_receipts": [],
            "error": str(exc),
        })
    classification = {
        "schema_version": CLASSIFICATION_SCHEMA,
        "complete": not failures and len(parents) == EXPECTED_PARENT_COUNT,
        "source_overlap_sha256": _sha256(inputs.overlap_path),
        "source_expectations_sha256": _sha256(inputs.expectations_path),
        "support_action_family_manifest_sha256": (
            verified_families.manifest_sha256
        ),
        "support_action_family_candidate_count": (
            verified_families.candidate_count
        ),
        "support_action_family_candidate_digest": (
            verified_families.candidate_digest
        ),
        "support_action_family_entry_count": verified_families.entry_count,
        "support_action_family_entry_digest": verified_families.entry_digest,
        **_kind_summary(root, kind),
        "classified_count": len(parents),
        "unresolved_count": len(failures),
        "classifications": diagnostics,
        "failures": failures,
    }
    _atomic_write(root / CLASSIFICATION_RELATIVE_PATH, _canonical_bytes(classification))
    if failures or len(parents) != EXPECTED_PARENT_COUNT:
        raise RuntimeError(
            "cash-scale evidence classification is unresolved: "
            f"parents={len(parents)} failures={len(failures)} "
            f"samples={failures[:5]}"
        )
    if len({parent["evidence_key"] for parent in parents}) != len(parents):
        raise RuntimeError("duplicate evidence parent key")
    payload = _manifest_payload(parents)
    destination = root / MANIFEST_RELATIVE_PATH
    verified = _publish_source_manifest(
        root,
        payload,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    summary = {
        "schema_version": BUILD_SUMMARY_SCHEMA,
        "complete": True,
        "source_overlap_sha256": _sha256(inputs.overlap_path),
        "source_expectations_sha256": _sha256(inputs.expectations_path),
        "source_missing_pairs_sha256": _sha256(inputs.missing_pairs_path),
        "source_resolved_cash_sha256": _sha256(inputs.resolved_cash_path),
        "dart_coverage_intervals": [
            {"from": start.isoformat(), "to": end.isoformat()}
            for start, end in intervals
        ],
        "dart_prepare_stats": prepare_stats,
        "price_object_count": len(price_objects),
        "price_object_manifest_sha256": _sha256(
            root / PRICE_OBJECT_MANIFEST_RELATIVE_PATH
        ),
        "support_action_family_manifest_sha256": (
            verified_families.manifest_sha256
        ),
        "support_action_family_candidate_count": (
            verified_families.candidate_count
        ),
        "support_action_family_candidate_digest": (
            verified_families.candidate_digest
        ),
        "support_action_family_entry_count": verified_families.entry_count,
        "support_action_family_entry_digest": verified_families.entry_digest,
        **_kind_summary(root, kind),
        "support_action_family_used_roots": sorted({
            (str(item["ticker"]), str(item.get("stock_family_root_receipt")))
            for item in diagnostics if item.get("stock_family_root_receipt")
        } | {
            (str(item["ticker"]), str(item.get("bonus_family_root_receipt")))
            for item in diagnostics if item.get("bonus_family_root_receipt")
        }),
        **verified.metadata,
        "classification_sha256": _sha256(root / CLASSIFICATION_RELATIVE_PATH),
        "first_listing_exclusion_count": len(inputs.missing_pairs),
        "unresolved_count": 0,
    }
    summary_path = root / BUILD_SUMMARY_RELATIVE_PATH
    _atomic_write(summary_path, _canonical_bytes(summary))
    return BuildResult(
        manifest_path=str(destination),
        manifest_sha256=verified.manifest_sha256,
        parent_count=verified.row_count,
        parent_digest=verified.row_digest,
        support_action_count=len(verified.support_frame),
        support_action_digest=support_manifest_digest(verified.support_frame),
        semantic_group_count=int(
            verified.metadata["manifest_support_semantic_group_count"]
        ),
        summary_path=str(summary_path),
    )


def _inputs_from_args(args: argparse.Namespace) -> RecoveryInputs:
    return load_recovery_inputs(args.overlap, args.expectations)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base", required=True)
    common.add_argument("--overlap", required=True)
    common.add_argument("--expectations", required=True)

    download = subparsers.add_parser("download-prices", parents=[common])
    download.add_argument("--s3-root", required=True)
    download.add_argument("--aws-profile")
    download.add_argument("--aws-region", default="ap-northeast-2")

    verify_prices = subparsers.add_parser("verify-prices", parents=[common])
    verify_prices.add_argument("--s3-root", required=True)

    download_kind = subparsers.add_parser("download-kind")
    download_kind.add_argument("--base", required=True)
    download_kind.add_argument("--reference-requests", required=True)
    download_kind.add_argument("--component-requests", required=True)

    build = subparsers.add_parser("build", parents=[common])
    build.add_argument(
        "--coverage-start", type=date.fromisoformat, default=DEFAULT_COVERAGE_START,
    )
    build.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    build.add_argument("--s3-root", required=True)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if args.command == "download-kind":
        result = download_kind_evidence(
            args.base,
            args.reference_requests,
            args.component_requests,
        )
        output: object = {
            "complete": True,
            "support_count": len(result),
            "manifest": str(
                Path(args.base).expanduser().resolve()
                / KIND_SUPPORT_MANIFEST_RELATIVE_PATH
            ),
        }
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return
    inputs = _inputs_from_args(args)
    if args.command == "download-prices":
        result = download_price_objects(
            args.base,
            inputs,
            s3_root=args.s3_root,
            profile=args.aws_profile,
            region=args.aws_region,
        )
        output = {
            "complete": True,
            "object_count": len(result),
            "manifest": str(
                Path(args.base).expanduser().resolve()
                / PRICE_OBJECT_MANIFEST_RELATIVE_PATH
            ),
        }
    elif args.command == "verify-prices":
        result = verify_price_object_manifest(
            args.base, inputs, expected_s3_root=args.s3_root,
        )
        output = {"complete": True, "object_count": len(result)}
    else:
        output = build_source_evidence(
            args.base,
            inputs,
            coverage_start=args.coverage_start,
            coverage_end=args.coverage_end,
            expected_s3_root=args.s3_root,
        ).__dict__
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
