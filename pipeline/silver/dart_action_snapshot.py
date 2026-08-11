"""Content-addressed completeness contract for local DART action snapshots.

The native Bronze collector writes one discovery manifest and two completion
markers per requested interval.  This module verifies that those completed
intervals cover every calendar day from the total-return contract start, then
binds every local action body to an immutable SHA-256 manifest.  Silver loaders
must verify this manifest before parsing or publishing any action.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence

from pipeline.bronze.corporate_actions import (
    _event_api_for_title,
    _needs_document,
)
from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
)
from pipeline.bronze.dart_viewer_corrections import (
    required_viewer_receipts,
    verify_viewer_corrections,
)
from pipeline.bronze.dart_support_action_families import (
    MANIFEST_RELATIVE_PATH as SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH,
    external_evidence_paths as support_family_external_evidence_paths,
)
from pipeline.silver.reviewed_dividend_corrections import (
    MANIFEST_RELATIVE_PATH as REVIEWED_CORRECTIONS_RELATIVE_PATH,
    active_corrections as active_reviewed_corrections,
    canonical_manifest_bytes as reviewed_corrections_manifest_bytes,
    external_evidence_paths as reviewed_external_evidence_paths,
)
from pipeline.silver.cash_adjustment_scale_evidence import (
    external_evidence_paths as cash_scale_external_evidence_paths,
    verify_source_evidence_manifest,
)
from pipeline.silver.return_contract import is_valid_krx_ticker


SCHEMA_VERSION = "dart_total_return_action_snapshot_v5"
DEFAULT_COVERAGE_START = date(2015, 1, 1)
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/action_snapshot_manifest.json"
)


@dataclass(frozen=True)
class VerifiedActionSnapshot:
    base: str
    manifest_path: str
    manifest_sha256: str
    body_digest: str
    body_count: int
    coverage_start: date
    coverage_end: date
    coverage_intervals: tuple[tuple[date, date], ...]
    disclosure_observation_audit: dict[str, object]
    cash_adjustment_scale_source_evidence: dict[str, object]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _date_value(value: object, *, field: str) -> date:
    rendered = str(value or "").strip()
    try:
        if len(rendered) == 8 and rendered.isdigit():
            return date(
                int(rendered[:4]), int(rendered[4:6]), int(rendered[6:8])
            )
        return date.fromisoformat(rendered)
    except ValueError as exc:
        raise RuntimeError(f"invalid {field}: {value!r}") from exc


def _is_total_return_disclosure(report_name: object) -> bool:
    rendered = str(report_name or "").replace(" ", "")
    return (
        "현금" in rendered
        and "현물" in rendered
        and "배당결정" in rendered
    ) or "배당락" in rendered or "주식배당결정" in rendered or "권배락" in rendered


def _disclosure_observation_audit(root: Path) -> dict[str, object]:
    observations: list[tuple[Path, dict]] = []
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    for disclosure in sorted(
        manifest_root.glob("from=*/to=*/disclosures_v3.json")
    ):
        try:
            rows = json.loads(disclosure.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid DART disclosures body: {disclosure}"
            ) from exc
        if not isinstance(rows, list):
            raise RuntimeError(f"DART disclosures must be a list: {disclosure}")
        observations.extend(
            (disclosure, row)
            for row in rows
            if isinstance(row, dict)
            and re.fullmatch(
                r"[0-9]{14}", str(row.get("rcept_no") or "").strip()
            )
        )
    _, audit = canonicalize_disclosures(
        observations, audit_root=root,
    )
    return audit


def _native_complete_intervals(root: Path) -> list[tuple[date, date]]:
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    intervals: list[tuple[date, date]] = []
    required_document_receipts: set[str] = set()
    complete_disclosure_receipts: set[str] = set()
    incomplete_disclosures: dict[str, dict] = {}
    for disclosure in sorted(
        manifest_root.glob("from=*/to=*/disclosures_v3.json")
    ):
        interval_dir = disclosure.parent
        try:
            start = _date_value(
                interval_dir.parent.name.removeprefix("from="),
                field="native interval start",
            )
            end = _date_value(
                interval_dir.name.removeprefix("to="),
                field="native interval end",
            )
        except RuntimeError:
            continue
        if start > end:
            raise RuntimeError(f"reversed DART interval: {interval_dir}")
        try:
            disclosures = json.loads(disclosure.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid DART disclosures body: {disclosure}") from exc
        if not isinstance(disclosures, list):
            raise RuntimeError(f"DART disclosures must be a list: {disclosure}")
        malformed_total_return_rows = [
            {
                "rcept_no": row.get("rcept_no"),
                "stock_code": row.get("stock_code"),
                "corp_code": row.get("corp_code"),
                "corp_cls": row.get("corp_cls"),
                "report_name": row.get("report_nm"),
            }
            for row in disclosures
            if isinstance(row, dict)
            and _is_total_return_disclosure(row.get("report_nm"))
            and (
                re.fullmatch(
                    r"[0-9]{14}", str(row.get("rcept_no") or "").strip()
                ) is None
                or not is_valid_krx_ticker(row.get("stock_code"))
            )
        ]
        if malformed_total_return_rows:
            raise RuntimeError(
                "TR-relevant DART disclosure has no receipt/ticker identity: "
                f"{malformed_total_return_rows[:20]}"
            )
        interval_document_receipts = {
            str(row.get("rcept_no") or "")
            for row in disclosures
            if isinstance(row, dict)
            and _needs_document(row.get("report_nm"))
            and str(row.get("rcept_no") or "")
        }
        interval_disclosure_receipts = {
            str(row.get("rcept_no") or "")
            for row in disclosures
            if isinstance(row, dict)
            and str(row.get("rcept_no") or "")
        }
        marker_paths = [
            interval_dir / "structured_complete_v3.json",
            interval_dir / "documents_complete_v5.json",
        ]
        # Overlapping collector runs are normal.  A v1/in-progress interval is
        # harmless only when every receipt it discovered is also present in a
        # fully completed v2 interval.  Otherwise the snapshot is incomplete.
        if not all(path.is_file() for path in marker_paths):
            incomplete_disclosures.update({
                str(row.get("rcept_no")): row
                for row in disclosures
                if isinstance(row, dict) and str(row.get("rcept_no") or "")
            })
            continue
        # v5 completion covers the full document keyword set, including
        # structured bonus-decision originals/corrections.  Rechecking only
        # the narrower historical TR-title subset would let a required bonus
        # correction ZIP disappear after the completion marker was written.
        required_document_receipts.update(interval_document_receipts)
        complete_disclosure_receipts.update(interval_disclosure_receipts)
        for marker_path in marker_paths:
            marker_name = marker_path.name
            try:
                marker = json.loads(marker_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"missing/invalid DART completion marker: {marker_path}"
                ) from exc
            if marker.get("status") != "COMPLETE":
                raise RuntimeError(f"DART marker is not COMPLETE: {marker_path}")
            if _date_value(marker.get("fromdate"), field="marker fromdate") != start:
                raise RuntimeError(f"DART marker start mismatch: {marker_path}")
            if _date_value(marker.get("todate"), field="marker todate") != end:
                raise RuntimeError(f"DART marker end mismatch: {marker_path}")
            if (
                marker_name == "documents_complete_v5.json"
                and int(marker.get("candidate_count", -1))
                != len(interval_document_receipts)
            ):
                raise RuntimeError(
                    f"DART document candidate parity mismatch: {marker_path}"
                )
            if marker_name == "structured_complete_v3.json":
                structured_queries = {
                    (event_api.slug, str(row.get("corp_code") or ""))
                    for row in disclosures
                    if isinstance(row, dict)
                    and str(row.get("stock_code") or "").strip()
                    and str(row.get("corp_code") or "")
                    and (event_api := _event_api_for_title(
                        row.get("report_nm")
                    )) is not None
                }
                if int(marker.get("query_count", -1)) != len(
                    structured_queries
                ):
                    raise RuntimeError(
                        f"DART structured query parity mismatch: {marker_path}"
                    )
        intervals.append((start, end))
    if not intervals:
        raise RuntimeError("no complete DART corporate-action intervals")
    uncovered_receipts = sorted(
        set(incomplete_disclosures) - complete_disclosure_receipts
    )
    if uncovered_receipts:
        blocking = []
        for receipt in uncovered_receipts:
            row = incomplete_disclosures[receipt]
            relevant = _is_total_return_disclosure(row.get("report_nm"))
            # corp_cls is raw provenance, never scope evidence.  Historical
            # listed KOSDAQ issuers are demonstrably labelled E, so every
            # uncovered economically relevant receipt blocks certification.
            if relevant:
                blocking.append({
                    "rcept_no": receipt,
                    "corp_code": row.get("corp_code"),
                    "stock_code": row.get("stock_code"),
                    "corp_cls": row.get("corp_cls"),
                    "report_name": row.get("report_nm"),
                })
        if blocking:
            raise RuntimeError(
                "incomplete DART intervals contain potentially listed action "
                f"receipts absent from every complete interval: {blocking[:20]}"
            )
    document_paths: dict[str, list[Path]] = {}
    unavailable_paths: dict[str, list[Path]] = {}
    for path in root.glob("corporate_actions/dart/documents/**/rcept=*.zip"):
        if path.is_file():
            document_paths.setdefault(
                path.stem.removeprefix("rcept="), []
            ).append(path)
    for path in root.glob(
        "corporate_actions/dart/documents_unavailable/**/rcept=*.xml"
    ):
        if path.is_file():
            unavailable_paths.setdefault(
                path.stem.removeprefix("rcept="), []
            ).append(path)
    invalid_documents = []
    for receipt in sorted(required_document_receipts):
        documents = document_paths.get(receipt, [])
        unavailable = unavailable_paths.get(receipt, [])
        if len(documents) + len(unavailable) != 1:
            invalid_documents.append({
                "receipt": receipt,
                "zip_count": len(documents),
                "status014_count": len(unavailable),
                "reason": "MISSING_OR_AMBIGUOUS_BODY",
            })
            continue
        if documents and not zipfile.is_zipfile(documents[0]):
            invalid_documents.append({
                "receipt": receipt,
                "reason": "INVALID_OPENDART_ZIP",
            })
        if unavailable:
            body = unavailable[0].read_text(encoding="utf-8", errors="replace")
            statuses = re.findall(r"<status>\s*([^<]+)\s*</status>", body)
            if statuses != ["014"]:
                invalid_documents.append({
                    "receipt": receipt,
                    "reason": "INVALID_DOCUMENT_UNAVAILABLE_STATUS",
                })
    if invalid_documents:
        raise RuntimeError(
            "DART document completion marker has missing/invalid bodies: "
            f"{invalid_documents[:20]}"
        )
    return intervals


def _assert_continuous(
    intervals: Iterable[tuple[date, date]],
    *,
    required_start: date,
    required_end: date,
) -> tuple[tuple[date, date], ...]:
    if required_end < required_start:
        raise ValueError("coverage_end must be on/after coverage_start")
    ordered = sorted(set(intervals))
    cursor = required_start
    retained: list[tuple[date, date]] = []
    for start, end in ordered:
        if end < cursor:
            continue
        if start > cursor:
            raise RuntimeError(
                "DART snapshot coverage gap: "
                f"{cursor.isoformat()}~{(start - timedelta(days=1)).isoformat()}"
            )
        retained.append((start, end))
        cursor = max(cursor, end + timedelta(days=1))
        if cursor > required_end:
            break
    if cursor <= required_end:
        raise RuntimeError(
            "DART snapshot coverage ends early: "
            f"covered_through={(cursor - timedelta(days=1)).isoformat()} "
            f"required={required_end.isoformat()}"
        )
    return tuple(retained)


def _evidence_paths(root: Path) -> list[Path]:
    action_root = root / "corporate_actions" / "dart"
    if not action_root.is_dir():
        raise RuntimeError(f"missing DART action root: {action_root}")
    paths = [
        path
        for path in action_root.rglob("*")
        if path.is_file() and path.resolve() != (root / MANIFEST_RELATIVE_PATH).resolve()
    ]
    paths.extend(reviewed_external_evidence_paths(root))
    paths.extend(cash_scale_external_evidence_paths(str(root)))
    if (root / SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH).is_file():
        paths.extend(
            root / relative
            for relative in support_family_external_evidence_paths(root)
        )
    missing_external = [path for path in paths if not path.is_file()]
    if missing_external:
        raise RuntimeError(
            "DART reviewed correction evidence is missing: "
            f"{[str(path) for path in missing_external[:10]]}"
        )
    if not paths:
        raise RuntimeError(f"DART action snapshot has no bodies: {action_root}")
    return sorted(
        set(paths), key=lambda path: path.relative_to(root).as_posix(),
    )


def _body_entries(root: Path, paths: Sequence[Path]) -> list[dict]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "content_length": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in paths
    ]


def _body_digest(entries: Sequence[dict]) -> str:
    canonical = json.dumps(
        list(entries),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _verify_required_viewer_evidence(root: Path) -> None:
    required = required_viewer_receipts(str(root))
    if required:
        verify_viewer_corrections(
            str(root), required_receipts=required,
        )


def _write_reviewed_correction_manifest(root: Path) -> None:
    if not active_reviewed_corrections(root):
        return
    destination = root / REVIEWED_CORRECTIONS_RELATIVE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = reviewed_corrections_manifest_bytes(root)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _verify_reviewed_correction_manifest(root: Path) -> None:
    if not active_reviewed_corrections(root):
        return
    path = root / REVIEWED_CORRECTIONS_RELATIVE_PATH
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(
            f"missing reviewed dividend correction manifest: {path}"
        ) from exc
    if actual != reviewed_corrections_manifest_bytes(root):
        raise RuntimeError("reviewed dividend correction manifest changed")


def build_snapshot_manifest(
    base: str,
    *,
    coverage_end: date,
    coverage_start: date = DEFAULT_COVERAGE_START,
) -> VerifiedActionSnapshot:
    """Verify native markers and atomically write the content manifest."""
    root = Path(base).expanduser().resolve()
    _write_reviewed_correction_manifest(root)
    intervals = _assert_continuous(
        _native_complete_intervals(root),
        required_start=coverage_start,
        required_end=coverage_end,
    )
    _verify_required_viewer_evidence(root)
    _verify_reviewed_correction_manifest(root)
    scale_evidence = verify_source_evidence_manifest(str(root))
    entries = _body_entries(root, _evidence_paths(root))
    disclosure_observation_audit = _disclosure_observation_audit(root)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "coverage_start": coverage_start.isoformat(),
        "coverage_end": coverage_end.isoformat(),
        "coverage_intervals": [
            {"from": start.isoformat(), "to": end.isoformat()}
            for start, end in intervals
        ],
        "body_count": len(entries),
        "body_digest": _body_digest(entries),
        "disclosure_observation_audit": disclosure_observation_audit,
        "cash_adjustment_scale_source_evidence": scale_evidence.metadata,
        "objects": entries,
    }
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    destination = root / MANIFEST_RELATIVE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    try:
        with os.fdopen(file_descriptor, "wb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return verify_snapshot_manifest(
        str(root),
        required_start=coverage_start,
        required_end=coverage_end,
    )


def verify_snapshot_manifest(
    base: str,
    *,
    required_start: date = DEFAULT_COVERAGE_START,
    required_end: date | None = None,
) -> VerifiedActionSnapshot:
    """Fail closed unless markers, coverage and every body hash agree."""
    root = Path(base).expanduser().resolve()
    manifest_path = root / MANIFEST_RELATIVE_PATH
    try:
        raw = manifest_path.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"missing/invalid DART action snapshot manifest: {manifest_path}"
        ) from exc
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("unsupported DART action snapshot schema")
    if payload.get("complete") is not True:
        raise RuntimeError("DART action snapshot is not complete")
    coverage_start = _date_value(
        payload.get("coverage_start"), field="coverage_start"
    )
    coverage_end = _date_value(payload.get("coverage_end"), field="coverage_end")
    disclosure_observation_audit = _disclosure_observation_audit(root)
    if payload.get("disclosure_observation_audit") != (
        disclosure_observation_audit
    ):
        raise RuntimeError(
            "DART disclosure observation canonicalization metadata changed"
        )
    expected_end = required_end or coverage_end
    if coverage_start > required_start:
        raise RuntimeError(
            f"DART coverage starts at {coverage_start}, required {required_start}"
        )
    if coverage_end < expected_end:
        raise RuntimeError(
            f"DART coverage ends at {coverage_end}, required {expected_end}"
        )
    declared_intervals = tuple(
        (
            _date_value(item.get("from"), field="interval from"),
            _date_value(item.get("to"), field="interval to"),
        )
        for item in payload.get("coverage_intervals") or []
    )
    continuous = _assert_continuous(
        declared_intervals,
        required_start=required_start,
        required_end=expected_end,
    )
    native = set(_native_complete_intervals(root))
    if any(interval not in native for interval in declared_intervals):
        raise RuntimeError("snapshot manifest declares an unverified native interval")
    _verify_required_viewer_evidence(root)
    _verify_reviewed_correction_manifest(root)
    scale_evidence = verify_source_evidence_manifest(str(root))
    if payload.get("cash_adjustment_scale_source_evidence") != (
        scale_evidence.metadata
    ):
        raise RuntimeError(
            "cash-adjustment scale source evidence metadata changed"
        )

    declared_entries = payload.get("objects")
    if not isinstance(declared_entries, list):
        raise RuntimeError("DART snapshot objects must be a list")
    actual_paths = _evidence_paths(root)
    actual_relative = [path.relative_to(root).as_posix() for path in actual_paths]
    declared_relative = [str(item.get("path") or "") for item in declared_entries]
    if declared_relative != actual_relative:
        raise RuntimeError("DART snapshot manifest/body path set changed")
    actual_entries = _body_entries(root, actual_paths)
    if declared_entries != actual_entries:
        raise RuntimeError("DART snapshot body SHA/content length mismatch")
    digest = _body_digest(actual_entries)
    if payload.get("body_digest") != digest:
        raise RuntimeError("DART snapshot aggregate body digest mismatch")
    if int(payload.get("body_count", -1)) != len(actual_entries):
        raise RuntimeError("DART snapshot body count mismatch")
    return VerifiedActionSnapshot(
        base=str(root),
        manifest_path=str(manifest_path),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        body_digest=digest,
        body_count=len(actual_entries),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        coverage_intervals=continuous,
        disclosure_observation_audit=disclosure_observation_audit,
        cash_adjustment_scale_source_evidence=scale_evidence.metadata,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--coverage-start",
        type=date.fromisoformat,
        default=DEFAULT_COVERAGE_START,
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    verified = build_snapshot_manifest(
        args.base,
        coverage_start=args.coverage_start,
        coverage_end=args.coverage_end,
    )
    print(json.dumps(verified.__dict__, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
