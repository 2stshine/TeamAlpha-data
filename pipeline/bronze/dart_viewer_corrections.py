"""Content-addressed DART viewer fallback for correction receipts.

OpenDART ``document.xml`` can return status 014 for a correction receipt even
though the official DART report viewer exposes both its revision lineage and
the final corrected body.  This collector is deliberately separate from
Silver: it downloads immutable public evidence into Bronze, writes one
completion manifest, and lets Silver parse only that verified snapshot.

Nothing is fetched unless ``--apply`` is supplied.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import random
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import requests

from pipeline.bronze.corporate_actions import (
    _event_api_for_title,
    _needs_document,
)
from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
)
from pipeline.bronze.dart_support_action_families import (
    official_dart_viewer_url,
    parse_dart_date,
    parse_official_dart_main_page,
)


SCHEMA_VERSION = "dart_viewer_correction_snapshot_v2"
SOURCE_CONTRACT = "dart_official_main_family_attachment_viewer_body_v2"
FAMILY_ORDER_CONTRACT = "OFFICIAL_MAIN_NEWEST_TO_OLDEST_WITH_ATTACHMENT_KEYS"
ATTACHMENT_PARENT_CONTRACT = "OFFICIAL_FAMILY_ROOT_NOT_DIRECT_ATTACHMENT_TARGET"
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/viewer_corrections/manifest.json"
)
KNOWN_DAMAGED_DOCUMENT_RECEIPTS = {
    # The cached document body contains literal '?' bytes in every Korean
    # label.  The official viewer body is intact and is required permanently,
    # rather than being an ad-hoc CLI exception for this recovery run.
    "20220802900375": "CACHED_ZIP_LABEL_BYTES_CORRUPTED",
}
DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_REQUEST_JITTER_SECONDS = 0.20
_INTERVAL_DATE = re.compile(r"^[0-9]{8}$")


@dataclass(frozen=True)
class ViewerReceiptEvidence:
    """Immutable viewer evidence for one cash disclosure receipt.

    For ``ATTACHMENT_ONLY``, ``correction_of_receipt_no`` is the exact root
    proven by DART's attachment selector.  It is intentionally not represented
    as the unknown direct economic revision corrected by that attachment.
    """

    receipt_no: str
    dcm_no: str
    dtd: str
    current_selector: str
    attachment_keys: tuple[str, ...]
    correction_of_receipt_no: str | None
    revision_root_receipt_no: str
    family_receipt_nos: tuple[str, ...]
    official_family_order: tuple[str, ...]
    revision_kind: str
    economic_body_receipt_no: str
    economic_body_dcm_no: str
    economic_body_dtd: str
    economic_main_path: str
    economic_main_content_length: int
    economic_main_sha256: str
    economic_classification: str
    common_cash_amount: float | None
    record_date: str | None
    main_path: str
    main_content_length: int
    main_sha256: str
    viewer_path: str
    viewer_content_length: int
    viewer_sha256: str
    economic_viewer_path: str
    economic_viewer_content_length: int
    economic_viewer_sha256: str


@dataclass(frozen=True)
class VerifiedViewerCorrectionSnapshot:
    base: str
    manifest_path: str
    manifest_sha256: str
    receipt_count: int
    receipt_digest: str
    receipts: tuple[ViewerReceiptEvidence, ...]


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _cash_disclosures(root: Path) -> dict[str, dict]:
    observations: list[tuple[Path, dict]] = []
    complete_receipts: set[str] = set()
    incomplete_relevant: dict[str, dict] = {}
    complete_interval_count = 0
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    for path in sorted(manifest_root.glob("from=*/to=*/disclosures_v3.json")):
        start = path.parent.parent.name.removeprefix("from=")
        end = path.parent.name.removeprefix("to=")
        if (
            _INTERVAL_DATE.fullmatch(start) is None
            or _INTERVAL_DATE.fullmatch(end) is None
            or start > end
        ):
            raise RuntimeError(f"invalid DART disclosures interval: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid DART disclosures body: {path}") from exc
        if not isinstance(payload, list):
            raise RuntimeError(f"DART disclosures must be a list: {path}")
        relevant = {
            str(row.get("rcept_no") or ""): row
            for row in payload
            if isinstance(row, dict)
            and "현금현물배당결정" in _compact(row.get("report_nm"))
            and re.fullmatch(r"\d{14}", str(row.get("rcept_no") or ""))
        }
        structured_marker = path.parent / "structured_complete_v3.json"
        document_marker = path.parent / "documents_complete_v5.json"
        if not structured_marker.is_file() or not document_marker.is_file():
            incomplete_relevant.update(relevant)
            continue
        structured_queries = {
            (event_api.slug, str(row.get("corp_code") or ""))
            for row in payload
            if isinstance(row, dict)
            and str(row.get("stock_code") or "").strip()
            and str(row.get("corp_code") or "")
            and (event_api := _event_api_for_title(row.get("report_nm")))
            is not None
        }
        document_candidates = {
            str(row.get("rcept_no") or "")
            for row in payload
            if isinstance(row, dict)
            and _needs_document(row.get("report_nm"))
            and str(row.get("rcept_no") or "")
        }
        for marker_path, count_field, expected_count in (
            (
                structured_marker,
                "query_count",
                len(structured_queries),
            ),
            (
                document_marker,
                "candidate_count",
                len(document_candidates),
            ),
        ):
            try:
                marker = json.loads(marker_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                ) from exc
            if not isinstance(marker, dict):
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                )
            try:
                observed_count = int(marker.get(count_field, -1))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                ) from exc
            if (
                marker.get("status") != "COMPLETE"
                or str(marker.get("fromdate") or "") != start
                or str(marker.get("todate") or "") != end
                or observed_count != expected_count
            ):
                raise RuntimeError(
                    "DART completion marker interval/count mismatch: "
                    f"{marker_path}"
                )
        complete_interval_count += 1
        for row in payload:
            if not isinstance(row, dict):
                continue
            receipt = str(row.get("rcept_no") or "")
            if (
                re.fullmatch(r"\d{14}", receipt)
                and "현금현물배당결정" in _compact(row.get("report_nm"))
            ):
                observations.append((path, row))
                complete_receipts.add(receipt)
    if complete_interval_count == 0:
        raise RuntimeError("no documents_complete_v5 DART cash interval")
    uncovered = sorted(set(incomplete_relevant) - complete_receipts)
    if uncovered:
        raise RuntimeError(
            "cash disclosures occur only in incomplete v5 intervals: "
            f"{uncovered[:20]}"
        )
    legacy_uncovered: set[str] = set()
    for legacy_path in sorted(
        manifest_root.glob("from=*/to=*/disclosures.json")
    ):
        try:
            legacy_rows = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid legacy DART disclosures body: {legacy_path}"
            ) from exc
        if not isinstance(legacy_rows, list):
            raise RuntimeError(
                f"legacy DART disclosures must be a list: {legacy_path}"
            )
        legacy_uncovered.update(
            str(row.get("rcept_no") or "")
            for row in legacy_rows
            if isinstance(row, dict)
            and "현금현물배당결정" in _compact(row.get("report_nm"))
            and re.fullmatch(r"\d{14}", str(row.get("rcept_no") or ""))
            and str(row.get("rcept_no") or "") not in complete_receipts
        )
    if legacy_uncovered:
        raise RuntimeError(
            "legacy cash disclosures are not authenticated by v3/v5: "
            f"{sorted(legacy_uncovered)[:20]}"
        )
    canonical, _ = canonicalize_disclosures(
        observations, audit_root=root,
    )
    return {
        receipt: row
        for receipt, (_, row) in canonical.items()
    }


def required_viewer_receipts(base: str) -> tuple[str, ...]:
    """Return every cash correction plus known damaged source bodies.

    ZIP availability does not close revision lineage: a correction can change
    DPS, record date, or cancel the event.  Every correction/withdrawal is
    therefore bound to the official viewer family and final corrected body.
    """
    root = Path(base).expanduser().resolve()
    disclosures = _cash_disclosures(root)
    required = set(KNOWN_DAMAGED_DOCUMENT_RECEIPTS).intersection(disclosures)
    unavailable_root = (
        root / "corporate_actions" / "dart" / "documents_unavailable"
    )
    unavailable: dict[str, Path] = {
        path.stem.removeprefix("rcept="): path
        for path in unavailable_root.glob("year=*/corp=*/rcept=*.xml")
        if path.is_file()
    }
    for receipt, row in disclosures.items():
        title = _compact(row.get("report_nm"))
        if (
            "자회사의주요경영사항" in title
            or "종속회사의주요경영사항" in title
        ):
            continue
        is_revision = any(
            marker in title for marker in ("정정", "철회", "취소", "부결")
        )
        if is_revision:
            required.add(receipt)
        unavailable_path = unavailable.get(receipt)
        if unavailable_path is not None:
            body = unavailable_path.read_text(encoding="utf-8", errors="replace")
            if "<status>014</status>" not in body:
                raise RuntimeError(
                    "DART unavailable evidence is not status 014: "
                    f"{unavailable_path}"
                )
    return tuple(sorted(required))


def _decode_main(payload: bytes) -> str:
    candidates: list[str] = []
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            candidates.append(payload.decode(encoding))
        except UnicodeDecodeError:
            continue
    if not candidates:
        return payload.decode("utf-8", errors="replace")
    return max(
        candidates,
        key=lambda value: (
            len(re.findall(r"[가-힣]", value)),
            -value.count("�"),
            -value.count("?"),
        ),
    )


def _parse_main_page(
    receipt: str,
    payload: bytes,
    *,
    attachment_correction: bool = False,
) -> tuple[
    str, str | None, str, tuple[str, ...], str, tuple[str, ...]
]:
    page = parse_official_dart_main_page(
        receipt,
        payload,
        expected_attachment_only=attachment_correction,
    )
    official_family_order = page.family_receipts
    revision_root = official_family_order[-1]
    if attachment_correction:
        # Cash Silver's source-receipt contract needs a non-null parent.  The
        # official att selector proves family-root membership, not a direct
        # economic-revision parent, so this field deliberately means the
        # verified family root for ATTACHMENT_ONLY rows.
        correction_of = revision_root
        economic_body_receipt = official_family_order[0]
    else:
        position = official_family_order.index(receipt)
        correction_of = (
            official_family_order[position + 1]
            if position + 1 < len(official_family_order)
            else None
        )
        economic_body_receipt = receipt
    family = tuple(sorted(
        set(official_family_order) | {receipt}, key=int,
    ))
    return (
        page.dcm_no,
        correction_of,
        revision_root,
        family,
        economic_body_receipt,
        official_family_order,
    )


def _number(value: str) -> float | None:
    rendered = value.replace(",", "").strip()
    try:
        return float(rendered)
    except ValueError:
        return None


def _parse_viewer_economic_body(
    payload: bytes,
    *,
    report_name: object,
) -> tuple[str, float | None, str | None]:
    rendered = _decode_main(payload)
    if "<table" not in rendered.lower():
        raise RuntimeError("DART viewer body has no report table")
    visible = html.unescape(re.sub(r"<[^>]+>", " ", rendered))
    visible = re.sub(r"\s+", " ", visible)
    compact_title = _compact(report_name)
    if "첨부정정" in compact_title:
        return "ATTACHMENT_CORRECTION", None, None
    if any(marker in compact_title for marker in ("철회", "취소", "부결")):
        return "NO_ECONOMIC_EVENT", None, None

    amount_matches = list(re.finditer(
        r"1\s*주당\s*배당금\s*"
        r"(?:\(\s*원\s*\)|원)?\s*"
        r"보통주(?:식)?\s*[:：]?\s*"
        r"([0-9][0-9,]*(?:\.[0-9]+)?)",
        visible,
    ))
    no_common_matches = list(re.finditer(
        r"1\s*주당\s*배당금\s*"
        r"(?:\(\s*원\s*\)|원)?\s*"
        r"보통주(?:식)?\s*[:：]?\s*"
        r"(?:-|해당\s*없음|없음|미지급)",
        visible,
    ))
    record_matches = list(re.finditer(
        r"배당\s*기준일\s*[:：]?\s*(?:"
        r"(?P<compact>(?:19|20)\d{6})"
        r"|"
        r"(?P<year>(?:19|20)\d{2})\s*(?:년|[./-])\s*"
        r"(?P<month>\d{1,2})\s*(?:월|[./-])\s*"
        r"(?P<day>\d{1,2})\s*일?"
        r")",
        visible,
    ))
    record_date = None
    if record_matches:
        match = record_matches[-1]
        compact = match.group("compact")
        if compact:
            year, month, day = (
                int(compact[:4]), int(compact[4:6]), int(compact[6:8])
            )
        else:
            year, month, day = (
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            )
        record_date = parse_dart_date(f"{year}-{month}-{day}")
        if record_date is None:
            raise RuntimeError("DART viewer record date is invalid")
    if no_common_matches and (
        not amount_matches
        or no_common_matches[-1].start() > amount_matches[-1].start()
    ):
        return "NO_COMMON_CASH_DIVIDEND", None, record_date
    if not amount_matches:
        raise RuntimeError("DART viewer economic body has no common-share DPS")
    amount = _number(amount_matches[-1].group(1))
    if amount is None:
        raise RuntimeError("DART viewer common-share DPS is invalid")
    if amount <= 0:
        return "NO_COMMON_CASH_DIVIDEND", None, record_date
    if record_date is None:
        return "POSITIVE_PENDING_RECORD_DATE", amount, None
    return "ECONOMIC_DECISION", amount, record_date


class _RateLimiter:
    def __init__(self, interval_seconds: float, jitter_seconds: float):
        self.interval_seconds = interval_seconds
        self.jitter_seconds = jitter_seconds
        self._lock = threading.Lock()
        self._next_request = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_request - now)
            self._next_request = (
                max(now, self._next_request)
                + self.interval_seconds
                + random.uniform(0.0, self.jitter_seconds)
            )
        if delay:
            time.sleep(delay)


def _get(
    url: str,
    *,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
) -> bytes:
    last_error: Exception | None = None
    headers = {"User-Agent": "TeamAlpha-data dividend-lineage audit/1.0"}
    for attempt in range(tries):
        try:
            rate_limiter.wait()
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(min(60.0, max(0.0, float(retry_after))))
                    except ValueError:
                        pass
            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"DART viewer server error {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            payload = response.content
            if len(payload) < 200:
                raise RuntimeError(f"unexpectedly short DART viewer body: {url}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(
                    min(60.0, 2.0 * (2**attempt))
                    + random.uniform(0.0, 1.0)
                )
    raise RuntimeError(f"DART viewer request failed: {url}") from last_error


def _receipt_main_path(root: Path, receipt: str) -> Path:
    directory = (
        root / "corporate_actions" / "dart" / "viewer_corrections"
        / f"receipt={receipt}"
    )
    return directory / "main.html"


def _viewer_evidence_path(main_path: Path, *, prefix: str, dtd: str) -> Path:
    if prefix not in {"viewer", "economic_viewer"} or re.fullmatch(
        r"[0-9A-Za-z_.-]+", dtd,
    ) is None:
        raise RuntimeError("invalid DART viewer evidence path identity")
    return main_path.with_name(f"{prefix}.dtd={dtd}.html")


def _fetch_one(
    root: Path,
    receipt: str,
    *,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
    report_name: object,
    report_names: dict[str, object] | None = None,
) -> ViewerReceiptEvidence:
    attachment_correction = "첨부정정" in _compact(report_name)
    main_path = _receipt_main_path(root, receipt)
    main_url = f"{MAIN_URL}?{urlencode({'rcpNo': receipt})}"
    if main_path.is_file():
        main_payload = main_path.read_bytes()
    else:
        main_payload = _get(
            main_url,
            tries=tries,
            timeout=timeout,
            rate_limiter=rate_limiter,
        )
        _atomic_write(main_path, main_payload)
    (
        dcm_no,
        correction_of,
        revision_root,
        family,
        economic_body_receipt,
        official_family_order,
    ) = _parse_main_page(
        receipt,
        main_payload,
        attachment_correction=attachment_correction,
    )
    source_page = parse_official_dart_main_page(
        receipt,
        main_payload,
        expected_attachment_only=attachment_correction,
    )
    dtd = source_page.dtd
    current_selector = source_page.current_selector
    attachment_keys = source_page.attachment_keys
    viewer_path = _viewer_evidence_path(
        main_path, prefix="viewer", dtd=dtd,
    )
    viewer_url = official_dart_viewer_url(receipt, dcm_no, dtd)
    if viewer_path.is_file():
        viewer_payload = viewer_path.read_bytes()
    else:
        viewer_payload = _get(
            viewer_url,
            tries=tries,
            timeout=timeout,
            rate_limiter=rate_limiter,
        )
        _atomic_write(viewer_path, viewer_payload)
    if attachment_correction:
        economic_main_path = main_path.with_name(
            f"economic_main.receipt={economic_body_receipt}.html"
        )
        if economic_main_path.is_file():
            economic_main_payload = economic_main_path.read_bytes()
        else:
            economic_main_url = (
                f"{MAIN_URL}?"
                f"{urlencode({'rcpNo': economic_body_receipt})}"
            )
            economic_main_payload = _get(
                economic_main_url,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
            )
            _atomic_write(economic_main_path, economic_main_payload)
        economic_page = parse_official_dart_main_page(
            economic_body_receipt,
            economic_main_payload,
            expected_attachment_only=False,
        )
        economic_body_dcm = economic_page.dcm_no
        economic_body_dtd = economic_page.dtd
        if (
            report_names is None
            or economic_body_receipt not in report_names
        ):
            raise RuntimeError(
                "DART attachment economic disclosure title is unavailable: "
                f"source={receipt} economic={economic_body_receipt}"
            )
        economic_report_name = report_names[economic_body_receipt]
        if (
            economic_page.receipt_no != economic_body_receipt
            or economic_page.current_selector != "FAMILY"
            or economic_page.family_receipts != source_page.family_receipts
            or economic_page.attachment_keys != source_page.attachment_keys
        ):
            raise RuntimeError(
                "DART attachment/economic main selectors disagree: "
                f"source={receipt} economic={economic_body_receipt}"
            )
        economic_viewer_path = _viewer_evidence_path(
            main_path, prefix="economic_viewer", dtd=economic_body_dtd,
        )
        if economic_viewer_path.is_file():
            economic_viewer_payload = economic_viewer_path.read_bytes()
        else:
            economic_viewer_url = official_dart_viewer_url(
                economic_body_receipt,
                economic_body_dcm,
                economic_body_dtd,
            )
            economic_viewer_payload = _get(
                economic_viewer_url,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
            )
            _atomic_write(economic_viewer_path, economic_viewer_payload)
    else:
        economic_main_path = main_path
        economic_main_payload = main_payload
        economic_body_dcm = dcm_no
        economic_body_dtd = dtd
        economic_report_name = report_name
        economic_viewer_path = viewer_path
        economic_viewer_payload = viewer_payload
    try:
        source_classification, _, _ = _parse_viewer_economic_body(
            viewer_payload,
            report_name=report_name,
        )
        if attachment_correction and source_classification != "ATTACHMENT_CORRECTION":
            raise RuntimeError("attachment receipt was not classified as attachment")
        economic_classification, common_cash_amount, record_date = (
            _parse_viewer_economic_body(
                economic_viewer_payload,
                report_name=economic_report_name,
            )
        )
    except RuntimeError as exc:
        raise RuntimeError(
            f"DART viewer economic evidence invalid: receipt={receipt}: {exc}"
        ) from exc
    return ViewerReceiptEvidence(
        receipt_no=receipt,
        dcm_no=dcm_no,
        dtd=dtd,
        current_selector=current_selector,
        attachment_keys=attachment_keys,
        correction_of_receipt_no=correction_of,
        revision_root_receipt_no=revision_root,
        family_receipt_nos=family,
        official_family_order=official_family_order,
        revision_kind=(
            "ATTACHMENT_ONLY" if attachment_correction else "ECONOMIC_REVISION"
        ),
        economic_body_receipt_no=economic_body_receipt,
        economic_body_dcm_no=economic_body_dcm,
        economic_body_dtd=economic_body_dtd,
        economic_main_path=economic_main_path.relative_to(root).as_posix(),
        economic_main_content_length=len(economic_main_payload),
        economic_main_sha256=_sha256_bytes(economic_main_payload),
        economic_classification=economic_classification,
        common_cash_amount=common_cash_amount,
        record_date=record_date,
        main_path=main_path.relative_to(root).as_posix(),
        main_content_length=len(main_payload),
        main_sha256=_sha256_bytes(main_payload),
        viewer_path=viewer_path.relative_to(root).as_posix(),
        viewer_content_length=len(viewer_payload),
        viewer_sha256=_sha256_bytes(viewer_payload),
        economic_viewer_path=economic_viewer_path.relative_to(root).as_posix(),
        economic_viewer_content_length=len(economic_viewer_payload),
        economic_viewer_sha256=_sha256_bytes(economic_viewer_payload),
    )


def _receipt_digest(receipts: Iterable[ViewerReceiptEvidence]) -> str:
    rendered = json.dumps(
        [asdict(item) for item in receipts],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


_TERMINAL_ECONOMIC_CLASSIFICATIONS = frozenset({
    "ECONOMIC_DECISION",
    "NO_COMMON_CASH_DIVIDEND",
    "NO_ECONOMIC_EVENT",
})


def _validate_terminal_families(
    receipts: Iterable[ViewerReceiptEvidence],
    disclosures: dict[str, dict],
) -> None:
    """Allow incomplete intermediate revisions, never an incomplete terminal.

    The official ``family`` selector provides the order.  Receipt-number
    magnitude is never used as a chronology proxy.  Attachment-only receipts
    are source evidence and resolve to the official economic terminal.
    """
    rows = tuple(receipts)
    by_root: dict[str, list[ViewerReceiptEvidence]] = {}
    for item in rows:
        by_root.setdefault(item.revision_root_receipt_no, []).append(item)
        if (
            not item.official_family_order
            or len(item.official_family_order)
            != len(set(item.official_family_order))
            or not set(item.official_family_order).issubset(
                item.family_receipt_nos
            )
            or item.revision_root_receipt_no
            != item.official_family_order[-1]
            or (
                item.revision_kind == "ATTACHMENT_ONLY"
                and (
                    item.current_selector != "ATTACHMENT"
                    or item.correction_of_receipt_no
                    != item.revision_root_receipt_no
                )
            )
            or (
                item.revision_kind == "ECONOMIC_REVISION"
                and item.current_selector != "FAMILY"
            )
        ):
            raise RuntimeError(
                f"DART official family order is invalid: {item.receipt_no}"
            )

    for root, group in by_root.items():
        official_orders = {item.official_family_order for item in group}
        attachment_orders = {item.attachment_keys for item in group}
        if len(official_orders) != 1 or len(attachment_orders) != 1:
            raise RuntimeError(
                f"DART official family/attachment selectors disagree: {root}"
            )
        official_order = next(iter(official_orders))
        terminal_receipt = official_order[0]
        terminal_candidates = [
            item for item in group
            if item.economic_body_receipt_no == terminal_receipt
        ]
        terminal_values = {
            (
                item.economic_classification,
                item.common_cash_amount,
                item.record_date,
            )
            for item in terminal_candidates
        }
        if len(terminal_values) != 1:
            raise RuntimeError(
                "DART terminal economic receipt evidence is missing/ambiguous: "
                f"root={root} terminal={terminal_receipt}"
            )
        terminal_classification, terminal_amount, terminal_date = next(
            iter(terminal_values)
        )
        if terminal_classification not in _TERMINAL_ECONOMIC_CLASSIFICATIONS:
            raise RuntimeError(
                "DART terminal economic revision is incomplete: "
                f"root={root} terminal={terminal_receipt} "
                f"classification={terminal_classification}"
            )
        if terminal_classification == "ECONOMIC_DECISION" and (
            terminal_amount is None or terminal_amount <= 0
            or terminal_date is None
        ):
            raise RuntimeError(
                f"DART terminal positive decision is incomplete: {terminal_receipt}"
            )
        for item in group:
            if item.economic_classification == "POSITIVE_PENDING_RECORD_DATE":
                if (
                    item.receipt_no not in official_order
                    or official_order.index(terminal_receipt)
                    >= official_order.index(item.receipt_no)
                ):
                    raise RuntimeError(
                        "DART incomplete positive receipt is not superseded by "
                        f"the official family terminal: {item.receipt_no}"
                    )


def _pending_terminal_dependencies(
    receipts: Iterable[ViewerReceiptEvidence],
    disclosures: dict[str, dict],
) -> tuple[str, ...]:
    """Return economic terminals needed to close exact official families."""
    rows = tuple(receipts)
    economic_bodies = {item.economic_body_receipt_no for item in rows}
    dependencies: set[str] = set()
    for item in rows:
        if not item.official_family_order:
            raise RuntimeError(
                f"DART pending family has no economic member: {item.receipt_no}"
            )
        terminal = item.official_family_order[0]
        if terminal not in disclosures:
            raise RuntimeError(
                "DART pending family terminal is absent from the immutable "
                f"disclosure snapshot: {terminal}"
            )
        if terminal not in economic_bodies:
            dependencies.add(terminal)
    return tuple(sorted(dependencies))


def collect_viewer_corrections(
    base: str,
    *,
    extra_receipts: Iterable[str] = (),
    apply: bool = False,
    workers: int = 4,
    tries: int = 4,
    timeout: float = 30.0,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> VerifiedViewerCorrectionSnapshot | dict:
    root = Path(base).expanduser().resolve()
    disclosures = _cash_disclosures(root)
    required = set(required_viewer_receipts(str(root)))
    required.update(str(value) for value in extra_receipts)
    invalid = sorted(
        receipt for receipt in required
        if not re.fullmatch(r"\d{14}", receipt) or receipt not in disclosures
    )
    if invalid:
        raise RuntimeError(
            f"viewer fallback receipts absent from cash disclosures: {invalid[:20]}"
        )
    ordered = tuple(sorted(required))
    if not apply:
        return {
            "apply": False,
            "schema_version": SCHEMA_VERSION,
            "source_contract": SOURCE_CONTRACT,
            "family_order": FAMILY_ORDER_CONTRACT,
            "attachment_parent_contract": ATTACHMENT_PARENT_CONTRACT,
            "required_receipt_count": len(ordered),
            "required_receipts": list(ordered),
        }
    if workers < 1 or workers > 8:
        raise ValueError("workers must be in [1, 8]")
    if request_interval_seconds <= 0:
        raise ValueError("request_interval_seconds must be positive")
    evidence: list[ViewerReceiptEvidence] = []
    report_names = {
        key: row.get("report_nm") for key, row in disclosures.items()
    }
    rate_limiter = _RateLimiter(
        request_interval_seconds,
        DEFAULT_REQUEST_JITTER_SECONDS,
    )
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = {
            executor.submit(
                _fetch_one,
                root,
                receipt,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
                report_name=disclosures[receipt].get("report_nm"),
                report_names=report_names,
            ): receipt
            for receipt in ordered
        }
    try:
        for completed, future in enumerate(as_completed(futures), start=1):
            evidence.append(future.result())
            if completed % 50 == 0 or completed == len(futures):
                print(
                    "[dart-viewer-corrections] "
                    f"downloaded={completed}/{len(futures)}",
                    flush=True,
                )
    except BaseException:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    # An intermediate correction can point to a later plain (non-correction)
    # terminal receipt, which is not in the initial correction-only set.  Add
    # those exact official-family dependencies and repeat until closure.
    while dependencies := _pending_terminal_dependencies(
        evidence, disclosures,
    ):
        dependency_executor = ThreadPoolExecutor(max_workers=workers)
        dependency_futures = {
            dependency_executor.submit(
                _fetch_one,
                root,
                receipt,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
                report_name=disclosures[receipt].get("report_nm"),
                report_names=report_names,
            ): receipt
            for receipt in dependencies
        }
        try:
            evidence.extend(
                future.result() for future in as_completed(dependency_futures)
            )
        except BaseException:
            for future in dependency_futures:
                future.cancel()
            dependency_executor.shutdown(wait=False, cancel_futures=True)
            raise
        else:
            dependency_executor.shutdown(wait=True)
        required.update(dependencies)
    ordered = tuple(sorted(required))
    evidence.sort(key=lambda item: item.receipt_no)
    _validate_terminal_families(evidence, disclosures)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_contract": SOURCE_CONTRACT,
        "family_order": FAMILY_ORDER_CONTRACT,
        "attachment_parent_contract": ATTACHMENT_PARENT_CONTRACT,
        "complete": True,
        "required_receipts": list(ordered),
        "receipt_count": len(evidence),
        "receipt_digest": _receipt_digest(evidence),
        "receipts": [asdict(item) for item in evidence],
    }
    manifest = root / MANIFEST_RELATIVE_PATH
    _atomic_write(
        manifest,
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    return verify_viewer_corrections(str(root), required_receipts=ordered)


def verify_viewer_corrections(
    base: str,
    *,
    required_receipts: Iterable[str] | None = None,
) -> VerifiedViewerCorrectionSnapshot:
    root = Path(base).expanduser().resolve()
    manifest = root / MANIFEST_RELATIVE_PATH
    try:
        raw = manifest.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"missing/invalid DART viewer correction manifest: {manifest}"
        ) from exc
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if raw != canonical:
        raise RuntimeError("DART viewer correction manifest is not canonical")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("unsupported DART viewer correction schema")
    if (
        payload.get("source_contract") != SOURCE_CONTRACT
        or payload.get("family_order") != FAMILY_ORDER_CONTRACT
        or payload.get("attachment_parent_contract")
        != ATTACHMENT_PARENT_CONTRACT
    ):
        raise RuntimeError("unsupported DART viewer correction provenance")
    if payload.get("complete") is not True:
        raise RuntimeError("DART viewer correction snapshot is incomplete")
    declared_required = tuple(sorted(set(
        str(value) for value in payload.get("required_receipts") or []
    )))
    expected = tuple(sorted(set(
        required_viewer_receipts(str(root))
        if required_receipts is None else (str(value) for value in required_receipts)
    )))
    if not set(expected).issubset(declared_required):
        missing = sorted(set(expected) - set(declared_required))
        raise RuntimeError(
            f"DART viewer correction receipts are missing: {missing[:20]}"
        )
    rows = payload.get("receipts")
    if not isinstance(rows, list):
        raise RuntimeError("DART viewer correction receipts must be a list")
    receipts: list[ViewerReceiptEvidence] = []
    seen: set[str] = set()
    disclosures = _cash_disclosures(root)
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("invalid DART viewer correction receipt entry")
        evidence = ViewerReceiptEvidence(
            receipt_no=str(row.get("receipt_no") or ""),
            dcm_no=str(row.get("dcm_no") or ""),
            dtd=str(row.get("dtd") or ""),
            current_selector=str(row.get("current_selector") or ""),
            attachment_keys=tuple(
                str(value) for value in row.get("attachment_keys") or []
            ),
            correction_of_receipt_no=(
                str(row["correction_of_receipt_no"])
                if row.get("correction_of_receipt_no") else None
            ),
            revision_root_receipt_no=str(
                row.get("revision_root_receipt_no") or ""
            ),
            family_receipt_nos=tuple(
                str(value) for value in row.get("family_receipt_nos") or []
            ),
            official_family_order=tuple(
                str(value) for value in row.get("official_family_order") or []
            ),
            revision_kind=str(row.get("revision_kind") or ""),
            economic_body_receipt_no=str(
                row.get("economic_body_receipt_no") or ""
            ),
            economic_body_dcm_no=str(
                row.get("economic_body_dcm_no") or ""
            ),
            economic_body_dtd=str(row.get("economic_body_dtd") or ""),
            economic_main_path=str(row.get("economic_main_path") or ""),
            economic_main_content_length=int(
                row.get("economic_main_content_length", -1)
            ),
            economic_main_sha256=str(
                row.get("economic_main_sha256") or ""
            ),
            economic_classification=str(
                row.get("economic_classification") or ""
            ),
            common_cash_amount=(
                float(row["common_cash_amount"])
                if row.get("common_cash_amount") is not None else None
            ),
            record_date=(
                str(row["record_date"]) if row.get("record_date") else None
            ),
            main_path=str(row.get("main_path") or ""),
            main_content_length=int(row.get("main_content_length", -1)),
            main_sha256=str(row.get("main_sha256") or ""),
            viewer_path=str(row.get("viewer_path") or ""),
            viewer_content_length=int(row.get("viewer_content_length", -1)),
            viewer_sha256=str(row.get("viewer_sha256") or ""),
            economic_viewer_path=str(
                row.get("economic_viewer_path") or ""
            ),
            economic_viewer_content_length=int(
                row.get("economic_viewer_content_length", -1)
            ),
            economic_viewer_sha256=str(
                row.get("economic_viewer_sha256") or ""
            ),
        )
        if evidence.receipt_no in seen:
            raise RuntimeError("duplicate DART viewer correction receipt")
        seen.add(evidence.receipt_no)
        if (
            not re.fullmatch(r"\d{14}", evidence.receipt_no)
            or not evidence.dcm_no.isdigit()
            or re.fullmatch(r"[0-9A-Za-z_.-]+", evidence.dtd) is None
            or evidence.current_selector not in {"FAMILY", "ATTACHMENT"}
            or len(evidence.attachment_keys)
            != len(set(evidence.attachment_keys))
            or any(
                re.fullmatch(r"\d{14}:\d+", value) is None
                for value in evidence.attachment_keys
            )
            or not re.fullmatch(
                r"\d{14}", evidence.economic_body_receipt_no
            )
            or not evidence.economic_body_dcm_no.isdigit()
            or re.fullmatch(
                r"[0-9A-Za-z_.-]+", evidence.economic_body_dtd,
            ) is None
            or evidence.revision_kind not in {
                "ATTACHMENT_ONLY", "ECONOMIC_REVISION",
            }
        ):
            raise RuntimeError("invalid DART viewer receipt identity")
        for relative, length, digest in (
            (
                evidence.main_path,
                evidence.main_content_length,
                evidence.main_sha256,
            ),
            (
                evidence.viewer_path,
                evidence.viewer_content_length,
                evidence.viewer_sha256,
            ),
            (
                evidence.economic_main_path,
                evidence.economic_main_content_length,
                evidence.economic_main_sha256,
            ),
            (
                evidence.economic_viewer_path,
                evidence.economic_viewer_content_length,
                evidence.economic_viewer_sha256,
            ),
        ):
            path = (root / relative).resolve()
            if root not in path.parents or not path.is_file():
                raise RuntimeError(
                    f"DART viewer evidence path is missing/unsafe: {relative}"
                )
            if path.stat().st_size != length or _sha256_path(path) != digest:
                raise RuntimeError(
                    f"DART viewer evidence SHA/content length mismatch: {relative}"
                )
        report_name = (disclosures.get(evidence.receipt_no) or {}).get(
            "report_nm"
        )
        title_is_attachment = "첨부정정" in _compact(report_name)
        expected_main = _receipt_main_path(root, evidence.receipt_no)
        expected_viewer = _viewer_evidence_path(
            expected_main, prefix="viewer", dtd=evidence.dtd,
        )
        expected_economic_main = (
            expected_main.with_name(
                "economic_main.receipt="
                f"{evidence.economic_body_receipt_no}.html"
            )
            if title_is_attachment else expected_main
        )
        expected_economic_viewer = (
            _viewer_evidence_path(
                expected_main,
                prefix="economic_viewer",
                dtd=evidence.economic_body_dtd,
            )
            if title_is_attachment else expected_viewer
        )
        if (
            evidence.main_path != expected_main.relative_to(root).as_posix()
            or evidence.viewer_path
            != expected_viewer.relative_to(root).as_posix()
            or evidence.economic_main_path
            != expected_economic_main.relative_to(root).as_posix()
            or evidence.economic_viewer_path
            != expected_economic_viewer.relative_to(root).as_posix()
        ):
            raise RuntimeError(
                "DART viewer evidence path is non-canonical: "
                f"{evidence.receipt_no}"
            )
        source_page = parse_official_dart_main_page(
            evidence.receipt_no,
            (root / evidence.main_path).read_bytes(),
            expected_attachment_only=title_is_attachment,
        )
        (
            parsed_dcm,
            parsed_origin,
            parsed_root,
            parsed_family,
            parsed_economic_receipt,
            parsed_official_order,
        ) = _parse_main_page(
            evidence.receipt_no,
            (root / evidence.main_path).read_bytes(),
            attachment_correction=title_is_attachment,
        )
        if (
            parsed_dcm != evidence.dcm_no
            or parsed_origin != evidence.correction_of_receipt_no
            or parsed_root != evidence.revision_root_receipt_no
            or parsed_family != evidence.family_receipt_nos
            or parsed_economic_receipt != evidence.economic_body_receipt_no
            or parsed_official_order != evidence.official_family_order
            or source_page.dtd != evidence.dtd
            or source_page.current_selector != evidence.current_selector
            or source_page.attachment_keys != evidence.attachment_keys
        ):
            raise RuntimeError(
                f"DART viewer lineage changed: {evidence.receipt_no}"
            )
        economic_page = parse_official_dart_main_page(
            evidence.economic_body_receipt_no,
            (root / evidence.economic_main_path).read_bytes(),
            expected_attachment_only=False,
        )
        if (
            economic_page.dcm_no != evidence.economic_body_dcm_no
            or economic_page.dtd != evidence.economic_body_dtd
            or economic_page.current_selector != "FAMILY"
            or economic_page.family_receipts
            != evidence.official_family_order
            or economic_page.attachment_keys != evidence.attachment_keys
        ):
            raise RuntimeError(
                f"DART viewer economic main changed: {evidence.receipt_no}"
            )
        if title_is_attachment != (
            evidence.revision_kind == "ATTACHMENT_ONLY"
        ):
            raise RuntimeError(
                f"DART viewer revision kind/title mismatch: "
                f"{evidence.receipt_no}"
            )
        source_classification = _parse_viewer_economic_body(
            (root / evidence.viewer_path).read_bytes(),
            report_name=report_name,
        )[0]
        expected_source_classification = (
            "ATTACHMENT_CORRECTION"
            if evidence.revision_kind == "ATTACHMENT_ONLY"
            else evidence.economic_classification
        )
        if source_classification != expected_source_classification:
            raise RuntimeError(
                f"DART viewer source classification changed: "
                f"{evidence.receipt_no}"
            )
        economic_disclosure = disclosures.get(
            evidence.economic_body_receipt_no
        )
        if economic_disclosure is None:
            raise RuntimeError(
                "DART viewer economic disclosure disappeared: "
                f"{evidence.economic_body_receipt_no}"
            )
        economic = _parse_viewer_economic_body(
            (root / evidence.economic_viewer_path).read_bytes(),
            report_name=economic_disclosure.get("report_nm"),
        )
        if economic != (
            evidence.economic_classification,
            evidence.common_cash_amount,
            evidence.record_date,
        ):
            raise RuntimeError(
                f"DART viewer economic evidence changed: {evidence.receipt_no}"
            )
        receipts.append(evidence)
    receipts.sort(key=lambda item: item.receipt_no)
    _validate_terminal_families(receipts, disclosures)
    if set(declared_required) != {item.receipt_no for item in receipts}:
        raise RuntimeError("DART viewer manifest receipt set mismatch")
    digest = _receipt_digest(receipts)
    if payload.get("receipt_digest") != digest:
        raise RuntimeError("DART viewer correction receipt digest mismatch")
    if int(payload.get("receipt_count", -1)) != len(receipts):
        raise RuntimeError("DART viewer correction receipt count mismatch")
    return VerifiedViewerCorrectionSnapshot(
        base=str(root),
        manifest_path=str(manifest),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        receipt_count=len(receipts),
        receipt_digest=digest,
        receipts=tuple(receipts),
    )


def evidence_by_receipt(base: str) -> dict[str, ViewerReceiptEvidence]:
    verified = verify_viewer_corrections(base)
    return {item.receipt_no: item for item in verified.receipts}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--receipt", action="append", default=[])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = collect_viewer_corrections(
        args.base,
        extra_receipts=args.receipt,
        workers=args.workers,
        apply=args.apply,
    )
    if isinstance(result, dict):
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
