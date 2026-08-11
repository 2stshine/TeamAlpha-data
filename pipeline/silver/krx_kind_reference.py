"""Content-addressed KRX KIND support for cash-adjustment scale evidence.

The reviewed cross-class stock-dividend decision and downloaded reference-price
notices share one immutable manifest.  Reference notices are accepted only
when the official external URL, issuer, security class, applied date, reason,
and labelled reference price all agree with the body.  In particular, a
preferred-share notice cannot corroborate a common-share price row.
"""
from __future__ import annotations

import hashlib
import html
import json
import math
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from html.parser import HTMLParser
from typing import Sequence
from urllib.parse import parse_qs, urlparse

from pipeline.silver import corporate_actions


KIND_SUPPORT_MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/krx/kind/cash_adjustment_scale_support.json"
)
KIND_REQUEST_OBJECT_ROOT = Path("corporate_actions/krx/kind/request_objects")
KIND_BODY_OBJECT_ROOT = Path("corporate_actions/krx/kind/body_objects")
KIND_IDENTITY_OBJECT_ROOT = Path("corporate_actions/krx/kind/identity_objects")
KIND_CONTENTS_OBJECT_ROOT = Path("corporate_actions/krx/kind/contents_objects")
KIND_SUPPORT_SCHEMA = "krx_kind_cash_adjustment_support_v3"
KIND_REQUEST_SCHEMA = "krx_kind_cash_adjustment_requests_v2"
KIND_COMPONENT_REQUEST_SCHEMA = "krx_kind_cash_adjustment_component_requests_v1"
KIND_REFERENCE_REPORT_NAME_99311 = "배당락 기준가격 안내"
KIND_REFERENCE_REPORT_NAME_70767 = "배당락"
KIND_COMPONENT_REPORT_NAME_61474 = "주식배당 결정"

_SHA = re.compile(r"^[0-9a-f]{64}$")
_TICKER = re.compile(r"^[0-9A-Z]{6}$")
_REFERENCE_ACTION_TYPES = frozenset({
    "ex_dividend", "rights_detachment", "combined_detachment",
})
_SUPPORT_ROLES = frozenset({"ADJUSTMENT_COMPONENT", "CORROBORATION"})
_SUPPORT_FIELDS = frozenset({
    "ticker", "issuer_name", "source_form_id", "source_url",
    "target_cash_receipt_no", "target_adjustment_date",
    "identity_source_url", "identity_body_path",
    "identity_body_content_length", "identity_body_sha256",
    "contents_source_url", "contents_body_path",
    "contents_body_content_length", "contents_body_sha256",
    "terminal_acceptance_no",
    "support_action_source", "support_action_key", "support_action_type",
    "support_semantic_role", "support_action_body_path",
    "support_action_body_content_length", "support_action_body_sha256",
    "support_announcement_date", "support_ex_date", "support_record_date",
    "support_ratio_numerator", "support_ratio_denominator",
    "support_entitlement_security_class",
    "support_distributed_security_class", "support_expected_price_factor",
    "support_reference_price", "support_reason", "support_report_name",
    "support_action_scope",
})


@dataclass(frozen=True)
class KindReferenceNotice:
    issuer_name: str
    security_class: str
    reference_price: float
    effective_date: date
    reason: str
    action_type: str
    form_id: str
    ticker: str | None


@dataclass(frozen=True)
class KindIdentityReceipt:
    acceptance_no: str
    issuer_name: str
    ticker: str
    selected_document_no: str
    selected_report_name: str


@dataclass(frozen=True)
class KindStockDividendComponent:
    decision_date: date
    record_date: date
    ratio_numerator: float
    ratio_denominator: float
    entitlement_security_class: str
    distributed_security_class: str
    report_name: str


@dataclass(frozen=True)
class _TableCell:
    text: str
    rowspan: int
    colspan: int


class _KindTableParser(HTMLParser):
    """Extract table cells while preserving their rowspan/colspan contract."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[_TableCell]]] = []
        self._table_stack: list[list[list[_TableCell]]] = []
        self._row: list[_TableCell] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_rowspan = 1
        self._cell_colspan = 1

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        name = tag.lower()
        if name == "table":
            self._table_stack.append([])
        elif name == "tr" and self._table_stack:
            self._row = []
        elif name in {"td", "th"} and self._row is not None:
            attributes = {key.lower(): value for key, value in attrs}
            try:
                self._cell_rowspan = int(attributes.get("rowspan") or 1)
                self._cell_colspan = int(attributes.get("colspan") or 1)
            except ValueError as exc:
                raise RuntimeError("KIND table span is invalid") from exc
            if self._cell_rowspan < 1 or self._cell_colspan < 1:
                raise RuntimeError("KIND table span is invalid")
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        name = tag.lower()
        if name in {"td", "th"} and self._cell_parts is not None:
            assert self._row is not None
            value = re.sub(
                r"\s+", " ", html.unescape(" ".join(self._cell_parts)),
            ).strip()
            self._row.append(_TableCell(
                text=value,
                rowspan=self._cell_rowspan,
                colspan=self._cell_colspan,
            ))
            self._cell_parts = None
        elif name == "tr" and self._row is not None and self._table_stack:
            if self._row:
                self._table_stack[-1].append(self._row)
            self._row = None
        elif name == "table" and self._table_stack:
            completed = self._table_stack.pop()
            if completed:
                self.tables.append(completed)


def _expanded_kind_tables(payload: bytes) -> list[list[list[str]]]:
    parser = _KindTableParser()
    parser.feed(corporate_actions._decode_document(payload))
    expanded_tables: list[list[list[str]]] = []
    for raw_table in parser.tables:
        active: dict[int, tuple[int, str]] = {}
        expanded: list[list[str]] = []
        for raw_row in raw_table:
            grid = {column: value for column, (_, value) in active.items()}
            next_active = {
                column: (remaining - 1, value)
                for column, (remaining, value) in active.items()
                if remaining > 1
            }
            column = 0
            for cell in raw_row:
                while column in grid:
                    column += 1
                for offset in range(cell.colspan):
                    target = column + offset
                    if target in grid:
                        raise RuntimeError("KIND table span overlaps another cell")
                    grid[target] = cell.text
                    if cell.rowspan > 1:
                        next_active[target] = (cell.rowspan - 1, cell.text)
                column += cell.colspan
            width = max(grid, default=-1) + 1
            expanded.append([grid.get(index, "") for index in range(width)])
            active = next_active
        if active:
            raise RuntimeError("KIND table rowspan extends past the table")
        expanded_tables.append(expanded)
    return expanded_tables


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _kind_external_url_parts(value: object) -> tuple[re.Match[str], str]:
    url = str(value or "").strip()
    parsed = urlparse(url)
    match = re.fullmatch(
        r"/external/(\d{4})/(\d{2})/(\d{2})/(\d{6})/"
        r"(\d{14})/(\d{5})\.htm",
        parsed.path,
    )
    if (
        parsed.scheme != "https"
        or parsed.netloc != "kind.krx.co.kr"
        or parsed.query
        or parsed.fragment
        or match is None
    ):
        raise RuntimeError("KIND source URL is not one exact official external body")
    return match, url


def kind_url_identity(value: object) -> tuple[str, date, str]:
    """Return acceptance number, date, and form for one exact body URL."""
    match, _ = _kind_external_url_parts(value)
    year, month, day, sequence, document_no, form_id = match.groups()
    announcement = date(int(year), int(month), int(day))
    receipt = f"{year}{month}{day}{sequence}"
    if not document_no.startswith(f"{year}{month}{day}"):
        raise RuntimeError("KIND document/date identity mismatch")
    if form_id not in {"99311", "70767"}:
        raise RuntimeError("KIND reference notice form is unsupported")
    return receipt, announcement, form_id


def kind_url_document_no(value: object) -> str:
    match, _ = _kind_external_url_parts(value)
    return match.group(5)


def kind_component_url_identity(value: object) -> tuple[str, date, str]:
    match, _ = _kind_external_url_parts(value)
    year, month, day, sequence, document_no, form_id = match.groups()
    if form_id != "61474":
        raise RuntimeError("KIND component body must use form 61474")
    return (
        f"{year}{month}{day}{sequence}",
        date(int(year), int(month), int(day)),
        document_no,
    )


def kind_identity_url_acceptance(value: object) -> str:
    url = str(value or "").strip()
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    allowed = {"method", "acptno", "docno", "viewerhost", "viewerport"}
    if (
        parsed.scheme != "https"
        or parsed.netloc != "kind.krx.co.kr"
        or parsed.path != "/common/disclsviewer.do"
        or parsed.fragment
        or set(query) - allowed
        or query.get("method") != ["search"]
        or len(query.get("acptno", [])) != 1
        or re.fullmatch(r"[0-9]{14}", query["acptno"][0]) is None
        or any(len(values) != 1 for values in query.values())
    ):
        raise RuntimeError("KIND identity URL is not one exact official viewer")
    return query["acptno"][0]


def kind_contents_url_document_no(value: object) -> str:
    url = str(value or "").strip()
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "kind.krx.co.kr"
        or parsed.path != "/common/disclsviewer.do"
        or parsed.fragment
        or set(query) != {"method", "docNo"}
        or query.get("method") != ["searchContents"]
        or len(query.get("docNo", [])) != 1
        or re.fullmatch(r"[0-9]{14}", query["docNo"][0]) is None
    ):
        raise RuntimeError("KIND contents URL is not one exact official viewer")
    return query["docNo"][0]


def parse_kind_contents_body_url(payload: bytes) -> str:
    decoded = corporate_actions._decode_document(payload)
    matches = re.findall(
        r"parent\.setPath\(\s*['\"]['\"]\s*,\s*['\"]([^'\"]+)['\"]",
        decoded,
    )
    matches = list(dict.fromkeys(html.unescape(value) for value in matches))
    if len(matches) != 1:
        raise RuntimeError("KIND contents selected body URL is ambiguous")
    _kind_external_url_parts(matches[0])
    return matches[0]


class _KindIdentityParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.acceptances: list[str] = []
        self.headings: list[str] = []
        self.selected: list[tuple[str, str]] = []
        self._heading_parts: list[str] | None = None
        self._option_parts: list[str] | None = None
        self._option_value: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        name = tag.lower()
        attributes = {key.lower(): value for key, value in attrs}
        if name == "input" and (
            str(attributes.get("id") or "").lower() == "acptno"
            or str(attributes.get("name") or "").lower() == "acptno"
        ):
            value = str(attributes.get("value") or "")
            if value:
                self.acceptances.append(value)
        elif name == "h1":
            self._heading_parts = []
        elif name == "option" and "selected" in attributes:
            self._option_parts = []
            self._option_value = str(attributes.get("value") or "")

    def handle_data(self, data: str) -> None:
        if self._heading_parts is not None:
            self._heading_parts.append(data)
        if self._option_parts is not None:
            self._option_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        name = tag.lower()
        if name == "h1" and self._heading_parts is not None:
            self.headings.append(re.sub(r"\s+", " ", " ".join(
                self._heading_parts
            )).strip())
            self._heading_parts = None
        elif name == "option" and self._option_parts is not None:
            text = re.sub(r"\s+", " ", " ".join(self._option_parts)).strip()
            self.selected.append((str(self._option_value or ""), text))
            self._option_parts = None
            self._option_value = None


def parse_kind_identity_receipt(payload: bytes) -> KindIdentityReceipt:
    parser = _KindIdentityParser()
    parser.feed(corporate_actions._decode_document(payload))
    acceptances = sorted(set(parser.acceptances))
    headings = sorted(set(parser.headings))
    selected = list(dict.fromkeys(parser.selected))
    if len(acceptances) != 1 or len(selected) != 1:
        raise RuntimeError("KIND identity acceptance/selection is ambiguous")
    issuer_tickers: list[tuple[str, str]] = []
    for heading in headings:
        match = re.fullmatch(r"\s*(.*?)\s*\(([0-9A-Z]{6})\)\s*", heading)
        if match is not None:
            issuer_tickers.append((match.group(1).strip(), match.group(2)))
    issuer_tickers = list(dict.fromkeys(issuer_tickers))
    if len(issuer_tickers) != 1:
        raise RuntimeError("KIND identity issuer/ticker is ambiguous")
    value, report_name = selected[0]
    selected_match = re.fullmatch(r"([0-9]{14})\|Y", value)
    if selected_match is None or not report_name:
        raise RuntimeError("KIND identity selected document is invalid")
    issuer, ticker = issuer_tickers[0]
    return KindIdentityReceipt(
        acceptance_no=acceptances[0],
        issuer_name=issuer,
        ticker=ticker,
        selected_document_no=selected_match.group(1),
        selected_report_name=report_name,
    )


def parse_kind_stock_dividend_component(
    payload: bytes,
) -> KindStockDividendComponent:
    decoded = corporate_actions._decode_document(payload)
    compact = corporate_actions._compact(decoded)
    if "61474주식배당결정" not in compact:
        raise RuntimeError("KIND component is not form 61474 stock dividend")
    if any(token in compact for token in ("철회", "취소", "부결")):
        raise RuntimeError("KIND component terminal body is inadmissible")
    if not all(token in compact for token in (
        "보통주주및우선주주", "신형우선주식", "배정비율동일",
    )):
        raise RuntimeError("KIND component cross-class semantics changed")

    record_dates: list[date] = []
    decision_dates: list[date] = []
    ratios: list[float] = []
    for rows in _expanded_kind_tables(payload):
        for index, cells in enumerate(rows):
            compact_cells = [corporate_actions._compact(cell) for cell in cells]
            if any(value in {"배당기준일", "4배당기준일"} for value in compact_cells):
                parsed_dates = {
                    parsed for cell in cells
                    if (parsed := corporate_actions._parse_date(cell)) is not None
                }
                record_dates.extend(sorted(parsed_dates))
            if any(
                value in {"이사회결의일결정일", "5이사회결의일결정일"}
                for value in compact_cells
            ):
                parsed_dates = {
                    parsed for cell in cells
                    if (parsed := corporate_actions._parse_date(cell)) is not None
                }
                decision_dates.extend(sorted(parsed_dates))
            ratio_headers = [
                position for position, value in enumerate(compact_cells)
                if value in {"1주당배당주식수주", "1주당주식배당"}
            ]
            class_headers = [
                position for position, value in enumerate(compact_cells)
                if value in {"종류주식구분", "주권종류"}
            ]
            if not ratio_headers and not class_headers:
                continue
            if len(ratio_headers) != 1 or len(class_headers) != 1:
                raise RuntimeError("KIND component ratio columns are ambiguous")
            ratio_index, class_index = ratio_headers[0], class_headers[0]
            if index + 1 >= len(rows):
                raise RuntimeError("KIND component ratio data is missing")
            candidate = rows[index + 1]
            if max(ratio_index, class_index) >= len(candidate):
                raise RuntimeError("KIND component ratio data is incomplete")
            if corporate_actions._compact(candidate[class_index]) != "우선주":
                raise RuntimeError("KIND component distributed class changed")
            ratio = corporate_actions._number(candidate[ratio_index])
            if ratio is None or ratio <= 0:
                raise RuntimeError("KIND component ratio is invalid")
            ratios.append(float(ratio))
    record = _unique(record_dates, label="component record date")
    decision = _unique(decision_dates, label="component decision date")
    ratio = _unique(ratios, label="component ratio")
    assert isinstance(record, date) and isinstance(decision, date)
    return KindStockDividendComponent(
        decision_date=decision,
        record_date=record,
        ratio_numerator=float(ratio),
        ratio_denominator=1.0,
        entitlement_security_class="COMMON_AND_PREFERRED",
        distributed_security_class="NEW_PREFERRED",
        report_name=KIND_COMPONENT_REPORT_NAME_61474,
    )


def _kind_security_class(value: object) -> str:
    compact = corporate_actions._compact(value)
    if compact in {"보통주", "보통주식"}:
        return "COMMON"
    if "우선주" in compact or "종류주" in compact:
        return "PREFERRED"
    raise RuntimeError("KIND reference notice security class is unsupported")


def _kind_action_type(reason: str) -> str:
    compact = corporate_actions._compact(reason)
    has_stock = "주식배당" in compact
    has_bonus = "무상증자" in compact
    if has_stock and has_bonus:
        return "combined_detachment"
    if has_stock:
        return "ex_dividend"
    if has_bonus:
        return "rights_detachment"
    raise RuntimeError("KIND reference notice has no supported non-cash reason")


def _unique(values: Sequence[object], *, label: str) -> object:
    unique = list(dict.fromkeys(values))
    if len(unique) != 1:
        raise RuntimeError(f"KIND reference notice {label} is ambiguous")
    return unique[0]


def _parse_99311(payload: bytes) -> KindReferenceNotice:
    decoded = corporate_actions._decode_document(payload)
    if "배당락기준가격안내" not in corporate_actions._compact(decoded):
        raise RuntimeError("KIND 99311 body title changed")
    issuers: list[str] = []
    reasons: list[str] = []
    effective_dates: list[date] = []
    security_prices: list[tuple[str, float]] = []
    for rows in _expanded_kind_tables(payload):
        for index, cells in enumerate(rows):
            compact_cells = [corporate_actions._compact(cell) for cell in cells]
            for offset, label in enumerate(compact_cells[:-1]):
                value = cells[offset + 1].strip()
                if label in {"회사명", "1회사명"} and value:
                    issuers.append(value)
                elif label in {"사유", "3사유"} and value:
                    reasons.append(value)
                elif label in {"적용일", "4적용일"} and value:
                    parsed = corporate_actions._parse_date(value)
                    if parsed is not None:
                        effective_dates.append(parsed)
            class_indices = [
                position for position, value in enumerate(compact_cells)
                if value == "주권종류"
            ]
            price_indices = [
                position for position, value in enumerate(compact_cells)
                if value in {"기준가격", "기준가격원"}
            ]
            if not class_indices and not price_indices:
                continue
            if len(class_indices) != 1 or len(price_indices) != 1:
                raise RuntimeError("KIND 99311 columns are ambiguous")
            class_index, price_index = class_indices[0], price_indices[0]
            for candidate in rows[index + 1:index + 3]:
                if max(class_index, price_index) >= len(candidate):
                    continue
                raw_class = corporate_actions._compact(candidate[class_index])
                raw_price = corporate_actions._number(candidate[price_index])
                if raw_class and raw_price is not None and raw_price > 0:
                    security_prices.append((raw_class, float(raw_price)))
                    break
    issuer = str(_unique(issuers, label="issuer"))
    reason = str(_unique(reasons, label="reason"))
    effective = _unique(effective_dates, label="effective date")
    raw_class, reference = _unique(
        security_prices, label="security/price",
    )
    assert isinstance(effective, date)
    return KindReferenceNotice(
        issuer_name=issuer,
        security_class=_kind_security_class(raw_class),
        reference_price=float(reference),
        effective_date=effective,
        reason=reason,
        action_type=_kind_action_type(reason),
        form_id="99311",
        ticker=None,
    )


def _parse_70767(payload: bytes) -> KindReferenceNotice:
    decoded = corporate_actions._decode_document(payload)
    compact = corporate_actions._compact(decoded)
    if "배당락" not in compact or "배당락기준가격안내" in compact:
        raise RuntimeError("KIND 70767 body title changed")
    parsed_rows: list[tuple[str, str, str, float, date, str]] = []
    aliases = {
        "issuer": {"회사명", "1회사명"},
        "security": {"주권종류", "2주권종류"},
        "ticker": {"단축코드", "3단축코드"},
        "reference": {"기준가", "기준가원", "4기준가", "4기준가원"},
        "effective": {"배당락실시일", "5배당락실시일"},
        "reason": {"사유", "6사유"},
    }
    for rows in _expanded_kind_tables(payload):
        for index, header in enumerate(rows[:-1]):
            labels = [corporate_actions._compact(cell) for cell in header]
            positions: dict[str, int] = {}
            for field, accepted in aliases.items():
                matches = [i for i, value in enumerate(labels) if value in accepted]
                if len(matches) > 1:
                    raise RuntimeError("KIND 70767 columns are ambiguous")
                if matches:
                    positions[field] = matches[0]
            if set(positions) != set(aliases):
                continue
            candidate = rows[index + 1]
            if max(positions.values()) >= len(candidate):
                raise RuntimeError("KIND 70767 data row is incomplete")
            effective = corporate_actions._parse_date(
                candidate[positions["effective"]]
            )
            reference = corporate_actions._number(candidate[positions["reference"]])
            ticker = corporate_actions._compact(candidate[positions["ticker"]])
            if ticker.startswith("A"):
                ticker = ticker[1:]
            if (
                effective is None or reference is None or reference <= 0
                or re.fullmatch(r"[0-9A-Z]{6}", ticker) is None
            ):
                raise RuntimeError("KIND 70767 data identity is invalid")
            parsed_rows.append((
                candidate[positions["issuer"]].strip(),
                candidate[positions["security"]].strip(),
                ticker,
                float(reference),
                effective,
                candidate[positions["reason"]].strip(),
            ))
    issuer, raw_class, ticker, reference, effective, reason = _unique(
        parsed_rows, label="70767 data row",
    )
    return KindReferenceNotice(
        issuer_name=str(issuer),
        security_class=_kind_security_class(raw_class),
        reference_price=float(reference),
        effective_date=effective,
        reason=str(reason),
        action_type=_kind_action_type(str(reason)),
        form_id="70767",
        ticker=str(ticker),
    )


def parse_kind_reference_notice(
    payload: bytes, *, expected_form_id: str | None = None,
) -> KindReferenceNotice:
    """Parse one exact KOSPI 99311 or KOSDAQ 70767 notice body."""
    compact = corporate_actions._compact(corporate_actions._decode_document(payload))
    detected = "99311" if "배당락기준가격안내" in compact else "70767"
    if expected_form_id is not None and expected_form_id != detected:
        raise RuntimeError("KIND URL/body form identity mismatch")
    if detected == "99311":
        return _parse_99311(payload)
    return _parse_70767(payload)


def _relative_file(root: Path, value: object, *, label: str) -> Path:
    relative = Path(str(value or ""))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise RuntimeError(f"KIND {label} path escapes snapshot")
    path = root / relative
    if not path.is_file():
        raise RuntimeError(f"KIND {label} body is missing")
    return path


def _content_addressed_file(
    root: Path,
    *,
    relative_path: object,
    declared_sha256: object,
    declared_length: object,
    object_root: Path,
    label: str,
) -> Path:
    path = _relative_file(root, relative_path, label=label)
    digest = str(declared_sha256 or "")
    if (
        _SHA.fullmatch(digest) is None
        or not isinstance(declared_length, int)
        or declared_length <= 0
        or path.stat().st_size != declared_length
        or sha256(path) != digest
        or path.relative_to(root)
        != object_root / f"sha256={digest}.html"
    ):
        raise RuntimeError(f"KIND {label} is not exact content-addressed evidence")
    return path


def _same_number(actual: object, expected: float, *, label: str) -> None:
    if actual is None or not math.isclose(
        float(actual), float(expected), rel_tol=0, abs_tol=1e-12,
    ):
        raise RuntimeError(f"KIND {label} changed")


def verify_kind_request_object(
    root: Path,
    *,
    relative_path: object,
    expected_sha256: object,
) -> tuple[dict[str, object], dict[tuple[str, str], dict[str, object]]]:
    path = _relative_file(root, relative_path, label="request")
    declared_sha = str(expected_sha256 or "")
    if _SHA.fullmatch(declared_sha) is None or sha256(path) != declared_sha:
        raise RuntimeError("KIND request object SHA mismatch")
    expected_relative = KIND_REQUEST_OBJECT_ROOT / f"sha256={declared_sha}.json"
    if path.relative_to(root) != expected_relative:
        raise RuntimeError("KIND request object is not content addressed")
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid KIND request manifest JSON") from exc
    if raw != canonical_bytes(payload):
        raise RuntimeError("KIND request manifest is not canonical JSON bytes")
    requests = payload.get("requests")
    if (
        not isinstance(payload, dict)
        or set(payload) != {
            "schema_version", "complete", "request_count",
            "request_digest", "requests", "provenance",
        }
        or payload.get("schema_version") != KIND_REQUEST_SCHEMA
        or payload.get("provenance")
        != "HUMAN_REVIEWED_OFFICIAL_KIND_MAIN_AND_SELECTED_BODY"
        or payload.get("complete") is not True
        or not isinstance(requests, list)
        or int(payload.get("request_count", -1)) != len(requests)
        or payload.get("request_digest")
        != hashlib.sha256(canonical_bytes(requests)).hexdigest()
    ):
        raise RuntimeError("KIND request manifest contract mismatch")
    identities: dict[tuple[str, str], dict[str, object]] = {}
    canonical_rows: list[dict[str, object]] = []
    for raw_row in requests:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "ticker", "asset_name", "security_class", "source_url",
            "identity_source_url", "support_semantic_role",
            "source_form_code", "target_adjustment_date",
            "target_cash_receipt_no", "identity_content_length",
            "identity_sha256", "body_content_length", "body_sha256",
        }:
            raise RuntimeError("KIND request row fields changed")
        row = dict(raw_row)
        ticker = str(row.get("ticker") or "").zfill(6)
        security_class = str(row.get("security_class") or "")
        if _TICKER.fullmatch(ticker) is None or security_class not in {
            "COMMON", "PREFERRED",
        }:
            raise RuntimeError("KIND request identity is invalid")
        action_key, _, form_id = kind_url_identity(row.get("source_url"))
        identity_acceptance = kind_identity_url_acceptance(
            row.get("identity_source_url")
        )
        try:
            target_date = date.fromisoformat(str(row["target_adjustment_date"]))
        except ValueError as exc:
            raise RuntimeError("KIND request target date is invalid") from exc
        if (
            identity_acceptance != action_key
            or row.get("support_semantic_role") != "CORROBORATION"
            or row.get("source_form_code") != form_id
            or re.fullmatch(
                r"[0-9]{14}", str(row.get("target_cash_receipt_no") or "")
            ) is None
            or target_date.year < 2015
            or not isinstance(row.get("identity_content_length"), int)
            or int(row["identity_content_length"]) <= 0
            or _SHA.fullmatch(str(row.get("identity_sha256") or "")) is None
            or not isinstance(row.get("body_content_length"), int)
            or int(row["body_content_length"]) <= 0
            or _SHA.fullmatch(str(row.get("body_sha256") or "")) is None
        ):
            raise RuntimeError("KIND request reviewed identity fields are invalid")
        identity = (ticker, action_key)
        if identity in identities:
            raise RuntimeError("duplicate KIND request identity")
        row["ticker"] = ticker
        identities[identity] = row
        canonical_rows.append(row)
    if requests != sorted(
        canonical_rows,
        key=lambda row: (str(row["ticker"]), str(row["source_url"])),
    ):
        raise RuntimeError("KIND request rows are not canonically sorted")
    return payload, identities


def verify_kind_component_request_object(
    root: Path,
    *,
    relative_path: object,
    expected_sha256: object,
) -> tuple[dict[str, object], dict[tuple[str, str], dict[str, object]]]:
    path = _relative_file(root, relative_path, label="component request")
    declared_sha = str(expected_sha256 or "")
    if _SHA.fullmatch(declared_sha) is None or sha256(path) != declared_sha:
        raise RuntimeError("KIND component request object SHA mismatch")
    expected_relative = KIND_REQUEST_OBJECT_ROOT / f"sha256={declared_sha}.json"
    if path.relative_to(root) != expected_relative:
        raise RuntimeError("KIND component request object is not content addressed")
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid KIND component request JSON") from exc
    if raw != canonical_bytes(payload):
        raise RuntimeError("KIND component request is not canonical JSON bytes")
    components = payload.get("components")
    if (
        not isinstance(payload, dict)
        or set(payload) != {
            "schema_version", "provenance", "complete", "component_count",
            "component_digest", "components",
        }
        or payload.get("schema_version") != KIND_COMPONENT_REQUEST_SCHEMA
        or payload.get("provenance")
        != "HUMAN_REVIEWED_OFFICIAL_KIND_TERMINAL_COMPONENT"
        or payload.get("complete") is not True
        or not isinstance(components, list)
        or int(payload.get("component_count", -1)) != len(components)
        or payload.get("component_digest")
        != hashlib.sha256(canonical_bytes(components)).hexdigest()
    ):
        raise RuntimeError("KIND component request contract mismatch")
    expected_fields = {
        "adjustment_date", "announcement_date", "asset_name",
        "body_content_length", "body_sha256", "body_url",
        "component_action_key", "component_action_source",
        "component_action_type", "contents_content_length",
        "contents_sha256", "contents_url", "distributed_security_class",
        "entitlement_security_class", "main_content_length", "main_sha256",
        "main_url", "ratio_denominator",
        "ratio_numerator", "record_date", "report_name", "semantic_role",
        "source_form_code", "target_cash_receipt_no",
        "terminal_acceptance_no", "terminal_announcement_date", "ticker",
    }
    identities: dict[tuple[str, str], dict[str, object]] = {}
    canonical_rows: list[dict[str, object]] = []
    for raw_row in components:
        if not isinstance(raw_row, dict) or set(raw_row) != expected_fields:
            raise RuntimeError("KIND component request row fields changed")
        row = dict(raw_row)
        ticker = str(row.get("ticker") or "").zfill(6)
        key = str(row.get("component_action_key") or "")
        terminal = str(row.get("terminal_acceptance_no") or "")
        body_acceptance, body_date, body_document = kind_component_url_identity(
            row.get("body_url")
        )
        main_acceptance = kind_identity_url_acceptance(row.get("main_url"))
        contents_document = kind_contents_url_document_no(row.get("contents_url"))
        try:
            adjustment = date.fromisoformat(str(row["adjustment_date"]))
            announcement = date.fromisoformat(str(row["announcement_date"]))
            terminal_announcement = date.fromisoformat(
                str(row["terminal_announcement_date"])
            )
            record = date.fromisoformat(str(row["record_date"]))
        except ValueError as exc:
            raise RuntimeError("KIND component request date is invalid") from exc
        ratio_numerator = float(row["ratio_numerator"])
        ratio_denominator = float(row["ratio_denominator"])
        if (
            _TICKER.fullmatch(ticker) is None
            or re.fullmatch(r"[0-9]{14}", key) is None
            or terminal != main_acceptance
            or terminal != body_acceptance
            or key != body_document
            or key != contents_document
            or row.get("component_action_source") != "KRX_KIND"
            or row.get("component_action_type") != "stock_dividend"
            or row.get("semantic_role") != "ADJUSTMENT_COMPONENT"
            or row.get("source_form_code") != "61474"
            or row.get("entitlement_security_class") != "COMMON_AND_PREFERRED"
            or row.get("distributed_security_class") != "NEW_PREFERRED"
            or row.get("report_name") != KIND_COMPONENT_REPORT_NAME_61474
            or re.fullmatch(
                r"[0-9]{14}", str(row.get("target_cash_receipt_no") or "")
            ) is None
            or announcement
            != date(int(key[:4]), int(key[4:6]), int(key[6:8]))
            or terminal_announcement != body_date
            or not (announcement <= terminal_announcement < adjustment < record)
            or ratio_numerator <= 0
            or ratio_denominator <= 0
        ):
            raise RuntimeError("KIND component request identity is invalid")
        for prefix in ("main", "contents", "body"):
            length = row.get(f"{prefix}_content_length")
            digest = str(row.get(f"{prefix}_sha256") or "")
            if (
                not isinstance(length, int) or length <= 0
                or _SHA.fullmatch(digest) is None
            ):
                raise RuntimeError("KIND component expected body receipt is invalid")
        identity = (ticker, key)
        if identity in identities:
            raise RuntimeError("duplicate KIND component request identity")
        row["ticker"] = ticker
        identities[identity] = row
        canonical_rows.append(row)
    if components != sorted(
        canonical_rows,
        key=lambda row: (str(row["ticker"]), str(row["component_action_key"])),
    ):
        raise RuntimeError("KIND component request rows are not canonically sorted")
    return payload, identities


def verify_kind_support_manifest(base: str | Path) -> list[dict[str, object]]:
    """Verify immutable KIND support rows and their downloaded request set."""
    root = Path(base).expanduser().resolve()
    path = root / KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    if not path.is_file():
        return []
    raw_manifest = path.read_bytes()
    try:
        payload = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid KIND support manifest") from exc
    if raw_manifest != canonical_bytes(payload):
        raise RuntimeError("KIND support manifest is not canonical JSON bytes")
    supports = payload.get("supports")
    if (
        not isinstance(payload, dict)
        or set(payload) != {
            "schema_version", "complete",
            "reference_request_path", "reference_request_sha256",
            "component_request_path", "component_request_sha256",
            "support_count", "support_digest", "supports",
        }
        or payload.get("schema_version") != KIND_SUPPORT_SCHEMA
        or payload.get("complete") is not True
        or not isinstance(supports, list)
        or int(payload.get("support_count", -1)) != len(supports)
        or payload.get("support_digest")
        != hashlib.sha256(canonical_bytes(supports)).hexdigest()
    ):
        raise RuntimeError("KIND support manifest contract mismatch")
    _, request_by_identity = verify_kind_request_object(
        root,
        relative_path=payload.get("reference_request_path"),
        expected_sha256=payload.get("reference_request_sha256"),
    )
    _, component_by_identity = verify_kind_component_request_object(
        root,
        relative_path=payload.get("component_request_path"),
        expected_sha256=payload.get("component_request_sha256"),
    )
    identities: set[tuple[str, str, str, str]] = set()
    requested_consumed: set[tuple[str, str]] = set()
    components_consumed: set[tuple[str, str]] = set()
    normalized: list[dict[str, object]] = []
    for raw_row in supports:
        if not isinstance(raw_row, dict) or set(raw_row) != _SUPPORT_FIELDS:
            raise RuntimeError("KIND support entry fields changed")
        item = dict(raw_row)
        ticker = str(item.get("ticker") or "").zfill(6)
        key = str(item.get("support_action_key") or "")
        action_type = str(item.get("support_action_type") or "")
        role = str(item.get("support_semantic_role") or "")
        if _TICKER.fullmatch(ticker) is None or role not in _SUPPORT_ROLES:
            raise RuntimeError("KIND support identity is invalid")
        identity = (ticker, key, action_type, role)
        if identity in identities:
            raise RuntimeError("duplicate KIND support identity")
        identities.add(identity)
        body_sha = str(item.get("support_action_body_sha256") or "")
        body_path = _content_addressed_file(
            root,
            relative_path=item.get("support_action_body_path"),
            declared_sha256=body_sha,
            declared_length=item.get("support_action_body_content_length"),
            object_root=KIND_BODY_OBJECT_ROOT,
            label="support body",
        )
        if str(item.get("support_action_source") or "") != "KRX_KIND":
            raise RuntimeError("KIND support source changed")
        if item.get("support_action_scope") != "ISSUER":
            raise RuntimeError("KIND support scope changed")
        source_url = str(item.get("source_url") or "")
        if role == "CORROBORATION":
            if action_type not in _REFERENCE_ACTION_TYPES:
                raise RuntimeError("KIND corroboration action type changed")
            action_key, announcement, form_id = kind_url_identity(source_url)
            if key != action_key:
                raise RuntimeError("KIND support URL/action identity mismatch")
            notice = parse_kind_reference_notice(
                body_path.read_bytes(), expected_form_id=form_id,
            )
            request = request_by_identity.get((ticker, key))
            if request is None or (ticker, key) in requested_consumed:
                raise RuntimeError("KIND support request is missing/ambiguous")
            requested_consumed.add((ticker, key))
            identity_path = _content_addressed_file(
                root,
                relative_path=item.get("identity_body_path"),
                declared_sha256=item.get("identity_body_sha256"),
                declared_length=item.get("identity_body_content_length"),
                object_root=KIND_IDENTITY_OBJECT_ROOT,
                label="reference identity body",
            )
            identity_receipt = parse_kind_identity_receipt(identity_path.read_bytes())
            requested_identity_url = str(request["identity_source_url"])
            selected_document = kind_url_document_no(source_url)
            expected_report = (
                KIND_REFERENCE_REPORT_NAME_99311
                if notice.form_id == "99311"
                else KIND_REFERENCE_REPORT_NAME_70767
            )
            selected_report = corporate_actions._compact(
                identity_receipt.selected_report_name
            )
            expected_selected_report = corporate_actions._compact(
                f"{expected_report} ({announcement.strftime('%Y.%m.%d')})"
            )
            expected_values = {
                "issuer_name": notice.issuer_name,
                "support_action_type": notice.action_type,
                "support_announcement_date": announcement.isoformat(),
                "support_ex_date": notice.effective_date.isoformat(),
                "support_entitlement_security_class": notice.security_class,
                "support_reference_price": notice.reference_price,
                "support_reason": notice.reason,
                "support_report_name": expected_report,
                "support_action_scope": "ISSUER",
                "source_url": str(request["source_url"]),
                "source_form_id": notice.form_id,
                "target_cash_receipt_no": request["target_cash_receipt_no"],
                "target_adjustment_date": request["target_adjustment_date"],
                "identity_source_url": requested_identity_url,
                "identity_body_path": (
                    KIND_IDENTITY_OBJECT_ROOT
                    / f"sha256={request['identity_sha256']}.html"
                ).as_posix(),
                "identity_body_content_length": request[
                    "identity_content_length"
                ],
                "identity_body_sha256": request["identity_sha256"],
                "terminal_acceptance_no": key,
            }
            for field, expected in expected_values.items():
                actual = item.get(field)
                if isinstance(expected, float):
                    if actual is None or not math.isclose(
                        float(actual), expected, rel_tol=0, abs_tol=1e-8,
                    ):
                        raise RuntimeError(f"KIND notice {field} changed")
                elif actual != expected:
                    raise RuntimeError(f"KIND notice {field} changed")
            if (
                corporate_actions._compact(request["asset_name"])
                != corporate_actions._compact(notice.issuer_name)
                or request["security_class"] != notice.security_class
                or request["source_form_code"] != form_id
                or request["body_sha256"] != body_sha
                or request["body_content_length"]
                != item["support_action_body_content_length"]
                or kind_identity_url_acceptance(requested_identity_url) != key
                or identity_receipt.acceptance_no != key
                or identity_receipt.ticker != ticker
                or corporate_actions._compact(identity_receipt.issuer_name)
                != corporate_actions._compact(request["asset_name"])
                or corporate_actions._compact(identity_receipt.issuer_name)
                != corporate_actions._compact(notice.issuer_name)
                or identity_receipt.selected_document_no != selected_document
                or selected_report != expected_selected_report
                or (notice.ticker is not None and notice.ticker != ticker)
                or item.get("support_record_date") is not None
                or item.get("support_ratio_numerator") is not None
                or item.get("support_ratio_denominator") is not None
                or item.get("support_distributed_security_class") is not None
                or item.get("support_expected_price_factor") is not None
                or item.get("contents_source_url") is not None
                or item.get("contents_body_path") is not None
                or item.get("contents_body_content_length") is not None
                or item.get("contents_body_sha256") is not None
            ):
                raise RuntimeError("KIND notice request/body semantics changed")
        else:
            request = component_by_identity.get((ticker, key))
            if request is None or (ticker, key) in components_consumed:
                raise RuntimeError("KIND component request is missing/ambiguous")
            components_consumed.add((ticker, key))
            if action_type != "stock_dividend" or source_url != request["body_url"]:
                raise RuntimeError("KIND component action identity changed")
            body_acceptance, body_date, body_document = kind_component_url_identity(
                source_url
            )
            identity_path = _content_addressed_file(
                root,
                relative_path=item.get("identity_body_path"),
                declared_sha256=item.get("identity_body_sha256"),
                declared_length=item.get("identity_body_content_length"),
                object_root=KIND_IDENTITY_OBJECT_ROOT,
                label="component identity body",
            )
            contents_path = _content_addressed_file(
                root,
                relative_path=item.get("contents_body_path"),
                declared_sha256=item.get("contents_body_sha256"),
                declared_length=item.get("contents_body_content_length"),
                object_root=KIND_CONTENTS_OBJECT_ROOT,
                label="component contents body",
            )
            identity_receipt = parse_kind_identity_receipt(identity_path.read_bytes())
            component = parse_kind_stock_dividend_component(body_path.read_bytes())
            terminal = str(request["terminal_acceptance_no"])
            selected_report = corporate_actions._compact(
                identity_receipt.selected_report_name
            )
            expected_selected_report = corporate_actions._compact(
                "[정정]주식배당결정 "
                f"({date.fromisoformat(str(request['terminal_announcement_date'])).strftime('%Y.%m.%d')})"
            )
            expected_values = {
                "ticker": ticker,
                "issuer_name": request["asset_name"],
                "source_form_id": "61474",
                "source_url": request["body_url"],
                "target_cash_receipt_no": request["target_cash_receipt_no"],
                "target_adjustment_date": request["adjustment_date"],
                "identity_source_url": request["main_url"],
                "identity_body_path": (
                    KIND_IDENTITY_OBJECT_ROOT
                    / f"sha256={request['main_sha256']}.html"
                ).as_posix(),
                "identity_body_content_length": request["main_content_length"],
                "identity_body_sha256": request["main_sha256"],
                "contents_source_url": request["contents_url"],
                "contents_body_path": (
                    KIND_CONTENTS_OBJECT_ROOT
                    / f"sha256={request['contents_sha256']}.html"
                ).as_posix(),
                "contents_body_content_length": request["contents_content_length"],
                "contents_body_sha256": request["contents_sha256"],
                "terminal_acceptance_no": terminal,
                "support_action_source": "KRX_KIND",
                "support_action_key": key,
                "support_action_type": "stock_dividend",
                "support_semantic_role": "ADJUSTMENT_COMPONENT",
                "support_announcement_date": request[
                    "terminal_announcement_date"
                ],
                "support_ex_date": None,
                "support_record_date": request["record_date"],
                "support_entitlement_security_class": request[
                    "entitlement_security_class"
                ],
                "support_distributed_security_class": request[
                    "distributed_security_class"
                ],
                "support_expected_price_factor": None,
                "support_reference_price": None,
                "support_reason": None,
                "support_report_name": request["report_name"],
                "support_action_scope": "ISSUER",
            }
            for field, expected in expected_values.items():
                if item.get(field) != expected:
                    raise RuntimeError(f"KIND component {field} changed")
            _same_number(
                item.get("support_ratio_numerator"),
                float(request["ratio_numerator"]),
                label="component ratio numerator",
            )
            _same_number(
                item.get("support_ratio_denominator"),
                float(request["ratio_denominator"]),
                label="component ratio denominator",
            )
            if (
                body_path.relative_to(root)
                != KIND_BODY_OBJECT_ROOT / f"sha256={request['body_sha256']}.html"
                or body_sha != request["body_sha256"]
                or item["support_action_body_content_length"]
                != request["body_content_length"]
                or body_acceptance != terminal
                or body_document != key
                or body_date
                != date.fromisoformat(str(request["terminal_announcement_date"]))
                or kind_identity_url_acceptance(request["main_url"]) != terminal
                or identity_receipt.acceptance_no != terminal
                or identity_receipt.ticker != ticker
                or corporate_actions._compact(identity_receipt.issuer_name)
                != corporate_actions._compact(request["asset_name"])
                or identity_receipt.selected_document_no != key
                or selected_report != expected_selected_report
                or kind_contents_url_document_no(request["contents_url"]) != key
                or parse_kind_contents_body_url(contents_path.read_bytes())
                != source_url
                or component.decision_date
                != date.fromisoformat(str(request["announcement_date"]))
                or component.record_date
                != date.fromisoformat(str(request["record_date"]))
                or not math.isclose(
                    component.ratio_numerator,
                    float(request["ratio_numerator"]),
                    rel_tol=0,
                    abs_tol=1e-12,
                )
                or not math.isclose(
                    component.ratio_denominator,
                    float(request["ratio_denominator"]),
                    rel_tol=0,
                    abs_tol=1e-12,
                )
                or component.entitlement_security_class
                != request["entitlement_security_class"]
                or component.distributed_security_class
                != request["distributed_security_class"]
                or component.report_name != request["report_name"]
            ):
                raise RuntimeError("KIND component official evidence chain changed")
        item["ticker"] = ticker
        normalized.append(item)
    if set(request_by_identity) != requested_consumed:
        raise RuntimeError("KIND request manifest has unused notice identities")
    if set(component_by_identity) != components_consumed:
        raise RuntimeError("KIND component manifest has unused identities")
    expected_order = sorted(
        normalized,
        key=lambda item: (
            str(item["ticker"]), str(item["support_action_key"]),
            str(item["support_action_type"]),
            str(item["support_semantic_role"]),
        ),
    )
    if supports != expected_order:
        raise RuntimeError("KIND support rows are not canonically sorted")
    return normalized


def external_evidence_paths(base: str | Path) -> tuple[Path, ...]:
    root = Path(base).expanduser().resolve()
    supports = verify_kind_support_manifest(root)
    if not supports:
        return ()
    manifest = root / KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_bytes())
    paths = {
        manifest,
        root / str(payload["reference_request_path"]),
        root / str(payload["component_request_path"]),
        *(root / str(row["support_action_body_path"]) for row in supports),
        *(root / str(row["identity_body_path"]) for row in supports),
        *(
            root / str(row["contents_body_path"])
            for row in supports if row["contents_body_path"] is not None
        ),
    }
    return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))
