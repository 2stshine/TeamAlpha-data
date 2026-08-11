"""Content-addressed evidence for cash dividends on price-adjustment dates.

``adj_close`` changes scale when KRX resets a security's reference price for
an action such as a stock dividend or a rights detachment.  A cash dividend
that is applied on the same date cannot choose its share scale from the price
series alone.  This module binds that exceptional choice to three immutable
inputs:

* the exact cash-decision body;
* the exact supporting corporate-action body; and
* the exact KRX price object containing the adjusted reference price.

The generic total-return path remains fail closed.  A changed-scale cash event
must match exactly one verified row; a stable-scale event must not consume one.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Sequence

import pandas as pd

from pipeline.bronze.dart_support_action_families import (
    external_evidence_paths as support_family_external_evidence_paths,
    verify_support_action_families,
)
from pipeline.silver.krx_kind_reference import (
    KIND_COMPONENT_REPORT_NAME_61474,
    KIND_REFERENCE_REPORT_NAME_70767,
    KIND_REFERENCE_REPORT_NAME_99311,
    external_evidence_paths as kind_external_evidence_paths,
    parse_kind_reference_notice,
    parse_kind_stock_dividend_component,
    verify_kind_support_manifest,
)


SOURCE_EVIDENCE_CONTRACT = "krx_cash_adjustment_scale_source_evidence_v1"
RESOLUTION_EVIDENCE_CONTRACT = "krx_cash_adjustment_scale_resolution_v1"
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/krx/cash_adjustment_scale_source_evidence.json"
)
PRICE_OBJECT_MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/krx/cash_adjustment_scale_price_objects.json"
)
PRICE_OBJECT_MANIFEST_CONTRACT = "krx_cash_adjustment_scale_price_objects_v1"
STABLE_PRICE_SCALE = "STABLE_PRICE_SCALE"
PRE_EVENT_PRICE_SCALE = "PRE_EVENT_PRICE_SCALE"

# This order is the public canonical digest contract.  Run identifiers are
# included so a persisted row cannot be replayed under a different certified
# action snapshot while retaining the same digest.
SOURCE_EVIDENCE_COLUMNS = (
    "action_snapshot_run_id",
    "evidence_key",
    "asset_id",
    "ticker",
    "cash_receipt_no",
    "cash_source_evidence_status",
    "cash_action_body_path",
    "cash_action_body_sha256",
    "cash_economic_body_path",
    "cash_economic_body_schema",
    "cash_economic_sha256",
    "support_action_count",
    "support_action_digest",
    "support_semantic_group_count",
    "price_source",
    "previous_price_source_object_key",
    "previous_price_source_content_sha256",
    "previous_price_source_etag",
    "previous_price_source_schema",
    "adjustment_price_source_object_key",
    "adjustment_price_source_content_sha256",
    "adjustment_price_source_etag",
    "adjustment_price_source_schema",
    "previous_trade_date",
    "adjustment_trade_date",
    "raw_previous_close",
    "raw_applied_close",
    "raw_reference_price",
    "expected_price_factor",
    "cash_scale_basis",
    "manifest_row_sha256",
)

SUPPORT_ACTION_COLUMNS = (
    "action_snapshot_run_id",
    "evidence_key",
    "support_action_source",
    "support_action_key",
    "support_action_type",
    "target_cash_receipt_no",
    "target_adjustment_date",
    "support_action_body_path",
    "support_action_body_sha256",
    "support_action_quality_run_id",
    "support_announcement_date",
    "support_ex_date",
    "support_record_date",
    "support_ratio_numerator",
    "support_ratio_denominator",
    "support_entitlement_security_class",
    "support_distributed_security_class",
    "support_expected_price_factor",
    "support_reference_price",
    "support_reason",
    "support_report_name",
    "support_action_scope",
    "support_semantic_group_keys",
    "support_semantic_role",
    "manifest_support_row_sha256",
)

# The stable manifest digest deliberately excludes database-assigned IDs and
# the self digest.  It is used as ``manifest_row_sha256`` and as the durable
# evidence key material before an action-snapshot run exists.
MANIFEST_ROW_COLUMNS = tuple(
    column for column in SOURCE_EVIDENCE_COLUMNS
    if column not in {
        "action_snapshot_run_id", "asset_id",
        "manifest_row_sha256",
    }
)
MANIFEST_SUPPORT_ACTION_COLUMNS = tuple(
    column for column in SUPPORT_ACTION_COLUMNS
    if column not in {
        "action_snapshot_run_id", "support_action_quality_run_id",
        "manifest_support_row_sha256",
    }
)

RESOLUTION_DIGEST_COLUMNS = (
    "asset_id",
    "source",
    "action_key",
    "resolution_version",
    "applied_trade_date",
    "raw_cash_amount",
    "adjusted_cash_amount",
    "previous_trade_date",
    "previous_close",
    "previous_adj_close",
    "applied_close",
    "applied_adj_close",
    "previous_price_scale",
    "applied_price_scale",
    "selected_cash_scale",
    "cash_adjustment_scale_basis",
    "scale_change_detected",
    "scale_evidence_action_snapshot_run_id",
    "scale_evidence_key",
    "scale_price_factor_observed",
    "scale_price_factor_reference",
    "scale_price_factor_parity",
)

_DATE_COLUMNS = frozenset({
    "previous_trade_date", "adjustment_trade_date", "applied_trade_date",
    "target_adjustment_date", "support_announcement_date", "support_ex_date",
    "support_record_date",
})
_INTEGER_COLUMNS = frozenset({
    "asset_id", "support_action_count", "support_semantic_group_count",
})
_BOOLEAN_COLUMNS = frozenset({
    "scale_change_detected", "scale_price_factor_parity",
})
_DECIMAL_PLACES = {
    "raw_previous_close": 8,
    "raw_applied_close": 8,
    "raw_reference_price": 8,
    "raw_cash_amount": 8,
    "adjusted_cash_amount": 8,
    "previous_close": 8,
    "previous_adj_close": 8,
    "applied_close": 8,
    "applied_adj_close": 8,
    "expected_price_factor": 12,
    "support_ratio_numerator": 8,
    "support_ratio_denominator": 8,
    "support_expected_price_factor": 12,
    "support_reference_price": 8,
    "previous_price_scale": 12,
    "applied_price_scale": 12,
    "selected_cash_scale": 12,
    "scale_price_factor_observed": 12,
    "scale_price_factor_reference": 12,
}
_SHA_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")
_TICKER_PATTERN = re.compile(r"^[0-9A-Z]{6}$")


@dataclass(frozen=True)
class VerifiedScaleSourceEvidence:
    """Verified local manifest and its canonical, database-neutral rows."""

    frame: pd.DataFrame
    support_frame: pd.DataFrame
    manifest_path: str | None
    manifest_sha256: str
    row_count: int
    row_digest: str

    @property
    def metadata(self) -> dict[str, object]:
        return {
            "contract": SOURCE_EVIDENCE_CONTRACT,
            "manifest_sha256": self.manifest_sha256,
            "manifest_parent_row_count": self.row_count,
            "manifest_parent_row_digest": self.row_digest,
            "manifest_support_action_count": len(self.support_frame),
            "manifest_support_action_digest": support_manifest_digest(
                self.support_frame
            ),
            "manifest_support_semantic_group_count": (
                _support_group_count(self.support_frame)
            ),
        }


@dataclass(frozen=True)
class BoundScaleSourceEvidence:
    """One snapshot run's immutable parent and child evidence rows."""

    frame: pd.DataFrame
    support_frame: pd.DataFrame


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_value(column: str, value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if column in _DATE_COLUMNS:
        if isinstance(value, (date, datetime, pd.Timestamp)):
            return value.isoformat()[:10]
        return pd.Timestamp(value).date().isoformat()
    if column in _INTEGER_COLUMNS:
        return int(value)
    if column in _BOOLEAN_COLUMNS:
        return bool(value)
    if column in _DECIMAL_PLACES:
        try:
            quantum = Decimal(1).scaleb(-_DECIMAL_PLACES[column])
            return format(Decimal(str(value)).quantize(quantum), "f")
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(
                f"invalid cash-scale decimal {column}={value!r}"
            ) from exc
    return str(value)


def _canonical_rows(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str],
    order_by: Sequence[str],
) -> list[dict[str, object]]:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"cash-scale digest columns are missing: {missing}")
    ordered = frame.sort_values(list(order_by), kind="stable")
    return [
        {
            column: _canonical_value(column, getattr(row, column))
            for column in columns
        }
        for row in ordered[list(columns)].itertuples(index=False)
    ]


def _rows_digest(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str],
    order_by: Sequence[str],
) -> str:
    payload = _canonical_rows(
        frame, columns=columns, order_by=order_by,
    )
    rendered = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def source_evidence_digest(frame: pd.DataFrame) -> str:
    """Digest persisted source evidence, including both run identities."""
    return _rows_digest(
        frame,
        columns=SOURCE_EVIDENCE_COLUMNS,
        order_by=("action_snapshot_run_id", "evidence_key"),
    )


def source_manifest_digest(frame: pd.DataFrame) -> str:
    """Digest stable manifest rows before database identities are assigned."""
    return _rows_digest(
        frame,
        columns=MANIFEST_ROW_COLUMNS,
        order_by=("evidence_key",),
    )


def support_action_digest(frame: pd.DataFrame) -> str:
    """Digest persisted support rows, including both immutable run IDs."""
    return _rows_digest(
        frame,
        columns=SUPPORT_ACTION_COLUMNS,
        order_by=(
            "action_snapshot_run_id", "evidence_key",
            "support_action_source", "support_action_key",
            "support_action_type",
        ),
    )


def support_manifest_digest(frame: pd.DataFrame) -> str:
    """Digest database-neutral support rows nested under evidence parents."""
    return _rows_digest(
        frame,
        columns=MANIFEST_SUPPORT_ACTION_COLUMNS,
        order_by=(
            "evidence_key", "support_action_source", "support_action_key",
            "support_action_type",
        ),
    )


def resolution_evidence_digest(frame: pd.DataFrame) -> str:
    """Digest every applied-event scale decision in resolution-v2."""
    return _rows_digest(
        frame,
        columns=RESOLUTION_DIGEST_COLUMNS,
        order_by=("asset_id", "source", "action_key"),
    )


def manifest_parent_row_sha256(row: dict[str, object]) -> str:
    """Return the frozen canonical self-digest for one manifest parent."""
    frame = pd.DataFrame([row])
    payload = _canonical_rows(
        frame, columns=MANIFEST_ROW_COLUMNS, order_by=("evidence_key",),
    )[0]
    rendered = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def manifest_support_row_sha256(row: dict[str, object]) -> str:
    """Return the frozen canonical self-digest for one support child."""
    frame = pd.DataFrame([row])
    payload = _canonical_rows(
        frame,
        columns=MANIFEST_SUPPORT_ACTION_COLUMNS,
        order_by=(
            "evidence_key", "support_action_source", "support_action_key",
            "support_action_type",
        ),
    )[0]
    rendered = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


# Backward-compatible aliases for the original internal callers.  Builders
# should use the public names above so canonical rendering is not duplicated.
_manifest_row_sha = manifest_parent_row_sha256
_manifest_support_row_sha = manifest_support_row_sha256


def _require_relative_body(
    root: Path,
    row: dict[str, object],
    *,
    path_field: str,
    sha_field: str,
) -> Path:
    rendered = str(row.get(path_field) or "").strip()
    candidate = Path(rendered)
    if not rendered or candidate.is_absolute() or ".." in candidate.parts:
        raise RuntimeError(f"invalid evidence body path: {path_field}={rendered!r}")
    path = (root / candidate).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"evidence body escapes snapshot: {rendered}") from exc
    expected = str(row.get(sha_field) or "")
    if not path.is_file() or _SHA_PATTERN.fullmatch(expected) is None:
        raise RuntimeError(f"cash-scale evidence body missing/invalid: {rendered}")
    if _sha256(path) != expected:
        raise RuntimeError(f"cash-scale evidence SHA mismatch: {rendered}")
    return path


def _number(value: object, *, field: str) -> float:
    try:
        rendered = float(str(value).replace(",", ""))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid cash-scale {field}: {value!r}") from exc
    if not math.isfinite(rendered) or rendered <= 0:
        raise RuntimeError(f"cash-scale {field} must be finite and positive")
    return rendered


def _price_observation(
    path: Path,
    *,
    schema: str,
    ticker: str,
    trade_date: date,
) -> tuple[float, float | None]:
    """Read one exact KRX close and optional adjusted reference price."""
    if schema == "marcap_parquet_v1":
        required = ["Code", "Date", "Close", "Changes"]
        code_field, date_field = "Code", "Date"
        close_field, change_field = "Close", "Changes"
    elif schema == "krxapi_stock_parquet_v1":
        required = ["ISU_CD", "BAS_DD", "TDD_CLSPRC", "CMPPREVDD_PRC"]
        code_field, date_field = "ISU_CD", "BAS_DD"
        close_field, change_field = "TDD_CLSPRC", "CMPPREVDD_PRC"
    else:
        raise RuntimeError(f"unsupported cash-scale KRX price schema: {schema}")
    try:
        raw = pd.read_parquet(path, columns=required)
    except Exception as exc:  # noqa: BLE001 - fail closed on provider drift
        raise RuntimeError(f"invalid KRX evidence parquet: {path}") from exc
    code = raw[code_field].astype(str).str.upper().str.zfill(6)
    observed_date = pd.to_datetime(
        raw[date_field].astype(str), errors="coerce",
    ).dt.date
    matching = raw[code.eq(ticker) & observed_date.eq(trade_date)]
    if len(matching) != 1:
        raise RuntimeError(
            "KRX price evidence row is missing/ambiguous: "
            f"ticker={ticker} date={trade_date} rows={len(matching)}"
        )
    price = matching.iloc[0]
    close = _number(price[close_field], field="raw close")
    rendered_change = str(price[change_field]).replace(",", "").strip()
    try:
        change = float(rendered_change)
    except ValueError as exc:
        raise RuntimeError("invalid KRX previous-price difference") from exc
    reference = close - change if math.isfinite(change) else None
    return close, reference


def _price_body(
    root: Path,
    row: dict[str, object],
    *,
    prefix: str,
    trade_date: date,
) -> tuple[float, float | None]:
    path = _require_relative_body(
        root,
        row,
        path_field=f"{prefix}_price_source_object_key",
        sha_field=f"{prefix}_price_source_content_sha256",
    )
    if path.suffix != ".parquet":
        raise RuntimeError("cash-scale KRX price body must be parquet")
    etag = str(row.get(f"{prefix}_price_source_etag") or "").strip('"')
    if re.fullmatch(r"[0-9a-f]{32}(?:-[0-9]+)?", etag) is None:
        raise RuntimeError("cash-scale KRX price object ETag is invalid")
    if "-" not in etag:
        md5 = hashlib.md5(usedforsecurity=False)
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                md5.update(chunk)
        if md5.hexdigest() != etag:
            raise RuntimeError("cash-scale KRX price object ETag/body mismatch")
    return _price_observation(
        path,
        schema=str(row.get(f"{prefix}_price_source_schema") or ""),
        ticker=str(row["ticker"]),
        trade_date=trade_date,
    )


def _verify_price_bodies(root: Path, row: dict[str, object]) -> None:
    if row.get("price_source") != "KRX":
        raise RuntimeError("cash-scale price source must be KRX")
    previous_date = pd.Timestamp(row["previous_trade_date"]).date()
    adjustment_date = pd.Timestamp(row["adjustment_trade_date"]).date()
    previous_close, _ = _price_body(
        root, row, prefix="previous", trade_date=previous_date,
    )
    applied_close, reference = _price_body(
        root, row, prefix="adjustment", trade_date=adjustment_date,
    )
    if reference is None or reference <= 0:
        raise RuntimeError("KRX adjustment body lacks a positive reference price")
    declared_previous = _number(
        row["raw_previous_close"], field="declared raw previous close",
    )
    declared_applied = _number(
        row["raw_applied_close"], field="declared raw applied close",
    )
    declared_reference = _number(
        row["raw_reference_price"], field="declared raw reference price",
    )
    for observed, declared, label in (
        (previous_close, declared_previous, "previous close"),
        (applied_close, declared_applied, "applied close"),
        (reference, declared_reference, "reference price"),
    ):
        if not math.isclose(observed, declared, rel_tol=0, abs_tol=1e-8):
            raise RuntimeError(f"KRX evidence {label} changed")
    expected = _number(
        row["expected_price_factor"], field="expected price factor",
    )
    if not math.isclose(
        reference / previous_close, expected, rel_tol=0, abs_tol=5e-13,
    ):
        raise RuntimeError("cash-scale expected price factor is inconsistent")


def _visible_html(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("utf-8", errors="replace")
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))


def _body_has_date(visible: str, value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    digits = re.sub(r"[^0-9]", "", visible)
    return pd.Timestamp(value).strftime("%Y%m%d") in digits


def _exact_labelled_values(payloads: Sequence[bytes]) -> dict[str, list[str]]:
    """Extract exact table-label values from an official disclosure body.

    Combined detachment notices can contain old/new values in correction
    prose.  Searching the whole visible body would let an unrelated number or
    reason satisfy the contract.  Only the current labelled table cells are
    admissible, and callers require exact cardinality.
    """
    # Import lazily so the evidence module stays usable by the corporate-action
    # publisher without introducing an import cycle at module import time.
    from pipeline.silver import corporate_actions as action_parser

    values: dict[str, list[str]] = {
        "reference": [],
        "reason": [],
    }
    for payload in payloads:
        decoded = action_parser._decode_document(payload)
        for cells in action_parser._table_rows(decoded):
            for index, cell in enumerate(cells[:-1]):
                label = action_parser._compact(cell)
                value = re.sub(r"\s+", " ", cells[index + 1]).strip()
                if label in {"기준가격", "기준가격원"}:
                    values["reference"].append(value)
                elif label == "사유":
                    values["reason"].append(value)
    return values


def _has_exact_decimal_token(visible: str, value: object) -> bool:
    rendered = format(Decimal(str(value)).normalize(), "f")
    return re.search(
        rf"(?<![0-9.]){re.escape(rendered)}(?![0-9.])",
        visible,
    ) is not None


def _verify_stock_dividend_security_classes(
    visible: str,
    *,
    entitlement: object,
    distributed: object,
) -> None:
    classes = (entitlement, distributed)
    if classes == ("COMMON", "COMMON"):
        if "보통주" not in visible or any(
            token in visible
            for token in ("보통주주및우선주주", "신형우선주")
        ):
            raise RuntimeError(
                "stock-dividend component security class changed"
            )
        return
    if classes == ("COMMON_AND_PREFERRED", "NEW_PREFERRED"):
        if any(
            token not in visible
            for token in ("보통주주및우선주주", "신형우선주")
        ):
            raise RuntimeError(
                "stock-dividend component security class changed"
            )
        return
    raise RuntimeError("stock-dividend component security class changed")


def _verify_support_body(
    root: Path,
    parent: dict[str, object],
    row: dict[str, object],
) -> None:
    path = _require_relative_body(
        root,
        row,
        path_field="support_action_body_path",
        sha_field="support_action_body_sha256",
    )
    source = str(row.get("support_action_source") or "")
    action_type = str(row.get("support_action_type") or "")
    action_key = str(row.get("support_action_key") or "")
    raw_report_name = str(row.get("support_report_name") or "")
    report_name = re.sub(r"\s+", "", raw_report_name)
    adjustment_date = pd.Timestamp(parent["adjustment_trade_date"]).date()
    if any(token in report_name for token in ("철회", "취소", "부결")):
        raise RuntimeError(
            "withdrawn/cancelled corporate action cannot support cash scale"
        )
    if row.get("support_action_scope") != "ISSUER":
        raise RuntimeError("cash-scale support action must be issuer scoped")
    if row.get("support_semantic_role") not in {
        "ADJUSTMENT_COMPONENT", "CORROBORATION",
    }:
        raise RuntimeError("cash-scale support semantic role is invalid")
    if row["support_semantic_role"] == "ADJUSTMENT_COMPONENT":
        allowed_component = (
            (source == "DART_STRUCTURED" and action_type == "bonus_issue")
            or (
                source in {"DART_DISCLOSURE", "KRX_KIND"}
                and action_type == "stock_dividend"
            )
        )
        if not allowed_component:
            raise RuntimeError("unsupported adjustment-component action semantics")
        entitlement = row.get("support_entitlement_security_class")
        distributed = row.get("support_distributed_security_class")
        if entitlement not in {"COMMON", "PREFERRED", "COMMON_AND_PREFERRED"}:
            raise RuntimeError("support entitlement security class is invalid")
        if distributed not in {"COMMON", "PREFERRED", "NEW_PREFERRED"}:
            raise RuntimeError("support distributed security class is invalid")
    _support_groups(row.get("support_semantic_group_keys"))
    if source == "DART_DISCLOSURE":
        if _RECEIPT_PATTERN.fullmatch(action_key) is None:
            raise RuntimeError("DART support action key must be a receipt")
        if not zipfile.is_zipfile(path):
            raise RuntimeError("DART support action body must be an official ZIP")
        with zipfile.ZipFile(path) as archive:
            payloads = [archive.read(name) for name in archive.namelist()]
            visible = " ".join(_visible_bytes(payload) for payload in payloads)
        compact = re.sub(r"\s+", "", visible)
        if action_type == "ex_dividend":
            if (
                "배당락" not in compact or "주식배당" not in compact
                or "배당락" not in report_name or "주식배당" not in report_name
            ):
                raise RuntimeError(
                    "DART support body is not a stock-dividend ex notice"
                )
            if not _body_has_date(compact, adjustment_date):
                raise RuntimeError("DART support body lacks the adjustment date")
        elif action_type == "rights_detachment":
            if (
                "권리락" not in compact or "무상증자" not in compact
                or "권리락" not in report_name
            ):
                raise RuntimeError(
                    "DART support body is not a bonus rights-detachment notice"
                )
            if not _body_has_date(compact, adjustment_date):
                raise RuntimeError("DART support body lacks the adjustment date")
        elif action_type == "stock_dividend":
            if (
                "주식배당" not in compact or "결정" not in compact
                or "주식배당결정" not in report_name
            ):
                raise RuntimeError(
                    "DART support body is not a stock-dividend decision"
                )
            if any(token in compact for token in ("철회", "취소", "부결")):
                raise RuntimeError(
                    "withdrawn/cancelled stock-dividend body is inadmissible"
                )
            if re.search(
                r"보통주(?:식)?[^0-9]{0,30}0(?:\.0+)?주", compact,
            ):
                raise RuntimeError(
                    "zero-share stock-dividend body is inadmissible"
                )
            if not _body_has_date(compact, row.get("support_record_date")):
                raise RuntimeError("stock-dividend body lacks its record date")
            if row.get("support_ex_date") is not None and not pd.isna(
                row.get("support_ex_date")
            ):
                raise RuntimeError("stock-dividend record date cannot be an ex-date")
            if row.get("support_semantic_role") == "ADJUSTMENT_COMPONENT":
                numerator = row.get("support_ratio_numerator")
                denominator = row.get("support_ratio_denominator")
                if (
                    numerator is None or denominator is None
                    or pd.isna(numerator) or pd.isna(denominator)
                ):
                    raise RuntimeError("stock-dividend component ratio is missing")
                rendered_ratio = Decimal(str(numerator)) / Decimal(
                    str(denominator)
                )
                if not _has_exact_decimal_token(compact, rendered_ratio):
                    raise RuntimeError("stock-dividend component ratio changed")
                _verify_stock_dividend_security_classes(
                    compact,
                    entitlement=row.get(
                        "support_entitlement_security_class"
                    ),
                    distributed=row.get(
                        "support_distributed_security_class"
                    ),
                )
        elif action_type == "combined_detachment":
            if "권배락" not in compact or "권배락" not in report_name:
                raise RuntimeError(
                    "DART support body is not a combined rights/dividend notice"
                )
            if not _body_has_date(compact, adjustment_date):
                raise RuntimeError("combined-detachment body lacks adjustment date")
            labelled = _exact_labelled_values(payloads)
            if (
                len(labelled["reference"]) != 1
                or len(labelled["reason"]) != 1
            ):
                raise RuntimeError(
                    "combined-detachment labelled fields are missing/ambiguous"
                )
            reason = re.sub(r"\s+", " ", str(
                row.get("support_reason") or ""
            )).strip()
            body_reason = re.sub(r"\s+", " ", labelled["reason"][0]).strip()
            reference = row.get("support_reference_price")
            if (
                not reason or reason != body_reason
                or "무상증자" not in re.sub(r"\s+", "", body_reason)
                or "배당" not in re.sub(r"\s+", "", body_reason)
            ):
                raise RuntimeError("combined-detachment labelled reason changed")
            if reference is None or pd.isna(reference) or not math.isclose(
                float(reference), float(parent["raw_reference_price"]),
                rel_tol=0, abs_tol=1e-8,
            ):
                raise RuntimeError("combined-detachment reference price mismatch")
            body_reference = _number(
                labelled["reference"][0],
                field="combined-detachment labelled reference price",
            )
            if not math.isclose(
                float(reference), body_reference, rel_tol=0, abs_tol=1e-8,
            ):
                raise RuntimeError(
                    "combined-detachment labelled reference price changed"
                )
        else:
            raise RuntimeError(
                f"unsupported DART cash-scale support type: {action_type}"
            )
    elif source == "DART_STRUCTURED":
        if action_type != "bonus_issue" or "무상증자" not in report_name:
            raise RuntimeError("structured support must be an issuer bonus issue")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("structured bonus support body is invalid JSON") from exc
        if str(payload.get("rcept_no") or "") != action_key:
            raise RuntimeError("structured support receipt/body mismatch")
        ratio = _number(
            payload.get("nstk_ascnt_ps_ostk"),
            field="bonus shares per ordinary share",
        )
        numerator = row.get("support_ratio_numerator")
        denominator = row.get("support_ratio_denominator")
        if (
            numerator is None or denominator is None
            or pd.isna(numerator) or pd.isna(denominator)
            or not math.isclose(
                float(numerator) / float(denominator),
                ratio,
                rel_tol=0,
                abs_tol=5e-13,
            )
        ):
            raise RuntimeError("structured bonus ratio mismatch")
        expected = row.get("support_expected_price_factor")
        if expected is None or pd.isna(expected) or not math.isclose(
            float(expected), 1.0 / (1.0 + ratio), rel_tol=0, abs_tol=5e-13,
        ):
            raise RuntimeError("structured bonus expected factor mismatch")
        if (
            row.get("support_entitlement_security_class") != "COMMON"
            or row.get("support_distributed_security_class") != "COMMON"
        ):
            raise RuntimeError("structured bonus security-class semantics changed")
    elif source == "KRX_KIND":
        role = str(row.get("support_semantic_role") or "")
        if role == "CORROBORATION":
            if action_type not in {
                "ex_dividend", "rights_detachment", "combined_detachment",
            }:
                raise RuntimeError("KIND corroboration action type changed")
            notice = parse_kind_reference_notice(path.read_bytes())
            reference = row.get("support_reference_price")
            expected_report_name = (
                KIND_REFERENCE_REPORT_NAME_99311
                if notice.form_id == "99311"
                else KIND_REFERENCE_REPORT_NAME_70767
            )
            if (
                notice.action_type != action_type
                or (
                    notice.ticker is not None
                    and notice.ticker != str(parent.get("ticker") or "").zfill(6)
                )
                or notice.effective_date != adjustment_date
                or row.get("support_ex_date") != adjustment_date
                or row.get("support_record_date") is not None
                or row.get("support_entitlement_security_class")
                != notice.security_class
                or row.get("support_distributed_security_class") is not None
                or reference is None
                or pd.isna(reference)
                or not math.isclose(
                    float(reference), notice.reference_price,
                    rel_tol=0, abs_tol=1e-8,
                )
                or not math.isclose(
                    float(reference), float(parent["raw_reference_price"]),
                    rel_tol=0, abs_tol=1e-8,
                )
                or re.sub(r"\s+", " ", str(row.get("support_reason") or "")).strip()
                != notice.reason
                or raw_report_name != expected_report_name
            ):
                raise RuntimeError("KIND reference notice semantics changed")
            return
        if role != "ADJUSTMENT_COMPONENT" or action_type != "stock_dividend":
            raise RuntimeError("KIND cash-scale component must be stock_dividend")
        component = parse_kind_stock_dividend_component(path.read_bytes())
        numerator = row.get("support_ratio_numerator")
        denominator = row.get("support_ratio_denominator")
        if (
            numerator is None or denominator is None
            or pd.isna(numerator) or pd.isna(denominator)
            or float(numerator) <= 0 or float(denominator) <= 0
        ):
            raise RuntimeError("KIND stock-dividend ratio is missing")
        if not math.isclose(
            float(numerator) / float(denominator),
            component.ratio_numerator / component.ratio_denominator,
            rel_tol=0,
            abs_tol=5e-13,
        ):
            raise RuntimeError("KIND stock-dividend ratio changed")
        if (
            row.get("support_record_date") != component.record_date
            or row.get("support_entitlement_security_class")
            != component.entitlement_security_class
            or row.get("support_distributed_security_class")
            != component.distributed_security_class
            or raw_report_name != KIND_COMPONENT_REPORT_NAME_61474
            or component.report_name != KIND_COMPONENT_REPORT_NAME_61474
        ):
            raise RuntimeError("KIND stock-dividend semantics changed")
    else:
        raise RuntimeError(f"unsupported cash-scale support source: {source}")


def _visible_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("utf-8", errors="replace")
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))


def _verify_cash_bodies(root: Path, row: dict[str, object]) -> None:
    action_body = _require_relative_body(
        root,
        row,
        path_field="cash_action_body_path",
        sha_field="cash_action_body_sha256",
    )
    economic_body = _require_relative_body(
        root,
        row,
        path_field="cash_economic_body_path",
        sha_field="cash_economic_sha256",
    )
    status = str(row.get("cash_source_evidence_status") or "")
    schema = str(row.get("cash_economic_body_schema") or "")
    if status == "VERIFIED_OPENDART_DOCUMENT":
        if schema != "OPENDART_DOCUMENT_ZIP_V1":
            raise RuntimeError("OpenDART cash body schema mismatch")
        if action_body != economic_body or not zipfile.is_zipfile(economic_body):
            raise RuntimeError("OpenDART cash evidence must bind the exact ZIP")
    elif status == "VERIFIED_DART_VIEWER_BODY":
        if schema != "DART_VIEWER_HTML_V1":
            raise RuntimeError("DART viewer cash body schema mismatch")
        if zipfile.is_zipfile(economic_body):
            raise RuntimeError("DART viewer economic body cannot be a ZIP")
        visible = _visible_html(economic_body)
        if "배당" not in visible or not re.search(r"현금|원", visible):
            raise RuntimeError("DART viewer body lacks cash-dividend semantics")
    elif status == "VERIFIED_REVIEWED_SOURCE_ERRATUM":
        if schema != "REVIEWED_PERIODIC_JSON_V1":
            raise RuntimeError("reviewed cash body schema mismatch")
        if not zipfile.is_zipfile(action_body):
            raise RuntimeError("reviewed cash evidence lacks its source ZIP")
        try:
            payload = json.loads(economic_body.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("reviewed cash economic body is invalid JSON") from exc
        if not isinstance(payload.get("list"), list):
            raise RuntimeError("reviewed cash economic response has no list")
    else:
        raise RuntimeError(
            f"unsupported terminal cash evidence status: {status}"
        )


def _validate_manifest_row(root: Path, row: dict[str, object]) -> dict[str, object]:
    missing = sorted(set(MANIFEST_ROW_COLUMNS) - set(row))
    if missing:
        raise RuntimeError(f"cash-scale evidence row fields missing: {missing}")
    evidence_key = str(row.get("evidence_key") or "").strip()
    if not evidence_key or len(evidence_key) > 300:
        raise RuntimeError("cash-scale evidence key is invalid")
    if _TICKER_PATTERN.fullmatch(str(row.get("ticker") or "")) is None:
        raise RuntimeError("cash-scale evidence ticker is invalid")
    if _RECEIPT_PATTERN.fullmatch(
        str(row.get("cash_receipt_no") or "")
    ) is None:
        raise RuntimeError("cash-scale cash receipt is invalid")
    if row.get("cash_scale_basis") != PRE_EVENT_PRICE_SCALE:
        raise RuntimeError("changed-scale evidence must select PRE_EVENT_PRICE_SCALE")
    for field in (
        "cash_action_body_sha256", "cash_economic_sha256",
        "previous_price_source_content_sha256",
        "adjustment_price_source_content_sha256",
    ):
        if _SHA_PATTERN.fullmatch(str(row.get(field) or "")) is None:
            raise RuntimeError(f"cash-scale evidence SHA is invalid: {field}")
    previous_date = pd.Timestamp(row["previous_trade_date"]).date()
    adjustment_date = pd.Timestamp(row["adjustment_trade_date"]).date()
    if previous_date >= adjustment_date:
        raise RuntimeError("cash-scale previous date must precede adjustment date")
    _verify_cash_bodies(root, row)
    _verify_price_bodies(root, row)
    normalized = dict(row)
    normalized["previous_trade_date"] = previous_date
    normalized["adjustment_trade_date"] = adjustment_date
    for field in (
        "raw_previous_close", "raw_applied_close", "raw_reference_price",
        "expected_price_factor",
    ):
        normalized[field] = _number(row[field], field=field)
    expected_row_sha = _manifest_row_sha(normalized)
    declared_row_sha = str(row.get("manifest_row_sha256") or "")
    if declared_row_sha != expected_row_sha:
        raise RuntimeError(
            "cash-scale manifest row digest mismatch: "
            f"evidence_key={evidence_key}"
        )
    normalized["manifest_row_sha256"] = expected_row_sha
    return normalized


def _validate_support_row(
    root: Path,
    parent: dict[str, object],
    row: dict[str, object],
) -> dict[str, object]:
    missing = sorted(set(MANIFEST_SUPPORT_ACTION_COLUMNS) - set(row))
    if missing:
        raise RuntimeError(f"cash-scale support row fields missing: {missing}")
    if str(row.get("evidence_key") or "") != str(parent["evidence_key"]):
        raise RuntimeError("cash-scale support evidence key mismatch")
    if str(row.get("target_cash_receipt_no") or "") != str(
        parent["cash_receipt_no"]
    ):
        raise RuntimeError("cash-scale support target cash receipt mismatch")
    if _SHA_PATTERN.fullmatch(
        str(row.get("support_action_body_sha256") or "")
    ) is None:
        raise RuntimeError("cash-scale support body SHA is invalid")
    for field in (
        "target_adjustment_date", "support_announcement_date", "support_ex_date",
        "support_record_date",
    ):
        value = row.get(field)
        row[field] = (
            None if value is None or pd.isna(value)
            else pd.Timestamp(value).date()
        )
    if row["target_adjustment_date"] != pd.Timestamp(
        parent["adjustment_trade_date"]
    ).date():
        raise RuntimeError("cash-scale support target adjustment date mismatch")
    for field in (
        "support_ratio_numerator", "support_ratio_denominator",
        "support_expected_price_factor", "support_reference_price",
    ):
        value = row.get(field)
        row[field] = (
            None if value is None or pd.isna(value)
            else _number(value, field=field)
        )
    _verify_support_body(root, parent, row)
    expected_row_sha = _manifest_support_row_sha(row)
    if str(row.get("manifest_support_row_sha256") or "") != expected_row_sha:
        raise RuntimeError(
            "cash-scale support manifest row digest mismatch: "
            f"evidence_key={parent['evidence_key']} "
            f"action={row.get('support_action_key')}"
        )
    row["manifest_support_row_sha256"] = expected_row_sha
    return row


def _support_groups(value: object) -> tuple[str, ...]:
    rendered = str(value or "")
    try:
        decoded = json.loads(rendered)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "cash-scale support semantic groups must be canonical JSON"
        ) from exc
    if (
        not isinstance(decoded, list) or not decoded
        or any(not isinstance(item, str) or not item.strip() for item in decoded)
    ):
        raise RuntimeError("cash-scale support semantic group list is invalid")
    canonical = sorted(set(item.strip() for item in decoded))
    if len(canonical) != len(decoded) or decoded != canonical:
        raise RuntimeError(
            "cash-scale support semantic groups must be sorted and unique"
        )
    if any(len(item) > 300 for item in canonical):
        raise RuntimeError("cash-scale support semantic group is too long")
    exact = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":"),
    )
    if rendered != exact:
        raise RuntimeError(
            "cash-scale support semantic groups are not canonically rendered"
        )
    return tuple(canonical)


def _support_group_count(support: pd.DataFrame) -> int:
    groups: set[str] = set()
    for value in support.get(
        "support_semantic_group_keys", pd.Series(dtype="object")
    ):
        groups.update(_support_groups(value))
    return len(groups)


def _validate_support_groups(
    parent: dict[str, object],
    support: pd.DataFrame,
) -> None:
    if support.empty:
        raise RuntimeError("changed-scale evidence requires support actions")
    identity = [
        "support_action_source", "support_action_key", "support_action_type",
    ]
    if support.duplicated(identity).any():
        raise RuntimeError("duplicate cash-scale support action identity")
    memberships: dict[str, list[int]] = {}
    for index, row in support.iterrows():
        groups = _support_groups(row["support_semantic_group_keys"])
        if (
            row["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
            and len(groups) != 1
        ):
            raise RuntimeError(
                "an adjustment component must belong to exactly one group"
            )
        for group_key in groups:
            memberships.setdefault(group_key, []).append(index)
    for group_key, indices in memberships.items():
        group = support.loc[indices]
        component_count = int(group[
            "support_semantic_role"
        ].eq("ADJUSTMENT_COMPONENT").sum())
        if component_count != 1:
            raise RuntimeError(
                "each cash-scale semantic group requires exactly one "
                f"adjustment component: group={group_key} "
                f"components={component_count}"
            )
    expected_count = len(support)
    expected_groups = len(memberships)
    expected_digest = support_manifest_digest(support)
    if int(parent.get("support_action_count", -1)) != expected_count:
        raise RuntimeError("cash-scale parent/support action-count mismatch")
    if int(parent.get("support_semantic_group_count", -1)) != expected_groups:
        raise RuntimeError("cash-scale parent/support group-count mismatch")
    if str(parent.get("support_action_digest") or "") != expected_digest:
        raise RuntimeError("cash-scale parent/support digest mismatch")


def _validate_support_family_bindings(
    root: Path,
    parents: pd.DataFrame,
    support: pd.DataFrame,
) -> None:
    """Rebind every DART adjustment child to an admissible official family."""
    if support.empty:
        return
    components = support[
        support["support_semantic_role"].eq("ADJUSTMENT_COMPONENT")
        & (
            (
                support["support_action_source"].eq("DART_DISCLOSURE")
                & support["support_action_type"].eq("stock_dividend")
            )
            | (
                support["support_action_source"].eq("DART_STRUCTURED")
                & support["support_action_type"].eq("bonus_issue")
            )
        )
    ]
    if components.empty:
        return
    verified = verify_support_action_families(root)
    parent_by_key = {
        str(row["evidence_key"]): row for row in parents.to_dict("records")
    }
    for child in components.to_dict("records"):
        parent = parent_by_key.get(str(child["evidence_key"]))
        if parent is None:
            raise RuntimeError("DART support-family child has no parent")
        receipt = str(child["support_action_key"])
        action_type = str(child["support_action_type"])
        ticker = str(parent["ticker"])
        matches = [
            entry for entry in verified.entries
            if entry.ticker == ticker
            and entry.action_type == action_type
            and entry.terminal_economic_receipt_no == receipt
        ]
        if len(matches) != 1:
            raise RuntimeError(
                "DART adjustment child does not bind exactly one verified "
                f"support family: ticker={ticker} action={action_type} "
                f"receipt={receipt} matches={len(matches)}"
            )
        entry = matches[0]
        if not entry.terminal_admissible or entry.terminal_ratio is None:
            raise RuntimeError(
                "DART adjustment child binds an inadmissible support family: "
                f"root={entry.root_receipt_no} status={entry.terminal_status}"
            )
        family_sources = [
            source for source in entry.sources
            if source.receipt_no == receipt
        ]
        if len(family_sources) != 1:
            raise RuntimeError("support-family terminal source is ambiguous")
        family_source = family_sources[0]
        numerator = child.get("support_ratio_numerator")
        denominator = child.get("support_ratio_denominator")
        if (
            numerator is None or denominator is None
            or pd.isna(numerator) or pd.isna(denominator)
            or float(denominator) <= 0
            or not math.isclose(
                float(numerator) / float(denominator),
                float(entry.terminal_ratio),
                rel_tol=0,
                abs_tol=5e-13,
            )
            or str(child.get("support_report_name") or "")
            != family_source.report_name
            or child.get("support_announcement_date")
            != pd.Timestamp(family_source.receipt_date).date()
        ):
            raise RuntimeError(
                "DART adjustment child/family economic-row parity failed: "
                f"receipt={receipt}"
            )
        body_path = str(child["support_action_body_path"])
        body_sha = str(child["support_action_body_sha256"])
        if action_type == "bonus_issue":
            if (
                family_source.structured_path is None
                or family_source.structured_sha256 is None
                or body_path != family_source.structured_path
                or body_sha != family_source.structured_sha256
            ):
                raise RuntimeError(
                    "bonus adjustment child/family structured-body parity failed"
                )
        else:
            expected_path = (
                f"corporate_actions/dart/documents/year={receipt[:4]}/"
                f"corp={ticker}/rcept={receipt}.zip"
            )
            if body_path != expected_path:
                raise RuntimeError(
                    "stock-dividend child/family exact ZIP identity failed"
                )


def _validate_kind_support_bindings(
    root: Path,
    parents: pd.DataFrame,
    support: pd.DataFrame,
) -> None:
    """Rebind every KRX child to the immutable KIND support manifest."""
    declared = verify_kind_support_manifest(root)
    children = support[support["support_action_source"].eq("KRX_KIND")]
    if not declared and children.empty:
        return
    parent_identity = {
        str(row["evidence_key"]): (
            str(row["ticker"]), str(row["cash_receipt_no"]),
            pd.Timestamp(row["adjustment_trade_date"]).date(),
        )
        for row in parents.to_dict("records")
    }
    child_by_identity: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for child in children.to_dict("records"):
        parent = parent_identity.get(str(child["evidence_key"]))
        if parent is None:
            raise RuntimeError("KIND support child has no parent")
        ticker, cash_receipt, adjustment_date = parent
        if (
            str(child.get("target_cash_receipt_no") or "") != cash_receipt
            or pd.Timestamp(child.get("target_adjustment_date")).date()
            != adjustment_date
        ):
            raise RuntimeError("KIND support child target/parent parity failed")
        identity = (
            ticker,
            str(child["support_action_key"]),
            str(child["support_action_type"]),
            str(child["support_semantic_role"]),
        )
        child_by_identity.setdefault(identity, []).append(child)
    declared_by_identity = {
        (
            str(item["ticker"]),
            str(item["support_action_key"]),
            str(item["support_action_type"]),
            str(item["support_semantic_role"]),
        ): item
        for item in declared
    }
    if set(child_by_identity) != set(declared_by_identity) or any(
        len(rows) != 1 for rows in child_by_identity.values()
    ):
        raise RuntimeError(
            "KIND manifest/source children are not an exact one-to-one set"
        )

    date_fields = {
        "target_adjustment_date", "support_announcement_date", "support_ex_date",
        "support_record_date",
    }
    numeric_fields = {
        "support_ratio_numerator", "support_ratio_denominator",
        "support_expected_price_factor", "support_reference_price",
    }
    fields = (
        "support_action_source", "support_action_key", "support_action_type",
        "target_cash_receipt_no", "target_adjustment_date",
        "support_action_body_path", "support_action_body_sha256",
        "support_announcement_date", "support_ex_date", "support_record_date",
        "support_ratio_numerator", "support_ratio_denominator",
        "support_entitlement_security_class",
        "support_distributed_security_class",
        "support_expected_price_factor", "support_reference_price",
        "support_reason", "support_report_name", "support_action_scope",
        "support_semantic_role",
    )
    for identity, item in declared_by_identity.items():
        child = child_by_identity[identity][0]
        for field in fields:
            expected = item.get(field)
            actual = child.get(field)
            if field in date_fields:
                expected = (
                    None if expected is None
                    else pd.Timestamp(expected).date()
                )
                actual = (
                    None if actual is None or pd.isna(actual)
                    else pd.Timestamp(actual).date()
                )
            elif field in numeric_fields:
                expected = (
                    None if expected is None
                    else float(expected)
                )
                actual = (
                    None if actual is None or pd.isna(actual)
                    else float(actual)
                )
                if expected is not None and actual is not None:
                    tolerance = 1e-8 if field == "support_reference_price" else 5e-13
                    if math.isclose(actual, expected, rel_tol=0, abs_tol=tolerance):
                        continue
            if actual != expected:
                raise RuntimeError(
                    "KIND manifest/source child parity failed: "
                    f"identity={identity} field={field}"
                )


def verify_source_evidence_manifest(base: str) -> VerifiedScaleSourceEvidence:
    """Verify every source body and return stable evidence rows.

    A missing manifest is represented by an empty, content-addressed row set.
    This permits test/minimal snapshots with no changed-scale events.  The
    total-return preflight independently requires exact 1:1 coverage whenever
    a changed-scale cash event is actually observed.
    """
    root = Path(base).expanduser().resolve()
    path = root / MANIFEST_RELATIVE_PATH
    empty = pd.DataFrame(columns=SOURCE_EVIDENCE_COLUMNS)
    empty_support = pd.DataFrame(columns=SUPPORT_ACTION_COLUMNS)
    if not path.is_file():
        digest = source_manifest_digest(empty)
        return VerifiedScaleSourceEvidence(
            frame=empty,
            support_frame=empty_support,
            manifest_path=None,
            manifest_sha256=hashlib.sha256(b"").hexdigest(),
            row_count=0,
            row_digest=digest,
        )
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid cash-scale evidence manifest JSON") from exc
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    if raw != canonical:
        raise RuntimeError("cash-scale evidence manifest is not canonical JSON bytes")
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version", "complete", "row_count", "row_digest",
        "support_action_count", "support_action_digest",
        "support_semantic_group_count", "evidence",
    }:
        raise RuntimeError("cash-scale evidence manifest fields changed")
    if payload.get("schema_version") != SOURCE_EVIDENCE_CONTRACT:
        raise RuntimeError("unsupported cash-scale source evidence schema")
    if payload.get("complete") is not True:
        raise RuntimeError("cash-scale source evidence manifest is not complete")
    entries = payload.get("evidence")
    if not isinstance(entries, list):
        raise RuntimeError("cash-scale source evidence must be a list")
    rows: list[dict[str, object]] = []
    support_rows: list[dict[str, object]] = []
    for item in entries:
        if not isinstance(item, dict) or set(item) != {
            *MANIFEST_ROW_COLUMNS, "manifest_row_sha256", "support_actions",
        }:
            raise RuntimeError("cash-scale evidence parent fields changed")
        raw_parent = dict(item)
        raw_support = raw_parent.pop("support_actions", None)
        if not isinstance(raw_support, list):
            raise RuntimeError("cash-scale support_actions must be a list")
        if any(
            not isinstance(support, dict)
            or set(support) != {
                *MANIFEST_SUPPORT_ACTION_COLUMNS, "manifest_support_row_sha256",
            }
            for support in raw_support
        ):
            raise RuntimeError("cash-scale support row fields changed")
        normalized_support = [
            _validate_support_row(root, raw_parent, dict(support))
            for support in raw_support
        ]
        support_frame = pd.DataFrame(normalized_support)
        for column in SUPPORT_ACTION_COLUMNS:
            if column not in support_frame:
                support_frame[column] = None
        support_frame = support_frame[list(SUPPORT_ACTION_COLUMNS)]
        _validate_support_groups(raw_parent, support_frame)
        rows.append(_validate_manifest_row(root, raw_parent))
        support_rows.extend(normalized_support)
    frame = pd.DataFrame(rows)
    for column in SOURCE_EVIDENCE_COLUMNS:
        if column not in frame:
            frame[column] = None
    frame = frame[list(SOURCE_EVIDENCE_COLUMNS)]
    support_frame = pd.DataFrame(support_rows)
    for column in SUPPORT_ACTION_COLUMNS:
        if column not in support_frame:
            support_frame[column] = None
    support_frame = support_frame[list(SUPPORT_ACTION_COLUMNS)]
    _validate_support_family_bindings(root, frame, support_frame)
    _validate_kind_support_bindings(root, frame, support_frame)
    if frame["evidence_key"].astype(str).duplicated().any():
        raise RuntimeError("duplicate cash-scale evidence keys")
    identity = [
        "ticker", "cash_receipt_no", "adjustment_trade_date",
    ]
    if frame.duplicated(identity).any():
        raise RuntimeError("duplicate cash-scale cash/date evidence")
    support_identity = [
        "evidence_key", "support_action_source", "support_action_key",
        "support_action_type",
    ]
    if support_frame.duplicated(support_identity).any():
        raise RuntimeError("duplicate cash-scale support action rows")
    row_digest = source_manifest_digest(frame)
    if int(payload.get("row_count", -1)) != len(frame):
        raise RuntimeError("cash-scale manifest row-count mismatch")
    if payload.get("row_digest") != row_digest:
        raise RuntimeError("cash-scale manifest aggregate digest mismatch")
    support_row_digest = support_manifest_digest(support_frame)
    if int(payload.get("support_action_count", -1)) != len(support_frame):
        raise RuntimeError("cash-scale manifest support-count mismatch")
    if payload.get("support_action_digest") != support_row_digest:
        raise RuntimeError("cash-scale manifest support digest mismatch")
    if int(payload.get("support_semantic_group_count", -1)) != (
        _support_group_count(support_frame)
    ):
        raise RuntimeError("cash-scale manifest support-group count mismatch")
    return VerifiedScaleSourceEvidence(
        frame=frame,
        support_frame=support_frame,
        manifest_path=str(path),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        row_count=len(frame),
        row_digest=row_digest,
    )


def external_evidence_paths(base: str) -> tuple[Path, ...]:
    """Return every body declared by a successfully verified manifest."""
    verified = verify_source_evidence_manifest(base)
    if verified.frame.empty:
        return ()
    root = Path(base).expanduser().resolve()
    paths = {
        root / MANIFEST_RELATIVE_PATH,
        _verify_price_object_receipt(root, verified.frame),
    }
    for row in verified.frame.to_dict("records"):
        paths.update({
            root / str(row["cash_action_body_path"]),
            root / str(row["cash_economic_body_path"]),
            root / str(row["previous_price_source_object_key"]),
            root / str(row["adjustment_price_source_object_key"]),
        })
    for row in verified.support_frame.to_dict("records"):
        paths.add(root / str(row["support_action_body_path"]))
    if (
        verified.support_frame["support_semantic_role"].eq(
            "ADJUSTMENT_COMPONENT"
        )
        & verified.support_frame["support_action_source"].isin(
            {"DART_DISCLOSURE", "DART_STRUCTURED"}
        )
    ).any():
        paths.update(
            root / relative
            for relative in support_family_external_evidence_paths(root)
        )
    if verified.support_frame["support_action_source"].eq("KRX_KIND").any():
        paths.update(kind_external_evidence_paths(root))
    return tuple(sorted(paths, key=lambda item: item.relative_to(root).as_posix()))


def _verify_price_object_receipt(root: Path, parents: pd.DataFrame) -> Path:
    """Bind remote HEAD receipts to every local KRX body used by parents."""
    path = root / PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    try:
        raw_manifest = path.read_bytes()
        payload = json.loads(raw_manifest)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"missing/invalid cash-scale price-object receipt: {path}"
        ) from exc
    canonical_manifest = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    if raw_manifest != canonical_manifest:
        raise RuntimeError("cash-scale price-object receipt is not canonical JSON")
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version", "complete", "source_bucket", "source_prefix",
        "object_count", "object_digest", "objects",
    }:
        raise RuntimeError("cash-scale price-object receipt fields changed")
    rows = payload.get("objects")
    if (
        payload.get("schema_version") != PRICE_OBJECT_MANIFEST_CONTRACT
        or payload.get("complete") is not True
        or not isinstance(rows, list)
        or int(payload.get("object_count", -1)) != len(rows)
    ):
        raise RuntimeError("cash-scale price-object receipt contract mismatch")
    rendered_rows = json.dumps(
        rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    if payload.get("object_digest") != hashlib.sha256(rendered_rows).hexdigest():
        raise RuntimeError("cash-scale price-object receipt digest mismatch")
    bucket = str(payload.get("source_bucket") or "")
    prefix = str(payload.get("source_prefix") or "").strip("/")
    if not bucket or "/" in bucket:
        raise RuntimeError("cash-scale price-object source bucket is invalid")

    declared: dict[str, dict[str, object]] = {}
    row_fields = {
        "trade_date", "source_object_key", "local_path", "etag",
        "content_length", "content_sha256", "version_id",
        "server_side_encryption", "source_schema",
    }
    for raw in rows:
        if not isinstance(raw, dict) or set(raw) != row_fields:
            raise RuntimeError("cash-scale price-object receipt row fields changed")
        local_path = str(raw.get("local_path") or "")
        relative = Path(local_path)
        etag = str(raw.get("etag") or "").strip('"').lower()
        version_id = raw.get("version_id")
        if (
            not local_path or relative.is_absolute() or ".." in relative.parts
            or local_path in declared
            or re.fullmatch(r"[0-9a-f]{32}", etag) is None
            or (version_id is not None and version_id != "")
            or raw.get("source_schema") != "marcap_parquet_v1"
        ):
            raise RuntimeError("cash-scale price-object provenance row is invalid")
        expected_source = "/".join(
            item for item in (prefix, local_path) if item
        )
        if raw.get("source_object_key") != expected_source:
            raise RuntimeError("cash-scale price-object source key mismatch")
        body = root / relative
        if (
            not body.is_file()
            or int(raw.get("content_length", -1)) != body.stat().st_size
            or raw.get("content_sha256") != _sha256(body)
        ):
            raise RuntimeError("cash-scale price-object receipt/body mismatch")
        md5 = hashlib.md5(usedforsecurity=False)
        with body.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                md5.update(chunk)
        if md5.hexdigest() != etag:
            raise RuntimeError("cash-scale price-object ETag/body mismatch")
        declared[local_path] = raw

    referenced: dict[str, tuple[str, str, str]] = {}
    for parent in parents.to_dict("records"):
        for side in ("previous", "adjustment"):
            local_path = str(parent[f"{side}_price_source_object_key"])
            identity = (
                str(parent[f"{side}_price_source_content_sha256"]),
                str(parent[f"{side}_price_source_etag"]).strip('"').lower(),
                str(parent[f"{side}_price_source_schema"]),
            )
            previous = referenced.setdefault(local_path, identity)
            if previous != identity:
                raise RuntimeError(
                    "cash-scale parents conflict on one price-object identity"
                )
    if set(referenced) != set(declared):
        raise RuntimeError(
            "cash-scale price-object receipt has missing/unused bodies"
        )
    for local_path, (content_sha, etag, schema) in referenced.items():
        raw = declared[local_path]
        if (
            raw.get("content_sha256") != content_sha
            or str(raw.get("etag") or "").strip('"').lower() != etag
            or raw.get("source_schema") != schema
        ):
            raise RuntimeError(
                "cash-scale parent/price-object receipt parity failed"
            )
    return path


def bind_source_evidence(
    verified: VerifiedScaleSourceEvidence,
    *,
    receipt_frame: pd.DataFrame,
    published_actions: pd.DataFrame,
    action_snapshot_run_id,
) -> BoundScaleSourceEvidence:
    """Bind stable manifest rows to one immutable action-snapshot run.

    Every evidence row must match one included terminal cash receipt and one
    support ``corporate_action`` row published by the same run.  The database
    table is mutable across later snapshots, so this exact join is repeated by
    the audit instead of being delegated only to a SQL foreign key.
    """
    if verified.frame.empty:
        return BoundScaleSourceEvidence(
            frame=pd.DataFrame(columns=SOURCE_EVIDENCE_COLUMNS),
            support_frame=pd.DataFrame(columns=SUPPORT_ACTION_COLUMNS),
        )
    required_receipts = {
        "receipt_no", "asset_id", "ticker", "economic_evidence_sha256",
        "source_evidence_status", "mapping_status",
        "is_terminal_economic_revision",
        "record_date",
    }
    if not required_receipts.issubset(receipt_frame.columns):
        raise RuntimeError("cash-scale receipt binding columns are missing")
    receipts = receipt_frame[
        receipt_frame["mapping_status"].eq("INCLUDED")
        & receipt_frame["is_terminal_economic_revision"].fillna(False)
    ].copy()
    if receipts["receipt_no"].astype(str).duplicated().any():
        raise RuntimeError("cash-scale terminal receipts are not unique")
    receipt_by_key = receipts.set_index("receipt_no")

    required_actions = {
        "asset_id", "source", "action_key", "action_type", "quality_run_id",
        "source_body_sha256", "announcement_date", "ex_date", "record_date",
        "ratio_numerator", "ratio_denominator", "expected_price_factor",
        "report_name", "action_scope",
    }
    if not required_actions.issubset(published_actions.columns):
        raise RuntimeError("cash-scale support-action binding columns are missing")
    action_keys = ["asset_id", "source", "action_key", "action_type"]
    if published_actions.duplicated(action_keys).any():
        raise RuntimeError("cash-scale support actions are not unique")
    action_index = published_actions.set_index(action_keys, drop=False)

    records: list[dict[str, object]] = []
    bound_support_records: list[dict[str, object]] = []
    for raw in verified.frame.to_dict("records"):
        receipt_no = str(raw["cash_receipt_no"])
        if receipt_no not in receipt_by_key.index:
            raise RuntimeError(
                f"cash-scale evidence has no included terminal receipt: {receipt_no}"
            )
        receipt = receipt_by_key.loc[receipt_no]
        if (
            str(receipt["ticker"]) != str(raw["ticker"])
            or str(receipt["source_evidence_status"])
            != str(raw["cash_source_evidence_status"])
            or str(receipt["economic_evidence_sha256"])
            != str(raw["cash_economic_sha256"])
        ):
            raise RuntimeError("cash-scale cash receipt body/identity mismatch")
        asset_id = int(receipt["asset_id"])
        cash_key = (
            asset_id,
            "DART_DISCLOSURE",
            receipt_no,
            "cash_dividend",
        )
        if cash_key not in action_index.index:
            raise RuntimeError(
                "cash-scale cash corporate_action is absent from the same "
                f"snapshot: {cash_key}"
            )
        cash_action = action_index.loc[cash_key]
        if isinstance(cash_action, pd.DataFrame):
            raise RuntimeError("cash-scale cash action match is ambiguous")
        if str(cash_action["quality_run_id"]) != str(action_snapshot_run_id):
            raise RuntimeError("cash-scale cash action run parity failed")
        if str(cash_action["source_body_sha256"]) != str(
            raw["cash_action_body_sha256"]
        ):
            raise RuntimeError("cash-scale cash action body SHA parity failed")
        record = dict(raw)
        record.update({
            "action_snapshot_run_id": action_snapshot_run_id,
            "asset_id": asset_id,
        })
        records.append(record)
        evidence_support = verified.support_frame[
            verified.support_frame["evidence_key"].eq(raw["evidence_key"])
        ]
        _validate_support_groups(raw, evidence_support)
        for support_raw in evidence_support.to_dict("records"):
            support_key = (
                asset_id,
                str(support_raw["support_action_source"]),
                str(support_raw["support_action_key"]),
                str(support_raw["support_action_type"]),
            )
            if support_key not in action_index.index:
                raise RuntimeError(
                    "cash-scale support corporate_action is absent from the "
                    f"same snapshot: {support_key}"
                )
            support = action_index.loc[support_key]
            if isinstance(support, pd.DataFrame):
                raise RuntimeError("cash-scale support action match is ambiguous")
            if str(support["quality_run_id"]) != str(action_snapshot_run_id):
                raise RuntimeError("cash-scale support action run parity failed")
            if str(support["source_body_sha256"]) != str(
                support_raw["support_action_body_sha256"]
            ):
                raise RuntimeError("cash-scale support action body SHA parity failed")
            field_pairs = (
                ("support_announcement_date", "announcement_date"),
                ("support_ex_date", "ex_date"),
                ("support_record_date", "record_date"),
                ("support_ratio_numerator", "ratio_numerator"),
                ("support_ratio_denominator", "ratio_denominator"),
                ("support_expected_price_factor", "expected_price_factor"),
                ("support_report_name", "report_name"),
                ("support_action_scope", "action_scope"),
            )
            for evidence_field, action_field in field_pairs:
                if _canonical_value(
                    evidence_field, support_raw.get(evidence_field)
                ) != _canonical_value(evidence_field, support.get(action_field)):
                    raise RuntimeError(
                        "cash-scale support action snapshot-field parity failed: "
                        f"{evidence_field}"
                    )
            if (
                support_raw["support_action_type"] == "stock_dividend"
                and _canonical_value(
                    "support_record_date", support_raw["support_record_date"]
                ) != _canonical_value("support_record_date", receipt["record_date"])
            ):
                raise RuntimeError(
                    "stock-dividend support/cash record-date parity failed"
                )
            bound_support = dict(support_raw)
            bound_support.update({
                "action_snapshot_run_id": action_snapshot_run_id,
                "support_action_quality_run_id": action_snapshot_run_id,
            })
            bound_support_records.append(bound_support)
    result = pd.DataFrame(records, columns=SOURCE_EVIDENCE_COLUMNS)
    if result.duplicated([
        "action_snapshot_run_id", "asset_id", "cash_receipt_no",
        "adjustment_trade_date",
    ]).any():
        raise RuntimeError("cash-scale bound evidence is not one-to-one")
    bound_support = pd.DataFrame(
        bound_support_records, columns=SUPPORT_ACTION_COLUMNS,
    )
    if len(bound_support) != len(verified.support_frame):
        raise RuntimeError("cash-scale support binding left unused rows")
    return BoundScaleSourceEvidence(
        frame=result,
        support_frame=bound_support,
    )


def source_evidence_metadata(
    frame: pd.DataFrame,
    support_frame: pd.DataFrame,
    *,
    verified: VerifiedScaleSourceEvidence,
) -> dict[str, object]:
    """Metadata persisted beside the certified action snapshot."""
    manifest = verified.metadata
    return {
        **manifest,
        "persisted_parent_row_count": len(frame),
        "persisted_parent_row_digest": source_evidence_digest(frame),
        "persisted_support_action_count": len(support_frame),
        "persisted_support_action_digest": support_action_digest(support_frame),
        "persisted_support_semantic_group_count": _support_group_count(
            support_frame
        ),
        "changed_scale_coverage_count": len(frame),
        # Full coverage is independently recomputed from prices and canonical
        # cash receipts.  No manifest is allowed to declare silent exclusions.
        "unresolved_count": 0,
    }
