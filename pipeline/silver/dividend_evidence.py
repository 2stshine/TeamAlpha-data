"""Exact fail-closed contracts for DART cash-dividend receipt evidence."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd


VERIFIED_SOURCE_STATUSES = frozenset({
    "VERIFIED_OPENDART_DOCUMENT",
    "VERIFIED_DART_VIEWER_BODY",
    "VERIFIED_ATTACHMENT_CORRECTION",
    "VERIFIED_REVIEWED_SOURCE_ERRATUM",
})

SUPPORTED_CASH_STATUSES = frozenset({
    "POSITIVE",
    "POSITIVE_PENDING_RECORD_DATE",
    "NO_COMMON_CASH_DIVIDEND",
    "NO_ECONOMIC_EVENT",
    "ATTACHMENT_ONLY",
})

SOURCE_RECEIPT_DIGEST_COLUMNS = (
    "receipt_no", "asset_id", "ticker", "corp_cls", "report_name",
    "dart_rm", "announcement_date", "revision_kind",
    "revision_root_receipt_no", "previous_receipt_no",
    "terminal_receipt_no", "terminal_announcement_date",
    "is_terminal_economic_revision", "source_evidence_status",
    "cash_amount_status", "record_date", "payment_date", "cash_amount",
    "viewer_evidence_sha256", "economic_evidence_sha256",
    "reviewed_correction_id", "payment_date_quality_status",
    "pit_event_date", "mapping_status", "excluded_reason",
)

PUBLISHED_ACTION_DIGEST_COLUMNS = (
    "asset_id", "source", "action_key", "action_type",
    "announcement_date", "ex_date", "record_date", "payment_date",
    "cash_amount", "adjusted_cash_amount", "currency", "frequency",
    "ratio_numerator", "ratio_denominator", "expected_price_factor",
    "share_count_factor", "status", "confidence", "filing_id",
    "report_name", "dart_rm", "corp_cls", "action_scope",
    "cash_amount_status", "source_evidence_status",
    "correction_of_action_key", "revision_root_action_key",
    "revision_kind", "viewer_evidence_sha256",
    "economic_evidence_sha256", "reviewed_correction_id",
    "payment_date_quality_status", "source_body_sha256",
)

INCLUDED_CASH_PARITY_COLUMNS = (
    "asset_id", "receipt_no", "announcement_date", "record_date",
    "payment_date", "cash_amount", "cash_amount_status",
    "source_evidence_status", "previous_receipt_no",
    "revision_root_receipt_no", "revision_kind",
    "viewer_evidence_sha256", "economic_evidence_sha256",
    "reviewed_correction_id", "payment_date_quality_status",
)

_DATE_COLUMNS = frozenset({
    "announcement_date", "terminal_announcement_date", "ex_date",
    "record_date", "payment_date", "pit_event_date",
})
_DECIMAL_PLACES = {
    "cash_amount": 8,
    "adjusted_cash_amount": 8,
    "ratio_numerator": 8,
    "ratio_denominator": 8,
    "expected_price_factor": 12,
    "share_count_factor": 12,
}


def _digest_value(column: str, value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if column in _DATE_COLUMNS:
        if isinstance(value, (date, datetime, pd.Timestamp)):
            return value.isoformat()[:10]
        return pd.Timestamp(value).date().isoformat()
    if column == "asset_id":
        return int(value)
    if column == "is_terminal_economic_revision":
        return bool(value)
    if column in _DECIMAL_PLACES:
        try:
            places = _DECIMAL_PLACES[column]
            quantum = Decimal(1).scaleb(-places)
            return format(Decimal(str(value)).quantize(quantum), "f")
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(f"invalid source receipt cash amount: {value!r}") from exc
    return str(value)


def _frame_digest(
    frame: pd.DataFrame,
    *,
    columns: tuple[str, ...],
    order_by: tuple[str, ...],
) -> str:
    missing = [
        column for column in columns
        if column not in frame.columns
    ]
    if missing:
        raise RuntimeError(f"digest columns are missing: {missing}")
    ordered = frame.sort_values(list(order_by), kind="stable")
    payload = [
        {
            column: _digest_value(column, getattr(row, column))
            for column in columns
        }
        for row in ordered[list(columns)].itertuples(
            index=False
        )
    ]
    rendered = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def source_receipt_digest(frame: pd.DataFrame) -> str:
    """Digest every immutable receipt/evidence/mapping field, excluding run id."""
    if "receipt_no" in frame and frame["receipt_no"].astype(str).duplicated().any():
        raise RuntimeError("source receipt digest input contains duplicate receipts")
    return _frame_digest(
        frame,
        columns=SOURCE_RECEIPT_DIGEST_COLUMNS,
        order_by=("receipt_no",),
    )


def terminal_source_receipt_digest(frame: pd.DataFrame) -> str:
    terminal = frame[
        frame["is_terminal_economic_revision"].fillna(False).astype(bool)
    ]
    return source_receipt_digest(terminal)


def published_action_digest(frame: pd.DataFrame) -> str:
    keys = ["asset_id", "source", "action_key"]
    if all(column in frame for column in keys) and frame.duplicated(keys).any():
        raise RuntimeError("published action digest input contains duplicate keys")
    return _frame_digest(
        frame,
        columns=PUBLISHED_ACTION_DIGEST_COLUMNS,
        order_by=("asset_id", "source", "action_key"),
    )


def included_cash_parity_digest(frame: pd.DataFrame) -> str:
    if "receipt_no" in frame and frame["receipt_no"].astype(str).duplicated().any():
        raise RuntimeError("included cash parity input contains duplicate receipts")
    return _frame_digest(
        frame,
        columns=INCLUDED_CASH_PARITY_COLUMNS,
        order_by=("asset_id", "receipt_no"),
    )


def _series(frame: pd.DataFrame, name: str) -> pd.Series:
    return frame.get(
        name, pd.Series(None, index=frame.index, dtype="object")
    )


def invalid_cash_evidence_mask(
    frame: pd.DataFrame,
    *,
    action_key_column: str,
    root_key_column: str,
) -> pd.Series:
    """Return rows that do not satisfy an exact source/evidence contract."""
    key = _series(frame, action_key_column).fillna("").astype(str).str.strip()
    root = _series(frame, root_key_column).fillna("").astype(str).str.strip()
    source_status = _series(
        frame, "source_evidence_status"
    ).fillna("").astype(str)
    cash_status = _series(frame, "cash_amount_status").fillna("").astype(str)
    revision_kind = _series(frame, "revision_kind").fillna("").astype(str)
    correction_key = _series(
        frame, "correction_of_action_key"
    ).fillna("").astype(str).str.strip()
    viewer_sha = _series(
        frame, "viewer_evidence_sha256"
    ).fillna("").astype(str)
    economic_sha = _series(
        frame, "economic_evidence_sha256"
    ).fillna("").astype(str)
    reviewed_id = _series(
        frame, "reviewed_correction_id"
    ).fillna("").astype(str).str.strip()
    receipt_pattern = r"^[0-9]{14}$"
    key_valid = key.str.fullmatch(receipt_pattern)
    root_valid = root.str.fullmatch(receipt_pattern)
    correction_valid = correction_key.eq("") | correction_key.str.fullmatch(
        receipt_pattern
    )

    sha_pattern = r"^[0-9a-f]{64}$"
    viewer_valid = viewer_sha.str.fullmatch(sha_pattern)
    economic_valid = economic_sha.str.fullmatch(sha_pattern)
    source_shape = (
        source_status.eq("VERIFIED_OPENDART_DOCUMENT")
        & viewer_sha.eq("")
        & economic_valid
    ) | (
        source_status.eq("VERIFIED_DART_VIEWER_BODY")
        & viewer_valid
        & economic_valid
        & viewer_sha.eq(economic_sha)
    ) | (
        source_status.eq("VERIFIED_ATTACHMENT_CORRECTION")
        & viewer_valid
        & economic_valid
        # An attachment-only correction body is non-economic.  Its own body
        # must differ from the prior economic viewer body selected by the
        # content-addressed official-family lineage.
        & viewer_sha.ne(economic_sha)
        & cash_status.eq("ATTACHMENT_ONLY")
        & revision_kind.eq("ATTACHMENT_ONLY")
        & correction_key.ne("")
    ) | (
        source_status.eq("VERIFIED_REVIEWED_SOURCE_ERRATUM")
        & viewer_sha.eq("")
        & economic_valid
        & reviewed_id.ne("")
    )

    record_date = pd.to_datetime(_series(frame, "record_date"), errors="coerce")
    cash_amount = pd.to_numeric(_series(frame, "cash_amount"), errors="coerce")
    economic_shape = (
        cash_status.eq("POSITIVE")
        & record_date.notna()
        & cash_amount.gt(0)
    ) | (
        cash_status.eq("POSITIVE_PENDING_RECORD_DATE")
        & record_date.isna()
        & cash_amount.gt(0)
    ) | (
        cash_status.isin({
            "NO_COMMON_CASH_DIVIDEND", "NO_ECONOMIC_EVENT",
            "ATTACHMENT_ONLY",
        })
        & cash_amount.isna()
    )
    return (
        ~key_valid
        | ~root_valid
        | ~correction_valid
        | ~source_status.isin(VERIFIED_SOURCE_STATUSES)
        | ~cash_status.isin(SUPPORTED_CASH_STATUSES)
        | ~source_shape
        | ~economic_shape
    )


def invalid_attachment_lineage_mask(
    frame: pd.DataFrame,
    *,
    action_key_column: str,
    root_key_column: str,
) -> pd.Series:
    """Reject attachment receipts whose official predecessor is not present.

    The content-addressed viewer manifest proves that an attachment's
    ``correction_of`` receipt is also its economic-body receipt.  At the
    parsed-row boundary we additionally require that receipt to survive in
    the exact same issuer/root family.  We deliberately do not compare the
    two rows' economic SHA values: a plain predecessor is backed by an
    OpenDART ZIP, while the attachment is backed by the official viewer HTML.
    """
    invalid = pd.Series(False, index=frame.index, dtype="bool")
    source_status = _series(
        frame, "source_evidence_status"
    ).fillna("").astype(str)
    attachment = source_status.eq("VERIFIED_ATTACHMENT_CORRECTION")
    if not attachment.any():
        return invalid

    key = _series(frame, action_key_column).fillna("").astype(str).str.strip()
    root = _series(frame, root_key_column).fillna("").astype(str).str.strip()
    previous = _series(
        frame, "correction_of_action_key"
    ).fillna("").astype(str).str.strip()
    identity_column = next(
        (
            column for column in ("ticker", "identifier", "asset_id")
            if column in frame.columns
        ),
        None,
    )
    identity = (
        _series(frame, identity_column).fillna("").astype(str).str.strip()
        if identity_column is not None
        else pd.Series("", index=frame.index, dtype="object")
    )
    key_counts = key.value_counts(dropna=False)
    unique_keys = key_counts[key_counts.eq(1)].index
    reference = pd.DataFrame({
        "_key": key,
        "_root": root,
        "_identity": identity,
    })
    reference = reference[reference["_key"].isin(unique_keys)].set_index(
        "_key"
    )
    prior_root = previous.map(reference["_root"])
    prior_identity = previous.map(reference["_identity"])
    invalid.loc[attachment] = (
        previous.eq("")
        | prior_root.isna()
        | prior_root.ne(root)
        | prior_identity.ne(identity)
        | identity.eq("")
    ).loc[attachment]
    return invalid


def assert_verified_cash_evidence(
    frame: pd.DataFrame,
    *,
    action_key_column: str,
    root_key_column: str,
) -> None:
    invalid = invalid_cash_evidence_mask(
        frame,
        action_key_column=action_key_column,
        root_key_column=root_key_column,
    )
    invalid |= invalid_attachment_lineage_mask(
        frame,
        action_key_column=action_key_column,
        root_key_column=root_key_column,
    )
    if invalid.any():
        sample_columns = [
            column for column in (
                action_key_column, root_key_column, "source_evidence_status",
                "cash_amount_status", "revision_kind",
                "viewer_evidence_sha256", "economic_evidence_sha256",
                "reviewed_correction_id",
            ) if column in frame
        ]
        raise RuntimeError(
            "cash receipt violates the exact evidence contract: "
            f"failure_count={int(invalid.sum())} "
            f"{frame.loc[invalid, sample_columns].head(20).to_dict('records')}"
        )
