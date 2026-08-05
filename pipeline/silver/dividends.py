"""DART 정기보고서 배당 Bronze를 Silver fundamental long 형식으로 변환한다."""
from __future__ import annotations

import glob
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd


METRIC_SPECS = {
    "현금배당금총액(백만원)": ("total_cash_dividend", "currency", 1_000_000.0),
    "(연결)현금배당성향(%)": ("payout_ratio", "percent", 1.0),
    "현금배당수익률(%)": ("dividend_yield", "percent", 1.0),
    "주당현금배당금(원)": ("cash_dividend_per_share", "per_share", 1.0),
    "주당주식배당(주)": ("stock_dividend_per_share", "shares", 1.0),
}
TERM_FIELDS = (("thstrm", 0), ("frmtrm", 1), ("lwfr", 2))
COLUMNS = [
    "identifier", "source", "statement_type", "data_basis", "period_end",
    "fiscal_period", "fs_type", "filing_id", "filed", "accepted_at",
    "available_date", "available_at", "metric", "value", "currency",
    "unit_type", "revision_key", "source_file",
]


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def _compact(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip()


def _number(value: object) -> float | None:
    rendered = str(value or "").replace(",", "").replace("%", "").strip()
    if rendered in {"", "-", "-0", "N/A", "nan"}:
        return None
    rendered = rendered.replace("△", "-")
    try:
        return float(rendered)
    except ValueError:
        return None


def _filed(receipt: str) -> date | None:
    if len(receipt) < 8 or not receipt[:8].isdigit():
        return None
    try:
        return datetime.strptime(receipt[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _path_identifier(path: str) -> str | None:
    match = re.search(r"/corp=(\d{6})/", path.replace("\\", "/"))
    return match.group(1) if match else None


def _select_rows(rows: list[dict]) -> list[dict]:
    """한 filing에서 보통주 지표와 연결 배당성향 한 행만 선택한다."""
    selected: list[dict] = []
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(_compact(row.get("se")), []).append(row)
    for label, candidates in grouped.items():
        if label not in METRIC_SPECS:
            continue
        if label == "(연결)현금배당성향(%)":
            selected.append(candidates[0])
            continue
        common = [
            row for row in candidates
            if _compact(row.get("stock_knd")) in {"보통주", "보통주식"}
        ]
        if common:
            selected.append(common[0])
        elif all(not _compact(row.get("stock_knd")) for row in candidates):
            selected.append(candidates[0])
    return selected


def prepare(
    base: str,
    *,
    files: list[str] | None = None,
    years: set[int] | None = None,
) -> tuple[pd.DataFrame, dict]:
    paths = files if files is not None else sorted(glob.glob(
        f"{base}/dividends/dart/alot-matter/year=*/report=*/corp=*/rcept=*/response.json"
    ))
    records: list[dict] = []
    input_rows = 0
    rejected_rows = 0
    for path in paths:
        match = re.search(r"/year=(\d{4})/", path.replace("\\", "/"))
        if years and match and int(match.group(1)) not in years:
            continue
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            input_rows += 1
            rejected_rows += 1
            continue
        rows = payload.get("list") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            continue
        identifier = _path_identifier(path)
        for row in _select_rows(rows):
            label = _compact(row.get("se"))
            metric, unit_type, scale = METRIC_SPECS[label]
            receipt = str(row.get("rcept_no") or "").strip()
            identifier = identifier or str(row.get("corp_code") or "").zfill(6)
            filed = _filed(receipt)
            settlement = str(row.get("stlm_dt") or "")
            digits = re.sub(r"\D", "", settlement)
            try:
                period = datetime.strptime(digits[:8], "%Y%m%d").date()
            except (ValueError, TypeError):
                period = None
            if not identifier or period is None or filed is None:
                rejected_rows += 1
                continue
            available_date = filed + timedelta(days=1)
            available_at = datetime.combine(
                available_date, time.min, tzinfo=timezone.utc,
            )
            for term, offset in TERM_FIELDS:
                input_rows += 1
                value = _number(row.get(term))
                if value is None:
                    continue
                records.append({
                    "identifier": str(identifier).zfill(6),
                    "source": "DART",
                    "statement_type": "DIVIDEND",
                    "data_basis": "REPORTED",
                    "period_end": _subtract_years(period, offset),
                    "fiscal_period": "FY",
                    "fs_type": "UNKNOWN",
                    "filing_id": receipt,
                    "filed": filed,
                    "accepted_at": None,
                    "available_date": available_date,
                    "available_at": available_at,
                    "metric": metric,
                    "value": value * scale,
                    "currency": "KRW",
                    "unit_type": unit_type,
                    "revision_key": f"{receipt}:{term}",
                    "source_file": path,
                })
    frame = pd.DataFrame(records, columns=COLUMNS)
    before = len(frame)
    key = [
        "identifier", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "revision_key", "metric",
    ]
    if not frame.empty:
        frame = frame.sort_values("source_file").drop_duplicates(
            key, keep="last",
        ).reset_index(drop=True)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": max(0, input_rows - len(frame)),
        "rejected_rows": rejected_rows,
        "duplicate_rows_removed": before - len(frame),
        "source_file_count": len(paths),
    }
