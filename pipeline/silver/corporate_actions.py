"""DART 기업행사 Bronze를 가격 DQ용 표준 이벤트로 변환한다.

원천 공시를 수정하지 않고 다음 증거만 표준화한다.

- DART 구조화 주요사항보고서: 효력일과 계산 가능한 주식수 조정계수
- DART/거래소 공시 목록: 권리락·배당락·액면분할·병합 등 직접 공시일

이 결과는 Silver 테이블에 publish하지 않고 DQ 규칙의 외부 근거로 사용한다.
"""
from __future__ import annotations

import glob
import json
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd


COLUMNS = [
    "identifier",
    "event_type",
    "announcement_date",
    "effective_date",
    "match_window_days",
    "expected_factor",
    "expects_price_adjustment",
    "confidence",
    "rcept_no",
    "report_name",
    "source",
    "source_file",
]

STRUCTURED_DATE_FIELDS = {
    "paid_increase": (),
    "bonus_issue": ("nstk_asstd", "nstk_lstprd", "nstk_dividrk"),
    "combined_offering": (
        "fric_nstk_asstd",
        "fric_nstk_lstprd",
        "fric_nstk_dividrk",
    ),
    "capital_reduction": ("crsc_nstklstprd", "cr_std"),
    "merger": ("mgsc_mgdt", "mgsc_nstklstprd"),
    "company_split": ("abcr_nstklstprd", "abcr_nstkasstd", "dvdt"),
    "split_merger": (
        "abcr_nstklstprd",
        "abcr_nstkasstd",
        "dvmgsc_dvmgdt",
    ),
    "share_exchange": ("extrsc_extrdt", "extrsc_nstklstprd"),
}

PRICE_ADJUSTING_STRUCTURED = {
    "bonus_issue",
    "combined_offering",
    "capital_reduction",
}


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _parse_date(value: object) -> date | None:
    rendered = str(value or "").strip()
    if not rendered or rendered == "-":
        return None
    digits = re.sub(r"\D", "", rendered)
    if len(digits) < 8:
        return None
    try:
        return datetime.strptime(digits[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _number(value: object) -> float | None:
    rendered = str(value or "").replace(",", "").replace("%", "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return float(rendered)
    except ValueError:
        return None


def _ticker_from_path(path: str) -> str | None:
    match = re.search(r"/corp=(\d{6})/", path.replace("\\", "/"))
    return match.group(1) if match else None


def _event_from_path(path: str) -> str | None:
    match = re.search(r"/event=([^/]+)/", path.replace("\\", "/"))
    return match.group(1) if match else None


def _announcement_date(row: dict) -> date | None:
    rcept_no = str(row.get("rcept_no") or "")
    return _parse_date(rcept_no[:8])


def _first_date(row: dict, fields: tuple[str, ...]) -> date | None:
    for field in fields:
        parsed = _parse_date(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _structured_expected_factor(event_type: str, row: dict) -> float | None:
    if event_type == "bonus_issue":
        ratio = _number(row.get("nstk_ascnt_ps_ostk"))
        if ratio is not None and ratio >= 0:
            return 1 / (1 + ratio)
    # 유무상증자는 유상 신주 비율·발행가와 무상 신주 비율이 함께
    # 이론권리락 가격을 결정한다. 이전 종가까지 필요한 값을 DART
    # 무상분만으로 계산하면 거짓 불일치가 되므로 단독 계수를 만들지 않는다.
    if event_type == "capital_reduction":
        before = _number(row.get("bfcr_tisstk_ostk"))
        after = _number(row.get("atcr_tisstk_ostk"))
        if before is not None and after is not None and before > 0 and after > 0:
            return before / after
    return None


def _structured_row(
    path: str,
    row: dict,
    report_name: object = None,
) -> dict | None:
    ticker = _ticker_from_path(path)
    event_type = _event_from_path(path)
    if not ticker or event_type not in STRUCTURED_DATE_FIELDS:
        return None
    effective_date = _first_date(
        row,
        STRUCTURED_DATE_FIELDS[event_type],
    )
    compact_report = _compact(report_name)
    related_company_event = (
        "종속회사" in compact_report
        or "자회사" in compact_report
        or "철회" in compact_report
        or "부결" in compact_report
    )
    return {
        "identifier": ticker,
        "event_type": event_type,
        "announcement_date": _announcement_date(row),
        "effective_date": effective_date,
        "match_window_days": 7 if effective_date else 0,
        "expected_factor": _structured_expected_factor(event_type, row),
        "expects_price_adjustment": (
            event_type in PRICE_ADJUSTING_STRUCTURED
            and effective_date is not None
            and not related_company_event
        ),
        "confidence": "EFFECTIVE_DATE" if effective_date else "ANNOUNCEMENT_ONLY",
        "rcept_no": str(row.get("rcept_no") or ""),
        "report_name": report_name,
        "source": "DART_STRUCTURED",
        "source_file": path,
    }


def _disclosure_type(report_name: object) -> tuple[str, bool, int] | None:
    title = _compact(report_name)
    if "권리락" in title:
        return "rights_detachment", True, 3
    if "배당락" in title:
        return "ex_dividend", True, 3
    if "액면분할" in title or "주식분할" in title:
        executed = "변경상장" in title or "거래정지해제" in title
        cancelled = "철회" in title or "부결" in title
        return (
            "stock_split_cancelled" if cancelled else "stock_split",
            executed and not cancelled,
            10 if executed and not cancelled else 0,
        )
    if "액면병합" in title or "주식병합" in title:
        executed = "변경상장" in title or "거래정지해제" in title
        cancelled = "철회" in title or "부결" in title
        return (
            "reverse_split_cancelled" if cancelled else "reverse_split",
            executed and not cancelled,
            10 if executed and not cancelled else 0,
        )
    if "현금현물배당결정" in title:
        return "cash_dividend", False, 0
    if "변경상장" in title:
        return "listing_change", False, 0
    if "거래정지" in title:
        return "trading_halt", False, 0
    if "상장폐지" in title or "정리매매" in title:
        return "delisting", False, 0
    return None


def _disclosure_row(path: str, row: dict) -> dict | None:
    event = _disclosure_type(row.get("report_nm"))
    if event is None:
        return None
    event_type, expects_adjustment, window = event
    ticker = str(row.get("stock_code") or "").strip() or _ticker_from_path(path)
    announced = _parse_date(row.get("rcept_dt")) or _announcement_date(row)
    if not ticker or announced is None:
        return None
    return {
        "identifier": ticker,
        "event_type": event_type,
        "announcement_date": announced,
        # 거래소의 권리락·배당락·변경상장 공시는 효력일에 근접해
        # 제출되므로 직접 공시일을 좁은 매칭 창의 기준으로 사용한다.
        "effective_date": announced if expects_adjustment else None,
        "match_window_days": window,
        "expected_factor": None,
        "expects_price_adjustment": expects_adjustment,
        "confidence": "EXCHANGE_NOTICE" if expects_adjustment else "ANNOUNCEMENT_ONLY",
        "rcept_no": str(row.get("rcept_no") or ""),
        "report_name": row.get("report_nm"),
        "source": "DART_DISCLOSURE",
        "source_file": path,
    }


def _read_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _disclosure_rows(base: str) -> list[tuple[str, dict]]:
    manifests = sorted(glob.glob(
        f"{base}/corporate_actions/dart/manifests/"
        "from=*/to=*/disclosures.json"
    ))
    rows: dict[str, tuple[str, dict]] = {}
    for path in manifests:
        payload = _read_json(path)
        if not isinstance(payload, list):
            continue
        for row in payload:
            rcept_no = str(row.get("rcept_no") or "")
            if rcept_no:
                rows[rcept_no] = (path, row)
    if rows:
        return list(rows.values())

    for path in sorted(glob.glob(
        f"{base}/corporate_actions/dart/disclosures/"
        "year=*/date=*/corp=*/rcept=*.json"
    )):
        row = _read_json(path)
        rcept_no = str(row.get("rcept_no") or "")
        if rcept_no:
            rows[rcept_no] = (path, row)
    return list(rows.values())


def prepare(
    base: str,
    *,
    target_date: date | None = None,
) -> tuple[pd.DataFrame, dict]:
    """로컬 Bronze에서 기업행사 증거를 읽고 표준 DataFrame과 통계를 반환한다."""
    records: list[dict] = []
    disclosure_rows = _disclosure_rows(base)
    disclosure_by_receipt = {
        str(row.get("rcept_no") or ""): row
        for _, row in disclosure_rows
    }
    structured_files = sorted(glob.glob(
        f"{base}/corporate_actions/dart/structured/"
        "event=*/year=*/corp=*/rcept=*.json"
    ))
    for path in structured_files:
        raw = _read_json(path)
        disclosure = disclosure_by_receipt.get(
            str(raw.get("rcept_no") or ""),
            {},
        )
        parsed = _structured_row(
            path,
            raw,
            disclosure.get("report_nm"),
        )
        if parsed is not None:
            records.append(parsed)

    disclosure_count = 0
    for path, row in disclosure_rows:
        parsed = _disclosure_row(path, row)
        if parsed is not None:
            records.append(parsed)
            disclosure_count += 1

    if not records:
        return _empty(), {
            "row_count": 0,
            "structured_file_count": len(structured_files),
            "disclosure_event_count": 0,
        }

    events = pd.DataFrame(records, columns=COLUMNS)
    events["identifier"] = events["identifier"].astype(str).str.zfill(6)
    events = events.drop_duplicates(
        ["identifier", "event_type", "rcept_no", "source"],
        keep="last",
    ).reset_index(drop=True)
    if target_date is not None:
        lower = target_date - pd.Timedelta(days=180)
        upper = target_date + pd.Timedelta(days=30)
        relevant = (
            events["effective_date"].between(lower, upper)
            | events["announcement_date"].between(lower, upper)
        )
        events = events[relevant].reset_index(drop=True)
    return events, {
        "row_count": len(events),
        "structured_file_count": len(structured_files),
        "disclosure_event_count": disclosure_count,
        "effective_date_count": int(events["effective_date"].notna().sum()),
        "expected_factor_count": int(events["expected_factor"].notna().sum()),
    }
