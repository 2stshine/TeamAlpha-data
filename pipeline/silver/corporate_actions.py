"""DART 기업행사 Bronze를 가격 DQ용 표준 이벤트로 변환한다.

원천 공시를 수정하지 않고 다음 증거만 표준화한다.

- DART 구조화 주요사항보고서: 효력일과 계산 가능한 주식수 조정계수
- DART/거래소 공시 목록: 권리락·배당락·액면분할·병합 등 직접 공시일

이 결과는 Silver 테이블에 publish하지 않고 DQ 규칙의 외부 근거로 사용한다.
"""
from __future__ import annotations

import glob
import hashlib
import html
import json
import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db


COLUMNS = [
    "identifier",
    "event_type",
    "announcement_date",
    "effective_date",
    "match_window_days",
    "expected_factor",
    "share_count_factor",
    "share_count_before",
    "share_count_after",
    "share_count_factor_comparable",
    "share_count_comparison_reason",
    "action_method",
    "record_date",
    "payment_date",
    "cash_amount",
    "adjusted_cash_amount",
    "currency",
    "frequency",
    "confirms_price_adjustment",
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
    return None


def _structured_share_count_factor(
    event_type: str,
    row: dict,
) -> float | None:
    """DART 감자 전·후 보통주 수 비율.

    이것은 가격 조정계수가 아니다. KRX의 실제 상장주식 수 변화와만
    비교하기 위해 별도 필드로 보존한다.
    """
    if event_type != "capital_reduction":
        return None
    before = _number(row.get("bfcr_tisstk_ostk"))
    after = _number(row.get("atcr_tisstk_ostk"))
    if before is None or after is None or before <= 0 or after <= 0:
        return None
    return before / after


def _share_count_factor_comparable(event_type: str, row: dict) -> bool:
    """감자비율을 실제 전체 상장주식 수 변화와 비교할 수 있는지 판정한다.

    전체 보통주를 같은 비율로 병합하는 경우만 비교한다. 특정 주주/주식의
    소각, 유상감자, 액면가 감소, 동시 주식분할은 DART의 감자 전후 숫자가
    KRX 일별 LIST_SHRS 변화와 같은 경제적 범위를 나타내지 않는다.
    """
    if event_type != "capital_reduction":
        return False
    method = _compact(row.get("cr_mth"))
    if "병합" not in method and "무상감자" not in method:
        return False
    non_comparable = (
        "특정",
        "대주주",
        "최대주주",
        "자기주식",
        "보유주식",
        "유상",
        "액면감소",
        "액면액감소",
        "주식분할",
        "주식수변동없음",
        "출자전환",
    )
    return not any(marker in method for marker in non_comparable)


def _classify_share_count_comparability(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Exclude reductions whose isolated DART ratio cannot match KRX shares."""
    if events.empty:
        return events
    classified = events.copy()
    classified["share_count_comparison_reason"] = None
    reductions = classified["event_type"].eq("capital_reduction")
    classified.loc[
        reductions
        & ~classified["share_count_factor_comparable"].fillna(False),
        "share_count_comparison_reason",
    ] = "ACTION_METHOD_NOT_UNIFORM"

    by_identifier = {
        str(identifier): group
        for identifier, group in classified.groupby(
            classified["identifier"].astype(str),
            sort=False,
        )
    }
    financing_types = {
        "paid_increase",
        "combined_offering",
        "bonus_issue",
    }
    for index, reduction in classified[reductions].iterrows():
        if not bool(reduction["share_count_factor_comparable"]):
            continue
        peers = by_identifier.get(str(reduction["identifier"]))
        if peers is None:
            continue
        announcement = reduction["announcement_date"]
        effective = reduction["effective_date"]
        simultaneous_financing = peers[
            peers["event_type"].isin(financing_types)
            & peers["announcement_date"].notna()
            & (
                peers["announcement_date"].map(
                    lambda value: abs((value - announcement).days)
                    if pd.notna(announcement)
                    else 9999
                ).le(3)
            )
        ]
        simultaneous_split = peers[
            peers["event_type"].eq("stock_split")
            & (
                peers.apply(
                    lambda row: min(
                        abs((value - effective).days)
                        for value in (
                            row["effective_date"],
                            row["announcement_date"],
                        )
                        if pd.notna(value) and pd.notna(effective)
                    )
                    if (
                        pd.notna(effective)
                        and (
                            pd.notna(row["effective_date"])
                            or pd.notna(row["announcement_date"])
                        )
                    )
                    else 9999,
                    axis=1,
                ).le(30)
            )
        ]
        if not simultaneous_split.empty:
            classified.at[index, "share_count_factor_comparable"] = False
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "SIMULTANEOUS_STOCK_SPLIT"
        elif not simultaneous_financing.empty:
            classified.at[index, "share_count_factor_comparable"] = False
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "SIMULTANEOUS_FINANCING_DISCLOSURE"
        else:
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "UNIFORM_REDUCTION"
    return classified


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
        "share_count_factor": _structured_share_count_factor(event_type, row),
        "share_count_before": (
            _number(row.get("bfcr_tisstk_ostk"))
            if event_type == "capital_reduction"
            else None
        ),
        "share_count_after": (
            _number(row.get("atcr_tisstk_ostk"))
            if event_type == "capital_reduction"
            else None
        ),
        "share_count_factor_comparable": _share_count_factor_comparable(
            event_type,
            row,
        ),
        "share_count_comparison_reason": None,
        "action_method": row.get("cr_mth") if event_type == "capital_reduction" else None,
        "confirms_price_adjustment": (
            event_type in PRICE_ADJUSTING_STRUCTURED
            and effective_date is not None
            and not related_company_event
        ),
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
        # 배당락 공시는 현금배당처럼 KRX 기준가격 조정계수가 생기지 않는
        # 경우가 있다. 관측된 기준가 변경의 근거로는 쓰되, 역방향으로
        # 모든 배당락에 가격조정을 요구하지 않는다.
        return "ex_dividend", False, 0
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


def _document_effective_date(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
    event_type: str,
) -> date | None:
    """원문 ZIP에서 거래소 공시의 실제 실시일을 읽는다."""
    labels = {
        "rights_detachment": ("권리락 실시일",),
        "ex_dividend": ("배당락 실시일",),
    }.get(event_type, ())
    if not labels:
        return None
    paths = glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    )
    for path in sorted(paths):
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile):
            continue
        for payload in payloads:
            text = None
            for encoding in ("utf-8", "euc-kr", "cp949"):
                try:
                    text = payload.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            if text is None:
                text = payload.decode("utf-8", errors="replace")
            visible = html.unescape(re.sub(r"<[^>]+>", " ", text))
            visible = re.sub(r"\s+", " ", visible)
            for label in labels:
                match = re.search(
                    re.escape(label)
                    + r".{0,600}?((?:19|20)\d{2}\s*[년./-]\s*"
                    r"\d{1,2}\s*[월./-]\s*\d{1,2}\s*일?)",
                    visible,
                )
                if match:
                    parsed = _parse_date(match.group(1))
                    if parsed is not None:
                        return parsed
    return None


def _document_texts(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
) -> list[str]:
    paths = glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    )
    texts: list[str] = []
    for path in sorted(paths):
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile):
            continue
        for payload in payloads:
            decoded = None
            for encoding in ("utf-8", "euc-kr", "cp949"):
                try:
                    decoded = payload.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            if decoded is None:
                decoded = payload.decode("utf-8", errors="replace")
            visible = html.unescape(re.sub(r"<[^>]+>", " ", decoded))
            texts.append(re.sub(r"\s+", " ", visible))
    return texts


def _cash_dividend_details(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
) -> dict:
    details = {
        "record_date": None,
        "payment_date": None,
        "cash_amount": None,
        "adjusted_cash_amount": None,
        "currency": "KRW",
        "frequency": None,
    }
    for visible in _document_texts(
        base, ticker=ticker, rcept_no=rcept_no,
    ):
        compact = _compact(visible)
        amount = re.search(
            r"1주당배당금원보통주(?:식)?([0-9,]+(?:\.[0-9]+)?)",
            compact,
        )
        if amount:
            details["cash_amount"] = _number(amount.group(1))
        for label, field in (
            ("배당기준일", "record_date"),
            ("배당금지급예정일자", "payment_date"),
        ):
            match = re.search(
                label + r"((?:19|20)\d{2}[년./-]?\d{1,2}[월./-]?\d{1,2}일?)",
                compact,
            )
            if match:
                details[field] = _parse_date(match.group(1))
        if "분기배당" in compact:
            details["frequency"] = "quarterly"
        elif "중간배당" in compact:
            details["frequency"] = "interim"
        elif "결산배당" in compact:
            details["frequency"] = "annual"
        elif "배당구분" in compact:
            details["frequency"] = "irregular"
        if details["cash_amount"] is not None:
            break
    return details


def _disclosure_row(base: str, path: str, row: dict) -> dict | None:
    event = _disclosure_type(row.get("report_nm"))
    if event is None:
        return None
    event_type, expects_adjustment, window = event
    ticker = str(row.get("stock_code") or "").strip() or _ticker_from_path(path)
    announced = _parse_date(row.get("rcept_dt")) or _announcement_date(row)
    if not ticker or announced is None:
        return None
    document_date = _document_effective_date(
        base,
        ticker=ticker,
        rcept_no=str(row.get("rcept_no") or ""),
        event_type=event_type,
    )
    dividend_details = (
        _cash_dividend_details(
            base,
            ticker=ticker,
            rcept_no=str(row.get("rcept_no") or ""),
        )
        if event_type == "cash_dividend"
        else {}
    )
    confirms_adjustment = expects_adjustment or event_type == "ex_dividend"
    effective_date = (
        document_date or announced
        if confirms_adjustment
        else None
    )
    match_window_days = (
        0
        if document_date is not None
        else (3 if event_type == "ex_dividend" else window)
    )
    return {
        "identifier": ticker,
        "event_type": event_type,
        "announcement_date": announced,
        # 거래소의 권리락·배당락·변경상장 공시는 효력일에 근접해
        # 제출되므로 직접 공시일을 좁은 매칭 창의 기준으로 사용한다.
        "effective_date": effective_date,
        "match_window_days": match_window_days,
        "expected_factor": None,
        "share_count_factor": None,
        "share_count_before": None,
        "share_count_after": None,
        "share_count_factor_comparable": False,
        "share_count_comparison_reason": None,
        "action_method": None,
        "record_date": dividend_details.get("record_date"),
        "payment_date": dividend_details.get("payment_date"),
        "cash_amount": dividend_details.get("cash_amount"),
        "adjusted_cash_amount": dividend_details.get("adjusted_cash_amount"),
        "currency": dividend_details.get("currency"),
        "frequency": dividend_details.get("frequency"),
        "confirms_price_adjustment": confirms_adjustment,
        "expects_price_adjustment": expects_adjustment,
        "confidence": (
            "EXCHANGE_NOTICE" if confirms_adjustment else "ANNOUNCEMENT_ONLY"
        ),
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
        parsed = _disclosure_row(base, path, row)
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
    events = _classify_share_count_comparability(events)
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
        "share_count_factor_count": int(
            events["share_count_factor"].notna().sum()
        ),
    }


def exclude_nontradable(
    events: pd.DataFrame,
    stats: dict,
    tradable_identifiers: set[str],
    unsupported_market_identifiers: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Keep DART actions only where the KRX price universe can use them.

    DART contains disclosures for KONEX, unlisted and pre-coverage issuers.
    They remain in Bronze; the exclusion is surfaced as an explicit Silver DQ
    modification rather than appearing as an identifier-mapping failure.
    """
    allowed = {str(value) for value in tradable_identifiers}
    unsupported = {
        str(value) for value in (unsupported_market_identifiers or set())
    }
    if events.empty:
        updated = dict(stats)
        for key in (
            "no_tradable_price_action", "unsupported_market_action",
        ):
            updated[key] = {
                "row_count": 0, "ticker_count": 0, "samples": [],
            }
        return events, updated

    identifiers = events["identifier"].astype(str)
    unsupported_excluded = events[
        ~identifiers.isin(allowed) & identifiers.isin(unsupported)
    ].copy()
    no_price_excluded = events[
        ~identifiers.isin(allowed) & ~identifiers.isin(unsupported)
    ].copy()
    retained = events[identifiers.isin(allowed)].reset_index(drop=True)

    def summarize(frame: pd.DataFrame) -> dict:
        if frame.empty:
            return {"row_count": 0, "ticker_count": 0, "samples": []}
        summary = (
            frame.assign(
                event_date=frame["effective_date"].where(
                    frame["effective_date"].notna(),
                    frame["announcement_date"],
                )
            )
            .groupby("identifier", as_index=False)
            .agg(
                row_count=("identifier", "size"),
                first_event_date=("event_date", "min"),
                last_event_date=("event_date", "max"),
            )
            .sort_values(
                ["row_count", "identifier"], ascending=[False, True],
            )
        )
        head = summary.head(20)
        return {
            "row_count": len(frame),
            "ticker_count": int(frame["identifier"].nunique()),
            "samples": (
                head.astype(object)
                .where(pd.notna(head), None)
                .to_dict("records")
            ),
        }

    updated = dict(stats)
    updated["transformed_rows"] = len(retained)
    updated["excluded_rows"] = int(updated.get("excluded_rows", 0)) + (
        len(unsupported_excluded) + len(no_price_excluded)
    )
    updated["no_tradable_price_action"] = summarize(no_price_excluded)
    updated["unsupported_market_action"] = summarize(unsupported_excluded)
    return retained, updated


def inherit_issuer_events(
    events: pd.DataFrame,
    preferred_to_common: dict[str, str],
) -> tuple[pd.DataFrame, dict]:
    """보통주 DART 행사를 동일 발행회사의 우선주에 증거로 복제한다."""
    if events.empty or not preferred_to_common:
        return events, {"preferred_ticker_count": 0, "inherited_event_count": 0}
    inherited = []
    identifiers = events["identifier"].astype(str)
    for preferred, common in sorted(preferred_to_common.items()):
        rows = events[identifiers.eq(str(common))].copy()
        if rows.empty:
            continue
        rows["issuer_parent_identifier"] = str(common)
        rows["issuer_event_inherited"] = True
        rows["identifier"] = str(preferred)
        cash_events = rows["event_type"].eq("cash_dividend")
        for column in ("cash_amount", "adjusted_cash_amount"):
            if column in rows:
                rows.loc[cash_events, column] = None
        inherited.append(rows)
    original = events.copy()
    original["issuer_parent_identifier"] = None
    original["issuer_event_inherited"] = False
    if not inherited:
        return original, {
            "preferred_ticker_count": 0,
            "inherited_event_count": 0,
        }
    expanded = pd.concat([original, *inherited], ignore_index=True)
    return expanded, {
        "preferred_ticker_count": len(inherited),
        "inherited_event_count": len(expanded) - len(original),
    }


PUBLISH_COLUMNS = [
    "asset_id", "source", "action_key", "action_type", "announcement_date",
    "ex_date", "record_date", "payment_date", "cash_amount",
    "adjusted_cash_amount", "currency", "frequency",
    "ratio_numerator", "ratio_denominator", "expected_price_factor",
    "share_count_factor", "status", "confidence", "filing_id",
    "quality_run_id",
]


def _action_key(row) -> str:
    receipt = str(getattr(row, "rcept_no", "") or "").strip()
    if receipt:
        return receipt
    material = "|".join(
        str(value or "")
        for value in (
            getattr(row, "identifier", None),
            getattr(row, "event_type", None),
            getattr(row, "announcement_date", None),
            getattr(row, "effective_date", None),
            getattr(row, "source_file", None),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def normalize_for_publish(candidates: pd.DataFrame) -> pd.DataFrame:
    """Convert DART evidence rows to the persistent corporate-action shape."""
    if candidates.empty:
        return pd.DataFrame(columns=["identifier", *PUBLISH_COLUMNS[1:-1]])
    records = []
    for row in candidates.itertuples(index=False):
        records.append({
            "identifier": str(row.identifier),
            "source": str(row.source),
            "action_key": _action_key(row),
            "action_type": str(row.event_type),
            "announcement_date": row.announcement_date,
            "ex_date": row.effective_date,
            "record_date": getattr(row, "record_date", None),
            "payment_date": getattr(row, "payment_date", None),
            "cash_amount": getattr(row, "cash_amount", None),
            "adjusted_cash_amount": getattr(
                row, "adjusted_cash_amount", None,
            ),
            "currency": getattr(row, "currency", None) or "KRW",
            "frequency": getattr(row, "frequency", None),
            "ratio_numerator": None,
            "ratio_denominator": None,
            "expected_price_factor": row.expected_factor,
            "share_count_factor": row.share_count_factor,
            "status": (
                "confirmed" if row.effective_date is not None else "announced"
            ),
            "confidence": row.confidence,
            "filing_id": str(row.rcept_no or "") or None,
        })
    return pd.DataFrame(records)


def publish(
    conn,
    candidates: pd.DataFrame,
    identifier_map: dict[str, int],
    quality_run_id: UUID,
) -> int:
    """DQ에만 쓰던 DART 기업행사를 Silver 테이블에도 영속화한다."""
    frame = normalize_for_publish(candidates)
    if frame.empty:
        return 0
    frame["asset_id"] = frame["identifier"].map(identifier_map)
    frame = frame[frame["asset_id"].notna()].copy()
    frame["asset_id"] = frame["asset_id"].astype("int64")
    frame["quality_run_id"] = quality_run_id
    records = frame.to_dict("records")
    if not records:
        return 0
    rows = list(
        frame[PUBLISH_COLUMNS].astype(object).where(
            pd.notna(frame[PUBLISH_COLUMNS]), None,
        ).itertuples(index=False, name=None)
    )
    count = db.upsert(
        conn,
        "corporate_action",
        PUBLISH_COLUMNS,
        rows,
        conflict=["asset_id", "source", "action_key"],
        update=[
            "action_type", "announcement_date", "ex_date", "record_date",
            "payment_date", "cash_amount", "adjusted_cash_amount", "currency",
            "frequency", "ratio_numerator",
            "ratio_denominator", "expected_price_factor", "share_count_factor",
            "status", "confidence", "filing_id", "quality_run_id", "loaded_at",
        ],
        temp_name="_stg_corporate_action_publish",
    )
    print(f"[corporate-actions] corporate_action upsert {count}행")
    return count
