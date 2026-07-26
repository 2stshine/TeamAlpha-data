"""fundamental 적재: DART 주요계정 JSON → long 정규화 + PIT(available_date).

계정명(account_nm)을 표준지표로 매핑(주요계정만, 나머지 스킵). thstrm_amount(당기값) 사용.
period_end/fiscal_period 는 bsns_year + reprt 로, available_date 는 rcept_no 접수일 +1일
(접수일 못 구하면 법정기한+1일). 소스 fnlttMultiAcnt 라 source='DART'.
"""
from __future__ import annotations

import glob
import json
import re
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db

# DART 주요계정 account_nm → 표준지표 (매핑 없는 계정은 스킵)
METRIC_MAP = {
    "자산총계": "total_assets", "유동자산": "current_assets", "비유동자산": "noncurrent_assets",
    "부채총계": "total_liabilities", "유동부채": "current_liabilities", "비유동부채": "noncurrent_liabilities",
    "자본총계": "total_equity", "자본금": "capital_stock", "이익잉여금": "retained_earnings",
    "매출액": "revenue", "영업이익": "operating_income", "영업이익(손실)": "operating_income",
    "법인세차감전 순이익": "pretax_income",
    "당기순이익": "net_income", "당기순이익(손실)": "net_income",
    "총포괄손익": "comprehensive_income",
}
SUPPLEMENT_STATEMENT_BY_METRIC = {
    "total_assets": {"BS"},
    "current_assets": {"BS"},
    "noncurrent_assets": {"BS"},
    "total_liabilities": {"BS"},
    "current_liabilities": {"BS"},
    "noncurrent_liabilities": {"BS"},
    "total_equity": {"BS"},
    "capital_stock": {"BS"},
    "retained_earnings": {"BS"},
    "revenue": {"IS", "CIS"},
    "operating_income": {"IS", "CIS"},
    "pretax_income": {"IS", "CIS"},
    "net_income": {"IS", "CIS"},
    "comprehensive_income": {"IS", "CIS"},
}
# reprt_code → (fiscal_period, 12월 결산 기준 종료 월, 일). thstrm_dt 를 못 읽을 때만 쓰는 fallback.
REPRT = {"11011": ("FY", 12, 31), "11013": ("Q1", 3, 31), "11012": ("Q2", 6, 30), "11014": ("Q3", 9, 30)}
_DT_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")
COLS = ["asset_id", "source", "period_end", "fiscal_period", "fs_type",
        "filing_id", "filed", "available_date", "metric", "value",
        "currency", "revision_key", "quality_run_id"]


def _available_date(period_end: date, fiscal_period: str, filed: date | None) -> date:
    if filed is not None:                       # 접수일 있으면 +1일 (PIT)
        return filed + timedelta(days=1)
    d = period_end + timedelta(days=90 if fiscal_period == "FY" else 45)  # 법정 제출기한
    if d.weekday() >= 5:                         # 주말이면 다음 월요일
        d += timedelta(days=7 - d.weekday())
    return d + timedelta(days=1)


def _amount(s) -> float | None:
    s = (s or "").replace(",", "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _period_end_from_dt(dt: str | None) -> date | None:
    """DART thstrm_dt 에서 회계기간 종료일을 뽑는다.

    비12월 결산법인이 있어 bsns_year 로 결산월을 가정하면 안 된다(3월 결산이면 최대 9개월 어긋남).
    형식은 손익계산서 '2025.04.01 ~ 2026.03.31', 재무상태표 '2026.03.31 현재' 두 가지 —
    둘 다 마지막 날짜가 기간 종료일이다.
    """
    hits = _DT_RE.findall(dt or "")
    if not hits:
        return None
    y, m, d = hits[-1]
    try:
        return date(int(y), int(m), int(d))
    except ValueError:
        return None


def _filed_from_rcept(rcept: str) -> date | None:
    if len(rcept) >= 8 and rcept[:8].isdigit():
        try:
            return date(int(rcept[:4]), int(rcept[4:6]), int(rcept[6:8]))
        except ValueError:
            return None
    return None


def _iter_files(base: str, years: set[int] | None, files: list[str] | None) -> list[str]:
    if files is not None:
        return files
    out = []
    patterns = (
        f"{base}/financials/dart/year=*/corp=*/*.json",
        f"{base}/financials/dart_full/year=*/corp=*/*.json",
    )
    for f in (
        path
        for pattern in patterns
        for path in glob.glob(pattern)
    ):
        year = int(f.split("year=")[1].split("/")[0])
        if years is None or year in years:
            out.append(f)
    return out


def _file_meta(path: str) -> tuple[str, str]:
    ticker = path.split("corp=")[1].split("/")[0]
    reprt = Path(path).name[:5]
    return ticker, reprt


def _ord_sort_key(value) -> tuple[int, int, str]:
    """DART 표시 순서를 결정적으로 비교한다."""
    rendered = str(value or "").strip()
    try:
        return 0, int(rendered), rendered
    except ValueError:
        return 1, 0, rendered


def prepare(
    base: str,
    years: set[int] | None = None,
    files: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """DART 원본을 후보로 변환한다. 미매핑/중복을 조용히 제거하지 않는다."""
    recs = []
    input_rows = excluded_rows = rejected_rows = 0
    known_duplicate_rows = known_duplicate_groups = 0
    unexpected_duplicate_rows = unexpected_duplicate_groups = 0
    known_duplicate_samples: list[dict] = []
    unexpected_duplicate_samples: list[dict] = []
    selected_files = sorted(
        _iter_files(base, years, files),
        key=lambda path: (
            "/financials/dart_full/" in path.replace("\\", "/"),
            path,
        ),
    )
    primary_period_ends: dict[tuple[str, str, str, str], date] = {}
    for f in selected_files:
        ticker, reprt = _file_meta(f)
        supplemental = "/financials/dart_full/" in f.replace("\\", "/")
        supplemental_fs_type = (
            Path(f).stem.rsplit("-", 1)[-1]
            if supplemental
            else None
        )
        if reprt not in REPRT:
            rejected_rows += 1
            continue
        fp, mm, dd = REPRT[reprt]
        with open(f, encoding="utf-8") as fh:
            rows = json.load(fh)
        file_groups: dict[tuple, list[dict]] = {}
        for r in rows:
            input_rows += 1
            row_ticker = (
                ticker if supplemental else str(r.get("stock_code") or "")
            )
            row_fs_type = (
                supplemental_fs_type if supplemental else r.get("fs_div")
            )
            if row_ticker != ticker or str(r.get("reprt_code") or "") != reprt:
                rejected_rows += 1
                continue
            metric = METRIC_MAP.get(r.get("account_nm"))
            if not metric:
                excluded_rows += 1
                continue
            if (
                supplemental
                and str(r.get("sj_div") or "") not in
                SUPPLEMENT_STATEMENT_BY_METRIC[metric]
            ):
                excluded_rows += 1
                continue
            raw_amount = (r.get("thstrm_amount") or "").strip()
            val = _amount(raw_amount)
            if val is None:
                if not raw_amount or raw_amount == "-":
                    excluded_rows += 1
                else:
                    rejected_rows += 1
                continue
            rcept = r.get("rcept_no", "") or ""
            scope_key = (ticker, reprt, str(row_fs_type or ""), rcept)
            period_end = (
                _period_end_from_dt(r.get("thstrm_dt"))
                or primary_period_ends.get(scope_key)
                or date(int(r["bsns_year"]), mm, dd)
            )
            if not supplemental:
                primary_period_ends[scope_key] = period_end
            filed = _filed_from_rcept(rcept)
            available = _available_date(period_end, fp, filed)
            revision_key = rcept or (
                f"fallback:{ticker}:{period_end.isoformat()}:{fp}:"
                f"{r.get('fs_div')}:{available.isoformat()}"
            )
            candidate = (
                ticker, "DART", period_end, fp, row_fs_type,
                rcept or None, filed, available, metric, val,
                (r.get("currency") or "").strip().upper(), revision_key, f,
                supplemental,
            )
            exact_key = (
                ticker, "DART", period_end, fp, row_fs_type,
                revision_key, metric, val,
                (r.get("currency") or "").strip().upper(),
            )
            raw_without_ord = json.dumps(
                {
                    key: value
                    for key, value in r.items()
                    if key != "ord"
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            file_groups.setdefault(exact_key, []).append({
                "candidate": candidate,
                "account_name": str(r.get("account_nm") or "").strip(),
                "ord": str(r.get("ord") or "").strip(),
                "raw_without_ord": raw_without_ord,
            })

        for exact_key, occurrences in file_groups.items():
            if len(occurrences) == 1:
                recs.append(occurrences[0]["candidate"])
                continue

            names = {item["account_name"] for item in occurrences}
            ords = {item["ord"] for item in occurrences}
            raw_shapes = {
                item["raw_without_ord"]
                for item in occurrences
            }
            known_net_income_ord_duplicate = (
                exact_key[6] == "net_income"
                and names.issubset({"당기순이익", "당기순이익(손실)"})
                and len(names) == 1
                and len(occurrences) == 2
                and len(ords) == 2
                and "" not in ords
                and len(raw_shapes) == 1
            )
            if known_net_income_ord_duplicate:
                selected = min(
                    occurrences,
                    key=lambda item: _ord_sort_key(item["ord"]),
                )
                recs.append(selected["candidate"])
                duplicate_rows = len(occurrences) - 1
                known_duplicate_rows += duplicate_rows
                known_duplicate_groups += 1
                excluded_rows += duplicate_rows
                if len(known_duplicate_samples) < 20:
                    known_duplicate_samples.append({
                        "identifier": ticker,
                        "period_end": period_end,
                        "fiscal_period": fp,
                        "fs_type": exact_key[4],
                        "revision_key": revision_key,
                        "metric": metric,
                        "value": val,
                        "source_file": f,
                        "source_ords": sorted(
                            ords,
                            key=_ord_sort_key,
                        ),
                        "selected_ord": selected["ord"],
                    })
                continue

            # 예상하지 못한 중복은 제거하지 않는다. 동일 business key가
            # 후보에 남아 COMMON_DUPLICATE_KEY와 전용 규칙이 publish를 차단한다.
            recs.extend(item["candidate"] for item in occurrences)
            unexpected_duplicate_rows += len(occurrences)
            unexpected_duplicate_groups += 1
            if len(unexpected_duplicate_samples) < 20:
                unexpected_duplicate_samples.append({
                    "identifier": ticker,
                    "period_end": period_end,
                    "fiscal_period": fp,
                    "fs_type": exact_key[4],
                    "revision_key": revision_key,
                    "metric": metric,
                    "value": val,
                    "source_file": f,
                    "account_names": sorted(names),
                    "source_ords": sorted(ords, key=_ord_sort_key),
                    "occurrence_count": len(occurrences),
                    "different_raw_shapes": len(raw_shapes),
                })

    candidate_cols = [
        "identifier", "source", "period_end", "fiscal_period", "fs_type",
        "filing_id", "filed", "available_date", "metric", "value",
        "currency", "revision_key", "source_file",
        "_supplemental",
    ]
    df = pd.DataFrame(recs, columns=candidate_cols)
    business_key = [
        "identifier", "source", "period_end", "fiscal_period",
        "fs_type", "revision_key", "metric",
    ]
    primary = df[~df["_supplemental"]].copy()
    supplemental_df = df[df["_supplemental"]].copy()
    if not supplemental_df.empty:
        primary_index = pd.MultiIndex.from_frame(primary[business_key])
        supplemental_index = pd.MultiIndex.from_frame(
            supplemental_df[business_key]
        )
        already_present = supplemental_index.isin(primary_index)
        excluded_rows += int(already_present.sum())
        supplemental_df = supplemental_df[~already_present]
    supplemented_rows = len(supplemental_df)
    df = pd.concat([primary, supplemental_df], ignore_index=True).drop(
        columns="_supplemental",
    )
    return df, {
        "input_rows": input_rows,
        "transformed_rows": len(df),
        "excluded_rows": excluded_rows,
        "rejected_rows": rejected_rows,
        "known_net_income_ord_duplicate": {
            "row_count": known_duplicate_rows,
            "group_count": known_duplicate_groups,
            "samples": known_duplicate_samples,
        },
        "unexpected_exact_duplicate": {
            "row_count": unexpected_duplicate_rows,
            "group_count": unexpected_duplicate_groups,
            "samples": unexpected_duplicate_samples,
        },
        "source_file_count": len(selected_files),
        "full_statement_supplement": {
            "row_count": supplemented_rows,
            "file_count": sum(
                "/financials/dart_full/" in path.replace("\\", "/")
                for path in selected_files
            ),
        },
    }


def exclude_nontradable(
    candidates: pd.DataFrame,
    stats: dict,
    tradable_identifiers: set[str],
    unsupported_market_identifiers: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """전체 가격 유니버스에 없는 DART-only 기업을 명시적으로 제외한다.

    제외 행은 조용히 버리지 않고 reconciliation 수치와
    NO_TRADABLE_PRICE_ASSET DQ 결과용 ticker 요약에 보존한다.
    """
    if candidates.empty:
        updated = dict(stats)
        updated["no_tradable_price_asset"] = {
            "row_count": 0,
            "ticker_count": 0,
            "samples": [],
        }
        updated["unsupported_market_asset"] = {
            "row_count": 0,
            "ticker_count": 0,
            "samples": [],
        }
        return candidates, updated

    allowed = {str(identifier) for identifier in tradable_identifiers}
    unsupported = {
        str(identifier)
        for identifier in (unsupported_market_identifiers or set())
    }
    candidate_identifiers = candidates["identifier"].astype(str)
    unsupported_excluded = candidates[
        ~candidate_identifiers.isin(allowed)
        & candidate_identifiers.isin(unsupported)
    ].copy()
    no_price_excluded = candidates[
        ~candidate_identifiers.isin(allowed)
        & ~candidate_identifiers.isin(unsupported)
    ].copy()
    excluded = pd.concat(
        [unsupported_excluded, no_price_excluded],
        ignore_index=True,
    )
    retained = candidates[
        candidate_identifiers.isin(allowed)
    ].reset_index(drop=True)

    def summarize(frame: pd.DataFrame) -> dict:
        if frame.empty:
            return {"row_count": 0, "ticker_count": 0, "samples": []}
        if "source_file" not in frame:
            frame = frame.assign(source_file=None)
        summary = (
            frame.groupby("identifier", as_index=False)
            .agg(
                row_count=("identifier", "size"),
                first_period_end=("period_end", "min"),
                last_period_end=("period_end", "max"),
                source_file_count=("source_file", "nunique"),
            )
            .sort_values(
                ["row_count", "identifier"],
                ascending=[False, True],
            )
        )
        return {
            "row_count": len(frame),
            "ticker_count": int(frame["identifier"].nunique()),
            "samples": (
                summary.head(20)
                .astype(object)
                .where(pd.notna(summary.head(20)), None)
                .to_dict("records")
            ),
        }

    updated = dict(stats)
    updated["transformed_rows"] = len(retained)
    updated["excluded_rows"] = int(updated.get("excluded_rows", 0)) + len(excluded)
    updated["no_tradable_price_asset"] = summarize(no_price_excluded)
    updated["unsupported_market_asset"] = summarize(unsupported_excluded)
    return retained, updated


def publish(
    conn,
    candidates: pd.DataFrame,
    krx_map: dict[str, int],
    quality_run_id: UUID,
    *,
    replace_scopes: bool = True,
) -> int:
    if candidates.empty:
        return 0
    df = candidates.copy()
    df["asset_id"] = df["identifier"].map(krx_map)
    if df["asset_id"].isna().any():
        missing = sorted(df.loc[df["asset_id"].isna(), "identifier"].astype(str).unique())
        raise RuntimeError(f"quality gate missed unmapped fundamental identifiers: {missing[:20]}")
    df["asset_id"] = df["asset_id"].astype("int64")
    df["quality_run_id"] = quality_run_id
    if replace_scopes:
        scopes = df[[
            "asset_id", "source", "period_end", "fiscal_period",
            "fs_type", "revision_key",
        ]].drop_duplicates()
        with conn.cursor() as cur:
            for scope in scopes.itertuples(index=False, name=None):
                cur.execute(
                    """
                    DELETE FROM fundamental
                    WHERE asset_id=%s AND source=%s AND period_end=%s
                      AND fiscal_period=%s AND fs_type=%s AND revision_key=%s
                    """,
                    scope,
                )
    rows = list(
        df[COLS].astype(object).where(pd.notna(df[COLS]), None)
        .itertuples(index=False, name=None)
    )
    n = db.upsert(conn, "fundamental", COLS, rows,
                  conflict=["asset_id", "source", "period_end", "fiscal_period",
                            "fs_type", "revision_key", "metric"],
                  update=["filing_id", "filed", "available_date", "value",
                          "currency", "quality_run_id", "loaded_at"],
                  temp_name="_stg_fundamental_publish")
    print(f"[financials] fundamental upsert {n}행")
    return n


def run(conn, base: str, krx_map: dict[str, int], years: set[int] | None = None,
        files: list[str] | None = None, replace_existing: bool = False,
        quality_run_id: UUID | None = None) -> None:
    """호환 wrapper. revision을 보존하므로 replace_existing은 무시한다."""
    if quality_run_id is None:
        raise RuntimeError("financials.run requires quality_run_id; use silver.load")
    candidates, _ = prepare(base, years=years, files=files)
    publish(conn, candidates, krx_map, quality_run_id)
