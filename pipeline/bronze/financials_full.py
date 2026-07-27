"""누락된 DART filing scope를 전체 재무제표 API로 선택 보강한다.

예:
  uv run python -m pipeline.bronze.financials_full \
    --scope 004990:2015:11011:CFS \
    --scope 096760:2016:11014:CFS \
    --dest s3

원본은 ``financials/dart_full``에 별도로 저장한다. Silver 변환은 주요계정
원본에 없는 business key를 채운다. 자산·부채·자본은 같은 공시 revision의
전체재무제표가 회계식을 1% 이내로 만족할 때만 세 값을 원자적으로 교체한다.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import write_text_if_changed

_YEAR_PARTITION_RE = re.compile(r"(?:^|/)year=(\d{4})(?:/|$)")


def _parse_scope(value: str) -> tuple[str, int, str, str]:
    try:
        ticker, year, report_code, fs_div = value.split(":")
        parsed_year = int(year)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "scope must be TICKER:YEAR:REPORT_CODE:CFS|OFS"
        ) from exc
    if (
        len(ticker) != 6
        or not ticker.isdigit()
        or report_code not in financials.REPRT_CODES
        or fs_div not in {"CFS", "OFS"}
    ):
        raise argparse.ArgumentTypeError(
            "scope must be TICKER:YEAR:REPORT_CODE:CFS|OFS"
        )
    return ticker, parsed_year, report_code, fs_div


def accounting_warning_scopes(
    source_root: str,
) -> list[tuple[str, int, str, str]]:
    """주요계정의 회계식 불일치 filing scope를 보충 수집 대상으로 만든다."""
    from pipeline.silver import financials as silver_financials

    candidates, _ = silver_financials.prepare(source_root)
    scope_key = [
        "identifier",
        "source",
        "period_end",
        "fiscal_period",
        "fs_type",
        "revision_key",
    ]
    pivot = candidates.pivot_table(
        index=scope_key,
        columns="metric",
        values="value",
        aggfunc="first",
    )
    required = list(silver_financials.ACCOUNTING_METRICS)
    if not set(required).issubset(pivot.columns):
        return []
    values = pivot[required].dropna().copy()
    values["relative_error"] = (
        values["total_assets"]
        - values["total_liabilities"]
        - values["total_equity"]
    ).abs() / values["total_assets"].abs().replace(0, float("nan"))
    failed = values[
        values["relative_error"].gt(
            silver_financials.ACCOUNTING_TOLERANCE
        )
    ].reset_index()
    if failed.empty:
        return []

    failed_index = set(
        failed[scope_key].itertuples(index=False, name=None)
    )
    source_rows = candidates[
        candidates["metric"].isin(required)
        & candidates[scope_key].apply(tuple, axis=1).isin(failed_index)
        & ~candidates["source_file"].astype(str).str.contains(
            "/financials/dart_full/",
            regex=False,
        )
    ]
    scopes: set[tuple[str, int, str, str]] = set()
    for row in source_rows.itertuples():
        source_file = str(row.source_file).replace("\\", "/")
        year_match = _YEAR_PARTITION_RE.search(source_file)
        report_code = Path(source_file).name[:5]
        if year_match is None or report_code not in financials.REPRT_CODES:
            raise RuntimeError(
                "cannot derive DART API scope from source file: "
                f"{source_file}"
            )
        scopes.add((
            str(row.identifier),
            int(year_match.group(1)),
            report_code,
            str(row.fs_type),
        ))
    return sorted(scopes)


def run(
    scopes: list[tuple[str, int, str, str]],
    dest: str,
) -> list[str]:
    base = base_uri(dest)
    corp_by_stock = {
        stock_code: corp_code
        for corp_code, stock_code
        in financials.ensure_corp_code_xml(base)
    }
    saved = []
    for ticker, year, report_code, fs_div in scopes:
        corp_code = corp_by_stock.get(ticker)
        if corp_code is None:
            raise RuntimeError(f"DART corp code missing for ticker={ticker}")
        status, payload = financials._fetch_single_all(  # noqa: SLF001
            corp_code,
            year,
            report_code,
            fs_div,
        )
        if status == "020":
            raise financials.QuotaExceeded(
                f"full-statement {ticker}:{year}:{report_code}:{fs_div}"
            )
        if status != "000" or not payload or not payload.get("list"):
            raise RuntimeError(
                "DART full statement unavailable: "
                f"scope={ticker}:{year}:{report_code}:{fs_div}, "
                f"status={status}, message={payload and payload.get('message')}"
            )
        path = (
            f"{base}/financials/dart_full/year={year}/corp={ticker}/"
            f"{report_code}-{fs_div}.json"
        )
        write_text_if_changed(
            json.dumps(payload["list"], ensure_ascii=False),
            path,
        )
        saved.append(path)
        print(f"[financials-full] saved scope={ticker}:{year}:{report_code}:{fs_div}")
        time.sleep(financials.CALL_GAP_SEC)
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scope",
        action="append",
        type=_parse_scope,
    )
    parser.add_argument(
        "--accounting-warnings-root",
        help=(
            "scan this downloaded Bronze root and collect full statements "
            "for every accounting-equation warning scope"
        ),
    )
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    args = parser.parse_args()
    scopes = list(args.scope or [])
    if args.accounting_warnings_root:
        scopes.extend(
            accounting_warning_scopes(args.accounting_warnings_root)
        )
    scopes = sorted(set(scopes))
    if not scopes:
        parser.error(
            "provide --scope or --accounting-warnings-root with failing scopes"
        )
    print(f"[financials-full] target scopes={len(scopes)}")
    run(scopes, args.dest)


if __name__ == "__main__":
    main()
