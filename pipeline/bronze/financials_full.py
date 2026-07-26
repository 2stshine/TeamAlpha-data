"""누락된 DART filing scope를 전체 재무제표 API로 선택 보강한다.

예:
  uv run python -m pipeline.bronze.financials_full \
    --scope 004990:2015:11011:CFS \
    --scope 096760:2016:11014:CFS \
    --dest s3

원본은 ``financials/dart_full``에 별도로 저장한다. Silver 변환은 주요계정
원본에 없는 business key만 이 원천에서 채우며 기존 값을 덮어쓰지 않는다.
"""
from __future__ import annotations

import argparse
import json
import time

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import write_text_if_changed


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
        required=True,
    )
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    args = parser.parse_args()
    run(args.scope, args.dest)


if __name__ == "__main__":
    main()
