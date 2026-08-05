"""OpenDART 배당에 관한 사항 API -> Bronze 원문.

과거 백필은 이미 수집된 DART 정기보고서 경로를 후보 인덱스로 사용한다. 따라서
상장사×연도×보고서 조합을 무작정 호출하지 않고 실제 보고서가 존재하는 경우만
``alotMatter.json``을 요청한다.

저장 경로::

  dividends/dart/alot-matter/year=YYYY/report=<reprt_code>/corp=<ticker>/
    rcept=<접수번호>/response.json
    rcept=<접수번호>/manifest.json

응답은 byte-for-byte로 저장하고 manifest에는 API key를 제외한 요청 정보와
SHA-256을 기록한다. 정정 보고서는 새로운 접수번호 경로에 저장되므로 이전 원문을
덮어쓰지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import write_bytes


API_URL = "https://opendart.fss.or.kr/api/alotMatter.json"
ANNUAL_REPORT_CODE = "11011"
REPORT_CODES = ("11011", "11013", "11012", "11014")
CALL_GAP_SEC = 0.3
FINANCIAL_PATH_RE = re.compile(
    r"(?:^|/)financials/dart/year=(\d{4})/corp=(\d{6})/"
    r"(11011|11012|11013|11014)\.json$"
)


class DividendApiError(RuntimeError):
    """재시도로 해결되지 않은 OpenDART 배당 API 오류."""


class QuotaExceeded(DividendApiError):
    """OpenDART 일일 사용한도 초과(status=020)."""


class CallBudgetReached(DividendApiError):
    """한 실행의 안전 호출 예산에 도달함. 재실행하면 완료분을 건너뛴다."""


@dataclass(frozen=True, order=True)
class Candidate:
    year: int
    report_code: str
    ticker: str
    corp_code: str


@dataclass(frozen=True)
class RawResponse:
    body: bytes
    payload: dict
    status_code: int
    content_type: str
    received_at: datetime


def _candidate_from_financial_path(
    path: str,
    *,
    stock_to_corp: dict[str, str],
    fromyear: int | None = None,
    toyear: int | None = None,
    report_codes: set[str] | None = None,
) -> Candidate | None:
    match = FINANCIAL_PATH_RE.search(path.replace("\\", "/"))
    if not match:
        return None
    year = int(match.group(1))
    ticker = match.group(2)
    report_code = match.group(3)
    if fromyear is not None and year < fromyear:
        return None
    if toyear is not None and year > toyear:
        return None
    if report_codes is not None and report_code not in report_codes:
        return None
    corp_code = stock_to_corp.get(ticker)
    if not corp_code:
        return None
    return Candidate(year, report_code, ticker, corp_code)


def _financial_paths(base: str, fromyear: int, toyear: int) -> list[str]:
    if not base.startswith("s3://"):
        root = Path(base)
        return [
            str(path)
            for year in range(fromyear, toyear + 1)
            for path in root.glob(f"financials/dart/year={year}/corp=*/*.json")
        ]

    import boto3

    without_scheme = base.removeprefix("s3://")
    bucket, _, base_prefix = without_scheme.partition("/")
    s3 = boto3.client("s3")
    paths: list[str] = []
    for year in range(fromyear, toyear + 1):
        prefix = "/".join(
            part
            for part in (base_prefix.rstrip("/"), f"financials/dart/year={year}/")
            if part
        )
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=prefix,
        ):
            paths.extend(
                f"s3://{bucket}/{item['Key']}"
                for item in page.get("Contents", [])
                if item["Key"].endswith(".json")
            )
    return paths


def discover_candidates(
    base: str,
    fromyear: int,
    toyear: int,
    *,
    report_codes: Iterable[str] = (ANNUAL_REPORT_CODE,),
    financial_paths: Iterable[str] | None = None,
) -> list[Candidate]:
    corps = financials.ensure_corp_code_xml(base)
    stock_to_corp = {ticker: corp_code for corp_code, ticker in corps}
    allowed = set(report_codes)
    paths = list(financial_paths) if financial_paths is not None else _financial_paths(
        base,
        fromyear,
        toyear,
    )
    candidates = {
        candidate
        for path in paths
        if (
            candidate := _candidate_from_financial_path(
                path,
                stock_to_corp=stock_to_corp,
                fromyear=fromyear,
                toyear=toyear,
                report_codes=allowed,
            )
        )
        is not None
    }
    return sorted(candidates)


def _candidate_prefix(base: str, candidate: Candidate) -> str:
    return (
        f"{base}/dividends/dart/alot-matter/year={candidate.year}/"
        f"report={candidate.report_code}/corp={candidate.ticker}"
    )


class _Writer:
    """Manifest-last 저장과 완료 후보 인덱스를 담당한다."""

    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self._existing: set[str] = set()
        if not self.base.startswith("s3://"):
            root = Path(self.base) / "dividends/dart/alot-matter"
            if root.exists():
                self._existing.update(str(path) for path in root.rglob("*"))
            self._bucket = None
            self._s3 = None
            return

        import boto3

        without_scheme = self.base.removeprefix("s3://")
        self._bucket, _, base_prefix = without_scheme.partition("/")
        prefix = "/".join(
            part
            for part in (base_prefix.rstrip("/"), "dividends/dart/alot-matter/")
            if part
        )
        self._s3 = boto3.client("s3")
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket,
            Prefix=prefix,
        ):
            self._existing.update(
                f"s3://{self._bucket}/{item['Key']}"
                for item in page.get("Contents", [])
            )

    def complete(self, candidate: Candidate) -> bool:
        prefix = _candidate_prefix(self.base, candidate) + "/rcept="
        return any(
            path.startswith(prefix) and path.endswith("/manifest.json")
            and path.removesuffix("manifest.json") + "response.json" in self._existing
            for path in self._existing
        )

    def save_pair(self, response_uri: str, body: bytes, manifest: bytes) -> bool:
        manifest_uri = response_uri.removesuffix("response.json") + "manifest.json"
        if response_uri in self._existing and manifest_uri in self._existing:
            return False
        write_bytes(body, response_uri)
        write_bytes(manifest, manifest_uri)
        self._existing.update((response_uri, manifest_uri))
        return True


def _fetch(candidate: Candidate, *, tries: int = 4) -> RawResponse:
    params = {
        "crtfc_key": os.environ["DART_API_KEY"],
        "corp_code": candidate.corp_code,
        "bsns_year": str(candidate.year),
        "reprt_code": candidate.report_code,
    }
    last_error: Exception | None = None
    for attempt in range(tries):
        try:
            response = requests.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "")
            if status == "020":
                raise QuotaExceeded(
                    f"OpenDART quota exceeded: year={candidate.year} "
                    f"report={candidate.report_code} ticker={candidate.ticker}"
                )
            if status not in {"000", "013"}:
                raise DividendApiError(
                    f"OpenDART dividend error: status={status} "
                    f"message={payload.get('message')} ticker={candidate.ticker}"
                )
            return RawResponse(
                body=response.content,
                payload=payload,
                status_code=response.status_code,
                content_type=response.headers.get("Content-Type", "application/json"),
                received_at=datetime.now(timezone.utc),
            )
        except (QuotaExceeded, DividendApiError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    raise DividendApiError(
        f"OpenDART dividend request failed: ticker={candidate.ticker}"
    ) from last_error


def _receipt_partition(raw: RawResponse) -> tuple[str, list[str]]:
    receipts = sorted({
        str(row.get("rcept_no") or "")
        for row in raw.payload.get("list") or []
        if re.fullmatch(r"\d{14}", str(row.get("rcept_no") or ""))
    })
    return (receipts[-1] if receipts else "no-data"), receipts


def _manifest(
    candidate: Candidate,
    raw: RawResponse,
    response_uri: str,
    receipts: list[str],
) -> bytes:
    payload = {
        "provider": "OpenDART",
        "endpoint": "alotMatter.json",
        "request_params": {
            "corp_code": candidate.corp_code,
            "stock_code": candidate.ticker,
            "bsns_year": str(candidate.year),
            "reprt_code": candidate.report_code,
        },
        "dart_status": raw.payload.get("status"),
        "status_code": raw.status_code,
        "content_type": raw.content_type,
        "received_at": raw.received_at.isoformat(),
        "rcept_nos": receipts,
        "object_uri": response_uri,
        "content_length": len(raw.body),
        "sha256": hashlib.sha256(raw.body).hexdigest(),
        "complete": True,
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def run(
    fromyear: int,
    toyear: int,
    dest: str,
    *,
    report_codes: Iterable[str] = (ANNUAL_REPORT_CODE,),
    financial_paths: Iterable[str] | None = None,
    refresh_existing: bool = False,
    max_calls: int | None = None,
) -> list[str]:
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    if not os.environ.get("DART_API_KEY"):
        raise SystemExit("DART_API_KEY 환경변수가 없습니다 (.env 확인)")
    base = base_uri(dest)
    candidates = discover_candidates(
        base,
        fromyear,
        toyear,
        report_codes=report_codes,
        financial_paths=financial_paths,
    )
    writer = _Writer(base)
    changed: list[str] = []
    calls = skipped = no_data = 0
    print(
        f"[dividends-dart] candidates={len(candidates)} "
        f"years={fromyear}-{toyear} dest={dest}",
        flush=True,
    )
    for candidate in candidates:
        if not refresh_existing and writer.complete(candidate):
            skipped += 1
            continue
        if max_calls is not None and calls >= max_calls:
            raise CallBudgetReached(
                f"DART dividend call budget reached: calls={calls}; "
                "같은 명령을 재실행하면 완료 후보를 건너뜁니다."
            )
        raw = _fetch(candidate)
        calls += 1
        receipt, receipts = _receipt_partition(raw)
        response_uri = (
            f"{_candidate_prefix(base, candidate)}/rcept={receipt}/response.json"
        )
        manifest = _manifest(candidate, raw, response_uri, receipts)
        if writer.save_pair(response_uri, raw.body, manifest):
            changed.extend((
                response_uri,
                response_uri.removesuffix("response.json") + "manifest.json",
            ))
        if raw.payload.get("status") == "013":
            no_data += 1
        if calls % 100 == 0:
            print(
                f"[dividends-dart] calls={calls} skipped={skipped} "
                f"no_data={no_data}",
                flush=True,
            )
        time.sleep(CALL_GAP_SEC)
    print(
        f"[dividends-dart] complete calls={calls} skipped={skipped} "
        f"no_data={no_data} changed={len(changed)}",
        flush=True,
    )
    return changed


def run_for_financial_paths(paths: Iterable[str], dest: str) -> list[str]:
    paths = list(paths)
    years = [
        int(match.group(1))
        for path in paths
        if (match := FINANCIAL_PATH_RE.search(path.replace("\\", "/")))
    ]
    if not years:
        return []
    return run(
        min(years),
        max(years),
        dest,
        report_codes=REPORT_CODES,
        financial_paths=paths,
        refresh_existing=True,
        max_calls=len(paths),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="fromyear", type=int, required=True)
    parser.add_argument("--to", dest="toyear", type=int, required=True)
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    parser.add_argument("--reports", choices=["annual", "all"], default="annual")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--max-calls",
        type=int,
        default=int(os.environ.get("DART_DIVIDEND_MAX_CALLS", "15000")),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        args.fromyear,
        args.toyear,
        args.dest,
        report_codes=(ANNUAL_REPORT_CODE,) if args.reports == "annual" else REPORT_CODES,
        refresh_existing=args.refresh,
        max_calls=args.max_calls,
    )


if __name__ == "__main__":
    main()
