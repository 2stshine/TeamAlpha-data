"""OpenDART 기업행사 공시와 구조화 주요사항보고서 → Bronze.

JSON API:
  list.json              전체 공시 발견(주요사항보고·거래소공시)
  piicDecsn.json         유상증자
  fricDecsn.json         무상증자
  pifricDecsn.json       유무상증자
  crDecsn.json           감자
  cmpMgDecsn.json        합병
  cmpDvDecsn.json        회사분할
  cmpDvmgDecsn.json      회사분할합병
  stkExtrDecsn.json      주식교환·이전

document.xml은 이름과 달리 공시 원문 ZIP binary를 반환한다. 액면분할·병합,
권리락·배당락처럼 가격조정 효력일·비율 확인이 필요하지만 전용 구조화 API가
없는 거래소 공시에만 사용한다. 배당결정, 변경상장, 거래정지·상장폐지는
목록 JSON만 보존한다.

저장:
  corporate_actions/dart/disclosures/year=YYYY/date=YYYY-MM-DD/
    corp=<ticker>/rcept=<rcept_no>.json
  corporate_actions/dart/structured/event=<event>/year=YYYY/
    corp=<ticker>/rcept=<rcept_no>.json
  corporate_actions/dart/documents/year=YYYY/corp=<ticker>/
    rcept=<rcept_no>.zip

모든 JSON 파일은 OpenDART list[]의 개별 원본 객체를 값 변환 없이 저장한다.
정정·철회는 별도 rcept_no이므로 기존 공시를 덮어쓰지 않는다.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import time
import zipfile
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator

import requests

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import (
    exists,
    read_bytes,
    write_bytes,
    write_text_if_changed,
)

API_ROOT = "https://opendart.fss.or.kr/api"
LIST_URL = f"{API_ROOT}/list.json"
DOCUMENT_URL = f"{API_ROOT}/document.xml"
CALL_GAP_SEC = 0.3
PAGE_COUNT = 100
DISCLOSURE_TYPES = ("B", "I")  # 주요사항보고, 거래소공시
STRUCTURED_API_START = "20150101"
API_WORKERS = 4


class DartApiError(RuntimeError):
    """재시도로 해결되지 않은 OpenDART 오류."""


class QuotaExceeded(DartApiError):
    """OpenDART 사용한도 초과(status=020)."""


class DocumentUnavailable(DartApiError):
    """공시 목록은 있지만 원문 파일이 없는 응답(status=014)."""

    def __init__(self, rcept_no: str, response_text: str):
        super().__init__(f"OpenDART document unavailable: rcept_no={rcept_no}")
        self.response_text = response_text


@dataclass(frozen=True)
class EventApi:
    slug: str
    endpoint: str
    title_tokens: tuple[str, ...]


class _BronzeWriter:
    """로컬은 동기 저장, S3는 기존 key 인덱스와 병렬 PUT을 사용한다."""

    def __init__(self, base: str, *, workers: int = 16):
        self.base = base
        self._existing: set[str] = set()
        self._futures: dict[Future, str] = {}
        self._executor: ThreadPoolExecutor | None = None
        self._s3 = None
        self._bucket = ""
        if not base.startswith("s3://"):
            return

        import boto3
        from botocore.config import Config

        without_scheme = base.removeprefix("s3://")
        self._bucket, _, base_prefix = without_scheme.partition("/")
        prefix = "/".join(
            part for part in (base_prefix.rstrip("/"), "corporate_actions/dart/")
            if part
        )
        self._s3 = boto3.client(
            "s3",
            config=Config(max_pool_connections=workers),
        )
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                self._existing.add(f"s3://{self._bucket}/{obj['Key']}")
        self._executor = ThreadPoolExecutor(max_workers=workers)

    def exists(self, path: str) -> bool:
        if self._executor is not None:
            return path in self._existing
        return exists(path)

    def _put_s3(self, path: str, data: bytes) -> None:
        assert self._s3 is not None
        key = path.removeprefix(f"s3://{self._bucket}/")
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=data)

    def _read_s3_json(self, path: str) -> dict:
        assert self._s3 is not None
        key = path.removeprefix(f"s3://{self._bucket}/")
        body = self._s3.get_object(Bucket=self._bucket, Key=key)["Body"]
        try:
            return json.loads(body.read())
        finally:
            body.close()

    def recover_disclosures(
        self,
        fromdate: str,
        todate: str,
    ) -> list[dict]:
        """완료 marker는 있지만 manifest가 없을 때 S3 개별 JSON에서 복구한다."""
        if self._executor is None:
            return []
        start = _parse_ymd(fromdate).isoformat()
        finish = _parse_ymd(todate).isoformat()
        paths = []
        for path in self._existing:
            if "/corporate_actions/dart/disclosures/" not in path:
                continue
            match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path)
            if match and start <= match.group(1) <= finish:
                paths.append(path)
        paths.sort()

        by_receipt: dict[str, dict] = {}
        chunk_size = 1000
        for chunk_start in range(0, len(paths), chunk_size):
            chunk = paths[chunk_start : chunk_start + chunk_size]
            futures = [
                self._executor.submit(self._read_s3_json, path)
                for path in chunk
            ]
            for future in as_completed(futures):
                row = future.result()
                rcept_no = str(row.get("rcept_no") or "")
                if re.fullmatch(r"\d{14}", rcept_no):
                    by_receipt[rcept_no] = row
            done = min(chunk_start + len(chunk), len(paths))
            print(
                "[corporate-actions] recover manifest "
                f"{done}/{len(paths)}",
                flush=True,
            )
        return [by_receipt[key] for key in sorted(by_receipt)]

    def dependency_paths(
        self,
        fromdate: str,
        todate: str,
    ) -> list[str]:
        """S3에 이미 있는 구조화 공시·원문 중 증거 기간을 반환한다."""
        if self._executor is None:
            return []
        selected = []
        for path in self._existing:
            if not (
                "/corporate_actions/dart/structured/" in path
                or "/corporate_actions/dart/documents/" in path
            ):
                continue
            match = re.search(r"/rcept=(\d{8})\d{6}\.(?:json|zip)$", path)
            if match and fromdate <= match.group(1) <= todate:
                selected.append(path)
        return sorted(selected)

    def _submit(self, path: str, data: bytes) -> bool:
        if path in self._existing:
            return False
        assert self._executor is not None
        self._existing.add(path)
        future = self._executor.submit(self._put_s3, path, data)
        self._futures[future] = path
        return True

    def save_json(self, row: object, path: str) -> bool:
        rendered = json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if self._executor is not None:
            return self._submit(path, rendered.encode("utf-8"))
        return write_text_if_changed(rendered, path)

    def save_bytes(self, data: bytes, path: str) -> bool:
        if self._executor is not None:
            return self._submit(path, data)
        if exists(path):
            return False
        write_bytes(data, path)
        return True

    def close(self) -> None:
        if self._executor is None:
            return
        try:
            for future in as_completed(self._futures):
                future.result()
        finally:
            self._executor.shutdown(wait=True, cancel_futures=False)


# 더 긴/구체적인 제목을 먼저 검사한다.
EVENT_APIS = (
    EventApi("combined_offering", "pifricDecsn.json", ("유무상증자결정",)),
    EventApi("paid_increase", "piicDecsn.json", ("유상증자결정",)),
    EventApi("bonus_issue", "fricDecsn.json", ("무상증자결정",)),
    EventApi("capital_reduction", "crDecsn.json", ("감자결정",)),
    EventApi("split_merger", "cmpDvmgDecsn.json", ("회사분할합병결정",)),
    EventApi("company_split", "cmpDvDecsn.json", ("회사분할결정",)),
    EventApi("merger", "cmpMgDecsn.json", ("회사합병결정",)),
    EventApi(
        "share_exchange",
        "stkExtrDecsn.json",
        ("주식교환이전결정", "주식의포괄적교환이전"),
    ),
)

DOCUMENT_KEYWORDS = (
    "액면분할",
    "주식분할",
    "액면병합",
    "주식병합",
    "권리락",
    "배당락",
)

# 목록 JSON만으로 공시 발생과 접수일을 보존한다. 가격조정 효력일·비율을
# 확인해야 하는 위 DOCUMENT_KEYWORDS만 원문 ZIP을 추가 다운로드한다.
DISCLOSURE_ONLY_KEYWORDS = (
    "현금현물배당결정",
    "변경상장",
    "신규상장",
    "재상장",
    "매매거래정지",
    "거래정지",
    "상장폐지",
    "정리매매",
)


def _compact_title(value: object) -> str:
    """공백·괄호·정정 표식을 제거해 공시 제목을 안정적으로 비교한다."""
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _event_api_for_title(title: object) -> EventApi | None:
    compact = _compact_title(title)
    for event_api in EVENT_APIS:
        if any(token in compact for token in event_api.title_tokens):
            return event_api
    return None


def _needs_document(title: object) -> bool:
    compact = _compact_title(title)
    return any(keyword in compact for keyword in DOCUMENT_KEYWORDS)


def _is_relevant_disclosure(title: object) -> bool:
    compact = _compact_title(title)
    return _needs_document(title) or any(
        keyword in compact for keyword in DISCLOSURE_ONLY_KEYWORDS
    )


def _parse_ymd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _month_windows(fromdate: str, todate: str) -> Iterator[tuple[date, date]]:
    start = _parse_ymd(fromdate)
    finish = _parse_ymd(todate)
    if start > finish:
        raise ValueError(f"fromdate must be <= todate: {fromdate}>{todate}")
    current = start
    while current <= finish:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        window_end = min(finish, next_month - timedelta(days=1))
        yield current, window_end
        current = window_end + timedelta(days=1)


def _fetch_json(
    url: str,
    params: dict[str, str],
    *,
    tries: int = 4,
) -> dict:
    last_error: Exception | None = None
    for attempt in range(tries):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "")
            if status == "020":
                raise QuotaExceeded(
                    f"OpenDART quota exceeded: endpoint={url.rsplit('/', 1)[-1]}"
                )
            if status == "800" and attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
                continue
            if status not in {"000", "013"}:
                raise DartApiError(
                    "OpenDART error: "
                    f"endpoint={url.rsplit('/', 1)[-1]}, "
                    f"status={status}, message={payload.get('message')}"
                )
            return payload
        except QuotaExceeded:
            raise
        except DartApiError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    raise DartApiError(f"OpenDART JSON request failed: {url}") from last_error


def _fetch_document(rcept_no: str, *, tries: int = 4) -> bytes:
    last_error: Exception | None = None
    for attempt in range(tries):
        try:
            response = requests.get(
                DOCUMENT_URL,
                params={
                    "crtfc_key": os.environ["DART_API_KEY"],
                    "rcept_no": rcept_no,
                },
                timeout=90,
            )
            response.raise_for_status()
            content = response.content
            if zipfile.is_zipfile(io.BytesIO(content)):
                return content
            rendered = content.decode("utf-8", errors="replace")
            if "<status>020</status>" in rendered:
                raise QuotaExceeded(
                    f"OpenDART quota exceeded: document rcept_no={rcept_no}"
                )
            if "<status>014</status>" in rendered:
                raise DocumentUnavailable(rcept_no, rendered)
            raise DartApiError(
                f"OpenDART document is not ZIP: rcept_no={rcept_no}, "
                f"response={rendered[:200]}"
            )
        except QuotaExceeded:
            raise
        except DartApiError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    raise DartApiError(
        f"OpenDART document request failed: rcept_no={rcept_no}"
    ) from last_error


def _disclosure_path(base: str, row: dict, ticker: str) -> str:
    rcept_dt = str(row.get("rcept_dt") or "")
    if not re.fullmatch(r"\d{8}", rcept_dt):
        raise DartApiError(f"invalid rcept_dt: {rcept_dt!r}")
    rendered_date = datetime.strptime(rcept_dt, "%Y%m%d").date().isoformat()
    return (
        f"{base}/corporate_actions/dart/disclosures/"
        f"year={rcept_dt[:4]}/date={rendered_date}/"
        f"corp={ticker}/rcept={row['rcept_no']}.json"
    )


def _structured_path(
    base: str,
    event_api: EventApi,
    row: dict,
    ticker: str,
) -> str:
    rcept_no = str(row.get("rcept_no") or "")
    year = rcept_no[:4]
    if not re.fullmatch(r"\d{14}", rcept_no):
        raise DartApiError(f"invalid structured rcept_no: {rcept_no!r}")
    return (
        f"{base}/corporate_actions/dart/structured/"
        f"event={event_api.slug}/year={year}/corp={ticker}/"
        f"rcept={rcept_no}.json"
    )


def _document_path(base: str, row: dict, ticker: str) -> str:
    rcept_no = str(row.get("rcept_no") or "")
    if not re.fullmatch(r"\d{14}", rcept_no):
        raise DartApiError(f"invalid document rcept_no: {rcept_no!r}")
    return (
        f"{base}/corporate_actions/dart/documents/year={rcept_no[:4]}/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    )


def _document_unavailable_path(base: str, row: dict, ticker: str) -> str:
    return _document_path(base, row, ticker).replace(
        "/documents/",
        "/documents_unavailable/",
    ).removesuffix(".zip") + ".xml"


def _manifest_path(base: str, fromdate: str, todate: str, name: str) -> str:
    return (
        f"{base}/corporate_actions/dart/manifests/"
        f"from={fromdate}/to={todate}/{name}.json"
    )


def _discover_window(
    api_key: str,
    window_start: date,
    window_end: date,
) -> tuple[date, list[dict], int]:
    """한 달의 관련 공시 행을 찾는다. 월 단위 호출은 서로 독립적이다."""
    rows: list[dict] = []
    calls = 0
    for disclosure_type in DISCLOSURE_TYPES:
        page = 1
        while True:
            payload = _fetch_json(
                LIST_URL,
                {
                    "crtfc_key": api_key,
                    "bgn_de": window_start.strftime("%Y%m%d"),
                    "end_de": window_end.strftime("%Y%m%d"),
                    "last_reprt_at": "N",
                    "pblntf_ty": disclosure_type,
                    "sort": "date",
                    "sort_mth": "asc",
                    "page_no": str(page),
                    "page_count": str(PAGE_COUNT),
                },
            )
            calls += 1
            for row in payload.get("list") or []:
                report_name = row.get("report_nm")
                if (
                    _event_api_for_title(report_name) is not None
                    or _is_relevant_disclosure(report_name)
                ):
                    rows.append(row)
            total_page = int(payload.get("total_page") or 0)
            if page >= total_page:
                break
            page += 1
            time.sleep(CALL_GAP_SEC)
        time.sleep(CALL_GAP_SEC)
    return window_end, rows, calls


def _fetch_structured(
    api_key: str,
    todate: str,
    corp_code: str,
    event_api: EventApi,
) -> tuple[str, EventApi, dict]:
    payload = _fetch_json(
        f"{API_ROOT}/{event_api.endpoint}",
        {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            # 구조화 API의 기간은 정정 접수일이 아니라 최초 접수일
            # 기준이다. 최근 정정공시가 발견돼도 원공시는 오래전일 수 있어
            # 가용 데이터 시작일부터 다시 조회한다.
            "bgn_de": STRUCTURED_API_START,
            "end_de": todate,
        },
    )
    time.sleep(CALL_GAP_SEC)
    return corp_code, event_api, payload


def run(
    fromdate: str,
    todate: str,
    dest: str,
    *,
    download_documents: bool = True,
    include_dependencies: bool = False,
    dependency_fromdate: str | None = None,
) -> list[str]:
    """기간 내 기업행사 원본을 수집하고 Silver 입력 URI를 반환한다.

    기본값은 새로 쓰인 Bronze URI만 반환한다. 일일 ECS처럼 빈 로컬
    디스크에서 시작하는 호출자는 ``include_dependencies=True``로 이미 S3에
    존재하는 구조화 공시와 원문 ZIP도 함께 받아야 한다.
    """
    api_key = os.environ.get("DART_API_KEY")
    if not api_key:
        raise SystemExit("DART_API_KEY 환경변수가 없습니다 (.env 확인)")
    base = base_uri(dest)
    corps = financials.ensure_corp_code_xml(base)
    corp_to_stock = dict(corps)
    writer = _BronzeWriter(base)

    changed_paths: list[str] = []
    dependency_paths: set[str] = set()
    candidates: dict[str, dict] = {}
    list_calls = structured_calls = document_calls = unavailable_documents = 0
    print(
        f"[corporate-actions] discover {fromdate}~{todate}, dest={dest}",
        flush=True,
    )

    try:
        if include_dependencies:
            dependency_paths.update(writer.dependency_paths(
                dependency_fromdate or fromdate,
                todate,
            ))
        structured_marker = _manifest_path(
            base,
            fromdate,
            todate,
            "structured_complete",
        )
        discovery_manifest_path = _manifest_path(
            base,
            fromdate,
            todate,
            "disclosures",
        )
        manifest_bytes = read_bytes(discovery_manifest_path)
        if manifest_bytes is not None:
            discovered_rows = json.loads(manifest_bytes)
            print(
                "[corporate-actions] discovery manifest reused "
                f"rows={len(discovered_rows)}",
                flush=True,
            )
        else:
            discovered_rows = []
            if writer.exists(structured_marker):
                discovered_rows = writer.recover_disclosures(
                    fromdate,
                    todate,
                )
            if not discovered_rows:
                windows = list(_month_windows(fromdate, todate))
                discovered_by_receipt: dict[str, dict] = {}
                with ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
                    discovery_futures = [
                        executor.submit(
                            _discover_window,
                            api_key,
                            window_start,
                            window_end,
                        )
                        for window_start, window_end in windows
                    ]
                    for completed_windows, future in enumerate(
                        as_completed(discovery_futures),
                        start=1,
                    ):
                        window_end, rows, calls = future.result()
                        list_calls += calls
                        for row in rows:
                            rcept_no = str(row.get("rcept_no") or "")
                            if re.fullmatch(r"\d{14}", rcept_no):
                                discovered_by_receipt[rcept_no] = row
                        print(
                            "[corporate-actions] discovery "
                            f"{completed_windows}/{len(windows)} "
                            f"window_end={window_end.isoformat()} "
                            f"rows={len(discovered_by_receipt)}",
                            flush=True,
                        )
                discovered_rows = [
                    discovered_by_receipt[key]
                    for key in sorted(discovered_by_receipt)
                ]
            if writer.save_json(discovered_rows, discovery_manifest_path):
                changed_paths.append(discovery_manifest_path)

        for row in discovered_rows:
            event_api = _event_api_for_title(row.get("report_nm"))
            report_name = row.get("report_nm")
            needs_document = _needs_document(report_name)
            corp_code = str(row.get("corp_code") or "")
            ticker = (
                str(row.get("stock_code") or "").strip()
                or corp_to_stock.get(corp_code, "")
            )
            rcept_no = str(row.get("rcept_no") or "")
            if not ticker or not re.fullmatch(r"\d{14}", rcept_no):
                continue
            path = _disclosure_path(base, row, ticker)
            if writer.save_json(row, path):
                changed_paths.append(path)
            candidates[rcept_no] = {
                "row": row,
                "ticker": ticker,
                "event_api": event_api,
                "needs_document": needs_document,
            }
            if include_dependencies and event_api is not None:
                structured_path = _structured_path(
                    base,
                    event_api,
                    row,
                    ticker,
                )
                if writer.exists(structured_path):
                    dependency_paths.add(structured_path)
            if include_dependencies and needs_document:
                document_path = _document_path(base, row, ticker)
                if writer.exists(document_path):
                    dependency_paths.add(document_path)
        print(
            f"[corporate-actions] candidates prepared={len(candidates)}",
            flush=True,
        )

        structured_queries: dict[tuple[str, str], EventApi] = {}
        for candidate in candidates.values():
            event_api = candidate["event_api"]
            if event_api is None:
                continue
            row = candidate["row"]
            corp_code = str(row.get("corp_code") or "")
            if corp_code:
                structured_queries[(event_api.slug, corp_code)] = event_api

        if writer.exists(structured_marker):
            print(
                "[corporate-actions] structured phase already complete",
                flush=True,
            )
        else:
            with ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
                structured_futures = [
                    executor.submit(
                        _fetch_structured,
                        api_key,
                        todate,
                        corp_code,
                        event_api,
                    )
                    for (_, corp_code), event_api in sorted(
                        structured_queries.items(),
                        key=lambda item: item[0],
                    )
                ]
                for query_no, future in enumerate(
                    as_completed(structured_futures),
                    start=1,
                ):
                    corp_code, event_api, payload = future.result()
                    structured_calls += 1
                    ticker = corp_to_stock.get(corp_code, "")
                    for row in payload.get("list") or []:
                        row_ticker = (
                            ticker
                            or str(row.get("stock_code") or "").strip()
                        )
                        if not row_ticker:
                            continue
                        path = _structured_path(
                            base,
                            event_api,
                            row,
                            row_ticker,
                        )
                        if writer.save_json(row, path):
                            changed_paths.append(path)
                        if include_dependencies:
                            announced = str(row.get("rcept_no") or "")[:8]
                            if fromdate <= announced <= todate:
                                dependency_paths.add(path)
                    if query_no % 100 == 0:
                        print(
                            "[corporate-actions] structured "
                            f"{query_no}/{len(structured_queries)}",
                            flush=True,
                        )
            marker = {
                "status": "COMPLETE",
                "fromdate": fromdate,
                "todate": todate,
                "query_count": len(structured_queries),
            }
            if writer.save_json(marker, structured_marker):
                changed_paths.append(structured_marker)

        if download_documents:
            documents_marker = _manifest_path(
                base,
                fromdate,
                todate,
                "documents_complete",
            )
            if writer.exists(documents_marker):
                print(
                    "[corporate-actions] document phase already complete",
                    flush=True,
                )
            else:
                document_candidates = [
                    (rcept_no, candidate)
                    for rcept_no, candidate in sorted(candidates.items())
                    if candidate["needs_document"]
                ]
                missing_documents = []
                for rcept_no, candidate in document_candidates:
                    path = _document_path(
                        base,
                        candidate["row"],
                        candidate["ticker"],
                    )
                    unavailable_path = _document_unavailable_path(
                        base,
                        candidate["row"],
                        candidate["ticker"],
                    )
                    if include_dependencies and writer.exists(path):
                        dependency_paths.add(path)
                    if not writer.exists(path) and not writer.exists(
                        unavailable_path
                    ):
                        missing_documents.append((rcept_no, path))
                for document_no, (rcept_no, path) in enumerate(
                    missing_documents,
                    start=1,
                ):
                    candidate = candidates[rcept_no]
                    try:
                        content = _fetch_document(rcept_no)
                    except DocumentUnavailable as exc:
                        unavailable_path = _document_unavailable_path(
                            base,
                            candidate["row"],
                            candidate["ticker"],
                        )
                        if writer.save_bytes(
                            exc.response_text.encode("utf-8"),
                            unavailable_path,
                        ):
                            changed_paths.append(unavailable_path)
                        unavailable_documents += 1
                    else:
                        if writer.save_bytes(content, path):
                            changed_paths.append(path)
                            document_calls += 1
                    if document_no % 100 == 0:
                        print(
                            "[corporate-actions] documents "
                            f"{document_no}/{len(missing_documents)} "
                            f"unavailable={unavailable_documents}",
                            flush=True,
                        )
                    time.sleep(CALL_GAP_SEC)
                marker = {
                    "status": "COMPLETE",
                    "fromdate": fromdate,
                    "todate": todate,
                    "candidate_count": len(document_candidates),
                    "requested_count": len(missing_documents),
                    "downloaded_count": document_calls,
                    "unavailable_count": unavailable_documents,
                }
                if writer.save_json(marker, documents_marker):
                    changed_paths.append(documents_marker)
    finally:
        writer.close()

    print(
        "[corporate-actions] complete "
        f"candidates={len(candidates)}, list_calls={list_calls}, "
        f"structured_calls={structured_calls}, document_calls={document_calls}, "
        f"unavailable_documents={unavailable_documents}, "
        f"changed={len(changed_paths)}",
        flush=True,
    )
    return sorted(set(changed_paths) | dependency_paths)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--from", dest="fromdate", required=True, help="YYYYMMDD")
    parser.add_argument("--to", dest="todate", required=True, help="YYYYMMDD")
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    parser.add_argument(
        "--no-documents",
        action="store_true",
        help="전용 API 없는 공시의 ZIP 원문 다운로드 생략",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        args.fromdate,
        args.todate,
        args.dest,
        download_documents=not args.no_documents,
    )


if __name__ == "__main__":
    main()
