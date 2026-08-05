"""FMP stable API -> Bronze raw objects.

The response body is written byte-for-byte.  Universe filtering, ETF/fund
exclusion, renaming, type conversion and deduplication belong to Silver.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

import exchange_calendars as xcals
import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import object_size, read_bytes, write_bytes
from pipeline.fmp_commodities import COMMODITY_SPECS

BASE_URL = "https://financialmodelingprep.com/stable"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
PROFILE_PARTS = range(4)
US_EXCHANGES = ("NASDAQ", "NYSE", "AMEX")
CALENDAR_ROW_LIMIT = 4000
SCREENER_PAGE_SIZE = 10000
DELISTED_PAGE_SIZE = 100
SYMBOL_CHANGE_LIMIT = 10000


class FMPError(RuntimeError):
    pass


@dataclass(frozen=True)
class RawResponse:
    endpoint: str
    params: dict[str, object]
    status_code: int
    content_type: str
    received_at: datetime
    body: bytes


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(
                0.0,
                (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds(),
            )
        except (TypeError, ValueError, OverflowError):
            return None


class FMPClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        session: requests.Session | None = None,
        max_attempts: int = 5,
        timeout: tuple[float, float] = (10.0, 180.0),
        min_interval: float = 0.0,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.api_key = api_key or os.environ.get("FMP_API_KEY")
        if not self.api_key:
            raise FMPError("FMP_API_KEY 환경변수가 없습니다 (.env 확인)")
        self.session = session or requests.Session()
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.min_interval = max(0.0, min_interval)
        self.sleeper = sleeper
        self._last_request_started = 0.0
        self.logical_request_count = 0
        self.request_count = 0
        self.retry_count = 0
        self.rate_limit_count = 0
        self._endpoint_min_intervals: dict[str, float] = {}
        self._endpoint_successes: dict[str, int] = {}

    def get(self, endpoint: str, params: dict[str, object] | None = None) -> RawResponse:
        endpoint = endpoint.strip("/")
        safe_params = dict(params or {})
        url = f"{BASE_URL}/{endpoint}"
        last_error: Exception | None = None
        self.logical_request_count += 1
        for attempt in range(1, self.max_attempts + 1):
            request_interval = max(
                self.min_interval,
                self._endpoint_min_intervals.get(endpoint, 0.0),
            )
            elapsed = time.monotonic() - self._last_request_started
            if self._last_request_started and elapsed < request_interval:
                self.sleeper(request_interval - elapsed)
            self._last_request_started = time.monotonic()
            self.request_count += 1
            try:
                # Header auth keeps the credential out of URLs, logs and manifests.
                response = self.session.get(
                    url,
                    params=safe_params,
                    headers={"apikey": self.api_key, "Accept-Encoding": "identity"},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                self.sleeper(min(30.0, 2 ** (attempt - 1) + random.random()))
                continue
            if response.status_code in RETRYABLE_STATUS and attempt < self.max_attempts:
                self.retry_count += 1
                delay = _retry_after(response.headers.get("Retry-After"))
                if delay is None:
                    delay = min(30.0, 2 ** (attempt - 1) + random.random())
                if response.status_code == 429:
                    self.rate_limit_count += 1
                    self._endpoint_successes[endpoint] = 0
                    current_interval = self._endpoint_min_intervals.get(endpoint, 0.0)
                    if endpoint == "eod-bulk":
                        # This large endpoint has a much lower effective rate
                        # than lightweight APIs. Learn its cadence after the
                        # first rejection instead of wasting three retries per
                        # successful daily payload.
                        learned_interval = (
                            10.0 if current_interval < 10.0
                            else min(30.0, current_interval * 1.2)
                        )
                        self._endpoint_min_intervals[endpoint] = learned_interval
                    print(
                        f"[fmp-client] rate limited attempt={attempt} "
                        f"retry_after={delay:.2f}s "
                        f"next_interval={self._endpoint_min_intervals.get(endpoint, self.min_interval):.2f}s",
                        flush=True,
                    )
                self.sleeper(min(60.0, delay))
                continue
            if response.status_code < 200 or response.status_code >= 300:
                raise FMPError(
                    f"FMP request failed endpoint={endpoint} "
                    f"status={response.status_code}"
                )
            if endpoint in self._endpoint_min_intervals:
                successes = self._endpoint_successes.get(endpoint, 0) + 1
                self._endpoint_successes[endpoint] = successes
                if successes >= 100:
                    self._endpoint_min_intervals[endpoint] = max(
                        self.min_interval,
                        self._endpoint_min_intervals[endpoint] * 0.9,
                    )
                    self._endpoint_successes[endpoint] = 0
            return RawResponse(
                endpoint=endpoint,
                params=safe_params,
                status_code=response.status_code,
                content_type=response.headers.get("Content-Type", "application/octet-stream"),
                received_at=datetime.now(timezone.utc),
                body=response.content,
            )
        raise FMPError(
            f"FMP request failed endpoint={endpoint} attempts={self.max_attempts}: "
            f"{type(last_error).__name__ if last_error else 'retryable response'}"
        )


def _manifest(response: RawResponse, object_uri: str) -> bytes:
    body = {
        "provider": "FMP",
        "endpoint": response.endpoint,
        "request_params": response.params,
        "status_code": response.status_code,
        "content_type": response.content_type,
        "received_at": response.received_at.isoformat(),
        "object_uri": object_uri,
        "content_length": len(response.body),
        "sha256": hashlib.sha256(response.body).hexdigest(),
        "complete": True,
    }
    return json.dumps(
        body, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def verify_raw_object(object_uri: str, manifest_uri: str) -> bool:
    payload = read_bytes(object_uri)
    raw_manifest = read_bytes(manifest_uri)
    if payload is None or raw_manifest is None:
        return False
    try:
        metadata = json.loads(raw_manifest)
    except (TypeError, ValueError):
        return False
    return (
        metadata.get("complete") is True
        and metadata.get("object_uri") == object_uri
        and metadata.get("content_length") == len(payload)
        and metadata.get("sha256") == hashlib.sha256(payload).hexdigest()
    )


def verify_raw_object_for_resume(object_uri: str, manifest_uri: str) -> bool:
    """Cheaply certify a completed partition before resuming.

    Bronze writes the payload first and its checksum manifest last.  Therefore a
    complete manifest plus the matching object byte length is a safe completion
    marker.  This avoids downloading every multi-megabyte payload on each retry;
    ``verify_raw_object`` remains available for full checksum audits.
    """
    if not object_uri.startswith("s3://"):
        return verify_raw_object(object_uri, manifest_uri)
    raw_manifest = read_bytes(manifest_uri)
    if raw_manifest is None:
        return False
    try:
        metadata = json.loads(raw_manifest)
    except (TypeError, ValueError):
        return False
    content_length = metadata.get("content_length")
    checksum = metadata.get("sha256")
    return (
        metadata.get("complete") is True
        and metadata.get("object_uri") == object_uri
        and isinstance(content_length, int)
        and content_length >= 0
        and object_size(object_uri) == content_length
        and isinstance(checksum, str)
        and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None
    )


def collect_raw(
    client: FMPClient,
    *,
    root: str,
    endpoint: str,
    params: dict[str, object] | None,
    prefix: str,
    extension: str,
) -> list[str]:
    """Store one logical response without parsing or filtering it."""
    object_uri = f"{root.rstrip('/')}/{prefix.strip('/')}/response.{extension}"
    manifest_uri = f"{root.rstrip('/')}/{prefix.strip('/')}/manifest.json"
    if verify_raw_object_for_resume(object_uri, manifest_uri):
        return [object_uri, manifest_uri]
    response = client.get(endpoint, params)
    write_bytes(response.body, object_uri)
    write_bytes(_manifest(response, object_uri), manifest_uri)
    # Successful S3 PUTs plus the manifest-last protocol form the completion
    # marker. Local writes retain immediate byte-for-byte verification.
    if not object_uri.startswith("s3://") and not verify_raw_object(
        object_uri, manifest_uri,
    ):
        raise FMPError(f"Bronze checksum verification failed: {object_uri}")
    return [object_uri, manifest_uri]


def _calendar_rows(payload: bytes) -> list[dict]:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError) as exc:
        raise FMPError("calendar response is not valid JSON") from exc
    if not isinstance(value, list):
        raise FMPError(
            f"calendar response must be a list: type={type(value).__name__}"
        )
    return [item for item in value if isinstance(item, dict)]


def _collect_calendar_window(
    client: FMPClient,
    *,
    root: str,
    endpoint: str,
    prefix: str,
    start: date,
    end: date,
) -> list[str]:
    """Collect one calendar window, bisecting any capped 4,000-row response."""
    object_uri = f"{root.rstrip('/')}/{prefix.strip('/')}/response.json"
    manifest_uri = f"{root.rstrip('/')}/{prefix.strip('/')}/manifest.json"
    if verify_raw_object_for_resume(object_uri, manifest_uri):
        return [object_uri, manifest_uri]

    response = client.get(
        endpoint,
        {"from": start.isoformat(), "to": end.isoformat()},
    )
    rows = _calendar_rows(response.body)
    if len(rows) >= CALENDAR_ROW_LIMIT:
        if start >= end:
            raise FMPError(
                f"calendar daily response is capped: endpoint={endpoint} "
                f"date={start.isoformat()} rows={len(rows)}"
            )
        midpoint = start + timedelta(days=(end - start).days // 2)
        paths: list[str] = []
        for child_start, child_end in (
            (start, midpoint),
            (midpoint + timedelta(days=1), end),
        ):
            paths += _collect_calendar_window(
                client,
                root=root,
                endpoint=endpoint,
                prefix=(
                    f"{prefix.rstrip('/')}/segment="
                    f"{child_start.isoformat()}_{child_end.isoformat()}"
                ),
                start=child_start,
                end=child_end,
            )
        return paths

    write_bytes(response.body, object_uri)
    write_bytes(_manifest(response, object_uri), manifest_uri)
    if not object_uri.startswith("s3://") and not verify_raw_object(
        object_uri,
        manifest_uri,
    ):
        raise FMPError(f"Bronze checksum verification failed: {object_uri}")
    return [object_uri, manifest_uri]


def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    windows = []
    current = start
    while current <= end:
        next_month = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
        window_end = min(end, next_month - timedelta(days=1))
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _collect_dividend_backfill(
    client: FMPClient,
    root: str,
    fromyear: int,
    toyear: int,
) -> list[str]:
    paths: list[str] = []
    windows = _month_windows(
        date(fromyear, 1, 1),
        date(toyear, 12, 31),
    )
    for number, (start, end) in enumerate(windows, start=1):
        paths += _collect_calendar_window(
            client,
            root=root,
            endpoint="dividends-calendar",
            prefix=(
                f"corporate_actions/fmp/dividends/year={start.year}/"
                f"window={start:%Y-%m}"
            ),
            start=start,
            end=end,
        )
        print(
            f"[fmp-dividends] windows={number}/{len(windows)} "
            f"through={end.isoformat()}",
            flush=True,
        )
    return paths


def run_dividend_backfill(
    fromyear: int,
    toyear: int,
    dest: str = "s3",
) -> list[str]:
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    root = base_uri(dest)
    client = FMPClient(
        max_attempts=20,
        min_interval=float(
            os.environ.get("FMP_BACKFILL_MIN_INTERVAL_SECONDS", "0.5")
        ),
    )
    print(
        f"[fmp-dividends] backfill start years={fromyear}-{toyear} dest={dest}",
        flush=True,
    )
    paths = _collect_dividend_backfill(client, root, fromyear, toyear)
    print(
        f"[fmp-dividends] backfill complete objects={len(paths)} "
        f"api_calls={client.logical_request_count} "
        f"rate_limits={client.rate_limit_count}",
        flush=True,
    )
    return paths


def _json_rows(payload: bytes) -> list[dict]:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError):
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _safe_partition(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _collect_universe(
    client: FMPClient,
    root: str,
    snapshot: str,
    *,
    all_delisted_pages: bool = False,
) -> list[str]:
    paths: list[str] = []
    paths += collect_raw(
        client, root=root, endpoint="stock-list", params=None,
        prefix=f"stock/fmp/universe/stock-list/snapshot_date={snapshot}",
        extension="json",
    )
    # The stable screener caps each page at 10,000 rows even when a larger
    # limit is requested.  Preserve each provider page instead of silently
    # accepting only the first 10,000 instruments.
    for page in range(100):
        page_prefix = (
            f"stock/fmp/universe/company-screener/snapshot_date={snapshot}"
            if page == 0
            else (
                f"stock/fmp/universe/company-screener/snapshot_date={snapshot}/"
                f"page={page}"
            )
        )
        page_paths = collect_raw(
            client,
            root=root,
            endpoint="company-screener",
            params={"page": page, "limit": SCREENER_PAGE_SIZE},
            prefix=page_prefix,
            extension="json",
        )
        paths += page_paths
        payload = read_bytes(page_paths[0]) or b"[]"
        if len(_json_rows(payload)) < SCREENER_PAGE_SIZE:
            break
    else:
        raise FMPError("company-screener pagination exceeded 100 pages")
    for part in PROFILE_PARTS:
        paths += collect_raw(
            client, root=root, endpoint="profile-bulk", params={"part": part},
            prefix=(
                f"stock/fmp/universe/profile-bulk/snapshot_date={snapshot}/part={part}"
            ),
            extension="csv",
        )
    # Without an explicit limit FMP returns only the latest 100 changes.
    paths += collect_raw(
        client,
        root=root,
        endpoint="symbol-change",
        params={"limit": SYMBOL_CHANGE_LIMIT},
        prefix=(
            f"stock/fmp/universe/symbol-change/snapshot_date={snapshot}/"
            f"limit={SYMBOL_CHANGE_LIMIT}"
        ),
        extension="json",
    )
    max_pages = 100 if all_delisted_pages else 1
    for page in range(max_pages):
        page_paths = collect_raw(
            client, root=root, endpoint="delisted-companies",
            params={"page": page, "limit": DELISTED_PAGE_SIZE},
            prefix=(
                f"stock/fmp/universe/delisted/snapshot_date={snapshot}/page={page}"
            ),
            extension="json",
        )
        paths += page_paths
        payload = read_bytes(page_paths[0]) or b"[]"
        if len(_json_rows(payload)) < DELISTED_PAGE_SIZE:
            break
    return paths


def _collect_market_metadata(client: FMPClient, root: str, snapshot: str) -> list[str]:
    paths: list[str] = []
    for exchange in US_EXCHANGES:
        for endpoint in ("exchange-market-hours", "holidays-by-exchange"):
            paths += collect_raw(
                client, root=root, endpoint=endpoint, params={"exchange": exchange},
                prefix=(
                    f"market/fmp/{endpoint}/snapshot_date={snapshot}/exchange={exchange}"
                ),
                extension="json",
            )
    return paths


def _collect_commodity_prices(
    client: FMPClient,
    root: str,
    *,
    start: date,
    end: date,
    snapshot: str,
    daily: bool,
) -> list[str]:
    """Preserve the full provider list and the 28 admitted price responses."""
    paths = collect_raw(
        client,
        root=root,
        endpoint="commodities-list",
        params=None,
        prefix=f"commodities/fmp/list/snapshot_date={snapshot}",
        extension="json",
    )
    for number, spec in enumerate(COMMODITY_SPECS, start=1):
        prefix = (
            f"commodities/fmp/eod/symbol={spec.symbol}/date={start.isoformat()}"
            if daily and start == end
            else (
                f"commodities/fmp/eod/symbol={spec.symbol}/"
                f"from={start.isoformat()}/to={end.isoformat()}"
            )
        )
        paths += collect_raw(
            client,
            root=root,
            endpoint="historical-price-eod/full",
            params={
                "symbol": spec.symbol,
                "from": start.isoformat(),
                "to": end.isoformat(),
            },
            prefix=prefix,
            extension="json",
        )
        if not daily and (number % 7 == 0 or number == len(COMMODITY_SPECS)):
            print(
                f"[fmp-commodities] collected={number}/{len(COMMODITY_SPECS)}",
                flush=True,
            )
    return paths


def run_commodity_backfill(
    fromyear: int,
    toyear: int,
    dest: str = "s3",
) -> list[str]:
    """Collect continuous-futures history with one price call per series."""
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    start = date(fromyear, 1, 1)
    end = min(date(toyear, 12, 31), date.today())
    if start > end:
        return []
    root = base_uri(dest)
    client = FMPClient(
        max_attempts=20,
        min_interval=float(
            os.environ.get("FMP_BACKFILL_MIN_INTERVAL_SECONDS", "0.5")
        ),
    )
    print(
        f"[fmp-commodities] backfill start range={start}:{end} dest={dest}",
        flush=True,
    )
    paths = _collect_commodity_prices(
        client,
        root,
        start=start,
        end=end,
        snapshot=date.today().isoformat(),
        daily=False,
    )
    print(
        f"[fmp-commodities] backfill complete objects={len(paths)} "
        f"api_calls={client.logical_request_count} "
        f"rate_limits={client.rate_limit_count}",
        flush=True,
    )
    return paths


def run_daily(day: str, dest: str = "s3") -> list[str]:
    target = datetime.strptime(day, "%Y%m%d").date()
    snapshot = target.isoformat()
    root = base_uri(dest)
    client = FMPClient(
        max_attempts=20,
        min_interval=float(
            os.environ.get("FMP_DAILY_MIN_INTERVAL_SECONDS", "0.5")
        ),
    )
    paths = _collect_universe(client, root, snapshot)
    paths += collect_raw(
        client, root=root, endpoint="eod-bulk", params={"date": snapshot},
        prefix=f"stock/fmp/eod-bulk/date={snapshot}", extension="csv",
    )
    action_from = (target - timedelta(days=30)).isoformat()
    action_to = (target + timedelta(days=30)).isoformat()
    paths += collect_raw(
        client, root=root, endpoint="splits-calendar",
        params={"from": action_from, "to": action_to},
        prefix=(
            f"corporate_actions/fmp/splits/snapshot_date={snapshot}/"
            f"from={action_from}/to={action_to}"
        ),
        extension="json",
    )
    paths += _collect_calendar_window(
        client,
        root=root,
        endpoint="dividends-calendar",
        prefix=(
            f"corporate_actions/fmp/dividends/snapshot_date={snapshot}/"
            f"from={action_from}/to={action_to}"
        ),
        start=date.fromisoformat(action_from),
        end=date.fromisoformat(action_to),
    )
    latest_paths = collect_raw(
        client, root=root, endpoint="latest-financial-statements",
        params={"page": 0, "limit": 250},
        prefix=f"financials/fmp/latest/snapshot_date={snapshot}/page=0",
        extension="json",
    )
    paths += latest_paths
    latest_payload = read_bytes(latest_paths[0]) or b"[]"
    cutoff = target - timedelta(days=7)
    changed_symbols = set()
    for row in _json_rows(latest_payload):
        symbol = str(row.get("symbol") or "").strip()
        try:
            added = date.fromisoformat(str(row.get("dateAdded") or "")[:10])
        except ValueError:
            added = target
        if symbol and cutoff <= added <= target + timedelta(days=1):
            changed_symbols.add(symbol)
    for symbol in sorted(changed_symbols):
        safe_symbol = _safe_partition(symbol)
        for endpoint in (
            "income-statement",
            "balance-sheet-statement",
            "cash-flow-statement",
        ):
            paths += collect_raw(
                client,
                root=root,
                endpoint=endpoint,
                params={"symbol": symbol, "limit": 20},
                prefix=(
                    f"financials/fmp/by-symbol/snapshot_date={snapshot}/"
                    f"symbol={safe_symbol}/{endpoint}"
                ),
                extension="json",
            )
    paths += collect_raw(
        client, root=root, endpoint="historical-price-eod/full",
        params={"symbol": "USDKRW", "from": snapshot, "to": snapshot},
        prefix=f"fx/fmp/pair=USDKRW/date={snapshot}", extension="json",
    )
    # FMP labels Sunday-evening futures observations with the Sunday calendar
    # date.  When the completed equity target is Monday, include Sunday so the
    # valid opening session is not skipped by the daily incremental pipeline.
    commodity_start = (
        target - timedelta(days=1) if target.weekday() == 0 else target
    )
    paths += _collect_commodity_prices(
        client,
        root,
        start=commodity_start,
        end=target,
        snapshot=snapshot,
        daily=True,
    )
    paths += _collect_market_metadata(client, root, snapshot)
    return paths


def _periods() -> tuple[str, ...]:
    return "Q1", "Q2", "Q3", "Q4", "FY"


def _xnys_sessions(start: date, end: date) -> list[date]:
    """Return actual completed US equity sessions for the requested range."""
    if start > end:
        return []
    calendar = xcals.get_calendar("XNYS")
    return [
        timestamp.date()
        for timestamp in calendar.sessions_in_range(start.isoformat(), end.isoformat())
    ]


def run_backfill(fromyear: int, toyear: int, dest: str = "s3") -> list[str]:
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    root = base_uri(dest)
    client = FMPClient(
        max_attempts=20,
        min_interval=float(
            os.environ.get("FMP_BACKFILL_MIN_INTERVAL_SECONDS", "0.5")
        ),
    )
    snapshot = date.today().isoformat()
    print(
        f"[fmp-bronze] backfill start years={fromyear}-{toyear} dest={dest}",
        flush=True,
    )
    paths = _collect_universe(
        client, root, snapshot, all_delisted_pages=True,
    )
    start = date(fromyear, 1, 1)
    # A KST/UTC batch can run before the current US session has closed.  Never
    # certify a future/incomplete date as an immutable empty Bronze partition.
    end = min(date(toyear, 12, 31), date.today() - timedelta(days=1))
    sessions = _xnys_sessions(start, end)
    business_days = len(sessions)
    processed_days = 0
    eod_request_start = client.logical_request_count
    eod_started_at = time.monotonic()
    for session_date in sessions:
        ds = session_date.isoformat()
        paths += collect_raw(
            client, root=root, endpoint="eod-bulk", params={"date": ds},
            prefix=f"stock/fmp/eod-bulk/date={ds}", extension="csv",
        )
        processed_days += 1
        if processed_days % 25 == 0 or processed_days == business_days:
            elapsed = max(time.monotonic() - eod_started_at, 0.001)
            eod_api_calls = client.logical_request_count - eod_request_start
            print(
                f"[fmp-bronze] eod {processed_days}/{business_days} date={ds} "
                f"api_calls={eod_api_calls} reused={processed_days - eod_api_calls} "
                f"rate={processed_days / elapsed:.2f}/s "
                f"rate_limits={client.rate_limit_count}",
                flush=True,
            )
    for year in range(fromyear, toyear + 1):
        print(f"[fmp-bronze] statements/actions year={year}", flush=True)
        for period in _periods():
            for endpoint, statement in (
                ("income-statement-bulk", "income"),
                ("balance-sheet-statement-bulk", "balance"),
                ("cash-flow-statement-bulk", "cashflow"),
            ):
                paths += collect_raw(
                    client, root=root, endpoint=endpoint,
                    params={"year": year, "period": period},
                    prefix=(
                        f"financials/fmp/{statement}/year={year}/period={period}"
                    ),
                    extension="csv",
                )
        start_date = date(year, 1, 1)
        finish_date = min(date(year, 12, 31), date.today())
        if start_date > finish_date:
            continue
        start = start_date.isoformat()
        finish = finish_date.isoformat()
        paths += collect_raw(
            client, root=root, endpoint="splits-calendar",
            params={"from": start, "to": finish},
            prefix=(
                f"corporate_actions/fmp/splits/year={year}/"
                f"from={start}/to={finish}"
            ),
            extension="json",
        )
    paths += _collect_dividend_backfill(client, root, fromyear, toyear)
    paths += collect_raw(
        client, root=root, endpoint="historical-price-eod/full",
        params={
            "symbol": "USDKRW",
            "from": date(fromyear, 1, 1).isoformat(),
            "to": date(toyear, 12, 31).isoformat(),
        },
        prefix=f"fx/fmp/pair=USDKRW/from={fromyear}/to={toyear}",
        extension="json",
    )
    paths += _collect_commodity_prices(
        client,
        root,
        start=date(fromyear, 1, 1),
        end=min(date(toyear, 12, 31), date.today()),
        snapshot=snapshot,
        daily=False,
    )
    paths += _collect_market_metadata(client, root, snapshot)
    print(
        f"[fmp-bronze] backfill complete objects={len(paths)}",
        flush=True,
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("daily", "backfill", "dividends", "commodities"),
        required=True,
    )
    parser.add_argument("--date", help="daily date YYYYMMDD")
    parser.add_argument("--from", dest="fromyear", type=int)
    parser.add_argument("--to", dest="toyear", type=int)
    parser.add_argument("--dest", choices=("local", "s3"), default="local")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "daily":
        if not args.date:
            raise SystemExit("daily mode requires --date YYYYMMDD")
        run_daily(args.date, args.dest)
        return
    if args.fromyear is None or args.toyear is None:
        raise SystemExit("backfill/dividends/commodities mode requires --from and --to")
    if args.mode == "dividends":
        run_dividend_backfill(args.fromyear, args.toyear, args.dest)
        return
    if args.mode == "commodities":
        run_commodity_backfill(args.fromyear, args.toyear, args.dest)
        return
    run_backfill(args.fromyear, args.toyear, args.dest)


if __name__ == "__main__":
    main()
