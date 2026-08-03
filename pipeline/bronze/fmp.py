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

import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, read_bytes, write_bytes

BASE_URL = "https://financialmodelingprep.com/stable"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
PROFILE_PARTS = range(4)
US_EXCHANGES = ("NASDAQ", "NYSE", "AMEX")


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

    def get(self, endpoint: str, params: dict[str, object] | None = None) -> RawResponse:
        endpoint = endpoint.strip("/")
        safe_params = dict(params or {})
        url = f"{BASE_URL}/{endpoint}"
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            elapsed = time.monotonic() - self._last_request_started
            if self._last_request_started and elapsed < self.min_interval:
                self.sleeper(self.min_interval - elapsed)
            self._last_request_started = time.monotonic()
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
                delay = _retry_after(response.headers.get("Retry-After"))
                if delay is None:
                    delay = min(30.0, 2 ** (attempt - 1) + random.random())
                self.sleeper(min(60.0, delay))
                continue
            if response.status_code < 200 or response.status_code >= 300:
                raise FMPError(
                    f"FMP request failed endpoint={endpoint} "
                    f"status={response.status_code}"
                )
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
    if exists(object_uri) and verify_raw_object(object_uri, manifest_uri):
        return [object_uri, manifest_uri]
    response = client.get(endpoint, params)
    write_bytes(response.body, object_uri)
    write_bytes(_manifest(response, object_uri), manifest_uri)
    if not verify_raw_object(object_uri, manifest_uri):
        raise FMPError(f"Bronze checksum verification failed: {object_uri}")
    return [object_uri, manifest_uri]


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
    paths += collect_raw(
        client, root=root, endpoint="company-screener", params={"limit": 100000},
        prefix=f"stock/fmp/universe/company-screener/snapshot_date={snapshot}",
        extension="json",
    )
    for part in PROFILE_PARTS:
        paths += collect_raw(
            client, root=root, endpoint="profile-bulk", params={"part": part},
            prefix=(
                f"stock/fmp/universe/profile-bulk/snapshot_date={snapshot}/part={part}"
            ),
            extension="csv",
        )
    paths += collect_raw(
        client, root=root, endpoint="symbol-change", params=None,
        prefix=f"stock/fmp/universe/symbol-change/snapshot_date={snapshot}",
        extension="json",
    )
    limit = 1000
    max_pages = 100 if all_delisted_pages else 1
    for page in range(max_pages):
        page_paths = collect_raw(
            client, root=root, endpoint="delisted-companies",
            params={"page": page, "limit": limit},
            prefix=(
                f"stock/fmp/universe/delisted/snapshot_date={snapshot}/page={page}"
            ),
            extension="json",
        )
        paths += page_paths
        payload = read_bytes(page_paths[0]) or b"[]"
        if len(_json_rows(payload)) < limit:
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


def run_daily(day: str, dest: str = "s3") -> list[str]:
    target = datetime.strptime(day, "%Y%m%d").date()
    snapshot = target.isoformat()
    root = base_uri(dest)
    client = FMPClient(
        max_attempts=20,
        min_interval=float(
            os.environ.get("FMP_BACKFILL_MIN_INTERVAL_SECONDS", "12.5")
        ),
    )
    paths = _collect_universe(client, root, snapshot)
    paths += collect_raw(
        client, root=root, endpoint="eod-bulk", params={"date": snapshot},
        prefix=f"stock/fmp/eod-bulk/date={snapshot}", extension="csv",
    )
    action_from = (target - timedelta(days=30)).isoformat()
    action_to = (target + timedelta(days=30)).isoformat()
    for endpoint, directory in (
        ("splits-calendar", "splits"),
        ("dividends-calendar", "dividends"),
    ):
        paths += collect_raw(
            client, root=root, endpoint=endpoint,
            params={"from": action_from, "to": action_to},
            prefix=(
                f"corporate_actions/fmp/{directory}/snapshot_date={snapshot}/"
                f"from={action_from}/to={action_to}"
            ),
            extension="json",
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
    paths += _collect_market_metadata(client, root, snapshot)
    return paths


def _periods() -> tuple[str, ...]:
    return "Q1", "Q2", "Q3", "Q4", "FY"


def run_backfill(fromyear: int, toyear: int, dest: str = "s3") -> list[str]:
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    root = base_uri(dest)
    client = FMPClient()
    snapshot = date.today().isoformat()
    print(
        f"[fmp-bronze] backfill start years={fromyear}-{toyear} dest={dest}",
        flush=True,
    )
    paths = _collect_universe(
        client, root, snapshot, all_delisted_pages=True,
    )
    current = date(fromyear, 1, 1)
    end = date(toyear, 12, 31)
    business_days = sum(
        1
        for offset in range((end - current).days + 1)
        if (current + timedelta(days=offset)).weekday() < 5
    )
    processed_days = 0
    while current <= end:
        if current.weekday() < 5:
            ds = current.isoformat()
            paths += collect_raw(
                client, root=root, endpoint="eod-bulk", params={"date": ds},
                prefix=f"stock/fmp/eod-bulk/date={ds}", extension="csv",
            )
            processed_days += 1
            if processed_days % 25 == 0 or processed_days == business_days:
                print(
                    f"[fmp-bronze] eod {processed_days}/{business_days} date={ds}",
                    flush=True,
                )
        current += timedelta(days=1)
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
        start = date(year, 1, 1).isoformat()
        finish = date(year, 12, 31).isoformat()
        for endpoint, directory in (
            ("splits-calendar", "splits"),
            ("dividends-calendar", "dividends"),
        ):
            paths += collect_raw(
                client, root=root, endpoint=endpoint,
                params={"from": start, "to": finish},
                prefix=(
                    f"corporate_actions/fmp/{directory}/year={year}/"
                    f"from={start}/to={finish}"
                ),
                extension="json",
            )
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
    paths += _collect_market_metadata(client, root, snapshot)
    print(
        f"[fmp-bronze] backfill complete objects={len(paths)}",
        flush=True,
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("daily", "backfill"), required=True)
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
        raise SystemExit("backfill mode requires --from and --to")
    run_backfill(args.fromyear, args.toyear, args.dest)


if __name__ == "__main__":
    main()
