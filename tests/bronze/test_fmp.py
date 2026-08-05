import hashlib
import json
from datetime import date

from pipeline.bronze import fmp
from pipeline.fmp_commodities import COMMODITY_SPECS


class _Response:
    def __init__(self, status, body, headers=None):
        self.status_code = status
        self.content = body
        self.headers = headers or {"Content-Type": "application/json"}


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def test_raw_payload_is_byte_exact_and_manifest_has_no_api_key(tmp_path):
    payload = b'[{"symbol":"SPY","isEtf":true},{"symbol":"AAPL","isEtf":false}]\n'
    session = _Session([_Response(200, payload)])
    client = fmp.FMPClient(api_key="top-secret", session=session)

    paths = fmp.collect_raw(
        client,
        root=str(tmp_path),
        endpoint="company-screener",
        params={"limit": 100000},
        prefix="stock/fmp/universe/snapshot_date=2026-08-03",
        extension="json",
    )

    assert (tmp_path / "stock/fmp/universe/snapshot_date=2026-08-03/response.json").read_bytes() == payload
    manifest_raw = (tmp_path / "stock/fmp/universe/snapshot_date=2026-08-03/manifest.json").read_bytes()
    manifest = json.loads(manifest_raw)
    assert b"top-secret" not in manifest_raw
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["content_length"] == len(payload)
    assert fmp.verify_raw_object(*paths)
    assert session.calls[0][1]["headers"]["apikey"] == "top-secret"
    assert "top-secret" not in session.calls[0][0]

    # A certified logical partition is immutable/idempotent.
    assert fmp.collect_raw(
        client,
        root=str(tmp_path),
        endpoint="company-screener",
        params={"limit": 100000},
        prefix="stock/fmp/universe/snapshot_date=2026-08-03",
        extension="json",
    ) == paths
    assert len(session.calls) == 1


def test_client_retries_429_without_exposing_secret():
    waits = []
    session = _Session([
        _Response(429, b"rate limited", {"Retry-After": "0"}),
        _Response(200, b"[]"),
    ])
    client = fmp.FMPClient(
        api_key="secret", session=session, sleeper=waits.append,
    )

    response = client.get("stock-list")

    assert response.body == b"[]"
    assert waits == [0.0]
    assert len(session.calls) == 2
    assert client.logical_request_count == 1
    assert client.request_count == 2
    assert client.rate_limit_count == 1


def test_eod_bulk_learns_endpoint_specific_rate_after_429():
    waits = []
    session = _Session([
        _Response(429, b"rate limited", {"Retry-After": "0"}),
        _Response(200, b"csv"),
    ])
    client = fmp.FMPClient(
        api_key="secret", session=session, sleeper=waits.append,
    )

    assert client.get("eod-bulk", {"date": "2025-01-02"}).body == b"csv"
    assert client._endpoint_min_intervals["eod-bulk"] == 10.0
    assert client.logical_request_count == 1
    assert client.request_count == 2


def test_s3_resume_uses_manifest_and_head_without_downloading_payload(monkeypatch):
    object_uri = "s3://bronze/stock/fmp/eod-bulk/date=2026-07-31/response.csv"
    manifest_uri = "s3://bronze/stock/fmp/eod-bulk/date=2026-07-31/manifest.json"
    manifest = json.dumps({
        "complete": True,
        "object_uri": object_uri,
        "content_length": 123,
        "sha256": "a" * 64,
    }).encode()
    reads = []

    def fake_read(uri):
        reads.append(uri)
        return manifest if uri == manifest_uri else None

    monkeypatch.setattr(fmp, "read_bytes", fake_read)
    monkeypatch.setattr(fmp, "object_size", lambda uri: 123)

    assert fmp.verify_raw_object_for_resume(object_uri, manifest_uri)
    assert reads == [manifest_uri]


def test_xnys_sessions_exclude_weekdays_when_market_is_closed():
    sessions = fmp._xnys_sessions(date(2025, 1, 1), date(2025, 1, 10))

    assert date(2025, 1, 1) not in sessions
    assert date(2025, 1, 9) not in sessions  # Carter national day of mourning
    assert date(2025, 1, 2) in sessions


def test_calendar_window_bisects_capped_response(tmp_path):
    capped = json.dumps([
        {"symbol": f"S{i}", "date": "2015-01-02"}
        for i in range(fmp.CALENDAR_ROW_LIMIT)
    ]).encode()
    left = b'[{"symbol":"AAPL","date":"2015-01-01"}]'
    right = b'[{"symbol":"MSFT","date":"2015-01-02"}]'
    session = _Session([
        _Response(200, capped),
        _Response(200, left),
        _Response(200, right),
    ])
    client = fmp.FMPClient(api_key="secret", session=session)

    paths = fmp._collect_calendar_window(
        client,
        root=str(tmp_path),
        endpoint="dividends-calendar",
        prefix="corporate_actions/fmp/dividends/year=2015/window=2015-01",
        start=date(2015, 1, 1),
        end=date(2015, 1, 2),
    )

    parent = (
        tmp_path
        / "corporate_actions/fmp/dividends/year=2015/window=2015-01"
    )
    assert not (parent / "response.json").exists()
    assert (parent / "segment=2015-01-01_2015-01-01/response.json").read_bytes() == left
    assert (parent / "segment=2015-01-02_2015-01-02/response.json").read_bytes() == right
    assert len(paths) == 4
    assert len(session.calls) == 3


def test_month_windows_cover_requested_years():
    windows = fmp._month_windows(date(2019, 12, 15), date(2020, 2, 2))

    assert windows == [
        (date(2019, 12, 15), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2020, 1, 31)),
        (date(2020, 2, 1), date(2020, 2, 2)),
    ]


def test_commodity_collection_uses_only_28_allowlisted_series(tmp_path):
    provider_list = json.dumps([
        {"symbol": spec.symbol, "currency": spec.raw_currency}
        for spec in COMMODITY_SPECS
    ]).encode()
    price_payloads = [
        json.dumps([{
            "symbol": spec.symbol,
            "date": "2026-08-04",
            "open": 1,
            "high": 2,
            "low": 0.5,
            "close": 1.5,
            "volume": 10,
        }]).encode()
        for spec in COMMODITY_SPECS
    ]
    session = _Session([
        _Response(200, provider_list),
        *[_Response(200, payload) for payload in price_payloads],
    ])
    client = fmp.FMPClient(api_key="secret", session=session)

    paths = fmp._collect_commodity_prices(
        client,
        str(tmp_path),
        start=date(2026, 8, 4),
        end=date(2026, 8, 4),
        snapshot="2026-08-04",
        daily=True,
    )

    assert len(paths) == 2 * (1 + len(COMMODITY_SPECS))
    calls = session.calls
    assert calls[0][0].endswith("/commodities-list")
    requested = [call[1]["params"]["symbol"] for call in calls[1:]]
    assert requested == [spec.symbol for spec in COMMODITY_SPECS]


def test_monday_daily_collects_sunday_futures_session(tmp_path, monkeypatch):
    captured = {}

    def fake_collect(client, root, *, start, end, snapshot, daily):
        captured.update(
            start=start, end=end, snapshot=snapshot, daily=daily,
        )
        return []

    latest = tmp_path / "latest.json"
    latest.write_bytes(b"[]")

    def fake_collect_raw(*args, **kwargs):
        if kwargs.get("endpoint") == "latest-financial-statements":
            return [str(latest)]
        return []

    monkeypatch.setattr(fmp, "_collect_universe", lambda *args: [])
    monkeypatch.setattr(fmp, "collect_raw", fake_collect_raw)
    monkeypatch.setattr(fmp, "_collect_calendar_window", lambda *args, **kwargs: [])
    monkeypatch.setattr(fmp, "_collect_market_metadata", lambda *args: [])
    monkeypatch.setattr(fmp, "_collect_commodity_prices", fake_collect)
    monkeypatch.setattr(fmp, "base_uri", lambda dest: str(tmp_path))

    fmp.run_daily("20260803", "local")

    assert captured == {
        "start": date(2026, 8, 2),
        "end": date(2026, 8, 3),
        "snapshot": "2026-08-03",
        "daily": True,
    }


def test_universe_collects_all_screener_and_delisted_pages(tmp_path):
    screener_full = json.dumps([
        {"symbol": f"S{i}"} for i in range(fmp.SCREENER_PAGE_SIZE)
    ]).encode()
    delisted_full = json.dumps([
        {"symbol": f"D{i}"} for i in range(fmp.DELISTED_PAGE_SIZE)
    ]).encode()
    session = _Session([
        _Response(200, b"[]"),             # stock-list
        _Response(200, screener_full),      # screener page 0
        _Response(200, b'[{"symbol":"LAST"}]'),
        *[_Response(200, b"") for _ in fmp.PROFILE_PARTS],
        _Response(200, b'[{"oldSymbol":"OLD","newSymbol":"NEW"}]'),
        _Response(200, delisted_full),      # delisted page 0
        _Response(200, b'[{"symbol":"FINAL"}]'),
    ])
    client = fmp.FMPClient(api_key="secret", session=session)

    paths = fmp._collect_universe(
        client,
        str(tmp_path),
        "2026-08-04",
        all_delisted_pages=True,
    )

    assert any("company-screener/snapshot_date=2026-08-04/page=1" in p for p in paths)
    assert any("delisted/snapshot_date=2026-08-04/page=1" in p for p in paths)
    assert any("symbol-change/snapshot_date=2026-08-04/limit=10000" in p for p in paths)
    requested = [call[1].get("params") for call in session.calls]
    assert {"page": 1, "limit": fmp.SCREENER_PAGE_SIZE} in requested
    assert {"limit": fmp.SYMBOL_CHANGE_LIMIT} in requested
    assert {"page": 1, "limit": fmp.DELISTED_PAGE_SIZE} in requested
