import hashlib
import json
from datetime import date

from pipeline.bronze import fmp


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
