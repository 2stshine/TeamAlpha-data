import hashlib
import json

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
