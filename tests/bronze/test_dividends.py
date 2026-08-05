import hashlib
import json
from datetime import datetime, timezone

from pipeline.bronze import dividends


def _financial_file(root, year="2025", ticker="005930", report="11011"):
    path = (
        root / f"financials/dart/year={year}/corp={ticker}/{report}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]", encoding="utf-8")
    return path


def test_discover_candidates_uses_existing_reports_only(tmp_path, monkeypatch):
    annual = _financial_file(tmp_path)
    _financial_file(tmp_path, report="11013")
    monkeypatch.setattr(
        dividends.financials,
        "ensure_corp_code_xml",
        lambda _base: [("00126380", "005930")],
    )

    candidates = dividends.discover_candidates(
        str(tmp_path),
        2025,
        2025,
    )

    assert candidates == [
        dividends.Candidate(2025, "11011", "005930", "00126380")
    ]
    assert dividends._candidate_from_financial_path(
        str(annual),
        stock_to_corp={"005930": "00126380"},
    ) == candidates[0]


def test_run_preserves_raw_response_and_resumes(tmp_path, monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "top-secret")
    monkeypatch.setattr(dividends, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        dividends.financials,
        "ensure_corp_code_xml",
        lambda _base: [("00126380", "005930")],
    )
    _financial_file(tmp_path)
    payload = {
        "status": "000",
        "message": "정상",
        "list": [{
            "rcept_no": "20260310002820",
            "corp_code": "00126380",
            "se": "주당 현금배당금(원)",
            "stock_knd": "보통주",
            "thstrm": "1,668",
            "stlm_dt": "2025-12-31",
        }],
    }
    raw_body = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    calls = []

    def fake_fetch(candidate):
        calls.append(candidate)
        return dividends.RawResponse(
            body=raw_body,
            payload=payload,
            status_code=200,
            content_type="application/json;charset=UTF-8",
            received_at=datetime(2026, 3, 10, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(dividends, "_fetch", fake_fetch)
    monkeypatch.setattr(dividends.time, "sleep", lambda _seconds: None)

    changed = dividends.run(2025, 2025, "local")

    root = (
        tmp_path / "dividends/dart/alot-matter/year=2025/report=11011"
        / "corp=005930/rcept=20260310002820"
    )
    assert (root / "response.json").read_bytes() == raw_body
    manifest_raw = (root / "manifest.json").read_bytes()
    manifest = json.loads(manifest_raw)
    assert b"top-secret" not in manifest_raw
    assert manifest["sha256"] == hashlib.sha256(raw_body).hexdigest()
    assert manifest["request_params"]["stock_code"] == "005930"
    assert len(changed) == 2
    assert len(calls) == 1

    assert dividends.run(2025, 2025, "local") == []
    assert len(calls) == 1


def test_no_data_response_is_a_resume_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "test-key")
    monkeypatch.setattr(dividends, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        dividends.financials,
        "ensure_corp_code_xml",
        lambda _base: [("00126380", "005930")],
    )
    _financial_file(tmp_path)
    payload = {"status": "013", "message": "조회된 데이타가 없습니다."}
    raw = dividends.RawResponse(
        body=json.dumps(payload).encode(),
        payload=payload,
        status_code=200,
        content_type="application/json",
        received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(dividends, "_fetch", lambda _candidate: raw)
    monkeypatch.setattr(dividends.time, "sleep", lambda _seconds: None)

    dividends.run(2025, 2025, "local")

    assert (
        tmp_path / "dividends/dart/alot-matter/year=2025/report=11011"
        / "corp=005930/rcept=no-data/manifest.json"
    ).exists()
