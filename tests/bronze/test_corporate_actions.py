import json
import traceback
import zipfile
from io import BytesIO

import pytest
import requests

from pipeline.bronze import corporate_actions, financials


def _zip_bytes() -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("document.xml", "<document />")
    return output.getvalue()


def _failed_response(secret: str, status_code: int = 500) -> requests.Response:
    request = requests.Request(
        "GET",
        "https://opendart.fss.or.kr/api/test.json",
        params={"crtfc_key": secret},
    ).prepare()
    response = requests.Response()
    response.request = request
    response.url = request.url
    response.status_code = status_code
    response.reason = "sentinel server error"
    response._content = b"{}"
    return response


def _assert_secret_free_exception(exc_info, secret: str) -> None:
    rendered = (
        str(exc_info.value),
        repr(exc_info.value),
        "".join(
            traceback.format_exception(
                exc_info.type,
                exc_info.value,
                exc_info.tb,
            )
        ),
    )
    assert all(secret not in value for value in rendered)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    current = exc_info.value.__traceback__
    while current is not None:
        if "/pipeline/bronze/" in current.tb_frame.f_code.co_filename:
            assert secret not in repr(current.tb_frame.f_locals)
        current = current.tb_next


@pytest.mark.parametrize("failure_kind", ["http_500", "timeout"])
@pytest.mark.parametrize("fetch_kind", ["json", "document", "corp_code"])
def test_dart_request_failures_never_expose_api_key(
    failure_kind,
    fetch_kind,
    monkeypatch,
):
    secret = "DART_SECRET_SENTINEL_DO_NOT_LEAK"
    monkeypatch.setenv("DART_API_KEY", secret)

    def fail_request(*_args, **_kwargs):
        if failure_kind == "timeout":
            raise requests.Timeout(
                "timeout at https://opendart.fss.or.kr/api/test.json"
                f"?crtfc_key={secret}"
            )
        return _failed_response(secret)

    if fetch_kind == "corp_code":
        monkeypatch.setattr(financials.requests, "get", fail_request)
        call = financials._download_corp_code_xml
        expected = financials.CorpCodeDownloadError
    else:
        monkeypatch.setattr(corporate_actions.requests, "get", fail_request)
        if fetch_kind == "json":
            call = lambda: corporate_actions._fetch_json(
                f"{corporate_actions.LIST_URL}?crtfc_key={secret}",
                {"crtfc_key": secret},
                tries=1,
            )
        else:
            call = lambda: corporate_actions._fetch_document(
                "20260101000001",
                tries=1,
            )
        expected = corporate_actions.DartApiError

    with pytest.raises(expected) as exc_info:
        call()

    _assert_secret_free_exception(exc_info, secret)
    message = str(exc_info.value)
    assert "Timeout" in message if failure_kind == "timeout" else "500" in message


def test_event_title_classification():
    assert (
        corporate_actions._event_api_for_title(
            "[기재정정] 주요사항보고서(유무상증자결정)"
        ).slug
        == "combined_offering"
    )
    assert (
        corporate_actions._event_api_for_title("회사분할합병 결정").slug
        == "split_merger"
    )
    assert corporate_actions._needs_document("변경상장(액면분할)")
    assert not corporate_actions._needs_document("상장폐지에 따른 정리매매")
    assert corporate_actions._is_relevant_disclosure(
        "상장폐지에 따른 정리매매"
    )
    assert corporate_actions._is_relevant_disclosure("현금ㆍ현물배당결정")
    assert corporate_actions._needs_document("현금ㆍ현물배당결정")
    assert corporate_actions._needs_document("권배락(무상증자 및 배당)")
    assert corporate_actions._needs_document("[기재정정]주요사항보고서(무상증자결정)")
    assert not corporate_actions._needs_document("분기보고서")


def test_month_windows_cover_requested_range():
    windows = list(corporate_actions._month_windows("20260120", "20260302"))
    assert [(start.isoformat(), end.isoformat()) for start, end in windows] == [
        ("2026-01-20", "2026-01-31"),
        ("2026-02-01", "2026-02-28"),
        ("2026-03-01", "2026-03-02"),
    ]


def test_collector_base_override_is_absolute_local_only(tmp_path, monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "test-key")

    with pytest.raises(ValueError, match="absolute local"):
        corporate_actions.run(
            "20260101", "20260102", "local", base_override="relative/root",
        )
    with pytest.raises(ValueError, match="mutually exclusive"):
        corporate_actions.run(
            "20260101", "20260102", "s3", base_override=str(tmp_path),
        )

    args = corporate_actions.parse_args([
        "--from", "20260101", "--to", "20260102", "--base", str(tmp_path),
    ])
    assert args.base == str(tmp_path)


def test_local_writer_never_exposes_partial_manifest_on_replace_failure(
    tmp_path, monkeypatch,
):
    writer = corporate_actions._BronzeWriter(str(tmp_path))
    target = tmp_path / "manifests/disclosures_v3.json"
    target.parent.mkdir(parents=True)
    target.write_text('{"status":"OLD"}', encoding="utf-8")

    monkeypatch.setattr(
        corporate_actions.os,
        "replace",
        lambda *_: (_ for _ in ()).throw(OSError("simulated interruption")),
    )
    with pytest.raises(OSError, match="simulated interruption"):
        writer.save_json({"status": "COMPLETE"}, str(target))

    assert target.read_text(encoding="utf-8") == '{"status":"OLD"}'
    assert not list(target.parent.glob(f".{target.name}.*.tmp"))


def test_local_writer_resumes_after_orphaned_temporary_file(tmp_path):
    writer = corporate_actions._BronzeWriter(str(tmp_path))
    target = tmp_path / "documents/rcept=20260101000001.zip"
    target.parent.mkdir(parents=True)
    orphan = target.parent / f".{target.name}.interrupted.tmp"
    orphan.write_bytes(b"partial")
    payload = _zip_bytes()

    assert writer.save_bytes(payload, str(target)) is True
    assert target.read_bytes() == payload
    assert zipfile.is_zipfile(target)


def test_dependency_paths_selects_structured_and_documents_in_window():
    writer = object.__new__(corporate_actions._BronzeWriter)
    writer._executor = object()
    writer._existing = {
        "s3://bucket/corporate_actions/dart/structured/event=bonus_issue/"
        "year=2026/corp=002070/rcept=20260715000358.json",
        "s3://bucket/corporate_actions/dart/documents/year=2026/"
        "corp=008830/rcept=20260731901116.zip",
        "s3://bucket/corporate_actions/dart/structured/event=bonus_issue/"
        "year=2025/corp=000001/rcept=20250101000001.json",
        "s3://bucket/corporate_actions/dart/disclosures/year=2026/"
        "date=2026-07-31/corp=008830/rcept=20260731901116.json",
    }

    selected = writer.dependency_paths("20260202", "20260731")

    assert len(selected) == 2
    assert all("2026" in path for path in selected)


def test_run_saves_json_rows_and_binary_document(tmp_path, monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "test-key")
    monkeypatch.setattr(
        corporate_actions,
        "base_uri",
        lambda _dest: str(tmp_path),
    )
    monkeypatch.setattr(
        corporate_actions.financials,
        "ensure_corp_code_xml",
        lambda _base: [("00126380", "005930")],
    )

    disclosure_rows = [
        {
            "corp_code": "00126380",
            "stock_code": "005930",
            "report_nm": "주요사항보고서(무상증자결정)",
            "rcept_no": "20260102000001",
            "rcept_dt": "20260102",
        },
        {
            "corp_code": "00126380",
            "stock_code": "005930",
            "report_nm": "변경상장(액면분할)",
            "rcept_no": "20260103000002",
            "rcept_dt": "20260103",
        },
    ]
    structured_row = {
        "corp_code": "00126380",
        "rcept_no": "20260102000001",
        "nstk_asstd": "20260120",
    }

    structured_params = []

    def fake_fetch_json(url, params):
        if url.endswith("/list.json"):
            return {
                "status": "000",
                "total_page": 1,
                "list": disclosure_rows,
            }
        if url.endswith("/fricDecsn.json"):
            structured_params.append(params)
            return {"status": "000", "list": [structured_row]}
        raise AssertionError(url)

    monkeypatch.setattr(corporate_actions, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(
        corporate_actions,
        "_fetch_document",
        lambda _rcept_no: _zip_bytes(),
    )
    monkeypatch.setattr(corporate_actions.time, "sleep", lambda _seconds: None)

    first_sink: list[str] = []
    changed = corporate_actions.run(
        "20260101",
        "20260131",
        "local",
        changed_sink=first_sink,
    )

    disclosure_path = (
        tmp_path
        / "corporate_actions/dart/disclosures/year=2026/date=2026-01-02"
        / "corp=005930/rcept=20260102000001.json"
    )
    structured_path = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260102000001.json"
    )
    document_path = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260103000002.zip"
    )
    bonus_document_path = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260102000001.zip"
    )
    assert json.loads(disclosure_path.read_text()) == disclosure_rows[0]
    assert json.loads(structured_path.read_text()) == structured_row
    assert zipfile.is_zipfile(document_path)
    assert zipfile.is_zipfile(bonus_document_path)
    marker_root = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260101/to=20260131"
    )
    assert (marker_root / "documents_complete_v5.json").is_file()
    assert not (marker_root / "documents_complete_v4.json").exists()
    assert structured_params[0]["bgn_de"] == "20150101"
    assert structured_params[0]["end_de"] == "20260131"
    assert len(changed) == 8
    # changed_sink mirrors the genuine new/changed writes daily_full uses to
    # decide whether a market-closed day still has corporate-action work.
    assert sorted(set(first_sink)) == changed

    second_sink: list[str] = []
    second_changed = corporate_actions.run(
        "20260101",
        "20260131",
        "local",
        changed_sink=second_sink,
    )
    assert second_changed == []
    assert second_sink == []

    dependencies = corporate_actions.run(
        "20260101",
        "20260131",
        "local",
        include_dependencies=True,
    )
    assert str(structured_path) in dependencies
    assert str(document_path) in dependencies
    assert str(bonus_document_path) in dependencies
