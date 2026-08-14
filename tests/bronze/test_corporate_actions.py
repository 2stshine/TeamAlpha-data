import json
import traceback
import zipfile
from io import BytesIO

import pytest
import requests
from botocore.exceptions import ClientError

from pipeline.bronze import corporate_actions, financials


def test_bronze_writer_calls_fail_closed_boundary_before_local_publication(
    tmp_path,
):
    target = tmp_path / "corporate_actions/dart/new.json"
    observations: list[str] = []

    def before_change(path: str) -> None:
        assert path == str(target)
        assert not target.exists()
        observations.append(path)

    writer = corporate_actions._BronzeWriter(
        str(tmp_path), before_change=before_change,
    )

    assert writer.save_json({"new": True}, str(target)) is True
    assert writer.save_json({"new": True}, str(target)) is False
    assert observations == [str(target)]


def test_bronze_writer_does_not_publish_when_fail_closed_boundary_fails(
    tmp_path,
):
    target = tmp_path / "corporate_actions/dart/new.json"
    writer = corporate_actions._BronzeWriter(
        str(tmp_path),
        before_change=lambda _path: (_ for _ in ()).throw(
            RuntimeError("contract invalidation failed")
        ),
    )

    with pytest.raises(RuntimeError, match="contract invalidation failed"):
        writer.save_json({"new": True}, str(target))
    assert not target.exists()


def test_s3_writer_uses_create_only_and_accepts_identical_concurrent_body():
    payload = b"immutable action body"

    class FakeS3:
        def put_object(self, **kwargs):
            assert kwargs["IfNoneMatch"] == "*"
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed"}}, "PutObject",
            )

        def get_object(self, **kwargs):
            return {"Body": BytesIO(payload)}

    writer = object.__new__(corporate_actions._BronzeWriter)
    writer._s3 = FakeS3()
    writer._bucket = "bronze"

    writer._put_s3(
        "s3://bronze/corporate_actions/dart/action.json", payload,
    )


def test_s3_writer_rejects_different_concurrent_body():
    class FakeS3:
        def put_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed"}}, "PutObject",
            )

        def get_object(self, **kwargs):
            return {"Body": BytesIO(b"different")}

    writer = object.__new__(corporate_actions._BronzeWriter)
    writer._s3 = FakeS3()
    writer._bucket = "bronze"

    with pytest.raises(RuntimeError, match="different bytes"):
        writer._put_s3(
            "s3://bronze/corporate_actions/dart/action.json", b"candidate",
        )


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


def test_completion_candidates_share_exact_producer_listing_resolution():
    rows = [{
        "rcept_no": "20250101000001",
        "corp_code": "listed-fallback",
        "stock_code": "",
        "report_nm": "주요사항보고서(무상증자결정)",
    }, {
        "rcept_no": "20250101000002",
        "corp_code": "unlisted",
        "stock_code": "",
        "report_nm": "주요사항보고서(무상증자결정)",
    }, {
        "rcept_no": "20250101000003",
        "corp_code": "direct",
        "stock_code": "000660",
        "report_nm": "주요사항보고서(무상증자결정)",
    }]
    corp_to_stock = {"listed-fallback": "005930"}

    assert corporate_actions._document_candidate_receipts(
        rows, corp_to_stock,
    ) == {"20250101000001", "20250101000003"}
    assert corporate_actions._structured_query_keys(
        rows, corp_to_stock,
    ) == {
        ("bonus_issue", "listed-fallback"),
        ("bonus_issue", "direct"),
    }


def test_month_windows_cover_requested_range():
    windows = list(corporate_actions._month_windows("20260120", "20260302"))
    assert [(start.isoformat(), end.isoformat()) for start, end in windows] == [
        ("2026-01-20", "2026-01-31"),
        ("2026-02-01", "2026-02-28"),
        ("2026-03-01", "2026-03-02"),
    ]


def test_structured_query_uses_extended_connection_retry_budget(monkeypatch):
    captured = {}

    def fake_fetch(url, params, *, tries=4):
        captured.update(url=url, params=params, tries=tries)
        return {"status": "013", "list": []}

    monkeypatch.setattr(corporate_actions, "_fetch_json", fake_fetch)
    monkeypatch.setattr(corporate_actions.time, "sleep", lambda _seconds: None)
    event_api = corporate_actions.EVENT_APIS[1]

    corp_code, returned_api, payload = corporate_actions._fetch_structured(
        "secret", "20260810", "00126380", event_api,
    )

    assert (corp_code, returned_api, payload) == (
        "00126380", event_api, {"status": "013", "list": []},
    )
    assert captured["tries"] == 8
    assert captured["params"]["bgn_de"] == "20150101"
    assert captured["params"]["end_de"] == "20260810"


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


def test_direct_s3_action_publication_is_disabled_before_any_source_access(
    monkeypatch,
):
    monkeypatch.setenv("DART_API_KEY", "test-key")
    monkeypatch.setattr(
        corporate_actions.financials,
        "ensure_corp_code_xml",
        lambda _base: pytest.fail("unsafe source access reached"),
    )

    with pytest.raises(RuntimeError, match="direct S3 corporate-action"):
        corporate_actions.run("20260101", "20260102", "s3")


def test_s3_action_publication_requires_fail_closed_callback(monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "test-key")
    monkeypatch.setattr(
        corporate_actions,
        "base_uri",
        lambda _dest: pytest.fail("guard should run before base resolution"),
    )

    with pytest.raises(RuntimeError, match="pipeline.daily_full"):
        corporate_actions.run("20260101", "20260102", "s3")


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
    structured_tries = []

    def fake_fetch_json(url, params, **kwargs):
        if url.endswith("/list.json"):
            assert kwargs == {}
            return {
                "status": "000",
                "total_page": 1,
                "list": disclosure_rows,
            }
        if url.endswith("/fricDecsn.json"):
            structured_params.append(params)
            structured_tries.append(kwargs.get("tries"))
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
    assert structured_tries == [8]
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


def test_structured_failure_cancels_pending_queries_without_waiting(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("DART_API_KEY", "test-key")
    monkeypatch.setattr(
        corporate_actions,
        "base_uri",
        lambda _dest: str(tmp_path),
    )
    monkeypatch.setattr(
        corporate_actions.financials,
        "ensure_corp_code_xml",
        lambda _base: [
            ("00126380", "005930"),
            ("00164779", "000660"),
        ],
    )
    discovery_manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260101/to=20260131/disclosures_v3.json"
    )
    discovery_manifest.parent.mkdir(parents=True)
    disclosure_rows = [
        {
            "corp_code": corp_code,
            "stock_code": ticker,
            "report_nm": "주요사항보고서(무상증자결정)",
            "rcept_no": f"2026010200000{index}",
            "rcept_dt": "20260102",
        }
        for index, (corp_code, ticker) in enumerate(
            [("00126380", "005930"), ("00164779", "000660")],
            start=1,
        )
    ]
    discovery_manifest.write_text(
        json.dumps(disclosure_rows, ensure_ascii=False),
        encoding="utf-8",
    )

    def fake_fetch_json(url, _params, **_kwargs):
        if url.endswith("/list.json"):
            return {
                "status": "000",
                "total_page": 1,
                "list": disclosure_rows,
            }
        raise AssertionError("structured work should use the fake executor")

    class FakeFuture:
        def __init__(self, *, fail=False):
            self.fail = fail
            self.cancelled = False

        def result(self):
            if self.fail:
                raise corporate_actions.DartApiError("simulated outage")
            return (
                "00164779",
                corporate_actions.EVENT_APIS[1],
                {"status": "013", "list": []},
            )

        def cancel(self):
            self.cancelled = True
            return True

    class FakeExecutor:
        instance = None

        def __init__(self, max_workers):
            assert max_workers == corporate_actions.API_WORKERS
            self.futures = []
            self.shutdown_calls = []
            FakeExecutor.instance = self

        def submit(self, *_args):
            future = FakeFuture(fail=not self.futures)
            self.futures.append(future)
            return future

        def shutdown(self, **kwargs):
            self.shutdown_calls.append(kwargs)

    monkeypatch.setattr(corporate_actions, "_fetch_json", fake_fetch_json)
    class PhaseExecutor:
        def __new__(cls, max_workers):
            return FakeExecutor(max_workers)

    monkeypatch.setattr(corporate_actions, "ThreadPoolExecutor", PhaseExecutor)
    monkeypatch.setattr(
        corporate_actions,
        "as_completed",
        lambda futures: iter(futures),
    )
    monkeypatch.setattr(corporate_actions.time, "sleep", lambda _seconds: None)

    with pytest.raises(corporate_actions.DartApiError, match="simulated"):
        corporate_actions.run(
            "20260101",
            "20260131",
            "local",
            download_documents=False,
        )

    executor = FakeExecutor.instance
    assert executor is not None
    assert executor.shutdown_calls == [
        {"wait": False, "cancel_futures": True},
    ]
    assert all(future.cancelled for future in executor.futures)
