import json
import zipfile
from io import BytesIO

from pipeline.bronze import corporate_actions


def _zip_bytes() -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("document.xml", "<document />")
    return output.getvalue()


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
    assert not corporate_actions._needs_document("분기보고서")


def test_month_windows_cover_requested_range():
    windows = list(corporate_actions._month_windows("20260120", "20260302"))
    assert [(start.isoformat(), end.isoformat()) for start, end in windows] == [
        ("2026-01-20", "2026-01-31"),
        ("2026-02-01", "2026-02-28"),
        ("2026-03-01", "2026-03-02"),
    ]


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
    assert json.loads(disclosure_path.read_text()) == disclosure_rows[0]
    assert json.loads(structured_path.read_text()) == structured_row
    assert zipfile.is_zipfile(document_path)
    assert structured_params[0]["bgn_de"] == "20150101"
    assert structured_params[0]["end_de"] == "20260131"
    assert len(changed) == 7
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
