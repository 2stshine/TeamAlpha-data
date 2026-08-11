import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.bronze import dart_viewer_corrections as viewer
from pipeline.bronze.dart_viewer_corrections import (
    _parse_main_page,
    _parse_viewer_economic_body,
    _pending_terminal_dependencies,
    _validate_terminal_families,
    required_viewer_receipts,
    ViewerReceiptEvidence,
)


def _dart_fixture(name: str) -> Path:
    return Path(__file__).parents[1] / "fixtures" / "dart" / name


def _write_disclosures(
    root, rows, *, start="20150101", end="20150131",
):
    interval = (
        root / "corporate_actions" / "dart" / "manifests"
        / f"from={start}" / f"to={end}"
    )
    interval.mkdir(parents=True, exist_ok=True)
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    structured_queries = {
        (event_api.slug, str(row.get("corp_code") or ""))
        for row in rows
        if str(row.get("stock_code") or "").strip()
        and str(row.get("corp_code") or "")
        and (event_api := viewer._event_api_for_title(row.get("report_nm")))
        is not None
    }
    document_candidates = {
        str(row.get("rcept_no") or "")
        for row in rows
        if viewer._needs_document(row.get("report_nm"))
        and str(row.get("rcept_no") or "")
    }
    marker = {"status": "COMPLETE", "fromdate": start, "todate": end}
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": len(structured_queries)}),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({**marker, "candidate_count": len(document_candidates)}),
        encoding="utf-8",
    )


def test_main_page_uses_family_select_not_attachment_receipts():
    payload = b"""
    <script>
      viewDoc("20150102900228", "4435128", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150102900228" selected>current</option>
      <option value="rcpNo=20141215900118">original</option>
    </select>
    <select id="att">
      <option value="rcpNo=19990101000001&amp;dcmNo=1">attachment</option>
    </select>
    """

    dcm, previous, root, family, economic_receipt, official_order = _parse_main_page(
        "20150102900228", payload,
    )

    assert dcm == "4435128"
    assert previous == "20141215900118"
    assert root == "20141215900118"
    assert family == ("20141215900118", "20150102900228")
    assert economic_receipt == "20150102900228"
    assert official_order == ("20150102900228", "20141215900118")


def test_attachment_current_may_be_absent_from_main_family():
    payload = b"""
    <script>
      viewDoc("20150205900125", "4500001", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150204900001">original</option>
    </select>
    <select id="att">
      <option value="rcpNo=20150205900125&amp;dcmNo=4500001" selected>
        [attachment correction]
      </option>
    </select>
    """

    result = _parse_main_page(
        "20150205900125", payload, attachment_correction=True,
    )

    assert result == (
        "4500001",
        "20150204900001",
        "20150204900001",
        ("20150204900001", "20150205900125"),
        "20150204900001",
        ("20150204900001",),
    )


def test_live_cash_attachment_main_has_exact_family_att_and_viewer_target():
    attachment = viewer.parse_official_dart_main_page(
        "20210223800413",
        _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        expected_attachment_only=True,
    )
    economic = viewer.parse_official_dart_main_page(
        "20210223800278",
        _dart_fixture("20210223800278-cash-family-main.html").read_bytes(),
        expected_attachment_only=False,
    )

    assert attachment.current_selector == "ATTACHMENT"
    assert attachment.family_receipts == ("20210223800278",)
    assert attachment.dcm_no == "7821388"
    assert attachment.dtd == "HTML"
    assert attachment.attachment_keys == (
        "20210223800564:7822175",
        "20210223800413:7821388",
        "20210223800278:7820625",
    )
    assert economic.current_selector == "FAMILY"
    assert economic.dcm_no == "7820624"
    assert economic.family_receipts == attachment.family_receipts
    assert economic.attachment_keys == attachment.attachment_keys


def test_fetch_one_cash_attachment_uses_exact_selector_dtd_and_terminal(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    fixtures = {
        ("main.do", source): _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        ("main.do", economic): _dart_fixture(
            "20210223800278-cash-family-main.html"
        ).read_bytes(),
        ("viewer.do", source): _dart_fixture(
            "20210223800413-cash-attachment-viewer.html"
        ).read_bytes(),
        ("viewer.do", economic): _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes(),
    }
    calls: list[str] = []

    def fetch(url, **_kwargs):
        calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        endpoint = parsed.path.rsplit("/", 1)[-1]
        if endpoint == "viewer.do":
            assert query["dtd"] == ["HTML"]
        return fixtures[(endpoint, receipt)]

    monkeypatch.setattr(viewer, "_get", fetch)
    # A legacy hardcoded-HTML cache must never satisfy the v2 DTD-bound path.
    legacy = viewer._receipt_main_path(tmp_path, source).with_name("viewer.html")
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"<html><body><table>stale</table></body></html>")
    legacy.with_name("economic_main.html").write_bytes(b"stale heuristic main")

    evidence = viewer._fetch_one(
        tmp_path,
        source,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="[첨부정정]현금ㆍ현물배당결정",
        report_names={
            source: "[첨부정정]현금ㆍ현물배당결정",
            economic: "현금ㆍ현물배당결정",
        },
    )

    assert evidence.correction_of_receipt_no == economic
    assert evidence.revision_root_receipt_no == economic
    assert evidence.current_selector == "ATTACHMENT"
    assert evidence.dtd == "HTML"
    assert evidence.economic_body_receipt_no == economic
    assert evidence.economic_body_dcm_no == "7820624"
    assert evidence.economic_body_dtd == "HTML"
    assert evidence.economic_classification == "ECONOMIC_DECISION"
    assert evidence.common_cash_amount == 50.0
    assert evidence.record_date == "2020-12-31"
    assert evidence.viewer_path.endswith("viewer.dtd=HTML.html")
    assert evidence.economic_viewer_path.endswith(
        "economic_viewer.dtd=HTML.html"
    )
    assert len(calls) == 4


def test_cash_attachment_and_economic_selector_mismatch_fails_closed(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    source_main = _dart_fixture(
        "20210223800413-cash-attachment-main.html"
    ).read_bytes()
    economic_main = _dart_fixture(
        "20210223800278-cash-family-main.html"
    ).read_text(encoding="utf-8").replace(
        "20210223800564&amp;dcmNo=7822175",
        "20210223800564&amp;dcmNo=9999999",
    ).encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return source_main if receipt == source else economic_main
        if receipt == source:
            return _dart_fixture(
                "20210223800413-cash-attachment-viewer.html"
            ).read_bytes()
        return _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes()

    monkeypatch.setattr(viewer, "_get", fetch)
    with pytest.raises(RuntimeError, match="selectors disagree"):
        viewer._fetch_one(
            tmp_path,
            source,
            tries=1,
            timeout=1,
            rate_limiter=viewer._RateLimiter(1.0, 0.0),
            report_name="[첨부정정]현금ㆍ현물배당결정",
            report_names={
                source: "[첨부정정]현금ㆍ현물배당결정",
                economic: "현금ㆍ현물배당결정",
            },
        )


def test_cash_attachment_manifest_roundtrip_and_body_corruption(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    _write_disclosures(tmp_path, [
        {
            "rcept_no": source,
            "rcept_dt": "20210223",
            "stock_code": "005930",
            "report_nm": "[첨부정정]현금ㆍ현물배당결정",
        },
        {
            "rcept_no": economic,
            "rcept_dt": "20210223",
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        },
    ])
    fixtures = {
        ("main.do", source): _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        ("main.do", economic): _dart_fixture(
            "20210223800278-cash-family-main.html"
        ).read_bytes(),
        ("viewer.do", source): _dart_fixture(
            "20210223800413-cash-attachment-viewer.html"
        ).read_bytes(),
        ("viewer.do", economic): _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes(),
    }

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        return fixtures[(parsed.path.rsplit("/", 1)[-1], query["rcpNo"][0])]

    monkeypatch.setattr(viewer, "_get", fetch)
    verified = viewer.collect_viewer_corrections(
        str(tmp_path),
        apply=True,
        workers=1,
        request_interval_seconds=0.001,
    )

    assert verified.receipt_count == 1
    evidence = verified.receipts[0]
    assert evidence.receipt_no == source
    assert evidence.correction_of_receipt_no == economic
    assert evidence.economic_body_receipt_no == economic
    assert viewer.verify_viewer_corrections(str(tmp_path)) == verified

    economic_viewer = tmp_path / evidence.economic_viewer_path
    original = economic_viewer.read_bytes()
    economic_viewer.write_bytes(original + b"corruption")
    with pytest.raises(RuntimeError, match="SHA/content length mismatch"):
        viewer.verify_viewer_corrections(str(tmp_path))
    economic_viewer.write_bytes(original)
    assert viewer.verify_viewer_corrections(str(tmp_path)) == verified

    manifest = tmp_path / viewer.MANIFEST_RELATIVE_PATH
    canonical = manifest.read_bytes()
    manifest.write_text(
        json.dumps(json.loads(canonical), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="manifest is not canonical"):
        viewer.verify_viewer_corrections(str(tmp_path))
    manifest.write_bytes(canonical)
    assert viewer.verify_viewer_corrections(str(tmp_path)) == verified


def test_cash_attachment_does_not_revive_withdrawn_economic_terminal(
    tmp_path, monkeypatch,
):
    source = "20211224000003"
    withdrawal = "20211223000002"
    original = "20211222000001"
    source_main = f"""
      <script>viewDoc("{source}", "3", "0", "0", "0", "HTML", "");</script>
      <select id="family">
        <option value="rcpNo={withdrawal}">withdrawal</option>
        <option value="rcpNo={original}">original</option>
      </select>
      <select id="att">
        <option value="rcpNo={source}&amp;dcmNo=3" selected>attachment</option>
      </select>
    """.encode()
    terminal_main = f"""
      <script>viewDoc("{withdrawal}", "2", "0", "0", "0", "HTML", "");</script>
      <select id="family">
        <option value="rcpNo={withdrawal}" selected>withdrawal</option>
        <option value="rcpNo={original}">original</option>
      </select>
      <select id="att">
        <option value="rcpNo={source}&amp;dcmNo=3">attachment</option>
      </select>
    """.encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        receipt = parse_qs(parsed.query)["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return source_main if receipt == source else terminal_main
        if receipt == source:
            return b"<html><body><table><tr><td>attachment</td></tr></table></body></html>"
        # Even a stale-looking amount in the terminal body cannot revive a
        # withdrawal because classification is bound to its disclosure title.
        return _economic_viewer(amount="999", record_date="2021-12-31")

    monkeypatch.setattr(viewer, "_get", fetch)
    evidence = viewer._fetch_one(
        tmp_path,
        source,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="[첨부정정]현금ㆍ현물배당결정",
        report_names={
            source: "[첨부정정]현금ㆍ현물배당결정",
            withdrawal: "[철회]현금ㆍ현물배당결정",
            original: "현금ㆍ현물배당결정",
        },
    )

    assert evidence.economic_body_receipt_no == withdrawal
    assert evidence.economic_classification == "NO_ECONOMIC_EVENT"
    assert evidence.common_cash_amount is None
    assert evidence.record_date is None


def test_non_attachment_current_must_be_in_main_family():
    payload = b"""
    <script>
      viewDoc("20150205900125", "4500001", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150204900001">original</option>
    </select>
    <select id="att"></select>
    """

    with pytest.raises(RuntimeError, match="current selection is missing"):
        _parse_main_page("20150205900125", payload)


def _economic_viewer(amount="300", record_date="2022-06-30"):
    return f"""
    <html><body><table>
      <tr><td>3. 1주당 배당금(원)</td><td>보통주식</td><td>{amount}</td></tr>
      <tr><td>6. 배당기준일</td><td>{record_date}</td></tr>
    </table></body></html>
    """.encode()


def test_viewer_economic_body_requires_common_dps_and_record_date():
    assert _parse_viewer_economic_body(
        _economic_viewer(), report_name="현금ㆍ현물배당결정",
    ) == ("ECONOMIC_DECISION", 300.0, "2022-06-30")


@pytest.mark.parametrize(
    "rendered",
    ["2025년 3월 10일", "2025-3-10"],
)
def test_viewer_economic_body_normalizes_non_padded_record_date(rendered):
    assert _parse_viewer_economic_body(
        _economic_viewer(record_date=rendered),
        report_name="현금ㆍ현물배당결정",
    ) == ("ECONOMIC_DECISION", 300.0, "2025-03-10")


def test_viewer_economic_body_rejects_invalid_calendar_date():
    with pytest.raises(RuntimeError, match="record date is invalid"):
        _parse_viewer_economic_body(
            _economic_viewer(record_date="2025-02-30"),
            report_name="현금ㆍ현물배당결정",
        )


def test_intermediate_positive_without_record_date_is_preserved_pending():
    assert _parse_viewer_economic_body(
        _economic_viewer(record_date="-"),
        report_name="[기재정정]현금ㆍ현물배당결정",
    ) == ("POSITIVE_PENDING_RECORD_DATE", 300.0, None)


def test_mutable_overlap_uses_latest_explicit_coverage_end(tmp_path):
    for start, end, marker in (
        ("20150101", "20150131", "유"),
        ("20150115", "20150201", "유정"),
    ):
        _write_disclosures(
            tmp_path,
            [{
                "rcept_no": "20150102900228",
                "stock_code": "005930",
                "report_nm": "현금ㆍ현물배당결정",
                "rm": marker,
            }],
            start=start,
            end=end,
        )

    assert viewer._cash_disclosures(tmp_path)["20150102900228"]["rm"] == "유정"


def test_mutable_overlap_same_coverage_end_fails_closed(tmp_path):
    for start, marker in (("20150101", "유"), ("20150115", "유정")):
        _write_disclosures(
            tmp_path,
            [{
                "rcept_no": "20150102900228",
                "stock_code": "005930",
                "report_nm": "현금ㆍ현물배당결정",
                "rm": marker,
            }],
            start=start,
            end="20150131",
        )
    with pytest.raises(RuntimeError, match="same latest coverage end"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_rejects_legacy_only_receipt(tmp_path):
    _write_disclosures(tmp_path, [])
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101" / "to=20150131"
    )
    interval.joinpath("disclosures.json").write_text(json.dumps([{
        "rcept_no": "20150102900228",
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="not authenticated by v3/v5"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_rejects_incomplete_v3_only_receipt(tmp_path):
    _write_disclosures(
        tmp_path, [], start="20150201", end="20150228",
    )
    incomplete = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101" / "to=20150131"
    )
    incomplete.mkdir(parents=True)
    incomplete.joinpath("disclosures_v3.json").write_text(json.dumps([{
        "rcept_no": "20150102900228",
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="incomplete v5 intervals"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_rejects_v5_candidate_count_drift(tmp_path):
    receipt = "20150102900228"
    _write_disclosures(tmp_path, [{
        "rcept_no": receipt,
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }])
    marker = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20150131/documents_complete_v5.json"
    )
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["candidate_count"] = 0
    marker.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="interval/count mismatch"):
        viewer._cash_disclosures(tmp_path)


def _evidence(receipt, classification, *, family, amount=150.0, record=None):
    return ViewerReceiptEvidence(
        receipt_no=receipt,
        dcm_no="1",
        dtd="HTML",
        current_selector="FAMILY",
        attachment_keys=(),
        correction_of_receipt_no=(family[-2] if len(family) > 1 else None),
        revision_root_receipt_no=family[0],
        family_receipt_nos=tuple(family),
        official_family_order=tuple(reversed(family)),
        revision_kind="ECONOMIC_REVISION",
        economic_body_receipt_no=receipt,
        economic_body_dcm_no="1",
        economic_body_dtd="HTML",
        economic_main_path="main.html",
        economic_main_content_length=1,
        economic_main_sha256="a" * 64,
        economic_classification=classification,
        common_cash_amount=amount,
        record_date=record,
        main_path="main.html",
        main_content_length=1,
        main_sha256="a" * 64,
        viewer_path="viewer.html",
        viewer_content_length=1,
        viewer_sha256="b" * 64,
        economic_viewer_path="viewer.html",
        economic_viewer_content_length=1,
        economic_viewer_sha256="b" * 64,
    )


def test_official_family_allows_incomplete_intermediate_when_terminal_complete():
    family = ("20250228801790", "20250304800639")
    rows = [
        _evidence(
            family[0], "POSITIVE_PENDING_RECORD_DATE", family=family,
        ),
        _evidence(
            family[1], "ECONOMIC_DECISION", family=family,
            record="2025-03-19",
        ),
    ]
    disclosures = {
        receipt: {"report_nm": "[기재정정]현금ㆍ현물배당결정"}
        for receipt in family
    }

    _validate_terminal_families(rows, disclosures)


def test_pending_family_adds_plain_terminal_as_dynamic_dependency():
    family = (
        "20250228800605", "20250228801700", "20250228801790",
        "20250304800639",
    )
    pending = _evidence(
        "20250228801790",
        "POSITIVE_PENDING_RECORD_DATE",
        family=family,
    )
    disclosures = {
        receipt: {
            "report_nm": (
                "현금ㆍ현물배당결정"
                if receipt == "20250304800639"
                else "[기재정정]현금ㆍ현물배당결정"
            )
        }
        for receipt in family
    }

    assert _pending_terminal_dependencies([pending], disclosures) == (
        "20250304800639",
    )


def test_official_family_rejects_incomplete_terminal():
    receipt = "20250228801790"
    row = _evidence(
        receipt, "POSITIVE_PENDING_RECORD_DATE", family=(receipt,),
    )

    with pytest.raises(RuntimeError, match="terminal economic revision is incomplete"):
        _validate_terminal_families(
            [row], {receipt: {"report_nm": "[기재정정]현금ㆍ현물배당결정"}},
        )


def test_fetch_one_reuses_verified_partial_files_without_network(
    tmp_path, monkeypatch,
):
    receipt = "20220802900375"
    directory = (
        tmp_path / "corporate_actions" / "dart" / "viewer_corrections"
        / f"receipt={receipt}"
    )
    directory.mkdir(parents=True)
    directory.joinpath("main.html").write_bytes(f"""
    <script>
      viewDoc("{receipt}", "999", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo={receipt}" selected>current</option>
    </select>
    <select id="att"></select>
    """.encode())
    directory.joinpath("viewer.dtd=HTML.html").write_bytes(_economic_viewer())
    monkeypatch.setattr(
        viewer, "_get",
        lambda *args, **kwargs: pytest.fail("resume unexpectedly refetched"),
    )

    evidence = viewer._fetch_one(
        tmp_path,
        receipt,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="현금ㆍ현물배당결정",
    )

    assert evidence.economic_classification == "ECONOMIC_DECISION"
    assert evidence.common_cash_amount == 300.0
    assert evidence.record_date == "2022-06-30"


def test_required_receipts_include_all_issuer_corrections_but_not_subsidiary(
    tmp_path,
):
    _write_disclosures(tmp_path, [
        {
            "rcept_no": "20240102800001",
            "report_nm": "[기재정정]현금ㆍ현물배당결정",
        },
        {
            "rcept_no": "20240102800002",
            "report_nm": "[기재정정]현금ㆍ현물배당결정(자회사의 주요경영사항)",
        },
        {
            "rcept_no": "20240102800003",
            "report_nm": "현금ㆍ현물배당결정",
        },
    ])

    assert required_viewer_receipts(str(tmp_path)) == ("20240102800001",)


def test_unavailable_body_must_really_be_status_014(tmp_path):
    receipt = "20240102800001"
    _write_disclosures(tmp_path, [{
        "rcept_no": receipt,
        "report_nm": "[첨부정정]현금ㆍ현물배당결정",
    }])
    unavailable = (
        tmp_path / "corporate_actions" / "dart" / "documents_unavailable"
        / "year=2024" / "corp=000001" / f"rcept={receipt}.xml"
    )
    unavailable.parent.mkdir(parents=True)
    unavailable.write_text("<result><status>999</status></result>")

    with pytest.raises(RuntimeError, match="not status 014"):
        required_viewer_receipts(str(tmp_path))
