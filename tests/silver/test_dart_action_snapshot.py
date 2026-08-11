import hashlib
import json
import zipfile
from datetime import date

import pytest

from pipeline.silver.dart_action_snapshot import (
    MANIFEST_RELATIVE_PATH,
    build_snapshot_manifest,
    verify_snapshot_manifest,
)


def _write_zip(path, *, body: str = "<document>fixture</document>"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("document.xml", body)


def _complete_interval(root, start: str, end: str):
    interval = (
        root
        / "corporate_actions"
        / "dart"
        / "manifests"
        / f"from={start}"
        / f"to={end}"
    )
    interval.mkdir(parents=True, exist_ok=True)
    (interval / "disclosures_v3.json").write_text("[]", encoding="utf-8")
    marker = {
        "status": "COMPLETE",
        "fromdate": start,
        "todate": end,
        "candidate_count": 0,
    }
    (interval / "structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": 0}), encoding="utf-8",
    )
    (interval / "documents_complete_v5.json").write_text(
        json.dumps(marker), encoding="utf-8",
    )


def test_snapshot_manifest_hashes_every_body_and_verifies_continuous_coverage(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    _complete_interval(tmp_path, "20160101", "20161231")
    body = (
        tmp_path
        / "corporate_actions"
        / "dart"
        / "disclosures"
        / "year=2016"
        / "event.json"
    )
    body.parent.mkdir(parents=True)
    body.write_bytes(b"immutable-source-body")

    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2016, 12, 31)
    )
    verified = verify_snapshot_manifest(
        str(tmp_path), required_end=date(2016, 12, 31)
    )

    assert verified == built
    payload = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())
    entry = next(item for item in payload["objects"] if item["path"].endswith("event.json"))
    assert entry["sha256"] == hashlib.sha256(b"immutable-source-body").hexdigest()
    assert verified.body_count == len(payload["objects"])


def test_snapshot_verification_fails_after_body_tamper(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    build_snapshot_manifest(str(tmp_path), coverage_end=date(2015, 12, 31))
    disclosure = next(
        (tmp_path / "corporate_actions" / "dart" / "manifests").rglob(
            "disclosures_v3.json"
        )
    )
    disclosure.write_text("[{}]", encoding="utf-8")

    with pytest.raises(RuntimeError, match="SHA/content length"):
        verify_snapshot_manifest(
            str(tmp_path), required_end=date(2015, 12, 31)
        )


def test_snapshot_build_fails_on_calendar_coverage_gap(tmp_path):
    _complete_interval(tmp_path, "20150101", "20150630")
    _complete_interval(tmp_path, "20150702", "20151231")

    with pytest.raises(RuntimeError, match="coverage gap"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_incomplete_overlap_cannot_use_corp_class_as_scope_waiver(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    overlap = (
        tmp_path
        / "corporate_actions"
        / "dart"
        / "manifests"
        / "from=20150601"
        / "to=20150630"
    )
    overlap.mkdir(parents=True)
    overlap.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": "20150601000001",
            "stock_code": "",
            "corp_cls": "E",
            "report_nm": "현금ㆍ현물배당결정",
        }]),
        encoding="utf-8",
    )
    overlap.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150601",
            "todate": "20150630",
        }),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="no receipt/ticker identity"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_snapshot_accepts_uppercase_alphanumeric_krx_ticker_identities(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    codes = ("0008Z0", "0010V0", "0039P0")
    rows = [
        {
            "rcept_no": f"2015123100000{index}",
            "rcept_dt": "20151231",
            "stock_code": ticker,
            "corp_code": f"0000000{index}",
            "corp_cls": "K",
            "report_nm": "현금ㆍ현물배당결정",
        }
        for index, ticker in enumerate(codes, start=1)
    ]
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": len(rows),
        }),
        encoding="utf-8",
    )
    for row in rows:
        body = (
            tmp_path / "corporate_actions/dart/documents/year=2015"
            / f"corp={row['stock_code']}"
            / f"rcept={row['rcept_no']}.zip"
        )
        _write_zip(body)

    snapshot = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )

    assert snapshot.coverage_end == date(2015, 12, 31)


def test_incomplete_overlap_non_total_return_receipt_does_not_block(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    overlap = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20150601/to=20150630"
    )
    overlap.mkdir(parents=True)
    overlap.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": "20150601000001",
            "stock_code": "",
            "corp_cls": "E",
            "report_nm": "유상증자결정",
        }]),
        encoding="utf-8",
    )

    verified = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )

    assert verified.coverage_end == date(2015, 12, 31)


def test_v5_marker_cannot_hide_missing_bonus_decision_document(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    receipt = "20150601000001"
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": receipt,
            "rcept_dt": "20150601",
            "stock_code": "000001",
            "corp_code": "00000001",
            "corp_cls": "Y",
            "report_nm": "[정정] 무상증자결정",
        }], ensure_ascii=False),
        encoding="utf-8",
    )
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "query_count": 1,
        }),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": 1,
        }),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="MISSING_OR_AMBIGUOUS_BODY"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_v5_document_unavailable_requires_exact_status_014(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    receipt = "20150601000001"
    row = {
        "rcept_no": receipt,
        "rcept_dt": "20150601",
        "stock_code": "000001",
        "corp_code": "00000001",
        "corp_cls": "Y",
        "report_nm": "무상증자결정",
    }
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([row], ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "query_count": 1,
        }),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": 1,
        }),
        encoding="utf-8",
    )
    unavailable = (
        tmp_path / "corporate_actions/dart/documents_unavailable/year=2015"
        / "corp=000001" / f"rcept={receipt}.xml"
    )
    unavailable.parent.mkdir(parents=True, exist_ok=True)
    unavailable.write_text(
        "<result><status>013</status></result>", encoding="utf-8",
    )

    with pytest.raises(
        RuntimeError, match="INVALID_DOCUMENT_UNAVAILABLE_STATUS",
    ):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )

    unavailable.write_text(
        "<result><status>014</status></result>", encoding="utf-8",
    )
    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )
    assert built.coverage_end == date(2015, 12, 31)
