import hashlib
import json
from datetime import date

import pytest

from pipeline.silver import reviewed_dividend_corrections as reviewed


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _reviewed_fixture(tmp_path, *, corrected_value="2015-12-31"):
    ticker = "093320"
    receipt = "20160224900227"
    evidence_receipt = "20160330000241"
    response_path = (
        tmp_path / "dividends/dart/alot-matter/year=2015/report=11011"
        / f"corp={ticker}/rcept={evidence_receipt}/response.json"
    )
    response = json.dumps({
        "status": "000",
        "list": [{
            "rcept_no": evidence_receipt,
            "corp_code": "00603348",
            "stlm_dt": "2015-12-31",
            "se": "주당 현금배당금(원)",
            "stock_knd": "보통주",
            "thstrm": "100",
        }],
    }, ensure_ascii=False, separators=(",", ":")).encode()
    response_path.parent.mkdir(parents=True)
    response_path.write_bytes(response)
    response_sha = _sha256(response)
    manifest_path = response_path.with_name("manifest.json")
    manifest = json.dumps({
        "complete": True,
        "provider": "OpenDART",
        "endpoint": "alotMatter.json",
        "dart_status": "000",
        "sha256": response_sha,
        "rcept_nos": [evidence_receipt],
        "request_params": {
            "corp_code": "00603348",
            "stock_code": ticker,
            "bsns_year": "2015",
            "reprt_code": "11011",
        },
    }, separators=(",", ":")).encode()
    manifest_path.write_bytes(manifest)
    action_path = (
        tmp_path / "corporate_actions/dart/documents/year=2016"
        / f"corp={ticker}/rcept={receipt}.zip"
    )
    action_path.parent.mkdir(parents=True)
    action_payload = b"immutable action ZIP fixture"
    action_path.write_bytes(action_payload)
    correction = {
        "correction_id": f"{ticker}-{receipt}-record-date",
        "receipt_no": receipt,
        "ticker": ticker,
        "action_zip_sha256": _sha256(action_payload),
        "field": "record_date",
        "raw_value": "None",
        "corrected_value": corrected_value,
        "expected_common_cash_amount": 100.0,
        "basis": "INDEPENDENT_OPENDART_FY_PERIODIC_RESPONSE",
        "evidence_path": response_path.relative_to(tmp_path).as_posix(),
        "evidence_sha256": response_sha,
        "evidence_manifest_path": manifest_path.relative_to(
            tmp_path
        ).as_posix(),
        "evidence_manifest_sha256": _sha256(manifest),
        "evidence_receipt_no": evidence_receipt,
        "evidence_corp_code": "00603348",
        "evidence_settlement_date": "2015-12-31",
    }
    return correction


def test_reviewed_record_date_uses_content_addressed_periodic_evidence(
    tmp_path, monkeypatch,
):
    correction = _reviewed_fixture(tmp_path)
    monkeypatch.setattr(reviewed, "CORRECTIONS", (correction,))

    updated = reviewed.apply_reviewed_correction(
        str(tmp_path),
        ticker="093320",
        receipt="20160224900227",
        details={"record_date": None, "cash_amount": 100.0},
    )

    assert updated["record_date"] == date(2015, 12, 31)
    assert updated["reviewed_economic_correction"] is True
    assert updated["reviewed_correction_id"] == correction["correction_id"]
    assert updated["reviewed_evidence_sha256"] == correction[
        "evidence_sha256"
    ]


def test_reviewed_record_date_rejects_correction_not_equal_to_settlement(
    tmp_path, monkeypatch,
):
    correction = _reviewed_fixture(
        tmp_path, corrected_value="2015-12-30",
    )
    monkeypatch.setattr(reviewed, "CORRECTIONS", (correction,))

    with pytest.raises(RuntimeError, match="corrected/settlement date"):
        reviewed.apply_reviewed_correction(
            str(tmp_path),
            ticker="093320",
            receipt="20160224900227",
            details={"record_date": None, "cash_amount": 100.0},
        )


def test_non_economic_payment_annotation_never_replaces_economic_evidence(
    tmp_path, monkeypatch,
):
    ticker = "008870"
    receipt = "20211210800570"
    action_path = (
        tmp_path / "corporate_actions/dart/documents/year=2021"
        / f"corp={ticker}/rcept={receipt}.zip"
    )
    action_path.parent.mkdir(parents=True)
    action_path.write_bytes(b"payment annotation fixture")
    correction = {
        "correction_id": f"{ticker}-{receipt}-payment-date",
        "receipt_no": receipt,
        "ticker": ticker,
        "action_zip_sha256": _sha256(action_path.read_bytes()),
        "field": "payment_date",
        "raw_value": "2021-01-10",
        "corrected_value": "2022-01-10",
        "basis": "REVIEWED_ONE_YEAR_PAYMENT_DATE_ENTRY_ERROR",
        "economic_effect": "NONE_PAYMENT_DATE_NOT_USED_BY_TOTAL_RETURN",
    }
    monkeypatch.setattr(reviewed, "CORRECTIONS", (correction,))

    updated = reviewed.apply_reviewed_correction(
        str(tmp_path), ticker=ticker, receipt=receipt,
        details={"payment_date": date(2021, 1, 10), "cash_amount": 150.0},
    )

    assert updated["payment_date"] == date(2021, 1, 10)
    assert updated["payment_date_quality_status"] == (
        "REVIEWED_RAW_ONE_YEAR_ENTRY_ERROR_NOT_USED"
    )
    assert "reviewed_evidence_sha256" not in updated


def test_actual_kinx_supplements_are_exact_receipt_specific_contracts():
    actual = {
        item["receipt_no"]: item for item in reviewed.CORRECTIONS
        if item["ticker"] == "093320" and item["field"] == "record_date"
    }

    assert set(actual) == {"20160224900227", "20170316900231"}
    assert actual["20160224900227"]["corrected_value"] == "2015-12-31"
    assert actual["20160224900227"]["expected_common_cash_amount"] == 100.0
    assert actual["20160224900227"]["evidence_corp_code"] == "00603348"
    assert actual["20160224900227"]["basis"] == (
        "RECEIPT_SPECIFIC_REVIEWED_FY_END_RECORD_DATE_SUPPLEMENT"
    )
    assert actual["20170316900231"]["corrected_value"] == "2016-12-31"
    assert actual["20170316900231"]["expected_common_cash_amount"] == 120.0
    assert actual["20170316900231"]["evidence_corp_code"] == "00603348"
    assert actual["20170316900231"]["basis"] == (
        "RECEIPT_SPECIFIC_REVIEWED_FY_END_RECORD_DATE_SUPPLEMENT"
    )
