import json
import zipfile
from datetime import date
from io import BytesIO

import pandas as pd
import pytest

from pipeline.silver import corporate_actions


@pytest.mark.parametrize(
    "rendered",
    ["20250310", "2025-03-10", "2025년 3월 10일"],
)
def test_parse_date_accepts_padded_and_non_padded_calendar_dates(rendered):
    assert corporate_actions._parse_date(rendered) == date(2025, 3, 10)


def test_parse_date_rejects_invalid_calendar_date():
    assert corporate_actions._parse_date("2025-02-30") is None


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_document(path, xml):
    path.parent.mkdir(parents=True, exist_ok=True)
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("document.xml", xml)
    path.write_bytes(output.getvalue())


def test_prepare_normalizes_structured_factor_and_exchange_notice(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260601000001.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000001",
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000002",
        "rcept_dt": "20260707",
        "report_nm": "권리락(무상증자)",
    }])

    events, stats = corporate_actions.prepare(str(tmp_path))

    assert len(events) == 2
    bonus = events[events["source"].eq("DART_STRUCTURED")].iloc[0]
    assert bonus["event_type"] == "bonus_issue"
    assert bonus["effective_date"] == date(2026, 7, 8)
    assert bonus["expected_factor"] == pytest.approx(0.5)
    notice = events[events["source"].eq("DART_DISCLOSURE")].iloc[0]
    assert notice["event_type"] == "rights_detachment"
    assert notice["effective_date"] == date(2026, 7, 7)
    assert stats["effective_date_count"] == 2
    assert stats["expected_factor_count"] == 1


def test_cancelled_structured_bonus_remains_issuer_revision_but_not_support(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260601000009.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000009",
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260601/to=20260601/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260601000009",
        "rcept_dt": "20260601",
        "report_nm": "무상증자결정(취소)",
    }])

    events, _ = corporate_actions.prepare(str(tmp_path))

    bonus = events[events["event_type"].eq("bonus_issue")].iloc[0]
    assert bonus["action_scope"] == "ISSUER"
    assert not bonus["confirms_price_adjustment"]
    assert not bonus["expects_price_adjustment"]


@pytest.mark.parametrize(
    ("report_name", "scope", "expects"),
    [
        ("[기재정정]주요사항보고서(무상증자결정)", "ISSUER", True),
        (
            "주요사항보고서(무상증자결정)(종속회사의 주요경영사항)",
            "RELATED_COMPANY",
            False,
        ),
    ],
)
def test_structured_revision_and_related_company_scope_are_separate(
    tmp_path, report_name, scope, expects,
):
    receipt = "20260601000019"
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / f"corp=005930/rcept={receipt}.json"
    )
    _write_json(structured, {
        "rcept_no": receipt,
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260601/to=20260601/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": receipt,
        "rcept_dt": "20260601",
        "report_nm": report_name,
    }])

    events, _ = corporate_actions.prepare(str(tmp_path))
    bonus = events[events["event_type"].eq("bonus_issue")].iloc[0]

    assert bonus["action_scope"] == scope
    assert bool(bonus["expects_price_adjustment"]) is expects
    assert bool(bonus["confirms_price_adjustment"]) is expects


def test_rights_notice_uses_document_execution_date(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260202/to=20260731/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "008830",
        "rcept_no": "20260731901116",
        "rcept_dt": "20260731",
        "report_nm": "권리락 (무상증자)",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=008830"
        / "rcept=20260731901116.zip"
    )
    _write_document(
        document,
        "<document><label>권리락 실시일</label>"
        "<value>2026-08-03</value></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["effective_date"] == date(2026, 8, 3)
    assert event["match_window_days"] == 0


def test_stock_dividend_decision_keeps_record_date_out_of_ex_date(tmp_path):
    interval = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20261201/to=20261231"
    )
    # Stale v1/v2 manifests must not mask the v3 discovery contract.
    _write_json(interval / "disclosures.json", [])
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "001040",
        "rcept_no": "20261220000001",
        "rcept_dt": "20261220",
        "report_nm": "주식배당결정",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=001040"
        / "rcept=20261220000001.zip"
    )
    _write_document(
        document,
        "<document><label>배당기준일</label>"
        "<value>2026-12-31</value><label>주식배당결정</label></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["event_type"] == "stock_dividend"
    assert event["record_date"] == date(2026, 12, 31)
    assert pd.isna(event["effective_date"])
    assert event["match_window_days"] == 0
    assert not event["confirms_price_adjustment"]
    assert event["expects_price_adjustment"]
    published = corporate_actions.normalize_for_publish(events).iloc[0]
    assert pd.isna(published["ex_date"])
    assert published["record_date"] == date(2026, 12, 31)


def test_iwin_stock_dividend_decision_publishes_exact_ordinary_ratio(tmp_path):
    interval = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231"
    )
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "090150",
        "rcept_no": "20211224900781",
        "rcept_dt": "20211224",
        "report_nm": "주식배당결정(정정)",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2021/corp=090150"
        / "rcept=20211224900781.zip"
    )
    _write_document(
        document,
        "<document>주식배당결정 배당기준일 2021-12-31<table>"
        "<tr><td>1주당 주식배당</td><td>보통주식</td>"
        "<td>0.1주</td></tr></table></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    published = corporate_actions.normalize_for_publish(events).iloc[0]

    assert event["record_date"] == date(2021, 12, 31)
    assert event["ratio_numerator"] == pytest.approx(0.1)
    assert event["ratio_denominator"] == pytest.approx(1.0)
    assert published["ratio_numerator"] == pytest.approx(0.1)
    assert published["ratio_denominator"] == pytest.approx(1.0)


def test_stock_dividend_ratio_ignores_issued_share_total_lure(tmp_path):
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260301/to=20260331"
    )
    receipt = "20260313809999"
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "006800",
        "rcept_no": receipt,
        "rcept_dt": "20260313",
        "report_nm": "[기재정정]주식배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=006800"
        / f"rcept={receipt}.zip"
    )
    _write_document(
        document,
        "<document>주식배당결정 배당기준일 2026-03-17<table>"
        "<tr><td>발행주식총수</td><td>보통주식</td>"
        "<td>555,316,408</td></tr>"
        "<tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
        "<td>0.0073206</td></tr>"
        "<tr><td>배당주식총수</td><td>보통주식</td>"
        "<td>4,065,108</td></tr></table></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert len(events) == 1
    assert events.iloc[0]["ratio_numerator"] == pytest.approx(0.0073206)


def test_combined_detachment_requires_exact_date_reference_and_reason(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005950",
        "rcept_no": "20211228900755",
        "rcept_dt": "20211228",
        "report_nm": "권배락(무상증자 및 배당)",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211228900755.zip"
    )
    _write_document(document, """
        <table>
          <tr><td>권배락 실시일</td><td>2021-12-29</td></tr>
          <tr><td>기준가격</td><td>4,960</td></tr>
          <tr><td>사유</td><td>무상증자 및 배당</td></tr>
        </table>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["event_type"] == "combined_detachment"
    assert event["effective_date"] == date(2021, 12, 29)
    assert event["confirms_price_adjustment"]
    assert event["action_method"] == "무상증자 및 배당"


def test_combined_detachment_duplicate_reference_field_is_not_confirmed(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005950", "rcept_no": "20211228900755",
        "rcept_dt": "20211228", "report_nm": "권배락",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211228900755.zip"
    )
    _write_document(document, """
        <table>
          <tr><td>권배락 실시일</td><td>2021-12-29</td></tr>
          <tr><td>기준가격</td><td>5,100</td></tr>
          <tr><td>기준가격</td><td>4,960</td></tr>
          <tr><td>사유</td><td>무상증자 및 배당</td></tr>
        </table>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert not events.iloc[0]["confirms_price_adjustment"]


def test_capital_reduction_share_factor_is_not_a_price_factor(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000003.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000003",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "capital_reduction"
    assert pd.isna(events.iloc[0]["expected_factor"])
    assert events.iloc[0]["share_count_factor"] == pytest.approx(8.0)
    assert events.iloc[0]["share_count_before"] == pytest.approx(8_000)
    assert events.iloc[0]["share_count_after"] == pytest.approx(1_000)
    assert events.iloc[0]["share_count_factor_comparable"]
    assert (
        events.iloc[0]["share_count_comparison_reason"]
        == "UNIFORM_REDUCTION"
    )
    assert events.iloc[0]["action_method"] == "보통주식 8대 1 무상감자"


def test_non_uniform_reduction_is_not_share_factor_comparable(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000005.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000005",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "최대주주 보유주식만 8대 1 병합",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert not events.iloc[0]["share_count_factor_comparable"]


def test_combined_offering_does_not_infer_factor_from_bonus_leg_only(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=combined_offering/year=2026"
        / "corp=005930/rcept=20260601000004.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000004",
        "fric_nstk_asstd": "2026년 07월 08일",
        "fric_nstk_ascnt_ps_ostk": "1.0",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "combined_offering"
    assert pd.isna(events.iloc[0]["expected_factor"])


def test_simultaneous_financing_makes_reduction_ratio_not_comparable(tmp_path):
    reduction = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000010.json"
    )
    financing = (
        tmp_path
        / "corporate_actions/dart/structured/event=paid_increase/year=2026"
        / "corp=005930/rcept=20260601000011.json"
    )
    _write_json(reduction, {
        "rcept_no": "20260601000010",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })
    _write_json(financing, {
        "rcept_no": "20260601000011",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    reduction_event = events[
        events["event_type"].eq("capital_reduction")
    ].iloc[0]
    assert not reduction_event["share_count_factor_comparable"]
    assert (
        reduction_event["share_count_comparison_reason"]
        == "SIMULTANEOUS_FINANCING_DISCLOSURE"
    )


def test_ex_dividend_notice_is_evidence_not_required_price_adjustment(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000005",
        "rcept_dt": "20260707",
        "report_nm": "배당락",
    }])
    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    assert event["event_type"] == "ex_dividend"
    assert not event["confirms_price_adjustment"]
    assert not event["expects_price_adjustment"]
    assert pd.isna(event["effective_date"])


def test_ex_dividend_notice_uses_only_actual_execution_date(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000006",
        "rcept_dt": "20260707",
        "report_nm": "배당락",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260707000006.zip"
    )
    _write_document(
        document,
        "<document><label>배당락 실시일</label>"
        "<value>2026-07-06</value></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["effective_date"] == date(2026, 7, 6)
    assert event["confirms_price_adjustment"]


def test_cash_dividend_decision_parses_common_amount_dates_and_frequency(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260724/to=20260724/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "006120",
        "corp_cls": "K",
        "rcept_no": "20260724800658",
        "rcept_dt": "20260724",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=006120"
        / "rcept=20260724800658.zip"
    )
    _write_document(document, """
        <document>1. 배당구분 중간배당 2. 배당종류 현금배당
        3. 1주당 배당금(원) 보통주식 500 종류주식 500
        6. 배당기준일 2026-08-10
        7. 배당금지급 예정일자 2026-08-21
        주석 배당기준일 2026년 8월 10일</document>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    assert event["event_type"] == "cash_dividend"
    assert event["cash_amount"] == pytest.approx(500)
    assert event["record_date"] == date(2026, 8, 10)
    assert event["payment_date"] == date(2026, 8, 21)
    assert event["frequency"] == "interim"
    published = corporate_actions.normalize_for_publish(events).iloc[0]
    assert published["cash_amount"] == pytest.approx(500)
    assert published["adjusted_cash_amount"] is None
    assert published["action_scope"] == "ISSUER"
    assert published["report_name"] == "현금ㆍ현물배당결정"
    assert published["corp_cls"] == "K"


def test_zero_common_dps_normalizes_to_no_common_with_null_amount(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20250101/to=20251231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "010950",
        "rcept_no": "20150227801008",
        "rcept_dt": "20150227",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2015/corp=010950"
        / "rcept=20150227801008.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 0 "
        "배당기준일 2014-12-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount_status"] == "NO_COMMON_CASH_DIVIDEND"
    assert pd.isna(event["cash_amount"])


def test_positive_original_without_record_date_is_explicitly_pending(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20240101/to=20241231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "029780",
        "rcept_no": "20240129800952",
        "rcept_dt": "20240129",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2024/corp=029780"
        / "rcept=20240129800952.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 2,500 "
        "배당기준일 -</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount_status"] == "POSITIVE_PENDING_RECORD_DATE"
    assert event["cash_amount"] == pytest.approx(2500)
    assert pd.isna(event["record_date"])


@pytest.mark.parametrize(
    ("ticker", "receipt"),
    [
        ("0008Z0", "20260120900486"),
        ("0010V0", "20260206900936"),
        ("0039P0", "20260708900856"),
    ],
)
def test_cash_dividend_parser_preserves_new_alphanumeric_krx_tickers(
    tmp_path, ticker, receipt,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260101/to=20260731/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": ticker.lower(),
        "corp_cls": "K",
        "rcept_no": receipt,
        "rcept_dt": receipt[:8],
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026"
        / f"corp={ticker}" / f"rcept={receipt}.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 150 "
        "배당기준일 2026-07-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events["identifier"].tolist() == [ticker]
    assert events.iloc[0]["cash_amount"] == pytest.approx(150)
    assert events.iloc[0]["record_date"] == date(2026, 7, 31)


def test_ticker_path_fallback_uppercases_alphanumeric_krx_code():
    assert corporate_actions._ticker_from_path(
        "/structured/year=2026/corp=0008z0/rcept=receipt.json"
    ) == "0008Z0"


def test_subsidiary_cash_dividend_is_not_assigned_to_parent_ticker(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260312/to=20260312/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "128940",
        "rcept_no": "20260312800001",
        "rcept_dt": "20260312",
        "report_nm": "현금ㆍ현물배당 결정(자회사의 주요경영사항)",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=128940"
        / "rcept=20260312800001.zip"
    )
    _write_document(
        document,
        "<document>자회사인 한미약품의 주요경영사항 "
        "1주당 배당금(원) 보통주식 2,000 "
        "배당기준일 2026-03-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.empty


def test_subsidiary_form_body_is_excluded_even_without_title_suffix(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260312/to=20260312/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "128940",
        "rcept_no": "20260312800002",
        "rcept_dt": "20260312",
        "report_nm": "현금ㆍ현물배당 결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=128940"
        / "rcept=20260312800002.zip"
    )
    _write_document(
        document,
        "<document>자회사인 한미약품 주식회사의 주요경영사항신고 "
        "1주당 배당금(원) 보통주식 2,000 "
        "배당기준일 2026-03-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.empty


def test_original_cash_filing_is_removed_when_correction_marks_subsidiary(
    tmp_path,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20230310/to=20230313/disclosures_v3.json"
    )
    _write_json(manifest, [
        {
            "stock_code": "009440",
            "rcept_no": "20230310801178",
            "rcept_dt": "20230310",
            "report_nm": "현금ㆍ현물배당 결정",
        },
        {
            "stock_code": "009440",
            "rcept_no": "20230313800096",
            "rcept_dt": "20230313",
            "report_nm": "[기재정정]현금ㆍ현물배당 결정(자회사의 주요경영사항)",
        },
    ])
    original = (
        tmp_path / "corporate_actions/dart/documents/year=2023/corp=009440"
        / "rcept=20230310801178.zip"
    )
    correction = (
        tmp_path / "corporate_actions/dart/documents/year=2023/corp=009440"
        / "rcept=20230313800096.zip"
    )
    body = (
        "1주당 배당금(원) 보통주식 2,949 "
        "배당기준일 2022-12-31 배당금지급 예정일자 2023-04-28"
    )
    _write_document(original, f"<document>{body}</document>")
    _write_document(
        correction,
        "<document>정정관련 공시서류제출일 2023-03-10 "
        f"{body} 자회사인 케이씨환경서비스의 주요경영사항</document>",
    )

    events, stats = corporate_actions.prepare(str(tmp_path))

    assert events.empty
    assert stats["related_company_correction_excluded_count"] == 1


def test_corrected_cash_dividend_uses_last_body_values_without_joining_numbers(
    tmp_path,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260410/to=20260410/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260410800001",
        "rcept_dt": "20260410",
        "report_nm": "[기재정정]현금ㆍ현물배당 결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260410800001.zip"
    )
    _write_document(document, """
        <document>
          <table class="correction">
            <tr><td>정정전</td><td>1주당 배당금(원)</td>
                <td>보통주식 300</td><td>정정후 500</td></tr>
            <tr><td>배당기준일</td><td>2026-03-30</td>
                <td>2026-03-31</td></tr>
          </table>
          <section class="corrected-body">
            1. 배당구분 분기배당
            3. 1 주당 배당금 ( 원 ) 보통주식 500 종류주식 -
            6. 배당 기준일 : 2026 - 03 - 31
            7. 배당금 지급 예정일자 : 2026.04.25
          </section>
        </document>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount"] == pytest.approx(500)
    assert event["record_date"] == date(2026, 3, 31)
    assert event["payment_date"] == date(2026, 4, 25)
    assert event["frequency"] == "quarterly"


def test_common_issuer_events_are_inherited_by_preferred_share():
    event = pd.DataFrame([{
        "identifier": "001520",
        "event_type": "delisting",
        "rcept_no": "20260707000006",
    }])
    expanded, stats = corporate_actions.inherit_issuer_events(
        event,
        {"001529": "001520"},
    )
    inherited = expanded[expanded["identifier"].eq("001529")].iloc[0]
    assert inherited["issuer_event_inherited"]
    assert inherited["issuer_parent_identifier"] == "001520"
    assert stats["inherited_event_count"] == 1


def test_nontradable_actions_are_explicitly_excluded():
    events = pd.DataFrame([
        {
            "identifier": "005930", "event_type": "bonus_issue",
            "effective_date": date(2026, 8, 1), "announcement_date": None,
        },
        {
            "identifier": "026870", "event_type": "bonus_issue",
            "effective_date": date(2026, 8, 2), "announcement_date": None,
        },
        {
            "identifier": "123456", "event_type": "cash_dividend",
            "effective_date": None, "announcement_date": date(2026, 8, 3),
        },
    ])

    retained, stats = corporate_actions.exclude_nontradable(
        events,
        {"row_count": 3},
        {"005930"},
        {"123456"},
    )

    assert list(retained["identifier"]) == ["005930"]
    assert stats["transformed_rows"] == 1
    assert stats["excluded_rows"] == 2
    assert stats["no_tradable_price_action"]["row_count"] == 1
    assert stats["unsupported_market_action"]["row_count"] == 1


def test_immutable_overlap_disclosure_receipt_fails_closed(tmp_path):
    for start, end, ticker in (
        ("20260101", "20260131", "005930"),
        ("20260115", "20260201", "000660"),
    ):
        manifest = (
            tmp_path / "corporate_actions/dart/manifests"
            / f"from={start}" / f"to={end}/disclosures_v3.json"
        )
        _write_json(manifest, [{
            "stock_code": ticker,
            "rcept_no": "20260102900228",
            "rcept_dt": "20260102",
            "report_nm": "현금ㆍ현물배당결정",
            "rm": "유",
        }])

    with pytest.raises(RuntimeError, match="immutable DART disclosure"):
        corporate_actions._disclosure_rows(str(tmp_path))
