import hashlib
from datetime import date
from pathlib import Path

import pytest

from pipeline.silver.krx_kind_reference import (
    kind_url_identity,
    parse_dart_detachment_notice,
    parse_kind_identity_receipt,
    parse_kind_reference_notice,
    parse_kind_stock_dividend_component,
)


FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "kind"


def _numbered_detachment_body(
    *, ticker: str, effective: str, reference: str, date_label: str, reason: str,
) -> bytes:
    return (
        "<html><body><table><tr>"
        "<th>1. 회사명</th><th>2. 주권종류</th><th>3. 단축코드</th>"
        f"<th>4. 기준가(원)</th><th>5. {date_label}</th><th>6. 사유</th>"
        "</tr><tr><td>테스트회사</td><td>보통주식</td>"
        f"<td>A{ticker}</td><td>{reference}</td><td>{effective}</td>"
        f"<td>{reason}</td></tr></table></body></html>"
    ).encode()


def _legacy_combined_body(
    *, issuer: str, ticker: str, effective: str, reference: str,
) -> bytes:
    return (
        "<html><body><table>"
        f'<tr><th rowspan="1">1. 권배락 실시일</th>'
        f'<td colspan="4">{effective}</td></tr>'
        '<tr><th>2. 권배락 사유</th><td colspan="4">무상증자 및 배당</td></tr>'
        "<tr><th rowspan=\"2\">3. 권배락 내역</th><th>회사명</th>"
        "<th>주권종류</th><th>단축코드</th><th>기준가(원)</th></tr>"
        f"<tr><td>{issuer}</td><td>보통주식</td><td>A{ticker}</td>"
        f"<td>{reference}</td></tr>"
        '<tr><th>4. 기타</th><td colspan="4">-</td></tr>'
        "</table></body></html>"
    ).encode()


@pytest.mark.parametrize(
    ("date_label", "reason", "action_type"),
    [
        ("배당락 실시일", "주식배당", "ex_dividend"),
        ("권리락 실시일", "유상증자", "rights_detachment"),
    ],
)
def test_dart_numbered_detachment_notice_binds_every_identity_field(
    date_label, reason, action_type,
):
    notice = parse_dart_detachment_notice(_numbered_detachment_body(
        ticker="208140",
        effective="2020-09-02",
        reference="2,630",
        date_label=date_label,
        reason=reason,
    ))

    assert notice.issuer_name == "테스트회사"
    assert notice.ticker == "208140"
    assert notice.security_class == "COMMON"
    assert notice.effective_date == date(2020, 9, 2)
    assert notice.reference_price == 2_630.0
    assert notice.reason == reason
    assert notice.action_type == action_type


@pytest.mark.parametrize(
    ("issuer", "ticker", "effective", "reference"),
    [
        ("풍국주정", "023900", "2016-11-09", "8,200"),
        ("와토스코리아", "079000", "2017-12-27", "6,530"),
        ("유진테크", "084370", "2015-12-29", "12,600"),
        ("광진윈텍", "090150", "2021-12-29", "4,960"),
        ("하이텍팜", "106190", "2023-12-27", "9,660"),
        ("다나와", "119860", "2016-12-28", "7,120"),
        ("이노메트리", "302430", "2019-12-27", "15,400"),
    ],
)
def test_actual_legacy_combined_shapes_parse_exactly(
    issuer, ticker, effective, reference,
):
    notice = parse_dart_detachment_notice(_legacy_combined_body(
        issuer=issuer,
        ticker=ticker,
        effective=effective,
        reference=reference,
    ))

    assert notice.issuer_name == issuer
    assert notice.ticker == ticker
    assert notice.security_class == "COMMON"
    assert notice.effective_date == date.fromisoformat(effective)
    assert notice.reference_price == float(reference.replace(",", ""))
    assert notice.reason == "무상증자 및 배당"
    assert notice.action_type == "combined_detachment"


def test_dart_detachment_notice_rejects_two_exact_rows():
    first = _numbered_detachment_body(
        ticker="208140", effective="2020-09-02", reference="2,630",
        date_label="권리락 실시일", reason="유상증자",
    ).decode()
    second = _numbered_detachment_body(
        ticker="208140", effective="2020-09-02", reference="2,640",
        date_label="권리락 실시일", reason="유상증자",
    ).decode()

    with pytest.raises(RuntimeError, match="ambiguous"):
        parse_dart_detachment_notice((first + second).encode())


@pytest.mark.parametrize(
    (
        "name", "sha256", "issuer", "security", "reference", "effective",
        "form_id", "ticker",
    ),
    [
        (
            "001040-20181226-99311.html",
            "8148d7d040ada57504933ebb234d4b87aa801e5b991137912fc37ecfff1d18bf",
            "CJ", "COMMON", 124_000.0, date(2018, 12, 27), "99311", None,
        ),
        (
            "001530-20221227-99311.html",
            "8a1ba54591537a944008f72981b729718d3ca791f98f76ca6072b58014a0c608",
            "디아이동일", "COMMON", 15_950.0, date(2022, 12, 28), "99311", None,
        ),
        (
            "006800-20260313-99311.html",
            "6d24251bbabc1ca2b7f6dba7639d6b448e9a7df6a1ac2ebed44f7139578e6d02",
            "미래에셋증권", "COMMON", 69_200.0, date(2026, 3, 16), "99311", None,
        ),
        (
            "033540-20161227-70767.html",
            "fba1eaca63a58ffe524b304292909e9890652f59d9543fcb81eb13fb6e74472b",
            "파라텍", "COMMON", 7_800.0, date(2016, 12, 28), "70767", "033540",
        ),
        (
            "045300-20181226-70767.html",
            "20b89026b44fb3bdaf13a76c21fed62c8977dbcf268ae8eb00ad98de16c72619",
            "성우테크론", "COMMON", 4_030.0, date(2018, 12, 27), "70767", "045300",
        ),
        (
            "095340-20171226-70767.html",
            "c5418f4d2cc3e10f787a9dddcd01f3a92e2268ec7f2983f61cf9f748ce49c62b",
            "ISC", "COMMON", 18_350.0, date(2017, 12, 27), "70767", "095340",
        ),
        (
            "145270-20171226-99311.html",
            "af84d95181a1e9f7845c81995d7950c29baeaa4e34982747cd3d6af7b0e971b2",
            "케이탑리츠", "COMMON", 1_025.0, date(2017, 12, 27), "99311", None,
        ),
        (
            "145270-20181226-99311.html",
            "abeff1b236552af3a8ea1e28939ff94acb8f1b5d634315da610dfcf2425f7d16",
            "케이탑리츠", "COMMON", 1_130.0, date(2018, 12, 27), "99311", None,
        ),
    ],
)
def test_actual_kind_reference_bodies_parse_exactly(
    name, sha256, issuer, security, reference, effective, form_id, ticker,
):
    payload = (FIXTURE_ROOT / name).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == sha256

    notice = parse_kind_reference_notice(payload, expected_form_id=form_id)

    assert notice.issuer_name == issuer
    assert notice.security_class == security
    assert notice.reference_price == reference
    assert notice.effective_date == effective
    assert notice.reason == "주식배당"
    assert notice.action_type == "ex_dividend"
    assert notice.form_id == form_id
    assert notice.ticker == ticker


@pytest.mark.parametrize(
    ("name", "sha256", "acceptance", "issuer", "ticker", "document"),
    [
        ("001040-20181226-main.html", "2aa270e2111e095fc48804ac63d6ecdd6c97a79a65f068895c722a1b72ae843b", "20181226000844", "CJ", "001040", "20181226003327"),
        ("001530-20221227-main.html", "90379c1fe1b701458e7b4859a33a408b6a6777968122db5f89a1615f1a336f32", "20221227000756", "디아이동일", "001530", "20221227002134"),
        ("006800-20260313-main.html", "3929b000dbda1b92b51a240ed7224a3433be9d51aed2e6560f23f1ccd977ec5b", "20260313001262", "미래에셋증권", "006800", "20260313003064"),
        ("033540-20161227-main.html", "0c2221cc49f9d8e4a19327ec180b03509d437317b4fde9565f89e572f2241922", "20161227000594", "파라텍", "033540", "20161227001299"),
        ("045300-20181226-main.html", "6254d6caf0d4a44c43c1e59b514c9d768e1bee6b5f4a746d95efae7a6a60d643", "20181226000659", "성우테크론", "045300", "20181226002040"),
        ("095340-20171226-main.html", "f1bcf38a4fc76640f8b2e65878aa31840eb01cb7193311cd4cfd9f5a37c2acd3", "20171226000944", "ISC", "095340", "20171226002788"),
        ("145270-20171226-main.html", "20566ed33f3e0135bf760800af51d983152b9e5bede1851f8be44ff9b3775d42", "20171226000861", "케이탑리츠", "145270", "20171226004095"),
        ("145270-20181226-main.html", "a463f856f36f3118b6494ff04e8a732ae84ba8e09e716522fa3d482fa39de6b1", "20181226000861", "케이탑리츠", "145270", "20181226004067"),
    ],
)
def test_actual_kind_identity_pages_bind_ticker_and_selected_document(
    name, sha256, acceptance, issuer, ticker, document,
):
    payload = (FIXTURE_ROOT / name).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == sha256

    identity = parse_kind_identity_receipt(payload)

    assert identity.acceptance_no == acceptance
    assert identity.issuer_name == issuer
    assert identity.ticker == ticker
    assert identity.selected_document_no == document


def test_99311_rowspan_is_part_of_the_column_contract():
    payload = (FIXTURE_ROOT / "006800-20260313-99311.html").read_bytes()
    forged = payload.replace(b' rowspan="2"', b'', 1)

    with pytest.raises(RuntimeError, match="security/price is ambiguous"):
        parse_kind_reference_notice(forged, expected_form_id="99311")


def test_reference_notice_rejects_url_body_form_confusion():
    payload = (FIXTURE_ROOT / "033540-20161227-70767.html").read_bytes()

    with pytest.raises(RuntimeError, match="URL/body form identity mismatch"):
        parse_kind_reference_notice(payload, expected_form_id="99311")


def test_url_identity_rejects_unknown_kind_form():
    with pytest.raises(RuntimeError, match="form is unsupported"):
        kind_url_identity(
            "https://kind.krx.co.kr/external/2026/03/13/001262/"
            "20260313003064/12345.htm"
        )


def test_actual_cj_component_main_and_body_bind_terminal_cross_class_terms():
    main = (FIXTURE_ROOT / "001040-20181221-component-main.html").read_bytes()
    body = (FIXTURE_ROOT / "001040-20181221-61474.html").read_bytes()
    assert hashlib.sha256(main).hexdigest() == (
        "ca3e649baffcfbafdfe05ba0608dd475b48c3d131cb8e48853868cfaf3e072f5"
    )
    assert hashlib.sha256(body).hexdigest() == (
        "899a27931ba1b921ddf77f891115c6c2f11b0e39c0b894f25f2e26735087f81c"
    )

    identity = parse_kind_identity_receipt(main)
    component = parse_kind_stock_dividend_component(body)

    assert identity.acceptance_no == "20181221000001"
    assert identity.issuer_name == "CJ"
    assert identity.ticker == "001040"
    assert identity.selected_document_no == "20181220002252"
    assert component.decision_date == date(2018, 12, 20)
    assert component.record_date == date(2018, 12, 31)
    assert component.ratio_numerator == 0.15
    assert component.ratio_denominator == 1.0
    assert component.entitlement_security_class == "COMMON_AND_PREFERRED"
    assert component.distributed_security_class == "NEW_PREFERRED"
