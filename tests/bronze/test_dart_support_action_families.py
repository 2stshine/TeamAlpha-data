import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.bronze import dart_support_action_families as families


def _dart_fixture(name: str) -> Path:
    return Path(__file__).parents[1] / "fixtures" / "dart" / name


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_interval(
    root: Path,
    rows: list[dict],
    *,
    start: str = "20211201",
    end: str = "20211231",
    marker_version: int = 5,
) -> None:
    interval = (
        root / "corporate_actions" / "dart" / "manifests"
        / f"from={start}" / f"to={end}"
    )
    _write_json(interval / "disclosures_v3.json", rows)
    marker = {"status": "COMPLETE", "fromdate": start, "todate": end}
    structured_queries = {
        ("bonus_issue", str(row.get("corp_code") or ""))
        for row in rows
        if "무상증자결정" in str(row.get("report_nm") or "")
        and str(row.get("stock_code") or "")
        and str(row.get("corp_code") or "")
    }
    _write_json(
        interval / "structured_complete_v3.json",
        {**marker, "query_count": len(structured_queries)},
    )
    _write_json(
        interval / f"documents_complete_v{marker_version}.json",
        {**marker, "candidate_count": len(rows)},
    )
    for row in rows:
        receipt = row["rcept_no"]
        ticker = str(row["stock_code"]).zfill(6)
        rendered_date = (
            f"{row['rcept_dt'][:4]}-{row['rcept_dt'][4:6]}-"
            f"{row['rcept_dt'][6:8]}"
        )
        _write_json(
            root / "corporate_actions" / "dart" / "disclosures"
            / f"year={receipt[:4]}" / f"date={rendered_date}"
            / f"corp={ticker}" / f"rcept={receipt}.json",
            row,
        )


def _stock_row(
    receipt: str,
    *,
    ticker: str = "090150",
    report: str = "주식배당결정",
    **extra,
) -> dict:
    return {
        "rcept_no": receipt,
        "rcept_dt": receipt[:8],
        "stock_code": ticker,
        "corp_code": f"corp-{ticker}",
        "report_nm": report,
        **extra,
    }


def _bonus_row(
    receipt: str,
    *,
    ticker: str = "090150",
    report: str = "주요사항보고서(무상증자결정)",
    **extra,
) -> dict:
    return _stock_row(
        receipt, ticker=ticker, report=report, **extra,
    )


def _write_bonus_structured(
    root: Path,
    receipt: str,
    *,
    ticker: str = "090150",
    ratio: str = "1.0",
    effective: str = "20211229",
) -> Path:
    path = (
        root / "corporate_actions" / "dart" / "structured"
        / "event=bonus_issue" / f"year={receipt[:4]}"
        / f"corp={ticker}" / f"rcept={receipt}.json"
    )
    _write_json(path, {
        "rcept_no": receipt,
        "nstk_ascnt_ps_ostk": ratio,
        "nstk_asstd": effective,
    })
    return path


def _stock_body(ratio: str = "0.1", *, origin: str | None = None) -> bytes:
    correction = (
        f"<p>정정 관련 공시서류 제출일 : {origin}</p>" if origin else ""
    )
    return (
        "<html><body>" + correction
        + "<table><tr><td>배당기준일</td><td>2021-12-31</td></tr>"
        + "<tr><td>1. 1주당 배당주식수 (주)</td><td>보통주식</td>"
        + f"<td>{ratio}</td></tr>"
        + "</table></body></html>"
    ).encode()


class _Remote:
    def __init__(
        self,
        families_by_receipt: dict[str, tuple[str, ...]],
        reports: dict[str, str],
        bodies: dict[str, bytes],
        *,
        attachments_by_receipt: dict[
            str, tuple[tuple[str, str], ...]
        ] | None = None,
        dcms: dict[str, str] | None = None,
        dtds: dict[str, str] | None = None,
    ) -> None:
        self.families_by_receipt = families_by_receipt
        self.reports = reports
        self.bodies = bodies
        self.attachments_by_receipt = attachments_by_receipt or {}
        self.dcms = dcms or {}
        self.dtds = dtds or {}
        self.calls: list[str] = []

    def dcm(self, receipt: str) -> str:
        return self.dcms.get(
            receipt, str(1_000_000 + int(receipt[-6:])),
        )

    def dtd(self, receipt: str) -> str:
        return self.dtds.get(receipt, "HTML")

    def main(self, receipt: str) -> bytes:
        order = self.families_by_receipt[receipt]
        attachment_options = self.attachments_by_receipt.get(receipt, ())
        attachment_current = any(
            member == receipt and dcm == self.dcm(receipt)
            for member, dcm in attachment_options
        ) and receipt not in order
        options = []
        for member in order:
            selected = (
                " selected"
                if member == receipt and not attachment_current else ""
            )
            options.append(
                f'<option value="rcpNo={member}"{selected} '
                f'title="{self.reports[member]}">'
                f'{self.reports[member]}</option>'
            )
        attachments = []
        for member, dcm in attachment_options:
            selected = (
                " selected"
                if attachment_current
                and member == receipt
                and dcm == self.dcm(receipt)
                else ""
            )
            attachments.append(
                f'<option value="rcpNo={member}&amp;dcmNo={dcm}"{selected}>'
                f'attachment {member}</option>'
            )
        return (
            f'<script>viewDoc("{receipt}", "{self.dcm(receipt)}", '
            f'"0", "0", "0", "{self.dtd(receipt)}", "");</script>'
            '<select id="family"><option value="null">select</option>'
            + "".join(options)
            + '</select><select id="att"><option value="null">attach</option>'
            + "".join(attachments)
            + "</select>"
        ).encode()

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return self.main(receipt)
        assert query["dcmNo"] == [self.dcm(receipt)]
        assert query["dtd"] == [self.dtd(receipt)]
        return self.bodies[receipt]


def _stock_fixture(root: Path):
    original = "20211217000001"
    correction = "20211224000002"
    rows = [
        _stock_row(original),
        _stock_row(correction, report="[기재정정] 주식배당결정"),
    ]
    _write_interval(root, rows)
    order = (correction, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            correction: _stock_body("0.2", origin="2021년 12월 17일"),
        },
    )
    return original, correction, remote


def test_real_006800_labelled_row_rejects_adjacent_share_count_traps():
    fixture = _dart_fixture("20260313800897-stock-dividend-table.html")

    ratio = families.stock_dividend_common_ratio_from_body(
        fixture.read_bytes(),
    )

    assert ratio == pytest.approx(0.0073206, rel=0, abs=0)
    assert ratio not in {4_250_472.0, 555_316_408.0, 150_575_750.0}


def test_stock_ratio_missing_or_preferred_only_fails_closed():
    no_ratio = b"""
      <table>
        <tr><td>2. total dividend shares</td><td>ordinary</td><td>4250472</td></tr>
        <tr><td>3. issued shares</td><td>ordinary</td><td>555316408</td></tr>
      </table>
    """
    preferred_only = """
      <table><tr><td>1. 1주당 배당주식수 (주)</td>
      <td>종류주식</td><td>0.8</td></tr></table>
    """.encode()

    assert families.stock_dividend_common_ratio_from_body(no_ratio) is None
    assert (
        families.stock_dividend_common_ratio_from_body(preferred_only) is None
    )


def test_stock_ratio_ambiguous_exact_ordinary_rows_fail_closed():
    body = """
      <table>
        <tr><td>1. 1주당 배당주식수 (주)</td>
        <td>보통주식</td><td>0.1</td></tr>
        <tr><td>1. 1주당 배당주식수 (주)</td>
        <td>보통주식</td><td>0.2</td></tr>
      </table>
    """.encode()

    with pytest.raises(RuntimeError, match="ratio is ambiguous"):
        families.stock_dividend_common_ratio_from_body(body)


@pytest.mark.parametrize(
    ("rendered", "expected"),
    [
        ("20241209", "2024-12-09"),
        ("2024년 12월 9일", "2024-12-09"),
        ("2024년 7월 1일", "2024-07-01"),
        ("2024-7-1", "2024-07-01"),
        ("2024.07.01", "2024-07-01"),
    ],
)
def test_dart_date_normalizes_compact_and_non_padded_dates(
    rendered, expected,
):
    assert families.parse_dart_date(rendered) == expected


@pytest.mark.parametrize("rendered", ["2025-02-30", "202471", "date=2024-7-1"])
def test_dart_date_rejects_invalid_or_non_exact_values(rendered):
    assert families.parse_dart_date(rendered) is None


def test_correction_origin_accepts_actual_korean_non_padded_date_shape():
    body = """
      <table><tr><td>최초 제출일</td><td>2024년 12월 9일</td></tr></table>
    """.encode()
    assert families._correction_origin_date(body) == "2024-12-09"


def test_correction_origin_rejects_invalid_calendar_date():
    body = """
      <table><tr><td>최초 제출일</td><td>2025년 2월 30일</td></tr></table>
    """.encode()
    with pytest.raises(RuntimeError, match="missing/ambiguous"):
        families._correction_origin_date(body)


def test_live_attachment_main_binds_family_att_dcm_and_dtd_exactly():
    attachment = families.parse_official_dart_main_page(
        "20241209000064",
        _dart_fixture("20241209000064-attachment-main.html").read_bytes(),
        expected_attachment_only=True,
    )
    root = families.parse_official_dart_main_page(
        "20241209000042",
        _dart_fixture("20241209000042-family-main.html").read_bytes(),
        expected_attachment_only=False,
    )

    assert attachment.current_selector == "ATTACHMENT"
    assert attachment.family_receipts == ("20241209000042",)
    assert attachment.family_root_receipt_no == "20241209000042"
    assert attachment.dcm_no == "10216564"
    assert attachment.dtd == "dart4.xsd"
    assert attachment.attachment_keys == (
        "20241209000064:10216564",
        "20241209000042:10216522",
        "20241209000042:10216523",
    )
    assert root.current_selector == "FAMILY"
    assert root.family_receipts == attachment.family_receipts
    assert root.attachment_keys == attachment.attachment_keys
    assert root.dtd == "dart4.xsd"
    assert "dtd=dart4.xsd" in families.official_dart_viewer_url(
        attachment.receipt_no, attachment.dcm_no, attachment.dtd,
    )


def test_attachment_bonus_end_to_end_uses_official_dtd_and_no_fake_origin(
    tmp_path,
):
    root_receipt = "20241209000042"
    attachment_receipt = "20241209000064"
    rows = [
        _bonus_row(root_receipt),
        _bonus_row(
            attachment_receipt,
            report="[첨부정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(
        tmp_path, rows, start="20241201", end="20241231",
    )
    _write_bonus_structured(
        tmp_path, root_receipt, ratio="0.25", effective="20241231",
    )
    mains = {
        root_receipt: _dart_fixture(
            "20241209000042-family-main.html"
        ).read_bytes(),
        attachment_receipt: _dart_fixture(
            "20241209000064-attachment-main.html"
        ).read_bytes(),
    }
    attachment_body = _dart_fixture(
        "20241209000064-attachment-dart4.html"
    ).read_bytes()
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return mains[receipt]
        assert query["dtd"] == ["dart4.xsd"]
        if receipt == attachment_receipt:
            return attachment_body
        return b"<html><body>official bonus decision</body></html>"

    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=fetch,
    )

    entry = verified.entries[0]
    assert entry.ordered_family_receipts == (
        attachment_receipt, root_receipt,
    )
    assert entry.root_receipt_no == root_receipt
    assert entry.terminal_receipt_no == attachment_receipt
    assert entry.terminal_economic_receipt_no == root_receipt
    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_ratio == pytest.approx(0.25)
    attachment_source = entry.sources[0]
    assert attachment_source.current_selector == "ATTACHMENT"
    assert attachment_source.correction_origin_date is None
    assert attachment_source.correction_of_receipt_no is None
    assert (
        attachment_source.attachment_family_root_receipt_no == root_receipt
    )
    assert attachment_source.dtd == "dart4.xsd"
    assert attachment_source.body_sha256 == families._sha256_bytes(
        attachment_body
    )
    assert all("dtd=HTML" not in url for url in calls)


@pytest.mark.parametrize("corruption", ["dual_selected", "dcm", "dtd"])
def test_attachment_main_corruption_fails_closed(corruption):
    receipt = "20241209000064"
    rendered = _dart_fixture(
        "20241209000064-attachment-main.html"
    ).read_text(encoding="utf-8")
    if corruption == "dual_selected":
        rendered = rendered.replace(
            'value="rcpNo=20241209000042" title=',
            'value="rcpNo=20241209000042" selected title=',
        )
    elif corruption == "dcm":
        rendered = rendered.replace(
            "dcmNo=10216564\" selected", "dcmNo=10216565\" selected",
        )
    else:
        rendered = rendered.replace('"dart4.xsd", ""', '"", ""')

    with pytest.raises(RuntimeError):
        families.parse_official_dart_main_page(
            receipt,
            rendered.encode(),
            expected_attachment_only=True,
        )


def test_family_pages_with_different_attachment_keys_fail_closed(tmp_path):
    root_receipt = "20241209000042"
    attachment_receipt = "20241209000064"
    rows = [
        _bonus_row(root_receipt),
        _bonus_row(
            attachment_receipt,
            report="[첨부정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(
        tmp_path, rows, start="20241201", end="20241231",
    )
    _write_bonus_structured(tmp_path, root_receipt, ratio="0.25")
    attachment_main = _dart_fixture(
        "20241209000064-attachment-main.html"
    ).read_bytes()
    root_main = _dart_fixture(
        "20241209000042-family-main.html"
    ).read_text(encoding="utf-8").replace(
        '<option value="rcpNo=20241209000042&amp;dcmNo=10216523">'
        '이사회의사록등증빙서류</option>',
        "",
    ).encode()

    def fetch(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return (
                attachment_main if receipt == attachment_receipt else root_main
            )
        return b"<html><body>body</body></html>"

    with pytest.raises(RuntimeError, match="attachment selectors disagree"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=fetch,
        )


def test_dry_run_has_no_network_or_writes(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)

    preview = families.collect_support_action_families(
        tmp_path, apply=False, fetcher=remote,
    )

    assert preview["status"] == "PREVIEW"
    assert preview["candidate_count"] == 2
    assert preview["network_requests_made"] == 0
    assert preview["writes_made"] == 0
    assert remote.calls == []
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()
    assert not (tmp_path / families.OBJECT_ROOT_RELATIVE_PATH).exists()


def test_collects_official_family_and_roundtrip_verifies(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)

    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )

    assert verified.candidate_count == 2
    assert verified.entry_count == 1
    entry = verified.entries[0]
    assert entry.ticker == "090150"
    assert entry.action_type == "stock_dividend"
    assert entry.root_receipt_no == original
    assert entry.terminal_receipt_no == correction
    assert entry.terminal_economic_receipt_no == correction
    assert entry.ordered_family_receipts == (correction, original)
    assert entry.original_submission_date == "2021-12-17"
    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_admissible
    assert entry.terminal_ratio == pytest.approx(0.2)
    assert entry.sources[0].correction_of_receipt_no == original
    assert entry.sources[0].correction_origin_date == "2021-12-17"
    assert entry.sources[1].revision_kind == "ORIGINAL"
    assert all(
        source.main_path.startswith(
            families.OBJECT_ROOT_RELATIVE_PATH.as_posix()
        )
        and source.body_path.startswith(
            families.OBJECT_ROOT_RELATIVE_PATH.as_posix()
        )
        for source in entry.sources
    )
    assert families.verify_support_action_families(tmp_path) == verified
    external = families.external_evidence_paths(tmp_path)
    assert families.MANIFEST_RELATIVE_PATH.as_posix() in external
    assert entry.sources[0].main_path in external
    assert entry.sources[0].body_path in external


def test_same_date_independent_roots_are_not_merged(tmp_path):
    root_one = "20211217000001"
    root_two = "20211217000002"
    rows = [
        _stock_row(root_one, ticker="000001"),
        _stock_row(root_two, ticker="000001"),
    ]
    _write_interval(tmp_path, rows)
    family_one = (root_one,)
    family_two = (root_two,)
    remote = _Remote(
        {
            root_one: family_one,
            root_two: family_two,
        },
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            root_one: _stock_body("0.1"),
            root_two: _stock_body("0.3"),
        },
    )

    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )

    assert verified.entry_count == 2
    assert {entry.root_receipt_no for entry in verified.entries} == {
        root_one, root_two,
    }
    assert {
        entry.root_receipt_no: entry.ordered_family_receipts
        for entry in verified.entries
    } == {
        root_one: family_one,
        root_two: family_two,
    }


def test_same_date_multiple_originals_make_correction_origin_ambiguous(tmp_path):
    root_one = "20211217000001"
    root_two = "20211217000002"
    correction = "20211224000003"
    rows = [
        _stock_row(root_one, ticker="000001"),
        _stock_row(root_two, ticker="000001"),
        _stock_row(
            correction,
            ticker="000001",
            report="[기재정정]주식배당결정",
        ),
    ]
    _write_interval(tmp_path, rows)
    family_one = (correction, root_one)
    family_two = (root_two,)
    remote = _Remote(
        {
            root_one: family_one,
            correction: family_one,
            root_two: family_two,
        },
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            root_one: _stock_body("0.1"),
            correction: _stock_body("0.2", origin="2021-12-17"),
            root_two: _stock_body("0.3"),
        },
    )

    with pytest.raises(RuntimeError, match="one global original"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=remote,
        )


def test_active_bonus_binds_exact_terminal_structured_row(tmp_path):
    original = "20211217000406"
    correction = "20211224000781"
    rows = [
        _bonus_row(original),
        _bonus_row(correction, report="[기재정정]무상증자결정"),
    ]
    _write_interval(tmp_path, rows)
    structured = _write_bonus_structured(
        tmp_path, correction, ratio="1.0", effective="20211229",
    )
    order = (correction, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: b"<html><body>bonus original</body></html>",
            correction: (
                b"<html><body>correction original submission date "
                b"2021-12-17</body></html>"
            ),
        },
    )
    # Use the exact Korean label required by the correction-body parser.
    remote.bodies[correction] = (
        "<html><body>정정 관련 공시서류 제출일 2021-12-17 "
        "무상증자 결정</body></html>"
    ).encode()

    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )

    entry = verified.entries[0]
    assert entry.action_type == "bonus_issue"
    assert entry.terminal_ratio == pytest.approx(1.0)
    assert entry.terminal_economic_receipt_no == correction
    assert entry.sources[0].structured_path == structured.relative_to(
        tmp_path
    ).as_posix()
    assert entry.sources[1].structured_path is None


@pytest.mark.parametrize(
    ("report", "expected_status"),
    [
        ("[철회]주식배당결정", "WITHDRAWN"),
        ("[취소]주식배당결정", "CANCELLED"),
        ("[부결]주식배당결정", "DENIED"),
    ],
)
def test_terminal_termination_is_visible_but_inadmissible(
    tmp_path, report, expected_status,
):
    original = "20211217000001"
    terminal = "20211224000002"
    rows = [_stock_row(original), _stock_row(terminal, report=report)]
    _write_interval(tmp_path, rows)
    order = (terminal, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            terminal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
        },
    )

    entry = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == expected_status
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None
    assert entry.terminal_economic_receipt_no == original


def test_zero_ratio_is_visible_but_inadmissible(tmp_path):
    receipt = "20211217000406"
    row = _bonus_row(receipt)
    _write_interval(tmp_path, [row])
    _write_bonus_structured(tmp_path, receipt, ratio="0")
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: b"<html><body>bonus zero</body></html>"},
    )

    entry = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == "ZERO_RATIO"
    assert entry.terminal_ratio == 0
    assert not entry.terminal_admissible


@pytest.mark.parametrize(
    ("termination_report", "expected_status"),
    [
        ("[철회]주식배당결정", "WITHDRAWN"),
        ("[취소]주식배당결정", "CANCELLED"),
        ("[부결]주식배당결정", "DENIED"),
    ],
)
def test_attachment_only_terminal_inherits_prior_termination(
    tmp_path, termination_report, expected_status,
):
    original = "20211217000001"
    withdrawal = "20211223000002"
    attachment = "20211224000003"
    rows = [
        _stock_row(original),
        _stock_row(withdrawal, report=termination_report),
        _stock_row(attachment, report="[첨부정정]주식배당결정"),
    ]
    _write_interval(tmp_path, rows)
    official_order = (withdrawal, original)
    all_receipts = (attachment, withdrawal, original)
    attachment_key = (attachment, str(1_000_000 + int(attachment[-6:])))
    remote = _Remote(
        {receipt: official_order for receipt in all_receipts},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            withdrawal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
            attachment: (
                "<html>대표이사 등의 확인 첨부정정</html>"
            ).encode(),
        },
        attachments_by_receipt={
            receipt: (attachment_key,) for receipt in all_receipts
        },
    )

    entry = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_receipt_no == attachment
    assert entry.terminal_economic_receipt_no == original
    assert entry.terminal_status == expected_status
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None


def test_alphanumeric_krx_ticker_is_preserved(tmp_path):
    receipt = "20211217000001"
    row = _stock_row(receipt, ticker="0008Z0")
    _write_interval(tmp_path, [row])
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: _stock_body("0.1")},
    )

    entry = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.ticker == "0008Z0"


def test_bonus_disclosure_without_structured_row_is_not_silently_omitted(
    tmp_path,
):
    original = "20211217000406"
    withdrawal = "20211224000781"
    rows = [
        _bonus_row(original),
        _bonus_row(withdrawal, report="[철회]무상증자결정"),
    ]
    _write_interval(tmp_path, rows)
    order = (withdrawal, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: b"<html>bonus original</html>",
            withdrawal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
        },
    )

    preview = families.collect_support_action_families(tmp_path)
    assert preview["candidate_receipts"] == [original, withdrawal]
    with pytest.raises(RuntimeError, match="no fresh structured candidate"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=remote,
        )


def test_bonus_withdrawal_with_original_structured_row_is_visible(tmp_path):
    original = "20211217000406"
    withdrawal = "20211224000781"
    rows = [
        _bonus_row(original),
        _bonus_row(withdrawal, report="[철회]무상증자결정"),
    ]
    _write_interval(tmp_path, rows)
    _write_bonus_structured(tmp_path, original, ratio="1.0")
    order = (withdrawal, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: b"<html>bonus original</html>",
            withdrawal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
        },
    )

    entry = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == "WITHDRAWN"
    assert not entry.terminal_admissible
    assert entry.terminal_economic_receipt_no == original


def test_correction_origin_must_match_official_root(tmp_path):
    _, correction, remote = _stock_fixture(tmp_path)
    remote.bodies[correction] = _stock_body(
        "0.2", origin="2021-12-18",
    )

    with pytest.raises(RuntimeError, match="does not bind the official root"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=remote,
        )
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()


def test_family_member_missing_from_fresh_disclosures_fails_closed(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)
    missing = "20211223000009"
    order = (correction, missing, original)
    for receipt in order:
        remote.families_by_receipt[receipt] = order
    remote.reports[missing] = "[기재정정]주식배당결정"
    remote.bodies[missing] = _stock_body("0.15", origin="2021-12-17")

    with pytest.raises(RuntimeError, match="absent from fresh disclosures"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=remote,
        )


def test_ambiguous_main_metadata_fails_closed(tmp_path):
    _, correction, remote = _stock_fixture(tmp_path)
    original_main = remote.main

    def ambiguous(receipt):
        payload = original_main(receipt).decode()
        if receipt == correction:
            payload = payload.replace(
                f'value="rcpNo={remote.families_by_receipt[receipt][1]}"',
                f'value="rcpNo={remote.families_by_receipt[receipt][1]}" selected',
            )
        return payload.encode()

    remote.main = ambiguous
    with pytest.raises(RuntimeError, match="selection is missing/ambiguous"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=remote,
        )


def test_old_document_completion_marker_is_rejected(tmp_path):
    row = _stock_row("20211217000001")
    _write_interval(tmp_path, [row], marker_version=4)

    with pytest.raises(RuntimeError, match="no documents_complete_v5"):
        families.collect_support_action_families(tmp_path)


def test_same_latest_mutable_disclosure_conflict_fails_closed(tmp_path):
    receipt = "20211217000001"
    original = _stock_row(receipt, rm="original")
    _write_interval(
        tmp_path, [original], start="20211201", end="20211231",
    )
    changed = _stock_row(receipt, rm="changed")
    _write_interval(
        tmp_path, [changed], start="20211215", end="20211231",
    )

    with pytest.raises(RuntimeError, match="same latest coverage end"):
        families.collect_support_action_families(tmp_path)


def test_mutable_disclosure_uses_latest_coverage_and_binds_all_observations(
    tmp_path,
):
    receipt = "20211217000001"
    old = _stock_row(receipt, rm="old-display")
    latest = _stock_row(receipt, rm="latest-display")
    _write_interval(
        tmp_path, [old], start="20211201", end="20211220",
    )
    _write_interval(
        tmp_path, [latest], start="20211215", end="20211231",
    )
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: latest["report_nm"]},
        {receipt: _stock_body("0.1")},
    )
    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )
    source = verified.entries[0].sources[0]
    assert source.disclosure_row_sha256 == families._row_digest(latest)

    old_manifest = (
        tmp_path / "corporate_actions" / "dart" / "manifests"
        / "from=20211201" / "to=20211220" / "disclosures_v3.json"
    )
    mutated = dict(old)
    mutated["rm"] = "another-old-display"
    _write_json(old_manifest, [mutated])
    with pytest.raises(RuntimeError, match="derived manifest row changed"):
        families.verify_support_action_families(tmp_path)


def test_marker_count_parity_is_required(tmp_path):
    row = _stock_row("20211217000001")
    _write_interval(tmp_path, [row])
    marker = (
        tmp_path / "corporate_actions" / "dart" / "manifests"
        / "from=20211201" / "to=20211231"
        / "documents_complete_v5.json"
    )
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["candidate_count"] = 0
    _write_json(marker, payload)

    with pytest.raises(RuntimeError, match="candidate_count parity mismatch"):
        families.collect_support_action_families(tmp_path)


def test_content_addressed_object_path_is_enforced(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)
    families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["entries"][0]["sources"][0]["main_path"] = (
        "corporate_actions/dart/support_action_families/not-content-addressed.html"
    )
    payload["entry_digest"] = families._sha256_bytes(
        families._canonical_bytes(payload["entries"])
    )
    manifest.write_bytes(families._canonical_bytes(payload))

    with pytest.raises(RuntimeError, match="not content-addressed"):
        families.verify_support_action_families(tmp_path)


def test_manifest_must_use_canonical_json_bytes(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)
    families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="not canonical JSON bytes"):
        families.verify_support_action_families(tmp_path)


@pytest.mark.parametrize("target", ["main", "body", "disclosure", "manifest"])
def test_roundtrip_rejects_content_corruption(tmp_path, target):
    _, _, remote = _stock_fixture(tmp_path)
    verified = families.collect_support_action_families(
        tmp_path, apply=True, fetcher=remote,
    )
    source = verified.entries[0].sources[0]
    if target == "main":
        path = tmp_path / source.main_path
        path.write_bytes(path.read_bytes() + b"corrupt")
    elif target == "body":
        path = tmp_path / source.body_path
        path.write_bytes(path.read_bytes() + b"corrupt")
    elif target == "disclosure":
        path = tmp_path / source.disclosure_path
        row = json.loads(path.read_text(encoding="utf-8"))
        row["rm"] = "mutated"
        _write_json(path, row)
    else:
        path = tmp_path / families.MANIFEST_RELATIVE_PATH
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["entries"][0]["terminal_status"] = "WITHDRAWN"
        _write_json(path, payload)

    with pytest.raises(RuntimeError):
        families.verify_support_action_families(tmp_path)


def test_network_failure_never_publishes_partial_manifest(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)

    def failing(url: str) -> bytes:
        if urlparse(url).path.endswith("viewer.do") and (
            parse_qs(urlparse(url).query)["rcpNo"] == [correction]
        ):
            raise RuntimeError("injected network failure")
        return remote(url)

    with pytest.raises(RuntimeError, match="injected network failure"):
        families.collect_support_action_families(
            tmp_path, apply=True, fetcher=failing,
        )
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()
    assert not list(
        (tmp_path / families.MANIFEST_RELATIVE_PATH.parent).glob(".manifest.*")
    )
