import json
from datetime import date

import pytest

from pipeline.silver import dividends
from pipeline.silver import financials
from pipeline.silver_quality.rules.financials import check_financials


def test_dart_annual_dividends_expand_three_pit_revisions(tmp_path):
    path = (
        tmp_path / "dividends/dart/alot-matter/year=2020/report=11011"
        / "corp=005930/rcept=20210317000578/response.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"list": [
        {
            "rcept_no": "20210317000578", "corp_code": "00126380",
            "se": "주당 현금배당금(원)", "stock_knd": "보통주",
            "thstrm": "2,994", "frmtrm": "1,416", "lwfr": "1,416",
            "stlm_dt": "2020-12-31",
        },
        {
            "rcept_no": "20210317000578", "corp_code": "00126380",
            "se": "주당 현금배당금(원)", "stock_knd": "우선주",
            "thstrm": "2,995", "frmtrm": "1,417", "lwfr": "1,417",
            "stlm_dt": "2020-12-31",
        },
        {
            "rcept_no": "20210317000578", "corp_code": "00126380",
            "se": "현금배당금총액(백만원)", "stock_knd": "",
            "thstrm": "20", "frmtrm": "10", "lwfr": "-",
            "stlm_dt": "2020-12-31",
        },
    ]}, ensure_ascii=False), encoding="utf-8")

    frame, stats = dividends.prepare(str(tmp_path))

    assert len(frame) == 5
    assert set(frame["statement_type"]) == {"DIVIDEND"}
    assert set(frame["data_basis"]) == {"REPORTED"}
    assert set(frame["fs_type"]) == {"UNKNOWN"}
    current = frame[
        frame["metric"].eq("cash_dividend_per_share")
        & frame["period_end"].eq(date(2020, 12, 31))
    ].iloc[0]
    assert current["value"] == pytest.approx(2_994)
    total = frame[
        frame["metric"].eq("total_cash_dividend")
        & frame["period_end"].eq(date(2020, 12, 31))
    ].iloc[0]
    assert total["value"] == pytest.approx(20_000_000)
    assert current["available_date"] == date(2021, 3, 18)
    assert stats["input_rows"] == stats["transformed_rows"] + stats["excluded_rows"]
    assert not any(result.blocks_publish for result in check_financials(frame))


def test_empty_changed_financial_file_list_keeps_candidate_schema(tmp_path):
    frame, stats = financials.prepare(str(tmp_path), files=[])
    assert frame.empty
    assert {"metric", "statement_type", "data_basis", "unit_type"} <= set(frame)
    assert stats["input_rows"] == 0
