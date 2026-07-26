from datetime import date
import json

import pandas as pd

from pipeline.silver import financials
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver_quality.registry import run_registered_rules
from pipeline.silver_quality.rules.financials import check_financials
from pipeline.silver_quality.rules.reconciliation import check_reconciliation


def _frame():
    base = {
        "identifier": "005930",
        "source": "DART",
        "period_end": date(2026, 3, 31),
        "fiscal_period": "Q1",
        "fs_type": "CFS",
        "filing_id": "20260515000001",
        "filed": date(2026, 5, 15),
        "available_date": date(2026, 5, 16),
        "currency": "KRW",
        "revision_key": "20260515000001",
    }
    return pd.DataFrame([
        {**base, "metric": "total_assets", "value": 100.0},
        {**base, "metric": "total_liabilities", "value": 60.0},
        {**base, "metric": "total_equity", "value": 40.0},
    ])


def test_valid_financial_passes_blocking_rules():
    results = check_financials(_frame())
    assert not any(result.blocks_publish for result in results)


def test_point_in_time_violation_blocks():
    frame = _frame()
    frame["available_date"] = date(2026, 5, 15)
    result = next(
        r for r in check_financials(frame)
        if r.rule_code == "FUNDAMENTAL_PIT_ORDER"
    )
    assert result.blocks_publish


def test_accounting_difference_is_warning():
    frame = _frame()
    frame.loc[frame["metric"] == "total_equity", "value"] = 10.0
    result = next(
        r for r in check_financials(frame)
        if r.rule_code == "FUNDAMENTAL_ACCOUNTING_EQUATION"
    )
    assert result.failed_count == 1
    assert "affected_rows=3" in result.actual
    assert not result.blocks_publish


def test_dart_only_rows_are_explicitly_excluded_and_reported():
    frame = pd.concat([
        _frame(),
        _frame().assign(identifier="016830"),
        _frame().assign(identifier="250030"),
    ], ignore_index=True)
    stats = {
        "input_rows": len(frame),
        "transformed_rows": len(frame),
        "excluded_rows": 0,
        "rejected_rows": 0,
    }
    retained, updated = financials.exclude_nontradable(
        frame, stats, {"005930"}, {"250030"},
    )
    assert set(retained["identifier"]) == {"005930"}
    assert updated["transformed_rows"] == 3
    assert updated["excluded_rows"] == 6
    detail = updated["no_tradable_price_asset"]
    assert detail["row_count"] == 3
    assert detail["ticker_count"] == 1
    assert detail["samples"][0]["identifier"] == "016830"
    unsupported = updated["unsupported_market_asset"]
    assert unsupported["row_count"] == 3
    assert unsupported["ticker_count"] == 1
    assert unsupported["samples"][0]["identifier"] == "250030"

    identifiers = pd.DataFrame([{
        "natural_key": "005930",
        "source": "KRX",
        "identifier": "005930",
    }])
    results = run_registered_rules(CandidateBundle(
        identifiers=identifiers,
        fundamentals=retained,
        stats={"fundamental": updated},
    ))
    warning = next(
        result for result in results
        if result.rule_code == "NO_TRADABLE_PRICE_ASSET"
    )
    assert warning.failed_count == 3
    assert warning.samples[0]["identifier"] == "016830"
    assert not warning.blocks_publish
    excluded_market = next(
        result for result in results
        if result.rule_code == "UNSUPPORTED_MARKET_ASSET_EXCLUDED"
    )
    assert excluded_market.status.value == "PASS"
    assert "excluded_rows=3" in excluded_market.actual


def _dart_row(*, ord_value: str, account_nm: str = "당기순이익(손실)"):
    return {
        "account_nm": account_nm,
        "account_id": None,
        "bsns_year": "2026",
        "corp_code": "00126380",
        "currency": "KRW",
        "frmtrm_amount": "90",
        "frmtrm_dt": "2025.01.01 ~ 2025.12.31",
        "fs_div": "CFS",
        "fs_nm": "연결재무제표",
        "ord": ord_value,
        "rcept_no": "20260331000001",
        "reprt_code": "11011",
        "sj_div": "IS",
        "sj_nm": "손익계산서",
        "stock_code": "005930",
        "thstrm_amount": "100",
        "thstrm_dt": "2026.01.01 ~ 2026.12.31",
    }


def _write_dart_file(tmp_path, rows):
    path = (
        tmp_path
        / "financials"
        / "dart"
        / "year=2026"
        / "corp=005930"
        / "11011.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return path


def test_known_dart_net_income_ord_duplicate_keeps_smallest_ord(tmp_path):
    path = _write_dart_file(tmp_path, [
        _dart_row(ord_value="61"),
        _dart_row(ord_value="29"),
    ])

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(path)],
    )

    assert len(frame) == 1
    assert frame.iloc[0]["metric"] == "net_income"
    known = stats["known_net_income_ord_duplicate"]
    assert known["row_count"] == 1
    assert known["group_count"] == 1
    assert known["samples"][0]["selected_ord"] == "29"
    assert stats["unexpected_exact_duplicate"]["row_count"] == 0
    assert stats["input_rows"] == (
        stats["transformed_rows"]
        + stats["excluded_rows"]
        + stats["rejected_rows"]
    )

    results = check_reconciliation({"fundamental": stats})
    known_rule = next(
        item for item in results
        if item.rule_code == "DART_NET_INCOME_ORD_DUPLICATE"
    )
    unexpected_rule = next(
        item for item in results
        if item.rule_code == "DART_UNEXPECTED_EXACT_DUPLICATE"
    )
    assert known_rule.status.value == "PASS"
    assert known_rule.failed_count == 0
    assert unexpected_rule.status.value == "PASS"


def test_unexpected_dart_duplicate_is_preserved_and_blocks(tmp_path):
    first = _dart_row(ord_value="29")
    second = _dart_row(ord_value="61")
    second["frmtrm_amount"] = "91"
    path = _write_dart_file(tmp_path, [first, second])

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(path)],
    )

    assert len(frame) == 2
    unexpected = stats["unexpected_exact_duplicate"]
    assert unexpected["row_count"] == 2
    assert unexpected["group_count"] == 1
    assert stats["known_net_income_ord_duplicate"]["row_count"] == 0

    reconciliation = check_reconciliation({"fundamental": stats})
    unexpected_rule = next(
        item for item in reconciliation
        if item.rule_code == "DART_UNEXPECTED_EXACT_DUPLICATE"
    )
    assert unexpected_rule.blocks_publish
    duplicate_rule = next(
        item for item in check_financials(frame)
        if item.rule_code == "COMMON_DUPLICATE_KEY"
    )
    assert duplicate_rule.blocks_publish


def test_plain_net_income_label_maps_to_signed_net_income(tmp_path):
    path = _write_dart_file(
        tmp_path,
        [_dart_row(ord_value="29", account_nm="당기순이익")],
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(path)],
    )

    assert len(frame) == 1
    assert frame.iloc[0]["metric"] == "net_income"
    assert frame.iloc[0]["value"] == 100.0
    assert stats["known_net_income_ord_duplicate"]["row_count"] == 0
