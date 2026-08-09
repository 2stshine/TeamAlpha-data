from datetime import date
import json

import pandas as pd

from pipeline.silver import financials
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver_quality.registry import run_registered_rules
from pipeline.silver_quality.rules.financials import (
    FUNDAMENTAL_KEYS,
    check_financials,
)
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
    exclusion = next(
        result for result in results
        if result.rule_code == "NO_TRADABLE_PRICE_ASSET"
    )
    assert exclusion.failed_count == 0
    assert exclusion.status.value == "PASS"
    assert exclusion.severity.value == "MODIFIED"
    assert exclusion.samples[0]["identifier"] == "016830"
    assert "excluded_rows=3" in exclusion.actual
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


def test_full_statement_only_fills_missing_primary_metrics(tmp_path):
    primary = _write_dart_file(
        tmp_path,
        [_dart_row(ord_value="1", account_nm="유동자산")],
    )
    supplemental = (
        tmp_path
        / "financials"
        / "dart_full"
        / "year=2026"
        / "corp=005930"
        / "11011-CFS.json"
    )
    supplemental.parent.mkdir(parents=True)
    supplemental_rows = [
        _dart_row(ord_value="1", account_nm="유동자산"),
        _dart_row(ord_value="5", account_nm="자산총계"),
        _dart_row(ord_value="9", account_nm="당기순이익"),
    ]
    for row in supplemental_rows:
        row["sj_div"] = (
            "BS" if row["account_nm"] in {"유동자산", "자산총계"} else "CF"
        )
        row.pop("stock_code")
        row.pop("fs_div")
        row.pop("thstrm_dt")
    supplemental.write_text(
        json.dumps(supplemental_rows, ensure_ascii=False),
        encoding="utf-8",
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(primary), str(supplemental)],
    )

    assert sorted(frame["metric"]) == ["current_assets", "total_assets"]
    assert stats["full_statement_supplement"]["row_count"] == 1
    assert stats["excluded_rows"] == 2
    assert stats["input_rows"] == (
        stats["transformed_rows"]
        + stats["excluded_rows"]
        + stats["rejected_rows"]
    )


def test_balancing_full_statement_atomically_replaces_bad_primary_triple(
    tmp_path,
):
    primary_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(primary_rows, ("100", "80", "10"), strict=True):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
    primary = _write_dart_file(tmp_path, primary_rows)

    supplemental = (
        tmp_path
        / "financials"
        / "dart_full"
        / "year=2026"
        / "corp=005930"
        / "11011-CFS.json"
    )
    supplemental.parent.mkdir(parents=True)
    supplemental_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(
        supplemental_rows,
        ("100", "60", "40"),
        strict=True,
    ):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
        row.pop("stock_code")
        row.pop("fs_div")
    supplemental.write_text(
        json.dumps(supplemental_rows, ensure_ascii=False),
        encoding="utf-8",
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(primary), str(supplemental)],
    )

    values = frame.set_index("metric")["value"].to_dict()
    assert values["total_assets"] == 100
    assert values["total_liabilities"] == 60
    assert values["total_equity"] == 40
    assert set(frame["source_file"]) == {str(supplemental)}
    detail = stats["accounting_equation_supplement_replacement"]
    assert detail["scope_count"] == 1
    assert detail["row_count"] == 3
    assert detail["samples"][0]["before_relative_error"] == 0.1
    assert detail["samples"][0]["after_relative_error"] == 0
    assert stats["input_rows"] == (
        stats["transformed_rows"]
        + stats["excluded_rows"]
        + stats["rejected_rows"]
    )

    results = run_registered_rules(CandidateBundle(
        identifiers=pd.DataFrame([{
            "natural_key": "005930",
            "source": "KRX",
            "identifier": "005930",
        }]),
        fundamentals=frame,
        stats={"fundamental": stats},
    ))
    replacement = next(
        item for item in results
        if item.rule_code
        == "DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT"
    )
    assert replacement.status.value == "PASS"
    assert "replaced_scopes=1" in replacement.actual
    accounting = next(
        item for item in results
        if item.rule_code == "FUNDAMENTAL_ACCOUNTING_EQUATION"
    )
    assert accounting.status.value == "PASS"


def test_unbalanced_full_statement_does_not_replace_primary_values(tmp_path):
    primary_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(primary_rows, ("100", "80", "10"), strict=True):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
    primary = _write_dart_file(tmp_path, primary_rows)
    supplemental = (
        tmp_path
        / "financials"
        / "dart_full"
        / "year=2026"
        / "corp=005930"
        / "11011-CFS.json"
    )
    supplemental.parent.mkdir(parents=True)
    supplemental_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(
        supplemental_rows,
        ("100", "70", "10"),
        strict=True,
    ):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
        row.pop("stock_code")
        row.pop("fs_div")
    supplemental.write_text(
        json.dumps(supplemental_rows, ensure_ascii=False),
        encoding="utf-8",
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(primary), str(supplemental)],
    )

    assert set(frame["source_file"]) == {str(primary)}
    assert stats["accounting_equation_supplement_replacement"]["row_count"] == 0


def test_same_bad_values_from_both_dart_apis_are_source_warning(tmp_path):
    primary_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(primary_rows, ("100", "80", "10"), strict=True):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
    primary = _write_dart_file(tmp_path, primary_rows)
    supplemental = (
        tmp_path
        / "financials"
        / "dart_full"
        / "year=2026"
        / "corp=005930"
        / "11011-CFS.json"
    )
    supplemental.parent.mkdir(parents=True)
    supplemental_rows = [
        _dart_row(ord_value="1", account_nm="자산총계"),
        _dart_row(ord_value="2", account_nm="부채총계"),
        _dart_row(ord_value="3", account_nm="자본총계"),
    ]
    for row, amount in zip(
        supplemental_rows,
        ("100", "80", "10"),
        strict=True,
    ):
        row["sj_div"] = "BS"
        row["thstrm_amount"] = amount
        row.pop("stock_code")
        row.pop("fs_div")
    supplemental.write_text(
        json.dumps(supplemental_rows, ensure_ascii=False),
        encoding="utf-8",
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(primary), str(supplemental)],
    )
    source_issue = stats["source_accounting_inconsistency"]
    assert source_issue["scope_count"] == 1
    assert source_issue["row_count"] == 3
    assert source_issue["samples"][0]["relative_error"] == 0.1

    results = run_registered_rules(CandidateBundle(
        identifiers=pd.DataFrame([{
            "natural_key": "005930",
            "source": "KRX",
            "identifier": "005930",
        }]),
        fundamentals=frame,
        stats={"fundamental": stats},
    ))
    generic = next(
        item for item in results
        if item.rule_code == "FUNDAMENTAL_ACCOUNTING_EQUATION"
    )
    source_warning = next(
        item for item in results
        if item.rule_code == "DART_SOURCE_ACCOUNTING_INCONSISTENCY"
    )
    assert generic.status.value == "PASS"
    assert source_warning.status.value == "FAIL"
    assert source_warning.failed_count == 1
    assert not source_warning.blocks_publish


def test_full_statement_is_cis_net_income_duplicate_prefers_is(tmp_path):
    primary = _write_dart_file(
        tmp_path,
        [_dart_row(ord_value="1", account_nm="유동자산")],
    )
    supplemental = (
        tmp_path
        / "financials"
        / "dart_full"
        / "year=2026"
        / "corp=005930"
        / "11011-CFS.json"
    )
    supplemental.parent.mkdir(parents=True)
    is_row = _dart_row(ord_value="15", account_nm="당기순이익(손실)")
    is_row["sj_div"] = "IS"
    is_row["sj_nm"] = "손익계산서"
    cis_row = dict(is_row)
    cis_row["sj_div"] = "CIS"
    cis_row["sj_nm"] = "포괄손익계산서"
    cis_row["ord"] = "0"
    for row in (is_row, cis_row):
        row.pop("stock_code")
        row.pop("fs_div")
    supplemental.write_text(
        json.dumps([is_row, cis_row], ensure_ascii=False),
        encoding="utf-8",
    )

    frame, stats = financials.prepare(
        str(tmp_path),
        files=[str(primary), str(supplemental)],
    )

    net_income = frame[frame["metric"].eq("net_income")]
    assert len(net_income) == 1
    assert net_income.iloc[0]["value"] == 100
    known = stats["known_full_statement_presentation_duplicate"]
    assert known["row_count"] == 1
    assert known["group_count"] == 1
    assert known["samples"][0]["selected_statement"] == "IS"
    assert known["samples"][0]["metric"] == "net_income"
    assert known["samples"][0]["value"] == 100
    assert stats["unexpected_exact_duplicate"]["row_count"] == 0
    assert stats["input_rows"] == (
        stats["transformed_rows"]
        + stats["excluded_rows"]
        + stats["rejected_rows"]
    )

    reconciliation = check_reconciliation({"fundamental": stats})
    result = next(
        item for item in reconciliation
        if item.rule_code
        == "DART_FULL_STATEMENT_PRESENTATION_DUPLICATE"
    )
    assert result.status.value == "PASS"
    assert "duplicate_groups=1" in result.actual


def test_operating_income_presentation_conflict_keeps_first_line(tmp_path):
    # One filing presents operating income twice with different values via dual
    # income-statement formats (general vs financial-industry). Keep the
    # first-presented (min ord) line, drop the other; no blocking duplicate.
    primary = _dart_row(ord_value="25", account_nm="영업이익")
    primary["thstrm_amount"] = "1000"
    secondary = _dart_row(ord_value="59", account_nm="영업이익(손실)")
    secondary["thstrm_amount"] = "400"
    path = _write_dart_file(tmp_path, [primary, secondary])

    frame, stats = financials.prepare(str(tmp_path), files=[str(path)])

    op = frame[frame["metric"] == "operating_income"]
    assert len(op) == 1
    assert float(op.iloc[0]["value"]) == 1000.0
    conflict = stats["presentation_conflict_resolved"]
    assert conflict["row_count"] == 1
    assert conflict["group_count"] == 1
    assert not frame.duplicated(subset=FUNDAMENTAL_KEYS).any()
