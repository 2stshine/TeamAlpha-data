from pipeline.bronze.dart_disclosure_observations import (
    immutable_disclosure_changes,
)


def test_display_only_disclosure_changes_are_not_immutable():
    original = {
        "rcept_no": "20191210000064",
        "stock_code": "299900",
        "report_nm": "주요사항보고서(무상증자결정)",
        "corp_cls": "K",
    }
    current = {**original, "corp_cls": "E"}

    assert immutable_disclosure_changes(original, current) == ()


def test_economic_identity_disclosure_changes_remain_blocked():
    original = {
        "rcept_no": "20191210000064",
        "stock_code": "299900",
        "report_nm": "주요사항보고서(무상증자결정)",
        "corp_cls": "K",
    }
    changed = {**original, "stock_code": "000000"}

    assert immutable_disclosure_changes(original, changed) == ("stock_code",)
