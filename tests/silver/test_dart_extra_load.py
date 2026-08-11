from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest

from pipeline.silver.dividend_evidence import (
    assert_verified_cash_evidence,
    invalid_cash_evidence_mask,
)
from pipeline.silver.dart_extra_load import (
    _manifest_support_action_candidates,
    _published_action_contract,
    _source_receipt_frame,
    _source_receipt_stats,
    _total_return_actions,
    parse_args,
    run,
)


def _kind_scale_evidence(*, conflicting=False):
    common = {
        "support_action_source": "KRX_KIND",
        "support_action_key": "20181220002252",
        "support_action_type": "stock_dividend",
        "support_action_body_path": "corporate_actions/krx/kind/cj.html",
        "support_action_body_sha256": "a" * 64,
        "support_announcement_date": date(2018, 12, 20),
        "support_ex_date": None,
        "support_record_date": date(2018, 12, 31),
        "support_ratio_numerator": 0.15,
        "support_ratio_denominator": 1.0,
        "support_entitlement_security_class": "COMMON_AND_PREFERRED",
        "support_distributed_security_class": "NEW_PREFERRED",
        "support_expected_price_factor": None,
        "support_reference_price": None,
        "support_reason": None,
        "support_report_name": "주식배당 결정",
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
    }
    first = {
        **common, "evidence_key": "first",
        "target_cash_receipt_no": "20190211800997",
        "target_adjustment_date": date(2018, 12, 27),
    }
    second = {
        **common,
        "evidence_key": "second",
        "target_cash_receipt_no": "20190211800998",
        "target_adjustment_date": date(2018, 12, 27),
        "support_ratio_numerator": 0.20 if conflicting else 0.15,
    }
    return SimpleNamespace(
        frame=pd.DataFrame([
            {
                "evidence_key": "first", "ticker": "001040",
                "cash_receipt_no": "20190211800997",
                "adjustment_trade_date": date(2018, 12, 27),
            },
            {
                "evidence_key": "second", "ticker": "001040",
                "cash_receipt_no": "20190211800998",
                "adjustment_trade_date": date(2018, 12, 27),
            },
        ]),
        support_frame=pd.DataFrame([first, second]),
    )


def test_reused_kind_support_is_published_once_but_lineage_stays_per_parent():
    scale_evidence = _kind_scale_evidence()

    result = _manifest_support_action_candidates(scale_evidence)

    assert len(scale_evidence.support_frame) == 2
    assert len(result) == 1
    assert result.iloc[0]["rcept_no"] == "20181220002252"
    assert result.iloc[0]["ratio_numerator"] == pytest.approx(0.15)


def test_reused_kind_support_with_conflicting_semantics_fails_closed():
    with pytest.raises(RuntimeError, match="conflicting immutable semantics"):
        _manifest_support_action_candidates(
            _kind_scale_evidence(conflicting=True)
        )


def test_total_return_action_scope_is_minimal_and_issuer_only():
    frame = pd.DataFrame([
        {"source": "DART_DISCLOSURE", "event_type": "cash_dividend", "action_scope": "ISSUER", "key": 1},
        {"source": "DART_DISCLOSURE", "event_type": "ex_dividend", "action_scope": "ISSUER", "key": 2},
        {"source": "DART_DISCLOSURE", "event_type": "stock_split", "action_scope": "ISSUER", "key": 3},
        {
            "source": "DART_DISCLOSURE",
            "event_type": "cash_dividend",
            "action_scope": "RELATED_COMPANY",
            "key": 4,
        },
        {"source": "DART_DISCLOSURE", "event_type": "cash_dividend", "action_scope": "UNKNOWN", "key": 5},
        {"source": "DART_STRUCTURED", "event_type": "cash_dividend", "action_scope": "ISSUER", "key": 6},
    ])
    frame["rcept_no"] = frame["key"].astype(str)
    scale_evidence = SimpleNamespace(support_frame=pd.DataFrame(columns=[
        "support_action_source", "support_action_key", "support_action_type",
    ]))

    result = _total_return_actions(frame, scale_evidence)

    assert result["key"].tolist() == [1, 2]


def _cash_receipt(
    receipt, *, mapping_status, excluded_reason, asset_id, corp_cls,
    ticker="230360",
):
    return {
        "identifier": ticker,
        "event_type": "cash_dividend",
        "announcement_date": date(2025, 2, 28),
        "effective_date": None,
        "match_window_days": 0,
        "expected_factor": None,
        "share_count_factor": None,
        "share_count_before": None,
        "share_count_after": None,
        "share_count_factor_comparable": False,
        "share_count_comparison_reason": None,
        "action_method": None,
        "record_date": date(2025, 3, 19),
        "payment_date": None,
        "cash_amount": 150.0,
        "adjusted_cash_amount": None,
        "currency": "KRW",
        "frequency": None,
        "confirms_price_adjustment": False,
        "expects_price_adjustment": False,
        "confidence": "ANNOUNCEMENT_ONLY",
        "rcept_no": receipt,
        "report_name": "현금ㆍ현물배당결정",
        "dart_rm": None,
        "corp_cls": corp_cls,
        "action_scope": "ISSUER",
        "cash_amount_status": "POSITIVE",
        "source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
        "correction_of_action_key": None,
        "revision_root_action_key": receipt,
        "revision_kind": "ECONOMIC_REVISION",
        "viewer_evidence_sha256": None,
        "economic_evidence_sha256": "a" * 64,
        "reviewed_correction_id": None,
        "payment_date_quality_status": None,
        "source": "DART_DISCLOSURE",
        "source_file": "fixture",
        "asset_id": asset_id,
        "pit_event_date": date(2025, 3, 19),
        "pit_mapping_status": mapping_status,
        "pit_excluded_reason": excluded_reason,
    }


def test_full_source_receipt_preserves_included_and_excluded_rows():
    run_id = uuid4()
    actions = pd.DataFrame([
        _cash_receipt(
            "20250228801790", mapping_status="INCLUDED", excluded_reason=None,
            asset_id=1, corp_cls="E",
        ),
        _cash_receipt(
            "20250228801791", mapping_status="EXCLUDED",
            excluded_reason="NO_EVENT_DATE_PIT_IDENTITY",
            asset_id=None, corp_cls="N",
        ),
    ])

    receipts = _source_receipt_frame(actions, quality_run_id=run_id)
    stats = _source_receipt_stats(receipts)

    assert receipts["receipt_no"].tolist() == [
        "20250228801790", "20250228801791",
    ]
    assert receipts.set_index("receipt_no").loc[
        "20250228801791", "asset_id"
    ] != 1
    assert stats["source_cash_receipt_count"] == 2
    assert stats["included_cash_receipt_count"] == 1
    assert stats["excluded_cash_receipt_count"] == 1
    assert stats["included_cash_receipts_by_corp_cls"] == {"E": 1}
    assert stats["excluded_cash_receipts_by_corp_cls"] == {"N": 1}
    assert stats["cash_receipt_exclusion_reasons"] == {
        "NO_EVENT_DATE_PIT_IDENTITY": 1,
    }


def test_source_receipt_partition_fails_closed_without_exclusion_reason():
    action = pd.DataFrame([_cash_receipt(
        "20250228801791", mapping_status="EXCLUDED", excluded_reason=None,
        asset_id=None, corp_cls="N",
    )])

    with pytest.raises(RuntimeError, match="invalid DART cash receipt"):
        _source_receipt_frame(action, quality_run_id=uuid4())


@pytest.mark.parametrize(
    ("source_status", "economic_sha"),
    [("FAKE", "a" * 64), ("VERIFIED_OPENDART_DOCUMENT", None)],
)
def test_source_receipt_requires_exact_status_evidence_pair(
    source_status,
    economic_sha,
):
    row = _cash_receipt(
        "20250228801792", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    row["source_evidence_status"] = source_status
    row["economic_evidence_sha256"] = economic_sha

    with pytest.raises(RuntimeError, match="exact evidence contract"):
        _source_receipt_frame(pd.DataFrame([row]), quality_run_id=uuid4())


def test_opendart_evidence_rejects_an_unexpected_viewer_sha():
    row = _cash_receipt(
        "20250228801793", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    row["viewer_evidence_sha256"] = "b" * 64

    with pytest.raises(RuntimeError, match="exact evidence contract"):
        _source_receipt_frame(pd.DataFrame([row]), quality_run_id=uuid4())


def test_viewer_evidence_requires_identical_viewer_and_economic_sha():
    row = _cash_receipt(
        "20250228801794", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    row["source_evidence_status"] = "VERIFIED_DART_VIEWER_BODY"
    row["viewer_evidence_sha256"] = "b" * 64
    row["economic_evidence_sha256"] = "c" * 64

    with pytest.raises(RuntimeError, match="exact evidence contract"):
        _source_receipt_frame(pd.DataFrame([row]), quality_run_id=uuid4())


def test_attachment_evidence_requires_distinct_source_and_economic_bodies():
    prior = _cash_receipt(
        "20250228801794", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    row = _cash_receipt(
        "20250228801795", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    row.update({
        "source_evidence_status": "VERIFIED_ATTACHMENT_CORRECTION",
        "cash_amount_status": "ATTACHMENT_ONLY",
        "revision_kind": "ATTACHMENT_ONLY",
        "correction_of_action_key": "20250228801794",
        "revision_root_action_key": "20250228801794",
        "viewer_evidence_sha256": "b" * 64,
        "economic_evidence_sha256": "c" * 64,
        "record_date": None,
        "cash_amount": None,
    })

    receipts = _source_receipt_frame(
        pd.DataFrame([prior, row]), quality_run_id=uuid4(),
    )

    assert receipts.set_index("receipt_no").loc[
        "20250228801795", "source_evidence_status"
    ] == "VERIFIED_ATTACHMENT_CORRECTION"

    row["economic_evidence_sha256"] = row["viewer_evidence_sha256"]
    with pytest.raises(RuntimeError, match="exact evidence contract"):
        _source_receipt_frame(
            pd.DataFrame([prior, row]), quality_run_id=uuid4(),
        )


@pytest.mark.parametrize("corruption", ["missing", "different_root", "ticker"])
def test_attachment_evidence_requires_same_ticker_root_predecessor(corruption):
    prior = _cash_receipt(
        "20250228801794", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    attachment = _cash_receipt(
        "20250228801795", mapping_status="INCLUDED", excluded_reason=None,
        asset_id=1, corp_cls="Y",
    )
    attachment.update({
        "source_evidence_status": "VERIFIED_ATTACHMENT_CORRECTION",
        "cash_amount_status": "ATTACHMENT_ONLY",
        "revision_kind": "ATTACHMENT_ONLY",
        "correction_of_action_key": "20250228809999"
        if corruption == "missing" else "20250228801794",
        "revision_root_action_key": "20250228809998"
        if corruption == "different_root" else "20250228801794",
        "viewer_evidence_sha256": "b" * 64,
        "economic_evidence_sha256": "c" * 64,
        "record_date": None,
        "cash_amount": None,
    })
    if corruption == "ticker":
        attachment["identifier"] = "230361"

    with pytest.raises(RuntimeError, match="exact evidence contract"):
        assert_verified_cash_evidence(
            pd.DataFrame([prior, attachment]),
            action_key_column="rcept_no",
            root_key_column="revision_root_action_key",
        )


def test_actual_preview_receipt_shapes_satisfy_exact_evidence_contract():
    no_common = {
        "20150227801008": "VERIFIED_OPENDART_DOCUMENT",
        "20160331600430": "VERIFIED_DART_VIEWER_BODY",
        "20170323800201": "VERIFIED_DART_VIEWER_BODY",
        "20180330604261": "VERIFIED_DART_VIEWER_BODY",
        "20210304800912": "VERIFIED_OPENDART_DOCUMENT",
        "20230313801108": "VERIFIED_OPENDART_DOCUMENT",
        "20230926800901": "VERIFIED_DART_VIEWER_BODY",
    }
    pending = (
        "20240129800952", "20240131801052", "20240207800734",
        "20240207801399", "20250123800748", "20250204801062",
        "20250206800745", "20250206800758", "20260122800397",
        "20260130801398", "20260211801212", "20260224900442",
        "20260313902184",
    )
    rows = []
    for receipt, source_status in no_common.items():
        viewer_sha = "b" * 64 if "VIEWER" in source_status else None
        rows.append({
            "receipt_no": receipt,
            "revision_root_receipt_no": receipt,
            "previous_receipt_no": None,
            "source_evidence_status": source_status,
            "cash_amount_status": "NO_COMMON_CASH_DIVIDEND",
            "revision_kind": "ECONOMIC_REVISION",
            "viewer_evidence_sha256": viewer_sha,
            "economic_evidence_sha256": viewer_sha or "a" * 64,
            "reviewed_correction_id": None,
            "record_date": date(2025, 1, 1),
            "cash_amount": None,
        })
    for receipt in pending:
        rows.append({
            "receipt_no": receipt,
            "revision_root_receipt_no": receipt,
            "previous_receipt_no": None,
            "source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
            "cash_amount_status": "POSITIVE_PENDING_RECORD_DATE",
            "revision_kind": "ORIGINAL_DECISION",
            "viewer_evidence_sha256": None,
            "economic_evidence_sha256": "a" * 64,
            "reviewed_correction_id": None,
            "record_date": None,
            "cash_amount": 100.0,
        })
    for receipt, record_date in (
        ("20160224900227", date(2015, 12, 31)),
        ("20170316900231", date(2016, 12, 31)),
    ):
        rows.append({
            "receipt_no": receipt,
            "revision_root_receipt_no": receipt,
            "previous_receipt_no": None,
            "source_evidence_status": "VERIFIED_REVIEWED_SOURCE_ERRATUM",
            "cash_amount_status": "POSITIVE",
            "revision_kind": "ORIGINAL_DECISION",
            "viewer_evidence_sha256": None,
            "economic_evidence_sha256": "d" * 64,
            "reviewed_correction_id": f"093320-{receipt}-record-date",
            "record_date": record_date,
            "cash_amount": 100.0,
        })
    frame = pd.DataFrame(rows)

    invalid = invalid_cash_evidence_mask(
        frame,
        action_key_column="receipt_no",
        root_key_column="revision_root_receipt_no",
    )

    assert len(frame) == 22
    assert not invalid.any()


def test_exact_evidence_error_reports_total_failure_count_not_sample_size():
    rows = []
    for index in range(22):
        receipt = f"20260101{index:06d}"
        rows.append({
            "receipt_no": receipt,
            "revision_root_receipt_no": receipt,
            "previous_receipt_no": None,
            "source_evidence_status": "FAKE",
            "cash_amount_status": "POSITIVE",
            "revision_kind": "ORIGINAL_DECISION",
            "viewer_evidence_sha256": None,
            "economic_evidence_sha256": "a" * 64,
            "reviewed_correction_id": None,
            "record_date": date(2025, 1, 1),
            "cash_amount": 100.0,
        })
    frame = pd.DataFrame(rows)

    with pytest.raises(RuntimeError, match="failure_count=22"):
        assert_verified_cash_evidence(
            frame,
            action_key_column="receipt_no",
            root_key_column="revision_root_receipt_no",
        )


@pytest.mark.parametrize(
    ("ticker", "receipt", "asset_id"),
    [
        ("0008Z0", "20260120900486", 6590),
        ("0010V0", "20260206900936", 6592),
        ("0039P0", "20260708900856", 6671),
    ],
)
def test_alphanumeric_krx_cash_receipt_and_published_action_exact_parity(
    ticker, receipt, asset_id,
):
    run_id = uuid4()
    actions = pd.DataFrame([_cash_receipt(
        receipt,
        mapping_status="INCLUDED",
        excluded_reason=None,
        asset_id=asset_id,
        corp_cls="K",
        ticker=ticker.lower(),
    )])

    receipts = _source_receipt_frame(actions, quality_run_id=run_id)
    contract = _published_action_contract(actions, receipts)

    assert receipts["ticker"].tolist() == [ticker]
    assert receipts["mapping_status"].tolist() == ["INCLUDED"]
    assert contract["published_action_count"] == 1
    assert contract["included_cash_action_parity_count"] == 1
    assert len(contract["published_action_row_digest"]) == 64
    assert len(contract["included_cash_action_parity_digest"]) == 64


def test_cli_is_read_only_by_default_and_apply_is_explicit():
    assert parse_args([]).apply is False
    assert parse_args(["--apply"]).apply is True


def test_apply_requires_explicit_expected_snapshot_end(monkeypatch):
    with pytest.raises(ValueError, match="expected-coverage-end"):
        run(apply=True, base_override="/does/not/matter")


def test_apply_refuses_uncertified_generic_dividend_fundamentals():
    with pytest.raises(ValueError, match="alot-matter content-addressed"):
        run(
            apply=True,
            total_return_actions_only=False,
            expected_coverage_end=date(2026, 8, 10),
            base_override="/does/not/matter",
        )
