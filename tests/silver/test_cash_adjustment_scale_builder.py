import copy
import hashlib
import json
import zipfile
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest

from pipeline.silver import cash_adjustment_scale_builder as builder
from pipeline.silver import cash_adjustment_scale_evidence as core_evidence
from pipeline.silver import krx_kind_reference
from pipeline.bronze import financials
from pipeline.bronze import dart_support_action_families
from pipeline.silver.cash_adjustment_scale_evidence import (
    MANIFEST_RELATIVE_PATH,
    external_evidence_paths as _external_evidence_paths,
    verify_source_evidence_manifest as _verify_source_evidence_manifest,
)
from pipeline.silver.dart_action_snapshot import (
    MANIFEST_RELATIVE_PATH as ACTION_SNAPSHOT_MANIFEST_RELATIVE_PATH,
    build_snapshot_manifest,
)
from pipeline.silver.total_return_audit import _validate_scale_source_rows


def _family_bounds(base: str | Path) -> tuple[date, date]:
    payload = json.loads(
        (Path(base) / dart_support_action_families.MANIFEST_RELATIVE_PATH)
        .read_text(encoding="utf-8")
    )
    return (
        date.fromisoformat(payload["seed_coverage_start"]),
        date.fromisoformat(payload["seed_coverage_end"]),
    )


def verify_source_evidence_manifest(base: str):
    start, end = _family_bounds(base)
    return _verify_source_evidence_manifest(
        base, required_start=start, required_end=end,
    )


def external_evidence_paths(base: str):
    start, end = _family_bounds(base)
    return _external_evidence_paths(
        base, required_start=start, required_end=end,
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes(), usedforsecurity=False).hexdigest()


def _zip(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("document.xml", text)


def _dart_numbered_notice(
    *, ticker: str, effective: date, reference: str,
    reason: str = "주식배당", security: str = "보통주식",
) -> str:
    return (
        "<document><table><tr>"
        "<td>1. 회사명</td><td>2. 주권종류</td><td>3. 단축코드</td>"
        "<td>4. 기준가(원)</td><td>5. 배당락 실시일</td>"
        "<td>6. 사유</td></tr><tr><td>테스트회사</td>"
        f"<td>{security}</td><td>A{ticker}</td><td>{reference}</td>"
        f"<td>{effective.isoformat()}</td><td>{reason}</td>"
        "</tr></table></document>"
    )


def _event(
    root: Path,
    *,
    ticker: str,
    receipt: str,
    event_type: str,
    body: Path,
    announcement: date,
    effective: date | None = None,
    record: date | None = None,
    report: str,
    source: str = "DART_DISCLOSURE",
    ratio: float | None = None,
    expected: float | None = None,
    cash: float | None = None,
) -> dict:
    return {
        "identifier": ticker,
        "event_type": event_type,
        "announcement_date": announcement,
        "effective_date": effective,
        "match_window_days": 7 if source == "DART_STRUCTURED" else 0,
        "expected_factor": expected,
        "record_date": record,
        "cash_amount": cash,
        "ratio_numerator": ratio,
        "ratio_denominator": 1.0 if ratio is not None else None,
        "rcept_no": receipt,
        "report_name": report,
        "action_scope": "ISSUER",
        "cash_amount_status": "POSITIVE" if cash is not None else None,
        "source_evidence_status": (
            "VERIFIED_OPENDART_DOCUMENT" if cash is not None else None
        ),
        "revision_root_action_key": receipt if cash is not None else None,
        "revision_kind": "ORIGINAL_DECISION" if cash is not None else None,
        "economic_evidence_sha256": _sha(body) if cash is not None else None,
        "source_body_sha256": _sha(body),
        "source": source,
        "source_file": str(body),
    }


def _family(
    events,
    *,
    ticker: str,
    action_type: str,
    root_receipt: str,
    terminal_receipt: str,
    ordered_receipts: tuple[str, ...],
    ratio: float,
):
    terminal = next(
        event for event in events
        if event.get("source") == (
            "DART_DISCLOSURE"
            if action_type == "stock_dividend"
            else "DART_STRUCTURED"
        )
        and event.get("event_type") == action_type
        and event.get("rcept_no") == terminal_receipt
    )
    path = Path(terminal["source_file"])
    source = SimpleNamespace(
        receipt_no=terminal_receipt,
        report_name=terminal["report_name"],
        receipt_date=terminal["announcement_date"].isoformat(),
        structured_path=(
            path.relative_to(path.parents[6]).as_posix()
            if action_type == "bonus_issue" else None
        ),
        structured_sha256=(
            terminal["source_body_sha256"]
            if action_type == "bonus_issue" else None
        ),
    )
    return SimpleNamespace(
        ticker=ticker,
        action_type=action_type,
        root_receipt_no=root_receipt,
        terminal_receipt_no=ordered_receipts[0],
        terminal_economic_receipt_no=terminal_receipt,
        ordered_family_receipts=ordered_receipts,
        terminal_status="ACTIVE",
        terminal_admissible=True,
        terminal_ratio=ratio,
        fresh_row_bind_digest="a" * 64,
        sources=(source,),
    )


def _viewer_bonus_family(
    root: Path,
    *,
    ticker: str,
    receipt: str,
    ratio: float,
    record_date: date,
):
    body = (
        "<html><body><table>"
        f"<tr><td>4. 신주배정기준일</td><td>{record_date.isoformat()}</td></tr>"
        "<tr><td>5. 1주당 신주배정 주식수</td>"
        f"<td>보통주식 (주)</td><td>{ratio}</td></tr>"
        "</table></body></html>"
    ).encode()
    digest = hashlib.sha256(body).hexdigest()
    path = root / (
        "corporate_actions/dart/support_action_families/objects/"
        f"sha256={digest}.html"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    source = SimpleNamespace(
        receipt_no=receipt,
        report_name="주요사항보고서(무상증자결정)",
        receipt_date=receipt[:4] + "-" + receipt[4:6] + "-" + receipt[6:8],
        body_path=path.relative_to(root).as_posix(),
        body_content_length=len(body),
        body_sha256=digest,
        structured_path=None,
        structured_sha256=None,
    )
    return SimpleNamespace(
        ticker=ticker,
        action_type="bonus_issue",
        root_receipt_no=receipt,
        terminal_receipt_no=receipt,
        terminal_economic_receipt_no=receipt,
        ordered_family_receipts=(receipt,),
        terminal_status="ACTIVE",
        terminal_admissible=True,
        terminal_ratio=ratio,
        fresh_row_bind_digest="b" * 64,
        sources=(source,),
    )


def _viewer_stock_dividend_family(root: Path):
    ticker = "032960"
    receipt = "20151228900387"
    body = (
        "<html><body><table>"
        "<tr><td>1. 1주당 배당주식수 (주)</td>"
        "<td>보통주식</td><td>0.05</td></tr>"
        "<tr><td>4. 배당기준일</td><td>2015-12-31</td></tr>"
        "</table></body></html>"
    ).encode()
    digest = hashlib.sha256(body).hexdigest()
    path = root / (
        "corporate_actions/dart/support_action_families/objects/"
        f"sha256={digest}.html"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    source = SimpleNamespace(
        receipt_no=receipt,
        report_name="[기재정정]주식배당결정",
        receipt_date="2015-12-28",
        body_path=path.relative_to(root).as_posix(),
        body_content_length=len(body),
        body_sha256=digest,
        structured_path=None,
        structured_sha256=None,
    )
    return SimpleNamespace(
        ticker=ticker,
        action_type="stock_dividend",
        root_receipt_no="20151216900093",
        terminal_receipt_no=receipt,
        terminal_economic_receipt_no=receipt,
        ordered_family_receipts=(receipt, "20151216900093"),
        terminal_status="ACTIVE",
        terminal_admissible=True,
        terminal_ratio=0.05,
        fresh_row_bind_digest="c" * 64,
        sources=(source,),
    )


def _price(root: Path, ticker: str, trade_date: date, close: float, change: float):
    path = root / f"stock/marcap/date={trade_date.isoformat()}/all.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "Code": ticker,
        "Date": trade_date,
        "Close": close,
        "Changes": change,
    }]).to_parquet(path, index=False)
    return builder.PriceObject(
        trade_date=trade_date,
        source_object_key=f"stock/marcap/date={trade_date.isoformat()}/all.parquet",
        local_path=str(path.relative_to(root)),
        etag=_md5(path),
        content_length=path.stat().st_size,
        content_sha256=_sha(path),
        version_id=None,
        server_side_encryption="AES256",
    )


def _006800_fixture(root: Path):
    ticker = "006800"
    previous_date = date(2026, 3, 13)
    adjustment_date = date(2026, 3, 16)
    record_date = date(2026, 3, 17)
    cash_receipt = "20260316800587"
    cash_body = root / (
        "corporate_actions/dart/documents/year=2026/corp=006800/"
        f"rcept={cash_receipt}.zip"
    )
    _zip(
        cash_body,
        "<document>현금배당 1주당 배당금 보통주식 300 "
        "배당기준일 2026-03-17</document>",
    )
    original_receipt = "20260224801317"
    original = root / (
        "corporate_actions/dart/documents/year=2026/corp=006800/"
        f"rcept={original_receipt}.zip"
    )
    _zip(
        original,
        "<document>주식배당결정 배당기준일 2026-03-17 "
        "<table><tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
        "<td>0.0073206</td></tr></table></document>",
    )
    corrected_receipt = "20260313800897"
    corrected = root / (
        "corporate_actions/dart/documents/year=2026/corp=006800/"
        f"rcept={corrected_receipt}.zip"
    )
    _zip(
        corrected,
        "<document>정정 관련 공시서류 제출일 2026-02-24 "
        "주식배당결정 배당기준일 2026-03-17<table>"
        "<tr><td>발행주식총수</td><td>보통주식</td><td>555,316,408</td></tr>"
        "<tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
        "<td>0.0073206</td></tr></table></document>",
    )
    events = [
        _event(
            root,
            ticker=ticker,
            receipt=cash_receipt,
            event_type="cash_dividend",
            body=cash_body,
            announcement=adjustment_date,
            record=record_date,
            report="현금ㆍ현물배당결정",
            cash=300.0,
        ),
        _event(
            root,
            ticker=ticker,
            receipt=original_receipt,
            event_type="stock_dividend",
            body=original,
            announcement=date(2026, 2, 24),
            record=record_date,
            report="주식배당결정",
            ratio=0.0073206,
        ),
        _event(
            root,
            ticker=ticker,
            receipt=corrected_receipt,
            event_type="stock_dividend",
            body=corrected,
            announcement=previous_date,
            record=record_date,
            report="[기재정정]주식배당결정",
            ratio=0.0073206,
        ),
    ]
    previous = _price(root, ticker, previous_date, 69_500.0, 500.0)
    applied = _price(root, ticker, adjustment_date, 70_900.0, 1_700.0)
    overlap = pd.DataFrame([{
        "asset_id": 7,
        "ticker": ticker,
        "asset_name": "미래에셋증권",
        "previous_date": previous_date,
        "applied_date": adjustment_date,
        "cash_receipts": json.dumps([cash_receipt]),
        "cash_amounts": json.dumps([300.0]),
        "record_dates": json.dumps([record_date.isoformat()]),
        "previous_close": 69_500.0,
        "previous_adj_close": 69_200.0,
        "applied_close": 70_900.0,
        "applied_adj_close": 70_900.0,
        "source_adjustment_factor": 69_200.0 / 69_500.0,
    }]).itertuples(index=False).__next__()
    return overlap, events, {previous_date: previous, adjustment_date: applied}


def _006800_families(events):
    return [_family(
        events,
        ticker="006800",
        action_type="stock_dividend",
        root_receipt="20260224801317",
        terminal_receipt="20260313800897",
        ordered_receipts=("20260313800897", "20260224801317"),
        ratio=0.0073206,
    )]


KIND_FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "kind"


def _empty_kind_component_request(root: Path) -> Path:
    components: list[dict] = []
    payload = {
        "schema_version": builder.KIND_COMPONENT_REQUEST_SCHEMA,
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_TERMINAL_COMPONENT",
        "complete": True,
        "component_count": 0,
        "component_digest": hashlib.sha256(b"[]").hexdigest(),
        "components": components,
    }
    path = root / "kind-component-requests.json"
    builder._atomic_write(path, builder._canonical_bytes(payload))
    return path


def _write_006800_kind_request(root: Path) -> Path:
    reviewed = json.loads(
        (KIND_FIXTURE_ROOT / "reference-requests-v2.json").read_bytes()
    )
    request = [
        row for row in reviewed["requests"] if row["ticker"] == "006800"
    ]
    payload = {
        "schema_version": builder.KIND_REQUEST_SCHEMA,
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_MAIN_AND_SELECTED_BODY",
        "complete": True,
        "request_count": 1,
        "request_digest": hashlib.sha256(
            builder._canonical_bytes(request)
        ).hexdigest(),
        "requests": request,
    }
    path = root / "kind-requests.json"
    builder._atomic_write(path, builder._canonical_bytes(payload))
    return path


def _write_006800_kind_support(root: Path):
    reference = _write_006800_kind_request(root)
    component = _empty_kind_component_request(root)
    payload = json.loads(reference.read_bytes())["requests"][0]
    bodies = {
        payload["identity_source_url"]: (
            KIND_FIXTURE_ROOT / "006800-20260313-main.html"
        ).read_bytes(),
        payload["source_url"]: (
            KIND_FIXTURE_ROOT / "006800-20260313-99311.html"
        ).read_bytes(),
    }
    return builder.download_kind_evidence(
        root, reference, component, fetcher=bodies.__getitem__,
    )


def _write_kind_request_object(root: Path, requests: list[dict]) -> tuple[str, str]:
    payload = {
        "schema_version": builder.KIND_REQUEST_SCHEMA,
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_MAIN_AND_SELECTED_BODY",
        "complete": True,
        "request_count": len(requests),
        "request_digest": hashlib.sha256(
            builder._canonical_bytes(requests)
        ).hexdigest(),
        "requests": requests,
    }
    raw = builder._canonical_bytes(payload)
    digest = hashlib.sha256(raw).hexdigest()
    relative = builder.KIND_REQUEST_OBJECT_ROOT / f"sha256={digest}.json"
    builder._atomic_write(root / relative, raw)
    return relative.as_posix(), digest


def _write_official_006800_family_snapshot(root: Path):
    original = "20260224801317"
    correction = "20260313800897"
    rows = [
        {
            "rcept_no": original,
            "rcept_dt": "20260224",
            "stock_code": "006800",
            "corp_code": "00111722",
            "corp_cls": "Y",
            "report_nm": "주식배당결정",
        },
        {
            "rcept_no": correction,
            "rcept_dt": "20260313",
            "stock_code": "006800",
            "corp_code": "00111722",
            "corp_cls": "Y",
            "report_nm": "[기재정정]주식배당결정",
        },
    ]
    interval = (
        root / "corporate_actions/dart/manifests"
        / "from=20260101/to=20261231"
    )
    interval.mkdir(parents=True, exist_ok=True)
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    marker = {
        "status": "COMPLETE", "fromdate": "20260101", "todate": "20261231",
    }
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": 0}), encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({**marker, "candidate_count": 2}), encoding="utf-8",
    )
    for row in rows:
        rendered = (
            f"{row['rcept_dt'][:4]}-{row['rcept_dt'][4:6]}-"
            f"{row['rcept_dt'][6:8]}"
        )
        path = (
            root / "corporate_actions/dart/disclosures"
            / f"year={row['rcept_dt'][:4]}" / f"date={rendered}"
            / "corp=006800" / f"rcept={row['rcept_no']}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(row, ensure_ascii=False), encoding="utf-8")
    order = (correction, original)
    bodies = {
        original: (
            "<html><body><table><tr><td>1주당 배당주식수(주)</td>"
            "<td>보통주식</td><td>0.0073206</td></tr></table></body></html>"
        ).encode(),
        correction: (
            "<html><body><p>정정 관련 공시서류 제출일 2026-02-24</p>"
            "<table><tr><td>발행주식총수</td><td>보통주식</td>"
            "<td>555,316,408</td></tr>"
            "<tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
            "<td>0.0073206</td></tr></table></body></html>"
        ).encode(),
    }

    def fetcher(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        dcm = str(1_000_000 + int(receipt[-6:]))
        if parsed.path.endswith("/main.do"):
            options = "".join(
                f'<option value="rcpNo={member}"'
                f'{" selected" if member == receipt else ""}>'
                f'{next(row["report_nm"] for row in rows if row["rcept_no"] == member)}'
                "</option>"
                for member in order
            )
            return (
                f'<script>viewDoc("{receipt}", "{dcm}", "0", "0", "0", '
                '"HTML", "");</script><select id="family">'
                f'{options}</select><select id="att"></select>'
            ).encode()
        assert query["dcmNo"] == [dcm]
        assert query["dtd"] == ["HTML"]
        return bodies[receipt]

    return dart_support_action_families.collect_support_action_families(
        root,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        apply=True,
        fetcher=fetcher,
    )


def _write_verified_scale_fixture(root: Path):
    overlap, events, prices = _006800_fixture(root)
    official_families = _write_official_006800_family_snapshot(root)
    kind = _write_006800_kind_support(root)
    parent, _ = builder._build_one(
        root,
        overlap,
        events=events,
        viewer_by_receipt={},
        kind_supports=kind,
        support_families=official_families.entries,
        prices=prices,
    )
    source_path = root / MANIFEST_RELATIVE_PATH
    builder._atomic_write(
        source_path,
        builder._canonical_bytes(builder._manifest_payload([parent])),
    )
    price_path = root / builder.PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    price_payload = builder._price_object_payload(
        list(prices.values()), bucket="bronze-bucket", prefix="",
    )
    builder._atomic_write(
        price_path, builder._canonical_bytes(price_payload),
    )
    return parent, prices, source_path, price_path


def test_006800_numeric_cash_coincidence_cannot_replace_official_stock_family(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)
    kind = _write_006800_kind_support(tmp_path)

    parent, diagnostic = builder._build_one(
        tmp_path,
        overlap,
        events=events,
        viewer_by_receipt={},
        kind_supports=kind,
        support_families=_006800_families(events),
        prices=prices,
    )

    assert diagnostic["stock_family_root_receipt"] == "20260224801317"
    assert diagnostic["terminal_stock_receipts"] == ["20260313800897"]
    assert diagnostic["cash_dps_numeric_coincidence"] is True
    assert diagnostic["classification_basis"] == (
        "OFFICIAL_NON_CASH_COMPONENT_AND_KRX_REFERENCE"
    )
    assert diagnostic["component_entitlement_ratios"] == [{
        "receipt": "20260313800897",
        "action_type": "stock_dividend",
        "ratio": 0.0073206,
        "semantics": "PER_ELIGIBLE_SHARE_ENTITLEMENT",
    }]
    assert "theoretical_component_factor" not in diagnostic
    assert "theoretical_factor_relative_error" not in diagnostic
    assert len(parent["support_actions"]) == 2
    reference = next(
        row for row in parent["support_actions"]
        if row["support_semantic_role"] == "CORROBORATION"
    )
    assert reference["support_action_key"] == "20260313001262"
    assert reference["support_entitlement_security_class"] == "COMMON"
    assert reference["support_reference_price"] == 69_200.0


def test_builder_reference_factor_uses_stored_price_rounding_interval(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)
    kind = _write_006800_kind_support(tmp_path)
    rounded = overlap._replace(
        previous_adj_close=23_066.6667,
        applied_adj_close=23_633.3333,
        source_adjustment_factor=(23_066.6667 / 69_500.0)
        / (23_633.3333 / 70_900.0),
    )
    observed = 69_200.0 / 69_500.0

    assert abs(observed - rounded.source_adjustment_factor) > 5e-13
    parent, _ = builder._build_one(
        tmp_path,
        rounded,
        events=events,
        viewer_by_receipt={},
        kind_supports=kind,
        support_families=_006800_families(events),
        prices=prices,
    )

    assert parent["expected_price_factor"] == pytest.approx(observed)


def test_006800_parent_roundtrips_through_frozen_core_verifier(tmp_path):
    _write_verified_scale_fixture(tmp_path)

    verified = verify_source_evidence_manifest(str(tmp_path))

    assert verified.row_count == 1
    assert len(verified.support_frame) == 2
    assert verified.metadata["manifest_support_semantic_group_count"] == 1
    external = external_evidence_paths(str(tmp_path))
    assert tmp_path / builder.PRICE_OBJECT_MANIFEST_RELATIVE_PATH in external
    assert (
        tmp_path / dart_support_action_families.MANIFEST_RELATIVE_PATH
        in external
    )


def test_builder_produced_006800_kind_row_passes_runtime_audit(tmp_path):
    parent, _, _, _ = _write_verified_scale_fixture(tmp_path)
    action_run = "00000000-0000-0000-0000-000000006800"
    parent_record = {
        **{key: value for key, value in parent.items() if key != "support_actions"},
        "action_snapshot_run_id": action_run,
    }
    support_records = [{
        **row,
        "action_snapshot_run_id": action_run,
        "support_action_quality_run_id": action_run,
    } for row in parent["support_actions"]]
    parents = pd.DataFrame([parent_record], columns=core_evidence.SOURCE_EVIDENCE_COLUMNS)
    supports = pd.DataFrame(
        support_records, columns=core_evidence.SUPPORT_ACTION_COLUMNS,
    )
    reference = next(
        row for row in support_records
        if row["support_action_source"] == "KRX_KIND"
    )

    valid, group_count = _validate_scale_source_rows(parents, supports)

    assert reference["support_report_name"] == (
        krx_kind_reference.KIND_REFERENCE_REPORT_NAME_99311
    )
    assert valid is True
    assert group_count == 1


def test_core_rebinds_kind_child_to_exact_manifest_identity(tmp_path):
    parent, _, source_path, _ = _write_verified_scale_fixture(tmp_path)
    forged = copy.deepcopy(parent)
    child = next(
        row for row in forged["support_actions"]
        if row["support_action_source"] == "KRX_KIND"
    )
    child["support_action_key"] = "20260313001268"
    child["manifest_support_row_sha256"] = (
        core_evidence.manifest_support_row_sha256(child)
    )
    forged["support_action_digest"] = core_evidence.support_manifest_digest(
        pd.DataFrame(forged["support_actions"])
    )
    forged["manifest_row_sha256"] = core_evidence.manifest_parent_row_sha256(
        forged
    )
    builder._atomic_write(
        source_path,
        builder._canonical_bytes(builder._manifest_payload([forged])),
    )

    with pytest.raises(RuntimeError, match="not an exact one-to-one set"):
        verify_source_evidence_manifest(str(tmp_path))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("missing_receipt", "missing/invalid cash-scale price-object receipt"),
        ("unused", "missing/unused bodies"),
        ("duplicate_local", "provenance row is invalid"),
        ("source_prefix", "source key mismatch"),
        ("source_key", "source key mismatch"),
        ("etag", "ETag/body mismatch"),
        ("sha", "receipt/body mismatch"),
        ("schema", "provenance row is invalid"),
    ],
)
def test_core_price_receipt_fails_closed_on_provenance_rewrite(
    tmp_path, mutation, message,
):
    _, prices, _, price_path = _write_verified_scale_fixture(tmp_path)
    payload = json.loads(price_path.read_text(encoding="utf-8"))
    if mutation == "missing_receipt":
        price_path.unlink()
    elif mutation == "unused":
        extra = dict(payload["objects"][0])
        extra["local_path"] = "stock/marcap/date=1999-01-01/all.parquet"
        extra["source_object_key"] = extra["local_path"]
        body = tmp_path / extra["local_path"]
        body.parent.mkdir(parents=True, exist_ok=True)
        body.write_bytes(
            tmp_path.joinpath(payload["objects"][0]["local_path"]).read_bytes()
        )
        extra["content_length"] = body.stat().st_size
        extra["content_sha256"] = _sha(body)
        extra["etag"] = _md5(body)
        payload["objects"].append(extra)
    elif mutation == "duplicate_local":
        payload["objects"].append(dict(payload["objects"][0]))
    elif mutation == "source_prefix":
        payload["source_prefix"] = "forged"
    elif mutation == "source_key":
        payload["objects"][0]["source_object_key"] = "stock/other.parquet"
    elif mutation == "etag":
        payload["objects"][0]["etag"] = "0" * 32
    elif mutation == "sha":
        payload["objects"][0]["content_sha256"] = "0" * 64
    elif mutation == "schema":
        payload["objects"][0]["source_schema"] = "forged"
    if mutation != "missing_receipt":
        payload["object_count"] = len(payload["objects"])
        payload["object_digest"] = hashlib.sha256(
            builder._canonical_bytes(payload["objects"])
        ).hexdigest()
        builder._atomic_write(price_path, builder._canonical_bytes(payload))

    with pytest.raises(RuntimeError, match=message):
        external_evidence_paths(str(tmp_path))


def test_core_price_receipt_detects_local_body_tamper(tmp_path):
    _, prices, _, _ = _write_verified_scale_fixture(tmp_path)
    local = tmp_path / next(iter(prices.values())).local_path
    local.write_bytes(local.read_bytes() + b"tamper")

    with pytest.raises(RuntimeError, match="evidence SHA mismatch"):
        external_evidence_paths(str(tmp_path))


@pytest.mark.parametrize(
    "field",
    [
        "previous_price_source_content_sha256",
        "previous_price_source_etag",
        "previous_price_source_schema",
    ],
)
def test_core_price_receipt_rejects_parent_identity_conflict(tmp_path, field):
    _write_verified_scale_fixture(tmp_path)
    verified = verify_source_evidence_manifest(str(tmp_path))
    parents = verified.frame.copy()
    parents.loc[0, field] = {
        "previous_price_source_content_sha256": "0" * 64,
        "previous_price_source_etag": "0" * 32,
        "previous_price_source_schema": "forged",
    }[field]

    with pytest.raises(RuntimeError, match="parity failed"):
        core_evidence._verify_price_object_receipt(tmp_path, parents)


def test_action_snapshot_hashes_price_head_receipt_manifest(tmp_path):
    _write_verified_scale_fixture(tmp_path)
    corp_codes = tmp_path / financials.CORPCODE_BRONZE_PATH
    if not corp_codes.is_file():
        corp_codes.parent.mkdir(parents=True, exist_ok=True)
        corp_codes.write_text("<result></result>", encoding="utf-8")

    build_snapshot_manifest(
        str(tmp_path), coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
    )
    payload = json.loads(
        (tmp_path / ACTION_SNAPSHOT_MANIFEST_RELATIVE_PATH).read_text(
            encoding="utf-8"
        )
    )
    paths = {row["path"] for row in payload["objects"]}
    assert builder.PRICE_OBJECT_MANIFEST_RELATIVE_PATH.as_posix() in paths
    assert (
        dart_support_action_families.MANIFEST_RELATIVE_PATH.as_posix()
        in paths
    )
    assert krx_kind_reference.KIND_SUPPORT_MANIFEST_RELATIVE_PATH.as_posix() in paths
    kind_external = {
        path.relative_to(tmp_path).as_posix()
        for path in krx_kind_reference.external_evidence_paths(tmp_path)
    }
    assert kind_external.issubset(paths)


def test_plain_ex_notice_cannot_substitute_for_non_cash_component(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)
    plain_path = tmp_path / (
        "corporate_actions/dart/documents/year=2026/corp=006800/"
        "rcept=20260313808888.zip"
    )
    _zip(
        plain_path,
        "<document>배당락 현금배당<table>"
        "<tr><td>배당락 실시일</td><td>2026-03-16</td></tr>"
        "<tr><td>기준가격</td><td>99.5</td></tr>"
        "<tr><td>사유</td><td>현금배당</td></tr></table></document>",
    )
    plain = _event(
        tmp_path,
        ticker="006800",
        receipt="20260313808888",
        event_type="ex_dividend",
        body=plain_path,
        announcement=date(2026, 3, 13),
        effective=date(2026, 3, 16),
        report="배당락(현금배당)",
    )
    events = [events[0], plain]

    with pytest.raises(RuntimeError, match="no official non-cash"):
        builder._build_one(
            tmp_path,
            overlap,
            events=events,
            viewer_by_receipt={},
            kind_supports=[],
            support_families=[],
            prices=prices,
        )


def test_labelled_notice_ignores_unrelated_payload_and_binds_exact_table(tmp_path):
    path = tmp_path / "notice.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("correction.xml", "<document>정정 첨부 문서</document>")
        archive.writestr(
            "notice.xml",
            _dart_numbered_notice(
                ticker="038680", effective=date(2024, 12, 27),
                reference="4,200",
            ),
        )

    notice = builder._labelled_notice(path)

    assert notice.ticker == "038680"
    assert notice.security_class == "COMMON"
    assert notice.effective_date == date(2024, 12, 27)
    assert notice.reference_price == 4_200.0
    assert notice.reason == "주식배당"


def test_labelled_notice_isolates_incomplete_marker_attachment(tmp_path):
    path = tmp_path / "notice.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "correction.xml",
            "<document><table><tr><td>5. 배당락 실시일</td>"
            "<td>2024-12-27</td></tr></table></document>",
        )
        archive.writestr(
            "notice.xml",
            _dart_numbered_notice(
                ticker="038680", effective=date(2024, 12, 27),
                reference="4,200",
            ),
        )

    notice = builder._labelled_notice(path)

    assert notice.ticker == "038680"
    assert notice.reference_price == 4_200.0


def test_labelled_notice_rejects_incomplete_marker_only(tmp_path):
    path = tmp_path / "notice.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "correction.xml",
            "<document><table><tr><td>5. 배당락 실시일</td>"
            "<td>2024-12-27</td></tr></table></document>",
        )

    with pytest.raises(
        krx_kind_reference.DartDetachmentNoticeNotFound,
        match="absent",
    ):
        builder._labelled_notice(path)


def test_labelled_notice_rejects_two_distinct_exact_payloads(tmp_path):
    path = tmp_path / "notice.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, reference in (("first.xml", "4,200"), ("second.xml", "4,210")):
            archive.writestr(
                name,
                _dart_numbered_notice(
                    ticker="038680", effective=date(2024, 12, 27),
                    reference=reference,
                ),
            )

    with pytest.raises(RuntimeError, match="ambiguous"):
        builder._labelled_notice(path)


def test_corroborations_skip_non_notice_candidate_and_bind_exact_identity(tmp_path):
    ticker = "038680"
    adjustment = date(2024, 12, 27)
    paths = [
        tmp_path / (
            "corporate_actions/dart/documents/year=2024/corp=038680/"
            f"rcept={receipt}.zip"
        )
        for receipt in ("20241226900001", "20241226901050")
    ]
    _zip(paths[0], "<document>[기재정정] 첨부 문서</document>")
    _zip(paths[1], _dart_numbered_notice(
        ticker=ticker, effective=adjustment, reference="4,200",
    ))
    events = [
        _event(
            tmp_path, ticker=ticker, receipt=receipt,
            event_type="ex_dividend", body=path,
            announcement=date(2024, 12, 26), effective=adjustment,
            report=report,
        )
        for receipt, path, report in (
            ("20241226900001", paths[0], "[기재정정]배당락(주식배당)"),
            ("20241226901050", paths[1], "배당락(주식배당)"),
        )
    ]

    supports, groups = builder._corroborations(
        tmp_path,
        events,
        ticker=ticker,
        asset_name="에스넷",
        adjustment_date=adjustment,
        raw_reference=4_200.0,
        evidence_key="038680:test",
        cash_receipt_no="20250310000001",
        groups_by_kind={"stock": ["stock-group"], "bonus": []},
    )

    assert [row["support_action_key"] for row in supports] == [
        "20241226901050"
    ]
    assert groups == {"stock-group"}


@pytest.mark.parametrize(
    ("body_ticker", "security", "body_date", "reference", "error"),
    [
        ("000000", "보통주식", date(2024, 12, 27), "4,200", "identity"),
        ("038680", "우선주", date(2024, 12, 27), "4,200", "identity"),
        ("038680", "보통주식", date(2024, 12, 30), "4,200", "identity"),
        ("038680", "보통주식", date(2024, 12, 27), "4,210", "reference"),
    ],
)
def test_corroboration_notice_identity_mismatch_fails_closed(
    tmp_path, body_ticker, security, body_date, reference, error,
):
    path = tmp_path / (
        "corporate_actions/dart/documents/year=2024/corp=038680/"
        "rcept=20241226901050.zip"
    )
    _zip(path, _dart_numbered_notice(
        ticker=body_ticker, effective=body_date, reference=reference,
        security=security,
    ))
    event = _event(
        tmp_path, ticker="038680", receipt="20241226901050",
        event_type="ex_dividend", body=path,
        announcement=date(2024, 12, 26), effective=date(2024, 12, 27),
        report="배당락(주식배당)",
    )

    with pytest.raises(RuntimeError, match=error):
        builder._corroborations(
            tmp_path,
            [event],
            ticker="038680",
            asset_name="에스넷",
            adjustment_date=date(2024, 12, 27),
            raw_reference=4_200.0,
            evidence_key="038680:test",
            cash_receipt_no="20250310000001",
            groups_by_kind={"stock": ["stock-group"], "bonus": []},
        )


def test_corroboration_uses_exact_notice_reason_not_disclosure_title(tmp_path):
    path = tmp_path / (
        "corporate_actions/dart/documents/year=2024/corp=038680/"
        "rcept=20241226901050.zip"
    )
    _zip(path, _dart_numbered_notice(
        ticker="038680", effective=date(2024, 12, 27),
        reference="4,200", reason="현금배당",
    ))
    event = _event(
        tmp_path, ticker="038680", receipt="20241226901050",
        event_type="ex_dividend", body=path,
        announcement=date(2024, 12, 26), effective=date(2024, 12, 27),
        report="배당락(주식배당)",
    )

    supports, groups = builder._corroborations(
        tmp_path,
        [event],
        ticker="038680",
        asset_name="에스넷",
        adjustment_date=date(2024, 12, 27),
        raw_reference=4_200.0,
        evidence_key="038680:test",
        cash_receipt_no="20250310000001",
        groups_by_kind={"stock": ["stock-group"], "bonus": []},
    )

    assert supports == []
    assert groups == set()


def test_two_exact_notice_candidates_for_one_group_fail_closed(tmp_path):
    events = []
    for receipt in ("20241226901050", "20241226901051"):
        path = tmp_path / (
            "corporate_actions/dart/documents/year=2024/corp=038680/"
            f"rcept={receipt}.zip"
        )
        _zip(path, _dart_numbered_notice(
            ticker="038680", effective=date(2024, 12, 27),
            reference="4,200",
        ))
        events.append(_event(
            tmp_path, ticker="038680", receipt=receipt,
            event_type="ex_dividend", body=path,
            announcement=date(2024, 12, 26), effective=date(2024, 12, 27),
            report="배당락(주식배당)",
        ))

    with pytest.raises(RuntimeError, match="ambiguous for a component"):
        builder._corroborations(
            tmp_path,
            events,
            ticker="038680",
            asset_name="에스넷",
            adjustment_date=date(2024, 12, 27),
            raw_reference=4_200.0,
            evidence_key="038680:test",
            cash_receipt_no="20250310000001",
            groups_by_kind={"stock": ["stock-group"], "bonus": []},
        )


def test_stock_component_without_exact_ex_reference_notice_is_rejected(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)

    with pytest.raises(RuntimeError, match="lacks exact ex/reference"):
        builder._build_one(
            tmp_path,
            overlap,
            events=events,
            viewer_by_receipt={},
            kind_supports=[],
            support_families=_006800_families(events),
            prices=prices,
        )


def test_matching_inadmissible_official_family_is_held(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)
    family = _006800_families(events)[0]
    family.terminal_admissible = False
    family.terminal_status = "WITHDRAWN"

    with pytest.raises(RuntimeError, match="family is inadmissible"):
        builder._build_one(
            tmp_path,
            overlap,
            events=events,
            viewer_by_receipt={},
            kind_supports=[],
            support_families=[family],
            prices=prices,
        )


def test_prepared_terminal_ratio_must_match_verified_family(tmp_path):
    overlap, events, prices = _006800_fixture(tmp_path)
    family = _006800_families(events)[0]
    family.terminal_ratio = 0.5

    with pytest.raises(RuntimeError, match="terminal ratio parity failed"):
        builder._build_one(
            tmp_path,
            overlap,
            events=events,
            viewer_by_receipt={},
            kind_supports=[],
            support_families=[family],
            prices=prices,
        )


def test_cash_receipt_must_be_fresh_family_terminal(tmp_path):
    overlap, events, _ = _006800_fixture(tmp_path)
    correction_body = tmp_path / (
        "corporate_actions/dart/documents/year=2026/corp=006800/"
        "rcept=20260317800001.zip"
    )
    _zip(correction_body, "<document>현금배당 0.5 배당기준일 2026-03-17</document>")
    later = _event(
        tmp_path,
        ticker="006800",
        receipt="20260317800001",
        event_type="cash_dividend",
        body=correction_body,
        announcement=date(2026, 3, 17),
        record=date(2026, 3, 17),
        report="[기재정정]현금ㆍ현물배당결정",
        cash=0.5,
    )
    later["revision_root_action_key"] = "20260316800587"
    events[0]["revision_root_action_key"] = "20260316800587"
    viewer = SimpleNamespace(
        revision_root_receipt_no="20260316800587",
        official_family_order=("20260317800001", "20260316800587"),
        economic_body_receipt_no="20260317800001",
    )

    with pytest.raises(RuntimeError, match="not the fresh official family terminal"):
        builder._cash_event(
            [events[0], later],
            {
                "20260316800587": viewer,
                "20260317800001": viewer,
            },
            ticker="006800",
            receipt="20260316800587",
            cash_amount=0.5,
            record_date=overlap.record_dates and date(2026, 3, 17),
        )


def test_cash_terminal_uses_official_selector_not_receipt_or_date_order(tmp_path):
    ticker = "006800"
    record = date(2026, 3, 17)
    root_receipt = "20260331999999"
    terminal_receipt = "20260301000001"
    root_body = tmp_path / f"{root_receipt}.zip"
    terminal_body = tmp_path / f"{terminal_receipt}.zip"
    _zip(root_body, "<document>보통주 100 배당기준일 2026-03-17</document>")
    _zip(terminal_body, "<document>보통주 300 배당기준일 2026-03-17</document>")
    original = _event(
        tmp_path,
        ticker=ticker,
        receipt=root_receipt,
        event_type="cash_dividend",
        body=root_body,
        announcement=date(2026, 3, 31),
        record=record,
        report="현금ㆍ현물배당결정",
        cash=100.0,
    )
    terminal = _event(
        tmp_path,
        ticker=ticker,
        receipt=terminal_receipt,
        event_type="cash_dividend",
        body=terminal_body,
        announcement=date(2026, 3, 1),
        record=record,
        report="[기재정정]현금ㆍ현물배당결정",
        cash=300.0,
    )
    terminal["revision_root_action_key"] = root_receipt
    terminal["revision_kind"] = "ECONOMIC_REVISION"
    viewer = SimpleNamespace(
        receipt_no=terminal_receipt,
        revision_root_receipt_no=root_receipt,
        official_family_order=(terminal_receipt, root_receipt),
        economic_body_receipt_no=terminal_receipt,
    )

    selected = builder._cash_event(
        [original, terminal],
        {terminal_receipt: viewer},
        ticker=ticker,
        receipt=terminal_receipt,
        cash_amount=300.0,
        record_date=record,
    )

    assert selected is terminal


def test_cash_attachment_terminal_resolves_to_official_economic_receipt(tmp_path):
    ticker = "006800"
    record = date(2026, 3, 17)
    root_receipt = "20260201000001"
    economic_receipt = "20260215000001"
    attachment_receipt = "20260331000001"
    events = []
    for receipt, announcement, amount, report, revision_kind in (
        (root_receipt, date(2026, 2, 1), 100.0, "현금ㆍ현물배당결정", "ORIGINAL_DECISION"),
        (economic_receipt, date(2026, 2, 15), 300.0, "[기재정정]현금ㆍ현물배당결정", "ECONOMIC_REVISION"),
        (attachment_receipt, date(2026, 3, 31), 0.0, "[첨부정정]현금ㆍ현물배당결정", "ATTACHMENT_ONLY"),
    ):
        body = tmp_path / f"{receipt}.zip"
        _zip(body, "<document>배당 공시</document>")
        event = _event(
            tmp_path,
            ticker=ticker,
            receipt=receipt,
            event_type="cash_dividend",
            body=body,
            announcement=announcement,
            record=record if revision_kind != "ATTACHMENT_ONLY" else None,
            report=report,
            cash=amount,
        )
        event["revision_root_action_key"] = root_receipt
        event["revision_kind"] = revision_kind
        if revision_kind == "ATTACHMENT_ONLY":
            event["cash_amount"] = None
            event["cash_amount_status"] = "ATTACHMENT_ONLY"
        events.append(event)
    viewer = SimpleNamespace(
        receipt_no=attachment_receipt,
        revision_root_receipt_no=root_receipt,
        # Attachment receipts live in the separate official attachment
        # selector; this economic family order therefore starts at the latest
        # non-attachment receipt.
        official_family_order=(economic_receipt, root_receipt),
        economic_body_receipt_no=economic_receipt,
    )

    selected = builder._cash_event(
        events,
        {attachment_receipt: viewer},
        ticker=ticker,
        receipt=economic_receipt,
        cash_amount=300.0,
        record_date=record,
    )

    assert selected["rcept_no"] == economic_receipt


def test_unrelated_or_related_company_support_families_do_not_block_parent(tmp_path):
    overlap, events, _ = _006800_fixture(tmp_path)
    target_family = _006800_families(events)[0]
    extra_families = []
    for receipt, record, report in (
        ("20191201000001", date(2019, 12, 31), "[철회]주식배당결정"),
        (
            "20260301000002",
            date(2026, 3, 17),
            "[철회]자회사의 주요경영사항(주식배당결정)",
        ),
    ):
        body = tmp_path / (
            "corporate_actions/dart/documents/"
            f"year={receipt[:4]}/corp=006800/rcept={receipt}.zip"
        )
        _zip(body, "<document>주식배당결정 보통주 0.2주</document>")
        event = _event(
            tmp_path,
            ticker="006800",
            receipt=receipt,
            event_type="stock_dividend",
            body=body,
            announcement=record - timedelta(days=10),
            record=record,
            report=report,
            ratio=0.2,
        )
        events.append(event)
        family = _family(
            events,
            ticker="006800",
            action_type="stock_dividend",
            root_receipt=receipt,
            terminal_receipt=receipt,
            ordered_receipts=(receipt,),
            ratio=0.2,
        )
        family.terminal_admissible = False
        family.terminal_status = "WITHDRAWN"
        family.terminal_ratio = None
        extra_families.append(family)

    components, _, _ = builder._component_supports(
        tmp_path,
        events,
        [],
        [target_family, *extra_families],
        ticker="006800",
        record_date=date(2026, 3, 17),
        adjustment_date=date(2026, 3, 16),
        evidence_key="006800:cash:2026-03-16",
        cash_receipt_no="20260316800587",
    )

    assert [row["support_action_key"] for row in components] == [
        "20260313800897"
    ]


def test_same_target_inadmissible_support_family_holds_parent(tmp_path):
    _, events, _ = _006800_fixture(tmp_path)
    family = _006800_families(events)[0]
    family.terminal_admissible = False
    family.terminal_status = "WITHDRAWN"
    family.terminal_ratio = None

    with pytest.raises(RuntimeError, match="family is inadmissible"):
        builder._component_supports(
            tmp_path,
            events,
            [],
            [family],
            ticker="006800",
            record_date=date(2026, 3, 17),
            adjustment_date=date(2026, 3, 16),
            evidence_key="006800:cash:2026-03-16",
            cash_receipt_no="20260316800587",
        )


def test_viewer_backed_bonus_family_supplies_missing_structured_component(
    tmp_path,
):
    family = _viewer_bonus_family(
        tmp_path,
        ticker="001060",
        receipt="20161216000097",
        ratio=0.02,
        record_date=date(2017, 1, 1),
    )

    components, groups, diagnostic = builder._component_supports(
        tmp_path,
        [],
        [],
        [family],
        ticker="001060",
        record_date=date(2016, 12, 31),
        adjustment_date=date(2016, 12, 28),
        evidence_key="001060:2016-12-28",
        cash_receipt_no="20170217800213",
    )

    assert len(components) == 1
    component = components[0]
    assert component["support_action_source"] == "DART_VIEWER"
    assert component["support_action_key"] == "20161216000097"
    assert component["support_action_type"] == "bonus_issue"
    assert component["support_ex_date"] == date(2017, 1, 1)
    assert component["support_ratio_numerator"] == pytest.approx(0.02)
    assert component["support_expected_price_factor"] == pytest.approx(
        1 / 1.02,
    )
    assert component["support_action_body_path"] == family.sources[0].body_path
    assert groups["bonus"]
    assert diagnostic["bonus_family_root_receipt"] == "20161216000097"


def test_viewer_backed_bonus_family_rejects_body_tamper(tmp_path):
    family = _viewer_bonus_family(
        tmp_path,
        ticker="060560",
        receipt="20170919000279",
        ratio=1.0,
        record_date=date(2017, 10, 1),
    )
    body = tmp_path / family.sources[0].body_path
    body.write_bytes(body.read_bytes().replace(b"1.0", b"0.5"))

    with pytest.raises(RuntimeError, match="body changed"):
        builder._component_supports(
            tmp_path,
            [],
            [],
            [family],
            ticker="060560",
            record_date=date(2017, 9, 30),
            adjustment_date=date(2017, 9, 28),
            evidence_key="060560:2017-09-28",
            cash_receipt_no="20170919900263",
        )


def test_viewer_backed_stock_dividend_family_supplies_corrected_component(
    tmp_path,
):
    family = _viewer_stock_dividend_family(tmp_path)

    components, groups, diagnostic = builder._component_supports(
        tmp_path,
        [],
        [],
        [family],
        ticker="032960",
        record_date=date(2015, 12, 31),
        adjustment_date=date(2015, 12, 29),
        evidence_key="032960:2015-12-29",
        cash_receipt_no="20160229800375",
    )

    assert len(components) == 1
    component = components[0]
    assert component["support_action_source"] == "DART_VIEWER"
    assert component["support_action_key"] == "20151228900387"
    assert component["support_action_type"] == "stock_dividend"
    assert component["support_ex_date"] is None
    assert component["support_record_date"] == date(2015, 12, 31)
    assert component["support_ratio_numerator"] == pytest.approx(0.05)
    assert component["support_expected_price_factor"] is None
    assert groups["stock"] == [
        "032960|2015-12-31|STOCK_DIVIDEND|0.05"
    ]
    assert diagnostic["stock_family_root_receipt"] == "20151216900093"


def test_viewer_stock_dividend_family_rejects_unreviewed_or_tampered_identity(
    tmp_path,
):
    family = _viewer_stock_dividend_family(tmp_path)
    family.root_receipt_no = "20151216900094"

    with pytest.raises(RuntimeError, match="no official non-cash"):
        builder._component_supports(
            tmp_path, [], [], [family], ticker="032960",
            record_date=date(2015, 12, 31),
            adjustment_date=date(2015, 12, 29),
            evidence_key="032960:2015-12-29",
            cash_receipt_no="20160229800375",
        )

    family = _viewer_stock_dividend_family(tmp_path)
    body = tmp_path / family.sources[0].body_path
    body.write_bytes(body.read_bytes().replace(b"0.05", b"0.06"))
    with pytest.raises(RuntimeError, match="body changed"):
        builder._component_supports(
            tmp_path, [], [], [family], ticker="032960",
            record_date=date(2015, 12, 31),
            adjustment_date=date(2015, 12, 29),
            evidence_key="032960:2015-12-29",
            cash_receipt_no="20160229800375",
        )


def test_cross_class_dart_family_defers_only_to_exact_kind_component(tmp_path):
    receipt = "20181221800001"
    record = date(2018, 12, 31)
    body = tmp_path / (
        "corporate_actions/dart/documents/year=2018/corp=001040/"
        f"rcept={receipt}.zip"
    )
    _zip(body, "<document>주식배당결정 배당기준일 2018-12-31 종류주식 0.15</document>")
    event = _event(
        tmp_path,
        ticker="001040",
        receipt=receipt,
        event_type="stock_dividend",
        body=body,
        announcement=date(2018, 12, 21),
        record=record,
        report="[기재정정]주식배당결정",
    )
    source = SimpleNamespace(
        receipt_no=receipt,
        report_name=event["report_name"],
        receipt_date="2018-12-21",
        body_path=body.relative_to(tmp_path).as_posix(),
        body_content_length=body.stat().st_size,
        body_sha256=_sha(body),
        structured_path=None,
        structured_sha256=None,
    )
    family = SimpleNamespace(
        ticker="001040",
        action_type="stock_dividend",
        root_receipt_no="20181220800750",
        terminal_receipt_no=receipt,
        terminal_economic_receipt_no=receipt,
        ordered_family_receipts=(receipt, "20181220800750"),
        terminal_status="CROSS_CLASS_DISTRIBUTION",
        terminal_admissible=False,
        terminal_ratio=None,
        fresh_row_bind_digest="c" * 64,
        sources=(source,),
    )
    kind = {
        "ticker": "001040",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_action_type": "stock_dividend",
        "target_cash_receipt_no": "20190211800997",
        "target_adjustment_date": "2018-12-27",
        "support_record_date": "2018-12-31",
        "support_report_name": krx_kind_reference.KIND_COMPONENT_REPORT_NAME_61474,
        "support_ratio_numerator": 0.15,
        "support_ratio_denominator": 1.0,
        "support_action_key": "20181220002252",
        "support_announcement_date": "2018-12-21",
        "support_action_body_path": "corporate_actions/krx/kind/body.html",
        "support_action_body_sha256": "d" * 64,
        "support_entitlement_security_class": "COMMON_AND_PREFERRED",
        "support_distributed_security_class": "NEW_PREFERRED",
        "support_expected_price_factor": None,
    }

    components, _, _ = builder._component_supports(
        tmp_path,
        [event],
        [kind],
        [family],
        ticker="001040",
        record_date=record,
        adjustment_date=date(2018, 12, 27),
        evidence_key="001040:2018-12-27",
        cash_receipt_no="20190211800997",
    )

    assert len(components) == 1
    assert components[0]["support_action_source"] == "KRX_KIND"
    assert components[0]["support_action_key"] == "20181220002252"


def test_iwin_composite_notice_corroborates_two_distinct_components(tmp_path):
    ticker = "090150"
    record = date(2021, 12, 31)
    adjustment = date(2021, 12, 29)
    bonus = tmp_path / "corporate_actions/dart/structured/event=bonus_issue/year=2021/corp=090150/rcept=20211217000406.json"
    bonus.parent.mkdir(parents=True)
    bonus.write_text(json.dumps({
        "rcept_no": "20211217000406", "nstk_ascnt_ps_ostk": "1.0",
    }), encoding="utf-8")
    stock = tmp_path / "corporate_actions/dart/documents/year=2021/corp=090150/rcept=20211224900781.zip"
    _zip(stock, "<document>주식배당결정 배당기준일 2021-12-31 보통주 0.1주</document>")
    combined = tmp_path / "corporate_actions/dart/documents/year=2021/corp=090150/rcept=20211228900755.zip"
    _zip(
        combined,
        "<document>권배락<table>"
        '<tr><td>1. 권배락 실시일</td><td colspan="4">2021-12-29</td></tr>'
        '<tr><td>2. 권배락 사유</td><td colspan="4">무상증자 및 배당</td></tr>'
        '<tr><td rowspan="2">3. 권배락 내역</td><td>회사명</td>'
        '<td>주권종류</td><td>단축코드</td><td>기준가(원)</td></tr>'
        '<tr><td>광진윈텍</td><td>보통주식</td><td>A090150</td>'
        '<td>4,960</td></tr></table></document>',
    )
    events = [
        _event(
            tmp_path, ticker=ticker, receipt="20211217000406",
            event_type="bonus_issue", body=bonus,
            announcement=date(2021, 12, 17), effective=record,
            report="주요사항보고서(무상증자결정)", source="DART_STRUCTURED",
            ratio=1.0, expected=0.5,
        ),
        _event(
            tmp_path, ticker=ticker, receipt="20211224900781",
            event_type="stock_dividend", body=stock,
            announcement=date(2021, 12, 24), record=record,
            report="주식배당결정", ratio=0.1,
        ),
        _event(
            tmp_path, ticker=ticker, receipt="20211228900755",
            event_type="combined_detachment", body=combined,
            announcement=date(2021, 12, 28), effective=adjustment,
            report="권배락(무상증자 및 배당)",
        ),
    ]
    components, groups, _ = builder._component_supports(
        tmp_path,
        events,
        [],
        [
            _family(
                events,
                ticker=ticker,
                action_type="stock_dividend",
                root_receipt="20211224900781",
                terminal_receipt="20211224900781",
                ordered_receipts=("20211224900781",),
                ratio=0.1,
            ),
            _family(
                events,
                ticker=ticker,
                action_type="bonus_issue",
                root_receipt="20211217000406",
                terminal_receipt="20211217000406",
                ordered_receipts=("20211217000406",),
                ratio=1.0,
            ),
        ],
        ticker=ticker,
        record_date=record,
        adjustment_date=adjustment,
        evidence_key="iwin",
        cash_receipt_no="20220315901234",
    )
    corroborations, corroborated = builder._corroborations(
        tmp_path,
        events,
        ticker=ticker,
        adjustment_date=adjustment,
        raw_reference=4960,
        evidence_key="iwin",
        cash_receipt_no="20220315901234",
        groups_by_kind=groups,
    )

    assert len(components) == 2
    assert len(corroborations) == 1
    assert len(corroborated) == 2
    assert json.loads(corroborations[0]["support_semantic_group_keys"]) == sorted(
        groups["stock"] + groups["bonus"]
    )


def _actual_kind_fetcher():
    by_sha = {
        hashlib.sha256(path.read_bytes()).hexdigest(): path.read_bytes()
        for path in KIND_FIXTURE_ROOT.glob("*.html")
    }
    reference = json.loads(
        (KIND_FIXTURE_ROOT / "reference-requests-v2.json").read_bytes()
    )
    component = json.loads(
        (KIND_FIXTURE_ROOT / "component-requests-v1.json").read_bytes()
    )
    bodies = {}
    for row in reference["requests"]:
        bodies[row["identity_source_url"]] = by_sha[row["identity_sha256"]]
        bodies[row["source_url"]] = by_sha[row["body_sha256"]]
    for row in component["components"]:
        bodies[row["main_url"]] = by_sha[row["main_sha256"]]
        bodies[row["contents_url"]] = by_sha[row["contents_sha256"]]
        bodies[row["body_url"]] = by_sha[row["body_sha256"]]
    assert len(bodies) == 19
    return bodies.__getitem__


def _download_actual_kind(root: Path):
    return builder.download_kind_evidence(
        root,
        KIND_FIXTURE_ROOT / "reference-requests-v2.json",
        KIND_FIXTURE_ROOT / "component-requests-v1.json",
        fetcher=_actual_kind_fetcher(),
    )


def test_kind_actual_reviewed_artifacts_bootstrap_fresh_snapshot(tmp_path):
    assert _sha(KIND_FIXTURE_ROOT / "reference-requests-v2.json") == (
        "ce090cab0dbe1f7821bc80979124335bd16446ac302cac8a241a44d5fa8385bf"
    )
    assert _sha(KIND_FIXTURE_ROOT / "component-requests-v1.json") == (
        "1e6a1634d51188ad2ef7f4a04d9c6a16c79dae1abfce919abcf6092124c9e0d7"
    )

    supports = _download_actual_kind(tmp_path)

    assert len(supports) == 9
    assert sum(row["support_semantic_role"] == "CORROBORATION" for row in supports) == 8
    component = next(
        row for row in supports
        if row["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
    )
    assert component["ticker"] == "001040"
    assert component["support_action_key"] == "20181220002252"
    assert component["support_ratio_numerator"] == 0.15
    reference = next(
        row for row in supports
        if row["ticker"] == "006800"
        and row["support_semantic_role"] == "CORROBORATION"
    )
    assert reference["support_reference_price"] == 69_200.0
    assert reference["target_cash_receipt_no"] == "20260316800587"
    external = krx_kind_reference.external_evidence_paths(tmp_path)
    assert len(external) == 22
    assert all(path.is_file() for path in external)


def test_kind_request_cli_requires_both_reviewed_request_sets(tmp_path):
    parsed = builder.parse_args([
        "download-kind", "--base", str(tmp_path),
        "--reference-requests", str(KIND_FIXTURE_ROOT / "reference-requests-v2.json"),
        "--component-requests", str(KIND_FIXTURE_ROOT / "component-requests-v1.json"),
    ])
    assert parsed.command == "download-kind"
    assert parsed.reference_requests.endswith("reference-requests-v2.json")
    assert parsed.component_requests.endswith("component-requests-v1.json")


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("body", "content-addressed evidence"),
        ("identity", "content-addressed evidence"),
        ("contents", "content-addressed evidence"),
        ("request", "request object SHA mismatch"),
        ("unused_reference", "unused notice identities"),
        ("unused_component", "unused identities"),
        ("target_cash", "target_cash_receipt_no changed"),
        ("target_date", "target_adjustment_date changed"),
    ],
)
def test_kind_support_manifest_corruption_fails_closed(tmp_path, mutation, message):
    supports = _download_actual_kind(tmp_path)
    manifest = tmp_path / builder.KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_bytes())
    component = next(
        row for row in payload["supports"]
        if row["support_semantic_role"] == "ADJUSTMENT_COMPONENT"
    )
    reference = next(
        row for row in payload["supports"]
        if row["ticker"] == "006800"
        and row["support_semantic_role"] == "CORROBORATION"
    )
    if mutation == "body":
        body = tmp_path / reference["support_action_body_path"]
        body.write_bytes(body.read_bytes() + b"tamper")
    elif mutation == "identity":
        body = tmp_path / reference["identity_body_path"]
        body.write_bytes(body.read_bytes() + b"tamper")
    elif mutation == "contents":
        body = tmp_path / component["contents_body_path"]
        body.write_bytes(body.read_bytes() + b"tamper")
    elif mutation == "request":
        request = tmp_path / payload["reference_request_path"]
        request.write_bytes(request.read_bytes() + b" ")
    elif mutation in {"unused_reference", "unused_component"}:
        role = (
            "CORROBORATION" if mutation == "unused_reference"
            else "ADJUSTMENT_COMPONENT"
        )
        payload["supports"] = [
            row for row in payload["supports"]
            if row["support_semantic_role"] != role
        ]
        payload["support_count"] = len(payload["supports"])
        payload["support_digest"] = hashlib.sha256(
            builder._canonical_bytes(payload["supports"])
        ).hexdigest()
        builder._atomic_write(manifest, builder._canonical_bytes(payload))
    else:
        field = (
            "target_cash_receipt_no"
            if mutation == "target_cash" else "target_adjustment_date"
        )
        reference[field] = (
            "20260316800588" if mutation == "target_cash" else "2026-03-17"
        )
        payload["support_digest"] = hashlib.sha256(
            builder._canonical_bytes(payload["supports"])
        ).hexdigest()
        builder._atomic_write(manifest, builder._canonical_bytes(payload))

    with pytest.raises(RuntimeError, match=message):
        builder._kind_supports(tmp_path)


def test_kind_publish_restores_previous_manifest_on_second_verify_failure(
    tmp_path, monkeypatch,
):
    _write_006800_kind_support(tmp_path)
    manifest = tmp_path / builder.KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    previous = manifest.read_bytes()
    original = builder._kind_supports
    calls = 0

    def flaky(root):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("second read failed")
        return original(root)

    monkeypatch.setattr(builder, "_kind_supports", flaky)
    with pytest.raises(RuntimeError, match="second read failed"):
        _download_actual_kind(tmp_path)
    assert manifest.read_bytes() == previous


@pytest.mark.parametrize("status", [204, 302, 307])
def test_kind_http_fetch_rejects_every_non_200_response(monkeypatch, status):
    class Response:
        status_code = status
        url = "https://kind.krx.co.kr/example"
        content = b"nonempty"

    monkeypatch.setattr(builder.requests, "get", lambda *args, **kwargs: Response())
    with pytest.raises(RuntimeError, match="exact HTTP 200"):
        builder._http_body("https://kind.krx.co.kr/example")


@pytest.mark.parametrize(
    ("response_url", "content"),
    [
        ("https://kind.krx.co.kr/redirected", b"nonempty"),
        ("https://kind.krx.co.kr/example", b""),
    ],
)
def test_kind_http_fetch_rejects_url_rewrite_or_empty_200(
    monkeypatch, response_url, content,
):
    class Response:
        status_code = 200
        url = response_url

    Response.content = content
    monkeypatch.setattr(builder.requests, "get", lambda *args, **kwargs: Response())
    with pytest.raises(RuntimeError, match="exact HTTP 200"):
        builder._http_body("https://kind.krx.co.kr/example")


def _synthetic_inputs() -> builder.RecoveryInputs:
    start = date(2020, 1, 1)
    rows = []
    dates = [start + timedelta(days=index) for index in range(49)]
    for index in range(331):
        rows.append({
            "asset_id": index + 1,
            "previous_date": dates[index % 48],
            "applied_date": dates[(index % 48) + 1],
        })
    return builder.RecoveryInputs(
        overlap=pd.DataFrame(rows),
        missing_pairs=pd.DataFrame(),
        resolved_cash=pd.DataFrame(),
        expectations={},
        overlap_path=Path("overlap"),
        expectations_path=Path("expectations"),
        missing_pairs_path=Path("missing"),
        resolved_cash_path=Path("resolved"),
    )


@pytest.mark.parametrize("target", ["overlap", "expectations"])
def test_frozen_recovery_input_rejects_one_byte_tamper(tmp_path, target):
    overlap = tmp_path / "overlap.csv"
    expectations = tmp_path / "expectations.json"
    overlap.write_bytes(b"frozen-overlap")
    expectations.write_bytes(b"{}")
    overlap_sha = _sha(overlap)
    expectations_sha = _sha(expectations)
    path = overlap if target == "overlap" else expectations
    path.write_bytes(path.read_bytes() + b"!")

    with pytest.raises(RuntimeError, match=f"recovery {target} SHA mismatch"):
        builder.load_recovery_inputs(
            overlap,
            expectations,
            expected_overlap_sha256=overlap_sha,
            expected_expectations_sha256=expectations_sha,
        )


def test_kind_support_identity_must_have_a_consumer():
    kind = [{
        "ticker": "001040",
        "support_action_key": "20181220002252",
        "support_action_type": "stock_dividend",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "target_cash_receipt_no": "20190315900001",
        "target_adjustment_date": "2018-12-27",
    }]

    with pytest.raises(RuntimeError, match="unused/orphan"):
        builder._assert_kind_support_consumed(kind, [])

    builder._assert_kind_support_consumed(kind, [{
        "ticker": "001040",
        "support_actions": [{
            "support_action_source": "KRX_KIND",
            "support_action_key": "20181220002252",
            "support_action_type": "stock_dividend",
            "support_semantic_role": "ADJUSTMENT_COMPONENT",
            "target_cash_receipt_no": kind[0]["target_cash_receipt_no"],
            "target_adjustment_date": kind[0]["target_adjustment_date"],
        }],
    }])


def test_atomic_manifest_publish_restores_previous_on_second_read_failure(
    tmp_path, monkeypatch,
):
    destination = tmp_path / MANIFEST_RELATIVE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"previous-canonical-manifest")
    calls = []

    def fake_verify(_base, **_kwargs):
        calls.append(len(calls))
        return SimpleNamespace(
            row_count=1,
            metadata={"digest": "first" if len(calls) == 1 else "changed"},
        )

    monkeypatch.setattr(builder, "verify_source_evidence_manifest", fake_verify)
    with pytest.raises(RuntimeError, match="roundtrip changed"):
        builder._publish_source_manifest(
            tmp_path,
            {"schema_version": "fixture"},
            coverage_start=date(2026, 1, 1),
            coverage_end=date(2026, 12, 31),
            expected_parent_count=1,
        )

    assert len(calls) == 2
    assert destination.read_bytes() == b"previous-canonical-manifest"


def test_downloader_uses_head_then_if_match_and_verifies_md5_sha(tmp_path):
    inputs = _synthetic_inputs()
    calls = []

    def fake_aws(arguments, *, profile, region):
        calls.append(tuple(arguments))
        key = arguments[arguments.index("--key") + 1]
        trade_date = key.split("date=")[1].split("/")[0]
        fixture = tmp_path / f"fixture-{trade_date}.parquet"
        if not fixture.is_file():
            pd.DataFrame([{"Code": "000001", "Date": trade_date}]).to_parquet(
                fixture, index=False,
            )
        etag = _md5(fixture)
        if arguments[:2] == ["s3api", "head-object"]:
            return {
                "ETag": f'"{etag}"',
                "ContentLength": fixture.stat().st_size,
                "ServerSideEncryption": "AES256",
            }
        assert arguments[:2] == ["s3api", "get-object"]
        assert arguments[arguments.index("--if-match") + 1] == etag
        Path(arguments[-1]).write_bytes(fixture.read_bytes())
        return {"ETag": f'"{etag}"'}

    result = builder.download_price_objects(
        tmp_path,
        inputs,
        s3_root="s3://bronze-bucket",
        profile="teamalpha",
        region="ap-northeast-2",
        aws_runner=fake_aws,
    )

    assert len(result) == 49
    assert len(calls) == 98
    assert all(calls[index][:2] == ("s3api", "head-object") for index in range(0, 98, 2))
    assert all("--if-match" in calls[index] for index in range(1, 98, 2))


def test_downloader_rejects_get_version_when_head_was_unversioned(tmp_path):
    inputs = _synthetic_inputs()

    def fake_aws(arguments, *, profile, region):
        key = arguments[arguments.index("--key") + 1]
        trade_date = key.split("date=")[1].split("/")[0]
        fixture = tmp_path / f"fixture-{trade_date}.parquet"
        if not fixture.is_file():
            pd.DataFrame([{"Code": "000001", "Date": trade_date}]).to_parquet(
                fixture, index=False,
            )
        etag = _md5(fixture)
        if arguments[:2] == ["s3api", "head-object"]:
            return {"ETag": f'"{etag}"', "ContentLength": fixture.stat().st_size}
        Path(arguments[-1]).write_bytes(fixture.read_bytes())
        return {"ETag": f'"{etag}"', "VersionId": "unexpected-version"}

    with pytest.raises(RuntimeError, match="versioned price object during GET"):
        builder.download_price_objects(
            tmp_path,
            inputs,
            s3_root="s3://bronze-bucket",
            profile="teamalpha",
            region="ap-northeast-2",
            aws_runner=fake_aws,
        )


def test_price_downloader_requires_get_etag_receipt(tmp_path):
    inputs = _synthetic_inputs()

    def fake_aws(arguments, *, profile, region):
        key = arguments[arguments.index("--key") + 1]
        trade_date = key.split("date=")[1].split("/")[0]
        fixture = tmp_path / f"fixture-{trade_date}.parquet"
        if not fixture.is_file():
            pd.DataFrame([{"Code": "000001", "Date": trade_date}]).to_parquet(
                fixture, index=False,
            )
        etag = _md5(fixture)
        if arguments[:2] == ["s3api", "head-object"]:
            return {"ETag": f'"{etag}"', "ContentLength": fixture.stat().st_size}
        Path(arguments[-1]).write_bytes(fixture.read_bytes())
        return {}

    with pytest.raises(RuntimeError, match="changed during GET"):
        builder.download_price_objects(
            tmp_path,
            inputs,
            s3_root="s3://bronze-bucket",
            profile="teamalpha",
            region="ap-northeast-2",
            aws_runner=fake_aws,
        )


def test_price_publish_restores_all_bodies_and_manifest_on_second_read_failure(
    tmp_path, monkeypatch,
):
    inputs = _synthetic_inputs()
    previous_objects = [
        _price(tmp_path, "000001", trade_date, 100.0, 1.0)
        for trade_date in inputs.price_dates
    ]
    manifest_path = tmp_path / builder.PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    previous_manifest = builder._canonical_bytes(builder._price_object_payload(
        previous_objects, bucket="bronze-bucket", prefix="",
    ))
    builder._atomic_write(manifest_path, previous_manifest)
    previous_bodies = {
        item.local_path: (tmp_path / item.local_path).read_bytes()
        for item in previous_objects
    }

    def fake_aws(arguments, *, profile, region):
        key = arguments[arguments.index("--key") + 1]
        trade_date = key.split("date=")[1].split("/")[0]
        fixture = tmp_path / f"replacement-{trade_date}.parquet"
        if not fixture.is_file():
            pd.DataFrame([{
                "Code": "999999", "Date": trade_date, "replacement": True,
            }]).to_parquet(fixture, index=False)
        etag = _md5(fixture)
        if arguments[:2] == ["s3api", "head-object"]:
            return {"ETag": f'"{etag}"', "ContentLength": fixture.stat().st_size}
        Path(arguments[-1]).write_bytes(fixture.read_bytes())
        return {"ETag": f'"{etag}"'}

    real_verify = builder.verify_price_object_manifest
    calls = []

    def flaky_verify(*args, **kwargs):
        result = real_verify(*args, **kwargs)
        calls.append(result)
        if len(calls) == 2:
            raise RuntimeError("synthetic second-read failure")
        return result

    monkeypatch.setattr(builder, "verify_price_object_manifest", flaky_verify)
    with pytest.raises(RuntimeError, match="second-read failure"):
        builder.download_price_objects(
            tmp_path,
            inputs,
            s3_root="s3://bronze-bucket",
            profile="teamalpha",
            region="ap-northeast-2",
            aws_runner=fake_aws,
        )

    assert len(calls) == 2
    assert manifest_path.read_bytes() == previous_manifest
    assert {
        path: (tmp_path / path).read_bytes() for path in previous_bodies
    } == previous_bodies


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("bucket", "source root mismatch"),
        ("source_key", "source key mismatch"),
        ("multipart_etag", "single-part ETag"),
        ("version", "versioned"),
        ("extra_top", "fields changed"),
        ("extra_row", "entry fields changed"),
        ("noncanonical", "not canonical JSON bytes"),
    ],
)
def test_price_manifest_rejects_provenance_rewrite(tmp_path, mutation, message):
    inputs = _synthetic_inputs()
    objects = []
    for trade_date in inputs.price_dates:
        item = _price(tmp_path, "000001", trade_date, 100, 1)
        objects.append(item)
    payload = builder._price_object_payload(
        objects, bucket="bronze-bucket", prefix=""
    )
    if mutation == "bucket":
        payload["source_bucket"] = "other"
    elif mutation == "source_key":
        payload["objects"][0]["source_object_key"] = "stock/other.parquet"
    elif mutation == "multipart_etag":
        payload["objects"][0]["etag"] += "-1"
    elif mutation == "version":
        payload["objects"][0]["version_id"] = "v1"
    elif mutation == "extra_top":
        payload["aws_secret_access_key"] = "must-never-survive"
    elif mutation == "extra_row":
        payload["objects"][0]["profile"] = "must-never-survive"
    payload["object_digest"] = hashlib.sha256(
        builder._canonical_bytes(payload["objects"])
    ).hexdigest()
    path = tmp_path / builder.PRICE_OBJECT_MANIFEST_RELATIVE_PATH
    raw = builder._canonical_bytes(payload)
    if mutation == "noncanonical":
        raw = b" \n" + raw
    builder._atomic_write(path, raw)

    with pytest.raises(RuntimeError, match=message):
        builder.verify_price_object_manifest(
            tmp_path, inputs, expected_s3_root="s3://bronze-bucket"
        )


def test_native_marker_verifier_requires_v3_v3_v5_complete(tmp_path):
    interval = (
        tmp_path / "corporate_actions/dart/manifests/"
        "from=20200101/to=20200102"
    )
    interval.mkdir(parents=True)
    (interval / "disclosures_v3.json").write_text("[]", encoding="utf-8")
    (interval / "structured_complete_v3.json").write_text(json.dumps({
        "status": "COMPLETE", "fromdate": "20200101", "todate": "20200102",
        "query_count": 0,
    }), encoding="utf-8")
    # The previous marker proves a narrower document keyword set and must
    # never be accepted after adding bonus-decision correction bodies.
    (interval / "documents_complete_v4.json").write_text(json.dumps({
        "status": "COMPLETE", "fromdate": "20200101", "todate": "20200102",
        "candidate_count": 0,
    }), encoding="utf-8")
    with pytest.raises(RuntimeError, match="no complete"):
        builder.verify_fresh_dart_snapshot(
            tmp_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
        )
    (interval / "documents_complete_v5.json").write_text(json.dumps({
        "status": "COMPLETE", "fromdate": "20200101", "todate": "20200102",
        "candidate_count": 0,
    }), encoding="utf-8")

    verified = builder.verify_fresh_dart_snapshot(
        tmp_path,
        coverage_start=date(2020, 1, 1),
        coverage_end=date(2020, 1, 2),
    )
    assert verified == ((date(2020, 1, 1), date(2020, 1, 2)),)

    (interval / "documents_complete_v5.json").unlink()
    with pytest.raises(RuntimeError, match="no complete|ends early"):
        builder.verify_fresh_dart_snapshot(
            tmp_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
        )


def test_cli_requires_s3_root_for_verify_and_build():
    verify = builder.parse_args([
        "verify-prices", "--base", "/tmp/base", "--overlap", "/tmp/o",
        "--expectations", "/tmp/e", "--s3-root", "s3://bucket",
    ])
    build = builder.parse_args([
        "build", "--base", "/tmp/base", "--overlap", "/tmp/o",
        "--expectations", "/tmp/e", "--coverage-end", "2026-08-10",
        "--s3-root", "s3://bucket",
    ])
    assert verify.s3_root == "s3://bucket"
    assert build.coverage_end == date(2026, 8, 10)

    kind = builder.parse_args([
        "download-kind", "--base", "/tmp/base",
        "--reference-requests", "/tmp/reference-v2.json",
        "--component-requests", "/tmp/component-v1.json",
    ])
    assert kind.reference_requests == "/tmp/reference-v2.json"
    assert kind.component_requests == "/tmp/component-v1.json"


def test_s3_root_parser_accepts_only_canonical_secret_free_uri():
    assert builder._parse_s3_root("s3://bronze-bucket") == (
        "bronze-bucket", "",
    )
    assert builder._parse_s3_root("s3://bronze-bucket/team-alpha/date=2026") == (
        "bronze-bucket", "team-alpha/date=2026",
    )
    invalid = (
        "s3://user:super-secret@bronze-bucket/prefix",
        "s3://bronze-bucket:443/prefix",
        "s3://bronze-bucket/prefix?token=super-secret",
        "s3://bronze-bucket/prefix#fragment",
        "s3://bronze-bucket//prefix",
        "s3://bronze-bucket/prefix/",
        "s3://bronze-bucket/.",
        "s3://bronze-bucket/../prefix",
        "s3://bronze-bucket/pre\\fix",
        "s3://bronze-bucket/pre%2Ffix",
        "s3://Bronze-Bucket/prefix",
        "s3://127.0.0.1/prefix",
        "s3://bronze-bucket/pre\nfix",
    )
    for value in invalid:
        with pytest.raises(ValueError) as caught:
            builder._parse_s3_root(value)
        rendered = str(caught.value)
        assert rendered == "price source root is not a canonical safe S3 URI"
        assert "super-secret" not in rendered
