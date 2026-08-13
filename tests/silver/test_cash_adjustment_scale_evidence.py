import hashlib
import json
import zipfile
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import pandas as pd
import pytest

from pipeline.silver import cash_adjustment_scale_evidence as evidence
from pipeline.bronze import dart_support_action_families


def _verify(base: str | Path):
    root = Path(base)
    payload = json.loads(
        (root / dart_support_action_families.MANIFEST_RELATIVE_PATH)
        .read_text(encoding="utf-8")
    )
    return evidence.verify_source_evidence_manifest(
        str(root),
        required_start=date.fromisoformat(payload["seed_coverage_start"]),
        required_end=date.fromisoformat(payload["seed_coverage_end"]),
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, default=str, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _zip(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("document.xml", text)


def _dart_numbered_notice(
    *,
    ticker: str = "005950",
    effective: date = date(2021, 12, 29),
    reference: str = "4,960",
    reason: str = "주식배당",
    security: str = "보통주식",
    date_label: str = "배당락 실시일",
) -> str:
    return (
        "<document><table><tr>"
        "<td>1. 회사명</td><td>2. 주권종류</td><td>3. 단축코드</td>"
        f"<td>4. 기준가(원)</td><td>5. {date_label}</td>"
        "<td>6. 사유</td></tr><tr><td>테스트회사</td>"
        f"<td>{security}</td><td>A{ticker}</td><td>{reference}</td>"
        f"<td>{effective.isoformat()}</td><td>{reason}</td>"
        "</tr></table></document>"
    )


def _numbered_notice_support(
    root: Path,
    *,
    reason: str = "주식배당",
    action_type: str = "ex_dividend",
    date_label: str = "배당락 실시일",
    groups: tuple[str, ...] = (
        "005950|2021-12-31|STOCK_DIVIDEND|0.1",
    ),
) -> tuple[dict, dict, Path]:
    body = (
        root / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211228900001.zip"
    )
    _zip(body, _dart_numbered_notice(
        reason=reason, date_label=date_label,
    ))
    row = _support(
        key="20211228900001", source="DART_DISCLOSURE",
        action_type=action_type, path=str(body.relative_to(root)),
        body_sha=_sha(body), report="배당락", groups=list(groups),
        role="CORROBORATION", announcement=date(2021, 12, 28),
        ex_date=date(2021, 12, 29), reference=4_960, reason=reason,
        entitlement="COMMON",
    )
    parent = {
        "ticker": "005950",
        "adjustment_trade_date": date(2021, 12, 29),
        "raw_reference_price": 4_960,
    }
    return parent, row, body


def _legacy_combined_notice(
    *,
    ticker: str = "005950",
    effective: date = date(2021, 12, 29),
    reference: str = "4,960",
    reason: str = "무상증자 및 배당",
    security: str = "보통주식",
) -> str:
    return (
        "<document>권배락<table>"
        f"<tr><td>1. 권배락 실시일</td><td>{effective.isoformat()}</td></tr>"
        f"<tr><td>2. 권배락 사유</td><td>{reason}</td></tr>"
        "<tr><td>3. 권배락 내역</td><td>회사명</td>"
        "<td>주권종류</td><td>단축코드</td><td>기준가(원)</td></tr>"
        f"<tr><td></td><td>삼성전자</td><td>{security}</td>"
        f"<td>A{ticker}</td><td>{reference}</td></tr>"
        "</table></document>"
    )


def _support(
    *, key, source, action_type, path, body_sha, report, groups, role,
    announcement=date(2021, 12, 17), ex_date=None, record_date=None,
    expected=None, reference=None, reason=None, ratio_numerator=None,
    ratio_denominator=None, entitlement=None, distributed=None,
):
    row = {
        "evidence_key": "005950:20220214901227:2021-12-29",
        "support_action_source": source,
        "support_action_key": key,
        "support_action_type": action_type,
        "target_cash_receipt_no": "20220214901227",
        "target_adjustment_date": date(2021, 12, 29),
        "support_action_body_path": path,
        "support_action_body_sha256": body_sha,
        "support_announcement_date": announcement,
        "support_ex_date": ex_date,
        "support_record_date": record_date,
        "support_ratio_numerator": ratio_numerator,
        "support_ratio_denominator": ratio_denominator,
        "support_entitlement_security_class": entitlement,
        "support_distributed_security_class": distributed,
        "support_expected_price_factor": expected,
        "support_reference_price": reference,
        "support_reason": reason,
        "support_report_name": report,
        "support_action_scope": "ISSUER",
        "support_semantic_group_keys": json.dumps(
            sorted(groups), ensure_ascii=False, separators=(",", ":"),
        ),
        "support_semantic_role": role,
    }
    row["manifest_support_row_sha256"] = evidence._manifest_support_row_sha(row)
    return row


def _write_support_families(root: Path) -> None:
    rows = [
        {
            "rcept_no": "20211217000406", "rcept_dt": "20211217",
            "stock_code": "005950", "corp_code": "00100001",
            "corp_cls": "Y",
            "report_nm": "주요사항보고서(무상증자결정)",
        },
        {
            "rcept_no": "20211224900781", "rcept_dt": "20211224",
            "stock_code": "005950", "corp_code": "00100001",
            "corp_cls": "Y", "report_nm": "주식배당결정",
        },
    ]
    interval = (
        root / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231"
    )
    interval.mkdir(parents=True, exist_ok=True)
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    marker = {
        "status": "COMPLETE", "fromdate": "20211201", "todate": "20211231",
    }
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": 1}), encoding="utf-8",
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
            / "year=2021" / f"date={rendered}" / "corp=005950"
            / f"rcept={row['rcept_no']}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(row, ensure_ascii=False), encoding="utf-8")
    bodies = {
        "20211217000406": b"<html><body>bonus issue</body></html>",
        "20211224900781": (
            "<html><body><table><tr><td>1주당 배당주식수(주)</td>"
            "<td>보통주식</td><td>0.1</td></tr></table></body></html>"
        ).encode(),
    }

    def fetcher(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        dcm = str(1_000_000 + int(receipt[-6:]))
        if parsed.path.endswith("/main.do"):
            return (
                f'<script>viewDoc("{receipt}", "{dcm}", "0", "0", "0", '
                f'"HTML", "");</script><select id="family"><option '
                f'value="rcpNo={receipt}" selected>{receipt}</option></select>'
                '<select id="att"></select>'
            ).encode()
        assert query["dcmNo"] == [dcm]
        return bodies[receipt]

    dart_support_action_families.collect_support_action_families(
        root,
        coverage_end=date(2026, 12, 31),
        apply=True,
        fetcher=fetcher,
    )


def _fixture(root: Path):
    cash = root / "corporate_actions/dart/documents/year=2022/corp=005950/rcept=20220214901227.zip"
    _zip(cash, "<document>현금배당 1주당 배당금 보통주식 100 배당기준일 2021-12-31</document>")
    bonus = root / "corporate_actions/dart/structured/event=bonus_issue/year=2021/corp=005950/rcept=20211217000406.json"
    bonus.parent.mkdir(parents=True, exist_ok=True)
    bonus.write_text(json.dumps({
        "rcept_no": "20211217000406",
        "nstk_ascnt_ps_ostk": "1.0",
    }), encoding="utf-8")
    stock = root / "corporate_actions/dart/documents/year=2021/corp=005950/rcept=20211224900781.zip"
    _zip(
        stock,
        "<document>주식배당결정 배당기준일 2021-12-31<table>"
        "<tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
        "<td>0.1</td></tr></table></document>",
    )
    combined = root / "corporate_actions/dart/documents/year=2021/corp=005950/rcept=20211228900755.zip"
    _zip(
        combined,
        "<document>권배락<table>"
        "<tr><td>1. 권배락 실시일</td><td>2021-12-29</td></tr>"
        "<tr><td>2. 권배락 사유</td><td>무상증자 및 배당</td></tr>"
        "<tr><td>3. 권배락 내역</td><td>회사명</td>"
        "<td>주권종류</td><td>단축코드</td><td>기준가(원)</td></tr>"
        "<tr><td></td><td>삼성전자</td><td>보통주식</td>"
        "<td>A005950</td><td>4,960</td></tr>"
        "</table></document>",
    )
    previous = root / "stock/marcap/date=2021-12-28/all.parquet"
    previous.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "Code": "005950", "Date": date(2021, 12, 28),
        "Close": 10200, "Changes": 100,
    }]).to_parquet(previous, index=False)
    applied = root / "stock/marcap/date=2021-12-29/all.parquet"
    applied.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "Code": "005950", "Date": date(2021, 12, 29),
        "Close": 4900, "Changes": -60,
    }]).to_parquet(applied, index=False)
    _write_support_families(root)

    bonus_group = "005950|2021-12-31|BONUS_ISSUE|1.0"
    stock_group = "005950|2021-12-31|STOCK_DIVIDEND|0.1"
    supports = [
        _support(
            key="20211217000406", source="DART_STRUCTURED",
            action_type="bonus_issue",
            path=str(bonus.relative_to(root)), body_sha=_sha(bonus),
            report="주요사항보고서(무상증자결정)", groups=[bonus_group],
            role="ADJUSTMENT_COMPONENT", expected=0.5,
            ratio_numerator=1.0, ratio_denominator=1.0,
            entitlement="COMMON", distributed="COMMON",
        ),
        _support(
            key="20211224900781", source="DART_DISCLOSURE",
            action_type="stock_dividend",
            path=str(stock.relative_to(root)), body_sha=_sha(stock),
            report="주식배당결정", groups=[stock_group],
            role="ADJUSTMENT_COMPONENT", announcement=date(2021, 12, 24),
            record_date=date(2021, 12, 31), ratio_numerator=0.1,
            ratio_denominator=1, entitlement="COMMON", distributed="COMMON",
        ),
        _support(
            key="20211228900755", source="DART_DISCLOSURE",
            action_type="combined_detachment",
            path=str(combined.relative_to(root)), body_sha=_sha(combined),
            report="권배락(무상증자 및 배당)",
            groups=[bonus_group, stock_group], role="CORROBORATION",
            announcement=date(2021, 12, 28), ex_date=date(2021, 12, 29),
            reference=4960, reason="무상증자 및 배당",
        ),
    ]
    support_frame = pd.DataFrame(supports)
    parent = {
        "evidence_key": "005950:20220214901227:2021-12-29",
        "ticker": "005950",
        "cash_receipt_no": "20220214901227",
        "cash_source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
        "cash_action_body_path": str(cash.relative_to(root)),
        "cash_action_body_sha256": _sha(cash),
        "cash_economic_body_path": str(cash.relative_to(root)),
        "cash_economic_body_schema": "OPENDART_DOCUMENT_ZIP_V1",
        "cash_economic_sha256": _sha(cash),
        "support_action_count": len(supports),
        "support_action_digest": evidence.support_manifest_digest(support_frame),
        "support_semantic_group_count": 2,
        "price_source": "KRX",
        "previous_price_source_object_key": str(previous.relative_to(root)),
        "previous_price_source_content_sha256": _sha(previous),
        "previous_price_source_etag": hashlib.md5(
            previous.read_bytes(), usedforsecurity=False,
        ).hexdigest(),
        "previous_price_source_schema": "marcap_parquet_v1",
        "adjustment_price_source_object_key": str(applied.relative_to(root)),
        "adjustment_price_source_content_sha256": _sha(applied),
        "adjustment_price_source_etag": hashlib.md5(
            applied.read_bytes(), usedforsecurity=False,
        ).hexdigest(),
        "adjustment_price_source_schema": "marcap_parquet_v1",
        "previous_trade_date": date(2021, 12, 28),
        "adjustment_trade_date": date(2021, 12, 29),
        "raw_previous_close": 10200,
        "raw_applied_close": 4900,
        "raw_reference_price": 4960,
        "expected_price_factor": 4960 / 10200,
        "cash_scale_basis": evidence.PRE_EVENT_PRICE_SCALE,
    }
    parent["manifest_row_sha256"] = evidence._manifest_row_sha(parent)
    parent_with_support = {**parent, "support_actions": supports}
    parent_frame = pd.DataFrame([parent])
    manifest = {
        "schema_version": evidence.SOURCE_EVIDENCE_CONTRACT,
        "complete": True,
        "row_count": 1,
        "row_digest": evidence.source_manifest_digest(parent_frame),
        "support_action_count": 3,
        "support_action_digest": evidence.support_manifest_digest(support_frame),
        "support_semantic_group_count": 2,
        "evidence": [parent_with_support],
    }
    manifest_path = root / evidence.MANIFEST_RELATIVE_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(_canonical_json(manifest))
    return parent, supports, manifest_path


def _refresh_manifest(manifest_path: Path, manifest: dict | None = None) -> dict:
    """Recompute every content digest after an intentional test mutation."""
    if manifest is None:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parent_rows = []
    all_supports = []
    all_groups: set[str] = set()
    for parent in manifest["evidence"]:
        supports = parent["support_actions"]
        for support in supports:
            support["manifest_support_row_sha256"] = (
                evidence._manifest_support_row_sha(support)
            )
            decoded = json.loads(support["support_semantic_group_keys"])
            all_groups.update(str(value).strip() for value in decoded)
        support_frame = pd.DataFrame(supports)
        parent["support_action_digest"] = evidence.support_manifest_digest(
            support_frame
        )
        parent["manifest_row_sha256"] = evidence._manifest_row_sha(parent)
        parent_rows.append({
            key: value for key, value in parent.items()
            if key != "support_actions"
        })
        all_supports.extend(supports)
    manifest["row_count"] = len(parent_rows)
    manifest["row_digest"] = evidence.source_manifest_digest(
        pd.DataFrame(parent_rows)
    )
    manifest["support_action_count"] = len(all_supports)
    manifest["support_action_digest"] = evidence.support_manifest_digest(
        pd.DataFrame(all_supports)
    )
    manifest["support_semantic_group_count"] = len(all_groups)
    manifest_path.write_bytes(_canonical_json(manifest))
    return manifest


def test_public_manifest_row_digest_helpers_match_frozen_contract(tmp_path):
    parent, supports, _ = _fixture(tmp_path)
    assert evidence.manifest_parent_row_sha256(parent) == parent[
        "manifest_row_sha256"
    ]
    for support in supports:
        assert evidence.manifest_support_row_sha256(support) == support[
            "manifest_support_row_sha256"
        ]


def test_manifest_accepts_generic_numbered_dart_dividend_notice(tmp_path):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    notice = manifest["evidence"][0]["support_actions"][2]
    body = tmp_path / notice["support_action_body_path"]
    _zip(body, _dart_numbered_notice())
    stock_group = manifest["evidence"][0]["support_actions"][1][
        "support_semantic_group_keys"
    ]
    notice.update({
        "support_action_type": "ex_dividend",
        "support_action_body_sha256": _sha(body),
        "support_report_name": "배당락",
        "support_semantic_group_keys": stock_group,
        "support_reason": "주식배당",
    })
    _refresh_manifest(manifest_path, manifest)

    verified = _verify(str(tmp_path))

    exact = verified.support_frame[
        verified.support_frame["support_action_key"].eq("20211228900755")
    ].iloc[0]
    assert exact["support_action_type"] == "ex_dividend"
    assert exact["support_report_name"] == "배당락"


def test_manifest_accepts_valid_notice_beside_incomplete_correction(tmp_path):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    notice = manifest["evidence"][0]["support_actions"][2]
    body = tmp_path / notice["support_action_body_path"]
    with zipfile.ZipFile(body, "w") as archive:
        archive.writestr(
            "correction.xml",
            "<document><table><tr><td>5. 배당락 실시일</td>"
            "<td>2021-12-29</td></tr></table></document>",
        )
        archive.writestr("notice.xml", _dart_numbered_notice())
    stock_group = manifest["evidence"][0]["support_actions"][1][
        "support_semantic_group_keys"
    ]
    notice.update({
        "support_action_type": "ex_dividend",
        "support_action_body_sha256": _sha(body),
        "support_report_name": "배당락",
        "support_semantic_group_keys": stock_group,
        "support_reason": "주식배당",
    })
    _refresh_manifest(manifest_path, manifest)

    assert _verify(str(tmp_path)).row_count == 1


@pytest.mark.parametrize(
    ("reason", "action_type", "date_label", "group"),
    [
        (
            "현금배당", "ex_dividend", "배당락 실시일",
            "005950|2021-12-31|STOCK_DIVIDEND|0.1",
        ),
        (
            "유상증자", "rights_detachment", "권리락 실시일",
            "005950|2021-12-31|BONUS_ISSUE|1",
        ),
    ],
)
def test_evidence_rejects_notice_reason_group_forgery(
    tmp_path, reason, action_type, date_label, group,
):
    parent, row, _ = _numbered_notice_support(
        tmp_path, reason=reason, action_type=action_type,
        date_label=date_label, groups=(group,),
    )

    with pytest.raises(RuntimeError, match="reason/group semantics"):
        evidence._verify_support_body(tmp_path, parent, row)


def test_support_body_swap_after_frozen_read_fails_closed(tmp_path, monkeypatch):
    parent, row, body = _numbered_notice_support(tmp_path)
    forged = tmp_path / "forged.zip"
    _zip(
        forged,
        _dart_numbered_notice().replace("테스트회사", "동기화위조회사"),
    )
    forged_bytes = forged.read_bytes()
    original = evidence._read_regular_file_no_follow
    calls = 0

    def swap_after_first_read(path):
        nonlocal calls
        payload, identity = original(path)
        calls += 1
        if calls == 1:
            body.write_bytes(forged_bytes)
        return payload, identity

    monkeypatch.setattr(
        evidence, "_read_regular_file_no_follow", swap_after_first_read,
    )

    with pytest.raises(
        RuntimeError,
        match="changed (?:while being frozen|during verification)",
    ):
        evidence._verify_support_body(tmp_path, parent, row)


def test_support_body_symlink_is_rejected(tmp_path):
    parent, row, body = _numbered_notice_support(tmp_path)
    target = tmp_path / "immutable-target.zip"
    target.write_bytes(body.read_bytes())
    body.unlink()
    body.symlink_to(target)

    with pytest.raises(RuntimeError, match="contains a symlink"):
        evidence._verify_support_body(tmp_path, parent, row)


def test_invocation_end_gate_rejects_post_validation_support_swap(
    tmp_path, monkeypatch,
):
    _, supports, _ = _fixture(tmp_path)
    target = tmp_path / supports[0]["support_action_body_path"]
    forged = target.read_bytes() + b"\n"
    original = evidence._validate_support_family_bindings

    def swap_after_family_validation(*args, **kwargs):
        result = original(*args, **kwargs)
        target.write_bytes(forged)
        return result

    monkeypatch.setattr(
        evidence,
        "_validate_support_family_bindings",
        swap_after_family_validation,
    )

    with pytest.raises(RuntimeError, match="changed during verification"):
        _verify(tmp_path)


def test_invocation_end_gate_rejects_source_manifest_swap(
    tmp_path, monkeypatch,
):
    _, _, manifest_path = _fixture(tmp_path)
    original = evidence._validate_kind_support_bindings

    def swap_after_semantic_validation(*args, **kwargs):
        result = original(*args, **kwargs)
        manifest_path.write_bytes(manifest_path.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(
        evidence,
        "_validate_kind_support_bindings",
        swap_after_semantic_validation,
    )

    with pytest.raises(RuntimeError, match="changed during verification"):
        _verify(tmp_path)


def test_invocation_end_gate_rejects_support_family_manifest_swap(
    tmp_path, monkeypatch,
):
    _fixture(tmp_path)
    support_manifest = (
        tmp_path / dart_support_action_families.MANIFEST_RELATIVE_PATH
    )
    original = evidence._validate_support_family_bindings
    mutated = False

    def swap_after_family_validation(*args, **kwargs):
        nonlocal mutated
        result = original(*args, **kwargs)
        if not mutated:
            support_manifest.write_bytes(support_manifest.read_bytes() + b"\n")
            mutated = True
        return result

    monkeypatch.setattr(
        evidence,
        "_validate_support_family_bindings",
        swap_after_family_validation,
    )

    with pytest.raises(RuntimeError, match="changed during verification"):
        _verify(tmp_path)


def test_invocation_end_gate_rejects_kind_manifest_appearing(
    tmp_path, monkeypatch,
):
    _fixture(tmp_path)
    kind_manifest = tmp_path / evidence.KIND_SUPPORT_MANIFEST_RELATIVE_PATH
    assert not kind_manifest.exists()
    original = evidence._validate_kind_support_bindings
    mutated = False

    def create_after_kind_validation(*args, **kwargs):
        nonlocal mutated
        result = original(*args, **kwargs)
        if not mutated:
            kind_manifest.parent.mkdir(parents=True, exist_ok=True)
            kind_manifest.write_bytes(b"{}")
            mutated = True
        return result

    monkeypatch.setattr(
        evidence,
        "_validate_kind_support_bindings",
        create_after_kind_validation,
    )

    with pytest.raises(RuntimeError, match="existence changed"):
        _verify(tmp_path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("support_reference_price", 4_959, "reference price changed"),
        ("support_reason", "무상증자와 주식배당", "reason changed"),
    ],
)
def test_manifest_rejects_fully_rehashed_notice_value_drift(
    tmp_path, field, value, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    manifest["evidence"][0]["support_actions"][2][field] = value
    _refresh_manifest(manifest_path, manifest)

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("mutation", "value"),
    [
        ("ticker", "005951"),
        ("security", "우선주식"),
        ("effective", date(2021, 12, 30)),
        ("action_type", "ex_dividend"),
    ],
)
def test_manifest_rejects_fully_rehashed_notice_identity_drift(
    tmp_path, mutation, value,
):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    notice = manifest["evidence"][0]["support_actions"][2]
    if mutation == "action_type":
        notice["support_action_type"] = value
    else:
        arguments = {mutation: value}
        body = tmp_path / notice["support_action_body_path"]
        _zip(body, _legacy_combined_notice(**arguments))
        notice["support_action_body_sha256"] = _sha(body)
    _refresh_manifest(manifest_path, manifest)

    with pytest.raises(RuntimeError, match="identity changed"):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("target_cash_receipt_no", "20220214901228", "target cash receipt"),
        ("target_adjustment_date", "2021-12-30", "target adjustment date"),
    ],
)
def test_manifest_rejects_support_cross_parent_target_swap(
    tmp_path, field, value, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    payload = json.loads(manifest_path.read_bytes())
    payload["evidence"][0]["support_actions"][0][field] = value
    manifest_path.write_bytes(_canonical_json(payload))

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("noncanonical", "not canonical JSON bytes"),
        ("top", "manifest fields changed"),
        ("parent", "parent fields changed"),
        ("support", "support row fields changed"),
    ],
)
def test_source_manifest_rejects_unknown_or_noncanonical_fields(
    tmp_path, mutation, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    if mutation == "noncanonical":
        manifest_path.write_bytes(manifest_path.read_bytes() + b"\n")
    else:
        payload = json.loads(manifest_path.read_bytes())
        if mutation == "top":
            payload["aws_secret"] = "must-not-survive"
        elif mutation == "parent":
            payload["evidence"][0]["secret_profile"] = "must-not-survive"
        else:
            payload["evidence"][0]["support_actions"][0]["secret"] = "x"
        manifest_path.write_bytes(_canonical_json(payload))

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


def _receipts(parent):
    return pd.DataFrame([{
        "receipt_no": parent["cash_receipt_no"], "asset_id": 7,
        "ticker": "005950",
        "economic_evidence_sha256": parent["cash_economic_sha256"],
        "source_evidence_status": parent["cash_source_evidence_status"],
        "mapping_status": "INCLUDED", "is_terminal_economic_revision": True,
        "record_date": date(2021, 12, 31),
    }])


def _published(parent, supports, run_id):
    common = {
        "asset_id": 7, "quality_run_id": run_id,
        "ratio_numerator": None, "ratio_denominator": None,
    }
    rows = [{
        **common,
        "source": "DART_DISCLOSURE", "action_key": parent["cash_receipt_no"],
        "action_type": "cash_dividend",
        "source_body_sha256": parent["cash_action_body_sha256"],
        "announcement_date": date(2022, 2, 14), "ex_date": None,
        "record_date": date(2021, 12, 31), "expected_price_factor": None,
        "report_name": "현금ㆍ현물배당결정", "action_scope": "ISSUER",
    }]
    for support in supports:
        rows.append({
            **common,
            "source": support["support_action_source"],
            "action_key": support["support_action_key"],
            "action_type": support["support_action_type"],
            "source_body_sha256": support["support_action_body_sha256"],
            "announcement_date": support["support_announcement_date"],
            "ex_date": support["support_ex_date"],
            "record_date": support["support_record_date"],
            "ratio_numerator": support["support_ratio_numerator"],
            "ratio_denominator": support["support_ratio_denominator"],
            "expected_price_factor": support["support_expected_price_factor"],
            "report_name": support["support_report_name"],
            "action_scope": support["support_action_scope"],
        })
    return pd.DataFrame(rows)


def test_manifest_and_binding_preserve_composite_support_lineage(tmp_path):
    parent, supports, _ = _fixture(tmp_path)
    verified = _verify(str(tmp_path))
    run_id = uuid4()
    receipts = pd.DataFrame([{
        "receipt_no": parent["cash_receipt_no"], "asset_id": 7,
        "ticker": "005950", "economic_evidence_sha256": parent["cash_economic_sha256"],
        "source_evidence_status": parent["cash_source_evidence_status"],
        "mapping_status": "INCLUDED", "is_terminal_economic_revision": True,
        "record_date": date(2021, 12, 31),
    }])

    bound = evidence.bind_source_evidence(
        verified,
        receipt_frame=receipts,
        published_actions=_published(parent, supports, run_id),
        action_snapshot_run_id=run_id,
    )
    metadata = evidence.source_evidence_metadata(
        bound.frame, bound.support_frame, verified=verified,
    )

    assert len(bound.frame) == 1
    assert len(bound.support_frame) == 3
    assert metadata["persisted_support_semantic_group_count"] == 2
    assert metadata["changed_scale_coverage_count"] == 1
    assert metadata["unresolved_count"] == 0


def test_source_manifest_revalidation_rejects_family_terminal_replacement(
    tmp_path,
):
    _fixture(tmp_path)
    family_path = tmp_path / dart_support_action_families.MANIFEST_RELATIVE_PATH
    payload = json.loads(family_path.read_text(encoding="utf-8"))
    stock = next(
        entry for entry in payload["entries"]
        if entry["action_type"] == "stock_dividend"
    )
    stock["terminal_economic_receipt_no"] = "20211217000406"
    payload["entry_digest"] = hashlib.sha256(
        dart_support_action_families._canonical_bytes(payload["entries"])
    ).hexdigest()
    family_path.write_bytes(
        dart_support_action_families._canonical_bytes(payload)
    )

    with pytest.raises(RuntimeError, match="derived manifest row changed"):
        _verify(str(tmp_path))


def test_manifest_rejects_support_group_without_exactly_one_component(tmp_path):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["evidence"][0]["support_actions"][1][
        "support_semantic_role"
    ] = "CORROBORATION"
    manifest_path.write_bytes(_canonical_json(manifest))

    with pytest.raises(RuntimeError, match="row digest mismatch"):
        _verify(str(tmp_path))


def test_binding_rejects_cash_action_body_substitution(tmp_path):
    parent, supports, _ = _fixture(tmp_path)
    verified = _verify(str(tmp_path))
    run_id = uuid4()
    receipts = pd.DataFrame([{
        "receipt_no": parent["cash_receipt_no"], "asset_id": 7,
        "ticker": "005950", "economic_evidence_sha256": parent["cash_economic_sha256"],
        "source_evidence_status": parent["cash_source_evidence_status"],
        "mapping_status": "INCLUDED", "is_terminal_economic_revision": True,
        "record_date": date(2021, 12, 31),
    }])
    published = _published(parent, supports, run_id)
    published.loc[published["action_type"].eq("cash_dividend"), "source_body_sha256"] = "f" * 64

    with pytest.raises(RuntimeError, match="cash action body SHA parity"):
        evidence.bind_source_evidence(
            verified, receipt_frame=receipts, published_actions=published,
            action_snapshot_run_id=run_id,
        )


def test_manifest_rejects_previous_price_body_tamper(tmp_path):
    _fixture(tmp_path)
    previous = tmp_path / "stock/marcap/date=2021-12-28/all.parquet"
    previous.write_bytes(previous.read_bytes() + b"tamper")

    with pytest.raises(RuntimeError, match="SHA mismatch"):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("support_action_count", 4, "action-count mismatch"),
        ("support_semantic_group_count", 3, "group-count mismatch"),
        ("support_action_digest", "f" * 64, "support digest mismatch"),
    ],
)
def test_manifest_rejects_parent_child_aggregate_mismatch(
    tmp_path, field, value, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parent = manifest["evidence"][0]
    parent[field] = value
    # Keep the intentionally wrong parent declaration while making its own
    # row and top-level rowset digests internally consistent.
    parent["manifest_row_sha256"] = evidence._manifest_row_sha(parent)
    parent_frame = pd.DataFrame([{
        key: item for key, item in parent.items() if key != "support_actions"
    }])
    manifest["row_digest"] = evidence.source_manifest_digest(parent_frame)
    manifest_path.write_bytes(_canonical_json(manifest))

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("reverse", "sorted and unique"),
        ("duplicate", "sorted and unique"),
        ("component_multi_group", "exactly one group"),
        ("zero_component", "exactly one adjustment component"),
        ("two_components", "exactly one adjustment component"),
    ],
)
def test_manifest_rejects_invalid_semantic_group_graph(
    tmp_path, mutation, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    supports = manifest["evidence"][0]["support_actions"]
    bonus_group = json.loads(supports[0]["support_semantic_group_keys"])[0]
    stock_group = json.loads(supports[1]["support_semantic_group_keys"])[0]
    if mutation == "reverse":
        supports[2]["support_semantic_group_keys"] = json.dumps(
            [stock_group, bonus_group], ensure_ascii=False, separators=(",", ":"),
        )
    elif mutation == "duplicate":
        supports[2]["support_semantic_group_keys"] = json.dumps(
            [bonus_group, bonus_group], ensure_ascii=False, separators=(",", ":"),
        )
    elif mutation == "component_multi_group":
        supports[0]["support_semantic_group_keys"] = json.dumps(
            sorted([bonus_group, stock_group]),
            ensure_ascii=False, separators=(",", ":"),
        )
    elif mutation == "zero_component":
        supports[0]["support_semantic_role"] = "CORROBORATION"
    elif mutation == "two_components":
        duplicate_component = dict(supports[1])
        duplicate_component["support_action_key"] = "20211224900782"
        supports.append(duplicate_component)
        manifest["evidence"][0]["support_action_count"] = len(supports)
    _refresh_manifest(manifest_path, manifest)

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("previous_price_source_etag", "f" * 32, "ETag/body mismatch"),
        ("raw_previous_close", 10199, "previous close changed"),
        ("raw_applied_close", 4901, "applied close changed"),
        (
            "raw_reference_price", 4959,
            "reference price (?:mismatch|changed)",
        ),
        (
            "previous_price_source_schema", "krxapi_stock_parquet_v1",
            "invalid KRX evidence parquet",
        ),
    ],
)
def test_manifest_rejects_price_provenance_or_value_mutation(
    tmp_path, field, value, message,
):
    _, _, manifest_path = _fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["evidence"][0][field] = value
    _refresh_manifest(manifest_path, manifest)

    with pytest.raises(RuntimeError, match=message):
        _verify(str(tmp_path))


def test_manifest_rejects_semantically_different_valid_support_body(tmp_path):
    _, _, manifest_path = _fixture(tmp_path)
    replacement = (
        tmp_path
        / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211224900781-replacement.zip"
    )
    _zip(
        replacement,
        "<document>주식배당결정 배당기준일 2021-12-31 보통주 0.2주</document>",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    stock = manifest["evidence"][0]["support_actions"][1]
    stock["support_action_body_path"] = str(replacement.relative_to(tmp_path))
    stock["support_action_body_sha256"] = _sha(replacement)
    _refresh_manifest(manifest_path, manifest)

    with pytest.raises(RuntimeError, match="component ratio changed"):
        _verify(str(tmp_path))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("quality_run_id", "different-run", "run parity"),
        ("source_body_sha256", "f" * 64, "body SHA parity"),
        ("announcement_date", date(2021, 12, 23), "snapshot-field parity"),
        ("ratio_numerator", 0.2, "snapshot-field parity"),
        ("report_name", "다른 공시", "snapshot-field parity"),
    ],
)
def test_binding_rejects_support_action_snapshot_mutation(
    tmp_path, field, value, message,
):
    parent, supports, _ = _fixture(tmp_path)
    verified = _verify(str(tmp_path))
    run_id = uuid4()
    published = _published(parent, supports, run_id)
    target = published["action_type"].eq("stock_dividend")
    published.loc[target, field] = value

    with pytest.raises(RuntimeError, match=message):
        evidence.bind_source_evidence(
            verified,
            receipt_frame=_receipts(parent),
            published_actions=published,
            action_snapshot_run_id=run_id,
        )


def test_binding_rejects_unused_orphan_support_child(tmp_path):
    parent, supports, _ = _fixture(tmp_path)
    verified = _verify(str(tmp_path))
    orphan = verified.support_frame.iloc[[0]].copy()
    orphan["evidence_key"] = "orphan-evidence-key"
    verified_with_orphan = replace(
        verified,
        support_frame=pd.concat(
            [verified.support_frame, orphan], ignore_index=True,
        ),
    )
    run_id = uuid4()

    with pytest.raises(RuntimeError, match="left unused rows"):
        evidence.bind_source_evidence(
            verified_with_orphan,
            receipt_frame=_receipts(parent),
            published_actions=_published(parent, supports, run_id),
            action_snapshot_run_id=run_id,
        )


def test_kind_cross_class_stock_dividend_binds_cj_semantics(tmp_path):
    body = tmp_path / "corporate_actions/krx/kind/cj-final-corrected.html"
    body.parent.mkdir(parents=True)
    body.write_bytes(
        (Path(__file__).parents[1] / "fixtures/kind"
         / "001040-20181221-61474.html").read_bytes()
    )
    parent = {"adjustment_trade_date": date(2018, 12, 27)}
    row = {
        "support_action_source": "KRX_KIND",
        "support_action_key": "20181220002252",
        "support_action_type": "stock_dividend",
        "support_action_body_path": str(body.relative_to(tmp_path)),
        "support_action_body_sha256": _sha(body),
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_semantic_group_keys": '["cj-stock-dividend"]',
        "support_record_date": date(2018, 12, 31),
        "support_ratio_numerator": 0.15,
        "support_ratio_denominator": 1,
        "support_entitlement_security_class": "COMMON_AND_PREFERRED",
        "support_distributed_security_class": "NEW_PREFERRED",
        "support_report_name": "주식배당 결정",
    }

    evidence._verify_support_body(tmp_path, parent, row)

    row["support_entitlement_security_class"] = "COMMON"
    row["support_distributed_security_class"] = "COMMON"
    with pytest.raises(RuntimeError, match="KIND stock-dividend semantics changed"):
        evidence._verify_support_body(tmp_path, parent, row)


def test_kind_stock_dividend_ratio_requires_exact_numeric_token(tmp_path):
    body = tmp_path / "corporate_actions/krx/kind/cj-final-corrected.html"
    body.parent.mkdir(parents=True)
    body.write_bytes(
        (Path(__file__).parents[1] / "fixtures/kind"
         / "001040-20181221-61474.html").read_bytes()
    )
    row = {
        "support_action_source": "KRX_KIND",
        "support_action_key": "20181220002252",
        "support_action_type": "stock_dividend",
        "support_action_body_path": str(body.relative_to(tmp_path)),
        "support_action_body_sha256": _sha(body),
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_semantic_group_keys": '["cj-stock-dividend"]',
        "support_record_date": date(2018, 12, 31),
        "support_ratio_numerator": 0.1,
        "support_ratio_denominator": 1,
        "support_entitlement_security_class": "COMMON_AND_PREFERRED",
        "support_distributed_security_class": "NEW_PREFERRED",
        "support_report_name": "주식배당 결정",
    }

    with pytest.raises(RuntimeError, match="ratio changed"):
        evidence._verify_support_body(
            tmp_path,
            {"adjustment_trade_date": date(2018, 12, 27)},
            row,
        )


def _viewer_bonus_support(tmp_path: Path) -> tuple[dict, Path]:
    payload = (
        "<html><body><table>"
        "<tr><td>4. 신주배정기준일</td><td>2021-12-31</td></tr>"
        "<tr><td>5. 1주당 신주배정 주식수</td>"
        "<td>보통주식 (주)</td><td>1.0</td></tr>"
        "</table></body></html>"
    ).encode()
    digest = hashlib.sha256(payload).hexdigest()
    body = (
        tmp_path / "corporate_actions/dart/support_action_families/objects"
        / f"sha256={digest}.html"
    )
    body.parent.mkdir(parents=True)
    body.write_bytes(payload)
    row = {
        "support_action_source": "DART_VIEWER",
        "support_action_key": "20211217000406",
        "support_action_type": "bonus_issue",
        "support_action_body_path": str(body.relative_to(tmp_path)),
        "support_action_body_sha256": digest,
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_semantic_group_keys": (
            '["005950|2021-12-31|BONUS_ISSUE|1"]'
        ),
        "support_announcement_date": date(2021, 12, 17),
        "support_ex_date": date(2021, 12, 31),
        "support_record_date": None,
        "support_ratio_numerator": 1.0,
        "support_ratio_denominator": 1.0,
        "support_entitlement_security_class": "COMMON",
        "support_distributed_security_class": "COMMON",
        "support_expected_price_factor": 0.5,
        "support_report_name": "주요사항보고서(무상증자결정)",
    }
    return row, body


def _viewer_stock_dividend_support(tmp_path: Path) -> tuple[dict, Path]:
    payload = (
        "<html><body><table>"
        "<tr><td>1. 1주당 배당주식수 (주)</td>"
        "<td>보통주식</td><td>0.05</td></tr>"
        "<tr><td>4. 배당기준일</td><td>2015-12-31</td></tr>"
        "</table></body></html>"
    ).encode()
    digest = hashlib.sha256(payload).hexdigest()
    body = (
        tmp_path / "corporate_actions/dart/support_action_families/objects"
        / f"sha256={digest}.html"
    )
    body.parent.mkdir(parents=True)
    body.write_bytes(payload)
    row = {
        "support_action_source": "DART_VIEWER",
        "support_action_key": "20151228900387",
        "support_action_type": "stock_dividend",
        "support_action_body_path": str(body.relative_to(tmp_path)),
        "support_action_body_sha256": digest,
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_semantic_group_keys": (
            '["032960|2015-12-31|STOCK_DIVIDEND|0.05"]'
        ),
        "support_announcement_date": date(2015, 12, 28),
        "support_ex_date": None,
        "support_record_date": date(2015, 12, 31),
        "support_ratio_numerator": 0.05,
        "support_ratio_denominator": 1.0,
        "support_entitlement_security_class": "COMMON",
        "support_distributed_security_class": "COMMON",
        "support_expected_price_factor": None,
        "support_report_name": "[기재정정]주식배당결정",
    }
    return row, body


def test_viewer_bonus_component_requires_exact_body_terms_and_semantics(
    tmp_path,
):
    row, _ = _viewer_bonus_support(tmp_path)
    parent = {"adjustment_trade_date": date(2021, 12, 29)}

    evidence._verify_support_body(tmp_path, parent, row)

    row["support_expected_price_factor"] = 0.6
    with pytest.raises(RuntimeError, match="expected factor mismatch"):
        evidence._verify_support_body(tmp_path, parent, row)


def test_viewer_stock_dividend_requires_exact_body_terms_and_semantics(
    tmp_path,
):
    row, _ = _viewer_stock_dividend_support(tmp_path)
    parent = {
        "ticker": "032960",
        "adjustment_trade_date": date(2015, 12, 29),
    }

    evidence._verify_support_body(tmp_path, parent, row)

    row["support_record_date"] = date(2016, 1, 1)
    with pytest.raises(RuntimeError, match="date/factor parity"):
        evidence._verify_support_body(tmp_path, parent, row)


def test_viewer_stock_dividend_group_rejects_fully_rehashed_drift(tmp_path):
    row, _ = _viewer_stock_dividend_support(tmp_path)
    row.update({
        "evidence_key": "viewer-stock-parent",
        "target_cash_receipt_no": "20160229800375",
        "target_adjustment_date": date(2015, 12, 29),
        "support_semantic_group_keys": '["arbitrary-group"]',
        "support_reference_price": None,
        "support_reason": None,
    })
    row["manifest_support_row_sha256"] = evidence._manifest_support_row_sha(row)
    support = pd.DataFrame([row])
    parent = {
        "ticker": "032960",
        "support_action_count": 1,
        "support_semantic_group_count": 1,
        "support_action_digest": evidence.support_manifest_digest(support),
    }

    with pytest.raises(RuntimeError, match="does not bind ticker/date/ratio"):
        evidence._validate_support_groups(parent, support)


def test_viewer_bonus_group_rejects_fully_rehashed_arbitrary_identity(
    tmp_path,
):
    row, _ = _viewer_bonus_support(tmp_path)
    row.update({
        "evidence_key": "viewer-parent",
        "target_cash_receipt_no": "20220214901227",
        "target_adjustment_date": date(2021, 12, 29),
        "support_semantic_group_keys": '["arbitrary-group"]',
        "support_reference_price": None,
        "support_reason": None,
    })
    row["manifest_support_row_sha256"] = (
        evidence._manifest_support_row_sha(row)
    )
    support = pd.DataFrame([row])
    parent = {
        "ticker": "005950",
        "support_action_count": 1,
        "support_semantic_group_count": 1,
        "support_action_digest": evidence.support_manifest_digest(support),
    }

    with pytest.raises(RuntimeError, match="does not bind ticker/date/ratio"):
        evidence._validate_support_groups(parent, support)


def test_viewer_bonus_component_rebinds_exact_terminal_family_body(
    tmp_path, monkeypatch,
):
    row, _ = _viewer_bonus_support(tmp_path)
    row.update({
        "evidence_key": "viewer-parent",
        "target_cash_receipt_no": "20220214901227",
        "target_adjustment_date": date(2021, 12, 29),
    })
    family_source = SimpleNamespace(
        receipt_no="20211217000406",
        report_name="주요사항보고서(무상증자결정)",
        receipt_date="2021-12-17",
        body_path=row["support_action_body_path"],
        body_sha256=row["support_action_body_sha256"],
        structured_path=None,
        structured_sha256=None,
    )
    family = SimpleNamespace(
        ticker="005950",
        action_type="bonus_issue",
        terminal_economic_receipt_no="20211217000406",
        terminal_admissible=True,
        terminal_ratio=1.0,
        root_receipt_no="20211217000406",
        terminal_status="ACTIVE",
        sources=(family_source,),
    )
    monkeypatch.setattr(
        evidence,
        "verify_support_action_families",
        lambda *args, **kwargs: SimpleNamespace(entries=(family,)),
    )
    parents = pd.DataFrame([{
        "evidence_key": "viewer-parent", "ticker": "005950",
    }])
    supports = pd.DataFrame([row])

    evidence._validate_support_family_bindings(
        tmp_path,
        parents,
        supports,
        required_start=date(2015, 1, 1),
        required_end=date(2026, 8, 10),
    )

    supports.loc[0, "support_action_body_sha256"] = "f" * 64
    with pytest.raises(RuntimeError, match="viewer-body parity"):
        evidence._validate_support_family_bindings(
            tmp_path,
            parents,
            supports,
            required_start=date(2015, 1, 1),
            required_end=date(2026, 8, 10),
        )


@pytest.mark.parametrize(
    ("report_name", "body_text", "message"),
    [
        (
            "주식배당결정(철회)",
            "주식배당결정 배당기준일 2021-12-31 보통주 0.1주",
            "withdrawn/cancelled",
        ),
        (
            "주식배당결정(정정)",
            "주식배당결정 배당기준일 2021-12-31 "
            "보통주 0.1주 정정후 보통주 0주",
            "zero-share",
        ),
    ],
)
def test_stock_dividend_component_rejects_withdrawal_or_zero_terminal_body(
    tmp_path, report_name, body_text, message,
):
    body = tmp_path / "corporate_actions/dart/documents/stock.zip"
    _zip(body, f"<document>{body_text}</document>")
    row = {
        "support_action_source": "DART_DISCLOSURE",
        "support_action_key": "20211224900781",
        "support_action_type": "stock_dividend",
        "support_action_body_path": str(body.relative_to(tmp_path)),
        "support_action_body_sha256": _sha(body),
        "support_action_scope": "ISSUER",
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "support_semantic_group_keys": '["stock-dividend"]',
        "support_record_date": date(2021, 12, 31),
        "support_ex_date": None,
        "support_ratio_numerator": 0.1,
        "support_ratio_denominator": 1,
        "support_entitlement_security_class": "COMMON",
        "support_distributed_security_class": "COMMON",
        "support_report_name": report_name,
    }

    with pytest.raises(RuntimeError, match=message):
        evidence._verify_support_body(
            tmp_path,
            {"adjustment_trade_date": date(2021, 12, 29)},
            row,
        )
