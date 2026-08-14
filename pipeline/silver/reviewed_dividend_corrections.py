"""Content-addressed, receipt-specific DART field errata.

These are deliberately not broad date heuristics.  Each entry is bound to the
exact OpenDART ZIP bytes and expected parsed value.  Record-date corrections
additionally require an independent OpenDART periodic response whose
settlement date and common-share DPS agree.  Any changed byte or field fails
closed and requires a new review.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path


SCHEMA_VERSION = "dart_reviewed_dividend_corrections_v1"
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/reviewed_dividend_corrections.json"
)

# Payment dates below are obvious one-year field-entry errors (payment before
# its own record date).  Payment date does not enter the total-return formula,
# but retaining a reviewed classification prevents silent dirty metadata.
_PAYMENT_DATE_ERRATA = (
    ("20150312900757", "072470", "5a60d6a7ebf1a927a0e19ebd31963c9180ade55686ac32f996ca8ffcc6f49afd", "2014-04-24", "2015-04-24"),
    ("20170804800081", "004450", "0d9d360a0c26d2294658af3b442b013df1a762673970af0443f452d7867a9fc9", "2016-08-17", "2017-08-17"),
    ("20180130900667", "072020", "9fd84c0c00bb707b8cf6920db62203dca3078c043d7fb9f13e5873c3acd35dbd", "2017-04-25", "2018-04-25"),
    ("20210308901015", "064820", "1bbb85d599c156c6157bf11932d2f68992b1466639cc6fef63983182fff6920e", "2020-04-26", "2021-04-26"),
    ("20211209800219", "008870", "54274eed092745aa6f73db611e1b50adf9a0ca0ff7188ab72a0d40a4f4f1be89", "2021-01-10", "2022-01-10"),
    ("20211210800570", "008870", "b7f285f685d59cef7e24043695425fdd95b275ce2d7a4057320333754167e521", "2021-01-10", "2022-01-10"),
    ("20230303800528", "002240", "5f20c2917d845a393164d26a9a1a9d5a608c0f55e1149b712e322bd9301ab031", "2022-04-17", "2023-04-17"),
    ("20240229900834", "021320", "2261fb2120a9b948c37af6f50d01a0d42050bd2c3f19fd694de5374ef19f1637", "2023-04-26", "2024-04-26"),
    ("20260209901858", "065680", "4654c6a9385875d5a0ac6851542f9c6cc610f3b31991fca03da8d882f9e234ee", "2025-04-23", "2026-04-23"),
    ("20260224901188", "053700", "a506f14f1bc1d6f48add0a87c49107c8a61f2d3b0a2f1733e05b16390fe9c318", "2025-04-24", "2026-04-24"),
    ("20260304900345", "241690", "98233740711794349721b0e491c1b29e86d224d24979c19e369ef522b2ef9794", "2025-04-30", "2026-04-30"),
)


CORRECTIONS = (
    {
        "correction_id": "093320-20160224900227-record-date",
        "receipt_no": "20160224900227",
        "ticker": "093320",
        "action_zip_sha256": "4b4d921ffa4f1a141a3b5d9418e09ff5d4621dd440dd90e253e7b006f3478f13",
        "field": "record_date",
        "raw_value": "None",
        "corrected_value": "2015-12-31",
        "expected_common_cash_amount": 100.0,
        # alotMatter.stlm_dt is a fiscal settlement date, not a general
        # record-date source.  This exact receipt/DPS/SHA-bound supplement is
        # a reviewed one-off and must never become a stlm_dt heuristic.
        "basis": (
            "RECEIPT_SPECIFIC_REVIEWED_FY_END_RECORD_DATE_SUPPLEMENT"
        ),
        "evidence_path": (
            "dividends/dart/alot-matter/year=2015/report=11011/"
            "corp=093320/rcept=20160330000241/response.json"
        ),
        "evidence_sha256": "f0a90a0dfef3acfa9d5f827c702153023c77c7d3968868cf46525c3371aa5c29",
        "evidence_manifest_path": (
            "dividends/dart/alot-matter/year=2015/report=11011/"
            "corp=093320/rcept=20160330000241/manifest.json"
        ),
        "evidence_manifest_sha256": "93b411145f1611b2d26432f509fb282d9170fd361d3f2a26ee8e09a0c90c4e37",
        "evidence_receipt_no": "20160330000241",
        "evidence_corp_code": "00603348",
        "evidence_settlement_date": "2015-12-31",
    },
    {
        "correction_id": "093320-20170316900231-record-date",
        "receipt_no": "20170316900231",
        "ticker": "093320",
        "action_zip_sha256": "eaabcb662d529fa7a3509d11b37a873ecf645d0f442b8d4c088fc9570a42db32",
        "field": "record_date",
        "raw_value": "None",
        "corrected_value": "2016-12-31",
        "expected_common_cash_amount": 120.0,
        "basis": (
            "RECEIPT_SPECIFIC_REVIEWED_FY_END_RECORD_DATE_SUPPLEMENT"
        ),
        "evidence_path": (
            "dividends/dart/alot-matter/year=2016/report=11011/"
            "corp=093320/rcept=20170331004628/response.json"
        ),
        "evidence_sha256": "3d3e3c679ce28c452cebc55886baf1ddf232f4fdfed3b4173aa3dcef120d1626",
        "evidence_manifest_path": (
            "dividends/dart/alot-matter/year=2016/report=11011/"
            "corp=093320/rcept=20170331004628/manifest.json"
        ),
        "evidence_manifest_sha256": "4727b3d0e39c2c5ac393a4c5e281ba0ab04c8bd3115478857ced2d356e9d23cc",
        "evidence_receipt_no": "20170331004628",
        "evidence_corp_code": "00603348",
        "evidence_settlement_date": "2016-12-31",
    },
    {
        "correction_id": "065510-20200212900283-record-date",
        "receipt_no": "20200212900283",
        "ticker": "065510",
        "action_zip_sha256": "694c1ace5993772b4c90b55ddd84bc4dd416b5de2865e6a5ba995369b5082b7d",
        "field": "record_date",
        "raw_value": "2018-12-31",
        "corrected_value": "2019-12-31",
        "expected_common_cash_amount": 150.0,
        "basis": "INDEPENDENT_OPENDART_FY_PERIODIC_RESPONSE",
        "evidence_path": (
            "dividends/dart/alot-matter/year=2019/report=11011/"
            "corp=065510/rcept=20200330002792/response.json"
        ),
        "evidence_sha256": "809c1023b1d3e1a4b51a0a3b123bc20bc5e2f8e5c44c2460c5ad6bcf513bec8f",
        "evidence_manifest_path": (
            "dividends/dart/alot-matter/year=2019/report=11011/"
            "corp=065510/rcept=20200330002792/manifest.json"
        ),
        "evidence_manifest_sha256": "e6dd46adc8a8686dc06dc0200559aed096a08586bcc940b5bf1f16b042c83c0f",
        "evidence_receipt_no": "20200330002792",
        "evidence_corp_code": "00398668",
        "evidence_settlement_date": "2019-12-31",
    },
    *tuple({
        "correction_id": f"{ticker}-{receipt}-payment-date",
        "receipt_no": receipt,
        "ticker": ticker,
        "action_zip_sha256": digest,
        "field": "payment_date",
        "raw_value": raw,
        "corrected_value": corrected,
        "basis": "REVIEWED_ONE_YEAR_PAYMENT_DATE_ENTRY_ERROR",
        "economic_effect": "NONE_PAYMENT_DATE_NOT_USED_BY_TOTAL_RETURN",
    } for receipt, ticker, digest, raw, corrected in _PAYMENT_DATE_ERRATA),
)


def active_corrections(root: Path) -> tuple[dict, ...]:
    action_root = root / "corporate_actions" / "dart" / "documents"
    return tuple(
        item for item in CORRECTIONS
        if any(action_root.glob(
            f"year=*/corp={item['ticker']}/rcept={item['receipt_no']}.zip"
        ))
    )


def manifest_payload(root: Path) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "corrections": list(active_corrections(root)),
    }


def canonical_manifest_bytes(root: Path) -> bytes:
    return json.dumps(
        manifest_payload(root),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def external_evidence_paths(root: Path) -> tuple[Path, ...]:
    paths: list[Path] = []
    for item in active_corrections(root):
        for field in ("evidence_path", "evidence_manifest_path"):
            if item.get(field):
                paths.append(root / str(item[field]))
    return tuple(paths)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _action_zip(root: Path, *, ticker: str, receipt: str) -> Path:
    matches = list((root / "corporate_actions" / "dart" / "documents").glob(
        f"year=*/corp={ticker}/rcept={receipt}.zip"
    ))
    if len(matches) != 1:
        raise RuntimeError(
            "reviewed correction source ZIP is missing/ambiguous: "
            f"ticker={ticker} receipt={receipt} matches={len(matches)}"
        )
    return matches[0]


def _verify_periodic_evidence(root: Path, correction: dict) -> None:
    if (
        correction.get("field") == "record_date"
        and correction.get("corrected_value")
        != correction.get("evidence_settlement_date")
    ):
        raise RuntimeError(
            "reviewed dividend corrected/settlement date mismatch: "
            f"{correction['correction_id']}"
        )
    path = root / correction["evidence_path"]
    if not path.is_file() or _sha256(path) != correction["evidence_sha256"]:
        raise RuntimeError(
            "reviewed dividend periodic evidence SHA mismatch: "
            f"{correction['correction_id']}"
        )
    manifest_path = root / correction["evidence_manifest_path"]
    if (
        not manifest_path.is_file()
        or _sha256(manifest_path)
        != correction["evidence_manifest_sha256"]
    ):
        raise RuntimeError(
            "reviewed dividend periodic manifest SHA mismatch: "
            f"{correction['correction_id']}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    params = manifest.get("request_params") or {}
    if not (
        manifest.get("complete") is True
        and manifest.get("provider") == "OpenDART"
        and manifest.get("endpoint") == "alotMatter.json"
        and manifest.get("dart_status") == "000"
        and manifest.get("sha256") == correction["evidence_sha256"]
        and manifest.get("rcept_nos") == [
            correction["evidence_receipt_no"]
        ]
        and str(params.get("corp_code") or "")
        == correction["evidence_corp_code"]
        and str(params.get("stock_code") or "") == correction["ticker"]
        and str(params.get("bsns_year") or "")
        == correction["evidence_settlement_date"][:4]
        and str(params.get("reprt_code") or "") == "11011"
    ):
        raise RuntimeError(
            "reviewed dividend periodic manifest contract mismatch: "
            f"{correction['correction_id']}"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    matching = [
        row for row in payload.get("list") or []
        if str(row.get("rcept_no") or "")
        == correction["evidence_receipt_no"]
        and str(row.get("stlm_dt") or "")
        == correction["evidence_settlement_date"]
        and str(row.get("corp_code") or "")
        == correction["evidence_corp_code"]
        and str(row.get("se") or "").replace(" ", "")
        == "주당현금배당금(원)"
        and str(row.get("stock_knd") or "").strip() == "보통주"
    ]
    if len(matching) != 1:
        raise RuntimeError(
            "reviewed dividend periodic evidence row missing/ambiguous: "
            f"{correction['correction_id']}"
        )
    amount = float(str(matching[0].get("thstrm") or "").replace(",", ""))
    if amount != float(correction["expected_common_cash_amount"]):
        raise RuntimeError(
            "reviewed dividend periodic evidence DPS mismatch: "
            f"{correction['correction_id']}"
        )


def apply_reviewed_correction(
    base: str,
    *,
    ticker: str,
    receipt: str,
    details: dict,
) -> dict:
    """Verify and apply the single registered correction, if any."""
    correction = next(
        (item for item in CORRECTIONS if item["receipt_no"] == receipt),
        None,
    )
    if correction is None:
        return details
    if str(ticker).zfill(6) != correction["ticker"]:
        raise RuntimeError(
            f"reviewed correction ticker mismatch: {receipt} {ticker}"
        )
    root = Path(base).expanduser().resolve()
    source = _action_zip(root, ticker=correction["ticker"], receipt=receipt)
    if _sha256(source) != correction["action_zip_sha256"]:
        raise RuntimeError(
            f"reviewed correction source ZIP SHA mismatch: {receipt}"
        )
    field = correction["field"]
    actual = details.get(field)
    actual_text = actual.isoformat() if isinstance(actual, date) else str(actual)
    if actual_text != correction["raw_value"]:
        raise RuntimeError(
            "reviewed correction raw field changed: "
            f"{receipt} {field} expected={correction['raw_value']} "
            f"actual={actual_text}"
        )
    if correction.get("evidence_path"):
        if float(details.get("cash_amount")) != float(
            correction["expected_common_cash_amount"]
        ):
            raise RuntimeError(f"reviewed correction source DPS changed: {receipt}")
        _verify_periodic_evidence(root, correction)
    updated = dict(details)
    if correction.get("economic_effect"):
        # Preserve non-economic source metadata exactly.  The reviewed value
        # is an audit annotation only; total-return logic never reads it.
        updated["payment_date_quality_status"] = (
            "REVIEWED_RAW_ONE_YEAR_ENTRY_ERROR_NOT_USED"
        )
    else:
        updated[field] = date.fromisoformat(correction["corrected_value"])
        updated["reviewed_economic_correction"] = True
        updated["reviewed_evidence_sha256"] = correction.get(
            "evidence_sha256", correction["action_zip_sha256"]
        )
    updated["reviewed_correction_id"] = correction["correction_id"]
    return updated
