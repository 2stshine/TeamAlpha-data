"""Build the exact reviewed KIND request inputs for the 331-row recovery."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_recovery_kind_requests REFERENCE COMPONENT")
    draft = json.loads(Path(
        "/private/tmp/teamalpha-kind-reference-requests-v3-draft-20260813.json"
    ).read_bytes())
    rows = draft["requests"]
    checkpoint = json.loads(Path(
        "/private/tmp/teamalpha-kind-discovery-checkpoint-81-20260813.json"
    ).read_bytes())
    extra_checkpoint = json.loads(Path(
        "/private/tmp/teamalpha-kind-discovery-checkpoint-viewer25-20260813.json"
    ).read_bytes())
    existing_urls = {row["source_url"] for row in rows}
    for result in extra_checkpoint["results"]:
        target = result["target"]
        chain = result["role_results"][0]["exact_chains"][0]
        objects = {item["kind"]: item for item in chain["objects"]}
        url = objects["body"]["url"]
        if url in existing_urls:
            continue
        semantics = chain["parsed_semantics"]
        rows.append({
            "asset_name": semantics["asset_name"],
            "body_content_length": objects["body"]["content_length"],
            "body_sha256": objects["body"]["sha256"],
            "identity_content_length": objects["main"]["content_length"],
            "identity_sha256": objects["main"]["sha256"],
            "identity_source_url": objects["main"]["url"],
            "security_class": semantics["security_class"],
            "source_form_code": semantics["form_code"],
            "source_url": url,
            "support_semantic_role": "CORROBORATION",
            "target_adjustment_date": target["target_adjustment_date"],
            "target_cash_receipt_no": target["target_cash_receipt_no"],
            "ticker": target["ticker"],
        })
        existing_urls.add(url)
    official_names = {}
    for result in checkpoint["results"]:
        for role in result["role_results"]:
            for chain in role["exact_chains"]:
                official_names[chain["objects"][2]["url"]] = (
                    chain["parsed_semantics"]["asset_name"]
                )
    rows = [
        {**row, "asset_name": official_names.get(
            row["source_url"], row["asset_name"],
        )}
        for row in rows
    ]
    rows.sort(key=lambda row: (row["ticker"], row["source_url"]))
    reference = {
        "schema_version": "krx_kind_cash_adjustment_requests_v2",
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_MAIN_AND_SELECTED_BODY",
        "complete": True,
        "request_count": len(rows),
        "request_digest": hashlib.sha256(canonical(rows)).hexdigest(),
        "requests": rows,
    }
    old = json.loads(Path(
        "/private/tmp/teamalpha-kind-cj-component-acquisition-20260812.json"
    ).read_bytes())["components"]
    paid = {
        "adjustment_date": "2017-12-27",
        "announcement_date": "2017-12-27",
        "asset_name": "아세아시멘트",
        "body_content_length": 42460,
        "body_sha256": "cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9",
        "body_url": "https://kind.krx.co.kr/external/2018/02/01/000047/20180201000086/11306.htm",
        "component_action_key": "20180201000086",
        "component_action_source": "KRX_KIND",
        "component_action_type": "paid_increase",
        "contents_content_length": 1046,
        "contents_sha256": "d9fdaacae60f43ac42a6c551c6a8559de4c2ec3edf050f764893be57ef8b5e28",
        "contents_url": "https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&docNo=20180201000086",
        "distributed_security_class": "COMMON",
        "entitlement_security_class": "COMMON",
        "main_content_length": 24808,
        "main_sha256": "6472611a5b11e9036922960a43a891d10e27cd5aa8659857ebdbdc6a12938814",
        "main_url": "https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=20180201000047&docno=&viewerhost=&viewerport=",
        "ratio_denominator": 1.0,
        "ratio_numerator": 0.1456981704,
        "record_date": "2017-12-31",
        "report_name": "유상증자 결정",
        "semantic_role": "ADJUSTMENT_COMPONENT",
        "source_form_code": "11306",
        "target_cash_receipt_no": "20180226800579",
        "terminal_acceptance_no": "20180201000047",
        "terminal_announcement_date": "2018-02-01",
        "ticker": "183190",
    }
    components = sorted(old + [paid], key=lambda row: (
        row["ticker"], row["component_action_key"],
    ))
    component = {
        "schema_version": "krx_kind_cash_adjustment_component_requests_v2",
        "provenance": "HUMAN_REVIEWED_OFFICIAL_KIND_TERMINAL_COMPONENT",
        "complete": True,
        "component_count": len(components),
        "component_digest": hashlib.sha256(canonical(components)).hexdigest(),
        "components": components,
    }
    Path(sys.argv[1]).write_bytes(canonical(reference))
    Path(sys.argv[2]).write_bytes(canonical(component))


if __name__ == "__main__":
    main()
