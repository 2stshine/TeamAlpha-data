"""Bronze → 후보 데이터 대사."""
from __future__ import annotations

from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity


def check_reconciliation(stats: dict, partition_key: str | None = None) -> list[CheckResult]:
    checks: list[CheckResult] = []
    for dataset, values in stats.items():
        if not isinstance(values, dict) or "input_rows" not in values:
            continue
        source = int(values.get("input_rows", 0))
        transformed = int(values.get("transformed_rows", 0))
        excluded = int(values.get("excluded_rows", 0))
        rejected = int(values.get("rejected_rows", 0))
        balanced = source == transformed + excluded + rejected
        checks.append(CheckResult(
            rule_code="RECONCILIATION_ROW_BALANCE",
            dataset=dataset,
            severity=Severity.ERROR,
            status=CheckStatus.PASS if balanced and rejected == 0 else CheckStatus.FAIL,
            expected="input = transformed + intentional_excluded + rejected; rejected=0",
            actual=(
                f"input={source}, transformed={transformed}, "
                f"excluded={excluded}, rejected={rejected}"
            ),
            failed_count=0 if balanced and rejected == 0 else max(1, rejected),
            partition_key=partition_key,
        ))
        source_files = int(values.get("source_file_count", 0))
        empty_with_input = source_files > 0 and transformed == 0
        checks.append(CheckResult(
            rule_code="RECONCILIATION_EMPTY_TRANSFORM",
            dataset=dataset,
            severity=Severity.CRITICAL,
            status=CheckStatus.FAIL if empty_with_input else CheckStatus.PASS,
            expected="source files with rows produce candidate rows",
            actual=f"source_files={source_files}, input={source}, transformed={transformed}",
            failed_count=1 if empty_with_input else 0,
            partition_key=partition_key,
        ))
        known_duplicate = values.get(
            "known_net_income_ord_duplicate",
            {"row_count": 0, "group_count": 0, "samples": []},
        )
        known_rows = int(known_duplicate.get("row_count", 0))
        known_groups = int(known_duplicate.get("group_count", 0))
        if "known_net_income_ord_duplicate" in values:
            checks.append(CheckResult(
                rule_code="DART_NET_INCOME_ORD_DUPLICATE",
                dataset=dataset,
                severity=Severity.WARNING,
                status=CheckStatus.PASS,
                expected=(
                    "DART net_income duplicates are exactly two identical "
                    "source rows differing only by ord; keep the smallest ord"
                ),
                actual=(
                    f"deduplicated_rows={known_rows}, "
                    f"duplicate_groups={known_groups}"
                ),
                failed_count=0,
                samples=list(known_duplicate.get("samples", []))[:20],
                partition_key=partition_key,
            ))

        unexpected_duplicate = values.get(
            "unexpected_exact_duplicate",
            {"row_count": 0, "group_count": 0, "samples": []},
        )
        unexpected_rows = int(unexpected_duplicate.get("row_count", 0))
        unexpected_groups = int(unexpected_duplicate.get("group_count", 0))
        if "unexpected_exact_duplicate" in values:
            checks.append(CheckResult(
                rule_code="DART_UNEXPECTED_EXACT_DUPLICATE",
                dataset=dataset,
                severity=Severity.ERROR,
                status=(
                    CheckStatus.FAIL
                    if unexpected_rows
                    else CheckStatus.PASS
                ),
                expected=(
                    "no exact business-key duplicate outside the known "
                    "net_income ord-only source pattern"
                ),
                actual=(
                    f"duplicate_rows={unexpected_rows}, "
                    f"duplicate_groups={unexpected_groups}"
                ),
                failed_count=unexpected_rows,
                samples=list(unexpected_duplicate.get("samples", []))[:20],
                partition_key=partition_key,
            ))
    return checks
