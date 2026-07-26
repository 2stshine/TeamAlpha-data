"""공통 schema, key, reconciliation 규칙."""
from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity

SAMPLE_LIMIT = 20


def _records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return df.head(SAMPLE_LIMIT).astype(object).where(pd.notna(df), None).to_dict("records")


def result(
    code: str,
    dataset: str,
    severity: Severity,
    failed: pd.DataFrame | int,
    expected: str,
    actual: str | None = None,
    partition_key: str | None = None,
) -> CheckResult:
    count = len(failed) if isinstance(failed, pd.DataFrame) else int(failed)
    return CheckResult(
        rule_code=code,
        dataset=dataset,
        severity=severity,
        status=CheckStatus.FAIL if count else CheckStatus.PASS,
        expected=expected,
        actual=actual or str(count),
        failed_count=count,
        samples=_records(failed) if isinstance(failed, pd.DataFrame) else [],
        partition_key=partition_key,
    )


def required_columns(
    df: pd.DataFrame,
    dataset: str,
    columns: Iterable[str],
    partition_key: str | None = None,
) -> CheckResult:
    missing = sorted(set(columns) - set(df.columns))
    return result(
        "COMMON_REQUIRED_COLUMNS",
        dataset,
        Severity.CRITICAL,
        len(missing),
        "all required columns present",
        ",".join(missing) if missing else "present",
        partition_key,
    )


def null_keys(
    df: pd.DataFrame,
    dataset: str,
    keys: list[str],
    partition_key: str | None = None,
) -> CheckResult:
    if df.empty or any(k not in df for k in keys):
        failed = df.iloc[0:0]
    else:
        failed = df[df[keys].isna().any(axis=1)]
        for key in keys:
            failed = pd.concat(
                [failed, df[df[key].astype(str).str.strip().eq("")]],
                ignore_index=True,
            )
        failed = failed.drop_duplicates()
    return result(
        "COMMON_NULL_KEY",
        dataset,
        Severity.CRITICAL,
        failed,
        f"non-null/non-blank keys: {keys}",
        partition_key=partition_key,
    )


def duplicate_keys(
    df: pd.DataFrame,
    dataset: str,
    keys: list[str],
    partition_key: str | None = None,
) -> CheckResult:
    if df.empty or any(k not in df for k in keys):
        failed = df.iloc[0:0]
    else:
        failed = df[df.duplicated(keys, keep=False)].sort_values(keys)
    return result(
        "COMMON_DUPLICATE_KEY",
        dataset,
        Severity.CRITICAL,
        failed,
        f"unique keys: {keys}",
        partition_key=partition_key,
    )


def finite_numbers(
    df: pd.DataFrame,
    dataset: str,
    columns: list[str],
    partition_key: str | None = None,
) -> CheckResult:
    present = [c for c in columns if c in df]
    if df.empty or not present:
        failed = df.iloc[0:0]
    else:
        numeric = df[present].apply(pd.to_numeric, errors="coerce")
        required = df[present].notna()
        bad = required & (numeric.isna() | ~np.isfinite(numeric))
        failed = df[bad.any(axis=1)]
    return result(
        "COMMON_NUMERIC_PARSE",
        dataset,
        Severity.ERROR,
        failed,
        f"finite numeric values when present: {present}",
        partition_key=partition_key,
    )
