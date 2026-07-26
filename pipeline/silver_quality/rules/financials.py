"""fundamental PIT, revision, currency, accounting 규칙."""
from __future__ import annotations

import pandas as pd

from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity
from pipeline.silver_quality.rules.common import duplicate_keys, null_keys, result

FUNDAMENTAL_KEYS = [
    "identifier", "source", "period_end", "fiscal_period",
    "fs_type", "revision_key", "metric",
]


def check_financials(
    df: pd.DataFrame,
    partition_key: str | None = None,
) -> list[CheckResult]:
    checks = [
        null_keys(
            df, "fundamental",
            FUNDAMENTAL_KEYS + ["available_date", "currency", "value"],
            partition_key,
        ),
        duplicate_keys(df, "fundamental", FUNDAMENTAL_KEYS, partition_key),
    ]
    if df.empty:
        return checks

    enum_bad = df[
        ~df["fiscal_period"].isin(["FY", "Q1", "Q2", "Q3", "Q4"])
        | ~df["fs_type"].isin(["CFS", "OFS"])
        | ~df["currency"].astype(str).str.fullmatch(r"[A-Z]{3}")
    ]
    checks.append(result(
        "FUNDAMENTAL_ENUM_CURRENCY", "fundamental", Severity.ERROR, enum_bad,
        "valid fiscal_period/fs_type and 3-letter currency", partition_key=partition_key,
    ))

    filed = pd.to_datetime(df["filed"], errors="coerce")
    period = pd.to_datetime(df["period_end"], errors="coerce")
    available = pd.to_datetime(df["available_date"], errors="coerce")
    pit_bad = df[
        (filed.notna() & (period > filed))
        | (filed.notna() & (available != filed + pd.offsets.Day(1)))
        | (available <= period)
    ]
    checks.append(result(
        "FUNDAMENTAL_PIT_ORDER", "fundamental", Severity.CRITICAL, pit_bad,
        "period_end <= filed < available_date", partition_key=partition_key,
    ))

    filing_scope = [
        "identifier", "source", "period_end", "fiscal_period",
        "fs_type", "revision_key",
    ]
    currency_count = df.groupby(filing_scope, dropna=False)["currency"].transform("nunique")
    checks.append(result(
        "FUNDAMENTAL_CURRENCY_CONSISTENCY", "fundamental", Severity.ERROR,
        df[currency_count.gt(1)], "one currency per filing revision",
        partition_key=partition_key,
    ))

    pivot = df.pivot_table(
        index=filing_scope,
        columns="metric",
        values="value",
        aggfunc="first",
    )
    required = {"total_assets", "total_liabilities", "total_equity"}
    if required.issubset(pivot.columns):
        base = pivot["total_assets"].abs().replace(0, pd.NA)
        rel = (
            pivot["total_assets"]
            - pivot["total_liabilities"]
            - pivot["total_equity"]
        ).abs() / base
        bad_idx = rel[rel.gt(0.01)].index
        source_index = pd.MultiIndex.from_frame(df[filing_scope])
        accounting_bad = df[source_index.isin(bad_idx)]
        accounting_scopes = pivot.loc[bad_idx].reset_index()
        accounting_scopes["relative_error"] = rel.loc[bad_idx].to_numpy()
    else:
        accounting_bad = df.iloc[0:0]
        accounting_scopes = pd.DataFrame()
    accounting_samples = (
        accounting_scopes.head(20).astype(object)
        .where(pd.notna(accounting_scopes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="FUNDAMENTAL_ACCOUNTING_EQUATION",
        dataset="fundamental",
        severity=Severity.WARNING,
        status=(
            CheckStatus.FAIL
            if len(accounting_scopes)
            else CheckStatus.PASS
        ),
        expected="Assets ≈ Liabilities + Equity within 1% per filing revision",
        actual=(
            f"failed_scopes={len(accounting_scopes)}, "
            f"affected_rows={len(accounting_bad)}"
        ),
        failed_count=len(accounting_scopes),
        samples=accounting_samples,
        partition_key=partition_key,
    ))

    major = {"total_assets", "revenue", "net_income"}
    scope_metrics = (
        df.groupby(filing_scope, dropna=False)["metric"]
        .agg(lambda values: sorted(set(values)))
        .rename("present_metrics")
        .reset_index()
    )
    missing_major_scopes = scope_metrics[
        ~scope_metrics["present_metrics"].map(
            lambda values: bool(set(values) & major)
        )
    ]
    source_index = pd.MultiIndex.from_frame(df[filing_scope])
    missing_index = pd.MultiIndex.from_frame(
        missing_major_scopes[filing_scope]
    )
    affected_rows = int(source_index.isin(missing_index).sum())
    major_samples = (
        missing_major_scopes.head(20).astype(object)
        .where(pd.notna(missing_major_scopes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="FUNDAMENTAL_MAJOR_METRIC_COVERAGE",
        dataset="fundamental",
        severity=Severity.WARNING,
        status=(
            CheckStatus.FAIL
            if len(missing_major_scopes)
            else CheckStatus.PASS
        ),
        expected=(
            "at least one of total_assets/revenue/net_income "
            "per filing revision"
        ),
        actual=(
            f"failed_scopes={len(missing_major_scopes)}, "
            f"affected_rows={affected_rows}"
        ),
        failed_count=len(missing_major_scopes),
        samples=major_samples,
        partition_key=partition_key,
    ))
    return checks
