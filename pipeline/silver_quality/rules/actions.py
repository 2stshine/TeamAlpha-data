"""Corporate-action identity, amount and date checks."""
from __future__ import annotations

import pandas as pd

from pipeline.silver_quality.models import CheckResult, Severity
from pipeline.silver_quality.rules.common import duplicate_keys, null_keys, result


def check_actions(
    df: pd.DataFrame,
    partition_key: str | None = None,
) -> list[CheckResult]:
    checks = [
        null_keys(
            df, "corporate_action",
            ["identifier", "source", "event_type", "rcept_no"],
            partition_key,
        ),
        duplicate_keys(
            df, "corporate_action",
            ["identifier", "source", "event_type", "rcept_no"],
            partition_key,
        ),
    ]
    if df.empty:
        return checks
    cash = df[df["event_type"].eq("cash_dividend")]
    if not cash.empty:
        amount = pd.to_numeric(cash["cash_amount"], errors="coerce")
        adjusted = pd.to_numeric(
            cash["adjusted_cash_amount"], errors="coerce",
        )
        checks.append(result(
            "CASH_DIVIDEND_AMOUNT", "corporate_action", Severity.WARNING,
            cash[(amount.notna() & amount.le(0)) | (adjusted.notna() & adjusted.le(0))],
            "cash dividend amounts are positive when present",
            partition_key=partition_key,
        ))
        missing = cash[amount.isna()]
        checks.append(result(
            "CASH_DIVIDEND_AMOUNT_COVERAGE", "corporate_action", Severity.WARNING,
            missing, "cash-dividend decision document exposes common-share amount",
            partition_key=partition_key,
        ))
        record = pd.to_datetime(cash["record_date"], errors="coerce")
        payment = pd.to_datetime(cash["payment_date"], errors="coerce")
        checks.append(result(
            "CASH_DIVIDEND_DATE_ORDER", "corporate_action", Severity.WARNING,
            cash[record.notna() & payment.notna() & payment.lt(record)],
            "payment_date >= record_date",
            partition_key=partition_key,
        ))
    return checks
