"""Fail-closed compatibility facade for the retired v1 return builder.

The former implementation inferred ex-dates from ``record_date`` and updated
selected assets without a certified action snapshot.  That cannot satisfy the
v3 total-return contract.  Keep the import path only so stale jobs fail with an
actionable error instead of silently publishing an uncertified label.

Use :mod:`pipeline.silver.total_return_rebuild` after publishing a certified
DART action snapshot, then run :mod:`pipeline.silver.total_return_audit`.
"""
from __future__ import annotations


_DISABLED_MESSAGE = (
    "legacy partial total-return maintenance is disabled; run the certified "
    "action snapshot -> pipeline.silver.total_return_rebuild -> "
    "pipeline.silver.total_return_audit workflow"
)


def _disabled(*_args, **_kwargs):
    raise RuntimeError(_DISABLED_MESSAGE)


def derive_ex_dates(*args, **kwargs):
    """Reject record-date inference from the retired v1 methodology."""
    return _disabled(*args, **kwargs)


def compute_total_return_close(*args, **kwargs):
    """Reject calculations that bypass certified v3 evidence and guards."""
    return _disabled(*args, **kwargs)


def assets_with_recent_dividend_changes(*args, **kwargs):
    """Reject partial-asset maintenance outside the full rebuild contract."""
    return _disabled(*args, **kwargs)


def run(*args, **kwargs):
    """Reject the retired v1 batch writer."""
    return _disabled(*args, **kwargs)


def run_daily(*args, **kwargs):
    """Reject the retired v1 daily writer."""
    return _disabled(*args, **kwargs)


if __name__ == "__main__":
    raise SystemExit(_DISABLED_MESSAGE)
