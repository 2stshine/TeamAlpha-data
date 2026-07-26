from datetime import date
from uuid import uuid4

import pandas as pd
import pytest

from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality.runner import assert_publishable


def test_error_failure_raises_gate_error():
    result = CheckResult(
        "TEST", "price_daily", Severity.ERROR, CheckStatus.FAIL,
        "valid", "invalid", 1,
    )
    with pytest.raises(QualityGateError):
        assert_publishable([result])


def test_warning_does_not_block():
    result = CheckResult(
        "TEST", "price_daily", Severity.WARNING, CheckStatus.FAIL,
        "normal", "spike", 1,
    )
    assert_publishable([result])
