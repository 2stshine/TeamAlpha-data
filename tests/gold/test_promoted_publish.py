from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import pytest

from pipeline.gold.run import build_upsert_sql
from pipeline.gold.publish import (
    DEFAULT_APPROVAL_PATH,
    _acquire_publish_lock,
    _build_correlation_values,
    _desired_metadata,
    _month_count,
    _release_publish_lock,
    _replace_values,
    _validate_dividend_lineage,
    load_approval,
    validate_approval,
)


SELECTED = {
    "max_daily_return_1m",
    "net_equity_issuance_price_adjusted_12m",
    "realized_volatility_252d",
    "operating_income_to_liabilities",
}


def test_approval_selects_only_nonduplicate_promoted_factors():
    document = load_approval()
    assert set(document["factors"]) == SELECTED
    assert "dividend_event_frequency_ttm" not in document["factors"]
    assert "dividend_yield_ttm" not in document["factors"]
    assert document["selection"]["excluded_unpublishable"]["factor"] == (
        "dividend_yield_ttm"
    )
    assert document["selection"]["selected_pair_max_oos"] < 0.70


def test_approval_is_bound_to_exact_verified_sql_hashes():
    document = load_approval()
    validated = validate_approval(document)
    assert set(validated) == SELECTED
    for factor_key, item in validated.items():
        assert (
            item["implementation_hash"]
            == document["factors"][factor_key]["implementation_sha256"]
        )


def test_approval_requires_36_month_promote_evidence():
    document = load_approval()
    for approval in document["factors"].values():
        evaluation = approval["evaluation"]
        assert evaluation["passed"] is True
        assert evaluation["verdict"] == "PROMOTE"
        assert evaluation["oos_signal_months"] == 36
        assert evaluation["oos_rank_ic"] >= evaluation["oos_required_rank_ic"]


def test_backfill_window_has_one_hundred_signal_months():
    document = load_approval()
    assert _month_count(
        document["backfill_start_month"],
        document["backfill_end_month"],
    ) == 100


def test_gold_metadata_keeps_raw_value_and_directional_rank_contract():
    document = load_approval()
    validated = validate_approval(document)
    for factor_key, item in validated.items():
        config, evaluation = _desired_metadata(
            document, factor_key, item,
        )
        assert config["value_contract"]["id"] == (
            "raw_value_direction_adjusted_rank_v1"
        )
        assert config["predicted_sign"] in {-1, 1}
        assert evaluation["passed"] is True


def test_approval_document_is_plain_json_for_auditability():
    raw = DEFAULT_APPROVAL_PATH.read_text(encoding="utf-8")
    assert json.loads(raw)["approval_id"] == "promoted-20260810-nonduplicate-v2"
    assert Path(DEFAULT_APPROVAL_PATH).suffix == ".json"


def test_approval_is_bound_to_an_immutable_research_commit():
    document = load_approval()
    source = document["research_source"]
    assert source["repository"].startswith("https://github.com/")
    assert len(source["commit"]) == 40
    validate_approval(document)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("oos_rank_ic", 0.0),
        ("oos_rank_ic_retention", 0.1),
        ("oos_by_qvalue", 0.11),
        ("oos_signal_months", 35),
        ("oos_end", "2026-04"),
    ],
)
def test_tampered_confirmation_metrics_are_rejected(field, value):
    document = deepcopy(load_approval())
    evaluation = document["factors"]["max_daily_return_1m"]["evaluation"]
    evaluation[field] = value
    with pytest.raises(ValueError):
        validate_approval(document)


def test_nonfinite_correlation_threshold_is_rejected():
    document = deepcopy(load_approval())
    document["selection"][
        "maximum_allowed_median_absolute_correlation"
    ] = float("nan")
    with pytest.raises(ValueError):
        validate_approval(document)


def test_upsert_wrapper_cannot_write_outside_approved_month_range():
    sql = build_upsert_sql(
        """
        SELECT asset_id, as_of_date, value, rank
        FROM public.price_daily
        WHERE trade_date BETWEEN %(start_month)s AND %(end_month)s
        """
    )
    assert "as_of_date >= %(start_month)s::date" in sql
    assert "as_of_date < (%(end_month)s::date + interval '1 month')" in sql


def test_correlation_cache_uses_an_equivalent_month_equality_join():
    class FakeCursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append((statement, params))

        def fetchall(self):
            return [(17, 2, 1, 1)]

    cur = FakeCursor()
    summary = _build_correlation_values(
        cur,
        factor_ids={"example": 17},
        start_month="2026-01",
        end_month="2026-01",
    )

    create_sql, params = cur.statements[0]
    assert (
        "date_trunc('month', v.as_of_date)::date = u.signal_month"
        in create_sql
    )
    assert "v.as_of_date >= u.signal_month" not in create_sql
    assert "v.as_of_date < (u.signal_month + interval '1 month')" not in create_sql
    assert params == ([17], date(2026, 1, 1), date(2026, 1, 1))
    assert summary["factors"]["example"] == {
        "rows": 2,
        "signal_months": 1,
        "max_rows_per_asset_month": 1,
    }


def test_dividend_lineage_preflight_fails_closed_on_missing_snapshots():
    class FakeCursor:
        def execute(self, *_):
            return None

        def fetchone(self):
            return (0, 0, 0)

    with pytest.raises(ValueError, match="배당 Silver lineage"):
        _validate_dividend_lineage(FakeCursor())


def test_dividend_lineage_preflight_reports_live_rows():
    class FakeCursor:
        def execute(self, *_):
            return None

        def fetchone(self):
            return (14560, 16259, 13594)

    assert _validate_dividend_lineage(FakeCursor()) == {
        "resolution_rows": 14560,
        "action_snapshot_rows": 16259,
        "canonical_join_rows": 13594,
    }


def test_constant_monthly_ranks_are_rejected_before_correlation():
    class FakeCursor:
        def __init__(self):
            self.execute_count = 0
            self.rowcount = -1
            self.responses = iter([
                (60, 1, date(2026, 6, 30), date(2026, 6, 30), 1, 0),
                (60, 60, True, 1, 1, True, 1, 1),
            ])

        def execute(self, *_):
            self.execute_count += 1
            if self.execute_count == 1:
                self.rowcount = 0
            elif self.execute_count == 2:
                self.rowcount = 60
            else:
                self.rowcount = -1

        def fetchone(self):
            return next(self.responses)

    item = validate_approval(load_approval())["max_daily_return_1m"]
    with pytest.raises(ValueError, match="rank가 상수로 퇴화"):
        _replace_values(
            FakeCursor(),
            factor_id=99,
            factor_key="max_daily_return_1m",
            item=item,
            start_month="2026-06",
            end_month="2026-06",
            expected_months=1,
        )


def test_publisher_lock_is_session_scoped_and_committed_before_snapshot():
    class FakeCursor:
        def __init__(self, statements):
            self.statements = statements

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, statement, params=None):
            self.statements.append((statement, params))

        def fetchone(self):
            return (True,)

    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.commits = 0
            self.rollbacks = 0

        def cursor(self):
            return FakeCursor(self.statements)

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    conn = FakeConnection()
    _acquire_publish_lock(conn)
    assert "pg_advisory_lock" in conn.statements[1][0]
    assert "pg_advisory_xact_lock" not in conn.statements[1][0]
    assert conn.commits == 1
    _release_publish_lock(conn)
    assert "pg_advisory_unlock" in conn.statements[-1][0]
    assert conn.commits == 2
