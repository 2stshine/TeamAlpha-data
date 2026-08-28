from datetime import date
from uuid import uuid4

import pandas as pd

from pipeline.silver import corporate_actions, prices
from pipeline.silver.return_contract import invalidate_krx_total_return
from pipeline.silver.total_return_rebuild import RebuildSummary, _certify_contract


class _Cursor:
    def __init__(self, *, contract_exists=True, update_count=1):
        self.contract_exists = contract_exists
        self.update_count = update_count
        self.rowcount = -1
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if "UPDATE price_return_contract" in sql:
            self.rowcount = self.update_count

    def fetchone(self):
        return ("price_return_contract",) if self.contract_exists else (None,)


class _Connection:
    def __init__(self, *, contract_exists=True, update_count=1):
        self.cursor_instance = _Cursor(
            contract_exists=contract_exists,
            update_count=update_count,
        )

    def cursor(self):
        return self.cursor_instance


def test_missing_contract_table_is_backward_compatible_noop():
    connection = _Connection(contract_exists=False)

    changed = invalidate_krx_total_return(
        connection,
        reason="KRX_PRICE_PUBLISHED",
        quality_run_id=uuid4(),
    )

    assert changed is False
    assert len(connection.cursor_instance.statements) == 1
    assert "to_regclass" in connection.cursor_instance.statements[0][0]


def test_invalidation_demotes_contract_and_clears_certification():
    run_id = uuid4()
    connection = _Connection(contract_exists=True)

    changed = invalidate_krx_total_return(
        connection,
        reason="ISSUER_CASH_DIVIDEND_PUBLISHED",
        quality_run_id=run_id,
    )

    assert changed is True
    sql, params = connection.cursor_instance.statements[-1]
    compact = " ".join(sql.split())
    assert "SET status='BUILDING'" in compact
    assert "certified_at=NULL" in compact
    assert "field_name=%s" in compact
    assert "'invalidated_reason', %s::text" in compact
    assert "'invalidated_by_run_id', %s::uuid::text" in compact
    assert params == (
        run_id,
        "ISSUER_CASH_DIVIDEND_PUBLISHED",
        run_id,
        "KRX",
        "stock",
        "total_return_close",
    )


class _CertificationCursor:
    def __init__(self, summary):
        self.summary = summary
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return (
            self.summary.price_row_count,
            self.summary.asset_count,
            date.fromisoformat(self.summary.coverage_start),
            date.fromisoformat(self.summary.coverage_end),
        )


class _CertificationConnection:
    def __init__(self, summary):
        self.cursor_instance = _CertificationCursor(summary)

    def cursor(self):
        return self.cursor_instance


def test_successful_full_rebuild_is_the_certified_recovery_path():
    run_id = uuid4()
    summary = RebuildSummary(
        apply=True,
        asset_count=1,
        price_row_count=2,
        cash_action_count=1,
        canonical_event_count=1,
        applied_event_count=1,
        excluded_event_count=0,
        coverage_start="2026-01-02",
        coverage_end="2026-01-05",
        run_id=str(run_id),
    )
    connection = _CertificationConnection(summary)

    _certify_contract(connection, summary, run_id)

    sql, _ = connection.cursor_instance.statements[-1]
    compact = " ".join(sql.split())
    assert "'CERTIFIED'" in compact
    assert "status='CERTIFIED'" in compact
    assert "certified_at=clock_timestamp()" in compact


def _price_candidate(asset_type="stock"):
    return pd.DataFrame([{
        "identifier": "005930" if asset_type == "stock" else "KOSPI200",
        "source": "KRX",
        "trade_date": date(2026, 8, 8),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "adj_close": 100.0,
        "available_at": pd.Timestamp("2026-08-09T08:30:00+09:00"),
        "volume": 1,
        "trading_value": 100.0,
        "shares": 1 if asset_type == "stock" else None,
        "market_cap": 100.0,
        "market": "KOSPI" if asset_type == "stock" else None,
        "asset_type": asset_type,
    }])


def test_krx_stock_price_publish_invalidates_contract(monkeypatch):
    calls = []
    monkeypatch.setattr(prices.db, "upsert", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        prices,
        "invalidate_krx_total_return",
        lambda conn, **kwargs: calls.append((conn, kwargs)) or True,
    )
    connection = object()
    run_id = uuid4()

    prices.publish(
        connection,
        _price_candidate(),
        {"005930": 1},
        run_id,
    )

    assert calls == [(connection, {
        "reason": "KRX_PRICE_PUBLISHED",
        "quality_run_id": run_id,
    })]


def test_krx_index_only_publish_does_not_invalidate_stock_contract(monkeypatch):
    calls = []
    monkeypatch.setattr(prices.db, "upsert", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        prices,
        "invalidate_krx_total_return",
        lambda *args, **kwargs: calls.append(kwargs) or True,
    )

    prices.publish(
        object(),
        _price_candidate("index"),
        {"KOSPI200": 1},
        uuid4(),
    )

    assert calls == []


def _published_action_frame(scope="ISSUER"):
    row = {column: None for column in corporate_actions.PUBLISH_COLUMNS[1:-1]}
    row.update({
        "identifier": "005930",
        "source": "DART_DISCLOSURE",
        "action_key": "20260808000001",
        "action_type": "cash_dividend",
        "record_date": date(2026, 8, 31),
        "cash_amount": 500.0,
        "status": "announced",
        "confidence": "ANNOUNCEMENT_ONLY",
        "action_scope": scope,
    })
    return pd.DataFrame([row])


def test_issuer_cash_action_publish_invalidates_contract(monkeypatch):
    calls = []
    frame = _published_action_frame()
    monkeypatch.setattr(
        corporate_actions,
        "normalize_for_publish",
        lambda _: frame.copy(),
    )
    monkeypatch.setattr(
        corporate_actions.db,
        "upsert",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        corporate_actions,
        "invalidate_krx_total_return",
        lambda conn, **kwargs: calls.append((conn, kwargs)) or True,
    )
    connection = object()
    run_id = uuid4()

    corporate_actions.publish(
        connection,
        pd.DataFrame([{"unused": 1}]),
        {"005930": 1},
        run_id,
    )

    assert calls == [(connection, {
        "reason": "ISSUER_CASH_DIVIDEND_PUBLISHED",
        "quality_run_id": run_id,
    })]


def test_nonissuer_cash_action_does_not_invalidate_contract(monkeypatch):
    calls = []
    frame = _published_action_frame("RELATED_COMPANY")
    monkeypatch.setattr(
        corporate_actions,
        "normalize_for_publish",
        lambda _: frame.copy(),
    )
    monkeypatch.setattr(
        corporate_actions.db,
        "upsert",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        corporate_actions,
        "invalidate_krx_total_return",
        lambda *args, **kwargs: calls.append(kwargs) or True,
    )

    corporate_actions.publish(
        object(),
        pd.DataFrame([{"unused": 1}]),
        {"005930": 1},
        uuid4(),
    )

    assert calls == []
