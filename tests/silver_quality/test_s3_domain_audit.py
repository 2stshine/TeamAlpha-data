from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from pipeline.silver_quality import s3_domain_audit
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)


def test_price_universes_streams_supported_market_tickers(tmp_path):
    marcap = tmp_path / "stock" / "marcap" / "date=2026-01-02" / "all.parquet"
    marcap.parent.mkdir(parents=True)
    pd.DataFrame({
        "Code": ["005930", "950001"],
        "Market": ["KOSPI", "KONEX"],
    }).to_parquet(marcap, index=False)
    krx = tmp_path / "stock" / "krxapi" / "date=2026-01-03" / "kosdaq.parquet"
    krx.parent.mkdir(parents=True)
    pd.DataFrame({
        "ISU_CD": ["035420", "950002"],
        "MKT_NM": ["KOSDAQ", "KONEX"],
    }).to_parquet(krx, index=False)

    all_identifiers, supported = s3_domain_audit._price_universes(
        str(tmp_path)
    )

    assert all_identifiers == {"005930", "035420", "950001", "950002"}
    assert supported == {"005930", "035420"}


def test_fundamental_bundle_does_not_prepare_full_price_frame(monkeypatch):
    asset_frame = pd.DataFrame([
        {
            "natural_key": "005930",
            "name": "삼성전자",
            "asset_type": "stock",
            "exchange": "KRX",
            "currency": "KRW",
        }
    ])
    identifier_frame = pd.DataFrame([
        {
            "natural_key": "005930",
            "source": "KRX",
            "identifier": "005930",
        }
    ])
    fundamental_frame = pd.DataFrame([
        {
            "identifier": "005930",
            "source": "DART",
            "period_end": date(2025, 12, 31),
            "fiscal_period": "FY",
            "fs_type": "CFS",
            "revision_key": "20260301000001",
            "metric": "total_assets",
            "value": 100,
        }
    ])
    monkeypatch.setattr(
        s3_domain_audit.assets,
        "prepare",
        lambda base: (asset_frame, identifier_frame),
    )
    monkeypatch.setattr(
        s3_domain_audit.assets,
        "restrict_to_price_universe",
        lambda assets, identifiers, supported: (assets, identifiers),
    )
    monkeypatch.setattr(
        s3_domain_audit,
        "_price_universes",
        lambda base: ({"005930"}, {"005930"}),
    )
    monkeypatch.setattr(
        s3_domain_audit.financials,
        "prepare",
        lambda base: (
            fundamental_frame,
            {
                "input_rows": 1,
                "transformed_rows": 1,
                "excluded_rows": 0,
                "rejected_rows": 0,
            },
        ),
    )
    monkeypatch.setattr(
        s3_domain_audit.financials,
        "exclude_nontradable",
        lambda frame, stats, supported, unsupported: (frame, stats),
    )
    monkeypatch.setattr(
        s3_domain_audit.prices,
        "prepare",
        lambda base: (_ for _ in ()).throw(
            AssertionError("fundamental domain must not build prices")
        ),
    )

    bundle = s3_domain_audit._fundamental_bundle("/unused")

    assert bundle.prices.empty
    assert len(bundle.fundamentals) == 1


def test_price_bundle_does_not_prepare_fundamentals(monkeypatch):
    asset_frame = pd.DataFrame([
        {
            "natural_key": "005930",
            "name": "삼성전자",
            "asset_type": "stock",
            "exchange": "KRX",
            "currency": "KRW",
        }
    ])
    identifier_frame = pd.DataFrame([
        {
            "natural_key": "005930",
            "source": "KRX",
            "identifier": "005930",
        }
    ])
    price_frame = pd.DataFrame([
        {
            "identifier": "005930",
            "asset_type": "stock",
            "trade_date": date(2026, 1, 2),
        }
    ])
    monkeypatch.setattr(
        s3_domain_audit.assets,
        "prepare",
        lambda base: (asset_frame, identifier_frame),
    )
    monkeypatch.setattr(
        s3_domain_audit.assets,
        "restrict_to_price_universe",
        lambda assets, identifiers, supported: (assets, identifiers),
    )
    monkeypatch.setattr(
        s3_domain_audit.assets,
        "preferred_share_issuer_map",
        lambda assets: {},
    )
    monkeypatch.setattr(
        s3_domain_audit.prices,
        "prepare",
        lambda base: (
            price_frame,
            {
                "input_rows": 1,
                "transformed_rows": 1,
                "excluded_rows": 0,
                "rejected_rows": 0,
            },
        ),
    )
    verify_calls = []
    monkeypatch.setattr(
        s3_domain_audit,
        "verify_snapshot_manifest",
        lambda base, **kwargs: verify_calls.append((base, kwargs))
        or SimpleNamespace(
            coverage_start=date(2015, 1, 1),
            coverage_end=date(2026, 1, 2),
        ),
    )
    action_calls = []
    monkeypatch.setattr(
        s3_domain_audit.corporate_actions,
        "prepare",
        lambda base, **kwargs: action_calls.append((base, kwargs))
        or (pd.DataFrame(), {}),
    )
    monkeypatch.setattr(
        s3_domain_audit.corporate_actions,
        "inherit_issuer_events",
        lambda frame, mapping: (frame, {}),
    )
    monkeypatch.setattr(
        s3_domain_audit.financials,
        "prepare",
        lambda base: (_ for _ in ()).throw(
            AssertionError("price domain must not build fundamentals")
        ),
    )

    bundle = s3_domain_audit._price_bundle("/unused")

    assert bundle.fundamentals.empty
    assert len(bundle.prices) == 1
    assert verify_calls == [(
        "/unused", {"required_start": date(2015, 1, 1)},
    )]
    assert action_calls == [(
        "/unused",
        {
            "coverage_start": date(2015, 1, 1),
            "coverage_end": date(2026, 1, 2),
        },
    )]


def test_price_history_tail_keeps_only_last_twenty_trading_days():
    frame = pd.DataFrame({
        "identifier": ["005930"] * 25,
        "trade_date": [
            date(2025, 12, day) for day in range(1, 26)
        ],
        "close": range(25),
    })

    tail = s3_domain_audit._price_history_tail(
        pd.DataFrame(),
        frame,
    )

    assert len(tail) == 20
    assert tail["trade_date"].min() == date(2025, 12, 6)


def test_align_history_adj_close_matches_first_current_return():
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2025, 12, 30),
        "close": 100.0,
        "adj_close": 100.0,
    }])
    current = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 1, 2),
        "close": 55.0,
        "adj_close": 55.0,
        "prev_diff": 5.0,
    }])

    aligned = s3_domain_audit._align_history_adj_close(history, current)

    # KRX reference is 50, so the economic return is 10%.
    assert aligned.iloc[0]["adj_close"] == pytest.approx(50.0)


def test_warning_totals_sum_repeated_annual_rules():
    results = [
        CheckResult(
            rule_code="PRICE_WARNING",
            dataset="price_daily",
            severity=Severity.WARNING,
            status=CheckStatus.FAIL,
            expected="fixture",
            actual="fixture",
            failed_count=count,
            partition_key=f"year:{year}",
        )
        for year, count in ((2025, 2), (2026, 3))
    ]

    assert s3_domain_audit._warning_totals(results) == {
        "PRICE_WARNING": 5,
    }
