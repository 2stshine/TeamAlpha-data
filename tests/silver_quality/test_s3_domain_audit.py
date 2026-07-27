from datetime import date

import pandas as pd

from pipeline.silver_quality import s3_domain_audit


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
    monkeypatch.setattr(
        s3_domain_audit.corporate_actions,
        "prepare",
        lambda base: (pd.DataFrame(), {}),
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
