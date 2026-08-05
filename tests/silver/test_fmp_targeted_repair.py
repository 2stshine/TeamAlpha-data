from pipeline.silver.fmp_targeted_repair import ExistingAsset, _stale_assets


def test_stale_assets_requires_no_admitted_ticker_episode():
    existing = [
        ExistingAsset(1, "Apple", ("AAPL",)),
        ExistingAsset(2, "Renamed", ("OLD", "NEW")),
        ExistingAsset(3, "Warrant", ("ACMEW",)),
    ]

    stale = _stale_assets(existing, {"AAPL", "OLD", "NEW"})

    assert stale == [ExistingAsset(3, "Warrant", ("ACMEW",))]
