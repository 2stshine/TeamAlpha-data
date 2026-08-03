from pipeline.fmp_backfill_ecs import _silver_prefixes


def test_silver_year_downloads_only_bounded_partitions():
    prefixes = _silver_prefixes(2019)

    assert "stock/fmp/eod-bulk/date=2019-" in prefixes
    assert "financials/fmp/income/year=2019/" in prefixes
    assert "corporate_actions/fmp/dividends/year=2019/" in prefixes
    assert "corporate_actions/fmp/splits/year=" in prefixes
    assert "fx/fmp/pair=USDKRW/from=" in prefixes
