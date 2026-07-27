from datetime import date

import pandas as pd

from pipeline.silver import prices


def _write_marcap(root, day: str, close: int) -> None:
    path = root / "stock" / "marcap" / f"date={day}" / "all.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame([{
        "Code": "005930",
        "Date": day,
        "Open": close,
        "High": close,
        "Low": close,
        "Close": close,
        "Volume": 1,
        "Amount": close,
        "Stocks": 100,
        "Marcap": close * 100,
        "Changes": 0,
        "Market": "KOSPI",
    }]).to_parquet(path, index=False)


def test_prepare_reads_only_requested_year(tmp_path):
    _write_marcap(tmp_path, "2025-12-30", 100)
    _write_marcap(tmp_path, "2026-01-02", 110)

    frame, stats = prices.prepare(
        str(tmp_path),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert prices.available_years(str(tmp_path)) == [2025, 2026]
    assert frame["trade_date"].tolist() == [date(2026, 1, 2)]
    assert stats["source_file_count"] == 1
