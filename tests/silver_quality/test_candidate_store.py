from datetime import date

import pandas as pd
import pytest

from pipeline.silver_quality.candidate_store import CandidateStore


def test_candidate_round_trip_and_resume_marker(tmp_path):
    store = CandidateStore(str(tmp_path / "candidates"))
    frame = pd.DataFrame([
        {"identifier": "005930", "trade_date": date(2026, 7, 24), "close": 100.0},
        {"identifier": "000660", "trade_date": date(2026, 7, 24), "close": 200.0},
    ])

    part = store.save("price_daily", "price:year=2026", frame, "bronze-fp")
    marker = store.metadata("price_daily", "price:year=2026", "bronze-fp")

    assert marker == part
    assert store.metadata(
        "price_daily", "price:year=2026", "different-fp",
    ) is None
    loaded = store.load(part)
    pd.testing.assert_frame_equal(loaded, frame)


def test_candidate_checksum_detects_tampering(tmp_path):
    store = CandidateStore(str(tmp_path / "candidates"))
    frame = pd.DataFrame([{"identifier": "005930", "close": 100.0}])
    part = store.save("price_daily", "price:year=2026", frame, "bronze-fp")

    with open(part.object_uri, "ab") as output:
        output.write(b"tampered")

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        store.load(part)
