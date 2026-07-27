from datetime import date

import pytest

from pipeline.silver_quality.streaming_adj_close import (
    _SeriesState,
    _factor_for_row,
)


def test_factor_uses_krx_reference_price():
    previous = _SeriesState(date(2026, 1, 2), 100.0, 0)

    episode, factor = _factor_for_row(
        date(2026, 1, 5),
        55.0,
        5.0,
        previous,
    )

    assert episode == 0
    assert factor == pytest.approx(0.5)


def test_factor_resets_after_new_listing_episode():
    previous = _SeriesState(date(2024, 1, 2), 100.0, 2)

    episode, factor = _factor_for_row(
        date(2026, 1, 5),
        55.0,
        5.0,
        previous,
    )

    assert episode == 3
    assert factor == 1.0
