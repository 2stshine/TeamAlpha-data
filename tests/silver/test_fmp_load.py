from datetime import date

import pandas as pd

from pipeline.silver.fmp_load import (
    _existing_identifier_map,
    _fundamental_partitions,
)


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, *_):
        return None

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


def test_existing_identifier_map_includes_historical_ticker_episodes():
    identifiers = pd.DataFrame([
        {
            "natural_key": "FMP:NEW",
            "source": "FMP",
            "identifier": "OLD",
            "identifier_type": "ticker",
            "valid_from": date.min,
            "valid_to": date(2025, 12, 31),
        },
        {
            "natural_key": "FMP:NEW",
            "source": "FMP",
            "identifier": "NEW",
            "identifier_type": "ticker",
            "valid_from": date(2026, 1, 1),
            "valid_to": None,
        },
    ])
    stored = [
        ("ticker", "OLD", 42, date(2025, 12, 31), date.min),
        ("ticker", "NEW", 42, None, date(2026, 1, 1)),
    ]

    mapping = _existing_identifier_map(_Connection(stored), identifiers)

    assert mapping == {"OLD": 42, "NEW": 42, "FMP:NEW": 42}


def test_fundamentals_are_partitioned_by_statement_and_fiscal_period():
    frame = pd.DataFrame([
        {"statement_type": "IS", "fiscal_period": "FY", "value": 1},
        {"statement_type": "IS", "fiscal_period": "Q1", "value": 2},
        {"statement_type": "BS", "fiscal_period": "FY", "value": 3},
    ])

    partitions = _fundamental_partitions(2025, frame)

    assert [key for key, _ in partitions] == [
        "fundamental:year=2025:statement=BS:period=FY",
        "fundamental:year=2025:statement=IS:period=FY",
        "fundamental:year=2025:statement=IS:period=Q1",
    ]
    assert [len(partition) for _, partition in partitions] == [1, 1, 1]
