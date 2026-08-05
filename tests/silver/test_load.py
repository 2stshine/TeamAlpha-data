from datetime import date

import pandas as pd

from pipeline.silver import load
from pipeline.silver_quality.models import CandidateBundle


def test_daily_candidate_filter_excludes_unmapped_actions_explicitly():
    bundle = CandidateBundle(
        actions=pd.DataFrame([
            {
                "identifier": "005930",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
            {
                "identifier": "999999",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
            {
                "identifier": "250030",
                "effective_date": date(2026, 8, 5),
                "announcement_date": None,
            },
        ]),
        stats={
            "fundamental": {},
            "corporate_action": {
                "transformed_rows": 3,
                "excluded_rows": 0,
            },
        },
    )

    load._exclude_nontradable_candidates(
        bundle,
        {"005930"},
        {"250030"},
    )

    assert bundle.actions["identifier"].tolist() == ["005930"]
    stats = bundle.stats["corporate_action"]
    assert stats["transformed_rows"] == 1
    assert stats["excluded_rows"] == 2
    assert stats["no_tradable_price_action"]["row_count"] == 1
    assert stats["unsupported_market_action"]["row_count"] == 1
