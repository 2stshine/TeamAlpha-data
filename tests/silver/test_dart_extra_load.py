import pandas as pd

from pipeline.silver.dart_extra_load import _total_return_actions


def test_total_return_action_scope_is_minimal_and_issuer_only():
    frame = pd.DataFrame([
        {"event_type": "cash_dividend", "action_scope": "ISSUER", "key": 1},
        {"event_type": "ex_dividend", "action_scope": "ISSUER", "key": 2},
        {"event_type": "stock_split", "action_scope": "ISSUER", "key": 3},
        {
            "event_type": "cash_dividend",
            "action_scope": "RELATED_COMPANY",
            "key": 4,
        },
        {"event_type": "cash_dividend", "action_scope": "UNKNOWN", "key": 5},
    ])

    result = _total_return_actions(frame)

    assert result["key"].tolist() == [1, 2]
