"""asset 및 외부 identifier 규칙."""
from __future__ import annotations

import pandas as pd

from pipeline.silver_quality.models import CheckResult, Severity
from pipeline.silver_quality.rules.common import duplicate_keys, null_keys, result


def check_assets(
    assets: pd.DataFrame,
    identifiers: pd.DataFrame,
    partition_key: str | None = None,
) -> list[CheckResult]:
    checks = [
        null_keys(assets, "asset", ["natural_key", "name", "asset_type", "exchange", "currency"], partition_key),
        duplicate_keys(assets, "asset", ["natural_key"], partition_key),
        null_keys(identifiers, "asset_identifier", ["natural_key", "source", "identifier"], partition_key),
        duplicate_keys(identifiers, "asset_identifier", ["source", "identifier"], partition_key),
    ]
    if assets.empty:
        return checks

    invalid_type = assets[~assets["asset_type"].isin(["stock", "index"])]
    checks.append(result(
        "ASSET_TYPE_ENUM", "asset", Severity.ERROR, invalid_type,
        "asset_type in {stock,index}", partition_key=partition_key,
    ))

    if not identifiers.empty:
        missing_asset = identifiers[~identifiers["natural_key"].isin(set(assets["natural_key"]))]
        checks.append(result(
            "ASSET_IDENTIFIER_ORPHAN", "asset_identifier", Severity.ERROR, missing_asset,
            "every identifier maps to an asset candidate", partition_key=partition_key,
        ))
    return checks
