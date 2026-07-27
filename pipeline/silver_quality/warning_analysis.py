"""운영 cutoff의 비차단 Warning을 전건 원인 분류한다.

Silver를 수정하지 않고 immutable Bronze cutoff를 내려받아 후보를 재구성한 뒤
기업행사 미대사, 감자 주식수 불일치, 가격 분포 drift, 회계식 불일치를 JSON으로
출력한다. ECS one-off 진단용이다.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.silver import assets, corporate_actions, prices as price_loader
from pipeline.silver.prices import LISTING_EPISODE_GAP_DAYS
from pipeline.silver_quality.backfill import _candidate_bundle
from pipeline.silver_quality.ecs_backfill import _sync_cutoff
from pipeline.silver_quality.rules.prices import (
    _attach_corporate_action_evidence,
    _dart_actions_without_krx_adjustment,
    _dart_share_count_factor_results,
)


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def _buckets(values: pd.Series, boundaries: list[float]) -> dict[str, int]:
    labels = [
        f"({boundaries[index - 1] if index else '-inf'},{boundary}]"
        for index, boundary in enumerate(boundaries)
    ] + [f"({boundaries[-1]},inf)"]
    return {
        str(key): int(count)
        for key, count in pd.cut(
            values,
            [-np.inf, *boundaries, np.inf],
            labels=labels,
        ).value_counts(sort=False).items()
    }


def _combined_prices(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    for column in ("close", "prev_diff", "shares", "market_cap"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(["identifier", "trade_date"]).reset_index(drop=True)
    previous_date = frame.groupby("identifier")["trade_date"].shift(1)
    gap_days = (
        pd.to_datetime(frame["trade_date"]) - pd.to_datetime(previous_date)
    ).dt.days
    frame["listing_episode"] = (
        gap_days.gt(LISTING_EPISODE_GAP_DAYS)
        .groupby(frame["identifier"])
        .cumsum()
    )
    keys = [frame["identifier"], frame["listing_episode"]]
    series = frame.groupby(keys)
    frame["previous_close"] = series["close"].shift(1)
    frame["previous_shares"] = series["shares"].shift(1)
    frame["previous_market_cap"] = series["market_cap"].shift(1)
    frame["source_reference"] = frame["close"] - frame["prev_diff"]
    frame["source_adjustment_factor"] = (
        frame["source_reference"]
        / frame["previous_close"].replace(0, np.nan)
    )
    frame["source_adjustment_event"] = (
        frame["source_adjustment_factor"].sub(1).abs().gt(0.005)
    )
    frame["identifier"] = frame["identifier"].astype(str)
    return frame


def _nearest_dart_distance(
    rows: pd.DataFrame,
    actions: pd.DataFrame,
) -> pd.Series:
    evidence = actions.copy()
    confirmation = (
        evidence["confirms_price_adjustment"]
        if "confirms_price_adjustment" in evidence
        else evidence["expects_price_adjustment"]
    )
    evidence = evidence[
        confirmation.fillna(False)
        & evidence["effective_date"].notna()
    ]
    evidence["identifier"] = evidence["identifier"].astype(str)
    evidence["effective_date"] = pd.to_datetime(
        evidence["effective_date"], errors="coerce"
    ).dt.date
    dates = {
        identifier: list(group["effective_date"].dropna())
        for identifier, group in evidence.groupby("identifier", sort=False)
    }
    return pd.Series([
        min(
            (
                abs((row.trade_date - value).days)
                for value in dates.get(str(row.identifier), [])
            ),
            default=np.nan,
        )
        for row in rows.itertuples()
    ], index=rows.index, dtype="float64")


def _price_adjustment_analysis(
    combined: pd.DataFrame,
    actions: pd.DataFrame,
    candidate_dates: set | None = None,
) -> dict:
    attached = _attach_corporate_action_evidence(combined, actions)
    current = attached[attached["source_adjustment_event"]].copy()
    if candidate_dates is not None:
        current = current[current["trade_date"].isin(candidate_dates)]
    share_ratio = current["shares"] / current["previous_shares"].replace(0, np.nan)
    structure = (
        share_ratio.notna()
        & (
            current["source_adjustment_factor"] * share_ratio
        ).sub(1).abs().le(0.02)
    )
    missing = current[
        ~current["dart_event_confirmed"] & ~structure
    ].copy()
    missing["share_ratio"] = share_ratio.loc[missing.index]
    missing["reciprocal_share_error"] = (
        missing["source_adjustment_factor"] * missing["share_ratio"] - 1
    ).abs()
    missing["share_change"] = missing["share_ratio"].sub(1).abs()
    missing["nearest_dart_days"] = _nearest_dart_distance(missing, actions)
    categories = np.select(
        [
            missing["reciprocal_share_error"].le(0.02),
            missing["share_change"].le(0.005)
            & missing["source_adjustment_factor"].sub(1).abs().le(0.02),
            missing["share_change"].le(0.005),
            missing["share_change"].gt(0.005),
        ],
        [
            "KRX_RECIPROCAL_SHARE_STRUCTURE",
            "SMALL_REFERENCE_RESET_WITHOUT_SHARE_CHANGE",
            "REFERENCE_RESET_WITHOUT_SHARE_CHANGE",
            "NON_RECIPROCAL_SHARE_CHANGE",
        ],
        default="MISSING_SHARE_OBSERVATION",
    )
    return {
        "count": len(missing),
        "cause": dict(Counter(categories)),
        "by_year": {
            str(year): int(count)
            for year, count in missing.groupby(
                missing["trade_date"].map(lambda value: value.year)
            ).size().items()
        },
        "samples": (
            missing[[
                "identifier",
                "trade_date",
                "source_adjustment_factor",
                "share_ratio",
                "nearest_dart_days",
            ]]
            .head(20)
            .to_dict("records")
        ),
        "nearest_dart_days": _buckets(
            missing["nearest_dart_days"], [7, 14, 30, 60, 180]
        ),
        "factor_abs_change": _buckets(
            missing["source_adjustment_factor"].sub(1).abs(),
            [0.01, 0.02, 0.05, 0.10, 0.25, 0.50, 1.0],
        ),
    }


def _dart_without_krx_analysis(
    combined: pd.DataFrame,
    actions: pd.DataFrame,
    candidate_dates: set,
) -> dict:
    missing = _dart_actions_without_krx_adjustment(
        combined,
        actions,
        candidate_dates,
    )
    if missing.empty:
        return {"count": 0}
    detail_columns = [
        "identifier", "event_type", "effective_date", "rcept_no",
        "source", "report_name", "action_method",
        "share_count_factor_comparable",
    ]
    details = actions[[c for c in detail_columns if c in actions]].copy()
    details["identifier"] = details["identifier"].astype(str)
    details["effective_date"] = pd.to_datetime(
        details["effective_date"], errors="coerce"
    ).dt.date
    missing = missing.merge(
        details,
        on=["identifier", "event_type", "effective_date", "rcept_no"],
        how="left",
    )
    stock = combined[combined["asset_type"].eq("stock")]
    by_identifier = {
        identifier: group
        for identifier, group in stock.groupby("identifier", sort=False)
    }
    causes = []
    for event in missing.itertuples():
        group = by_identifier.get(event.identifier)
        if group is None or group.empty:
            causes.append("NO_PRICE_SERIES")
            continue
        action_row = actions[
            actions["rcept_no"].astype(str).eq(str(event.rcept_no))
            & actions["identifier"].astype(str).eq(str(event.identifier))
        ]
        window = int(
            action_row["match_window_days"].iloc[0]
            if not action_row.empty
            else 0
        )
        distances = group["trade_date"].map(
            lambda value: abs((value - event.effective_date).days)
        )
        nearby = group[distances.le(window)]
        max_adjustment = (
            nearby["source_adjustment_factor"].sub(1).abs().max()
            if not nearby.empty
            else np.nan
        )
        share_factor = (
            nearby["previous_shares"]
            / nearby["shares"].replace(0, np.nan)
        )
        share_changed = share_factor.sub(1).abs().gt(0.005).any()
        if pd.notna(max_adjustment) and 1e-9 <= max_adjustment <= 0.005:
            causes.append("KRX_ADJUSTMENT_BELOW_WARNING_THRESHOLD")
        elif share_changed:
            causes.append("SHARE_CHANGE_WITHOUT_KRX_REFERENCE_RESET")
        else:
            causes.append("NO_OBSERVED_PRICE_OR_SHARE_ADJUSTMENT")
    missing["cause"] = causes
    missing["share_count_factor_comparable"] = (
        missing["share_count_factor_comparable"].fillna(False).astype(bool)
    )
    return {
        "count": len(missing),
        "cause": dict(Counter(causes)),
        "event_type": {
            str(key): int(value)
            for key, value in missing["event_type"].value_counts().items()
        },
        "source": {
            str(key): int(value)
            for key, value in missing["source"].fillna("UNKNOWN").value_counts().items()
        },
        "event_type_by_cause": {
            f"{event_type}:{cause}": int(count)
            for (event_type, cause), count in missing.groupby(
                ["event_type", "cause"],
                dropna=False,
            ).size().items()
        },
        "comparable_by_cause": {
            f"{'COMPARABLE' if comparable else 'NOT_COMPARABLE'}:{cause}": int(count)
            for (comparable, cause), count in missing.groupby(
                ["share_count_factor_comparable", "cause"],
                dropna=False,
            ).size().items()
        },
        "by_year": {
            str(year): int(count)
            for year, count in missing.groupby(
                missing["effective_date"].map(lambda value: value.year)
            ).size().items()
        },
        "samples": (
            missing[[
                column
                for column in (
                    "identifier",
                    "event_type",
                    "effective_date",
                    "rcept_no",
                    "source",
                    "report_name",
                    "action_method",
                    "share_count_factor_comparable",
                    "cause",
                )
                if column in missing
            ]]
            .head(20)
            .to_dict("records")
        ),
    }


def _share_mismatch_analysis(
    combined: pd.DataFrame,
    actions: pd.DataFrame,
) -> dict:
    mismatches, explained = _dart_share_count_factor_results(
        combined,
        actions,
    )
    stock = combined[combined["asset_type"].eq("stock")]
    by_identifier = {
        identifier: group.sort_values("trade_date")
        for identifier, group in stock.groupby("identifier", sort=False)
    }
    wider_causes = []
    for row in mismatches.itertuples():
        group = by_identifier.get(row.identifier)
        if group is None or group.empty:
            wider_causes.append("NO_PRICE_SERIES")
            continue
        distances = group["trade_date"].map(
            lambda value: abs((value - row.effective_date).days)
        )
        matched = None
        for window in (14, 30, 60):
            nearby = group[
                distances.le(window)
                & group["previous_shares"].gt(0)
                & group["shares"].gt(0)
            ]
            if nearby.empty:
                continue
            factors = (
                nearby["previous_shares"]
                / nearby["shares"].replace(0, np.nan)
            )
            error = (
                factors - row.dart_share_count_factor
            ).abs() / row.dart_share_count_factor
            if error.min() <= 0.02:
                matched = f"MATCHED_WITHIN_{window}_DAYS"
                break
        if matched:
            wider_causes.append(matched)
        elif abs(float(row.krx_share_count_factor) - 1) <= 0.005:
            wider_causes.append("NO_MATCHING_KRX_SHARE_CHANGE_WITHIN_60_DAYS")
        else:
            wider_causes.append("DIFFERENT_KRX_SHARE_CHANGE_WITHIN_60_DAYS")
    return {
        "warning_count": len(mismatches),
        "explained_count": len(explained),
        "wider_window_cause": dict(Counter(wider_causes)),
        "relative_error": _buckets(
            mismatches["share_factor_relative_error"],
            [0.02, 0.05, 0.10, 0.25, 0.50, 1.0],
        ) if not mismatches.empty else {},
        "methods": {
            str(method): int(count)
            for method, count in mismatches["action_method"].fillna(
                "UNKNOWN"
            ).value_counts().head(20).items()
        },
        "samples": mismatches.head(20).to_dict("records"),
    }


def _distribution_analysis(prices: pd.DataFrame) -> dict:
    stock = prices[prices["asset_type"].eq("stock")].copy()
    stock["return"] = (
        stock.sort_values(["identifier", "trade_date"])
        .groupby("identifier")["close"]
        .pct_change(fill_method=None)
    )
    daily = (
        stock.groupby("trade_date")["return"].median()
        .rename("median_return")
        .reset_index()
        .sort_values("trade_date")
    )
    daily["baseline"] = daily["median_return"].shift(1).rolling(
        20, min_periods=20
    ).median()
    deviation = (daily["median_return"] - daily["baseline"]).abs()
    daily["mad"] = deviation.shift(1).rolling(20, min_periods=20).median()
    threshold = np.maximum(0.05, daily["mad"].fillna(0) * 5)
    bad = daily[deviation.gt(threshold) & daily["baseline"].notna()].copy()
    bad["direction"] = np.where(
        bad["median_return"].gt(0), "MARKET_WIDE_UP", "MARKET_WIDE_DOWN"
    )
    return {
        "count": len(bad),
        "direction": dict(Counter(bad["direction"])),
        "dates": bad[
            ["trade_date", "median_return", "baseline", "mad", "direction"]
        ].to_dict("records"),
    }


def _merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + int(value)


def _price_partition_warning_analysis(base: str) -> dict:
    """Analyze every price warning with bounded memory and boundary context."""
    asset_frame, _ = assets.prepare(base)
    actions, _ = corporate_actions.prepare(base)
    actions, _ = corporate_actions.inherit_issuer_events(
        actions,
        assets.preferred_share_issuer_map(asset_frame),
    )
    years = price_loader.available_years(base)
    history = pd.DataFrame()
    annual: dict[str, dict] = {}
    totals = {
        "price_adjustment_without_dart": {
            "count": 0,
            "cause": {},
            "nearest_dart_days": {},
            "factor_abs_change": {},
            "samples": [],
        },
        "dart_action_without_krx": {
            "count": 0,
            "cause": {},
            "event_type": {},
            "source": {},
            "event_type_by_cause": {},
            "comparable_by_cause": {},
            "samples": [],
        },
        "dart_share_count_mismatch": {
            "warning_count": 0,
            "explained_count": 0,
            "wider_window_cause": {},
            "relative_error": {},
            "methods": {},
            "samples": [],
        },
        "price_distribution_drift": {
            "count": 0,
            "direction": {},
            "dates": [],
        },
    }
    for year in years:
        current, _ = price_loader.prepare(
            base,
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31),
        )
        lookahead = pd.DataFrame()
        if year < years[-1]:
            lookahead, _ = price_loader.prepare(
                base,
                start_date=date(year + 1, 1, 1),
                end_date=date(year + 1, 1, 31),
            )
        context = pd.concat([history, current, lookahead], ignore_index=True)
        combined = _combined_prices(context)
        candidate_dates = set(current["trade_date"])
        effective = pd.to_datetime(
            actions["effective_date"],
            errors="coerce",
        )
        scoped_actions = actions[
            effective.between(
                pd.Timestamp(date(year, 1, 1)),
                pd.Timestamp(date(year, 12, 31)),
            )
        ]
        report = {
            "price_adjustment_without_dart": _price_adjustment_analysis(
                combined,
                actions,
                candidate_dates,
            ),
            "dart_action_without_krx": _dart_without_krx_analysis(
                combined,
                scoped_actions,
                candidate_dates,
            ),
            "dart_share_count_mismatch": _share_mismatch_analysis(
                combined,
                scoped_actions,
            ),
            "price_distribution_drift": _distribution_analysis(context),
        }
        report["price_distribution_drift"]["dates"] = [
            item
            for item in report["price_distribution_drift"].get("dates", [])
            if item["trade_date"] in candidate_dates
        ]
        report["price_distribution_drift"]["count"] = len(
            report["price_distribution_drift"]["dates"]
        )
        report["price_distribution_drift"]["direction"] = dict(Counter(
            item["direction"]
            for item in report["price_distribution_drift"]["dates"]
        ))
        annual[str(year)] = report

        price_adjustment = report["price_adjustment_without_dart"]
        totals["price_adjustment_without_dart"]["count"] += int(
            price_adjustment["count"]
        )
        for key in ("cause", "nearest_dart_days", "factor_abs_change"):
            _merge_counts(
                totals["price_adjustment_without_dart"][key],
                price_adjustment.get(key, {}),
            )
        totals["price_adjustment_without_dart"]["samples"].extend(
            price_adjustment.get("samples", [])
        )
        totals["price_adjustment_without_dart"]["samples"] = (
            totals["price_adjustment_without_dart"]["samples"][:20]
        )
        dart_missing = report["dart_action_without_krx"]
        totals["dart_action_without_krx"]["count"] += int(
            dart_missing.get("count", 0)
        )
        for key in (
            "cause",
            "event_type",
            "source",
            "event_type_by_cause",
            "comparable_by_cause",
        ):
            _merge_counts(
                totals["dart_action_without_krx"][key],
                dart_missing.get(key, {}),
            )
        totals["dart_action_without_krx"]["samples"].extend(
            dart_missing.get("samples", [])
        )
        totals["dart_action_without_krx"]["samples"] = (
            totals["dart_action_without_krx"]["samples"][:20]
        )
        share = report["dart_share_count_mismatch"]
        for key in ("warning_count", "explained_count"):
            totals["dart_share_count_mismatch"][key] += int(
                share.get(key, 0)
            )
        for key in ("wider_window_cause", "relative_error", "methods"):
            _merge_counts(
                totals["dart_share_count_mismatch"][key],
                share.get(key, {}),
            )
        totals["dart_share_count_mismatch"]["samples"].extend(
            share.get("samples", [])
        )
        totals["dart_share_count_mismatch"]["samples"] = (
            totals["dart_share_count_mismatch"]["samples"][:20]
        )
        drift = report["price_distribution_drift"]
        totals["price_distribution_drift"]["count"] += int(
            drift["count"]
        )
        _merge_counts(
            totals["price_distribution_drift"]["direction"],
            drift["direction"],
        )
        totals["price_distribution_drift"]["dates"].extend(drift["dates"])

        dates = sorted(current["trade_date"].dropna().unique())
        tail_dates = set(dates[-60:])
        history = current[current["trade_date"].isin(tail_dates)].copy()
    return {"totals": totals, "annual": annual}


def _accounting_analysis(fundamentals: pd.DataFrame) -> dict:
    scope = [
        "identifier", "source", "period_end", "fiscal_period",
        "fs_type", "revision_key",
    ]
    pivot = fundamentals.pivot_table(
        index=scope,
        columns="metric",
        values="value",
        aggfunc="first",
    )
    required = ["total_assets", "total_liabilities", "total_equity"]
    if not set(required).issubset(pivot.columns):
        return {"count": 0}
    values = pivot[required].dropna().copy()
    values["relative_error"] = (
        values["total_assets"]
        - values["total_liabilities"]
        - values["total_equity"]
    ).abs() / values["total_assets"].abs().replace(0, np.nan)
    bad = values[values["relative_error"].gt(0.01)].reset_index()
    absolute_balance_error = (
        bad["total_assets"].abs()
        - bad["total_liabilities"].abs()
        - bad["total_equity"].abs()
    ).abs() / bad["total_assets"].abs().replace(0, np.nan)
    bad["cause"] = np.select(
        [
            absolute_balance_error.le(0.01),
            bad["relative_error"].le(0.03),
            bad["relative_error"].le(0.10),
        ],
        [
            "LIABILITY_OR_EQUITY_SIGN_INVERSION",
            "SMALL_1_TO_3_PERCENT_DIFFERENCE",
            "MODERATE_3_TO_10_PERCENT_DIFFERENCE",
        ],
        default="LARGE_OVER_10_PERCENT_DIFFERENCE",
    )
    bad_index = pd.MultiIndex.from_frame(bad[scope])
    source_rows = fundamentals[
        fundamentals["metric"].isin(required)
        & pd.MultiIndex.from_frame(fundamentals[scope]).isin(bad_index)
    ].copy()
    source_rows["supplemental"] = source_rows["source_file"].astype(str).str.contains(
        "/financials/dart_full/",
        regex=False,
    )
    source_mix = (
        source_rows.groupby(scope)["supplemental"]
        .agg(["sum", "count"])
        .reset_index()
    )
    source_mix["source_mix"] = np.select(
        [
            source_mix["sum"].eq(0),
            source_mix["sum"].eq(source_mix["count"]),
        ],
        ["PRIMARY_MAJOR_ACCOUNTS_ONLY", "FULL_STATEMENT_ONLY"],
        default="MIXED_MAJOR_AND_FULL_STATEMENT",
    )
    return {
        "count": len(bad),
        "cause": dict(Counter(bad["cause"])),
        "fs_type": dict(Counter(bad["fs_type"])),
        "fiscal_period": dict(Counter(bad["fiscal_period"])),
        "source_mix": dict(Counter(source_mix["source_mix"])),
        "relative_error": _buckets(
            bad["relative_error"], [0.03, 0.10, 0.25, 0.50, 1.0]
        ),
        "largest": (
            bad.sort_values("relative_error", ascending=False)
            .head(10)
            .to_dict("records")
        ),
    }


def analyze_bundle(bundle, fingerprint: str) -> dict:
    """이미 생성된 cutoff 후보를 재사용해 Warning 분석 결과를 만든다."""
    combined = _combined_prices(bundle.prices)
    actions = bundle.stats["_corporate_actions"]
    return {
        "fingerprint": fingerprint,
        "price_adjustment_without_dart": _price_adjustment_analysis(
            combined, actions
        ),
        "dart_action_without_krx": _dart_without_krx_analysis(
            combined,
            actions,
            set(bundle.prices["trade_date"]),
        ),
        "dart_share_count_mismatch": _share_mismatch_analysis(
            combined, actions
        ),
        "price_distribution_drift": _distribution_analysis(bundle.prices),
        "fundamental_accounting_equation": _accounting_analysis(
            bundle.fundamentals
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        help=(
            "existing immutable Bronze cutoff root; when omitted the cutoff "
            "is synchronized from S3 into BACKFILL_DATA_ROOT"
        ),
    )
    parser.add_argument(
        "--domain",
        choices=("prices", "fundamentals", "all"),
        default="all",
    )
    args = parser.parse_args()

    root = Path(
        args.base
        or os.environ.get("BACKFILL_DATA_ROOT", "/app/data")
    )
    if args.base:
        marker = root / ".bronze-input-fingerprint"
        if not marker.exists():
            raise RuntimeError(
                f"immutable Bronze fingerprint marker is missing: {marker}"
            )
        fingerprint = marker.read_text(encoding="utf-8").strip()
    else:
        fingerprint = _sync_cutoff(root)

    report = {"fingerprint": fingerprint}
    if args.domain in {"prices", "all"}:
        report["prices"] = _price_partition_warning_analysis(str(root))
    if args.domain in {"fundamentals", "all"}:
        # Import lazily so a price-only audit does not load the fundamental
        # candidate path or retain both domains in one process.
        from pipeline.silver_quality.s3_domain_audit import (
            _fundamental_bundle,
        )

        bundle = _fundamental_bundle(str(root))
        report["fundamentals"] = {
            "accounting_equation": _accounting_analysis(
                bundle.fundamentals
            ),
        }
    print("WARNING_ANALYSIS " + _json(report), flush=True)


if __name__ == "__main__":
    main()
