"""price_daily 결정적 규칙과 통계적 경고."""
from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.silver.prices import LISTING_EPISODE_GAP_DAYS
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity
from pipeline.silver_quality.reviewed_exceptions import (
    REVIEWED_SETTLEMENT_TRADING_IDENTIFIERS,
)
from pipeline.silver_quality.rules.common import (
    duplicate_keys,
    finite_numbers,
    null_keys,
    result,
)

PRICE_KEYS = ["identifier", "source", "trade_date"]
NUMERIC = [
    "open", "high", "low", "close", "adj_close", "volume",
    "trading_value", "shares", "market_cap",
]


def _attach_corporate_action_evidence(
    frame: pd.DataFrame,
    corporate_actions: pd.DataFrame | None,
) -> pd.DataFrame:
    """KRX 조정계수 행에 종목·효력일 기준 DART 외부 근거를 붙인다."""
    frame = frame.copy()
    frame["dart_event_confirmed"] = False
    frame["dart_event_type"] = None
    frame["dart_rcept_no"] = None
    frame["dart_effective_date"] = None
    frame["dart_expected_factor"] = np.nan
    frame["dart_share_count_factor"] = np.nan
    frame["dart_action_method"] = None
    frame["dart_match_days"] = np.nan
    frame["dart_event_inherited"] = False
    frame["dart_issuer_parent_identifier"] = None
    if corporate_actions is None or corporate_actions.empty:
        return frame

    events = corporate_actions.copy()
    required = {
        "identifier",
        "event_type",
        "effective_date",
        "match_window_days",
        "expects_price_adjustment",
        "rcept_no",
    }
    if not required.issubset(events.columns):
        return frame
    events["identifier"] = events["identifier"].astype(str)
    events["effective_date"] = pd.to_datetime(
        events["effective_date"],
        errors="coerce",
    ).dt.date
    confirmation = (
        events["confirms_price_adjustment"]
        if "confirms_price_adjustment" in events
        else events["expects_price_adjustment"]
    )
    events = events[
        confirmation.fillna(False)
        & events["effective_date"].notna()
    ]
    if events.empty:
        return frame

    adjustment_rows = frame[
        frame["asset_type"].eq("stock")
        & frame["source_adjustment_event"].fillna(False)
    ]
    by_identifier = {
        identifier: group.to_dict("records")
        for identifier, group in events.groupby(
            "identifier",
            sort=False,
        )
    }
    for index, row in adjustment_rows.iterrows():
        matches = []
        for event in by_identifier.get(str(row["identifier"]), []):
            distance = abs((row["trade_date"] - event["effective_date"]).days)
            window = int(event.get("match_window_days") or 0)
            if distance <= window:
                matches.append((
                    distance,
                    0 if pd.notna(event.get("expected_factor")) else 1,
                    str(event.get("rcept_no") or ""),
                    event,
                ))
        if not matches:
            continue
        distance, _, _, event = min(
            matches,
            key=lambda item: (item[0], item[1], item[2]),
        )
        frame.at[index, "dart_event_confirmed"] = True
        frame.at[index, "dart_event_type"] = event.get("event_type")
        frame.at[index, "dart_rcept_no"] = event.get("rcept_no")
        frame.at[index, "dart_effective_date"] = event.get("effective_date")
        frame.at[index, "dart_expected_factor"] = event.get("expected_factor")
        frame.at[index, "dart_share_count_factor"] = event.get(
            "share_count_factor"
        )
        frame.at[index, "dart_action_method"] = event.get("action_method")
        frame.at[index, "dart_match_days"] = distance
        frame.at[index, "dart_event_inherited"] = bool(
            event.get("issuer_event_inherited") or False
        )
        frame.at[index, "dart_issuer_parent_identifier"] = event.get(
            "issuer_parent_identifier"
        )
    return frame


def _attach_special_trading_evidence(
    frame: pd.DataFrame,
    corporate_actions: pd.DataFrame | None,
) -> pd.DataFrame:
    """정리매매·거래재개처럼 큰 실제 수익률을 설명하는 DART 근거를 붙인다."""
    frame = frame.copy()
    frame["dart_special_event_confirmed"] = False
    frame["dart_special_event_type"] = None
    frame["dart_special_rcept_no"] = None
    frame["dart_special_match_days"] = np.nan
    frame["dart_special_event_inherited"] = False
    frame["dart_special_issuer_parent_identifier"] = None
    if corporate_actions is None or corporate_actions.empty:
        return frame
    required = {
        "identifier",
        "event_type",
        "announcement_date",
        "rcept_no",
        "report_name",
    }
    if not required.issubset(corporate_actions.columns):
        return frame
    events = corporate_actions.copy()
    events["identifier"] = events["identifier"].astype(str)
    events["announcement_date"] = pd.to_datetime(
        events["announcement_date"],
        errors="coerce",
    ).dt.date
    title = events["report_name"].astype(str)
    events = events[
        events["announcement_date"].notna()
        & (
            title.str.contains("정리매매", na=False)
            | title.str.contains("거래정지해제", na=False)
            | title.str.contains("상장폐지", na=False)
            | title.str.contains("재상장", na=False)
            | title.str.contains("변경상장", na=False)
        )
    ]
    if events.empty:
        return frame
    by_identifier = {
        identifier: group.to_dict("records")
        for identifier, group in events.groupby("identifier", sort=False)
    }
    candidates = frame[
        frame["asset_type"].eq("stock")
        & frame["economic_return"].abs().gt(0.305)
    ]
    for index, row in candidates.iterrows():
        matches = []
        for event in by_identifier.get(str(row["identifier"]), []):
            distance = (row["trade_date"] - event["announcement_date"]).days
            if 0 <= distance <= 120:
                matches.append((distance, event))
        if not matches:
            continue
        distance, event = min(
            matches,
            key=lambda item: (item[0], str(item[1].get("rcept_no") or "")),
        )
        frame.at[index, "dart_special_event_confirmed"] = True
        frame.at[index, "dart_special_event_type"] = event.get("event_type")
        frame.at[index, "dart_special_rcept_no"] = event.get("rcept_no")
        frame.at[index, "dart_special_match_days"] = distance
        frame.at[index, "dart_special_event_inherited"] = bool(
            event.get("issuer_event_inherited") or False
        )
        frame.at[index, "dart_special_issuer_parent_identifier"] = event.get(
            "issuer_parent_identifier"
        )
    return frame


def _dart_actions_without_krx_adjustment(
    frame: pd.DataFrame,
    corporate_actions: pd.DataFrame | None,
    candidate_dates: set,
) -> pd.DataFrame:
    """효력일 근처 거래는 있지만 KRX 조정계수가 없는 강한 DART 행사를 찾는다."""
    columns = [
        "identifier",
        "event_type",
        "effective_date",
        "rcept_no",
        "nearest_trade_date",
        "source_adjustment_factor",
    ]
    if (
        corporate_actions is None
        or corporate_actions.empty
        or not candidate_dates
    ):
        return pd.DataFrame(columns=columns)
    events = corporate_actions.copy()
    required = {
        "identifier",
        "event_type",
        "effective_date",
        "match_window_days",
        "expects_price_adjustment",
        "rcept_no",
    }
    if not required.issubset(events.columns):
        return pd.DataFrame(columns=columns)
    events["identifier"] = events["identifier"].astype(str)
    events["effective_date"] = pd.to_datetime(
        events["effective_date"],
        errors="coerce",
    ).dt.date
    events = events[
        events["expects_price_adjustment"].fillna(False)
        & events["effective_date"].notna()
    ].drop_duplicates(["identifier", "event_type", "effective_date"])
    if "issuer_event_inherited" in events:
        events = events[
            ~events["issuer_event_inherited"].fillna(False)
        ]
    stock = frame[frame["asset_type"].eq("stock")]
    by_identifier = {
        identifier: group
        for identifier, group in stock.groupby("identifier", sort=False)
    }
    missing: list[dict] = []
    for event in events.to_dict("records"):
        group = by_identifier.get(event["identifier"])
        if group is None or group.empty:
            continue
        window = int(event.get("match_window_days") or 0)
        distances = group["trade_date"].map(
            lambda value: abs((value - event["effective_date"]).days)
        )
        in_window = group.loc[distances.le(window)]
        if in_window.empty:
            continue
        # 효력일이 휴일이면 전·후 거래일의 달력상 거리가 같을 수 있다.
        # 가장 가까운 한 행만 검사하면 다음 거래일에 실제 반영된 KRX
        # 조정계수를 놓치므로, 허용 창 전체에 조정 행이 하나라도 있는지 본다.
        if in_window["source_adjustment_event"].fillna(False).any():
            continue
        nearest_index = distances.loc[in_window.index].idxmin()
        nearest = group.loc[nearest_index]
        if nearest["trade_date"] not in candidate_dates:
            continue
        if pd.isna(nearest["source_adjustment_factor"]):
            continue
        missing.append({
            "identifier": event["identifier"],
            "event_type": event["event_type"],
            "effective_date": event["effective_date"],
            "rcept_no": event["rcept_no"],
            "nearest_trade_date": nearest["trade_date"],
            "source_adjustment_factor": nearest["source_adjustment_factor"],
        })
    return pd.DataFrame(missing, columns=columns)


def _dart_share_count_factor_results(
    frame: pd.DataFrame,
    corporate_actions: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """균등 감자의 DART 비율을 효력일 주변 실제 KRX 주식수 변화와 대사한다."""
    output_columns = [
        "identifier",
        "effective_date",
        "rcept_no",
        "action_method",
        "dart_share_count_factor",
        "krx_share_count_factor",
        "share_factor_relative_error",
        "observed_trade_date",
        "reason",
    ]
    empty = pd.DataFrame(columns=output_columns)
    if corporate_actions is None or corporate_actions.empty:
        return empty, empty
    required = {
        "identifier",
        "event_type",
        "effective_date",
        "match_window_days",
        "share_count_factor",
        "share_count_factor_comparable",
        "rcept_no",
    }
    if not required.issubset(corporate_actions.columns):
        return empty, empty

    events = corporate_actions.copy()
    events["identifier"] = events["identifier"].astype(str)
    events["effective_date"] = pd.to_datetime(
        events["effective_date"],
        errors="coerce",
    ).dt.date
    events["share_count_factor"] = pd.to_numeric(
        events["share_count_factor"],
        errors="coerce",
    )
    events = events[
        events["event_type"].eq("capital_reduction")
        & events["effective_date"].notna()
        & events["share_count_factor"].gt(0)
    ]
    if "issuer_event_inherited" in events:
        events = events[~events["issuer_event_inherited"].fillna(False)]
    # 정정공시는 같은 효력일의 최신 접수번호를 최종 근거로 사용한다.
    events = (
        events.sort_values("rcept_no")
        .drop_duplicates(["identifier", "effective_date"], keep="last")
    )

    stock = frame[frame["asset_type"].eq("stock")].copy()
    by_identifier = {
        identifier: group.sort_values("trade_date")
        for identifier, group in stock.groupby("identifier", sort=False)
    }
    mismatches: list[dict] = []
    excluded: list[dict] = []
    for event in events.to_dict("records"):
        base = {
            "identifier": event["identifier"],
            "effective_date": event["effective_date"],
            "rcept_no": event.get("rcept_no"),
            "action_method": event.get("action_method"),
            "dart_share_count_factor": event["share_count_factor"],
        }
        if not bool(event.get("share_count_factor_comparable")):
            excluded.append({
                **base,
                "reason": "non-uniform or non-share-count-comparable reduction",
            })
            continue
        group = by_identifier.get(event["identifier"])
        if group is None or group.empty:
            continue
        window = int(event.get("match_window_days") or 0)
        distances = group["trade_date"].map(
            lambda value: abs((value - event["effective_date"]).days)
        )
        nearby = group.loc[distances.le(window)].copy()
        nearby = nearby[
            nearby["previous_shares"].gt(0)
            & nearby["shares"].gt(0)
        ]
        if nearby.empty:
            excluded.append({
                **base,
                "reason": "no comparable KRX listed-share observations in window",
            })
            continue

        # 일별 변경과 허용 창 전체의 누적 변경을 모두 후보로 본다.
        observed: list[tuple[float, object]] = [
            (
                float(row.previous_shares) / float(row.shares),
                row.trade_date,
            )
            for row in nearby.itertuples()
        ]
        first = nearby.iloc[0]
        last = nearby.iloc[-1]
        observed.append((
            float(first["previous_shares"]) / float(last["shares"]),
            last["trade_date"],
        ))
        expected = float(event["share_count_factor"])
        actual, observed_date = min(
            observed,
            key=lambda item: abs(item[0] - expected) / expected,
        )
        relative_error = abs(actual - expected) / expected
        if relative_error > 0.02:
            mismatches.append({
                **base,
                "krx_share_count_factor": actual,
                "share_factor_relative_error": relative_error,
                "observed_trade_date": observed_date,
                "reason": "DART ratio differs from nearby KRX listed-share change",
            })
    return (
        pd.DataFrame(mismatches, columns=output_columns),
        pd.DataFrame(excluded, columns=output_columns),
    )


def check_prices(
    prices: pd.DataFrame,
    target_date=None,
    history: pd.DataFrame | None = None,
    corporate_actions: pd.DataFrame | None = None,
    partition_key: str | None = None,
) -> list[CheckResult]:
    checks = [
        null_keys(prices, "price_daily", PRICE_KEYS, partition_key),
        duplicate_keys(prices, "price_daily", PRICE_KEYS, partition_key),
        finite_numbers(prices, "price_daily", NUMERIC, partition_key),
    ]
    if prices.empty:
        return checks

    if target_date is not None:
        bad_date = prices[prices["trade_date"] != target_date]
        checks.append(result(
            "PRICE_TARGET_DATE", "price_daily", Severity.CRITICAL, bad_date,
            f"trade_date={target_date}", partition_key=partition_key,
        ))

    if "source_file" in prices:
        partition_dates = prices["source_file"].astype(str).str.extract(
            r"date=(\d{4}-\d{2}-\d{2})", expand=False,
        )
        parsed_partition_dates = pd.to_datetime(partition_dates, errors="coerce").dt.date
        source_date_bad = prices[
            parsed_partition_dates.notna()
            & (parsed_partition_dates != prices["trade_date"])
        ]
    else:
        source_date_bad = prices.iloc[0:0]
    checks.append(result(
        "PRICE_SOURCE_PARTITION_DATE", "price_daily", Severity.ERROR,
        source_date_bad, "source date partition equals trade_date",
        partition_key=partition_key,
    ))

    stock = prices["asset_type"].eq("stock")
    index = prices["asset_type"].eq("index")
    required_positive = prices[
        prices["close"].isna() | (prices["close"] <= 0)
        | prices["adj_close"].isna() | (prices["adj_close"] <= 0)
        | (
            stock
            & (prices["market_cap"].isna() | (prices["market_cap"] <= 0))
        )
    ]
    checks.append(result(
        "PRICE_REQUIRED_POSITIVE", "price_daily", Severity.ERROR, required_positive,
        "stock: close/adj_close/market_cap > 0; index: close/adj_close > 0",
        partition_key=partition_key,
    ))

    negatives = prices[
        (prices[["volume", "trading_value", "market_cap"]].fillna(0) < 0).any(axis=1)
        | ((prices["asset_type"] == "stock") & (prices["shares"].fillna(0) <= 0))
    ]
    checks.append(result(
        "PRICE_NON_NEGATIVE", "price_daily", Severity.ERROR, negatives,
        "volume/trading_value/market_cap >= 0 and stock shares > 0",
        partition_key=partition_key,
    ))

    ohl = prices[["open", "high", "low"]]
    missing_ohl = ohl.isna() | ohl.eq(0)
    incomplete_ohlc = missing_ohl.all(axis=1) & prices["close"].fillna(0).gt(0)
    no_trade_ohlc = (
        incomplete_ohlc
        & prices["volume"].fillna(0).eq(0)
        & prices["trading_value"].fillna(0).eq(0)
    )
    active_incomplete_ohlc = incomplete_ohlc & ~no_trade_ohlc
    partial_missing = missing_ohl.any(axis=1) & ~missing_ohl.all(axis=1)
    complete = ~missing_ohl.any(axis=1)
    logical = complete & (
        (prices["high"] < prices[["open", "low", "close"]].max(axis=1))
        | (prices["low"] > prices[["open", "high", "close"]].min(axis=1))
    )
    checks.append(result(
        "PRICE_OHLC_LOGIC", "price_daily", Severity.ERROR,
        prices[partial_missing | logical],
        "complete valid OHLC, or all O/H/L absent with close preserved",
        partition_key=partition_key,
    ))
    checks.append(result(
        "SOURCE_INCOMPLETE_OHLC", "price_daily", Severity.WARNING,
        prices[active_incomplete_ohlc],
        "traded row has no usable O/H/L; values remain NULL without fabrication",
        partition_key=partition_key,
    ))
    no_trade_samples = (
        prices[no_trade_ohlc].head(20)
        .astype(object).where(pd.notna(prices[no_trade_ohlc].head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="SOURCE_NO_TRADE_OHLC",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected="no-trade rows may preserve close while O/H/L remain NULL",
        actual=f"observed_rows={int(no_trade_ohlc.sum())}",
        failed_count=0,
        samples=no_trade_samples,
        partition_key=partition_key,
    ))

    market_bad = prices[
        (stock & ~prices["market"].isin(["KOSPI", "KOSDAQ", "KONEX"]))
        | (index & prices["market"].notna())
        | (index & prices["shares"].notna())
    ]
    checks.append(result(
        "PRICE_ASSET_SHAPE", "price_daily", Severity.ERROR, market_bad,
        "stock market enum; index market/shares null", partition_key=partition_key,
    ))

    stock_markets = (
        prices[stock]
        .groupby("trade_date")["market"]
        .agg(lambda values: set(values.dropna()))
    )
    missing_market_dates = stock_markets[
        ~stock_markets.apply(lambda values: {"KOSPI", "KOSDAQ"}.issubset(values))
    ]
    checks.append(result(
        "PRICE_MARKET_COMPLETENESS", "price_daily", Severity.CRITICAL,
        len(missing_market_dates),
        "every trade_date includes KOSPI and KOSDAQ stocks",
        actual=f"bad_dates={list(missing_market_dates.index[:20])}",
        partition_key=partition_key,
    ))

    expected_cap = prices["close"] * prices["shares"]
    cap_diff = (prices["market_cap"] - expected_cap).abs() / expected_cap.replace(0, np.nan)
    cap_bad = prices[stock & expected_cap.notna() & cap_diff.gt(0.01)]
    checks.append(result(
        "PRICE_MARKET_CAP_RECONCILIATION", "price_daily", Severity.ERROR, cap_bad,
        "abs(market_cap-close*shares)/(close*shares) <= 1%",
        partition_key=partition_key,
    ))

    expected_benchmarks = {"1028", "2203"}
    benchmark_rows = prices[index].copy()
    benchmark_rows["identifier"] = benchmark_rows["identifier"].astype(str)
    benchmark_bad_dates: list = []
    for trade_date, group in benchmark_rows.groupby("trade_date"):
        identifiers = group["identifier"]
        if set(identifiers) != expected_benchmarks or identifiers.duplicated().any():
            benchmark_bad_dates.append(trade_date)
    all_dates = set(prices["trade_date"])
    seen_dates = set(benchmark_rows["trade_date"])
    benchmark_bad_dates.extend(sorted(all_dates - seen_dates))
    checks.append(result(
        "PRICE_BENCHMARK_COMPLETENESS", "price_daily", Severity.CRITICAL,
        len(set(benchmark_bad_dates)),
        "every trade_date has exactly one KOSPI200(1028) and KOSDAQ150(2203)",
        actual=f"bad_dates={sorted(set(benchmark_bad_dates))[:20]}",
        partition_key=partition_key,
    ))

    if {"prev_diff", "fluc_rate"}.issubset(prices.columns):
        adjustment_source_bad = prices[
            prices["prev_diff"].isna() | prices["fluc_rate"].isna()
        ]
        checks.append(result(
            "ADJ_CLOSE_SOURCE_FIELDS",
            "price_daily",
            Severity.ERROR,
            adjustment_source_bad,
            "prev_diff and fluc_rate are present for adjusted-close validation",
            partition_key=partition_key,
        ))
        previous = prices["close"] - prices["prev_diff"]
        calculated = prices["prev_diff"] / previous.replace(0, np.nan) * 100
        arithmetic_bad = prices[
            previous.le(0)
            | (calculated - prices["fluc_rate"]).abs().gt(0.05)
        ]
        checks.append(result(
            "PRICE_KRX_ARITHMETIC", "price_daily", Severity.ERROR, arithmetic_bad,
            "KRX close/prev_diff/fluc_rate agree within 0.05%p",
            partition_key=partition_key,
        ))
    else:
        checks.append(result(
            "ADJ_CLOSE_SOURCE_FIELDS",
            "price_daily",
            Severity.ERROR,
            len(prices),
            "prev_diff and fluc_rate columns are present",
            actual="missing required adjusted-close source columns",
            partition_key=partition_key,
        ))

    series_columns = [
        "identifier", "trade_date", "close", "adj_close", "market", "asset_type",
        "prev_diff", "shares", "market_cap",
    ]
    combined = prices[series_columns].copy()
    if history is not None and not history.empty:
        historic = history.copy()
        if "adj_close" not in historic:
            historic["adj_close"] = historic["close"]
        if "market" not in historic:
            historic["market"] = "UNKNOWN"
        if "asset_type" not in historic:
            historic["asset_type"] = "stock"
        if "prev_diff" not in historic:
            historic["prev_diff"] = np.nan
        if "shares" not in historic:
            historic["shares"] = np.nan
        if "market_cap" not in historic:
            historic["market_cap"] = np.nan
        combined = pd.concat([historic[series_columns], combined], ignore_index=True)
    for column in ("close", "adj_close", "prev_diff", "shares", "market_cap"):
        combined[column] = pd.to_numeric(combined[column], errors="coerce")
    combined = combined.sort_values(["identifier", "trade_date"])
    unsegmented_previous_date = (
        combined.groupby("identifier")["trade_date"].shift(1)
    )
    combined["listing_gap_days"] = (
        pd.to_datetime(combined["trade_date"])
        - pd.to_datetime(unsegmented_previous_date)
    ).dt.days
    combined["listing_episode_boundary"] = combined[
        "listing_gap_days"
    ].gt(LISTING_EPISODE_GAP_DAYS)
    combined["listing_episode"] = (
        combined["listing_episode_boundary"]
        .groupby(combined["identifier"])
        .cumsum()
    )
    series_keys = [combined["identifier"], combined["listing_episode"]]
    series = combined.groupby(series_keys)
    combined["previous_close"] = series["close"].shift(1)
    combined["previous_shares"] = series["shares"].shift(1)
    combined["previous_market_cap"] = (
        series["market_cap"].shift(1)
    )
    combined["lag2_close"] = series["close"].shift(2)
    combined["return"] = combined["close"] / combined["previous_close"] - 1
    combined["previous_return"] = series["return"].shift(1)
    combined["source_reference"] = combined["close"] - combined["prev_diff"]
    combined["source_adjustment_factor"] = (
        combined["source_reference"]
        / combined["previous_close"].replace(0, np.nan)
    )
    # 수정종가 계산은 유효한 원천 조정계수를 크기와 관계없이 모두 적용한다.
    # 0.5% 기준은 사람이 검토할 기업행사 이벤트를 추리는 기준일 뿐,
    # 계산에서 작은 계수를 버리는 허용오차가 아니다.
    combined["source_factor_applied"] = (
        combined["previous_close"].gt(0)
        & combined["source_reference"].gt(0)
        & combined["source_adjustment_factor"].sub(1).abs().ge(1e-9)
    )
    combined["source_adjustment_event"] = (
        combined["source_adjustment_factor"].sub(1).abs().gt(0.005)
    )
    combined["identifier"] = combined["identifier"].astype(str)
    combined = _attach_corporate_action_evidence(
        combined,
        corporate_actions,
    )
    adjusted_previous = combined["previous_close"] * (
        combined["source_adjustment_factor"]
        .where(combined["source_factor_applied"], 1.0)
    )
    combined["economic_return"] = (
        combined["close"] / adjusted_previous.replace(0, np.nan) - 1
    )
    combined = _attach_special_trading_evidence(
        combined,
        corporate_actions,
    )
    combined["previous_economic_return"] = (
        combined.groupby(series_keys)["economic_return"].shift(1)
    )
    combined["previous_source_adjustment_event"] = (
        combined.groupby(series_keys)["source_adjustment_event"]
        .shift(1)
        .eq(True)
    )
    combined["previous_dart_event_confirmed"] = (
        combined.groupby(series_keys)["dart_event_confirmed"]
        .shift(1)
        .eq(True)
    )
    combined["previous_dart_special_event_confirmed"] = (
        combined.groupby(series_keys)["dart_special_event_confirmed"]
        .shift(1)
        .eq(True)
    )
    combined["adjustment_factor"] = combined["adj_close"] / combined["close"]
    combined["previous_factor"] = (
        combined.groupby(series_keys)["adjustment_factor"].shift(1)
    )
    combined["previous_adj_close"] = (
        combined.groupby(series_keys)["adj_close"].shift(1)
    )
    current_index = pd.MultiIndex.from_frame(
        prices.assign(identifier=prices["identifier"].astype(str))[
            ["identifier", "trade_date"]
        ]
    )
    combined_index = pd.MultiIndex.from_frame(
        combined[["identifier", "trade_date"]]
    )
    current = combined[combined_index.isin(current_index)]
    episode_boundaries = current[current["listing_episode_boundary"]]
    episode_samples = (
        episode_boundaries.head(20).astype(object)
        .where(pd.notna(episode_boundaries.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="LISTING_EPISODE_BOUNDARY",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected=(
            f"price adjustment and return chains reset after >"
            f"{LISTING_EPISODE_GAP_DAYS}-day ticker absence"
        ),
        actual=f"observed_boundaries={len(episode_boundaries)}",
        failed_count=0,
        samples=episode_samples,
        partition_key=partition_key,
    ))

    # 전체 backfill에서는 변환 코드와 독립적으로 수정계수를 다시 누적해
    # 저장 후보 adj_close를 4자리 정밀도로 대사한다. 연도/일자 파티션은
    # 미래 기업행사가 잘려 절대 scale을 알 수 없으므로 전기간 검사에서만 실행한다.
    is_full_series = target_date is None and partition_key is None
    if is_full_series:
        valid_reference = (
            combined["previous_close"].gt(0)
            & combined["source_reference"].gt(0)
        )
        recomputed_event_factor = combined["source_adjustment_factor"].where(
            valid_reference,
            1.0,
        )
        recomputed_event_factor = recomputed_event_factor.where(
            recomputed_event_factor.sub(1).abs().ge(1e-9),
            1.0,
        )
        cumulative_factor = recomputed_event_factor.groupby(
            series_keys,
        ).cumprod()
        final_factor = cumulative_factor.groupby(series_keys).transform("last")
        combined["expected_adj_close"] = (
            combined["close"] * final_factor / cumulative_factor
        ).round(4)
        reconciliation_error = (
            combined["adj_close"] - combined["expected_adj_close"]
        ).abs()
        reconciliation_bad = combined[
            combined["expected_adj_close"].notna()
            & reconciliation_error.gt(0.00011)
        ]
    else:
        reconciliation_bad = combined.iloc[0:0]
    checks.append(result(
        "ADJ_CLOSE_RECONCILIATION",
        "price_daily",
        Severity.ERROR,
        reconciliation_bad,
        "adj_close equals independent full-series recomputation to 4 decimals",
        partition_key=partition_key,
    ))

    # 전기간 후보는 직전 adj_close를 그대로 사용한다. 일별 후보는 publish
    # 과정에서 당일 기업행사 계수만큼 과거 adj_close가 소급 조정될 예정이므로
    # 그 pending 계수를 직전 값에 먼저 반영해 연속성을 검사한다.
    pending_factor = pd.Series(1.0, index=combined.index)
    if target_date is not None and history is not None:
        pending_factor = combined["source_adjustment_factor"].where(
            combined["source_factor_applied"],
            1.0,
        )
    expected_from_previous = (
        combined["previous_adj_close"]
        * pending_factor
        * (1 + combined["economic_return"])
    )
    continuity_tolerance = np.maximum(
        0.0002,
        expected_from_previous.abs() * 1e-7,
    )
    continuity_error = (
        combined["adj_close"] - expected_from_previous
    ).abs()
    continuity_bad = current[
        current["previous_adj_close"].notna()
        & expected_from_previous.loc[current.index].notna()
        & continuity_error.loc[current.index].gt(
            continuity_tolerance.loc[current.index]
        )
    ]
    checks.append(result(
        "ADJ_CLOSE_RETURN_CONTINUITY",
        "price_daily",
        Severity.ERROR,
        continuity_bad,
        "adj_close return matches KRX corporate-action-adjusted return",
        partition_key=partition_key,
    ))

    all_spikes = current[current["economic_return"].abs().gt(0.305)]
    special_spikes = all_spikes[
        all_spikes["dart_special_event_confirmed"]
    ]
    # 전수 검토로 정리매매가 확인된 종목만 전체 cutoff보다 먼저 종료된
    # 시계열의 마지막 7거래일에서 Explained로 분류한다. 활성 종목이나
    # 같은 ticker의 과거 일반 급변까지 넓게 예외 처리하지 않는다.
    stock_cutoff = combined.loc[
        combined["asset_type"].eq("stock"), "trade_date"
    ].max()
    episode_last_date = combined.groupby(series_keys)["trade_date"].transform(
        "max"
    )
    reverse_trade_number = combined.iloc[::-1].groupby(
        [
            combined["identifier"].iloc[::-1],
            combined["listing_episode"].iloc[::-1],
        ],
        sort=False,
    ).cumcount().iloc[::-1]
    reviewed_settlement = (
        combined["identifier"].isin(REVIEWED_SETTLEMENT_TRADING_IDENTIFIERS)
        & episode_last_date.lt(stock_cutoff)
        & reverse_trade_number.lt(7)
    )
    settlement_spikes = all_spikes[
        reviewed_settlement.loc[all_spikes.index]
        & ~all_spikes["dart_special_event_confirmed"]
    ]
    spike = all_spikes[
        ~all_spikes["dart_special_event_confirmed"]
        & ~reviewed_settlement.loc[all_spikes.index]
    ]
    checks.append(result(
        "PRICE_RETURN_SPIKE", "price_daily", Severity.WARNING, spike,
        "absolute corporate-action-adjusted return <= 30.5%",
        partition_key=partition_key,
    ))
    round_trip = current[
        current["previous_economic_return"].abs().gt(0.305)
        & ~current["dart_event_confirmed"]
        & ~current["previous_dart_event_confirmed"]
        & ~current["dart_special_event_confirmed"]
        & ~current["previous_dart_special_event_confirmed"]
        & ~reviewed_settlement.loc[current.index]
        & ((current["close"] / current["lag2_close"] - 1).abs().le(0.05))
    ]
    checks.append(result(
        "PRICE_ROUND_TRIP_SPIKE", "price_daily", Severity.WARNING, round_trip,
        "no >30.5% spike followed by return within 5% of the prior level",
        partition_key=partition_key,
    ))
    ratios = current["close"] / current["previous_close"].replace(0, np.nan)
    scale_mask = pd.Series(False, index=current.index)
    for target in (0.01, 0.1, 10.0, 100.0):
        scale_mask |= ((ratios - target).abs() / target).le(0.005)
    share_ratios = (
        current["shares"]
        / current["previous_shares"].replace(0, np.nan)
    )
    market_cap_ratios = (
        current["market_cap"]
        / current["previous_market_cap"].replace(0, np.nan)
    )
    krx_structure_confirmed = (
        current["source_adjustment_event"]
        & ratios.notna()
        & share_ratios.notna()
        & market_cap_ratios.notna()
        & (ratios * share_ratios).sub(1).abs().le(0.005)
        & market_cap_ratios.sub(1).abs().le(0.02)
    )
    inferred_structure = current[
        krx_structure_confirmed
        & ~current["dart_event_confirmed"]
    ]
    inferred_samples = (
        inferred_structure.head(20).astype(object)
        .where(pd.notna(inferred_structure.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="CORPORATE_ACTION_INFERRED_FROM_KRX_STRUCTURE",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected=(
            "10x/100x price and reciprocal share-count changes preserve "
            "market capitalization"
        ),
        actual=f"observed_events={len(inferred_structure)}",
        failed_count=0,
        samples=inferred_samples,
        partition_key=partition_key,
    ))
    special_samples = (
        special_spikes.head(20).astype(object)
        .where(pd.notna(special_spikes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="SETTLEMENT_TRADING_PRICE_SPIKE",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected=(
            "reviewed delisting series spike occurs only within its final "
            "7 unrestricted settlement-trading sessions"
        ),
        actual=f"explained_events={len(settlement_spikes)}",
        failed_count=0,
        samples=(
            settlement_spikes.head(20).astype(object)
            .where(pd.notna(settlement_spikes.head(20)), None)
            .to_dict("records")
        ),
        partition_key=partition_key,
    ))
    checks.append(CheckResult(
        rule_code="SPECIAL_TRADING_EVENT",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected=(
            "DART delisting/resumption evidence explains an actual "
            ">30.5% price move"
        ),
        actual=f"observed_events={len(special_spikes)}",
        failed_count=0,
        samples=special_samples,
        partition_key=partition_key,
    ))
    scale = current[
        scale_mask
        & ~current["dart_event_confirmed"]
        & ~krx_structure_confirmed
        & ~current["dart_special_event_confirmed"]
    ]
    checks.append(result(
        "PRICE_SCALE_JUMP", "price_daily", Severity.WARNING, scale,
        "not a near 10x/100x unit-scale change", partition_key=partition_key,
    ))

    corporate_action = current[current["source_adjustment_event"]]
    unconfirmed_action = corporate_action[
        ~corporate_action["dart_event_confirmed"]
        & ~krx_structure_confirmed.loc[corporate_action.index]
    ]
    checks.append(result(
        "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT",
        "price_daily",
        Severity.WARNING,
        unconfirmed_action,
        "every >0.5% KRX reference-price adjustment has nearby DART evidence",
        partition_key=partition_key,
    ))

    expected_factor = pd.to_numeric(
        corporate_action["dart_expected_factor"],
        errors="coerce",
    )
    factor_relative_error = (
        corporate_action["source_adjustment_factor"] - expected_factor
    ).abs() / expected_factor.replace(0, np.nan)
    factor_mismatch = corporate_action[
        corporate_action["dart_event_confirmed"]
        & corporate_action["dart_match_days"].eq(0)
        & expected_factor.notna()
        & factor_relative_error.gt(0.02)
    ]
    checks.append(result(
        "CORPORATE_ACTION_FACTOR_MISMATCH",
        "price_daily",
        Severity.WARNING,
        factor_mismatch,
        "KRX adjustment factor agrees with calculable DART factor within 2%",
        partition_key=partition_key,
    ))

    # DART 감자 전/후 발행주식 수는 가격계수가 아니다. 균등병합만 효력일
    # 주변 전체 KRX 상장주식 수 변화와 비교하고 나머지는 Explained로 남긴다.
    share_factor_mismatch, non_comparable_reductions = (
        _dart_share_count_factor_results(combined, corporate_actions)
    )
    checks.append(result(
        "DART_SHARE_COUNT_FACTOR_MISMATCH",
        "price_daily",
        Severity.WARNING,
        share_factor_mismatch,
        (
            "DART capital-reduction before/after share ratio agrees with "
            "actual KRX listed-share change within 2%"
        ),
        partition_key=partition_key,
    ))
    checks.append(CheckResult(
        rule_code="DART_SHARE_COUNT_FACTOR_NOT_COMPARABLE",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected=(
            "non-uniform, par-value-only, simultaneous-split, or "
            "unobservable reductions are excluded from share-factor comparison"
        ),
        actual=f"explained_events={len(non_comparable_reductions)}",
        failed_count=0,
        samples=(
            non_comparable_reductions.head(20).astype(object)
            .where(pd.notna(non_comparable_reductions.head(20)), None)
            .to_dict("records")
        ),
        partition_key=partition_key,
    ))

    missing_krx_adjustment = _dart_actions_without_krx_adjustment(
        combined,
        corporate_actions,
        set(prices["trade_date"]),
    )
    checks.append(result(
        "DART_ACTION_WITHOUT_KRX_ADJUSTMENT",
        "price_daily",
        Severity.WARNING,
        missing_krx_adjustment,
        "price-adjusting DART event has a nearby KRX reference-price adjustment",
        partition_key=partition_key,
    ))

    corporate_action_samples = (
        corporate_action.head(20).astype(object)
        .where(pd.notna(corporate_action.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="PRICE_ADJUSTMENT_FACTOR_CHANGE",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected="KRX reference-price adjustment is reconciled to DART evidence",
        actual=(
            f"observed_events={len(corporate_action)}, "
            f"dart_confirmed={int(corporate_action['dart_event_confirmed'].sum())}, "
            f"dart_unconfirmed={len(unconfirmed_action)}"
        ),
        failed_count=0,
        samples=corporate_action_samples,
        partition_key=partition_key,
    ))

    candidate_dates = set(prices["trade_date"])
    stock_series = combined[combined["asset_type"].eq("stock")]
    coverage = (
        stock_series.groupby(["trade_date", "market"])["identifier"]
        .nunique()
        .rename("instrument_count")
        .reset_index()
        .sort_values(["market", "trade_date"])
    )
    coverage["baseline"] = coverage.groupby("market")["instrument_count"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).median()
    )
    coverage["deviation"] = (
        (coverage["instrument_count"] - coverage["baseline"])
        / coverage["baseline"].replace(0, np.nan)
    )
    coverage_bad = coverage[
        coverage["trade_date"].isin(candidate_dates)
        & coverage["deviation"].lt(-0.10)
    ]
    checks.append(result(
        "PRICE_COVERAGE_DRIFT", "price_daily", Severity.WARNING, coverage_bad,
        "market instrument count does not fall >10% below previous 20-day median",
        partition_key=partition_key,
    ))
    coverage_growth = coverage[
        coverage["trade_date"].isin(candidate_dates)
        & coverage["deviation"].gt(0.10)
    ]
    growth_samples = (
        coverage_growth.head(20).astype(object)
        .where(pd.notna(coverage_growth.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="PRICE_COVERAGE_GROWTH",
        dataset="price_daily",
        severity=Severity.INFO,
        status=CheckStatus.PASS,
        expected="coverage increases are recorded separately from data-loss risk",
        actual=f"observed_dates={len(coverage_growth)}",
        failed_count=0,
        samples=growth_samples,
        partition_key=partition_key,
    ))

    daily_return = (
        combined.groupby("trade_date")["return"].median()
        .rename("median_return")
        .reset_index()
        .sort_values("trade_date")
    )
    daily_return["baseline"] = daily_return["median_return"].shift(1).rolling(
        20, min_periods=20,
    ).median()
    absolute_deviation = (
        daily_return["median_return"] - daily_return["baseline"]
    ).abs()
    daily_return["mad"] = absolute_deviation.shift(1).rolling(
        20, min_periods=20,
    ).median()
    threshold = np.maximum(0.05, daily_return["mad"].fillna(0) * 5)
    distribution_bad = daily_return[
        daily_return["trade_date"].isin(candidate_dates)
        & absolute_deviation.gt(threshold)
        & daily_return["baseline"].notna()
    ]
    checks.append(result(
        "PRICE_DISTRIBUTION_DRIFT", "price_daily", Severity.WARNING,
        distribution_bad,
        "cross-sectional median return within max(5%, 5×rolling MAD)",
        partition_key=partition_key,
    ))
    return checks
