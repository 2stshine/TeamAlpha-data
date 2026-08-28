"""Deterministic KRX gross total-return reconstruction.

The functions in this module are intentionally database-free.  They turn
certified Silver price/action candidates into auditable resolved dividend
events and a gross (pre-tax) total-return close.  Callers own persistence and
transaction boundaries.

Contract
--------
* one canonical cash-dividend decision per security and record date;
* an explicit ex-dividend date wins, otherwise the second most recent market
  session on or before the record date is used (KRX T+2 convention);
* a non-trading ex-date is applied on the security's first subsequent trade;
* events never cross a listing-episode gap;
* cash is converted to the terminal split-adjusted price scale before use;
* each listing episode starts at ``adj_close`` and compounds gross returns as
  ``(adj_close[t] + adjusted_cash[t]) / adj_close[t-1]``.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from pipeline.silver.prices import LISTING_EPISODE_GAP_DAYS


def _column(frame: pd.DataFrame, *names: str) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _dates(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").dt.normalize()


def classify_cash_dividend_revisions(actions: pd.DataFrame) -> pd.DataFrame:
    """Classify every cash-action row for append-only resolution auditing.

    The returned rows retain their source action identity and add
    ``revision_group_key``, ``is_canonical`` and ``excluded_reason``.  This is
    the shape needed to persist both the selected event and every superseded
    or incomplete source action in ``dividend_event_resolution``.
    """
    if actions.empty:
        result = actions.copy()
        result["revision_group_key"] = pd.Series(dtype="object")
        result["is_canonical"] = pd.Series(dtype="bool")
        result["excluded_reason"] = pd.Series(dtype="object")
        result["dividend_key"] = pd.Series(dtype="object")
        return result

    action_type = _column(actions, "event_type", "action_type")
    if action_type is None or "identifier" not in actions:
        raise ValueError("actions require identifier and event_type/action_type")
    required = {"record_date", "cash_amount"}
    missing = required - set(actions.columns)
    if missing:
        raise ValueError(f"cash-dividend actions missing columns: {sorted(missing)}")

    frame = actions[actions[action_type].eq("cash_dividend")].copy()
    frame["identifier"] = frame["identifier"].astype(str)
    frame["record_date"] = _dates(frame["record_date"])
    frame["cash_amount"] = pd.to_numeric(
        frame["cash_amount"], errors="coerce",
    )
    announcement = _column(frame, "announcement_date")
    frame["_revision_announcement"] = (
        _dates(frame[announcement]) if announcement is not None else pd.NaT
    )
    action_key = _column(frame, "rcept_no", "action_key", "filing_id")
    frame["dividend_key"] = (
        frame[action_key].fillna("").astype(str)
        if action_key is not None
        else ""
    )
    missing_key = frame["dividend_key"].eq("")
    frame.loc[missing_key, "dividend_key"] = [
        f"source-row:{index}" for index in frame.index[missing_key]
    ]
    rendered_record = frame["record_date"].dt.strftime("%Y-%m-%d")
    frame["revision_group_key"] = (
        frame["identifier"] + ":" + rendered_record.fillna("UNRESOLVED")
    )
    frame["is_canonical"] = False
    frame["excluded_reason"] = None
    missing_record = frame["record_date"].isna()
    invalid_cash = frame["cash_amount"].isna() | frame["cash_amount"].le(0)
    frame.loc[missing_record, "excluded_reason"] = "MISSING_RECORD_DATE"
    frame.loc[
        ~missing_record & invalid_cash,
        "excluded_reason",
    ] = "INVALID_CASH_AMOUNT"

    eligible = frame[~missing_record & ~invalid_cash].sort_values(
        [
            "identifier",
            "record_date",
            "_revision_announcement",
            "dividend_key",
        ],
        kind="mergesort",
        na_position="first",
    )
    if not eligible.empty:
        canonical_indices = eligible.drop_duplicates(
            ["identifier", "record_date"], keep="last",
        ).index
        superseded_indices = eligible.index.difference(canonical_indices)
        frame.loc[canonical_indices, "is_canonical"] = True
        frame.loc[superseded_indices, "excluded_reason"] = (
            "SUPERSEDED_REVISION"
        )
    return frame.drop(columns="_revision_announcement").reset_index(drop=True)


def canonicalize_cash_dividends(actions: pd.DataFrame) -> pd.DataFrame:
    """Select the last valid DART decision for each security/record date.

    DART keeps corrections as new receipt numbers.  Once subsidiary filings
    have been excluded by the parser, ``identifier + record_date`` is the
    economic event key and the latest announcement/receipt is its canonical
    revision.  Invalid or incomplete rows stay in Bronze and are counted in
    ``DataFrame.attrs`` instead of being interpreted as zero dividends.
    """
    classified = classify_cash_dividend_revisions(actions)
    canonical = classified[classified["is_canonical"]].copy().reset_index(
        drop=True,
    )
    superseded = classified["excluded_reason"].eq("SUPERSEDED_REVISION").sum()
    rejected = (~classified["is_canonical"]).sum() - superseded
    canonical.attrs["canonicalization"] = {
        "input_rows": len(classified),
        "eligible_rows": int(classified["excluded_reason"].isin([
            None, "SUPERSEDED_REVISION",
        ]).sum()),
        "canonical_rows": len(canonical),
        "superseded_rows": int(superseded),
        "rejected_rows": int(rejected),
    }
    return canonical


def _market_session_index(
    market_sessions: pd.DataFrame | pd.Series | Iterable,
) -> pd.DatetimeIndex:
    if isinstance(market_sessions, pd.DataFrame):
        column = _column(market_sessions, "trade_date", "session_date")
        if column is None:
            raise ValueError("market_sessions require trade_date/session_date")
        values = market_sessions[column]
    elif isinstance(market_sessions, pd.Series):
        values = market_sessions
    else:
        values = pd.Series(list(market_sessions))
    return pd.DatetimeIndex(_dates(pd.Series(values)).dropna().unique()).sort_values()


def resolve_dividend_ex_dates(
    dividends: pd.DataFrame,
    actions: pd.DataFrame,
    market_sessions: pd.DataFrame | pd.Series | Iterable,
    *,
    notice_window_days: int = 15,
) -> pd.DataFrame:
    """Resolve cash events to an ex-date with explicit evidence first."""
    resolved = dividends.copy()
    if resolved.empty:
        for column in ("resolved_ex_date", "ex_date_basis"):
            resolved[column] = pd.Series(dtype="object")
        return resolved

    sessions = _market_session_index(market_sessions)
    cash_ex_column = _column(resolved, "ex_date", "effective_date")
    action_type = _column(actions, "event_type", "action_type")
    notice_ex_column = _column(actions, "effective_date", "ex_date")
    action_key = _column(actions, "rcept_no", "action_key", "filing_id")

    notices_by_identifier: dict[str, list[tuple[pd.Timestamp, str]]] = {}
    if action_type is not None and notice_ex_column is not None:
        notices = actions[actions[action_type].eq("ex_dividend")].copy()
        if not notices.empty:
            notices["_notice_date"] = _dates(notices[notice_ex_column])
            notices = notices[notices["_notice_date"].notna()]
            notices["_notice_key"] = (
                notices[action_key].fillna("").astype(str)
                if action_key is not None
                else ""
            )
            for identifier, group in notices.groupby(
                notices["identifier"].astype(str), sort=False,
            ):
                notices_by_identifier[str(identifier)] = sorted(
                    zip(group["_notice_date"], group["_notice_key"]),
                    key=lambda item: (item[0], item[1]),
                )

    resolved_dates: list[pd.Timestamp | pd.NaT] = []
    bases: list[str] = []
    for row in resolved.itertuples(index=False):
        direct = (
            pd.to_datetime(getattr(row, cash_ex_column), errors="coerce")
            if cash_ex_column is not None
            else pd.NaT
        )
        if pd.notna(direct):
            resolved_dates.append(pd.Timestamp(direct).normalize())
            bases.append("KRX_NOTICE")
            continue

        record_date = pd.to_datetime(row.record_date, errors="coerce")
        eligible_sessions = sessions[sessions <= record_date]
        inferred = (
            pd.Timestamp(eligible_sessions[-2])
            if len(eligible_sessions) >= 2
            else pd.NaT
        )
        identifier = str(row.identifier)
        candidates: list[tuple[int, pd.Timestamp, str]] = []
        for notice_date, notice_key in notices_by_identifier.get(identifier, []):
            if notice_date > record_date:
                continue
            anchor = inferred if pd.notna(inferred) else record_date
            distance = abs((notice_date - anchor).days)
            if distance <= notice_window_days:
                candidates.append((distance, notice_date, notice_key))
        if candidates:
            _, notice_date, _ = min(
                candidates,
                key=lambda item: (item[0], item[1], item[2]),
            )
            resolved_dates.append(notice_date)
            bases.append("KRX_NOTICE")
        elif pd.notna(inferred):
            resolved_dates.append(inferred)
            bases.append("KRX_T2_INFERRED")
        else:
            resolved_dates.append(pd.NaT)
            bases.append(None)

    resolved["resolved_ex_date"] = pd.to_datetime(resolved_dates)
    resolved["ex_date_basis"] = bases
    return resolved


def apply_dividends_to_prices(
    prices: pd.DataFrame,
    dividends: pd.DataFrame,
    *,
    listing_gap_days: int = LISTING_EPISODE_GAP_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply resolved dividends and calculate episode-local gross TR closes."""
    required_prices = {"identifier", "trade_date", "close", "adj_close"}
    missing = required_prices - set(prices.columns)
    if missing:
        raise ValueError(f"prices missing columns: {sorted(missing)}")
    if not dividends.empty and "resolved_ex_date" not in dividends:
        raise ValueError("dividends require resolved_ex_date")

    output = prices.copy()
    output["identifier"] = output["identifier"].astype(str)
    output["trade_date"] = _dates(output["trade_date"])
    output["close"] = pd.to_numeric(output["close"], errors="coerce")
    output["adj_close"] = pd.to_numeric(
        output["adj_close"], errors="coerce",
    )
    if output[["trade_date", "close", "adj_close"]].isna().any().any():
        raise ValueError("prices require finite trade_date, close and adj_close")
    if output[["close", "adj_close"]].le(0).any().any():
        raise ValueError("stock close and adj_close must be positive")
    if output.duplicated(["identifier", "trade_date"]).any():
        raise ValueError("duplicate identifier/trade_date price rows")

    output["_input_order"] = np.arange(len(output))
    output = output.sort_values(
        ["identifier", "trade_date"], kind="mergesort",
    ).reset_index(drop=True)
    gaps = output.groupby("identifier", sort=False)["trade_date"].diff().dt.days
    output["listing_episode"] = gaps.gt(listing_gap_days).groupby(
        output["identifier"], sort=False,
    ).cumsum().astype(int)
    output["adjusted_cash_dividend"] = 0.0

    event_output = dividends.copy().reset_index(drop=True)
    if "cash_amount" in event_output:
        event_output["cash_amount"] = pd.to_numeric(
            event_output["cash_amount"], errors="coerce",
        )
    event_output["applied_trade_date"] = pd.NaT
    event_output["adjusted_cash_amount"] = np.nan
    event_output["application_status"] = "unresolved_ex_date"

    groups = {
        identifier: group.index.to_numpy()
        for identifier, group in output.groupby("identifier", sort=False)
    }
    for event_index, event in event_output.iterrows():
        ex_date = pd.to_datetime(
            event.get("resolved_ex_date"), errors="coerce",
        )
        if pd.isna(ex_date):
            continue
        indices = groups.get(str(event.get("identifier")))
        if indices is None or len(indices) == 0:
            event_output.at[event_index, "application_status"] = "no_price_series"
            continue
        dates = output.loc[indices, "trade_date"].to_numpy(
            dtype="datetime64[ns]",
        )
        position = int(np.searchsorted(dates, np.datetime64(ex_date), side="left"))
        if position >= len(indices):
            event_output.at[event_index, "application_status"] = "pending_future_trade"
            continue
        applied_index = int(indices[position])
        if position == 0:
            event_output.at[event_index, "application_status"] = (
                "before_listing_or_episode_start"
            )
            continue
        previous_index = int(indices[position - 1])
        if (
            output.at[applied_index, "listing_episode"]
            != output.at[previous_index, "listing_episode"]
        ):
            event_output.at[event_index, "application_status"] = "listing_episode_gap"
            continue

        exact_trade = output.at[applied_index, "trade_date"] == ex_date.normalize()
        scale_index = applied_index if exact_trade else previous_index
        cash_amount = event.get("cash_amount")
        if pd.isna(cash_amount) or float(cash_amount) <= 0:
            event_output.at[event_index, "application_status"] = "invalid_cash_amount"
            continue
        scale = (
            float(output.at[scale_index, "adj_close"])
            / float(output.at[scale_index, "close"])
        )
        adjusted_cash = float(cash_amount) * scale
        output.at[applied_index, "adjusted_cash_dividend"] += adjusted_cash
        event_output.at[event_index, "applied_trade_date"] = output.at[
            applied_index, "trade_date"
        ]
        event_output.at[event_index, "adjusted_cash_amount"] = adjusted_cash
        event_output.at[event_index, "application_status"] = "applied"

    output["total_return_close"] = np.nan
    for _, indices_frame in output.groupby(
        ["identifier", "listing_episode"], sort=False,
    ):
        indices = indices_frame.index.to_numpy()
        adjusted = output.loc[indices, "adj_close"].to_numpy(dtype=float)
        cash = output.loc[
            indices, "adjusted_cash_dividend"
        ].to_numpy(dtype=float)
        total_return = np.empty(len(indices), dtype=float)
        total_return[0] = adjusted[0]
        for offset in range(1, len(indices)):
            gross_return = (adjusted[offset] + cash[offset]) / adjusted[offset - 1]
            total_return[offset] = total_return[offset - 1] * gross_return
        output.loc[indices, "total_return_close"] = total_return

    output = output.sort_values("_input_order").drop(
        columns="_input_order",
    ).reset_index(drop=True)
    return output, event_output


def build_total_return_close(
    prices: pd.DataFrame,
    actions: pd.DataFrame,
    market_sessions: pd.DataFrame | pd.Series | Iterable,
    *,
    notice_window_days: int = 15,
    listing_gap_days: int = LISTING_EPISODE_GAP_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Canonicalize, resolve and apply KRX cash dividends in one pure call."""
    canonical = canonicalize_cash_dividends(actions)
    resolved = resolve_dividend_ex_dates(
        canonical,
        actions,
        market_sessions,
        notice_window_days=notice_window_days,
    )
    return apply_dividends_to_prices(
        prices,
        resolved,
        listing_gap_days=listing_gap_days,
    )
