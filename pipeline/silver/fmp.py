"""FMP Bronze raw responses -> Silver candidates and publishers."""
from __future__ import annotations

import glob
import hashlib
import io
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db
from pipeline.common.sink import read_bytes
from pipeline.silver_quality.models import CandidateBundle

SUPPORTED_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
NON_EQUITY_NAME = re.compile(
    r"(?:\bETF\b|\bETN\b|\bWarrants?\b|\bUnits?\b|"
    r"\bSenior Notes?\b|\bSubordinated Notes?\b|\bBonds?\b)",
    re.IGNORECASE,
)
PREFERRED_NAME = re.compile(r"\bPreferred\b|\bPreference\b", re.IGNORECASE)
REIT_NAME = re.compile(r"\bREIT\b|Real Estate Investment Trust", re.IGNORECASE)

FINANCIAL_METRICS = {
    "income": {
        "revenue": ("revenue", "currency"),
        "costOfRevenue": ("cost_of_revenue", "currency"),
        "grossProfit": ("gross_profit", "currency"),
        "operatingIncome": ("operating_income", "currency"),
        "incomeBeforeTax": ("pretax_income", "currency"),
        "netIncome": ("net_income", "currency"),
        "eps": ("eps", "per_share"),
        "epsDiluted": ("eps_diluted", "per_share"),
        "weightedAverageShsOut": ("weighted_average_shares", "shares"),
        "weightedAverageShsOutDil": ("weighted_average_shares_diluted", "shares"),
    },
    "balance": {
        "cashAndCashEquivalents": ("cash_and_equivalents", "currency"),
        "totalCurrentAssets": ("current_assets", "currency"),
        "totalAssets": ("total_assets", "currency"),
        "totalCurrentLiabilities": ("current_liabilities", "currency"),
        "totalLiabilities": ("total_liabilities", "currency"),
        "totalStockholdersEquity": ("total_equity", "currency"),
        "totalDebt": ("total_debt", "currency"),
        "netDebt": ("net_debt", "currency"),
    },
    "cashflow": {
        "netCashProvidedByOperatingActivities": ("operating_cash_flow", "currency"),
        "operatingCashFlow": ("operating_cash_flow", "currency"),
        "capitalExpenditure": ("capital_expenditure", "currency"),
        "freeCashFlow": ("free_cash_flow", "currency"),
        "commonStockIssued": ("common_stock_issued", "currency"),
        "commonStockRepurchased": ("common_stock_repurchased", "currency"),
        "dividendsPaid": ("dividends_paid", "currency"),
    },
}
STATEMENT_TYPES = {"income": "IS", "balance": "BS", "cashflow": "CF"}


def _raw_frame(path: str) -> pd.DataFrame:
    payload = read_bytes(path)
    if payload is None or not payload.strip():
        return pd.DataFrame()
    stripped = payload.lstrip()
    if stripped[:1] in {b"[", b"{"}:
        try:
            value = json.loads(payload)
        except (TypeError, ValueError):
            return pd.DataFrame()
        if isinstance(value, list):
            return pd.DataFrame(item for item in value if isinstance(item, dict))
        if isinstance(value, dict):
            for key in ("data", "historical", "results"):
                rows = value.get(key)
                if isinstance(rows, list):
                    return pd.DataFrame(
                        item for item in rows if isinstance(item, dict)
                    )
            return pd.DataFrame([value])
        return pd.DataFrame()
    try:
        return pd.read_csv(io.BytesIO(payload))
    except (pd.errors.EmptyDataError, UnicodeDecodeError):
        return pd.DataFrame()


def _as_bool(value) -> bool | None:
    if value is None or (not isinstance(value, bool) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"true", "1", "yes"}:
        return True
    if rendered in {"false", "0", "no"}:
        return False
    return None


def _text(value) -> str | None:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    rendered = str(value).strip()
    return rendered or None


def _parse_date(value) -> date | None:
    rendered = _text(value)
    if not rendered:
        return None
    try:
        return date.fromisoformat(rendered[:10])
    except ValueError:
        return None


def _parse_timestamp(value) -> datetime | None:
    rendered = _text(value)
    if not rendered:
        return None
    parsed = pd.to_datetime(rendered, errors="coerce")
    if pd.isna(parsed):
        return None
    timestamp = pd.Timestamp(parsed)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(
            "America/New_York", ambiguous="raise", nonexistent="shift_forward",
        )
    return timestamp.tz_convert("UTC").to_pydatetime()


def _number(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(parsed) else parsed


def _integer(value) -> int | None:
    parsed = _number(value)
    return None if parsed is None else int(parsed)


def _exchange(row: dict) -> str | None:
    raw = _text(
        row.get("exchangeShortName")
        or row.get("exchange")
        or row.get("exchangeFullName")
    )
    if not raw:
        return None
    normalized = re.sub(r"[^A-Z]", "", raw.upper())
    if normalized.startswith("NASDAQ"):
        return "NASDAQ"
    if normalized in {"AMEX", "NYSEAMERICAN", "AMERICANSTOCKEXCHANGE"}:
        return "AMEX"
    if normalized in {"NYSE", "NEWYORKSTOCKEXCHANGE"}:
        return "NYSE"
    return None


def _country_code(value) -> str | None:
    rendered = (_text(value) or "").upper()
    aliases = {"US": "US", "USA": "US", "UNITED STATES": "US"}
    return aliases.get(rendered, rendered[:2] or None)


def _latest_snapshot_files(pattern: str, target_date: date | None) -> list[str]:
    paths = sorted(glob.glob(pattern))
    by_snapshot: dict[date, list[str]] = defaultdict(list)
    for path in paths:
        match = re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", path)
        if not match:
            continue
        snapshot = date.fromisoformat(match.group(1))
        if target_date is None or snapshot <= target_date:
            by_snapshot[snapshot].append(path)
    if not by_snapshot:
        return []
    return by_snapshot[max(by_snapshot)]


def _universe_files(base: str, target_date: date | None) -> tuple[list[str], list[str], list[str]]:
    screener = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/company-screener/snapshot_date=*/response.*",
        target_date,
    )
    profiles = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/profile-bulk/snapshot_date=*/part=*/response.*",
        target_date,
    )
    delisted = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/delisted/snapshot_date=*/page=*/response.*",
        target_date,
    )
    return screener + profiles, delisted, _latest_snapshot_files(
        f"{base}/stock/fmp/universe/symbol-change/snapshot_date=*/response.*",
        target_date,
    )


def prepare_universe(
    base: str,
    target_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    source_files, delisted_files, change_files = _universe_files(base, target_date)
    merged: dict[str, dict] = {}
    for path in source_files:
        frame = _raw_frame(path)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if not symbol:
                continue
            current = merged.setdefault(symbol, {"symbol": symbol, "_files": []})
            for key, value in raw.items():
                if _text(value) is not None or isinstance(value, bool):
                    current[key] = value
            current["_files"].append(path)

    delisted_by_symbol: dict[str, dict] = {}
    for path in delisted_files:
        for raw in _raw_frame(path).to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if not symbol:
                continue
            entry = delisted_by_symbol.setdefault(symbol, {})
            entry.update(raw)
            entry["_source_file"] = path
            current = merged.setdefault(symbol, {"symbol": symbol, "_files": []})
            for key, value in raw.items():
                if _text(value) is not None:
                    current.setdefault(key, value)
            current["_files"].append(path)

    changes: list[dict] = []
    for path in change_files:
        for raw in _raw_frame(path).to_dict("records"):
            raw["_source_file"] = path
            changes.append(raw)

    exclusions: Counter[str] = Counter()
    samples: list[dict] = []
    admitted: dict[str, dict] = {}
    for symbol, row in sorted(merged.items()):
        exchange = _exchange(row)
        is_etf = _as_bool(row.get("isEtf"))
        is_fund = _as_bool(row.get("isFund"))
        name = _text(row.get("companyName") or row.get("name")) or symbol
        reason = None
        if exchange not in SUPPORTED_EXCHANGES:
            reason = "UNSUPPORTED_EXCHANGE"
        elif is_etf is True:
            reason = "ETF"
        elif is_fund is True:
            reason = "FUND"
        elif NON_EQUITY_NAME.search(name):
            reason = "NON_EQUITY_NAME"
        elif (is_etf is None or is_fund is None) and symbol not in delisted_by_symbol:
            reason = "AMBIGUOUS_CLASSIFICATION"
        if reason:
            exclusions[reason] += 1
            if len(samples) < 30:
                samples.append({
                    "symbol": symbol,
                    "name": name,
                    "exchange": exchange,
                    "isEtf": is_etf,
                    "isFund": is_fund,
                    "reason": reason,
                })
            continue
        industry = _text(row.get("industry")) or ""
        if _as_bool(row.get("isAdr")) is True:
            instrument_type = "adr"
        elif PREFERRED_NAME.search(name):
            instrument_type = "preferred_stock"
        elif REIT_NAME.search(name) or REIT_NAME.search(industry):
            instrument_type = "reit"
        else:
            instrument_type = "common_stock"
        currency = (_text(row.get("currency")) or "USD").upper()
        admitted[symbol] = {
            **row,
            "name": name,
            "exchange_norm": exchange,
            "instrument_type": instrument_type,
            "currency_norm": currency,
            "listed_from_norm": _parse_date(row.get("ipoDate")),
            "listed_to_norm": _parse_date(row.get("delistedDate")),
        }

    # Symbol changes identify one security across ticker episodes.  The current
    # symbol owns the asset; old tickers become bounded identifiers.
    canonical = {symbol: symbol for symbol in admitted}
    change_by_new: dict[str, list[tuple[str, date]]] = defaultdict(list)
    for raw in sorted(changes, key=lambda item: str(item.get("date") or "")):
        old = _text(raw.get("oldSymbol"))
        new = _text(raw.get("newSymbol"))
        changed = _parse_date(raw.get("date"))
        if not old or not new or not changed or new not in admitted:
            continue
        canonical[old] = canonical.get(new, new)
        change_by_new[new].append((old, changed))
        if old in admitted and old != new:
            admitted.pop(old)

    asset_rows: list[dict] = []
    identifier_rows: list[dict] = []
    for symbol, row in admitted.items():
        natural_key = f"FMP:{canonical.get(symbol, symbol)}"
        listed_from = row["listed_from_norm"]
        listed_to = row["listed_to_norm"]
        asset_rows.append({
            "natural_key": natural_key,
            "name": row["name"],
            "asset_type": "stock",
            "instrument_type": row["instrument_type"],
            "exchange": row["exchange_norm"],
            "currency": row["currency_norm"],
            "country_code": _country_code(row.get("country")),
            "base_currency": row["currency_norm"],
            "listed_from": listed_from,
            "listed_to": listed_to,
            "is_etf": False,
            "is_fund": False,
            "source_file": (row.get("_files") or [None])[-1],
        })
        current_valid_from = listed_from or date.min
        for old, changed in change_by_new.get(symbol, []):
            identifier_rows.append({
                "natural_key": natural_key,
                "source": "FMP",
                "identifier": old,
                "identifier_type": "ticker",
                "valid_from": date.min,
                "valid_to": changed - timedelta(days=1),
                "source_file": None,
            })
            current_valid_from = max(current_valid_from, changed)
        identifier_rows.append({
            "natural_key": natural_key,
            "source": "FMP",
            "identifier": symbol,
            "identifier_type": "ticker",
            "valid_from": current_valid_from,
            "valid_to": listed_to,
            "source_file": (row.get("_files") or [None])[-1],
        })
        for identifier_type, key in (
            ("cik", "cik"), ("cusip", "cusip"), ("isin", "isin"),
        ):
            value = _text(row.get(key))
            if value:
                identifier_rows.append({
                    "natural_key": natural_key,
                    "source": "FMP",
                    "identifier": value,
                    "identifier_type": identifier_type,
                    "valid_from": listed_from or date.min,
                    "valid_to": listed_to,
                    "source_file": (row.get("_files") or [None])[-1],
                })

    assets = pd.DataFrame(asset_rows)
    identifiers = pd.DataFrame(identifier_rows)
    if not identifiers.empty:
        identifiers = identifiers.drop_duplicates(
            ["natural_key", "source", "identifier_type", "identifier", "valid_from"],
            keep="last",
        ).reset_index(drop=True)
    return assets, identifiers, {
        "raw_symbol_count": len(merged),
        "admitted_symbol_count": len(admitted),
        "excluded_symbol_count": sum(exclusions.values()),
        "excluded_by_reason": dict(exclusions),
        "excluded_samples": samples,
        "source_file_count": len(source_files) + len(delisted_files) + len(change_files),
    }


def _manifest_received_at(path: str) -> datetime | None:
    manifest_path = str(Path(path).with_name("manifest.json"))
    raw = read_bytes(manifest_path)
    if raw is None:
        return None
    try:
        return _parse_timestamp(json.loads(raw).get("received_at"))
    except (TypeError, ValueError):
        return None


def _split_events(base: str) -> dict[str, list[tuple[date, float]]]:
    result: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for path in glob.glob(f"{base}/corporate_actions/fmp/splits/**/response.*", recursive=True):
        for row in _raw_frame(path).to_dict("records"):
            symbol = _text(row.get("symbol"))
            event_date = _parse_date(row.get("date"))
            numerator = _number(row.get("numerator"))
            denominator = _number(row.get("denominator"))
            if symbol and event_date and numerator and denominator and denominator != 0:
                result[symbol].append((event_date, numerator / denominator))
    return result


def _ticker_metadata(assets: pd.DataFrame, identifiers: pd.DataFrame) -> dict[str, dict]:
    if assets.empty or identifiers.empty:
        return {}
    by_key = assets.set_index("natural_key").to_dict("index")
    return {
        str(row.identifier): by_key[str(row.natural_key)]
        for row in identifiers.itertuples(index=False)
        if row.identifier_type == "ticker" and str(row.natural_key) in by_key
    }


def prepare_prices(
    base: str,
    assets: pd.DataFrame,
    identifiers: pd.DataFrame,
    target_date: date | None = None,
    year: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    ticker_meta = _ticker_metadata(assets, identifiers)
    splits = _split_events(base)
    paths = sorted(glob.glob(f"{base}/stock/fmp/eod-bulk/date=*/response.*"))
    rows: list[dict] = []
    input_rows = 0
    for path in paths:
        match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path.replace("\\", "/"))
        if not match:
            continue
        partition_date = date.fromisoformat(match.group(1))
        if target_date is not None and partition_date != target_date:
            continue
        if year is not None and partition_date.year != year:
            continue
        frame = _raw_frame(path)
        input_rows += len(frame)
        received_at = _manifest_received_at(path)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            meta = ticker_meta.get(symbol or "")
            trade_date = _parse_date(raw.get("date")) or partition_date
            if meta is None:
                continue
            factor = 1.0
            for split_date, split_ratio in splits.get(symbol, []):
                if split_date > trade_date:
                    factor *= split_ratio
            adjusted_close = _number(raw.get("close"))
            total_return = _number(raw.get("adjClose"))
            rows.append({
                "identifier": symbol,
                "source": "FMP",
                "trade_date": trade_date,
                "open": None if _number(raw.get("open")) is None else _number(raw.get("open")) * factor,
                "high": None if _number(raw.get("high")) is None else _number(raw.get("high")) * factor,
                "low": None if _number(raw.get("low")) is None else _number(raw.get("low")) * factor,
                "close": None if adjusted_close is None else adjusted_close * factor,
                "adj_close": adjusted_close,
                "total_return_close": total_return,
                "currency": meta["base_currency"],
                "vwap": _number(raw.get("vwap")),
                "available_at": received_at,
                "volume": _integer(raw.get("volume")),
                "trading_value": None,
                "shares": None,
                "market_cap": None,
                "market": meta["exchange"],
                "asset_type": "stock",
                "source_file": path,
            })
    frame = pd.DataFrame(rows)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "price_semantics": {
            "close": "raw reconstructed from split-adjusted close",
            "adj_close": "FMP close (split-adjusted)",
            "total_return_close": "FMP adjClose (dividend-adjusted)",
        },
    }


def prepare_fx(
    base: str,
    target_date: date | None = None,
    year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    asset_rows = [{
        "natural_key": "FMP:FX:USDKRW",
        "name": "USD/KRW",
        "asset_type": "fx",
        "instrument_type": "fx",
        "exchange": "FX",
        "currency": "KRW",
        "country_code": None,
        "base_currency": "KRW",
        "listed_from": None,
        "listed_to": None,
        "is_etf": False,
        "is_fund": False,
        "source_file": None,
    }]
    identifier_rows = [{
        "natural_key": "FMP:FX:USDKRW",
        "source": "FMP",
        "identifier": "USDKRW",
        "identifier_type": "fx_pair",
        "valid_from": date.min,
        "valid_to": None,
        "source_file": None,
    }]
    paths = sorted(glob.glob(f"{base}/fx/fmp/pair=USDKRW/**/response.*", recursive=True))
    rows = []
    input_rows = 0
    for path in paths:
        frame = _raw_frame(path)
        input_rows += len(frame)
        received_at = _manifest_received_at(path)
        for raw in frame.to_dict("records"):
            trade_date = _parse_date(raw.get("date"))
            if trade_date is None or (
                target_date is not None and trade_date != target_date
            ):
                continue
            if year is not None and trade_date.year != year:
                continue
            close = _number(raw.get("close"))
            rows.append({
                "identifier": "USDKRW",
                "source": "FMP_FX",
                "trade_date": trade_date,
                "open": _number(raw.get("open")),
                "high": _number(raw.get("high")),
                "low": _number(raw.get("low")),
                "close": close,
                "adj_close": close,
                "total_return_close": close,
                "currency": "KRW",
                "vwap": _number(raw.get("vwap")),
                "available_at": received_at,
                "volume": _integer(raw.get("volume")),
                "trading_value": None,
                "shares": None,
                "market_cap": None,
                "market": "FX",
                "asset_type": "fx",
                "source_file": path,
            })
    if not rows:
        asset_rows = []
        identifier_rows = []
    return (
        pd.DataFrame(asset_rows),
        pd.DataFrame(identifier_rows),
        pd.DataFrame(rows),
        {
            "input_rows": input_rows,
            "transformed_rows": len(rows),
            "excluded_rows": input_rows - len(rows),
        },
    )


def _financial_kind(path: str) -> str | None:
    normalized = path.replace("\\", "/")
    for kind in FINANCIAL_METRICS:
        if f"/financials/fmp/{kind}/" in normalized:
            return kind
    if "/financials/fmp/by-symbol/" in normalized:
        name = Path(path).parent.name
        return {"income-statement": "income", "balance-sheet-statement": "balance", "cash-flow-statement": "cashflow"}.get(name)
    return None


def prepare_fundamentals(
    base: str,
    identifiers: pd.DataFrame,
    year: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    symbols = set(
        identifiers.loc[identifiers["identifier_type"].eq("ticker"), "identifier"].astype(str)
    ) if not identifiers.empty else set()
    paths = sorted(set(
        glob.glob(f"{base}/financials/fmp/income/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/balance/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/cashflow/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/by-symbol/**/response.*", recursive=True)
    ))
    rows: list[dict] = []
    input_rows = 0
    for path in paths:
        kind = _financial_kind(path)
        if kind is None:
            continue
        year_partition = re.search(r"/year=(\d{4})/", path.replace("\\", "/"))
        if year is not None and year_partition and int(year_partition.group(1)) != year:
            continue
        frame = _raw_frame(path)
        input_rows += len(frame)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if symbol not in symbols:
                continue
            period_end = _parse_date(raw.get("date"))
            fiscal_period = (_text(raw.get("period")) or "").upper()
            if not period_end or fiscal_period not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
                continue
            if year is not None and period_end.year != year:
                continue
            filed = _parse_date(raw.get("filingDate"))
            accepted_at = _parse_timestamp(raw.get("acceptedDate"))
            available_at = accepted_at
            if available_at is None and filed is not None:
                available_at = datetime.combine(
                    filed + timedelta(days=1), datetime.min.time(), timezone.utc,
                )
            available_date = available_at.date() if available_at else filed
            revision_key = (
                _text(raw.get("acceptedDate"))
                or _text(raw.get("filingDate"))
                or f"{symbol}:{period_end}:{fiscal_period}:{kind}"
            )
            reported_currency = (_text(raw.get("reportedCurrency")) or "").upper() or None
            for source_metric, (metric, unit_type) in FINANCIAL_METRICS[kind].items():
                value = _number(raw.get(source_metric))
                if value is None:
                    continue
                rows.append({
                    "identifier": symbol,
                    "source": "FMP",
                    "statement_type": STATEMENT_TYPES[kind],
                    "data_basis": "STANDARDIZED",
                    "period_end": period_end,
                    "fiscal_period": fiscal_period,
                    "fs_type": "UNKNOWN",
                    "filing_id": None,
                    "filed": filed,
                    "accepted_at": accepted_at,
                    "available_date": available_date,
                    "available_at": available_at,
                    "metric": metric,
                    "value": value,
                    "currency": reported_currency if unit_type != "shares" else None,
                    "unit_type": unit_type,
                    "revision_key": revision_key,
                    "source_file": path,
                })
    frame = pd.DataFrame(rows)
    before = len(frame)
    key = [
        "identifier", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "revision_key", "metric",
    ]
    if not frame.empty:
        frame = frame.sort_values("source_file").drop_duplicates(key, keep="last").reset_index(drop=True)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "duplicate_rows_removed": before - len(frame),
    }


def prepare_actions(
    base: str,
    identifiers: pd.DataFrame,
    year: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    symbols = set(
        identifiers.loc[identifiers["identifier_type"].eq("ticker"), "identifier"].astype(str)
    ) if not identifiers.empty else set()
    records: list[dict] = []
    input_rows = 0
    patterns = {
        "split": f"{base}/corporate_actions/fmp/splits/**/response.*",
        "dividend": f"{base}/corporate_actions/fmp/dividends/**/response.*",
    }
    for kind, pattern in patterns.items():
        for path in glob.glob(pattern, recursive=True):
            year_partition = re.search(
                r"/year=(\d{4})/", path.replace("\\", "/"),
            )
            if year is not None and year_partition and int(year_partition.group(1)) != year:
                continue
            frame = _raw_frame(path)
            input_rows += len(frame)
            for raw in frame.to_dict("records"):
                symbol = _text(raw.get("symbol"))
                event_date = _parse_date(raw.get("date"))
                if symbol not in symbols or event_date is None:
                    continue
                if year is not None and event_date.year != year:
                    continue
                if kind == "split":
                    numerator = _number(raw.get("numerator"))
                    denominator = _number(raw.get("denominator"))
                    price_factor = (
                        denominator / numerator
                        if numerator and denominator else None
                    )
                    share_factor = (
                        numerator / denominator
                        if numerator and denominator else None
                    )
                    action_type = "stock_split"
                    cash_amount = None
                    record_date = payment_date = announcement_date = None
                    source = "FMP_SPLIT"
                else:
                    numerator = denominator = price_factor = share_factor = None
                    action_type = "cash_dividend"
                    cash_amount = _number(raw.get("dividend"))
                    announcement_date = _parse_date(raw.get("declarationDate"))
                    record_date = _parse_date(raw.get("recordDate"))
                    payment_date = _parse_date(raw.get("paymentDate"))
                    source = "FMP_DIVIDEND"
                material = json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))
                records.append({
                    "identifier": symbol,
                    "source": source,
                    "action_key": hashlib.sha256(material.encode()).hexdigest(),
                    "action_type": action_type,
                    "announcement_date": announcement_date,
                    "ex_date": event_date,
                    "record_date": record_date,
                    "payment_date": payment_date,
                    "cash_amount": cash_amount,
                    "currency": "USD",
                    "ratio_numerator": numerator,
                    "ratio_denominator": denominator,
                    "expected_price_factor": price_factor,
                    "share_count_factor": share_factor,
                    "status": "confirmed",
                    "confidence": "FMP_CALENDAR",
                    "filing_id": None,
                    "source_file": path,
                })
    frame = pd.DataFrame(records)
    before = len(frame)
    if not frame.empty:
        frame = frame.drop_duplicates(
            ["identifier", "source", "action_key"], keep="last",
        ).reset_index(drop=True)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "duplicate_rows_removed": before - len(frame),
    }


def market_closed(base: str, target_date: date) -> bool:
    if target_date.weekday() >= 5:
        return True
    paths = _latest_snapshot_files(
        f"{base}/market/fmp/holidays-by-exchange/snapshot_date=*/exchange=*/response.*",
        target_date,
    )
    closed_exchanges = set()
    for path in paths:
        match = re.search(r"/exchange=([^/]+)/", path.replace("\\", "/"))
        exchange = match.group(1) if match else None
        for row in _raw_frame(path).to_dict("records"):
            if _parse_date(row.get("date")) != target_date:
                continue
            is_closed = _as_bool(row.get("isClosed"))
            if is_closed is not False and exchange:
                closed_exchanges.add(exchange)
    return SUPPORTED_EXCHANGES.issubset(closed_exchanges)


def build_candidates(base: str, target_date: date | None = None) -> CandidateBundle:
    assets, identifiers, universe_stats = prepare_universe(base, target_date)
    prices, price_stats = prepare_prices(base, assets, identifiers, target_date)
    fx_assets, fx_identifiers, fx_prices, fx_stats = prepare_fx(base, target_date)
    assets = pd.concat([assets, fx_assets], ignore_index=True)
    identifiers = pd.concat([identifiers, fx_identifiers], ignore_index=True)
    prices = pd.concat([prices, fx_prices], ignore_index=True)
    fundamentals, fundamental_stats = prepare_fundamentals(base, identifiers)
    actions, action_stats = prepare_actions(base, identifiers)
    return CandidateBundle(
        assets=assets,
        identifiers=identifiers,
        prices=prices,
        fundamentals=fundamentals,
        actions=actions,
        stats={
            "asset": universe_stats,
            "price_daily": price_stats,
            "fx": fx_stats,
            "fundamental": fundamental_stats,
            "corporate_action": action_stats,
            "_source": "FMP",
            "_target_date": target_date,
            "_market_closed": (
                market_closed(base, target_date)
                if target_date is not None else False
            ),
        },
    )


def publish_assets(
    conn,
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    quality_run_id: UUID,
) -> dict[str, int]:
    natural_to_id: dict[str, int] = {}
    with conn.cursor() as cur:
        for natural_key, group in identifier_candidates.groupby("natural_key", sort=False):
            asset_id = None
            for row in group.itertuples(index=False):
                if row.identifier_type == "cik":
                    continue
                cur.execute(
                    "SELECT asset_id FROM asset_identifier "
                    "WHERE source='FMP' AND identifier_type=%s AND identifier=%s "
                    "ORDER BY (valid_to IS NULL) DESC, valid_from DESC LIMIT 1",
                    (row.identifier_type, str(row.identifier)),
                )
                found = cur.fetchone()
                if found:
                    asset_id = found[0]
                    break
            asset_row = asset_candidates[
                asset_candidates["natural_key"].eq(natural_key)
            ].iloc[0]
            values = (
                asset_row["name"], asset_row["asset_type"],
                asset_row["instrument_type"], asset_row["exchange"],
                asset_row["currency"], asset_row["country_code"],
                asset_row["base_currency"], asset_row["listed_from"],
                asset_row["listed_to"], quality_run_id,
            )
            if asset_id is None:
                cur.execute(
                    """
                    INSERT INTO asset(
                        name,asset_type,instrument_type,exchange,currency,
                        country_code,base_currency,listed_from,listed_to,
                        quality_run_id,loaded_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                    RETURNING asset_id
                    """,
                    values,
                )
                asset_id = cur.fetchone()[0]
            else:
                cur.execute(
                    """
                    UPDATE asset SET
                        name=%s,asset_type=%s,instrument_type=%s,exchange=%s,
                        currency=%s,country_code=%s,base_currency=%s,
                        listed_from=%s,listed_to=%s,quality_run_id=%s,loaded_at=now()
                    WHERE asset_id=%s
                    """,
                    values + (asset_id,),
                )
            natural_to_id[str(natural_key)] = asset_id

        for row in identifier_candidates.itertuples(index=False):
            asset_id = natural_to_id[str(row.natural_key)]
            if row.identifier_type != "cik" and row.valid_to is None:
                cur.execute(
                    "SELECT asset_id FROM asset_identifier "
                    "WHERE source=%s AND identifier_type=%s AND identifier=%s "
                    "AND valid_to IS NULL",
                    (row.source, row.identifier_type, str(row.identifier)),
                )
                existing = cur.fetchone()
                if existing and existing[0] != asset_id:
                    raise RuntimeError(
                        "FMP identifier conflict: "
                        f"type={row.identifier_type} identifier={row.identifier}"
                    )
            cur.execute(
                """
                INSERT INTO asset_identifier(
                    asset_id,source,identifier,identifier_type,valid_from,valid_to,
                    quality_run_id,loaded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT(
                    asset_id,source,identifier_type,identifier,valid_from
                ) DO UPDATE SET valid_to=EXCLUDED.valid_to,
                    quality_run_id=EXCLUDED.quality_run_id,loaded_at=now()
                """,
                (
                    asset_id, row.source, str(row.identifier), row.identifier_type,
                    row.valid_from, row.valid_to, quality_run_id,
                ),
            )
    return {
        str(row.identifier): natural_to_id[str(row.natural_key)]
        for row in identifier_candidates.itertuples(index=False)
        if row.identifier_type in {"ticker", "fx_pair"}
    }


def _publish_frame(
    conn,
    table: str,
    candidates: pd.DataFrame,
    identifier_map: dict[str, int],
    quality_run_id: UUID,
    columns: list[str],
    conflict: list[str],
    update: list[str],
) -> int:
    if candidates.empty:
        return 0
    frame = candidates.copy()
    frame["asset_id"] = frame["identifier"].map(identifier_map)
    if frame["asset_id"].isna().any():
        missing = sorted(frame.loc[frame["asset_id"].isna(), "identifier"].unique())
        raise RuntimeError(f"unmapped FMP identifiers: {missing[:20]}")
    frame["asset_id"] = frame["asset_id"].astype("int64")
    frame["quality_run_id"] = quality_run_id
    rows = list(
        frame[columns].astype(object).where(pd.notna(frame[columns]), None)
        .itertuples(index=False, name=None)
    )
    return db.upsert(
        conn, table, columns, rows, conflict, update,
        temp_name=f"_stg_fmp_{table}",
    )


def publish_prices(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "trade_date", "open", "high", "low", "close",
        "adj_close", "total_return_close", "currency", "vwap", "available_at",
        "volume", "trading_value", "shares", "market_cap", "market",
        "quality_run_id",
    ]
    return _publish_frame(
        conn, "price_daily", candidates, identifier_map, quality_run_id, columns,
        ["asset_id", "source", "trade_date"],
        [column for column in columns if column not in {"asset_id", "source", "trade_date"}],
    )


def publish_fundamentals(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "filing_id", "filed", "accepted_at",
        "available_date", "available_at", "metric", "value", "currency",
        "unit_type", "revision_key", "quality_run_id",
    ]
    conflict = [
        "asset_id", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "revision_key", "metric",
    ]
    return _publish_frame(
        conn, "fundamental", candidates, identifier_map, quality_run_id, columns,
        conflict, [column for column in columns if column not in set(conflict)],
    )


def publish_actions(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "action_key", "action_type", "announcement_date",
        "ex_date", "record_date", "payment_date", "cash_amount", "currency",
        "ratio_numerator", "ratio_denominator", "expected_price_factor",
        "share_count_factor", "status", "confidence", "filing_id",
        "quality_run_id",
    ]
    return _publish_frame(
        conn, "corporate_action", candidates, identifier_map, quality_run_id,
        columns, ["asset_id", "source", "action_key"],
        [column for column in columns if column not in {"asset_id", "source", "action_key"}],
    )
