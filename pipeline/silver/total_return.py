"""KRX 배당 재투자 총수익 시계열(total_return_close) 계산.

adj_close 는 분할·증자 등 *가격수정*만 반영한다(배당 미반영). total_return_close
는 여기에 현금배당 재투자를 더한 *총수익* 시계열이다. FMP 와 동일하게 최신일을
adj_close 에 앵커한 back-adjusted 시리즈이므로, 최신일 total_return_close ==
adj_close 이고 과거는 재투자분만큼 낮게 스케일된다(수익률을 취하면 배당이 더해진다).

정의(표준 ex-date 재투자):
    ex-date d 의 배당수익률 y(d) = 주당현금배당(d) / close(d)   # ex-date 종가로 재투자
    일별 배당계수 f(d) = 1 + y(d)  (배당 없는 날은 1)
    누적 배당계수 D(t) = Π_{s<=t} f(s)   (자산·상장에피소드별)
    total_return_close(t) = adj_close(t) × D(t) / D_last

배당 없는 자산/날짜는 total_return_close == adj_close.
주당배당액이 없는(cash_amount NULL) 배당은 재투자에 못 넣으므로 건너뛴다(부분 총수익).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def derive_ex_dates(
    prices: pd.DataFrame,
    dividends: pd.DataFrame,
) -> pd.DataFrame:
    """KRX 배당은 ex_date(배당락일)가 없고 record_date(배당기준일)만 있다.

    배당락일 ≈ **record_date 직전 거래일**(결제 T+2)로 근사한다. 자산별 실제
    거래일(price_daily)을 써서 record_date 보다 엄격히 이전인 마지막 거래일에
    배당을 붙인다. record_date 가 첫 거래일보다 앞서는 배당은 버린다.

    dividends: [asset_id, record_date, cash_amount] → [asset_id, ex_date, cash_amount].
    """
    empty = pd.DataFrame(columns=["asset_id", "ex_date", "cash_amount"])
    if dividends is None or len(dividends) == 0:
        return empty
    px = prices[["asset_id", "trade_date"]].copy()
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px = px.sort_values("trade_date").reset_index(drop=True)
    div = dividends.dropna(subset=["record_date"]).copy()
    if div.empty:
        return empty
    div["record_date"] = pd.to_datetime(div["record_date"])
    div = div.sort_values("record_date").reset_index(drop=True)
    merged = pd.merge_asof(
        div, px, by="asset_id",
        left_on="record_date", right_on="trade_date",
        direction="backward", allow_exact_matches=False,
    )
    out = merged.dropna(subset=["trade_date"]).copy()
    out["ex_date"] = out["trade_date"].dt.date
    return out[["asset_id", "ex_date", "cash_amount"]]


def compute_total_return_close(
    prices: pd.DataFrame,
    dividends: pd.DataFrame,
    *,
    group_keys: list[str] | None = None,
) -> pd.Series:
    """total_return_close 를 계산해 prices.index 에 맞춰 반환한다.

    prices: 최소 [<group_keys>, trade_date, close, adj_close]. group_keys 기본은
        ["asset_id"]. 상장 재개로 시계열이 끊기면 에피소드 컬럼을 group_keys 에
        포함해 넘긴다(예: ["asset_id", "listing_episode"]).
    dividends: [<asset key>, ex_date, cash_amount] (주당 원화, raw). 비어도 된다.
        asset key 는 group_keys 의 첫 컬럼(asset 식별자)과 조인한다.
    """
    keys = group_keys or ["asset_id"]
    asset_key = keys[0]
    if prices.empty:
        return pd.Series([], dtype="float64", index=prices.index)

    p = prices.reset_index()  # 'index' 컬럼에 원래 인덱스 보존
    p = p.sort_values(keys + ["trade_date"]).reset_index(drop=True)

    factor = pd.Series(1.0, index=p.index)
    if dividends is not None and len(dividends):
        div = dividends.dropna(subset=["cash_amount"]).copy()
        div = div[pd.to_numeric(div["cash_amount"], errors="coerce") > 0]
        if len(div):
            div = (
                div.groupby([asset_key, "ex_date"], as_index=False)["cash_amount"]
                .sum()
            )
            merged = p.merge(
                div,
                left_on=[asset_key, "trade_date"],
                right_on=[asset_key, "ex_date"],
                how="left",
            )
            close = pd.to_numeric(merged["close"], errors="coerce")
            cash = pd.to_numeric(merged["cash_amount"], errors="coerce")
            yld = cash / close.where(close > 0)
            # 배당수익률이 비정상(음수·>100%)이면 재투자에서 제외(오염 방지)
            valid = cash.notna() & (close > 0) & (yld >= 0) & (yld <= 1.0)
            factor = pd.Series(
                np.where(valid, 1.0 + yld.fillna(0.0), 1.0), index=p.index,
            )

    p["_f"] = factor.to_numpy()
    d_cum = p.groupby(keys)["_f"].cumprod()
    d_last = d_cum.groupby([p[k] for k in keys]).transform("last")
    adj = pd.to_numeric(p["adj_close"], errors="coerce")
    p["total_return_close"] = (adj * d_cum / d_last).round(4)
    return p.set_index("index")["total_return_close"].reindex(prices.index)


# ---------------------------------------------------------------------------
# Batch recompute over the KRX price series in the DB.
# ---------------------------------------------------------------------------

def _load_krx(conn, asset_ids=None):
    import pandas as pd
    scope = ""
    params: tuple = ()
    if asset_ids is not None:
        ids = [int(a) for a in asset_ids]
        if not ids:
            empty_p = pd.DataFrame(
                columns=["asset_id", "trade_date", "close", "adj_close",
                         "current_trc"])
            empty_d = pd.DataFrame(
                columns=["asset_id", "record_date", "cash_amount"])
            return empty_p, empty_d
        scope = " AND asset_id = ANY(%s)"
        params = (ids,)
    with conn.cursor() as c:
        c.execute(
            "SELECT asset_id, trade_date, close, adj_close, total_return_close "
            f"FROM price_daily WHERE source='KRX'{scope}", params,
        )
        prices = pd.DataFrame(
            c.fetchall(),
            columns=["asset_id", "trade_date", "close", "adj_close",
                     "current_trc"],
        )
        # KRX cash dividends carry record_date (배당기준일), not ex_date.
        # ex_date is derived from record_date against each asset's trading days.
        c.execute(
            "SELECT asset_id, record_date, cash_amount FROM corporate_action "
            "WHERE action_type='cash_dividend' AND cash_amount IS NOT NULL "
            f"AND record_date IS NOT NULL{scope}", params,
        )
        dividends = pd.DataFrame(
            c.fetchall(), columns=["asset_id", "record_date", "cash_amount"],
        )
    for col in ("close", "adj_close", "current_trc"):
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    dividends["cash_amount"] = pd.to_numeric(
        dividends["cash_amount"], errors="coerce")
    return prices, dividends


def assets_with_recent_dividend_changes(conn, since_date) -> list[int]:
    """since_date 이후 loaded_at 된 현금배당이 있는 KRX asset_id 목록.

    daily 증분에서 새로 들어오거나 정정된 배당의 자산만 골라 스코프 재계산한다.
    """
    with conn.cursor() as c:
        c.execute(
            "SELECT DISTINCT ca.asset_id FROM corporate_action ca "
            "WHERE ca.action_type='cash_dividend' AND ca.loaded_at >= %s "
            "AND EXISTS (SELECT 1 FROM price_daily p "
            "            WHERE p.asset_id=ca.asset_id AND p.source='KRX')",
            (since_date,),
        )
        return [int(r[0]) for r in c.fetchall()]


def run(conn=None, *, dry_run: bool = True, asset_ids=None) -> dict:
    """KRX total_return_close 를 배당 재투자로 재계산한다.

    dry_run=True: 통계만 반환(쓰기 없음). False: 값이 바뀌는 행만 UPDATE 한다.
    asset_ids 를 주면 해당 자산만 스코프 재계산한다(daily 증분용). 자산은 서로
    독립이라 부분 재계산 결과가 전체와 동일하다.
    total_return_close 한 컬럼만 건드리므로 truncate/reload 는 없다.
    """
    import pandas as pd
    from pipeline.common import db

    owns = conn is None
    conn = conn or db.connect()
    try:
        prices, dividends = _load_krx(conn, asset_ids=asset_ids)
        if prices.empty:
            if owns:
                conn.rollback()
            return {"krx_price_rows": 0, "rows_changed": 0, "rows_updated": 0,
                    "dry_run": dry_run, "scoped": asset_ids is not None}
        ex_dividends = derive_ex_dates(prices, dividends)
        new_trc = compute_total_return_close(
            prices[["asset_id", "trade_date", "close", "adj_close"]],
            ex_dividends,
            group_keys=["asset_id"],
        )
        prices["new_trc"] = new_trc.to_numpy()
        div_assets = set(ex_dividends["asset_id"])
        changed = prices[
            prices["new_trc"].notna()
            & (
                prices["current_trc"].isna()
                | ((prices["new_trc"] - prices["current_trc"]).abs() > 1e-4)
            )
        ]
        # sanity: every asset's LATEST total return must equal adj_close (anchor)
        latest = prices.sort_values("trade_date").groupby("asset_id").tail(1)
        anchor_bad = int(
            ((latest["new_trc"] - latest["adj_close"]).abs() > 1e-4).sum()
        )
        stats = {
            "krx_price_rows": int(len(prices)),
            "dividend_events_with_record_date": int(len(dividends)),
            "dividend_assets": int(len(div_assets)),
            "ex_dividends_aligned": int(len(ex_dividends)),
            "rows_changed": int(len(changed)),
            "anchor_mismatch": anchor_bad,
            "dry_run": dry_run,
        }
        if dry_run or changed.empty:
            conn.rollback()
            return stats
        if anchor_bad:
            conn.rollback()
            stats["aborted"] = "anchor mismatch > 0"
            return stats
        with conn.cursor() as c:
            c.execute(
                "CREATE TEMP TABLE _trc_stg (asset_id BIGINT, trade_date DATE, "
                "total_return_close NUMERIC(28,8)) ON COMMIT DROP"
            )
            with c.copy(
                "COPY _trc_stg (asset_id, trade_date, total_return_close) "
                "FROM STDIN"
            ) as cp:
                for r in changed[["asset_id", "trade_date", "new_trc"]].itertuples(
                    index=False
                ):
                    cp.write_row((int(r[0]), r[1], float(r[2])))
            c.execute("CREATE INDEX ON _trc_stg (asset_id, trade_date)")
            c.execute(
                "UPDATE price_daily p SET total_return_close = s.total_return_close "
                "FROM _trc_stg s WHERE p.source='KRX' "
                "AND p.asset_id=s.asset_id AND p.trade_date=s.trade_date"
            )
            stats["rows_updated"] = c.rowcount
        conn.commit()
        return stats
    finally:
        if owns:
            conn.close()


def run_daily(target_date, conn=None) -> dict:
    """daily 증분 유지: target_date 이후 로드된 배당이 있는 KRX 자산만 재계산한다.

    비배당일 신규 행은 total_return_close == adj_close 가 이미 정답(계수 1)이라
    건드릴 필요가 없다. 새 배당이 들어온 자산만 back-adjustment 앵커가 바뀌므로
    그 자산 전체 시계열을 스코프 재계산한다.
    """
    from pipeline.common import db

    owns = conn is None
    conn = conn or db.connect()
    try:
        assets = assets_with_recent_dividend_changes(conn, target_date)
        if not assets:
            return {"changed_dividend_assets": 0, "rows_updated": 0}
        stats = run(conn, dry_run=False, asset_ids=assets)
        stats["changed_dividend_assets"] = len(assets)
        return stats
    finally:
        if owns:
            conn.close()


if __name__ == "__main__":
    import json
    import sys

    print(json.dumps(run(dry_run="--apply" not in sys.argv), default=str))
