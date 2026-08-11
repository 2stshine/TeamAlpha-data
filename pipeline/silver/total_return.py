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

def _load_krx(conn):
    import pandas as pd
    with conn.cursor() as c:
        c.execute(
            "SELECT asset_id, trade_date, close, adj_close, total_return_close "
            "FROM price_daily WHERE source='KRX'"
        )
        prices = pd.DataFrame(
            c.fetchall(),
            columns=["asset_id", "trade_date", "close", "adj_close",
                     "current_trc"],
        )
        c.execute(
            "SELECT asset_id, ex_date, cash_amount FROM corporate_action "
            "WHERE action_type='cash_dividend' AND cash_amount IS NOT NULL "
            "AND ex_date IS NOT NULL"
        )
        dividends = pd.DataFrame(
            c.fetchall(), columns=["asset_id", "ex_date", "cash_amount"],
        )
    for col in ("close", "adj_close", "current_trc"):
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    dividends["cash_amount"] = pd.to_numeric(
        dividends["cash_amount"], errors="coerce")
    return prices, dividends


def run(conn=None, *, dry_run: bool = True) -> dict:
    """KRX total_return_close 를 배당 재투자로 재계산한다.

    dry_run=True: 통계만 반환(쓰기 없음). False: 값이 바뀌는 행만 UPDATE 한다.
    total_return_close 한 컬럼만 건드리므로 truncate/reload 는 없다.
    """
    import pandas as pd
    from pipeline.common import db

    owns = conn is None
    conn = conn or db.connect()
    try:
        prices, dividends = _load_krx(conn)
        new_trc = compute_total_return_close(
            prices[["asset_id", "trade_date", "close", "adj_close"]],
            dividends,
            group_keys=["asset_id"],
        )
        prices["new_trc"] = new_trc.to_numpy()
        div_assets = set(dividends.dropna(subset=["cash_amount"])["asset_id"])
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
            "dividend_assets": int(len(div_assets)),
            "dividend_events": int(len(dividends.dropna(subset=["cash_amount"]))),
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


if __name__ == "__main__":
    import json
    import sys

    print(json.dumps(run(dry_run="--apply" not in sys.argv), default=str))
