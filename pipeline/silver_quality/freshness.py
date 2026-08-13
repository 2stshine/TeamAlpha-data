"""데이터 신선도(staleness) 감시.

파이프라인이 조용히 멈추면(스케줄러 중지·연속 실패) 최신 데이터가 정체된다.
소스별 max(trade_date)가 기대 lag 안에 있는지 확인해, 정체를 알린다.

- daily_full 이 끝에서 assert_fresh() 를 호출해 자기점검(로그 경고).
- 파이프라인이 아예 안 도는 경우까지 잡으려면 **독립 스케줄**로
  `python -m pipeline.silver_quality.freshness` 를 매일 돌린다(stale 이면 exit 1).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from pipeline.common import db

KST = timezone(timedelta(hours=9))

# 소스별 허용 lag(달력일). 주말·연휴를 감안해 여유를 둔다. FMP 는 설계상 KRX 보다
# 1거래일 늦으므로 더 크게 잡는다.
MAX_LAG_DAYS = {
    "KRX": 5,
    "FMP": 6,
    "FMP_COMMODITY": 6,
    "FMP_FX": 6,
}


def total_return_contract_report(conn) -> dict:
    """Public read-only readiness report for orchestration preflights."""
    return _total_return_contract_report(conn)


def evaluate(conn, *, as_of: date | None = None) -> dict:
    as_of = as_of or datetime.now(KST).date()
    with conn.cursor() as c:
        c.execute(
            "SELECT source, max(trade_date) FROM price_daily GROUP BY source"
        )
        max_dates = dict(c.fetchall())
    report = {}
    stale = []
    for source, limit in MAX_LAG_DAYS.items():
        md = max_dates.get(source)
        lag = (as_of - md).days if md else None
        is_stale = md is None or lag > limit
        report[source] = {
            "max_trade_date": str(md) if md else None,
            "lag_days": lag,
            "limit": limit,
            "stale": is_stale,
        }
        if is_stale:
            stale.append(source)
    return {"as_of": str(as_of), "sources": report, "stale_sources": stale}


def assert_fresh(conn=None, *, as_of: date | None = None) -> dict:
    """정체면 RuntimeError. daily 자기점검·독립 스케줄 공용."""
    owns = conn is None
    conn = conn or db.connect()
    try:
        result = evaluate(conn, as_of=as_of)
        conn.rollback()
    finally:
        if owns:
            conn.close()
    if result["stale_sources"]:
        raise RuntimeError(f"stale data sources: {json.dumps(result)}")
    return result


if __name__ == "__main__":
    import sys

    conn = db.connect()
    try:
        result = evaluate(conn)
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False))
    sys.exit(1 if result["stale_sources"] else 0)
