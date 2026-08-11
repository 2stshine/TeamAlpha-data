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
from pipeline.silver.return_contract import CONTRACT_RELEASE

KST = timezone(timedelta(hours=9))

# 소스별 허용 lag(달력일). 주말·연휴를 감안해 여유를 둔다. FMP 는 설계상 KRX 보다
# 1거래일 늦으므로 더 크게 잡는다.
MAX_LAG_DAYS = {
    "KRX": 5,
    "FMP": 6,
    "FMP_COMMODITY": 6,
    "FMP_FX": 6,
}

KRX_TOTAL_RETURN_SOURCE = "KRX_TOTAL_RETURN"
KRX_TOTAL_RETURN_METHODOLOGY = "krx_gross_dividend_reinvested_v3"
KRX_TOTAL_RETURN_COVERAGE_START = date(2015, 1, 1)


def _total_return_contract_report(conn) -> dict:
    """Return a cheap fail-closed readiness check for the certified KRX label."""
    with conn.cursor() as c:
        c.execute(
            """
            WITH certified_common_stock_prices AS (
                SELECT min(p.trade_date) AS first_trade,
                       max(p.trade_date) AS last_trade
                FROM price_daily p
                JOIN asset a ON a.asset_id=p.asset_id
                JOIN dq_run price_q ON price_q.run_id=p.quality_run_id
                WHERE p.source='KRX'
                  AND a.asset_type='stock'
                  AND a.instrument_type='common_stock'
                  AND a.exchange='KRX'
                  AND p.market IN ('KOSPI','KOSDAQ')
                  AND price_q.status='CERTIFIED'
                  AND p.trade_date >= %s
            )
            SELECT c.status,
                   c.methodology_version,
                   c.coverage_start,
                   c.coverage_end,
                   c.quality_run_id,
                   c.metadata->>'contract_release',
                   q.status,
                   q.mode,
                   prices.first_trade,
                   prices.last_trade
            FROM price_return_contract c
            LEFT JOIN dq_run q ON q.run_id=c.quality_run_id
            CROSS JOIN certified_common_stock_prices prices
            WHERE c.source='KRX'
              AND c.asset_type='stock'
              AND c.field_name='total_return_close'
            """,
            (KRX_TOTAL_RETURN_COVERAGE_START,),
        )
        row = c.fetchone()

    if row is None:
        return {
            "status": None,
            "methodology_version": None,
            "contract_release": None,
            "coverage_start": None,
            "coverage_end": None,
            "quality_run_id": None,
            "dq_status": None,
            "dq_mode": None,
            "first_certified_trade": None,
            "last_certified_trade": None,
            "ready": False,
            "issues": ["missing price_return_contract row"],
        }

    (
        status,
        methodology,
        coverage_start,
        coverage_end,
        quality_run_id,
        contract_release,
        dq_status,
        dq_mode,
        first_certified_trade,
        last_certified_trade,
    ) = row
    issues = []
    if status != "CERTIFIED":
        issues.append(f"status={status!r}, expected 'CERTIFIED'")
    if methodology != KRX_TOTAL_RETURN_METHODOLOGY:
        issues.append(
            f"methodology={methodology!r}, expected "
            f"{KRX_TOTAL_RETURN_METHODOLOGY!r}"
        )
    if contract_release != CONTRACT_RELEASE:
        issues.append(
            f"contract_release={contract_release!r}, expected "
            f"{CONTRACT_RELEASE!r}"
        )
    if first_certified_trade is None or last_certified_trade is None:
        issues.append("certified KRX common-stock price coverage is empty")
    if coverage_start is None or coverage_end is None:
        issues.append("contract coverage bounds are missing")
    if coverage_start != first_certified_trade:
        issues.append(
            f"coverage_start={coverage_start!r}, expected "
            f"first certified trade {first_certified_trade!r}"
        )
    if coverage_end != last_certified_trade:
        issues.append(
            f"coverage_end={coverage_end!r}, expected last certified trade "
            f"{last_certified_trade!r}"
        )
    if quality_run_id is None:
        issues.append("quality_run_id is missing")
    if dq_status != "CERTIFIED":
        issues.append(f"dq_status={dq_status!r}, expected 'CERTIFIED'")
    if dq_mode != "krx_total_return_rebuild":
        issues.append(
            f"dq_mode={dq_mode!r}, expected 'krx_total_return_rebuild'"
        )

    return {
        "status": status,
        "methodology_version": methodology,
        "contract_release": contract_release,
        "coverage_start": str(coverage_start) if coverage_start else None,
        "coverage_end": str(coverage_end) if coverage_end else None,
        "quality_run_id": str(quality_run_id) if quality_run_id else None,
        "dq_status": dq_status,
        "dq_mode": dq_mode,
        "first_certified_trade": (
            str(first_certified_trade) if first_certified_trade else None
        ),
        "last_certified_trade": (
            str(last_certified_trade) if last_certified_trade else None
        ),
        "ready": not issues,
        "issues": issues,
    }


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
    total_return = _total_return_contract_report(conn)
    report[KRX_TOTAL_RETURN_SOURCE] = total_return
    if not total_return["ready"]:
        stale.append(KRX_TOTAL_RETURN_SOURCE)
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
