"""운영 Silver에서 KONEX 범위를 원자적으로 제거한다.

KONEX 가격 행은 모두 제거한다. 전체 가격 이력이 KONEX뿐인 주식은 asset,
identifier, fundamental도 함께 제거한다. KOSPI/KOSDAQ 이력이 있는 이전상장
종목은 자산과 지원 시장 기간을 유지한다.
"""
from __future__ import annotations

import argparse
import json

from pipeline.common import db
from pipeline.silver_quality import repository
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality.backfill import PUBLISH_LOCK_ID
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)


def _prepare_scope(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE _konex_only_asset (
                asset_id BIGINT PRIMARY KEY
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO _konex_only_asset(asset_id)
            SELECT a.asset_id
            FROM asset a
            WHERE a.asset_type='stock'
              AND EXISTS (
                  SELECT 1 FROM price_daily p
                  WHERE p.asset_id=a.asset_id AND p.market='KONEX'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM price_daily p
                  WHERE p.asset_id=a.asset_id
                    AND p.market IN ('KOSPI','KOSDAQ')
              )
            """
        )
        cur.execute(
            """
            SELECT
                (SELECT count(*) FROM price_daily WHERE market='KONEX'),
                (SELECT count(*) FROM _konex_only_asset),
                (SELECT count(*) FROM asset_identifier
                 WHERE asset_id IN (SELECT asset_id FROM _konex_only_asset)),
                (SELECT count(*) FROM fundamental
                 WHERE asset_id IN (SELECT asset_id FROM _konex_only_asset))
            """
        )
        row = cur.fetchone()
    return {
        "price_daily": int(row[0]),
        "asset": int(row[1]),
        "asset_identifier": int(row[2]),
        "fundamental": int(row[3]),
    }


def run(*, apply: bool = False) -> dict[str, int]:
    conn = db.connect()
    context = None
    try:
        repository.assert_schema(conn)
        if not apply:
            with conn.transaction():
                counts = _prepare_scope(conn)
            print(json.dumps({"mode": "dry-run", **counts}, ensure_ascii=False))
            return counts

        context = repository.start_run(
            conn,
            mode="maintenance_konex_exclusion",
            status="RUNNING",
        )
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (PUBLISH_LOCK_ID,),
                )
            acquire_return_writer_transaction_lock(conn)
            counts = _prepare_scope(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM fundamental
                    WHERE asset_id IN (
                        SELECT asset_id FROM _konex_only_asset
                    )
                    """
                )
                deleted_fundamental = cur.rowcount
                cur.execute("DELETE FROM price_daily WHERE market='KONEX'")
                deleted_prices = cur.rowcount
                cur.execute(
                    """
                    DELETE FROM asset_identifier
                    WHERE asset_id IN (
                        SELECT asset_id FROM _konex_only_asset
                    )
                    """
                )
                deleted_identifiers = cur.rowcount
                cur.execute(
                    """
                    DELETE FROM asset
                    WHERE asset_id IN (
                        SELECT asset_id FROM _konex_only_asset
                    )
                    """
                )
                deleted_assets = cur.rowcount
                cur.execute(
                    "SELECT count(*) FROM price_daily WHERE market='KONEX'"
                )
                remaining_konex = int(cur.fetchone()[0])

            actual = {
                "price_daily": deleted_prices,
                "asset": deleted_assets,
                "asset_identifier": deleted_identifiers,
                "fundamental": deleted_fundamental,
                "remaining_konex_price_rows": remaining_konex,
            }
            matched = (
                deleted_prices == counts["price_daily"]
                and deleted_assets == counts["asset"]
                and deleted_identifiers == counts["asset_identifier"]
                and deleted_fundamental == counts["fundamental"]
                and remaining_konex == 0
            )
            result = CheckResult(
                rule_code="KONEX_SILVER_EXCLUSION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.PASS if matched else CheckStatus.FAIL,
                expected=(
                    "all KONEX price rows and pure-KONEX dependent rows "
                    "are removed exactly"
                ),
                actual=json.dumps(actual, ensure_ascii=False, sort_keys=True),
                failed_count=0 if matched else 1,
            )
            if not matched:
                raise RuntimeError(result.actual)
            repository.finish_run(
                conn,
                context,
                "CERTIFIED",
                [result],
                commit=False,
            )
        print(json.dumps({"mode": "apply", **actual}, ensure_ascii=False))
        return actual
    except Exception as exc:
        conn.rollback()
        if context is not None:
            failure = CheckResult(
                rule_code="KONEX_SILVER_EXCLUSION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic KONEX exclusion",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn,
                context,
                "FAILED",
                [failure],
                error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 운영 Silver에서 제외한다. 생략하면 건수만 조회한다.",
    )
    args = parser.parse_args()
    run(apply=args.apply)


if __name__ == "__main__":
    main()
