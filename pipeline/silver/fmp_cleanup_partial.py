"""참조 사실이 없는 FMP 전용 부분 자산만 제거해 재생 가능한 백필을 초기화한다."""
from __future__ import annotations

from pipeline.common import db


def main() -> None:
    conn = db.connect()
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT count(*)
                    FROM asset a
                    WHERE EXISTS (
                        SELECT 1 FROM asset_identifier ai
                        WHERE ai.asset_id=a.asset_id AND ai.source='FMP'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM asset_identifier ai
                        WHERE ai.asset_id=a.asset_id AND ai.source<>'FMP'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM price_daily p WHERE p.asset_id=a.asset_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM fundamental f WHERE f.asset_id=a.asset_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM corporate_action c WHERE c.asset_id=a.asset_id
                    )
                """)
                eligible = int(cur.fetchone()[0])
                cur.execute("""
                    DELETE FROM asset a
                    WHERE EXISTS (
                        SELECT 1 FROM asset_identifier ai
                        WHERE ai.asset_id=a.asset_id AND ai.source='FMP'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM asset_identifier ai
                        WHERE ai.asset_id=a.asset_id AND ai.source<>'FMP'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM price_daily p WHERE p.asset_id=a.asset_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM fundamental f WHERE f.asset_id=a.asset_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM corporate_action c WHERE c.asset_id=a.asset_id
                    )
                """)
                deleted = cur.rowcount
                if deleted != eligible:
                    raise RuntimeError(
                        f"partial cleanup changed during transaction: "
                        f"eligible={eligible}, deleted={deleted}"
                    )
        print(f"[silver-fmp] removed partial FMP-only assets={deleted}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
