"""Bronze에서 재생 가능한 FMP Silver 파생행만 안전하게 초기화한다."""
from __future__ import annotations

from pipeline.common import db


def main() -> None:
    conn = db.connect()
    counts: dict[str, int] = {}
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TEMP TABLE _fmp_reset_assets ON COMMIT DROP AS
                    SELECT DISTINCT asset_id
                    FROM asset_identifier WHERE source='FMP'
                """)
                cur.execute("SELECT count(*) FROM _fmp_reset_assets")
                counts["assets"] = int(cur.fetchone()[0])
                cur.execute("""
                    SELECT count(*) FROM asset_identifier ai
                    JOIN _fmp_reset_assets t USING(asset_id)
                    WHERE ai.source <> 'FMP'
                """)
                foreign_identifiers = int(cur.fetchone()[0])
                cur.execute("""
                    SELECT
                      (SELECT count(*) FROM price_daily p
                       JOIN _fmp_reset_assets t USING(asset_id)
                       WHERE p.source NOT IN ('FMP','FMP_FX','FMP_COMMODITY')),
                      (SELECT count(*) FROM fundamental f
                       JOIN _fmp_reset_assets t USING(asset_id)
                       WHERE f.source <> 'FMP'),
                      (SELECT count(*) FROM corporate_action c
                       JOIN _fmp_reset_assets t USING(asset_id)
                       WHERE c.source NOT IN ('FMP_DIVIDEND','FMP_SPLIT'))
                """)
                foreign_facts = tuple(int(value) for value in cur.fetchone())
                cur.execute("""
                    SELECT count(*) FROM gold.factor_value g
                    JOIN _fmp_reset_assets t USING(asset_id)
                """)
                gold_references = int(cur.fetchone()[0])
                if foreign_identifiers or any(foreign_facts) or gold_references:
                    raise RuntimeError(
                        "FMP reset refused: cross-source references found "
                        f"identifiers={foreign_identifiers}, facts={foreign_facts}, "
                        f"gold={gold_references}"
                    )
                for table in ("price_daily", "fundamental", "corporate_action"):
                    cur.execute(
                        f"DELETE FROM {table} WHERE asset_id IN "
                        "(SELECT asset_id FROM _fmp_reset_assets)"
                    )
                    counts[table] = cur.rowcount
                cur.execute(
                    "DELETE FROM asset WHERE asset_id IN "
                    "(SELECT asset_id FROM _fmp_reset_assets)"
                )
                if cur.rowcount != counts["assets"]:
                    raise RuntimeError(
                        "FMP reset asset count changed during transaction: "
                        f"expected={counts['assets']}, deleted={cur.rowcount}"
                    )
        print(f"[silver-fmp] reset complete counts={counts}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
