"""asset + asset_identifier 후보 생성 및 publish (bronze → silver).

종목 유니버스: marcap + krxapi 전 날짜 union(상폐 포함) → 종목별 asset + KRX 티커 identifier.
DART corp_code: bronze corpCode.xml 로 티커→corp_code 매핑해 DART identifier 도 기록(있으면).
지수: 벤치마크(코스피200·코스닥150)를 asset_type='index' 로 등록, KRX 지수코드 identifier.

후보 생성 단계는 DB를 변경하지 않는다. publish는 상위 quality transaction 안에서 실행한다.
"""
from __future__ import annotations

import glob

import pandas as pd
from uuid import UUID

from pipeline.bronze import financials

# IDX_NM → (asset name, KRX 지수코드)
BENCHMARKS = {"코스피 200": ("KOSPI200", "1028"), "코스닥 150": ("KOSDAQ150", "2203")}


def _stock_universe(base: str) -> dict[str, str]:
    """marcap + krxapi 전 날짜 union → {ticker: name} (최근 이름 우선, 상폐 포함)."""
    names: dict[str, str] = {}
    for f in sorted(glob.glob(f"{base}/stock/marcap/date=*/all.parquet")):
        df = pd.read_parquet(f, columns=["Code", "Name"])
        names.update(zip(df["Code"].astype(str), df["Name"].astype(str)))
    for f in sorted(glob.glob(f"{base}/stock/krxapi/date=*/*.parquet")):
        df = pd.read_parquet(f, columns=["ISU_CD", "ISU_NM"])
        names.update(zip(df["ISU_CD"].astype(str), df["ISU_NM"].astype(str)))
    return names


def prepare(base: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Bronze에서 asset/identifier 후보를 만든다.

    natural_key는 현재 KRX ticker다. identifier 충돌은 quality rule과 publish 양쪽에서 차단한다.
    """
    names = _stock_universe(base)
    corp = {sc: cc for cc, sc in financials.load_listed_corps_from_bronze(base)}  # ticker→corp_code

    asset_rows = [
        {
            "natural_key": ticker,
            "name": name,
            "asset_type": "stock",
            "exchange": "KRX",
            "currency": "KRW",
            "source_file": None,
        }
        for ticker, name in names.items()
    ]
    identifier_rows = [
        {
            "natural_key": ticker,
            "source": "KRX",
            "identifier": ticker,
            "source_file": None,
        }
        for ticker in names
    ]
    identifier_rows.extend(
        {
            "natural_key": ticker,
            "source": "DART",
            "identifier": corp_code,
            "source_file": "financials/dart/corpCode.xml",
        }
        for ticker, corp_code in corp.items()
        if ticker in names
    )
    for _, (name, code) in BENCHMARKS.items():
        asset_rows.append({
            "natural_key": code,
            "name": name,
            "asset_type": "index",
            "exchange": "KRX",
            "currency": "KRW",
            "source_file": None,
        })
        identifier_rows.append({
            "natural_key": code,
            "source": "KRX",
            "identifier": code,
            "source_file": None,
        })
    return pd.DataFrame(asset_rows), pd.DataFrame(identifier_rows)


def restrict_to_price_universe(
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    supported_price_identifiers: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """지원 시장 가격이 있는 주식과 벤치마크 지수만 후보로 유지한다."""
    supported = {str(value) for value in supported_price_identifiers}
    keep = (
        asset_candidates["asset_type"].eq("index")
        | asset_candidates["natural_key"].astype(str).isin(supported)
    )
    retained_assets = asset_candidates[keep].reset_index(drop=True)
    retained_keys = set(retained_assets["natural_key"].astype(str))
    retained_identifiers = identifier_candidates[
        identifier_candidates["natural_key"].astype(str).isin(retained_keys)
    ].reset_index(drop=True)
    return retained_assets, retained_identifiers


def publish(
    conn,
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    quality_run_id: UUID,
) -> dict[str, int]:
    """후보를 현재 transaction에 반영하고 KRX identifier→asset_id를 반환한다."""
    with conn.cursor() as cur:
        cur.execute("SELECT identifier, asset_id FROM asset_identifier WHERE source='KRX'")
        krx_map = dict(cur.fetchall())
        natural_to_id: dict[str, int] = {}
        for row in asset_candidates.itertuples(index=False):
            natural_key = str(row.natural_key)
            if natural_key in krx_map:
                natural_to_id[natural_key] = krx_map[natural_key]
                cur.execute(
                    """
                    UPDATE asset
                    SET name=%s, asset_type=%s, exchange=%s, currency=%s,
                        quality_run_id=%s, loaded_at=now()
                    WHERE asset_id=%s
                    """,
                    (
                        row.name, row.asset_type, row.exchange, row.currency,
                        quality_run_id, krx_map[natural_key],
                    ),
                )
                continue
            cur.execute(
                """
                INSERT INTO asset (
                    name, asset_type, exchange, currency, quality_run_id, loaded_at
                ) VALUES (%s,%s,%s,%s,%s,now()) RETURNING asset_id
                """,
                (row.name, row.asset_type, row.exchange, row.currency, quality_run_id),
            )
            aid = cur.fetchone()[0]
            natural_to_id[natural_key] = aid

        for row in identifier_candidates.itertuples(index=False):
            aid = natural_to_id.get(str(row.natural_key)) or krx_map.get(str(row.natural_key))
            if aid is None:
                continue
            cur.execute(
                "SELECT asset_id FROM asset_identifier WHERE source=%s AND identifier=%s",
                (row.source, str(row.identifier)),
            )
            existing = cur.fetchone()
            if existing and existing[0] != aid:
                raise RuntimeError(
                    "identifier mapping conflict: "
                    f"source={row.source}, identifier={row.identifier}, "
                    f"existing_asset_id={existing[0]}, candidate_asset_id={aid}"
                )
            cur.execute(
                """
                INSERT INTO asset_identifier (
                    asset_id, source, identifier, quality_run_id, loaded_at
                ) VALUES (%s,%s,%s,%s,now())
                ON CONFLICT (source, identifier) DO UPDATE
                SET quality_run_id=EXCLUDED.quality_run_id, loaded_at=now()
                """,
                (aid, row.source, str(row.identifier), quality_run_id),
            )

        cur.execute("SELECT identifier, asset_id FROM asset_identifier WHERE source='KRX'")
        krx_map = dict(cur.fetchall())
    print(f"[assets] asset candidates={len(asset_candidates)}, KRX identifiers={len(krx_map)}")
    return krx_map


def build(conn, base: str, quality_run_id: UUID | None = None) -> dict[str, int]:
    """호환 wrapper. 새 적재 코드는 prepare/publish를 직접 사용한다."""
    if quality_run_id is None:
        raise RuntimeError("assets.build requires quality_run_id; use silver.load")
    a, i = prepare(base)
    return publish(conn, a, i, quality_run_id)
