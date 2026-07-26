"""silver(PostgreSQL/RDS) 접속 + 대량 upsert 헬퍼."""
from __future__ import annotations

import json
import os

import psycopg


def database_url() -> str:
    """운영 secret을 우선하고, ECS에서는 주입된 URL을 그대로 사용한다."""
    secret_id = os.environ.get("SILVER_DB_SECRET_ID")
    if secret_id:
        import boto3

        secret = boto3.client("secretsmanager").get_secret_value(
            SecretId=secret_id,
        )
        payload = json.loads(secret["SecretString"])
        url = payload.get("SILVER_DB_URL")
        if not url:
            raise SystemExit(
                f"Secrets Manager {secret_id!r}에 SILVER_DB_URL이 없습니다."
            )
        return url

    url = os.environ.get("SILVER_DB_URL")
    if not url:
        raise SystemExit(
            "SILVER_DB_SECRET_ID 또는 SILVER_DB_URL 환경변수가 없습니다 (.env)"
        )
    return url


def connect():
    # 장시간 backfill 중 RDS 재시작·네트워크 단절이 생겨도 죽은 TCP 연결에서
    # 무기한 기다리지 않는다. 실패한 파티션은 영구 staging을 이용해 resume한다.
    return psycopg.connect(
        database_url(),
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        tcp_user_timeout=60_000,
    )


def upsert(conn, table: str, columns: list[str], rows: list[tuple],
           conflict: list[str], update: list[str], *, temp_name: str = "_stg") -> int:
    """rows → 임시테이블 COPY → INSERT ... ON CONFLICT DO UPDATE.

    트랜잭션 경계는 호출자가 소유한다. 품질 검사 실패 시 Silver 변경 전체를 rollback할 수
    있도록 이 함수에서는 commit하지 않는다.
    """
    rows = list(rows)
    if not rows:
        return 0
    cols = ", ".join(columns)
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {temp_name} "
            f"(LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with cur.copy(f"COPY {temp_name} ({cols}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)
        if update:
            action = "DO UPDATE SET " + ", ".join(f"{c}=EXCLUDED.{c}" for c in update)
        else:
            action = "DO NOTHING"
        cur.execute(
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {temp_name} "
            f"ON CONFLICT ({', '.join(conflict)}) {action}"
        )
    return len(rows)
