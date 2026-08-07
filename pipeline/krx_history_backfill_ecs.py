"""ECS one-off: KRX 히스토리 전체 재구축 (Bronze 다운로드 → truncate → s3_backfill).

RDS가 사설망이라 로컬에서 직접 적재할 수 없으므로 VPC 내부 ECS one-off로 실행한다.
현재 s3_backfill은 전기간 일괄 빌드(adj_close 전시계열 대사)라 연도별 분할이 불가능하고
빈 Silver를 요구한다. 따라서 이 진입점은:

  1. marcap/krxapi/index/DART Bronze 전체를 /app/data로 내려받고
  2. (다운로드 성공 후) Silver 5개 테이블을 TRUNCATE 하고
  3. s3_backfill.run() 으로 1995~현재 전체를 재구축·인증한다.

파괴적 TRUNCATE는 반드시 실행 전에 RDS 스냅샷을 만든 뒤, ``--confirm REBUILD``
(또는 env ``KRX_HISTORY_REBUILD_CONFIRM=REBUILD``)를 명시했을 때만 수행한다.
스냅샷 생성은 이 태스크가 아니라 이를 트리거하는 워크플로/운영자가 책임진다.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3

from pipeline.common import db
from pipeline.silver_quality import s3_backfill

CONFIRM_TOKEN = "REBUILD"

# s3_backfill 의 _candidate_bundle 이 읽는 KRX/DART Bronze 전체.
BRONZE_PREFIXES = (
    "stock/marcap/",
    "stock/krxapi/",
    "index/krxapi/",
    "financials/dart/",
    "dividends/dart/",
    "corporate_actions/dart/",
)

# 재구축은 public.asset 을 새로 만들어 asset_id 가 재부여되므로, 이를 FK 로
# 참조하는 gold 값도 함께 무효화된다. CASCADE 로 정리한다.
SILVER_TABLES = (
    "public.asset",
    "public.asset_identifier",
    "public.price_daily",
    "public.fundamental",
    "public.corporate_action",
)


def _download_prefixes(bucket: str, root: Path, prefixes: tuple[str, ...]) -> int:
    s3 = boto3.client("s3")
    downloaded = 0
    for prefix in prefixes:
        prefix_count = 0
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix,
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith("/"):
                    continue
                destination = root / key
                destination.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(destination))
                downloaded += 1
                prefix_count += 1
                if downloaded % 500 == 0:
                    print(f"[krx-rebuild] downloaded={downloaded}", flush=True)
        print(
            f"[krx-rebuild] prefix={prefix} objects={prefix_count}",
            flush=True,
        )
    return downloaded


def _truncate_silver() -> None:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE "
                + ", ".join(SILVER_TABLES)
                + " RESTART IDENTITY CASCADE"
            )
        conn.commit()
        print(
            f"[krx-rebuild] truncated {', '.join(SILVER_TABLES)} (CASCADE)",
            flush=True,
        )
    finally:
        conn.close()


def _confirmed(confirm: str | None) -> bool:
    token = confirm or os.environ.get("KRX_HISTORY_REBUILD_CONFIRM")
    return token == CONFIRM_TOKEN


def run(*, confirm: str | None = None, resume: str | None = None) -> None:
    if not _confirmed(confirm):
        raise SystemExit(
            "refusing destructive rebuild without confirmation: pass "
            f"--confirm {CONFIRM_TOKEN} (or env KRX_HISTORY_REBUILD_CONFIRM="
            f"{CONFIRM_TOKEN}). Create an RDS snapshot first."
        )
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise SystemExit("S3_BRONZE_BUCKET is required")
    root = Path("/app/data")

    # 1) Bronze 전체를 먼저 확보한다. 다운로드가 실패하면 truncate 하지 않는다.
    count = _download_prefixes(bucket, root, BRONZE_PREFIXES)
    if count == 0:
        raise RuntimeError("no KRX/DART Bronze objects downloaded")
    print(f"[krx-rebuild] bronze download complete objects={count}", flush=True)

    # resume 은 이미 truncate 된 상태에서 중단분을 이어가는 경로다.
    if not resume:
        _truncate_silver()

    # 2) 전기간 일괄 재구축·인증.
    run_id = s3_backfill.run(src="local", resume=resume)
    print(f"[krx-rebuild] s3_backfill certified run={run_id}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        help=f"destructive rebuild guard; must be '{CONFIRM_TOKEN}'",
    )
    parser.add_argument(
        "--resume",
        help="기존 backfill_s3 dq_run UUID (truncate 를 건너뛰고 이어서 진행)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(confirm=args.confirm, resume=args.resume)


if __name__ == "__main__":
    main()
