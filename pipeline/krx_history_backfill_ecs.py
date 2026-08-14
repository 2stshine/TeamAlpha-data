"""ECS one-off: KRX 히스토리 전체 재구축 (Bronze 다운로드 → truncate → s3_backfill).

RDS가 사설망이라 로컬에서 직접 적재할 수 없으므로 VPC 내부 ECS one-off로 실행한다.
현재 s3_backfill은 전기간 일괄 빌드(adj_close 전시계열 대사)라 연도별 분할이 불가능하고
빈 Silver를 요구한다. 따라서 이 진입점은:

  1. marcap/krxapi/index/DART Bronze 전체를 /app/data로 내려받고
  2. (다운로드 성공 후) Silver 5개 테이블을 TRUNCATE 하고
  3. s3_backfill.run() 으로 1995~현재 KRX/DART 전체를 재구축·인증하고
  4. TRUNCATE 로 함께 지워졌지만 s3_backfill 이 복구하지 않는 나머지 소스를
     재적재해야 한다.

현재 total-return ``dart-extras``는 인증된 action만 복구하고 정기보고서
``DIVIDEND/REPORTED`` fundamental은 의도적으로 쓰지 않는다. 따라서 이 destructive
entrypoint는 그 전용 completeness/reload 계약이 생길 때까지 apply를 fail-closed로
차단한다. dry-run만 허용한다.

파괴적 TRUNCATE는 반드시 실행 전에 RDS 스냅샷을 만든 뒤, ``--confirm REBUILD``
(또는 env ``KRX_HISTORY_REBUILD_CONFIRM=REBUILD``)를 명시했을 때만 수행한다.
스냅샷 생성은 이 태스크가 아니라 이를 트리거하는 워크플로/운영자가 책임진다.
"""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3

from pipeline import dart_silver_backfill_ecs
from pipeline.common import db
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality import s3_backfill

_DOWNLOAD_WORKERS = 32

CONFIRM_TOKEN = "REBUILD"
# The full rebuild truncates ALL Silver tables, but s3_backfill only rebuilds
# KRX/DART. FMP lives in the same tables and must be reloaded afterwards or it
# is silently wiped. FMP price/financial bronze starts in 2015.
FMP_SILVER_FROM_YEAR = 2015

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
    # Bronze는 수십만 개의 작은 객체(DART JSON)라 순차 다운로드는 매우 느리다.
    # 키를 먼저 나열한 뒤 스레드풀로 병렬 다운로드한다(boto3 client는 스레드 안전).
    s3 = boto3.client("s3")
    keys: list[str] = []
    for prefix in prefixes:
        before = len(keys)
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix,
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith("/"):
                    keys.append(key)
        print(
            f"[krx-rebuild] listed prefix={prefix} objects={len(keys) - before}",
            flush=True,
        )
    total = len(keys)
    print(f"[krx-rebuild] downloading {total} objects "
          f"with {_DOWNLOAD_WORKERS} workers", flush=True)

    def _download(key: str) -> None:
        destination = root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(destination))

    downloaded = 0
    with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as executor:
        for future in as_completed(
            executor.submit(_download, key) for key in keys
        ):
            future.result()
            downloaded += 1
            if downloaded % 10000 == 0:
                print(
                    f"[krx-rebuild] downloaded={downloaded}/{total}",
                    flush=True,
                )
    return downloaded


def _truncate_silver() -> None:
    conn = db.connect()
    try:
        with conn.transaction():
            acquire_return_writer_transaction_lock(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE "
                    + ", ".join(SILVER_TABLES)
                    + " RESTART IDENTITY CASCADE"
                )
        print(
            f"[krx-rebuild] truncated {', '.join(SILVER_TABLES)} (CASCADE)",
            flush=True,
        )
    finally:
        conn.close()


def _confirmed(confirm: str | None) -> bool:
    token = confirm or os.environ.get("KRX_HISTORY_REBUILD_CONFIRM")
    return token == CONFIRM_TOKEN


def _dry_run() -> None:
    """후보 빌드 + 품질 게이트만 실행(트런케이트·publish·RDS 쓰기 없음).

    파괴적 재구축 전에 전체 실데이터로 게이트 통과 여부를 안전하게 확인한다.
    차단(Critical/Error FAIL)이 있으면 종료코드 1로 알린다.
    """
    from pipeline.common.paths import base_uri
    from pipeline.silver_quality import backfill as sq_backfill
    from pipeline.silver_quality.runner import evaluate, print_summary

    bundle = sq_backfill._candidate_bundle(base_uri("local"))
    results = evaluate(bundle) + sq_backfill._required_backfill_results(bundle)
    print_summary(results)
    print(
        "[krx-rebuild] DRY-RUN candidates: "
        f"assets={len(bundle.assets)} prices={len(bundle.prices)} "
        f"fundamentals={len(bundle.fundamentals)} actions={len(bundle.actions)}",
        flush=True,
    )
    blocking = [
        (r.rule_code, r.failed_count) for r in results if r.blocks_publish
    ]
    print(
        f"[krx-rebuild] DRY-RUN blocking={len(blocking)}: {blocking}",
        flush=True,
    )
    if blocking:
        raise SystemExit(1)
    print("[krx-rebuild] DRY-RUN clean: gate passes, safe to rebuild", flush=True)


def run(
    *,
    confirm: str | None = None,
    resume: str | None = None,
    dry_run: bool = False,
) -> None:
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise SystemExit("S3_BRONZE_BUCKET is required")
    if not dry_run and not _confirmed(confirm):
        raise SystemExit(
            "refusing destructive rebuild without confirmation: pass "
            f"--confirm {CONFIRM_TOKEN} (or env KRX_HISTORY_REBUILD_CONFIRM="
            f"{CONFIRM_TOKEN}). Create an RDS snapshot first."
        )
    if not dry_run:
        raise RuntimeError(
            "destructive KRX history rebuild is disabled: authenticated "
            "DART regular-report DIVIDEND/REPORTED fundamental reload is "
            "not implemented; refusing permanent data loss"
        )
    if dry_run:
        _run_locked(
            bucket=bucket,
            resume=resume,
            dry_run=True,
            certification_lock=None,
        )
        return
    certification_lock = (
        dart_silver_backfill_ecs.acquire_daily_certification_lock()
    )
    try:
        _run_locked(
            bucket=bucket,
            resume=resume,
            dry_run=False,
            certification_lock=certification_lock,
        )
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(
            certification_lock,
        )


def _run_locked(
    *,
    bucket: str,
    resume: str | None,
    dry_run: bool,
    certification_lock=None,
) -> None:
    root = Path("/app/data")

    # 1) Bronze 전체를 먼저 확보한다. 다운로드가 실패하면 truncate 하지 않는다.
    count = _download_prefixes(bucket, root, BRONZE_PREFIXES)
    if count == 0:
        raise RuntimeError("no KRX/DART Bronze objects downloaded")
    print(f"[krx-rebuild] bronze download complete objects={count}", flush=True)

    # dry-run 은 게이트만 검증하고 종료(비파괴).
    if dry_run:
        _dry_run()
        return

    dart_silver_backfill_ecs.assert_daily_certification_lock(
        certification_lock,
    )

    # resume 은 이미 truncate 된 상태에서 중단분을 이어가는 경로다.
    if not resume:
        _truncate_silver()

    # 2) 전기간 일괄 재구축·인증 (KRX/DART).
    run_id = s3_backfill.run(src="local", resume=resume)
    print(f"[krx-rebuild] s3_backfill certified run={run_id}", flush=True)

    # 3) TRUNCATE 로 함께 지워졌지만 s3_backfill(KRX/DART 주요계정·기업행사)이
    #    복구하지 않는 나머지 소스를 각자의 로더로 source-scoped 재적재한다.
    #    이 단계를 빼면 해당 데이터가 통째로 사라진다.
    #    - FMP: 미국주식·재무·기업행사·USDKRW·원자재
    #    - DART dart-extras: 정기보고서 배당(fundamental DIVIDEND/REPORTED) 등
    _reload_fmp()
    _reload_dart_extras(certification_lock)


def _reload_dart_extras(certification_lock=None) -> None:
    """Reload DART dividend-report fundamentals (DIVIDEND/REPORTED) etc.

    These come from dart_silver_backfill_ecs.run_dart_extras (dividends.py /
    alotMatter), not s3_backfill, so a full rebuild drops them otherwise.
    """
    print("[krx-rebuild] reloading DART dart-extras (DIVIDEND reports)", flush=True)
    # The encompassing destructive rebuild already holds the common session
    # epoch lock.  Re-entering through the public wrapper would use another DB
    # session and deadlock on ourselves.
    dart_silver_backfill_ecs._run_dart_extras_locked(certification_lock)
    print("[krx-rebuild] DART dart-extras reload complete", flush=True)


def _reload_fmp() -> None:
    """Reload FMP silver after a KRX/DART rebuild truncated the shared tables."""
    from datetime import date

    from pipeline import fmp_backfill_ecs

    to_year = date.today().year
    print(
        f"[krx-rebuild] reloading FMP silver {FMP_SILVER_FROM_YEAR}-{to_year}",
        flush=True,
    )
    fmp_backfill_ecs.run_silver_range(FMP_SILVER_FROM_YEAR, to_year)
    print("[krx-rebuild] FMP silver reload complete", flush=True)


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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="후보 빌드 + 품질 게이트만 실행(트런케이트·publish 없음)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(confirm=args.confirm, resume=args.resume, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
