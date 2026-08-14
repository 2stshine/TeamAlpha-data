"""Legacy S3-candidate Silver backfill (direct CLI disabled).

Bronze 전체를 ECS에서 검사하고 통과 후보를 S3 Parquet으로 고정한다.
RDS에는 quality_stage를 누적하지 않고 최종 Silver만 bounded transaction으로 적재한다.
The retained implementation does not close the derived total-return contract,
so ``main`` fails before argument parsing, base resolution, S3, or DB access.
"""
from __future__ import annotations

import argparse
import gc
import os
from uuid import UUID

import pandas as pd

from pipeline.common import db
from pipeline.silver import assets, corporate_actions, financials, prices
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.backfill import (
    PUBLISH_LOCK_ID,
    _asset_stage_frames,
    _candidate_bundle,
    _fingerprint,
    _fundamental_stage_frame,
    _price_stage_frame,
    _required_backfill_results,
)
from pipeline.silver_quality.candidate_store import CandidatePart, CandidateStore
from pipeline.silver_quality.models import CandidateBundle, QualityGateError
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary


DIRECT_CLI_DISABLED_MESSAGE = (
    "direct S3 Silver quality backfill CLI is disabled: it can leave the "
    "KRX total-return contract BUILDING without closed recertification"
)


def _store_for(run_id: UUID) -> CandidateStore:
    explicit = os.environ.get("S3_CANDIDATE_ROOT")
    if explicit:
        root = f"{explicit.rstrip('/')}/run={run_id}"
    else:
        bucket = os.environ.get("S3_BRONZE_BUCKET")
        if not bucket:
            raise RuntimeError("S3_BRONZE_BUCKET or S3_CANDIDATE_ROOT is required")
        root = f"s3://{bucket}/quality/candidates/silver-backfill/run={run_id}"
    return CandidateStore(root)


def _record_candidate(
    conn,
    context,
    store: CandidateStore,
    *,
    dataset: str,
    partition_key: str,
    frame: pd.DataFrame,
    bundle: CandidateBundle,
    fingerprint: str,
) -> CandidatePart:
    existing = store.metadata(dataset, partition_key, fingerprint)
    if existing is not None:
        # 객체 내용도 확인해 marker만 남은 불완전 업로드를 차단한다.
        store.load(existing)
        print(
            f"[s3-backfill] reuse candidate={partition_key} "
            f"rows={existing.row_count}",
            flush=True,
        )
        return existing

    child = repository.start_run(
        conn,
        mode="backfill_candidate",
        status="RUNNING",
        parent_run_id=context.run_id,
        partition_key=partition_key,
    )
    results = evaluate(bundle, partition_key=partition_key)
    print_summary(results)
    try:
        assert_publishable(results)
        part = store.save(dataset, partition_key, frame, fingerprint)
        repository.save_metrics(conn, child.run_id, bundle)
        repository.finish_run(conn, child, "CERTIFIED", results)
        print(
            f"[s3-backfill] certified candidate={partition_key} "
            f"rows={len(frame)} sha256={part.sha256[:12]}",
            flush=True,
        )
        return part
    except QualityGateError as exc:
        conn.rollback()
        repository.finish_run(
            conn, child, "FAILED", results, error_message=str(exc),
        )
        raise


def _candidate_parts(
    conn,
    context,
    store: CandidateStore,
    bundle: CandidateBundle,
    fingerprint: str,
) -> list[CandidatePart]:
    parts: list[CandidatePart] = []
    asset_frame, identifier_frame = _asset_stage_frames(bundle)
    asset_bundle = CandidateBundle(
        assets=bundle.assets,
        identifiers=bundle.identifiers,
    )
    parts.append(_record_candidate(
        conn, context, store, dataset="asset", partition_key="asset:all",
        frame=asset_frame, bundle=asset_bundle, fingerprint=fingerprint,
    ))
    parts.append(_record_candidate(
        conn, context, store, dataset="asset_identifier",
        partition_key="identifier:all", frame=identifier_frame,
        bundle=asset_bundle, fingerprint=fingerprint,
    ))

    for year, frame in bundle.prices.groupby(
        bundle.prices["trade_date"].map(lambda value: value.year),
        sort=True,
    ):
        frame = frame.reset_index(drop=True)
        partition_key = f"price:year={year}"
        price_bundle = CandidateBundle(
            assets=bundle.assets,
            identifiers=bundle.identifiers,
            prices=frame,
            actions=bundle.actions,
            stats={
                "price_daily": {
                    "input_rows": len(frame),
                    "transformed_rows": len(frame),
                    "excluded_rows": 0,
                    "rejected_rows": 0,
                },
                "corporate_action": bundle.stats.get(
                    "corporate_action", {}
                ),
            },
        )
        parts.append(_record_candidate(
            conn, context, store, dataset="price_daily",
            partition_key=partition_key, frame=_price_stage_frame(frame),
            bundle=price_bundle, fingerprint=fingerprint,
        ))

    groups = bundle.fundamentals.groupby(
        [
            bundle.fundamentals["period_end"].map(lambda value: value.year),
            "fiscal_period",
        ],
        sort=True,
    )
    for (year, fiscal_period), frame in groups:
        frame = frame.reset_index(drop=True)
        partition_key = f"fundamental:year={year}:period={fiscal_period}"
        fundamental_bundle = CandidateBundle(
            assets=bundle.assets,
            identifiers=bundle.identifiers,
            fundamentals=frame,
        )
        parts.append(_record_candidate(
            conn, context, store, dataset="fundamental",
            partition_key=partition_key,
            frame=_fundamental_stage_frame(frame),
            bundle=fundamental_bundle, fingerprint=fingerprint,
        ))
    if not bundle.actions.empty:
        parts.append(_record_candidate(
            conn,
            context,
            store,
            dataset="corporate_action",
            partition_key="corporate_action:all",
            frame=bundle.actions,
            bundle=CandidateBundle(
                assets=bundle.assets,
                identifiers=bundle.identifiers,
                actions=bundle.actions,
                stats={"corporate_action": bundle.stats.get("corporate_action", {})},
            ),
            fingerprint=fingerprint,
        ))
    return parts


def _reload_bundle(
    store: CandidateStore,
    parts: list[CandidatePart],
    stats: dict,
) -> CandidateBundle:
    grouped: dict[str, list[pd.DataFrame]] = {}
    for part in parts:
        grouped.setdefault(part.dataset, []).append(store.load(part))

    def combined(dataset: str) -> pd.DataFrame:
        frames = grouped.get(dataset, [])
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    return CandidateBundle(
        assets=combined("asset"),
        identifiers=combined("asset_identifier"),
        prices=combined("price_daily"),
        fundamentals=combined("fundamental"),
        actions=combined("corporate_action"),
        stats=stats,
    )


def _only_current_run_or_empty(conn, run_id: UUID) -> None:
    with conn.cursor() as cur:
        for table in (
            "asset", "asset_identifier", "price_daily", "fundamental",
            "corporate_action",
        ):
            cur.execute(
                f"SELECT count(*), count(*) FILTER "
                f"(WHERE quality_run_id=%s) FROM {table}",
                (run_id,),
            )
            total, current = cur.fetchone()
            if total != current:
                raise RuntimeError(
                    f"Silver contains rows from another run: "
                    f"table={table}, total={total}, current_run={current}"
                )


def _set_autovacuum(conn, enabled: bool) -> None:
    value = "true" if enabled else "false"
    with conn.cursor() as cur:
        for table in (
            "asset", "asset_identifier", "price_daily", "fundamental",
            "corporate_action",
        ):
            cur.execute(
                f"ALTER TABLE {table} SET "
                f"(autovacuum_enabled={value}, toast.autovacuum_enabled={value})"
            )
    conn.commit()


def _year_complete(conn, table: str, run_id: UUID, year: int, expected: int) -> bool:
    date_column = "trade_date" if table == "price_daily" else "period_end"
    # psycopg의 기본 autocommit=False에서는 SELECT도 transaction을 연다.
    # 이 transaction을 닫지 않으면 뒤의 ``conn.transaction()``이 savepoint가
    # 되고, 연도 종료 시 연결을 닫을 때 월별 publish 전체가 rollback될 수 있다.
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {table} "
                f"WHERE quality_run_id=%s "
                f"AND EXTRACT(YEAR FROM {date_column})=%s",
                (run_id, year),
            )
            complete = cur.fetchone()[0] == expected
    return complete


def _publish(
    context,
    store: CandidateStore,
    parts: list[CandidatePart],
    bundle: CandidateBundle,
    results,
) -> None:
    lock_conn = db.connect()
    conn = db.connect()
    autovacuum_disabled = False
    try:
        repository.assert_schema(lock_conn)
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (PUBLISH_LOCK_ID,))
        lock_conn.commit()
        repository.assert_schema(conn)
        _only_current_run_or_empty(conn, context.run_id)
        _set_autovacuum(conn, False)
        autovacuum_disabled = True

        with conn.transaction():
            acquire_return_writer_transaction_lock(conn)
            krx_map = assets.publish(
                conn, bundle.assets, bundle.identifiers, context.run_id,
            )

        price_parts = [part for part in parts if part.dataset == "price_daily"]
        for part in price_parts:
            frame = store.load(part)
            year = int(part.partition_key.split("year=")[1])
            if _year_complete(conn, "price_daily", context.run_id, year, len(frame)):
                print(f"[s3-backfill] reuse RDS price year={year}", flush=True)
                continue
            frame["_month"] = frame["trade_date"].map(
                lambda value: (value.year, value.month)
            )
            for (chunk_year, month), chunk in frame.groupby("_month", sort=True):
                chunk = chunk.drop(columns="_month")
                with conn.transaction():
                    prices.publish(
                        conn, chunk, krx_map, context.run_id,
                    )
                print(
                    f"[s3-backfill] published price={chunk_year}-{month:02d} "
                    f"rows={len(chunk)}",
                    flush=True,
                )
            conn.close()
            conn = db.connect()

        fundamental_parts = [
            part for part in parts if part.dataset == "fundamental"
        ]
        for part in fundamental_parts:
            frame = store.load(part)
            year = int(part.partition_key.split("year=")[1].split(":")[0])
            if _year_complete(
                conn, "fundamental", context.run_id, year,
                sum(
                    item.row_count for item in fundamental_parts
                    if f"year={year}:" in item.partition_key
                ),
            ):
                print(f"[s3-backfill] reuse RDS fundamental year={year}", flush=True)
                continue
            with conn.transaction():
                financials.publish(
                    conn, frame, krx_map, context.run_id,
                    replace_scopes=False,
                )
            print(
                f"[s3-backfill] published {part.partition_key} rows={len(frame)}",
                flush=True,
            )
            conn.close()
            conn = db.connect()

        with conn.transaction():
            corporate_actions.publish(
                conn, bundle.actions, krx_map, context.run_id,
            )

        expected = {
            "asset": len(bundle.assets),
            "asset_identifier": len(bundle.identifiers),
            "price_daily": len(bundle.prices),
            "fundamental": len(bundle.fundamentals),
            "corporate_action": len(bundle.actions),
        }
        with conn.transaction():
            with conn.cursor() as cur:
                for table, count in expected.items():
                    cur.execute(
                        f"SELECT count(*), count(*) FILTER "
                        f"(WHERE quality_run_id=%s) FROM {table}",
                        (context.run_id,),
                    )
                    total, current = cur.fetchone()
                    if total != count or current != count:
                        raise RuntimeError(
                            f"publish reconciliation failed {table}: "
                            f"expected={count}, total={total}, current={current}"
                        )
            repository.save_metrics(conn, context.run_id, bundle)
            repository.finish_run(
                conn, context, "CERTIFIED", results, commit=False,
            )
        _set_autovacuum(conn, True)
        autovacuum_disabled = False
        print(f"[s3-backfill] CERTIFIED run={context.run_id}", flush=True)
    finally:
        if autovacuum_disabled:
            try:
                if getattr(conn, "closed", False):
                    conn = db.connect()
                else:
                    conn.rollback()
                _set_autovacuum(conn, True)
            except Exception as exc:
                print(
                    "[s3-backfill] WARNING failed to restore autovacuum: "
                    f"{exc}",
                    flush=True,
                )
        try:
            with lock_conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (PUBLISH_LOCK_ID,))
            lock_conn.commit()
        except Exception:
            pass
        conn.close()
        lock_conn.close()


def run(src: str = "local", resume: str | None = None) -> UUID:
    from pipeline.common.paths import base_uri

    base = base_uri(src)
    conn = db.connect()
    context = None
    recorded_results = []
    try:
        repository.assert_schema(conn)
        fingerprint = _fingerprint(base)
        if resume:
            context = repository.get_run(conn, UUID(resume))
            if context.mode != "backfill_s3":
                raise ValueError(f"not an S3 candidate backfill run: {resume}")
            if context.input_fingerprint != fingerprint:
                raise RuntimeError("Bronze fingerprint changed; resume refused")
            with conn.cursor() as cur:
                cur.execute("DELETE FROM dq_result WHERE run_id=%s", (context.run_id,))
                cur.execute("DELETE FROM dq_metric WHERE run_id=%s", (context.run_id,))
            repository.update_status(conn, context.run_id, "BUILDING")
        else:
            context = repository.start_run(
                conn, mode="backfill_s3", status="BUILDING",
                input_fingerprint=fingerprint,
            )

        print(
            f"[s3-backfill] preparing candidates run={context.run_id}",
            flush=True,
        )
        bundle = _candidate_bundle(base)
        preflight = evaluate(bundle) + _required_backfill_results(bundle)
        recorded_results = preflight
        print_summary(preflight)
        assert_publishable(preflight)

        store = _store_for(context.run_id)
        parts = _candidate_parts(
            conn, context, store, bundle, fingerprint,
        )
        manifest_uri = store.save_run_manifest(
            parts, input_fingerprint=fingerprint,
        )
        print(f"[s3-backfill] candidate manifest={manifest_uri}", flush=True)

        stats = bundle.stats
        del bundle
        gc.collect()
        certified_bundle = _reload_bundle(store, parts, stats)
        repository.update_status(conn, context.run_id, "VALIDATING")
        global_results = (
            evaluate(certified_bundle)
            + _required_backfill_results(certified_bundle)
        )
        recorded_results = global_results
        print_summary(global_results)
        assert_publishable(global_results)
        conn.close()
        _publish(context, store, parts, certified_bundle, global_results)
        return context.run_id
    except Exception as exc:
        try:
            try:
                conn.rollback()
            except Exception:
                pass
            if getattr(conn, "closed", False):
                conn = db.connect()
            if context is not None:
                repository.finish_run(
                    conn, context, "FAILED", recorded_results,
                    error_message=str(exc),
                )
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", choices=["local"], default="local")
    parser.add_argument("--resume", help="기존 backfill_s3 dq_run UUID")
    return parser.parse_args()


def main() -> None:
    """Reject the unsafe direct write path before parsing or external access."""
    raise RuntimeError(DIRECT_CLI_DISABLED_MESSAGE)


def _unsafe_legacy_main() -> None:
    """Retained for a future closed orchestrator; never dispatch directly."""
    arguments = parse_args()
    run(arguments.src, arguments.resume)


if __name__ == "__main__":
    main()
