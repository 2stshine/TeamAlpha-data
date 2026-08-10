"""Explicit, transactional publication of human-approved Gold factors.

Dry-run is the default.  It registers the metadata, computes the complete
backfill, validates coverage and pairwise rank correlations, then rolls the
whole transaction back.  ``--apply`` repeats the same transaction and commits.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date
from itertools import combinations
from pathlib import Path

from pipeline.common import db
from pipeline.gold.run import (
    ROOT,
    build_upsert_sql,
    implementation_hash,
    load_manifest,
    validate_query_sql,
)


DEFAULT_APPROVAL_PATH = (
    Path(__file__).with_name("approvals")
    / "promoted_20260810_nonduplicate.json"
)
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
CAMPAIGN_PATTERN = re.compile(r"^campaign-\d{8}-\d{3}$")
SUPPORTED_RULESET = "fr-3.10.1"
MIN_DISCOVERY_IC = 0.03
MIN_OOS_IC = 0.02
MIN_OOS_RETENTION = 0.50
MAX_FDR_Q = 0.10
MIN_CORRELATION_MONTHS = 36
MIN_MONTHLY_OBSERVATIONS = 30
PUBLISH_LOCK_KEY = "teamalpha_gold_publish"


def _month_start(value: str) -> date:
    if not MONTH_PATTERN.fullmatch(value):
        raise ValueError(f"월은 YYYY-MM 형식이어야 합니다: {value}")
    return date.fromisoformat(f"{value}-01")


def _month_count(start_month: str, end_month: str) -> int:
    start = _month_start(start_month)
    end = _month_start(end_month)
    count = (end.year - start.year) * 12 + end.month - start.month + 1
    if count <= 0:
        raise ValueError("backfill 종료월은 시작월보다 빠를 수 없습니다")
    return count


def _acquire_publish_lock(conn) -> None:
    """Serialize publishers before opening the repeatable-read snapshot."""
    try:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '10s'")
            cur.execute(
                "SELECT pg_advisory_lock(hashtext(%s))",
                (PUBLISH_LOCK_KEY,),
            )
        # Session advisory locks survive commit.  Ending this short transaction
        # ensures the publication transaction receives a snapshot newer than
        # every publisher that previously held the lock.
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _release_publish_lock(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))",
                (PUBLISH_LOCK_KEY,),
            )
            unlocked = bool(cur.fetchone()[0])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if not unlocked:
        raise RuntimeError("Gold publisher session advisory lock이 없습니다")


def _emit_progress(stage: str, **fields: object) -> None:
    print(json.dumps(
        {"gold_publish_progress": stage, **fields},
        ensure_ascii=False,
    ), flush=True)


def load_approval(path: Path = DEFAULT_APPROVAL_PATH) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "approval_id", "approval_source", "ruleset_version",
        "research_source", "backfill_start_month", "backfill_end_month",
        "selection", "factors",
    }
    missing = sorted(required - set(document))
    if missing:
        raise ValueError(f"Gold approval 필드가 없습니다: {missing}")
    _month_count(
        document["backfill_start_month"],
        document["backfill_end_month"],
    )
    if not document["factors"]:
        raise ValueError("승인된 Gold 팩터가 없습니다")
    return document


def _finite_number(value: object, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"유효한 숫자가 아닙니다: {field}={value!r}") from exc
    if not math.isfinite(number):
        raise ValueError(f"유한한 숫자여야 합니다: {field}={value!r}")
    return number


def _validate_evaluation(factor_key: str, evaluation: dict) -> None:
    if evaluation.get("passed") is not True:
        raise ValueError(f"통과하지 않은 팩터는 승인할 수 없습니다: {factor_key}")
    if evaluation.get("verdict") != "PROMOTE":
        raise ValueError(f"PROMOTE가 아닌 팩터입니다: {factor_key}")

    discovery_ic = _finite_number(
        evaluation.get("discovery_investable_rank_ic"),
        field=f"{factor_key}.discovery_investable_rank_ic",
    )
    oos_ic = _finite_number(
        evaluation.get("oos_rank_ic"), field=f"{factor_key}.oos_rank_ic",
    )
    required_ic = _finite_number(
        evaluation.get("oos_required_rank_ic"),
        field=f"{factor_key}.oos_required_rank_ic",
    )
    retention = _finite_number(
        evaluation.get("oos_rank_ic_retention"),
        field=f"{factor_key}.oos_rank_ic_retention",
    )
    qvalue = _finite_number(
        evaluation.get("oos_by_qvalue"),
        field=f"{factor_key}.oos_by_qvalue",
    )
    months = int(evaluation.get("oos_signal_months", 0))
    start = evaluation.get("oos_start")
    end = evaluation.get("oos_end")
    if discovery_ic < MIN_DISCOVERY_IC:
        raise ValueError(f"Discovery IC 기준 미달: {factor_key} {discovery_ic}")
    expected_required = max(MIN_OOS_IC, MIN_OOS_RETENTION * discovery_ic)
    if not math.isclose(required_ic, expected_required, rel_tol=1e-10, abs_tol=1e-12):
        raise ValueError(
            f"OOS 필요 IC 계산 불일치: {factor_key}, "
            f"expected={expected_required}, observed={required_ic}"
        )
    if oos_ic < required_ic:
        raise ValueError(f"OOS IC 기준 미달: {factor_key} {oos_ic}<{required_ic}")
    if retention < MIN_OOS_RETENTION or not math.isclose(
        retention, oos_ic / discovery_ic, rel_tol=1e-10, abs_tol=1e-12,
    ):
        raise ValueError(f"OOS IC 유지율 불일치 또는 기준 미달: {factor_key}")
    if not 0.0 <= qvalue <= MAX_FDR_Q:
        raise ValueError(f"OOS BY q 기준 미달: {factor_key} {qvalue}")
    if months != MIN_CORRELATION_MONTHS:
        raise ValueError(f"OOS 36개월 계약 불일치: {factor_key}")
    if not isinstance(start, str) or not isinstance(end, str):
        raise ValueError(f"OOS 시작·종료월이 없습니다: {factor_key}")
    if _month_count(start, end) != months:
        raise ValueError(f"OOS 월 범위가 연속 36개월이 아닙니다: {factor_key}")
    if evaluation.get("oos_evidence_class") != "HISTORICAL_REUSED_WINDOW":
        raise ValueError(f"OOS evidence class가 명시되지 않았습니다: {factor_key}")


def validate_approval(document: dict, manifest: dict | None = None) -> dict:
    manifest = manifest or load_manifest()
    validated: dict[str, dict] = {}
    if document["ruleset_version"] != SUPPORTED_RULESET:
        raise ValueError(
            f"지원하지 않는 연구 ruleset입니다: {document['ruleset_version']}"
        )
    research_source = document["research_source"]
    if not isinstance(research_source, dict):
        raise ValueError("research_source는 객체여야 합니다")
    if not str(research_source.get("repository", "")).startswith("https://github.com/"):
        raise ValueError("research_source.repository가 GitHub 저장소가 아닙니다")
    if not GIT_COMMIT_PATTERN.fullmatch(str(research_source.get("commit", ""))):
        raise ValueError("research_source.commit은 고정된 40자리 Git SHA여야 합니다")

    threshold = _finite_number(
        document["selection"].get(
            "maximum_allowed_median_absolute_correlation"
        ),
        field="selection.maximum_allowed_median_absolute_correlation",
    )
    if not 0.0 <= threshold < 1.0:
        raise ValueError(f"Gold 중복 상관 임계값 범위 오류: {threshold}")
    duplicate = document["selection"]["excluded_duplicate"]["factor"]
    if duplicate in document["factors"]:
        raise ValueError(f"중복 제외 팩터가 승인 목록에 있습니다: {duplicate}")

    for factor_key, approval in document["factors"].items():
        if factor_key not in manifest:
            raise ValueError(f"Gold manifest에 없는 승인 팩터입니다: {factor_key}")
        spec = manifest[factor_key]
        sql_path = ROOT / spec["sql"]
        validate_query_sql(sql_path.read_text(encoding="utf-8"))
        observed_hash = implementation_hash(sql_path)
        expected_hash = approval["implementation_sha256"]
        if not SHA256_PATTERN.fullmatch(str(expected_hash)):
            raise ValueError(f"SQL SHA-256 형식 오류: {factor_key}")
        if observed_hash != expected_hash:
            raise ValueError(
                f"{factor_key} SQL SHA-256 불일치: "
                f"expected={expected_hash}, observed={observed_hash}"
            )
        if int(approval.get("version", 0)) <= 0:
            raise ValueError(f"Gold factor version 형식 오류: {factor_key}")
        if not CAMPAIGN_PATTERN.fullmatch(str(approval.get("campaign_id", ""))):
            raise ValueError(f"campaign_id 형식 오류: {factor_key}")
        evaluation = approval["evaluation"]
        _validate_evaluation(factor_key, evaluation)
        validated[factor_key] = {
            "approval": approval,
            "manifest": spec,
            "sql_path": sql_path,
            "implementation_hash": observed_hash,
        }
    return validated


def _desired_metadata(
    document: dict,
    factor_key: str,
    item: dict,
) -> tuple[dict, dict]:
    approval = item["approval"]
    spec = item["manifest"]
    config = {
        "approval_id": document["approval_id"],
        "approval_source": document["approval_source"],
        "research_source": document["research_source"],
        "backfill_window": {
            "start_month": document["backfill_start_month"],
            "end_month": document["backfill_end_month"],
        },
        "predicted_sign": int(spec["predicted_sign"]),
        "research_campaign_id": approval["campaign_id"],
        "research_definition_hash": spec["research_definition_hash"],
        "ruleset_version": document["ruleset_version"],
        "value_contract": {"id": spec["value_contract"]},
    }
    evaluation = dict(approval["evaluation"])
    evaluation.update({
        "approval_id": document["approval_id"],
        "ruleset_version": document["ruleset_version"],
        "selection": document["selection"],
    })
    return config, evaluation


def _register_metadata(cur, document: dict, factor_key: str, item: dict) -> int:
    approval = item["approval"]
    spec = item["manifest"]
    version = int(approval["version"])
    config, evaluation = _desired_metadata(document, factor_key, item)
    uri = f"repo://TeamAlpha-data/{spec['sql']}"

    cur.execute(
        """
        SELECT factor_id, version, status, description, implementation_uri,
               implementation_hash, config, evaluation
        FROM gold.factor
        WHERE factor_key = %s
        ORDER BY version
        FOR UPDATE
        """,
        (factor_key,),
    )
    rows = cur.fetchall()
    for row in rows:
        if row[2] == "APPROVED" and row[1] != version:
            raise ValueError(
                f"다른 APPROVED 버전이 이미 있습니다: {factor_key} v{row[1]}"
            )
    target = next((row for row in rows if row[1] == version), None)
    if target is None:
        cur.execute(
            """
            INSERT INTO gold.factor (
                factor_key, version, description, implementation_uri,
                implementation_hash, config, evaluation, status
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, 'APPROVED')
            RETURNING factor_id
            """,
            (
                factor_key, version, approval["description"], uri,
                item["implementation_hash"],
                json.dumps(config, ensure_ascii=False),
                json.dumps(evaluation, ensure_ascii=False),
            ),
        )
        return int(cur.fetchone()[0])

    (
        factor_id, _, status, old_description, old_uri, old_hash,
        old_config, old_evaluation,
    ) = target
    if status in {"REJECTED", "RETIRED"}:
        raise ValueError(
            f"종료된 Gold 버전을 덮어쓸 수 없습니다: {factor_key} {status}"
        )
    exact_matches = (
        old_description == approval["description"]
        and old_uri == uri
        and old_hash == item["implementation_hash"]
        and old_config == config
        and old_evaluation == evaluation
    )
    if not exact_matches:
        raise ValueError(
            f"기존 {status} v{version}의 감사 메타데이터와 다릅니다. "
            f"새 버전이 필요합니다: {factor_key}"
        )
    if status == "APPROVED":
        return int(factor_id)
    cur.execute(
        """
        UPDATE gold.factor
        SET status = 'APPROVED'
        WHERE factor_id = %s
        """,
        (factor_id,),
    )
    return int(factor_id)


def _replace_values(
    cur,
    *,
    factor_id: int,
    factor_key: str,
    item: dict,
    start_month: str,
    end_month: str,
    expected_months: int,
) -> dict:
    start_date = _month_start(start_month)
    end_date = _month_start(end_month)
    cur.execute(
        """
        DELETE FROM gold.factor_value
        WHERE factor_id = %s
          AND as_of_date >= %s
          AND as_of_date < (%s::date + interval '1 month')
        """,
        (factor_id, start_date, end_date),
    )
    deleted = max(cur.rowcount, 0)
    sql = item["sql_path"].read_text(encoding="utf-8")
    cur.execute(build_upsert_sql(sql), {
        "factor_id": factor_id,
        "start_month": start_date,
        "end_month": end_date,
    })
    inserted = max(cur.rowcount, 0)
    cur.execute(
        """
        SELECT count(*)::bigint,
               count(DISTINCT date_trunc('month', as_of_date))::integer,
               min(as_of_date), max(as_of_date), min(rank),
               count(*) FILTER (
                   WHERE value::text IN ('NaN', 'Infinity', '-Infinity')
               )::bigint
        FROM gold.factor_value
        WHERE factor_id = %s
          AND as_of_date >= %s
          AND as_of_date < (%s::date + interval '1 month')
        """,
        (factor_id, start_date, end_date),
    )
    rows, months, min_date, max_date, min_rank, nonfinite = cur.fetchone()
    if rows <= 0 or inserted <= 0:
        raise ValueError(f"Gold 값이 생성되지 않았습니다: {factor_key}")
    if months != expected_months:
        raise ValueError(
            f"Gold signal month 누락: {factor_key}, "
            f"expected={expected_months}, observed={months}"
        )
    if min_rank != 1:
        raise ValueError(f"Gold rank 1이 없습니다: {factor_key}")
    if nonfinite:
        raise ValueError(f"Gold 비유한 값이 있습니다: {factor_key} {nonfinite}")
    cur.execute(
        """
        WITH monthly AS (
            SELECT date_trunc('month', as_of_date)::date AS signal_month,
                   count(*)::integer AS observations,
                   count(DISTINCT asset_id)::integer AS assets,
                   count(DISTINCT as_of_date)::integer AS signal_dates,
                   min(rank)::integer AS min_rank
            FROM gold.factor_value
            WHERE factor_id = %s
              AND as_of_date >= %s
              AND as_of_date < (%s::date + interval '1 month')
            GROUP BY date_trunc('month', as_of_date)
        )
        SELECT min(observations)::integer,
               max(observations)::integer,
               bool_and(observations = assets),
               min(signal_dates)::integer,
               max(signal_dates)::integer,
               bool_and(min_rank = 1)
        FROM monthly
        """,
        (factor_id, start_date, end_date),
    )
    (
        min_month_observations, max_month_observations,
        unique_asset_months, min_month_signal_dates,
        max_month_signal_dates, monthly_rank_one,
    ) = cur.fetchone()
    if min_month_observations < MIN_MONTHLY_OBSERVATIONS:
        raise ValueError(
            f"Gold 월별 표본 부족: {factor_key}, "
            f"minimum={min_month_observations}"
        )
    if not unique_asset_months:
        raise ValueError(f"Gold asset-month 중복이 있습니다: {factor_key}")
    if not monthly_rank_one:
        raise ValueError(f"Gold 일부 월에 rank 1이 없습니다: {factor_key}")
    cur.execute(
        """
        SELECT count(*)::bigint
        FROM gold.factor_value v
        JOIN gold_publish_investable_universe u
          ON u.asset_id = v.asset_id
         AND u.signal_month = date_trunc('month', v.as_of_date)::date
        WHERE v.factor_id = %s
          AND v.as_of_date >= %s
          AND v.as_of_date < (%s::date + interval '1 month')
          AND v.as_of_date <> u.signal_date
        """,
        (factor_id, start_date, end_date),
    )
    signal_date_mismatches = int(cur.fetchone()[0])
    if signal_date_mismatches:
        raise ValueError(
            f"Gold/Silver 종목별 signal date 불일치: "
            f"{factor_key} {signal_date_mismatches}"
        )
    return {
        "factor": factor_key,
        "factor_id": factor_id,
        "deleted_rows": deleted,
        "inserted_rows": inserted,
        "stored_rows": int(rows),
        "signal_months": int(months),
        "min_month_observations": int(min_month_observations),
        "max_month_observations": int(max_month_observations),
        "min_month_distinct_signal_dates": int(min_month_signal_dates),
        "max_month_distinct_signal_dates": int(max_month_signal_dates),
        "silver_signal_date_mismatches": signal_date_mismatches,
        "min_date": str(min_date),
        "max_date": str(max_date),
    }


def _build_investable_universe(
    cur,
    *,
    start_month: str,
    end_month: str,
    expected_months: int,
) -> dict:
    """Materialize the exact research investable universe in this snapshot."""
    cur.execute(
        """
        SELECT count(*)::integer
        FROM public.price_return_contract
        WHERE source = 'KRX'
          AND asset_type = 'stock'
          AND field_name = 'total_return_close'
          AND methodology_version = 'krx_gross_dividend_reinvested_v1'
          AND status = 'CERTIFIED'
          AND certified_at IS NOT NULL
        """
    )
    if int(cur.fetchone()[0]) != 1:
        raise ValueError("인증된 KRX 총수익률 계약이 정확히 하나가 아닙니다")

    cur.execute(
        """
        CREATE TEMP TABLE gold_publish_investable_universe
        ON COMMIT DROP AS
        WITH certified AS (
            SELECT p.asset_id,
                   p.trade_date,
                   p.total_return_close,
                   p.trading_value,
                   p.market_cap,
                   p.market,
                   a.name,
                   a.instrument_type,
                   avg(p.trading_value) OVER (
                       PARTITION BY p.asset_id ORDER BY p.trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS adv20,
                   row_number() OVER (
                       PARTITION BY p.asset_id ORDER BY p.trade_date
                   ) AS age_days,
                   min(p.trade_date) OVER (
                       PARTITION BY p.asset_id
                   ) AS first_seen,
                   min(p.trade_date) OVER () AS dataset_start
            FROM public.price_daily p
            JOIN public.asset a ON a.asset_id = p.asset_id
            JOIN public.dq_run q
              ON q.run_id = p.quality_run_id
             AND q.status = 'CERTIFIED'
            JOIN LATERAL (
                SELECT 1
                FROM public.asset_identifier ai
                WHERE ai.asset_id = p.asset_id
                  AND ai.source = 'KRX'
                  AND ai.identifier_type = 'ticker'
                  AND ai.valid_from <= p.trade_date
                  AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
                ORDER BY ai.valid_from DESC
                LIMIT 1
            ) identifier ON true
            WHERE p.source = 'KRX'
              AND a.exchange = 'KRX'
              AND a.asset_type = 'stock'
              AND p.market IN ('KOSPI', 'KOSDAQ')
              AND p.trade_date < (%s::date + interval '1 month')
        ), monthly AS (
            SELECT certified.*,
                   row_number() OVER (
                       PARTITION BY asset_id, date_trunc('month', trade_date)
                       ORDER BY trade_date DESC
                   ) AS month_rank
            FROM certified
        )
        SELECT asset_id,
               date_trunc('month', trade_date)::date AS signal_month,
               trade_date AS signal_date
        FROM monthly
        WHERE month_rank = 1
          AND trade_date >= %s::date
          AND trade_date < (%s::date + interval '1 month')
          AND instrument_type = 'common_stock'
          AND coalesce(name, '') !~* '(스팩|SPAC)'
          AND coalesce(name, '') !~ '리츠'
          AND (age_days >= 250 OR first_seen = dataset_start)
          AND market_cap > 0
          AND total_return_close > 0
          AND adv20 > 0
        """,
        (
            _month_start(end_month),
            _month_start(start_month),
            _month_start(end_month),
        ),
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX ON gold_publish_investable_universe
            (signal_month, asset_id)
        """
    )
    cur.execute("ANALYZE gold_publish_investable_universe")
    cur.execute(
        """
        SELECT count(*)::bigint,
               count(DISTINCT signal_month)::integer,
               min(monthly_observations)::integer,
               max(monthly_observations)::integer
        FROM (
            SELECT signal_month, count(*)::integer AS monthly_observations
            FROM gold_publish_investable_universe
            GROUP BY signal_month
        ) monthly
        """
    )
    rows, months, min_observations, max_observations = cur.fetchone()
    if months != expected_months:
        raise ValueError(
            f"투자가능 유니버스 월 누락: expected={expected_months}, "
            f"observed={months}"
        )
    if min_observations < MIN_MONTHLY_OBSERVATIONS:
        raise ValueError(f"투자가능 유니버스 월별 표본 부족: {min_observations}")
    return {
        "rows": int(rows),
        "signal_months": int(months),
        "min_month_observations": int(min_observations),
        "max_month_observations": int(max_observations),
        "rule": "research_panel_universe_and_adv20_gt_0",
    }


def _build_correlation_values(
    cur,
    *,
    factor_ids: dict[str, int],
    start_month: str,
    end_month: str,
) -> dict:
    """Aggregate investable factor values once for every pairwise check."""
    cur.execute(
        """
        CREATE TEMP TABLE gold_publish_correlation_values
        ON COMMIT DROP AS
        SELECT v.factor_id,
               u.signal_month,
               u.asset_id,
               min(v.rank)::double precision AS stored_rank,
               count(*)::integer AS rows_per_asset_month
        FROM gold_publish_investable_universe u
        JOIN gold.factor_value v
          ON v.asset_id = u.asset_id
         AND v.as_of_date >= u.signal_month
         AND v.as_of_date < (u.signal_month + interval '1 month')
        WHERE v.factor_id = ANY(%s)
          AND v.as_of_date >= %s::date
          AND v.as_of_date < (%s::date + interval '1 month')
        GROUP BY v.factor_id, u.signal_month, u.asset_id
        """,
        (
            list(factor_ids.values()),
            _month_start(start_month),
            _month_start(end_month),
        ),
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX ON gold_publish_correlation_values
            (factor_id, signal_month, asset_id)
        """
    )
    cur.execute("ANALYZE gold_publish_correlation_values")
    cur.execute(
        """
        SELECT factor_id,
               count(*)::bigint,
               count(DISTINCT signal_month)::integer,
               max(rows_per_asset_month)::integer
        FROM gold_publish_correlation_values
        GROUP BY factor_id
        """
    )
    observed = {
        int(factor_id): {
            "rows": int(rows),
            "signal_months": int(months),
            "max_rows_per_asset_month": int(max_rows),
        }
        for factor_id, rows, months, max_rows in cur.fetchall()
    }
    for factor_key, factor_id in factor_ids.items():
        summary = observed.get(factor_id)
        if summary is None:
            raise ValueError(f"Gold 중복 비교 값이 없습니다: {factor_key}")
        if summary["max_rows_per_asset_month"] > 1:
            raise ValueError(f"Gold asset-month 중복이 있습니다: {factor_key}")
    return {
        "rows": sum(item["rows"] for item in observed.values()),
        "factors": {
            factor_key: observed[factor_id]
            for factor_key, factor_id in factor_ids.items()
        },
    }


def _pairwise_correlation(
    cur,
    *,
    left_key: str,
    left_id: int,
    right_key: str,
    right_id: int,
) -> dict:
    cur.execute(
        """
        WITH aligned AS (
            SELECT l.signal_month,
                   l.asset_id,
                   l.stored_rank AS left_rank,
                   r.stored_rank AS right_rank
            FROM gold_publish_correlation_values l
            JOIN gold_publish_correlation_values r
              ON r.signal_month = l.signal_month
             AND r.asset_id = l.asset_id
            WHERE l.factor_id = %s
              AND r.factor_id = %s
        ), tied AS (
            SELECT aligned.*,
                   rank() OVER (
                       PARTITION BY signal_month ORDER BY left_rank
                   )::double precision AS left_min_rank,
                   count(*) OVER (
                       PARTITION BY signal_month, left_rank
                   )::double precision AS left_ties,
                   rank() OVER (
                       PARTITION BY signal_month ORDER BY right_rank
                   )::double precision AS right_min_rank,
                   count(*) OVER (
                       PARTITION BY signal_month, right_rank
                   )::double precision AS right_ties
            FROM aligned
        ), average_ranked AS (
            SELECT signal_month,
                   left_min_rank + (left_ties - 1.0) / 2.0 AS left_rank,
                   right_min_rank + (right_ties - 1.0) / 2.0 AS right_rank
            FROM tied
        ), monthly AS (
            SELECT signal_month,
                   corr(left_rank, right_rank) AS rho,
                   count(*)::integer AS observations
            FROM average_ranked
            GROUP BY signal_month
            HAVING count(*) >= 30
        )
        SELECT count(*)::integer,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(rho)),
               max(abs(rho)),
               min(observations)
        FROM monthly
        WHERE rho IS NOT NULL
        """,
        (
            left_id, right_id,
        ),
    )
    months, median_abs, max_abs, min_observations = cur.fetchone()
    return {
        "left": left_key,
        "right": right_key,
        "months": int(months),
        "median_abs_spearman": (
            float(median_abs) if median_abs is not None else None
        ),
        "max_month_abs_spearman": (
            float(max_abs) if max_abs is not None else None
        ),
        "min_month_observations": (
            int(min_observations) if min_observations is not None else 0
        ),
    }


def publish(
    conn,
    *,
    approval_path: Path = DEFAULT_APPROVAL_PATH,
    apply: bool = False,
) -> dict:
    document = load_approval(approval_path)
    validated = validate_approval(document)
    start_month = document["backfill_start_month"]
    end_month = document["backfill_end_month"]
    expected_months = _month_count(start_month, end_month)
    threshold = float(
        document["selection"]["maximum_allowed_median_absolute_correlation"]
    )
    factor_ids: dict[str, int] = {}
    value_summaries: list[dict] = []
    correlations: list[dict] = []
    investable_universe: dict = {}
    correlation_values: dict = {}

    _acquire_publish_lock(conn)
    try:
        try:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SET LOCAL statement_timeout = '45min'")
                cur.execute(
                    """
                    SELECT factor_id, factor_key
                    FROM gold.factor
                    WHERE status = 'APPROVED'
                      AND NOT (factor_key = ANY(%s))
                    ORDER BY factor_key
                    """,
                    (list(validated),),
                )
                existing_approved = {
                    str(factor_key): int(factor_id)
                    for factor_id, factor_key in cur.fetchall()
                }
                investable_universe = _build_investable_universe(
                    cur,
                    start_month=start_month,
                    end_month=end_month,
                    expected_months=expected_months,
                )
                _emit_progress("investable_universe", **investable_universe)
                for factor_key, item in validated.items():
                    factor_ids[factor_key] = _register_metadata(
                        cur, document, factor_key, item,
                    )
                for factor_key, item in validated.items():
                    summary = _replace_values(
                        cur,
                        factor_id=factor_ids[factor_key],
                        factor_key=factor_key,
                        item=item,
                        start_month=start_month,
                        end_month=end_month,
                        expected_months=expected_months,
                    )
                    value_summaries.append(summary)
                    _emit_progress("factor_values", **summary)
                comparisons = list(combinations(validated, 2))
                comparisons.extend(
                    (new_key, existing_key)
                    for new_key in validated
                    for existing_key in existing_approved
                )
                comparison_ids = {**existing_approved, **factor_ids}
                correlation_values = _build_correlation_values(
                    cur,
                    factor_ids=comparison_ids,
                    start_month=start_month,
                    end_month=end_month,
                )
                _emit_progress(
                    "correlation_values",
                    rows=correlation_values["rows"],
                    factors=len(correlation_values["factors"]),
                )
                for left_key, right_key in comparisons:
                    comparison = _pairwise_correlation(
                        cur,
                        left_key=left_key,
                        left_id=comparison_ids[left_key],
                        right_key=right_key,
                        right_id=comparison_ids[right_key],
                    )
                    if comparison["months"] < MIN_CORRELATION_MONTHS:
                        raise ValueError(
                            f"Gold 중복 비교월 부족: {left_key}/{right_key} "
                            f"{comparison['months']}"
                        )
                    if comparison["median_abs_spearman"] > threshold:
                        raise ValueError(
                            f"Gold 중복 기준 초과: {left_key}/{right_key} "
                            f"{comparison['median_abs_spearman']:.6f}>{threshold}"
                        )
                    correlations.append(comparison)
                    _emit_progress("pairwise_correlation", **comparison)
            if apply:
                conn.commit()
            else:
                conn.rollback()
        except Exception:
            conn.rollback()
            raise
    finally:
        _release_publish_lock(conn)
    return {
        "approval_id": document["approval_id"],
        "mode": "APPLY" if apply else "DRY_RUN_ROLLBACK",
        "backfill_start_month": start_month,
        "backfill_end_month": end_month,
        "investable_universe": investable_universe,
        "correlation_values": correlation_values,
        "factors": value_summaries,
        "pairwise_correlations": correlations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--approval-file",
        type=Path,
        default=DEFAULT_APPROVAL_PATH,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="생략하면 전체 승인·backfill·중복 검사를 수행한 뒤 rollback",
    )
    args = parser.parse_args()
    conn = db.connect()
    try:
        result = publish(
            conn,
            approval_path=args.approval_file,
            apply=args.apply,
        )
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
