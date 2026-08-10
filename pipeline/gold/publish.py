"""Explicit, transactional publication of human-approved Gold factors.

Dry-run is the default.  It registers the metadata, computes the complete
backfill, validates coverage and pairwise rank correlations, then rolls the
whole transaction back.  ``--apply`` repeats the same transaction and commits.
"""
from __future__ import annotations

import argparse
import json
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


def load_approval(path: Path = DEFAULT_APPROVAL_PATH) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "approval_id", "approval_source", "ruleset_version",
        "backfill_start_month", "backfill_end_month", "selection", "factors",
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


def validate_approval(document: dict, manifest: dict | None = None) -> dict:
    manifest = manifest or load_manifest()
    validated: dict[str, dict] = {}
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
        if observed_hash != expected_hash:
            raise ValueError(
                f"{factor_key} SQL SHA-256 불일치: "
                f"expected={expected_hash}, observed={observed_hash}"
            )
        evaluation = approval["evaluation"]
        if evaluation.get("passed") is not True:
            raise ValueError(f"통과하지 않은 팩터는 승인할 수 없습니다: {factor_key}")
        if evaluation.get("verdict") != "PROMOTE":
            raise ValueError(f"PROMOTE가 아닌 팩터입니다: {factor_key}")
        if int(evaluation.get("oos_signal_months", 0)) != 36:
            raise ValueError(f"OOS 36개월 계약 불일치: {factor_key}")
        if evaluation.get("oos_evidence_class") != "HISTORICAL_REUSED_WINDOW":
            raise ValueError(f"OOS evidence class가 명시되지 않았습니다: {factor_key}")
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
        SELECT factor_id, version, status, implementation_uri,
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

    factor_id, _, status, old_uri, old_hash, old_config, _ = target
    if status in {"REJECTED", "RETIRED"}:
        raise ValueError(f"종료된 Gold 버전을 덮어쓸 수 없습니다: {factor_key} {status}")
    if status == "APPROVED":
        core_matches = (
            old_uri == uri
            and old_hash == item["implementation_hash"]
            and old_config.get("predicted_sign") == config["predicted_sign"]
            and old_config.get("research_definition_hash")
            == config["research_definition_hash"]
        )
        if not core_matches:
            raise ValueError(f"기존 APPROVED 계약과 불일치합니다: {factor_key}")
    cur.execute(
        """
        UPDATE gold.factor
        SET description = %s,
            implementation_uri = %s,
            implementation_hash = %s,
            config = %s::jsonb,
            evaluation = %s::jsonb,
            status = 'APPROVED'
        WHERE factor_id = %s
        """,
        (
            approval["description"], uri, item["implementation_hash"],
            json.dumps(config, ensure_ascii=False),
            json.dumps(evaluation, ensure_ascii=False),
            factor_id,
        ),
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
    return {
        "factor": factor_key,
        "factor_id": factor_id,
        "deleted_rows": deleted,
        "inserted_rows": inserted,
        "stored_rows": int(rows),
        "signal_months": int(months),
        "min_date": str(min_date),
        "max_date": str(max_date),
    }


def _pairwise_correlation(
    cur,
    *,
    left_key: str,
    left_id: int,
    right_key: str,
    right_id: int,
    start_month: str,
    end_month: str,
) -> dict:
    cur.execute(
        """
        WITH monthly AS (
            SELECT date_trunc('month', l.as_of_date)::date AS signal_month,
                   corr(l.rank::double precision, r.rank::double precision) AS rho,
                   count(*)::integer AS observations
            FROM gold.factor_value l
            JOIN gold.factor_value r
              ON r.asset_id = l.asset_id
             AND date_trunc('month', r.as_of_date)
                 = date_trunc('month', l.as_of_date)
            WHERE l.factor_id = %s
              AND r.factor_id = %s
              AND l.as_of_date >= %s::date
              AND l.as_of_date < (%s::date + interval '1 month')
            GROUP BY date_trunc('month', l.as_of_date)
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
            _month_start(start_month), _month_start(end_month),
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

    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '10s'")
            cur.execute("SET LOCAL statement_timeout = '45min'")
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("teamalpha_gold_publish",),
            )
            for factor_key, item in validated.items():
                factor_ids[factor_key] = _register_metadata(
                    cur, document, factor_key, item,
                )
            for factor_key, item in validated.items():
                value_summaries.append(_replace_values(
                    cur,
                    factor_id=factor_ids[factor_key],
                    factor_key=factor_key,
                    item=item,
                    start_month=start_month,
                    end_month=end_month,
                    expected_months=expected_months,
                ))
            for left_key, right_key in combinations(validated, 2):
                comparison = _pairwise_correlation(
                    cur,
                    left_key=left_key,
                    left_id=factor_ids[left_key],
                    right_key=right_key,
                    right_id=factor_ids[right_key],
                    start_month=start_month,
                    end_month=end_month,
                )
                if comparison["months"] < 36:
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
        if apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    return {
        "approval_id": document["approval_id"],
        "mode": "APPLY" if apply else "DRY_RUN_ROLLBACK",
        "backfill_start_month": start_month,
        "backfill_end_month": end_month,
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
