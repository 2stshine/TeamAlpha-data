"""DART 배당·기업행사 Bronze를 기존 KRX Silver 자산에 source-scoped 적재한다."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import corporate_actions, dividends, financials
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary


def _fingerprint(base: str) -> str:
    digest = hashlib.sha256()
    root = Path(base)
    for pattern in (
        "dividends/dart/**/*.json",
        "corporate_actions/dart/**/*.json",
        "corporate_actions/dart/**/*.zip",
    ):
        for path in sorted(root.glob(pattern)):
            stat = path.stat()
            digest.update(str(path.relative_to(root)).encode())
            digest.update(f"{stat.st_size}:{stat.st_mtime_ns}".encode())
    return digest.hexdigest()


def _identifier_map(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier, asset_id FROM asset_identifier "
            "WHERE source='KRX' AND valid_to IS NULL"
        )
        return {str(identifier): int(asset_id) for identifier, asset_id in cur.fetchall()}


def _exclude_unmapped(frame, allowed: set[str]) -> tuple[object, dict]:
    if frame.empty:
        return frame, {"row_count": 0, "ticker_count": 0, "samples": []}
    identifiers = frame["identifier"].astype(str)
    missing = frame[~identifiers.isin(allowed)]
    retained = frame[identifiers.isin(allowed)].reset_index(drop=True)
    return retained, {
        "row_count": len(missing),
        "ticker_count": int(missing["identifier"].astype(str).nunique()),
        "samples": (
            missing[["identifier", "source_file"]].drop_duplicates()
            .head(20).to_dict("records")
        ),
    }


def _total_return_actions(frame):
    """Keep only issuer dividend evidence consumed by the TR rebuild."""
    if frame.empty:
        return frame.copy()
    return frame[
        frame["event_type"].isin(("cash_dividend", "ex_dividend"))
        & frame["action_scope"].eq("ISSUER")
    ].reset_index(drop=True)


def run(
    *,
    src: str = "local",
    base_override: str | None = None,
    total_return_actions_only: bool = False,
) -> None:
    """Publish a complete local DART snapshot into existing KRX Silver.

    ``base_override`` is intended for an immutable temporary download used by
    repair/audit jobs.  It does not change the configured Bronze root.
    """
    base = str(Path(base_override).resolve()) if base_override else base_uri(src)
    if total_return_actions_only:
        dividend_frame = pd.DataFrame(columns=dividends.COLUMNS)
        dividend_stats = {
            "input_rows": 0,
            "transformed_rows": 0,
            "excluded_rows": 0,
            "rejected_rows": 0,
            "duplicate_rows_removed": 0,
            "source_file_count": 0,
        }
    else:
        dividend_frame, dividend_stats = dividends.prepare(base)
    action_frame, action_stats = corporate_actions.prepare(base)
    if total_return_actions_only:
        before_filter = len(action_frame)
        action_frame = _total_return_actions(action_frame)
        action_stats = dict(action_stats)
        action_stats.update({
            "row_count": len(action_frame),
            "total_return_action_input_rows": before_filter,
            "total_return_action_excluded_rows": before_filter - len(action_frame),
        })
    conn = db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(conn)
        identifier_map = _identifier_map(conn)
        allowed = set(identifier_map)
        dividend_frame, dividend_unmapped = _exclude_unmapped(
            dividend_frame, allowed,
        )
        action_frame, action_unmapped = _exclude_unmapped(action_frame, allowed)
        dividend_stats = dict(dividend_stats)
        excluded_rows = int(dividend_unmapped["row_count"])
        dividend_stats["transformed_rows"] = len(dividend_frame)
        dividend_stats["excluded_rows"] = (
            int(dividend_stats.get("excluded_rows", 0)) + excluded_rows
        )
        context = repository.start_run(
            conn,
            mode="dart_dividend_action_backfill",
            input_fingerprint=_fingerprint(base),
        )
        bundle = CandidateBundle(
            fundamentals=dividend_frame,
            actions=action_frame,
            stats={
                "fundamental": dividend_stats,
                "corporate_action": action_stats,
                "_existing_krx_identifiers": allowed,
                "_dividend_unmapped": dividend_unmapped,
                "_action_unmapped": action_unmapped,
            },
        )
        results = evaluate(bundle)
        print_summary(results)
        assert_publishable(results)
        with conn.transaction():
            if not total_return_actions_only:
                financials.publish(
                    conn, dividend_frame, identifier_map, context.run_id,
                    replace_scopes=True,
                )
            corporate_actions.publish(
                conn, action_frame, identifier_map, context.run_id,
            )
            repository.save_metrics(conn, context.run_id, bundle)
            repository.finish_run(
                conn, context, "CERTIFIED", results, commit=False,
            )
        print(
            f"[silver-dart-extra] certified run={context.run_id} "
            f"dividends={len(dividend_frame)} actions={len(action_frame)} "
            f"unmapped_dividends={excluded_rows} "
            f"unmapped_actions={action_unmapped['row_count']}",
            flush=True,
        )
    except Exception as exc:
        conn.rollback()
        if context is not None:
            repository.finish_run(
                conn, context, "FAILED", results, error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", choices=("local",), default="local")
    parser.add_argument(
        "--base",
        help="전체 DART snapshot이 있는 로컬 root (기본: repo data/)",
    )
    parser.add_argument(
        "--total-return-actions-only",
        action="store_true",
        help=(
            "총수익 재구축에 필요한 ISSUER cash/ex-dividend action만 적재하고 "
            "fundamental 배당 지표는 변경하지 않음"
        ),
    )
    args = parser.parse_args()
    run(
        src=args.src,
        base_override=args.base,
        total_return_actions_only=args.total_return_actions_only,
    )


if __name__ == "__main__":
    main()
