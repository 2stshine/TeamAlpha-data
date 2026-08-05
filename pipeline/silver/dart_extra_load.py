"""DART 배당·기업행사 Bronze를 기존 KRX Silver 자산에 source-scoped 적재한다."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

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


def run(*, src: str = "local") -> None:
    base = base_uri(src)
    dividend_frame, dividend_stats = dividends.prepare(base)
    action_frame, action_stats = corporate_actions.prepare(base)
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
    args = parser.parse_args()
    run(src=args.src)


if __name__ == "__main__":
    main()
