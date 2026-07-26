"""불변 Bronze cutoff manifest를 내려받아 publish 없이 전체 DQ를 실행한다."""
from __future__ import annotations

import json
import os
from pathlib import Path

from pipeline.common import db
from pipeline.silver_quality import repository
from pipeline.silver_quality.backfill import (
    _candidate_bundle,
    _required_backfill_results,
)
from pipeline.silver_quality.ecs_backfill import _sync_cutoff
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.runner import (
    assert_publishable,
    evaluate,
    print_summary,
)


def main() -> None:
    root = Path(os.environ.get("BACKFILL_DATA_ROOT", "/app/data"))
    fingerprint = _sync_cutoff(root)
    conn = db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn,
            mode="s3_quality_audit",
            status="VALIDATING",
            input_fingerprint=fingerprint,
        )
        bundle = _candidate_bundle(str(root))
        results = evaluate(bundle) + _required_backfill_results(bundle)
        print_summary(results)
        assert_publishable(results)
        repository.save_metrics(conn, context.run_id, bundle)
        repository.finish_run(conn, context, "CERTIFIED", results)
        summary = {
            "run_id": str(context.run_id),
            "fingerprint": fingerprint,
            "rows": {
                "asset": len(bundle.assets),
                "asset_identifier": len(bundle.identifiers),
                "price_daily": len(bundle.prices),
                "fundamental": len(bundle.fundamentals),
            },
            "warnings": {
                item.rule_code: {
                    "failed_count": item.failed_count,
                    "actual": item.actual,
                }
                for item in results
                if item.severity == Severity.WARNING
                and item.status == CheckStatus.FAIL
            },
        }
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    except Exception as exc:
        conn.rollback()
        if context is not None:
            failure = CheckResult(
                rule_code="S3_QUALITY_AUDIT",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="S3 cutoff produces publishable Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn,
                context,
                "FAILED",
                results + [failure],
                error_message=str(exc),
            )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
