"""Warning worklist review tool.

The immutable observation log lives in dq_result; dq_warning_state is the compact
worklist projected from it for every silver-loading run. This CLI lets a reviewer
work that list down over time:

    # what still needs review (OPEN), most-failed first
    uv run python -m pipeline.silver_quality.review list
    uv run python -m pipeline.silver_quality.review list --rule PRICE_RETURN_SPIKE --limit 100

    # full detail + sample offending rows for one item
    uv run python -m pipeline.silver_quality.review show --id 1234

    # mark one reviewed & accepted (drops it from the OPEN list; stays down
    # across re-runs unless the observed value changes)
    uv run python -m pipeline.silver_quality.review ack --id 1234 --note "pre-2015 KRX reset, benign" --by leeyongjun

    # seed the worklist from history already in dq_result (one-off / idempotent)
    uv run python -m pipeline.silver_quality.review project
    uv run python -m pipeline.silver_quality.review project --since 2026-08-07

All reads roll back; ack/project commit.
"""
from __future__ import annotations

import argparse
import json

from pipeline.common import db
from pipeline.silver_quality import repository


def _rows(cur):
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def cmd_list(args) -> None:
    where = ["status = 'OPEN'"]
    params: list = []
    if args.mode:
        where.append("mode = %s")
        params.append(args.mode)
    if args.rule:
        where.append("rule_code = %s")
        params.append(args.rule)
    if args.dataset:
        where.append("dataset_name = %s")
        params.append(args.dataset)
    params.append(args.limit)
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT warning_state_id, mode, dataset_name, rule_code,
                       scope_key, latest_failed_count, observation_count,
                       reopen_count, last_failed_at
                FROM dq_warning_state
                WHERE {' AND '.join(where)}
                ORDER BY latest_failed_count DESC, rule_code, warning_state_id
                LIMIT %s
                """,
                params,
            )
            rows = _rows(cur)
        conn.rollback()
    finally:
        conn.close()
    if args.json:
        print(json.dumps(rows, ensure_ascii=False, default=str))
        return
    if not rows:
        print("(no OPEN warnings)")
        return
    print(f"{'id':>7}  {'failed':>7}  {'obs':>4}  {'rule':38} {'dataset':16} scope")
    for r in rows:
        print(
            f"{r['warning_state_id']:>7}  {r['latest_failed_count']:>7}  "
            f"{r['observation_count']:>4}  {r['rule_code']:38} "
            f"{r['dataset_name']:16} {r['scope_key']} [{r['mode']}]"
        )
    print(f"\n{len(rows)} open row(s) shown.")


def cmd_show(args) -> None:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM dq_warning_state WHERE warning_state_id=%s",
                (args.id,),
            )
            rows = _rows(cur)
        conn.rollback()
    finally:
        conn.close()
    if not rows:
        print(f"no warning_state_id={args.id}")
        return
    print(json.dumps(rows[0], ensure_ascii=False, default=str, indent=2))


def cmd_ack(args) -> None:
    conn = db.connect()
    try:
        ok = repository.acknowledge_warning(
            conn, args.id, note=args.note, by=args.by,
        )
        if ok:
            conn.commit()
            print(f"acknowledged warning_state_id={args.id}")
        else:
            conn.rollback()
            print(
                f"warning_state_id={args.id} is not OPEN "
                "(already acknowledged/resolved or unknown); nothing changed"
            )
    finally:
        conn.close()


def cmd_project(args) -> None:
    conn = db.connect()
    try:
        n = repository.project_result_history_to_warning_state(
            conn, since=args.since,
        )
        conn.commit()
        print(f"[review] projected {n} worklist row(s) from dq_result history")
        opened, failed_rows = repository.open_warning_counts(conn)
        print(f"[review] worklist now OPEN={opened} latest_failed_rows={failed_rows}")
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    pl = sub.add_parser("list", help="list OPEN worklist rows")
    pl.add_argument("--mode")
    pl.add_argument("--rule")
    pl.add_argument("--dataset")
    pl.add_argument("--limit", type=int, default=50)
    pl.add_argument("--json", action="store_true")
    pl.set_defaults(func=cmd_list)

    ps = sub.add_parser("show", help="show one row with samples")
    ps.add_argument("--id", type=int, required=True)
    ps.set_defaults(func=cmd_show)

    pa = sub.add_parser("ack", help="acknowledge (accept) one OPEN row")
    pa.add_argument("--id", type=int, required=True)
    pa.add_argument("--note")
    pa.add_argument("--by")
    pa.set_defaults(func=cmd_ack)

    pp = sub.add_parser("project", help="seed worklist from dq_result history")
    pp.add_argument(
        "--since",
        help="only project runs on/after this date (YYYY-MM-DD)",
    )
    pp.set_defaults(func=cmd_project)
    return p


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
