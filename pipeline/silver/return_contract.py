"""Lifecycle helpers for the derived KRX total-return contract.

Raw KRX prices and issuer cash-dividend actions are inputs to
``total_return_close``.  Publishing either input invalidates any prior
certification in the same transaction.  The bounded full rebuild is the only
workflow that promotes the contract back to ``CERTIFIED``.

Migration 008 is intentionally optional while older development databases and
unit-test fixtures are still in use.  Missing contract tables therefore make
invalidation a safe no-op; absence of a contract can never be mistaken for a
certified one by research readers.
"""
from __future__ import annotations

from uuid import UUID


CONTRACT_SOURCE = "KRX"
CONTRACT_ASSET_TYPE = "stock"
CONTRACT_FIELD = "total_return_close"


def invalidate_krx_total_return(
    conn,
    *,
    reason: str,
    quality_run_id: UUID | None,
) -> bool:
    """Atomically demote an existing KRX total-return contract to BUILDING.

    Returns ``True`` when the contract row existed and was updated.  The
    caller owns the transaction; a failed price/action publish rolls this
    invalidation back with the source write.
    """
    rendered_reason = str(reason).strip()
    if not rendered_reason:
        raise ValueError("total-return invalidation reason must be non-empty")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.price_return_contract')"
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return False
        cur.execute(
            """
            UPDATE price_return_contract
            SET status='BUILDING',
                quality_run_id=%s,
                metadata=coalesce(metadata, '{}'::jsonb) ||
                    jsonb_strip_nulls(jsonb_build_object(
                        'invalidated_reason', %s::text,
                        'invalidated_by_run_id', %s::uuid::text,
                        'invalidated_at', now()
                    )),
                certified_at=NULL,
                updated_at=now()
            WHERE source=%s
              AND asset_type=%s
              AND field_name=%s
            """,
            (
                quality_run_id,
                rendered_reason,
                quality_run_id,
                CONTRACT_SOURCE,
                CONTRACT_ASSET_TYPE,
                CONTRACT_FIELD,
            ),
        )
        return cur.rowcount > 0
