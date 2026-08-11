import inspect

from pipeline.silver import load
from pipeline.silver_quality import backfill, ecs_backfill, konex_cleanup


def _position(source: str, token: str) -> int:
    position = source.find(token)
    assert position >= 0, token
    return position


def test_daily_lock_precedes_asset_and_price_mutations():
    source = inspect.getsource(load.incremental)
    lock = _position(source, "acquire_return_writer_transaction_lock(conn)")
    assert lock < _position(source, "assets.publish(")
    assert lock < _position(source, '"DELETE FROM price_daily "')
    assert lock < _position(source, "prices.publish(")


def test_initial_backfill_lock_precedes_first_final_table_check():
    source = inspect.getsource(backfill._publish)
    assert _position(
        source, "acquire_return_writer_transaction_lock(conn)"
    ) < _position(source, "_assert_final_empty(conn)")


def test_ecs_truncate_lock_precedes_first_table_read_or_mutation():
    source = inspect.getsource(ecs_backfill._prepare_rds)
    assert _position(
        source, "acquire_return_writer_transaction_lock(conn)"
    ) < _position(source, "SELECT current_database()")


def test_konex_cleanup_lock_precedes_scope_and_delete():
    source = inspect.getsource(konex_cleanup.run)
    lock = _position(source, "acquire_return_writer_transaction_lock(conn)")
    # The first _prepare_scope is the dry-run branch.  Compare against the
    # apply-branch occurrence that follows the common writer lock.
    apply_scope = source.find("_prepare_scope(conn)", lock)
    assert lock < apply_scope
    assert lock < source.find('DELETE FROM price_daily WHERE market=\'KONEX\'', lock)
