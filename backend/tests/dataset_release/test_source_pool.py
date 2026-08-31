from __future__ import annotations

import threading
import time

import pytest

import backend.services.dataset_release.source_pool as source_pool_module
from backend.services.dataset_release.profile import ResourcePolicy, apply_resource_overrides
from backend.services.dataset_release.source_pool import (
    ReadOnlySourcePool,
    SourcePoolClosed,
    SourceReadOnlyViolation,
    SourceRowChunkTooLarge,
)


class FakeCursor:
    def __init__(self, connection, *, oversize: bool = False):
        self.connection = connection
        self.rows = []
        self.offset = 0
        self.closed = False
        self.oversize = oversize
        self.itersize = None

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))
        if "current_setting('transaction_read_only')" in sql:
            self.rows = [("on",)]
        elif "set_config" in sql:
            self.rows = [("300000",)]
        else:
            self.rows = list(self.connection.query_rows)
        self.offset = 0

    def fetchone(self):
        if self.offset >= len(self.rows):
            return None
        row = self.rows[self.offset]
        self.offset += 1
        return row

    def fetchmany(self, size):
        actual = size + 1 if self.oversize else size
        rows = self.rows[self.offset : self.offset + actual]
        self.offset += len(rows)
        return rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows=((1,), (2,), (3,))):
        self.query_rows = list(rows)
        self.executed = []
        self.sessions = []
        self.rollbacks = 0
        self.closed = False

    def set_session(self, **kwargs):
        self.sessions.append(kwargs)

    def cursor(self, name=None):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_independent_pool_enforces_readonly_session_and_reuses_own_connection() -> None:
    created = []

    def factory():
        connection = FakeConnection()
        created.append(connection)
        return connection

    pool = ReadOnlySourcePool(factory, ResourcePolicy())
    assert pool.fetch_all_small("SELECT value FROM fixture", max_rows=3) == [
        (1,),
        (2,),
        (3,),
    ]
    assert pool.fetch_all_small("SELECT value FROM fixture", max_rows=3) == [
        (1,),
        (2,),
        (3,),
    ]

    assert len(created) == 1
    assert created[0].sessions[-1] == {
        "isolation_level": "REPEATABLE READ",
        "readonly": True,
        "deferrable": False,
        "autocommit": False,
    }
    assert created[0].rollbacks == 2
    assert (
        "SELECT set_config('statement_timeout', %s, true)",
        ("300000",),
    ) in created[0].executed
    assert pool.stats().peak_active_row_queries == 1
    pool.close()
    assert created[0].closed is True
    with pytest.raises(SourcePoolClosed):
        with pool.connection():
            pass


def test_source_pool_hard_bounds_and_readonly_sql_guard() -> None:
    assert not hasattr(source_pool_module, "SourcePoolConfig")

    pool = ReadOnlySourcePool(FakeConnection, ResourcePolicy())
    with pytest.raises(SourceReadOnlyViolation):
        with pool.row_stream("UPDATE market.x SET value=1"):
            pass
    with pytest.raises(SourceReadOnlyViolation, match="multiple"):
        with pool.row_stream("SELECT 1; SELECT 2"):
            pass
    with pytest.raises(ValueError, match="fetch_rows"):
        with pool.row_stream("SELECT 1", fetch_rows=0):
            pass
    pool.close()


def test_row_query_semaphore_serializes_two_threads() -> None:
    pool = ReadOnlySourcePool(FakeConnection, ResourcePolicy())
    first_inside = threading.Event()
    release_first = threading.Event()
    second_inside = threading.Event()

    def first():
        with pool.row_stream("SELECT 1") as rows:
            first_inside.set()
            assert next(rows) == (1,)
            release_first.wait(timeout=3)

    def second():
        with pool.row_stream("SELECT 1") as rows:
            second_inside.set()
            assert next(rows) == (1,)

    thread_one = threading.Thread(target=first)
    thread_two = threading.Thread(target=second)
    thread_one.start()
    assert first_inside.wait(timeout=3)
    thread_two.start()
    time.sleep(0.05)
    assert not second_inside.is_set()
    release_first.set()
    thread_one.join(timeout=3)
    thread_two.join(timeout=3)

    assert second_inside.is_set()
    assert pool.stats().peak_active_row_queries == 1
    assert pool.stats().row_query_count == 2
    pool.close()


def test_driver_cannot_return_more_than_fetch_bound() -> None:
    connection = FakeConnection(rows=((1,), (2,), (3,)))
    policy = apply_resource_overrides(ResourcePolicy(), {"validation_read_chunk_rows": 2}, source="test")
    pool = ReadOnlySourcePool(lambda: connection, policy)

    def cursor_factory(conn, _name):
        return FakeCursor(conn, oversize=True)

    with pytest.raises(SourceRowChunkTooLarge, match="driver returned"):
        with pool.row_stream(
            "SELECT value FROM fixture",
            fetch_rows=2,
            cursor_factory=cursor_factory,
        ) as rows:
            list(rows)
    pool.close()
