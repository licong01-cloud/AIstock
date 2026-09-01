"""Independent read-only PostgreSQL pool for dataset-release workers.

The pool is intentionally process-local and never imports or mutates
``backend.db.pg_pool``.  At most four connections may exist, while a single
semaphore serializes row-producing queries so batching cannot multiply memory.
Every borrowed transaction also applies and reads back a fixed query-local
PostgreSQL memory/parallelism contract; global database settings are untouched.
"""

from __future__ import annotations

import queue
import re
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence

from .errors import DatasetReleaseError
from .profile import ResourcePolicy, validate_resource_policy


ConnectionFactory = Callable[[], Any]
_READ_ONLY_SQL = re.compile(r"^\s*(?:SELECT|WITH|SHOW|EXPLAIN)\b", re.IGNORECASE)


class SourcePoolError(DatasetReleaseError):
    """Base class for source-pool contract failures."""

    code = "DATASET_RELEASE_SOURCE_POOL_ERROR"


class SourcePoolClosed(SourcePoolError):
    """Raised when a closed pool is reused."""

    code = "DATASET_RELEASE_SOURCE_POOL_CLOSED"


class SourceReadOnlyViolation(SourcePoolError):
    """Raised before a caller attempts non-read-only SQL."""

    code = "BLOCKED_SOURCE_READ_ONLY_VIOLATION"


class SourceRowChunkTooLarge(SourcePoolError):
    """Raised when a driver violates the configured fetch bound."""

    code = "BLOCKED_SOURCE_ROW_CHUNK_TOO_LARGE"


class SourceQueryResourceContractViolation(SourcePoolError):
    """Raised when PostgreSQL does not confirm the source-query limits."""

    code = "BLOCKED_SOURCE_QUERY_RESOURCE_CONTRACT"


_DEFAULT_FETCH_ROWS = 10_000
SOURCE_QUERY_RESOURCE_CONTRACT_SCHEMA = "dataset_release_source_query_resource_v1"
SOURCE_QUERY_WORK_MEM_KIB = 8 * 1024
SOURCE_QUERY_MAX_PARALLEL_WORKERS_PER_GATHER = 1


@dataclass(frozen=True, slots=True)
class SourcePoolStats:
    created_connections: int
    idle_connections: int
    checked_out_connections: int
    active_row_queries: int
    peak_active_row_queries: int
    row_query_count: int


class RowCursor(Protocol):
    description: Sequence[Any] | None

    def execute(self, sql: str, params: Any = None) -> Any: ...

    def fetchmany(self, size: int) -> Sequence[Any]: ...

    def close(self) -> Any: ...


class ReadOnlySourcePool:
    """Small lazy pool with a hard single-row-stream admission gate."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        policy: ResourcePolicy,
    ) -> None:
        if not callable(connection_factory):
            raise TypeError("connection_factory must be callable")
        self.policy = validate_resource_policy(policy)
        self._max_connections = self.policy.db_pool_size
        self._row_query_concurrency = self.policy.row_query_concurrency
        self._statement_timeout_ms = self.policy.db_statement_timeout_seconds * 1_000
        self._max_fetch_rows = self.policy.validation_read_chunk_rows
        self._default_fetch_rows = min(_DEFAULT_FETCH_ROWS, self._max_fetch_rows)
        self._factory = connection_factory
        self._idle: queue.LifoQueue[Any] = queue.LifoQueue(maxsize=self._max_connections)
        self._row_gate = threading.BoundedSemaphore(self._row_query_concurrency)
        self._condition = threading.Condition()
        self._created = 0
        self._checked_out = 0
        self._active_row_queries = 0
        self._peak_active_row_queries = 0
        self._row_query_count = 0
        self._closed = False

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """Borrow one independently-owned READ ONLY, REPEATABLE READ connection."""

        connection = self._acquire_connection()
        discard = False
        try:
            self._prepare_connection(connection)
            yield connection
        except BaseException:
            discard = not self._safe_rollback(connection)
            raise
        else:
            discard = not self._safe_rollback(connection)
        finally:
            self._release_connection(connection, discard=discard)

    @contextmanager
    def row_stream(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        fetch_rows: int | None = None,
        cursor_factory: Callable[[Any, str], RowCursor] | None = None,
    ) -> Iterator[Iterator[tuple[Any, ...]]]:
        """Yield a bounded iterator while holding the global row-query semaphore.

        The context must remain open while the iterator is consumed.  A named
        server-side cursor is used by default when the driver supports it.
        """

        _assert_read_only_sql(sql)
        fetch_size = self._default_fetch_rows if fetch_rows is None else int(fetch_rows)
        if not 1 <= fetch_size <= self._max_fetch_rows:
            raise ValueError(f"fetch_rows must be in 1..{self._max_fetch_rows}")
        self._row_gate.acquire()
        with self._condition:
            self._active_row_queries += 1
            self._peak_active_row_queries = max(self._peak_active_row_queries, self._active_row_queries)
            self._row_query_count += 1
        try:
            with self.connection() as connection:
                cursor_name = f"dataset_release_{uuid.uuid4().hex}"
                cursor = (
                    cursor_factory(connection, cursor_name)
                    if cursor_factory is not None
                    else _open_cursor(connection, cursor_name)
                )
                try:
                    if hasattr(cursor, "itersize"):
                        cursor.itersize = fetch_size
                    cursor.execute(sql, params)
                    yield _bounded_cursor_rows(cursor, fetch_size)
                finally:
                    cursor.close()
        finally:
            with self._condition:
                self._active_row_queries -= 1
                self._condition.notify_all()
            self._row_gate.release()

    def fetch_all_small(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        max_rows: int = 10_000,
    ) -> list[tuple[Any, ...]]:
        """Materialize only explicitly small metadata queries."""

        if not 1 <= int(max_rows) <= self._max_fetch_rows:
            raise ValueError("max_rows exceeds source pool hard bound")
        rows: list[tuple[Any, ...]] = []
        with self.row_stream(sql, params, fetch_rows=max_rows) as stream:
            for row in stream:
                rows.append(row)
                if len(rows) > max_rows:
                    raise SourceRowChunkTooLarge(f"metadata query exceeded max_rows={max_rows}")
        return rows

    def stats(self) -> SourcePoolStats:
        with self._condition:
            return SourcePoolStats(
                created_connections=self._created,
                idle_connections=self._idle.qsize(),
                checked_out_connections=self._checked_out,
                active_row_queries=self._active_row_queries,
                peak_active_row_queries=self._peak_active_row_queries,
                row_query_count=self._row_query_count,
            )

    def close(self) -> None:
        """Close only connections created by this pool."""

        with self._condition:
            if self._checked_out:
                raise SourcePoolError("cannot close dataset source pool while connections are checked out")
            self._closed = True
            connections: list[Any] = []
            while True:
                try:
                    connections.append(self._idle.get_nowait())
                except queue.Empty:
                    break
            self._created = 0
            self._condition.notify_all()
        for connection in connections:
            _safe_close(connection)

    def _acquire_connection(self) -> Any:
        while True:
            with self._condition:
                if self._closed:
                    raise SourcePoolClosed("dataset source pool is closed")
                try:
                    connection = self._idle.get_nowait()
                except queue.Empty:
                    if self._created < self._max_connections:
                        self._created += 1
                        self._checked_out += 1
                        create = True
                    else:
                        self._condition.wait()
                        continue
                else:
                    self._checked_out += 1
                    create = False
            if not create:
                if _connection_closed(connection):
                    with self._condition:
                        self._created -= 1
                        self._checked_out -= 1
                        self._condition.notify_all()
                    continue
                return connection
            try:
                return self._factory()
            except BaseException:
                with self._condition:
                    self._created -= 1
                    self._checked_out -= 1
                    self._condition.notify_all()
                raise

    def _release_connection(self, connection: Any, *, discard: bool) -> None:
        should_close = discard or _connection_closed(connection)
        with self._condition:
            self._checked_out -= 1
            if self._closed or should_close:
                self._created -= 1
                close = True
            else:
                self._idle.put_nowait(connection)
                close = False
            self._condition.notify_all()
        if close:
            _safe_close(connection)

    def _prepare_connection(self, connection: Any) -> None:
        try:
            connection.set_session(
                isolation_level="REPEATABLE READ",
                readonly=True,
                deferrable=False,
                autocommit=False,
            )
        except (AttributeError, TypeError):
            # Lightweight test doubles and some DB-API wrappers expose only
            # autocommit.  The SQL transaction command remains fail-closed on
            # a real PostgreSQL server.
            if hasattr(connection, "autocommit"):
                connection.autocommit = False
            cursor = connection.cursor()
            try:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            finally:
                cursor.close()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, true),"
                "set_config('work_mem', %s, true),"
                "set_config('max_parallel_workers_per_gather', %s, true)",
                (
                    str(self._statement_timeout_ms),
                    f"{SOURCE_QUERY_WORK_MEM_KIB}kB",
                    str(SOURCE_QUERY_MAX_PARALLEL_WORKERS_PER_GATHER),
                ),
            )
            cursor.execute(
                "SELECT current_setting('transaction_read_only'),"
                "pg_size_bytes(current_setting('work_mem'))::bigint,"
                "current_setting('max_parallel_workers_per_gather')::integer"
            )
            row = cursor.fetchone() if hasattr(cursor, "fetchone") else None
            if not row or len(row) != 3:
                raise SourceQueryResourceContractViolation(
                    "database returned incomplete source-query resource settings"
                )
            if str(row[0]).lower() not in {"on", "true", "1"}:
                raise SourceReadOnlyViolation("database did not acknowledge transaction_read_only=on")
            try:
                work_mem_bytes = int(row[1])
                parallel_workers = int(row[2])
            except (TypeError, ValueError) as exc:
                raise SourceQueryResourceContractViolation(
                    "database returned invalid source-query resource settings"
                ) from exc
            if (
                work_mem_bytes != SOURCE_QUERY_WORK_MEM_KIB * 1024
                or parallel_workers != SOURCE_QUERY_MAX_PARALLEL_WORKERS_PER_GATHER
            ):
                raise SourceQueryResourceContractViolation(
                    "database did not acknowledge source-query resource settings"
                )
        finally:
            cursor.close()

    @staticmethod
    def _safe_rollback(connection: Any) -> bool:
        try:
            connection.rollback()
            return True
        except BaseException:
            return False


def _assert_read_only_sql(sql: str) -> None:
    value = str(sql or "")
    if not _READ_ONLY_SQL.match(value):
        raise SourceReadOnlyViolation("dataset source pool accepts read-only SQL only")
    stripped = value.rstrip().rstrip(";")
    if ";" in stripped:
        raise SourceReadOnlyViolation("multiple SQL statements are forbidden")


def _open_cursor(connection: Any, name: str) -> RowCursor:
    try:
        return connection.cursor(name=name)
    except TypeError:
        return connection.cursor()


def _bounded_cursor_rows(cursor: RowCursor, fetch_rows: int) -> Iterator[tuple[Any, ...]]:
    while True:
        batch = cursor.fetchmany(fetch_rows)
        if not batch:
            return
        if len(batch) > fetch_rows:
            raise SourceRowChunkTooLarge(f"driver returned {len(batch)} rows for fetch_rows={fetch_rows}")
        for row in batch:
            yield tuple(row)


def _connection_closed(connection: Any) -> bool:
    try:
        return bool(connection.closed)
    except AttributeError:
        return False


def _safe_close(connection: Any) -> None:
    try:
        connection.close()
    except BaseException:
        return
