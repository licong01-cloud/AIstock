"""One PostgreSQL repeatable-read snapshot for a monthly source input set.

The coordinator deliberately exposes no SQL or table selection to API callers.
Data-owned producers receive an already imported read-only transaction and
stream their registered queries before the coordinator transaction closes.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Protocol, Sequence, TypeVar

from .monthly_repair_journal import ManagedRepairImpactJournal


SNAPSHOT_ID_RE = re.compile(r"^[0-9]+-[0-9A-Fa-f]+-[0-9]+$")
T = TypeVar("T")


class SnapshotConnection(Protocol):
    autocommit: bool

    def cursor(self): ...  # type: ignore[no-untyped-def]

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MonthlySnapshotError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MonthlySnapshotIdentity:
    snapshot_id: str
    source_as_of: str
    initial_repair_watermark: str


class MonthlySnapshotCoordinator(AbstractContextManager["MonthlySnapshotCoordinator"]):
    """Keep one exporter transaction alive while bounded readers import it."""

    def __init__(
        self,
        connection_factory: Callable[[], SnapshotConnection],
        *,
        repair_watermark_reader: Callable[[SnapshotConnection], str],
        overlapping_repair_reader: Callable[[SnapshotConnection, str], Sequence[str]],
    ) -> None:
        self._connection_factory = connection_factory
        self._watermark_reader = repair_watermark_reader
        self._overlapping_repair_reader = overlapping_repair_reader
        self._coordinator: SnapshotConnection | None = None
        self.identity: MonthlySnapshotIdentity | None = None
        self._lost = False

    @staticmethod
    def _begin(connection: SnapshotConnection) -> None:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    def __enter__(self) -> "MonthlySnapshotCoordinator":
        if self._coordinator is not None:
            raise MonthlySnapshotError("monthly snapshot coordinator is already open")
        connection = self._connection_factory()
        try:
            self._begin(connection)
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_export_snapshot(), transaction_timestamp()")
                row = cursor.fetchone()
            if not row or len(row) != 2:
                raise MonthlySnapshotError("database did not return a snapshot identity")
            snapshot_id = str(row[0] or "")
            if SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
                raise MonthlySnapshotError("database returned an invalid snapshot identity")
            source_as_of = row[1]
            if not isinstance(source_as_of, datetime) or source_as_of.tzinfo is None:
                raise MonthlySnapshotError("database snapshot timestamp is invalid")
            watermark = str(self._watermark_reader(connection) or "")
            if not watermark:
                raise MonthlySnapshotError("repair impact watermark is empty")
            self._coordinator = connection
            self.identity = MonthlySnapshotIdentity(
                snapshot_id=snapshot_id,
                source_as_of=source_as_of.isoformat(),
                initial_repair_watermark=watermark,
            )
            return self
        except Exception:
            try:
                connection.rollback()
            finally:
                connection.close()
            raise

    def read(self, reader: Callable[[SnapshotConnection, MonthlySnapshotIdentity], T]) -> T:
        if self._coordinator is None or self.identity is None or self._lost:
            raise MonthlySnapshotError("monthly snapshot is not available")
        connection = self._connection_factory()
        try:
            self._begin(connection)
            # PostgreSQL does not accept a bind parameter in SET TRANSACTION
            # SNAPSHOT.  The exported server value is restricted before the
            # quoted literal is emitted.
            with connection.cursor() as cursor:
                cursor.execute(f"SET TRANSACTION SNAPSHOT '{self.identity.snapshot_id}'")
            return reader(connection, self.identity)
        except Exception as exc:
            self._lost = True
            raise MonthlySnapshotError("monthly snapshot reader failed; the input set is invalid") from exc
        finally:
            try:
                connection.rollback()
            finally:
                connection.close()

    def assert_no_overlapping_repairs(self) -> None:
        if self._coordinator is None or self.identity is None or self._lost:
            raise MonthlySnapshotError("monthly snapshot is not available")
        current = self._connection_factory()
        try:
            # Deliberately do not import the old snapshot here: this seal-time
            # read must observe repairs committed after the source view began.
            overlaps = tuple(
                str(value)
                for value in self._overlapping_repair_reader(
                    current, self.identity.initial_repair_watermark
                )
            )
        finally:
            current.close()
        if overlaps:
            raise MonthlySnapshotError(
                f"managed repairs overlapped source materialization: {overlaps[:3]}"
            )

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        connection = self._coordinator
        self._coordinator = None
        if connection is not None:
            try:
                connection.rollback()
            finally:
                connection.close()


def managed_monthly_snapshot(
    connection_factory: Callable[[], SnapshotConnection],
    *,
    journal: ManagedRepairImpactJournal | None = None,
) -> MonthlySnapshotCoordinator:
    """Construct the production coordinator with the managed write journal.

    Keeping this wiring in data-owned code prevents a stage producer from
    substituting permissive watermark callbacks.
    """

    selected = journal or ManagedRepairImpactJournal()
    return MonthlySnapshotCoordinator(
        connection_factory,
        repair_watermark_reader=selected.initial_watermark,
        overlapping_repair_reader=selected.overlapping_repairs,
    )


__all__ = (
    "MonthlySnapshotCoordinator",
    "MonthlySnapshotError",
    "MonthlySnapshotIdentity",
    "SnapshotConnection",
    "managed_monthly_snapshot",
)
