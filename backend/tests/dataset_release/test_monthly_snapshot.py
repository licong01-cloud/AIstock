from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.services.dataset_release.monthly_snapshot import (
    MonthlySnapshotCoordinator,
    MonthlySnapshotError,
    managed_monthly_snapshot,
)


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.connection.commands.append(sql)

    def fetchone(self):  # type: ignore[no-untyped-def]
        return ("00000003-0000001B-1", datetime(2026, 10, 1, tzinfo=UTC))


class Connection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commands: list[str] = []
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> Cursor:
        return Cursor(self)

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def test_all_readers_import_one_snapshot_before_registered_queries() -> None:
    connections: list[Connection] = []

    def factory() -> Connection:
        value = Connection()
        connections.append(value)
        return value

    with MonthlySnapshotCoordinator(
        factory,
        repair_watermark_reader=lambda _connection: "repair-watermark-9",
        overlapping_repair_reader=lambda _connection, _watermark: (),
    ) as snapshot:
        result = snapshot.read(
            lambda connection, identity: (
                connection.cursor().execute("SELECT registered_source_fields"),
                identity.snapshot_id,
            )[1]
        )
        snapshot.assert_no_overlapping_repairs()
    assert result == "00000003-0000001B-1"
    assert connections[1].commands == [
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
        "SELECT registered_source_fields",
    ]
    assert all(connection.closed for connection in connections)


def test_failed_reader_invalidates_snapshot_attempt() -> None:
    connections: list[Connection] = []

    def factory() -> Connection:
        value = Connection()
        connections.append(value)
        return value

    with MonthlySnapshotCoordinator(
        factory,
        repair_watermark_reader=lambda _connection: "repair-watermark-9",
        overlapping_repair_reader=lambda _connection, _watermark: (),
    ) as snapshot:
        with pytest.raises(MonthlySnapshotError, match="input set is invalid"):
            snapshot.read(lambda _connection, _identity: (_ for _ in ()).throw(OSError("lost")))
        with pytest.raises(MonthlySnapshotError, match="not available"):
            snapshot.read(lambda _connection, _identity: None)


def test_seal_time_check_rejects_only_repairs_overlapping_materialized_scope() -> None:
    seen_watermarks: list[str] = []

    def overlapping_repairs(_connection: Connection, watermark: str) -> tuple[str, ...]:
        seen_watermarks.append(watermark)
        return ("repair-10:daily:000001.SZ:2026-09-30",)

    with MonthlySnapshotCoordinator(
        Connection,
        repair_watermark_reader=lambda _connection: "repair-watermark-9",
        overlapping_repair_reader=overlapping_repairs,
    ) as snapshot:
        with pytest.raises(MonthlySnapshotError, match="overlapped"):
            snapshot.assert_no_overlapping_repairs()
    assert seen_watermarks == ["repair-watermark-9"]


def test_unrelated_repairs_do_not_invalidate_snapshot() -> None:
    with MonthlySnapshotCoordinator(
        Connection,
        repair_watermark_reader=lambda _connection: "repair-watermark-9",
        overlapping_repair_reader=lambda _connection, _watermark: (),
    ) as snapshot:
        snapshot.assert_no_overlapping_repairs()


def test_production_factory_owns_managed_journal_callbacks() -> None:
    class Journal:
        def initial_watermark(self, _connection: Connection) -> str:
            return "repair-watermark-12"

        def overlapping_repairs(
            self, _connection: Connection, watermark: str
        ) -> tuple[str, ...]:
            assert watermark == "repair-watermark-12"
            return ()

    with managed_monthly_snapshot(Connection, journal=Journal()) as snapshot:  # type: ignore[arg-type]
        assert snapshot.identity is not None
        assert snapshot.identity.initial_repair_watermark == "repair-watermark-12"
        snapshot.assert_no_overlapping_repairs()
