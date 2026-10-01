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


@pytest.mark.parametrize("snapshot_id", ["0000000C-0016AC39-1", "000000af-000001bc-12"])
def test_hexadecimal_snapshot_identity_is_shared_by_coordinator_and_source_readers(
    monkeypatch: pytest.MonkeyPatch, snapshot_id: str
) -> None:
    from backend.services.dataset_release.source_authority import imported_source_session_factory

    monkeypatch.setattr(
        Cursor, "fetchone", lambda _self: (snapshot_id, datetime(2026, 10, 1, tzinfo=UTC))
    )
    connections: list[Connection] = []

    def factory() -> Connection:
        value = Connection()
        connections.append(value)
        return value

    with MonthlySnapshotCoordinator(
        factory,
        repair_watermark_reader=lambda _connection: "repair-hex",
        overlapping_repair_reader=lambda _connection, _watermark: (),
    ) as snapshot:
        assert snapshot.read(lambda _connection, identity: identity.snapshot_id) == snapshot_id
        assert callable(imported_source_session_factory(snapshot_id))
    assert connections[1].commands[1] == f"SET TRANSACTION SNAPSHOT '{snapshot_id}'"
    assert all(connection.closed and connection.rollback_count for connection in connections)


@pytest.mark.parametrize(
    "snapshot_id",
    ["", "0000000G-0016AC39-1", "0000000C-0016AC39--1", "0000000C-0016AC39-1\n",
     "0000000C-0016AC39-1'; SELECT 1; --", "0000000C-0016AC39-１"],
)
def test_malformed_snapshot_is_rejected_before_import_and_connection_is_closed(
    monkeypatch: pytest.MonkeyPatch, snapshot_id: str
) -> None:
    from backend.services.dataset_release.source_authority import imported_source_session_factory

    monkeypatch.setattr(
        Cursor, "fetchone", lambda _self: (snapshot_id, datetime(2026, 10, 1, tzinfo=UTC))
    )
    connection = Connection()
    with pytest.raises(MonthlySnapshotError, match="invalid snapshot identity"):
        with MonthlySnapshotCoordinator(
            lambda: connection,
            repair_watermark_reader=lambda _connection: "repair-hex",
            overlapping_repair_reader=lambda _connection, _watermark: (),
        ):
            pytest.fail("invalid identity must fail before any imported reader")
    with pytest.raises(ValueError, match="snapshot identity is invalid"):
        imported_source_session_factory(snapshot_id)
    assert connection.closed and connection.rollback_count == 1
    assert not any(command.startswith("SET TRANSACTION SNAPSHOT") for command in connection.commands)
