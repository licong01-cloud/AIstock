"""Opt-in DEV-DB readback gate for the Advisory dated binding lifecycle."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import pytest

from backend.services.advisory_program import (
    PACKAGE_MODE_SINGLE,
    AdvisoryProgramPGRepository,
    AdvisoryProgramService,
)

pytest_plugins = ("backend.tests.paper_trading_v2.fixtures_dev_db",)
pytestmark = pytest.mark.skipif(
    os.environ.get("AISTOCK_DEV_DB_E2E") != "1",
    reason="set AISTOCK_DEV_DB_E2E=1 to run the rollback-only DEV-DB gate",
)


class _NoCommitConn:
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn = conn

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        return self._conn.cursor(*args, **kwargs)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        self._conn.rollback()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@contextmanager
def _conn_factory(conn: psycopg2.extensions.connection) -> Iterator[_NoCommitConn]:
    yield _NoCommitConn(conn)


class _Calendar:
    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        current = start_date
        rows: list[date] = []
        while current <= end_date:
            if current.weekday() < 5:
                rows.append(current)
            current += timedelta(days=1)
        return rows

    def next_trading_day(self, anchor_date: date, *, inclusive: bool = False) -> date:
        current = anchor_date if inclusive else anchor_date + timedelta(days=1)
        while current.weekday() >= 5:
            current += timedelta(days=1)
        return current


def test_devdb_binding_replace_persists_right_open_interval_and_payload_readback(dev_db_conn) -> None:
    repository = AdvisoryProgramPGRepository(conn_factory=lambda: _conn_factory(dev_db_conn))
    service = AdvisoryProgramService(
        repository=repository,
        calendar_provider=_Calendar(),
        now_provider=lambda: datetime(2026, 5, 29, 20, 0, tzinfo=UTC),
    )
    program = service.create_program(
        program_name="test_int_advisory_binding_lifecycle",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["test_int_pkg_a"],
        target_count=5,
    )
    initial = repository.get_active_binding_version(program.program_id)
    assert initial is not None
    defaults = service.binding_defaults(program.program_id)

    applied = service.apply_binding(
        program.program_id,
        binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["test_int_pkg_b"], "target_count": 5},
        activation_reason="rollback-only devdb readback",
        expected_program_version=defaults["expected_program_version"],
        expected_binding_version_id=defaults["expected_binding_version_id"],
    )
    successor_id = applied["binding"]["binding_version_id"]
    rows = repository.list_binding_versions(program.program_id)
    retired = next(row for row in rows if row.binding_version_id == initial.binding_version_id)
    successor = next(row for row in rows if row.binding_version_id == successor_id)
    assert retired.activation_status == "RETIRED"
    assert retired.effective_to_trade_date == successor.effective_from_trade_date
    assert successor.activation_status == "ACTIVE"
    assert successor.effective_to_trade_date is None

    with dev_db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT activation_status, effective_to_trade_date, binding_payload_json
            FROM app.advisory_strategy_binding_version
            WHERE binding_version_id = %s
            """,
            (initial.binding_version_id,),
        )
        raw = cur.fetchone()
    assert raw is not None
    assert raw["activation_status"] == raw["binding_payload_json"]["activation_status"] == "RETIRED"
    assert str(raw["effective_to_trade_date"]) == raw["binding_payload_json"]["effective_to_trade_date"]
