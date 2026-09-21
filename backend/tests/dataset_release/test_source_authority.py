from __future__ import annotations

import pytest

from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.source_authority import (
    PRODUCTION_QUERY_SPECS,
    PostgresSourceSnapshotSession,
    SourceQuerySpec,
    imported_source_session_factory,
)


def test_production_source_allowlist_preserves_exact_daily_and_minute_ordering() -> None:
    daily = PRODUCTION_QUERY_SPECS["kline_daily_raw"]
    minute = PRODUCTION_QUERY_SPECS["kline_minute_raw"]

    assert daily.key_columns == ("ts_code", "trade_date")
    assert daily.sql.endswith("ORDER BY row_key, row_payload")
    assert minute.key_columns == ("ts_code", "trade_time", "freq")
    assert minute.date_range_policy == "timestamp_day_half_open"
    assert "source_row.trade_time < (%(end)s::date + interval '1 day')" in minute.sql
    assert minute.sql.endswith("ORDER BY source_row.ts_code,source_row.trade_time,source_row.freq")
    assert "to_jsonb(source_row)" not in daily.sql + minute.sql


def test_dated_source_query_cannot_bypass_refresh_audit_contract() -> None:
    with pytest.raises(ValueError, match="refresh-audit dataset"):
        SourceQuerySpec(
            query_id="fixture_daily",
            schema_name="market",
            table_name="fixture_daily",
            components=(Component.DAILY_BIN,),
            key_columns=("ts_code", "trade_date"),
            value_columns=("close",),
            derived_value_columns=(),
            required_columns=("ts_code", "trade_date", "close"),
            non_null_value_columns=("close",),
            date_expression="source_row.trade_date",
            start_policy="daily",
            query_version="fixture_v1",
        )


def test_imported_postgres_snapshot_identity_is_strict() -> None:
    with pytest.raises(ValueError, match="snapshot identity"):
        PostgresSourceSnapshotSession(
            ResourcePolicy(),
            imported_snapshot_id="x'; SELECT pg_sleep(1); --",
        )

    with pytest.raises(ValueError, match="snapshot identity"):
        imported_source_session_factory("not-a-snapshot")


def test_imported_source_factory_binds_every_session_to_same_snapshot() -> None:
    connections = []

    def connection_factory():
        value = object()
        connections.append(value)
        return value

    factory = imported_source_session_factory(
        "00000003-0000001B-1",
        connection_factory=connection_factory,
    )

    first = factory(ResourcePolicy())
    second = factory(ResourcePolicy())

    assert first is not second
    assert first._imported_snapshot_id == second._imported_snapshot_id == "00000003-0000001B-1"
