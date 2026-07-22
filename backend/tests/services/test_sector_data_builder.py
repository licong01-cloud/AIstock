from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from backend.services import sector_data_builder as module


class _Cursor:
    def __init__(self, preflight=(0, 0, 0, 0), build_rows=7):
        self.preflight = preflight
        self.build_rows = build_rows
        self.executed = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))
        self.rowcount = self.build_rows if sql is module._BUILD_DAY_SQL else 0

    def fetchone(self):
        return self.preflight


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def test_build_date_persists_exact_pit_identity_and_removes_stale_rows(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    rows = module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert rows == 7
    assert connection.commits == 1
    assert [sql for sql, _ in cursor.executed] == [
        module._PREFLIGHT_DAY_SQL,
        module._DELETE_STALE_DAY_SQL,
        module._BUILD_DAY_SQL,
    ]
    assert "l1_code, l2_code, mapping_in_date" in module._BUILD_DAY_SQL
    assert "l1_code             = EXCLUDED.l1_code" in module._BUILD_DAY_SQL
    assert "DISTINCT ON" not in module._BUILD_DAY_SQL


@pytest.mark.parametrize(
    ("preflight", "message"),
    [
        ((1, 0, 0, 0), "ambiguous_latest_mappings=1"),
        ((0, 1, 0, 0), "invalid_mapping_identities=1"),
        ((0, 0, 1, 0), "missing_sw_daily_facts=1"),
        ((0, 0, 0, 1), "missing_l2_moneyflow_facts=1"),
    ],
)
def test_build_date_fails_loudly_before_mutation(monkeypatch, preflight, message):
    cursor = _Cursor(preflight=preflight)
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    with pytest.raises(module.SectorDataBuildContractError, match=message):
        module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert connection.commits == 0
    assert [sql for sql, _ in cursor.executed] == [module._PREFLIGHT_DAY_SQL]


def test_schema_and_migration_keep_identity_contract_aligned():
    root = Path(__file__).resolve().parents[3]
    schema = (root / "scripts/create_sw_sector_tables.py").read_text(encoding="utf-8")
    migration = (
        root / "backend/db/migrations/sector_data_pit_identity_v1.sql"
    ).read_text(encoding="utf-8")

    for column in ("l1_code", "l2_code", "mapping_in_date"):
        assert column in schema
        assert f"ADD COLUMN IF NOT EXISTS {column}" in migration
        assert f"ALTER COLUMN {column} SET NOT NULL" in migration

    assert "SECTOR_DATA_PIT_IDENTITY_BACKFILL_INCOMPLETE" in migration
    assert "SECTOR_DATA_PERSISTED_L2_FACT_INCOMPLETE_OR_CONFLICTING" in migration
    assert "IS NOT DISTINCT FROM candidate.close" in migration
    assert "COUNT(DISTINCT sw2_mf_net_vol) > 1" in migration
