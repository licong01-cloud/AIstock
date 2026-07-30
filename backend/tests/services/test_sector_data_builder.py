from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from backend.services import sector_data_builder as module


class _Cursor:
    def __init__(self, preflight=(0, 0, 0, 0, 0, 0, 0), build_rows=7):
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


def test_build_date_uses_dynamic_industry_mapping_without_persisted_identity(monkeypatch):
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
    for _, params in cursor.executed:
        assert params == {
            "trade_date": dt.date(2026, 7, 22),
            "live_universe_key": module.DEFAULT_ST_PIT_UNIVERSE_KEY,
            "qe_universe_pattern": f"{module.IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX}%",
        }
    assert "market.stock_universe_pit_spans" in module._BUILD_DAY_SQL
    assert "JOIN authoritative_universes USING (universe_key)" in module._BUILD_DAY_SQL
    assert "market.sw_index_member" in module._BUILD_DAY_SQL
    assert "l1_code, l2_code, mapping_in_date" not in module._BUILD_DAY_SQL
    assert "mapping_in_date     = EXCLUDED.mapping_in_date" not in module._BUILD_DAY_SQL
    assert "DISTINCT ON" not in module._BUILD_DAY_SQL


@pytest.mark.parametrize(
    ("preflight", "message"),
    [
        ((1, 0, 0, 0, 0, 0, 0), "universe_not_ready=1"),
        ((0, 1, 0, 0, 0, 0, 0), "missing_pit_mappings=1"),
        ((0, 0, 1, 0, 0, 0, 0), "ambiguous_latest_mappings=1"),
        ((0, 0, 0, 1, 0, 0, 0), "invalid_mapping_identities=1"),
        ((0, 0, 0, 0, 1, 0, 0), "missing_sw_daily_facts=1"),
        ((0, 0, 0, 0, 0, 1, 0), "missing_l2_moneyflow_facts=1"),
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


def test_preflight_exempts_unpublished_l2_from_sw_daily_contract():
    sql = module._PREFLIGHT_DAY_SQL

    assert "unpublished_l2 AS" in sql
    assert "market.sw_index_classify" in sql
    assert "is_pub = '0'" in sql
    assert "pit.l2_code NOT IN (SELECT index_code FROM unpublished_l2)" in sql
    assert "pit.l2_code IN (SELECT index_code FROM unpublished_l2)" in sql


def test_build_date_exempted_unpublished_l2_logs_warning_and_builds(monkeypatch, caplog):
    cursor = _Cursor(preflight=(0, 0, 0, 0, 0, 0, 16))
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    with caplog.at_level("WARNING", logger=module.logger.name):
        rows = module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert rows == 7
    assert connection.commits == 1
    assert [sql for sql, _ in cursor.executed] == [
        module._PREFLIGHT_DAY_SQL,
        module._DELETE_STALE_DAY_SQL,
        module._BUILD_DAY_SQL,
    ]
    assert any(
        "16 stocks exempted" in record.getMessage()
        and "is_pub=0" in record.getMessage()
        for record in caplog.records
    )


def test_schema_and_retirement_keep_sector_data_fact_only():
    root = Path(__file__).resolve().parents[3]
    schema = (root / "scripts/create_sw_sector_tables.py").read_text(encoding="utf-8")
    retirement = (
        root / "backend/db/migrations/sector_data_pit_identity_retirement_v1.sql"
    ).read_text(encoding="utf-8")

    for column in ("l1_code", "l2_code", "mapping_in_date"):
        assert f"{column}              TEXT" not in schema
        assert f"DROP COLUMN IF EXISTS {column}" in retirement

    assert not (root / "backend/db/migrations/sector_data_pit_identity_v1.sql").exists()
    assert "SECTOR_DATA_PERSISTED_PIT_IDENTITY_RETIREMENT_INCOMPLETE" in retirement
    assert "resolve industry identity dynamically from sw_index_member" in retirement
