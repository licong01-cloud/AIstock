from __future__ import annotations

import datetime as dt

import pytest

from backend.services import sector_data_repair as module


class _Cursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.executed = []
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._rows = self.responses.pop(0)

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, responses, *, host="127.0.0.1", port=5433):
        self._cursor = _Cursor(responses)
        self.commits = 0
        self.rollbacks = 0
        self.host = host
        self.port = port

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def get_dsn_parameters(self):
        return {"host": self.host, "port": str(self.port)}


def _resolution():
    return module.MappingResolution(
        trade_date=dt.date(2026, 7, 21),
        ts_code="000406.SZ",
        l1_code="801760.SI",
        l2_code="801767.SI",
        mapping_in_date=dt.date(1996, 6, 28),
    )


def test_audit_reports_every_blocker_without_mutation():
    connection = _Connection(
        [[(3, 1, 2, {"repairable": 1, "missing_sw_daily": 2}, {"missing_sw_daily": []})]]
    )

    audit = module.SectorDataRepairService().audit(connection=connection)

    assert audit.incomplete_rows == 3
    assert audit.repairable_rows == 1
    assert audit.blocker_count == 2
    assert audit.can_apply is False
    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert [sql for sql, _ in connection._cursor.executed] == [module._AUDIT_SQL]


def test_repair_fails_closed_before_update_when_any_blocker_exists():
    connection = _Connection(
        [
            [("aistock_dev", "127.0.0.1", 5433)],
            [],
            [(3, 2, 1, {"repairable": 2, "ambiguous_mapping": 1}, {})],
        ]
    )

    with pytest.raises(module.SectorDataRepairContractError, match="blocker_count=1"):
        module.SectorDataRepairService().repair(
            target="dev", connection=connection
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert [sql for sql, _ in connection._cursor.executed] == [
        module._DATABASE_IDENTITY_SQL,
        module._LOCK_SQL,
        module._AUDIT_SQL,
    ]


def test_repair_updates_all_rows_and_requires_clean_readback():
    connection = _Connection(
        [
            [("aistock_dev", "127.0.0.1", 5433)],
            [],
            [(2, 2, 0, {"repairable": 2}, {})],
            [[dt.date(2026, 7, 21), "000001.SZ"], [dt.date(2026, 7, 21), "000002.SZ"]],
            [(0, 0)],
        ]
    )

    result = module.SectorDataRepairService().repair(
        target="dev", connection=connection
    )

    assert result.updated_rows == 2
    assert result.readback_incomplete_rows == 0
    assert result.readback_source_mismatches == 0
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert [sql for sql, _ in connection._cursor.executed][-2:] == [
        module._UPDATE_SQL,
        module._READBACK_SQL,
    ]


def test_repair_rolls_back_when_readback_differs_from_canonical_source():
    connection = _Connection(
        [
            [("aistock_dev", "127.0.0.1", 5433)],
            [],
            [(1, 1, 0, {"repairable": 1}, {})],
            [[dt.date(2026, 7, 21), "000001.SZ"]],
            [(0, 1)],
        ]
    )

    with pytest.raises(module.SectorDataRepairContractError, match="source_mismatches=1"):
        module.SectorDataRepairService().repair(
            target="dev", connection=connection
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_production_apply_requires_separate_authorization_before_database_access():
    connection = _Connection([])

    with pytest.raises(
        module.SectorDataRepairTargetError,
        match="PRODUCTION_NOT_AUTHORIZED",
    ):
        module.SectorDataRepairService().repair(
            target="production", connection=connection
        )

    assert connection._cursor.executed == []
    assert connection.rollbacks == 1


def test_resolution_must_be_unique_exact_and_point_in_time():
    service = module.SectorDataRepairService()
    resolution = _resolution()

    with pytest.raises(module.SectorDataRepairContractError, match="DUPLICATE_RESOLUTION"):
        service.audit([resolution, resolution], connection=_Connection([]))

    with pytest.raises(module.SectorDataRepairContractError, match="FUTURE_RESOLUTION"):
        service.audit(
            [
                module.MappingResolution(
                    trade_date=dt.date(2026, 7, 21),
                    ts_code="000001.SZ",
                    l1_code="801010.SI",
                    l2_code="801011.SI",
                    mapping_in_date=dt.date(2026, 7, 22),
                )
            ],
            connection=_Connection([]),
        )


def test_sql_contract_rebuilds_all_facts_without_guessing_or_partial_mode():
    assert "DISTINCT ON" not in module._AUDIT_SQL
    assert "valid_resolutions" in module._AUDIT_SQL
    assert "unresolved_l2_membership" in module._AUDIT_SQL
    assert "incomplete_member_count" in module._AUDIT_SQL
    assert "source.repair_status = 'repairable'" in module._UPDATE_SQL
    assert "sw2_mf_net_vol = source.sw2_mf_net_vol" in module._UPDATE_SQL
    assert "IS DISTINCT FROM source.sw2_mf_net_vol" in module._READBACK_SQL
