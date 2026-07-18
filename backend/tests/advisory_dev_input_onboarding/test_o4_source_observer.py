from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import DatabaseConnectionConfig
from backend.services.advisory_phase1.source_observer import REASON_OBSERVER_CONFIG_INVALID, SourceObserverError
from backend.services.advisory_phase1.source_observer import o4_advisory_input_source_observer_config
from backend.services.advisory_phase1.source_observer_postgres import explicit_dev_observer_connection
from backend.services.advisory_dev_input_onboarding.phase1e_source_mapping import compiled_o4_source_mapping_registry


class _Cursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql: str) -> None:
        return None

    def fetchone(self) -> tuple[object, ...]:
        return self._row


class _Connection:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self.row)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


def _config(target: TargetLabel = TargetLabel.DEV) -> DatabaseConnectionConfig:
    return DatabaseConnectionConfig(
        target_label=target,
        host="dev-db",
        port=5432,
        database="aistock_dev",
        user="aistock",
        password="secret",
        environment_contract_hash="a" * 64,
    )


def test_explicit_dev_observer_factory_commits_only_the_exact_dev_identity() -> None:
    connection = _Connection(("aistock_dev", 5432, "off"))

    with explicit_dev_observer_connection(_config(), connector=lambda **_kwargs: connection) as opened:
        assert opened is connection
        assert connection.autocommit is False

    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1


def test_explicit_dev_observer_factory_rejects_production_without_connecting() -> None:
    calls = 0

    def connector(**_kwargs):
        nonlocal calls
        calls += 1
        return _Connection(("aistock", 5432, "off"))

    with pytest.raises(SourceObserverError) as captured:
        with explicit_dev_observer_connection(_config(TargetLabel.PRODUCTION), connector=connector):
            pass

    assert captured.value.reason_code == REASON_OBSERVER_CONFIG_INVALID
    assert calls == 0


def test_o4_observer_cli_has_no_enable_approval_or_production_target_switch() -> None:
    root = Path(__file__).resolve().parents[3]
    source = (root / "scripts/advisory_phase1_source_observer.py").read_text(encoding="utf-8")

    forbidden = (
        "SOURCE_OBSERVER_ENABLED",
        "--target-db",
        "--prod",
        "approved_by",
        "approval_status",
        "acknowledgement",
        "backup",
        "force",
        "skip",
    )
    assert all(token not in source for token in forbidden)
    assert "--env-file" in source


def test_o4_observer_config_covers_all_audit_backed_physical_templates() -> None:
    config = o4_advisory_input_source_observer_config()
    configured_templates = {item.query_template_id for item in config.dataset_specs}
    mapped_templates = {
        item.observer_query_template_id
        for entry in compiled_o4_source_mapping_registry().entries
        for item in entry.physical_requirements
    }

    assert mapped_templates - configured_templates == {
        "market_stock_universe_pit_spans_as_of_v2",
        "market_stock_universe_pit_state_as_of_v2",
    }
    assert all(item.audit_dataset_name for item in config.dataset_specs)
