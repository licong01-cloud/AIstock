from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.db import init_trading_core_v2_schema
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.simulation_runtime.successor_models import (
    SimulationLedgerScopeKind,
    SimulationLedgerScopeV1,
)


ROOT = Path(__file__).resolve().parents[3]
APPLY = ROOT / "backend/migrations/localsim_product_cutover_bridge_20260831.sql"
PREFLIGHT = ROOT / "backend/migrations/localsim_product_cutover_bridge_20260831.preflight.sql"
ROLLBACK = ROOT / "backend/migrations/localsim_product_cutover_bridge_20260831.rollback.sql"


def _scope(*, scope_id: str, kind: SimulationLedgerScopeKind, native_account_id: str | None):
    identity = {
        "schema_version": "simulation_ledger_scope_v1",
        "ledger_scope_id": scope_id,
        "scope_kind": kind.value,
        "source_identity": scope_id,
        "native_account_id": native_account_id,
    }
    return SimulationLedgerScopeV1(
        ledger_scope_id=scope_id,
        ledger_scope_hash=canonical_json_sha256(identity),
        scope_kind=kind,
        source_identity=scope_id,
        native_account_id=native_account_id,
        created_by="test",
        created_at=datetime(2026, 8, 31, tzinfo=UTC),
    )


def test_ledger_scope_identity_is_immutable_and_does_not_model_a_shadow_portfolio() -> None:
    legacy = _scope(
        scope_id="legacy_portfolio_1",
        kind=SimulationLedgerScopeKind.LEGACY_PORTFOLIO,
        native_account_id=None,
    )
    native = _scope(
        scope_id="simacct_native_1",
        kind=SimulationLedgerScopeKind.SUCCESSOR_NATIVE,
        native_account_id="simacct_native_1",
    )

    assert legacy.native_account_id is None
    assert native.native_account_id == native.ledger_scope_id
    with pytest.raises(ValueError, match="legacy ledger scope"):
        _scope(
            scope_id="legacy_portfolio_1",
            kind=SimulationLedgerScopeKind.LEGACY_PORTFOLIO,
            native_account_id="simacct_forbidden",
        )


def test_cutover_bridge_migration_and_bootstrap_use_exact_two_runtime_active_fks() -> None:
    sql = APPLY.read_text(encoding="utf-8")
    bootstrap = init_trading_core_v2_schema.DDL[-1]

    assert "CREATE TABLE IF NOT EXISTS paper_v2.simulation_ledger_scope_v1" in sql
    assert "CREATE TABLE IF NOT EXISTS paper_v2.simulation_ledger_scope_v1" in bootstrap
    assert "fk_paper_v2_run_ledger_scope_v1" in sql
    assert "fk_paper_v2_intraday_snapshots_ledger_scope_v1" in sql
    assert "new_runtime_fk_count <> 2" in sql
    assert "paper_v2.broker_account_binding" not in sql
    assert "COMMIT;" not in bootstrap
    assert "SET LOCAL" not in bootstrap


def test_cutover_bridge_preflight_is_read_only_and_rollback_is_reference_guarded() -> None:
    preflight = PREFLIGHT.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")

    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY" in preflight
    for forbidden in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM"):
        assert forbidden not in preflight
    assert "SUCCESSOR_NATIVE" in rollback
    assert "legacy_localsim_account_lineage_v1" in rollback
    assert rollback.index("DROP CONSTRAINT IF EXISTS fk_paper_v2_run_ledger_scope_v1") < rollback.index(
        "DROP TABLE paper_v2.simulation_ledger_scope_v1"
    )
