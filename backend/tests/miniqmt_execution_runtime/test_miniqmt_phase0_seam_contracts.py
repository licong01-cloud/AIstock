from __future__ import annotations

import inspect

import pytest

from backend.execution_algos.vnpy_style import (
    SOURCE_FILE_MAP,
    VnpyAction,
    create_vnpy_style_core,
)
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    MiniQMTGateway,
    QmtClientMiniQMTGateway,
)
from backend.services.miniqmt_execution_runtime.config import (
    MINIQMT_EXECUTION_RUNTIME_ENV,
    MiniQMTExecutionRuntimeKind,
    get_miniqmt_execution_runtime_kind,
)
from backend.services.miniqmt_execution_runtime.contracts import (
    MiniQMTStrategyLedgerOmsContract,
    MiniQMTVnpyAlgoCoreContract,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


def test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values() -> None:
    assert get_miniqmt_execution_runtime_kind({}) == MiniQMTExecutionRuntimeKind.COMPILER
    assert get_miniqmt_execution_runtime_kind({MINIQMT_EXECUTION_RUNTIME_ENV: "event_loop"}) == (
        MiniQMTExecutionRuntimeKind.EVENT_LOOP
    )

    with pytest.raises(ValueError, match="MINIQMT_EXECUTION_RUNTIME_UNSUPPORTED"):
        get_miniqmt_execution_runtime_kind({MINIQMT_EXECUTION_RUNTIME_ENV: "durable"})


def test_vnpy_style_algo_core_contract_is_shared_and_broker_neutral() -> None:
    core = create_vnpy_style_core(
        algo_code="SNIPER_MINIQMT",
        symbol="000001.SZ",
        side="BUY",
        price=10.5,
        volume=1000,
    )

    assert isinstance(core, MiniQMTVnpyAlgoCoreContract)
    assert all(isinstance(action, VnpyAction) for action in core.start())
    attribution = core.audit_metadata()["source_attribution"]
    assert attribution["upstream_source_file"] == SOURCE_FILE_MAP["SNIPER_MINIQMT"]
    assert attribution["upstream_commit"] == "4133987530eb28f3538d1983545d81c4f83d7d59"


def test_gateway_contract_signature_is_frozen_for_a_and_b_paths() -> None:
    expected = {
        "connect": ("self", "runtime_id"),
        "sync_orders": ("self", "runtime_id"),
        "sync_trades": ("self", "runtime_id"),
        "sync_positions": ("self", "runtime_id"),
        "submit_child_order": ("self", "order"),
        "cancel_child_order": ("self", "order", "reason"),
    }
    for method_name, parameter_names in expected.items():
        signature = inspect.signature(getattr(MiniQMTGateway, method_name))
        assert tuple(signature.parameters) == parameter_names

    fake = FakeMiniQMTGateway()
    real_adapter = QmtClientMiniQMTGateway(qmt_client=object())
    for gateway in (fake, real_adapter):
        for method_name in expected:
            assert callable(getattr(gateway, method_name))


def test_qmt_strategy_ledger_repository_satisfies_event_loop_oms_contract() -> None:
    repository = InMemoryQmtStrategyLedgerRepository()

    assert isinstance(repository, MiniQMTStrategyLedgerOmsContract)
