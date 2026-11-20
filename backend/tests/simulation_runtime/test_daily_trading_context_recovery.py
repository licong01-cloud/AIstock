"""LocalSIM plan carrier and recovery contracts."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from backend.services.simulation_data.daily_context import SimulationBrokerBackend
from backend.services.simulation_runtime.decision import ExecutionPlanCompiler
from backend.services.simulation_runtime.localsim_daily_limit_authority import LocalSimDailyLimitAuthorityProvider
from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.tests.simulation_runtime.test_localsim_limit_authority import (
    SYMBOLS,
    TRADE_DATE,
    _attempt,
    _load,
    _row,
)


def test_execution_plan_and_recovery_read_back_exact_v2_context() -> None:
    context = _load(
        _attempt([_row(symbol, 10.0, 11.0, 9.0) for symbol in SYMBOLS]),
        lambda symbols: pytest.fail(f"unexpected TDX lookup: {symbols}"),
    )
    statuses = LocalSimDailyLimitAuthorityProvider.to_pre_trade_statuses(context)
    decisions = [
        SimpleNamespace(
            decision_id=f"decision_{symbol}",
            symbol=symbol,
            price_limit_rule={"pre_trade_tradability": status},
        )
        for symbol, status in statuses.items()
    ]
    binding = SimpleNamespace(
        binding_id="binding_localsim",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    carrier = ExecutionPlanCompiler._daily_trading_context_carrier(
        binding=binding,
        target_trade_date=TRADE_DATE,
        trading_rule_decisions=decisions,
    )
    assert carrier == context.carrier_payload()
    plan = SimpleNamespace(
        plan_id="plan_localsim",
        trading_rule_decisions=decisions,
        plan_payload_json={"daily_trading_context": carrier},
    )
    recovered = ProductionSimulationRunContextProvider._frozen_pre_trade_tradability(plan)
    assert set(recovered) == set(SYMBOLS)

    corrupted = deepcopy(statuses[SYMBOLS[0]])
    corrupted["daily_trading_context"]["source_evidence_hash"] = "f" * 64
    bad_decisions = [
        SimpleNamespace(
            decision_id="decision_bad",
            symbol=SYMBOLS[0],
            price_limit_rule={"pre_trade_tradability": corrupted},
        )
    ]
    with pytest.raises(RuntimeConfigInvalidError):
        ExecutionPlanCompiler._daily_trading_context_carrier(
            binding=binding,
            target_trade_date=TRADE_DATE,
            trading_rule_decisions=bad_decisions,
        )
