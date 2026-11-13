"""Native TWAP remains a real algorithm, never a renamed timing baseline."""

from decimal import Decimal
import pytest

from backend.services.trading_core.models import OrderIntent
from backend.services.simulation_execution.localsim.native_research import (
    native_backend_for, run_native_portfolio_day, run_cash_only_portfolio_day,
)
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.simulation_execution.test_localsim_human_operation import full_markets
from backend.tests.simulation_execution.test_localsim_timing_campaign import cost


def test_native_twap_uses_existing_engine_without_calling_timing_core(monkeypatch):
    from backend.services.simulation_execution.localsim import timing

    def forbidden(*args, **kwargs):
        raise AssertionError("Native TWAP must not call the timing rule")

    monkeypatch.setattr(timing, "evaluate", forbidden)
    plans, markets = full_markets()
    manifest = make_paper_enabled_manifest()
    account = {"portfolio_id": "native-account", "package_id": manifest.package_id,
               "origin_initial_cash_cny": "10000000", "cash_cny": "10000000", "positions": {}}
    backend = native_backend_for(account=account, markets=markets, cost=cost(), manifest=manifest,
                                  source_identity="a" * 64)
    intent = OrderIntent(intent_id="native-intent", package_id=manifest.package_id,
                         portfolio_id=account["portfolio_id"], symbol=plans[0].symbol, side="BUY",
                         quantity=1000, target_trade_date=plans[0].target_trade_date)
    result = run_native_portfolio_day(backend=backend, markets=markets, intents=[intent],
        initial_marks={s: Decimal("10") for s in markets}, run_id="native-functional", source_commit="1"*40)
    assert sum(f.quantity for f in backend._ledger.fills) == 1000
    assert sum(e.fee for e in backend._ledger.cash_entries) == Decimal("5")
    assert backend._ledger.cash_decimal == Decimal("9989995")
    assert Decimal(result["account"]["terminal_nav_cny"]) == Decimal("9999995")
    session = result["sessions"][0]
    assert session.execution_state.algo_code == "TWAP"
    assert session.snapshot()["execution_policy"]["policy_json"]["algo_code"] == "TWAP"
    assert session.facade.decision is None
    assert len(backend._ledger.fills) == 3


@pytest.mark.parametrize("human", [False,True])
def test_authoritative_cash_only_day_has_native_valuation_without_inventing_security(human):
    from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy
    from backend.tests.simulation_execution.test_localsim_position_timing import at
    manifest = make_paper_enabled_manifest()
    account = {"portfolio_id":"cash-only", "package_id":manifest.package_id,
               "origin_initial_cash_cny":"10000000", "cash_cny":"10000000", "positions":{}}
    with pytest.raises(ValueError, match="FROZEN_MARKET_INVALID"):
        native_backend_for(account=account, markets={}, cost=cost(), manifest=manifest, source_identity="a"*64)
    backend = native_backend_for(account=account, markets={}, cost=cost(), manifest=manifest,
        source_identity="a"*64, cash_only_trade_date=at().date())
    result = run_cash_only_portfolio_day(backend=backend, trade_date=at().date(),
        human_policy=HumanOperationPolicy(reaction_delay_seconds=60) if human else None)
    assert result["account"]["parents"] == [] and not backend._records
    assert result["account"]["positions"] == {} and not backend._ledger.fills
    assert Decimal(result["account"]["terminal_nav_cny"]) == 10000000
    assert result["account"]["max_drawdown"] == "0" and result["account"]["fees_cny"] == "0"
    if human:
        assert result["account"]["human_operation_state"]["actions"] == []
