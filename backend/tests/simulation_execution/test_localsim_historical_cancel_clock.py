"""Historical cancellation is a confirmed native event, not wall-clock audit."""

from decimal import Decimal

import pytest

from backend.services.simulation_execution.localsim.economic import local_sim_fact_payload
from backend.tests.simulation_execution.test_localsim_position_timing import at, execution


def test_historical_cancel_events_are_exact_reproducible_and_retry_is_noop():
    _, first = execution()
    _, second = execution()
    for session in (first, second):
        session.cancel("COMMON_TERMINAL_1457", confirmed_at=at("14:57:00"))
        session.cancel("COMMON_TERMINAL_1457", confirmed_at=at("14:57:00"))
        assert len(session.events) == 1
        assert session.events[0].event_time == at("14:57:00")
        assert session.order.filled_quantity == session.state.executed_quantity == 0
        assert session.execution_state.is_terminal
    assert local_sim_fact_payload(first.events[0], fact_type="order_event") == local_sim_fact_payload(
        second.events[0], fact_type="order_event")
    _, later = execution()
    later.cancel("COMMON_TERMINAL_1457", confirmed_at=at("14:56:00"))
    assert later.events[0].event_id != first.events[0].event_id


def test_partial_fill_cancellation_preserves_native_cash_fee_and_quantity():
    backend, session = execution()
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    session.step(session.market.bars[2].model_copy(update={"volume_shares": Decimal(300)}))
    before = backend._ledger.cash_decimal
    session.cancel("HUMAN_CANCEL_CONFIRMED", confirmed_at=at("09:33:10"))
    assert backend._ledger.cash_decimal == before and session.order.filled_quantity == 300
    assert session.state.executed_quantity == 300 and len(backend._ledger.fills) == 1
    assert session.events[-1].event_time == at("09:33:10")
    assert backend._ledger.positions[session.order.symbol].available_quantity == 0


def test_invalid_historical_cancel_clock_fails_before_mutation():
    _, session = execution()
    before = session.snapshot()
    with pytest.raises(ValueError, match="LOCALSIM_HISTORICAL_CANCEL_CLOCK_INVALID"):
        session.cancel("TEST", confirmed_at=at("09:29:59"))
    assert session.snapshot() == before


def test_legacy_default_retains_native_oms_cancellation_event(monkeypatch):
    backend, session = execution()
    original = backend._oms.cancel_order
    produced = []

    def recorded(order, reason):
        result = original(order, reason)
        produced.append(result)
        return result

    monkeypatch.setattr(backend._oms, "cancel_order", recorded)
    session.cancel("LEGACY")
    assert session.order == produced[0][0] and session.events == [produced[0][1]]
