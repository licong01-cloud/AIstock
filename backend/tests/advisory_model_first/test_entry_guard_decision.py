from __future__ import annotations

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.entry_guard_decision import (
    AdvisoryEntryGuardDecisionV1,
    EntryGuardAction,
    EntryGuardMarketObservationV1,
    EntryGuardMode,
    EntryGuardSlotState,
    build_entry_guard_policy,
    build_entry_guard_signal,
    evaluate_entry_guard,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def _signal(**overrides):
    values = {
        "decision_date": date(2026, 1, 2),
        "target_trade_date": date(2026, 1, 5),
        "instrument": "000001.sz",
        "selection_rank": 1,
        "reference_price": 10.0,
        "entry_gap_q10": -0.01,
        "entry_gap_q50": 0.0,
        "entry_gap_q90": 0.02,
        "max_acceptable_gap_bps": 250.0,
        "max_buy_price": 10.25,
        "source_binding_sha256": "a" * 64,
        "feature_schema_sha256": "b" * 64,
        "information_cutoff": datetime(2026, 1, 2, 15, 0),
    }
    values.update(overrides)
    return build_entry_guard_signal(**values)


def _observation(**overrides):
    values = {
        "target_trade_date": date(2026, 1, 5),
        "instrument": "000001.SZ",
        "observed_at": datetime(2026, 1, 5, 9, 31),
        "open_price": 10.2,
        "current_price": None,
        "limit_up_price": 11.0,
        "limit_down_price": 9.0,
        "suspended": False,
    }
    values.update(overrides)
    return EntryGuardMarketObservationV1(**values)


def test_fixed_and_dynamic_entry_guard_actions_are_deterministic() -> None:
    fixed3 = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.FIXED_GAP_3),
        signal=_signal(),
        observation=_observation(open_price=10.2),
    )
    fixed5 = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.FIXED_GAP_5),
        signal=_signal(),
        observation=_observation(open_price=10.2),
    )
    dynamic = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.FROZEN_DYNAMIC),
        signal=_signal(max_acceptable_gap_bps=100.0, max_buy_price=10.08),
        observation=_observation(open_price=10.15),
    )

    assert fixed3.action == EntryGuardAction.REDUCE
    assert fixed3.slot_state == EntryGuardSlotState.FILLED_ADVISORY_CAUTION
    assert fixed3.advisory_only is True
    assert fixed5.action == EntryGuardAction.ACCEPT
    assert dynamic.action == EntryGuardAction.SKIP
    assert dynamic.slot_state == EntryGuardSlotState.CASH_EMPTY
    assert dynamic.silent_replacement is False
    assert dynamic.dynamic_position_authorized is False


def test_entry_guard_waits_for_normal_missing_or_suspended_data() -> None:
    policy = build_entry_guard_policy(EntryGuardMode.FIXED_GAP_3)
    missing = evaluate_entry_guard(
        policy=policy,
        signal=_signal(),
        observation=_observation(open_price=None, current_price=None),
    )
    suspended = evaluate_entry_guard(
        policy=policy,
        signal=_signal(),
        observation=_observation(open_price=None, current_price=None, suspended=True),
    )
    assert missing.action == EntryGuardAction.WAITING
    assert missing.reason_code == "WAITING_OPEN_OR_CURRENT_PRICE"
    assert suspended.action == EntryGuardAction.WAITING
    assert suspended.reason_code == "WAITING_SUSPENDED"


def test_entry_guard_skips_near_limit_up_without_replacement() -> None:
    result = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.FIXED_GAP_5),
        signal=_signal(),
        observation=_observation(open_price=10.95, limit_up_price=11.0),
    )
    assert result.action == EntryGuardAction.SKIP
    assert result.reason_code == "SKIP_NEAR_LIMIT_UP"
    assert result.slot_state == EntryGuardSlotState.CASH_EMPTY


def test_entry_guard_market_schema_rejects_future_and_position_fields() -> None:
    with pytest.raises(ValidationError):
        EntryGuardMarketObservationV1(
            **_observation().model_dump(mode="python"),
            close_price=10.5,
            future_return=0.1,
        )
    decision = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.NO_GUARD),
        signal=_signal(),
        observation=_observation(),
    )
    with pytest.raises(ValidationError):
        AdvisoryEntryGuardDecisionV1.model_validate({**decision.model_dump(mode="python"), "target_weight": 0.2})


def test_entry_guard_rejects_clock_or_instrument_mismatch() -> None:
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        evaluate_entry_guard(
            policy=build_entry_guard_policy(EntryGuardMode.NO_GUARD),
            signal=_signal(),
            observation=_observation(instrument="000002.SZ"),
        )
    assert excinfo.value.reason_code == "ADVISORY_ENTRY_GUARD_CLOCK_MISMATCH"


def test_dynamic_entry_guard_accepts_a_frozen_zero_gap_ceiling() -> None:
    result = evaluate_entry_guard(
        policy=build_entry_guard_policy(EntryGuardMode.FROZEN_DYNAMIC),
        signal=_signal(max_acceptable_gap_bps=0.0, max_buy_price=10.0),
        observation=_observation(open_price=10.01),
    )

    assert result.action == EntryGuardAction.SKIP
    assert result.reason_code == "SKIP_OPEN_GAP_EXCEEDED"
    assert result.applied_max_gap_bps == 0.0
