from __future__ import annotations

import pytest

from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.execution_algos.vnpy_style import create_vnpy_style_core
from backend.execution_algos.vnpy_style.models import VnpyAction
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2ControllerFactory,
    ParentQuoteControlAssignmentV1,
    _action_payload,
    assert_b0_quote_v2_parity,
    project_vnpy_tick,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import ActionQuoteEvaluator, ActionQuoteRequest

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import (
    TRADE_DATE,
    _context,
    _observation,
    _revision,
    _runtime_controller,
    _sha,
)
from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_lifecycle import _LifecycleSupervisor


def _ticks(side: str):  # type: ignore[no-untyped-def]
    context = _context()
    observation = _observation(context)
    revision = _revision(context)
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-p1e",
        binding_hash=_sha("e"),
        trade_date=TRADE_DATE,
        parent_intent_id="parent-p1e",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    eligibility = (
        ActionQuoteEvaluator()
        .evaluate(
            request=ActionQuoteRequest(
                runtime_id="runtime-p1e",
                parent_intent_id="parent-p1e",
                algo_instance_id="algo-p1e",
                symbol="000001.SZ",
                side=side,
                control_revision=ControlRevision.B0_QUOTE_V2,
                policy_sha256=context.policy.policy_sha256,
                config_sha256=_sha("f"),
                adapter_sha256=revision.adapter_sha256,
            ),
            context=context,
            observation=observation,
        )
        .eligibility
    )
    v2_tick = project_vnpy_tick(observation=observation, eligibility=eligibility, assignment=assignment)
    legacy_tick = type(v2_tick)(
        symbol=v2_tick.symbol,
        datetime=v2_tick.datetime,
        bid_price_1=v2_tick.bid_price_1,
        bid_volume_1=v2_tick.bid_volume_1,
        ask_price_1=v2_tick.ask_price_1,
        ask_volume_1=v2_tick.ask_volume_1,
        raw={"legacy_source": True},
    )
    return legacy_tick, v2_tick


def _core_actions(algo_code: str, side: str, tick, *, timer_count: int = 0) -> list[VnpyAction]:  # type: ignore[no-untyped-def]
    config = {
        "BEST_LIMIT_MINIQMT": {"min_volume": 100, "max_volume": 100},
        "TWAP_LITE_MINIQMT": {"time": 120, "interval": 60},
    }.get(algo_code, {})
    price = 10.02 if side == "BUY" else 9.98
    core = create_vnpy_style_core(
        algo_code=algo_code,
        symbol="000001.SZ",
        side=side,
        price=price,
        volume=200,
        algo_config=config,
        algo_name="parity-core",
        random_volume_provider=(lambda _minimum, _maximum: 100),
    )
    core.start()
    actions = core.update_tick(tick)
    for _ in range(timer_count):
        actions.extend(core.update_timer())
    return [action for action in actions if action.action_type.value != "LOG"]


def _assert_actions_match(legacy: list[VnpyAction], v2: list[VnpyAction]) -> None:
    assert len(legacy) == len(v2)
    for legacy_action, v2_action in zip(legacy, v2, strict=True):
        assert_b0_quote_v2_parity(
            legacy_payload=_action_payload(legacy_action),
            v2_payload=_action_payload(v2_action),
        )


def test_sniper_fresh_valid_buy_sell_business_fields_match_legacy_b0() -> None:
    for side in ("BUY", "SELL"):
        legacy_tick, v2_tick = _ticks(side)
        _assert_actions_match(
            _core_actions("SNIPER_MINIQMT", side, legacy_tick),
            _core_actions("SNIPER_MINIQMT", side, v2_tick),
        )


def test_best_limit_fresh_valid_buy_sell_business_fields_match_legacy_b0() -> None:
    for side in ("BUY", "SELL"):
        legacy_tick, v2_tick = _ticks(side)
        _assert_actions_match(
            _core_actions("BEST_LIMIT_MINIQMT", side, legacy_tick),
            _core_actions("BEST_LIMIT_MINIQMT", side, v2_tick),
        )


def test_twap_lite_fresh_valid_buy_sell_tail_and_protection_fields_match_legacy_b0() -> None:
    for side in ("BUY", "SELL"):
        legacy_tick, v2_tick = _ticks(side)
        legacy = _core_actions("TWAP_LITE_MINIQMT", side, legacy_tick, timer_count=60)
        v2 = _core_actions("TWAP_LITE_MINIQMT", side, v2_tick, timer_count=60)
        _assert_actions_match(legacy, v2)
        assert_b0_quote_v2_parity(
            legacy_payload={
                "actions": [_action_payload(item) for item in legacy],
                "tail_sweep": True,
                "protection_band": "BUG-614",
            },
            v2_payload={
                "actions": [_action_payload(item) for item in v2],
                "tail_sweep": True,
                "protection_band": "BUG-614",
            },
        )


def test_only_registered_quote_safety_differences_are_allowed() -> None:
    assert_b0_quote_v2_parity(
        legacy_payload={"price": 10.01, "action_id": "legacy", "vt_orderid": "legacy-order"},
        v2_payload={"price": 10.01, "action_id": "v2", "vt_orderid": "v2-order"},
    )
    with pytest.raises(QuoteContractError) as exc_info:
        assert_b0_quote_v2_parity(
            legacy_payload={"price": 10.01, "business_guard": "legacy"},
            v2_payload={"price": 10.01, "business_guard": "changed"},
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.PARITY_VIOLATION


def test_parity_violation_invalidates_revision_without_legacy_fallback() -> None:
    source, runtime, _gateway, _repository = _runtime_controller()
    factory = B0QuoteV2ControllerFactory(
        supervisor=_LifecycleSupervisor(source),
        config=source.config,
        data_session_key="parity-session-p1e",
    )
    revision = next(iter(source.assignments.values())).revision
    assert revision is not None

    with pytest.raises(QuoteContractError):
        factory.validate_revision_parity(
            revision_id=revision.revision_id,
            legacy_payload={"price": 10.01},
            v2_payload={"price": 10.02},
        )
    with pytest.raises(QuoteContractError) as exc_info:
        factory.create(runtime=runtime, assignments=source.assignments, symbols=tuple(source.symbols))

    assert exc_info.value.reason_code == QuoteContractReasonCode.PARITY_VIOLATION
    assert factory.health()["invalid_revision_ids"] == [revision.revision_id]
    assert factory.health()["controller_count"] == 0
