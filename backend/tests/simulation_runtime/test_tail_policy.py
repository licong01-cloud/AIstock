from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from backend.services.paper_trading_v2.broker.base import CancelAck, OrderHandle, OrderHandleStatus
from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyRuntimeReleaseService,
    TailHandlingPolicyService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.models import PositionLot


TRADE_DATE = date(2026, 5, 21)


def _release_binding_repo():
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_tail_policy",
        manifest_sha256="manifest_tail_policy",
        runtime_profile_id="runtime_tail",
        runtime_profile_version_id="runtime_tail_v1",
        runtime_profile_sha256="runtime_tail_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="tail policy test",
    )
    binding = service.create_binding(
        strategy_id="strategy_tail_local",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="tail policy test",
    )
    return release, binding, repo


def _candidates() -> list[SelectionCandidate]:
    return [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_quantity=1000,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
    ]


def _evidence(release) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": TRADE_DATE.isoformat(),
        "cutoff_date": "2026-05-20",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile_version_id": release.runtime_profile_version_id,
        "runtime_profile_hash": release.runtime_profile_sha256,
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "selected_candidates": [item.model_dump(mode="json") for item in _candidates()],
        "excluded_candidates": [],
        "valid_no_candidate": False,
        "no_candidate_reason": None,
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=TRADE_DATE,
        cutoff_date=date(2026, 5, 20),
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=1,
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


class FakeSelectionService:
    def __init__(self, release) -> None:
        self.release = release

    def run_selection(self, **kwargs):
        candidates = _candidates()
        return StrategyPackageSelectionResult(
            runtime_config={"runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}},
            package_results={self.release.package_id: candidates},
            aggregate_results=candidates,
            excluded_results={self.release.package_id: []},
            manifest_sha256_by_package={self.release.package_id: self.release.manifest_sha256},
            evidence_by_package={self.release.package_id: _evidence(self.release)},
            valid_no_candidate=False,
            no_candidate_reason=None,
        )


class TailAwareLocalBroker:
    backend_id = "local_sim"
    backend_version = "unit-test"

    def __init__(self, states_by_intent: dict[str, str | tuple[str, int]]) -> None:
        self.states_by_intent = states_by_intent
        self.handles: list[OrderHandle] = []
        self.cancelled: list[str] = []

    def submit_order_intent(self, intent):
        handle = OrderHandle(
            handle_id=f"handle_{len(self.handles) + 1}",
            backend_id="local_sim",
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )
        self.handles.append(handle)
        return handle

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        raw_state = self.states_by_intent.get(handle.intent_id, "pending")
        state, filled = raw_state if isinstance(raw_state, tuple) else (raw_state, 0)
        return OrderHandleStatus(
            handle_id=handle.handle_id,
            state=state,
            filled_quantity=int(filled),
            avg_fill_price=Decimal("10") if filled else None,
            last_event_at=datetime.now(UTC),
            rejection_reason=None,
        )

    def cancel(self, handle: OrderHandle) -> CancelAck:
        self.cancelled.append(handle.intent_id)
        return CancelAck(handle_id=handle.handle_id, accepted=True, reason="tail_cancel_unfilled_at_close")


def test_tail_policy_cancels_no_fill_and_partial_unfilled_orders_after_localsim_submit() -> None:
    release, binding, repo = _release_binding_repo()
    scheduler = SimulationLifecycleScheduler(repository=repo, selection_service=FakeSelectionService(release))
    initial_context = SimulationRunContext(
        portfolio_id="portfolio_tail",
        current_positions={
            "000002.SZ": PositionLot(
                portfolio_id="portfolio_tail",
                symbol="000002.SZ",
                quantity=100,
                available_quantity=100,
                avg_cost=8.0,
                trade_date=date(2026, 5, 20),
            )
        },
        current_prices={"000002.SZ": 8.0},
        tail_policy_payload={"policy": "cancel_unfilled_at_close"},
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: initial_context})
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    plan = planned.results[0].execution_plan
    states = {
        intent.intent_id: ("partial_filled", 100) if intent.side.value == "BUY" else "pending"
        for intent in plan.intents
    }
    broker = TailAwareLocalBroker(states)
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={
            binding.binding_id: SimulationRunContext(
                portfolio_id="portfolio_tail",
                current_positions=initial_context.current_positions,
                current_prices=initial_context.current_prices,
                local_broker=broker,  # type: ignore[arg-type]
                tail_policy_payload={"policy": "cancel_unfilled_at_close"},
                tail_policy_service=TailHandlingPolicyService(),
            )
        }
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert submitted.results[0].status == "TAIL_HANDLED"
    tail_payload = submitted.results[0].run.run_payload_json["tail_handling"]
    assert tail_payload["policy"] == "cancel_unfilled_at_close"
    assert tail_payload["partial_cancelled_count"] == 1
    assert tail_payload["no_fill_cancelled_count"] == 1
    assert sorted(broker.cancelled) == sorted(intent.intent_id for intent in plan.intents)


def test_tail_policy_fails_fast_when_policy_payload_is_missing() -> None:
    release, binding, repo = _release_binding_repo()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_tail",
                    current_positions={},
                    local_broker=TailAwareLocalBroker({}),  # type: ignore[arg-type]
                    tail_policy_service=TailHandlingPolicyService(),
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert result.failed_count == 1
    assert result.results[0].error["type"] == "StrategyPackageValidationError"
    assert "TailHandlingPolicy execution requires explicit policy payload" in result.results[0].error["message"]
