"""Archived scheduler scenarios that exercise MiniQMT execution."""


from __future__ import annotations

from datetime import timedelta

import pytest

from backend.services.simulation_data.daily_context import SimulationBrokerBackend
from backend.services.simulation_runtime import SimulationDailyRunStatus, StaticSimulationRunContextProvider
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.trading_core.errors import BrokerSubmitError
from backend.tests.simulation_runtime.test_lifecycle_scheduler import (
    TRADE_DATE,
    FakeSelectionService,
    SimulationLifecycleScheduler,
    _candidate_rows,
    _create_extra_binding,
    _position_context,
    _release_and_bindings,
)


def test_scheduler_plans_active_local_and_miniqmt_bindings_from_same_selection_evidence() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert result.failed_count == 0
    plans = [item.execution_plan for item in result.results]
    assert {plan.selection_evidence_hash for plan in plans if plan is not None} == {plans[0].selection_evidence_hash}
    normalized_intents = [
        [(intent.symbol, intent.side.value, intent.order_quantity, intent.rebalance_reason) for intent in plan.intents]
        for plan in plans
        if plan is not None
    ]
    assert normalized_intents[0] == normalized_intents[1]
    assert ("000003.SZ", "SELL", 77, "DROPPED_FROM_SELECTION") in normalized_intents[0]


def test_scheduler_miniqmt_durable_replay_failure_does_not_starve_later_localsim_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt_replay_failure"),
                local_binding.binding_id: _position_context(portfolio_id="portfolio_local_after_qmt_failure"),
            }
        ),
    )
    original_run_binding = scheduler._run_binding_with_watchdog

    def replay_failure_then_continue(*, binding, **kwargs):
        if binding.binding_id == qmt_binding.binding_id:
            raise BrokerSubmitError(
                "durable result identity conflict",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY",
                    "qmt_batch_id": "qmtbatch_scheduler_isolation",
                    "binding_id": binding.binding_id,
                },
            )
        return original_run_binding(binding=binding, **kwargs)

    monkeypatch.setattr(scheduler, "_run_binding_with_watchdog", replay_failure_then_continue)

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)
    by_binding_id = {item.binding_id: item for item in result.results}
    qmt_failure = by_binding_id[qmt_binding.binding_id]
    local_result = by_binding_id[local_binding.binding_id]
    qmt_run = repo.get_simulation_daily_run_by_key(
        strategy_id=qmt_binding.strategy_id,
        binding_id=qmt_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    local_run = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert qmt_failure.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert qmt_failure.error["context"]["reason_code"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT"
    assert (
        qmt_failure.error["context"]["pre_run_failure"]["context"]["stage"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY"
    )
    assert local_result.status == "PLANNED"
    assert qmt_run is not None
    assert qmt_run.run_payload_json["pre_run_failure"]["context"]["stage"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY"
    assert local_run is not None
    assert local_run.execution_plan_id is not None


def test_scheduler_isolates_invalid_legacy_miniqmt_roll_forward_without_starving_valid_bindings() -> None:
    release, local_binding, invalid_qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    valid_qmt_binding = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_valid_qmt_after_invalid_roll_forward",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="ValidAfterInvalidRollForward",
        order_remark_prefix="valid-after-invalid-roll-forward",
    )
    invalid_rebase_binding = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_invalid_qmt_manifest_rebase",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="InvalidManifestRebase",
        order_remark_prefix="invalid-manifest-rebase",
    )
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    invalid_config = dict(invalid_qmt_binding.binding_config_json)
    invalid_config.pop("miniqmt_quote_control")
    invalid_qmt_binding = invalid_qmt_binding.model_copy(
        update={
            "binding_config_json": invalid_config,
            "binding_hash": canonical_json_sha256(invalid_config),
            "effective_from": prepared_day,
            "effective_to": prepared_day,
        }
    )
    repo.bindings[invalid_qmt_binding.binding_id] = invalid_qmt_binding
    invalid_rebase_config = dict(invalid_rebase_binding.binding_config_json)
    invalid_rebase_config.pop("miniqmt_quote_control")
    invalid_rebase_config["metadata"] = {
        **dict(invalid_rebase_config.get("metadata") or {}),
        "purpose": "miniqmt_unattended_daily_roll_forward",
        "extends_binding_id": "simbind_previous_day_source",
        "manifest_identity_source": "strategy_package_current_manifest",
    }
    invalid_rebase_binding = invalid_rebase_binding.model_copy(
        update={
            "binding_config_json": invalid_rebase_config,
            "binding_hash": canonical_json_sha256(invalid_rebase_config),
            "effective_from": next_trade_day,
            "effective_to": next_trade_day,
        }
    )
    repo.bindings[invalid_rebase_binding.binding_id] = invalid_rebase_binding
    release_count_before = len(repo.releases)
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    selection.package_repository.manifest_sha256 = "manifest_changed_before_invalid_rebase"
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding.strategy_id: _position_context(portfolio_id="portfolio_valid_local"),
                valid_qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_valid_qmt"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=False,
    )

    by_strategy_id = {item.strategy_id: item for item in result.results}
    invalid_result = by_strategy_id[invalid_qmt_binding.strategy_id]
    invalid_rebase_result = by_strategy_id[invalid_rebase_binding.strategy_id]
    invalid_run = repo.get_simulation_daily_run_by_key(
        strategy_id=invalid_qmt_binding.strategy_id,
        binding_id=invalid_qmt_binding.binding_id,
        trade_date=next_trade_day,
    )
    assert result.total_bindings == 4
    assert result.failed_count == 2
    assert result.planned_count == 2
    assert invalid_result.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert invalid_result.error["context"]["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"
    assert invalid_result.error["context"]["legacy_fallback"] is False
    assert invalid_result.execution_result is None
    assert invalid_run is not None
    assert invalid_run.status is SimulationDailyRunStatus.FAILED_RETRYABLE
    assert invalid_rebase_result.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert invalid_rebase_result.error["context"]["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"
    assert invalid_rebase_result.error["context"]["legacy_fallback"] is False
    assert by_strategy_id[local_binding.strategy_id].status == "PLANNED"
    assert by_strategy_id[valid_qmt_binding.strategy_id].status == "PLANNED"
    assert len(repo.releases) == release_count_before
    assert repo.list_simulation_release_bindings(
        strategy_id=invalid_qmt_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        limit=10,
    ) == [invalid_qmt_binding]
    assert repo.list_simulation_release_bindings(
        strategy_id=invalid_rebase_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        limit=10,
    ) == [invalid_rebase_binding]


def test_scheduler_rolls_forward_local_and_miniqmt_when_backend_filter_is_omitted() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding.strategy_id: _position_context(portfolio_id="portfolio_local_roll_all"),
                qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_all"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=False,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert {item.broker_backend for item in result.results} == {
        SimulationBrokerBackend.LOCAL_SIM,
        SimulationBrokerBackend.MINIQMT_SIM,
    }
    rolled_bindings = [repo.get_simulation_release_binding(item.run.binding_id) for item in result.results]
    assert {binding.binding_config_json["metadata"]["purpose"] for binding in rolled_bindings} == {
        "localsim_unattended_daily_roll_forward",
        "miniqmt_unattended_daily_roll_forward",
    }


def test_scheduler_rebuilds_b0_manifest_conflict_once_and_keeps_repeated_runtime_drift_loud() -> None:
    conflicts = {
        "code_sha256": {
            "expected": "53b579e4f5a273dd340354de5876baa8e15070fe52a70f90f7648e3d3dae6996",
            "received": "0f4702d27b3bd873b39fd8d2473f6a9cbd9a98965fe10d7fbf7534fa326b3100",
        }
    }
    payload = {
        "broker_called": False,
        "submitted_intents": 0,
        "failed_intents": 36,
        "submit_failure": {
            "type": "BrokerSubmitError",
            "stage": "ADAPTER",
            "message": "B0_QUOTE_V2 frozen build/schema manifest differs from runtime readback",
            "context": {
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "manifest_conflicts": conflicts,
                "broker_called": False,
            },
        },
    }

    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is True
    fingerprint = canonical_json_sha256(
        {
            "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
            "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
            "manifest_conflicts": conflicts,
        }
    )
    payload["miniqmt_side_effect_free_rebuild"] = {
        "schema_version": "miniqmt_side_effect_free_plan_rebuild_v1",
        "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
        "failure_fingerprint": fingerprint,
        "broker_called": False,
    }

    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is False

    payload["submit_failure"]["context"]["manifest_conflicts"] = {
        "code_sha256": {
            "expected": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "received": conflicts["code_sha256"]["received"],
        }
    }
    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is True

    payload["broker_called"] = True
    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is False
