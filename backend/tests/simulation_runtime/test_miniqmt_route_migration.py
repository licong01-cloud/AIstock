from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import psycopg2
import pytest

from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.services.simulation_runtime.models import (
    DailySelectionEvidence,
    ExecutionPathNotCanonicalError,
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    canonical_json_sha256,
)
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import QuoteControlBindingV1
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTChildOrder,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.repository import (
    InMemoryMiniQMTExecutionRuntimeRepository,
)
from backend.services.simulation_runtime.decision import ExecutionPlanCompiler
from backend.services.simulation_runtime.miniqmt_route_migration import (
    MiniQMTRouteMigrationError,
    MiniQMTRouteMigrationInventoryV1,
    MiniQMTRouteMigrationMarkerV1,
    MiniQMTRouteMigrationService,
    build_inventory,
)
from backend.services.simulation_runtime.repository import (
    InMemorySimulationRuntimeRepository,
    SimulationRuntimeRepository,
)
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide


EFFECTIVE_DATE = date(2026, 7, 16)
OBSERVED_AT = datetime(2026, 7, 15, 8, 0, tzinfo=UTC)
B0_CONTROL = {
    "schema_version": "miniqmt_quote_control_binding_v1",
    "control_revision": "B0_QUOTE_V2",
}


def _quote_policy() -> dict[str, Any]:
    return {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {},
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "CALENDAR",
                "DEPTH_UNIT_SHARES",
                "EXCHANGE_TIMESTAMP",
                "FIVE_LEVEL_DEPTH",
                "RAW_PRICE_BASIS",
                "TRADABILITY",
            ],
            "max_receive_age_ms": 20_000,
            "max_source_lag_ms": 20_000,
            "max_exchange_age_ms": 20_000,
            "max_negative_skew_ms": 1_000,
            "max_clock_age_divergence_ms": 1_000,
            "max_dependency_group_skew_ms": 20_000,
            "auction_mode": "OBSERVE_ONLY",
        },
    }


def _source_and_target(
    repository: InMemorySimulationRuntimeRepository,
) -> tuple[Any, SimulationReleaseBinding, Any]:
    service = StrategyRuntimeReleaseService(repository=repository)
    common = {
        "package_id": "pkg-route-migration",
        "manifest_sha256": "manifest-route-migration",
        "runtime_profile_id": "runtime-profile",
        "runtime_profile_version_id": "runtime-profile-v1",
        "runtime_profile_sha256": "runtime-profile-sha",
        "daily_strategy_profile_version_id": "daily-v1",
        "tail_policy_version_id": "tail-v1",
        "tail_policy_sha256": "tail-sha",
        "validation_state": RuntimeReleaseValidationState.SIM_PASSED,
    }
    source_release = service.create_release(
        **common,
        execution_policy_version_id="legacy-execution-v1",
        execution_policy_sha256="legacy-execution-sha",
        execution_policy_json={"algo_code": "SNIPER_MINIQMT", "algo_config": {}},
        effective_from=date(2026, 7, 14),
    )
    source_config = {
        "schema_version": "simulation_release_binding_v1",
        "strategy_id": "strategy-route-migration",
        "release_id": source_release.release_id,
        "release_hash": source_release.release_hash,
        "package_id": source_release.package_id,
        "manifest_sha256": source_release.manifest_sha256,
        "broker_backend": "minqmt_sim",
        "broker_account_id": "sim-account",
        "capital_allocation": 100_000.0,
        "strategy_name": "strategy-route-migration",
        "order_remark_prefix": "route-migration",
        "approval_state": "SIM_PASSED",
        "metadata": {"source": "unit-test"},
        "account_group_id": "sim-account",
        "strategy_slot_id": "slot-route-migration",
    }
    source_hash = canonical_json_sha256(source_config)
    source_binding = repository.save_simulation_release_binding(
        SimulationReleaseBinding(
            binding_id=f"simbind_{source_hash[:16]}",
            strategy_id="strategy-route-migration",
            release_id=source_release.release_id,
            release_hash=source_release.release_hash or "",
            package_id=source_release.package_id,
            manifest_sha256=source_release.manifest_sha256,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            broker_account_id="sim-account",
            account_group_id="sim-account",
            strategy_slot_id="slot-route-migration",
            capital_allocation=100_000.0,
            strategy_name="strategy-route-migration",
            order_remark_prefix="route-migration",
            effective_from=date(2026, 7, 14),
            approval_state=SimulationBindingApprovalState.SIM_PASSED,
            binding_config_json=source_config,
            binding_hash=source_hash,
        )
    )
    target_release = service.create_release(
        **common,
        execution_policy_version_id="b0-execution-v1",
        execution_policy_sha256="b0-execution-sha",
        execution_policy_json=_quote_policy(),
        effective_from=EFFECTIVE_DATE,
    )
    return source_release, source_binding, target_release


def _service(
    simulation_repository: InMemorySimulationRuntimeRepository,
    runtime_repository: InMemoryMiniQMTExecutionRuntimeRepository,
    *,
    broker_rows: list[dict[str, Any]] | None = None,
    broker_calls: list[str] | None = None,
) -> MiniQMTRouteMigrationService:
    def read_orders() -> list[dict[str, Any]]:
        if broker_calls is not None:
            broker_calls.append("read")
        return list(broker_rows or [])

    return MiniQMTRouteMigrationService(
        simulation_repository=simulation_repository,
        runtime_repository=runtime_repository,
        broker_open_order_reader=read_orders,
        clock=lambda: OBSERVED_AT,
    )


def _apply(service: MiniQMTRouteMigrationService, source: SimulationReleaseBinding, target: Any):
    return service.apply(
        source_binding_id=source.binding_id,
        target_release_id=target.release_id,
        effective_trade_date=EFFECTIVE_DATE,
        operator="unit-test",
    )


def test_new_miniqmt_binding_requires_exact_b0_while_historical_omitted_stays_readable() -> None:
    repository = InMemorySimulationRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    service = StrategyRuntimeReleaseService(repository=repository)

    assert QuoteControlBindingV1.from_binding_config(source.binding_config_json).control_revision is ControlRevision.LEGACY_B0
    with pytest.raises(RuntimeConfigInvalidError) as missing:
        service.create_binding(
            strategy_id=source.strategy_id,
            release=target,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            broker_account_id=source.broker_account_id,
            capital_allocation=source.capital_allocation,
        )
    assert missing.value.context["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"

    with pytest.raises(RuntimeConfigInvalidError) as legacy:
        service.create_binding(
            strategy_id=source.strategy_id,
            release=target,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            broker_account_id=source.broker_account_id,
            capital_allocation=source.capital_allocation,
            miniqmt_quote_control={
                "schema_version": "miniqmt_quote_control_binding_v1",
                "control_revision": "LEGACY_B0",
            },
        )
    assert legacy.value.context["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"


def test_legacy_binding_cannot_compile_new_parent() -> None:
    repository = InMemorySimulationRuntimeRepository()
    source_release, source, _target = _source_and_target(repository)
    payload = {"signals": []}
    digest = canonical_json_sha256(payload)
    evidence = DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=EFFECTIVE_DATE,
        package_id=source.package_id,
        manifest_sha256=source.manifest_sha256,
        release_id=source_release.release_id,
        release_hash=source_release.release_hash,
        runtime_profile_version_id=source_release.runtime_profile_version_id,
        runtime_profile_hash=source_release.runtime_profile_sha256,
        source_type="unit-test",
        data_source="DB_HISTORICAL",
        candidate_count=0,
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
    )

    with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
        ExecutionPlanCompiler().compile_plan(
            runtime_release=source_release,
            binding=source,
            selection_evidence=evidence,
            order_intents=[],
            trading_rule_decisions=[],
        )
    assert exc_info.value.context["reason_code"] == "MINIQMT_LEGACY_B0_NEW_PARENT_FORBIDDEN"
    assert exc_info.value.context["broker_called"] is False


def test_route_migration_is_atomic_rebuildable_and_idempotent() -> None:
    repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    broker_calls: list[str] = []
    service = _service(repository, runtime_repository, broker_calls=broker_calls)

    first = _apply(service, source, target)
    second = _apply(service, source, target)

    assert second == first
    assert broker_calls == ["read"]
    assert first.source_binding_id == source.binding_id
    assert first.target_release_id == target.release_id
    assert first.target_control_revision == "B0_QUOTE_V2"
    assert repository.get_simulation_release_binding(source.binding_id).effective_to == date(2026, 7, 15)
    target_binding = repository.get_simulation_release_binding(first.target_binding_id)
    assert target_binding.binding_hash == first.target_binding_hash
    assert QuoteControlBindingV1.from_binding_config(target_binding.binding_config_json).control_revision is ControlRevision.B0_QUOTE_V2


def test_active_parent_and_attributed_broker_order_each_block_with_zero_writes() -> None:
    repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id="runtime-active-legacy",
            account_group_id="sim-account",
            trade_date=date(2026, 7, 15),
            runtime_config_hash="runtime-config-hash",
        )
    )
    runtime_repository.upsert_algo_instance(
        MiniQMTExecutionAlgoInstance(
            algo_instance_id="algo-active-legacy",
            runtime_id="runtime-active-legacy",
            parent_intent_id="parent-active-legacy",
            strategy_slot_id="slot-route-migration",
            symbol="600000.SH",
            side=OrderSide.BUY,
            target_quantity=100,
            remaining_quantity=100,
            algo_code="SNIPER_MINIQMT",
            metadata={"runtime_child_context": {"binding_id": source.binding_id}},
        )
    )
    service = _service(repository, runtime_repository)

    with pytest.raises(MiniQMTRouteMigrationError) as active:
        _apply(service, source, target)
    assert active.value.context["reason_code"] == "MINIQMT_ROUTE_MIGRATION_ACTIVE_FACTS_PRESENT"
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None
    assert repository.find_miniqmt_route_migration_target(
        source_binding_id=source.binding_id, effective_trade_date=EFFECTIVE_DATE
    ) is None

    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    service = _service(
        repository,
        runtime_repository,
        broker_rows=[
            {
                "order_id": "broker-open-1",
                "strategy_name": source.strategy_name,
                "order_remark": f"{source.order_remark_prefix}-open",
            }
        ],
    )
    with pytest.raises(MiniQMTRouteMigrationError) as broker:
        _apply(service, source, target)
    assert broker.value.context["reason_code"] == "MINIQMT_ROUTE_MIGRATION_ACTIVE_FACTS_PRESENT"
    assert broker.value.context["blockers"]["broker_open_order_ids"] == ["broker-open-1"]
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None


def test_active_child_attribution_conflict_and_inventory_overflow_fail_before_write() -> None:
    repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id="runtime-active-child",
            account_group_id="sim-account",
            trade_date=date(2026, 7, 15),
            runtime_config_hash="runtime-config-hash",
        )
    )
    runtime_repository.upsert_child_order(
        MiniQMTChildOrder(
            child_order_id="child-active-legacy",
            runtime_id="runtime-active-child",
            algo_instance_id="algo-active-child",
            parent_intent_id="parent-active-child",
            strategy_slot_id="slot-route-migration",
            symbol="600000.SH",
            side=OrderSide.BUY,
            quantity=100,
            price=10.0,
            metadata={"managed_order_request": {"binding_id": source.binding_id}},
        )
    )
    with pytest.raises(MiniQMTRouteMigrationError) as active_child:
        _apply(_service(repository, runtime_repository), source, target)
    assert active_child.value.context["blockers"]["active_child_order_ids"] == [
        "child-active-legacy"
    ]
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None

    with pytest.raises(MiniQMTRouteMigrationError) as attribution:
        _apply(
            _service(
                repository,
                InMemoryMiniQMTExecutionRuntimeRepository(),
                broker_rows=[
                    {
                        "order_id": "broker-ambiguous-1",
                        "strategy_name": source.strategy_name,
                        "order_remark": "unrelated-prefix",
                    }
                ],
            ),
            source,
            target,
        )
    assert attribution.value.context["blockers"]["broker_attribution_conflicts"] == [
        "broker-ambiguous-1"
    ]
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None

    overflow_runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    for runtime_id in ("runtime-overflow-1", "runtime-overflow-2"):
        overflow_runtime_repository.upsert_runtime(
            MiniQMTExecutionRuntimeRecord(
                runtime_id=runtime_id,
                account_group_id="sim-account",
                trade_date=date(2026, 7, 15),
                runtime_config_hash=f"config-{runtime_id}",
            )
        )
    overflow_service = MiniQMTRouteMigrationService(
        simulation_repository=repository,
        runtime_repository=overflow_runtime_repository,
        broker_open_order_reader=lambda: [],
        clock=lambda: OBSERVED_AT,
        runtime_limit=1,
    )
    with pytest.raises(MiniQMTRouteMigrationError) as overflow:
        _apply(overflow_service, source, target)
    assert overflow.value.context["reason_code"] == "MINIQMT_ROUTE_MIGRATION_INVENTORY_LIMIT"
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None


def test_inmemory_transaction_rolls_back_target_when_insert_path_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    plan = _service(repository, runtime_repository).plan(
        source_binding_id=source.binding_id,
        target_release_id=target.release_id,
        effective_trade_date=EFFECTIVE_DATE,
        operator="unit-test",
    )
    real_save = repository.save_simulation_release_binding

    def fail_after_insert(binding: SimulationReleaseBinding) -> SimulationReleaseBinding:
        saved = real_save(binding)
        if binding.binding_id == plan.target_binding.binding_id:
            raise RuntimeError("injected failure after target insert")
        return saved

    monkeypatch.setattr(repository, "save_simulation_release_binding", fail_after_insert)
    with pytest.raises(RuntimeError, match="injected failure"):
        repository.migrate_miniqmt_binding_route(
            source_binding_id=source.binding_id,
            expected_source_binding_hash=source.binding_hash or "",
            source_effective_to=date(2026, 7, 15),
            target_binding=plan.target_binding,
        )

    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None
    assert repository.get_simulation_release_binding_by_hash(plan.target_binding.binding_hash or "") is None


def test_retryable_database_failure_retries_exactly_three_and_business_failure_does_not_retry() -> None:
    class RetryRepository(InMemorySimulationRuntimeRepository):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def migrate_miniqmt_binding_route(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            self.attempts += 1
            if self.attempts < 3:
                raise psycopg2.OperationalError("transient connection failure")
            return super().migrate_miniqmt_binding_route(**kwargs)

    repository = RetryRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    receipt = _apply(_service(repository, runtime_repository), source, target)
    assert receipt.target_release_id == target.release_id
    assert repository.attempts == 3

    class BusinessRepository(InMemorySimulationRuntimeRepository):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def migrate_miniqmt_binding_route(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            self.attempts += 1
            return super().migrate_miniqmt_binding_route(**kwargs)

    conflict_repository = BusinessRepository()
    _source_release, source, target = _source_and_target(conflict_repository)
    source_with_conflict = source.model_copy(update={"effective_to": date(2026, 7, 20)})
    conflict_repository.bindings[source.binding_id] = source_with_conflict
    with pytest.raises(Exception) as conflict:
        _apply(_service(conflict_repository, InMemoryMiniQMTExecutionRuntimeRepository()), source, target)
    assert not isinstance(conflict.value, psycopg2.Error)
    assert conflict_repository.attempts == 1


def test_commit_unknown_uses_durable_readback_without_replaying_migration() -> None:
    class CommitUnknownRepository(InMemorySimulationRuntimeRepository):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def migrate_miniqmt_binding_route(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            self.attempts += 1
            super().migrate_miniqmt_binding_route(**kwargs)
            raise psycopg2.OperationalError("connection lost after commit")

    repository = CommitUnknownRepository()
    _source_release, source, target = _source_and_target(repository)
    receipt = _apply(
        _service(repository, InMemoryMiniQMTExecutionRuntimeRepository()),
        source,
        target,
    )
    assert receipt.target_release_id == target.release_id
    assert repository.attempts == 1
    assert repository.get_simulation_release_binding(source.binding_id).effective_to == date(2026, 7, 15)


def test_migration_hash_and_identity_tampering_fail_loud() -> None:
    repository = InMemorySimulationRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    receipt = _apply(
        _service(repository, InMemoryMiniQMTExecutionRuntimeRepository()),
        source,
        target,
    )
    with pytest.raises(ValueError, match="receipt_sha256"):
        type(receipt).model_validate(
            {**receipt.model_dump(mode="python"), "receipt_sha256": "0" * 64}
        )

    target_binding = repository.get_simulation_release_binding(receipt.target_binding_id)
    tampered = target_binding.model_copy(
        update={"capital_allocation": target_binding.capital_allocation + 1.0}
    )
    from backend.services.simulation_runtime.miniqmt_route_migration import rebuild_receipt

    with pytest.raises(MiniQMTRouteMigrationError) as mismatch:
        rebuild_receipt(
            source_binding=repository.get_simulation_release_binding(source.binding_id),
            target_binding=tampered,
        )
    assert mismatch.value.context["reason_code"] == "MINIQMT_ROUTE_MIGRATION_READBACK_MISMATCH"
    assert "capital_allocation" in mismatch.value.context["mismatches"]


def test_inventory_marker_schema_and_preflight_invalid_inputs_fail_loud() -> None:
    repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    plan = _service(repository, runtime_repository).plan(
        source_binding_id=source.binding_id,
        target_release_id=target.release_id,
        effective_trade_date=EFFECTIVE_DATE,
        operator="unit-test",
    )

    inventory_payload = plan.inventory.model_dump(mode="python")
    with pytest.raises(ValueError, match="inventory_sha256"):
        MiniQMTRouteMigrationInventoryV1.model_validate(
            {**inventory_payload, "inventory_sha256": "0" * 64}
        )
    with pytest.raises(ValueError, match="source_binding_id is required"):
        MiniQMTRouteMigrationInventoryV1.model_validate(
            {**inventory_payload, "source_binding_id": ""}
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        MiniQMTRouteMigrationInventoryV1.model_validate(
            {**inventory_payload, "observed_at_utc": datetime(2026, 7, 15, 8, 0)}
        )

    marker_payload = plan.marker.model_dump(mode="python")
    with pytest.raises(ValueError, match="source_effective_to"):
        MiniQMTRouteMigrationMarkerV1.model_validate(
            {**marker_payload, "source_effective_to": EFFECTIVE_DATE}
        )
    with pytest.raises(ValueError, match="marker_sha256"):
        MiniQMTRouteMigrationMarkerV1.model_validate(
            {**marker_payload, "marker_sha256": "0" * 64}
        )

    with pytest.raises(MiniQMTRouteMigrationError) as identity:
        build_inventory(
            source_binding=source,
            target_release=target.model_copy(update={"package_id": "different-package"}),
            effective_trade_date=EFFECTIVE_DATE,
            runtime_repository=runtime_repository,
            broker_open_orders=[],
            observed_at_utc=OBSERVED_AT,
        )
    assert identity.value.context["reason_code"] == "MINIQMT_ROUTE_MIGRATION_IDENTITY_CONFLICT"

    with pytest.raises(ValueError, match="runtime_limit must be positive"):
        build_inventory(
            source_binding=source,
            target_release=target,
            effective_trade_date=EFFECTIVE_DATE,
            runtime_repository=runtime_repository,
            broker_open_orders=[],
            observed_at_utc=OBSERVED_AT,
            runtime_limit=0,
        )


@pytest.mark.parametrize(
    "broker_rows, expected_reason",
    [
        ([None], "MINIQMT_ROUTE_MIGRATION_BROKER_SCHEMA_INVALID"),
        ([{"strategy_name": "missing-order-id"}], "MINIQMT_ROUTE_MIGRATION_BROKER_SCHEMA_INVALID"),
    ],
)
def test_broker_inventory_schema_invalid_is_not_silently_skipped(
    broker_rows: list[Any],
    expected_reason: str,
) -> None:
    repository = InMemorySimulationRuntimeRepository()
    _source_release, source, target = _source_and_target(repository)
    service = MiniQMTRouteMigrationService(
        simulation_repository=repository,
        runtime_repository=InMemoryMiniQMTExecutionRuntimeRepository(),
        broker_open_order_reader=lambda: broker_rows,
        clock=lambda: OBSERVED_AT,
    )
    with pytest.raises(MiniQMTRouteMigrationError) as invalid:
        _apply(service, source, target)
    assert invalid.value.context["reason_code"] == expected_reason
    assert repository.get_simulation_release_binding(source.binding_id).effective_to is None


def _binding_row(
    binding: SimulationReleaseBinding,
    *,
    effective_to: date | None = None,
) -> dict[str, Any]:
    return {
        "binding_id": binding.binding_id,
        "strategy_id": binding.strategy_id,
        "release_id": binding.release_id,
        "release_hash": binding.release_hash,
        "package_id": binding.package_id,
        "manifest_sha256": binding.manifest_sha256,
        "broker_backend": binding.broker_backend.value,
        "broker_account_id": binding.broker_account_id,
        "account_group_id": binding.account_group_id,
        "strategy_slot_id": binding.strategy_slot_id,
        "capital_allocation": binding.capital_allocation,
        "strategy_name": binding.strategy_name,
        "order_remark_prefix": binding.order_remark_prefix,
        "effective_from": binding.effective_from,
        "effective_to": effective_to,
        "approval_state": binding.approval_state.value,
        "binding_config_json": binding.binding_config_json,
        "binding_hash": binding.binding_hash,
        "created_by": binding.created_by,
        "created_reason": binding.created_reason,
        "created_at": binding.created_at,
        "updated_at": binding.updated_at,
    }


class _MigrationCursor:
    def __init__(self, connection: "_MigrationConnection") -> None:
        self.connection = connection
        self.rowcount = -1
        self._row: dict[str, Any] | None = None

    def __enter__(self) -> "_MigrationCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, params))
        self.rowcount = -1
        if "WHERE binding_id = %s FOR UPDATE" in normalized:
            self._row = _binding_row(self.connection.source)
            return
        if "binding_hash <> %s" in normalized:
            self._row = None
            return
        if normalized.startswith("SELECT *") and "WHERE binding_hash = %s" in normalized:
            self._row = (
                _binding_row(self.connection.target)
                if self.connection.target_inserted
                else None
            )
            return
        if normalized.startswith("INSERT INTO paper_v2.simulation_release_binding"):
            self.connection.target_inserted = True
            self._row = None
            if self.connection.fail_after_target_insert:
                raise RuntimeError("injected PostgreSQL failure after target insert")
            return
        if normalized.startswith("UPDATE paper_v2.simulation_release_binding"):
            self.connection.source_closed = True
            self.rowcount = 1
            self._row = None
            return
        if normalized.startswith("SELECT *") and "WHERE binding_id = %s" in normalized:
            self._row = _binding_row(
                self.connection.source,
                effective_to=(date(2026, 7, 15) if self.connection.source_closed else None),
            )
            return
        raise AssertionError(f"unexpected migration SQL: {normalized}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._row


class _MigrationConnection:
    def __init__(
        self,
        *,
        source: SimulationReleaseBinding,
        target: SimulationReleaseBinding,
        fail_after_target_insert: bool = False,
    ) -> None:
        self.source = source
        self.target = target
        self.fail_after_target_insert = fail_after_target_insert
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.target_inserted = False
        self.source_closed = False
        self.commit_count = 0
        self.rollback_count = 0

    def __enter__(self) -> "_MigrationConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.commit_count += 1
        else:
            self.rollback_count += 1
            self.target_inserted = False
            self.source_closed = False
        return None

    def cursor(self, **kwargs: Any) -> _MigrationCursor:
        return _MigrationCursor(self)


def test_postgres_route_migration_uses_one_transaction_and_rolls_back_after_insert_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repository = InMemorySimulationRuntimeRepository()
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    _source_release, source, target_release = _source_and_target(source_repository)
    plan = _service(source_repository, runtime_repository).plan(
        source_binding_id=source.binding_id,
        target_release_id=target_release.release_id,
        effective_trade_date=EFFECTIVE_DATE,
        operator="unit-test",
    )

    success_connection = _MigrationConnection(source=source, target=plan.target_binding)
    repository = SimulationRuntimeRepository(conn_factory=lambda: success_connection)
    monkeypatch.setattr(
        repository,
        "get_strategy_runtime_release",
        lambda release_id: target_release,
    )
    persisted_source, persisted_target = repository.migrate_miniqmt_binding_route(
        source_binding_id=source.binding_id,
        expected_source_binding_hash=source.binding_hash or "",
        source_effective_to=date(2026, 7, 15),
        target_binding=plan.target_binding,
    )

    assert persisted_source.effective_to == date(2026, 7, 15)
    assert persisted_target.binding_hash == plan.target_binding.binding_hash
    assert success_connection.commit_count == 1
    assert success_connection.rollback_count == 0
    sql = [statement for statement, _params in success_connection.statements]
    assert "FOR UPDATE" in sql[0]
    assert next(index for index, statement in enumerate(sql) if statement.startswith("INSERT")) < next(
        index for index, statement in enumerate(sql) if statement.startswith("UPDATE")
    )

    failed_connection = _MigrationConnection(
        source=source,
        target=plan.target_binding,
        fail_after_target_insert=True,
    )
    failed_repository = SimulationRuntimeRepository(conn_factory=lambda: failed_connection)
    monkeypatch.setattr(
        failed_repository,
        "get_strategy_runtime_release",
        lambda release_id: target_release,
    )
    with pytest.raises(RuntimeError, match="injected PostgreSQL failure after target insert"):
        failed_repository.migrate_miniqmt_binding_route(
            source_binding_id=source.binding_id,
            expected_source_binding_hash=source.binding_hash or "",
            source_effective_to=date(2026, 7, 15),
            target_binding=plan.target_binding,
        )
    assert failed_connection.commit_count == 0
    assert failed_connection.rollback_count == 1
    assert failed_connection.target_inserted is False
    assert failed_connection.source_closed is False
