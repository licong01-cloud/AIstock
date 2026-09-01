"""LocalSIM economic persistence orchestration owner."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.daily_context import SimulationBrokerBackend, canonical_json_sha256
from backend.services.simulation_execution.localsim.economic import (
    LocalSimEconomicCommitRequest,
    LocalSimEconomicCoordinator,
    canonical_local_sim_json_value,
    local_sim_fact_payload,
)
from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    LocalSimMarketMarkV1,
    LocalSimPersistenceResult,
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
)
from backend.services.simulation_execution.localsim.projection import (
    LocalSimProjector,
    build_local_sim_projection_payload,
    derive_local_sim_projection_state_contract,
    local_sim_projection_error_is_retryable,
    local_sim_projection_paper_error,
)
from backend.services.simulation_execution.localsim.valuation import LocalSimValuationCoordinator
from backend.services.trading_core.errors import BrokerConnectivityError, DataUnavailableError
from backend.services.trading_core.models import AccountSnapshot, PositionLot, RunStatus


def local_sim_hashed_fact_map(
    items: tuple[Any, ...],
    *,
    identity_field: str,
    fact_type: str,
) -> dict[str, str]:
    hashed: dict[str, str] = {}
    for item in items:
        identity = str(getattr(item, identity_field, "") or "").strip()
        if not identity or identity in hashed:
            raise DataUnavailableError(
                "LocalSim economic fact identity is missing or duplicated",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_FACT_IDENTITY_INVALID",
                    "fact_type": fact_type,
                    "identity": identity or None,
                },
            )
        hashed[identity] = canonical_json_sha256(local_sim_fact_payload(item, fact_type=fact_type))
    return dict(sorted(hashed.items()))


def local_sim_is_first_causal_bar_wait(
    *,
    states: tuple[LocalSimExecutionStateV1, ...],
    orders: tuple[Any, ...],
    fills: tuple[Any, ...],
    events: tuple[Any, ...],
    cash_entries: tuple[Any, ...],
) -> bool:
    if not states or not orders or fills or events or cash_entries:
        return False
    return all(
        state.runtime_status == LocalSimExecutionRuntimeStatus.WAITING_FOR_CAUSAL_BAR
        and state.sequence == 0
        and state.filled_quantity == 0
        and state.remaining_quantity == state.total_quantity
        and state.last_processed_bar_time is None
        and state.last_applied_bar_identity is None
        for state in states
    )


def local_sim_persistence_failure_stage(exc: BaseException) -> str:
    context = getattr(exc, "context", None)
    if isinstance(context, dict):
        reason_code = str(context.get("reason_code") or "")
        if reason_code == "LOCALSIM_PERSISTENCE_CASH_CONTEXT_MISSING":
            return "LOCAL_SIM_PERSISTENCE_CASH_CONTEXT_MISSING"
        if reason_code.startswith("LOCALSIM_MARK_") or reason_code.startswith("LOCALSIM_SUSPENDED_"):
            return "LOCAL_SIM_MARK_VALIDATION_FAILED"
        if reason_code.startswith("LOCALSIM_PROJECTION_"):
            return "LOCAL_SIM_PROJECTION_FAILED"
        if reason_code.startswith("LOCALSIM_ECONOMIC_"):
            return "LOCAL_SIM_ECONOMIC_COMMIT_FAILED"
    message = str(exc)
    if "no execution snapshot" in message:
        return "LOCAL_SIM_PERSISTENCE_SNAPSHOT_MISSING"
    if "requires account cash or cash ledger entries" in message:
        return "LOCAL_SIM_PERSISTENCE_CASH_CONTEXT_MISSING"
    if "order count does not match" in message:
        return "LOCAL_SIM_PERSISTENCE_ORDER_MISMATCH"
    if "without durable fills and cash ledger entries" in message:
        return "LOCAL_SIM_PERSISTENCE_EMPTY_EFFECTS"
    return "LOCAL_SIM_PERSISTENCE_FAILED"


class LocalSimPersistenceCoordinator:
    """Own the complete broker-snapshot to economic-generation workflow."""

    def __init__(
        self,
        *,
        runtime_repository: Any,
        performance_service: Any,
        filter_snapshot_by_plan: Callable[..., Any],
        validate_execution_states: Callable[..., None],
        validate_snapshot_for_progress: Callable[..., None],
        validate_snapshot_for_success: Callable[..., None],
        paper_repository_for: Callable[..., Any],
        historical_residual_payload: Callable[..., dict[str, Any] | None],
        snapshot_time: Callable[..., datetime],
        cash_after: Callable[..., float],
        position_marks: Callable[..., Any],
        mark_failure_allows_pending: Callable[[BaseException], bool],
        ensure_paper_run: Callable[..., None],
        mark_submit_failure: Callable[..., Any],
        load_existing_plan_context: Callable[..., Any],
        effective_market_data_source: Callable[..., str],
        normalize_time: Callable[[datetime | None], datetime],
        binding_result_factory: Callable[..., Any],
    ) -> None:
        self._runtime_repository = runtime_repository
        self._performance_service = performance_service
        self._filter_snapshot_by_plan = filter_snapshot_by_plan
        self._validate_execution_states = validate_execution_states
        self._validate_snapshot_for_progress = validate_snapshot_for_progress
        self._validate_snapshot_for_success = validate_snapshot_for_success
        self._paper_repository_for = paper_repository_for
        self._historical_residual_payload = historical_residual_payload
        self._snapshot_time = snapshot_time
        self._cash_after = cash_after
        self._position_marks = position_marks
        self._mark_failure_allows_pending = mark_failure_allows_pending
        self._ensure_paper_run = ensure_paper_run
        self._mark_submit_failure = mark_submit_failure
        self._load_existing_plan_context = load_existing_plan_context
        self._effective_market_data_source = effective_market_data_source
        self._normalize_time = normalize_time
        self._binding_result_factory = binding_result_factory

    def _project_outbox(self, *, run_id: str, paper_repository: Any) -> tuple[Any, dict[str, Any]]:
        return LocalSimProjector(
            runtime_repository=self._runtime_repository,
            paper_repository=paper_repository,
            performance_service=self._performance_service,
        ).project_outbox(run_id=run_id, paper_repository=paper_repository)

    def _existing_projection_result(
        self,
        *,
        run_id: str,
        paper_repository: Any,
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> LocalSimPersistenceResult:
        return LocalSimProjector(
            runtime_repository=self._runtime_repository,
            paper_repository=paper_repository,
            performance_service=self._performance_service,
        ).existing_projection_result(
            run_id=run_id,
            observed_positions=observed_positions,
            observed_account=observed_account,
        )

    def replay_pending_projection(
        self,
        *,
        run_id: str,
        paper_repository: Any,
        context: Any | None = None,
        execution: Any | None = None,
        observed_positions: dict[str, PositionLot] | None = None,
        observed_account: Any | None = None,
        valuation_as_of_time: datetime | None = None,
    ) -> None:
        valuation_projector = None
        if context is not None and execution is not None:
            valuation_coordinator = LocalSimValuationCoordinator(
                runtime_repository=self._runtime_repository,
                paper_repository=paper_repository,
                performance_service=self._performance_service,
                position_marks=self._position_marks,
                snapshot_time=self._snapshot_time,
                mark_failure_allows_pending=self._mark_failure_allows_pending,
            )

            def project_valuation_pending(run: Any, outbox: LocalSimProjectionOutboxV1) -> None:
                valuation_coordinator.complete_pending_outbox(
                    run=run,
                    outbox=outbox,
                    paper_repository=paper_repository,
                    context=context,
                    execution=execution,
                    observed_positions=observed_positions,
                    observed_account=observed_account,
                    valuation_as_of_time=valuation_as_of_time,
                )

            valuation_projector = project_valuation_pending
        projector = LocalSimProjector(
            runtime_repository=self._runtime_repository,
            paper_repository=paper_repository,
            performance_service=self._performance_service,
        )
        projector.replay_pending(
            run_id=run_id,
            project_valuation_pending=valuation_projector,
            project_outbox=lambda pending_run_id: projector.project_outbox(
                run_id=pending_run_id,
                paper_repository=paper_repository,
            ),
        )

    def _readback_pending_economic_generation(
        self,
        *,
        binding: Any,
        run: Any,
        plan: Any,
        outbox: LocalSimProjectionOutboxV1,
        paper_repository: Any,
    ) -> None:
        """Prove one valuation-pending economic generation before projection.

        A committed outbox is not a substitute for independent readback of
        the receipt, durable states, and Paper economic facts.  Projection is
        allowed only after both durable planes prove the exact generation.
        """

        raw_receipts = run.run_payload_json.get("local_sim_economic_receipts_v1")
        if not isinstance(raw_receipts, dict):
            raise DataUnavailableError(
                "LocalSim valuation-pending economic receipt map is missing",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_MISSING",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "economic_readback_failure": True,
                },
            )
        try:
            receipts = {
                receipt_id: LocalSimEconomicReceiptV1.model_validate(raw) for receipt_id, raw in raw_receipts.items()
            }
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim valuation-pending economic receipt schema is invalid",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "economic_readback_failure": True,
                },
            ) from exc
        if any(receipt.receipt_id != key for key, receipt in receipts.items()):
            raise DataUnavailableError(
                "LocalSim valuation-pending economic receipt identity conflicts with its map key",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "economic_readback_failure": True,
                },
            )
        receipt = receipts.get(outbox.receipt_id)
        if receipt is None:
            raise DataUnavailableError(
                "LocalSim valuation-pending outbox has no economic receipt",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_MISSING",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "receipt_id": outbox.receipt_id,
                    "economic_readback_failure": True,
                },
            )
        if (
            receipt.run_id != run.run_id
            or receipt.binding_id != binding.binding_id
            or receipt.trade_date != run.trade_date
            or receipt.plan_id != plan.plan_id
            or receipt.generation != outbox.generation
            or receipt.economic_hash != outbox.economic_hash
        ):
            raise DataUnavailableError(
                "LocalSim valuation-pending economic generation identity conflicts",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "receipt_id": receipt.receipt_id,
                    "economic_readback_failure": True,
                },
            )

        facts = receipt.economic_facts
        payload = outbox.projection_payload
        fact_maps: dict[str, dict[str, Any]] = {}
        for field_name in (
            "order_hashes",
            "fill_hashes",
            "order_event_hashes",
            "cash_entry_hashes",
            "state_hashes",
            "position_hashes",
        ):
            raw_hashes = facts.get(field_name)
            if not isinstance(raw_hashes, dict):
                raise DataUnavailableError(
                    "LocalSim valuation-pending economic fact identities are invalid",
                    context={
                        "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "outbox_id": outbox.outbox_id,
                        "field": field_name,
                        "economic_readback_failure": True,
                    },
                )
            fact_maps[field_name] = raw_hashes
        actual_state_hashes = {
            state.state_id: state.state_hash
            for state in self._runtime_repository.list_local_sim_execution_states(run.run_id, authoritative=True)
        }
        if (
            facts.get("schema_version") != "local_sim_valuation_pending_economic_facts_v1"
            or facts.get("fact_kind") != "ECONOMIC_FACTS_WITH_VALUATION_PENDING"
            or facts.get("run_id") != run.run_id
            or facts.get("binding_id") != binding.binding_id
            or facts.get("trade_date") != run.trade_date.isoformat()
            or facts.get("plan_id") != plan.plan_id
            or fact_maps["state_hashes"] != actual_state_hashes
            or fact_maps["position_hashes"] != payload.get("position_hashes")
            or facts.get("cash_reference") != payload.get("cash_reference")
            or facts.get("mark_hashes") != {}
            or facts.get("account_snapshot_hash") is not None
            or payload.get("economic_hash") != receipt.economic_hash
        ):
            raise DataUnavailableError(
                "LocalSim valuation-pending economic facts conflict with the outbox generation",
                context={
                    "reason_code": "LOCALSIM_ECONOMIC_RECEIPT_READBACK_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "receipt_id": receipt.receipt_id,
                    "economic_readback_failure": True,
                },
            )
        try:
            self._runtime_repository.readback_local_sim_economic_commit(
                run_id=run.run_id,
                receipt=receipt,
                outbox=outbox,
            )
            paper_repository.readback_local_sim_economic_facts(
                run_id=run.run_id,
                order_ids=set(fact_maps["order_hashes"]),
                fill_ids=set(fact_maps["fill_hashes"]),
                order_event_ids=set(fact_maps["order_event_hashes"]),
                cash_fill_ids=set(fact_maps["cash_entry_hashes"]),
            )
        except Exception as exc:
            cause_context = dict(getattr(exc, "context", None) or {})
            retryable = local_sim_projection_error_is_retryable(exc)
            raise DataUnavailableError(
                "LocalSim valuation-pending economic facts failed independent readback",
                context={
                    **cause_context,
                    "reason_code": (
                        "LOCALSIM_ECONOMIC_READBACK_RETRYABLE"
                        if retryable
                        else str(cause_context.get("reason_code") or "LOCALSIM_ECONOMIC_READBACK_FAILED")
                    ),
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "receipt_id": receipt.receipt_id,
                    "generation": receipt.generation,
                    "economic_readback_failure": True,
                },
            ) from exc

    def recover_pending_projection_if_needed(
        self,
        *,
        binding: Any,
        run: Any,
        plan: Any,
        runtime_release: Any,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
    ) -> Any | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        raw_outbox = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw_outbox is None:
            return None
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw_outbox)
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim failed run projection outbox cannot be recovered",
                context={
                    "reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                },
            ) from exc
        if outbox.run_id != run.run_id or outbox.plan_id != plan.plan_id:
            raise DataUnavailableError(
                "LocalSim failed run projection outbox identity conflicts with the frozen plan",
                context={
                    "reason_code": "LOCALSIM_PROJECTION_OUTBOX_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "outbox_run_id": outbox.run_id,
                    "outbox_plan_id": outbox.plan_id,
                },
            )
        valuation_pending = outbox.projection_payload.get(
            "schema_version"
        ) == "local_sim_valuation_pending_projection_payload_v1" and outbox.status in {
            LocalSimProjectionOutboxStatus.PENDING,
            LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE,
        }
        failed_projection_recovery = run.status == type(run.status).FAILED_RETRYABLE and (
            outbox.status
            in {
                LocalSimProjectionOutboxStatus.PENDING,
                LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE,
            }
            or isinstance(
                run.run_payload_json.get("local_sim_projection_readback_failure"),
                dict,
            )
        )
        needs_recovery = failed_projection_recovery or (
            run.status == type(run.status).INTRADAY_RUNNING and valuation_pending
        )
        if not needs_recovery:
            return None
        persistence = run.run_payload_json.get("local_sim_persistence")
        if valuation_pending and (
            not isinstance(persistence, dict) or persistence.get("status") != "INTRADAY_VALUATION_PENDING"
        ):
            raise DataUnavailableError(
                "LocalSim valuation-pending outbox has no matching persistence state",
                context={
                    "reason_code": "LOCALSIM_VALUATION_PENDING_STATE_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )
        context = self._load_existing_plan_context(
            runtime_release=runtime_release,
            binding=binding,
            plan=plan,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        paper_repository = self._paper_repository_for(binding=binding, run=run, context=context)
        if valuation_pending:
            self._readback_pending_economic_generation(
                binding=binding,
                run=run,
                plan=plan,
                outbox=outbox,
                paper_repository=paper_repository,
            )
        self.replay_pending_projection(
            run_id=run.run_id,
            paper_repository=paper_repository,
            context=context,
            execution=SimpleNamespace(run=run, execution_plan=plan),
            valuation_as_of_time=self._normalize_time(as_of_time),
        )
        recovered = self._runtime_repository.get_simulation_daily_run(run.run_id)
        if recovered.status == type(run.status).FAILED_RETRYABLE:
            raise DataUnavailableError(
                "LocalSim projection replay returned without clearing the retryable state",
                context={
                    "reason_code": "LOCALSIM_PROJECTION_RECOVERY_INCOMPLETE",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )
        recovered_outbox = LocalSimProjectionOutboxV1.model_validate(
            recovered.run_payload_json.get("local_sim_projection_outbox_v1")
        )
        recovery_status = "LOCALSIM_PROJECTION_RECOVERED"
        if (
            recovered_outbox.projection_payload.get("schema_version")
            == "local_sim_valuation_pending_projection_payload_v1"
            and recovered_outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
        ):
            recovery_status = "LOCALSIM_VALUATION_PENDING"
        return self._binding_result_factory(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=recovery_status,
            run=recovered,
            execution_plan=plan,
            data_source=context.market_data_source
            or self._effective_market_data_source(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    @staticmethod
    def _economic_facts(
        *,
        run: Any,
        execution: Any,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
        states: tuple[LocalSimExecutionStateV1, ...],
        positions: dict[str, PositionLot],
        marks: dict[str, LocalSimMarketMarkV1],
        account_snapshot: AccountSnapshot,
    ) -> dict[str, Any]:
        return {
            "schema_version": "local_sim_economic_facts_v1",
            "run_id": run.run_id,
            "binding_id": run.binding_id,
            "trade_date": run.trade_date.isoformat(),
            "plan_id": execution.execution_plan.plan_id,
            "order_hashes": local_sim_hashed_fact_map(orders, identity_field="order_id", fact_type="order"),
            "fill_hashes": local_sim_hashed_fact_map(fills, identity_field="fill_id", fact_type="fill"),
            "order_event_hashes": local_sim_hashed_fact_map(events, identity_field="event_id", fact_type="order_event"),
            "cash_entry_hashes": local_sim_hashed_fact_map(
                cash_entries, identity_field="fill_id", fact_type="cash_entry"
            ),
            "state_hashes": {
                state.state_id: state.state_hash for state in sorted(states, key=lambda item: item.state_id)
            },
            "position_hashes": {
                symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
                for symbol, position in sorted(positions.items())
            },
            "mark_hashes": {symbol: mark.mark_hash for symbol, mark in sorted(marks.items())},
            "account_snapshot_hash": canonical_json_sha256(
                local_sim_fact_payload(account_snapshot, fact_type="account_snapshot")
            ),
        }

    def _persist_first_causal_bar_wait(
        self,
        *,
        binding: Any,
        run: Any,
        execution: Any,
        context: Any,
        paper_repository: Any,
        orders: tuple[Any, ...],
        states: tuple[LocalSimExecutionStateV1, ...],
        positions: dict[str, PositionLot],
        account: Any,
        current_states: dict[str, LocalSimExecutionStateV1],
    ) -> LocalSimPersistenceResult:
        """Persist order acceptance and durable wait without inventing a market mark.

        Before the first causal minute closes, no authoritative current-day mark
        exists.  Order/state acceptance is still durable business state, but NAV,
        performance and position valuation are not yet derivable.  This separate
        projection kind keeps the run active and leaves valuation absent rather
        than rolling back the minute runtime or fabricating previous/plan prices.
        """

        committed = False
        try:
            snapshot_time = self._snapshot_time(
                fills=(),
                events=(),
                run=run,
                local_broker=context.local_broker,
                market_data_source=context.market_data_source,
            )
            raw_cash = getattr(account, "cash", None) if account is not None else context.cash
            if isinstance(raw_cash, bool):
                raise ValueError("boolean cash is invalid")
            cash = float(raw_cash)
            if not math.isfinite(cash) or cash < 0:
                raise ValueError("cash must be finite and non-negative")
            order_hashes = local_sim_hashed_fact_map(
                orders,
                identity_field="order_id",
                fact_type="order",
            )
            state_hashes = {
                state.state_id: state.state_hash for state in sorted(states, key=lambda item: item.state_id)
            }
            position_hashes = {
                symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
                for symbol, position in sorted(positions.items())
            }
            economic_facts = {
                "schema_version": "local_sim_waiting_economic_facts_v1",
                "fact_kind": "FIRST_CAUSAL_BAR_WAIT",
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "trade_date": run.trade_date.isoformat(),
                "plan_id": execution.execution_plan.plan_id,
                "observed_until": snapshot_time.isoformat(),
                "order_hashes": order_hashes,
                "fill_hashes": {},
                "order_event_hashes": {},
                "cash_entry_hashes": {},
                "state_hashes": state_hashes,
                "position_hashes": position_hashes,
                "mark_hashes": {},
                "account_snapshot_hash": None,
                "cash_reference": cash,
            }
            economic_hash = canonical_json_sha256(economic_facts)
            persistence = {
                "schema_version": "local_sim_persistence_v2",
                "status": "INTRADAY_WAITING_FOR_CAUSAL_BAR",
                "paper_v2_run_id": run.run_id,
                "order_count": len(orders),
                "fill_count": 0,
                "order_event_count": 0,
                "cash_ledger_count": 0,
                "position_count": len(positions),
                "snapshot_time": None,
                "observed_until": snapshot_time.isoformat(),
                "cash": cash,
                "nav": None,
                "terminal": False,
                "execution_state_count": len(states),
                "active_state_count": len(states),
                "residual_state_count": 0,
                "failed_terminal_state_count": 0,
                "valuation_status": "WAITING_FOR_FIRST_CAUSAL_MARK",
            }
            projection_payload = {
                "schema_version": "local_sim_waiting_projection_payload_v1",
                "projection_kind": "FIRST_CAUSAL_BAR_WAIT",
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "plan_id": execution.execution_plan.plan_id,
                "trade_date": run.trade_date.isoformat(),
                "portfolio_id": str(context.portfolio_id or execution.execution_plan.portfolio_id),
                "observed_until": snapshot_time.isoformat(),
                "cash_reference": cash,
                "positions": [item.model_dump(mode="json") for _, item in sorted(positions.items())],
                "position_hashes": position_hashes,
                "order_hashes": order_hashes,
                "state_hashes": state_hashes,
                "final_simulation_status": type(run.status).INTRADAY_RUNNING.value,
                "final_paper_status": RunStatus.RUNNING.value,
                "final_event_type": "RUN_INTRADAY_WAITING_FOR_CAUSAL_BAR",
                "final_event_message": "LocalSim is durably waiting for the first closed causal minute bar",
                "local_sim_persistence": persistence,
                "economic_hash": economic_hash,
            }
            expected_versions = {
                state.state_id: (
                    (current_states[state.state_id].sequence, current_states[state.state_id].state_hash)
                    if state.state_id in current_states
                    else None
                )
                for state in states
            }
            payload_patch = {
                "local_sim_persistence": {**persistence, "status": "PROJECTION_PENDING"},
                "local_sim_durable_minute_loop": {
                    "schema_version": "local_sim_durable_minute_loop_v1",
                    "state_count": len(states),
                    "active_state_count": len(states),
                    "terminal": False,
                },
                "local_sim_first_causal_bar_wait_v1": {
                    "schema_version": "local_sim_first_causal_bar_wait_v1",
                    "reason_code": "LOCALSIM_WAITING_FOR_FIRST_CAUSAL_BAR",
                    "observed_until": snapshot_time.isoformat(),
                    "state_ids": sorted(state_hashes),
                    "broker_called": True,
                    "terminal": False,
                },
                "broker_called": True,
                "submitted_intents": len(orders),
                "failed_intents": 0,
                "last_stage": "LOCAL_SIM_WAITING_ECONOMIC_COMMITTED",
            }
            commit_result = LocalSimEconomicCoordinator(
                runtime_repository=self._runtime_repository,
                paper_repository=paper_repository,
                ensure_paper_run=lambda: self._ensure_paper_run(
                    repository=paper_repository,
                    run=run,
                    context=context,
                ),
            ).commit(
                LocalSimEconomicCommitRequest(
                    run_id=run.run_id,
                    binding_id=binding.binding_id,
                    trade_date=run.trade_date,
                    plan_id=execution.execution_plan.plan_id,
                    states=states,
                    expected_versions=expected_versions,
                    economic_facts=economic_facts,
                    projection_payload=projection_payload,
                    status=type(run.status).INTRADAY_RUNNING,
                    payload_patch=payload_patch,
                    payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
                    orders=orders,
                    event_message="LocalSim order/state wait facts committed; first causal mark is pending",
                    event_context={
                        "source": "simulation_runtime_local_sim",
                        "simulation_run_id": run.run_id,
                        "execution_plan_id": execution.execution_plan.plan_id,
                        "projection_kind": "FIRST_CAUSAL_BAR_WAIT",
                    },
                )
            )
            receipt = commit_result.receipt
            outbox = commit_result.outbox
            committed = True
            projected_run, performance = self._project_outbox(
                run_id=run.run_id,
                paper_repository=paper_repository,
            )
            projected_persistence = projected_run.run_payload_json.get("local_sim_persistence")
            if not isinstance(projected_persistence, dict):
                raise DataUnavailableError(
                    "LocalSim first-bar wait projection lost its persistence receipt",
                    context={"reason_code": "LOCALSIM_PERSISTENCE_RECEIPT_MISSING", "run_id": run.run_id},
                )
            return LocalSimPersistenceResult(
                payload={
                    "order_count": len(orders),
                    "fill_count": 0,
                    "cash_ledger_count": 0,
                    "position_count": len(positions),
                    "cash": cash,
                    "nav": None,
                    "terminal": False,
                    "active_state_count": len(states),
                    "residual_state_count": 0,
                },
                positions=positions,
                marks={},
                cash=cash,
                economic_receipt_id=receipt.receipt_id,
                outbox_id=outbox.outbox_id,
                generation=receipt.generation,
                performance_payload=performance,
            )
        except Exception as exc:
            context_payload = {
                **dict(getattr(exc, "context", None) or {}),
                "reason_code": str(
                    dict(getattr(exc, "context", None) or {}).get("reason_code")
                    or "LOCALSIM_FIRST_CAUSAL_BAR_WAIT_PERSISTENCE_FAILED"
                ),
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "plan_id": execution.execution_plan.plan_id,
                "economic_commit_staged": committed,
            }
            if committed:
                context_payload.update(
                    {
                        "broker_called": True,
                        "submitted_intents": len(orders),
                        "failed_intents": 0,
                    }
                )
            raise DataUnavailableError(
                "LocalSim first causal-bar wait could not be persisted or projected durably",
                context=context_payload,
            ) from exc

    def _persist_valuation_pending(
        self,
        *,
        binding: Any,
        run: Any,
        execution: Any,
        context: Any,
        paper_repository: Any,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
        states: tuple[LocalSimExecutionStateV1, ...],
        positions: dict[str, PositionLot],
        cash: float,
        snapshot_time: datetime,
        current_states: dict[str, LocalSimExecutionStateV1],
        contract: dict[str, Any],
        mark_error: BaseException,
    ) -> LocalSimPersistenceResult:
        """Commit economic facts while leaving valuation explicitly pending."""

        if not math.isfinite(cash) or cash < 0:
            raise DataUnavailableError(
                "LocalSim valuation-pending cash reference is invalid",
                context={
                    "reason_code": "LOCALSIM_VALUATION_CASH_INVALID",
                    "run_id": run.run_id,
                },
            )
        mark_context = dict(getattr(mark_error, "context", None) or {})
        mark_reason = str(mark_context.get("reason_code") or "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE")
        missing_symbols = sorted(
            {
                str(mark_context.get("symbol") or "").strip(),
                *(str(item).strip() for item in mark_context.get("missing_symbols", []) if str(item).strip()),
            }
            - {""}
        )
        if not missing_symbols:
            missing_symbols = sorted(positions)
        order_hashes = local_sim_hashed_fact_map(orders, identity_field="order_id", fact_type="order")
        fill_hashes = local_sim_hashed_fact_map(fills, identity_field="fill_id", fact_type="fill")
        event_hashes = local_sim_hashed_fact_map(events, identity_field="event_id", fact_type="order_event")
        cash_hashes = local_sim_hashed_fact_map(cash_entries, identity_field="fill_id", fact_type="cash_entry")
        state_hashes = {state.state_id: state.state_hash for state in sorted(states, key=lambda item: item.state_id)}
        position_hashes = {
            symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
            for symbol, position in sorted(positions.items())
        }
        valuation_error = {
            "reason_code": mark_reason,
            "type": type(mark_error).__name__,
            "message": str(mark_error),
            "missing_symbols": missing_symbols,
            "context": canonical_local_sim_json_value(mark_context),
            "observed_at": datetime.now(UTC).isoformat(),
        }
        economic_facts = {
            "schema_version": "local_sim_valuation_pending_economic_facts_v1",
            "fact_kind": "ECONOMIC_FACTS_WITH_VALUATION_PENDING",
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "trade_date": run.trade_date.isoformat(),
            "plan_id": execution.execution_plan.plan_id,
            "observed_until": snapshot_time.isoformat(),
            "order_hashes": order_hashes,
            "fill_hashes": fill_hashes,
            "order_event_hashes": event_hashes,
            "cash_entry_hashes": cash_hashes,
            "state_hashes": state_hashes,
            "position_hashes": position_hashes,
            "mark_hashes": {},
            "account_snapshot_hash": None,
            "cash_reference": cash,
            "valuation_error": valuation_error,
        }
        economic_hash = canonical_json_sha256(economic_facts)
        active_states = contract["active_states"]
        residual_states = contract["residual_states"]
        failed_terminal_states = contract["failed_terminal_states"]
        historical_residual = contract["historical_residual"]
        nonfilled_terminal_states = contract["nonfilled_terminal_states"]
        paper_error = local_sim_projection_paper_error(
            historical_residual=historical_residual,
            nonfilled_terminal_states=nonfilled_terminal_states,
            terminal_failure=bool(contract["terminal_failure"]),
        )
        pending_persistence = {
            "schema_version": "local_sim_persistence_v2",
            "status": "INTRADAY_VALUATION_PENDING",
            "paper_v2_run_id": run.run_id,
            "order_count": len(orders),
            "fill_count": len(fills),
            "order_event_count": len(events),
            "cash_ledger_count": len(cash_entries),
            "position_count": len(positions),
            "snapshot_time": None,
            "observed_until": snapshot_time.isoformat(),
            "cash": cash,
            "nav": None,
            "terminal": False,
            "economic_terminal": bool(contract["terminal"]),
            "execution_state_count": len(states),
            "active_state_count": len(active_states),
            "residual_state_count": len(residual_states),
            "failed_terminal_state_count": len(failed_terminal_states),
            "valuation_status": "WAITING_FOR_AUTHORITATIVE_MARKS",
            "missing_mark_symbols": missing_symbols,
        }
        projection_payload = {
            "schema_version": "local_sim_valuation_pending_projection_payload_v1",
            "projection_kind": "VALUATION_PENDING",
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "plan_id": execution.execution_plan.plan_id,
            "trade_date": run.trade_date.isoformat(),
            "portfolio_id": str(context.portfolio_id or execution.execution_plan.portfolio_id),
            "initial_capital": float(binding.capital_allocation),
            "realized_pnl": float(context.realized_pnl),
            "positions": [item.model_dump(mode="json") for _, item in sorted(positions.items())],
            "position_hashes": position_hashes,
            "cash_reference": cash,
            "valuation_snapshot_time": snapshot_time.isoformat(),
            "valuation_error": valuation_error,
            "snapshot_metadata": {
                "source": "simulation_runtime_local_sim",
                "simulation_run_id": run.run_id,
                "execution_plan_id": execution.execution_plan.plan_id,
                "order_count": len(orders),
                "fill_count": len(fills),
                "cash_ledger_count": len(cash_entries),
                "position_count": len(positions),
                "terminal": bool(contract["terminal"]),
            },
            "final_simulation_status": contract["final_status"].value,
            "final_paper_status": contract["final_paper_status"].value,
            "final_event_type": contract["final_event_type"],
            "final_event_message": contract["final_event_message"],
            "final_event_context": {
                "source": "simulation_runtime_local_sim",
                "simulation_run_id": run.run_id,
                "execution_plan_id": execution.execution_plan.plan_id,
                "order_count": len(orders),
                "fill_count": len(fills),
                "cash_ledger_count": len(cash_entries),
                "position_count": len(positions),
                "snapshot_time": snapshot_time.isoformat(),
                "local_sim_historical_residual": historical_residual,
                "terminal": bool(contract["terminal"]),
                "active_state_ids": [item.state_id for item in active_states],
                "residual_state_ids": [item.state_id for item in residual_states],
            },
            "paper_error": paper_error,
            "completed_persistence": {
                "schema_version": "local_sim_persistence_v2",
                "status": contract["persistence_status"],
                "paper_v2_run_id": run.run_id,
                "order_count": len(orders),
                "fill_count": len(fills),
                "order_event_count": len(events),
                "cash_ledger_count": len(cash_entries),
                "position_count": len(positions),
                "snapshot_time": snapshot_time.isoformat(),
                "cash": cash,
                "nav": None,
                "terminal": bool(contract["terminal"]),
                "execution_state_count": len(states),
                "active_state_count": len(active_states),
                "residual_state_count": len(residual_states),
                "failed_terminal_state_count": len(failed_terminal_states),
            },
            "economic_hash": economic_hash,
            "tca_generation": {
                "schema_version": "local_sim_tca_generation_v1",
                "execution_plan_id": execution.execution_plan.plan_id,
                "execution_plan_hash": execution.execution_plan.plan_hash,
                "economic_hash": economic_hash,
            },
        }
        payload_patch: dict[str, Any] = {
            "local_sim_persistence": pending_persistence,
            "local_sim_valuation_pending_v1": {
                "schema_version": "local_sim_valuation_pending_v1",
                "status": "WAITING_FOR_AUTHORITATIVE_MARKS",
                "run_id": run.run_id,
                "plan_id": execution.execution_plan.plan_id,
                "missing_symbols": missing_symbols,
                "valuation_error": valuation_error,
                "economic_terminal": bool(contract["terminal"]),
                "broker_called": True,
            },
            "last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED_VALUATION_PENDING",
            "broker_called": True,
            "submitted_intents": len(orders),
            "failed_intents": 0,
        }
        if states:
            payload_patch["local_sim_durable_minute_loop"] = {
                "schema_version": "local_sim_durable_minute_loop_v1",
                "state_count": len(states),
                "active_state_count": len(active_states),
                "terminal": bool(contract["terminal"]),
            }
        if historical_residual or residual_states:
            payload_patch["local_sim_capacity_residual_terminalization"] = {
                "schema_version": "localsim_residual_terminalization_v2",
                "reason": (
                    "broker_execution_cash_limited_buy_residual"
                    if historical_residual and int(historical_residual.get("schedule_residual_count") or 0) == 0
                    else "historical_execution_schedule_residual"
                    if historical_residual
                    else "execution_schedule_residual_at_close"
                ),
                "status": contract["final_status"].value,
                "residual_order_count": int((historical_residual or {}).get("residual_order_count") or 0),
                "capital_residual_count": int((historical_residual or {}).get("capital_residual_count") or 0),
                "schedule_residual_count": int((historical_residual or {}).get("schedule_residual_count") or 0),
                "prepared_intent_count": int((historical_residual or {}).get("prepared_intent_count") or 0),
                "residual_orders": list((historical_residual or {}).get("residual_orders") or []),
                "residual_state_ids": [state.state_id for state in residual_states],
                "terminalized_at": datetime.now(UTC).isoformat(),
            }
        if failed_terminal_states:
            payload_patch["local_sim_terminal_failure_v1"] = {
                "schema_version": "local_sim_terminal_failure_v1",
                "reason": "market_data_integrity_failure",
                "status": contract["final_status"].value,
                "failed_states": [
                    {
                        "state_id": state.state_id,
                        "intent_id": state.intent_id,
                        "symbol": state.symbol,
                        "reason_code": state.terminal_reason,
                        "context": state.waiting_context,
                    }
                    for state in failed_terminal_states
                ],
                "terminalized_at": datetime.now(UTC).isoformat(),
            }
        expected_versions = {
            state.state_id: (
                (current_states[state.state_id].sequence, current_states[state.state_id].state_hash)
                if state.state_id in current_states
                else None
            )
            for state in states
        }

        def on_valuation_generation_created(
            receipt: LocalSimEconomicReceiptV1,
            outbox: LocalSimProjectionOutboxV1,
        ) -> None:
            paper_repository.save_run_event(
                run_id=run.run_id,
                event_type="RUN_INTRADAY_VALUATION_PENDING",
                message="LocalSim is waiting for authoritative marks without rolling back economic facts",
                context={
                    "source": "simulation_runtime_local_sim",
                    "simulation_run_id": run.run_id,
                    "local_sim_generation": receipt.generation,
                    "local_sim_outbox_id": outbox.outbox_id,
                    "missing_symbols": missing_symbols,
                    "reason_code": mark_reason,
                },
            )
            paper_repository.update_run_status(
                paper_repository.get_run(run.run_id),
                RunStatus.RUNNING,
                error=None,
            )

        committed = False
        try:
            commit_result = LocalSimEconomicCoordinator(
                runtime_repository=self._runtime_repository,
                paper_repository=paper_repository,
                ensure_paper_run=lambda: self._ensure_paper_run(
                    repository=paper_repository,
                    run=run,
                    context=context,
                ),
            ).commit(
                LocalSimEconomicCommitRequest(
                    run_id=run.run_id,
                    binding_id=binding.binding_id,
                    trade_date=run.trade_date,
                    plan_id=execution.execution_plan.plan_id,
                    states=states,
                    expected_versions=expected_versions,
                    economic_facts=economic_facts,
                    projection_payload=projection_payload,
                    status=type(run.status).INTRADAY_RUNNING,
                    payload_patch=payload_patch,
                    payload_unset=(
                        "submit_failure",
                        "local_sim_retry_diagnostics",
                        "strategy_performance",
                        "performance_projection",
                    ),
                    orders=orders,
                    fills=fills,
                    events=events,
                    cash_entries=cash_entries,
                    event_message="LocalSim economic facts committed; authoritative valuation marks are pending",
                    event_context={
                        "source": "simulation_runtime_local_sim",
                        "simulation_run_id": run.run_id,
                        "execution_plan_id": execution.execution_plan.plan_id,
                        "projection_kind": "VALUATION_PENDING",
                        "missing_symbols": missing_symbols,
                    },
                    on_created=on_valuation_generation_created,
                )
            )
            receipt = commit_result.receipt
            outbox = commit_result.outbox
            committed = True
            return LocalSimPersistenceResult(
                payload={
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                    "position_count": len(positions),
                    "cash": cash,
                    "nav": None,
                    "terminal": False,
                    "economic_terminal": bool(contract["terminal"]),
                    "active_state_count": len(active_states),
                    "residual_state_count": len(residual_states),
                    "valuation_status": "WAITING_FOR_AUTHORITATIVE_MARKS",
                    "missing_mark_symbols": missing_symbols,
                },
                positions=positions,
                marks={},
                cash=cash,
                economic_receipt_id=receipt.receipt_id,
                outbox_id=outbox.outbox_id,
                generation=receipt.generation,
                performance_payload={},
            )
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim valuation-pending economic facts could not be persisted durably",
                context={
                    "reason_code": "LOCALSIM_VALUATION_PENDING_PERSISTENCE_FAILED",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "economic_commit_staged": committed,
                    "broker_called": committed,
                    "submitted_intents": len(orders) if committed else 0,
                    "failed_intents": 0 if committed else len(execution.execution_plan.intents),
                    "cause": str(exc),
                },
            ) from exc

    def persist_execution_result(
        self,
        *,
        binding: Any,
        run: Any,
        execution: Any,
        context: Any,
    ) -> LocalSimPersistenceResult | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or execution.status != "SUBMITTED":
            return None
        economic_commit_staged = False
        try:
            snapshot = getattr(execution.broker_result, "execution_snapshot", None)
            if snapshot is None:
                raise DataUnavailableError(
                    "LocalSim submit returned no execution snapshot for durable persistence",
                    context={
                        "run_id": run.run_id,
                        "strategy_id": binding.strategy_id,
                        "binding_id": binding.binding_id,
                        "plan_id": execution.execution_plan.plan_id,
                    },
                )
            orders, fills, events, cash_entries = self._filter_snapshot_by_plan(
                execution=execution,
                orders=tuple(getattr(snapshot, "orders", ()) or ()),
                fills=tuple(getattr(snapshot, "fills", ()) or ()),
                events=tuple(getattr(snapshot, "events", ()) or ()),
                cash_entries=tuple(getattr(snapshot, "cash_entries", ()) or ()),
            )
            positions = dict(getattr(snapshot, "positions", {}) or {})
            account = getattr(snapshot, "account", None)
            execution_states: tuple[LocalSimExecutionStateV1, ...] = ()
            if context.market_data_source == MinuteDataSource.TDX_REALTIME.value:
                exporter = getattr(context.local_broker, "export_execution_snapshot", None)
                if not callable(exporter):
                    raise DataUnavailableError(
                        "LocalSim realtime broker cannot export durable execution states",
                        context={
                            "reason_code": "LOCALSIM_DURABLE_STATE_EXPORT_UNSUPPORTED",
                            "run_id": run.run_id,
                            "plan_id": execution.execution_plan.plan_id,
                        },
                    )
                raw_snapshot = exporter(handles=tuple(getattr(execution.broker_result, "handles", ()) or ()))
                execution_states = tuple(raw_snapshot.get("execution_states") or ())
                self._validate_execution_states(
                    binding=binding,
                    run=run,
                    execution=execution,
                    orders=orders,
                    states=execution_states,
                )
                self._validate_snapshot_for_progress(run=run, execution=execution, orders=orders)
            else:
                self._validate_snapshot_for_success(
                    run=run, execution=execution, orders=orders, fills=fills, cash_entries=cash_entries
                )

            paper_repository = self._paper_repository_for(binding=binding, run=run, context=context)
            self.replay_pending_projection(
                run_id=run.run_id,
                paper_repository=paper_repository,
                context=context,
                execution=execution,
                observed_positions=positions,
                observed_account=account,
            )
            current_states = {
                state.state_id: state for state in self._runtime_repository.list_local_sim_execution_states(run.run_id)
            }
            if (
                execution_states
                and not fills
                and not events
                and not cash_entries
                and all(
                    state.state_id in current_states
                    and current_states[state.state_id].sequence == state.sequence
                    and current_states[state.state_id].state_hash == state.state_hash
                    for state in execution_states
                )
            ):
                return self._existing_projection_result(
                    run_id=run.run_id,
                    paper_repository=paper_repository,
                    observed_positions=positions,
                    observed_account=account,
                )
            if local_sim_is_first_causal_bar_wait(
                states=execution_states,
                orders=orders,
                fills=fills,
                events=events,
                cash_entries=cash_entries,
            ):
                return self._persist_first_causal_bar_wait(
                    binding=binding,
                    run=run,
                    execution=execution,
                    context=context,
                    paper_repository=paper_repository,
                    orders=orders,
                    states=execution_states,
                    positions=positions,
                    account=account,
                    current_states=current_states,
                )
            snapshot_time = self._snapshot_time(
                fills=fills,
                events=events,
                run=run,
                local_broker=context.local_broker,
                market_data_source=context.market_data_source,
            )
            cash = float(getattr(account, "cash")) if account is not None else self._cash_after(cash_entries, context)
            historical_residual = (
                self._historical_residual_payload(run=run, orders=orders) if not execution_states else None
            )
            contract = derive_local_sim_projection_state_contract(
                execution_states=execution_states,
                historical_residual=historical_residual,
                intraday_status=type(run.status).INTRADAY_RUNNING,
                failed_status=type(run.status).FAILED_TERMINAL,
                succeeded_status=type(run.status).SUCCEEDED,
            )
            historical_residual = contract["historical_residual"]
            active_states = contract["active_states"]
            residual_states = contract["residual_states"]
            failed_terminal_states = contract["failed_terminal_states"]
            nonfilled_terminal_states = contract["nonfilled_terminal_states"]
            terminal = contract["terminal"]
            terminal_failure = contract["terminal_failure"]
            final_event_type = contract["final_event_type"]
            final_event_message = contract["final_event_message"]
            final_paper_status = contract["final_paper_status"]
            final_status = contract["final_status"]
            persistence_status = contract["persistence_status"]
            try:
                marks, mark_records = self._position_marks(
                    positions=positions,
                    context=context,
                    execution=execution,
                    snapshot_time=snapshot_time,
                )
            except (DataUnavailableError, BrokerConnectivityError) as exc:
                if not self._mark_failure_allows_pending(exc):
                    raise
                return self._persist_valuation_pending(
                    binding=binding,
                    run=run,
                    execution=execution,
                    context=context,
                    paper_repository=paper_repository,
                    orders=orders,
                    fills=fills,
                    events=events,
                    cash_entries=cash_entries,
                    states=execution_states,
                    positions=positions,
                    cash=cash,
                    snapshot_time=snapshot_time,
                    current_states=current_states,
                    contract=contract,
                    mark_error=exc,
                )
            market_value = sum(int(position.quantity) * marks[position.symbol] for position in positions.values())
            account_snapshot = AccountSnapshot(
                portfolio_id=str(context.portfolio_id or execution.execution_plan.portfolio_id),
                cash=cash,
                market_value=market_value,
                nav=cash + market_value,
                snapshot_time=snapshot_time,
            )

            final_persistence_payload = {
                "schema_version": "local_sim_persistence_v2",
                "status": persistence_status,
                "paper_v2_run_id": run.run_id,
                "order_count": len(orders),
                "fill_count": len(fills),
                "order_event_count": len(events),
                "cash_ledger_count": len(cash_entries),
                "position_count": len(positions),
                "snapshot_time": snapshot_time.isoformat(),
                "cash": cash,
                "nav": account_snapshot.nav,
                "terminal": terminal,
                "execution_state_count": len(execution_states),
                "active_state_count": len(active_states),
                "residual_state_count": len(residual_states),
                "failed_terminal_state_count": len(failed_terminal_states),
            }
            payload_patch: dict[str, Any] = {
                "local_sim_persistence": {**final_persistence_payload, "status": "PROJECTION_PENDING"},
                "last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED",
            }
            if execution_states:
                payload_patch["local_sim_durable_minute_loop"] = {
                    "schema_version": "local_sim_durable_minute_loop_v1",
                    "state_count": len(execution_states),
                    "active_state_count": len(active_states),
                    "terminal": terminal,
                }
            if historical_residual or residual_states:
                payload_patch["local_sim_capacity_residual_terminalization"] = {
                    "schema_version": "localsim_residual_terminalization_v2",
                    "reason": (
                        "broker_execution_cash_limited_buy_residual"
                        if historical_residual and int(historical_residual.get("schedule_residual_count") or 0) == 0
                        else "historical_execution_schedule_residual"
                        if historical_residual
                        else "execution_schedule_residual_at_close"
                    ),
                    "status": final_status.value,
                    "residual_order_count": int((historical_residual or {}).get("residual_order_count") or 0),
                    "capital_residual_count": int((historical_residual or {}).get("capital_residual_count") or 0),
                    "schedule_residual_count": int((historical_residual or {}).get("schedule_residual_count") or 0),
                    "prepared_intent_count": int((historical_residual or {}).get("prepared_intent_count") or 0),
                    "residual_orders": list((historical_residual or {}).get("residual_orders") or []),
                    "residual_state_ids": [state.state_id for state in residual_states],
                    "terminalized_at": datetime.now(UTC).isoformat(),
                }
            if failed_terminal_states:
                payload_patch["local_sim_terminal_failure_v1"] = {
                    "schema_version": "local_sim_terminal_failure_v1",
                    "reason": "market_data_integrity_failure",
                    "status": final_status.value,
                    "failed_states": [
                        {
                            "state_id": state.state_id,
                            "intent_id": state.intent_id,
                            "symbol": state.symbol,
                            "reason_code": state.terminal_reason,
                            "context": state.waiting_context,
                        }
                        for state in failed_terminal_states
                    ],
                    "terminalized_at": datetime.now(UTC).isoformat(),
                }
            economic_facts = self._economic_facts(
                run=run,
                execution=execution,
                orders=orders,
                fills=fills,
                events=events,
                cash_entries=cash_entries,
                states=execution_states,
                positions=positions,
                marks=mark_records,
                account_snapshot=account_snapshot,
            )
            economic_hash = canonical_json_sha256(economic_facts)
            projection_payload = build_local_sim_projection_payload(
                binding=binding,
                run=run,
                execution=execution,
                context=context,
                positions=positions,
                marks=mark_records,
                account_snapshot=account_snapshot,
                orders=orders,
                fills=fills,
                cash_entries=cash_entries,
                active_states=active_states,
                residual_states=residual_states,
                nonfilled_terminal_states=nonfilled_terminal_states,
                historical_residual=historical_residual,
                terminal=terminal,
                terminal_failure=terminal_failure,
                final_status=final_status,
                final_paper_status=final_paper_status,
                final_event_type=final_event_type,
                final_event_message=final_event_message,
                final_persistence_payload=final_persistence_payload,
                economic_hash=economic_hash,
            )
            expected_versions = {
                state.state_id: (
                    (current_states[state.state_id].sequence, current_states[state.state_id].state_hash)
                    if state.state_id in current_states
                    else None
                )
                for state in execution_states
            }
            commit_result = LocalSimEconomicCoordinator(
                runtime_repository=self._runtime_repository,
                paper_repository=paper_repository,
                ensure_paper_run=lambda: self._ensure_paper_run(
                    repository=paper_repository,
                    run=run,
                    context=context,
                ),
            ).commit(
                LocalSimEconomicCommitRequest(
                    run_id=run.run_id,
                    binding_id=binding.binding_id,
                    trade_date=run.trade_date,
                    plan_id=execution.execution_plan.plan_id,
                    states=execution_states,
                    expected_versions=expected_versions,
                    economic_facts=economic_facts,
                    projection_payload=projection_payload,
                    status=type(run.status).INTRADAY_RUNNING,
                    payload_patch=payload_patch,
                    payload_unset=(
                        "submit_failure",
                        "local_sim_retry_diagnostics",
                        *(("local_sim_synchronous_terminal",) if execution_states else ()),
                    ),
                    orders=orders,
                    fills=fills,
                    events=events,
                    cash_entries=cash_entries,
                    event_message="simulation runtime LocalSim economic facts committed; projection outbox pending",
                    event_context={
                        "source": "simulation_runtime_local_sim",
                        "simulation_run_id": run.run_id,
                        "execution_plan_id": execution.execution_plan.plan_id,
                    },
                )
            )
            receipt = commit_result.receipt
            outbox = commit_result.outbox
            economic_commit_staged = True
            projected_run, performance_payload = self._project_outbox(
                run_id=run.run_id, paper_repository=paper_repository
            )
            if not isinstance(projected_run.run_payload_json.get("local_sim_persistence"), dict):
                raise DataUnavailableError(
                    "LocalSim projected persistence receipt is missing",
                    context={"reason_code": "LOCALSIM_PERSISTENCE_RECEIPT_MISSING", "run_id": run.run_id},
                )
            return LocalSimPersistenceResult(
                payload={
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                    "position_count": len(positions),
                    "cash": cash,
                    "nav": account_snapshot.nav,
                    "terminal": terminal,
                    "active_state_count": len(active_states),
                    "residual_state_count": len(residual_states),
                },
                positions=positions,
                marks=marks,
                cash=cash,
                economic_receipt_id=receipt.receipt_id,
                outbox_id=outbox.outbox_id,
                generation=receipt.generation,
                performance_payload=performance_payload,
            )
        except Exception as exc:
            explicit_failure_context = dict(getattr(exc, "context", None) or {})
            economic_commit_staged = economic_commit_staged or bool(
                explicit_failure_context.get("economic_commit_staged")
            )
            if not isinstance(exc, DataUnavailableError):
                context_payload = {
                    "reason_code": "LOCALSIM_ECONOMIC_PERSISTENCE_UNEXPECTED",
                    "run_id": run.run_id,
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "cause": str(exc),
                    "economic_commit_staged": economic_commit_staged,
                }
                if not economic_commit_staged:
                    context_payload.update(
                        {
                            "broker_called": False,
                            "submitted_intents": 0,
                            "failed_intents": len(execution.execution_plan.intents),
                        }
                    )
                exc = DataUnavailableError(
                    "LocalSim execution side effects could not be persisted durably",
                    context=context_payload,
                )
            elif not economic_commit_staged:
                exc = DataUnavailableError(
                    str(exc),
                    context={
                        **dict(getattr(exc, "context", None) or {}),
                        "economic_commit_staged": False,
                        "broker_called": False,
                        "submitted_intents": 0,
                        "failed_intents": len(execution.execution_plan.intents),
                    },
                )
            failure_context = dict(getattr(exc, "context", None) or {})
            reason_code = str(failure_context.get("reason_code") or "")
            failure_stage = local_sim_persistence_failure_stage(exc)
            if reason_code in {
                "LOCALSIM_PROJECTION_NON_RETRYABLE",
                "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
            }:
                self._runtime_repository.update_simulation_daily_run(
                    run.run_id,
                    status=type(run.status).FAILED_TERMINAL,
                    payload_patch={
                        "last_stage": type(run.status).FAILED_TERMINAL.value,
                        "submit_failure": {
                            "stage": failure_stage,
                            "outer_stage": failure_stage,
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": failure_context,
                        },
                    },
                )
            else:
                self._mark_submit_failure(
                    run=run,
                    stage=failure_stage,
                    exc=exc,
                )
            raise exc


__all__ = [
    "LocalSimPersistenceCoordinator",
    "local_sim_hashed_fact_map",
    "local_sim_is_first_causal_bar_wait",
    "local_sim_persistence_failure_stage",
]
