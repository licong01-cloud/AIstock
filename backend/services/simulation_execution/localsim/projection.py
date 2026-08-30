"""Outbox-only LocalSIM projection transaction owner."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

import psycopg2

from backend.services.simulation_execution.localsim.models import (
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    LocalSimMarketMarkV1,
    LocalSimPersistenceResult,
)
from backend.services.simulation_execution.localsim.economic import (
    local_sim_fact_payload,
    validate_local_sim_duplicate_account_truth,
)
from backend.services.simulation_data.daily_context import SimulationBrokerBackend, canonical_json_sha256
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    SessionLockTimeoutError,
)
from backend.services.trading_core.models import AccountSnapshot, PositionLot, RunStatus


LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES = frozenset({"40001", "40P01", "55P03"})
LOCAL_SIM_PROJECTION_MAX_ATTEMPTS = 3


def local_sim_projection_error_is_retryable(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (SessionLockTimeoutError, psycopg2.OperationalError, psycopg2.InterfaceError)):
            return True
        if str(getattr(current, "pgcode", "") or "") in LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES:
            return True
        current = current.__cause__ or current.__context__
    return False


def derive_local_sim_projection_state_contract(
    *,
    execution_states: tuple[LocalSimExecutionStateV1, ...],
    historical_residual: dict[str, Any] | None,
    intraday_status: Any,
    failed_status: Any,
    succeeded_status: Any,
) -> dict[str, Any]:
    active_states = tuple(state for state in execution_states if not state.is_terminal)
    residual_states = tuple(
        state
        for state in execution_states
        if state.runtime_status == LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL
    )
    failed_terminal_states = tuple(
        state for state in execution_states if state.runtime_status == LocalSimExecutionRuntimeStatus.FAILED_TERMINAL
    )
    nonfilled_terminal_states = tuple(
        state
        for state in execution_states
        if state.is_terminal and state.runtime_status != LocalSimExecutionRuntimeStatus.FILLED
    )
    terminal = not active_states
    terminal_failure = bool(historical_residual or residual_states or nonfilled_terminal_states)
    if active_states:
        final_event_type = "RUN_INTRADAY_PROGRESS"
        final_event_message = "simulation runtime LocalSim minute progress projected"
        final_paper_status = RunStatus.RUNNING
        final_status = intraday_status
        persistence_status = "INTRADAY_PERSISTED"
    elif terminal_failure:
        final_event_type = "RUN_FAILED_TERMINAL" if failed_terminal_states else "RUN_TERMINATED_WITH_RESIDUAL"
        final_event_message = (
            "simulation runtime LocalSim symbol execution failed on durable market-data integrity evidence"
            if failed_terminal_states
            else "simulation runtime LocalSim execution terminalized with explicit residual"
        )
        final_paper_status = RunStatus.FAILED
        final_status = failed_status
        persistence_status = (
            "PERSISTED_WITH_CAPACITY_RESIDUAL"
            if historical_residual and int(historical_residual.get("schedule_residual_count") or 0) == 0
            else "PERSISTED_WITH_TERMINAL_FAILURE"
            if failed_terminal_states
            else "PERSISTED_WITH_RESIDUAL"
        )
    else:
        final_event_type = "RUN_SUCCEEDED"
        final_event_message = "simulation runtime LocalSim terminal execution projected to Paper v2"
        final_paper_status = RunStatus.SUCCEEDED
        final_status = succeeded_status
        persistence_status = "PERSISTED"
    return {
        "historical_residual": historical_residual,
        "active_states": active_states,
        "residual_states": residual_states,
        "failed_terminal_states": failed_terminal_states,
        "nonfilled_terminal_states": nonfilled_terminal_states,
        "terminal": terminal,
        "terminal_failure": terminal_failure,
        "final_event_type": final_event_type,
        "final_event_message": final_event_message,
        "final_paper_status": final_paper_status,
        "final_status": final_status,
        "persistence_status": persistence_status,
    }


def local_sim_projection_paper_error(
    *,
    historical_residual: dict[str, Any] | None,
    nonfilled_terminal_states: tuple[LocalSimExecutionStateV1, ...],
    terminal_failure: bool,
) -> dict[str, Any] | None:
    if not terminal_failure:
        return None
    has_market_data_terminal_failure = any(
        item.runtime_status == LocalSimExecutionRuntimeStatus.FAILED_TERMINAL for item in nonfilled_terminal_states
    )
    if historical_residual:
        if int(historical_residual.get("schedule_residual_count") or 0) == 0:
            code = "LOCALSIM_CAPITAL_RESIDUAL_TERMINAL"
            message = "LocalSim terminalized BUY residual from authoritative broker cash-fit execution"
        else:
            code = "LOCALSIM_HISTORICAL_EXECUTION_RESIDUAL"
            message = "LocalSim historical execution ended with an explicit schedule residual"
    elif has_market_data_terminal_failure:
        code = "LOCALSIM_MARKET_DATA_INTEGRITY_FAILURE"
        message = "LocalSim symbol execution failed on durable market-data integrity evidence"
    else:
        code = "LOCALSIM_EXECUTION_TERMINATED_WITH_RESIDUAL"
        message = "LocalSim closed with explicit unfilled execution residual"
    return {
        "code": code,
        "message": message,
        "context": {
            "local_sim_historical_residual": historical_residual,
            "states": [item.model_dump(mode="json") for item in nonfilled_terminal_states],
        },
    }


def build_local_sim_projection_payload(
    *,
    binding: Any,
    run: Any,
    execution: Any,
    context: Any,
    positions: dict[str, Any],
    marks: dict[str, Any],
    account_snapshot: Any,
    orders: tuple[Any, ...],
    fills: tuple[Any, ...],
    cash_entries: tuple[Any, ...],
    active_states: tuple[LocalSimExecutionStateV1, ...],
    residual_states: tuple[LocalSimExecutionStateV1, ...],
    nonfilled_terminal_states: tuple[LocalSimExecutionStateV1, ...],
    historical_residual: dict[str, Any] | None,
    terminal: bool,
    terminal_failure: bool,
    final_status: Any,
    final_paper_status: RunStatus,
    final_event_type: str,
    final_event_message: str,
    final_persistence_payload: dict[str, Any],
    economic_hash: str,
) -> dict[str, Any]:
    paper_error = local_sim_projection_paper_error(
        historical_residual=historical_residual,
        nonfilled_terminal_states=nonfilled_terminal_states,
        terminal_failure=terminal_failure,
    )
    return {
        "schema_version": "local_sim_projection_payload_v1",
        "run_id": run.run_id,
        "binding_id": binding.binding_id,
        "strategy_id": binding.strategy_id,
        "plan_id": execution.execution_plan.plan_id,
        "trade_date": run.trade_date.isoformat(),
        "portfolio_id": account_snapshot.portfolio_id,
        "initial_capital": float(binding.capital_allocation),
        "realized_pnl": float(context.realized_pnl),
        "positions": [item.model_dump(mode="json") for _, item in sorted(positions.items())],
        "marks": [item.model_dump(mode="json") for _, item in sorted(marks.items())],
        "account_snapshot": account_snapshot.model_dump(mode="json"),
        "snapshot_metadata": {
            "source": "simulation_runtime_local_sim",
            "simulation_run_id": run.run_id,
            "execution_plan_id": execution.execution_plan.plan_id,
            "order_count": len(orders),
            "fill_count": len(fills),
            "cash_ledger_count": len(cash_entries),
            "position_count": len(positions),
            "terminal": terminal,
        },
        "final_simulation_status": final_status.value,
        "final_paper_status": final_paper_status.value,
        "final_event_type": final_event_type,
        "final_event_message": final_event_message,
        "final_event_context": {
            "source": "simulation_runtime_local_sim",
            "simulation_run_id": run.run_id,
            "execution_plan_id": execution.execution_plan.plan_id,
            "order_count": len(orders),
            "fill_count": len(fills),
            "cash_ledger_count": len(cash_entries),
            "position_count": len(positions),
            "snapshot_time": account_snapshot.snapshot_time.isoformat(),
            "local_sim_historical_residual": historical_residual,
            "terminal": terminal,
            "active_state_ids": [item.state_id for item in active_states],
            "residual_state_ids": [item.state_id for item in residual_states],
        },
        "paper_error": paper_error,
        "local_sim_persistence": final_persistence_payload,
        "economic_hash": economic_hash,
        "tca_generation": {
            "schema_version": "local_sim_tca_generation_v1",
            "execution_plan_id": execution.execution_plan.plan_id,
            "execution_plan_hash": execution.execution_plan.plan_hash,
            "economic_hash": economic_hash,
        },
    }


class LocalSimRuntimeProjectionRepository(Protocol):
    def local_sim_economic_transaction_scope(self) -> Any: ...

    def stage_local_sim_projection_commit(self, **kwargs: Any) -> LocalSimProjectionReceiptV1: ...


class LocalSimPaperProjectionRepository(Protocol):
    def local_sim_economic_transaction(self, run_id: str) -> Any: ...


@dataclass(frozen=True)
class LocalSimProjectionCommitRequest:
    run_id: str
    outbox_id: str
    generation: int
    final_status: Any
    projection_result: dict[str, Any]
    payload_patch: dict[str, Any]
    payload_unset: tuple[str, ...]
    apply_paper_projection: Callable[[], None]
    readback: Callable[[LocalSimProjectionReceiptV1], Any]
    on_staged: Callable[[], None] | None = None


@dataclass(frozen=True)
class LocalSimProjectionCommitResult:
    receipt: LocalSimProjectionReceiptV1
    projected: Any


class LocalSimProjector:
    """Consume one durable outbox generation without broker or signal access."""

    def __init__(
        self,
        *,
        runtime_repository: LocalSimRuntimeProjectionRepository,
        paper_repository: LocalSimPaperProjectionRepository | None = None,
        performance_service: Any | None = None,
    ) -> None:
        self._runtime_repository = runtime_repository
        self._paper_repository = paper_repository
        self._performance_service = performance_service

    def commit(self, request: LocalSimProjectionCommitRequest) -> LocalSimProjectionCommitResult:
        if self._paper_repository is None:
            raise RuntimeError("LocalSim projection commit requires a Paper projection repository")
        with self._runtime_repository.local_sim_economic_transaction_scope():
            with self._paper_repository.local_sim_economic_transaction(request.run_id) as connection:
                request.apply_paper_projection()
                receipt = self._runtime_repository.stage_local_sim_projection_commit(
                    connection=connection,
                    run_id=request.run_id,
                    outbox_id=request.outbox_id,
                    generation=request.generation,
                    final_status=request.final_status,
                    projection_result=request.projection_result,
                    payload_patch=request.payload_patch,
                    payload_unset=request.payload_unset,
                )
        if request.on_staged is not None:
            request.on_staged()
        projected = request.readback(receipt)
        return LocalSimProjectionCommitResult(receipt=receipt, projected=projected)

    def replay_pending(
        self,
        *,
        run_id: str,
        project_valuation_pending: Callable[[Any, LocalSimProjectionOutboxV1], None] | None,
        project_outbox: Callable[[str], None],
    ) -> None:
        """Validate and dispatch exactly one committed outbox generation."""

        run = self._runtime_repository.get_simulation_daily_run(run_id)
        raw = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw is None:
            return
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw)
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim projection outbox cannot be recovered",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID", "run_id": run_id},
            ) from exc
        if outbox.projection_payload.get("schema_version") == "local_sim_valuation_pending_projection_payload_v1":
            if project_valuation_pending is not None:
                project_valuation_pending(run, outbox)
            return
        if outbox.status in {
            LocalSimProjectionOutboxStatus.PENDING,
            LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE,
        } or run.run_payload_json.get("local_sim_projection_readback_failure"):
            project_outbox(run_id)

    def project_outbox(
        self, *, run_id: str, paper_repository: Any
    ) -> tuple[Any, dict[str, Any]]:
        if self._performance_service is None:
            raise RuntimeError("LocalSim outbox projection requires a performance service")
        run = self._runtime_repository.get_simulation_daily_run(run_id)
        terminal_failure = run.run_payload_json.get("local_sim_projection_terminal_failure")
        if isinstance(terminal_failure, dict):
            terminal_error = dict(terminal_failure.get("error") or {})
            raise DataUnavailableError(
                "LocalSim projection is terminal and cannot be retried automatically",
                context={
                    "reason_code": str(terminal_error.get("reason_code") or "LOCALSIM_PROJECTION_NON_RETRYABLE"),
                    "run_id": run_id,
                    "outbox_id": terminal_failure.get("outbox_id"),
                    "attempt_count": terminal_failure.get("attempt_count"),
                    "cause": terminal_error.get("message"),
                },
            )
        raw = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw is None:
            raise DataUnavailableError(
                "LocalSim economic commit has no projection outbox",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_MISSING", "run_id": run_id},
            )
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw)
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim projection outbox cannot be read",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID", "run_id": run_id},
            ) from exc
        if outbox.projection_payload.get("schema_version") == "local_sim_waiting_projection_payload_v1":
            return self.project_first_causal_bar_wait_outbox(
                run=run,
                outbox=outbox,
                paper_repository=paper_repository,
            )
        performance = run.run_payload_json.get("strategy_performance")
        if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
            if (
                not isinstance(performance, dict)
                or int(performance.get("local_sim_generation") or 0) != outbox.generation
            ):
                raise DataUnavailableError(
                    "LocalSim projected outbox has no matching performance generation",
                    context={"reason_code": "LOCALSIM_PERFORMANCE_GENERATION_CONFLICT", "run_id": run_id},
                )
            readback_failure = run.run_payload_json.get("local_sim_projection_readback_failure")
            if readback_failure:
                if not isinstance(readback_failure, dict):
                    raise DataUnavailableError(
                        "LocalSim projection readback failure receipt is invalid",
                        context={
                            "reason_code": "LOCALSIM_PROJECTION_READBACK_SCHEMA_INVALID",
                            "run_id": run_id,
                        },
                    )
                previous_attempts = int(readback_failure.get("attempt_count") or 0)
                if previous_attempts >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                    raise DataUnavailableError(
                        "LocalSim projection readback exhausted its automatic retry budget",
                        context={
                            "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
                            "run_id": run_id,
                            "outbox_id": outbox.outbox_id,
                            "attempt_count": previous_attempts,
                        },
                    )
                raw_receipts = run.run_payload_json.get("local_sim_projection_receipts_v1")
                if not isinstance(raw_receipts, dict):
                    raise DataUnavailableError(
                        "LocalSim projection readback recovery has no receipt map",
                        context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run_id},
                    )
                try:
                    matching_receipts = [
                        LocalSimProjectionReceiptV1.model_validate(item)
                        for item in raw_receipts.values()
                        if isinstance(item, dict) and item.get("outbox_id") == outbox.outbox_id
                    ]
                except Exception as exc:
                    raise DataUnavailableError(
                        "LocalSim projection readback recovery receipt schema is invalid",
                        context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_SCHEMA_INVALID", "run_id": run_id},
                    ) from exc
                if len(matching_receipts) != 1:
                    raise DataUnavailableError(
                        "LocalSim projection readback recovery requires one exact matching receipt",
                        context={
                            "reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING",
                            "run_id": run_id,
                            "matching_receipt_count": len(matching_receipts),
                        },
                    )
                receipt = matching_receipts[0]
                payload = outbox.projection_payload
                snapshot = AccountSnapshot.model_validate(payload.get("account_snapshot"))
                trade_date_value = date.fromisoformat(str(payload.get("trade_date")))
                final_status = type(run.status)(str(payload.get("final_simulation_status")))
                try:
                    self._runtime_repository.readback_local_sim_projection_commit(run_id=run_id, receipt=receipt)
                    paper_repository.readback_local_sim_projection(
                        run_id=run_id,
                        portfolio_id=snapshot.portfolio_id,
                        trade_date=trade_date_value,
                        outbox_id=outbox.outbox_id,
                        generation=outbox.generation,
                        expected_position_count=len(payload.get("positions") or []),
                    )
                except Exception as exc:
                    attempt_count = previous_attempts + 1
                    error = {
                        "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRYABLE",
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "outbox_id": outbox.outbox_id,
                        "generation": outbox.generation,
                        "attempt_count": attempt_count,
                    }
                    self._runtime_repository.mark_local_sim_projection_readback_retryable(
                        run_id=run_id,
                        outbox_id=outbox.outbox_id,
                        error=error,
                    )
                    if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                        self._runtime_repository.update_simulation_daily_run(
                            run_id,
                            status=type(run.status).FAILED_TERMINAL,
                            payload_patch={
                                "local_sim_projection_readback_terminal_failure": error,
                                "last_stage": type(run.status).FAILED_TERMINAL.value,
                            },
                        )
                    reason_code = (
                        "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
                        if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS
                        else "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
                    )
                    raise DataUnavailableError(
                        "LocalSim projection readback must be retried",
                        context={
                            "reason_code": reason_code,
                            "run_id": run_id,
                            "outbox_id": outbox.outbox_id,
                            "attempt_count": attempt_count,
                            "cause": str(exc),
                        },
                    ) from exc
                run = self._runtime_repository.clear_local_sim_projection_readback_failure(
                    run_id=run_id, outbox_id=outbox.outbox_id, final_status=final_status
                )
            return run, performance

        payload = outbox.projection_payload
        try:
            raw_positions = payload.get("positions") or []
            raw_marks = payload.get("marks") or []
            positions = {
                item.symbol: item for item in (PositionLot.model_validate(raw_item) for raw_item in raw_positions)
            }
            mark_records = {
                item.symbol: item for item in (LocalSimMarketMarkV1.model_validate(raw_item) for raw_item in raw_marks)
            }
            if len(positions) != len(raw_positions) or len(mark_records) != len(raw_marks):
                raise ValueError("LocalSim projection positions or marks are duplicated")
            account_snapshot = AccountSnapshot.model_validate(payload.get("account_snapshot"))
            final_status = type(run.status)(str(payload.get("final_simulation_status")))
            final_paper_status = RunStatus(str(payload.get("final_paper_status")))
            projection_trade_date = date.fromisoformat(str(payload.get("trade_date")))
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim projection payload failed schema validation",
                context={"reason_code": "LOCALSIM_PROJECTION_PAYLOAD_SCHEMA_INVALID", "run_id": run_id},
            ) from exc
        if payload.get("economic_hash") != outbox.economic_hash:
            raise DataUnavailableError(
                "LocalSim projection payload economic hash does not match outbox",
                context={"reason_code": "LOCALSIM_PROJECTION_ECONOMIC_HASH_CONFLICT", "run_id": run_id},
            )
        strategy_id = str(payload.get("strategy_id") or "").strip()
        if not strategy_id or "initial_capital" not in payload or "realized_pnl" not in payload:
            raise DataUnavailableError(
                "LocalSim projection payload is missing performance identity",
                context={"reason_code": "LOCALSIM_PROJECTION_PAYLOAD_SCHEMA_INVALID", "run_id": run_id},
            )
        marks = {symbol: mark.price for symbol, mark in mark_records.items()}
        performance = self._performance_service.project_strategy(
            strategy_id=strategy_id,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            initial_capital=float(payload["initial_capital"]),
            cash=float(account_snapshot.cash),
            frozen_cash=0.0,
            realized_pnl=float(payload["realized_pnl"]),
            positions=positions,
            marks=marks,
        ).to_dict()
        performance.update(
            {
                "local_sim_generation": outbox.generation,
                "local_sim_outbox_id": outbox.outbox_id,
                "local_sim_economic_hash": outbox.economic_hash,
                "tca_generation": {**dict(payload.get("tca_generation") or {}), "generation": outbox.generation},
            }
        )
        snapshot_metadata = {
            **dict(payload.get("snapshot_metadata") or {}),
            "local_sim_generation": outbox.generation,
            "local_sim_outbox_id": outbox.outbox_id,
            "local_sim_economic_hash": outbox.economic_hash,
            "projection_payload_hash": outbox.projection_payload_hash,
        }
        projection_result = {
            "schema_version": "local_sim_projection_result_v1",
            "outbox_id": outbox.outbox_id,
            "generation": outbox.generation,
            "economic_hash": outbox.economic_hash,
            "position_hashes": {
                symbol: canonical_json_sha256(local_sim_fact_payload(item, fact_type="position"))
                for symbol, item in sorted(positions.items())
            },
            "mark_hashes": {symbol: item.mark_hash for symbol, item in sorted(mark_records.items())},
            "account_snapshot_hash": canonical_json_sha256(
                local_sim_fact_payload(account_snapshot, fact_type="account_snapshot")
            ),
            "performance_hash": canonical_json_sha256(performance),
        }
        projection_committed = False

        def apply_terminal_projection() -> None:
            paper_repository.save_positions(
                run_id=run_id,
                trade_date=projection_trade_date,
                positions=list(positions.values()),
                prices=marks,
            )
            paper_repository.save_daily_snapshot(
                run_id=run_id,
                trade_date=projection_trade_date,
                snapshot=account_snapshot,
                metadata=snapshot_metadata,
            )
            paper_repository.save_run_event(
                run_id=run_id,
                event_type=str(payload.get("final_event_type")),
                message=str(payload.get("final_event_message")),
                context={
                    **dict(payload.get("final_event_context") or {}),
                    "local_sim_generation": outbox.generation,
                    "local_sim_outbox_id": outbox.outbox_id,
                    "local_sim_economic_hash": outbox.economic_hash,
                },
            )
            paper_repository.update_run_status(
                paper_repository.get_run(run_id),
                final_paper_status,
                error=payload.get("paper_error"),
            )

        def mark_terminal_projection_staged() -> None:
            nonlocal projection_committed
            projection_committed = True

        def readback_terminal_projection(receipt: LocalSimProjectionReceiptV1) -> Any:
            projected_run = self._runtime_repository.readback_local_sim_projection_commit(run_id=run_id, receipt=receipt)
            paper_repository.readback_local_sim_projection(
                run_id=run_id,
                portfolio_id=account_snapshot.portfolio_id,
                trade_date=projection_trade_date,
                outbox_id=outbox.outbox_id,
                generation=outbox.generation,
                expected_position_count=len(positions),
            )
            return projected_run

        try:
            result = self.commit(
                LocalSimProjectionCommitRequest(
                    run_id=run_id,
                    outbox_id=outbox.outbox_id,
                    generation=outbox.generation,
                    final_status=final_status,
                    projection_result=projection_result,
                    payload_patch={
                        "strategy_performance": performance,
                        "performance_projection": performance,
                        "local_sim_persistence": dict(payload.get("local_sim_persistence") or {}),
                        "local_sim_projection_generation": {
                            "schema_version": "local_sim_projection_generation_v1",
                            "generation": outbox.generation,
                            "outbox_id": outbox.outbox_id,
                            "economic_hash": outbox.economic_hash,
                        },
                        "last_stage": final_status.value,
                    },
                    payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
                    apply_paper_projection=apply_terminal_projection,
                    readback=readback_terminal_projection,
                    on_staged=mark_terminal_projection_staged,
                )
            )
            return result.projected, performance
        except Exception as exc:
            previous_readback_failure = run.run_payload_json.get("local_sim_projection_readback_failure")
            previous_readback_attempts = (
                int(previous_readback_failure.get("attempt_count") or 0)
                if isinstance(previous_readback_failure, dict)
                else 0
            )
            attempt_count = previous_readback_attempts + 1 if projection_committed else outbox.attempt_count + 1
            retryable = local_sim_projection_error_is_retryable(exc)
            if projection_committed:
                reason_code = "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
            elif not retryable:
                reason_code = "LOCALSIM_PROJECTION_NON_RETRYABLE"
            elif attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                reason_code = "LOCALSIM_PROJECTION_RETRY_EXHAUSTED"
            else:
                reason_code = "LOCALSIM_PROJECTION_RETRYABLE"
            error = {
                "reason_code": reason_code,
                "type": type(exc).__name__,
                "message": str(exc),
                "outbox_id": outbox.outbox_id,
                "generation": outbox.generation,
                "attempt_count": attempt_count,
                "max_attempts": LOCAL_SIM_PROJECTION_MAX_ATTEMPTS,
            }
            try:
                if projection_committed:
                    self._runtime_repository.mark_local_sim_projection_readback_retryable(
                        run_id=run_id, outbox_id=outbox.outbox_id, error=error
                    )
                    if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                        self._runtime_repository.update_simulation_daily_run(
                            run_id,
                            status=type(run.status).FAILED_TERMINAL,
                            payload_patch={
                                "local_sim_projection_readback_terminal_failure": error,
                                "last_stage": type(run.status).FAILED_TERMINAL.value,
                            },
                        )
                elif reason_code in {
                    "LOCALSIM_PROJECTION_NON_RETRYABLE",
                    "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                }:
                    self._runtime_repository.mark_local_sim_projection_terminal(
                        run_id=run_id,
                        outbox_id=outbox.outbox_id,
                        error=error,
                    )
                else:
                    self._runtime_repository.mark_local_sim_projection_retryable(
                        run_id=run_id, outbox_id=outbox.outbox_id, error=error
                    )
            except Exception as persistence_exc:
                raise DataUnavailableError(
                    "LocalSim projection failed and retry state could not be persisted",
                    context={
                        "reason_code": "LOCALSIM_PROJECTION_FAILURE_PERSISTENCE_FAILED",
                        "run_id": run_id,
                        "outbox_id": outbox.outbox_id,
                        "projection_error": str(exc),
                        "persistence_error": str(persistence_exc),
                    },
                ) from persistence_exc
            if projection_committed and attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                reason_code = "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
            message = (
                "LocalSim economic facts committed but projection cannot be retried"
                if reason_code == "LOCALSIM_PROJECTION_NON_RETRYABLE"
                else "LocalSim economic facts committed but projection retry budget is exhausted"
                if reason_code
                in {
                    "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                    "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
                }
                else "LocalSim economic facts committed but projection must be retried"
            )
            raise DataUnavailableError(
                message,
                context={
                    "reason_code": reason_code,
                    "run_id": run_id,
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                    "attempt_count": attempt_count,
                    "max_attempts": LOCAL_SIM_PROJECTION_MAX_ATTEMPTS,
                    "cause": str(exc),
                },
            ) from exc


    def project_first_causal_bar_wait_outbox(
        self,
        *,
        run: Any,
        outbox: LocalSimProjectionOutboxV1,
        paper_repository: Any,
    ) -> tuple[Any, dict[str, Any]]:
        payload = outbox.projection_payload
        required_keys = {
            "schema_version",
            "projection_kind",
            "run_id",
            "binding_id",
            "strategy_id",
            "plan_id",
            "trade_date",
            "portfolio_id",
            "observed_until",
            "cash_reference",
            "positions",
            "position_hashes",
            "order_hashes",
            "state_hashes",
            "final_simulation_status",
            "final_paper_status",
            "final_event_type",
            "final_event_message",
            "local_sim_persistence",
            "economic_hash",
        }
        if set(payload) != required_keys:
            raise DataUnavailableError(
                "LocalSim first-bar wait projection payload is not exact",
                context={
                    "reason_code": "LOCALSIM_WAITING_PROJECTION_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "missing_fields": sorted(required_keys - set(payload)),
                    "unknown_fields": sorted(set(payload) - required_keys),
                },
            )
        if (
            payload.get("projection_kind") != "FIRST_CAUSAL_BAR_WAIT"
            or payload.get("run_id") != run.run_id
            or payload.get("binding_id") != run.binding_id
            or payload.get("plan_id") != run.execution_plan_id
            or payload.get("trade_date") != run.trade_date.isoformat()
            or payload.get("economic_hash") != outbox.economic_hash
            or payload.get("final_simulation_status") != type(run.status).INTRADAY_RUNNING.value
            or payload.get("final_paper_status") != RunStatus.RUNNING.value
            or payload.get("final_event_type") != "RUN_INTRADAY_WAITING_FOR_CAUSAL_BAR"
        ):
            raise DataUnavailableError(
                "LocalSim first-bar wait projection identity conflicts with the durable run",
                context={
                    "reason_code": "LOCALSIM_WAITING_PROJECTION_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )
        try:
            observed_until = datetime.fromisoformat(str(payload["observed_until"]))
            cash = float(payload["cash_reference"])
            if isinstance(payload["cash_reference"], bool) or not math.isfinite(cash) or cash < 0:
                raise ValueError("cash_reference is invalid")
            positions = {
                item.symbol: item for item in (PositionLot.model_validate(raw) for raw in payload["positions"])
            }
            if len(positions) != len(payload["positions"]):
                raise ValueError("position symbols are duplicated")
            position_hashes = dict(payload["position_hashes"])
            order_hashes = dict(payload["order_hashes"])
            state_hashes = dict(payload["state_hashes"])
            persistence = dict(payload["local_sim_persistence"])
            persistence_counts: dict[str, int] = {}
            for field_name in (
                "order_count",
                "execution_state_count",
                "active_state_count",
                "position_count",
            ):
                raw_count = persistence.get(field_name)
                if isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count < 0:
                    raise ValueError(f"{field_name} must be a non-negative integer")
                persistence_counts[field_name] = raw_count
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim first-bar wait projection fields are invalid",
                context={"reason_code": "LOCALSIM_WAITING_PROJECTION_SCHEMA_INVALID", "run_id": run.run_id},
            ) from exc
        actual_position_hashes = {
            symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
            for symbol, position in sorted(positions.items())
        }
        if (
            observed_until.date() != run.trade_date
            or actual_position_hashes != position_hashes
            or not order_hashes
            or not state_hashes
            or persistence.get("status") != "INTRADAY_WAITING_FOR_CAUSAL_BAR"
            or persistence.get("terminal") is not False
            or persistence_counts["order_count"] != len(order_hashes)
            or persistence_counts["execution_state_count"] != len(state_hashes)
            or persistence_counts["active_state_count"] != len(state_hashes)
            or persistence_counts["position_count"] != len(positions)
            or persistence.get("valuation_status") != "WAITING_FOR_FIRST_CAUSAL_MARK"
            or persistence.get("snapshot_time") is not None
            or persistence.get("nav") is not None
        ):
            raise DataUnavailableError(
                "LocalSim first-bar wait projection facts do not close",
                context={
                    "reason_code": "LOCALSIM_WAITING_PROJECTION_FACT_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )

        def readback_projection(receipt: LocalSimProjectionReceiptV1) -> None:
            self._runtime_repository.readback_local_sim_projection_commit(run_id=run.run_id, receipt=receipt)
            paper_run = paper_repository.get_run(run.run_id)
            if paper_run.status != RunStatus.RUNNING:
                raise InvalidStateTransitionError(
                    "LocalSim wait projection Paper run is not running",
                    context={"reason_code": "LOCALSIM_WAITING_PROJECTION_READBACK_FAILED", "run_id": run.run_id},
                )
            persisted_orders = {
                str(order.order_id): canonical_json_sha256(local_sim_fact_payload(order, fact_type="order"))
                for order in paper_repository.list_orders_for_run(run.run_id)
            }
            if persisted_orders != order_hashes:
                raise InvalidStateTransitionError(
                    "LocalSim wait projection order readback does not match the committed generation",
                    context={"reason_code": "LOCALSIM_WAITING_PROJECTION_READBACK_FAILED", "run_id": run.run_id},
                )

        def projection_receipt(current_run: Any) -> LocalSimProjectionReceiptV1:
            raw_receipts = current_run.run_payload_json.get("local_sim_projection_receipts_v1")
            if not isinstance(raw_receipts, dict):
                raise DataUnavailableError(
                    "LocalSim wait projection receipt map is missing",
                    context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run.run_id},
                )
            matches = [
                LocalSimProjectionReceiptV1.model_validate(raw)
                for raw in raw_receipts.values()
                if isinstance(raw, dict) and raw.get("outbox_id") == outbox.outbox_id
            ]
            if len(matches) != 1:
                raise DataUnavailableError(
                    "LocalSim wait projection receipt identity is missing or duplicated",
                    context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run.run_id},
                )
            return matches[0]

        performance_payload: dict[str, Any] = {}
        if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
            if "strategy_performance" in run.run_payload_json or "performance_projection" in run.run_payload_json:
                raise DataUnavailableError(
                    "LocalSim first-bar wait projection retained an unmarked performance projection",
                    context={
                        "reason_code": "LOCALSIM_WAITING_PROJECTION_PERFORMANCE_CONFLICT",
                        "run_id": run.run_id,
                    },
                )
            receipt = projection_receipt(run)
            try:
                readback_projection(receipt)
            except Exception as exc:
                previous = run.run_payload_json.get("local_sim_projection_readback_failure")
                previous_attempts = int(previous.get("attempt_count") or 0) if isinstance(previous, dict) else 0
                attempt_count = previous_attempts + 1
                error = {
                    "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRYABLE",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                    "attempt_count": attempt_count,
                }
                self._runtime_repository.mark_local_sim_projection_readback_retryable(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    error=error,
                )
                if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                    self._runtime_repository.update_simulation_daily_run(
                        run.run_id,
                        status=type(run.status).FAILED_TERMINAL,
                        payload_patch={
                            "local_sim_projection_readback_terminal_failure": error,
                            "last_stage": type(run.status).FAILED_TERMINAL.value,
                        },
                    )
                raise DataUnavailableError(
                    "LocalSim wait projection readback must be retried",
                    context={
                        "reason_code": (
                            "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
                            if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS
                            else "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
                        ),
                        "run_id": run.run_id,
                        "outbox_id": outbox.outbox_id,
                        "attempt_count": attempt_count,
                    },
                ) from exc
            if run.run_payload_json.get("local_sim_projection_readback_failure"):
                run = self._runtime_repository.clear_local_sim_projection_readback_failure(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    final_status=type(run.status).INTRADAY_RUNNING,
                )
            return run, performance_payload

        projection_result = {
            "schema_version": "local_sim_waiting_projection_result_v1",
            "outbox_id": outbox.outbox_id,
            "generation": outbox.generation,
            "economic_hash": outbox.economic_hash,
            "order_hashes": order_hashes,
            "state_hashes": state_hashes,
            "position_hashes": position_hashes,
            "mark_hashes": {},
            "account_snapshot_hash": None,
            "performance_hash": None,
        }
        projection_committed = False

        def apply_wait_projection() -> None:
            paper_repository.save_run_event(
                run_id=run.run_id,
                event_type=str(payload["final_event_type"]),
                message=str(payload["final_event_message"]),
                context={
                    "source": "simulation_runtime_local_sim",
                    "simulation_run_id": run.run_id,
                    "execution_plan_id": run.execution_plan_id,
                    "observed_until": observed_until.isoformat(),
                    "active_state_ids": sorted(state_hashes),
                    "local_sim_generation": outbox.generation,
                    "local_sim_outbox_id": outbox.outbox_id,
                    "local_sim_economic_hash": outbox.economic_hash,
                },
            )
            paper_repository.update_run_status(
                paper_repository.get_run(run.run_id),
                RunStatus.RUNNING,
                error=None,
            )

        def mark_wait_projection_staged() -> None:
            nonlocal projection_committed
            projection_committed = True

        def readback_wait_projection(receipt: LocalSimProjectionReceiptV1) -> Any:
            readback_projection(receipt)
            return self._runtime_repository.get_simulation_daily_run(run.run_id)

        try:
            result = self.commit(
                LocalSimProjectionCommitRequest(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    generation=outbox.generation,
                    final_status=type(run.status).INTRADAY_RUNNING,
                    projection_result=projection_result,
                    payload_patch={
                        "local_sim_persistence": persistence,
                        "local_sim_projection_generation": {
                            "schema_version": "local_sim_projection_generation_v1",
                            "generation": outbox.generation,
                            "outbox_id": outbox.outbox_id,
                            "economic_hash": outbox.economic_hash,
                        },
                        "last_stage": type(run.status).INTRADAY_RUNNING.value,
                    },
                    payload_unset=(
                        "submit_failure",
                        "local_sim_retry_diagnostics",
                        "strategy_performance",
                        "performance_projection",
                    ),
                    apply_paper_projection=apply_wait_projection,
                    readback=readback_wait_projection,
                    on_staged=mark_wait_projection_staged,
                )
            )
            return result.projected, performance_payload
        except Exception as exc:
            attempt_count = outbox.attempt_count + 1
            retryable = local_sim_projection_error_is_retryable(exc)
            reason_code = (
                "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
                if projection_committed
                else "LOCALSIM_PROJECTION_NON_RETRYABLE"
                if not retryable
                else "LOCALSIM_PROJECTION_RETRY_EXHAUSTED"
                if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS
                else "LOCALSIM_PROJECTION_RETRYABLE"
            )
            error = {
                "reason_code": reason_code,
                "type": type(exc).__name__,
                "message": str(exc),
                "outbox_id": outbox.outbox_id,
                "generation": outbox.generation,
                "attempt_count": attempt_count,
                "max_attempts": LOCAL_SIM_PROJECTION_MAX_ATTEMPTS,
            }
            if projection_committed:
                self._runtime_repository.mark_local_sim_projection_readback_retryable(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    error=error,
                )
            elif reason_code == "LOCALSIM_PROJECTION_RETRYABLE":
                self._runtime_repository.mark_local_sim_projection_retryable(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    error=error,
                )
            else:
                self._runtime_repository.mark_local_sim_projection_terminal(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    error=error,
                )
            raise DataUnavailableError(
                "LocalSim first causal-bar wait projection failed",
                context={
                    "reason_code": reason_code,
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "attempt_count": attempt_count,
                },
            ) from exc


    def existing_projection_result(
        self,
        *,
        run_id: str,
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> LocalSimPersistenceResult:
        run = self._runtime_repository.get_simulation_daily_run(run_id)
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(
                run.run_payload_json.get("local_sim_projection_outbox_v1")
            )
            payload = outbox.projection_payload
            if payload.get("schema_version") == "local_sim_valuation_pending_projection_payload_v1":
                return self._existing_valuation_projection_result(
                    run=run,
                    outbox=outbox,
                    payload=payload,
                    observed_positions=observed_positions,
                    observed_account=observed_account,
                )
            if outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
                raise ValueError("projection outbox is not projected")
            if payload.get("schema_version") == "local_sim_waiting_projection_payload_v1":
                return self._existing_waiting_projection_result(
                    run=run,
                    outbox=outbox,
                    payload=payload,
                    observed_positions=observed_positions,
                    observed_account=observed_account,
                )
            positions = {
                item.symbol: item
                for item in (PositionLot.model_validate(raw) for raw in payload.get("positions") or [])
            }
            marks = {
                item.symbol: item.price
                for item in (LocalSimMarketMarkV1.model_validate(raw) for raw in payload.get("marks") or [])
            }
            if len(positions) != len(payload.get("positions") or []):
                raise ValueError("projection positions are duplicated")
            if len(marks) != len(payload.get("marks") or []):
                raise ValueError("projection marks are duplicated")
            account = AccountSnapshot.model_validate(payload.get("account_snapshot"))
            performance = run.run_payload_json["strategy_performance"]
            persistence = run.run_payload_json["local_sim_persistence"]
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim duplicate event cannot rebuild the projected generation",
                context={"reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED", "run_id": run_id},
            ) from exc
        if not isinstance(performance, dict) or not isinstance(persistence, dict):
            raise DataUnavailableError(
                "LocalSim duplicate event is missing projected receipts",
                context={"reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED", "run_id": run_id},
            )
        validate_local_sim_duplicate_account_truth(
            run_id=run_id,
            projected_positions=positions,
            projected_cash=float(account.cash),
            observed_positions=observed_positions,
            observed_account=observed_account,
        )
        required_counts = (
            "order_count",
            "fill_count",
            "cash_ledger_count",
            "position_count",
            "active_state_count",
            "residual_state_count",
            "terminal",
        )
        missing = [key for key in required_counts if key not in persistence]
        if missing:
            raise DataUnavailableError(
                "LocalSim duplicate projection receipt is incomplete",
                context={
                    "reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED",
                    "run_id": run_id,
                    "missing_fields": missing,
                },
            )
        return LocalSimPersistenceResult(
            payload={
                "order_count": int(persistence["order_count"]),
                "fill_count": int(persistence["fill_count"]),
                "cash_ledger_count": int(persistence["cash_ledger_count"]),
                "position_count": int(persistence["position_count"]),
                "cash": float(account.cash),
                "nav": float(account.nav),
                "terminal": bool(persistence["terminal"]),
                "active_state_count": int(persistence["active_state_count"]),
                "residual_state_count": int(persistence["residual_state_count"]),
            },
            positions=positions,
            marks=marks,
            cash=float(account.cash),
            economic_receipt_id=outbox.receipt_id,
            outbox_id=outbox.outbox_id,
            generation=outbox.generation,
            performance_payload=performance,
        )

    @staticmethod
    def _existing_waiting_projection_result(
        *,
        run: Any,
        outbox: LocalSimProjectionOutboxV1,
        payload: dict[str, Any],
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> LocalSimPersistenceResult:
        persistence = run.run_payload_json["local_sim_persistence"]
        if (
            not isinstance(persistence, dict)
            or persistence.get("status") != "INTRADAY_WAITING_FOR_CAUSAL_BAR"
            or persistence.get("terminal") is not False
            or persistence.get("valuation_status") != "WAITING_FOR_FIRST_CAUSAL_MARK"
            or "strategy_performance" in run.run_payload_json
            or "performance_projection" in run.run_payload_json
        ):
            raise ValueError("waiting projection persistence is invalid")
        positions = {
            item.symbol: item
            for item in (PositionLot.model_validate(raw) for raw in payload.get("positions") or [])
        }
        if len(positions) != len(payload.get("positions") or []):
            raise ValueError("waiting projection positions are duplicated")
        cash = float(payload["cash_reference"])
        if isinstance(payload["cash_reference"], bool) or not math.isfinite(cash) or cash < 0:
            raise ValueError("waiting projection cash is invalid")
        validate_local_sim_duplicate_account_truth(
            run_id=run.run_id,
            projected_positions=positions,
            projected_cash=cash,
            observed_positions=observed_positions,
            observed_account=observed_account,
        )
        return LocalSimPersistenceResult(
            payload={
                "order_count": int(persistence["order_count"]),
                "fill_count": 0,
                "cash_ledger_count": 0,
                "position_count": int(persistence["position_count"]),
                "cash": cash,
                "nav": None,
                "terminal": False,
                "active_state_count": int(persistence["active_state_count"]),
                "residual_state_count": 0,
            },
            positions=positions,
            marks={},
            cash=cash,
            economic_receipt_id=outbox.receipt_id,
            outbox_id=outbox.outbox_id,
            generation=outbox.generation,
            performance_payload={},
        )

    @staticmethod
    def _existing_valuation_projection_result(
        *,
        run: Any,
        outbox: LocalSimProjectionOutboxV1,
        payload: dict[str, Any],
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> LocalSimPersistenceResult:
        persistence = run.run_payload_json["local_sim_persistence"]
        positions = {
            item.symbol: item
            for item in (PositionLot.model_validate(raw) for raw in payload.get("positions") or [])
        }
        if len(positions) != len(payload.get("positions") or []):
            raise ValueError("valuation projection positions are duplicated")
        cash = float(payload["cash_reference"])
        projected_position_hashes = {
            symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
            for symbol, position in sorted(positions.items())
        }
        if (
            isinstance(payload["cash_reference"], bool)
            or not math.isfinite(cash)
            or cash < 0
            or dict(payload.get("position_hashes") or {}) != projected_position_hashes
        ):
            raise ValueError("valuation projection position hashes or cash are invalid")
        validate_local_sim_duplicate_account_truth(
            run_id=run.run_id,
            projected_positions=positions,
            projected_cash=cash,
            observed_positions=observed_positions,
            observed_account=observed_account,
        )
        if outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
            if (
                not isinstance(persistence, dict)
                or persistence.get("status") != "INTRADAY_VALUATION_PENDING"
                or persistence.get("valuation_status") != "WAITING_FOR_AUTHORITATIVE_MARKS"
                or persistence.get("nav") is not None
                or "strategy_performance" in run.run_payload_json
                or "performance_projection" in run.run_payload_json
            ):
                raise ValueError("valuation-pending persistence is invalid")
            return LocalSimPersistenceResult(
                payload={
                    "order_count": int(persistence["order_count"]),
                    "fill_count": int(persistence["fill_count"]),
                    "cash_ledger_count": int(persistence["cash_ledger_count"]),
                    "position_count": int(persistence["position_count"]),
                    "cash": cash,
                    "nav": None,
                    "terminal": False,
                    "economic_terminal": bool(persistence.get("economic_terminal")),
                    "active_state_count": int(persistence["active_state_count"]),
                    "residual_state_count": int(persistence["residual_state_count"]),
                    "valuation_status": "WAITING_FOR_AUTHORITATIVE_MARKS",
                    "missing_mark_symbols": list(persistence.get("missing_mark_symbols") or []),
                },
                positions=positions,
                marks={},
                cash=cash,
                economic_receipt_id=outbox.receipt_id,
                outbox_id=outbox.outbox_id,
                generation=outbox.generation,
                performance_payload={},
            )
        completion = run.run_payload_json.get("local_sim_valuation_completion_v1")
        if not isinstance(completion, dict):
            raise ValueError("valuation completion evidence is missing")
        completion_body = dict(completion)
        completion_hash = str(completion_body.pop("completion_hash", ""))
        expected_completion_keys = {
            "schema_version",
            "outbox_id",
            "generation",
            "economic_hash",
            "position_hashes",
            "marks",
            "mark_hashes",
            "account_snapshot",
            "account_snapshot_hash",
            "performance_hash",
            "completed_at",
        }
        if (
            set(completion_body) != expected_completion_keys
            or completion_body.get("schema_version") != "local_sim_valuation_completion_v1"
            or completion_body.get("outbox_id") != outbox.outbox_id
            or int(completion_body.get("generation") or 0) != outbox.generation
            or completion_body.get("economic_hash") != outbox.economic_hash
            or completion_body.get("position_hashes") != projected_position_hashes
            or completion_hash != canonical_json_sha256(completion_body)
        ):
            raise ValueError("valuation completion identity or hash is invalid")
        mark_items = [LocalSimMarketMarkV1.model_validate(raw) for raw in completion_body.get("marks") or []]
        marks = {item.symbol: item.price for item in mark_items}
        actual_mark_hashes = {item.symbol: item.mark_hash for item in mark_items}
        if (
            len(marks) != len(mark_items)
            or set(marks) != set(positions)
            or completion_body.get("mark_hashes") != actual_mark_hashes
        ):
            raise ValueError("valuation completion marks are duplicated or incomplete")
        account = AccountSnapshot.model_validate(completion_body.get("account_snapshot"))
        performance = run.run_payload_json["strategy_performance"]
        if not isinstance(performance, dict) or not isinstance(persistence, dict):
            raise ValueError("valuation completion projection is incomplete")
        account_hash = canonical_json_sha256(local_sim_fact_payload(account, fact_type="account_snapshot"))
        if (
            completion_body.get("account_snapshot_hash") != account_hash
            or completion_body.get("performance_hash") != canonical_json_sha256(performance)
            or float(account.cash) != cash
            or persistence.get("status") != payload.get("completed_persistence", {}).get("status")
            or float(persistence.get("cash")) != cash
            or float(persistence.get("nav")) != float(account.nav)
            or int(performance.get("local_sim_generation") or 0) != outbox.generation
            or performance.get("local_sim_outbox_id") != outbox.outbox_id
            or performance.get("local_sim_economic_hash") != outbox.economic_hash
        ):
            raise ValueError("valuation completion account, persistence or performance evidence conflicts")
        raw_receipts = run.run_payload_json.get("local_sim_projection_receipts_v1")
        if not isinstance(raw_receipts, dict):
            raise ValueError("valuation projection receipt map is missing")
        receipts = [
            LocalSimProjectionReceiptV1.model_validate(raw)
            for raw in raw_receipts.values()
            if isinstance(raw, dict) and raw.get("outbox_id") == outbox.outbox_id
        ]
        if len(receipts) != 1 or receipts[0].generation != outbox.generation:
            raise ValueError("valuation projection receipt is missing or duplicated")
        return LocalSimPersistenceResult(
            payload={
                "order_count": int(persistence["order_count"]),
                "fill_count": int(persistence["fill_count"]),
                "cash_ledger_count": int(persistence["cash_ledger_count"]),
                "position_count": int(persistence["position_count"]),
                "cash": float(account.cash),
                "nav": float(account.nav),
                "terminal": bool(persistence["terminal"]),
                "active_state_count": int(persistence["active_state_count"]),
                "residual_state_count": int(persistence["residual_state_count"]),
            },
            positions=positions,
            marks=marks,
            cash=float(account.cash),
            economic_receipt_id=outbox.receipt_id,
            outbox_id=outbox.outbox_id,
            generation=outbox.generation,
            performance_payload=performance,
        )


__all__ = [
    "build_local_sim_projection_payload",
    "derive_local_sim_projection_state_contract",
    "LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES",
    "LocalSimPaperProjectionRepository",
    "LocalSimProjectionCommitRequest",
    "LocalSimProjectionCommitResult",
    "LocalSimProjector",
    "LocalSimRuntimeProjectionRepository",
    "local_sim_projection_error_is_retryable",
    "local_sim_projection_paper_error",
]
