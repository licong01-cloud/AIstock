"""LocalSIM valuation completion owner for committed economic generations."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from backend.services.simulation_data.daily_context import SimulationBrokerBackend, canonical_json_sha256
from backend.services.simulation_execution.localsim.economic import (
    canonical_local_sim_json_value,
    local_sim_fact_payload,
    validate_local_sim_duplicate_account_truth,
)
from backend.services.simulation_execution.localsim.models import (
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
)
from backend.services.simulation_execution.localsim.projection import (
    LOCAL_SIM_PROJECTION_MAX_ATTEMPTS,
    LocalSimProjectionCommitRequest,
    LocalSimProjector,
    local_sim_projection_error_is_retryable,
)
from backend.services.trading_core.errors import BrokerConnectivityError, DataUnavailableError
from backend.services.trading_core.models import AccountSnapshot, PositionLot, RunStatus


class LocalSimValuationCoordinator:
    """Resolve authoritative marks before handing a complete outbox to the projector."""

    def __init__(
        self,
        *,
        runtime_repository: Any,
        paper_repository: Any,
        performance_service: Any,
        position_marks: Callable[..., Any],
        snapshot_time: Callable[..., datetime],
        mark_failure_allows_pending: Callable[[BaseException], bool],
    ) -> None:
        self._runtime_repository = runtime_repository
        self._paper_repository = paper_repository
        self._performance_service = performance_service
        self._position_marks = position_marks
        self._snapshot_time = snapshot_time
        self._mark_failure_allows_pending = mark_failure_allows_pending
        self._projector = LocalSimProjector(
            runtime_repository=runtime_repository,
            paper_repository=paper_repository,
            performance_service=performance_service,
        )


    def complete_pending_outbox(
        self,
        *,
        run: Any,
        outbox: LocalSimProjectionOutboxV1,
        paper_repository: Any,
        context: Any,
        execution: Any,
        observed_positions: dict[str, PositionLot] | None,
        observed_account: Any | None,
        valuation_as_of_time: datetime | None,
    ) -> tuple[Any, dict[str, Any]]:
        """Complete one pending valuation without creating a new generation."""

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
            "initial_capital",
            "realized_pnl",
            "positions",
            "position_hashes",
            "cash_reference",
            "valuation_snapshot_time",
            "valuation_error",
            "snapshot_metadata",
            "final_simulation_status",
            "final_paper_status",
            "final_event_type",
            "final_event_message",
            "final_event_context",
            "paper_error",
            "completed_persistence",
            "economic_hash",
            "tca_generation",
        }
        if set(payload) != required_keys:
            raise DataUnavailableError(
                "LocalSim valuation-pending projection payload is not exact",
                context={
                    "reason_code": "LOCALSIM_VALUATION_PROJECTION_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "missing_fields": sorted(required_keys - set(payload)),
                    "unknown_fields": sorted(set(payload) - required_keys),
                    "economic_commit_staged": True,
                },
            )
        if (
            payload.get("projection_kind") != "VALUATION_PENDING"
            or payload.get("run_id") != run.run_id
            or payload.get("binding_id") != run.binding_id
            or payload.get("plan_id") != run.execution_plan_id
            or payload.get("trade_date") != run.trade_date.isoformat()
            or payload.get("economic_hash") != outbox.economic_hash
        ):
            raise DataUnavailableError(
                "LocalSim valuation-pending projection identity conflicts with the durable run",
                context={
                    "reason_code": "LOCALSIM_VALUATION_PROJECTION_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "economic_commit_staged": True,
                },
            )
        try:
            positions = {
                item.symbol: item for item in (PositionLot.model_validate(raw) for raw in payload["positions"])
            }
            if len(positions) != len(payload["positions"]):
                raise ValueError("position symbols are duplicated")
            position_hashes = dict(payload["position_hashes"])
            actual_position_hashes = {
                symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
                for symbol, position in sorted(positions.items())
            }
            cash = float(payload["cash_reference"])
            if (
                isinstance(payload["cash_reference"], bool)
                or not math.isfinite(cash)
                or cash < 0
                or actual_position_hashes != position_hashes
            ):
                raise ValueError("position hashes or cash reference are invalid")
            final_status = type(run.status)(str(payload["final_simulation_status"]))
            final_paper_status = RunStatus(str(payload["final_paper_status"]))
            completed_persistence = dict(payload["completed_persistence"])
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim valuation-pending projection fields are invalid",
                context={
                    "reason_code": "LOCALSIM_VALUATION_PROJECTION_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "economic_commit_staged": True,
                },
            ) from exc
        if (observed_positions is None) != (observed_account is None):
            raise DataUnavailableError(
                "LocalSim valuation duplicate-account evidence is incomplete",
                context={
                    "reason_code": "LOCALSIM_VALUATION_ACCOUNT_EVIDENCE_INCOMPLETE",
                    "run_id": run.run_id,
                    "economic_commit_staged": True,
                },
            )
        if observed_positions is not None and observed_account is not None:
            validate_local_sim_duplicate_account_truth(
                run_id=run.run_id,
                projected_positions=positions,
                projected_cash=cash,
                observed_positions=observed_positions,
                observed_account=observed_account,
            )

        raw_completion = run.run_payload_json.get("local_sim_valuation_completion_v1")
        performance = run.run_payload_json.get("strategy_performance")
        if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
            if not isinstance(raw_completion, dict) or not isinstance(performance, dict):
                raise DataUnavailableError(
                    "LocalSim projected valuation has no completion evidence",
                    context={
                        "reason_code": "LOCALSIM_VALUATION_COMPLETION_MISSING",
                        "run_id": run.run_id,
                        "economic_commit_staged": True,
                    },
                )
            completion_body = dict(raw_completion)
            completion_hash = str(completion_body.pop("completion_hash", ""))
            if (
                completion_body.get("outbox_id") != outbox.outbox_id
                or int(completion_body.get("generation") or 0) != outbox.generation
                or completion_body.get("economic_hash") != outbox.economic_hash
                or completion_hash != canonical_json_sha256(completion_body)
            ):
                raise DataUnavailableError(
                    "LocalSim valuation completion evidence conflicts with the generation",
                    context={
                        "reason_code": "LOCALSIM_VALUATION_COMPLETION_IDENTITY_CONFLICT",
                        "run_id": run.run_id,
                        "economic_commit_staged": True,
                    },
                )
            raw_receipts = run.run_payload_json.get("local_sim_projection_receipts_v1")
            if not isinstance(raw_receipts, dict):
                raise DataUnavailableError(
                    "LocalSim valuation projection receipt map is missing",
                    context={
                        "reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING",
                        "run_id": run.run_id,
                        "economic_commit_staged": True,
                    },
                )
            receipts = [
                LocalSimProjectionReceiptV1.model_validate(raw)
                for raw in raw_receipts.values()
                if isinstance(raw, dict) and raw.get("outbox_id") == outbox.outbox_id
            ]
            if len(receipts) != 1:
                raise DataUnavailableError(
                    "LocalSim valuation projection receipt is missing or duplicated",
                    context={
                        "reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING",
                        "run_id": run.run_id,
                        "economic_commit_staged": True,
                    },
                )
            account_snapshot = AccountSnapshot.model_validate(completion_body.get("account_snapshot"))
            readback_failure = run.run_payload_json.get("local_sim_projection_readback_failure")
            if readback_failure is not None and not isinstance(readback_failure, dict):
                raise DataUnavailableError(
                    "LocalSim valuation readback failure receipt is invalid",
                    context={
                        "reason_code": "LOCALSIM_PROJECTION_READBACK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "economic_commit_staged": True,
                    },
                )
            previous_attempts = int((readback_failure or {}).get("attempt_count") or 0)
            if previous_attempts >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS:
                raise DataUnavailableError(
                    "LocalSim valuation readback exhausted its automatic retry budget",
                    context={
                        "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
                        "run_id": run.run_id,
                        "outbox_id": outbox.outbox_id,
                        "attempt_count": previous_attempts,
                        "economic_commit_staged": True,
                    },
                )
            try:
                self._runtime_repository.readback_local_sim_projection_commit(
                    run_id=run.run_id,
                    receipt=receipts[0],
                )
                paper_repository.readback_local_sim_projection(
                    run_id=run.run_id,
                    portfolio_id=account_snapshot.portfolio_id,
                    trade_date=run.trade_date,
                    outbox_id=outbox.outbox_id,
                    generation=outbox.generation,
                    expected_position_count=len(positions),
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
                reason_code = (
                    "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
                    if attempt_count >= LOCAL_SIM_PROJECTION_MAX_ATTEMPTS
                    else "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
                )
                raise DataUnavailableError(
                    "LocalSim valuation projection readback must be retried",
                    context={
                        "reason_code": reason_code,
                        "run_id": run.run_id,
                        "outbox_id": outbox.outbox_id,
                        "attempt_count": attempt_count,
                        "cause": str(exc),
                        "economic_commit_staged": True,
                    },
                ) from exc
            if readback_failure is not None:
                run = self._runtime_repository.clear_local_sim_projection_readback_failure(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    final_status=final_status,
                )
            return run, performance

        completion_snapshot_time = (
            valuation_as_of_time
            if valuation_as_of_time is not None
            else self._snapshot_time(
                fills=(),
                events=(),
                run=run,
                local_broker=context.local_broker,
                market_data_source=context.market_data_source,
            )
        )
        if completion_snapshot_time.date() != run.trade_date:
            raise DataUnavailableError(
                "LocalSim valuation completion time does not match the run trade date",
                context={
                    "reason_code": "LOCALSIM_MARK_AS_OF_DATE_CONFLICT",
                    "run_id": run.run_id,
                    "trade_date": run.trade_date.isoformat(),
                    "as_of_time": completion_snapshot_time.isoformat(),
                    "economic_commit_staged": True,
                },
            )
        try:
            marks, mark_records = self._position_marks(
                positions=positions,
                context=context,
                execution=execution,
                snapshot_time=completion_snapshot_time,
            )
        except (DataUnavailableError, BrokerConnectivityError) as exc:
            if not self._mark_failure_allows_pending(exc):
                raise DataUnavailableError(
                    str(exc),
                    context={
                        **dict(getattr(exc, "context", None) or {}),
                        "economic_commit_staged": True,
                        "run_id": run.run_id,
                    },
                ) from exc
            error_context = dict(getattr(exc, "context", None) or {})
            reason_code = str(error_context.get("reason_code") or "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE")
            observed = datetime.now(UTC).isoformat()
            pending = self._runtime_repository.update_simulation_daily_run(
                run.run_id,
                status=type(run.status).INTRADAY_RUNNING,
                payload_patch={
                    "local_sim_valuation_pending_v1": {
                        "schema_version": "local_sim_valuation_pending_v1",
                        "status": "WAITING_FOR_AUTHORITATIVE_MARKS",
                        "run_id": run.run_id,
                        "plan_id": run.execution_plan_id,
                        "missing_symbols": sorted(
                            {
                                str(error_context.get("symbol") or "").strip(),
                                *(
                                    str(item).strip()
                                    for item in error_context.get("missing_symbols", [])
                                    if str(item).strip()
                                ),
                            }
                            - {""}
                        )
                        or sorted(positions),
                        "valuation_error": {
                            "reason_code": reason_code,
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": canonical_local_sim_json_value(error_context),
                            "observed_at": observed,
                        },
                        "economic_terminal": bool(completed_persistence.get("terminal")),
                        "broker_called": True,
                        "last_attempt_at": observed,
                    },
                    "last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED_VALUATION_PENDING",
                },
                payload_unset=(
                    "submit_failure",
                    "local_sim_retry_diagnostics",
                    "local_sim_failed_run_recovery_failure_v1",
                ),
            )
            return pending, {}

        market_value = sum(int(position.quantity) * marks[position.symbol] for position in positions.values())
        account_snapshot = AccountSnapshot(
            portfolio_id=str(payload["portfolio_id"]),
            cash=cash,
            market_value=market_value,
            nav=cash + market_value,
            snapshot_time=completion_snapshot_time,
        )
        completed_persistence.update(
            {
                "snapshot_time": completion_snapshot_time.isoformat(),
                "cash": cash,
                "nav": account_snapshot.nav,
            }
        )
        performance = self._performance_service.project_strategy(
            strategy_id=str(payload["strategy_id"]),
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            initial_capital=float(payload["initial_capital"]),
            cash=cash,
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
                "tca_generation": {
                    **dict(payload["tca_generation"]),
                    "generation": outbox.generation,
                },
            }
        )
        snapshot_metadata = {
            **dict(payload["snapshot_metadata"]),
            "local_sim_generation": outbox.generation,
            "local_sim_outbox_id": outbox.outbox_id,
            "local_sim_economic_hash": outbox.economic_hash,
            "projection_payload_hash": outbox.projection_payload_hash,
            "valuation_completed_from_pending": True,
        }
        completion_body = {
            "schema_version": "local_sim_valuation_completion_v1",
            "outbox_id": outbox.outbox_id,
            "generation": outbox.generation,
            "economic_hash": outbox.economic_hash,
            "position_hashes": position_hashes,
            "marks": [item.model_dump(mode="json") for _, item in sorted(mark_records.items())],
            "mark_hashes": {symbol: item.mark_hash for symbol, item in sorted(mark_records.items())},
            "account_snapshot": account_snapshot.model_dump(mode="json"),
            "account_snapshot_hash": canonical_json_sha256(
                local_sim_fact_payload(
                    account_snapshot,
                    fact_type="account_snapshot",
                )
            ),
            "performance_hash": canonical_json_sha256(performance),
            "completed_at": datetime.now(UTC).isoformat(),
        }
        completion = {
            **completion_body,
            "completion_hash": canonical_json_sha256(completion_body),
        }
        projection_result = {
            "schema_version": "local_sim_projection_result_v1",
            "projection_kind": "VALUATION_COMPLETED",
            "outbox_id": outbox.outbox_id,
            "generation": outbox.generation,
            "economic_hash": outbox.economic_hash,
            "position_hashes": position_hashes,
            "mark_hashes": completion_body["mark_hashes"],
            "account_snapshot_hash": completion_body["account_snapshot_hash"],
            "performance_hash": completion_body["performance_hash"],
            "completion_hash": completion["completion_hash"],
        }
        projection_committed = False

        def apply_valuation_projection() -> None:
            paper_repository.save_positions(
                run_id=run.run_id,
                trade_date=run.trade_date,
                positions=list(positions.values()),
                prices=marks,
            )
            paper_repository.save_daily_snapshot(
                run_id=run.run_id,
                trade_date=run.trade_date,
                snapshot=account_snapshot,
                metadata=snapshot_metadata,
            )
            paper_repository.save_run_event(
                run_id=run.run_id,
                event_type=str(payload["final_event_type"]),
                message=str(payload["final_event_message"]),
                context={
                    **dict(payload["final_event_context"]),
                    "snapshot_time": completion_snapshot_time.isoformat(),
                    "local_sim_generation": outbox.generation,
                    "local_sim_outbox_id": outbox.outbox_id,
                    "local_sim_economic_hash": outbox.economic_hash,
                    "valuation_completed_from_pending": True,
                },
            )
            paper_repository.update_run_status(
                paper_repository.get_run(run.run_id),
                final_paper_status,
                error=payload["paper_error"],
            )

        def mark_projection_staged() -> None:
            nonlocal projection_committed
            projection_committed = True

        def readback_valuation_projection(receipt: LocalSimProjectionReceiptV1) -> Any:
            projected_run = self._runtime_repository.readback_local_sim_projection_commit(
                run_id=run.run_id,
                receipt=receipt,
            )
            paper_repository.readback_local_sim_projection(
                run_id=run.run_id,
                portfolio_id=account_snapshot.portfolio_id,
                trade_date=run.trade_date,
                outbox_id=outbox.outbox_id,
                generation=outbox.generation,
                expected_position_count=len(positions),
            )
            return projected_run

        try:
            result = self._projector.commit(
                LocalSimProjectionCommitRequest(
                    run_id=run.run_id,
                    outbox_id=outbox.outbox_id,
                    generation=outbox.generation,
                    final_status=final_status,
                    projection_result=projection_result,
                    payload_patch={
                        "strategy_performance": performance,
                        "performance_projection": performance,
                        "local_sim_persistence": completed_persistence,
                        "local_sim_valuation_completion_v1": completion,
                        "local_sim_projection_generation": {
                            "schema_version": "local_sim_projection_generation_v1",
                            "generation": outbox.generation,
                            "outbox_id": outbox.outbox_id,
                            "economic_hash": outbox.economic_hash,
                        },
                        "last_stage": final_status.value,
                    },
                    payload_unset=(
                        "submit_failure",
                        "local_sim_retry_diagnostics",
                        "local_sim_valuation_pending_v1",
                        "local_sim_failed_run_recovery_failure_v1",
                    ),
                    apply_paper_projection=apply_valuation_projection,
                    readback=readback_valuation_projection,
                    on_staged=mark_projection_staged,
                )
            )
            return result.projected, performance
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
                "LocalSim valuation completion failed after economic commit",
                context={
                    "reason_code": reason_code,
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                    "attempt_count": attempt_count,
                    "economic_commit_staged": True,
                },
            ) from exc


__all__ = ["LocalSimValuationCoordinator"]
