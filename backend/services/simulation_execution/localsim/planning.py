"""LocalSIM-specific execution-plan preparation and causality ownership."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.services.simulation_data.daily_context import SimulationBrokerBackend, canonical_json_sha256
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide


class LocalSimPlanner:
    def __init__(
        self,
        *,
        repository: Any,
        normalize_time: Callable[[datetime | None], datetime],
        schedule_windows: Callable[..., list[dict[str, Any]]],
    ) -> None:
        self._repository = repository
        self._normalize_time = normalize_time
        self._schedule_windows = schedule_windows

    @staticmethod
    def assert_plan_uses_twap(*, binding: Any, plan: Any) -> None:
        if binding.broker_backend is not SimulationBrokerBackend.LOCAL_SIM:
            return
        plan_policy_id = str(plan.execution_policy_version_id or "").strip()

        def malformed(detail: str) -> RuntimeConfigInvalidError:
            return RuntimeConfigInvalidError(
                "LocalSIM existing execution plan policy payload is missing or malformed",
                context={
                    "reason_code": "LOCALSIM_EXECUTION_PLAN_POLICY_MISSING_OR_MALFORMED",
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                    "plan_execution_policy_version_id": plan_policy_id or None,
                    "plan_algo_code": None,
                    "required_algo_code": "TWAP",
                    "malformed_detail": detail,
                    "broker_call_attempted": False,
                    "fallback_used": False,
                    "required_action": (
                        "inspect or lawfully rebuild the frozen execution plan; its execution "
                        "policy payload is missing or malformed"
                    ),
                },
            )

        policy_container = plan.plan_payload_json.get("execution_policy")
        if not isinstance(policy_container, dict):
            raise malformed("execution_policy container is missing or not an object")
        payload = policy_container.get("payload")
        if not isinstance(payload, dict):
            raise malformed("execution_policy payload is missing or not an object")
        policy_json = payload.get("policy_json")
        if policy_json is None:
            policy_json = payload
        elif not isinstance(policy_json, dict):
            raise malformed("execution_policy policy_json is present but not an object")
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not algo_code:
            raise malformed("execution_policy algo_code is missing or blank")
        if algo_code == "TWAP":
            return
        raise RuntimeConfigInvalidError(
            "LocalSIM existing execution plan is not eligible under the TWAP-only runtime policy",
            context={
                "reason_code": "LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED",
                "binding_id": binding.binding_id,
                "plan_id": plan.plan_id,
                "plan_execution_policy_version_id": plan_policy_id or None,
                "plan_algo_code": algo_code,
                "required_algo_code": "TWAP",
                "broker_call_attempted": False,
                "fallback_used": False,
                "required_action": "create a new LocalSIM execution plan under the TWAP-only runtime policy",
            },
        )

    def prepare_execution_plan_for_submit(
        self,
        *,
        binding: Any,
        run: Any,
        plan: Any,
        context: Any,
    ) -> tuple[Any, Any]:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or not plan.intents:
            return run, plan
        prepared_plan, fit_payload = self.cash_fit_execution_plan(
            binding=binding,
            run=run,
            plan=plan,
            context=context,
        )
        if fit_payload["status"] == "UNCHANGED":
            return run, plan
        prepared_plan = self._repository.save_execution_plan(prepared_plan)
        updated = self._repository.update_simulation_daily_run(
            run.run_id,
            execution_plan=prepared_plan,
            payload_patch={
                "local_sim_cash_fit": fit_payload,
                "execution_plan_intent_count": len(prepared_plan.intents),
                "order_intent_count": len(prepared_plan.intents),
            },
            payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
        )
        return updated, prepared_plan

    @classmethod
    def cash_fit_execution_plan(
        cls,
        *,
        binding: Any,
        run: Any,
        plan: Any,
        context: Any,
    ) -> tuple[Any, dict[str, Any]]:
        if context.cash is None:
            raise DataUnavailableError(
                "LocalSim cash-fit requires explicit account cash; context.cash is missing",
                context={
                    "reason_code": "LOCALSIM_CASH_CONTEXT_MISSING",
                    "stage": "LOCALSIM_CASH_FIT",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "trade_date": run.trade_date.isoformat(),
                    "broker_backend": binding.broker_backend.value,
                    "required_action": (
                        "load authoritative Paper v2 portfolio cash before LocalSim submit; "
                        "do not default missing cash to 0.0"
                    ),
                },
            )
        cash = float(context.cash)
        sells = [intent for intent in plan.intents if intent.side == OrderSide.SELL]
        buys = [intent for intent in plan.intents if intent.side == OrderSide.BUY]
        prepared = [*sells, *buys]
        payload = {
            "schema_version": "localsim_capital_dependency_v1",
            "status": "UNCHANGED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": round(cash, 6),
            "original_intent_count": len(plan.intents),
            "prepared_intent_count": len(prepared),
            "sell_intent_count": len(sells),
            "buy_intent_count": len(buys),
            "dependent_buy_count": len(buys),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        }
        if [intent.intent_id for intent in prepared] == [intent.intent_id for intent in plan.intents]:
            return plan, payload
        payload["status"] = "SELL_FIRST_DEPENDENCY_ORDERED"
        return cls._copy_plan_with_intents(plan=plan, intents=prepared, cash_fit_payload=payload), payload

    @staticmethod
    def _copy_plan_with_intents(*, plan: Any, intents: list[Any], cash_fit_payload: dict[str, Any]) -> Any:
        payload = deepcopy(plan.plan_payload_json)
        payload["intents"] = [
            {
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "target_quantity": intent.target_quantity,
                "delta_quantity": intent.delta_quantity,
                "order_quantity": intent.order_quantity,
                "target_weight": intent.target_weight,
                "reference_price": intent.price_policy.get("reference_price"),
                "current_quantity": intent.current_quantity,
                "current_available_quantity": intent.current_available_quantity,
                "rebalance_reason": intent.rebalance_reason,
                "trading_rule_decision_id": intent.trading_rule_decision_id,
                "order_type": intent.price_policy.get("order_type"),
                "limit_price": intent.price_policy.get("limit_price"),
                "schedule_window": intent.schedule_window,
                "price_policy": intent.price_policy,
                "risk_context": intent.risk_context,
                "metadata": intent.metadata,
            }
            for intent in intents
        ]
        payload["local_sim_cash_fit"] = cash_fit_payload
        plan_hash = canonical_json_sha256(payload)
        plan_id = f"plan_{plan_hash[:16]}"
        plan_intents = [intent.model_copy(update={"plan_id": plan_id}) for intent in intents]
        return plan.model_copy(
            update={
                "plan_id": plan_id,
                "intents": plan_intents,
                "plan_payload_json": payload,
                "plan_hash": plan_hash,
                "created_at": datetime.now(UTC),
            }
        )

    def causality_cursor(self, plan: Any | None) -> datetime | None:
        if plan is None:
            return None
        payload = plan.plan_payload_json.get("local_sim_execution_causality")
        if not isinstance(payload, dict) or not payload.get("eligible_bar_after"):
            return None
        raw = payload["eligible_bar_after"]
        try:
            return self._normalize_time(datetime.fromisoformat(str(raw)))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "LocalSim execution plan has an invalid causality cursor",
                context={"plan_id": plan.plan_id, "eligible_bar_after": raw},
            ) from exc

    def attach_causality_cursor(
        self,
        *,
        build_result: Any,
        as_of_time: datetime | None,
        preserved_cursor: datetime | None,
    ) -> Any:
        plan = build_result.execution_plan
        local_as_of = self._normalize_time(as_of_time)
        cursor = self._normalize_time(preserved_cursor) if preserved_cursor is not None else None
        cursor_source = "preserved_execution_plan"
        if cursor is None:
            windows = self._schedule_windows(trade_date=plan.target_trade_date, as_of_time=local_as_of)
            submit_windows = [window for window in windows if window.get("action") == "submit"]
            active = next((window for window in submit_windows if window.get("state") == "ACTIVE"), None)
            if active is not None:
                cursor = local_as_of
                cursor_source = "first_plan_during_submit_window"
            else:
                next_window = next(
                    (
                        window
                        for window in submit_windows
                        if datetime.fromisoformat(str(window["start_at"])) > local_as_of
                    ),
                    None,
                )
                if next_window is not None:
                    cursor = datetime.fromisoformat(str(next_window["start_at"])) - timedelta(microseconds=1)
                    cursor_source = "next_submit_window_boundary"
                else:
                    cursor = local_as_of
                    cursor_source = "after_last_submit_window"
        causality = {
            "schema_version": "local_sim_execution_causality_v1",
            "eligible_bar_after": cursor.isoformat(),
            "captured_as_of_time": local_as_of.isoformat(),
            "cursor_source": cursor_source,
            "bar_selection_rule": "strictly_after_cursor_and_not_after_scheduler_as_of",
        }
        payload = deepcopy(plan.plan_payload_json)
        payload["local_sim_execution_causality"] = causality
        payload_intents = payload.get("intents")
        if not isinstance(payload_intents, list):
            raise RuntimeConfigInvalidError(
                "LocalSim execution plan payload is missing intents",
                context={"plan_id": plan.plan_id},
            )
        updated_intents = [
            intent.model_copy(
                update={"metadata": {**dict(intent.metadata), "local_sim_execution_causality": causality}}
            )
            for intent in plan.intents
        ]
        by_intent_id = {intent.intent_id: intent for intent in updated_intents}
        for item in payload_intents:
            if not isinstance(item, dict):
                raise RuntimeConfigInvalidError(
                    "LocalSim execution plan contains an invalid intent payload",
                    context={"plan_id": plan.plan_id},
                )
            updated = by_intent_id.get(str(item.get("intent_id") or ""))
            if updated is None:
                raise RuntimeConfigInvalidError(
                    "LocalSim execution plan intent payload cannot be reconstructed",
                    context={"plan_id": plan.plan_id, "intent_id": item.get("intent_id")},
                )
            item["metadata"] = dict(updated.metadata)
        plan_hash = canonical_json_sha256(payload)
        plan_id = f"plan_{plan_hash[:16]}"
        updated_intents = [intent.model_copy(update={"plan_id": plan_id}) for intent in updated_intents]
        prepared_plan = plan.model_copy(
            update={
                "plan_id": plan_id,
                "intents": updated_intents,
                "plan_payload_json": payload,
                "plan_hash": plan_hash,
                "created_at": datetime.now(UTC),
            }
        )
        persisted_plan = self._repository.save_execution_plan(prepared_plan)
        updated_run = self._repository.update_simulation_daily_run(
            build_result.run.run_id,
            execution_plan=persisted_plan,
            payload_patch={"local_sim_execution_causality": causality},
        )
        return replace(build_result, run=updated_run, execution_plan=persisted_plan)


__all__ = ["LocalSimPlanner"]
