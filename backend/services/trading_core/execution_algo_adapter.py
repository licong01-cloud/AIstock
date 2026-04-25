"""Adapter from existing execution_algos to Trading Core StepFill objects."""

from __future__ import annotations

from typing import Any

from backend.execution_algos import ALGO_REGISTRY, get_algo
from backend.execution_algos.v24_plan_algo import V24PlanUnavailableError

from .errors import ExecutionAlgoError, UnsupportedFeatureError
from .models import MinuteBar, Order, StepFill


class ExecutionAlgoAdapter:
    """Wrap existing execution algorithms without letting them touch ledgers."""

    def create_state(self, order: Order, algo_code: str, config: dict[str, Any]):
        algo_code = self._normalize_algo_code(algo_code)
        algo = self._get_algo(algo_code, config)
        state = algo.init_order(order.symbol, order.side.value, order.quantity)
        if state.total_quantity != order.quantity:
            raise ExecutionAlgoError(
                "execution algorithm changed order quantity during init",
                context={
                    "order_id": order.order_id,
                    "requested_quantity": order.quantity,
                    "algo_quantity": state.total_quantity,
                    "algo_code": algo_code,
                },
            )
        return algo, state

    def compute_step(
        self,
        *,
        algo: Any,
        state: Any,
        bar: MinuteBar,
        market_context: dict[str, Any],
    ) -> StepFill | None:
        if bar.is_suspended:
            return None
        result = algo.compute_step(state, bar.to_algo_bar(), market_context)
        if result is None:
            return None
        try:
            return StepFill(
                symbol=result.symbol,
                side=result.side,
                quantity=result.quantity,
                price=result.price,
                bar_time=bar.bar_time,
                reason=result.reason or f"{algo.ALGO_CODE} minute step",
                metadata={"algo_code": algo.ALGO_CODE},
            )
        except Exception as exc:
            raise ExecutionAlgoError(
                "execution algorithm returned invalid step result",
                context={
                    "algo_code": getattr(algo, "ALGO_CODE", None),
                    "symbol": getattr(result, "symbol", None),
                    "quantity": getattr(result, "quantity", None),
                    "price": getattr(result, "price", None),
                },
            ) from exc

    def is_complete(self, algo: Any, state: Any) -> bool:
        return bool(algo.is_complete(state))

    def _get_algo(self, algo_code: str, config: dict[str, Any]):
        if algo_code not in ALGO_REGISTRY:
            raise UnsupportedFeatureError(
                "minute execution algorithm is not registered",
                context={"algo_code": algo_code, "registered_algos": sorted(ALGO_REGISTRY)},
            )
        try:
            return get_algo(algo_code, config=config)
        except V24PlanUnavailableError as exc:
            raise ExecutionAlgoError(
                "V24_PLAN is unavailable for authoritative minute execution",
                context={"algo_code": algo_code, "reason": str(exc)},
            ) from exc
        except Exception as exc:
            raise ExecutionAlgoError(
                "failed to initialize execution algorithm",
                context={"algo_code": algo_code},
            ) from exc

    @staticmethod
    def _normalize_algo_code(algo_code: str) -> str:
        normalized = str(algo_code or "").strip().upper()
        if not normalized:
            raise UnsupportedFeatureError("algo_code is required")
        return normalized
