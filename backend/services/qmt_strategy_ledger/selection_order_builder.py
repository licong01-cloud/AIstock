"""Build managed MiniQMT order requests from Selection Center results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.selection_center.models import SelectionCandidate, SelectionRun, SelectionRunStatus
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

from .models import BUY_ORDER_TYPE, SELL_ORDER_TYPE, StrategyPackageBinding
from .order_service import ManagedOrderRequest


@dataclass(frozen=True)
class SelectionOrderBuildConfig:
    default_target_weight: Decimal | None = None
    top_k: int | None = None
    price_type: int = 5
    buy_price_slippage_bps: Decimal = Decimal("0")
    sell_price_slippage_bps: Decimal = Decimal("0")
    order_remark_prefix: str = "qmtpkg"
    mode: str = "SIM"


@dataclass(frozen=True)
class SelectionOrderBuildResult:
    strategy_id: str
    strategy_name: str
    package_id: str
    selection_run_id: str
    trade_date: date
    requests: tuple[ManagedOrderRequest, ...]
    skipped: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "package_id": self.package_id,
            "selection_run_id": self.selection_run_id,
            "trade_date": self.trade_date.isoformat(),
            "requests": [
                {
                    "symbol": request.symbol,
                    "side": request.side,
                    "order_type": request.order_type,
                    "quantity": request.quantity,
                    "price_type": request.price_type,
                    "price": float(request.price),
                    "order_remark": request.order_remark,
                    "target_weight": float(request.target_weight) if request.target_weight is not None else None,
                    "package_id": request.package_id,
                    "selection_run_id": request.selection_run_id,
                }
                for request in self.requests
            ],
            "skipped": list(self.skipped),
        }


class SelectionOrderBuilder:
    def __init__(self, *, repository: Any, selection_reader: Any) -> None:
        self._repository = repository
        self._selection_reader = selection_reader

    def build_for_active_binding(
        self,
        *,
        strategy_id: str,
        config: SelectionOrderBuildConfig | None = None,
    ) -> SelectionOrderBuildResult:
        binding = self._repository.get_active_package_binding(strategy_id)
        if binding is None:
            raise DataUnavailableError("active package binding does not exist", context={"strategy_id": strategy_id})
        return self.build_for_binding(binding=binding, config=config or SelectionOrderBuildConfig())

    def build_for_binding(
        self,
        *,
        binding: StrategyPackageBinding,
        config: SelectionOrderBuildConfig | None = None,
    ) -> SelectionOrderBuildResult:
        config = config or SelectionOrderBuildConfig()
        account = self._repository.get_virtual_account(binding.strategy_id)
        selection_run: SelectionRun = self._selection_reader.get_run(binding.selection_run_id)
        if selection_run.status != SelectionRunStatus.SUCCEEDED:
            raise DataUnavailableError(
                "selection run is not succeeded",
                context={"selection_run_id": binding.selection_run_id, "status": selection_run.status.value},
            )
        if binding.package_id not in selection_run.package_ids:
            raise StrategyPackageValidationError(
                "selection run does not contain active binding package",
                context={"package_id": binding.package_id, "selection_run_id": selection_run.run_id},
            )

        candidates = self._candidates_for_binding(selection_run, binding)
        top_k = config.top_k or binding.top_k
        if top_k is not None:
            candidates = candidates[:top_k]
        if not candidates:
            raise DataUnavailableError(
                "selection run has no candidates for active binding",
                context={"package_id": binding.package_id, "selection_run_id": selection_run.run_id},
            )

        requests: list[ManagedOrderRequest] = []
        skipped: list[dict[str, Any]] = []
        current_quantities = self._current_quantities(account.strategy_id)
        for candidate in candidates:
            target_quantity = self._target_quantity(candidate, account.initial_cash, binding, config)
            current_quantity = current_quantities.get(candidate.symbol, 0)
            delta = target_quantity - current_quantity
            if delta == 0:
                skipped.append({"symbol": candidate.symbol, "reason": "ALREADY_AT_TARGET", "target_quantity": target_quantity})
                continue
            side = "BUY" if delta > 0 else "SELL"
            order_type = BUY_ORDER_TYPE if delta > 0 else SELL_ORDER_TYPE
            raw_quantity = abs(delta)
            quantity = round_to_board_lot(raw_quantity, candidate.symbol, side=side)
            if quantity <= 0:
                skipped.append(
                    {
                        "symbol": candidate.symbol,
                        "reason": "BOARD_LOT_ROUNDED_TO_ZERO",
                        "raw_quantity": raw_quantity,
                    }
                )
                continue
            if candidate.reference_price is None or candidate.reference_price <= 0:
                raise DataUnavailableError(
                    "reference_price is required to build managed order request",
                    context={"symbol": candidate.symbol, "selection_run_id": selection_run.run_id},
                )
            price = _price_with_slippage(
                Decimal(str(candidate.reference_price)),
                config.buy_price_slippage_bps if side == "BUY" else config.sell_price_slippage_bps,
                side,
            )
            requests.append(
                ManagedOrderRequest(
                    account_id=account.account_id,
                    strategy_name=account.strategy_name,
                    symbol=candidate.symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price_type=config.price_type,
                    price=price,
                    order_remark=_order_remark(config.order_remark_prefix, account.strategy_name, selection_run.run_id, candidate.symbol),
                    trade_date=selection_run.trade_date,
                    mode=config.mode,
                    package_id=binding.package_id,
                    selection_run_id=selection_run.run_id,
                    target_weight=_candidate_weight(candidate, binding, config),
                    metadata={
                        "rank": candidate.rank,
                        "score": candidate.score,
                        "target_quantity": target_quantity,
                        "current_quantity": current_quantity,
                    },
                )
            )

        return SelectionOrderBuildResult(
            strategy_id=account.strategy_id,
            strategy_name=account.strategy_name,
            package_id=binding.package_id,
            selection_run_id=selection_run.run_id,
            trade_date=selection_run.trade_date,
            requests=tuple(requests),
            skipped=tuple(skipped),
        )

    def _candidates_for_binding(
        self,
        selection_run: SelectionRun,
        binding: StrategyPackageBinding,
    ) -> list[SelectionCandidate]:
        if selection_run.aggregate_results:
            return sorted(selection_run.aggregate_results, key=lambda candidate: candidate.rank)
        return sorted(selection_run.package_results.get(binding.package_id, []), key=lambda candidate: candidate.rank)

    def _current_quantities(self, strategy_id: str) -> dict[str, int]:
        quantities: dict[str, int] = {}
        for lot in self._repository.list_position_lots(strategy_id):
            quantities[lot.symbol] = quantities.get(lot.symbol, 0) + lot.remaining_quantity
        return quantities

    def _target_quantity(
        self,
        candidate: SelectionCandidate,
        initial_cash: Decimal,
        binding: StrategyPackageBinding,
        config: SelectionOrderBuildConfig,
    ) -> int:
        if candidate.target_quantity is not None:
            return int(candidate.target_quantity)
        weight = _candidate_weight(candidate, binding, config)
        if weight is None:
            raise DataUnavailableError(
                "candidate target_quantity or target_weight is required",
                context={"symbol": candidate.symbol},
            )
        if candidate.reference_price is None or candidate.reference_price <= 0:
            raise DataUnavailableError(
                "reference_price is required for target_weight sizing",
                context={"symbol": candidate.symbol},
            )
        raw_quantity = int((initial_cash * weight / Decimal(str(candidate.reference_price))).to_integral_value(rounding="ROUND_FLOOR"))
        return round_to_board_lot(raw_quantity, candidate.symbol, side="BUY")


def _candidate_weight(
    candidate: SelectionCandidate,
    binding: StrategyPackageBinding,
    config: SelectionOrderBuildConfig,
) -> Decimal | None:
    if candidate.target_weight is not None:
        return Decimal(str(candidate.target_weight))
    if binding.target_weight is not None:
        return binding.target_weight
    return config.default_target_weight


def _price_with_slippage(price: Decimal, slippage_bps: Decimal, side: str) -> Decimal:
    sign = Decimal("1") if side == "BUY" else Decimal("-1")
    multiplier = Decimal("1") + sign * (slippage_bps / Decimal("10000"))
    return (price * multiplier).quantize(Decimal("0.000001"))


def _order_remark(prefix: str, strategy_name: str, selection_run_id: str, symbol: str) -> str:
    safe_strategy = "".join(ch for ch in strategy_name if ch.isalnum() or ch in {"_", "-"})[:24]
    safe_run = selection_run_id.replace("sel_", "")[:10]
    safe_symbol = symbol.replace(".", "")
    return f"{prefix}_{safe_strategy}_{safe_run}_{safe_symbol}"
