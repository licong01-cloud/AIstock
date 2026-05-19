"""Build managed MiniQMT order requests from Selection Center results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.selection_center.models import SelectionCandidate, SelectionRun, SelectionRunStatus
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

from .lot_availability import (
    DbTradingCalendarProvider,
    TradingCalendarProvider,
    effective_strategy_available_sell_quantity,
)
from .models import BUY_ORDER_TYPE, SELL_ORDER_TYPE, BindingStatus, StrategyPackageBinding, VirtualAccount
from .order_service import ManagedOrderRequest

LATEST_PRICE_TYPE = 5


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
                    "metadata": request.metadata,
                }
                for request in self.requests
            ],
            "skipped": list(self.skipped),
        }


@dataclass(frozen=True)
class _PositionSummary:
    symbol: str
    remaining_quantity: int
    available_quantity: int
    lot_count: int
    reference_price: Decimal | None = None
    reference_price_source: str | None = None


class SelectionOrderBuilder:
    def __init__(
        self,
        *,
        repository: Any,
        selection_reader: Any,
        calendar_provider: TradingCalendarProvider | None = None,
    ) -> None:
        self._repository = repository
        self._selection_reader = selection_reader
        self._calendar_provider = calendar_provider or DbTradingCalendarProvider()

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
        if binding.binding_status != BindingStatus.ACTIVE:
            raise StrategyPackageValidationError(
                "package binding is not ACTIVE",
                context={"binding_id": binding.binding_id, "binding_status": binding.binding_status.value},
            )
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
        position_summaries = self._position_summaries(account.strategy_id, selection_run.trade_date)
        target_symbols: set[str] = set()
        for candidate in candidates:
            target_symbols.add(candidate.symbol)
            target_quantity = self._target_quantity(candidate, account.initial_cash, binding, config)
            current_position = position_summaries.get(candidate.symbol)
            current_quantity = current_position.remaining_quantity if current_position is not None else 0
            available_quantity = current_position.available_quantity if current_position is not None else 0
            delta = target_quantity - current_quantity
            if delta == 0:
                skipped.append(
                    {
                        "symbol": candidate.symbol,
                        "reason": "ALREADY_AT_TARGET",
                        "target_quantity": target_quantity,
                        "current_quantity": current_quantity,
                        "available_quantity": available_quantity,
                    }
                )
                continue
            if delta < 0 and available_quantity <= 0:
                self._append_sell_request(
                    requests,
                    skipped,
                    account=account,
                    binding=binding,
                    selection_run=selection_run,
                    config=config,
                    symbol=candidate.symbol,
                    requested_quantity=abs(delta),
                    available_quantity=available_quantity,
                    current_quantity=current_quantity,
                    target_quantity=target_quantity,
                    price=Decimal("0"),
                    price_source="not_required_no_available_lot",
                    target_weight=_candidate_weight(candidate, binding, config),
                    metadata={
                        "rank": candidate.rank,
                        "score": candidate.score,
                        "rebalance_reason": "REDUCE_TO_TARGET",
                    },
                    no_available_reason="NO_AVAILABLE_LOT",
                )
                continue
            if candidate.reference_price is None or candidate.reference_price <= 0:
                raise DataUnavailableError(
                    "reference_price is required to build managed order request",
                    context={"symbol": candidate.symbol, "selection_run_id": selection_run.run_id},
                )
            if delta > 0:
                self._append_buy_request(
                    requests,
                    skipped,
                    account=account,
                    binding=binding,
                    selection_run=selection_run,
                    candidate=candidate,
                    config=config,
                    raw_quantity=delta,
                    target_quantity=target_quantity,
                    current_quantity=current_quantity,
                )
                continue

            price = _price_with_slippage(Decimal(str(candidate.reference_price)), config.sell_price_slippage_bps, "SELL")
            self._append_sell_request(
                requests,
                skipped,
                account=account,
                binding=binding,
                selection_run=selection_run,
                config=config,
                symbol=candidate.symbol,
                requested_quantity=abs(delta),
                available_quantity=available_quantity,
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                price=price,
                price_source="selection_reference_price",
                target_weight=_candidate_weight(candidate, binding, config),
                metadata={
                    "rank": candidate.rank,
                    "score": candidate.score,
                    "rebalance_reason": "REDUCE_TO_TARGET",
                },
                no_available_reason="NO_AVAILABLE_LOT",
            )

        for position in sorted(position_summaries.values(), key=lambda item: item.symbol):
            if position.symbol in target_symbols or position.remaining_quantity <= 0:
                continue
            if position.available_quantity <= 0:
                self._append_sell_request(
                    requests,
                    skipped,
                    account=account,
                    binding=binding,
                    selection_run=selection_run,
                    config=config,
                    symbol=position.symbol,
                    requested_quantity=position.remaining_quantity,
                    available_quantity=position.available_quantity,
                    current_quantity=position.remaining_quantity,
                    target_quantity=0,
                    price=Decimal("0"),
                    price_source="not_required_no_available_lot",
                    target_weight=None,
                    metadata={
                        "rebalance_reason": "DROPPED_FROM_SELECTION",
                        "lot_count": position.lot_count,
                    },
                    no_available_reason="NO_AVAILABLE_LOT_FOR_DROPPED_HOLDING",
                )
                continue
            price, price_source = self._dropped_sell_price(position, config)
            if price is None:
                skipped.append(
                    {
                        "symbol": position.symbol,
                        "reason": "MISSING_REFERENCE_PRICE_FOR_DROPPED_HOLDING",
                        "current_quantity": position.remaining_quantity,
                        "available_quantity": position.available_quantity,
                        "price_type": config.price_type,
                    }
                )
                continue
            self._append_sell_request(
                requests,
                skipped,
                account=account,
                binding=binding,
                selection_run=selection_run,
                config=config,
                symbol=position.symbol,
                requested_quantity=position.remaining_quantity,
                available_quantity=position.available_quantity,
                current_quantity=position.remaining_quantity,
                target_quantity=0,
                price=price,
                price_source=price_source,
                target_weight=None,
                metadata={
                    "rebalance_reason": "DROPPED_FROM_SELECTION",
                    "lot_count": position.lot_count,
                },
                no_available_reason="NO_AVAILABLE_LOT_FOR_DROPPED_HOLDING",
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

    def _position_summaries(self, strategy_id: str, trade_date: date) -> dict[str, _PositionSummary]:
        summaries: dict[str, _PositionSummary] = {}
        pending_sell_intents_by_symbol: dict[str, list[Any]] = {}
        for lot in self._repository.list_position_lots(strategy_id):
            existing = summaries.get(lot.symbol)
            lot_reference_price, lot_reference_price_source = _lot_reference_price(lot)
            remaining_quantity = max(int(lot.remaining_quantity), 0)
            if remaining_quantity <= 0:
                continue
            if lot.symbol not in pending_sell_intents_by_symbol:
                pending_sell_intents_by_symbol[lot.symbol] = self._repository.list_open_sell_intents(
                    strategy_id,
                    symbol=lot.symbol,
                    trade_date=trade_date,
                )
            available_quantity = effective_strategy_available_sell_quantity(
                lots=[lot],
                pending_sell_intents=pending_sell_intents_by_symbol[lot.symbol],
                as_of_date=trade_date,
                calendar=self._calendar_provider,
            )
            pending_sell_intents_by_symbol[lot.symbol] = []
            if existing is None:
                summaries[lot.symbol] = _PositionSummary(
                    symbol=lot.symbol,
                    remaining_quantity=remaining_quantity,
                    available_quantity=available_quantity,
                    lot_count=1,
                    reference_price=lot_reference_price,
                    reference_price_source=lot_reference_price_source,
                )
                continue
            summaries[lot.symbol] = _PositionSummary(
                symbol=lot.symbol,
                remaining_quantity=existing.remaining_quantity + remaining_quantity,
                available_quantity=existing.available_quantity + available_quantity,
                lot_count=existing.lot_count + 1,
                reference_price=existing.reference_price or lot_reference_price,
                reference_price_source=existing.reference_price_source or lot_reference_price_source,
            )
        return summaries

    def _append_buy_request(
        self,
        requests: list[ManagedOrderRequest],
        skipped: list[dict[str, Any]],
        *,
        account: VirtualAccount,
        binding: StrategyPackageBinding,
        selection_run: SelectionRun,
        candidate: SelectionCandidate,
        config: SelectionOrderBuildConfig,
        raw_quantity: int,
        target_quantity: int,
        current_quantity: int,
    ) -> None:
        quantity = round_to_board_lot(raw_quantity, candidate.symbol, side="BUY")
        if quantity <= 0:
            skipped.append(
                {
                    "symbol": candidate.symbol,
                    "reason": "BOARD_LOT_ROUNDED_TO_ZERO",
                    "side": "BUY",
                    "raw_quantity": raw_quantity,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                }
            )
            return
        if quantity < raw_quantity:
            skipped.append(
                {
                    "symbol": candidate.symbol,
                    "reason": "BUY_BOARD_LOT_RESIDUAL_NOT_EMITTED",
                    "raw_quantity": raw_quantity,
                    "emitted_quantity": quantity,
                    "residual_quantity": raw_quantity - quantity,
                }
            )
        price = _price_with_slippage(Decimal(str(candidate.reference_price)), config.buy_price_slippage_bps, "BUY")
        requests.append(
            self._managed_order_request(
                account=account,
                binding=binding,
                selection_run=selection_run,
                config=config,
                symbol=candidate.symbol,
                side="BUY",
                order_type=BUY_ORDER_TYPE,
                quantity=quantity,
                price=price,
                target_weight=_candidate_weight(candidate, binding, config),
                metadata={
                    "rank": candidate.rank,
                    "score": candidate.score,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                    "requested_quantity": raw_quantity,
                    "price_source": "selection_reference_price",
                },
            )
        )

    def _append_sell_request(
        self,
        requests: list[ManagedOrderRequest],
        skipped: list[dict[str, Any]],
        *,
        account: VirtualAccount,
        binding: StrategyPackageBinding,
        selection_run: SelectionRun,
        config: SelectionOrderBuildConfig,
        symbol: str,
        requested_quantity: int,
        available_quantity: int,
        current_quantity: int,
        target_quantity: int,
        price: Decimal,
        price_source: str,
        target_weight: Decimal | None,
        metadata: dict[str, Any],
        no_available_reason: str,
    ) -> None:
        if available_quantity <= 0:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": no_available_reason,
                    "side": "SELL",
                    "requested_quantity": requested_quantity,
                    "current_quantity": current_quantity,
                    "target_quantity": target_quantity,
                    "available_quantity": available_quantity,
                }
            )
            return

        raw_quantity = min(requested_quantity, available_quantity)
        if raw_quantity < requested_quantity:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "SELL_QUANTITY_CAPPED_BY_AVAILABLE_LOT",
                    "requested_quantity": requested_quantity,
                    "emittable_quantity": raw_quantity,
                    "blocked_quantity": requested_quantity - raw_quantity,
                    "current_quantity": current_quantity,
                    "target_quantity": target_quantity,
                    "available_quantity": available_quantity,
                }
            )

        quantity = round_to_board_lot(raw_quantity, symbol, side="SELL")
        if quantity <= 0:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "BOARD_LOT_ROUNDED_TO_ZERO",
                    "side": "SELL",
                    "raw_quantity": raw_quantity,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                }
            )
            return
        if quantity < raw_quantity:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "SELL_BOARD_LOT_RESIDUAL_DEFERRED",
                    "raw_quantity": raw_quantity,
                    "emitted_quantity": quantity,
                    "residual_quantity": raw_quantity - quantity,
                }
            )

        requests.append(
            self._managed_order_request(
                account=account,
                binding=binding,
                selection_run=selection_run,
                config=config,
                symbol=symbol,
                side="SELL",
                order_type=SELL_ORDER_TYPE,
                quantity=quantity,
                price=price,
                target_weight=target_weight,
                metadata={
                    **metadata,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                    "available_quantity": available_quantity,
                    "requested_quantity": requested_quantity,
                    "price_source": price_source,
                },
            )
        )

    def _managed_order_request(
        self,
        *,
        account: VirtualAccount,
        binding: StrategyPackageBinding,
        selection_run: SelectionRun,
        config: SelectionOrderBuildConfig,
        symbol: str,
        side: str,
        order_type: int,
        quantity: int,
        price: Decimal,
        target_weight: Decimal | None,
        metadata: dict[str, Any],
    ) -> ManagedOrderRequest:
        return ManagedOrderRequest(
            account_id=account.account_id,
            strategy_name=account.strategy_name,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price_type=config.price_type,
            price=price,
            order_remark=_order_remark(config.order_remark_prefix, account.strategy_name, selection_run.run_id, symbol),
            trade_date=selection_run.trade_date,
            mode=config.mode,
            package_id=binding.package_id,
            selection_run_id=selection_run.run_id,
            target_weight=target_weight,
            metadata=metadata,
        )

    def _dropped_sell_price(
        self,
        position: _PositionSummary,
        config: SelectionOrderBuildConfig,
    ) -> tuple[Decimal | None, str]:
        if position.reference_price is not None:
            return _price_with_slippage(position.reference_price, config.sell_price_slippage_bps, "SELL"), (
                position.reference_price_source or "position_metadata"
            )
        if config.price_type == LATEST_PRICE_TYPE:
            return Decimal("0"), "latest_price_type"
        return None, "missing"

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


def _lot_reference_price(lot: Any) -> tuple[Decimal | None, str | None]:
    metadata = getattr(lot, "metadata", None) or {}
    for key in ("reference_price", "last_price", "market_price", "close_price"):
        price = _positive_decimal(metadata.get(key))
        if price is not None:
            return price, f"position_lot.metadata.{key}"
    return None, None


def _positive_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return price if price > 0 else None


def _order_remark(prefix: str, strategy_name: str, selection_run_id: str, symbol: str) -> str:
    safe_strategy = "".join(ch for ch in strategy_name if ch.isalnum() or ch in {"_", "-"})[:24]
    safe_run = selection_run_id.replace("sel_", "")[:10]
    safe_symbol = symbol.replace(".", "")
    return f"{prefix}_{safe_strategy}_{safe_run}_{safe_symbol}"
