"""Build managed MiniQMT order requests from Selection Center results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.selection_center.models import SelectionCandidate, SelectionRun, SelectionRunStatus, SignalSnapshot
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.runtime import RebalanceEngine, TargetPositionEngine
from backend.services.strategy_package.selection_artifact import selection_artifact_runtime_hash
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, UnsupportedFeatureError
from backend.services.trading_core.models import OrderIntent, OrderSide, PositionLot

from .lot_availability import (
    DbTradingCalendarProvider,
    TradingCalendarProvider,
    effective_strategy_available_sell_quantity,
)
from .models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    BindingStatus,
    StrategyBindingSelectionEvidence,
    StrategyPackageBinding,
    VirtualAccount,
)
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
    daily_selection_evidence_id: str
    runtime_config_hash: str
    requests: tuple[ManagedOrderRequest, ...]
    skipped: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "package_id": self.package_id,
            "selection_run_id": self.selection_run_id,
            "trade_date": self.trade_date.isoformat(),
            "daily_selection_evidence_id": self.daily_selection_evidence_id,
            "runtime_config_hash": self.runtime_config_hash,
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
        allow_legacy_direct_order_generation: bool = False,
    ) -> None:
        self._repository = repository
        self._selection_reader = selection_reader
        self._calendar_provider = calendar_provider or DbTradingCalendarProvider()
        self._allow_legacy_direct_order_generation = allow_legacy_direct_order_generation
        self._target_engine = TargetPositionEngine()
        self._rebalance_engine = RebalanceEngine()

    def build_for_active_binding(
        self,
        *,
        strategy_id: str,
        trade_date: date | None = None,
        config: SelectionOrderBuildConfig | None = None,
    ) -> SelectionOrderBuildResult:
        binding = self._repository.get_active_package_binding(strategy_id)
        if binding is None:
            raise DataUnavailableError("active package binding does not exist", context={"strategy_id": strategy_id})
        return self.build_for_binding(
            binding=binding,
            trade_date=trade_date,
            config=config or SelectionOrderBuildConfig(),
        )

    def build_for_binding(
        self,
        *,
        binding: StrategyPackageBinding,
        trade_date: date | None = None,
        config: SelectionOrderBuildConfig | None = None,
    ) -> SelectionOrderBuildResult:
        config = config or SelectionOrderBuildConfig()
        if not self._allow_legacy_direct_order_generation:
            self._raise_execution_bridge_required(binding)
        if binding.binding_status != BindingStatus.ACTIVE:
            raise StrategyPackageValidationError(
                "package binding is not ACTIVE",
                context={"binding_id": binding.binding_id, "binding_status": binding.binding_status.value},
            )
        requested_trade_date = trade_date or date.today()
        account = self._repository.get_virtual_account(binding.strategy_id)
        evidence = self._repository.get_binding_selection_evidence(binding.binding_id, requested_trade_date)
        selection_run: SelectionRun = self._selection_run_from_evidence(binding, evidence, requested_trade_date)
        self._require_daily_selection_evidence_asset(binding, evidence)

        candidates = self._candidates_for_binding(selection_run, binding)
        top_k = config.top_k or binding.top_k
        if top_k is not None:
            candidates = candidates[:top_k]
        if not candidates:
            raise DataUnavailableError(
                "selection run has no candidates for active binding",
                context={"package_id": binding.package_id, "selection_run_id": selection_run.run_id},
            )

        position_summaries = self._position_summaries(account.strategy_id, selection_run.trade_date)
        snapshot = self._signal_snapshot_for_binding(selection_run, binding, candidates)
        current_positions = self._trading_core_positions(account.strategy_id, selection_run.trade_date, position_summaries)
        target_positions = self._target_engine.build_targets(
            snapshot=snapshot,
            total_equity=float(account.initial_cash),
            top_k=len(candidates),
            current_positions=current_positions,
            current_prices=self._current_prices(candidates, position_summaries),
            default_target_weight=_float_or_none(_first_not_none(binding.target_weight, config.default_target_weight)),
        )
        intents = self._rebalance_engine.build_order_intents(
            package_id=binding.package_id,
            portfolio_id=account.strategy_id,
            trade_date=selection_run.trade_date,
            current_positions=current_positions,
            target_positions=target_positions,
        )
        requests: list[ManagedOrderRequest] = []
        skipped: list[dict[str, Any]] = self._no_change_skips(
            target_positions=target_positions,
            position_summaries=position_summaries,
        )
        self._append_requests_from_intents(
            requests,
            skipped,
            account=account,
            binding=binding,
            selection_run=selection_run,
            config=config,
            intents=intents,
            candidates={candidate.symbol: candidate for candidate in candidates},
            position_summaries=position_summaries,
        )

        return SelectionOrderBuildResult(
            strategy_id=account.strategy_id,
            strategy_name=account.strategy_name,
            package_id=binding.package_id,
            selection_run_id=selection_run.run_id,
            trade_date=selection_run.trade_date,
            daily_selection_evidence_id=evidence.evidence_id,
            runtime_config_hash=evidence.runtime_config_hash,
            requests=tuple(requests),
            skipped=tuple(skipped),
        )

    def _selection_run_from_evidence(
        self,
        binding: StrategyPackageBinding,
        evidence: StrategyBindingSelectionEvidence,
        requested_trade_date: date,
    ) -> SelectionRun:
        if evidence.binding_id != binding.binding_id:
            raise StrategyPackageValidationError(
                "daily selection evidence does not belong to active binding",
                context={"binding_id": binding.binding_id, "evidence_binding_id": evidence.binding_id},
            )
        if evidence.strategy_id != binding.strategy_id or evidence.package_id != binding.package_id:
            raise StrategyPackageValidationError(
                "daily selection evidence strategy/package identity does not match active binding",
                context={
                    "binding_id": binding.binding_id,
                    "evidence_strategy_id": evidence.strategy_id,
                    "binding_strategy_id": binding.strategy_id,
                    "evidence_package_id": evidence.package_id,
                    "binding_package_id": binding.package_id,
                },
            )
        if evidence.trade_date != requested_trade_date:
            raise DataUnavailableError(
                "daily selection evidence trade_date does not match requested trade_date",
                context={
                    "binding_id": binding.binding_id,
                    "requested_trade_date": requested_trade_date.isoformat(),
                    "evidence_trade_date": evidence.trade_date.isoformat(),
                },
            )
        selection_run = self._selection_reader.get_run(evidence.selection_run_id)
        if selection_run.status != SelectionRunStatus.SUCCEEDED:
            raise DataUnavailableError(
                "selection run is not succeeded",
                context={"selection_run_id": evidence.selection_run_id, "status": selection_run.status.value},
            )
        if selection_run.trade_date != requested_trade_date:
            raise DataUnavailableError(
                "selection run is stale for requested trade_date",
                context={
                    "selection_run_id": selection_run.run_id,
                    "selection_trade_date": selection_run.trade_date.isoformat(),
                    "requested_trade_date": requested_trade_date.isoformat(),
                },
            )
        if selection_run.data_source != evidence.data_source:
            raise DataUnavailableError(
                "selection run data_source does not match daily evidence",
                context={
                    "selection_run_id": selection_run.run_id,
                    "selection_data_source": selection_run.data_source,
                    "evidence_data_source": evidence.data_source,
                },
            )
        if binding.package_id not in selection_run.package_ids:
            raise StrategyPackageValidationError(
                "selection run does not contain active binding package",
                context={"package_id": binding.package_id, "selection_run_id": selection_run.run_id},
            )
        selection_manifest = selection_run.manifest_sha256_by_package.get(binding.package_id)
        if selection_manifest != binding.manifest_sha256 or evidence.manifest_sha256 != binding.manifest_sha256:
            raise StrategyPackageValidationError(
                "selection run manifest hash does not match active binding",
                context={
                    "package_id": binding.package_id,
                    "binding_manifest_sha256": binding.manifest_sha256,
                    "selection_manifest_sha256": selection_manifest,
                    "evidence_manifest_sha256": evidence.manifest_sha256,
                },
            )
        selection_runtime_hash = selection_artifact_runtime_hash(selection_run.runtime_config)
        if selection_runtime_hash != evidence.runtime_config_hash:
            raise DataUnavailableError(
                "selection run runtime hash does not match daily evidence",
                context={
                    "selection_run_id": selection_run.run_id,
                    "selection_runtime_config_hash": selection_runtime_hash,
                    "evidence_runtime_config_hash": evidence.runtime_config_hash,
                    "asset_stage": "daily_order_build",
                },
            )
        return selection_run

    def _candidates_for_binding(
        self,
        selection_run: SelectionRun,
        binding: StrategyPackageBinding,
    ) -> list[SelectionCandidate]:
        if selection_run.aggregate_results:
            return sorted(selection_run.aggregate_results, key=lambda candidate: candidate.rank)
        return sorted(selection_run.package_results.get(binding.package_id, []), key=lambda candidate: candidate.rank)

    def _require_daily_selection_evidence_asset(
        self,
        binding: StrategyPackageBinding,
        evidence: StrategyBindingSelectionEvidence,
    ) -> None:
        if not any((evidence.artifact_id, evidence.artifact_sha256, evidence.source_type, evidence.authority_scope)):
            return
        missing = [key for key in ("artifact_id", "artifact_sha256", "runtime_config_hash", "source_type", "authority_scope") if not getattr(evidence, key)]
        if missing:
            raise DataUnavailableError(
                "frozen MiniQMT daily selection evidence is incomplete; resolve current-day SelectionRun before daily execution",
                context={"binding_id": binding.binding_id, "missing": missing, "asset_stage": "daily_order_build"},
            )
        if evidence.manifest_sha256 != binding.manifest_sha256:
            raise DataUnavailableError(
                "frozen MiniQMT daily selection evidence manifest hash does not match active binding",
                context={
                    "binding_id": binding.binding_id,
                    "binding_manifest_sha256": binding.manifest_sha256,
                    "asset_manifest_sha256": evidence.manifest_sha256,
                    "asset_stage": "daily_order_build",
                },
            )
        if evidence.source_type != AUTHORITATIVE_SELECTION_SOURCE_TYPE or evidence.authority_scope != AUTHORITATIVE_SELECTION_SCOPE:
            raise DataUnavailableError(
                "frozen MiniQMT daily selection evidence is not authoritative live inference output",
                context={
                    "binding_id": binding.binding_id,
                    "evidence_id": evidence.evidence_id,
                    "source_type": evidence.source_type,
                    "authority_scope": evidence.authority_scope,
                    "required_source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "required_authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                    "asset_stage": "daily_order_build",
                },
            )

    def _raise_execution_bridge_required(self, binding: StrategyPackageBinding) -> None:
        raise UnsupportedFeatureError(
            "MiniQMT StrategyPackage execution bridge is required; "
            "SelectionOrderBuilder direct broker-order generation is disabled",
            context={
                "issue": "BUG-077",
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "package_id": binding.package_id,
                "selection_run_id": binding.selection_run_id,
                "trade_date": binding.trade_date.isoformat() if binding.trade_date else None,
                "disabled_path": "SelectionRun -> SelectionOrderBuilder -> ManagedOrderRequest",
                "required_path": (
                    "StrategyPackage alpha core -> daily target/rebalance intent -> "
                    "validated execution policy -> MiniQMT execution bridge -> ManagedOrderRequest"
                ),
                "reason": "selection_order_builder_bypasses_validated_execution_policy",
            },
        )
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

    def _signal_snapshot_for_binding(
        self,
        selection_run: SelectionRun,
        binding: StrategyPackageBinding,
        candidates: list[SelectionCandidate],
    ) -> SignalSnapshot:
        return SignalSnapshot(
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            trade_date=selection_run.trade_date,
            data_source=selection_run.data_source,
            candidates=candidates,
            runtime_config={
                "source": "qmt_strategy_ledger.selection_order_builder",
                "selection_run_id": selection_run.run_id,
                "binding_id": binding.binding_id,
            },
        )

    def _trading_core_positions(
        self,
        strategy_id: str,
        trade_date: date,
        position_summaries: dict[str, _PositionSummary],
    ) -> dict[str, PositionLot]:
        return {
            symbol: PositionLot(
                portfolio_id=strategy_id,
                symbol=symbol,
                quantity=summary.remaining_quantity,
                available_quantity=summary.available_quantity,
                avg_cost=float(summary.reference_price or Decimal("0")),
                trade_date=trade_date,
            )
            for symbol, summary in position_summaries.items()
        }

    @staticmethod
    def _current_prices(
        candidates: list[SelectionCandidate],
        position_summaries: dict[str, _PositionSummary],
    ) -> dict[str, float]:
        prices: dict[str, float] = {}
        for candidate in candidates:
            if candidate.reference_price is not None:
                prices[candidate.symbol] = float(candidate.reference_price)
        for symbol, summary in position_summaries.items():
            if summary.reference_price is not None:
                prices.setdefault(symbol, float(summary.reference_price))
        return prices

    @staticmethod
    def _no_change_skips(
        *,
        target_positions: list[Any],
        position_summaries: dict[str, _PositionSummary],
    ) -> list[dict[str, Any]]:
        skipped: list[dict[str, Any]] = []
        for target in target_positions:
            current_position = position_summaries.get(target.symbol)
            current_quantity = current_position.remaining_quantity if current_position is not None else 0
            if target.target_quantity != current_quantity:
                continue
            skipped.append(
                {
                    "symbol": target.symbol,
                    "reason": "ALREADY_AT_TARGET",
                    "target_quantity": target.target_quantity,
                    "current_quantity": current_quantity,
                    "available_quantity": current_position.available_quantity if current_position is not None else 0,
                    "decision_engine": "RebalanceEngine",
                }
            )
        return skipped

    def _append_requests_from_intents(
        self,
        requests: list[ManagedOrderRequest],
        skipped: list[dict[str, Any]],
        *,
        account: VirtualAccount,
        binding: StrategyPackageBinding,
        selection_run: SelectionRun,
        config: SelectionOrderBuildConfig,
        intents: list[OrderIntent],
        candidates: dict[str, SelectionCandidate],
        position_summaries: dict[str, _PositionSummary],
    ) -> None:
        for intent in intents:
            target_quantity = int(intent.metadata.get("target_quantity") or 0)
            current_quantity = int(intent.metadata.get("current_quantity") or 0)
            requested_quantity = int(intent.metadata.get("requested_quantity") or intent.quantity)
            if intent.side == OrderSide.BUY:
                candidate = candidates.get(intent.symbol)
                if candidate is None:
                    raise StrategyPackageValidationError(
                        "shared decision engine emitted BUY for a symbol outside Selection Run",
                        context={"symbol": intent.symbol, "selection_run_id": selection_run.run_id},
                    )
                self._append_buy_request(
                    requests,
                    skipped,
                    account=account,
                    binding=binding,
                    selection_run=selection_run,
                    candidate=candidate,
                    config=config,
                    intent=intent,
                    raw_quantity=requested_quantity,
                    target_quantity=target_quantity,
                    current_quantity=current_quantity,
                )
                continue
            position = position_summaries.get(intent.symbol)
            available_quantity = position.available_quantity if position is not None else 0
            price, price_source = self._sell_price_for_intent(intent, candidates, position, config)
            no_available_reason = (
                "NO_AVAILABLE_LOT_FOR_DROPPED_HOLDING"
                if intent.metadata.get("rebalance_reason") == "DROPPED_FROM_SELECTION"
                else "NO_AVAILABLE_LOT"
            )
            self._append_sell_request(
                requests,
                skipped,
                account=account,
                binding=binding,
                selection_run=selection_run,
                config=config,
                symbol=intent.symbol,
                requested_quantity=requested_quantity,
                available_quantity=available_quantity,
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                price=price,
                price_source=price_source,
                target_weight=_target_weight_from_intent(intent),
                metadata={
                    **intent.metadata,
                    "shared_order_intent_id": intent.intent_id,
                    "decision_engine": "RebalanceEngine",
                },
                no_available_reason=no_available_reason,
            )

    def _sell_price_for_intent(
        self,
        intent: OrderIntent,
        candidates: dict[str, SelectionCandidate],
        position: _PositionSummary | None,
        config: SelectionOrderBuildConfig,
    ) -> tuple[Decimal | None, str]:
        candidate = candidates.get(intent.symbol)
        if candidate is not None and candidate.reference_price is not None and candidate.reference_price > 0:
            return (
                _price_with_slippage(Decimal(str(candidate.reference_price)), config.sell_price_slippage_bps, "SELL"),
                "selection_reference_price",
            )
        if position is not None:
            return self._dropped_sell_price(position, config)
        if config.price_type == LATEST_PRICE_TYPE:
            return Decimal("0"), "latest_price_type"
        return None, "missing"

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
        intent: OrderIntent,
        raw_quantity: int,
        target_quantity: int,
        current_quantity: int,
    ) -> None:
        quantity = intent.quantity
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
                    **intent.metadata,
                    "rank": candidate.rank,
                    "score": candidate.score,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                    "requested_quantity": raw_quantity,
                    "price_source": "selection_reference_price",
                    "shared_order_intent_id": intent.intent_id,
                    "decision_engine": "RebalanceEngine",
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
        price: Decimal | None,
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

        if price is None and config.price_type != LATEST_PRICE_TYPE:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "MISSING_REFERENCE_PRICE_FOR_DROPPED_HOLDING",
                    "current_quantity": current_quantity,
                    "available_quantity": available_quantity,
                    "price_type": config.price_type,
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
                    "adapter_adjustment": "T_PLUS_ONE_AVAILABLE_LOT_CAP",
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
                price=price or Decimal("0"),
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


def _target_weight_from_intent(intent: OrderIntent) -> Decimal | None:
    value = intent.metadata.get("target_weight")
    if value is None:
        return None
    return Decimal(str(value))


def _float_or_none(value: Decimal | float | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _first_not_none(*values: Decimal | None) -> Decimal | None:
    for value in values:
        if value is not None:
            return value
    return None

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



