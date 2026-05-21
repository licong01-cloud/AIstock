"""Shared target, rebalance, trading-rule and execution-plan services.

This module is broker-neutral. LocalSim and MiniQMT must both consume the
objects produced here; broker adapters may submit/cancel/sync, but must not
recompute strategy targets or re-implement A-share board-lot rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from backend.execution_algos.board_lot import board_lot_rule, round_to_board_lot
from backend.services.selection_center.models import SignalSnapshot, TargetPosition
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, PositionLot

from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    ExecutionPlanIntent,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    TradingRuleDecision,
    canonical_json_sha256,
)


TRADING_RULE_SOURCE_VERSION = "a_share_board_lot_v20260504"


@dataclass(frozen=True)
class RebalanceIntentResult:
    order_intents: list[OrderIntent]
    trading_rule_decisions: list[TradingRuleDecision]


class TargetPositionService:
    """Build shared target positions from daily selection evidence."""

    def __init__(self, *, target_engine: TargetPositionEngine | None = None) -> None:
        self.target_engine = target_engine or TargetPositionEngine()

    def build_target_positions(
        self,
        *,
        selection_evidence: DailySelectionEvidence,
        signal_snapshot: SignalSnapshot,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding | None = None,
        total_equity: float | None = None,
        top_k: int | None = None,
        manifest: StrategyPackageManifest | None = None,
        current_positions: dict[str, PositionLot] | None = None,
        current_prices: dict[str, float] | None = None,
    ) -> list[TargetPosition]:
        self._validate_signal_identity(
            selection_evidence=selection_evidence,
            signal_snapshot=signal_snapshot,
            runtime_release=runtime_release,
        )
        equity = float(total_equity if total_equity is not None else binding.capital_allocation if binding else 0)
        if equity <= 0:
            raise StrategyPackageValidationError(
                "TargetPositionService requires positive total_equity or binding capital_allocation",
                context={"package_id": runtime_release.package_id, "release_id": runtime_release.release_id},
            )
        effective_top_k = int(top_k or len(signal_snapshot.candidates))
        targets = self.target_engine.build_targets(
            snapshot=signal_snapshot,
            total_equity=equity,
            top_k=effective_top_k,
            manifest=manifest,
            current_positions=current_positions or {},
            current_prices=current_prices or {},
        )
        return [
            target.model_copy(
                update={
                    "metadata": {
                        **target.metadata,
                        "daily_selection_evidence_id": selection_evidence.evidence_id,
                        "daily_selection_evidence_hash": selection_evidence.artifact_hash,
                        "release_id": runtime_release.release_id,
                        "release_hash": runtime_release.release_hash,
                        "daily_strategy_profile_version_id": runtime_release.daily_strategy_profile_version_id,
                    }
                }
            )
            for target in targets
        ]

    @staticmethod
    def _validate_signal_identity(
        *,
        selection_evidence: DailySelectionEvidence,
        signal_snapshot: SignalSnapshot,
        runtime_release: StrategyRuntimeRelease,
    ) -> None:
        context = {
            "evidence_id": selection_evidence.evidence_id,
            "snapshot_id": signal_snapshot.snapshot_id,
            "release_id": runtime_release.release_id,
        }
        if selection_evidence.package_id != signal_snapshot.package_id:
            raise StrategyPackageValidationError("selection evidence package does not match signal snapshot", context=context)
        if selection_evidence.package_id != runtime_release.package_id:
            raise StrategyPackageValidationError("selection evidence package does not match runtime release", context=context)
        if selection_evidence.manifest_sha256 != signal_snapshot.manifest_sha256:
            raise StrategyPackageValidationError("selection evidence manifest does not match signal snapshot", context=context)
        if selection_evidence.manifest_sha256 != runtime_release.manifest_sha256:
            raise StrategyPackageValidationError("selection evidence manifest does not match runtime release", context=context)
        if selection_evidence.release_id and selection_evidence.release_id != runtime_release.release_id:
            raise StrategyPackageValidationError("selection evidence release_id does not match runtime release", context=context)
        if selection_evidence.release_hash and selection_evidence.release_hash != runtime_release.release_hash:
            raise StrategyPackageValidationError("selection evidence release_hash does not match runtime release", context=context)


class TradingRuleService:
    """Authoritative A-share trading-rule decision service for simulation runtimes."""

    source_version = TRADING_RULE_SOURCE_VERSION

    def decide_order_quantity(
        self,
        *,
        symbol: str,
        side: OrderSide | str,
        requested_quantity: int,
        tplus1_available_quantity: int | None = None,
        price_limit_rule: dict[str, Any] | None = None,
    ) -> TradingRuleDecision:
        side_value = side if isinstance(side, OrderSide) else OrderSide(str(side).upper())
        requested = int(requested_quantity)
        min_qty, increment = board_lot_rule(symbol)
        market_board = self.market_board(symbol)
        available = int(tplus1_available_quantity) if tplus1_available_quantity is not None else None

        if requested <= 0:
            return self._build_decision(
                symbol=symbol,
                market_board=market_board,
                side=side_value,
                requested_quantity=max(requested, 0),
                legal_quantity=0,
                lot_rule={"min_quantity": min_qty, "increment": increment},
                price_limit_rule=price_limit_rule or {},
                tplus1_available_quantity=available,
                decision="REJECT",
                reason_code="INVALID_QUANTITY",
            )

        emittable = requested
        reason_code = "OK"
        if side_value == OrderSide.SELL and available is not None and available < requested:
            emittable = max(available, 0)
            reason_code = "TPLUS1_AVAILABLE_PARTIAL" if emittable > 0 else "TPLUS1_AVAILABLE_ZERO"

        legal_quantity = round_to_board_lot(emittable, symbol, side=side_value.value)
        if legal_quantity <= 0:
            decision = "REJECT"
            reason_code = reason_code if reason_code != "OK" else "BOARD_LOT_ZERO"
        elif legal_quantity == requested and reason_code == "OK":
            decision = "EMIT"
        else:
            decision = "ADJUST"
            if reason_code == "OK":
                reason_code = "BOARD_LOT_ADJUSTED"

        return self._build_decision(
            symbol=symbol,
            market_board=market_board,
            side=side_value,
            requested_quantity=requested,
            legal_quantity=legal_quantity,
            lot_rule={"min_quantity": min_qty, "increment": increment},
            price_limit_rule=price_limit_rule or {},
            tplus1_available_quantity=available,
            decision=decision,
            reason_code=reason_code,
        )

    @staticmethod
    def market_board(symbol: str) -> str:
        code = str(symbol).split(".")[0].strip()
        if code.startswith(("688", "689")):
            return "STAR"
        if code.startswith(("300", "301", "302")):
            return "CHINEXT"
        if code.startswith(("60", "00")):
            return "MAIN"
        return "UNKNOWN"

    def _build_decision(
        self,
        *,
        symbol: str,
        market_board: str,
        side: OrderSide,
        requested_quantity: int,
        legal_quantity: int,
        lot_rule: dict[str, Any],
        price_limit_rule: dict[str, Any],
        tplus1_available_quantity: int | None,
        decision: str,
        reason_code: str,
    ) -> TradingRuleDecision:
        payload = {
            "schema_version": "trading_rule_decision_v1",
            "symbol": symbol,
            "market_board": market_board,
            "side": side.value,
            "requested_quantity": requested_quantity,
            "legal_quantity": legal_quantity,
            "lot_rule": lot_rule,
            "price_limit_rule": price_limit_rule,
            "tplus1_available_quantity": tplus1_available_quantity,
            "decision": decision,
            "reason_code": reason_code,
            "source_version": self.source_version,
        }
        digest = canonical_json_sha256(payload)
        return TradingRuleDecision(
            decision_id=f"trd_{digest[:16]}",
            symbol=symbol,
            market_board=market_board,
            side=side,
            requested_quantity=requested_quantity,
            legal_quantity=legal_quantity,
            lot_rule=lot_rule,
            price_limit_rule=price_limit_rule,
            tplus1_available_quantity=tplus1_available_quantity,
            decision=decision,  # type: ignore[arg-type]
            reason_code=reason_code,
            source_version=self.source_version,
            decision_hash=digest,
        )


class RebalanceIntentService:
    """Diff current strategy positions and shared targets into order intents."""

    def __init__(self, *, trading_rule_service: TradingRuleService | None = None) -> None:
        self.trading_rule_service = trading_rule_service or TradingRuleService()

    def build_order_intents(
        self,
        *,
        package_id: str,
        portfolio_id: str,
        strategy_id: str,
        trade_date: date,
        current_positions: dict[str, PositionLot],
        target_positions: list[TargetPosition],
    ) -> RebalanceIntentResult:
        if not target_positions:
            raise StrategyPackageValidationError(
                "RebalanceIntentService requires target positions",
                context={"package_id": package_id, "portfolio_id": portfolio_id, "strategy_id": strategy_id},
            )
        target_by_symbol = {target.symbol: target for target in target_positions}
        symbols = sorted(set(current_positions) | set(target_by_symbol))
        order_intents: list[OrderIntent] = []
        trading_rule_decisions: list[TradingRuleDecision] = []

        for symbol in symbols:
            position = current_positions.get(symbol)
            current_quantity = int(position.quantity) if position is not None else 0
            current_available = int(position.available_quantity) if position is not None else None
            target = target_by_symbol.get(symbol)
            target_quantity = int(target.target_quantity) if target is not None else 0
            delta_quantity = target_quantity - current_quantity
            if delta_quantity == 0:
                continue
            side = OrderSide.BUY if delta_quantity > 0 else OrderSide.SELL
            requested_quantity = abs(delta_quantity)
            decision = self.trading_rule_service.decide_order_quantity(
                symbol=symbol,
                side=side,
                requested_quantity=requested_quantity,
                tplus1_available_quantity=current_available if side == OrderSide.SELL else None,
            )
            trading_rule_decisions.append(decision)
            if decision.legal_quantity <= 0 or decision.decision == "REJECT":
                continue

            rebalance_reason = target.reason if target is not None else "DROPPED_FROM_SELECTION"
            intent_payload = {
                "schema_version": "shared_rebalance_intent_v1",
                "package_id": package_id,
                "portfolio_id": portfolio_id,
                "strategy_id": strategy_id,
                "trade_date": trade_date.isoformat(),
                "symbol": symbol,
                "side": side.value,
                "quantity": decision.legal_quantity,
                "target_quantity": target_quantity,
                "current_quantity": current_quantity,
                "current_available_quantity": current_available,
                "requested_quantity": requested_quantity,
                "trading_rule_decision_id": decision.decision_id,
                "rebalance_reason": rebalance_reason,
            }
            intent_hash = canonical_json_sha256(intent_payload)
            order_intents.append(
                OrderIntent(
                    intent_id=f"intent_{intent_hash[:16]}",
                    package_id=package_id,
                    portfolio_id=portfolio_id,
                    symbol=symbol,
                    side=side,
                    quantity=decision.legal_quantity,
                    order_type=OrderType.MARKET,
                    target_trade_date=trade_date,
                    metadata={
                        "strategy_id": strategy_id,
                        "target_quantity": target_quantity,
                        "current_quantity": current_quantity,
                        "current_available_quantity": current_available,
                        "requested_quantity": requested_quantity,
                        "delta_quantity": delta_quantity,
                        "target_weight": target.target_weight if target is not None else None,
                        "rebalance_reason": rebalance_reason,
                        "target_metadata": target.metadata if target is not None else {},
                        "trading_rule_decision_id": decision.decision_id,
                        "trading_rule_decision_hash": decision.decision_hash,
                        "trading_rule_reason_code": decision.reason_code,
                        "shared_rebalance_intent_hash": intent_hash,
                    },
                )
            )
        return RebalanceIntentResult(order_intents=order_intents, trading_rule_decisions=trading_rule_decisions)


class ExecutionPlanCompiler:
    """Compile shared rebalance intents into an immutable broker-neutral plan."""

    def compile_plan(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        selection_evidence: DailySelectionEvidence,
        order_intents: list[OrderIntent],
        trading_rule_decisions: list[TradingRuleDecision],
        portfolio_id: str | None = None,
        execution_policy_payload: dict[str, Any] | None = None,
        tail_policy_payload: dict[str, Any] | None = None,
    ) -> ExecutionPlan:
        self._validate_identity(runtime_release=runtime_release, binding=binding, selection_evidence=selection_evidence)
        if not order_intents:
            raise StrategyPackageValidationError(
                "ExecutionPlanCompiler requires at least one shared order intent",
                context={"release_id": runtime_release.release_id, "binding_id": binding.binding_id},
            )
        execution_policy = dict(execution_policy_payload or runtime_release.release_config_json.get("execution_policy") or {})
        tail_policy = dict(tail_policy_payload or runtime_release.release_config_json.get("tail_policy") or {})
        self._reject_paper_only_policy(execution_policy)
        decision_by_id = {decision.decision_id: decision for decision in trading_rule_decisions}
        missing = [
            intent.metadata.get("trading_rule_decision_id")
            for intent in order_intents
            if intent.metadata.get("trading_rule_decision_id") not in decision_by_id
        ]
        if missing:
            raise StrategyPackageValidationError(
                "ExecutionPlanCompiler requires every intent to reference a TradingRuleDecision",
                context={"missing_trading_rule_decision_ids": missing},
            )
        effective_portfolio_id = str(portfolio_id or order_intents[0].portfolio_id or binding.strategy_id).strip()
        if not effective_portfolio_id:
            raise StrategyPackageValidationError(
                "ExecutionPlanCompiler requires portfolio_id",
                context={"release_id": runtime_release.release_id, "binding_id": binding.binding_id},
            )

        schedule_window = execution_policy.get("schedule_window")
        if not isinstance(schedule_window, dict):
            schedule_window = {"mode": "full_day", "source": runtime_release.execution_policy_version_id}
        risk_context = execution_policy.get("risk_context") if isinstance(execution_policy.get("risk_context"), dict) else {}

        seed_intents: list[dict[str, Any]] = []
        for intent in order_intents:
            seed_intents.append(
                {
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "side": intent.side.value,
                    "target_quantity": int(intent.metadata.get("target_quantity") or 0),
                    "delta_quantity": int(intent.metadata.get("delta_quantity") or 0),
                    "order_quantity": intent.quantity,
                    "target_weight": intent.metadata.get("target_weight"),
                    "current_quantity": int(intent.metadata.get("current_quantity") or 0),
                    "current_available_quantity": intent.metadata.get("current_available_quantity"),
                    "rebalance_reason": str(intent.metadata.get("rebalance_reason") or ""),
                    "trading_rule_decision_id": str(intent.metadata["trading_rule_decision_id"]),
                    "order_type": intent.order_type.value,
                    "limit_price": intent.limit_price,
                }
            )
        seed_intents.sort(key=lambda item: (item["symbol"], item["side"], item["intent_id"]))
        payload = {
            "schema_version": "execution_plan_v1",
            "strategy_id": binding.strategy_id,
            "portfolio_id": effective_portfolio_id,
            "package_id": runtime_release.package_id,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "selection_evidence_id": selection_evidence.evidence_id,
            "selection_evidence_hash": selection_evidence.artifact_hash,
            "target_trade_date": selection_evidence.target_trade_date.isoformat(),
            "execution_policy": {
                "version_id": runtime_release.execution_policy_version_id,
                "sha256": runtime_release.execution_policy_sha256,
                "payload": execution_policy,
            },
            "tail_policy": {
                "version_id": runtime_release.tail_policy_version_id,
                "sha256": runtime_release.tail_policy_sha256,
                "payload": tail_policy,
            },
            "intents": seed_intents,
            "trading_rule_decision_ids": sorted(decision_by_id),
        }
        plan_hash = canonical_json_sha256(payload)
        plan_id = f"plan_{plan_hash[:16]}"
        plan_intents = [
            ExecutionPlanIntent(
                intent_id=item["intent_id"],
                plan_id=plan_id,
                strategy_id=binding.strategy_id,
                portfolio_id=effective_portfolio_id,
                package_id=runtime_release.package_id,
                release_id=runtime_release.release_id,
                release_hash=runtime_release.release_hash or "",
                binding_id=binding.binding_id,
                binding_hash=binding.binding_hash or "",
                symbol=item["symbol"],
                side=OrderSide(item["side"]),
                target_quantity=item["target_quantity"],
                delta_quantity=item["delta_quantity"],
                order_quantity=item["order_quantity"],
                target_weight=item["target_weight"],
                current_quantity=item["current_quantity"],
                current_available_quantity=item["current_available_quantity"],
                rebalance_reason=item["rebalance_reason"],
                trading_rule_decision_id=item["trading_rule_decision_id"],
                schedule_window=schedule_window,
                price_policy={"order_type": item["order_type"], "limit_price": item["limit_price"]},
                risk_context=risk_context,
                metadata={"source_order_intent_id": item["intent_id"]},
            )
            for item in seed_intents
        ]
        return ExecutionPlan(
            plan_id=plan_id,
            strategy_id=binding.strategy_id,
            portfolio_id=effective_portfolio_id,
            package_id=runtime_release.package_id,
            release_id=runtime_release.release_id,
            release_hash=runtime_release.release_hash or "",
            binding_id=binding.binding_id,
            binding_hash=binding.binding_hash or "",
            selection_evidence_id=selection_evidence.evidence_id,
            selection_evidence_hash=selection_evidence.artifact_hash,
            target_trade_date=selection_evidence.target_trade_date,
            execution_policy_version_id=runtime_release.execution_policy_version_id,
            execution_policy_sha256=runtime_release.execution_policy_sha256,
            tail_policy_version_id=runtime_release.tail_policy_version_id,
            tail_policy_sha256=runtime_release.tail_policy_sha256,
            intents=plan_intents,
            trading_rule_decisions=trading_rule_decisions,
            plan_payload_json=payload,
            plan_hash=plan_hash,
        )

    @staticmethod
    def _validate_identity(
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        selection_evidence: DailySelectionEvidence,
    ) -> None:
        context = {
            "release_id": runtime_release.release_id,
            "binding_id": binding.binding_id,
            "evidence_id": selection_evidence.evidence_id,
        }
        if binding.release_id != runtime_release.release_id or binding.release_hash != runtime_release.release_hash:
            raise StrategyPackageValidationError("simulation binding does not match runtime release", context=context)
        if selection_evidence.package_id != runtime_release.package_id:
            raise StrategyPackageValidationError("selection evidence does not match runtime release package", context=context)
        if selection_evidence.release_id and selection_evidence.release_id != runtime_release.release_id:
            raise StrategyPackageValidationError("selection evidence release_id does not match runtime release", context=context)
        if selection_evidence.release_hash and selection_evidence.release_hash != runtime_release.release_hash:
            raise StrategyPackageValidationError("selection evidence release_hash does not match runtime release", context=context)

    @staticmethod
    def _reject_paper_only_policy(policy: dict[str, Any]) -> None:
        algo_code = str(policy.get("algo_code") or policy.get("policy_version_id") or "").strip().lower()
        if bool(policy.get("paper_only")) or algo_code in {"paper_only", "selection_order_builder", "manual"}:
            raise StrategyPackageValidationError(
                "ExecutionPlanCompiler only accepts validated execution policies, not paper-only or manual algorithms",
                context={"execution_policy": policy},
            )
