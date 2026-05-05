"""Unified event-risk policy for selection, Paper v2, and future QE wiring.

The first implemented provider is the ST PIT universe. Announcement-risk
events will map into the same decision contract, so trading adapters do not
need a second architecture when announcement warnings are enabled later.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.services.trading_core.models import PositionLot


class RiskDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    can_buy: bool = True
    force_exit: bool = False
    hold_only: bool = False
    sell_only: bool = False
    position_target_override: int | None = None
    score_multiplier: float = 1.0
    score_delta: float = 0.0
    rank_penalty: float = 0.0
    max_weight_multiplier: float | None = None
    reason_codes: list[str] = Field(default_factory=list)
    source_events: list[dict[str, Any]] = Field(default_factory=list)

    def merge(self, other: "RiskDecision") -> "RiskDecision":
        return RiskDecision(
            symbol=self.symbol,
            can_buy=self.can_buy and other.can_buy,
            force_exit=self.force_exit or other.force_exit,
            hold_only=self.hold_only or other.hold_only,
            sell_only=self.sell_only or other.sell_only,
            position_target_override=(
                0
                if self.position_target_override == 0 or other.position_target_override == 0
                else self.position_target_override
                if self.position_target_override is not None
                else other.position_target_override
            ),
            score_multiplier=self.score_multiplier * other.score_multiplier,
            score_delta=self.score_delta + other.score_delta,
            rank_penalty=self.rank_penalty + other.rank_penalty,
            max_weight_multiplier=self._merge_cap(self.max_weight_multiplier, other.max_weight_multiplier),
            reason_codes=_dedupe([*self.reason_codes, *other.reason_codes]),
            source_events=[*self.source_events, *other.source_events],
        )

    @staticmethod
    def _merge_cap(left: float | None, right: float | None) -> float | None:
        if left is None:
            return right
        if right is None:
            return left
        return min(left, right)


class RiskDecisionProvider(Protocol):
    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: RuntimeRiskPolicyProfile,
        current_positions: dict[str, PositionLot] | None = None,
    ) -> dict[str, RiskDecision]:
        ...


class StPitRiskDecisionProvider:
    """Map ST PIT buy-universe spans to the unified risk decision contract."""

    source_name = "market.stock_universe_pit_spans"

    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: RuntimeRiskPolicyProfile,
        current_positions: dict[str, PositionLot] | None = None,
    ) -> dict[str, RiskDecision]:
        normalized = _normalize_symbols(symbols)
        if not normalized:
            return {}
        if profile.strict_data_ready:
            self._require_ready_pit_state(universe_key=profile.st_universe_key, trade_date=trade_date)
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, eligible_start, eligible_end, entry_reason,
                               exit_reason, rule_version, metadata
                          FROM market.stock_universe_pit_spans
                         WHERE universe_key = %s
                           AND ts_code = ANY(%s)
                           AND eligible_start <= %s
                           AND eligible_end >= %s
                        """,
                        (profile.st_universe_key, normalized, trade_date, trade_date),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "ST PIT risk policy lookup failed",
                context={
                    "trade_date": trade_date.isoformat(),
                    "symbol_count": len(normalized),
                    "universe_key": profile.st_universe_key,
                },
            ) from exc
        eligible = {str(row[0]) for row in rows}
        span_context = {
            str(row[0]): {
                "source_table": self.source_name,
                "universe_key": profile.st_universe_key,
                "visible_trade_date": trade_date.isoformat(),
                "eligible_start": row[1].isoformat() if row[1] else None,
                "eligible_end": row[2].isoformat() if row[2] else None,
                "entry_reason": row[3],
                "exit_reason": row[4],
                "rule_version": row[5],
                "metadata": row[6] or {},
            }
            for row in rows
        }
        decisions: dict[str, RiskDecision] = {}
        holding_symbols = set(current_positions or {})
        for symbol in normalized:
            if symbol in eligible:
                decisions[symbol] = RiskDecision(
                    symbol=symbol,
                    source_events=[span_context.get(symbol, {"source_table": self.source_name})],
                )
                continue
            hard_actions = set(profile.hard_actions)
            force_exit = symbol in holding_symbols and "force_exit" in hard_actions
            decisions[symbol] = RiskDecision(
                symbol=symbol,
                can_buy="block_buy" not in hard_actions,
                force_exit=force_exit,
                sell_only=force_exit,
                position_target_override=0 if force_exit else None,
                reason_codes=["st_pit_not_eligible"],
                source_events=[
                    {
                        "source_table": self.source_name,
                        "universe_key": profile.st_universe_key,
                        "visible_trade_date": trade_date.isoformat(),
                        "event_type": "st_pit_not_eligible",
                        "risk_level": "P0_BLOCK",
                        "action": sorted(hard_actions),
                        "rule_version": profile.policy_version,
                    }
                ],
            )
        return decisions

    def _require_ready_pit_state(self, *, universe_key: str, trade_date: date) -> None:
        """Paper/Selection are consumers; data management owns PIT rebuilds."""

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, dirty, start_date, end_date, last_error
                          FROM market.stock_universe_pit_state
                         WHERE universe_key = %s
                        """,
                        (universe_key,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "ST PIT risk policy readiness check failed",
                context={"trade_date": trade_date.isoformat(), "universe_key": universe_key},
            ) from exc
        if not row:
            raise DataUnavailableError(
                "ST PIT risk policy universe state is missing",
                context={"trade_date": trade_date.isoformat(), "universe_key": universe_key},
            )
        status, dirty, start_date, end_date, last_error = row
        if str(status or "").lower() != "ready" or bool(dirty) or start_date is None or end_date is None:
            raise DataUnavailableError(
                "ST PIT risk policy universe is not ready",
                context={
                    "trade_date": trade_date.isoformat(),
                    "universe_key": universe_key,
                    "status": status,
                    "dirty": bool(dirty),
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "last_error": last_error,
                },
            )
        if start_date > trade_date or end_date < trade_date:
            raise DataUnavailableError(
                "ST PIT risk policy universe does not cover trade_date",
                context={
                    "trade_date": trade_date.isoformat(),
                    "universe_key": universe_key,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )


class AnnouncementRiskDecisionProvider:
    """Reserved provider for future announcement title/PDF risk events."""

    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: RuntimeRiskPolicyProfile,
        current_positions: dict[str, PositionLot] | None = None,
    ) -> dict[str, RiskDecision]:
        raise StrategyPackageValidationError(
            "announcement_risk provider is not implemented yet",
            context={"trade_date": trade_date.isoformat(), "symbol_count": len(symbols)},
        )


class StockRiskPolicyService:
    def __init__(
        self,
        providers: dict[str, RiskDecisionProvider] | None = None,
    ) -> None:
        self.providers = providers or {
            "st_pit": StPitRiskDecisionProvider(),
            "announcement_risk": AnnouncementRiskDecisionProvider(),
        }

    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: RuntimeRiskPolicyProfile,
        current_positions: dict[str, PositionLot] | None = None,
    ) -> dict[str, RiskDecision]:
        normalized = _normalize_symbols(symbols)
        if not profile.enabled or not normalized:
            return {symbol: RiskDecision(symbol=symbol) for symbol in normalized}
        merged = {symbol: RiskDecision(symbol=symbol) for symbol in normalized}
        for provider_name in profile.providers:
            provider = self.providers.get(provider_name)
            if provider is None:
                raise StrategyPackageValidationError(
                    "risk policy provider is not registered",
                    context={"provider": provider_name, "registered": sorted(self.providers)},
                )
            decisions = provider.evaluate(
                symbols=normalized,
                trade_date=trade_date,
                profile=profile,
                current_positions=current_positions,
            )
            for symbol, decision in decisions.items():
                merged[symbol] = merged.get(symbol, RiskDecision(symbol=symbol)).merge(decision)
        return merged

    def apply_to_candidates(
        self,
        *,
        candidates: list[SelectionCandidate],
        decisions: dict[str, RiskDecision],
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
        top_k: int,
        allow_empty: bool = False,
    ) -> tuple[list[SelectionCandidate], list[SelectionExclusion]]:
        if top_k <= 0:
            raise StrategyPackageValidationError(
                "risk policy candidate filter requires positive top_k",
                context={"package_id": package_id, "top_k": top_k},
            )
        adjusted: list[SelectionCandidate] = []
        excluded: list[SelectionExclusion] = []
        ordered = sorted(candidates, key=lambda item: (item.rank, -item.score, item.symbol))
        uses_score_overlay = any(
            decisions.get(candidate.symbol, RiskDecision(symbol=candidate.symbol)).score_multiplier != 1.0
            or decisions.get(candidate.symbol, RiskDecision(symbol=candidate.symbol)).score_delta != 0.0
            or decisions.get(candidate.symbol, RiskDecision(symbol=candidate.symbol)).rank_penalty != 0.0
            or decisions.get(candidate.symbol, RiskDecision(symbol=candidate.symbol)).max_weight_multiplier is not None
            for candidate in ordered
        )
        for candidate in ordered:
            decision = decisions.get(candidate.symbol, RiskDecision(symbol=candidate.symbol))
            risk_context = self._decision_context(
                decision=decision,
                trade_date=trade_date,
                package_id=package_id,
                manifest_sha256=manifest_sha256,
            )
            if not decision.can_buy:
                excluded.append(
                    SelectionExclusion(
                        symbol=candidate.symbol,
                        score=candidate.score,
                        rank=candidate.rank,
                        reason="risk_policy_block_buy",
                        source="runtime_profile.risk_policy",
                        context=risk_context,
                    )
                )
                continue
            score = candidate.score * decision.score_multiplier + decision.score_delta
            target_weight = candidate.target_weight
            if target_weight is not None and decision.max_weight_multiplier is not None:
                target_weight *= decision.max_weight_multiplier
            component_scores = dict(candidate.component_scores or {})
            component_scores.setdefault("raw_rank", candidate.rank)
            component_scores["event_risk"] = risk_context
            component_scores["event_risk"]["risk_sort_rank"] = candidate.rank + float(decision.rank_penalty or 0)
            adjusted.append(
                candidate.model_copy(
                    update={
                        "score": score,
                        "rank": candidate.rank,
                        "target_weight": target_weight,
                        "component_scores": component_scores,
                        "reason": candidate.reason or "risk_policy_adjusted",
                    }
                )
            )
        if uses_score_overlay:
            adjusted.sort(
                key=lambda item: (
                    -item.score,
                    (item.component_scores.get("event_risk") or {}).get("risk_sort_rank", item.rank),
                    item.symbol,
                )
            )
        else:
            adjusted.sort(
                key=lambda item: (
                    (item.component_scores.get("event_risk") or {}).get("risk_sort_rank", item.rank),
                    -item.score,
                    item.symbol,
                )
            )
        reranked = []
        for final_rank, candidate in enumerate(adjusted, start=1):
            reranked.append(candidate.model_copy(update={"rank": final_rank}))
        if not reranked and allow_empty:
            return [], excluded
        if not reranked:
            raise StrategyPackageValidationError(
                "all ranked candidates are excluded by risk policy",
                context={
                    "package_id": package_id,
                    "manifest_sha256": manifest_sha256,
                    "trade_date": trade_date.isoformat(),
                    "candidate_count": len(candidates),
                    "excluded_count": len(excluded),
                    "exclusion_reasons": sorted({item.reason for item in excluded}),
                },
            )
        return reranked, excluded

    def forced_exit_targets(
        self,
        *,
        decisions: dict[str, RiskDecision],
        current_positions: dict[str, PositionLot],
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
        existing_target_symbols: set[str],
    ) -> list["TargetPosition"]:
        from backend.services.selection_center.models import TargetPosition

        targets: list[TargetPosition] = []
        for symbol, position in sorted(current_positions.items()):
            decision = decisions.get(symbol)
            if decision is None or not decision.force_exit or symbol in existing_target_symbols:
                continue
            targets.append(
                TargetPosition(
                    symbol=symbol,
                    target_quantity=0,
                    target_weight=None,
                    reference_price=None,
                    score=0.0,
                    rank=999999,
                    reason="risk_policy_forced_exit",
                    metadata={
                        "risk_policy": self._decision_context(
                            decision=decision,
                            trade_date=trade_date,
                            package_id=package_id,
                            manifest_sha256=manifest_sha256,
                        )
                    },
                )
            )
        return targets

    @staticmethod
    def _decision_context(
        *,
        decision: RiskDecision,
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
    ) -> dict[str, Any]:
        return {
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "trade_date": trade_date.isoformat(),
            "can_buy": decision.can_buy,
            "force_exit": decision.force_exit,
            "hold_only": decision.hold_only,
            "sell_only": decision.sell_only,
            "position_target_override": decision.position_target_override,
            "score_multiplier": decision.score_multiplier,
            "score_delta": decision.score_delta,
            "rank_penalty": decision.rank_penalty,
            "max_weight_multiplier": decision.max_weight_multiplier,
            "reason_codes": decision.reason_codes,
            "source_events": decision.source_events,
        }


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        text = str(symbol or "").strip().upper()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result
