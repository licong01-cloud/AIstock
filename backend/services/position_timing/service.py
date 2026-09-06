"""Daily position-timing card service.

The service reads the legacy portfolio, confirmed watchlist, calendar, and
daily market authorities.  Its only writes are delegated to the timing-owned
artifact store.  No method produces an order or controls a process.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from backend.services.market_data.instrument_validator import normalize_and_validate_ts_codes
from backend.services.trading_core.a_share_live_limit_rule import derive_live_reference_limit_prices
from backend.services.trading_core.exit_guard import ExitGuardContext, evaluate as evaluate_exit_guard
from backend.services.trading_core.price_guard import PriceGuardContext, evaluate as evaluate_price_guard

from .artifact_store import PositionTimingArtifactStore
from .alerts import (
    ParsedAlertQuote,
    eligibility_identity,
    eligibility_payload,
    evaluate_frozen_trigger,
    fetch_quotes_in_contract_chunks,
    parse_alert_quote,
)
from .contracts import (
    AlertClaimRequest,
    AlertEmissionAuthorizedEventV1,
    CHINA_TIMEZONE,
    POSITION_SOURCE,
    POSITION_TIMING_L2_FORMAL_AUDIT_REFERENCE_V1,
    POSITION_TIMING_L2_RESEARCH_CONTRACT_V1,
    ExecutionWindow,
    HoldingAgeBucket,
    LegCostEstimateV1,
    MarketRegime,
    MaturityStatus,
    OutcomeEvaluatedEventV1,
    PolicyFillStatus,
    PositionTimingAnalysisScopeV1,
    PositionTimingCardSetV1,
    PositionTimingCardV1,
    PositionTimingIntentV1,
    PositionTimingMaterializationStateV1,
    SourceRole,
    TimingAction,
    TradabilityStatus,
    TriggerOperator,
    TriggerSide,
    TriggerV1,
    TypedStatus,
    alert_event_idempotency_key,
    canonical_sha256,
    outcome_event_idempotency_key,
)
from .policy import (
    COST_POLICY_SHA256,
    EXIT_GUARD_SNAPSHOT_ENVELOPE_V1,
    EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    PERSONAL_MANUAL_COMPONENT_COST_V1,
    PRICE_GUARD_SNAPSHOT_ENVELOPE_V1,
    PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    assert_shared_guard_defaults_unmodified,
    board_lot_identity,
    component_cost_for_parent_notionals,
    estimate_leg_cost,
    frozen_exit_guard_policy,
    frozen_price_guard_policy,
    legal_target_quantity,
    planned_full_notional_threshold,
)


CHINA_TZ = ZoneInfo(CHINA_TIMEZONE)
ACTIVE_WATCHLIST_LIFECYCLES = frozenset({"CANDIDATE", "ENTERED", "HOLDING"})
_CARD_ADJUSTMENT_NOT_APPLICABLE_BASE = {
    "status": "NOT_APPLICABLE",
    "reason_code": "BLOCK_ONE_CARD_USES_RAW_PRICE_ONLY",
    "future_use": "OUTCOME_EVALUATION_ONLY",
}
CARD_ADJUSTMENT_NOT_APPLICABLE_V1 = {
    **_CARD_ADJUSTMENT_NOT_APPLICABLE_BASE,
    "identity_sha256": canonical_sha256(_CARD_ADJUSTMENT_NOT_APPLICABLE_BASE),
}


class PositionTimingServiceError(RuntimeError):
    def __init__(self, code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        return {"error_code": self.code, "message": self.message, "context": self.context}


@dataclass(frozen=True)
class _UniverseMember:
    canonical_symbol: str
    display_name: str | None
    primary_source_role: SourceRole
    source_roles: tuple[SourceRole, ...]
    holding: Mapping[str, Any] | None = None
    watchlist: Mapping[str, Any] | None = None
    normalization_reason: str | None = None


@dataclass(frozen=True)
class _OutcomeFill:
    status: PolicyFillStatus
    selected_trigger: TriggerV1 | None
    planned_delta_qty: int
    fill_price_raw: Decimal | None
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class PositionTimingDependencies:
    holdings_loader: Callable[[], list[dict[str, Any]]]
    watchlist_page_loader: Callable[[int, int], dict[str, Any]]
    calendar_service: Any
    daily_snapshot_loader: Callable[[list[str], date], dict[str, Any]]
    supporting_facts_loader: Callable[[list[str], date], dict[str, Any]]
    delist_snapshot_loader: Callable[[list[str], date], dict[str, Any]]
    now_provider: Callable[[], datetime]
    source_commit_provider: Callable[[], str]
    realtime_quote_loader: Callable[[list[str]], dict[str, dict[str, Any]]] | None = None
    outcome_snapshot_loader: Callable[[list[str], date, date], dict[str, Any]] | None = None


class PositionTimingService:
    def __init__(self, *, store: PositionTimingArtifactStore, dependencies: PositionTimingDependencies) -> None:
        self.store = store
        self.dependencies = dependencies

    def list_intents(self) -> dict[str, Any]:
        members, discovery_identity = self._load_universe()
        scope = self.store.get_analysis_scope()
        analysis_members, analysis_identity = self._analysis_members(members=members, scope=scope)
        analysis_symbols = {member.canonical_symbol for member in analysis_members}
        holding_symbols = {
            member.canonical_symbol
            for member in members
            if member.primary_source_role is SourceRole.HOLDING
        }
        confirmed_watchlist_symbols = {
            member.canonical_symbol
            for member in members
            if SourceRole.WATCHLIST in member.source_roles
        }
        selected_symbols = set(scope.selected_watchlist_symbols)
        intent_by_symbol = {item.canonical_symbol: item for item in self.store.list_intents()}
        return {
            "schema_version": "position_timing_intent_list_v1",
            "position_source": POSITION_SOURCE,
            "universe_identity_sha256": discovery_identity,
            "discovery_universe_identity_sha256": discovery_identity,
            "analysis_universe_identity_sha256": analysis_identity,
            "analysis_scope": scope,
            "scope_warnings": [
                {
                    "canonical_symbol": symbol,
                    "reason_code": "SELECTED_SOURCE_INELIGIBLE",
                }
                for symbol in scope.selected_watchlist_symbols
                if symbol not in confirmed_watchlist_symbols and symbol not in holding_symbols
            ],
            "items": [
                {
                    "canonical_symbol": member.canonical_symbol,
                    "display_name": member.display_name,
                    "primary_source_role": member.primary_source_role,
                    "source_roles": member.source_roles,
                    "pre_action_qty": _non_negative_int((member.holding or {}).get("quantity")),
                    "intent": intent_by_symbol.get(member.canonical_symbol),
                    "normalization_reason": member.normalization_reason,
                    "analysis_selected": member.canonical_symbol in selected_symbols,
                    "analysis_effective": member.canonical_symbol in analysis_symbols,
                    "analysis_locked": member.primary_source_role is SourceRole.HOLDING,
                    "analysis_reason_code": (
                        "HOLDING_ALWAYS_INCLUDED"
                        if member.primary_source_role is SourceRole.HOLDING
                        else "SELECTED"
                        if member.canonical_symbol in analysis_symbols
                        else "NOT_SELECTED"
                    ),
                }
                for member in members
            ],
        }

    def put_analysis_scope(self, *, raw_symbol: str, analysis_enabled: bool) -> dict[str, Any]:
        canonical_symbol, reason = normalize_position_symbol(raw_symbol)
        if reason is not None or canonical_symbol is None or not _is_supported_a_share(canonical_symbol):
            raise PositionTimingServiceError(
                "UNSUPPORTED_SYMBOL", "首发仅支持可识别的沪深 A 股代码", context={"symbol": raw_symbol}
            )
        members, _ = self._load_universe()
        member_by_symbol = {item.canonical_symbol: item for item in members}
        member = member_by_symbol.get(canonical_symbol)
        current = self.store.get_analysis_scope()
        selected = canonical_symbol in current.selected_watchlist_symbols
        if member is not None and member.primary_source_role is SourceRole.HOLDING:
            return {
                "schema_version": "position_timing_analysis_scope_write_result_v1",
                "status": "UNCHANGED",
                "changed": False,
                "canonical_symbol": canonical_symbol,
                "analysis_selected": selected,
                "analysis_effective": True,
                "analysis_locked": True,
                "analysis_reason_code": "HOLDING_ALWAYS_INCLUDED",
                "scope_sha256": current.scope_sha256,
                "effective_card_policy": "NEXT_CARD_SET_ONLY",
            }
        confirmed_watchlist = member is not None and SourceRole.WATCHLIST in member.source_roles
        if analysis_enabled and not confirmed_watchlist:
            raise PositionTimingServiceError(
                "SYMBOL_OUTSIDE_TIMING_UNIVERSE",
                "只能启用当前已确认自选池中的股票",
                context={"canonical_symbol": canonical_symbol},
            )
        if not analysis_enabled and not selected:
            return {
                "schema_version": "position_timing_analysis_scope_write_result_v1",
                "status": "UNCHANGED",
                "changed": False,
                "canonical_symbol": canonical_symbol,
                "analysis_selected": False,
                "analysis_effective": False,
                "analysis_locked": False,
                "analysis_reason_code": "NOT_SELECTED",
                "scope_sha256": current.scope_sha256,
                "effective_card_policy": "NEXT_CARD_SET_ONLY",
            }
        updated, changed = self.store.put_analysis_scope_symbol(
            canonical_symbol=canonical_symbol,
            analysis_enabled=analysis_enabled,
            updated_at=self._now(),
        )
        return {
            "schema_version": "position_timing_analysis_scope_write_result_v1",
            "status": "UPDATED" if changed else "UNCHANGED",
            "changed": changed,
            "canonical_symbol": canonical_symbol,
            "analysis_selected": canonical_symbol in updated.selected_watchlist_symbols,
            "analysis_effective": bool(analysis_enabled and confirmed_watchlist),
            "analysis_locked": False,
            "analysis_reason_code": "SELECTED" if analysis_enabled else "NOT_SELECTED",
            "scope_sha256": updated.scope_sha256,
            "effective_card_policy": "NEXT_CARD_SET_ONLY",
        }

    def put_intent(
        self,
        *,
        raw_symbol: str,
        planned_full_notional_cny: Decimal,
        desired_target_exposure: Decimal,
    ) -> tuple[PositionTimingIntentV1, bool]:
        canonical_symbol, reason = normalize_position_symbol(raw_symbol)
        if reason is not None or canonical_symbol is None or not _is_supported_a_share(canonical_symbol):
            raise PositionTimingServiceError(
                "UNSUPPORTED_SYMBOL", "首发仅支持可识别的沪深 A 股代码", context={"symbol": raw_symbol}
            )
        members, _ = self._load_universe()
        if canonical_symbol not in {item.canonical_symbol for item in members}:
            raise PositionTimingServiceError(
                "SYMBOL_OUTSIDE_TIMING_UNIVERSE",
                "择时意图仅适用于当前持仓或已确认自选池",
                context={"canonical_symbol": canonical_symbol},
            )
        existing = self.store.get_intent(canonical_symbol)
        if (
            existing is not None
            and existing.planned_full_notional_cny == planned_full_notional_cny
            and existing.desired_target_exposure == desired_target_exposure
        ):
            return existing, False
        updated_at = self._now()
        intent_sha256 = canonical_sha256(
            {
                "canonical_symbol": canonical_symbol,
                "planned_full_notional_cny": planned_full_notional_cny,
                "desired_target_exposure": desired_target_exposure,
                "updated_at": updated_at,
            }
        )
        intent = PositionTimingIntentV1(
            canonical_symbol=canonical_symbol,
            planned_full_notional_cny=planned_full_notional_cny,
            desired_target_exposure=desired_target_exposure,
            updated_at=updated_at,
            intent_sha256=intent_sha256,
        )
        self.store.put_intent(intent)
        return intent, True

    def materialize(self) -> dict[str, Any]:
        card_result = self._materialize_card_set()
        outcome_result = self._materialize_due_outcomes()
        return {
            **card_result,
            "outcome_materialization_status": outcome_result["status"],
            "outcome_materialization": outcome_result,
        }

    def _materialize_card_set(self) -> dict[str, Any]:
        now = self._now()
        decision_date, decision_as_of, target_date, calendar_identity = self._resolve_decision_clock(now)
        existing = self.store.get_card_set(decision_trade_date=decision_date)
        if existing is not None:
            return {
                "schema_version": "position_timing_materialize_result_v1",
                "status": "ALREADY_MATERIALIZED",
                "created": False,
                "card_set_artifact_sha256": canonical_sha256(existing),
                "card_set": existing,
                "note": "同一 decision date 的已签发卡片不可被后来 intent 改写",
            }

        try:
            assert_shared_guard_defaults_unmodified()
        except RuntimeError as exc:
            raise PositionTimingServiceError(
                "SHARED_GUARD_DEFAULT_DRIFT",
                "共享 guard 默认值已偏离择时侧冻结快照，必须新建显式 snapshot 版本",
                context={"cause": str(exc)},
            ) from exc
        try:
            source_commit = self.dependencies.source_commit_provider().strip().lower()
        except Exception as exc:
            raise PositionTimingServiceError(
                "SOURCE_COMMIT_IDENTITY_UNAVAILABLE",
                "无法获得生成卡片所需的源码提交身份",
                context={"cause": type(exc).__name__},
            ) from exc
        if len(source_commit) != 40 or any(char not in "0123456789abcdef" for char in source_commit):
            raise PositionTimingServiceError(
                "SOURCE_COMMIT_IDENTITY_UNAVAILABLE", "无法获得生成卡片所需的源码提交身份"
            )

        self.store.publish_policy_snapshot(name="price-guard-v1", payload=PRICE_GUARD_SNAPSHOT_ENVELOPE_V1)
        self.store.publish_policy_snapshot(name="exit-guard-v1", payload=EXIT_GUARD_SNAPSHOT_ENVELOPE_V1)
        self.store.publish_policy_snapshot(name="personal-manual-component-cost-v1", payload=PERSONAL_MANUAL_COMPONENT_COST_V1)

        discovery_members, discovery_universe_identity = self._load_universe()
        analysis_scope = self.store.get_analysis_scope()
        members, analysis_universe_identity = self._analysis_members(
            members=discovery_members,
            scope=analysis_scope,
        )
        if not members:
            discovery_empty = not discovery_members
            return {
                "schema_version": "position_timing_materialize_result_v1",
                "status": (
                    "UNIVERSE_EMPTY_NO_NEW_CARD"
                    if discovery_empty
                    else "ANALYSIS_UNIVERSE_EMPTY_NO_NEW_CARD"
                ),
                "created": False,
                "card_set": None,
                "decision_trade_date": decision_date,
                "target_trade_date": target_date,
                "reason_codes": ["TIMING_UNIVERSE_EMPTY" if discovery_empty else "ANALYSIS_UNIVERSE_EMPTY"],
            }
        intents = self.store.list_intents()
        intent_by_symbol = {item.canonical_symbol: item for item in intents}
        member_symbols = {member.canonical_symbol for member in members}
        active_intents = tuple(
            intent_by_symbol[symbol]
            for symbol in sorted(intent_by_symbol)
            if symbol in member_symbols
        )
        position_snapshot_as_of = now
        position_snapshot_sha256 = canonical_sha256(
            [
                {
                    "symbol": member.canonical_symbol,
                    "holding": dict(member.holding or {}),
                    "source_roles": member.source_roles,
                }
                for member in members
            ]
        )
        intent_snapshot_sha256 = canonical_sha256(active_intents)
        supported_symbols = [
            member.canonical_symbol
            for member in members
            if member.normalization_reason is None and _is_supported_a_share(member.canonical_symbol)
        ]

        daily_snapshot = self._safe_batch_load(
            self.dependencies.daily_snapshot_loader,
            supported_symbols,
            decision_date,
            unavailable_code="DAILY_BAR_SOURCE_UNAVAILABLE",
        )
        supporting_facts = self._safe_batch_load(
            self.dependencies.supporting_facts_loader,
            supported_symbols,
            decision_date,
            unavailable_code="TRADING_FACT_SOURCE_UNAVAILABLE",
        )
        delist_snapshot = self._safe_batch_load(
            self.dependencies.delist_snapshot_loader,
            supported_symbols,
            decision_date,
            unavailable_code="DELIST_SOURCE_UNAVAILABLE",
        )
        if supported_symbols:
            immature_sources: list[str] = []
            suspension_map = supporting_facts.get("suspend_facts") or {}
            daily_rows = daily_snapshot.get("rows") or {}
            usable_daily_symbols = {
                symbol
                for symbol in supported_symbols
                if _daily_reference_is_usable(
                    daily_rows.get(symbol),
                    decision_date=decision_date,
                    decision_day_suspended=bool((suspension_map.get(symbol) or {}).get("is_suspended")),
                )
            }
            if not _has_valid_batch_identity(daily_snapshot):
                immature_sources.append("DAILY_BAR_SOURCE_IDENTITY_INVALID")
            if not _has_valid_batch_identity(supporting_facts):
                immature_sources.append("TRADING_FACT_SOURCE_IDENTITY_INVALID")
            if not _has_valid_batch_identity(delist_snapshot):
                immature_sources.append("DELIST_SOURCE_IDENTITY_INVALID")
            if not usable_daily_symbols:
                immature_sources.append("DAILY_BAR_SOURCE_ZERO_COVERAGE")
            if not (supporting_facts.get("stock_st_facts") or {}):
                immature_sources.append("ST_SOURCE_ZERO_COVERAGE")
            if not (supporting_facts.get("suspend_facts") or {}):
                immature_sources.append("SUSPEND_SOURCE_ZERO_COVERAGE")
            if not (delist_snapshot.get("rows") or {}):
                immature_sources.append("DELIST_SOURCE_ZERO_COVERAGE")
            if immature_sources:
                return {
                    "schema_version": "position_timing_materialize_result_v1",
                    "status": "SOURCE_NOT_MATURE_NO_NEW_CARD",
                    "created": False,
                    "card_set": None,
                    "decision_trade_date": decision_date,
                    "target_trade_date": target_date,
                    "reason_codes": immature_sources,
                    "coverage": {
                        "expected_supported_symbols": len(supported_symbols),
                        "daily_rows": len(daily_rows),
                        "usable_daily_references": len(usable_daily_symbols),
                        "st_rows": len(supporting_facts.get("stock_st_facts") or {}),
                        "suspend_rows": len(supporting_facts.get("suspend_facts") or {}),
                        "delist_rows": len(delist_snapshot.get("rows") or {}),
                    },
                }

        input_identity = {
            "universe_identity_sha256": analysis_universe_identity,
            "analysis_universe_identity_sha256": analysis_universe_identity,
            "discovery_universe_identity_sha256": discovery_universe_identity,
            "analysis_scope_snapshot": analysis_scope.model_dump(mode="json"),
            "analysis_scope_snapshot_sha256": canonical_sha256(analysis_scope),
            "position_snapshot_sha256": position_snapshot_sha256,
            "intent_snapshot_sha256": intent_snapshot_sha256,
            "daily_snapshot_identity": daily_snapshot.get("identity"),
            "supporting_facts_identity": supporting_facts.get("identity"),
            "delist_snapshot_identity": delist_snapshot.get("identity"),
            "calendar_identity": calendar_identity,
            "source_repository_commit": source_commit,
        }
        input_identity_sha256 = canonical_sha256(input_identity)
        policy_identity = {
            "price_guard_snapshot_sha256": PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            "exit_guard_snapshot_sha256": EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            "cost_policy_sha256": COST_POLICY_SHA256,
        }
        policy_identity_sha256 = canonical_sha256(policy_identity)
        semantic_identity_sha256 = canonical_sha256(
            {
                "position_source": POSITION_SOURCE,
                "decision_trade_date": decision_date,
                "target_trade_date": target_date,
                "input_identity_sha256": input_identity_sha256,
                "policy_identity_sha256": policy_identity_sha256,
            }
        )
        card_set_id = f"ptset_{semantic_identity_sha256[:24]}"
        cards = tuple(
            self._build_card(
                member=member,
                intent=intent_by_symbol.get(member.canonical_symbol),
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                created_at=now,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=canonical_sha256(
                    {
                        "canonical_symbol": member.canonical_symbol,
                        "holding": dict(member.holding or {}),
                        "position_source": POSITION_SOURCE,
                    }
                ),
                intent_snapshot_as_of=(
                    intent_by_symbol[member.canonical_symbol].updated_at
                    if member.canonical_symbol in intent_by_symbol
                    else None
                ),
                intent_snapshot_sha256=canonical_sha256(
                    {
                        "canonical_symbol": member.canonical_symbol,
                        "intent": intent_by_symbol.get(member.canonical_symbol),
                    }
                ),
                daily_snapshot=daily_snapshot,
                supporting_facts=supporting_facts,
                delist_snapshot=delist_snapshot,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
            )
            for member in members
        )
        card_set = PositionTimingCardSetV1(
            card_set_id=card_set_id,
            decision_trade_date=decision_date,
            decision_as_of=decision_as_of,
            target_trade_date=target_date,
            created_at=now,
            semantic_identity_sha256=semantic_identity_sha256,
            input_identity_sha256=input_identity_sha256,
            policy_identity_sha256=policy_identity_sha256,
            cards_sha256=canonical_sha256(cards),
            input_identity=input_identity,
            policy_identity=policy_identity,
            cards=cards,
        )
        published, artifact_sha256, created = self.store.publish_card_set(card_set)
        return {
            "schema_version": "position_timing_materialize_result_v1",
            "status": "MATERIALIZED" if created else "ALREADY_MATERIALIZED",
            "created": created,
            "card_set_artifact_sha256": artifact_sha256,
            "card_set": published,
        }

    def _materialize_due_outcomes(self) -> dict[str, Any]:
        now = self._now()
        completed_trade_date = self._latest_completed_trade_date(now)
        card_sets = self.store.list_card_sets()
        due: list[dict[str, Any]] = []
        calendar_error: str | None = None
        for card_set in card_sets:
            if card_set.target_trade_date > completed_trade_date:
                continue
            try:
                timeline = self.dependencies.calendar_service.list_trading_days(
                    card_set.target_trade_date,
                    completed_trade_date,
                    allow_empty=True,
                )
            except Exception as exc:
                calendar_error = type(exc).__name__
                break
            for card in card_set.cards:
                for horizon in (1, 3, 5, 10, 20):
                    if len(timeline) < horizon:
                        continue
                    due.append(
                        {
                            "card": card,
                            "horizon": horizon,
                            "nominal_terminal_trade_date": timeline[horizon - 1],
                            "timeline": timeline,
                            "nominal_index": horizon - 1,
                        }
                    )

        existing_events = {
            str(item["idempotency_key"]): item
            for item in self.store.list_events(event_type="OUTCOME_EVALUATED")
        }
        expected_due = len(due)
        prior_state = self.store.get_materialization_state()
        failure_reasons: list[str] = []
        if calendar_error is not None:
            failure_reasons.append(f"TRADING_CALENDAR_UNAVAILABLE:{calendar_error}")
        missing_due = [
            item
            for item in due
            if outcome_event_idempotency_key(item["card"].card_id, item["horizon"])
            not in existing_events
        ]
        snapshot: dict[str, Any] | None = None
        if missing_due and not failure_reasons:
            if self.dependencies.outcome_snapshot_loader is None:
                failure_reasons.append("OUTCOME_DATA_SOURCE_UNAVAILABLE")
            else:
                try:
                    snapshot = self.dependencies.outcome_snapshot_loader(
                        sorted({item["card"].canonical_symbol for item in missing_due}),
                        min(item["card"].target_trade_date for item in missing_due),
                        completed_trade_date,
                    )
                    if not isinstance(snapshot, dict) or not _valid_outcome_snapshot_identity(snapshot):
                        raise ValueError("OUTCOME_DATA_IDENTITY_INVALID")
                except Exception as exc:
                    snapshot = None
                    failure_reasons.append(f"OUTCOME_DATA_SOURCE_UNAVAILABLE:{type(exc).__name__}")

        pending_events: list[tuple[dict[str, Any], str]] = []
        waiting_for_defer = 0
        if snapshot is not None:
            for item in missing_due:
                try:
                    event = self._evaluate_outcome(
                        card=item["card"],
                        horizon=item["horizon"],
                        nominal_terminal_trade_date=item["nominal_terminal_trade_date"],
                        timeline=item["timeline"],
                        nominal_index=item["nominal_index"],
                        snapshot=snapshot,
                    )
                except Exception as exc:
                    failure_reasons.append(
                        f"OUTCOME_EVALUATION_FAILED:{item['card'].card_id}:{item['horizon']}:{type(exc).__name__}"
                    )
                    continue
                if event is None:
                    waiting_for_defer += 1
                    continue
                pending_events.append((event.model_dump(mode="python"), event.idempotency_key))
        appended = self.store.append_events(pending_events) if pending_events else 0
        after_events = {
            str(item["idempotency_key"]): item
            for item in self.store.list_events(event_type="OUTCOME_EVALUATED")
        }
        accounted = sum(
            1
            for item in due
            if outcome_event_idempotency_key(item["card"].card_id, item["horizon"])
            in after_events
        )
        complete = calendar_error is None and accounted == expected_due
        state = PositionTimingMaterializationStateV1(
            last_successful_materialization_scan_through_trade_date=(
                completed_trade_date
                if complete
                else prior_state.last_successful_materialization_scan_through_trade_date
            ),
            last_run_at=now,
            expected_due_count=expected_due,
            accounted_outcome_count=accounted,
            run_status="COMPLETE" if complete else "PARTIAL",
        )
        state = self.store.put_materialization_state(state)
        if complete and expected_due == 0:
            status = "NO_DUE_OUTCOMES"
        elif complete and appended:
            status = "OUTCOMES_MATERIALIZED"
        elif complete:
            status = "OUTCOMES_ALREADY_MATERIALIZED"
        else:
            status = "OUTCOME_MATERIALIZATION_PARTIAL"
        return {
            "schema_version": "position_timing_outcome_materialization_result_v1",
            "status": status,
            "completed_trade_date": completed_trade_date,
            "expected_due_count": expected_due,
            "accounted_outcome_count": accounted,
            "materialized_count": appended,
            "waiting_for_terminal_defer_count": waiting_for_defer,
            "reason_codes": tuple(dict.fromkeys(failure_reasons)),
            "materialization_state": state,
        }

    def _evaluate_outcome(
        self,
        *,
        card: PositionTimingCardV1,
        horizon: int,
        nominal_terminal_trade_date: date,
        timeline: list[date],
        nominal_index: int,
        snapshot: Mapping[str, Any],
    ) -> OutcomeEvaluatedEventV1 | None:
        target_row = _outcome_row(snapshot, card.canonical_symbol, card.target_trade_date)
        fill = self._select_outcome_fill(card=card, target_row=target_row)
        if "TARGET_DAY_MARKET_DATA_UNAVAILABLE" in fill.reason_codes:
            return self._unavailable_outcome_event(
                card=card,
                horizon=horizon,
                nominal_terminal_trade_date=nominal_terminal_trade_date,
                effective_terminal_trade_date=nominal_terminal_trade_date,
                deferred_trading_days=0,
                fill=fill,
                snapshot=snapshot,
                reason_codes=fill.reason_codes,
            )
        if fill.status is not PolicyFillStatus.FILLED:
            return self._zero_lift_outcome_event(
                card=card,
                horizon=horizon,
                nominal_terminal_trade_date=nominal_terminal_trade_date,
                fill=fill,
                snapshot=snapshot,
            )
        assert fill.fill_price_raw is not None
        fill_adjustment = _positive_decimal((target_row or {}).get("adj_factor"))
        if fill_adjustment is None:
            return self._unavailable_outcome_event(
                card=card,
                horizon=horizon,
                nominal_terminal_trade_date=nominal_terminal_trade_date,
                effective_terminal_trade_date=nominal_terminal_trade_date,
                deferred_trading_days=0,
                fill=fill,
                snapshot=snapshot,
                reason_codes=("FILL_ADJUSTMENT_IDENTITY_UNAVAILABLE",),
            )

        start_index = nominal_index
        reason_codes = list(fill.reason_codes)
        if fill.planned_delta_qty > 0 and horizon == 1:
            start_index += 1
            reason_codes.append("TERMINAL_T1_LOCKED")
        max_index = nominal_index + 5
        terminal_row: Mapping[str, Any] | None = None
        terminal_date: date | None = None
        terminal_unavailable_reasons: list[str] = []
        for index in range(start_index, min(len(timeline), max_index + 1)):
            candidate_date = timeline[index]
            row = _outcome_row(snapshot, card.canonical_symbol, candidate_date)
            terminal_status = _terminal_sellability_status(row)
            if terminal_status == "SELLABLE":
                terminal_row = row
                terminal_date = candidate_date
                break
            terminal_unavailable_reasons.append(terminal_status)
        if terminal_row is None or terminal_date is None:
            if len(timeline) <= max_index:
                return None
            return self._unavailable_outcome_event(
                card=card,
                horizon=horizon,
                nominal_terminal_trade_date=nominal_terminal_trade_date,
                effective_terminal_trade_date=timeline[max_index],
                deferred_trading_days=5,
                fill=fill,
                snapshot=snapshot,
                reason_codes=(
                    *reason_codes,
                    *tuple(dict.fromkeys(terminal_unavailable_reasons)),
                    "TERMINAL_SELL_UNAVAILABLE_AFTER_MAX_DEFER",
                ),
            )

        terminal_close = _positive_decimal(terminal_row.get("close"))
        terminal_adjustment = _positive_decimal(terminal_row.get("adj_factor"))
        assert terminal_close is not None and terminal_adjustment is not None
        quantity = abs(fill.planned_delta_qty)
        fill_notional = fill.fill_price_raw * quantity
        terminal_quantity_equivalent = Decimal(quantity) * terminal_adjustment / fill_adjustment
        terminal_notional = terminal_close * terminal_quantity_equivalent
        terminal_sell_cost = component_cost_for_parent_notionals(
            side=TriggerSide.SELL,
            notionals=(terminal_notional,),
        )
        if fill.planned_delta_qty > 0:
            fill_cost = component_cost_for_parent_notionals(
                side=TriggerSide.BUY,
                notionals=(fill_notional,),
            )
            candidate_net = terminal_notional - terminal_sell_cost["total"] - fill_cost["total"]
            do_nothing_net = fill_notional
            candidate_path = {
                "path": "BUY_THEN_TERMINAL_SELL",
                "quantity": quantity,
                "fill_notional_cny": fill_notional,
                "fill_cost_components": fill_cost,
                "terminal_quantity_equivalent": terminal_quantity_equivalent,
                "terminal_gross_notional_cny": terminal_notional,
                "terminal_sell_cost_components": terminal_sell_cost,
                "net_value_cny": candidate_net,
                "cash_yield": "0",
            }
            do_nothing_path = {
                "path": "KEEP_CASH",
                "starting_cash_cny": fill_notional,
                "cash_yield": "0",
                "net_value_cny": do_nothing_net,
            }
        else:
            fill_cost = component_cost_for_parent_notionals(
                side=TriggerSide.SELL,
                notionals=(fill_notional,),
            )
            candidate_net = fill_notional - fill_cost["total"]
            do_nothing_net = terminal_notional - terminal_sell_cost["total"]
            candidate_path = {
                "path": "SELL_THEN_HOLD_CASH",
                "quantity": quantity,
                "fill_notional_cny": fill_notional,
                "fill_cost_components": fill_cost,
                "cash_yield": "0",
                "net_value_cny": candidate_net,
            }
            do_nothing_path = {
                "path": "HOLD_THEN_TERMINAL_SELL",
                "quantity": quantity,
                "terminal_quantity_equivalent": terminal_quantity_equivalent,
                "terminal_gross_notional_cny": terminal_notional,
                "terminal_sell_cost_components": terminal_sell_cost,
                "net_value_cny": do_nothing_net,
            }
        net_lift_bps = (
            (candidate_net - do_nothing_net) / fill_notional * Decimal("10000")
            if fill_notional > 0
            else Decimal("0")
        )
        deferred = timeline.index(terminal_date) - nominal_index
        maturity = MaturityStatus.MATURED if deferred == 0 else MaturityStatus.DEFERRED_THEN_MATURED
        if deferred > 0 and "TERMINAL_T1_LOCKED" not in reason_codes:
            reason_codes.append("TERMINAL_VALUE_DEFERRED")
        reason_codes.extend(terminal_unavailable_reasons)
        return self._outcome_event(
            card=card,
            horizon=horizon,
            fill=fill,
            maturity_status=maturity,
            nominal_terminal_trade_date=nominal_terminal_trade_date,
            effective_terminal_trade_date=terminal_date,
            deferred_trading_days=deferred,
            reason_codes=tuple(dict.fromkeys(reason_codes)),
            candidate_path=candidate_path,
            do_nothing_path=do_nothing_path,
            candidate_net_value_cny=candidate_net,
            do_nothing_net_value_cny=do_nothing_net,
            net_lift_bps=net_lift_bps,
            snapshot=snapshot,
        )

    @staticmethod
    def _select_outcome_fill(
        *, card: PositionTimingCardV1, target_row: Mapping[str, Any] | None
    ) -> _OutcomeFill:
        if not card.triggers or card.requested_delta_qty == 0:
            return _OutcomeFill(
                status=PolicyFillStatus.NO_ACTION,
                selected_trigger=None,
                planned_delta_qty=0,
                fill_price_raw=None,
                reason_codes=("NO_ACTION",),
            )
        if target_row is None:
            return _OutcomeFill(
                status=PolicyFillStatus.POLICY_FILL_UNAVAILABLE_EXPIRED,
                selected_trigger=None,
                planned_delta_qty=card.requested_delta_qty,
                fill_price_raw=None,
                reason_codes=("TARGET_DAY_MARKET_DATA_UNAVAILABLE",),
            )
        if bool(target_row.get("is_suspended")):
            return _OutcomeFill(
                status=PolicyFillStatus.POLICY_FILL_UNAVAILABLE_EXPIRED,
                selected_trigger=None,
                planned_delta_qty=card.requested_delta_qty,
                fill_price_raw=None,
                reason_codes=("TARGET_DAY_SUSPENDED",),
            )
        values = {
            name: _positive_decimal(target_row.get(name))
            for name in ("open", "high", "low", "close")
        }
        if any(value is None for value in values.values()):
            return _OutcomeFill(
                status=PolicyFillStatus.POLICY_FILL_UNAVAILABLE_EXPIRED,
                selected_trigger=None,
                planned_delta_qty=card.requested_delta_qty,
                fill_price_raw=None,
                reason_codes=("TARGET_DAY_MARKET_DATA_UNAVAILABLE",),
            )
        open_price = values["open"]
        high_price = values["high"]
        low_price = values["low"]
        close_price = values["close"]
        assert open_price is not None and high_price is not None and low_price is not None and close_price is not None
        side = TriggerSide.BUY if card.requested_delta_qty > 0 else TriggerSide.SELL
        limit = card.limit_up_raw if side is TriggerSide.BUY else card.limit_down_raw
        if limit is not None and all(abs(value - limit) < Decimal("0.005") for value in values.values()):
            return _OutcomeFill(
                status=PolicyFillStatus.POLICY_FILL_UNAVAILABLE_EXPIRED,
                selected_trigger=None,
                planned_delta_qty=card.requested_delta_qty,
                fill_price_raw=None,
                reason_codes=(
                    "TARGET_DAY_ONE_WORD_LIMIT_UP_BUY_UNAVAILABLE"
                    if side is TriggerSide.BUY
                    else "TARGET_DAY_ONE_WORD_LIMIT_DOWN_SELL_UNAVAILABLE",
                ),
            )
        actionable = [trigger for trigger in card.triggers if trigger.planned_delta_qty != 0]
        always = [trigger for trigger in actionable if trigger.operator is TriggerOperator.ALWAYS]
        if always:
            selected = always[0]
            return _OutcomeFill(
                status=PolicyFillStatus.FILLED,
                selected_trigger=selected,
                planned_delta_qty=selected.planned_delta_qty,
                fill_price_raw=open_price,
                reason_codes=("AT_OPEN_FILL",),
            )
        if side is TriggerSide.BUY:
            candidates = [
                trigger
                for trigger in actionable
                if trigger.operator is TriggerOperator.LTE and trigger.trigger_price_raw is not None
            ]
            open_eligible = [trigger for trigger in candidates if open_price <= trigger.trigger_price_raw]
            if open_eligible:
                selected = min(open_eligible, key=lambda item: item.trigger_price_raw or Decimal("Infinity"))
                return _OutcomeFill(
                    PolicyFillStatus.FILLED,
                    selected,
                    selected.planned_delta_qty,
                    open_price,
                    ("OPEN_SATISFIED_FROZEN_TRIGGER",),
                )
            touched = [trigger for trigger in candidates if low_price <= trigger.trigger_price_raw]
            if touched:
                selected = min(
                    touched,
                    key=lambda item: (abs(item.planned_delta_qty), -(item.trigger_price_raw or Decimal("0"))),
                )
                reason = (
                    "INTRADAY_SEQUENCE_UNOBSERVED_CONSERVATIVE_FILL"
                    if len(touched) > 1
                    else "INTRADAY_LOW_TOUCHED_FROZEN_TRIGGER"
                )
                return _OutcomeFill(
                    PolicyFillStatus.FILLED,
                    selected,
                    selected.planned_delta_qty,
                    selected.trigger_price_raw,
                    (reason,),
                )
        else:
            candidates = [
                trigger
                for trigger in actionable
                if trigger.operator is TriggerOperator.GTE and trigger.trigger_price_raw is not None
            ]
            open_eligible = [trigger for trigger in candidates if open_price >= trigger.trigger_price_raw]
            if open_eligible:
                selected = max(open_eligible, key=lambda item: item.trigger_price_raw or Decimal("0"))
                return _OutcomeFill(
                    PolicyFillStatus.FILLED,
                    selected,
                    selected.planned_delta_qty,
                    open_price,
                    ("OPEN_SATISFIED_FROZEN_TRIGGER",),
                )
            touched = [trigger for trigger in candidates if high_price >= trigger.trigger_price_raw]
            if touched:
                selected = min(
                    touched,
                    key=lambda item: (abs(item.planned_delta_qty), item.trigger_price_raw or Decimal("0")),
                )
                reason = (
                    "INTRADAY_SEQUENCE_UNOBSERVED_CONSERVATIVE_FILL"
                    if len(touched) > 1
                    else "INTRADAY_HIGH_TOUCHED_FROZEN_TRIGGER"
                )
                return _OutcomeFill(
                    PolicyFillStatus.FILLED,
                    selected,
                    selected.planned_delta_qty,
                    selected.trigger_price_raw,
                    (reason,),
                )
        skip = next((trigger for trigger in card.triggers if trigger.planned_delta_qty == 0), None)
        return _OutcomeFill(
            status=PolicyFillStatus.SKIPPED_BY_GUARD,
            selected_trigger=skip,
            planned_delta_qty=0,
            fill_price_raw=None,
            reason_codes=((skip.reason_code if skip is not None else "FROZEN_TRIGGER_NOT_MET"),),
        )

    def _zero_lift_outcome_event(
        self,
        *,
        card: PositionTimingCardV1,
        horizon: int,
        nominal_terminal_trade_date: date,
        fill: _OutcomeFill,
        snapshot: Mapping[str, Any],
    ) -> OutcomeEvaluatedEventV1:
        path = {
            "path": "INTENTION_TO_TREAT_DO_NOTHING",
            "status": fill.status,
            "quantity": 0,
            "gross_value_cny": Decimal("0"),
            "net_value_cny": Decimal("0"),
        }
        return self._outcome_event(
            card=card,
            horizon=horizon,
            fill=fill,
            maturity_status=MaturityStatus.MATURED,
            nominal_terminal_trade_date=nominal_terminal_trade_date,
            effective_terminal_trade_date=nominal_terminal_trade_date,
            deferred_trading_days=0,
            reason_codes=fill.reason_codes,
            candidate_path=path,
            do_nothing_path=path,
            candidate_net_value_cny=Decimal("0"),
            do_nothing_net_value_cny=Decimal("0"),
            net_lift_bps=Decimal("0"),
            snapshot=snapshot,
        )

    def _unavailable_outcome_event(
        self,
        *,
        card: PositionTimingCardV1,
        horizon: int,
        nominal_terminal_trade_date: date,
        effective_terminal_trade_date: date,
        deferred_trading_days: int,
        fill: _OutcomeFill,
        snapshot: Mapping[str, Any],
        reason_codes: tuple[str, ...],
    ) -> OutcomeEvaluatedEventV1:
        unavailable_path = {"status": "UNAVAILABLE", "reason_codes": reason_codes}
        return self._outcome_event(
            card=card,
            horizon=horizon,
            fill=fill,
            maturity_status=MaturityStatus.UNAVAILABLE_AT_HORIZON,
            nominal_terminal_trade_date=nominal_terminal_trade_date,
            effective_terminal_trade_date=effective_terminal_trade_date,
            deferred_trading_days=deferred_trading_days,
            reason_codes=reason_codes,
            candidate_path=unavailable_path,
            do_nothing_path=unavailable_path,
            candidate_net_value_cny=None,
            do_nothing_net_value_cny=None,
            net_lift_bps=None,
            snapshot=snapshot,
        )

    @staticmethod
    def _outcome_event(
        *,
        card: PositionTimingCardV1,
        horizon: int,
        fill: _OutcomeFill,
        maturity_status: MaturityStatus,
        nominal_terminal_trade_date: date,
        effective_terminal_trade_date: date,
        deferred_trading_days: int,
        reason_codes: tuple[str, ...],
        candidate_path: dict[str, Any],
        do_nothing_path: dict[str, Any],
        candidate_net_value_cny: Decimal | None,
        do_nothing_net_value_cny: Decimal | None,
        net_lift_bps: Decimal | None,
        snapshot: Mapping[str, Any],
    ) -> OutcomeEvaluatedEventV1:
        key = outcome_event_idempotency_key(card.card_id, horizon)
        occurred_date = effective_terminal_trade_date or nominal_terminal_trade_date
        return OutcomeEvaluatedEventV1(
            event_id=f"evt_{canonical_sha256({'event_type': 'OUTCOME_EVALUATED', 'key': key})[:24]}",
            idempotency_key=key,
            occurred_at=datetime.combine(occurred_date, time(15, 0), tzinfo=CHINA_TZ),
            card_id=card.card_id,
            card_artifact_sha256=canonical_sha256(card),
            horizon_trading_days=horizon,
            policy_fill_status=fill.status,
            maturity_status=maturity_status,
            selected_trigger_id=fill.selected_trigger.trigger_id if fill.selected_trigger else None,
            planned_delta_qty=fill.planned_delta_qty,
            effective_target_exposure=(
                fill.selected_trigger.target_exposure if fill.selected_trigger is not None else card.pre_action_exposure
            ),
            fill_price_raw=fill.fill_price_raw,
            fill_time_policy=(
                "DAILY_OHLC_CONSERVATIVE_FILL_V1" if fill.status is PolicyFillStatus.FILLED else None
            ),
            nominal_terminal_trade_date=nominal_terminal_trade_date,
            effective_terminal_trade_date=effective_terminal_trade_date,
            deferred_trading_days=deferred_trading_days,
            reason_codes=reason_codes,
            candidate_path=candidate_path,
            do_nothing_path=do_nothing_path,
            candidate_net_value_cny=candidate_net_value_cny,
            do_nothing_net_value_cny=do_nothing_net_value_cny,
            net_lift_bps=net_lift_bps,
            dataset_identity_sha256=_identity_hash(snapshot.get("identity")),
            calendar_identity_sha256=_identity_hash(card.calendar_identity),
            limit_identity_sha256=_identity_hash(snapshot.get("limit_identity")),
            board_lot_identity_sha256=_identity_hash(card.board_lot_identity),
            adjustment_identity_sha256=_identity_hash(snapshot.get("adjustment_identity")),
            cost_policy_sha256=card.cost_policy_sha256,
        )

    def _latest_completed_trade_date(self, now: datetime) -> date:
        try:
            status = self.dependencies.calendar_service.status(as_of_date=now.date())
            if bool(status.get("is_trading_day")) and now.time() >= time(15, 0):
                value = now.date()
            elif bool(status.get("is_trading_day")):
                value = date.fromisoformat(str(status["previous_trading_day"]))
            else:
                value = date.fromisoformat(str(status["latest_completed_trading_day"]))
            return value
        except Exception as exc:
            raise PositionTimingServiceError(
                "TRADING_CALENDAR_UNAVAILABLE",
                "无法确定 outcome 的最近完成交易日",
                context={"cause": type(exc).__name__},
            ) from exc

    def current_cards(self) -> dict[str, Any]:
        now = self._now()
        card_set = self.store.latest_card_set()
        if card_set is None:
            return {
                "schema_version": "position_timing_current_cards_v1",
                "status": "NO_CARD_SET",
                "card_set": None,
            }
        target_close = datetime.combine(card_set.target_trade_date, time(15, 0), tzinfo=CHINA_TZ)
        if now.date() < card_set.target_trade_date:
            status = "UPCOMING"
        elif now.date() == card_set.target_trade_date and now <= target_close:
            status = "VALID_TODAY"
        else:
            status = "EXPIRED"
        return {
            "schema_version": "position_timing_current_cards_v1",
            "status": status,
            "card_set": card_set,
        }

    def poll_alerts(self) -> dict[str, Any]:
        """Evaluate frozen T+1 trigger edges without writing any artifact."""

        now = self._now()
        card_set = self.store.latest_card_set()
        if card_set is None:
            return {
                "schema_version": "position_timing_alert_poll_v1",
                "status": "NO_CARD_SET",
                "evaluated_at": now,
                "items": [],
            }
        if now.date() != card_set.target_trade_date or now > datetime.combine(
            card_set.target_trade_date, time(15, 0), tzinfo=CHINA_TZ
        ):
            return {
                "schema_version": "position_timing_alert_poll_v1",
                "status": "NO_VALID_CARD_TODAY",
                "evaluated_at": now,
                "card_set_id": card_set.card_set_id,
                "items": [],
            }
        active_cards = [card for card in card_set.cards if card.triggers]
        if not active_cards:
            return {
                "schema_version": "position_timing_alert_poll_v1",
                "status": "NO_ACTIVE_TRIGGER",
                "evaluated_at": now,
                "card_set_id": card_set.card_set_id,
                "items": [],
            }
        members, _ = self._load_universe()
        member_by_symbol = {item.canonical_symbol: item for item in members}
        quote_payload: dict[str, dict[str, Any]] = {}
        quote_source_error: str | None = None
        if self.dependencies.realtime_quote_loader is None:
            quote_source_error = "QUOTE_SOURCE_UNAVAILABLE"
        else:
            try:
                quote_payload = fetch_quotes_in_contract_chunks(
                    symbols=[card.canonical_symbol for card in active_cards],
                    quote_loader=self.dependencies.realtime_quote_loader,
                )
            except Exception as exc:
                quote_source_error = f"QUOTE_SOURCE_UNAVAILABLE:{type(exc).__name__}"

        items: list[dict[str, Any]] = []
        for card in active_cards:
            item = {
                "card_id": card.card_id,
                "card_artifact_sha256": canonical_sha256(card),
                "canonical_symbol": card.canonical_symbol,
                "action": card.action,
                "system_edge_eligibility": False,
                "market_touch_opportunity": "NOT_EVALUATED_RUNTIME",
                "already_alerted": False,
            }
            snapshots = self._current_alert_snapshot_hashes(
                card=card,
                member=member_by_symbol.get(card.canonical_symbol),
            )
            item.update(snapshots)
            if snapshots["position_snapshot_sha256"] != card.position_snapshot_sha256:
                item["status"] = "POSITION_SNAPSHOT_CHANGED"
                items.append(item)
                continue
            if snapshots["intent_snapshot_sha256"] != card.intent_snapshot_sha256:
                item["status"] = "INTENT_SNAPSHOT_CHANGED"
                items.append(item)
                continue
            if quote_source_error is not None:
                item["status"] = "QUOTE_UNAVAILABLE"
                item["reason_code"] = quote_source_error
                items.append(item)
                continue
            quote, quote_status, quote_details = parse_alert_quote(
                symbol=card.canonical_symbol,
                raw_quote=quote_payload.get(card.canonical_symbol),
                evaluated_at=now,
            )
            item.update(quote_details)
            if quote is None:
                item["status"] = quote_status
                items.append(item)
                continue
            item.update(
                {
                    "quote_price_raw": quote.price_raw,
                    "quote_open_raw": quote.open_raw,
                    "quote_observed_at": quote.observed_at,
                    "alert_evaluated_at": quote.evaluated_at,
                    "quote_source": quote.source,
                    "staleness_state": "FRESH",
                }
            )
            trigger, status = evaluate_frozen_trigger(card=card, quote=quote)
            item["status"] = status
            if trigger is None:
                items.append(item)
                continue
            identity_payload = eligibility_payload(
                card=card,
                trigger=trigger,
                quote=quote,
                position_snapshot_sha256=snapshots["position_snapshot_sha256"],
                intent_snapshot_sha256=snapshots["intent_snapshot_sha256"],
            )
            item.update(
                {
                    "system_edge_eligibility": True,
                    "trigger": trigger,
                    "trigger_id": trigger.trigger_id,
                    "eligibility_identity": eligibility_identity(identity_payload),
                    "already_alerted": self.store.get_event(
                        event_type="ALERT_EMISSION_AUTHORIZED",
                        idempotency_key=alert_event_idempotency_key(card.card_id, trigger.trigger_id),
                    )
                    is not None,
                }
            )
            items.append(item)
        return {
            "schema_version": "position_timing_alert_poll_v1",
            "status": "EVALUATED",
            "evaluated_at": now,
            "card_set_id": card_set.card_set_id,
            "items": items,
        }

    def claim_alert(self, *, trigger_id: str, request: AlertClaimRequest) -> dict[str, Any]:
        """Revalidate a polled edge and atomically grant one alert authorization."""

        now = self._now()
        card_set = self.store.latest_card_set()
        if card_set is None:
            raise PositionTimingServiceError("NO_CARD_SET", "当前没有可用于提醒的行动卡")
        if now.date() != card_set.target_trade_date or now > datetime.combine(
            card_set.target_trade_date, time(15, 0), tzinfo=CHINA_TZ
        ):
            raise PositionTimingServiceError("CARD_NOT_VALID_TODAY", "行动卡已过期或尚未生效")
        card = next((item for item in card_set.cards if item.card_id == request.card_id), None)
        if card is None:
            raise PositionTimingServiceError("CARD_NOT_FOUND", "提醒卡片不属于当前 card set")
        trigger = next((item for item in card.triggers if item.trigger_id == trigger_id), None)
        if trigger is None:
            raise PositionTimingServiceError("TRIGGER_NOT_FOUND", "提醒分支不属于当前卡片")
        key = alert_event_idempotency_key(card.card_id, trigger.trigger_id)
        existing = self.store.get_event(event_type="ALERT_EMISSION_AUTHORIZED", idempotency_key=key)
        if existing is not None:
            return {
                "schema_version": "position_timing_alert_claim_result_v1",
                "status": "ALREADY_AUTHORIZED",
                "granted": False,
                "event": existing,
            }

        members, _ = self._load_universe()
        member = next((item for item in members if item.canonical_symbol == card.canonical_symbol), None)
        snapshots = self._current_alert_snapshot_hashes(card=card, member=member)
        if snapshots["position_snapshot_sha256"] != card.position_snapshot_sha256:
            raise PositionTimingServiceError("POSITION_SNAPSHOT_CHANGED", "持仓快照已经变化，旧提醒失效")
        if snapshots["intent_snapshot_sha256"] != card.intent_snapshot_sha256:
            raise PositionTimingServiceError("INTENT_SNAPSHOT_CHANGED", "择时意图已经变化，旧提醒失效")
        if (
            request.position_snapshot_sha256 != snapshots["position_snapshot_sha256"]
            or request.intent_snapshot_sha256 != snapshots["intent_snapshot_sha256"]
        ):
            raise PositionTimingServiceError("ELIGIBILITY_IDENTITY_MISMATCH", "提醒资格快照与当前状态不一致")
        polled_quote = ParsedAlertQuote(
            symbol=card.canonical_symbol,
            price_raw=request.quote_price_raw,
            open_raw=request.quote_open_raw,
            observed_at=request.quote_observed_at.astimezone(CHINA_TZ),
            evaluated_at=request.alert_evaluated_at.astimezone(CHINA_TZ),
            source=request.quote_source,
        )
        claimed_payload = eligibility_payload(
            card=card,
            trigger=trigger,
            quote=polled_quote,
            position_snapshot_sha256=request.position_snapshot_sha256,
            intent_snapshot_sha256=request.intent_snapshot_sha256,
        )
        if eligibility_identity(claimed_payload) != request.eligibility_identity:
            raise PositionTimingServiceError("ELIGIBILITY_IDENTITY_MISMATCH", "提醒资格 identity 校验失败")
        original_quote, original_status, _ = parse_alert_quote(
            symbol=card.canonical_symbol,
            raw_quote={
                "quote_price_raw": request.quote_price_raw,
                "quote_open_raw": request.quote_open_raw,
                "quote_observed_at": request.quote_observed_at,
                "price_basis": "raw_cny",
            },
            evaluated_at=request.alert_evaluated_at,
        )
        original_trigger = (
            evaluate_frozen_trigger(card=card, quote=original_quote)[0]
            if original_quote is not None
            else None
        )
        if original_status != "FRESH" or original_trigger is None or original_trigger.trigger_id != trigger_id:
            raise PositionTimingServiceError("ELIGIBILITY_IDENTITY_MISMATCH", "原始轮询快照不具备该提醒资格")
        if self.dependencies.realtime_quote_loader is None:
            raise PositionTimingServiceError("QUOTE_UNAVAILABLE", "TDX 实时报价源不可用")
        try:
            current_payload = fetch_quotes_in_contract_chunks(
                symbols=[card.canonical_symbol],
                quote_loader=self.dependencies.realtime_quote_loader,
            )
        except Exception as exc:
            raise PositionTimingServiceError(
                "QUOTE_UNAVAILABLE", "TDX 实时报价重验失败", context={"cause": type(exc).__name__}
            ) from exc
        current_quote, current_status, _ = parse_alert_quote(
            symbol=card.canonical_symbol,
            raw_quote=current_payload.get(card.canonical_symbol),
            evaluated_at=now,
        )
        current_trigger = (
            evaluate_frozen_trigger(card=card, quote=current_quote)[0]
            if current_quote is not None
            else None
        )
        if current_quote is None:
            raise PositionTimingServiceError(current_status, "当前报价不满足提醒新鲜度契约")
        if current_trigger is None or current_trigger.trigger_id != trigger_id:
            raise PositionTimingServiceError("EDGE_NO_LONGER_ELIGIBLE", "价格已离开该冻结触发分支")
        event = AlertEmissionAuthorizedEventV1(
            event_id=f"evt_{canonical_sha256({'event_type': 'ALERT_EMISSION_AUTHORIZED', 'key': key})[:24]}",
            idempotency_key=key,
            occurred_at=now,
            card_id=card.card_id,
            card_artifact_sha256=canonical_sha256(card),
            trigger_id=trigger.trigger_id,
            eligibility_identity=request.eligibility_identity,
            quote_price_raw=request.quote_price_raw,
            quote_open_raw=request.quote_open_raw,
            quote_observed_at=request.quote_observed_at,
            alert_evaluated_at=request.alert_evaluated_at,
            quote_source=request.quote_source,
            staleness_state="FRESH",
            quote_age_seconds=polled_quote.age_seconds,
        )
        authorized, granted = self.store.claim_alert_authorization(event)
        return {
            "schema_version": "position_timing_alert_claim_result_v1",
            "status": "AUTHORIZED" if granted else "ALREADY_AUTHORIZED",
            "granted": granted,
            "event": authorized,
        }

    def _current_alert_snapshot_hashes(
        self, *, card: PositionTimingCardV1, member: _UniverseMember | None
    ) -> dict[str, str]:
        position_snapshot = canonical_sha256(
            {
                "canonical_symbol": card.canonical_symbol,
                "holding": dict(member.holding or {}) if member is not None else {"status": "MISSING"},
                "position_source": POSITION_SOURCE,
            }
        )
        current_intent = self.store.get_intent(card.canonical_symbol)
        intent_snapshot = canonical_sha256(
            {"canonical_symbol": card.canonical_symbol, "intent": current_intent}
        )
        return {
            "position_snapshot_sha256": position_snapshot,
            "intent_snapshot_sha256": intent_snapshot,
        }

    def evidence(self) -> dict[str, Any]:
        outcome_evidence = self._outcome_evidence()
        return {
            "schema_version": "position_timing_evidence_v1",
            "product_evidence_tier": "RULE_BASED_RISK_MANAGEMENT",
            "event_counts": self.store.event_counts(),
            "l2_research_contract_sha256": canonical_sha256(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1),
            "l2_runtime_status": POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.implementation_status,
            "l2_formal_audit": POSITION_TIMING_L2_FORMAL_AUDIT_REFERENCE_V1,
            "hmm_runtime_role": "CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE",
            "selection_runtime_role": "CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE",
            "outcome_evidence": outcome_evidence,
            "cost_disclosure": {
                "min_commission_scope": PERSONAL_MANUAL_COMPONENT_COST_V1["min_commission_scope"],
                "min_commission_scope_verification": PERSONAL_MANUAL_COMPONENT_COST_V1[
                    "min_commission_scope_verification"
                ],
                "thresholds_cny": {
                    "1.00": planned_full_notional_threshold(Decimal("1")),
                    "0.50": planned_full_notional_threshold(Decimal("0.5")),
                    "0.25": planned_full_notional_threshold(Decimal("0.25")),
                },
            },
        }

    def _outcome_evidence(self) -> dict[str, Any]:
        card_sets = self.store.list_card_sets()
        state = self.store.get_materialization_state()
        if not card_sets:
            return {
                "status": "NO_CARD_HISTORY",
                "coverage_counts": {
                    "matured": 0,
                    "pending": 0,
                    "pending_derived": 0,
                    "pending_materialization": 0,
                    "unavailable": 0,
                    "materialization_missing": 0,
                },
                "paired_matured": {"count": 0, "mean_net_lift_bps": None, "median_net_lift_bps": None},
                "intervention_intent": {"count": 0, "mean_net_lift_bps": None, "median_net_lift_bps": None},
                "base_rates": [],
                "materialization_state": state,
            }
        now = self._now()
        try:
            completed_trade_date = self._latest_completed_trade_date(now)
        except PositionTimingServiceError as exc:
            return {
                "status": "OUTCOME_COVERAGE_UNAVAILABLE",
                "reason_codes": [exc.code],
                "materialization_state": state,
            }
        events = {
            str(item["idempotency_key"]): item
            for item in self.store.list_events(event_type="OUTCOME_EVALUATED")
        }
        counts = {
            "matured": 0,
            "pending": 0,
            "pending_derived": 0,
            "pending_materialization": 0,
            "unavailable": 0,
            "materialization_missing": 0,
        }
        paired_values: list[Decimal] = []
        intervention_values: list[Decimal] = []
        grouped: dict[tuple[str, str, str], list[Decimal]] = {}
        calendar_failures: list[str] = []
        for card_set in card_sets:
            if card_set.target_trade_date > completed_trade_date:
                timeline: list[date] = []
            else:
                try:
                    timeline = self.dependencies.calendar_service.list_trading_days(
                        card_set.target_trade_date,
                        completed_trade_date,
                        allow_empty=True,
                    )
                except Exception as exc:
                    calendar_failures.append(f"{card_set.card_set_id}:{type(exc).__name__}")
                    continue
            for card in card_set.cards:
                intervention_intent = any(trigger.planned_delta_qty != 0 for trigger in card.triggers)
                action_side = (
                    "BUY" if card.requested_delta_qty > 0 else "SELL" if card.requested_delta_qty < 0 else "NONE"
                )
                for horizon in (1, 3, 5, 10, 20):
                    key = outcome_event_idempotency_key(card.card_id, horizon)
                    event = events.get(key)
                    if event is not None:
                        maturity = _enum_value(event.get("maturity_status"))
                        value = event.get("net_lift_bps")
                        if maturity == MaturityStatus.UNAVAILABLE_AT_HORIZON.value:
                            counts["unavailable"] += 1
                        else:
                            counts["matured"] += 1
                            if value is not None:
                                lift = Decimal(str(value))
                                paired_values.append(lift)
                                if intervention_intent:
                                    intervention_values.append(lift)
                                    grouped.setdefault(
                                        (
                                            card.primary_source_role.value,
                                            action_side,
                                            card.holding_age_bucket.value,
                                        ),
                                        [],
                                    ).append(lift)
                        continue
                    if len(timeline) < horizon:
                        counts["pending"] += 1
                        counts["pending_derived"] += 1
                        continue
                    nominal = timeline[horizon - 1]
                    watermark = state.last_successful_materialization_scan_through_trade_date
                    if watermark is not None and nominal <= watermark:
                        counts["materialization_missing"] += 1
                    else:
                        counts["pending"] += 1
                        counts["pending_materialization"] += 1
        base_rates = []
        for (role, action_side, age_bucket), values in sorted(grouped.items()):
            positive = sum(1 for value in values if value > 0)
            base_rates.append(
                {
                    "primary_source_role": role,
                    "action_side": action_side,
                    "holding_age_bucket": age_bucket,
                    "sample_count": len(values),
                    "positive_lift_count": positive,
                    "positive_lift_ratio": Decimal(positive) / Decimal(len(values)),
                    "median_lift_bps": _median_decimal(values),
                    "display_status": "AVAILABLE" if len(values) >= 30 else "INSUFFICIENT_HISTORY",
                }
            )
        return {
            "status": "AVAILABLE" if not calendar_failures else "PARTIAL_CALENDAR_UNAVAILABLE",
            "reason_codes": calendar_failures,
            "coverage_counts": counts,
            "paired_matured": _lift_summary(paired_values),
            "intervention_intent": _lift_summary(intervention_values),
            "base_rates": base_rates,
            "materialization_state": state,
        }

    def _build_card(
        self,
        *,
        member: _UniverseMember,
        intent: PositionTimingIntentV1 | None,
        card_set_id: str,
        decision_date: date,
        decision_as_of: datetime,
        target_date: date,
        created_at: datetime,
        position_snapshot_as_of: datetime,
        position_snapshot_sha256: str,
        intent_snapshot_as_of: datetime | None,
        intent_snapshot_sha256: str,
        daily_snapshot: Mapping[str, Any],
        supporting_facts: Mapping[str, Any],
        delist_snapshot: Mapping[str, Any],
        calendar_identity: Mapping[str, Any],
        source_commit: str,
    ) -> PositionTimingCardV1:
        symbol = member.canonical_symbol
        card_id = f"ptcard_{canonical_sha256({'card_set_id': card_set_id, 'symbol': symbol})[:24]}"
        valid_until = datetime.combine(target_date, time(15, 0), tzinfo=CHINA_TZ)
        holding = dict(member.holding or {})
        pre_qty = _non_negative_int(holding.get("quantity"))
        base_reasons: list[str] = ["TARGET_DAY_TRADABILITY_RECHECK_REQUIRED"]
        if member.normalization_reason is not None or not _is_supported_a_share(symbol):
            reason = member.normalization_reason or "UNSUPPORTED_SYMBOL"
            return self._unavailable_card(
                member=member,
                intent=intent,
                card_id=card_id,
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                valid_until=valid_until,
                created_at=created_at,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=position_snapshot_sha256,
                intent_snapshot_as_of=intent_snapshot_as_of,
                intent_snapshot_sha256=intent_snapshot_sha256,
                pre_qty=pre_qty,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
                reason_codes=(reason,),
            )

        daily_row = dict((daily_snapshot.get("rows") or {}).get(symbol) or {})
        st_fact = dict((supporting_facts.get("stock_st_facts") or {}).get(symbol) or {})
        suspend_fact = dict((supporting_facts.get("suspend_facts") or {}).get(symbol) or {})
        delist_fact = dict((delist_snapshot.get("rows") or {}).get(symbol) or {})
        missing: list[str] = []
        reference_price = _positive_decimal(daily_row.get("close"))
        decision_day_suspended = bool(suspend_fact.get("is_suspended"))
        if reference_price is None or not _daily_reference_is_usable(
            daily_row,
            decision_date=decision_date,
            decision_day_suspended=decision_day_suspended,
        ):
            missing.append("DAILY_BAR_UNAVAILABLE")
        elif not _available_by(daily_row.get("feature_available_at"), decision_as_of):
            missing.append("DAILY_BAR_PIT_UNAVAILABLE")
        if not st_fact or "is_st" not in st_fact or not st_fact.get("evidence_hash"):
            missing.append("ST_IDENTITY_UNAVAILABLE")
        elif not _available_by(st_fact.get("feature_available_at"), decision_as_of):
            missing.append("ST_PIT_UNAVAILABLE")
        if not suspend_fact:
            missing.append("SUSPEND_IDENTITY_UNAVAILABLE")
        elif not _available_by(suspend_fact.get("feature_available_at"), decision_as_of):
            missing.append("SUSPEND_PIT_UNAVAILABLE")
        if not delist_fact or "delist_flag" not in delist_fact or not delist_fact.get("evidence_hash"):
            missing.append("DELIST_IDENTITY_UNAVAILABLE")
        elif not _available_by(delist_fact.get("feature_available_at"), decision_as_of):
            missing.append("DELIST_PIT_UNAVAILABLE")
        if member.primary_source_role is SourceRole.HOLDING and pre_qty <= 0:
            missing.append("POSITION_QUANTITY_UNAVAILABLE")
        cost_price = _positive_decimal(holding.get("cost_price"))
        if member.primary_source_role is SourceRole.HOLDING and cost_price is None:
            missing.append("EXIT_GUARD_INPUT_UNAVAILABLE")
        if missing:
            return self._unavailable_card(
                member=member,
                intent=intent,
                card_id=card_id,
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                valid_until=valid_until,
                created_at=created_at,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=position_snapshot_sha256,
                intent_snapshot_as_of=intent_snapshot_as_of,
                intent_snapshot_sha256=intent_snapshot_sha256,
                pre_qty=pre_qty,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
                reason_codes=tuple(missing),
                st_flag=bool(st_fact["is_st"]) if "is_st" in st_fact else None,
                delist_flag=bool(delist_fact["delist_flag"]) if "delist_flag" in delist_fact else None,
                delist_context_status=(
                    TypedStatus.AVAILABLE
                    if "delist_flag" in delist_fact
                    and _available_by(delist_fact.get("feature_available_at"), decision_as_of)
                    else TypedStatus.UNAVAILABLE
                ),
                dataset_identity=_per_card_identity_ref(daily_snapshot.get("identity")),
                limit_identity=_per_card_identity_ref(supporting_facts.get("identity")),
                delist_identity=_per_card_identity_ref(delist_snapshot.get("identity")),
            )
        assert reference_price is not None
        assert cost_price is not None or member.primary_source_role is SourceRole.WATCHLIST

        daily_row_hash = canonical_sha256(daily_row)
        try:
            limit = derive_live_reference_limit_prices(
                ts_code=symbol,
                trade_date=target_date,
                reference_pre_close=reference_price,
                reference_evidence_hash=daily_row_hash,
                price_tick=Decimal("0.01"),
                is_st=bool(st_fact["is_st"]),
            )
            limit_up = limit.up_limit
            limit_down = limit.down_limit
            limit_identity = {
                **limit.canonical_payload(),
                "derivation_hash": limit.derivation_hash,
                "target_day_status": "RECHECK_REQUIRED",
                "supporting_facts_identity_sha256": (supporting_facts.get("identity") or {}).get(
                    "identity_sha256"
                ),
            }
        except Exception as exc:
            return self._unavailable_card(
                member=member,
                intent=intent,
                card_id=card_id,
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                valid_until=valid_until,
                created_at=created_at,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=position_snapshot_sha256,
                intent_snapshot_as_of=intent_snapshot_as_of,
                intent_snapshot_sha256=intent_snapshot_sha256,
                pre_qty=pre_qty,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
                reason_codes=("DAILY_LIMIT_DERIVATION_UNAVAILABLE", type(exc).__name__),
                st_flag=bool(st_fact["is_st"]),
                delist_flag=bool(delist_fact["delist_flag"]),
                delist_context_status=TypedStatus.AVAILABLE,
                dataset_identity=_per_card_identity_ref(daily_snapshot.get("identity")),
                delist_identity=_per_card_identity_ref(delist_snapshot.get("identity")),
            )
        if limit_up is None or limit_down is None:
            return self._unavailable_card(
                member=member,
                intent=intent,
                card_id=card_id,
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                valid_until=valid_until,
                created_at=created_at,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=position_snapshot_sha256,
                intent_snapshot_as_of=intent_snapshot_as_of,
                intent_snapshot_sha256=intent_snapshot_sha256,
                pre_qty=pre_qty,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
                reason_codes=("NO_DAILY_LIMIT_STATUS_UNSUPPORTED",),
                st_flag=bool(st_fact["is_st"]),
                delist_flag=bool(delist_fact["delist_flag"]),
                delist_context_status=TypedStatus.AVAILABLE,
                dataset_identity=_per_card_identity_ref(daily_snapshot.get("identity")),
                limit_identity=limit_identity,
                delist_identity=_per_card_identity_ref(delist_snapshot.get("identity")),
            )

        lot_identity = board_lot_identity(symbol)
        if member.primary_source_role is SourceRole.WATCHLIST and bool(delist_fact["delist_flag"]):
            return self._unavailable_card(
                member=member,
                intent=intent,
                card_id=card_id,
                card_set_id=card_set_id,
                decision_date=decision_date,
                decision_as_of=decision_as_of,
                target_date=target_date,
                valid_until=valid_until,
                created_at=created_at,
                position_snapshot_as_of=position_snapshot_as_of,
                position_snapshot_sha256=position_snapshot_sha256,
                intent_snapshot_as_of=intent_snapshot_as_of,
                intent_snapshot_sha256=intent_snapshot_sha256,
                pre_qty=pre_qty,
                calendar_identity=calendar_identity,
                source_commit=source_commit,
                reason_codes=("CONFIRMED_DELISTING_BUY_UNAVAILABLE",),
                st_flag=bool(st_fact["is_st"]),
                delist_flag=True,
                delist_context_status=TypedStatus.AVAILABLE,
                dataset_identity=_per_card_identity_ref(
                    daily_snapshot.get("identity"), row_sha256=daily_row_hash
                ),
                limit_identity=limit_identity,
                delist_identity=_per_card_identity_ref(delist_snapshot.get("identity"), row=delist_fact),
            )
        sizing_reason: str | None = None
        risk_exit = False
        exit_reason = "HOLD"
        if decision_day_suspended:
            base_reasons.append("DECISION_DAY_SUSPENDED_TARGET_DAY_RECHECK")
            if daily_row.get("trade_date") != decision_date.isoformat():
                base_reasons.append("DECISION_DAY_SUSPENDED_USING_LAST_EXECUTABLE_CLOSE")
        if member.primary_source_role is SourceRole.HOLDING:
            exit_decision = evaluate_exit_guard(
                ExitGuardContext(
                    actual_entry_cost=float(cost_price),
                    current_price=float(reference_price),
                    t1_eligible=True,
                    alpha_decay_confirm_days=0,
                    suspend_status="ACTIVE",
                    st_flag=bool(st_fact["is_st"]),
                    delist_flag=bool(delist_fact["delist_flag"]),
                    price_basis="raw",
                    feature_availability_ts=decision_as_of.isoformat(),
                ),
                frozen_exit_guard_policy(),
            )
            risk_exit = bool(exit_decision.should_exit)
            exit_reason = exit_decision.reason_code

        if intent is None:
            if member.primary_source_role is SourceRole.WATCHLIST:
                return PositionTimingCardV1(
                    card_id=card_id,
                    card_set_id=card_set_id,
                    canonical_symbol=symbol,
                    display_name=member.display_name,
                    primary_source_role=member.primary_source_role,
                    source_roles=member.source_roles,
                    decision_trade_date=decision_date,
                    decision_as_of=decision_as_of,
                    target_trade_date=target_date,
                    valid_until=valid_until,
                    created_at=created_at,
                    position_snapshot_as_of=position_snapshot_as_of,
                    pre_action_qty=0,
                    t1_sellable_qty=0,
                    pre_action_exposure=Decimal("0"),
                    planned_full_notional_cny=None,
                    desired_target_exposure=None,
                    requested_delta_qty=0,
                    requested_leg_notional_cny=Decimal("0"),
                    action=TimingAction.WAIT,
                    execution_window=ExecutionWindow.WAIT_UNAVAILABLE,
                    reference_price_raw=reference_price,
                    tradability_status=TradabilityStatus.TARGET_DAY_RECHECK_REQUIRED,
                    st_flag=bool(st_fact["is_st"]),
                    delist_flag=bool(delist_fact["delist_flag"]),
                    delist_context_status=TypedStatus.AVAILABLE,
                    limit_up_raw=limit_up,
                    limit_down_raw=limit_down,
                    reason_codes=("SIZING_INPUT_UNAVAILABLE", *base_reasons),
                    position_snapshot_sha256=position_snapshot_sha256,
                    intent_snapshot_sha256=intent_snapshot_sha256,
                    dataset_identity=_per_card_identity_ref(
                        daily_snapshot.get("identity"), row_sha256=daily_row_hash
                    ),
                    calendar_identity=dict(calendar_identity),
                    limit_identity=limit_identity,
                    adjustment_identity=dict(CARD_ADJUSTMENT_NOT_APPLICABLE_V1),
                    delist_identity=_per_card_identity_ref(delist_snapshot.get("identity"), row=delist_fact),
                    board_lot_identity=lot_identity,
                    price_guard_snapshot_sha256=PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
                    exit_guard_snapshot_sha256=EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
                    cost_policy_sha256=COST_POLICY_SHA256,
                    source_repository_commit=source_commit,
                )
            full_notional = reference_price * pre_qty
            desired_exposure = Decimal("1")
        else:
            full_notional = intent.planned_full_notional_cny
            desired_exposure = intent.desired_target_exposure

        pre_value = reference_price * pre_qty
        pre_exposure = pre_value / full_notional if full_notional > 0 else Decimal("0")
        if risk_exit:
            target_qty = 0
            requested_delta = -pre_qty
            action = TimingAction.EXIT
        elif intent is None:
            target_qty = pre_qty
            requested_delta = 0
            action = TimingAction.HOLD
            base_reasons.append("RISK_GUARD_HOLD_CURRENT_POSITION")
        else:
            intended_notional = full_notional * desired_exposure
            ideal_target_qty = legal_target_quantity(
                notional_cny=intended_notional, price_raw=reference_price, symbol=symbol
            )
            raw_delta = ideal_target_qty - pre_qty
            if raw_delta > 0:
                requested_delta = legal_target_quantity(
                    notional_cny=reference_price * raw_delta,
                    price_raw=reference_price,
                    symbol=symbol,
                )
            elif raw_delta < 0:
                from backend.execution_algos.board_lot import round_to_board_lot

                full_exit = ideal_target_qty == 0
                sell_qty = pre_qty if full_exit else round_to_board_lot(
                    -raw_delta, symbol, side="SELL", allow_sell_residual=False
                )
                requested_delta = -min(pre_qty, sell_qty)
            else:
                requested_delta = 0
            if requested_delta == 0:
                if pre_qty == 0 and intended_notional > 0:
                    sizing_reason = "TARGET_QUANTITY_BELOW_BOARD_MINIMUM"
                elif desired_exposure != pre_exposure:
                    sizing_reason = "DELTA_BELOW_BOARD_LOT"
                else:
                    sizing_reason = "TARGET_EXPOSURE_ALREADY_SATISFIED"
            target_qty = pre_qty + requested_delta
            if requested_delta > 0:
                action = TimingAction.OPEN if pre_qty == 0 else TimingAction.ADD
            elif requested_delta < 0:
                action = TimingAction.EXIT if target_qty == 0 else TimingAction.REDUCE
            else:
                action = TimingAction.HOLD if pre_qty > 0 else TimingAction.WAIT

        requested_notional = reference_price * abs(requested_delta)
        triggers: tuple[TriggerV1, ...] = ()
        execution_window = ExecutionWindow.WAIT_UNAVAILABLE
        cost_estimate = None
        trigger_cost_estimates: dict[str, LegCostEstimateV1] = {}
        if requested_delta > 0:
            execution_window = ExecutionWindow.ON_PRICE_TRIGGER
            price_guard_policy = frozen_price_guard_policy()
            buy_policy = dict(price_guard_policy.buy)
            max_buy = _floor_to_tick(
                min(
                    reference_price
                    * (Decimal("1") + Decimal(str(buy_policy["max_chase_bps"])) / Decimal("10000")),
                    limit_up,
                )
            )
            raw_green_buy = min(
                reference_price
                * (Decimal("1") + Decimal(str(buy_policy["yellow_chase_bps"])) / Decimal("10000")),
                max_buy,
            )
            green_buy = _floor_to_tick(raw_green_buy)
            boundary_action: str | None = None
            for _ in range(2):
                if green_buy <= 0:
                    break
                boundary_decision = evaluate_price_guard(
                    PriceGuardContext(
                        signal_ref_price=float(reference_price),
                        prev_close=float(reference_price),
                        current_price=float(green_buy),
                        limit_up=float(limit_up),
                        limit_down=float(limit_down),
                        st_flag=bool(st_fact["is_st"]),
                        side="buy",
                        price_basis="raw",
                        feature_availability_ts=decision_as_of.isoformat(),
                    ),
                    price_guard_policy,
                )
                boundary_action = boundary_decision.action
                if boundary_decision.action == "ACCEPT":
                    break
                green_buy = _floor_to_tick(green_buy - Decimal("0.01"))
            if green_buy <= 0 or boundary_action != "ACCEPT":
                raise PositionTimingServiceError(
                    "PRICE_GUARD_GREEN_BRANCH_UNAVAILABLE",
                    "冻结 price guard 无法形成合法的 green trigger 价格",
                    context={"canonical_symbol": symbol, "reference_price_raw": str(reference_price)},
                )
            yellow_qty = legal_target_quantity(
                notional_cny=(
                    reference_price * requested_delta * Decimal(str(buy_policy["yellow_size_multiplier"]))
                ),
                price_raw=reference_price,
                symbol=symbol,
            )
            triggers = (
                self._trigger(
                    card_id=card_id,
                    branch="BUY_GREEN_ACCEPT",
                    side=TriggerSide.BUY,
                    operator=TriggerOperator.LTE,
                    trigger_price=green_buy,
                    guard_action="ACCEPT",
                    planned_delta=requested_delta,
                    reference_price=reference_price,
                    full_notional=full_notional,
                    pre_qty=pre_qty,
                    reason="ACCEPT_WITHIN_GREEN_ZONE",
                    conditions={
                        "shared_price_guard_authority": True,
                        "price_guard_action": "ACCEPT",
                        "price_guard_reason_codes": ["ACCEPT_WITHIN_GREEN_ZONE"],
                    },
                ),
                self._trigger(
                    card_id=card_id,
                    branch="BUY_YELLOW_REDUCE",
                    side=TriggerSide.BUY,
                    operator=TriggerOperator.LTE,
                    trigger_price=max_buy,
                    guard_action="REDUCE" if yellow_qty > 0 else "SKIP",
                    planned_delta=yellow_qty,
                    reference_price=reference_price,
                    full_notional=full_notional,
                    pre_qty=pre_qty,
                    reason=(
                        "REDUCE_YELLOW_GUARD_BRANCH"
                        if yellow_qty > 0
                        else "REDUCE_BRANCH_BELOW_BOARD_LOT_SKIP"
                    ),
                    conditions={
                        "size_multiplier": buy_policy["yellow_size_multiplier"],
                        "shared_price_guard_authority": True,
                        "price_guard_action": "REDUCE",
                        "timing_branch_action": "REDUCE" if yellow_qty > 0 else "SKIP",
                        "price_guard_reason_codes": [
                            "REDUCE_YELLOW_OPEN_GAP",
                            "REDUCE_YELLOW_CHASE_BAND",
                        ],
                    },
                ),
                self._trigger(
                    card_id=card_id,
                    branch="BUY_SKIP",
                    side=TriggerSide.BUY,
                    operator=TriggerOperator.NEVER,
                    trigger_price=max_buy,
                    guard_action="SKIP",
                    planned_delta=0,
                    reference_price=reference_price,
                    full_notional=full_notional,
                    pre_qty=pre_qty,
                    reason="SKIP_PRICE_GUARD_BRANCH",
                    conditions={
                        "shared_price_guard_authority": True,
                        "price_guard_action": "SKIP",
                        "price_guard_reason_codes": [
                            "SKIP_OPEN_GAP_EXCEEDED",
                            "SKIP_ABOVE_MAX_BUY_PRICE",
                            "SKIP_NEAR_LIMIT_UP",
                        ],
                    },
                ),
            )
            cost_estimate = estimate_leg_cost(
                side=TriggerSide.BUY,
                quantity=requested_delta,
                reference_price_raw=reference_price,
                symbol=symbol,
            )
            for trigger in triggers:
                if trigger.planned_delta_qty <= 0:
                    continue
                assert trigger.trigger_price_raw is not None
                trigger_cost_estimates[trigger.trigger_id] = estimate_leg_cost(
                    side=TriggerSide.BUY,
                    quantity=trigger.planned_delta_qty,
                    reference_price_raw=trigger.trigger_price_raw,
                    symbol=symbol,
                )
        elif requested_delta < 0:
            full_exit = target_qty == 0
            sell_policy = dict(frozen_price_guard_policy().sell)
            if risk_exit:
                execution_window = ExecutionWindow.AT_OPEN
                triggers = (
                    self._trigger(
                        card_id=card_id,
                        branch="RISK_EXIT_AT_OPEN",
                        side=TriggerSide.SELL,
                        operator=TriggerOperator.ALWAYS,
                        trigger_price=reference_price,
                        guard_action="SELL",
                        planned_delta=requested_delta,
                        reference_price=reference_price,
                        full_notional=full_notional,
                        pre_qty=pre_qty,
                        reason=exit_reason,
                        conditions={
                            "sell_reason": "risk_exit",
                            "shared_price_guard_authority": True,
                            "price_guard_action": "SELL",
                            "price_guard_reason_codes": ["EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT"],
                        },
                    ),
                )
            else:
                execution_window = ExecutionWindow.ON_PRICE_TRIGGER
                min_sell = _ceil_to_tick(
                    reference_price
                    * (
                        Decimal("1")
                        - Decimal(str(sell_policy["rebalance_max_slippage_bps"])) / Decimal("10000")
                    )
                )
                triggers = (
                    self._trigger(
                        card_id=card_id,
                        branch="SELL_REBALANCE_ACCEPT",
                        side=TriggerSide.SELL,
                        operator=TriggerOperator.GTE,
                        trigger_price=min_sell,
                        guard_action="SELL",
                        planned_delta=requested_delta,
                        reference_price=reference_price,
                        full_notional=full_notional,
                        pre_qty=pre_qty,
                        reason="ACCEPT_WITHIN_GREEN_ZONE",
                        conditions={
                            "sell_reason": "rebalance",
                            "shared_price_guard_authority": True,
                            "price_guard_action": "SELL",
                            "price_guard_reason_codes": ["ACCEPT_WITHIN_GREEN_ZONE"],
                        },
                    ),
                    self._trigger(
                        card_id=card_id,
                        branch="SELL_REBALANCE_SKIP",
                        side=TriggerSide.SELL,
                        operator=TriggerOperator.NEVER,
                        trigger_price=min_sell,
                        guard_action="SKIP",
                        planned_delta=0,
                        reference_price=reference_price,
                        full_notional=full_notional,
                        pre_qty=pre_qty,
                        reason="SKIP_BELOW_MIN_SELL_PRICE_REBALANCE",
                        conditions={
                            "sell_reason": "rebalance",
                            "shared_price_guard_authority": True,
                            "price_guard_action": "SKIP",
                            "price_guard_reason_codes": ["SKIP_BELOW_MIN_SELL_PRICE_REBALANCE"],
                        },
                    ),
                )
            cost_estimate = estimate_leg_cost(
                side=TriggerSide.SELL,
                quantity=-requested_delta,
                reference_price_raw=reference_price,
                symbol=symbol,
                full_exit=full_exit,
            )
            for trigger in triggers:
                if trigger.planned_delta_qty >= 0:
                    continue
                assert trigger.trigger_price_raw is not None
                trigger_cost_estimates[trigger.trigger_id] = estimate_leg_cost(
                    side=TriggerSide.SELL,
                    quantity=-trigger.planned_delta_qty,
                    reference_price_raw=trigger.trigger_price_raw,
                    symbol=symbol,
                    full_exit=full_exit,
                )
        if cost_estimate is not None and cost_estimate.small_trade_cost_heavy:
            base_reasons.append("SMALL_TRADE_COST_HEAVY")
        if risk_exit:
            base_reasons.append(exit_reason)
        if sizing_reason is not None:
            base_reasons.append(sizing_reason)

        return PositionTimingCardV1(
            card_id=card_id,
            card_set_id=card_set_id,
            canonical_symbol=symbol,
            display_name=member.display_name,
            primary_source_role=member.primary_source_role,
            source_roles=member.source_roles,
            decision_trade_date=decision_date,
            decision_as_of=decision_as_of,
            target_trade_date=target_date,
            valid_until=valid_until,
            created_at=created_at,
            position_snapshot_as_of=position_snapshot_as_of,
            intent_snapshot_as_of=intent.updated_at if intent else intent_snapshot_as_of,
            pre_action_qty=pre_qty,
            t1_sellable_qty=pre_qty,
            pre_action_exposure=pre_exposure,
            planned_full_notional_cny=full_notional,
            desired_target_exposure=desired_exposure,
            requested_delta_qty=requested_delta,
            requested_leg_notional_cny=requested_notional,
            action=action,
            execution_window=execution_window,
            reference_price_raw=reference_price,
            triggers=triggers,
            tradability_status=TradabilityStatus.TARGET_DAY_RECHECK_REQUIRED,
            st_flag=bool(st_fact["is_st"]),
            delist_flag=bool(delist_fact["delist_flag"]),
            delist_context_status=TypedStatus.AVAILABLE,
            limit_up_raw=limit_up,
            limit_down_raw=limit_down,
            reason_codes=tuple(dict.fromkeys(base_reasons)),
            cost_estimate=cost_estimate,
            trigger_cost_estimates=trigger_cost_estimates,
            holding_age_bucket=HoldingAgeBucket.UNKNOWN,
            market_regime=MarketRegime.UNKNOWN,
            market_regime_status=TypedStatus.UNAVAILABLE,
            selection_context_status=TypedStatus.UNAVAILABLE,
            hmm_context_status=TypedStatus.UNAVAILABLE,
            position_snapshot_sha256=position_snapshot_sha256,
            intent_snapshot_sha256=intent_snapshot_sha256,
            dataset_identity=_per_card_identity_ref(
                daily_snapshot.get("identity"), row_sha256=daily_row_hash
            ),
            calendar_identity=dict(calendar_identity),
            limit_identity=limit_identity,
            adjustment_identity=dict(CARD_ADJUSTMENT_NOT_APPLICABLE_V1),
            delist_identity=_per_card_identity_ref(delist_snapshot.get("identity"), row=delist_fact),
            board_lot_identity=lot_identity,
            price_guard_snapshot_sha256=PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            exit_guard_snapshot_sha256=EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            cost_policy_sha256=COST_POLICY_SHA256,
            source_repository_commit=source_commit,
        )

    @staticmethod
    def _trigger(
        *,
        card_id: str,
        branch: str,
        side: TriggerSide,
        operator: TriggerOperator,
        trigger_price: Decimal,
        guard_action: str,
        planned_delta: int,
        reference_price: Decimal,
        full_notional: Decimal,
        pre_qty: int,
        reason: str,
        conditions: dict[str, Any],
    ) -> TriggerV1:
        target_qty = pre_qty + planned_delta
        branch_price = trigger_price if planned_delta != 0 else reference_price
        target_exposure = branch_price * target_qty / full_notional if full_notional > 0 else Decimal("0")
        trigger_id = f"pttrg_{canonical_sha256({'card_id': card_id, 'branch': branch})[:24]}"
        return TriggerV1(
            trigger_id=trigger_id,
            branch=branch,
            side=side,
            operator=operator,
            trigger_price_raw=trigger_price,
            guard_action=guard_action,
            planned_delta_qty=planned_delta,
            planned_leg_notional_cny=branch_price * abs(planned_delta),
            target_exposure=target_exposure,
            conditions=conditions,
            reason_code=reason,
        )

    @staticmethod
    def _unavailable_card(
        *,
        member: _UniverseMember,
        intent: PositionTimingIntentV1 | None,
        card_id: str,
        card_set_id: str,
        decision_date: date,
        decision_as_of: datetime,
        target_date: date,
        valid_until: datetime,
        created_at: datetime,
        position_snapshot_as_of: datetime,
        position_snapshot_sha256: str,
        intent_snapshot_as_of: datetime | None,
        intent_snapshot_sha256: str,
        pre_qty: int,
        calendar_identity: Mapping[str, Any],
        source_commit: str,
        reason_codes: tuple[str, ...],
        st_flag: bool | None = None,
        delist_flag: bool | None = None,
        delist_context_status: TypedStatus = TypedStatus.UNAVAILABLE,
        dataset_identity: Mapping[str, Any] | None = None,
        limit_identity: Mapping[str, Any] | None = None,
        delist_identity: Mapping[str, Any] | None = None,
    ) -> PositionTimingCardV1:
        return PositionTimingCardV1(
            card_id=card_id,
            card_set_id=card_set_id,
            canonical_symbol=member.canonical_symbol,
            display_name=member.display_name,
            primary_source_role=member.primary_source_role,
            source_roles=member.source_roles,
            decision_trade_date=decision_date,
            decision_as_of=decision_as_of,
            target_trade_date=target_date,
            valid_until=valid_until,
            created_at=created_at,
            position_snapshot_as_of=position_snapshot_as_of,
            intent_snapshot_as_of=intent.updated_at if intent else intent_snapshot_as_of,
            pre_action_qty=pre_qty,
            t1_sellable_qty=pre_qty,
            pre_action_exposure=Decimal("0"),
            planned_full_notional_cny=intent.planned_full_notional_cny if intent else None,
            desired_target_exposure=intent.desired_target_exposure if intent else None,
            requested_delta_qty=0,
            requested_leg_notional_cny=Decimal("0"),
            action=TimingAction.UNAVAILABLE,
            execution_window=ExecutionWindow.WAIT_UNAVAILABLE,
            tradability_status=TradabilityStatus.UNAVAILABLE,
            st_flag=st_flag,
            delist_flag=delist_flag,
            delist_context_status=delist_context_status,
            reason_codes=reason_codes,
            position_snapshot_sha256=position_snapshot_sha256,
            intent_snapshot_sha256=intent_snapshot_sha256,
            dataset_identity=dict(dataset_identity or {"status": "UNAVAILABLE"}),
            calendar_identity=dict(calendar_identity),
            limit_identity=dict(limit_identity or {"status": "UNAVAILABLE"}),
            adjustment_identity=dict(CARD_ADJUSTMENT_NOT_APPLICABLE_V1),
            delist_identity=dict(delist_identity or {"status": "UNAVAILABLE"}),
            board_lot_identity={"status": "UNAVAILABLE"},
            price_guard_snapshot_sha256=PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            exit_guard_snapshot_sha256=EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
            cost_policy_sha256=COST_POLICY_SHA256,
            source_repository_commit=source_commit,
        )

    def _load_universe(self) -> tuple[tuple[_UniverseMember, ...], str]:
        try:
            holding_rows = list(self.dependencies.holdings_loader())
        except Exception as exc:
            raise PositionTimingServiceError(
                "LEGACY_PORTFOLIO_UNAVAILABLE", "无法读取唯一持仓账本", context={"cause": type(exc).__name__}
            ) from exc
        watchlist_rows: list[dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            try:
                payload = self.dependencies.watchlist_page_loader(page, page_size)
            except Exception as exc:
                raise PositionTimingServiceError(
                    "CONFIRMED_WATCHLIST_UNAVAILABLE",
                    "无法读取已确认自选池",
                    context={"cause": type(exc).__name__, "page": page},
                ) from exc
            items = list(payload.get("items") or [])
            watchlist_rows.extend(items)
            total = int(payload.get("total") or len(watchlist_rows))
            if len(watchlist_rows) >= total or not items:
                break
            page += 1
            if page > 1000:
                raise PositionTimingServiceError("WATCHLIST_PAGINATION_INVALID", "自选池分页未能收敛")

        by_symbol: dict[str, _UniverseMember] = {}
        for row in holding_rows:
            symbol, reason = normalize_position_symbol(str(row.get("code") or ""))
            if symbol is not None and reason is None and not _is_supported_a_share(symbol):
                reason = "UNSUPPORTED_SECURITY_FIRST_RELEASE"
            identity = symbol or _unsupported_identity(str(row.get("code") or ""))
            if identity in by_symbol:
                raise PositionTimingServiceError(
                    "POSITION_IDENTITY_CONFLICT",
                    "持仓账本在 canonical symbol 归一化后出现重复",
                    context={"canonical_symbol": identity},
                )
            by_symbol[identity] = _UniverseMember(
                canonical_symbol=identity,
                display_name=_clean_optional_text(row.get("name")),
                primary_source_role=SourceRole.HOLDING,
                source_roles=(SourceRole.HOLDING,),
                holding=dict(row),
                normalization_reason=reason,
            )
        for row in watchlist_rows:
            if not bool(row.get("advisory_enabled")):
                continue
            if str(row.get("lifecycle_status") or "").strip().upper() not in ACTIVE_WATCHLIST_LIFECYCLES:
                continue
            symbol, reason = normalize_position_symbol(str(row.get("code") or ""))
            if symbol is not None and reason is None and not _is_supported_a_share(symbol):
                reason = "UNSUPPORTED_SECURITY_FIRST_RELEASE"
            identity = symbol or _unsupported_identity(str(row.get("code") or ""))
            existing = by_symbol.get(identity)
            if existing is not None:
                if SourceRole.WATCHLIST not in existing.source_roles:
                    by_symbol[identity] = _UniverseMember(
                        canonical_symbol=existing.canonical_symbol,
                        display_name=existing.display_name or _clean_optional_text(row.get("name")),
                        primary_source_role=existing.primary_source_role,
                        source_roles=(SourceRole.HOLDING, SourceRole.WATCHLIST),
                        holding=existing.holding,
                        watchlist=dict(row),
                        normalization_reason=existing.normalization_reason or reason,
                    )
                continue
            by_symbol[identity] = _UniverseMember(
                canonical_symbol=identity,
                display_name=_clean_optional_text(row.get("name")),
                primary_source_role=SourceRole.WATCHLIST,
                source_roles=(SourceRole.WATCHLIST,),
                watchlist=dict(row),
                normalization_reason=reason,
            )
        members = tuple(
            sorted(
                by_symbol.values(),
                key=lambda member: (
                    0 if member.primary_source_role is SourceRole.HOLDING else 1,
                    member.canonical_symbol,
                ),
            )
        )
        identity = canonical_sha256(
            [
                {
                    "canonical_symbol": member.canonical_symbol,
                    "primary_source_role": member.primary_source_role,
                    "source_roles": member.source_roles,
                    "display_name": member.display_name,
                    "holding_id": (member.holding or {}).get("id"),
                    "watchlist_id": (member.watchlist or {}).get("id"),
                    "normalization_reason": member.normalization_reason,
                }
                for member in members
            ]
        )
        return members, identity

    @staticmethod
    def _analysis_members(
        *,
        members: tuple[_UniverseMember, ...],
        scope: PositionTimingAnalysisScopeV1,
    ) -> tuple[tuple[_UniverseMember, ...], str]:
        selected = set(scope.selected_watchlist_symbols)
        analysis_members = tuple(
            member
            for member in members
            if member.primary_source_role is SourceRole.HOLDING
            or (
                SourceRole.WATCHLIST in member.source_roles
                and member.canonical_symbol in selected
            )
        )
        identity = canonical_sha256(
            [
                {
                    "canonical_symbol": member.canonical_symbol,
                    "primary_source_role": member.primary_source_role,
                    "source_roles": member.source_roles,
                    "display_name": member.display_name,
                    "holding_id": (member.holding or {}).get("id"),
                    "watchlist_id": (member.watchlist or {}).get("id"),
                    "normalization_reason": member.normalization_reason,
                }
                for member in analysis_members
            ]
        )
        return analysis_members, identity

    def _resolve_decision_clock(self, now: datetime) -> tuple[date, datetime, date, dict[str, Any]]:
        try:
            status = self.dependencies.calendar_service.status(as_of_date=now.date())
            if status.get("is_trading_day") and now.time() >= time(15, 0):
                decision_date = now.date()
            elif status.get("is_trading_day"):
                previous = status.get("previous_trading_day")
                if not previous:
                    raise PositionTimingServiceError("DECISION_TRADE_DATE_UNAVAILABLE", "交易日前一交易日不可用")
                decision_date = date.fromisoformat(str(previous))
            else:
                latest = status.get("latest_completed_trading_day")
                if not latest:
                    raise PositionTimingServiceError("DECISION_TRADE_DATE_UNAVAILABLE", "最近完成交易日不可用")
                decision_date = date.fromisoformat(str(latest))
            target_date = self.dependencies.calendar_service.next_trading_day(decision_date)
        except PositionTimingServiceError:
            raise
        except Exception as exc:
            raise PositionTimingServiceError(
                "TRADING_CALENDAR_UNAVAILABLE", "全局交易日服务不可用", context={"cause": type(exc).__name__}
            ) from exc
        decision_as_of = datetime.combine(decision_date, time(15, 0), tzinfo=CHINA_TZ)
        identity_payload = {
            "source": status.get("source"),
            "timezone": status.get("timezone"),
            "cache": status.get("cache"),
            "decision_trade_date": decision_date.isoformat(),
            "target_trade_date": target_date.isoformat(),
        }
        return decision_date, decision_as_of, target_date, {
            **identity_payload,
            "identity_sha256": canonical_sha256(identity_payload),
        }

    @staticmethod
    def _safe_batch_load(
        loader: Callable[[list[str], date], dict[str, Any]],
        symbols: list[str],
        trade_date: date,
        *,
        unavailable_code: str,
    ) -> dict[str, Any]:
        if not symbols:
            payload = {"status": "NOT_APPLICABLE", "rows": {}, "symbol_set": []}
            return {**payload, "identity": {"status": "NOT_APPLICABLE", "identity_sha256": canonical_sha256(payload)}}
        try:
            loaded = loader(symbols, trade_date)
            if not isinstance(loaded, dict):
                raise TypeError("batch loader must return a dictionary payload")
            return loaded
        except Exception as exc:
            payload = {
                "status": "UNAVAILABLE",
                "reason_code": unavailable_code,
                "cause": type(exc).__name__,
                "symbol_set": symbols,
                "trade_date": trade_date.isoformat(),
                "rows": {},
            }
            return {**payload, "identity": {**payload, "identity_sha256": canonical_sha256(payload)}}

    def _now(self) -> datetime:
        value = self.dependencies.now_provider()
        if value.tzinfo is None:
            raise PositionTimingServiceError("CLOCK_TIMEZONE_REQUIRED", "position timing clock must be timezone-aware")
        return value.astimezone(CHINA_TZ)


def normalize_position_symbol(raw_symbol: str) -> tuple[str | None, str | None]:
    raw = str(raw_symbol or "").strip().upper()
    if not raw:
        return None, "UNSUPPORTED_SYMBOL"
    if "." in raw or (len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"}):
        normalized = raw if "." in raw else f"{raw[2:]}.{raw[:2]}"
    elif len(raw) == 6 and raw.isdigit():
        if raw.startswith(("5", "6", "9")):
            normalized = f"{raw}.SH"
        elif raw.startswith(("0", "1", "2", "3")):
            normalized = f"{raw}.SZ"
        elif raw.startswith(("4", "8")):
            normalized = f"{raw}.BJ"
        else:
            return None, "UNSUPPORTED_SYMBOL"
    else:
        return None, "UNSUPPORTED_SYMBOL"
    try:
        validated = normalize_and_validate_ts_codes(
            [normalized], source="position_timing.universe", allow_empty=False
        )[0]
    except ValueError:
        return None, "UNSUPPORTED_SYMBOL"
    if validated.endswith(".BJ"):
        return validated, "UNSUPPORTED_BJ_FIRST_RELEASE"
    return validated, None


def _is_supported_a_share(symbol: str) -> bool:
    if symbol.endswith(".SH"):
        return symbol.startswith(("600", "601", "603", "605", "688", "689"))
    if symbol.endswith(".SZ"):
        return symbol.startswith(("000", "001", "002", "003", "300", "301", "302"))
    return False


def _unsupported_identity(raw_symbol: str) -> str:
    return f"UNSUPPORTED-{canonical_sha256({'raw_symbol': raw_symbol})[:16]}"


def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _positive_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _clean_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _available_by(value: Any, decision_as_of: datetime) -> bool:
    try:
        observed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return False
    if observed.tzinfo is None:
        return False
    return observed.astimezone(CHINA_TZ) <= decision_as_of


def _daily_reference_is_usable(
    row: Any,
    *,
    decision_date: date,
    decision_day_suspended: bool,
) -> bool:
    if not isinstance(row, Mapping) or _positive_decimal(row.get("close")) is None:
        return False
    try:
        observed_date = date.fromisoformat(str(row.get("trade_date")))
    except ValueError:
        return False
    if observed_date == decision_date:
        return True
    return (
        decision_day_suspended
        and observed_date < decision_date
        and row.get("reference_state") == "LAST_EXECUTABLE_CLOSE"
    )


def _per_card_identity_ref(identity: Any, **details: Any) -> dict[str, Any]:
    source = dict(identity or {})
    reference = {
        key: source[key]
        for key in (
            "schema_version",
            "source",
            "table_contract",
            "trade_date",
            "status",
            "reason_code",
            "identity_sha256",
        )
        if key in source
    }
    return {**reference, **details}


def _has_valid_batch_identity(payload: Mapping[str, Any]) -> bool:
    identity = payload.get("identity")
    if not isinstance(identity, Mapping):
        return False
    material = dict(identity)
    claimed = str(material.pop("identity_sha256", "")).strip().lower()
    return len(claimed) == 64 and claimed == canonical_sha256(material)


def _valid_identity_hash(identity: Mapping[str, Any]) -> bool:
    material = dict(identity)
    claimed = str(material.pop("identity_sha256", "")).strip().lower()
    return len(claimed) == 64 and claimed == canonical_sha256(material)


def _valid_outcome_snapshot_identity(snapshot: Mapping[str, Any]) -> bool:
    if not _has_valid_batch_identity(snapshot):
        return False
    rows = snapshot.get("rows")
    identity = snapshot.get("identity")
    adjustment_identity = snapshot.get("adjustment_identity")
    limit_identity = snapshot.get("limit_identity")
    if not isinstance(rows, Mapping) or not isinstance(identity, Mapping):
        return False
    if str(identity.get("rows_sha256") or "") != canonical_sha256(dict(rows)):
        return False
    if not isinstance(adjustment_identity, Mapping) or not _valid_identity_hash(adjustment_identity):
        return False
    if not isinstance(limit_identity, Mapping) or not _valid_identity_hash(limit_identity):
        return False
    adjustment_rows: dict[str, dict[str, str]] = {}
    limit_rows: dict[str, dict[str, dict[str, str]]] = {}
    for symbol, by_date in rows.items():
        if not isinstance(by_date, Mapping):
            return False
        values: dict[str, str] = {}
        symbol_limits: dict[str, dict[str, str]] = {}
        for trade_date, row in by_date.items():
            if not isinstance(row, Mapping):
                return False
            factor = _positive_decimal(row.get("adj_factor"))
            if factor is not None:
                values[str(trade_date)] = format(factor, "f")
            up_limit = _positive_decimal(row.get("up_limit"))
            down_limit = _positive_decimal(row.get("down_limit"))
            if up_limit is not None and down_limit is not None:
                symbol_limits[str(trade_date)] = {
                    "up_limit": format(up_limit, "f"),
                    "down_limit": format(down_limit, "f"),
                }
        adjustment_rows[str(symbol)] = values
        limit_rows[str(symbol)] = symbol_limits
    return (
        str(adjustment_identity.get("rows_sha256") or "") == canonical_sha256(adjustment_rows)
        and str(limit_identity.get("rows_sha256") or "") == canonical_sha256(limit_rows)
    )


def _identity_hash(identity: Any) -> str:
    material = dict(identity or {})
    if _valid_identity_hash(material):
        return str(material["identity_sha256"])
    return canonical_sha256(material)


def _outcome_row(snapshot: Mapping[str, Any], symbol: str, trade_date: date) -> dict[str, Any] | None:
    symbol_rows = (snapshot.get("rows") or {}).get(symbol)
    if not isinstance(symbol_rows, Mapping):
        return None
    row = symbol_rows.get(trade_date.isoformat())
    return dict(row) if isinstance(row, Mapping) else None


def _terminal_sellability_status(row: Mapping[str, Any] | None) -> str:
    if not isinstance(row, Mapping):
        return "TERMINAL_MARKET_DATA_UNAVAILABLE"
    if bool(row.get("is_suspended")):
        return "TERMINAL_SUSPENDED"
    prices = {
        name: _positive_decimal(row.get(name))
        for name in ("open", "high", "low", "close")
    }
    if any(value is None for value in prices.values()):
        return "TERMINAL_MARKET_DATA_UNAVAILABLE"
    if _positive_decimal(row.get("adj_factor")) is None:
        return "TERMINAL_ADJUSTMENT_UNAVAILABLE"
    down_limit = _positive_decimal(row.get("down_limit"))
    if down_limit is None:
        return "TERMINAL_LIMIT_AUTHORITY_UNAVAILABLE"
    if all(abs(value - down_limit) < Decimal("0.005") for value in prices.values() if value is not None):
        return "TERMINAL_ONE_WORD_LIMIT_DOWN"
    return "SELLABLE"


def _enum_value(value: Any) -> str:
    return str(value.value) if hasattr(value, "value") else str(value)


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _lift_summary(values: list[Decimal]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "mean_net_lift_bps": None, "median_net_lift_bps": None}
    return {
        "count": len(values),
        "mean_net_lift_bps": sum(values, Decimal("0")) / Decimal(len(values)),
        "median_net_lift_bps": _median_decimal(values),
    }


def _floor_to_tick(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_FLOOR)


def _ceil_to_tick(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def _default_daily_snapshot_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    from datetime import datetime as dt

    import pandas as pd

    from backend.data_service.timescaledb_adapter import fetch_history_window_ts

    validated = normalize_and_validate_ts_codes(
        symbols, source="position_timing.daily_snapshot", start_date=trade_date, end_date=trade_date, allow_empty=False
    )
    frame = fetch_history_window_ts(
        validated,
        start=dt.combine(trade_date, time.min),
        end=dt.combine(trade_date, time.max),
        fields=["open", "high", "low", "close", "volume", "amount"],
        freq="1d",
        adj="none",
    )
    rows: dict[str, dict[str, Any]] = {}
    if not frame.empty:
        reset = frame.reset_index()
        for _, row in reset.iterrows():
            symbol = str(row.get("instrument") or row.get("ts_code") or "").strip().upper()
            observed_date = pd.Timestamp(row.get("datetime") or row.get("trade_date")).date()
            if symbol not in validated or observed_date != trade_date:
                continue
            if symbol in rows:
                raise ValueError(f"DAILY_BAR_IDENTITY_CONFLICT:{symbol}:{trade_date.isoformat()}")
            values = {
                key: (None if pd.isna(row.get(key)) else float(row.get(key)))
                for key in ("open", "high", "low", "close", "volume", "amount")
            }
            rows[symbol] = {
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                **values,
                "price_basis": "raw_cny",
                "reference_state": "DECISION_DAY_CLOSE",
                "feature_available_at": datetime.combine(trade_date, time(15, 0), tzinfo=CHINA_TZ).isoformat(),
            }
    missing_symbols = sorted(set(validated) - set(rows))
    if missing_symbols:
        fallback = fetch_history_window_ts(
            missing_symbols,
            end=dt.combine(trade_date, time.max),
            bars=120,
            fields=["close"],
            freq="1d",
            adj="none",
        )
        if not fallback.empty:
            latest = (
                fallback.reset_index()
                .sort_values(["instrument", "datetime"])
                .groupby("instrument")
                .tail(1)
            )
            for _, row in latest.iterrows():
                symbol = str(row.get("instrument") or row.get("ts_code") or "").strip().upper()
                observed_date = pd.Timestamp(row.get("datetime") or row.get("trade_date")).date()
                close = None if pd.isna(row.get("close")) else float(row.get("close"))
                if symbol not in missing_symbols or observed_date >= trade_date or close is None:
                    continue
                rows[symbol] = {
                    "symbol": symbol,
                    "trade_date": observed_date.isoformat(),
                    "close": close,
                    "price_basis": "raw_cny",
                    "reference_state": "LAST_EXECUTABLE_CLOSE",
                    "feature_available_at": datetime.combine(
                        observed_date, time(15, 0), tzinfo=CHINA_TZ
                    ).isoformat(),
                }
    payload = {
        "source": "backend.data_service.timescaledb_adapter.fetch_history_window_ts",
        "table_contract": "DAILY_RAW_TABLE",
        "trade_date": trade_date.isoformat(),
        "symbol_set": validated,
        "rows": rows,
    }
    identity = {k: v for k, v in payload.items() if k != "rows"}
    identity["rows_sha256"] = canonical_sha256(rows)
    identity["identity_sha256"] = canonical_sha256(identity)
    return {**payload, "identity": identity}


def _default_supporting_facts_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    from backend.services.simulation_data.daily_context_provider import DailyTradingContextProvider

    payload = DailyTradingContextProvider().load_supporting_facts(symbols=symbols, trade_date=trade_date)
    session_known_at = datetime.combine(trade_date, time(9, 15), tzinfo=CHINA_TZ).isoformat()
    stock_st_facts = {
        symbol: {**dict(fact), "feature_available_at": session_known_at}
        for symbol, fact in dict(payload.get("stock_st_facts") or {}).items()
    }
    suspend_facts = {
        symbol: {**dict(fact), "feature_available_at": session_known_at}
        for symbol, fact in dict(payload.get("suspend_facts") or {}).items()
    }
    identity = {
        "schema_version": payload.get("schema_version"),
        "trade_date": payload.get("trade_date"),
        "symbol_set": payload.get("symbol_set"),
        "stock_st": payload.get("stock_st"),
        "suspend_d": payload.get("suspend_d"),
        "feature_availability_basis": "EFFECTIVE_FOR_TRADE_DATE_AT_SESSION_START",
        "feature_available_at": session_known_at,
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return {
        **payload,
        "stock_st_facts": stock_st_facts,
        "suspend_facts": suspend_facts,
        "identity": identity,
    }


def _default_delist_snapshot_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    """Read only confirmed, timestamp-causal terminal listing facts.

    The event-signal lifecycle overlay remains research-only, so it is not a
    runtime authority here.  A verified canonical terminal event can make the
    flag effective before the actual delist date; ``stock_basic`` is used only
    after its terminal date is no later than the decision date.
    """

    import psycopg2.extras as pgx

    from backend.db.pg_pool import get_conn
    from backend.services.canonical_equity_pit import CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT

    validated = normalize_and_validate_ts_codes(
        symbols,
        source="position_timing.delist_snapshot",
        start_date=trade_date,
        end_date=trade_date,
        allow_empty=False,
    )
    decision_as_of = datetime.combine(trade_date, time(15, 0), tzinfo=CHINA_TZ)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                WITH requested AS (
                    SELECT unnest(%s::text[]) AS ts_code
                ), exact_terminal AS (
                    SELECT DISTINCT ON (signal.ts_code)
                           signal.ts_code, signal.signal_id, signal.available_at,
                           signal.effective_trade_date, signal.source_pk, signal.rule_version
                    FROM market.event_signal AS signal
                    WHERE signal.ts_code = ANY(%s)
                      AND signal.event_type = 'stock_delisting_confirmed'
                      AND signal.time_mode = 'backtest'
                      AND signal.signal_status IN ('ACTIVE', 'RESOLVED', 'EXPIRED')
                      AND signal.available_at IS NOT NULL
                      AND signal.available_at <= %s
                      AND signal.effective_trade_date <= %s
                      AND signal.evidence->>'terminal_evidence_contract' = %s
                      AND signal.evidence#>>'{issuer_binding,schema_version}' = 'announcement_issuer_binding_v1'
                      AND signal.evidence#>>'{issuer_binding,status}' = 'EXACT'
                      AND signal.evidence#>>'{issuer_binding,actionable}' = 'true'
                      AND signal.evidence#>>'{issuer_binding,resolved_ts_code}' = signal.ts_code
                      AND COALESCE(
                            signal.evidence#>>'{terminal_cross_check,matched}',
                            signal.evidence#>>'{st_cross_check,matched}'
                          ) = 'true'
                      AND COALESCE(
                            signal.evidence#>>'{terminal_cross_check,terminal}',
                            signal.evidence#>>'{st_cross_check,terminal}'
                          ) = 'true'
                    ORDER BY signal.ts_code, signal.effective_trade_date DESC,
                             signal.available_at DESC, signal.signal_id DESC
                )
                SELECT requested.ts_code, basic.list_status, basic.delist_date,
                       terminal.signal_id, terminal.available_at,
                       terminal.effective_trade_date, terminal.source_pk,
                       terminal.rule_version
                FROM requested
                LEFT JOIN market.stock_basic AS basic ON basic.ts_code = requested.ts_code
                LEFT JOIN exact_terminal AS terminal ON terminal.ts_code = requested.ts_code
                ORDER BY requested.ts_code
                """,
                (
                    validated,
                    validated,
                    decision_as_of,
                    trade_date,
                    CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT,
                ),
            )
            source_rows = [dict(row) for row in cur.fetchall()]

    rows: dict[str, dict[str, Any]] = {}
    for source_row in source_rows:
        symbol = str(source_row.get("ts_code") or "").strip().upper()
        if symbol not in validated or symbol in rows:
            raise ValueError(f"DELIST_IDENTITY_CONFLICT:{symbol}:{trade_date.isoformat()}")
        delist_date = source_row.get("delist_date")
        stock_basic_terminal = (
            str(source_row.get("list_status") or "").strip().upper() == "D"
            and isinstance(delist_date, date)
            and delist_date <= trade_date
        )
        exact_event_terminal = source_row.get("signal_id") is not None
        if source_row.get("list_status") is None and not exact_event_terminal:
            continue
        terminal_available_at = source_row.get("available_at")
        feature_available_at = (
            terminal_available_at.astimezone(CHINA_TZ).isoformat()
            if isinstance(terminal_available_at, datetime)
            else decision_as_of.isoformat()
        )
        evidence = {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "delist_flag": exact_event_terminal or stock_basic_terminal,
            "terminal_basis": (
                "CANONICAL_CONFIRMED_EVENT"
                if exact_event_terminal
                else "STOCK_BASIC_EFFECTIVE_DELIST_DATE"
                if stock_basic_terminal
                else "NO_CONFIRMED_TERMINAL_FACT_AS_OF_CUTOFF"
            ),
            "list_status": source_row.get("list_status"),
            "delist_date": delist_date.isoformat() if isinstance(delist_date, date) else None,
            "signal_id": source_row.get("signal_id"),
            "signal_source_pk": source_row.get("source_pk"),
            "signal_rule_version": source_row.get("rule_version"),
            "signal_effective_trade_date": (
                source_row["effective_trade_date"].isoformat()
                if isinstance(source_row.get("effective_trade_date"), date)
                else None
            ),
            "terminal_evidence_contract": CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT,
            "feature_available_at": feature_available_at,
        }
        rows[symbol] = {**evidence, "evidence_hash": canonical_sha256(evidence)}
    identity = {
        "source": "market.event_signal+market.stock_basic",
        "terminal_event_policy": CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT,
        "negative_fact_scope": "NO_CONFIRMED_TERMINAL_FACT_AS_OF_CUTOFF",
        "trade_date": trade_date.isoformat(),
        "decision_as_of": decision_as_of.isoformat(),
        "symbol_set": validated,
        "rows_sha256": canonical_sha256(rows),
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return {"rows": rows, "identity": identity}


def _default_outcome_snapshot_loader(
    symbols: list[str], start_date: date, end_date: date
) -> dict[str, Any]:
    """Read mature raw OHLC, suspension, and adjustment rows without fallback."""

    from datetime import datetime as dt

    import pandas as pd
    import psycopg2.extras as pgx

    from backend.data_service.timescaledb_adapter import fetch_history_window_ts
    from backend.db.pg_pool import get_conn

    validated = normalize_and_validate_ts_codes(
        symbols,
        source="position_timing.outcome_snapshot",
        start_date=start_date,
        end_date=end_date,
        allow_empty=False,
    )
    frame = fetch_history_window_ts(
        validated,
        start=dt.combine(start_date, time.min),
        end=dt.combine(end_date, time.max),
        fields=["open", "high", "low", "close", "volume", "amount"],
        freq="1d",
        adj="none",
    )
    rows: dict[str, dict[str, dict[str, Any]]] = {symbol: {} for symbol in validated}
    if not frame.empty:
        for _, source_row in frame.reset_index().iterrows():
            symbol = str(source_row.get("instrument") or source_row.get("ts_code") or "").strip().upper()
            trade_date = pd.Timestamp(source_row.get("datetime") or source_row.get("trade_date")).date()
            if symbol not in rows or not (start_date <= trade_date <= end_date):
                continue
            key = trade_date.isoformat()
            if key in rows[symbol]:
                raise ValueError(f"OUTCOME_DAILY_IDENTITY_CONFLICT:{symbol}:{key}")
            rows[symbol][key] = {
                "symbol": symbol,
                "trade_date": key,
                **{
                    field: None if pd.isna(source_row.get(field)) else float(source_row.get(field))
                    for field in ("open", "high", "low", "close", "volume", "amount")
                },
                "price_basis": "raw_cny",
                "is_suspended": False,
            }

    with get_conn() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts_code, trade_date, adj_factor
                FROM market.adj_factor
                WHERE ts_code = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                ORDER BY ts_code, trade_date
                """,
                (validated, start_date, end_date),
            )
            adjustment_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT ts_code, trade_date, up_limit, down_limit
                FROM market.stk_limit
                WHERE ts_code = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                ORDER BY ts_code, trade_date
                """,
                (validated, start_date, end_date),
            )
            limit_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT ts_code, trade_date
                FROM market.suspend_d
                WHERE ts_code = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                  AND suspend_type = 'S'
                GROUP BY ts_code, trade_date
                ORDER BY ts_code, trade_date
                """,
                (validated, start_date, end_date),
            )
            suspension_rows = [dict(row) for row in cur.fetchall()]

    adjustment_map: dict[str, dict[str, str]] = {symbol: {} for symbol in validated}
    for source_row in adjustment_rows:
        symbol = str(source_row.get("ts_code") or "").strip().upper()
        observed_date = source_row.get("trade_date")
        factor = _positive_decimal(source_row.get("adj_factor"))
        if symbol not in adjustment_map or not isinstance(observed_date, date) or factor is None:
            continue
        key = observed_date.isoformat()
        if key in adjustment_map[symbol]:
            raise ValueError(f"OUTCOME_ADJUSTMENT_IDENTITY_CONFLICT:{symbol}:{key}")
        adjustment_map[symbol][key] = format(factor, "f")
        rows[symbol].setdefault(
            key,
            {
                "symbol": symbol,
                "trade_date": key,
                "price_basis": "raw_cny",
                "is_suspended": False,
            },
        )["adj_factor"] = factor
    limit_map: dict[str, dict[str, dict[str, str]]] = {symbol: {} for symbol in validated}
    for source_row in limit_rows:
        symbol = str(source_row.get("ts_code") or "").strip().upper()
        observed_date = source_row.get("trade_date")
        up_limit = _positive_decimal(source_row.get("up_limit"))
        down_limit = _positive_decimal(source_row.get("down_limit"))
        if (
            symbol not in limit_map
            or not isinstance(observed_date, date)
            or up_limit is None
            or down_limit is None
            or down_limit >= up_limit
        ):
            continue
        key = observed_date.isoformat()
        if key in limit_map[symbol]:
            raise ValueError(f"OUTCOME_LIMIT_IDENTITY_CONFLICT:{symbol}:{key}")
        limit_map[symbol][key] = {
            "up_limit": format(up_limit, "f"),
            "down_limit": format(down_limit, "f"),
        }
        rows[symbol].setdefault(
            key,
            {
                "symbol": symbol,
                "trade_date": key,
                "price_basis": "raw_cny",
                "is_suspended": False,
            },
        ).update({"up_limit": up_limit, "down_limit": down_limit})
    for source_row in suspension_rows:
        symbol = str(source_row.get("ts_code") or "").strip().upper()
        observed_date = source_row.get("trade_date")
        if symbol not in rows or not isinstance(observed_date, date):
            continue
        key = observed_date.isoformat()
        rows[symbol].setdefault(
            key,
            {
                "symbol": symbol,
                "trade_date": key,
                "price_basis": "raw_cny",
            },
        )["is_suspended"] = True

    adjustment_identity = {
        "source": "market.adj_factor",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "symbol_set": validated,
        "rows_sha256": canonical_sha256(adjustment_map),
        "corporate_action_valuation": "RAW_CLOSE_TIMES_ADJ_FACTOR_RATIO_V1",
    }
    adjustment_identity["identity_sha256"] = canonical_sha256(adjustment_identity)
    limit_identity = {
        "source": "market.stk_limit",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "symbol_set": validated,
        "rows_sha256": canonical_sha256(limit_map),
        "terminal_sellability_policy": "ONE_WORD_LIMIT_DOWN_DEFERS_TERMINAL_V1",
    }
    limit_identity["identity_sha256"] = canonical_sha256(limit_identity)
    identity = {
        "source": "market.kline_daily_raw+market.suspend_d+market.stk_limit+market.adj_factor",
        "table_contract": "POSITION_TIMING_OUTCOME_RAW_V1",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "symbol_set": validated,
        "rows_sha256": canonical_sha256(rows),
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return {
        "rows": rows,
        "identity": identity,
        "adjustment_identity": adjustment_identity,
        "limit_identity": limit_identity,
    }


def _resolve_source_commit() -> str:
    """Resolve the code identity once while this module is being imported."""

    from_env = str(os.getenv("AISTOCK_GIT_COMMIT") or "").strip().lower()
    if len(from_env) == 40 and all(char in "0123456789abcdef" for char in from_env):
        return from_env
    root = Path(__file__).resolve().parents[3]
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    resolved = completed.stdout.strip().lower()
    if len(resolved) != 40 or any(char not in "0123456789abcdef" for char in resolved):
        raise RuntimeError("resolved source commit is not a 40-character lowercase git sha")
    return resolved


try:
    _PROCESS_SOURCE_REPOSITORY_COMMIT = _resolve_source_commit()
    _PROCESS_SOURCE_REPOSITORY_COMMIT_ERROR: str | None = None
except Exception as exc:  # Keep unrelated backend routes importable; materialization fails typed.
    _PROCESS_SOURCE_REPOSITORY_COMMIT = None
    _PROCESS_SOURCE_REPOSITORY_COMMIT_ERROR = type(exc).__name__


def _source_commit() -> str:
    """Return the immutable code identity captured for this running process."""

    if _PROCESS_SOURCE_REPOSITORY_COMMIT is None:
        raise RuntimeError(
            "position timing source commit was unavailable when process code loaded: "
            f"{_PROCESS_SOURCE_REPOSITORY_COMMIT_ERROR or 'UNKNOWN'}"
        )
    return _PROCESS_SOURCE_REPOSITORY_COMMIT


def build_position_timing_service(*, artifact_root: str | Path | None = None) -> PositionTimingService:
    from portfolio_manager import portfolio_manager

    from backend.repositories.watchlist_repo_impl import WatchlistRepoPG
    from backend.services.simulation_data.tdx_causal_minute import fetch_tdx_realtime_quotes
    from backend.services.trading_calendar_status import TradingCalendarStatusService

    watchlist_repo = WatchlistRepoPG()
    dependencies = PositionTimingDependencies(
        holdings_loader=lambda: portfolio_manager.get_all_stocks(auto_monitor_only=False),
        watchlist_page_loader=lambda page, page_size: watchlist_repo.list_items(
            page=page, page_size=page_size, sort_by="updated_at", sort_dir="desc"
        ),
        calendar_service=TradingCalendarStatusService(),
        daily_snapshot_loader=_default_daily_snapshot_loader,
        supporting_facts_loader=_default_supporting_facts_loader,
        delist_snapshot_loader=_default_delist_snapshot_loader,
        now_provider=lambda: datetime.now(CHINA_TZ),
        source_commit_provider=_source_commit,
        realtime_quote_loader=fetch_tdx_realtime_quotes,
        outcome_snapshot_loader=_default_outcome_snapshot_loader,
    )
    return PositionTimingService(store=PositionTimingArtifactStore(artifact_root), dependencies=dependencies)


__all__ = [
    "PositionTimingDependencies",
    "PositionTimingService",
    "PositionTimingServiceError",
    "build_position_timing_service",
    "normalize_position_symbol",
]
