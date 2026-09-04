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
from .contracts import (
    CHINA_TIMEZONE,
    POSITION_SOURCE,
    POSITION_TIMING_L2_RESEARCH_CONTRACT_V1,
    ExecutionWindow,
    HoldingAgeBucket,
    LegCostEstimateV1,
    MarketRegime,
    PositionTimingCardSetV1,
    PositionTimingCardV1,
    PositionTimingIntentV1,
    SourceRole,
    TimingAction,
    TradabilityStatus,
    TriggerOperator,
    TriggerSide,
    TriggerV1,
    TypedStatus,
    canonical_sha256,
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
class PositionTimingDependencies:
    holdings_loader: Callable[[], list[dict[str, Any]]]
    watchlist_page_loader: Callable[[int, int], dict[str, Any]]
    calendar_service: Any
    daily_snapshot_loader: Callable[[list[str], date], dict[str, Any]]
    supporting_facts_loader: Callable[[list[str], date], dict[str, Any]]
    delist_snapshot_loader: Callable[[list[str], date], dict[str, Any]]
    now_provider: Callable[[], datetime]
    source_commit_provider: Callable[[], str]


class PositionTimingService:
    def __init__(self, *, store: PositionTimingArtifactStore, dependencies: PositionTimingDependencies) -> None:
        self.store = store
        self.dependencies = dependencies

    def list_intents(self) -> dict[str, Any]:
        members, universe_identity = self._load_universe()
        intent_by_symbol = {item.canonical_symbol: item for item in self.store.list_intents()}
        return {
            "schema_version": "position_timing_intent_list_v1",
            "position_source": POSITION_SOURCE,
            "universe_identity_sha256": universe_identity,
            "items": [
                {
                    "canonical_symbol": member.canonical_symbol,
                    "display_name": member.display_name,
                    "primary_source_role": member.primary_source_role,
                    "source_roles": member.source_roles,
                    "pre_action_qty": _non_negative_int((member.holding or {}).get("quantity")),
                    "intent": intent_by_symbol.get(member.canonical_symbol),
                    "normalization_reason": member.normalization_reason,
                }
                for member in members
            ],
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

        members, universe_identity = self._load_universe()
        if not members:
            return {
                "schema_version": "position_timing_materialize_result_v1",
                "status": "UNIVERSE_EMPTY_NO_NEW_CARD",
                "created": False,
                "card_set": None,
                "decision_trade_date": decision_date,
                "target_trade_date": target_date,
                "reason_codes": ["TIMING_UNIVERSE_EMPTY"],
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
            "universe_identity_sha256": universe_identity,
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

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": "position_timing_evidence_v1",
            "product_evidence_tier": "RULE_BASED_RISK_MANAGEMENT",
            "event_counts": self.store.event_counts(),
            "l2_research_contract_sha256": canonical_sha256(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1),
            "l2_runtime_status": "PIPELINE_DEFERRED_BY_APPROVED_SCOPE",
            "hmm_runtime_role": "CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE",
            "selection_runtime_role": "CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE",
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


def _source_commit() -> str:
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
    return completed.stdout.strip().lower()


def build_position_timing_service(*, artifact_root: str | Path | None = None) -> PositionTimingService:
    from portfolio_manager import portfolio_manager

    from backend.repositories.watchlist_repo_impl import WatchlistRepoPG
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
    )
    return PositionTimingService(store=PositionTimingArtifactStore(artifact_root), dependencies=dependencies)


__all__ = [
    "PositionTimingDependencies",
    "PositionTimingService",
    "PositionTimingServiceError",
    "build_position_timing_service",
    "normalize_position_symbol",
]
