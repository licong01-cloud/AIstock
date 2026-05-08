"""Policy lifecycle helpers for event-signal research overlays.

This module stays inside the event_signal package.  It seeds policy metadata
and expands ST-related event_signal rows into state-span drafts for offline
validation, without wiring those decisions into QE, Selection Center, Paper v2,
or live trading consumers.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Iterable, Optional

from dotenv import load_dotenv

from backend.db.init_unified_event_signal_schema import init_unified_event_signal_schema
from backend.db.pg_pool import get_conn


DEFAULT_ST_POLICY_PROFILE_ID = "event_signal_policy_st_force_exit_v1_20260507"
DEFAULT_ST_POLICY_VERSION = "st_force_exit_v1_20260507"
POLICY_ENGINE_VERSION = "event_signal_policy_lifecycle_v1_20260507"

ST_HARD_RISK_EVENT_TYPES: tuple[str, ...] = (
    "stock_st_imposed",
    "stock_st_added_or_continued",
    "stock_delisting_risk_warning",
    "stock_delisting_confirmed",
)
ST_REMOVAL_APPLIED_EVENT_TYPE = "stock_st_removal_applied"
ST_REMOVED_CONFIRMED_EVENT_TYPE = "stock_st_removed_confirmed"
ST_FIRST_SIGNAL_RULE_VERSION = "unified_event_signal_rules_st_first_v1_20260506"


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _date_value(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _optional_date_value(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    return _date_value(value)


def _optional_datetime_value(value: Any) -> Optional[dt.datetime]:
    if value is None or isinstance(value, dt.datetime):
        return value
    return dt.datetime.fromisoformat(str(value))


def _numeric(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _previous_trading_day(trading_days: list[dt.date], trade_date: dt.date) -> Optional[dt.date]:
    previous = [day for day in trading_days if day < trade_date]
    return previous[-1] if previous else None


def _trading_day_offset(trading_days: list[dt.date], start: dt.date, offset: int) -> dt.date:
    if offset < 0:
        raise ValueError("offset must be non-negative")
    eligible = [day for day in trading_days if day >= start]
    if not eligible:
        raise ValueError(f"trading calendar does not cover start date {start}")
    index = min(offset, len(eligible) - 1)
    return eligible[index]


@dataclass(frozen=True)
class PolicyProfileDraft:
    profile_id: str
    profile_name: str
    profile_version: str
    profile_status: str
    policy_scope: str
    time_mode: str
    base_rule_versions: dict[str, Any]
    default_action_mode: str
    positive_overlay_enabled: bool
    formal_st_removal_required: bool
    st_removal_cooldown_trading_days: int
    allow_buy_on_st_removal_expectation: bool
    max_positive_score_delta: float
    max_negative_score_delta: float
    config_hash: str
    config: dict[str, Any]
    created_by: str = "codex"


@dataclass(frozen=True)
class EffectRuleDraft:
    rule_key: str
    rule_status: str
    event_family: str
    event_type: str
    source_type: Optional[str]
    source_rule_version: Optional[str]
    match_expression: dict[str, Any]
    lifecycle_kind: str
    state_family: Optional[str]
    state_type: Optional[str]
    opens_state: bool
    closes_state: bool
    requires_formal_resolution: bool
    resolution_event_types: tuple[str, ...]
    policy_risk_level: str
    primary_action: str
    block_buy: bool
    block_add: bool
    force_exit: bool
    sell_only: bool
    validity_trading_days: Optional[int]
    decay_start_trading_days: Optional[int]
    decay_half_life_trading_days: Optional[int]
    cooldown_trading_days: int
    severity_weight: float
    confidence_floor: float
    score_delta: float
    score_multiplier: float
    score_overlay_enabled: bool
    priority: int
    is_enabled: bool
    effective_from: Optional[dt.date]
    effective_to: Optional[dt.date]
    rule_params: dict[str, Any]


@dataclass(frozen=True)
class StateSpanDraft:
    state_key: str
    profile_id: str
    ts_code: str
    time_mode: str
    state_family: str
    state_type: str
    state_status: str
    opened_by_signal_id: Optional[int]
    closed_by_signal_id: Optional[int]
    open_event_type: str
    close_event_type: Optional[str]
    start_trade_date: dt.date
    end_trade_date: Optional[dt.date]
    expiry_trade_date: Optional[dt.date]
    cooldown_until_trade_date: Optional[dt.date]
    available_at_start: Optional[dt.datetime]
    available_at_end: Optional[dt.datetime]
    source_time_quality: str
    policy_risk_level: str
    primary_action: str
    severity_score: float
    confidence: float
    score_delta: float
    score_multiplier: float
    effect_rule_key: Optional[str]
    policy_snapshot_hash: str
    evidence: dict[str, Any]
    state_span_id: Optional[int] = None


def default_st_policy_config(
    *,
    base_rule_versions: Optional[dict[str, Any]] = None,
    cooldown_trading_days: int = 5,
) -> dict[str, Any]:
    """Return the deterministic first-stage ST policy config."""

    if cooldown_trading_days < 0:
        raise ValueError("cooldown_trading_days must be non-negative")
    return {
        "engine_version": POLICY_ENGINE_VERSION,
        "profile_id": DEFAULT_ST_POLICY_PROFILE_ID,
        "policy_version": DEFAULT_ST_POLICY_VERSION,
        "default_action_mode": "risk_first",
        "positive_overlay_enabled": False,
        "formal_st_removal_required": True,
        "st_removal_cooldown_trading_days": cooldown_trading_days,
        "allow_buy_on_st_removal_expectation": False,
        "base_rule_versions": base_rule_versions or {},
        "signal_onboarding": {
            "requires_qe_loop1_validation": True,
            "baseline_experiment_id": "qe_20260507_132049_d4e7",
            "baseline_loop_id": "Loop1",
            "stack_only_after_single_signal_validation": True,
        },
        "hard_risk_event_types": list(ST_HARD_RISK_EVENT_TYPES),
        "formal_resolution_event_types": [ST_REMOVED_CONFIRMED_EVENT_TYPE],
        "expectation_event_types": [ST_REMOVAL_APPLIED_EVENT_TYPE],
    }


def default_st_policy_profile(
    *,
    base_rule_versions: Optional[dict[str, Any]] = None,
    cooldown_trading_days: int = 5,
    time_mode: str = "backtest",
) -> PolicyProfileDraft:
    config = default_st_policy_config(
        base_rule_versions=base_rule_versions,
        cooldown_trading_days=cooldown_trading_days,
    )
    return PolicyProfileDraft(
        profile_id=DEFAULT_ST_POLICY_PROFILE_ID,
        profile_name="ST hard-risk force-exit research policy v1",
        profile_version=DEFAULT_ST_POLICY_VERSION,
        profile_status="DRAFT",
        policy_scope="research_overlay",
        time_mode=time_mode,
        base_rule_versions=base_rule_versions or {},
        default_action_mode="risk_first",
        positive_overlay_enabled=False,
        formal_st_removal_required=True,
        st_removal_cooldown_trading_days=cooldown_trading_days,
        allow_buy_on_st_removal_expectation=False,
        max_positive_score_delta=0.0,
        max_negative_score_delta=0.0,
        config_hash=_stable_hash(config),
        config=config,
    )


def default_st_effect_rules(
    *,
    cooldown_trading_days: int = 5,
    source_rule_version: Optional[str] = None,
) -> list[EffectRuleDraft]:
    """Return first-stage ST effect rules without mutating raw event_signal rows."""

    hard_rules = [
        EffectRuleDraft(
            rule_key=f"{event_type}_force_exit_v1",
            rule_status="ENABLED",
            event_family="announcement_risk",
            event_type=event_type,
            source_type="announcement",
            source_rule_version=source_rule_version,
            match_expression={"risk_level": "P0_BLOCK"},
            lifecycle_kind="state",
            state_family="st_hard_risk",
            state_type="st_active_block",
            opens_state=True,
            closes_state=False,
            requires_formal_resolution=True,
            resolution_event_types=(ST_REMOVED_CONFIRMED_EVENT_TYPE,),
            policy_risk_level="P0_FORCE_EXIT",
            primary_action="force_exit",
            block_buy=True,
            block_add=True,
            force_exit=True,
            sell_only=True,
            validity_trading_days=None,
            decay_start_trading_days=None,
            decay_half_life_trading_days=None,
            cooldown_trading_days=cooldown_trading_days,
            severity_weight=1.0,
            confidence_floor=0.0,
            score_delta=0.0,
            score_multiplier=1.0,
            score_overlay_enabled=False,
            priority=0,
            is_enabled=True,
            effective_from=None,
            effective_to=None,
            rule_params={"formal_resolution_required": True},
        )
        for event_type in ST_HARD_RISK_EVENT_TYPES
    ]
    return [
        *hard_rules,
        EffectRuleDraft(
            rule_key="stock_st_removal_applied_record_only_v1",
            rule_status="ENABLED",
            event_family="announcement_risk",
            event_type=ST_REMOVAL_APPLIED_EVENT_TYPE,
            source_type="announcement",
            source_rule_version=source_rule_version,
            match_expression={},
            lifecycle_kind="record_only",
            state_family=None,
            state_type=None,
            opens_state=False,
            closes_state=False,
            requires_formal_resolution=False,
            resolution_event_types=(),
            policy_risk_level="P2_REVIEW",
            primary_action="record_only",
            block_buy=False,
            block_add=False,
            force_exit=False,
            sell_only=False,
            validity_trading_days=20,
            decay_start_trading_days=None,
            decay_half_life_trading_days=None,
            cooldown_trading_days=0,
            severity_weight=0.0,
            confidence_floor=0.0,
            score_delta=0.0,
            score_multiplier=1.0,
            score_overlay_enabled=False,
            priority=80,
            is_enabled=True,
            effective_from=None,
            effective_to=None,
            rule_params={"does_not_close_st_hard_risk": True},
        ),
        EffectRuleDraft(
            rule_key="stock_st_removed_confirmed_close_with_cooldown_v1",
            rule_status="ENABLED",
            event_family="announcement_risk",
            event_type=ST_REMOVED_CONFIRMED_EVENT_TYPE,
            source_type="announcement",
            source_rule_version=source_rule_version,
            match_expression={},
            lifecycle_kind="close_state",
            state_family="st_hard_risk",
            state_type="st_removal_cooldown",
            opens_state=True,
            closes_state=True,
            requires_formal_resolution=True,
            resolution_event_types=(),
            policy_risk_level="P0_BLOCK",
            primary_action="block_buy",
            block_buy=True,
            block_add=False,
            force_exit=False,
            sell_only=False,
            validity_trading_days=cooldown_trading_days,
            decay_start_trading_days=None,
            decay_half_life_trading_days=None,
            cooldown_trading_days=cooldown_trading_days,
            severity_weight=0.5,
            confidence_floor=0.0,
            score_delta=0.0,
            score_multiplier=1.0,
            score_overlay_enabled=False,
            priority=10,
            is_enabled=True,
            effective_from=None,
            effective_to=None,
            rule_params={"cooldown_after_formal_removal": True},
        ),
    ]


def seed_default_st_policy(
    conn: Any,
    *,
    profile: Optional[PolicyProfileDraft] = None,
    effect_rules: Optional[list[EffectRuleDraft]] = None,
) -> None:
    """Upsert the default ST policy profile and effect rules."""

    profile = profile or default_st_policy_profile()
    effect_rules = effect_rules or default_st_effect_rules()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.event_signal_policy_profile
                (
                    profile_id, profile_name, profile_version, profile_status,
                    policy_scope, time_mode, base_rule_versions, default_action_mode,
                    positive_overlay_enabled, formal_st_removal_required,
                    st_removal_cooldown_trading_days, allow_buy_on_st_removal_expectation,
                    max_positive_score_delta, max_negative_score_delta, config_hash,
                    config, created_by, updated_at
                )
            VALUES
                (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
            ON CONFLICT (profile_id) DO UPDATE SET
                profile_name = EXCLUDED.profile_name,
                profile_version = EXCLUDED.profile_version,
                profile_status = EXCLUDED.profile_status,
                policy_scope = EXCLUDED.policy_scope,
                time_mode = EXCLUDED.time_mode,
                base_rule_versions = EXCLUDED.base_rule_versions,
                default_action_mode = EXCLUDED.default_action_mode,
                positive_overlay_enabled = EXCLUDED.positive_overlay_enabled,
                formal_st_removal_required = EXCLUDED.formal_st_removal_required,
                st_removal_cooldown_trading_days = EXCLUDED.st_removal_cooldown_trading_days,
                allow_buy_on_st_removal_expectation = EXCLUDED.allow_buy_on_st_removal_expectation,
                max_positive_score_delta = EXCLUDED.max_positive_score_delta,
                max_negative_score_delta = EXCLUDED.max_negative_score_delta,
                config_hash = EXCLUDED.config_hash,
                config = EXCLUDED.config,
                created_by = EXCLUDED.created_by,
                updated_at = NOW()
            """,
            (
                profile.profile_id,
                profile.profile_name,
                profile.profile_version,
                profile.profile_status,
                profile.policy_scope,
                profile.time_mode,
                _json_dumps(profile.base_rule_versions),
                profile.default_action_mode,
                profile.positive_overlay_enabled,
                profile.formal_st_removal_required,
                profile.st_removal_cooldown_trading_days,
                profile.allow_buy_on_st_removal_expectation,
                profile.max_positive_score_delta,
                profile.max_negative_score_delta,
                profile.config_hash,
                _json_dumps(profile.config),
                profile.created_by,
            ),
        )
        for rule in effect_rules:
            cur.execute(
                """
                INSERT INTO market.event_signal_effect_rule
                    (
                        profile_id, rule_key, rule_status, event_family, event_type,
                        source_type, source_rule_version, match_expression, lifecycle_kind,
                        state_family, state_type, opens_state, closes_state,
                        requires_formal_resolution, resolution_event_types, policy_risk_level,
                        primary_action, block_buy, block_add, force_exit, sell_only,
                        validity_trading_days, decay_start_trading_days,
                        decay_half_life_trading_days, cooldown_trading_days,
                        severity_weight, confidence_floor, score_delta, score_multiplier,
                        score_overlay_enabled, priority, is_enabled, effective_from,
                        effective_to, rule_params, updated_at
                    )
                VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s,
                        %s, %s::text[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
                    )
                ON CONFLICT (profile_id, rule_key) DO UPDATE SET
                    rule_status = EXCLUDED.rule_status,
                    event_family = EXCLUDED.event_family,
                    event_type = EXCLUDED.event_type,
                    source_type = EXCLUDED.source_type,
                    source_rule_version = EXCLUDED.source_rule_version,
                    match_expression = EXCLUDED.match_expression,
                    lifecycle_kind = EXCLUDED.lifecycle_kind,
                    state_family = EXCLUDED.state_family,
                    state_type = EXCLUDED.state_type,
                    opens_state = EXCLUDED.opens_state,
                    closes_state = EXCLUDED.closes_state,
                    requires_formal_resolution = EXCLUDED.requires_formal_resolution,
                    resolution_event_types = EXCLUDED.resolution_event_types,
                    policy_risk_level = EXCLUDED.policy_risk_level,
                    primary_action = EXCLUDED.primary_action,
                    block_buy = EXCLUDED.block_buy,
                    block_add = EXCLUDED.block_add,
                    force_exit = EXCLUDED.force_exit,
                    sell_only = EXCLUDED.sell_only,
                    validity_trading_days = EXCLUDED.validity_trading_days,
                    decay_start_trading_days = EXCLUDED.decay_start_trading_days,
                    decay_half_life_trading_days = EXCLUDED.decay_half_life_trading_days,
                    cooldown_trading_days = EXCLUDED.cooldown_trading_days,
                    severity_weight = EXCLUDED.severity_weight,
                    confidence_floor = EXCLUDED.confidence_floor,
                    score_delta = EXCLUDED.score_delta,
                    score_multiplier = EXCLUDED.score_multiplier,
                    score_overlay_enabled = EXCLUDED.score_overlay_enabled,
                    priority = EXCLUDED.priority,
                    is_enabled = EXCLUDED.is_enabled,
                    effective_from = EXCLUDED.effective_from,
                    effective_to = EXCLUDED.effective_to,
                    rule_params = EXCLUDED.rule_params,
                    updated_at = NOW()
                """,
                (
                    profile.profile_id,
                    rule.rule_key,
                    rule.rule_status,
                    rule.event_family,
                    rule.event_type,
                    rule.source_type,
                    rule.source_rule_version,
                    _json_dumps(rule.match_expression),
                    rule.lifecycle_kind,
                    rule.state_family,
                    rule.state_type,
                    rule.opens_state,
                    rule.closes_state,
                    rule.requires_formal_resolution,
                    list(rule.resolution_event_types),
                    rule.policy_risk_level,
                    rule.primary_action,
                    rule.block_buy,
                    rule.block_add,
                    rule.force_exit,
                    rule.sell_only,
                    rule.validity_trading_days,
                    rule.decay_start_trading_days,
                    rule.decay_half_life_trading_days,
                    rule.cooldown_trading_days,
                    rule.severity_weight,
                    rule.confidence_floor,
                    rule.score_delta,
                    rule.score_multiplier,
                    rule.score_overlay_enabled,
                    rule.priority,
                    rule.is_enabled,
                    rule.effective_from,
                    rule.effective_to,
                    _json_dumps(rule.rule_params),
                ),
            )


def generate_st_state_spans(
    signals: Iterable[dict[str, Any]],
    *,
    trading_days: Iterable[dt.date],
    profile: Optional[PolicyProfileDraft] = None,
    effect_rules: Optional[list[EffectRuleDraft]] = None,
) -> list[StateSpanDraft]:
    """Generate ST hard-risk and cooldown state spans from signal-like rows."""

    profile = profile or default_st_policy_profile()
    rules = {rule.event_type: rule for rule in (effect_rules or default_st_effect_rules())}
    removal_rule = rules[ST_REMOVED_CONFIRMED_EVENT_TYPE]
    calendar = sorted({_date_value(day) for day in trading_days})
    rows = sorted(
        signals,
        key=lambda row: (
            str(row.get("ts_code") or ""),
            _date_value(row.get("effective_trade_date")),
            int(row.get("signal_id") or 0),
            str(row.get("event_type") or ""),
        ),
    )
    spans: list[StateSpanDraft] = []
    active_by_symbol: dict[tuple[str, str], StateSpanDraft] = {}

    for row in rows:
        event_type = str(row.get("event_type") or "")
        if event_type not in rules:
            continue
        ts_code = str(row.get("ts_code") or "").strip().upper()
        time_mode = str(row.get("time_mode") or profile.time_mode)
        if not ts_code:
            continue
        trade_date = _date_value(row.get("effective_trade_date"))
        signal_id = int(row["signal_id"]) if row.get("signal_id") is not None else None
        symbol_key = (ts_code, time_mode)

        if event_type in ST_HARD_RISK_EVENT_TYPES:
            if symbol_key in active_by_symbol:
                continue
            span = _open_state_span(row, profile=profile, rule=rules[event_type], signal_id=signal_id)
            active_by_symbol[symbol_key] = span
            spans.append(span)
            continue

        if event_type == ST_REMOVAL_APPLIED_EVENT_TYPE:
            continue

        if event_type == ST_REMOVED_CONFIRMED_EVENT_TYPE:
            active = active_by_symbol.pop(symbol_key, None)
            if active is not None:
                previous_day = _previous_trading_day(calendar, trade_date)
                closed = StateSpanDraft(
                    **{
                        **active.__dict__,
                        "state_status": "CLOSED",
                        "closed_by_signal_id": signal_id,
                        "close_event_type": event_type,
                        "end_trade_date": previous_day or trade_date,
                        "available_at_end": _optional_datetime_value(row.get("available_at")),
                        "evidence": {
                            **active.evidence,
                            "closed_by": _signal_evidence(row),
                            "close_rule_key": removal_rule.rule_key,
                        },
                    }
                )
                spans[spans.index(active)] = closed
            spans.append(_cooldown_state_span(row, profile=profile, rule=removal_rule, trading_days=calendar))

    return spans


def _open_state_span(
    row: dict[str, Any],
    *,
    profile: PolicyProfileDraft,
    rule: EffectRuleDraft,
    signal_id: Optional[int],
) -> StateSpanDraft:
    trade_date = _date_value(row.get("effective_trade_date"))
    ts_code = str(row.get("ts_code") or "").strip().upper()
    time_mode = str(row.get("time_mode") or profile.time_mode)
    state_key = (
        f"event_signal_state:{profile.profile_id}:{time_mode}:{ts_code}:"
        f"{rule.state_type}:{signal_id or _stable_hash(_signal_evidence(row))[:16]}"
    )
    return StateSpanDraft(
        state_key=state_key,
        profile_id=profile.profile_id,
        ts_code=ts_code,
        time_mode=time_mode,
        state_family=rule.state_family or "st_hard_risk",
        state_type=rule.state_type or "st_active_block",
        state_status="OPEN",
        opened_by_signal_id=signal_id,
        closed_by_signal_id=None,
        open_event_type=str(row.get("event_type")),
        close_event_type=None,
        start_trade_date=trade_date,
        end_trade_date=None,
        expiry_trade_date=None,
        cooldown_until_trade_date=None,
        available_at_start=_optional_datetime_value(row.get("available_at")),
        available_at_end=None,
        source_time_quality=str(row.get("source_time_quality") or "MISSING"),
        policy_risk_level=rule.policy_risk_level,
        primary_action=rule.primary_action,
        severity_score=min(1.0, _numeric(row.get("severity_score"), 1.0) * rule.severity_weight),
        confidence=_numeric(row.get("confidence"), 1.0),
        score_delta=rule.score_delta,
        score_multiplier=rule.score_multiplier,
        effect_rule_key=rule.rule_key,
        policy_snapshot_hash=profile.config_hash,
        evidence={"opened_by": _signal_evidence(row), "open_rule_key": rule.rule_key},
    )


def _cooldown_state_span(
    row: dict[str, Any],
    *,
    profile: PolicyProfileDraft,
    rule: EffectRuleDraft,
    trading_days: list[dt.date],
) -> StateSpanDraft:
    start = _date_value(row.get("effective_trade_date"))
    cooldown_days = max(1, profile.st_removal_cooldown_trading_days)
    cooldown_until = _trading_day_offset(trading_days, start, cooldown_days - 1)
    signal_id = int(row["signal_id"]) if row.get("signal_id") is not None else None
    ts_code = str(row.get("ts_code") or "").strip().upper()
    time_mode = str(row.get("time_mode") or profile.time_mode)
    state_key = (
        f"event_signal_state:{profile.profile_id}:{time_mode}:{ts_code}:"
        f"st_removal_cooldown:{signal_id or _stable_hash(_signal_evidence(row))[:16]}"
    )
    return StateSpanDraft(
        state_key=state_key,
        profile_id=profile.profile_id,
        ts_code=ts_code,
        time_mode=time_mode,
        state_family="st_removal_cooldown",
        state_type="st_removal_cooldown",
        state_status="CLOSED",
        opened_by_signal_id=signal_id,
        closed_by_signal_id=signal_id,
        open_event_type=ST_REMOVED_CONFIRMED_EVENT_TYPE,
        close_event_type=ST_REMOVED_CONFIRMED_EVENT_TYPE,
        start_trade_date=start,
        end_trade_date=cooldown_until,
        expiry_trade_date=cooldown_until,
        cooldown_until_trade_date=cooldown_until,
        available_at_start=_optional_datetime_value(row.get("available_at")),
        available_at_end=_optional_datetime_value(row.get("available_at")),
        source_time_quality=str(row.get("source_time_quality") or "MISSING"),
        policy_risk_level=rule.policy_risk_level,
        primary_action=rule.primary_action,
        severity_score=min(1.0, _numeric(row.get("severity_score"), 0.5) * rule.severity_weight),
        confidence=_numeric(row.get("confidence"), 1.0),
        score_delta=rule.score_delta,
        score_multiplier=rule.score_multiplier,
        effect_rule_key=rule.rule_key,
        policy_snapshot_hash=profile.config_hash,
        evidence={
            "opened_by": _signal_evidence(row),
            "open_rule_key": rule.rule_key,
            "cooldown_trading_days": profile.st_removal_cooldown_trading_days,
        },
    )


def _signal_evidence(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": row.get("signal_id"),
        "ts_code": row.get("ts_code"),
        "event_type": row.get("event_type"),
        "effective_trade_date": _optional_date_value(row.get("effective_trade_date")),
        "source_event_date": _optional_date_value(row.get("source_event_date")),
        "source_time_quality": row.get("source_time_quality"),
        "rule_version": row.get("rule_version"),
        "reason": row.get("reason"),
    }


@dataclass(frozen=True)
class DailyOverlayDraft:
    overlay_key: str
    profile_id: str
    trade_date: dt.date
    ts_code: str
    time_mode: str
    decision_status: str
    can_buy: bool
    can_add: bool
    force_exit: bool
    sell_only: bool
    position_target_override: Optional[float]
    policy_risk_level: str
    primary_action: str
    risk_score: float
    alpha_score_delta: float
    score_multiplier: float
    score_overlay_enabled: bool
    active_state_span_ids: tuple[int, ...]
    active_state_keys: tuple[str, ...]
    active_signal_ids: tuple[int, ...]
    reason_codes: tuple[str, ...]
    evidence: dict[str, Any]


_RISK_RANK = {
    "P0_FORCE_EXIT": 0,
    "P0_BLOCK": 1,
    "P1_HIGH": 2,
    "P2_REVIEW": 3,
    "P3_POSITIVE_CANDIDATE": 4,
    "P4_NEUTRAL": 5,
}


_ACTION_RANK = {
    "force_exit": 0,
    "block_buy": 1,
    "block_add": 2,
    "score_down": 3,
    "warn": 4,
    "score_up": 5,
    "record_only": 6,
    "none": 7,
}


def generate_daily_overlays(
    state_spans: Iterable[StateSpanDraft],
    *,
    trading_days: Iterable[dt.date],
    profile: Optional[PolicyProfileDraft] = None,
) -> list[DailyOverlayDraft]:
    """Generate per-date overlay decisions from state spans."""

    profile = profile or default_st_policy_profile()
    calendar = sorted({_date_value(day) for day in trading_days})
    active_by_day_symbol: dict[tuple[str, str, dt.date], list[StateSpanDraft]] = {}
    for span in state_spans:
        start = span.start_trade_date
        end = span.end_trade_date or calendar[-1]
        for trade_date in calendar:
            if start <= trade_date <= end:
                active_by_day_symbol.setdefault((span.ts_code, span.time_mode, trade_date), []).append(span)

    overlays: list[DailyOverlayDraft] = []
    for (ts_code, time_mode, trade_date), spans in sorted(active_by_day_symbol.items(), key=lambda item: item[0]):
        overlays.append(_combine_overlay(profile=profile, ts_code=ts_code, time_mode=time_mode, trade_date=trade_date, spans=spans))
    return overlays


def _combine_overlay(
    *,
    profile: PolicyProfileDraft,
    ts_code: str,
    time_mode: str,
    trade_date: dt.date,
    spans: list[StateSpanDraft],
) -> DailyOverlayDraft:
    can_buy = True
    can_add = True
    force_exit = False
    sell_only = False
    position_target_override: Optional[float] = None
    risk_level = "P4_NEUTRAL"
    primary_action = "none"
    risk_score = 0.0
    alpha_score_delta = 0.0
    score_multiplier = 1.0
    score_overlay_enabled = False
    reason_codes: list[str] = []
    active_signal_ids: list[int] = []
    active_state_ids: list[int] = []
    active_state_keys: list[str] = []

    for span in spans:
        risk_score = max(risk_score, span.severity_score)
        if _RISK_RANK.get(span.policy_risk_level, 99) < _RISK_RANK.get(risk_level, 99):
            risk_level = span.policy_risk_level
        if _ACTION_RANK.get(span.primary_action, 99) < _ACTION_RANK.get(primary_action, 99):
            primary_action = span.primary_action
        if span.opened_by_signal_id is not None:
            active_signal_ids.append(span.opened_by_signal_id)
        state_id = getattr(span, "state_span_id", None)
        if state_id is not None:
            active_state_ids.append(int(state_id))
        active_state_keys.append(span.state_key)
        reason_codes.append(f"{span.state_family}:{span.state_type}:{span.primary_action}")

        if span.primary_action == "force_exit" or span.policy_risk_level == "P0_FORCE_EXIT":
            can_buy = False
            can_add = False
            force_exit = True
            sell_only = True
            position_target_override = 0.0
        elif span.primary_action == "block_buy" or span.policy_risk_level == "P0_BLOCK":
            can_buy = False
            can_add = False
        elif span.primary_action == "block_add":
            can_add = False
        elif span.primary_action == "score_down":
            alpha_score_delta += span.score_delta
            score_multiplier *= span.score_multiplier
            score_overlay_enabled = score_overlay_enabled or profile.positive_overlay_enabled
        elif span.primary_action == "score_up":
            if profile.positive_overlay_enabled and span.score_delta > 0:
                alpha_score_delta += min(span.score_delta, profile.max_positive_score_delta)
                score_multiplier *= span.score_multiplier
                score_overlay_enabled = True

    overlay_key = f"event_signal_overlay:{profile.profile_id}:{time_mode}:{trade_date.isoformat()}:{ts_code}"
    return DailyOverlayDraft(
        overlay_key=overlay_key,
        profile_id=profile.profile_id,
        trade_date=trade_date,
        ts_code=ts_code,
        time_mode=time_mode,
        decision_status="ACTIVE",
        can_buy=can_buy,
        can_add=can_add,
        force_exit=force_exit,
        sell_only=sell_only,
        position_target_override=position_target_override,
        policy_risk_level=risk_level,
        primary_action=primary_action,
        risk_score=risk_score,
        alpha_score_delta=alpha_score_delta,
        score_multiplier=score_multiplier,
        score_overlay_enabled=score_overlay_enabled,
        active_state_span_ids=tuple(sorted(set(active_state_ids))),
        active_state_keys=tuple(sorted(set(active_state_keys))),
        active_signal_ids=tuple(sorted(set(active_signal_ids))),
        reason_codes=tuple(sorted(set(reason_codes))),
        evidence={
            "profile_config_hash": profile.config_hash,
            "active_state_count": len(spans),
            "active_states": [span.state_key for span in spans],
        },
    )


def fetch_trading_days(conn: Any, *, start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    """Load trading days from the local calendar for lifecycle expansion."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date BETWEEN %s AND %s
             ORDER BY cal_date
            """,
            (start_date, end_date),
        )
        return [_date_value(row[0]) for row in cur.fetchall()]


def fetch_st_policy_source_signals(
    conn: Any,
    *,
    end_date: dt.date,
    start_date: Optional[dt.date] = None,
    time_mode: str = "backtest",
    rule_version: str = ST_FIRST_SIGNAL_RULE_VERSION,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Fetch ST-first source event_signal rows for state/overlay research."""

    params: list[Any] = [rule_version, time_mode, list((*ST_HARD_RISK_EVENT_TYPES, ST_REMOVAL_APPLIED_EVENT_TYPE, ST_REMOVED_CONFIRMED_EVENT_TYPE))]
    date_sql = " AND effective_trade_date <= %s"
    params.append(end_date)
    if start_date is not None:
        date_sql += " AND effective_trade_date >= %s"
        params.append(start_date)
    limit_sql = ""
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive when provided")
        limit_sql = " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT signal_id, signal_key, ts_code, source_event_date,
                   source_time_quality, available_at, effective_trade_date,
                   time_mode, event_family, event_type, risk_level, action,
                   severity_score, confidence, reason, evidence, rule_version
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND signal_status = 'ACTIVE'
               AND event_type = ANY(%s)
               {date_sql}
             ORDER BY ts_code, effective_trade_date, signal_id
             {limit_sql}
            """,
            tuple(params),
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _effect_rule_ids_by_key(conn: Any, profile_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rule_key, effect_rule_id
              FROM market.event_signal_effect_rule
             WHERE profile_id = %s
            """,
            (profile_id,),
        )
        return {str(row[0]): int(row[1]) for row in cur.fetchall()}


def upsert_state_spans(
    conn: Any,
    spans: Iterable[StateSpanDraft],
    *,
    run_id: Optional[str] = None,
) -> dict[str, int]:
    """Persist lifecycle state spans and return state_key -> state_span_id."""

    span_list = list(spans)
    if not span_list:
        return {}
    rule_ids = _effect_rule_ids_by_key(conn, span_list[0].profile_id)
    returned: dict[str, int] = {}
    with conn.cursor() as cur:
        for span in span_list:
            cur.execute(
                """
                INSERT INTO market.event_signal_state_span
                    (
                        state_key, profile_id, ts_code, time_mode, state_family,
                        state_type, state_status, opened_by_signal_id,
                        closed_by_signal_id, open_event_type, close_event_type,
                        start_trade_date, end_trade_date, expiry_trade_date,
                        cooldown_until_trade_date, available_at_start, available_at_end,
                        source_time_quality, policy_risk_level, primary_action,
                        severity_score, confidence, score_delta, score_multiplier,
                        effect_rule_id, run_id, policy_snapshot_hash, evidence,
                        updated_at
                    )
                VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, NOW()
                    )
                ON CONFLICT (state_key) DO UPDATE SET
                    profile_id = EXCLUDED.profile_id,
                    ts_code = EXCLUDED.ts_code,
                    time_mode = EXCLUDED.time_mode,
                    state_family = EXCLUDED.state_family,
                    state_type = EXCLUDED.state_type,
                    state_status = EXCLUDED.state_status,
                    opened_by_signal_id = EXCLUDED.opened_by_signal_id,
                    closed_by_signal_id = EXCLUDED.closed_by_signal_id,
                    open_event_type = EXCLUDED.open_event_type,
                    close_event_type = EXCLUDED.close_event_type,
                    start_trade_date = EXCLUDED.start_trade_date,
                    end_trade_date = EXCLUDED.end_trade_date,
                    expiry_trade_date = EXCLUDED.expiry_trade_date,
                    cooldown_until_trade_date = EXCLUDED.cooldown_until_trade_date,
                    available_at_start = EXCLUDED.available_at_start,
                    available_at_end = EXCLUDED.available_at_end,
                    source_time_quality = EXCLUDED.source_time_quality,
                    policy_risk_level = EXCLUDED.policy_risk_level,
                    primary_action = EXCLUDED.primary_action,
                    severity_score = EXCLUDED.severity_score,
                    confidence = EXCLUDED.confidence,
                    score_delta = EXCLUDED.score_delta,
                    score_multiplier = EXCLUDED.score_multiplier,
                    effect_rule_id = EXCLUDED.effect_rule_id,
                    run_id = EXCLUDED.run_id,
                    policy_snapshot_hash = EXCLUDED.policy_snapshot_hash,
                    evidence = EXCLUDED.evidence,
                    updated_at = NOW()
                RETURNING state_key, state_span_id
                """,
                (
                    span.state_key,
                    span.profile_id,
                    span.ts_code,
                    span.time_mode,
                    span.state_family,
                    span.state_type,
                    span.state_status,
                    span.opened_by_signal_id,
                    span.closed_by_signal_id,
                    span.open_event_type,
                    span.close_event_type,
                    span.start_trade_date,
                    span.end_trade_date,
                    span.expiry_trade_date,
                    span.cooldown_until_trade_date,
                    span.available_at_start,
                    span.available_at_end,
                    span.source_time_quality,
                    span.policy_risk_level,
                    span.primary_action,
                    span.severity_score,
                    span.confidence,
                    span.score_delta,
                    span.score_multiplier,
                    rule_ids.get(span.effect_rule_key or ""),
                    run_id,
                    span.policy_snapshot_hash,
                    _json_dumps(span.evidence),
                ),
            )
            state_key, state_span_id = cur.fetchone()
            returned[str(state_key)] = int(state_span_id)
    return returned


def attach_state_span_ids(spans: Iterable[StateSpanDraft], ids_by_key: dict[str, int]) -> list[StateSpanDraft]:
    """Return copies of spans with DB ids attached for overlay traceability."""

    return [replace(span, state_span_id=ids_by_key.get(span.state_key)) for span in spans]


def upsert_daily_overlays(
    conn: Any,
    overlays: Iterable[DailyOverlayDraft],
    *,
    run_id: Optional[str] = None,
) -> int:
    """Persist daily overlay rows.  Returns rows upserted."""

    count = 0
    with conn.cursor() as cur:
        for overlay in overlays:
            cur.execute(
                """
                INSERT INTO market.event_signal_daily_overlay
                    (
                        overlay_key, profile_id, trade_date, ts_code, time_mode,
                        decision_status, can_buy, can_add, force_exit, sell_only,
                        position_target_override, policy_risk_level, primary_action,
                        risk_score, alpha_score_delta, score_multiplier,
                        score_overlay_enabled, active_state_span_ids,
                        active_signal_ids, reason_codes, evidence, run_id, updated_at
                    )
                VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s::bigint[], %s::bigint[], %s::text[],
                        %s::jsonb, %s, NOW()
                    )
                ON CONFLICT (profile_id, time_mode, trade_date, ts_code) DO UPDATE SET
                    overlay_key = EXCLUDED.overlay_key,
                    decision_status = EXCLUDED.decision_status,
                    can_buy = EXCLUDED.can_buy,
                    can_add = EXCLUDED.can_add,
                    force_exit = EXCLUDED.force_exit,
                    sell_only = EXCLUDED.sell_only,
                    position_target_override = EXCLUDED.position_target_override,
                    policy_risk_level = EXCLUDED.policy_risk_level,
                    primary_action = EXCLUDED.primary_action,
                    risk_score = EXCLUDED.risk_score,
                    alpha_score_delta = EXCLUDED.alpha_score_delta,
                    score_multiplier = EXCLUDED.score_multiplier,
                    score_overlay_enabled = EXCLUDED.score_overlay_enabled,
                    active_state_span_ids = EXCLUDED.active_state_span_ids,
                    active_signal_ids = EXCLUDED.active_signal_ids,
                    reason_codes = EXCLUDED.reason_codes,
                    evidence = EXCLUDED.evidence,
                    run_id = EXCLUDED.run_id,
                    updated_at = NOW()
                """,
                (
                    overlay.overlay_key,
                    overlay.profile_id,
                    overlay.trade_date,
                    overlay.ts_code,
                    overlay.time_mode,
                    overlay.decision_status,
                    overlay.can_buy,
                    overlay.can_add,
                    overlay.force_exit,
                    overlay.sell_only,
                    overlay.position_target_override,
                    overlay.policy_risk_level,
                    overlay.primary_action,
                    overlay.risk_score,
                    overlay.alpha_score_delta,
                    overlay.score_multiplier,
                    overlay.score_overlay_enabled,
                    list(overlay.active_state_span_ids),
                    list(overlay.active_signal_ids),
                    list(overlay.reason_codes),
                    _json_dumps({**overlay.evidence, "active_state_keys": list(overlay.active_state_keys)}),
                    run_id,
                ),
            )
            count += 1
    return count


def build_st_policy_overlay(
    conn: Any,
    *,
    start_date: dt.date,
    end_date: dt.date,
    source_start_date: Optional[dt.date] = None,
    time_mode: str = "backtest",
    write: bool = False,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Build ST policy state spans and overlays, optionally writing them to DB."""

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")
    profile = default_st_policy_profile(
        base_rule_versions={"st_first": ST_FIRST_SIGNAL_RULE_VERSION},
        time_mode=time_mode,
    )
    effect_rules = default_st_effect_rules(source_rule_version=ST_FIRST_SIGNAL_RULE_VERSION)
    source_start = source_start_date or dt.date(2018, 8, 1)
    signal_rows = fetch_st_policy_source_signals(
        conn,
        start_date=source_start,
        end_date=end_date,
        time_mode=time_mode,
        rule_version=ST_FIRST_SIGNAL_RULE_VERSION,
        limit=limit,
    )
    calendar_start = min([start_date, *[_date_value(row["effective_trade_date"]) for row in signal_rows]], default=start_date)
    calendar_end = end_date + dt.timedelta(days=30)
    trading_days = fetch_trading_days(conn, start_date=calendar_start, end_date=calendar_end)
    spans = generate_st_state_spans(signal_rows, trading_days=trading_days, profile=profile, effect_rules=effect_rules)
    overlays = [
        overlay
        for overlay in generate_daily_overlays(spans, trading_days=trading_days, profile=profile)
        if start_date <= overlay.trade_date <= end_date
    ]
    ids_by_key: dict[str, int] = {}
    if write:
        seed_default_st_policy(conn, profile=profile, effect_rules=effect_rules)
        ids_by_key = upsert_state_spans(conn, spans)
        overlays = [
            overlay
            for overlay in generate_daily_overlays(attach_state_span_ids(spans, ids_by_key), trading_days=trading_days, profile=profile)
            if start_date <= overlay.trade_date <= end_date
        ]
        upsert_daily_overlays(conn, overlays)
    action_counts: dict[str, int] = {}
    for overlay in overlays:
        action_counts[overlay.primary_action] = action_counts.get(overlay.primary_action, 0) + 1
    return {
        "profile_id": profile.profile_id,
        "time_mode": time_mode,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "source_start_date": source_start.isoformat(),
        "source_signal_rows": len(signal_rows),
        "state_spans": len(spans),
        "daily_overlays": len(overlays),
        "force_exit_overlays": sum(1 for row in overlays if row.force_exit),
        "block_buy_overlays": sum(1 for row in overlays if not row.can_buy),
        "unique_symbols": len({row.ts_code for row in overlays}),
        "action_counts": action_counts,
        "written": bool(write),
        "state_span_ids_attached": len(ids_by_key),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build ST event-signal policy lifecycle overlays")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--source-start-date", default=None)
    parser.add_argument("--time-mode", default="backtest", choices=["backtest", "paper", "live", "observed"])
    parser.add_argument("--write", action="store_true", help="Persist profile, state spans, and daily overlays")
    parser.add_argument("--limit", type=int, default=None, help="Optional source signal row limit for smoke tests")
    parser.add_argument("--ensure-schema", action="store_true", help="Apply event signal schema migrations before running")
    args = parser.parse_args(argv)

    load_dotenv(override=True)
    if args.ensure_schema:
        init_unified_event_signal_schema()
    with get_conn() as conn:
        summary = build_st_policy_overlay(
            conn,
            start_date=dt.date.fromisoformat(args.start_date),
            end_date=dt.date.fromisoformat(args.end_date),
            source_start_date=dt.date.fromisoformat(args.source_start_date) if args.source_start_date else None,
            time_mode=args.time_mode,
            write=args.write,
            limit=args.limit,
        )
        conn.commit()
    print(_json_dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
