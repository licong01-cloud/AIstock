import datetime as dt
from decimal import Decimal

from backend.services.event_signal.policy_lifecycle import (
    DEFAULT_ST_POLICY_PROFILE_ID,
    ST_HARD_RISK_EVENT_TYPES,
    ST_REMOVAL_APPLIED_EVENT_TYPE,
    ST_REMOVED_CONFIRMED_EVENT_TYPE,
    default_st_effect_rules,
    default_st_policy_profile,
    generate_st_state_spans,
)


TRADING_DAYS = [
    dt.date(2026, 1, 2),
    dt.date(2026, 1, 5),
    dt.date(2026, 1, 6),
    dt.date(2026, 1, 7),
    dt.date(2026, 1, 8),
    dt.date(2026, 1, 9),
    dt.date(2026, 1, 12),
]


def _signal(signal_id: int, event_type: str, trade_date: dt.date) -> dict:
    return {
        "signal_id": signal_id,
        "ts_code": "000001.SZ",
        "time_mode": "backtest",
        "event_type": event_type,
        "source_event_date": trade_date,
        "source_time_quality": "EXACT",
        "effective_trade_date": trade_date,
        "available_at": dt.datetime(2026, 1, 1, 18, 0, tzinfo=dt.timezone.utc),
        "severity_score": Decimal("1.0"),
        "confidence": Decimal("0.95"),
        "rule_version": "unified_event_signal_rules_st_first_v1_20260506",
        "reason": event_type,
    }


def test_default_st_policy_profile_is_research_only_and_formal_removal_required():
    profile = default_st_policy_profile(base_rule_versions={"st_first": "v1"})

    assert profile.profile_id == DEFAULT_ST_POLICY_PROFILE_ID
    assert profile.policy_scope == "research_overlay"
    assert profile.positive_overlay_enabled is False
    assert profile.formal_st_removal_required is True
    assert profile.allow_buy_on_st_removal_expectation is False
    assert profile.st_removal_cooldown_trading_days == 5
    assert profile.max_positive_score_delta == 0.0
    assert profile.max_negative_score_delta == 0.0
    assert profile.config["signal_onboarding"]["baseline_experiment_id"] == "qe_20260507_132049_d4e7"
    assert profile.config["base_rule_versions"] == {"st_first": "v1"}


def test_default_st_effect_rules_map_hard_events_to_policy_force_exit():
    rules = {rule.event_type: rule for rule in default_st_effect_rules(source_rule_version="st-v1")}

    for event_type in ST_HARD_RISK_EVENT_TYPES:
        rule = rules[event_type]
        assert rule.policy_risk_level == "P0_FORCE_EXIT"
        assert rule.primary_action == "force_exit"
        assert rule.block_buy is True
        assert rule.block_add is True
        assert rule.force_exit is True
        assert rule.sell_only is True
        assert rule.score_overlay_enabled is False
        assert rule.source_rule_version == "st-v1"

    applied = rules[ST_REMOVAL_APPLIED_EVENT_TYPE]
    assert applied.lifecycle_kind == "record_only"
    assert applied.force_exit is False
    assert applied.block_buy is False
    assert applied.rule_params["does_not_close_st_hard_risk"] is True

    removed = rules[ST_REMOVED_CONFIRMED_EVENT_TYPE]
    assert removed.lifecycle_kind == "close_state"
    assert removed.policy_risk_level == "P0_BLOCK"
    assert removed.primary_action == "block_buy"
    assert removed.cooldown_trading_days == 5


def test_generate_st_state_spans_waits_for_formal_removal_and_adds_cooldown():
    spans = generate_st_state_spans(
        [
            _signal(1, "stock_st_imposed", dt.date(2026, 1, 2)),
            _signal(2, ST_REMOVAL_APPLIED_EVENT_TYPE, dt.date(2026, 1, 5)),
            _signal(3, ST_REMOVED_CONFIRMED_EVENT_TYPE, dt.date(2026, 1, 6)),
        ],
        trading_days=TRADING_DAYS,
    )

    assert len(spans) == 2
    hard = spans[0]
    cooldown = spans[1]

    assert hard.state_family == "st_hard_risk"
    assert hard.policy_risk_level == "P0_FORCE_EXIT"
    assert hard.primary_action == "force_exit"
    assert hard.start_trade_date == dt.date(2026, 1, 2)
    assert hard.end_trade_date == dt.date(2026, 1, 5)
    assert hard.closed_by_signal_id == 3
    assert hard.close_event_type == ST_REMOVED_CONFIRMED_EVENT_TYPE
    assert hard.evidence["closed_by"]["signal_id"] == 3

    assert cooldown.state_family == "st_removal_cooldown"
    assert cooldown.policy_risk_level == "P0_BLOCK"
    assert cooldown.primary_action == "block_buy"
    assert cooldown.start_trade_date == dt.date(2026, 1, 6)
    assert cooldown.end_trade_date == dt.date(2026, 1, 12)
    assert cooldown.cooldown_until_trade_date == dt.date(2026, 1, 12)
    assert cooldown.evidence["cooldown_trading_days"] == 5


def test_generate_st_state_spans_creates_cooldown_even_when_open_event_is_outside_window():
    spans = generate_st_state_spans(
        [_signal(10, ST_REMOVED_CONFIRMED_EVENT_TYPE, dt.date(2026, 1, 6))],
        trading_days=TRADING_DAYS,
    )

    assert len(spans) == 1
    cooldown = spans[0]
    assert cooldown.state_family == "st_removal_cooldown"
    assert cooldown.start_trade_date == dt.date(2026, 1, 6)
    assert cooldown.end_trade_date == dt.date(2026, 1, 12)
    assert cooldown.opened_by_signal_id == 10


def test_generate_st_state_spans_does_not_open_overlapping_hard_states():
    spans = generate_st_state_spans(
        [
            _signal(1, "stock_st_imposed", dt.date(2026, 1, 2)),
            _signal(2, "stock_delisting_risk_warning", dt.date(2026, 1, 5)),
        ],
        trading_days=TRADING_DAYS,
    )

    assert len(spans) == 1
    assert spans[0].opened_by_signal_id == 1
    assert spans[0].state_status == "OPEN"


def test_generate_daily_overlays_maps_hard_risk_to_force_exit_decision():
    spans = generate_st_state_spans(
        [_signal(1, "stock_st_imposed", dt.date(2026, 1, 2))],
        trading_days=TRADING_DAYS,
    )

    from backend.services.event_signal.policy_lifecycle import generate_daily_overlays

    overlays = generate_daily_overlays(spans, trading_days=TRADING_DAYS)
    first = overlays[0]

    assert first.trade_date == dt.date(2026, 1, 2)
    assert first.policy_risk_level == "P0_FORCE_EXIT"
    assert first.primary_action == "force_exit"
    assert first.can_buy is False
    assert first.can_add is False
    assert first.force_exit is True
    assert first.sell_only is True
    assert first.position_target_override == 0.0
    assert first.active_signal_ids == (1,)


def test_generate_daily_overlays_maps_removal_cooldown_to_buy_block_only():
    spans = generate_st_state_spans(
        [_signal(10, ST_REMOVED_CONFIRMED_EVENT_TYPE, dt.date(2026, 1, 6))],
        trading_days=TRADING_DAYS,
    )

    from backend.services.event_signal.policy_lifecycle import generate_daily_overlays

    overlays = generate_daily_overlays(spans, trading_days=TRADING_DAYS)
    assert [row.trade_date for row in overlays] == [
        dt.date(2026, 1, 6),
        dt.date(2026, 1, 7),
        dt.date(2026, 1, 8),
        dt.date(2026, 1, 9),
        dt.date(2026, 1, 12),
    ]
    for overlay in overlays:
        assert overlay.policy_risk_level == "P0_BLOCK"
        assert overlay.primary_action == "block_buy"
        assert overlay.can_buy is False
        assert overlay.force_exit is False
        assert overlay.position_target_override is None
