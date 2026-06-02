from __future__ import annotations

import inspect

import pytest

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.price_guard import (
    ADD_BREAKOUT_NEAR_LIMIT,
    ACCEPT_WITHIN_GREEN_ZONE,
    LIMIT_PRICE_MISSING_DATA_ERROR,
    PRICE_BASIS_MISMATCH_ERROR,
    PG_SKIP_NEAR_LIMIT_UP,
    PRE_FILTER_LIMIT_UP,
    REDUCE_YELLOW_CHASE_BAND,
    REDUCE_YELLOW_OPEN_GAP,
    REASON_CODES,
    SIGNAL_REF_PRICE_MISSING_DATA_ERROR,
    SKIP_ABOVE_MAX_BUY_PRICE,
    SKIP_BREAKOUT_LOW_FILL_PROBABILITY,
    SKIP_NEAR_LIMIT_UP,
    SKIP_OPEN_GAP_EXCEEDED,
    PriceGuardContext,
    PriceGuardPolicy,
    evaluate,
)


def _ctx(**updates) -> PriceGuardContext:
    base = {
        "signal_ref_price": 10.0,
        "prev_close": 10.0,
        "open_price": 10.0,
        "current_price": 10.0,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "price_basis": "raw",
        "side": "buy",
    }
    base.update(updates)
    return PriceGuardContext(**base)


def test_s1_1_price_guard_is_pure_and_deterministic() -> None:
    source = inspect.getsource(evaluate)

    assert "get_conn" not in source
    assert "requests" not in source
    assert "open(" not in source
    policy = PriceGuardPolicy()
    ctx = _ctx()

    assert evaluate(ctx, policy) == evaluate(ctx, policy)


def test_s1_2_green_yellow_red_and_chase_decisions() -> None:
    policy = PriceGuardPolicy()

    assert evaluate(_ctx(open_price=10.04, current_price=10.04), policy).reason_code == ACCEPT_WITHIN_GREEN_ZONE
    open_gap_policy = PriceGuardPolicy.from_dict({"buy": {"max_chase_bps": 300}})
    assert evaluate(_ctx(open_price=10.2, current_price=10.2), open_gap_policy).reason_code == REDUCE_YELLOW_OPEN_GAP
    assert evaluate(_ctx(open_price=10.31, current_price=10.31), open_gap_policy).reason_code == SKIP_OPEN_GAP_EXCEEDED
    assert evaluate(
        _ctx(open_price=10.04, current_price=10.07, open_gap_bps=40, current_gap_bps=70),
        policy,
    ).reason_code == REDUCE_YELLOW_CHASE_BAND
    assert evaluate(
        _ctx(open_price=None, current_price=10.12, open_gap_bps=40, current_gap_bps=120),
        policy,
    ).reason_code == SKIP_ABOVE_MAX_BUY_PRICE


def test_s1_2_near_limit_and_breakout_addon() -> None:
    policy = PriceGuardPolicy()

    assert evaluate(_ctx(open_price=10.95, current_price=10.95, dist_to_limit_up_bps=45), policy).reason_code == SKIP_NEAR_LIMIT_UP

    breakout_policy = PriceGuardPolicy.from_dict(
        {
            "buy": {
                "breakout_addon": {
                    "enabled": True,
                    "require_momentum_regime": True,
                    "min_score_bucket": "top5",
                    "dist_to_limit_up_lt_bps": 200,
                    "min_volume_ratio_open": 1.5,
                    "add_size_multiplier": 0.5,
                    "min_fill_probability": 0.6,
                }
            }
        }
    )
    add = evaluate(
        _ctx(
            open_price=10.9,
            current_price=10.9,
            dist_to_limit_up_bps=90,
            momentum_regime="momentum",
            score_bucket="top5",
            volume_ratio_open=2.0,
            fill_probability=0.8,
        ),
        breakout_policy,
    )
    assert add.reason_code == ADD_BREAKOUT_NEAR_LIMIT
    assert add.size_multiplier == pytest.approx(0.5)

    low_fill = evaluate(
        _ctx(
            open_price=10.9,
            current_price=10.9,
            dist_to_limit_up_bps=90,
            momentum_regime="momentum",
            score_bucket="top5",
            volume_ratio_open=2.0,
            fill_probability=0.2,
        ),
        breakout_policy,
    )
    assert low_fill.reason_code == SKIP_BREAKOUT_LOW_FILL_PROBABILITY


def test_s1_2_fail_fast_missing_signal_ref_price_basis_and_limits() -> None:
    with pytest.raises(DataUnavailableError) as missing_signal:
        evaluate(_ctx(signal_ref_price=None), PriceGuardPolicy())
    assert missing_signal.value.context["reason_code"] == SIGNAL_REF_PRICE_MISSING_DATA_ERROR

    with pytest.raises(RuntimeConfigInvalidError) as basis:
        evaluate(_ctx(price_basis="adjusted"), PriceGuardPolicy())
    assert basis.value.context["reason_code"] == PRICE_BASIS_MISMATCH_ERROR

    with pytest.raises(DataUnavailableError) as missing_limit:
        evaluate(_ctx(limit_up=None), PriceGuardPolicy())
    assert missing_limit.value.context["reason_code"] == LIMIT_PRICE_MISSING_DATA_ERROR


def test_s1_2_reason_code_catalog_is_complete() -> None:
    expected = {
        PRE_FILTER_LIMIT_UP,
        PG_SKIP_NEAR_LIMIT_UP,
        SIGNAL_REF_PRICE_MISSING_DATA_ERROR,
        PRICE_BASIS_MISMATCH_ERROR,
        LIMIT_PRICE_MISSING_DATA_ERROR,
    }

    assert expected.issubset(REASON_CODES)
