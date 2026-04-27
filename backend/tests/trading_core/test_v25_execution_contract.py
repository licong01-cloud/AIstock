from __future__ import annotations

import math
from datetime import date, datetime

import numpy as np
import pytest

from backend.execution_algos.base_algo import BaseExecutionAlgo
from backend.execution_algos.v25_core import (
    EARLY_LEN,
    EARLY_WEIGHT,
    LATE_LEN,
    LATE_WEIGHT,
    REASON_LIMIT_UP_BUY_BLOCKED,
    REASON_P0_LIMIT_BUY_AT_DOWN_LIMIT,
    REASON_PREV_CLOSE_MISSING_DATA_ERROR,
    REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
    TOTAL_LEN,
    V25MarketAction,
    V25TwoStageCore,
    classify_v25_minute_market_state,
)
from backend.execution_algos.v25_two_stage_algo import V25TwoStageAlgo, V25TwoStageUnavailableError
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import MinuteBar, OrderEventType, OrderIntent, OrderSide, OrderStatus
from backend.services.trading_core.oms import OMS


def _early_predictor(*_args) -> np.ndarray:
    return np.ones(EARLY_LEN, dtype=np.float64) / EARLY_LEN


def _late_predictor(*_args) -> np.ndarray:
    return np.ones(LATE_LEN, dtype=np.float64) / LATE_LEN


def _core() -> V25TwoStageCore:
    return V25TwoStageCore(early_predictor=_early_predictor, late_predictor=_late_predictor)


def _algo() -> V25TwoStageAlgo:
    algo = object.__new__(V25TwoStageAlgo)
    BaseExecutionAlgo.__init__(algo, config={})
    algo._core = _core()
    algo._plan = None
    algo._plan_key = None
    algo._plan_metadata = {}
    algo._last_no_fill_reason = None
    algo._last_no_fill_context = {}
    return algo


def _patch_registry_v25(monkeypatch) -> None:
    def fake_init(self, config=None):
        BaseExecutionAlgo.__init__(self, config=config or {})
        self._core = _core()
        self._plan = None
        self._plan_key = None
        self._plan_metadata = {}
        self._last_no_fill_reason = None
        self._last_no_fill_context = {}

    monkeypatch.setattr(V25TwoStageAlgo, "__init__", fake_init)


def _state(side: str = "BUY", quantity: int = 10000):
    return _algo().init_order("000001.SZ", side, quantity)


def _context(
    *,
    bars: int = 1,
    observed_only: bool = True,
    prev_close=10.0,
    day_features=True,
):
    ctx = {
        "stock_id": "000001.SZ",
        "prev_close": prev_close,
        "full_day_open": [10.0] * bars,
        "full_day_close": [10.1] * bars,
        "full_day_volume": [10000] * bars,
        "full_day_high": [10.2] * bars,
        "full_day_low": [9.8] * bars,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "observed_only": observed_only,
        "v25_realtime_streaming": observed_only,
    }
    if day_features:
        ctx["day_features"] = [0.1] * 10
    return ctx


def _bar(close: float = 10.1, *, limit_up=11.0, limit_down=9.0, suspended: bool = False):
    return {
        "open": 10.0,
        "high": max(10.2, close),
        "low": min(9.8, close),
        "close": close,
        "volume": 10000,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "is_suspended": suspended,
    }


def _minute_bar(close: float = 10.1) -> MinuteBar:
    return MinuteBar(
        symbol="000001.SZ",
        bar_time=datetime(2024, 1, 2, 9, 31),
        open=10.0,
        high=max(10.2, close),
        low=min(9.8, close),
        close=close,
        volume=10000,
        limit_up=11.0,
        limit_down=9.0,
    )


def _order(quantity: int = 300):
    return OMS().create_order(
        OrderIntent(
            package_id="pkg",
            portfolio_id="paper",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=quantity,
            target_trade_date=date(2024, 1, 2),
        )
    )


def test_v25_core_generates_authoritative_240_bar_plan() -> None:
    result = _core().generate_plan(
        open_price=10.1,
        prev_close=10.0,
        stock_id="000001.SZ",
        side="BUY",
        limit_pct=0.10,
        day_features=np.ones(10, dtype=np.float32),
    )

    assert len(result.weights) == TOTAL_LEN
    assert math.isclose(float(result.weights[:EARLY_LEN].sum()), EARLY_WEIGHT, abs_tol=1e-4)
    assert math.isclose(float(result.weights[EARLY_LEN:].sum()), LATE_WEIGHT, abs_tol=1e-4)
    assert math.isclose(float(result.weights.sum()), 1.0, abs_tol=1e-8)


def test_v25_market_classifier_separates_business_state_from_data_error() -> None:
    suspended = classify_v25_minute_market_state(
        side="BUY",
        price=10.0,
        prev_close=float("nan"),
        limit_up=None,
        limit_down=None,
        suspend_status={"is_suspended": True, "source": "market.suspend_d", "suspend_type": "S"},
    )
    assert suspended.action == V25MarketAction.SKIP
    assert suspended.reason == REASON_PREV_CLOSE_MISSING_WITH_SUSPEND

    data_error = classify_v25_minute_market_state(
        side="BUY",
        price=10.0,
        prev_close=float("nan"),
        limit_up=11.0,
        limit_down=9.0,
    )
    assert data_error.action == V25MarketAction.DATA_ERROR
    assert data_error.reason == REASON_PREV_CLOSE_MISSING_DATA_ERROR


def test_v25_paper_adapter_skips_confirmed_suspend_without_requiring_full_context() -> None:
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 300)

    result = algo.compute_step(
        state,
        _bar(close=10.0, suspended=True, limit_up=None, limit_down=None),
        {
            "prev_close": float("nan"),
            "suspend_status": {"is_suspended": True, "source": "market.suspend_d", "suspend_type": "S"},
        },
    )

    assert result is None
    assert state.step == 1
    assert algo._last_no_fill_reason == REASON_PREV_CLOSE_MISSING_WITH_SUSPEND


def test_v25_paper_adapter_fails_prev_close_missing_without_suspend_evidence() -> None:
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 300)

    with pytest.raises(V25TwoStageUnavailableError, match=REASON_PREV_CLOSE_MISSING_DATA_ERROR):
        algo.compute_step(
            state,
            _bar(close=10.0),
            _context(prev_close=float("nan")),
        )


def test_v25_paper_adapter_handles_limit_block_and_p0_without_fallback() -> None:
    algo = _algo()
    blocked_state = algo.init_order("000001.SZ", "BUY", 300)
    blocked = algo.compute_step(
        blocked_state,
        _bar(close=11.0),
        _context(),
    )
    assert blocked is None
    assert blocked_state.step == 1
    assert algo._last_no_fill_reason == REASON_LIMIT_UP_BUY_BLOCKED

    p0_algo = _algo()
    p0_state = p0_algo.init_order("000001.SZ", "BUY", 300)
    p0 = p0_algo.compute_step(
        p0_state,
        _bar(close=9.0),
        _context(),
    )
    assert p0 is not None
    assert p0.quantity == 300
    assert p0.reason == REASON_P0_LIMIT_BUY_AT_DOWN_LIMIT
    assert p0_state.is_complete is True


def test_v25_realtime_streaming_accepts_one_observed_bar_and_persists_plan() -> None:
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 10000)

    result = algo.compute_step(
        state,
        _bar(close=10.1),
        _context(bars=1, observed_only=True),
    )

    assert result is not None
    assert result.quantity > 0
    assert algo._plan is not None
    assert len(algo._plan) == TOTAL_LEN
    assert algo._plan_metadata["plan_horizon_bars"] == TOTAL_LEN


def test_v25_historical_replay_requires_full_day_bars() -> None:
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 10000)

    with pytest.raises(V25TwoStageUnavailableError, match="at least 240"):
        algo.compute_step(
            state,
            _bar(close=10.1),
            _context(bars=1, observed_only=False),
        )


def test_v25_requires_day_features_in_authoritative_mode() -> None:
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 10000)

    with pytest.raises(V25TwoStageUnavailableError, match="day_features"):
        algo.compute_step(
            state,
            _bar(close=10.1),
            _context(bars=1, observed_only=True, day_features=False),
        )


def test_minute_execution_engine_lets_v25_handle_limit_block_as_business_state(monkeypatch) -> None:
    _patch_registry_v25(monkeypatch)
    engine = MinuteExecutionEngine()

    final_order, fills, events = engine.execute_order(
        order=_order(quantity=300),
        minute_bars=[_minute_bar(close=11.0)],
        algo_code="V25_TWO_STAGE",
        algo_config={},
        market_context=_context(bars=1, observed_only=True),
    )

    assert final_order.status == OrderStatus.SUBMITTED
    assert fills == []
    assert len(events) == 1
    assert events[0].event_type == OrderEventType.NO_FILL
    assert events[0].reason == REASON_LIMIT_UP_BUY_BLOCKED
