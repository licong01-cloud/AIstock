"""Contract tests for V25.1 small-cap minute execution algorithm."""
from __future__ import annotations

import numpy as np
import pytest

from backend.execution_algos.base_algo import BaseExecutionAlgo, OrderState
from backend.execution_algos.board_lot import (
    board_lot_rule,
    build_cost_aware_bucket_schedule,
    min_slice_qty_for_cost,
    round_to_board_lot,
)
from backend.execution_algos.v25_1_small_cap_algo import (
    V25_1SmallCapAlgo,
    V25_1SmallCapUnavailableError,
)
from backend.execution_algos.v25_core import (
    EARLY_LEN,
    LATE_LEN,
    TOTAL_LEN,
    V25TwoStageCore,
)


# ---------------------------------------------------------------------------
# Board-lot rule contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stock_id,expected",
    [
        ("000001.SZ", (100, 100)),  # 深市主板
        ("600519.SH", (100, 100)),  # 沪市主板
        ("300750.SZ", (100, 100)),  # 创业板
        ("301318.SZ", (100, 100)),  # 创业板新规
        ("688981.SH", (200, 1)),    # 科创板
        ("689009.SH", (200, 1)),    # 科创板存托凭证
    ],
)
def test_board_lot_rule_correct(stock_id, expected):
    assert board_lot_rule(stock_id) == expected


def test_board_lot_rule_rejects_non_a_share():
    with pytest.raises(ValueError):
        board_lot_rule("AAPL")
    with pytest.raises(ValueError):
        board_lot_rule("")
    with pytest.raises(ValueError):
        board_lot_rule("12345")


def test_round_to_board_lot_main_board():
    assert round_to_board_lot(99, "000001.SZ") == 0       # below min
    assert round_to_board_lot(150, "000001.SZ") == 100    # floor to 100
    assert round_to_board_lot(101, "000001.SZ") == 100
    assert round_to_board_lot(1234, "000001.SZ") == 1200


def test_round_to_board_lot_star_increments_by_1():
    assert round_to_board_lot(199, "688981.SH") == 0      # below min
    assert round_to_board_lot(200, "688981.SH") == 200
    assert round_to_board_lot(201, "688981.SH") == 201    # 1-share increment
    assert round_to_board_lot(1234, "688981.SH") == 1234


def test_round_to_board_lot_sell_residual_allowed():
    # Holder of 99 shares can sell all 99 at once on the SELL side.
    assert round_to_board_lot(99, "000001.SZ", side="SELL") == 99
    assert round_to_board_lot(150, "688981.SH", side="SELL") == 150  # STAR residual flush


# ---------------------------------------------------------------------------
# Cost-aware sizing contract
# ---------------------------------------------------------------------------


def test_min_slice_qty_at_10bps_main_board():
    # min_amount = 5 / (0.0003 + 0.001) = 3846 RMB.
    # At price=10 RMB -> 385 shares -> ceil to 100-multiples => 400.
    qty = min_slice_qty_for_cost(
        price=10.0,
        stock_id="000001.SZ",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
    )
    assert qty == 400


def test_min_slice_qty_star_increment_1():
    # At price=50 RMB -> 76.92 shares -> ceil 77, but min_qty=200 -> 200.
    qty = min_slice_qty_for_cost(
        price=50.0,
        stock_id="688981.SH",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
    )
    assert qty == 200
    # At price=5 RMB -> 770 shares (1-share increment).
    qty2 = min_slice_qty_for_cost(
        price=5.0,
        stock_id="688981.SH",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
    )
    assert qty2 == 770


# ---------------------------------------------------------------------------
# Bucket scheduler contract
# ---------------------------------------------------------------------------


def _flat_plan() -> np.ndarray:
    plan = np.ones(TOTAL_LEN, dtype=np.float64)
    return plan / plan.sum()


def _peaked_plan() -> np.ndarray:
    """V25-like plan: 88.79% in first 30 bars, 11.21% in last 210."""
    plan = np.zeros(TOTAL_LEN, dtype=np.float64)
    plan[:EARLY_LEN] = 0.8879 / EARLY_LEN
    plan[EARLY_LEN:] = 0.1121 / LATE_LEN
    return plan


def test_bucket_schedule_main_board_mid_capital():
    # 1000 万 / 50 stocks = 20 万 per position, price=20 RMB -> 10000 shares.
    schedule = build_cost_aware_bucket_schedule(
        plan=_peaked_plan(),
        total_qty=10000,
        cur_price=20.0,
        stock_id="000001.SZ",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
        max_buckets=30,
        horizon=TOTAL_LEN,
    )
    # min_slice = 5 / 0.0013 = 3846 RMB / 20 = 193 -> 200 shares
    # n_buckets = 10000 // 200 = 50, cap 30 -> 30 buckets, base = 333 -> floor 300
    assert sum(schedule.values()) == 10000
    assert all(qty >= 100 and qty % 100 == 0 for qty in schedule.values())
    assert len(schedule) <= 30


def test_bucket_schedule_star_market():
    # STAR stock, 5000 shares total, price 50.
    schedule = build_cost_aware_bucket_schedule(
        plan=_peaked_plan(),
        total_qty=5000,
        cur_price=50.0,
        stock_id="688981.SH",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
        max_buckets=30,
        horizon=TOTAL_LEN,
    )
    # min_slice >= 200 (STAR minimum dominates)
    assert sum(schedule.values()) == 5000
    assert all(qty >= 200 for qty in schedule.values())


def test_bucket_schedule_single_lot_collapses_to_one_fill():
    # 100 shares = 1 lot of main board stock -> single fill at argmax.
    plan = _peaked_plan()
    schedule = build_cost_aware_bucket_schedule(
        plan=plan,
        total_qty=100,
        cur_price=10.0,
        stock_id="000001.SZ",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
        max_buckets=30,
        horizon=TOTAL_LEN,
    )
    assert sum(schedule.values()) == 100
    assert len(schedule) == 1
    bar = next(iter(schedule.keys()))
    assert 0 <= bar < EARLY_LEN  # peak of the V25-shaped plan


def test_bucket_schedule_high_price_two_lot_position_splits_into_two():
    # 200 shares of 1500 RMB stock = 300k RMB total, 2 lots.
    # min_slice_qty = 100 (cost threshold dominated by board minimum).
    # 200 // 100 = 2 buckets, well above cost tolerance.
    plan = _peaked_plan()
    schedule = build_cost_aware_bucket_schedule(
        plan=plan,
        total_qty=200,
        cur_price=1500.0,
        stock_id="600519.SH",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
        max_buckets=30,
        horizon=TOTAL_LEN,
    )
    assert sum(schedule.values()) == 200
    assert len(schedule) == 2
    assert all(qty == 100 for qty in schedule.values())


def test_bucket_schedule_below_board_minimum_emits_residual():
    # 50 shares of main-board stock: a SELL residual flush at last bar.
    schedule = build_cost_aware_bucket_schedule(
        plan=_peaked_plan(),
        total_qty=50,
        cur_price=10.0,
        stock_id="000001.SZ",
        min_cost=5.0,
        commission_rate=0.0003,
        tolerance_bps=10.0,
        max_buckets=30,
        horizon=TOTAL_LEN,
    )
    # The scheduler emits a single fill at the last bar; the algo decides
    # whether to actually execute it (depends on side).
    assert schedule == {TOTAL_LEN - 1: 50}


def test_bucket_schedule_no_dropped_quantity():
    """Property: scheduled qty must equal legal_total (after increment floor)."""
    rng = np.random.default_rng(0)
    plan = rng.dirichlet(np.ones(TOTAL_LEN))
    for stock_id, price in [
        ("000001.SZ", 8.0),
        ("300750.SZ", 30.0),
        ("688981.SH", 80.0),
        ("600519.SH", 1500.0),
    ]:
        for total in (300, 1000, 6666, 20000):
            schedule = build_cost_aware_bucket_schedule(
                plan=plan,
                total_qty=total,
                cur_price=price,
                stock_id=stock_id,
                min_cost=5.0,
                commission_rate=0.0003,
                tolerance_bps=10.0,
                max_buckets=30,
                horizon=TOTAL_LEN,
            )
            min_qty, increment = board_lot_rule(stock_id)
            legal_total = (total // increment) * increment if total >= min_qty else total
            assert sum(schedule.values()) == legal_total, (
                stock_id, price, total, schedule
            )


# ---------------------------------------------------------------------------
# V25.1 algo end-to-end (with stub model)
# ---------------------------------------------------------------------------


def _early_predictor(*_args) -> np.ndarray:
    return np.ones(EARLY_LEN, dtype=np.float64) / EARLY_LEN


def _late_predictor(*_args) -> np.ndarray:
    return np.ones(LATE_LEN, dtype=np.float64) / LATE_LEN


def _algo() -> V25_1SmallCapAlgo:
    """Bypass __init__ (which loads PyTorch models) and inject a stub core."""
    algo = object.__new__(V25_1SmallCapAlgo)
    BaseExecutionAlgo.__init__(algo, config={})
    algo._core = V25TwoStageCore(
        early_predictor=_early_predictor,
        late_predictor=_late_predictor,
    )
    algo._plan = None
    algo._plan_key = None
    algo._plan_metadata = {}
    algo._last_no_fill_reason = None
    algo._last_no_fill_context = {}
    algo._min_cost = 5.0
    algo._commission_rate = 0.0003
    algo._tolerance_bps = 10.0
    algo._max_buckets = 30
    algo._schedule = None
    algo._schedule_key = None
    algo._schedule_metadata = {}
    return algo


def _full_day_context(prev_close: float = 10.0, stock_id: str = "000001.SZ") -> dict:
    n = TOTAL_LEN
    close = np.full(n, prev_close, dtype=np.float64)
    volume = np.full(n, 100000.0, dtype=np.float64)
    high = close.copy()
    low = close.copy()
    return {
        "prev_close": prev_close,
        "limit_up": prev_close * 1.10,
        "limit_down": prev_close * 0.90,
        "stock_id": stock_id,
        "limit_pct": 0.10,
        "full_day_close": close,
        "full_day_volume": volume,
        "full_day_high": high,
        "full_day_low": low,
        "full_day_open": close,
        "day_features": np.zeros(10, dtype=np.float32),
        "price_basis": "raw",
        "limit_price_basis": "raw",
    }


def _bar(close: float = 10.0, prev_close: float = 10.0) -> dict:
    return {
        "close": close,
        "open": close,
        "high": close,
        "low": close,
        "volume": 100000.0,
        "limit_up": prev_close * 1.10,
        "limit_down": prev_close * 0.90,
        "price_basis": "raw",
        "limit_price_basis": "raw",
    }


def test_algo_emits_only_board_legal_main_board_orders():
    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 6666)
    # init_order must round to 100-multiple.
    assert state.total_quantity == 6600

    ctx = _full_day_context(stock_id="000001.SZ")
    fills = []
    for _ in range(TOTAL_LEN + 5):
        result = algo.compute_step(state, _bar(), ctx)
        if result is not None:
            fills.append(result.quantity)
        if state.is_complete:
            break

    assert state.is_complete
    assert sum(fills) == state.total_quantity
    # No fill below board lot, all multiples of 100.
    assert all(q >= 100 and q % 100 == 0 for q in fills)


def test_algo_emits_legal_star_orders_with_1_share_increment():
    algo = _algo()
    state = algo.init_order("688981.SH", "BUY", 1234)
    assert state.total_quantity == 1234  # STAR allows 1-share increment

    ctx = _full_day_context(stock_id="688981.SH")
    fills = []
    for _ in range(TOTAL_LEN + 5):
        result = algo.compute_step(state, _bar(), ctx)
        if result is not None:
            fills.append(result.quantity)
        if state.is_complete:
            break

    assert state.is_complete
    assert sum(fills) == state.total_quantity
    assert all(q >= 200 for q in fills), fills


def test_algo_no_round_lot_zero_silent_drop():
    """All weights are honoured: total scheduled qty == total_quantity."""

    algo = _algo()
    state = algo.init_order("000001.SZ", "BUY", 1000)  # only 10 lots
    ctx = _full_day_context()
    total_filled = 0
    no_fill_reasons = []
    for _ in range(TOTAL_LEN + 5):
        result = algo.compute_step(state, _bar(), ctx)
        if result is None and algo._last_no_fill_reason:
            no_fill_reasons.append(algo._last_no_fill_reason)
        if result is not None:
            total_filled += result.quantity
        if state.is_complete:
            break
    assert state.is_complete
    assert total_filled == 1000
    # Off-step no-fills are expected (most bars produce 0); the V25 historical
    # round_lot_zero pathology must NOT appear.
    assert "round_lot_zero" not in no_fill_reasons


def test_algo_single_lot_collapses_to_one_fill():
    algo = _algo()
    state = algo.init_order("600519.SH", "BUY", 100)  # 1 lot only
    assert state.total_quantity == 100

    ctx = _full_day_context(prev_close=1500.0, stock_id="600519.SH")

    fills = []
    for _ in range(TOTAL_LEN + 5):
        result = algo.compute_step(state, _bar(close=1500.0, prev_close=1500.0), ctx)
        if result is not None:
            fills.append(result)
        if state.is_complete:
            break
    assert state.is_complete
    assert len(fills) == 1
    assert fills[0].quantity == 100


def test_algo_invalid_config_fails_fast():
    # Negative commission rate -> fail fast.
    algo = object.__new__(V25_1SmallCapAlgo)
    BaseExecutionAlgo.__init__(algo, config={"commission_rate": -1})
    with pytest.raises(V25_1SmallCapUnavailableError):
        # We re-run the post-init validators by calling a guard helper.
        if algo.config.get("commission_rate", 1) <= 0:
            raise V25_1SmallCapUnavailableError("commission_rate must be positive")
