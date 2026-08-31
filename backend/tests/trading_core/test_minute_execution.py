from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pytest

from backend.execution_algos.base_algo import BaseExecutionAlgo
from backend.execution_algos.twap_algo import TWAPAlgo
from backend.execution_algos.v24_plan_algo import V24PlanAlgo
from backend.execution_algos.v25_1_small_cap_algo import V25_1SmallCapAlgo
from backend.execution_algos.v25_two_stage_algo import V25TwoStageAlgo
from backend.services.trading_core.errors import DataUnavailableError, ExecutionAlgoError, UnsupportedFeatureError
from backend.services.trading_core.execution_algo_adapter import ExecutionAlgoAdapter
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderStatus
from backend.services.trading_core.oms import OMS
from backend.services.paper_trading_v2.models import OrderExecutionState


def make_order(
    quantity: int = 600,
    *,
    symbol: str = "000001.SZ",
    side: OrderSide = OrderSide.BUY,
):
    intent = OrderIntent(
        package_id="pkg_1",
        portfolio_id="paper_1",
        symbol=symbol,
        side=side,
        quantity=quantity,
        target_trade_date=date(2024, 1, 2),
    )
    return OMS().create_order(intent)


def make_bars(
    count: int = 3,
    volume: int = 10000,
    *,
    symbol: str = "000001.SZ",
) -> list[MinuteBar]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        MinuteBar(
            symbol=symbol,
            bar_time=start + timedelta(minutes=i),
            open=10.0 + i * 0.01,
            high=10.2 + i * 0.01,
            low=9.9 + i * 0.01,
            close=10.1 + i * 0.01,
            volume=volume,
            limit_up=11.0,
            limit_down=9.0,
        )
        for i in range(count)
    ]


def test_minute_execution_twap_fills_order() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=600)

    final_order, fills, events = engine.execute_order(
        order=order,
        minute_bars=make_bars(3),
        algo_code="TWAP",
        algo_config={"split_count": 3},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert sum(fill.quantity for fill in fills) == 600
    assert len(fills) == 3
    assert len(events) == 3


@pytest.mark.parametrize(
    ("algo_code", "algo_config", "market_context"),
    [
        ("TWAP", {"split_count": 6}, {}),
        ("VWAP", {}, {"volume_profile": [1.0] * 6}),
        ("AC_OPTIMAL", {"total_bars": 6}, {"sigma": 0.02}),
        ("SBB_EMA", {"split_count": 6}, {"close_series": [10.0] * 20}),
    ],
)
def test_sell_small_multi_slice_never_emits_intermediate_residual_child(
    algo_code: str,
    algo_config: dict,
    market_context: dict,
) -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=300, side=OrderSide.SELL)

    final_order, fills, _ = engine.execute_order(
        order=order,
        minute_bars=make_bars(6),
        algo_code=algo_code,
        algo_config=algo_config,
        market_context=market_context,
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert sum(fill.quantity for fill in fills) == 300
    assert all(fill.quantity >= 100 and fill.quantity % 100 == 0 for fill in fills)


def test_pov_sell_residual_is_only_emitted_when_child_closes_remaining_parent() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=300, side=OrderSide.SELL)
    bars = make_bars(6, volume=1_000)
    bars[-1] = bars[-1].model_copy(update={"volume": 10_000})

    final_order, fills, _ = engine.execute_order(
        order=order,
        minute_bars=bars,
        algo_code="POV",
        algo_config={"target_participation": 0.05, "max_participation": 0.2},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert [fill.quantity for fill in fills] == [300]


def test_twap_final_child_can_close_exact_subminimum_sell_residual() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=50, side=OrderSide.SELL)

    final_order, fills, _ = engine.execute_order(
        order=order,
        minute_bars=make_bars(6),
        algo_code="TWAP",
        algo_config={"split_count": 6},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert [fill.quantity for fill in fills] == [50]


def test_v24_v25_sell_residual_is_only_emitted_for_exact_remaining_parent() -> None:
    class FakeV24Executor:
        WARMUP = 0
        _current_plan = (1.0,)

        @staticmethod
        def decide(**_kwargs):
            return 1 / 6, 0.0

    v24 = object.__new__(V24PlanAlgo)
    BaseExecutionAlgo.__init__(v24, config={})
    v24._executor = FakeV24Executor()
    v24._initialized = True
    v24_context = {
        "stock_id": "000001.SZ",
        "prev_close": 10.0,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "full_day_close": np.asarray([10.1] * 31),
        "full_day_volume": np.asarray([10_000] * 31),
        "full_day_high": np.asarray([10.2] * 31),
        "full_day_low": np.asarray([9.8] * 31),
    }
    bar = {"close": 10.1}

    v24_nonfinal = v24.init_order("000001.SZ", "SELL", 300)
    assert v24.compute_step(v24_nonfinal, bar, v24_context) is None
    assert v24_nonfinal.executed_quantity == 0

    v24_final = v24.init_order("000001.SZ", "SELL", 50)
    v24_final.step = 30
    v24_result = v24.compute_step(v24_final, bar, v24_context)
    assert v24_result is not None
    assert v24_result.quantity == 50
    assert v24_final.is_complete is True

    plan = np.zeros(240, dtype=float)
    plan[:6] = 1 / 6

    def make_v25() -> V25TwoStageAlgo:
        algo = object.__new__(V25TwoStageAlgo)
        BaseExecutionAlgo.__init__(algo, config={})
        algo._plan = None
        algo._plan_key = None
        algo._plan_metadata = {}
        algo._last_no_fill_reason = None
        algo._last_no_fill_context = {}
        algo._generate_plan = lambda **_kwargs: plan.copy()
        return algo

    v25_context = {
        "stock_id": "000001.SZ",
        "price_basis": "raw",
        "limit_price_basis": "raw",
        "prev_close_basis": "raw",
        "prev_close": 10.0,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "full_day_open": [10.0],
        "full_day_close": [10.1],
        "full_day_volume": [10_000],
        "full_day_high": [10.2],
        "full_day_low": [9.8],
        "day_features": [0.1] * 10,
        "observed_only": True,
        "v25_realtime_streaming": True,
    }
    v25_bar = {
        "open": 10.0,
        "high": 10.2,
        "low": 9.8,
        "close": 10.1,
        "volume": 10_000,
        "limit_up": 11.0,
        "limit_down": 9.0,
    }

    v25 = make_v25()
    v25_nonfinal = v25.init_order("000001.SZ", "SELL", 300)
    assert v25.compute_step(v25_nonfinal, v25_bar, v25_context) is None
    assert v25_nonfinal.executed_quantity == 0

    v25_final_algo = make_v25()
    v25_final = v25_final_algo.init_order("000001.SZ", "SELL", 50)
    v25_final.step = 239
    v25_result = v25_final_algo.compute_step(v25_final, v25_bar, v25_context)
    assert v25_result is not None
    assert v25_result.quantity == 50
    assert v25_final.is_complete is True

    assert V25_1SmallCapAlgo._legalize_step_qty(50, 300, "000001.SZ", "SELL") == 0
    assert V25_1SmallCapAlgo._legalize_step_qty(50, 50, "000001.SZ", "SELL") == 50


def test_minute_execution_preserves_valid_star_board_lot_quantity() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=201, symbol="688001.SH")

    final_order, fills, events = engine.execute_order(
        order=order,
        minute_bars=make_bars(6, symbol="688001.SH"),
        algo_code="TWAP",
        algo_config={"split_count": 6},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert final_order.quantity == 201
    assert [fill.quantity for fill in fills] == [201]
    assert len(events) == 1


@pytest.mark.parametrize(
    ("algo_code", "config"),
    [
        ("TWAP", {"split_count": 3}),
        ("VWAP", {}),
        ("AC_OPTIMAL", {"total_bars": 3}),
        ("POV", {"target_participation": 0.05, "max_participation": 0.2}),
        ("SBB_EMA", {"split_count": 3}),
    ],
)
@pytest.mark.parametrize(("symbol", "quantity"), [("000001.SZ", 100), ("688001.SH", 201)])
def test_legacy_execution_algorithms_preserve_trading_core_board_lot_authority(
    algo_code: str,
    config: dict,
    symbol: str,
    quantity: int,
) -> None:
    order = make_order(quantity=quantity, symbol=symbol)
    _, state = ExecutionAlgoAdapter().create_state(order, algo_code, config)
    assert state.symbol == symbol
    assert state.total_quantity == quantity


def test_minute_execution_participation_limit_uses_star_increment() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=201, symbol="688001.SH")

    final_order, fills, _ = engine.execute_order(
        order=order,
        minute_bars=make_bars(1, volume=10_000, symbol="688001.SH"),
        algo_code="CLOSE_PRICE",
        algo_config={"max_participation_rate": 0.0201},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert [fill.quantity for fill in fills] == [201]


def test_minute_execution_requires_minute_bars() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(DataUnavailableError, match="minute bars are required"):
        engine.execute_order(
            order=make_order(),
            minute_bars=[],
            algo_code="TWAP",
            algo_config={"split_count": 3},
        )


def test_minute_execution_rejects_unsupported_algo() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(UnsupportedFeatureError, match="not registered"):
        engine.execute_order(
            order=make_order(),
            minute_bars=make_bars(),
            algo_code="UNKNOWN_ALGO",
            algo_config={},
        )


def test_minute_execution_rejects_unavailable_v24_plan_model() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(ExecutionAlgoError, match="V24_PLAN is unavailable"):
        engine.execute_order(
            order=make_order(),
            minute_bars=make_bars(31),
            algo_code="V24_PLAN",
            algo_config={"model_path": "missing/v24_plan_net.pt"},
            market_context={
                "prev_close": 10.0,
                "full_day_close": [10.0] * 31,
                "full_day_volume": [10000] * 31,
                "full_day_high": [10.2] * 31,
                "full_day_low": [9.8] * 31,
            },
        )


@pytest.mark.parametrize(
    "volume_profile",
    [None, [], [0, 0, 0], [1, -1, 1], [1, float("nan"), 1], [1, "bad", 1]],
)
def test_minute_execution_vwap_fails_loud_without_valid_authoritative_profile(
    volume_profile,
) -> None:
    engine = MinuteExecutionEngine()
    market_context = {}
    if volume_profile is not None:
        market_context["volume_profile"] = volume_profile
    with pytest.raises(ExecutionAlgoError) as exc_info:
        engine.execute_order(
            order=make_order(quantity=600),
            minute_bars=make_bars(3),
            algo_code="VWAP",
            market_context=market_context,
            allow_partial_fill=False,
        )
    assert exc_info.value.context["reason_code"] == "VWAP_VOLUME_PROFILE_INVALID"
    assert exc_info.value.context["algo_code"] == "VWAP"
    assert exc_info.value.context["order_id"]
    assert exc_info.value.context["symbol"] == "000001.SZ"


def test_minute_execution_vwap_uses_authoritative_profile_without_full_fill_fallback() -> None:
    engine = MinuteExecutionEngine()
    final_order, fills, _ = engine.execute_order(
        order=make_order(quantity=600),
        minute_bars=make_bars(3),
        algo_code="VWAP",
        market_context={"volume_profile": [1.0, 1.0, 1.0]},
        allow_partial_fill=False,
    )
    assert final_order.status == OrderStatus.FILLED
    assert [fill.quantity for fill in fills] == [200, 200, 200]
    assert [fill.reason for fill in fills] == [
        "VWAP step 1/3",
        "VWAP step 2/3",
        "VWAP step 3/3",
    ]


def test_minute_execution_fails_when_participation_rate_exceeded() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(ExecutionAlgoError, match="max_participation_rate"):
        engine.execute_order(
            order=make_order(quantity=600),
            minute_bars=make_bars(1, volume=1000),
            algo_code="CLOSE_PRICE",
            algo_config={"max_participation_rate": 0.1},
        )


def test_incremental_minute_execution_advances_cursor_without_duplicate_fills() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=600)
    state = OrderExecutionState(
        session_id="psess_1",
        run_id="prun_1",
        order_id=order.order_id,
        symbol=order.symbol,
        trade_date=date(2024, 1, 2),
        algo_code="TWAP",
        filled_quantity=0,
        remaining_quantity=order.quantity,
        status=order.status.value,
    )

    updated_order, updated_state, fills, events = engine.execute_order_incremental(
        order=order,
        execution_state=state,
        new_bars=make_bars(1),
        algo_code="TWAP",
        algo_config={"split_count": 3},
    )

    assert updated_order.status == OrderStatus.PARTIALLY_FILLED
    assert updated_state.last_processed_bar_time == make_bars(1)[0].bar_time
    assert sum(fill.quantity for fill in fills) == 200
    assert len(events) == 1

    replayed_order, replayed_state, replayed_fills, replayed_events = engine.execute_order_incremental(
        order=updated_order,
        execution_state=updated_state,
        new_bars=[],
        algo_code="TWAP",
        algo_config={"split_count": 3},
    )
    assert replayed_order == updated_order
    assert replayed_state == updated_state
    assert replayed_fills == []
    assert replayed_events == []


def test_incremental_twap_caps_each_step_to_authoritative_minute_volume_and_carries_residual() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=1900)
    state = OrderExecutionState(
        session_id="psess_twap_volume",
        run_id="prun_twap_volume",
        order_id=order.order_id,
        symbol=order.symbol,
        trade_date=date(2024, 1, 2),
        algo_code="TWAP",
        filled_quantity=0,
        remaining_quantity=order.quantity,
        status=order.status.value,
    )
    first_bar = make_bars(1, volume=1400)[0]

    partial_order, partial_state, first_fills, _ = engine.execute_order_incremental(
        order=order,
        execution_state=state,
        new_bars=[first_bar],
        algo_code="TWAP",
        algo_config={"split_count": 1},
    )

    assert partial_order.status == OrderStatus.PARTIALLY_FILLED
    assert [fill.quantity for fill in first_fills] == [1400]
    assert partial_state.filled_quantity == 1400
    assert partial_state.remaining_quantity == 500

    second_bar = first_bar.model_copy(
        update={"bar_time": first_bar.bar_time + timedelta(minutes=1), "volume": 1000}
    )
    final_order, final_state, second_fills, _ = engine.execute_order_incremental(
        order=partial_order,
        execution_state=partial_state,
        new_bars=[second_bar],
        algo_code="TWAP",
        algo_config={"split_count": 1},
    )

    assert final_order.status == OrderStatus.FILLED
    assert [fill.quantity for fill in second_fills] == [500]
    assert final_state.filled_quantity == 1900
    assert final_state.remaining_quantity == 0


@pytest.mark.parametrize("volume", [None, "bad", -1, 1.5, float("inf"), float("nan"), True])
def test_twap_fails_loud_for_invalid_minute_volume(volume) -> None:
    algo = TWAPAlgo(config={"split_count": 1})
    state = algo.init_order("000001.SZ", "BUY", 100)

    with pytest.raises(ValueError, match="finite non-negative integral minute volume"):
        algo.compute_step(state, {"close": 10.0, "volume": volume}, {})
