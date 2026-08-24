from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


_CHILD_PROGRAM = textwrap.dedent(
    """
    import json
    import sys
    import types

    import pandas as pd

    qlib = types.ModuleType("qlib")
    qlib_contrib = types.ModuleType("qlib.contrib")
    qlib_contrib_strategy = types.ModuleType("qlib.contrib.strategy")
    qlib_signal_strategy = types.ModuleType("qlib.contrib.strategy.signal_strategy")
    qlib_backtest = types.ModuleType("qlib.backtest")
    qlib_decision = types.ModuleType("qlib.backtest.decision")

    class TopkDropoutStrategy:
        def __init__(self, signal=None, topk=50, n_drop=5, **kwargs):
            self.signal = signal
            self.topk = topk
            self.n_drop = n_drop

    class Order:
        def __init__(self, stock_id, amount, direction, start_time, end_time):
            self.stock_id = stock_id
            self.amount = amount
            self.direction = direction
            self.start_time = start_time
            self.end_time = end_time

    class OrderDir:
        BUY = 1
        SELL = 0

    class TradeDecisionWO:
        def __init__(self, order_list, strategy):
            self.order_list = order_list
            self.strategy = strategy

    qlib_signal_strategy.TopkDropoutStrategy = TopkDropoutStrategy
    qlib_decision.Order = Order
    qlib_decision.OrderDir = OrderDir
    qlib_decision.TradeDecisionWO = TradeDecisionWO
    for name, module in {
        "qlib": qlib,
        "qlib.contrib": qlib_contrib,
        "qlib.contrib.strategy": qlib_contrib_strategy,
        "qlib.contrib.strategy.signal_strategy": qlib_signal_strategy,
        "qlib.backtest": qlib_backtest,
        "qlib.backtest.decision": qlib_decision,
    }.items():
        sys.modules[name] = module

    from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2

    import os

    score_by_instrument = json.loads(os.environ["SCORE_BY_INSTRUMENT"])
    instrument_order = json.loads(os.environ["SCORE_INSTRUMENT_ORDER"])
    ranked = pd.Series(
        [score_by_instrument[instrument] for instrument in instrument_order],
        index=instrument_order,
        dtype=float,
    )
    class Signal:
        def get_signal(self, start_time=None, end_time=None):
            return ranked

    class Calendar:
        def get_trade_step(self):
            return 0

        def get_step_time(self, trade_step, shift=0):
            return pd.Timestamp("2024-11-07"), pd.Timestamp("2024-11-07")

        def get_freq(self):
            return "day"

    class Position:
        def get_stock_list(self):
            return json.loads(os.environ["CURRENT_HOLDINGS"])

        def get_cash(self):
            return 10_000_000.0

        def get_stock_amount(self, stock_id):
            return 100_000.0 if stock_id in self.get_stock_list() else 0.0

    class Exchange:
        trade_w_adj_price = False
        trade_unit = 100.0

        def get_deal_price(self, **kwargs):
            return 10.0

        def get_factor(self, **kwargs):
            return 1.0

        def get_amount_of_trade_unit(self, **kwargs):
            return 100.0

    class RecordingStrategy(ScoreWeightedTopkStrategyV2):
        def _adjust_target_weight_map(self, weight_map, trade_start_time):
            self.captured_weight_map = dict(weight_map)
            return super()._adjust_target_weight_map(weight_map, trade_start_time)

    strategy_class = (
        RecordingStrategy
        if os.environ["CAPTURE_WEIGHT_MAP"] == "1"
        else ScoreWeightedTopkStrategyV2
    )
    strategy = strategy_class(
        signal=Signal(),
        topk=int(os.environ["TOPK"]),
        n_drop=1,
        max_n_drop=1,
        weight_method=os.environ["WEIGHT_METHOD"],
        max_weight=1.0,
        min_weight=0.0,
        max_position_ratio=1.0,
    )
    strategy.trade_calendar = Calendar()
    strategy.trade_position = Position()
    strategy.trade_exchange = Exchange()
    decision = strategy.generate_trade_decision()
    payload = [order.stock_id for order in decision.order_list]
    if os.environ["CAPTURE_WEIGHT_MAP"] == "1":
        payload = {
            "orders": payload,
            "weight_map": strategy.captured_weight_map,
        }
    print(json.dumps(payload, sort_keys=True))
    """
)


def _run_with_hash_seed(
    seed: int,
    *,
    instrument_order: list[str] | None = None,
    current_holdings: list[str] | None = None,
    topk: int = 3,
    score_by_instrument: dict[str, float] | None = None,
    weight_method: str = "equal",
    capture_weight_map: bool = False,
) -> dict[str, object]:
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = str(seed)
    env["SCORE_INSTRUMENT_ORDER"] = json.dumps(
        instrument_order
        or ["603190.SH", "301133.SZ", "000001.SZ", "000002.SZ"]
    )
    env["CURRENT_HOLDINGS"] = json.dumps(current_holdings or ["000001.SZ"])
    env["TOPK"] = str(topk)
    env["WEIGHT_METHOD"] = weight_method
    env["CAPTURE_WEIGHT_MAP"] = "1" if capture_weight_map else "0"
    env["SCORE_BY_INSTRUMENT"] = json.dumps(
        score_by_instrument
        or {
            "603190.SH": 0.8,
            "301133.SZ": 0.8,
            "000001.SZ": 0.7,
            "000002.SZ": 0.6,
        }
    )
    scripts_path = str(PROJECT_ROOT / "scripts")
    env["PYTHONPATH"] = scripts_path + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD_PROGRAM],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return json.loads(completed.stdout)


def test_equal_score_topk_buy_order_is_stable_across_fresh_processes() -> None:
    receipts = [_run_with_hash_seed(seed) for seed in (1, 2, 17, 101)]

    assert receipts == [receipts[0]] * len(receipts)
    assert receipts[0] == ["301133.SZ", "603190.SH"]


def test_equal_score_topk_boundary_is_stable_across_input_order_permutations() -> None:
    permutations = [
        ["603190.SH", "301133.SZ", "000001.SZ", "000002.SZ"],
        ["301133.SZ", "603190.SH", "000001.SZ", "000002.SZ"],
        ["000002.SZ", "603190.SH", "000001.SZ", "301133.SZ"],
        ["000001.SZ", "301133.SZ", "000002.SZ", "603190.SH"],
    ]

    receipts = [
        _run_with_hash_seed(
            seed,
            instrument_order=instrument_order,
            current_holdings=["000001.SZ"],
            topk=1,
        )
        for seed, instrument_order in zip((1, 2, 17, 101), permutations)
    ]

    assert receipts == [receipts[0]] * len(receipts)
    assert receipts[0] == ["000001.SZ", "301133.SZ"]


def test_equal_score_sell_boundary_is_stable_across_holding_order() -> None:
    scores = {
        "603190.SH": 0.8,
        "301133.SZ": 0.8,
        "000001.SZ": 0.7,
        "000002.SZ": 0.7,
    }
    receipts = [
        _run_with_hash_seed(
            seed,
            current_holdings=holdings,
            topk=2,
            score_by_instrument=scores,
        )
        for seed, holdings in (
            (1, ["000001.SZ", "000002.SZ"]),
            (2, ["000002.SZ", "000001.SZ"]),
        )
    ]

    assert receipts == [receipts[0]] * len(receipts)
    assert receipts[0] == ["000001.SZ", "301133.SZ"]


def test_canonical_tie_break_does_not_change_distinct_score_order() -> None:
    scores = {
        "603190.SH": 0.9,
        "301133.SZ": 0.8,
        "000001.SZ": 0.7,
        "000002.SZ": 0.6,
    }
    receipts = [
        _run_with_hash_seed(
            seed,
            instrument_order=instrument_order,
            score_by_instrument=scores,
        )
        for seed, instrument_order in (
            (1, ["000002.SZ", "000001.SZ", "301133.SZ", "603190.SH"]),
            (2, ["603190.SH", "301133.SZ", "000001.SZ", "000002.SZ"]),
        )
    ]

    assert receipts == [["603190.SH", "301133.SZ"]] * len(receipts)


def test_rank_weight_map_is_stable_across_tied_holding_order() -> None:
    scores = {
        "603190.SH": 0.8,
        "301133.SZ": 0.8,
        "000001.SZ": 0.7,
        "000002.SZ": 0.7,
    }
    receipts = [
        _run_with_hash_seed(
            seed,
            current_holdings=holdings,
            topk=4,
            score_by_instrument=scores,
            weight_method="rank",
            capture_weight_map=True,
        )
        for seed, holdings in (
            (1, ["000001.SZ", "000002.SZ"]),
            (2, ["000002.SZ", "000001.SZ"]),
        )
    ]

    assert receipts == [receipts[0]] * len(receipts)
