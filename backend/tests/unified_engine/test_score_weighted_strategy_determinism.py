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

    ranked = pd.Series(
        [0.8, 0.8, 0.7, 0.6],
        index=["603190.SH", "301133.SZ", "000001.SZ", "000002.SZ"],
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
            return ["000001.SZ"]

        def get_cash(self):
            return 10_000_000.0

        def get_stock_amount(self, stock_id):
            return 100_000.0 if stock_id == "000001.SZ" else 0.0

    class Exchange:
        trade_w_adj_price = False
        trade_unit = 100.0

        def get_deal_price(self, **kwargs):
            return 10.0

        def get_factor(self, **kwargs):
            return 1.0

        def get_amount_of_trade_unit(self, **kwargs):
            return 100.0

    strategy = ScoreWeightedTopkStrategyV2(
        signal=Signal(),
        topk=3,
        n_drop=1,
        max_n_drop=1,
        weight_method="equal",
    )
    strategy.trade_calendar = Calendar()
    strategy.trade_position = Position()
    strategy.trade_exchange = Exchange()
    decision = strategy.generate_trade_decision()
    print(json.dumps([order.stock_id for order in decision.order_list]))
    """
)


def _run_with_hash_seed(seed: int) -> dict[str, object]:
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = str(seed)
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
    assert receipts[0] == ["603190.SH", "301133.SZ"]
