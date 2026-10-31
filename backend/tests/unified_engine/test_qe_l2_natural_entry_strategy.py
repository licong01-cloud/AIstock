"""Actual Qlib natural strategy orders, with deterministic synthetic market IO."""

# ruff: noqa: E402 -- worker import path matches execution bundle.
from pathlib import Path
import json
import sys
from types import ModuleType, SimpleNamespace
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_natural_entry_strategy as natural


def native_orders(original_source, adapted_source, log_root):
    from qlib.backtest.signal import Signal
    from qlib.backtest.utils import CommonInfrastructure, LevelInfrastructure
    from qe_suspend_filter_score_weighted_strategy import _SuspendFilterScoreWeightedMixin

    class FixtureSignal(Signal):
        def __init__(self, scores):
            self.scores = scores

        def get_signal(self, start_time, end_time):
            return self.scores

    class Calendar:
        def get_trade_step(self):
            return 0

        def get_step_time(self, step=None, shift=0):
            day = pd.Timestamp("2026-09-29" if shift else "2026-09-30")
            return day, day

        def get_freq(self):
            return "day"

    class Position:
        def __init__(self, held):
            self.held = held

        def get_stock_list(self):
            return list(self.held)

        def get_cash(self):
            return 100_000_000.0 - len(self.held) * 100_000.0

        def calculate_value(self):
            return 100_000_000.0

        def get_stock_amount(self, sid):
            return 10_000.0 if sid in self.held else 0.0

        def get_stock_count(self, sid, bar=None):
            return 5

    class Exchange:
        trade_w_adj_price = False
        trade_unit = 100.0

        def get_deal_price(self, **kwargs):
            return 10.0

        def get_factor(self, **kwargs):
            return 1.0

        def get_amount_of_trade_unit(self, **kwargs):
            return 100.0

        def get_close(self, *args, **kwargs):
            return 10.0

        def is_stock_tradable(self, **kwargs):
            return True

    rows = {
        str(i): {
            "sector_code": str(i),
            "trade_date": "2026-09-30",
            "as_of_date": "2026-09-29",
            "availability": "available",
            "reason_code": None,
            "expected_loss": i / 130,
        }
        for i in range(131)
    }

    def module(source):
        mod = ModuleType("fixture_strategy")
        exec(compile(source, "<reviewed-strategy>", "exec"), mod.__dict__)
        return mod.ScoreWeightedTopkStrategyV2

    original, adapted = module(original_source), module(adapted_source)

    class OriginalWrapped(_SuspendFilterScoreWeightedMixin, original):
        pass

    class AdaptedWrapped(_SuspendFilterScoreWeightedMixin, adapted):
        pass

    class Enabled(natural.NaturalEntryMixin, AdaptedWrapped):
        pass

    stocks = [f"{i:06d}.SZ" for i in range(1, 56)]
    score_values = [1.0 - (i // 2) / 100 for i in range(55)]  # deliberate score ties
    cases = [
        ([], False, False),
        ([stocks[0], stocks[30], "ghost"], False, False),
        ([stocks[0]], True, False),
        ([stocks[0]], True, True),
    ]
    receipts = []
    for index, (held, empty, force_exit) in enumerate(cases):
        scores = pd.Series([] if empty else score_values, index=[] if empty else stocks, dtype=float)
        kwargs = dict(
            signal=FixtureSignal(scores),
            topk=50,
            n_drop=5,
            max_n_drop=5,
            weight_method="softmax",
            max_weight=0.05,
            min_weight=0.0,
            max_position_ratio=0.95,
            common_infra=CommonInfrastructure(
                trade_exchange=Exchange(), trade_account=SimpleNamespace(current_position=Position(held))
            ),
            level_infra=LevelInfrastructure(trade_calendar=Calendar()),
        )
        policy = SimpleNamespace(
            filter_scores=lambda scores, day: scores,
            force_exit_symbols=lambda stocks, day: set(stocks) if force_exit else set(),
        )
        base = OriginalWrapped(**kwargs)
        base._qe_risk_policy = policy
        expected = base.generate_trade_decision().get_decision()
        expected = [(o.stock_id, int(o.direction), o.amount) for o in expected]
        outcomes = {}
        for arm in ("B0", "R-entry"):
            inputs = SimpleNamespace(
                manifest={
                    "arm": arm,
                    "seed": 42,
                    "execution_files_sha256": {"fixture": "only"},
                    "execution_sha256": "fixture",
                },
                as_of={"2026-09-30": "2026-09-29"},
                risk_context=lambda day, buys: (
                    None
                    if arm == "B0"
                    else {"sector_rows": rows, "stock_sectors": {s: str(stocks.index(s) * 2) for s in buys}}
                ),
            )
            real_reader = natural.NaturalEntryInputs
            natural.NaturalEntryInputs = lambda path: inputs
            try:
                log = str(Path(log_root) / f"case{index}_{arm}.jsonl")
                obj = Enabled(
                    **kwargs,
                    qe_l2_natural_entry_manifest_file="explicit-synthetic-fixture",
                    qe_l2_natural_entry_action_log=log,
                )
            finally:
                natural.NaturalEntryInputs = real_reader
            obj._qe_risk_policy = policy
            orders = obj.generate_trade_decision().get_decision()
            observed = [(o.stock_id, int(o.direction), o.amount) for o in orders]
            audit = json.loads(Path(log).read_text(encoding="utf-8"))
            assert not (set(audit["natural_new_buys"]) & set(held))
            assert obj.trade_position.get_stock_list() == held
            if arm == "B0":
                assert observed == expected and audit["risk_inputs"] == {}
                assert obj._backup_candidates == base._backup_candidates
            else:
                assert len(audit["risk_inputs"]) == 131
                assert [(s, d) for s, d, a in observed] == [(s, d) for s, d, a in expected]
                assert [(s, d, a) for s, d, a in observed if s in held] == [
                    (s, d, a) for s, d, a in expected if s in held
                ]
                if not empty:
                    assert observed != expected
            outcomes[arm] = {"orders": observed, "groups": audit["groups"]}
        receipts.append(outcomes)
    return receipts


def test_real_qlib_b0_identity_existing_holdings_and_terminal_day(tmp_path, monkeypatch):
    import pytest

    pytest.importorskip("qlib")
    from backend.services.quantevolver.l2_natural_entry_consumer import bind_natural_entry_hook

    source = (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")
    adapted, _ = bind_natural_entry_hook(source)
    monkeypatch.chdir(tmp_path)
    assert len(native_orders(source, adapted, ".")) == 4
