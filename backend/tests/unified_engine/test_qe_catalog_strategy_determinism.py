"""Exercise the catalog -> Composer -> real wrapper path in fresh processes."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import register_score_weighted_strategy_v2 as legacy_registration
from scripts import register_score_weighted_strategy_v2_deterministic_v1 as registration


ROOT = Path(__file__).resolve().parents[3]
LEGACY = Path(__file__).with_name("fixtures") / "qe_score_weighted_topk_v2_20f1d21b.py"


def parent_row():
    return {
        "strategy_id": registration.PARENT_ID,
        "source_code": LEGACY.read_text(encoding="utf-8"),
        "scenario": "qlib_factor", "strategy_type": "daily",
        "portfolio_config": deepcopy(legacy_registration.PORTFOLIO_CONFIG),
        "default_kwargs": deepcopy(legacy_registration.DEFAULT_KWARGS),
        "param_schema": deepcopy(legacy_registration.PARAM_SCHEMA),
        "python_implementation": {"module_path": legacy_registration.MODULE_PATH,
                                  "class_name": registration.CLASS_NAME},
        "in_selection_center": True,
    }


def compose(tmp_path, *, legacy=False):
    from backend.services.quantevolver.config_composer import ConfigComposer

    row = parent_row() if legacy else registration.build_payload(parent_row())
    source, dependencies = ConfigComposer()._build_strategy_py_content(row)
    (tmp_path / "custom_strategy.py").write_text(source, encoding="utf-8")
    for name, body in dependencies.items():
        (tmp_path / name).write_text(body, encoding="utf-8")
    # This is the wrapper selected by the formal Composer for the unchanged
    # class name; its imports must consume the catalog-generated custom module.
    name = "qe_suspend_filter_score_weighted_strategy.py"
    (tmp_path / name).write_text((ROOT / "scripts" / name).read_text(encoding="utf-8"), encoding="utf-8")
    return source


CHILD = r'''
import hashlib, inspect, json, os, sys, types
from pathlib import Path
import pandas as pd

# Qlib exchange plumbing is isolated here. The strategy, wrapper, parent
# weights and Composer output under test are the real shipped Python source.
for name in ['qlib','qlib.contrib','qlib.contrib.strategy',
             'qlib.contrib.strategy.signal_strategy','qlib.backtest','qlib.backtest.decision']:
    sys.modules[name] = types.ModuleType(name)
class TopkDropoutStrategy:
    def __init__(self, signal=None, topk=50, n_drop=5, **kwargs):
        self.signal, self.topk, self.n_drop = signal, topk, n_drop
        for k,v in kwargs.items(): setattr(self,k,v)
class Order:
    def __init__(self, stock_id, amount, direction, start_time, end_time):
        self.stock_id, self.amount, self.direction = stock_id, amount, direction
class TradeDecisionWO:
    def __init__(self, order_list, strategy): self.order_list = order_list
    def get_decision(self): return self.order_list
sys.modules['qlib.contrib.strategy.signal_strategy'].TopkDropoutStrategy = TopkDropoutStrategy
d = sys.modules['qlib.backtest.decision']
d.Order, d.TradeDecisionWO, d.OrderDir = Order, TradeDecisionWO, types.SimpleNamespace(BUY=1,SELL=0)
sys.path.insert(0, os.getcwd())
from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2 as Strategy
base = next(t for t in Strategy.__mro__ if t.__name__=='ScoreWeightedTopkStrategyV2')
assert base.__module__ == 'custom_strategy'
assert Path(inspect.getfile(base)).resolve() == Path('custom_strategy.py').resolve()
case = json.loads(os.environ['CATALOG_CASE'])
scores = pd.Series([case['scores'][s] for s in case['order']],index=case['order'],dtype=float)
class Signal:
    def get_signal(self, **kwargs): return scores
class Calendar:
    def get_trade_step(self): return 0
    def get_step_time(self, step, shift=0): return pd.Timestamp('2026-07-03'),pd.Timestamp('2026-07-03')
    def get_freq(self): return 'day'
class Position:
    def get_stock_list(self): return case['holdings']
    def get_cash(self): return 10_000_000.
    def get_stock_amount(self, sid): return 10_000. if sid in case['holdings'] else 0.
    def get_stock_count(self, sid, bar=None): return 5
class Exchange:
    trade_w_adj_price=False
    trade_unit=100.
    def get_deal_price(self, **kwargs): return 10.
    def get_factor(self, **kwargs): return 1.
    def get_amount_of_trade_unit(self, **kwargs): return 100.
    def get_close(self, *args, **kwargs): return 10.
    def is_stock_tradable(self, **kwargs): return True
strategy = Strategy(signal=Signal(),topk=case['topk'],n_drop=1,max_n_drop=1,
    weight_method=case.get('weight_method','softmax'),max_weight=1.,min_weight=0.,max_position_ratio=1.)
strategy.trade_calendar, strategy.trade_position, strategy.trade_exchange = Calendar(),Position(),Exchange()
decision = strategy.generate_trade_decision()
print(json.dumps({'orders':[(o.stock_id,o.direction,o.amount) for o in decision.get_decision()],
                  'backup':strategy._backup_candidates},sort_keys=True))
'''


def run_child(directory, case, seed):
    env = dict(os.environ, PYTHONHASHSEED=str(seed), CATALOG_CASE=json.dumps(case),
               PYTHONPATH=str(ROOT / "scripts") + os.pathsep + os.environ.get("PYTHONPATH", ""))
    result = subprocess.run([sys.executable, "-B", "-c", CHILD], cwd=directory,
                            env=env, capture_output=True, text=True, check=True, timeout=45)
    return json.loads(result.stdout)


@pytest.mark.parametrize("weight_method", ["softmax", "rank"])
def test_real_composed_consumer_is_stable_across_hash_input_and_holding_order(tmp_path, weight_method):
    source = compose(tmp_path)
    assert source == registration.SOURCE_FILE.read_text(encoding="utf-8")
    scores = {"603190.SH": .8, "301133.SZ": .8, "000001.SZ": .7, "000002.SZ": .7}
    cases = [
        {"scores": scores, "order": order, "holdings": holdings, "topk": 2, "weight_method": weight_method}
        for order, holdings in [
            (list(scores), ["000001.SZ", "000002.SZ"]),
            (list(reversed(scores)), ["000002.SZ", "000001.SZ"]),
            (["000002.SZ", "301133.SZ", "000001.SZ", "603190.SH"], ["000001.SZ", "000002.SZ"]),
            (["301133.SZ", "000001.SZ", "603190.SH", "000002.SZ"], ["000002.SZ", "000001.SZ"]),
        ]
    ]
    results = [run_child(tmp_path, case, seed) for case, seed in zip(cases, (0, 1, 42, 2024))]
    assert results == [results[0]] * 4
    assert [order[0] for order in results[0]["orders"]] == ["000001.SZ", "301133.SZ"]


def test_topk_boundary_and_backup_exact_ties_have_instrument_secondary_key(tmp_path):
    compose(tmp_path)
    scores = {"603190.SH": .8, "301133.SZ": .8, "000001.SZ": .7, "000002.SZ": .6}
    results = [run_child(tmp_path, {"scores":scores, "order":order, "holdings":[], "topk":1}, seed)
               for order,seed in [(list(scores),0),(list(reversed(scores)),2024)]]
    assert results[0] == results[1]
    assert results[0]["orders"][0][0] == "301133.SZ"
    assert results[0]["backup"][0][0] == "603190.SH"


@pytest.mark.parametrize("weight_method", ["softmax", "linear", "equal", "rank"])
def test_distinct_scores_keep_legacy_orders_and_sizing(tmp_path, weight_method):
    original, fixed = tmp_path / "original", tmp_path / "fixed"
    original.mkdir()
    fixed.mkdir()
    compose(original, legacy=True)
    compose(fixed)
    scores = {"603190.SH": .9, "301133.SZ": .8, "000001.SZ": .7, "000002.SZ": .6}
    case = {"scores":scores, "order":list(reversed(scores)), "holdings":["000001.SZ"],
            "topk":3, "weight_method":weight_method}
    assert run_child(original, case, 1) == run_child(fixed, case, 2024)


def test_near_ties_are_not_rounded_into_exact_ties(tmp_path):
    compose(tmp_path)
    scores = {"603190.SH": .8 + 2e-16, "301133.SZ": .8}
    result = run_child(tmp_path, {"scores":scores, "order":list(scores), "holdings":[], "topk":1}, 42)
    assert result["orders"][0][0] == "603190.SH"


def test_catalog_successor_preserves_parent_and_defaults():
    parent = parent_row()
    before = deepcopy(parent)
    payload = registration.build_payload(parent)
    assert parent == before
    assert hashlib.sha256(parent["source_code"].encode()).hexdigest() == registration.PARENT_SOURCE_SHA256
    assert payload["strategy_id"] != parent["strategy_id"]
    assert payload["default_kwargs"] == parent["default_kwargs"]
    assert payload["param_schema"] == parent["param_schema"]
    assert payload["portfolio_config"]["kwargs"] == parent["portfolio_config"]["kwargs"]
    assert payload["portfolio_config"]["class"] == parent["portfolio_config"]["class"]


@pytest.mark.parametrize("field", ["source_code", "default_kwargs", "param_schema"])
def test_drifted_parent_is_rejected(field):
    parent = parent_row()
    parent[field] = parent[field] + "\n" if field == "source_code" else {}
    with pytest.raises(ValueError, match="PARENT_.*DRIFT"):
        registration.build_payload(parent)


def test_production_requires_matching_committed_dev_evidence():
    payload = registration.build_payload(parent_row())
    valid = {"schema_version":"qe_strategy_catalog_registration_v1", "target":"dev",
             "database":"aistock_dev", "committed":True, "readback_matches":True,
             "payload_sha256":registration.digest(payload), "strategy_id":registration.STRATEGY_ID,
             "legacy_unchanged":True}
    registration.validate_dev_receipt(valid, payload)
    for change in ({"target":"prod"}, {"committed":False}, {"payload_sha256":"drift"},
                   {"readback_matches":False}, {"legacy_unchanged":False}):
        with pytest.raises(ValueError, match="MATCHING_COMMITTED_DEV_RECEIPT_REQUIRED"):
            registration.validate_dev_receipt({**valid, **change}, payload)


def test_conflicting_successor_cannot_be_overwritten():
    payload = registration.build_payload(parent_row())
    conflicting = {**payload, "source_code": payload["source_code"] + "\n# changed"}

    class Cursor:
        def __init__(self):
            self.results = iter([{"database": "aistock_dev"}, None, conflicting])
            self.commands = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, command, values=None):
            self.commands.append(command)

        def fetchone(self):
            return next(self.results)

    class Connection:
        rolled_back = False

        def __init__(self):
            self.cursor_instance = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, *args):
            self.rolled_back = exc_type is not None

        def set_session(self, **kwargs):
            pass

        def cursor(self, **kwargs):
            return self.cursor_instance

    conn = Connection()
    with pytest.raises(ValueError, match="EXISTING_SUCCESSOR_DRIFT_REFUSE_OVERWRITE"):
        registration.apply_catalog(conn, target="dev", payload=payload, execute=True)
    assert conn.rolled_back
    assert all(not command.startswith(("INSERT", "UPDATE", "DELETE"))
               for command in conn.cursor_instance.commands)


def test_registration_rejects_another_row_before_database_access():
    payload = registration.build_payload(parent_row())
    with pytest.raises(ValueError, match="EXACT_TARGET_AND_SUCCESSOR_ID_REQUIRED"):
        registration.apply_catalog(None, target="dev", payload={**payload, "strategy_id":registration.PARENT_ID}, execute=True)
    with pytest.raises(ValueError, match="EXACT_TARGET_AND_SUCCESSOR_ID_REQUIRED"):
        registration.apply_catalog(None, target="unknown", payload=payload, execute=True)
