from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pandas as pd

from backend.services.quantevolver.sector_risk_overlay import canonical_json_sha256


ROOT = Path(__file__).resolve().parents[3]


def _load_strategy_module(monkeypatch):
    decision = types.ModuleType("qlib.backtest.decision")

    class OrderDir:
        BUY = 1
        SELL = 0

    class Order:
        def __init__(self, stock_id, amount, direction, start_time, end_time):
            self.stock_id = stock_id
            self.amount = amount
            self.direction = direction
            self.start_time = start_time
            self.end_time = end_time

    decision.Order = Order
    decision.OrderDir = OrderDir
    monkeypatch.setitem(sys.modules, "qlib", types.ModuleType("qlib"))
    monkeypatch.setitem(sys.modules, "qlib.backtest", types.ModuleType("qlib.backtest"))
    monkeypatch.setitem(sys.modules, "qlib.backtest.decision", decision)

    runtime_spec = importlib.util.spec_from_file_location(
        "qe_sector_risk_overlay", ROOT / "scripts" / "qe_sector_risk_overlay.py"
    )
    runtime = importlib.util.module_from_spec(runtime_spec)
    runtime_spec.loader.exec_module(runtime)
    monkeypatch.setitem(sys.modules, "qe_sector_risk_overlay", runtime)

    dependency = types.ModuleType("qe_suspend_filter_score_weighted_strategy")

    class SuspendMixin:
        pass

    class ScoreV2:
        pass

    class CapacityV1:
        pass

    dependency._SuspendFilterScoreWeightedMixin = SuspendMixin
    dependency.ScoreWeightedTopkStrategyV2 = ScoreV2
    dependency.ScoreWeightedTopkStrategyV2CapacityV1 = CapacityV1
    monkeypatch.setitem(sys.modules, "qe_suspend_filter_score_weighted_strategy", dependency)

    spec = importlib.util.spec_from_file_location(
        "qe_sector_risk_overlay_strategy_under_test",
        ROOT / "scripts" / "qe_sector_risk_overlay_strategy.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, OrderDir


def _artifact(tmp_path, state="HIGH"):
    data_path = tmp_path / "runtime.parquet"
    frame = pd.DataFrame(
        [
            {
                "signal_date": pd.Timestamp("2026-01-04"),
                "effective_trade_date": pd.Timestamp("2026-01-05"),
                "instrument": "000001.SZ",
                "l2_code_id": 1,
                "risk_score": 0.85,
                "risk_state": state,
                "rs_turn_risk": 0.8,
                "breadth_deterioration": 0.8,
                "flow_divergence_risk": 0.8,
                "leadership_concentration": 0.8,
                "vol_crowding_risk": 0.8,
            }
        ]
    )
    frame.to_parquet(data_path, index=False)
    manifest = {
        "schema_version": "qe_sector_risk_overlay_manifest_v1",
        "dataset_identity": "fixture-v1",
        "artifacts": {
            "runtime": {"sha256": hashlib.sha256(data_path.read_bytes()).hexdigest()}
        },
    }
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, data_path


def test_entry_gate_uses_effective_trade_date_not_prediction_date(tmp_path, monkeypatch) -> None:
    module, _ = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path)

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = types.SimpleNamespace(get_stock_list=lambda: [])
            self._qe_suspend_filter_trade_time = pd.Timestamp("2026-01-05")

        def _normalize_signal_scores(self, scores, pred_end_time):
            return scores

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / "actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="entry_gate",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
    )
    scores = strategy._normalize_signal_scores(
        pd.Series([1.0], index=["000001.SZ"]),
        pd.Timestamp("2026-01-04"),
    )

    assert scores.empty
    event = json.loads(action_log.read_text(encoding="utf-8").strip())
    assert event["trade_date"] == "2026-01-05"
    assert event["action_type"] == "ENTRY_BLOCK"


def test_bounded_de_risk_emits_factor_and_lot_aware_partial_sell(tmp_path, monkeypatch) -> None:
    module, order_dir = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path)

    class Position:
        def get_cash(self):
            return 0.0

        def get_stock_amount(self, instrument):
            return 1000.0

    class Exchange:
        trade_w_adj_price = False
        trade_unit = 100

        def get_amount_of_trade_unit(self, **kwargs):
            return 100.0

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = Position()
            self.trade_exchange = Exchange()
            self.lot_size = 100
            self._qe_suspend_filter_trade_time = pd.Timestamp("2026-01-05")

        def _adjust_target_weight_map(self, weights, trade_start_time):
            return dict(weights)

        def _build_additional_rebalance_orders(self, **kwargs):
            return []

        def _get_current_price(self, instrument, trade_step, direction):
            return 10.0

        def _get_current_factor(self, instrument, trade_step):
            return 1.0

        def _shares_to_adjusted_amount(self, shares, factor):
            return float(shares) / float(factor)

        def _is_orderable_without_warning(self, *args, **kwargs):
            return True

        def _can_sell_under_hold_thresh(self, instrument, trade_start_time):
            return False

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / "actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="bounded_de_risk",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
        sector_risk_overlay_override_hold_thresh=True,
    )
    adjusted = strategy._adjust_target_weight_map(
        {"000001.SZ": 0.5}, pd.Timestamp("2026-01-05")
    )
    orders = strategy._build_additional_rebalance_orders(
        weight_map=adjusted,
        current_holdings=["000001.SZ"],
        existing_sell_ids=set(),
        planned_buy_orders=[],
        total_account_value=10_000.0,
        trade_step=0,
        trade_start_time=pd.Timestamp("2026-01-05"),
        trade_end_time=pd.Timestamp("2026-01-05"),
    )

    assert adjusted == {"000001.SZ": 0.25}
    assert len(orders) == 1
    assert orders[0].direction == order_dir.SELL
    assert orders[0].amount == 700.0
    event = json.loads(action_log.read_text(encoding="utf-8").strip())
    assert event["action_type"] == "DE_RISK_SELL"
    assert event["hold_thresh_overridden"] is True
    assert event["order_generated"] is True

    blocked_log = tmp_path / "blocked_actions.jsonl"
    blocked = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="bounded_de_risk",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=blocked_log,
        sector_risk_overlay_override_hold_thresh=False,
    )
    blocked_weights = blocked._adjust_target_weight_map(
        {"000001.SZ": 0.5}, pd.Timestamp("2026-01-05")
    )
    blocked_orders = blocked._build_additional_rebalance_orders(
        weight_map=blocked_weights,
        current_holdings=["000001.SZ"],
        existing_sell_ids=set(),
        planned_buy_orders=[],
        total_account_value=10_000.0,
        trade_step=0,
        trade_start_time=pd.Timestamp("2026-01-05"),
        trade_end_time=pd.Timestamp("2026-01-05"),
    )
    assert blocked_orders == []
    blocked_event = json.loads(blocked_log.read_text(encoding="utf-8").strip())
    assert blocked_event["action_type"] == "DE_RISK_BLOCKED_BY_HOLD"
    assert blocked_event["order_generated"] is False


def test_exit_reentry_critical_state_emits_full_exit(tmp_path, monkeypatch) -> None:
    module, order_dir = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path, state="CRITICAL")

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = types.SimpleNamespace(
                get_cash=lambda: 0.0,
                get_stock_amount=lambda instrument: 1000.0,
            )
            self.trade_exchange = types.SimpleNamespace(
                trade_w_adj_price=False,
                trade_unit=100,
                get_amount_of_trade_unit=lambda **kwargs: 100.0,
            )
            self.lot_size = 100
            self._qe_suspend_filter_trade_time = pd.Timestamp("2026-01-05")

        def _adjust_target_weight_map(self, weights, trade_start_time):
            return dict(weights)

        def _build_additional_rebalance_orders(self, **kwargs):
            return []

        def _get_current_price(self, *args):
            return 10.0

        def _get_current_factor(self, *args):
            return 1.0

        def _shares_to_adjusted_amount(self, shares, factor):
            return float(shares)

        def _is_orderable_without_warning(self, *args, **kwargs):
            return True

        def _can_sell_under_hold_thresh(self, *args):
            return False

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / "critical_actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="exit_reentry",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
    )
    weights = strategy._adjust_target_weight_map(
        {"000001.SZ": 0.5}, pd.Timestamp("2026-01-05")
    )
    orders = strategy._build_additional_rebalance_orders(
        weight_map=weights,
        current_holdings=["000001.SZ"],
        existing_sell_ids=set(),
        planned_buy_orders=[],
        total_account_value=10_000.0,
        trade_step=0,
        trade_start_time=pd.Timestamp("2026-01-05"),
        trade_end_time=pd.Timestamp("2026-01-05"),
    )

    assert weights == {"000001.SZ": 0.0}
    assert len(orders) == 1
    assert orders[0].direction == order_dir.SELL
    assert orders[0].amount == 1000.0
    assert json.loads(action_log.read_text(encoding="utf-8"))["action_type"] == "EXIT"
