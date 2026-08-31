from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from backend.services.quantevolver.sector_risk_overlay import canonical_json_sha256


ROOT = Path(__file__).resolve().parents[3]


def _load_strategy_module(monkeypatch, *, dedicated_v2=None, dedicated_capacity=None):
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

    class DefaultV2:
        def _adjust_target_weight_map(self, weights, trade_start_time):
            return dict(weights)

        def _build_additional_rebalance_orders(self, **kwargs):
            return []

    class DefaultCapacity(DefaultV2):
        pass

    v2_module = types.ModuleType("score_weighted_strategy_v2")
    v2_module.ScoreWeightedTopkStrategyV2 = dedicated_v2 or DefaultV2
    monkeypatch.setitem(sys.modules, "score_weighted_strategy_v2", v2_module)
    capacity_module = types.ModuleType("score_weighted_strategy_v2_capacity_v1")
    capacity_module.ScoreWeightedTopkStrategyV2CapacityV1 = (
        dedicated_capacity or DefaultCapacity
    )
    monkeypatch.setitem(
        sys.modules,
        "score_weighted_strategy_v2_capacity_v1",
        capacity_module,
    )

    spec = importlib.util.spec_from_file_location(
        "qe_sector_risk_overlay_strategy_under_test",
        ROOT / "scripts" / "qe_sector_risk_overlay_strategy.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, OrderDir


def _artifact(tmp_path, state="HIGH", include_gap_instrument=False):
    data_path = tmp_path / "runtime.parquet"
    rows = [
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
    if include_gap_instrument:
        for effective_date in ("2026-01-05", "2026-01-07"):
            rows.append(
                {
                    "signal_date": pd.Timestamp(effective_date) - pd.offsets.BDay(1),
                    "effective_trade_date": pd.Timestamp(effective_date),
                    "instrument": "603227.SH",
                    "l2_code_id": 2,
                    "risk_score": 0.40,
                    "risk_state": "NORMAL",
                    "rs_turn_risk": 0.4,
                    "breadth_deterioration": 0.4,
                    "flow_divergence_risk": 0.4,
                    "leadership_concentration": 0.4,
                    "vol_crowding_risk": 0.4,
                }
            )
        rows.append(
            {
                "signal_date": pd.Timestamp("2026-01-05"),
                "effective_trade_date": pd.Timestamp("2026-01-06"),
                "instrument": "000002.SZ",
                "l2_code_id": 3,
                "risk_score": 0.40,
                "risk_state": "NORMAL",
                "rs_turn_risk": 0.4,
                "breadth_deterioration": 0.4,
                "flow_divergence_risk": 0.4,
                "leadership_concentration": 0.4,
                "vol_crowding_risk": 0.4,
            }
        )
    frame = pd.DataFrame(rows)
    frame.to_parquet(data_path, index=False)
    manifest = {
        "schema_version": "qe_sector_risk_overlay_manifest_v1",
        "dataset_identity": "fixture-v1",
        "output_start": "2026-01-05",
        "output_end": "2026-01-09",
        "artifacts": {
            "runtime": {"sha256": hashlib.sha256(data_path.read_bytes()).hexdigest()}
        },
    }
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, data_path


def _reentry_artifact(tmp_path):
    data_path = tmp_path / "reentry_runtime.parquet"
    rows = []
    for effective_date in pd.date_range("2026-01-05", periods=3, freq="B"):
        rows.append(
            {
                "signal_date": effective_date - pd.offsets.BDay(1),
                "effective_trade_date": effective_date,
                "instrument": "000001.SZ",
                "l2_code_id": 1,
                "risk_score": 0.40,
                "risk_state": "NORMAL",
                "rs_turn_risk": 0.4,
                "breadth_deterioration": 0.4,
                "flow_divergence_risk": 0.4,
                "leadership_concentration": 0.4,
                "vol_crowding_risk": 0.4,
            }
        )
    pd.DataFrame(rows).to_parquet(data_path, index=False)
    manifest = {
        "schema_version": "qe_sector_risk_overlay_manifest_v1",
        "dataset_identity": "reentry-fixture-v1",
        "output_start": "2026-01-05",
        "output_end": "2026-01-07",
        "artifacts": {
            "runtime": {"sha256": hashlib.sha256(data_path.read_bytes()).hexdigest()}
        },
    }
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path = tmp_path / "reentry_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, data_path


def test_de_risk_modes_fail_closed_when_parent_rebalance_hooks_are_missing(
    tmp_path, monkeypatch
) -> None:
    module, _ = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path)

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = types.SimpleNamespace(get_stock_list=lambda: [])

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    with pytest.raises(RuntimeError, match="missing required rebalance hooks"):
        Strategy(
            sector_risk_overlay_enabled=True,
            sector_risk_overlay_mode="bounded_de_risk",
            sector_risk_overlay_manifest_file=manifest_path,
            sector_risk_overlay_data_file=data_path,
            sector_risk_overlay_action_log=tmp_path / "actions.jsonl",
        )


def test_overlay_prefers_dedicated_rebalance_capable_parent(monkeypatch) -> None:
    class DedicatedV2:
        def _adjust_target_weight_map(self, weights, trade_start_time):
            return dict(weights)

        def _build_additional_rebalance_orders(self, **kwargs):
            return []

    class DedicatedCapacity(DedicatedV2):
        pass

    module, _ = _load_strategy_module(
        monkeypatch,
        dedicated_v2=DedicatedV2,
        dedicated_capacity=DedicatedCapacity,
    )

    assert DedicatedV2 in module.QESectorRiskOverlayScoreWeightedTopkStrategyV2.__mro__
    assert (
        DedicatedCapacity
        in module.QESectorRiskOverlayScoreWeightedTopkStrategyV2CapacityV1.__mro__
    )


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


def test_entry_gate_keeps_in_domain_missing_row_and_records_neutral_evidence(
    tmp_path, monkeypatch
) -> None:
    module, _ = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path, include_gap_instrument=True)

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = types.SimpleNamespace(get_stock_list=lambda: [])
            self._qe_suspend_filter_trade_time = pd.Timestamp("2026-01-06")

        def _normalize_signal_scores(self, scores, pred_end_time):
            return scores

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / "missing_actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="entry_gate",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
    )
    scores = strategy._normalize_signal_scores(
        pd.Series([1.0], index=["603227.SH"]),
        pd.Timestamp("2026-01-05"),
    )

    assert list(scores.index) == ["603227.SH"]
    event = json.loads(action_log.read_text(encoding="utf-8").strip())
    assert event["action_type"] == "MISSING_ARTIFACT_ROW"
    assert event["source_status"] == "MISSING_ARTIFACT_ROW"
    assert event["risk_state"] == "UNMAPPED"
    assert event["target_multiplier"] == 1.0
    assert event["order_generated"] is False


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


@pytest.mark.parametrize("state", ["NORMAL", "INCOMPLETE", "UNMAPPED"])
def test_neutral_multiplier_preserves_parent_behavior_without_overlay_orders(
    tmp_path, monkeypatch, state
) -> None:
    module, _ = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _artifact(tmp_path, state=state)

    class Position:
        def get_cash(self):
            return 9_000.0

        def get_stock_amount(self, instrument):
            return 1_000.0

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = Position()
            self.trade_exchange = types.SimpleNamespace(
                trade_w_adj_price=False,
                trade_unit=100,
                get_amount_of_trade_unit=lambda **kwargs: 100.0,
            )
            self.lot_size = 100

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
            return True

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / f"{state.lower()}_actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="bounded_de_risk",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
    )
    if state in {"INCOMPLETE", "UNMAPPED"}:
        strategy._qe_sector_risk_last_multiplier["000001.SZ"] = 0.5
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

    assert weights == {"000001.SZ": 0.5}
    assert orders == []
    assert action_log.read_text(encoding="utf-8") == ""
    if state in {"INCOMPLETE", "UNMAPPED"}:
        assert strategy._qe_sector_risk_last_multiplier["000001.SZ"] == 0.5


def test_exit_reentry_waits_until_low_risk_confirmation_is_ready(
    tmp_path, monkeypatch
) -> None:
    module, order_dir = _load_strategy_module(monkeypatch)
    manifest_path, data_path = _reentry_artifact(tmp_path)

    class Position:
        def get_cash(self):
            return 9_000.0

        def get_stock_amount(self, instrument):
            return 100.0

    class Base:
        def __init__(self, **kwargs):
            self.trade_position = Position()
            self.trade_exchange = types.SimpleNamespace(
                trade_w_adj_price=False,
                trade_unit=100,
                get_amount_of_trade_unit=lambda **kwargs: 100.0,
            )
            self.lot_size = 100

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

    class Strategy(module._QESectorRiskOverlayMixin, Base):
        pass

    action_log = tmp_path / "confirmed_reentry_actions.jsonl"
    strategy = Strategy(
        sector_risk_overlay_enabled=True,
        sector_risk_overlay_mode="exit_reentry",
        sector_risk_overlay_manifest_file=manifest_path,
        sector_risk_overlay_data_file=data_path,
        sector_risk_overlay_action_log=action_log,
        sector_risk_overlay_reentry_confirm_days=3,
    )
    strategy._qe_sector_risk_last_multiplier["000001.SZ"] = 0.5

    for trade_date in pd.date_range("2026-01-05", periods=2, freq="B"):
        weights = strategy._adjust_target_weight_map({"000001.SZ": 0.5}, trade_date)
        orders = strategy._build_additional_rebalance_orders(
            weight_map=weights,
            current_holdings=["000001.SZ"],
            existing_sell_ids=set(),
            planned_buy_orders=[],
            total_account_value=10_000.0,
            trade_step=0,
            trade_start_time=trade_date,
            trade_end_time=trade_date,
        )
        assert orders == []
        assert strategy._qe_sector_risk_last_multiplier["000001.SZ"] == 0.5

    confirmed_date = pd.Timestamp("2026-01-07")
    weights = strategy._adjust_target_weight_map({"000001.SZ": 0.5}, confirmed_date)
    orders = strategy._build_additional_rebalance_orders(
        weight_map=weights,
        current_holdings=["000001.SZ"],
        existing_sell_ids=set(),
        planned_buy_orders=[],
        total_account_value=10_000.0,
        trade_step=0,
        trade_start_time=confirmed_date,
        trade_end_time=confirmed_date,
    )

    assert len(orders) == 1
    assert orders[0].direction == order_dir.BUY
    assert orders[0].amount == 400.0
    assert strategy._qe_sector_risk_last_multiplier["000001.SZ"] == 1.0
    event = json.loads(action_log.read_text(encoding="utf-8").strip())
    assert event["action_type"] == "REENTRY_BUY"
    assert event["risk_state"] == "NORMAL"


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
