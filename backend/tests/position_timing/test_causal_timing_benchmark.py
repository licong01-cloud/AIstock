from __future__ import annotations

import json

import numpy as np
import pandas as pd

from backend.services.position_timing.causal_timing_benchmark import (
    _execution_diagnostics, _joint_block_intervals, _minute_required_symbols,
    _oracle_summary, _seal, inspect,
)
from backend.services.position_timing.action_value_data import DailyCandidate
from backend.services.position_timing.pattern_close_cash_benchmark import publish_json


def test_joint_bootstrap_is_deterministic_and_keeps_paired_paths() -> None:
    baseline = np.linspace(10_000_000.0, 11_000_000.0, 80)
    candidate = baseline * np.linspace(1.0, 1.05, 80)
    first = _joint_block_intervals(candidate, baseline, alpha=0.05)
    second = _joint_block_intervals(candidate, baseline, alpha=0.05)
    assert first == second
    assert first["terminal_excess_bps"]["lower"] > 0


def test_immutable_bundle_manifest_detects_member_drift(tmp_path) -> None:
    publish_json(tmp_path / "receipt.json", {"status": "EXPLORATORY"})
    manifest = _seal(tmp_path, ["receipt.json"], request_sha256="1" * 64)
    result = inspect(tmp_path, request_sha256="1" * 64)
    assert result["manifest_sha256"] == manifest["manifest_sha256"]
    (tmp_path / "receipt.json").write_text(json.dumps({"status": "changed"}), encoding="utf-8")
    try:
        inspect(tmp_path, request_sha256="1" * 64)
    except ValueError as exc:
        assert str(exc) == "CLOSE_CASH_IMMUTABLE_FILE_DRIFT"
    else:  # pragma: no cover - a changed artifact must never be accepted
        raise AssertionError("changed immutable bundle was accepted")


def test_minute_preflight_scope_keeps_training_only_history_out_of_execution_requirement(tmp_path) -> None:
    spans = pd.DataFrame({
        "symbol": ["000001.SZ", "000002.SZ", "000003.SZ"],
        "start": pd.to_datetime(["2018-08-01", "2018-08-01", "2025-01-01"]),
        "end": pd.to_datetime(["2023-12-29", "2026-08-31", "2026-08-31"]),
    })
    candidate = DailyCandidate(
        tmp_path, pd.DatetimeIndex([]), spans, set(), {},
    )
    assert _minute_required_symbols(candidate) == ("000002.SZ", "000003.SZ")


def test_oracle_and_execution_diagnostics_keep_hindsight_separate() -> None:
    events = pd.DataFrame([{
        "status": "MATURED", "oracle_net_action_value_bps": 20.0,
        "fixed5_net_action_value_bps": -5.0, "best_delay_sessions": 3,
    }])
    oracle = [{
        "status": "COMPLETE", "path_count": 4,
        "best_terminal": {"terminal_nav_cny": 10_100_000.0},
        "bh": {"terminal_nav_cny": 10_000_000.0},
        "constrained": {
            "mdd_20": {"terminal_nav_cny": 10_050_000.0},
            "mdd_30": "NO_FEASIBLE_PATH",
        },
    }]
    summary = _oracle_summary(events, oracle)
    assert summary["restricted_account_oracle"]["policy_access"] is False
    assert summary["restricted_account_oracle"]["best_excess_vs_bh_bps"]["median"] == 100.0
    fills = pd.DataFrame([
        {"symbol": "000001.SZ", "policy_id": "P", "authority": "A", "decision_ordinal": 1,
         "execution_ordinal": 2, "side": "SELL", "execution_view": "DAILY_CLOSE",
         "status": "FILLED", "raw_price": 10.0, "raw_quantity": 100},
        {"symbol": "000001.SZ", "policy_id": "P", "authority": "A", "decision_ordinal": 1,
         "execution_ordinal": 2, "side": "SELL", "execution_view": "MINUTE_CLOSE_PROXY",
         "status": "FILLED", "raw_price": 10.1, "raw_quantity": 100},
        {"symbol": "000001.SZ", "policy_id": "P", "authority": "A", "decision_ordinal": 1,
         "execution_ordinal": 2, "side": "SELL", "execution_view": "SCHEDULED_1000_PROXY",
         "status": "FILLED", "raw_price": 9.9, "raw_quantity": 100},
    ])
    execution = _execution_diagnostics(fills)
    assert execution["MINUTE_CLOSE_PROXY"]["same_quantity_count"] == 1
    assert execution["status"] == "PRICE_PROXY_DIAGNOSTIC_NOT_QUEUE_PROVEN"
