from copy import deepcopy
from datetime import UTC, datetime

from backend.services.simulation_runtime.localsim_integrity_recovery import inspect_integrity, prepare_recovery_scope
from backend.tests.simulation_runtime.test_localsim_workbench import Service, facts


NOW = datetime(2026, 1, 7, 3, tzinfo=UTC)


def report(data):
    service = Service(data)
    return inspect_integrity(data, positions=service._positions, is_close=service._is_close, now=NOW)


def test_closed_cash_and_inventory_are_checked_at_same_valuation_boundary():
    data = facts()
    before = deepcopy(data)
    data["runs"].append({"run_id": "future", "trade_date": "2026-01-07", "status": "RUNNING"})
    data["cash"].append({**data["cash"][0], "trade_date": "2026-01-07", "run_id": "future", "cash_delta": "-50"})
    result = report(data)
    assert result["cash_bridge_residual"] == "0"
    assert result["inventory_mismatch_count"] == 0
    assert result["economic_recomputation_performed"] is False
    assert before["snapshots"] == data["snapshots"]


def test_unexplained_legacy_cash_is_never_a_repairable_external_capital_flow():
    data = facts()
    data["native"] = False
    data["capital_origin"] = {"initial_cash": "10000"}
    data["snapshots"][-1].update(cash="10290", nav="10290")
    result = report(data)
    assert result["cash_bridge_residual"] == "100"
    assert result["issues"][0]["code"] == "ECONOMIC_BRIDGE_UNVERIFIED"
    service = Service(data)
    plan = prepare_recovery_scope(data, positions=service._positions, is_close=service._is_close, now=NOW)
    assert plan["new_capital_added"] == "0" and plan["production_write_authorized"] is False
    assert plan["economic_replay_allowed"] is False
    assert "DEV validation receipt" in plan["required_evidence"]


def test_pending_completion_and_historical_close_have_independent_coverage():
    data = facts()
    data["snapshots"][-1]["snapshot_time"] = datetime(2026, 1, 6, 1, 33, tzinfo=UTC)
    data["valuation"] = [{"run_id": "r2", "trade_date": "2026-01-06", "status": "FAILED_RETRYABLE",
        "outbox_status": "PENDING", "pending": {"status": "WAITING_FOR_AUTHORITATIVE_MARKS",
        "missing_symbols": ["600000.SH"], "valuation_error": {"reason_code": "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE"}}}]
    result = report(data)
    assert result["missing_close_dates"] == ["2026-01-06"]
    assert result["pending_valuations"][0]["symbols"] == ["600000.SH"]
    assert Service(data)._summary(data).valuation_state == "PENDING"
    data["valuation"][0]["outbox_status"] = "PROJECTED"
    assert report(data)["pending_valuations"] == []


def test_missing_replay_registration_does_not_hide_successful_native_run():
    data = facts()
    data["runs"][-1]["status"] = "SUCCEEDED"
    summary = Service(data)._summary(data)
    assert summary.stage == "日常模拟，交易已完成"
    assert summary.source == "本地模拟账本"
    assert summary.cumulative_pnl == "190"


def test_recovery_identity_changes_for_old_fact_correction_without_changing_snapshot_clock():
    data = facts()
    service = Service(data)
    args = {"positions": service._positions, "is_close": service._is_close, "now": NOW}
    first = prepare_recovery_scope(data, **args)
    assert prepare_recovery_scope(data, **args) == first
    data["positions"][0]["avg_cost"] = "9.99"
    assert prepare_recovery_scope(data, **args)["identity"] != first["identity"]
