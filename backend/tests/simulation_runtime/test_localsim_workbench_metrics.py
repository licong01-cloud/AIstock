from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from backend.services.simulation_runtime.localsim_workbench_metrics import apply_valuation_metrics
from backend.services.simulation_runtime.localsim_workbench_metrics import validate_workbench_overview
from backend.services.simulation_runtime.localsim_workbench_models import WorkbenchAccount, WorkbenchOverview
from backend.services.simulation_runtime.localsim_workbench_presentation import account_name, strategy_name
from backend.tests.simulation_runtime.test_localsim_workbench import Service, facts
from backend.services.trading_core.errors import DataUnavailableError


def snapshot_summary(data, now=None):
    service = Service(data)
    summary = service._summary(data)
    if now:
        apply_valuation_metrics(data, summary, positions=service._positions, is_close=service._is_close, now=now)
    return summary


def test_intraday_lifecycle_profit_survives_missing_daily_closes_without_becoming_closed_return():
    data = facts()
    for snap in data["snapshots"]:
        snap["snapshot_time"] = snap["snapshot_time"].replace(hour=1, minute=34)
    summary = snapshot_summary(data, datetime(2026, 10, 20, tzinfo=UTC))
    # Independent cash/valuation oracle: -1000-5+1200-5 = 190, actual fees once.
    assert Decimal(summary.cumulative_pnl) == 190
    assert Decimal(summary.cumulative_return_pct) == Decimal("1.9")
    assert Decimal(summary.cumulative_fees) == 10
    assert summary.valuation_day == "2026-01-06" and summary.valuation_day_pnl is None
    assert "前一交易日" in summary.valuation_day_note
    assert summary.today_pnl is None and "今天尚无" in summary.today_note
    assert Decimal(summary.valuation_change) == 95
    assert "不能代替日盈亏" in summary.change_note
    assert Service(data).view("a", "pnl").summary.pnl is None


def test_today_profit_can_use_previous_close_and_current_intraday_valuation():
    data = facts()
    data["snapshots"][-1]["snapshot_time"] = datetime(2026, 1, 6, 1, 34, tzinfo=UTC)
    summary = snapshot_summary(data, datetime(2026, 1, 6, 3, tzinfo=UTC))
    assert summary.valuation_day_pnl == summary.today_pnl == "95"
    assert summary.cumulative_pnl == "190"
    assert Service(data).view("a", "pnl", start=datetime(2026, 1, 6).date()).summary.pnl is None


def test_calendar_gaps_do_not_erase_proven_lifecycle_profit_or_create_daily_return():
    data = facts()
    data["snapshots"].pop(0)
    summary = snapshot_summary(data)
    assert summary.cumulative_pnl == "190"
    assert summary.valuation_day_pnl is None


def test_first_account_day_uses_actual_initial_capital():
    data = facts()
    data["snapshots"] = data["snapshots"][:1]
    data["runs"] = data["runs"][:1]
    data["fills"] = data["fills"][:1]
    data["cash"] = data["cash"][:1]
    data["snapshots"][0]["snapshot_time"] = datetime(2026, 1, 5, 1, 33, tzinfo=UTC)
    summary = snapshot_summary(data, datetime(2026, 1, 5, 3, tzinfo=UTC))
    assert summary.cumulative_pnl == summary.valuation_day_pnl == summary.today_pnl == "95"


@pytest.mark.parametrize("fault", ["missing_fill", "duplicate_fill", "cash_adjustment", "quantity_adjustment", "external_flow", "wrong_fill_side", "unknown_run", "wrong_fee"])
def test_unexplained_cash_or_inventory_never_becomes_profit(fault):
    data = facts()
    if fault == "missing_fill":
        data["fills"].pop(0)
    elif fault == "duplicate_fill":
        data["fills"].append(deepcopy(data["fills"][0]))
    elif fault == "cash_adjustment":
        data["snapshots"][-1].update(cash="11190", nav="11190")
    elif fault == "quantity_adjustment":
        data["fills"][0].update(quantity=50, price="20")
    elif fault == "external_flow":
        data["cash"].append({**data["cash"][0], "fill_id": None, "symbol": None, "side": "DEPOSIT"})
    elif fault == "wrong_fill_side":
        data["fills"][0]["side"] = "SELL"
    elif fault == "unknown_run":
        data["fills"][0]["run_id"] = "unattributed"
    else:
        data["cash"][0]["fee"] = "-5"
    summary = snapshot_summary(data)
    assert summary.cumulative_pnl is None and summary.cumulative_return_pct is None
    assert summary.cumulative_note != "尚无已核验的期初资金和估值"
    assert summary.nav is not None


def test_unbalanced_positions_are_not_accepted_for_lifecycle_profit():
    data = facts()
    data["snapshots"][-1]["position_count"] = 1
    summary = snapshot_summary(data)
    assert summary.cumulative_pnl is None and "未对齐" in summary.cumulative_note


def test_legacy_origin_must_be_authoritative_and_still_pass_the_complete_ledger_bridge():
    data = facts()
    data["native"] = False
    assert snapshot_summary(data).cumulative_pnl is None
    data["capital_origin"] = {"initial_cash": "10000"}
    assert snapshot_summary(data).cumulative_pnl == "190"
    data["snapshots"][-1].update(cash="11000", nav="11000")
    summary = snapshot_summary(data)
    assert summary.cumulative_pnl is None and "期初资金" in summary.cumulative_note


def test_later_failed_run_facts_do_not_change_an_earlier_committed_valuation():
    data = facts()
    data["runs"].append({**data["runs"][-1], "run_id": "future", "trade_date": "2026-01-07"})
    data["fills"].append({**data["fills"][-1], "fill_id": "future", "run_id": "future"})
    data["cash"].append({**data["cash"][-1], "fill_id": "future", "run_id": "future", "trade_date": "2026-01-07"})
    assert snapshot_summary(data).cumulative_pnl == "190"


def test_separate_accounts_do_not_share_baselines_or_profit_results():
    first, second = facts(), facts()
    second["account"]["initial_capital"] = "20000"
    for snap in second["snapshots"]:
        snap["cash"] = str(Decimal(snap["cash"])+10000)
        snap["nav"] = str(Decimal(snap["nav"])+10000)
    assert snapshot_summary(first).cumulative_return_pct == "1.900"
    assert snapshot_summary(second).cumulative_return_pct == "0.9500"


@pytest.mark.parametrize("model,expected", [("__seed_LSTM_10D_hs64_d02__", "LSTM长短期记忆"), ("__seed_TCN_10D_d02__", "TCN时序卷积"), ("unrecognized_private_model", "自定义模型")])
def test_frozen_model_and_factor_names_replace_opaque_names_without_package_allowlists(model, expected):
    package = {"manifest_sha256": "frozen", "display_name": "qe_20261007_141342_0221 策略回测4", "manifest_json": {
        "model_asset": {"model_id": model}, "factor_set": [{"factor_id": f"dynamic_{i}"} for i in range(26)],
        "runtime_assets": {"alpha158": {"enabled": True}},
    }}
    label = strategy_name(package, "frozen")
    assert label == expected+"＋Alpha158价量因子＋26项因子"
    assert account_name({"account_name": "simacct_private", "manifest_sha256": "frozen"}, package) == label+" · 模拟账户"
    assert "qe_" not in label and "__" not in label
    assert "待核验" in strategy_name(package, "another-frozen-manifest")


def test_existing_readable_name_and_multi_model_packages_remain_supported():
    assert strategy_name({"display_name": "稳健成长"}) == "稳健成长"
    label = strategy_name({"manifest_json": {"model_asset": [{"model_type": "LSTM"}, {"model_type": "TCN"}], "factor_set": [{}]}})
    assert "LSTM长短期记忆 / TCN时序卷积" in label and "1项因子" in label


def test_nonfinite_cash_facts_fail_closed_with_safe_error():
    data = facts()
    data["cash"][0]["fee"] = "NaN"
    with pytest.raises(DataUnavailableError, match="金额数据未通过校验"):
        snapshot_summary(data)


def test_snapshot_cannot_borrow_another_trading_dates_run():
    data = facts()
    data["snapshots"][-1]["run_id"] = "r1"
    data["snapshots"][-1]["position_count"] = 0
    summary = snapshot_summary(data)
    assert summary.cumulative_pnl is None


def overview_payload():
    return WorkbenchOverview(items=[WorkbenchAccount(key="a", summary=snapshot_summary(facts()))],
                             generated_at="2026-10-20T02:00:00+08:00", complete=True).model_dump(mode="json")


def test_post_restart_semantics_validate_actual_workbench_fields_and_allow_honest_unknowns():
    payload = overview_payload()
    assert validate_workbench_overview(payload)[0] == "passed"
    summary = payload["items"][0]["summary"]
    summary["cumulative_pnl"] = summary["cumulative_return_pct"] = None
    summary["cumulative_note"] = "期初资金与累计流水不能对齐"
    assert validate_workbench_overview(payload)[0] == "passed"


@pytest.mark.parametrize("fault", ["old_response", "opaque_name", "assets", "return", "fake_today", "scope", "empty", "naive_clock", "nan"])
def test_post_restart_smoke_cannot_pass_on_http_success_alone(fault):
    payload = overview_payload()
    summary = payload["items"][0]["summary"]
    if fault == "old_response":
        summary.pop("cumulative_pnl")
    elif fault == "opaque_name":
        summary["strategy"] = "qe_private_package 策略回测4"
    elif fault == "assets":
        summary["cash"] = "0"
    elif fault == "return":
        summary["cumulative_return_pct"] = "999"
    elif fault == "fake_today":
        summary["today_pnl"] = "95"
    elif fault == "scope":
        payload["items"].append(deepcopy(payload["items"][0]))
    elif fault == "empty":
        payload["items"] = []
    elif fault == "naive_clock":
        payload["generated_at"] = "2026-10-20T02:00:00"
    else:
        summary["initial_capital"] = "NaN"
    assert validate_workbench_overview(payload)[0] == "failed"


def test_workbench_endpoint_has_its_own_registered_business_smoke():
    from scripts.aistock_issue_workflow import _business_smoke_semantic_contract

    contract, validator = _business_smoke_semantic_contract("/api/v1/simulation-runtime/localsim/workbench/overview")
    assert contract == "localsim_workbench_valuation"
    assert validator(overview_payload())[0] == "passed"
