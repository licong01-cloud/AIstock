from __future__ import annotations

import pandas as pd

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def test_meta_label_priority_changes_entry_but_not_active_selection_exit_rank() -> None:
    calendar = pd.bdate_range("2026-01-02", periods=3)
    rankings = pd.DataFrame([
        {"decision_as_of_trade_date": decision, "target_trade_date": calendar[index + 1], "instrument": f"{rank:06d}.SZ", "selection_effective_rank": rank, "combined_score": 100-rank}
        for index, decision in enumerate(calendar[:2]) for rank in range(1, 41)
    ])
    market = pd.DataFrame([
        {"datetime": day, "instrument": f"{rank:06d}.SZ", "open": 10.0, "high": 10.1, "low": 9.9, "factor": 1.0, "up_limit_price": 20.0, "down_limit_price": 1.0, "limit_up": 0, "limit_down": 0}
        for day in calendar for rank in range(1, 41)
    ]).set_index(["datetime", "instrument"])
    benchmark = pd.DataFrame({"datetime": calendar, "instrument": "000300.SH", "open": [100.0]*3}).set_index(["datetime", "instrument"])
    priorities = pd.DataFrame([
        {"decision_as_of_trade_date": day, "instrument": f"{rank:06d}.SZ", "entry_priority_rank": 21-rank}
        for day in calendar[:2] for rank in range(1, 21)
    ])
    policy = AdvisoryTransitionPolicyV1(target_count=5, rank_enter_threshold=5, rank_exit_threshold=40, rank_exit_confirm_days=1, daily_replacement_budget=5, stop_loss_bps=0, take_profit_bps=0, trailing_stop_bps=0, time_stop_days=20)
    result = replay_shadow_portfolio(rankings=rankings, daily=market, benchmark_daily=benchmark, suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]), trading_calendar=calendar, policy=policy, policy_sha256="a"*64, cost_policy=AdvisoryPolicyCostV1(buy_cost_bps=0, sell_cost_bps=0), request_id="meta", entry_priorities=priorities)
    entered = set(result.episodes["instrument"])
    assert entered == {f"{rank:06d}.SZ" for rank in range(16, 21)}
    assert result.daily.iloc[1]["exited_count"] == 0
