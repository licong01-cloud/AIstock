from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def test_shadow_portfolio_enforces_top5_capacity_and_daily_lifecycle() -> None:
    calendar = pd.bdate_range("2026-01-02", periods=4)
    decisions = calendar[:3]
    rankings = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": decision,
                "target_trade_date": calendar[index + 1],
                "instrument": f"{rank:06d}.SZ",
                "selection_effective_rank": rank,
                "combined_score": 100.0 - rank,
            }
            for index, decision in enumerate(decisions)
            for rank in range(1, 41)
        ]
    )
    market = pd.DataFrame(
        [
            {
                "datetime": day,
                "instrument": f"{rank:06d}.SZ",
                "open": 10.0 + day_index,
                "high": 10.1 + day_index,
                "low": 9.9 + day_index,
                "factor": 1.0,
                "up_limit_price": 20.0,
                "down_limit_price": 1.0,
                "limit_up": 0,
                "limit_down": 0,
            }
            for day_index, day in enumerate(calendar)
            for rank in range(1, 41)
        ]
    ).set_index(["datetime", "instrument"])
    benchmark = pd.DataFrame(
        {"datetime": calendar, "instrument": "000300.SH", "open": [100.0] * len(calendar)}
    ).set_index(["datetime", "instrument"])
    policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=5,
        rank_exit_threshold=40,
        rank_exit_confirm_days=2,
        daily_replacement_budget=5,
        stop_loss_bps=0,
        take_profit_bps=0,
        trailing_stop_bps=0,
        time_stop_days=1,
    )
    result = replay_shadow_portfolio(
        rankings=rankings,
        daily=market,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256="a" * 64,
        cost_policy=AdvisoryPolicyCostV1(buy_cost_bps=1.0, sell_cost_bps=2.0),
        request_id="advpolreq_test",
    )
    first, second, third = result.daily.to_dict("records")
    assert first["entered_count"] == 5 and first["active_count"] == 5
    assert second["exited_count"] == 5 and second["active_count"] == 0
    assert third["entered_count"] == 5 and third["active_count"] == 5
    assert result.metrics["episode_count"] == 10
    assert result.metrics["exited_episode_count"] == 5


def test_shadow_portfolio_does_not_release_one_price_limit_down_holding() -> None:
    calendar = pd.bdate_range("2026-01-02", periods=3)
    decisions = calendar[:2]
    rankings = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": decision,
                "target_trade_date": calendar[index + 1],
                "instrument": f"{rank:06d}.SZ",
                "selection_effective_rank": rank,
                "combined_score": 100.0 - rank,
            }
            for index, decision in enumerate(decisions)
            for rank in range(1, 41)
        ]
    )
    market_rows = []
    for day_index, day in enumerate(calendar):
        for rank in range(1, 41):
            limited = day == calendar[2] and rank == 1
            price = 9.0 if limited else 10.0 + day_index
            market_rows.append(
                {
                    "datetime": day,
                    "instrument": f"{rank:06d}.SZ",
                    "open": price,
                    "high": price if limited else price + 0.1,
                    "low": price if limited else price - 0.1,
                    "factor": 1.0,
                    "up_limit_price": 20.0,
                    "down_limit_price": 9.0 if limited else 1.0,
                    "limit_up": 0,
                    "limit_down": 1 if limited else 0,
                }
            )
    market = pd.DataFrame(market_rows).set_index(["datetime", "instrument"])
    benchmark = pd.DataFrame(
        {"datetime": calendar, "instrument": "000300.SH", "open": [100.0] * len(calendar)}
    ).set_index(["datetime", "instrument"])
    policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=5,
        rank_exit_threshold=40,
        rank_exit_confirm_days=1,
        daily_replacement_budget=5,
        stop_loss_bps=100,
        take_profit_bps=0,
        trailing_stop_bps=0,
        time_stop_days=1,
    )
    result = replay_shadow_portfolio(
        rankings=rankings,
        daily=market,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256="a" * 64,
        cost_policy=AdvisoryPolicyCostV1(buy_cost_bps=0.0, sell_cost_bps=0.0),
        request_id="advpolreq_test",
    )
    last = result.daily.iloc[-1]
    assert last["waiting_count"] == 1
    assert last["active_count"] == 1
    assert last["exited_count"] == 4
    active_symbol = result.episodes[
        (result.episodes["instrument"] == "000001.SZ") & (result.episodes["status"] == "ACTIVE")
    ]
    assert len(active_symbol) == 1
    expected = (4 * (12.0 / 11.0 - 1.0) + (9.0 / 11.0 - 1.0)) * 10000.0 / 5
    assert last["gross_return_bps"] == pytest.approx(expected)
