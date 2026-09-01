from __future__ import annotations

import pandas as pd

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels


def _rankings(decisions: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    calendar = pd.bdate_range(decisions.min(), periods=len(decisions) + 1)
    target = {value: calendar[index + 1] for index, value in enumerate(decisions)}
    for day_index, decision in enumerate(decisions):
        for rank in range(1, 41):
            symbol = "000006.SZ" if rank == 6 else f"{rank:06d}.SZ"
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target[decision],
                    "trade_date": decision,
                    "instrument": symbol,
                    "selection_effective_rank": rank,
                    "combined_score": 100 - rank - day_index * 0.01,
                }
            )
    return pd.DataFrame(rows)


def _market(calendar: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for index, day in enumerate(calendar):
        for rank in range(1, 41):
            symbol = "000006.SZ" if rank == 6 else f"{rank:06d}.SZ"
            price = 10.0 + index * 0.1
            rows.append(
                {
                    "datetime": day,
                    "instrument": symbol,
                    "open": price,
                    "high": price + 0.1,
                    "low": price - 0.1,
                    "close": price,
                    "factor": 1.0,
                    "up_limit_price": price * 1.1,
                    "down_limit_price": price * 0.9,
                    "limit_up": 0,
                    "limit_down": 0,
                }
            )
    return pd.DataFrame(rows).set_index(["datetime", "instrument"]).sort_index()


def test_policy_labels_include_rank6_and_use_next_open_time_stop() -> None:
    calendar = pd.bdate_range("2026-01-02", periods=7)
    decisions = calendar[:6]
    policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=5,
        rank_exit_threshold=40,
        rank_exit_confirm_days=2,
        daily_replacement_budget=5,
        stop_loss_bps=0,
        take_profit_bps=0,
        trailing_stop_bps=0,
        time_stop_days=2,
    )
    benchmark = pd.DataFrame(
        {"datetime": calendar, "instrument": "000300.SH", "open": [100 + index for index in range(7)]}
    ).set_index(["datetime", "instrument"])
    result = build_policy_episode_labels(
        rankings=_rankings(decisions),
        daily=_market(calendar),
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256="a" * 64,
        cost_policy=AdvisoryPolicyCostV1(buy_cost_bps=0.0, sell_cost_bps=0.0),
        request_identity={"request_id": "advpolreq_test", "request_sha256": "b" * 64},
    )
    row = result.labels[
        (result.labels["decision_as_of_trade_date"] == decisions[0])
        & (result.labels["instrument"] == "000006.SZ")
    ].iloc[0]
    assert row["selection_rank"] == 6
    assert row["label_status"] == "MATURED"
    assert row["entry_trade_date"] == calendar[1]
    assert row["effective_exit_date"] == calendar[3]
    assert row["exit_reason"] == "TIME_STOP"
    assert row["holding_trading_days"] == 2
