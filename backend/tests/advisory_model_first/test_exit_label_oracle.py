from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.action_value_contracts import AdvisoryActionValueStatus
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.exit_label_oracle import (
    AdvisoryExitDecisionV1,
    ExitOracleAction,
    ExitOracleExecutionState,
    build_exit_label_oracle,
    _build_exit_label_oracle_from_baseline,
)
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.policy_episode_labels import PolicyEpisodeLabelResult


POLICY_SHA = "a" * 64
INTERVENTION_SHA = "b" * 64
COST = AdvisoryPolicyCostV1(buy_cost_bps=0.0, sell_cost_bps=0.0)
CALENDAR = pd.bdate_range("2026-01-02", periods=5)


def _baseline(*, policy_sha: str = POLICY_SHA, status: str = "MATURED", liability: float = 0.0):
    labels = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": CALENDAR[0],
                "target_trade_date": CALENDAR[0],
                "instrument": "000001.SZ",
                "episode_label_id": "advpolep_exit_test",
                "shadow_policy_sha256": policy_sha,
                "cost_policy_sha256": COST.policy_sha256,
                "entry_trade_date": CALENDAR[0],
                "entry_price": 10.0,
                "exit_signal_date": CALENDAR[3],
                "effective_exit_date": CALENDAR[4],
                "exit_price": 9.0,
                "net_return_bps": -1000.0,
                "label_status": status,
                "label_reason": None if status == "MATURED" else "test_unavailable",
                "label_information_end": CALENDAR[4],
                "liability_target": liability,
            }
        ]
    )
    return PolicyEpisodeLabelResult(labels=labels, coverage=pd.DataFrame())


def _market() -> pd.DataFrame:
    prices = [10.0, 10.5, 10.8, 9.5, 9.0]
    rows = []
    for day, price in zip(CALENDAR, prices, strict=True):
        rows.append(
            {
                "datetime": day,
                "instrument": "000001.SZ",
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


def _run(*, baseline=None, market=None, suspend_rows=None):
    return _build_exit_label_oracle_from_baseline(
        baseline=baseline or _baseline(),
        daily=market if market is not None else _market(),
        suspend_rows=(suspend_rows if suspend_rows is not None else pd.DataFrame(columns=["trade_date", "instrument"])),
        trading_calendar=CALENDAR,
        policy_sha256=POLICY_SHA,
        intervention_policy_sha256=INTERVENTION_SHA,
        cost_policy=COST,
    )


def test_exit_oracle_measures_action_advantage_against_same_baseline() -> None:
    result = _run()
    first = result.labels[0]
    last = result.labels[-1]
    assert len(result.labels) == 4
    assert first.baseline_net_value_bps == -1000.0
    assert first.action_net_value_bps == pytest.approx(500.0)
    assert first.incremental_net_value_bps == pytest.approx(1500.0)
    assert result.decisions[0].action == ExitOracleAction.EXIT_NEXT_OPEN
    assert result.decisions[0].execution_state == ExitOracleExecutionState.EXECUTED_NEXT_OPEN
    assert last.incremental_net_value_bps == 0.0
    assert result.decisions[-1].action == ExitOracleAction.HOLD
    assert result.decisions[-1].execution_state == ExitOracleExecutionState.BASELINE_CONTINUE


def test_exit_oracle_defers_suspension_and_one_price_limit_down() -> None:
    market = _market()
    market.loc[(CALENDAR[2], "000001.SZ"), ["open", "high", "low", "close"]] = 8.8
    market.loc[(CALENDAR[2], "000001.SZ"), "down_limit_price"] = 8.8
    market.loc[(CALENDAR[2], "000001.SZ"), "limit_down"] = 1
    suspend = pd.DataFrame([{"trade_date": CALENDAR[1], "instrument": "000001.SZ"}])
    result = _run(market=market, suspend_rows=suspend)
    first_label = result.labels[0]
    first_decision = result.decisions[0]
    assert first_label.effective_action_date == CALENDAR[3].date()
    assert first_decision.action == ExitOracleAction.EXIT_NEXT_OPEN
    assert first_decision.execution_state == ExitOracleExecutionState.DEFERRED_TO_FIRST_EXECUTABLE
    assert first_decision.deferred_trading_days == 2


def test_exit_oracle_keeps_censored_and_unavailable_rows_typed() -> None:
    suspend = pd.DataFrame([{"trade_date": value, "instrument": "000001.SZ"} for value in CALENDAR[1:]])
    censored = _run(suspend_rows=suspend)
    assert censored.labels[0].status == AdvisoryActionValueStatus.CENSORED_RIGHT_BOUNDARY
    assert censored.labels[0].incremental_net_value_bps is None
    assert censored.decisions[0].action == ExitOracleAction.WAITING

    unavailable = _run(baseline=_baseline(status="DATA_UNAVAILABLE"))
    assert unavailable.labels[0].status == AdvisoryActionValueStatus.BASELINE_UNAVAILABLE
    assert unavailable.labels[0].incremental_net_value_bps is None
    assert unavailable.decisions[0].execution_state == ExitOracleExecutionState.BASELINE_UNAVAILABLE


def test_exit_oracle_distinguishes_market_missing_from_right_censoring() -> None:
    market = _market().drop(index=(CALENDAR[1], "000001.SZ"))
    result = _run(market=market)
    assert result.labels[0].status == AdvisoryActionValueStatus.DATA_UNAVAILABLE
    assert result.decisions[0].execution_state == ExitOracleExecutionState.DATA_UNAVAILABLE


def test_exit_oracle_ignores_liability_and_rejects_policy_drift() -> None:
    normal = _run(baseline=_baseline(liability=0.0)).labels[0].incremental_net_value_bps
    poisoned = _run(baseline=_baseline(liability=999999.0)).labels[0].incremental_net_value_bps
    assert normal == poisoned
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _run(baseline=_baseline(policy_sha="f" * 64))
    assert excinfo.value.reason_code == "ADVISORY_ACTION_VALUE_POLICY_MISMATCH"


def test_public_exit_oracle_calls_the_existing_baseline_simulator(monkeypatch) -> None:
    called = {}

    def fake_builder(**kwargs):
        called.update(kwargs)
        return _baseline()

    monkeypatch.setattr(
        "backend.services.advisory_model_first.exit_label_oracle.build_policy_episode_labels",
        fake_builder,
    )
    policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=5,
        rank_exit_threshold=40,
        rank_exit_confirm_days=2,
        daily_replacement_budget=5,
        stop_loss_bps=0,
        take_profit_bps=0,
        trailing_stop_bps=0,
        time_stop_days=20,
    )
    result = build_exit_label_oracle(
        rankings=pd.DataFrame(),
        daily=_market(),
        benchmark_daily=pd.DataFrame(),
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
        trading_calendar=CALENDAR,
        policy=policy,
        policy_sha256=POLICY_SHA,
        intervention_policy_sha256=INTERVENTION_SHA,
        cost_policy=COST,
        request_identity={"request_id": "test", "request_sha256": "c" * 64},
    )
    assert called["candidate_depth"] == 5
    assert result.labels[0].shadow_simulator_sha256


def test_public_exit_oracle_is_compatible_with_real_policy_episode_builder() -> None:
    calendar = pd.bdate_range("2026-02-02", periods=7)
    decisions = calendar[:6]
    ranking_rows = []
    market_rows = []
    for index, day in enumerate(calendar):
        for rank in range(1, 41):
            symbol = f"{rank:06d}.SZ"
            price = 10.0 + index * 0.1 + rank * 0.001
            market_rows.append(
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
            if index < len(decisions):
                ranking_rows.append(
                    {
                        "decision_as_of_trade_date": day,
                        "target_trade_date": calendar[index + 1],
                        "trade_date": day,
                        "instrument": symbol,
                        "selection_effective_rank": rank,
                        "combined_score": 100.0 - rank,
                    }
                )
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
    market = pd.DataFrame(market_rows).set_index(["datetime", "instrument"]).sort_index()
    benchmark = pd.DataFrame({"datetime": calendar, "instrument": "000300.SH", "open": range(100, 107)}).set_index(
        ["datetime", "instrument"]
    )
    result = build_exit_label_oracle(
        rankings=pd.DataFrame(ranking_rows),
        daily=market,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=POLICY_SHA,
        intervention_policy_sha256=INTERVENTION_SHA,
        cost_policy=COST,
        request_identity={"request_id": "test", "request_sha256": "c" * 64},
    )
    assert not result.baseline.labels.empty
    assert result.labels
    assert all(item.baseline_policy_sha256 == POLICY_SHA for item in result.labels)


def test_exit_decision_schema_rejects_position_fields() -> None:
    decision = _run().decisions[0]
    with pytest.raises(ValidationError):
        AdvisoryExitDecisionV1.model_validate({**decision.model_dump(mode="python"), "target_weight": 0.2})


def test_exit_oracle_rejects_non_top5_scope_before_simulation() -> None:
    policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=5,
        rank_exit_threshold=40,
        rank_exit_confirm_days=2,
        daily_replacement_budget=5,
        stop_loss_bps=0,
        take_profit_bps=0,
        trailing_stop_bps=0,
        time_stop_days=20,
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        build_exit_label_oracle(
            rankings=pd.DataFrame(),
            daily=_market(),
            benchmark_daily=pd.DataFrame(),
            suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]),
            trading_calendar=CALENDAR,
            policy=policy,
            policy_sha256=POLICY_SHA,
            intervention_policy_sha256=INTERVENTION_SHA,
            cost_policy=COST,
            request_identity={"request_id": "test", "request_sha256": "c" * 64},
            candidate_depth=20,
        )
    assert excinfo.value.reason_code == "ADVISORY_EXIT_BASELINE_UNAVAILABLE"


def test_exit_oracle_does_not_create_exit_decisions_for_not_entered_candidates() -> None:
    baseline = _baseline(status="NOT_ENTERED_SUSPENDED")
    baseline.labels.loc[:, "entry_trade_date"] = pd.NaT
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _run(baseline=baseline)
    assert excinfo.value.reason_code == "ADVISORY_EXIT_BASELINE_UNAVAILABLE"
