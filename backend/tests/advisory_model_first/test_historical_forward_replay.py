from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_forward.evaluation import (
    AdvisoryForwardEvaluationMarketData,
)
from backend.services.advisory_model_first.historical_forward_replay import (
    EVIDENCE_HISTORICAL_OUT_OF_TIME,
    EVIDENCE_HISTORICAL_REPLAY,
    HistoricalForwardReplayArtifactStore,
    HistoricalForwardReplayDayV1,
    HistoricalForwardEvaluationMarketSource,
    HistoricalForwardReplayPriorityV1,
    HistoricalForwardReplayRankV1,
    HistoricalForwardReplayRequestV1,
    WINDOW_CONSUMED_OR_UNKNOWN,
    WINDOW_UNCONSUMED,
    build_historical_forward_replay,
)
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


POLICY = {
    "target_count": 5,
    "rank_enter_threshold": 5,
    "rank_exit_threshold": 40,
    "rank_exit_confirm_days": 1,
    "daily_replacement_budget": 5,
    "stop_loss_bps": 100000,
    "take_profit_bps": 100000,
    "trailing_stop_bps": 100000,
    "time_stop_days": 1,
    "take_profit_mode": "trailing",
    "entry_price_basis": "next_open_executable",
    "exit_price_basis": "next_open_executable",
}
COST = AdvisoryPolicyCostV1(buy_cost_bps=3.0, sell_cost_bps=13.0)


def _day(decision: date, target: date, *, scored: bool) -> HistoricalForwardReplayDayV1:
    rankings = tuple(
        HistoricalForwardReplayRankV1(
            symbol=f"{rank:06d}.SZ",
            selection_effective_rank=rank,
            combined_score=1.0 - rank / 100.0,
        )
        for rank in range(1, 41)
    )
    priorities = (
        tuple(
            HistoricalForwardReplayPriorityV1(
                symbol=f"{rank:06d}.SZ",
                entry_priority_rank=21 - rank,
                take_probability=0.5 + rank / 100.0,
                skip_probability=0.5 - rank / 100.0,
                advisory_model_confidence=rank / 50.0,
            )
            for rank in range(1, 21)
        )
        if scored
        else ()
    )
    return HistoricalForwardReplayDayV1(
        decision_as_of_trade_date=decision,
        target_trade_date=target,
        parent_candidate_artifact_hash=(decision.isoformat().replace("-", "") * 8)[:64],
        rankings=rankings,
        entry_priorities=priorities,
    )


def _request(
    *, window_usage: str = WINDOW_UNCONSUMED
) -> HistoricalForwardReplayRequestV1:
    days = (
        _day(date(2026, 5, 15), date(2026, 5, 18), scored=True),
        _day(date(2026, 5, 18), date(2026, 5, 19), scored=True),
        _day(date(2026, 5, 19), date(2026, 5, 20), scored=False),
    )
    return HistoricalForwardReplayRequestV1(
        request_id="advhreplay_fixture",
        parent_range_run_id="ahrr_fixture",
        program_id="advp_fixture",
        package_id="pkg_fixture",
        model_descriptor_sha256="d" * 64,
        bundle_id="e" * 64,
        bundle_manifest_sha256="a" * 64,
        shadow_policy=POLICY,
        shadow_policy_sha256=canonical_json_sha256(POLICY),
        cost_policy=COST.model_dump(mode="json"),
        cost_policy_sha256=COST.policy_sha256,
        model_training_data_cutoff_trade_date=date(2026, 3, 10),
        window_usage=window_usage,
        replay_as_of_trade_date=date(2026, 5, 20),
        maturity_horizon_trade_days=1,
        market_input_sha256="f" * 64,
        implementation_sha256="1" * 64,
        context_days=days,
    )


def _market(*, future_poison: bool = False) -> AdvisoryForwardEvaluationMarketData:
    calendar = pd.DatetimeIndex(
        pd.to_datetime(["2026-05-15", "2026-05-18", "2026-05-19", "2026-05-20"])
    )
    rows: list[dict] = []
    for day_index, day in enumerate(calendar[1:]):
        for rank in range(1, 41):
            rows.append(
                {
                    "datetime": day,
                    "instrument": f"{rank:06d}.SZ",
                    "factor": 1.0,
                    "open": 10.0 + day_index * 0.1 + rank / 1000.0,
                    "high": 10.2 + day_index * 0.1 + rank / 1000.0,
                    "low": 9.8 + day_index * 0.1 + rank / 1000.0,
                    "close": 10.1 + day_index * 0.1 + rank / 1000.0,
                    "up_limit_price": 20.0,
                    "down_limit_price": 1.0,
                    "limit_up": 0.0,
                    "limit_down": 0.0,
                }
            )
    if future_poison:
        for rank in range(1, 41):
            rows.append(
                {
                    "datetime": pd.Timestamp("2026-05-21"),
                    "instrument": f"{rank:06d}.SZ",
                    "factor": 99.0,
                    "open": 9999.0,
                    "high": 9999.0,
                    "low": 9999.0,
                    "close": 9999.0,
                    "up_limit_price": 9999.0,
                    "down_limit_price": 1.0,
                    "limit_up": 1.0,
                    "limit_down": 0.0,
                }
            )
    daily = pd.DataFrame(rows).set_index(["datetime", "instrument"]).sort_index()
    benchmark = pd.DataFrame(
        {
            "datetime": calendar[1:],
            "instrument": "000300.SH",
            "open": [100.0, 101.0, 102.0],
        }
    ).set_index(["datetime", "instrument"])
    return AdvisoryForwardEvaluationMarketData(
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        trading_calendar=calendar,
        input_sha256="f" * 64,
    )


def test_historical_forward_replay_uses_shared_policy_and_classifies_locked_oot() -> (
    None
):
    artifact = build_historical_forward_replay(request=_request(), market=_market())

    assert artifact.evidence_classification == EVIDENCE_HISTORICAL_OUT_OF_TIME
    assert artifact.model_training_data_cutoff_trade_date == date(2026, 3, 10)
    assert artifact.decision_start_trade_date == date(2026, 5, 15)
    assert artifact.decision_end_trade_date == date(2026, 5, 18)
    assert artifact.replay_as_of_trade_date == date(2026, 5, 20)
    assert artifact.decision_observation_count == 2
    assert artifact.context_day_count == 3
    assert (
        artifact.metrics["schema_version"]
        == "advisory_p0d_historical_forward_metrics_v1"
    )
    assert artifact.metrics["completed_episode_hit_rate"] is not None
    assert artifact.metrics["mean_turnover_fraction"] is not None
    assert artifact.metrics["maximum_drawdown"] is not None
    assert artifact.baseline_metrics["completed_episode_hit_rate"] is not None
    assert artifact.comparison_metrics["paired_day_count"] > 0
    assert (
        artifact.comparison_metrics["comparison"]
        == "P0D_ENTRY_PRIORITY_VS_SELECTION_TOP5_MATCHED_POLICY"
    )
    assert len(artifact.daily) == 2


def test_historical_forward_replay_ignores_market_rows_after_explicit_watermark() -> (
    None
):
    clean = build_historical_forward_replay(request=_request(), market=_market())
    poisoned = build_historical_forward_replay(
        request=_request(), market=_market(future_poison=True)
    )

    assert poisoned.artifact_hash == clean.artifact_hash
    assert poisoned.metrics == clean.metrics
    assert poisoned.daily == clean.daily
    assert poisoned.episodes == clean.episodes
    assert poisoned.baseline_metrics == clean.baseline_metrics
    assert poisoned.comparison_metrics == clean.comparison_metrics


def test_historical_forward_replay_downgrades_consumed_window() -> None:
    artifact = build_historical_forward_replay(
        request=_request(window_usage=WINDOW_CONSUMED_OR_UNKNOWN),
        market=_market(),
    )
    assert artifact.evidence_classification == EVIDENCE_HISTORICAL_REPLAY


def test_historical_forward_replay_rejects_missing_exit_context_tail() -> None:
    payload = _request().model_dump(mode="json")
    payload["context_days"] = payload["context_days"][:2]
    payload["replay_as_of_trade_date"] = payload["context_days"][-1][
        "target_trade_date"
    ]
    payload["request_sha256"] = None
    with pytest.raises(ValueError, match="tail is shorter"):
        HistoricalForwardReplayRequestV1.model_validate(payload)


def test_historical_forward_artifact_store_is_immutable_and_exact_retry_safe(
    tmp_path: Path,
) -> None:
    artifact = build_historical_forward_replay(request=_request(), market=_market())
    store = HistoricalForwardReplayArtifactStore(root=tmp_path.resolve())

    first = store.publish(artifact)
    second = store.publish(artifact)

    assert first == second
    assert store.load(artifact_hash=str(artifact.artifact_hash)) == artifact


def test_historical_forward_artifact_store_rejects_repository_root() -> None:
    with pytest.raises(ValueError, match="repo-external"):
        HistoricalForwardReplayArtifactStore(root=Path.cwd().resolve())


def test_historical_market_source_derives_only_fully_missing_limit_row() -> None:
    calendar = [{"cal_date": date(2026, 5, 15)}, {"cal_date": date(2026, 5, 18)}]
    history = [
        {
            "trade_date": date(2026, 5, 15),
            "ts_code": "000001.SZ",
            "open_li": 100000,
            "high_li": 101000,
            "low_li": 99000,
            "close_li": 100000,
            "adj_factor": 1.0,
            "base_adj_factor": 1.0,
            "pre_close": 9.9,
            "up_limit": 10.89,
            "down_limit": 8.91,
            "is_st": False,
        },
        {
            "trade_date": date(2026, 5, 18),
            "ts_code": "000001.SZ",
            "open_li": 101000,
            "high_li": 102000,
            "low_li": 100000,
            "close_li": 101000,
            "adj_factor": 1.0,
            "base_adj_factor": 1.0,
            "pre_close": None,
            "up_limit": 111.0,
            "down_limit": 89.0,
            "is_st": False,
        },
    ]
    connection = _Connection(
        results=[
            calendar,
            history,
            [],
            [],
            [],
            [{"trade_date": date(2026, 5, 18), "ts_code": "000300.SH", "open": 100.0}],
        ]
    )

    market = HistoricalForwardEvaluationMarketSource(
        conn_factory=lambda: connection
    ).load(
        symbols=["000001.SZ"],
        benchmark_instrument="000300.SH",
        start_trade_date=date(2026, 5, 18),
        end_trade_date=date(2026, 5, 18),
    )

    row = market.daily.loc[(pd.Timestamp("2026-05-18"), "000001.SZ")]
    assert row["up_limit_price"] == pytest.approx(111.0)
    assert row["down_limit_price"] == pytest.approx(89.0)


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection
        self.current: list[dict] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, *_: object, **__: object) -> None:
        self.current = self.connection.results.pop(0)

    def fetchall(self) -> list[dict]:
        return self.current


class _Connection:
    def __init__(self, *, results: list[list[dict]]) -> None:
        self.results = list(results)

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self, **_: object) -> _Cursor:
        return _Cursor(self)
