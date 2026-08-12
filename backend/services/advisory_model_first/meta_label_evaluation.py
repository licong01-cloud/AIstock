from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def evaluate_meta_label_validation_blocks(
    *,
    rankings: pd.DataFrame,
    predictions: pd.DataFrame,
    validation_blocks: Sequence[int],
    block_by_date: dict[str, int],
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy: AdvisoryTransitionPolicyV1,
    policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    request_id: str,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    prediction_dates = set(
        pd.DatetimeIndex(pd.to_datetime(predictions["decision_as_of_trade_date"])).normalize()
    )
    expected_dates = {
        pd.Timestamp(value).normalize()
        for value, block in block_by_date.items()
        if int(block) in {int(item) for item in validation_blocks}
    }
    if prediction_dates != expected_dates:
        raise AdvisoryModelFirstError(
            "meta-label validation predictions do not cover the exact validation blocks",
            reason_code="ADVISORY_META_LABEL_EVALUATION_INVALID",
            context={
                "missing_dates": sorted(value.date().isoformat() for value in expected_dates - prediction_dates),
                "extra_dates": sorted(value.date().isoformat() for value in prediction_dates - expected_dates),
            },
        )
    daily_parts: list[pd.DataFrame] = []
    episode_parts: list[pd.DataFrame] = []
    block_metrics: list[dict[str, Any]] = []
    for block in sorted({int(item) for item in validation_blocks}):
        dates = pd.DatetimeIndex(
            sorted(pd.Timestamp(value).normalize() for value, owner in block_by_date.items() if int(owner) == block)
        )
        block_predictions = predictions[
            pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize().isin(dates)
        ].copy()
        result = replay_shadow_portfolio(
            rankings=rankings,
            daily=daily,
            benchmark_daily=benchmark_daily,
            suspend_rows=suspend_rows,
            trading_calendar=trading_calendar,
            policy=policy,
            policy_sha256=policy_sha256,
            cost_policy=cost_policy,
            request_id=f"{request_id}_block_{block}",
            candidate_decision_dates=dates,
            entry_priorities=block_predictions,
        )
        scored_daily = result.daily.copy()
        scored_daily["is_candidate_decision"] = scored_daily["decision_as_of_trade_date"].isin(dates)
        scored_daily["validation_block"] = block
        block_episode = result.episodes.copy()
        block_episode["validation_block"] = block
        daily_parts.append(scored_daily)
        episode_parts.append(block_episode)
        block_metrics.append(
            {
                "block_id": block,
                "mean_daily_net_excess_return_bps": float(scored_daily["net_excess_return_bps"].mean()),
                "mean_daily_net_return_bps": float(scored_daily["net_return_bps"].mean()),
                "maximum_drawdown": float(scored_daily["drawdown"].min()),
                "mean_turnover_fraction": float(scored_daily["turnover_fraction"].mean()),
                "day_count": len(scored_daily),
            }
        )
    all_daily = pd.concat(daily_parts, ignore_index=True)
    all_episodes = pd.concat(episode_parts, ignore_index=True)
    metrics = {
        "schema_version": "advisory_meta_label_policy_evaluation_v1",
        "mean_daily_net_excess_return_bps": float(all_daily["net_excess_return_bps"].mean()),
        "mean_daily_net_return_bps": float(all_daily["net_return_bps"].mean()),
        "maximum_drawdown": min(item["maximum_drawdown"] for item in block_metrics),
        "mean_turnover_fraction": float(all_daily["turnover_fraction"].mean()),
        "day_count": len(all_daily),
        "block_metrics": block_metrics,
    }
    return metrics, all_daily, all_episodes
