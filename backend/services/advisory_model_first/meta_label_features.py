from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.fresh_hmm import (
    build_sector_observations,
    fit_fresh_sector_hmm,
)
from backend.services.advisory_model_first.shared_feature_builder import build_advisory_feature_matrix


@dataclass(frozen=True)
class MetaLabelFeatureResult:
    features: pd.DataFrame
    coverage: pd.DataFrame
    walk_forward_hmm_receipt: dict[str, Any]
    runtime_hmm_models: dict[str, Any]
    runtime_hmm_unavailable: tuple[dict[str, Any], ...]


def build_meta_label_feature_matrix(
    *,
    rankings: pd.DataFrame,
    block_by_date: dict[str, int],
    candidate_daily: pd.DataFrame,
    candidate_static: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    static_all: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    hmm_history_start: str,
    runtime_cutoff: str,
) -> MetaLabelFeatureResult:
    candidates = rankings.loc[rankings["is_candidate_decision"]].copy()
    candidates = candidates[candidates["selection_effective_rank"] <= 20].copy()
    if candidates.empty:
        raise AdvisoryModelFirstError(
            "meta-label feature builder has no candidate rows",
            reason_code="ADVISORY_META_LABEL_FEATURE_EMPTY",
        )
    candidates["selection_source_rank"] = candidates["selection_effective_rank"]
    candidates["candidate_group_size"] = 20
    observations = build_sector_observations(
        static_all=static_all,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
    )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    blocks: dict[int, pd.DatetimeIndex] = {}
    for raw_date, block in block_by_date.items():
        blocks.setdefault(int(block), []).append(pd.Timestamp(raw_date).normalize())
    block_dates = {
        block: pd.DatetimeIndex(sorted(values)) for block, values in sorted(blocks.items())
    }
    if set(block_dates) != set(range(8)):
        raise AdvisoryModelFirstError(
            "meta-label feature builder requires the exact eight CPCV blocks",
            reason_code="ADVISORY_META_LABEL_HMM_BLOCK_INVALID",
            context={"block_ids": sorted(block_dates)},
        )
    hmm_states: list[pd.DataFrame] = []
    block_receipts: list[dict[str, Any]] = []
    latest_result = None
    history_start = pd.Timestamp(hmm_history_start).normalize()
    hmm_calendar = calendar[calendar >= history_start]
    for block, dates in block_dates.items():
        start = dates[0]
        train_dates = hmm_calendar[hmm_calendar < start]
        if len(train_dates) < 120:
            block_receipts.append(
                {
                    "block_id": block,
                    "status": "HMM_UNAVAILABLE_FOR_BLOCK",
                    "reason_code": "INSUFFICIENT_PAST_TRADING_DAYS",
                    "train_date_count": len(train_dates),
                    "block_start": start.date().isoformat(),
                }
            )
            continue
        result = fit_fresh_sector_hmm(
            static_all=static_all,
            market_daily=market_daily,
            benchmark_daily=benchmark_daily,
            trading_calendar=hmm_calendar,
            train_dates=train_dates,
            continuation_cutoff=dates[-1].date().isoformat(),
            precomputed_observations=observations,
        )
        state = result.states[result.states["decision_as_of_trade_date"].isin(dates)].copy()
        state["hmm_block_id"] = block
        hmm_states.append(state)
        block_receipts.append(
            {
                "block_id": block,
                "status": "AVAILABLE",
                "train_start": train_dates[0].date().isoformat(),
                "train_end": train_dates[-1].date().isoformat(),
                "block_start": dates[0].date().isoformat(),
                "block_end": dates[-1].date().isoformat(),
                "model_count": len(result.models["models"]),
                "unavailable_count": len(result.unavailable),
            }
        )
        latest_result = result
    if latest_result is None or not hmm_states:
        raise AdvisoryModelFirstError(
            "walk-forward HMM produced no usable blocks",
            reason_code="ADVISORY_META_LABEL_HMM_NOT_AVAILABLE",
        )
    walk_forward_states = pd.concat(hmm_states, ignore_index=True)
    runtime_train_dates = hmm_calendar[
        hmm_calendar <= pd.Timestamp(runtime_cutoff).normalize()
    ]
    runtime_result = fit_fresh_sector_hmm(
        static_all=static_all,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
        trading_calendar=hmm_calendar,
        train_dates=runtime_train_dates,
        continuation_cutoff=runtime_cutoff,
        precomputed_observations=observations,
    )
    built = build_advisory_feature_matrix(
        candidates=candidates,
        candidate_daily=candidate_daily,
        candidate_static=candidate_static,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        hmm_states=walk_forward_states,
    )
    return MetaLabelFeatureResult(
        features=built.features,
        coverage=built.coverage,
        walk_forward_hmm_receipt={
            "schema_version": "advisory_meta_label_walk_forward_hmm_v1",
            "history_start": history_start.date().isoformat(),
            "runtime_cutoff": runtime_cutoff,
            "blocks": block_receipts,
        },
        runtime_hmm_models=runtime_result.models,
        runtime_hmm_unavailable=runtime_result.unavailable,
    )
