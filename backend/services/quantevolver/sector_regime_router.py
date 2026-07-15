"""QE-only observable-state routing between stock-only and sector-soft signals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_evaluation import (
    moving_block_bootstrap_mean,
    newey_west_mean_test,
)


SECTOR_ROUTER_CLASSIFICATION = "REAL_QE_OBSERVABLE_SECTOR_ROUTER"


@dataclass(frozen=True)
class SectorAgreementRouterConfig:
    horizon: int
    lookback: int = 126
    min_periods: int = 60
    agreement_quantile: float = 0.75
    bootstrap_samples: int = 500
    bootstrap_seed: int = 20260716

    def __post_init__(self) -> None:
        if self.horizon <= 0:
            raise ValueError("router horizon must be positive")
        if self.lookback <= 1:
            raise ValueError("router lookback must exceed one day")
        if not 2 <= self.min_periods <= self.lookback:
            raise ValueError("router min_periods must be in [2, lookback]")
        if not 0.0 < self.agreement_quantile < 1.0:
            raise ValueError("router agreement_quantile must be in (0, 1)")


@dataclass
class SectorAgreementRouterResult:
    daily: pd.DataFrame
    metrics: list[dict[str, Any]]
    audit: dict[str, Any]


@dataclass(frozen=True)
class SectorWalkForwardRouterConfig:
    horizon: int
    top_m: int = 5
    min_train_days: int = 80
    ridge_alpha: float = 10.0
    route_threshold: float = 0.0
    bootstrap_samples: int = 500
    bootstrap_seed: int = 20260716

    def __post_init__(self) -> None:
        if self.horizon <= 0:
            raise ValueError("walk-forward router horizon must be positive")
        if self.top_m <= 0:
            raise ValueError("walk-forward router top_m must be positive")
        if self.min_train_days < 10:
            raise ValueError("walk-forward router min_train_days must be at least 10")
        if self.ridge_alpha <= 0.0:
            raise ValueError("walk-forward router ridge_alpha must be positive")


@dataclass
class SectorWalkForwardRouterResult:
    daily: pd.DataFrame
    coefficients: pd.DataFrame
    metrics: list[dict[str, Any]]
    audit: dict[str, Any]


def _normalize_scores(frame: pd.DataFrame, *, score_name: str) -> pd.DataFrame:
    required = {"signal_date", "l2_code_id", "sector_score"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{score_name} sector scores are missing columns: {missing}")
    result = frame.loc[:, sorted(required)].copy()
    result["signal_date"] = pd.to_datetime(result["signal_date"], errors="coerce").dt.normalize()
    result["l2_code_id"] = pd.to_numeric(result["l2_code_id"], errors="coerce")
    result["sector_score"] = pd.to_numeric(result["sector_score"], errors="coerce")
    invalid = (
        result["signal_date"].isna()
        | result["l2_code_id"].isna()
        | result["l2_code_id"].lt(0)
        | result.duplicated(["signal_date", "l2_code_id"])
    )
    if bool(invalid.any()):
        raise ValueError(f"{score_name} sector scores contain invalid identities")
    result["l2_code_id"] = result["l2_code_id"].astype("int64")
    return result.rename(columns={"sector_score": score_name})


def _extract_strategy_daily(oracle_daily: pd.DataFrame) -> pd.DataFrame:
    comparison_columns = {
        "signal_date",
        "baseline_net_forward_return_proxy",
        "sector_soft_net_forward_return_proxy",
    }
    if comparison_columns.issubset(oracle_daily.columns):
        result = oracle_daily.loc[:, sorted(comparison_columns)].copy()
        result["signal_date"] = pd.to_datetime(
            result["signal_date"], errors="coerce"
        ).dt.normalize()
        if result["signal_date"].isna().any() or result.duplicated("signal_date").any():
            raise ValueError("sector strategy comparison has invalid or duplicate dates")
        for column in (
            "baseline_net_forward_return_proxy",
            "sector_soft_net_forward_return_proxy",
        ):
            result[column] = pd.to_numeric(result[column], errors="coerce")
        if not np.isfinite(
            result[
                [
                    "baseline_net_forward_return_proxy",
                    "sector_soft_net_forward_return_proxy",
                ]
            ].to_numpy(dtype="float64")
        ).all():
            raise ValueError("sector strategy comparison contains non-finite returns")
        return result.sort_values("signal_date", kind="mergesort").reset_index(
            drop=True
        )

    required = {"signal_date", "cell", "mode", "net_forward_return_proxy"}
    missing = sorted(required - set(oracle_daily.columns))
    if missing:
        raise ValueError(f"sector oracle daily artifact is missing columns: {missing}")
    frame = oracle_daily.copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.normalize()
    baseline = frame.loc[
        frame["cell"].eq("one_layer_reality") & frame["mode"].eq("one_layer"),
        ["signal_date", "net_forward_return_proxy"],
    ].rename(columns={"net_forward_return_proxy": "baseline_net_forward_return_proxy"})
    overlay = frame.loc[
        frame["cell"].eq("reality_sector__reality_stock") & frame["mode"].eq("soft"),
        ["signal_date", "net_forward_return_proxy"],
    ].rename(columns={"net_forward_return_proxy": "sector_soft_net_forward_return_proxy"})
    if baseline.duplicated("signal_date").any() or overlay.duplicated("signal_date").any():
        raise ValueError("sector oracle daily artifact has duplicate strategy/date rows")
    result = baseline.merge(overlay, on="signal_date", how="inner", validate="one_to_one")
    if result.empty:
        raise ValueError("sector oracle daily artifact has no comparable strategy dates")
    return result.sort_values("signal_date", kind="mergesort").reset_index(drop=True)


def _slice_metric(
    frame: pd.DataFrame,
    *,
    config: SectorAgreementRouterConfig,
    seed_offset: int,
) -> dict[str, Any]:
    incremental = frame["incremental_net_return_proxy"].dropna().astype("float64")
    return {
        "day_count": int(len(frame)),
        "route_to_sector_day_count": int(frame["route_to_sector_soft"].sum()),
        "route_to_sector_rate": float(frame["route_to_sector_soft"].mean()),
        "evidence_available_rate": float(frame["route_evidence_available"].mean()),
        "baseline_mean_net_forward_return_proxy": float(
            frame["baseline_net_forward_return_proxy"].mean()
        ),
        "sector_soft_mean_net_forward_return_proxy": float(
            frame["sector_soft_net_forward_return_proxy"].mean()
        ),
        "routed_mean_net_forward_return_proxy": float(
            frame["routed_net_forward_return_proxy"].mean()
        ),
        "mean_incremental_net_return_proxy": float(incremental.mean()),
        "incremental_newey_west": newey_west_mean_test(
            incremental.tolist(), lag=max(config.horizon - 1, 0)
        ),
        "incremental_moving_block_bootstrap": moving_block_bootstrap_mean(
            incremental.tolist(),
            block_length=max(config.horizon, 1),
            samples=config.bootstrap_samples,
            seed=config.bootstrap_seed + seed_offset,
        ),
        "research_decision": None,
        "research_note": "current QE routing evidence; no direction elimination",
    }


def compute_sector_agreement_router(
    regression_scores: pd.DataFrame,
    breadth_scores: pd.DataFrame,
    oracle_daily: pd.DataFrame,
    *,
    config: SectorAgreementRouterConfig,
) -> SectorAgreementRouterResult:
    regression = _normalize_scores(regression_scores, score_name="regression_score")
    breadth = _normalize_scores(breadth_scores, score_name="breadth_score")
    joined = regression.merge(
        breadth,
        on=["signal_date", "l2_code_id"],
        how="inner",
        validate="one_to_one",
    )
    correlations = (
        joined.groupby("signal_date", sort=True)
        .apply(
            lambda day: day["regression_score"].corr(
                day["breadth_score"], method="spearman"
            ),
            include_groups=False,
        )
        .rename("candidate_rank_agreement")
        .reset_index()
    )
    correlations = correlations.sort_values("signal_date", kind="mergesort")
    correlations["agreement_threshold"] = (
        correlations["candidate_rank_agreement"]
        .shift(1)
        .rolling(config.lookback, min_periods=config.min_periods)
        .quantile(config.agreement_quantile)
    )
    correlations["route_evidence_available"] = (
        np.isfinite(correlations["candidate_rank_agreement"])
        & np.isfinite(correlations["agreement_threshold"])
    )
    correlations["route_to_sector_soft"] = (
        correlations["route_evidence_available"]
        & correlations["candidate_rank_agreement"].ge(
            correlations["agreement_threshold"]
        )
    )

    strategies = _extract_strategy_daily(oracle_daily)
    daily = strategies.merge(
        correlations,
        on="signal_date",
        how="left",
        validate="one_to_one",
    )
    daily["route_evidence_available"] = daily["route_evidence_available"].fillna(False)
    daily["route_to_sector_soft"] = daily["route_to_sector_soft"].fillna(False)
    daily["routed_net_forward_return_proxy"] = np.where(
        daily["route_to_sector_soft"],
        daily["sector_soft_net_forward_return_proxy"],
        daily["baseline_net_forward_return_proxy"],
    )
    daily["incremental_net_return_proxy"] = (
        daily["routed_net_forward_return_proxy"]
        - daily["baseline_net_forward_return_proxy"]
    )
    daily["route_reason"] = np.select(
        [
            ~daily["route_evidence_available"],
            daily["route_to_sector_soft"],
        ],
        [
            "baseline_cold_start_or_missing_observable_state",
            "sector_soft_candidate_agreement_above_trailing_quantile",
        ],
        default="baseline_candidate_agreement_below_trailing_quantile",
    )

    slices = {
        "all_test": pd.Series(True, index=daily.index),
        "2024H2": daily["signal_date"].between("2024-07-01", "2024-12-31"),
        "2025": daily["signal_date"].between("2025-01-01", "2025-12-31"),
        "2026H1": daily["signal_date"].between("2026-01-01", "2026-06-30"),
    }
    metrics = [
        {
            "slice": name,
            "classification": SECTOR_ROUTER_CLASSIFICATION,
            **_slice_metric(daily.loc[mask], config=config, seed_offset=index),
        }
        for index, (name, mask) in enumerate(slices.items(), start=1)
    ]
    audit = {
        "regression_score_rows": int(len(regression)),
        "breadth_score_rows": int(len(breadth)),
        "matched_sector_score_rows": int(len(joined)),
        "score_date_count": int(joined["signal_date"].nunique()),
        "strategy_date_count": int(len(strategies)),
        "routed_date_count": int(len(daily)),
        "cold_start_or_missing_evidence_days": int(
            (~daily["route_evidence_available"]).sum()
        ),
        "causal_contract": (
            "signal_date_score_rank_agreement_compared_with_shifted_trailing_"
            "agreement_quantile; no future outcome in routing"
        ),
        "cold_start_policy": "explicit_stock_only_baseline",
    }
    return SectorAgreementRouterResult(daily=daily, metrics=metrics, audit=audit)


def build_observable_sector_state(
    score_frames: dict[str, pd.DataFrame],
    *,
    top_m: int = 5,
) -> pd.DataFrame:
    """Build current-date regime features from multiple real sector scorers."""

    if top_m <= 0:
        raise ValueError("observable sector state top_m must be positive")
    if len(score_frames) < 2:
        raise ValueError("observable sector state requires at least two score families")
    normalized: dict[str, pd.DataFrame] = {}
    for raw_name, frame in score_frames.items():
        name = str(raw_name).strip().lower()
        if not name or not name.replace("_", "").isalnum():
            raise ValueError(f"invalid observable sector score name: {raw_name!r}")
        if name in normalized:
            raise ValueError(f"duplicate observable sector score name: {name}")
        normalized[name] = _normalize_scores(frame, score_name=name)

    merged: pd.DataFrame | None = None
    for name, frame in normalized.items():
        current = frame.rename(columns={name: f"score__{name}"})
        merged = (
            current
            if merged is None
            else merged.merge(
                current,
                on=["signal_date", "l2_code_id"],
                how="inner",
                validate="one_to_one",
            )
        )
    if merged is None or merged.empty:
        raise ValueError("observable sector score families have no common rows")

    names = sorted(normalized)
    score_columns = [f"score__{name}" for name in names]
    records: list[dict[str, Any]] = []
    for signal_date, raw_day in merged.groupby("signal_date", sort=True):
        source_sector_count = int(len(raw_day))
        finite_rows = np.isfinite(
            raw_day[score_columns].apply(pd.to_numeric, errors="coerce")
        ).all(axis=1)
        day = raw_day.loc[finite_rows].copy()
        if len(day) <= top_m:
            raise ValueError(
                f"observable sector state date={signal_date} has only {len(day)} "
                "common finite sector rows"
            )
        record: dict[str, Any] = {
            "signal_date": pd.Timestamp(signal_date),
            "matched_sector_count": int(len(day)),
            "source_sector_count": source_sector_count,
            "score_coverage_ratio": float(len(day) / source_sector_count),
        }
        top_sets: dict[str, set[int]] = {}
        for name in names:
            column = f"score__{name}"
            values = pd.to_numeric(day[column], errors="coerce")
            ordered = day.assign(_score=values).sort_values(
                ["_score", "l2_code_id"],
                ascending=[False, True],
                kind="mergesort",
            )
            top = ordered.head(top_m)
            remainder = ordered.iloc[top_m:]
            top_sets[name] = set(top["l2_code_id"].astype("int64"))
            record[f"{name}__score_std"] = float(values.std(ddof=0))
            record[f"{name}__score_iqr"] = float(values.quantile(0.75) - values.quantile(0.25))
            record[f"{name}__top_m_margin"] = float(
                top["_score"].mean() - remainder["_score"].mean()
            )
            record[f"{name}__top1_top2_gap"] = float(
                ordered["_score"].iloc[0] - ordered["_score"].iloc[1]
            )
        for left_index, left in enumerate(names):
            for right in names[left_index + 1 :]:
                correlation = day[f"score__{left}"].corr(
                    day[f"score__{right}"], method="spearman"
                )
                if not np.isfinite(correlation):
                    raise ValueError(
                        f"observable sector state correlation {left}/{right} "
                        f"is not finite on {signal_date}"
                    )
                union = top_sets[left] | top_sets[right]
                record[f"{left}__{right}__rank_agreement"] = float(correlation)
                record[f"{left}__{right}__top_m_jaccard"] = float(
                    len(top_sets[left] & top_sets[right]) / len(union)
                )
        records.append(record)
    result = pd.DataFrame.from_records(records).sort_values(
        "signal_date", kind="mergesort"
    )
    if result.duplicated("signal_date").any():
        raise ValueError("observable sector state contains duplicate signal dates")
    return result.reset_index(drop=True)


def _fit_ridge(
    train_features: np.ndarray,
    train_target: np.ndarray,
    current_features: np.ndarray,
    *,
    alpha: float,
) -> tuple[float, np.ndarray, np.ndarray, np.ndarray]:
    mean = train_features.mean(axis=0)
    scale = train_features.std(axis=0, ddof=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    standardized = (train_features - mean) / scale
    design = np.column_stack([np.ones(len(standardized)), standardized])
    penalty = np.eye(design.shape[1], dtype="float64") * float(alpha)
    penalty[0, 0] = 0.0
    gram = design.T @ design + penalty
    rhs = design.T @ train_target
    try:
        coefficients = np.linalg.solve(gram, rhs)
    except np.linalg.LinAlgError as exc:
        raise ValueError("walk-forward ridge system is singular") from exc
    current_standardized = (current_features - mean) / scale
    prediction = float(
        np.concatenate([[1.0], current_standardized]) @ coefficients
    )
    return prediction, coefficients, mean, scale


def compute_sector_walk_forward_router(
    observable_state: pd.DataFrame,
    oracle_daily: pd.DataFrame,
    *,
    config: SectorWalkForwardRouterConfig,
) -> SectorWalkForwardRouterResult:
    """Route with an expanding model trained only on already-mature outcomes."""

    if "signal_date" not in observable_state:
        raise ValueError("observable sector state is missing signal_date")
    state = observable_state.copy()
    state["signal_date"] = pd.to_datetime(
        state["signal_date"], errors="coerce"
    ).dt.normalize()
    if state["signal_date"].isna().any() or state.duplicated("signal_date").any():
        raise ValueError("observable sector state contains invalid signal dates")
    feature_columns = [
        column
        for column in state.columns
        if column not in {"signal_date", "matched_sector_count"}
    ]
    if not feature_columns:
        raise ValueError("observable sector state has no model features")
    for column in feature_columns:
        state[column] = pd.to_numeric(state[column], errors="coerce")
    if not np.isfinite(state[feature_columns].to_numpy(dtype="float64")).all():
        raise ValueError("observable sector state contains non-finite model features")

    strategies = _extract_strategy_daily(oracle_daily)
    daily = strategies.merge(
        state,
        on="signal_date",
        how="inner",
        validate="one_to_one",
    ).sort_values("signal_date", kind="mergesort").reset_index(drop=True)
    if daily.empty:
        raise ValueError("walk-forward router has no common strategy/feature dates")
    daily["realized_incremental_net_return_proxy"] = (
        daily["sector_soft_net_forward_return_proxy"]
        - daily["baseline_net_forward_return_proxy"]
    )
    predicted = np.full(len(daily), np.nan, dtype="float64")
    train_counts = np.zeros(len(daily), dtype="int64")
    coefficient_rows: list[dict[str, Any]] = []
    feature_matrix = daily[feature_columns].to_numpy(dtype="float64")
    target = daily["realized_incremental_net_return_proxy"].to_numpy(
        dtype="float64"
    )
    for current_index in range(len(daily)):
        # T signal enters at T+1 and the h-day outcome ends at T+h+1.  At the
        # current signal close, only rows ending on or before today are known.
        mature_end_index = current_index - config.horizon - 1
        if mature_end_index < 0:
            continue
        train_index = np.arange(mature_end_index + 1)
        finite_target = np.isfinite(target[train_index])
        train_index = train_index[finite_target]
        if len(train_index) < config.min_train_days:
            continue
        prediction, coefficients, means, scales = _fit_ridge(
            feature_matrix[train_index],
            target[train_index],
            feature_matrix[current_index],
            alpha=config.ridge_alpha,
        )
        predicted[current_index] = prediction
        train_counts[current_index] = len(train_index)
        coefficient_rows.append(
            {
                "signal_date": daily.at[current_index, "signal_date"],
                "mature_training_end_date": daily.at[
                    mature_end_index, "signal_date"
                ],
                "training_day_count": int(len(train_index)),
                "intercept": float(coefficients[0]),
                **{
                    f"coefficient__{column}": float(coefficients[index + 1])
                    for index, column in enumerate(feature_columns)
                },
                **{
                    f"training_mean__{column}": float(means[index])
                    for index, column in enumerate(feature_columns)
                },
                **{
                    f"training_scale__{column}": float(scales[index])
                    for index, column in enumerate(feature_columns)
                },
            }
        )

    daily["predicted_incremental_net_return_proxy"] = predicted
    daily["walk_forward_training_day_count"] = train_counts
    daily["route_evidence_available"] = np.isfinite(predicted)
    daily["route_to_sector_soft"] = (
        daily["route_evidence_available"]
        & daily["predicted_incremental_net_return_proxy"].gt(
            config.route_threshold
        )
    )
    daily["routed_net_forward_return_proxy"] = np.where(
        daily["route_to_sector_soft"],
        daily["sector_soft_net_forward_return_proxy"],
        daily["baseline_net_forward_return_proxy"],
    )
    daily["incremental_net_return_proxy"] = (
        daily["routed_net_forward_return_proxy"]
        - daily["baseline_net_forward_return_proxy"]
    )
    daily["route_reason"] = np.select(
        [
            ~daily["route_evidence_available"],
            daily["route_to_sector_soft"],
        ],
        [
            "baseline_until_mature_training_history_available",
            "sector_soft_predicted_incremental_return_positive",
        ],
        default="baseline_predicted_incremental_return_non_positive",
    )

    metric_config = SectorAgreementRouterConfig(
        horizon=config.horizon,
        lookback=max(config.min_train_days, 2),
        min_periods=max(min(config.min_train_days, 60), 2),
        agreement_quantile=0.5,
        bootstrap_samples=config.bootstrap_samples,
        bootstrap_seed=config.bootstrap_seed,
    )
    slices = {
        "all_test": pd.Series(True, index=daily.index),
        "2024H2": daily["signal_date"].between("2024-07-01", "2024-12-31"),
        "2025": daily["signal_date"].between("2025-01-01", "2025-12-31"),
        "2026H1": daily["signal_date"].between("2026-01-01", "2026-06-30"),
    }
    metrics = [
        {
            "slice": name,
            "classification": "REAL_QE_WALK_FORWARD_SECTOR_ROUTER",
            **_slice_metric(
                daily.loc[mask], config=metric_config, seed_offset=index
            ),
        }
        for index, (name, mask) in enumerate(slices.items(), start=1)
    ]
    audit = {
        "horizon": int(config.horizon),
        "feature_columns": feature_columns,
        "feature_count": int(len(feature_columns)),
        "strategy_date_count": int(len(strategies)),
        "observable_state_date_count": int(len(state)),
        "matched_date_count": int(len(daily)),
        "route_evidence_day_count": int(daily["route_evidence_available"].sum()),
        "first_route_evidence_date": (
            daily.loc[daily["route_evidence_available"], "signal_date"].min().isoformat()
            if daily["route_evidence_available"].any()
            else None
        ),
        "maturity_delay_trading_days": int(config.horizon + 1),
        "min_train_days": int(config.min_train_days),
        "ridge_alpha": float(config.ridge_alpha),
        "route_threshold": float(config.route_threshold),
        "causal_contract": (
            "current_date_sector_score_shape_features; expanding ridge fits use "
            "only outcomes whose T_plus_h_plus_1 date is no later than the "
            "current signal date"
        ),
        "research_note": (
            "online expanding horizon-specific hypothesis probe; current trial "
            "evidence does not eliminate any routing direction"
        ),
    }
    return SectorWalkForwardRouterResult(
        daily=daily,
        coefficients=pd.DataFrame.from_records(coefficient_rows),
        metrics=metrics,
        audit=audit,
    )
