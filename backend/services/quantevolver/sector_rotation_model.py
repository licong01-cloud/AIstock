"""QE-only sector-rotation model primitives.

The module builds a PIT Shenwan L2 sector panel, aggregates stock-level factor
values to sector-level breadth/rank features, trains real (non-oracle) sector
models, and emits out-of-sample sector scores.  It has no database, selection,
paper-trading, advisory, or live-trading side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


REAL_SECTOR_MODEL_CLASSIFICATION = "REAL_QE_SECTOR_MODEL"

_STATIC_COLUMNS = (
    "datetime",
    "instrument",
    "l2_code_id",
    "sw2_close",
    "sw2_amount",
    "sw2_vol",
    "sw2_mf_net_amt",
    "sw2_mf_net_vol",
    "sw2_total_mv",
    "sw2_pb",
    "sw2_pe",
)


@dataclass(frozen=True)
class SectorModelConfig:
    horizon: int
    train_start: str = "2018-08-01"
    train_end: str = "2022-12-31"
    valid_start: str = "2023-01-01"
    valid_end: str = "2024-06-30"
    test_start: str = "2024-07-01"
    test_end: str = "2026-06-29"
    top_m: int = 5
    n_estimators: int = 600
    early_stopping_rounds: int = 50
    learning_rate: float = 0.03
    num_leaves: int = 31
    max_depth: int = 6
    min_child_samples: int = 40

    def __post_init__(self) -> None:
        if self.horizon <= 0:
            raise ValueError("sector model horizon must be positive")
        if self.top_m <= 0:
            raise ValueError("sector model top_m must be positive")
        dates = [
            pd.Timestamp(self.train_start),
            pd.Timestamp(self.train_end),
            pd.Timestamp(self.valid_start),
            pd.Timestamp(self.valid_end),
            pd.Timestamp(self.test_start),
            pd.Timestamp(self.test_end),
        ]
        if not (dates[0] <= dates[1] < dates[2] <= dates[3] < dates[4] <= dates[5]):
            raise ValueError("sector model train/valid/test date ranges overlap or are unordered")


@dataclass
class SectorPanelBuildResult:
    membership: pd.DataFrame
    sector_base: pd.DataFrame
    audit: dict[str, Any]


@dataclass
class SectorModelResult:
    predictions: pd.DataFrame
    ensemble_scores: pd.DataFrame
    metrics: list[dict[str, Any]]
    feature_importance: pd.DataFrame
    models: dict[str, Any]
    feature_columns: tuple[str, ...]
    data_audit: dict[str, Any]


def _require_columns(frame: pd.DataFrame, required: Iterable[str], *, name: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}")


def _normalize_datetime(value: pd.Series) -> pd.Series:
    return pd.to_datetime(value, errors="coerce").dt.normalize()


def build_sector_base(static_frame: pd.DataFrame) -> SectorPanelBuildResult:
    """Build daily PIT membership and one row per SW2 sector/date.

    Conflicting repeated sector-index values are not hidden: the deterministic
    median is used for continued research and conflict counts are returned in
    the audit.  Ambiguous stock/date memberships are excluded and counted.
    """

    source_row_count = int(len(static_frame))
    frame = (
        static_frame.reset_index()
        if not {"datetime", "instrument"}.issubset(static_frame.columns)
        else static_frame.copy()
    )
    _require_columns(frame, _STATIC_COLUMNS, name="static factor frame")
    frame = frame.loc[:, list(_STATIC_COLUMNS)].copy()
    frame["datetime"] = _normalize_datetime(frame["datetime"])
    frame["instrument"] = frame["instrument"].astype("string")
    frame["l2_code_id"] = pd.to_numeric(frame["l2_code_id"], errors="coerce")
    negative_sector_id = frame["l2_code_id"].lt(0).fillna(False)
    invalid_identity = (
        frame["datetime"].isna()
        | frame["instrument"].isna()
        | frame["l2_code_id"].isna()
        | negative_sector_id
    )
    frame = frame.loc[~invalid_identity].copy()
    frame["l2_code_id"] = frame["l2_code_id"].astype("int64")

    membership_counts = (
        frame.groupby(["datetime", "instrument"], sort=False)["l2_code_id"]
        .nunique()
        .rename("membership_count")
    )
    ambiguous_keys = membership_counts.loc[membership_counts.gt(1)].index
    if len(ambiguous_keys):
        identity = pd.MultiIndex.from_frame(frame.loc[:, ["datetime", "instrument"]])
        frame = frame.loc[~identity.isin(ambiguous_keys)].copy()

    membership = (
        frame.loc[:, ["datetime", "instrument", "l2_code_id"]]
        .drop_duplicates(["datetime", "instrument"], keep="last")
        .sort_values(["datetime", "instrument"], kind="mergesort")
        .reset_index(drop=True)
    )

    value_columns = [column for column in _STATIC_COLUMNS if column.startswith("sw2_")]
    grouped = frame.groupby(["datetime", "l2_code_id"], sort=True)
    sector_base = grouped[value_columns].median().reset_index()
    member_count = grouped["instrument"].nunique().rename("member_count").reset_index()
    sector_base = sector_base.merge(
        member_count,
        on=["datetime", "l2_code_id"],
        how="left",
        validate="one_to_one",
    )

    conflict_counts: dict[str, int] = {}
    for column in value_columns:
        finite = frame.loc[np.isfinite(pd.to_numeric(frame[column], errors="coerce"))]
        if finite.empty:
            conflict_counts[column] = 0
            continue
        bounds = finite.groupby(["datetime", "l2_code_id"], sort=False)[column].agg(
            ["min", "max"]
        )
        scale = np.maximum(np.maximum(np.abs(bounds["min"]), np.abs(bounds["max"])), 1.0)
        conflict_counts[column] = int(
            ((bounds["max"] - bounds["min"]).abs() > scale * 1e-7).sum()
        )

    sector_base = sector_base.sort_values(
        ["l2_code_id", "datetime"], kind="mergesort"
    ).reset_index(drop=True)
    audit = {
        "input_rows": source_row_count,
        "invalid_identity_rows": int(invalid_identity.sum()),
        "negative_sector_id_rows": int(negative_sector_id.sum()),
        "ambiguous_stock_date_memberships": int(len(ambiguous_keys)),
        "membership_rows": int(len(membership)),
        "sector_day_rows": int(len(sector_base)),
        "sector_count": int(sector_base["l2_code_id"].nunique()),
        "date_start": sector_base["datetime"].min().isoformat(),
        "date_end": sector_base["datetime"].max().isoformat(),
        "repeated_sector_value_conflicts": conflict_counts,
        "conflict_resolution": "deterministic_group_median_with_explicit_audit",
    }
    return SectorPanelBuildResult(membership=membership, sector_base=sector_base, audit=audit)


def aggregate_factor_to_sector(
    factor_frame: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    factor_name: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Aggregate one stock factor to scale-free sector breadth features."""

    frame = factor_frame.reset_index() if not {"datetime", "instrument"}.issubset(
        factor_frame.columns
    ) else factor_frame.copy()
    _require_columns(frame, ("datetime", "instrument", "value"), name=factor_name)
    frame = frame.loc[:, ["datetime", "instrument", "value"]].copy()
    frame["datetime"] = _normalize_datetime(frame["datetime"])
    frame["instrument"] = frame["instrument"].astype("string")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    duplicate_count = int(frame.duplicated(["datetime", "instrument"], keep=False).sum())
    if duplicate_count:
        frame = (
            frame.groupby(["datetime", "instrument"], sort=False, as_index=False)["value"]
            .mean()
        )
    merged = frame.merge(
        membership,
        on=["datetime", "instrument"],
        how="inner",
        validate="one_to_one",
    )
    finite = np.isfinite(merged["value"])
    merged = merged.loc[finite].copy()
    merged["rank_pct"] = merged.groupby("datetime", sort=False)["value"].rank(
        method="average", pct=True
    )
    merged["top_quintile"] = merged["rank_pct"].ge(0.8).astype("float64")
    prefix = str(factor_name)
    aggregate = (
        merged.groupby(["datetime", "l2_code_id"], sort=True)
        .agg(
            **{
                f"{prefix}__mean_rank": ("rank_pct", "mean"),
                f"{prefix}__rank_dispersion": ("rank_pct", "std"),
                f"{prefix}__top_quintile_share": ("top_quintile", "mean"),
                f"{prefix}__finite_count": ("rank_pct", "size"),
            }
        )
        .reset_index()
    )
    audit = {
        "factor_name": prefix,
        "input_rows": int(len(factor_frame)),
        "duplicate_identity_rows": duplicate_count,
        "matched_rows": int(len(merged)),
        "matched_sector_days": int(len(aggregate)),
        "date_start": merged["datetime"].min().isoformat() if len(merged) else None,
        "date_end": merged["datetime"].max().isoformat() if len(merged) else None,
    }
    return aggregate, audit


def engineer_sector_panel(
    sector_base: pd.DataFrame,
    factor_aggregates: Mapping[str, pd.DataFrame],
    *,
    horizon: int,
) -> tuple[pd.DataFrame, tuple[str, ...], dict[str, Any]]:
    """Create causal sector features and the T+1 -> T+h+1 sector target."""

    if horizon <= 0:
        raise ValueError("horizon must be positive")
    _require_columns(
        sector_base,
        ("datetime", "l2_code_id", "sw2_close", "sw2_amount", "sw2_mf_net_amt"),
        name="sector base",
    )
    normalized_base = sector_base.copy()
    normalized_base["datetime"] = _normalize_datetime(normalized_base["datetime"])
    dates = pd.Index(sorted(normalized_base["datetime"].dropna().unique()))
    sectors = pd.Index(sorted(normalized_base["l2_code_id"].dropna().unique()))
    complete_calendar = pd.MultiIndex.from_product(
        [sectors, dates], names=["l2_code_id", "datetime"]
    ).to_frame(index=False)
    panel = complete_calendar.merge(
        normalized_base,
        on=["l2_code_id", "datetime"],
        how="left",
        validate="one_to_one",
    ).sort_values(
        ["l2_code_id", "datetime"], kind="mergesort"
    )
    missing_sector_calendar_rows = int(panel["sw2_close"].isna().sum())
    grouped = panel.groupby("l2_code_id", sort=False, group_keys=False)
    for window in (5, 10, 20, 40, 60, 120):
        panel[f"sector_return_{window}d"] = grouped["sw2_close"].transform(
            lambda values, w=window: values / values.shift(w) - 1.0
        )
    daily_return = grouped["sw2_close"].pct_change(fill_method=None)
    panel["sector_volatility_20d"] = daily_return.groupby(panel["l2_code_id"]).transform(
        lambda values: values.rolling(20, min_periods=10).std()
    )
    panel["sector_volatility_60d"] = daily_return.groupby(panel["l2_code_id"]).transform(
        lambda values: values.rolling(60, min_periods=30).std()
    )
    panel["sector_drawdown_60d"] = grouped["sw2_close"].transform(
        lambda values: values / values.rolling(60, min_periods=20).max() - 1.0
    )
    amount = pd.to_numeric(panel["sw2_amount"], errors="coerce")
    panel["sector_log_amount"] = np.log1p(amount.clip(lower=0))
    for window in (5, 20):
        amount_mean = amount.groupby(panel["l2_code_id"]).transform(
            lambda values, w=window: values.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        panel[f"sector_amount_ratio_{window}d"] = amount / amount_mean.replace(0, np.nan)
    flow = pd.to_numeric(panel["sw2_mf_net_amt"], errors="coerce")
    panel["sector_flow_amount_ratio_1d"] = flow / amount.abs().replace(0, np.nan)
    panel["sector_flow_amount_ratio_5d"] = flow.groupby(panel["l2_code_id"]).transform(
        lambda values: values.rolling(5, min_periods=3).sum()
    ) / amount.groupby(panel["l2_code_id"]).transform(
        lambda values: values.abs().rolling(5, min_periods=3).sum()
    ).replace(0, np.nan)
    panel["sector_flow_amount_ratio_20d"] = flow.groupby(panel["l2_code_id"]).transform(
        lambda values: values.rolling(20, min_periods=10).sum()
    ) / amount.groupby(panel["l2_code_id"]).transform(
        lambda values: values.abs().rolling(20, min_periods=10).sum()
    ).replace(0, np.nan)

    raw_rank_columns = [
        "sw2_pb",
        "sw2_pe",
        "sw2_total_mv",
        "sector_return_5d",
        "sector_return_20d",
        "sector_return_60d",
        "sector_drawdown_60d",
        "sector_amount_ratio_20d",
        "sector_flow_amount_ratio_20d",
    ]
    for column in raw_rank_columns:
        if column in panel:
            panel[f"{column}__cs_rank"] = panel.groupby("datetime", sort=False)[column].rank(
                method="average", pct=True
            )
    panel["member_count__cs_rank"] = panel.groupby("datetime", sort=False)[
        "member_count"
    ].rank(method="average", pct=True)

    factor_feature_columns: list[str] = []
    factor_audit: dict[str, Any] = {}
    for factor_name, aggregate in factor_aggregates.items():
        _require_columns(aggregate, ("datetime", "l2_code_id"), name=factor_name)
        feature_columns = [
            column
            for column in aggregate.columns
            if column not in {"datetime", "l2_code_id"}
        ]
        panel = panel.merge(
            aggregate,
            on=["datetime", "l2_code_id"],
            how="left",
            validate="one_to_one",
        )
        factor_feature_columns.extend(feature_columns)
        factor_audit[factor_name] = {
            "sector_day_rows": int(len(aggregate)),
            "feature_columns": feature_columns,
        }

    panel = panel.sort_values(["l2_code_id", "datetime"], kind="mergesort")
    grouped = panel.groupby("l2_code_id", sort=False, group_keys=False)
    panel["entry_date"] = grouped["datetime"].shift(-1)
    panel["label_end_date"] = grouped["datetime"].shift(-(horizon + 1))
    entry_close = grouped["sw2_close"].shift(-1)
    exit_close = grouped["sw2_close"].shift(-(horizon + 1))
    panel["target_return"] = exit_close / entry_close - 1.0
    panel["target_rank"] = panel.groupby("datetime", sort=False)["target_return"].rank(
        method="average", pct=True
    )

    # Keep the model causal and scale-stable across market eras.  Raw amount,
    # volume, valuation and market-cap levels are retained in the artifact for
    # audit, but the model receives returns/ratios/volatility, daily
    # cross-sectional ranks, and scale-free factor breadth summaries only.
    candidate_features = [
        column
        for column in panel.columns
        if (
            column.startswith("sector_return_")
            or column.startswith("sector_volatility_")
            or column.startswith("sector_drawdown_")
            or column.startswith("sector_amount_ratio_")
            or column.startswith("sector_flow_amount_ratio_")
            or column.endswith("__cs_rank")
            or column in factor_feature_columns
        )
        and not column.endswith("__finite_count")
    ]
    audit = {
        "horizon": int(horizon),
        "panel_rows": int(len(panel)),
        "sector_count": int(panel["l2_code_id"].nunique()),
        "date_count": int(panel["datetime"].nunique()),
        "complete_calendar_rows": int(len(complete_calendar)),
        "missing_sector_calendar_rows": missing_sector_calendar_rows,
        "mature_target_rows": int(np.isfinite(panel["target_return"]).sum()),
        "factor_aggregates": factor_audit,
        "candidate_feature_count": int(len(candidate_features)),
        "feature_policy": (
            "causal_scale_stable_returns_ratios_volatility_cross_sectional_ranks_"
            "and_factor_breadth; raw_levels_audit_only"
        ),
    }
    return panel.reset_index(drop=True), tuple(candidate_features), audit


def _segment(
    panel: pd.DataFrame,
    *,
    start: str,
    end: str,
    require_mature_within_segment: bool,
) -> pd.DataFrame:
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp(end)
    mask = panel["datetime"].between(start_date, end_date)
    if require_mature_within_segment:
        mask &= panel["label_end_date"].notna() & panel["label_end_date"].le(end_date)
        mask &= np.isfinite(panel["target_rank"])
    return panel.loc[mask].copy()


def _daily_metrics(
    frame: pd.DataFrame,
    *,
    score_column: str,
    top_m: int,
) -> dict[str, Any]:
    mature = frame.loc[
        np.isfinite(frame[score_column]) & np.isfinite(frame["target_return"])
    ].copy()
    daily: list[dict[str, float]] = []
    for _, day in mature.groupby("datetime", sort=True):
        if len(day) < 3:
            continue
        ic = day[score_column].corr(day["target_return"], method="pearson")
        rank_ic = day[score_column].corr(day["target_return"], method="spearman")
        predicted = set(
            day.nlargest(top_m, score_column, keep="all")["l2_code_id"].head(top_m)
        )
        actual = set(
            day.nlargest(top_m, "target_return", keep="all")["l2_code_id"].head(top_m)
        )
        daily.append(
            {
                "ic": float(ic),
                "rank_ic": float(rank_ic),
                "recall_at_m": float(len(predicted & actual) / max(len(actual), 1)),
            }
        )
    daily_frame = pd.DataFrame.from_records(daily)
    return {
        "row_count": int(len(mature)),
        "date_count": int(len(daily_frame)),
        "ic_mean": float(daily_frame["ic"].mean()) if len(daily_frame) else None,
        "rank_ic_mean": float(daily_frame["rank_ic"].mean()) if len(daily_frame) else None,
        "rank_ic_std": float(daily_frame["rank_ic"].std(ddof=1)) if len(daily_frame) > 1 else None,
        "recall_at_m_mean": float(daily_frame["recall_at_m"].mean()) if len(daily_frame) else None,
    }


def _rank_groups(frame: pd.DataFrame) -> list[int]:
    return (
        frame.groupby("datetime", sort=False).size().astype("int64").tolist()
    )


def train_sector_model_suite(
    panel: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    config: SectorModelConfig,
    seeds: Sequence[int],
    model_kinds: Sequence[str] = ("lgbm_regression", "lambdarank"),
) -> SectorModelResult:
    """Train real sector models and return per-model plus ensemble OOS scores."""

    try:
        import lightgbm as lgb
    except ImportError as exc:  # pragma: no cover - deployment environment fact
        raise RuntimeError("lightgbm is required for QE sector-model research") from exc

    train = _segment(
        panel,
        start=config.train_start,
        end=config.train_end,
        require_mature_within_segment=True,
    ).sort_values(["datetime", "l2_code_id"], kind="mergesort")
    valid = _segment(
        panel,
        start=config.valid_start,
        end=config.valid_end,
        require_mature_within_segment=True,
    ).sort_values(["datetime", "l2_code_id"], kind="mergesort")
    test = _segment(
        panel,
        start=config.test_start,
        end=config.test_end,
        require_mature_within_segment=False,
    ).sort_values(["datetime", "l2_code_id"], kind="mergesort")
    if train.empty or valid.empty or test.empty:
        raise ValueError(
            f"empty sector model segment train={len(train)} valid={len(valid)} test={len(test)}"
        )

    usable_features: list[str] = []
    omitted_features: dict[str, str] = {}
    for column in feature_columns:
        if column not in panel:
            omitted_features[column] = "missing_column"
            continue
        finite_count = int(np.isfinite(pd.to_numeric(train[column], errors="coerce")).sum())
        if finite_count == 0:
            omitted_features[column] = "no_finite_training_values"
            continue
        usable_features.append(column)
    if not usable_features:
        raise ValueError("sector model has no usable causal features")

    x_train = train.loc[:, usable_features]
    x_valid = valid.loc[:, usable_features]
    x_test = test.loc[:, usable_features]
    predictions: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    importance: list[pd.DataFrame] = []
    models: dict[str, Any] = {}
    supported = {"lgbm_regression", "lambdarank"}
    unknown = sorted(set(model_kinds) - supported)
    if unknown:
        raise ValueError(f"unsupported sector model kinds: {unknown}")

    for model_kind in model_kinds:
        for seed in seeds:
            common = {
                "n_estimators": config.n_estimators,
                "learning_rate": config.learning_rate,
                "num_leaves": config.num_leaves,
                "max_depth": config.max_depth,
                "min_child_samples": config.min_child_samples,
                "subsample": 0.85,
                "colsample_bytree": 0.85,
                "reg_alpha": 0.1,
                "reg_lambda": 0.1,
                "random_state": int(seed),
                "n_jobs": -1,
                "verbosity": -1,
            }
            callbacks = [
                lgb.early_stopping(config.early_stopping_rounds, verbose=False),
                lgb.log_evaluation(period=0),
            ]
            if model_kind == "lgbm_regression":
                model = lgb.LGBMRegressor(objective="regression_l2", **common)
                model.fit(
                    x_train,
                    train["target_rank"],
                    eval_set=[(x_valid, valid["target_rank"])],
                    eval_metric="l2",
                    callbacks=callbacks,
                )
            else:
                model = lgb.LGBMRanker(
                    objective="lambdarank",
                    metric="ndcg",
                    **common,
                )
                train_relevance = np.floor(train["target_rank"] * 20).clip(0, 19).astype("int32")
                valid_relevance = np.floor(valid["target_rank"] * 20).clip(0, 19).astype("int32")
                model.fit(
                    x_train,
                    train_relevance,
                    group=_rank_groups(train),
                    eval_set=[(x_valid, valid_relevance)],
                    eval_group=[_rank_groups(valid)],
                    eval_at=[3, 5, 10],
                    callbacks=callbacks,
                )
            model_key = f"{model_kind}__seed_{int(seed)}"
            models[model_key] = model
            test_scores = np.asarray(model.predict(x_test), dtype="float64")
            valid_scores = np.asarray(model.predict(x_valid), dtype="float64")
            prediction = test.loc[:, ["datetime", "l2_code_id"]].copy()
            prediction = prediction.rename(columns={"datetime": "signal_date"})
            prediction["sector_score"] = test_scores
            prediction["model_kind"] = model_kind
            prediction["seed"] = int(seed)
            prediction["horizon"] = int(config.horizon)
            predictions.append(prediction)
            valid_eval = valid.loc[:, ["datetime", "l2_code_id", "target_return"]].copy()
            valid_eval["sector_score"] = valid_scores
            test_eval = test.loc[:, ["datetime", "l2_code_id", "target_return"]].copy()
            test_eval["sector_score"] = test_scores
            metrics.append(
                {
                    "classification": REAL_SECTOR_MODEL_CLASSIFICATION,
                    "model_kind": model_kind,
                    "seed": int(seed),
                    "horizon": int(config.horizon),
                    "best_iteration": int(getattr(model, "best_iteration_", 0) or 0),
                    "valid": _daily_metrics(
                        valid_eval, score_column="sector_score", top_m=config.top_m
                    ),
                    "test_mature_only": _daily_metrics(
                        test_eval, score_column="sector_score", top_m=config.top_m
                    ),
                    "research_decision": None,
                    "research_note": "current QE trial evidence; no direction elimination",
                }
            )
            importance.append(
                pd.DataFrame(
                    {
                        "model_kind": model_kind,
                        "seed": int(seed),
                        "horizon": int(config.horizon),
                        "feature": usable_features,
                        "importance_gain": model.booster_.feature_importance(
                            importance_type="gain"
                        ),
                        "importance_split": model.booster_.feature_importance(
                            importance_type="split"
                        ),
                    }
                )
            )

    prediction_frame = pd.concat(predictions, ignore_index=True, sort=False)
    prediction_frame["daily_score_rank"] = prediction_frame.groupby(
        ["model_kind", "seed", "signal_date"], sort=False
    )["sector_score"].rank(method="average", pct=True)
    ensemble = (
        prediction_frame.groupby(["signal_date", "l2_code_id"], sort=True)
        .agg(
            sector_score=("daily_score_rank", "mean"),
            component_count=("daily_score_rank", "count"),
        )
        .reset_index()
    )
    expected_components = len(seeds) * len(model_kinds)
    ensemble["complete_ensemble"] = ensemble["component_count"].eq(expected_components)

    mature_test = test.loc[:, ["datetime", "l2_code_id", "target_return"]].rename(
        columns={"datetime": "signal_date"}
    )
    ensemble_eval = ensemble.merge(
        mature_test,
        on=["signal_date", "l2_code_id"],
        how="left",
        validate="one_to_one",
    ).rename(columns={"signal_date": "datetime"})
    metrics.append(
        {
            "classification": REAL_SECTOR_MODEL_CLASSIFICATION,
            "model_kind": "rank_mean_ensemble",
            "seed": None,
            "horizon": int(config.horizon),
            "test_mature_only": _daily_metrics(
                ensemble_eval, score_column="sector_score", top_m=config.top_m
            ),
            "research_decision": None,
            "research_note": "current QE trial evidence; no direction elimination",
        }
    )
    data_audit = {
        "train_rows": int(len(train)),
        "valid_rows": int(len(valid)),
        "test_rows": int(len(test)),
        "train_date_count": int(train["datetime"].nunique()),
        "valid_date_count": int(valid["datetime"].nunique()),
        "test_date_count": int(test["datetime"].nunique()),
        "usable_feature_count": int(len(usable_features)),
        "omitted_features": omitted_features,
        "expected_ensemble_components": int(expected_components),
        "complete_ensemble_rows": int(ensemble["complete_ensemble"].sum()),
        "purge_contract": "label_end_date_must_not_exceed_train_or_valid_segment_end",
        "target_contract": f"sw2_close[T+{config.horizon + 1}]/sw2_close[T+1]-1",
    }
    return SectorModelResult(
        predictions=prediction_frame,
        ensemble_scores=ensemble,
        metrics=metrics,
        feature_importance=pd.concat(importance, ignore_index=True, sort=False),
        models=models,
        feature_columns=tuple(usable_features),
        data_audit=data_audit,
    )
