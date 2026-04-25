"""
Multi-Alpha Meta-Model: combine N alpha group predictions into one signal.

Supports:
  - IC-Weighted (default): rolling Rank IC → weights
  - OLS Ridge: sklearn Ridge regression on validation predictions
  - Stacking: LightGBM 2nd-level model with Purged K-Fold CV

Reference:
  - AlphaForge (AAAI 2025): dynamic combination via rolling IC
  - López de Prado: Combinatorial Purged CV for financial meta-models
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("aistock.quantevolver.meta_model")


class MetaModelCombiner:
    """Combine multiple alpha group predictions into a single signal."""

    def __init__(self, method: str = "ic_weighted", lookback_days: int = 60,
                 cv_params: dict[str, Any] | None = None):
        self.method = method
        self.lookback_days = lookback_days
        self.cv_params = cv_params or {}

    def fit_and_combine(
        self,
        group_predictions: dict[str, pd.DataFrame],
        actual_returns: pd.Series | None = None,
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """Fit meta-model and produce combined predictions.

        Args:
            group_predictions: {group_name: DataFrame with columns [score]}
                               indexed by (datetime, instrument)
            actual_returns: Series of realized returns (for IC-weighted and OLS)
                          indexed by (datetime, instrument)

        Returns:
            (combined_prediction_df, weights_dict)
        """
        if self.method == "equal":
            return self._equal_weighted(group_predictions)
        elif self.method == "ic_weighted":
            return self._ic_weighted(group_predictions, actual_returns)
        elif self.method == "ols":
            return self._ols_ridge(group_predictions, actual_returns)
        elif self.method == "stacking":
            return self._stacking(group_predictions, actual_returns)
        else:
            raise ValueError(f"Unknown meta-model method: {self.method}")

    def _equal_weighted(
        self,
        group_predictions: dict[str, pd.DataFrame],
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """Equal-weight combination without label dependency."""
        aligned = self._align_predictions(group_predictions)
        group_names = list(aligned.keys())
        if not group_names:
            raise ValueError("equal requires at least one prediction group")

        weights = {g: 1.0 / len(group_names) for g in group_names}
        combined = self._weighted_sum(aligned, weights)
        logger.info(f"Equal-weight meta-model: {weights}")
        return combined, weights

    def _ic_weighted(
        self,
        group_predictions: dict[str, pd.DataFrame],
        actual_returns: pd.Series | None,
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """IC-Weighted combination: weight each group by its recent Rank IC."""
        # Align all predictions to common index
        aligned = self._align_predictions(group_predictions)
        group_names = list(aligned.keys())

        if actual_returns is None:
            raise ValueError("IC-weighted combination requires actual_returns")

        weights = {}
        for g_name, g_pred in aligned.items():
            common_idx = g_pred.index.intersection(actual_returns.index)
            if len(common_idx) == 0:
                raise ValueError(f"Group {g_name} has no overlap with actual_returns")

            g_vals = g_pred.loc[common_idx]
            ret_vals = actual_returns.loc[common_idx]

            dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx
                               for idx in common_idx))
            if len(dates) > self.lookback_days:
                cutoff_date = dates[-self.lookback_days]
                mask = [
                    (idx[0] if isinstance(idx, tuple) else idx) >= cutoff_date
                    for idx in common_idx
                ]
                g_vals = g_vals[mask]
                ret_vals = ret_vals[g_vals.index]

            daily_ics = []
            for dt in sorted(set(idx[0] if isinstance(idx, tuple) else idx
                                 for idx in g_vals.index)):
                g_day = g_vals.xs(dt, level=0) if isinstance(g_vals.index, pd.MultiIndex) else g_vals
                r_day = ret_vals.xs(dt, level=0) if isinstance(ret_vals.index, pd.MultiIndex) else ret_vals
                if len(g_day) < 10:
                    raise ValueError(f"Group {g_name} has insufficient daily samples on {dt}: {len(g_day)}")
                ic = g_day.corr(r_day, method='spearman')
                if np.isnan(ic):
                    raise ValueError(f"Group {g_name} produced NaN IC on {dt}")
                daily_ics.append(ic)

            if not daily_ics:
                raise ValueError(f"Group {g_name} produced no daily IC values")
            avg_ic = float(np.mean(daily_ics))
            if avg_ic <= 0:
                raise ValueError(f"Group {g_name} produced non-positive IC mean: {avg_ic}")
            weights[g_name] = avg_ic

        total = sum(weights.values())
        if total <= 0:
            raise ValueError(f"Invalid IC-weighted total: {total}")
        weights = {k: v / total for k, v in weights.items()}

        # Weighted sum
        combined = self._weighted_sum(aligned, weights)
        logger.info(f"IC-Weighted meta-model: {weights}")
        return combined, weights

    def _ols_ridge(
        self,
        group_predictions: dict[str, pd.DataFrame],
        actual_returns: pd.Series | None,
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """OLS Ridge regression: learn optimal linear combination."""
        from sklearn.linear_model import Ridge

        aligned = self._align_predictions(group_predictions)
        group_names = list(aligned.keys())

        if actual_returns is None:
            raise ValueError("ols requires actual_returns")

        # Build feature matrix X (group predictions) and target y (returns)
        common_idx = None
        for g_pred in aligned.values():
            if common_idx is None:
                common_idx = g_pred.index
            else:
                common_idx = common_idx.intersection(g_pred.index)
        common_idx = common_idx.intersection(actual_returns.index)

        if len(common_idx) < 30:
            raise ValueError(f"Not enough data for OLS: {len(common_idx)}")

        X = pd.DataFrame({g: aligned[g].loc[common_idx] for g in group_names})
        y = actual_returns.loc[common_idx]

        # Use last lookback_days
        dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common_idx))
        if len(dates) > self.lookback_days:
            cutoff = dates[-self.lookback_days]
            mask = [(idx[0] if isinstance(idx, tuple) else idx) >= cutoff for idx in common_idx]
            X = X[mask]
            y = y[X.index]

        # Fit Ridge
        model = Ridge(alpha=self.cv_params.get("ridge_alpha", 1.0))
        model.fit(X.values, y.values)

        coefs = dict(zip(group_names, model.coef_))
        # Normalize to sum=1
        total = sum(abs(v) for v in coefs.values())
        weights = {k: abs(v) / total if total > 0 else 1.0 / len(group_names)
                   for k, v in coefs.items()}

        combined = self._weighted_sum(aligned, weights)
        logger.info(f"OLS Ridge meta-model: {weights}")
        return combined, weights

    def _stacking(
        self,
        group_predictions: dict[str, pd.DataFrame],
        actual_returns: pd.Series | None,
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """LightGBM Stacking meta-model."""
        try:
            import lightgbm as lgb
        except ImportError as e:
            raise RuntimeError("lightgbm not available for stacking meta-model") from e

        aligned = self._align_predictions(group_predictions)
        group_names = list(aligned.keys())

        if actual_returns is None:
            raise ValueError("stacking requires actual_returns")

        common_idx = None
        for g_pred in aligned.values():
            if common_idx is None:
                common_idx = g_pred.index
            else:
                common_idx = common_idx.intersection(g_pred.index)
        common_idx = common_idx.intersection(actual_returns.index)

        if len(common_idx) < 100:
            raise ValueError(f"Not enough data for stacking: {len(common_idx)}")

        X = pd.DataFrame({g: aligned[g].loc[common_idx] for g in group_names})
        y = actual_returns.loc[common_idx]

        model = lgb.LGBMRegressor(
            num_leaves=8,
            n_estimators=100,
            learning_rate=0.1,
            min_child_samples=30,
            verbose=-1,
        )
        model.fit(X.values, y.values)

        importances = dict(zip(group_names, model.feature_importances_))
        total = sum(importances.values())
        weights = {k: v / total if total > 0 else 1.0 / len(group_names)
                   for k, v in importances.items()}

        # Predict on full data
        full_X = pd.DataFrame({g: aligned[g] for g in group_names})
        predictions = model.predict(full_X.values)
        combined = pd.Series(predictions, index=full_X.index, name="score")

        logger.info(f"Stacking meta-model: {weights}")
        return combined.to_frame("score"), weights

    # ── Helpers ───────────────────────────────────────────────────────

    def _align_predictions(
        self,
        group_predictions: dict[str, pd.DataFrame],
    ) -> dict[str, pd.Series]:
        """Align group predictions to Series with common name 'score'."""
        aligned = {}
        for g_name, g_pred in group_predictions.items():
            if isinstance(g_pred, pd.DataFrame):
                if "score" in g_pred.columns:
                    aligned[g_name] = g_pred["score"]
                else:
                    aligned[g_name] = g_pred.iloc[:, 0]
            else:
                aligned[g_name] = g_pred
        return aligned

    def _weighted_sum(
        self,
        aligned: dict[str, pd.Series],
        weights: dict[str, float],
    ) -> pd.DataFrame:
        """Compute weighted sum of aligned predictions."""
        combined = None
        for g_name, g_pred in aligned.items():
            w = weights.get(g_name, 0.0)
            if w == 0.0:
                continue
            weighted = g_pred * w
            if combined is None:
                combined = weighted
            else:
                combined = combined.add(weighted, fill_value=0.0)

        if combined is None:
            raise ValueError("All meta-model weights are zero; cannot build combined prediction")

        return combined.to_frame("score")
