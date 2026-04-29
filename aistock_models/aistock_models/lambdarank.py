"""
LambdaMART ranking model for cross-sectional stock selection.

Implements Qlib's Model interface using LightGBM's LGBMRanker
with lambdarank objective. Each trading date forms a query group
— stocks are ranked relative to each other within the same date.

Use case: Replace MSE regression with ranking objective when the
downstream task is "select top K stocks by predicted score".

References:
  - Burges, C.J.C. (2010) "From RankNet to LambdaRank to LambdaMART"
  - Kinlay, J. (2025) "Python Equities Entity Store" (production validation)
"""

import logging
import numpy as np
import pandas as pd
import lightgbm as lgb
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP

logger = logging.getLogger(__name__)


class LambdaRankModel(Model):
    """LightGBM LambdaMART ranking model.

    Training workflow:
      1. Extract features/labels from Qlib DatasetH (same as LGBModel)
      2. Build query groups from the date level of MultiIndex
         (each trading date = one query; stocks within date are ranked)
      3. Train LGBMRanker with lambdarank objective
      4. Predict on test set → pd.Series with stock scores

    Configuration (kwargs in conf.yaml):
      - objective: 'lambdarank' (default, ranking)
      - num_leaves: 64 (default)
      - max_depth: 8 (default)
      - learning_rate: 0.05 (default)
      - n_estimators: 300 (default)
      - early_stopping_rounds: 20 (default)
      - All other lightgbm LGBMRanker parameters are forwarded.
    """

    def __init__(self,
                 objective="lambdarank",
                 metric="ndcg",
                 ndcg_eval_at=None,
                 num_leaves=64,
                 max_depth=8,
                 learning_rate=0.05,
                 n_estimators=300,
                 min_child_samples=100,
                 subsample=0.8,
                 colsample_bytree=0.8,
                 reg_alpha=0.1,
                 reg_lambda=0.1,
                 early_stopping_rounds=20,
                 verbose_eval=20,
                 random_state=42,
                 **kwargs):
        super().__init__()
        self.objective = objective
        self.metric = metric
        self.ndcg_eval_at = ndcg_eval_at or [10, 30, 50]
        self.num_leaves = num_leaves
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.n_estimators = n_estimators
        self.min_child_samples = min_child_samples
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self.reg_alpha = reg_alpha
        self.reg_lambda = reg_lambda
        self.early_stopping_rounds = early_stopping_rounds
        self.verbose_eval = verbose_eval
        self.random_state = random_state
        self.extra_kwargs = kwargs
        self.model = None

    def fit(self, dataset: DatasetH, reweighter=None,
            num_boost_round=None, early_stopping_rounds=None,
            verbose_eval=None, evals_result=None, **kwargs):
        """Train LambdaMART model on Qlib DatasetH.

        Parameters
        ----------
        dataset : DatasetH
            Qlib dataset. Must have MultiIndex (datetime, instrument)
            to construct query groups per trading date.
        reweighter : optional
            Qlib reweighter (not used by LambdaMART).
        """
        # 1. Extract train/valid data (same pattern as Qlib LGBModel._prepare_data)
        assert "train" in dataset.segments, "dataset missing 'train' segment"
        data_parts = {}
        for key in ["train", "valid"]:
            if key in dataset.segments:
                df = dataset.prepare(key, col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
                if df.empty:
                    raise ValueError(f"Empty data for segment '{key}'")
                data_parts[key] = df

        df_train = data_parts["train"]
        x_train = df_train["feature"].values.astype(np.float64)
        y_train_raw = df_train["label"].values.astype(np.float64).ravel()

        if "valid" in data_parts:
            df_valid = data_parts["valid"]
            x_valid = df_valid["feature"].values.astype(np.float64)
            y_valid_raw = df_valid["label"].values.astype(np.float64).ravel()
        else:
            x_valid, y_valid_raw = None, None

        # LambdaMART requires integer relevance labels.
        # Discretize continuous returns into ordered relevance classes.
        all_labels = y_train_raw.copy()
        if y_valid_raw is not None:
            all_labels = np.concatenate([all_labels, y_valid_raw])
        n_bins = min(20, len(np.unique(np.round(all_labels, 6))))
        n_bins = max(n_bins, 5)
        bin_edges = np.percentile(all_labels, np.linspace(0, 100, n_bins + 1))
        bin_edges = np.unique(bin_edges)
        n_classes = len(bin_edges) - 1
        if n_classes < 2:
            n_classes = 2
            bin_edges = np.array([all_labels.min() - 0.01, all_labels.mean(), all_labels.max() + 0.01])
        self._n_classes_ = n_classes
        self._bin_edges_ = bin_edges
        y_train = np.digitize(y_train_raw, bin_edges[1:-1]).astype(np.int32)
        y_valid = np.digitize(y_valid_raw, bin_edges[1:-1]).astype(np.int32) if y_valid_raw is not None else None

        # 2. Build query groups from date index
        train_groups = self._build_query_groups(df_train.index)
        valid_groups = self._build_query_groups(df_valid.index) if "valid" in data_parts else None

        logger.info(
            "LambdaRankModel.fit: train=%d samples %d queries, valid=%d samples %d queries, %d rank classes",
            len(y_train), len(train_groups),
            len(y_valid) if y_valid is not None else 0, len(valid_groups) if valid_groups else 0, n_classes
        )

        # 3. Create and train LGBMRanker
        params = {
            "objective": self.objective,
            "metric": self.metric,
            "ndcg_eval_at": self.ndcg_eval_at,
            "num_leaves": self.num_leaves,
            "max_depth": self.max_depth,
            "learning_rate": self.learning_rate,
            "n_estimators": self.n_estimators,
            "min_child_samples": self.min_child_samples,
            "subsample": self.subsample,
            "colsample_bytree": self.colsample_bytree,
            "reg_alpha": self.reg_alpha,
            "reg_lambda": self.reg_lambda,
            "verbosity": -1,
            "random_state": self.random_state,
        }
        params.update(self.extra_kwargs)

        self.model = lgb.LGBMRanker(**params)

        ve = verbose_eval if verbose_eval is not None else self.verbose_eval
        es = early_stopping_rounds if early_stopping_rounds is not None else self.early_stopping_rounds

        fit_kw = dict(
            X=x_train, y=y_train, group=train_groups,
        )
        if y_valid is not None and valid_groups is not None:
            fit_kw.update(
                eval_set=[(x_valid, y_valid)],
                eval_group=[valid_groups],
                eval_at=self.ndcg_eval_at,
                callbacks=[lgb.early_stopping(es), lgb.log_evaluation(ve)],
            )
        else:
            fit_kw["callbacks"] = [lgb.log_evaluation(ve)]

        self.model.fit(**fit_kw)

        # Log best iteration
        if self.model.best_iteration_ > 0:
            logger.info(
                "LambdaRankModel: best_iteration=%d/%d",
                self.model.best_iteration_, self.n_estimators
            )

        return self

    def predict(self, dataset: DatasetH, segment="test"):
        """Predict stock ranking scores.

        Returns
        -------
        pd.Series
            Predicted scores indexed by (datetime, instrument).
            Higher score = better predicted ranking.
        """
        if self.model is None:
            raise RuntimeError("Model not trained. Call fit() first.")

        df_test = dataset.prepare(
            segment, col_set="feature", data_key=DataHandlerLP.DK_L
        )
        x_test = df_test.values.astype(np.float64)
        scores = self.model.predict(x_test)

        return pd.Series(scores, index=df_test.index, name="score")

    # ---- helpers ----

    def _build_query_groups(self, index: pd.Index) -> list:
        """Build query group sizes from index.

        For cross-sectional stock ranking:
        - Each trading date = one query group
        - Stocks within the same date are ranked against each other
        - Group size = number of stocks on that date

        Parameters
        ----------
        index : pd.Index or pd.MultiIndex
            Qlib data index. If MultiIndex (datetime, instrument),
            groups are built from the first (date) level.

        Returns
        -------
        list[int]
            Group sizes, one entry per trading date.
        """
        if isinstance(index, pd.MultiIndex):
            # (datetime, instrument) → group by date
            dates = index.get_level_values(0)
            group_sizes = pd.Series(1, index=dates).groupby(level=0).count()
            return group_sizes.values.tolist()
        else:
            # Flat index: all samples in one group (should not happen
            # for cross-sectional data, but handle gracefully)
            return [len(index)]

    # ---- persistence (qlib compatible) ----

    def save(self, path: str):
        """Save model to file (lightgbm native format)."""
        if self.model is not None:
            import os
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self.model.booster_.save_model(path)

    def load(self, path: str):
        """Load model from file."""
        self.model = lgb.LGBMRanker()
        self.model.booster_ = lgb.Booster(model_file=path)
