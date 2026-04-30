"""AIstock XGBoost model wrapper for Qlib.

Qlib's built-in XGBModel passes constructor kwargs directly to xgboost.train
as booster params, so sklearn-style ``n_estimators`` is ignored. This wrapper
keeps the same Qlib Model interface but maps training-round settings to the
actual ``num_boost_round`` argument used by xgboost.train.
"""

from __future__ import annotations

from typing import Text, Union

import numpy as np
import pandas as pd
import xgboost as xgb
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP
from qlib.data.dataset.weight import Reweighter
from qlib.model.interpret.base import FeatureInt
from qlib.model.base import Model


class AIStockXGBModel(Model, FeatureInt):
    """XGBoost model with explicit, effective boosting-round controls."""

    def __init__(
        self,
        num_boost_round: int | None = None,
        n_estimators: int | None = None,
        early_stopping_rounds: int = 50,
        verbose_eval: int | bool = 20,
        **kwargs,
    ):
        self.num_boost_round = int(num_boost_round or n_estimators or 500)
        self.early_stopping_rounds = int(early_stopping_rounds)
        self.verbose_eval = verbose_eval
        self._params = dict(kwargs)
        self.model = None

    def fit(
        self,
        dataset: DatasetH,
        num_boost_round: int | None = None,
        early_stopping_rounds: int | None = None,
        verbose_eval: int | bool | None = None,
        evals_result: dict | None = None,
        reweighter=None,
        **kwargs,
    ):
        if evals_result is None:
            evals_result = {}

        df_train, df_valid = dataset.prepare(
            ["train", "valid"],
            col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L,
        )
        x_train, y_train = df_train["feature"], df_train["label"]
        x_valid, y_valid = df_valid["feature"], df_valid["label"]

        if y_train.values.ndim == 2 and y_train.values.shape[1] == 1:
            y_train_1d, y_valid_1d = np.squeeze(y_train.values), np.squeeze(y_valid.values)
        else:
            raise ValueError("XGBoost doesn't support multi-label training")

        if reweighter is None:
            w_train = None
            w_valid = None
        elif isinstance(reweighter, Reweighter):
            w_train = reweighter.reweight(df_train)
            w_valid = reweighter.reweight(df_valid)
        else:
            raise ValueError("Unsupported reweighter type.")

        dtrain = xgb.DMatrix(x_train.values, label=y_train_1d, weight=w_train)
        dvalid = xgb.DMatrix(x_valid.values, label=y_valid_1d, weight=w_valid)

        self.model = xgb.train(
            self._params,
            dtrain=dtrain,
            num_boost_round=int(num_boost_round or self.num_boost_round),
            evals=[(dtrain, "train"), (dvalid, "valid")],
            early_stopping_rounds=(
                self.early_stopping_rounds
                if early_stopping_rounds is None
                else int(early_stopping_rounds)
            ),
            verbose_eval=self.verbose_eval if verbose_eval is None else verbose_eval,
            evals_result=evals_result,
            **kwargs,
        )
        evals_result["train"] = list(evals_result["train"].values())[0]
        evals_result["valid"] = list(evals_result["valid"].values())[0]

    def predict(self, dataset: DatasetH, segment: Union[Text, slice] = "test"):
        if self.model is None:
            raise ValueError("model is not fitted yet!")
        x_test = dataset.prepare(segment, col_set="feature", data_key=DataHandlerLP.DK_I)
        return pd.Series(self.model.predict(xgb.DMatrix(x_test)), index=x_test.index)

    def get_feature_importance(self, *args, **kwargs) -> pd.Series:
        return pd.Series(self.model.get_score(*args, **kwargs)).sort_values(ascending=False)
