"""QE-owned TCN adapter with explicit optimizer regularization semantics.

Qlib's ``pytorch_tcn_ts.TCN`` is a complete Qlib Model and already provides
the temporal convolution architecture, training loop, prediction path, and
TSDatasetH contract.  Its constructor accepts ``**kwargs`` but does not apply
``weight_decay`` to the optimizer.  AIstock's registered TCN research seeds do
declare weight decay, so passing that field directly to upstream Qlib would be
a silent semantic loss.  This adapter rebuilds the optimizer with the recorded
regularization value while preserving the upstream implementation otherwise.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import torch
import torch.optim as optim
from qlib.contrib.model.pytorch_tcn_ts import TCN
from qlib.data.dataset.handler import DataHandlerLP
from torch.utils.data import DataLoader


class AIStockTCN(TCN):
    """Qlib TCN with canonical tensor axes and QE optimizer semantics.

    ``TSDatasetH`` yields ``[batch, time, feature-plus-label]`` tensors while
    ``nn.Conv1d`` requires ``[batch, channel, time]``.  The Qlib build used by
    AIstock predates the upstream training-axis fix, and the current upstream
    prediction path still does not transpose its input.  Leaving either path
    unchanged can make a 20-day canary appear to work only because
    ``step_len == d_feat`` while convolving across the factor axis.  This
    adapter owns the complete train/eval/predict boundary so every path uses
    features as channels and history as the convolution axis.
    """

    def __init__(self, *, weight_decay: float = 0.0, **kwargs: Any) -> None:
        try:
            parsed_weight_decay = float(weight_decay)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "reason_code=qe_tcn_weight_decay_invalid: "
                f"weight_decay must be a non-negative number, got {weight_decay!r}"
            ) from exc
        if parsed_weight_decay < 0:
            raise ValueError(
                "reason_code=qe_tcn_weight_decay_invalid: "
                f"weight_decay must be non-negative, got {parsed_weight_decay!r}"
            )

        super().__init__(**kwargs)
        self.weight_decay = parsed_weight_decay
        if self.optimizer == "adam":
            self.train_optimizer = optim.Adam(
                self.TCN_model.parameters(),
                lr=self.lr,
                weight_decay=self.weight_decay,
            )
        elif self.optimizer == "gd":
            self.train_optimizer = optim.SGD(
                self.TCN_model.parameters(),
                lr=self.lr,
                weight_decay=self.weight_decay,
            )
        else:  # pragma: no cover - the upstream constructor rejects this first.
            raise NotImplementedError(f"optimizer {self.optimizer} is not supported")

    def _split_batch(self, data: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Return ``(feature, label)`` using the audited TSDatasetH axis order."""

        if not isinstance(data, torch.Tensor) or data.ndim != 3:
            shape = tuple(data.shape) if isinstance(data, torch.Tensor) else None
            raise ValueError(
                "reason_code=qe_tcn_batch_shape_invalid: "
                f"expected a rank-3 tensor, got type={type(data).__name__}, shape={shape}"
            )
        if data.shape[2] < 2:
            raise ValueError(
                "reason_code=qe_tcn_batch_columns_invalid: "
                f"feature-plus-label width must be >=2, got shape={tuple(data.shape)}"
            )

        channel_first = torch.transpose(data, 1, 2)
        feature = channel_first[:, 0:-1, :]
        label = channel_first[:, -1, -1]
        actual_d_feat = int(feature.shape[1])
        if actual_d_feat != int(self.d_feat):
            raise ValueError(
                "reason_code=qe_tcn_feature_dimension_mismatch: "
                f"configured d_feat={self.d_feat}, batch features={actual_d_feat}, "
                f"input_shape={tuple(data.shape)}"
            )
        return feature.to(self.device), label.to(self.device)

    def train_epoch(self, data_loader: DataLoader) -> None:
        self.TCN_model.train()
        for data in data_loader:
            feature, label = self._split_batch(data)
            pred = self.TCN_model(feature.float())
            loss = self.loss_fn(pred, label)

            self.train_optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_value_(self.TCN_model.parameters(), 3.0)
            self.train_optimizer.step()

    def test_epoch(self, data_loader: DataLoader) -> tuple[float, float]:
        self.TCN_model.eval()
        scores: list[float] = []
        losses: list[float] = []
        for data in data_loader:
            feature, label = self._split_batch(data)
            with torch.no_grad():
                pred = self.TCN_model(feature.float())
                loss = self.loss_fn(pred, label)
                score = self.metric_fn(pred, label)
            losses.append(float(loss.item()))
            scores.append(float(score.item()))
        if not losses:
            raise ValueError(
                "reason_code=qe_tcn_empty_evaluation_loader: "
                "TCN evaluation produced no batches"
            )
        return float(np.mean(losses)), float(np.mean(scores))

    def predict(self, dataset: Any) -> pd.Series:
        if not self.fitted:
            raise ValueError("model is not fitted yet")
        dl_test = dataset.prepare(
            "test",
            col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_I,
        )
        dl_test.config(fillna_type="ffill+bfill")
        test_loader = DataLoader(
            dl_test,
            batch_size=self.batch_size,
            num_workers=self.n_jobs,
        )

        self.TCN_model.eval()
        preds: list[np.ndarray] = []
        for data in test_loader:
            feature, _ = self._split_batch(data)
            with torch.no_grad():
                pred = self.TCN_model(feature.float()).detach().cpu().numpy()
            preds.append(np.asarray(pred, dtype="float64").reshape(-1))
        if not preds:
            raise ValueError(
                "reason_code=qe_tcn_empty_prediction_loader: "
                "TCN prediction produced no batches"
            )
        values = np.concatenate(preds)
        index = dl_test.get_index()
        if len(values) != len(index):
            raise ValueError(
                "reason_code=qe_tcn_prediction_length_mismatch: "
                f"prediction rows={len(values)}, dataset index rows={len(index)}"
            )
        return pd.Series(values, index=index, name="score")
