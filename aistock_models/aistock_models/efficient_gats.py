"""Memory-efficient qlib GATs implementation.

This module keeps qlib's GATs fit/predict/DailyBatchSampler behavior intact
while replacing the attention score construction with the mathematically
equivalent additive form:

    a^T [h_col, h_row] = a_l^T h_col + a_r^T h_row

The qlib implementation materializes ``[N, N, 2 * hidden]`` before projecting
with ``a``.  This implementation materializes only the scalar ``[N, N]`` score
matrix required by softmax while preserving qlib's real expand/transpose
indexing order.
"""

from __future__ import annotations

import numpy as np
import torch
from torch import optim

from qlib.contrib.model.pytorch_gats_ts import GATModel as QlibGATModel
from qlib.contrib.model.pytorch_gats_ts import GATs as QlibGATs


def additive_gats_attention_logits(
    column_hidden: torch.Tensor,
    row_hidden: torch.Tensor,
    attention_vector: torch.Tensor,
) -> torch.Tensor:
    """Return qlib-equivalent raw GAT logits without ``[N, N, 2H]`` allocation."""

    hidden_size = column_hidden.shape[1]
    if row_hidden.shape[1] != hidden_size:
        raise ValueError(
            "reason_code=efficient_gats_hidden_size_mismatch: "
            f"column_hidden={tuple(column_hidden.shape)} row_hidden={tuple(row_hidden.shape)}"
        )
    if attention_vector.shape != (hidden_size * 2, 1):
        raise ValueError(
            "reason_code=efficient_gats_attention_shape_invalid: "
            f"attention_vector={tuple(attention_vector.shape)} expected={(hidden_size * 2, 1)}"
        )

    a_left = attention_vector[:hidden_size]
    a_right = attention_vector[hidden_size:]
    column_scores = column_hidden.mm(a_left)
    row_scores = row_hidden.mm(a_right)
    return row_scores + column_scores.t()


class EfficientGATModel(QlibGATModel):
    """Qlib GATModel with equivalent additive attention score construction."""

    def cal_attention(self, x, y):
        left_hidden = self.transformation(x)
        right_hidden = self.transformation(y)
        attention_out = additive_gats_attention_logits(left_hidden, right_hidden, self.a)
        attention_out = self.leaky_relu(attention_out)
        return self.softmax(attention_out)


class EfficientGATs(QlibGATs):
    """Drop-in qlib GATs model using :class:`EfficientGATModel` internally."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.seed is not None:
            np.random.seed(self.seed)
            torch.manual_seed(self.seed)

        self.GAT_model = EfficientGATModel(
            d_feat=self.d_feat,
            hidden_size=self.hidden_size,
            num_layers=self.num_layers,
            dropout=self.dropout,
            base_model=self.base_model,
        )
        self.GAT_model.to(self.device)

        if self.optimizer == "adam":
            self.train_optimizer = optim.Adam(self.GAT_model.parameters(), lr=self.lr)
        elif self.optimizer == "gd":
            self.train_optimizer = optim.SGD(self.GAT_model.parameters(), lr=self.lr)
        else:
            raise NotImplementedError(f"optimizer {self.optimizer} is not supported!")
