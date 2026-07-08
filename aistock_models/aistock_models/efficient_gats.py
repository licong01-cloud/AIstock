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
import pandas as pd
import torch
import copy
from torch import optim

from qlib.contrib.model.pytorch_gats_ts import DailyBatchSampler
from qlib.contrib.model.pytorch_gats_ts import GATModel as QlibGATModel
from qlib.contrib.model.pytorch_gats_ts import GATs as QlibGATs
from qlib.contrib.model.pytorch_gru import GRUModel
from qlib.contrib.model.pytorch_lstm import LSTMModel
from qlib.contrib.model.pytorch_utils import count_parameters
from qlib.data.dataset.handler import DataHandlerLP
from qlib.utils import get_or_create_path


_DEFAULT_VRAM_MARGIN_BYTES = 512 * 1024**2
_DEFAULT_WORKING_MEMORY_MULTIPLIER = 4


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
        self.gpu_resident = bool(kwargs.pop("gpu_resident", False))
        self.gpu_resident_shuffle_days = bool(kwargs.pop("gpu_resident_shuffle_days", True))
        self.gpu_resident_vram_margin_bytes = int(
            kwargs.pop("gpu_resident_vram_margin_bytes", _DEFAULT_VRAM_MARGIN_BYTES)
        )
        self.gpu_resident_working_memory_multiplier = int(
            kwargs.pop("gpu_resident_working_memory_multiplier", _DEFAULT_WORKING_MEMORY_MULTIPLIER)
        )
        self.gpu_resident_active = False
        self.gpu_resident_last_fallback = None
        self._gpu_resident_rng = None

        super().__init__(*args, **kwargs)

        if self.seed is not None:
            np.random.seed(self.seed)
            torch.manual_seed(self.seed)
        self._gpu_resident_rng = np.random.RandomState(self.seed)

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

    def _loud_gpu_resident_fallback(self, reason_code, **details):
        self.gpu_resident_active = False
        payload = {"reason_code": reason_code, **details}
        self.gpu_resident_last_fallback = payload
        message = " ".join(f"{key}={value}" for key, value in payload.items())
        self.logger.warning("EfficientGATs GPU resident fallback: %s", message)
        print(message)

    def _normalise_segment_tensor(self, data, *, segment_name):
        if isinstance(data, (list, tuple)) and len(data) == 1:
            data = data[0]
        tensor = data if torch.is_tensor(data) else torch.as_tensor(data)
        tensor = tensor.detach().cpu()
        if tensor.ndim == 4 and tensor.shape[0] == 1:
            tensor = tensor.squeeze(0)
        if tensor.ndim != 3:
            raise ValueError(
                "reason_code=efficient_gats_gpu_resident_shape_invalid: "
                f"segment={segment_name} shape={tuple(tensor.shape)} expected=[rows, seq, feature_plus_label]"
            )
        return tensor.contiguous()

    def _preload_segment_to_cpu(self, segment, *, segment_name):
        sampler = DailyBatchSampler(segment)
        daily_indices = [np.asarray(batch, dtype=np.int64) for batch in sampler]
        if not daily_indices:
            raise ValueError(
                "reason_code=efficient_gats_gpu_resident_empty_segment: "
                f"segment={segment_name} has no daily batches"
            )

        all_indices = np.arange(len(segment), dtype=np.int64)
        tensor = self._normalise_segment_tensor(segment[all_indices], segment_name=segment_name)
        if tensor.shape[0] != len(segment):
            raise ValueError(
                "reason_code=efficient_gats_gpu_resident_row_count_mismatch: "
                f"segment={segment_name} tensor_rows={tensor.shape[0]} expected={len(segment)}"
            )
        return {
            "segment_name": segment_name,
            "tensor": tensor,
            "daily_indices": daily_indices,
            "index": segment.get_index(),
        }

    def _model_parameter_bytes(self):
        total = 0
        for tensor in list(self.GAT_model.parameters()) + list(self.GAT_model.buffers()):
            total += tensor.numel() * tensor.element_size()
        return total

    def _resident_estimate(self, resident_segments):
        resident_bytes = sum(
            segment["tensor"].numel() * segment["tensor"].element_size()
            for segment in resident_segments
        )
        max_daily_count = 0
        max_feature_bytes = 0
        for segment in resident_segments:
            tensor = segment["tensor"]
            for daily_index in segment["daily_indices"]:
                count = int(len(daily_index))
                max_daily_count = max(max_daily_count, count)
                max_feature_bytes = max(
                    max_feature_bytes,
                    count * int(tensor.shape[1]) * int(tensor.shape[2]) * int(tensor.element_size()),
                )
        attention_bytes = max_daily_count * max_daily_count * 4
        working_bytes = (
            attention_bytes * max(1, self.gpu_resident_working_memory_multiplier)
            + max_feature_bytes
        )
        model_bytes = self._model_parameter_bytes()
        required_bytes = resident_bytes + working_bytes + model_bytes + self.gpu_resident_vram_margin_bytes
        return {
            "resident_bytes": int(resident_bytes),
            "working_bytes": int(working_bytes),
            "model_bytes": int(model_bytes),
            "margin_bytes": int(self.gpu_resident_vram_margin_bytes),
            "required_bytes": int(required_bytes),
            "max_daily_count": int(max_daily_count),
        }

    def _cuda_available_bytes(self):
        free_bytes, total_bytes = torch.cuda.mem_get_info(self.device)
        return int(free_bytes), int(total_bytes)

    def _can_activate_gpu_resident(self, resident_segments):
        if not self.gpu_resident:
            return False
        if self.device.type != "cuda" or not torch.cuda.is_available():
            self._loud_gpu_resident_fallback(
                "efficient_gats_gpu_resident_not_cuda",
                device=str(self.device),
                available_bytes=0,
                required_bytes=0,
            )
            return False

        estimate = self._resident_estimate(resident_segments)
        try:
            available_bytes, total_bytes = self._cuda_available_bytes()
        except RuntimeError as exc:
            self._loud_gpu_resident_fallback(
                "efficient_gats_gpu_resident_vram_query_failed",
                error=str(exc),
                **estimate,
            )
            return False

        if estimate["required_bytes"] > available_bytes:
            self._loud_gpu_resident_fallback(
                "efficient_gats_gpu_resident_vram_insufficient",
                available_bytes=available_bytes,
                total_bytes=total_bytes,
                **estimate,
            )
            return False
        return True

    def _move_segment_to_gpu(self, resident_segment):
        return {
            "segment_name": resident_segment["segment_name"],
            "tensor": resident_segment["tensor"].to(self.device),
            "daily_indices": [
                torch.as_tensor(index, dtype=torch.long, device=self.device)
                for index in resident_segment["daily_indices"]
            ],
            "index": resident_segment["index"],
        }

    def _daily_order(self, daily_count, *, shuffle):
        order = np.arange(daily_count)
        if shuffle and daily_count > 1:
            if self._gpu_resident_rng is None:
                self._gpu_resident_rng = np.random.RandomState(self.seed)
            self._gpu_resident_rng.shuffle(order)
        return order

    def _iter_resident_batches(self, resident_segment, *, shuffle):
        daily_indices = resident_segment["daily_indices"]
        for day_idx in self._daily_order(len(daily_indices), shuffle=shuffle):
            index = daily_indices[int(day_idx)]
            yield resident_segment["tensor"].index_select(0, index)

    def _load_pretrained_base_model(self):
        if self.base_model == "LSTM":
            pretrained_model = LSTMModel(
                d_feat=self.d_feat,
                hidden_size=self.hidden_size,
                num_layers=self.num_layers,
            )
        elif self.base_model == "GRU":
            pretrained_model = GRUModel(
                d_feat=self.d_feat,
                hidden_size=self.hidden_size,
                num_layers=self.num_layers,
            )
        else:
            raise ValueError("unknown base model name `%s`" % self.base_model)

        if self.model_path is not None:
            self.logger.info("Loading pretrained model...")
            pretrained_model.load_state_dict(torch.load(self.model_path, map_location=self.device))

        model_dict = self.GAT_model.state_dict()
        pretrained_dict = {
            k: v for k, v in pretrained_model.state_dict().items() if k in model_dict
        }
        model_dict.update(pretrained_dict)
        self.GAT_model.load_state_dict(model_dict)
        self.logger.info("Loading pretrained model Done...")

    def train_epoch(self, data_loader):
        if isinstance(data_loader, dict) and "daily_indices" in data_loader:
            self.GAT_model.train()
            for data in self._iter_resident_batches(
                data_loader,
                shuffle=self.gpu_resident_shuffle_days,
            ):
                feature = data[:, :, 0:-1]
                label = data[:, -1, -1]

                pred = self.GAT_model(feature.float())
                loss = self.loss_fn(pred, label)

                self.train_optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_value_(self.GAT_model.parameters(), 3.0)
                self.train_optimizer.step()
            return

        self.GAT_model.train()
        for data in data_loader:
            data = data.squeeze()
            feature = data[:, :, 0:-1].to(self.device)
            label = data[:, -1, -1].to(self.device)

            pred = self.GAT_model(feature.float())
            loss = self.loss_fn(pred, label)

            self.train_optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_value_(self.GAT_model.parameters(), 3.0)
            self.train_optimizer.step()

    def test_epoch(self, data_loader):
        self.GAT_model.eval()

        scores = []
        losses = []

        if isinstance(data_loader, dict) and "daily_indices" in data_loader:
            batch_iter = self._iter_resident_batches(data_loader, shuffle=False)
        else:
            batch_iter = data_loader

        for data in batch_iter:
            data = data.squeeze()
            feature = data[:, :, 0:-1]
            label = data[:, -1, -1]
            if feature.device != self.device:
                feature = feature.to(self.device)
            if label.device != self.device:
                label = label.to(self.device)

            pred = self.GAT_model(feature.float())
            loss = self.loss_fn(pred, label)
            losses.append(loss.item())

            score = self.metric_fn(pred, label)
            scores.append(score.item())

        return np.mean(losses), np.mean(scores)

    def _fit_streaming(self, dataset, evals_result, save_path):
        return QlibGATs.fit(self, dataset, evals_result=evals_result, save_path=save_path)

    def fit(
        self,
        dataset,
        evals_result=dict(),
        save_path=None,
    ):
        if not self.gpu_resident:
            return self._fit_streaming(dataset, evals_result, save_path)

        dl_train = dataset.prepare("train", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        dl_valid = dataset.prepare("valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        if dl_train.empty or dl_valid.empty:
            raise ValueError("Empty data from dataset, please check your dataset config.")

        dl_train.config(fillna_type="ffill+bfill")
        dl_valid.config(fillna_type="ffill+bfill")

        train_cpu = self._preload_segment_to_cpu(dl_train, segment_name="train")
        valid_cpu = self._preload_segment_to_cpu(dl_valid, segment_name="valid")
        resident_cpu = [train_cpu, valid_cpu]

        if not self._can_activate_gpu_resident(resident_cpu):
            return self._fit_streaming(dataset, evals_result, save_path)

        try:
            train_resident = self._move_segment_to_gpu(train_cpu)
            valid_resident = self._move_segment_to_gpu(valid_cpu)
        except RuntimeError as exc:
            if self.device.type == "cuda":
                torch.cuda.empty_cache()
            self._loud_gpu_resident_fallback(
                "efficient_gats_gpu_resident_preload_failed",
                error=str(exc),
                **self._resident_estimate(resident_cpu),
            )
            return self._fit_streaming(dataset, evals_result, save_path)

        self.gpu_resident_active = True
        self.gpu_resident_last_fallback = None
        self.logger.info(
            "EfficientGATs GPU resident mode active: train_rows=%s valid_rows=%s model_size=%.4f MB",
            train_resident["tensor"].shape[0],
            valid_resident["tensor"].shape[0],
            count_parameters(self.GAT_model),
        )

        save_path = get_or_create_path(save_path)
        stop_steps = 0
        best_score = -np.inf
        best_epoch = 0
        best_param = copy.deepcopy(self.GAT_model.state_dict())
        evals_result["train"] = []
        evals_result["valid"] = []

        self._load_pretrained_base_model()

        self.logger.info("training...")
        self.fitted = True

        for step in range(self.n_epochs):
            self.logger.info("Epoch%d:", step)
            self.logger.info("training...")
            self.train_epoch(train_resident)
            self.logger.info("evaluating...")
            train_loss, train_score = self.test_epoch(train_resident)
            val_loss, val_score = self.test_epoch(valid_resident)
            self.logger.info("train %.6f, valid %.6f" % (train_score, val_score))
            evals_result["train"].append(train_score)
            evals_result["valid"].append(val_score)

            if val_score > best_score:
                best_score = val_score
                stop_steps = 0
                best_epoch = step
                best_param = copy.deepcopy(self.GAT_model.state_dict())
            else:
                stop_steps += 1
                if stop_steps >= self.early_stop:
                    self.logger.info("early stop")
                    break

        self.logger.info("best score: %.6lf @ %d" % (best_score, best_epoch))
        self.GAT_model.load_state_dict(best_param)
        torch.save(best_param, save_path)

        if self.use_gpu:
            torch.cuda.empty_cache()

    def predict(self, dataset):
        if not self.fitted:
            raise ValueError("model is not fitted yet!")
        if not self.gpu_resident:
            return QlibGATs.predict(self, dataset)

        dl_test = dataset.prepare("test", col_set=["feature", "label"], data_key=DataHandlerLP.DK_I)
        dl_test.config(fillna_type="ffill+bfill")
        test_cpu = self._preload_segment_to_cpu(dl_test, segment_name="test")

        if not self._can_activate_gpu_resident([test_cpu]):
            return QlibGATs.predict(self, dataset)

        try:
            test_resident = self._move_segment_to_gpu(test_cpu)
        except RuntimeError as exc:
            if self.device.type == "cuda":
                torch.cuda.empty_cache()
            self._loud_gpu_resident_fallback(
                "efficient_gats_gpu_resident_predict_preload_failed",
                error=str(exc),
                **self._resident_estimate([test_cpu]),
            )
            return QlibGATs.predict(self, dataset)

        self.GAT_model.eval()
        preds = []
        for data in self._iter_resident_batches(test_resident, shuffle=False):
            feature = data[:, :, 0:-1]
            with torch.no_grad():
                pred = self.GAT_model(feature.float()).detach().cpu().numpy()
            preds.append(pred)

        return pd.Series(np.concatenate(preds), index=dl_test.get_index())
