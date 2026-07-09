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
_GATS_ADJACENCY_OFF = "off"
_GATS_ADJACENCY_INDUSTRY_BIAS = "industry_bias"
_GATS_ADJACENCY_MODES = {_GATS_ADJACENCY_OFF, _GATS_ADJACENCY_INDUSTRY_BIAS}
_MISSING_INDUSTRY_ID = -1


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

    def __init__(
        self,
        *args,
        gats_adjacency_mode=_GATS_ADJACENCY_OFF,
        gats_industry_gamma_init=0.0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if gats_adjacency_mode not in _GATS_ADJACENCY_MODES:
            raise ValueError(
                "reason_code=efficient_gats_adjacency_mode_invalid: "
                f"mode={gats_adjacency_mode} allowed={sorted(_GATS_ADJACENCY_MODES)}"
            )
        self.gats_adjacency_mode = gats_adjacency_mode
        if self.gats_adjacency_mode == _GATS_ADJACENCY_INDUSTRY_BIAS:
            self.industry_bias_gamma = torch.nn.Parameter(
                torch.tensor(float(gats_industry_gamma_init), dtype=torch.float32)
            )
        else:
            self.register_parameter("industry_bias_gamma", None)
        self.last_attention_weight = None

    @staticmethod
    def industry_same_matrix(industry_ids, *, device, dtype):
        if industry_ids is None:
            return None
        ids = industry_ids.to(device=device, dtype=torch.long).view(-1)
        valid = ids >= 0
        same = (ids.view(-1, 1) == ids.view(1, -1)) & valid.view(-1, 1) & valid.view(1, -1)
        return same.to(dtype=dtype)

    def cal_attention(self, x, y, industry_ids=None):
        left_hidden = self.transformation(x)
        right_hidden = self.transformation(y)
        attention_out = additive_gats_attention_logits(left_hidden, right_hidden, self.a)
        attention_out = self.leaky_relu(attention_out)
        if self.gats_adjacency_mode == _GATS_ADJACENCY_INDUSTRY_BIAS and industry_ids is not None:
            same_industry = self.industry_same_matrix(
                industry_ids,
                device=attention_out.device,
                dtype=attention_out.dtype,
            )
            attention_out = attention_out + self.industry_bias_gamma.to(attention_out.dtype) * same_industry
        attention_weight = self.softmax(attention_out)
        self.last_attention_weight = attention_weight
        return attention_weight

    def forward(self, x, industry_ids=None):
        out, _ = self.rnn(x)
        hidden = out[:, -1, :]
        att_weight = self.cal_attention(hidden, hidden, industry_ids=industry_ids)
        hidden = att_weight.mm(hidden) + hidden
        hidden = self.fc(hidden)
        hidden = self.leaky_relu(hidden)
        return self.fc_out(hidden).squeeze()


class EfficientGATs(QlibGATs):
    """Drop-in qlib GATs model using :class:`EfficientGATModel` internally."""

    def __init__(self, *args, **kwargs):
        self.gats_adjacency_mode = kwargs.pop("gats_adjacency_mode", _GATS_ADJACENCY_OFF)
        self.gats_industry_gamma_init = float(kwargs.pop("gats_industry_gamma_init", 0.0))
        self.gats_industry_id_provider = kwargs.pop("gats_industry_id_provider", None)
        if self.gats_adjacency_mode not in _GATS_ADJACENCY_MODES:
            raise ValueError(
                "reason_code=efficient_gats_adjacency_mode_invalid: "
                f"mode={self.gats_adjacency_mode} allowed={sorted(_GATS_ADJACENCY_MODES)}"
            )
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
        self.gats_adjacency_last_event = None
        self.gats_adjacency_events = []
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
            gats_adjacency_mode=self.gats_adjacency_mode,
            gats_industry_gamma_init=self.gats_industry_gamma_init,
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

    def _industry_bias_enabled(self):
        return self.gats_adjacency_mode == _GATS_ADJACENCY_INDUSTRY_BIAS

    def _loud_adjacency_event(self, reason_code, **details):
        payload = {"reason_code": reason_code, **details}
        self.gats_adjacency_last_event = payload
        self.gats_adjacency_events.append(payload)
        message = " ".join(f"{key}={value}" for key, value in payload.items())
        self.logger.warning("EfficientGATs adjacency: %s", message)
        print(message)
        return payload

    def _provider_industry_values(self, segment, index, *, segment_name):
        provider = self.gats_industry_id_provider
        if provider is None:
            return None
        if callable(provider):
            for args in (
                (segment, index, segment_name),
                (segment, index),
                (index,),
            ):
                try:
                    return provider(*args)
                except TypeError:
                    continue
        if isinstance(provider, dict):
            values = []
            for row_key in index:
                if isinstance(row_key, tuple):
                    date_key = row_key[0] if row_key else None
                    instrument = row_key[-1] if row_key else None
                else:
                    date_key = None
                    instrument = row_key
                candidates = (
                    row_key,
                    (str(date_key), str(instrument)),
                    str(instrument),
                )
                values.append(next((provider[key] for key in candidates if key in provider), None))
            return values
        return None

    def _segment_industry_values(self, segment, index, *, segment_name):
        values = self._provider_industry_values(segment, index, segment_name=segment_name)
        if values is not None:
            return values

        for method_name in ("get_industry_ids", "get_sector_ids", "get_sw2_ids"):
            method = getattr(segment, method_name, None)
            if method is None:
                continue
            for args in ((index,), ()):
                try:
                    values = method(*args)
                    break
                except TypeError:
                    continue
            if values is not None:
                return values

        for attr_name in (
            "industry_ids",
            "_industry_ids",
            "sector_ids",
            "_sector_ids",
            "sw2_ids",
            "_sw2_ids",
        ):
            if hasattr(segment, attr_name):
                return getattr(segment, attr_name)

        if isinstance(index, pd.MultiIndex):
            for level_name in ("industry", "industry_id", "sector", "sector_id", "sw2", "sw2_id"):
                if level_name in index.names:
                    return index.get_level_values(level_name)
        return None

    def _normalise_industry_ids(self, values, index, *, segment_name):
        if values is None:
            self._loud_adjacency_event(
                "efficient_gats_industry_ids_missing",
                segment=segment_name,
                rows=len(index),
            )
            return None

        if isinstance(values, pd.Series):
            if values.index.equals(index):
                values = values.to_numpy()
            else:
                values = values.reindex(index).to_numpy()
        else:
            values = np.asarray(values, dtype=object)

        values = np.asarray(values, dtype=object).reshape(-1)
        if len(values) != len(index):
            raise ValueError(
                "reason_code=efficient_gats_industry_id_count_mismatch: "
                f"segment={segment_name} ids={len(values)} rows={len(index)}"
            )

        series = pd.Series(values, dtype="object")
        missing = series.isna() | series.astype(str).isin(["", "nan", "NaN", "None", "none", "<NA>"])
        codes = np.full(len(series), _MISSING_INDUSTRY_ID, dtype=np.int64)
        if (~missing).any():
            factorized, _uniques = pd.factorize(series[~missing], sort=True)
            codes[~missing.to_numpy()] = factorized.astype(np.int64)

        missing_count = int(missing.sum())
        if missing_count:
            self._loud_adjacency_event(
                "efficient_gats_industry_id_missing",
                segment=segment_name,
                missing_rows=missing_count,
                rows=len(index),
            )
        return torch.as_tensor(codes, dtype=torch.long)

    def _extract_segment_industry_ids(self, segment, *, segment_name):
        if not self._industry_bias_enabled():
            return None
        index = segment.get_index()
        values = self._segment_industry_values(segment, index, segment_name=segment_name)
        return self._normalise_industry_ids(values, index, segment_name=segment_name)

    def _require_segment_industry_ids(self, segment, *, segment_name):
        industry_ids = self._extract_segment_industry_ids(segment, segment_name=segment_name)
        if self._industry_bias_enabled() and industry_ids is None:
            raise ValueError(
                "reason_code=efficient_gats_industry_ids_missing: "
                f"segment={segment_name} rows={len(segment.get_index())}"
            )
        return industry_ids

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
        if tensor.dtype != torch.float32:
            tensor = tensor.to(dtype=torch.float32)
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
            "industry_ids": self._require_segment_industry_ids(segment, segment_name=segment_name),
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
        reserved_bytes = int(torch.cuda.memory_reserved(self.device))
        allocated_bytes = int(torch.cuda.memory_allocated(self.device))
        reclaimable_bytes = max(0, reserved_bytes - allocated_bytes)
        return int(free_bytes) + reclaimable_bytes, int(total_bytes)

    def _release_cached_cuda_blocks(self):
        if self.device.type == "cuda" and torch.cuda.is_available():
            torch.cuda.empty_cache()

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
        moved = {
            "segment_name": resident_segment["segment_name"],
            "tensor": resident_segment["tensor"].to(self.device, non_blocking=True),
            "daily_indices": [
                torch.as_tensor(index, dtype=torch.long, device=self.device)
                for index in resident_segment["daily_indices"]
            ],
            "index": resident_segment["index"],
        }
        industry_ids = resident_segment.get("industry_ids")
        moved["industry_ids"] = (
            industry_ids.to(self.device, non_blocking=True)
            if torch.is_tensor(industry_ids)
            else None
        )
        return moved

    def _daily_order(self, daily_count, *, shuffle):
        order = np.arange(daily_count)
        if shuffle and daily_count > 1:
            if self._gpu_resident_rng is None:
                self._gpu_resident_rng = np.random.RandomState(self.seed)
            self._gpu_resident_rng.shuffle(order)
        return order

    def _reset_fit_rng(self):
        if self.seed is None:
            return
        np.random.seed(self.seed)
        torch.manual_seed(self.seed)
        self._gpu_resident_rng = np.random.RandomState(self.seed)

    def _iter_resident_batches(self, resident_segment, *, shuffle, include_industry=False):
        daily_indices = resident_segment["daily_indices"]
        for day_idx in self._daily_order(len(daily_indices), shuffle=shuffle):
            index = daily_indices[int(day_idx)]
            data = resident_segment["tensor"].index_select(0, index).contiguous()
            if include_industry:
                industry_ids = resident_segment.get("industry_ids")
                if torch.is_tensor(industry_ids):
                    industry_ids = industry_ids.index_select(0, index).contiguous()
                else:
                    industry_ids = None
                yield data, industry_ids
            else:
                yield data

    def _preload_streaming_segment_metadata(self, segment, *, segment_name):
        sampler = DailyBatchSampler(segment)
        daily_indices = [np.asarray(batch, dtype=np.int64) for batch in sampler]
        return {
            "segment_name": segment_name,
            "segment": segment,
            "daily_indices": daily_indices,
            "index": segment.get_index(),
            "industry_ids": self._require_segment_industry_ids(segment, segment_name=segment_name),
        }

    def _iter_streaming_batches(self, streaming_segment, *, shuffle):
        segment = streaming_segment["segment"]
        daily_indices = streaming_segment["daily_indices"]
        full_industry_ids = streaming_segment.get("industry_ids")
        for day_idx in self._daily_order(len(daily_indices), shuffle=shuffle):
            index = daily_indices[int(day_idx)]
            data = segment[index]
            industry_ids = None
            if torch.is_tensor(full_industry_ids):
                industry_ids = full_industry_ids.index_select(
                    0,
                    torch.as_tensor(index, dtype=torch.long),
                ).contiguous()
            yield data, industry_ids

    def _load_pretrained_base_model(self):
        self._reset_fit_rng()
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
            if "tensor" in data_loader:
                batch_iter = self._iter_resident_batches(
                    data_loader,
                    shuffle=self.gpu_resident_shuffle_days,
                    include_industry=True,
                )
            else:
                batch_iter = self._iter_streaming_batches(data_loader, shuffle=False)

            for data, industry_ids in batch_iter:
                data = data.squeeze()
                feature = data[:, :, 0:-1]
                label = data[:, -1, -1]
                if feature.device != self.device:
                    feature = feature.to(self.device)
                if label.device != self.device:
                    label = label.to(self.device)
                if torch.is_tensor(industry_ids) and industry_ids.device != self.device:
                    industry_ids = industry_ids.to(self.device)
                if data_loader.get("tensor") is None:
                    feature = feature.float()

                pred = self.GAT_model(feature, industry_ids=industry_ids)
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

        resident_loader = isinstance(data_loader, dict) and "daily_indices" in data_loader
        if resident_loader:
            if "tensor" in data_loader:
                batch_iter = self._iter_resident_batches(data_loader, shuffle=False, include_industry=True)
            else:
                batch_iter = self._iter_streaming_batches(data_loader, shuffle=False)
        else:
            batch_iter = data_loader

        for batch in batch_iter:
            if resident_loader:
                data, industry_ids = batch
            else:
                data = batch
                industry_ids = None

            if resident_loader:
                data = data.squeeze()
                feature = data[:, :, 0:-1]
                label = data[:, -1, -1]
                if feature.device != self.device:
                    feature = feature.to(self.device)
                if label.device != self.device:
                    label = label.to(self.device)
                if torch.is_tensor(industry_ids) and industry_ids.device != self.device:
                    industry_ids = industry_ids.to(self.device)
                if data_loader.get("tensor") is None:
                    feature = feature.float()
            else:
                data = data.squeeze()
                feature = data[:, :, 0:-1]
                label = data[:, -1, -1]
                if feature.device != self.device:
                    feature = feature.to(self.device)
                if label.device != self.device:
                    label = label.to(self.device)
                feature = feature.float()

            pred = self.GAT_model(feature, industry_ids=industry_ids)
            loss = self.loss_fn(pred, label)
            losses.append(loss.detach())

            score = self.metric_fn(pred, label)
            scores.append(score.detach())

        return torch.stack(losses).mean().item(), torch.stack(scores).mean().item()

    def _fit_streaming(self, dataset, evals_result, save_path):
        if self._industry_bias_enabled():
            return self._fit_streaming_with_industry(dataset, evals_result, save_path)
        self._reset_fit_rng()
        return QlibGATs.fit(self, dataset, evals_result=evals_result, save_path=save_path)

    def _fit_streaming_with_industry(self, dataset, evals_result, save_path):
        dl_train = dataset.prepare("train", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        dl_valid = dataset.prepare("valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        if dl_train.empty or dl_valid.empty:
            raise ValueError("Empty data from dataset, please check your dataset config.")

        dl_train.config(fillna_type="ffill+bfill")
        dl_valid.config(fillna_type="ffill+bfill")

        train_loader = self._preload_streaming_segment_metadata(dl_train, segment_name="train")
        valid_loader = self._preload_streaming_segment_metadata(dl_valid, segment_name="valid")

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
            self.train_epoch(train_loader)
            self.logger.info("evaluating...")
            train_loss, train_score = self.test_epoch(train_loader)
            val_loss, val_score = self.test_epoch(valid_loader)
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

        self._release_cached_cuda_blocks()

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
            return self._predict_streaming(dataset)

        self._release_cached_cuda_blocks()

        dl_test = dataset.prepare("test", col_set=["feature", "label"], data_key=DataHandlerLP.DK_I)
        dl_test.config(fillna_type="ffill+bfill")
        test_cpu = self._preload_segment_to_cpu(dl_test, segment_name="test")

        if not self._can_activate_gpu_resident([test_cpu]):
            return self._predict_streaming(dataset)

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
            return self._predict_streaming(dataset)

        self.GAT_model.eval()
        preds = []
        for data, industry_ids in self._iter_resident_batches(test_resident, shuffle=False, include_industry=True):
            feature = data.narrow(2, 0, data.shape[2] - 1)
            with torch.no_grad():
                pred = self.GAT_model(feature, industry_ids=industry_ids).detach().cpu().numpy()
            preds.append(pred)

        return pd.Series(np.concatenate(preds), index=dl_test.get_index())

    def _predict_streaming(self, dataset):
        if not self._industry_bias_enabled():
            return QlibGATs.predict(self, dataset)

        dl_test = dataset.prepare("test", col_set=["feature", "label"], data_key=DataHandlerLP.DK_I)
        dl_test.config(fillna_type="ffill+bfill")
        test_loader = self._preload_streaming_segment_metadata(dl_test, segment_name="test")

        self.GAT_model.eval()
        preds = []
        for data, industry_ids in self._iter_streaming_batches(test_loader, shuffle=False):
            data = data.squeeze()
            feature = data[:, :, 0:-1].to(self.device)
            if torch.is_tensor(industry_ids):
                industry_ids = industry_ids.to(self.device)
            with torch.no_grad():
                pred = self.GAT_model(feature.float(), industry_ids=industry_ids).detach().cpu().numpy()
            preds.append(pred)

        return pd.Series(np.concatenate(preds), index=dl_test.get_index())
