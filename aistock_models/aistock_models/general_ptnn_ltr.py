"""AIstock GeneralPTNN adapter with opt-in date-grouped listwise LTR loss.

The default ``mse`` path mirrors Qlib's ``GeneralPTNN`` semantics.  The
``approx_ndcg_at_k`` path is intentionally fail-loud: listwise LTR needs one
cross-sectional query group per trading date, so malformed indices, tiny
groups, all-NaN labels, or degenerate relevance must stop the run instead of
falling back to regression.
"""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from statistics import median
from typing import Any, Iterable, Union

import numpy as np
import pandas as pd
import torch
import torch.optim as optim
from qlib.data.dataset import DatasetH, TSDatasetH
from qlib.data.dataset.handler import DataHandlerLP
from qlib.data.dataset.weight import Reweighter
from qlib.log import get_module_logger
from qlib.model.base import Model
from qlib.model.utils import ConcatDataset
from qlib.utils import get_or_create_path, init_instance_by_config
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader, Dataset

from qlib.contrib.model.pytorch_utils import count_parameters


class GeneralPTNNLTRError(ValueError):
    """ValueError carrying a stable reason_code for QE fail-loud reporting."""

    def __init__(self, reason_code: str, message: str, **context: Any) -> None:
        self.reason_code = reason_code
        self.context = context
        details = ", ".join(f"{key}={value}" for key, value in context.items() if value is not None)
        suffix = f"; {details}" if details else ""
        super().__init__(f"reason_code={reason_code}: {message}{suffix}")


@dataclass
class LTRBatchDiagnostics:
    loss_sum: float = 0.0
    ndcg_sum: float = 0.0
    query_count: int = 0
    degenerate_group_count: int = 0
    group_sizes: list[int] | None = None

    def add(self, *, loss: float, ndcg: float, group_size: int) -> None:
        if self.group_sizes is None:
            self.group_sizes = []
        self.loss_sum += float(loss)
        self.ndcg_sum += float(ndcg)
        self.query_count += 1
        self.group_sizes.append(int(group_size))

    def as_dict(self, *, loss_mode: str, topk_train_k: int) -> dict[str, Any]:
        sizes = self.group_sizes or []
        if not self.query_count:
            return {
                "loss_mode": loss_mode,
                "topk_train_k": topk_train_k,
                "loss": math.nan,
                "ndcg_at_k": math.nan,
                "query_count": 0,
                "min_group_size": 0,
                "median_group_size": 0,
                "max_group_size": 0,
                "degenerate_group_count": self.degenerate_group_count,
            }
        return {
            "loss_mode": loss_mode,
            "topk_train_k": topk_train_k,
            "loss": self.loss_sum / self.query_count,
            "ndcg_at_k": self.ndcg_sum / self.query_count,
            "query_count": self.query_count,
            "min_group_size": min(sizes),
            "median_group_size": median(sizes),
            "max_group_size": max(sizes),
            "degenerate_group_count": self.degenerate_group_count,
        }


class DateGroupedDataset(Dataset):
    """Wrap a Qlib TSDataSampler so each item is one trading-date query group."""

    def __init__(
        self,
        sampler: Any,
        *,
        segment: str,
        min_group_size: int,
        topk_train_k: int,
    ) -> None:
        self.sampler = sampler
        self.segment = segment
        self.min_group_size = int(min_group_size)
        self.topk_train_k = int(topk_train_k)
        self.required_group_size = max(self.min_group_size, self.topk_train_k)
        self.index = self._extract_index(sampler, segment=segment)
        self.date_level, self.instrument_level = self._resolve_levels(self.index, segment=segment)
        self.groups = self._build_groups()

    @staticmethod
    def _extract_index(sampler: Any, *, segment: str) -> pd.MultiIndex:
        if not hasattr(sampler, "get_index"):
            raise GeneralPTNNLTRError(
                "ltr_query_index_invalid",
                "LTR requires a sampler exposing get_index() with datetime/instrument levels",
                dataset_segment=segment,
            )
        index = sampler.get_index()
        if not isinstance(index, pd.MultiIndex) or index.nlevels < 2:
            raise GeneralPTNNLTRError(
                "ltr_query_index_invalid",
                "LTR query index must be a MultiIndex with datetime and instrument",
                dataset_segment=segment,
                index_type=type(index).__name__,
            )
        return index

    @staticmethod
    def _resolve_levels(index: pd.MultiIndex, *, segment: str) -> tuple[int | str, int | str]:
        names = list(index.names)
        if "datetime" in names and "instrument" in names:
            return "datetime", "instrument"
        if index.nlevels >= 2 and all(name is None for name in names[:2]):
            return 0, 1
        raise GeneralPTNNLTRError(
            "ltr_query_index_invalid",
            "LTR query index cannot resolve datetime/instrument levels",
            dataset_segment=segment,
            index_names=names,
        )

    def _build_groups(self) -> list[tuple[Any, np.ndarray, list[Any]]]:
        dates = self.index.get_level_values(self.date_level)
        instruments = self.index.get_level_values(self.instrument_level)
        try:
            for date in pd.Index(dates).unique():
                pd.Timestamp(date)
        except Exception as exc:
            raise GeneralPTNNLTRError(
                "ltr_query_index_invalid",
                "LTR query datetime level cannot be parsed as dates",
                dataset_segment=self.segment,
                dtype=str(pd.Index(dates).dtype),
            ) from exc
        if pd.Index(instruments).isna().any():
            raise GeneralPTNNLTRError(
                "ltr_query_index_invalid",
                "LTR query instrument level contains null values",
                dataset_segment=self.segment,
            )
        by_date: dict[Any, list[int]] = {}
        instruments_by_date: dict[Any, list[Any]] = {}
        for pos, (date, instrument) in enumerate(zip(dates, instruments, strict=True)):
            by_date.setdefault(date, []).append(pos)
            instruments_by_date.setdefault(date, []).append(instrument)
        return [
            (date, np.asarray(positions, dtype=np.int64), instruments_by_date[date])
            for date, positions in by_date.items()
        ]

    def __len__(self) -> int:
        return len(self.groups)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor, Any, list[Any]]:
        date, positions, instruments = self.groups[idx]
        if len(positions) < self.required_group_size:
            raise GeneralPTNNLTRError(
                "ltr_query_too_small",
                "LTR query group has fewer samples than required",
                dataset_segment=self.segment,
                date=date,
                group_size=len(positions),
                min_group_size=self.required_group_size,
            )
        data = self.sampler[positions]
        tensor = torch.as_tensor(data, dtype=torch.float32)
        feature, label = _split_feature_label(tensor, device=None)
        return feature, label, date, instruments


def date_group_collate(batch: list[tuple[torch.Tensor, torch.Tensor, Any, list[Any]]]) -> tuple[torch.Tensor, torch.Tensor, Any, list[Any]]:
    if len(batch) != 1:
        raise GeneralPTNNLTRError(
            "ltr_query_index_invalid",
            "Date-grouped LTR dataloader expects one query group per batch",
            batch_size=len(batch),
        )
    return batch[0]


def _split_feature_label(data: torch.Tensor, *, device: torch.device | None) -> tuple[torch.Tensor, torch.Tensor]:
    if data.dim() == 3:
        feature = data[:, :, 0:-1]
        label = data[:, -1, -1]
    elif data.dim() == 2:
        feature = data[:, 0:-1]
        label = data[:, -1]
    else:
        raise ValueError("Unsupported data shape.")
    if device is not None:
        feature = feature.to(device)
        label = label.to(device)
    return feature, label


def _cross_section_quantile_relevance(label: torch.Tensor, *, bins: int, segment: str, date: Any) -> torch.Tensor:
    if label.numel() == 0:
        raise GeneralPTNNLTRError(
            "ltr_query_too_small",
            "LTR query has no finite labels",
            dataset_segment=segment,
            date=date,
            group_size=0,
        )
    if torch.max(label) <= torch.min(label):
        raise GeneralPTNNLTRError(
            "ltr_degenerate_relevance",
            "LTR query labels are constant and cannot form relevance bins",
            dataset_segment=segment,
            date=date,
            group_size=int(label.numel()),
        )
    order = torch.argsort(label, descending=False)
    ranks = torch.empty_like(order, dtype=torch.float32)
    ranks[order] = torch.arange(label.numel(), device=label.device, dtype=torch.float32)
    relevance = torch.floor(ranks * int(bins) / max(1, label.numel())).clamp(min=0, max=int(bins) - 1)
    return relevance.to(dtype=torch.float32)


def approx_ndcg_at_k_loss(
    scores: torch.Tensor,
    labels: torch.Tensor,
    *,
    topk_train_k: int,
    temperature: float = 1.0,
    gate_temperature: float = 1.0,
    relevance_mode: str = "cross_section_quantile",
    relevance_bins: int = 5,
    min_group_size: int | None = None,
    segment: str = "train",
    date: Any = None,
) -> tuple[torch.Tensor, torch.Tensor, int]:
    """Return ``(loss, exact_ndcg, valid_group_size)`` for one date query group."""

    scores = scores.reshape(-1).to(dtype=torch.float32)
    labels = labels.reshape(-1).to(dtype=torch.float32, device=scores.device)
    finite_mask = torch.isfinite(labels) & torch.isfinite(scores)
    if not torch.isfinite(labels).any():
        raise GeneralPTNNLTRError(
            "ltr_query_all_nan_label",
            "LTR query labels are all NaN or non-finite",
            dataset_segment=segment,
            date=date,
            group_size=int(labels.numel()),
        )
    scores = scores[finite_mask]
    labels = labels[finite_mask]
    valid_size = int(labels.numel())
    required_size = max(int(topk_train_k), int(min_group_size or topk_train_k))
    if valid_size < required_size:
        raise GeneralPTNNLTRError(
            "ltr_query_too_small",
            "LTR query has fewer finite samples than required",
            dataset_segment=segment,
            date=date,
            group_size=valid_size,
            min_group_size=required_size,
        )
    if relevance_mode != "cross_section_quantile":
        raise GeneralPTNNLTRError(
            "general_ptnn_ltr_invalid_loss_mode",
            "Unsupported LTR relevance mode",
            dataset_segment=segment,
            date=date,
            relevance_mode=relevance_mode,
        )
    relevance = _cross_section_quantile_relevance(labels, bins=int(relevance_bins), segment=segment, date=date)
    gains = torch.pow(torch.tensor(2.0, device=scores.device), relevance) - 1.0

    diff = (scores.reshape(1, -1) - scores.reshape(-1, 1)) / float(temperature)
    pairwise_higher = torch.sigmoid(torch.clamp(diff, min=-20.0, max=20.0))
    pairwise_higher = pairwise_higher * (1.0 - torch.eye(valid_size, device=scores.device, dtype=torch.float32))
    approx_rank = 1.0 + pairwise_higher.sum(dim=1)
    topk_gate = torch.sigmoid((float(topk_train_k) + 0.5 - approx_rank) / float(gate_temperature))
    discount = 1.0 / torch.log2(1.0 + approx_rank)
    approx_dcg = torch.sum(gains * discount * topk_gate)

    ideal_count = min(int(topk_train_k), valid_size)
    ideal_gains = torch.sort(gains, descending=True).values[:ideal_count]
    ideal_positions = torch.arange(2, ideal_count + 2, device=scores.device, dtype=torch.float32)
    ideal_dcg = torch.sum(ideal_gains / torch.log2(ideal_positions))
    if (not torch.isfinite(ideal_dcg)) or ideal_dcg <= 0:
        raise GeneralPTNNLTRError(
            "ltr_degenerate_relevance",
            "LTR query ideal DCG is not positive",
            dataset_segment=segment,
            date=date,
            group_size=valid_size,
        )

    loss = -(approx_dcg / ideal_dcg)
    if not torch.isfinite(loss):
        raise GeneralPTNNLTRError(
            "ltr_non_finite_loss",
            "ApproxNDCG@K loss is NaN or Inf",
            dataset_segment=segment,
            date=date,
            group_size=valid_size,
        )

    predicted_order = torch.argsort(scores, descending=True)[:ideal_count]
    exact_dcg = torch.sum(gains[predicted_order] / torch.log2(ideal_positions))
    exact_ndcg = exact_dcg / ideal_dcg
    if not torch.isfinite(exact_ndcg):
        raise GeneralPTNNLTRError(
            "ltr_non_finite_loss",
            "Exact NDCG@K diagnostic is NaN or Inf",
            dataset_segment=segment,
            date=date,
            group_size=valid_size,
        )
    return loss, exact_ndcg.detach(), valid_size


class AIStockGeneralPTNNLTR(Model):
    """Qlib-compatible GeneralPTNN adapter with opt-in ApproxNDCG@K."""

    def __init__(
        self,
        n_epochs: int = 200,
        lr: float = 0.001,
        metric: str = "",
        batch_size: int = 2000,
        early_stop: int = 20,
        loss: str = "mse",
        weight_decay: float = 0.0,
        optimizer: str = "adam",
        n_jobs: int = 10,
        GPU: int = 0,
        seed: int | None = None,
        pt_model_uri: str = "qlib.contrib.model.pytorch_gru_ts.GRUModel",
        pt_model_kwargs: dict[str, Any] | None = None,
        ltr_loss_mode: str = "mse",
        topk_train_k: int = 25,
        ltr_temperature: float = 1.0,
        ltr_gate_temperature: float = 1.0,
        ltr_relevance_mode: str = "cross_section_quantile",
        ltr_relevance_bins: int = 5,
        ltr_min_group_size: int = 25,
        ltr_fail_on_degenerate_group: bool = True,
        use_amp: bool = False,
        gradient_accumulation_steps: int = 1,
        pin_memory: bool = True,
        prefetch_factor: int = 2,
        persistent_workers: bool = False,
    ) -> None:
        self.logger = get_module_logger("AIStockGeneralPTNNLTR")
        self.logger.info("AIStockGeneralPTNNLTR pytorch version...")

        if loss == "approx_ndcg_at_k" and ltr_loss_mode == "mse":
            ltr_loss_mode = "approx_ndcg_at_k"
        if ltr_loss_mode == "approx_ndcg_at_k" and loss == "mse":
            loss = "approx_ndcg_at_k"
        self._validate_ltr_config(
            ltr_loss_mode=ltr_loss_mode,
            topk_train_k=topk_train_k,
            ltr_temperature=ltr_temperature,
            ltr_gate_temperature=ltr_gate_temperature,
            ltr_relevance_mode=ltr_relevance_mode,
            ltr_relevance_bins=ltr_relevance_bins,
            ltr_min_group_size=ltr_min_group_size,
        )
        if not ltr_fail_on_degenerate_group:
            raise GeneralPTNNLTRError(
                "ltr_degenerate_relevance",
                "ltr_fail_on_degenerate_group=false is not supported; degenerate queries must fail loud",
                loss_mode=ltr_loss_mode,
            )

        self.n_epochs = int(n_epochs)
        self.lr = float(lr)
        self.metric = metric
        self.batch_size = int(batch_size)
        self.early_stop = int(early_stop)
        self.optimizer = optimizer.lower()
        self.loss = loss
        self.ltr_loss_mode = ltr_loss_mode
        self.topk_train_k = int(topk_train_k)
        self.ltr_temperature = float(ltr_temperature)
        self.ltr_gate_temperature = float(ltr_gate_temperature)
        self.ltr_relevance_mode = ltr_relevance_mode
        self.ltr_relevance_bins = int(ltr_relevance_bins)
        self.ltr_min_group_size = int(ltr_min_group_size)
        self.ltr_fail_on_degenerate_group = bool(ltr_fail_on_degenerate_group)
        self.weight_decay = float(weight_decay)
        self.device = torch.device("cuda:%d" % GPU if torch.cuda.is_available() and GPU >= 0 else "cpu")
        self.n_jobs = int(n_jobs)
        self.seed = seed
        self.use_amp = bool(use_amp)
        self.gradient_accumulation_steps = max(1, int(gradient_accumulation_steps))
        self.pin_memory = bool(pin_memory)
        self.prefetch_factor = int(prefetch_factor)
        self.persistent_workers = bool(persistent_workers)

        self.pt_model_uri = pt_model_uri
        self.pt_model_kwargs = pt_model_kwargs or {
            "d_feat": 6,
            "hidden_size": 64,
            "num_layers": 2,
            "dropout": 0.0,
        }
        self.dnn_model = init_instance_by_config({"class": pt_model_uri, "kwargs": self.pt_model_kwargs})

        if self.seed is not None:
            np.random.seed(self.seed)
            torch.manual_seed(self.seed)

        self.logger.info(
            "AIStockGeneralPTNNLTR parameters setting:"
            "\nn_epochs : %s"
            "\nlr : %s"
            "\nmetric : %s"
            "\nbatch_size : %s"
            "\nearly_stop : %s"
            "\noptimizer : %s"
            "\nloss_type : %s"
            "\nltr_loss_mode : %s"
            "\ntopk_train_k : %s"
            "\ndevice : %s"
            "\nn_jobs : %s"
            "\nuse_GPU : %s"
            "\nweight_decay : %s"
            "\nseed : %s"
            "\npt_model_uri: %s"
            "\npt_model_kwargs: %s",
            self.n_epochs,
            self.lr,
            metric,
            self.batch_size,
            self.early_stop,
            self.optimizer,
            loss,
            self.ltr_loss_mode,
            self.topk_train_k,
            self.device,
            self.n_jobs,
            self.use_gpu,
            self.weight_decay,
            seed,
            pt_model_uri,
            self.pt_model_kwargs,
        )
        self.logger.info("model:\n%s", self.dnn_model)
        self.logger.info("model size: %.4f MB", count_parameters(self.dnn_model))

        if self.optimizer == "adam":
            self.train_optimizer = optim.Adam(self.dnn_model.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        elif self.optimizer == "gd":
            self.train_optimizer = optim.SGD(self.dnn_model.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        else:
            raise NotImplementedError("optimizer {} is not supported!".format(optimizer))

        self.lr_scheduler = ReduceLROnPlateau(
            self.train_optimizer, mode="min", factor=0.5, patience=5, min_lr=1e-6, threshold=1e-5
        )
        self.fitted = False
        self.dnn_model.to(self.device)

    @staticmethod
    def _validate_ltr_config(
        *,
        ltr_loss_mode: str,
        topk_train_k: int,
        ltr_temperature: float,
        ltr_gate_temperature: float,
        ltr_relevance_mode: str,
        ltr_relevance_bins: int,
        ltr_min_group_size: int,
    ) -> None:
        if ltr_loss_mode not in {"mse", "approx_ndcg_at_k"}:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_loss_mode",
                "Unsupported ltr_loss_mode",
                requested_loss=ltr_loss_mode,
            )
        try:
            topk_value = int(topk_train_k)
        except (TypeError, ValueError) as exc:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_topk_train_k",
                "topk_train_k must be a positive integer",
                topk_train_k=topk_train_k,
            ) from exc
        if topk_value <= 0:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_topk_train_k",
                "topk_train_k must be a positive integer",
                topk_train_k=topk_train_k,
            )
        if float(ltr_temperature) <= 0 or float(ltr_gate_temperature) <= 0:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_temperature",
                "LTR temperatures must be positive",
                ltr_temperature=ltr_temperature,
                ltr_gate_temperature=ltr_gate_temperature,
            )
        if ltr_relevance_mode != "cross_section_quantile":
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_loss_mode",
                "Only cross_section_quantile relevance is supported",
                ltr_relevance_mode=ltr_relevance_mode,
            )
        if int(ltr_relevance_bins) < 2:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_relevance_bins",
                "ltr_relevance_bins must be >= 2",
                ltr_relevance_bins=ltr_relevance_bins,
            )
        if int(ltr_min_group_size) <= 0:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_topk_train_k",
                "ltr_min_group_size must be positive",
                ltr_min_group_size=ltr_min_group_size,
            )

    @property
    def use_gpu(self) -> bool:
        return self.device != torch.device("cpu")

    def mse(self, pred: torch.Tensor, label: torch.Tensor, weight: torch.Tensor) -> torch.Tensor:
        loss = weight * (pred - label) ** 2
        return torch.mean(loss)

    def loss_fn(self, pred: torch.Tensor, label: torch.Tensor, weight: torch.Tensor | None = None) -> torch.Tensor:
        mask = ~torch.isnan(label)
        if weight is None:
            weight = torch.ones_like(label)
        if self.loss == "mse":
            return self.mse(pred[mask], label[mask].view(-1, 1), weight[mask])
        if self.loss == "approx_ndcg_at_k":
            loss, _, _ = approx_ndcg_at_k_loss(
                pred,
                label,
                topk_train_k=self.topk_train_k,
                temperature=self.ltr_temperature,
                gate_temperature=self.ltr_gate_temperature,
                relevance_mode=self.ltr_relevance_mode,
                relevance_bins=self.ltr_relevance_bins,
                min_group_size=self.ltr_min_group_size,
            )
            return loss
        raise ValueError("unknown loss `%s`" % self.loss)

    def metric_fn(self, pred: torch.Tensor, label: torch.Tensor) -> torch.Tensor:
        mask = torch.isfinite(label)
        if self.metric in ("", "loss"):
            return self.loss_fn(pred[mask], label[mask])
        raise ValueError("unknown metric `%s`" % self.metric)

    def _get_fl(self, data: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        return _split_feature_label(data, device=self.device)

    def train_epoch(self, data_loader: Iterable[Any]) -> None:
        self.dnn_model.train()
        for data, weight in data_loader:
            feature, label = self._get_fl(data)
            pred = self.dnn_model(feature.float())
            loss = self.loss_fn(pred, label, weight.to(self.device))
            self.train_optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_value_(self.dnn_model.parameters(), 3.0)
            self.train_optimizer.step()

    def test_epoch(self, data_loader: Iterable[Any]) -> tuple[float, float]:
        self.dnn_model.eval()
        scores = []
        losses = []
        for data, weight in data_loader:
            feature, label = self._get_fl(data)
            with torch.no_grad():
                pred = self.dnn_model(feature.float())
                loss = self.loss_fn(pred, label, weight.to(self.device))
                losses.append(loss.item())
                score = self.metric_fn(pred, label)
                scores.append(score.item())
        return np.mean(losses), np.mean(scores)

    def _ltr_loss_for_group(
        self,
        pred: torch.Tensor,
        label: torch.Tensor,
        *,
        date: Any,
        segment: str,
    ) -> tuple[torch.Tensor, torch.Tensor, int]:
        return approx_ndcg_at_k_loss(
            pred,
            label,
            topk_train_k=self.topk_train_k,
            temperature=self.ltr_temperature,
            gate_temperature=self.ltr_gate_temperature,
            relevance_mode=self.ltr_relevance_mode,
            relevance_bins=self.ltr_relevance_bins,
            min_group_size=self.ltr_min_group_size,
            segment=segment,
            date=date,
        )

    def train_ltr_epoch(self, data_loader: Iterable[Any]) -> dict[str, Any]:
        self.dnn_model.train()
        diagnostics = LTRBatchDiagnostics()
        self.train_optimizer.zero_grad()
        pending_steps = 0
        for batch_index, (feature, label, date, _instruments) in enumerate(data_loader, start=1):
            feature = feature.to(self.device, dtype=torch.float32)
            label = label.to(self.device, dtype=torch.float32)
            pred = self.dnn_model(feature)
            loss, ndcg, group_size = self._ltr_loss_for_group(pred, label, date=date, segment="train")
            (loss / self.gradient_accumulation_steps).backward()
            pending_steps += 1
            if batch_index % self.gradient_accumulation_steps == 0:
                torch.nn.utils.clip_grad_value_(self.dnn_model.parameters(), 3.0)
                self.train_optimizer.step()
                self.train_optimizer.zero_grad()
                pending_steps = 0
            diagnostics.add(loss=loss.item(), ndcg=ndcg.item(), group_size=group_size)
        if pending_steps:
            torch.nn.utils.clip_grad_value_(self.dnn_model.parameters(), 3.0)
            self.train_optimizer.step()
            self.train_optimizer.zero_grad()
        return diagnostics.as_dict(loss_mode=self.ltr_loss_mode, topk_train_k=self.topk_train_k)

    def test_ltr_epoch(self, data_loader: Iterable[Any], *, segment: str) -> dict[str, Any]:
        self.dnn_model.eval()
        diagnostics = LTRBatchDiagnostics()
        for feature, label, date, _instruments in data_loader:
            feature = feature.to(self.device, dtype=torch.float32)
            label = label.to(self.device, dtype=torch.float32)
            with torch.no_grad():
                pred = self.dnn_model(feature)
                loss, ndcg, group_size = self._ltr_loss_for_group(pred, label, date=date, segment=segment)
            diagnostics.add(loss=loss.item(), ndcg=ndcg.item(), group_size=group_size)
        return diagnostics.as_dict(loss_mode=self.ltr_loss_mode, topk_train_k=self.topk_train_k)

    def _fit_mse(
        self,
        dataset: Union[DatasetH, TSDatasetH],
        evals_result: dict[str, Any],
        save_path: str | None,
        reweighter: Any,
    ) -> None:
        ists = isinstance(dataset, TSDatasetH)
        dl_train = dataset.prepare("train", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        dl_valid = dataset.prepare("valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        self.logger.info("Train samples: %s", len(dl_train))
        self.logger.info("Valid samples: %s", len(dl_valid))
        if dl_train.empty or dl_valid.empty:
            raise ValueError("Empty data from dataset, please check your dataset config.")

        if reweighter is None:
            wl_train = np.ones(len(dl_train))
            wl_valid = np.ones(len(dl_valid))
        elif isinstance(reweighter, Reweighter):
            wl_train = reweighter.reweight(dl_train)
            wl_valid = reweighter.reweight(dl_valid)
        else:
            raise ValueError("Unsupported reweighter type.")

        if ists:
            dl_train.config(fillna_type="ffill+bfill")
            dl_valid.config(fillna_type="ffill+bfill")
        else:
            dl_train = dl_train.values
            dl_valid = dl_valid.values

        train_loader = DataLoader(
            ConcatDataset(dl_train, wl_train),
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.n_jobs,
            drop_last=True,
        )
        valid_loader = DataLoader(
            ConcatDataset(dl_valid, wl_valid),
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.n_jobs,
            drop_last=True,
        )
        self._run_epoch_loop(train_loader, valid_loader, evals_result=evals_result, save_path=save_path)

    def _fit_ltr(
        self,
        dataset: Union[DatasetH, TSDatasetH],
        evals_result: dict[str, Any],
        save_path: str | None,
        reweighter: Any,
    ) -> None:
        if not isinstance(dataset, TSDatasetH):
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_requires_timeseries_dataset",
                "ApproxNDCG@K requires TSDatasetH so query groups preserve datetime/instrument",
                loss_mode=self.ltr_loss_mode,
            )
        if reweighter is not None:
            raise GeneralPTNNLTRError(
                "general_ptnn_ltr_invalid_loss_mode",
                "ApproxNDCG@K does not support reweighter in the first implementation",
                loss_mode=self.ltr_loss_mode,
            )

        dl_train = dataset.prepare("train", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        dl_valid = dataset.prepare("valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        self.logger.info("Train samples: %s", len(dl_train))
        self.logger.info("Valid samples: %s", len(dl_valid))
        if dl_train.empty or dl_valid.empty:
            raise ValueError("Empty data from dataset, please check your dataset config.")
        dl_train.config(fillna_type="ffill+bfill")
        dl_valid.config(fillna_type="ffill+bfill")

        train_groups = DateGroupedDataset(
            dl_train,
            segment="train",
            min_group_size=self.ltr_min_group_size,
            topk_train_k=self.topk_train_k,
        )
        valid_groups = DateGroupedDataset(
            dl_valid,
            segment="valid",
            min_group_size=self.ltr_min_group_size,
            topk_train_k=self.topk_train_k,
        )
        train_loader = DataLoader(train_groups, batch_size=1, shuffle=True, num_workers=0, collate_fn=date_group_collate)
        valid_loader = DataLoader(valid_groups, batch_size=1, shuffle=False, num_workers=0, collate_fn=date_group_collate)
        self._run_ltr_epoch_loop(train_loader, valid_loader, evals_result=evals_result, save_path=save_path)

    def _run_epoch_loop(
        self,
        train_loader: Iterable[Any],
        valid_loader: Iterable[Any],
        *,
        evals_result: dict[str, Any],
        save_path: str | None,
    ) -> None:
        save_path = get_or_create_path(save_path)
        stop_steps = 0
        best_score = np.inf
        best_epoch = 0
        best_param = copy.deepcopy(self.dnn_model.state_dict())
        evals_result["train"] = []
        evals_result["valid"] = []
        self.logger.info("training...")
        self.fitted = True
        for step in range(self.n_epochs):
            self.logger.info("Epoch%d:", step)
            self.logger.info("training...")
            self.train_epoch(train_loader)
            self.logger.info("evaluating...")
            _train_loss, train_score = self.test_epoch(train_loader)
            _val_loss, val_score = self.test_epoch(valid_loader)
            self.logger.info("Epoch%d: train %.6f, valid %.6f", step, train_score, val_score)
            evals_result["train"].append(train_score)
            evals_result["valid"].append(val_score)
            self.lr_scheduler.step(val_score)
            if val_score < best_score:
                best_score = val_score
                stop_steps = 0
                best_epoch = step
                best_param = copy.deepcopy(self.dnn_model.state_dict())
            else:
                stop_steps += 1
                if stop_steps >= self.early_stop:
                    self.logger.info("early stop")
                    break
        self.logger.info("best score: %.6lf @ %d epoch", best_score, best_epoch)
        self.dnn_model.load_state_dict(best_param)
        torch.save(best_param, save_path)
        if self.use_gpu:
            torch.cuda.empty_cache()

    def _run_ltr_epoch_loop(
        self,
        train_loader: Iterable[Any],
        valid_loader: Iterable[Any],
        *,
        evals_result: dict[str, Any],
        save_path: str | None,
    ) -> None:
        save_path = get_or_create_path(save_path)
        stop_steps = 0
        best_score = np.inf
        best_epoch = 0
        best_param = copy.deepcopy(self.dnn_model.state_dict())
        evals_result["train"] = []
        evals_result["valid"] = []
        evals_result["train_ndcg_at_k"] = []
        evals_result["valid_ndcg_at_k"] = []
        evals_result["ltr_diagnostics"] = []
        self.logger.info("training...")
        self.fitted = True
        for step in range(self.n_epochs):
            self.logger.info("Epoch%d:", step)
            self.logger.info("training LTR...")
            train_diag = self.train_ltr_epoch(train_loader)
            self.logger.info("evaluating LTR...")
            valid_diag = self.test_ltr_epoch(valid_loader, segment="valid")
            train_score = float(train_diag["loss"])
            val_score = float(valid_diag["loss"])
            self.logger.info(
                "Epoch%d LTR: train_loss %.6f valid_loss %.6f train_ndcg %.6f valid_ndcg %.6f "
                "train_queries %s valid_queries %s group_size[min/median/max]=%s/%s/%s",
                step,
                train_score,
                val_score,
                float(train_diag["ndcg_at_k"]),
                float(valid_diag["ndcg_at_k"]),
                train_diag["query_count"],
                valid_diag["query_count"],
                valid_diag["min_group_size"],
                valid_diag["median_group_size"],
                valid_diag["max_group_size"],
            )
            evals_result["train"].append(train_score)
            evals_result["valid"].append(val_score)
            evals_result["train_ndcg_at_k"].append(float(train_diag["ndcg_at_k"]))
            evals_result["valid_ndcg_at_k"].append(float(valid_diag["ndcg_at_k"]))
            evals_result["ltr_diagnostics"].append({"epoch": step, "train": train_diag, "valid": valid_diag})
            self.lr_scheduler.step(val_score)
            if val_score < best_score:
                best_score = val_score
                stop_steps = 0
                best_epoch = step
                best_param = copy.deepcopy(self.dnn_model.state_dict())
            else:
                stop_steps += 1
                if stop_steps >= self.early_stop:
                    self.logger.info("early stop")
                    break
        self.logger.info("best score: %.6lf @ %d epoch", best_score, best_epoch)
        self.dnn_model.load_state_dict(best_param)
        torch.save(best_param, save_path)
        if self.use_gpu:
            torch.cuda.empty_cache()

    def fit(
        self,
        dataset: Union[DatasetH, TSDatasetH],
        evals_result: dict[str, Any] = dict(),
        save_path: str | None = None,
        reweighter: Any = None,
    ) -> None:
        if self.ltr_loss_mode == "approx_ndcg_at_k":
            self._fit_ltr(dataset, evals_result, save_path, reweighter)
            return
        self._fit_mse(dataset, evals_result, save_path, reweighter)

    def predict(
        self,
        dataset: Union[DatasetH, TSDatasetH],
        batch_size: int | None = None,
        n_jobs: int | None = None,
    ) -> pd.Series:
        if not self.fitted:
            raise ValueError("model is not fitted yet!")
        effective_batch_size = int(batch_size or self.batch_size)
        effective_n_jobs = int(n_jobs if n_jobs is not None else self.n_jobs)
        dl_test = dataset.prepare("test", col_set=["feature", "label"], data_key=DataHandlerLP.DK_I)
        self.logger.info("Test samples: %s", len(dl_test))
        if isinstance(dataset, TSDatasetH):
            dl_test.config(fillna_type="ffill+bfill")
            index = dl_test.get_index()
        else:
            index = dl_test.index
            dl_test = dl_test.values
        test_loader = DataLoader(dl_test, batch_size=effective_batch_size, num_workers=effective_n_jobs)
        self.dnn_model.eval()
        preds = []
        for data in test_loader:
            feature, _ = self._get_fl(data)
            feature = feature.to(self.device)
            with torch.no_grad():
                pred = self.dnn_model(feature.float()).detach().cpu().numpy()
            preds.append(pred)
        preds_concat = np.concatenate(preds)
        if preds_concat.ndim != 1:
            preds_concat = preds_concat.ravel()
        return pd.Series(preds_concat, index=index)
