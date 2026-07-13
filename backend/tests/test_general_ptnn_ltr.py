from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("qlib")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AISTOCK_MODELS_ROOT = PROJECT_ROOT / "aistock_models"
if str(AISTOCK_MODELS_ROOT) not in sys.path:
    sys.path.insert(0, str(AISTOCK_MODELS_ROOT))

from aistock_models.general_ptnn_ltr import (  # noqa: E402
    DateGroupedDataset,
    GeneralPTNNLTRError,
    approx_ndcg_at_k_loss,
)


class _SimpleTSSampler:
    def __init__(self, index: pd.MultiIndex, data: np.ndarray) -> None:
        self._index = index
        self._data = data

    def get_index(self) -> pd.MultiIndex:
        return self._index

    def __getitem__(self, idx):
        return self._data[idx]


def _sampler_for_dates(group_sizes: list[int], *, step_len: int = 2) -> _SimpleTSSampler:
    dates = []
    instruments = []
    rows = []
    for date_offset, size in enumerate(group_sizes):
        date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=date_offset)
        for instrument_idx in range(size):
            dates.append(date)
            instruments.append(f"STK{date_offset}{instrument_idx:03d}")
            label = float(instrument_idx)
            rows.append([[1.0, label], [2.0, label]])
    index = pd.MultiIndex.from_arrays([dates, instruments], names=["datetime", "instrument"])
    data = np.asarray(rows, dtype=np.float32).reshape(len(rows), step_len, 2)
    return _SimpleTSSampler(index=index, data=data)


def test_approx_ndcg_label_direction_high_return_score_reduces_loss() -> None:
    labels = torch.tensor([0.01, 0.02, 0.03, 0.04, 0.20], dtype=torch.float32)
    neutral_scores = torch.zeros_like(labels)
    improved_scores = torch.tensor([0.0, 0.0, 0.0, 0.0, 3.0], dtype=torch.float32)

    neutral_loss, _neutral_ndcg, _ = approx_ndcg_at_k_loss(
        neutral_scores,
        labels,
        topk_train_k=3,
        min_group_size=3,
        relevance_bins=5,
        segment="train",
        date="2024-01-02",
    )
    improved_loss, improved_ndcg, _ = approx_ndcg_at_k_loss(
        improved_scores,
        labels,
        topk_train_k=3,
        min_group_size=3,
        relevance_bins=5,
        segment="train",
        date="2024-01-02",
    )

    assert improved_loss.item() < neutral_loss.item()
    assert 0.0 <= improved_ndcg.item() <= 1.0


def test_date_grouped_dataset_returns_one_cross_section_per_batch() -> None:
    sampler = _sampler_for_dates([4, 5])
    grouped = DateGroupedDataset(sampler, segment="train", min_group_size=3, topk_train_k=3)

    feature, label, date, instruments = grouped[0]

    assert feature.shape == (4, 2, 1)
    assert label.shape == (4,)
    assert date == pd.Timestamp("2024-01-01")
    assert instruments == ["STK0000", "STK0001", "STK0002", "STK0003"]
    original_dates = sampler.get_index().get_level_values("datetime")[: len(instruments)]
    assert set(original_dates) == {date}


def test_ltr_fail_loud_for_all_nan_tiny_group_and_invalid_index() -> None:
    labels = torch.tensor([float("nan"), float("nan"), float("nan")], dtype=torch.float32)
    scores = torch.tensor([0.1, 0.2, 0.3], dtype=torch.float32)
    with pytest.raises(GeneralPTNNLTRError, match="ltr_query_all_nan_label"):
        approx_ndcg_at_k_loss(scores, labels, topk_train_k=2, min_group_size=2, segment="valid", date="nan-day")

    with pytest.raises(GeneralPTNNLTRError, match="ltr_query_too_small"):
        DateGroupedDataset(_sampler_for_dates([1]), segment="train", min_group_size=2, topk_train_k=2)[0]

    invalid_index = pd.MultiIndex.from_arrays(
        [["not-a-date", "also-bad", "still-bad"], ["a", "b", "c"]],
        names=["datetime", "instrument"],
    )
    invalid_sampler = _SimpleTSSampler(invalid_index, np.zeros((3, 2, 2), dtype=np.float32))  # type: ignore[arg-type]
    with pytest.raises(GeneralPTNNLTRError, match="ltr_query_index_invalid"):
        DateGroupedDataset(invalid_sampler, segment="train", min_group_size=2, topk_train_k=2)


def _install_train_per_stock_import_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    module_names = [
        "next_app",
        "next_app.backend",
        "next_app.backend.db",
        "next_app.backend.quant_datasets",
    ]
    for name in module_names:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))

    pg_pool = types.ModuleType("next_app.backend.db.pg_pool")
    pg_pool.get_conn = lambda: pytest.fail("per-stock LTR fail-fast must not reach DB")
    monkeypatch.setitem(sys.modules, "next_app.backend.db.pg_pool", pg_pool)

    dataset_module = types.ModuleType("next_app.backend.quant_datasets.lstm_dataset")

    class LSTMDatasetConfig:
        pass

    dataset_module.LSTMDatasetConfig = LSTMDatasetConfig
    dataset_module.load_lstm_timeseries_for_symbol = lambda *_args, **_kwargs: pytest.fail(
        "per-stock LTR fail-fast must not load data"
    )
    monkeypatch.setitem(sys.modules, "next_app.backend.quant_datasets.lstm_dataset", dataset_module)


def test_train_per_stock_rejects_listwise_before_loading_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_train_per_stock_import_stubs(monkeypatch)
    module_name = "backend.quant_models.lstm.train_per_stock"
    sys.modules.pop(module_name, None)
    train_per_stock = importlib.import_module(module_name)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "train_per_stock.py",
            "--symbol",
            "SH600000",
            "--start",
            "2020-01-01T09:30:00",
            "--end",
            "2020-02-01T15:00:00",
            "--seq-len",
            "60",
            "--ltr-loss-mode",
            "approx_ndcg_at_k",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        train_per_stock.main()

    message = str(exc_info.value)
    assert "reason_code=ltr_per_stock_not_supported" in message
    assert "symbol=SH600000" in message
    assert "seq_len=60" in message
    assert "requested_loss=approx_ndcg_at_k" in message
