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
    AIStockGeneralPTNNLTR,
    DateGroupedDataset,
    GeneralPTNNLTRError,
    LTR_LOSS_MODES,
    approx_ndcg_at_k_loss,
    ndcg_at_k_loss,
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


@pytest.mark.parametrize("mode", sorted(LTR_LOSS_MODES))
@pytest.mark.parametrize("group_size,score_span", [(512, 0.0), (4500, 0.1)])
def test_market_scale_ltr_has_informative_gradients(group_size: int, score_span: float, mode: str) -> None:
    # One real-sized daily query, without datasets, services, CUDA or model training.
    previous_threads = torch.get_num_threads()
    torch.set_num_threads(1)
    try:
        labels = torch.linspace(-0.2, 0.2, group_size)
        scores = torch.linspace(score_span, -score_span, group_size).requires_grad_()
        loss, ndcg, size = ndcg_at_k_loss(scores, labels, topk_train_k=50, min_group_size=50, loss_mode=mode)
        loss.backward()
        assert size == group_size
        assert torch.isfinite(loss) and torch.isfinite(ndcg)
        assert torch.isfinite(scores.grad).all()
        assert torch.count_nonzero(scores.grad) > group_size // 2
        # Gradient descent must raise high-relevance scores relative to low ones.
        assert scores.grad[-group_size // 5 :].mean() < scores.grad[: group_size // 5].mean()
    finally:
        torch.set_num_threads(previous_threads)


@pytest.mark.parametrize("mode", sorted(LTR_LOSS_MODES))
def test_ranking_loss_matches_independent_reference_and_gradient(mode):
    # Five distinct labels map to gains 0,1,3,7,15; no tied score boundary.
    labels = torch.arange(5, dtype=torch.float32)
    scores = torch.tensor([0.2, -0.7, 0.5, 0.1, 0.9], requires_grad=True)
    loss, ndcg, _ = ndcg_at_k_loss(scores, labels, topk_train_k=3, loss_mode=mode)
    ref_scores = scores.detach().clone().requires_grad_()
    gains = torch.tensor([0., 1., 3., 7., 15.])
    ideal = sum(gains[4 - i] / np.log2(i + 2) for i in range(3))
    order = torch.argsort(ref_scores, descending=True).tolist()
    if mode == "log_approx_ndcg_at_k":
        dcg = 0
        for i in range(5):
            rank = 1 + sum(torch.sigmoid(ref_scores[j] - ref_scores[i]) for j in range(5) if i != j)
            dcg = dcg + gains[i] * torch.sigmoid(3.5 - rank) / torch.log2(1 + rank)
        ref_loss = -torch.log(dcg / ideal)
    else:
        total, denominator = 0, 0
        for i in range(5):
            for j in range(i + 1, 5):
                ri, rj = order.index(i), order.index(j)
                di = 1 / np.log2(ri + 2) if ri < 3 else 0
                dj = 1 / np.log2(rj + 2) if rj < 3 else 0
                weight = abs((gains[i] - gains[j]) * (di - dj)) / ideal
                margin = torch.sign(gains[i] - gains[j]) * (ref_scores[i] - ref_scores[j])
                total = total + weight * torch.nn.functional.softplus(-margin)
                denominator = denominator + weight
        ref_loss = total / denominator
    torch.testing.assert_close(loss, ref_loss)
    torch.testing.assert_close(
        torch.autograd.grad(loss, scores)[0], torch.autograd.grad(ref_loss, ref_scores)[0],
    )
    exact = sum(gains[order[i]] / np.log2(i + 2) for i in range(3)) / ideal
    torch.testing.assert_close(ndcg, exact)
    perm = torch.tensor([4, 2, 0, 3, 1])
    permuted, _, _ = ndcg_at_k_loss(scores[perm] + 7, labels[perm], topk_train_k=3, loss_mode=mode)
    torch.testing.assert_close(permuted, loss)


@pytest.mark.parametrize("mode", sorted(LTR_LOSS_MODES))
def test_adapter_dispatch_diagnostics_and_fail_closed(mode):
    # Exercise the real constructor/epoch interface on CPU, no fit or data read.
    model = AIStockGeneralPTNNLTR(
        loss=mode, GPU=-1, n_jobs=0, topk_train_k=3, ltr_min_group_size=3,
        pt_model_uri="torch.nn.Linear", pt_model_kwargs={"in_features": 1, "out_features": 1},
    )
    scores = torch.tensor([0.2, 0.1, 0.3, 0.7, 0.5])
    labels = torch.arange(5, dtype=torch.float32)
    expected, _, _ = ndcg_at_k_loss(scores, labels, topk_train_k=3, loss_mode=mode)
    torch.testing.assert_close(model.loss_fn(scores, labels), expected)
    assert model.loss == model.ltr_loss_mode == mode
    diag = model.test_ltr_epoch([(scores[:, None], labels, "2024-01-02", [])], segment="valid")
    assert diag["loss_mode"] == mode and diag["query_count"] == 1
    for invalid_labels, reason in [(torch.zeros(5), "ltr_degenerate_relevance"),
                                   (torch.full((5,), float("nan")), "ltr_query_all_nan_label")]:
        with pytest.raises(GeneralPTNNLTRError, match=reason):
            model.loss_fn(scores, invalid_labels)
    with pytest.raises(GeneralPTNNLTRError, match="ltr_query_too_small"):
        model.loss_fn(scores[:2], labels[:2])
    with pytest.raises(GeneralPTNNLTRError, match="general_ptnn_ltr_invalid_loss_mode"):
        AIStockGeneralPTNNLTR(loss=mode, ltr_loss_mode=next(iter(LTR_LOSS_MODES - {mode})))


@pytest.mark.parametrize("mode", sorted(LTR_LOSS_MODES))
def test_ranking_optimizer_updates_and_mse_is_unchanged(mode):
    from qlib.contrib.model.pytorch_general_nn import GeneralPTNN

    kwargs = dict(GPU=-1, n_jobs=0, seed=42, pt_model_uri="torch.nn.Linear",
                  pt_model_kwargs={"in_features": 1, "out_features": 1})
    # qrun applies the runtime seed before construction for every arm.
    torch.manual_seed(42)
    mse = GeneralPTNN(**kwargs)
    torch.manual_seed(42)
    ranking = AIStockGeneralPTNNLTR(loss=mode, topk_train_k=3, ltr_min_group_size=3, **kwargs)
    torch.testing.assert_close(mse.dnn_model.weight, ranking.dnn_model.weight)
    scores, labels = torch.arange(5, dtype=torch.float32)[:, None], torch.arange(5, dtype=torch.float32)
    before = ranking.dnn_model.weight.detach().clone()
    diag = ranking.train_ltr_epoch([(scores, labels, "2024-01-02", [])])
    assert diag["loss_mode"] == mode and diag["query_count"] == 1
    assert torch.isfinite(ranking.dnn_model.weight).all()
    assert not torch.equal(before, ranking.dnn_model.weight)
    legacy = AIStockGeneralPTNNLTR(**kwargs)
    labels[0] = float("nan")
    torch.testing.assert_close(legacy.loss_fn(scores, labels), mse.loss_fn(scores, labels))


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
