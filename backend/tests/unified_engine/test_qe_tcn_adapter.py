from __future__ import annotations

import pytest


torch = pytest.importorskip("torch")
pytest.importorskip("qlib")

from aistock_models.tcn import AIStockTCN  # noqa: E402


def test_tcn_adapter_applies_weight_decay_to_optimizer():
    model = AIStockTCN(
        d_feat=2,
        n_chans=4,
        kernel_size=2,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        batch_size=4,
        n_jobs=0,
        GPU=-1,
        weight_decay=0.0125,
        seed=123,
    )

    assert model.weight_decay == pytest.approx(0.0125)
    assert model.train_optimizer.param_groups
    assert all(
        group["weight_decay"] == pytest.approx(0.0125)
        for group in model.train_optimizer.param_groups
    )


@pytest.mark.parametrize("weight_decay", [-0.1, "not-a-number"])
def test_tcn_adapter_rejects_invalid_weight_decay(weight_decay):
    with pytest.raises(ValueError, match="reason_code=qe_tcn_weight_decay_invalid"):
        AIStockTCN(weight_decay=weight_decay)


def test_tcn_adapter_transposes_time_and_feature_axes_explicitly():
    model = AIStockTCN(
        d_feat=3,
        n_chans=4,
        kernel_size=2,
        num_layers=1,
        n_epochs=1,
        batch_size=2,
        n_jobs=0,
        GPU=-1,
    )
    batch = torch.arange(2 * 5 * 4, dtype=torch.float32).reshape(2, 5, 4)

    feature, label = model._split_batch(batch)

    assert feature.shape == (2, 3, 5)
    assert torch.equal(feature.cpu(), batch[:, :, 0:-1].transpose(1, 2))
    assert torch.equal(label.cpu(), batch[:, -1, -1])


def test_tcn_adapter_rejects_configured_feature_dimension_drift():
    model = AIStockTCN(
        d_feat=2,
        n_chans=4,
        kernel_size=2,
        num_layers=1,
        n_epochs=1,
        batch_size=2,
        n_jobs=0,
        GPU=-1,
    )
    batch = torch.zeros((2, 5, 4), dtype=torch.float32)

    with pytest.raises(ValueError, match="reason_code=qe_tcn_feature_dimension_mismatch"):
        model._split_batch(batch)


def test_tcn_adapter_prediction_uses_the_same_channel_first_contract():
    class TinySampler(torch.utils.data.Dataset):
        def __init__(self):
            self.data = torch.arange(2 * 5 * 4, dtype=torch.float32).reshape(2, 5, 4)

        def __len__(self):
            return len(self.data)

        def __getitem__(self, index):
            return self.data[index]

        def config(self, **kwargs):
            assert kwargs == {"fillna_type": "ffill+bfill"}

        def get_index(self):
            return ["sample-1", "sample-2"]

    class TinyDataset:
        def __init__(self):
            self.sampler = TinySampler()

        def prepare(self, segment, *, col_set, data_key):
            assert segment == "test"
            assert col_set == ["feature", "label"]
            assert data_key is not None
            return self.sampler

    model = AIStockTCN(
        d_feat=3,
        n_chans=4,
        kernel_size=2,
        num_layers=1,
        n_epochs=1,
        batch_size=2,
        n_jobs=0,
        GPU=-1,
    )
    model.fitted = True

    prediction = model.predict(TinyDataset())

    assert list(prediction.index) == ["sample-1", "sample-2"]
    assert len(prediction) == 2
    assert prediction.notna().all()
