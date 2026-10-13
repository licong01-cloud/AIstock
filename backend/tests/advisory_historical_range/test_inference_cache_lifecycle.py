from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend import inference_engine
from backend.services.advisory_historical_range.inference_session import FrozenInferProcessorBundle, InferenceResourceSession


class _CallLocalProcessor:
    def __init__(self):
        self.calls = 0

    def __call__(self, frame):
        self.calls += 1
        return frame + self.calls

    def __deepcopy__(self, memo):
        raise TypeError("custom fitted processor does not support deepcopy")


class _NonSerializableProcessor:
    def __getstate__(self):
        raise ValueError("custom fitted state is intentionally not serializable")


class _SharedNetwork:
    def __deepcopy__(self, memo):
        return self


def test_dataset_loaded_once_for_sequence_and_independent_infer_processor_calls(tmp_path):
    path = tmp_path / "processor.pkl"
    path.write_bytes(b"admitted-test-asset")
    count = []

    def load(source):
        count.append(source)
        return SimpleNamespace(step_len=3, handler=SimpleNamespace(infer_processors=[_CallLocalProcessor()]), unused_training_data=np.zeros(10000))

    session = InferenceResourceSession()
    bundle = session.processors(path=path, namespace="package/model/schema-v1", loader=load)
    primary = {"dataset_processor_relpath": path.name}
    assert inference_engine._saved_qe_step_len(tmp_path, primary, bundle) == 3
    features = pd.DataFrame({"factor_a": [4.0]})
    for _ in range(2):
        cached = session.processors(path=path, namespace="package/model/schema-v1", loader=load)
        result = inference_engine._apply_saved_qe_infer_processors(features, task_dir=tmp_path, primary_assets=primary, processor_bundle=cached)
        assert result.iloc[0, 0] == 5.0
    assert len(count) == 1
    assert not hasattr(bundle, "unused_training_data")
    assert bundle.for_call()[0].calls == 0
    assert features.iloc[0, 0] == 4.0
    session.close()
    assert session.cache_bytes == 0


def test_resource_budget_namespace_and_asset_revision_do_not_return_stale_values(tmp_path):
    path = tmp_path / "asset"
    path.write_bytes(b"one")
    session = InferenceResourceSession(max_bytes=3, max_entries=1)
    def load(namespace):
        return session.load(path=path, namespace=namespace, kind="test", loader=lambda p: p.read_bytes(), size_of=len)
    assert load("v1") == b"one"
    assert load("v1") == b"one"
    assert session.loads == 1
    assert load("v2") == b"one"
    assert session.loads == 2
    path.write_bytes(b"large-new-value")
    assert load("v2") == b"large-new-value"
    assert load("v2") == b"large-new-value"
    assert session.loads == 4
    assert session.cache_bytes <= 3
    def changing_loader(source):
        value = source.read_bytes()
        source.write_bytes(b"asset-modified-during-load")
        return value
    with pytest.raises(RuntimeError, match="changed during load"):
        session.load(path=path, namespace="during-load", kind="test", loader=changing_loader, size_of=len)


def test_concurrent_resource_build_is_single_flight(tmp_path):
    path = tmp_path / "asset"
    path.write_bytes(b"one")
    session = InferenceResourceSession()

    def load(_):
        return session.load(path=path, namespace="v1", kind="test", loader=lambda p: p.read_bytes(), size_of=len)

    with ThreadPoolExecutor(max_workers=3) as workers:
        assert list(workers.map(load, range(6))) == [b"one"] * 6
    assert session.loads == 1
    assert session.hits == 5


def test_model_loaded_once_but_each_forward_has_fresh_mutable_state(tmp_path):
    path = tmp_path / "model.pkl"
    path.write_bytes(b"frozen-model")
    session = InferenceResourceSession()
    count = []

    def load(source):
        count.append(source)
        network = SimpleNamespace(weights=np.array([2.0]), calls=0)
        return SimpleNamespace(dnn_model=network), "pytorch", network, 1

    for value in (3.0, 4.0, 3.0):
        model, _, network, _ = session.model(path=path, namespace="model/code/device", loader=load)
        assert model.dnn_model is network
        assert network.calls == 0
        assert float(network.weights[0] * value) == 2 * value
        network.calls += 1
        network.weights[0] = -99.0
    assert len(count) == 1
    assert session.hits == 2


def test_optional_reuse_never_requires_custom_resource_serialization_or_copying(tmp_path):
    processor = _NonSerializableProcessor()
    bundle = FrozenInferProcessorBundle.from_dataset(SimpleNamespace(
        step_len=3, handler=SimpleNamespace(infer_processors=[processor]),
    ))
    assert bundle.for_call() == [processor]
    assert InferenceResourceSession._processor_size(bundle) is None

    path = tmp_path / "model.pkl"
    path.write_bytes(b"frozen-model")
    loaded = []
    def load(_):
        network = _SharedNetwork()
        loaded.append(network)
        return SimpleNamespace(dnn_model=network), "pytorch", network, 1
    session = InferenceResourceSession()
    model, _, network, _ = session.model(path=path, namespace="custom-model", loader=load)
    assert model.dnn_model is network is loaded[-1]
    assert network is not loaded[0]
    assert session.cache_bytes == 0


def test_lazy_package_exports_keep_the_existing_business_type_identity():
    from backend.services import advisory_historical_range
    from backend.services.advisory_historical_range import models

    for name in advisory_historical_range.__all__:
        assert getattr(advisory_historical_range, name) is getattr(models, name)
