from __future__ import annotations

import hashlib
import io
import pickle
import tarfile
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.strategy_package.frozen_runtime_self_check import runtime_asset_closure_sha256
from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
from backend.services.strategy_package.manifest import compute_manifest_json_sha256, freeze_manifest
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.package_asset_freeze import (
    PackageAssetBytes, PackageAssetFreezeService, QERuntimeAssetLocator, _fitted_dataset_from_mlruns_archive,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import ArtifactGenerationFailedError, DataUnavailableError, PackageAssetInvalidError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


CONF = b"task:\n  dataset:\n    kwargs:\n      handler:\n        kwargs:\n          infer_processors:\n            - class: RobustZScoreNorm\n            - class: Fillna\n"
DATASET = pickle.dumps({"fitted_mean": [10.0, 20.0], "fitted_scale": [2.0, 4.0]})


class Source:
    def fitted_preprocessor_bytes(self, manifest, *, model_weight_sha256):
        assert model_weight_sha256 == hashlib.sha256(b"weights").hexdigest()
        return PackageAssetBytes(DATASET, "unit://original-recorder/dataset")


def freeze(tmp_path, source=None, conf=CONF):
    store = LocalPackageAssetStore(tmp_path / "assets")
    freezer = PackageAssetFreezeService(
        asset_store=store, source=source or Source(),
        conf_yaml_reader=lambda m: PackageAssetBytes(conf),
        model_params_reader=lambda m: PackageAssetBytes(b"weights"),
        factor_code_reader=lambda f, m: PackageAssetBytes(b"VALUE = 1\n"),
    )
    return store, freezer.freeze_manifest_assets(make_manifest())


def test_freeze_protects_original_fitted_processors_and_frozen_runtime_needs_no_qe_source(tmp_path):
    store, result = freeze(tmp_path)
    model = result.manifest.model_asset
    asset = getattr(model, "preprocessor_asset", None)
    assert asset is not None
    assert store.get(asset.asset_ref) == DATASET
    assert asset.sha256 == hashlib.sha256(DATASET).hexdigest()
    rows = [a for a in result.assets if a.asset_type == StrategyPackageAssetType.PREPROCESSOR]
    assert len(rows) == 1 and rows[0].protected_asset is True
    assert rows[0].asset_sha256 == asset.sha256
    resolver = QEExperimentRuntimeAssetResolver(
        asset_store=store, cache_root=tmp_path / "cache",
        conn_factory=lambda: pytest.fail("package-owned processors must not query QE records"),
    )
    runtime = resolver.load_frozen_source_for_strategy_package(
        manifest=result.manifest, package_id=result.manifest.package_id
    )
    path = next(runtime.asset_workspace_path.glob("**/artifacts/dataset"))
    assert path.read_bytes() == DATASET


def test_declared_processors_missing_at_source_fail_before_package_persistence(tmp_path):
    class Missing(Source):
        def fitted_preprocessor_bytes(self, manifest, *, model_weight_sha256):
            raise DataUnavailableError("original fitted dataset is missing")
    with pytest.raises(DataUnavailableError):
        freeze(tmp_path, source=Missing())


def test_processor_identity_is_in_manifest_and_admission_closure(tmp_path):
    _, result = freeze(tmp_path)
    asset = result.manifest.model_asset.preprocessor_asset
    changed_model = result.manifest.model_asset.model_copy(
        update={"preprocessor_asset": asset.model_copy(update={"sha256": "0" * 64})}
    )
    changed = freeze_manifest(result.manifest.model_copy(update={"model_asset": changed_model}))
    assert changed.manifest_sha256 != result.manifest.manifest_sha256
    assert runtime_asset_closure_sha256(changed) != runtime_asset_closure_sha256(result.manifest)
    resolver = QEExperimentRuntimeAssetResolver(asset_store=LocalPackageAssetStore(tmp_path / "assets"), cache_root=tmp_path / "cache")
    with pytest.raises(PackageAssetInvalidError):
        resolver.load_frozen_source_for_strategy_package(manifest=changed, package_id=changed.package_id)


def test_legacy_manifest_hash_remains_unchanged_without_processor_asset():
    manifest = make_manifest()
    old = manifest.model_dump(mode="json")
    old["model_asset"].pop("preprocessor_asset", None)
    assert freeze_manifest(manifest).manifest_sha256 == compute_manifest_json_sha256(old)


class OrderedProcessor:
    def __init__(self, names):
        self.cols = [("feature", name) for name in names]

    def __call__(self, frame):
        return frame


@pytest.mark.parametrize("names,expected", [
    (["a", "b"], ["a", "b"]),
    (["b", "a"], ["b", "a"]),
    (["a", "missing"], None),
])
def test_fitted_feature_order_is_authoritative_and_gaps_fail_closed(tmp_path, names, expected):
    from backend import inference_engine
    path = tmp_path / "dataset"
    path.write_bytes(pickle.dumps(SimpleNamespace(handler=SimpleNamespace(infer_processors=[OrderedProcessor(names)]))))
    frame = pd.DataFrame({"b": [20.0], "a": [10.0]})
    args = {"task_dir": tmp_path, "primary_assets": {"dataset_processor_relpath": "dataset"}}
    if expected is None:
        with pytest.raises(ValueError, match="fitted feature schema"):
            inference_engine._apply_saved_qe_infer_processors(frame, **args)
    else:
        actual = inference_engine._apply_saved_qe_infer_processors(frame, **args)
        pd.testing.assert_frame_equal(actual, frame[expected])


@pytest.mark.parametrize("length", [1, 20, 0])
def test_saved_sequence_length_is_not_inferred_from_model_name(tmp_path, length):
    from backend import inference_engine
    (tmp_path / "dataset").write_bytes(pickle.dumps(SimpleNamespace(step_len=length)))
    args = (tmp_path, {"dataset_processor_relpath": "dataset"})
    if length == 0:
        with pytest.raises(ValueError, match="invalid sequence length"):
            inference_engine._saved_qe_step_len(*args)
    else:
        assert inference_engine._saved_qe_step_len(*args) == length


@pytest.mark.parametrize("empty", [False, True])
def test_fresh_probe_validates_fitted_schema_without_fitting(tmp_path, monkeypatch, empty):
    from backend import inference_engine
    from backend.services.strategy_package.frozen_runtime_self_check import frozen_model_probe_payload
    model_path = tmp_path / "params.pkl"
    model_path.write_bytes(b"weights")
    processors = [] if empty else [OrderedProcessor(["a", "b"])]
    (tmp_path / "dataset").write_bytes(pickle.dumps(SimpleNamespace(step_len=20, handler=SimpleNamespace(infer_processors=processors))))
    monkeypatch.setattr(inference_engine, "load_model_from_pkl", lambda path: (None, "pytorch", None, 2))
    if empty:
        with pytest.raises(ValueError, match="no inference processors"):
            frozen_model_probe_payload(model_path)
    else:
        payload = frozen_model_probe_payload(model_path)
        assert payload["fitted_feature_order"] == ["a", "b"]
        assert payload["sequence_length"] == 20


def test_admission_rejects_fitted_schema_mismatch_with_same_feature_count():
    from backend.services.strategy_package.frozen_runtime_self_check import FrozenRuntimeModelProbeResult, _validate_fitted_feature_schema
    from backend.services.trading_core.errors import StrategyPackageValidationError
    probe = FrozenRuntimeModelProbeResult(model_kind="pytorch", expected_features=2, backend="test", metadata={"probe_payload": {"fitted_feature_order": ["a", "b"]}})
    _validate_fitted_feature_schema(probe, ["b", "a"])
    with pytest.raises(StrategyPackageValidationError):
        _validate_fitted_feature_schema(probe, ["a", "wrong"])


def test_cpu_loader_preserves_tensor_values_without_cuda(tmp_path, monkeypatch):
    torch = pytest.importorskip("torch")
    from backend import inference_engine
    path = tmp_path / "weights"
    expected = torch.tensor([[1.25, -3.5]])
    path.write_bytes(pickle.dumps(expected))
    loader = torch.load
    calls = []
    def cpu_load(*args, **kwargs):
        calls.append(kwargs["map_location"])
        return loader(*args, **kwargs)
    monkeypatch.setattr(torch, "load", cpu_load)
    actual = inference_engine._load_admitted_strategy_package_pickle(path)
    assert calls == ["cpu"]
    assert actual.device.type == "cpu"
    assert torch.equal(actual, expected)


@pytest.mark.parametrize("future", [False, True])
def test_native_qlib_sequence_uses_all_past_steps_and_never_future(future):
    pytest.importorskip("qlib")
    import numpy as np
    from backend import inference_engine
    dates = pd.date_range("2026-08-01", periods=21)
    frame = pd.DataFrame({"a": np.arange(21, dtype=float)}, index=pd.MultiIndex.from_product([dates, ["000001.SZ"]], names=["datetime", "instrument"]))
    target = dates[19]
    if not future:
        frame = frame.iloc[:20]
    scored, batch = inference_engine._qe_sequence_inputs(frame, trade_date=target, step_len=20)
    assert scored.index.tolist() == [(target, "000001.SZ")]
    assert scored.columns.tolist() == ["a"]
    assert batch.shape == (1, 20, 1)
    np.testing.assert_array_equal(batch[0, :, 0], np.arange(20))
    with pytest.raises(ValueError, match="shorter"):
        inference_engine._qe_sequence_inputs(frame.iloc[18:20], trade_date=target, step_len=20)


def _archive(rows):
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as archive:
        for name, data in rows:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return out.getvalue()


@pytest.mark.parametrize("case,expected", [
    ("paired", None), ("wrong_weight", "strategy_package_preprocessor_weight_mismatch"),
    ("missing", "strategy_package_preprocessor_missing"),
    ("conflict", "strategy_package_preprocessor_ambiguous"),
])
def test_recorder_pairing_never_uses_another_models_dataset(case, expected):
    rows = [("mlruns/e/a/artifacts/params.pkl", b"weights")]
    if case != "missing":
        rows.append(("mlruns/e/a/artifacts/dataset", DATASET))
    rows.extend([("mlruns/e/b/artifacts/params.pkl", b"unrelated"), ("mlruns/e/b/artifacts/dataset", b"wrong")])
    if case == "conflict":
        rows.extend([("mlruns/e/c/artifacts/params.pkl", b"weights"), ("mlruns/e/c/artifacts/dataset", b"conflicting")])
    args = {"locator": QERuntimeAssetLocator(node_id="test"),
            "model_weight_sha256": hashlib.sha256(b"unknown" if case == "wrong_weight" else b"weights").hexdigest()}
    if expected is None:
        assert _fitted_dataset_from_mlruns_archive(_archive(rows), **args) == DATASET
    else:
        with pytest.raises((DataUnavailableError, ArtifactGenerationFailedError)) as caught:
            _fitted_dataset_from_mlruns_archive(_archive(rows), **args)
        assert caught.value.context["reason_code"] == expected
