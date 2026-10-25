from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.inference_window import FrozenSequenceLengthReader
from backend.services.strategy_package.models import FittedPreprocessorAsset, ModelAsset
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import DataUnavailableError, PackageAssetInvalidError, RuntimeConfigInvalidError
from backend.tests.strategy_package.test_multi_alpha_live_selection import _make_parent


@pytest.fixture
def frozen_reader(tmp_path: Path, monkeypatch):  # noqa: ANN001, ANN201
    # The subprocess is mocked in these pure contracts; logical node paths
    # stay on X while CI owns its actual pytest temporary directory.
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.win_to_wsl_path",
                        lambda path: "/mnt/x/test-window/" + Path(path).name)
    store = LocalPackageAssetStore(root=tmp_path / "protected")
    weight = store.put(b"frozen weights", kind="model_weight")
    dataset = store.put(b"fitted dataset", kind="preprocessor")
    model = ModelAsset(model_id="any-new-model", asset_ref=weight.uri, sha256=weight.sha256, size_bytes=weight.size_bytes,
                       preprocessor_asset=FittedPreprocessorAsset(asset_ref=dataset.uri, sha256=dataset.sha256, size_bytes=dataset.size_bytes))
    reader = FrozenSequenceLengthReader(asset_store=store, repository_root=Path(__file__).parents[3],
                                       temporary_root=tmp_path / "metadata")
    return reader, model


def test_native_fitted_length_is_read_without_copying_or_guessing_model_family(frozen_reader, monkeypatch) -> None:  # noqa: ANN001
    reader, model = frozen_reader
    calls = []
    def run(args, **kwargs):  # noqa: ANN001, ANN003, ANN202
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout='ADVISORY_FROZEN_SEQUENCE_METADATA={"model_kind":"pytorch","sequence_length":20}\n')
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.subprocess.run", run)

    assert reader._read(model) == 20
    args, kwargs = calls[0]
    assert args[:2] == ["wsl", "-d"] and kwargs["timeout"] == 120
    assert "os.symlink(source, target)" in args[-1]
    assert "_saved_qe_step_len" in args[-1] and "CUDA_VISIBLE_DEVICES=" in args[-1]
    assert "/mnt/x/" in args[-1] and "C:/" not in args[-1]
    assert not list(reader.temporary_root.iterdir())


def test_protected_asset_drift_fails_before_metadata_dispatch(frozen_reader, monkeypatch) -> None:  # noqa: ANN001
    reader, model = frozen_reader
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.subprocess.run",
                        lambda *_args, **_kwargs: pytest.fail("drifted asset must never be dispatched"))
    with pytest.raises(PackageAssetInvalidError):
        reader._read(model.model_copy(update={"sha256": "0" * 64}))


def test_asset_drift_during_metadata_read_cannot_be_frozen(frozen_reader, monkeypatch) -> None:  # noqa: ANN001
    reader, model = frozen_reader
    def run(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        reader.asset_store._path_from_uri(model.asset_ref).write_bytes(b"changed while reading")
        return SimpleNamespace(returncode=0, stdout='ADVISORY_FROZEN_SEQUENCE_METADATA={"sequence_length":20}\n')
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.subprocess.run", run)
    with pytest.raises(PackageAssetInvalidError):
        reader._read(model)


@pytest.mark.parametrize("length", [True, 0, 1.5, None])
def test_invalid_native_metadata_cannot_fall_back_to_one_day(frozen_reader, monkeypatch, length) -> None:  # noqa: ANN001
    reader, model = frozen_reader
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.subprocess.run",
                        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="ADVISORY_FROZEN_SEQUENCE_METADATA=" + json.dumps({"sequence_length": length})))
    with pytest.raises(RuntimeConfigInvalidError):
        reader._read(model)


def test_failed_native_read_is_not_reported_as_a_one_day_model(frozen_reader, monkeypatch) -> None:  # noqa: ANN001
    reader, model = frozen_reader
    monkeypatch.setattr("backend.services.advisory_historical_range.inference_window.subprocess.run",
                        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout=""))
    with pytest.raises(DataUnavailableError):
        reader._read(model)


def test_multi_alpha_preserves_each_component_window_and_nonsequence_models(monkeypatch) -> None:  # noqa: ANN001
    _, parent = _make_parent(live_weight_policy=False)
    manifest = parent.current_manifest()
    reader = FrozenSequenceLengthReader(asset_store=LocalPackageAssetStore(), repository_root=Path(__file__).parents[3],
                                       temporary_root=Path("X:/AIstock_temp/advisory_inference_window"))
    calls = []
    def read(model):  # noqa: ANN001, ANN202
        calls.append(model.model_id)
        return len(calls) * 20
    monkeypatch.setattr(reader, "_read", read)
    lengths = reader(manifest)
    assert set(lengths) == {component.alpha_id for component in manifest.alpha_components}
    assert sorted(lengths.values()) == [20, 40] and len(calls) == 2
    assert FrozenSequenceLengthReader._read(reader, manifest.model_asset[0]) == 1
