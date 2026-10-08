from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from backend.services.strategy_package.manifest import compute_manifest_sha256, freeze_manifest
from backend.services.strategy_package.frozen_runtime_self_check import (
    FrozenRuntimeSelfCheckResult,
    runtime_asset_admission_status,
)
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.package_asset_freeze import (
    PackageAssetBytes,
    PackageAssetFreezeService,
)
from backend.services.strategy_package.package_asset_store import (
    LocalPackageAssetStore,
    ObjectPackageAssetStore,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, PackageAssetInvalidError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class NoopFrozenRuntimeSelfCheck:
    def assert_manifest_self_contained(self, manifest):  # noqa: ANN001, ANN201
        return FrozenRuntimeSelfCheckResult(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            origin="package_asset",
            model_kind="unit",
            model_expected_features=len(manifest.factor_set),
            dynamic_factor_count=len(manifest.factor_set),
            alpha158_alias_count=0,
            factor_order_count=len(manifest.factor_set),
            feature_count_delta=0,
            model_params_path="unit://params.pkl",
            model_probe_backend="unit",
        )


class FakeResolver:
    def __init__(self, manifest=None) -> None:  # noqa: ANN001
        self.manifest = manifest or make_manifest()

    def build_from_experiment(self, experiment_id: str, *, resolve_runtime_assets: bool = False):  # noqa: ANN201
        assert resolve_runtime_assets is False
        return freeze_manifest(
            self.manifest.model_copy(
                update={
                    "source": self.manifest.source.model_copy(update={"source_id": experiment_id, "run_id": "qear_run_unit"}),
                    "manifest_sha256": None,
                }
            )
        )

    def build_from_evolution_loop(
        self,
        *,
        qe_task_id: str,
        qe_loop_id: str,
        resolve_runtime_assets: bool = False,
    ):  # noqa: ANN201
        assert resolve_runtime_assets is False
        return freeze_manifest(
            self.manifest.model_copy(
                update={
                    "source": self.manifest.source.model_copy(
                        update={"source_id": qe_task_id, "loop_id": qe_loop_id, "run_id": "qear_run_loop"}
                    ),
                    "manifest_sha256": None,
                }
            )
        )


def _freezer(tmp_path: Path) -> PackageAssetFreezeService:
    return PackageAssetFreezeService(
        asset_store=LocalPackageAssetStore(tmp_path / "package_assets"),
        conf_yaml_reader=lambda manifest: PackageAssetBytes(b"task: {}\n", f"unit://conf/{manifest.package_id}/conf.yaml"),
        model_params_reader=lambda manifest: PackageAssetBytes(
            b"model-params",
            f"unit://model/{manifest.package_id}/params.pkl",
        ),
        factor_code_reader=lambda factor, manifest: PackageAssetBytes(
            f"# factor {factor.factor_name}\nVALUE = 1\n".encode("utf-8"),
            f"unit://factor/{factor.factor_name}.py",
        ),
    )


def test_local_package_asset_store_put_get_exists_and_sha_mismatch(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "package_assets")
    payload = b"asset bytes"
    digest = hashlib.sha256(payload).hexdigest()

    blob = store.put(payload, kind="factor_code", sha256=digest)

    assert blob.sha256 == digest
    assert blob.uri.startswith("aistock-package-asset://blobs/")
    assert store.exists(blob.uri)
    assert store.get(blob.uri) == payload
    with pytest.raises(PackageAssetInvalidError) as excinfo:
        store.put(payload, kind="factor_code", sha256="0" * 64)
    assert excinfo.value.context["reason_code"] == "strategy_package_asset_sha_mismatch"


def test_object_package_asset_store_is_explicitly_not_implemented() -> None:
    store = ObjectPackageAssetStore()

    with pytest.raises(NotImplementedError):
        store.put(b"x", kind="factor_code")
    with pytest.raises(NotImplementedError):
        store.get("aistock-package-asset://blobs/" + "0" * 64)
    with pytest.raises(NotImplementedError):
        store.exists("aistock-package-asset://blobs/" + "0" * 64)


@pytest.mark.parametrize("previous_copy", [False, True])
def test_model_weight_freeze_and_runtime_share_one_physical_blob(tmp_path: Path, previous_copy: bool) -> None:
    import os

    source = tmp_path / "central_model_blob"
    source.write_bytes(b"immutable-trained-model")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    store = LocalPackageAssetStore(tmp_path / "package_assets")
    if previous_copy:
        store.put(source.read_bytes(), kind="model_weight")
    blob = store.put_file(source, kind="model_weight", sha256=digest)
    first = tmp_path / "runtime_one" / "params.pkl"
    second = tmp_path / "runtime_two" / "params.pkl"
    store.materialize_file(blob.uri, first, sha256=digest, size_bytes=source.stat().st_size)
    store.materialize_file(blob.uri, second, sha256=digest, size_bytes=source.stat().st_size)
    assert os.path.samefile(source, first)
    assert os.path.samefile(first, second)
    source.unlink()
    first.unlink()
    assert store.get(blob.uri) == second.read_bytes() == b"immutable-trained-model"
    with pytest.raises(FileExistsError):
        store.materialize_file(blob.uri, second, sha256=digest, size_bytes=second.stat().st_size)


@pytest.mark.parametrize("attribute,width,shape", [
    ("in_channels", 44, (64, 44, 3)),
    ("input_size", 46, (256, 46)),
    ("in_features", 44, (64, 44)),
])
def test_pytorch_input_width_uses_input_layer_not_kernel(attribute, width, shape):
    from types import SimpleNamespace

    from backend.inference_engine import _pytorch_input_feature_count

    layer = SimpleNamespace(**{attribute: width})
    network = SimpleNamespace(
        named_modules=lambda: iter([("", layer)]),
        parameters=lambda: iter([SimpleNamespace(shape=shape)]),
    )
    assert _pytorch_input_feature_count(network) == width


def test_pytorch_unknown_kernel_tensor_is_not_a_feature_width():
    from types import SimpleNamespace

    from backend.inference_engine import _pytorch_input_feature_count

    network = SimpleNamespace(parameters=lambda: iter([SimpleNamespace(shape=(64, 44, 3))]))
    with pytest.raises(ValueError, match="input feature"):
        _pytorch_input_feature_count(network)


def test_empty_asset_defaults_do_not_change_legacy_manifest_hash() -> None:
    legacy = freeze_manifest(make_manifest())
    round_tripped = freeze_manifest(legacy.model_copy(update={"manifest_sha256": None}))

    assert compute_manifest_sha256(legacy) == legacy.manifest_sha256
    assert round_tripped.manifest_sha256 == legacy.manifest_sha256


def test_create_from_qe_experiment_freezes_runtime_assets_and_is_idempotent(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    service = StrategyPackageService(
        repository=repo,
        resolver=FakeResolver(),
        asset_freezer=_freezer(tmp_path),
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
    )

    first = service.create_from_qe_experiment("qe_asset_freeze_unit", resolve_runtime_assets=True)
    second = service.create_from_qe_experiment("qe_asset_freeze_unit", resolve_runtime_assets=True)

    assert first.package_id == second.package_id
    assert first.manifest_sha256 == second.manifest_sha256
    assets = service.list_package_assets(first.package_id)
    assert len(assets) == 3
    assert sorted(asset.asset_type for asset in assets) == [
        StrategyPackageAssetType.FACTOR_CODE,
        StrategyPackageAssetType.FACTOR_CODE,
        StrategyPackageAssetType.MODEL_WEIGHT,
    ]
    manifest = first.current_manifest()
    assert runtime_asset_admission_status(manifest)[0] is True
    assert service.asset_eligibility.summarize(first).eligible is True
    assert all(factor.asset_ref and factor.sha256 for factor in manifest.factor_set)
    assert manifest.runtime_assets is not None
    assert manifest.runtime_assets.alpha158.enabled is False
    model_asset = manifest.model_asset
    assert not isinstance(model_asset, list)
    assert model_asset.asset_ref and model_asset.sha256
    assert all("pred.pkl" not in asset.asset_ref for asset in assets)
    assert all("combined_prediction.pkl" not in asset.asset_ref for asset in assets)


def test_create_from_qe_experiment_is_idempotent_when_resolver_generates_fresh_package_ids(tmp_path: Path) -> None:
    class FreshPackageResolver:
        def build_from_experiment(self, experiment_id: str, *, resolve_runtime_assets: bool = False):  # noqa: ANN201
            assert resolve_runtime_assets is False
            manifest = make_manifest().model_copy(
                update={
                    "source": make_manifest().source.model_copy(update={"source_id": experiment_id, "run_id": "qear_run_unit"}),
                    "manifest_sha256": None,
                }
            )
            return freeze_manifest(manifest)

    repo = InMemoryStrategyPackageRepository()
    service = StrategyPackageService(
        repository=repo,
        resolver=FreshPackageResolver(),
        asset_freezer=_freezer(tmp_path),
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
    )

    first = service.create_from_qe_experiment("qe_asset_freeze_fresh_pkg_id")
    second = service.create_from_qe_experiment("qe_asset_freeze_fresh_pkg_id")

    assert first.package_id == second.package_id
    assert len(repo.records) == 1
    assert len(service.list_package_assets(first.package_id)) == 3


def test_freeze_missing_factor_fails_before_package_save(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()

    def factor_reader(factor, manifest):  # noqa: ANN001, ANN202
        if factor.factor_name == "factor_b":
            raise DataUnavailableError(
                "factor missing",
                context={"reason_code": "strategy_package_factor_code_missing", "factor_name": factor.factor_name},
            )
        return PackageAssetBytes(b"factor-a", "unit://factor/factor_a.py")

    freezer = PackageAssetFreezeService(
        asset_store=LocalPackageAssetStore(tmp_path / "package_assets"),
        conf_yaml_reader=lambda manifest: PackageAssetBytes(b"task: {}\n", f"unit://conf/{manifest.package_id}/conf.yaml"),
        model_params_reader=lambda manifest: PackageAssetBytes(b"model", "unit://model/params.pkl"),
        factor_code_reader=factor_reader,
    )
    service = StrategyPackageService(
        repository=repo,
        resolver=FakeResolver(),
        asset_freezer=freezer,
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        service.create_from_qe_experiment("qe_missing_factor")

    assert excinfo.value.context["reason_code"] == "strategy_package_factor_code_missing"
    assert repo.records == {}
    assert repo.package_assets == {}


def test_freeze_missing_model_fails_before_package_save(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    freezer = PackageAssetFreezeService(
        asset_store=LocalPackageAssetStore(tmp_path / "package_assets"),
        conf_yaml_reader=lambda manifest: PackageAssetBytes(b"task: {}\n", f"unit://conf/{manifest.package_id}/conf.yaml"),
        model_params_reader=lambda manifest: (_ for _ in ()).throw(
            DataUnavailableError(
                "model missing",
                context={"reason_code": "strategy_package_model_params_missing", "package_id": manifest.package_id},
            )
        ),
        factor_code_reader=lambda factor, manifest: PackageAssetBytes(b"factor", f"unit://factor/{factor.factor_name}.py"),
    )
    service = StrategyPackageService(
        repository=repo,
        resolver=FakeResolver(),
        asset_freezer=freezer,
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        service.create_from_qe_experiment("qe_missing_model")

    assert excinfo.value.context["reason_code"] == "strategy_package_model_params_missing"
    assert repo.records == {}
    assert repo.package_assets == {}
