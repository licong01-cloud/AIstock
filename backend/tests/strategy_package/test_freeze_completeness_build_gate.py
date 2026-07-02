from __future__ import annotations

import importlib
import pickle
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.strategy_package.frozen_runtime_self_check import FrozenRuntimeSelfCheckService
from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import ModelAsset
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.package_asset_freeze import PackageAssetBytes, PackageAssetFreezeService
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore, PackageAssetStore
from backend.services.strategy_package.runtime_schema import extract_alpha158_aliases, load_conf_yaml_file
from backend.services.trading_core.errors import PackageAssetInvalidError, StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


ALPHA158_CONF = b"""
task:
  dataset:
    kwargs:
      handler:
        kwargs:
          data_loader:
            class: qlib.contrib.data.loader.Alpha158DL
            kwargs:
              config:
                feature:
                  - ["Ref($close, 5) / $close", "Mean($close, 5) / $close"]
                  - ["RESI5", "WVMA5"]
"""

CUSTOM_MODEL_CONF = b"""
task:
  model:
    kwargs:
      pt_model_uri: model.model_cls
"""


class ShaOverrideStore(PackageAssetStore):
    def __init__(self, delegate: LocalPackageAssetStore, overrides: dict[str, bytes]) -> None:
        self.delegate = delegate
        self.overrides = overrides

    def put(self, data: bytes, *, kind: str, sha256: str | None = None):  # noqa: ANN201
        return self.delegate.put(data, kind=kind, sha256=sha256)

    def get(self, uri: str) -> bytes:
        return self.overrides.get(uri, self.delegate.get(uri))

    def exists(self, uri: str) -> bool:
        return uri in self.overrides or self.delegate.exists(uri)


def _manifest(label: str = "complete"):
    base = make_manifest()
    return base.model_copy(update={"package_id": f"pkg_{label}", "manifest_sha256": None})


def _pickled_model_instance_payload(tmp_path: Path) -> bytes:
    module_root = tmp_path / "pickle_model_module"
    module_root.mkdir()
    (module_root / "model.py").write_text("class LSTM_10D_hs64_d02:\n    pass\n", encoding="utf-8")
    sys.path.insert(0, str(module_root))
    try:
        sys.modules.pop("model", None)
        module = importlib.import_module("model")
        return pickle.dumps(module.LSTM_10D_hs64_d02(), protocol=4)
    finally:
        sys.modules.pop("model", None)
        sys.path.remove(str(module_root))


def _freezer(
    tmp_path: Path,
    *,
    conf_bytes: bytes = b"task: {}\n",
    model_params: bytes = b"model-params",
    model_code_files: dict[str, bytes] | None = None,
) -> PackageAssetFreezeService:
    model_code_files = model_code_files or {}

    def workspace_file(manifest, rel_path: str):  # noqa: ANN001, ANN202
        rel = rel_path.replace("\\", "/")
        if rel not in model_code_files:
            from backend.services.trading_core.errors import DataUnavailableError

            raise DataUnavailableError(
                "missing workspace file",
                context={"reason_code": "unit_workspace_file_missing", "rel_path": rel, "package_id": manifest.package_id},
            )
        return PackageAssetBytes(model_code_files[rel], f"unit://workspace/{rel}")

    source = SimpleNamespace(workspace_file_bytes=workspace_file)
    return PackageAssetFreezeService(
        asset_store=LocalPackageAssetStore(tmp_path / "package_assets"),
        source=source,
        conf_yaml_reader=lambda manifest: PackageAssetBytes(conf_bytes, f"unit://conf/{manifest.package_id}/conf.yaml"),
        model_params_reader=lambda manifest: PackageAssetBytes(model_params, f"unit://model/{manifest.package_id}/params.pkl"),
        factor_code_reader=lambda factor, manifest: PackageAssetBytes(
            f"# factor {factor.factor_name}\nVALUE = 1\n".encode("utf-8"),
            f"unit://factor/{factor.factor_name}.py",
        ),
    )


def _records_from_manifest(manifest) -> list[StrategyPackageAssetRecord]:  # noqa: ANN001
    rows: list[StrategyPackageAssetRecord] = []
    for factor in manifest.factor_set:
        rows.append(
            StrategyPackageAssetRecord(
                package_id=manifest.package_id,
                asset_type=StrategyPackageAssetType.FACTOR_CODE,
                asset_ref=factor.asset_ref,
                asset_sha256=factor.sha256,
                asset_size_bytes=factor.size_bytes,
                source_uri=factor.source_uri,
            )
        )
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    for model in model_assets:
        rows.append(
            StrategyPackageAssetRecord(
                package_id=manifest.package_id,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                asset_ref=model.asset_ref,
                asset_sha256=model.sha256,
                asset_size_bytes=model.size_bytes,
                source_uri=model.source_uri,
            )
        )
        for code_asset in model.model_code_assets:
            rows.append(
                StrategyPackageAssetRecord(
                    package_id=manifest.package_id,
                    asset_type=StrategyPackageAssetType.MODEL_CODE,
                    asset_ref=code_asset.asset_ref,
                    asset_sha256=code_asset.sha256,
                    asset_size_bytes=code_asset.size_bytes,
                    source_uri=code_asset.source_uri,
                )
            )
    runtime_assets = manifest.runtime_assets
    if runtime_assets is not None and runtime_assets.alpha158.enabled:
        alpha158 = runtime_assets.alpha158
        rows.append(
            StrategyPackageAssetRecord(
                package_id=manifest.package_id,
                asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
                asset_ref=alpha158.asset_ref,
                asset_sha256=alpha158.sha256,
                asset_size_bytes=alpha158.size_bytes,
                source_uri=alpha158.source_uri,
            )
        )
    return rows


def test_alpha158_schema_freezes_full_node_and_runtime_conf_is_readable(tmp_path: Path) -> None:
    freezer = _freezer(tmp_path, conf_bytes=ALPHA158_CONF)

    frozen = freezer.freeze_manifest_assets(_manifest("alpha158"))

    manifest = frozen.manifest
    assert manifest.runtime_assets is not None
    alpha158 = manifest.runtime_assets.alpha158
    assert alpha158.enabled is True
    assert alpha158.aliases == ["RESI5", "WVMA5"]
    assert any(row.asset_type == StrategyPackageAssetType.FACTOR_SCHEMA for row in frozen.assets)

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime", asset_store=freezer.asset_store)
    source = resolver.load_source_for_strategy_package(
        source_type=manifest.source.source_type.value,
        source_id="__missing_source__",
        loop_id="__missing_loop__",
        run_id="__missing_run__",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    conf_path = source.asset_workspace_path / "conf.yaml"
    aliases = extract_alpha158_aliases(load_conf_yaml_file(conf_path, purpose="unit alpha158 runtime"))

    assert source.model_params_origin == "package_asset"
    assert source.source_workspace_type == "strategy_package_asset_store"
    assert aliases == ["RESI5", "WVMA5"]


def test_alpha158_schema_missing_and_sha_mismatch_fail_closed(tmp_path: Path) -> None:
    freezer = _freezer(tmp_path, conf_bytes=ALPHA158_CONF)
    manifest = freezer.freeze_manifest_assets(_manifest("alpha158_fail")).manifest
    runtime_assets = manifest.runtime_assets
    assert runtime_assets is not None
    bad_manifest = freeze_manifest(
        manifest.model_copy(
            update={
                "runtime_assets": runtime_assets.model_copy(
                    update={"alpha158": runtime_assets.alpha158.model_copy(update={"asset_ref": None})}
                ),
                "manifest_sha256": None,
            }
        )
    )
    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_missing", asset_store=freezer.asset_store)

    with pytest.raises(PackageAssetInvalidError) as missing:
        resolver.load_source_for_strategy_package(
            source_type=bad_manifest.source.source_type.value,
            source_id="missing",
            loop_id="missing",
            run_id="missing",
            manifest=bad_manifest,
            package_id=bad_manifest.package_id,
        )

    assert missing.value.context["reason_code"] == "strategy_package_alpha158_schema_missing"

    alpha = manifest.runtime_assets.alpha158
    corrupt_store = ShaOverrideStore(freezer.asset_store, {alpha.asset_ref: b"{}"})
    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_sha", asset_store=corrupt_store)
    with pytest.raises(PackageAssetInvalidError) as mismatch:
        resolver.load_source_for_strategy_package(
            source_type=manifest.source.source_type.value,
            source_id="missing",
            loop_id="missing",
            run_id="missing",
            manifest=manifest,
            package_id=manifest.package_id,
        )

    assert mismatch.value.context["reason_code"] == "strategy_package_alpha158_schema_sha_mismatch"


def test_model_code_freeze_and_runtime_materializes_next_to_params(tmp_path: Path) -> None:
    model_py = b"from helper import scale\nclass LSTM_10D_hs64_d02:\n    pass\nmodel_cls = LSTM_10D_hs64_d02\n"
    helper_py = b"def scale(value):\n    return value\n"
    freezer = _freezer(
        tmp_path,
        conf_bytes=CUSTOM_MODEL_CONF,
        model_code_files={"model.py": model_py, "helper.py": helper_py},
    )

    frozen = freezer.freeze_manifest_assets(_manifest("model_code"))
    manifest = frozen.manifest
    model = manifest.model_asset
    assert isinstance(model, ModelAsset)
    assert model.model_code_required is True
    assert sorted(asset.relative_path for asset in model.model_code_assets) == ["helper.py", "model.py"]
    assert sum(row.asset_type == StrategyPackageAssetType.MODEL_CODE for row in frozen.assets) == 2

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime", asset_store=freezer.asset_store)
    source = resolver.load_source_for_strategy_package(
        source_type=manifest.source.source_type.value,
        source_id="missing",
        loop_id="missing",
        run_id="missing",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    prepared = resolver.prepare_workspace(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        source=source,
    )

    assert (prepared.model_params_path.parent / "model.py").read_bytes() == model_py
    assert (prepared.model_params_path.parent / "helper.py").read_bytes() == helper_py


def test_pickled_local_model_freezes_code_without_pt_model_uri(tmp_path: Path) -> None:
    model_py = b"from helper import scale\nclass LSTM_10D_hs64_d02:\n    pass\n"
    helper_py = b"def scale(value):\n    return value\n"
    freezer = _freezer(
        tmp_path,
        conf_bytes=b"task: {}\n",
        model_params=_pickled_model_instance_payload(tmp_path),
        model_code_files={"model.py": model_py, "helper.py": helper_py},
    )

    frozen = freezer.freeze_manifest_assets(_manifest("pickle_model_code"))
    manifest = frozen.manifest
    model = manifest.model_asset
    assert isinstance(model, ModelAsset)
    assert model.model_code_required is True
    assert sorted(asset.relative_path for asset in model.model_code_assets) == ["helper.py", "model.py"]
    assert sum(row.asset_type == StrategyPackageAssetType.MODEL_CODE for row in frozen.assets) == 2

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_pickle", asset_store=freezer.asset_store)
    source = resolver.load_source_for_strategy_package(
        source_type=manifest.source.source_type.value,
        source_id="missing",
        loop_id="missing",
        run_id="missing",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    prepared = resolver.prepare_workspace(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        source=source,
    )

    assert (prepared.model_params_path.parent / "model.py").read_bytes() == model_py
    assert (prepared.model_params_path.parent / "helper.py").read_bytes() == helper_py
    sys.path.insert(0, str(prepared.model_params_path.parent))
    try:
        sys.modules.pop("model", None)
        with prepared.model_params_path.open("rb") as handle:
            loaded = pickle.load(handle)  # noqa: S301 - unit fixture asserts self-contained trusted pickle payload.
    finally:
        sys.modules.pop("model", None)
        sys.path.remove(str(prepared.model_params_path.parent))
    assert type(loaded).__name__ == "LSTM_10D_hs64_d02"


def test_pickled_local_model_missing_source_code_fails_closed(tmp_path: Path) -> None:
    freezer = _freezer(
        tmp_path,
        conf_bytes=b"task: {}\n",
        model_params=_pickled_model_instance_payload(tmp_path),
        model_code_files={},
    )

    with pytest.raises(Exception) as excinfo:
        freezer.freeze_manifest_assets(_manifest("pickle_model_code_missing"))

    context = getattr(excinfo.value, "context", {})
    assert context["reason_code"] == "strategy_package_model_code_missing"
    assert context["relative_path"] == "model.py"
    assert context["module_name"] == "model"


def test_custom_model_missing_code_fails_closed(tmp_path: Path) -> None:
    freezer = _freezer(tmp_path, conf_bytes=CUSTOM_MODEL_CONF, model_code_files={})

    with pytest.raises(Exception) as excinfo:
        freezer.freeze_manifest_assets(_manifest("model_code_missing"))

    context = getattr(excinfo.value, "context", {})
    assert context["reason_code"] == "strategy_package_model_code_missing"
    assert context["relative_path"] == "model.py"
    assert context["module_name"] == "model"


def test_custom_model_missing_import_helper_fails_closed(tmp_path: Path) -> None:
    model_py = b"from helper import scale\nclass LSTM_10D_hs64_d02:\n    pass\nmodel_cls = LSTM_10D_hs64_d02\n"
    freezer = _freezer(
        tmp_path,
        conf_bytes=CUSTOM_MODEL_CONF,
        model_code_files={"model.py": model_py},
    )

    with pytest.raises(Exception) as excinfo:
        freezer.freeze_manifest_assets(_manifest("model_code_helper_missing"))

    context = getattr(excinfo.value, "context", {})
    assert context["reason_code"] == "strategy_package_model_code_missing"
    assert context["relative_path"] == "helper.py"
    assert context["module_name"] == "helper"


def test_self_check_reports_feature_decomposition_on_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    freezer = _freezer(tmp_path, conf_bytes=ALPHA158_CONF, model_params=b"pickle-placeholder")
    frozen = freezer.freeze_manifest_assets(_manifest("self_check"))

    def fake_load_model_from_pkl(_path):  # noqa: ANN001, ANN202
        return object(), "lgb", object(), 5

    _ = monkeypatch
    checker = FrozenRuntimeSelfCheckService(
        runtime_asset_resolver=QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime", asset_store=freezer.asset_store),
        model_loader=fake_load_model_from_pkl,
    )

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        checker.assert_manifest_self_contained(frozen.manifest)

    context = excinfo.value.context
    assert context["reason_code"] == "strategy_package_frozen_self_check_feature_count_mismatch"
    assert context["dynamic_factor_count"] == 2
    assert context["alpha158_alias_count"] == 2
    assert context["factor_order_count"] == 4
    assert context["model_expected_features"] == 5
    assert context["feature_count_delta"] == 1
    assert context["model_probe_backend"] == "injected"


def test_self_check_passes_when_model_expected_matches_alpha158_and_dynamic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    freezer = _freezer(tmp_path, conf_bytes=ALPHA158_CONF, model_params=b"pickle-placeholder")
    frozen = freezer.freeze_manifest_assets(_manifest("self_check_pass"))

    def fake_load_model_from_pkl(_path):  # noqa: ANN001, ANN202
        return object(), "lgb", object(), 4

    _ = monkeypatch
    checker = FrozenRuntimeSelfCheckService(
        runtime_asset_resolver=QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime", asset_store=freezer.asset_store),
        model_loader=fake_load_model_from_pkl,
    )

    result = checker.assert_manifest_self_contained(frozen.manifest)

    assert result.origin == "package_asset"
    assert result.dynamic_factor_count == 2
    assert result.alpha158_alias_count == 2
    assert result.factor_order_count == 4
    assert result.feature_count_delta == 0
    assert result.model_probe_backend == "injected"
