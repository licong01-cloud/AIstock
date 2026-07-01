from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from backend.services.strategy_package.live_inference import (
    LiveInferencePreflightError,
    LiveInferenceResult,
    QEExperimentRuntimeAssetResolver,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import FactorAsset, ModelAsset, ModelCodeAsset
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    StrategyPackageSelectionArtifactService,
)
from backend.services.trading_core.errors import DataUnavailableError, PackageAssetInvalidError, RuntimeConfigInvalidError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class _ForbiddenConn:
    def __enter__(self) -> "_ForbiddenConn":
        raise AssertionError("frozen package runtime must not query qe_experiments")

    def __exit__(self, *_args: Any) -> None:
        return None


def _put(store: LocalPackageAssetStore, payload: bytes, *, kind: str) -> tuple[str, str]:
    blob = store.put(payload, kind=kind)
    return blob.uri, blob.sha256


def _frozen_manifest(
    store: LocalPackageAssetStore,
    *,
    model_payload: bytes = b"model params",
    model_code_files: dict[str, bytes] | None = None,
):
    factor_a_ref, factor_a_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_a': [1.0]})\n",
        kind="factor_code",
    )
    factor_b_ref, factor_b_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_b': [2.0]})\n",
        kind="factor_code",
    )
    model_ref, model_sha = _put(store, model_payload, kind="model_weight")
    model_code_assets: list[ModelCodeAsset] = []
    for rel_path, payload in sorted((model_code_files or {}).items()):
        code_ref, code_sha = _put(store, payload, kind="model_code")
        module_name = rel_path.removesuffix(".py").replace("/", ".").replace("\\", ".")
        model_code_assets.append(
            ModelCodeAsset(
                module_name=module_name,
                relative_path=rel_path,
                asset_ref=code_ref,
                sha256=code_sha,
                size_bytes=len(payload),
            )
        )
    base = make_manifest().model_copy(
        update={
            "manifest_version": "alpha_core_v1",
            "strategy_config": {},
            "universe_policy": None,
            "portfolio_policy": None,
            "execution_policy": None,
            "minute_execution_policy": None,
            "source_evidence": {
                "schema_version": "strategy_package_source_evidence_v1",
                "experiment_id": "qe_pkg_asset_exp",
                "qe_task_id": "qe_pkg_task",
                "qe_loop_id": "Loop7",
                "custom_params": {"topk": 50},
                "data_split": {"test_start": "2024-01-01"},
                "authority": "audit_only_not_runtime_authority",
            },
            "backtest_context": {
                "schema_version": "qe_backtest_context_v1",
                "authority": "source_evidence_not_runtime_authority",
                "daily_strategy": {"topk": 50, "custom_params": {"topk": 50}},
                "data_split": {"backtest_end": "2024-02-01"},
            },
            "factor_set": [
                FactorAsset(
                    factor_id="factor_a",
                    factor_name="factor_a",
                    asset_ref=factor_a_ref,
                    sha256=factor_a_sha,
                    size_bytes=len(store.get(factor_a_ref)),
                ),
                FactorAsset(
                    factor_id="factor_b",
                    factor_name="factor_b",
                    asset_ref=factor_b_ref,
                    sha256=factor_b_sha,
                    size_bytes=len(store.get(factor_b_ref)),
                ),
            ],
            "model_asset": ModelAsset(
                model_id="model_1",
                asset_ref=model_ref,
                sha256=model_sha,
                size_bytes=len(model_payload),
                model_code_required=bool(model_code_assets),
                model_code_assets=model_code_assets,
            ),
            "manifest_sha256": None,
        }
    )
    return freeze_manifest(base)


def test_frozen_package_runtime_materializes_assets_without_qe_db(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store, model_payload=b"params-from-package")
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="would_query_if_legacy",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    prepared = resolver.prepare_workspace(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        source=source,
    )

    assert source.source_workspace_type == "strategy_package_asset_store"
    assert source.model_params_origin == "package_asset"
    assert source.execution_node_id is None
    assert (source.asset_workspace_path / "factors" / "factor_a.py").read_bytes().startswith(b"import pandas")
    assert (source.asset_workspace_path / "mlruns" / "package_asset" / "artifacts" / "params.pkl").read_bytes() == b"params-from-package"
    assert prepared.model_params_path.read_bytes() == b"params-from-package"
    assert prepared.model_params_origin == "package_asset"
    assert prepared.dynamic_factors == ["factor_a", "factor_b"]
    assert prepared.alpha158_factors == []
    factor_order = json.loads(prepared.factor_order_path.read_text(encoding="utf-8"))
    assert factor_order["dynamic_factor_source"] == "strategy_package_manifest.factor_set"
    diagnostics = json.loads(prepared.manifest_path.read_text(encoding="utf-8"))["diagnostics"]
    assert diagnostics["source_workspace_type"] == "strategy_package_asset_store"
    assert diagnostics["package_id"] == manifest.package_id
    assert diagnostics["model_params_origin"] == "package_asset"


def test_frozen_package_runtime_rejects_asset_sha_mismatch(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store)
    bad_factor = manifest.factor_set[0].model_copy(update={"sha256": "0" * 64})
    bad_manifest = freeze_manifest(
        manifest.model_copy(update={"factor_set": [bad_factor, *manifest.factor_set[1:]], "manifest_sha256": None})
    )
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    with pytest.raises(PackageAssetInvalidError) as excinfo:
        resolver.load_source_for_strategy_package(
            source_type="qe_experiment",
            source_id="qe_pkg_asset_exp",
            manifest=bad_manifest,
            package_id=bad_manifest.package_id,
        )

    assert excinfo.value.context["reason_code"] == "strategy_package_asset_sha_mismatch"
    assert excinfo.value.context["asset_kind"] == "factor_code"
    assert excinfo.value.context["package_id"] == bad_manifest.package_id


def test_frozen_package_runtime_rejects_missing_asset_blob(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store)
    missing_ref = "aistock-package-asset://blobs/" + hashlib.sha256(b"missing").hexdigest()
    bad_model = manifest.model_asset.model_copy(update={"asset_ref": missing_ref})
    bad_manifest = freeze_manifest(manifest.model_copy(update={"model_asset": bad_model, "manifest_sha256": None}))
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    with pytest.raises(PackageAssetInvalidError) as excinfo:
        resolver.load_source_for_strategy_package(
            source_type="qe_experiment",
            source_id="qe_pkg_asset_exp",
            manifest=bad_manifest,
            package_id=bad_manifest.package_id,
        )

    assert excinfo.value.context["reason_code"] == "strategy_package_asset_blob_missing"
    assert excinfo.value.context["asset_kind"] == "model_weight"


def test_frozen_package_runtime_rejects_explicit_model_override(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store)
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )
    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="qe_pkg_asset_exp",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    override_path = tmp_path / "external_params.pkl"
    override_path.write_bytes(b"external override must not be runtime authority")

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        resolver.prepare_workspace(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            source=source,
            runtime_config={"selection_artifact_config": {"model_params_path": str(override_path)}},
        )

    assert excinfo.value.context["reason_code"] == "strategy_package_runtime_model_override_forbidden"
    assert excinfo.value.context["package_id"] == manifest.package_id


def test_unfrozen_package_keeps_legacy_qe_source_resolution(monkeypatch, tmp_path: Path) -> None:
    manifest = freeze_manifest(make_manifest())
    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_cache")
    calls: list[dict[str, Any]] = []

    def fake_load(experiment_id: str):
        calls.append({"experiment_id": experiment_id})
        return {"legacy_source": experiment_id}

    monkeypatch.setattr(resolver, "load_source", fake_load)

    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="qe_legacy_exp",
        manifest=manifest,
        package_id=manifest.package_id,
    )

    assert source == {"legacy_source": "qe_legacy_exp"}
    assert calls == [{"experiment_id": "qe_legacy_exp"}]


def test_preflight_for_frozen_package_marks_qe_node_not_required(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store)
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    result = resolver.preflight_for_strategy_package(
        source_type="qe_experiment",
        source_id="would_query_if_legacy",
        runtime_config={},
        manifest=manifest,
        package_id=manifest.package_id,
    )

    assert result.passed is True
    assert [check.name for check in result.checks] == ["qe_source", "qe_node", "conf_yaml", "factor_source", "model_params"]
    assert (result.checks[0].context or {})["source_workspace_type"] == "strategy_package_asset_store"
    assert result.checks[1].message == "package-owned runtime assets do not require QE node access"


def test_preflight_rejects_package_asset_params_with_missing_pickled_model_code(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(
        store,
        model_payload=b"cmodel\nLSTM_10D_hs64_d02\n.",
    )
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    result = resolver.preflight_for_strategy_package(
        source_type="qe_experiment",
        source_id="would_query_if_legacy",
        runtime_config={},
        manifest=manifest,
        package_id=manifest.package_id,
    )

    assert result.passed is False
    assert result.blocked_check is not None
    assert result.blocked_check.name == "model_params"
    context = result.blocked_check.context or {}
    assert context["reason_code"] == "strategy_package_model_code_missing"
    assert context["missing_modules"] == ["model"]
    assert context["missing_relative_paths"] == ["model.py"]

    with pytest.raises(LiveInferencePreflightError) as excinfo:
        resolver.require_preflight_or_raise(
            source_type="qe_experiment",
            source_id="would_query_if_legacy",
            runtime_config={},
            manifest=manifest,
            package_id=manifest.package_id,
        )
    assert excinfo.value.context["blocked_check"] == "model_params"


def test_prepare_workspace_rejects_package_asset_params_with_missing_pickled_model_code(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(
        store,
        model_payload=b"cmodel\nLSTM_10D_hs64_d02\n.",
    )
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )
    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="would_query_if_legacy",
        manifest=manifest,
        package_id=manifest.package_id,
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        resolver.prepare_workspace(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            source=source,
        )

    assert excinfo.value.context["reason_code"] == "strategy_package_model_code_missing"
    assert excinfo.value.context["missing_modules"] == ["model"]


def test_package_asset_params_with_model_code_materialize_successfully(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    model_py = b"class LSTM_10D_hs64_d02:\n    pass\nmodel_cls = LSTM_10D_hs64_d02\n"
    manifest = _frozen_manifest(
        store,
        model_payload=b"cmodel\nLSTM_10D_hs64_d02\n.",
        model_code_files={"model.py": model_py},
    )
    resolver = QEExperimentRuntimeAssetResolver(
        conn_factory=lambda: _ForbiddenConn(),
        cache_root=tmp_path / "runtime_cache",
        asset_store=store,
    )

    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="would_query_if_legacy",
        manifest=manifest,
        package_id=manifest.package_id,
    )
    prepared = resolver.prepare_workspace(
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        source=source,
    )

    assert (prepared.model_params_path.parent / "model.py").read_bytes() == model_py
    diagnostics = json.loads(prepared.manifest_path.read_text(encoding="utf-8"))["diagnostics"]
    assert diagnostics["referenced_model_modules"] == ["model"]


def test_selection_artifact_service_passes_frozen_manifest_to_source_loader(tmp_path: Path) -> None:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _frozen_manifest(store)
    package_repo = InMemoryStrategyPackageRepository()
    package_repo.save_manifest(manifest)

    class ResolverSpy:
        def __init__(self) -> None:
            self.load_calls: list[dict[str, Any]] = []
            self.prepare_calls: list[dict[str, Any]] = []

        def load_source_for_strategy_package(
            self,
            *,
            source_type: str,
            source_id: str,
            loop_id: str | None = None,
            run_id: str | None = None,
            manifest: Any,
            package_id: str,
        ) -> dict[str, Any]:
            self.load_calls.append(
                {
                    "source_type": source_type,
                    "source_id": source_id,
                    "loop_id": loop_id,
                    "run_id": run_id,
                    "manifest": manifest,
                    "package_id": package_id,
                }
            )
            return {"source": "package_asset"}

        def prepare_workspace(self, **kwargs: Any) -> Any:
            self.prepare_calls.append(kwargs)

            class Prepared:
                workspace_path = tmp_path / "prepared"
                factor_order_path = tmp_path / "factor_order.json"
                factor_entry_path = tmp_path / "factor_entry.py"
                model_params_path = tmp_path / "params.pkl"
                model_source_path = tmp_path / "source_params.pkl"
                factor_source_dir = tmp_path / "factors"
                factor_order = ["factor_a", "factor_b"]
                alpha158_factors: list[str] = []
                dynamic_factors = ["factor_a", "factor_b"]
                model_candidate_count = 1

            return Prepared()

    class Provider:
        backend_name = "fake_live"

        def run(self, **_kwargs: Any) -> LiveInferenceResult:
            return LiveInferenceResult(scores=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1}], metadata={})

    resolver = ResolverSpy()
    service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
        runtime_asset_resolver=resolver,
        live_inference_provider=Provider(),
    )

    artifact = service.generate_from_live_inference(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        include_reference_price=False,
    )

    assert artifact.status.value == "SUCCEEDED"
    assert len(resolver.load_calls) == 1
    assert resolver.load_calls[0]["manifest"].manifest_sha256 == manifest.manifest_sha256
    assert resolver.load_calls[0]["package_id"] == manifest.package_id
    assert resolver.prepare_calls[0]["source"] == {"source": "package_asset"}

