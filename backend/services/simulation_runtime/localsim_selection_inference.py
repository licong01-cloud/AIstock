"""LocalSIM transport and immutable-asset addressing for the shared node code.

There is no scoring, feature, sequence, fusion or selection algorithm here.
Native package preparation owns those contracts; the node runs the existing CLI.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from typing import Any
from uuid import uuid4

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.canonical_equity_pit import PitAuthorityStatus
from backend.services.canonical_pit_inference_boundary import InferencePitMode, manifest_inference_pit_identity
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from backend.services.selection_center.canonical_pit_runtime import CANONICAL_PIT_RUNTIME_LEASE_KEY, SelectionPitRuntimeLease
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    LiveInferenceResult,
    PreparedInferenceWorkspace,
    QEExperimentRuntimeAssetResolver,
    QEExperimentRuntimeSource,
    WslStrategyPackageInferenceProvider,
    _manifest_runtime_custom_params,
    _manifest_runtime_data_split,
    _manifest_runtime_experiment_id,
    _runtime_factor_name,
    _single_model_asset_for_runtime,
    _write_inside_runtime_source,
)
from backend.services.trading_core.errors import DataUnavailableError
from scripts.strategy_package_live_inference import _atomic_json


@dataclass
class LocalSimInferenceScope:
    run_id: str
    root: Path
    clocks: SelectionSignalDayV1
    artifacts: dict[str, Any]
    result: Any = None
    signal_result: Any = None
    clocks_bound: bool = False
    source_universes: list[dict[str, Any]] = field(default_factory=list)
    completed_prefix: list[dict[str, Any]] = field(default_factory=list)
    stage: str = "native_policy_preparation"


LOCALSIM_INFERENCE_SCOPE: ContextVar[LocalSimInferenceScope | None] = ContextVar("localsim_inference_scope", default=None)


def own_scope() -> LocalSimInferenceScope:
    scope = LOCALSIM_INFERENCE_SCOPE.get()
    if scope is None:
        raise ValueError("LocalSIM inference requires its own explicit run scope")
    return scope


def windows_x_root(value: Path | str) -> Path:
    root = Path(value).resolve()
    if root.drive.upper() != "X:" or not root.is_relative_to(Path("X:/AIstock_temp").resolve()):
        raise ValueError("LocalSIM temporary storage must resolve beneath X:/AIstock_temp")
    root.mkdir(parents=True, exist_ok=True)
    return root


class LocalSimReferencedAssetResolver(QEExperimentRuntimeAssetResolver):
    """Address native frozen assets in place; materialize only small code/config.

    Windows cross-volume hard links and privileged symlinks are unavailable.
    This consumer's thin workspace address adapter delegates schema/order/code
    construction to the original resolver, and references weight/processor blobs
    through absolute node paths. Native multi-alpha still dispatches every seed.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._models: dict[str, Any] = {}
        self._package_context: dict[str, Any] = {}

    def _source_from_package_assets(self, manifest, *, package_id, model_asset_override=None,
                                    factor_set_override=None, runtime_assets_override=None, cache_namespace=None):
        scope = own_scope()
        scope.stage = "native_asset_addressing"
        model = model_asset_override or _single_model_asset_for_runtime(manifest)
        root = scope.root / "workspaces" / uuid4().hex
        root.mkdir(parents=True)
        factors = list(manifest.factor_set if factor_set_override is None else factor_set_override)
        factor_names = []
        for factor in factors:
            name = _runtime_factor_name(factor, package_id=package_id)
            factor_names.append(name)
            payload = self._read_package_asset_bytes(asset_ref=factor.asset_ref, expected_sha256=factor.sha256,
                                                    package_id=package_id, asset_kind="factor_code", logical_name=name)
            target = root / "factors" / (name + ".py")
            _write_inside_runtime_source(target, payload, source_dir=root, package_id=package_id)
        self._materialize_alpha158_conf(manifest, source_dir=root, package_id=package_id,
                                        runtime_assets_override=runtime_assets_override)
        self._materialize_model_code_assets(model, model_dir=root / "model", source_dir=root, package_id=package_id)
        self._models[str(root)] = model
        evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
        backtest = manifest.backtest_context if isinstance(manifest.backtest_context, dict) else {}
        # An inference declaration is distinct from a training compatibility
        # binding. Keep native stock-pool metadata without promoting the latter
        # into a frozen inference identity or constructing a member list.
        self._package_context[str(root)] = {
            "pit_identity": manifest_inference_pit_identity(evidence),
            "universe_policy": manifest.universe_policy.model_dump(mode="json") if manifest.universe_policy else None,
            "canonical_pit_binding": manifest.canonical_pit_binding.model_dump(mode="json") if manifest.canonical_pit_binding else None,
        }
        return QEExperimentRuntimeSource(
            experiment_id=_manifest_runtime_experiment_id(manifest), db_workspace_path=Path(), asset_workspace_path=root,
            factor_names=factor_names, custom_params=_manifest_runtime_custom_params(manifest, runtime_assets_override=runtime_assets_override),
            data_split=_manifest_runtime_data_split(evidence, backtest), model_params_origin="package_asset",
            source_workspace_type="strategy_package_asset_store", package_id=package_id, manifest_sha256=manifest.manifest_sha256,
        )

    def load_source_for_strategy_package(self, *, manifest, package_id, **_kwargs):
        return self.load_frozen_source_for_strategy_package(manifest=manifest, package_id=package_id)

    def _blob(self, descriptor) -> Path:
        self.asset_store.verify(descriptor.asset_ref, sha256=descriptor.sha256, size_bytes=descriptor.size_bytes)
        # Existing LocalPackageAssetStore's immutable documented layout.
        root = getattr(self.asset_store, "root", None)
        if root is None:
            raise DataUnavailableError("LocalSIM asset store has no read-only local addressing contract",
                                       context={"reason_code": "LOCALSIM_ASSET_REFERENCE_UNAVAILABLE"})
        path = (Path(root) / "blobs" / descriptor.sha256[:2] / descriptor.sha256).resolve(strict=True)
        if not path.is_relative_to(Path(root).resolve()):
            raise ValueError("immutable package reference escaped its asset root")
        return path

    def prepare_workspace(self, *, package_id, manifest_sha256, source, runtime_config=None,
                          path_converter=None, cache_namespace=None, verify_model_code_contract=True):
        if path_converter is None:
            raise ValueError("LocalSIM shared inference requires its WSL node")
        root = source.asset_workspace_path
        model = self._models.pop(str(root))
        context = self._package_context.pop(str(root))
        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config") or config.get("selection_artifact") or {}
        if not isinstance(artifact_config, dict):
            raise ValueError("native LocalSIM selection artifact config must be an object")
        pit_identity = artifact_config.get("pit_identity", context["pit_identity"])
        lease_value = config.get(CANONICAL_PIT_RUNTIME_LEASE_KEY)
        lease = SelectionPitRuntimeLease.from_mapping(lease_value) if lease_value else None
        if pit_identity is None and lease is not None and lease.authority_status is PitAuthorityStatus.ACTIVE_CANONICAL:
            pit_identity = {"mode": InferencePitMode.ROLLING_RUNTIME.value, **lease.as_dict()}
        order = self._build_factor_order(source=source, conf_path=self._resolve_conf_path(source))
        factor_files = self._resolve_factor_files(root / "factors", order.dynamic_factors)
        weight = self._blob(model)
        processor = self._blob(model.preprocessor_asset) if model.preprocessor_asset else None
        factor_order_path, entry_path, manifest_path = root / "factor_order.json", root / "strategy_package_factor_entry.py", root / "manifest.json"
        _atomic_json(factor_order_path, {**vars(order), "package_id": package_id, "source_experiment_id": source.experiment_id,
                                       "total_factors": len(order.factor_order), "alpha158_count": len(order.alpha158_factors),
                                       "dynamic_count": len(order.dynamic_factors), "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                                       "is_aligned": True})
        entry_path.write_text(self._build_factor_entry_source(factor_files=factor_files, path_converter=path_converter), encoding="utf-8")
        _atomic_json(manifest_path, {
            "schema_version": 1, "task_id": package_id, "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
            "primary_assets": {"model_weight_relpath": path_converter(str(weight)),
                               "factor_entry_relpath": entry_path.name,
                               "dataset_processor_relpath": path_converter(str(processor)) if processor else None},
            "assets": {"model_weight": path_converter(str(weight)), "factor_entry": entry_path.name,
                       "factor_order": factor_order_path.name, "factors_count": len(order.factor_order),
                       "dataset_processor": path_converter(str(processor)) if processor else None},
            "diagnostics": {"package_id": package_id, "package_manifest_sha256": manifest_sha256,
                            "source_workspace_type": source.source_workspace_type, "model_params_origin": "package_asset",
                            "model_source_path": str(weight), "asset_addressing": "READ_ONLY_REFERENCE"},
            "native_package_identity": context,
            "canonical_pit_runtime_lease": lease.as_dict() if lease else None,
            **({"pit_identity": pit_identity} if pit_identity is not None else {}),
        })
        return PreparedInferenceWorkspace(
            workspace_path=root, manifest_path=manifest_path, factor_order_path=factor_order_path, factor_entry_path=entry_path,
            model_params_path=weight, source_workspace_path=root, factor_source_dir=root / "factors",
            factor_order=order.factor_order, alpha158_factors=order.alpha158_factors, dynamic_factors=order.dynamic_factors,
            model_source_path=weight, model_candidate_count=1, dataset_processor_path=processor, model_params_origin="package_asset",
        )


class LocalSimSharedInferenceProvider(WslStrategyPackageInferenceProvider):
    """One explicit day per native workspace; no Advisory container or cache."""

    def __init__(self, *, performance_reference: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.performance_reference = performance_reference

    def _build_env_exports(self, *, historical_read_only: bool = False) -> str:
        # The production factory supplies no optional safe_artifact_roots.
        # Bind the validated owned run, without mutating this shared provider
        # or process-wide environment while other days may run concurrently.
        root = windows_x_root(own_scope().root)
        roots = tuple(dict.fromkeys((root, *self.safe_artifact_roots)))
        translated = ":".join(win_to_wsl_path(str(item)) for item in roots)
        native = super()._build_env_exports(historical_read_only=historical_read_only)
        return f"{native} AISTOCK_STRATEGY_PACKAGE_RUNTIME_ROOTS={self._quote(translated)}"

    def run(self, *, workspace, trade_date: date, cutoff_date: date | None = None, historical_read_only=False):
        scope = own_scope()
        if not Path(workspace.workspace_path).resolve().is_relative_to(scope.root.resolve()):
            raise ValueError("LocalSIM workspace must belong to its own run")
        scope.stage = "shared_raw_inference"
        clocks = SelectionSignalDayV1(trade_date, cutoff_date or trade_date, scope.clocks.target_trade_date)
        if clocks.decision_date != scope.clocks.decision_date or (scope.clocks_bound and clocks != scope.clocks):
            raise ValueError("native LocalSIM inference requested different C/D/T")
        # The native selection service resolves PREVIOUS_TRADING_DAY_CLOSE
        # after entering this scope; do not reconstruct that calendar policy.
        scope.clocks, scope.clocks_bound = clocks, True
        root = scope.root / "node" / uuid4().hex
        root.mkdir(parents=True)
        plan, output = root / "plan.json", root / "batch.json"
        _atomic_json(plan, [clocks.to_dict()])
        # Actual bidirectional path verification, rather than assuming /mnt/x.
        witness = root / "path-witness.txt"
        witness.write_text(scope.run_id, encoding="utf-8")
        probe = subprocess.run(["wsl", "-d", self.distro, "--", "cat", win_to_wsl_path(str(witness))],
                               capture_output=True, text=True, timeout=20, check=False,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        if probe.returncode or probe.stdout != scope.run_id:
            raise DataUnavailableError("WSL cannot read this LocalSIM X directory",
                                       context={"reason_code": "LOCALSIM_WSL_X_PATH_UNAVAILABLE"})
        arguments = ["scripts/strategy_package_live_inference.py", "--runtime-workspace", win_to_wsl_path(str(workspace.workspace_path)),
                     "--historical-plan", win_to_wsl_path(str(plan)), "--historical-read-only", "--consumer-scope", "LOCALSIM",
                     "--run-id", scope.run_id, "--temp-root", win_to_wsl_path(str(root)), "--output-path", win_to_wsl_path(str(output))]
        if self.performance_reference:
            arguments.append("--performance-reference")
        command = (f"source {self.conda_sh} && conda activate {self.conda_env} && cd {self._quote(win_to_wsl_path(str(self.repo_root)))} && "
                   + self._build_env_exports(historical_read_only=True)
                   + " PGOPTIONS='-c default_transaction_read_only=on' PYTHONDONTWRITEBYTECODE=1 python "
                   + " ".join(self._quote(arg) for arg in arguments))
        started = time.perf_counter()
        try:
            completed = subprocess.run(["wsl", "-d", self.distro, "bash", "-lc", command], capture_output=True,
                                       text=True, encoding="utf-8", errors="replace", check=False, timeout=self.timeout_seconds,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except subprocess.TimeoutExpired:
            # TimeoutExpired includes the credential-bearing command in its
            # default text. Never allow that object into native runtime logs.
            raise DataUnavailableError("LocalSIM owned node exceeded its inference timeout", context={
                "reason_code": "LOCALSIM_NODE_TIMEOUT", "run_id": scope.run_id,
                "timeout_seconds": self.timeout_seconds}) from None
        log_text = completed.stdout + "\n" + completed.stderr
        for key, value in os.environ.items():
            if value and any(marker in key.upper() for marker in ("PASSWORD", "TOKEN", "SECRET", "API_KEY")):
                log_text = log_text.replace(value, "[REDACTED]")
        (root / "execution.log").write_text(log_text, encoding="utf-8")
        # Never include subprocess args or raw stdout/stderr (may contain credentials).
        if not output.is_file():
            raise DataUnavailableError("LocalSIM node did not publish its own index", context={
                "reason_code": "LOCALSIM_NODE_INDEX_MISSING", "returncode": completed.returncode, "run_id": scope.run_id})
        index = json.loads(output.read_text(encoding="utf-8"))
        if (index.get("consumer_scope") != "LOCALSIM" or index.get("run_id") != scope.run_id
                or index.get("status") != "COMPLETE" or index.get("ok") is not True or completed.returncode != 0
                or len(index.get("results", [])) != 1):
            raise DataUnavailableError("LocalSIM node computation failed", context={
                "reason_code": index.get("reason_code", "LOCALSIM_NODE_FAILED"), "failed_stage": index.get("failed_stage"),
                "failed_clocks": index.get("failed_clocks"), "completed_prefix": index.get("results", []), "run_id": scope.run_id})
        path = root / "batch_dates" / trade_date.isoformat() / "scores.json"
        item = index["results"][0]
        if item.get("output_path") != win_to_wsl_path(str(path)) or item.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest():
            raise ValueError("LocalSIM node output path/content identity mismatch")
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata", {})
        if (payload.get("ok") is not True or metadata.get("consumer_scope") != "LOCALSIM"
                or metadata.get("run_id") != scope.run_id or metadata.get("clocks") != clocks.to_dict()
                or metadata.get("binding") != index.get("binding")):
            raise ValueError("LocalSIM node consumer/run/clock binding mismatch")
        native_manifest = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
        if native_manifest.get("canonical_pit_runtime_lease") is not None:
            expected_pit = native_manifest["canonical_pit_runtime_lease"]
            actual_pit = metadata.get("pit_identity", {})
            if any(actual_pit.get(key) != value for key, value in expected_pit.items() if key != "schema_version"):
                raise ValueError("LocalSIM node population differs from the native daily PIT lease")
        metadata.update(inference_backend="wsl", inference_wall_seconds=time.perf_counter() - started,
                        inference_batch_index=str(output), wsl_distro=self.distro, wsl_conda_env=self.conda_env)
        scope.source_universes.append({"members": metadata.get("universe_members"), "count": payload["universe_count"],
                                       "input_hash": payload["input_context"].get("universe_input_hash")})
        scope.completed_prefix.append({"stage": "shared_raw_inference", "clocks": clocks.to_dict(), "output_sha256": item["sha256"]})
        return LiveInferenceResult(scores=payload["scores"], metadata=metadata, universe_count=payload["universe_count"],
                                   source_read_receipts=payload["source_read_receipts"], input_context=payload["input_context"])
