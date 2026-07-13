"""Fail-closed self-check for frozen StrategyPackage runtime assets."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

from backend.services.trading_core.errors import (
    DataUnavailableError,
    PackageAssetInvalidError,
    StrategyPackageValidationError,
    TradingCoreError,
)

from .live_inference import QEExperimentRuntimeAssetResolver, win_to_wsl_path
from .manifest import freeze_manifest
from .models import AlphaMode, StrategyPackageManifest
from .package_asset_store import PackageAssetStore

ModelLoader = Callable[[Path], tuple[Any, str, Any, int]]

RUNTIME_ASSET_ADMISSION_KEY = "runtime_asset_admission"
RUNTIME_ASSET_ADMISSION_SCHEMA = "strategy_package_runtime_asset_admission_v1"


@dataclass(frozen=True)
class FrozenRuntimeSelfCheckResult:
    package_id: str
    manifest_sha256: str
    origin: str
    model_kind: str
    model_expected_features: int
    dynamic_factor_count: int
    alpha158_alias_count: int
    factor_order_count: int
    feature_count_delta: int
    model_params_path: str
    model_probe_backend: str
    leg_results: dict[str, dict[str, Any]] | None = None
    combined_signal_smoke: dict[str, Any] | None = None

    def to_context(self) -> dict[str, Any]:
        context = {
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "origin": self.origin,
            "model_kind": self.model_kind,
            "model_expected_features": self.model_expected_features,
            "dynamic_factor_count": self.dynamic_factor_count,
            "alpha158_alias_count": self.alpha158_alias_count,
            "factor_order_count": self.factor_order_count,
            "feature_count_delta": self.feature_count_delta,
            "model_params_path": self.model_params_path,
            "model_probe_backend": self.model_probe_backend,
        }
        if self.leg_results is not None:
            context["leg_results"] = self.leg_results
        if self.combined_signal_smoke is not None:
            context["combined_signal_smoke"] = self.combined_signal_smoke
        return context


def attach_runtime_asset_admission(
    manifest: StrategyPackageManifest,
    result: FrozenRuntimeSelfCheckResult,
) -> StrategyPackageManifest:
    """Persist the one-time frozen-asset self-check receipt in the manifest.

    The receipt is keyed by the immutable asset closure rather than by the final
    manifest hash because adding the receipt itself changes that hash.  Later
    Selection/LocalSim/MiniQMT paths may trust this receipt and must not rerun
    model-code discovery or frozen-runtime self-checks.
    """

    if result.package_id != manifest.package_id:
        raise StrategyPackageValidationError(
            "runtime asset admission result belongs to another StrategyPackage",
            context={
                "reason_code": "strategy_package_runtime_asset_admission_package_mismatch",
                "package_id": manifest.package_id,
                "self_check_package_id": result.package_id,
            },
        )
    if not _is_sha256(result.manifest_sha256):
        raise StrategyPackageValidationError(
            "runtime asset admission requires a frozen self-check manifest hash",
            context={
                "reason_code": "strategy_package_runtime_asset_admission_self_check_hash_invalid",
                "package_id": manifest.package_id,
                "self_check_manifest_sha256": result.manifest_sha256,
            },
        )
    if result.origin != "package_asset":
        raise StrategyPackageValidationError(
            "runtime asset admission self-check must use package-owned assets",
            context={
                "reason_code": "strategy_package_runtime_asset_admission_origin_invalid",
                "package_id": manifest.package_id,
                "origin": result.origin,
            },
        )

    source_evidence = json.loads(json.dumps(manifest.source_evidence or {}, ensure_ascii=False, default=str))
    source_evidence.pop(RUNTIME_ASSET_ADMISSION_KEY, None)
    receipt = {
        "schema_version": RUNTIME_ASSET_ADMISSION_SCHEMA,
        "passed": True,
        "persisted_for_simulation_admission": True,
        "package_id": manifest.package_id,
        "alpha_mode": manifest.alpha_mode.value,
        "self_check_manifest_sha256": result.manifest_sha256,
        "asset_closure_sha256": runtime_asset_closure_sha256(manifest),
        "model_code_contract": _model_code_contract(manifest),
        "self_check_summary": {
            "origin": result.origin,
            "model_kind": result.model_kind,
            "model_expected_features": result.model_expected_features,
            "dynamic_factor_count": result.dynamic_factor_count,
            "alpha158_alias_count": result.alpha158_alias_count,
            "factor_order_count": result.factor_order_count,
            "feature_count_delta": result.feature_count_delta,
            "model_probe_backend": result.model_probe_backend,
            "leg_count": len(result.leg_results or {}),
            "combined_signal_smoke_schema": (
                (result.combined_signal_smoke or {}).get("schema_version")
                if isinstance(result.combined_signal_smoke, dict)
                else None
            ),
        },
    }
    source_evidence[RUNTIME_ASSET_ADMISSION_KEY] = receipt
    return freeze_manifest(
        manifest.model_copy(
            update={
                "source_evidence": source_evidence,
                "manifest_sha256": None,
            }
        )
    )


def runtime_asset_admission_status(manifest: StrategyPackageManifest) -> tuple[bool, dict[str, Any]]:
    """Validate only persisted admission identity; never read or materialize assets."""

    receipt = (manifest.source_evidence or {}).get(RUNTIME_ASSET_ADMISSION_KEY)
    context: dict[str, Any] = {
        "package_id": manifest.package_id,
        "manifest_sha256": manifest.manifest_sha256,
        "schema_version": receipt.get("schema_version") if isinstance(receipt, dict) else None,
    }
    if not isinstance(receipt, dict):
        return False, {**context, "reason_code": "strategy_package_runtime_asset_admission_missing"}
    expected_closure = runtime_asset_closure_sha256(manifest)
    expected_model_contract = _model_code_contract(manifest)
    failures: list[str] = []
    if receipt.get("schema_version") != RUNTIME_ASSET_ADMISSION_SCHEMA:
        failures.append("schema_version")
    if receipt.get("passed") is not True or receipt.get("persisted_for_simulation_admission") is not True:
        failures.append("passed")
    if receipt.get("package_id") != manifest.package_id:
        failures.append("package_id")
    if receipt.get("alpha_mode") != manifest.alpha_mode.value:
        failures.append("alpha_mode")
    if not _is_sha256(receipt.get("self_check_manifest_sha256")):
        failures.append("self_check_manifest_sha256")
    if receipt.get("asset_closure_sha256") != expected_closure:
        failures.append("asset_closure_sha256")
    if receipt.get("model_code_contract") != expected_model_contract:
        failures.append("model_code_contract")
    if failures:
        return False, {
            **context,
            "reason_code": "strategy_package_runtime_asset_admission_invalid",
            "invalid_fields": failures,
            "expected_asset_closure_sha256": expected_closure,
            "actual_asset_closure_sha256": receipt.get("asset_closure_sha256"),
        }
    return True, {
        **context,
        "reason_code": "strategy_package_runtime_asset_admission_passed",
        "asset_closure_sha256": expected_closure,
        "self_check_manifest_sha256": receipt.get("self_check_manifest_sha256"),
    }


def require_runtime_asset_admission(manifest: StrategyPackageManifest) -> dict[str, Any]:
    passed, context = runtime_asset_admission_status(manifest)
    if not passed:
        raise PackageAssetInvalidError(
            "StrategyPackage has not completed one-time runtime asset admission",
            context=context,
        )
    return context


def runtime_asset_closure_sha256(manifest: StrategyPackageManifest) -> str:
    payload = {
        "package_id": manifest.package_id,
        "alpha_mode": manifest.alpha_mode.value,
        "factors": [
            {
                "factor_id": factor.factor_id,
                "required": factor.required,
                "asset_ref": factor.asset_ref,
                "sha256": factor.sha256,
            }
            for factor in manifest.factor_set
        ],
        "models": _model_code_contract(manifest),
        "runtime_assets": manifest.runtime_assets.model_dump(mode="json") if manifest.runtime_assets is not None else None,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _model_code_contract(manifest: StrategyPackageManifest) -> list[dict[str, Any]]:
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    return [
        {
            "model_id": model.model_id,
            "model_type": model.model_type,
            "asset_ref": model.asset_ref,
            "sha256": model.sha256,
            "model_code_required": model.model_code_required,
            "model_code_assets": [
                {
                    "module_name": asset.module_name,
                    "relative_path": asset.relative_path,
                    "required": asset.required,
                    "asset_ref": asset.asset_ref,
                    "sha256": asset.sha256,
                }
                for asset in sorted(model.model_code_assets, key=lambda item: (item.relative_path, item.module_name))
            ],
        }
        for model in sorted(model_assets, key=lambda item: item.model_id)
    ]


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


@dataclass(frozen=True)
class FrozenRuntimeModelProbeResult:
    model_kind: str
    expected_features: int
    backend: str
    metadata: dict[str, Any]


class WslFrozenRuntimeModelProbe:
    def __init__(
        self,
        *,
        distro: str | None = None,
        conda_sh: str | None = None,
        conda_env: str | None = None,
        repo_root: Path | str | None = None,
        timeout_seconds: int = 600,
    ) -> None:
        self.distro = distro or os.getenv("QLIB_WSL_DISTRO") or "Ubuntu"
        self.conda_sh = conda_sh or os.getenv("QLIB_WSL_CONDA_SH") or "~/miniconda3/etc/profile.d/conda.sh"
        self.conda_env = conda_env or os.getenv("QLIB_WSL_CONDA_ENV") or "rdagent-gpu"
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
        self.timeout_seconds = timeout_seconds

    def probe(self, model_params_path: Path) -> FrozenRuntimeModelProbeResult:
        with tempfile.TemporaryDirectory(prefix="sp_frozen_self_check_") as tmp:
            output_path = Path(tmp) / "model_probe.json"
            args = [
                "scripts/strategy_package_frozen_self_check.py",
                "--model-params-path",
                win_to_wsl_path(str(model_params_path)),
                "--output-path",
                win_to_wsl_path(str(output_path)),
            ]
            command = (
                f"source {self.conda_sh} && "
                f"conda activate {self.conda_env} && "
                f"cd {win_to_wsl_path(str(self.repo_root))} && "
                "PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 AISTOCK_STRICT_INFERENCE=1 "
                + "python "
                + " ".join(_shell_quote(arg) for arg in args)
            )
            completed = subprocess.run(
                ["wsl", "-d", self.distro, "bash", "-lc", command],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout_seconds,
                check=False,
            )
            if completed.returncode != 0:
                raise DataUnavailableError(
                    "WSL frozen StrategyPackage model probe failed",
                    context={
                        "reason_code": "strategy_package_frozen_self_check_model_probe_failed",
                        "model_params_path": str(model_params_path),
                        "returncode": completed.returncode,
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "runner_args": args,
                        "wsl_distro": self.distro,
                        "wsl_conda_env": self.conda_env,
                    },
                )
            if not output_path.exists():
                raise DataUnavailableError(
                    "WSL frozen StrategyPackage model probe did not write output JSON",
                    context={
                        "reason_code": "strategy_package_frozen_self_check_model_probe_failed",
                        "model_params_path": str(model_params_path),
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "output_path": str(output_path),
                        "runner_args": args,
                    },
                )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        return FrozenRuntimeModelProbeResult(
            model_kind=str(payload.get("model_kind") or "unknown"),
            expected_features=int(payload.get("model_expected_features") or 0),
            backend="wsl",
            metadata={
                "wsl_distro": self.distro,
                "wsl_conda_env": self.conda_env,
                "probe_payload": payload,
            },
        )


class FrozenRuntimeSelfCheckService:
    def __init__(
        self,
        *,
        runtime_asset_resolver: QEExperimentRuntimeAssetResolver | None = None,
        cache_root: Path | str | None = None,
        asset_store: PackageAssetStore | None = None,
        model_loader: ModelLoader | None = None,
        model_probe_backend: str | None = None,
        wsl_model_probe: WslFrozenRuntimeModelProbe | None = None,
    ) -> None:
        self.runtime_asset_resolver = runtime_asset_resolver or QEExperimentRuntimeAssetResolver(
            cache_root=cache_root,
            asset_store=asset_store,
        )
        self.model_loader = model_loader
        self.model_probe_backend = (model_probe_backend or os.getenv("STRATEGY_PACKAGE_FROZEN_SELF_CHECK_BACKEND") or "auto").strip().lower()
        self.wsl_model_probe = wsl_model_probe or WslFrozenRuntimeModelProbe()

    def assert_manifest_self_contained(self, manifest: StrategyPackageManifest) -> FrozenRuntimeSelfCheckResult:
        manifest_sha = str(manifest.manifest_sha256 or "").strip().lower()
        if not manifest_sha:
            raise StrategyPackageValidationError(
                "frozen runtime self-check requires manifest_sha256",
                context={"reason_code": "strategy_package_frozen_self_check_manifest_unfrozen", "package_id": manifest.package_id},
            )
        if manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            return self._assert_multi_alpha_manifest_self_contained(manifest, manifest_sha=manifest_sha)
        try:
            source = self.runtime_asset_resolver.load_source_for_strategy_package(
                source_type=manifest.source.source_type.value,
                source_id="__aistock_missing_qe_source_for_package_asset_self_check__",
                loop_id="__missing_loop__",
                run_id="__missing_run__",
                manifest=manifest,
                package_id=manifest.package_id,
            )
            if source.model_params_origin != "package_asset" or source.source_workspace_type != "strategy_package_asset_store":
                raise StrategyPackageValidationError(
                    "frozen runtime self-check did not use package-owned assets",
                    context={
                        "reason_code": "strategy_package_frozen_self_check_origin_not_package_asset",
                        "package_id": manifest.package_id,
                        "model_params_origin": source.model_params_origin,
                        "source_workspace_type": source.source_workspace_type,
                    },
                )
            prepared = self.runtime_asset_resolver.prepare_workspace(
                package_id=manifest.package_id,
                manifest_sha256=manifest_sha,
                source=source,
            )
            probe = self._probe_model(prepared.model_params_path)
            model_kind = probe.model_kind
            expected_features = probe.expected_features
        except TradingCoreError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "frozen StrategyPackage runtime self-check failed while loading model",
                context={
                    "reason_code": "strategy_package_frozen_self_check_model_load_failed",
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest_sha,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ) from exc

        dynamic_count = len(prepared.dynamic_factors)
        alpha_count = len(prepared.alpha158_factors)
        factor_order_count = len(prepared.factor_order)
        delta = int(expected_features or 0) - factor_order_count
        context = {
            "reason_code": "strategy_package_frozen_self_check_feature_count_mismatch",
            "package_id": manifest.package_id,
            "manifest_sha256": manifest_sha,
            "model_kind": model_kind,
            "model_expected_features": int(expected_features or 0),
            "dynamic_factor_count": dynamic_count,
            "alpha158_alias_count": alpha_count,
            "factor_order_count": factor_order_count,
            "feature_count_delta": delta,
            "model_params_path": str(prepared.model_params_path),
            "model_probe_backend": probe.backend,
        }
        if expected_features and int(expected_features) != factor_order_count:
            raise StrategyPackageValidationError(
                "frozen StrategyPackage runtime self-check feature count mismatch",
                context=context,
            )
        return FrozenRuntimeSelfCheckResult(
            package_id=manifest.package_id,
            manifest_sha256=manifest_sha,
            origin=source.model_params_origin,
            model_kind=model_kind,
            model_expected_features=int(expected_features or 0),
            dynamic_factor_count=dynamic_count,
            alpha158_alias_count=alpha_count,
            factor_order_count=factor_order_count,
            feature_count_delta=delta,
            model_params_path=str(prepared.model_params_path),
            model_probe_backend=probe.backend,
        )

    def _assert_multi_alpha_manifest_self_contained(
        self,
        manifest: StrategyPackageManifest,
        *,
        manifest_sha: str,
    ) -> FrozenRuntimeSelfCheckResult:
        try:
            from .multi_alpha_live import _multi_alpha_evidence, _parent_leg_runtime_slices

            leg_slices = _parent_leg_runtime_slices(
                manifest,
                evidence=_multi_alpha_evidence(manifest),
                package_id=manifest.package_id,
            )
            leg_results: dict[str, dict[str, Any]] = {}
            first_result: FrozenRuntimeSelfCheckResult | None = None
            total_dynamic = 0
            total_alpha = 0
            total_factor_order = 0
            for leg_slice in leg_slices:
                source = self.runtime_asset_resolver.load_source_for_strategy_package_leg(
                    manifest=manifest,
                    package_id=manifest.package_id,
                    leg_id=leg_slice.leg_id,
                    model_asset=leg_slice.model_asset,
                    factor_set=list(leg_slice.factor_set),
                    runtime_assets=leg_slice.runtime_assets,
                )
                if source.model_params_origin != "package_asset" or source.source_workspace_type != "strategy_package_asset_store":
                    raise StrategyPackageValidationError(
                        "multi-alpha frozen runtime self-check did not use parent package-owned assets",
                        context={
                            "reason_code": "multi_alpha_parent_leg_runtime_assets_incomplete",
                            "package_id": manifest.package_id,
                            "leg_id": leg_slice.leg_id,
                            "model_id": leg_slice.model_asset.model_id,
                            "model_params_origin": source.model_params_origin,
                            "source_workspace_type": source.source_workspace_type,
                        },
                    )
                prepared = self.runtime_asset_resolver.prepare_workspace(
                    package_id=manifest.package_id,
                    manifest_sha256=manifest_sha,
                    source=source,
                    cache_namespace=f"leg_{leg_slice.leg_id}",
                )
                probe = self._probe_model(prepared.model_params_path)
                expected_features = int(probe.expected_features or 0)
                factor_order_count = len(prepared.factor_order)
                delta = expected_features - factor_order_count
                context = {
                    "reason_code": "multi_alpha_parent_leg_feature_count_mismatch",
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest_sha,
                    "leg_id": leg_slice.leg_id,
                    "model_id": leg_slice.model_asset.model_id,
                    "model_kind": probe.model_kind,
                    "model_expected_features": expected_features,
                    "dynamic_factor_count": len(prepared.dynamic_factors),
                    "alpha158_alias_count": len(prepared.alpha158_factors),
                    "factor_order_count": factor_order_count,
                    "feature_count_delta": delta,
                    "model_params_path": str(prepared.model_params_path),
                    "model_probe_backend": probe.backend,
                }
                if expected_features and expected_features != factor_order_count:
                    raise StrategyPackageValidationError(
                        "multi-alpha frozen StrategyPackage runtime self-check feature count mismatch",
                        context=context,
                    )
                expected_refs = {
                    str(ref or "").strip()
                    for ref in (leg_slice.component.lineage.factor_artifact_refs or [])
                    if str(ref or "").strip()
                }
                prepared_refs = set(prepared.dynamic_factors)
                if expected_refs != prepared_refs:
                    raise StrategyPackageValidationError(
                        "multi-alpha frozen StrategyPackage runtime self-check factor refs mismatch",
                        context={
                            "reason_code": "multi_alpha_parent_leg_factor_refs_mismatch",
                            "package_id": manifest.package_id,
                            "manifest_sha256": manifest_sha,
                            "leg_id": leg_slice.leg_id,
                            "model_id": leg_slice.model_asset.model_id,
                            "expected_factor_refs": sorted(expected_refs),
                            "prepared_dynamic_factors": sorted(prepared_refs),
                        },
                    )
                result = FrozenRuntimeSelfCheckResult(
                    package_id=manifest.package_id,
                    manifest_sha256=manifest_sha,
                    origin=source.model_params_origin,
                    model_kind=probe.model_kind,
                    model_expected_features=expected_features,
                    dynamic_factor_count=len(prepared.dynamic_factors),
                    alpha158_alias_count=len(prepared.alpha158_factors),
                    factor_order_count=factor_order_count,
                    feature_count_delta=delta,
                    model_params_path=str(prepared.model_params_path),
                    model_probe_backend=probe.backend,
                )
                first_result = first_result or result
                leg_results[leg_slice.leg_id] = {**result.to_context(), "model_id": leg_slice.model_asset.model_id}
                total_dynamic += len(prepared.dynamic_factors)
                total_alpha += len(prepared.alpha158_factors)
                total_factor_order += factor_order_count
            if first_result is None:
                raise StrategyPackageValidationError(
                    "multi-alpha frozen runtime self-check requires at least one leg",
                    context={"reason_code": "multi_alpha_parent_leg_mapping_missing", "package_id": manifest.package_id},
                )
            combined_signal_smoke = _combined_signal_smoke(manifest, leg_results=leg_results)
            return FrozenRuntimeSelfCheckResult(
                package_id=manifest.package_id,
                manifest_sha256=manifest_sha,
                origin="package_asset",
                model_kind="multi_alpha_parent",
                model_expected_features=sum(item["model_expected_features"] for item in leg_results.values()),
                dynamic_factor_count=total_dynamic,
                alpha158_alias_count=total_alpha,
                factor_order_count=total_factor_order,
                feature_count_delta=sum(item["feature_count_delta"] for item in leg_results.values()),
                model_params_path=first_result.model_params_path,
                model_probe_backend=first_result.model_probe_backend,
                leg_results=leg_results,
                combined_signal_smoke=combined_signal_smoke,
            )
        except TradingCoreError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "multi-alpha frozen StrategyPackage runtime self-check failed while loading parent leg assets",
                context={
                    "reason_code": "multi_alpha_promotion_parent_self_check_failed",
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest_sha,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ) from exc

    def _probe_model(self, model_params_path: Path) -> FrozenRuntimeModelProbeResult:
        backend = self.model_probe_backend
        if self.model_loader is not None:
            _model, model_kind, _inner_model, expected_features = self.model_loader(model_params_path)
            return FrozenRuntimeModelProbeResult(
                model_kind=str(model_kind),
                expected_features=int(expected_features or 0),
                backend="injected",
                metadata={},
            )
        if backend not in {"auto", "local", "wsl"}:
            raise StrategyPackageValidationError(
                "unsupported frozen runtime self-check backend",
                context={
                    "reason_code": "strategy_package_frozen_self_check_backend_invalid",
                    "backend": backend,
                    "supported": ["auto", "local", "wsl"],
                },
            )
        if backend == "wsl" or (backend == "auto" and os.name == "nt"):
            return self.wsl_model_probe.probe(model_params_path)
        try:
            from backend import inference_engine

            _model, model_kind, _inner_model, expected_features = inference_engine.load_model_from_pkl(model_params_path)
            return FrozenRuntimeModelProbeResult(
                model_kind=str(model_kind),
                expected_features=int(expected_features or 0),
                backend="local",
                metadata={},
            )
        except ModuleNotFoundError:
            if backend == "auto":
                return self.wsl_model_probe.probe(model_params_path)
            raise


def _shell_quote(value: str) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _combined_signal_smoke(
    manifest: StrategyPackageManifest,
    *,
    leg_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Fail-closed structural combined-signal smoke without reading child/QE sources."""

    from .multi_alpha_live import _multi_alpha_evidence, _parent_leg_runtime_slices

    evidence = _multi_alpha_evidence(manifest)
    leg_slices = _parent_leg_runtime_slices(manifest, evidence=evidence, package_id=manifest.package_id)
    weights = dict(manifest.alpha_combination_policy.weights or {})
    smoke_date = _self_check_trade_date(manifest)
    if not smoke_date:
        raise StrategyPackageValidationError(
            "multi-alpha frozen StrategyPackage self-check requires a trade date for combined signal smoke",
            context={
                "reason_code": "multi_alpha_promotion_parent_combined_signal_failed",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
    rows: list[dict[str, Any]] = []
    combined = 0.0
    for index, leg_slice in enumerate(leg_slices, start=1):
        leg_id = leg_slice.leg_id
        weight = weights.get(leg_id)
        if weight is None:
            raise StrategyPackageValidationError(
                "multi-alpha frozen StrategyPackage self-check weights do not cover every leg",
                context={
                    "reason_code": "multi_alpha_promotion_parent_combined_signal_failed",
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest.manifest_sha256,
                    "leg_id": leg_id,
                },
            )
        if leg_id not in leg_results:
            raise StrategyPackageValidationError(
                "multi-alpha frozen StrategyPackage self-check leg result is missing",
                context={
                    "reason_code": "multi_alpha_promotion_parent_combined_signal_failed",
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest.manifest_sha256,
                    "leg_id": leg_id,
                },
            )
        normalized_score = float(index)
        rows.append(
            {
                "instrument": "__self_check__",
                "leg_id": leg_id,
                "normalized_score": normalized_score,
                "weight": float(weight),
            }
        )
        combined += normalized_score * float(weight)
    if not rows:
        raise StrategyPackageValidationError(
            "multi-alpha frozen StrategyPackage self-check produced no combined smoke rows",
            context={
                "reason_code": "multi_alpha_promotion_parent_combined_signal_failed",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
    replay = sum(item["normalized_score"] * item["weight"] for item in rows)
    if replay != combined:
        raise StrategyPackageValidationError(
            "multi-alpha frozen StrategyPackage self-check combined replay is not deterministic",
            context={
                "reason_code": "multi_alpha_promotion_parent_combined_signal_failed",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "combined_score": combined,
                "replay_score": replay,
            },
        )
    return {
        "schema_version": "multi_alpha_parent_combined_signal_smoke_v1",
        "trade_date": smoke_date.isoformat(),
        "instrument": "__self_check__",
        "leg_count": len(rows),
        "combined_score": combined,
        "deterministic_replay": True,
    }


def _self_check_trade_date(manifest: StrategyPackageManifest) -> date | None:
    for value in (
        (manifest.backtest_context or {}).get("self_check_trade_date"),
        (manifest.backtest_context or {}).get("oos_end"),
    ):
        parsed = _parse_date(value)
        if parsed is not None:
            return parsed
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None
