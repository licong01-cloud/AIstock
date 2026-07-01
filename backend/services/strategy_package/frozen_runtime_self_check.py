"""Fail-closed self-check for frozen StrategyPackage runtime assets."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, TradingCoreError

from .live_inference import QEExperimentRuntimeAssetResolver, win_to_wsl_path
from .models import StrategyPackageManifest
from .package_asset_store import PackageAssetStore

ModelLoader = Callable[[Path], tuple[Any, str, Any, int]]


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

    def to_context(self) -> dict[str, Any]:
        return {
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
