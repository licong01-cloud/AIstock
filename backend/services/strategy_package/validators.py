"""Fail-fast validators for Strategy Package manifests."""

from __future__ import annotations

from pathlib import Path

from backend.execution_algos import ALGO_REGISTRY, get_algo

from backend.services.trading_core.errors import (
    DataUnavailableError,
    ExecutionAlgoError,
    StrategyPackageValidationError,
    UnsupportedFeatureError,
)

from .manifest import compute_manifest_sha256
from .models import PackageStatus, StrategyPackageManifest


class StrategyPackageValidator:
    """Validate package structure and runtime readiness without side effects."""

    def validate_manifest(self, manifest: StrategyPackageManifest) -> None:
        failed = [check for check in manifest.asset_checks if not check.passed]
        if failed:
            raise StrategyPackageValidationError(
                "strategy package has failed asset checks",
                context={"failed_checks": [check.model_dump() for check in failed]},
            )
        if manifest.manifest_sha256:
            actual = compute_manifest_sha256(manifest)
            if manifest.manifest_sha256 != actual:
                raise StrategyPackageValidationError(
                    "manifest_sha256 does not match manifest payload",
                    context={
                        "expected": manifest.manifest_sha256,
                        "actual": actual,
                        "package_id": manifest.package_id,
                    },
                )

    def validate_for_paper_trading(self, manifest: StrategyPackageManifest) -> None:
        self.validate_manifest(manifest)
        if manifest.package_status not in {
            PackageStatus.BACKTEST_APPROVED,
            PackageStatus.SELECTION_ENABLED,
            PackageStatus.PAPER_ENABLED,
        }:
            raise StrategyPackageValidationError(
                "package is not approved for paper trading",
                context={
                    "package_id": manifest.package_id,
                    "package_status": manifest.package_status.value,
                },
            )
        algo_code = manifest.minute_execution_policy.algo_code
        if algo_code not in ALGO_REGISTRY:
            raise UnsupportedFeatureError(
                "minute execution algorithm is not registered",
                context={
                    "package_id": manifest.package_id,
                    "algo_code": algo_code,
                    "registered_algos": sorted(ALGO_REGISTRY),
                },
            )
        if algo_code == "V24_PLAN":
            self._validate_v24_plan(manifest)

    def _validate_v24_plan(self, manifest: StrategyPackageManifest) -> None:
        model_path = str(manifest.minute_execution_policy.algo_config.get("model_path") or "").strip()
        if not model_path:
            raise DataUnavailableError(
                "V24_PLAN requires model_path",
                context={"package_id": manifest.package_id},
            )
        if not Path(model_path).exists():
            raise DataUnavailableError(
                "V24_PLAN model_path is not accessible from AIstock backend",
                context={"package_id": manifest.package_id, "model_path": model_path},
            )
        try:
            get_algo("V24_PLAN", config=manifest.minute_execution_policy.algo_config)
        except Exception as exc:
            raise ExecutionAlgoError(
                "V24_PLAN runtime is not available for paper trading",
                context={
                    "package_id": manifest.package_id,
                    "model_path": model_path,
                    "reason": str(exc),
                },
            ) from exc
