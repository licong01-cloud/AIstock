"""Fail-fast validators for Strategy Package manifests."""

from __future__ import annotations

from backend.execution_algos import ALGO_REGISTRY, get_algo

from backend.services.trading_core.execution_algo_capabilities import (
    normalize_execution_algo_code,
    validate_runtime_asset_paths,
)
from backend.services.trading_core.errors import (
    ExecutionAlgoError,
    StrategyPackageValidationError,
    UnsupportedFeatureError,
)

from .execution_policy import normalize_execution_policy_json
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
        self.validate_manifest_identity_for_paper_trading(manifest)

    def validate_manifest_identity_for_paper_trading(self, manifest: StrategyPackageManifest) -> None:
        """Validate immutable package lineage/status without binding execution algo.

        Paper v2 freezes the StrategyPackage factor/model manifest, while the
        minute execution policy can be selected from a separate backtest-
        validated policy snapshot. Callers that already validate such a policy
        should use this method instead of validating the manifest's historical
        minute policy asset.
        """

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

    def validate_execution_policy_for_paper(
        self,
        *,
        package_id: str,
        policy_json: dict,
        instantiate_runtime: bool = True,
        require_runtime_assets: bool = True,
    ) -> None:
        normalized_policy = normalize_execution_policy_json(policy_json)
        algo_code = normalize_execution_algo_code(normalized_policy.get("algo_code"))
        if algo_code not in ALGO_REGISTRY:
            raise UnsupportedFeatureError(
                "minute execution algorithm is not registered",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "registered_algos": sorted(ALGO_REGISTRY),
                },
            )
        algo_config = dict(normalized_policy.get("algo_config") or {})
        if algo_code == "V25_TWO_STAGE" and bool(algo_config.get("allow_default_day_features")):
            raise StrategyPackageValidationError(
                "V25_TWO_STAGE allow_default_day_features is diagnostic-only and cannot enter Paper Trading v2",
                context={"package_id": package_id, "algo_code": algo_code},
            )
        asset_paths = {}
        if require_runtime_assets:
            asset_paths = validate_runtime_asset_paths(
                algo_code=algo_code,
                algo_config=algo_config,
                package_id=package_id,
            )
        if not instantiate_runtime:
            return
        try:
            get_algo(algo_code, config=normalized_policy.get("algo_config") or {})
        except Exception as exc:
            raise ExecutionAlgoError(
                f"{algo_code} runtime is not available for paper trading",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "runtime_asset_paths": {key: str(path) for key, path in asset_paths.items()},
                    "reason": f"{type(exc).__name__}: {exc}",
                },
            ) from exc
