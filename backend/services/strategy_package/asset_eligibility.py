"""Asset-level StrategyPackage eligibility for Selection and simulations."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from backend.services.trading_core.errors import StrategyPackageValidationError

from .manifest import compute_manifest_sha256
from .models import PackageStatus
from .validators import StrategyPackageValidator


@dataclass(frozen=True)
class StrategyPackageAssetEligibilityCheck:
    name: str
    status: str
    severity: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyPackageAssetEligibilityResult:
    package_id: str
    manifest_sha256: str | None
    alpha_core_sha256: str | None
    eligible: bool
    status: str
    blockers: list[str]
    warnings: list[str]
    checks: list[StrategyPackageAssetEligibilityCheck]
    legacy_status: str | None
    legacy_status_normalized_to: str | None
    evaluated_at: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["checks"] = [check.to_dict() for check in self.checks]
        return payload


class StrategyPackageAssetEligibilityService:
    """Validate immutable package assets without Paper/Selection lifecycle gates."""

    def __init__(self, *, validator: StrategyPackageValidator | None = None) -> None:
        self.validator = validator or StrategyPackageValidator()

    def summarize(self, record: Any) -> StrategyPackageAssetEligibilityResult:
        package_id = str(getattr(record, "package_id", "") or "")
        manifest_sha256 = getattr(record, "manifest_sha256", None)
        legacy_status = _status_value(getattr(record, "package_status", None))
        checks: list[StrategyPackageAssetEligibilityCheck] = []

        if legacy_status == PackageStatus.RETIRED.value:
            checks.append(
                _check(
                    "package_lifecycle",
                    "FAIL",
                    "hard",
                    "retired StrategyPackage cannot start new selection or simulation runs",
                    {"package_id": package_id, "legacy_status": legacy_status},
                )
            )
        else:
            checks.append(
                _check(
                    "package_lifecycle",
                    "PASS",
                    "hard",
                    "package is not retired",
                    {"package_id": package_id, "legacy_status": legacy_status},
                )
            )

        manifest = _current_manifest(record)
        if manifest is None:
            if manifest_sha256:
                checks.append(
                    _check(
                        "manifest_identity",
                        "WARN",
                        "warning",
                        "manifest payload is not available to this reader; deferred to runtime repository",
                        {"package_id": package_id, "manifest_sha256": manifest_sha256},
                    )
                )
            else:
                checks.append(
                    _check(
                        "manifest_identity",
                        "FAIL",
                        "hard",
                        "manifest_sha256 is required for StrategyPackage asset eligibility",
                        {"package_id": package_id},
                    )
                )
            return self._result(package_id, manifest_sha256, None, legacy_status, checks)

        if getattr(manifest, "package_id", None) != package_id:
            checks.append(
                _check(
                    "manifest_identity",
                    "FAIL",
                    "hard",
                    "manifest package_id does not match repository record",
                    {
                        "package_id": package_id,
                        "manifest_package_id": getattr(manifest, "package_id", None),
                    },
                )
            )
        if not manifest_sha256 or not getattr(manifest, "manifest_sha256", None):
            checks.append(
                _check(
                    "manifest_identity",
                    "FAIL",
                    "hard",
                    "frozen manifest_sha256 is required",
                    {
                        "package_id": package_id,
                        "record_manifest_sha256": manifest_sha256,
                        "manifest_sha256": getattr(manifest, "manifest_sha256", None),
                    },
                )
            )
        elif manifest_sha256 != manifest.manifest_sha256:
            checks.append(
                _check(
                    "manifest_identity",
                    "FAIL",
                    "hard",
                    "record manifest_sha256 does not match manifest payload",
                    {
                        "package_id": package_id,
                        "record_manifest_sha256": manifest_sha256,
                        "manifest_sha256": manifest.manifest_sha256,
                    },
                )
            )
        else:
            checks.append(
                _check(
                    "manifest_identity",
                    "PASS",
                    "hard",
                    "record and manifest identity match",
                    {"package_id": package_id, "manifest_sha256": manifest_sha256},
                )
            )

        try:
            self.validator.validate_manifest(manifest)
        except StrategyPackageValidationError as exc:
            checks.append(
                _check(
                    "manifest_asset_checks",
                    "FAIL",
                    "hard",
                    exc.message,
                    {"package_id": package_id, **exc.context},
                )
            )
        else:
            checks.append(
                _check(
                    "manifest_asset_checks",
                    "PASS",
                    "hard",
                    "manifest hash and asset checks passed",
                    {"package_id": package_id},
                )
            )

        actual: str | None = None
        try:
            actual = compute_manifest_sha256(manifest)
        except Exception as exc:
            checks.append(
                _check(
                    "manifest_hash_compute",
                    "FAIL",
                    "hard",
                    "manifest hash could not be recomputed",
                    {"package_id": package_id, "reason": f"{type(exc).__name__}: {exc}"},
                )
            )
        else:
            if getattr(manifest, "manifest_sha256", None) and actual != manifest.manifest_sha256:
                checks.append(
                    _check(
                        "manifest_hash_compute",
                        "FAIL",
                        "hard",
                        "manifest_sha256 does not match canonical manifest payload",
                        {"package_id": package_id, "expected": manifest.manifest_sha256, "actual": actual},
                    )
                )
            else:
                checks.append(
                    _check(
                        "manifest_hash_compute",
                        "PASS",
                        "hard",
                        "canonical manifest hash is stable",
                        {"package_id": package_id, "manifest_sha256": actual},
                    )
                )

        checks.extend(_alpha_core_shape_checks(manifest))
        return self._result(package_id, manifest_sha256, actual, legacy_status, checks)

    def require_eligible(self, record: Any) -> StrategyPackageAssetEligibilityResult:
        result = self.summarize(record)
        if result.eligible:
            return result
        raise StrategyPackageValidationError(
            "strategy package asset eligibility failed",
            context=result.to_dict(),
        )

    @staticmethod
    def _result(
        package_id: str,
        manifest_sha256: str | None,
        alpha_core_sha256: str | None,
        legacy_status: str | None,
        checks: list[StrategyPackageAssetEligibilityCheck],
    ) -> StrategyPackageAssetEligibilityResult:
        blockers = [check.name for check in checks if check.severity == "hard" and check.status == "FAIL"]
        warnings = [check.name for check in checks if check.status == "WARN"]
        return StrategyPackageAssetEligibilityResult(
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            alpha_core_sha256=alpha_core_sha256,
            eligible=not blockers,
            status="ELIGIBLE" if not blockers else "BLOCKED",
            blockers=blockers,
            warnings=warnings,
            checks=checks,
            legacy_status=legacy_status,
            legacy_status_normalized_to=_normalized_legacy_status(legacy_status),
            evaluated_at=datetime.now(timezone.utc).isoformat(),
        )


def _check(
    name: str,
    status: str,
    severity: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> StrategyPackageAssetEligibilityCheck:
    return StrategyPackageAssetEligibilityCheck(
        name=name,
        status=status,
        severity=severity,
        message=message,
        context=context or {},
    )


def _current_manifest(record: Any) -> Any | None:
    current_manifest = getattr(record, "current_manifest", None)
    if callable(current_manifest):
        return current_manifest()
    return getattr(record, "manifest", None)


def _status_value(status: Any) -> str | None:
    if status is None:
        return None
    return str(getattr(status, "value", status))


def _normalized_legacy_status(status: str | None) -> str | None:
    if not status:
        return None
    if status in {
        PackageStatus.DRAFT.value,
        PackageStatus.ASSET_VALIDATED.value,
        PackageStatus.BACKTEST_APPROVED.value,
        PackageStatus.RETIRED.value,
    }:
        return status
    return PackageStatus.BACKTEST_APPROVED.value


def _alpha_core_shape_checks(manifest: Any) -> list[StrategyPackageAssetEligibilityCheck]:
    checks: list[StrategyPackageAssetEligibilityCheck] = []
    if not getattr(manifest, "factor_set", None):
        checks.append(_check("factor_assets", "FAIL", "hard", "factor_set is required", {"package_id": manifest.package_id}))
    else:
        checks.append(
            _check(
                "factor_assets",
                "PASS",
                "hard",
                "factor assets are declared",
                {"package_id": manifest.package_id, "factor_count": len(manifest.factor_set)},
            )
        )
    model_asset = getattr(manifest, "model_asset", None)
    if isinstance(model_asset, list) and not model_asset:
        missing_model = True
    else:
        missing_model = model_asset is None
    if missing_model:
        checks.append(_check("model_assets", "FAIL", "hard", "model_asset is required", {"package_id": manifest.package_id}))
    else:
        count = len(model_asset) if isinstance(model_asset, list) else 1
        checks.append(
            _check(
                "model_assets",
                "PASS",
                "hard",
                "model assets are declared",
                {"package_id": manifest.package_id, "model_asset_count": count},
            )
        )
    if not getattr(manifest, "backtest_summary", None):
        checks.append(
            _check(
                "source_backtest_evidence",
                "FAIL",
                "hard",
                "backtest_summary is required",
                {"package_id": manifest.package_id},
            )
        )
    else:
        checks.append(
            _check(
                "source_backtest_evidence",
                "PASS",
                "hard",
                "source backtest evidence is present",
                {"package_id": manifest.package_id},
            )
        )
    return checks
