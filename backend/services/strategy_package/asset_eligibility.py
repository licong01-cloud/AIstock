"""Asset-level StrategyPackage eligibility for Selection and simulations."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from backend.services.paper_trading_v2.models import BrokerBackendId
from backend.services.trading_core.errors import PackageAssetInvalidError, StrategyPackageValidationError

from .frozen_runtime_self_check import runtime_asset_admission_status
from .manifest import compute_manifest_sha256
from .models import PackageStatus, SelectionScoreArtifactStatus
from .validators import StrategyPackageValidator

MULTI_ALPHA_PAPER_ADMISSION_BLOCKER = "multi_alpha_runtime_not_validated_until_dry_run"
MULTI_ALPHA_LOCALSIM_DRY_RUN_NOT_REQUIRED = "multi_alpha_localsim_dry_run_not_required"
MULTI_ALPHA_SIGNAL_ADMISSION_SCHEMA = "multi_alpha_signal_admission_v1"
MULTI_ALPHA_COMBINED_SIGNAL_SMOKE_SCHEMA = "multi_alpha_parent_combined_signal_smoke_v1"
MULTI_ALPHA_SIGNAL_ADMISSION_PASSED = "multi_alpha_signal_admission_passed"
MULTI_ALPHA_SIGNAL_SELF_CHECK_PASSED = "multi_alpha_signal_self_check_passed"
MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE = "multi_alpha_selection_artifact_available"
MULTI_ALPHA_LEGACY_DRY_RUN_BLOCKER_SUPERSEDED = "multi_alpha_legacy_paper_dry_run_blocker_superseded"
MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED = "multi_alpha_signal_admission_not_validated"
MULTI_ALPHA_SIGNAL_SELF_CHECK_FAILED = "multi_alpha_signal_self_check_failed"
MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY = "multi_alpha_signal_selection_artifact_empty"
MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC = "multi_alpha_signal_selection_artifact_nondeterministic"
MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE = "multi_alpha_signal_selection_artifact_unavailable"
MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER = "multi_alpha_signal_unknown_manifest_blocker"
MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING = "multi_alpha_signal_evidence_missing"
_DEFAULT_SELECTION_ARTIFACT_READER = object()


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

    def __init__(
        self,
        *,
        validator: StrategyPackageValidator | None = None,
        admission_reader: Any | None = None,
        selection_artifact_reader: Any | None = None,
    ) -> None:
        self.validator = validator or StrategyPackageValidator()
        # Kept for constructor compatibility only; multi-alpha signal admission
        # must not consult the legacy paper dry-run admission repository.
        self.admission_reader = admission_reader
        self.selection_artifact_reader = (
            selection_artifact_reader
            if selection_artifact_reader is not None
            else _DEFAULT_SELECTION_ARTIFACT_READER
        )

    def summarize(
        self,
        record: Any,
        *,
        broker_backend: BrokerBackendId = "local_sim",
        runtime_variant: str | None = None,
    ) -> StrategyPackageAssetEligibilityResult:
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

        admission_passed, admission_context = runtime_asset_admission_status(manifest)
        checks.append(
            _check(
                "runtime_asset_admission",
                "PASS" if admission_passed else "FAIL",
                "hard",
                (
                    "one-time StrategyPackage runtime asset admission passed"
                    if admission_passed
                    else "StrategyPackage must complete one-time runtime asset admission before Selection or simulation"
                ),
                admission_context,
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
        checks.extend(
            _multi_alpha_signal_admission_checks(
                manifest,
                broker_backend=broker_backend,
                runtime_variant=runtime_variant,
                selection_artifact_reader=self.selection_artifact_reader,
            )
        )
        return self._result(package_id, manifest_sha256, actual, legacy_status, checks)

    def require_eligible(
        self,
        record: Any,
        *,
        broker_backend: BrokerBackendId = "local_sim",
        runtime_variant: str | None = None,
    ) -> StrategyPackageAssetEligibilityResult:
        result = self.summarize(record, broker_backend=broker_backend, runtime_variant=runtime_variant)
        if result.eligible:
            return result
        raise PackageAssetInvalidError(
            "strategy package alpha core asset eligibility failed",
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
    if status in {item.value for item in PackageStatus}:
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


def _multi_alpha_signal_admission_checks(
    manifest: Any,
    *,
    broker_backend: BrokerBackendId = "local_sim",
    runtime_variant: str | None = None,
    selection_artifact_reader: Any | None = None,
) -> list[StrategyPackageAssetEligibilityCheck]:
    if _status_value(getattr(manifest, "alpha_mode", None)) != "multi_alpha":
        return []
    evidence = getattr(manifest, "source_evidence", {}) or {}
    multi_alpha = evidence.get("multi_alpha") if isinstance(evidence, dict) else None
    paper_admission = multi_alpha.get("paper_admission") if isinstance(multi_alpha, dict) else None
    blocking = list(paper_admission.get("blocking") or []) if isinstance(paper_admission, dict) else []
    resolved_variant = runtime_variant or _runtime_variant_from_manifest(manifest)
    checks: list[StrategyPackageAssetEligibilityCheck] = []
    legacy_blocker_seen = False
    for reason in blocking:
        reason_text = str(reason)
        if reason_text == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER:
            legacy_blocker_seen = True
            continue
        checks.append(
            _check(
                MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER,
                "FAIL",
                "hard",
                "MULTI_ALPHA manifest contains an unknown paper admission blocker; only the legacy dry-run blocker can be superseded by signal admission",
                {
                    "reason_code": MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER,
                    "unknown_blocker": reason_text,
                    "package_id": manifest.package_id,
                    "alpha_mode": "multi_alpha",
                    "broker_backend": broker_backend,
                    "runtime_variant": resolved_variant,
                },
            )
        )

    signal_checks = _evaluate_multi_alpha_signal_evidence(
        manifest,
        multi_alpha=multi_alpha,
        broker_backend=broker_backend,
        runtime_variant=resolved_variant,
        selection_artifact_reader=selection_artifact_reader,
    )
    checks.extend(signal_checks)
    signal_blocked = any(check.severity == "hard" and check.status == "FAIL" for check in signal_checks)
    if legacy_blocker_seen and not signal_blocked:
        checks.append(
            _check(
                MULTI_ALPHA_LEGACY_DRY_RUN_BLOCKER_SUPERSEDED,
                "WARN",
                "warning",
                "legacy MULTI_ALPHA paper dry-run blocker is superseded by persisted signal admission evidence",
                {
                    "reason_code": MULTI_ALPHA_LEGACY_DRY_RUN_BLOCKER_SUPERSEDED,
                    "original_blocker": MULTI_ALPHA_PAPER_ADMISSION_BLOCKER,
                    "package_id": manifest.package_id,
                    "alpha_mode": "multi_alpha",
                    "manifest_sha256": getattr(manifest, "manifest_sha256", None),
                    "broker_backend": broker_backend,
                    "runtime_variant": resolved_variant,
                    "dry_run_required_for_signal_admission": False,
                },
            )
        )
    return checks


def _evaluate_multi_alpha_signal_evidence(
    manifest: Any,
    *,
    multi_alpha: Any,
    broker_backend: BrokerBackendId,
    runtime_variant: str,
    selection_artifact_reader: Any | None,
) -> list[StrategyPackageAssetEligibilityCheck]:
    persisted = _persisted_signal_admission_evidence(multi_alpha)
    if persisted is not None:
        return _checks_from_persisted_signal_evidence(
            manifest,
            persisted,
            broker_backend=broker_backend,
            runtime_variant=runtime_variant,
        )

    artifact_check = _selection_artifact_evidence_check(
        manifest,
        broker_backend=broker_backend,
        runtime_variant=runtime_variant,
        selection_artifact_reader=selection_artifact_reader,
    )
    if artifact_check is not None:
        if artifact_check.status == "PASS":
            return [
                artifact_check,
                _check(
                    MULTI_ALPHA_SIGNAL_ADMISSION_PASSED,
                    "PASS",
                    "hard",
                    "MULTI_ALPHA signal admission passed from persisted selection artifact evidence",
                    {
                        "package_id": manifest.package_id,
                        "alpha_mode": "multi_alpha",
                        "manifest_sha256": getattr(manifest, "manifest_sha256", None),
                        "broker_backend": broker_backend,
                        "runtime_variant": runtime_variant,
                        "evidence_source": "selection_score_artifact",
                        "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_PASSED,
                        "hot_path_full_self_check_replayed": False,
                    },
                ),
            ]
        return [artifact_check]

    return _legacy_structural_signal_smoke_checks(
        manifest,
        multi_alpha=multi_alpha,
        broker_backend=broker_backend,
        runtime_variant=runtime_variant,
    )


def _persisted_signal_admission_evidence(multi_alpha: Any) -> dict[str, Any] | None:
    if not isinstance(multi_alpha, dict):
        return None
    evidence = multi_alpha.get("signal_admission")
    return dict(evidence) if isinstance(evidence, dict) else None


def _checks_from_persisted_signal_evidence(
    manifest: Any,
    evidence: dict[str, Any],
    *,
    broker_backend: BrokerBackendId,
    runtime_variant: str,
) -> list[StrategyPackageAssetEligibilityCheck]:
    base_context = {
        "package_id": manifest.package_id,
        "alpha_mode": "multi_alpha",
        "manifest_sha256": getattr(manifest, "manifest_sha256", None),
        "broker_backend": broker_backend,
        "runtime_variant": runtime_variant,
        "evidence_source": "persisted_manifest_signal_admission",
        "evidence_schema_version": evidence.get("schema_version"),
        "hot_path_full_self_check_replayed": False,
    }
    if evidence.get("schema_version") != MULTI_ALPHA_SIGNAL_ADMISSION_SCHEMA:
        return [
            _check(
                MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                "FAIL",
                "hard",
                "MULTI_ALPHA signal admission evidence has an unknown schema version",
                {**base_context, "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED},
            )
        ]
    if not _is_sha256(evidence.get("self_check_manifest_sha256")) or evidence.get("persisted_for_hot_path") is not True:
        return [
            _check(
                MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                "FAIL",
                "hard",
                "MULTI_ALPHA signal admission evidence is not a persisted build-time self-check marker",
                {
                    **base_context,
                    "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                    "self_check_manifest_sha256": evidence.get("self_check_manifest_sha256"),
                    "persisted_for_hot_path": evidence.get("persisted_for_hot_path"),
                },
            )
        ]
    if evidence.get("paper_runtime_dry_run_required") is not False:
        return [
            _check(
                MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                "FAIL",
                "hard",
                "MULTI_ALPHA signal admission evidence must declare paper dry-run is not required for signal admission",
                {
                    **base_context,
                    "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                    "paper_runtime_dry_run_required": evidence.get("paper_runtime_dry_run_required"),
                },
            )
        ]
    if evidence.get("self_check_passed") is not True or evidence.get("self_check_origin") != "package_asset":
        return [
            _check(
                MULTI_ALPHA_SIGNAL_SELF_CHECK_FAILED,
                "FAIL",
                "hard",
                "MULTI_ALPHA build-time frozen self-check evidence is missing or failed",
                {
                    **base_context,
                    "reason_code": MULTI_ALPHA_SIGNAL_SELF_CHECK_FAILED,
                    "self_check_passed": evidence.get("self_check_passed"),
                    "self_check_origin": evidence.get("self_check_origin"),
                },
            )
        ]
    smoke = evidence.get("combined_signal_smoke")
    if not isinstance(smoke, dict):
        return [
            _check(
                MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
                "FAIL",
                "hard",
                "MULTI_ALPHA signal admission evidence is missing combined signal smoke",
                {**base_context, "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE},
            )
        ]
    smoke_schema = smoke.get("schema_version")
    leg_count = _positive_int(evidence.get("leg_count")) or _positive_int(smoke.get("leg_count"))
    if smoke_schema != MULTI_ALPHA_COMBINED_SIGNAL_SMOKE_SCHEMA or leg_count is None:
        return [
            _check(
                MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
                "FAIL",
                "hard",
                "MULTI_ALPHA combined signal smoke is empty or invalid",
                {
                    **base_context,
                    "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
                    "combined_signal_smoke_schema": smoke_schema,
                    "leg_count": smoke.get("leg_count"),
                },
            )
        ]
    deterministic = evidence.get("deterministic")
    if deterministic is None:
        deterministic = smoke.get("deterministic_replay")
    if deterministic is not True:
        return [
            _check(
                MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC,
                "FAIL",
                "hard",
                "MULTI_ALPHA combined signal smoke replay is not deterministic",
                {
                    **base_context,
                    "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC,
                    "deterministic": deterministic,
                },
            )
        ]
    return [
        _check(
            MULTI_ALPHA_SIGNAL_SELF_CHECK_PASSED,
            "PASS",
            "hard",
            "persisted build-time frozen self-check evidence proves parent package assets are self-contained",
            {
                **base_context,
                "reason_code": MULTI_ALPHA_SIGNAL_SELF_CHECK_PASSED,
                "self_check_passed": True,
                "self_check_origin": "package_asset",
            },
        ),
        _check(
            MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
            "PASS",
            "hard",
            "persisted combined signal smoke proves a deterministic non-empty selection signal can be produced",
            {
                **base_context,
                "reason_code": MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
                "combined_signal_smoke_schema": smoke_schema,
                "leg_count": leg_count,
                "deterministic": True,
            },
        ),
        _check(
            MULTI_ALPHA_SIGNAL_ADMISSION_PASSED,
            "PASS",
            "hard",
            "MULTI_ALPHA signal admission passed without paper dry-run admission",
            {**base_context, "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_PASSED},
        ),
    ]


def _selection_artifact_evidence_check(
    manifest: Any,
    *,
    broker_backend: BrokerBackendId,
    runtime_variant: str,
    selection_artifact_reader: Any | None,
) -> StrategyPackageAssetEligibilityCheck | None:
    if selection_artifact_reader is None or not getattr(manifest, "manifest_sha256", None):
        return None
    if selection_artifact_reader is _DEFAULT_SELECTION_ARTIFACT_READER:
        selection_artifact_reader = _default_selection_artifact_reader()
    context = {
        "package_id": manifest.package_id,
        "alpha_mode": "multi_alpha",
        "manifest_sha256": getattr(manifest, "manifest_sha256", None),
        "broker_backend": broker_backend,
        "runtime_variant": runtime_variant,
        "evidence_source": "selection_score_artifact",
        "hot_path_full_self_check_replayed": False,
    }
    try:
        artifacts = selection_artifact_reader.list(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            limit=5,
        )
    except Exception as exc:
        return _check(
            MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
            "FAIL",
            "hard",
            "MULTI_ALPHA signal admission could not read selection artifact evidence and no persisted self-check evidence was available",
            {
                **context,
                "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
                "artifact_lookup_error": f"{type(exc).__name__}: {exc}",
            },
        )
    artifacts = list(artifacts or [])
    if not artifacts:
        return None
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for artifact in artifacts:
        valid = _validate_selection_artifact(artifact)
        if valid["status"] == "PASS":
            return _check(
                MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
                "PASS",
                "hard",
                "successful selection artifact proves a non-empty deterministic MULTI_ALPHA selection signal",
                {
                    **context,
                    "reason_code": MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
                    **valid["context"],
                },
            )
        if valid["status"] == "FAIL":
            failed.append({"reason_code": valid["reason_code"], "message": valid["message"], **valid["context"]})
        else:
            skipped.append(valid["context"])
    if failed:
        first_failure = failed[0]
        reason_code = str(first_failure.pop("reason_code"))
        message = str(first_failure.pop("message"))
        return _check(
            reason_code,
            "FAIL",
            "hard",
            message,
            {**context, **first_failure, "reason_code": reason_code},
        )
    return _check(
        MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
        "FAIL",
        "hard",
        "MULTI_ALPHA signal admission found selection artifact records, but none were successful",
        {
            **context,
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
            "observed_artifacts": skipped[:5],
        },
    )


def _validate_selection_artifact(artifact: Any) -> dict[str, Any]:
    status = getattr(artifact, "status", None)
    status_value = str(getattr(status, "value", status))
    rows = list(getattr(artifact, "scores_json", None) or [])
    score_count = int(getattr(artifact, "score_count", 0) or 0)
    context = {
        "artifact_id": getattr(artifact, "artifact_id", None),
        "artifact_sha256": getattr(artifact, "artifact_sha256", None),
        "score_count": score_count,
        "row_count": len(rows),
        "status": status_value,
        "trade_date": getattr(artifact, "trade_date", None).isoformat()
        if getattr(artifact, "trade_date", None)
        else None,
    }
    if status_value != SelectionScoreArtifactStatus.SUCCEEDED.value:
        return {
            "status": "SKIP",
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
            "message": "selection artifact is not successful",
            "context": context,
        }
    if score_count < 1 or not rows:
        return {
            "status": "FAIL",
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
            "message": "MULTI_ALPHA selection artifact is empty",
            "context": context,
        }
    required = {"symbol", "rank", "score", "target_weight"}
    missing_rows = [
        index
        for index, row in enumerate(rows)
        if not isinstance(row, dict) or any(row.get(key) in (None, "") for key in required)
    ]
    if missing_rows:
        return {
            "status": "FAIL",
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
            "message": "MULTI_ALPHA selection artifact rows are missing required signal fields",
            "context": {**context, "missing_required_row_indexes": missing_rows[:10]},
        }
    artifact_sha = str(getattr(artifact, "artifact_sha256", "") or "").strip().lower()
    if _is_sha256(artifact_sha):
        context["deterministic_digest_present"] = True
    else:
        return {
            "status": "FAIL",
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC,
            "message": "MULTI_ALPHA selection artifact is missing its deterministic digest",
            "context": context,
        }
    metadata = getattr(artifact, "metadata", {}) or {}
    if isinstance(metadata, dict):
        context["target_weight_policy"] = metadata.get("target_weight_policy") or metadata.get("weight_policy")
        context["topk"] = metadata.get("final_topk") or metadata.get("topk")
    if context.get("target_weight_policy") in (None, "") or context.get("topk") in (None, ""):
        return {
            "status": "FAIL",
            "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
            "message": "MULTI_ALPHA selection artifact is missing target weight policy or topK metadata",
            "context": {
                **context,
                "missing_metadata_fields": [
                    key
                    for key in ("target_weight_policy", "topk")
                    if context.get(key) in (None, "")
                ],
            },
        }
    return {"status": "PASS", "reason_code": MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE, "context": context}


def _default_selection_artifact_reader() -> Any:
    from .selection_artifact import StrategyPackageSelectionArtifactRepository

    return StrategyPackageSelectionArtifactRepository()


def _legacy_structural_signal_smoke_checks(
    manifest: Any,
    *,
    multi_alpha: Any,
    broker_backend: BrokerBackendId,
    runtime_variant: str,
) -> list[StrategyPackageAssetEligibilityCheck]:
    context = {
        "package_id": manifest.package_id,
        "alpha_mode": "multi_alpha",
        "manifest_sha256": getattr(manifest, "manifest_sha256", None),
        "broker_backend": broker_backend,
        "runtime_variant": runtime_variant,
        "evidence_source": "legacy_structural_smoke_fallback",
        "cost_class": "cheap_structural_no_workspace_no_model_probe_no_wsl",
        "timeout_ms": 0,
        "hot_path_full_self_check_replayed": False,
    }
    top_authority = getattr(manifest, "source_evidence", {}) or {}
    if not isinstance(top_authority, dict) or top_authority.get("authority") != "parent_package_asset_runtime_authority":
        return [
            _check(
                MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                "FAIL",
                "hard",
                "MULTI_ALPHA signal admission requires persisted self-check evidence, a successful selection artifact, or parent package-asset build provenance",
                {
                    **context,
                    "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                    "source_evidence_authority": top_authority.get("authority") if isinstance(top_authority, dict) else None,
                },
            )
        ]
    if not isinstance(multi_alpha, dict):
        return [
            _check(
                MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
                "FAIL",
                "hard",
                "MULTI_ALPHA source_evidence.multi_alpha is missing",
                {**context, "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED},
            )
        ]
    legs = multi_alpha.get("legs")
    leg_ids = [
        str(leg.get("leg_id") or "").strip()
        for leg in legs
        if isinstance(legs, list) and isinstance(leg, dict) and str(leg.get("leg_id") or "").strip()
    ] if isinstance(legs, list) else []
    component_ids = [
        str(component.alpha_id)
        for component in getattr(manifest, "alpha_components", [])
        if str(component.alpha_id or "").strip()
    ]
    weights = getattr(getattr(manifest, "alpha_combination_policy", None), "weights", {}) or {}
    weight_ids = [str(key) for key in weights]
    component_id_set = set(component_ids)
    if (
        not isinstance(legs, list)
        or not legs
        or not component_id_set
        or set(leg_ids) != component_id_set
        or set(weight_ids) != component_id_set
    ):
        return [
            _check(
                MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
                "FAIL",
                "hard",
                "MULTI_ALPHA legacy structural smoke could not prove a non-empty deterministic combined signal",
                {
                    **context,
                    "reason_code": MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
                    "leg_count": len(legs) if isinstance(legs, list) else 0,
                    "leg_ids": sorted(leg_ids),
                    "component_count": len(component_ids),
                    "component_ids": sorted(component_ids),
                    "weight_ids": sorted(weight_ids),
                },
            )
        ]
    return [
        _check(
            MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING,
            "WARN",
            "warning",
            "MULTI_ALPHA package lacks persisted signal admission evidence and successful selection artifact evidence; using bounded structural legacy smoke",
            {**context, "reason_code": MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING},
        ),
        _check(
            MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
            "PASS",
            "hard",
            "legacy structural smoke proves deterministic non-empty combined signal shape without order generation",
            {
                **context,
                "reason_code": MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
                "leg_count": len(leg_ids),
                "component_count": len(component_ids),
                "deterministic": True,
            },
        ),
        _check(
            MULTI_ALPHA_SIGNAL_ADMISSION_PASSED,
            "PASS",
            "hard",
            "MULTI_ALPHA signal admission passed using bounded structural legacy smoke without order generation",
            {**context, "reason_code": MULTI_ALPHA_SIGNAL_ADMISSION_PASSED},
        ),
    ]


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _runtime_variant_from_manifest(manifest: Any) -> str:
    daily_strategy = (getattr(manifest, "backtest_context", {}) or {}).get("daily_strategy")
    topk = daily_strategy.get("topk") if isinstance(daily_strategy, dict) else None
    if topk is None:
        return "top_k=unknown"
    try:
        return f"top_k={int(topk)}"
    except (TypeError, ValueError):
        return f"top_k={topk}"
