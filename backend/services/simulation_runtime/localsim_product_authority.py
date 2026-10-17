"""Server-resolved product authorities for successor LocalSIM commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping, Protocol

from backend.services.hmm_training_service import HMMTrainingService
from backend.services.strategy_package.execution_policy import (
    ExecutionPolicyValidationStatus,
    ValidatedExecutionPolicy,
)
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime_variant import RuntimeVariantValidationStatus
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .localsim_runtime_profile import (
    LocalSimRuntimeProfileConfigRequestV1,
    LocalSimRuntimeProfileConfigV1,
    LocalSimRuntimeProfileStatus,
    LocalSimRuntimeProfileValidationStatus,
    LocalSimRuntimeProfileV1,
    LocalSimRuntimeProfileVersionV1,
)
from .localsim_runtime_profile_repository import LocalSimRuntimeProfileRepositoryProtocol
from .models import canonical_json_sha256


LOCALSIM_ADMISSION_RECEIPT_SCHEMA = "localsim_package_admission_receipt_v1"
LOCALSIM_TAIL_POLICY_SCHEMA = "localsim_tail_policy_v1"
_HMM_READY_STATUSES = frozenset({"completed", "success", "succeeded", "ready"})
_RUNTIME_VARIANT_ALLOWED_KEYS = frozenset({"strategy_config", "portfolio_policy", "notes"})


class HMMSnapshotAuthorityProtocol(Protocol):
    def get_snapshot(self, snapshot_id: str) -> Mapping[str, Any] | None: ...

    def get_config(self, config_id: str) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True)
class LocalSimResolvedProductAuthorityV1:
    package_id: str
    manifest_sha256: str
    admission_receipt_id: str
    admission_receipt_hash: str
    admission_receipt_payload: dict[str, Any]
    runtime_profile: LocalSimRuntimeProfileV1
    runtime_profile_version: LocalSimRuntimeProfileVersionV1
    execution_policy: ValidatedExecutionPolicy
    tail_policy_version_id: str
    tail_policy_sha256: str
    tail_policy_json: dict[str, Any]

    def release_validation_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": "localsim_product_authority_evidence_v1",
            "admission_receipt": {
                "receipt_id": self.admission_receipt_id,
                "receipt_hash": self.admission_receipt_hash,
                "payload": self.admission_receipt_payload,
            },
            "runtime_profile": {
                "profile_id": self.runtime_profile.profile_id,
                "profile_hash": self.runtime_profile.profile_hash,
                "profile_version_id": self.runtime_profile_version.profile_version_id,
                "profile_version_hash": self.runtime_profile_version.profile_version_hash,
                "config_sha256": self.runtime_profile_version.config_sha256,
                "validation_evidence": self.runtime_profile_version.validation_evidence,
            },
            "execution_policy": {
                "policy_id": self.execution_policy.policy_id,
                "policy_sha256": self.execution_policy.policy_sha256,
                "validation_status": self.execution_policy.validation_status.value,
                "source_backtest_id": self.execution_policy.source_backtest_id,
                "source_backtest_status": self.execution_policy.source_backtest_status,
            },
            "tail_policy": {
                "policy_version_id": self.tail_policy_version_id,
                "policy_sha256": self.tail_policy_sha256,
                "policy_json": self.tail_policy_json,
            },
        }


class LocalSimProductAuthority:
    """Resolve every non-user product identity from durable repositories."""

    def __init__(
        self,
        *,
        profile_repository: LocalSimRuntimeProfileRepositoryProtocol,
        package_repository: Any | None = None,
        package_service: Any | None = None,
        hmm_authority: HMMSnapshotAuthorityProtocol | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.package_service = package_service or StrategyPackageService(repository=self.package_repository)
        self.profile_repository = profile_repository
        self.hmm_authority = hmm_authority or HMMTrainingService()

    def require_package_identity(self, *, package_id: str, manifest_sha256: str) -> None:
        record = self._package_record(package_id)
        if record.manifest_sha256 != manifest_sha256:
            self._fail(
                "LocalSIM package manifest identity drifted",
                "LOCALSIM_PACKAGE_MANIFEST_MISMATCH",
                package_id=package_id,
                requested_manifest_sha256=manifest_sha256,
                current_manifest_sha256=record.manifest_sha256,
            )
        self._require_package_admission(record)

    def resolve_current_manifest_sha256(self, package_id: str) -> str:
        record = self._package_record(package_id)
        self._require_package_admission(record)
        return str(record.manifest_sha256)

    def validate_and_materialize_config(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        config: LocalSimRuntimeProfileConfigRequestV1,
    ) -> tuple[LocalSimRuntimeProfileConfigV1, dict[str, Any]]:
        self.require_package_identity(package_id=package_id, manifest_sha256=manifest_sha256)
        hmm_evidence = self._resolve_hmm(config)
        variant_config, variant_evidence = self._resolve_runtime_variant(
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            config=config,
        )
        materialized = config.materialize(runtime_variant_materialized_config=variant_config)
        evidence = {
            "schema_version": "localsim_runtime_profile_validation_evidence_v1",
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "hmm_reference": hmm_evidence,
            "runtime_variant": variant_evidence,
            "materialized_config_sha256": canonical_json_sha256(materialized.model_dump(mode="json")),
        }
        return materialized, evidence

    def resolve_product(
        self,
        *,
        package_id: str,
        runtime_profile_version_id: str,
        execution_policy_version_id: str,
    ) -> LocalSimResolvedProductAuthorityV1:
        record = self._package_record(package_id)
        admission_receipt_id, admission_hash, admission_payload = self._admission_receipt(record)
        profile_version = self.profile_repository.get_version(runtime_profile_version_id)
        profile = self.profile_repository.get_profile(profile_version.profile_id)
        if profile.status is not LocalSimRuntimeProfileStatus.ACTIVE:
            self._fail("LocalSIM runtime profile is retired", "LOCALSIM_RUNTIME_PROFILE_RETIRED")
        if profile_version.validation_status is not LocalSimRuntimeProfileValidationStatus.VALIDATED:
            self._fail("LocalSIM runtime profile version is not validated", "LOCALSIM_RUNTIME_PROFILE_NOT_VALIDATED")
        expected = (record.package_id, record.manifest_sha256)
        if (profile.package_id, profile.manifest_sha256) != expected or (
            profile_version.package_id,
            profile_version.manifest_sha256,
        ) != expected:
            self._fail(
                "LocalSIM runtime profile is not bound to the requested package identity",
                "LOCALSIM_RUNTIME_PROFILE_PACKAGE_MISMATCH",
            )

        policy = self.package_repository.get_execution_policy(package_id, execution_policy_version_id)
        if policy.package_id != package_id or policy.manifest_sha256 != record.manifest_sha256:
            self._fail(
                "LocalSIM execution policy is not bound to the requested package identity",
                "LOCALSIM_EXECUTION_POLICY_PACKAGE_MISMATCH",
            )
        if policy.validation_status is not ExecutionPolicyValidationStatus.BACKTEST_VALIDATED:
            self._fail("LocalSIM execution policy is not validated", "LOCALSIM_EXECUTION_POLICY_NOT_VALIDATED")
        if str(policy.algo_code or "").strip().upper() != "TWAP":
            self._fail(
                "LocalSIM effective execution policy must be TWAP",
                "LOCALSIM_TWAP_ONLY_POLICY_REQUIRED",
                execution_policy_version_id=execution_policy_version_id,
                algo_code=policy.algo_code,
            )
        tail_json = _tail_policy_snapshot(policy.policy_json)
        tail_hash = canonical_json_sha256(tail_json)
        return LocalSimResolvedProductAuthorityV1(
            package_id=record.package_id,
            manifest_sha256=record.manifest_sha256,
            admission_receipt_id=admission_receipt_id,
            admission_receipt_hash=admission_hash,
            admission_receipt_payload=admission_payload,
            runtime_profile=profile,
            runtime_profile_version=profile_version,
            execution_policy=policy,
            tail_policy_version_id=f"lstail_{tail_hash[:16]}",
            tail_policy_sha256=tail_hash,
            tail_policy_json=tail_json,
        )

    def _package_record(self, package_id: str) -> Any:
        record = self.package_repository.get(package_id)
        if record.package_status is PackageStatus.RETIRED:
            self._fail("retired StrategyPackage cannot create a LocalSIM product", "LOCALSIM_PACKAGE_RETIRED")
        return record

    def _require_package_admission(self, record: Any) -> dict[str, Any]:
        admission = self.package_service.paper_simulation_admission(record.package_id, governance_limit=0)
        if admission.get("manifest_sha256") != record.manifest_sha256:
            self._fail("StrategyPackage admission manifest drifted", "LOCALSIM_PACKAGE_ADMISSION_DRIFT")
        if not admission.get("paper_simulation_allowed"):
            self._fail(
                "StrategyPackage is not eligible for LocalSIM",
                "LOCALSIM_PACKAGE_ADMISSION_BLOCKED",
                blockers=list(admission.get("blockers") or []),
            )
        return admission

    def _admission_receipt(self, record: Any) -> tuple[str, str, dict[str, Any]]:
        admission = self._require_package_admission(record)
        events = self.package_repository.list_status_events(record.package_id, limit=200)
        matching = [event for event in events if event.to_status is record.package_status]
        if not matching:
            self._fail(
                "StrategyPackage current status has no durable status event",
                "LOCALSIM_PACKAGE_STATUS_EVENT_MISSING",
            )
        status_event = matching[-1]
        asset = dict(admission.get("asset_eligibility") or {})
        checks = []
        for check in asset.get("checks") or []:
            checks.append(
                {
                    "name": check.get("name"),
                    "status": check.get("status"),
                    "severity": check.get("severity"),
                    "context": check.get("context") or {},
                }
            )
        payload = {
            "schema_version": LOCALSIM_ADMISSION_RECEIPT_SCHEMA,
            "package_id": record.package_id,
            "manifest_sha256": record.manifest_sha256,
            "package_status": record.package_status.value,
            "status_event": {
                "from_status": status_event.from_status.value if status_event.from_status else None,
                "to_status": status_event.to_status.value,
                "reason": status_event.reason,
                "context": status_event.context,
                "created_at": _iso_utc(status_event.created_at),
            },
            "asset_eligibility": {
                "manifest_sha256": asset.get("manifest_sha256"),
                "alpha_core_sha256": asset.get("alpha_core_sha256"),
                "eligible": bool(asset.get("eligible")),
                "status": asset.get("status"),
                "blockers": sorted(str(item) for item in asset.get("blockers") or []),
                "checks": checks,
            },
        }
        digest = canonical_json_sha256(payload)
        return f"lsadm_{digest[:16]}", digest, payload

    def _resolve_hmm(self, config: LocalSimRuntimeProfileConfigRequestV1) -> dict[str, Any]:
        if not config.hmm.enabled:
            return {"enabled": False, "reference_sha256": canonical_json_sha256({"enabled": False})}
        snapshot_id = str(config.hmm.snapshot_id)
        if config.hmm.model_version != snapshot_id:
            self._fail(
                "HMM model_version must identify the exact immutable snapshot",
                "LOCALSIM_HMM_MODEL_VERSION_MISMATCH",
            )
        snapshot = self.hmm_authority.get_snapshot(snapshot_id)
        if snapshot is None:
            self._fail("HMM snapshot does not exist", "LOCALSIM_HMM_SNAPSHOT_MISSING", snapshot_id=snapshot_id)
        status = str(snapshot.get("status") or "").strip().casefold()
        if status not in _HMM_READY_STATUSES:
            self._fail(
                "HMM snapshot is not ready",
                "LOCALSIM_HMM_SNAPSHOT_NOT_READY",
                snapshot_id=snapshot_id,
                snapshot_status=snapshot.get("status"),
            )
        config_id = str(snapshot.get("config_id") or "").strip()
        hmm_config = self.hmm_authority.get_config(config_id) if config_id else None
        if hmm_config is None:
            self._fail("HMM snapshot config does not exist", "LOCALSIM_HMM_CONFIG_MISSING", config_id=config_id)
        config_json = hmm_config.get("config_json") or {}
        presets = config_json.get("signal_presets") if isinstance(config_json, Mapping) else None
        preset = str(config.hmm.preset)
        if isinstance(presets, Mapping) and presets and preset not in presets:
            self._fail(
                "HMM preset does not exist in the snapshot config",
                "LOCALSIM_HMM_PRESET_MISSING",
                preset=preset,
            )
        reference = {
            "enabled": True,
            "snapshot_id": snapshot_id,
            "model_version": snapshot_id,
            "preset": preset,
            "config_id": config_id,
            "snapshot_status": snapshot.get("status"),
            "trained_at": _json_time(snapshot.get("trained_at")),
            "sector_count": snapshot.get("sector_count"),
            "metrics_sha256": canonical_json_sha256(snapshot.get("metrics_json") or {}),
            "config_sha256": canonical_json_sha256(config_json),
        }
        return {**reference, "reference_sha256": canonical_json_sha256(reference)}

    def _resolve_runtime_variant(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        config: LocalSimRuntimeProfileConfigRequestV1,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        if config.runtime_variant_id is None:
            return None, {"present": False}
        variant = self.package_repository.get_runtime_variant(package_id, config.runtime_variant_id)
        if variant.package_id != package_id or variant.manifest_sha256 != manifest_sha256:
            self._fail("runtime variant package identity mismatch", "LOCALSIM_RUNTIME_VARIANT_PACKAGE_MISMATCH")
        if variant.validation_status is not RuntimeVariantValidationStatus.VALIDATION_PASSED:
            self._fail("runtime variant is not validated", "LOCALSIM_RUNTIME_VARIANT_NOT_VALIDATED")
        if variant.variant_hash != config.runtime_variant_hash:
            self._fail("runtime variant hash drifted", "LOCALSIM_RUNTIME_VARIANT_HASH_MISMATCH")
        unknown = sorted(set(variant.variant_config).difference(_RUNTIME_VARIANT_ALLOWED_KEYS))
        if unknown:
            self._fail(
                "runtime variant contains fields outside LocalSIM profile authority",
                "LOCALSIM_RUNTIME_VARIANT_FIELDS_FORBIDDEN",
                forbidden_fields=unknown,
            )
        materialized = dict(variant.variant_config)
        return materialized, {
            "present": True,
            "variant_id": variant.variant_id,
            "variant_hash": variant.variant_hash,
            "validation_status": variant.validation_status.value,
            "validation_evidence_sha256": canonical_json_sha256(variant.validation_evidence),
            "materialized_config_sha256": canonical_json_sha256(materialized),
        }

    @staticmethod
    def _fail(message: str, reason_code: str, **context: Any) -> None:
        raise RuntimeConfigInvalidError(message, context={"reason_code": reason_code, **context})


def _tail_policy_snapshot(policy_json: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": LOCALSIM_TAIL_POLICY_SCHEMA,
        "algo_code": "TWAP",
        "unfilled_handler": policy_json.get("unfilled_handler") or "default_fail_fast",
        "unfilled_handler_params": dict(policy_json.get("unfilled_handler_params") or {}),
        "fallback_policy": dict(policy_json.get("fallback_policy") or {}),
    }


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeConfigInvalidError(
            "StrategyPackage status event timestamp is not timezone-aware",
            context={"reason_code": "LOCALSIM_PACKAGE_STATUS_EVENT_TIME_INVALID"},
        )
    return value.astimezone(UTC).isoformat()


def _json_time(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso_utc(value)
    return value
