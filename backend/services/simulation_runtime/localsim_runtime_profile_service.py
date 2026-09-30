"""Business service for LocalSIM package-scoped runtime profile versions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, Protocol

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError

from .localsim_runtime_profile import (
    LocalSimRuntimeProfileConfigV1,
    LocalSimRuntimeProfileConfigRequestV1,
    LocalSimRuntimeProfileValidationStatus,
    LocalSimRuntimeProfileV1,
    LocalSimRuntimeProfileVersionV1,
    build_localsim_runtime_profile,
)
from .localsim_runtime_profile_repository import LocalSimRuntimeProfileRepositoryProtocol


Clock = Callable[[], datetime]


class LocalSimRuntimeProfileAuthorityProtocol(Protocol):
    def resolve_current_manifest_sha256(self, package_id: str) -> str: ...

    def require_package_identity(self, *, package_id: str, manifest_sha256: str) -> None: ...

    def validate_and_materialize_config(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        config: LocalSimRuntimeProfileConfigRequestV1,
    ) -> tuple[LocalSimRuntimeProfileConfigV1, dict[str, Any]]: ...


class LocalSimRuntimeProfileService:
    def __init__(
        self,
        *,
        repository: LocalSimRuntimeProfileRepositoryProtocol,
        authority: LocalSimRuntimeProfileAuthorityProtocol,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self.repository = repository
        self.authority = authority
        self.clock = clock

    def create_profile(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        profile_name: str,
        created_by: str,
    ) -> LocalSimRuntimeProfileV1:
        self.authority.require_package_identity(package_id=package_id, manifest_sha256=manifest_sha256)
        now = self._now()
        profile = build_localsim_runtime_profile(
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            profile_name=profile_name,
            created_by=created_by,
            now=now,
        )
        return self.repository.create_profile(profile)

    def create_profile_for_package(
        self, *, package_id: str, profile_name: str, created_by: str
    ) -> LocalSimRuntimeProfileV1:
        return self.create_profile(
            package_id=package_id,
            manifest_sha256=self.authority.resolve_current_manifest_sha256(package_id),
            profile_name=profile_name,
            created_by=created_by,
        )

    def create_version(
        self,
        *,
        profile_id: str,
        expected_profile_version: int,
        config_json: dict[str, Any],
        created_by: str,
    ) -> tuple[LocalSimRuntimeProfileV1, LocalSimRuntimeProfileVersionV1]:
        profile = self.repository.get_profile(profile_id)
        requested = LocalSimRuntimeProfileConfigRequestV1.model_validate(config_json)
        try:
            materialized, evidence = self.authority.validate_and_materialize_config(
                package_id=profile.package_id,
                manifest_sha256=profile.manifest_sha256,
                config=requested,
            )
            status = LocalSimRuntimeProfileValidationStatus.VALIDATED
        except RuntimeConfigInvalidError:
            raise
        except Exception as exc:
            raise RuntimeConfigInvalidError(
                "LocalSIM runtime profile authority validation failed",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_AUTHORITY_FAILED"},
            ) from exc
        if not evidence:
            raise RuntimeConfigInvalidError(
                "LocalSIM runtime profile validation returned no durable evidence",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_EVIDENCE_MISSING"},
            )
        return self.repository.create_version(
            profile_id=profile_id,
            expected_profile_version=expected_profile_version,
            config=materialized,
            validation_status=status,
            validation_evidence=evidence,
            created_by=created_by,
            created_at=self._now(),
        )

    def get_profile(self, profile_id: str) -> LocalSimRuntimeProfileV1:
        return self.repository.get_profile(profile_id)

    def get_version(self, profile_version_id: str) -> LocalSimRuntimeProfileVersionV1:
        return self.repository.get_version(profile_version_id)

    def retire_profile(
        self, *, profile_id: str, expected_version: int
    ) -> LocalSimRuntimeProfileV1:
        return self.repository.retire_profile(
            profile_id=profile_id,
            expected_version=expected_version,
            updated_at=self._now(),
        )

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise DataUnavailableError(
                "LocalSIM runtime profile clock must be timezone-aware",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CLOCK_INVALID"},
            )
        return value.astimezone(UTC)
