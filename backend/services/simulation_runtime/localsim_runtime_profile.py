"""Package-scoped mutable-configuration authority for successor LocalSIM accounts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .models import canonical_json_sha256


LOCALSIM_RUNTIME_PROFILE_SCHEMA = "localsim_runtime_profile_v1"
LOCALSIM_RUNTIME_PROFILE_VERSION_SCHEMA = "localsim_runtime_profile_version_v1"


class LocalSimRuntimeProfileStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class LocalSimRuntimeProfileValidationStatus(str, Enum):
    VALIDATED = "VALIDATED"
    INVALID = "INVALID"
    RETIRED = "RETIRED"


class LocalSimDailyStrategyConfigV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy_id: str
    strategy_version: str
    top_k: int = Field(gt=0, le=10_000)
    industry_filters: tuple[str, ...] = ()
    sector_filters: tuple[str, ...] = ()
    parameters: dict[str, Any] = Field(default_factory=dict)

    @field_validator("strategy_id", "strategy_version")
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("daily strategy identity is required")
        return text


class LocalSimHmmConfigV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    snapshot_id: str | None = None
    model_version: str | None = None
    preset: str | None = None
    state_mapping: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _enabled_identity_is_complete(self) -> "LocalSimHmmConfigV1":
        identities = tuple(str(value or "").strip() for value in (self.snapshot_id, self.model_version, self.preset))
        if self.enabled and any(not value for value in identities):
            raise ValueError("enabled HMM requires snapshot_id, model_version, and preset")
        if not self.enabled and any(value for value in identities):
            raise ValueError("disabled HMM cannot retain an active snapshot, model version, or preset")
        return self


class LocalSimRuntimeProfileConfigV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_runtime_profile_config_v1"] = "localsim_runtime_profile_config_v1"
    daily_strategy: LocalSimDailyStrategyConfigV1
    hmm: LocalSimHmmConfigV1
    risk_policy: dict[str, Any]
    fee_policy: dict[str, Any]
    runtime_variant_id: str | None = None
    runtime_variant_hash: str | None = None
    runtime_variant_materialized_config: dict[str, Any] | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_variant_hash")
    @classmethod
    def _variant_hash_is_sha256(cls, value: str | None) -> str | None:
        if value is not None:
            _require_sha256(value, field="runtime_variant_hash")
        return value

    @model_validator(mode="after")
    def _variant_identity_is_complete(self) -> "LocalSimRuntimeProfileConfigV1":
        values = (self.runtime_variant_id, self.runtime_variant_hash, self.runtime_variant_materialized_config)
        if any(value is not None for value in values) and not all(value is not None for value in values):
            raise ValueError("runtime variant id, hash, and materialized config must be set together")
        _reject_forbidden_runtime_values(self.model_dump(mode="json"))
        return self


class LocalSimRuntimeProfileConfigRequestV1(BaseModel):
    """Client-writable profile fields before server authority materialization."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_runtime_profile_config_request_v1"] = (
        "localsim_runtime_profile_config_request_v1"
    )
    daily_strategy: LocalSimDailyStrategyConfigV1
    hmm: LocalSimHmmConfigV1
    risk_policy: dict[str, Any]
    fee_policy: dict[str, Any]
    runtime_variant_id: str | None = None
    runtime_variant_hash: str | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_variant_hash")
    @classmethod
    def _variant_hash_is_sha256(cls, value: str | None) -> str | None:
        if value is not None:
            _require_sha256(value, field="runtime_variant_hash")
        return value

    @model_validator(mode="after")
    def _variant_identity_is_complete(self) -> "LocalSimRuntimeProfileConfigRequestV1":
        if (self.runtime_variant_id is None) != (self.runtime_variant_hash is None):
            raise ValueError("runtime variant id and hash must be set together")
        _reject_forbidden_runtime_values(self.model_dump(mode="json"))
        return self

    def materialize(self, *, runtime_variant_materialized_config: dict[str, Any] | None) -> LocalSimRuntimeProfileConfigV1:
        return LocalSimRuntimeProfileConfigV1(
            daily_strategy=self.daily_strategy,
            hmm=self.hmm,
            risk_policy=self.risk_policy,
            fee_policy=self.fee_policy,
            runtime_variant_id=self.runtime_variant_id,
            runtime_variant_hash=self.runtime_variant_hash,
            runtime_variant_materialized_config=runtime_variant_materialized_config,
            notes=self.notes,
            metadata=self.metadata,
        )


class LocalSimRuntimeProfileV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_runtime_profile_v1"] = LOCALSIM_RUNTIME_PROFILE_SCHEMA
    profile_id: str
    profile_hash: str
    package_id: str
    manifest_sha256: str
    profile_name: str
    status: LocalSimRuntimeProfileStatus = LocalSimRuntimeProfileStatus.ACTIVE
    version: int = Field(default=1, ge=1)
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("profile_id", "profile_hash", "package_id", "manifest_sha256", "profile_name", "created_by")
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("LocalSim runtime profile identity field is required")
        return text

    @model_validator(mode="after")
    def _identity_is_canonical(self) -> "LocalSimRuntimeProfileV1":
        identity = {
            "schema_version": LOCALSIM_RUNTIME_PROFILE_SCHEMA,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "profile_name": self.profile_name,
        }
        expected_hash = canonical_json_sha256(identity)
        if self.profile_hash != expected_hash or self.profile_id != f"lsrprof_{expected_hash[:16]}":
            raise ValueError("LocalSim runtime profile identity is not canonical")
        _require_sha256(self.manifest_sha256, field="manifest_sha256")
        return self


class LocalSimRuntimeProfileVersionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_runtime_profile_version_v1"] = LOCALSIM_RUNTIME_PROFILE_VERSION_SCHEMA
    profile_version_id: str
    profile_version_hash: str
    profile_id: str
    package_id: str
    manifest_sha256: str
    version_no: int = Field(ge=1)
    config_json: dict[str, Any]
    config_sha256: str
    daily_strategy_profile_version_id: str
    validation_status: LocalSimRuntimeProfileValidationStatus
    validation_evidence: dict[str, Any]
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "profile_version_id",
        "profile_version_hash",
        "profile_id",
        "package_id",
        "manifest_sha256",
        "config_sha256",
        "daily_strategy_profile_version_id",
        "created_by",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("LocalSim runtime profile version identity field is required")
        return text

    @model_validator(mode="after")
    def _identity_is_canonical(self) -> "LocalSimRuntimeProfileVersionV1":
        config = LocalSimRuntimeProfileConfigV1.model_validate(self.config_json)
        normalized = config.model_dump(mode="json")
        if normalized != self.config_json or canonical_json_sha256(normalized) != self.config_sha256:
            raise ValueError("LocalSim runtime profile version config hash is not canonical")
        daily_hash = canonical_json_sha256(config.daily_strategy.model_dump(mode="json"))
        expected_daily_id = f"lsdaily_{daily_hash[:16]}"
        if self.daily_strategy_profile_version_id != expected_daily_id:
            raise ValueError("daily strategy profile version identity is not canonical")
        identity = {
            "schema_version": LOCALSIM_RUNTIME_PROFILE_VERSION_SCHEMA,
            "profile_id": self.profile_id,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "config_sha256": self.config_sha256,
        }
        expected_hash = canonical_json_sha256(identity)
        if self.profile_version_hash != expected_hash or self.profile_version_id != f"lsrpv_{expected_hash[:16]}":
            raise ValueError("LocalSim runtime profile version identity is not canonical")
        _require_sha256(self.manifest_sha256, field="manifest_sha256")
        _require_sha256(self.config_sha256, field="config_sha256")
        if self.validation_status is LocalSimRuntimeProfileValidationStatus.VALIDATED and not self.validation_evidence:
            raise ValueError("validated LocalSim runtime profile version requires evidence")
        return self


def build_localsim_runtime_profile(
    *, package_id: str, manifest_sha256: str, profile_name: str, created_by: str, now: datetime
) -> LocalSimRuntimeProfileV1:
    identity = {
        "schema_version": LOCALSIM_RUNTIME_PROFILE_SCHEMA,
        "package_id": str(package_id).strip(),
        "manifest_sha256": str(manifest_sha256).strip().lower(),
        "profile_name": str(profile_name).strip(),
    }
    digest = canonical_json_sha256(identity)
    return LocalSimRuntimeProfileV1(
        profile_id=f"lsrprof_{digest[:16]}",
        profile_hash=digest,
        package_id=identity["package_id"],
        manifest_sha256=identity["manifest_sha256"],
        profile_name=identity["profile_name"],
        created_by=str(created_by).strip(),
        created_at=now,
        updated_at=now,
    )


def _reject_forbidden_runtime_values(payload: dict[str, Any]) -> None:
    forbidden = {
        "alpha_components",
        "alpha_combination_policy",
        "alpha_weights",
        "factor_set",
        "model_asset",
        "model_code",
        "model_weight",
        "manifest_json",
        "manifest_sha256",
        "package_id",
        "broker_account_id",
        "ledger_scope_id",
        "orders",
        "fills",
        "cash",
        "positions",
        "execution_policy",
        "minute_execution_policy",
        "execution_algo",
        "tail_policy",
        "unfilled_handler",
    }

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).strip().lower()
                if normalized in forbidden:
                    raise RuntimeConfigInvalidError(
                        "LocalSim runtime profile contains a forbidden authority field",
                        context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_FORBIDDEN_FIELD", "field": f"{path}.{key}"},
                    )
                walk(child, f"{path}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(payload, "runtime_profile")


def _require_sha256(value: str, *, field: str) -> None:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
