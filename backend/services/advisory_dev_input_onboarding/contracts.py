"""Typed, hash-closed contracts for Advisory real DEV input onboarding."""

from __future__ import annotations

import base64
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
)
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity


REQUEST_SCHEMA_VERSION = "advisory_real_dev_onboarding_request_v1"
INVENTORY_QUERY_SCHEMA_VERSION = "advisory_real_dev_onboarding_inventory_query_v1"
INVENTORY_SCHEMA_VERSION = "advisory_real_dev_onboarding_inventory_v1"
BUNDLE_SCHEMA_VERSION = "advisory_real_dev_portable_bundle_v1"
ROW_SET_SCHEMA_VERSION = "advisory_real_dev_relation_row_set_v1"
CONTRACT_SCHEMA_VERSION = "advisory_real_dev_onboarding_contract_v1"

STORE_POLICY_PAYLOAD = {
    "schema_version": "advisory_real_dev_onboarding_store_policy_v1",
    "layout": {
        "request": "requests/<prefix>/<hash>.json",
        "inventory_query": "inventory-queries/<prefix>/<hash>.json",
        "inventory": "inventories/<prefix>/<hash>.json",
        "bundle": "bundles/<prefix>/<hash>.json",
        "blob": "blobs/<prefix>/<sha256>.blob",
    },
    "canonical_json": True,
    "atomic_no_replace": True,
    "latest_pointer": False,
}
STORE_POLICY_HASH = canonical_json_sha256(STORE_POLICY_PAYLOAD)

REASON_REQUEST_INVALID = "ADVISORY_REAL_DEV_REQUEST_INVALID"
REASON_ENV_INVALID = "ADVISORY_REAL_DEV_ENV_INVALID"
REASON_DATABASE_CONNECTION_FAILED = "ADVISORY_REAL_DEV_DATABASE_CONNECTION_FAILED"
REASON_READONLY_ASSERTION_FAILED = "ADVISORY_REAL_DEV_READONLY_ASSERTION_FAILED"
REASON_PROJECTION_FAILED = "ADVISORY_REAL_DEV_READONLY_PROJECTION_FAILED"
REASON_SOURCE_TARGET_IDENTITY_COLLISION = "ADVISORY_REAL_DEV_SOURCE_TARGET_IDENTITY_COLLISION"
REASON_RELEASE_RECEIPT_INVALID = "ADVISORY_REAL_DEV_RELEASE_RECEIPT_INVALID"
REASON_PACKAGE_MISSING = "ADVISORY_REAL_DEV_PACKAGE_MISSING"
REASON_PACKAGE_MANIFEST_MISMATCH = "ADVISORY_REAL_DEV_PACKAGE_MANIFEST_MISMATCH"
REASON_PACKAGE_ASSET_MISSING = "ADVISORY_REAL_DEV_PACKAGE_ASSET_MISSING"
REASON_SOURCE_PROGRAM_MISSING = "ADVISORY_REAL_DEV_SOURCE_PROGRAM_MISSING"
REASON_SINGLE_TRACK_MISSING = "ADVISORY_REAL_DEV_SINGLE_TRACK_MISSING"
REASON_MULTI_TRACK_MISSING = "ADVISORY_REAL_DEV_MULTI_TRACK_MISSING"
REASON_DSE_V2_MISSING = "ADVISORY_REAL_DEV_DSE_V2_MISSING"
REASON_LEGACY_BINDING_INELIGIBLE = "ADVISORY_REAL_DEV_LEGACY_BINDING_INELIGIBLE"
REASON_TARGET_CONFLICT = "ADVISORY_REAL_DEV_TARGET_CONFLICT"
REASON_EVIDENCE_STORE_FAILED = "ADVISORY_REAL_DEV_EVIDENCE_STORE_FAILED"
REASON_BUNDLE_INVALID = "ADVISORY_REAL_DEV_BUNDLE_INVALID"
REASON_UNEXPECTED_ERROR = "ADVISORY_REAL_DEV_UNEXPECTED_ERROR"


class RealDevOnboardingError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        self.reason_code = reason_code
        self.context = canonicalize(context) if context is not None else None
        super().__init__(message)


class StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def validate_sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{field_name} must be lowercase sha256")
    return normalized


def sorted_unique(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(sorted(str(value).strip() for value in values))
    if any(not value for value in normalized) or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must contain sorted unique non-empty values")
    return normalized


class HashClosedContract(StrictContract):
    hash_field: ClassVar[str]

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={self.hash_field})

    def close_hash(self) -> None:
        supplied = getattr(self, self.hash_field)
        digest = canonical_json_sha256(self.canonical_payload())
        if supplied is not None and supplied != digest:
            raise ValueError(f"{self.hash_field} does not match canonical payload")
        object.__setattr__(self, self.hash_field, digest)


class AlphaMode(str, Enum):
    SINGLE = "single_alpha"
    MULTI = "multi_alpha"


class SourceFactEligibility(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    DSE_V1_INELIGIBLE = "DSE_V1_INELIGIBLE"
    LEGACY_BINDING_INELIGIBLE = "LEGACY_BINDING_INELIGIBLE"
    MISSING = "MISSING"
    INVALID = "INVALID"


class InventoryClassification(str, Enum):
    DUAL_TRACK_AVAILABLE = "DUAL_TRACK_AVAILABLE"
    INPUT_INCOMPLETE = "INPUT_INCOMPLETE"
    TARGET_CONFLICT = "TARGET_CONFLICT"


class PackageClosureStatus(str, Enum):
    O2_EXPORT_VERIFICATION_REQUIRED = "O2_EXPORT_VERIFICATION_REQUIRED"
    INPUT_INCOMPLETE = "INPUT_INCOMPLETE"


class EvidenceKind(str, Enum):
    REQUEST = "request"
    INVENTORY_QUERY = "inventory_query"
    INVENTORY = "inventory"
    BUNDLE = "bundle"


class OnboardingArtifactRef(StrictContract):
    schema_version: Literal[CONTRACT_SCHEMA_VERSION] = CONTRACT_SCHEMA_VERSION
    evidence_kind: EvidenceKind
    store_policy_hash: Literal[STORE_POLICY_HASH] = STORE_POLICY_HASH
    relative_path: str = Field(min_length=1, max_length=800)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("semantic_content_hash", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @field_validator("relative_path")
    @classmethod
    def _relative_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ":" in normalized or ".." in normalized.split("/"):
            raise ValueError("relative_path must be a contained relative path")
        return normalized


class OnboardingBlobRef(StrictContract):
    schema_version: Literal[CONTRACT_SCHEMA_VERSION] = CONTRACT_SCHEMA_VERSION
    store_policy_hash: Literal[STORE_POLICY_HASH] = STORE_POLICY_HASH
    relative_path: str = Field(min_length=1, max_length=800)
    blob_sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)

    @field_validator("blob_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return validate_sha256(value, field_name="blob_sha256")

    @field_validator("relative_path")
    @classmethod
    def _relative_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ":" in normalized or ".." in normalized.split("/"):
            raise ValueError("blob relative_path must be a contained relative path")
        return normalized


class TargetDevProgramSpec(StrictContract):
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    alpha_mode: AlphaMode
    target_count: int = Field(gt=0)
    review_policy: dict[str, Any]
    style: str = Field(min_length=1, max_length=120)


def _validate_release_receipt_ref(ref: Phase1GInputArtifactRef) -> None:
    if ref.artifact_kind is not Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT:
        raise ValueError("release_receipt_ref must address one exact Phase 1F.2 release receipt")
    if ref.store_policy_hash != PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash:
        raise ValueError("release_receipt_ref store policy is not the registered Phase 1F.2 policy")


def _normalize_dual_track_inputs(
    *,
    source_program_refs: tuple[str, ...],
    source_package_ids: tuple[str, ...],
    target_dev_program_specs: tuple[TargetDevProgramSpec, ...],
    required_alpha_modes: tuple[AlphaMode, ...],
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[TargetDevProgramSpec, ...]]:
    packages = sorted_unique(source_package_ids, field_name="source_package_ids")
    programs = tuple(sorted(target_dev_program_specs, key=lambda item: item.program_id))
    if len({item.program_id for item in programs}) != len(programs):
        raise ValueError("target_dev_program_specs program ids must be unique")
    if set(required_alpha_modes) != {AlphaMode.SINGLE, AlphaMode.MULTI}:
        raise ValueError("required_alpha_modes must contain exactly single and native multi")
    if {item.package_id for item in programs} != set(packages):
        raise ValueError("every source package must map to one target DEV Program")
    if {item.alpha_mode for item in programs} != {AlphaMode.SINGLE, AlphaMode.MULTI}:
        raise ValueError("target DEV Programs must include single and native multi tracks")
    refs = sorted_unique(source_program_refs, field_name="source_program_refs") if source_program_refs else ()
    return refs, packages, programs


class RealDevOnboardingInventoryQuery(HashClosedContract):
    """Explicit candidate inventory input; discovers hashes without latest/name inference."""

    hash_field: ClassVar[str] = "inventory_query_hash"
    schema_version: Literal[INVENTORY_QUERY_SCHEMA_VERSION] = INVENTORY_QUERY_SCHEMA_VERSION
    source_target: Literal["PRODUCTION_READ_ONLY"] = "PRODUCTION_READ_ONLY"
    target_target: Literal["DEV"] = "DEV"
    source_program_refs: tuple[str, ...] = ()
    source_package_ids: tuple[str, ...] = Field(min_length=2)
    target_dev_program_specs: tuple[TargetDevProgramSpec, ...] = Field(min_length=2)
    binding_effective_from_trade_date: date
    decision_trade_date: date
    required_alpha_modes: tuple[AlphaMode, ...] = (AlphaMode.SINGLE, AlphaMode.MULTI)
    release_receipt_ref: Phase1GInputArtifactRef
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: Literal[True] = True
    inventory_query_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("inventory_query_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="inventory_query_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevOnboardingInventoryQuery":
        if self.decision_trade_date < self.binding_effective_from_trade_date:
            raise ValueError("decision_trade_date must be inside the new binding interval")
        refs, packages, programs = _normalize_dual_track_inputs(
            source_program_refs=self.source_program_refs,
            source_package_ids=self.source_package_ids,
            target_dev_program_specs=self.target_dev_program_specs,
            required_alpha_modes=self.required_alpha_modes,
        )
        _validate_release_receipt_ref(self.release_receipt_ref)
        object.__setattr__(self, "source_program_refs", refs)
        object.__setattr__(self, "source_package_ids", packages)
        object.__setattr__(self, "target_dev_program_specs", programs)
        object.__setattr__(self, "required_alpha_modes", (AlphaMode.SINGLE, AlphaMode.MULTI))
        self.close_hash()
        return self


class RealDevOnboardingRequest(HashClosedContract):
    hash_field: ClassVar[str] = "request_hash"
    schema_version: Literal[REQUEST_SCHEMA_VERSION] = REQUEST_SCHEMA_VERSION
    source_target: Literal["PRODUCTION_READ_ONLY"] = "PRODUCTION_READ_ONLY"
    target_target: Literal["DEV"] = "DEV"
    source_program_refs: tuple[str, ...] = ()
    source_package_ids: tuple[str, ...] = Field(min_length=2)
    target_dev_program_specs: tuple[TargetDevProgramSpec, ...] = Field(min_length=2)
    binding_effective_from_trade_date: date
    decision_trade_date: date
    expected_program_packages: dict[str, str]
    expected_package_manifest_sha256s: dict[str, str]
    required_alpha_modes: tuple[AlphaMode, ...] = (AlphaMode.SINGLE, AlphaMode.MULTI)
    policy_registry_id: str = Field(min_length=1, max_length=160)
    policy_registry_version: str = Field(min_length=1, max_length=80)
    policy_registry_hash: str = Field(min_length=64, max_length=64)
    release_receipt_ref: Phase1GInputArtifactRef
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: Literal[True] = True
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("policy_registry_hash", "request_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevOnboardingRequest":
        if self.decision_trade_date < self.binding_effective_from_trade_date:
            raise ValueError("decision_trade_date must be inside the new binding interval")
        refs, packages, programs = _normalize_dual_track_inputs(
            source_program_refs=self.source_program_refs,
            source_package_ids=self.source_package_ids,
            target_dev_program_specs=self.target_dev_program_specs,
            required_alpha_modes=self.required_alpha_modes,
        )
        expected_programs = {item.program_id: item.package_id for item in programs}
        if self.expected_program_packages != expected_programs:
            raise ValueError("expected_program_packages must exactly match target program specs")
        if set(self.expected_package_manifest_sha256s) != set(packages):
            raise ValueError("expected manifest map must exactly match source package ids")
        for package_id, digest in self.expected_package_manifest_sha256s.items():
            validate_sha256(digest, field_name=f"expected_package_manifest_sha256s[{package_id}]")
        _validate_release_receipt_ref(self.release_receipt_ref)
        object.__setattr__(self, "source_program_refs", refs)
        object.__setattr__(self, "source_package_ids", packages)
        object.__setattr__(self, "target_dev_program_specs", programs)
        object.__setattr__(self, "required_alpha_modes", (AlphaMode.SINGLE, AlphaMode.MULTI))
        self.close_hash()
        return self


class AlphaComponentEvidence(HashClosedContract):
    hash_field: ClassVar[str] = "component_hash"
    alpha_id: str = Field(min_length=1, max_length=160)
    alpha_name: str = Field(min_length=1, max_length=240)
    component_weight: float = Field(gt=0)
    model_id: str | None = Field(default=None, max_length=160)
    holding_period: str = Field(min_length=1, max_length=80)
    rebalance_frequency: str = Field(min_length=1, max_length=80)
    score_direction: Literal["higher_better", "lower_better"]
    score_normalization: str = Field(min_length=1, max_length=80)
    factor_ids: tuple[str, ...] = Field(min_length=1)
    window_evidence_status: Literal["PROSPECTIVE_DSE_V2_REQUIRED"] = "PROSPECTIVE_DSE_V2_REQUIRED"
    component_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("component_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="component_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "AlphaComponentEvidence":
        object.__setattr__(self, "factor_ids", sorted_unique(self.factor_ids, field_name="factor_ids"))
        self.close_hash()
        return self


class PackageInventoryCandidate(HashClosedContract):
    hash_field: ClassVar[str] = "candidate_hash"
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    package_status: str = Field(min_length=1, max_length=80)
    components: tuple[AlphaComponentEvidence, ...]
    package_asset_count: int = Field(ge=0)
    has_runtime_assets: bool
    has_source_evidence: bool
    closure_status: PackageClosureStatus
    source_program_refs: tuple[str, ...] = ()
    dse_schema_counts: dict[str, int] = Field(default_factory=dict)
    completed_dse_v2_trade_dates: tuple[date, ...] = ()
    binding_fact_eligibility: SourceFactEligibility
    dse_fact_eligibility: SourceFactEligibility
    package_eligible: bool
    reason_codes: tuple[str, ...] = ()
    candidate_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "candidate_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "PackageInventoryCandidate":
        components = tuple(sorted(self.components, key=lambda item: item.alpha_id))
        if len({item.alpha_id for item in components}) != len(components):
            raise ValueError("components must have unique alpha ids")
        if self.package_eligible and self.alpha_mode is AlphaMode.SINGLE and len(components) != 1:
            raise ValueError("eligible single Alpha package must contain exactly one component")
        if self.package_eligible and self.alpha_mode is AlphaMode.MULTI and len(components) < 2:
            raise ValueError("eligible native multi Alpha package must contain at least two components")
        if any(value < 0 for value in self.dse_schema_counts.values()):
            raise ValueError("dse schema counts must be non-negative")
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if self.package_eligible and reasons:
            raise ValueError("eligible package cannot carry failure reasons")
        if not self.package_eligible and not reasons:
            raise ValueError("ineligible package requires reason codes")
        expected_closure_status = (
            PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED
            if self.package_eligible
            else PackageClosureStatus.INPUT_INCOMPLETE
        )
        if self.closure_status is not expected_closure_status:
            raise ValueError("package closure status differs from inventory eligibility")
        object.__setattr__(self, "components", components)
        object.__setattr__(self, "source_program_refs", sorted_unique(self.source_program_refs, field_name="source_program_refs") if self.source_program_refs else ())
        object.__setattr__(self, "completed_dse_v2_trade_dates", tuple(sorted(set(self.completed_dse_v2_trade_dates))))
        object.__setattr__(self, "reason_codes", reasons)
        self.close_hash()
        return self


class RealDevOnboardingInventoryReceipt(HashClosedContract):
    hash_field: ClassVar[str] = "inventory_hash"
    schema_version: Literal[INVENTORY_SCHEMA_VERSION] = INVENTORY_SCHEMA_VERSION
    inventory_invocation_id: str = Field(min_length=1, max_length=160)
    source_database_identity: DatabaseIdentity
    target_database_identity: DatabaseIdentity
    release_receipt_ref: Phase1GInputArtifactRef
    release_catalog_fingerprint: str = Field(min_length=64, max_length=64)
    program_candidates: tuple[PackageInventoryCandidate, ...]
    common_completed_trade_dates: tuple[date, ...] = ()
    selected_input_ref: OnboardingArtifactRef
    selected_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    selected_inventory_query_hash: str | None = Field(default=None, min_length=64, max_length=64)
    relation_row_counts: dict[str, int]
    dependency_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    classification: InventoryClassification
    reason_codes: tuple[str, ...] = ()
    observed_at: datetime
    inventory_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "release_catalog_fingerprint",
        "selected_request_hash",
        "selected_inventory_query_hash",
        "dependency_closure_hash",
        "inventory_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevOnboardingInventoryReceipt":
        candidates = tuple(sorted(self.program_candidates, key=lambda item: item.package_id))
        if len({item.package_id for item in candidates}) != len(candidates):
            raise ValueError("program_candidates package ids must be unique")
        if self.selected_input_ref.evidence_kind is EvidenceKind.REQUEST:
            if self.selected_request_hash != self.selected_input_ref.semantic_content_hash or self.selected_inventory_query_hash is not None:
                raise ValueError("request-driven inventory must close exactly one request ref")
        elif self.selected_input_ref.evidence_kind is EvidenceKind.INVENTORY_QUERY:
            if self.selected_inventory_query_hash != self.selected_input_ref.semantic_content_hash or self.selected_request_hash is not None:
                raise ValueError("candidate inventory must close exactly one inventory query ref")
        else:
            raise ValueError("inventory selected_input_ref must be a request or inventory query")
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if self.classification is InventoryClassification.DUAL_TRACK_AVAILABLE and reasons:
            raise ValueError("available inventory cannot carry failure reasons")
        if self.classification is InventoryClassification.DUAL_TRACK_AVAILABLE and {
            item.alpha_mode for item in candidates if item.package_eligible
        } != {AlphaMode.SINGLE, AlphaMode.MULTI}:
            raise ValueError("available inventory requires eligible single and native multi tracks")
        if self.classification is not InventoryClassification.DUAL_TRACK_AVAILABLE and not reasons:
            raise ValueError("incomplete/conflict inventory requires reason codes")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if any(value < 0 for value in self.relation_row_counts.values()):
            raise ValueError("relation row counts must be non-negative")
        object.__setattr__(self, "program_candidates", candidates)
        object.__setattr__(self, "common_completed_trade_dates", tuple(sorted(set(self.common_completed_trade_dates))))
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(timezone.utc))
        self.close_hash()
        return self


def serialize_postgres_value(value: Any) -> Any:
    """Serialize PostgreSQL values without relying on driver text dumps."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return {"type": "numeric", "value": format(value, "f")}
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("PostgreSQL timestamp must be timezone-aware")
        return {"type": "timestamptz", "value": value.astimezone(timezone.utc).isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, UUID):
        return {"type": "uuid", "value": str(value)}
    if isinstance(value, bytes):
        return {"type": "bytea", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return {"type": "array", "items": [serialize_postgres_value(item) for item in value]}
    if isinstance(value, dict) and value.get("type") in {
        "numeric",
        "float",
        "timestamptz",
        "date",
        "uuid",
        "bytea",
        "array",
        "jsonb",
    }:
        value_type = value["type"]
        if value_type == "bytea" and set(value) == {"type", "base64"}:
            base64.b64decode(str(value["base64"]), validate=True)
            return {"type": "bytea", "base64": str(value["base64"])}
        if value_type == "array" and set(value) == {"type", "items"} and isinstance(value["items"], list):
            return {"type": "array", "items": [serialize_postgres_value(item) for item in value["items"]]}
        if value_type == "jsonb" and set(value) == {"type", "value"} and isinstance(value["value"], dict):
            return {
                "type": "jsonb",
                "value": {
                    str(key): serialize_postgres_value(item)
                    for key, item in sorted(value["value"].items(), key=lambda pair: str(pair[0]))
                },
            }
        if value_type in {"numeric", "float", "timestamptz", "date", "uuid"} and set(value) == {"type", "value"}:
            text = str(value["value"])
            if value_type == "numeric":
                Decimal(text)
            elif value_type == "float":
                float.fromhex(text)
            elif value_type == "timestamptz":
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError("typed timestamptz value must be timezone-aware")
            elif value_type == "date":
                date.fromisoformat(text)
            elif value_type == "uuid":
                UUID(text)
            return {"type": value_type, "value": text}
        raise ValueError("invalid typed PostgreSQL value envelope")
    if isinstance(value, dict):
        return {
            "type": "jsonb",
            "value": {str(key): serialize_postgres_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))},
        }
    raise ValueError(f"unsupported PostgreSQL value type: {type(value).__name__}")


class PortableRelationRowSet(HashClosedContract):
    hash_field: ClassVar[str] = "row_set_hash"
    schema_version: Literal[ROW_SET_SCHEMA_VERSION] = ROW_SET_SCHEMA_VERSION
    relation_name: Literal["strategy_pkg.package", "strategy_pkg.package_asset"]
    primary_or_natural_key_fields: tuple[str, ...] = Field(min_length=1)
    semantic_column_names: tuple[str, ...] = Field(min_length=1)
    source_provenance_column_names: tuple[str, ...] = ()
    column_contract_hash: str | None = Field(default=None, min_length=64, max_length=64)
    sorted_rows: tuple[dict[str, Any], ...]
    row_content_hashes: tuple[str, ...] = ()
    row_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("column_contract_hash", "row_set_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "relation_name": self.relation_name,
            "primary_or_natural_key_fields": self.primary_or_natural_key_fields,
            "semantic_column_names": self.semantic_column_names,
            "source_provenance_column_names": self.source_provenance_column_names,
            "column_contract_hash": self.column_contract_hash,
            "row_content_hashes": self.row_content_hashes,
        }

    @model_validator(mode="after")
    def _close(self) -> "PortableRelationRowSet":
        keys = sorted_unique(self.primary_or_natural_key_fields, field_name="primary_or_natural_key_fields")
        semantic = sorted_unique(self.semantic_column_names, field_name="semantic_column_names")
        provenance = sorted_unique(self.source_provenance_column_names, field_name="source_provenance_column_names") if self.source_provenance_column_names else ()
        if set(semantic) & set(provenance):
            raise ValueError("semantic and provenance columns must be disjoint")
        if not set(keys).issubset(semantic):
            raise ValueError("natural key fields must be semantic columns")
        if self.relation_name == "strategy_pkg.package_asset" and "asset_id" not in provenance:
            raise ValueError("package_asset.asset_id must be provenance-only")
        expected_columns = set(semantic) | set(provenance)
        rows: list[dict[str, Any]] = []
        semantic_hashes: list[str] = []
        sort_pairs: list[tuple[str, dict[str, Any], str]] = []
        key_hashes: set[str] = set()
        for raw in self.sorted_rows:
            if set(raw) != expected_columns:
                raise ValueError("row columns differ from the frozen column contract")
            serialized = {name: serialize_postgres_value(raw[name]) for name in sorted(raw)}
            semantic_payload = {name: serialized[name] for name in semantic}
            row_hash = canonical_json_sha256(semantic_payload)
            key_hash = canonical_json_sha256({name: serialized[name] for name in keys})
            if key_hash in key_hashes:
                raise ValueError("relation row set contains a duplicate natural key")
            key_hashes.add(key_hash)
            sort_pairs.append((key_hash, serialized, row_hash))
        sort_pairs.sort(key=lambda item: (item[0], item[2]))
        rows = [item[1] for item in sort_pairs]
        semantic_hashes = [item[2] for item in sort_pairs]
        column_hash = canonical_json_sha256({
            "relation_name": self.relation_name,
            "primary_or_natural_key_fields": keys,
            "semantic_column_names": semantic,
            "source_provenance_column_names": provenance,
        })
        if self.column_contract_hash is not None and self.column_contract_hash != column_hash:
            raise ValueError("column_contract_hash does not match the frozen column contract")
        supplied_hashes = tuple(self.row_content_hashes)
        if supplied_hashes and supplied_hashes != tuple(semantic_hashes):
            raise ValueError("row_content_hashes do not match semantic rows")
        object.__setattr__(self, "primary_or_natural_key_fields", keys)
        object.__setattr__(self, "semantic_column_names", semantic)
        object.__setattr__(self, "source_provenance_column_names", provenance)
        object.__setattr__(self, "column_contract_hash", column_hash)
        object.__setattr__(self, "sorted_rows", tuple(rows))
        object.__setattr__(self, "row_content_hashes", tuple(semantic_hashes))
        self.close_hash()
        return self


class BundlePackageRef(StrictContract):
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode

    @field_validator("manifest_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return validate_sha256(value, field_name="manifest_sha256")


class NativeMultiComponentRef(HashClosedContract):
    hash_field: ClassVar[str] = "component_ref_hash"
    parent_package_id: str = Field(min_length=1, max_length=160)
    component: AlphaComponentEvidence
    component_ref_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("component_ref_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="component_ref_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "NativeMultiComponentRef":
        self.close_hash()
        return self


class BundleBlobRef(StrictContract):
    package_id: str = Field(min_length=1, max_length=160)
    asset_type: str = Field(min_length=1, max_length=120)
    asset_ref: str = Field(min_length=1, max_length=800)
    blob_ref: OnboardingBlobRef

    @property
    def asset_sha256(self) -> str:
        return self.blob_ref.blob_sha256

    @property
    def size_bytes(self) -> int:
        return self.blob_ref.size_bytes

    @field_validator("asset_ref")
    @classmethod
    def _asset_ref(cls, value: str) -> str:
        if value.startswith(("/", "\\")) or ":" in value or ".." in value.replace("\\", "/").split("/"):
            raise ValueError("asset_ref must not contain an absolute or escaping path")
        return value.replace("\\", "/")


class DependencyEdge(StrictContract):
    parent_identity: str = Field(min_length=1, max_length=800)
    child_identity: str = Field(min_length=1, max_length=800)
    relation: Literal["PACKAGE_COMPONENT", "PACKAGE_ASSET", "ASSET_BLOB"]


class PortableAdvisoryEvidenceBundle(HashClosedContract):
    hash_field: ClassVar[str] = "bundle_content_hash"
    schema_version: Literal[BUNDLE_SCHEMA_VERSION] = BUNDLE_SCHEMA_VERSION
    request: RealDevOnboardingRequest
    source_database_identity_hash: str = Field(min_length=64, max_length=64)
    export_snapshot_identity: str = Field(min_length=1, max_length=240)
    package_refs: tuple[BundlePackageRef, ...]
    native_multi_component_refs: tuple[NativeMultiComponentRef, ...]
    relation_row_sets: tuple[PortableRelationRowSet, ...]
    artifact_blob_refs: tuple[BundleBlobRef, ...]
    dependency_edges: tuple[DependencyEdge, ...]
    dependency_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    bundle_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("source_database_identity_hash", "dependency_closure_hash", "bundle_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "PortableAdvisoryEvidenceBundle":
        packages = tuple(sorted(self.package_refs, key=lambda item: item.package_id))
        if len({item.package_id for item in packages}) != len(packages):
            raise ValueError("package_refs package ids must be unique")
        if {item.package_id for item in packages} != set(self.request.source_package_ids):
            raise ValueError("bundle package refs must exactly close the request")
        if {item.alpha_mode for item in packages} != {AlphaMode.SINGLE, AlphaMode.MULTI}:
            raise ValueError("bundle must contain both single and native multi packages")
        components = tuple(
            sorted(
                self.native_multi_component_refs,
                key=lambda item: (item.parent_package_id, item.component.alpha_id),
            )
        )
        if len({(item.parent_package_id, item.component.alpha_id) for item in components}) != len(components):
            raise ValueError("native multi component refs must be unique")
        multi_package_ids = {item.package_id for item in packages if item.alpha_mode is AlphaMode.MULTI}
        grouped_components = {
            package_id: [item for item in components if item.parent_package_id == package_id]
            for package_id in multi_package_ids
        }
        if set(item.parent_package_id for item in components) != multi_package_ids or any(
            len(values) < 2 for values in grouped_components.values()
        ):
            raise ValueError("every native multi parent requires its complete component closure")
        row_sets = tuple(sorted(self.relation_row_sets, key=lambda item: item.relation_name))
        if {item.relation_name for item in row_sets} != {"strategy_pkg.package", "strategy_pkg.package_asset"}:
            raise ValueError("bundle relation set must equal the fixed package import allowlist")
        blobs = tuple(sorted(self.artifact_blob_refs, key=lambda item: (item.package_id, item.asset_type, item.asset_ref)))
        package_row_set = next(item for item in row_sets if item.relation_name == "strategy_pkg.package")
        if not {"package_id", "manifest_sha256"}.issubset(package_row_set.semantic_column_names):
            raise ValueError("package row set lacks its identity columns")
        package_row_identities = {
            (str(row["package_id"]), str(row["manifest_sha256"])) for row in package_row_set.sorted_rows
        }
        expected_package_identities = {(item.package_id, item.manifest_sha256) for item in packages}
        if package_row_identities != expected_package_identities:
            raise ValueError("package row set differs from package refs")
        asset_row_set = next(item for item in row_sets if item.relation_name == "strategy_pkg.package_asset")
        if not {"package_id", "asset_type", "asset_ref", "asset_sha256"}.issubset(asset_row_set.semantic_column_names):
            raise ValueError("package asset row set lacks its identity columns")
        asset_rows = {
            (str(row["package_id"]), str(row["asset_type"]), str(row["asset_ref"])): str(row["asset_sha256"])
            for row in asset_row_set.sorted_rows
        }
        if any(package_id not in {item.package_id for item in packages} for package_id, _, _ in asset_rows):
            raise ValueError("package asset row set contains an unrelated package")
        for blob in blobs:
            key = (blob.package_id, blob.asset_type, blob.asset_ref)
            if asset_rows.get(key) != blob.asset_sha256:
                raise ValueError("artifact blob ref differs from its package asset row")
        edges = tuple(sorted(self.dependency_edges, key=lambda item: (item.parent_identity, item.relation, item.child_identity)))
        if len(set((item.parent_identity, item.relation, item.child_identity) for item in edges)) != len(edges):
            raise ValueError("dependency edges must be unique")
        expected_edges = {
            (
                item.parent_package_id,
                "PACKAGE_COMPONENT",
                f"alpha_component:{item.parent_package_id}:{item.component.alpha_id}",
            )
            for item in components
        }
        for (package_id, asset_type, asset_ref), _ in asset_rows.items():
            asset_identity = f"package_asset:{package_id}:{asset_type}:{asset_ref}"
            expected_edges.add((package_id, "PACKAGE_ASSET", asset_identity))
        for item in blobs:
            asset_identity = f"package_asset:{item.package_id}:{item.asset_type}:{item.asset_ref}"
            expected_edges.add((asset_identity, "ASSET_BLOB", f"sha256:{item.asset_sha256}"))
        actual_edges = {(item.parent_identity, item.relation, item.child_identity) for item in edges}
        if actual_edges != expected_edges:
            raise ValueError("dependency graph must exactly match package/component/blob refs")
        closure_hash = canonical_json_sha256({
            "packages": [item.model_dump(mode="json") for item in packages],
            "components": [item.model_dump(mode="json") for item in components],
            "row_sets": [item.row_set_hash for item in row_sets],
            "blobs": [item.model_dump(mode="json") for item in blobs],
            "edges": [item.model_dump(mode="json") for item in edges],
        })
        if self.dependency_closure_hash is not None and self.dependency_closure_hash != closure_hash:
            raise ValueError("dependency_closure_hash does not match bundle children")
        object.__setattr__(self, "package_refs", packages)
        object.__setattr__(self, "native_multi_component_refs", components)
        object.__setattr__(self, "relation_row_sets", row_sets)
        object.__setattr__(self, "artifact_blob_refs", blobs)
        object.__setattr__(self, "dependency_edges", edges)
        object.__setattr__(self, "dependency_closure_hash", closure_hash)
        self.close_hash()
        return self


StoredOnboardingModel = (
    RealDevOnboardingRequest
    | RealDevOnboardingInventoryQuery
    | RealDevOnboardingInventoryReceipt
    | PortableAdvisoryEvidenceBundle
)


def database_identity_hash(identity: DatabaseIdentity) -> str:
    return canonical_json_sha256(identity.canonical_payload())
