"""Typed, hash-closed contracts for Advisory real DEV input onboarding."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar, Literal
from urllib.parse import urlparse
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
)
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel


REQUEST_SCHEMA_VERSION = "advisory_real_dev_onboarding_request_v1"
INVENTORY_QUERY_SCHEMA_VERSION = "advisory_real_dev_onboarding_inventory_query_v1"
INVENTORY_SCHEMA_VERSION = "advisory_real_dev_onboarding_inventory_v1"
BUNDLE_SCHEMA_VERSION = "advisory_real_dev_portable_bundle_v2"
ROW_SET_SCHEMA_VERSION = "advisory_real_dev_relation_row_set_v1"
CONTRACT_SCHEMA_VERSION = "advisory_real_dev_onboarding_contract_v1"
IMPORT_PLAN_SCHEMA_VERSION = "advisory_real_dev_import_plan_v1"
IMPORT_RECEIPT_SCHEMA_VERSION = "advisory_real_dev_import_receipt_v2"
HISTORICAL_RUN_REQUEST_SCHEMA_VERSION = "advisory_real_dev_historical_run_request_v1"
HISTORICAL_RUN_RECEIPT_SCHEMA_VERSION = "advisory_real_dev_historical_run_receipt_v1"
PHASE1E_REAL_INPUT_BUILD_REQUEST_SCHEMA_VERSION = "advisory_phase1e_real_input_build_request_v1"
SOURCE_MAPPING_REGISTRY_SCHEMA_VERSION = "advisory_source_mapping_registry_v1"
SOURCE_OBSERVATION_SCOPE_REQUEST_SCHEMA_VERSION = "advisory_source_observation_scope_request_v1"
SOURCE_REQUIREMENT_REGISTRY_SCHEMA_VERSION = "advisory_source_requirement_registry_v1"
PHASE1E_PROGRAM_INPUT_SCHEMA_VERSION = "advisory_phase1e_program_input_v1"
PHASE1E_REAL_INPUT_BUNDLE_SCHEMA_VERSION = "advisory_phase1e_real_input_bundle_v1"
O4_ARTIFACT_REF_SCHEMA_VERSION = "advisory_phase1e_input_artifact_ref_v1"
STRATEGY_PACKAGE_INPUT_PROJECTION_SCHEMA_VERSION = "strategy_package_advisory_input_projection_v1"
STRATEGY_PACKAGE_INPUT_PROJECTION_SOURCE = "ADMITTED_MANIFEST_ONLY"
STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_ID = "strategy_package_live_inference_inputs"
STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_VERSION = "v2"
STRATEGY_PACKAGE_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD = {
    "contract_id": STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_ID,
    "contract_version": STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_VERSION,
    "logical_inputs": [
        {
            "source_role": "pit_universe",
            "dataset_id": "market.stock_universe_pit",
            "query_template_id": "StockUniversePitService.get_eligible_codes",
            "query_template_version": "v1",
            "fixed_parameters": {
                "universe_key": STRATEGY_PACKAGE_PIT_UNIVERSE_KEY,
                "ensure": True,
            },
        },
        {
            "source_role": "market_history",
            "dataset_id": "market.kline_daily_raw",
            "query_template_id": "get_history_window",
            "query_template_version": "v1",
        },
        {
            "source_role": "fundamental_moneyflow",
            "dataset_id": "timescaledb.fundamental_moneyflow",
            "query_template_id": "timescaledb_adapter.fetch_fundamental_data_ts",
            "query_template_version": "v1",
        },
        {
            "source_role": "trading_calendar",
            "dataset_id": "market.trading_calendar",
            "query_template_id": "InferenceEngine.trade_date_and_window_resolution",
            "query_template_version": "v1",
        },
        {
            "source_role": "reference_price",
            "dataset_id": "market.kline_daily_raw",
            "query_template_id": "SelectionArtifact.reference_price",
            "query_template_version": "v1",
        },
    ],
}
STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_HASH = canonical_json_sha256(
    STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD
)
ALLOWED_EXPORT_PACKAGE_STATUSES = frozenset(
    {
        "DRAFT",
        "ASSET_VALIDATED",
        "BACKTEST_APPROVED",
        "SELECTION_ENABLED",
        "PAPER_ENABLED",
        "PAPER_RUNNING",
        "PAPER_PASSED",
        "PAPER_FAILED",
    }
)

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

O4_ARTIFACT_STORE_POLICY_PAYLOAD = {
    "schema_version": "advisory_phase1e_input_artifact_store_policy_v1",
    "namespace": "advisory/phase1e/inputs",
    "layout": "<kind>/<semantic_hash_prefix>/<semantic_hash>.json",
    "artifact_kinds": [
        "capacity_program_coverage",
        "capacity_program_workload",
        "capacity_policy",
        "capacity_receipt",
        "capacity_request",
        "input_bundle",
        "phase1e_batch_request",
        "phase1e_program_date_request",
        "program_input",
        "real_input_build_request",
        "source_mapping_registry",
        "source_observation_scope_request",
        "source_requirement_set",
        "source_requirement_registry",
        "strategy_package_input_projection",
    ],
    "canonical_json": True,
    "atomic_no_replace": True,
    "latest_pointer": False,
}
O4_ARTIFACT_STORE_POLICY_HASH = canonical_json_sha256(O4_ARTIFACT_STORE_POLICY_PAYLOAD)

PORTABLE_MANIFEST_PROJECTION_POLICY = {
    "schema_version": "advisory_real_dev_manifest_projection_v1",
    "removed_manifest_paths": [
        ["backtest_context"],
        ["source_evidence", "custom_params", "execution_algo_params", "early_model_path"],
        ["source_evidence", "custom_params", "execution_algo_params", "late_model_path"],
        ["source_evidence", "multi_alpha", "combined_prediction_ref_uri"],
    ],
    "manifest_replacements": [
        {
            "path": ["source", "source_type"],
            "value": "candidate_strategy_package",
        }
    ],
    "preserved_identities": ["package_id", "alpha_mode", "alpha_components"],
    "runtime_asset_closure": "exact",
    "absolute_workstation_paths": "prohibited",
}
PORTABLE_MANIFEST_PROJECTION_POLICY_HASH = canonical_json_sha256(PORTABLE_MANIFEST_PROJECTION_POLICY)

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
REASON_BUNDLE_EXPORT_FAILED = "ADVISORY_REAL_DEV_BUNDLE_EXPORT_FAILED"
REASON_IMPORT_PLAN_CONFLICT = "ADVISORY_REAL_DEV_IMPORT_PLAN_CONFLICT"
REASON_IMPORT_PLAN_INVALID = "ADVISORY_REAL_DEV_IMPORT_PLAN_INVALID"
REASON_IMPORT_TRANSACTION_FAILED = "ADVISORY_REAL_DEV_IMPORT_TRANSACTION_FAILED"
REASON_IMPORT_READBACK_FAILED = "ADVISORY_REAL_DEV_IMPORT_READBACK_FAILED"
REASON_IMPORT_COMMIT_NOT_OBSERVED = "ADVISORY_REAL_DEV_IMPORT_COMMIT_NOT_OBSERVED"
REASON_IMPORT_COMMIT_STATE_UNKNOWN = "ADVISORY_REAL_DEV_IMPORT_COMMIT_STATE_UNKNOWN"
REASON_HISTORICAL_INPUT_PENDING = "ADVISORY_DEV_ONBOARDING_INPUT_PENDING"
REASON_PROGRAM_BINDING_INVALID = "ADVISORY_DEV_ONBOARDING_PROGRAM_BINDING_INVALID"
REASON_DSE_INVALID = "ADVISORY_DEV_ONBOARDING_DSE_INVALID"
REASON_HISTORICAL_RUN_FAILED = "ADVISORY_DEV_ONBOARDING_HISTORICAL_RUN_FAILED"
REASON_SOURCE_MAPPING_MISSING = "ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_MISSING"
REASON_SOURCE_MAPPING_CONFLICT = "ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_CONFLICT"
REASON_PHASE1E_INPUT_INVALID = "ADVISORY_DEV_ONBOARDING_PHASE1E_INPUT_INVALID"
REASON_UNEXPECTED_ERROR = "ADVISORY_REAL_DEV_UNEXPECTED_ERROR"
IMPORT_RELATION_SET = frozenset({"strategy_pkg.package", "strategy_pkg.package_asset"})


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


class ImportRowDisposition(str, Enum):
    INSERT = "INSERT"
    EXACT_MATCH = "EXACT_MATCH"
    CONFLICT = "CONFLICT"


class ImportPlanStatus(str, Enum):
    EXECUTABLE = "EXECUTABLE"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    CONFLICT = "CONFLICT"


class ImportCommitOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    STATE_UNKNOWN = "STATE_UNKNOWN"


class HistoricalProgramStatus(str, Enum):
    COMPLETE = "COMPLETE"
    WAITING_INPUT = "WAITING_INPUT"
    FAILED = "FAILED"


class O4ArtifactKind(str, Enum):
    REAL_INPUT_BUILD_REQUEST = "real_input_build_request"
    STRATEGY_PACKAGE_INPUT_PROJECTION = "strategy_package_input_projection"
    SOURCE_MAPPING_REGISTRY = "source_mapping_registry"
    SOURCE_OBSERVATION_SCOPE_REQUEST = "source_observation_scope_request"
    SOURCE_REQUIREMENT_REGISTRY = "source_requirement_registry"
    SOURCE_REQUIREMENT_SET = "source_requirement_set"
    CAPACITY_POLICY = "capacity_policy"
    CAPACITY_REQUEST = "capacity_request"
    CAPACITY_PROGRAM_WORKLOAD = "capacity_program_workload"
    CAPACITY_RECEIPT = "capacity_receipt"
    CAPACITY_PROGRAM_COVERAGE = "capacity_program_coverage"
    PROGRAM_INPUT = "program_input"
    INPUT_BUNDLE = "input_bundle"
    PHASE1E_PROGRAM_DATE_REQUEST = "phase1e_program_date_request"
    PHASE1E_BATCH_REQUEST = "phase1e_batch_request"


class AdvisoryStrategyPackageInputLegV1(StrictContract):
    alpha_component_id: str = Field(min_length=1, max_length=160)
    factor_order: tuple[str, ...] = Field(min_length=1)
    factor_order_hash: str = Field(min_length=64, max_length=64)
    required_window: int = Field(ge=1)
    window_resolution: Literal["trading_day"] = "trading_day"
    alpha158_alias_set_hash: str = Field(min_length=64, max_length=64)
    dynamic_factor_ref_set_hash: str = Field(min_length=64, max_length=64)

    @field_validator("factor_order_hash", "alpha158_alias_set_hash", "dynamic_factor_ref_set_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _coherent(self) -> "AdvisoryStrategyPackageInputLegV1":
        if len(self.factor_order) != len(set(self.factor_order)):
            raise ValueError("factor_order must not contain duplicate factors")
        if self.factor_order_hash != canonical_json_sha256(list(self.factor_order)):
            raise ValueError("factor_order_hash does not match factor_order")
        return self


class AdvisoryStrategyPackageInputProjectionV1(HashClosedContract):
    hash_field: ClassVar[str] = "projection_hash"
    schema_version: Literal[STRATEGY_PACKAGE_INPUT_PROJECTION_SCHEMA_VERSION] = (
        STRATEGY_PACKAGE_INPUT_PROJECTION_SCHEMA_VERSION
    )
    projection_source: Literal[STRATEGY_PACKAGE_INPUT_PROJECTION_SOURCE] = (
        STRATEGY_PACKAGE_INPUT_PROJECTION_SOURCE
    )
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    selection_query_contract_id: Literal[STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_ID] = (
        STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_ID
    )
    selection_query_contract_version: Literal[STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_VERSION] = (
        STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_VERSION
    )
    selection_query_contract_hash: Literal[STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_HASH] = (
        STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_HASH
    )
    legs: tuple[AdvisoryStrategyPackageInputLegV1, ...] = Field(min_length=1)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "projection_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "AdvisoryStrategyPackageInputProjectionV1":
        component_ids = tuple(item.alpha_component_id for item in self.legs)
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("projection must contain one leg per alpha component")
        if self.alpha_mode is AlphaMode.SINGLE and len(self.legs) != 1:
            raise ValueError("single-alpha input projection requires exactly one leg")
        if self.alpha_mode is AlphaMode.MULTI and len(self.legs) < 2:
            raise ValueError("multi-alpha input projection requires at least two legs")
        self.close_hash()
        return self


class PartitionGranularity(str, Enum):
    DAILY = "DAILY"
    AS_OF_SNAPSHOT = "AS_OF_SNAPSHOT"


class ProgramIdentityReadiness(str, Enum):
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"


class ProgramSourceReadiness(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    PENDING = "PENDING"
    READY = "READY"
    BLOCKED = "BLOCKED"


class ProgramCapacityStatus(str, Enum):
    NOT_MEASURED = "NOT_MEASURED"
    PARTIAL = "PARTIAL"
    MEASURED = "MEASURED"
    INSUFFICIENT = "INSUFFICIENT"


class ProgramPlanReadiness(str, Enum):
    IDENTITY_PENDING = "IDENTITY_PENDING"
    IDENTITY_COMPLETE_SOURCE_PENDING = "IDENTITY_COMPLETE_SOURCE_PENDING"
    SOURCE_READY_CAPACITY_PARTIAL = "SOURCE_READY_CAPACITY_PARTIAL"
    FULL_READY = "FULL_READY"
    BLOCKED = "BLOCKED"


class AggregateInputReadiness(str, Enum):
    ALL_FULL_READY = "ALL_FULL_READY"
    MIXED = "MIXED"
    ALL_PENDING = "ALL_PENDING"
    BLOCKED = "BLOCKED"


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


def deserialize_postgres_value(value: Any) -> Any:
    """Invert the portable typed PostgreSQL envelope without text parsing SQL values."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if not isinstance(value, dict) or "type" not in value:
        raise ValueError("portable PostgreSQL value must use a typed envelope")
    value_type = value.get("type")
    if value_type == "numeric" and set(value) == {"type", "value"}:
        return Decimal(str(value["value"]))
    if value_type == "float" and set(value) == {"type", "value"}:
        return float.fromhex(str(value["value"]))
    if value_type == "timestamptz" and set(value) == {"type", "value"}:
        parsed = datetime.fromisoformat(str(value["value"]))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("portable timestamptz must be timezone-aware")
        return parsed
    if value_type == "date" and set(value) == {"type", "value"}:
        return date.fromisoformat(str(value["value"]))
    if value_type == "uuid" and set(value) == {"type", "value"}:
        return UUID(str(value["value"]))
    if value_type == "bytea" and set(value) == {"type", "base64"}:
        return base64.b64decode(str(value["base64"]), validate=True)
    if value_type == "array" and set(value) == {"type", "items"} and isinstance(value["items"], list):
        return [deserialize_postgres_value(item) for item in value["items"]]
    if value_type == "jsonb" and set(value) == {"type", "value"} and isinstance(value["value"], dict):
        return {str(key): deserialize_postgres_value(item) for key, item in value["value"].items()}
    raise ValueError("portable PostgreSQL typed envelope is invalid")


def compute_portable_manifest_json_sha256(manifest: Mapping[str, Any]) -> str:
    """Recompute the persisted StrategyPackage manifest hash without runtime imports."""

    payload = deepcopy(dict(manifest))
    payload["manifest_sha256"] = None
    payload["package_status"] = None
    payload = _drop_empty_manifest_asset_fields(payload)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def portable_manifest_runtime_asset_refs(manifest: Mapping[str, Any]) -> dict[str, str]:
    refs: dict[str, str] = {}

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            asset_ref = node.get("asset_ref")
            sha256 = node.get("sha256")
            if asset_ref is not None or sha256 is not None:
                if not isinstance(asset_ref, str) or not isinstance(sha256, str):
                    raise ValueError("manifest runtime asset ref is not hash-closed")
                digest = validate_sha256(sha256, field_name="manifest_asset_sha256")
                existing = refs.setdefault(asset_ref, digest)
                if existing != digest:
                    raise ValueError("manifest reuses one runtime asset ref with different hashes")
            for child in node.values():
                visit(child)
        elif isinstance(node, (list, tuple)):
            for child in node:
                visit(child)

    roots: list[Any] = []
    for key in ("factor_set", "model_asset", "runtime_assets"):
        value = manifest.get(key)
        if value not in (None, {}, []):
            roots.append(value)
    source_evidence = manifest.get("source_evidence")
    multi_alpha = source_evidence.get("multi_alpha") if isinstance(source_evidence, Mapping) else None
    legs = multi_alpha.get("legs") if isinstance(multi_alpha, Mapping) else None
    if isinstance(legs, list):
        for leg in legs:
            if not isinstance(leg, Mapping):
                continue
            for key in ("runtime_assets", "seed_runtime_assets"):
                value = leg.get(key)
                if isinstance(value, Mapping):
                    roots.append(value)
    for root in roots:
        visit(root)
    return dict(sorted(refs.items()))


def _drop_empty_manifest_asset_fields(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if key == "factor_set" and isinstance(item, list):
            cleaned[key] = [_drop_empty_manifest_asset_defaults(asset) for asset in item]
        elif key == "model_asset" and isinstance(item, list):
            cleaned[key] = [_drop_empty_manifest_asset_defaults(asset) for asset in item]
        elif key == "model_asset" and isinstance(item, dict):
            cleaned[key] = _drop_empty_manifest_asset_defaults(item)
        elif key == "runtime_assets" and item in (None, {}, []):
            continue
        else:
            cleaned[key] = item
    return cleaned


def _drop_empty_manifest_asset_defaults(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    empty_asset_keys = {"asset_ref", "sha256", "size_bytes", "source_uri"}
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if key in empty_asset_keys and item in (None, "", [], {}):
            continue
        if key == "model_code_assets" and item in (None, [], {}):
            continue
        if key == "model_code_required" and item is False:
            continue
        cleaned[key] = item
    return cleaned


class PortableManifestProjectionEvidence(HashClosedContract):
    hash_field: ClassVar[str] = "projection_hash"
    package_id: str = Field(min_length=1, max_length=160)
    source_manifest_sha256: str = Field(min_length=64, max_length=64)
    portable_manifest_sha256: str = Field(min_length=64, max_length=64)
    projection_policy_hash: Literal[PORTABLE_MANIFEST_PROJECTION_POLICY_HASH] = (
        PORTABLE_MANIFEST_PROJECTION_POLICY_HASH
    )
    removed_source_provenance_hash: str = Field(min_length=64, max_length=64)
    runtime_asset_closure_hash: str = Field(min_length=64, max_length=64)
    alpha_component_closure_hash: str = Field(min_length=64, max_length=64)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "source_manifest_sha256",
        "portable_manifest_sha256",
        "removed_source_provenance_hash",
        "runtime_asset_closure_hash",
        "alpha_component_closure_hash",
        "projection_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "PortableManifestProjectionEvidence":
        self.close_hash()
        return self


def project_portable_manifest(
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], PortableManifestProjectionEvidence]:
    """Remove non-runtime backtest provenance while preserving runtime identity."""

    source = deepcopy(dict(manifest))
    package_id = str(source.get("package_id") or "")
    source_manifest_sha256 = validate_sha256(
        str(source.get("manifest_sha256") or ""),
        field_name="source_manifest_sha256",
    )
    if compute_portable_manifest_json_sha256(source) != source_manifest_sha256:
        raise ValueError("source manifest hash is inconsistent before portable projection")
    source_runtime_refs = portable_manifest_runtime_asset_refs(source)
    source_alpha_mode = deepcopy(source.get("alpha_mode"))
    source_components = deepcopy(source.get("alpha_components"))
    removed: dict[str, Any] = {}
    for raw_path in PORTABLE_MANIFEST_PROJECTION_POLICY["removed_manifest_paths"]:
        path = tuple(str(part) for part in raw_path)
        found, value = _pop_manifest_path(source, path)
        if found:
            removed[".".join(path)] = value
    for replacement in PORTABLE_MANIFEST_PROJECTION_POLICY["manifest_replacements"]:
        path = tuple(str(part) for part in replacement["path"])
        found, previous = _replace_manifest_path(source, path, replacement["value"])
        if found and previous != replacement["value"]:
            removed[f"replaced:{'.'.join(path)}"] = previous
    source["manifest_sha256"] = None
    portable_manifest_sha256 = compute_portable_manifest_json_sha256(source)
    source["manifest_sha256"] = portable_manifest_sha256
    if str(source.get("package_id") or "") != package_id:
        raise ValueError("portable projection changed package_id")
    if source.get("alpha_mode") != source_alpha_mode:
        raise ValueError("portable projection changed alpha_mode")
    if source.get("alpha_components") != source_components:
        raise ValueError("portable projection changed alpha_components")
    portable_runtime_refs = portable_manifest_runtime_asset_refs(source)
    if portable_runtime_refs != source_runtime_refs:
        raise ValueError("portable projection changed runtime asset closure")
    evidence = PortableManifestProjectionEvidence(
        package_id=package_id,
        source_manifest_sha256=source_manifest_sha256,
        portable_manifest_sha256=portable_manifest_sha256,
        removed_source_provenance_hash=canonical_json_sha256(removed),
        runtime_asset_closure_hash=canonical_json_sha256(source_runtime_refs),
        alpha_component_closure_hash=canonical_json_sha256(source_components),
    )
    return source, evidence


def _pop_manifest_path(document: dict[str, Any], path: tuple[str, ...]) -> tuple[bool, Any]:
    current: Any = document
    for part in path[:-1]:
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    if not isinstance(current, dict) or path[-1] not in current:
        return False, None
    return True, current.pop(path[-1])


def _replace_manifest_path(
    document: dict[str, Any],
    path: tuple[str, ...],
    replacement: Any,
) -> tuple[bool, Any]:
    current: Any = document
    for part in path[:-1]:
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    if not isinstance(current, dict) or path[-1] not in current:
        return False, None
    previous = current[path[-1]]
    current[path[-1]] = replacement
    return True, previous


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
    source_manifest_sha256: str = Field(min_length=64, max_length=64)
    removed_source_row_provenance_hash: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    projection: PortableManifestProjectionEvidence

    @field_validator("manifest_sha256", "source_manifest_sha256", "removed_source_row_provenance_hash")
    @classmethod
    def _hash(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _projection_identity(self) -> "BundlePackageRef":
        if (
            self.projection.package_id != self.package_id
            or self.projection.source_manifest_sha256 != self.source_manifest_sha256
            or self.projection.portable_manifest_sha256 != self.manifest_sha256
        ):
            raise ValueError("bundle package projection identity is inconsistent")
        return self


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
        normalized = value.replace("\\", "/")
        parsed = urlparse(normalized)
        if parsed.scheme:
            if parsed.scheme != "aistock-package-asset" or parsed.netloc != "blobs":
                raise ValueError("asset_ref URI must use the controlled package asset scheme")
            digest_parts = tuple(part for part in parsed.path.strip("/").split("/") if part)
            if len(digest_parts) != 1:
                raise ValueError("package asset URI must contain exactly one blob digest")
            validate_sha256(digest_parts[0], field_name="asset_ref_digest")
            return normalized
        if normalized.startswith("/") or ":" in normalized or ".." in normalized.split("/"):
            raise ValueError("asset_ref must not contain an absolute or escaping path")
        return normalized

    @model_validator(mode="after")
    def _controlled_uri_closes_blob(self) -> "BundleBlobRef":
        parsed = urlparse(self.asset_ref)
        if parsed.scheme:
            digest = parsed.path.strip("/")
            if digest != self.blob_ref.blob_sha256:
                raise ValueError("package asset URI digest differs from the blob authority")
        return self


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
        if {
            item.package_id: item.source_manifest_sha256 for item in packages
        } != self.request.expected_package_manifest_sha256s:
            raise ValueError("bundle source package manifest identities must exactly match the request")
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
        runtime_refs_by_package: dict[str, dict[str, str]] = {}
        if {"manifest_json", "package_status"}.issubset(package_row_set.semantic_column_names):
            for row in package_row_set.sorted_rows:
                package_id = str(deserialize_postgres_value(row["package_id"]))
                manifest_sha = str(deserialize_postgres_value(row["manifest_sha256"])).lower()
                package_status = str(deserialize_postgres_value(row["package_status"])).upper()
                manifest = deserialize_postgres_value(row["manifest_json"])
                if not isinstance(manifest, Mapping):
                    raise ValueError("package manifest_json must decode to an object")
                if (
                    str(manifest.get("manifest_sha256") or "").lower() != manifest_sha
                    or compute_portable_manifest_json_sha256(manifest) != manifest_sha
                    or package_status not in ALLOWED_EXPORT_PACKAGE_STATUSES
                ):
                    raise ValueError("package row manifest hash or lifecycle is invalid")
                runtime_refs = portable_manifest_runtime_asset_refs(manifest)
                if not runtime_refs:
                    raise ValueError("package row has no governed runtime asset closure")
                package_ref = next(item for item in packages if item.package_id == package_id)
                if (
                    package_ref.projection.runtime_asset_closure_hash
                    != canonical_json_sha256(runtime_refs)
                    or package_ref.projection.alpha_component_closure_hash
                    != canonical_json_sha256(manifest.get("alpha_components"))
                ):
                    raise ValueError("package row differs from its portable projection evidence")
                runtime_refs_by_package[package_id] = runtime_refs
        asset_row_set = next(item for item in row_sets if item.relation_name == "strategy_pkg.package_asset")
        if not {"package_id", "asset_type", "asset_ref", "asset_sha256"}.issubset(asset_row_set.semantic_column_names):
            raise ValueError("package asset row set lacks its identity columns")
        asset_rows = {
            (str(row["package_id"]), str(row["asset_type"]), str(row["asset_ref"])): str(row["asset_sha256"])
            for row in asset_row_set.sorted_rows
        }
        if any(package_id not in {item.package_id for item in packages} for package_id, _, _ in asset_rows):
            raise ValueError("package asset row set contains an unrelated package")
        projected_assets = {
            (
                str(deserialize_postgres_value(row["package_id"])),
                str(deserialize_postgres_value(row["asset_ref"])),
                str(deserialize_postgres_value(row["asset_sha256"])).lower(),
            )
            for row in asset_row_set.sorted_rows
        }
        required_assets = {
            (package_id, asset_ref, digest)
            for package_id, refs in runtime_refs_by_package.items()
            for asset_ref, digest in refs.items()
        }
        if runtime_refs_by_package and (
            not required_assets.issubset(projected_assets)
            or any(
                (package_id, asset_ref, digest) not in required_assets
                for package_id, asset_ref, digest in projected_assets
            )
        ):
            raise ValueError("package asset rows differ from the manifest runtime closure")
        blob_rows = {
            (blob.package_id, blob.asset_type, blob.asset_ref): blob.asset_sha256
            for blob in blobs
        }
        if len(blob_rows) != len(blobs):
            raise ValueError("artifact blob refs must have unique package asset identities")
        if blob_rows != asset_rows:
            raise ValueError("every package asset row must close to exactly one artifact blob ref")
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


class PlannedImportRow(HashClosedContract):
    """One exact bundle row classified against a fresh DEV readback."""

    hash_field: ClassVar[str] = "row_plan_hash"
    relation_name: Literal["strategy_pkg.package", "strategy_pkg.package_asset"]
    natural_key_fields: tuple[str, ...] = Field(min_length=1)
    natural_key_values: dict[str, Any]
    semantic_row: dict[str, Any]
    expected_row_hash: str = Field(min_length=64, max_length=64)
    disposition: ImportRowDisposition
    actual_row_hash: str | None = Field(default=None, min_length=64, max_length=64)
    row_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("expected_row_hash", "actual_row_hash", "row_plan_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "PlannedImportRow":
        key_fields = sorted_unique(self.natural_key_fields, field_name="natural_key_fields")
        if set(self.natural_key_values) != set(key_fields):
            raise ValueError("natural key values must exactly match natural key fields")
        if not set(key_fields).issubset(self.semantic_row):
            raise ValueError("natural key fields must be present in the semantic row")
        expected_key_values = {name: self.semantic_row[name] for name in key_fields}
        if self.natural_key_values != expected_key_values:
            raise ValueError("natural key values differ from the semantic row")
        expected_hash = canonical_json_sha256(self.semantic_row)
        if self.expected_row_hash != expected_hash:
            raise ValueError("expected row hash differs from the semantic row")
        if self.disposition is ImportRowDisposition.INSERT and self.actual_row_hash is not None:
            raise ValueError("INSERT row cannot carry an actual row hash")
        if self.disposition is ImportRowDisposition.EXACT_MATCH and self.actual_row_hash != self.expected_row_hash:
            raise ValueError("EXACT_MATCH row must have the expected actual hash")
        if self.disposition is ImportRowDisposition.CONFLICT and (
            self.actual_row_hash is None or self.actual_row_hash == self.expected_row_hash
        ):
            raise ValueError("CONFLICT row requires a different actual hash")
        object.__setattr__(self, "natural_key_fields", key_fields)
        self.close_hash()
        return self


class ImportWriteOperation(StrictContract):
    relation_name: Literal["strategy_pkg.package", "strategy_pkg.package_asset"]
    row_plan_hash: str = Field(min_length=64, max_length=64)
    expected_row_hash: str = Field(min_length=64, max_length=64)
    natural_key_values: dict[str, Any]
    semantic_row: dict[str, Any]

    @field_validator("row_plan_hash", "expected_row_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)


class RealDevImportPlan(HashClosedContract):
    hash_field: ClassVar[str] = "plan_hash"
    schema_version: Literal[IMPORT_PLAN_SCHEMA_VERSION] = IMPORT_PLAN_SCHEMA_VERSION
    bundle_ref: OnboardingArtifactRef
    target_database_identity: DatabaseIdentity
    release_receipt_ref: Phase1GInputArtifactRef
    classified_rows: tuple[PlannedImportRow, ...]
    insert_rows_by_relation: dict[str, tuple[str, ...]]
    exact_match_rows_by_relation: dict[str, tuple[str, ...]]
    conflict_rows_by_relation: dict[str, tuple[str, ...]]
    ordered_write_operations: tuple[ImportWriteOperation, ...]
    planned_write_relation_set: tuple[str, ...]
    status: ImportPlanStatus
    reason_codes: tuple[str, ...] = ()
    plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("plan_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="plan_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevImportPlan":
        if self.bundle_ref.evidence_kind is not EvidenceKind.BUNDLE:
            raise ValueError("import plan bundle_ref must reference a bundle")
        _validate_release_receipt_ref(self.release_receipt_ref)
        if self.target_database_identity.target_label is not TargetLabel.DEV:
            raise ValueError("import plan target database must be DEV")
        rows = tuple(
            sorted(
                self.classified_rows,
                key=lambda item: (item.relation_name, canonical_json_sha256(item.natural_key_values)),
            )
        )
        row_hashes = tuple(str(item.row_plan_hash) for item in rows)
        if len(row_hashes) != len(set(row_hashes)):
            raise ValueError("classified rows must have unique row plan hashes")
        grouped = {
            disposition: {
                relation: tuple(
                    str(item.row_plan_hash)
                    for item in rows
                    if item.disposition is disposition and item.relation_name == relation
                )
                for relation in ("strategy_pkg.package", "strategy_pkg.package_asset")
                if any(item.disposition is disposition and item.relation_name == relation for item in rows)
            }
            for disposition in ImportRowDisposition
        }
        supplied_groups = {
            ImportRowDisposition.INSERT: self.insert_rows_by_relation,
            ImportRowDisposition.EXACT_MATCH: self.exact_match_rows_by_relation,
            ImportRowDisposition.CONFLICT: self.conflict_rows_by_relation,
        }
        for disposition, supplied in supplied_groups.items():
            normalized = {name: tuple(values) for name, values in sorted(supplied.items())}
            if normalized != grouped[disposition]:
                raise ValueError(f"{disposition.value} row summary differs from classified rows")
        insert_rows = [item for item in rows if item.disposition is ImportRowDisposition.INSERT]
        relation_order = {"strategy_pkg.package": 0, "strategy_pkg.package_asset": 1}
        insert_rows.sort(key=lambda item: (relation_order[item.relation_name], canonical_json_sha256(item.natural_key_values)))
        has_conflict = any(item.disposition is ImportRowDisposition.CONFLICT for item in rows)
        expected_operations = (
            ()
            if has_conflict
            else tuple(
                ImportWriteOperation(
                    relation_name=item.relation_name,
                    row_plan_hash=str(item.row_plan_hash),
                    expected_row_hash=item.expected_row_hash,
                    natural_key_values=item.natural_key_values,
                    semantic_row=item.semantic_row,
                )
                for item in insert_rows
            )
        )
        if self.ordered_write_operations != expected_operations:
            raise ValueError("ordered write operations differ from INSERT rows")
        relation_set = () if has_conflict else tuple(sorted({item.relation_name for item in insert_rows}))
        if self.planned_write_relation_set != relation_set:
            raise ValueError("planned write relation set differs from INSERT rows")
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        expected_status = (
            ImportPlanStatus.CONFLICT
            if has_conflict
            else ImportPlanStatus.EXECUTABLE
            if insert_rows
            else ImportPlanStatus.ALREADY_PRESENT
        )
        if self.status is not expected_status:
            raise ValueError("import plan status differs from row classifications")
        if expected_status is ImportPlanStatus.CONFLICT:
            if REASON_IMPORT_PLAN_CONFLICT not in reasons or self.ordered_write_operations:
                raise ValueError("conflict plan requires its reason and zero write operations")
        elif reasons:
            raise ValueError("executable/already-present plan cannot carry failure reasons")
        object.__setattr__(self, "classified_rows", rows)
        object.__setattr__(self, "planned_write_relation_set", relation_set)
        object.__setattr__(self, "reason_codes", reasons)
        self.close_hash()
        return self


class RealDevImportReceipt(HashClosedContract):
    hash_field: ClassVar[str] = "receipt_hash"
    schema_version: Literal[IMPORT_RECEIPT_SCHEMA_VERSION] = IMPORT_RECEIPT_SCHEMA_VERSION
    import_invocation_id: str = Field(min_length=1, max_length=160)
    bundle_ref: OnboardingArtifactRef
    request_hash: str = Field(min_length=64, max_length=64)
    bundle_hash: str = Field(min_length=64, max_length=64)
    plan_hash: str = Field(min_length=64, max_length=64)
    source_database_identity_hash: str = Field(min_length=64, max_length=64)
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    transaction_id: str | None = Field(default=None, min_length=1, max_length=160)
    inserted_row_counts: dict[str, int]
    matched_row_counts: dict[str, int]
    write_relation_set: tuple[str, ...]
    post_readback_row_hashes: dict[str, tuple[str, ...]]
    post_dependency_closure_hash: str = Field(min_length=64, max_length=64)
    physical_commit_count: int | None = Field(default=None, ge=0, le=1)
    commit_outcome: ImportCommitOutcome
    started_at: datetime
    finished_at: datetime
    reason_codes: tuple[str, ...] = ()
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "request_hash",
        "bundle_hash",
        "plan_hash",
        "source_database_identity_hash",
        "target_database_identity_hash",
        "post_dependency_closure_hash",
        "receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevImportReceipt":
        if self.bundle_ref.evidence_kind is not EvidenceKind.BUNDLE or self.bundle_ref.semantic_content_hash != self.bundle_hash:
            raise ValueError("import receipt bundle identity is invalid")
        if set(self.inserted_row_counts) != IMPORT_RELATION_SET or set(self.matched_row_counts) != IMPORT_RELATION_SET:
            raise ValueError("import receipt relation counts must exactly match the fixed import allowlist")
        if not set(self.post_readback_row_hashes).issubset(IMPORT_RELATION_SET):
            raise ValueError("import receipt post readback relations exceed the fixed import allowlist")
        if any(value < 0 for value in (*self.inserted_row_counts.values(), *self.matched_row_counts.values())):
            raise ValueError("import receipt row counts must be non-negative")
        write_set = tuple(sorted(name for name, count in self.inserted_row_counts.items() if count > 0))
        if self.write_relation_set != write_set:
            raise ValueError("import receipt write relation set differs from inserted row counts")
        post_hashes = {
            name: tuple(validate_sha256(item, field_name="post_readback_row_hash") for item in values)
            for name, values in sorted(self.post_readback_row_hashes.items())
        }
        if any(tuple(sorted(values)) != values for values in post_hashes.values()):
            raise ValueError("post readback row hashes must be sorted")
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        if self.finished_at.tzinfo is None or self.finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        started = self.started_at.astimezone(timezone.utc)
        finished = self.finished_at.astimezone(timezone.utc)
        if finished < started:
            raise ValueError("finished_at must not precede started_at")
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if self.commit_outcome is ImportCommitOutcome.COMMITTED:
            if self.transaction_id is None or self.physical_commit_count != 1 or reasons:
                raise ValueError("COMMITTED receipt requires one physical commit and no failure reason")
        elif self.commit_outcome is ImportCommitOutcome.ROLLED_BACK:
            if (
                self.transaction_id is None
                or self.physical_commit_count != 0
                or not any(self.inserted_row_counts.values())
                or any(self.post_readback_row_hashes.values())
                or reasons
            ):
                raise ValueError(
                    "ROLLED_BACK receipt requires attempted inserts, zero commit, zero residue and no failure reason"
                )
        elif self.commit_outcome is ImportCommitOutcome.ALREADY_PRESENT:
            if self.transaction_id is not None or self.physical_commit_count != 0 or any(self.inserted_row_counts.values()) or reasons:
                raise ValueError("ALREADY_PRESENT receipt requires zero DML and zero failure reason")
        else:
            if self.transaction_id is None or self.physical_commit_count is not None or REASON_IMPORT_COMMIT_STATE_UNKNOWN not in reasons:
                raise ValueError("STATE_UNKNOWN receipt requires unknown commit count and stable reason")
        object.__setattr__(self, "write_relation_set", write_set)
        object.__setattr__(self, "post_readback_row_hashes", post_hashes)
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        object.__setattr__(self, "reason_codes", reasons)
        self.close_hash()
        return self


class HistoricalProgramSpec(StrictContract):
    program_id: str = Field(min_length=1, max_length=160)
    program_name: str = Field(min_length=1, max_length=240)
    package_id: str = Field(min_length=1, max_length=160)
    alpha_mode: AlphaMode
    style: str = Field(min_length=1, max_length=120)
    target_count: int = Field(gt=0, le=100)
    review_policy: dict[str, Any]
    runtime_config: dict[str, Any]
    review_schedule: dict[str, Any] = Field(default_factory=lambda: {"frequency": "daily_after_close"})
    entry_price_basis: Literal["next_open_executable"] = "next_open_executable"
    exit_price_basis: Literal["next_open_executable"] = "next_open_executable"
    created_by: str = Field(default="advisory_real_dev_onboarding", min_length=1, max_length=160)

    @field_validator("program_id", "program_name", "package_id", "style", "created_by")
    @classmethod
    def _text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("historical program text fields cannot be blank")
        return normalized


class RealDevHistoricalRunRequest(HashClosedContract):
    hash_field: ClassVar[str] = "historical_request_hash"
    schema_version: Literal[HISTORICAL_RUN_REQUEST_SCHEMA_VERSION] = HISTORICAL_RUN_REQUEST_SCHEMA_VERSION
    onboarding_request_ref: OnboardingArtifactRef
    onboarding_request_hash: str = Field(min_length=64, max_length=64)
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    target_package_asset_root_hash: str = Field(min_length=64, max_length=64)
    program_specs: tuple[HistoricalProgramSpec, ...] = Field(min_length=2)
    binding_effective_from_trade_date: date
    decision_trade_date: date
    policy_registry_id: str = Field(min_length=1, max_length=160)
    policy_registry_version: str = Field(min_length=1, max_length=80)
    policy_registry_hash: str = Field(min_length=64, max_length=64)
    code_release_id: str = Field(min_length=1, max_length=160)
    code_release_hash: str = Field(min_length=64, max_length=64)
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: Literal[True] = True
    historical_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "onboarding_request_hash",
        "target_database_identity_hash",
        "target_package_asset_root_hash",
        "policy_registry_hash",
        "code_release_hash",
        "historical_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevHistoricalRunRequest":
        if self.onboarding_request_ref.evidence_kind is not EvidenceKind.REQUEST:
            raise ValueError("historical run must reference one exact onboarding request")
        if self.onboarding_request_ref.semantic_content_hash != self.onboarding_request_hash:
            raise ValueError("onboarding request ref hash differs from onboarding_request_hash")
        if self.decision_trade_date < self.binding_effective_from_trade_date:
            raise ValueError("decision_trade_date must be inside the dated binding interval")
        programs = tuple(sorted(self.program_specs, key=lambda item: item.program_id))
        if len({item.program_id for item in programs}) != len(programs):
            raise ValueError("historical program ids must be unique")
        if len({item.package_id for item in programs}) != len(programs):
            raise ValueError("historical packages must map one-to-one to Programs")
        if {item.alpha_mode for item in programs} != {AlphaMode.SINGLE, AlphaMode.MULTI}:
            raise ValueError("historical run must include single and native multi tracks")
        object.__setattr__(self, "program_specs", programs)
        self.close_hash()
        return self


class HistoricalProgramResult(StrictContract):
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    alpha_mode: AlphaMode
    status: HistoricalProgramStatus
    program_payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)
    binding_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    binding_payload_hash: str | None = Field(default=None, min_length=64, max_length=64)
    selection_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    evidence_id: str | None = Field(default=None, min_length=1, max_length=160)
    evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)
    artifact_id: str | None = Field(default=None, min_length=1, max_length=160)
    artifact_payload_hash: str | None = Field(default=None, min_length=64, max_length=64)
    historical_program_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    reason_codes: tuple[str, ...] = ()

    @field_validator("program_payload_sha256", "binding_payload_hash", "evidence_hash", "artifact_payload_hash")
    @classmethod
    def _optional_hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _status_is_coherent(self) -> "HistoricalProgramResult":
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if self.status is HistoricalProgramStatus.COMPLETE:
            required = (
                self.program_payload_sha256,
                self.binding_version_id,
                self.binding_payload_hash,
                self.evidence_id,
                self.evidence_hash,
                self.artifact_id,
                self.artifact_payload_hash,
                self.historical_program_run_id,
            )
            if not all(required) or reasons:
                raise ValueError("complete historical Program result requires full identities and no failure reason")
        elif not reasons:
            raise ValueError("non-complete historical Program result requires a stable reason")
        object.__setattr__(self, "reason_codes", reasons)
        return self


class RealDevHistoricalRunReceipt(HashClosedContract):
    hash_field: ClassVar[str] = "receipt_hash"
    schema_version: Literal[HISTORICAL_RUN_RECEIPT_SCHEMA_VERSION] = HISTORICAL_RUN_RECEIPT_SCHEMA_VERSION
    historical_request_hash: str = Field(min_length=64, max_length=64)
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    target_package_asset_root_hash: str = Field(min_length=64, max_length=64)
    batch_id: str = Field(min_length=1, max_length=160)
    batch_key: str = Field(min_length=64, max_length=64)
    batch_status: Literal["COMPLETE", "WAITING_INPUT", "FAILED"]
    formal_batch_receipt_hash: str = Field(min_length=64, max_length=64)
    program_results: tuple[HistoricalProgramResult, ...] = Field(min_length=2)
    started_at: datetime
    finished_at: datetime
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "historical_request_hash",
        "target_database_identity_hash",
        "target_package_asset_root_hash",
        "batch_key",
        "formal_batch_receipt_hash",
        "receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "RealDevHistoricalRunReceipt":
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        if self.finished_at.tzinfo is None or self.finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        started = self.started_at.astimezone(timezone.utc)
        finished = self.finished_at.astimezone(timezone.utc)
        if finished < started:
            raise ValueError("finished_at must not precede started_at")
        results = tuple(sorted(self.program_results, key=lambda item: item.program_id))
        if len({item.program_id for item in results}) != len(results):
            raise ValueError("historical receipt Program ids must be unique")
        statuses = {item.status for item in results}
        expected_batch_status = (
            HistoricalProgramStatus.FAILED.value
            if HistoricalProgramStatus.FAILED in statuses
            else HistoricalProgramStatus.WAITING_INPUT.value
            if HistoricalProgramStatus.WAITING_INPUT in statuses
            else HistoricalProgramStatus.COMPLETE.value
        )
        if self.batch_status != expected_batch_status:
            raise ValueError("historical receipt batch_status must equal the aggregate Program status")
        object.__setattr__(self, "program_results", results)
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        self.close_hash()
        return self


class AdvisoryImmutableArtifactRef(StrictContract):
    """Location plus both semantic and physical identities for one immutable artifact."""

    schema_version: Literal[O4_ARTIFACT_REF_SCHEMA_VERSION] = O4_ARTIFACT_REF_SCHEMA_VERSION
    artifact_kind: str = Field(min_length=1, max_length=120)
    store_policy_hash: str = Field(min_length=64, max_length=64)
    relative_path: str = Field(min_length=1, max_length=800)
    semantic_hash: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("store_policy_hash", "semantic_hash", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @field_validator("artifact_kind")
    @classmethod
    def _kind(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for character in normalized):
            raise ValueError("artifact_kind must be one lowercase path-safe identifier")
        return normalized

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        normalized = str(value or "").replace("\\", "/").strip()
        if not normalized or normalized.startswith("/") or ":" in normalized or ".." in normalized.split("/"):
            raise ValueError("relative_path must be a contained relative path")
        return normalized


def _validate_ref_hash(
    *,
    ref: AdvisoryImmutableArtifactRef | None,
    semantic_hash: str | None,
    field_name: str,
    expected_artifact_kind: O4ArtifactKind | None = None,
) -> None:
    if (ref is None) != (semantic_hash is None):
        raise ValueError(f"{field_name} ref and hash must either both be present or both be absent")
    if ref is not None and ref.semantic_hash != semantic_hash:
        raise ValueError(f"{field_name} ref semantic hash differs from its bound hash")
    if ref is not None and expected_artifact_kind is not None and ref.artifact_kind != expected_artifact_kind.value:
        raise ValueError(
            f"{field_name} ref kind must be {expected_artifact_kind.value}, got {ref.artifact_kind}"
        )


class Phase1EProgramDateInput(StrictContract):
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    style_family: str = Field(min_length=1, max_length=120)
    historical_status: HistoricalProgramStatus
    historical_program_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    historical_reason_codes: tuple[str, ...] = ()
    historical_batch_receipt_ref: AdvisoryImmutableArtifactRef | None = None
    historical_batch_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "historical_batch_receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _coherent(self) -> "Phase1EProgramDateInput":
        reasons = sorted_unique(self.historical_reason_codes, field_name="historical_reason_codes") if self.historical_reason_codes else ()
        _validate_ref_hash(
            ref=self.historical_batch_receipt_ref,
            semantic_hash=self.historical_batch_receipt_hash,
            field_name="historical_batch_receipt",
        )
        if self.historical_status is HistoricalProgramStatus.COMPLETE:
            if self.historical_program_run_id is None or self.historical_batch_receipt_ref is None or reasons:
                raise ValueError("COMPLETE Program date requires exact historical run and batch receipt identities")
        elif not reasons:
            raise ValueError("non-complete Program date requires stable historical reason codes")
        object.__setattr__(self, "historical_reason_codes", reasons)
        return self


class Phase1ERealInputBuildRequest(HashClosedContract):
    hash_field: ClassVar[str] = "build_request_hash"
    schema_version: Literal[PHASE1E_REAL_INPUT_BUILD_REQUEST_SCHEMA_VERSION] = (
        PHASE1E_REAL_INPUT_BUILD_REQUEST_SCHEMA_VERSION
    )
    historical_run_request_ref: AdvisoryImmutableArtifactRef
    historical_run_request_hash: str = Field(min_length=64, max_length=64)
    historical_run_receipt_ref: AdvisoryImmutableArtifactRef
    historical_run_receipt_hash: str = Field(min_length=64, max_length=64)
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    target_package_asset_root_hash: str = Field(min_length=64, max_length=64)
    program_dates: tuple[Phase1EProgramDateInput, ...] = Field(min_length=1)
    phase0a_policy_registry_ref: AdvisoryImmutableArtifactRef
    phase0a_policy_registry_hash: str = Field(min_length=64, max_length=64)
    source_mapping_registry_ref: AdvisoryImmutableArtifactRef
    source_mapping_registry_hash: str = Field(min_length=64, max_length=64)
    source_query_registry_ref: AdvisoryImmutableArtifactRef
    source_query_registry_hash: str = Field(min_length=64, max_length=64)
    calendar_registry_ref: AdvisoryImmutableArtifactRef
    calendar_registry_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_ref: AdvisoryImmutableArtifactRef
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    partition_policy_ref: AdvisoryImmutableArtifactRef
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    store_backend_policy_ref: AdvisoryImmutableArtifactRef
    store_backend_policy_hash: str = Field(min_length=64, max_length=64)
    capacity_policy_ref: AdvisoryImmutableArtifactRef
    capacity_policy_hash: str = Field(min_length=64, max_length=64)
    phase1e_artifact_store_policy_ref: AdvisoryImmutableArtifactRef
    phase1e_artifact_store_policy_hash: str = Field(min_length=64, max_length=64)
    code_release_id: str = Field(min_length=1, max_length=160)
    code_release_hash: str = Field(min_length=64, max_length=64)
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: Literal[True] = True
    build_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "historical_run_request_hash",
        "historical_run_receipt_hash",
        "target_database_identity_hash",
        "target_package_asset_root_hash",
        "phase0a_policy_registry_hash",
        "source_mapping_registry_hash",
        "source_query_registry_hash",
        "calendar_registry_hash",
        "label_policy_bundle_hash",
        "partition_policy_hash",
        "store_backend_policy_hash",
        "capacity_policy_hash",
        "phase1e_artifact_store_policy_hash",
        "code_release_hash",
        "build_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1ERealInputBuildRequest":
        for field_name in (
            "historical_run_request",
            "historical_run_receipt",
            "phase0a_policy_registry",
            "source_mapping_registry",
            "source_query_registry",
            "calendar_registry",
            "label_policy_bundle",
            "partition_policy",
            "store_backend_policy",
            "capacity_policy",
            "phase1e_artifact_store_policy",
        ):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
            )
        programs = tuple(sorted(self.program_dates, key=lambda item: (item.program_id, item.decision_trade_date)))
        identities = tuple((item.program_id, item.decision_trade_date) for item in programs)
        if len(identities) != len(set(identities)):
            raise ValueError("program_dates must contain unique Program/date identities")
        object.__setattr__(self, "program_dates", programs)
        self.close_hash()
        return self


class SourceBindParameter(StrictContract):
    name: str = Field(min_length=1, max_length=120)
    data_type: str = Field(min_length=1, max_length=120)
    nullable: bool = False


class SourcePhysicalRequirementMapping(StrictContract):
    source_role: str = Field(min_length=1, max_length=120)
    dataset_name: str = Field(min_length=1, max_length=240)
    observer_query_template_id: str = Field(min_length=1, max_length=160)
    observer_query_template_version: str = Field(min_length=1, max_length=80)
    observer_query_template_hash: str = Field(min_length=64, max_length=64)
    audit_evidence_policy_id: str = Field(min_length=1, max_length=160)
    audit_evidence_policy_version: str = Field(min_length=1, max_length=80)
    audit_evidence_policy_hash: str = Field(min_length=64, max_length=64)
    partition_mapper_id: str = Field(min_length=1, max_length=160)
    partition_mapper_version: str = Field(min_length=1, max_length=80)
    partition_mapper_hash: str = Field(min_length=64, max_length=64)
    partition_granularity: PartitionGranularity
    bind_parameter_schema: tuple[SourceBindParameter, ...]
    canonical_sort_columns: tuple[str, ...] = Field(min_length=1)
    capacity_date_column: str | None = Field(default=None, min_length=1, max_length=120)
    business_window_derivation: str = Field(min_length=1, max_length=160)
    availability_requirement: str = Field(min_length=1, max_length=160)
    cutoff_predicate_id: str = Field(min_length=1, max_length=160)
    cutoff_predicate_version: str = Field(min_length=1, max_length=80)
    cutoff_predicate_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "observer_query_template_hash",
        "audit_evidence_policy_hash",
        "partition_mapper_hash",
        "cutoff_predicate_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _normalize(self) -> "SourcePhysicalRequirementMapping":
        bind_names = tuple(item.name for item in self.bind_parameter_schema)
        if len(bind_names) != len(set(bind_names)):
            raise ValueError("bind_parameter_schema names must be unique")
        sort_columns = tuple(str(value or "").strip() for value in self.canonical_sort_columns)
        if any(not value for value in sort_columns) or len(sort_columns) != len(set(sort_columns)):
            raise ValueError("canonical_sort_columns must contain ordered unique non-empty values")
        object.__setattr__(self, "canonical_sort_columns", sort_columns)
        return self


class SourceMappingEntry(StrictContract):
    dse_source_role: str = Field(min_length=1, max_length=120)
    dse_dataset_id: str = Field(min_length=1, max_length=240)
    dse_query_template_id: str = Field(min_length=1, max_length=160)
    dse_query_template_version: str = Field(min_length=1, max_length=80)
    physical_requirements: tuple[SourcePhysicalRequirementMapping, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _normalize(self) -> "SourceMappingEntry":
        items = tuple(
            sorted(
                self.physical_requirements,
                key=lambda item: (item.source_role, item.dataset_name, item.observer_query_template_id),
            )
        )
        identities = tuple(
            (
                item.source_role,
                item.dataset_name,
                item.observer_query_template_id,
                item.observer_query_template_version,
            )
            for item in items
        )
        if len(identities) != len(set(identities)):
            raise ValueError("physical requirements must have unique identities")
        object.__setattr__(self, "physical_requirements", items)
        return self


class AdvisorySourceMappingRegistry(HashClosedContract):
    hash_field: ClassVar[str] = "registry_hash"
    schema_version: Literal[SOURCE_MAPPING_REGISTRY_SCHEMA_VERSION] = SOURCE_MAPPING_REGISTRY_SCHEMA_VERSION
    registry_id: str = Field(min_length=1, max_length=160)
    registry_version: str = Field(min_length=1, max_length=80)
    entries: tuple[SourceMappingEntry, ...] = Field(min_length=1)
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("registry_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="registry_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "AdvisorySourceMappingRegistry":
        entries = tuple(sorted(self.entries, key=lambda item: (item.dse_source_role, item.dse_dataset_id, item.dse_query_template_id)))
        identities = tuple((item.dse_source_role, item.dse_dataset_id, item.dse_query_template_id, item.dse_query_template_version) for item in entries)
        if len(identities) != len(set(identities)):
            raise ValueError("source mapping entries must have unique logical identities")
        object.__setattr__(self, "entries", entries)
        self.close_hash()
        return self


class ExpectedLogicalInput(StrictContract):
    alpha_component_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=120)
    dataset_id: str = Field(min_length=1, max_length=240)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    expected_window_start_date: date
    effective_trade_date: date
    required_window: int = Field(ge=1)
    window_resolution: str = Field(min_length=1, max_length=80)
    expected_window_lineage_hash: str = Field(min_length=64, max_length=64)
    physical_requirement_templates: tuple[SourcePhysicalRequirementMapping, ...] = Field(min_length=1)

    @field_validator("expected_window_lineage_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return validate_sha256(value, field_name="expected_window_lineage_hash")

    @model_validator(mode="after")
    def _window(self) -> "ExpectedLogicalInput":
        if self.expected_window_start_date > self.effective_trade_date:
            raise ValueError("expected source window start must not follow its effective trade date")
        return self


class AdvisorySourceObservationScopeRequest(HashClosedContract):
    hash_field: ClassVar[str] = "observation_scope_hash"
    schema_version: Literal[SOURCE_OBSERVATION_SCOPE_REQUEST_SCHEMA_VERSION] = (
        SOURCE_OBSERVATION_SCOPE_REQUEST_SCHEMA_VERSION
    )
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    pit_universe_key: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    style_family: str = Field(min_length=1, max_length=120)
    binding_version_id: str = Field(min_length=1, max_length=160)
    binding_payload_hash: str = Field(min_length=64, max_length=64)
    selection_normalized_config_hash: str = Field(min_length=64, max_length=64)
    strategy_package_input_projection_ref: AdvisoryImmutableArtifactRef
    strategy_package_input_projection_hash: str = Field(min_length=64, max_length=64)
    source_mapping_registry_ref: AdvisoryImmutableArtifactRef
    source_mapping_registry_hash: str = Field(min_length=64, max_length=64)
    source_query_registry_ref: AdvisoryImmutableArtifactRef
    source_query_registry_hash: str = Field(min_length=64, max_length=64)
    window_policy_ref: AdvisoryImmutableArtifactRef
    window_policy_hash: str = Field(min_length=64, max_length=64)
    decision_cutoff_ts: datetime
    expected_logical_inputs: tuple[ExpectedLogicalInput, ...] = Field(min_length=1)
    observation_scope_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "target_database_identity_hash",
        "manifest_sha256",
        "binding_payload_hash",
        "selection_normalized_config_hash",
        "strategy_package_input_projection_hash",
        "source_mapping_registry_hash",
        "source_query_registry_hash",
        "window_policy_hash",
        "observation_scope_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("decision_cutoff_ts")
    @classmethod
    def _cutoff(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("decision_cutoff_ts must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _close(self) -> "AdvisorySourceObservationScopeRequest":
        _validate_ref_hash(
            ref=self.strategy_package_input_projection_ref,
            semantic_hash=self.strategy_package_input_projection_hash,
            field_name="strategy_package_input_projection",
            expected_artifact_kind=O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION,
        )
        for field_name in ("source_mapping_registry", "source_query_registry", "window_policy"):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
            )
        inputs = tuple(
            sorted(
                self.expected_logical_inputs,
                key=lambda item: (item.alpha_component_id or "", item.source_role, item.dataset_id, item.query_template_id),
            )
        )
        identities = tuple((item.alpha_component_id, item.source_role, item.dataset_id, item.query_template_id) for item in inputs)
        if len(identities) != len(set(identities)):
            raise ValueError("expected logical inputs must have unique leg/role/dataset/query identities")
        object.__setattr__(self, "expected_logical_inputs", inputs)
        self.close_hash()
        return self


class SourcePartitionRequirement(StrictContract):
    alpha_component_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=120)
    dataset_name: str = Field(min_length=1, max_length=240)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    partition_granularity: PartitionGranularity
    partition_key: dict[str, Any]
    partition_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("partition_identity_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field_name="partition_identity_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "SourcePartitionRequirement":
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"partition_identity_hash"}))
        if self.partition_identity_hash is not None and self.partition_identity_hash != digest:
            raise ValueError("partition_identity_hash does not match the physical requirement")
        object.__setattr__(self, "partition_identity_hash", digest)
        return self


class ProgramSourceRequirementSet(HashClosedContract):
    hash_field: ClassVar[str] = "requirement_set_hash"
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    observation_scope_ref: AdvisoryImmutableArtifactRef
    observation_scope_hash: str = Field(min_length=64, max_length=64)
    dse_evidence_hash: str = Field(min_length=64, max_length=64)
    selection_artifact_hash: str = Field(min_length=64, max_length=64)
    physical_requirements: tuple[SourcePartitionRequirement, ...] = Field(min_length=1)
    requirement_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "observation_scope_hash", "dse_evidence_hash", "selection_artifact_hash", "requirement_set_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "ProgramSourceRequirementSet":
        _validate_ref_hash(
            ref=self.observation_scope_ref,
            semantic_hash=self.observation_scope_hash,
            field_name="observation_scope",
            expected_artifact_kind=O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST,
        )
        requirements = tuple(sorted(self.physical_requirements, key=lambda item: str(item.partition_identity_hash)))
        identities = tuple(item.partition_identity_hash for item in requirements)
        if len(identities) != len(set(identities)):
            raise ValueError("physical source requirements must be unique")
        object.__setattr__(self, "physical_requirements", requirements)
        self.close_hash()
        return self


class AdvisorySourceRequirementRegistry(HashClosedContract):
    hash_field: ClassVar[str] = "registry_hash"
    schema_version: Literal[SOURCE_REQUIREMENT_REGISTRY_SCHEMA_VERSION] = SOURCE_REQUIREMENT_REGISTRY_SCHEMA_VERSION
    build_request_hash: str = Field(min_length=64, max_length=64)
    source_mapping_registry_hash: str = Field(min_length=64, max_length=64)
    source_query_registry_hash: str = Field(min_length=64, max_length=64)
    program_requirement_sets: tuple[ProgramSourceRequirementSet, ...] = Field(min_length=1)
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("build_request_hash", "source_mapping_registry_hash", "source_query_registry_hash", "registry_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "AdvisorySourceRequirementRegistry":
        items = tuple(sorted(self.program_requirement_sets, key=lambda item: (item.program_id, item.decision_trade_date)))
        identities = tuple((item.program_id, item.decision_trade_date) for item in items)
        if len(identities) != len(set(identities)):
            raise ValueError("source requirement registry must have one set per Program/date")
        object.__setattr__(self, "program_requirement_sets", items)
        self.close_hash()
        return self


class Phase1EProgramInputUnit(HashClosedContract):
    hash_field: ClassVar[str] = "program_input_hash"
    schema_version: Literal[PHASE1E_PROGRAM_INPUT_SCHEMA_VERSION] = PHASE1E_PROGRAM_INPUT_SCHEMA_VERSION
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    style_family: str = Field(min_length=1, max_length=120)
    historical_program_run_ref: AdvisoryImmutableArtifactRef | None = None
    historical_program_run_hash: str | None = Field(default=None, min_length=64, max_length=64)
    phase0a_audit_ref: AdvisoryImmutableArtifactRef | None = None
    phase0a_audit_hash: str | None = Field(default=None, min_length=64, max_length=64)
    handoff_readiness_ref: AdvisoryImmutableArtifactRef | None = None
    handoff_readiness_hash: str | None = Field(default=None, min_length=64, max_length=64)
    handoff_bundle_ref: AdvisoryImmutableArtifactRef | None = None
    handoff_bundle_hash: str | None = Field(default=None, min_length=64, max_length=64)
    source_requirement_set_ref: AdvisoryImmutableArtifactRef | None = None
    source_requirement_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    source_resolution_receipt_ref: AdvisoryImmutableArtifactRef | None = None
    source_resolution_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_program_workload_ref: AdvisoryImmutableArtifactRef | None = None
    capacity_program_workload_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_coverage_ref: AdvisoryImmutableArtifactRef | None = None
    capacity_coverage_hash: str | None = Field(default=None, min_length=64, max_length=64)
    phase1e_program_date_request_ref: AdvisoryImmutableArtifactRef | None = None
    phase1e_program_date_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    identity_readiness: ProgramIdentityReadiness
    source_readiness: ProgramSourceReadiness
    capacity_status: ProgramCapacityStatus
    plan_readiness: ProgramPlanReadiness
    missing_slots: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    program_input_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "manifest_sha256",
        "historical_program_run_hash",
        "phase0a_audit_hash",
        "handoff_readiness_hash",
        "handoff_bundle_hash",
        "source_requirement_set_hash",
        "source_resolution_receipt_hash",
        "capacity_program_workload_hash",
        "capacity_coverage_hash",
        "phase1e_program_date_request_hash",
        "program_input_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1EProgramInputUnit":
        for field_name in (
            "historical_program_run",
            "phase0a_audit",
            "handoff_readiness",
            "handoff_bundle",
            "source_resolution_receipt",
        ):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
            )
        for field_name, artifact_kind in (
            ("source_requirement_set", O4ArtifactKind.SOURCE_REQUIREMENT_SET),
            ("capacity_program_workload", O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD),
            ("capacity_coverage", O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE),
            ("phase1e_program_date_request", O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST),
        ):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
                expected_artifact_kind=artifact_kind,
            )
        missing = sorted_unique(self.missing_slots, field_name="missing_slots") if self.missing_slots else ()
        reasons = sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        blocked = (
            self.identity_readiness is ProgramIdentityReadiness.BLOCKED
            or self.source_readiness is ProgramSourceReadiness.BLOCKED
            or self.capacity_status is ProgramCapacityStatus.INSUFFICIENT
        )
        expected = (
            ProgramPlanReadiness.BLOCKED
            if blocked
            else ProgramPlanReadiness.IDENTITY_PENDING
            if self.identity_readiness is not ProgramIdentityReadiness.COMPLETE
            else ProgramPlanReadiness.IDENTITY_COMPLETE_SOURCE_PENDING
            if self.source_readiness is not ProgramSourceReadiness.READY
            else ProgramPlanReadiness.SOURCE_READY_CAPACITY_PARTIAL
            if self.capacity_status is not ProgramCapacityStatus.MEASURED
            else ProgramPlanReadiness.FULL_READY
        )
        if self.plan_readiness is not expected:
            raise ValueError("plan_readiness does not match Program identity/source/capacity state")
        if expected is ProgramPlanReadiness.FULL_READY:
            required_refs = (
                self.historical_program_run_ref,
                self.phase0a_audit_ref,
                self.handoff_readiness_ref,
                self.handoff_bundle_ref,
                self.source_requirement_set_ref,
                self.source_resolution_receipt_ref,
                self.capacity_program_workload_ref,
                self.capacity_coverage_ref,
                self.phase1e_program_date_request_ref,
            )
            if not all(required_refs) or missing or reasons:
                raise ValueError("FULL_READY Program input requires the complete immutable evidence closure")
        elif not missing:
            raise ValueError("non-ready Program input requires exact missing_slots")
        if expected is ProgramPlanReadiness.BLOCKED and not reasons:
            raise ValueError("BLOCKED Program input requires stable reason_codes")
        object.__setattr__(self, "missing_slots", missing)
        object.__setattr__(self, "reason_codes", reasons)
        self.close_hash()
        return self


class Phase1ERealInputBundle(HashClosedContract):
    hash_field: ClassVar[str] = "input_bundle_hash"
    schema_version: Literal[PHASE1E_REAL_INPUT_BUNDLE_SCHEMA_VERSION] = PHASE1E_REAL_INPUT_BUNDLE_SCHEMA_VERSION
    build_request_ref: AdvisoryImmutableArtifactRef
    build_request_hash: str = Field(min_length=64, max_length=64)
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    phase0a_policy_registry_ref: AdvisoryImmutableArtifactRef
    phase0a_policy_registry_hash: str = Field(min_length=64, max_length=64)
    source_query_registry_ref: AdvisoryImmutableArtifactRef
    source_query_registry_hash: str = Field(min_length=64, max_length=64)
    calendar_registry_ref: AdvisoryImmutableArtifactRef
    calendar_registry_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_ref: AdvisoryImmutableArtifactRef
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    partition_policy_ref: AdvisoryImmutableArtifactRef
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    store_backend_policy_ref: AdvisoryImmutableArtifactRef
    store_backend_policy_hash: str = Field(min_length=64, max_length=64)
    capacity_policy_ref: AdvisoryImmutableArtifactRef
    capacity_policy_hash: str = Field(min_length=64, max_length=64)
    phase1e_artifact_store_policy_ref: AdvisoryImmutableArtifactRef
    phase1e_artifact_store_policy_hash: str = Field(min_length=64, max_length=64)
    source_mapping_registry_ref: AdvisoryImmutableArtifactRef
    source_mapping_registry_hash: str = Field(min_length=64, max_length=64)
    source_requirement_registry_ref: AdvisoryImmutableArtifactRef | None = None
    source_requirement_registry_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_request_ref: AdvisoryImmutableArtifactRef | None = None
    capacity_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_receipt_ref: AdvisoryImmutableArtifactRef | None = None
    capacity_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    phase1e_revalidation_batch_request_ref: AdvisoryImmutableArtifactRef | None = None
    phase1e_revalidation_batch_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    program_inputs: tuple[Phase1EProgramInputUnit, ...] = Field(min_length=1)
    counts_by_identity_readiness: dict[str, int]
    counts_by_source_readiness: dict[str, int]
    counts_by_capacity_status: dict[str, int]
    counts_by_plan_readiness: dict[str, int]
    aggregate_readiness: AggregateInputReadiness
    dependency_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    input_bundle_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "build_request_hash",
        "target_database_identity_hash",
        "phase0a_policy_registry_hash",
        "source_query_registry_hash",
        "calendar_registry_hash",
        "label_policy_bundle_hash",
        "partition_policy_hash",
        "store_backend_policy_hash",
        "capacity_policy_hash",
        "phase1e_artifact_store_policy_hash",
        "source_mapping_registry_hash",
        "source_requirement_registry_hash",
        "capacity_request_hash",
        "capacity_receipt_hash",
        "phase1e_revalidation_batch_request_hash",
        "dependency_closure_hash",
        "input_bundle_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1ERealInputBundle":
        _validate_ref_hash(
            ref=self.build_request_ref,
            semantic_hash=self.build_request_hash,
            field_name="build_request",
            expected_artifact_kind=O4ArtifactKind.REAL_INPUT_BUILD_REQUEST,
        )
        for field_name in (
            "phase0a_policy_registry",
            "source_query_registry",
            "calendar_registry",
            "label_policy_bundle",
            "partition_policy",
            "store_backend_policy",
            "phase1e_artifact_store_policy",
        ):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
            )
        for field_name, artifact_kind in (
            ("capacity_policy", O4ArtifactKind.CAPACITY_POLICY),
            ("source_mapping_registry", O4ArtifactKind.SOURCE_MAPPING_REGISTRY),
            ("source_requirement_registry", O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY),
            ("capacity_request", O4ArtifactKind.CAPACITY_REQUEST),
            ("capacity_receipt", O4ArtifactKind.CAPACITY_RECEIPT),
            ("phase1e_revalidation_batch_request", O4ArtifactKind.PHASE1E_BATCH_REQUEST),
        ):
            _validate_ref_hash(
                ref=getattr(self, f"{field_name}_ref"),
                semantic_hash=getattr(self, f"{field_name}_hash"),
                field_name=field_name,
                expected_artifact_kind=artifact_kind,
            )
        programs = tuple(sorted(self.program_inputs, key=lambda item: (item.program_id, item.decision_trade_date)))
        identities = tuple((item.program_id, item.decision_trade_date) for item in programs)
        if len(identities) != len(set(identities)):
            raise ValueError("input bundle must contain one unit per Program/date")

        expected_identity = _enum_counts(programs, "identity_readiness")
        expected_source = _enum_counts(programs, "source_readiness")
        expected_capacity = _enum_counts(programs, "capacity_status")
        expected_plan = _enum_counts(programs, "plan_readiness")
        if self.counts_by_identity_readiness != expected_identity:
            raise ValueError("counts_by_identity_readiness differs from Program inputs")
        if self.counts_by_source_readiness != expected_source:
            raise ValueError("counts_by_source_readiness differs from Program inputs")
        if self.counts_by_capacity_status != expected_capacity:
            raise ValueError("counts_by_capacity_status differs from Program inputs")
        if self.counts_by_plan_readiness != expected_plan:
            raise ValueError("counts_by_plan_readiness differs from Program inputs")

        plan_states = {item.plan_readiness for item in programs}
        expected_aggregate = (
            AggregateInputReadiness.ALL_FULL_READY
            if plan_states == {ProgramPlanReadiness.FULL_READY}
            else AggregateInputReadiness.BLOCKED
            if plan_states == {ProgramPlanReadiness.BLOCKED}
            else AggregateInputReadiness.ALL_PENDING
            if plan_states <= {
                ProgramPlanReadiness.IDENTITY_PENDING,
                ProgramPlanReadiness.IDENTITY_COMPLETE_SOURCE_PENDING,
            }
            else AggregateInputReadiness.MIXED
        )
        if self.aggregate_readiness is not expected_aggregate:
            raise ValueError("aggregate_readiness differs from independent Program readiness")
        if any(item.source_readiness is not ProgramSourceReadiness.NOT_EVALUATED for item in programs):
            if self.source_requirement_registry_ref is None:
                raise ValueError("evaluated Program source state requires the batch source requirement registry")
        if any(item.capacity_status is not ProgramCapacityStatus.NOT_MEASURED for item in programs):
            if self.capacity_request_ref is None or self.capacity_receipt_ref is None:
                raise ValueError("measured Program capacity state requires the batch capacity request and receipt")
        if any(item.plan_readiness is ProgramPlanReadiness.FULL_READY for item in programs):
            if self.phase1e_revalidation_batch_request_ref is None:
                raise ValueError("FULL_READY Program input requires the Phase 1E batch request artifact")

        dependency_refs = [
            self.build_request_ref,
            self.phase0a_policy_registry_ref,
            self.source_query_registry_ref,
            self.calendar_registry_ref,
            self.label_policy_bundle_ref,
            self.partition_policy_ref,
            self.store_backend_policy_ref,
            self.capacity_policy_ref,
            self.phase1e_artifact_store_policy_ref,
            self.source_mapping_registry_ref,
            self.source_requirement_registry_ref,
            self.capacity_request_ref,
            self.capacity_receipt_ref,
            self.phase1e_revalidation_batch_request_ref,
        ]
        dependency_hash = canonical_json_sha256(
            {
                "artifact_semantic_hashes": sorted(ref.semantic_hash for ref in dependency_refs if ref is not None),
                "program_input_hashes": [str(item.program_input_hash) for item in programs],
            }
        )
        if self.dependency_closure_hash is not None and self.dependency_closure_hash != dependency_hash:
            raise ValueError("dependency_closure_hash differs from the immutable input closure")
        object.__setattr__(self, "program_inputs", programs)
        object.__setattr__(self, "dependency_closure_hash", dependency_hash)
        self.close_hash()
        return self


def _enum_counts(items: tuple[Phase1EProgramInputUnit, ...], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = getattr(item, field_name).value
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


StoredOnboardingModel = (
    RealDevOnboardingRequest
    | RealDevOnboardingInventoryQuery
    | RealDevOnboardingInventoryReceipt
    | PortableAdvisoryEvidenceBundle
)


def database_identity_hash(identity: DatabaseIdentity) -> str:
    return canonical_json_sha256(identity.canonical_payload())
