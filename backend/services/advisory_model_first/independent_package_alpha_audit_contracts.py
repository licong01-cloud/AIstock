from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    N1_DATASET_IDENTITY,
    N1_DATA_CUTOFF,
    N1_DECISION_END,
    N1_DECISION_START,
    N1_WINDOW_ID,
    Tier1InferencePolicyV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
REQUEST_SCHEMA_VERSION = "frozen_advisory_independent_package_alpha_audit_request_v1"
BUNDLE_SCHEMA_VERSION = "advisory_independent_package_alpha_audit_bundle_v1"
RECEIPT_SCHEMA_VERSION = "advisory_independent_package_alpha_audit_receipt_v1"
EXPERIMENT_ID = "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT"
PARENT_LINEAGE = ("ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",)

CURRENT_PARENT_ARM_ID = "CURRENT_IC_PARENT"
PKG_378_ARM_ID = "PKG_378EB9"
PKG_5A5_ARM_ID = "PKG_5A5CCB"
PKG_B668_ARM_ID = "PKG_B668F8"
ARM_IDS = (
    CURRENT_PARENT_ARM_ID,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    PKG_B668_ARM_ID,
)

PACKAGE_378_ID = "pkg_378eb9c91e104c64935404e257e932ee"
PACKAGE_5A5_ID = "pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27"
PACKAGE_B668_ID = "pkg_b668f8a633c44b72a5d557a2cb8970e3"
PACKAGE_IDS = (PACKAGE_378_ID, PACKAGE_5A5_ID, PACKAGE_B668_ID)
PACKAGE_ARM_IDS = (PKG_378_ARM_ID, PKG_5A5_ARM_ID, PKG_B668_ARM_ID)
PACKAGE_STATUSES = ("BACKTEST_APPROVED", "PAPER_ENABLED", "SELECTION_ENABLED")
FACTOR_CLOSURE_57 = "977c29e8e328d393bd8235821070e19a96bb23ef0434430c5437621261fb542c"
FACTOR_CLOSURE_50 = "f19cf6214cb0d38f75736550698916097b99c2ac1ddd28dc28e7a464558663a4"
FACTOR_GROUP_CLOSURES = (FACTOR_CLOSURE_57, FACTOR_CLOSURE_50)
CAUSALITY_ANCHORS = (date(2024, 7, 4), date(2025, 4, 22), date(2026, 2, 2))

RESOURCE_MAX_RSS_BYTES = 16 * 1024**3
RESOURCE_MAX_TEMP_BYTES = 32 * 1024**3
# Retained only so historical frozen request JSON remains readable. Execution
# ignores this deprecated value; new requests default to ``None``.
RESOURCE_MAX_WALL_SECONDS = 8 * 60 * 60
SCORE_PARITY_ATOL = 1e-6
RANK_PARITY_MIN_SPEARMAN = 0.999999


class WorkspaceFileDescriptorV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    relative_path: str = Field(min_length=1)
    sha256: str = Field(pattern=SHA256_PATTERN)
    size_bytes: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_relative_path(self) -> "WorkspaceFileDescriptorV1":
        normalized = self.relative_path.replace("\\", "/")
        if normalized.startswith("/") or normalized.startswith("../") or "/../" in normalized:
            raise ValueError("workspace descriptor path must stay relative to its root")
        if normalized != self.relative_path:
            raise ValueError("workspace descriptor path must use canonical forward slashes")
        return self


class FrozenPackageAuditArmV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    arm_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    package_status: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    package_snapshot_ref: EvidenceReferenceV1
    alpha_mode: Literal["single_alpha"] = "single_alpha"
    factor_count: int = Field(gt=0)
    factor_closure_sha256: str = Field(pattern=SHA256_PATTERN)
    model_closure_sha256: str = Field(pattern=SHA256_PATTERN)
    workspace_root: str = Field(min_length=1)
    workspace_files: tuple[WorkspaceFileDescriptorV1, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_workspace(self) -> "FrozenPackageAuditArmV1":
        paths = tuple(item.relative_path for item in self.workspace_files)
        if len(paths) != len(set(paths)) or tuple(sorted(paths)) != paths:
            raise ValueError("workspace file descriptors must be unique and sorted")
        required = {"factor_order.json", "manifest.json", "strategy_package_factor_entry.py", "model/params.pkl"}
        if not required.issubset(paths):
            raise ValueError("frozen package workspace is missing required inference files")
        if self.package_snapshot_ref.role != f"n2b_package_snapshot__{self.arm_id}":
            raise ValueError("package snapshot evidence role differs from arm identity")
        return self


class AdvisoryIndependentPackageAlphaAuditRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal[REQUEST_SCHEMA_VERSION] = REQUEST_SCHEMA_VERSION
    request_id: str = Field(pattern=r"^advpkgareq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    study_type: Literal[ResearchStudyType.ORACLE_DIAGNOSTIC] = ResearchStudyType.ORACLE_DIAGNOSTIC
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY

    n0_completion_ref: EvidenceReferenceV1
    n0_completion_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    research_window_contract_ref: EvidenceReferenceV1
    research_window_contract_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_request_ref: EvidenceReferenceV1
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_manifest_ref: EvidenceReferenceV1
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2a_request_ref: EvidenceReferenceV1
    n2a_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n2a_bundle_path: str = Field(min_length=1)
    n2a_bundle_manifest_ref: EvidenceReferenceV1
    n2a_bundle_id: str = Field(pattern=SHA256_PATTERN)

    registry_path: str = Field(min_length=1)
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    current_parent_package_id: str = Field(min_length=1)
    current_parent_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_dataset_bundle_id: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    pit_spans_sha256: str = Field(pattern=SHA256_PATTERN)
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)

    arm_ids: tuple[str, ...] = ARM_IDS
    packages: tuple[FrozenPackageAuditArmV1, ...]
    factor_group_closures: tuple[str, ...] = FACTOR_GROUP_CLOSURES
    causality_anchor_dates: tuple[date, ...] = CAUSALITY_ANCHORS
    score_parity_atol: Literal[SCORE_PARITY_ATOL] = SCORE_PARITY_ATOL
    rank_parity_min_spearman: Literal[RANK_PARITY_MIN_SPEARMAN] = RANK_PARITY_MIN_SPEARMAN

    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    prediction_store_root: str = Field(min_length=1)
    output_root: str = Field(min_length=1)
    decision_date_start: date = N1_DECISION_START
    decision_date_end: date = N1_DECISION_END
    data_cutoff: date = N1_DATA_CUTOFF
    window_id: Literal[N1_WINDOW_ID] = N1_WINDOW_ID
    dataset_identity: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    inference_policy: Tier1InferencePolicyV1 = Field(default_factory=Tier1InferencePolicyV1)
    resource_max_rss_bytes: Literal[RESOURCE_MAX_RSS_BYTES] = RESOURCE_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[RESOURCE_MAX_TEMP_BYTES] = RESOURCE_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[RESOURCE_MAX_WALL_SECONDS] | None = None

    @model_validator(mode="after")
    def validate_frozen_identity(self) -> "AdvisoryIndependentPackageAlphaAuditRequestV1":
        if (
            self.decision_date_start != N1_DECISION_START
            or self.decision_date_end != N1_DECISION_END
            or self.data_cutoff != N1_DATA_CUTOFF
        ):
            raise ValueError("independent package audit must use the exact N1 development dates")
        if self.arm_ids != ARM_IDS:
            raise ValueError("independent package audit arm roster/order drifted")
        if self.factor_group_closures != FACTOR_GROUP_CLOSURES:
            raise ValueError("factor closure group order drifted")
        if self.causality_anchor_dates != CAUSALITY_ANCHORS:
            raise ValueError("causality anchor roster/order drifted")
        if tuple(item.arm_id for item in self.packages) != PACKAGE_ARM_IDS:
            raise ValueError("package arm order drifted")
        if tuple(item.package_id for item in self.packages) != PACKAGE_IDS:
            raise ValueError("package roster/order drifted")
        if tuple(item.package_status for item in self.packages) != PACKAGE_STATUSES:
            raise ValueError("package lifecycle status roster drifted")
        if tuple(item.factor_count for item in self.packages) != (57, 57, 50):
            raise ValueError("package factor counts drifted")
        if tuple(item.factor_closure_sha256 for item in self.packages) != (
            FACTOR_CLOSURE_57,
            FACTOR_CLOSURE_57,
            FACTOR_CLOSURE_50,
        ):
            raise ValueError("package factor asset closures drifted")
        if len({item.package_id for item in self.packages}) != len(self.packages):
            raise ValueError("package audit roster contains duplicates")
        if self.n1_request_ref.role != "n1_frozen_request":
            raise ValueError("N1 request evidence role is invalid")
        if self.n1_bundle_manifest_ref.role != "n1_formal_bundle_manifest":
            raise ValueError("N1 bundle evidence role is invalid")
        if self.n2a_request_ref.role != "n2a_frozen_request":
            raise ValueError("N2-A request evidence role is invalid")
        if self.n2a_bundle_manifest_ref.role != "n2a_formal_bundle_manifest":
            raise ValueError("N2-A bundle evidence role is invalid")
        for bundle_path, manifest_ref, label in (
            (self.n1_bundle_path, self.n1_bundle_manifest_ref, "N1"),
            (self.n2a_bundle_path, self.n2a_bundle_manifest_ref, "N2-A"),
        ):
            expected_uri = bundle_path.rstrip("/") + "/manifest.json"
            if manifest_ref.artifact_uri.replace("\\", "/") != expected_uri.replace("\\", "/"):
                raise ValueError(f"{label} bundle manifest evidence path differs from its bundle")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected:
            raise ValueError("independent package audit request_sha256 mismatch")
        if self.request_id != f"advpkgareq_{expected[:24]}":
            raise ValueError("independent package audit request_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class AdvisoryIndependentPackageAlphaAuditReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal[RECEIPT_SCHEMA_VERSION] = RECEIPT_SCHEMA_VERSION
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    prediction_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    causality_parity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    arm_ids: tuple[str, ...]
    decision_date_count: int = Field(gt=0)
    signal_row_count_by_arm: dict[str, int]
    evaluable_recall_day_count_by_arm: dict[str, int]
    evaluable_top5_day_count_by_arm: dict[str, int]
    planned_trial_count: Literal[0] = 0
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    result_class: Literal[ResearchResultClass.EXPLORATORY] = ResearchResultClass.EXPLORATORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    sealed_holdout_accessed: Literal[False] = False
    runtime_eligible: Literal[False] = False
    activated: Literal[False] = False
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryIndependentPackageAlphaAuditReceiptV1":
        if self.arm_ids != ARM_IDS:
            raise ValueError("independent package audit receipt arm roster drifted")
        for field_name in (
            "signal_row_count_by_arm",
            "evaluable_recall_day_count_by_arm",
            "evaluable_top5_day_count_by_arm",
        ):
            counts = getattr(self, field_name)
            if set(counts) != set(ARM_IDS) or any(value < 0 for value in counts.values()):
                raise ValueError(f"{field_name} must contain non-negative counts for every arm")
        if any(value <= 0 for value in self.signal_row_count_by_arm.values()):
            raise ValueError("every arm must contain at least one signal row")
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected:
            raise ValueError("independent package audit receipt_sha256 mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_sha256", "created_at"})


def build_independent_package_alpha_audit_request(
    **values: Any,
) -> AdvisoryIndependentPackageAlphaAuditRequestV1:
    payload = dict(values)
    payload.setdefault("schema_version", REQUEST_SCHEMA_VERSION)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload.setdefault("arm_ids", ARM_IDS)
    payload.setdefault("factor_group_closures", FACTOR_GROUP_CLOSURES)
    payload.setdefault("causality_anchor_dates", CAUSALITY_ANCHORS)
    functional_fields = set(AdvisoryIndependentPackageAlphaAuditRequestV1.model_fields) - {
        "request_id",
        "request_sha256",
        "created_at",
    }
    functional = {key: value for key, value in payload.items() if key in functional_fields}
    normalized = AdvisoryIndependentPackageAlphaAuditRequestV1.model_construct(
        request_id="advpkgareq_" + "0" * 24,
        request_sha256="0" * 64,
        created_at=payload["created_at"],
        **functional,
    ).model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})
    digest = canonical_json_sha256(normalized)
    payload["request_sha256"] = digest
    payload["request_id"] = f"advpkgareq_{digest[:24]}"
    return AdvisoryIndependentPackageAlphaAuditRequestV1.model_validate(payload)


def build_independent_package_alpha_audit_receipt(
    **values: Any,
) -> AdvisoryIndependentPackageAlphaAuditReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", RECEIPT_SCHEMA_VERSION)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    functional_fields = set(AdvisoryIndependentPackageAlphaAuditReceiptV1.model_fields) - {
        "receipt_sha256",
        "created_at",
    }
    functional = {key: value for key, value in payload.items() if key in functional_fields}
    normalized = AdvisoryIndependentPackageAlphaAuditReceiptV1.model_construct(
        receipt_sha256="0" * 64,
        created_at=payload["created_at"],
        **functional,
    ).model_dump(mode="json", exclude={"receipt_sha256", "created_at"})
    payload["receipt_sha256"] = canonical_json_sha256(normalized)
    return AdvisoryIndependentPackageAlphaAuditReceiptV1.model_validate(payload)


__all__ = [
    "ARM_IDS",
    "BUNDLE_SCHEMA_VERSION",
    "CAUSALITY_ANCHORS",
    "CURRENT_PARENT_ARM_ID",
    "EXPERIMENT_ID",
    "FACTOR_CLOSURE_50",
    "FACTOR_CLOSURE_57",
    "FACTOR_GROUP_CLOSURES",
    "PACKAGE_378_ID",
    "PACKAGE_5A5_ID",
    "PACKAGE_ARM_IDS",
    "PACKAGE_B668_ID",
    "PACKAGE_IDS",
    "PARENT_LINEAGE",
    "PKG_378_ARM_ID",
    "PKG_5A5_ARM_ID",
    "PKG_B668_ARM_ID",
    "RESOURCE_MAX_RSS_BYTES",
    "RESOURCE_MAX_TEMP_BYTES",
    "RESOURCE_MAX_WALL_SECONDS",
    "AdvisoryIndependentPackageAlphaAuditReceiptV1",
    "AdvisoryIndependentPackageAlphaAuditRequestV1",
    "FrozenPackageAuditArmV1",
    "WorkspaceFileDescriptorV1",
    "build_independent_package_alpha_audit_receipt",
    "build_independent_package_alpha_audit_request",
]
