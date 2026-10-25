from __future__ import annotations

from datetime import date
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import Scope


class DatasetReleaseApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MonthlyReleaseRequest(DatasetReleaseApiModel):
    schema_version: Literal["dataset_release_monthly_request_v1"] = "dataset_release_monthly_request_v1"
    profile: str = Field(default="qe_hmm_full_v1", min_length=1, max_length=64)
    cutoff_policy: Literal["auto-previous-month"] = "auto-previous-month"
    scope: Scope = Scope.FULL
    candidate_only: bool = True
    preview_token: str | None = Field(
        default=None,
        min_length=70,
        max_length=96,
        pattern=r"^dsp1_[0-9]{10}_[0-9a-f]{64}$",
    )


class EmptyCommandRequest(DatasetReleaseApiModel):
    schema_version: Literal["dataset_release_command_request_v1"] = "dataset_release_command_request_v1"


class UnifiedMonthlyReleaseRequest(DatasetReleaseApiModel):
    schema_version: Literal["aistock_monthly_release_request_v1"] = "aistock_monthly_release_request_v1"
    target_cutoff: date
    product_profile: Literal["qe_hmm_full_v2"] = "qe_hmm_full_v2"
    activation_mode: Literal["prepare_only", "activate_when_ready"] = "prepare_only"
    activation_authorization_ref: str | None = Field(
        default=None,
        pattern=r"^dsauth_[0-9a-f]{32}$",
    )
    repair_authorization_refs: tuple[
        str,
        ...,
    ] = Field(default=(), max_length=128)

    @model_validator(mode="after")
    def validate_authorization_shape(self) -> "UnifiedMonthlyReleaseRequest":
        if any(re.fullmatch(r"dsauth_[0-9a-f]{32}", item) is None for item in self.repair_authorization_refs):
            raise ValueError("repair authorization reference format is invalid")
        if len(set(self.repair_authorization_refs)) != len(self.repair_authorization_refs):
            raise ValueError("repair authorization references are duplicated")
        if self.activation_mode == "activate_when_ready":
            if self.activation_authorization_ref is None:
                raise ValueError("activate_when_ready requires activation authorization")
        elif self.activation_authorization_ref is not None:
            raise ValueError("prepare_only forbids activation authorization")
        return self


class UnifiedMonthlyActionRequest(DatasetReleaseApiModel):
    schema_version: Literal["aistock_monthly_release_action_v1"] = "aistock_monthly_release_action_v1"
    authorization_ref: str = Field(pattern=r"^dsauth_[0-9a-f]{32}$")


class UnifiedMonthlyRepairInputsRequest(DatasetReleaseApiModel):
    schema_version: Literal["aistock_monthly_repair_inputs_request_v1"] = "aistock_monthly_repair_inputs_request_v1"
    inputs: dict[str, Any]


class ExistingSuccessorEvidenceRef(DatasetReleaseApiModel):
    relative_path: str = Field(min_length=1, max_length=512)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: str = Field(min_length=1, max_length=128)


class UnifiedMonthlyAdoptRequest(DatasetReleaseApiModel):
    schema_version: Literal["aistock_monthly_existing_successor_adoption_v1"] = (
        "aistock_monthly_existing_successor_adoption_v1"
    )
    target_cutoff: date
    product_profile: Literal["qe_hmm_full_v2"] = "qe_hmm_full_v2"
    candidate_root: str = Field(min_length=3, max_length=1024)
    profile_candidate: str = Field(min_length=3, max_length=1024)
    predecessor_profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    node_manifest_file_sha256: dict[
        Literal["controller", "wsl2-5080", "rdagent-node1"], str
    ]
    evidence_refs: tuple[ExistingSuccessorEvidenceRef, ...] = Field(min_length=2, max_length=32)

    @model_validator(mode="after")
    def validate_adoption_shape(self) -> "UnifiedMonthlyAdoptRequest":
        if set(self.node_manifest_file_sha256) != {
            "controller",
            "wsl2-5080",
            "rdagent-node1",
        }:
            raise ValueError("node manifest attestations must cover all release nodes")
        if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in self.node_manifest_file_sha256.values()):
            raise ValueError("node manifest attestation hash is invalid")
        if len({item.relative_path for item in self.evidence_refs}) != len(self.evidence_refs):
            raise ValueError("adoption evidence paths are duplicated")
        return self


class UnifiedMonthlyAuthorizationIssueRequest(DatasetReleaseApiModel):
    schema_version: Literal["aistock_monthly_authorization_issue_request_v1"] = (
        "aistock_monthly_authorization_issue_request_v1"
    )
    action: Literal["ACTIVATE"] = "ACTIVATE"


__all__ = (
    "DatasetReleaseApiModel",
    "EmptyCommandRequest",
    "ExistingSuccessorEvidenceRef",
    "MonthlyReleaseRequest",
    "UnifiedMonthlyActionRequest",
    "UnifiedMonthlyAdoptRequest",
    "UnifiedMonthlyAuthorizationIssueRequest",
    "UnifiedMonthlyReleaseRequest",
    "UnifiedMonthlyRepairInputsRequest",
)
