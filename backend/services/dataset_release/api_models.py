from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

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
