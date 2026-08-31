"""Strict HTTP contracts for Advisory Phase 1R historical-range research."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeOutcomeRevisionReason,
)


class R5StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExistingProgramInput(R5StrictModel):
    source_kind: Literal["EXISTING_PROGRAM"]
    program_id: str = Field(min_length=1, max_length=160)
    expected_program_version: int = Field(ge=1)
    expected_binding_version_id: str = Field(min_length=1, max_length=160)


class ResearchProgramInput(R5StrictModel):
    source_kind: Literal["RESEARCH_PROGRAM_SPEC"]
    program_name: str = Field(min_length=1, max_length=200)
    package_id: str = Field(min_length=1, max_length=160)
    target_count: int = Field(ge=1, le=100)
    review_policy: dict[str, Any] = Field(default_factory=dict)
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    entry_price_basis: str = Field(default="next_open_executable", min_length=1, max_length=80)
    exit_price_basis: str = Field(default="next_open_executable", min_length=1, max_length=80)
    style_profile_ref: str | None = Field(default=None, min_length=1, max_length=500)
    style_profile_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_style_identity(self) -> "ResearchProgramInput":
        if (self.style_profile_ref is None) != (self.style_profile_hash is None):
            raise ValueError("style_profile_ref and style_profile_hash must be supplied together")
        return self


HistoricalRangeProgramInput = ExistingProgramInput | ResearchProgramInput


class HistoricalRangeCreateRequest(R5StrictModel):
    program_specs: list[HistoricalRangeProgramInput] = Field(min_length=1)
    start_trade_date: date
    end_trade_date: date

    @model_validator(mode="after")
    def validate_dates(self) -> "HistoricalRangeCreateRequest":
        if self.start_trade_date > self.end_trade_date:
            raise ValueError("start_trade_date must not be after end_trade_date")
        return self


class HistoricalRangeCommandRequest(R5StrictModel):
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    expected_row_version: int = Field(ge=1)


class HistoricalRangeRefreshOutcomesRequest(HistoricalRangeCommandRequest):
    label_as_of_trade_date: date
    range_run_ids: list[str] = Field(default_factory=list)
    horizons: list[int] = Field(min_length=1)
    requested_outcome_logical_ids: list[str] = Field(default_factory=list)
    correction_reason: HistoricalRangeOutcomeRevisionReason | None = None
    correction_evidence_ref: HistoricalRangeArtifactRefV1 | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> "HistoricalRangeRefreshOutcomesRequest":
        if self.range_run_ids != sorted(set(self.range_run_ids)):
            raise ValueError("range_run_ids must be sorted and unique")
        if self.horizons != sorted(set(self.horizons)) or any(item < 1 for item in self.horizons):
            raise ValueError("horizons must be sorted, unique, and positive")
        if self.requested_outcome_logical_ids != sorted(set(self.requested_outcome_logical_ids)):
            raise ValueError("requested_outcome_logical_ids must be sorted and unique")
        if any(
            not item.strip() or item != item.strip() or len(item) > 160
            for item in self.requested_outcome_logical_ids
        ):
            raise ValueError("requested_outcome_logical_ids contain an invalid identity")
        if (self.correction_reason is None) != (self.correction_evidence_ref is None):
            raise ValueError("outcome correction reason/evidence must be supplied together")
        if self.correction_reason is not None and self.correction_reason not in {
            HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
            HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        }:
            raise ValueError("outcome correction reason must be SOURCE or CALCULATION correction")
        return self


class HistoricalRangeBuildBridgeRequest(HistoricalRangeCommandRequest):
    range_run_ids: list[str] = Field(default_factory=list)
    requested_horizons: list[int] = Field(min_length=1)
    requested_maturity_statuses: list[Literal["COMPLETE", "CENSORED", "TERMINAL"]] = Field(
        min_length=1
    )

    @model_validator(mode="after")
    def validate_scope(self) -> "HistoricalRangeBuildBridgeRequest":
        if self.range_run_ids != sorted(set(self.range_run_ids)):
            raise ValueError("range_run_ids must be sorted and unique")
        if self.requested_horizons != sorted(set(self.requested_horizons)) or any(
            item < 1 for item in self.requested_horizons
        ):
            raise ValueError("requested_horizons must be sorted, unique, and positive")
        if self.requested_maturity_statuses != sorted(set(self.requested_maturity_statuses)):
            raise ValueError("requested_maturity_statuses must be sorted and unique")
        return self


class HistoricalRangeErrorDetail(R5StrictModel):
    error_code: str = "ADVISORY_HISTORICAL_RANGE_ERROR"
    reason_code: str
    message: str
    retryable: bool = False
    context: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str


class HistoricalRangePage(R5StrictModel):
    limit: int = Field(ge=1, le=500)
    next_cursor: str | None = None
    has_more: bool = False


class HistoricalRangeEnvelope(R5StrictModel):
    ok: Literal[True] = True
    data: dict[str, Any]
    page: HistoricalRangePage | None = None


TERMINAL_OPERATION_STATUSES = frozenset({"COMPLETED", "FAILED"})
DISPATCHABLE_OPERATION_STATUSES = frozenset({"QUEUED", "WAITING_INPUT", "RETRYABLE_FAILED"})


def json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    return value
