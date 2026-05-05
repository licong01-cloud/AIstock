"""Model freshness metadata for Strategy Packages."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ModelStalenessStatus(str, Enum):
    CURRENT = "CURRENT"
    STALE = "STALE"
    STALE_INITIAL_BACKTEST_MODEL = "STALE_INITIAL_BACKTEST_MODEL"
    RETRAINING = "RETRAINING"
    RETRAIN_FAILED = "RETRAIN_FAILED"
    UNKNOWN = "UNKNOWN"


class ModelRetrainJobStatus(str, Enum):
    PENDING_CONFIRMATION = "PENDING_CONFIRMATION"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StrategyPackageModelState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    active_model_version_id: str | None = None
    train_start_date: date | None = None
    train_end_date: date | None = None
    trained_at: datetime | None = None
    last_retrain_job_id: str | None = None
    last_retrained_at: datetime | None = None
    stale_after_days: int = Field(default=30, gt=0)
    staleness_status: ModelStalenessStatus = ModelStalenessStatus.UNKNOWN
    warning: str | None = None
    last_checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("package_id")
    @classmethod
    def _package_id_required(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("package_id is required")
        return value


class ModelRetrainPreview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    job_type: str
    recommended_train_start_date: date | None
    recommended_train_end_date: date
    stale_after_days: int
    requires_manual_confirmation: bool = True
    reason: str
    config: dict[str, Any] = Field(default_factory=dict)


class StrategyPackageModelRetrainJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(default_factory=lambda: f"mretrain_{uuid4().hex}")
    package_id: str
    job_type: str
    requested_train_start_date: date | None = None
    requested_train_end_date: date
    stale_after_days: int = Field(gt=0)
    config: dict[str, Any] = Field(default_factory=dict)
    status: ModelRetrainJobStatus = ModelRetrainJobStatus.QUEUED
    requires_manual_confirmation: bool = True
    confirmed: bool = False
    status_reason: str | None = None
    error: dict[str, Any] | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @field_validator("package_id", "job_type")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value


def evaluate_model_staleness(state: StrategyPackageModelState, *, as_of_date: date) -> StrategyPackageModelState:
    if state.staleness_status in {ModelStalenessStatus.RETRAINING, ModelStalenessStatus.RETRAIN_FAILED}:
        return state.model_copy(update={"last_checked_at": datetime.now(timezone.utc)})
    if state.active_model_version_id is None and state.last_retrained_at is None:
        return state.model_copy(
            update={
                "staleness_status": ModelStalenessStatus.STALE_INITIAL_BACKTEST_MODEL,
                "warning": "strategy package still uses the original backtest model; manual retrain is recommended before paper trading",
                "last_checked_at": datetime.now(timezone.utc),
            }
        )
    if state.train_end_date is None:
        return state.model_copy(
            update={
                "staleness_status": ModelStalenessStatus.UNKNOWN,
                "warning": "model training end date is unknown",
                "last_checked_at": datetime.now(timezone.utc),
            }
        )
    age_days = (as_of_date - state.train_end_date).days
    if age_days > state.stale_after_days:
        return state.model_copy(
            update={
                "staleness_status": ModelStalenessStatus.STALE,
                "warning": f"model training data is {age_days} days old; rolling retrain is recommended",
                "last_checked_at": datetime.now(timezone.utc),
            }
        )
    return state.model_copy(
        update={
            "staleness_status": ModelStalenessStatus.CURRENT,
            "warning": None,
            "last_checked_at": datetime.now(timezone.utc),
        }
    )
