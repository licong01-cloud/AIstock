"""Selection Center domain models."""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.execution_algos.board_lot import board_lot_rule


class SelectionMode(str, Enum):
    SINGLE_PACKAGE = "single_package"
    INTERSECTION = "intersection"
    UNION = "union"
    WEIGHTED_FUSION = "weighted_fusion"


class SelectionRunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    VALID_NO_CANDIDATE = "VALID_NO_CANDIDATE"


class SelectionCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    score: float
    rank: int = Field(gt=0)
    target_weight: float | None = Field(default=None, gt=0)
    target_quantity: int | None = Field(default=None, ge=0)
    reference_price: float | None = Field(default=None, gt=0)
    stock_name: str | None = None
    selection_entry_price: float | None = Field(default=None, gt=0)
    selection_entry_price_source: str | None = None
    selection_entry_price_time: str | None = None
    previous_close: float | None = Field(default=None, gt=0)
    volume: float | None = Field(default=None, ge=0)
    current_price: float | None = Field(default=None, gt=0)
    current_price_source: str | None = None
    current_price_time: str | None = None
    component_scores: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_required(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("symbol is required")
        return value

    @model_validator(mode="after")
    def _target_quantity_board_lot(self) -> "SelectionCandidate":
        if self.target_quantity is not None:
            _validate_target_quantity(self.symbol, self.target_quantity, label="target_quantity", require_buyable=True)
        return self


class SelectionExclusion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    score: float
    rank: int = Field(gt=0)
    reason: str
    source: str
    context: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _excluded_symbol_required(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("symbol is required")
        return value


class SignalSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_id: str = Field(default_factory=lambda: f"sig_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    candidates: list[SelectionCandidate]
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _candidate_rules(self) -> "SignalSnapshot":
        if not self.candidates and not self.valid_no_candidate:
            raise ValueError("signal snapshot requires candidates or valid_no_candidate")
        if self.valid_no_candidate and not self.no_candidate_reason:
            raise ValueError("valid_no_candidate requires no_candidate_reason")
        return self


class TargetPosition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    target_quantity: int = Field(ge=0)
    target_weight: float | None = Field(default=None, gt=0)
    reference_price: float | None = Field(default=None, gt=0)
    score: float
    rank: int = Field(gt=0)
    reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reference_price_required_for_nonzero_target(self) -> "TargetPosition":
        _validate_target_quantity(
            self.symbol,
            self.target_quantity,
            label="target_quantity",
            require_buyable="buy" in str(self.reason or "").lower(),
        )
        if self.target_quantity > 0 and self.reference_price is None:
            raise ValueError("reference_price is required for non-zero target positions")
        return self


def _validate_target_quantity(symbol: str, quantity: int, *, label: str, require_buyable: bool) -> None:
    if quantity == 0:
        return
    min_qty, increment = board_lot_rule(symbol)
    if quantity >= min_qty and quantity % increment == 0:
        return
    if not require_buyable and 0 < quantity < min_qty:
        return
    raise ValueError(
        f"{label} must follow board-lot rules for {symbol}: "
        f"min_qty={min_qty}, increment={increment}"
    )


class SelectionRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(default_factory=lambda: f"sel_{uuid4().hex}")
    mode: SelectionMode
    trade_date: date
    data_source: str
    package_ids: list[str]
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    status: SelectionRunStatus = SelectionRunStatus.RUNNING
    package_results: dict[str, list[SelectionCandidate]] = Field(default_factory=dict)
    aggregate_results: list[SelectionCandidate] = Field(default_factory=list)
    excluded_results: dict[str, list[SelectionExclusion]] = Field(default_factory=dict)
    manifest_sha256_by_package: dict[str, str] = Field(default_factory=dict)
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None
    error: dict[str, Any] | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None


class SelectionPaperPortfolioLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    link_id: int | None = None
    run_id: str
    portfolio_id: str
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    start_date: date
    initial_cash: float
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
