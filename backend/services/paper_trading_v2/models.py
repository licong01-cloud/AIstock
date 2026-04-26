"""Paper Trading v2 persistent domain models."""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.models import AccountSnapshot, Fill, Order, OrderEvent, PositionLot, RunStatus


class PortfolioStatus(str, Enum):
    DRAFT = "DRAFT"
    READY = "READY"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"
    RETIRED = "RETIRED"


class PaperPortfolio(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str = Field(default_factory=lambda: f"paper_{uuid4().hex}")
    portfolio_name: str
    package_id: str
    manifest_sha256: str
    frozen_manifest: StrategyPackageManifest
    initial_cash: float = Field(gt=0)
    start_date: date
    data_source: MinuteDataSource
    fee_policy: dict[str, Any] = Field(default_factory=dict)
    risk_policy: dict[str, Any] = Field(default_factory=dict)
    execution_policy: dict[str, Any] = Field(default_factory=dict)
    status: PortfolioStatus = PortfolioStatus.READY
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _frozen_manifest_matches(self) -> "PaperPortfolio":
        if self.frozen_manifest.package_id != self.package_id:
            raise ValueError("frozen_manifest package_id must match portfolio package_id")
        if self.frozen_manifest.manifest_sha256 != self.manifest_sha256:
            raise ValueError("frozen_manifest manifest_sha256 must match portfolio manifest_sha256")
        return self


class PaperRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(default_factory=lambda: f"prun_{uuid4().hex}")
    portfolio_id: str
    trade_date: date
    status: RunStatus = RunStatus.PENDING
    data_source: MinuteDataSource
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    error: dict[str, Any] | None = None


class ExecutionPolicyActivationStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"


class PaperExecutionPolicyActivation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    activation_id: str = Field(default_factory=lambda: f"epact_{uuid4().hex}")
    portfolio_id: str
    trade_date: date
    policy_id: str
    policy_sha256: str
    policy_name: str | None = None
    policy_json: dict[str, Any] = Field(default_factory=dict)
    status: ExecutionPolicyActivationStatus = ExecutionPolicyActivationStatus.ACTIVE
    activated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    activated_by: str | None = None
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    superseded_at: datetime | None = None


class PaperDayRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio: PaperPortfolio
    run: PaperRun
    orders: list[Order]
    fills: list[Fill]
    events: list[OrderEvent]
    positions: list[PositionLot]
    account_snapshot: AccountSnapshot


class PaperReplayDayResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_date: date
    run_id: str
    status: RunStatus
    nav: float
    order_count: int
    fill_count: int
    position_count: int


class PaperReplayResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    start_date: date
    end_date: date
    data_source: MinuteDataSource
    trading_days: list[date]
    day_results: list[PaperReplayDayResult]
    reset_audit: dict[str, Any] | None = None


class PaperReadinessCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    check_name: str
    status: Literal["passed"] = "passed"
    context: dict[str, Any] = Field(default_factory=dict)


class PaperDayReadinessResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    trade_date: date
    data_source: MinuteDataSource
    checks: list[PaperReadinessCheck]
    raw_candidate_count: int
    tradable_candidate_count: int
    excluded_candidate_count: int
    target_count: int
    order_intent_count: int
    checked_symbols: list[str]
    runtime_config_keys: list[str]
