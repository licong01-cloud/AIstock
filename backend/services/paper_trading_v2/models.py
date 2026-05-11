"""Paper Trading v2 persistent domain models."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.paper_trading_v2.market_data import (
    ALLOWED_MARKET_SOURCES,
    MinuteDataSource,
    assert_broker_market_source_match,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.models import AccountSnapshot, Fill, Order, OrderEvent, PositionLot, RunStatus


# Strategy Engine design 2026-05-08 §3.6.1 (R-Q9 D1): broker_backend Literal
# kept in sync with ALLOWED_MARKET_SOURCES keys. minqmt_live is reserved for
# future live admission (main design §11) and not creatable through Paper v2
# portfolio APIs in this round.
BrokerBackendId = Literal["local_sim", "minqmt_sim"]


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
    broker_backend: BrokerBackendId = "local_sim"
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

    @model_validator(mode="after")
    def _broker_backend_matches_data_source(self) -> "PaperPortfolio":
        # Strategy Engine design §3.6.4 (R-Q9 D3): minute data source is
        # strongly bound to broker backend. Reject cross-pairing fail-fast at
        # the model layer so DB-level CHECK is never the only line of defense.
        if self.broker_backend not in ALLOWED_MARKET_SOURCES:
            raise ValueError(
                f"unknown broker_backend {self.broker_backend!r}; "
                f"allowed: {sorted(ALLOWED_MARKET_SOURCES.keys())}"
            )
        assert_broker_market_source_match(self.broker_backend, self.data_source)
        return self


ModelParamsOrigin = Literal["node", "cache", "unavailable"]


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
    # Provenance of the QE model params used for this run. Default 'node'
    # is intentional — it covers (a) existing rows post-migration and
    # (b) PaperRun construction sites that fire BEFORE the live inference
    # workspace is materialized (e.g. day_runner / live_session) and
    # subsequently UPDATE the field once inference resolves origin.
    # Cache fallback ('cache') is only legitimate when the live inference
    # call site explicitly opted in via allow_cache_fallback=True; see
    # backend/services/strategy_package/live_inference.py.
    # TODO: once update_run_model_params_origin is wired through every
    # live inference call site, consider tightening this to require an
    # explicit value at INSERT time (no model-level default).
    model_params_origin: ModelParamsOrigin = "node"


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


class RuntimeProfileStatus(str, Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class RuntimeProfileValidationStatus(str, Enum):
    VALIDATED = "VALIDATED"
    INVALID = "INVALID"


class RuntimeConfigActivationStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    CANCELLED = "CANCELLED"


class ConfigChangeType(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    ACTIVATE = "ACTIVATE"
    SUPERSEDE = "SUPERSEDE"
    RESET = "RESET"
    RETIRE = "RETIRE"


def compute_runtime_config_sha256(config_json: dict[str, Any]) -> str:
    encoded = json.dumps(
        config_json,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class PaperRuntimeProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile_id: str = Field(default_factory=lambda: f"rprof_{uuid4().hex}")
    portfolio_id: str
    package_id: str
    profile_name: str
    status: RuntimeProfileStatus = RuntimeProfileStatus.ACTIVE
    current_version_id: str | None = None
    created_by: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PaperRuntimeProfileVersion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile_version_id: str = Field(default_factory=lambda: f"rpver_{uuid4().hex}")
    profile_id: str
    version_no: int = Field(ge=1)
    config_json: dict[str, Any]
    config_sha256: str | None = None
    validation_status: RuntimeProfileValidationStatus = RuntimeProfileValidationStatus.VALIDATED
    validation_errors: list[dict[str, Any]] = Field(default_factory=list)
    created_by: str | None = None
    reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    supersedes_version_id: str | None = None

    @model_validator(mode="after")
    def _hash_matches_config(self) -> "PaperRuntimeProfileVersion":
        digest = compute_runtime_config_sha256(self.config_json)
        if self.config_sha256 is not None and self.config_sha256 != digest:
            raise ValueError("config_sha256 does not match config_json")
        object.__setattr__(self, "config_sha256", digest)
        return self


class PaperRuntimeConfigActivation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    activation_id: str = Field(default_factory=lambda: f"rcact_{uuid4().hex}")
    portfolio_id: str
    trade_date: date
    profile_version_id: str
    status: RuntimeConfigActivationStatus = RuntimeConfigActivationStatus.ACTIVE
    activated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    activated_by: str | None = None
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    superseded_at: datetime | None = None


class PaperConfigChangeAudit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_id: int | None = None
    portfolio_id: str | None = None
    package_id: str | None = None
    object_type: str
    object_id: str
    change_type: ConfigChangeType
    before_json: dict[str, Any] | None = None
    after_json: dict[str, Any] | None = None
    before_sha256: str | None = None
    after_sha256: str | None = None
    reason: str | None = None
    created_by: str | None = None
    request_id: str | None = None
    code_version: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


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


class PaperSessionMode(str, Enum):
    REPLAY_ONLY = "REPLAY_ONLY"
    LIVE_ONLY = "LIVE_ONLY"
    CATCHUP_THEN_LIVE = "CATCHUP_THEN_LIVE"


class PaperSessionStatus(str, Enum):
    CREATED = "CREATED"
    PREFLIGHTING = "PREFLIGHTING"
    REPLAYING = "REPLAYING"
    CATCHING_UP = "CATCHING_UP"
    SWITCHING_TO_LIVE = "SWITCHING_TO_LIVE"
    LIVE_RUNNING = "LIVE_RUNNING"
    LIVE_WAITING_FOR_BAR = "LIVE_WAITING_FOR_BAR"
    LIVE_WAITING_NEXT_TRADING_DAY = "LIVE_WAITING_NEXT_TRADING_DAY"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class PaperSessionPhase(str, Enum):
    HISTORICAL_REPLAY = "historical_replay"
    CURRENT_DAY_CATCHUP = "current_day_catchup"
    LIVE_INTRADAY = "live_intraday"
    DAY_FINALIZATION = "day_finalization"
    WAITING_NEXT_DAY = "waiting_next_day"


class PaperTradingSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(default_factory=lambda: f"psess_{uuid4().hex}")
    portfolio_id: str
    mode: PaperSessionMode
    status: PaperSessionStatus = PaperSessionStatus.CREATED
    phase: PaperSessionPhase
    start_date: date
    end_date: date | None = None
    historical_data_source: MinuteDataSource | None = None
    live_data_source: MinuteDataSource | None = None
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    validated_execution_policy: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_error: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_mode_sources(self) -> "PaperTradingSession":
        if self.mode == PaperSessionMode.REPLAY_ONLY:
            if self.historical_data_source is None:
                raise ValueError("REPLAY_ONLY requires historical_data_source")
            if self.live_data_source is not None:
                raise ValueError("REPLAY_ONLY must not set live_data_source")
            if self.end_date is None:
                raise ValueError("REPLAY_ONLY requires end_date")
        if self.mode == PaperSessionMode.LIVE_ONLY:
            if self.live_data_source is None:
                raise ValueError("LIVE_ONLY requires live_data_source")
            if self.historical_data_source is not None:
                raise ValueError("LIVE_ONLY must not set historical_data_source")
        if self.mode == PaperSessionMode.CATCHUP_THEN_LIVE:
            if self.historical_data_source is None or self.live_data_source is None:
                raise ValueError("CATCHUP_THEN_LIVE requires both historical_data_source and live_data_source")
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError("end_date cannot be before start_date")
        return self


class PaperSessionDay(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_day_id: str = Field(default_factory=lambda: f"psday_{uuid4().hex}")
    session_id: str
    portfolio_id: str
    trade_date: date
    run_id: str | None = None
    status: PaperSessionStatus
    phase: PaperSessionPhase
    data_source: MinuteDataSource
    expected_bar_count: int | None = None
    latest_available_bar_time: datetime | None = None
    last_processed_bar_time: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OrderExecutionState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_state_id: str = Field(default_factory=lambda: f"oexec_{uuid4().hex}")
    session_id: str
    run_id: str
    order_id: str
    symbol: str
    trade_date: date
    algo_code: str
    algo_state: dict[str, Any] = Field(default_factory=dict)
    plan: dict[str, Any] | None = None
    plan_sha256: str | None = None
    last_processed_bar_time: datetime | None = None
    filled_quantity: int = Field(ge=0)
    remaining_quantity: int = Field(ge=0)
    status: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class IntradaySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_id: str = Field(default_factory=lambda: f"isnap_{uuid4().hex}")
    session_id: str
    run_id: str
    portfolio_id: str
    trade_date: date
    snapshot_time: datetime
    cash: float
    market_value: float
    nav: float
    positions: list[dict[str, Any]]
    source: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PaperSessionProgress(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session: PaperTradingSession
    current_trade_date: date | None = None
    last_processed_bar_time: datetime | None = None
    latest_available_bar_time: datetime | None = None
    next_expected_bar_time: datetime | None = None
    day_count: int = 0
    events: list[dict[str, Any]] = Field(default_factory=list)


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
