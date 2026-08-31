"""Successor LocalSIM account, lineage, and historical replay contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.simulation_data.daily_context import SimulationBrokerBackend

from .models import canonical_json_sha256


SIMULATION_ACCOUNT_SCHEMA = "simulation_account_v1"
SIMULATION_LEDGER_SCOPE_SCHEMA = "simulation_ledger_scope_v1"
LEGACY_LOCALSIM_LINEAGE_SCHEMA = "legacy_localsim_account_lineage_v1"
LOCALSIM_REPLAY_JOB_SCHEMA = "localsim_replay_job_v1"
LOCALSIM_DAILY_ENGINE_CONTRACT = "simulation_daily_engine_v1"


class SimulationAccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    RETIRED = "RETIRED"


class SimulationLedgerScopeKind(str, Enum):
    LEGACY_PORTFOLIO = "LEGACY_PORTFOLIO"
    SUCCESSOR_NATIVE = "SUCCESSOR_NATIVE"


class LegacyLocalSimLineageStatus(str, Enum):
    PREPARED = "PREPARED"
    ACTIVATION_PENDING_SAFE_BOUNDARY = "ACTIVATION_PENDING_SAFE_BOUNDARY"
    ACTIVE = "ACTIVE"


class LocalSimReplayStatus(str, Enum):
    CREATED = "CREATED"
    RUNNING_HISTORICAL = "RUNNING_HISTORICAL"
    CAUGHT_UP = "CAUGHT_UP"
    READY_FOR_LIVE = "READY_FOR_LIVE"
    ACTIVATION_PENDING_SAFE_BOUNDARY = "ACTIVATION_PENDING_SAFE_BOUNDARY"
    LIVE_ACTIVE = "LIVE_ACTIVE"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    CANCELLED = "CANCELLED"


class LegacyLocalSimAccountInventoryV1(BaseModel):
    """Exact retained-account inventory consumed by one lineage preparation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    legacy_account_id: str
    account_name: str
    package_id: str
    manifest_sha256: str
    admission_receipt_id: str
    initial_capital: float = Field(gt=0)
    release_id: str
    release_hash: str
    binding_id: str
    binding_hash: str
    ledger_scope_id: str
    economic_facts_sha256: str
    current_status: SimulationAccountStatus
    runtime_owned: bool
    retained_by_user: bool
    in_flight_economic_transactions: int = Field(ge=0)

    @field_validator(
        "legacy_account_id",
        "account_name",
        "package_id",
        "manifest_sha256",
        "admission_receipt_id",
        "release_id",
        "release_hash",
        "binding_id",
        "binding_hash",
        "ledger_scope_id",
        "economic_facts_sha256",
    )
    @classmethod
    def _required_fields(cls, value: str) -> str:
        return _required_text(value)

    @field_validator("manifest_sha256", "release_hash", "binding_hash", "economic_facts_sha256")
    @classmethod
    def _hash_fields(cls, value: str) -> str:
        return _sha256_text(value)


class LocalSimSafeBoundaryDecisionV1(BaseModel):
    """Automatic technical activation decision; it is not an operator approval."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    eligible: bool
    evaluated_at: datetime
    current_trading_date: date
    activation_trade_date: date
    market_phase: Literal["PRE_OPEN", "TRADING", "POST_CLOSE", "NON_TRADING_DAY"]
    in_flight_economic_transactions: int = Field(ge=0)
    writer_claim_available: bool
    historical_provider_closed: bool
    reason_code: str

    @field_validator("evaluated_at")
    @classmethod
    def _timestamp_is_utc(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @field_validator("reason_code")
    @classmethod
    def _reason_is_present(cls, value: str) -> str:
        return _required_text(value)

    @model_validator(mode="after")
    def _eligibility_is_consistent(self) -> "LocalSimSafeBoundaryDecisionV1":
        if self.market_phase == "TRADING" and self.activation_trade_date <= self.current_trading_date:
            raise ValueError("intraday catch-up cannot activate on the current trading date")
        technical_conditions = (
            self.market_phase == "PRE_OPEN"
            and self.in_flight_economic_transactions == 0
            and self.writer_claim_available
            and self.historical_provider_closed
            and self.activation_trade_date == self.current_trading_date
        )
        if self.eligible != technical_conditions:
            raise ValueError("safe-boundary eligibility does not match technical conditions")
        return self


def _required_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("field is required")
    return text


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sha256_text(value: str) -> str:
    text = _required_text(value).lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError("field must be a lowercase SHA-256 hex digest")
    return text


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


class SimulationAccountV1(BaseModel):
    """Logical LocalSIM account; mutable lifecycle is guarded by ``version`` CAS."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["simulation_account_v1"] = SIMULATION_ACCOUNT_SCHEMA
    account_id: str
    account_hash: str
    account_name: str
    broker_backend: Literal[SimulationBrokerBackend.LOCAL_SIM] = SimulationBrokerBackend.LOCAL_SIM
    package_id: str
    manifest_sha256: str
    admission_receipt_id: str
    initial_capital: float = Field(gt=0)
    lineage_source_legacy_account_id: str | None = None
    account_config_json: dict[str, Any]
    status: SimulationAccountStatus = SimulationAccountStatus.ACTIVE
    version: int = Field(default=1, ge=1)
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "account_id",
        "account_hash",
        "account_name",
        "package_id",
        "manifest_sha256",
        "admission_receipt_id",
        "created_by",
    )
    @classmethod
    def _required_fields(cls, value: str) -> str:
        return _required_text(value)

    @field_validator("account_hash", "manifest_sha256")
    @classmethod
    def _hash_fields(cls, value: str) -> str:
        return _sha256_text(value)

    @field_validator("created_at", "updated_at")
    @classmethod
    def _timestamps_are_utc(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @model_validator(mode="after")
    def _identity_matches_config(self) -> "SimulationAccountV1":
        expected_hash = canonical_json_sha256(self.account_config_json)
        if self.account_hash != expected_hash:
            raise ValueError("account_hash does not match account_config_json")
        if self.account_id != f"simacct_{expected_hash[:16]}":
            raise ValueError("account_id does not match account_hash")
        expected = {
            "schema_version": SIMULATION_ACCOUNT_SCHEMA,
            "account_name": self.account_name,
            "broker_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "admission_receipt_id": self.admission_receipt_id,
            "initial_capital": float(self.initial_capital),
        }
        if self.lineage_source_legacy_account_id is not None:
            expected["lineage_source_legacy_account_id"] = self.lineage_source_legacy_account_id
        if self.account_config_json != expected:
            raise ValueError("account_config_json does not match immutable account fields")
        return self


class SimulationLedgerScopeV1(BaseModel):
    """Immutable economic-ledger namespace; never an account or portfolio truth."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["simulation_ledger_scope_v1"] = SIMULATION_LEDGER_SCOPE_SCHEMA
    ledger_scope_id: str
    ledger_scope_hash: str
    scope_kind: SimulationLedgerScopeKind
    source_identity: str
    native_account_id: str | None = None
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("ledger_scope_id", "ledger_scope_hash", "source_identity", "created_by")
    @classmethod
    def _required_fields(cls, value: str) -> str:
        return _required_text(value)

    @field_validator("ledger_scope_hash")
    @classmethod
    def _hash_fields(cls, value: str) -> str:
        return _sha256_text(value)

    @field_validator("native_account_id")
    @classmethod
    def _optional_fields(cls, value: str | None) -> str | None:
        return _optional_text(value)

    @field_validator("created_at")
    @classmethod
    def _timestamps_are_utc(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SIMULATION_LEDGER_SCOPE_SCHEMA,
            "ledger_scope_id": self.ledger_scope_id,
            "scope_kind": self.scope_kind.value,
            "source_identity": self.source_identity,
            "native_account_id": self.native_account_id,
        }

    @model_validator(mode="after")
    def _identity_is_canonical(self) -> "SimulationLedgerScopeV1":
        expected_hash = canonical_json_sha256(self.identity_payload())
        if self.ledger_scope_hash != expected_hash:
            raise ValueError("ledger_scope_hash does not match ledger scope identity")
        if self.scope_kind is SimulationLedgerScopeKind.LEGACY_PORTFOLIO:
            if self.native_account_id is not None or self.ledger_scope_id != self.source_identity:
                raise ValueError("legacy ledger scope must preserve its portfolio source identity")
        elif (
            self.native_account_id is None
            or self.ledger_scope_id != self.native_account_id
            or self.source_identity != self.native_account_id
        ):
            raise ValueError("native ledger scope identity must equal its SimulationAccountV1 identity")
        return self


class LegacyLocalSimAccountLineageV1(BaseModel):
    """One fail-closed legacy identity mapping without economic-row migration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["legacy_localsim_account_lineage_v1"] = LEGACY_LOCALSIM_LINEAGE_SCHEMA
    lineage_id: str
    lineage_hash: str
    legacy_account_id: str
    account_id: str
    release_id: str
    binding_id: str
    ledger_scope_id: str
    economic_facts_sha256: str
    status: LegacyLocalSimLineageStatus = LegacyLocalSimLineageStatus.PREPARED
    version: int = Field(default=1, ge=1)
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "lineage_id",
        "lineage_hash",
        "legacy_account_id",
        "account_id",
        "release_id",
        "binding_id",
        "ledger_scope_id",
        "economic_facts_sha256",
        "created_by",
    )
    @classmethod
    def _required_fields(cls, value: str) -> str:
        return _required_text(value)

    @field_validator("lineage_hash", "economic_facts_sha256")
    @classmethod
    def _hash_fields(cls, value: str) -> str:
        return _sha256_text(value)

    @field_validator("created_at", "updated_at")
    @classmethod
    def _timestamps_are_utc(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": LEGACY_LOCALSIM_LINEAGE_SCHEMA,
            "legacy_account_id": self.legacy_account_id,
            "account_id": self.account_id,
            "release_id": self.release_id,
            "binding_id": self.binding_id,
            "ledger_scope_id": self.ledger_scope_id,
            "economic_facts_sha256": self.economic_facts_sha256,
        }

    @model_validator(mode="after")
    def _identity_is_canonical(self) -> "LegacyLocalSimAccountLineageV1":
        expected_hash = canonical_json_sha256(self.identity_payload())
        if self.lineage_hash != expected_hash:
            raise ValueError("lineage_hash does not match lineage identity")
        if self.lineage_id != f"lslineage_{expected_hash[:16]}":
            raise ValueError("lineage_id does not match lineage_hash")
        return self


class LocalSimReplayJobV1(BaseModel):
    """Durable cursor for one isolated historical account and binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_replay_job_v1"] = LOCALSIM_REPLAY_JOB_SCHEMA
    replay_job_id: str
    replay_hash: str
    simulation_account_id: str
    release_id: str
    binding_id: str
    day_engine_contract_id: Literal["simulation_daily_engine_v1"] = LOCALSIM_DAILY_ENGINE_CONTRACT
    start_trade_date: date
    end_trade_date: date
    historical_source_id: str
    historical_source_sha256: str
    calendar_snapshot_sha256: str
    status: LocalSimReplayStatus = LocalSimReplayStatus.CREATED
    next_trade_date: date | None = None
    completed_trade_date: date | None = None
    live_release_id: str | None = None
    live_binding_id: str | None = None
    activation_trade_date: date | None = None
    version: int = Field(default=1, ge=1)
    failure_code: str | None = None
    failure_context: dict[str, Any] | None = None
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "replay_job_id",
        "replay_hash",
        "simulation_account_id",
        "release_id",
        "binding_id",
        "historical_source_id",
        "historical_source_sha256",
        "calendar_snapshot_sha256",
        "created_by",
    )
    @classmethod
    def _required_fields(cls, value: str) -> str:
        return _required_text(value)

    @field_validator("replay_hash", "historical_source_sha256", "calendar_snapshot_sha256")
    @classmethod
    def _hash_fields(cls, value: str) -> str:
        return _sha256_text(value)

    @field_validator("live_release_id", "live_binding_id", "failure_code")
    @classmethod
    def _optional_fields(cls, value: str | None) -> str | None:
        return _optional_text(value)

    @field_validator("created_at", "updated_at")
    @classmethod
    def _timestamps_are_utc(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": LOCALSIM_REPLAY_JOB_SCHEMA,
            "simulation_account_id": self.simulation_account_id,
            "release_id": self.release_id,
            "binding_id": self.binding_id,
            "day_engine_contract_id": self.day_engine_contract_id,
            "start_trade_date": self.start_trade_date.isoformat(),
            "end_trade_date": self.end_trade_date.isoformat(),
            "historical_source_id": self.historical_source_id,
            "historical_source_sha256": self.historical_source_sha256,
            "calendar_snapshot_sha256": self.calendar_snapshot_sha256,
        }

    @model_validator(mode="after")
    def _identity_and_state_are_consistent(self) -> "LocalSimReplayJobV1":
        if self.end_trade_date < self.start_trade_date:
            raise ValueError("end_trade_date must not precede start_trade_date")
        expected_hash = canonical_json_sha256(self.identity_payload())
        if self.replay_hash != expected_hash:
            raise ValueError("replay_hash does not match replay identity")
        if self.replay_job_id != f"lsreplay_{expected_hash[:16]}":
            raise ValueError("replay_job_id does not match replay_hash")
        if (self.live_release_id is None) != (self.live_binding_id is None):
            raise ValueError("live release and binding must be set together")
        live_states = {
            LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY,
            LocalSimReplayStatus.LIVE_ACTIVE,
        }
        if self.status in live_states and (
            self.live_release_id is None or self.live_binding_id is None or self.activation_trade_date is None
        ):
            raise ValueError("live successor states require release, binding, and activation date")
        if self.live_release_id is not None and self.status not in live_states:
            raise ValueError("live successor identity is invalid before safe-boundary state")
        caught_up_states = {
            LocalSimReplayStatus.CAUGHT_UP,
            LocalSimReplayStatus.READY_FOR_LIVE,
            *live_states,
        }
        if self.status in caught_up_states and (
            self.completed_trade_date != self.end_trade_date or self.next_trade_date is not None
        ):
            raise ValueError("caught-up replay states require the terminal historical cursor")
        failed_states = {
            LocalSimReplayStatus.FAILED_RETRYABLE,
            LocalSimReplayStatus.FAILED_TERMINAL,
        }
        if self.status in failed_states and not self.failure_code:
            raise ValueError("failed replay states require failure_code")
        if self.status not in failed_states and (self.failure_code is not None or self.failure_context is not None):
            raise ValueError("non-failed replay states cannot retain failure evidence")
        return self
