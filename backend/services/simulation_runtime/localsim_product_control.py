"""Versioned product commands for the successor LocalSIM control plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.simulation_data.trading_calendar import TradeCalendarProvider
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .localsim_control import LocalSimControlPlaneService
from .localsim_product_authority import LocalSimProductAuthority
from .localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator
from .localsim_runtime_profile import (
    LocalSimRuntimeProfileConfigRequestV1,
    LocalSimRuntimeProfileV1,
    LocalSimRuntimeProfileVersionV1,
)
from .models import SimulationReleaseBinding, StrategyRuntimeRelease, canonical_json_sha256
from .successor_models import LocalSimReplayJobV1, SimulationAccountV1, SimulationLedgerScopeV1
from .successor_repository import LocalSimSuccessorRepositoryProtocol


LOCALSIM_CONTROL_RESPONSE_SCHEMA = "localsim_control_response_v1"
LOCALSIM_HISTORICAL_SOURCE_ID = "market.kline_minute_raw.v1"


class _LocalSimAccountInputsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_name: str
    package_id: str
    initial_capital: Decimal = Field(gt=0, max_digits=20, decimal_places=4)
    runtime_profile_version_id: str
    execution_policy_version_id: str
    effective_from: date
    effective_to: date | None = None
    created_reason: str | None = None
    requested_execution_policy_audit: dict[str, Any] | None = None

    @field_validator(
        "account_name",
        "package_id",
        "runtime_profile_version_id",
        "execution_policy_version_id",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field is required")
        return text

    @field_validator("created_reason")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return str(value).strip() or None

    @model_validator(mode="after")
    def _effective_window_is_valid(self) -> "_LocalSimAccountInputsV1":
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not precede effective_from")
        return self


class LocalSimAccountCreateRequestV1(_LocalSimAccountInputsV1):
    schema_version: Literal["localsim_account_create_request_v1"] = "localsim_account_create_request_v1"


class LocalSimSuccessorReleaseRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_successor_release_request_v1"] = "localsim_successor_release_request_v1"
    base_release_id: str
    base_binding_id: str
    runtime_profile_version_id: str
    execution_policy_version_id: str
    effective_from: date
    created_reason: str | None = None
    requested_execution_policy_audit: dict[str, Any] | None = None

    @field_validator(
        "base_release_id",
        "base_binding_id",
        "runtime_profile_version_id",
        "execution_policy_version_id",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field is required")
        return text


class LocalSimReplayCreateRequestV1(_LocalSimAccountInputsV1):
    schema_version: Literal["localsim_replay_create_request_v1"] = "localsim_replay_create_request_v1"
    start_trade_date: date
    end_trade_date: date
    historical_source_id: str

    @model_validator(mode="after")
    def _replay_window_is_valid(self) -> "LocalSimReplayCreateRequestV1":
        if self.start_trade_date > self.end_trade_date:
            raise ValueError("start_trade_date must not follow end_trade_date")
        if self.effective_to is not None and self.effective_to != self.end_trade_date:
            raise ValueError("replay effective_to must equal end_trade_date")
        return self


class LocalSimLifecycleRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_lifecycle_request_v1"] = "localsim_lifecycle_request_v1"
    expected_version: int = Field(ge=1)


class LocalSimBulkLifecycleItemV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    account_id: str
    expected_version: int = Field(ge=1)


class LocalSimBulkLifecycleRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_bulk_lifecycle_request_v1"] = "localsim_bulk_lifecycle_request_v1"
    action: Literal["pause", "resume", "retire"]
    items: list[LocalSimBulkLifecycleItemV1] = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def _account_ids_are_unique(self) -> "LocalSimBulkLifecycleRequestV1":
        ids = [item.account_id for item in self.items]
        if len(ids) != len(set(ids)):
            raise ValueError("bulk lifecycle account_id values must be unique")
        return self


class LocalSimReplayCancelRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_replay_cancel_request_v1"] = "localsim_replay_cancel_request_v1"
    expected_version: int = Field(ge=1)


class LocalSimRuntimeProfileCreateRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_runtime_profile_create_request_v1"] = (
        "localsim_runtime_profile_create_request_v1"
    )
    package_id: str
    profile_name: str

    @field_validator("package_id", "profile_name")
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field is required")
        return text


class LocalSimRuntimeProfileVersionCreateRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_runtime_profile_version_create_request_v1"] = (
        "localsim_runtime_profile_version_create_request_v1"
    )
    expected_profile_version: int = Field(ge=1)
    config: LocalSimRuntimeProfileConfigRequestV1


class LocalSimRuntimeProfileRetireRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["localsim_runtime_profile_retire_request_v1"] = (
        "localsim_runtime_profile_retire_request_v1"
    )
    expected_version: int = Field(ge=1)


class LocalSimSelectionLinkContextV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    trade_date: date
    data_source: str
    runtime_config: dict[str, Any]

    @field_validator("run_id", "data_source")
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field is required")
        return text


class LocalSimControlResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: Literal[True] = True
    schema_version: Literal["localsim_control_response_v1"] = LOCALSIM_CONTROL_RESPONSE_SCHEMA
    account: SimulationAccountV1 | None = None
    ledger_scope: SimulationLedgerScopeV1 | None = None
    release: StrategyRuntimeRelease | None = None
    binding: SimulationReleaseBinding | None = None
    replay: LocalSimReplayJobV1 | None = None


class LocalSimBulkLifecycleResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: Literal[True] = True
    schema_version: Literal["localsim_bulk_lifecycle_response_v1"] = "localsim_bulk_lifecycle_response_v1"
    action: Literal["pause", "resume", "retire"]
    accounts: list[SimulationAccountV1]


class LocalSimRuntimeProfileResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: Literal[True] = True
    schema_version: Literal["localsim_runtime_profile_response_v1"] = "localsim_runtime_profile_response_v1"
    profile: LocalSimRuntimeProfileV1 | None = None
    version: LocalSimRuntimeProfileVersionV1 | None = None


@dataclass(frozen=True)
class LocalSimHistoricalSourceResolutionV1:
    historical_source_id: str
    historical_source_sha256: str
    trading_days: tuple[date, ...]
    current_trading_date: date
    latest_completed_trade_date: date


class LocalSimHistoricalSourceAuthorityProtocol(Protocol):
    def resolve(
        self, *, historical_source_id: str, start_trade_date: date, end_trade_date: date
    ) -> LocalSimHistoricalSourceResolutionV1: ...


class LocalSimMutationReadinessProtocol(Protocol):
    def require_ready(self) -> None: ...


class LocalSimHistoricalSourceAuthority:
    """Resolve the single completed-day DB source and global calendar snapshot."""

    def __init__(self, *, calendar: Any | None = None, clock: Any | None = None) -> None:
        self.calendar = calendar or TradeCalendarProvider()
        self.clock = clock or (lambda: datetime.now(ZoneInfo("Asia/Shanghai")))

    def resolve(
        self, *, historical_source_id: str, start_trade_date: date, end_trade_date: date
    ) -> LocalSimHistoricalSourceResolutionV1:
        source_id = str(historical_source_id or "").strip()
        if source_id != LOCALSIM_HISTORICAL_SOURCE_ID:
            raise DataUnavailableError(
                "LocalSIM historical source is unsupported",
                context={"reason_code": "LOCALSIM_HISTORICAL_SOURCE_UNSUPPORTED", "historical_source_id": source_id},
            )
        current_date = self.clock().astimezone(ZoneInfo("Asia/Shanghai")).date()
        if end_trade_date >= current_date:
            raise InvalidStateTransitionError(
                "LocalSIM replay cannot include the current or future trading date",
                context={"reason_code": "LOCALSIM_REPLAY_CURRENT_DAY_SOURCE_FORBIDDEN"},
            )
        days = tuple(self.calendar.list_trading_days(start_trade_date, end_trade_date))
        if not days or days[0] != start_trade_date or days[-1] != end_trade_date:
            raise DataUnavailableError(
                "LocalSIM replay range must start and end on covered trading dates",
                context={"reason_code": "LOCALSIM_REPLAY_CALENDAR_RANGE_INVALID"},
            )
        latest_completed = self.calendar.latest_trading_day_on_or_before(current_date - timedelta(days=1))
        if latest_completed is None:
            raise DataUnavailableError(
                "LocalSIM historical source has no completed trading day",
                context={"reason_code": "LOCALSIM_REPLAY_COMPLETED_DAY_UNAVAILABLE"},
            )
        if end_trade_date != latest_completed:
            raise InvalidStateTransitionError(
                "LocalSIM catch-up replay must end at the latest completed trading day",
                context={
                    "reason_code": "LOCALSIM_REPLAY_NOT_CAUGHT_UP_TO_LATEST_COMPLETED_DAY",
                    "requested_end_trade_date": end_trade_date.isoformat(),
                    "latest_completed_trade_date": latest_completed.isoformat(),
                },
            )
        source_payload = {
            "schema_version": "localsim_historical_source_v1",
            "historical_source_id": source_id,
            "minute_source": "market.kline_minute_raw",
            "date_boundary": "completed_days_only",
            "daily_engine_contract_id": "simulation_daily_engine_v1",
        }
        return LocalSimHistoricalSourceResolutionV1(
            historical_source_id=source_id,
            historical_source_sha256=canonical_json_sha256(source_payload),
            trading_days=days,
            current_trading_date=current_date,
            latest_completed_trade_date=latest_completed,
        )


class LocalSimProductControlPlaneService:
    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        control: LocalSimControlPlaneService,
        authority: LocalSimProductAuthority,
        readiness: LocalSimMutationReadinessProtocol,
        historical_source_authority: LocalSimHistoricalSourceAuthorityProtocol | None = None,
        historical_day_callback: Any | None = None,
    ) -> None:
        self.repository = repository
        self.control = control
        self.authority = authority
        self.readiness = readiness
        self.historical_source_authority = historical_source_authority or LocalSimHistoricalSourceAuthority()
        self.historical_day_callback = historical_day_callback or _unconfigured_historical_day_callback

    def create_account(
        self, request: LocalSimAccountCreateRequestV1, *, created_by: str
    ) -> LocalSimControlResponseV1:
        self.readiness.require_ready()
        resolved = self.authority.resolve_product(
            package_id=request.package_id,
            runtime_profile_version_id=request.runtime_profile_version_id,
            execution_policy_version_id=request.execution_policy_version_id,
        )
        account, scope, release, binding = self.control.create_account(
            **self._account_bundle_kwargs(request=request, resolved=resolved, created_by=created_by)
        )
        return LocalSimControlResponseV1(
            account=account,
            ledger_scope=scope,
            release=release,
            binding=binding,
        )

    def create_account_from_selection(
        self,
        request: LocalSimAccountCreateRequestV1,
        *,
        link_context: LocalSimSelectionLinkContextV1,
        created_by: str,
    ) -> tuple[LocalSimControlResponseV1, dict[str, Any]]:
        self.readiness.require_ready()
        resolved = self.authority.resolve_product(
            package_id=request.package_id,
            runtime_profile_version_id=request.runtime_profile_version_id,
            execution_policy_version_id=request.execution_policy_version_id,
        )
        account, scope, release, binding = self.control.build_account_bundle(
            **self._account_bundle_kwargs(request=request, resolved=resolved, created_by=created_by)
        )
        selection_link = {
            "run_id": link_context.run_id,
            "simulation_account_id": account.account_id,
            "package_id": account.package_id,
            "manifest_sha256": account.manifest_sha256,
            "trade_date": link_context.trade_date,
            "data_source": link_context.data_source,
            "start_date": request.effective_from,
            "initial_cash": float(request.initial_capital),
            "runtime_config": link_context.runtime_config,
            "created_at": account.created_at,
        }
        account, scope, release, binding, persisted_link = self.repository.create_selection_account_bundle(
            account=account,
            ledger_scope=scope,
            release=release,
            binding=binding,
            selection_link=selection_link,
        )
        return (
            LocalSimControlResponseV1(
                account=account,
                ledger_scope=scope,
                release=release,
                binding=binding,
            ),
            persisted_link,
        )

    def create_successor_release(
        self,
        *,
        account_id: str,
        request: LocalSimSuccessorReleaseRequestV1,
        created_by: str,
    ) -> LocalSimControlResponseV1:
        self.readiness.require_ready()
        account = self.repository.get_account(account_id)
        resolved = self.authority.resolve_product(
            package_id=account.package_id,
            runtime_profile_version_id=request.runtime_profile_version_id,
            execution_policy_version_id=request.execution_policy_version_id,
        )
        release, binding = self.control.create_successor_release(
            account_id=account_id,
            base_release_id=request.base_release_id,
            base_binding_id=request.base_binding_id,
            runtime_profile_id=resolved.runtime_profile.profile_id,
            runtime_profile_version_id=resolved.runtime_profile_version.profile_version_id,
            runtime_profile_sha256=resolved.runtime_profile_version.config_sha256,
            daily_strategy_profile_version_id=resolved.runtime_profile_version.daily_strategy_profile_version_id,
            execution_policy_version_id=resolved.execution_policy.policy_id,
            execution_policy_sha256=str(resolved.execution_policy.policy_sha256),
            execution_policy_json=resolved.execution_policy.policy_json,
            tail_policy_version_id=resolved.tail_policy_version_id,
            tail_policy_sha256=resolved.tail_policy_sha256,
            release_validation_evidence=resolved.release_validation_evidence(),
            release_metadata={"localsim_runtime_profile_config": resolved.runtime_profile_version.config_json},
            requested_execution_policy_audit=request.requested_execution_policy_audit,
            effective_from=request.effective_from,
            created_by=created_by,
            created_reason=request.created_reason,
        )
        return LocalSimControlResponseV1(account=account, release=release, binding=binding)

    def create_replay(
        self, request: LocalSimReplayCreateRequestV1, *, created_by: str
    ) -> LocalSimControlResponseV1:
        self.readiness.require_ready()
        source = self.historical_source_authority.resolve(
            historical_source_id=request.historical_source_id,
            start_trade_date=request.start_trade_date,
            end_trade_date=request.end_trade_date,
        )
        resolved = self.authority.resolve_product(
            package_id=request.package_id,
            runtime_profile_version_id=request.runtime_profile_version_id,
            execution_policy_version_id=request.execution_policy_version_id,
        )
        kwargs = self._account_bundle_kwargs(request=request, resolved=resolved, created_by=created_by)
        kwargs["effective_to"] = request.end_trade_date
        account, scope, release, binding = self.control.build_account_bundle(**kwargs)
        runner = LocalSimHistoricalDayRunner(
            historical_source_id=source.historical_source_id,
            historical_source_sha256=source.historical_source_sha256,
            run_day=self.historical_day_callback,
        )
        replay = LocalSimReplayCoordinator(repository=self.repository, historical_day_runner=runner).build_job(
            account=account,
            release=release,
            binding=binding,
            start_trade_date=request.start_trade_date,
            end_trade_date=request.end_trade_date,
            historical_source_id=source.historical_source_id,
            historical_source_sha256=source.historical_source_sha256,
            trading_days=source.trading_days,
            created_by=created_by,
        )
        account, scope, release, binding, replay = self.repository.create_replay_bundle(
            account=account,
            ledger_scope=scope,
            release=release,
            binding=binding,
            replay_job=replay,
        )
        return LocalSimControlResponseV1(
            account=account,
            ledger_scope=scope,
            release=release,
            binding=binding,
            replay=replay,
        )

    def transition_account(
        self, *, account_id: str, action: Literal["pause", "resume", "retire"], expected_version: int
    ) -> LocalSimControlResponseV1:
        self.readiness.require_ready()
        handler = {
            "pause": self.control.pause_account,
            "resume": self.control.resume_account,
            "retire": self.control.retire_account,
        }[action]
        return LocalSimControlResponseV1(account=handler(account_id=account_id, expected_version=expected_version))

    def transition_accounts_bulk(
        self, request: LocalSimBulkLifecycleRequestV1
    ) -> LocalSimBulkLifecycleResponseV1:
        self.readiness.require_ready()
        accounts = self.control.transition_accounts_bulk(
            action=request.action,
            expected_versions={item.account_id: item.expected_version for item in request.items},
        )
        return LocalSimBulkLifecycleResponseV1(
            action=request.action,
            accounts=accounts,
        )

    def cancel_replay(self, *, replay_job_id: str, expected_version: int) -> LocalSimControlResponseV1:
        self.readiness.require_ready()
        job = self.repository.get_replay_job(replay_job_id)
        runner = LocalSimHistoricalDayRunner(
            historical_source_id=job.historical_source_id,
            historical_source_sha256=job.historical_source_sha256,
            run_day=self.historical_day_callback,
        )
        replay = LocalSimReplayCoordinator(repository=self.repository, historical_day_runner=runner).cancel(
            replay_job_id=replay_job_id,
            expected_version=expected_version,
        )
        return LocalSimControlResponseV1(replay=replay)

    def _account_bundle_kwargs(self, *, request: Any, resolved: Any, created_by: str) -> dict[str, Any]:
        return {
            "account_name": request.account_name,
            "package_id": resolved.package_id,
            "manifest_sha256": resolved.manifest_sha256,
            "admission_receipt_id": resolved.admission_receipt_id,
            "initial_capital": float(request.initial_capital),
            "runtime_profile_id": resolved.runtime_profile.profile_id,
            "runtime_profile_version_id": resolved.runtime_profile_version.profile_version_id,
            "runtime_profile_sha256": resolved.runtime_profile_version.config_sha256,
            "daily_strategy_profile_version_id": resolved.runtime_profile_version.daily_strategy_profile_version_id,
            "execution_policy_version_id": resolved.execution_policy.policy_id,
            "execution_policy_sha256": str(resolved.execution_policy.policy_sha256),
            "execution_policy_json": resolved.execution_policy.policy_json,
            "tail_policy_version_id": resolved.tail_policy_version_id,
            "tail_policy_sha256": resolved.tail_policy_sha256,
            "release_validation_evidence": resolved.release_validation_evidence(),
            "release_metadata": {"localsim_runtime_profile_config": resolved.runtime_profile_version.config_json},
            "requested_execution_policy_audit": request.requested_execution_policy_audit,
            "effective_from": request.effective_from,
            "effective_to": request.effective_to,
            "created_by": created_by,
            "created_reason": request.created_reason,
        }


def _unconfigured_historical_day_callback(job: LocalSimReplayJobV1, trade_date: date) -> None:
    raise DataUnavailableError(
        "LocalSIM historical replay day runner is not configured",
        context={
            "reason_code": "LOCALSIM_HISTORICAL_RUNNER_UNAVAILABLE",
            "replay_job_id": job.replay_job_id,
            "trade_date": trade_date.isoformat(),
        },
    )
