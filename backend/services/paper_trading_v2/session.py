"""Durable Paper Trading v2 session orchestration.

The session layer adds long-running command state around the existing strict
day runner. It intentionally does not weaken day-run or replay semantics.
Unsupported real-time execution paths fail before a fake live session is
created.
"""

from __future__ import annotations

import threading
from datetime import UTC, date, datetime
from typing import Any, Literal

from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    SessionAlreadyRunningError,
    SessionConfigError,
    SessionLockTimeoutError,
    SessionSourceUnsupportedError,
    TradingCoreError,
    UnsupportedFeatureError,
)
from backend.services.trading_core.execution_algo_capabilities import (
    require_execution_algo_supports_mode,
)
from backend.services.trading_core.models import RunStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository, StrategyPackageRepository
from backend.services.selection_center.runtime_profile import validate_runtime_profile_binding

from .market_data import MinuteDataSource, TradeCalendarProvider
from .models import (
    PaperReplayResult,
    PaperSessionDay,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionProgress,
    PaperSessionStatus,
    PaperTradingSession,
    PortfolioStatus,
)
from .live_session import PaperTradingLiveMinuteExecutor
from .replay import PaperTradingHistoricalReplay
from .repository import PaperTradingV2Repository
from .service import PaperTradingV2PortfolioService


SUPPORTED_HISTORICAL_SESSION_SOURCES = {MinuteDataSource.DB_HISTORICAL}
SUPPORTED_LIVE_SESSION_SOURCES = {MinuteDataSource.TDX_REALTIME}
TERMINAL_SESSION_STATUSES = {
    PaperSessionStatus.SUCCEEDED,
    PaperSessionStatus.FAILED,
    PaperSessionStatus.STOPPED,
}
PAUSED_RESUMABLE_STATUSES = {
    PaperSessionStatus.CREATED,
    PaperSessionStatus.PREFLIGHTING,
    PaperSessionStatus.REPLAYING,
    PaperSessionStatus.CATCHING_UP,
    PaperSessionStatus.LIVE_RUNNING,
    PaperSessionStatus.LIVE_WAITING_FOR_BAR,
    PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
}
TICKABLE_SESSION_STATUSES = {
    PaperSessionStatus.CREATED,
    PaperSessionStatus.PREFLIGHTING,
    PaperSessionStatus.REPLAYING,
    PaperSessionStatus.CATCHING_UP,
    PaperSessionStatus.SWITCHING_TO_LIVE,
    PaperSessionStatus.LIVE_RUNNING,
    PaperSessionStatus.LIVE_WAITING_FOR_BAR,
    PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
}
_SESSION_TICK_LOCKS: dict[str, threading.Lock] = {}
_SESSION_TICK_LOCKS_GUARD = threading.RLock()


def _session_tick_lock(session_id: str) -> threading.Lock:
    with _SESSION_TICK_LOCKS_GUARD:
        lock = _SESSION_TICK_LOCKS.get(session_id)
        if lock is None:
            lock = threading.Lock()
            _SESSION_TICK_LOCKS[session_id] = lock
        return lock


class PaperTradingSessionService:
    """Create and control Paper v2 replay/live sessions with fail-fast gates."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        package_repository: StrategyPackageRepository | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        enforce_non_trading_window: bool = False,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.package_repository = package_repository
        self.calendar_provider = calendar_provider or TradeCalendarProvider()
        self.enforce_non_trading_window = enforce_non_trading_window

    def create_session(
        self,
        *,
        portfolio_id: str,
        mode: PaperSessionMode | str,
        start_date: date,
        end_date: date | None = None,
        historical_data_source: MinuteDataSource | str | None = None,
        live_data_source: MinuteDataSource | str | None = None,
        runtime_config: dict[str, Any] | None = None,
        rerun_policy: Literal["reject_existing", "reset_portfolio"] = "reject_existing",
        auto_switch_to_live: bool = False,
        confirm_reset: bool = False,
        confirm_text: str | None = None,
        created_by: str | None = None,
        as_of_time: datetime | None = None,
    ) -> PaperTradingSession:
        self._require_non_trading_operation_window(
            action="create_session",
            portfolio_id=portfolio_id,
            as_of_time=as_of_time,
        )
        portfolio = self.repository.get_portfolio(portfolio_id)
        if portfolio.status != PortfolioStatus.READY:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY before creating a trade session",
                context={"portfolio_id": portfolio_id, "status": portfolio.status.value},
            )
        active = self.repository.list_active_sessions(portfolio_id)
        if active:
            raise SessionAlreadyRunningError(
                "paper v2 portfolio already has an active trade session",
                context={
                    "portfolio_id": portfolio_id,
                    "active_sessions": [
                        {"session_id": item.session_id, "mode": item.mode.value, "status": item.status.value}
                        for item in active
                    ],
                },
            )
        session_mode = self._parse_mode(mode)
        historical_source = self._parse_source(historical_data_source, field_name="historical_data_source")
        live_source = self._parse_source(live_data_source, field_name="live_data_source")
        if auto_switch_to_live:
            if session_mode != PaperSessionMode.REPLAY_ONLY:
                raise SessionConfigError(
                    "auto_switch_to_live can only be used with REPLAY_ONLY requests",
                    context={"portfolio_id": portfolio_id, "mode": session_mode.value},
                )
            session_mode = PaperSessionMode.CATCHUP_THEN_LIVE
            if live_source is None:
                live_source = MinuteDataSource.TDX_REALTIME
        self._validate_dates(
            mode=session_mode,
            portfolio_start_date=portfolio.start_date,
            start_date=start_date,
            end_date=end_date,
        )
        self._validate_sources(
            mode=session_mode,
            portfolio_data_source=portfolio.data_source,
            historical_data_source=historical_source,
            live_data_source=live_source,
        )
        if rerun_policy not in {"reject_existing", "reset_portfolio"}:
            raise UnsupportedFeatureError(
                "paper v2 session rerun policy is not implemented",
                context={"portfolio_id": portfolio_id, "rerun_policy": rerun_policy},
            )
        if rerun_policy == "reset_portfolio" and (not confirm_reset or confirm_text != portfolio_id):
            raise SessionConfigError(
                "reset_portfolio session requires explicit confirmation text matching portfolio_id",
                context={
                    "portfolio_id": portfolio_id,
                    "confirm_reset": confirm_reset,
                    "confirm_text_matches": confirm_text == portfolio_id,
                },
            )

        package_repository = self.package_repository
        if package_repository is None and hasattr(self.repository, "portfolios"):
            package_repository = InMemoryStrategyPackageRepository()
            portfolio_manifest = getattr(portfolio, "frozen_manifest", None)
            if portfolio_manifest is not None:
                package_repository.save_manifest(portfolio_manifest)
        config = PaperTradingV2PortfolioService(
            package_repository=package_repository,
            repository=self.repository,
        ).resolve_runtime_config_for_date(
            portfolio=portfolio,
            trade_date=start_date,
            runtime_config=runtime_config or {},
        )
        validate_runtime_profile_binding(
            config,
            context={"portfolio_id": portfolio_id, "trade_date": start_date.isoformat(), "check": "session_create"},
        )
        self._reject_raw_execution_overrides(config)
        policy_context = self._portfolio_policy_context(portfolio.execution_policy, portfolio_id=portfolio_id)
        require_execution_algo_supports_mode(
            policy_context["policy_json"],
            mode="HISTORICAL" if session_mode == PaperSessionMode.REPLAY_ONLY else session_mode.value,
            package_id=portfolio.package_id,
        )
        config["paper_v2_session"] = {
            "rerun_policy": rerun_policy,
            "auto_switch_to_live": auto_switch_to_live,
            "confirm_reset": confirm_reset,
            "confirm_text": confirm_text,
            "freeze_runtime_profile": True,
            **dict(config.get("paper_v2_session") or {}),
        }
        manual_tick_only = bool(config["paper_v2_session"].get("manual_tick_only", False))
        session = PaperTradingSession(
            portfolio_id=portfolio_id,
            mode=session_mode,
            status=PaperSessionStatus.PAUSED if manual_tick_only else PaperSessionStatus.CREATED,
            phase=self._initial_phase(session_mode),
            start_date=start_date,
            end_date=end_date,
            historical_data_source=historical_source,
            live_data_source=live_source,
            runtime_config=config,
            validated_execution_policy=policy_context,
            created_by=created_by,
        )
        saved = self.repository.create_session(session)
        self.repository.save_session_event(
            session_id=saved.session_id,
            event_type="SESSION_CREATED",
            message="paper v2 trade session created",
            context={
                "portfolio_id": portfolio_id,
                "mode": session_mode.value,
                "historical_data_source": historical_source.value if historical_source else None,
                "live_data_source": live_source.value if live_source else None,
                "rerun_policy": rerun_policy,
                "manual_tick_only": manual_tick_only,
            },
        )
        return saved

    def switch_session_mode(
        self,
        *,
        session_id: str,
        target_mode: PaperSessionMode | str,
        start_date: date,
        end_date: date | None = None,
        historical_data_source: MinuteDataSource | str | None = None,
        live_data_source: MinuteDataSource | str | None = None,
        runtime_config: dict[str, Any] | None = None,
        rerun_policy: Literal["reject_existing", "reset_portfolio"] = "reject_existing",
        auto_switch_to_live: bool = False,
        confirm_reset: bool = False,
        confirm_text: str | None = None,
        created_by: str | None = None,
        as_of_time: datetime | None = None,
    ) -> PaperTradingSession:
        target = self._parse_mode(target_mode)
        self._require_non_trading_operation_window(
            action="switch_session_mode",
            session_id=session_id,
            as_of_time=as_of_time,
        )
        current = self.repository.get_session(session_id)
        if current.status in TERMINAL_SESSION_STATUSES:
            raise InvalidStateTransitionError(
                "terminal paper v2 session cannot be switched",
                context={"session_id": session_id, "status": current.status.value},
            )
        if current.mode == target:
            raise InvalidStateTransitionError(
                "paper v2 session is already using the requested mode",
                context={"session_id": session_id, "mode": current.mode.value},
            )
        portfolio = self.repository.get_portfolio(current.portfolio_id)
        if portfolio.status not in {PortfolioStatus.READY, PortfolioStatus.RUNNING, PortfolioStatus.PAUSED}:
            raise InvalidStateTransitionError(
                "paper v2 session mode switch requires an operable portfolio state",
                context={
                    "session_id": session_id,
                    "portfolio_id": portfolio.portfolio_id,
                    "portfolio_status": portfolio.status.value,
                },
            )
        if self._has_running_run(portfolio.portfolio_id):
            raise InvalidStateTransitionError(
                "paper v2 session mode cannot be switched while a run is still RUNNING",
                context={
                    "session_id": session_id,
                    "portfolio_id": portfolio.portfolio_id,
                    "target_mode": target.value,
                    "reason": "wait until the active run is finalized before switching the session mode",
                },
            )
        stopped = self.repository.update_session_status(
            session_id,
            status=PaperSessionStatus.STOPPED,
            completed_at=as_of_time or datetime.now(UTC),
        )
        self.repository.save_session_event(
            session_id=session_id,
            event_type="SESSION_MODE_SWITCH_STOPPED_SOURCE",
            message="paper v2 session stopped before switching operation mode",
            context={
                "source_mode": current.mode.value,
                "target_mode": target.value,
                "created_by": created_by,
            },
        )
        if portfolio.status in {PortfolioStatus.RUNNING, PortfolioStatus.PAUSED}:
            self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
        new_session = self.create_session(
            portfolio_id=stopped.portfolio_id,
            mode=target,
            start_date=start_date,
            end_date=end_date,
            historical_data_source=historical_data_source,
            live_data_source=live_data_source,
            runtime_config=runtime_config,
            rerun_policy=rerun_policy,
            auto_switch_to_live=auto_switch_to_live,
            confirm_reset=confirm_reset,
            confirm_text=confirm_text,
            created_by=created_by,
            as_of_time=as_of_time,
        )
        self.repository.save_session_event(
            session_id=new_session.session_id,
            event_type="SESSION_MODE_SWITCH_CREATED_TARGET",
            message="paper v2 session created from mode switch",
            context={
                "source_session_id": session_id,
                "source_mode": current.mode.value,
                "target_mode": new_session.mode.value,
            },
        )
        return new_session

    def get_session(self, session_id: str) -> PaperTradingSession:
        return self.repository.get_session(session_id)

    def list_sessions(self, portfolio_id: str, *, limit: int = 100) -> list[PaperTradingSession]:
        return self.repository.list_sessions(portfolio_id, limit=limit)

    def session_capabilities(self, portfolio_id: str) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        policy_context = self._portfolio_policy_context(portfolio.execution_policy, portfolio_id=portfolio_id)
        policy_json = policy_context["policy_json"]
        capabilities: dict[str, Any] = {
            "portfolio_id": portfolio_id,
            "portfolio_data_source": portfolio.data_source.value,
            "algo_code": policy_context.get("algo_code"),
            "validated_execution_policy_id": policy_context.get("validated_execution_policy_id"),
            "modes": {},
        }
        for mode in PaperSessionMode:
            mode_payload: dict[str, Any] = {"can_start": False, "errors": []}
            try:
                if mode == PaperSessionMode.REPLAY_ONLY:
                    self._validate_sources(
                        mode=mode,
                        portfolio_data_source=portfolio.data_source,
                        historical_data_source=MinuteDataSource.DB_HISTORICAL,
                        live_data_source=None,
                    )
                    require_execution_algo_supports_mode(
                        policy_json,
                        mode="HISTORICAL",
                        package_id=portfolio.package_id,
                    )
                    mode_payload["can_start"] = True
                elif mode == PaperSessionMode.LIVE_ONLY:
                    self._validate_sources(
                        mode=mode,
                        portfolio_data_source=portfolio.data_source,
                        historical_data_source=None,
                        live_data_source=MinuteDataSource.TDX_REALTIME,
                    )
                    require_execution_algo_supports_mode(
                        policy_json,
                        mode=mode.value,
                        package_id=portfolio.package_id,
                    )
                    mode_payload["can_start"] = True
                elif mode == PaperSessionMode.CATCHUP_THEN_LIVE:
                    self._validate_sources(
                        mode=mode,
                        portfolio_data_source=portfolio.data_source,
                        historical_data_source=MinuteDataSource.DB_HISTORICAL,
                        live_data_source=MinuteDataSource.TDX_REALTIME,
                    )
                    require_execution_algo_supports_mode(
                        policy_json,
                        mode=mode.value,
                        package_id=portfolio.package_id,
                    )
                    mode_payload["can_start"] = True
            except TradingCoreError as exc:
                mode_payload["errors"].append(exc.to_dict())
            capabilities["modes"][mode.value] = mode_payload
        return capabilities

    def progress(self, session_id: str, *, event_limit: int = 100) -> PaperSessionProgress:
        session = self.repository.get_session(session_id)
        days = self.repository.list_session_days(session_id)
        current_day = days[-1] if days else None
        return PaperSessionProgress(
            session=session,
            current_trade_date=current_day.trade_date if current_day else None,
            last_processed_bar_time=current_day.last_processed_bar_time if current_day else None,
            latest_available_bar_time=current_day.latest_available_bar_time if current_day else None,
            next_expected_bar_time=None,
            day_count=len(days),
            events=self.repository.list_session_events(session_id, limit=event_limit),
        )

    def require_non_trading_operation_window(
        self,
        *,
        action: str,
        portfolio_id: str | None = None,
        session_id: str | None = None,
        as_of_time: datetime | None = None,
    ) -> None:
        self._require_non_trading_operation_window(
            action=action,
            portfolio_id=portfolio_id,
            session_id=session_id,
            as_of_time=as_of_time,
        )

    def pause(self, session_id: str) -> PaperTradingSession:
        self._require_non_trading_operation_window(action="pause_session", session_id=session_id)
        session = self.repository.get_session(session_id)
        if session.status in TERMINAL_SESSION_STATUSES:
            raise InvalidStateTransitionError(
                "terminal paper v2 session cannot be paused",
                context={"session_id": session_id, "status": session.status.value},
            )
        paused = self.repository.update_session_status(session_id, status=PaperSessionStatus.PAUSED)
        self.repository.save_session_event(
            session_id=session_id,
            event_type="SESSION_PAUSED",
            message="paper v2 trade session paused",
        )
        return paused

    def resume(self, session_id: str) -> PaperTradingSession:
        self._require_non_trading_operation_window(action="resume_session", session_id=session_id)
        session = self.repository.get_session(session_id)
        if session.status != PaperSessionStatus.PAUSED:
            raise InvalidStateTransitionError(
                "only paused paper v2 sessions can be resumed",
                context={"session_id": session_id, "status": session.status.value},
            )
        resumed_status = PaperSessionStatus.REPLAYING if session.mode == PaperSessionMode.REPLAY_ONLY else PaperSessionStatus.LIVE_WAITING_FOR_BAR
        resumed = self.repository.update_session_status(session_id, status=resumed_status)
        self.repository.save_session_event(
            session_id=session_id,
            event_type="SESSION_RESUMED",
            message="paper v2 trade session resumed",
            context={"status": resumed_status.value},
        )
        return resumed

    def stop(self, session_id: str) -> PaperTradingSession:
        self._require_non_trading_operation_window(action="stop_session", session_id=session_id)
        session = self.repository.get_session(session_id)
        if session.status in {PaperSessionStatus.SUCCEEDED, PaperSessionStatus.FAILED, PaperSessionStatus.STOPPED}:
            return session
        stopped = self.repository.update_session_status(
            session_id,
            status=PaperSessionStatus.STOPPED,
            completed_at=datetime.now(UTC),
        )
        self.repository.save_session_event(
            session_id=session_id,
            event_type="SESSION_STOPPED",
            message="paper v2 trade session stopped without deleting persisted artifacts",
        )
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        if portfolio.status in {PortfolioStatus.RUNNING, PortfolioStatus.PAUSED} and not self._has_running_run(portfolio.portfolio_id):
            self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
        return stopped

    def _require_non_trading_operation_window(
        self,
        *,
        action: str,
        portfolio_id: str | None = None,
        session_id: str | None = None,
        as_of_time: datetime | None = None,
    ) -> None:
        """Allow intraday operational recovery while keeping execution fail-fast.

        Older Paper v2 APIs used this hook to block all session and portfolio
        state changes from 09:15 to 15:00. That prevented real recovery
        scenarios, for example MiniQMT being unavailable in the morning and
        becoming usable after lunch. The time window is no longer a safety
        gate: local_sim, minqmt_sim, and future live paths may be started or
        stopped intraday. Actual trading safety remains enforced by runtime
        preflight, broker health checks, explicit MiniQMT/live authorization,
        order submission checks, and fail-fast data validation.
        """
        _ = (action, portfolio_id, session_id, as_of_time, self.enforce_non_trading_window)
        return

    def _has_running_run(self, portfolio_id: str) -> bool:
        try:
            return any(str(row.get("status") or "").upper() == "RUNNING" for row in self.repository.list_runs(portfolio_id, limit=10_000))
        except (DataUnavailableError, TradingCoreError) as exc:
            raise SessionConfigError(
                "paper v2 session could not verify running day-run state",
                context={"portfolio_id": portfolio_id, "cause": str(exc)},
            ) from exc

    @staticmethod
    def _portfolio_policy_context(execution_policy: dict[str, Any], *, portfolio_id: str) -> dict[str, Any]:
        policy_json = execution_policy.get("policy_json")
        policy_id = execution_policy.get("validated_execution_policy_id")
        policy_sha256 = execution_policy.get("policy_sha256")
        if not isinstance(policy_json, dict) or not policy_id or not policy_sha256:
            raise SessionConfigError(
                "paper v2 session requires a backtest-validated execution policy snapshot",
                context={"portfolio_id": portfolio_id},
            )
        return {
            "validated_execution_policy_id": str(policy_id),
            "policy_sha256": str(policy_sha256),
            "policy_name": execution_policy.get("policy_name"),
            "algo_code": execution_policy.get("algo_code") or policy_json.get("algo_code"),
            "source_backtest_id": execution_policy.get("source_backtest_id"),
            "source_backtest_status": execution_policy.get("source_backtest_status"),
            "validation_status": execution_policy.get("validation_status"),
            "paper_enabled": execution_policy.get("paper_enabled"),
            "policy_json": policy_json,
        }

    @staticmethod
    def _parse_mode(mode: PaperSessionMode | str) -> PaperSessionMode:
        try:
            return mode if isinstance(mode, PaperSessionMode) else PaperSessionMode(str(mode))
        except ValueError as exc:
            raise SessionConfigError(
                "unsupported Paper v2 trade session mode",
                context={"mode": str(mode), "supported_modes": [item.value for item in PaperSessionMode]},
            ) from exc

    @staticmethod
    def _parse_source(source: MinuteDataSource | str | None, *, field_name: str) -> MinuteDataSource | None:
        if source is None:
            return None
        try:
            return source if isinstance(source, MinuteDataSource) else MinuteDataSource(str(source))
        except ValueError as exc:
            raise SessionSourceUnsupportedError(
                "unsupported Paper v2 minute data source",
                context={"field": field_name, "source": str(source), "supported_sources": [item.value for item in MinuteDataSource]},
            ) from exc

    @staticmethod
    def _validate_dates(
        *,
        mode: PaperSessionMode,
        portfolio_start_date: date,
        start_date: date,
        end_date: date | None,
    ) -> None:
        if start_date < portfolio_start_date:
            raise SessionConfigError(
                "session start_date cannot be before portfolio start_date",
                context={"start_date": start_date.isoformat(), "portfolio_start_date": portfolio_start_date.isoformat()},
            )
        if mode == PaperSessionMode.REPLAY_ONLY and end_date is None:
            raise SessionConfigError("REPLAY_ONLY session requires end_date")
        if end_date is not None and end_date < start_date:
            raise SessionConfigError(
                "session end_date cannot be before start_date",
                context={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )

    @staticmethod
    def _validate_sources(
        *,
        mode: PaperSessionMode,
        portfolio_data_source: MinuteDataSource,
        historical_data_source: MinuteDataSource | None,
        live_data_source: MinuteDataSource | None,
    ) -> None:
        if mode == PaperSessionMode.REPLAY_ONLY:
            if historical_data_source is None:
                raise SessionConfigError("REPLAY_ONLY session requires historical_data_source")
            if live_data_source is not None:
                raise SessionConfigError("REPLAY_ONLY session must not set live_data_source")
            if historical_data_source not in SUPPORTED_HISTORICAL_SESSION_SOURCES:
                raise SessionSourceUnsupportedError(
                    "historical Paper v2 sessions require an implemented historical source",
                    context={"historical_data_source": historical_data_source.value},
                )
            if portfolio_data_source != historical_data_source:
                raise SessionConfigError(
                    "current replay runner requires portfolio data_source to match historical_data_source",
                    context={
                        "portfolio_data_source": portfolio_data_source.value,
                        "historical_data_source": historical_data_source.value,
                        "reason": "source-role split is not implemented in the closed-day runner yet",
                    },
                )
        elif mode == PaperSessionMode.LIVE_ONLY:
            if live_data_source is None:
                raise SessionConfigError("LIVE_ONLY session requires live_data_source")
            if historical_data_source is not None:
                raise SessionConfigError("LIVE_ONLY session must not set historical_data_source")
            if live_data_source not in SUPPORTED_LIVE_SESSION_SOURCES:
                raise SessionSourceUnsupportedError(
                    "live Paper v2 sessions require an implemented live source",
                    context={"live_data_source": live_data_source.value},
                )
            if portfolio_data_source != live_data_source:
                return
        elif mode == PaperSessionMode.CATCHUP_THEN_LIVE:
            if historical_data_source is None or live_data_source is None:
                raise SessionConfigError("CATCHUP_THEN_LIVE session requires both historical_data_source and live_data_source")
            if historical_data_source not in SUPPORTED_HISTORICAL_SESSION_SOURCES:
                raise SessionSourceUnsupportedError(
                    "catch-up historical source is not implemented",
                    context={"historical_data_source": historical_data_source.value},
                )
            if live_data_source not in SUPPORTED_LIVE_SESSION_SOURCES:
                raise SessionSourceUnsupportedError(
                    "catch-up live source is not implemented",
                    context={"live_data_source": live_data_source.value},
                )
            if portfolio_data_source != historical_data_source:
                raise SessionConfigError(
                    "CATCHUP_THEN_LIVE historical replay currently requires portfolio data_source to match historical_data_source",
                    context={
                        "portfolio_data_source": portfolio_data_source.value,
                        "historical_data_source": historical_data_source.value,
                    },
                )

    @staticmethod
    def _reject_raw_execution_overrides(runtime_config: dict[str, Any]) -> None:
        forbidden = {
            "algo_code",
            "algo_config",
            "execution_policy",
            "unfilled_handler",
            "unfilled_handler_params",
            "unfilled_policy",
            "validated_execution_policy",
        }
        present = sorted(key for key in forbidden if key in runtime_config)
        if present:
            raise SessionConfigError(
                "paper v2 session runtime_config cannot override execution policy; use a backtest-validated policy",
                context={"forbidden_keys": present},
            )

    @staticmethod
    def _initial_phase(mode: PaperSessionMode) -> PaperSessionPhase:
        if mode == PaperSessionMode.REPLAY_ONLY:
            return PaperSessionPhase.HISTORICAL_REPLAY
        if mode == PaperSessionMode.CATCHUP_THEN_LIVE:
            return PaperSessionPhase.HISTORICAL_REPLAY
        return PaperSessionPhase.LIVE_INTRADAY


class PaperTradingSessionRunner:
    """Process bounded units of Paper v2 session work.

    The first implemented unit is strict historical replay. Live modes remain
    fail-fast until incremental execution state and per-minute processing are
    implemented.
    """

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        package_repository: StrategyPackageRepository | Any | None = None,
        replay_service: PaperTradingHistoricalReplay | None = None,
        live_executor: PaperTradingLiveMinuteExecutor | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.package_repository = package_repository
        self.replay_service = replay_service or PaperTradingHistoricalReplay(
            repository=self.repository,
            package_repository=self.package_repository,
        )
        self.live_executor = live_executor or PaperTradingLiveMinuteExecutor(
            repository=self.repository,
            package_repository=self.package_repository,
        )

    def tick(
        self,
        session_id: str,
        *,
        as_of_time: datetime | None = None,
        allow_paused: bool = False,
    ) -> PaperSessionProgress:
        lock = _session_tick_lock(session_id)
        if not lock.acquire(blocking=False):
            exc = SessionLockTimeoutError(
                "paper v2 session is already being processed by another tick",
                context={"session_id": session_id},
            )
            try:
                session = self.repository.get_session(session_id)
                self.repository.save_session_event(
                    session_id=session_id,
                    event_type="SESSION_LOCK_TIMEOUT",
                    message=exc.message,
                    context=exc.context,
                )
            except TradingCoreError:
                raise exc
            raise exc
        try:
            with self.repository.session_tick_lock(session_id):
                session = self.repository.get_session(session_id)
                if session.status == PaperSessionStatus.PAUSED and not allow_paused:
                    self.repository.save_session_event(
                        session_id=session_id,
                        event_type="SESSION_TICK_SKIPPED",
                        message="paper v2 session tick skipped because session is paused",
                    )
                    return PaperTradingSessionService(repository=self.repository).progress(session_id)
                if session.status == PaperSessionStatus.PAUSED:
                    self.repository.save_session_event(
                        session_id=session_id,
                        event_type="SESSION_MANUAL_TICK_STARTED",
                        message="paper v2 paused manual-tick session claimed by explicit tick request",
                        context={"allow_paused": True},
                    )
                if session.status in TERMINAL_SESSION_STATUSES:
                    return PaperTradingSessionService(repository=self.repository).progress(session_id)
                if session.mode != PaperSessionMode.REPLAY_ONLY:
                    try:
                        return self.live_executor.tick(session, as_of_time=as_of_time)
                    except TradingCoreError as exc:
                        self._mark_failed(session, exc)
                        raise
                    except Exception as exc:
                        wrapped = TradingCoreError(
                            "paper v2 live session tick failed",
                            context={
                                "session_id": session.session_id,
                                "portfolio_id": session.portfolio_id,
                                "reason": f"{type(exc).__name__}: {exc}",
                            },
                        )
                        self._mark_failed(session, wrapped)
                        raise wrapped from exc
                return self._run_replay_only(session, as_of_time=as_of_time)
        finally:
            lock.release()

    def _run_replay_only(self, session: PaperTradingSession, *, as_of_time: datetime | None) -> PaperSessionProgress:
        if session.end_date is None or session.historical_data_source is None:
            error = SessionConfigError(
                "REPLAY_ONLY session is missing required replay fields",
                context={"session_id": session.session_id},
            )
            self._mark_failed(session, error)
            raise error
        started_at = session.started_at or (as_of_time or datetime.now(UTC))
        replay_status = (
            PaperSessionStatus.PAUSED
            if bool((session.runtime_config.get("paper_v2_session") or {}).get("manual_tick_only"))
            else PaperSessionStatus.REPLAYING
        )
        self.repository.update_session_status(
            session.session_id,
            status=replay_status,
            phase=PaperSessionPhase.HISTORICAL_REPLAY,
            started_at=started_at,
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="SESSION_REPLAY_STARTED",
            message="paper v2 historical replay session started",
            context={
                "start_date": session.start_date.isoformat(),
                "end_date": session.end_date.isoformat(),
                "historical_data_source": session.historical_data_source.value,
            },
        )
        replay_opts = dict(session.runtime_config.get("paper_v2_session") or {})
        try:
            result = self.replay_service.run(
                portfolio_id=session.portfolio_id,
                start_date=session.start_date,
                end_date=session.end_date,
                runtime_config=session.runtime_config,
                rerun_policy=str(replay_opts.get("rerun_policy") or "reject_existing"),
                confirm_reset=bool(replay_opts.get("confirm_reset", False)),
                confirm_text=replay_opts.get("confirm_text"),
            )
        except TradingCoreError as exc:
            self._mark_failed(session, exc)
            raise
        except Exception as exc:
            wrapped = TradingCoreError(
                "paper v2 session replay failed",
                context={"session_id": session.session_id, "portfolio_id": session.portfolio_id, "reason": f"{type(exc).__name__}: {exc}"},
            )
            self._mark_failed(session, wrapped)
            raise wrapped from exc
        self._persist_replay_days(session, result)
        completed = self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.SUCCEEDED,
            phase=PaperSessionPhase.HISTORICAL_REPLAY,
            completed_at=datetime.now(UTC),
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="SESSION_REPLAY_SUCCEEDED",
            message="paper v2 historical replay session succeeded",
            context={
                "trading_day_count": len(result.trading_days),
                "run_ids": [item.run_id for item in result.day_results],
            },
        )
        return PaperTradingSessionService(repository=self.repository).progress(completed.session_id)

    def _persist_replay_days(self, session: PaperTradingSession, result: PaperReplayResult) -> None:
        for day in result.day_results:
            self.repository.save_session_day(
                PaperSessionDay(
                    session_id=session.session_id,
                    portfolio_id=session.portfolio_id,
                    trade_date=day.trade_date,
                    run_id=day.run_id,
                    status=PaperSessionStatus.SUCCEEDED,
                    phase=PaperSessionPhase.HISTORICAL_REPLAY,
                    data_source=result.data_source,
                )
            )

    def _mark_failed(self, session: PaperTradingSession, exc: TradingCoreError) -> None:
        error = exc.to_dict()
        self._mark_current_live_run_failed(session, error)
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.FAILED,
            last_error=error,
            completed_at=datetime.now(UTC),
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="SESSION_FAILED",
            message=exc.message,
            context=error["context"],
        )
        self.repository.save_error(run_id=None, portfolio_id=session.portfolio_id, error=error)

    def _mark_current_live_run_failed(self, session: PaperTradingSession, error: dict[str, Any]) -> None:
        if session.mode == PaperSessionMode.REPLAY_ONLY:
            return
        run = self._resolve_current_live_run_for_failure(session, error)
        if run is None or run.status != RunStatus.RUNNING:
            return
        context = dict(error.get("context") or {})
        context.setdefault("session_id", session.session_id)
        context.setdefault("portfolio_id", session.portfolio_id)
        self.repository.save_error(run_id=run.run_id, portfolio_id=session.portfolio_id, error=error)
        self.repository.update_run_status(run, RunStatus.FAILED, error=error)
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="RUN_FAILED",
            message=str(error.get("message") or "paper v2 live run failed with its session"),
            context=context,
        )

    def _resolve_current_live_run_for_failure(
        self,
        session: PaperTradingSession,
        error: dict[str, Any],
    ):
        for day in reversed(self.repository.list_session_days(session.session_id)):
            if not day.run_id:
                continue
            try:
                run = self.repository.get_run(day.run_id)
            except TradingCoreError:
                continue
            if run.status == RunStatus.RUNNING:
                return run
        raw_trade_date = (error.get("context") or {}).get("trade_date")
        if raw_trade_date:
            try:
                run = self.repository.get_run_by_portfolio_date(
                    session.portfolio_id,
                    date.fromisoformat(str(raw_trade_date)),
                )
            except (TradingCoreError, ValueError):
                run = None
            if run is not None and run.status == RunStatus.RUNNING:
                return run
        for row in self.repository.list_runs(session.portfolio_id, limit=20):
            if str(row.get("status") or "").upper() != RunStatus.RUNNING.value:
                continue
            try:
                return self.repository.get_run(str(row["run_id"]))
            except TradingCoreError:
                continue
        return None
