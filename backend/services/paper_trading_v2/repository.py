"""Persistence repositories for Paper Trading v2."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError
from backend.services.trading_core.ledger import CashLedgerEntry
from backend.services.trading_core.models import AccountSnapshot, Fill, Order, OrderEvent, PositionLot, RunStatus

from .market_data import MinuteDataSource
from .models import (
    ConfigChangeType,
    ExecutionPolicyActivationStatus,
    IntradaySnapshot,
    OrderExecutionState,
    PaperConfigChangeAudit,
    PaperExecutionPolicyActivation,
    PaperPortfolio,
    PaperRun,
    PaperRuntimeConfigActivation,
    PaperRuntimeProfile,
    PaperRuntimeProfileVersion,
    PaperSessionDay,
    PaperSessionPhase,
    PaperSessionStatus,
    PaperTradingSession,
    PortfolioStatus,
    RuntimeConfigActivationStatus,
    RuntimeProfileStatus,
    RuntimeProfileValidationStatus,
)

ConnFactory = Callable[[], Iterator[Any]]


class PaperTradingV2Repository:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create_portfolio(self, portfolio: PaperPortfolio) -> PaperPortfolio:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.portfolio (
                        portfolio_id, portfolio_name, package_id, manifest_sha256,
                        frozen_manifest_json, initial_cash, start_date, data_source,
                        fee_policy, risk_policy, execution_policy, status,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        portfolio.portfolio_id,
                        portfolio.portfolio_name,
                        portfolio.package_id,
                        portfolio.manifest_sha256,
                        psycopg2.extras.Json(portfolio.frozen_manifest.model_dump(mode="json")),
                        portfolio.initial_cash,
                        portfolio.start_date,
                        portfolio.data_source.value,
                        psycopg2.extras.Json(portfolio.fee_policy),
                        psycopg2.extras.Json(portfolio.risk_policy),
                        psycopg2.extras.Json(portfolio.execution_policy),
                        portfolio.status.value,
                        portfolio.created_at,
                        portfolio.updated_at,
                    ),
                )
        return portfolio

    def get_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM paper_v2.portfolio WHERE portfolio_id = %s", (portfolio_id,))
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id})
        from backend.services.strategy_package.models import StrategyPackageManifest

        return PaperPortfolio(
            portfolio_id=row["portfolio_id"],
            portfolio_name=row["portfolio_name"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            frozen_manifest=StrategyPackageManifest.model_validate(row["frozen_manifest_json"]),
            initial_cash=float(row["initial_cash"]),
            start_date=row["start_date"],
            data_source=MinuteDataSource(row["data_source"]),
            fee_policy=row["fee_policy"] or {},
            risk_policy=row["risk_policy"] or {},
            execution_policy=row["execution_policy"] or {},
            status=PortfolioStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def list_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT portfolio_id FROM paper_v2.portfolio ORDER BY created_at DESC LIMIT %s", (limit,))
                ids = [row["portfolio_id"] for row in cur.fetchall()]
        return [self.get_portfolio(portfolio_id) for portfolio_id in ids]

    def update_portfolio_status(self, portfolio_id: str, status: PortfolioStatus) -> PaperPortfolio:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_v2.portfolio SET status = %s, updated_at = NOW() WHERE portfolio_id = %s",
                    (status.value, portfolio_id),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id})
        return self.get_portfolio(portfolio_id)

    def create_run(self, run: PaperRun) -> PaperRun:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.run (
                        run_id, portfolio_id, trade_date, status, data_source,
                        runtime_config, started_at, completed_at, error_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run.run_id,
                        run.portfolio_id,
                        run.trade_date,
                        run.status.value,
                        run.data_source.value,
                        psycopg2.extras.Json(run.runtime_config),
                        run.started_at,
                        run.completed_at,
                        psycopg2.extras.Json(run.error) if run.error else None,
                    ),
                )
        return run

    def get_run_by_portfolio_date(self, portfolio_id: str, trade_date: date) -> PaperRun | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run_id, portfolio_id, trade_date, status, data_source,
                           runtime_config, started_at, completed_at, error_json
                    FROM paper_v2.run
                    WHERE portfolio_id = %s AND trade_date = %s
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        if not row:
            return None
        return PaperRun(
            run_id=row["run_id"],
            portfolio_id=row["portfolio_id"],
            trade_date=row["trade_date"],
            status=RunStatus(row["status"]),
            data_source=MinuteDataSource(row["data_source"]),
            runtime_config=row["runtime_config"] or {},
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error=row["error_json"],
        )

    def update_run_status(self, run: PaperRun, status: RunStatus, error: dict[str, Any] | None = None) -> PaperRun:
        updated = run.model_copy(
            update={"status": status, "error": error, "completed_at": datetime.now(UTC) if status in {RunStatus.SUCCEEDED, RunStatus.FAILED} else run.completed_at}
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_v2.run SET status = %s, completed_at = %s, error_json = %s WHERE run_id = %s",
                    (
                        updated.status.value,
                        updated.completed_at,
                        psycopg2.extras.Json(error) if error else None,
                        updated.run_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise InvalidStateTransitionError("paper run update failed", context={"run_id": run.run_id})
        return updated

    def create_session(self, session: PaperTradingSession) -> PaperTradingSession:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.trade_session (
                        session_id, portfolio_id, mode, status, phase, start_date,
                        end_date, historical_data_source, live_data_source,
                        runtime_config_json, validated_execution_policy_json,
                        created_by, created_at, updated_at, started_at,
                        completed_at, last_error_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        session.session_id,
                        session.portfolio_id,
                        session.mode.value,
                        session.status.value,
                        session.phase.value,
                        session.start_date,
                        session.end_date,
                        session.historical_data_source.value if session.historical_data_source else None,
                        session.live_data_source.value if session.live_data_source else None,
                        psycopg2.extras.Json(session.runtime_config),
                        psycopg2.extras.Json(session.validated_execution_policy),
                        session.created_by,
                        session.created_at,
                        session.updated_at,
                        session.started_at,
                        session.completed_at,
                        psycopg2.extras.Json(session.last_error) if session.last_error else None,
                    ),
                )
        return session

    def get_session(self, session_id: str) -> PaperTradingSession:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM paper_v2.trade_session WHERE session_id = %s", (session_id,))
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("paper v2 trade session does not exist", context={"session_id": session_id})
        return self._session_from_row(dict(row))

    def list_sessions(self, portfolio_id: str, *, limit: int = 100) -> list[PaperTradingSession]:
        self.get_portfolio(portfolio_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.trade_session
            WHERE portfolio_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )
        return [self._session_from_row(row) for row in rows]

    def list_active_sessions(self, portfolio_id: str) -> list[PaperTradingSession]:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.trade_session
            WHERE portfolio_id = %s
              AND status NOT IN ('SUCCEEDED', 'FAILED', 'STOPPED')
            ORDER BY created_at DESC
            """,
            (portfolio_id,),
        )
        return [self._session_from_row(row) for row in rows]

    def list_tickable_sessions(
        self,
        *,
        statuses: set[PaperSessionStatus],
        limit: int = 100,
    ) -> list[PaperTradingSession]:
        if not statuses:
            return []
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.trade_session
            WHERE status = ANY(%s)
            ORDER BY updated_at ASC, created_at ASC
            LIMIT %s
            """,
            ([item.value for item in statuses], limit),
        )
        return [self._session_from_row(row) for row in rows]

    def update_session_status(
        self,
        session_id: str,
        *,
        status: PaperSessionStatus,
        phase: PaperSessionPhase | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        last_error: dict[str, Any] | None = None,
    ) -> PaperTradingSession:
        current = self.get_session(session_id)
        updated = current.model_copy(
            update={
                "status": status,
                "phase": phase or current.phase,
                "started_at": started_at if started_at is not None else current.started_at,
                "completed_at": completed_at if completed_at is not None else current.completed_at,
                "last_error": last_error,
                "updated_at": datetime.now(UTC),
            }
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.trade_session
                    SET status = %s, phase = %s, updated_at = %s, started_at = %s,
                        completed_at = %s, last_error_json = %s
                    WHERE session_id = %s
                    """,
                    (
                        updated.status.value,
                        updated.phase.value,
                        updated.updated_at,
                        updated.started_at,
                        updated.completed_at,
                        psycopg2.extras.Json(last_error) if last_error else None,
                        session_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError("paper v2 trade session does not exist", context={"session_id": session_id})
        return updated

    def save_session_day(self, day: PaperSessionDay) -> PaperSessionDay:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.session_day (
                        session_day_id, session_id, portfolio_id, trade_date, run_id,
                        status, phase, data_source, expected_bar_count,
                        latest_available_bar_time, last_processed_bar_time,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id, trade_date) DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        status = EXCLUDED.status,
                        phase = EXCLUDED.phase,
                        data_source = EXCLUDED.data_source,
                        expected_bar_count = EXCLUDED.expected_bar_count,
                        latest_available_bar_time = EXCLUDED.latest_available_bar_time,
                        last_processed_bar_time = EXCLUDED.last_processed_bar_time,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        day.session_day_id,
                        day.session_id,
                        day.portfolio_id,
                        day.trade_date,
                        day.run_id,
                        day.status.value,
                        day.phase.value,
                        day.data_source.value,
                        day.expected_bar_count,
                        day.latest_available_bar_time,
                        day.last_processed_bar_time,
                        day.created_at,
                        day.updated_at,
                    ),
                )
        return day

    def list_session_days(self, session_id: str) -> list[PaperSessionDay]:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.session_day
            WHERE session_id = %s
            ORDER BY trade_date
            """,
            (session_id,),
        )
        return [self._session_day_from_row(row) for row in rows]

    def save_session_event(
        self,
        *,
        session_id: str,
        event_type: str,
        message: str,
        run_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.session_events (session_id, run_id, event_type, message, context)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        session_id,
                        run_id,
                        event_type,
                        message,
                        psycopg2.extras.Json(context or {}),
                    ),
                )

    def list_session_events(self, session_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.session_events
            WHERE session_id = %s
            ORDER BY created_at ASC, event_id ASC
            LIMIT %s
            """,
            (session_id, limit),
        )

    def save_order_execution_state(self, state: OrderExecutionState) -> OrderExecutionState:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.order_execution_state (
                        execution_state_id, session_id, run_id, order_id, symbol,
                        trade_date, algo_code, algo_state_json, plan_json, plan_sha256,
                        last_processed_bar_time, filled_quantity, remaining_quantity,
                        status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        algo_state_json = EXCLUDED.algo_state_json,
                        plan_json = EXCLUDED.plan_json,
                        plan_sha256 = EXCLUDED.plan_sha256,
                        last_processed_bar_time = EXCLUDED.last_processed_bar_time,
                        filled_quantity = EXCLUDED.filled_quantity,
                        remaining_quantity = EXCLUDED.remaining_quantity,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        state.execution_state_id,
                        state.session_id,
                        state.run_id,
                        state.order_id,
                        state.symbol,
                        state.trade_date,
                        state.algo_code,
                        psycopg2.extras.Json(state.algo_state),
                        psycopg2.extras.Json(state.plan) if state.plan is not None else None,
                        state.plan_sha256,
                        state.last_processed_bar_time,
                        state.filled_quantity,
                        state.remaining_quantity,
                        state.status,
                        state.created_at,
                        state.updated_at,
                    ),
                )
        return state

    def list_order_execution_states(
        self,
        *,
        session_id: str,
        run_id: str | None = None,
    ) -> list[OrderExecutionState]:
        params: list[Any] = [session_id]
        run_filter = ""
        if run_id is not None:
            run_filter = " AND run_id = %s"
            params.append(run_id)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.order_execution_state
            WHERE session_id = %s{run_filter}
            ORDER BY created_at ASC, order_id ASC
            """,
            tuple(params),
        )
        return [self._order_execution_state_from_row(row) for row in rows]

    def save_intraday_snapshot(self, snapshot: IntradaySnapshot) -> IntradaySnapshot:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.intraday_snapshots (
                        snapshot_id, session_id, run_id, portfolio_id, trade_date,
                        snapshot_time, cash, market_value, nav, positions_json, source, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, snapshot_time) DO NOTHING
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.session_id,
                        snapshot.run_id,
                        snapshot.portfolio_id,
                        snapshot.trade_date,
                        snapshot.snapshot_time,
                        snapshot.cash,
                        snapshot.market_value,
                        snapshot.nav,
                        psycopg2.extras.Json(snapshot.positions),
                        snapshot.source,
                        snapshot.created_at,
                    ),
                )
        return snapshot

    def list_intraday_snapshots(
        self,
        *,
        session_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.intraday_snapshots
            WHERE session_id = %s
            ORDER BY snapshot_time DESC
            LIMIT %s
            """,
            (session_id, limit),
        )

    def save_execution_policy_activation(
        self,
        activation: PaperExecutionPolicyActivation,
    ) -> PaperExecutionPolicyActivation:
        self.get_portfolio(activation.portfolio_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.execution_policy_activation (
                        activation_id, portfolio_id, trade_date, policy_id, policy_sha256,
                        policy_name, policy_json, status, activated_at, activated_by,
                        reason, context, superseded_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        activation.activation_id,
                        activation.portfolio_id,
                        activation.trade_date,
                        activation.policy_id,
                        activation.policy_sha256,
                        activation.policy_name,
                        psycopg2.extras.Json(activation.policy_json),
                        activation.status.value,
                        activation.activated_at,
                        activation.activated_by,
                        activation.reason,
                        psycopg2.extras.Json(activation.context),
                        activation.superseded_at,
                    ),
                )
        active = self.get_active_execution_policy_activation(activation.portfolio_id, activation.trade_date)
        if active is None:
            raise InvalidStateTransitionError(
                "paper execution policy activation was not persisted as active",
                context={"activation_id": activation.activation_id},
            )
        return active

    def get_active_execution_policy_activation(
        self,
        portfolio_id: str,
        trade_date: date,
    ) -> PaperExecutionPolicyActivation | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM paper_v2.execution_policy_activation
                    WHERE portfolio_id = %s AND trade_date = %s AND status = 'ACTIVE'
                    ORDER BY activated_at DESC
                    LIMIT 1
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        return self._execution_policy_activation_from_row(dict(row)) if row else None

    def supersede_execution_policy_activation(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
    ) -> int:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.execution_policy_activation
                    SET status = 'SUPERSEDED', superseded_at = NOW()
                    WHERE portfolio_id = %s AND trade_date = %s AND status = 'ACTIVE'
                    """,
                    (portfolio_id, trade_date),
                )
                return int(cur.rowcount)

    def list_execution_policy_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperExecutionPolicyActivation]:
        self.get_portfolio(portfolio_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.execution_policy_activation
            WHERE portfolio_id = %s
            ORDER BY trade_date DESC, activated_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )
        return [self._execution_policy_activation_from_row(row) for row in rows]

    def save_runtime_profile(self, profile: PaperRuntimeProfile) -> PaperRuntimeProfile:
        self.get_portfolio(profile.portfolio_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.runtime_profile (
                        profile_id, portfolio_id, package_id, profile_name, status,
                        current_version_id, created_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        profile.profile_id,
                        profile.portfolio_id,
                        profile.package_id,
                        profile.profile_name,
                        profile.status.value,
                        profile.current_version_id,
                        profile.created_by,
                        profile.created_at,
                        profile.updated_at,
                    ),
                )
        return profile

    def update_runtime_profile_current_version(
        self,
        *,
        profile_id: str,
        current_version_id: str,
    ) -> PaperRuntimeProfile:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.runtime_profile
                    SET current_version_id = %s, updated_at = NOW()
                    WHERE profile_id = %s
                    """,
                    (current_version_id, profile_id),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError("paper v2 runtime profile does not exist", context={"profile_id": profile_id})
        return self.get_runtime_profile(profile_id)

    def get_runtime_profile(self, profile_id: str) -> PaperRuntimeProfile:
        rows = self._fetch_rows("SELECT * FROM paper_v2.runtime_profile WHERE profile_id = %s", (profile_id,))
        if not rows:
            raise DataUnavailableError("paper v2 runtime profile does not exist", context={"profile_id": profile_id})
        return self._runtime_profile_from_row(rows[0])

    def list_runtime_profiles(self, portfolio_id: str, *, limit: int = 100) -> list[PaperRuntimeProfile]:
        self.get_portfolio(portfolio_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.runtime_profile
            WHERE portfolio_id = %s
            ORDER BY updated_at DESC, created_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )
        return [self._runtime_profile_from_row(row) for row in rows]

    def save_runtime_profile_version(
        self,
        version: PaperRuntimeProfileVersion,
    ) -> PaperRuntimeProfileVersion:
        self.get_runtime_profile(version.profile_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.runtime_profile_version (
                        profile_version_id, profile_id, version_no, config_json,
                        config_sha256, validation_status, validation_errors,
                        created_by, reason, created_at, supersedes_version_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        version.profile_version_id,
                        version.profile_id,
                        version.version_no,
                        psycopg2.extras.Json(version.config_json),
                        version.config_sha256,
                        version.validation_status.value,
                        psycopg2.extras.Json(version.validation_errors),
                        version.created_by,
                        version.reason,
                        version.created_at,
                        version.supersedes_version_id,
                    ),
                )
        return version

    def get_runtime_profile_version(self, profile_version_id: str) -> PaperRuntimeProfileVersion:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.runtime_profile_version WHERE profile_version_id = %s",
            (profile_version_id,),
        )
        if not rows:
            raise DataUnavailableError(
                "paper v2 runtime profile version does not exist",
                context={"profile_version_id": profile_version_id},
            )
        return self._runtime_profile_version_from_row(rows[0])

    def list_runtime_profile_versions(
        self,
        profile_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeProfileVersion]:
        self.get_runtime_profile(profile_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.runtime_profile_version
            WHERE profile_id = %s
            ORDER BY version_no DESC, created_at DESC
            LIMIT %s
            """,
            (profile_id, limit),
        )
        return [self._runtime_profile_version_from_row(row) for row in rows]

    def save_runtime_config_activation(
        self,
        activation: PaperRuntimeConfigActivation,
    ) -> PaperRuntimeConfigActivation:
        self.get_portfolio(activation.portfolio_id)
        self.get_runtime_profile_version(activation.profile_version_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.runtime_config_activation (
                        activation_id, portfolio_id, trade_date, profile_version_id,
                        status, activated_at, activated_by, reason, context,
                        superseded_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        activation.activation_id,
                        activation.portfolio_id,
                        activation.trade_date,
                        activation.profile_version_id,
                        activation.status.value,
                        activation.activated_at,
                        activation.activated_by,
                        activation.reason,
                        psycopg2.extras.Json(activation.context),
                        activation.superseded_at,
                    ),
                )
        active = self.get_active_runtime_config_activation(activation.portfolio_id, activation.trade_date)
        if active is None:
            raise InvalidStateTransitionError(
                "paper runtime config activation was not persisted as active",
                context={"activation_id": activation.activation_id},
            )
        return active

    def get_active_runtime_config_activation(
        self,
        portfolio_id: str,
        trade_date: date,
    ) -> PaperRuntimeConfigActivation | None:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.runtime_config_activation
            WHERE portfolio_id = %s AND trade_date = %s AND status = 'ACTIVE'
            ORDER BY activated_at DESC
            LIMIT 1
            """,
            (portfolio_id, trade_date),
        )
        return self._runtime_config_activation_from_row(rows[0]) if rows else None

    def supersede_runtime_config_activation(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
    ) -> int:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.runtime_config_activation
                    SET status = 'SUPERSEDED', superseded_at = NOW()
                    WHERE portfolio_id = %s AND trade_date = %s AND status = 'ACTIVE'
                    """,
                    (portfolio_id, trade_date),
                )
                return int(cur.rowcount)

    def list_runtime_config_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeConfigActivation]:
        self.get_portfolio(portfolio_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.runtime_config_activation
            WHERE portfolio_id = %s
            ORDER BY trade_date DESC, activated_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )
        return [self._runtime_config_activation_from_row(row) for row in rows]

    def save_config_change_audit(self, audit: PaperConfigChangeAudit) -> PaperConfigChangeAudit:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.config_change_audit (
                        portfolio_id, package_id, object_type, object_id, change_type,
                        before_json, after_json, before_sha256, after_sha256,
                        reason, created_by, request_id, code_version, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        audit.portfolio_id,
                        audit.package_id,
                        audit.object_type,
                        audit.object_id,
                        audit.change_type.value,
                        psycopg2.extras.Json(audit.before_json) if audit.before_json is not None else None,
                        psycopg2.extras.Json(audit.after_json) if audit.after_json is not None else None,
                        audit.before_sha256,
                        audit.after_sha256,
                        audit.reason,
                        audit.created_by,
                        audit.request_id,
                        audit.code_version,
                        audit.created_at,
                    ),
                )
                row = dict(cur.fetchone())
        return self._config_change_audit_from_row(row)

    def list_config_change_audit(
        self,
        portfolio_id: str,
        *,
        limit: int = 200,
    ) -> list[PaperConfigChangeAudit]:
        self.get_portfolio(portfolio_id)
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.config_change_audit
            WHERE portfolio_id = %s
            ORDER BY created_at DESC, audit_id DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )
        return [self._config_change_audit_from_row(row) for row in rows]

    def list_runs(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT run_id, portfolio_id, trade_date, status, data_source,
                   runtime_config, started_at, completed_at, error_json
            FROM paper_v2.run
            WHERE portfolio_id = %s
            ORDER BY trade_date DESC, started_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    @staticmethod
    def _execution_policy_activation_from_row(row: dict[str, Any]) -> PaperExecutionPolicyActivation:
        return PaperExecutionPolicyActivation(
            activation_id=row["activation_id"],
            portfolio_id=row["portfolio_id"],
            trade_date=row["trade_date"],
            policy_id=row["policy_id"],
            policy_sha256=row["policy_sha256"],
            policy_name=row["policy_name"],
            policy_json=row["policy_json"] or {},
            status=ExecutionPolicyActivationStatus(row["status"]),
            activated_at=row["activated_at"],
            activated_by=row["activated_by"],
            reason=row["reason"],
            context=row["context"] or {},
            superseded_at=row["superseded_at"],
        )

    def save_order(self, run_id: str, order: Order) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.orders (
                        order_id, run_id, portfolio_id, package_id, intent_id, symbol,
                        side, quantity, order_type, limit_price, status,
                        filled_quantity, avg_fill_price, metadata, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(order_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        filled_quantity = EXCLUDED.filled_quantity,
                        avg_fill_price = EXCLUDED.avg_fill_price,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        order.order_id,
                        run_id,
                        order.portfolio_id,
                        order.package_id,
                        order.intent_id,
                        order.symbol,
                        order.side.value,
                        order.quantity,
                        order.order_type.value,
                        order.limit_price,
                        order.status.value,
                        order.filled_quantity,
                        order.avg_fill_price,
                        psycopg2.extras.Json(order.metadata),
                        order.created_at,
                        order.updated_at,
                    ),
                )

    def get_order(self, order_id: str) -> Order:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.orders WHERE order_id = %s",
            (order_id,),
        )
        if not rows:
            raise DataUnavailableError("paper v2 order does not exist", context={"order_id": order_id})
        return self._order_from_row(rows[0])

    def list_orders_for_run(self, run_id: str) -> list[Order]:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.orders
            WHERE run_id = %s
            ORDER BY created_at ASC, order_id ASC
            """,
            (run_id,),
        )
        return [self._order_from_row(row) for row in rows]

    def save_fill(self, run_id: str, fill: Fill) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.fills (
                        fill_id, run_id, order_id, symbol, side, quantity, price,
                        trade_time, bar_time, reason, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(fill_id) DO NOTHING
                    """,
                    (
                        fill.fill_id,
                        run_id,
                        fill.order_id,
                        fill.symbol,
                        fill.side.value,
                        fill.quantity,
                        fill.price,
                        fill.trade_time,
                        fill.bar_time,
                        fill.reason,
                        psycopg2.extras.Json(fill.metadata),
                    ),
                )

    def list_fills_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.fills
            WHERE run_id = %s
            ORDER BY trade_time ASC, fill_id ASC
            """,
            (run_id,),
        )

    def save_order_event(self, run_id: str, event: OrderEvent) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.order_events (
                        event_id, run_id, order_id, event_type, event_time,
                        reason, metadata, fill_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(event_id) DO NOTHING
                    """,
                    (
                        event.event_id,
                        run_id,
                        event.order_id,
                        event.event_type.value,
                        event.event_time,
                        event.reason,
                        psycopg2.extras.Json(event.metadata),
                        psycopg2.extras.Json(event.fill.model_dump(mode="json")) if event.fill else None,
                    ),
                )

    def save_cash_entry(self, run_id: str, entry: CashLedgerEntry) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.cash_ledger (
                        run_id, portfolio_id, fill_id, trade_date, symbol, side,
                        notional, fee, cash_delta, cash_after
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        entry.portfolio_id,
                        entry.fill_id,
                        entry.trade_date,
                        entry.symbol,
                        entry.side.value,
                        entry.notional,
                        entry.fee,
                        entry.cash_delta,
                        entry.cash_after,
                    ),
                )

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM paper_v2.positions WHERE run_id = %s", (run_id,))
                for position in positions:
                    price = prices[position.symbol]
                    cur.execute(
                        """
                        INSERT INTO paper_v2.positions (
                            run_id, portfolio_id, trade_date, symbol, quantity,
                            available_quantity, avg_cost, market_price, market_value, metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            run_id,
                            position.portfolio_id,
                            trade_date,
                            position.symbol,
                            position.quantity,
                            position.available_quantity,
                            position.avg_cost,
                            price,
                            position.quantity * price,
                            psycopg2.extras.Json({"position_trade_date": position.trade_date.isoformat()}),
                        ),
                    )

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot: AccountSnapshot, metadata: dict[str, Any] | None = None) -> None:
        metadata = metadata or {}
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.daily_snapshots (
                        run_id, portfolio_id, trade_date, cash, market_value, nav,
                        position_count, snapshot_time, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(portfolio_id, trade_date) DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        cash = EXCLUDED.cash,
                        market_value = EXCLUDED.market_value,
                        nav = EXCLUDED.nav,
                        position_count = EXCLUDED.position_count,
                        snapshot_time = EXCLUDED.snapshot_time,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        run_id,
                        snapshot.portfolio_id,
                        trade_date,
                        snapshot.cash,
                        snapshot.market_value,
                        snapshot.nav,
                        int(metadata.get("position_count") or 0),
                        snapshot.snapshot_time,
                        psycopg2.extras.Json(metadata),
                    ),
                )

    def save_run_event(self, *, run_id: str, event_type: str, message: str, context: dict[str, Any] | None = None) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO paper_v2.run_events (run_id, event_type, message, context) VALUES (%s, %s, %s, %s)",
                    (run_id, event_type, message, psycopg2.extras.Json(context or {})),
                )

    def save_error(self, *, run_id: str | None, portfolio_id: str | None, error: dict[str, Any]) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.errors (run_id, portfolio_id, error_code, message, context)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        portfolio_id,
                        error.get("error_code", "PAPER_V2_ERROR"),
                        error.get("message", "paper v2 error"),
                        psycopg2.extras.Json(error.get("context") or {}),
                    ),
                )

    def reset_portfolio_runs(
        self,
        *,
        portfolio_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, int]:
        self.get_portfolio(portfolio_id)
        params: list[Any] = [portfolio_id]
        date_filter = ""
        if start_date is not None:
            date_filter += " AND trade_date >= %s"
            params.append(start_date)
        if end_date is not None:
            date_filter += " AND trade_date <= %s"
            params.append(end_date)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s{date_filter}", tuple(params))
                run_ids = [row[0] for row in cur.fetchall()]
                counts = {
                    "order_execution_state": 0,
                    "intraday_snapshots": 0,
                    "session_day_run_links": 0,
                    "order_events": 0,
                    "fills": 0,
                    "cash_ledger": 0,
                    "positions": 0,
                    "daily_snapshots": 0,
                    "run_events": 0,
                    "errors": 0,
                    "orders": 0,
                    "run": 0,
                }
                if not run_ids:
                    return counts
                cur.execute("DELETE FROM paper_v2.order_execution_state WHERE run_id = ANY(%s)", (run_ids,))
                counts["order_execution_state"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.intraday_snapshots WHERE run_id = ANY(%s)", (run_ids,))
                counts["intraday_snapshots"] = cur.rowcount
                cur.execute("UPDATE paper_v2.session_day SET run_id = NULL, updated_at = NOW() WHERE run_id = ANY(%s)", (run_ids,))
                counts["session_day_run_links"] = cur.rowcount
                for table in ("order_events", "fills", "cash_ledger", "positions", "daily_snapshots", "run_events"):
                    cur.execute(f"DELETE FROM paper_v2.{table} WHERE run_id = ANY(%s)", (run_ids,))
                    counts[table] = cur.rowcount
                cur.execute(
                    "DELETE FROM paper_v2.errors WHERE portfolio_id = %s AND (run_id IS NULL OR run_id = ANY(%s))",
                    (portfolio_id, run_ids),
                )
                counts["errors"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.orders WHERE run_id = ANY(%s)", (run_ids,))
                counts["orders"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.run WHERE run_id = ANY(%s)", (run_ids,))
                counts["run"] = cur.rowcount
        return counts

    def save_reset_audit(
        self,
        *,
        portfolio_id: str,
        rerun_policy: str,
        start_date: date,
        end_date: date,
        confirm_text: str,
        deleted_counts: dict[str, int],
        status: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.reset_audit (
                        portfolio_id, rerun_policy, start_date, end_date, confirm_text,
                        deleted_counts, status, context
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING audit_id, portfolio_id, rerun_policy, start_date, end_date,
                              confirm_text, deleted_counts, status, context, created_at
                    """,
                    (
                        portfolio_id,
                        rerun_policy,
                        start_date,
                        end_date,
                        confirm_text,
                        psycopg2.extras.Json(deleted_counts),
                        status,
                        psycopg2.extras.Json(context or {}),
                    ),
                )
                row = cur.fetchone()
        return dict(row)

    def list_run_events(self, portfolio_id: str, *, run_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        params: list[Any] = [portfolio_id]
        run_filter = ""
        if run_id is not None:
            run_filter = " AND e.run_id = %s"
            params.append(run_id)
        params.append(limit)
        return self._fetch_rows(
            f"""
            SELECT e.*
            FROM paper_v2.run_events e
            JOIN paper_v2.run r ON r.run_id = e.run_id
            WHERE r.portfolio_id = %s{run_filter}
            ORDER BY e.event_seq ASC
            LIMIT %s
            """,
            tuple(params),
        )

    def list_errors(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.errors
            WHERE portfolio_id = %s
            ORDER BY created_at DESC, error_id DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    def load_latest_positions(self, portfolio_id: str, before_or_on: date) -> dict[str, PositionLot]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) portfolio_id, symbol, quantity,
                           available_quantity, avg_cost, trade_date
                    FROM paper_v2.positions
                    WHERE portfolio_id = %s AND trade_date <= %s
                    ORDER BY symbol, trade_date DESC, position_id DESC
                    """,
                    (portfolio_id, before_or_on),
                )
                rows = cur.fetchall()
        return {
            row["symbol"]: PositionLot(
                portfolio_id=row["portfolio_id"],
                symbol=row["symbol"],
                quantity=int(row["quantity"]),
                available_quantity=int(row["available_quantity"]),
                avg_cost=float(row["avg_cost"]),
                trade_date=row["trade_date"],
            )
            for row in rows
            if int(row["quantity"]) > 0
        }

    def load_latest_cash(self, portfolio: PaperPortfolio, before_or_on: date) -> float:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.cash_after
                    FROM paper_v2.cash_ledger c
                    JOIN paper_v2.run r ON r.run_id = c.run_id
                    WHERE c.portfolio_id = %s
                      AND c.trade_date <= %s
                    ORDER BY c.trade_date DESC, c.created_at DESC, c.cash_id DESC
                    LIMIT 1
                    """,
                    (portfolio.portfolio_id, before_or_on),
                )
                row = cur.fetchone()
                if row:
                    return float(row[0])
                cur.execute(
                    """
                    SELECT cash FROM paper_v2.daily_snapshots
                    WHERE portfolio_id = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    (portfolio.portfolio_id, before_or_on),
                )
                row = cur.fetchone()
        return float(row[0]) if row else float(portfolio.initial_cash)

    def list_orders(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT * FROM paper_v2.orders
            WHERE portfolio_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    def list_fills(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT f.*
            FROM paper_v2.fills f
            JOIN paper_v2.run r ON r.run_id = f.run_id
            WHERE r.portfolio_id = %s
            ORDER BY f.trade_time DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    def list_cash_ledger(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT c.*
            FROM paper_v2.cash_ledger c
            JOIN paper_v2.run r ON r.run_id = c.run_id
            WHERE r.portfolio_id = %s
            ORDER BY c.trade_date DESC, c.cash_id DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    def list_positions(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT * FROM paper_v2.positions
            WHERE portfolio_id = %s
            ORDER BY trade_date DESC, symbol
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    def list_daily_snapshots(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT * FROM paper_v2.daily_snapshots
            WHERE portfolio_id = %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (portfolio_id, limit),
        )

    @staticmethod
    def _runtime_profile_from_row(row: dict[str, Any]) -> PaperRuntimeProfile:
        return PaperRuntimeProfile(
            profile_id=row["profile_id"],
            portfolio_id=row["portfolio_id"],
            package_id=row["package_id"],
            profile_name=row["profile_name"],
            status=RuntimeProfileStatus(row["status"]),
            current_version_id=row.get("current_version_id"),
            created_by=row.get("created_by"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _runtime_profile_version_from_row(row: dict[str, Any]) -> PaperRuntimeProfileVersion:
        return PaperRuntimeProfileVersion(
            profile_version_id=row["profile_version_id"],
            profile_id=row["profile_id"],
            version_no=int(row["version_no"]),
            config_json=row["config_json"] or {},
            config_sha256=row["config_sha256"],
            validation_status=RuntimeProfileValidationStatus(row["validation_status"]),
            validation_errors=row["validation_errors"] or [],
            created_by=row.get("created_by"),
            reason=row.get("reason"),
            created_at=row["created_at"],
            supersedes_version_id=row.get("supersedes_version_id"),
        )

    @staticmethod
    def _runtime_config_activation_from_row(row: dict[str, Any]) -> PaperRuntimeConfigActivation:
        return PaperRuntimeConfigActivation(
            activation_id=row["activation_id"],
            portfolio_id=row["portfolio_id"],
            trade_date=row["trade_date"],
            profile_version_id=row["profile_version_id"],
            status=RuntimeConfigActivationStatus(row["status"]),
            activated_at=row["activated_at"],
            activated_by=row.get("activated_by"),
            reason=row.get("reason"),
            context=row.get("context") or {},
            superseded_at=row.get("superseded_at"),
        )

    @staticmethod
    def _config_change_audit_from_row(row: dict[str, Any]) -> PaperConfigChangeAudit:
        return PaperConfigChangeAudit(
            audit_id=row.get("audit_id"),
            portfolio_id=row.get("portfolio_id"),
            package_id=row.get("package_id"),
            object_type=row["object_type"],
            object_id=row["object_id"],
            change_type=ConfigChangeType(row["change_type"]),
            before_json=row.get("before_json"),
            after_json=row.get("after_json"),
            before_sha256=row.get("before_sha256"),
            after_sha256=row.get("after_sha256"),
            reason=row.get("reason"),
            created_by=row.get("created_by"),
            request_id=row.get("request_id"),
            code_version=row.get("code_version"),
            created_at=row["created_at"],
        )

    @staticmethod
    def _session_from_row(row: dict[str, Any]) -> PaperTradingSession:
        return PaperTradingSession(
            session_id=row["session_id"],
            portfolio_id=row["portfolio_id"],
            mode=row["mode"],
            status=row["status"],
            phase=row["phase"],
            start_date=row["start_date"],
            end_date=row.get("end_date"),
            historical_data_source=MinuteDataSource(row["historical_data_source"]) if row.get("historical_data_source") else None,
            live_data_source=MinuteDataSource(row["live_data_source"]) if row.get("live_data_source") else None,
            runtime_config=row.get("runtime_config_json") or {},
            validated_execution_policy=row.get("validated_execution_policy_json") or {},
            created_by=row.get("created_by"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            last_error=row.get("last_error_json"),
        )

    @staticmethod
    def _session_day_from_row(row: dict[str, Any]) -> PaperSessionDay:
        return PaperSessionDay(
            session_day_id=row["session_day_id"],
            session_id=row["session_id"],
            portfolio_id=row["portfolio_id"],
            trade_date=row["trade_date"],
            run_id=row.get("run_id"),
            status=row["status"],
            phase=row["phase"],
            data_source=MinuteDataSource(row["data_source"]),
            expected_bar_count=row.get("expected_bar_count"),
            latest_available_bar_time=row.get("latest_available_bar_time"),
            last_processed_bar_time=row.get("last_processed_bar_time"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _order_execution_state_from_row(row: dict[str, Any]) -> OrderExecutionState:
        return OrderExecutionState(
            execution_state_id=row["execution_state_id"],
            session_id=row["session_id"],
            run_id=row["run_id"],
            order_id=row["order_id"],
            symbol=row["symbol"],
            trade_date=row["trade_date"],
            algo_code=row["algo_code"],
            algo_state=row["algo_state_json"] or {},
            plan=row["plan_json"],
            plan_sha256=row["plan_sha256"],
            last_processed_bar_time=row["last_processed_bar_time"],
            filled_quantity=int(row["filled_quantity"]),
            remaining_quantity=int(row["remaining_quantity"]),
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _order_from_row(row: dict[str, Any]) -> Order:
        return Order(
            order_id=row["order_id"],
            intent_id=row["intent_id"],
            package_id=row["package_id"],
            portfolio_id=row["portfolio_id"],
            symbol=row["symbol"],
            side=row["side"],
            quantity=int(row["quantity"]),
            order_type=row["order_type"],
            limit_price=float(row["limit_price"]) if row.get("limit_price") is not None else None,
            status=row["status"],
            filled_quantity=int(row["filled_quantity"] or 0),
            avg_fill_price=float(row["avg_fill_price"]) if row.get("avg_fill_price") is not None else None,
            metadata=row.get("metadata") or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _fetch_rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]


class InMemoryPaperTradingV2Repository:
    def __init__(self) -> None:
        self.portfolios: dict[str, PaperPortfolio] = {}
        self.runs: dict[str, PaperRun] = {}
        self.orders: dict[str, list[Order]] = {}
        self.fills: dict[str, list[Fill]] = {}
        self.events: dict[str, list[OrderEvent]] = {}
        self.cash_entries: dict[str, list[CashLedgerEntry]] = {}
        self.positions: dict[str, list[PositionLot]] = {}
        self.snapshots: dict[str, AccountSnapshot] = {}
        self.errors: list[dict[str, Any]] = []
        self.run_events: list[dict[str, Any]] = []
        self.reset_audits: list[dict[str, Any]] = []
        self.execution_policy_activations: dict[str, PaperExecutionPolicyActivation] = {}
        self.runtime_profiles: dict[str, PaperRuntimeProfile] = {}
        self.runtime_profile_versions: dict[str, PaperRuntimeProfileVersion] = {}
        self.runtime_config_activations: dict[str, PaperRuntimeConfigActivation] = {}
        self.config_change_audits: list[PaperConfigChangeAudit] = []
        self.sessions: dict[str, PaperTradingSession] = {}
        self.session_days: dict[tuple[str, date], PaperSessionDay] = {}
        self.session_events: list[dict[str, Any]] = []
        self.order_execution_states: dict[str, OrderExecutionState] = {}
        self.intraday_snapshots: dict[tuple[str, datetime], IntradaySnapshot] = {}

    def create_portfolio(self, portfolio: PaperPortfolio) -> PaperPortfolio:
        self.portfolios[portfolio.portfolio_id] = portfolio
        return portfolio

    def get_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        try:
            return self.portfolios[portfolio_id]
        except KeyError as exc:
            raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id}) from exc

    def list_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        return list(self.portfolios.values())[:limit]

    def update_portfolio_status(self, portfolio_id: str, status: PortfolioStatus) -> PaperPortfolio:
        portfolio = self.get_portfolio(portfolio_id)
        updated = portfolio.model_copy(update={"status": status, "updated_at": datetime.now(UTC)})
        self.portfolios[portfolio_id] = updated
        return updated

    def create_run(self, run: PaperRun) -> PaperRun:
        self.runs[run.run_id] = run
        return run

    def get_run_by_portfolio_date(self, portfolio_id: str, trade_date: date) -> PaperRun | None:
        for run in self.runs.values():
            if run.portfolio_id == portfolio_id and run.trade_date == trade_date:
                return run
        return None

    def update_run_status(self, run: PaperRun, status: RunStatus, error: dict[str, Any] | None = None) -> PaperRun:
        updated = run.model_copy(
            update={"status": status, "error": error, "completed_at": datetime.now(UTC) if status in {RunStatus.SUCCEEDED, RunStatus.FAILED} else run.completed_at}
        )
        self.runs[run.run_id] = updated
        return updated

    def create_session(self, session: PaperTradingSession) -> PaperTradingSession:
        self.get_portfolio(session.portfolio_id)
        self.sessions[session.session_id] = session
        return session

    def get_session(self, session_id: str) -> PaperTradingSession:
        try:
            return self.sessions[session_id]
        except KeyError as exc:
            raise DataUnavailableError("paper v2 trade session does not exist", context={"session_id": session_id}) from exc

    def list_sessions(self, portfolio_id: str, *, limit: int = 100) -> list[PaperTradingSession]:
        self.get_portfolio(portfolio_id)
        rows = [session for session in self.sessions.values() if session.portfolio_id == portfolio_id]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows[:limit]

    def list_active_sessions(self, portfolio_id: str) -> list[PaperTradingSession]:
        terminal = {PaperSessionStatus.SUCCEEDED, PaperSessionStatus.FAILED, PaperSessionStatus.STOPPED}
        return [
            session
            for session in self.list_sessions(portfolio_id, limit=10_000)
            if session.status not in terminal
        ]

    def list_tickable_sessions(
        self,
        *,
        statuses: set[PaperSessionStatus],
        limit: int = 100,
    ) -> list[PaperTradingSession]:
        rows = [session for session in self.sessions.values() if session.status in statuses]
        rows.sort(key=lambda item: (item.updated_at, item.created_at))
        return rows[:limit]

    def update_session_status(
        self,
        session_id: str,
        *,
        status: PaperSessionStatus,
        phase: PaperSessionPhase | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        last_error: dict[str, Any] | None = None,
    ) -> PaperTradingSession:
        current = self.get_session(session_id)
        updated = current.model_copy(
            update={
                "status": status,
                "phase": phase or current.phase,
                "started_at": started_at if started_at is not None else current.started_at,
                "completed_at": completed_at if completed_at is not None else current.completed_at,
                "last_error": last_error,
                "updated_at": datetime.now(UTC),
            }
        )
        self.sessions[session_id] = updated
        return updated

    def save_session_day(self, day: PaperSessionDay) -> PaperSessionDay:
        self.session_days[(day.session_id, day.trade_date)] = day.model_copy(update={"updated_at": datetime.now(UTC)})
        return self.session_days[(day.session_id, day.trade_date)]

    def list_session_days(self, session_id: str) -> list[PaperSessionDay]:
        rows = [day for (stored_session_id, _date), day in self.session_days.items() if stored_session_id == session_id]
        rows.sort(key=lambda item: item.trade_date)
        return rows

    def save_session_event(
        self,
        *,
        session_id: str,
        event_type: str,
        message: str,
        run_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.session_events.append(
            {
                "event_id": len(self.session_events) + 1,
                "session_id": session_id,
                "run_id": run_id,
                "event_type": event_type,
                "message": message,
                "context": context or {},
                "created_at": datetime.now(UTC),
            }
        )

    def list_session_events(self, session_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return [dict(item) for item in self.session_events if item.get("session_id") == session_id][:limit]

    def save_order_execution_state(self, state: OrderExecutionState) -> OrderExecutionState:
        self.order_execution_states[state.order_id] = state
        return state

    def list_order_execution_states(
        self,
        *,
        session_id: str,
        run_id: str | None = None,
    ) -> list[OrderExecutionState]:
        rows = [
            state
            for state in self.order_execution_states.values()
            if state.session_id == session_id and (run_id is None or state.run_id == run_id)
        ]
        rows.sort(key=lambda item: (item.created_at, item.order_id))
        return rows

    def save_intraday_snapshot(self, snapshot: IntradaySnapshot) -> IntradaySnapshot:
        key = (snapshot.run_id, snapshot.snapshot_time)
        self.intraday_snapshots.setdefault(key, snapshot)
        return self.intraday_snapshots[key]

    def list_intraday_snapshots(
        self,
        *,
        session_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        rows = [
            snapshot.model_dump(mode="json")
            for snapshot in self.intraday_snapshots.values()
            if snapshot.session_id == session_id
        ]
        rows.sort(key=lambda item: item["snapshot_time"], reverse=True)
        return rows[:limit]

    def save_execution_policy_activation(
        self,
        activation: PaperExecutionPolicyActivation,
    ) -> PaperExecutionPolicyActivation:
        self.get_portfolio(activation.portfolio_id)
        if (
            activation.status == ExecutionPolicyActivationStatus.ACTIVE
            and self.get_active_execution_policy_activation(activation.portfolio_id, activation.trade_date) is not None
        ):
            raise InvalidStateTransitionError(
                "active execution policy activation already exists for portfolio trade_date",
                context={"portfolio_id": activation.portfolio_id, "trade_date": activation.trade_date.isoformat()},
            )
        self.execution_policy_activations[activation.activation_id] = activation
        return activation

    def get_active_execution_policy_activation(
        self,
        portfolio_id: str,
        trade_date: date,
    ) -> PaperExecutionPolicyActivation | None:
        active = [
            activation
            for activation in self.execution_policy_activations.values()
            if activation.portfolio_id == portfolio_id
            and activation.trade_date == trade_date
            and activation.status == ExecutionPolicyActivationStatus.ACTIVE
        ]
        active.sort(key=lambda item: item.activated_at, reverse=True)
        return active[0] if active else None

    def supersede_execution_policy_activation(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
    ) -> int:
        count = 0
        for activation_id, activation in list(self.execution_policy_activations.items()):
            if (
                activation.portfolio_id == portfolio_id
                and activation.trade_date == trade_date
                and activation.status == ExecutionPolicyActivationStatus.ACTIVE
            ):
                self.execution_policy_activations[activation_id] = activation.model_copy(
                    update={
                        "status": ExecutionPolicyActivationStatus.SUPERSEDED,
                        "superseded_at": datetime.now(UTC),
                    }
                )
                count += 1
        return count

    def list_execution_policy_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperExecutionPolicyActivation]:
        self.get_portfolio(portfolio_id)
        rows = [
            activation
            for activation in self.execution_policy_activations.values()
            if activation.portfolio_id == portfolio_id
        ]
        rows.sort(key=lambda item: (item.trade_date, item.activated_at), reverse=True)
        return rows[:limit]

    def save_runtime_profile(self, profile: PaperRuntimeProfile) -> PaperRuntimeProfile:
        self.get_portfolio(profile.portfolio_id)
        self.runtime_profiles[profile.profile_id] = profile
        return profile

    def update_runtime_profile_current_version(
        self,
        *,
        profile_id: str,
        current_version_id: str,
    ) -> PaperRuntimeProfile:
        profile = self.get_runtime_profile(profile_id)
        updated = profile.model_copy(
            update={
                "current_version_id": current_version_id,
                "updated_at": datetime.now(UTC),
            }
        )
        self.runtime_profiles[profile_id] = updated
        return updated

    def get_runtime_profile(self, profile_id: str) -> PaperRuntimeProfile:
        try:
            return self.runtime_profiles[profile_id]
        except KeyError as exc:
            raise DataUnavailableError("paper v2 runtime profile does not exist", context={"profile_id": profile_id}) from exc

    def list_runtime_profiles(self, portfolio_id: str, *, limit: int = 100) -> list[PaperRuntimeProfile]:
        self.get_portfolio(portfolio_id)
        rows = [profile for profile in self.runtime_profiles.values() if profile.portfolio_id == portfolio_id]
        rows.sort(key=lambda item: (item.updated_at, item.created_at), reverse=True)
        return rows[:limit]

    def save_runtime_profile_version(
        self,
        version: PaperRuntimeProfileVersion,
    ) -> PaperRuntimeProfileVersion:
        self.get_runtime_profile(version.profile_id)
        duplicate = [
            item
            for item in self.runtime_profile_versions.values()
            if item.profile_id == version.profile_id
            and (item.version_no == version.version_no or item.config_sha256 == version.config_sha256)
        ]
        if duplicate:
            raise InvalidStateTransitionError(
                "runtime profile version already exists for version_no or config hash",
                context={"profile_id": version.profile_id, "version_no": version.version_no},
            )
        self.runtime_profile_versions[version.profile_version_id] = version
        return version

    def get_runtime_profile_version(self, profile_version_id: str) -> PaperRuntimeProfileVersion:
        try:
            return self.runtime_profile_versions[profile_version_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "paper v2 runtime profile version does not exist",
                context={"profile_version_id": profile_version_id},
            ) from exc

    def list_runtime_profile_versions(
        self,
        profile_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeProfileVersion]:
        self.get_runtime_profile(profile_id)
        rows = [version for version in self.runtime_profile_versions.values() if version.profile_id == profile_id]
        rows.sort(key=lambda item: (item.version_no, item.created_at), reverse=True)
        return rows[:limit]

    def save_runtime_config_activation(
        self,
        activation: PaperRuntimeConfigActivation,
    ) -> PaperRuntimeConfigActivation:
        self.get_portfolio(activation.portfolio_id)
        self.get_runtime_profile_version(activation.profile_version_id)
        if (
            activation.status == RuntimeConfigActivationStatus.ACTIVE
            and self.get_active_runtime_config_activation(activation.portfolio_id, activation.trade_date) is not None
        ):
            raise InvalidStateTransitionError(
                "active runtime config activation already exists for portfolio trade_date",
                context={"portfolio_id": activation.portfolio_id, "trade_date": activation.trade_date.isoformat()},
            )
        self.runtime_config_activations[activation.activation_id] = activation
        return activation

    def get_active_runtime_config_activation(
        self,
        portfolio_id: str,
        trade_date: date,
    ) -> PaperRuntimeConfigActivation | None:
        active = [
            activation
            for activation in self.runtime_config_activations.values()
            if activation.portfolio_id == portfolio_id
            and activation.trade_date == trade_date
            and activation.status == RuntimeConfigActivationStatus.ACTIVE
        ]
        active.sort(key=lambda item: item.activated_at, reverse=True)
        return active[0] if active else None

    def supersede_runtime_config_activation(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
    ) -> int:
        count = 0
        for activation_id, activation in list(self.runtime_config_activations.items()):
            if (
                activation.portfolio_id == portfolio_id
                and activation.trade_date == trade_date
                and activation.status == RuntimeConfigActivationStatus.ACTIVE
            ):
                self.runtime_config_activations[activation_id] = activation.model_copy(
                    update={
                        "status": RuntimeConfigActivationStatus.SUPERSEDED,
                        "superseded_at": datetime.now(UTC),
                    }
                )
                count += 1
        return count

    def list_runtime_config_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeConfigActivation]:
        self.get_portfolio(portfolio_id)
        rows = [
            activation
            for activation in self.runtime_config_activations.values()
            if activation.portfolio_id == portfolio_id
        ]
        rows.sort(key=lambda item: (item.trade_date, item.activated_at), reverse=True)
        return rows[:limit]

    def save_config_change_audit(self, audit: PaperConfigChangeAudit) -> PaperConfigChangeAudit:
        saved = audit.model_copy(update={"audit_id": len(self.config_change_audits) + 1})
        self.config_change_audits.append(saved)
        return saved

    def list_config_change_audit(
        self,
        portfolio_id: str,
        *,
        limit: int = 200,
    ) -> list[PaperConfigChangeAudit]:
        self.get_portfolio(portfolio_id)
        rows = [item for item in self.config_change_audits if item.portfolio_id == portfolio_id]
        rows.sort(key=lambda item: (item.created_at, item.audit_id or 0), reverse=True)
        return rows[:limit]

    def list_runs(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows = [
            run.model_dump(mode="json")
            for run in self.runs.values()
            if run.portfolio_id == portfolio_id
        ]
        rows.sort(key=lambda item: (item["trade_date"], item["started_at"]), reverse=True)
        return rows[:limit]

    def save_order(self, run_id: str, order: Order) -> None:
        existing = [item for item in self.orders.get(run_id, []) if item.order_id != order.order_id]
        existing.append(order)
        self.orders[run_id] = existing

    def get_order(self, order_id: str) -> Order:
        for orders in self.orders.values():
            for order in orders:
                if order.order_id == order_id:
                    return order
        raise DataUnavailableError("paper v2 order does not exist", context={"order_id": order_id})

    def list_orders_for_run(self, run_id: str) -> list[Order]:
        return list(self.orders.get(run_id, []))

    def save_fill(self, run_id: str, fill: Fill) -> None:
        existing = self.fills.setdefault(run_id, [])
        if not any(item.fill_id == fill.fill_id for item in existing):
            existing.append(fill)

    def list_fills_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return [fill.model_dump(mode="json") for fill in self.fills.get(run_id, [])]

    def save_order_event(self, run_id: str, event: OrderEvent) -> None:
        existing = self.events.setdefault(run_id, [])
        if not any(item.event_id == event.event_id for item in existing):
            existing.append(event)

    def save_cash_entry(self, run_id: str, entry: CashLedgerEntry) -> None:
        self.cash_entries.setdefault(run_id, []).append(entry)

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        self.positions[run_id] = positions

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot: AccountSnapshot, metadata: dict[str, Any] | None = None) -> None:
        self.snapshots[run_id] = snapshot

    def save_run_event(self, *, run_id: str, event_type: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.run_events.append({"run_id": run_id, "event_type": event_type, "message": message, "context": context or {}})

    def save_error(self, *, run_id: str | None, portfolio_id: str | None, error: dict[str, Any]) -> None:
        self.errors.append({"run_id": run_id, "portfolio_id": portfolio_id, "error": error})

    def list_run_events(self, portfolio_id: str, *, run_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in self.run_events:
            item_run_id = item.get("run_id")
            run = self.runs.get(str(item_run_id))
            if not run or run.portfolio_id != portfolio_id:
                continue
            if run_id is not None and item_run_id != run_id:
                continue
            rows.append(dict(item))
        return rows[:limit]

    def list_errors(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return [dict(item) for item in self.errors if item.get("portfolio_id") == portfolio_id][:limit]

    def reset_portfolio_runs(
        self,
        *,
        portfolio_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, int]:
        self.get_portfolio(portfolio_id)
        run_ids = [
            run_id
            for run_id, run in self.runs.items()
            if run.portfolio_id == portfolio_id
            and (start_date is None or run.trade_date >= start_date)
            and (end_date is None or run.trade_date <= end_date)
        ]
        counts = {
            "order_execution_state": 0,
            "intraday_snapshots": 0,
            "session_day_run_links": 0,
            "order_events": 0,
            "fills": 0,
            "cash_ledger": 0,
            "positions": 0,
            "daily_snapshots": 0,
            "run_events": 0,
            "errors": 0,
            "orders": 0,
            "run": 0,
        }
        for run_id in run_ids:
            removed_states = [key for key, state in self.order_execution_states.items() if state.run_id == run_id]
            for key in removed_states:
                self.order_execution_states.pop(key, None)
            counts["order_execution_state"] += len(removed_states)
            removed_snaps = [key for key, snap in self.intraday_snapshots.items() if snap.run_id == run_id]
            for key in removed_snaps:
                self.intraday_snapshots.pop(key, None)
            counts["intraday_snapshots"] += len(removed_snaps)
            for key, day in list(self.session_days.items()):
                if day.run_id == run_id:
                    self.session_days[key] = day.model_copy(update={"run_id": None, "updated_at": datetime.now(UTC)})
                    counts["session_day_run_links"] += 1
            counts["orders"] += len(self.orders.pop(run_id, []))
            counts["fills"] += len(self.fills.pop(run_id, []))
            counts["order_events"] += len(self.events.pop(run_id, []))
            counts["cash_ledger"] += len(self.cash_entries.pop(run_id, []))
            counts["positions"] += len(self.positions.pop(run_id, []))
            counts["daily_snapshots"] += 1 if self.snapshots.pop(run_id, None) is not None else 0
            self.runs.pop(run_id, None)
            counts["run"] += 1
        before_events = len(self.run_events)
        self.run_events = [item for item in self.run_events if item.get("run_id") not in run_ids]
        counts["run_events"] = before_events - len(self.run_events)
        before_errors = len(self.errors)
        self.errors = [
            item
            for item in self.errors
            if item.get("portfolio_id") != portfolio_id or (item.get("run_id") is not None and item.get("run_id") not in run_ids)
        ]
        counts["errors"] = before_errors - len(self.errors)
        return counts

    def save_reset_audit(
        self,
        *,
        portfolio_id: str,
        rerun_policy: str,
        start_date: date,
        end_date: date,
        confirm_text: str,
        deleted_counts: dict[str, int],
        status: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not hasattr(self, "reset_audits"):
            self.reset_audits = []
        audit = {
            "audit_id": len(self.reset_audits) + 1,
            "portfolio_id": portfolio_id,
            "rerun_policy": rerun_policy,
            "start_date": start_date,
            "end_date": end_date,
            "confirm_text": confirm_text,
            "deleted_counts": dict(deleted_counts),
            "status": status,
            "context": context or {},
            "created_at": datetime.now(UTC),
        }
        self.reset_audits.append(audit)
        return audit

    def load_latest_positions(self, portfolio_id: str, before_or_on: date) -> dict[str, PositionLot]:
        candidates: list[PositionLot] = []
        for positions in self.positions.values():
            candidates.extend([pos for pos in positions if pos.portfolio_id == portfolio_id and pos.trade_date <= before_or_on])
        by_symbol: dict[str, PositionLot] = {}
        for pos in candidates:
            if pos.quantity <= 0:
                continue
            existing = by_symbol.get(pos.symbol)
            if existing is None or pos.trade_date >= existing.trade_date:
                by_symbol[pos.symbol] = pos
        return by_symbol

    def load_latest_cash(self, portfolio: PaperPortfolio, before_or_on: date) -> float:
        latest_cash_entry: tuple[date, datetime, float] | None = None
        for run_id, entries in self.cash_entries.items():
            run = self.runs.get(run_id)
            if not run or run.portfolio_id != portfolio.portfolio_id:
                continue
            for entry in entries:
                if entry.trade_date > before_or_on:
                    continue
                created_at = datetime.now(UTC)
                if latest_cash_entry is None or (entry.trade_date, created_at) >= (latest_cash_entry[0], latest_cash_entry[1]):
                    latest_cash_entry = (entry.trade_date, created_at, entry.cash_after)
        if latest_cash_entry is not None:
            return latest_cash_entry[2]
        latest: tuple[date, AccountSnapshot] | None = None
        for run_id, snapshot in self.snapshots.items():
            run = self.runs.get(run_id)
            if not run or run.portfolio_id != portfolio.portfolio_id or run.trade_date > before_or_on:
                continue
            if latest is None or run.trade_date >= latest[0]:
                latest = (run.trade_date, snapshot)
        return latest[1].cash if latest else portfolio.initial_cash

    def list_orders(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id, orders in self.orders.items():
            run = self.runs.get(run_id)
            if run and run.portfolio_id == portfolio_id:
                rows.extend(order.model_dump(mode="json") for order in orders)
        return rows[:limit]

    def list_fills(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id, fills in self.fills.items():
            run = self.runs.get(run_id)
            if run and run.portfolio_id == portfolio_id:
                rows.extend(fill.model_dump(mode="json") for fill in fills)
        return rows[:limit]

    def list_cash_ledger(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id, entries in self.cash_entries.items():
            run = self.runs.get(run_id)
            if run and run.portfolio_id == portfolio_id:
                for entry in entries:
                    item = asdict(entry)
                    item["side"] = entry.side.value
                    item["run_id"] = run_id
                    rows.append(item)
        return rows[:limit]

    def list_positions(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for positions in self.positions.values():
            rows.extend(pos.model_dump(mode="json") for pos in positions if pos.portfolio_id == portfolio_id)
        return rows[:limit]

    def list_daily_snapshots(self, portfolio_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id, snapshot in self.snapshots.items():
            run = self.runs.get(run_id)
            if run and run.portfolio_id == portfolio_id:
                item = snapshot.model_dump(mode="json")
                item["run_id"] = run_id
                item["trade_date"] = run.trade_date
                item["position_count"] = len(self.positions.get(run_id, []))
                rows.append(item)
        return rows[:limit]
