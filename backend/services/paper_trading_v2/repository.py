"""Persistence repositories for Paper Trading v2."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, date, datetime
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, SessionLockTimeoutError
from backend.services.trading_core.ledger import CashLedgerEntry
from backend.services.trading_core.models import AccountSnapshot, Fill, Order, OrderEvent, PositionLot, RunStatus

from .market_data import MinuteDataSource
from .models import (
    BrokerAccountBindingStatus,
    ConfigChangeType,
    ExecutionPolicyActivationStatus,
    IntradaySnapshot,
    OrderExecutionState,
    PaperBrokerAccountBinding,
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
from .symbol_names import PaperV2SymbolNameResolver

ConnFactory = Callable[[], Iterator[Any]]

RUNNING_SUMMARY_ACTIVE_STATUSES = (
    PortfolioStatus.RUNNING.value,
    PortfolioStatus.PAUSED.value,
)
RUNNING_SUMMARY_TICKABLE_SESSION_STATUSES = {
    PaperSessionStatus.CREATED.value,
    PaperSessionStatus.PREFLIGHTING.value,
    PaperSessionStatus.REPLAYING.value,
    PaperSessionStatus.CATCHING_UP.value,
    PaperSessionStatus.SWITCHING_TO_LIVE.value,
    PaperSessionStatus.LIVE_RUNNING.value,
    PaperSessionStatus.LIVE_WAITING_FOR_BAR.value,
    PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY.value,
    PaperSessionStatus.LIVE_WAITING_MARKET_WINDOW.value,
    PaperSessionStatus.LIVE_WAITING_PLATFORM_DATA.value,
    PaperSessionStatus.LIVE_WAITING_BROKER.value,
    PaperSessionStatus.LIVE_RETRYING.value,
}
RUNNING_SUMMARY_SORT_COLUMNS = {
    "portfolio_name": "portfolio_name",
    "status": "status",
    "initial_cash": "initial_cash",
    "latest_run_time": "latest_run_time",
    "created_at": "created_at",
    "updated_at": "updated_at",
}
RUNNING_SUMMARY_SEARCH_COLUMNS = {
    "portfolio_name": "p.portfolio_name",
    "portfolio_id": "p.portfolio_id",
    "package_id": "p.package_id",
    "manifest_sha256": "p.manifest_sha256",
    "status": "p.status",
    "data_source": "p.data_source",
    "initial_cash": "p.initial_cash",
    "latest_run_status": "lr.status",
    "latest_run_trade_date": "lr.trade_date",
    "latest_run_time": "COALESCE(lr.started_at, lr.completed_at)",
}


def _running_summary_operability(
    *,
    portfolio_status: PortfolioStatus,
    latest_session: PaperTradingSession | None,
    tickable_session_count: int,
) -> dict[str, Any]:
    no_operable = portfolio_status in {PortfolioStatus.RUNNING, PortfolioStatus.PAUSED} and tickable_session_count == 0
    latest_status = latest_session.status.value if latest_session else None
    latest_mode = latest_session.mode.value if latest_session else None
    latest_terminal = latest_status in {
        PaperSessionStatus.SUCCEEDED.value,
        PaperSessionStatus.FAILED.value,
        PaperSessionStatus.STOPPED.value,
    } if latest_status else False
    hint = None
    if no_operable:
        hint = (
            "Portfolio status is active but no scheduler-tickable live/replay session exists. "
            "Review the latest terminal session, then create or resume a session; intraday recovery is allowed."
        )
    return {
        "tickable_session_count": tickable_session_count,
        "has_tickable_session": tickable_session_count > 0,
        "latest_session_status": latest_status,
        "latest_session_mode": latest_mode,
        "latest_session_is_terminal": latest_terminal,
        "no_operable_session": no_operable,
        "remediation_hint": hint,
    }


class PaperTradingV2Repository:
    def __init__(
        self,
        conn_factory: ConnFactory | None = None,
        *,
        symbol_name_resolver: PaperV2SymbolNameResolver | Any | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self._symbol_name_resolver = symbol_name_resolver or PaperV2SymbolNameResolver(self._conn_factory)

    def _resolve_stock_names(self, symbols: list[str | None] | tuple[str | None, ...]) -> dict[str, str]:
        try:
            return self._symbol_name_resolver.resolve(symbols)
        except Exception:
            # Stock names are display/audit metadata only; trading writes must
            # never fail because a reference-table lookup failed.
            return {}

    @contextmanager
    def session_tick_lock(self, session_id: str) -> Iterator[None]:
        """Hold a PostgreSQL advisory lock so multiple backend processes do not tick one session."""

        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(2402, hashtext(%s))", (session_id,))
                locked = bool(cur.fetchone()[0])
            if not locked:
                raise SessionLockTimeoutError(
                    "paper v2 session is already being processed by another backend process",
                    context={"session_id": session_id, "lock_scope": "postgres_advisory_lock"},
                )
            try:
                yield
            finally:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(2402, hashtext(%s))", (session_id,))

    def create_portfolio(self, portfolio: PaperPortfolio) -> PaperPortfolio:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.portfolio (
                        portfolio_id, portfolio_name, package_id, manifest_sha256,
                        frozen_manifest_json, initial_cash, start_date, data_source,
                        broker_backend, fee_policy, risk_policy, execution_policy, status,
                        auto_run_enabled, auto_run_config, auto_run_config_sha256,
                        auto_run_updated_at, auto_run_updated_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        portfolio.broker_backend,
                        psycopg2.extras.Json(portfolio.fee_policy),
                        psycopg2.extras.Json(portfolio.risk_policy),
                        psycopg2.extras.Json(portfolio.execution_policy),
                        portfolio.status.value,
                        portfolio.auto_run_enabled,
                        psycopg2.extras.Json(portfolio.auto_run_config),
                        portfolio.auto_run_config_sha256,
                        portfolio.auto_run_updated_at,
                        portfolio.auto_run_updated_by,
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
        return self._portfolio_from_row(dict(row))

    def list_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT portfolio_id FROM paper_v2.portfolio ORDER BY created_at DESC LIMIT %s", (limit,))
                ids = [row["portfolio_id"] for row in cur.fetchall()]
        return [self.get_portfolio(portfolio_id) for portfolio_id in ids]

    def list_running_summaries(
        self,
        *,
        limit: int = 100,
        snapshot_limit: int = 30,
        position_limit: int = 8,
    ) -> list[dict[str, Any]]:
        return self.list_running_summaries_page(
            page=1,
            page_size=limit,
            snapshot_limit=snapshot_limit,
            position_limit=position_limit,
        )["summaries"]

    def list_running_summaries_page(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        snapshot_limit: int = 30,
        position_limit: int = 8,
        statuses: list[str] | None = None,
        sort_by: str = "latest_run_time",
        sort_dir: str = "desc",
        search: str | None = None,
        search_fields: list[str] | None = None,
        min_initial_cash: float | None = None,
        max_initial_cash: float | None = None,
    ) -> dict[str, Any]:
        """Return a compact running-portfolio dashboard in one backend call.

        The UI used to fan out seven requests per active portfolio, which can
        exhaust browser connection/request resources once many validation
        portfolios exist. This method keeps the same persisted data sources but
        aggregates them server-side without triggering trading side effects.
        """

        if page <= 0 or page_size <= 0 or snapshot_limit <= 0 or position_limit <= 0:
            raise DataUnavailableError(
                "paper v2 running summary limits must be positive",
                context={
                    "page": page,
                    "page_size": page_size,
                    "snapshot_limit": snapshot_limit,
                    "position_limit": position_limit,
                },
            )
        if min_initial_cash is not None and max_initial_cash is not None and min_initial_cash > max_initial_cash:
            raise DataUnavailableError(
                "paper v2 running summary initial cash filter is invalid",
                context={"min_initial_cash": min_initial_cash, "max_initial_cash": max_initial_cash},
            )

        status_values = [str(item).strip().upper() for item in (statuses or list(RUNNING_SUMMARY_ACTIVE_STATUSES)) if str(item).strip()]
        invalid_statuses = [item for item in status_values if item not in {status.value for status in PortfolioStatus}]
        if invalid_statuses:
            raise DataUnavailableError(
                "paper v2 running summary status filter is invalid",
                context={"statuses": statuses, "invalid_statuses": invalid_statuses},
            )
        if not status_values:
            raise DataUnavailableError(
                "paper v2 running summary requires at least one status",
                context={"statuses": statuses},
            )

        normalized_sort_by = str(sort_by or "latest_run_time").strip().lower()
        if normalized_sort_by not in RUNNING_SUMMARY_SORT_COLUMNS:
            raise DataUnavailableError(
                "paper v2 running summary sort field is invalid",
                context={"sort_by": sort_by, "allowed_sort_fields": sorted(RUNNING_SUMMARY_SORT_COLUMNS)},
            )
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise DataUnavailableError(
                "paper v2 running summary sort direction is invalid",
                context={"sort_dir": sort_dir, "allowed_sort_dirs": ["asc", "desc"]},
            )

        requested_search_fields = [str(item).strip().lower() for item in (search_fields or []) if str(item).strip()]
        if not requested_search_fields or "all" in requested_search_fields:
            requested_search_fields = list(RUNNING_SUMMARY_SEARCH_COLUMNS)
        invalid_search_fields = [field for field in requested_search_fields if field not in RUNNING_SUMMARY_SEARCH_COLUMNS]
        if invalid_search_fields:
            raise DataUnavailableError(
                "paper v2 running summary search field is invalid",
                context={
                    "search_fields": search_fields,
                    "invalid_search_fields": invalid_search_fields,
                    "allowed_search_fields": sorted(RUNNING_SUMMARY_SEARCH_COLUMNS),
                },
            )

        where_clauses = ["p.status = ANY(%s)"]
        params: list[Any] = [status_values]
        if min_initial_cash is not None:
            where_clauses.append("p.initial_cash >= %s")
            params.append(min_initial_cash)
        if max_initial_cash is not None:
            where_clauses.append("p.initial_cash <= %s")
            params.append(max_initial_cash)
        normalized_search = str(search or "").strip()
        if normalized_search:
            search_like = f"%{normalized_search}%"
            where_clauses.append(
                "("
                + " OR ".join(
                    f"CAST({RUNNING_SUMMARY_SEARCH_COLUMNS[field]} AS TEXT) ILIKE %s"
                    for field in requested_search_fields
                )
                + ")"
            )
            params.extend([search_like] * len(requested_search_fields))

        where_sql = " AND ".join(where_clauses)
        filtered_cte = f"""
            WITH latest_run AS (
                SELECT *
                FROM (
                    SELECT r.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.portfolio_id
                               ORDER BY r.started_at DESC NULLS LAST, r.completed_at DESC NULLS LAST, r.run_id DESC
                           ) AS rn
                    FROM paper_v2.run r
                ) ranked_run
                WHERE rn = 1
            ),
            filtered AS (
                SELECT p.*,
                       lr.run_id AS latest_run_id,
                       lr.trade_date AS latest_run_trade_date,
                       lr.status AS latest_run_status,
                       lr.data_source AS latest_run_data_source,
                       lr.runtime_config AS latest_run_runtime_config,
                       lr.started_at AS latest_run_started_at,
                       lr.completed_at AS latest_run_completed_at,
                       lr.error_json AS latest_run_error_json,
                       COALESCE(lr.started_at, lr.completed_at) AS latest_run_time
                FROM paper_v2.portfolio p
                LEFT JOIN latest_run lr ON lr.portfolio_id = p.portfolio_id
                WHERE {where_sql}
            )
        """
        sort_column = RUNNING_SUMMARY_SORT_COLUMNS[normalized_sort_by]
        offset = (page - 1) * page_size
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"{filtered_cte} SELECT COUNT(*) AS total FROM filtered",
                    tuple(params),
                )
                total = int((cur.fetchone() or {}).get("total") or 0)
                cur.execute(
                    f"""
                    {filtered_cte}
                    SELECT *
                    FROM filtered
                    ORDER BY {sort_column} {normalized_sort_dir.upper()} NULLS LAST,
                             created_at DESC NULLS LAST,
                             portfolio_id ASC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [page_size, offset]),
                )
                portfolio_rows = [dict(row) for row in cur.fetchall()]
                portfolio_ids = [row["portfolio_id"] for row in portfolio_rows]
                if not portfolio_ids:
                    return {
                        "summaries": [],
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total": total,
                            "total_pages": 0,
                            "sort_by": normalized_sort_by,
                            "sort_dir": normalized_sort_dir,
                            "statuses": status_values,
                            "search": normalized_search or None,
                            "search_fields": requested_search_fields,
                            "min_initial_cash": min_initial_cash,
                            "max_initial_cash": max_initial_cash,
                        },
                    }

                latest_runs: dict[str, dict[str, Any]] = {}
                for row in portfolio_rows:
                    if row.get("latest_run_id"):
                        latest_runs[row["portfolio_id"]] = {
                            "run_id": row["latest_run_id"],
                            "portfolio_id": row["portfolio_id"],
                            "trade_date": row["latest_run_trade_date"],
                            "status": row["latest_run_status"],
                            "data_source": row["latest_run_data_source"],
                            "runtime_config": row["latest_run_runtime_config"] or {},
                            "started_at": row["latest_run_started_at"],
                            "completed_at": row["latest_run_completed_at"],
                            "error_json": row["latest_run_error_json"],
                        }

                cur.execute(
                    """
                    SELECT *
                    FROM (
                        SELECT s.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY s.portfolio_id
                                   ORDER BY s.created_at DESC NULLS LAST, s.updated_at DESC NULLS LAST, s.session_id DESC
                               ) AS rn
                        FROM paper_v2.trade_session s
                        WHERE s.portfolio_id = ANY(%s)
                    ) ranked
                    WHERE rn = 1
                    """,
                    (portfolio_ids,),
                )
                latest_sessions = {row["portfolio_id"]: self._session_from_row(dict(row)) for row in cur.fetchall()}

                cur.execute(
                    """
                    SELECT portfolio_id, COUNT(*) AS tickable_session_count
                    FROM paper_v2.trade_session
                    WHERE portfolio_id = ANY(%s)
                      AND status = ANY(%s)
                    GROUP BY portfolio_id
                    """,
                    (portfolio_ids, sorted(RUNNING_SUMMARY_TICKABLE_SESSION_STATUSES)),
                )
                tickable_session_counts = {
                    row["portfolio_id"]: int(row["tickable_session_count"] or 0)
                    for row in cur.fetchall()
                }

                cur.execute(
                    """
                    SELECT p.portfolio_id,
                           COALESCE(o.order_count, 0) AS order_count,
                           COALESCE(f.fill_count, 0) AS fill_count,
                           COALESCE(e.error_count, 0) AS error_count
                    FROM unnest(%s::text[]) AS p(portfolio_id)
                    LEFT JOIN (
                        SELECT portfolio_id, COUNT(*) AS order_count
                        FROM paper_v2.orders
                        WHERE portfolio_id = ANY(%s)
                        GROUP BY portfolio_id
                    ) o ON o.portfolio_id = p.portfolio_id
                    LEFT JOIN (
                        SELECT r.portfolio_id, COUNT(*) AS fill_count
                        FROM paper_v2.fills f
                        JOIN paper_v2.run r ON r.run_id = f.run_id
                        WHERE r.portfolio_id = ANY(%s)
                        GROUP BY r.portfolio_id
                    ) f ON f.portfolio_id = p.portfolio_id
                    LEFT JOIN (
                        SELECT portfolio_id, COUNT(*) AS error_count
                        FROM paper_v2.errors
                        WHERE portfolio_id = ANY(%s)
                        GROUP BY portfolio_id
                    ) e ON e.portfolio_id = p.portfolio_id
                    """,
                    (portfolio_ids, portfolio_ids, portfolio_ids, portfolio_ids),
                )
                counts = {row["portfolio_id"]: dict(row) for row in cur.fetchall()}

                cur.execute(
                    """
                    SELECT *
                    FROM (
                        SELECT ds.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ds.portfolio_id
                                   ORDER BY ds.trade_date DESC, ds.snapshot_id DESC
                               ) AS rn
                        FROM paper_v2.daily_snapshots ds
                        WHERE ds.portfolio_id = ANY(%s)
                    ) ranked
                    WHERE rn <= %s
                    ORDER BY portfolio_id, trade_date DESC
                    """,
                    (portfolio_ids, snapshot_limit),
                )
                snapshots_by_portfolio: dict[str, list[dict[str, Any]]] = {portfolio_id: [] for portfolio_id in portfolio_ids}
                for row in cur.fetchall():
                    payload = dict(row)
                    payload.pop("rn", None)
                    snapshots_by_portfolio.setdefault(payload["portfolio_id"], []).append(payload)

                cur.execute(
                    """
                    WITH latest_date AS (
                        SELECT portfolio_id, MAX(trade_date) AS trade_date
                        FROM paper_v2.positions
                        WHERE portfolio_id = ANY(%s)
                        GROUP BY portfolio_id
                    )
                    SELECT *
                    FROM (
                        SELECT p.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p.portfolio_id
                                   ORDER BY p.symbol ASC, p.position_id DESC
                               ) AS rn
                        FROM paper_v2.positions p
                        JOIN latest_date d
                          ON d.portfolio_id = p.portfolio_id
                         AND d.trade_date = p.trade_date
                    ) ranked
                    WHERE rn <= %s
                    ORDER BY portfolio_id, symbol
                    """,
                    (portfolio_ids, position_limit),
                )
                positions_by_portfolio: dict[str, list[dict[str, Any]]] = {portfolio_id: [] for portfolio_id in portfolio_ids}
                for row in cur.fetchall():
                    payload = dict(row)
                    payload.pop("rn", None)
                    positions_by_portfolio.setdefault(payload["portfolio_id"], []).append(payload)

        summaries: list[dict[str, Any]] = []
        for row in portfolio_rows:
            portfolio = self._portfolio_from_row(row)
            portfolio_id = portfolio.portfolio_id
            recent_snapshots = snapshots_by_portfolio.get(portfolio_id, [])
            latest_positions = positions_by_portfolio.get(portfolio_id, [])
            count_row = counts.get(portfolio_id, {})
            summaries.append(
                {
                    "portfolio": portfolio,
                    "latest_run": latest_runs.get(portfolio_id),
                    "latest_session": latest_sessions.get(portfolio_id),
                    "operability": _running_summary_operability(
                        portfolio_status=portfolio.status,
                        latest_session=latest_sessions.get(portfolio_id),
                        tickable_session_count=tickable_session_counts.get(portfolio_id, 0),
                    ),
                    "latest_snapshot": recent_snapshots[0] if recent_snapshots else None,
                    "recent_snapshots": recent_snapshots,
                    "latest_positions": latest_positions,
                    "counts": {
                        "orders": int(count_row.get("order_count") or 0),
                        "fills": int(count_row.get("fill_count") or 0),
                        "positions": len(latest_positions),
                        "errors": int(count_row.get("error_count") or 0),
                    },
                }
            )
        return {
            "summaries": summaries,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size,
                "sort_by": normalized_sort_by,
                "sort_dir": normalized_sort_dir,
                "statuses": status_values,
                "search": normalized_search or None,
                "search_fields": requested_search_fields,
                "min_initial_cash": min_initial_cash,
                "max_initial_cash": max_initial_cash,
            },
        }

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

    def update_portfolio_auto_run(
        self,
        portfolio_id: str,
        *,
        enabled: bool,
        config: dict[str, Any],
        config_sha256: str,
        updated_by: str | None = None,
    ) -> PaperPortfolio:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.portfolio
                    SET auto_run_enabled = %s,
                        auto_run_config = %s,
                        auto_run_config_sha256 = %s,
                        auto_run_updated_at = NOW(),
                        auto_run_updated_by = %s,
                        updated_at = NOW()
                    WHERE portfolio_id = %s
                    """,
                    (
                        enabled,
                        psycopg2.extras.Json(config),
                        config_sha256,
                        updated_by,
                        portfolio_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id})
        return self.get_portfolio(portfolio_id)

    def list_auto_run_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.portfolio
            WHERE auto_run_enabled IS TRUE
              AND status IN ('READY', 'RUNNING', 'PAUSED')
            ORDER BY auto_run_updated_at DESC NULLS LAST, updated_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [self._portfolio_from_row(row) for row in rows]

    def create_broker_account_binding(self, binding: PaperBrokerAccountBinding) -> PaperBrokerAccountBinding:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.broker_account_binding (
                            binding_id, broker_backend, broker_mode, broker_account_id,
                            portfolio_id, binding_status, allocation_mode, initial_cash,
                            created_at, updated_at, created_by
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            binding.binding_id,
                            binding.broker_backend,
                            binding.broker_mode,
                            binding.broker_account_id,
                            binding.portfolio_id,
                            binding.binding_status.value,
                            binding.allocation_mode,
                            binding.initial_cash,
                            binding.created_at,
                            binding.updated_at,
                            binding.created_by,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "MiniQMT account already has an active Paper v2 auto-run binding",
                        context={
                            "broker_backend": binding.broker_backend,
                            "broker_mode": binding.broker_mode,
                            "broker_account_id": binding.broker_account_id,
                            "portfolio_id": binding.portfolio_id,
                            "allocation_mode": binding.allocation_mode,
                        },
                    ) from exc
        return binding

    def get_active_broker_account_binding(
        self,
        *,
        broker_backend: str,
        broker_mode: str,
        broker_account_id: str,
    ) -> PaperBrokerAccountBinding | None:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.broker_account_binding
            WHERE broker_backend = %s
              AND broker_mode = %s
              AND broker_account_id = %s
              AND binding_status = 'ACTIVE'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (broker_backend, broker_mode, broker_account_id),
        )
        return self._broker_account_binding_from_row(rows[0]) if rows else None

    def list_active_broker_account_bindings(self, portfolio_id: str | None = None) -> list[PaperBrokerAccountBinding]:
        if portfolio_id:
            rows = self._fetch_rows(
                """
                SELECT *
                FROM paper_v2.broker_account_binding
                WHERE portfolio_id = %s AND binding_status = 'ACTIVE'
                ORDER BY updated_at DESC
                """,
                (portfolio_id,),
            )
        else:
            rows = self._fetch_rows(
                """
                SELECT *
                FROM paper_v2.broker_account_binding
                WHERE binding_status = 'ACTIVE'
                ORDER BY updated_at DESC
                """,
                (),
            )
        return [self._broker_account_binding_from_row(row) for row in rows]

    def update_broker_account_binding_status(
        self,
        binding_id: str,
        status: BrokerAccountBindingStatus,
    ) -> PaperBrokerAccountBinding:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.broker_account_binding
                    SET binding_status = %s, updated_at = NOW()
                    WHERE binding_id = %s
                    """,
                    (status.value, binding_id),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError(
                        "paper v2 broker account binding does not exist",
                        context={"binding_id": binding_id},
                    )
        rows = self._fetch_rows("SELECT * FROM paper_v2.broker_account_binding WHERE binding_id = %s", (binding_id,))
        return self._broker_account_binding_from_row(rows[0])

    def delete_portfolio(self, portfolio_id: str) -> dict[str, int]:
        self.get_portfolio(portfolio_id)
        counts = {
            "broker_account_binding": 0,
            "selection_paper_portfolio_link": 0,
            "order_execution_state": 0,
            "intraday_snapshots": 0,
            "session_events": 0,
            "session_day": 0,
            "trade_session": 0,
            "execution_policy_activation": 0,
            "runtime_config_activation": 0,
            "runtime_profile_version": 0,
            "runtime_profile": 0,
            "config_change_audit": 0,
            "reset_audit": 0,
            "errors": 0,
            "order_events": 0,
            "fills": 0,
            "cash_ledger": 0,
            "positions": 0,
            "daily_snapshots": 0,
            "run_events": 0,
            "orders": 0,
            "run": 0,
            "portfolio": 0,
        }
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s", (portfolio_id,))
                run_ids = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT session_id FROM paper_v2.trade_session WHERE portfolio_id = %s", (portfolio_id,))
                session_ids = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT profile_id FROM paper_v2.runtime_profile WHERE portfolio_id = %s", (portfolio_id,))
                profile_ids = [row[0] for row in cur.fetchall()]

                cur.execute(
                    "UPDATE paper_v2.broker_account_binding SET binding_status = 'RETIRED', updated_at = NOW() WHERE portfolio_id = %s AND binding_status = 'ACTIVE'",
                    (portfolio_id,),
                )
                counts["broker_account_binding"] = cur.rowcount
                cur.execute("DELETE FROM selection.paper_portfolio_link WHERE portfolio_id = %s", (portfolio_id,))
                counts["selection_paper_portfolio_link"] = cur.rowcount
                if run_ids:
                    cur.execute("DELETE FROM paper_v2.order_execution_state WHERE run_id = ANY(%s)", (run_ids,))
                    counts["order_execution_state"] += cur.rowcount
                    for table in ("order_events", "fills", "cash_ledger", "positions", "daily_snapshots", "run_events"):
                        cur.execute(f"DELETE FROM paper_v2.{table} WHERE run_id = ANY(%s)", (run_ids,))
                        counts[table] = cur.rowcount
                    cur.execute("DELETE FROM paper_v2.orders WHERE run_id = ANY(%s)", (run_ids,))
                    counts["orders"] = cur.rowcount
                    cur.execute("DELETE FROM paper_v2.run WHERE run_id = ANY(%s)", (run_ids,))
                    counts["run"] = cur.rowcount
                if session_ids:
                    cur.execute("DELETE FROM paper_v2.order_execution_state WHERE session_id = ANY(%s)", (session_ids,))
                    counts["order_execution_state"] += cur.rowcount
                    cur.execute("DELETE FROM paper_v2.session_events WHERE session_id = ANY(%s)", (session_ids,))
                    counts["session_events"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.intraday_snapshots WHERE portfolio_id = %s", (portfolio_id,))
                counts["intraday_snapshots"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.session_day WHERE portfolio_id = %s", (portfolio_id,))
                counts["session_day"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.trade_session WHERE portfolio_id = %s", (portfolio_id,))
                counts["trade_session"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.execution_policy_activation WHERE portfolio_id = %s", (portfolio_id,))
                counts["execution_policy_activation"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.runtime_config_activation WHERE portfolio_id = %s", (portfolio_id,))
                counts["runtime_config_activation"] = cur.rowcount
                if profile_ids:
                    cur.execute("DELETE FROM paper_v2.runtime_profile_version WHERE profile_id = ANY(%s)", (profile_ids,))
                    counts["runtime_profile_version"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.runtime_profile WHERE portfolio_id = %s", (portfolio_id,))
                counts["runtime_profile"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.config_change_audit WHERE portfolio_id = %s", (portfolio_id,))
                counts["config_change_audit"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.reset_audit WHERE portfolio_id = %s", (portfolio_id,))
                counts["reset_audit"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.errors WHERE portfolio_id = %s", (portfolio_id,))
                counts["errors"] = cur.rowcount
                cur.execute("DELETE FROM paper_v2.portfolio WHERE portfolio_id = %s", (portfolio_id,))
                counts["portfolio"] = cur.rowcount
        return counts

    def create_run(self, run: PaperRun) -> PaperRun:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.run (
                        run_id, portfolio_id, trade_date, status, data_source,
                        runtime_config, started_at, completed_at, error_json,
                        model_params_origin
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        run.model_params_origin,
                    ),
                )
        return run

    def update_run_model_params_origin(
        self, run: PaperRun, model_params_origin: str
    ) -> PaperRun:
        """Record the resolved provenance of model params on an existing run.

        Called by the live inference flow once
        ``StrategyPackageLiveInference.prepare_workspace`` returns a
        ``PreparedInferenceWorkspace`` whose ``model_params_origin`` is known.
        """

        if model_params_origin not in ("node", "cache", "unavailable"):
            raise InvalidStateTransitionError(
                "invalid paper v2 run.model_params_origin value",
                context={
                    "run_id": run.run_id,
                    "model_params_origin": model_params_origin,
                },
            )
        updated = run.model_copy(update={"model_params_origin": model_params_origin})
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_v2.run SET model_params_origin = %s WHERE run_id = %s",
                    (model_params_origin, run.run_id),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError(
                        "paper v2 run does not exist",
                        context={"run_id": run.run_id},
                    )
        return updated

    def get_run_by_portfolio_date(self, portfolio_id: str, trade_date: date) -> PaperRun | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run_id, portfolio_id, trade_date, status, data_source,
                           runtime_config, started_at, completed_at, error_json,
                           model_params_origin
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
            model_params_origin=row["model_params_origin"],
        )

    def get_run(self, run_id: str) -> PaperRun:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run_id, portfolio_id, trade_date, status, data_source,
                           runtime_config, started_at, completed_at, error_json,
                           model_params_origin
                    FROM paper_v2.run
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("paper v2 run does not exist", context={"run_id": run_id})
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
            model_params_origin=row["model_params_origin"],
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

    def update_run_runtime_config(self, run: PaperRun, runtime_config: dict[str, Any]) -> PaperRun:
        updated = run.model_copy(update={"runtime_config": runtime_config})
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_v2.run SET runtime_config = %s WHERE run_id = %s",
                    (psycopg2.extras.Json(runtime_config), run.run_id),
                )
                if cur.rowcount != 1:
                    raise DataUnavailableError("paper v2 run does not exist", context={"run_id": run.run_id})
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

    def list_intraday_snapshots_for_portfolio(
        self,
        portfolio_id: str,
        *,
        trade_date: date | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [portfolio_id]
        date_filter = ""
        if trade_date is not None:
            date_filter = " AND trade_date = %s"
            params.append(trade_date)
        params.append(limit)
        return self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.intraday_snapshots
            WHERE portfolio_id = %s{date_filter}
            ORDER BY snapshot_time DESC
            LIMIT %s
            """,
            tuple(params),
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
        stock_name = self._resolve_stock_names([order.symbol]).get(order.symbol)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.orders (
                        order_id, run_id, portfolio_id, package_id, intent_id, symbol,
                        stock_name, side, quantity, order_type, limit_price, status,
                        filled_quantity, avg_fill_price, metadata, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(order_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        filled_quantity = EXCLUDED.filled_quantity,
                        avg_fill_price = EXCLUDED.avg_fill_price,
                        metadata = EXCLUDED.metadata,
                        stock_name = COALESCE(orders.stock_name, EXCLUDED.stock_name),
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        order.order_id,
                        run_id,
                        order.portfolio_id,
                        order.package_id,
                        order.intent_id,
                        order.symbol,
                        stock_name,
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

    def save_fill(
        self,
        run_id: str,
        fill: Fill,
        *,
        intended_price: float | None = None,
        fill_market_context: dict[str, Any] | None = None,
    ) -> None:
        # T5 capture fields for DW ETL: intended_price + fill_market_context
        # are NULLable. Default None preserves backward compat for any caller
        # that has not threaded the values through yet (and is the recorded
        # value when the order has no intended price — i.e. MARKET orders).
        # T6.1 wired the production callers (day_runner.py + live_session.py)
        # to pass OrderIntent.limit_price / Order.limit_price as intended_price
        # and the market_input.market_context dict (the same one fed to the
        # execution engine) as fill_market_context.
        # created_at / updated_at are passed explicitly via now() so the
        # InMemoryPaperTradingV2Repository fallback (which does not have
        # DEFAULT NOW()) sees the same value the PG path writes.
        # R6 sentinel endpoint path: when caller did not pass kwargs but
        # threaded values via Fill.metadata (coldstart_sentinel.py), allow
        # metadata.get() to fill in the kwargs.
        if isinstance(fill.metadata, dict):
            if intended_price is None:
                intended_price = fill.metadata.get("intended_price")
            if fill_market_context is None:
                fill_market_context = fill.metadata.get("fill_market_context")
        stock_name = self._resolve_stock_names([fill.symbol]).get(fill.symbol)
        now = datetime.now(UTC)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.fills (
                        fill_id, run_id, order_id, symbol, stock_name, side, quantity, price,
                        trade_time, bar_time, reason, metadata,
                        created_at, updated_at, intended_price, fill_market_context
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s)
                    ON CONFLICT(fill_id) DO NOTHING
                    """,
                    (
                        fill.fill_id,
                        run_id,
                        fill.order_id,
                        fill.symbol,
                        stock_name,
                        fill.side.value,
                        fill.quantity,
                        fill.price,
                        fill.trade_time,
                        fill.bar_time,
                        fill.reason,
                        psycopg2.extras.Json(fill.metadata),
                        now,
                        now,
                        intended_price,
                        psycopg2.extras.Json(fill_market_context) if fill_market_context is not None else None,
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

    def list_order_events(self, portfolio_id: str, *, run_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        params: list[Any] = [portfolio_id]
        run_filter = ""
        if run_id is not None:
            run_filter = " AND oe.run_id = %s"
            params.append(run_id)
        params.append(limit)
        return self._fetch_rows(
            f"""
            SELECT oe.*, o.symbol, o.side, o.quantity AS order_quantity,
                   o.filled_quantity AS order_filled_quantity,
                   o.avg_fill_price AS order_avg_fill_price,
                   o.status AS order_status
            FROM paper_v2.order_events oe
            JOIN paper_v2.run r ON r.run_id = oe.run_id
            LEFT JOIN paper_v2.orders o ON o.order_id = oe.order_id
            WHERE r.portfolio_id = %s{run_filter}
            ORDER BY oe.event_time DESC, oe.event_id DESC
            LIMIT %s
            """,
            tuple(params),
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
        stock_name = self._resolve_stock_names([entry.symbol]).get(entry.symbol)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.cash_ledger (
                        run_id, portfolio_id, fill_id, trade_date, symbol, stock_name, side,
                        notional, fee, cash_delta, cash_after
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        entry.portfolio_id,
                        entry.fill_id,
                        entry.trade_date,
                        entry.symbol,
                        stock_name,
                        entry.side.value,
                        entry.notional,
                        entry.fee,
                        entry.cash_delta,
                        entry.cash_after,
                    ),
                )

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        # T5: created_at / updated_at watermark fields for DW ETL. Passed
        # explicitly via now() so InMemory parity is preserved.
        now = datetime.now(UTC)
        stock_names = self._resolve_stock_names([position.symbol for position in positions])
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM paper_v2.positions WHERE run_id = %s", (run_id,))
                for position in positions:
                    price = prices[position.symbol]
                    cur.execute(
                        """
                        INSERT INTO paper_v2.positions (
                            run_id, portfolio_id, trade_date, symbol, stock_name, quantity,
                            available_quantity, avg_cost, market_price, market_value, metadata,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            run_id,
                            position.portfolio_id,
                            trade_date,
                            position.symbol,
                            stock_names.get(position.symbol),
                            position.quantity,
                            position.available_quantity,
                            position.avg_cost,
                            price,
                            position.quantity * price,
                            psycopg2.extras.Json({"position_trade_date": position.trade_date.isoformat()}),
                            now,
                            now,
                        ),
                    )

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot: AccountSnapshot, metadata: dict[str, Any] | None = None) -> None:
        metadata = metadata or {}
        # T5: created_at / updated_at watermark fields for DW ETL. updated_at
        # is bumped on every upsert (ON CONFLICT path); created_at is set on
        # INSERT only and preserved on conflict.
        now = datetime.now(UTC)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_v2.daily_snapshots (
                        run_id, portfolio_id, trade_date, cash, market_value, nav,
                        position_count, snapshot_time, metadata,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(portfolio_id, trade_date) DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        cash = EXCLUDED.cash,
                        market_value = EXCLUDED.market_value,
                        nav = EXCLUDED.nav,
                        position_count = EXCLUDED.position_count,
                        snapshot_time = EXCLUDED.snapshot_time,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
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
                        now,
                        now,
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
    def _portfolio_from_row(row: dict[str, Any]) -> PaperPortfolio:
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
            broker_backend=row.get("broker_backend") or "local_sim",
            fee_policy=row["fee_policy"] or {},
            risk_policy=row["risk_policy"] or {},
            execution_policy=row["execution_policy"] or {},
            auto_run_enabled=bool(row.get("auto_run_enabled", False)),
            auto_run_config=row.get("auto_run_config") or {},
            auto_run_config_sha256=row.get("auto_run_config_sha256"),
            auto_run_updated_at=row.get("auto_run_updated_at"),
            auto_run_updated_by=row.get("auto_run_updated_by"),
            status=PortfolioStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _broker_account_binding_from_row(row: dict[str, Any]) -> PaperBrokerAccountBinding:
        return PaperBrokerAccountBinding(
            binding_id=row["binding_id"],
            broker_backend=row["broker_backend"],
            broker_mode=row["broker_mode"],
            broker_account_id=row["broker_account_id"],
            portfolio_id=row["portfolio_id"],
            binding_status=BrokerAccountBindingStatus(row["binding_status"]),
            allocation_mode=row.get("allocation_mode") or "exclusive_account",
            initial_cash=float(row["initial_cash"]) if row.get("initial_cash") is not None else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            created_by=row.get("created_by"),
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
        # T5 side-channel for DW ETL capture fields. Indexed by fill_id so
        # the underlying Fill model (in trading_core, outside D1 boundary)
        # does not need to grow new fields. Keys: created_at, updated_at,
        # intended_price, fill_market_context.
        self.fill_capture: dict[str, dict[str, Any]] = {}
        self.events: dict[str, list[OrderEvent]] = {}
        self.cash_entries: dict[str, list[CashLedgerEntry]] = {}
        self.positions: dict[str, list[PositionLot]] = {}
        # T5 side-channel for positions watermarks. Indexed by run_id since
        # save_positions overwrites all rows for a run.
        self.position_capture: dict[str, dict[str, datetime]] = {}
        self.snapshots: dict[str, AccountSnapshot] = {}
        # T5 side-channel for daily_snapshots watermarks. Keyed by
        # (portfolio_id, trade_date) to preserve created_at on upsert.
        self.snapshot_capture: dict[tuple[str, date], dict[str, datetime]] = {}
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
        self.broker_account_bindings: dict[str, PaperBrokerAccountBinding] = {}

    @contextmanager
    def session_tick_lock(self, session_id: str) -> Iterator[None]:
        yield

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

    def list_running_summaries(
        self,
        *,
        limit: int = 100,
        snapshot_limit: int = 30,
        position_limit: int = 8,
    ) -> list[dict[str, Any]]:
        return self.list_running_summaries_page(
            page=1,
            page_size=limit,
            snapshot_limit=snapshot_limit,
            position_limit=position_limit,
        )["summaries"]

    def list_running_summaries_page(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        snapshot_limit: int = 30,
        position_limit: int = 8,
        statuses: list[str] | None = None,
        sort_by: str = "latest_run_time",
        sort_dir: str = "desc",
        search: str | None = None,
        search_fields: list[str] | None = None,
        min_initial_cash: float | None = None,
        max_initial_cash: float | None = None,
    ) -> dict[str, Any]:
        if page <= 0 or page_size <= 0:
            raise DataUnavailableError(
                "paper v2 running summary limits must be positive",
                context={"page": page, "page_size": page_size},
            )
        rows: list[dict[str, Any]] = []
        status_values = {str(item).strip().upper() for item in (statuses or list(RUNNING_SUMMARY_ACTIVE_STATUSES)) if str(item).strip()}
        active = {PortfolioStatus(status) for status in status_values}
        for portfolio in [item for item in self.list_portfolios(limit=10_000) if item.status in active]:
            runs = self.list_runs(portfolio.portfolio_id, limit=1)
            all_sessions = self.list_sessions(portfolio.portfolio_id, limit=10_000)
            sessions = all_sessions[:1]
            tickable_session_count = sum(
                1
                for session in all_sessions
                if session.status.value in RUNNING_SUMMARY_TICKABLE_SESSION_STATUSES
            )
            snapshots = self.list_daily_snapshots(portfolio.portfolio_id, limit=snapshot_limit)
            latest_trade_date = snapshots[0]["trade_date"] if snapshots else None
            positions = self.list_positions(portfolio.portfolio_id, limit=10_000)
            if positions:
                latest_position_date = positions[0]["trade_date"]
                latest_positions = [row for row in positions if row["trade_date"] == latest_position_date][:position_limit]
            else:
                latest_positions = []
            rows.append(
                {
                    "portfolio": portfolio,
                    "latest_run": runs[0] if runs else None,
                    "latest_session": sessions[0] if sessions else None,
                    "operability": _running_summary_operability(
                        portfolio_status=portfolio.status,
                        latest_session=sessions[0] if sessions else None,
                        tickable_session_count=tickable_session_count,
                    ),
                    "latest_snapshot": snapshots[0] if snapshots else None,
                    "recent_snapshots": snapshots,
                    "latest_positions": latest_positions,
                    "counts": {
                        "orders": len(self.list_orders(portfolio.portfolio_id, limit=10_000)),
                        "fills": len(self.list_fills(portfolio.portfolio_id, limit=10_000)),
                        "positions": len(latest_positions),
                        "errors": len(self.list_errors(portfolio.portfolio_id, limit=10_000)),
                    },
                    "latest_trade_date": latest_trade_date,
                }
            )
        if min_initial_cash is not None:
            rows = [row for row in rows if row["portfolio"].initial_cash >= min_initial_cash]
        if max_initial_cash is not None:
            rows = [row for row in rows if row["portfolio"].initial_cash <= max_initial_cash]
        normalized_search = str(search or "").strip().lower()
        requested_search_fields = [str(item).strip().lower() for item in (search_fields or []) if str(item).strip()]
        if not requested_search_fields or "all" in requested_search_fields:
            requested_search_fields = list(RUNNING_SUMMARY_SEARCH_COLUMNS)
        if normalized_search:
            def _field_text(row: dict[str, Any], field: str) -> str:
                portfolio = row["portfolio"]
                latest_run = row.get("latest_run") or {}
                values = {
                    "portfolio_name": portfolio.portfolio_name,
                    "portfolio_id": portfolio.portfolio_id,
                    "package_id": portfolio.package_id,
                    "manifest_sha256": portfolio.manifest_sha256,
                    "status": portfolio.status.value,
                    "data_source": portfolio.data_source.value,
                    "initial_cash": portfolio.initial_cash,
                    "latest_run_status": latest_run.get("status"),
                    "latest_run_trade_date": latest_run.get("trade_date"),
                    "latest_run_time": latest_run.get("started_at") or latest_run.get("completed_at"),
                }
                return str(values.get(field) or "").lower()

            rows = [
                row
                for row in rows
                if any(normalized_search in _field_text(row, field) for field in requested_search_fields)
            ]
        normalized_sort_by = str(sort_by or "latest_run_time").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()

        def _sort_value(row: dict[str, Any]) -> Any:
            portfolio = row["portfolio"]
            latest_run = row.get("latest_run") or {}
            if normalized_sort_by == "status":
                return portfolio.status.value
            if normalized_sort_by == "initial_cash":
                return portfolio.initial_cash
            if normalized_sort_by == "portfolio_name":
                return portfolio.portfolio_name
            if normalized_sort_by == "created_at":
                return portfolio.created_at
            if normalized_sort_by == "updated_at":
                return portfolio.updated_at
            return latest_run.get("started_at") or latest_run.get("completed_at") or portfolio.updated_at

        rows.sort(key=_sort_value, reverse=normalized_sort_dir != "asc")
        total = len(rows)
        offset = (page - 1) * page_size
        page_rows = rows[offset:offset + page_size]
        return {
            "summaries": page_rows,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size,
                "sort_by": normalized_sort_by,
                "sort_dir": normalized_sort_dir,
                "statuses": sorted(status_values),
                "search": normalized_search or None,
                "search_fields": requested_search_fields,
                "min_initial_cash": min_initial_cash,
                "max_initial_cash": max_initial_cash,
            },
        }

    def update_portfolio_status(self, portfolio_id: str, status: PortfolioStatus) -> PaperPortfolio:
        portfolio = self.get_portfolio(portfolio_id)
        updated = portfolio.model_copy(update={"status": status, "updated_at": datetime.now(UTC)})
        self.portfolios[portfolio_id] = updated
        return updated

    def update_portfolio_auto_run(
        self,
        portfolio_id: str,
        *,
        enabled: bool,
        config: dict[str, Any],
        config_sha256: str,
        updated_by: str | None = None,
    ) -> PaperPortfolio:
        portfolio = self.get_portfolio(portfolio_id)
        updated = portfolio.model_copy(
            update={
                "auto_run_enabled": enabled,
                "auto_run_config": config,
                "auto_run_config_sha256": config_sha256,
                "auto_run_updated_at": datetime.now(UTC),
                "auto_run_updated_by": updated_by,
                "updated_at": datetime.now(UTC),
            }
        )
        self.portfolios[portfolio_id] = updated
        return updated

    def list_auto_run_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        rows = [
            portfolio
            for portfolio in self.portfolios.values()
            if portfolio.auto_run_enabled and portfolio.status in {PortfolioStatus.READY, PortfolioStatus.RUNNING, PortfolioStatus.PAUSED}
        ]
        rows.sort(key=lambda item: item.auto_run_updated_at or item.updated_at, reverse=True)
        return rows[:limit]

    def create_broker_account_binding(self, binding: PaperBrokerAccountBinding) -> PaperBrokerAccountBinding:
        conflict = self.get_active_broker_account_binding(
            broker_backend=binding.broker_backend,
            broker_mode=binding.broker_mode,
            broker_account_id=binding.broker_account_id,
        )
        if conflict is not None and conflict.portfolio_id != binding.portfolio_id:
            raise InvalidStateTransitionError(
                "MiniQMT account already has an active Paper v2 auto-run binding",
                context={
                    "broker_backend": binding.broker_backend,
                    "broker_mode": binding.broker_mode,
                    "broker_account_id": binding.broker_account_id,
                    "existing_portfolio_id": conflict.portfolio_id,
                    "portfolio_id": binding.portfolio_id,
                },
            )
        self.broker_account_bindings[binding.binding_id] = binding
        return binding

    def get_active_broker_account_binding(
        self,
        *,
        broker_backend: str,
        broker_mode: str,
        broker_account_id: str,
    ) -> PaperBrokerAccountBinding | None:
        for binding in self.broker_account_bindings.values():
            if (
                binding.broker_backend == broker_backend
                and binding.broker_mode == broker_mode
                and binding.broker_account_id == broker_account_id
                and binding.binding_status == BrokerAccountBindingStatus.ACTIVE
            ):
                return binding
        return None

    def list_active_broker_account_bindings(self, portfolio_id: str | None = None) -> list[PaperBrokerAccountBinding]:
        rows = [
            binding
            for binding in self.broker_account_bindings.values()
            if binding.binding_status == BrokerAccountBindingStatus.ACTIVE
            and (portfolio_id is None or binding.portfolio_id == portfolio_id)
        ]
        rows.sort(key=lambda item: item.updated_at, reverse=True)
        return rows

    def update_broker_account_binding_status(
        self,
        binding_id: str,
        status: BrokerAccountBindingStatus,
    ) -> PaperBrokerAccountBinding:
        try:
            binding = self.broker_account_bindings[binding_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "paper v2 broker account binding does not exist",
                context={"binding_id": binding_id},
            ) from exc
        updated = binding.model_copy(update={"binding_status": status, "updated_at": datetime.now(UTC)})
        self.broker_account_bindings[binding_id] = updated
        return updated

    def delete_portfolio(self, portfolio_id: str) -> dict[str, int]:
        self.get_portfolio(portfolio_id)
        run_ids = [run_id for run_id, run in self.runs.items() if run.portfolio_id == portfolio_id]
        session_ids = [session_id for session_id, session in self.sessions.items() if session.portfolio_id == portfolio_id]
        profile_ids = [profile_id for profile_id, profile in self.runtime_profiles.items() if profile.portfolio_id == portfolio_id]
        counts = self.reset_portfolio_runs(portfolio_id=portfolio_id)
        counts.update(
            {
                "selection_paper_portfolio_link": 0,
                "broker_account_binding": len([item for item in self.broker_account_bindings.values() if item.portfolio_id == portfolio_id and item.binding_status == BrokerAccountBindingStatus.ACTIVE]),
                "session_events": len([item for item in self.session_events if item.get("session_id") in session_ids]),
                "session_day": len([key for key, day in self.session_days.items() if day.portfolio_id == portfolio_id]),
                "trade_session": len(session_ids),
                "execution_policy_activation": len([item for item in self.execution_policy_activations.values() if item.portfolio_id == portfolio_id]),
                "runtime_config_activation": len([item for item in self.runtime_config_activations.values() if item.portfolio_id == portfolio_id]),
                "runtime_profile_version": len([item for item in self.runtime_profile_versions.values() if item.profile_id in profile_ids]),
                "runtime_profile": len(profile_ids),
                "config_change_audit": len([item for item in self.config_change_audits if item.portfolio_id == portfolio_id]),
                "reset_audit": len([item for item in self.reset_audits if item.get("portfolio_id") == portfolio_id]),
                "portfolio": 1,
            }
        )
        self.session_events = [item for item in self.session_events if item.get("session_id") not in session_ids]
        for key, day in list(self.session_days.items()):
            if day.portfolio_id == portfolio_id:
                self.session_days.pop(key, None)
        for session_id in session_ids:
            self.sessions.pop(session_id, None)
        for key, activation in list(self.execution_policy_activations.items()):
            if activation.portfolio_id == portfolio_id:
                self.execution_policy_activations.pop(key, None)
        for key, activation in list(self.runtime_config_activations.items()):
            if activation.portfolio_id == portfolio_id:
                self.runtime_config_activations.pop(key, None)
        for key, version in list(self.runtime_profile_versions.items()):
            if version.profile_id in profile_ids:
                self.runtime_profile_versions.pop(key, None)
        for profile_id in profile_ids:
            self.runtime_profiles.pop(profile_id, None)
        self.config_change_audits = [item for item in self.config_change_audits if item.portfolio_id != portfolio_id]
        self.reset_audits = [item for item in self.reset_audits if item.get("portfolio_id") != portfolio_id]
        self.errors = [item for item in self.errors if item.get("portfolio_id") != portfolio_id]
        for key, binding in list(self.broker_account_bindings.items()):
            if binding.portfolio_id == portfolio_id and binding.binding_status == BrokerAccountBindingStatus.ACTIVE:
                self.broker_account_bindings[key] = binding.model_copy(
                    update={"binding_status": BrokerAccountBindingStatus.RETIRED, "updated_at": datetime.now(UTC)}
                )
        for key, snapshot in list(self.intraday_snapshots.items()):
            if snapshot.portfolio_id == portfolio_id:
                self.intraday_snapshots.pop(key, None)
        for key, state in list(self.order_execution_states.items()):
            if state.run_id in run_ids or state.session_id in session_ids:
                self.order_execution_states.pop(key, None)
        self.portfolios.pop(portfolio_id, None)
        return counts

    def create_run(self, run: PaperRun) -> PaperRun:
        self.runs[run.run_id] = run
        return run

    def get_run_by_portfolio_date(self, portfolio_id: str, trade_date: date) -> PaperRun | None:
        for run in self.runs.values():
            if run.portfolio_id == portfolio_id and run.trade_date == trade_date:
                return run
        return None

    def get_run(self, run_id: str) -> PaperRun:
        try:
            return self.runs[run_id]
        except KeyError as exc:
            raise DataUnavailableError("paper v2 run does not exist", context={"run_id": run_id}) from exc

    def update_run_status(self, run: PaperRun, status: RunStatus, error: dict[str, Any] | None = None) -> PaperRun:
        updated = run.model_copy(
            update={"status": status, "error": error, "completed_at": datetime.now(UTC) if status in {RunStatus.SUCCEEDED, RunStatus.FAILED} else run.completed_at}
        )
        self.runs[run.run_id] = updated
        return updated

    def update_run_runtime_config(self, run: PaperRun, runtime_config: dict[str, Any]) -> PaperRun:
        updated = run.model_copy(update={"runtime_config": runtime_config})
        self.runs[run.run_id] = updated
        return updated

    def update_run_model_params_origin(
        self, run: PaperRun, model_params_origin: str
    ) -> PaperRun:
        if model_params_origin not in ("node", "cache", "unavailable"):
            raise InvalidStateTransitionError(
                "invalid paper v2 run.model_params_origin value",
                context={
                    "run_id": run.run_id,
                    "model_params_origin": model_params_origin,
                },
            )
        updated = run.model_copy(update={"model_params_origin": model_params_origin})
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

    def list_intraday_snapshots_for_portfolio(
        self,
        portfolio_id: str,
        *,
        trade_date: date | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        rows = [
            snapshot.model_dump(mode="json")
            for snapshot in self.intraday_snapshots.values()
            if snapshot.portfolio_id == portfolio_id and (trade_date is None or snapshot.trade_date == trade_date)
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

    def save_fill(
        self,
        run_id: str,
        fill: Fill,
        *,
        intended_price: float | None = None,
        fill_market_context: dict[str, Any] | None = None,
    ) -> None:
        existing = self.fills.setdefault(run_id, [])
        if not any(item.fill_id == fill.fill_id for item in existing):
            existing.append(fill)
            # T5 capture fields: only set on first INSERT (mirrors
            # ON CONFLICT(fill_id) DO NOTHING semantics on the PG path).
            now = datetime.now(UTC)
            self.fill_capture[fill.fill_id] = {
                "created_at": now,
                "updated_at": now,
                "intended_price": intended_price,
                "fill_market_context": (
                    dict(fill_market_context) if fill_market_context is not None else None
                ),
            }

    def list_fills_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return [fill.model_dump(mode="json") for fill in self.fills.get(run_id, [])]

    def save_order_event(self, run_id: str, event: OrderEvent) -> None:
        existing = self.events.setdefault(run_id, [])
        if not any(item.event_id == event.event_id for item in existing):
            existing.append(event)

    def list_order_events(self, portfolio_id: str, *, run_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for stored_run_id, events in self.events.items():
            run = self.runs.get(stored_run_id)
            if not run or run.portfolio_id != portfolio_id:
                continue
            if run_id is not None and stored_run_id != run_id:
                continue
            order_by_id = {order.order_id: order for order in self.orders.get(stored_run_id, [])}
            for event in events:
                item = event.model_dump(mode="json")
                order = order_by_id.get(event.order_id)
                if order is not None:
                    item.update(
                        {
                            "symbol": order.symbol,
                            "side": order.side.value,
                            "order_quantity": order.quantity,
                            "order_filled_quantity": order.filled_quantity,
                            "order_avg_fill_price": order.avg_fill_price,
                            "order_status": order.status.value,
                        }
                    )
                rows.append(item)
        rows.sort(key=lambda item: str(item.get("event_time") or ""), reverse=True)
        return rows[:limit]

    def save_cash_entry(self, run_id: str, entry: CashLedgerEntry) -> None:
        self.cash_entries.setdefault(run_id, []).append(entry)

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        self.positions[run_id] = positions
        # T5 watermark: save_positions deletes-and-inserts at the PG level
        # (DELETE ... WHERE run_id = %s, then INSERT each row), so both
        # created_at and updated_at advance to now() on every call.
        now = datetime.now(UTC)
        self.position_capture[run_id] = {"created_at": now, "updated_at": now}

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot: AccountSnapshot, metadata: dict[str, Any] | None = None) -> None:
        self.snapshots[run_id] = snapshot
        # T5 watermark: PG path is upsert keyed by (portfolio_id, trade_date),
        # preserving created_at on conflict and bumping updated_at. Mirror
        # that here so tests can verify updated_at moves but created_at does not.
        now = datetime.now(UTC)
        cap_key = (snapshot.portfolio_id, trade_date)
        existing = self.snapshot_capture.get(cap_key)
        if existing is None:
            self.snapshot_capture[cap_key] = {"created_at": now, "updated_at": now}
        else:
            existing["updated_at"] = now

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
