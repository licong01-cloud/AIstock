"""Read-only LocalSIM run, ledger, and performance projections by ledger scope."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable

import psycopg2.extras

from backend.db.pg_pool import get_conn

from .successor_repository import LocalSimSuccessorRepositoryProtocol


EconomicQueryConnFactory = Callable[[], AbstractContextManager[Any]]


class LocalSimEconomicQueryService:
    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        conn_factory: EconomicQueryConnFactory | None = None,
    ) -> None:
        self.repository = repository
        self._conn_factory = conn_factory or get_conn

    def list_runs(self, account_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        scope = self.repository.resolve_ledger_scope_for_account(account_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM (
                        SELECT lifecycle.run_id, lifecycle.trade_date, lifecycle.status,
                               lifecycle.broker_backend, lifecycle.package_id,
                               lifecycle.release_id, lifecycle.binding_id,
                               lifecycle.run_payload_json AS evidence,
                               lifecycle.created_at, lifecycle.updated_at,
                               'simulation_daily_run'::text AS run_kind
                        FROM paper_v2.simulation_daily_run AS lifecycle
                        WHERE lifecycle.broker_backend = 'local_sim'
                          AND lifecycle.strategy_id IN (%s, %s)
                        UNION ALL
                        SELECT legacy.run_id, legacy.trade_date, legacy.status,
                               'local_sim'::text AS broker_backend,
                               account.package_id, binding.release_id, binding.binding_id,
                               legacy.runtime_config AS evidence,
                               legacy.started_at AS created_at,
                               COALESCE(legacy.completed_at, legacy.started_at) AS updated_at,
                               'economic_run'::text AS run_kind
                        FROM paper_v2.run AS legacy
                        JOIN paper_v2.simulation_account_v1 AS account ON account.account_id = %s
                        LEFT JOIN LATERAL (
                            SELECT candidate.release_id, candidate.binding_id
                            FROM paper_v2.simulation_release_binding AS candidate
                            WHERE candidate.strategy_id IN (%s, %s)
                              AND (candidate.effective_from IS NULL OR candidate.effective_from <= legacy.trade_date)
                              AND (candidate.effective_to IS NULL OR candidate.effective_to >= legacy.trade_date)
                            ORDER BY candidate.created_at DESC, candidate.binding_id DESC
                            LIMIT 1
                        ) AS binding ON TRUE
                        WHERE legacy.portfolio_id = %s
                    ) AS projected
                    ORDER BY trade_date DESC, created_at DESC, run_id DESC
                    LIMIT %s
                    """,
                    (
                        account_id,
                        scope.ledger_scope_id,
                        account_id,
                        account_id,
                        scope.ledger_scope_id,
                        scope.ledger_scope_id,
                        limit,
                    ),
                )
                return [dict(row) for row in cur.fetchall()]

    def ledger(self, account_id: str, *, limit: int = 200) -> dict[str, Any]:
        scope = self.repository.resolve_ledger_scope_for_account(account_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                run_ids = self._run_ids(cur, scope.ledger_scope_id, limit=limit)
                return {
                    "schema_version": "localsim_ledger_projection_v1",
                    "account_id": account_id,
                    "ledger_scope": scope.model_dump(mode="json"),
                    "orders": self._payload_rows(
                        cur,
                        "SELECT to_jsonb(row_value) AS payload FROM ("
                        "SELECT * FROM paper_v2.orders WHERE portfolio_id = %s "
                        "ORDER BY created_at DESC, order_id DESC LIMIT %s) AS row_value",
                        (scope.ledger_scope_id, limit),
                    ),
                    "fills": self._payload_rows(
                        cur,
                        "SELECT to_jsonb(row_value) AS payload FROM ("
                        "SELECT * FROM paper_v2.fills WHERE run_id = ANY(%s) "
                        "ORDER BY created_at DESC, fill_id DESC LIMIT %s) AS row_value",
                        (run_ids, limit),
                    ) if run_ids else [],
                    "cash_ledger": self._payload_rows(
                        cur,
                        "SELECT to_jsonb(row_value) AS payload FROM ("
                        "SELECT * FROM paper_v2.cash_ledger WHERE portfolio_id = %s "
                        "ORDER BY created_at DESC, cash_id DESC LIMIT %s) AS row_value",
                        (scope.ledger_scope_id, limit),
                    ),
                    "positions": self._payload_rows(
                        cur,
                        "SELECT to_jsonb(row_value) AS payload FROM ("
                        "SELECT * FROM paper_v2.positions WHERE portfolio_id = %s "
                        "ORDER BY trade_date DESC, symbol ASC LIMIT %s) AS row_value",
                        (scope.ledger_scope_id, limit),
                    ),
                    "daily_snapshots": self._payload_rows(
                        cur,
                        "SELECT to_jsonb(row_value) AS payload FROM ("
                        "SELECT * FROM paper_v2.daily_snapshots WHERE portfolio_id = %s "
                        "ORDER BY trade_date DESC, snapshot_id DESC LIMIT %s) AS row_value",
                        (scope.ledger_scope_id, limit),
                    ),
                }

    def performance(self, account_id: str, *, limit: int = 500) -> dict[str, Any]:
        scope = self.repository.resolve_ledger_scope_for_account(account_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                snapshots = self._payload_rows(
                    cur,
                    "SELECT to_jsonb(row_value) AS payload FROM ("
                    "SELECT * FROM paper_v2.daily_snapshots WHERE portfolio_id = %s "
                    "ORDER BY trade_date ASC, snapshot_id ASC LIMIT %s) AS row_value",
                    (scope.ledger_scope_id, limit),
                )
        return {
            "schema_version": "localsim_performance_projection_v1",
            "account_id": account_id,
            "ledger_scope_id": scope.ledger_scope_id,
            "snapshots": snapshots,
        }

    @staticmethod
    def _run_ids(cur: Any, ledger_scope_id: str, *, limit: int) -> list[str]:
        cur.execute(
            "SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s "
            "ORDER BY trade_date DESC, run_id DESC LIMIT %s",
            (ledger_scope_id, limit),
        )
        return [str(row["run_id"]) for row in cur.fetchall()]

    @staticmethod
    def _payload_rows(cur: Any, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        cur.execute(sql, params)
        return [dict(row["payload"] or {}) for row in cur.fetchall()]
