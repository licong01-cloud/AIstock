"""Fail-closed read-only authority for LocalSIM product-cutover mutations."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Any, Callable

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError


ReadinessConnFactory = Callable[[], AbstractContextManager[Any]]

_REQUIRED_RELATIONS = (
    "paper_v2.simulation_account_v1",
    "paper_v2.legacy_localsim_account_lineage_v1",
    "paper_v2.localsim_replay_job_v1",
    "paper_v2.localsim_runtime_profile_v1",
    "paper_v2.localsim_runtime_profile_version_v1",
    "paper_v2.simulation_ledger_scope_v1",
    "strategy_pkg.strategy_runtime_release",
    "paper_v2.simulation_release_binding",
    "paper_v2.simulation_daily_run",
    "paper_v2.run",
    "paper_v2.intraday_snapshots",
)
_LEGACY_TERMINAL_SESSION_STATUSES = ("STOPPED", "SUCCEEDED", "FAILED")


class LocalSimCutoverReadinessV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "localsim_cutover_readiness_v1"
    ready: bool
    checked_at: datetime
    blockers: tuple[str, ...]
    relation_presence: dict[str, bool]
    runtime_fk_count: int = Field(ge=0)
    orphan_ledger_scope_count: int = Field(ge=0)
    invalid_ledger_scope_count: int = Field(ge=0)
    retained_legacy_account_ids: tuple[str, ...]
    missing_lineage_account_ids: tuple[str, ...]
    legacy_active_session_count: int = Field(ge=0)
    legacy_auto_run_count: int = Field(ge=0)
    legacy_sentinel_count: int = Field(ge=0)
    in_flight_economic_run_count: int = Field(ge=0)


class LocalSimCutoverReadiness:
    """Read schema and durable owner facts without importing the retired product."""

    def __init__(self, *, conn_factory: ReadinessConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def read(self) -> LocalSimCutoverReadinessV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                cur.execute(
                    "SELECT requested.relation_name, to_regclass(requested.relation_name) IS NOT NULL AS present "
                    "FROM unnest(%s::text[]) AS requested(relation_name)",
                    (list(_REQUIRED_RELATIONS),),
                )
                relation_presence = {str(row["relation_name"]): bool(row["present"]) for row in cur.fetchall()}
                missing = sorted(name for name, present in relation_presence.items() if not present)
                if missing:
                    return self._result(
                        blockers=tuple(f"required_relation_missing:{name}" for name in missing),
                        relation_presence=relation_presence,
                    )

                cur.execute(
                    """
                    SELECT count(*) AS count
                    FROM pg_constraint
                    WHERE contype = 'f'
                      AND conrelid IN ('paper_v2.run'::regclass, 'paper_v2.intraday_snapshots'::regclass)
                      AND confrelid = 'paper_v2.simulation_ledger_scope_v1'::regclass
                    """
                )
                runtime_fk_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT count(*) AS count
                    FROM (
                        SELECT run.portfolio_id AS ledger_scope_id FROM paper_v2.run AS run
                        UNION
                        SELECT snapshot.portfolio_id FROM paper_v2.intraday_snapshots AS snapshot
                    ) AS referenced
                    LEFT JOIN paper_v2.simulation_ledger_scope_v1 AS scope
                      ON scope.ledger_scope_id = referenced.ledger_scope_id
                    WHERE scope.ledger_scope_id IS NULL
                    """
                )
                orphan_scope_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT count(*) AS count
                    FROM paper_v2.simulation_ledger_scope_v1 AS scope
                    WHERE length(scope.ledger_scope_hash) <> 64
                       OR scope.ledger_scope_hash <> lower(scope.ledger_scope_hash)
                       OR (scope.scope_kind = 'LEGACY_PORTFOLIO' AND scope.native_account_id IS NOT NULL)
                       OR (scope.scope_kind = 'SUCCESSOR_NATIVE'
                           AND scope.native_account_id IS DISTINCT FROM scope.ledger_scope_id)
                    """
                )
                invalid_scope_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT DISTINCT portfolio.portfolio_id
                    FROM paper_v2.portfolio AS portfolio
                    JOIN paper_v2.simulation_release_binding AS binding
                      ON binding.strategy_id = portfolio.portfolio_id
                     AND binding.broker_backend = 'local_sim'
                     AND binding.approval_state <> 'RETIRED'
                    WHERE portfolio.broker_backend = 'local_sim'
                      AND portfolio.status IN ('READY', 'RUNNING', 'PAUSED')
                      AND portfolio.portfolio_id NOT LIKE 'paper_v2_coldstart_sanity_%'
                    ORDER BY portfolio.portfolio_id
                    """
                )
                retained_ids = tuple(str(row["portfolio_id"]) for row in cur.fetchall())
                missing_lineages: tuple[str, ...] = ()
                if retained_ids:
                    cur.execute(
                        """
                        SELECT candidate.legacy_account_id
                        FROM unnest(%s::text[]) AS candidate(legacy_account_id)
                        LEFT JOIN paper_v2.legacy_localsim_account_lineage_v1 AS lineage
                          ON lineage.legacy_account_id = candidate.legacy_account_id
                        WHERE lineage.lineage_id IS NULL
                        ORDER BY candidate.legacy_account_id
                        """,
                        (list(retained_ids),),
                    )
                    missing_lineages = tuple(str(row["legacy_account_id"]) for row in cur.fetchall())
                cur.execute(
                    "SELECT count(*) AS count FROM paper_v2.trade_session WHERE status <> ALL(%s::text[])",
                    (list(_LEGACY_TERMINAL_SESSION_STATUSES),),
                )
                active_session_count = int(cur.fetchone()["count"])
                cur.execute("SELECT count(*) AS count FROM paper_v2.portfolio WHERE auto_run_enabled IS TRUE")
                auto_run_count = int(cur.fetchone()["count"])
                cur.execute(
                    "SELECT count(*) AS count FROM paper_v2.portfolio "
                    "WHERE portfolio_id LIKE 'paper_v2_coldstart_sanity_%' "
                    "AND status NOT IN ('FAILED', 'COMPLETED', 'RETIRED')"
                )
                sentinel_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT count(*) AS count
                    FROM paper_v2.simulation_daily_run AS run
                    WHERE run.broker_backend = 'local_sim'
                      AND run.status IN (
                          'CREATED', 'PRECHECKING', 'SIGNAL_GENERATING', 'TARGET_GENERATING',
                          'PLANNING_EXECUTION', 'SUBMITTING', 'INTRADAY_RUNNING',
                          'TAIL_HANDLING', 'RECONCILING'
                      )
                    """
                )
                in_flight_count = int(cur.fetchone()["count"])

        blockers: list[str] = []
        if runtime_fk_count != 2:
            blockers.append(f"ledger_scope_runtime_fk_count:{runtime_fk_count}")
        if orphan_scope_count:
            blockers.append(f"orphan_ledger_scope_count:{orphan_scope_count}")
        if invalid_scope_count:
            blockers.append(f"invalid_ledger_scope_count:{invalid_scope_count}")
        if missing_lineages:
            blockers.append(f"retained_lineage_missing:{len(missing_lineages)}")
        if active_session_count:
            blockers.append(f"legacy_active_session_count:{active_session_count}")
        if auto_run_count:
            blockers.append(f"legacy_auto_run_count:{auto_run_count}")
        if sentinel_count:
            blockers.append(f"legacy_sentinel_count:{sentinel_count}")
        if in_flight_count:
            blockers.append(f"in_flight_economic_run_count:{in_flight_count}")
        return self._result(
            blockers=tuple(blockers),
            relation_presence=relation_presence,
            runtime_fk_count=runtime_fk_count,
            orphan_ledger_scope_count=orphan_scope_count,
            invalid_ledger_scope_count=invalid_scope_count,
            retained_legacy_account_ids=retained_ids,
            missing_lineage_account_ids=missing_lineages,
            legacy_active_session_count=active_session_count,
            legacy_auto_run_count=auto_run_count,
            legacy_sentinel_count=sentinel_count,
            in_flight_economic_run_count=in_flight_count,
        )

    def require_ready(self) -> None:
        readiness = self.read()
        if not readiness.ready:
            raise DataUnavailableError(
                "LocalSIM product cutover is not ready",
                context={
                    "reason_code": "LOCALSIM_CUTOVER_NOT_READY",
                    "blockers": list(readiness.blockers),
                    "readiness": readiness.model_dump(mode="json"),
                },
            )

    @staticmethod
    def _result(
        *,
        blockers: tuple[str, ...],
        relation_presence: dict[str, bool],
        runtime_fk_count: int = 0,
        orphan_ledger_scope_count: int = 0,
        invalid_ledger_scope_count: int = 0,
        retained_legacy_account_ids: tuple[str, ...] = (),
        missing_lineage_account_ids: tuple[str, ...] = (),
        legacy_active_session_count: int = 0,
        legacy_auto_run_count: int = 0,
        legacy_sentinel_count: int = 0,
        in_flight_economic_run_count: int = 0,
    ) -> LocalSimCutoverReadinessV1:
        return LocalSimCutoverReadinessV1(
            ready=not blockers,
            checked_at=datetime.now(UTC),
            blockers=blockers,
            relation_presence=relation_presence,
            runtime_fk_count=runtime_fk_count,
            orphan_ledger_scope_count=orphan_ledger_scope_count,
            invalid_ledger_scope_count=invalid_ledger_scope_count,
            retained_legacy_account_ids=retained_legacy_account_ids[:200],
            missing_lineage_account_ids=missing_lineage_account_ids[:200],
            legacy_active_session_count=legacy_active_session_count,
            legacy_auto_run_count=legacy_auto_run_count,
            legacy_sentinel_count=legacy_sentinel_count,
            in_flight_economic_run_count=in_flight_economic_run_count,
        )
