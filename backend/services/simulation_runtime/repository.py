"""Runtime release and simulation binding repositories."""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Iterable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.tca_sidecar import (
    TCA_OBSERVATION_KEY,
    CaptureMergeOutcome,
    merge_parent_first_write,
    new_run_tca_sidecar,
    preserve_tca_sidecar,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    ExecutionPlanIntent,
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    TradingRuleDecision,
)

ConnFactory = Callable[[], Iterator[Any]]


class SimulationRuntimeRepository:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def save_strategy_runtime_release(self, release: StrategyRuntimeRelease) -> StrategyRuntimeRelease:
        existing_by_hash = self.get_strategy_runtime_release_by_hash(release.release_hash or "")
        if existing_by_hash is not None:
            return existing_by_hash
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.strategy_runtime_release (
                            release_id, package_id, manifest_sha256, base_release_id,
                            runtime_profile_id, runtime_profile_version_id, runtime_profile_sha256,
                            daily_strategy_profile_version_id, execution_policy_version_id,
                            execution_policy_sha256, tail_policy_version_id, tail_policy_sha256,
                            release_config_json, release_hash, validation_state, validation_evidence,
                            effective_from, effective_to, created_by, created_reason, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            release.release_id,
                            release.package_id,
                            release.manifest_sha256,
                            release.base_release_id,
                            release.runtime_profile_id,
                            release.runtime_profile_version_id,
                            release.runtime_profile_sha256,
                            release.daily_strategy_profile_version_id,
                            release.execution_policy_version_id,
                            release.execution_policy_sha256,
                            release.tail_policy_version_id,
                            release.tail_policy_sha256,
                            psycopg2.extras.Json(release.release_config_json),
                            release.release_hash,
                            release.validation_state.value,
                            psycopg2.extras.Json(release.validation_evidence),
                            release.effective_from,
                            release.effective_to,
                            release.created_by,
                            release.created_reason,
                            release.created_at,
                            release.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "strategy runtime release conflicts with an existing immutable release",
                        context={"release_id": release.release_id, "release_hash": release.release_hash},
                    ) from exc
        return release

    def get_strategy_runtime_release(self, release_id: str) -> StrategyRuntimeRelease:
        rows = self._fetch_rows(
            "SELECT * FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
            (release_id,),
        )
        if not rows:
            raise DataUnavailableError("strategy runtime release does not exist", context={"release_id": release_id})
        return self._release_from_row(rows[0])

    def get_strategy_runtime_release_by_hash(self, release_hash: str) -> StrategyRuntimeRelease | None:
        if not release_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM strategy_pkg.strategy_runtime_release WHERE release_hash = %s",
            (release_hash,),
        )
        return self._release_from_row(rows[0]) if rows else None

    def save_simulation_release_binding(self, binding: SimulationReleaseBinding) -> SimulationReleaseBinding:
        existing_by_hash = self.get_simulation_release_binding_by_hash(binding.binding_hash or "")
        if existing_by_hash is not None:
            return existing_by_hash
        self.get_strategy_runtime_release(binding.release_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.simulation_release_binding (
                            binding_id, strategy_id, release_id, release_hash, package_id, manifest_sha256,
                            broker_backend, broker_account_id, account_group_id, strategy_slot_id,
                            capital_allocation, strategy_name,
                            order_remark_prefix, effective_from, effective_to, approval_state,
                            binding_config_json, binding_hash, created_by, created_reason, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        """,
                        (
                            binding.binding_id,
                            binding.strategy_id,
                            binding.release_id,
                            binding.release_hash,
                            binding.package_id,
                            binding.manifest_sha256,
                            binding.broker_backend.value,
                            binding.broker_account_id,
                            binding.account_group_id,
                            binding.strategy_slot_id,
                            binding.capital_allocation,
                            binding.strategy_name,
                            binding.order_remark_prefix,
                            binding.effective_from,
                            binding.effective_to,
                            binding.approval_state.value,
                            psycopg2.extras.Json(binding.binding_config_json),
                            binding.binding_hash,
                            binding.created_by,
                            binding.created_reason,
                            binding.created_at,
                            binding.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "simulation release binding conflicts with an existing immutable binding",
                        context={"binding_id": binding.binding_id, "binding_hash": binding.binding_hash},
                    ) from exc
        return binding

    def get_simulation_release_binding(self, binding_id: str) -> SimulationReleaseBinding:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
            (binding_id,),
        )
        if not rows:
            raise DataUnavailableError("simulation release binding does not exist", context={"binding_id": binding_id})
        return self._binding_from_row(rows[0])

    def get_simulation_release_binding_by_hash(self, binding_hash: str) -> SimulationReleaseBinding | None:
        if not binding_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_release_binding WHERE binding_hash = %s",
            (binding_hash,),
        )
        return self._binding_from_row(rows[0]) if rows else None

    def list_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        release_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        active_on: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if release_id is not None:
            clauses.append("release_id = %s")
            params.append(release_id)
        if broker_backend is not None:
            backend = (
                broker_backend.value
                if isinstance(broker_backend, SimulationBrokerBackend)
                else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        states = [
            state.value if isinstance(state, SimulationBindingApprovalState) else str(state)
            for state in (approval_states or [])
        ]
        if states:
            placeholders = ", ".join(["%s"] * len(states))
            clauses.append(f"approval_state IN ({placeholders})")
            params.extend(states)
        if active_on is not None:
            clauses.append("(effective_from IS NULL OR effective_from <= %s)")
            params.append(active_on)
            clauses.append("(effective_to IS NULL OR effective_to >= %s)")
            params.append(active_on)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.simulation_release_binding
            {where}
            ORDER BY created_at DESC, binding_id
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._binding_from_row(row) for row in rows]

    def list_latest_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        effective_from_on_or_before: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if broker_backend is not None:
            backend = (
                broker_backend.value
                if isinstance(broker_backend, SimulationBrokerBackend)
                else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        states = [
            state.value if isinstance(state, SimulationBindingApprovalState) else str(state)
            for state in (approval_states or [])
        ]
        if states:
            placeholders = ", ".join(["%s"] * len(states))
            clauses.append(f"approval_state IN ({placeholders})")
            params.extend(states)
        if effective_from_on_or_before is not None:
            clauses.append("(effective_from IS NULL OR effective_from <= %s)")
            params.append(effective_from_on_or_before)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (strategy_id, broker_backend) *
                FROM paper_v2.simulation_release_binding
                {where}
                ORDER BY strategy_id, broker_backend, effective_from DESC NULLS LAST, created_at DESC, binding_id DESC
            ) latest
            ORDER BY created_at DESC, binding_id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._binding_from_row(row) for row in rows]

    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        existing_by_hash = self.get_daily_selection_evidence_by_hash(evidence.artifact_hash)
        if existing_by_hash is not None:
            return existing_by_hash
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO selection.daily_selection_evidence (
                            evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
                            release_id, release_hash, runtime_profile_version_id, runtime_profile_hash,
                            source_type, data_source, candidate_count, excluded_count, artifact_hash,
                            evidence_payload_json, created_at, created_by
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            evidence.evidence_id,
                            evidence.target_trade_date,
                            evidence.cutoff_date,
                            evidence.package_id,
                            evidence.manifest_sha256,
                            evidence.release_id,
                            evidence.release_hash,
                            evidence.runtime_profile_version_id,
                            evidence.runtime_profile_hash,
                            evidence.source_type,
                            evidence.data_source,
                            evidence.candidate_count,
                            evidence.excluded_count,
                            evidence.artifact_hash,
                            psycopg2.extras.Json(evidence.evidence_payload_json),
                            evidence.created_at,
                            evidence.created_by,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "daily selection evidence conflicts with an existing immutable evidence row",
                        context={"evidence_id": evidence.evidence_id, "artifact_hash": evidence.artifact_hash},
                    ) from exc
        return evidence

    def get_daily_selection_evidence(self, evidence_id: str) -> DailySelectionEvidence:
        rows = self._fetch_rows(
            "SELECT * FROM selection.daily_selection_evidence WHERE evidence_id = %s",
            (evidence_id,),
        )
        if not rows:
            raise DataUnavailableError("daily selection evidence does not exist", context={"evidence_id": evidence_id})
        return self._evidence_from_row(rows[0])

    def get_daily_selection_evidence_by_hash(self, artifact_hash: str) -> DailySelectionEvidence | None:
        if not artifact_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM selection.daily_selection_evidence WHERE artifact_hash = %s",
            (artifact_hash,),
        )
        return self._evidence_from_row(rows[0]) if rows else None

    def save_execution_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        existing_by_hash = self.get_execution_plan_by_hash(plan.plan_hash)
        if existing_by_hash is not None:
            return existing_by_hash
        self.get_strategy_runtime_release(plan.release_id)
        self.get_simulation_release_binding(plan.binding_id)
        self.get_daily_selection_evidence(plan.selection_evidence_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.execution_plan (
                            plan_id, strategy_id, portfolio_id, package_id, release_id, release_hash,
                            binding_id, binding_hash, selection_evidence_id, selection_evidence_hash,
                            target_trade_date, execution_policy_version_id, execution_policy_sha256,
                            tail_policy_version_id, tail_policy_sha256, intent_count,
                            trading_rule_decision_count, plan_payload_json, plan_hash, created_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            plan.plan_id,
                            plan.strategy_id,
                            plan.portfolio_id,
                            plan.package_id,
                            plan.release_id,
                            plan.release_hash,
                            plan.binding_id,
                            plan.binding_hash,
                            plan.selection_evidence_id,
                            plan.selection_evidence_hash,
                            plan.target_trade_date,
                            plan.execution_policy_version_id,
                            plan.execution_policy_sha256,
                            plan.tail_policy_version_id,
                            plan.tail_policy_sha256,
                            len(plan.intents),
                            len(plan.trading_rule_decisions),
                            psycopg2.extras.Json(plan.plan_payload_json),
                            plan.plan_hash,
                            plan.created_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "execution plan conflicts with an existing immutable plan",
                        context={"plan_id": plan.plan_id, "plan_hash": plan.plan_hash},
                    ) from exc
        return plan

    def get_execution_plan(self, plan_id: str) -> ExecutionPlan:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.execution_plan WHERE plan_id = %s",
            (plan_id,),
        )
        if not rows:
            raise DataUnavailableError("execution plan does not exist", context={"plan_id": plan_id})
        return self._execution_plan_from_row(rows[0])

    def get_execution_plan_by_hash(self, plan_hash: str) -> ExecutionPlan | None:
        if not plan_hash:
            return None
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.execution_plan WHERE plan_hash = %s",
            (plan_hash,),
        )
        return self._execution_plan_from_row(rows[0]) if rows else None

    def save_simulation_daily_run(self, run: SimulationDailyRun) -> SimulationDailyRun:
        existing = self.get_simulation_daily_run_by_key(
            strategy_id=run.strategy_id,
            binding_id=run.binding_id,
            trade_date=run.trade_date,
        )
        if existing is not None:
            return existing
        self.get_strategy_runtime_release(run.release_id)
        binding = self.get_simulation_release_binding(run.binding_id)
        run = self._daily_run_with_binding_slots(run, binding)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.simulation_daily_run (
                            run_id, trade_date, strategy_id, broker_backend, package_id, manifest_sha256,
                            release_id, release_hash, binding_id, binding_hash,
                            account_group_id, strategy_slot_id,
                            selection_evidence_id, selection_artifact_hash, execution_plan_id,
                            execution_plan_hash, status, run_payload_json, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            run.run_id,
                            run.trade_date,
                            run.strategy_id,
                            run.broker_backend.value,
                            run.package_id,
                            run.manifest_sha256,
                            run.release_id,
                            run.release_hash,
                            run.binding_id,
                            run.binding_hash,
                            run.account_group_id,
                            run.strategy_slot_id,
                            run.selection_evidence_id,
                            run.selection_artifact_hash,
                            run.execution_plan_id,
                            run.execution_plan_hash,
                            run.status.value,
                            psycopg2.extras.Json(run.run_payload_json),
                            run.created_at,
                            run.updated_at,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise InvalidStateTransitionError(
                        "simulation daily run conflicts with an existing run",
                        context={"run_id": run.run_id, "strategy_id": run.strategy_id, "binding_id": run.binding_id},
                    ) from exc
        return run

    def get_simulation_daily_run(self, run_id: str) -> SimulationDailyRun:
        rows = self._fetch_rows(
            "SELECT * FROM paper_v2.simulation_daily_run WHERE run_id = %s",
            (run_id,),
        )
        if not rows:
            raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id})
        return self._daily_run_from_row(rows[0])

    def get_simulation_daily_run_by_key(
        self,
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: Any,
    ) -> SimulationDailyRun | None:
        rows = self._fetch_rows(
            """
            SELECT *
            FROM paper_v2.simulation_daily_run
            WHERE strategy_id = %s AND binding_id = %s AND trade_date = %s
            """,
            (strategy_id, binding_id, trade_date),
        )
        return self._daily_run_from_row(rows[0]) if rows else None

    def list_simulation_daily_runs(
        self,
        *,
        trade_date: Any | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        status: SimulationDailyRunStatus | str | None = None,
        limit: int = 100,
    ) -> list[SimulationDailyRun]:
        clauses: list[str] = []
        params: list[Any] = []
        if trade_date is not None:
            clauses.append("trade_date = %s")
            params.append(trade_date)
        if broker_backend is not None:
            backend = (
                broker_backend.value
                if isinstance(broker_backend, SimulationBrokerBackend)
                else str(broker_backend)
            )
            clauses.append("broker_backend = %s")
            params.append(backend)
        if strategy_id is not None:
            clauses.append("strategy_id = %s")
            params.append(strategy_id)
        if status is not None:
            status_value = status.value if isinstance(status, SimulationDailyRunStatus) else str(status)
            clauses.append("status = %s")
            params.append(status_value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._fetch_rows(
            f"""
            SELECT *
            FROM paper_v2.simulation_daily_run
            {where}
            ORDER BY trade_date DESC, updated_at DESC, created_at DESC, run_id
            LIMIT %s
            """,
            tuple(params),
        )
        return [self._daily_run_from_row(row) for row in rows]

    def update_simulation_daily_run(
        self,
        run_id: str,
        *,
        status: SimulationDailyRunStatus | None = None,
        selection_evidence: DailySelectionEvidence | None = None,
        execution_plan: ExecutionPlan | None = None,
        payload_patch: dict[str, Any] | None = None,
        payload_unset: Iterable[str] | None = None,
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged_payload = {**current.run_payload_json, **(payload_patch or {})}
        merged_payload = preserve_tca_sidecar(current.run_payload_json, merged_payload)
        for key in payload_unset or ():
            merged_payload.pop(str(key), None)
        updated = current.model_copy(
            update={
                "status": status or current.status,
                "selection_evidence_id": selection_evidence.evidence_id if selection_evidence else current.selection_evidence_id,
                "selection_artifact_hash": selection_evidence.artifact_hash if selection_evidence else current.selection_artifact_hash,
                "execution_plan_id": execution_plan.plan_id if execution_plan else current.execution_plan_id,
                "execution_plan_hash": execution_plan.plan_hash if execution_plan else current.execution_plan_hash,
                "run_payload_json": merged_payload,
            }
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.simulation_daily_run
                    SET status = %s,
                        selection_evidence_id = %s,
                        selection_artifact_hash = %s,
                        execution_plan_id = %s,
                        execution_plan_hash = %s,
                        run_payload_json = %s,
                        updated_at = now()
                    WHERE run_id = %s
                    """,
                    (
                        updated.status.value,
                        updated.selection_evidence_id,
                        updated.selection_artifact_hash,
                        updated.execution_plan_id,
                        updated.execution_plan_hash,
                        psycopg2.extras.Json(updated.run_payload_json),
                        run_id,
                    ),
                )
        return self.get_simulation_daily_run(run_id)

    def merge_run_tca_capture_sidecar(
        self,
        *,
        run_id: str,
        expected_plan_id: str,
        expected_plan_hash: str,
        parent_intent_id: str,
        decision_capture: dict[str, Any] | None = None,
        capture_error: dict[str, Any] | None = None,
        capture_batch_id: str | None = None,
    ) -> CaptureMergeOutcome:
        """CAS-merge one TCA parent observation without touching run state.

        The generic update path intentionally cannot replace this namespace.
        This is the sole PostgreSQL writer that obtains a row lock and applies
        the first-write/hash comparison contract for run-side evidence.
        """

        if sum(value is not None for value in (decision_capture, capture_error, capture_batch_id)) != 1:
            raise ValueError("exactly one run TCA capture mutation is required")
        with self._conn_factory() as conn:
            original_autocommit = bool(getattr(conn, "autocommit", False))
            try:
                if original_autocommit:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT execution_plan_id, execution_plan_hash, run_payload_json
                        FROM paper_v2.simulation_daily_run
                        WHERE run_id = %s
                        FOR UPDATE
                        """,
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        conn.rollback()
                        return CaptureMergeOutcome.NOT_FOUND
                    if row.get("execution_plan_id") != expected_plan_id or row.get("execution_plan_hash") != expected_plan_hash:
                        conn.rollback()
                        return CaptureMergeOutcome.IDENTITY_DRIFT
                    payload = dict(row.get("run_payload_json") or {})
                    existing = payload.get(TCA_OBSERVATION_KEY)
                    if existing is None:
                        sidecar = new_run_tca_sidecar(
                            execution_plan_id=expected_plan_id,
                            execution_plan_hash=expected_plan_hash,
                        )
                    elif not isinstance(existing, dict):
                        conn.rollback()
                        return CaptureMergeOutcome.IDENTITY_DRIFT
                    else:
                        sidecar = dict(existing)
                        if (
                            sidecar.get("execution_plan_id") != expected_plan_id
                            or sidecar.get("execution_plan_hash") != expected_plan_hash
                        ):
                            conn.rollback()
                            return CaptureMergeOutcome.IDENTITY_DRIFT
                    if decision_capture is not None:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="decision_capture_by_parent",
                            parent_intent_id=parent_intent_id,
                            value=decision_capture,
                        )
                    elif capture_error is not None:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="capture_errors",
                            parent_intent_id=parent_intent_id,
                            value=capture_error,
                        )
                    else:
                        outcome = merge_parent_first_write(
                            sidecar,
                            section="capture_batch_id_by_parent",
                            parent_intent_id=parent_intent_id,
                            value=str(capture_batch_id),
                        )
                    if outcome == CaptureMergeOutcome.CONFLICT:
                        conn.rollback()
                        return outcome
                    payload[TCA_OBSERVATION_KEY] = sidecar
                    cur.execute(
                        """
                        UPDATE paper_v2.simulation_daily_run
                        SET run_payload_json = %s, updated_at = now()
                        WHERE run_id = %s
                        """,
                        (psycopg2.extras.Json(payload), run_id),
                    )
                conn.commit()
                return outcome
            except Exception:
                conn.rollback()
                raise
            finally:
                if original_autocommit:
                    conn.autocommit = True

    def _fetch_rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _daily_run_with_binding_slots(
        run: SimulationDailyRun,
        binding: SimulationReleaseBinding,
    ) -> SimulationDailyRun:
        updates: dict[str, Any] = {}
        if run.account_group_id is None and binding.account_group_id is not None:
            updates["account_group_id"] = binding.account_group_id
        if run.strategy_slot_id is None and binding.strategy_slot_id is not None:
            updates["strategy_slot_id"] = binding.strategy_slot_id
        if not updates:
            return run
        payload = {
            **run.run_payload_json,
            "account_group_id": updates.get("account_group_id", run.account_group_id),
            "strategy_slot_id": updates.get("strategy_slot_id", run.strategy_slot_id),
        }
        return run.model_copy(update={**updates, "run_payload_json": payload})

    @staticmethod
    def _release_from_row(row: dict[str, Any]) -> StrategyRuntimeRelease:
        return StrategyRuntimeRelease(
            release_id=row["release_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            base_release_id=row.get("base_release_id"),
            runtime_profile_id=row["runtime_profile_id"],
            runtime_profile_version_id=row["runtime_profile_version_id"],
            runtime_profile_sha256=row["runtime_profile_sha256"],
            daily_strategy_profile_version_id=row["daily_strategy_profile_version_id"],
            execution_policy_version_id=row["execution_policy_version_id"],
            execution_policy_sha256=row["execution_policy_sha256"],
            tail_policy_version_id=row["tail_policy_version_id"],
            tail_policy_sha256=row["tail_policy_sha256"],
            release_config_json=row.get("release_config_json") or {},
            release_hash=row["release_hash"],
            validation_state=RuntimeReleaseValidationState(row["validation_state"]),
            validation_evidence=row.get("validation_evidence") or {},
            effective_from=row.get("effective_from"),
            effective_to=row.get("effective_to"),
            created_by=row.get("created_by"),
            created_reason=row.get("created_reason"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _evidence_from_row(row: dict[str, Any]) -> DailySelectionEvidence:
        return DailySelectionEvidence(
            evidence_id=row["evidence_id"],
            target_trade_date=row["target_trade_date"],
            cutoff_date=row.get("cutoff_date"),
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            release_id=row.get("release_id"),
            release_hash=row.get("release_hash"),
            runtime_profile_version_id=row["runtime_profile_version_id"],
            runtime_profile_hash=row["runtime_profile_hash"],
            source_type=row["source_type"],
            data_source=row["data_source"],
            candidate_count=int(row["candidate_count"]),
            excluded_count=int(row["excluded_count"]),
            artifact_hash=row["artifact_hash"],
            evidence_payload_json=row.get("evidence_payload_json") or {},
            created_at=row["created_at"],
            created_by=row.get("created_by"),
        )

    @staticmethod
    def _binding_from_row(row: dict[str, Any]) -> SimulationReleaseBinding:
        return SimulationReleaseBinding(
            binding_id=row["binding_id"],
            strategy_id=row["strategy_id"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            broker_backend=SimulationBrokerBackend(row["broker_backend"]),
            broker_account_id=row.get("broker_account_id"),
            account_group_id=row.get("account_group_id"),
            strategy_slot_id=row.get("strategy_slot_id"),
            capital_allocation=float(row["capital_allocation"]),
            strategy_name=row.get("strategy_name"),
            order_remark_prefix=row.get("order_remark_prefix"),
            effective_from=row.get("effective_from"),
            effective_to=row.get("effective_to"),
            approval_state=SimulationBindingApprovalState(row["approval_state"]),
            binding_config_json=row.get("binding_config_json") or {},
            binding_hash=row["binding_hash"],
            created_by=row.get("created_by"),
            created_reason=row.get("created_reason"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _execution_plan_from_row(row: dict[str, Any]) -> ExecutionPlan:
        payload = row.get("plan_payload_json") or {}
        intent_payloads = payload.get("intents") if isinstance(payload.get("intents"), list) else []
        decisions_payloads = payload.get("trading_rule_decisions") if isinstance(payload.get("trading_rule_decisions"), list) else []
        intents = [
            ExecutionPlanIntent(
                intent_id=item["intent_id"],
                plan_id=row["plan_id"],
                strategy_id=row["strategy_id"],
                portfolio_id=row["portfolio_id"],
                package_id=row["package_id"],
                release_id=row["release_id"],
                release_hash=row["release_hash"],
                binding_id=row["binding_id"],
                binding_hash=row["binding_hash"],
                symbol=item["symbol"],
                side=item["side"],
                target_quantity=int(item.get("target_quantity") or 0),
                delta_quantity=int(item.get("delta_quantity") or 0),
                order_quantity=int(item.get("order_quantity") or item.get("quantity") or 0),
                target_weight=item.get("target_weight"),
                current_quantity=int(item.get("current_quantity") or 0),
                current_available_quantity=item.get("current_available_quantity"),
                rebalance_reason=str(item.get("rebalance_reason") or ""),
                trading_rule_decision_id=str(item.get("trading_rule_decision_id") or ""),
                schedule_window=item.get("schedule_window") if isinstance(item.get("schedule_window"), dict) else {},
                price_policy=item.get("price_policy") if isinstance(item.get("price_policy"), dict) else {},
                risk_context=item.get("risk_context") if isinstance(item.get("risk_context"), dict) else {},
                metadata=item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
            )
            for item in intent_payloads
        ]
        decisions = [
            TradingRuleDecision(
                decision_id=item["decision_id"],
                symbol=item["symbol"],
                market_board=item["market_board"],
                side=item["side"],
                requested_quantity=int(item.get("requested_quantity") or 0),
                legal_quantity=int(item.get("legal_quantity") or 0),
                lot_rule=item.get("lot_rule") if isinstance(item.get("lot_rule"), dict) else {},
                price_limit_rule=item.get("price_limit_rule") if isinstance(item.get("price_limit_rule"), dict) else {},
                tplus1_available_quantity=item.get("tplus1_available_quantity"),
                decision=item["decision"],
                reason_code=item["reason_code"],
                source_version=item["source_version"],
                decision_hash=item["decision_hash"],
            )
            for item in decisions_payloads
        ]
        return ExecutionPlan(
            plan_id=row["plan_id"],
            strategy_id=row["strategy_id"],
            portfolio_id=row["portfolio_id"],
            package_id=row["package_id"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            binding_id=row["binding_id"],
            binding_hash=row["binding_hash"],
            selection_evidence_id=row["selection_evidence_id"],
            selection_evidence_hash=row["selection_evidence_hash"],
            target_trade_date=row["target_trade_date"],
            execution_policy_version_id=row["execution_policy_version_id"],
            execution_policy_sha256=row["execution_policy_sha256"],
            tail_policy_version_id=row["tail_policy_version_id"],
            tail_policy_sha256=row["tail_policy_sha256"],
            intents=intents,
            trading_rule_decisions=decisions,
            plan_payload_json=payload,
            plan_hash=row["plan_hash"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _daily_run_from_row(row: dict[str, Any]) -> SimulationDailyRun:
        return SimulationDailyRun(
            run_id=row["run_id"],
            trade_date=row["trade_date"],
            strategy_id=row["strategy_id"],
            broker_backend=SimulationBrokerBackend(row["broker_backend"]),
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            release_id=row["release_id"],
            release_hash=row["release_hash"],
            binding_id=row["binding_id"],
            binding_hash=row["binding_hash"],
            account_group_id=row.get("account_group_id"),
            strategy_slot_id=row.get("strategy_slot_id"),
            selection_evidence_id=row.get("selection_evidence_id"),
            selection_artifact_hash=row.get("selection_artifact_hash"),
            execution_plan_id=row.get("execution_plan_id"),
            execution_plan_hash=row.get("execution_plan_hash"),
            status=SimulationDailyRunStatus(row["status"]),
            run_payload_json=row.get("run_payload_json") or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class InMemorySimulationRuntimeRepository:
    def __init__(self) -> None:
        self.releases: dict[str, StrategyRuntimeRelease] = {}
        self.release_hash_index: dict[str, str] = {}
        self.bindings: dict[str, SimulationReleaseBinding] = {}
        self.binding_hash_index: dict[str, str] = {}
        self.daily_selection_evidences: dict[str, DailySelectionEvidence] = {}
        self.daily_selection_hash_index: dict[str, str] = {}
        self.execution_plans: dict[str, ExecutionPlan] = {}
        self.execution_plan_hash_index: dict[str, str] = {}
        self.daily_runs: dict[str, SimulationDailyRun] = {}
        self.daily_run_key_index: dict[tuple[str, str, Any], str] = {}

    def save_strategy_runtime_release(self, release: StrategyRuntimeRelease) -> StrategyRuntimeRelease:
        if release.release_hash in self.release_hash_index:
            return self.releases[self.release_hash_index[release.release_hash or ""]]
        if release.release_id in self.releases:
            existing = self.releases[release.release_id]
            if existing.release_hash != release.release_hash:
                raise InvalidStateTransitionError(
                    "strategy runtime release conflicts with an existing immutable release",
                    context={"release_id": release.release_id, "release_hash": release.release_hash},
                )
            return existing
        self.releases[release.release_id] = release
        self.release_hash_index[release.release_hash or ""] = release.release_id
        return release

    def get_strategy_runtime_release(self, release_id: str) -> StrategyRuntimeRelease:
        try:
            return self.releases[release_id]
        except KeyError as exc:
            raise DataUnavailableError("strategy runtime release does not exist", context={"release_id": release_id}) from exc

    def get_strategy_runtime_release_by_hash(self, release_hash: str) -> StrategyRuntimeRelease | None:
        release_id = self.release_hash_index.get(release_hash)
        return self.releases[release_id] if release_id else None

    def save_simulation_release_binding(self, binding: SimulationReleaseBinding) -> SimulationReleaseBinding:
        self.get_strategy_runtime_release(binding.release_id)
        if binding.binding_hash in self.binding_hash_index:
            return self.bindings[self.binding_hash_index[binding.binding_hash or ""]]
        if binding.binding_id in self.bindings:
            existing = self.bindings[binding.binding_id]
            if existing.binding_hash != binding.binding_hash:
                raise InvalidStateTransitionError(
                    "simulation release binding conflicts with an existing immutable binding",
                    context={"binding_id": binding.binding_id, "binding_hash": binding.binding_hash},
                )
            return existing
        self.bindings[binding.binding_id] = binding
        self.binding_hash_index[binding.binding_hash or ""] = binding.binding_id
        return binding

    def get_simulation_release_binding(self, binding_id: str) -> SimulationReleaseBinding:
        try:
            return self.bindings[binding_id]
        except KeyError as exc:
            raise DataUnavailableError("simulation release binding does not exist", context={"binding_id": binding_id}) from exc

    def get_simulation_release_binding_by_hash(self, binding_hash: str) -> SimulationReleaseBinding | None:
        binding_id = self.binding_hash_index.get(binding_hash)
        return self.bindings[binding_id] if binding_id else None

    def list_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        release_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        active_on: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        rows = list(self.bindings.values())
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if release_id is not None:
            rows = [row for row in rows if row.release_id == release_id]
        if broker_backend is not None:
            backend = broker_backend if isinstance(broker_backend, SimulationBrokerBackend) else SimulationBrokerBackend(str(broker_backend))
            rows = [row for row in rows if row.broker_backend == backend]
        states = {
            state if isinstance(state, SimulationBindingApprovalState) else SimulationBindingApprovalState(str(state))
            for state in (approval_states or [])
        }
        if states:
            rows = [row for row in rows if row.approval_state in states]
        if active_on is not None:
            rows = [
                row
                for row in rows
                if (row.effective_from is None or row.effective_from <= active_on)
                and (row.effective_to is None or row.effective_to >= active_on)
            ]
        rows.sort(key=lambda item: (item.created_at, item.binding_id), reverse=True)
        return rows[:limit]

    def list_latest_simulation_release_bindings(
        self,
        *,
        strategy_id: str | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        approval_states: Iterable[SimulationBindingApprovalState | str] | None = None,
        effective_from_on_or_before: date | None = None,
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        rows = list(self.bindings.values())
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if broker_backend is not None:
            backend = broker_backend if isinstance(broker_backend, SimulationBrokerBackend) else SimulationBrokerBackend(str(broker_backend))
            rows = [row for row in rows if row.broker_backend == backend]
        states = {
            state if isinstance(state, SimulationBindingApprovalState) else SimulationBindingApprovalState(str(state))
            for state in (approval_states or [])
        }
        if states:
            rows = [row for row in rows if row.approval_state in states]
        if effective_from_on_or_before is not None:
            rows = [
                row
                for row in rows
                if row.effective_from is None or row.effective_from <= effective_from_on_or_before
            ]
        latest: dict[tuple[str, SimulationBrokerBackend], SimulationReleaseBinding] = {}
        for row in rows:
            key = (row.strategy_id, row.broker_backend)
            current = latest.get(key)
            if current is None or (
                row.effective_from or date.min,
                row.created_at,
                row.binding_id,
            ) > (
                current.effective_from or date.min,
                current.created_at,
                current.binding_id,
            ):
                latest[key] = row
        ordered = sorted(
            latest.values(),
            key=lambda item: (item.created_at, item.binding_id),
            reverse=True,
        )
        return ordered[:limit]

    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        if evidence.artifact_hash in self.daily_selection_hash_index:
            return self.daily_selection_evidences[self.daily_selection_hash_index[evidence.artifact_hash]]
        self.daily_selection_evidences[evidence.evidence_id] = evidence
        self.daily_selection_hash_index[evidence.artifact_hash] = evidence.evidence_id
        return evidence

    def get_daily_selection_evidence(self, evidence_id: str) -> DailySelectionEvidence:
        try:
            return self.daily_selection_evidences[evidence_id]
        except KeyError as exc:
            raise DataUnavailableError("daily selection evidence does not exist", context={"evidence_id": evidence_id}) from exc

    def get_daily_selection_evidence_by_hash(self, artifact_hash: str) -> DailySelectionEvidence | None:
        evidence_id = self.daily_selection_hash_index.get(artifact_hash)
        return self.daily_selection_evidences[evidence_id] if evidence_id else None

    def save_execution_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        self.get_strategy_runtime_release(plan.release_id)
        self.get_simulation_release_binding(plan.binding_id)
        self.get_daily_selection_evidence(plan.selection_evidence_id)
        if plan.plan_hash in self.execution_plan_hash_index:
            return self.execution_plans[self.execution_plan_hash_index[plan.plan_hash]]
        if plan.plan_id in self.execution_plans:
            existing = self.execution_plans[plan.plan_id]
            if existing.plan_hash != plan.plan_hash:
                raise InvalidStateTransitionError(
                    "execution plan conflicts with an existing immutable plan",
                    context={"plan_id": plan.plan_id, "plan_hash": plan.plan_hash},
                )
            return existing
        self.execution_plans[plan.plan_id] = plan
        self.execution_plan_hash_index[plan.plan_hash] = plan.plan_id
        return plan

    def get_execution_plan(self, plan_id: str) -> ExecutionPlan:
        try:
            return self.execution_plans[plan_id]
        except KeyError as exc:
            raise DataUnavailableError("execution plan does not exist", context={"plan_id": plan_id}) from exc

    def get_execution_plan_by_hash(self, plan_hash: str) -> ExecutionPlan | None:
        plan_id = self.execution_plan_hash_index.get(plan_hash)
        return self.execution_plans[plan_id] if plan_id else None

    def save_simulation_daily_run(self, run: SimulationDailyRun) -> SimulationDailyRun:
        existing = self.get_simulation_daily_run_by_key(
            strategy_id=run.strategy_id,
            binding_id=run.binding_id,
            trade_date=run.trade_date,
        )
        if existing is not None:
            return existing
        self.get_strategy_runtime_release(run.release_id)
        binding = self.get_simulation_release_binding(run.binding_id)
        run = SimulationRuntimeRepository._daily_run_with_binding_slots(run, binding)
        if run.run_id in self.daily_runs:
            existing_by_id = self.daily_runs[run.run_id]
            if (
                existing_by_id.strategy_id,
                existing_by_id.binding_id,
                existing_by_id.trade_date,
            ) != (run.strategy_id, run.binding_id, run.trade_date):
                raise InvalidStateTransitionError(
                    "simulation daily run conflicts with an existing run",
                    context={"run_id": run.run_id, "strategy_id": run.strategy_id, "binding_id": run.binding_id},
                )
            return existing_by_id
        self.daily_runs[run.run_id] = run
        self.daily_run_key_index[(run.strategy_id, run.binding_id, run.trade_date)] = run.run_id
        return run

    def get_simulation_daily_run(self, run_id: str) -> SimulationDailyRun:
        try:
            return self.daily_runs[run_id]
        except KeyError as exc:
            raise DataUnavailableError("simulation daily run does not exist", context={"run_id": run_id}) from exc

    def get_simulation_daily_run_by_key(
        self,
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: Any,
    ) -> SimulationDailyRun | None:
        run_id = self.daily_run_key_index.get((strategy_id, binding_id, trade_date))
        return self.daily_runs[run_id] if run_id else None

    def list_simulation_daily_runs(
        self,
        *,
        trade_date: Any | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        status: SimulationDailyRunStatus | str | None = None,
        limit: int = 100,
    ) -> list[SimulationDailyRun]:
        rows = list(self.daily_runs.values())
        if trade_date is not None:
            rows = [row for row in rows if row.trade_date == trade_date]
        if broker_backend is not None:
            backend = broker_backend if isinstance(broker_backend, SimulationBrokerBackend) else SimulationBrokerBackend(str(broker_backend))
            rows = [row for row in rows if row.broker_backend == backend]
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if status is not None:
            expected = status if isinstance(status, SimulationDailyRunStatus) else SimulationDailyRunStatus(str(status))
            rows = [row for row in rows if row.status == expected]
        rows.sort(key=lambda item: (item.trade_date, item.updated_at, item.created_at, item.run_id), reverse=True)
        return rows[:limit]

    def update_simulation_daily_run(
        self,
        run_id: str,
        *,
        status: SimulationDailyRunStatus | None = None,
        selection_evidence: DailySelectionEvidence | None = None,
        execution_plan: ExecutionPlan | None = None,
        payload_patch: dict[str, Any] | None = None,
        payload_unset: Iterable[str] | None = None,
    ) -> SimulationDailyRun:
        current = self.get_simulation_daily_run(run_id)
        merged_payload = {**current.run_payload_json, **(payload_patch or {})}
        merged_payload = preserve_tca_sidecar(current.run_payload_json, merged_payload)
        for key in payload_unset or ():
            merged_payload.pop(str(key), None)
        updated = current.model_copy(
            update={
                "status": status or current.status,
                "selection_evidence_id": selection_evidence.evidence_id if selection_evidence else current.selection_evidence_id,
                "selection_artifact_hash": selection_evidence.artifact_hash if selection_evidence else current.selection_artifact_hash,
                "execution_plan_id": execution_plan.plan_id if execution_plan else current.execution_plan_id,
                "execution_plan_hash": execution_plan.plan_hash if execution_plan else current.execution_plan_hash,
                "run_payload_json": merged_payload,
            }
        )
        self.daily_runs[run_id] = updated
        return updated

    def merge_run_tca_capture_sidecar(
        self,
        *,
        run_id: str,
        expected_plan_id: str,
        expected_plan_hash: str,
        parent_intent_id: str,
        decision_capture: dict[str, Any] | None = None,
        capture_error: dict[str, Any] | None = None,
        capture_batch_id: str | None = None,
    ) -> CaptureMergeOutcome:
        if sum(value is not None for value in (decision_capture, capture_error, capture_batch_id)) != 1:
            raise ValueError("exactly one run TCA capture mutation is required")
        current = self.daily_runs.get(run_id)
        if current is None:
            return CaptureMergeOutcome.NOT_FOUND
        if current.execution_plan_id != expected_plan_id or current.execution_plan_hash != expected_plan_hash:
            return CaptureMergeOutcome.IDENTITY_DRIFT
        payload = dict(current.run_payload_json or {})
        existing = payload.get(TCA_OBSERVATION_KEY)
        if existing is None:
            sidecar = new_run_tca_sidecar(
                execution_plan_id=expected_plan_id,
                execution_plan_hash=expected_plan_hash,
            )
        elif not isinstance(existing, dict):
            return CaptureMergeOutcome.IDENTITY_DRIFT
        else:
            sidecar = dict(existing)
            if sidecar.get("execution_plan_id") != expected_plan_id or sidecar.get("execution_plan_hash") != expected_plan_hash:
                return CaptureMergeOutcome.IDENTITY_DRIFT
        if decision_capture is not None:
            outcome = merge_parent_first_write(
                sidecar,
                section="decision_capture_by_parent",
                parent_intent_id=parent_intent_id,
                value=decision_capture,
            )
        elif capture_error is not None:
            outcome = merge_parent_first_write(
                sidecar,
                section="capture_errors",
                parent_intent_id=parent_intent_id,
                value=capture_error,
            )
        else:
            outcome = merge_parent_first_write(
                sidecar,
                section="capture_batch_id_by_parent",
                parent_intent_id=parent_intent_id,
                value=str(capture_batch_id),
            )
        if outcome == CaptureMergeOutcome.CONFLICT:
            return outcome
        payload[TCA_OBSERVATION_KEY] = sidecar
        self.daily_runs[run_id] = current.model_copy(update={"run_payload_json": payload})
        return outcome
