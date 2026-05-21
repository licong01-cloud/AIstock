"""Runtime release and simulation binding repositories."""

from __future__ import annotations

from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
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
                            broker_backend, broker_account_id, capital_allocation, strategy_name,
                            order_remark_prefix, effective_from, effective_to, approval_state,
                            binding_config_json, binding_hash, created_by, created_reason, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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

    def _fetch_rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

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


class InMemorySimulationRuntimeRepository:
    def __init__(self) -> None:
        self.releases: dict[str, StrategyRuntimeRelease] = {}
        self.release_hash_index: dict[str, str] = {}
        self.bindings: dict[str, SimulationReleaseBinding] = {}
        self.binding_hash_index: dict[str, str] = {}

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
        limit: int = 100,
    ) -> list[SimulationReleaseBinding]:
        rows = list(self.bindings.values())
        if strategy_id is not None:
            rows = [row for row in rows if row.strategy_id == strategy_id]
        if release_id is not None:
            rows = [row for row in rows if row.release_id == release_id]
        rows.sort(key=lambda item: (item.created_at, item.binding_id), reverse=True)
        return rows[:limit]
