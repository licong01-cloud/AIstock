"""Transactional repository for the internal LocalSIM successor control plane."""

from __future__ import annotations

from contextlib import AbstractContextManager
from copy import deepcopy
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Callable, Protocol

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import SimulationBrokerBackend, SimulationReleaseBinding, StrategyRuntimeRelease
from .successor_models import (
    LegacyLocalSimAccountLineageV1,
    LegacyLocalSimLineageStatus,
    LocalSimReplayJobV1,
    LocalSimReplayStatus,
    SimulationAccountStatus,
    SimulationAccountV1,
)


SuccessorConnFactory = Callable[[], AbstractContextManager[Any]]


class LocalSimSuccessorRepositoryProtocol(Protocol):
    def create_account_bundle(
        self,
        *,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[SimulationAccountV1, StrategyRuntimeRelease, SimulationReleaseBinding]: ...

    def get_account(self, account_id: str) -> SimulationAccountV1: ...

    def get_release(self, release_id: str) -> StrategyRuntimeRelease: ...

    def get_binding(self, binding_id: str) -> SimulationReleaseBinding: ...

    def create_successor_binding(
        self,
        *,
        account: SimulationAccountV1,
        source_binding_id: str,
        expected_source_binding_hash: str,
        source_effective_to: date,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding]: ...

    def transition_account(
        self,
        *,
        account_id: str,
        expected_version: int,
        target_status: SimulationAccountStatus,
        updated_at: datetime,
    ) -> SimulationAccountV1: ...

    def create_lineage_bundle(
        self,
        *,
        account: SimulationAccountV1,
        lineage: LegacyLocalSimAccountLineageV1,
    ) -> tuple[SimulationAccountV1, LegacyLocalSimAccountLineageV1]: ...

    def get_lineage_by_legacy_account(self, legacy_account_id: str) -> LegacyLocalSimAccountLineageV1 | None: ...

    def transition_lineage(
        self,
        *,
        lineage_id: str,
        expected_version: int,
        target_status: LegacyLocalSimLineageStatus,
        updated_at: datetime,
    ) -> LegacyLocalSimAccountLineageV1: ...

    def save_replay_job(self, job: LocalSimReplayJobV1) -> LocalSimReplayJobV1: ...

    def get_replay_job(self, replay_job_id: str) -> LocalSimReplayJobV1: ...

    def transition_replay_job(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        update: dict[str, Any],
        updated_at: datetime,
    ) -> LocalSimReplayJobV1: ...

    def create_replay_live_successor(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        activation_trade_date: date,
        updated_at: datetime,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding, LocalSimReplayJobV1]: ...


def _transaction_conn() -> AbstractContextManager[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class LocalSimSuccessorRepository:
    """PostgreSQL implementation; every mutation has CAS and transaction readback."""

    def __init__(self, conn_factory: SuccessorConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transaction_conn

    def create_account_bundle(
        self,
        *,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[SimulationAccountV1, StrategyRuntimeRelease, SimulationReleaseBinding]:
        self._validate_new_account_binding(account=account, release=release, binding=binding)
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    self._insert_release(cur, release)
                    self._insert_account(cur, account)
                    self._insert_binding(cur, binding)
                    persisted_account = self._select_account(cur, account.account_id, for_update=True)
                    persisted_release = self._select_release(cur, release.release_id)
                    persisted_binding = self._select_binding(cur, binding.binding_id)
                    self._require_same_identity(account, persisted_account, object_name="account")
                    self._require_same_identity(release, persisted_release, object_name="release")
                    self._require_same_identity(binding, persisted_binding, object_name="binding")
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM account, release, and binding transaction conflicts with existing authority",
                context={
                    "reason_code": "LOCALSIM_ACCOUNT_BUNDLE_CONFLICT",
                    "account_id": account.account_id,
                    "release_id": release.release_id,
                    "binding_id": binding.binding_id,
                },
            ) from exc
        return persisted_account, persisted_release, persisted_binding

    def get_account(self, account_id: str) -> SimulationAccountV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_account(cur, account_id)

    def get_release(self, release_id: str) -> StrategyRuntimeRelease:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_release(cur, release_id)

    def get_binding(self, binding_id: str) -> SimulationReleaseBinding:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_binding(cur, binding_id)

    def create_successor_binding(
        self,
        *,
        account: SimulationAccountV1,
        source_binding_id: str,
        expected_source_binding_hash: str,
        source_effective_to: date,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding]:
        self._validate_new_account_binding(account=account, release=release, binding=binding)
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    persisted_account = self._select_account(cur, account.account_id, for_update=True)
                    self._require_same_identity(account, persisted_account, object_name="account")
                    source = self._select_binding(cur, source_binding_id, for_update=True)
                    self._validate_successor_source(
                        account=account,
                        source=source,
                        expected_source_binding_hash=expected_source_binding_hash,
                        source_effective_to=source_effective_to,
                    )
                    if source.effective_to is None:
                        cur.execute(
                            """
                            UPDATE paper_v2.simulation_release_binding
                            SET effective_to = %s, updated_at = NOW()
                            WHERE binding_id = %s AND binding_hash = %s AND effective_to IS NULL
                            """,
                            (source_effective_to, source_binding_id, expected_source_binding_hash),
                        )
                        if cur.rowcount != 1:
                            raise InvalidStateTransitionError(
                                "LocalSIM successor source binding window CAS failed",
                                context={"reason_code": "LOCALSIM_SUCCESSOR_SOURCE_CAS_CONFLICT"},
                            )
                    self._insert_release(cur, release)
                    self._insert_binding(cur, binding)
                    persisted_release = self._select_release(cur, release.release_id)
                    persisted_binding = self._select_binding(cur, binding.binding_id)
                    self._require_same_identity(release, persisted_release, object_name="release")
                    self._require_same_identity(binding, persisted_binding, object_name="binding")
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM successor release and binding conflict with existing authority",
                context={
                    "reason_code": "LOCALSIM_SUCCESSOR_BINDING_CONFLICT",
                    "account_id": account.account_id,
                    "release_id": release.release_id,
                    "binding_id": binding.binding_id,
                },
            ) from exc
        return persisted_release, persisted_binding

    def transition_account(
        self,
        *,
        account_id: str,
        expected_version: int,
        target_status: SimulationAccountStatus,
        updated_at: datetime,
    ) -> SimulationAccountV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.simulation_account_v1
                    SET status = %s, version = version + 1, updated_at = %s
                    WHERE account_id = %s AND version = %s
                    """,
                    (target_status.value, updated_at, account_id, expected_version),
                )
                if cur.rowcount != 1:
                    raise InvalidStateTransitionError(
                        "LocalSIM account lifecycle CAS failed",
                        context={
                            "reason_code": "LOCALSIM_ACCOUNT_CAS_CONFLICT",
                            "account_id": account_id,
                            "expected_version": expected_version,
                        },
                    )
                return self._select_account(cur, account_id, for_update=True)

    def create_lineage_bundle(
        self,
        *,
        account: SimulationAccountV1,
        lineage: LegacyLocalSimAccountLineageV1,
    ) -> tuple[SimulationAccountV1, LegacyLocalSimAccountLineageV1]:
        if lineage.account_id != account.account_id:
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage account identity does not match successor account",
                context={"reason_code": "LOCALSIM_LINEAGE_ACCOUNT_MISMATCH"},
            )
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    self._insert_account(cur, account)
                    cur.execute(
                        """
                        INSERT INTO paper_v2.legacy_localsim_account_lineage_v1 (
                            lineage_id, lineage_hash, legacy_account_id, account_id, release_id,
                            binding_id, ledger_scope_id, economic_facts_sha256, status, version,
                            created_by, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (lineage_hash) DO NOTHING
                        """,
                        (
                            lineage.lineage_id,
                            lineage.lineage_hash,
                            lineage.legacy_account_id,
                            lineage.account_id,
                            lineage.release_id,
                            lineage.binding_id,
                            lineage.ledger_scope_id,
                            lineage.economic_facts_sha256,
                            lineage.status.value,
                            lineage.version,
                            lineage.created_by,
                            lineage.created_at,
                            lineage.updated_at,
                        ),
                    )
                    persisted_account = self._select_account(cur, account.account_id, for_update=True)
                    cur.execute(
                        "SELECT * FROM paper_v2.legacy_localsim_account_lineage_v1 WHERE lineage_id = %s",
                        (lineage.lineage_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise InvalidStateTransitionError(
                            "legacy LocalSIM lineage transaction readback is missing",
                            context={"reason_code": "LOCALSIM_LINEAGE_READBACK_MISSING"},
                        )
                    persisted_lineage = self._lineage_from_row(dict(row))
                    self._require_same_identity(account, persisted_account, object_name="account")
                    self._require_same_identity(lineage, persisted_lineage, object_name="lineage")
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage conflicts with an existing mapping",
                context={
                    "reason_code": "LOCALSIM_LINEAGE_CONFLICT",
                    "legacy_account_id": lineage.legacy_account_id,
                    "account_id": lineage.account_id,
                },
            ) from exc
        return persisted_account, persisted_lineage

    def get_lineage_by_legacy_account(self, legacy_account_id: str) -> LegacyLocalSimAccountLineageV1 | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM paper_v2.legacy_localsim_account_lineage_v1 WHERE legacy_account_id = %s",
                    (legacy_account_id,),
                )
                row = cur.fetchone()
                return self._lineage_from_row(dict(row)) if row is not None else None

    def transition_lineage(
        self,
        *,
        lineage_id: str,
        expected_version: int,
        target_status: LegacyLocalSimLineageStatus,
        updated_at: datetime,
    ) -> LegacyLocalSimAccountLineageV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.legacy_localsim_account_lineage_v1
                    SET status = %s, version = version + 1, updated_at = %s
                    WHERE lineage_id = %s AND version = %s
                    """,
                    (target_status.value, updated_at, lineage_id, expected_version),
                )
                if cur.rowcount != 1:
                    raise InvalidStateTransitionError(
                        "legacy LocalSIM lineage CAS failed",
                        context={
                            "reason_code": "LOCALSIM_LINEAGE_CAS_CONFLICT",
                            "lineage_id": lineage_id,
                            "expected_version": expected_version,
                        },
                    )
                cur.execute(
                    "SELECT * FROM paper_v2.legacy_localsim_account_lineage_v1 WHERE lineage_id = %s",
                    (lineage_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise DataUnavailableError(
                        "legacy LocalSIM lineage does not exist", context={"lineage_id": lineage_id}
                    )
                return self._lineage_from_row(dict(row))

    def save_replay_job(self, job: LocalSimReplayJobV1) -> LocalSimReplayJobV1:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    self._select_account(cur, job.simulation_account_id)
                    self._select_release(cur, job.release_id)
                    self._select_binding(cur, job.binding_id)
                    cur.execute(
                        """
                        INSERT INTO paper_v2.localsim_replay_job_v1 (
                            replay_job_id, replay_hash, simulation_account_id, release_id, binding_id,
                            day_engine_contract_id,
                            start_trade_date, end_trade_date, historical_source_id,
                            historical_source_sha256, calendar_snapshot_sha256, status,
                            next_trade_date, completed_trade_date, live_release_id, live_binding_id,
                            activation_trade_date, version, failure_code, failure_context,
                            created_by, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) ON CONFLICT (replay_hash) DO NOTHING
                        """,
                        self._replay_values(job),
                    )
                    persisted = self._select_replay(cur, job.replay_job_id, for_update=True)
                    self._require_same_identity(job, persisted, object_name="replay_job")
                    return persisted
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM replay job conflicts with existing account or binding authority",
                context={"reason_code": "LOCALSIM_REPLAY_CONFLICT", "replay_job_id": job.replay_job_id},
            ) from exc

    def get_replay_job(self, replay_job_id: str) -> LocalSimReplayJobV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_replay(cur, replay_job_id)

    def transition_replay_job(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        update: dict[str, Any],
        updated_at: datetime,
    ) -> LocalSimReplayJobV1:
        allowed = {
            "status",
            "next_trade_date",
            "completed_trade_date",
            "live_release_id",
            "live_binding_id",
            "activation_trade_date",
            "failure_code",
            "failure_context",
        }
        unknown = sorted(set(update) - allowed)
        if unknown:
            raise InvalidStateTransitionError(
                "LocalSIM replay update contains unsupported fields",
                context={"reason_code": "LOCALSIM_REPLAY_UPDATE_INVALID", "fields": unknown},
            )
        assignments = [f"{field} = %s" for field in update]
        values = [self._sql_value(update[field]) for field in update]
        assignments.extend(["version = version + 1", "updated_at = %s"])
        values.extend([updated_at, replay_job_id, expected_version])
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    UPDATE paper_v2.localsim_replay_job_v1
                    SET {", ".join(assignments)}
                    WHERE replay_job_id = %s AND version = %s
                    """,
                    tuple(values),
                )
                if cur.rowcount != 1:
                    raise InvalidStateTransitionError(
                        "LocalSIM replay job CAS failed",
                        context={
                            "reason_code": "LOCALSIM_REPLAY_CAS_CONFLICT",
                            "replay_job_id": replay_job_id,
                            "expected_version": expected_version,
                        },
                    )
                return self._select_replay(cur, replay_job_id, for_update=True)

    def create_replay_live_successor(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        activation_trade_date: date,
        updated_at: datetime,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding, LocalSimReplayJobV1]:
        self._validate_new_account_binding(account=account, release=release, binding=binding)
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    current = self._select_replay(cur, replay_job_id, for_update=True)
                    if current.version != expected_version or current.status is not LocalSimReplayStatus.READY_FOR_LIVE:
                        raise InvalidStateTransitionError(
                            "LocalSIM replay is not ready for an atomic live successor",
                            context={"reason_code": "LOCALSIM_REPLAY_LIVE_SUCCESSOR_STATE_INVALID"},
                        )
                    persisted_account = self._select_account(cur, account.account_id, for_update=True)
                    self._require_same_identity(account, persisted_account, object_name="account")
                    self._insert_release(cur, release)
                    self._insert_binding(cur, binding)
                    cur.execute(
                        """
                        UPDATE paper_v2.localsim_replay_job_v1
                        SET live_release_id = %s, live_binding_id = %s,
                            activation_trade_date = %s,
                            status = 'ACTIVATION_PENDING_SAFE_BOUNDARY',
                            version = version + 1, updated_at = %s
                        WHERE replay_job_id = %s AND version = %s AND status = 'READY_FOR_LIVE'
                        """,
                        (
                            release.release_id,
                            binding.binding_id,
                            activation_trade_date,
                            updated_at,
                            replay_job_id,
                            expected_version,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "LocalSIM replay live-successor CAS failed",
                            context={"reason_code": "LOCALSIM_REPLAY_CAS_CONFLICT"},
                        )
                    persisted_release = self._select_release(cur, release.release_id)
                    persisted_binding = self._select_binding(cur, binding.binding_id)
                    persisted_job = self._select_replay(cur, replay_job_id, for_update=True)
                    self._require_same_identity(release, persisted_release, object_name="release")
                    self._require_same_identity(binding, persisted_binding, object_name="binding")
                    return persisted_release, persisted_binding, persisted_job
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM replay live successor conflicts with existing authority",
                context={"reason_code": "LOCALSIM_REPLAY_LIVE_SUCCESSOR_CONFLICT"},
            ) from exc

    @staticmethod
    def _validate_new_account_binding(
        *, account: SimulationAccountV1, release: StrategyRuntimeRelease, binding: SimulationReleaseBinding
    ) -> None:
        metadata = binding.binding_config_json.get("metadata")
        if (
            binding.broker_backend is not SimulationBrokerBackend.LOCAL_SIM
            or binding.broker_account_id != account.account_id
            or binding.package_id != account.package_id
            or binding.manifest_sha256 != account.manifest_sha256
            or binding.release_id != release.release_id
            or release.package_id != account.package_id
            or release.manifest_sha256 != account.manifest_sha256
            or not isinstance(metadata, dict)
            or metadata.get("localsim_account_id") != account.account_id
        ):
            raise InvalidStateTransitionError(
                "LocalSIM account bundle identities are inconsistent",
                context={"reason_code": "LOCALSIM_ACCOUNT_BUNDLE_IDENTITY_MISMATCH"},
            )

    @staticmethod
    def _validate_successor_source(
        *,
        account: SimulationAccountV1,
        source: SimulationReleaseBinding,
        expected_source_binding_hash: str,
        source_effective_to: date,
    ) -> None:
        metadata = source.binding_config_json.get("metadata")
        if (
            source.binding_hash != expected_source_binding_hash
            or source.broker_backend is not SimulationBrokerBackend.LOCAL_SIM
            or source.broker_account_id != account.account_id
            or not isinstance(metadata, dict)
            or metadata.get("localsim_account_id") != account.account_id
            or source.effective_to not in {None, source_effective_to}
        ):
            raise InvalidStateTransitionError(
                "LocalSIM successor source binding authority changed",
                context={"reason_code": "LOCALSIM_SUCCESSOR_SOURCE_CAS_CONFLICT"},
            )

    @staticmethod
    def _insert_release(cur: Any, release: StrategyRuntimeRelease) -> None:
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (release_hash) DO NOTHING
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

    @staticmethod
    def _insert_account(cur: Any, account: SimulationAccountV1) -> None:
        cur.execute(
            """
            INSERT INTO paper_v2.simulation_account_v1 (
                account_id, account_hash, schema_version, account_name, broker_backend,
                package_id, manifest_sha256, admission_receipt_id, initial_capital,
                account_config_json, status, version, created_by, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_hash) DO NOTHING
            """,
            (
                account.account_id,
                account.account_hash,
                account.schema_version,
                account.account_name,
                account.broker_backend.value,
                account.package_id,
                account.manifest_sha256,
                account.admission_receipt_id,
                account.initial_capital,
                psycopg2.extras.Json(account.account_config_json),
                account.status.value,
                account.version,
                account.created_by,
                account.created_at,
                account.updated_at,
            ),
        )

    @staticmethod
    def _insert_binding(cur: Any, binding: SimulationReleaseBinding) -> None:
        cur.execute(
            """
            INSERT INTO paper_v2.simulation_release_binding (
                binding_id, strategy_id, release_id, release_hash, package_id, manifest_sha256,
                broker_backend, broker_account_id, account_group_id, strategy_slot_id,
                capital_allocation, strategy_name, order_remark_prefix, effective_from, effective_to,
                approval_state, binding_config_json, binding_hash, created_by, created_reason,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (binding_hash) DO NOTHING
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

    @classmethod
    def _select_account(cls, cur: Any, account_id: str, *, for_update: bool = False) -> SimulationAccountV1:
        suffix = " FOR UPDATE" if for_update else ""
        cur.execute(f"SELECT * FROM paper_v2.simulation_account_v1 WHERE account_id = %s{suffix}", (account_id,))
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("LocalSIM simulation account does not exist", context={"account_id": account_id})
        return cls._account_from_row(dict(row))

    @staticmethod
    def _select_release(cur: Any, release_id: str) -> StrategyRuntimeRelease:
        cur.execute("SELECT * FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s", (release_id,))
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("strategy runtime release does not exist", context={"release_id": release_id})
        values = dict(row)
        return StrategyRuntimeRelease(
            release_id=values["release_id"],
            package_id=values["package_id"],
            manifest_sha256=values["manifest_sha256"],
            base_release_id=values.get("base_release_id"),
            runtime_profile_id=values["runtime_profile_id"],
            runtime_profile_version_id=values["runtime_profile_version_id"],
            runtime_profile_sha256=values["runtime_profile_sha256"],
            daily_strategy_profile_version_id=values["daily_strategy_profile_version_id"],
            execution_policy_version_id=values["execution_policy_version_id"],
            execution_policy_sha256=values["execution_policy_sha256"],
            tail_policy_version_id=values["tail_policy_version_id"],
            tail_policy_sha256=values["tail_policy_sha256"],
            release_config_json=values.get("release_config_json") or {},
            release_hash=values["release_hash"],
            validation_state=values["validation_state"],
            validation_evidence=values.get("validation_evidence") or {},
            effective_from=values.get("effective_from"),
            effective_to=values.get("effective_to"),
            created_by=values.get("created_by"),
            created_reason=values.get("created_reason"),
            created_at=values["created_at"],
            updated_at=values["updated_at"],
        )

    @staticmethod
    def _select_binding(cur: Any, binding_id: str, *, for_update: bool = False) -> SimulationReleaseBinding:
        suffix = " FOR UPDATE" if for_update else ""
        cur.execute(
            f"SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id = %s{suffix}",
            (binding_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("simulation release binding does not exist", context={"binding_id": binding_id})
        values = dict(row)
        return SimulationReleaseBinding(
            binding_id=values["binding_id"],
            strategy_id=values["strategy_id"],
            release_id=values["release_id"],
            release_hash=values["release_hash"],
            package_id=values["package_id"],
            manifest_sha256=values["manifest_sha256"],
            broker_backend=values["broker_backend"],
            broker_account_id=values.get("broker_account_id"),
            account_group_id=values.get("account_group_id"),
            strategy_slot_id=values.get("strategy_slot_id"),
            capital_allocation=float(values["capital_allocation"]),
            strategy_name=values.get("strategy_name"),
            order_remark_prefix=values.get("order_remark_prefix"),
            effective_from=values.get("effective_from"),
            effective_to=values.get("effective_to"),
            approval_state=values["approval_state"],
            binding_config_json=values.get("binding_config_json") or {},
            binding_hash=values["binding_hash"],
            created_by=values.get("created_by"),
            created_reason=values.get("created_reason"),
            created_at=values["created_at"],
            updated_at=values["updated_at"],
        )

    @classmethod
    def _select_replay(cls, cur: Any, replay_job_id: str, *, for_update: bool = False) -> LocalSimReplayJobV1:
        suffix = " FOR UPDATE" if for_update else ""
        cur.execute(
            f"SELECT * FROM paper_v2.localsim_replay_job_v1 WHERE replay_job_id = %s{suffix}",
            (replay_job_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("LocalSIM replay job does not exist", context={"replay_job_id": replay_job_id})
        return cls._replay_from_row(dict(row))

    @staticmethod
    def _account_from_row(row: dict[str, Any]) -> SimulationAccountV1:
        return SimulationAccountV1(
            schema_version=row["schema_version"],
            account_id=row["account_id"],
            account_hash=row["account_hash"],
            account_name=row["account_name"],
            broker_backend=row["broker_backend"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            admission_receipt_id=row["admission_receipt_id"],
            initial_capital=float(row["initial_capital"]),
            lineage_source_legacy_account_id=(row.get("account_config_json") or {}).get(
                "lineage_source_legacy_account_id"
            ),
            account_config_json=row.get("account_config_json") or {},
            status=row["status"],
            version=int(row["version"]),
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _lineage_from_row(row: dict[str, Any]) -> LegacyLocalSimAccountLineageV1:
        return LegacyLocalSimAccountLineageV1(
            schema_version=row["schema_version"],
            lineage_id=row["lineage_id"],
            lineage_hash=row["lineage_hash"],
            legacy_account_id=row["legacy_account_id"],
            account_id=row["account_id"],
            release_id=row["release_id"],
            binding_id=row["binding_id"],
            ledger_scope_id=row["ledger_scope_id"],
            economic_facts_sha256=row["economic_facts_sha256"],
            status=row["status"],
            version=int(row["version"]),
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _replay_from_row(row: dict[str, Any]) -> LocalSimReplayJobV1:
        return LocalSimReplayJobV1(**dict(row))

    @staticmethod
    def _replay_values(job: LocalSimReplayJobV1) -> tuple[Any, ...]:
        return (
            job.replay_job_id,
            job.replay_hash,
            job.simulation_account_id,
            job.release_id,
            job.binding_id,
            job.day_engine_contract_id,
            job.start_trade_date,
            job.end_trade_date,
            job.historical_source_id,
            job.historical_source_sha256,
            job.calendar_snapshot_sha256,
            job.status.value,
            job.next_trade_date,
            job.completed_trade_date,
            job.live_release_id,
            job.live_binding_id,
            job.activation_trade_date,
            job.version,
            job.failure_code,
            psycopg2.extras.Json(job.failure_context) if job.failure_context is not None else None,
            job.created_by,
            job.created_at,
            job.updated_at,
        )

    @staticmethod
    def _sql_value(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return psycopg2.extras.Json(value)
        return value

    @staticmethod
    def _require_same_identity(expected: Any, actual: Any, *, object_name: str) -> None:
        identity_fields = {
            "account": ("account_id", "account_hash", "account_config_json"),
            "release": ("release_id", "release_hash", "release_config_json"),
            "binding": ("binding_id", "binding_hash", "binding_config_json"),
            "lineage": ("lineage_id", "lineage_hash"),
            "replay_job": ("replay_job_id", "replay_hash"),
        }[object_name]
        if any(getattr(expected, field) != getattr(actual, field) for field in identity_fields):
            raise InvalidStateTransitionError(
                f"LocalSIM {object_name} transaction identity readback differs",
                context={"reason_code": f"LOCALSIM_{object_name.upper()}_IDENTITY_READBACK_MISMATCH"},
            )


class InMemoryLocalSimSuccessorRepository:
    """Rollback-capable in-memory implementation used by direct contract tests."""

    def __init__(self) -> None:
        self.accounts: dict[str, SimulationAccountV1] = {}
        self.account_hash_index: dict[str, str] = {}
        self.releases: dict[str, StrategyRuntimeRelease] = {}
        self.release_hash_index: dict[str, str] = {}
        self.bindings: dict[str, SimulationReleaseBinding] = {}
        self.binding_hash_index: dict[str, str] = {}
        self.lineages: dict[str, LegacyLocalSimAccountLineageV1] = {}
        self.legacy_lineage_index: dict[str, str] = {}
        self.replay_jobs: dict[str, LocalSimReplayJobV1] = {}
        self.replay_hash_index: dict[str, str] = {}

    def create_account_bundle(
        self,
        *,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[SimulationAccountV1, StrategyRuntimeRelease, SimulationReleaseBinding]:
        LocalSimSuccessorRepository._validate_new_account_binding(account=account, release=release, binding=binding)
        snapshot = deepcopy(self.__dict__)
        try:
            persisted_release = self._save_immutable(
                values=self.releases,
                hash_index=self.release_hash_index,
                object_id=release.release_id,
                object_hash=release.release_hash or "",
                value=release,
                object_name="release",
            )
            persisted_account = self._save_immutable(
                values=self.accounts,
                hash_index=self.account_hash_index,
                object_id=account.account_id,
                object_hash=account.account_hash,
                value=account,
                object_name="account",
            )
            persisted_binding = self._save_immutable(
                values=self.bindings,
                hash_index=self.binding_hash_index,
                object_id=binding.binding_id,
                object_hash=binding.binding_hash or "",
                value=binding,
                object_name="binding",
            )
            return persisted_account, persisted_release, persisted_binding
        except Exception:
            self.__dict__.update(snapshot)
            raise

    def get_account(self, account_id: str) -> SimulationAccountV1:
        try:
            return self.accounts[account_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "LocalSIM simulation account does not exist", context={"account_id": account_id}
            ) from exc

    def get_release(self, release_id: str) -> StrategyRuntimeRelease:
        try:
            return self.releases[release_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "strategy runtime release does not exist", context={"release_id": release_id}
            ) from exc

    def get_binding(self, binding_id: str) -> SimulationReleaseBinding:
        try:
            return self.bindings[binding_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "simulation release binding does not exist", context={"binding_id": binding_id}
            ) from exc

    def create_successor_binding(
        self,
        *,
        account: SimulationAccountV1,
        source_binding_id: str,
        expected_source_binding_hash: str,
        source_effective_to: date,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding]:
        LocalSimSuccessorRepository._validate_new_account_binding(account=account, release=release, binding=binding)
        persisted_account = self.get_account(account.account_id)
        LocalSimSuccessorRepository._require_same_identity(account, persisted_account, object_name="account")
        snapshot = deepcopy(self.__dict__)
        try:
            source = self.get_binding(source_binding_id)
            LocalSimSuccessorRepository._validate_successor_source(
                account=account,
                source=source,
                expected_source_binding_hash=expected_source_binding_hash,
                source_effective_to=source_effective_to,
            )
            if source.effective_to is None:
                self.bindings[source_binding_id] = source.model_copy(
                    update={"effective_to": source_effective_to, "updated_at": datetime.now(UTC)}
                )
            persisted_release = self._save_immutable(
                values=self.releases,
                hash_index=self.release_hash_index,
                object_id=release.release_id,
                object_hash=release.release_hash or "",
                value=release,
                object_name="release",
            )
            persisted_binding = self._save_immutable(
                values=self.bindings,
                hash_index=self.binding_hash_index,
                object_id=binding.binding_id,
                object_hash=binding.binding_hash or "",
                value=binding,
                object_name="binding",
            )
            return persisted_release, persisted_binding
        except Exception:
            self.__dict__.update(snapshot)
            raise

    def transition_account(
        self,
        *,
        account_id: str,
        expected_version: int,
        target_status: SimulationAccountStatus,
        updated_at: datetime,
    ) -> SimulationAccountV1:
        current = self.get_account(account_id)
        if current.version != expected_version:
            raise InvalidStateTransitionError(
                "LocalSIM account lifecycle CAS failed",
                context={"reason_code": "LOCALSIM_ACCOUNT_CAS_CONFLICT", "account_id": account_id},
            )
        updated = current.model_copy(
            update={"status": target_status, "version": current.version + 1, "updated_at": updated_at}
        )
        self.accounts[account_id] = updated
        return updated

    def create_lineage_bundle(
        self,
        *,
        account: SimulationAccountV1,
        lineage: LegacyLocalSimAccountLineageV1,
    ) -> tuple[SimulationAccountV1, LegacyLocalSimAccountLineageV1]:
        snapshot = deepcopy(self.__dict__)
        try:
            if lineage.release_id not in self.releases or lineage.binding_id not in self.bindings:
                raise DataUnavailableError(
                    "legacy LocalSIM lineage release or binding does not exist",
                    context={"reason_code": "LOCALSIM_LINEAGE_AUTHORITY_MISSING"},
                )
            binding = self.bindings[lineage.binding_id]
            if (
                binding.release_id != lineage.release_id
                or binding.broker_backend is not SimulationBrokerBackend.LOCAL_SIM
            ):
                raise InvalidStateTransitionError(
                    "legacy LocalSIM lineage release and binding authority is inconsistent",
                    context={"reason_code": "LOCALSIM_LINEAGE_AUTHORITY_MISMATCH"},
                )
            existing_id = self.legacy_lineage_index.get(lineage.legacy_account_id)
            if existing_id is not None and existing_id != lineage.lineage_id:
                raise InvalidStateTransitionError(
                    "legacy LocalSIM account already maps to another successor",
                    context={"reason_code": "LOCALSIM_LINEAGE_CONFLICT"},
                )
            persisted_account = self._save_immutable(
                values=self.accounts,
                hash_index=self.account_hash_index,
                object_id=account.account_id,
                object_hash=account.account_hash,
                value=account,
                object_name="account",
            )
            persisted_lineage = self._save_immutable(
                values=self.lineages,
                hash_index={},
                object_id=lineage.lineage_id,
                object_hash=lineage.lineage_hash,
                value=lineage,
                object_name="lineage",
            )
            self.legacy_lineage_index[lineage.legacy_account_id] = lineage.lineage_id
            return persisted_account, persisted_lineage
        except Exception:
            self.__dict__.update(snapshot)
            raise

    def get_lineage_by_legacy_account(self, legacy_account_id: str) -> LegacyLocalSimAccountLineageV1 | None:
        lineage_id = self.legacy_lineage_index.get(legacy_account_id)
        return self.lineages.get(lineage_id) if lineage_id is not None else None

    def transition_lineage(
        self,
        *,
        lineage_id: str,
        expected_version: int,
        target_status: LegacyLocalSimLineageStatus,
        updated_at: datetime,
    ) -> LegacyLocalSimAccountLineageV1:
        try:
            current = self.lineages[lineage_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "legacy LocalSIM lineage does not exist", context={"lineage_id": lineage_id}
            ) from exc
        if current.version != expected_version:
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage CAS failed",
                context={"reason_code": "LOCALSIM_LINEAGE_CAS_CONFLICT", "lineage_id": lineage_id},
            )
        updated = current.model_copy(
            update={"status": target_status, "version": current.version + 1, "updated_at": updated_at}
        )
        self.lineages[lineage_id] = updated
        return updated

    def save_replay_job(self, job: LocalSimReplayJobV1) -> LocalSimReplayJobV1:
        if job.simulation_account_id not in self.accounts:
            raise DataUnavailableError(
                "LocalSIM replay account does not exist", context={"account_id": job.simulation_account_id}
            )
        if job.release_id not in self.releases or job.binding_id not in self.bindings:
            raise DataUnavailableError(
                "LocalSIM replay release or binding does not exist", context={"replay_job_id": job.replay_job_id}
            )
        return self._save_immutable(
            values=self.replay_jobs,
            hash_index=self.replay_hash_index,
            object_id=job.replay_job_id,
            object_hash=job.replay_hash,
            value=job,
            object_name="replay_job",
        )

    def get_replay_job(self, replay_job_id: str) -> LocalSimReplayJobV1:
        try:
            return self.replay_jobs[replay_job_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "LocalSIM replay job does not exist", context={"replay_job_id": replay_job_id}
            ) from exc

    def transition_replay_job(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        update: dict[str, Any],
        updated_at: datetime,
    ) -> LocalSimReplayJobV1:
        current = self.get_replay_job(replay_job_id)
        if current.version != expected_version:
            raise InvalidStateTransitionError(
                "LocalSIM replay job CAS failed",
                context={"reason_code": "LOCALSIM_REPLAY_CAS_CONFLICT", "replay_job_id": replay_job_id},
            )
        updated = current.model_copy(update={**update, "version": current.version + 1, "updated_at": updated_at})
        updated = LocalSimReplayJobV1.model_validate(updated.model_dump())
        self.replay_jobs[replay_job_id] = updated
        return updated

    def create_replay_live_successor(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        account: SimulationAccountV1,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        activation_trade_date: date,
        updated_at: datetime,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding, LocalSimReplayJobV1]:
        LocalSimSuccessorRepository._validate_new_account_binding(account=account, release=release, binding=binding)
        current = self.get_replay_job(replay_job_id)
        if current.version != expected_version or current.status is not LocalSimReplayStatus.READY_FOR_LIVE:
            raise InvalidStateTransitionError(
                "LocalSIM replay is not ready for an atomic live successor",
                context={"reason_code": "LOCALSIM_REPLAY_LIVE_SUCCESSOR_STATE_INVALID"},
            )
        persisted_account = self.get_account(account.account_id)
        LocalSimSuccessorRepository._require_same_identity(account, persisted_account, object_name="account")
        snapshot = deepcopy(self.__dict__)
        try:
            persisted_release = self._save_immutable(
                values=self.releases,
                hash_index=self.release_hash_index,
                object_id=release.release_id,
                object_hash=release.release_hash or "",
                value=release,
                object_name="release",
            )
            persisted_binding = self._save_immutable(
                values=self.bindings,
                hash_index=self.binding_hash_index,
                object_id=binding.binding_id,
                object_hash=binding.binding_hash or "",
                value=binding,
                object_name="binding",
            )
            persisted_job = self.transition_replay_job(
                replay_job_id=replay_job_id,
                expected_version=expected_version,
                update={
                    "live_release_id": release.release_id,
                    "live_binding_id": binding.binding_id,
                    "activation_trade_date": activation_trade_date,
                    "status": LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY,
                },
                updated_at=updated_at,
            )
            return persisted_release, persisted_binding, persisted_job
        except Exception:
            self.__dict__.update(snapshot)
            raise

    @staticmethod
    def _save_immutable(
        *,
        values: dict[str, Any],
        hash_index: dict[str, str],
        object_id: str,
        object_hash: str,
        value: Any,
        object_name: str,
    ) -> Any:
        existing_id = hash_index.get(object_hash)
        if existing_id is not None:
            return values[existing_id]
        existing = values.get(object_id)
        if existing is not None:
            if existing.model_dump(mode="json") != value.model_dump(mode="json"):
                raise InvalidStateTransitionError(
                    f"LocalSIM {object_name} immutable identity conflicts",
                    context={"reason_code": f"LOCALSIM_{object_name.upper()}_IDENTITY_CONFLICT"},
                )
            return existing
        values[object_id] = value
        hash_index[object_hash] = object_id
        return value
