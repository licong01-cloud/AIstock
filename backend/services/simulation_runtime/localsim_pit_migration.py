"""Explicit LocalSIM runtime-policy migration, independent of model assets.

Planning only SELECTs. Applying locks the original account, bindings, jobs and
uncompleted days, rechecks the plan, then publishes one immutable successor in
the existing transaction. No package, market-data or economic fact is written.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from backend.db.pg_pool import get_conn
from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver, PitAuthorityStatus
from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_RUNTIME_LEASE_KEY,
    SelectionPitRuntimeLease,
    freeze_canonical_pit_runtime_binding,
    migrate_runtime_config_to_canonical_pointer,
)
from backend.services.simulation_signal.strategy_package_selection import StrategyPackageSelectionService
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError, TradingCoreError

from .localsim_file_inputs import FILE_BINDING_KEY, resolve_replay_source
from .localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator, local_sim_calendar_snapshot_sha256
from .localsim_replay_recovery import require_no_economic_effects, economic_fact_counts
from .models import SimulationBindingApprovalState, SimulationBrokerBackend, canonical_json_sha256
from .repository import InMemorySimulationRuntimeRepository
from .service import StrategyRuntimeReleaseService
from .successor_models import LocalSimReplayStatus
from .successor_repository import LocalSimSuccessorRepository


MIGRATION_KEY = "localsim_runtime_pit_migration_v1"
POLICY_KEY = "localsim_runtime_pit_policy_v1"


class LocalSimPitMigrationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def _fail(message: str, code: str) -> None:
    raise InvalidStateTransitionError(message, context={"reason_code": code})


def _policy_identity(lease: dict[str, Any]) -> dict[str, Any]:
    policy = {"schema_version": POLICY_KEY, **{key: lease[key] for key in (
        "authority_id", "universe_key", "rule_version", "rule_parameters_digest")}}
    return {**policy, "policy_sha256": canonical_json_sha256(policy)}


def require_migrated_selection_projection(source_config: dict[str, Any], metadata: dict[str, Any]) -> None:
    """Verify the only permitted overlay using the public pure PIT migration.

    Loading a frozen release never refreshes the authority or changes its
    policy lease. All non-PIT selection values must still equal the source
    profile's validated projection.
    """
    lease = SelectionPitRuntimeLease.from_mapping(metadata.get(CANONICAL_PIT_RUNTIME_LEASE_KEY))
    expected = migrate_runtime_config_to_canonical_pointer(source_config)
    expected[CANONICAL_PIT_RUNTIME_LEASE_KEY] = lease.as_dict()
    policy = _policy_identity(lease.as_dict())
    if metadata.get("selection_runtime_config") != expected or metadata.get(POLICY_KEY) != policy:
        raise RuntimeConfigInvalidError("LocalSIM effective policy differs beyond the explicit PIT migration",
            context={"reason_code": "LOCALSIM_PIT_MIGRATED_PROJECTION_MISMATCH"})


def freeze_pit_policy(config, *, start, end=None, resolver, expected_policy=None):
    config = migrate_runtime_config_to_canonical_pointer(config)
    config, lease = freeze_canonical_pit_runtime_binding(config, trade_date=start, authority_resolver=resolver)
    if end is not None:
        lease.require_trade_date(end)
    identity = lease.as_dict()
    policy = _policy_identity(identity)
    if expected_policy is not None and expected_policy != policy:
        _fail("LocalSIM PIT rules changed; explicit policy migration is required", "LOCALSIM_PIT_POLICY_MIGRATION_REQUIRED")
    return config, identity, policy


def refresh_migrated_daily_pit(metadata, *, trade_date, resolver=None):
    """Freeze a new business day, retaining the explicitly chosen rule set."""
    config, lease, policy = freeze_pit_policy(metadata["selection_runtime_config"], start=trade_date,
        resolver=resolver or CanonicalPitAuthorityResolver(), expected_policy=metadata[POLICY_KEY])
    result = deepcopy(metadata)
    result.update(selection_runtime_config=config, **{CANONICAL_PIT_RUNTIME_LEASE_KEY: lease, POLICY_KEY: policy})
    return result


def build_pit_successor(*, base, binding, start: date, end: date | None,
                        resolver, evidence: dict[str, Any], predecessor_id: str | None = None,
                        package_lifecycle_reader=None, explicit_migration=False, metadata_transform=None):
    """Keep the complete asset/execution identity; version only effective PIT.

    The original full runtime profile remains provenance for fees, HMM and
    execution. The separate effective selection policy is frozen in the new
    release and contributes to its identity and native inference cache key.
    """
    source_metadata = base.release_config_json.get("metadata") or {}
    from .localsim_input_source_plan import PLAN_KEY
    if PLAN_KEY in source_metadata and metadata_transform is None:
        _fail("PIT migration requires an explicit new source plan; the frozen plan cannot be rebound",
              "LOCALSIM_PIT_FROZEN_SOURCE_PLAN_REBIND_REQUIRED")
    config, identity, policy = freeze_pit_policy(
        StrategyPackageSelectionService.release_selection_runtime_config(base), start=start, end=end, resolver=resolver,
        expected_policy=None if explicit_migration else source_metadata.get(POLICY_KEY))
    # Full account/binding snapshots belong to the binding's audit receipt.
    # Releases carry only the immutable migration reference, respecting the
    # public release boundary between runtime policy and broker/account state.
    release_evidence = {key: deepcopy(evidence[key]) for key in (
        "schema_version", "plan_sha256", "origin_plan_sha256", "source_release_id", "source_release_hash")
        if key in evidence}
    metadata = deepcopy(base.release_config_json.get("metadata") or {})
    metadata.update({"selection_runtime_config": config, CANONICAL_PIT_RUNTIME_LEASE_KEY: identity,
                     POLICY_KEY: policy, MIGRATION_KEY: release_evidence})
    # A successor owns its own range and predecessor; never inherit a stale
    # catch-up lineage or historical source override into a live release.
    metadata.pop("localsim_replay_predecessor_id", None)
    metadata.pop("localsim_replay_range", None)
    if end is not None and predecessor_id is not None:
        metadata["localsim_replay_range"] = {"start_trade_date": start.isoformat(), "end_trade_date": end.isoformat()}
    if end is None:
        metadata.pop(FILE_BINDING_KEY, None)
    if predecessor_id is not None:
        metadata["localsim_replay_predecessor_id"] = predecessor_id
    if metadata_transform is not None:
        metadata = metadata_transform(metadata)
    service = StrategyRuntimeReleaseService(repository=InMemorySimulationRuntimeRepository(),
                                           package_lifecycle_reader=package_lifecycle_reader)
    fields = ("package_id", "manifest_sha256", "runtime_profile_id", "runtime_profile_version_id",
              "runtime_profile_sha256", "daily_strategy_profile_version_id", "execution_policy_version_id",
              "execution_policy_sha256", "tail_policy_version_id", "tail_policy_sha256")
    release = service.create_release(
        **{key: getattr(base, key) for key in fields}, base_release_id=base.release_id,
        execution_policy_json=base.release_config_json["execution_policy"]["policy_json"],
        validation_state=base.validation_state,
        validation_evidence={"schema_version": MIGRATION_KEY, "runtime_policy": policy,
                             "asset_manifest_sha256": base.manifest_sha256, "migration": release_evidence},
        release_metadata=metadata, effective_from=start, effective_to=end,
        created_by="localsim_pit_migration", created_reason="explicit_runtime_pit_policy_migration")
    result = service.create_binding(
        strategy_id=binding.strategy_id, release=release, broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        broker_account_id=binding.broker_account_id, account_group_id=binding.account_group_id,
        strategy_slot_id=binding.strategy_slot_id, capital_allocation=binding.capital_allocation,
        strategy_name=binding.strategy_name, order_remark_prefix=binding.order_remark_prefix,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        binding_metadata={**deepcopy(binding.binding_config_json.get("metadata") or {}),
                          MIGRATION_KEY: evidence}, effective_from=start, effective_to=end,
        created_by="localsim_pit_migration", created_reason="explicit_runtime_pit_policy_migration")
    return release, result


class LocalSimPitMigrationService:
    def __init__(self, *, conn_factory=None, source_authority=None, clock=None):
        self.conn_factory = conn_factory or (lambda: get_conn(autocommit=False, manage_transaction=True))
        if source_authority is None:
            from .localsim_product_control import LocalSimHistoricalSourceAuthority
            source_authority = LocalSimHistoricalSourceAuthority()
        self.source_authority = source_authority
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    @contextmanager
    def _shared(conn):
        yield conn

    def plan(self, account_id: str) -> dict[str, Any]:
        with self.conn_factory() as conn:
            return self._plan(conn, account_id)[0]

    def _plan(self, conn, account_id: str, *, lock=False):
        def factory():
            return self._shared(conn)

        repo = LocalSimSuccessorRepository(conn_factory=factory)
        resolver = CanonicalPitAuthorityResolver(connection_factory=factory)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            account = repo._select_account(cur, account_id, for_update=lock)
            scope = repo.resolve_ledger_scope_for_account(account_id)
            if account.status.value == "RETIRED":
                _fail("retired LocalSIM account cannot migrate", "LOCALSIM_PIT_ACCOUNT_RETIRED")
            # Legacy consumers keep their exact existing portfolio identity.
            strategy = scope.source_identity if scope.scope_kind.value == "LEGACY_PORTFOLIO" else account_id
            suffix = " FOR UPDATE" if lock else ""
            cur.execute("SELECT * FROM paper_v2.localsim_replay_job_v1 WHERE simulation_account_id=%s "
                        "ORDER BY created_at DESC, replay_job_id DESC" + suffix, (account_id,))
            jobs = [repo._replay_from_row(dict(row)) for row in cur.fetchall()]
            superseded = {(repo.get_release(j.release_id).release_config_json.get("metadata") or {}).get(
                "localsim_replay_predecessor_id") for j in jobs}
            current = [j for j in jobs if j.replay_job_id not in superseded and j.next_trade_date is not None
                       and j.status not in {LocalSimReplayStatus.CANCELLED, LocalSimReplayStatus.LIVE_ACTIVE}]
            if len(current) > 1:
                _fail("LocalSIM account has multiple uncompleted replay lineages", "LOCALSIM_PIT_REPLAY_FORK")
            job = current[0] if current else None
            cur.execute("SELECT * FROM paper_v2.simulation_release_binding WHERE strategy_id=%s "
                        "AND broker_backend='local_sim' ORDER BY effective_from DESC NULLS LAST, created_at DESC" + suffix,
                        (strategy,))
            bindings = [repo._binding_from_values(dict(row)) for row in cur.fetchall()]
            if not bindings:
                _fail("LocalSIM migration requires an authoritative existing binding", "LOCALSIM_PIT_BINDING_MISSING")
            live = [b for b in bindings if b.approval_state is not SimulationBindingApprovalState.RETIRED]
            binding = repo.get_binding(job.binding_id) if job else (live[0] if live else bindings[0])
            base = repo.get_release(binding.release_id)
            if (base.package_id, base.manifest_sha256) != (account.package_id, account.manifest_sha256):
                _fail("LocalSIM account and immutable assets differ", "LOCALSIM_PIT_ASSET_DRIFT")
            cur.execute("SELECT package_id,manifest_sha256,package_status FROM strategy_pkg.package WHERE package_id=%s",
                        (account.package_id,))
            package = cur.fetchone()
            if package is None or package["manifest_sha256"] != account.manifest_sha256:
                _fail("LocalSIM published package identity changed", "LOCALSIM_PIT_ASSET_DRIFT")
            if job:
                start, end = job.next_trade_date, job.end_trade_date
                source = resolve_replay_source(self.source_authority, job=job, release=base)
                if (source.historical_source_sha256 != job.historical_source_sha256
                        or local_sim_calendar_snapshot_sha256(source.trading_days) != job.calendar_snapshot_sha256):
                    _fail("LocalSIM frozen replay source changed", "LOCALSIM_PIT_SOURCE_DRIFT")
            else:
                # Use actual successful account days, never wall-clock guesses
                # or an intraday valuation as an authoritative completion.
                cur.execute("SELECT max(trade_date) AS completed FROM paper_v2.simulation_daily_run "
                            "WHERE strategy_id=%s AND status IN ('SUCCEEDED','NO_REBALANCE')", (strategy,))
                completed = cur.fetchone()["completed"]
                if completed is None:
                    _fail("LocalSIM daily migration has no authoritative completed day", "LOCALSIM_PIT_COMPLETED_DAY_MISSING")
                today = self.clock().astimezone(ZoneInfo('Asia/Shanghai')).date()
                source = None
                days = [d for d in self.source_authority.calendar.list_trading_days(completed, today) if d > completed]
                if not days:
                    _fail("LocalSIM daily migration awaits the next legal trading day", "LOCALSIM_PIT_NEXT_DAY_NOT_READY")
                # Existing daily publication rolls forward one immutable day
                # at a time, refreshing the authority lease for that day.
                start, end = days[0], days[0]
            affected = [b for b in live if b.effective_to is None or b.effective_to >= start]
            cur.execute("SELECT * FROM paper_v2.simulation_daily_run WHERE strategy_id=%s AND trade_date >= %s "
                        "ORDER BY run_id" + suffix, (strategy, start))
            runs = [dict(row) for row in cur.fetchall()]
            effects = []
            blockers = []
            for run in runs:
                if lock:
                    cur.execute("SELECT pg_try_advisory_xact_lock(2403, hashtext(%s)) AS locked", (run["run_id"],))
                    if not cur.fetchone()["locked"]:
                        _fail("LocalSIM day execution is active", "LOCALSIM_PIT_DAY_LOCKED")
                counts = economic_fact_counts(cur, run["run_id"])
                payload = run.get("run_payload_json") or {}
                try:
                    require_no_economic_effects(payload, counts, now=self.clock())
                    if run["status"] not in {"FAILED_RETRYABLE", "FAILED_TERMINAL", "CANCELLED"}:
                        _fail("LocalSIM uncompleted day is still in flight", "LOCALSIM_PIT_DAY_IN_FLIGHT")
                except InvalidStateTransitionError as exc:
                    blockers.append({"run_id": run["run_id"], "reason": str(exc), "context": exc.context})
                effects.append({"run_id": run["run_id"], "status": run["status"], "counts": counts,
                                "payload_sha256": canonical_json_sha256(payload)})
            if job and job.status not in {LocalSimReplayStatus.CREATED, LocalSimReplayStatus.FAILED_RETRYABLE,
                                          LocalSimReplayStatus.FAILED_TERMINAL}:
                blockers.append({"reason": "replay is still in flight", "status": job.status.value})
            authority = resolver.resolve_live_binding()
            if authority.authority_status is not PitAuthorityStatus.ACTIVE_CANONICAL:
                blockers.append({"reason": "current PIT authority is not ACTIVE_CANONICAL"})
            identity = {"account_id": account_id, "account_hash": account.account_hash,
                        "account_version": account.version, "ledger_scope": scope.model_dump(mode="json"),
                        "base_release_id": base.release_id, "base_release_hash": base.release_hash,
                        "source_binding_id": binding.binding_id, "source_binding_hash": binding.binding_hash,
                        "package_id": account.package_id, "manifest_sha256": account.manifest_sha256,
                        "predecessor": job.model_dump(mode="json") if job else None,
                        "effective_from": start.isoformat(), "effective_to": end.isoformat() if end else None,
                        "affected_bindings": [b.model_dump(mode="json") for b in affected],
                        "uncompleted_days": effects,
                        "authority": TypeAdapter(type(authority)).dump_python(authority, mode="json")}
            # timestamps are persisted identities, never generated probe times.
            digest = canonical_json_sha256(identity)
            evidence = {"schema_version": MIGRATION_KEY, "plan_sha256": digest,
                        "source_release_id": base.release_id, "source_release_hash": base.release_hash, **identity}
            built = None
            try:
                # One immutable resolver observation is used throughout this
                # plan; never mix two authority generations in one receipt.
                built = build_pit_successor(base=base, binding=binding, start=start, end=end,
                    resolver=SimpleNamespace(resolve_live_binding=lambda: authority), evidence=evidence,
                    predecessor_id=job.replay_job_id if job else None,
                    package_lifecycle_reader=SimpleNamespace(get=lambda _: SimpleNamespace(**package)),
                    explicit_migration=True)
                cutoff = self.source_authority.calendar.latest_trading_day_on_or_before(start - timedelta(days=1))
                SelectionPitRuntimeLease.from_mapping(
                    built[0].release_config_json["metadata"][CANONICAL_PIT_RUNTIME_LEASE_KEY]).require_trade_date(cutoff)
            except (ValueError, TradingCoreError) as exc:
                blockers.append({"reason": str(exc), "context": getattr(exc, "context", None),
                                 "cause": str(exc.__cause__) if exc.__cause__ else None})
            plan = {"schema_version": MIGRATION_KEY, "ready": not blockers, "blockers": blockers,
                    "plan_sha256": digest, "identity": identity,
                    "asset_changes": False, "economic_writes": False,
                    "policy": built[0].release_config_json["metadata"][POLICY_KEY] if built else None}
            return plan, (repo, account, binding, job, affected, source, built)

    def apply(self, account_id: str, request: LocalSimPitMigrationRequest) -> dict[str, Any]:
        with self.conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT pg_try_advisory_xact_lock(2408, hashtext(%s)) AS locked", (account_id,))
                if not cur.fetchone()["locked"]:
                    _fail("LocalSIM PIT migration is already running", "LOCALSIM_PIT_MIGRATION_LOCKED")
                # Idempotency precedes current-state CAS: the successor may have
                # legitimately progressed since this exact plan was committed.
                cur.execute("SELECT release_id, binding_id FROM paper_v2.simulation_release_binding "
                            "WHERE created_by='localsim_pit_migration' "
                            "AND binding_config_json->'metadata'->%s->>'plan_sha256'=%s "
                            "AND binding_config_json->'metadata'->%s->>'account_id'=%s",
                            (MIGRATION_KEY, request.plan_sha256, MIGRATION_KEY, account_id))
                duplicate = cur.fetchall()
                if duplicate:
                    if len(duplicate) != 1:
                        _fail("LocalSIM migration lineage fork", "LOCALSIM_PIT_MIGRATION_FORK")
                    return {"status": "ALREADY_APPLIED", "release_id": duplicate[0]["release_id"],
                            "binding_id": duplicate[0]["binding_id"], "account_id": account_id,
                            "plan_sha256": request.plan_sha256}
                plan, context = self._plan(conn, account_id, lock=True)
                if plan["plan_sha256"] != request.plan_sha256:
                    _fail("LocalSIM PIT migration plan is stale; replan", "LOCALSIM_PIT_MIGRATION_CAS_CONFLICT")
                if not plan["ready"]:
                    _fail("LocalSIM PIT migration preflight is blocked", "LOCALSIM_PIT_MIGRATION_BLOCKED")
                repo, account, binding, job, affected, source, built = context
                release, successor = built
                # The same per-day lock is used by native replay recovery and
                # economic execution. Locks remain held through the commit.
                for day in plan["identity"]["uncompleted_days"]:
                    cur.execute("SELECT pg_try_advisory_xact_lock(2403, hashtext(%s)) AS locked", (day["run_id"],))
                    if not cur.fetchone()["locked"]:
                        _fail("LocalSIM day execution is active", "LOCALSIM_PIT_DAY_LOCKED")
                repo._insert_release(cur, release)
                repo._insert_binding(cur, successor)
                next_job = None
                if job:
                    coordinator = LocalSimReplayCoordinator(repository=repo,
                        historical_day_runner=LocalSimHistoricalDayRunner(
                            historical_source_id=source.historical_source_id,
                            historical_source_sha256=source.historical_source_sha256, run_day=lambda *_: None))
                    next_job = coordinator.build_job(account=account, release=release, binding=successor,
                        start_trade_date=job.next_trade_date, end_trade_date=job.end_trade_date,
                        historical_source_id=source.historical_source_id,
                        historical_source_sha256=source.historical_source_sha256,
                        trading_days=source.trading_days, created_by="localsim_pit_migration")
                    repo._insert_replay(cur, next_job)
                    # The successor's native predecessor link seals this job.
                    # Bump CAS to invalidate any previously sampled worker,
                    # preserving the original status, failure and cursors.
                    cur.execute("UPDATE paper_v2.localsim_replay_job_v1 SET version=version+1, "
                                "updated_at=%s WHERE replay_job_id=%s AND version=%s",
                                (self.clock(), job.replay_job_id, job.version))
                    if cur.rowcount != 1:
                        _fail("LocalSIM predecessor changed during migration", "LOCALSIM_PIT_MIGRATION_CAS_CONFLICT")
                start = date.fromisoformat(plan["identity"]["effective_from"])
                for old in affected:
                    # Preserve all historical identities and completed days.
                    # No invalid inverted window for a failed one-day binding.
                    cutoff = start - timedelta(days=1) if old.effective_from and old.effective_from < start else old.effective_to
                    cur.execute("UPDATE paper_v2.simulation_release_binding SET approval_state='RETIRED', "
                                "effective_to=%s, updated_at=%s WHERE binding_id=%s AND binding_hash=%s",
                                (cutoff, self.clock(), old.binding_id, old.binding_hash))
                    if cur.rowcount != 1:
                        _fail("LocalSIM source binding changed during migration", "LOCALSIM_PIT_MIGRATION_CAS_CONFLICT")
                stored_release = repo._select_release(cur, release.release_id)
                stored_binding = repo._select_binding(cur, successor.binding_id)
                repo._require_same_identity(release, stored_release, object_name="release")
                repo._require_same_identity(successor, stored_binding, object_name="binding")
                return {"status": "APPLIED", "plan_sha256": request.plan_sha256,
                        "account_id": account.account_id, "package_id": account.package_id,
                        "manifest_sha256": account.manifest_sha256, "release_id": release.release_id,
                        "binding_id": successor.binding_id,
                        "replay_job_id": next_job.replay_job_id if next_job else None,
                        "effective_from": start.isoformat(), "policy": plan["policy"],
                        "asset_changes": False, "economic_writes": False}
