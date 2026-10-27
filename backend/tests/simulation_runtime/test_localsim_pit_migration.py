from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, date, datetime
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_RUNTIME_LEASE_KEY, CanonicalPitRuntimeError,
)
from backend.services.simulation_runtime import localsim_pit_migration as migration
from backend.services.simulation_runtime.localsim_pit_migration import (
    MIGRATION_KEY, POLICY_KEY, LocalSimPitMigrationRequest, LocalSimPitMigrationService,
    build_pit_successor,
)
from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimReplayLifecycleOwner
from backend.services.simulation_runtime.localsim_native_context import frozen_localsim_profile
from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.tests.simulation_runtime.test_localsim_native_context import _canonical_resolver
from backend.tests.simulation_runtime.test_localsim_product_control_plane import _account_request, _service


def _old_bundle():
    service, repository, _ = _service(canonical=False)
    # The generic product-control fake uses placeholder hashes. Materialize a
    # real source profile here so runtime loading tests exercise its strict
    # persisted identity checks as well as the policy overlay.
    from backend.services.simulation_runtime.localsim_runtime_profile import LocalSimRuntimeProfileConfigV1
    from backend.services.simulation_runtime.models import canonical_json_sha256
    resolve = service.authority.resolve_product
    def validated_source(**kwargs):
        product = resolve(**kwargs)
        profile = LocalSimRuntimeProfileConfigV1.model_validate(product.runtime_profile_version.config_json)
        product.runtime_profile_version.config_json = profile.model_dump(mode="json")
        product.runtime_profile_version.config_sha256 = canonical_json_sha256(profile.model_dump(mode="json"))
        product.runtime_profile_version.daily_strategy_profile_version_id = "lsdaily_" + canonical_json_sha256(
            profile.daily_strategy.model_dump(mode="json"))[:16]
        return product
    service.authority.resolve_product = validated_source
    result = service.create_account(_account_request(), created_by="test_pit_migration")
    return repository, SimpleNamespace(release_id=result.release.release_id,
        binding_id=result.binding.binding_id, account_id=result.account.account_id)


def test_policy_migration_keeps_immutable_assets_and_complete_execution_configuration():
    repository, original = _old_bundle()
    base = repository.get_release(original.release_id)
    binding = repository.get_binding(original.binding_id)
    snapshot = base.model_dump(mode="json")
    evidence = {"schema_version": MIGRATION_KEY, "plan_sha256": "e" * 64,
                "account_id": original.account_id, "affected_bindings": [binding.model_dump(mode="json")]}
    release, successor = build_pit_successor(base=base, binding=binding, start=date(2026, 9, 1),
        end=date(2026, 9, 2), resolver=_canonical_resolver(), evidence=evidence, predecessor_id="lsreplay_old")
    for field in ("package_id", "manifest_sha256", "runtime_profile_id", "runtime_profile_version_id",
                  "runtime_profile_sha256", "daily_strategy_profile_version_id", "execution_policy_version_id",
                  "execution_policy_sha256", "tail_policy_version_id", "tail_policy_sha256"):
        assert getattr(release, field) == getattr(base, field)
    for key in ("daily_strategy", "runtime_profile", "execution_policy", "tail_policy"):
        assert release.release_config_json[key] == base.release_config_json[key]
    metadata = release.release_config_json["metadata"]
    assert metadata[POLICY_KEY]["rule_version"] == "shsz_a_252td_st_delist_asof_v2"
    assert metadata["selection_runtime_config"][CANONICAL_PIT_RUNTIME_LEASE_KEY] == metadata[CANONICAL_PIT_RUNTIME_LEASE_KEY]
    assert "account_id" not in metadata[MIGRATION_KEY]
    assert successor.binding_config_json["metadata"][MIGRATION_KEY] == evidence
    assert successor.strategy_id == binding.strategy_id
    assert successor.broker_account_id == binding.broker_account_id
    assert successor.capital_allocation == binding.capital_allocation
    assert base.model_dump(mode="json") == snapshot
    assert release.release_hash != base.release_hash
    assert release.base_release_id == base.release_id
    assert frozen_localsim_profile(release) == frozen_localsim_profile(base)


@pytest.mark.parametrize("tamper", ["top_k", "policy", "missing_lease"])
def test_effective_policy_does_not_relax_source_profile_or_selection_identity(tamper):
    repository, original = _old_bundle()
    release, _ = build_pit_successor(base=repository.get_release(original.release_id),
        binding=repository.get_binding(original.binding_id), start=date(2026, 9, 1), end=None,
        resolver=_canonical_resolver(), evidence={"plan_sha256": "a" * 64})
    config = deepcopy(release.release_config_json)
    metadata = config["metadata"]
    if tamper == "top_k":
        metadata["selection_runtime_config"]["runtime_profile"]["selection"]["top_k"] += 1
    elif tamper == "policy":
        metadata[POLICY_KEY]["rule_version"] = "pretend_compatible"
    else:
        metadata.pop(CANONICAL_PIT_RUNTIME_LEASE_KEY)
    with pytest.raises((RuntimeConfigInvalidError, CanonicalPitRuntimeError)):
        frozen_localsim_profile(release.model_copy(update={"release_config_json": config}))


@pytest.mark.parametrize("start,end", [(date(2026, 10, 20), None),
                                        (date(2026, 10, 19), date(2026, 10, 20))])
def test_policy_migration_does_not_extend_canonical_coverage(start, end):
    repository, original = _old_bundle()
    with pytest.raises(CanonicalPitRuntimeError):
        build_pit_successor(base=repository.get_release(original.release_id),
            binding=repository.get_binding(original.binding_id), start=start, end=end,
            resolver=_canonical_resolver(), evidence={"plan_sha256": "a" * 64})


def test_catchup_and_live_keep_explicit_policy_instead_of_resolving_old_profile():
    repository, original = _old_bundle()
    base, binding = repository.get_release(original.release_id), repository.get_binding(original.binding_id)
    release, binding = build_pit_successor(base=base, binding=binding, start=date(2026, 9, 1),
        end=date(2026, 9, 2), resolver=_canonical_resolver(), evidence={"plan_sha256": "e" * 64})
    owner = object.__new__(LocalSimReplayLifecycleOwner)
    owner.control = SimpleNamespace(pit_authority_resolver=_canonical_resolver())
    owner.product_authority = SimpleNamespace(resolve_product=lambda **_: pytest.fail("old policy re-resolved"))
    for end in (date(2026, 9, 4), None):
        previous = release
        owner.repository = SimpleNamespace(get_account=lambda _: SimpleNamespace(account_id=original.account_id),
            get_release=lambda _: previous, get_binding=lambda _: binding)
        job = SimpleNamespace(simulation_account_id=original.account_id, release_id=previous.release_id,
                              binding_id=binding.binding_id, replay_job_id="lsreplay_previous")
        _, _, release, binding = owner._build_successor_bundle(job=job, effective_from=date(2026, 9, 3), effective_to=end)
        metadata = release.release_config_json["metadata"]
        assert metadata[POLICY_KEY]["rule_version"] == "shsz_a_252td_st_delist_asof_v2"
        assert metadata[MIGRATION_KEY]["origin_plan_sha256"] == "e" * 64
        assert release.manifest_sha256 == base.manifest_sha256
        assert ("localsim_replay_predecessor_id" in metadata) is (end is not None)
        assert ("localsim_replay_range" in metadata) is (end is not None)


def test_daily_roll_forward_freezes_new_day_but_never_silently_changes_pit_rules(monkeypatch):
    from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
    repository, original = _old_bundle()
    release, binding = build_pit_successor(base=repository.get_release(original.release_id),
        binding=repository.get_binding(original.binding_id), start=date(2026, 10, 19), end=date(2026, 10, 19),
        resolver=_canonical_resolver(), evidence={"plan_sha256": "e" * 64})
    original_json = deepcopy(release.release_config_json)
    assert "localsim_replay_range" not in release.release_config_json["metadata"]
    monkeypatch.setattr(migration, "CanonicalPitAuthorityResolver",
                        lambda: _canonical_resolver(coverage_end=date(2026, 10, 20)))
    result = SimulationLifecycleScheduler._roll_forward_release_metadata(source_release=release,
        source_binding=binding, trade_date=date(2026, 10, 20), authoritative_manifest_sha256=release.manifest_sha256)
    assert result[CANONICAL_PIT_RUNTIME_LEASE_KEY]["coverage_end"] == "2026-10-20"
    assert result[POLICY_KEY] == original_json["metadata"][POLICY_KEY]
    assert release.release_config_json == original_json
    previous_policy_json = deepcopy(release.release_config_json)
    previous_policy_json["metadata"][POLICY_KEY]["rule_version"] = "previous_policy_version"
    release = release.model_copy(update={"release_config_json": previous_policy_json})
    with pytest.raises(InvalidStateTransitionError, match="explicit policy migration"):
        SimulationLifecycleScheduler._roll_forward_release_metadata(source_release=release,
            source_binding=binding, trade_date=date(2026, 10, 20), authoritative_manifest_sha256=release.manifest_sha256)


def test_native_canonical_daily_renewal_retains_full_frozen_profile(monkeypatch):
    from backend.tests.simulation_runtime.test_localsim_native_context import _bundle, _canonical_profile
    from backend.services.selection_center.canonical_pit_runtime import require_canonical_pit_runtime_binding

    _, _, profile, _, _, release, _ = _bundle(_canonical_profile(), pit_authority_resolver=_canonical_resolver())
    original = deepcopy(release.release_config_json)
    old_metadata = original['metadata']
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver',
                        lambda: _canonical_resolver(coverage_end=date(2026, 10, 20)))
    new_metadata = migration.refresh_daily_pit(old_metadata, trade_date=date(2026, 10, 20))
    assert POLICY_KEY not in new_metadata
    assert require_canonical_pit_runtime_binding(new_metadata['selection_runtime_config'],
        trade_date=date(2026, 10, 20)).coverage_end == date(2026, 10, 20)
    with pytest.raises(CanonicalPitRuntimeError):
        require_canonical_pit_runtime_binding(old_metadata['selection_runtime_config'], trade_date=date(2026, 10, 20))
    renewed = release.model_copy(update={'release_config_json': {**original, 'metadata': new_metadata}})
    assert frozen_localsim_profile(renewed) == profile
    assert release.release_config_json == original


@pytest.mark.parametrize("changes", [{"coverage_start": date(2026, 8, 28)},
                                      {"coverage_end": date(2026, 8, 27)}])
def test_existing_dev_c_and_d_coverage_fail_closed(dev_migration, changes):
    from dataclasses import replace
    f = dev_migration
    f.resolver.binding = replace(f.resolver.binding, **changes)
    plan = f.service.plan(f.account.account_id)
    assert not plan["ready"]
    with pytest.raises(InvalidStateTransitionError, match="blocked"):
        f.service.apply(f.account.account_id, LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"]))
    assert f.repo.get_replay_job(f.job.replay_job_id) == f.job


def test_pit_migration_api_is_explicit_and_get_does_not_apply():
    from backend.routers import simulation_runtime as router
    calls = []
    app = FastAPI()
    app.include_router(router.router, prefix="/api/v1")
    service = SimpleNamespace(plan=lambda account: {"account": account, "ready": False},
                              apply=lambda account, request: calls.append((account, request.plan_sha256)) or {"status": "APPLIED"})
    app.dependency_overrides[router.get_localsim_pit_migration_service] = lambda: service
    with TestClient(app) as client:
        path = "/api/v1/simulation-runtime/localsim/accounts/existing/pit-migration"
        assert client.get(path).json() == {"account": "existing", "ready": False}
        assert calls == []
        assert client.post(path, json={"plan_sha256": "wrong"}).status_code == 422
        assert client.post(path, json={"plan_sha256": "a" * 64, "reset": True}).status_code == 422
        assert calls == []
        assert client.post(path, json={"plan_sha256": "a" * 64}).json() == {"status": "APPLIED"}
        assert calls == [("existing", "a" * 64)]


@pytest.mark.parametrize("extra", [["--apply"], ["--apply", "--in-process", "--plan-receipt", "unused.json"]])
def test_operator_cannot_apply_without_receipt_or_through_unloaded_inprocess_code(tmp_path, extra):
    from backend.services.simulation_runtime.localsim_pit_operator import main
    with pytest.raises(SystemExit) as caught:
        main(["--account-id", "existing", "--output", str(tmp_path / "receipt.json"), *extra])
    assert caught.value.code == 2
    assert not (tmp_path / "receipt.json").exists()


def _failed_daily_run(f, *, payload=None):
    from backend.services.simulation_runtime.models import SimulationDailyRun, SimulationDailyRunStatus
    from psycopg2.extras import Json
    run = SimulationDailyRun(run_id="test_int_pit_failed_" + uuid4().hex, trade_date=f.job.next_trade_date,
        strategy_id=f.account.account_id, broker_backend="local_sim", package_id=f.account.package_id,
        manifest_sha256=f.account.manifest_sha256, release_id=f.release.release_id,
        release_hash=f.release.release_hash, binding_id=f.binding.binding_id, binding_hash=f.binding.binding_hash,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE, run_payload_json=payload or {})
    # Seed only this rollback fixture directly. The writer API correctly owns
    # a transaction-scoped execution lock, which an outer rollback fixture
    # would otherwise retain while the second connection tests contention.
    with f.conn.cursor() as cur:
        cur.execute("INSERT INTO paper_v2.simulation_daily_run "
                    "(run_id,trade_date,strategy_id,broker_backend,package_id,manifest_sha256,release_id,"
                    "release_hash,binding_id,binding_hash,status,run_payload_json) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (run.run_id, run.trade_date, run.strategy_id, run.broker_backend.value,
                     run.package_id, run.manifest_sha256, run.release_id, run.release_hash,
                     run.binding_id, run.binding_hash, run.status.value, Json(run.run_payload_json)))
    return run


@pytest.fixture
def dev_migration(monkeypatch):
    """Exercise actual repository transactions on the existing DEV; rollback all rows."""
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("existing DEV integration is an explicit gate")
    from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn
    from backend.tests.simulation_runtime.test_localsim_successor_core_postgres import _package_identity, _RuntimeProfileAuthority
    from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
    from backend.services.simulation_runtime.localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator
    from backend.services.simulation_runtime.localsim_runtime_profile_repository import LocalSimRuntimeProfileRepository
    from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
    from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository
    from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot

    dsn = _dev_dsn()
    package, manifest = _package_identity(dsn)
    conn = psycopg2.connect(**dsn)
    marker = "test_int_pit_migration_" + uuid4().hex
    now = datetime(2026, 9, 2, tzinfo=UTC)
    days = (date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31), date(2026, 9, 1))

    @contextmanager
    def factory():
        savepoint = "migration_" + uuid4().hex
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {savepoint}")
        try:
            yield conn
        except Exception:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            raise
        finally:
            with conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")

    try:
        repository = LocalSimSuccessorRepository(conn_factory=factory)
        profiles = LocalSimRuntimeProfileService(repository=LocalSimRuntimeProfileRepository(conn_factory=factory),
            authority=_RuntimeProfileAuthority(), clock=lambda: now)
        profile = profiles.create_profile(package_id=package, manifest_sha256=manifest, profile_name=marker, created_by=marker)
        profile, version = profiles.create_version(profile_id=profile.profile_id, expected_profile_version=profile.version,
            config_json={"schema_version": "localsim_runtime_profile_config_request_v1",
                         "daily_strategy": {"strategy_id": "test_int_daily_strategy", "strategy_version": "v1", "top_k": 50},
                         "hmm": {"enabled": False}, "risk_policy": {"enabled": False}, "fee_policy": {}}, created_by=marker)
        policy = local_sim_twap_only_policy_snapshot()
        control = LocalSimControlPlaneService(repository=repository, clock=lambda: now)
        account, scope, release, binding = control.create_account(account_name=marker, package_id=package,
            manifest_sha256=manifest, admission_receipt_id=marker, initial_capital=5_000_000,
            effective_from=days[0], effective_to=days[2], runtime_profile_id=profile.profile_id,
            runtime_profile_version_id=version.profile_version_id, runtime_profile_sha256=version.config_sha256,
            daily_strategy_profile_version_id=version.daily_strategy_profile_version_id,
            execution_policy_version_id=policy["policy_version_id"], execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"], tail_policy_version_id=marker,
            tail_policy_sha256="d" * 64, created_by=marker)
        source = SimpleNamespace(historical_source_id="market.kline_minute_raw.v1", historical_source_sha256="c" * 64,
            trading_days=days[:3], current_trading_date=now.date(), latest_completed_trade_date=days[-1])
        coordinator = LocalSimReplayCoordinator(repository=repository, clock=lambda: now,
            historical_day_runner=LocalSimHistoricalDayRunner(historical_source_id=source.historical_source_id,
                historical_source_sha256=source.historical_source_sha256, run_day=lambda *_: None))
        job = coordinator.create_job(simulation_account_id=account.account_id, release_id=release.release_id,
            binding_id=binding.binding_id, start_trade_date=days[0], end_trade_date=days[2], trading_days=days[:3],
            historical_source_id=source.historical_source_id, historical_source_sha256=source.historical_source_sha256,
            created_by=marker)
        job = repository.transition_replay_job(replay_job_id=job.replay_job_id, expected_version=job.version,
            update={"status": LocalSimReplayStatus.FAILED_RETRYABLE, "completed_trade_date": days[0],
                    "next_trade_date": days[1], "failure_code": "CanonicalPitInferenceBoundaryError",
                    "failure_context": {"trade_date": days[1].isoformat(), "diagnostic": marker}}, updated_at=now)
        calendar = SimpleNamespace(list_trading_days=lambda start, end: tuple(d for d in days if start <= d <= end),
            latest_trading_day_on_or_before=lambda target: max(d for d in days if d <= target))
        resolver = _canonical_resolver()
        monkeypatch.setattr(migration, "CanonicalPitAuthorityResolver", lambda **_: resolver)
        service = LocalSimPitMigrationService(conn_factory=factory, clock=lambda: now,
            source_authority=SimpleNamespace(resolve=lambda **_: source, calendar=calendar))
        yield SimpleNamespace(conn=conn, factory=factory, repo=repository, service=service, account=account,
            scope=scope, release=release, binding=binding, job=job, dsn=dsn, resolver=resolver, now=now)
    finally:
        conn.rollback()
        conn.close()


def test_existing_dev_atomic_migration_preserves_prefix_scope_and_idempotency(dev_migration):
    f = dev_migration
    original_job = f.repo.get_replay_job(f.job.replay_job_id)
    original_account = f.repo.get_account(f.account.account_id)
    original_scope = f.repo.resolve_ledger_scope_for_account(f.account.account_id)
    plan = f.service.plan(f.account.account_id)
    assert plan["ready"], plan["blockers"]
    assert f.service.plan(f.account.account_id)["plan_sha256"] == plan["plan_sha256"]
    request = LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"])
    result = f.service.apply(f.account.account_id, request)
    assert result["status"] == "APPLIED"
    assert result["asset_changes"] is False and result["economic_writes"] is False
    old = f.repo.get_replay_job(f.job.replay_job_id)
    assert old.status is original_job.status
    assert old.version == original_job.version + 1
    for field in ("completed_trade_date", "next_trade_date", "failure_code", "failure_context", "release_id", "binding_id"):
        assert getattr(old, field) == getattr(original_job, field)
    successor = f.repo.get_replay_job(result["replay_job_id"])
    assert successor.start_trade_date == original_job.next_trade_date
    assert successor.next_trade_date == original_job.next_trade_date
    assert successor.end_trade_date == original_job.end_trade_date
    assert successor.simulation_account_id == original_job.simulation_account_id
    assert successor.historical_source_sha256 == original_job.historical_source_sha256
    assert f.repo.get_account(f.account.account_id) == original_account
    assert f.repo.resolve_ledger_scope_for_account(f.account.account_id) == original_scope
    stored = f.repo.get_binding(result["binding_id"]).binding_config_json["metadata"][MIGRATION_KEY]
    assert stored["predecessor"] == original_job.model_dump(mode="json")
    assert f.repo.get_binding(f.binding.binding_id).approval_state.value == "RETIRED"
    owner = object.__new__(LocalSimReplayLifecycleOwner)
    owner.repository = f.repo
    with pytest.raises(InvalidStateTransitionError, match="retired"):
        owner._advance(job=original_job, as_of_time=f.now)
    assert f.service.apply(f.account.account_id, request)["status"] == "ALREADY_APPLIED"
    f.repo.transition_replay_job(replay_job_id=successor.replay_job_id, expected_version=successor.version,
        update={"status": LocalSimReplayStatus.RUNNING_HISTORICAL}, updated_at=f.now)
    assert f.service.apply(f.account.account_id, request)["status"] == "ALREADY_APPLIED"


def test_existing_dev_stale_plan_and_partial_publication_roll_back(dev_migration, monkeypatch):
    f = dev_migration
    plan = f.service.plan(f.account.account_id)
    stale = LocalSimPitMigrationRequest(plan_sha256="0" * 64)
    with pytest.raises(InvalidStateTransitionError, match="stale"):
        f.service.apply(f.account.account_id, stale)
    before = f.repo.get_binding(f.binding.binding_id)
    insert = migration.LocalSimSuccessorRepository._insert_binding
    def fail_after_binding(self, cur, binding):
        insert(cur, binding)
        raise RuntimeError("injected failure after immutable successor publication")
    monkeypatch.setattr(migration.LocalSimSuccessorRepository, "_insert_binding", fail_after_binding)
    with pytest.raises(RuntimeError, match="injected"):
        f.service.apply(f.account.account_id, LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"]))
    assert f.repo.get_binding(f.binding.binding_id) == before
    assert f.repo.get_replay_job(f.job.replay_job_id) == f.job
    with f.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM paper_v2.simulation_release_binding WHERE binding_config_json->'metadata'->%s->>'plan_sha256'=%s",
                    (MIGRATION_KEY, plan["plan_sha256"]))
        assert cur.fetchone()[0] == 0


def test_existing_dev_inflight_replay_and_account_lock_block_migration(dev_migration):
    f = dev_migration
    request = LocalSimPitMigrationRequest(plan_sha256=f.service.plan(f.account.account_id)["plan_sha256"])
    with psycopg2.connect(**f.dsn) as other:
        with other.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(2408, hashtext(%s))", (f.account.account_id,))
            assert cur.fetchone()[0]
            try:
                with pytest.raises(InvalidStateTransitionError, match="already running"):
                    f.service.apply(f.account.account_id, request)
            finally:
                cur.execute("SELECT pg_advisory_unlock(2408, hashtext(%s))", (f.account.account_id,))
    f.repo.transition_replay_job(replay_job_id=f.job.replay_job_id, expected_version=f.job.version,
        update={"status": LocalSimReplayStatus.RUNNING_HISTORICAL, "failure_code": None,
                "failure_context": None}, updated_at=f.now)
    plan = f.service.plan(f.account.account_id)
    assert plan["ready"] is False
    with pytest.raises(InvalidStateTransitionError, match="blocked"):
        f.service.apply(f.account.account_id, LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"]))


@pytest.mark.parametrize("payload", [
    {"broker_called": True},
    {"failed_intents": 1, "broker_order_handles": [{"handle_id": "test_existing_handle"}]},
    {"simulation_scheduler_retry_claims_v1": {"claims": {"one": {"lease_until": "2099-01-01T00:00:00+00:00"}}}},
])
def test_existing_dev_uncompleted_economic_evidence_and_claims_are_not_discarded(dev_migration, payload):
    f = dev_migration
    run = _failed_daily_run(f, payload=payload)
    plan = f.service.plan(f.account.account_id)
    assert plan["ready"] is False
    assert any(blocker.get("run_id") == run.run_id for blocker in plan["blockers"])
    with pytest.raises(InvalidStateTransitionError, match="blocked"):
        f.service.apply(f.account.account_id, LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"]))
    assert f.repo.get_replay_job(f.job.replay_job_id) == f.job


def test_existing_dev_day_writer_lock_and_real_fact_count_block_migration(dev_migration):
    f = dev_migration
    run = _failed_daily_run(f)
    plan = f.service.plan(f.account.account_id)
    assert plan["ready"], plan["blockers"]
    request = LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"])
    with psycopg2.connect(**f.dsn) as other:
        with other.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(2403, hashtext(%s))", (run.run_id,))
            assert cur.fetchone()[0]
            try:
                with pytest.raises(InvalidStateTransitionError, match="execution is active"):
                    f.service.apply(f.account.account_id, request)
            finally:
                cur.execute("SELECT pg_advisory_unlock(2403, hashtext(%s))", (run.run_id,))
    # Use a real durable native run fact: the count is not mocked and a missing
    # projection or empty order list cannot disguise that economic identity.
    with f.conn.cursor() as cur:
        cur.execute("INSERT INTO paper_v2.run (run_id,portfolio_id,trade_date,status,data_source) "
                    "VALUES (%s,%s,%s,'FAILED','DB_HISTORICAL')",
                    (run.run_id, f.scope.ledger_scope_id, run.trade_date))
    plan = f.service.plan(f.account.account_id)
    assert not plan["ready"]
    assert plan["identity"]["uncompleted_days"][0]["counts"]["run"] == 1
    with pytest.raises(InvalidStateTransitionError, match="blocked"):
        f.service.apply(f.account.account_id, LocalSimPitMigrationRequest(plan_sha256=plan["plan_sha256"]))
