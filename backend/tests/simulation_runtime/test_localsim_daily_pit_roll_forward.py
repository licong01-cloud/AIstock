"""Daily PIT admissions use the shared authority and the existing scheduler."""
from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_RUNTIME_LEASE_KEY, SelectionPitRuntimeLease,
)
from backend.services.simulation_runtime import localsim_pit_migration as migration
from backend.services.simulation_runtime.localsim_native_context import selection_config_from_localsim_profile
from backend.services.simulation_runtime.models import SimulationBrokerBackend, SimulationDailyRunStatus
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler, StaticSimulationRunContextProvider
from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.tests.simulation_runtime.test_lifecycle_scheduler import (
    TRADE_DATE, FakeSelectionService, _candidate_rows, _position_context, _release_and_bindings,
)
from backend.tests.simulation_runtime.test_localsim_native_context import _canonical_profile, _canonical_resolver


def _scheduler(monkeypatch, *, migrated=False):
    import backend.services.simulation_runtime.lifecycle as lifecycle_module
    import backend.services.simulation_runtime.scheduler as scheduler_module

    now = datetime.combine(TRADE_DATE + timedelta(days=1), datetime.min.time(), ZoneInfo('Asia/Shanghai')).replace(hour=10)
    monkeypatch.setattr(lifecycle_module, 'scheduler_now', lambda: now)
    monkeypatch.setattr(scheduler_module, 'scheduler_now', lambda: now)
    release, binding, _, repository = _release_and_bindings()
    resolver = _canonical_resolver(coverage_end=TRADE_DATE)
    lease = SelectionPitRuntimeLease.from_binding(resolver.binding).as_dict()
    profile = _canonical_profile()
    config = deepcopy(release.release_config_json)
    metadata = config.setdefault('metadata', {})
    metadata.update({CANONICAL_PIT_RUNTIME_LEASE_KEY: lease,
        'selection_runtime_config': selection_config_from_localsim_profile(profile, pit_lease=lease),
        'localsim_runtime_profile_config': profile.model_dump(mode='json')})
    if migrated:
        metadata[migration.POLICY_KEY] = migration._policy_identity(lease)
    release = release.model_copy(update={'release_config_json': config})
    repository.releases[release.release_id] = release
    binding = binding.model_copy(update={'effective_from': TRADE_DATE, 'effective_to': None})
    repository.bindings[binding.binding_id] = binding
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver',
                        lambda: _canonical_resolver(coverage_end=TRADE_DATE + timedelta(days=5)))
    scheduler = SimulationLifecycleScheduler(repository=repository,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={binding.strategy_id: _position_context(portfolio_id='daily_pit_test')}))
    return scheduler, repository, release, binding


def _tick(scheduler, *, day=None, **kwargs):
    return scheduler.run_once(trade_date=day or TRADE_DATE + timedelta(days=1),
        data_source='DB_HISTORICAL', broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False, **kwargs)


@pytest.mark.parametrize('migrated', [False, True])
def test_open_live_binding_gets_one_same_policy_daily_release_and_retries_reuse_it(monkeypatch, migrated):
    scheduler, repo, original, binding = _scheduler(monkeypatch, migrated=migrated)
    saved = original.model_dump(mode='json')
    result = _tick(scheduler, limit=1)
    assert result.total_bindings == result.planned_count == 1
    renewed = repo.get_simulation_release_binding(result.results[0].run.binding_id)
    assert renewed.binding_id != binding.binding_id
    assert renewed.effective_from == renewed.effective_to == TRADE_DATE + timedelta(days=1)
    release = repo.get_strategy_runtime_release(renewed.release_id)
    metadata = release.release_config_json['metadata']
    assert metadata[CANONICAL_PIT_RUNTIME_LEASE_KEY]['coverage_end'] == str(TRADE_DATE + timedelta(days=5))
    assert (migration.POLICY_KEY in metadata) == migrated
    assert release.base_release_id == original.release_id
    assert (release.package_id, release.manifest_sha256) == (original.package_id, original.manifest_sha256)
    assert original.model_dump(mode='json') == saved
    assert repo.get_simulation_release_binding(binding.binding_id).effective_to is None
    again = _tick(scheduler, limit=1)
    assert again.reused_count == 1
    assert again.results[0].run.run_id == result.results[0].run.run_id
    assert len(repo.releases) == 2 and len(repo.bindings) == 3


def _failed_run(scheduler, repo, release, binding, **changes):
    day = TRADE_DATE + timedelta(days=1)
    run = scheduler._record_pre_run_binding_failure_result(binding=binding, trade_date=day,
        data_source='DB_HISTORICAL', created_by='test',
        exc=migration.RuntimeConfigInvalidError('expired lease', context={
            'reason_code': 'CANONICAL_PIT_RUNTIME_INVALID'})).run
    if changes:
        run = run.model_copy(update=changes)
        repo.daily_runs[run.run_id] = run
    return run


def test_pre_run_only_failure_has_audited_successor_and_no_duplicate_plan(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    failed = _failed_run(scheduler, repo, release, binding)
    result = _tick(scheduler)
    old = repo.get_simulation_daily_run(failed.run_id)
    assert old.status is SimulationDailyRunStatus.CANCELLED
    receipt = old.run_payload_json['localsim_daily_pit_supersession_v1']
    assert receipt['previous_status'] == 'FAILED_RETRYABLE'
    assert receipt['successor_binding_id'] == result.results[0].run.binding_id
    assert receipt['broker_called'] is False
    assert old.execution_plan_id is None and old.selection_evidence_id is None
    plans = set(repo.execution_plans)
    again = _tick(scheduler)
    assert again.reused_count == 1
    assert set(repo.execution_plans) == plans
    assert again.results[0].run.run_id == result.results[0].run.run_id


@pytest.mark.parametrize('changes', [
    {'status': SimulationDailyRunStatus.FAILED_TERMINAL},
    {'status': SimulationDailyRunStatus.SIGNAL_GENERATING},
    {'execution_plan_id': 'existing_plan'},
    {'selection_evidence_id': 'existing_scores'},
    {'run_payload_json': {'broker_called': True}},
    {'run_payload_json': {'broker_side_effect_state': 'UNKNOWN'}},
])
def test_existing_inflight_economic_or_terminal_run_is_never_rebound(monkeypatch, changes):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    old = _failed_run(scheduler, repo, release, binding, **changes)
    snapshot = old.model_dump(mode='json')
    result = scheduler._renew_unattended_localsim_pit_binding(
        binding=binding, trade_date=TRADE_DATE + timedelta(days=1))
    assert result.binding_id == binding.binding_id
    assert len(repo.releases) == 1
    assert repo.get_simulation_daily_run(old.run_id).model_dump(mode='json') == snapshot


def test_historical_explicit_release_and_frozen_plan_are_not_renewed(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    _tick(scheduler, release_id=release.release_id)
    assert len(repo.releases) == 1
    config = deepcopy(release.release_config_json)
    config['metadata']['localsim_task_input_source_plan_v1'] = {'frozen': True}
    release = release.model_copy(update={'release_config_json': config})
    repo.releases[release.release_id] = release
    assert scheduler._renew_unattended_localsim_pit_binding(
        binding=binding, trade_date=TRADE_DATE + timedelta(days=1)) == binding
    with pytest.raises(InvalidStateTransitionError, match='frozen replay'):
        migration.refresh_daily_pit(config['metadata'], trade_date=TRADE_DATE + timedelta(days=1))


def test_insufficient_coverage_or_changed_policy_fails_before_successor_publish(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver', lambda: _canonical_resolver(coverage_end=TRADE_DATE))
    failed = _tick(scheduler)
    assert failed.failed_count == 1
    assert len(repo.releases) == 1 and not repo.execution_plans
    config = deepcopy(release.release_config_json)
    config['metadata'][migration.POLICY_KEY] = {}
    repo.releases[release.release_id] = release.model_copy(update={'release_config_json': config})
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver',
                        lambda: _canonical_resolver(coverage_end=TRADE_DATE + timedelta(days=5)))
    with pytest.raises(InvalidStateTransitionError, match='explicit policy migration'):
        _tick(scheduler, raise_on_error=True)
    assert len(repo.releases) == 1 and not repo.execution_plans


def test_disabled_renewal_and_valid_saved_lease_preserve_existing_binding(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    assert scheduler._renew_unattended_localsim_pit_binding(binding=binding, trade_date=TRADE_DATE) == binding
    binding = binding.model_copy(update={'binding_config_json': {
        **binding.binding_config_json, 'metadata': {'disable_unattended_roll_forward': True}}})
    assert scheduler._renew_unattended_localsim_pit_binding(
        binding=binding, trade_date=TRADE_DATE + timedelta(days=1)) == binding
    assert len(repo.releases) == 1


def test_successor_publish_interruption_finishes_audit_on_next_tick(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    failed = _failed_run(scheduler, repo, release, binding)
    update = repo.update_simulation_daily_run
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupted after successor publish')
    monkeypatch.setattr(repo, 'update_simulation_daily_run', interrupted)
    with pytest.raises(RuntimeError, match='interrupted'):
        scheduler._renew_unattended_localsim_pit_binding(binding=binding, trade_date=TRADE_DATE + timedelta(days=1))
    assert len(repo.releases) == 2
    monkeypatch.setattr(repo, 'update_simulation_daily_run', update)
    result = _tick(scheduler)
    assert result.planned_count == 1
    assert repo.get_simulation_daily_run(failed.run_id).status is SimulationDailyRunStatus.CANCELLED
    assert len(repo.releases) == 2


def test_one_renewal_failure_does_not_starve_other_binding(monkeypatch):
    scheduler, repo, release, binding = _scheduler(monkeypatch)
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver', lambda: _canonical_resolver(coverage_end=TRADE_DATE))
    other = next(b for b in repo.bindings.values() if b.broker_backend is SimulationBrokerBackend.MINIQMT_SIM)
    skips, blocked = [], set()
    result = scheduler._with_unattended_roll_forward_bindings(
        bindings=[binding, other], trade_date=TRADE_DATE + timedelta(days=1), limit=2,
        broker_backend=None, strategy_id=None, release_id=None, approval_states=None,
        data_source='DB_HISTORICAL', package_status_cache={}, lifecycle_skips=skips,
        blocked_binding_keys=blocked, created_by='test', raise_on_error=False)
    assert result == [other]
    assert len(skips) == 1 and (binding.strategy_id, binding.broker_backend) in blocked
    assert len(repo.releases) == 1


def test_noncanonical_original_twap_binding_never_resolves_new_pit(monkeypatch):
    release, binding, _, repo = _release_and_bindings()
    scheduler = SimulationLifecycleScheduler(repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider())
    def forbidden():
        raise AssertionError('legacy TWAP must not opt in to canonical PIT')
    monkeypatch.setattr(migration, 'CanonicalPitAuthorityResolver', forbidden)
    assert scheduler._renew_unattended_localsim_pit_binding(
        binding=binding, trade_date=TRADE_DATE + timedelta(days=1)) == binding
    assert len(repo.releases) == 1


@pytest.mark.parametrize('offset', [0, 2])
def test_noncurrent_business_day_does_not_admit_new_live_lease(monkeypatch, offset):
    scheduler, repo, _, binding = _scheduler(monkeypatch)
    assert scheduler._renew_unattended_localsim_pit_binding(
        binding=binding, trade_date=TRADE_DATE + timedelta(days=offset)) == binding
    assert len(repo.releases) == 1
