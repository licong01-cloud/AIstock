"""Historical valuation routing uses frozen facts, not a new trading session."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_execution.localsim.models import LocalSimProjectionOutboxV1
from backend.services.simulation_runtime import scheduler as module
from backend.services.simulation_runtime.models import SimulationBrokerBackend, SimulationDailyRunStatus
from backend.tests.simulation_runtime.test_lifecycle_scheduler import (
    TRADE_DATE, FakeSelectionService, _candidate_rows, _position_context, _release_and_bindings,
)

TZ = ZoneInfo('Asia/Shanghai')


def test_historical_causal_cursor_does_not_select_realtime_for_legacy_account(monkeypatch):
    today = TRADE_DATE + timedelta(days=4)
    monkeypatch.setattr(module, 'scheduler_now', lambda: datetime.combine(today, datetime.min.time(), TZ))
    source = module.ProductionSimulationRunContextProvider._resolve_local_sim_market_data_source(
        portfolio=SimpleNamespace(data_source=MinuteDataSource.DB_HISTORICAL), trade_date=TRADE_DATE,
        as_of_time=datetime.combine(TRADE_DATE, datetime.min.time(), TZ).replace(hour=10))
    assert source is MinuteDataSource.DB_HISTORICAL


@pytest.mark.parametrize('source', [MinuteDataSource.DB_HISTORICAL, MinuteDataSource.DATASET_FILES])
def test_current_day_is_realtime_and_past_day_keeps_declared_source(monkeypatch, source):
    today = TRADE_DATE + timedelta(days=4)
    monkeypatch.setattr(module, 'scheduler_now', lambda: datetime.combine(today, datetime.min.time(), TZ))
    provider = module.ProductionSimulationRunContextProvider
    portfolio = SimpleNamespace(data_source=source)
    assert provider._resolve_local_sim_market_data_source(portfolio=portfolio, trade_date=today) is MinuteDataSource.TDX_REALTIME
    assert provider._resolve_local_sim_market_data_source(portfolio=portfolio, trade_date=TRADE_DATE) is source


def _pending(monkeypatch, *, terminal=True):
    release, binding, _, repo = _release_and_bindings()
    scheduler = module.SimulationLifecycleScheduler(repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=module.StaticSimulationRunContextProvider(
            by_binding_id={binding.binding_id: _position_context(portfolio_id='historical_valuation')}))
    result = scheduler.run_once(trade_date=TRADE_DATE, data_source='DB_HISTORICAL',
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False,
        as_of_time=datetime.combine(TRADE_DATE, datetime.min.time(), TZ).replace(hour=9, minute=22))
    run, plan = result.results[0].run, result.results[0].execution_plan
    frozen = datetime.combine(TRADE_DATE, datetime.min.time(), TZ).replace(hour=9, minute=48)
    outbox = LocalSimProjectionOutboxV1(run_id=run.run_id, plan_id=plan.plan_id, generation=1,
        receipt_id='routing_receipt', economic_hash='frozen_economics', projection_payload={
            'schema_version': 'local_sim_valuation_pending_projection_payload_v1',
            'projection_kind': 'VALUATION_PENDING', 'valuation_snapshot_time': frozen.isoformat(),
            'completed_persistence': {'terminal': terminal}})
    run = repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.FAILED_TERMINAL,
        payload_patch={'local_sim_projection_outbox_v1': outbox.model_dump(mode='json'),
                       'local_sim_persistence': {'status': 'INTRADAY_VALUATION_PENDING'}})
    today = TRADE_DATE + timedelta(days=4)
    monkeypatch.setattr(module, 'scheduler_now', lambda: datetime.combine(today, datetime.min.time(), TZ).replace(hour=10))
    states = tuple(SimpleNamespace(intent_id=i.intent_id, run_id=run.run_id, binding_id=binding.binding_id,
        plan_id=plan.plan_id, trade_date=TRADE_DATE, is_terminal=terminal) for i in plan.intents)
    monkeypatch.setattr(repo, 'list_local_sim_execution_states', lambda *a, **kw: states)
    return scheduler, repo, release, binding, run, plan, outbox, today, frozen


def test_historical_pending_valuation_routes_only_frozen_projection_without_a_new_plan(monkeypatch):
    scheduler, repo, release, binding, run, plan, outbox, today, frozen = _pending(monkeypatch)
    calls = []
    def recover(**kwargs):
        calls.append(kwargs)
        projected = outbox.model_copy(update={'status': module.LocalSimProjectionOutboxStatus.PROJECTED})
        updated = repo.update_simulation_daily_run(run.run_id, status=run.status,
            payload_patch={'local_sim_projection_outbox_v1': projected.model_dump(mode='json')})
        return SimpleNamespace(run=updated, status='LOCALSIM_PROJECTION_RECOVERED')
    monkeypatch.setattr(scheduler, '_recover_pending_local_sim_projection_if_needed', recover)
    before = (set(repo.execution_plans), set(repo.releases), set(repo.bindings))
    result = scheduler._terminalize_historical_localsim_durable_run_if_safe(
        run=run, scheduler_trade_date=today, as_of_time=None)
    assert result['historical_valuation_recovered'] is True
    assert len(calls) == 1 and calls[0]['as_of_time'] == frozen
    assert calls[0]['plan'] == plan and calls[0]['runtime_release'] == release
    assert (set(repo.execution_plans), set(repo.releases), set(repo.bindings)) == before
    assert repo.get_simulation_daily_run(run.run_id).status is SimulationDailyRunStatus.FAILED_TERMINAL
    assert scheduler._terminalize_stale_localsim_failed_runs(trade_date=today,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, strategy_id=None, limit=100,
        as_of_time=None, raise_on_error=True) == []


def test_historical_valuation_cannot_advance_active_economic_states(monkeypatch):
    scheduler, repo, release, binding, run, plan, outbox, today, frozen = _pending(monkeypatch, terminal=False)
    def forbidden(**kwargs):
        raise AssertionError('active execution cannot use valuation-only recovery')
    monkeypatch.setattr(scheduler, '_recover_pending_local_sim_projection_if_needed', forbidden)
    assert scheduler._terminalize_historical_localsim_durable_run_if_safe(
        run=run, scheduler_trade_date=today, as_of_time=None) is None


def test_terminal_flag_does_not_override_authoritative_active_state(monkeypatch):
    scheduler, repo, release, binding, run, plan, outbox, today, frozen = _pending(monkeypatch)
    states = repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    states[0].is_terminal = False
    def forbidden(**kwargs):
        raise AssertionError('authoritative active state blocks valuation-only recovery')
    monkeypatch.setattr(scheduler, '_recover_pending_local_sim_projection_if_needed', forbidden)
    assert scheduler._terminalize_historical_localsim_durable_run_if_safe(
        run=run, scheduler_trade_date=today, as_of_time=None) is None


@pytest.mark.parametrize('invalid', ['2026-05-22T09:48:00+08:00', '2026-05-21T09:48:00'])
def test_frozen_valuation_time_cannot_be_replaced_by_another_day_or_naive_clock(monkeypatch, invalid):
    scheduler, repo, release, binding, run, plan, outbox, today, frozen = _pending(monkeypatch)
    changed = outbox.model_copy(update={'projection_payload': {**outbox.projection_payload,
                                                               'valuation_snapshot_time': invalid}})
    with pytest.raises(module.DataUnavailableError) as exc:
        scheduler._recover_historical_localsim_pending_valuation(
            run=run, outbox=changed, scheduler_trade_date=today)
    assert exc.value.context['reason_code'] == 'LOCALSIM_HISTORICAL_VALUATION_TIME_CONFLICT'


def test_historical_valuation_rejects_saved_plan_identity_drift(monkeypatch):
    scheduler, repo, release, binding, run, plan, outbox, today, frozen = _pending(monkeypatch)
    changed = run.model_copy(update={'execution_plan_hash': '0' * 64})
    with pytest.raises(module.DataUnavailableError, match='identity'):
        scheduler._recover_historical_localsim_pending_valuation(
            run=changed, outbox=outbox, scheduler_trade_date=today)
