"""Native historical replay closure: no second execution or invented close prices."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimHistoricalReplayDayExecutor
from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus
from backend.tests.simulation_runtime import test_lifecycle_scheduler as native
from backend.services.trading_core.errors import DataUnavailableError


class ClosingHistoricalProvider(native.FakeLocalSimMarketDataProvider):
    include_close = True

    def load_symbol_input(self, **kwargs):
        result = super().load_symbol_input(**kwargs)
        if self.include_close:
            closing = result.minute_bars[-1].model_copy(update={
                "bar_time": datetime.combine(kwargs["trade_date"], time(15)),
                "open": 10.8, "high": 10.9, "low": 10.7, "close": 10.8,
            })
            result = replace(result, minute_bars=[*result.minute_bars, closing])
        return result


def historical_day(monkeypatch, *, no_orders=False, include_close=True, retain_positions=False):
    release, binding, _, repo = native._release_and_bindings(qmt_only=False)
    binding = binding.model_copy(update={
        "broker_account_id": binding.strategy_id,
        "effective_from": native.TRADE_DATE, "effective_to": native.TRADE_DATE,
        "binding_config_json": {"metadata": {"localsim_account_id": binding.strategy_id,
                                               "account_schema_version": "simulation_account_v1"}},
    })
    repo.bindings[binding.binding_id] = binding
    paper = native.InMemoryPaperTradingV2Repository()
    provider = ClosingHistoricalProvider()
    provider.include_close = include_close
    held = {candidate.symbol: native.PositionLot(
        portfolio_id="paper_historical_close", symbol=candidate.symbol,
        quantity=candidate.target_quantity, available_quantity=candidate.target_quantity,
        avg_cost=9.0, trade_date=native.TRADE_DATE - timedelta(days=1),
    ) for candidate in native._candidate_rows()[:1]} if retain_positions else {}
    context = native._local_sim_context_with_real_broker(
        portfolio_id="paper_historical_close", release=release, paper_repository=paper,
        market_data_provider=provider, positions=held,
    )
    job = SimpleNamespace(
        replay_job_id="original-replay", simulation_account_id=binding.strategy_id,
        historical_source_id="market.kline_minute_raw.v1", binding_id=binding.binding_id,
        release_id=release.release_id, start_trade_date=native.TRADE_DATE,
        end_trade_date=native.TRADE_DATE, status=LocalSimReplayStatus.RUNNING_HISTORICAL,
        live_release_id=None,
    )
    scheduler = native.SimulationLifecycleScheduler(
        repository=repo,
        selection_service=native.FakeSelectionService(
            release, candidates=[] if no_orders and not retain_positions else native._candidate_rows(),
            valid_no_candidate=no_orders and not retain_positions),
        context_provider=native.StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
        localsim_successor_repository=SimpleNamespace(list_replay_jobs=lambda **_kwargs: [job]),
    )
    def now():
        return datetime(2026, 5, 22, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr("backend.services.simulation_runtime.lifecycle.scheduler_now", now)
    monkeypatch.setattr("backend.services.simulation_runtime.scheduler.scheduler_now", now)
    return SimpleNamespace(scheduler=scheduler, repo=repo, paper=paper, context=context, job=job,
                           provider=provider, binding=binding, release=release,
                           executor=LocalSimHistoricalReplayDayExecutor(lifecycle_scheduler=scheduler))


def current_run(day):
    return day.repo.get_simulation_daily_run_by_key(
        strategy_id=day.job.simulation_account_id, binding_id=day.job.binding_id,
        trade_date=native.TRADE_DATE,
    )


def economic_facts(day, run_id):
    return deepcopy({"fills": day.paper.list_fills_for_run(run_id),
                     "cash": day.paper.cash_entries.get(run_id, []),
                     "orders": day.paper.orders.get(run_id, []),
                     "positions": {symbol: (lot.quantity, lot.avg_cost) for symbol, lot
                                   in day.context.local_broker.query_positions().items()},
                     "cash_balance": day.context.local_broker.query_account().cash})


@pytest.mark.parametrize("no_orders", [False, True])
def test_native_replay_closes_original_day_once_without_repeating_economics(monkeypatch, no_orders):
    day = historical_day(monkeypatch, no_orders=no_orders)
    day.executor(day.job, native.TRADE_DATE)
    run = current_run(day)
    snapshot = day.paper.snapshots[run.run_id]
    assert snapshot.snapshot_time.time() == time(15)
    assert snapshot.market_value == pytest.approx(sum(
        position.quantity * 10.8 for position in day.paper.positions[run.run_id]))
    assert run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert run.run_payload_json["local_sim_close_valuation_v1"]["parent_resubmitted"] is False
    assert run.run_payload_json["broker_called"] is (not no_orders)
    assert bool(day.paper.list_fills_for_run(run.run_id)) is (not no_orders)
    facts, generation = economic_facts(day, run.run_id), run.run_payload_json["local_sim_economic_generation"]
    day.executor(day.job, native.TRADE_DATE)
    assert current_run(day).run_id == run.run_id
    assert current_run(day).run_payload_json["local_sim_economic_generation"] == generation
    assert economic_facts(day, run.run_id) == facts
    assert day.paper.snapshots[run.run_id] == snapshot
    assert len(day.scheduler.selection_service.calls) == 1


def test_missing_closing_bar_keeps_original_execution_then_recovers_without_duplicate_fills(monkeypatch):
    day = historical_day(monkeypatch, include_close=False)
    with pytest.raises(DataUnavailableError, match="closing (marks|minute)"):
        day.executor(day.job, native.TRADE_DATE)
    run = current_run(day)
    assert run.status in {native.SimulationDailyRunStatus.SUCCEEDED, native.SimulationDailyRunStatus.FAILED_TERMINAL}
    assert day.paper.snapshots[run.run_id].snapshot_time.time() < time(15)
    assert "local_sim_close_valuation_v1" not in run.run_payload_json
    before = economic_facts(day, run.run_id)
    generation = run.run_payload_json["local_sim_economic_generation"]
    day.provider.include_close = True
    day.executor(day.job, native.TRADE_DATE)
    assert current_run(day).run_id == run.run_id
    assert current_run(day).run_payload_json["local_sim_economic_generation"] == generation + 1
    assert day.paper.snapshots[run.run_id].snapshot_time.time() == time(15)
    assert economic_facts(day, run.run_id) == before
    assert len(day.scheduler.selection_service.calls) == 1


def test_no_rebalance_day_revalues_existing_holdings_without_trade_or_fee(monkeypatch):
    day = historical_day(monkeypatch, no_orders=True, retain_positions=True)
    before = economic_facts(day, "not-created")
    day.executor(day.job, native.TRADE_DATE)
    run = current_run(day)
    assert run.run_payload_json["no_rebalance_required"] is True
    assert run.run_payload_json["broker_called"] is False
    assert economic_facts(day, run.run_id) == before
    snapshot = day.paper.snapshots[run.run_id]
    assert snapshot.snapshot_time.time() == time(15)
    assert snapshot.market_value == pytest.approx(1000 * 10.8)
    assert snapshot.cash == 100_000


def test_close_projection_failure_replays_same_committed_generation(monkeypatch):
    day = historical_day(monkeypatch)
    original = day.paper.save_daily_snapshot
    failed = []

    def fail_close_once(**kwargs):
        if kwargs["snapshot"].snapshot_time.time() == time(15) and not failed:
            failed.append(True)
            raise native.psycopg2.OperationalError("injected close projection failure")
        return original(**kwargs)

    monkeypatch.setattr(day.paper, "save_daily_snapshot", fail_close_once)
    with pytest.raises(DataUnavailableError):
        day.executor(day.job, native.TRADE_DATE)
    run = current_run(day)
    assert failed
    generation = run.run_payload_json["local_sim_economic_generation"]
    before = economic_facts(day, run.run_id)
    day.executor(day.job, native.TRADE_DATE)
    assert current_run(day).run_payload_json["local_sim_economic_generation"] == generation
    assert current_run(day).run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert day.paper.snapshots[run.run_id].snapshot_time.time() == time(15)
    assert economic_facts(day, run.run_id) == before


@pytest.mark.parametrize("field", ["strategy_id", "binding_id", "release_id", "trade_date"])
def test_close_rejects_foreign_scope_without_changing_account(monkeypatch, field):
    day = historical_day(monkeypatch)
    day.executor(day.job, native.TRADE_DATE)
    run = current_run(day)
    args = {"run_id": run.run_id, "trade_date": native.TRADE_DATE,
            "strategy_id": run.strategy_id, "binding_id": run.binding_id, "release_id": run.release_id}
    args[field] = native.TRADE_DATE.replace(day=22) if field == "trade_date" else "foreign"
    facts = economic_facts(day, run.run_id)
    with pytest.raises(DataUnavailableError, match="scope does not match"):
        day.scheduler.complete_localsim_replay_close(**args)
    assert economic_facts(day, run.run_id) == facts
