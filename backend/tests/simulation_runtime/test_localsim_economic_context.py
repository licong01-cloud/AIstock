"""Committed cash must carry its own lots across a missing valuation."""
from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.simulation_execution.localsim.economic import local_sim_fact_payload
from backend.services.simulation_execution.localsim.models import LocalSimEconomicReceiptV1, LocalSimProjectionOutboxV1
from backend.services.simulation_runtime.localsim_economic_context import pending_economic_context
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import PositionLot


def generation():
    lot = PositionLot(portfolio_id="account", symbol="000001.SZ", quantity=200,
                      available_quantity=0, avg_cost=10, trade_date=date(2026, 11, 9))
    hashes = {lot.symbol: canonical_json_sha256(local_sim_fact_payload(lot, fact_type="position"))}
    identity = {"run_id": "run", "binding_id": "binding", "plan_id": "plan", "trade_date": "2026-11-09"}
    facts = dict(identity, schema_version="local_sim_valuation_pending_economic_facts_v1",
        fact_kind="ECONOMIC_FACTS_WITH_VALUATION_PENDING", position_hashes=hashes, cash_reference=98000,
        state_hashes={"state": "state-hash"}, order_hashes={"order": "order-hash"},
        fill_hashes={"fill": "fill-hash"}, order_event_hashes={}, cash_entry_hashes={"fill": "cash-hash"})
    receipt = LocalSimEconomicReceiptV1(**identity, generation=1, economic_facts=facts)
    payload = dict(identity, schema_version="local_sim_valuation_pending_projection_payload_v1",
        strategy_id="strategy", portfolio_id="account", projection_kind="VALUATION_PENDING",
        positions=[lot.model_dump(mode="json")], position_hashes=hashes, cash_reference=98000,
        economic_hash=receipt.economic_hash)
    outbox = LocalSimProjectionOutboxV1(receipt_id=receipt.receipt_id, run_id="run", plan_id="plan",
                                      generation=1, economic_hash=receipt.economic_hash, projection_payload=payload)
    run = SimpleNamespace(run_id="run", binding_id="binding", execution_plan_id="plan", strategy_id="strategy",
        trade_date=date(2026, 11, 9), run_payload_json={"local_sim_projection_outbox_v1": outbox.model_dump(mode="json"),
        "local_sim_economic_receipts_v1": {receipt.receipt_id: receipt.model_dump(mode="json")}})
    return run


def read(run, *, fail_readback=False):
    calls = []
    def durable(**kw):
        calls.append("durable_readback")
        if fail_readback:
            raise ValueError("durable facts lost")
    def paper(**kw):
        assert kw["fill_ids"] == {"fill"} and kw["cash_fill_ids"] == {"fill"}
        calls.append("paper_readback")
    repo = SimpleNamespace(get_latest_local_sim_economic_run=lambda **kw: run,
        list_local_sim_execution_states=lambda *args, **kw: [SimpleNamespace(state_id="state", state_hash="state-hash")],
        readback_local_sim_economic_commit=durable)
    value = pending_economic_context(runtime_repository=repo,
        paper_repository=SimpleNamespace(readback_local_sim_economic_facts=paper),
        strategy_id="strategy", portfolio_id="account", trade_date=date(2026, 11, 10))
    return value, calls


def test_next_day_carries_committed_buy_lots_and_cash_without_replay():
    run = generation()
    before = deepcopy(run.run_payload_json)
    (positions, cash, evidence), calls = read(run)
    assert positions["000001.SZ"].quantity == 200 and cash == 98000
    assert positions["000001.SZ"].available_quantity == 0  # settlement belongs to the provider
    assert calls == ["durable_readback", "paper_readback"]
    assert evidence["new_economic_commits"] == 0 and not evidence["economic_recomputation_performed"]
    assert run.run_payload_json == before


@pytest.mark.parametrize("mutation", ["cash", "position", "receipt", "scope", "future", "state"])
def test_conflicting_generation_never_falls_back_to_stale_snapshot(mutation):
    run = generation()
    if mutation == "cash":
        run.run_payload_json["local_sim_projection_outbox_v1"]["projection_payload"]["cash_reference"] = 1
    elif mutation == "position":
        run.run_payload_json["local_sim_projection_outbox_v1"]["projection_payload"]["positions"][0]["quantity"] = 999
    elif mutation == "receipt":
        run.run_payload_json["local_sim_economic_receipts_v1"] = {}
    elif mutation == "scope":
        run.strategy_id = "other-account"
    elif mutation == "future":
        run.trade_date = date(2026, 11, 11)
    else:
        receipt = next(iter(run.run_payload_json["local_sim_economic_receipts_v1"].values()))
        receipt["economic_facts"]["state_hashes"] = {}
    with pytest.raises(DataUnavailableError, match="generation cannot be verified"):
        read(run)


def test_independent_readback_is_required():
    with pytest.raises(DataUnavailableError, match="generation cannot be verified"):
        read(generation(), fail_readback=True)


def test_no_economic_generation_keeps_original_ledger_path():
    assert read(None) == (None, [])


def test_normal_projection_retry_also_carries_one_verified_generation():
    from datetime import datetime
    from backend.services.trading_core.models import AccountSnapshot
    run = generation()
    original = LocalSimProjectionOutboxV1.model_validate(run.run_payload_json["local_sim_projection_outbox_v1"])
    old_receipt = LocalSimEconomicReceiptV1.model_validate(
        run.run_payload_json["local_sim_economic_receipts_v1"][original.receipt_id])
    snapshot = AccountSnapshot(portfolio_id="account", cash=98000, market_value=2000, nav=100000,
                              snapshot_time=datetime.fromisoformat("2026-11-09T15:00:00+08:00"))
    facts = dict(old_receipt.economic_facts, schema_version="local_sim_economic_facts_v1",
                 account_snapshot_hash=canonical_json_sha256(local_sim_fact_payload(snapshot, fact_type="account_snapshot")))
    receipt = LocalSimEconomicReceiptV1(run_id="run", binding_id="binding", plan_id="plan",
        trade_date=run.trade_date, generation=1, economic_facts=facts)
    payload = dict(original.projection_payload, schema_version="local_sim_projection_payload_v1",
                   account_snapshot=snapshot.model_dump(mode="json"), economic_hash=receipt.economic_hash)
    for field in ("cash_reference", "position_hashes", "projection_kind"):
        payload.pop(field)
    outbox = LocalSimProjectionOutboxV1(receipt_id=receipt.receipt_id, run_id="run", plan_id="plan",
        generation=1, economic_hash=receipt.economic_hash, projection_payload=payload)
    run.run_payload_json = {"local_sim_projection_outbox_v1": outbox.model_dump(mode="json"),
        "local_sim_economic_receipts_v1": {receipt.receipt_id: receipt.model_dump(mode="json")}}
    (positions, cash, evidence), _ = read(run)
    assert cash == 98000 and positions["000001.SZ"].quantity == 200
    assert evidence["new_economic_commits"] == 0


def test_latest_generation_selection_excludes_other_accounts_and_future_days():
    from datetime import UTC, datetime
    from backend.services.simulation_data.daily_context import SimulationBrokerBackend
    from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
    repo = InMemorySimulationRuntimeRepository()
    run = generation()
    run.broker_backend = SimulationBrokerBackend.LOCAL_SIM
    run.updated_at = run.created_at = datetime(2026, 11, 9, tzinfo=UTC)
    repo.daily_runs[run.run_id] = run
    future = deepcopy(run)
    future.run_id, future.trade_date = "future", date(2026, 11, 11)
    repo.daily_runs[future.run_id] = future
    other = deepcopy(run)
    other.run_id, other.trade_date = "other", date(2026, 11, 10)
    other.run_payload_json["local_sim_projection_outbox_v1"]["projection_payload"]["portfolio_id"] = "other"
    repo.daily_runs[other.run_id] = other
    assert repo.get_latest_local_sim_economic_run(strategy_id="strategy", portfolio_id="account",
                                                  before_or_on=date(2026, 11, 10)) is run


def test_production_provider_uses_one_generation_before_tplus1_settlement(monkeypatch):
    from backend.tests.simulation_runtime import test_lifecycle_scheduler as fixtures
    from backend.services.simulation_runtime import localsim_economic_context as module
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider
    from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
    release = fixtures._make_test_release()
    portfolio = fixtures.PaperPortfolio(portfolio_id="strat1", portfolio_name="context regression",
        package_id=release.package_id, manifest_sha256=release.manifest_sha256,
        frozen_manifest=fixtures._frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256),
        initial_cash=100000, start_date=date(2026, 11, 9), data_source=fixtures.MinuteDataSource.DB_HISTORICAL)
    paper = fixtures.FakePaperRepository(portfolio, positions={}, cash=100000)
    def reject_stale(*args):
        pytest.fail("must not combine snapshot positions or cash with a pending generation")
    paper.load_latest_positions = paper.load_latest_cash = reject_stale
    lot = PositionLot(portfolio_id="strat1", symbol="000001.SZ", quantity=200, available_quantity=0,
                      avg_cost=10, trade_date=date(2026, 11, 9))
    monkeypatch.setattr(module, "pending_economic_context", lambda **kwargs:
                        ({lot.symbol: lot}, 98000, {"source": "verified_generation"}))
    provider = ProductionSimulationRunContextProvider(paper_repository_factory=lambda: paper,
        runtime_repository=InMemorySimulationRuntimeRepository(), price_loader=lambda symbols, day: {s: 10 for s in symbols},
        pre_trade_tradability_provider=fixtures.FakePreTradeTradabilityProvider())
    binding = fixtures._make_test_binding(release, broker_backend=fixtures.SimulationBrokerBackend.LOCAL_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date(2026, 11, 10))
    assert ctx.cash == 98000 and ctx.current_positions[lot.symbol].quantity == 200
    assert ctx.current_positions[lot.symbol].available_quantity == 200
    assert ctx.context_diagnostics["localsim_economic_context"]["source"] == "verified_generation"
