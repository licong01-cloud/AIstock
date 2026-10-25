"""Continuous-account functional evidence, never annual research outcomes."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import PlanEvent
from backend.services.position_timing.intraday.research import CostContract
from backend.services.simulation_execution.localsim.timing_portfolio import (
    commit_portfolio_day, compile_native_intents, run_portfolio_day,
)
from backend.services.simulation_execution.localsim.timing_research import backend_for
from backend.services.simulation_execution.localsim.timing_store import TimingFileRepository
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs


def cost():
    return CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")


def initial(cash="100000"):
    return {"portfolio_id": "functional_campaign", "package_id": "FUNCTIONAL_FIXTURE",
            "manifest_sha256": "a" * 64, "origin_initial_cash_cny": cash,
            "cash_cny": cash, "positions": {}}


def pair():
    p1, m1 = inputs()
    prototype = m1.bars[0]
    bars = tuple(prototype.model_copy(update={"start": at("09:30:00") + timedelta(minutes=n),
                                              "end": at("09:31:00") + timedelta(minutes=n),
                                              "received_at": at("09:31:05") + timedelta(minutes=n)})
                 for n in range(327) if n < 120 or n >= 210)
    m1 = m1.model_copy(update={"bars": bars})
    p2 = p1.model_copy(update={"plan_id": "other", "symbol": "600000.SH"})
    m2 = m1.model_copy(update={"symbol": p2.symbol,
                              "day_facts": m1.day_facts.model_copy(update={"symbol": p2.symbol})})
    return [PlanEvent(available_at=p.valid_from, plan=p) for p in (p1, p2)], {m.symbol: m for m in (m1, m2)}


def execute(account=None, *, events=None, markets=None, run_id="functional_campaign", delay=0):
    default_events, default_markets = pair()
    events = default_events if events is None else events
    markets = default_markets if markets is None else markets
    backend = backend_for(account or initial(), next(iter(markets.values())), cost())
    result = run_portfolio_day(backend=backend, markets=markets, events=events,
                               initial_marks={s: Decimal("10") for s in markets},
                               delay_seconds=delay, run_id=run_id)
    return result


@pytest.mark.parametrize("delay", [0, 60, 180, 300])
def test_multiple_stocks_share_real_cash_parent_fees_and_four_delays(delay):
    result = execute(initial("15010"), delay=delay)
    account, backend = result["account"], result["backend"]
    assert [p["filled_quantity"] for p in account["parents"]] == [1000, 500]
    assert [p["status"] for p in account["parents"]] == ["FILLED", "PARTIAL"]
    assert backend._ledger.cash_decimal == 0
    assert Decimal(account["fees_cny"]) == 10
    assert len(backend._ledger.fills) == 2
    assert all(p.available_quantity == 0 for p in backend._ledger.positions.values())
    assert all(s.waits[0][1] == Decimal(50 + delay) for s in result["sessions"])
    assert len(backend._records) == len(result["sessions"]) == 2
    audit = account["parents"][0]["session"]
    assert audit["schema_version"] == "localsim_timing_parent_audit_v1"
    assert not {"cash_cny", "positions", "cash_entries"}.intersection(audit)
    from backend.services.simulation_execution.localsim.timing import TimingExecution
    with pytest.raises(ValueError, match="RESTORE_IDENTITY_MISMATCH"):
        TimingExecution.restore(backend, result["sessions"][0].market, audit)
    # A new single-order study must still reject an existing portfolio scope.
    session = result["sessions"][0]
    with pytest.raises(ValueError, match="SINGLE_ORDER_SCOPE"):
        backend.open_timing_execution(plan=session.facade.plan, delay_seconds=0,
                                       mode=session.facade.mode, run_id="other", binding_id=backend.portfolio_id)


def next_day_events(events, markets):
    advanced, out = [], {}
    for event in events:
        plan = event.plan
        constraint = plan.price_constraint.model_copy(update={
            "valid_from": plan.price_constraint.valid_from + timedelta(days=1),
            "valid_until": plan.price_constraint.valid_until + timedelta(days=1)})
        plan = plan.model_copy(update={"plan_id": plan.plan_id + "_next", "side": "SELL",
                                      "target_trade_date": plan.target_trade_date + timedelta(days=1),
                                      "valid_from": plan.valid_from + timedelta(days=1),
                                      "valid_until": plan.valid_until + timedelta(days=1),
                                      "price_constraint": constraint})
        advanced.append(PlanEvent(available_at=plan.valid_from, plan=plan))
    for symbol, market in markets.items():
        bars = tuple(b.model_copy(update={key: getattr(b, key) + timedelta(days=1)
                                         for key in ("start", "end", "received_at")}) for b in market.bars)
        out[symbol] = market.model_copy(update={
            "trade_date": market.trade_date + timedelta(days=1), "bars": bars,
            "day_facts": market.day_facts.model_copy(update={"trade_date": market.trade_date + timedelta(days=1),
                                                            "available_at": market.day_facts.available_at + timedelta(days=1)}),
            "calendar": market.calendar.model_copy(update={"trade_date": market.trade_date + timedelta(days=1),
                                                          "available_at": market.calendar.available_at + timedelta(days=1)})})
    return advanced, out


def test_multi_day_t1_settlement_and_day_atomic_retry(tmp_path):
    events, markets = pair()
    first = execute(events=events, markets=markets)
    store = TimingFileRepository(tmp_path, "functional_campaign")
    commit_portfolio_day(store=store, result=first, input_identity="f" * 64, day_index=0)
    assert commit_portfolio_day(store=store, result=first, input_identity="f" * 64,
                                day_index=0)["status"] == "ALREADY_MATERIALIZED"
    account = {**initial(), "cash_cny": first["account"]["cash_cny"], "positions": first["account"]["positions"]}
    events, markets = next_day_events(events, markets)
    second = execute(account, events=events, markets=markets)
    assert [p["filled_quantity"] for p in second["account"]["parents"]] == [1000, 1000]
    assert second["backend"]._ledger.positions == {}
    assert Decimal(second["account"]["fees_cny"]) == Decimal("11.90")
    with pytest.raises(ValueError, match="SOURCE_CHANGED"):
        commit_portfolio_day(store=store, result=second, input_identity="e" * 64, day_index=1)
    commit_portfolio_day(store=store, result=second, input_identity="f" * 64, day_index=1)
    record = store.latest()
    assert record["generation"] == 2
    assert len(record["facts"]["fills"]) == len(record["facts"]["cash_entries"]) == 4
    assert record["checkpoint"]["account"] == second["account"]


def test_day_commit_failure_keeps_confirmed_prefix(tmp_path, monkeypatch):
    from backend.services.simulation_execution.localsim import timing_store

    events, markets = pair()
    first = execute(events=events, markets=markets)
    store = TimingFileRepository(tmp_path, "functional_campaign")
    commit_portfolio_day(store=store, result=first, input_identity="f" * 64, day_index=0)
    prior = digest(store.latest())
    account = {**initial(), "cash_cny": first["account"]["cash_cny"], "positions": first["account"]["positions"]}
    events, markets = next_day_events(events, markets)
    second = execute(account, events=events, markets=markets)
    publish = timing_store.publish

    def fail_before_publish(*args, **kwargs):
        raise OSError("injected publication failure")

    monkeypatch.setattr(timing_store, "publish", fail_before_publish)
    with pytest.raises(OSError, match="publication failure"):
        commit_portfolio_day(store=store, result=second, input_identity="f" * 64, day_index=1)
    assert digest(store.latest()) == prior
    monkeypatch.setattr(timing_store, "publish", publish)
    commit_portfolio_day(store=store, result=second, input_identity="f" * 64, day_index=1)
    assert len(store.latest()["facts"]["fills"]) == 4


def test_native_quantity_compiler_and_trading_rule_service_are_reused():
    _, markets = pair()
    projection = {"manifest_sha256_by_package": {"FUNCTIONAL_FIXTURE": "a" * 64},
                  "runtime_config": {"point_in_time_context": {"effective_trade_date": "2026-09-21",
                                                                "cutoff_date": "2026-09-18"}},
                  "aggregate_results": [{"symbol": symbol, "score": 1, "rank": rank,
                                         "target_weight": 0.5, "reference_price": 10}
                                        for rank, symbol in enumerate(markets, 1)]}
    backend, targets, result = compile_native_intents(
        projection=projection, manifest=None, account=initial("15010"), markets=markets,
        prices={s: Decimal("10") for s in markets}, cost=cost())
    assert backend._ledger.cash_decimal == Decimal("15010")
    assert [t.target_quantity for t in targets] == [700, 700]
    assert [i.quantity for i in result.order_intents] == [700, 700]
    assert len(result.trading_rule_decisions) == 2


def test_future_suffix_never_changes_prior_orders_or_core_decisions():
    events, markets = pair()
    original = execute(events=events, markets=markets)
    changed = {symbol: market.model_copy(update={"bars": tuple(
        b if b.start < at("09:40:00") else b.model_copy(update={"open": Decimal("10.5"), "high": Decimal("10.5"),
                                                               "low": Decimal("10.5"), "close": Decimal("10.5")})
        for b in market.bars)}) for symbol, market in markets.items()}
    amended = execute(events=events, markets=changed)
    assert [p["filled_quantity"] for p in amended["account"]["parents"]] == [1000, 1000]
    assert [f.model_dump(mode="json") for f in original["backend"]._ledger.fills] == [
        f.model_dump(mode="json") for f in amended["backend"]._ledger.fills]


def test_revised_price_plan_cancels_pending_intent_before_execution():
    events, markets = pair()
    events = [events[0]]
    original = events[0].plan
    revoked = original.model_copy(update={"revision": 2, "lifecycle": "CANCELLED"})
    events.append(PlanEvent(available_at=at("09:32:00"), plan=revoked))
    result = execute(events=events, markets=markets, delay=300)
    assert result["account"]["parents"][0]["status"] == "NO_FILL"
    assert result["backend"]._ledger.cash_decimal == Decimal("100000")


def test_visibility_contract_and_unknown_tradability_fail_before_account_effects():
    events, markets = pair()
    symbol = events[0].plan.symbol
    market = markets[symbol]
    bad = market.bars[0].model_copy(update={"received_at": market.bars[0].end})
    markets[symbol] = market.model_copy(update={"bars": (bad,) + market.bars[1:]})
    with pytest.raises(ValueError, match="VISIBILITY_CONTRACT"):
        execute(events=events, markets=markets)


def test_missing_terminal_minutes_are_unavailable_instead_of_morning_mark_carry():
    events, markets = pair()
    symbol = events[0].plan.symbol
    markets[symbol] = markets[symbol].model_copy(update={"bars": markets[symbol].bars[:30]})
    with pytest.raises(ValueError, match="MINUTE_PREFIX_INCOMPLETE"):
        execute(events=events, markets=markets)


def test_buy_waits_for_other_parents_confirmed_sell_cash():
    from backend.services.trading_core.models import PositionLot

    events, markets = pair()
    sell = events[0].plan.model_copy(update={"side": "SELL"})
    events[0] = PlanEvent(available_at=sell.valid_from, plan=sell)
    account = {**initial(), "cash_cny": "0", "positions": {
        sell.symbol: PositionLot(portfolio_id="functional_campaign", symbol=sell.symbol,
                                 quantity=1000, available_quantity=1000, avg_cost=10,
                                 trade_date=sell.target_trade_date - timedelta(days=3)).model_dump(mode="json")}}
    result = execute(account, events=events, markets=markets)
    seller, buyer = result["sessions"]
    assert seller.order.side.value == "SELL" and buyer.order.side.value == "BUY"
    assert seller.fills[0].trade_time == at("09:32:00")
    assert buyer.fills[0].trade_time == at("09:34:00")
    assert buyer.order.filled_quantity == 900
    assert result["backend"]._ledger.cash_decimal >= 0


def test_native_blocked_request_remains_in_denominator_and_minimum_fee():
    from backend.services.simulation_execution.localsim.timing_portfolio import attach_native_quantity_evidence

    events, markets = pair()
    symbol = "600000.SH"
    markets[symbol] = markets[symbol].model_copy(update={"bars": (), "day_facts": markets[symbol].day_facts.model_copy(
        update={"is_suspended": True, "has_daily_limit": None, "reference_price": None})})
    projection = {"manifest_sha256_by_package": {"FUNCTIONAL_FIXTURE": "a" * 64},
                  "runtime_config": {"point_in_time_context": {"effective_trade_date": "2026-09-21", "cutoff_date": "2026-09-18"}},
                  "aggregate_results": [{"symbol": s, "score": 1, "rank": rank, "target_weight": 0.5, "reference_price": 10}
                                        for rank, s in enumerate(markets, 1)]}
    backend, targets, native = compile_native_intents(projection=projection, manifest=None, account=initial("15010"),
                                                      markets=markets, prices={s: Decimal("10") for s in markets}, cost=cost())
    plan = events[0].plan.model_copy(update={"target_quantity": native.order_intents[0].quantity})
    result = run_portfolio_day(backend=backend, markets=markets, events=[PlanEvent(available_at=plan.valid_from, plan=plan)],
                               initial_marks={s: Decimal("10") for s in markets}, delay_seconds=0, run_id="functional_campaign")
    attach_native_quantity_evidence(result, targets=targets, native=native, projection_ref={}, price_policy_sha256="a" * 64)
    assert len(result["account"]["parents"]) == 2
    blocked = result["account"]["parents"][1]
    assert blocked["status"] == "NO_FILL" and blocked["requested_quantity"] == 700
    assert blocked["order_created"] is False and blocked["legal_quantity"] == 0
    assert blocked["reason_codes"] == ["SUSPENDED_BY_SUSPEND_D"]
    assert result["account"]["minimum_fee_parent_count"] == 1
    assert result["account"]["fees_cny"] == "5.00"


def test_portfolio_cancel_handle_reaches_only_the_matching_native_parent():
    from backend.services.trading_core.models import OrderStatus

    events, markets = pair()
    backend = backend_for(initial(), next(iter(markets.values())), cost())
    sessions = [backend.open_timing_portfolio_execution(plan=e.plan, market=markets[e.plan.symbol],
                                                        delay_seconds=0, run_id="functional_campaign",
                                                        binding_id=backend.portfolio_id) for e in events]
    assert backend.cancel(sessions[0].handle).accepted
    assert sessions[0].order.status == OrderStatus.CANCELLED
    assert sessions[1].order.status != OrderStatus.CANCELLED
    assert backend._ledger.cash_decimal == Decimal("100000")


def price_campaign_fixture(tmp_path, monkeypatch, *, two_days=False, cash="10000000"):
    """Synthetic file-contract fixture; never a real annual study artifact."""
    from datetime import date
    from pathlib import Path

    import pandas as pd

    from backend.services.simulation_execution.localsim import timing_campaign as module
    from backend.services.simulation_execution.localsim.timing_store import publish
    from backend.services.strategy_package.manifest import compute_manifest_json_sha256

    def put(name, value):
        path = tmp_path / name
        publish(path, value)
        return module.ref(path)

    raw_manifest = {"package_id": "FUNCTIONAL_FIXTURE"}
    manifest_sha = compute_manifest_json_sha256(raw_manifest)
    dates = ["2026-09-21", "2026-09-22"] if two_days else ["2026-09-21"]
    events, markets = pair()
    _, next_markets = next_day_events(events, markets)

    class FunctionalCandidate:
        def __init__(self, ref):
            self.ref = ref

        def calendar(self, frequency):
            assert frequency == "day"
            return pd.DatetimeIndex(dates)

        def market(self, symbol, day):
            chosen = markets if day == date(2026, 9, 21) else next_markets
            market = chosen[symbol]
            return market.model_copy(update={"bars": tuple(b.model_copy(update={"volume_shares": Decimal("1000000")})
                                                           for b in market.bars)})

        def verify_pins(self):
            pass

    monkeypatch.setattr(module, "CandidateReader", FunctionalCandidate)
    candidate = put("candidate.json", {"role": "FUNCTIONAL_FIXTURE_ONLY"})
    package = put("package.json", {"package": {"manifest": raw_manifest, "manifest_sha256": manifest_sha}})
    cost_ref = put("cost.json", cost().model_dump(mode="json"))
    dummy = put("source.json", {"role": "FUNCTIONAL_FIXTURE_ONLY"})
    inventory_days, price_days = [], []
    for day in dates:
        projection = {"manifest_sha256_by_package": {"FUNCTIONAL_FIXTURE": manifest_sha},
                      "runtime_config": {"point_in_time_context": {"effective_trade_date": day,
                                                                    "cutoff_date": "2026-09-18" if day == dates[0] else dates[0]}},
                      "aggregate_results": [{"symbol": symbol, "score": 1, "rank": rank,
                                             "target_weight": 0.5, "reference_price": 10}
                                            for rank, symbol in enumerate(markets, 1)]}
        projection_ref = put(day + "/projection.json", projection)
        inventory_days.append({"trade_date": day, "projection": projection_ref, "completion": dummy,
                               "target_count": 2, "data_cutoff": projection["runtime_config"]["point_in_time_context"]["cutoff_date"]})
        templates = []
        for symbol in markets:
            for side in ("BUY", "SELL"):
                templates.append({"symbol": symbol, "side": side, "trade_date": day, "revision": 1,
                                  "available_at": day + "T09:20:00+08:00", "valid_from": day + "T09:30:00+08:00",
                                  "valid_until": day + "T14:57:00+08:00", "source_file": dummy,
                                  "source_available_at": day + "T09:00:00+08:00",
                                  "price_constraint": {"mode": "PREFERRED", "intervals": [{"lower": "9.8", "upper": "10.2"}],
                                                       "source_ref": "FUNCTIONAL_FIXTURE_ONLY", "source_sha256": dummy["sha256"],
                                                       "valid_from": day + "T09:30:00+08:00", "valid_until": day + "T14:57:00+08:00"}})
        price_days.append({"trade_date": day, "events": put(day + "/price_events.json", templates)})
    inventory = put("inventory.json", {"days": inventory_days, "missing_trade_dates": [], "conflicting_trade_dates": []})
    initials = []
    for rule in module.ARMS:
        for delay in module.DELAYS:
            account = {"account_id": "functional_" + digest([rule, delay])[:24],
                       "package_id": "FUNCTIONAL_FIXTURE", "manifest_sha256": manifest_sha,
                       "rule": rule, "delay_seconds": delay, "cash_cny": cash,
                       "origin_initial_cash_cny": "10000000", "positions": {}, "fills": [],
                       "production_account_created": False}
            initials.append(put("initials/" + account["account_id"] + ".json", account))
    preflight = put("preflight.json", {"package_id": "FUNCTIONAL_FIXTURE", "manifest_sha256": manifest_sha,
                                      "selection_inventory": inventory, "initial_accounts": initials,
                                      "start_trade_date": dates[0], "candidate_end_trade_date": dates[-1],
                                      "inputs": {"package": package, "candidate_manifest": candidate, "cost_policy": cost_ref}})
    policy = put("policy.json", {"price_policy_id": "FUNCTIONAL_ONLY", "package_manifest_sha256": manifest_sha,
                                "candidate_manifest_sha256": candidate["sha256"], "start_trade_date": dates[0],
                                "end_trade_date": dates[-1], "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP",
                                "applicability_statement": "SYNTHETIC_FUNCTIONAL_FIXTURE_NOT_ANNUAL_EVIDENCE"})
    templates = put("templates.json", {"schema_version": "localsim_timing_campaign_price_templates_v1",
                                       "producer_source_commit": "1" * 40, "package_manifest_sha256": manifest_sha,
                                       "candidate_manifest_sha256": candidate["sha256"], "selection_inventory_sha256": inventory["sha256"],
                                       "price_policy_id": "FUNCTIONAL_ONLY", "policy_document": policy, "days": price_days})
    return module, module.FileRef.model_validate(preflight), module.FileRef.model_validate(templates), Path


def test_entire_price_batch_is_inspected_before_freezing_account_request(tmp_path, monkeypatch):
    module, preflight, templates, _ = price_campaign_fixture(tmp_path, monkeypatch)
    inspected = module.inspect_price_templates(preflight, templates)
    assert inspected["outcomes_read"] is False
    assert inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED"
    frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "output", producer_commit="1" * 40)
    assert frozen["account_count"] == 12
    assert frozen["status"] == "FROZEN"
    assert not list((tmp_path / "output").rglob("continuous_results.json"))


@pytest.mark.parametrize("cash", ["10000000.00", "10000000.0", "10000001", "9999999.99"])
def test_native_decimal_cash_representation_does_not_change_initial_capital(tmp_path, monkeypatch, cash):
    module, preflight, templates, _ = price_campaign_fixture(tmp_path, monkeypatch, cash=cash)
    if Decimal(cash) == Decimal("10000000"):
        frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                    output_root=tmp_path / "output", producer_commit="1" * 40)
        assert frozen["status"] == "FROZEN" and frozen["account_count"] == 12
    else:
        with pytest.raises(ValueError, match="INITIAL_ACCOUNT_CONFLICT"):
            module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                               output_root=tmp_path / "output", producer_commit="1" * 40)
        assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("problem", [None, "missing_held_sell", "missing_current_buy"])
def test_departed_target_needs_sell_preference_without_unused_buy(tmp_path, monkeypatch, problem):
    """A held stock may leave TopK; keep its exit without fabricating reentry."""
    module, preflight_ref, templates_ref, Path = price_campaign_fixture(tmp_path, monkeypatch, two_days=True)
    preflight = module.read_json(Path(preflight_ref.path))
    templates = module.read_json(Path(templates_ref.path))
    inventory = module.read_json(Path(preflight["selection_inventory"]["path"]))
    second = inventory["days"][1]
    projection = module.read_json(Path(second["projection"]["path"]))
    departed = projection["aggregate_results"].pop(0)["symbol"]
    remaining = projection["aggregate_results"][0]["symbol"]
    projection_path = tmp_path / "second_projection_one_target.json"
    module.publish(projection_path, projection)
    second.update(projection=module.ref(projection_path), target_count=1)
    inventory_path = tmp_path / "departed_inventory.json"
    module.publish(inventory_path, inventory)
    preflight["selection_inventory"] = module.ref(inventory_path)
    templates["selection_inventory_sha256"] = preflight["selection_inventory"]["sha256"]
    events = module.read_json(Path(templates["days"][1]["events"]["path"]))
    excluded = {(departed, "BUY")}
    if problem == "missing_held_sell":
        excluded.add((departed, "SELL"))
    elif problem == "missing_current_buy":
        excluded.add((remaining, "BUY"))
    events = [event for event in events if (event["symbol"], event["side"]) not in excluded]
    events_path = tmp_path / "departed_price_events.json"
    module.publish(events_path, events)
    templates["days"][1]["events"] = module.ref(events_path)
    changed_preflight = tmp_path / "departed_preflight.json"
    changed_templates = tmp_path / "departed_templates.json"
    module.publish(changed_preflight, preflight)
    module.publish(changed_templates, templates)
    args = (module.FileRef.model_validate(module.ref(changed_preflight)),
            module.FileRef.model_validate(module.ref(changed_templates)))
    if problem is not None:
        with pytest.raises(ValueError, match="PRICE_TEMPLATE_MISSING"):
            module.inspect_price_templates(*args)
    else:
        inspected = module.inspect_price_templates(*args)
        assert inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED"
        assert (departed, "SELL") in inspected["days"][second["trade_date"]]["series"]


@pytest.mark.parametrize("problem", ["missing_side", "late_source", "wrong_candidate", "unknown_scope", "reused_day"])
def test_bad_price_contract_fails_before_account_or_result_writes(tmp_path, monkeypatch, problem):
    from backend.services.simulation_execution.localsim.timing_store import read_json

    module, preflight, templates, Path = price_campaign_fixture(tmp_path, monkeypatch)
    body = read_json(Path(templates.path))
    if problem in {"missing_side", "late_source"}:
        events = read_json(Path(body["days"][0]["events"]["path"]))
        if problem == "missing_side":
            events.pop()
        else:
            events[0]["source_available_at"] = "2026-09-21T10:00:00+08:00"
        event_path = tmp_path / "bad_events.json"
        module.publish(event_path, events)
        body["days"][0]["events"] = module.ref(event_path)
    elif problem == "wrong_candidate":
        body["candidate_manifest_sha256"] = "b" * 64
    elif problem == "unknown_scope":
        policy = read_json(Path(body["policy_document"]["path"]))
        policy.pop("applicability_statement")
        policy_path = tmp_path / "bad_policy.json"
        module.publish(policy_path, policy)
        body["policy_document"] = module.ref(policy_path)
    else:
        body["days"].append(body["days"][0])
    bad_path = tmp_path / "bad_templates.json"
    module.publish(bad_path, body)
    bad = module.FileRef.model_validate(module.ref(bad_path))
    with pytest.raises(ValueError):
        module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=bad,
                                           output_root=tmp_path / "output", producer_commit="1" * 40)
    assert not (tmp_path / "output").exists()


def test_twelve_continuous_accounts_retry_and_committed_summary_recovery(tmp_path, monkeypatch):
    from backend.services.strategy_package.models import StrategyPackageManifest
    from backend.services.simulation_execution.localsim import timing_portfolio

    module, preflight, templates, Path = price_campaign_fixture(tmp_path, monkeypatch, two_days=True)
    # Synthetic native-weight fixture only; production validates the full manifest.
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: None)
    frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "output", producer_commit="1" * 40)
    request = module.FileRef.model_validate(frozen["request"])
    original_run = timing_portfolio.run_portfolio_day
    attempted = []

    def interrupt_third_account(**kwargs):
        attempted.append(kwargs["run_id"])
        if len(attempted) == 3:
            raise OSError("injected offline account interruption")
        return original_run(**kwargs)

    monkeypatch.setattr(timing_portfolio, "run_portfolio_day", interrupt_third_account)
    with pytest.raises(OSError, match="offline account interruption"):
        module.run_continuous_campaign(request)
    assert len(list((Path(request.path).parent / "journals").glob("*/generations/00000001.json"))) == 2
    resumed = []

    def count_resumed(**kwargs):
        resumed.append(kwargs["run_id"])
        return original_run(**kwargs)

    monkeypatch.setattr(timing_portfolio, "run_portfolio_day", count_resumed)
    first = module.run_continuous_campaign(request)
    assert len(resumed) == 22
    result = module.read_json(Path(first["results"]["path"]))
    assert len(result["accounts"]) == 12
    assert len(result["comparisons"]) == 36
    assert all(len(a["days"]) == 2 for a in result["accounts"])
    assert result["outcomes_read_stage"] == "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"
    assert result["economic_confirmation"] is False
    retry = module.run_continuous_campaign(request)
    assert retry["status"] == "ALREADY_MATERIALIZED"
    assert retry["results"] == first["results"]
    # Only an immutable index is removed in this synthetic recovery fixture.
    # The committed journal remains authoritative and cannot be recomputed.
    day_path = Path(result["accounts"][0]["days"][0]["path"])
    original = day_path.read_bytes()
    day_path.unlink()
    recovered = module.run_continuous_campaign(request)
    assert recovered["status"] == "ALREADY_MATERIALIZED"
    assert day_path.read_bytes() == original
    assert recovered["results"] == first["results"]


def test_unfinished_campaign_cannot_resume_with_changed_source(tmp_path, monkeypatch):
    module, preflight, templates, _ = price_campaign_fixture(tmp_path, monkeypatch)
    frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "output", producer_commit="1" * 40)
    monkeypatch.setattr(module, "campaign_source_identity", lambda: {"changed": True})
    with pytest.raises(ValueError, match="SOURCE_CHANGED"):
        module.run_continuous_campaign(module.FileRef.model_validate(frozen["request"]))


@pytest.mark.parametrize("changed", ["adapter", "ledger", "core", "quantity", "new_engine_file"])
def test_successor_only_allows_audited_adapter_source_transition(changed):
    from copy import deepcopy
    from backend.services.simulation_execution.localsim.timing_continuation import ADAPTER, validate_source_transition

    old = {"campaign": {ADAPTER: "a", "quantity": "b"}, "localsim": {"ledger": "c"}, "timing_core": {"core": "d"}}
    new = deepcopy(old)
    group, key = {"adapter": ("campaign", ADAPTER), "ledger": ("localsim", "ledger"),
                  "core": ("timing_core", "core"), "quantity": ("campaign", "quantity"),
                  "new_engine_file": ("timing_core", "added")}[changed]
    new[group][key] = "changed"
    if changed == "adapter":
        validate_source_transition(old, new)
    else:
        with pytest.raises(ValueError, match="ENGINE_CHANGED"):
            validate_source_transition(old, new)


@pytest.mark.parametrize("changed", ["sha256", "size", "missing"])
def test_successor_price_or_pit_component_changes_are_not_name_compatible(changed):
    from types import SimpleNamespace
    from backend.services.simulation_execution.localsim.timing_continuation import _same_component

    old = SimpleNamespace(components={"component": {"sha256": "a", "size": 100}})
    new = SimpleNamespace(components={"component": {"sha256": "a", "size": 100}})
    if changed == "missing":
        new.components.clear()
    else:
        new.components["component"][changed] = "different" if changed == "sha256" else 101
    with pytest.raises(ValueError, match="PRICE_INPUT_CHANGED"):
        _same_component(old, new, "component")


def successor_campaign_fixture(tmp_path, monkeypatch, *, prefix_accounts=12, changed_completed_state=False):
    """Two synthetic days; use real native orders/fees/ledger in all accounts."""
    from backend.services.strategy_package.models import StrategyPackageManifest
    from backend.services.simulation_execution.localsim import timing_continuation, timing_portfolio

    module, preflight, templates, Path = price_campaign_fixture(tmp_path, monkeypatch, two_days=True)
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: None)
    frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "output", producer_commit="1" * 40)
    request = module.FileRef.model_validate(frozen["request"])
    original = timing_portfolio.run_portfolio_day
    calls = []

    def stop_after_prefix(**kwargs):
        if len(calls) == prefix_accounts:
            raise OSError("controlled pre-adoption interruption")
        calls.append(kwargs["run_id"])
        return original(**kwargs)

    monkeypatch.setattr(timing_portfolio, "run_portfolio_day", stop_after_prefix)
    with pytest.raises(OSError, match="pre-adoption interruption"):
        module.run_continuous_campaign(request)
    monkeypatch.setattr(timing_portfolio, "run_portfolio_day", original)
    candidate = tmp_path / "repaired_candidate.json"
    module.publish(candidate, {"role": "REPAIRED_SYNTHETIC_CANDIDATE"})
    candidate_ref = module.FileRef.model_validate(module.ref(candidate))
    states = [{}, {("000001.SZ", "2026-09-21"): "FULL_DAY_SUSPENDED"} if changed_completed_state else {}]

    def inspected_successor(inspected, ref):
        module.checked_file(ref)
        return {"source_refs": [ref.model_dump()], "states": states, "stock_count": 2,
                "equivalent_price_component_count": 22, "pit_component_count": 1,
                "stage": "COMPLETE_SUCCESSOR_SOURCE_BATCH_INSPECTED"}

    monkeypatch.setattr(timing_continuation, "inspect_successor_sources", inspected_successor)
    return module, timing_continuation, request, candidate_ref, original


def test_adopted_successor_preserves_prefix_and_retries_same_twelve_accounts(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import timing_portfolio

    module, continuation, request, candidate, original = successor_campaign_fixture(tmp_path, monkeypatch)
    root = Path(request.path).parent
    prefix = {p: p.read_bytes() for p in (root / "days").glob("*/*.json")}
    assert len(prefix) == 12
    adopted = continuation.adopt_successor(request_ref=request, candidate_ref=candidate, producer_commit="2" * 40)
    assert adopted["preserved_account_days"] == 12
    assert adopted["resume_trade_date"] == "2026-09-22"
    replayed = []
    market_sources = []
    candidate_type = module.CandidateReader
    original_market = candidate_type.market

    def observe_market(self, symbol, day):
        market_sources.append(self.ref.sha256)
        return original_market(self, symbol, day)

    monkeypatch.setattr(candidate_type, "market", observe_market)

    def count_new_days(**kwargs):
        replayed.append(kwargs["run_id"])
        return original(**kwargs)

    monkeypatch.setattr(timing_portfolio, "run_portfolio_day", count_new_days)
    completed = module.run_continuous_campaign(request)
    assert len(replayed) == 12
    assert market_sources and set(market_sources) == {candidate.sha256}
    assert all(p.read_bytes() == body for p, body in prefix.items())
    result = module.read_json(Path(completed["results"]["path"]))
    assert result["producer_source_commit"] == "2" * 40
    assert result["original_producer_source_commit"] == "1" * 40
    assert result["input_successor"] == adopted["successor"]
    assert len(result["comparisons"]) == 36
    assert all(len(a["days"]) == 2 for a in result["accounts"])
    assert all(module.read_json(Path(a["days"][1]["path"]))["input_successor"] == adopted["successor"]
               for a in result["accounts"])
    retry = module.run_continuous_campaign(request)
    assert retry["status"] == "ALREADY_MATERIALIZED" and retry["results"] == completed["results"]
    again = continuation.adopt_successor(request_ref=request, candidate_ref=candidate, producer_commit="2" * 40)
    assert again["status"] == "ALREADY_ADOPTED" and again["successor"] == adopted["successor"]
    assert len(replayed) == 12
    # A committed post-cutover journal also restores its missing index without
    # another fill, just like the original day-atomic recovery contract.
    day_path = Path(result["accounts"][0]["days"][1]["path"])
    original_summary = day_path.read_bytes()
    day_path.unlink()
    recovered = module.run_continuous_campaign(request)
    assert recovered["results"] == completed["results"] and len(replayed) == 12
    assert day_path.read_bytes() == original_summary
    old_day = next(iter(prefix))
    old_day.unlink()
    recovered_prefix = module.run_continuous_campaign(request)
    assert recovered_prefix["results"] == completed["results"] and len(replayed) == 12
    assert old_day.read_bytes() == prefix[old_day]


@pytest.mark.parametrize("problem", ["completed_suspension", "incomplete_account_day"])
def test_successor_cannot_bypass_changed_or_partial_completed_prefix(tmp_path, monkeypatch, problem):
    from pathlib import Path

    module, continuation, request, candidate, _ = successor_campaign_fixture(
        tmp_path, monkeypatch, prefix_accounts=2 if problem == "incomplete_account_day" else 12,
        changed_completed_state=problem == "completed_suspension")
    root = Path(request.path).parent
    prefix = {p: p.read_bytes() for p in (root / "days").glob("*/*.json")}
    reason = "COMPLETED_INPUT_CHANGED" if problem == "completed_suspension" else "PARTIAL_OR_COMPLETE_PREFIX"
    with pytest.raises(ValueError, match=reason):
        continuation.adopt_successor(request_ref=request, candidate_ref=candidate, producer_commit="2" * 40)
    assert not (root / "input_successor.json").exists()
    assert all(p.read_bytes() == body for p, body in prefix.items())
    assert not (root / "continuous_results.json").exists()


@pytest.mark.parametrize("problem", ["candidate_drift", "prefix_drift", "reader_source_drift"])
def test_adopted_successor_refuses_drift_before_new_economic_writes(tmp_path, monkeypatch, problem):
    from pathlib import Path
    from backend.services.position_timing.intraday.store import IntradayError

    module, continuation, request, candidate, _ = successor_campaign_fixture(tmp_path, monkeypatch)
    continuation.adopt_successor(request_ref=request, candidate_ref=candidate, producer_commit="2" * 40)
    root = Path(request.path).parent
    if problem == "candidate_drift":
        p = Path(candidate.path)
        p.write_bytes(p.read_bytes() + b" ")
    elif problem == "prefix_drift":
        p = next((root / "days").glob("*/*.json"))
        p.write_bytes(p.read_bytes() + b" ")
    else:
        monkeypatch.setattr(continuation, "input_reader_identity", lambda: {"changed": True})
    with pytest.raises((ValueError, IntradayError), match="IDENTITY_MISMATCH|SOURCE_CHANGED"):
        module.run_continuous_campaign(request)
    assert len(list((root / "journals").glob("*/generations/00000001.json"))) == 12
    assert not (root / "continuous_results.json").exists()


def unknown_campaign_fixture(tmp_path, monkeypatch, problem=None):
    module, preflight, templates_ref, Path = price_campaign_fixture(tmp_path, monkeypatch)
    templates = module.read_json(Path(templates_ref.path))
    events = module.read_json(Path(templates["days"][0]["events"]["path"]))
    event = next(row for row in events if row["side"] == "SELL")
    declaration = {"symbol": event["symbol"], "trade_date": event["trade_date"], "sides": ["SELL"],
                   "availability": "UNKNOWN", "reason_code": "REFERENCE_NONFINITE",
                   "source_file": event["source_file"]}
    event["price_constraint"].update(mode="NONE", intervals=[])
    audit = {"schema_version": "position_timing_shared_price_reference_audit_v1",
             "price_policy_id": templates["price_policy_id"], "unknown_references": [declaration],
             "counts": {"BUY": 2, "SELL": 2, "AVAILABLE": 3, "UNKNOWN": 1},
             "stock_count": 2, "trade_day_count": 1, "outcomes_read": False}
    if problem == "undeclared":
        audit["unknown_references"] = []
    elif problem == "source":
        path = tmp_path / "other_source.json"
        module.publish(path, {"role": "OTHER_FUNCTIONAL_SOURCE"})
        declaration["source_file"] = module.ref(path)
    elif problem == "reason":
        declaration["reason_code"] = "REFERENCE_AVAILABLE"
    elif problem == "untyped_reason":
        declaration["reason_code"] = "IGNORE_MISSING_PRICE"
    elif problem == "counts":
        audit["counts"]["UNKNOWN"] = 0
    elif problem == "duplicate":
        audit["unknown_references"].append(dict(declaration))
    elif problem == "unused_declaration":
        audit["unknown_references"].append({**declaration, "symbol": "600519.SH"})
    elif problem == "available_empty":
        event["price_constraint"]["mode"] = "PREFERRED"
    elif problem == "none_interval":
        event["price_constraint"]["intervals"] = [{"lower": "9.8", "upper": "10.2"}]
    elif problem == "unknown_with_interval":
        event["price_constraint"].update(mode="PREFERRED", intervals=[{"lower": "9.8", "upper": "10.2"}])
    event_path = tmp_path / "unknown_events.json"
    audit_path = tmp_path / "reference_audit.json"
    policy_path = tmp_path / "unknown_policy.json"
    templates_path = tmp_path / "unknown_templates.json"
    module.publish(event_path, events)
    module.publish(audit_path, audit)
    policy = module.read_json(Path(templates["policy_document"]["path"]))
    policy["reference_audit"] = module.ref(audit_path)
    if problem == "audit_scope":
        policy["price_policy_id"] = "OTHER_POLICY"
    module.publish(policy_path, policy)
    templates["policy_document"] = module.ref(policy_path)
    templates["days"][0]["events"] = module.ref(event_path)
    module.publish(templates_path, templates)
    return module, preflight, module.FileRef.model_validate(module.ref(templates_path))


@pytest.mark.parametrize("problem", ["undeclared", "source", "reason", "untyped_reason", "counts", "duplicate",
                                     "unused_declaration", "available_empty", "none_interval", "unknown_with_interval", "audit_scope"])
def test_unknown_price_audit_must_match_exact_source_reason_and_population(tmp_path, monkeypatch, problem):
    module, preflight, templates = unknown_campaign_fixture(tmp_path, monkeypatch, problem)
    with pytest.raises(ValueError):
        module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                           output_root=tmp_path / "output", producer_commit="1" * 40)
    assert not (tmp_path / "output").exists()


def test_unused_unknown_sell_template_does_not_block_any_account_day(tmp_path, monkeypatch):
    from backend.services.strategy_package.models import StrategyPackageManifest

    module, preflight, templates = unknown_campaign_fixture(tmp_path, monkeypatch)
    inspected = module.inspect_price_templates(preflight, templates)
    assert inspected["reference_counts"]["UNKNOWN"] == 1
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: None)
    frozen = module.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "output", producer_commit="1" * 40)
    request = module.FileRef.model_validate(frozen["request"])
    completed = module.run_continuous_campaign(request)
    results = module.read_json(module.Path(completed["results"]["path"]))
    assert len(results["accounts"]) == 12
    for row in results["comparisons"]:
        assert row["unavailable_parent_count"] == 0
        assert row["parent_count"] == (2 if row["side"] != "SELL" else 0)
        assert (row["filled_parent_count"] + row["partial_parent_count"] + row["no_fill_parent_count"]
                == row["parent_count"])
    retry = module.run_continuous_campaign(request)
    assert retry["status"] == "ALREADY_MATERIALIZED" and retry["results"] == completed["results"]


@pytest.mark.parametrize("rule", ["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"])
def test_actual_unknown_sell_preserves_quantity_account_and_core_contract(tmp_path, rule):
    from backend.services.position_timing.intraday.price_plans import PriceReference, PricePlanPolicy, build_price_plan
    from backend.services.simulation_execution.localsim import timing_campaign as module
    from backend.services.simulation_execution.localsim.timing_portfolio import attach_native_quantity_evidence
    from backend.services.trading_core.models import PositionLot

    _, markets = pair()
    symbol = next(iter(markets))
    source_path = tmp_path / "reference.json"
    module.publish(source_path, {"role": "FUNCTIONAL_UNKNOWN_REFERENCE"})
    source_ref = module.FileRef.model_validate(module.ref(source_path))
    day = markets[symbol].trade_date
    built = build_price_plan(PriceReference(symbol=symbol, trade_date=day, reference_price_raw=None,
                                            source_file=source_ref, source_available_at=at("09:00:00"),
                                            reason_code="REFERENCE_NONFINITE"), "SELL", PricePlanPolicy(), at("09:00:00"))
    template = module.PriceTemplateEvent.model_validate(built.event)
    declaration = module.AuditedUnknownReference(symbol=symbol, trade_date=day, sides=("SELL",),
                                                  availability=built.availability, reason_code=built.reason_code,
                                                  source_file=source_ref)
    account = {**initial(), "cash_cny": "100000", "positions": {
        symbol: PositionLot(portfolio_id="functional_campaign", symbol=symbol, quantity=1000,
                            available_quantity=1000, avg_cost=10,
                            trade_date=day - timedelta(days=3)).model_dump(mode="json")}}
    projection = {"manifest_sha256_by_package": {"FUNCTIONAL_FIXTURE": "a" * 64},
                  "runtime_config": {"point_in_time_context": {"effective_trade_date": str(day), "cutoff_date": "2026-09-18"}},
                  "aggregate_results": [], "valid_no_candidate": True, "no_candidate_reason": "NO_ELIGIBLE_CANDIDATES"}
    prices = {s: Decimal("10") for s in markets}
    backend, targets, native = compile_native_intents(projection=projection, manifest=None, account=account,
                                                      markets=markets, prices=prices, cost=cost())
    assert len(native.order_intents) == 1 and native.order_intents[0].quantity == 1000
    events, unavailable = module.build_timing_plan_events(
        intents=native.order_intents, series={(symbol, "SELL"): [template]}, rule=rule,
        source_commit="1" * 40, day=day, quantity_source="f" * 64,
        unknown_references={(symbol, "SELL"): declaration})
    result = run_portfolio_day(backend=backend, markets=markets, events=events, initial_marks=prices,
                               delay_seconds=60, run_id="functional_campaign")
    attach_native_quantity_evidence(result, targets=targets, native=native, projection_ref={},
                                     price_policy_sha256="a" * 64, unavailable=unavailable)
    parent = result["account"]["parents"][0]
    assert parent["requested_quantity"] == parent["legal_quantity"] == 1000
    if rule == "PRICE_TOUCH_V1":
        assert parent["status"] == "UNAVAILABLE" and parent["filled_quantity"] == 0
        assert parent["order_created"] is False and not backend._records and not backend._ledger.fills
        assert parent["price_reference_audit"]["source_file"] == source_ref.model_dump()
        assert "REFERENCE_NONFINITE" in parent["reason_codes"]
        assert Decimal(result["account"]["terminal_nav_cny"]) == 110000
        assert backend._ledger.positions[symbol].quantity == 1000
    else:
        assert parent["status"] == "FILLED" and parent["filled_quantity"] == 1000
        assert len(result["sessions"]) == 1 and len(backend._ledger.fills) == 1
        assert result["sessions"][0].feedback(at("14:57:00")).confirmed_quantity == 1000
    store = TimingFileRepository(tmp_path / "journal", "functional_campaign")
    commit_portfolio_day(store=store, result=result, input_identity="f" * 64, day_index=0)
    assert store.latest()["checkpoint"]["account"]["parents"][0] == parent
    assert commit_portfolio_day(store=store, result=result, input_identity="f" * 64,
                                day_index=0)["status"] == "ALREADY_MATERIALIZED"
