"""Performance paths must preserve causal account and native economic facts."""

from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
from functools import partial
import json
import re

import pytest
from pydantic import BaseModel

from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.replay import PlanEvent
from backend.services.position_timing.intraday.research import CostContract
from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy
from backend.services.simulation_execution.localsim.human_portfolio import run_human_portfolio_day
from backend.services.simulation_execution.localsim.models import LocalSimExecutionStateV1
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions, VisibleAccountIndex
from backend.services.simulation_execution.localsim.timing_research import backend_for
from backend.services.simulation_runtime.models import TradingRuleDecision
from backend.services.trading_core.models import Fill, Order, OrderEvent
from backend.services.trading_core.oms import OMS
from backend.tests.simulation_execution.test_localsim_human_operation import full_markets
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs


def economic_account(value):
    """Keep trade/dispatch clocks; validate and separate native processing clocks.

    Native processing clocks and OMS cancel/reject UUIDs differ across fresh
    computations. Trade/dispatch clocks, deterministic ids, event multiplicity
    and parent links remain compared; original receipts stay untouched.
    """
    if isinstance(value, BaseModel):
        return economic_account(value.model_dump(mode="json"))
    if isinstance(value, (list, tuple)):
        return [economic_account(item) for item in value]
    if not isinstance(value, dict):
        return value
    event_keys = {"event_id", "event_type", "order_id", "event_time"}
    if value and all(isinstance(v, dict) and event_keys.issubset(v) and k == v["event_id"]
                     for k, v in value.items()):
        events = [economic_account(v) for v in value.values()]
        return {"__native_order_event_multiset__": sorted(
            events, key=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")))}
    excluded = set()
    if {"state_id", "state_hash", "runtime_status"}.issubset(value):
        LocalSimExecutionStateV1.model_validate(value)
        excluded = {"created_at", "updated_at", "state_hash"}
    elif event_keys.issubset(value):
        OrderEvent.model_validate(value)
        excluded = {"event_time"}
        if (value["event_type"] in {"CANCELLED", "REJECTED"} and value.get("fill") is None
                and re.fullmatch(r"evt_[0-9a-f]{32}", value["event_id"])):
            excluded.add("event_id")
    elif {"order_id", "filled_quantity", "created_at", "updated_at"}.issubset(value):
        excluded = {"updated_at"}
    elif {"decision_id", "decision_hash", "requested_quantity", "legal_quantity", "created_at"}.issubset(value):
        TradingRuleDecision.model_validate(value)
        excluded = {"created_at"}
    return {key: economic_account(item) for key, item in value.items() if key not in excluded}


def first_difference(left, right, path="result"):
    if isinstance(left, dict) and isinstance(right, dict):
        for key in left.keys() | right.keys():
            if key not in left or key not in right:
                return path + "." + key, "missing key"
            if left[key] != right[key]:
                return first_difference(left[key], right[key], path + "." + key)
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return path, len(left), len(right)
        for index, (a, b) in enumerate(zip(left, right)):
            if a != b:
                return first_difference(a, b, f"{path}[{index}]")
    return path, str(left)[:300], str(right)[:300]


def human_result(*, rule="FIRST_OBSERVABLE_V1", delay=0, optimized=False):
    plans, markets = full_markets()
    prototype, _ = inputs(rule)
    plans = [prototype.model_copy(update={"plan_id": plan.plan_id, "symbol": plan.symbol}) for plan in plans]
    cost = CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")
    backend = backend_for({"portfolio_id": "account", "package_id": "fixture",
                           "origin_initial_cash_cny": "10000000", "cash_cny": "10000000", "positions": {}},
                          markets[plans[0].symbol], cost)
    result = run_human_portfolio_day(backend=backend, markets=markets,
        events=[PlanEvent(available_at=plan.valid_from, plan=plan) for plan in plans],
        initial_marks={symbol: Decimal("10") for symbol in markets},
        policy=HumanOperationPolicy(reaction_delay_seconds=delay), run_id="performance-oracle", optimized=optimized)
    return backend, result


@pytest.mark.parametrize("rule", ["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"])
@pytest.mark.parametrize("delay", [0, 60, 180, 300])
def test_optimized_human_preserves_complete_daily_account_queue_and_nav(rule, delay):
    old_backend, old = human_result(rule=rule, delay=delay)
    new_backend, new = human_result(rule=rule, delay=delay, optimized=True)
    assert economic_account(payload(new["account"])) == economic_account(payload(old["account"]))
    assert new_backend._ledger.cash_decimal == old_backend._ledger.cash_decimal
    assert [payload(fill) for fill in new_backend._ledger.fills] == [payload(fill) for fill in old_backend._ledger.fills]
    if rule == "FIRST_OBSERVABLE_V1" and delay == 0:
        assert sum(fill.quantity for fill in new_backend._ledger.fills) == 2000
        assert new_backend._ledger.cash_decimal == Decimal("9979990")


def test_index_never_relabels_future_account_and_retains_equal_clock_order():
    opening = {"cash_cny": "100", "positions": {}}
    later = {"cash_cny": "50", "positions": {}}
    duplicate = {"cash_cny": "1", "positions": {}}
    views = [(at("09:30:00"), opening)]
    index = VisibleAccountIndex(views, "account")
    assert index.visible(at("09:30:00"))[1] is opening
    original = index.initial_feedback_id(1)
    assert original == digest(["account", opening, 1])
    assert index.merged_feedback_id(original) == digest([original, opening])
    views.extend([(at("09:32:05"), later), (at("09:32:05"), duplicate)])
    assert index.visible(at("09:32:04"))[1] is opening
    assert index.initial_feedback_id(1) == original
    assert index.visible(at("09:32:05"))[1] is later
    assert index.initial_feedback_id(1) == digest(["account", later, 1])
    assert index.visible(at("09:31:00"))[1] is opening
    with pytest.raises(ValueError, match="NOT_YET_VISIBLE"):
        index.visible(at("09:29:59"))
    views.append((at("09:31:00"), duplicate))
    with pytest.raises(ValueError, match="OBSERVATIONS_REVERSED"):
        index.visible(at("10:00:00"))


def test_lightweight_native_valuation_preserves_confirmed_ledger_and_observation_delay():
    backend, result = human_result(optimized=True)
    initial = result["account"]["initial_account"]
    current = {"cash_cny": str(backend._ledger.cash_decimal),
               "positions": {s: p.model_dump(mode="json") for s, p in backend._ledger.positions.items()}}
    index = VisibleAccountIndex([(at("09:30:00"), initial), (at("10:00:00"), current)])
    before = deepcopy(backend._ledger.__dict__)
    marks = {symbol: Decimal("12.34") for symbol in current["positions"]}
    assert index.nav(backend._ledger, marks=marks, clock=at("09:59:59")) == 10000000
    expected = backend._ledger.account_snapshot(prices={s: float(p) for s, p in marks.items()}, snapshot_time=at("10:00:00")).nav
    assert index.nav(backend._ledger, marks=marks, clock=at("10:00:00")) == expected
    assert index.nav(backend._ledger, marks=marks, clock=at("10:00:00") + timedelta(minutes=1)) == expected
    assert backend._ledger.__dict__ == before


@pytest.mark.parametrize("kwargs", [{"batch_days": True}, {"batch_days": 0}, {"batch_days": 21},
                                    {"account_workers": True}, {"account_workers": 0}, {"account_workers": 5},
                                    {"optimized": 1}])
def test_resource_options_reject_ambiguous_or_unbounded_configuration(kwargs):
    with pytest.raises(ValueError, match="PERFORMANCE_OPTIONS_INVALID"):
        ResearchPerformanceOptions(**kwargs)


def test_parallel_accounts_resume_same_native_prefix_and_exact_retry_without_workers(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.research_performance import AccountComputations, ResearchAccountPool
    from backend.services.simulation_execution.localsim.timing_store import read_json
    from backend.services.strategy_package.models import StrategyPackageManifest
    from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture

    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    manifest = make_paper_enabled_manifest().model_copy(update={"package_id": "FUNCTIONAL_FIXTURE"})
    # The existing source fixture has no model assets. Pass its declared valid
    # execution manifest to the child; matching, fees and ledger are not mocked.
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: manifest)
    original_result = AccountComputations.result
    checked = []
    def compare_with_serial(self, index):
        actual = original_result(self, index)
        baseline = dual._compute_account_day({**self.jobs[index], "optimized": False})
        def economic_commit(result):
            request = result["commit_request"]
            assert request.economic_facts["account_sha256"] == digest(result["account"])
            value = asdict(request)
            value["economic_facts"].pop("account_sha256")  # Derived from the separately validated audit clocks.
            return economic_account(value)
        actual_facts, expected_facts = economic_commit(actual), economic_commit(baseline)
        assert actual_facts == expected_facts, first_difference(actual_facts, expected_facts)
        checked.append(index)
        return actual
    monkeypatch.setattr(AccountComputations, "result", compare_with_serial)
    options = ResearchPerformanceOptions(account_workers=2, optimized=True)
    first = dual.run_study(request, performance=options, stop_after_account_days=1)
    assert first["status"] == "CHECKPOINTED" and first["computed_account_days"] == 1
    root = Path(request.path).parent
    prefix = {p: p.read_bytes() for p in (root / "journals").glob("*/generations/*.json")}
    assert len(prefix) == 1
    done = dual.run_study(request, performance=options)
    assert done["status"] == "MATERIALIZED" and done["computed_account_days"] == 3
    assert all(p.read_bytes() == raw for p, raw in prefix.items())
    result = read_json(Path(done["results"]["path"]))
    assert len(result["accounts"]) == 2 and all(len(a["days"]) == 2 for a in result["accounts"])
    assert {a["operation"]["operation_mode"] for a in result["accounts"]} == {"NATIVE", "HUMAN_SIM"}
    assert result["database_accessed"] is False and result["production_account_created"] is False
    def unexpected_pool():
        raise AssertionError("an exact retry must not start any account workers")
    monkeypatch.setattr(ResearchAccountPool, "start", unexpected_pool)
    retry = dual.run_study(request, performance=options)
    assert retry["status"] == "ALREADY_MATERIALIZED" and retry["computed_account_days"] == 0
    assert retry["results"] == done["results"]
    assert len(checked) == 4


def test_parallel_worker_failure_preserves_ordered_prefix_and_native_recovery(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.research_performance import AccountWorkerError, ResearchAccountPool
    from backend.services.simulation_execution.localsim.timing_store import read_json
    from backend.services.strategy_package.models import StrategyPackageManifest
    from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture

    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    manifest = make_paper_enabled_manifest().model_copy(update={"package_id": "FUNCTIONAL_FIXTURE"})
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: manifest)
    original_prepare = ResearchAccountPool.prepare
    def fail_second(self, jobs):
        jobs = list(jobs)
        if jobs[1] is not None:
            jobs[1] = deepcopy(jobs[1])
            jobs[1]["initial"]["rule"] = "INVALID_WORKER_INPUT"
        return original_prepare(self, jobs)
    options = ResearchPerformanceOptions(account_workers=2, optimized=True)
    with monkeypatch.context() as fault:
        fault.setattr(ResearchAccountPool, "prepare", fail_second)
        with pytest.raises(AccountWorkerError) as error:
            dual.run_study(request, performance=options)
    root = Path(request.path).parent
    prefix = {p: p.read_bytes() for p in (root / "journals").glob("*/generations/*.json")}
    assert len(prefix) == 1 and not (root / "results.json").exists()
    failure = read_json(next((root / "operational").glob("*.json")))
    assert failure["generation_file_count"] == 1
    assert failure["exception_type"] == error.value.native_exception_type
    assert failure["economic_result_published"] is False
    recovered = dual.run_study(request, performance=options)
    assert recovered["computed_account_days"] == 3
    assert all(p.read_bytes() == raw for p, raw in prefix.items())


def test_batch_request_without_formal_shared_interface_is_explicitly_unavailable(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture
    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    with pytest.raises(ValueError, match="SHARED_BATCH_PREPARATION_UNAVAILABLE"):
        dual.run_study(request, performance=ResearchPerformanceOptions(batch_days=5, optimized=True))
    assert not list((Path(request.path).parent / "journals").glob("*/generations/*.json"))


def test_batch_consumer_keeps_daily_market_contract_and_releases_owner_workspace(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.timing_store import read_json
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture
    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    original_reader = dual.CandidateReader
    preparations, consumed_days, closed = [], [], []
    class OwnerReaderContractFixture(original_reader):
        def prepare_window(self, start, end, symbols):
            preparations.append((start.isoformat(), end.isoformat(), tuple(symbols)))
        def market(self, symbol, day):
            market = super().market(symbol, day)
            assert market.trade_date == day
            consumed_days.append(day.isoformat())
            return market
        def close(self):
            closed.append(True)
    monkeypatch.setattr(dual, "CandidateReader", OwnerReaderContractFixture)
    done = dual.run_study(request, performance=ResearchPerformanceOptions(batch_days=2, optimized=True),
                          measure_performance=True)
    result = read_json(Path(done["results"]["path"]))
    dates = result["trade_dates"]
    assert len(dates) == 2 and set(consumed_days) == set(dates)
    assert len(preparations) == 1 and preparations[0][:2] == (dates[0], dates[-1])
    assert closed == [True]
    assert done["performance"]["batch_days"] == 2
    assert done["performance"]["account_compute_seconds_sum"] > 0
    assert "performance" not in result  # Timing noise cannot change economic artifacts.


@pytest.mark.parametrize("delay", [0, 60, 180, 300])
def test_optimized_future_suffix_retains_existing_causal_oracle(monkeypatch, delay):
    from backend.tests.simulation_execution import test_localsim_human_operation as native_oracles
    monkeypatch.setattr(native_oracles, "run_human_portfolio_day", partial(run_human_portfolio_day, optimized=True))
    native_oracles.test_future_suffix_never_changes_earlier_human_dispatch(delay)


def test_optimized_partial_cancel_cash_and_native_fact_replay_oracles(tmp_path, monkeypatch):
    from backend.tests.simulation_execution import test_localsim_human_operation as native_oracles
    monkeypatch.setattr(native_oracles, "run_human_portfolio_day", partial(run_human_portfolio_day, optimized=True))
    native_oracles.test_partial_fill_multiple_revisions_cancel_once_then_only_confirmed_remaining()
    native_oracles.test_cash_and_sellable_limits_do_not_turn_opportunity_into_a_fill()
    native_oracles.test_concurrent_partial_fills_follow_native_cash_fact_replay_order(tmp_path)


def test_worker_account_scope_mismatch_never_reaches_native_economic_commit(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.research_performance import AccountComputations
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture
    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2", two_days=False)
    original = AccountComputations.result
    def borrowed_account(self, index):
        value = original(self, index)
        value["account"]["portfolio_id"] = "another-account"
        return value
    monkeypatch.setattr(AccountComputations, "result", borrowed_account)
    with pytest.raises(ValueError, match="WORKER_ACCOUNT_SCOPE_CONFLICT"):
        dual.run_study(request)
    assert not list((Path(request.path).parent / "journals").glob("*/generations/*.json"))


def test_native_cancel_uuid_separation_preserves_order_links_and_reason():
    order = Order(order_id="oracle-order", intent_id="oracle-intent", package_id="fixture",
                  portfolio_id="account", symbol="600000.SH", side="BUY", quantity=1000,
                  order_type="LIMIT", limit_price=10, status="SUBMITTED", created_at=at("09:30:00"))
    _, first = OMS().cancel_order(order, "HUMAN_CANCEL_CONFIRMED")
    _, second = OMS().cancel_order(order, "HUMAN_CANCEL_CONFIRMED")
    assert first.event_id != second.event_id
    assert economic_account(first) == economic_account(second)
    assert economic_account({first.event_id: first.model_dump(mode="json")}) == economic_account(
        {second.event_id: second.model_dump(mode="json")})
    assert economic_account(first) != economic_account(second.model_copy(update={"order_id": "wrong-parent"}))
    assert economic_account(first) != economic_account(second.model_copy(update={"reason": "PLAN_HARD_EXPIRY"}))


def test_native_event_multiset_keeps_duplicate_count_and_deterministic_cancel_identity():
    first = OrderEvent(order_id="oracle-order", event_type="CANCELLED", reason="confirmed")
    second = OrderEvent(order_id="oracle-order", event_type="CANCELLED", reason="confirmed")
    one = {first.event_id: first.model_dump(mode="json")}
    two = {**one, second.event_id: second.model_dump(mode="json")}
    assert economic_account(one) != economic_account(two)
    stable_a = first.model_copy(update={"event_id": "evt_deterministic_a"})
    stable_b = first.model_copy(update={"event_id": "evt_deterministic_b"})
    assert economic_account(stable_a) != economic_account(stable_b)


def test_economic_comparison_never_discards_fill_ids_trade_clock_quantity_or_price():
    fill = Fill(fill_id="oracle-fill", order_id="oracle-order", symbol="600000.SH", side="BUY",
                quantity=100, price=10, trade_time=at("09:33:00"), bar_time=at("09:33:00"), reason="NEXT_ELIGIBLE_BAR_OPEN")
    event = OrderEvent(event_id="evt_oracle_fill", order_id="oracle-order", event_type="FILLED", fill=fill)
    variants = [event.model_copy(update={"event_id": "evt_other_fill"})]
    for updates in ({"fill_id": "wrong-fill"}, {"trade_time": at("09:34:00")}, {"quantity": 200}, {"price": 11}):
        variants.append(event.model_copy(update={"fill": fill.model_copy(update=updates)}))
    for changed in variants:
        assert economic_account(event) != economic_account(changed)


@pytest.mark.parametrize("optimized", [False, True])
def test_same_frozen_research_keeps_logical_day_identity_across_output_roots(tmp_path, monkeypatch, optimized):
    from pathlib import Path
    from backend.services.position_timing.intraday.replay import FileRef
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.timing_research import ref
    from backend.services.simulation_execution.localsim.timing_store import read_json
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture

    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    other = tmp_path / "independent-output" / Path(request.path).parent.name / "request.json"
    other.parent.mkdir(parents=True)
    other.write_bytes(Path(request.path).read_bytes())
    other_ref = FileRef.model_validate(ref(other))
    assert request.sha256 == other_ref.sha256
    options = ResearchPerformanceOptions(optimized=optimized)
    left = read_json(Path(dual.run_study(request, performance=options)["results"]["path"]))
    right = read_json(Path(dual.run_study(other_ref, performance=options)["results"]["path"]))
    assert len(left["trade_dates"]) == len(right["trade_dates"]) == 2
    assert len(left["accounts"]) == len(right["accounts"]) == 2

    def complete_business(day, native):
        return economic_account({"day": {k: v for k, v in day.items()
            if k not in {"account_receipt", "previous_day"}}, "facts": native["facts"], "states": native["states"]})

    for a, b in zip(left["accounts"], right["accounts"]):
        assert a["account_id"] == b["account_id"]
        assert len(a["days"]) == len(b["days"]) == 2
        previous = [None, None]
        for ar, br in zip(a["days"], b["days"]):
            da, db = read_json(Path(ar["path"])), read_json(Path(br["path"]))
            assert da["previous_day"] == previous[0] and db["previous_day"] == previous[1]
            ra = read_json(Path(da["account_receipt"]["path"]))["payload"]
            rb = read_json(Path(db["account_receipt"]["path"]))["payload"]
            assert ra["run_id"] == rb["run_id"], "physical output paths must not change logical day identity"
            assert complete_business(da, ra) == complete_business(db, rb)
            previous = [ar, br]


def test_day_identity_binds_execution_inputs_and_ignores_audit_only_fields():
    from backend.services.simulation_execution.localsim import dual_mode_research as dual

    prior = {"cash_cny": "10000000", "positions": {}, "marks_raw_cny": {"000001.SZ": "10"}}
    args = {"request_identity": "a" * 64, "account_id": "account-a", "trade_date": "2026-01-06", "prior": prior}
    baseline = dual._account_day_identity(**args)
    for changed in ({"request_identity": "b" * 64}, {"account_id": "account-b"}, {"trade_date": "2026-01-07"}):
        assert dual._account_day_identity(**{**args, **changed}) != baseline
    for changed in ({"cash_cny": "9999999"}, {"positions": {"000001.SZ": {"quantity": 100}}},
                    {"marks_raw_cny": {"000001.SZ": "10.01"}}):
        assert dual._account_day_identity(**{**args, "prior": {**prior, **changed}}) != baseline
    audit_only = {"previous_day": {"path": "/other/output.json", "sha256": "c" * 64},
                  "human_operation_metrics": {"maximum_queue": 99}, "created_at": "2026-10-28T00:00:00Z"}
    assert dual._account_day_identity(**{**args, "prior": {**prior, **audit_only}}) == baseline


def test_path_independent_day_identity_still_rejects_tampered_committed_account(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.simulation_execution.localsim import dual_mode_research as dual
    from backend.services.simulation_execution.localsim.timing_store import read_json
    from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture

    request, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    paused = dual.run_study(request, stop_after_account_days=1)
    assert paused["status"] == "CHECKPOINTED"
    root = Path(request.path).parent
    generations = {p: p.read_bytes() for p in (root / "journals").glob("*/generations/*.json")}
    day_path = next((root / "days").glob("*/*.json"))
    day = read_json(day_path)
    day["cash_cny"] = str(Decimal(day["cash_cny"]) + 1)
    day_path.write_text(json.dumps(day), encoding="utf-8")
    with pytest.raises(ValueError, match="LOCALSIM_TIMING_IMMUTABLE_ARTIFACT_CONFLICT"):
        dual.run_study(request)
    assert all(p.read_bytes() == body for p, body in generations.items())
    assert not (root / "results.json").exists()
