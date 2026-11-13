"""Parent capacity and native interface evidence, never a historical replay."""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import Policy, digest, payload, utc
from backend.services.position_timing.intraday import dispatch_coupling_research as timing_contracts
from backend.services.position_timing.intraday.replay import FileRef, PlanEvent
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_execution.localsim import dispatch_capacity_research as study
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy, HumanOperationQueue
from backend.services.simulation_execution.localsim.parent_lane_operation import ParentLaneOperationQueue, capacity_control
from backend.services.simulation_execution.localsim.runtime import LocalSimBackend
from backend.services.simulation_execution.localsim.human_portfolio import run_human_portfolio_day
from backend.services.simulation_execution.localsim.timing_research import backend_for, ref
from backend.services.simulation_execution.localsim.timing_store import publish
from backend.services.position_timing.intraday.research import CostContract
from backend.services.simulation_execution.localsim.timing import policy_snapshot
from backend.services.trading_core.models import PositionLot
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs
from backend.tests.simulation_execution.test_localsim_human_operation import human_day, full_markets
from backend.tests.simulation_execution.test_localsim_human_opportunity_renewal import native_decisions
from backend.tests.simulation_execution import test_localsim_natural_period_research as natural_tests


def queue(*parents):
    return ParentLaneOperationQueue(account_id="account", policy=HumanOperationPolicy(reaction_delay_seconds=60),
        control=capacity_control(), parent_plan_ids=parents)


def enqueue(q, parent, *, revision=1, opportunity=None, side="BUY", kind="SUBMIT", visible="09:31:10", until="14:57:00"):
    return q.enqueue(plan_id=parent, revision=revision, opportunity_id=opportunity or parent, symbol=parent,
        side=side, kind=kind, visible_at=at(visible), valid_until=at(until))


def test_independent_lanes_stable_priority_and_one_attempt_per_clock():
    q = queue("buy_b", "buy_a", "sell", "cancel")
    for key, side, kind in (("buy_b", "BUY", "SUBMIT"), ("sell", "SELL", "SUBMIT"),
                            ("cancel", "BUY", "CANCEL"), ("buy_a", "BUY", "SUBMIT")):
        enqueue(q, key, side=side, kind=kind)
    assert q.claim(at("09:32:09"), in_trading_session=True) is None
    claims = []
    for _ in range(q.dispatch_limit):
        action = q.claim(at("09:32:10"), in_trading_session=True)
        claims.append(action.plan_id)
        q.acknowledge(action.action_id, {"status": "NO_ORDER"})
    assert claims == ["cancel", "sell", "buy_a", "buy_b"]
    assert q.claim(at("09:32:10"), in_trading_session=True) is None
    assert all(lane.last_action_at == at("09:32:10") for lane in q.lanes.values())


def test_revision_and_new_opportunity_do_not_create_free_operator_after_no_order():
    q = queue("plan")
    first = enqueue(q, "plan")
    q.claim(at("09:32:10"), in_trading_session=True)
    q.acknowledge(first.action_id, {"status": "NO_ORDER"})
    second = enqueue(q, "plan", revision=2, opportunity="new", visible="09:31:11")
    assert len(q.lanes) == 1
    assert q.claim(at("09:33:09"), in_trading_session=True) is None
    assert q.claim(at("09:33:10"), in_trading_session=True).action_id == second.action_id


def test_native_renewal_keeps_first_ready_and_terminal_action_is_not_revived():
    plan, decisions = native_decisions("09:31:10", "09:32:10", "09:33:10")
    q = queue(plan.plan_id)
    first = q.enqueue(plan_id=plan.plan_id, revision=1, opportunity_id=decisions[0].opportunity_id,
        symbol=plan.symbol, side=plan.side, visible_at=decisions[0].advice_available_at,
        valid_until=decisions[0].valid_until)
    for decision in decisions[1:]:
        renewed = q.revalidate_opportunity(first.action_id, decision=decision, as_of=decision.as_of,
            plan_valid_until=plan.valid_until)
        assert (renewed.visible_at, renewed.ready_at) == (first.visible_at, first.ready_at)
    q.invalidate_plan(plan.plan_id, reason="OPPORTUNITY_WITHDRAWN")
    renewed = q.revalidate_opportunity(first.action_id, decision=decisions[-1], as_of=decisions[-1].as_of,
        plan_valid_until=plan.valid_until)
    assert renewed.status == "INVALIDATED"
    assert q.claim(at("09:33:10"), in_trading_session=True) is None


def test_expiry_lunch_and_restore_preserve_native_capacity_and_inflight():
    q = queue("z", "a")
    enqueue(q, "z", visible="11:29:10", until="13:01:00")
    enqueue(q, "a", visible="11:29:10", until="13:02:00")
    assert q.claim(at("12:00:00"), in_trading_session=False) is None
    a = q.claim(at("13:00:00"), in_trading_session=True)
    snapshot = q.snapshot()
    restored = ParentLaneOperationQueue.restore(snapshot, policy=q.policy, control=q.control, parent_plan_ids=q.parents)
    assert restored.snapshot() == snapshot
    with pytest.raises(ValueError, match="ACK_REQUIRED"):
        restored.claim(at("13:00:00"), in_trading_session=True)
    restored.acknowledge(a.action_id, {"status": "REJECTED"})
    b = restored.claim(at("13:00:00"), in_trading_session=True)
    restored.acknowledge(b.action_id, {"status": "REJECTED"})
    assert restored.claim(at("13:02:00"), in_trading_session=True) is None
    with pytest.raises(ValueError, match="STATE_IDENTITY"):
        HumanOperationQueue.restore(snapshot)
    forged = deepcopy(restored.snapshot())
    forged["lanes"][0]["parent_plan_id"] = "unregistered"
    forged["state_sha256"] = digest({k: v for k, v in forged.items() if k != "state_sha256"})
    with pytest.raises(ValueError, match="POPULATION_CONFLICT"):
        ParentLaneOperationQueue.restore(forged, policy=q.policy, control=q.control, parent_plan_ids=q.parents)


@pytest.mark.parametrize("change", ["reaction_delay_seconds", "same_clock_matching_passes", "shared_cash_and_positions"])
def test_control_or_unknown_parent_fails_closed(change):
    control = capacity_control()
    control[change] = 0
    with pytest.raises(ValueError, match="CONTROL_CONFLICT"):
        ParentLaneOperationQueue(account_id="account", policy=HumanOperationPolicy(reaction_delay_seconds=60),
            control=control, parent_plan_ids=["plan"])
    with pytest.raises(ValueError, match="UNKNOWN_PARENT"):
        enqueue(queue("plan"), "other")


@pytest.mark.parametrize("rule", ["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"])
def test_default_serial_rules_keep_fixed_short_window_economics(rule):
    plan, market = inputs(rule)
    backend, result = human_day(plan, market)
    account = result["account"]
    assert len(backend._ledger.fills) == 1
    assert backend._ledger.fills[0].quantity == 1000
    assert backend._ledger.cash_decimal == Decimal("9989995")
    assert account["human_operation_state"]["schema_version"] == "localsim_human_operation_state_v1"
    action = account["human_operation_state"]["actions"][0]
    assert utc(action["dispatch_at"]) == at("09:47:10" if rule == "BOUNDED_REVERSION_V1" else "09:32:10")
    assert backend._ledger.fills[0].trade_time == at("09:48:00" if rule == "BOUNDED_REVERSION_V1" else "09:33:00")


def test_native_submission_does_not_reserve_cash_but_matching_limits_total_spend():
    plan, market = inputs()
    backend = LocalSimBackend(portfolio_id="fixture", initial_cash=10005, manifest=None,
        package_id="FUNCTIONAL_FIXTURE", data_source=MinuteDataSource.DATASET_FILES,
        execution_policy=policy_snapshot(), market_data_provider=market)
    first = backend.open_timing_portfolio_execution(plan=plan, market=market,
        delay_seconds=0, run_id="test", binding_id="fixture", submitted_at=at("09:31:10"))
    second = backend.open_timing_portfolio_execution(plan=plan.model_copy(update={"plan_id": "second"}), market=market,
        delay_seconds=0, run_id="test", binding_id="fixture", submitted_at=at("09:31:10"))
    assert first.order.quantity == second.order.quantity == 1000
    assert backend._ledger.cash_decimal == Decimal("10005")
    for session in (first, second):
        session.facade.evaluate(market, session.feedback(at("09:31:10")), at("09:31:10"))
        session.step(market.bars[2])
    assert sum(f.quantity for f in backend._ledger.fills) == 1000
    assert backend._ledger.cash_decimal == 0
    assert second.reasons == ["INSUFFICIENT_CASH"]


def test_native_matching_can_use_sale_proceeds_before_confirmation_is_visible():
    sell_plan, market = inputs(side="SELL")
    buy_plan = inputs()[0].model_copy(update={"plan_id": "buy"})
    lot = PositionLot(portfolio_id="fixture", symbol=sell_plan.symbol, quantity=1000,
        available_quantity=1000, avg_cost=10, trade_date=sell_plan.target_trade_date-timedelta(days=3))
    backend = LocalSimBackend(portfolio_id="fixture", initial_cash=100, manifest=None,
        package_id="FUNCTIONAL_FIXTURE", data_source=MinuteDataSource.DATASET_FILES,
        execution_policy=policy_snapshot(), market_data_provider=market, initial_positions={lot.symbol: lot})
    sell, buy = [backend.open_timing_portfolio_execution(plan=p, market=market, delay_seconds=0,
        run_id="test", binding_id="fixture", submitted_at=at("09:31:10")) for p in (sell_plan, buy_plan)]
    for session in (sell, buy):
        session.facade.evaluate(market, session.feedback(at("09:31:10")), at("09:31:10"))
    sell.step(market.bars[2])
    assert sell.order.filled_quantity == 1000
    assert sell.feedback(at("09:32:00")).cash_cny == 100  # Confirmation remains invisible.
    buy.step(market.bars[2])
    assert buy.order.filled_quantity == 1000  # Native boundary uses the ledger, not that visible budget.
    assert all(fill.trade_time == at("09:32:00") for fill in backend._ledger.fills)
    assert backend._ledger.cash_decimal >= 0


@pytest.mark.parametrize("optimized", [False, True])
def test_two_parent_dispatches_one_clock_one_native_bar_and_one_cash_book(monkeypatch, optimized):
    plans, markets = full_markets()
    plans = [p.model_copy(update={"policy_id": "BOUNDED_REVERSION_V1", "policy": Policy(
        policy_id="BOUNDED_REVERSION_V1", source_commit="1"*40), "policy_sha256": digest(Policy(
            policy_id="BOUNDED_REVERSION_V1", source_commit="1"*40))}) for p in plans]
    cost = CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")
    backend = backend_for({"portfolio_id": "account", "package_id": "fixture", "origin_initial_cash_cny": "10000000",
        "cash_cny": "10005", "positions": {}}, next(iter(markets.values())), cost)
    from backend.services.simulation_execution.localsim.timing import TimingExecution
    calls = set()
    step = TimingExecution.step
    def once(session, bar):
        key = (session.order.order_id, bar.start)
        assert key not in calls
        calls.add(key)
        return step(session, bar)
    monkeypatch.setattr(TimingExecution, "step", once)
    result = run_human_portfolio_day(backend=backend, markets=markets,
        events=[PlanEvent(available_at=p.valid_from, plan=p) for p in plans],
        initial_marks={s: Decimal("10") for s in markets}, policy=HumanOperationPolicy(reaction_delay_seconds=60),
        run_id="lane-day", optimized=optimized, research_capacity_control=capacity_control())
    state = result["account"]["human_operation_state"]
    dispatches = [utc(a["dispatch_at"]) for a in state["actions"] if a["kind"] == "SUBMIT" and a["dispatch_at"]]
    assert dispatches == [at("09:47:10")]*2
    assert sum(fill.quantity for fill in backend._ledger.fills) == 1000
    assert all(fill.trade_time > dispatches[0] for fill in backend._ledger.fills)
    assert backend._ledger.cash_decimal == 0
    assert all(p.available_quantity == 0 for p in backend._ledger.positions.values())
    ParentLaneOperationQueue.restore(state, policy=HumanOperationPolicy(reaction_delay_seconds=60),
        control=capacity_control(), parent_plan_ids={p.plan_id for p in plans})


@pytest.fixture
def capacity_study(tmp_path, monkeypatch):
    # Substitute historical admission only. Native targets, account, matching,
    # fees, queue and journals remain real. No old account is executed here.
    old_request, openings, _ = natural_tests.natural.__wrapped__(tmp_path, monkeypatch)
    original = study.read(old_request)
    old_sources = study.natural._sources
    delivery, contract, context, inspected, base_ref, _ = old_sources(None)
    context = {**context, "delivery": {"preflight": original["preflight"]}}
    def native_sources(source):
        assert source == FileRef.model_validate(original["source_delivery"])
        assert source != FileRef.model_validate(original["input_preparation_delivery"])
        return delivery, contract, context, inspected, base_ref, "70e459c569020f09880d91c1f16d43e9e5d951f3"
    monkeypatch.setattr(study.natural, "_sources", native_sources)
    monkeypatch.setattr(study, "CandidateReader", study.natural.CandidateReader)
    def put(name, value):
        path = tmp_path/"capacity-inputs"/name
        publish(path, value)
        return ref(path)
    contracts = put("contracts.json", timing_contracts._producer_contracts())
    evidence = put("funding-evidence.json", {"fixture": "native interface evidence"})
    funding = {**timing_contracts.funding_boundary(), "owner_gap_evidence": evidence}
    funding_sha = digest(funding)
    monkeypatch.setattr(study, "FUNDING_SHA", funding_sha)
    monkeypatch.setattr(study.lanes, "FUNDING_SHA", funding_sha)
    opening_ref = original["initial_accounts"][2]
    plan = {"schema_version": "position_timing_dispatch_capacity_plan_v2", "trade_dates": study.DATES,
        "native_target_count": 850, "new_account_days": 17, "reused_baseline_account_days": 17,
        "base_execution_contract": dual.execution_contract(), "policy": payload(Policy(policy_id="BOUNDED_REVERSION_V1",
            source_commit="70e459c569020f09880d91c1f16d43e9e5d951f3")), "capacity_control": capacity_control(),
        "capacity_control_sha256": digest(capacity_control()), "funding_boundary": funding,
        "funding_boundary_sha256": funding_sha, "producer_contracts": contracts,
        "operation": study.lanes.operation_envelope(HumanOperationPolicy(reaction_delay_seconds=60), capacity_control()),
        "common_opening": opening_ref, "candidate_account_id": "fixture_C2L", "source_delivery": original["input_preparation_delivery"],
        "package": context["package"], "preflight": original["preflight"], "baseline_producer_request": payload(old_request),
        "baseline_source_identity": original["source_identity"],
        **{k: original[k] for k in ("candidate_manifest", "selection_inventory", "price_templates", "cost_policy", "package_id")},
        "package_manifest_sha256": original["manifest_sha256"]}
    plan["policy_sha256"] = digest(plan["policy"])
    plan["execution_contract"] = {"schema_version": "localsim_dispatch_capacity_execution_contract_v1",
        **{k: plan[k] for k in ("base_execution_contract", "capacity_control", "capacity_control_sha256",
            "funding_boundary", "funding_boundary_sha256")}}
    plan["baseline_account"] = {"arm_id": "C2", "account_id": openings[2]["account_id"], "opening": opening_ref,
        "days": [put("baseline-"+d+".json", {"synthetic_readonly_baseline": d}) for d in study.DATES]}
    plan["source_refs"] = payload(study.pins(inspected["source_refs"], opening_ref, contracts, evidence, plan["baseline_account"]))
    plan["identity"] = digest(plan)
    plan_ref = put("plan.json", plan)
    frozen = {k: plan[k] for k in ("funding_boundary", "funding_boundary_sha256", "capacity_control_sha256", "policy_sha256",
        "candidate_manifest", "selection_inventory", "price_templates", "cost_policy", "common_opening", "producer_contracts")}
    frozen["plan"] = plan_ref
    source = put("delivery.json", {"schema_version": "position_timing_capacity_funding_delivery_receipt_v1",
        "public_sdk": {"plan_schema": "position_timing_dispatch_capacity_plan_v2"}, "frozen": frozen})
    monkeypatch.setattr(study, "inspect_dispatch_capacity_plan", lambda *_: {"plan": plan})
    regression = put("regression.json", {"status": "PASSED", "producer_source_commit": "8"*40,
        "baseline_producer_request": plan["baseline_producer_request"],
        "baseline_source_identity_sha256": digest(plan["baseline_source_identity"]),
        "base_execution_contract_sha256": digest(plan["base_execution_contract"]),
        "funding_boundary_sha256": funding_sha, "account_days_recomputed": 0, "checks": ["native fixture regression"]})
    prepared = study.prepare(source_delivery=FileRef.model_validate(source), output_root=tmp_path.with_name(tmp_path.name+"-capacity-output"),
        producer_commit="8"*40, legacy_serial_regression=FileRef.model_validate(regression))
    return FileRef.model_validate(prepared["request"]), plan, source


def test_v2_prepare_prefix_resume_17_native_days_and_retry_zero_account_calls(capacity_study, monkeypatch):
    request_ref, plan, _ = capacity_study
    request = study.read(request_ref)
    root = Path(request_ref.path).parent
    before = {Path(r.path): Path(r.path).read_bytes() for r in study.pins(plan["source_refs"])}
    first = study.run(request_ref, stop_after_account_days=1)
    assert first["computed_account_days"] == 1 and first["status"] == "CHECKPOINTED"
    assert study.inspect(request_ref, allow_prefix=True)["verified_account_days"] == 1
    finished = study.run(request_ref)
    assert finished["computed_account_days"] == 16 and finished["verified_account_days"] == 17
    facts = study.read(finished["facts"])
    assert facts["accounts"][0] == plan["baseline_account"]
    assert len(facts["accounts"][1]["days"]) == 17
    opening = study.read(request["initial_opening"])
    assert opening["analysis_anchor"] == study.read(plan["common_opening"])["analysis_anchor"]
    assert all(path.read_bytes() == raw for path, raw in before.items())
    def forbidden(*args, **kwargs):
        pytest.fail("artifact retry entered source preparation, market preparation or account execution")
    monkeypatch.setattr(study, "sources", forbidden)
    monkeypatch.setattr(study, "CandidateReader", forbidden)
    monkeypatch.setattr(dual, "_compute_account_day", forbidden)
    monkeypatch.setattr(dual, "execute_account_days", forbidden)
    retry = study.exact_artifact_retry(request_ref)
    sealed = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*.json")}
    assert study.exact_artifact_retry(request_ref) == retry
    assert study.run(request_ref)["status"] == "ALREADY_MATERIALIZED"
    assert retry["recomputed_account_days"] == retry["new_economic_commits"] == 0
    assert sealed == {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*.json")}


def test_missing_source_pin_and_source_drift_fail_before_any_account(capacity_study, monkeypatch):
    request_ref, plan, _ = capacity_study
    def forbidden(*args, **kwargs):
        pytest.fail("source failure reached account computation")
    monkeypatch.setattr(study, "account_refs", forbidden)
    path = Path(request_ref.path).parent/"preparation.json"
    old = path.read_bytes()
    row = json.loads(old)
    row["source_refs"] = [r for r in row["source_refs"] if r != plan["candidate_manifest"]]
    path.write_text(json.dumps(row), encoding="utf-8")
    with pytest.raises(ValueError, match="MISSING_SOURCE_PIN"):
        study.inspect(request_ref)
    path.write_bytes(old)
    candidate = Path(plan["candidate_manifest"]["path"])
    candidate.write_bytes(candidate.read_bytes()+b" ")
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY"):
        study.inspect(request_ref)


@pytest.mark.parametrize("field,value", [("schema_version", "localsim_natural_period_request_v1"),
    ("account_id", "old_C2"), ("new_capital_added_cny", "1")])
def test_request_cannot_alias_old_scope_or_add_capital(capacity_study, monkeypatch, field, value):
    request_ref, _, _ = capacity_study
    row = study.read(request_ref)
    row[field] = value
    Path(request_ref.path).write_text(json.dumps(row), encoding="utf-8")
    def forbidden(*args, **kwargs):
        pytest.fail("invalid request reached market or account preparation")
    monkeypatch.setattr(study, "sources", forbidden)
    from jsonschema import ValidationError
    with pytest.raises((ValueError, ValidationError)):
        study.run(FileRef.model_validate(ref(Path(request_ref.path))))


def test_v1_plan_or_rehashed_funding_drift_is_never_an_implicit_upgrade(capacity_study):
    _, plan, _ = capacity_study
    older = {**plan, "schema_version": "position_timing_dispatch_capacity_plan_v1"}
    with pytest.raises(ValueError, match="PLAN_SCOPE"):
        study.plan_scope(older)
    changed = deepcopy(plan)
    changed["funding_boundary"]["predispatch_cash_reservation"] = "INVENTED"
    changed["funding_boundary_sha256"] = digest(changed["funding_boundary"])
    with pytest.raises(ValueError, match="EXECUTION_IDENTITY"):
        study.plan_scope(changed)


def test_large_source_membership_normalizes_batch_once(tmp_path, monkeypatch):
    refs = [FileRef(path=str(tmp_path/(str(i)+".json")), sha256=f"{i:064x}") for i in range(12000)]
    original = study.pins
    calls = []
    def counting(*args):
        calls.append(1)
        return original(*args)
    monkeypatch.setattr(study, "pins", counting)
    assert study.contains_pins(refs, [payload(r) for r in reversed(refs)])
    assert len(calls) == 1
    assert not study.contains_pins([FileRef(path=str(tmp_path/"absent"), sha256="e"*64)], refs)


def test_sealed_producer_readonly_retry_preserves_artifacts_and_never_allows_run(capacity_study, tmp_path, monkeypatch):
    request_ref, _, _ = capacity_study
    study.run(request_ref)
    original_retry = study.exact_artifact_retry(request_ref)
    root = Path(request_ref.path).parent
    frozen = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*.json")}
    request = study.read(request_ref)
    relative = "backend/services/simulation_execution/localsim/dispatch_capacity_research.py"
    producer_root = tmp_path/"sealed-producer"
    source = producer_root/relative
    source.parent.mkdir(parents=True)
    raw = Path(study.__file__).read_bytes()
    source.write_bytes(raw)
    normalized = raw.replace(b"\r\n", b"\n")
    blob = b"blob "+str(len(normalized)).encode()+b"\0"+normalized
    seal = {"schema_version": "localsim_windows_to_wsl_source_seal_v1", "producer_source_commit": "8"*40,
        "validated_against_git_blobs": True, "worktree_windows": str(producer_root), "worktree_wsl": str(producer_root),
        "files": [{"path": relative, "sha256": hashlib.sha256(normalized).hexdigest(),
            "windows_raw_sha256": hashlib.sha256(raw).hexdigest(),
            "git_blob": hashlib.sha1(blob, usedforsecurity=False).hexdigest()}]}
    seal_path = tmp_path/"producer-seal.json"
    publish(seal_path, seal)
    current = deepcopy(request["source_identity"])
    current["capacity_and_immutable_helpers"]["dispatch_capacity_research.py"] = "f"*64
    monkeypatch.setattr(study, "implementation_identity", lambda: current)
    def forbidden(*args, **kwargs):
        pytest.fail("sealed read-only retry reached economics or market preparation")
    monkeypatch.setattr(study, "sources", forbidden)
    monkeypatch.setattr(study, "CandidateReader", forbidden)
    monkeypatch.setattr(dual, "execute_account_days", forbidden)
    with pytest.raises(ValueError, match="REQUEST_IDENTITY"):
        study.run(request_ref)
    seal_ref = FileRef.model_validate(ref(seal_path))
    readback = study.exact_artifact_retry(request_ref, producer_source_seal=seal_ref)
    assert all(readback[k] == v for k, v in original_retry.items())
    assert readback["producer_source_seal"] == payload(seal_ref)
    assert readback["inspection_reader_source_identity"] == current
    assert frozen == {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*.json")}
    current["native_engine"]["dual_mode"]["human_portfolio.py"] = "e"*64
    with pytest.raises(ValueError, match="READONLY_NATIVE_IDENTITY_DRIFT"):
        study.inspect(request_ref, producer_source_seal=seal_ref)
