"""Capacity consumer contracts, not a synthetic account-engine acceptance."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import dispatch_coupling_research as study
from backend.services.position_timing.intraday import dispatch_coupling_results as results
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError


def day_view(day, account, opening, identity, *, candidate):
    clock = datetime.fromisoformat(day+"T09:31:00+08:00")
    parents, actions = [], []
    for i, side in enumerate(("BUY", "SELL")):
        dispatch = clock+timedelta(seconds=60 if candidate else 60*(i+1))
        plan = side.lower()+"_plan"
        actions.append(dict(action_id=plan+"_action", plan_id=plan, revision=1, opportunity_id="a"*64,
            symbol="000001.SZ", side=side, kind="SUBMIT", status="COMPLETED", visible_at=clock.isoformat(),
            ready_at=(clock+timedelta(seconds=60)).isoformat(), dispatch_at=dispatch.isoformat(),
            valid_until=(clock+timedelta(seconds=300)).isoformat(), outcome={"status": "SUBMITTED"}))
        parents.append(dict(plan_id=plan, symbol="000001.SZ", side=side, status="FILLED", target_quantity=100,
            filled_quantity=100, average_price_raw="10", research_timing={"first_dispatch_at": dispatch.isoformat(),
            "first_fill_at": (dispatch+timedelta(minutes=1)).isoformat(), "signal_revisions": [{"revision": 1,
                "batch_started_at": (clock-timedelta(minutes=1)).isoformat(), "first_outer_opportunity_at": clock.isoformat()}]}))
    state = {"schema_version": "localsim_human_operation_state_v1", "actions": actions}
    if candidate:
        state.update(schema_version="localsim_parent_lane_operation_state_v1", account_id=account,
            capacity_control_sha256=digest(study.capacity_control()), lanes=[{"parent_plan_id": a["plan_id"],
                "action_ids": [a["action_id"]], "last_action_at": a["dispatch_at"]} for a in actions])
        state["state_sha256"] = digest(state)
    return dict(schema_version="localsim_dispatch_capacity_day_v1" if candidate else "localsim_natural_period_day_v1",
        account_id=account, study_identity=identity, rule="BOUNDED_REVERSION_V1", delay_seconds=60,
        operation=opening["operation"], trade_date=day, terminal_as_of=day+"T14:57:00+08:00",
        terminal_nav_cny="10000000", fees_cny="8" if candidate else "5", cash_cny="1000", positions=opening["positions"],
        fees_by_side_cny={"BUY": "4" if candidate else "2", "SELL": "4" if candidate else "3"},
        nav_observations=[], parents=parents, human_operation_state=state, view_counts={"trades": 2})


@pytest.fixture
def source(tmp_path, monkeypatch):
    """Stub only the already-tested old four-arm preflight, not the new checks."""
    def put(name, value):
        return freeze(tmp_path/name, value)
    monkeypatch.setattr(study, "FUNDING_EVIDENCE", payload(put("funding-evidence.json", {"fixture": "owner contract gap"})))
    refs = {k: put(k+".json", {"fixture": k}) for k in ("candidate_manifest", "selection_inventory", "cost_policy", "price_templates")}
    operation = {"schema_version": "localsim_operation_snapshot_v1", "operation_mode": "HUMAN_SIM",
        "human_policy": {"schema_version": "localsim_human_operation_policy_v1", "policy_id": "localsim_human_operation_v1",
            "operator_scope": "ACCOUNT", "reaction_delay_seconds": 60, "minimum_action_interval_seconds": 60,
            "maximum_actions_per_dispatch": 1, "queue_order": "CANCEL_THEN_SELL_THEN_BUY_READY_FIFO", "allow_split": False,
            "automatic_reprice": False, "new_order_requires_known_account": True, "random_response_model": "disabled"}}
    opening = dict(schema_version="localsim_natural_period_opening_v1", account_id=study.BASELINE_ID, arm_id="C2",
        parent_account_id="september_c2", rule="BOUNDED_REVERSION_V1", delay_seconds=60, operation=operation,
        opening_nav_cny="10000000", origin_initial_cash_cny="10000000", new_capital_added_cny="0", cash_cny="1000",
        positions={"000001.SZ": {"portfolio_id": study.BASELINE_ID, "quantity": 100, "available_quantity": 100}},
        opening_marks_raw_cny={"000001.SZ": "10", "unused_nonposition_mark": "20"},
        analysis_anchor={"prior_nav_high_water_cny": "12500000", "prior_cumulative_max_drawdown": "0.25", "prior_cumulative_fees_cny": "100"})
    opening_ref = put("opening.json", opening)
    execution = {"human": {"capacity": "ACCOUNT_SINGLE_OPERATOR"}, "native": {"fixture": "unchanged"}}
    request = put("old-request.json", {"timing_source_commit": study.C2_SOURCE, "source_identity": {"fixture": "a"*64}})
    dates, days = list(study.TRADE_DATES), []
    for i, day in enumerate(dates):
        row = day_view(day, study.BASELINE_ID, opening, "b"*64, candidate=False)
        nav = str(10000000+1000*(i+1))
        row.update(terminal_nav_cny=nav, nav_observations=[{"as_of": day+"T09:30:00+08:00", "nav_cny": "9000000"},
            {"as_of": day+"T14:57:00+08:00", "nav_cny": nav}])
        days.append(put(f"old-{day}.json", row))
    accounts = [{"arm_id": arm, "account_id": study.BASELINE_ID if arm == "C2" else arm,
        "opening": opening_ref, "days": days} for arm in ("B0", "W15", "C2", "P2")]
    dummy = put("dummy.json", {"fixture": "old producer validator boundary"})
    facts = study.natural.NaturalFacts(preflight=dummy, price_templates=refs["price_templates"], producer_request=request,
        source_inspection=dummy, producer_source_commit="1"*40, study_identity="b"*64, source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS",
        accounts=accounts)
    old_spec = study.natural.NaturalAnalysisSpec(source_delivery=dummy, price_manifest=dummy, facts=dummy, producer_artifact_retry=dummy)
    spec = study.CapacityPreparationSpec(baseline_analysis_spec=put("old-spec.json", old_spec))
    context = {"preflight": {"trade_dates": dates, "preparation_request": payload(dummy)}, "openings": [opening],
        "package": dummy, "request": {**{k: payload(v) for k, v in refs.items()}, "native_target_count": 850,
            "package_id": "pkg_c8d3d51fb6d24be89cfb3a50c30c80e4",
            "manifest_sha256": "5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9", "execution_contract": execution}}
    source_refs = study.context.pins(refs, dummy, request, opening_ref, days, spec)
    monkeypatch.setattr(study.natural, "source_preflight", lambda unused: (context, facts, source_refs))
    pins = {k: v.sha256 for k, v in refs.items()} | {"opening": opening_ref.sha256,
        "producer_request": request.sha256, "analysis_spec": spec.baseline_analysis_spec.sha256}
    monkeypatch.setattr(study, "FROZEN", pins)
    return spec, context, facts


def results_spec(source, root, *, damage=None):
    spec, _, _ = source
    def put(name, value):
        return freeze(root/name, value)
    plan_ref = study.FileRef.model_validate(study.prepare(spec, "2"*40, root/"plans")["plan"])
    plan = read(plan_ref)
    opening = read(plan["common_opening"])
    opening.update(schema_version="localsim_dispatch_capacity_opening_v1", account_id=plan["candidate_account_id"],
        arm_id="C2L", operation=plan["operation"], research_plan=payload(plan_ref))
    opening["positions"]["000001.SZ"]["portfolio_id"] = opening["account_id"]
    if damage == "marks":
        del opening["opening_marks_raw_cny"]["unused_nonposition_mark"]
    opening_ref = put("new-opening.json", opening)
    raw = dict(producer_source_commit="3"*40, source_identity={"fixture": "c"*64}, research_plan=plan_ref,
        account_id=opening["account_id"], common_opening=plan["common_opening"], initial_opening=opening_ref,
        **{k: plan[k] for k in ("trade_dates", "candidate_manifest", "selection_inventory", "price_templates", "cost_policy",
            "package_id", "package_manifest_sha256", "capacity_control_sha256", "execution_contract")}, identity="0"*64)
    request = study.CapacityProducerRequest(**raw)
    request = request.model_copy(update={"identity": digest({k: v for k, v in payload(request).items() if k != "identity"})})
    request_ref, days = put("new-request.json", request), []
    for i, day in enumerate(plan["trade_dates"]):
        row = day_view(day, request.account_id, opening, request.identity, candidate=True)
        nav = str(10000000+2000*(i+1))
        row.update(terminal_nav_cny=nav, capacity_control_sha256=plan["capacity_control_sha256"],
            execution_contract_sha256=digest(plan["execution_contract"]), nav_observations=[
                {"as_of": day+"T09:30:00+08:00", "nav_cny": "8000000"}, {"as_of": day+"T14:57:00+08:00", "nav_cny": nav}])
        if damage == "lane" and i == 0:
            state = row["human_operation_state"]
            state["lanes"][0]["action_ids"].extend(state["lanes"][1]["action_ids"])
            state["lanes"] = state["lanes"][:1]
            state["state_sha256"] = digest({k: v for k, v in state.items() if k != "state_sha256"})
        days.append(put(f"new-{day}.json", row))
    candidate = study.CapacityAccount(account_id=request.account_id, arm_id="C2L", opening=opening_ref, days=days)
    accounts = [study.CapacityAccount.model_validate(plan["baseline_account"]), candidate]
    regression = put("regression.json", {"status": "PASSED", "producer_source_commit": request.producer_source_commit,
        "baseline_producer_request": plan["baseline_producer_request"], "baseline_source_identity_sha256": digest(plan["baseline_source_identity"]),
        "base_execution_contract_sha256": digest(plan["base_execution_contract"]), "account_days_recomputed": 0,
        "funding_boundary_sha256": "0"*64 if damage == "funding" else plan["funding_boundary_sha256"],
        "checks": ["synthetic contract fixture; not real LocalSIM acceptance"]})
    inspection_pins = study.context.pins(plan_ref, request_ref, request, candidate)
    if damage == "pin":
        inspection_pins = inspection_pins[:-1]
    inspection = study.CapacitySourceInspection(stage="COMPLETE_SOURCE_BATCH_INSPECTED", research_plan=plan_ref,
        producer_request=request_ref, producer_source_commit=request.producer_source_commit, study_identity=request.identity,
        accounts_sha256=digest(accounts), capacity_control_sha256=plan["capacity_control_sha256"],
        execution_contract_sha256=digest(plan["execution_contract"]), baseline_source_identity_sha256=digest(plan["baseline_source_identity"]),
        legacy_serial_regression=regression, economic_compatibility="PRODUCER_ATTESTED_BOUND_LEGACY_REGRESSION", source_refs=inspection_pins)
    facts = study.CapacityFacts(research_plan=plan_ref, producer_request=request_ref, source_inspection=put("inspection.json", inspection),
        producer_source_commit=request.producer_source_commit, study_identity=request.identity, source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", accounts=accounts)
    facts_ref = put("new-facts.json", facts)
    retry = study.CapacityArtifactRetry(status="EXACT_ARTIFACT_RETRY_VERIFIED", research_plan=plan_ref, facts=facts_ref,
        producer_request=request_ref, source_inspection=facts.source_inspection, verified_new_account_days=17,
        reused_baseline_account_days=17, recomputed_account_days=0, new_economic_commits=0)
    return study.CapacityAnalysisSpec(research_plan=plan_ref, facts=facts_ref, producer_artifact_retry=put("retry.json", retry))


def test_plan_public_inspect_retry_and_frozen_scope(source, tmp_path):
    spec, context, _ = source
    first = study.prepare(spec, "2"*40, tmp_path/"plans")
    assert study.prepare(spec, "2"*40, tmp_path/"plans") == first | {"status": "ALREADY_MATERIALIZED"}
    plan = study.inspect_dispatch_capacity_plan(first["plan"])["plan"]
    assert plan["schema_version"] == "position_timing_dispatch_capacity_plan_v2"
    assert plan["funding_boundary_sha256"] == digest(study.funding_boundary())
    assert plan["execution_contract"]["funding_boundary"] == plan["funding_boundary"]
    assert study.FUNDING_EVIDENCE in plan["source_refs"]
    assert plan["funding_boundary"]["predispatch_cash_reservation"] == "NONE"
    assert not plan["funding_boundary"]["decision_uses_invisible_feedback"]
    assert plan["policy_sha256"] == "8e456686452cc2e53b0e922cf915b53308e1b40f17358c2fff41b656228d8e79"
    assert plan["base_execution_contract"]["human"]["capacity"] == "ACCOUNT_SINGLE_OPERATOR"
    assert plan["execution_contract"]["capacity_control"]["capacity"] == "PARENT_PLAN_INDEPENDENT_OPERATORS"
    assert set(read(plan["producer_contracts"])["models"]) == {"CapacityProducerRequest", "CapacityFacts",
        "CapacitySourceInspection", "CapacityArtifactRetry", "CapacityAnalysisSpec"}
    assert study.context.pins(plan)  # Embedded JSON schema must not masquerade as a FileRef.
    changed = deepcopy(plan)
    changed["funding_boundary"]["predispatch_cash_reservation"] = "NEW_RESERVATION_MODEL"
    changed["funding_boundary_sha256"] = digest(changed["funding_boundary"])
    changed["execution_contract"].update(funding_boundary=changed["funding_boundary"],
        funding_boundary_sha256=changed["funding_boundary_sha256"])
    changed["identity"] = digest({k: v for k, v in changed.items() if k != "identity"})
    with pytest.raises(IntradayError, match="CAPACITY_PLAN_IDENTITY"):
        study.inspect_dispatch_capacity_plan(freeze(tmp_path/"other-funding.json", changed))
    context["request"]["native_target_count"] = 849
    with pytest.raises(IntradayError, match="CAPACITY_FROZEN_POPULATION"):
        study.inspect_dispatch_capacity_plan(first["plan"])


def test_capital_mdd_complete_denominators_and_artifact_retry(source, tmp_path):
    spec = results_spec(source, tmp_path)
    first = results.analyze(spec, tmp_path/"reports")
    assert results.analyze(spec, tmp_path/"reports") == first | {"status": "ALREADY_MATERIALIZED"}
    report = read(first["report"])
    comparison = report["comparisons"][0]
    assert len(report["comparisons"]) == 1 and comparison["period_net_increment_cny"] == "17000"
    assert comparison["period_fee_difference_cny"] == "51" and float(comparison["period_mdd_reduction"]) < 0
    assert report["account_rows"][1]["cumulative_fees_cny"] == "236"
    assert report["uncertainty"]["nominal_95_interval"] == [1.0, 1.0]
    assert all(row["side_diagnostics"]["BUY"]["parent_denominator"] == 17 for row in report["account_rows"])
    assert all(row["diagnostics"]["time_weighted_exposure"]["status"] == "UNAVAILABLE" for row in report["account_rows"])
    assert not report["economic_confirmation"] and not report["pure_timing_attribution"]
    assert report["funding_boundary_sha256"] == digest(study.funding_boundary())
    assert all(p["capacity_audit"]["maximum_same_clock_dispatches"] == 2 for p in report["mechanism_days"] if p["arm_id"] == "C2L")


@pytest.mark.parametrize("damage,code", [("marks", "CAPACITY_COMMON_ECONOMIC_OPENING"), ("pin", "CAPACITY_INSPECTION_MISSING_PIN"),
    ("lane", "CAPACITY_LANE_ACTION"), ("mutated", "SOURCE_IDENTITY"), ("late", "SOURCE_IDENTITY"),
    ("funding", "CAPACITY_LEGACY_REGRESSION")])
def test_source_clone_lane_and_publication_fail_closed(source, tmp_path, monkeypatch, damage, code):
    spec = results_spec(source, tmp_path, damage=damage)
    ref = read(spec.facts)["accounts"][-1]["days"][-1]
    path = Path(ref["path"])
    if damage == "mutated":
        path.write_text("{}")
    if damage == "late":
        compare = results.compare
        def mutate(rows):
            path.write_text("{}")
            return compare(rows)
        monkeypatch.setattr(results, "compare", mutate)
    with pytest.raises(IntradayError, match=code):
        results.analyze(spec, tmp_path/"reports")
    assert not (tmp_path/"reports").exists()


def test_every_day_is_checked_before_any_new_outcome_read(source, tmp_path, monkeypatch):
    spec = results_spec(source, tmp_path)
    targets = {d["path"] for a in read(spec.facts)["accounts"] for d in a["days"]}
    inspected, parsed = [], []
    original_verify, original_read = results.context.verify, results.read
    def verify(refs):
        original_verify(refs)
        inspected.extend(r.path for r in refs)
    def read_after_batch(ref, schema=None):
        path = study.FileRef.model_validate(ref).path
        if path in targets:
            assert targets <= set(inspected)
            parsed.append(path)
        return original_read(ref, schema)
    monkeypatch.setattr(results.context, "verify", verify)
    monkeypatch.setattr(results, "read", read_after_batch)
    results.analyze(spec, tmp_path/"reports")
    assert set(parsed) == targets


def test_revision_cannot_create_a_second_free_lane(source, tmp_path):
    spec = results_spec(source, tmp_path)
    facts = read(spec.facts)
    day = read(facts["accounts"][-1]["days"][0])
    state = day["human_operation_state"]
    state["actions"][0]["outcome"] = {"status": "NO_ORDER", "reason": "STALE_OPPORTUNITY_AT_DISPATCH"}
    action = deepcopy(state["actions"][0])
    action.update(action_id="revised", revision=2, dispatch_at="2026-10-08T09:32:30+08:00")
    state["actions"].append(action)
    state["lanes"][0]["action_ids"].append(action["action_id"])
    state["state_sha256"] = digest({k: v for k, v in state.items() if k != "state_sha256"})
    with pytest.raises(IntradayError, match="CAPACITY_LANE_INTERVAL"):
        results._lane_audit(day, read(spec.research_plan))


def test_one_lane_cannot_have_two_inflight_actions():
    day = day_view("2026-10-08", "candidate", {"operation": {}, "positions": {}}, "b"*64, candidate=True)
    state = day["human_operation_state"]
    first = state["actions"][0]
    first["status"] = "DISPATCHING"
    second = deepcopy(first)
    second.update(action_id="second_inflight", revision=2, dispatch_at="2026-10-08T09:33:00+08:00")
    state["actions"].append(second)
    state["lanes"][0]["action_ids"].append(second["action_id"])
    state["lanes"][0]["last_action_at"] = second["dispatch_at"]
    state["state_sha256"] = digest({k: v for k, v in state.items() if k != "state_sha256"})
    with pytest.raises(IntradayError, match="CAPACITY_MULTIPLE_INFLIGHT"):
        results._lane_audit(day, {"capacity_control_sha256": digest(study.capacity_control())})
