"""Independent three-arm native account study and immutable, non-economic retry."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.wait_control_results import (
    WaitBudgetAnalysisSpec, prepare as prepare_plan, source_preflight,
)
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim import period_research as period
from backend.services.simulation_execution.localsim import wait_budget_research as study
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish
from backend.tests.simulation_execution.test_localsim_period_research import period_fixture as inherited_period_fixture
from backend.tests.simulation_execution.test_localsim_period_continuation import continuation as inherited_continuation

period_fixture = inherited_period_fixture
continuation = inherited_continuation


@pytest.fixture
def wait_fixture(period_fixture, monkeypatch, tmp_path):
    request, _, _ = period_fixture
    parent = period.run_period(request, performance=ResearchPerformanceOptions(optimized=True))
    plan = prepare_plan(FileRef.model_validate(parent["analysis_spec"]), "7"*40, tmp_path / "timing",
                        input_kind="FUNCTIONAL_FIXTURE")
    monkeypatch.setattr(study, "CandidateReader", period.CandidateReader)
    frozen = study.freeze_wait_budget(research_plan=FileRef.model_validate(plan["plan"]),
        output_root=tmp_path.with_name(tmp_path.name + "-w15-output"), producer_commit="8"*40)
    return FileRef.model_validate(frozen["request"])


def test_three_clones_are_equal_economically_and_do_not_change_old_opening(wait_fixture):
    request = study.read(wait_fixture)
    plan = study.read(request["research_plan"])
    common = study.read(plan["common_opening"])
    before = Path(plan["common_opening"]["path"]).read_bytes()
    openings = [study.read(r) for r in request["initial_accounts"]]
    assert {a["arm_id"] for a in openings} == {"B0", "W15", "C2"}
    assert len({a["account_id"] for a in openings}) == 3
    assert all(study.economic_opening(a) == study.economic_opening(common) for a in openings)
    assert all(a["new_capital_added_cny"] == "0" for a in openings)
    assert all(p["portfolio_id"] == a["account_id"] for a in openings for state in (a, a["parent_state"]) for p in state["positions"].values())
    assert Path(plan["common_opening"]["path"]).read_bytes() == before


def test_short_prefix_resume_native_ledger_and_shared_analyzer_contract(wait_fixture):
    options = ResearchPerformanceOptions(optimized=True)
    first = study.run_wait_budget(wait_fixture, performance=options, stop_after_account_days=2)
    assert first["status"] == "CHECKPOINTED" and first["computed_account_days"] == 2
    assert study.inspect_wait_budget(wait_fixture, allow_prefix=True)["verified_account_days"] == 2
    result = study.run_wait_budget(wait_fixture, performance=options)
    assert result["computed_account_days"] == 1 and result["account_day_count"] == 3
    verified = study.inspect_wait_budget(wait_fixture)
    assert verified["status"] == "VERIFIED" and verified["computed_account_days"] == 0
    spec = WaitBudgetAnalysisSpec.model_validate(study.read(result["analysis_spec"]))
    # Test-only consumer compatibility; producer code does not import this
    # analyzer/private preflight or use it to write or simulate an account.
    _, facts, _, _, retry_status = source_preflight(spec)
    assert len(facts.accounts) == 3 and retry_status == "NOT_SUPPLIED"
    assert all(len(a.days) == 1 for a in facts.accounts)


def test_exact_artifact_retry_never_prepares_markets_or_recomputes(wait_fixture, monkeypatch):
    first = study.run_wait_budget(wait_fixture, performance=ResearchPerformanceOptions(optimized=True))
    root = Path(wait_fixture.path).parent
    preserved = {p: p.read_bytes() for p in root.rglob("*.json")}
    def forbidden(*a, **k):
        raise AssertionError("artifact retry cannot compute accounts or read market bars")
    monkeypatch.setattr(dual, "_compute_account_day", forbidden)
    monkeypatch.setattr(study, "CandidateReader", forbidden)
    retry = study.exact_artifact_retry(wait_fixture)
    assert retry["status"] == "EXACT_ARTIFACT_RETRY_VERIFIED"
    assert retry["recomputed_account_days"] == retry["new_committed_account_days"] == 0
    assert study.exact_artifact_retry(wait_fixture) == retry
    assert study.run_wait_budget(wait_fixture)["status"] == "ALREADY_MATERIALIZED"
    assert all(p.read_bytes() == b for p, b in preserved.items())
    assert retry["facts"] == first["facts"] and retry["analysis_spec"] == first["analysis_spec"]
    # A corrupt final binding is rejected before interpreting new NAVs.
    receipt_path = root / "result_receipt.json"
    corrupted = study.read(ref(receipt_path))
    corrupted["economic_confirmation"] = True
    receipt_path.write_text(json.dumps(corrupted),encoding="utf-8")
    monkeypatch.setattr(study, "_verify_days", forbidden)
    with pytest.raises(ValueError, match="RESULT_RECEIPT_CONFLICT"):
        study.inspect_wait_budget(wait_fixture)


def test_source_or_common_opening_conflict_fails_before_account_computation(wait_fixture, monkeypatch):
    request = study.read(wait_fixture)
    clone_ref = request["initial_accounts"][1]
    clone = study.read(clone_ref)
    forged = deepcopy(clone)
    forged["cash_cny"] = str(int(Decimal(clone["cash_cny"])) + 1)
    target = Path(wait_fixture.path).parent.parent.parent / "forged.json"
    publish(target, forged)
    request["initial_accounts"][1] = ref(target)
    request["identity"] = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    request["release_identity"] = "localsim_wait_budget_v1_" + request["identity"][:24]
    path = Path(wait_fixture.path).parent.parent / request["identity"] / "request.json"
    publish(path, request)
    monkeypatch.setattr(dual, "_compute_account_day", lambda *a, **k: pytest.fail("must fail before account computation"))
    with pytest.raises(ValueError, match="CLONE_IDENTITY_OR_ECONOMICS_CONFLICT"):
        study.run_wait_budget(FileRef.model_validate(ref(path)))


def test_foreign_legacy_request_does_not_create_lock_or_touch_old_study(tmp_path):
    path = tmp_path / "original/request.json"
    publish(path, {"schema_version": "localsim_period_study_v2"})
    before = path.read_bytes()
    for call in (study.run_wait_budget, study.exact_artifact_retry):
        with pytest.raises(ValueError, match="REQUEST_IDENTITY_CONFLICT"):
            call(FileRef.model_validate(ref(path)))
        assert path.read_bytes() == before and not (path.parent / "study-writer.lock").exists()
