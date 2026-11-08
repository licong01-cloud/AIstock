"""Cash-origin consumer contracts with real native accounts, never research returns."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import wait_control_history as owner
from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim import wait_budget_history_research as study
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish
from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture

market_pins = study._market_pins


@pytest.fixture
def history(tmp_path, monkeypatch):
    parent_ref, _ = dual_fixture(tmp_path, monkeypatch, matrix="BASIC_2")
    parent = study.read(parent_ref)
    original = study.read(parent["source_request"])
    inspected = study.inspect_price_templates(FileRef.model_validate(original["preflight"]),
                                             FileRef.model_validate(original["price_templates"]))
    p = inspected["preflight"]
    common = {"schema_version": "position_timing_wait_budget_history_common_opening_v1",
        "basis": "DECLARED_NEW_RESEARCH_CASH_ORIGIN_NOT_AN_EXISTING_ACCOUNT",
        "cash_cny": "10000000", "opening_nav_cny": "10000000", "origin_initial_cash_cny": "10000000",
        "positions": {}, "opening_marks_raw_cny": {}, "new_capital_added_cny": "0",
        "analysis_anchor": {"prior_nav_high_water_cny": "10000000", "prior_cumulative_max_drawdown": "0",
                            "prior_cumulative_fees_cny": "0"},
        "parent_account_id": "HISTORY_CASH_ORIGIN", "period_start": parent["trade_dates"][0],
        "available_quantity_as_of": parent["trade_dates"][0] + "T09:00:00+08:00"}
    common_path = tmp_path / "history-plan/common.json"
    publish(common_path, common)
    plan = {"schema_version": "position_timing_wait_budget_history_plan_v1", "input_kind": "REAL_HISTORY",
        "source_spec": {"request": parent_ref.model_dump()}, "timing_source_commit": "7"*40,
        "common_opening": ref(common_path), "source_refs": [parent_ref.model_dump()],
        "profiles": [{"arm_id": a, "rule": r} for a, r in study.PROFILES],
        "operation": parent["population"][1]["operation"], "execution_contract": parent["execution_contract"],
        "trade_dates": parent["trade_dates"], "candidate_manifest": parent["candidate_manifest"],
        "cost_policy": p["inputs"]["cost_policy"], "selection_inventory": p["selection_inventory"],
        "price_templates": original["price_templates"], "package_id": p["package_id"],
        "package_manifest_sha256": p["manifest_sha256"], "primary_delay_seconds": 60,
        "decision_ttl_seconds": 90, "common_terminal_local": "14:57", "allow_split": False,
        "history_scope": study.HISTORY_SCOPE}
    path = tmp_path / "history-plan/plan.json"
    publish(path, plan)
    plan_ref = FileRef.model_validate(ref(path))
    # Only the owner SDK's fixed 181-day source scope and synthetic byte-reader
    # are substituted. Native quantities, signals, queue, matching and ledger run.
    monkeypatch.setattr(study, "inspect_history_plan", lambda r: {
        "integrity": "VERIFIED", "new_study_outcomes_read": False, "plan": study.read(r)})
    monkeypatch.setattr(study, "_market_pins", lambda *a: [])
    monkeypatch.setattr(study, "CandidateReader", dual.CandidateReader)
    frozen = study.freeze_history(research_plan=plan_ref,
        output_root=tmp_path.with_name(tmp_path.name + "-history"), producer_commit="8"*40)
    return FileRef.model_validate(frozen["request"])


def test_cash_origin_exact_opening_fields_and_freeze_reuse(history):
    request = study.read(history)
    plan = study.read(request["research_plan"])
    common = study.read(plan["common_opening"])
    for opening in map(study.read, request["initial_accounts"]):
        extras = {"account_id", "arm_id", "rule", "delay_seconds", "operation", "common_opening"}
        assert set(opening) == set(common) | extras
        assert all(opening[k] == v for k, v in common.items() if k != "schema_version")
        assert opening["positions"] == {} and opening["cash_cny"] == "10000000"
    assert len({study.read(r)["account_id"] for r in request["initial_accounts"]}) == 3
    again = study.freeze_history(research_plan=FileRef.model_validate(request["research_plan"]),
        output_root=Path(history.path).parents[2], producer_commit=request["producer_source_commit"])
    assert again["status"] == "ALREADY_FROZEN" and again["request"] == history.model_dump()


def test_prefix_resume_preserves_continuous_cash_and_public_history_contract(history, monkeypatch):
    calls, native = [], dual._compute_account_day
    def compute(job):
        assert (Path(history.path).parent / "raw_source_inspection.json").is_file()
        calls.append((job["initial"]["account_id"], job["day"]))
        return native(job)
    monkeypatch.setattr(dual, "_compute_account_day", compute)
    options = ResearchPerformanceOptions(optimized=True)
    first = study.run_history(history, performance=options, stop_after_account_days=4)
    assert first["status"] == "CHECKPOINTED" and first["computed_account_days"] == 4
    assert study.inspect_history(history, allow_prefix=True)["verified_account_days"] == 4
    final = study.run_history(history, performance=options)
    assert final["computed_account_days"] == 2 and final["verified_account_days"] == 6
    assert len(calls) == len(set(calls)) == 6
    spec = study.HistoryAnalysisSpec.model_validate(study.read(final["analysis_spec"]))
    request, _, _, inspected = study._inspect_request(history)
    common = study.read(request["common_opening"])
    # Test the real owner consumer against actual account artifacts. Only its
    # historical-source scope is replaced by this functional two-day closure.
    monkeypatch.setattr(owner, "_verified_plan", lambda r: (study.read(r), common, list(inspected["source_refs"])))
    _, facts, openings, _, retry = owner.source_preflight(spec)
    assert retry == "NOT_SUPPLIED" and len(openings) == 3
    for account in facts.accounts:
        before, after = map(study.read, account.days)
        assert Decimal(after["initial_account"]["cash_cny"]) == Decimal(before["cash_cny"])
        assert before["initial_account"]["positions"] == {}
        assert after["initial_account"]["positions"]


def test_artifact_retry_and_completed_run_never_recompute_or_prepare_markets(history, monkeypatch):
    first = study.run_history(history, performance=ResearchPerformanceOptions(optimized=True))
    root = Path(history.path).parent
    before = {p: p.read_bytes() for p in root.rglob("*.json")}
    def forbidden(*a, **k):
        pytest.fail("immutable artifact retry must not compute accounts or prepare markets")
    monkeypatch.setattr(dual, "_compute_account_day", forbidden)
    monkeypatch.setattr(study, "CandidateReader", forbidden)
    retry = study.exact_artifact_retry(history)
    assert retry["status"] == "EXACT_ARTIFACT_RETRY_VERIFIED"
    assert retry["recomputed_account_days"] == retry["new_committed_account_days"] == 0
    assert study.exact_artifact_retry(history) == retry
    assert study.run_history(history)["status"] == "ALREADY_MATERIALIZED"
    assert all(p.read_bytes() == b for p, b in before.items())
    assert retry["facts"] == first["facts"]


@pytest.mark.parametrize("damage", ["cash", "extra_state"])
def test_forged_opening_rejected_before_computation(history, monkeypatch, damage):
    request = study.read(history)
    opening = deepcopy(study.read(request["initial_accounts"][0]))
    opening.update({"cash_cny": "10000001"} if damage == "cash" else {"parent_state": {}})
    path = Path(history.path).parents[2] / "forged-opening.json"
    publish(path, opening)
    request["initial_accounts"][0] = ref(path)
    request["identity"] = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    request["release_identity"] = study.RELEASE_PREFIX + request["identity"][:24]
    path = Path(history.path).parent.parent / request["identity"] / "request.json"
    publish(path, request)
    monkeypatch.setattr(dual, "_compute_account_day", lambda *a: pytest.fail("forged opening cannot execute"))
    with pytest.raises(ValueError, match="HISTORY_OPENING_CONFLICT"):
        study.run_history(FileRef.model_validate(ref(path)))


def test_late_source_failure_precedes_new_nav_and_final_binding_failure(history, monkeypatch):
    study.run_history(history, performance=ResearchPerformanceOptions(optimized=True))
    def forbidden(*a, **k):
        pytest.fail("NAV verification precedes full source/binding validation")
    monkeypatch.setattr(study.shared, "_verify_days", forbidden)
    with monkeypatch.context() as changed:
        def missing(*a):
            raise ValueError("MISSING_LAST_MARKET_SOURCE")
        changed.setattr(study, "_market_pins", missing)
        with pytest.raises(ValueError, match="MISSING_LAST_MARKET_SOURCE"):
            study.inspect_history(history)
    path = Path(history.path).parent / "result_receipt.json"
    body = study.read(ref(path)) | {"economic_confirmation": True}
    path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="RESULT_RECEIPT_CONFLICT"):
        study.inspect_history(history)


@pytest.mark.parametrize("schema", ["localsim_wait_budget_request_v1", "localsim_period_study_v2", "localsim_timing_research_request_v1"])
def test_legacy_scopes_cannot_create_locks_or_mutate_original(tmp_path, schema):
    path = tmp_path / "original/request.json"
    publish(path, {"schema_version": schema})
    original = path.read_bytes()
    for call in (study.run_history, study.exact_artifact_retry):
        with pytest.raises(ValueError, match="HISTORY_REQUEST_IDENTITY_CONFLICT"):
            call(FileRef.model_validate(ref(path)))
        assert not (path.parent / "study-writer.lock").exists() and path.read_bytes() == original


def test_committed_journal_without_day_projection_resumes_without_duplicate_execution(history, monkeypatch):
    native, publisher, calls = dual._compute_account_day, dual.publish, []
    def compute(job):
        calls.append((job["initial"]["account_id"], job["day"]))
        return native(job)
    def interrupted(path, value):
        if Path(path).parent.parent.name == "days":
            raise OSError("INTERRUPTED_AFTER_NATIVE_COMMIT")
        return publisher(path, value)
    monkeypatch.setattr(dual, "_compute_account_day", compute)
    monkeypatch.setattr(dual, "publish", interrupted)
    with pytest.raises(OSError, match="INTERRUPTED_AFTER_NATIVE_COMMIT"):
        study.run_history(history, performance=ResearchPerformanceOptions(optimized=True))
    journals = list((Path(history.path).parent / "journals").glob("*/generations/00000001.json"))
    assert len(journals) == 1
    before = journals[0].read_bytes()
    monkeypatch.setattr(dual, "publish", publisher)
    result = study.run_history(history, performance=ResearchPerformanceOptions(optimized=True))
    assert result["computed_account_days"] == 5 and result["verified_account_days"] == 6
    assert len(calls) == len(set(calls)) == 6 and journals[0].read_bytes() == before


def test_market_source_pins_use_effective_candidate_without_building_market_views(history, monkeypatch, tmp_path):
    plan = study.read(study.read(history)["research_plan"])
    parent = study.read(plan["source_spec"]["request"])
    original = study.read(parent["source_request"])
    inspected = study.inspect_price_templates(FileRef.model_validate(original["preflight"]),
                                             FileRef.model_validate(plan["price_templates"]))
    plan["candidate_manifest"] = {"path": str(tmp_path / "effective-candidate.json"), "sha256": "b"*64}
    used, closed = [], []
    class PinReader:
        DAILY_FIELDS, MINUTE_FIELDS = ("prev_close", "up_limit_price", "down_limit_price"), ("open", "high", "low", "close", "volume", "amount", "factor")
        def __init__(self, manifest):
            assert manifest.model_dump() == plan["candidate_manifest"]
            self.root, self.pins = tmp_path, {}
        def file(self, name):
            used.append(name)
            self.pins[name] = {"sha256": "a"*64}
        def calendar(self, frequency):
            import pandas as pd
            return pd.to_datetime(plan["trade_dates"])
        def verify_pins(self):
            assert used
        def close(self):
            closed.append(True)
    monkeypatch.setattr(study, "CandidateReader", PinReader)
    pins = market_pins(plan, inspected)
    symbols = {s for d in inspected["days"].values() for s, _ in d["series"]}
    assert len(pins) == 2 + 10 * len(symbols) and closed == [True]
    assert any("open.1min.bin" in name for name in used)
    def missing(*a):
        raise ValueError("MISSING_MARKET_SOURCE")
    monkeypatch.setattr(PinReader, "file", missing)
    with pytest.raises(ValueError, match="MISSING_MARKET_SOURCE"):
        market_pins(plan, inspected)
    assert closed == [True, True]
