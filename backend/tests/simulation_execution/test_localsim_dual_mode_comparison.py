"""File-contract functional population; not real-history economic evidence."""

from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.replay import FileRef
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import read_json, publish
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.simulation_execution.test_localsim_timing_campaign import price_campaign_fixture


def dual_fixture(tmp_path, monkeypatch, *, two_days=True, matrix="FULL_13"):
    campaign, preflight, templates, _ = price_campaign_fixture(tmp_path, monkeypatch, two_days=two_days)
    manifest = make_paper_enabled_manifest().model_copy(update={"package_id": "FUNCTIONAL_FIXTURE"})
    original_factory = dual.native_backend_for
    # The existing fixture intentionally has only explicit native target weights,
    # not fitted model assets. Only parsing is substituted, never execution/fees.
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: None)
    monkeypatch.setattr(dual, "CandidateReader", campaign.CandidateReader)
    def native_factory(**kwargs):
        return original_factory(**{**kwargs, "manifest": manifest})
    monkeypatch.setattr(dual, "native_backend_for", native_factory)
    source = campaign.freeze_continuous_campaign(preflight_ref=preflight, templates_ref=templates,
                                                output_root=tmp_path / "original", producer_commit="1" * 40)
    frozen = dual.freeze_study(source_request=FileRef.model_validate(source["request"]),
                              output_root=tmp_path / "dual", producer_commit="2" * 40, matrix=matrix)
    return FileRef.model_validate(frozen["request"]), tmp_path / "dual"


def test_thirteen_accounts_two_days_resume_native_journals_and_exact_retry(tmp_path, monkeypatch):
    request, _ = dual_fixture(tmp_path, monkeypatch)
    paused = dual.run_study(request, stop_after_account_days=2)
    assert paused["status"] == "CHECKPOINTED"
    root = Path(request.path).parent
    assert len(list((root / "journals").glob("*/generations/00000001.json"))) == 2
    assert not (root / "results.json").exists()
    first = dual.run_study(request)
    assert first["computed_account_days"] == 24
    result = read_json(Path(first["results"]["path"]))
    assert len(result["accounts"]) == 13
    assert len(result["comparisons"]) == 24
    assert {a["operation"]["operation_mode"] for a in result["accounts"]} == {"NATIVE", "HUMAN_SIM"}
    assert all(len(a["days"]) == 2 and a["initial_cash_cny"] == "10000000" for a in result["accounts"])
    assert len({a["initial_account_sha256"] for a in result["accounts"]}) == 1
    assert result["outcomes_read_stage"] == "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"
    assert result["economic_confirmation"] is False
    for a in result["accounts"]:
        for day_ref in a["days"]:
            day = read_json(Path(day_ref["path"]))
            assert Decimal(day["cash_cny"]) >= 0
            assert len(day["parents"]) == len(day["native_quantity_evidence"]["trading_rule_decisions"])
            if a["operation"]["operation_mode"] == "HUMAN_SIM":
                assert "human_operation_state" in day
            else:
                assert "human_operation_state" not in day
    retry = dual.run_study(request)
    assert retry["status"] == "ALREADY_MATERIALIZED" and retry["computed_account_days"] == 0
    assert retry["results"] == first["results"]
    # Index repair reads the committed account receipt; no second execution.
    lost = Path(result["accounts"][0]["days"][0]["path"])
    before = lost.read_bytes()
    lost.unlink()
    repaired = dual.run_study(request)
    assert repaired["computed_account_days"] == 0 and lost.read_bytes() == before
    assert repaired["results"] == first["results"]


def test_source_change_and_rehashed_population_change_fail_before_outputs(tmp_path, monkeypatch):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    original = dual.implementation_identity
    monkeypatch.setattr(dual, "implementation_identity", lambda: {"unexpected": True})
    with pytest.raises(ValueError, match="REQUEST_IDENTITY_CONFLICT"):
        dual.run_study(request)
    assert not (Path(request.path).parent / "source_inspection.json").exists()
    monkeypatch.setattr(dual, "implementation_identity", original)
    changed = read_json(Path(request.path))
    changed["population"][1]["rule"] = "BOUNDED_REVERSION_V1"
    from backend.services.position_timing.intraday.contracts import digest
    changed["identity"] = digest({k:v for k,v in changed.items() if k not in {"identity", "release_identity"}})
    changed["release_identity"] = "localsim_dual_mode_v1_" + changed["identity"][:24]
    bad = tmp_path / "changed.json"
    publish(bad, changed)
    with pytest.raises(ValueError, match="POPULATION_CONFLICT"):
        dual.run_study(FileRef.model_validate(ref(bad)))


def test_single_writer_and_failure_preserve_only_committed_prefix(tmp_path, monkeypatch):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    root = Path(request.path).parent
    from filelock import FileLock
    with FileLock(root / "study-writer.lock", timeout=0):
        assert dual.run_study(request) == {"status":"IN_PROGRESS", "computation_performed":False, "outcomes_read":False}
        assert not (root / "source_inspection.json").exists()
    real = dual.run_human_portfolio_day
    def fail(**kwargs):
        raise ValueError("TEST_ACCOUNT_INPUT_UNAVAILABLE")
    monkeypatch.setattr(dual, "run_human_portfolio_day", fail)
    with pytest.raises(ValueError, match="TEST_ACCOUNT_INPUT_UNAVAILABLE"):
        dual.run_study(request)
    generations = list((root / "journals").glob("*/generations/*.json"))
    assert len(generations) == 1 and not (root / "results.json").exists()
    prefix = {p:p.read_bytes() for p in generations}
    failure = read_json(next((root / "operational").glob("*.json")))
    assert failure["reason_code"] == "TEST_ACCOUNT_INPUT_UNAVAILABLE"
    assert failure["generation_file_count"] == 1 and not failure["economic_result_published"]
    monkeypatch.setattr(dual, "run_human_portfolio_day", real)
    result = dual.run_study(request)
    assert result["computed_account_days"] == 1
    assert all(p.read_bytes() == raw for p,raw in prefix.items())


def test_invalid_economic_day_fails_before_any_journal_is_committed(tmp_path, monkeypatch):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    real = dual.run_native_portfolio_day
    def inconsistent(**kwargs):
        result = real(**kwargs)
        result["account"]["cash_cny"] = "10000000"
        return result
    monkeypatch.setattr(dual, "run_native_portfolio_day", inconsistent)
    with pytest.raises(ValueError, match="LEDGER_RECONCILIATION_FAILED"):
        dual.run_study(request)
    root = Path(request.path).parent
    assert not list((root / "journals").glob("*/generations/*.json"))
    assert not (root / "results.json").exists()


def test_shared_owner_only_code_upgrade_cannot_resume_old_account_prefix(tmp_path,monkeypatch):
    request, _ = dual_fixture(tmp_path,monkeypatch,two_days=False,matrix="BASIC_2")
    original_read = Path.read_bytes
    def upgraded(path):
        original = original_read(path)
        return original + b"\n# different owner implementation\n" if path.as_posix().endswith(
            "/position_timing/intraday/core.py") else original
    monkeypatch.setattr(Path,"read_bytes",upgraded)
    with pytest.raises(ValueError,match="REQUEST_IDENTITY_CONFLICT"):
        dual.run_study(request)
    root=Path(request.path).parent
    assert not (root/"source_inspection.json").exists()
    assert not list((root/"journals").glob("*/generations/*.json"))


def test_fresh_candidate_is_explicitly_pinned_and_reinspected_before_real_accounts(tmp_path, monkeypatch):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    original_bytes = Path(request.path).read_bytes()
    original = read_json(Path(request.path))
    candidate_path = tmp_path / "authorized-repaired-candidate.json"
    publish(candidate_path, {"role": "FUNCTIONAL_REPAIRED_CANDIDATE"})
    candidate = FileRef.model_validate(ref(candidate_path))
    calls = []

    def inspect_sources(inspected, actual):
        assert actual == candidate
        assert inspected["preflight"]["inputs"]["candidate_manifest"] != candidate.model_dump()
        calls.append(actual)

    monkeypatch.setattr(dual, "inspect_successor_sources", inspect_sources)
    frozen = dual.freeze_study(source_request=FileRef.model_validate(original["source_request"]),
                              input_candidate=candidate, output_root=tmp_path / "repaired-study",
                              producer_commit="3" * 40, matrix="BASIC_2")
    fresh = FileRef.model_validate(frozen["request"])
    bound = read_json(Path(fresh.path))
    assert bound["input_candidate"] == bound["candidate_manifest"] == candidate.model_dump()
    assert bound["input_successor"] is None
    assert "input_candidate" not in original
    assert Path(request.path).read_bytes() == original_bytes
    assert len(calls) == 1 and not (Path(fresh.path).parent / "journals").exists()
    result = dual.run_study(fresh)
    assert result["computed_account_days"] == 2 and len(calls) == 2
    results = read_json(Path(result["results"]["path"]))
    inspection = read_json(Path(results["source_inspection"]["path"]))
    assert inspection["candidate_manifest"] == candidate.model_dump()
    assert all(a["initial_cash_cny"] == "10000000" for a in results["accounts"])
    retry = dual.run_study(fresh)
    assert retry["computed_account_days"] == 0 and retry["results"] == result["results"]
    assert len(calls) == 3 and Path(request.path).read_bytes() == original_bytes
    candidate_path.write_bytes(b"changed by a different input owner")
    from backend.services.position_timing.intraday.store import IntradayError
    with pytest.raises(IntradayError, match="RESEARCH_SOURCE_IDENTITY_MISMATCH"):
        dual.run_study(fresh)
    assert read_json(Path(result["results"]["path"])) == results


@pytest.mark.parametrize("reason", ["LOCALSIM_TIMING_SUCCESSOR_PRICE_INPUT_CHANGED", "LOCALSIM_TIMING_SUCCESSOR_QUOTE_SPAN_CHANGED"])
def test_fresh_candidate_equivalence_failure_leaves_no_new_request_or_accounts(tmp_path, monkeypatch, reason):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    original = read_json(Path(request.path))
    candidate_path = tmp_path / "incompatible-candidate.json"
    publish(candidate_path, {"role": "FUNCTIONAL_INCOMPATIBLE_CANDIDATE"})

    def reject(inspected, candidate):
        raise ValueError(reason)

    monkeypatch.setattr(dual, "inspect_successor_sources", reject)
    output = tmp_path / "must-not-publish"
    with pytest.raises(ValueError, match=reason):
        dual.freeze_study(source_request=FileRef.model_validate(original["source_request"]),
                          input_candidate=FileRef.model_validate(ref(candidate_path)), output_root=output,
                          producer_commit="3" * 40)
    assert not output.exists()


def test_fresh_candidate_and_old_successor_are_rejected_before_source_reads(tmp_path):
    missing = FileRef(path=str(tmp_path / "must-not-read.json"), sha256="a" * 64)
    with pytest.raises(ValueError, match="INPUT_BINDING_CONFLICT"):
        dual.freeze_study(source_request=missing, input_successor=missing, input_candidate=missing,
                          output_root=tmp_path / "must-not-publish", producer_commit="3" * 40)
    assert not (tmp_path / "must-not-publish").exists()


@pytest.mark.parametrize("invalid", [None, {}, "active"])
def test_malformed_explicit_candidate_never_becomes_an_implicit_original(tmp_path, monkeypatch, invalid):
    request, _ = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    changed = read_json(Path(request.path))
    changed["input_candidate"] = invalid
    from backend.services.position_timing.intraday.contracts import digest
    changed["identity"] = digest({k:v for k,v in changed.items() if k not in {"identity", "release_identity"}})
    changed["release_identity"] = "localsim_dual_mode_v1_" + changed["identity"][:24]
    path = tmp_path / "malformed.json"
    publish(path, changed)
    with pytest.raises(ValueError):
        dual.run_study(FileRef.model_validate(ref(path)))
    assert not (tmp_path / "source_inspection.json").exists()
    assert not (tmp_path / "journals").exists()


@pytest.mark.parametrize("flags", [["--candidate", "missing.json"], ["--candidate-sha256", "a" * 64],
    ["--candidate", "new.json", "--candidate-sha256", "a" * 64,
     "--input-successor", "old.json", "--input-successor-sha256", "b" * 64]])
def test_candidate_cli_rejects_incomplete_or_conflicting_bindings(tmp_path, monkeypatch, flags):
    monkeypatch.setattr("sys.argv", ["dual-mode", "freeze", "--source-request", "source.json",
        "--source-request-sha256", "c" * 64, "--producer-commit", "d" * 40,
        "--output-root", str(tmp_path / "must-not-publish"), *flags])
    with pytest.raises(SystemExit) as exc:
        dual.main()
    assert exc.value.code == 2 and not (tmp_path / "must-not-publish").exists()
