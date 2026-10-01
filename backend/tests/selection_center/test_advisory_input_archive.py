from datetime import date, datetime, timezone
import json
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from backend.services.selection_center.advisory_input_archive import AdvisorySelectionInputArchive, validate_archive_reference
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun
from backend.services.selection_center.prospective_evidence import canonical_evidence_json_sha256
from backend.services.trading_core.errors import RuntimeConfigInvalidError

D, T = date(2026, 9, 30), date(2026, 10, 9)


class Source(BaseModel):
    package_id: str = "package"
    manifest_sha256: str = "a" * 64
    trade_date: str = T.isoformat()
    data_source: str = "DB_HISTORICAL"
    status: str = "SUCCEEDED"
    universe_count: int = 2
    metadata: dict
    artifact_input_context_hash: str


def archive_case(tmp_path):
    members = ["000001.SZ", "600000.SH"]
    clock = dict(cutoff_date=D.isoformat(), score_trade_date=D.isoformat(), requested_trade_date=T.isoformat(),
                 universe_input_hash=canonical_evidence_json_sha256(members))
    source = Source(metadata={"artifact_input_context": clock},
                    artifact_input_context_hash=canonical_evidence_json_sha256(clock))
    rows = [SelectionCandidate(symbol=members[0], score=0.5, rank=1)]
    run = SelectionRun(mode=SelectionMode.SINGLE_PACKAGE, trade_date=T, data_source="DB_HISTORICAL",
                       package_ids=["package"], manifest_sha256_by_package={"package": "a" * 64},
                       aggregate_results=rows, package_results={"package": rows}, runtime_config={})
    selection = SimpleNamespace(score_artifacts_by_package={"package": source}, runtime_config={},
                                evidence_by_package={"package": source})
    context = dict(artifact_root=str(tmp_path), program_id="program", binding_version_id="binding",
                   decision_as_of_trade_date=D.isoformat(), review_policy_sha256="b" * 64)
    calls = []
    def read(**kwargs):
        calls.append(kwargs)
        return list(members)
    archive = AdvisorySelectionInputArchive(members_reader=read, calendar_reader=lambda *_: [D, T],
                                            now=lambda: datetime(2026, 10, 1, tzinfo=timezone.utc))
    return archive, run, selection, context, members, calls


def test_atomic_capture_keeps_members_roster_real_clock_and_hash_links(tmp_path):
    archive, run, selection, context, members, calls = archive_case(tmp_path)
    ref = archive.capture(run=run, selection=selection, context=context)
    assert archive.capture(run=run, selection=selection, context=context) == ref
    run.runtime_config["advisory_frozen_input_archive"] = ref
    value = validate_archive_reference(ref, run=run, program_id="program", binding_version_id="binding",
                                       review_policy_sha256="b" * 64)
    assert value["full_source_universe_members"] == members
    assert len(value["frozen_candidates"]) == 1  # Never confuse full pool with selected roster.
    assert not value["historical_capture_backfilled"]
    assert not value["historical_vintage_proven"]
    assert value["observed_at"].startswith("2026-10-01")
    assert calls[0]["decision_date"] == D
    assert ref["package_artifact_hashes"]["package"] == value["package_inputs"]["package"]["artifact_content_sha256"]
    assert not list((tmp_path / "selection_input_archives" / run.run_id).glob("*.tmp"))


def test_real_selection_completion_is_archive_bound_and_failure_is_not_success(tmp_path, monkeypatch):
    from backend.services.selection_center.service import SelectionCenterService
    from backend.services.selection_center.repository import InMemorySelectionCenterRepository
    from backend.services.selection_center.models import SelectionRunStatus
    archive, source_run, selection, context, *_ = archive_case(tmp_path)
    selection.package_results = source_run.package_results
    selection.aggregate_results = source_run.aggregate_results
    selection.excluded_results = {}
    selection.manifest_sha256_by_package = source_run.manifest_sha256_by_package
    selection.valid_no_candidate = False
    selection.no_candidate_reason = None
    service = SelectionCenterService.__new__(SelectionCenterService)
    service.repository = InMemorySelectionCenterRepository()
    service.strategy_selection_service = SimpleNamespace(run_selection=lambda **_: selection)
    service.result_enrichment_service = SimpleNamespace(enrich_candidates=lambda rows, **_: rows)
    service.advisory_input_archive = archive
    monkeypatch.setattr("backend.services.selection_center.service.attach_price_guidance", lambda rows, **_: rows)
    args = dict(package_ids=["package"], mode=SelectionMode.SINGLE_PACKAGE, trade_date=T,
                data_source="DB_HISTORICAL", advisory_archive_context=context)
    run = service.run_packages(**args)
    assert run.status == SelectionRunStatus.SUCCEEDED
    assert run.runtime_config["advisory_frozen_input_archive"]["selection_run_id"] == run.run_id
    assert run.aggregate_results == source_run.aggregate_results
    archive._members = lambda **_: ["wrong"]
    with pytest.raises(RuntimeConfigInvalidError):
        service.run_packages(**args)
    assert list(service.repository.runs.values())[-1].status == SelectionRunStatus.FAILED


def test_real_advisory_publication_copies_the_validated_selection_reference(tmp_path):
    from backend.services.advisory_program import AdvisoryProgramService, InMemoryAdvisoryProgramRepository, PACKAGE_MODE_SINGLE
    from backend.services.selection_center.models import SelectionRunStatus
    from backend.tests.watchlist.test_advisory_program import FakeTradingCalendar
    archive, run, selection, context, *_ = archive_case(tmp_path)
    run.aggregate_results[0].reference_price = 10.0
    run.aggregate_results[0].selection_entry_price = 10.0
    run.aggregate_results[0].stock_name = "fixture"
    class Producer:
        def run_packages(self, *, advisory_archive_context, runtime_config, **_):
            run.runtime_config = dict(runtime_config)
            selection.runtime_config = dict(runtime_config)
            run.runtime_config["advisory_frozen_input_archive"] = archive.capture(
                run=run, selection=selection, context=advisory_archive_context)
            run.status = SelectionRunStatus.SUCCEEDED
            return run
    repo = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(repository=repo, selection_service=Producer(),
                                     calendar_provider=FakeTradingCalendar([D, T]))
    program = service.create_program(program_name="fixture", package_mode=PACKAGE_MODE_SINGLE, package_ids=["package"],
                                     target_count=1, status="ENABLED")
    # No global environment mutations or real producer/DB call.
    from unittest.mock import patch
    with patch("backend.services.advisory_program.os.getenv", return_value=str(tmp_path)):
        result = service.run_review_from_selection(program.program_id, trade_date=T, selection_as_of_trade_date=D)
    expected = run.runtime_config["advisory_frozen_input_archive"]
    assert result.change_summary["advisory_frozen_input_archive"] == expected
    assert repo.list_version_for_date(program.program_id, T, status="PUBLISHED").summary_json[
        "advisory_frozen_input_archive"] == expected


@pytest.mark.parametrize("violation", ["members", "clock", "manifest", "artifact_hash", "missing_artifact", "missing_dse", "calendar", "root"])
def test_future_capture_inconsistency_never_publishes_success(tmp_path, violation):
    archive, run, selection, context, _, _ = archive_case(tmp_path)
    if violation == "members":
        archive._members = lambda **_: ["000001.SZ"]
    elif violation == "clock":
        selection.score_artifacts_by_package["package"].metadata["artifact_input_context"]["cutoff_date"] = T.isoformat()
    elif violation == "manifest":
        run.manifest_sha256_by_package["package"] = "f" * 64
    elif violation == "artifact_hash":
        selection.score_artifacts_by_package["package"].artifact_input_context_hash = "f" * 64
    elif violation == "missing_artifact":
        selection.score_artifacts_by_package.clear()
    elif violation == "missing_dse":
        selection.evidence_by_package.clear()
    elif violation == "calendar":
        archive._calendar = lambda *_: [D, date(2026, 10, 8), T]
    else:
        context["artifact_root"] = ""
    with pytest.raises(RuntimeConfigInvalidError):
        archive.capture(run=run, selection=selection, context=context)
    assert not list(tmp_path.glob("selection_input_archives/*/inputs.json"))


@pytest.mark.parametrize("violation", ["file", "policy", "roster", "runtime", "overwrite"])
def test_readback_and_retry_cannot_rebind_or_replace_archive(tmp_path, violation):
    archive, run, selection, context, *_ = archive_case(tmp_path)
    ref = archive.capture(run=run, selection=selection, context=context)
    if violation == "overwrite":
        run.aggregate_results[0].score = 0.9
        with pytest.raises(RuntimeConfigInvalidError):
            archive.capture(run=run, selection=selection, context=context)
        return
    if violation == "file":
        from pathlib import Path
        path = Path(ref["reference"]["artifact_uri"])
        path.write_text(json.dumps({"foreign": True}), encoding="utf-8")
    elif violation == "roster":
        run.aggregate_results[0].score = 0.9
    elif violation == "runtime":
        run.runtime_config["changed"] = True
    with pytest.raises(RuntimeConfigInvalidError):
        validate_archive_reference(ref, run=run, program_id="program", binding_version_id="binding",
                                   review_policy_sha256="f" * 64 if violation == "policy" else "b" * 64)
