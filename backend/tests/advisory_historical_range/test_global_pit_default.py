"""New public requests share one global PIT; frozen legacy requests remain reproducible."""
from copy import deepcopy
from dataclasses import replace
from datetime import date

import pytest

from backend.services import canonical_equity_pit as authority_module
from backend.services.advisory_historical_range import input_source_plan as sources
from backend.services.advisory_historical_range.api_models import HistoricalRangeCreateRequest
from backend.services.advisory_historical_range.models import HistoricalRangeResearchBatchRequestV1
from backend.services.advisory_historical_range.requirement_planner import (
    HistoricalRangeSourceRequirementPlanner, program_pit_universe_key,
)
from backend.services.advisory_historical_range.task_input_sources import (
    freeze_program_input_sources, validate_program_source_plan,
)
from backend.services.strategy_package.advisory_input_projection import (
    CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH, HISTORICAL_RANGE_QUERY_CONTRACT_HASH,
)
from backend.tests.advisory_historical_range.conftest import date_plan, digest, frozen_program, research_spec
from backend.tests.advisory_historical_range.test_canonical_pit_package_requirements import _canonical_program
from backend.tests.selection_center.test_canonical_pit_runtime import _binding


@pytest.fixture
def authority(monkeypatch):
    calls = []
    def resolve(_self):
        # A second resolution would observe a different generation.
        calls.append(len(calls) + 1)
        return replace(_binding(generation=calls[-1], active=True), coverage_end=date(2026, 11, 20))
    monkeypatch.setattr(authority_module.CanonicalPitAuthorityResolver, "resolve_live_binding", resolve)
    return calls


def _batch(*, canonical=False, count=2):
    specs = tuple(research_spec(name=f"global-{i}", package_id=f"pkg-{i}") for i in range(count))
    programs = tuple(frozen_program(spec) for spec in specs)
    if canonical:
        programs = tuple(_canonical_program(p) for p in programs)
    dates = date_plan(research_program_ids=tuple(p.research_program_id for p in programs))
    api = HistoricalRangeCreateRequest(program_specs=[s.model_dump(mode="json") for s in specs],
        start_trade_date=dates.start_trade_date, end_trade_date=dates.end_trade_date)
    request = HistoricalRangeResearchBatchRequestV1(**api.model_dump(mode="json"), client_idempotency_key="global-default")
    return request, programs, dates


@pytest.mark.parametrize("selection", [{}, {"selection_input_source_mode": None}])
def test_public_creation_omitted_and_null_source_both_use_global_database(selection):
    spec = research_spec()
    request = HistoricalRangeCreateRequest(program_specs=[spec.model_dump(mode="json")],
        start_trade_date="2026-06-01", end_trade_date="2026-06-03", **selection)
    assert request.selection_input_source_mode == "DATABASE_ONLY"
    stored = HistoricalRangeResearchBatchRequestV1(**request.model_dump(mode="json"), client_idempotency_key="default")
    assert stored.selection_input_source_mode == "DATABASE_ONLY"


@pytest.mark.parametrize("selection,expected", [
    ({}, "DATABASE_ONLY"), ({"selection_input_source_mode": None}, "DATABASE_ONLY"),
    ({"selection_input_source_mode": "FILE_ONLY"}, "FILE_ONLY"),
])
def test_http_creation_delivers_normalized_mode_to_application_without_starting_work(selection, expected):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers.advisory import get_historical_range_application_service, router

    received = []
    class Application:
        def create_batch(self, request, **kwargs):
            received.append(request)
            assert kwargs["idempotency_key"] == "global-route"
            return {"ok": True, "data": {"operation": {"status": "QUEUED"}}}
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_historical_range_application_service] = Application
    with TestClient(app) as client:
        response = client.post("/advisory/historical-range-batches", headers={"Idempotency-Key": "global-route"},
            json={"program_specs": [research_spec().model_dump(mode="json")],
                  "start_trade_date": "2026-06-01", "end_trade_date": "2026-06-03", **selection})
    assert response.status_code == 202
    assert len(received) == 1 and received[0].selection_input_source_mode == expected


@pytest.mark.parametrize("canonical", [False, True])
def test_global_batch_pins_once_and_uses_same_pit_for_marks_and_selection(authority, monkeypatch, canonical):
    request, originals, dates = _batch(canonical=canonical)
    programs = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=originals)
    assert authority == [1]
    identities = [p.selection_input_source_plan["runtime_pit_identity"] for p in programs]
    assert identities[0] == identities[1]
    assert identities[0]["activation_generation"] == 1
    assert identities[0] is not identities[1]
    for before, after in zip(originals, programs, strict=True):
        assert before.admitted_package_projection == after.admitted_package_projection
        assert before.manifest_sha256 == after.manifest_sha256
        assert program_pit_universe_key(after) == authority_module.CANONICAL_PIT_UNIVERSE_KEY
    plan = HistoricalRangeSourceRequirementPlanner().build(request=request, date_plan=dates,
        frozen_programs=programs, calendar_identity_hash=digest("calendar"), code_release_hash=programs[0].code_release_hash)
    assert plan.query_contract_hash == CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH
    marks = [r for r in plan.requirements if r.source_role == "decision_mark_market_state"]
    assert len(marks) == len(programs) * len(dates.ordered_trade_dates)
    assert all(r.parameter_template["universe_key"] == authority_module.CANONICAL_PIT_UNIVERSE_KEY for r in marks)
    monkeypatch.setattr(authority_module.CanonicalPitAuthorityResolver, "resolve_live_binding",
                        lambda _: pytest.fail("restoring a frozen run must not resolve the current pointer"))
    for program in programs:
        saved = program.model_dump_json()
        restored = type(program).model_validate_json(saved)
        validate_program_source_plan(program=restored, date_plan=dates, request=request)
        assert restored.model_dump_json() == saved
        assert restored.frozen_program_hash == program.frozen_program_hash


def test_unversioned_saved_plan_keeps_original_query_contract_and_hash(authority):
    request, originals, dates = _batch(count=1)
    programs = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=originals)
    saved = []
    for original in programs:
        payload = original.model_dump(mode="json")
        old_plan = payload["selection_input_source_plan"]
        old_plan["native_policy"].pop("pit_binding_policy")
        old_plan["plan_hash"] = sources.digest({k: v for k, v in old_plan.items() if k != "plan_hash"})
        payload.update(frozen_program_hash=None)
        saved.append(type(original).model_validate(payload))
    frozen_json = [p.model_dump_json() for p in saved]
    for program in saved:
        validate_program_source_plan(program=program)
    plan = HistoricalRangeSourceRequirementPlanner().build(request=request, date_plan=dates,
        frozen_programs=tuple(saved), calendar_identity_hash=digest("calendar"), code_release_hash=saved[0].code_release_hash)
    assert plan.query_contract_hash == HISTORICAL_RANGE_QUERY_CONTRACT_HASH
    assert [p.model_dump_json() for p in saved] == frozen_json


def test_unavailable_global_authority_never_falls_back_to_legacy(monkeypatch):
    request, programs, dates = _batch()
    def unavailable(_self):
        raise authority_module.CanonicalPitContractError("authority unavailable")
    monkeypatch.setattr(authority_module.CanonicalPitAuthorityResolver, "resolve_live_binding", unavailable)
    before = deepcopy([p.model_dump(mode="json") for p in programs])
    with pytest.raises(authority_module.CanonicalPitContractError, match="unavailable"):
        freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=programs)
    assert before == [p.model_dump(mode="json") for p in programs]
