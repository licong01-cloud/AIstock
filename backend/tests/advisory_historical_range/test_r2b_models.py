from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION,
    SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
    SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeCatalogPhase,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeUnresolvedRequirementV1,
    build_candidate_input_hash,
    build_day_input_hash,
)
from backend.tests.advisory_historical_range.conftest import digest, frozen_program, research_spec, resolved_request


def _requirement(requirement_id: str, *, depends_on: tuple[str, ...] = ()) -> HistoricalRangeSourceRequirementV1:
    return HistoricalRangeSourceRequirementV1(
        requirement_id=requirement_id,
        source_role="market_history",
        dataset_id="market.kline_daily_raw",
        query_template_id="get_history_window",
        query_template_version="v1",
        query_template_hash=digest("query-template"),
        parameter_template={"trade_date": "${decision_trade_date}"},
        partition_ref_template="market.kline_daily_raw/${decision_trade_date}",
        depends_on_requirement_ids=depends_on,
        decision_trade_date=date(2026, 6, 2),
        required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
        missing_reason_code="ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
    )


def _plan() -> HistoricalRangeSourceRequirementPlanV1:
    resolved = resolved_request()
    return HistoricalRangeSourceRequirementPlanV1(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=resolved.frozen_programs,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        requirements=(
            _requirement("history", depends_on=("universe",)),
            _requirement("universe"),
        ),
    )


def test_package_version_is_preserved_as_an_opaque_string() -> None:
    program = frozen_program(research_spec())

    assert program.package_version == "8.0.0-test"
    with pytest.raises(ValidationError):
        type(program).model_validate({**program.model_dump(mode="python"), "package_version": ""})


def test_requirement_plan_is_topological_and_planning_identity_is_stable() -> None:
    plan = _plan()

    assert tuple(item.requirement_id for item in plan.requirements) == ("universe", "history")
    assert plan.batch_id.startswith("ahrb_")
    assert len(plan.planning_identity_hash) == 64
    assert all(requirement.source_role != "reference_price" for requirement in plan.requirements)


def test_requirement_plan_rejects_missing_dependency_and_cycle() -> None:
    resolved = resolved_request()
    common = {
        "request": resolved.request,
        "date_plan": resolved.date_plan,
        "frozen_programs": resolved.frozen_programs,
        "query_contract_hash": digest("historical-query-contract"),
        "calendar_identity_hash": digest("calendar-identity"),
        "code_release_hash": resolved.frozen_programs[0].code_release_hash,
    }
    with pytest.raises(ValidationError, match="do not exist"):
        HistoricalRangeSourceRequirementPlanV1(**common, requirements=(_requirement("a", depends_on=("missing",)),))
    with pytest.raises(ValidationError, match="cycle"):
        HistoricalRangeSourceRequirementPlanV1(
            **common,
            requirements=(
                _requirement("a", depends_on=("b",)),
                _requirement("b", depends_on=("a",)),
            ),
        )


def test_revision_identity_excludes_observed_at_but_closes_bound_query() -> None:
    payload = {
        "requirement_id": "universe",
        "source_role": "pit_universe",
        "dataset_id": "market.stock_universe_pit",
        "partition_ref": "shsz_st_pit_active_v1/2026-06-02",
        "decision_trade_date": date(2026, 6, 2),
        "query_template_id": "StockUniversePitService.get_eligible_codes",
        "query_template_version": "v1",
        "query_template_hash": digest("universe-query"),
        "parameter_hash": digest("bound-parameters"),
        "row_count": 5120,
        "content_hash": digest("universe-content"),
        "admissibility": HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
    }
    first = HistoricalRangeSourceRevisionMemberV1(**payload, observed_at=datetime(2026, 7, 20, 1, tzinfo=UTC))
    second = HistoricalRangeSourceRevisionMemberV1(**payload, observed_at=datetime(2026, 7, 20, 2, tzinfo=UTC))

    assert first.revision_id == second.revision_id
    assert first.revision_hash == second.revision_hash
    first_catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=digest("requirement-plan"),
        catalog_generation=1,
        query_contract_hash=digest("query-contract"),
        calendar_identity_hash=digest("calendar"),
        members=(first,),
    )
    second_catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=digest("requirement-plan"),
        catalog_generation=1,
        query_contract_hash=digest("query-contract"),
        calendar_identity_hash=digest("calendar"),
        members=(second,),
    )
    assert first_catalog.catalog_hash == second_catalog.catalog_hash


def test_checkpoint_is_delta_only_and_closes_previous_ref(tmp_path: Path) -> None:
    plan = _plan()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    planning = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION,
        payload=plan.model_dump(mode="json"),
    )
    first = HistoricalRangeSourceCatalogCheckpointV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        ordinal_start=1,
        ordinal_end=1,
        next_requirement_ordinal=1,
        unresolved_requirement_delta=(
            HistoricalRangeUnresolvedRequirementV1(
                ordinal=1,
                requirement_id="universe",
                reason_code="ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
            ),
        ),
        cumulative_resolved_count=0,
        cumulative_member_chain_hash=digest("empty-chain"),
    )
    checkpoint = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
        payload=first.model_dump(mode="json"),
    )

    assert store.load_planning(planning.ref).payload == plan.model_dump(mode="json")
    assert store.load_planning(checkpoint.ref).payload == first.model_dump(mode="json")
    with pytest.raises(Exception):
        store.load(checkpoint.ref)


def test_candidate_and_day_input_hashes_are_one_way(tmp_path: Path) -> None:
    resolved = resolved_request()
    candidate_input_hash = build_candidate_input_hash(
        range_run_id=resolved.range_run_id(resolved.frozen_programs[0].research_program_id),
        research_program_id=resolved.frozen_programs[0].research_program_id,
        decision_trade_date=date(2026, 6, 2),
        frozen_program_hash=resolved.frozen_programs[0].frozen_program_hash,
        runtime_profile_hash=digest("runtime-profile"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        selection_semantics_hash=resolved.selection_semantics_hash,
        calendar_identity_hash=digest("calendar-identity"),
        universe_identity_hash=digest("universe-identity"),
        source_revision_catalog_hash=resolved.source_revision_catalog_hash,
        query_contract_hash=digest("historical-query-contract"),
    )
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    source_ref = {
        "revision_id": "revision-universe",
        "revision_hash": digest("revision-universe"),
    }
    raw_header = {
        "artifact_id": "raw-signal",
        "runtime_profile_hash": digest("runtime-profile"),
        "selection_semantics_hash": resolved.selection_semantics_hash,
        "code_release_hash": resolved.frozen_programs[0].code_release_hash,
        "calendar_identity_hash": digest("calendar-identity"),
        "universe_identity_hash": digest("universe-identity"),
    }
    payload = HistoricalRangeCandidateArtifactPayloadV2(
        range_run_id=resolved.range_run_id(resolved.frozen_programs[0].research_program_id),
        day_run_id="ahrd_test",
        research_program_id=resolved.frozen_programs[0].research_program_id,
        decision_trade_date=date(2026, 6, 2),
        candidate_input_hash=candidate_input_hash,
        package_id=resolved.frozen_programs[0].package_id,
        package_version=resolved.frozen_programs[0].package_version,
        manifest_sha256=resolved.frozen_programs[0].manifest_sha256,
        alpha_mode=resolved.frozen_programs[0].alpha_mode,
        runtime_profile_hash=digest("runtime-profile"),
        selection_semantics_hash=resolved.selection_semantics_hash,
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        calendar_identity_hash=digest("calendar-identity"),
        universe_identity_hash=digest("universe-identity"),
        universe_count=5000,
        raw_signal_identity_hash=canonical_json_sha256(raw_header),
        raw_signal_semantic_header=raw_header,
        raw_inference_receipt={"status": "COMPLETE", "score_count": 0},
        source_read_receipt_hashes=(digest("source-read"),),
        stage_trace={
            stage: {
                "stage": stage,
                "status": "COMPLETE",
                "input_count": 0,
                "output_count": 0,
                "excluded_count": 0,
            }
            for stage in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
        },
        candidate_outcome="VALID_NO_CANDIDATE",
        no_candidate_reason_codes=("NO_ALPHA_CANDIDATES",),
        source_revision_refs=(source_ref,),
        candidates=(),
    )
    artifact = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION,
        resolved_request_hash=resolved.request_payload_sha256,
        range_run_id=payload.range_run_id,
        day_run_id=payload.day_run_id,
        source_revision_refs=payload.source_revision_refs,
        payload=payload.model_dump(mode="json"),
    )
    day_input_hash = build_day_input_hash(
        candidate_input_hash=candidate_input_hash,
        candidate_artifact_ref=artifact.ref,
        previous_list_hash=None,
        previous_day_receipt_hash=None,
        list_semantics_hash=resolved.list_semantics_hash,
    )

    assert candidate_input_hash != artifact.ref.semantic_content_hash
    assert day_input_hash == canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_day_input_v2",
            "candidate_input_hash": candidate_input_hash,
            "candidate_artifact_ref": artifact.ref.model_dump(mode="json"),
            "previous_list_hash": None,
            "previous_day_receipt_hash": None,
            "list_semantics_hash": resolved.list_semantics_hash,
        }
    )
