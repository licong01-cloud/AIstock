from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.candidate_producer import _resolved_runtime_config_for_day
from backend.services.advisory_historical_range.catalog_planner import (
    HistoricalRangeCatalogPlanner,
    HistoricalRangeSourceInputUnavailable,
)
from backend.services.advisory_historical_range.catalog_postgres import _PostgresRequirementResolver
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCatalogPhase,
    HistoricalRangeContractError,
    HistoricalRangeHMMBindingSetV1,
    HistoricalRangeResearchBatchRequestV1,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionCatalogV1,
    normalize_hmm_binding_metadata,
)
from backend.services.advisory_historical_range.planning_service import HistoricalRangePlanningService
from backend.services.advisory_historical_range.requirement_planner import build_hmm_frozen_evidence_partition
from backend.services.trading_core.errors import HMMRuntimeUnavailableError
from backend.tests.advisory_historical_range.conftest import (
    date_plan,
    digest,
    frozen_program,
    research_spec,
)


TRADE_DATE = date(2026, 6, 2)


def _metadata(*, coefficient_seed: str = "coefficient") -> dict:
    return normalize_hmm_binding_metadata(
        {
            "model_config_id": "hmm-config-1",
            "model_snapshot_id": "hmm-snapshot-20260602",
            "signal_preset": "sector_trend_v1",
            "model_artifact_sha256": digest("model"),
            "coefficient_sha256": digest(coefficient_seed),
            "snapshot_trained_at": "2026-05-31T08:00:00+00:00",
            "available_at": "2026-06-01T08:00:00+00:00",
            "training_information_cutoff": "2026-05-30",
            "as_of_trade_date": TRADE_DATE.isoformat(),
            "effective_trade_date": TRADE_DATE.isoformat(),
            "generation_mode": "EXACT_SNAPSHOT",
            "input_data_max_dates": {"market": "2026-05-30", "industry": "2026-05-30"},
        },
        decision_trade_date=TRADE_DATE,
    )


def _program_and_plan(
    *, include_unnormalized_base_evidence: bool = False
) -> tuple[object, HistoricalRangeSourceRequirementPlanV1]:
    spec = research_spec(package_id="pkg-hmm-binding")
    base = frozen_program(spec)
    runtime_config = {
        "runtime_profile": {
            "selection": {"top_k": 5},
            "hmm": {
                "enabled": True,
                "model_config_id": "hmm-config-1",
                "signal_preset": "sector_trend_v1",
            },
        }
    }
    if include_unnormalized_base_evidence:
        base_evidence = _metadata()
        base_evidence.pop("input_data_max_dates_hash")
        runtime_config["phase0a_hmm_metadata_by_date"] = {
            TRADE_DATE.isoformat(): base_evidence,
        }
    payload = base.model_dump(mode="json")
    payload.update(
        {
            "runtime_config": runtime_config,
            "runtime_config_hash": digest(runtime_config),
            "frozen_program_hash": None,
        }
    )
    program = type(base).model_validate(payload)
    request_model = research_spec(package_id=program.package_id)
    batch_request = HistoricalRangeResearchBatchRequestV1(
        request_id="request-hmm-binding",
        client_idempotency_key="request-hmm-binding-key",
        program_specs=(request_model,),
        start_trade_date=TRADE_DATE,
        end_trade_date=TRADE_DATE,
    )
    day_plan = date_plan(
        trade_dates=(TRADE_DATE,),
        research_program_ids=(program.research_program_id,),
    )
    selector = {
        "schema_version": "advisory_hmm_frozen_evidence_selector_v1",
        "research_program_id": program.research_program_id,
        "package_id": program.package_id,
        "decision_trade_date": TRADE_DATE.isoformat(),
        "model_config_id": "hmm-config-1",
        "signal_preset": "sector_trend_v1",
    }
    requirement = HistoricalRangeSourceRequirementV1(
        requirement_id="hmm-evidence",
        source_role="hmm_frozen_evidence",
        dataset_id="hmm.frozen_evidence_bundle",
        query_template_id="historical_hmm_frozen_evidence_bundle",
        query_template_version="v1",
        query_template_hash=digest("hmm-query"),
        parameter_template={
            "selector": selector,
            "phase0a_hmm_metadata": None,
            "formal_partition_selector": {
                "schema_version": "advisory_hmm_frozen_evidence_partition_v1",
                "selector": selector,
            },
        },
        partition_ref_template="hmm-frozen-evidence:test",
        package_id=program.package_id,
        decision_trade_date=TRADE_DATE,
        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
        missing_reason_code="ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
    )
    plan = HistoricalRangeSourceRequirementPlanV1(
        request=batch_request,
        date_plan=day_plan,
        frozen_programs=(program,),
        query_contract_hash=digest("query-contract"),
        calendar_identity_hash=digest("calendar"),
        code_release_hash=program.code_release_hash,
        requirements=(requirement,),
    )
    return program, plan


class _EventCursor:
    def __init__(self, row: dict | None) -> None:
        self.row = row
        self.calls = 0

    def execute(self, query, params):  # noqa: ANN001, ANN201
        assert "advisory_source_availability_event" in str(query)
        assert params[0] == "hmm.frozen_evidence_bundle"
        assert params[1] == "hmm_frozen_evidence"
        self.calls += 1

    def fetchone(self):  # noqa: ANN201
        return self.row


def _event_row(metadata: dict) -> dict:
    selector = {
        "schema_version": "advisory_hmm_frozen_evidence_selector_v1",
        "research_program_id": "hrp_" + digest("unused")[:32],
        "package_id": "pkg-hmm-binding",
        "decision_trade_date": TRADE_DATE.isoformat(),
        "model_config_id": "hmm-config-1",
        "signal_preset": "sector_trend_v1",
    }
    partition_key = build_hmm_frozen_evidence_partition(
        selector=selector,
        phase0a_hmm_metadata=metadata,
    )
    return {
        "partition_key": partition_key,
        "event_content_hash": digest("event"),
        "schema_fingerprint": digest("schema"),
        "row_count": 1,
        "partition_content_hash": digest(metadata),
        "first_observed_at": datetime(2026, 7, 20, tzinfo=UTC),
        "event_type": "INGESTED",
        "quality_status": "PASS",
    }


def test_config_only_hmm_requirement_waits_then_resumes_same_plan() -> None:
    _program, plan = _program_and_plan()
    planner = HistoricalRangeCatalogPlanner()
    missing_cursor = _EventCursor(None)
    waiting = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolver=_PostgresRequirementResolver(
            cur=missing_cursor,
            observed_at=datetime(2026, 7, 20, tzinfo=UTC),
        ),
        resolved_members={},
    )
    assert waiting.waiting_input is True
    assert waiting.checkpoint.next_requirement_ordinal == 1

    event = _event_row(_metadata())
    event["partition_key"] = build_hmm_frozen_evidence_partition(
        selector=plan.requirements[0].parameter_template["selector"],
        phase0a_hmm_metadata=_metadata(),
    )
    resolved_cursor = _EventCursor(event)
    resumed = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolver=_PostgresRequirementResolver(
            cur=resolved_cursor,
            observed_at=datetime(2026, 7, 20, tzinfo=UTC),
        ),
        resolved_members={},
    )
    member = resumed.checkpoint.member_delta[0].member
    assert resumed.phase_complete is True
    assert member.admissibility is HistoricalRangeRevisionAdmissibility.FORMAL_EVENT
    assert member.bound_parameters["phase0a_hmm_metadata"]["model_snapshot_id"] == "hmm-snapshot-20260602"

    verified = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.VERIFY,
        start_ordinal=1,
        resolver=_PostgresRequirementResolver(
            cur=_EventCursor(event),
            observed_at=datetime(2026, 7, 20, tzinfo=UTC),
        ),
        resolved_members={},
        expected_members={member.requirement_id: member},
    )
    assert verified.phase_complete is True


def test_invalid_hmm_event_is_visible_as_waiting_input() -> None:
    _program, plan = _program_and_plan()
    event = _event_row(_metadata(coefficient_seed="changed"))
    event["partition_key"] = build_hmm_frozen_evidence_partition(
        selector=plan.requirements[0].parameter_template["selector"],
        phase0a_hmm_metadata=_metadata(coefficient_seed="changed"),
    )
    event["partition_content_hash"] = digest("wrong-content")
    resolver = _PostgresRequirementResolver(
        cur=_EventCursor(event),
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    with pytest.raises(HistoricalRangeSourceInputUnavailable, match="does not close"):
        resolver.resolve(
            requirement=plan.requirements[0],
            dependency_members={},
            phase=HistoricalRangeCatalogPhase.DISCOVER,
            expected_member=None,
        )


def test_seal_materializes_binding_set_and_day_local_runtime_config(tmp_path: Path) -> None:
    program, plan = _program_and_plan()
    event = _event_row(_metadata())
    event["partition_key"] = build_hmm_frozen_evidence_partition(
        selector=plan.requirements[0].parameter_template["selector"],
        phase0a_hmm_metadata=_metadata(),
    )
    member = _PostgresRequirementResolver(
        cur=_EventCursor(event),
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    ).resolve(
        requirement=plan.requirements[0],
        dependency_members={},
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        expected_member=None,
    )
    catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        query_contract_hash=plan.query_contract_hash,
        calendar_identity_hash=plan.calendar_identity_hash,
        members=(member,),
    )
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r-hmm-binding")
    service = HistoricalRangePlanningService(
        program_resolver=object(),
        calendar_resolver=object(),
        code_release_resolver=object(),
        requirement_planner=object(),
        catalog_executor=object(),
        repository=object(),
        artifact_store=store,
        selection_semantics_version=program.selection_semantics_version,
        selection_semantics_hash=program.selection_semantics_hash,
        list_semantics_version=program.list_semantics_version,
        list_semantics_hash=program.list_semantics_hash,
    )
    resolved_program = service._bind_resolved_hmm_evidence(  # noqa: SLF001
        plan=plan,
        catalog=catalog,
        catalog_generation=1,
    )[0]
    assert resolved_program.without_resolved_hmm_binding() == program
    assert resolved_program.resolved_hmm_binding_set_ref is not None
    envelope = store.load_planning(resolved_program.resolved_hmm_binding_set_ref)
    binding_set = HistoricalRangeHMMBindingSetV1.model_validate(envelope.payload)
    assert binding_set.binding_set_hash == resolved_program.resolved_hmm_binding_set_hash

    runtime_config = _resolved_runtime_config_for_day(
        program=resolved_program,
        decision_trade_date=TRADE_DATE,
        catalog=catalog,
        source_refs=catalog.source_revision_refs(),
        artifact_store=store,
    )
    assert runtime_config["runtime_profile"]["hmm"]["model_snapshot_id"] == "hmm-snapshot-20260602"
    assert runtime_config["phase0a_hmm_metadata_by_date"][TRADE_DATE.isoformat()][
        "coefficient_sha256"
    ] == digest("coefficient")
    assert program.runtime_config.get("phase0a_hmm_metadata_by_date") is None

    class _MissingBindingStore:
        @staticmethod
        def load_planning(_ref):  # noqa: ANN001, ANN205
            raise HistoricalRangeContractError(
                "ADVISORY_HISTORICAL_RANGE_ARTIFACT_NOT_FOUND",
                "binding set is absent",
            )

    with pytest.raises(HMMRuntimeUnavailableError) as exc_info:
        _resolved_runtime_config_for_day(
            program=resolved_program,
            decision_trade_date=TRADE_DATE,
            catalog=catalog,
            source_refs=catalog.source_revision_refs(),
            artifact_store=_MissingBindingStore(),  # type: ignore[arg-type]
        )
    assert exc_info.value.context["reason_code"] == "ADVISORY_HR_HMM_INPUT_UNAVAILABLE"


def test_day_runtime_normalizes_base_hmm_evidence_before_binding_comparison(
    tmp_path: Path,
) -> None:
    program, plan = _program_and_plan(include_unnormalized_base_evidence=True)
    event = _event_row(_metadata())
    event["partition_key"] = build_hmm_frozen_evidence_partition(
        selector=plan.requirements[0].parameter_template["selector"],
        phase0a_hmm_metadata=_metadata(),
    )
    member = _PostgresRequirementResolver(
        cur=_EventCursor(event),
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    ).resolve(
        requirement=plan.requirements[0],
        dependency_members={},
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        expected_member=None,
    )
    catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=plan.requirement_plan_hash,
        catalog_generation=1,
        query_contract_hash=plan.query_contract_hash,
        calendar_identity_hash=plan.calendar_identity_hash,
        members=(member,),
    )
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r-normalized-binding")
    service = HistoricalRangePlanningService(
        program_resolver=object(),
        calendar_resolver=object(),
        code_release_resolver=object(),
        requirement_planner=object(),
        catalog_executor=object(),
        repository=object(),
        artifact_store=store,
        selection_semantics_version=program.selection_semantics_version,
        selection_semantics_hash=program.selection_semantics_hash,
        list_semantics_version=program.list_semantics_version,
        list_semantics_hash=program.list_semantics_hash,
    )
    resolved_program = service._bind_resolved_hmm_evidence(  # noqa: SLF001
        plan=plan,
        catalog=catalog,
        catalog_generation=1,
    )[0]

    runtime_config = _resolved_runtime_config_for_day(
        program=resolved_program,
        decision_trade_date=TRADE_DATE,
        catalog=catalog,
        source_refs=catalog.source_revision_refs(),
        artifact_store=store,
    )

    assert runtime_config["phase0a_hmm_metadata_by_date"][TRADE_DATE.isoformat()][
        "input_data_max_dates_hash"
    ] == _metadata()["input_data_max_dates_hash"]
