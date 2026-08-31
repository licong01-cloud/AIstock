from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.catalog_planner import (
    REASON_SOURCE_REVISION_DRIFT,
    HistoricalRangeCatalogPlanner,
    HistoricalRangeSourceInputUnavailable,
)
from backend.services.advisory_historical_range.models import (
    SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeCatalogPhase,
    HistoricalRangeContractError,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
)
from backend.services.advisory_historical_range.planning_service import HistoricalRangePlanningService
from backend.services.advisory_historical_range.repository import (
    HistoricalRangeCatalogPlanningState,
    SealedHistoricalRangeBatch,
)
from backend.tests.advisory_historical_range.conftest import digest, resolved_request


def _plan(count: int) -> HistoricalRangeSourceRequirementPlanV1:
    resolved = resolved_request()
    requirements = tuple(
        HistoricalRangeSourceRequirementV1(
            requirement_id=f"requirement-{index:03d}",
            source_role="market_history",
            dataset_id="market.kline_daily_raw",
            query_template_id="get_history_window",
            query_template_version="v1",
            query_template_hash=digest("history-query"),
            parameter_template={"ordinal": index},
            partition_ref_template=f"market.kline_daily_raw/{index:03d}",
            depends_on_requirement_ids=((f"requirement-{index - 1:03d}",) if index > 1 else ()),
            decision_trade_date=date(2026, 6, 2),
            required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
            missing_reason_code="ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
        )
        for index in range(1, count + 1)
    )
    return HistoricalRangeSourceRequirementPlanV1(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=resolved.frozen_programs,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        requirements=requirements,
    )


class _Resolver:
    def __init__(self, *, missing: set[str] | None = None, drift: set[str] | None = None) -> None:
        self.missing = set(missing or ())
        self.drift = set(drift or ())
        self.calls: list[str] = []

    def resolve(self, *, requirement, dependency_members, phase, expected_member):
        self.calls.append(requirement.requirement_id)
        if requirement.requirement_id in self.missing:
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "source partition is not available",
                context={"requirement_id": requirement.requirement_id},
            )
        seed = requirement.requirement_id
        if phase is HistoricalRangeCatalogPhase.VERIFY and requirement.requirement_id in self.drift:
            seed += "-drift"
        return HistoricalRangeSourceRevisionMemberV1(
            requirement_id=requirement.requirement_id,
            source_role=requirement.source_role,
            dataset_id=requirement.dataset_id,
            partition_ref=requirement.partition_ref_template,
            package_id=requirement.package_id,
            component_id=requirement.component_id,
            decision_trade_date=requirement.decision_trade_date,
            query_template_id=requirement.query_template_id,
            query_template_version=requirement.query_template_version,
            query_template_hash=requirement.query_template_hash,
            parameter_hash=requirement.parameter_template_hash,
            row_count=100,
            content_hash=digest(f"content:{seed}"),
            admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
            observed_at=datetime(2026, 7, 20, 1, tzinfo=UTC),
        )


def _publish_checkpoint(store: HistoricalRangeArtifactStore, plan, checkpoint):
    return store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=checkpoint.catalog_generation,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
        payload=checkpoint.model_dump(mode="json"),
    ).ref


def test_catalog_chunks_are_stable_bounded_and_delta_only(tmp_path: Path) -> None:
    plan = _plan(35)
    planner = HistoricalRangeCatalogPlanner()
    resolver = _Resolver()
    first = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolver=resolver,
        resolved_members={},
    )

    assert len(first.checkpoint.member_delta) == 32
    assert first.checkpoint.next_requirement_ordinal == 33
    assert first.checkpoint.cumulative_resolved_count == 32
    assert first.phase_complete is False

    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    first_ref = _publish_checkpoint(store, plan, first.checkpoint)
    second = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=33,
        resolver=resolver,
        resolved_members=first.resolved_members,
        previous_checkpoint_ref=first_ref,
        previous_checkpoint=first.checkpoint,
    )

    assert len(second.checkpoint.member_delta) == 3
    assert second.checkpoint.cumulative_resolved_count == 35
    assert second.phase_complete is True
    assert len(second.checkpoint.model_dump_json()) < len(first.checkpoint.model_dump_json())


def test_missing_input_stops_at_first_unresolved_and_resumes_same_ordinal(tmp_path: Path) -> None:
    plan = _plan(3)
    planner = HistoricalRangeCatalogPlanner()
    waiting = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolver=_Resolver(missing={"requirement-002"}),
        resolved_members={},
    )

    assert waiting.waiting_input is True
    assert waiting.checkpoint.ordinal_end == 2
    assert waiting.checkpoint.next_requirement_ordinal == 2
    assert tuple(waiting.resolved_members) == ("requirement-001",)
    assert [item.requirement_id for item in waiting.checkpoint.unresolved_requirement_delta] == ["requirement-002"]

    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    waiting_ref = _publish_checkpoint(store, plan, waiting.checkpoint)
    resumed = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=2,
        resolver=_Resolver(),
        resolved_members=waiting.resolved_members,
        previous_checkpoint_ref=waiting_ref,
        previous_checkpoint=waiting.checkpoint,
    )

    assert resumed.waiting_input is False
    assert resumed.phase_complete is True
    assert resumed.checkpoint.ordinal_start == 2
    assert resumed.checkpoint.next_requirement_ordinal == 4


def test_verify_uses_discovered_members_and_rejects_revision_drift(tmp_path: Path) -> None:
    plan = _plan(2)
    planner = HistoricalRangeCatalogPlanner()
    discovered = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolver=_Resolver(),
        resolved_members={},
    )
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    discovered_ref = _publish_checkpoint(store, plan, discovered.checkpoint)

    verified = planner.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.VERIFY,
        start_ordinal=1,
        resolver=_Resolver(),
        resolved_members={},
        expected_members=discovered.resolved_members,
        previous_checkpoint_ref=discovered_ref,
        previous_checkpoint=discovered.checkpoint,
    )
    assert verified.phase_complete is True
    assert verified.checkpoint.cumulative_resolved_count == 2

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        planner.resolve_chunk(
            plan=plan,
            catalog_generation=1,
            phase=HistoricalRangeCatalogPhase.VERIFY,
            start_ordinal=1,
            resolver=_Resolver(drift={"requirement-002"}),
            resolved_members={},
            expected_members=discovered.resolved_members,
            previous_checkpoint_ref=discovered_ref,
            previous_checkpoint=discovered.checkpoint,
        )
    assert exc_info.value.reason_code == REASON_SOURCE_REVISION_DRIFT


class _CatalogExecutor:
    def __init__(self) -> None:
        self.planner = HistoricalRangeCatalogPlanner()
        self.resolver = _Resolver()

    def resolve_chunk(self, **kwargs):  # noqa: ANN003, ANN201
        return self.planner.resolve_chunk(resolver=self.resolver, **kwargs)


class _PlanningRepository:
    def __init__(self, plan) -> None:  # noqa: ANN001
        self.plan = plan
        self.batch = {"batch_id": plan.batch_id, "row_version": 1}
        self.operation = {
            "operation_id": "operation-catalog",
            "status": "RUNNING",
            "row_version": 1,
            "fencing_token": 1,
            "catalog_generation": 1,
            "catalog_phase": "DISCOVER",
            "stable_keyset_cursor_json": {"next_requirement_ordinal": 1},
            "cumulative_resolved_count": 0,
            "cumulative_unresolved_count": 0,
        }
        self.chain = []
        self.seal_calls = []

    def load_catalog_planning_state(self, *, operation_id):  # noqa: ANN001, ANN201
        assert operation_id == self.operation["operation_id"]
        discovered = {}
        current = {}
        for _ref, checkpoint in self.chain:
            for delta in checkpoint.member_delta:
                if checkpoint.phase is HistoricalRangeCatalogPhase.DISCOVER:
                    discovered[delta.member.requirement_id] = delta.member
                if checkpoint.phase.value == self.operation["catalog_phase"]:
                    current[delta.member.requirement_id] = delta.member
        return HistoricalRangeCatalogPlanningState(
            batch=dict(self.batch),
            operation=dict(self.operation),
            plan=self.plan,
            checkpoint_chain=tuple(self.chain),
            discovered_members=discovered,
            current_phase_members=current,
        )

    def commit_catalog_checkpoint(self, **kwargs):  # noqa: ANN003, ANN201
        checkpoint = kwargs["checkpoint"]
        self.chain.append((kwargs["checkpoint_ref"], checkpoint))
        self.operation["row_version"] += 1
        target = kwargs["target_status"].value
        self.operation["status"] = target
        self.operation["cumulative_resolved_count"] = checkpoint.cumulative_resolved_count
        self.operation["cumulative_unresolved_count"] = len(checkpoint.unresolved_requirement_delta)
        if target == "RUNNING":
            self.operation["fencing_token"] += 1
            if kwargs.get("advance_to_verify"):
                self.operation["catalog_phase"] = "VERIFY"
                self.operation["stable_keyset_cursor_json"] = {"next_requirement_ordinal": 1}
            else:
                self.operation["stable_keyset_cursor_json"] = {
                    "next_requirement_ordinal": checkpoint.next_requirement_ordinal
                }
        self.batch["row_version"] += 1
        return dict(self.operation)

    def seal_planning_batch(self, **kwargs):  # noqa: ANN003, ANN201
        self.seal_calls.append(kwargs)
        return SealedHistoricalRangeBatch(
            batch_id=self.plan.batch_id,
            canonical_batch_id=self.plan.batch_id,
            range_run_ids=tuple(
                kwargs["resolved"].range_run_id(program.research_program_id)
                for program in self.plan.frozen_programs
            ),
            deduplicated=False,
        )


def test_planning_service_rolls_discover_to_verify_and_seals(tmp_path: Path) -> None:
    plan = _plan(2)
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r-service")
    repository = _PlanningRepository(plan)
    service = HistoricalRangePlanningService(
        program_resolver=object(),
        calendar_resolver=object(),
        code_release_resolver=object(),
        requirement_planner=object(),
        catalog_executor=_CatalogExecutor(),
        repository=repository,
        artifact_store=store,
        selection_semantics_version=plan.frozen_programs[0].selection_semantics_version,
        selection_semantics_hash=plan.frozen_programs[0].selection_semantics_hash,
        list_semantics_version=plan.frozen_programs[0].list_semantics_version,
        list_semantics_hash=plan.frozen_programs[0].list_semantics_hash,
    )
    lease = datetime.now(UTC) + timedelta(minutes=5)

    discover = service.execute_claimed_chunk(
        operation_id="operation-catalog",
        expected_row_version=1,
        expected_fencing_token=1,
        next_worker_id="worker-verify",
        next_lease_token="verify",
        next_lease_expires_at=lease,
    )
    assert discover.operation["catalog_phase"] == "VERIFY"
    assert discover.sealed_batch is None

    verified = service.execute_claimed_chunk(
        operation_id="operation-catalog",
        expected_row_version=2,
        expected_fencing_token=2,
        next_worker_id="unused-worker",
        next_lease_token="unused",
        next_lease_expires_at=lease,
    )
    assert verified.operation["status"] == "COMPLETED"
    assert verified.sealed_batch is not None
    assert len(repository.seal_calls) == 1
    assert len(repository.chain) == 2

    recovered = service.seal_completed_catalog(operation_id="operation-catalog")
    assert recovered.batch_id == plan.batch_id
    assert len(repository.seal_calls) == 2
