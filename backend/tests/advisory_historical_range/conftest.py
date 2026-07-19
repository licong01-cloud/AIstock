from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeAdmittedComponentV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeDatePlanV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeProgramWarmupComponentV1,
    HistoricalRangeProgramWarmupRangeV1,
    HistoricalRangeResearchBatchRequestV1,
    ResearchProgramSpecV1,
    ResolvedHistoricalRangeRequestV1,
)


def digest(value: Any) -> str:
    return canonical_json_sha256(value)


def artifact_ref(kind: HistoricalRangeArtifactKind, seed: str) -> HistoricalRangeArtifactRefV1:
    semantic_hash = digest(f"semantic:{seed}")
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.DATE_PLAN: "date-plans",
        HistoricalRangeArtifactKind.FROZEN_PROGRAM: "frozen-programs",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
        HistoricalRangeArtifactKind.RANGE_RECEIPT: "range-receipts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
        HistoricalRangeArtifactKind.SUMMARY: "summaries",
        HistoricalRangeArtifactKind.DATASET_BRIDGE: "dataset-bridges",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{semantic_hash}.json",
        producer_contract_version="phase1r_r1",
        payload_schema_version="test_payload_v1",
        semantic_content_hash=semantic_hash,
        payload_sha256=digest(f"payload:{seed}"),
        file_sha256=digest(f"file:{seed}"),
    )


def research_spec(
    *,
    name: str = "research program",
    package_id: str = "pkg_test",
    target_count: int = 5,
) -> ResearchProgramSpecV1:
    return ResearchProgramSpecV1(
        program_name=name,
        package_id=package_id,
        target_count=target_count,
        review_policy={"replacement_budget": 2},
        runtime_config={"topk": 20},
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
    )


def existing_spec() -> ExistingProgramSpecV1:
    return ExistingProgramSpecV1(
        program_id="adv_program_1",
        expected_program_version=3,
        expected_binding_version_id="binding_v7",
    )


def frozen_program(
    spec: ResearchProgramSpecV1 | ExistingProgramSpecV1,
    *,
    alpha_mode: HistoricalRangeAlphaMode = HistoricalRangeAlphaMode.MULTI_ALPHA,
) -> HistoricalRangeFrozenProgramV1:
    package_id = spec.package_id if isinstance(spec, ResearchProgramSpecV1) else "pkg_existing"
    component_specs = (
        (("leg_a", "0.6"), ("leg_b", "0.4"))
        if alpha_mode is HistoricalRangeAlphaMode.MULTI_ALPHA
        else (("alpha", "1"),)
    )
    projection = HistoricalRangeAdmittedPackageProjectionV1(
        package_id=package_id,
        package_version=8,
        manifest_sha256=digest("manifest"),
        alpha_mode=alpha_mode,
        components=tuple(
            HistoricalRangeAdmittedComponentV1(
                component_id=component_id,
                weight=weight,
                factor_order=(f"factor_{component_id}_a", f"factor_{component_id}_b"),
                runtime_input_identity_hash=digest(f"runtime-input:{component_id}"),
                lookback_contract_hash=digest(f"lookback:{component_id}"),
            )
            for component_id, weight in component_specs
        ),
    )
    source = isinstance(spec, ExistingProgramSpecV1)
    return HistoricalRangeFrozenProgramV1(
        research_program_id=spec.research_program_id,
        source_program_id=spec.program_id if source else None,
        source_program_version=spec.expected_program_version if source else None,
        source_binding_version_id=spec.expected_binding_version_id if source else None,
        package_id=projection.package_id,
        package_version=8,
        manifest_sha256=digest("manifest"),
        alpha_mode=alpha_mode,
        program_config_hash=digest(spec.semantic_payload()),
        runtime_config_hash=digest("runtime"),
        review_policy_hash=digest("review"),
        code_release_id="release_20260719",
        code_release_hash=digest("release"),
        selection_semantics_version="selection_v1",
        selection_semantics_hash=digest("selection semantics"),
        list_semantics_version="list_v1",
        list_semantics_hash=digest("list semantics"),
        target_package_asset_root_hash=digest("asset root"),
        input_warmup_contract_hash=digest("warmup contract"),
        admitted_package_projection_hash=digest(projection.model_dump(mode="json")),
        admitted_package_projection=projection,
    )


def date_plan(
    *,
    trade_dates: tuple[date, ...] | None = None,
    research_program_ids: tuple[str, ...] = ("default",),
    component_ids: tuple[str, ...] = ("leg_a", "leg_b"),
) -> HistoricalRangeDatePlanV1:
    dates = trade_dates or (date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3))
    return HistoricalRangeDatePlanV1(
        calendar_id="cn_a_share",
        calendar_version="calendar_20260719",
        start_trade_date=dates[0],
        end_trade_date=dates[-1],
        ordered_trade_dates=dates,
        completed_trade_date_watermark=date(2026, 7, 18),
        per_program_input_warmup_ranges={
            research_program_id: HistoricalRangeProgramWarmupRangeV1(
                research_program_id=research_program_id,
                components=tuple(
                    HistoricalRangeProgramWarmupComponentV1(
                        component_id=component_id,
                        warmup_start_trade_date=dates[0] - timedelta(days=120)
                        if component_id == "leg_b"
                        else dates[0] - timedelta(days=30),
                        range_start_trade_date=dates[0],
                        lookback_contract_hash=digest(f"lookback:{component_id}"),
                    )
                    for component_id in component_ids
                ),
            )
            for research_program_id in research_program_ids
        },
    )


def resolved_request(
    *,
    specs: tuple[ResearchProgramSpecV1 | ExistingProgramSpecV1, ...] | None = None,
    client_key: str = "client-key-1",
    request_id: str = "request-1",
    requested_at: datetime | None = None,
    trade_dates: tuple[date, ...] | None = None,
) -> ResolvedHistoricalRangeRequestV1:
    selected_specs = specs or (research_spec(),)
    program_ids = tuple(spec.research_program_id for spec in selected_specs)
    plan = date_plan(trade_dates=trade_dates, research_program_ids=program_ids)
    request = HistoricalRangeResearchBatchRequestV1(
        request_id=request_id,
        client_idempotency_key=client_key,
        program_specs=selected_specs,
        start_trade_date=plan.start_trade_date,
        end_trade_date=plan.end_trade_date,
        requested_at=requested_at or datetime(2026, 7, 19, 1, 2, tzinfo=UTC),
        requested_by="local-user",
    )
    frozen = tuple(frozen_program(spec) for spec in request.program_specs)
    return ResolvedHistoricalRangeRequestV1(
        request=request,
        frozen_programs=frozen,
        date_plan=plan,
        source_revision_catalog_hash=digest("source revision catalog"),
        selection_semantics_version="selection_v1",
        selection_semantics_hash=digest("selection semantics"),
        list_semantics_version="list_v1",
        list_semantics_hash=digest("list semantics"),
    )
