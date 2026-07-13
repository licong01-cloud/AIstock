from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Barrier

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.calculation_evidence import LocalCalculationEvidenceStore
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchRequest,
    CaptureBatchStatus,
    CaptureMembership,
    CapturePlan,
    InMemoryCaptureBatchRepository,
)
from backend.services.advisory_phase1.label_builder import (
    InMemoryOutcomeLabelRepository,
    InMemorySelectedLabelMappingRepository,
    LabelAppendRequest,
    LabelBuilder,
    LabelBuilderError,
    LabelSelectionPolicy,
    LabelSelectionRequest,
    LabelSelectionStatus,
    OutcomeLabelPayload,
    OutcomeLabelVersion,
    SelectedLabelMapping,
    StageEvidenceReference,
    UniverseOutcomePlan,
    UniverseRawOutcomeRow,
    _alpha_raw_stage,
    _canonical_revalidate,
    _validate_header_payload,
    TerminalFirstLabelSelector,
    UniverseConstituent,
    enumerate_candidate_labels,
    enumerate_universe_outcome_plans,
    label_key_hash,
)
from backend.services.advisory_phase1.label_capture import (
    LabelCaptureAdmissionContext,
    LabelCaptureContractError,
    SelectedObservationMappingReference,
    build_label_capture_request,
)
from backend.services.advisory_phase1.label_policy import (
    BarrierPolicy,
    BenchmarkPolicy,
    CashReturnPolicy,
    CashReturnRule,
    CostPolicy,
    EntryBasis,
    EntryExecutionPolicy,
    ExitBasis,
    LabelPolicyBundle,
    MarketDataPolicy,
    OutcomePolicySet,
    Projection,
    StyleFamily,
    TerminalPolicy,
    TradingCalendar,
)
from backend.services.advisory_phase1.observation_selector import (
    FixtureObservationVersion,
    FixtureObservationVersionSelector,
    ObservationSelectionPolicy,
    ObservationStatus,
    ObservationSelectionRequest,
)
from backend.services.advisory_phase1.outcome_engine import (
    DailyPriceBar,
    OutcomeCalculationRequest,
    OutcomeEngine,
    OutcomeEventStatus,
    OutcomeOwner,
    OwnerType,
    PricePath,
    SourceMemberBinding,
    TerminalDisposition,
    TerminalResolution,
)
from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    build_source_revision_set,
)
from backend.services.advisory_phase1.stage_trace import TraceCaptureBinding, TraceCapturePolicy


UTC = timezone.utc
AS_OF = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _source_revision_set():
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: datetime(2026, 7, 2, 9, 0, tzinfo=UTC))
    event = ledger.append(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="PRICE_PATH",
            partition_key={"fixture": "label-builder"},
            revision_id="fixture-r1",
            event_revision_no=1,
            event_type=SourceAvailabilityEventType.INGESTED,
            schema_fingerprint="fixture-schema-v1",
            row_count=3,
            partition_content_hash=HASH_A,
            quality_status="PASS",
            created_by_service_principal="fixture",
        )
    )
    source = event.input
    member = SourceRevisionMemberInput(
        source_role=source.source_role,
        dataset_name=source.dataset_name,
        query_template_id="fixture-price-v1",
        query_template_version="1",
        query_template_hash=HASH_B,
        bound_parameter_hash=HASH_C,
        enforced_cutoff_predicate_hash=HASH_D,
        partition_key=source.partition_key,
        revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
        revision_id=source.revision_id,
        availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
        business_min_date=date(2026, 7, 3),
        business_max_date=date(2026, 7, 7),
        available_at_min=source.formal_available_at,
        available_at_max=source.formal_available_at,
        schema_fingerprint=source.schema_fingerprint,
        row_count=source.row_count,
        partition_content_hash=source.partition_content_hash,
        quality_status=source.quality_status,
        availability_event=event,
        research_only=True,
    )
    return build_source_revision_set(
        query_registry_hash=HASH_E,
        requested_source_cutoff=AS_OF,
        label_as_of_ts=AS_OF,
        research_only=True,
        members=[member],
    )


def _source_binding() -> SourceMemberBinding:
    member = _source_revision_set().members[0]
    return SourceMemberBinding(
        source_role=member.source_role,
        source_member_key=member.member_key,
        partition_content_hash=member.partition_content_hash,
    )


def _policies() -> OutcomePolicySet:
    calendar = TradingCalendar(
        calendar_version="fixture-calendar-v1",
        trading_dates=(date(2026, 7, 3), date(2026, 7, 6), date(2026, 7, 7)),
    )
    execution = EntryExecutionPolicy(
        policy_id="fixture-entry-v1",
        entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
        exit_basis=ExitBasis.HORIZON_CLOSE_V1,
        entry_time=time(9, 30),
        exit_time=time(15, 0),
    )
    cost = CostPolicy(
        policy_id="fixture-cost-v1",
        commission_buy_rate=Decimal("0.0003"),
        commission_sell_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"),
        stamp_duty_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0"),
        transfer_fee_sell_rate=Decimal("0"),
        slippage_bps=Decimal("5"),
        lot_size=10,
    )
    benchmark = BenchmarkPolicy(universe_layer="fixture-universe")
    cash_return = CashReturnPolicy(policy_id=CashReturnRule.CASH_RETURN_ZERO_V1, cash_return_rate=Decimal("0"))
    barrier = BarrierPolicy(policy_id="fixture-barrier-v1", target_return=Decimal("0.10"), stop_return=Decimal("-0.10"))
    terminal = TerminalPolicy(
        policy_id="fixture-terminal-v1",
        terminal_return_rule="EXACT_SETTLEMENT_OR_UNAVAILABLE_V1",
        censor_rule="EXPLICIT_RIGHT_CENSOR_REASON_V1",
    )
    assert calendar.calendar_hash and execution.policy_hash and cost.policy_hash
    assert benchmark.policy_hash and cash_return.policy_hash and barrier.policy_hash and terminal.policy_hash
    bundle = LabelPolicyBundle(
        label_policy_id="fixture-label-v1",
        label_policy_hash=HASH_B,
        label_policy_schema_version="fixture-label-schema-v1",
        phase1_handoff_bundle_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_E,
        audit_target_id="fixture-target",
        package_id="fixture-package",
        manifest_sha256=HASH_F,
        alpha_mode="single_alpha",
        style_family=StyleFamily.SHORT_REBOUND,
        style_assignment_policy_id="fixture-style-v1",
        style_assignment_policy_hash=HASH_A,
        style_decided_at=date(2026, 7, 3),
        calendar_version=calendar.calendar_version,
        calendar_hash=calendar.calendar_hash,
        price_policy_hash=HASH_B,
        adjustment_policy_hash=HASH_C,
        entry_execution_policy_hash=execution.policy_hash,
        cost_policy_hash=cost.policy_hash,
        benchmark_policy_hash=benchmark.policy_hash,
        cash_return_policy_hash=cash_return.policy_hash,
        terminal_return_policy_hash=terminal.policy_hash,
        barrier_policy_hash=barrier.policy_hash,
        corporate_action_policy_hash=HASH_D,
        symbol_normalization_policy_hash=HASH_E,
        horizons=(1,),
        projections_by_horizon={1: (Projection.RETURN_GROSS,)},
        gap_1d_enabled=False,
        candidate_reference_notional=Decimal("1000"),
        benchmark_portfolio_notional=Decimal("1000"),
    )
    return OutcomePolicySet(
        bundle=bundle,
        calendar=calendar,
        market_data=MarketDataPolicy(
            price_policy_hash=HASH_B,
            adjustment_policy_hash=HASH_C,
            corporate_action_policy_hash=HASH_D,
            symbol_normalization_policy_hash=HASH_E,
        ),
        execution=execution,
        cost=cost,
        benchmark=benchmark,
        cash_return=cash_return,
        barrier=barrier,
        terminal=terminal,
    )


def _bar(trade_date: date, *, entry_executable: bool = True) -> DailyPriceBar:
    return DailyPriceBar(
        trade_date=trade_date,
        open_li=Decimal("10000"),
        high_li=Decimal("11200"),
        low_li=Decimal("9900"),
        close_li=Decimal("11000"),
        adj_factor=Decimal("1"),
        entry_executable=entry_executable,
        sell_executable=True,
        source_available_at=AS_OF - timedelta(hours=1),
        price_source=_source_binding(),
        adjustment_source=_source_binding(),
        tradability_source=_source_binding(),
    )


def _path(*, entry_executable: bool = True) -> PricePath:
    return PricePath(
        symbol="000001.SZ",
        bars=(
            _bar(date(2026, 7, 3)),
            _bar(date(2026, 7, 6), entry_executable=entry_executable),
            _bar(date(2026, 7, 7)),
        ),
    )


def _trace_binding(*, batch_id: str) -> TraceCaptureBinding:
    return TraceCaptureBinding(
        control_binding_event_hash=HASH_A,
        binding_id="fixture-capture-binding",
        binding_version="1",
        handoff_readiness_hash=HASH_D,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_E,
        capture_batch_id=batch_id,
        capture_fencing_token=1,
        capture_policy=TraceCapturePolicy(
            policy_id="fixture-capture-policy",
            policy_version="1",
            max_candidates=20,
            max_bytes=100_000,
            max_capture_ms=1_000,
        ),
    )


def _capture_plan(source_set, *, valid_no_candidate: bool = False, alpha_mode: str = "single_alpha") -> CapturePlan:
    return CapturePlan(
        selection_run_id="fixture-selection-run",
        package_id="fixture-package",
        manifest_sha256=HASH_F,
        decision_as_of_trade_date=date(2026, 7, 3).isoformat(),
        selection_as_of_trade_date=date(2026, 7, 3).isoformat(),
        target_trade_date=date(2026, 7, 6).isoformat(),
        decision_cutoff_ts=AS_OF,
        alpha_mode=alpha_mode,
        selection_runtime_semantics_hash=HASH_A,
        package_effective_config_hash=HASH_B,
        calendar_version="fixture-calendar-v1",
        calendar_hash=HASH_C,
        stable_signal_semantics_hash=HASH_D,
        canonical_signal_scope_hash=HASH_E,
        phase0a_audit_id="fixture-audit",
        phase0a_audit_manifest_hash=HASH_F,
        handoff_readiness_hash=HASH_D,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_E,
        signal_source_revision_set_id=source_set.source_revision_set_id,
        signal_source_revision_set_hash=source_set.source_revision_set_hash,
        phase0a_signal_context_hash=HASH_A,
        evidence_bundle_hash=HASH_B,
        selection_evidence_id="fixture-selection-evidence",
        selection_evidence_hash=HASH_C,
        selection_run_content_hash=HASH_D,
        selection_score_artifact_id="fixture-selection-artifact",
        selection_score_artifact_hash=HASH_E,
        runtime_profile_version_id="fixture-runtime-profile",
        runtime_profile_version_hash=HASH_F,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash=HASH_A,
        universe_policy_hash=HASH_B,
        symbol_normalization_policy_hash=HASH_C,
        valid_no_candidate=valid_no_candidate,
        evidence_available_at=AS_OF,
        audit_target_id="fixture-target",
        target_scope_hash=HASH_D,
        capability="HISTORICAL_RESEARCH",
        oos_interval_id="fixture-oos",
        oos_interval_hash=HASH_E,
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        signal_evidence_level="RETROSPECTIVE_RESEARCH_ONLY",
        effective_cutoff_date=date(2026, 7, 3).isoformat(),
        program_id="fixture-program",
        binding_version_id="fixture-binding-version",
        source_run_id="fixture-source-run",
        lineage_source_type="PHASE0A_AUDIT",
    )


def _observation(
    source_set,
    *,
    candidates: tuple[dict[str, object], ...],
    alpha_mode: str = "single_alpha",
    stage_capability: str = "FULL",
) -> FixtureObservationVersion:
    stage_payload = {
        "stage": "alpha_raw",
        "capability_status": stage_capability,
        "input_count": len(candidates),
        "output_count": len(candidates),
        "excluded_count": 0,
        "candidates": list(candidates),
    }
    stage = {**stage_payload, "content_hash": canonical_json_sha256(canonicalize(stage_payload))}
    payload = {
        "schema_version": "advisory_signal_observation_version_v1",
        "canonical_signal_id": "advsig-fixture",
        "plan": {
            "admission_scope_id": "fixture-scope",
            "admission_scope_hash": HASH_E,
            "handoff_readiness_hash": HASH_D,
            "signal_source_revision_set_id": source_set.source_revision_set_id,
            "signal_source_revision_set_hash": source_set.source_revision_set_hash,
            "capability": "HISTORICAL_RESEARCH",
            "evidence_available_at": AS_OF.isoformat().replace("+00:00", "Z"),
            "decision_as_of_trade_date": date(2026, 7, 3).isoformat(),
            "alpha_mode": alpha_mode,
        },
        "stage_evidence_bundle_hash": canonical_json_sha256([stage["content_hash"]]),
        "observation_status": "COMPLETE",
        "stages": [stage],
        "observation_revision_no": 1,
        "supersedes_observation_version_id": None,
    }
    content_hash = canonical_json_sha256(canonicalize(payload))
    return FixtureObservationVersion(
        canonical_signal_id="advsig-fixture",
        observation_version_id=f"osv_{content_hash[:20]}",
        observation_content_hash=content_hash,
        observation_revision_no=1,
        supersedes_observation_version_id=None,
        evidence_available_at=AS_OF,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_E,
        handoff_readiness_hash=HASH_D,
        signal_source_revision_set_id=source_set.source_revision_set_id,
        signal_source_revision_set_hash=source_set.source_revision_set_hash,
        observation_status=ObservationStatus.COMPLETE,
        capability="HISTORICAL_RESEARCH",
        stage_content_hashes=(stage["content_hash"],),
        stage_evidence_bundle_hash=payload["stage_evidence_bundle_hash"],
        observation_payload=payload,
    )


def _default_candidate(alpha_mode: str) -> dict[str, object]:
    candidate: dict[str, object] = {
        "symbol": "000001.SZ",
        "membership_status": "INCLUDED",
        "rank": 1,
        "score_decimal": "1.0",
        "component_capability": "NOT_APPLICABLE",
    }
    if alpha_mode == "multi_alpha":
        component_evidence = {
            "legs": [
                {"alpha_id": "lstm_leg", "lookback_days": 60, "input_window": "PRICE_60D"},
                {"alpha_id": "fundamental_leg", "lookback_days": 4, "input_window": "REPORT_4Q"},
            ],
            "parent_rank": 1,
            "parent_score_decimal": "1.0",
        }
        candidate.update(
            {
                "component_capability": "FULL",
                "component_evidence_schema_version": "multi_alpha_component_evidence_v1",
                "component_evidence": component_evidence,
                "component_evidence_hash": canonical_json_sha256(canonicalize(component_evidence)),
                "component_reason_codes": [],
            }
        )
    return candidate


def _context(*, candidates: tuple[dict[str, object], ...] | None = None, valid_no_candidate: bool = False, alpha_mode: str = "single_alpha", stage_capability: str = "FULL"):
    policies = _policies()
    source_set = _source_revision_set()
    plan = _capture_plan(source_set, valid_no_candidate=valid_no_candidate, alpha_mode=alpha_mode)
    binding = _trace_binding(batch_id="observation-capture")
    observation = _observation(
        source_set,
        candidates=candidates
        if candidates is not None
        else (_default_candidate(alpha_mode),),
        alpha_mode=alpha_mode,
        stage_capability=stage_capability,
    )
    mapping = FixtureObservationVersionSelector().select(
        request=ObservationSelectionRequest(
            selection_policy=ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            canonical_signal_id=observation.canonical_signal_id,
            requested_source_cutoff=AS_OF,
            required_observation_status=ObservationStatus.COMPLETE,
            required_capability="HISTORICAL_RESEARCH",
            admission_scope_id="fixture-scope",
            admission_scope_hash=HASH_E,
            handoff_readiness_hash=HASH_D,
            signal_source_revision_set_hash=source_set.source_revision_set_hash,
        ),
        observation_versions=(observation,),
    )
    source_repository = InMemoryCaptureBatchRepository(now_provider=lambda: AS_OF)
    source_batch = source_repository.create(
        CaptureBatchRequest(capture_batch_id="observation-capture", binding=binding, plans=(plan,))
    )
    source_batch = source_repository.acquire(
        capture_batch_id=source_batch.request.capture_batch_id,
        expected_row_version=source_batch.row_version,
        lease_seconds=300,
    )
    membership = CaptureMembership(
        evidence_role="selected_observation_mapping",
        evidence_id=str(mapping.selected_mapping_id),
        evidence_content_hash=str(mapping.selected_mapping_hash),
    )
    source_batch = source_repository.add_membership(
        capture_batch_id=source_batch.request.capture_batch_id,
        expected_row_version=source_batch.row_version,
        fencing_token=source_batch.fencing_token,
        membership=membership,
    )
    source_batch = source_repository.complete(
        capture_batch_id=source_batch.request.capture_batch_id,
        expected_row_version=source_batch.row_version,
        fencing_token=source_batch.fencing_token,
    )
    context = LabelCaptureAdmissionContext(
        source_batch=source_batch,
        source_memberships=(membership,),
        source_plans=(plan,),
        selected_observation_mappings=(mapping,),
        selected_observations=(observation,),
        label_policy_bundle=policies.bundle,
        label_source_revision_set=source_set,
    )
    return context, policies, source_set


def _candidate_request(*, descriptor, policies, source_set, entry_executable: bool = True) -> OutcomeCalculationRequest:
    owner = OutcomeOwner(
        owner_type=OwnerType.CANDIDATE,
        owner_key=descriptor.label_key_hash,
        canonical_signal_id=descriptor.canonical_signal_id,
        observation_version_id=descriptor.observation_version_id,
        candidate_stage_evidence_id=descriptor.candidate_stage_evidence_id,
        symbol=descriptor.symbol,
        decision_as_of_trade_date=descriptor.decision_as_of_trade_date,
    )
    return OutcomeCalculationRequest(
        owner=owner,
        policies=policies,
        horizon_trading_days=descriptor.horizon_trading_days,
        projection=Projection(descriptor.projection),
        label_as_of_ts=AS_OF,
        label_source_revision_set=source_set,
        price_path=_path(entry_executable=entry_executable),
        terminal=TerminalResolution(disposition=TerminalDisposition.NONE),
    )


def _append_request(*, descriptor, policies, source_set, result, uri: str, predecessor=None) -> LabelAppendRequest:
    return LabelAppendRequest(
        label_key_hash=descriptor.label_key_hash,
        expected_predecessor_version_id=predecessor.label_version_id if predecessor else None,
        expected_predecessor_version_hash=predecessor.label_content_hash if predecessor else None,
        expected_predecessor_revision_no=predecessor.label_revision_no if predecessor else None,
        label_policy_bundle_id=str(policies.bundle.label_policy_bundle_id),
        label_policy_bundle_hash=str(policies.bundle.label_policy_bundle_hash),
        label_policy_hash=policies.bundle.label_policy_hash,
        label_source_revision_set_id=source_set.source_revision_set_id,
        label_source_revision_set_hash=source_set.source_revision_set_hash,
        owner=result.owner,
        horizon_trading_days=result.horizon_trading_days,
        projection=result.projection,
        outcome_result=result,
        projection_payload_hash=str(result.projection_payload_hash),
        calculation_evidence_sha256=str(result.calculation_evidence.evidence_hash),
        calculation_evidence_size_bytes=len(result.calculation_evidence.canonical_bytes()),
        calculation_evidence_store_backend_hash=HASH_F,
        calculation_evidence_uri=uri,
    )


def _selection_request(*, descriptor, source_set) -> LabelSelectionRequest:
    return LabelSelectionRequest(
        selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
        label_key_hash=descriptor.label_key_hash,
        requested_label_as_of_ts=AS_OF,
        required_maturity_statuses=("MATURED",),
        required_outcome_event_statuses=(OutcomeEventStatus.NONE,),
        expected_observation_version_id=descriptor.observation_version_id,
        expected_candidate_stage_evidence_id=descriptor.candidate_stage_evidence_id,
        expected_label_source_revision_set_hash=source_set.source_revision_set_hash,
    )


def test_append_uri_retry_and_concurrent_same_request_produce_one_revision() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    request = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=result,
        uri="file:///first-evidence",
    )
    retry = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=result,
        uri="file:///moved-evidence",
    )
    repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    first = repository.append(request=request, created_by_capture_batch_id="label-capture")
    assert repository.append(request=retry, created_by_capture_batch_id="other-capture") == first
    assert first.calculation_evidence_uri == "file:///first-evidence"

    barrier = Barrier(6)

    def append_once():
        barrier.wait()
        return repository.append(request=request, created_by_capture_batch_id="parallel-capture")

    with ThreadPoolExecutor(max_workers=6) as pool:
        versions = list(pool.map(lambda _: append_once(), range(6)))
    assert {version.label_version_id for version in versions} == {first.label_version_id}
    assert len(repository.chain_for(descriptor.label_key_hash)) == 1
    assert repository.header_for(str(first.label_version_id)) is not None
    assert repository.payload_for(str(first.label_version_id)) is not None


def test_terminal_first_selector_never_falls_back_to_old_matured_revision() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    engine = OutcomeEngine()
    matured = engine.calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    unavailable = engine.calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set, entry_executable=False)
    )
    repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    first = repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=matured,
            uri="file:///matured",
        ),
        created_by_capture_batch_id="label-capture-1",
    )
    empty_repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    with pytest.raises(LabelBuilderError, match="first label revision cannot name"):
        empty_repository.append(
            request=_append_request(
                descriptor=descriptor,
                policies=policies,
                source_set=source_set,
                result=matured,
                uri="file:///unexpected-predecessor",
                predecessor=first,
            ),
            created_by_capture_batch_id="label-capture-unexpected-predecessor",
        )
    stale_payload = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=matured,
        uri="file:///stale-predecessor",
        predecessor=first,
    ).model_dump(mode="python", exclude={"label_append_request_hash"})
    stale_payload["expected_predecessor_revision_no"] = first.label_revision_no + 1
    with pytest.raises(LabelBuilderError, match="stale or not terminal"):
        repository.append(
            request=LabelAppendRequest.model_validate(stale_payload),
            created_by_capture_batch_id="label-capture-stale-predecessor",
        )
    second = repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=unavailable,
            uri="file:///unavailable",
            predecessor=first,
        ),
        created_by_capture_batch_id="label-capture-2",
    )
    selected = TerminalFirstLabelSelector().select(
        request=_selection_request(descriptor=descriptor, source_set=source_set),
        label_versions=repository.chain_for(descriptor.label_key_hash),
    )
    assert second.label_revision_no == 2
    assert selected.terminal_label_version_id == second.label_version_id
    assert selected.selection_status is LabelSelectionStatus.UNAVAILABLE

    exact = LabelSelectionRequest(
        selection_policy=LabelSelectionPolicy.EXACT_REVISION_V1,
        label_key_hash=descriptor.label_key_hash,
        requested_label_as_of_ts=AS_OF,
        required_maturity_statuses=("MATURED",),
        required_outcome_event_statuses=(OutcomeEventStatus.NONE,),
        expected_observation_version_id=descriptor.observation_version_id,
        expected_candidate_stage_evidence_id=descriptor.candidate_stage_evidence_id,
        expected_label_source_revision_set_hash=source_set.source_revision_set_hash,
        explicit_label_version_id=str(first.label_version_id),
    )
    exact_result = TerminalFirstLabelSelector().select(
        request=exact,
        label_versions=repository.chain_for(descriptor.label_key_hash),
    )
    assert exact_result.selection_status is LabelSelectionStatus.CONFLICT

    selector = TerminalFirstLabelSelector()
    unavailable_without_versions = selector.select(
        request=_selection_request(descriptor=descriptor, source_set=source_set),
        label_versions=(),
    )
    assert unavailable_without_versions.selection_status is LabelSelectionStatus.UNAVAILABLE

    early_payload = _selection_request(descriptor=descriptor, source_set=source_set).model_dump(
        mode="python",
        exclude={"selection_policy_hash", "selector_request_hash"},
    )
    early_payload["requested_label_as_of_ts"] = AS_OF - timedelta(seconds=1)
    unavailable_before_first_revision = selector.select(
        request=LabelSelectionRequest.model_validate(early_payload),
        label_versions=(first,),
    )
    assert unavailable_before_first_revision.selection_status is LabelSelectionStatus.UNAVAILABLE

    wrong_key_payload = _selection_request(descriptor=descriptor, source_set=source_set).model_dump(
        mode="python",
        exclude={"selection_policy_hash", "selector_request_hash"},
    )
    wrong_key_payload["label_key_hash"] = HASH_A
    wrong_key = selector.select(
        request=LabelSelectionRequest.model_validate(wrong_key_payload),
        label_versions=(first,),
    )
    assert wrong_key.selection_status is LabelSelectionStatus.CONFLICT


def test_append_rejects_stale_predecessor_and_malformed_terminal_chain() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    first = repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=result,
            uri="file:///first",
        ),
        created_by_capture_batch_id="label-capture-1",
    )
    with pytest.raises(LabelBuilderError, match="requires new source or calculation evidence"):
        repository.append(
            request=_append_request(
                descriptor=descriptor,
                policies=policies,
                source_set=source_set,
                result=result,
                uri="file:///same-evidence",
                predecessor=first,
            ),
            created_by_capture_batch_id="label-capture-same-evidence",
        )
    right_censored_payload = _candidate_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
    ).model_dump(mode="python")
    right_censored_payload["terminal"] = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol=descriptor.symbol,
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        censor_reason_code="FIXTURE_RIGHT_CENSOR",
    )
    right_censored_result = OutcomeEngine().calculate(
        OutcomeCalculationRequest.model_validate(right_censored_payload)
    )
    with pytest.raises(LabelBuilderError, match="transition is not allowed"):
        repository.append(
            request=_append_request(
                descriptor=descriptor,
                policies=policies,
                source_set=source_set,
                result=right_censored_result,
                uri="file:///right-censored",
                predecessor=first,
            ),
            created_by_capture_batch_id="label-capture-right-censored",
        )
    changed_result = OutcomeEngine().calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set, entry_executable=False)
    )
    with pytest.raises(LabelBuilderError, match="requires the terminal predecessor"):
        repository.append(
            request=_append_request(
                descriptor=descriptor,
                policies=policies,
                source_set=source_set,
                result=changed_result,
                uri="file:///stale",
            ),
            created_by_capture_batch_id="label-capture-2",
        )
    second = repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=changed_result,
            uri="file:///second",
            predecessor=first,
        ),
        created_by_capture_batch_id="label-capture-2",
    )
    malformed = second.model_copy(update={"label_revision_no": 3})
    selected = TerminalFirstLabelSelector().select(
        request=_selection_request(descriptor=descriptor, source_set=source_set),
        label_versions=(first, malformed),
    )
    assert selected.selection_status is LabelSelectionStatus.CONFLICT


@pytest.mark.parametrize(
    ("field_name", "field_value", "message"),
    (
        ("schema_version", "wrong", "unsupported label append request schema"),
        ("expected_predecessor_version_id", "advlabel-stale", "nullable together"),
        ("projection_schema_version", "wrong", "unsupported projection schema"),
        ("label_key_hash", HASH_A, "candidate label key"),
        ("projection_payload_hash", HASH_A, "projection identity"),
        ("calculation_evidence_sha256", HASH_A, "evidence sha"),
    ),
)
def test_append_contract_rejects_semantic_drift(field_name: str, field_value: object, message: str) -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    request = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=result,
        uri="file:///contract",
    )
    payload = request.model_dump(mode="python", exclude={"label_append_request_hash"})
    payload[field_name] = field_value
    with pytest.raises(ValueError, match=message):
        LabelAppendRequest.model_validate(payload)


def test_append_and_version_contracts_reject_remaining_identity_drift() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set)
    )
    request = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=result,
        uri="file:///identity",
    )
    owner_drift = request.model_dump(mode="python", exclude={"label_append_request_hash"})
    owner_drift["owner"] = request.owner.model_copy(update={"symbol": "000002.SZ"})
    with pytest.raises(ValueError, match="owner does not match"):
        LabelAppendRequest.model_validate(owner_drift)
    with pytest.raises(ValueError, match="request hash"):
        LabelAppendRequest.model_validate(
            {**request.model_dump(mode="python"), "label_append_request_hash": HASH_A}
        )

    repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    version = repository.append(
        request=request,
        created_by_capture_batch_id="label-capture-identity",
    )
    base = version.model_dump(mode="python", exclude={"label_content_hash", "label_version_id"})
    mutations = (
        ({**base, "supersedes_label_version_id": "advlabel-partial"}, "nullable together"),
        ({**base, "label_revision_no": 2}, "requires predecessor"),
        ({**base, "owner": version.owner.model_copy(update={"symbol": "000002.SZ"})}, "owner does not match"),
        ({**base, "horizon_trading_days": version.horizon_trading_days + 1}, "does not match outcome"),
        ({**base, "label_content_hash": HASH_A}, "label_content_hash"),
        ({**base, "label_version_id": "advlabel-wrong"}, "label_version_id"),
    )
    for payload, message in mutations:
        with pytest.raises(ValueError, match=message):
            OutcomeLabelVersion.model_validate(payload)


def test_selector_and_mapping_contracts_reject_remaining_identity_drift() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    selection = _selection_request(descriptor=descriptor, source_set=source_set)
    latest_with_explicit = selection.model_dump(
        mode="python",
        exclude={"selection_policy_hash", "selector_request_hash"},
    )
    latest_with_explicit["explicit_label_version_id"] = "advlabel-explicit"
    with pytest.raises(ValueError, match="latest label selection"):
        LabelSelectionRequest.model_validate(latest_with_explicit)
    duplicate_events = selection.model_dump(
        mode="python",
        exclude={"selection_policy_hash", "selector_request_hash"},
    )
    duplicate_events["required_outcome_event_statuses"] = (
        OutcomeEventStatus.NONE,
        OutcomeEventStatus.NONE,
    )
    with pytest.raises(ValueError, match="outcome event statuses"):
        LabelSelectionRequest.model_validate(duplicate_events)
    with pytest.raises(ValueError, match="policy hash"):
        LabelSelectionRequest.model_validate(
            {**selection.model_dump(mode="python"), "selection_policy_hash": HASH_A}
        )
    with pytest.raises(ValueError, match="selector request hash"):
        LabelSelectionRequest.model_validate(
            {**selection.model_dump(mode="python"), "selector_request_hash": HASH_A}
        )

    result = OutcomeEngine().calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set)
    )
    version = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF).append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=result,
            uri="file:///selector-contract",
        ),
        created_by_capture_batch_id="label-capture-selector-contract",
    )
    mapping = TerminalFirstLabelSelector().select(request=selection, label_versions=(version,))
    mapping_base = mapping.model_dump(
        mode="python",
        exclude={"selected_label_mapping_id", "selected_label_mapping_hash"},
    )
    mapping_mutations = (
        ({**mapping_base, "terminal_label_content_hash": None}, "nullable together"),
        ({**mapping_base, "terminal_label_version_id": None, "terminal_label_content_hash": None, "terminal_label_revision_no": None, "terminal_maturity_status": None, "terminal_outcome_event_status": None, "terminal_reason_codes": ("terminal",), "selection_status": LabelSelectionStatus.UNAVAILABLE, "reason_codes": ("missing",)}, "cannot carry terminal reason"),
        ({**mapping_base, "reason_codes": ("unexpected",)}, "requires one terminal and no reason"),
        ({**mapping.model_dump(mode="python"), "selected_label_mapping_id": "slm-wrong"}, "mapping id"),
    )
    for payload, message in mapping_mutations:
        with pytest.raises(ValueError, match=message):
            SelectedLabelMapping.model_validate(payload)


def test_low_level_identity_helpers_reject_noncanonical_values() -> None:
    with pytest.raises(ValueError, match="lowercase sha256"):
        label_key_hash(
            canonical_signal_id="signal",
            symbol="000001.SZ",
            label_policy_hash="G" * 64,
            horizon_trading_days=1,
            projection=Projection.RETURN_GROSS,
        )
    with pytest.raises(ValueError, match="cannot be negative"):
        label_key_hash(
            canonical_signal_id="signal",
            symbol="000001.SZ",
            label_policy_hash=HASH_A,
            horizon_trading_days=-1,
            projection=Projection.RETURN_GROSS,
        )
    with pytest.raises(ValueError, match="explicit timezone"):
        SelectedLabelMapping(
            selector_request_hash=HASH_A,
            selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            selection_policy_hash=HASH_B,
            label_key_hash=HASH_C,
            requested_label_as_of_ts=datetime(2026, 7, 13, 9, 0),
            selection_status=LabelSelectionStatus.UNAVAILABLE,
            reason_codes=("fixture",),
        )
    mapping = SelectedLabelMapping(
        selector_request_hash=HASH_A,
        selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
        selection_policy_hash=HASH_B,
        label_key_hash=HASH_C,
        requested_label_as_of_ts=AS_OF,
        selection_status=LabelSelectionStatus.UNAVAILABLE,
        reason_codes=("a", "z"),
    )
    unsafe = mapping.model_copy(update={"reason_codes": ("z", "a")})
    with pytest.raises(LabelBuilderError, match="differs from canonical content"):
        _canonical_revalidate(
            unsafe,
            reason_code="FIXTURE_NONCANONICAL",
            label="mapping",
        )


def test_version_selection_mapping_and_stage_contracts_reject_invalid_identity() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    version = repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=result,
            uri="file:///version",
        ),
        created_by_capture_batch_id="label-capture",
    )
    invalid_version = version.model_dump(mode="python")
    invalid_version["supersedes_label_version_id"] = "advlabel-illegal"
    invalid_version["supersedes_label_version_hash"] = HASH_A
    with pytest.raises(ValueError, match="first label revision"):
        OutcomeLabelVersion.model_validate(invalid_version)
    with pytest.raises(ValueError, match="exact label selection"):
        LabelSelectionRequest(
            selection_policy=LabelSelectionPolicy.EXACT_REVISION_V1,
            label_key_hash=descriptor.label_key_hash,
            requested_label_as_of_ts=AS_OF,
            required_maturity_statuses=("MATURED",),
            required_outcome_event_statuses=(OutcomeEventStatus.NONE,),
            expected_observation_version_id=descriptor.observation_version_id,
            expected_candidate_stage_evidence_id=descriptor.candidate_stage_evidence_id,
            expected_label_source_revision_set_hash=source_set.source_revision_set_hash,
        )
    with pytest.raises(ValueError, match="stage evidence id"):
        StageEvidenceReference(
            observation_version_id=descriptor.observation_version_id,
            stage="alpha_raw",
            stage_content_hash=HASH_B,
            stage_evidence_id="wrong",
        )
    with pytest.raises(ValueError, match="stage evidence key hash"):
        StageEvidenceReference(
            observation_version_id=descriptor.observation_version_id,
            stage="alpha_raw",
            stage_content_hash=HASH_B,
            stage_evidence_key_hash=HASH_A,
        )
    selected = TerminalFirstLabelSelector().select(
        request=_selection_request(descriptor=descriptor, source_set=source_set),
        label_versions=(version,),
    )
    invalid_mapping = selected.model_copy(update={"selected_label_mapping_hash": HASH_A})
    with pytest.raises(ValueError, match="mapping hash"):
        SelectedLabelMapping.model_validate(invalid_mapping.model_dump(mode="python"))

    with pytest.raises(ValueError, match="unique"):
        LabelSelectionRequest(
            selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            label_key_hash=descriptor.label_key_hash,
            requested_label_as_of_ts=AS_OF,
            required_maturity_statuses=("MATURED", "MATURED"),
            required_outcome_event_statuses=(OutcomeEventStatus.NONE,),
            expected_observation_version_id=descriptor.observation_version_id,
            expected_candidate_stage_evidence_id=descriptor.candidate_stage_evidence_id,
            expected_label_source_revision_set_hash=source_set.source_revision_set_hash,
        )
    payload = OutcomeLabelPayload.from_version(version).model_copy(update={"calculation_evidence_uri": "file:///other"})
    with pytest.raises(LabelBuilderError, match="header and payload"):
        _validate_header_payload(
            version=version,
            header=repository.header_for(str(version.label_version_id)),
            payload=payload,
        )

    observation = context.selected_observations[0]
    with pytest.raises(LabelBuilderError, match="no stage list"):
        _alpha_raw_stage(observation.model_copy(update={"observation_payload": {}}))
    with pytest.raises(LabelBuilderError, match="exactly one alpha_raw"):
        _alpha_raw_stage(
            observation.model_copy(update={"observation_payload": {"stages": []}})
        )
    original_stage = observation.observation_payload["stages"][0]
    with pytest.raises(LabelBuilderError, match="absent from observation stage bundle"):
        _alpha_raw_stage(
            observation.model_copy(
                update={
                    "observation_payload": {
                        "stages": [{**original_stage, "content_hash": HASH_F}]
                    }
                }
            )
        )


def test_mapping_repository_rejects_same_id_with_different_hash() -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(_candidate_request(descriptor=descriptor, policies=policies, source_set=source_set))
    labels = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    version = labels.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=result,
            uri="file:///mapping",
        ),
        created_by_capture_batch_id="label-capture",
    )
    mapping = TerminalFirstLabelSelector().select(
        request=_selection_request(descriptor=descriptor, source_set=source_set),
        label_versions=(version,),
    )
    repository = InMemorySelectedLabelMappingRepository()
    assert repository.save(mapping) == mapping
    assert repository.save(mapping) == mapping
    conflicting = mapping.model_copy(update={"selected_label_mapping_hash": HASH_A})
    with pytest.raises(LabelBuilderError, match="mapping id"):
        repository.save(conflicting)
    conflicting_id = mapping.model_copy(update={"selected_label_mapping_id": "slm-other"})
    with pytest.raises(LabelBuilderError, match="mapping hash"):
        repository.save(conflicting_id)


def test_selected_mapping_normalizes_reasons_before_hashing() -> None:
    mapping = SelectedLabelMapping(
        selector_request_hash=HASH_A,
        selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
        selection_policy_hash=HASH_B,
        label_key_hash=HASH_C,
        requested_label_as_of_ts=AS_OF,
        selection_status=LabelSelectionStatus.UNAVAILABLE,
        reason_codes=("reason-z", "reason-a", "reason-z"),
    )

    assert mapping.reason_codes == ("reason-a", "reason-z")
    assert SelectedLabelMapping.model_validate(mapping.model_dump(mode="python")) == mapping
    with pytest.raises(ValueError, match="requires a stable reason"):
        SelectedLabelMapping(
            selector_request_hash=HASH_A,
            selection_policy=LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            selection_policy_hash=HASH_B,
            label_key_hash=HASH_C,
            requested_label_as_of_ts=AS_OF,
            selection_status=LabelSelectionStatus.UNAVAILABLE,
            reason_codes=("",),
        )


def test_candidate_enumerator_handles_native_multi_alpha_and_valid_empty_candidate() -> None:
    context, policies, _ = _context(alpha_mode="multi_alpha")
    result = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle)
    assert len(result.descriptors) == 1
    assert result.descriptors[0].candidate_stage_evidence_id.startswith("advstage_")
    assert len(result.component_evidence_hashes) == 1

    empty_context, empty_policies, _ = _context(candidates=(), valid_no_candidate=True)
    empty = enumerate_candidate_labels(context=empty_context, label_policy_bundle=empty_policies.bundle)
    assert empty.descriptors == ()
    assert empty.coverage.empty_observation_count == 1

    invalid_empty_context, invalid_empty_policies, _ = _context(candidates=(), valid_no_candidate=False)
    with pytest.raises(LabelBuilderError, match="valid_no_candidate"):
        enumerate_candidate_labels(context=invalid_empty_context, label_policy_bundle=invalid_empty_policies.bundle)

    with pytest.raises(LabelCaptureContractError, match="source memberships"):
        replace(empty_context, source_memberships=()).validate()

    unsafe_observation = context.selected_observations[0].model_copy(
        update={"observation_payload": {"tampered": True}}
    )
    with pytest.raises(LabelCaptureContractError, match="selected observation"):
        replace(context, selected_observations=(unsafe_observation,)).validate()

    partial_context, partial_policies, _ = _context(stage_capability="PARTIAL")
    with pytest.raises(LabelBuilderError, match="not complete"):
        enumerate_candidate_labels(context=partial_context, label_policy_bundle=partial_policies.bundle)

    bad_rank_context, bad_rank_policies, _ = _context(
        candidates=(
            {
                "symbol": "000001.SZ",
                "membership_status": "INCLUDED",
                "rank": 2,
                "score_decimal": "1.0",
            },
        )
    )
    with pytest.raises(LabelBuilderError, match="not continuous"):
        enumerate_candidate_labels(context=bad_rank_context, label_policy_bundle=bad_rank_policies.bundle)


def test_admission_context_rejects_source_plan_substitution() -> None:
    context, _, _ = _context()
    plan_payload = context.source_plans[0].model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["selection_run_id"] = "substituted-selection-run"
    substituted = CapturePlan.model_validate(plan_payload)

    with pytest.raises(LabelCaptureContractError, match="sealed source capture request"):
        replace(context, source_plans=(substituted,)).validate()


def test_admission_context_rejects_incomplete_or_ambiguous_frozen_inputs() -> None:
    context, _, _ = _context()
    incomplete_source = context.source_batch.model_copy(update={"status": CaptureBatchStatus.PLANNED})
    with pytest.raises(LabelCaptureContractError, match="must be complete"):
        replace(context, source_batch=incomplete_source).validate()
    with pytest.raises(LabelCaptureContractError, match="immutable Pydantic contract"):
        replace(context, source_memberships=(object(),)).validate()
    with pytest.raises(LabelCaptureContractError, match="requires source capture plans"):
        replace(context, source_plans=()).validate()
    with pytest.raises(LabelCaptureContractError, match="requires selected observations"):
        replace(context, selected_observation_mappings=()).validate()
    mapping = context.selected_observation_mappings[0]
    with pytest.raises(LabelCaptureContractError, match="requires a selected observation mapping"):
        SelectedObservationMappingReference.from_mapping(
            mapping.model_copy(update={"selection_status": "UNAVAILABLE"})
        )
    with pytest.raises(LabelCaptureContractError, match="mappings are duplicated"):
        replace(context, selected_observation_mappings=(mapping, mapping)).validate()
    observation = context.selected_observations[0]
    with pytest.raises(LabelCaptureContractError, match="observations are duplicated"):
        replace(context, selected_observations=(observation, observation)).validate()
    with pytest.raises(LabelCaptureContractError, match="terminal does not match"):
        replace(context, selected_observations=()).validate()


def _local_store(tmp_path: Path) -> LocalCalculationEvidenceStore:
    return LocalCalculationEvidenceStore(
        root=tmp_path / "label-cas",
        repository_root=Path.cwd(),
        store_identity={
            "durability_mode": "WINDOWS_FILE_AND_DIRECTORY_FLUSH_V1"
            if os.name == "nt"
            else "POSIX_FILE_AND_DIRECTORY_FSYNC_V1",
            "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
            "fixture": "label-builder",
        },
    )


def _builder(tmp_path: Path):
    capture_repository = InMemoryCaptureBatchRepository(now_provider=lambda: AS_OF)
    return (
        LabelBuilder(
            outcome_engine=OutcomeEngine(),
            capture_repository=capture_repository,
            label_repository=InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF),
            mapping_repository=InMemorySelectedLabelMappingRepository(),
            evidence_store=_local_store(tmp_path),
            now_provider=lambda: AS_OF,
        ),
        capture_repository,
    )


def test_label_builder_appends_from_terminal_predecessor(tmp_path: Path) -> None:
    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(
        context=context,
        label_policy_bundle=policies.bundle,
    ).descriptors[0]
    label_repository = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF)
    matured_result = OutcomeEngine().calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set)
    )
    first = label_repository.append(
        request=_append_request(
            descriptor=descriptor,
            policies=policies,
            source_set=source_set,
            result=matured_result,
            uri="file:///first-revision",
        ),
        created_by_capture_batch_id="label-capture-first-revision",
    )
    capture_repository = InMemoryCaptureBatchRepository(now_provider=lambda: AS_OF)
    builder = LabelBuilder(
        outcome_engine=OutcomeEngine(),
        capture_repository=capture_repository,
        label_repository=label_repository,
        mapping_repository=InMemorySelectedLabelMappingRepository(),
        evidence_store=_local_store(tmp_path),
        now_provider=lambda: AS_OF,
    )
    capture_request = build_label_capture_request(
        context,
        capture_batch_id="label-capture-second-revision",
        planned_labels=(descriptor,),
    )
    unavailable_request = _candidate_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        entry_executable=False,
    )

    run = builder.run(
        context=context,
        capture_request=capture_request,
        candidate_outcome_requests={descriptor.label_key_hash: unavailable_request},
        label_selection_requests={
            descriptor.label_key_hash: _selection_request(descriptor=descriptor, source_set=source_set)
        },
        universe_constituents=(),
        universe_outcome_requests={},
    )

    second = run.label_versions[0]
    assert second.label_revision_no == 2
    assert second.supersedes_label_version_id == first.label_version_id
    assert second.supersedes_label_version_hash == first.label_content_hash
    assert run.selected_label_mappings[0].terminal_label_version_id == second.label_version_id
    assert run.selected_label_mappings[0].selection_status is LabelSelectionStatus.UNAVAILABLE
    retry_result = OutcomeEngine().calculate(unavailable_request)
    retry_stored = builder._store_result(retry_result)
    retried = builder._append_or_reuse_candidate(
        descriptor=descriptor,
        context=context,
        result=retry_result,
        stored=retry_stored,
        created_by_capture_batch_id="label-capture-recovery",
    )
    assert retried == second
    assert len(label_repository.chain_for(descriptor.label_key_hash)) == 2


def test_label_builder_records_explicit_gap_and_completes_when_candidate_input_is_missing(tmp_path: Path) -> None:
    context, policies, _ = _context()
    descriptors = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors
    capture_request = build_label_capture_request(
        context,
        capture_batch_id="label-capture-gap",
        planned_labels=descriptors,
    )
    builder, _ = _builder(tmp_path)
    run = builder.run(
        context=context,
        capture_request=capture_request,
        candidate_outcome_requests={},
        label_selection_requests={},
        universe_constituents=(),
        universe_outcome_requests={},
    )
    assert run.capture_batch.status.value == "COMPLETE"
    assert run.label_versions == ()
    assert len(run.gaps) == 1
    assert run.gaps[0].reason_code == "ADVISORY_PHASE1C3_LABEL_CANDIDATE_REQUEST_MISSING"


def test_label_builder_fails_the_capture_when_a_materialized_label_has_no_selector(tmp_path: Path) -> None:
    context, policies, source_set = _context()
    descriptors = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors
    descriptor = descriptors[0]
    capture_request = build_label_capture_request(
        context,
        capture_batch_id="label-capture-missing-selector",
        planned_labels=descriptors,
    )
    builder, capture_repository = _builder(tmp_path)
    with pytest.raises(LabelBuilderError, match="selection request"):
        builder.run(
            context=context,
            capture_request=capture_request,
            candidate_outcome_requests={
                descriptor.label_key_hash: _candidate_request(
                    descriptor=descriptor,
                    policies=policies,
                    source_set=source_set,
                )
            },
            label_selection_requests={},
            universe_constituents=(),
            universe_outcome_requests={},
        )
    assert capture_repository.get("label-capture-missing-selector").status.value == "FAILED"


def test_label_builder_rejects_noncanonical_selector_and_logs_failure(tmp_path: Path, caplog) -> None:
    context, policies, source_set = _context()
    descriptors = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors
    descriptor = descriptors[0]
    capture_request = build_label_capture_request(
        context,
        capture_batch_id="label-capture-noncanonical-selector",
        planned_labels=descriptors,
    )
    selection_request = _selection_request(descriptor=descriptor, source_set=source_set)
    unsafe_selection_request = selection_request.model_copy(
        update={"expected_candidate_stage_evidence_id": "stale-stage-evidence"}
    )
    builder, capture_repository = _builder(tmp_path)

    with caplog.at_level("ERROR", logger="backend.services.advisory_phase1.label_builder"):
        with pytest.raises(LabelBuilderError, match="canonical revalidation"):
            builder.run(
                context=context,
                capture_request=capture_request,
                candidate_outcome_requests={
                    descriptor.label_key_hash: _candidate_request(
                        descriptor=descriptor,
                        policies=policies,
                        source_set=source_set,
                    )
                },
                label_selection_requests={descriptor.label_key_hash: unsafe_selection_request},
                universe_constituents=(),
                universe_outcome_requests={},
            )

    assert capture_repository.get(capture_request.capture_batch_id).status.value == "FAILED"
    diagnostic_records = [
        record for record in caplog.records if record.getMessage().startswith("advisory_label_builder_failed")
    ]
    assert len(diagnostic_records) == 1
    assert "ADVISORY_PHASE1C3_LABEL_SELECTOR_TERMINAL_CONFLICT" in diagnostic_records[0].getMessage()


def test_universe_enumerator_rejects_duplicate_symbols() -> None:
    context, policies, _ = _context()
    constituent = UniverseConstituent(
        symbol="000001.SZ",
        universe_layer="fixture-universe",
        universe_policy_hash=context.source_plans[0].universe_policy_hash,
        source_member_bindings=(_source_binding(),),
    )
    with pytest.raises(LabelBuilderError, match="duplicate symbols"):
        enumerate_universe_outcome_plans(
            context=context,
            constituents=(constituent, constituent),
            label_policy_bundle=policies.bundle,
        )
    wrong_policy_payload = constituent.model_dump(mode="python", exclude={"constituent_content_hash"})
    wrong_policy_payload["universe_policy_hash"] = HASH_F
    wrong_policy = UniverseConstituent.model_validate(wrong_policy_payload)
    with pytest.raises(LabelBuilderError, match="policy differs"):
        enumerate_universe_outcome_plans(
            context=context,
            constituents=(wrong_policy,),
            label_policy_bundle=policies.bundle,
        )
    wrong_source_binding = SourceMemberBinding(
        source_role=_source_binding().source_role,
        source_member_key=HASH_F,
        partition_content_hash=_source_binding().partition_content_hash,
    )
    wrong_source_payload = constituent.model_dump(mode="python", exclude={"constituent_content_hash"})
    wrong_source_payload["source_member_bindings"] = (wrong_source_binding,)
    wrong_source = UniverseConstituent.model_validate(wrong_source_payload)
    with pytest.raises(LabelBuilderError, match="source binding"):
        enumerate_universe_outcome_plans(
            context=context,
            constituents=(wrong_source,),
            label_policy_bundle=policies.bundle,
        )
    unsafe_copy = constituent.model_copy(update={"constituent_content_hash": HASH_F})
    with pytest.raises(LabelBuilderError, match="canonical revalidation"):
        enumerate_universe_outcome_plans(
            context=context,
            constituents=(unsafe_copy,),
            label_policy_bundle=policies.bundle,
        )


def test_universe_contracts_reject_identity_and_payload_drift() -> None:
    context, policies, source_set = _context()
    source_binding = _source_binding()
    with pytest.raises(ValueError, match="source members are duplicated"):
        UniverseConstituent(
            symbol="000001.SZ",
            universe_layer="fixture-universe",
            universe_policy_hash=context.source_plans[0].universe_policy_hash,
            source_member_bindings=(source_binding, source_binding),
        )
    constituent = UniverseConstituent(
        symbol="000001.SZ",
        universe_layer="fixture-universe",
        universe_policy_hash=context.source_plans[0].universe_policy_hash,
        source_member_bindings=(source_binding,),
    )
    with pytest.raises(ValueError, match="constituent content hash"):
        UniverseConstituent.model_validate(
            {**constituent.model_dump(mode="python"), "constituent_content_hash": HASH_F}
        )
    plan = enumerate_universe_outcome_plans(
        context=context,
        constituents=(constituent,),
        label_policy_bundle=policies.bundle,
    )[0]
    plan_base = plan.model_dump(mode="python", exclude={"plan_hash"})
    with pytest.raises(ValueError, match="unsupported universe outcome plan schema"):
        UniverseOutcomePlan.model_validate({**plan_base, "schema_version": "wrong"})
    with pytest.raises(ValueError, match="owner identity"):
        UniverseOutcomePlan.model_validate(
            {**plan_base, "canonical_signal_id": "different-signal"}
        )
    with pytest.raises(ValueError, match="plan hash"):
        UniverseOutcomePlan.model_validate({**plan.model_dump(mode="python"), "plan_hash": HASH_F})

    descriptor = enumerate_candidate_labels(
        context=context,
        label_policy_bundle=policies.bundle,
    ).descriptors[0]
    outcome_payload = _candidate_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
    ).model_dump(mode="python")
    outcome_payload["owner"] = plan.owner.model_dump(mode="python")
    result = OutcomeEngine().calculate(OutcomeCalculationRequest.model_validate(outcome_payload))
    row = UniverseRawOutcomeRow(
        plan=plan,
        outcome_result=result,
        calculation_evidence_sha256=str(result.calculation_evidence.evidence_hash),
        calculation_evidence_size_bytes=len(result.calculation_evidence.canonical_bytes()),
        calculation_evidence_store_backend_hash=HASH_F,
        calculation_evidence_uri="file:///universe-row",
    )
    row_base = row.model_dump(mode="python", exclude={"raw_outcome_id", "raw_outcome_hash"})
    with pytest.raises(ValueError, match="unsupported universe raw outcome schema"):
        UniverseRawOutcomeRow.model_validate({**row_base, "schema_version": "wrong"})
    alternate_constituent = UniverseConstituent(
        symbol="000002.SZ",
        universe_layer="fixture-universe",
        universe_policy_hash=context.source_plans[0].universe_policy_hash,
        source_member_bindings=(source_binding,),
    )
    alternate_plan = enumerate_universe_outcome_plans(
        context=context,
        constituents=(alternate_constituent,),
        label_policy_bundle=policies.bundle,
    )[0]
    with pytest.raises(ValueError, match="owner differs"):
        UniverseRawOutcomeRow.model_validate({**row_base, "plan": alternate_plan})
    with pytest.raises(ValueError, match="does not match plan"):
        UniverseRawOutcomeRow.model_validate(
            {**row_base, "calculation_evidence_sha256": HASH_A}
        )
    with pytest.raises(ValueError, match="raw outcome hash"):
        UniverseRawOutcomeRow.model_validate({**row.model_dump(mode="python"), "raw_outcome_hash": HASH_F})
    with pytest.raises(ValueError, match="raw outcome id"):
        UniverseRawOutcomeRow.model_validate({**row.model_dump(mode="python"), "raw_outcome_id": "uor-wrong"})


def test_label_builder_closes_candidate_and_universe_evidence_with_real_local_cas(tmp_path: Path) -> None:
    context, policies, source_set = _context(alpha_mode="multi_alpha")
    enumeration = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle)
    capture_request = build_label_capture_request(
        context,
        capture_batch_id="label-capture-run",
        planned_labels=enumeration.descriptors,
    )
    descriptor = enumeration.descriptors[0]
    candidate_request = _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set)
    constituent = UniverseConstituent(
        symbol="000001.SZ",
        universe_layer="fixture-universe",
        universe_policy_hash=context.source_plans[0].universe_policy_hash,
        source_member_bindings=(_source_binding(),),
    )
    universe_plans = enumerate_universe_outcome_plans(
        context=context,
        constituents=(constituent,),
        label_policy_bundle=policies.bundle,
    )
    universe_request_payload = candidate_request.model_dump(mode="python")
    universe_request_payload["owner"] = universe_plans[0].owner.model_dump(mode="python")
    universe_request = OutcomeCalculationRequest.model_validate(universe_request_payload)
    builder, capture_repository = _builder(tmp_path)
    run = builder.run(
        context=context,
        capture_request=capture_request,
        candidate_outcome_requests={descriptor.label_key_hash: candidate_request},
        label_selection_requests={descriptor.label_key_hash: _selection_request(descriptor=descriptor, source_set=source_set)},
        universe_constituents=(constituent,),
        universe_outcome_requests={str(universe_plans[0].plan_hash): universe_request},
    )
    assert run.capture_batch.status.value == "COMPLETE"
    assert len(run.label_versions) == 1
    assert len(run.selected_label_mappings) == 1
    assert len(run.universe_raw_rows) == 1
    assert run.selected_label_mappings[0].selection_status is LabelSelectionStatus.SELECTED
    assert run.gaps == ()
    membership_roles = {
        item.evidence_role
        for item in capture_repository.memberships_for(capture_request.capture_batch_id)
    }
    assert {
        "selected_observation",
        "candidate_stage_evidence",
        "multi_alpha_component_evidence",
    } <= membership_roles
    assert (
        builder.run(
            context=context,
            capture_request=capture_request,
            candidate_outcome_requests={descriptor.label_key_hash: candidate_request},
            label_selection_requests={descriptor.label_key_hash: _selection_request(descriptor=descriptor, source_set=source_set)},
            universe_constituents=(constituent,),
            universe_outcome_requests={str(universe_plans[0].plan_hash): universe_request},
        )
        is run
    )
