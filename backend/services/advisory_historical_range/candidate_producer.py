"""Produce one Phase 1R historical candidate artifact without operational writes."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any, Protocol

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.candidate_projector import HistoricalRangeCandidateProjector
from backend.services.advisory_historical_range.models import (
    CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION,
    REASON_ARTIFACT_NOT_FOUND,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeCandidateProductionResultV1,
    HistoricalRangeContractError,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeHMMBindingSetV1,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionRefV1,
    build_candidate_artifact_payload,
    build_candidate_input_hash,
    derive_day_run_id,
    derive_prefixed_id,
)
from backend.services.strategy_package.selection_computation import (
    SelectionMode,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionComputationRequestV1,
    StrategyPackageSelectionReadOnlyProvidersV1,
    parse_selection_runtime_profile_for_computation,
    selection_runtime_profile_sha256,
)
from backend.services.strategy_package.selection_signal_preparation import (
    StrategyPackageSelectionSignalPreparation,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    HMMRuntimeUnavailableError,
    RuntimeConfigInvalidError,
)


CANDIDATE_PRODUCER_CONTRACT_VERSION = "advisory_historical_range_candidate_producer_v2"


class HistoricalRangeSourceRevisionVerifier(Protocol):
    def verify_program_day(
        self,
        *,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        research_program_id: str,
        package_id: str,
        component_ids: set[str],
        decision_trade_date: date,
    ) -> tuple[HistoricalRangeSourceRevisionRefV1, ...]: ...


class HistoricalRangeCandidateProducer:
    def __init__(
        self,
        *,
        signal_preparation: StrategyPackageSelectionSignalPreparation,
        computation: StrategyPackageSelectionComputation,
        providers: StrategyPackageSelectionReadOnlyProvidersV1,
        source_verifier: HistoricalRangeSourceRevisionVerifier,
        artifact_store: HistoricalRangeArtifactStore,
        projector: HistoricalRangeCandidateProjector | None = None,
    ) -> None:
        dependencies = (signal_preparation, computation, providers, source_verifier, artifact_store)
        if any(item is None for item in dependencies):
            raise ValueError("historical candidate producer requires explicit read-only dependencies")
        self._signal_preparation = signal_preparation
        self._computation = computation
        self._providers = providers
        self._source_verifier = source_verifier
        self._artifact_store = artifact_store
        self._projector = projector or HistoricalRangeCandidateProjector()

    def produce(
        self,
        *,
        request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
        research_program_id: str,
        decision_trade_date: date,
        request_artifact_ref: HistoricalRangeArtifactRefV1 | None = None,
    ) -> HistoricalRangeCandidateProductionResultV1:
        resolved = request_payload.resolved_request
        program = next(
            (item for item in resolved.frozen_programs if item.research_program_id == research_program_id),
            None,
        )
        if program is None:
            raise RuntimeConfigInvalidError(
                "historical candidate Program is absent from the sealed request",
                context={"research_program_id": research_program_id},
            )
        try:
            ordinal = resolved.date_plan.ordered_trade_dates.index(decision_trade_date) + 1
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "historical candidate day is absent from the sealed date plan",
                context={"research_program_id": research_program_id, "decision_trade_date": decision_trade_date.isoformat()},
            ) from exc
        range_run_id = derive_prefixed_id(
            "ahrr",
            {
                "resolved_request_hash": resolved.request_payload_sha256,
                "research_program_id": research_program_id,
            },
        )
        day_run_id = derive_day_run_id(range_run_id, decision_trade_date, ordinal)
        component_ids = {item.component_id for item in program.admitted_package_projection.components}
        source_refs = self._source_verifier.verify_program_day(
            catalog=request_payload.source_revision_catalog,
            research_program_id=program.research_program_id,
            package_id=program.package_id,
            component_ids=component_ids,
            decision_trade_date=decision_trade_date,
        )
        runtime_config = _resolved_runtime_config_for_day(
            program=program,
            decision_trade_date=decision_trade_date,
            catalog=request_payload.source_revision_catalog,
            source_refs=source_refs,
            artifact_store=self._artifact_store,
        )
        prepared = self._signal_preparation.prepare_historical(
            package_id=program.package_id,
            trade_date=decision_trade_date,
            runtime_config=runtime_config,
        )
        profile = parse_selection_runtime_profile_for_computation(runtime_config)
        runtime_profile_hash = selection_runtime_profile_sha256(profile)
        top_k = _resolved_top_k(profile=profile, raw_metadata=prepared.raw.artifact.metadata)
        computation = self._computation.compute(
            request=StrategyPackageSelectionComputationRequestV1(
                trade_date=decision_trade_date,
                data_source="DB_HISTORICAL",
                selection_mode=SelectionMode.SINGLE_PACKAGE,
                ordered_package_ids=(program.package_id,),
                package_runtime_profiles={program.package_id: profile},
                package_runtime_profile_hashes={program.package_id: runtime_profile_hash},
                package_top_k={program.package_id: top_k},
                exhaustive_selection_evidence=True,
            ),
            prepared_signals={program.package_id: prepared.prepared_signal},
            providers=self._providers,
        )
        facts, stage_trace = self._projector.project(
            frozen_program=program,
            day_run_id=day_run_id,
            prepared_signal=prepared.prepared_signal,
            raw_artifact=prepared.raw,
            computation=computation,
            runtime_profile_hash=runtime_profile_hash,
        )
        final_rows = computation.package_results[program.package_id]
        candidate_outcome = "CANDIDATES_AVAILABLE" if final_rows else "VALID_NO_CANDIDATE"
        no_candidate_reasons = _no_candidate_reasons(
            outcome=candidate_outcome,
            raw_score_count=prepared.raw.artifact.score_count,
            stage_trace=stage_trace,
        )
        catalog = request_payload.source_revision_catalog
        input_context = prepared.raw.artifact.metadata.get("artifact_input_context")
        if not isinstance(input_context, dict):
            raise ArtifactGenerationFailedError("historical raw artifact has no input context")
        _validate_raw_input_catalog_alignment(
            program=program,
            catalog=catalog,
            input_context=input_context,
            decision_trade_date=decision_trade_date,
        )
        _validate_source_read_receipts_against_catalog(
            program=program,
            catalog=catalog,
            receipts=prepared.raw.source_read_receipts,
            input_context=input_context,
            decision_trade_date=decision_trade_date,
        )
        universe_identity_hash = str(input_context.get("universe_input_hash") or "")
        raw_header = {
            **dict(prepared.raw.semantic_header),
            "runtime_profile_hash": runtime_profile_hash,
            "selection_semantics_hash": program.selection_semantics_hash,
            "code_release_hash": program.code_release_hash,
            "calendar_identity_hash": catalog.calendar_identity_hash,
            "universe_identity_hash": universe_identity_hash,
        }
        raw_signal_identity_hash = canonical_json_sha256(raw_header)
        candidate_input_hash = build_candidate_input_hash(
            range_run_id=range_run_id,
            research_program_id=research_program_id,
            decision_trade_date=decision_trade_date,
            frozen_program_hash=str(program.frozen_program_hash),
            runtime_profile_hash=runtime_profile_hash,
            code_release_hash=program.code_release_hash,
            selection_semantics_hash=program.selection_semantics_hash,
            calendar_identity_hash=catalog.calendar_identity_hash,
            universe_identity_hash=universe_identity_hash,
            source_revision_catalog_hash=str(catalog.catalog_hash),
            query_contract_hash=catalog.query_contract_hash,
        )
        receipt_hashes = _source_receipt_hashes(prepared.raw.source_read_receipts, source_refs)
        payload_document = build_candidate_artifact_payload(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            research_program_id=research_program_id,
            decision_trade_date=decision_trade_date,
            candidate_input_hash=candidate_input_hash,
            package_id=program.package_id,
            package_version=program.package_version,
            manifest_sha256=program.manifest_sha256,
            alpha_mode=program.alpha_mode,
            runtime_profile_hash=runtime_profile_hash,
            selection_semantics_hash=program.selection_semantics_hash,
            code_release_hash=program.code_release_hash,
            calendar_identity_hash=catalog.calendar_identity_hash,
            universe_identity_hash=universe_identity_hash,
            universe_count=prepared.raw.artifact.universe_count,
            raw_signal_identity_hash=raw_signal_identity_hash,
            raw_signal_semantic_header=raw_header,
            raw_inference_receipt=dict(prepared.raw.raw_inference_receipt),
            source_read_receipt_hashes=receipt_hashes,
            stage_trace=stage_trace,
            candidate_outcome=candidate_outcome,
            no_candidate_reason_codes=no_candidate_reasons,
            candidates=facts,
            source_revision_refs=source_refs,
        )
        # Close a second DB observation after all inference/provider reads. A drifted
        # partition cannot be published under the earlier sealed catalog identity.
        final_refs = self._source_verifier.verify_program_day(
            catalog=catalog,
            research_program_id=program.research_program_id,
            package_id=program.package_id,
            component_ids=component_ids,
            decision_trade_date=decision_trade_date,
        )
        if final_refs != source_refs:
            raise ArtifactGenerationFailedError("historical source refs changed during candidate computation")
        upstream_refs = (request_artifact_ref,) if request_artifact_ref is not None else ()
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            producer_contract_version=CANDIDATE_PRODUCER_CONTRACT_VERSION,
            payload_schema_version=CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION,
            resolved_request_hash=resolved.request_payload_sha256,
            payload=payload_document,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            source_revision_refs=source_refs,
            upstream_refs=upstream_refs,
        )
        readback = self._artifact_store.load(stored.ref)
        parsed_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(readback.payload)
        expected_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(payload_document)
        parsed_document = parsed_payload.model_dump(mode="json")
        expected_document = expected_payload.model_dump(mode="json")
        readback_payload_hash = canonical_json_sha256(parsed_document)
        expected_payload_hash = canonical_json_sha256(expected_document)
        payload_equal = readback_payload_hash == expected_payload_hash
        if (
            not payload_equal
            or readback.source_revision_refs != source_refs
            or readback.range_run_id != range_run_id
            or readback.day_run_id != day_run_id
            or readback.resolved_request_hash != resolved.request_payload_sha256
        ):
            raise ArtifactGenerationFailedError(
                "historical candidate artifact readback differs from the produced payload",
                context={
                    "day_run_id": day_run_id,
                    "relative_path": stored.ref.relative_path,
                    "payload_equal": payload_equal,
                    "payload_differing_fields": sorted(
                        key
                        for key in set(parsed_document) | set(expected_document)
                        if parsed_document.get(key) != expected_document.get(key)
                    ),
                    "readback_payload_hash": readback_payload_hash,
                    "expected_payload_hash": expected_payload_hash,
                    "source_refs_equal": readback.source_revision_refs == source_refs,
                    "range_run_id_equal": readback.range_run_id == range_run_id,
                    "day_run_id_equal": readback.day_run_id == day_run_id,
                    "resolved_request_hash_equal": readback.resolved_request_hash == resolved.request_payload_sha256,
                },
            )
        return HistoricalRangeCandidateProductionResultV1(
            research_program_id=research_program_id,
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            decision_trade_date=decision_trade_date,
            candidate_input_hash=candidate_input_hash,
            candidate_outcome=candidate_outcome,
            no_candidate_reason_codes=no_candidate_reasons,
            candidates=facts,
            candidate_artifact_ref=stored.ref,
            stage_trace=stage_trace,
            source_revision_refs=source_refs,
            raw_signal_identity_hash=raw_signal_identity_hash,
        )


def _resolved_top_k(*, profile: Any, raw_metadata: dict[str, Any]) -> int:
    value = profile.selection.top_k
    if value is None:
        value = raw_metadata.get("topk", raw_metadata.get("final_topk"))
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeConfigInvalidError(
            "historical candidate computation cannot resolve the frozen selection top_k",
            context={"top_k": value},
        )
    return value


def _validate_raw_input_catalog_alignment(
    *,
    program: Any,
    catalog: HistoricalRangeSourceRevisionCatalogV1,
    input_context: dict[str, Any],
    decision_trade_date: date,
) -> None:
    if str(input_context.get("effective_trade_date") or "")[:10] != decision_trade_date.isoformat():
        raise ArtifactGenerationFailedError(
            "historical inference effective date differs from the frozen decision day",
            context={"reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH"},
        )
    universe_members = [
        member
        for member in catalog.members
        if member.source_role == "pit_universe" and member.decision_trade_date == decision_trade_date
    ]
    actual_universe_hash = str(input_context.get("universe_input_hash") or "")
    if len(universe_members) != 1 or universe_members[0].content_hash != actual_universe_hash:
        raise ArtifactGenerationFailedError(
            "historical inference universe differs from the frozen source catalog",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "decision_trade_date": decision_trade_date.isoformat(),
                "actual_universe_hash": actual_universe_hash or None,
                "catalog_universe_hashes": [member.content_hash for member in universe_members],
            },
        )
    components = {
        item.component_id: item for item in program.admitted_package_projection.components
    }
    raw_per_leg = input_context.get("per_leg_window_lineage")
    if raw_per_leg is None and len(components) == 1:
        raw_per_leg = {
            next(iter(components)): {
                "window_start_date": input_context.get("window_start_date"),
                "required_window": input_context.get("required_window"),
                "window_resolution": input_context.get("window_resolution"),
            }
        }
    if not isinstance(raw_per_leg, dict) or set(raw_per_leg) != set(components):
        raise ArtifactGenerationFailedError(
            "historical inference window lineage does not cover every frozen Alpha component",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "expected_components": sorted(components),
                "actual_components": sorted(raw_per_leg) if isinstance(raw_per_leg, dict) else None,
            },
        )
    for component_id, component in components.items():
        lineage = raw_per_leg[component_id]
        market_members = [
            member
            for member in catalog.members
            if member.source_role == "market_history"
            and member.package_id == program.package_id
            and member.component_id == component_id
            and member.decision_trade_date == decision_trade_date
        ]
        parameters = market_members[0].bound_parameters if len(market_members) == 1 else None
        expected = {
            "window_start_date": parameters.get("start_date") if isinstance(parameters, dict) else None,
            "required_window": component.required_window,
            "window_resolution": component.window_resolution,
        }
        actual = {
            "window_start_date": lineage.get("window_start_date") if isinstance(lineage, dict) else None,
            "required_window": lineage.get("required_window") if isinstance(lineage, dict) else None,
            "window_resolution": lineage.get("window_resolution") if isinstance(lineage, dict) else None,
        }
        if len(market_members) != 1 or actual != expected:
            raise ArtifactGenerationFailedError(
                "historical inference window differs from the frozen source catalog",
                context={
                    "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                    "component_id": component_id,
                    "expected": expected,
                    "actual": actual,
                    "catalog_member_count": len(market_members),
                },
            )
        calendar_identity = {
            "dataset_id": input_context.get("calendar_source"),
            "effective_trade_date": input_context.get("effective_trade_date"),
            "calendar_version": input_context.get("calendar_version"),
            "calendar_source": input_context.get("calendar_source"),
        }
        expected_calendar_identity_hash = canonical_json_sha256(calendar_identity)
        expected_window_hash = canonical_json_sha256(
            {
                "calendar_identity_hash": expected_calendar_identity_hash,
                **expected,
            }
        )
        actual_window_hash = (
            lineage.get("window_lineage_hash") if isinstance(lineage, dict) else None
        ) or (input_context.get("calendar_hash") if len(components) == 1 else None)
        if (
            input_context.get("calendar_version") != "market.trading_calendar.v1"
            or input_context.get("calendar_source") != "market.trading_calendar"
            or input_context.get("calendar_identity_hash") != expected_calendar_identity_hash
            or actual_window_hash != expected_window_hash
        ):
            raise ArtifactGenerationFailedError(
                "historical inference calendar lineage differs from its canonical DB contract",
                context={
                    "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                    "component_id": component_id,
                    "expected_window_hash": expected_window_hash,
                    "actual_window_hash": actual_window_hash,
                },
            )


def _resolved_runtime_config_for_day(
    *,
    program: HistoricalRangeFrozenProgramV1,
    decision_trade_date: date,
    catalog: HistoricalRangeSourceRevisionCatalogV1,
    source_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...],
    artifact_store: HistoricalRangeArtifactStore,
) -> dict[str, Any]:
    binding_ref = program.resolved_hmm_binding_set_ref
    base_profile = parse_selection_runtime_profile_for_computation(program.runtime_config)
    if binding_ref is None:
        if base_profile.hmm.enabled:
            raise RuntimeConfigInvalidError(
                "historical HMM Program has no sealed binding set",
                context={
                    "reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                    "research_program_id": program.research_program_id,
                    "decision_trade_date": decision_trade_date.isoformat(),
                },
            )
        return deepcopy(program.runtime_config)
    try:
        envelope = artifact_store.load_planning(binding_ref)
    except HistoricalRangeContractError as exc:
        if exc.reason_code != REASON_ARTIFACT_NOT_FOUND:
            raise
        raise HMMRuntimeUnavailableError(
            "sealed historical HMM binding set is temporarily unavailable",
            context={
                "reason_code": "ADVISORY_HR_HMM_INPUT_UNAVAILABLE",
                "research_program_id": program.research_program_id,
                "decision_trade_date": decision_trade_date.isoformat(),
                "binding_set_ref": binding_ref.model_dump(mode="json"),
            },
        ) from exc
    binding_set = HistoricalRangeHMMBindingSetV1.model_validate(envelope.payload)
    if (
        envelope.artifact_kind is not HistoricalRangeArtifactKind.HMM_BINDING_SET
        or binding_set.binding_set_hash != program.resolved_hmm_binding_set_hash
        or binding_set.research_program_id != program.research_program_id
        or binding_set.package_id != program.package_id
        or binding_set.base_runtime_config_hash != program.runtime_config_hash
    ):
        raise RuntimeConfigInvalidError(
            "historical HMM binding set differs from the frozen Program",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "research_program_id": program.research_program_id,
            },
        )
    binding = binding_set.binding_for_day(decision_trade_date)
    matching_members = [
        member
        for member in catalog.members
        if member.revision_id == binding.source_revision_ref.revision_id
        and member.revision_hash == binding.source_revision_ref.revision_hash
        and member.source_role == "hmm_frozen_evidence"
        and member.package_id == program.package_id
        and member.decision_trade_date == decision_trade_date
    ]
    if (
        len(matching_members) != 1
        or matching_members[0].content_hash != canonical_json_sha256(binding.phase0a_hmm_metadata)
        or binding.source_revision_ref not in source_refs
    ):
        raise RuntimeConfigInvalidError(
            "historical HMM binding does not close the sealed source catalog",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "research_program_id": program.research_program_id,
                "decision_trade_date": decision_trade_date.isoformat(),
            },
        )
    runtime_config = deepcopy(program.runtime_config)
    day_key = decision_trade_date.isoformat()
    by_date = runtime_config.get("phase0a_hmm_metadata_by_date")
    by_date = deepcopy(by_date) if isinstance(by_date, dict) else {}
    existing = by_date.get(day_key)
    if isinstance(existing, dict) and canonical_json_sha256(existing) != canonical_json_sha256(
        binding.phase0a_hmm_metadata
    ):
        raise RuntimeConfigInvalidError(
            "sealed HMM binding conflicts with base Program evidence",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "research_program_id": program.research_program_id,
                "decision_trade_date": day_key,
            },
        )
    by_date[day_key] = deepcopy(binding.phase0a_hmm_metadata)
    runtime_config["phase0a_hmm_metadata_by_date"] = by_date
    snapshot_id = binding.phase0a_hmm_metadata["model_snapshot_id"]
    runtime_profile = deepcopy(runtime_config.get("runtime_profile"))
    runtime_profile = runtime_profile if isinstance(runtime_profile, dict) else {}
    hmm_profile = deepcopy(runtime_profile.get("hmm"))
    hmm_profile = hmm_profile if isinstance(hmm_profile, dict) else {}
    hmm_profile["model_snapshot_id"] = snapshot_id
    runtime_profile["hmm"] = hmm_profile
    runtime_config["runtime_profile"] = runtime_profile
    hmm_config = deepcopy(runtime_config.get("hmm"))
    hmm_config = hmm_config if isinstance(hmm_config, dict) else {}
    hmm_config["model_snapshot_id"] = snapshot_id
    runtime_config["hmm"] = hmm_config
    runtime_config["hmm_model_snapshot_id"] = snapshot_id
    return runtime_config


def _validate_source_read_receipts_against_catalog(
    *,
    program: Any,
    catalog: HistoricalRangeSourceRevisionCatalogV1,
    receipts: tuple[Any, ...],
    input_context: dict[str, Any],
    decision_trade_date: date,
) -> None:
    normalized = [dict(receipt) for receipt in receipts]
    if not normalized or any(
        receipt.get("admissibility") != "RETROSPECTIVE_DB_CONTENT_HASH" for receipt in normalized
    ):
        raise ArtifactGenerationFailedError(
            "historical inference source receipts are not retrospective DB evidence",
            context={"reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH"},
        )
    universe_members = [
        member
        for member in catalog.members
        if member.source_role == "pit_universe" and member.decision_trade_date == decision_trade_date
    ]
    universe_receipts = [receipt for receipt in normalized if receipt.get("source_role") == "pit_universe"]
    if (
        len(universe_members) != 1
        or not universe_receipts
        or any(
            receipt.get("dataset_id") != "market.stock_universe_pit"
            or receipt.get("content_hash") != universe_members[0].content_hash
            or receipt.get("row_count") != universe_members[0].row_count
            for receipt in universe_receipts
        )
    ):
        raise ArtifactGenerationFailedError(
            "historical PIT universe receipts differ from the frozen catalog",
            context={"reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH"},
        )
    components = {
        item.component_id: item for item in program.admitted_package_projection.components
    }
    single_component_id = next(iter(components)) if len(components) == 1 else None
    for component_id in components:
        market_members = [
            member
            for member in catalog.members
            if member.source_role == "market_history"
            and member.package_id == program.package_id
            and member.component_id == component_id
            and member.decision_trade_date == decision_trade_date
        ]
        fundamental_members = [
            member
            for member in catalog.members
            if member.source_role == "fundamental_moneyflow"
            and member.package_id == program.package_id
            and member.component_id == component_id
            and member.decision_trade_date == decision_trade_date
        ]
        if len(market_members) != 1 or len(fundamental_members) != 1:
            raise ArtifactGenerationFailedError(
                "historical source catalog does not close every Alpha leg input",
                context={
                    "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                    "component_id": component_id,
                },
            )
        catalog_member_by_role = {
            "market_history": market_members[0],
            "fundamental_moneyflow": fundamental_members[0],
        }
        start_date = market_members[0].bound_parameters.get("start_date")
        expected_partition = f"{start_date}:{decision_trade_date.isoformat()}"
        for source_role in ("market_history", "fundamental_moneyflow", "trading_calendar"):
            matching = [
                receipt
                for receipt in normalized
                if receipt.get("source_role") == source_role
                and (
                    receipt.get("leg_id") == component_id
                    or (single_component_id == component_id and receipt.get("leg_id") is None)
                )
            ]
            expected_content_hash = None
            if source_role == "trading_calendar":
                raw_per_leg = input_context.get("per_leg_window_lineage")
                if isinstance(raw_per_leg, dict) and isinstance(raw_per_leg.get(component_id), dict):
                    expected_content_hash = raw_per_leg[component_id].get("window_lineage_hash")
                elif single_component_id == component_id:
                    expected_content_hash = input_context.get("calendar_hash")
            expected_dataset_id = {
                "market_history": "market.kline_daily_raw",
                "fundamental_moneyflow": "timescaledb.fundamental_moneyflow",
                "trading_calendar": "market.trading_calendar",
            }[source_role]
            invalid_input_count = any(
                isinstance(item.get("row_count"), bool)
                or not isinstance(item.get("row_count"), int)
                or item.get("row_count") <= 0
                for item in matching
            )
            catalog_input_empty = (
                source_role in catalog_member_by_role
                and catalog_member_by_role[source_role].row_count <= 0
            )
            if (
                len(matching) != 1
                or matching[0].get("partition_ref") != expected_partition
                or matching[0].get("dataset_id") != expected_dataset_id
                or invalid_input_count
                or catalog_input_empty
                or (
                    expected_content_hash is not None
                    and matching[0].get("content_hash") != expected_content_hash
                )
            ):
                raise ArtifactGenerationFailedError(
                    "historical source receipt partition differs from the frozen Alpha leg window",
                    context={
                        "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                        "component_id": component_id,
                        "source_role": source_role,
                        "expected_partition": expected_partition,
                        "actual_partitions": [item.get("partition_ref") for item in matching],
                        "expected_content_hash": expected_content_hash,
                        "actual_content_hashes": [item.get("content_hash") for item in matching],
                        "expected_dataset_id": expected_dataset_id,
                        "actual_dataset_ids": [item.get("dataset_id") for item in matching],
                        "actual_row_counts": [item.get("row_count") for item in matching],
                    },
                )


def _source_receipt_hashes(
    receipts: tuple[Any, ...],
    source_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...],
) -> tuple[str, ...]:
    hashes = {
        canonical_json_sha256(
            {key: value for key, value in dict(receipt).items() if key not in {"first_observed_at", "observed_at"}}
        )
        for receipt in receipts
    }
    hashes.update(ref.revision_hash for ref in source_refs)
    return tuple(sorted(hashes))


def _no_candidate_reasons(
    *,
    outcome: str,
    raw_score_count: int,
    stage_trace: dict[str, Any],
) -> tuple[str, ...]:
    if outcome != "VALID_NO_CANDIDATE":
        return ()
    if raw_score_count == 0:
        return ("NO_ALPHA_CANDIDATES",)
    reasons = {
        str(item.get("reason") or "").strip()
        for stage_name in ("risk_policy_adjusted", "selection_effective")
        for item in stage_trace[stage_name].get("exclusions", [])
        if str(item.get("reason") or "").strip()
    }
    if not reasons:
        raise ArtifactGenerationFailedError(
            "filtered no-candidate result has no explicit exclusion reason",
            context={"raw_score_count": raw_score_count},
        )
    return tuple(sorted(reasons))
