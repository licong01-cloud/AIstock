from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase0a.evidence_projection import (
    ProjectedHistoricalEvidenceV2ValidationError,
    canonical_evidence_json_sha256,
    parse_projected_historical_evidence_v2_strict,
    projected_manifest_json_sha256,
    validate_projected_historical_evidence_v2,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    Phase1GHistoricalTraceError,
    REASON_ARTIFACT_INVALID,
    build_phase1g_historical_trace_projection,
    build_phase1g_target_projection_snapshot,
    materialize_phase1g_stage_trace_envelope,
    project_phase1g_artifact,
    project_phase1g_dse,
    project_phase1g_manifest,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    parse_phase1g_source_operation,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.readiness_plan import (
    Phase1EEvidenceBinding,
    Phase1EExecutionPlan,
    Phase1EPlannedOperation,
)
from backend.services.advisory_phase1.stage_trace import (
    ComponentCapability,
    TraceCaptureContext,
)
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.tests.advisory_phase0a.test_evidence_projection import _valid_dse_payload
from backend.tests.advisory_phase1.test_phase1g_source_replay import (
    DECISION_DATE,
    g2_source_case,
)
from backend.tests.advisory_phase1.test_stage_trace import _binding


def _artifact_header(row: dict) -> dict:
    metadata = row["metadata"]
    return {
        "schema_version": "selection_score_artifact_v2",
        "package_id": row["package_id"],
        "manifest_sha256": row["manifest_sha256"],
        "trade_date": row["trade_date"],
        "data_source": row["data_source"],
        "runtime_config_hash": row["runtime_config_hash"],
        "artifact_sha256": row["artifact_sha256"],
        "score_count": row["score_count"],
        "universe_count": row["universe_count"],
        "top_score_symbol": row["top_score_symbol"],
        "status": row["status"],
        "authority_scope": metadata["authority_scope"],
        "candidate_outcome": metadata["candidate_outcome"],
        "artifact_input_context_hash": row["artifact_input_context_hash"],
        "source_revision_set_hash": row["source_revision_set_hash"],
        "asset_closure_hash": row["asset_closure_hash"],
        "provider_semantics_id": metadata["provider_semantics_id"],
        "provider_semantics_hash": metadata["provider_semantics_hash"],
        "multi_alpha_parent_parity_hash": metadata.get(
            "multi_alpha_parent_parity_hash"
        ),
    }


def _rebind_plan(
    plan: Phase1EExecutionPlanProjection,
    target: Phase1GTargetExecutionRequest,
    *,
    evidence_id: str,
    evidence_hash: str,
    artifact_id: str,
    artifact_payload_hash: str,
    asset_closure_hash: str,
) -> tuple[Phase1EExecutionPlanProjection, Phase1GTargetExecutionRequest]:
    binding_data = plan.evidence_binding.model_dump(
        mode="python", exclude={"evidence_binding_hash"}
    )
    binding_data.update(
        selection_evidence_id=evidence_id,
        selection_evidence_hash=evidence_hash,
        selection_artifact_id=artifact_id,
        selection_artifact_payload_hash=artifact_payload_hash,
        package_asset_closure_hash=asset_closure_hash,
    )
    binding = Phase1EEvidenceBinding(**binding_data)
    operations = []
    for operation in plan.planned_operations:
        data = operation.model_dump(mode="python")
        payload_key = (
            "complete_request_payload"
            if operation.complete_request_payload is not None
            else "request_template_payload"
        )
        hash_key = (
            "complete_request_hash"
            if operation.complete_request_payload is not None
            else "request_template_hash"
        )
        payload = deepcopy(data[payload_key])
        if payload is not None and isinstance(payload.get("scope_context"), dict):
            payload["scope_context"][
                "evidence_binding_hash"
            ] = binding.evidence_binding_hash
        data[payload_key] = payload
        data[hash_key] = canonical_json_sha256(payload)
        operations.append(Phase1EPlannedOperation.model_validate(data))
    plan_data = plan.model_dump(mode="python", exclude={"plan_hash", "plan_id"})
    plan_data.update(evidence_binding=binding, planned_operations=tuple(operations))
    domain_plan = Phase1EExecutionPlan.model_validate(plan_data)
    projected = Phase1EExecutionPlanProjection.model_validate(
        domain_plan.model_dump(mode="json")
    )
    target_data = target.model_dump(mode="python", exclude={"request_hash"})
    target_data.update(
        phase1e_plan_ref=target.phase1e_plan_ref.model_copy(
            update={"semantic_content_hash": projected.plan_hash}
        ),
        phase1e_plan_id=projected.plan_id,
        phase1e_plan_hash=projected.plan_hash,
        source_operation_hash=next(
            item.complete_request_hash
            for item in projected.planned_operations
            if item.complete_request_payload is not None
        ),
        observation_template_hash=next(
            item.request_template_hash
            for item in projected.planned_operations
            if item.request_template_payload is not None
        ),
    )
    return projected, Phase1GTargetExecutionRequest.model_validate(target_data)


def historical_raw_empty_case() -> dict:
    observed_at = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    target_date = date(2026, 7, 2)
    payload = _valid_dse_payload()
    payload["decision_clock"].update(
        decision_as_of_trade_date=DECISION_DATE,
        selection_as_of_trade_date=DECISION_DATE,
        target_trade_date=target_date,
        effective_entry_trade_date=target_date,
        score_trade_date=DECISION_DATE,
        reference_price_trade_date=DECISION_DATE,
        requested_selection_as_of_trade_date=DECISION_DATE,
        requested_cutoff_date=DECISION_DATE,
        effective_cutoff_date=DECISION_DATE,
        decision_cutoff_ts=observed_at,
        data_available_at=observed_at,
        decision_generated_at=observed_at,
    )
    payload["evidence_contract"]["captured_at"] = observed_at
    payload["point_in_time_context"] = {"cutoff_date": DECISION_DATE}
    payload["runtime_profile"] = {"selection": {"top_k": 20}}
    payload["selection_artifact_config"] = {"cutoff_date": DECISION_DATE}
    chain = payload["phase0a_effective_config_chain"]
    base_config = {"binding": "unit"}
    request_override = {}
    date_enforced = {"trade_date": DECISION_DATE.isoformat()}
    selection_config = {"selection": {"top_k": 20}}
    package_config = {
        "runtime_profile": selection_config,
        "selection_artifact_config": {"cutoff_date": DECISION_DATE.isoformat()},
    }
    chain.update(
        binding_base_config=base_config,
        binding_base_config_hash=canonical_evidence_json_sha256(base_config),
        binding_base_effective_from_trade_date=DECISION_DATE,
        binding_base_available_at=observed_at,
        request_override_config=request_override,
        request_override_hash=canonical_evidence_json_sha256(request_override),
        date_enforced_config=date_enforced,
        date_enforced_hash=canonical_evidence_json_sha256(date_enforced),
        selection_normalized_config=selection_config,
        selection_normalized_config_hash=canonical_evidence_json_sha256(
            selection_config
        ),
        package_effective_config=package_config,
        package_effective_config_hash=canonical_evidence_json_sha256(package_config),
        final_effective_config_hash=canonical_evidence_json_sha256(package_config),
    )
    chain.pop("chain_hash", None)
    payload["phase0a_package_lineage"] = {
        "package_id": "package-a",
        "manifest_sha256": "pending",
        "alpha_mode": "single_alpha",
    }
    payload["phase0a_asset_closure"] = []
    manifest_payload = {
        "manifest_version": "alpha_core_v1",
        "package_id": "package-a",
        "package_status": "ACTIVE",
        "alpha_mode": "single_alpha",
        "alpha_components": [{"alpha_id": "alpha-a"}],
        "alpha_combination_policy": {"method": "identity", "weights": {"alpha-a": 1.0}},
        "source_evidence": {},
        "factor_set": [],
        "model_asset": [],
    }
    manifest_sha = compute_manifest_json_sha256(manifest_payload)
    manifest_payload["manifest_sha256"] = manifest_sha
    payload["phase0a_package_lineage"]["manifest_sha256"] = manifest_sha
    source_plan, source_target, event = g2_source_case(manifest_sha256=manifest_sha)
    payload["phase0a_source_evidence"] = [
        {
            "source_role": event.input.source_role,
            "dataset_id": event.input.dataset_name,
            "row_count": event.input.row_count,
            "content_hash": event.input.partition_content_hash,
            "available_at": event.formal_available_at,
            "phase1_availability_event_ref": event.event_content_hash,
        }
    ]

    strict = parse_projected_historical_evidence_v2_strict(payload)
    source_rows = [
        item.model_dump(mode="json") for item in strict.phase0a_source_evidence
    ]
    source_hash = canonical_evidence_json_sha256(
        [
            {key: value for key, value in item.items() if key != "first_observed_at"}
            for item in source_rows
        ]
    )
    asset_hash = canonical_evidence_json_sha256([])
    runtime_config = dict(payload["selection_artifact_config"])
    runtime_config["artifact_contract_version"] = "selection_score_artifact_v2"
    provider_semantics = {"provider": "fixture"}
    artifact_row = {
        "artifact_id": "artifact-a",
        "package_id": "package-a",
        "manifest_sha256": manifest_sha,
        "trade_date": DECISION_DATE,
        "data_source": "DB_HISTORICAL",
        "runtime_config_hash": canonical_evidence_json_sha256(runtime_config),
        "scores_json": [],
        "artifact_sha256": canonical_evidence_json_sha256([]),
        "score_count": 0,
        "universe_count": 100,
        "top_score_symbol": None,
        "status": "SUCCEEDED",
        "metadata": {
            "authority_scope": "RAW_MODEL_SCORE",
            "candidate_outcome": "VALID_NO_CANDIDATE",
            "empty_stage": "alpha_raw",
            "provider_semantics_id": "fixture-provider",
            "provider_semantics_hash": canonical_evidence_json_sha256(
                provider_semantics
            ),
            "artifact_input_context": payload["point_in_time_context"],
            "source_read_receipts": source_rows,
            "asset_closure": [],
        },
        "artifact_contract_version": "selection_score_artifact_v2",
        "artifact_payload_sha256": "pending",
        "artifact_input_context_hash": canonical_evidence_json_sha256(
            payload["point_in_time_context"]
        ),
        "source_revision_set_hash": source_hash,
        "asset_closure_hash": asset_hash,
        "created_at": observed_at,
    }
    artifact_row["artifact_payload_sha256"] = canonical_evidence_json_sha256(
        _artifact_header(artifact_row)
    )
    payload["phase0a_candidate_lineage"] = {
        "selection_run_id": "selection-run-a",
        "selection_score_artifact_id": artifact_row["artifact_id"],
        "selection_score_artifact_sha256": artifact_row["artifact_sha256"],
        "selection_score_artifact_payload_sha256": artifact_row[
            "artifact_payload_sha256"
        ],
        "package_id": "package-a",
        "manifest_sha256": manifest_sha,
        "runtime_profile_version_id": chain["runtime_profile_version_id"],
        "runtime_profile_hash": chain["runtime_profile_hash"],
    }
    payload = parse_projected_historical_evidence_v2_strict(payload).model_dump(
        mode="json"
    )
    dse_hash = canonical_evidence_json_sha256(payload)
    evidence_id = f"dse_{dse_hash[:16]}"
    plan, target = _rebind_plan(
        source_plan,
        source_target,
        evidence_id=evidence_id,
        evidence_hash=dse_hash,
        artifact_id=artifact_row["artifact_id"],
        artifact_payload_hash=artifact_row["artifact_payload_sha256"],
        asset_closure_hash=asset_hash,
    )
    dse_row = {
        "evidence_id": evidence_id,
        "target_trade_date": target_date,
        "cutoff_date": DECISION_DATE,
        "package_id": "package-a",
        "manifest_sha256": manifest_sha,
        "runtime_profile_version_id": chain["runtime_profile_version_id"],
        "runtime_profile_hash": chain["runtime_profile_hash"],
        "source_type": "fixture",
        "data_source": "DB_HISTORICAL",
        "candidate_count": 0,
        "excluded_count": 0,
        "artifact_hash": dse_hash,
        "evidence_payload_json": payload,
        "created_at": observed_at,
    }
    package_row = {
        "package_id": "package-a",
        "manifest_json": manifest_payload,
        "manifest_sha256": manifest_sha,
        "alpha_mode": "single_alpha",
    }
    binding_row = {
        "binding_version_id": "binding-a",
        "program_id": "program-a",
        "package_mode": "single_package",
        "package_ids": ["package-a"],
        "runtime_config_json": base_config,
        "effective_from_trade_date": DECISION_DATE,
        "effective_to_trade_date": None,
        "activation_status": "RETIRED",
        "binding_payload_json": base_config,
    }
    return {
        "plan": plan,
        "target": target,
        "event": event,
        "dse_row": dse_row,
        "artifact_row": artifact_row,
        "package_row": package_row,
        "binding_row": binding_row,
    }


def _project_case(case: dict):  # type: ignore[no-untyped-def]
    operation = parse_phase1g_source_operation(
        phase1e_plan=case["plan"], target_request=case["target"]
    )
    replay = replay_phase1g_source_operation(
        projection=operation, availability_events=(case["event"],)
    )
    historical = build_phase1g_historical_trace_projection(
        phase1e_plan=case["plan"],
        source_operation=operation,
        source_replay=replay,
        dse=project_phase1g_dse(case["dse_row"]),
        artifact=project_phase1g_artifact(case["artifact_row"]),
        package_manifest=project_phase1g_manifest(case["package_row"]),
        binding_row=case["binding_row"],
    )
    return operation, replay, historical


def _rehash_artifact_and_dse(case: dict) -> dict:
    artifact = case["artifact_row"]
    artifact["artifact_sha256"] = canonical_evidence_json_sha256(
        artifact["scores_json"]
    )
    artifact["artifact_payload_sha256"] = canonical_evidence_json_sha256(
        _artifact_header(artifact)
    )
    payload = case["dse_row"]["evidence_payload_json"]
    payload["phase0a_candidate_lineage"].update(
        selection_score_artifact_sha256=artifact["artifact_sha256"],
        selection_score_artifact_payload_sha256=artifact["artifact_payload_sha256"],
    )
    payload = parse_projected_historical_evidence_v2_strict(payload).model_dump(
        mode="json"
    )
    dse_hash = canonical_evidence_json_sha256(payload)
    evidence_id = f"dse_{dse_hash[:16]}"
    plan, target = _rebind_plan(
        case["plan"],
        case["target"],
        evidence_id=evidence_id,
        evidence_hash=dse_hash,
        artifact_id=artifact["artifact_id"],
        artifact_payload_hash=artifact["artifact_payload_sha256"],
        asset_closure_hash=artifact["asset_closure_hash"],
    )
    case.update(plan=plan, target=target)
    case["dse_row"].update(
        evidence_id=evidence_id,
        artifact_hash=dse_hash,
        evidence_payload_json=payload,
        candidate_count=len(payload["selected_candidates"]),
        excluded_count=len(payload["excluded_candidates"]),
    )
    return case


def historical_filtered_empty_case() -> dict:
    case = historical_raw_empty_case()
    payload = deepcopy(case["dse_row"]["evidence_payload_json"])
    candidate = {
        "symbol": "000001.SZ",
        "score": 0.75,
        "rank": 1,
        "reason": "fixture_score",
        "component_scores": {},
    }
    exclusion = {
        "symbol": "000001.SZ",
        "score": 0.75,
        "rank": 1,
        "reason": "risk_filter",
        "source": "risk_policy",
        "context": {},
    }
    payload["candidate_outcome"] = "VALID_NO_CANDIDATE"
    payload["selected_candidates"] = []
    payload["excluded_candidates"] = [exclusion]
    payload["phase0a_stage_evidence"]["alpha_raw"].update(
        status="COMPLETE",
        input_count=1,
        output_count=1,
        excluded_count=0,
        candidates=[candidate],
        exclusions=[],
    )
    payload["phase0a_stage_evidence"]["hmm_adjusted"].update(
        status="COMPLETE",
        input_count=1,
        output_count=1,
        excluded_count=0,
        candidates=[candidate],
        exclusions=[],
    )
    payload["phase0a_stage_evidence"]["risk_policy_adjusted"].update(
        status="COMPLETE",
        input_count=1,
        output_count=0,
        excluded_count=1,
        candidates=[],
        exclusions=[exclusion],
    )
    payload["phase0a_stage_evidence"]["selection_effective"].update(
        status="COMPLETE",
        input_count=0,
        output_count=0,
        excluded_count=0,
        candidates=[],
        exclusions=[],
    )
    artifact = case["artifact_row"]
    artifact.update(
        scores_json=[candidate], score_count=1, top_score_symbol="000001.SZ"
    )
    artifact["metadata"]["candidate_outcome"] = "CANDIDATES_PRESENT"
    artifact["metadata"].pop("empty_stage", None)
    case["dse_row"]["evidence_payload_json"] = payload
    return _rehash_artifact_and_dse(case)


def historical_many_candidates_case(count: int = 128) -> dict:
    case = historical_raw_empty_case()
    payload = deepcopy(case["dse_row"]["evidence_payload_json"])
    candidates = [
        {
            "symbol": f"{index:06d}.SZ",
            "score": float(count - index),
            "rank": index,
            "reason": "fixture_score",
            "component_scores": {},
        }
        for index in range(1, count + 1)
    ]
    payload.update(
        candidate_outcome="CANDIDATES_PRESENT",
        selected_candidates=candidates,
        excluded_candidates=[],
    )
    for stage_name in (
        "alpha_raw",
        "hmm_adjusted",
        "risk_policy_adjusted",
        "selection_effective",
    ):
        payload["phase0a_stage_evidence"][stage_name].update(
            status="COMPLETE",
            input_count=count,
            output_count=count,
            excluded_count=0,
            candidates=candidates,
            exclusions=[],
        )
    artifact = case["artifact_row"]
    artifact.update(
        scores_json=candidates,
        score_count=count,
        universe_count=max(count, artifact["universe_count"]),
        top_score_symbol=candidates[0]["symbol"],
    )
    artifact["metadata"]["candidate_outcome"] = "CANDIDATES_PRESENT"
    artifact["metadata"].pop("empty_stage", None)
    case["dse_row"]["evidence_payload_json"] = payload
    return _rehash_artifact_and_dse(case)


def historical_multi_alpha_case(
    *, remove_leg_rank: bool = False, remove_component_scores: bool = False
) -> dict:
    case = historical_raw_empty_case()
    leg_a_hash = "a" * 64
    leg_b_hash = "b" * 64
    manifest_payload = {
        "manifest_version": "alpha_core_v1",
        "package_id": "package-a",
        "package_status": "ACTIVE",
        "alpha_mode": "multi_alpha",
        "alpha_components": [{"alpha_id": "leg_a"}, {"alpha_id": "leg_b"}],
        "alpha_combination_policy": {
            "method": "weighted_sum",
            "weights": {"leg_a": 0.4, "leg_b": 0.6},
        },
        "source_evidence": {
            "multi_alpha": {
                "legs": [
                    {
                        "leg_id": "leg_a",
                        "child_package_id": "pkg-a",
                        "child_manifest_sha256": leg_a_hash,
                    },
                    {
                        "leg_id": "leg_b",
                        "child_package_id": "pkg-b",
                        "child_manifest_sha256": leg_b_hash,
                    },
                ]
            }
        },
        "backtest_context": {"daily_strategy": {"topk": 5}},
        "factor_set": [],
        "model_asset": [],
    }
    manifest_sha = compute_manifest_json_sha256(manifest_payload)
    manifest_payload["manifest_sha256"] = manifest_sha
    source_plan, source_target, event = g2_source_case(
        manifest_sha256=manifest_sha,
        alpha_mode="multi_alpha",
        component_ids=("leg_a", "leg_b"),
    )
    payload = deepcopy(case["dse_row"]["evidence_payload_json"])
    payload["runtime_profile"] = {"selection": {"top_k": 5}}
    payload["selection_artifact_config"] = {"cutoff_date": DECISION_DATE}
    chain = payload["phase0a_effective_config_chain"]
    selection_config = payload["runtime_profile"]
    package_config = {
        "runtime_profile": selection_config,
        "selection_artifact_config": {"cutoff_date": DECISION_DATE.isoformat()},
    }
    chain.update(
        selection_normalized_config=selection_config,
        selection_normalized_config_hash=canonical_evidence_json_sha256(
            selection_config
        ),
        package_effective_config=package_config,
        package_effective_config_hash=canonical_evidence_json_sha256(package_config),
        final_effective_config_hash=canonical_evidence_json_sha256(package_config),
    )
    chain.pop("chain_hash", None)
    component_scores = {
        "leg_a": {
            "raw_score": 1.0,
            "normalized_score": 0.2,
            "weight": 0.4,
            "leg_rank": 2,
        },
        "leg_b": {
            "raw_score": 3.0,
            "normalized_score": 0.8,
            "weight": 0.6,
            "leg_rank": 1,
        },
    }
    if remove_leg_rank:
        component_scores["leg_a"].pop("leg_rank")
    if remove_component_scores:
        component_scores = {}
    candidate = {
        "symbol": "000001.SZ",
        "score": 0.56,
        "rank": 1,
        "reason": "live_multi_alpha_inference_score",
        "component_scores": component_scores,
    }
    for stage_name in (
        "alpha_raw",
        "hmm_adjusted",
        "risk_policy_adjusted",
        "selection_effective",
    ):
        payload["phase0a_stage_evidence"][stage_name].update(
            status="COMPLETE",
            input_count=1,
            output_count=1,
            excluded_count=0,
            candidates=[candidate],
            exclusions=[],
        )
    payload.update(
        candidate_outcome="CANDIDATES_PRESENT",
        selected_candidates=[candidate],
        excluded_candidates=[],
    )
    payload["phase0a_package_lineage"] = {
        "package_id": "package-a",
        "manifest_sha256": manifest_sha,
        "alpha_mode": "multi_alpha",
    }
    payload["phase0a_source_evidence"] = [
        {
            "source_role": event.input.source_role,
            "dataset_id": event.input.dataset_name,
            "row_count": event.input.row_count,
            "content_hash": event.input.partition_content_hash,
            "available_at": event.formal_available_at,
            "phase1_availability_event_ref": event.event_content_hash,
        }
    ]
    weights = {"leg_a": 0.4, "leg_b": 0.6}
    component_hashes = {"leg_a": leg_a_hash, "leg_b": leg_b_hash}
    parity = {
        "parent_package_id": "package-a",
        "parent_manifest_sha256": manifest_sha,
        "leg_ids": ["leg_a", "leg_b"],
        "component_score_artifact_sha256": component_hashes,
        "weight_artifact_id": "weight-1",
        "weight_artifact_sha256": "c" * 64,
        "combined_score_artifact_sha256": "d" * 64,
        "normalization_method": "zscore",
        "weights": weights,
    }
    multi = {
        "component_score_artifact_ids": {"leg_a": "artifact-a", "leg_b": "artifact-b"},
        "component_score_artifact_sha256": component_hashes,
        "weight_artifact_id": "weight-1",
        "weight_artifact_sha256": "c" * 64,
        "combined_score_artifact_sha256": "d" * 64,
        "multi_alpha_parent_parity_hash": canonical_json_sha256(parity),
        "multi_alpha_parent_parity": parity,
        "component_artifacts": {
            "leg_a": {"component_score_artifact_sha256": leg_a_hash},
            "leg_b": {"component_score_artifact_sha256": leg_b_hash},
        },
        "weights": weights,
    }
    payload["phase0a_package_lineage"]["multi_alpha"] = deepcopy(multi)
    artifact = case["artifact_row"]
    runtime_hash_payload = {
        "cutoff_date": DECISION_DATE,
        "multi_alpha_final_topk": 5,
        "multi_alpha_provider_version": "multi_alpha_live_selection_provider_v3",
        "artifact_contract_version": "selection_score_artifact_v2",
    }
    artifact.update(
        package_id="package-a",
        manifest_sha256=manifest_sha,
        scores_json=[candidate],
        score_count=1,
        top_score_symbol="000001.SZ",
        runtime_config_hash=canonical_evidence_json_sha256(runtime_hash_payload),
    )
    artifact["metadata"].update(
        candidate_outcome="CANDIDATES_PRESENT",
        final_topk=5,
        **multi,
    )
    artifact["metadata"].pop("empty_stage", None)
    case.update(plan=source_plan, target=source_target, event=event)
    case["package_row"] = {
        "package_id": "package-a",
        "manifest_json": manifest_payload,
        "manifest_sha256": manifest_sha,
        "alpha_mode": "multi_alpha",
    }
    case["binding_row"]["binding_payload_json"] = {"binding": "unit"}
    case["dse_row"].update(
        package_id="package-a",
        manifest_sha256=manifest_sha,
        evidence_payload_json=payload,
    )
    return _rehash_artifact_and_dse(case)


def test_strict_dse_parser_is_additive_and_reports_redacted_field_errors() -> None:
    payload = _valid_dse_payload()
    assert parse_projected_historical_evidence_v2_strict(payload)
    assert validate_projected_historical_evidence_v2(payload) is not None
    payload["phase0a_stage_evidence"].pop("alpha_raw")
    with pytest.raises(ProjectedHistoricalEvidenceV2ValidationError) as error:
        parse_projected_historical_evidence_v2_strict(payload)
    assert error.value.reason_code == "ADVISORY_PHASE0A_PROJECTED_DSE_V2_INVALID"
    assert error.value.context["errors"]
    assert "phase0a_source_evidence" not in error.value.context
    assert validate_projected_historical_evidence_v2(payload) is None


def test_projected_manifest_hash_is_byte_for_byte_compatible_with_strategy_package_authority() -> (
    None
):
    payload = historical_raw_empty_case()["package_row"]["manifest_json"]
    assert projected_manifest_json_sha256(payload) == compute_manifest_json_sha256(
        payload
    )


def test_raw_empty_projection_closes_source_dse_artifact_manifest_and_materializes_exact_envelope() -> (
    None
):
    case = historical_raw_empty_case()
    operation, replay, historical = _project_case(case)
    snapshot = build_phase1g_target_projection_snapshot(
        source_operation=operation,
        source_replay=replay,
        historical_trace=historical,
    )
    assert historical.candidate_outcome == "VALID_NO_CANDIDATE"
    assert historical.component_capability_summary is ComponentCapability.NOT_APPLICABLE
    assert snapshot.source_revision_freeze_intent == replay.freeze_intent
    assert snapshot.projected_candidate_rows == 0

    context = TraceCaptureContext(
        selection_run_id="selection-run-a",
        package_id=historical.dse.package_id,
        manifest_sha256=historical.dse.manifest_sha256,
        decision_as_of_trade_date=DECISION_DATE,
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        binding=_binding(admission_scope_id="scope-a", admission_scope_hash="2" * 64),
    )
    envelope = materialize_phase1g_stage_trace_envelope(
        context=context, projection=historical
    )
    assert envelope.trace_content["component_capability"] == "NOT_APPLICABLE"
    assert envelope == materialize_phase1g_stage_trace_envelope(
        context=context, projection=historical
    )


def test_artifact_hash_tamper_fails_closed_and_never_becomes_zero_candidate_success() -> (
    None
):
    case = historical_raw_empty_case()
    case["artifact_row"]["artifact_sha256"] = "f" * 64
    with pytest.raises(Phase1GHistoricalTraceError) as error:
        project_phase1g_artifact(case["artifact_row"])
    assert error.value.reason_code == REASON_ARTIFACT_INVALID
    assert "scores_json" not in error.value.context


def test_filtered_empty_preserves_real_stage_rows_without_becoming_raw_empty() -> None:
    _, _, historical = _project_case(historical_filtered_empty_case())
    assert historical.candidate_outcome == "VALID_NO_CANDIDATE"
    assert historical.artifact.candidate_outcome == "CANDIDATES_PRESENT"
    assert historical.stage_trace_builder_input.alpha_raw.output_count == 1
    assert historical.stage_trace_builder_input.risk_policy_adjusted.excluded_count == 1
    assert historical.stage_trace_builder_input.selection_effective.output_count == 0


def test_native_multi_alpha_full_and_degraded_component_evidence_preserve_parent_candidate() -> (
    None
):
    _, _, full = _project_case(historical_multi_alpha_case())
    assert full.component_capability_summary is ComponentCapability.FULL
    raw = full.stage_trace_builder_input.component_evidence_by_stage_and_symbol[
        "alpha_raw"
    ]["000001.SZ"]
    assert raw["capability"] == "FULL"
    assert full.stage_trace_builder_input.alpha_raw.candidates[0]["score"] == 0.56

    _, _, degraded = _project_case(historical_multi_alpha_case(remove_leg_rank=True))
    assert degraded.component_capability_summary is ComponentCapability.PARTIAL
    degraded_raw = (
        degraded.stage_trace_builder_input.component_evidence_by_stage_and_symbol[
            "alpha_raw"
        ]["000001.SZ"]
    )
    assert degraded_raw["capability"] == "PARTIAL"
    assert degraded.stage_trace_builder_input.alpha_raw.candidates[0]["score"] == 0.56

    _, _, unavailable = _project_case(
        historical_multi_alpha_case(remove_component_scores=True)
    )
    assert unavailable.component_capability_summary is ComponentCapability.UNAVAILABLE
    unavailable_raw = (
        unavailable.stage_trace_builder_input.component_evidence_by_stage_and_symbol[
            "alpha_raw"
        ]["000001.SZ"]
    )
    assert unavailable_raw["capability"] == "UNAVAILABLE"


def test_target_projection_failure_has_no_cross_target_state_or_retry_drift() -> None:
    valid_case = historical_raw_empty_case()
    _, _, first = _project_case(valid_case)
    invalid_case = historical_raw_empty_case()
    invalid_case["artifact_row"]["artifact_payload_sha256"] = "f" * 64
    with pytest.raises(Phase1GHistoricalTraceError):
        _project_case(invalid_case)
    _, _, retried = _project_case(valid_case)
    assert retried.projection_content_hash == first.projection_content_hash


def test_manual_multi_package_binding_is_rejected_without_guessing_fusion_semantics() -> (
    None
):
    case = historical_raw_empty_case()
    case["binding_row"].update(
        package_mode="union", package_ids=["package-a", "package-b"]
    )
    with pytest.raises(Phase1GHistoricalTraceError):
        _project_case(case)


def test_complete_candidate_and_stage_rows_are_not_truncated_or_sampled() -> None:
    count = 128
    _, _, historical = _project_case(historical_many_candidates_case(count))
    assert historical.candidate_count == count
    assert historical.stage_candidate_count == count * 4
    assert len(historical.artifact.scores_json) == count
    assert (
        len(historical.stage_trace_builder_input.selection_effective.candidates)
        == count
    )
    assert (
        historical.stage_trace_builder_input.selection_effective.candidates[-1]["rank"]
        == count
    )


def test_projection_contract_validators_reject_count_rank_hash_and_manifest_shape_tamper() -> (
    None
):
    operation, replay, historical = _project_case(historical_filtered_empty_case())

    artifact_data = historical.artifact.model_dump(mode="python")
    artifact_data["scores_json"][0]["rank"] = 2
    artifact_data["artifact_projection_hash"] = None
    with pytest.raises(ValidationError):
        type(historical.artifact).model_validate(artifact_data)

    artifact_data = historical.artifact.model_dump(mode="python")
    artifact_data["artifact_projection_hash"] = "f" * 64
    with pytest.raises(ValidationError):
        type(historical.artifact).model_validate(artifact_data)

    manifest_data = historical.package_manifest.model_dump(mode="python")
    manifest_data["alpha_components"] = []
    manifest_data["package_manifest_projection_hash"] = None
    with pytest.raises(ValidationError):
        type(historical.package_manifest).model_validate(manifest_data)

    stage_data = historical.stage_trace_builder_input.alpha_raw.model_dump(
        mode="python"
    )
    stage_data["input_count"] += 1
    with pytest.raises(ValidationError):
        type(historical.stage_trace_builder_input.alpha_raw).model_validate(stage_data)

    snapshot = build_phase1g_target_projection_snapshot(
        source_operation=operation,
        source_replay=replay,
        historical_trace=historical,
    )
    snapshot_data = snapshot.model_dump(mode="python")
    with pytest.raises(ValidationError):
        replay.freeze_intent.model_copy(update={"target_request_hash": "f" * 64})
    snapshot_data["source_revision_freeze_intent"] = type(
        replay.freeze_intent
    ).model_construct(
        **{
            **replay.freeze_intent.__dict__,
            "target_request_hash": "f" * 64,
        }
    )
    snapshot_data["target_projection_snapshot_hash"] = None
    with pytest.raises(ValidationError):
        type(snapshot).model_validate(snapshot_data)


def test_manifest_projection_copies_every_declared_runtime_asset_without_loading_assets() -> (
    None
):
    row = deepcopy(historical_raw_empty_case()["package_row"])
    payload = row["manifest_json"]
    payload["factor_set"] = [
        {"asset_ref": "factor.py", "sha256": "a" * 64},
        {"asset_ref": "missing-hash"},
    ]
    payload["model_asset"] = {
        "asset_ref": "model.bin",
        "sha256": "b" * 64,
        "model_code_assets": [
            {"asset_ref": "model.py", "sha256": "c" * 64},
            {"asset_ref": "missing-code-hash"},
        ],
    }
    payload["runtime_assets"] = {
        "alpha158": {
            "enabled": True,
            "asset_ref": "alpha158.json",
            "sha256": "d" * 64,
        }
    }
    row["manifest_sha256"] = compute_manifest_json_sha256(payload)
    payload["manifest_sha256"] = row["manifest_sha256"]
    projected = project_phase1g_manifest(row)
    assert [item["asset_type"] for item in projected.declared_runtime_assets] == [
        "factor_code",
        "factor_schema",
        "model_code",
        "model_weight",
    ]


def test_materialize_rejects_context_identity_drift_before_building_envelope() -> None:
    _, _, historical = _project_case(historical_raw_empty_case())
    context = TraceCaptureContext(
        selection_run_id="wrong-run",
        package_id=historical.dse.package_id,
        manifest_sha256=historical.dse.manifest_sha256,
        decision_as_of_trade_date=DECISION_DATE,
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        binding=_binding(admission_scope_id="scope-a", admission_scope_hash="2" * 64),
    )
    with pytest.raises(Phase1GHistoricalTraceError):
        materialize_phase1g_stage_trace_envelope(context=context, projection=historical)


def test_stage_transition_closure_rejects_silent_drop_and_raw_empty_fabrication() -> (
    None
):
    dropped = historical_many_candidates_case(2)
    payload = dropped["dse_row"]["evidence_payload_json"]
    retained = payload["phase0a_stage_evidence"]["alpha_raw"]["candidates"][0]
    payload["selected_candidates"] = [retained]
    payload["excluded_candidates"] = []
    for stage_name in (
        "hmm_adjusted",
        "risk_policy_adjusted",
        "selection_effective",
    ):
        payload["phase0a_stage_evidence"][stage_name].update(
            input_count=1,
            output_count=1,
            excluded_count=0,
            candidates=[retained],
            exclusions=[],
        )
    with pytest.raises(Phase1GHistoricalTraceError) as error:
        _project_case(_rehash_artifact_and_dse(dropped))
    assert error.value.reason_code == "ADVISORY_PHASE1G_HISTORICAL_TRACE_MISMATCH"

    fabricated = historical_raw_empty_case()
    payload = fabricated["dse_row"]["evidence_payload_json"]
    fake_candidate = {
        "symbol": "999999.SZ",
        "score": 1.0,
        "rank": 1,
        "reason": "fabricated",
        "component_scores": {},
    }
    payload["phase0a_stage_evidence"]["hmm_adjusted"].update(
        status="COMPLETE",
        input_count=1,
        output_count=1,
        excluded_count=0,
        candidates=[fake_candidate],
        exclusions=[],
    )
    with pytest.raises(Phase1GHistoricalTraceError) as error:
        _project_case(_rehash_artifact_and_dse(fabricated))
    assert error.value.reason_code == "ADVISORY_PHASE1G_HISTORICAL_TRACE_MISMATCH"


def test_stage_transition_closure_accepts_hmm_passthrough_and_exact_topk_tail() -> None:
    passthrough = historical_many_candidates_case(2)
    hmm = passthrough["dse_row"]["evidence_payload_json"]["phase0a_stage_evidence"][
        "hmm_adjusted"
    ]
    hmm.update(
        status="NOT_APPLICABLE",
        input_count=2,
        output_count=0,
        excluded_count=0,
        candidates=[],
        exclusions=[],
    )
    _, _, projected = _project_case(_rehash_artifact_and_dse(passthrough))
    assert projected.candidate_count == 2

    topk = historical_many_candidates_case(3)
    payload = topk["dse_row"]["evidence_payload_json"]
    selected = payload["phase0a_stage_evidence"]["risk_policy_adjusted"]["candidates"][
        :2
    ]
    payload["selected_candidates"] = selected
    payload["phase0a_stage_evidence"]["selection_effective"].update(
        input_count=2,
        output_count=2,
        excluded_count=0,
        candidates=selected,
        exclusions=[],
        semantic_payload={
            "candidate_pool_count": 3,
            "inspected_count": 2,
            "unprocessed_tail_count": 1,
        },
    )
    _, _, projected = _project_case(_rehash_artifact_and_dse(topk))
    assert projected.candidate_count == 2
    assert projected.stage_trace_builder_input.selection_effective.input_count == 2

    invalid_tail = historical_many_candidates_case(3)
    payload = invalid_tail["dse_row"]["evidence_payload_json"]
    selected = payload["phase0a_stage_evidence"]["risk_policy_adjusted"]["candidates"][
        :2
    ]
    payload["selected_candidates"] = selected
    payload["phase0a_stage_evidence"]["selection_effective"].update(
        input_count=2,
        output_count=2,
        excluded_count=0,
        candidates=selected,
        exclusions=[],
        semantic_payload={},
    )
    with pytest.raises(Phase1GHistoricalTraceError):
        _project_case(_rehash_artifact_and_dse(invalid_tail))


def test_projection_contracts_reject_derived_summary_drift_and_nested_mutation() -> (
    None
):
    operation, replay, historical = _project_case(historical_raw_empty_case())

    historical_data = historical.model_dump(mode="python")
    historical_data["candidate_count"] = 1
    historical_data["projection_content_hash"] = None
    with pytest.raises(ValidationError):
        type(historical).model_validate(historical_data)

    snapshot = build_phase1g_target_projection_snapshot(
        source_operation=operation,
        source_replay=replay,
        historical_trace=historical,
    )
    snapshot_data = snapshot.model_dump(mode="python")
    snapshot_data["projected_candidate_rows"] = 999
    snapshot_data["target_projection_snapshot_hash"] = None
    with pytest.raises(ValidationError):
        type(snapshot).model_validate(snapshot_data)

    snapshot_data = snapshot.model_dump(mode="python")
    snapshot_data["expected_capture_plan_count"] += 1
    snapshot_data["target_projection_snapshot_hash"] = None
    with pytest.raises(ValidationError):
        type(snapshot).model_validate(snapshot_data)

    _, _, candidate_projection = _project_case(historical_many_candidates_case(1))
    artifact_hash = candidate_projection.artifact.artifact_projection_hash
    projection_hash = candidate_projection.projection_content_hash
    with pytest.raises(TypeError, match="cannot be mutated"):
        candidate_projection.artifact.scores_json[0]["score"] = 999.0
    with pytest.raises(TypeError, match="cannot be mutated"):
        candidate_projection.stage_trace_builder_input.runtime_config["drift"] = True
    assert candidate_projection.artifact.artifact_projection_hash == artifact_hash
    assert candidate_projection.projection_content_hash == projection_hash
