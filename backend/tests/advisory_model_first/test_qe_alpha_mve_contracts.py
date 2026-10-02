from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    MVE_FAMILIES,
    FrozenAdvisoryQEAlphaMVERequestV1,
    build_default_proposals,
    build_n3_route_receipt,
    build_qe_alpha_mve_receipt,
    build_qe_alpha_mve_request,
    validate_expression,
)
from backend.services.advisory_model_first.research_control_contracts import (
    EvidenceReferenceV1,
)


EVIDENCE_ROLES = (
    "n3_n1_oracle_receipt",
    "n3_n1_learnability_receipt",
    "n3_n1_quadrant_receipt",
    "n3_n2a_audit_receipt",
    "n3_n2a_arm_summary",
    "n3_n2b_audit_receipt",
    "n3_n2b_arm_summary",
    "n3_n2b_pairwise_summary",
    "n3_n2_action_receipt",
    "n3_n2_entry_summary",
    "n3_n2_entry_support",
    "n3_n2_exit_summary",
    "n3_n2_exit_support",
    "n3_exit_learnability_receipt",
    "n3_qe_alpha_preparation",
    "n3_trial_registry_before",
)


def _ref(role: str, *, token: str = "a") -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"/tmp/{role}",
        sha256=token * 64,
        size_bytes=1,
    )


def make_qe_alpha_mve_request(**overrides: object) -> FrozenAdvisoryQEAlphaMVERequestV1:
    values: dict[str, object] = {
        "route_receipt": build_n3_route_receipt(
            candidate_top50_winner_recall=0.0176,
            candidate_top50_winner_recall_upper=0.0259,
        ),
        "evidence_refs": tuple(_ref(role) for role in EVIDENCE_ROLES),
        "preparation_path": "/tmp/preparation.json",
        "n2b_bundle_path": "/tmp/n2b",
        "outcomes_path": "/tmp/n2b/arm_signal_outcomes.parquet",
        "outcomes_ref": EvidenceReferenceV1(
            role="n3_current_parent_signal_outcomes",
            artifact_uri="/tmp/n2b/arm_signal_outcomes.parquet",
            sha256="9" * 64,
            size_bytes=1,
        ),
        "factor_root": "/tmp/factors",
        "static_factor_ref": _ref("n3_static_factors_parquet", token="b"),
        "static_schema_sha256": "c" * 64,
        "qlib_daily_root": "/tmp/qlib",
        "dataset_identity": "d" * 64,
        "registry_path": "/tmp/registry.jsonl",
        "route_path": "/tmp/current_route.md",
        "repository_root": "/tmp/repo",
        "repository_commit": "e" * 40,
        "output_root": "/tmp/output",
    }
    values.update(overrides)
    return build_qe_alpha_mve_request(**values)


def test_roster_freezes_exact_six_by_four_unique_proposals() -> None:
    proposals = build_default_proposals()

    assert len(proposals) == 24
    assert len({item.proposal_id for item in proposals}) == 24
    assert len({item.expression_sha256 for item in proposals}) == 24
    assert {family: sum(item.family == family for item in proposals) for family in MVE_FAMILIES} == {
        family: 4 for family in MVE_FAMILIES
    }
    assert all(item.direction_frozen for item in proposals)


def test_route_rejects_recall_upper_at_structural_minimum() -> None:
    with pytest.raises(ValidationError, match="recall upper"):
        build_n3_route_receipt(
            candidate_top50_winner_recall=0.10,
            candidate_top50_winner_recall_upper=0.20,
        )


def test_expression_validator_rejects_future_dynamic_and_malformed_nodes() -> None:
    with pytest.raises(ValueError, match="not implemented"):
        validate_expression({"op": "EVAL", "args": []})
    with pytest.raises(ValueError, match="unknown/missing"):
        validate_expression({"op": "FIELD", "field": "close", "future": True})
    with pytest.raises(ValueError, match="periods"):
        validate_expression({"op": "LAG", "args": [{"op": "FIELD", "field": "close"}], "periods": -1})


def test_request_rejects_budget_role_and_expression_identity_drift() -> None:
    request = make_qe_alpha_mve_request()
    assert request.planned_trial_count == 24
    assert request.generated_trial_count == 0
    assert request.sealed_holdout_accessed is False

    payload = request.model_dump(mode="json")
    payload["evidence_refs"] = payload["evidence_refs"][:-1]
    with pytest.raises(ValidationError, match="role roster drift"):
        FrozenAdvisoryQEAlphaMVERequestV1.model_validate(payload)

    payload = request.model_dump(mode="json")
    payload["proposals"][0]["expression_sha256"] = "f" * 64
    with pytest.raises(ValidationError, match="expression hash drift"):
        FrozenAdvisoryQEAlphaMVERequestV1.model_validate(payload)


def test_receipt_selection_and_next_task_are_relationally_bound() -> None:
    receipt = build_qe_alpha_mve_receipt(
        request_sha256="a" * 64,
        selected_trial_count=1,
        selected_proposal_id="N3_PRICE_VOLUME_BEHAVIOR_01",
        eligible_proposal_ids=("N3_PRICE_VOLUME_BEHAVIOR_01",),
        next_task="N3_ALPHA_CANDIDATE_CONFIRMATION_DESIGN",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
    )
    assert receipt.selected_trial_count == 1
    payload = receipt.model_dump(mode="json")
    payload["next_task"] = "N3_ALPHA_INFORMATION_SET_REVIEW"
    with pytest.raises(ValidationError, match="selection/next-task"):
        type(receipt).model_validate(payload)
