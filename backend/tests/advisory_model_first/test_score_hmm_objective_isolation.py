from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    ObjectiveContract,
    ResearchStudyType,
)
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    PACKAGE_SCORE_CALIBRATION_ONLY,
    SCORE_HMM_ARM_IDS,
    SCORE_HMM_PREDECESSOR_BY_ARM,
    SCORE_PLUS_MARKET_HMM,
    SCORE_PLUS_RAW_MARKET_SHAPE,
    build_score_hmm_frontier_receipt,
)
from backend.tests.advisory_model_first.test_score_hmm_admission_pipeline import build_test_request


def _receipt(**overrides: object):
    values: dict[str, object] = {
        "request_sha256": "1" * 64,
        "selected_trial_count": 0,
        "selected_arm_id": None,
        "eligible_arm_ids": (),
        "arm_statuses": {
            arm_id: ("EVALUATED" if index < 3 else "NOT_RUN_SOURCE_UNAVAILABLE")
            for index, arm_id in enumerate(SCORE_HMM_ARM_IDS)
        },
        "evidence_class": "AUX_EXECUTED_FRONTIER_SELECTED_ZERO",
        "next_task": "N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED",
        "result_files_sha256": "2" * 64,
        "resource_report_sha256": "3" * 64,
        "created_at": datetime(2026, 9, 5, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return build_score_hmm_frontier_receipt(**values)


def test_auxiliary_request_is_risk_contract_navigation_only_and_cannot_rerank_or_activate() -> None:
    request = build_test_request()
    assert request.objective_contract == ObjectiveContract.RISK_MANAGED_ADVISORY
    assert request.study_type == ResearchStudyType.LEARNABILITY_AUDIT
    assert request.decision_use == DecisionUse.NAVIGATION_ONLY
    assert request.selection_rank_change_allowed is False
    assert request.position_weight_output_allowed is False
    assert request.runtime_activation_allowed is False
    assert request.deployable is False


def test_nested_predecessor_graph_is_frozen_and_sector_unavailability_cannot_select() -> None:
    assert SCORE_HMM_PREDECESSOR_BY_ARM[PACKAGE_SCORE_CALIBRATION_ONLY] == ()
    assert SCORE_HMM_PREDECESSOR_BY_ARM[SCORE_PLUS_RAW_MARKET_SHAPE] == (PACKAGE_SCORE_CALIBRATION_ONLY,)
    assert SCORE_HMM_PREDECESSOR_BY_ARM[SCORE_PLUS_MARKET_HMM] == (SCORE_PLUS_RAW_MARKET_SHAPE,)
    request = build_test_request()
    assert [(item.arm_id, item.run_status) for item in request.arm_specs[3:]] == [
        (SCORE_HMM_ARM_IDS[3], "NOT_RUN_SOURCE_UNAVAILABLE"),
        (SCORE_HMM_ARM_IDS[4], "NOT_RUN_SOURCE_UNAVAILABLE"),
    ]


def test_frontier_permits_exactly_one_preselected_candidate_or_zero_and_never_activates() -> None:
    zero = _receipt()
    selected = _receipt(
        selected_trial_count=1,
        selected_arm_id=PACKAGE_SCORE_CALIBRATION_ONLY,
        eligible_arm_ids=(PACKAGE_SCORE_CALIBRATION_ONLY,),
        evidence_class="AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY",
        next_task="N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN",
    )
    assert zero.selected_trial_count == 0
    assert zero.next_task == "N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED"
    assert selected.selected_trial_count == 1
    assert selected.next_task == "N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN"
    assert selected.runtime_eligible is False
    assert selected.runtime_activation_written is False
    assert selected.selection_rank_changed is False


def test_zero_selection_cannot_keep_a_frontier_candidate_for_result_after_reselection() -> None:
    with pytest.raises(ValidationError):
        _receipt(eligible_arm_ids=(PACKAGE_SCORE_CALIBRATION_ONLY,))


def test_partial_executable_frontier_reports_true_evaluated_count_and_cannot_select() -> None:
    statuses = {
        arm_id: (
            "EVALUATED"
            if index == 0
            else "SOURCE_UNAVAILABLE_NO_POLICY_EVALUATION"
            if index < 3
            else "NOT_RUN_SOURCE_UNAVAILABLE"
        )
        for index, arm_id in enumerate(SCORE_HMM_ARM_IDS)
    }
    partial = _receipt(
        arm_statuses=statuses,
        evidence_class="AUX_PARTIAL_SOURCE_UNAVAILABLE",
        next_task="N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW",
    )

    assert partial.generated_trial_count == 3
    assert partial.evaluated_trial_count == 1
    assert partial.selected_arm_id is None

    with pytest.raises(ValidationError):
        _receipt(
            arm_statuses=statuses,
            evaluated_trial_count=3,
            evidence_class="AUX_PARTIAL_SOURCE_UNAVAILABLE",
            next_task="N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW",
        )

    with pytest.raises(ValidationError):
        _receipt(
            arm_statuses=statuses,
            evaluated_trial_count=1,
            selected_trial_count=1,
            selected_arm_id=PACKAGE_SCORE_CALIBRATION_ONLY,
            eligible_arm_ids=(PACKAGE_SCORE_CALIBRATION_ONLY,),
            evidence_class="AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY",
            next_task="N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN",
        )
