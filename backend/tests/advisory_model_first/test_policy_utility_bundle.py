from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_utility_bundle import (
    find_policy_utility_bundle_for_request,
    load_policy_utility_bundle,
    publish_policy_utility_bundle,
)
from backend.tests.advisory_model_first.test_policy_utility_contracts import _request


class _Booster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("utility-model", encoding="utf-8")


def _publish(tmp_path, resource, *, incomplete: bool = False):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    (policy / "manifest.json").write_text(json.dumps({"bundle": "source"}), encoding="utf-8")
    request = _request(policy_dataset_bundle_root=str(policy), output_root=str(tmp_path))
    advancement = {
        "experiment_status": ("NEGATIVE_STOP_INCOMPLETE_CPCV" if incomplete else "NEGATIVE_STOP_NOT_ADVANCED"),
        "advanced_to_stage_b": False,
    }
    return request, publish_policy_utility_bundle(
        request=request,
        arm_boosters=(
            None
            if incomplete
            else {
                "ARM_P0D_V2_BINARY_PARITY": _Booster(),
                "ARM_P0E_V2_WEIGHTED_BINARY": _Booster(),
                "ARM_P0F_V2_HUBER_UTILITY": _Booster(),
            }
        ),
        feature_schema={"trained_feature_names": ["x"], "categorical_vocabulary": {}},
        transform_receipt={"location_bps": 1.0, "scale_bps": 2.0},
        runtime_hmm_models={"models": {"1": {"cutoff": "2026-02-02"}}},
        runtime_hmm_unavailable=[],
        walk_forward_hmm_receipt={"blocks": []},
        trial_metrics=pd.DataFrame({"trial_id": ["a"], "path_id": ["path_00"]}),
        block_scores=pd.DataFrame({"trial_id": ["a"], "block_id": [0], "score": [1.0]}),
        pbo_receipt={"status": "COMPUTED", "pbo": 0.5},
        winner_receipt=(
            {"family_id": None, "seed": None, "status": "NOT_SELECTED_INCOMPLETE_CPCV"}
            if incomplete
            else {
                "winner_by_arm": {
                    "ARM_P0F_V2_HUBER_UTILITY": {
                        "family_id": "FAMILY_POLICY_UTILITY_CORE",
                        "seed": 20260813,
                    }
                }
            }
        ),
        baseline_comparison={"selection": 1.0},
        reference_comparison={"p0d": 1.0, "p0e": 2.0},
        advancement_receipt=advancement,
        training_log={"wall": resource},
        resource_report={"peak": resource},
    )


def test_policy_utility_bundle_is_immutable_and_exact_retry_reuses_identity(tmp_path) -> None:
    request, first = _publish(tmp_path, 1)
    _, second = _publish(tmp_path, 999)
    first_id, path, manifest = first
    assert second[0] == first_id
    assert second[1] == path
    assert manifest["experiment_status"] == "NEGATIVE_STOP_NOT_ADVANCED"
    assert manifest["stage_b_eligible"] is False
    assert manifest["runtime_eligible"] is False
    assert find_policy_utility_bundle_for_request(request)[0] == first_id
    (path / "p0f_v2_model.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError):
        load_policy_utility_bundle(path, expected_bundle_id=first_id, load_booster=False)


def test_incomplete_policy_utility_bundle_is_evidence_only_and_has_no_model(tmp_path) -> None:
    _, published = _publish(tmp_path, 1, incomplete=True)
    bundle_id, path, manifest = published
    assert manifest["experiment_status"] == "NEGATIVE_STOP_INCOMPLETE_CPCV"
    assert manifest["all_arm_models_available"] is False
    assert not (path / "p0f_v2_model.txt").exists()
    load_policy_utility_bundle(path, expected_bundle_id=bundle_id, load_booster=False)
    with pytest.raises(AdvisoryModelFirstError, match="no loadable arm models"):
        load_policy_utility_bundle(path, expected_bundle_id=bundle_id, load_booster=True)
