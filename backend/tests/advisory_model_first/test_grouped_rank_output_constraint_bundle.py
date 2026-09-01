from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.grouped_rank_output_constraint_bundle import (
    find_grouped_rank_bundle_for_request,
    load_grouped_rank_bundle,
    publish_grouped_rank_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_grouped_rank_output_constraint_contracts import _request


class _Booster:
    def __init__(self, name: str) -> None:
        self.name = name

    def save_model(self, path: str) -> None:
        Path(path).write_text(self.name, encoding="utf-8")


def _publish(tmp_path: Path, resource: int, *, incomplete: bool = False):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    (policy / "manifest.json").write_text(json.dumps({"bundle": "source"}), encoding="utf-8")
    request = _request(policy_dataset_bundle_root=str(policy), output_root=str(tmp_path))
    advancement = {
        "experiment_status": (
            "NEGATIVE_STOP_INCOMPLETE_CPCV" if incomplete else "NEGATIVE_STOP_NOT_ADVANCED"
        ),
        "advanced_to_stage_b": False,
    }
    published = publish_grouped_rank_bundle(
        request=request,
        return_booster=None if incomplete else _Booster("return-model"),
        liability_booster=None if incomplete else _Booster("liability-model"),
        feature_schema={"trained_feature_names": ["x"], "categorical_vocabulary": {}},
        runtime_hmm_models={"models": {"1": {"cutoff": "2026-02-02"}}},
        runtime_hmm_unavailable=[],
        walk_forward_hmm_receipt={"blocks": []},
        inner_oof_constraint_receipt={"status": "PASS"},
        transform_receipt={"return": {"location": 1.0}, "liability": {"location": 0.1}},
        trial_metrics=pd.DataFrame({"trial_id": ["a"], "path_id": ["path_00"]}),
        block_scores=pd.DataFrame({"trial_id": ["a"], "block_id": [0], "score": [1.0]}),
        candidate_diagnostics={"return_mae": 1.0},
        pbo_receipt={"status": "COMPUTED", "pbo": 0.5},
        winner_receipt={"family_id": "FAMILY_GROUPED_RANK_CORE", "seed": 20260813},
        baseline_comparison={"selection": 1.0},
        reference_comparison={"p0d": 1.0, "p0f": 2.0, "p0g": 3.0},
        advancement_receipt=advancement,
        training_log={"wall": resource},
        resource_report={"peak": resource},
    )
    return request, published


def test_bundle_is_immutable_and_exact_retry_reuses_identity(tmp_path: Path) -> None:
    request, first = _publish(tmp_path, 1)
    _, second = _publish(tmp_path, 999)
    first_id, path, manifest = first
    assert second[0] == first_id
    assert second[1] == path
    assert manifest["runtime_eligible"] is False
    assert manifest["activated"] is False
    assert set(manifest["model_files"]) == {"return", "liability"}
    assert find_grouped_rank_bundle_for_request(request)[0] == first_id
    (path / "inner_oof_constraint_receipt.json").write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="differs from manifest"):
        load_grouped_rank_bundle(path, expected_bundle_id=first_id, load_boosters=False)


def test_incomplete_bundle_is_evidence_only_and_has_no_models(tmp_path: Path) -> None:
    _, published = _publish(tmp_path, 1, incomplete=True)
    bundle_id, path, manifest = published
    assert manifest["experiment_status"] == "NEGATIVE_STOP_INCOMPLETE_CPCV"
    assert manifest["model_available"] is False
    assert not (path / "return_model.txt").exists()
    assert not (path / "liability_model.txt").exists()
    load_grouped_rank_bundle(path, expected_bundle_id=bundle_id, load_boosters=False)
    with pytest.raises(AdvisoryModelFirstError, match="no loadable models"):
        load_grouped_rank_bundle(path, expected_bundle_id=bundle_id, load_boosters=True)


def test_bundle_rejects_one_head_only(tmp_path: Path) -> None:
    policy = tmp_path / "policy"
    policy.mkdir()
    (policy / "manifest.json").write_text("{}", encoding="utf-8")
    request = _request(policy_dataset_bundle_root=str(policy), output_root=str(tmp_path))
    with pytest.raises(AdvisoryModelFirstError, match="both models or neither"):
        publish_grouped_rank_bundle(
            request=request,
            return_booster=_Booster("return"),
            liability_booster=None,
            feature_schema={},
            runtime_hmm_models={},
            runtime_hmm_unavailable=[],
            walk_forward_hmm_receipt={},
            inner_oof_constraint_receipt={},
            transform_receipt={},
            trial_metrics=pd.DataFrame(),
            block_scores=pd.DataFrame(),
            candidate_diagnostics={},
            pbo_receipt={},
            winner_receipt={},
            baseline_comparison={},
            reference_comparison={},
            advancement_receipt={"experiment_status": "X", "advanced_to_stage_b": False},
            training_log={},
            resource_report={},
        )

