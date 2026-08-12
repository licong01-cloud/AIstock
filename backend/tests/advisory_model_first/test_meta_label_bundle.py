from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_bundle import (
    _stable_hmm_payload,
    find_meta_label_bundle_for_request,
    load_meta_label_bundle,
    publish_meta_label_bundle,
    score_meta_label_bundle,
)
from backend.tests.advisory_model_first.test_meta_label_contracts import _request


class _Booster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("model", encoding="utf-8")


def _publish(tmp_path, resource):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    (policy / "manifest.json").write_text("{}", encoding="utf-8")
    request = _request(policy_dataset_bundle_root=str(policy), output_root=str(tmp_path))
    return publish_meta_label_bundle(
        request=request,
        booster=_Booster(),
        feature_schema={"trained_feature_names": ["x"], "categorical_vocabulary": {}},
        runtime_hmm_models={"models": {}},
        runtime_hmm_unavailable=[],
        walk_forward_hmm_receipt={"blocks": []},
        trial_metrics=pd.DataFrame({"trial_id": ["a"], "score": [1.0]}),
        block_scores=pd.DataFrame({"trial_id": ["a"], "block_id": [0], "score": [1.0]}),
        pbo_receipt={"status": "COMPUTED", "pbo": 0.5},
        winner_receipt={"family_id": "FAMILY_CORE", "seed": 20260813},
        baseline_comparison={"selection": 1.0},
        training_log={"wall": resource},
        resource_report={"peak": resource},
    )


def test_meta_label_bundle_identity_ignores_dynamic_resource_report(tmp_path) -> None:
    first_id, path, first = _publish(tmp_path, 1)
    second_id, second_path, second = _publish(tmp_path, 999)
    assert first_id == second_id
    assert path == second_path
    assert first == second
    (path / "model.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError):
        load_meta_label_bundle(path, expected_bundle_id=first_id, load_booster=False)


def test_meta_label_bundle_exact_request_reuse_and_hmm_jitter_are_deterministic(tmp_path) -> None:
    first_id, path, _ = _publish(tmp_path, 1)
    request = _request(
        policy_dataset_bundle_root=str(tmp_path / "policy"), output_root=str(tmp_path)
    )
    assert find_meta_label_bundle_for_request(request)[0] == first_id
    assert _stable_hmm_payload({"x": 0.5095992816185344}) == _stable_hmm_payload(
        {"x": 0.509599281618536}
    )
    assert _stable_hmm_payload({"final_log_likelihood_delta": 7.871711386542302e-05}) == {
        "final_log_likelihood_status": "NON_REGRESSING"
    }
    assert _stable_hmm_payload(
        {"reason": "fit_likelihood_regressed", "log_likelihood_delta": -0.08011144564834183}
    ) == {
        "reason": "fit_likelihood_regressed",
        "log_likelihood_status": "REGRESSED_BEYOND_TOLERANCE",
    }
    assert path.is_dir()


def test_meta_label_bundle_exact_request_rejects_multiple_claimants(tmp_path) -> None:
    _, path, _ = _publish(tmp_path, 1)
    shutil.copytree(path, path.parent / ("e" * 64))
    request = _request(
        policy_dataset_bundle_root=str(tmp_path / "policy"), output_root=str(tmp_path)
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        find_meta_label_bundle_for_request(request)
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


class _InvalidBooster:
    def predict(self, _matrix):
        return [float("nan")]


def test_meta_label_scorer_rejects_non_finite_probability(tmp_path) -> None:
    (tmp_path / "feature_schema.json").write_text(
        '{"trained_feature_names":["x"],"categorical_vocabulary":{}}', encoding="utf-8"
    )
    bundle = {"bundle_path": tmp_path, "booster": _InvalidBooster()}
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": ["2026-01-01"],
            "target_trade_date": ["2026-01-02"],
            "instrument": ["000001.SZ"],
            "selection_effective_rank": [1],
            "x": [1.0],
        }
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        score_meta_label_bundle(bundle, features)
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_SCORING_INVALID"
