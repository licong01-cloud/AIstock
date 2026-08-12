from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_bundle import (
    load_meta_label_bundle,
    publish_meta_label_bundle,
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
