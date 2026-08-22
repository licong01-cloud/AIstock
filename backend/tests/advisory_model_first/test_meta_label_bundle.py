from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_bundle import (
    _stable_hmm_payload,
    find_meta_label_bundle_for_request,
    load_exact_meta_label_runtime_bundle,
    load_meta_label_bundle,
    publish_meta_label_bundle,
    score_meta_label_bundle,
)
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_meta_label_contracts import _request


class _Booster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("model", encoding="utf-8")


def _shadow_policy() -> dict[str, object]:
    return {
        "target_count": 5,
        "rank_enter_threshold": 5,
        "rank_exit_threshold": 40,
        "rank_exit_confirm_days": 2,
        "daily_replacement_budget": 5,
        "stop_loss_bps": 800,
        "take_profit_bps": 1800,
        "trailing_stop_bps": 700,
        "time_stop_days": 20,
        "take_profit_mode": "trailing",
        "entry_price_basis": "next_open_executable",
        "exit_price_basis": "next_open_executable",
    }


def _cost_policy() -> AdvisoryPolicyCostV1:
    return AdvisoryPolicyCostV1(buy_cost_bps=3.0, sell_cost_bps=13.0)


def _publish(tmp_path, resource):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    shadow_policy = _shadow_policy()
    policy_bundle = tmp_path / "policy_datasets" / ("1" * 64)
    policy_bundle.mkdir(parents=True, exist_ok=True)
    (policy_bundle / "shadow_policy.json").write_text(
        json.dumps(shadow_policy), encoding="utf-8"
    )
    request = _request(
        policy_dataset_bundle_root=str(policy),
        output_root=str(tmp_path),
        shadow_policy_sha256=canonical_json_sha256(shadow_policy),
        cost_policy_sha256=_cost_policy().policy_sha256,
    )
    cost_policy = _cost_policy().model_dump(mode="json")
    (policy_bundle / "cost_policy.json").write_text(json.dumps(cost_policy), encoding="utf-8")
    (policy / "manifest.json").write_text(
        json.dumps(
            {
                "policy_dataset_bundle_id": "1" * 64,
                "shadow_policy_sha256": canonical_json_sha256(shadow_policy),
                "cost_policy_sha256": _cost_policy().policy_sha256,
            }
        ),
        encoding="utf-8",
    )
    return publish_meta_label_bundle(
        request=request,
        booster=_Booster(),
        feature_schema={"trained_feature_names": ["x"], "categorical_vocabulary": {}},
        runtime_hmm_models={
            "schema_version": "fresh_sector_hmm_bundle_v1",
            "observation_order": ["sector_return_1"],
            "models": {
                "1": {
                    "schema_version": "fresh_sector_hmm_v1",
                    "continuation_cutoff": "2026-02-02",
                }
            },
        },
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
        policy_dataset_bundle_root=str(tmp_path / "policy"),
        output_root=str(tmp_path),
        shadow_policy_sha256=canonical_json_sha256(_shadow_policy()),
        cost_policy_sha256=_cost_policy().policy_sha256,
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
        policy_dataset_bundle_root=str(tmp_path / "policy"),
        output_root=str(tmp_path),
        shadow_policy_sha256=canonical_json_sha256(_shadow_policy()),
        cost_policy_sha256=_cost_policy().policy_sha256,
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        find_meta_label_bundle_for_request(request)
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


def test_exact_meta_label_runtime_loader_validates_manifest_and_hmm_cutoff(tmp_path) -> None:
    bundle_id, path, _ = _publish(tmp_path, 1)
    manifest_sha256 = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()

    loaded = load_exact_meta_label_runtime_bundle(
        model_root=tmp_path,
        bundle_id=bundle_id,
        bundle_manifest_sha256=manifest_sha256,
        load_booster=False,
    )

    assert loaded["continuation_cutoff"] == "2026-02-02"
    assert loaded["manifest_file_sha256"] == manifest_sha256
    assert loaded["feature_schema"]["trained_feature_names"] == ["x"]
    assert loaded["shadow_policy_maturity_horizon_days"] == 20
    assert loaded["cost_policy_sha256"] == _cost_policy().policy_sha256


def test_exact_meta_label_runtime_loader_rejects_shadow_policy_drift(tmp_path) -> None:
    bundle_id, path, _ = _publish(tmp_path, 1)
    manifest_sha256 = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()
    policy_path = tmp_path / "policy_datasets" / ("1" * 64) / "shadow_policy.json"
    payload = _shadow_policy()
    payload["time_stop_days"] = 19
    policy_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        load_exact_meta_label_runtime_bundle(
            model_root=tmp_path,
            bundle_id=bundle_id,
            bundle_manifest_sha256=manifest_sha256,
            load_booster=False,
        )

    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


def test_exact_meta_label_runtime_loader_rejects_cost_policy_drift(tmp_path) -> None:
    bundle_id, path, _ = _publish(tmp_path, 1)
    manifest_sha256 = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()
    cost_path = tmp_path / "policy_datasets" / ("1" * 64) / "cost_policy.json"
    payload = _cost_policy().model_dump(mode="json")
    payload["sell_cost_bps"] = 99.0
    cost_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        load_exact_meta_label_runtime_bundle(
            model_root=tmp_path,
            bundle_id=bundle_id,
            bundle_manifest_sha256=manifest_sha256,
            load_booster=False,
        )

    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


def test_exact_meta_label_runtime_loader_rejects_descriptor_manifest_drift(tmp_path) -> None:
    bundle_id, _, _ = _publish(tmp_path, 1)

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        load_exact_meta_label_runtime_bundle(
            model_root=tmp_path,
            bundle_id=bundle_id,
            bundle_manifest_sha256="f" * 64,
            load_booster=False,
        )

    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


def test_exact_meta_label_runtime_loader_rejects_bundle_root_symlink_escape(tmp_path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    bundle_id, path, _ = _publish(outside, 1)
    manifest_sha256 = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()
    model_root = tmp_path / "model-root"
    model_root.mkdir()
    try:
        (model_root / "meta_label_bundles").symlink_to(
            outside / "meta_label_bundles", target_is_directory=True
        )
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        load_exact_meta_label_runtime_bundle(
            model_root=model_root,
            bundle_id=bundle_id,
            bundle_manifest_sha256=manifest_sha256,
            load_booster=False,
        )

    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


def test_exact_meta_label_runtime_loader_rejects_policy_root_symlink_escape(tmp_path) -> None:
    bundle_id, path, _ = _publish(tmp_path, 1)
    manifest_sha256 = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()
    outside = tmp_path.parent / f"{tmp_path.name}-policy-outside"
    shutil.move(str(tmp_path / "policy_datasets"), outside)
    try:
        (tmp_path / "policy_datasets").symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        load_exact_meta_label_runtime_bundle(
            model_root=tmp_path,
            bundle_id=bundle_id,
            bundle_manifest_sha256=manifest_sha256,
            load_booster=False,
        )

    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_BUNDLE_INVALID"


class _InvalidBooster:
    def feature_name(self):
        return ["x"]

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


class _InspectingBooster:
    def __init__(self) -> None:
        self.matrix = None

    def feature_name(self):
        return ["l2_code_id", "l2_code_id__missing"]

    def predict(self, matrix):
        self.matrix = matrix.copy()
        return [0.6]


def test_meta_label_scorer_marks_unseen_category_missing_instead_of_silent_nan(tmp_path) -> None:
    (tmp_path / "feature_schema.json").write_text(
        '{"trained_feature_names":["l2_code_id","l2_code_id__missing"],'
        '"categorical_vocabulary":{"l2_code_id":[1,2]}}',
        encoding="utf-8",
    )
    booster = _InspectingBooster()
    bundle = {"bundle_path": tmp_path, "booster": booster}
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": ["2026-01-01"],
            "target_trade_date": ["2026-01-02"],
            "instrument": ["000001.SZ"],
            "selection_effective_rank": [1],
            "l2_code_id": [999],
            "l2_code_id__missing": [0],
        }
    )

    scored = score_meta_label_bundle(bundle, features)

    assert scored.iloc[0]["take_probability"] == pytest.approx(0.6)
    assert pd.isna(booster.matrix["l2_code_id"].iloc[0])
    assert booster.matrix["l2_code_id__missing"].iloc[0] == 1
