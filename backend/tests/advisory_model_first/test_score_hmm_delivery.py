from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import advisory_score_hmm_admission_mve as cli
from backend.services.advisory_model_first import score_hmm_admission_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    SCORE_HMM_ARM_IDS,
    SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS,
)
from backend.services.advisory_model_first.score_hmm_admission_pipeline import (
    SCORE_HMM_BUNDLE_MEMBERS,
    ScoreHMMEvaluationResult,
    _deliver_bundle,
    _find_existing_bundle,
    _publish_bundle,
    inspect_score_hmm_admission_bundle,
)
from backend.tests.advisory_model_first.test_score_hmm_admission_pipeline import build_test_request


def _publish_fixture(tmp_path: Path, *, partial: bool = False) -> tuple[Path, object, bytes]:
    route = tmp_path / "current_route.md"
    route_bytes = b"# main route\n- next_task: N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION\n"
    route.write_bytes(route_bytes)
    request = build_test_request(
        output_root=tmp_path.as_posix(),
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        current_route_path=route.as_posix(),
        auxiliary_route_path=(tmp_path / "current_auxiliary_route.md").as_posix(),
    )
    decision = pd.Timestamp("2025-01-02")
    target = pd.Timestamp("2025-01-03")
    target_coverage = pd.DataFrame(
        {"decision_as_of_trade_date": [decision], "target_head": ["PRIMARY"], "known_count": [1]}
    )
    oof = pd.DataFrame(
        {
            "arm_id": [SCORE_HMM_ARM_IDS[0]],
            "decision_as_of_trade_date": [decision],
            "target_trade_date": [target],
            "instrument": ["000001.SZ"],
            "selection_effective_rank": [1],
            "prediction_status": ["AVAILABLE"],
        }
    )
    calibration = pd.DataFrame({"arm_id": [SCORE_HMM_ARM_IDS[0]], "target_head": ["PRIMARY"], "brier": [0.2]})
    decisions = pd.DataFrame(
        {
            "arm_id": [SCORE_HMM_ARM_IDS[0]],
            "decision_as_of_trade_date": [decision],
            "target_trade_date": [target],
            "instrument": ["000001.SZ"],
            "parent_rank": [1],
            "action": ["TAKE"],
        }
    )
    policy_daily = pd.DataFrame(
        {"arm_id": ["BASELINE_ALL_TAKE"], "decision_as_of_trade_date": [decision], "net_return_bps": [1.0]}
    )
    policy_episodes = pd.DataFrame({"arm_id": ["BASELINE_ALL_TAKE"], "status": ["EXITED"], "net_return_bps": [1.0]})
    arms = [
        {
            "arm_id": arm_id,
            "status": (
                "EVALUATED"
                if index < (1 if partial else 3)
                else "SOURCE_UNAVAILABLE_NO_POLICY_EVALUATION"
                if index < 3
                else "NOT_RUN_SOURCE_UNAVAILABLE"
            ),
            "eligible": False,
            "reason_codes": ["TEST_NOT_SELECTED"],
        }
        for index, arm_id in enumerate(SCORE_HMM_ARM_IDS)
    ]
    summary = {
        "schema_version": "advisory_score_hmm_arm_summary_v1",
        "request_sha256": request.request_sha256,
        "arms": arms,
        "selected_arm_id": None,
        "eligible_arm_ids": [],
        "candidate_reselection_allowed": False,
        "sealed_holdout_accessed": False,
    }
    evaluation = ScoreHMMEvaluationResult(
        policy_daily=policy_daily,
        policy_episodes=policy_episodes,
        arm_summary=summary,
        selected_arm_id=None,
        eligible_arm_ids=(),
        evidence_class=("AUX_PARTIAL_SOURCE_UNAVAILABLE" if partial else "AUX_EXECUTED_FRONTIER_SELECTED_ZERO"),
    )
    bundle = _publish_bundle(
        request=request,
        source_preflight={"schema_version": "test", "sealed_holdout_accessed": False},
        parent_context_exposure={"schema_version": "test", "name_only_duplicate_detection_used": False},
        feature_schema_by_arm={"schema_version": "test", "arms": []},
        aligned_parent_rankings_top50=oof,
        primary_policy_labels=oof,
        target_coverage=target_coverage,
        fold_receipts={"schema_version": "test", "hmm_folds": [], "model_folds": []},
        oof_predictions=oof,
        calibration_metrics=calibration,
        admission_decisions=decisions,
        policy_daily=policy_daily,
        policy_episodes=policy_episodes,
        arm_summary=summary,
        evaluation=evaluation,
        resource_report={"schema_version": "test", "peak_rss_bytes": 1, "temporary_bytes": 1},
    )
    return bundle, request, route_bytes


def test_bundle_publish_inspect_and_delivery_are_content_addressed_and_restart_safe(tmp_path: Path) -> None:
    bundle, request, route_bytes = _publish_fixture(tmp_path)
    inspected = inspect_score_hmm_admission_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["selected_arm_id"] is None
    assert inspected["source_unavailable_arm_ids"] == list(SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS)
    assert {item.name for item in bundle.iterdir()} == {*SCORE_HMM_BUNDLE_MEMBERS, "manifest.json"}

    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)
    assert first["registry"]["appended_count"] == 5
    assert second["registry"]["appended_count"] == 0
    assert second["registry"]["duplicate_noop_count"] == 5
    assert second["auxiliary_route"]["status"] == "EXACT_NOOP"
    assert Path(request.current_route_path).read_bytes() == route_bytes
    assert first["main_route_unchanged"] is True

    records = json.loads((bundle / "registry_records.json").read_text(encoding="utf-8"))
    assert request.reserved_candidate_indices == (84, 85, 86, 87, 88)
    assert [item["experiment_id"].rsplit(":", 1)[-1] for item in records] == list(SCORE_HMM_ARM_IDS)
    assert [item["generated_trial_count"] for item in records] == [1, 1, 1, 0, 0]
    assert [item["evaluated_trial_count"] for item in records] == [1, 1, 1, 0, 0]
    assert {item["objective_contract"] for item in records} == {"RISK_MANAGED_ADVISORY"}
    assert {item["decision_use"] for item in records} == {"NAVIGATION_ONLY"}


def test_partial_bundle_keeps_generated_and_evaluated_trial_counts_distinct(tmp_path: Path) -> None:
    bundle, _, _ = _publish_fixture(tmp_path, partial=True)
    inspected = inspect_score_hmm_admission_bundle(bundle)
    records = json.loads((bundle / "registry_records.json").read_text(encoding="utf-8"))

    assert inspected["evidence_class"] == "AUX_PARTIAL_SOURCE_UNAVAILABLE"
    assert inspected["next_task"] == "N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW"
    assert inspected["generated_trial_count"] == 3
    assert inspected["evaluated_trial_count"] == 1
    assert [item["generated_trial_count"] for item in records] == [1, 1, 1, 0, 0]
    assert [item["evaluated_trial_count"] for item in records] == [1, 0, 0, 0, 0]


@pytest.mark.parametrize("mutation", ["change", "extra", "missing", "manifest_semantics"])
def test_bundle_tamper_extra_and_partial_members_fail_closed(tmp_path: Path, mutation: str) -> None:
    bundle, _, _ = _publish_fixture(tmp_path)
    if mutation == "change":
        (bundle / "arm_summary.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "extra":
        (bundle / "unexpected.txt").write_text("x", encoding="utf-8")
    elif mutation == "missing":
        (bundle / "arm_summary.json").unlink()
    else:
        manifest_path = bundle / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["activated"] = True
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        inspect_score_hmm_admission_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_BUNDLE_INVALID"


def test_existing_bundle_discovery_does_not_silently_skip_corruption(tmp_path: Path) -> None:
    request = build_test_request(output_root=tmp_path.as_posix())
    corrupt = tmp_path / "score_hmm_admission_bundles" / ("a" * 64)
    corrupt.mkdir(parents=True)
    (corrupt / "manifest.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as caught:
        _find_existing_bundle(request)

    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_BUNDLE_INVALID"


def test_formal_environment_gate_requires_wsl_rdagent_gpu_and_explicit_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline.os, "name", "nt")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        pipeline._require_formal_environment()
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_REQUEST_INVALID"


def test_missing_request_has_typed_failure(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as caught:
        pipeline.run_score_hmm_admission_mve(tmp_path / "missing.json")
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_REQUEST_INVALID"


def test_freeze_cli_uses_declared_canonical_profile_instead_of_ambient_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "canonical.env"
    declared = {
        "TDX_DB_HOST": "canonical-host",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "canonical-db",
        "TDX_DB_USER": "canonical-user",
        "TDX_DB_PASSWORD": "canonical-password-placeholder",
    }
    env_file.write_text("\n".join(f"{key}={value}" for key, value in declared.items()) + "\n", encoding="utf-8")
    for key in declared:
        monkeypatch.setenv(key, "ambient-value")
    observed: dict[str, str] = {}

    def _freeze(*, output_path: str) -> dict[str, object]:
        observed.update({key: pipeline.os.environ[key] for key in declared})
        return {"status": "WRITTEN", "output_path": output_path, "database_write": False}

    monkeypatch.setattr(cli, "freeze_score_hmm_market_pit_snapshot", _freeze)
    output = tmp_path / "pit.json"
    result = cli._run(
        cli._parser().parse_args(
            ["freeze-market-pit", "--env-file", str(env_file), "--output", str(output)]
        )
    )

    assert observed == declared
    assert result["database_target"] == "PRIMARY_CANONICAL_READ_ONLY"
    assert result["database_write"] is False
