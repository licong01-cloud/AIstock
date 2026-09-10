from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first import financial_event_information_set_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.financial_event_information_set_pipeline import (
    _deliver_bundle,
    _publish_bundle,
    inspect_financial_event_information_set_bundle,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_financial_event_information_set_contracts import (
    build_test_request,
)


def _publish_fixture(tmp_path: Path) -> tuple[Path, object]:
    request = build_test_request(
        output_root=tmp_path.as_posix(),
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        route_path=(tmp_path / "current_route.md").as_posix(),
    )
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2025-01-02")],
            "instrument": ["000001.SZ"],
            "parent_rank_pct": [1.0],
        }
    )
    coverage = pd.DataFrame({"decision_as_of_trade_date": [pd.Timestamp("2025-01-02")], "instrument_count": [1]})
    oof = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2025-01-02")],
            "instrument": ["000001.SZ"],
            "signed_candidate_oof_score": [1.0],
        }
    )
    diagnostics = pd.DataFrame({"trial_id": ["EVENT_SIGNED_CONTENT_V1"], "path_id": ["p0"]})
    daily = pd.DataFrame({"decision_as_of_trade_date": [pd.Timestamp("2025-01-02")], "candidate_rank_ic": [0.1]})
    summary = {
        "schema_version": "advisory_n3_financial_event_model_summary_v1",
        "request_sha256": request.request_sha256,
        "support_sufficient": True,
        "evidence_class": "EXPLORATORY_NOT_SELECTED_NON_VINTAGE",
        "selected_trial_id": None,
    }
    stability = {
        "schema_version": "advisory_n3_financial_event_stability_v1",
        "request_sha256": request.request_sha256,
        "positive_joint_time_block_count": 0,
        "sealed_holdout_accessed": False,
    }
    stability["stability_sha256"] = canonical_json_sha256(stability)
    frontier = {
        "schema_version": "advisory_n3_financial_event_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": [],
        "selected_trial_id": None,
        "selected_trial_count": 0,
        "support_sufficient": True,
        "evidence_class": "EXPLORATORY_NOT_SELECTED_NON_VINTAGE",
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "sealed_holdout_accessed": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    bundle = _publish_bundle(
        request=request,
        features=features,
        coverage_daily=coverage,
        oof_scores=oof,
        fold_diagnostics=diagnostics,
        daily_metrics=daily,
        model_summary=summary,
        stability=stability,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return bundle, request


def test_bundle_publish_inspect_and_delivery_are_restart_safe(tmp_path: Path) -> None:
    bundle, request = _publish_fixture(tmp_path)
    inspected = inspect_financial_event_information_set_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_id"] is None
    assert inspected["next_task"] == "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)
    assert first["registry"]["appended_count"] == 3
    assert second["registry"]["appended_count"] == 0
    assert second["registry"]["duplicate_noop_count"] == 3
    assert second["route"]["status"] == "EXACT_NOOP"


@pytest.mark.parametrize("mutation", ["change", "extra", "missing"])
def test_bundle_mutation_extra_and_partial_are_rejected(tmp_path: Path, mutation: str) -> None:
    bundle, _ = _publish_fixture(tmp_path)
    if mutation == "change":
        (bundle / "model_summary.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "extra":
        (bundle / "unexpected.txt").write_text("x", encoding="utf-8")
    else:
        (bundle / "model_summary.json").unlink()
    with pytest.raises(AdvisoryModelFirstError) as caught:
        inspect_financial_event_information_set_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID"


def test_prepare_and_run_environment_gate_requires_wsl_formal_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline.os, "name", "nt")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        pipeline._require_formal_environment()
    assert caught.value.reason_code == "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID"


def test_missing_request_has_typed_failure(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as caught:
        pipeline.run_financial_event_information_set_mve(tmp_path / "missing.json")
    assert caught.value.reason_code == "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID"


def test_registry_records_are_three_distinct_non_activation_trials(tmp_path: Path) -> None:
    bundle, _ = _publish_fixture(tmp_path)
    records = json.loads((bundle / "registry_records.json").read_text(encoding="utf-8"))
    assert len(records) == 3
    assert len({item["experiment_id"] for item in records}) == 3
    assert {item["study_type"] for item in records} == {"LEARNABILITY_AUDIT"}
    assert {item["decision_use"] for item in records} == {"NAVIGATION_ONLY"}
    assert sum(item["selected_trial_count"] for item in records) == 0
