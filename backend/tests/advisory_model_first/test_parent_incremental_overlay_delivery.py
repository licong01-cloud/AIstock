from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.parent_incremental_overlay_contracts import (
    PARENT_OVERLAY_CANDIDATES,
    build_default_overlay_trials,
)
from backend.services.advisory_model_first.parent_incremental_overlay_pipeline import (
    _deliver_bundle,
    _find_existing_bundle,
    _publish_bundle,
    _validate_parent_navigation_source,
    inspect_parent_incremental_overlay_bundle,
    prepare_parent_overlay_request,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_parent_incremental_overlay_contracts import (
    make_parent_overlay_request,
)
from scripts.advisory_parent_incremental_overlay_run import main


def _published_bundle(tmp_path):
    request = make_parent_overlay_request(tmp_path)
    trials = build_default_overlay_trials()
    score_panel = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
            "parent_rank": [0.5],
            **{trial.trial_id: [0.5] for trial in trials},
        }
    )
    daily = pd.DataFrame(
        {
            "trial_id": [trials[0].trial_id],
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "rank_ic_delta": [0.0],
            "top5_lift_bps": [0.0],
        }
    )
    summary = {
        "schema_version": "advisory_parent_incremental_overlay_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": 24,
        "trials": [],
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
        "position_weight_output": False,
    }
    frontier = {
        "schema_version": "advisory_parent_incremental_overlay_frontier_v1",
        "request_sha256": request.request_sha256,
        "selection_rule": "TEST_FROZEN_NONE",
        "eligible_trial_ids": [],
        "selected_trial_id": None,
        "selected_trial_count": 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
        "position_weight_output": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    parent_loaded = {"record": SimpleNamespace(record_sha256="9" * 64)}
    bundle = _publish_bundle(
        request=request,
        parent_loaded=parent_loaded,
        score_panel=score_panel,
        daily_metrics=daily,
        overlay_summary=summary,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return request, bundle


def test_publish_inspect_delivery_and_exact_registry_noop(tmp_path) -> None:
    request, bundle = _published_bundle(tmp_path)

    inspected = inspect_parent_incremental_overlay_bundle(bundle)
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)

    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_count"] == 0
    assert inspected["position_weight_output"] is False
    assert _find_existing_bundle(request) == bundle
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert first["route"]["route_sha256"] == second["route"]["route_sha256"]


def test_bundle_mutation_fails_closed(tmp_path) -> None:
    _, bundle = _published_bundle(tmp_path)
    pd.DataFrame({"corrupted": [True]}).to_parquet(bundle / "daily_metrics.parquet", index=False)

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_parent_incremental_overlay_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID"


def test_partial_bundle_fails_with_typed_error(tmp_path) -> None:
    _, bundle = _published_bundle(tmp_path)
    (bundle / "request.json").unlink()

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_parent_incremental_overlay_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID"


def test_parent_navigation_source_requires_exact_selected_zero_roster(tmp_path) -> None:
    parent = tmp_path / ("a" * 64)
    parent.mkdir()
    trials = build_default_overlay_trials()
    candidate_hashes = {item.candidate_id: item.source_expression_sha256 for item in trials}
    proposals = [
        {
            "proposal_id": candidate,
            "expression_sha256": candidate_hashes[candidate],
            "familywise_rank_ic_lower": 0.01,
            "parent_score_spearman_mean": 0.1,
        }
        for candidate in PARENT_OVERLAY_CANDIDATES
    ]
    proposals.extend(
        {
            "proposal_id": f"N3_UNUSED_{index:02d}",
            "expression_sha256": f"{index + 1:064x}",
            "familywise_rank_ic_lower": -0.01,
            "parent_score_spearman_mean": 0.1,
        }
        for index in range(18)
    )
    (parent / "proposal_summary.json").write_text(
        __import__("json").dumps(
            {
                "trial_count": 24,
                "proposals": proposals,
                "decision_use": "NAVIGATION_ONLY",
                "sealed_holdout_accessed": False,
                "deployable": False,
            }
        ),
        encoding="utf-8",
    )
    (parent / "frontier_receipt.json").write_text(
        '{"selected_proposal_id":null,"selected_trial_count":0}',
        encoding="utf-8",
    )
    loaded = {
        "manifest": {
            "study_type": "EXPLORATORY_SCREEN",
            "decision_use": "NAVIGATION_ONLY",
            "sealed_holdout_accessed": False,
            "deployable": False,
            "runtime_eligible": False,
        },
        "receipt": SimpleNamespace(selected_trial_count=0, selected_proposal_id=None),
        "record": SimpleNamespace(
            experiment_id="ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1",
            evaluated_trial_count=24,
        ),
    }

    _validate_parent_navigation_source(parent, loaded)
    loaded["receipt"].selected_trial_count = 1
    with pytest.raises(AdvisoryModelFirstError):
        _validate_parent_navigation_source(parent, loaded)


def test_cli_argument_failure_is_typed(capsys) -> None:
    assert main(["prepare"]) == 1
    assert "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID" in capsys.readouterr().out


def test_prepare_rejects_clean_but_unmerged_head(monkeypatch, tmp_path) -> None:
    parent = tmp_path / ("a" * 64)
    parent.mkdir()
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._read_parent_bundle",
        lambda path: {},
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._validate_parent_navigation_source",
        lambda path, loaded: None,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._cross_os_git_dirty_paths",
        lambda path: [],
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._cross_os_git_commit",
        lambda path: "1" * 40,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._git_origin_main_commit",
        lambda path: "2" * 40,
    )

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        prepare_parent_overlay_request(
            parent_bundle_path=parent,
            repository_root=tmp_path,
            output_root=tmp_path / "out",
            output_path=tmp_path / "request.json",
        )
    assert exc_info.value.reason_code == "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID"
