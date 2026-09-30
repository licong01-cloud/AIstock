from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.services.advisory_model_first.parent_incremental_overlay_contracts import (
    PARENT_OVERLAY_CANDIDATES,
    PARENT_OVERLAY_WEIGHT_BPS,
    FrozenParentIncrementalOverlayRequestV1,
    build_default_overlay_trials,
    build_parent_overlay_receipt,
    build_parent_overlay_request,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file


def make_parent_overlay_request(tmp_path: Path, **overrides: object) -> FrozenParentIncrementalOverlayRequestV1:
    bundle_id = "a" * 64
    parent = tmp_path / bundle_id
    parent.mkdir(parents=True, exist_ok=True)
    role_files = {
        "n3_parent_overlay_parent_frontier": "frontier_receipt.json",
        "n3_parent_overlay_parent_manifest": "manifest.json",
        "n3_parent_overlay_parent_proposal_summary": "proposal_summary.json",
        "n3_parent_overlay_parent_score_panel": "score_panel.parquet",
    }
    for name in role_files.values():
        (parent / name).write_bytes(name.encode("utf-8"))
    values: dict[str, object] = {
        "evidence_refs": tuple(
            evidence_reference_for_file(parent / name, role=role) for role, name in role_files.items()
        ),
        "parent_bundle_path": parent.as_posix(),
        "parent_bundle_id": bundle_id,
        "parent_request_sha256": "b" * 64,
        "parent_receipt_sha256": "c" * 64,
        "parent_frontier_sha256": "d" * 64,
        "dataset_identity": "e" * 64,
        "policy_identity": "f" * 64,
        "registry_path": (tmp_path / "trial_registry.jsonl").as_posix(),
        "route_path": (tmp_path / "current_route.md").as_posix(),
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "1" * 40,
        "output_root": (tmp_path / "output").as_posix(),
        "created_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return build_parent_overlay_request(**values)


def test_default_roster_is_exact_six_by_four() -> None:
    trials = build_default_overlay_trials()

    assert len(trials) == 24
    assert tuple(dict.fromkeys(item.candidate_id for item in trials)) == PARENT_OVERLAY_CANDIDATES
    assert {item.weight_bps for item in trials} == set(PARENT_OVERLAY_WEIGHT_BPS)
    assert all(item.weight > 0 for item in trials)
    assert len({item.trial_id for item in trials}) == 24


def test_request_identity_and_extra_fields_fail_closed(tmp_path: Path) -> None:
    request = make_parent_overlay_request(tmp_path)
    payload = request.model_dump(mode="json")
    payload["unknown"] = True

    with pytest.raises(ValueError):
        FrozenParentIncrementalOverlayRequestV1.model_validate(payload)

    payload.pop("unknown")
    payload["request_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="identity mismatch"):
        FrozenParentIncrementalOverlayRequestV1.model_validate(payload)


def test_request_rejects_weight_or_candidate_roster_drift(tmp_path: Path) -> None:
    request = make_parent_overlay_request(tmp_path)
    payload = request.model_dump(mode="json")
    payload["trials"] = payload["trials"][:-1]

    with pytest.raises(ValueError):
        build_parent_overlay_request(
            **{key: value for key, value in payload.items() if key not in {"request_id", "request_sha256"}}
        )


def test_receipt_selection_and_next_task_are_relational() -> None:
    trial_id = build_default_overlay_trials()[0].trial_id
    receipt = build_parent_overlay_receipt(
        request_sha256="a" * 64,
        selected_trial_count=1,
        selected_trial_id=trial_id,
        eligible_trial_ids=(trial_id,),
        next_task="N3_PARENT_OVERLAY_CONFIRMATION_DESIGN",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
    )
    assert receipt.selected_trial_count == 1

    with pytest.raises(ValueError, match="selection/next-task"):
        build_parent_overlay_receipt(
            request_sha256="a" * 64,
            selected_trial_count=1,
            selected_trial_id=trial_id,
            eligible_trial_ids=(trial_id,),
            next_task="N3_ALPHA_INFORMATION_SET_EXPANSION_MVE",
            source_identity_sha256="b" * 64,
            result_files_sha256="c" * 64,
            resource_report_sha256="d" * 64,
        )
