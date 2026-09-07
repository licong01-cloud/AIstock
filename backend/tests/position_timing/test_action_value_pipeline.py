from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.position_timing.action_value import ActionValueError, FEATURE_SPEC_SHA256, POLICY_SHA256
from backend.services.position_timing.action_value_data import file_reference
from backend.services.position_timing.action_value_pipeline import (
    RECEIPT_SCHEMA,
    _bundle_manifest,
    _deliver_completed_bundle,
    inspect_bundle,
    publish_serving,
)
from backend.services.position_timing.artifact_store import PositionTimingArtifactStore
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


def _receipt() -> dict:
    payload = {
        "schema_version": RECEIPT_SCHEMA,
        "request_sha256": "1" * 64,
        "repository_commit": "a" * 40,
        "completed_at": "2026-09-07T20:30:00+08:00",
        "source_sha256": "2" * 64,
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "policy_sha256": POLICY_SHA256,
        "continuous_policy": {
            "comparisons": {
                "BUY_AND_HOLD": {"effect_evidence": "INCONCLUSIVE"},
                "FROZEN_L1_V1": {"effect_evidence": "NEGATIVE"},
            }
        },
        "final_model_sha256": "3" * 64,
        "effect_evidence": "INCONCLUSIVE",
    }
    payload["receipt_sha256"] = canonical_sha256(payload)
    return payload


def _supported_receipt() -> dict:
    payload = _receipt()
    payload["continuous_policy"]["comparisons"] = {
        "BUY_AND_HOLD": {"effect_evidence": "SUPPORTED"},
        "FROZEN_L1_V1": {"effect_evidence": "SUPPORTED"},
    }
    payload["effect_evidence"] = "SUPPORTED"
    payload.pop("receipt_sha256")
    payload["receipt_sha256"] = canonical_sha256(payload)
    return payload


def _request(root: Path, historical: Path) -> dict:
    return {
        "request_sha256": "1" * 64,
        "timing_root": root.as_posix(),
        "historical_registry": file_reference(historical),
        "population_spec": {"start": "2020-01-01", "end": "2026-06-30"},
    }


def test_delivery_retry_repairs_own_pointer_without_mutating_global_registry(tmp_path: Path) -> None:
    root = tmp_path / "timing"
    historical = tmp_path / "global-n0.jsonl"
    historical.write_text('{"historical":true}\n', encoding="utf-8")
    global_before = file_reference(historical)
    request = _request(root, historical)
    receipt = _receipt()
    bundle = root / "research" / "action_value_v2" / "bundles" / request["request_sha256"]
    PositionTimingArtifactStore._publish_immutable(
        bundle / "receipt.json", canonical_json_bytes(receipt) + b"\n"
    )

    first = _deliver_completed_bundle(
        request=request,
        bundle=bundle,
        receipt=receipt,
        global_before=global_before,
    )
    registry_path = root / "research_registry" / "timing_trial_registry_v1.jsonl"
    registry_bytes = registry_path.read_bytes()
    current_bytes = (root / "research" / "action_value_v2" / "current.json").read_bytes()
    second = _deliver_completed_bundle(
        request=request,
        bundle=bundle,
        receipt=receipt,
        global_before=global_before,
    )
    assert first["registry"]["appended_count"] == 2
    assert second["registry"]["duplicate_noop_count"] == 2
    assert first["current_research_delivery_status"] == "CURRENT_ADVANCED"
    assert second["current_research_delivery_status"] == "ALREADY_CURRENT"
    assert registry_path.read_bytes() == registry_bytes
    assert (root / "research" / "action_value_v2" / "current.json").read_bytes() == current_bytes
    assert file_reference(historical) == global_before
    records = [json.loads(line) for line in registry_bytes.splitlines()]
    assert {row["study_type"] for row in records} == {"LEARNABILITY_AUDIT"}
    assert sum(row["selected_trial_count"] for row in records) == 0


def test_joint_supported_policy_is_selected_only_once(tmp_path: Path) -> None:
    root = tmp_path / "timing"
    historical = tmp_path / "global-n0.jsonl"
    historical.write_text('{"historical":true}\n', encoding="utf-8")
    request = _request(root, historical)
    receipt = _supported_receipt()
    bundle = root / "research" / "action_value_v2" / "bundles" / request["request_sha256"]
    PositionTimingArtifactStore._publish_immutable(
        bundle / "receipt.json", canonical_json_bytes(receipt) + b"\n"
    )

    _deliver_completed_bundle(
        request=request,
        bundle=bundle,
        receipt=receipt,
        global_before=file_reference(historical),
    )

    records = [
        json.loads(line)
        for line in (root / "research_registry" / "timing_trial_registry_v1.jsonl")
        .read_bytes()
        .splitlines()
    ]
    assert len(records) == 2
    assert sum(row["selected_trial_count"] for row in records) == 1
    selected = next(row for row in records if row["selected_trial_count"] == 1)
    assert selected["unique_variable"] == "BUY_AND_HOLD"


def test_v4_delivery_uses_new_stable_experiment_identity_without_drifting_v2(tmp_path: Path) -> None:
    root = tmp_path / "timing"
    historical = tmp_path / "global-n0.jsonl"
    historical.write_text('{"historical":true}\n', encoding="utf-8")
    global_before = file_reference(historical)

    legacy = _receipt()
    legacy["schema_version"] = "position_timing_action_value_receipt_v2"
    legacy.pop("receipt_sha256")
    legacy["receipt_sha256"] = canonical_sha256(legacy)
    legacy_request = _request(root, historical)
    legacy_bundle = root / "research" / "action_value_v2" / "bundles" / legacy_request["request_sha256"]
    PositionTimingArtifactStore._publish_immutable(
        legacy_bundle / "receipt.json", canonical_json_bytes(legacy) + b"\n"
    )
    _deliver_completed_bundle(
        request=legacy_request,
        bundle=legacy_bundle,
        receipt=legacy,
        global_before=global_before,
    )

    current = _receipt()
    current["request_sha256"] = "4" * 64
    current["source_sha256"] = "5" * 64
    current["completed_at"] = "2026-09-08T20:30:00+08:00"
    current.pop("receipt_sha256")
    current["receipt_sha256"] = canonical_sha256(current)
    current_request = _request(root, historical)
    current_request["request_sha256"] = current["request_sha256"]
    current_bundle = root / "research" / "action_value_v2" / "bundles" / current_request["request_sha256"]
    PositionTimingArtifactStore._publish_immutable(
        current_bundle / "receipt.json", canonical_json_bytes(current) + b"\n"
    )

    result = _deliver_completed_bundle(
        request=current_request,
        bundle=current_bundle,
        receipt=current,
        global_before=global_before,
    )

    assert result["registry"]["appended_count"] == 2
    retry_attempt = _receipt()
    retry_attempt["request_sha256"] = "6" * 64
    retry_attempt["source_sha256"] = current["source_sha256"]
    retry_attempt.pop("receipt_sha256")
    retry_attempt["receipt_sha256"] = canonical_sha256(retry_attempt)
    retry_request = _request(root, historical)
    retry_request["request_sha256"] = retry_attempt["request_sha256"]
    retry_bundle = root / "research" / "action_value_v2" / "bundles" / retry_request["request_sha256"]
    PositionTimingArtifactStore._publish_immutable(
        retry_bundle / "receipt.json", canonical_json_bytes(retry_attempt) + b"\n"
    )
    second_attempt = _deliver_completed_bundle(
        request=retry_request,
        bundle=retry_bundle,
        receipt=retry_attempt,
        global_before=global_before,
    )
    assert second_attempt["registry"]["appended_count"] == 2
    assert result["current_research_delivery_status"] == "CURRENT_ADVANCED"
    assert second_attempt["current_research_delivery_status"] == "RETAINED_NEWER_CURRENT"
    persisted_current = json.loads(
        (root / "research" / "action_value_v2" / "current.json").read_text(encoding="utf-8")
    )
    assert persisted_current["receipt_sha256"] == current["receipt_sha256"]
    records = [
        json.loads(line)
        for line in (root / "research_registry" / "timing_trial_registry_v1.jsonl")
        .read_bytes()
        .splitlines()
    ]
    assert len(records) == 6
    assert {record["research_stage"] for record in records} == {
        "POSITION_TIMING_ACTION_VALUE_V2",
        "POSITION_TIMING_ACTION_VALUE_V4",
    }
    v4_records = [
        record
        for record in records
        if record["research_stage"] == "POSITION_TIMING_ACTION_VALUE_V4"
    ]
    assert {
        tuple(record["parent_lineage"])
        for record in v4_records
    } == {("POSITION_TIMING_ADVICE_V1", "POSITION_TIMING_ACTION_VALUE_V4")}
    assert file_reference(historical) == global_before


def test_bundle_manifest_detects_corruption_and_unsupported_cannot_publish(tmp_path: Path) -> None:
    receipt = _receipt()
    request = {"request_sha256": receipt["request_sha256"]}
    bundle = tmp_path / "bundle"
    PositionTimingArtifactStore._publish_immutable(bundle / "request.json", canonical_json_bytes(request))
    PositionTimingArtifactStore._publish_immutable(bundle / "coverage.json", b"{}")
    for name in (
        "training_rows.parquet",
        "oof_action_predictions.parquet",
        "continuous_sleeve_days.parquet",
        "daily_comparisons.parquet",
    ):
        PositionTimingArtifactStore._publish_immutable(bundle / name, name.encode("ascii"))
    PositionTimingArtifactStore._publish_immutable(bundle / "execution_realism.json", b"{}")
    PositionTimingArtifactStore._publish_immutable(bundle / "receipt.json", canonical_json_bytes(receipt))
    manifest = _bundle_manifest(bundle, receipt)
    PositionTimingArtifactStore._publish_immutable(
        bundle / "manifest.json", canonical_json_bytes(manifest)
    )
    assert inspect_bundle(bundle)["manifest"] == manifest
    with pytest.raises(ActionValueError, match="NOT_SUPPORTED"):
        publish_serving(bundle, timing_root=tmp_path / "timing")
    (bundle / "coverage.json").write_text('{"changed":true}', encoding="utf-8")
    with pytest.raises(ActionValueError, match="MANIFEST_MISMATCH"):
        inspect_bundle(bundle)
