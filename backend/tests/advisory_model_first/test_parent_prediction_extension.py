from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first import research_control as control
from backend.services.advisory_model_first.research_control_contracts import (
    ParentPredictionExtensionStatus,
)
from backend.services.advisory_model_first.target_binding import (
    EXPECTED_RUNTIME_SEMANTICS_HASH,
    LEG_IDS,
    MANIFEST_SHA256,
    PACKAGE_ID,
    REPRESENTATIVE_SEED_RUN_IDS,
)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


class _FakePredictionSource:
    descriptors: dict[str, PredictionArtifactDescriptor] = {}

    def __init__(self, _root: str | Path) -> None:
        pass

    def describe(self, run_id: str) -> PredictionArtifactDescriptor:
        return self.descriptors[run_id]


def _json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_fixture(tmp_path: Path, monkeypatch) -> dict[str, Path]:
    runtime_root = tmp_path / "runtime"
    _FakePredictionSource.descriptors = {}
    for index, leg_id in enumerate(LEG_IDS):
        run_id = REPRESENTATIVE_SEED_RUN_IDS[leg_id]
        _FakePredictionSource.descriptors[run_id] = PredictionArtifactDescriptor(
            run_id=run_id,
            run_key=run_id,
            artifact_uri=f"F:/prediction/{run_id}/pred.pkl",
            artifact_sha256=("d" if index == 0 else "e") * 64,
            size_bytes=100 + index,
            row_count=1000 + index,
            date_start="2024-07-04",
            date_end="2026-03-10" if index == 0 else "2026-04-28",
        )
        leg_root = runtime_root / f"{MANIFEST_SHA256[:16]}__leg_{leg_id}"
        model_path = leg_root / "model" / "params.pkl"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_bytes(f"model-{leg_id}".encode())
        monkeypatch.setitem(control.REPRESENTATIVE_MODEL_ASSET_SHA256, leg_id, _sha(model_path))
        (leg_root / "factor_entry.py").write_text("FACTORS = ['f1']\n", encoding="utf-8")
        _json(
            leg_root / "factor_order.json",
            {
                "package_id": PACKAGE_ID,
                "source": "live_qe_model_inference_v1",
                "total_factors": 1,
                "factor_order": ["f1"],
                "is_aligned": True,
            },
        )
        _json(
            leg_root / "manifest.json",
            {
                "schema_version": 1,
                "task_id": PACKAGE_ID,
                "source": "live_qe_model_inference_v1",
                "assets": {
                    "model_weight": "model/params.pkl",
                    "factor_entry": "factor_entry.py",
                    "factor_order": "factor_order.json",
                    "factors_count": 1,
                },
                "diagnostics": {
                    "package_id": PACKAGE_ID,
                    "package_manifest_sha256": MANIFEST_SHA256,
                },
            },
        )
    monkeypatch.setattr(control, "ExactPredictionSource", _FakePredictionSource)

    evidence_path = tmp_path / "model-challenger" / "artifact.json"
    artifact = {
        "schema_version": "advisory_historical_model_challenger_artifact_v1",
        "producer_contract_version": "advisory_historical_model_challenger_v1",
        "artifact_hash": HASH_A,
        "bundle_id": HASH_A,
        "parent_range_run_id": "range-run",
        "package_id": PACKAGE_ID,
        "manifest_sha256": MANIFEST_SHA256,
        "selection_runtime_semantics_hash": EXPECTED_RUNTIME_SEMANTICS_HASH,
        "decision_trade_date": "2026-05-20",
        "target_trade_date": "2026-05-21",
        "candidate_count": 1,
        "candidates": [{"symbol": "000001.SZ"}],
        "parent_candidate_artifact_hash": HASH_B,
        "parent_candidate_set_hash": HASH_C,
    }
    _json(evidence_path, artifact)
    state_path = tmp_path / "state.json"
    _json(
        state_path,
        {
            "schema_version": "advisory_historical_model_challenger_state_v2",
            "status": "COMPLETED",
            "bundle_id": HASH_A,
            "parent_range_run_id": "range-run",
            "days": {
                "2026-05-20": {
                    "status": "COMPLETE",
                    "candidate_count": 1,
                    "target_trade_date": "2026-05-21",
                    "duration_seconds": 12.5,
                    "parent_candidate_artifact_hash": HASH_B,
                    "artifact_ref": {
                        "artifact_hash": HASH_A,
                        "file_sha256": _sha(evidence_path),
                        "relative_path": f"model-challenger/{evidence_path.name}",
                    },
                }
            },
        },
    )
    return {
        "runtime_root": runtime_root,
        "evidence_path": evidence_path,
        "state_path": state_path,
    }


def _run(paths: dict[str, Path], **overrides):
    values = {
        "prediction_store_root": "F:/prediction-store",
        "runtime_asset_root": paths["runtime_root"],
        "post_cutoff_evidence_path": paths["evidence_path"],
        "comparison_state_path": paths["state_path"],
        "target_extension_start": "2026-03-11",
        "target_extension_end": "2026-06-30",
    }
    values.update(overrides)
    return control.inspect_parent_prediction_extension(**values)


def test_complete_assets_and_post_cutoff_execution_prove_frozen_model_can_infer(
    tmp_path, monkeypatch
):
    paths = _build_fixture(tmp_path, monkeypatch)

    receipt = _run(paths)

    assert receipt.status == ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER
    assert receipt.common_historical_prediction_cutoff.isoformat() == "2026-03-10"
    assert all(item.runtime_ready for item in receipt.legs)
    assert receipt.post_cutoff_evidence.candidate_count == 1


def test_missing_runtime_asset_downgrades_without_extrapolating_predictions(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    leg_root = paths["runtime_root"] / f"{MANIFEST_SHA256[:16]}__leg_{LEG_IDS[0]}"
    (leg_root / "factor_entry.py").unlink()

    receipt = _run(paths)

    assert receipt.status == ParentPredictionExtensionStatus.HISTORICAL_PREDICTION_ONLY
    assert any("factor_entry" in item for item in receipt.capability_gaps)


def test_runtime_identity_poison_is_a_typed_failure(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    leg_root = paths["runtime_root"] / f"{MANIFEST_SHA256[:16]}__leg_{LEG_IDS[0]}"
    manifest_path = leg_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["diagnostics"]["package_id"] = "different-package"
    _json(manifest_path, manifest)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _run(paths)
    assert captured.value.reason_code == "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH"


def test_prediction_descriptor_run_identity_poison_is_a_typed_failure(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    leg_id = LEG_IDS[0]
    expected_run_id = REPRESENTATIVE_SEED_RUN_IDS[leg_id]
    descriptor = _FakePredictionSource.descriptors[expected_run_id]
    _FakePredictionSource.descriptors[expected_run_id] = descriptor.model_copy(
        update={"run_id": "different-run"}
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _run(paths)
    assert captured.value.reason_code == "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH"


def test_runtime_model_hash_poison_is_a_typed_failure(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    leg_root = paths["runtime_root"] / f"{MANIFEST_SHA256[:16]}__leg_{LEG_IDS[0]}"
    (leg_root / "model" / "params.pkl").write_bytes(b"mutated-model")

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _run(paths)
    assert captured.value.reason_code == "ADVISORY_PARENT_RUNTIME_ASSET_INVALID"


def test_post_cutoff_target_identity_poison_is_a_typed_failure(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    artifact = json.loads(paths["evidence_path"].read_text(encoding="utf-8"))
    artifact["selection_runtime_semantics_hash"] = "f" * 64
    _json(paths["evidence_path"], artifact)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _run(paths)
    assert captured.value.reason_code == "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH"


def test_empty_or_pre_cutoff_parent_evidence_is_rejected(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    artifact = json.loads(paths["evidence_path"].read_text(encoding="utf-8"))
    artifact.update(candidate_count=0, candidates=[])
    _json(paths["evidence_path"], artifact)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _run(paths)
    assert captured.value.reason_code == "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID"


def test_retrain_state_requires_explicit_exact_typed_receipt(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    retrain = tmp_path / "retrain.json"
    _json(
        retrain,
        {
            "schema_version": "advisory_parent_retrain_requirement_v1",
            "status": "RETRAIN_NEW_LINEAGE_REQUIRED",
            "package_id": PACKAGE_ID,
            "manifest_sha256": MANIFEST_SHA256,
            "runtime_semantics_hash": EXPECTED_RUNTIME_SEMANTICS_HASH,
            "target_extension_start": "2026-03-11",
            "target_extension_end": "2026-06-30",
            "reason_code": "FROZEN_SCHEMA_INCOMPATIBLE_WITH_EXTENSION",
        },
    )

    receipt = _run(paths, retrain_receipt_path=retrain)
    assert receipt.status == ParentPredictionExtensionStatus.RETRAIN_NEW_LINEAGE_REQUIRED
    assert receipt.explicit_retrain_ref is not None


def test_post_cutoff_proof_must_fall_inside_requested_extension_range(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)

    with pytest.raises(ValidationError, match="outside the requested extension range"):
        _run(paths, target_extension_start="2026-06-01")


def test_parent_spike_exact_retry_preserves_the_first_immutable_receipt(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path, monkeypatch)
    output = tmp_path / "receipt.json"

    first = _run(paths, output_path=output)
    first_bytes = output.read_bytes()
    second = _run(paths, output_path=output)

    assert first.receipt_sha256 == second.receipt_sha256
    assert first.created_at == second.created_at
    assert output.read_bytes() == first_bytes
