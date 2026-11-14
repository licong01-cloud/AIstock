"""Read-only admission of one frozen interrupted-run prediction; never changes its status."""

from __future__ import annotations

import hashlib
import json
import base64
from dataclasses import dataclass

from .saved_prediction_recovery_contract import SavedPredictionRecovery


@dataclass(frozen=True)
class VerifiedSavedPrediction:
    receipt: dict
    prediction_bytes: bytes
    catalog_path: str

    def payload(self, *, source_task_id: str, source_loop_index: int, source_node_id: str) -> tuple[dict, dict]:
        ref = {"schema_version": "qe_prediction_replay_source_ref_v1", "mode": "prediction_replay",
               "source_task_id": source_task_id, "source_loop_index": source_loop_index,
               "source_node_id": source_node_id, "catalog_path": self.catalog_path,
               "sha256": hashlib.sha256(self.prediction_bytes).hexdigest(), "size_bytes": len(self.prediction_bytes),
               "recovery_receipt": self.receipt}
        return ref, {"frozen_prediction.pkl.b64": base64.b64encode(self.prediction_bytes).decode("ascii"),
                     "qe_prediction_replay_source_ref.json": json.dumps(ref, ensure_ascii=False, indent=2)}


async def verify_saved_prediction_recovery(
    spec: SavedPredictionRecovery, *, client, task_id: str, loop_index: int,
    source_task_id: str, source_loop_index: int, cfg, target_seed: int,
) -> VerifiedSavedPrediction:
    if task_id != source_task_id or loop_index <= source_loop_index:
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_REQUIRES_SAME_TASK_APPEND")
    if isinstance(target_seed, bool) or target_seed != spec.source_seed:
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_TARGET_SEED_MISMATCH")
    submission = await client.inspect_loop_submission(source_task_id, f"Loop{source_loop_index}")
    if submission.status != spec.source_status:
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_SOURCE_NOT_TERMINAL")
    catalog = await client.list_workspace_files(source_task_id, f"Loop{source_loop_index}")
    completeness = catalog.get("catalog_completeness")
    if completeness == "partial":
        warnings = catalog.get("warnings")
        terminal_warning = "loop_not_terminal:" + spec.source_status
        if not isinstance(warnings, list) or terminal_warning not in warnings or any(
            not isinstance(warning, str) or (warning != terminal_warning and (
                not warning.startswith("symlink_not_followed:")
                or warning.removeprefix("symlink_not_followed:") in spec.file_sha256
            )) for warning in warnings
        ):
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_CATALOG_INCOMPLETE")
    elif completeness != "complete":
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_CATALOG_INCOMPLETE")
    rows = catalog.get("files", catalog.get("assets"))
    if not isinstance(rows, list):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_CATALOG_INVALID")
    entries = {}
    predictions = []
    marker_present = False
    for row in rows:
        path = str(row.get("relative_path") or row.get("path") or row.get("filename") or "")
        path = path.replace("\\", "/").removeprefix("./")
        if path.endswith("/artifacts/pred.pkl") or path == "artifacts/pred.pkl":
            predictions.append(path)
        if path == "qe_current_recorder.json":
            if marker_present or row.get("is_symlink") or str(row.get("file_type", "")).lower() in {"directory", "symlink", "junction"}:
                raise ValueError("QE_SAVED_PREDICTION_RECOVERY_RECORDER_INVALID")
            marker_present = True
        if path in spec.file_sha256:
            if path in entries:
                raise ValueError("QE_SAVED_PREDICTION_RECOVERY_DUPLICATE_ASSET")
            entries[path] = row
    if predictions != [spec.artifact_root + "pred.pkl"]:
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_RECORDER_AMBIGUOUS")
    if marker_present:
        marker = await client.get_workspace_file(source_task_id, f"Loop{source_loop_index}", "qe_current_recorder.json")
        marker = json.loads(marker) if isinstance(marker, str) else marker
        if not isinstance(marker, dict) or marker.get("experiment_id") != spec.experiment_id or marker.get("recorder_id") != spec.recorder_id:
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_RECORDER_INVALID")
    documents = {}
    prediction_bytes = None
    for path, expected in spec.file_sha256.items():
        entry = entries.get(path)
        if not entry or entry.get("is_symlink") or str(entry.get("file_type", "")).lower() in {
            "directory", "symlink", "junction",
        }:
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_REGULAR_ASSET_REQUIRED")
        size = entry.get("size_bytes")
        limit = 256 * 1024 * 1024 if path.endswith(".pkl") else 4 * 1024 * 1024
        if isinstance(size, bool) or not isinstance(size, int) or not 0 < size <= limit:
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_ASSET_SIZE_INVALID")
        raw = await client.download_workspace_file_bytes(source_task_id, f"Loop{source_loop_index}", path)
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_ASSET_HASH_MISMATCH")
        if path in {"config.json", "qe_direct_v2_dataset_binding.json"}:
            documents[path] = json.loads(raw)
        if path == spec.artifact_root + "pred.pkl":
            prediction_bytes = raw
        del raw
    original = documents["config.json"]
    params = original.get("model_params") or {}
    summary = params.get("_qe_active_dataset_summary") or {}
    target = cfg.build_custom_params()
    expected_identity = {"dataset_manifest_sha256": spec.manifest_sha256, "profile_sha256": spec.profile_sha256}
    if any(summary.get(k) != v or (target.get("_qe_active_dataset_summary") or {}).get(k) != v
           for k, v in expected_identity.items()):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_DATA_IDENTITY_MISMATCH")
    if documents["qe_direct_v2_dataset_binding.json"] != target.get("_qe_direct_v2_dataset_binding"):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_BINDING_MISMATCH")
    if (original.get("factor_list") != cfg.factor_names or original.get("model_id") != cfg.model_id
            or params.get("label_horizon") != cfg.label_horizon
            or original.get("random_seed") != spec.source_seed
            or (original.get("data_split") or {}) != (cfg.data_split or {})):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_RECIPE_MISMATCH")
    if marker_present:
        latest_marker = await client.get_workspace_file(source_task_id, f"Loop{source_loop_index}", "qe_current_recorder.json")
        latest_marker = json.loads(latest_marker) if isinstance(latest_marker, str) else latest_marker
        if marker != latest_marker:
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_RECORDER_CHANGED")
    latest = await client.inspect_loop_submission(source_task_id, f"Loop{source_loop_index}")
    identity_fields = ("status", "pid", "submission_intent_hash", "start_time_ticks")
    if any(getattr(submission, key, None) != getattr(latest, key, None) for key in identity_fields):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_SOURCE_CHANGED_DURING_READ")
    receipt = {
        "schema_version": spec.schema_version, "original_source_status": spec.source_status,
        "original_source_task_id": source_task_id, "original_source_loop_index": source_loop_index,
        "file_sha256": spec.file_sha256, "manifest_sha256": spec.manifest_sha256,
        "profile_sha256": spec.profile_sha256, "processor_fingerprint": spec.processor_fingerprint,
        "fit_calls": 0, "model_inference_calls": 0, "source_status_changed": False,
        "original_catalog_completeness": completeness, "admission_scope": "EXACT_SEVEN_PINNED_FILES",
    }
    assert prediction_bytes is not None
    return VerifiedSavedPrediction(receipt, prediction_bytes, spec.artifact_root + "pred.pkl")
