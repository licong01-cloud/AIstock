from __future__ import annotations

import asyncio
import base64
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.quantevolver.saved_prediction_recovery_contract import SavedPredictionRecovery
from backend.routers.quantevolver_evolution import CustomEvoLoopConfig
from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler
from backend.services.quantevolver.saved_prediction_recovery import verify_saved_prediction_recovery


def fixture():
    summary = {"dataset_manifest_sha256": "a" * 64, "profile_sha256": "b" * 64}
    binding = {"release_id": "frozen-release", "day_pins": {"calendar_sha256": "c" * 64}}
    original = {"factor_list": ["f1"], "model_id": "model_lstm_v1", "random_seed": 42,
                "model_params": {"label_horizon": 20, "_qe_active_dataset_summary": summary}}
    root = "mlruns/1/rec/artifacts/"
    files = {"config.json": json.dumps(original).encode(), "conf.yaml": b"frozen-conf",
             "model.py": b"frozen-model", "qe_direct_v2_dataset_binding.json": json.dumps(binding).encode(),
             root + "params.pkl": b"model", root + "pred.pkl": b"prediction", root + "label.pkl": b"label"}
    spec = SavedPredictionRecovery(schema_version="qe_saved_prediction_recovery_v1", source_status="failed",
                                   experiment_id="1", recorder_id="rec", source_seed=42,
                                   manifest_sha256="a" * 64, profile_sha256="b" * 64,
                                   file_sha256={p: hashlib.sha256(b).hexdigest() for p, b in files.items()})
    cfg = ExperimentConfig(factor_names=["f1"], model_id="model_lstm_v1", label_horizon=20,
                           prediction_replay=True, prediction_source_task_id="qe_source",
                           prediction_source_loop_index=1,
                           prediction_source_sha256=spec.file_sha256[root + "pred.pkl"], prediction_recovery=spec,
                           extra_params={"_qe_active_dataset_summary": deepcopy(summary),
                                         "_qe_direct_v2_dataset_binding": deepcopy(binding)})
    client = SimpleNamespace(
        inspect_loop_submission=AsyncMock(return_value=SimpleNamespace(status="failed")),
        list_workspace_files=AsyncMock(return_value={"catalog_completeness": "complete", "files": [
            {"relative_path": p, "size_bytes": len(b)} for p, b in files.items()]}),
        download_workspace_file_bytes=AsyncMock(side_effect=lambda task, loop, path: files[path]),
    )
    return spec, cfg, client, files


def verify(spec, cfg, client, **updates):
    values = dict(spec=spec, client=client, cfg=cfg, task_id="qe_source", loop_index=6,
                  source_task_id="qe_source", source_loop_index=1, target_seed=42)
    values.update(updates)
    return asyncio.run(verify_saved_prediction_recovery(**values))


def test_explicit_recovery_reads_real_bytes_without_mutation_or_deserializing():
    spec, cfg, client, _ = fixture()
    verified = verify(spec, cfg, client)
    receipt = verified.receipt
    assert receipt["source_status_changed"] is False
    assert receipt["fit_calls"] == receipt["model_inference_calls"] == 0
    assert receipt["processor_fingerprint"] == "UNKNOWN"
    assert client.download_workspace_file_bytes.await_count == 7
    source, files = verified.payload(source_task_id="qe_source", source_loop_index=1, source_node_id="wsl2-5080")
    assert source["recovery_receipt"] == receipt
    assert json.loads(files["qe_prediction_replay_source_ref.json"]) == source
    assert base64.b64decode(files["frozen_prediction.pkl.b64"]) == b"prediction"


@pytest.mark.parametrize("mutation,error", [
    (lambda spec: spec.update(recorder_id=".."), "pattern"),
    (lambda spec: spec["file_sha256"].pop("model.py"), "EXACT_PINS"),
    (lambda spec: spec["file_sha256"].update({"../other.pkl": "c" * 64}), "EXACT_PINS"),
    (lambda spec: spec.update(processor_fingerprint="proven"), "UNKNOWN"),
    (lambda spec: spec.update(source_status="running"), "failed"),
    (lambda spec: spec.update(source_seed=True), "integer"),
])
def test_contract_rejects_incomplete_or_unapproved_identity(mutation, error):
    spec, _, _, _ = fixture()
    values = spec.model_dump()
    mutation(values)
    with pytest.raises(ValueError, match=error):
        SavedPredictionRecovery.model_validate(values)


@pytest.mark.parametrize("update,error", [
    ({"prediction_source_sha256": "c" * 64}, "PIN_CONFLICT"),
    ({"prediction_replay": False, "prediction_source_task_id": None,
      "prediction_source_loop_index": None, "prediction_source_sha256": None}, "PURE_REPLAY"),
    ({"backtest_only": True}, "mutually exclusive"),
])
def test_public_contract_fails_closed(update, error):
    _, cfg, _, _ = fixture()
    values = {"factor_keys": ["f1||official"], **{k: cfg.model_dump()[k] for k in [
        "model_id", "prediction_replay", "prediction_source_task_id", "prediction_source_loop_index",
        "prediction_source_sha256", "prediction_recovery"]}}
    values.update(update)
    with pytest.raises(ValueError, match=error):
        CustomEvoLoopConfig(**values)


def test_api_builder_preserves_recovery_without_executable_kwargs():
    spec, cfg, _, _ = fixture()
    loop = CustomEvoLoopConfig(factor_keys=["f1||official"], model_id=cfg.model_id,
                              prediction_replay=True, prediction_source_task_id="qe_source",
                              prediction_source_loop_index=1, prediction_source_sha256=cfg.prediction_source_sha256,
                              prediction_recovery=spec, label_horizon=20)
    values = loop.model_dump()
    values["source_label_horizon"] = 20
    built = build_config_from_custom_evo_loop(values, {}, experiment_name="qe_source/Loop6")
    assert built.prediction_recovery == spec
    assert "prediction_recovery" not in built.build_custom_params()


@pytest.mark.parametrize("status", ["running", "pending", "not_reserved", "unknown", "completed", "cancelled"])
def test_node_must_confirm_exact_terminal_state(status):
    spec, cfg, client, _ = fixture()
    client.inspect_loop_submission.return_value = SimpleNamespace(status=status)
    with pytest.raises(ValueError, match="SOURCE_NOT_TERMINAL"):
        verify(spec, cfg, client)
    client.download_workspace_file_bytes.assert_not_awaited()


@pytest.mark.parametrize("updates", [{"task_id": "qe_other"}, {"loop_index": 1}, {"loop_index": 0}])
def test_recovery_only_appends_within_original_task(updates):
    spec, cfg, client, _ = fixture()
    with pytest.raises(ValueError, match="SAME_TASK_APPEND"):
        verify(spec, cfg, client, **updates)


@pytest.mark.parametrize("seed", [None, True, 2024])
def test_target_cannot_relabel_a_different_seed(seed):
    spec, cfg, client, _ = fixture()
    with pytest.raises(ValueError, match="TARGET_SEED_MISMATCH"):
        verify(spec, cfg, client, target_seed=seed)


def test_source_restart_during_asset_reads_stops_recovery():
    spec, cfg, client, _ = fixture()
    client.inspect_loop_submission.side_effect = [SimpleNamespace(status="failed"), SimpleNamespace(status="running")]
    with pytest.raises(ValueError, match="SOURCE_CHANGED_DURING_READ"):
        verify(spec, cfg, client)


def test_interrupted_catalog_is_not_misrepresented_as_completed():
    spec, cfg, client, _ = fixture()
    client.list_workspace_files.return_value.update(
        catalog_completeness="partial", warnings=["loop_not_terminal:failed", "symlink_not_followed:daily_pv.h5"],
    )
    assert verify(spec, cfg, client).receipt["original_catalog_completeness"] == "partial"


@pytest.mark.parametrize("warnings", [["loop_not_terminal:running"], ["loop_not_terminal:failed", "scan_truncated"],
                                      ["loop_not_terminal:failed", "symlink_not_followed:model.py"]])
def test_partial_catalog_only_accepts_known_interrupted_status_and_unconsumed_links(warnings):
    spec, cfg, client, _ = fixture()
    client.list_workspace_files.return_value.update(catalog_completeness="partial", warnings=warnings)
    with pytest.raises(ValueError, match="CATALOG_INCOMPLETE"):
        verify(spec, cfg, client)


def test_multiple_prediction_recorders_cannot_be_selected_by_hash_guessing():
    spec, cfg, client, _ = fixture()
    client.list_workspace_files.return_value["files"].append(
        {"relative_path": "mlruns/other/run/artifacts/pred.pkl", "size_bytes": 10},
    )
    with pytest.raises(ValueError, match="RECORDER_AMBIGUOUS"):
        verify(spec, cfg, client)


@pytest.mark.parametrize("marker", [{"experiment_id": "1", "recorder_id": "wrong"}, None, "bad-json"])
def test_existing_root_recorder_must_match_pinned_source(marker):
    spec, cfg, client, _ = fixture()
    client.list_workspace_files.return_value["files"].append(
        {"relative_path": "qe_current_recorder.json", "size_bytes": 10},
    )
    client.get_workspace_file = AsyncMock(return_value=marker)
    with pytest.raises(ValueError):
        verify(spec, cfg, client)


def test_root_recorder_changed_after_asset_reads_stops_recovery():
    spec, cfg, client, _ = fixture()
    client.list_workspace_files.return_value["files"].append(
        {"relative_path": "qe_current_recorder.json", "size_bytes": 10},
    )
    client.get_workspace_file = AsyncMock(side_effect=[
        {"experiment_id": "1", "recorder_id": "rec"}, {"experiment_id": "1", "recorder_id": "other"},
    ])
    with pytest.raises(ValueError, match="RECORDER_CHANGED"):
        verify(spec, cfg, client)


@pytest.mark.parametrize("mutation,error", [
    (lambda c: c.update(catalog_completeness="partial"), "CATALOG_INCOMPLETE"),
    (lambda c: c["files"].pop(), "REGULAR_ASSET"),
    (lambda c: c["files"][0].update(is_symlink=True), "REGULAR_ASSET"),
    (lambda c: c["files"][0].update(size_bytes=0), "SIZE_INVALID"),
    (lambda c: c["files"][0].update(size_bytes=5 * 1024 * 1024), "SIZE_INVALID"),
    (lambda c: c["files"].append(c["files"][0]), "DUPLICATE_ASSET"),
])
def test_source_catalog_rejects_missing_links_duplicates_and_unbounded_reads(mutation, error):
    spec, cfg, client, _ = fixture()
    mutation(client.list_workspace_files.return_value)
    with pytest.raises(ValueError, match=error):
        verify(spec, cfg, client)


def test_actual_hash_and_transport_failure_never_fall_back():
    spec, cfg, client, files = fixture()
    files["model.py"] = b"other-bytes"
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        verify(spec, cfg, client)
    client.download_workspace_file_bytes.side_effect = TimeoutError("asset unavailable")
    with pytest.raises(TimeoutError):
        verify(spec, cfg, client)


@pytest.mark.parametrize("field", ["_qe_active_dataset_summary", "_qe_direct_v2_dataset_binding"])
def test_new_dataset_binding_cannot_consume_old_predictions(field):
    spec, cfg, client, _ = fixture()
    cfg.extra_params[field] = {}
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH|BINDING_MISMATCH"):
        verify(spec, cfg, client)


@pytest.mark.parametrize("field,value", [("factor_names", ["different"]), ("model_id", "other"), ("label_horizon", 10),
                                        ("data_split", {"train_end": "2025-01-01"})])
def test_recovery_preserves_prediction_recipe(field, value):
    spec, cfg, client, _ = fixture()
    setattr(cfg, field, value)
    with pytest.raises(ValueError, match="RECIPE_MISMATCH"):
        verify(spec, cfg, client)


def test_default_source_gate_stays_completed_only_and_recovery_requires_exact_db_status():
    spec, _, _, _ = fixture()
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"status": "failed", "loop_node_id": "wsl2-5080"}
    with patch("backend.services.quantevolver.qe_evolution_service.get_conn") as get_conn:
        get_conn.return_value.__enter__.return_value = conn
        scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
        with pytest.raises(ValueError, match="SOURCE_NOT_COMPLETED"):
            scheduler._get_prediction_replay_source_loop("qe_source", 1)
        assert scheduler._get_prediction_replay_source_loop("qe_source", 1, recovery=spec)["status"] == "failed"
        cursor.fetchone.return_value["status"] = "running"
        with pytest.raises(ValueError, match="SOURCE_NOT_COMPLETED"):
            scheduler._get_prediction_replay_source_loop("qe_source", 1, recovery=spec)
