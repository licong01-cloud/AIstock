from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.model_store.artifact_store import (
    PredictionArtifactStore,
    PredictionStoreError,
    PredictionStoreNotFound,
    validate_store_root,
)
from backend.services.qe_archive.payload_extractor import QEArchivePayloadExtractor


def test_prediction_artifact_store_writes_manifest_and_rejects_hdd_root(tmp_path: Path) -> None:
    with pytest.raises(PredictionStoreError, match="must not use E:"):
        validate_store_root(Path("E:/prediction_store"))

    pred = pd.DataFrame(
        {"score": [0.2, 0.1, 0.3]},
        index=pd.MultiIndex.from_product(
            [[pd.Timestamp("2026-01-02")], ["000001.SZ", "000002.SZ", "000003.SZ"]],
            names=["datetime", "instrument"],
        ),
    )
    pred_bytes = io.BytesIO()
    pred.to_pickle(pred_bytes)
    pred_bytes.seek(0)
    label = pd.DataFrame(
        {"LABEL0": [0.01, -0.02, 0.03]},
        index=pred.index,
    )
    label_bytes = io.BytesIO()
    label.to_pickle(label_bytes)
    label_bytes.seek(0)
    params_bytes = io.BytesIO(b"params")

    store = PredictionArtifactStore(root=tmp_path / "prediction_store")
    manifest = store.write_artifacts(
        run_key="qear_run_unit",
        files={"prediction": ("pred.pkl", pred_bytes), "model_params": ("params.pkl", params_bytes), "label": ("label.pkl", label_bytes)},
        metadata={"experiment_id": "exp_unit", "recorder_id": "rec_unit", "source_node_id": "wsl2-5080"},
    )

    assert manifest["mlflow_artifact_uri"] == "aistock-prediction-store://runs/qear_run_unit"
    items = {item["artifact_type"]: item for item in manifest["artifacts"]}
    assert set(items) == {"prediction", "model_params", "label"}
    assert items["prediction"]["row_count"] == 3
    assert items["prediction"]["symbol_count"] == 3
    assert items["prediction"]["parser_status"] == "parsed"
    assert items["label"]["artifact_name"] == "label.pkl"
    assert items["label"]["row_count"] == 3
    assert items["label"]["parser_status"] == "parsed"
    assert store.resolve_artifact_path(manifest["mlflow_artifact_uri"], artifact_type="prediction").exists()
    assert store.resolve_artifact_path(manifest["mlflow_artifact_uri"], artifact_type="model_params").exists()
    assert store.resolve_artifact_path(manifest["mlflow_artifact_uri"], artifact_type="label").exists()
    assert store.resolve_artifact_path(manifest["mlflow_artifact_uri"], artifact_name="label.pkl").exists()


def test_payload_extractor_attaches_prediction_store_manifest() -> None:
    manifest = {
        "schema_version": "aistock_prediction_store_manifest_v1",
        "uri": "aistock-prediction-store://runs/run_unit",
        "mlflow_artifact_uri": "aistock-prediction-store://runs/run_unit",
        "metadata": {"recorder_id": "rec_unit", "recorder_experiment_id": "exp_rec"},
        "artifacts": [
            {
                "artifact_type": "prediction",
                "artifact_name": "pred.pkl",
                "uri": "aistock-prediction-store://runs/run_unit/prediction",
                "sha256": "a" * 64,
                "size_bytes": 128,
                "row_count": 10,
                "symbol_count": 4,
                "date_start": "2026-01-02",
                "date_end": "2026-01-03",
                "parser_status": "parsed",
                "metadata": {"blob_rel_path": "blobs/aa/" + "a" * 64},
            }
        ],
    }
    extracted = QEArchivePayloadExtractor().extract(
        {
            "run_id": "qear_run_unit",
            "experiment_id": "exp_unit",
            "status": "completed",
            "config": {"data_context": {"freq": "day", "label_horizon": 5, "limit_suspend_authoritative": True}},
            "metrics": {"IC": 0.01, "enhanced_metrics": {"prediction_store_manifest": manifest}},
        }
    )

    assert extracted.source.mlflow_artifact_uri == "aistock-prediction-store://runs/run_unit"
    assert extracted.source.recorder_id == "rec_unit"
    assert extracted.source.recorder_experiment_id == "exp_rec"
    assert extracted.source.metadata["prediction_store"]["forward_only"] is True
    assert len(extracted.artifact_manifest) == 1
    artifact = extracted.artifact_manifest[0]
    assert artifact["artifact_type"] == "prediction"
    assert artifact["artifact_uri"] == "aistock-prediction-store://runs/run_unit/prediction"
    assert artifact["metadata"]["row_count"] == 10
    assert extracted.reproducibility_manifest.artifact_manifest_sha256 is not None


def test_prediction_artifact_store_accepts_label_manifest(tmp_path: Path) -> None:
    label = pd.DataFrame(
        {"label": [0.1, -0.2]},
        index=pd.MultiIndex.from_product(
            [[pd.Timestamp("2026-01-02")], ["000001.SZ", "000002.SZ"]],
            names=["datetime", "instrument"],
        ),
    )
    label_bytes = io.BytesIO()
    label.to_pickle(label_bytes)
    label_bytes.seek(0)

    store = PredictionArtifactStore(root=tmp_path / "prediction_store")
    manifest = store.write_artifacts(
        run_key="qear_run_label",
        files={"label": ("label.pkl", label_bytes)},
        metadata={"experiment_id": "exp_label"},
    )

    items = {item["artifact_type"]: item for item in manifest["artifacts"]}
    assert set(items) == {"label"}
    assert items["label"]["artifact_name"] == "label.pkl"
    assert items["label"]["row_count"] == 2
    assert items["label"]["parser_status"] == "parsed"
    assert store.resolve_artifact_path(manifest["mlflow_artifact_uri"], artifact_type="label").exists()


def test_prediction_store_client_requires_pred_when_upload_enabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import qe_prediction_store_client as client

    params_dir = tmp_path / "mlruns" / "exp" / "rec_unit" / "artifacts"
    params_dir.mkdir(parents=True)
    (params_dir / "params.pkl").write_bytes(b"params-only")
    monkeypatch.setenv("AISTOCK_PREDICTION_STORE_BASE_URL", "http://backend.local:8001")
    monkeypatch.chdir(tmp_path)

    class Recorder:
        info = {"id": "rec_unit", "experiment_id": "exp_unit"}

    with pytest.raises(RuntimeError, match="pred.pkl was not found"):
        client.maybe_upload_prediction_artifacts(
            recorder=Recorder(),
            recorder_ref={"recorder_id": "rec_unit", "target_mlruns_realpath": str(tmp_path / "mlruns")},
            experiment_name="exp_unit",
            mode="train_only",
            config={},
        )
    marker = tmp_path / client.UPLOAD_MARKER_FILE
    assert marker.exists()
    assert json.loads(marker.read_text(encoding="utf-8"))["status"] == "failed"


def test_prediction_store_client_discovers_label_artifact(tmp_path: Path) -> None:
    from scripts import qe_prediction_store_client as client

    artifact_dir = tmp_path / "mlruns" / "exp" / "rec_unit" / "artifacts"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "pred.pkl").write_bytes(b"pred")
    (artifact_dir / "params.pkl").write_bytes(b"params")
    (artifact_dir / "label.pkl").write_bytes(b"label")

    class Recorder:
        info = {"id": "rec_unit", "experiment_id": "exp_unit"}

    artifacts = client._find_artifact_paths(  # noqa: SLF001 - runner helper contract coverage.
        recorder=Recorder(),
        recorder_ref={"recorder_id": "rec_unit", "target_mlruns_realpath": str(tmp_path / "mlruns")},
        recorder_id="rec_unit",
    )

    assert artifacts["prediction"].name == "pred.pkl"
    assert artifacts["model_params"].name == "params.pkl"
    assert artifacts["label"].name == "label.pkl"


def test_label_download_missing_is_explicit_404(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import HTTPException

    from backend.routers import prediction_store as router
    from backend.services.model_store.service import ModelStoreService

    pred_bytes = io.BytesIO()
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_bytes)
    pred_bytes.seek(0)
    store = PredictionArtifactStore(root=tmp_path / "prediction_store")
    store.write_artifacts(run_key="run_without_label", files={"prediction": ("pred.pkl", pred_bytes)})

    service = ModelStoreService(artifact_store=store)
    monkeypatch.setattr(service, "_find_run", lambda **_kwargs: None)
    monkeypatch.setattr(router, "get_model_store_service", lambda: service)

    with pytest.raises(HTTPException) as excinfo:
        router.download_prediction_artifact("run_without_label", "label")

    assert excinfo.value.status_code == 404
    assert "label" in str(excinfo.value.detail)
    with pytest.raises(PredictionStoreNotFound):
        service.label_path(run_id="run_without_label")


def test_prediction_store_client_rejects_manifest_missing_uploaded_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import qe_prediction_store_client as client

    label_path = tmp_path / "label.pkl"
    label_path.write_bytes(b"label")

    class Response:
        status_code = 200
        text = "{}"

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "manifest": {
                        "artifacts": [
                            {"artifact_type": "prediction"},
                        ],
                    },
                },
            }

    monkeypatch.setenv("AISTOCK_PREDICTION_STORE_BASE_URL", "http://backend.local:8001")
    monkeypatch.setattr(client.requests, "post", lambda *args, **kwargs: Response())

    with pytest.raises(RuntimeError, match="manifest missing artifact types"):
        client._post_artifacts(  # noqa: SLF001 - validate runner helper contract.
            run_key="run_missing_label",
            artifacts={"prediction": label_path, "label": label_path},
            metadata={},
        )
