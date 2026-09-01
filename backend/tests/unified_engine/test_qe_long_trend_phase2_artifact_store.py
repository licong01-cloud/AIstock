from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.services.quantevolver.long_trend_artifact_store import (
    QELongTrendArtifactStore,
    QELongTrendArtifactStoreError,
)
from backend.services.quantevolver.long_trend_evaluation_contract import QELongTrendReason


EVALUATION_ID = "qelt_" + "a" * 64


def _write_json(path: Path, payload: dict[str, object]) -> dict[str, object]:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(encoded)
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "size_bytes": len(encoded)}


def _terminal() -> dict[str, object]:
    return {
        "schema_version": "qe_long_trend_worker_terminal_v1",
        "evaluation_id": EVALUATION_ID,
        "status": "partial",
        "attempt_id": "attempt-1",
        "node_id": "wsl2-5080",
        "input_manifest_sha256": "b" * 64,
        "bundle_sha256": "c" * 64,
        "execution_environment_snapshot_id": "qeenv-fixture",
        "execution_environment_manifest_sha256": "d" * 64,
        "family_status": {
            "signal_path": {"status": "NOT_COMPUTABLE", "reason_codes": ["prediction_missing"]},
            "position_episode": {"status": "NOT_VERIFIABLE", "reason_codes": ["position_missing"]},
        },
    }


def test_dedicated_cas_publishes_atomically_and_is_idempotent(tmp_path: Path) -> None:
    store = QELongTrendArtifactStore(
        tmp_path / "long-trend-cas",
        prediction_store_root=tmp_path / "prediction-store",
    )
    terminal = _terminal()
    terminal_path = tmp_path / "worker_terminal_receipt.json"
    compact_path = tmp_path / "worker_compact_receipt.json"
    terminal_meta = _write_json(terminal_path, terminal)
    compact_meta = _write_json(
        compact_path,
        {
            "schema_version": "qe_long_trend_worker_compact_v1",
            "evaluation_id": EVALUATION_ID,
        },
    )
    files = {
        "worker_terminal_receipt": terminal_path,
        "worker_compact_receipt": compact_path,
    }
    catalog = {
        "worker_terminal_receipt": terminal_meta,
        "worker_compact_receipt": compact_meta,
    }
    contradictory_path = tmp_path / "signal_observations.parquet"
    contradictory_meta = _write_json(contradictory_path, {"not": "used"})
    with pytest.raises(QELongTrendArtifactStoreError, match="typed-absent artifacts were also supplied"):
        store.publish(
            evaluation_id=EVALUATION_ID,
            worker_terminal=terminal,
            artifact_files={**files, "signal_observations": contradictory_path},
            expected_catalog={**catalog, "signal_observations": contradictory_meta},
        )

    first = store.publish(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        artifact_files=files,
        expected_catalog=catalog,
    )
    second = store.publish(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        artifact_files=files,
        expected_catalog=catalog,
    )
    assert second == first
    with ThreadPoolExecutor(max_workers=4) as pool:
        concurrent = list(
            pool.map(
                lambda _index: store.publish(
                    evaluation_id=EVALUATION_ID,
                    worker_terminal=terminal,
                    artifact_files=files,
                    expected_catalog=catalog,
                ),
                range(4),
            )
        )
    assert all(item["artifact_manifest_sha256"] == first["artifact_manifest_sha256"] for item in concurrent)
    assert store.load_manifest(first["uri"])["artifact_manifest_sha256"] == first["artifact_manifest_sha256"]
    assert set(first["typed_absence"]) == {"signal_observations", "holding_episodes"}

    manifest_path = store.manifest_path(EVALUATION_ID)
    manifest_bytes = manifest_path.read_bytes()
    tampered_manifest = json.loads(manifest_bytes)
    tampered_manifest["identity"]["node_id"] = "tampered-node"
    manifest_path.write_text(json.dumps(tampered_manifest), encoding="utf-8")
    with pytest.raises(QELongTrendArtifactStoreError, match="manifest hash is invalid"):
        store.publish(
            evaluation_id=EVALUATION_ID,
            worker_terminal=terminal,
            artifact_files=files,
            expected_catalog=catalog,
        )
    manifest_path.write_bytes(manifest_bytes)

    published_receipt = {
        "schema_version": "qe_long_trend_published_compact_v1",
        "evaluation_id": EVALUATION_ID,
        "artifact_manifest_uri": first["uri"],
        "artifact_manifest_sha256": first["artifact_manifest_sha256"],
    }
    published = store.publish_compact_receipt(
        evaluation_id=EVALUATION_ID,
        receipt=published_receipt,
    )
    assert published["sha256"] == hashlib.sha256(
        json.dumps(published_receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert store.load_json_artifact(
        evaluation_id=EVALUATION_ID,
        artifact_type="worker_terminal_receipt",
    ) == terminal
    loaded_published, loaded_meta = store.load_published_compact_receipt(EVALUATION_ID)
    assert loaded_published == published_receipt
    assert loaded_meta == {key: value for key, value in published.items() if key != "path"}
    assert "path" not in loaded_meta

    with pytest.raises(QELongTrendArtifactStoreError, match="not a manifest JSON receipt"):
        store.load_json_artifact(
            evaluation_id=EVALUATION_ID,
            artifact_type="published_compact_receipt",
        )

    manifest_item = next(
        item for item in first["artifacts"] if item["artifact_type"] == "worker_terminal_receipt"
    )
    store.blob_path(manifest_item["sha256"]).write_bytes(b"tampered")
    with pytest.raises(QELongTrendArtifactStoreError, match="differs from manifest"):
        store.load_json_artifact(
            evaluation_id=EVALUATION_ID,
            artifact_type="worker_terminal_receipt",
        )

    published_path = store.root / "evaluations" / EVALUATION_ID / "published_compact_receipt.json"
    published_path.write_text(
        json.dumps({**published_receipt, "artifact_manifest_sha256": "0" * 64}),
        encoding="utf-8",
    )
    with pytest.raises(QELongTrendArtifactStoreError, match="differs from the immutable manifest"):
        store.load_published_compact_receipt(EVALUATION_ID)
    published_path.unlink()
    with pytest.raises(QELongTrendArtifactStoreError, match="missing or linked"):
        store.load_published_compact_receipt(EVALUATION_ID)


def test_same_evaluation_with_different_terminal_content_conflicts(tmp_path: Path) -> None:
    store = QELongTrendArtifactStore(
        tmp_path / "long-trend-cas",
        prediction_store_root=tmp_path / "prediction-store",
    )
    terminal = _terminal()
    terminal_path = tmp_path / "worker_terminal_receipt.json"
    compact_path = tmp_path / "worker_compact_receipt.json"
    terminal_meta = _write_json(terminal_path, terminal)
    compact_meta = _write_json(
        compact_path,
        {"schema_version": "qe_long_trend_worker_compact_v1", "evaluation_id": EVALUATION_ID},
    )
    files = {"worker_terminal_receipt": terminal_path, "worker_compact_receipt": compact_path}
    catalog = {"worker_terminal_receipt": terminal_meta, "worker_compact_receipt": compact_meta}
    store.publish(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        artifact_files=files,
        expected_catalog=catalog,
    )

    conflicting_terminal = {**terminal, "reason_code": "different"}
    terminal_meta = _write_json(terminal_path, conflicting_terminal)
    with pytest.raises(QELongTrendArtifactStoreError) as exc_info:
        store.publish(
            evaluation_id=EVALUATION_ID,
            worker_terminal=conflicting_terminal,
            artifact_files=files,
            expected_catalog={**catalog, "worker_terminal_receipt": terminal_meta},
        )
    assert exc_info.value.reason_code == QELongTrendReason.CAS_MANIFEST_CONFLICT.value
