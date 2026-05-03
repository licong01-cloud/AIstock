from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.services.quantevolver.completion_contract import (
    ArtifactManifestItem,
    QECompletionPayload,
    compute_missing_required_fields,
    validate_completion_payload,
)

_VALID_SHA = "a" * 64


def _complete_payload() -> dict:
    return {
        "schema_version": "qe_completion_payload_v1",
        "task_id": "qe_task_1",
        "loop_id": "Loop1",
        "experiment_id": "qe_exp_1",
        "runtime_status": "completed",
        "collection_status": "complete",
        "effective_config": {"strategy": {"topk": 20, "n_drop": 2}, "initial_cash": 1000000},
        "metrics_summary": {"absolute_return": 0.12, "max_drawdown": -0.08},
        "position_summary": {"avg_position_count": 18.5},
        "holding_audit": {"min_holding_days": 1, "avg_holding_days": 3.2},
        "execution_event_summary": {"orders": 10, "fills": 9, "unfilled": 1},
        "cost_reconciliation": {"matched": True, "cost_diff_abs": 0.0},
        "training_source": {"source_task_id": "qe_train", "source_loop_id": "Loop0"},
        "factor_importance_summary": [{"factor_name": "alpha001", "importance_value": 0.3}],
        "data_quality_report": {"research_valid": True, "missing_fields": []},
        "artifact_manifest": [
            {
                "artifact_type": "metrics_summary",
                "uri": "aistock://qe_archive/artifacts/qe_exp_1/metrics_summary.json",
                "sha256": _VALID_SHA,
                "size_bytes": 128,
                "row_count": 1,
                "collection_status": "available",
                "parser_status": "parsed",
            }
        ],
        "reproducibility_level": "full",
    }


def test_complete_payload_contract_accepts_required_sections() -> None:
    payload = QECompletionPayload.model_validate(_complete_payload())

    assert payload.collection_status == "complete"
    assert payload.artifact_manifest[0].sha256 == _VALID_SHA
    assert compute_missing_required_fields(payload) == []
    result = validate_completion_payload(_complete_payload(), require_complete=True)
    assert result.valid is True
    assert result.missing_fields == []
    assert result.errors == []


def test_complete_payload_rejects_missing_required_sections() -> None:
    raw = _complete_payload()
    raw["position_summary"] = {}

    with pytest.raises(ValidationError, match="required fields are missing"):
        QECompletionPayload.model_validate(raw)


def test_partial_payload_reports_missing_fields_without_faking_complete() -> None:
    raw = {
        "task_id": "qe_task_1",
        "runtime_status": "completed",
        "collection_status": "partial",
        "metrics_summary": {"IC": 0.03},
    }

    result = validate_completion_payload(raw, require_complete=False)
    assert result.valid is True
    assert result.payload is not None
    assert "effective_config" in result.missing_fields
    assert "artifact_manifest" in result.missing_fields

    strict_result = validate_completion_payload(raw, require_complete=True)
    assert strict_result.valid is False
    assert strict_result.errors


def test_payload_requires_task_or_experiment_identity() -> None:
    raw = _complete_payload()
    raw.pop("task_id")
    raw.pop("experiment_id")

    with pytest.raises(ValidationError, match="task_id or experiment_id"):
        QECompletionPayload.model_validate(raw)


def test_artifact_manifest_rejects_raw_worker_paths() -> None:
    with pytest.raises(ValidationError, match="worker workspace"):
        ArtifactManifestItem.model_validate(
            {
                "artifact_type": "pred",
                "uri": "/home/lc999/rdagent_workspace/mlruns/1/pred.pkl",
                "sha256": _VALID_SHA,
            }
        )


def test_artifact_manifest_requires_valid_sha256_when_present() -> None:
    with pytest.raises(ValidationError, match="sha256"):
        ArtifactManifestItem.model_validate(
            {
                "artifact_type": "report",
                "uri": "aistock://qe_archive/artifacts/report.parquet",
                "sha256": "not-a-sha",
            }
        )


def test_available_artifact_requires_trace_metadata() -> None:
    with pytest.raises(ValidationError, match="available artifact"):
        ArtifactManifestItem.model_validate(
            {
                "artifact_type": "report",
                "uri": "aistock://qe_archive/artifacts/report.parquet",
            }
        )
