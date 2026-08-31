"""Unit tests for staged performance receipt helpers.

Covers the pure mapping/recording logic so the nox coverage gate sees real
behavior, not fabricated timings.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.models import (
    STAGE_COMPUTE,
    STAGE_MARKET_FREEZE,
    STAGE_QE_SOURCE_LOAD,
    CacheArtifactState,
)
from backend.services.hmm_evolution.performance_receipt import (
    StageRecorder,
    cache_evidence_from_artifact_info,
    capture_hardware_identity,
    capture_runtime_identity,
    current_rss_bytes,
    duration_ms,
    evidence_payload,
)

T0 = datetime(2026, 7, 22, 1, 0, 0, tzinfo=timezone.utc)


def test_duration_ms_rounds_and_clamps_to_zero() -> None:
    assert duration_ms(T0, T0 + timedelta(milliseconds=1500)) == 1500
    assert duration_ms(T0, T0) == 0
    # Negative deltas are never reported as negative durations.
    assert duration_ms(T0 + timedelta(seconds=2), T0) == 0


def test_stage_recorder_start_end_happy_path() -> None:
    recorder = StageRecorder()
    recorder.start(STAGE_QE_SOURCE_LOAD, at=T0)
    assert recorder.is_open(STAGE_QE_SOURCE_LOAD) is True
    assert recorder.has(STAGE_QE_SOURCE_LOAD) is False
    recorder.end(STAGE_QE_SOURCE_LOAD, at=T0 + timedelta(milliseconds=17093))
    assert recorder.is_open(STAGE_QE_SOURCE_LOAD) is False
    assert recorder.has(STAGE_QE_SOURCE_LOAD) is True
    payload = recorder.stage_payload()
    assert payload[STAGE_QE_SOURCE_LOAD]["duration_ms"] == 17093
    assert payload[STAGE_QE_SOURCE_LOAD]["started_at"] == T0.isoformat()


def test_stage_recorder_rejects_double_start() -> None:
    recorder = StageRecorder()
    recorder.start(STAGE_COMPUTE, at=T0)
    with pytest.raises(InvalidSpecError, match="started twice"):
        recorder.start(STAGE_COMPUTE, at=T0)


def test_stage_recorder_rejects_start_after_record() -> None:
    recorder = StageRecorder()
    recorder.record(STAGE_COMPUTE, started_at=T0, completed_at=T0 + timedelta(seconds=1))
    with pytest.raises(InvalidSpecError, match="started twice"):
        recorder.start(STAGE_COMPUTE, at=T0)


def test_stage_recorder_rejects_end_before_start() -> None:
    recorder = StageRecorder()
    with pytest.raises(InvalidSpecError, match="ended before it started"):
        recorder.end(STAGE_COMPUTE, at=T0)


def test_stage_recorder_rejects_double_record() -> None:
    recorder = StageRecorder()
    recorder.record(STAGE_COMPUTE, started_at=T0, completed_at=T0 + timedelta(seconds=1))
    with pytest.raises(InvalidSpecError, match="recorded twice"):
        recorder.record(STAGE_COMPUTE, started_at=T0, completed_at=T0 + timedelta(seconds=2))


def test_stage_recorder_rejects_unknown_stage() -> None:
    recorder = StageRecorder()
    with pytest.raises(InvalidSpecError, match="unknown receipt stage name"):
        recorder.start("total_duration", at=T0)
    with pytest.raises(InvalidSpecError, match="unknown receipt stage name"):
        recorder.record("total_duration", started_at=T0, completed_at=T0)
    with pytest.raises(InvalidSpecError, match="unknown receipt stage name"):
        recorder.end("total_duration", at=T0)


def test_stage_payload_is_sorted_and_json_ready() -> None:
    recorder = StageRecorder()
    recorder.record(STAGE_MARKET_FREEZE, started_at=T0, completed_at=T0 + timedelta(seconds=3))
    recorder.record(STAGE_QE_SOURCE_LOAD, started_at=T0, completed_at=T0 + timedelta(seconds=17))
    keys = list(recorder.stage_payload().keys())
    assert keys == sorted(keys) == [STAGE_MARKET_FREEZE, STAGE_QE_SOURCE_LOAD]


def test_capture_runtime_identity_observes_current_process() -> None:
    identity = capture_runtime_identity(owner_id="hmm-dev-soak-20260722", role="worker")
    assert identity["role"] == "worker"
    assert identity["owner_id"] == "hmm-dev-soak-20260722"
    assert identity["pid"] > 0
    assert identity["python_version"]
    # owner_id stays absent rather than fabricated when not provided.
    assert "owner_id" not in capture_runtime_identity(role="api")


def test_capture_hardware_identity_and_rss_are_observed() -> None:
    hardware = capture_hardware_identity()
    assert hardware["cpu_count_logical"] >= 1
    assert hardware["memory_total_bytes"] > 0
    assert current_rss_bytes() > 0


def test_cache_evidence_skips_missing_probe_entries() -> None:
    evidence = cache_evidence_from_artifact_info(
        {"pred.pkl": {"status": "missing", "source": "prediction_store"}}
    )
    assert evidence == ()


def test_cache_evidence_zero_copy_bypass() -> None:
    evidence = cache_evidence_from_artifact_info(
        {"pred.pkl": {"source": "prediction_store", "zero_copy": True}}
    )
    assert len(evidence) == 1
    entry = evidence[0]
    assert entry.state is CacheArtifactState.ZERO_COPY_BYPASS
    assert entry.zero_copy is True
    assert entry.source == "prediction_store"


def test_cache_evidence_workspace_cache_cold_warm_and_fallback() -> None:
    evidence = cache_evidence_from_artifact_info(
        {
            "label.pkl": {
                "source": "qe_workspace_cache",
                "downloaded_in_run": True,
                "fallback": True,
            },
            "pred.pkl": {"source": "qe_workspace_cache", "downloaded_in_run": False},
        }
    )
    by_name = {entry.artifact: entry for entry in evidence}
    # Sorted by artifact name.
    assert [entry.artifact for entry in evidence] == ["label.pkl", "pred.pkl"]
    assert by_name["label.pkl"].state is CacheArtifactState.FALLBACK_DOWNLOAD
    assert by_name["pred.pkl"].state is CacheArtifactState.WARM_HIT


def test_cache_evidence_workspace_cache_cold_miss_without_fallback() -> None:
    evidence = cache_evidence_from_artifact_info(
        {"pred.pkl": {"source": "qe_workspace_cache", "downloaded_in_run": True}}
    )
    assert evidence[0].state is CacheArtifactState.COLD_MISS


def test_cache_evidence_direct_workspace_download() -> None:
    cold = cache_evidence_from_artifact_info({"pred.pkl": {"source": "qe_workspace"}})
    fallback = cache_evidence_from_artifact_info(
        {"pred.pkl": {"source": "qe_workspace", "fallback": True}}
    )
    assert cold[0].state is CacheArtifactState.COLD_MISS
    assert fallback[0].state is CacheArtifactState.FALLBACK_DOWNLOAD


def test_cache_evidence_unknown_source_and_default_status() -> None:
    evidence = cache_evidence_from_artifact_info({"pred.pkl": {}})
    assert evidence[0].state is CacheArtifactState.UNKNOWN
    assert evidence[0].source == "unknown"


def test_evidence_payload_serializes_model_entries() -> None:
    evidence = cache_evidence_from_artifact_info(
        {"pred.pkl": {"source": "prediction_store", "zero_copy": True}}
    )
    payload = evidence_payload(evidence)
    assert payload == [
        {
            "artifact": "pred.pkl",
            "state": "zero_copy_bypass",
            "source": "prediction_store",
            "zero_copy": True,
            "detail": None,
        }
    ]
