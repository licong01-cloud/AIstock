from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import qe_runtime_resource as qrr


ROOT = Path(__file__).resolve().parents[3]
RESOURCE_METRIC_FIELDS = {
    "sample_count",
    "process_rss_peak_bytes",
    "process_vm_hwm_peak_bytes",
    "gpu_device_index",
    "gpu_name",
    "gpu_memory_used_peak_bytes",
    "gpu_process_memory_peak_bytes",
    "gpu_utilization_avg_pct",
    "gpu_utilization_peak_pct",
    "cuda_allocated_peak_bytes",
    "cuda_reserved_peak_bytes",
    "cuda_allocated_end_bytes",
    "cuda_reserved_end_bytes",
}


def _set_phase_env(monkeypatch, tmp_path: Path) -> None:
    values = {
        "QE_RESOURCE_SESSION_ID": "qers_test",
        "QE_RESOURCE_SOURCE_RUN_KEY": "qe_task_L1",
        "QE_RESOURCE_SESSION_TOKEN": "secret-token",
        "QE_TASK_ID": "qe_task",
        "QE_LOOP_ID": "Loop1",
        "QE_LOOP_INDEX": "1",
        "QE_NODE_ID": "wsl2-5080",
        "QE_PHASE_PIPELINE_ENABLED": "1",
        "QE_RESOURCE_UPLOAD_RETRY_INTERVAL_SEC": "0.001",
        "QE_RESOURCE_FINAL_UPLOAD_GRACE_SEC": "0.001",
        "AISTOCK_PREDICTION_STORE_BASE_URL": "http://127.0.0.1:8001",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir(tmp_path)


@pytest.fixture(autouse=True)
def _reset_phase_publisher():
    qrr._PUBLISHER = None
    qrr._RESOURCE_SECRET_CACHE = None
    qrr._PHASE_EVENT_STATE.defer_depth = 0
    qrr._PHASE_EVENT_STATE.defer_reason = None
    yield
    qrr._PUBLISHER = None
    qrr._RESOURCE_SECRET_CACHE = None


def test_gpu_resource_monitoring_implementations_are_removed_from_runtime_sources():
    runtime_source = (ROOT / "scripts" / "qe_runtime_resource.py").read_text(encoding="utf-8")
    model_source = (ROOT / "aistock_models" / "aistock_models" / "efficient_gats.py").read_text(
        encoding="utf-8"
    )
    runner_source = "\n".join(
        (ROOT / "scripts" / name).read_text(encoding="utf-8")
        for name in ("qrun_limit.py", "qrun_limit_minute.py")
    )

    forbidden_runtime_calls = (
        "nvidia-smi",
        "pynvml",
        "psutil",
        "torch.cuda.mem_get_info",
        "torch.cuda.memory_allocated",
        "torch.cuda.memory_reserved",
        "QERuntimeResourceMonitor",
        "start_resource_monitor",
        "resource_sample",
    )
    combined = runtime_source + model_source + runner_source
    for forbidden in forbidden_runtime_calls:
        assert forbidden not in combined

    assert not (ROOT / "monitoring" / "textfile_collector" / "collect_gpu_metrics.sh").exists()
    dashboard_source = (ROOT / "monitoring" / "grafana" / "patch_node_exporter_dashboard.py").read_text(
        encoding="utf-8"
    )
    assert '"expr": "gpu_memory_used_bytes"' not in dashboard_source
    assert '"expr": "gpu_power_draw_watts"' not in dashboard_source
    assert '"expr": "gpu_temperature_celsius"' not in dashboard_source


def test_phase_publisher_emits_ordered_lifecycle_without_resource_metrics(monkeypatch, tmp_path):
    _set_phase_env(monkeypatch, tmp_path)
    posts = []

    def fake_post(url, *, json, headers, timeout):
        posts.append({"url": url, "json": dict(json), "headers": dict(headers), "timeout": timeout})
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(qrr.requests, "post", fake_post)

    publisher = qrr.start_phase_publisher()
    assert publisher is not None
    qrr.transition_runtime_phase("train")
    qrr.record_model_resident_state(requested=True, active=True)
    qrr.transition_runtime_phase("predict")
    assert qrr.finalize_gpu_phase_lifecycle() is True
    qrr.finish_phase_publisher(status="completed")

    events = [item["json"] for item in posts]
    assert [event["phase"] for event in events] == [
        "bootstrap",
        "train",
        "predict",
        "gpu_phase_released",
        "backtest",
        "completed",
    ]
    assert [event["sequence_no"] for event in events] == list(range(1, 7))
    assert posts[0]["headers"] == {"X-QE-Resource-Token": "secret-token"}

    for event in events:
        assert RESOURCE_METRIC_FIELDS.isdisjoint(event)
    release_event = events[3]
    assert release_event["release_check_passed"] is None
    assert release_event["reason_code"] == "QE_GPU_PHASE_LIFECYCLE_COMPLETE"
    assert release_event["metadata"]["resource_monitoring_enabled"] is False
    assert events[1]["resident_requested"] is True
    assert events[1]["resident_active"] is True

    local = json.loads((tmp_path / qrr.PHASE_FILE).read_text(encoding="utf-8"))
    assert local["resource_monitoring_enabled"] is False
    assert local["resource_monitoring_reason_code"] == "QE_RESOURCE_MONITORING_DISABLED"
    assert local["pending_event_count"] == 0


def test_phase_publisher_reads_scoped_secret_file_without_exposing_token(monkeypatch, tmp_path):
    _set_phase_env(monkeypatch, tmp_path)
    monkeypatch.delenv("QE_RESOURCE_SESSION_TOKEN")
    (tmp_path / qrr.RESOURCE_SECRET_FILE).write_text(
        json.dumps(
            {
                "session_id": "qers_test",
                "source_run_key": "qe_task_L1",
                "token": "file-secret-token",
            }
        ),
        encoding="utf-8",
    )
    posts = []
    monkeypatch.setattr(
        qrr.requests,
        "post",
        lambda url, *, json, headers, timeout: (
            posts.append({"json": dict(json), "headers": dict(headers)})
            or SimpleNamespace(status_code=200, text="ok")
        ),
    )

    publisher = qrr.start_phase_publisher()
    assert publisher is not None
    qrr.transition_runtime_phase("train")

    assert posts[0]["headers"]["X-QE-Resource-Token"] == "file-secret-token"
    local_text = (tmp_path / qrr.PHASE_FILE).read_text(encoding="utf-8")
    assert "file-secret-token" not in local_text


def test_missing_phase_identity_fails_loudly_without_starting_background_work(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("QE_PHASE_PIPELINE_ENABLED", "1")
    for name in (
        "QE_RESOURCE_SESSION_ID",
        "QE_RESOURCE_SOURCE_RUN_KEY",
        "QE_RESOURCE_SESSION_TOKEN",
        "QE_TASK_ID",
        "QE_LOOP_ID",
        "QE_LOOP_INDEX",
    ):
        monkeypatch.delenv(name, raising=False)

    assert qrr.start_phase_publisher() is None
    output = capsys.readouterr().out
    assert "reason_code=QE_PHASE_PUBLISHER_MISSING" in output
    assert not (tmp_path / qrr.PHASE_FILE).exists()


def test_upload_failure_is_explicit_and_same_event_is_retried(monkeypatch, tmp_path):
    _set_phase_env(monkeypatch, tmp_path)
    calls = []
    outcomes = [RuntimeError("backend unavailable")] * 3 + [SimpleNamespace(status_code=200, text="ok")]

    def fake_post(url, *, json, headers, timeout):
        calls.append(dict(json))
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(qrr.requests, "post", fake_post)
    monkeypatch.setattr(qrr.time, "sleep", lambda _seconds: None)

    publisher = qrr.start_phase_publisher()
    assert publisher is not None
    qrr.transition_runtime_phase("train")
    assert publisher._upload_broken is True
    failure = json.loads((tmp_path / qrr.UPLOAD_FAILURE_FILE).read_text(encoding="utf-8"))
    assert failure["reason_code"] == "QE_PHASE_EVENT_UPLOAD_FAILED"
    assert calls[0] == calls[1] == calls[2]

    assert publisher._flush_pending_events() is True
    assert calls[3] == calls[0]
    assert publisher._last_uploaded_sequence_no == 1


def test_deferred_phase_events_do_not_publish_duplicate_model_events(monkeypatch, tmp_path):
    _set_phase_env(monkeypatch, tmp_path)
    posts = []
    monkeypatch.setattr(
        qrr.requests,
        "post",
        lambda url, *, json, headers, timeout: (
            posts.append(dict(json)) or SimpleNamespace(status_code=200, text="ok")
        ),
    )
    assert qrr.start_phase_publisher() is not None

    with qrr.defer_runtime_phase_events("seed_submodel"):
        qrr.transition_runtime_phase("train")
        assert qrr.publish_gpu_phase_complete() is False

    assert posts == []
    qrr.transition_runtime_phase("train")
    assert [event["phase"] for event in posts] == ["bootstrap"]


def test_prediction_failure_does_not_publish_false_gpu_lifecycle_completion(monkeypatch, tmp_path):
    _set_phase_env(monkeypatch, tmp_path)
    posts = []
    monkeypatch.setattr(
        qrr.requests,
        "post",
        lambda url, *, json, headers, timeout: (
            posts.append(dict(json)) or SimpleNamespace(status_code=200, text="ok")
        ),
    )
    assert qrr.start_phase_publisher() is not None
    qrr.transition_runtime_phase("train")
    qrr.transition_runtime_phase("predict")

    assert qrr.finalize_gpu_phase_lifecycle(predict_error=RuntimeError("predict failed")) is False
    assert all(event["phase"] != "gpu_phase_released" for event in posts)


def test_runners_use_phase_only_helpers_and_keep_model_aware_pipeline():
    runner_source = "\n".join(
        (ROOT / "scripts" / name).read_text(encoding="utf-8")
        for name in ("qrun_limit.py", "qrun_limit_minute.py")
    )
    for required in (
        "start_phase_publisher",
        "finish_phase_publisher",
        "transition_runtime_phase",
    ):
        assert required in runner_source
    assert "finalize_gpu_phase_lifecycle" in runner_source
    assert "start_resource_monitor" not in runner_source
    assert "capture_gpu_release_baseline" not in runner_source
