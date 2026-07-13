from __future__ import annotations

import json
from types import SimpleNamespace

from scripts import qe_runtime_resource as qrr


def _set_resource_env(monkeypatch):
    values = {
        "QE_RESOURCE_SESSION_ID": "qers_test",
        "QE_RESOURCE_SOURCE_RUN_KEY": "qe_task_L1",
        "QE_RESOURCE_SESSION_TOKEN": "secret-token",
        "QE_TASK_ID": "qe_task",
        "QE_LOOP_ID": "Loop1",
        "QE_LOOP_INDEX": "1",
        "QE_NODE_ID": "wsl2-5080",
        "QE_PHASE_PIPELINE_ENABLED": "1",
        "QE_RESOURCE_SAMPLE_INTERVAL_SEC": "60",
        "AISTOCK_PREDICTION_STORE_BASE_URL": "http://127.0.0.1:8001",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_runtime_resource_monitor_publishes_ordered_phase_aggregates(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    qrr._MONITOR = None
    qrr._RESOURCE_SECRET_CACHE = None
    monkeypatch.delenv("QE_RESOURCE_SESSION_TOKEN")
    (tmp_path / qrr.RESOURCE_SECRET_FILE).write_text(
        json.dumps(
            {
                "session_id": "qers_test",
                "source_run_key": "qe_task_L1",
                "token": "secret-token",
            }
        ),
        encoding="utf-8",
    )
    posts = []

    def fake_post(url, *, json, headers, timeout):
        posts.append({"url": url, "json": dict(json), "headers": dict(headers), "timeout": timeout})
        return SimpleNamespace(status_code=200, text="ok")

    def fake_run(command, **_kwargs):
        joined = " ".join(command)
        if "query-gpu" in joined:
            return SimpleNamespace(returncode=0, stdout="NVIDIA Test, 1024, 25\n")
        return SimpleNamespace(returncode=0, stdout=f"{qrr.os.getpid()}, 512\n")

    monkeypatch.setattr(qrr.requests, "post", fake_post)
    monkeypatch.setattr(qrr.subprocess, "run", fake_run)

    monitor = qrr.start_resource_monitor()
    assert monitor is not None
    qrr.transition_resource_phase("train")
    qrr.record_gpu_resident_state(requested=True, active=True)
    qrr.transition_resource_phase("predict")
    assert qrr.publish_gpu_phase_release(
        {
            "release_check_passed": True,
            "release_baseline_allocated_bytes": 100,
            "release_baseline_reserved_bytes": 200,
            "cuda_allocated_bytes_after": 100,
            "cuda_reserved_bytes_after": 200,
            "release_tolerance_bytes": 256,
        }
    ) is True
    qrr.finish_resource_monitor(status="completed")

    phases = [item["json"]["phase"] for item in posts]
    assert phases == ["bootstrap", "train", "predict", "gpu_phase_released", "backtest", "completed"]
    assert [item["json"]["sequence_no"] for item in posts] == list(range(1, 7))
    assert all(item["headers"] == {"X-QE-Resource-Token": "secret-token"} for item in posts)
    assert posts[0]["json"]["gpu_memory_used_peak_bytes"] == 1024 * 1024 * 1024
    assert posts[3]["json"]["reason_code"] == "QE_GPU_PHASE_RELEASE_CONFIRMED"

    marker = json.loads((tmp_path / qrr.RESOURCE_FILE).read_text(encoding="utf-8"))
    assert marker["last_sequence_no"] == 6
    assert marker["upload_broken"] is False


def test_runtime_resource_upload_failure_is_loud_and_release_remains_closed(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    qrr._MONITOR = None
    qrr._RESOURCE_SECRET_CACHE = None

    def failing_post(*_args, **_kwargs):
        raise TimeoutError("backend unavailable")

    monkeypatch.setattr(qrr.requests, "post", failing_post)
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_collect_sample",
        lambda _self: {"process_rss_bytes": 1, "process_vm_hwm_bytes": 1},
    )
    monkeypatch.setattr(qrr.time, "sleep", lambda _seconds: None)

    monitor = qrr.start_resource_monitor()
    assert monitor is not None
    qrr.transition_resource_phase("train")
    released = qrr.publish_gpu_phase_release(
        {
            "release_check_passed": True,
            "release_baseline_allocated_bytes": 0,
            "release_baseline_reserved_bytes": 0,
            "cuda_allocated_bytes_after": 0,
            "cuda_reserved_bytes_after": 0,
            "release_tolerance_bytes": 0,
        }
    )
    qrr.finish_resource_monitor(status="failed", error="test failure")

    assert released is False
    failure = json.loads((tmp_path / qrr.UPLOAD_FAILURE_FILE).read_text(encoding="utf-8"))
    assert failure["reason_code"] == "QE_RESOURCE_EVENT_UPLOAD_FAILED"
    assert "reason_code=QE_RESOURCE_EVENT_UPLOAD_FAILED" in capsys.readouterr().out
