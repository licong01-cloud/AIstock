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
        "QE_RESOURCE_UPLOAD_RETRY_INTERVAL_SEC": "0.01",
        "QE_RESOURCE_FINAL_UPLOAD_GRACE_SEC": "0.01",
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
        raise TimeoutError("backend unavailable X-QE-Resource-Token=secret-token")

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
    assert failure["error_type"] == "TimeoutError"
    failure_text = json.dumps(failure)
    resource_text = (tmp_path / qrr.RESOURCE_FILE).read_text(encoding="utf-8")
    console_text = capsys.readouterr().out
    assert "secret-token" not in failure_text
    assert "secret-token" not in resource_text
    assert "secret-token" not in console_text
    assert "reason_code=QE_RESOURCE_EVENT_UPLOAD_FAILED" in console_text


def test_runtime_resource_outbox_replays_ordered_events_after_backend_recovers(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    qrr._MONITOR = None
    qrr._RESOURCE_SECRET_CACHE = None
    available = False
    attempts = []

    def recovering_post(_url, *, json, **_kwargs):
        attempts.append((available, int(json["sequence_no"]), json["phase"]))
        if not available:
            raise TimeoutError("backend restarting")
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(qrr.requests, "post", recovering_post)
    monkeypatch.setattr(qrr.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_collect_sample",
        lambda _self: {"process_rss_bytes": 1, "process_vm_hwm_bytes": 1},
    )

    monitor = qrr.QERuntimeResourceMonitor()
    monitor._sample_once()
    monitor.transition("train")
    monitor.transition("predict")
    assert [event["sequence_no"] for event in monitor._events] == [1, 2]
    assert monitor._last_uploaded_sequence_no == 0
    assert monitor._upload_broken is True

    available = True
    monitor._next_upload_retry_monotonic = 0.0
    monitor._retry_pending_uploads_if_due()

    successful = [item for item in attempts if item[0]]
    assert successful == [(True, 1, "bootstrap"), (True, 2, "train")]
    assert monitor._last_uploaded_sequence_no == 2
    assert monitor._upload_broken is False
    marker = json.loads((tmp_path / qrr.UPLOAD_FAILURE_FILE).read_text(encoding="utf-8"))
    assert marker["status"] == "recovered"
    assert marker["reason_code"] == "QE_RESOURCE_EVENT_UPLOAD_RECOVERED"
    assert "reason_code=QE_RESOURCE_EVENT_UPLOAD_RECOVERED" in capsys.readouterr().out


def test_runtime_resource_gpu_sampling_failure_is_structured_and_loud(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    qrr._RESOURCE_SECRET_CACHE = None

    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_process_sample",
        lambda _self: {
            "process_rss_bytes": 1,
            "process_vm_hwm_bytes": 2,
            "process_pids": [qrr.os.getpid()],
        },
    )
    def fake_run(command, **_kwargs):
        if any("query-gpu" in part for part in command):
            return SimpleNamespace(returncode=9, stdout="", stderr="unavailable")
        return SimpleNamespace(returncode=0, stdout=f"{qrr.os.getpid()}, 256\n", stderr="")

    monkeypatch.setattr(qrr.subprocess, "run", fake_run)

    monitor = qrr.QERuntimeResourceMonitor()
    sample = monitor._collect_sample()

    assert sample["process_rss_bytes"] == 1
    assert sample["gpu_process_memory_bytes"] == 256 * 1024 * 1024
    assert sample["gpu_process_sample_available"] is True
    assert sample["gpu_device_sample_available"] is False
    assert sample["resource_sample_errors"] == [
        "QE_RESOURCE_GPU_SAMPLE_FAILED:device:RuntimeError"
    ]
    output = capsys.readouterr().out
    assert "reason_code=QE_RESOURCE_GPU_SAMPLE_FAILED" in output
    assert "component=device" in output


def test_runtime_resource_process_gpu_timeout_preserves_device_metrics(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    qrr._RESOURCE_SECRET_CACHE = None
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_process_sample",
        lambda _self: {
            "process_rss_bytes": 1,
            "process_vm_hwm_bytes": 2,
            "process_pids": [123],
        },
    )

    def fake_run(command, **_kwargs):
        if any("query-gpu" in part for part in command):
            return SimpleNamespace(returncode=0, stdout="NVIDIA Test, 1024, 25\n", stderr="")
        raise qrr.subprocess.TimeoutExpired(command, timeout=5)

    monkeypatch.setattr(qrr.subprocess, "run", fake_run)

    sample = qrr.QERuntimeResourceMonitor()._collect_sample()

    assert sample["gpu_memory_used_bytes"] == 1024 * 1024 * 1024
    assert sample["gpu_utilization_pct"] == 25.0
    assert sample["gpu_device_sample_available"] is True
    assert sample["gpu_process_sample_available"] is False
    assert sample["resource_sample_errors"] == [
        "QE_RESOURCE_GPU_SAMPLE_FAILED:process:TimeoutExpired"
    ]
    output = capsys.readouterr().out
    assert "reason_code=QE_RESOURCE_GPU_SAMPLE_FAILED" in output
    assert "component=process" in output


def test_runtime_resource_process_sample_keeps_sum_rss_and_adds_complete_pss(monkeypatch):
    class FakeProcess:
        def __init__(self, pid, rss, children=()):
            self.pid = pid
            self._rss = rss
            self._children = list(children)

        def children(self, *, recursive):
            assert recursive is True
            return list(self._children)

        def memory_info(self):
            return SimpleNamespace(rss=self._rss)

    child = FakeProcess(202, 200)
    root = FakeProcess(101, 300, [child])
    fake_psutil = SimpleNamespace(
        Process=lambda _pid: root,
        NoSuchProcess=RuntimeError,
        AccessDenied=PermissionError,
    )
    monkeypatch.setattr(qrr, "psutil", fake_psutil)
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_read_vm_hwm",
        staticmethod(lambda pid: {101: 350, 202: 250}[pid]),
    )
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_read_pss",
        staticmethod(lambda pid: {101: 180, 202: 120}[pid]),
    )

    monitor = object.__new__(qrr.QERuntimeResourceMonitor)
    sample = monitor._process_sample()

    assert sample["process_rss_bytes"] == 500
    assert sample["process_pss_bytes"] == 300
    assert sample["process_pss_complete"] is True
    assert sample["process_vm_hwm_bytes"] == 350
    assert sample["process_pids"] == [101, 202]

    aggregate = qrr._PhaseAggregate("train")
    aggregate.observe(sample)
    event = aggregate.event_fields()
    assert event["process_rss_peak_bytes"] == 500
    assert event["metadata"]["process_pss_peak_bytes"] == 300
    assert event["metadata"]["process_pss_complete_sample_count"] == 1
    assert event["metadata"]["process_capacity_metric"] == "process_pss_peak_bytes"
    assert "shared_pages_may_be_counted" in event["metadata"]["process_rss_semantics"]

    incomplete = qrr._PhaseAggregate("predict")
    incomplete.observe(
        {
            "process_rss_bytes": 500,
            "process_pss_bytes": 180,
            "process_pss_complete": False,
        }
    )
    incomplete_metadata = incomplete.event_fields()["metadata"]
    assert incomplete_metadata["process_pss_peak_bytes"] is None
    assert incomplete_metadata["process_pss_complete_sample_count"] == 0
    assert incomplete_metadata["process_capacity_metric"] is None
