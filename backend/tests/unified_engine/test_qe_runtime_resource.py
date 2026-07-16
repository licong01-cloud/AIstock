from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from scripts import qe_runtime_resource as qrr


def _set_resource_env(monkeypatch, tmp_path):
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
        "QE_RESOURCE_GPU_CACHE_DIR": str(tmp_path / "node-gpu-cache"),
        "QE_RESOURCE_UPLOAD_RETRY_INTERVAL_SEC": "0.01",
        "QE_RESOURCE_FINAL_UPLOAD_GRACE_SEC": "0.01",
        "AISTOCK_PREDICTION_STORE_BASE_URL": "http://127.0.0.1:8001",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_runtime_resource_monitor_publishes_ordered_phase_aggregates(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch, tmp_path)
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

    monkeypatch.setattr(qrr.requests, "post", fake_post)
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_query_nvml_device",
        lambda _self, gpu_index: {
            "gpu_device_index": gpu_index,
            "gpu_name": "NVIDIA Test",
            "gpu_memory_used_bytes": 1024 * 1024 * 1024,
            "gpu_utilization_pct": 25.0,
        },
    )
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_torch_sample",
        staticmethod(
            lambda: {
                "cuda_allocated_bytes": 256 * 1024 * 1024,
                "cuda_reserved_bytes": 512 * 1024 * 1024,
                "gpu_process_sample_source": "torch.cuda.memory_reserved",
            }
        ),
    )

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


def test_runtime_resource_same_phase_transition_is_idempotent(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(qrr.QERuntimeResourceMonitor, "_sample_once", lambda _self: None)
    monitor = qrr.QERuntimeResourceMonitor()
    monitor._phase = qrr._PhaseAggregate("train", metadata={"first": True})

    monitor.transition("train", metadata={"second": True})

    assert monitor._phase.phase == "train"
    assert monitor._phase.metadata == {"first": True, "second": True}
    assert monitor._events == []
    assert "reason_code=QE_RESOURCE_PHASE_ALREADY_ACTIVE" in capsys.readouterr().out


def test_runtime_resource_release_can_transition_directly_to_finalize(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(qrr.QERuntimeResourceMonitor, "_sample_once", lambda _self: None)

    def fake_publish(self, event):
        self._events.append(dict(event))
        self._last_uploaded_sequence_no = int(event["sequence_no"])

    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_publish",
        fake_publish,
    )
    monitor = qrr.QERuntimeResourceMonitor()
    monitor._phase = qrr._PhaseAggregate("predict")

    released = monitor.release_gpu_phase(
        proof={
            "release_check_passed": True,
            "cuda_allocated_bytes_after": 0,
            "cuda_reserved_bytes_after": 0,
        },
        next_phase="finalize",
    )

    assert released is True
    assert monitor._phase.phase == "finalize"
    assert [event["phase"] for event in monitor._events] == ["predict", "gpu_phase_released"]


def test_runtime_resource_upload_failure_is_loud_and_release_remains_closed(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch, tmp_path)
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
    _set_resource_env(monkeypatch, tmp_path)
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
    _set_resource_env(monkeypatch, tmp_path)
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
    def fail_nvml(_self, _gpu_index):
        raise RuntimeError("NVML device unavailable")

    monkeypatch.setattr(qrr.QERuntimeResourceMonitor, "_query_nvml_device", fail_nvml)
    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_torch_sample",
        staticmethod(
            lambda: {
                "cuda_allocated_bytes": 128 * 1024 * 1024,
                "cuda_reserved_bytes": 256 * 1024 * 1024,
                "gpu_process_sample_source": "torch.cuda.memory_reserved",
            }
        ),
    )

    monitor = qrr.QERuntimeResourceMonitor()
    sample = monitor._collect_sample()

    assert sample["process_rss_bytes"] == 1
    assert sample["gpu_process_memory_bytes"] == 256 * 1024 * 1024
    assert sample["gpu_process_sample_available"] is True
    assert sample["gpu_process_sample_source"] == "torch.cuda.memory_reserved"
    assert sample["gpu_device_sample_available"] is False
    assert sample["resource_sample_errors"] == [
        "QE_RESOURCE_GPU_SAMPLE_FAILED:device:RuntimeError"
    ]
    output = capsys.readouterr().out
    assert "reason_code=QE_RESOURCE_GPU_SAMPLE_FAILED" in output
    assert "component=device" in output


def test_runtime_resource_torch_gpu_failure_preserves_device_metrics(monkeypatch, tmp_path, capsys):
    _set_resource_env(monkeypatch, tmp_path)
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

    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_query_nvml_device",
        lambda _self, gpu_index: {
            "gpu_device_index": gpu_index,
            "gpu_name": "NVIDIA Test",
            "gpu_memory_used_bytes": 1024 * 1024 * 1024,
            "gpu_utilization_pct": 25.0,
        },
    )

    def fail_torch_sample():
        raise TimeoutError("CUDA allocator query failed")

    monkeypatch.setattr(
        qrr.QERuntimeResourceMonitor,
        "_torch_sample",
        staticmethod(fail_torch_sample),
    )

    sample = qrr.QERuntimeResourceMonitor()._collect_sample()

    assert sample["gpu_memory_used_bytes"] == 1024 * 1024 * 1024
    assert sample["gpu_utilization_pct"] == 25.0
    assert sample["gpu_device_sample_available"] is True
    assert sample["gpu_process_sample_available"] is False
    assert sample["resource_sample_errors"] == [
        "QE_RESOURCE_CUDA_SAMPLE_FAILED:TimeoutError"
    ]
    output = capsys.readouterr().out
    assert "reason_code=QE_RESOURCE_CUDA_SAMPLE_FAILED" in output


def test_runtime_resource_gpu_device_snapshot_is_shared_across_loops(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    qrr._RESOURCE_SECRET_CACHE = None
    query_count = 0

    def fake_nvml(_self, gpu_index):
        nonlocal query_count
        query_count += 1
        return {
            "gpu_device_index": gpu_index,
            "gpu_name": "NVIDIA Test",
            "gpu_memory_used_bytes": 2048,
            "gpu_utilization_pct": 42.0,
        }

    monkeypatch.setattr(qrr.QERuntimeResourceMonitor, "_query_nvml_device", fake_nvml)
    first = qrr.QERuntimeResourceMonitor()
    monkeypatch.setenv("QE_LOOP_ID", "Loop2")
    monkeypatch.setenv("QE_LOOP_INDEX", "2")
    second = qrr.QERuntimeResourceMonitor()

    first_sample = first._gpu_device_sample()
    second_sample = second._gpu_device_sample()

    assert query_count == 1
    assert first_sample["gpu_device_sample_cache_hit"] is False
    assert second_sample["gpu_device_sample_cache_hit"] is True
    assert first_sample["gpu_utilization_pct"] == second_sample["gpu_utilization_pct"] == 42.0
    assert first_sample["gpu_device_sample_source"] == "pynvml_shared_cache"
    assert second_sample["gpu_device_sample_source"] == "pynvml_shared_cache"


def test_runtime_resource_backtest_phase_does_not_query_gpu_driver(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    qrr._RESOURCE_SECRET_CACHE = None
    monitor = qrr.QERuntimeResourceMonitor()
    monitor._phase = qrr._PhaseAggregate("backtest")
    monkeypatch.setattr(
        monitor,
        "_query_nvml_device",
        lambda _gpu_index: (_ for _ in ()).throw(AssertionError("NVML must not run during backtest")),
    )

    sample = monitor._gpu_device_sample()

    assert sample == {
        "gpu_device_sample_available": False,
        "gpu_device_sample_source": "phase_not_gpu",
        "gpu_device_sample_skipped_phase": "backtest",
    }


def test_runtime_resource_wsl_uses_torch_only_without_adapter_query(monkeypatch, tmp_path):
    _set_resource_env(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    qrr._RESOURCE_SECRET_CACHE = None
    monitor = qrr.QERuntimeResourceMonitor()
    monitor.wsl_runtime = True
    monkeypatch.setattr(
        monitor,
        "_query_nvml_device",
        lambda _gpu_index: (_ for _ in ()).throw(AssertionError("WSL must not query NVML")),
    )

    sample = monitor._gpu_device_sample()
    aggregate = qrr._PhaseAggregate("train")
    aggregate.observe(sample)
    event = aggregate.event_fields()

    assert sample == {
        "gpu_device_sample_available": False,
        "gpu_device_sample_source": "wsl_torch_only",
        "gpu_device_sample_skipped_reason_code": qrr._WSL_GPU_QUERY_SUPPRESSED_REASON,
    }
    assert event["gpu_utilization_avg_pct"] is None
    assert event["gpu_utilization_peak_pct"] is None
    assert event["metadata"]["gpu_utilization_sample_count"] == 0

    aggregate.observe(
        {
            "gpu_device_sample_available": True,
            "gpu_device_sample_source": "windows_host",
            "gpu_utilization_pct": 40.0,
        }
    )
    mixed_event = aggregate.event_fields()
    assert mixed_event["gpu_utilization_avg_pct"] == 40.0
    assert mixed_event["gpu_utilization_peak_pct"] == 40.0
    assert mixed_event["metadata"]["gpu_utilization_sample_count"] == 1


def test_runtime_resource_has_no_nvidia_smi_subprocess_fallback():
    source = qrr.Path(qrr.__file__).read_text(encoding="utf-8")

    assert "subprocess.run" not in source
    assert "--query-gpu" not in source
    assert "--query-compute-apps" not in source


def test_qe_runners_wire_phase_aware_training_and_cyclic_deferral():
    scripts_dir = qrr.Path(qrr.__file__).parent
    daily_source = (scripts_dir / "qrun_limit.py").read_text(encoding="utf-8")
    minute_source = (scripts_dir / "qrun_limit_minute.py").read_text(encoding="utf-8")

    assert "return task_train_with_resource_phases(" in daily_source
    assert "return task_train_with_resource_phases(" in minute_source
    assert 'with defer_resource_phase_events("nested_qe_task_train")' in minute_source
    assert 'release_next_phase="finalize"' in minute_source
    assert 'transition_resource_phase("finalize"' in daily_source
    assert 'transition_resource_phase("finalize"' in minute_source


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


def test_generic_gpu_release_uses_only_in_process_torch_cuda(monkeypatch):
    calls = []
    allocated = iter([100, 120])
    reserved = iter([200, 220])
    fake_cuda = SimpleNamespace(
        is_initialized=lambda: True,
        synchronize=lambda: calls.append("synchronize"),
        empty_cache=lambda: calls.append("empty_cache"),
        memory_allocated=lambda: next(allocated),
        memory_reserved=lambda: next(reserved),
    )
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(cuda=fake_cuda))
    monkeypatch.setenv("QE_GPU_PHASE_RELEASE_TOLERANCE_BYTES", "1024")

    class FakeMonitor:
        phase_pipeline_enabled = True

        def __init__(self):
            self.release = None

        def last_gpu_release_event(self):
            return None

        def release_gpu_phase(self, *, proof, next_phase):
            self.release = (dict(proof), next_phase)
            return True

    monitor = FakeMonitor()
    monkeypatch.setattr(qrr, "_MONITOR", monitor)

    baseline = qrr.capture_gpu_phase_release_baseline()
    released = qrr.finalize_gpu_phase_release(baseline, next_phase="backtest")

    assert released is True
    assert baseline == {
        "release_baseline_allocated_bytes": 100,
        "release_baseline_reserved_bytes": 200,
        "release_capture_source": "torch_cuda_in_process",
    }
    proof, next_phase = monitor.release
    assert next_phase == "backtest"
    assert proof["cuda_allocated_bytes_after"] == 120
    assert proof["cuda_reserved_bytes_after"] == 220
    assert proof["release_check_passed"] is True
    assert proof["release_proof_source"] == "runner_generic_cuda_baseline_v1"
    assert calls == ["synchronize", "empty_cache", "synchronize", "empty_cache"]


def test_generic_gpu_release_reuses_existing_model_release_without_duplicate(monkeypatch, capsys):
    class FakeMonitor:
        phase_pipeline_enabled = True

        def last_gpu_release_event(self):
            return {"phase": "gpu_phase_released", "sequence_no": 7}

        def event_is_uploaded(self, sequence_no):
            return sequence_no == 7

        def release_gpu_phase(self, **_kwargs):
            raise AssertionError("existing EfficientGATs release must not be duplicated")

    monkeypatch.setattr(qrr, "_MONITOR", FakeMonitor())

    assert qrr.finalize_gpu_phase_release({}, next_phase="backtest") is True
    assert "reason_code=QE_GPU_PHASE_RELEASE_ALREADY_PUBLISHED" in capsys.readouterr().out


def test_deferred_phase_events_keep_cyclic_inner_tasks_in_outer_phase(monkeypatch, capsys):
    transitions = []
    releases = []

    class FakeMonitor:
        phase_pipeline_enabled = True

        def transition(self, phase, *, metadata=None):
            transitions.append((phase, metadata))

        def release_gpu_phase(self, *, proof, next_phase):
            releases.append((proof, next_phase))
            return True

    monkeypatch.setattr(qrr, "_MONITOR", FakeMonitor())

    with qrr.defer_resource_phase_events("seed_ensemble"):
        qrr.transition_resource_phase("predict")
        assert qrr.publish_gpu_phase_release({"release_check_passed": True}) is False

    qrr.transition_resource_phase("predict", metadata={"outer": True})

    assert transitions == [("predict", {"outer": True})]
    assert releases == []
    output = capsys.readouterr().out
    assert "reason_code=QE_RESOURCE_PHASE_EVENT_DEFERRED" in output
    assert "reason_code=QE_GPU_PHASE_RELEASE_DEFERRED" in output


def _install_fake_qlib_trainer(monkeypatch, events, *, failing_record=None, delegated=None):
    qlib_module = ModuleType("qlib")
    qlib_module.__path__ = []
    model_module = ModuleType("qlib.model")
    model_module.__path__ = []
    trainer_module = ModuleType("qlib.model.trainer")

    class FakeModel:
        def fit(self, dataset, *, reweighter=None):
            events.append(("fit", dataset, reweighter))

    class FakeDataset:
        def config(self, *, dump_all, recursive):
            events.append(("dataset.config", dump_all, recursive))

    class FakeRecorder:
        pass

    recorder = FakeRecorder()
    model = FakeModel()
    dataset = FakeDataset()

    class FakeRunContext:
        def __enter__(self):
            events.append("recorder.enter")
            return self

        def __exit__(self, exc_type, exc, traceback):
            events.append(("recorder.exit", exc_type))
            return False

    class FakeR:
        @staticmethod
        def start(*, experiment_name, recorder_name=None):
            events.append(("recorder.start", experiment_name, recorder_name))
            return FakeRunContext()

        @staticmethod
        def get_recorder():
            return recorder

        @staticmethod
        def save_objects(**objects):
            events.append(("save", tuple(objects)))

    class FakeRecord:
        def __init__(self, name):
            self.name = name

        def generate(self):
            events.append(("record", self.name))
            if self.name == failing_record:
                raise ValueError(f"failed {self.name}")

    def init_instance(config, **_kwargs):
        kind = config.get("kind")
        if kind == "model":
            return model
        if kind == "dataset":
            return dataset
        return FakeRecord(config["class"])

    def delegated_task_train(task_config, *, experiment_name, recorder_name=None):
        events.append(("delegated", task_config, experiment_name, recorder_name))
        return delegated

    trainer_module.R = FakeR
    trainer_module.Model = FakeModel
    trainer_module.Dataset = FakeDataset
    trainer_module._log_task_info = lambda task: events.append(("log_task", task))
    trainer_module.init_instance_by_config = init_instance
    trainer_module.auto_filter_kwargs = lambda function: function
    trainer_module.fill_placeholder = lambda task, _values: task
    trainer_module.task_train = delegated_task_train
    qlib_module.model = model_module
    model_module.trainer = trainer_module
    monkeypatch.setitem(sys.modules, "qlib", qlib_module)
    monkeypatch.setitem(sys.modules, "qlib.model", model_module)
    monkeypatch.setitem(sys.modules, "qlib.model.trainer", trainer_module)
    return recorder


def test_task_train_resource_phases_release_before_portfolio_backtest(monkeypatch):
    events = []
    recorder = _install_fake_qlib_trainer(monkeypatch, events)
    monkeypatch.setattr(qrr, "resource_phase_pipeline_active", lambda: True)
    monkeypatch.setattr(qrr, "_resource_phase_events_deferred", lambda: False)
    monkeypatch.setattr(
        qrr,
        "transition_resource_phase",
        lambda phase, metadata=None: events.append(("phase", phase, metadata)),
    )
    monkeypatch.setattr(
        qrr,
        "capture_gpu_phase_release_baseline",
        lambda: events.append("baseline") or {"baseline": True},
    )
    monkeypatch.setattr(qrr, "_last_gpu_release_event", lambda: None)

    def release(baseline, *, predict_error=None, next_phase="backtest"):
        events.append(("release", baseline, predict_error, next_phase))
        return True

    monkeypatch.setattr(qrr, "finalize_gpu_phase_release", release)
    task = {
        "model": {"kind": "model"},
        "dataset": {"kind": "dataset"},
        "record": [
            {"class": "SignalRecord"},
            {"class": "SigAnaRecord"},
            {"class": "PortAnaRecord"},
        ],
    }

    result = qrr.task_train_with_resource_phases(task, experiment_name="exp")

    assert result is recorder
    assert events.index(("phase", "train", {"phase_source": "qlib_task_train"})) < next(
        index for index, event in enumerate(events) if isinstance(event, tuple) and event[0] == "fit"
    )
    assert events.index(("phase", "predict", {"phase_source": "qlib_task_records"})) < events.index(
        ("record", "SignalRecord")
    )
    assert events.index(("record", "SigAnaRecord")) < events.index(
        ("release", {"baseline": True}, None, "backtest")
    ) < events.index(("record", "PortAnaRecord"))


def test_task_train_resource_phases_reject_release_on_prediction_failure(monkeypatch):
    events = []
    _install_fake_qlib_trainer(monkeypatch, events, failing_record="SignalRecord")
    monkeypatch.setattr(qrr, "resource_phase_pipeline_active", lambda: True)
    monkeypatch.setattr(qrr, "_resource_phase_events_deferred", lambda: False)
    monkeypatch.setattr(qrr, "transition_resource_phase", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(qrr, "capture_gpu_phase_release_baseline", lambda: {"baseline": True})
    monkeypatch.setattr(qrr, "_last_gpu_release_event", lambda: None)
    releases = []
    monkeypatch.setattr(
        qrr,
        "finalize_gpu_phase_release",
        lambda baseline, *, predict_error=None, next_phase="backtest": releases.append(
            (baseline, predict_error, next_phase)
        ),
    )
    task = {
        "model": {"kind": "model"},
        "dataset": {"kind": "dataset"},
        "record": [{"class": "SignalRecord"}, {"class": "PortAnaRecord"}],
    }

    with pytest.raises(ValueError, match="failed SignalRecord"):
        qrr.task_train_with_resource_phases(task, experiment_name="exp")

    assert len(releases) == 1
    baseline, predict_error, next_phase = releases[0]
    assert baseline == {"baseline": True}
    assert isinstance(predict_error, ValueError)
    assert next_phase == "finalize"


def test_task_train_resource_phases_train_only_releases_to_finalize(monkeypatch):
    events = []
    _install_fake_qlib_trainer(monkeypatch, events)
    monkeypatch.setattr(qrr, "resource_phase_pipeline_active", lambda: True)
    monkeypatch.setattr(qrr, "_resource_phase_events_deferred", lambda: False)
    monkeypatch.setattr(qrr, "transition_resource_phase", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(qrr, "capture_gpu_phase_release_baseline", lambda: {"baseline": True})
    releases = []
    monkeypatch.setattr(
        qrr,
        "finalize_gpu_phase_release",
        lambda baseline, *, predict_error=None, next_phase="backtest": releases.append(
            (baseline, predict_error, next_phase)
        ),
    )
    task = {
        "model": {"kind": "model"},
        "dataset": {"kind": "dataset"},
        "record": [{"class": "SignalRecord"}, {"class": "SigAnaRecord"}],
    }

    qrr.task_train_with_resource_phases(
        task,
        experiment_name="exp",
        release_next_phase="finalize",
    )

    assert releases == [({"baseline": True}, None, "finalize")]


def test_task_train_resource_phases_delegates_when_pipeline_is_inactive(monkeypatch):
    events = []
    sentinel = object()
    _install_fake_qlib_trainer(monkeypatch, events, delegated=sentinel)
    monkeypatch.setattr(qrr, "resource_phase_pipeline_active", lambda: False)

    task = {"model": {"kind": "model"}, "dataset": {"kind": "dataset"}}
    result = qrr.task_train_with_resource_phases(
        task,
        experiment_name="exp",
        recorder_name="rec",
    )

    assert result is sentinel
    assert events == [("delegated", task, "exp", "rec")]
