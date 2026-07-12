from __future__ import annotations

import json
import os
import queue
import threading
import time
from pathlib import Path

import pandas as pd
import pytest

from backend.services.quantevolver import official_factor_batch_compute_service as official_batch_svc
from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
from backend.services.quantevolver import offline_code_text_factor_executor as executor_mod
from backend.services.quantevolver.offline_code_text_factor_executor import FactorExecutionResult
from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
from backend.services.quantevolver.official_factor_batch_compute_service import FACTOR_TIMEOUT
from backend.services.quantevolver.official_factor_batch_compute_service import RESOURCE_GATE_FAILED
from backend.services.quantevolver.official_factor_batch_compute_service import BatchComputeConfig
from backend.services.quantevolver.official_factor_batch_compute_service import FactorResourceLimits
from backend.services.quantevolver.official_factor_batch_compute_service import OfficialFactorBatchComputeService
from backend.services.quantevolver.official_factor_batch_compute_service import ResourceGateDecision
from backend.services.quantevolver.official_factor_batch_compute_service import ResourceSnapshot


def test_factor_progress_receipt_tracks_metric_completion(monkeypatch, tmp_path):
    monkeypatch.setattr(official_batch_svc, "OFFICIAL_FACTOR_CACHE_CHECKPOINT_DIR", tmp_path)
    receipt = official_batch_svc.OfficialFactorComputeProgressReceipt("task-1")

    receipt.observe({"type": "factor_plan_ready", "eligible_count": 3})
    receipt.observe({"type": "factor_started", "factor_name": "factor_a"})
    receipt.observe({"type": "factor_done", "factor_name": "factor_a"})
    receipt.observe({"type": "metric_done", "factor_name": "factor_a", "ok": True})
    receipt.observe({"type": "factor_failed", "factor_name": "factor_b"})

    payload = json.loads((tmp_path / "task-1.progress.json").read_text(encoding="utf-8"))
    assert payload["total_factors"] == 3
    assert payload["value_ready_count"] == 1
    assert payload["completed_count"] == 2
    assert payload["success_count"] == 1
    assert payload["failed_count"] == 1
    assert payload["active_factor_names"] == []


def _base_df():
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2018-08-02")], ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"close": [1.0, 2.0, 3.0, 4.0]}, index=idx)


def test_base_data_memory_cache_reads_allowed_files_once(tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    df = _base_df()
    df.to_hdf(data_dir / "daily_pv.h5", key="data")
    df.to_parquet(data_dir / "static_factors.parquet")
    counts = {"h5": 0, "parquet": 0}

    def hdf_reader(path, *args, **kwargs):
        counts["h5"] += 1
        return df

    def parquet_reader(path, *args, **kwargs):
        counts["parquet"] += 1
        return df

    cache = BacktestBaseDataMemoryCache.load_once(data_dir, "2018-08-01", "2018-08-02", hdf_reader=hdf_reader, parquet_reader=parquet_reader)

    assert counts == {"h5": 1, "parquet": 1}
    assert cache.read_counts["daily_pv.h5"] == 1
    assert cache.get("daily_pv.h5").shape == (4, 1)


def test_offline_code_text_executor_redirects_pandas_reads_to_memory(tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    df = _base_df()
    df.to_hdf(data_dir / "daily_pv.h5", key="data")
    cache = BacktestBaseDataMemoryCache.load_once(data_dir, "2018-08-01", "2018-08-02")
    code_text = """
import pandas as pd
base = pd.read_hdf('daily_pv.h5')
result = base[['close']].rename(columns={'close': 'value'})
"""

    result = OfflineCodeTextFactorExecutor(cache).compute_factor("factor_a", code_text)

    assert result.success is True
    assert list(result.dataframe.columns) == ["value"]
    assert result.dataframe.index.names[:2] == ["datetime", "instrument"]


def test_offline_code_text_executor_supports_static_factor_existence_checks(monkeypatch, tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    static_df = _base_df().rename(columns={"close": "bb_pe_dyn"})
    (data_dir / "static_factors.parquet").write_bytes(b"fixture")

    cache = BacktestBaseDataMemoryCache.load_once(
        data_dir,
        "2018-08-01",
        "2018-08-02",
        parquet_reader=lambda *args, **kwargs: static_df,
    )

    def _fail_runtime_parquet_read(*args, **kwargs):
        raise AssertionError("runtime parquet reads must be redirected to BacktestBaseDataMemoryCache")

    monkeypatch.setattr(pd, "read_parquet", _fail_runtime_parquet_read)
    code_text = """
import os
from os.path import isfile
import pandas as pd
if not os.path.exists('static_factors.parquet'):
    raise ValueError('static_factors.parquet not found')
if not isfile('nested/static_factors.parquet'):
    raise ValueError('static_factors.parquet should be visible as loaded base data')
static_df = pd.read_parquet('static_factors.parquet', columns=['bb_pe_dyn'])
result = static_df.rename(columns={'bb_pe_dyn': 'value'})
"""

    result = OfflineCodeTextFactorExecutor(cache).compute_factor("factor_static", code_text)

    assert result.success is True
    assert list(result.dataframe.columns) == ["value"]
    assert result.dataframe["value"].tolist() == [1.0, 2.0, 3.0, 4.0]


def test_offline_code_text_executor_captures_result_h5_without_pytables(monkeypatch, tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    df = _base_df()
    df.to_hdf(data_dir / "daily_pv.h5", key="data")
    cache = BacktestBaseDataMemoryCache.load_once(data_dir, "2018-08-01", "2018-08-02")

    def _fail_to_hdf(*args, **kwargs):
        raise AssertionError("result.h5 writes must be captured in memory")

    def _fail_read_hdf(*args, **kwargs):
        raise AssertionError("captured result.h5 reads must not call PyTables")

    monkeypatch.setattr(executor_mod, "_ORIGINAL_DF_TO_HDF", _fail_to_hdf)
    monkeypatch.setattr(pd, "read_hdf", _fail_read_hdf)
    code_text = """
import pandas as pd
base = pd.read_hdf('daily_pv.h5')
out = base[['close']].rename(columns={'close': 'value'})
out.to_hdf('result.h5', key='data', mode='w')
result = pd.read_hdf('result.h5')
"""

    result = OfflineCodeTextFactorExecutor(cache).compute_factor("factor_a", code_text)

    assert result.success is True
    assert list(result.dataframe.columns) == ["value"]
    assert result.dataframe["value"].tolist() == [1.0, 2.0, 3.0, 4.0]


def test_offline_code_text_executor_thread_local_result_h5_capture(monkeypatch, tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    df = _base_df()
    df.to_hdf(data_dir / "daily_pv.h5", key="data")
    cache = BacktestBaseDataMemoryCache.load_once(data_dir, "2018-08-01", "2018-08-02")

    def _fail_to_hdf(*args, **kwargs):
        raise AssertionError("threaded result.h5 writes must be captured in memory")

    monkeypatch.setattr(executor_mod, "_ORIGINAL_DF_TO_HDF", _fail_to_hdf)
    executor = OfflineCodeTextFactorExecutor(cache)
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    batch = [
        {
            "factor_name": "factor_a",
            "code_text": """
import pandas as pd
base = pd.read_hdf('daily_pv.h5')
out = base[['close']].rename(columns={'close': 'value'}) * 10
out.to_hdf('result.h5', key='data', mode='w')
""",
        },
        {
            "factor_name": "factor_b",
            "code_text": """
import pandas as pd
base = pd.read_hdf('daily_pv.h5')
out = base[['close']].rename(columns={'close': 'value'}) * 100
out.to_hdf('result.h5', key='data', mode='w')
""",
        },
    ]

    result = service._compute_batch_frames(executor, batch, workers=2, timeout_per_factor=1800)

    assert result["factor_a"].success is True
    assert result["factor_b"].success is True
    assert result["factor_a"].dataframe["value"].tolist() == [10.0, 20.0, 30.0, 40.0]
    assert result["factor_b"].dataframe["value"].tolist() == [100.0, 200.0, 300.0, 400.0]


def test_batch_compute_uses_worker_threads_for_factor_values(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    monkeypatch.setattr(svc.multiprocessing, "get_all_start_methods", lambda: [])
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    calls: list[str] = []
    started = threading.Event()

    class _Executor:
        def compute_batch(self, batch):
            raise AssertionError("workers > 1 must not use sequential compute_batch")

        def compute_factor(self, factor_name, code_text):
            calls.append(factor_name)
            started.set()
            time.sleep(0.05)
            return FactorExecutionResult(factor_name=factor_name, success=True, dataframe=_base_df()[["close"]])

    batch = [
        {"factor_name": "factor_a", "code_text": "a"},
        {"factor_name": "factor_b", "code_text": "b"},
        {"factor_name": "factor_c", "code_text": "c"},
    ]

    result = service._compute_batch_frames(_Executor(), batch, workers=3, timeout_per_factor=1800)

    assert started.is_set()
    assert sorted(result) == ["factor_a", "factor_b", "factor_c"]
    assert sorted(calls) == ["factor_a", "factor_b", "factor_c"]


def test_batch_compute_streams_results_to_handler_without_returning_frames(monkeypatch):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    monkeypatch.setattr(svc.multiprocessing, "get_all_start_methods", lambda: [])
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    handled: list[tuple[str, bool]] = []

    class _Executor:
        def compute_batch(self, batch):
            raise AssertionError("workers > 1 must not use sequential compute_batch")

        def compute_factor(self, factor_name, code_text):
            return FactorExecutionResult(factor_name=factor_name, success=True, dataframe=_base_df()[["close"]])

    def _handle(name, result):
        assert result.success is True
        assert result.dataframe is not None
        result.dataframe = None
        handled.append(name)

    batch = [
        {"factor_name": "factor_a", "code_text": "a"},
        {"factor_name": "factor_b", "code_text": "b"},
    ]

    result = service._compute_batch_frames(
        _Executor(),
        batch,
        workers=2,
        timeout_per_factor=1800,
        result_handler=_handle,
    )

    assert result == {}
    assert sorted(handled) == ["factor_a", "factor_b"]


def test_process_result_streaming_does_not_report_false_missing_result(monkeypatch):
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=0,
        swap_growth_hard_stop_mb=10**9,
    )
    service._emit = lambda *args, **kwargs: None
    handled: list[str] = []

    class _FinishedProcess:
        pid = 12345

        def start(self):
            pass

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

    class _Context:
        def Queue(self):
            result = FactorExecutionResult(
                factor_name="factor_a",
                success=True,
                dataframe=_base_df()[["close"]],
            )

            class _Queue:
                def __init__(self):
                    self._items = [("factor_a", result)]

                def get_nowait(self):
                    if self._items:
                        return self._items.pop(0)
                    raise queue.Empty

                def close(self):
                    pass

                def join_thread(self):
                    pass

            return _Queue()

        def Process(self, **kwargs):
            return _FinishedProcess()

    monkeypatch.setattr(
        "backend.services.quantevolver.official_factor_batch_compute_service.multiprocessing.get_context",
        lambda method: _Context(),
    )

    result = service._compute_batch_frames_with_process_timeouts(
        executor=object(),
        batch=[{"factor_name": "factor_a", "code_text": "result = 1"}],
        max_workers=1,
        timeout_sec=10,
        task_id="task-streaming",
        batch_id="batch-streaming",
        swap_baseline_mb=0,
        result_handler=lambda name, value: handled.append((name, value.success)),
    )

    assert result == {}
    assert handled == [("factor_a", True)]


@pytest.mark.skipif(os.name == "nt", reason="fork based timeout is WSL/Linux only")
def test_batch_compute_enforces_per_factor_timeout(tmp_path):
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    df = _base_df()
    df.to_hdf(data_dir / "daily_pv.h5", key="data")
    cache = BacktestBaseDataMemoryCache.load_once(data_dir, "2018-08-01", "2018-08-02")
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = None
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=0,
        swap_growth_hard_stop_mb=10**9,
    )
    batch = [
        {
            "factor_name": "factor_slow",
            "code_text": """
import time
time.sleep(5)
result = None
""",
        }
    ]

    result = service._compute_batch_frames(
        OfflineCodeTextFactorExecutor(cache),
        batch,
        workers=1,
        timeout_per_factor=1,
        task_id="task-timeout",
        batch_id="batch-timeout",
    )

    assert result["factor_slow"].success is False
    assert result["factor_slow"].error_type == FACTOR_TIMEOUT
    assert result["factor_slow"].elapsed_sec < 4


def test_resource_gate_fails_on_swap_growth_and_emits_event(monkeypatch):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    events: list[dict[str, object]] = []
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = events.append
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=1000,
        hard_rss_mb=2000,
        min_available_mb=100,
        swap_growth_hard_stop_mb=10,
    )
    monkeypatch.setattr(
        svc,
        "_resource_snapshot",
        lambda extra_pids=None, fast=False: ResourceSnapshot(
            rss_mb=100.0,
            uss_mb=80.0,
            swap_mb=25.0,
            available_mb=1000.0,
            pss_mb=90.0,
        ),
    )

    decision = service._check_resource_gate("during_batch", swap_baseline_mb=0.0, task_id="task", batch_id="batch")

    assert decision.ok is False
    assert decision.reason == "swap_growth_hard_stop_exceeded"
    assert events[-1]["type"] == "resource_gate_failed"


def test_resource_gate_failure_result_is_classified():
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    decision = type(
        "_Decision",
        (),
        {"reason": "hard_rss_limit_exceeded", "detail": {"reason": "hard_rss_limit_exceeded"}},
    )()
    results: list[dict[str, object]] = []

    service._append_resource_failure_results([{"factor_name": "factor_a"}], results, decision)

    assert results == [
        {
            "name": "factor_a",
            "success": False,
            "error": f"{RESOURCE_GATE_FAILED}: hard_rss_limit_exceeded",
            "error_type": RESOURCE_GATE_FAILED,
            "resource_gate": {"reason": "hard_rss_limit_exceeded"},
        }
    ]


def test_resource_gate_failure_does_not_drain_killed_worker_queue(monkeypatch):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    fake_queue = type(
        "_Queue",
        (),
        {
            "get_count": 0,
            "cancelled": False,
            "closed": False,
            "joined": False,
            "get_nowait": lambda self: _raise_empty_once_then_fail(self),
            "cancel_join_thread": lambda self: setattr(self, "cancelled", True),
            "close": lambda self: setattr(self, "closed", True),
            "join_thread": lambda self: setattr(self, "joined", True),
        },
    )()

    class _Process:
        pid = 12345

        def __init__(self, *args, **kwargs):
            self.alive = False

        def start(self):
            self.alive = True

        def is_alive(self):
            return self.alive

        def terminate(self):
            self.alive = False

        def kill(self):
            self.alive = False

        def join(self, timeout=None):
            return None

    class _Context:
        def Queue(self):
            return fake_queue

        Process = _Process

    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = None
    service._check_resource_gate = lambda *args, **kwargs: ResourceGateDecision(
        False,
        "hard_rss_limit_exceeded",
        {"reason": "hard_rss_limit_exceeded"},
    )
    monkeypatch.setattr(svc.multiprocessing, "get_context", lambda method: _Context())

    result = service._compute_batch_frames_with_process_timeouts(
        object(),
        [{"factor_name": "factor_a", "code_text": "result = 1"}],
        max_workers=1,
        timeout_sec=1800,
        task_id="task-resource-gate",
        batch_id="batch-resource-gate",
        swap_baseline_mb=0.0,
    )

    assert result["factor_a"].success is False
    assert result["factor_a"].error_type == RESOURCE_GATE_FAILED
    assert fake_queue.get_count == 1
    assert fake_queue.cancelled is True
    assert fake_queue.closed is True
    assert fake_queue.joined is True


def test_available_memory_pressure_reduces_concurrency_and_keeps_running_workers(monkeypatch):
    """A transient available-memory trough is recoverable, unlike RSS/swap hard stops."""
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    events: list[dict[str, object]] = []
    emitted: list[tuple[str, FactorExecutionResult]] = []

    class _Queue:
        def __init__(self):
            self.release_results = False
            self.items = [
                ("factor_a", FactorExecutionResult(factor_name="factor_a", success=True)),
                ("factor_b", FactorExecutionResult(factor_name="factor_b", success=True)),
            ]

        def get_nowait(self):
            if not self.release_results or not self.items:
                raise queue.Empty()
            return self.items.pop(0)

        def close(self):
            return None

        def join_thread(self):
            return None

    fake_queue = _Queue()
    processes: list[object] = []

    class _Process:
        next_pid = 100

        def __init__(self, *args, **kwargs):
            self.pid = _Process.next_pid
            _Process.next_pid += 1
            self.terminated = False
            processes.append(self)

        def start(self):
            return None

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return None

    class _Context:
        Process = _Process

        @staticmethod
        def Queue():
            return fake_queue

    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = events.append
    service._create_worker_frame_dir = lambda **kwargs: None
    decisions = [
        ResourceGateDecision(
            False,
            "available_memory_below_minimum",
            {"reason": "available_memory_below_minimum", "available_mb": 4096},
        ),
        ResourceGateDecision(True, None, {"available_mb": 16384}),
    ]

    def _resource_gate(*args, **kwargs):
        decision = decisions.pop(0)
        if not decision.ok:
            fake_queue.release_results = True
        return decision

    service._check_resource_gate = _resource_gate
    monkeypatch.setattr(svc.multiprocessing, "get_context", lambda method: _Context())
    monkeypatch.setattr(svc, "DEFAULT_RESOURCE_POLL_SEC", 0.0)

    result = service._compute_batch_frames_with_process_timeouts(
        object(),
        [
            {"factor_name": "factor_a", "code_text": "result = 1"},
            {"factor_name": "factor_b", "code_text": "result = 1"},
        ],
        max_workers=2,
        timeout_sec=60,
        task_id="task-recovery",
        batch_id="batch-recovery",
        swap_baseline_mb=0.0,
        result_handler=lambda name, value: emitted.append((name, value)),
    )

    assert result == {}
    assert [name for name, value in emitted if value.success] == ["factor_a", "factor_b"]
    assert all(not proc.terminated for proc in processes)
    assert any(event["type"] == "resource_concurrency_reduced" for event in events)
    assert service._last_batch_resource_failure_detail is None


def test_select_batch_workers_throttles_when_available_memory_headroom_is_low(monkeypatch):
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=8192,
        swap_growth_hard_stop_mb=10**9,
    )
    monkeypatch.setenv("AISTOCK_OFFICIAL_FACTOR_ESTIMATED_WORKER_MB", "8192")

    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 70000, 90)) == 4
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 50000, 90)) == 4
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 39000, 90)) == 3
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 20000, 90)) == 1


def test_select_batch_workers_default_uses_conservative_observed_headroom(monkeypatch):
    monkeypatch.delenv("AISTOCK_OFFICIAL_FACTOR_ESTIMATED_WORKER_MB", raising=False)
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=8192,
        swap_growth_hard_stop_mb=10**9,
    )

    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 60000, 90)) == 4
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 33000, 90)) == 2


def test_batch_compute_precomputes_metrics_in_worker_when_context_available(monkeypatch):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    monkeypatch.setattr(svc.multiprocessing, "get_all_start_methods", lambda: [])
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    metric_calls: list[str] = []

    class _Executor:
        def compute_batch(self, batch):
            raise AssertionError("workers > 1 must not use sequential compute_batch")

        def compute_factor(self, factor_name, code_text):
            return FactorExecutionResult(
                factor_name=factor_name,
                success=True,
                dataframe=_base_df()[["close"]].rename(columns={"close": "value"}),
            )

    def _metrics(name, df, ctx):
        metric_calls.append(name)
        assert list(df.columns) == [name]
        assert ctx == {"ctx": True}
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    result = service._compute_batch_frames(
        _Executor(),
        [
            {"factor_name": "factor_a", "code_text": "a"},
            {"factor_name": "factor_b", "code_text": "b"},
        ],
        workers=2,
        timeout_per_factor=1800,
        metrics_ctx={"ctx": True},
        compute_single_factor_metrics=_metrics,
        eligible_index=_base_df().index,
    )

    assert sorted(metric_calls) == ["factor_a", "factor_b"]
    assert result["factor_a"].success is True
    assert result["factor_a"].official_metric_result == {"metrics": {"full": {"monthly_ic_series": []}}}
    assert result["factor_a"].official_metric_elapsed_sec >= 0


def test_process_batch_does_not_pass_metric_context_to_fork_workers_by_default(monkeypatch):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=0,
        swap_growth_hard_stop_mb=10**9,
    )
    service._emit = lambda *args, **kwargs: None
    service._check_resource_gate = lambda *args, **kwargs: ResourceGateDecision(True, None, {})
    captured: dict[str, object] = {}

    class _Process:
        pid = 12345

        def __init__(self, *args, **kwargs):
            captured["args"] = kwargs["args"]
            self.alive = False

        def start(self):
            pass

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

    class _Queue:
        def get_nowait(self):
            raise queue.Empty

        def close(self):
            pass

        def join_thread(self):
            pass

    class _Context:
        def Queue(self):
            return _Queue()

        Process = _Process

    monkeypatch.delenv("AISTOCK_OFFICIAL_FACTOR_WORKER_METRIC_PRECOMPUTE", raising=False)
    monkeypatch.setattr(svc.os, "name", "posix")
    monkeypatch.setattr(svc.multiprocessing, "get_all_start_methods", lambda: ["fork"])
    monkeypatch.setattr(svc.multiprocessing, "get_context", lambda method: _Context())

    service._compute_batch_frames(
        object(),
        [{"factor_name": "factor_a", "code_text": "result = 1"}],
        workers=1,
        timeout_per_factor=1800,
        metrics_ctx={"large": "ctx"},
        compute_single_factor_metrics=lambda *args, **kwargs: {},
    )

    args = captured["args"]
    assert args[4] is None
    assert args[5] is None


def test_parent_metric_compute_parallelizes_missing_metrics(monkeypatch):
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = None
    monkeypatch.setenv("AISTOCK_OFFICIAL_FACTOR_METRIC_WORKERS", "2")
    monkeypatch.setattr(
        "backend.services.quantevolver.official_factor_batch_compute_service._resource_snapshot",
        lambda *args, **kwargs: ResourceSnapshot(100.0, 80.0, 0.0, 70000.0, 90.0),
    )

    active = 0
    max_active = 0
    lock = threading.Lock()

    def _metrics(name, df, ctx):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    frames = [
        ("factor_a", _base_df()[["close"]].rename(columns={"close": "value"}), None, None),
        ("factor_b", _base_df()[["close"]].rename(columns={"close": "value"}), None, None),
    ]
    db_result = {
        "errors": [],
        "save_failures": [],
        "metric_parent_computed": 0,
        "metric_parent_compute_failures": [],
    }

    result = service._compute_parent_metrics_for_frames(
        frames,
        metrics_error=None,
        metrics_ctx={"ctx": True},
        compute_single_factor_metrics=_metrics,
        db_result=db_result,
        batch_id="batch",
    )

    assert sorted(result) == ["factor_a", "factor_b"]
    assert db_result["metric_parent_computed"] == 2
    assert db_result["save_failures"] == []
    assert max_active == 2


def test_parent_metric_compute_classifies_serial_failures(monkeypatch):
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = None
    monkeypatch.setenv("AISTOCK_OFFICIAL_FACTOR_METRIC_WORKERS", "1")

    def _metrics(name, df, ctx):
        if name == "factor_bad":
            raise RuntimeError("boom")
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    frames = [
        ("factor_good", _base_df()[["close"]].rename(columns={"close": "value"}), None, None),
        ("factor_bad", _base_df()[["close"]].rename(columns={"close": "value"}), None, None),
    ]
    db_result = {
        "errors": [],
        "save_failures": [],
        "metric_parent_computed": 0,
        "metric_parent_compute_failures": [],
    }

    result = service._compute_parent_metrics_for_frames(
        frames,
        metrics_error=None,
        metrics_ctx={"ctx": True},
        compute_single_factor_metrics=_metrics,
        db_result=db_result,
        batch_id="batch",
    )

    assert list(result) == ["factor_good"]
    assert db_result["metric_parent_computed"] == 1
    assert db_result["save_failures"] == ["factor_bad"]
    assert "factor_bad: RuntimeError: boom" in db_result["metric_parent_compute_failures"]


def test_drain_success_frames_keeps_factor_success_when_parent_metric_fails(monkeypatch):
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = None
    monkeypatch.setenv("AISTOCK_OFFICIAL_FACTOR_METRIC_WORKERS", "1")

    def _metrics(name, df, ctx):
        raise RuntimeError("metric failed")

    db_result = {
        "inserted": 0,
        "skipped": 0,
        "errors": [],
        "save_failures": [],
        "metric_precomputed": 0,
        "metric_parent_computed": 0,
        "metric_precompute_failures": [],
        "metric_parent_compute_failures": [],
    }
    results: list[dict[str, object]] = []

    success_delta = service._drain_success_frames(
        [("factor_a", _base_df()[["close"]].rename(columns={"close": "value"}), None, None)],
        results,
        db_result=db_result,
        metrics_error=None,
        metrics_ctx={"ctx": True},
        compute_single_factor_metrics=_metrics,
        calc_batch_id="calc",
        end_date="2026-04-30",
        factor_ids={},
        batch_id="batch",
    )

    assert success_delta == 1
    assert results[0]["success"] is True
    assert db_result["save_failures"] == ["factor_a"]
    assert db_result["metric_parent_computed"] == 0
    assert "factor_a: RuntimeError: metric failed" in db_result["metric_parent_compute_failures"]


def test_compute_aborts_remaining_batches_after_resource_gate_failure(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc
    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    factors = [
        {"factor_name": "factor_a", "code_text": "result = 1"},
        {"factor_name": "factor_b", "code_text": "result = 1"},
        {"factor_name": "factor_c", "code_text": "result = 1"},
        {"factor_name": "factor_d", "code_text": "result = 1"},
    ]

    class _Eligibility:
        def list_eligible_factors(self, **kwargs):
            return factors

    class _Universe:
        def metadata(self, **kwargs):
            return {"universe_key": "official", "index_policy": "st_pit_buy_eligible_reindexed_v1"}

        def build_eligible_index(self, **kwargs):
            return _base_df().index

    class _BaseCache:
        def manifest(self):
            return {"base_data_cache_policy": "load_once_readonly"}

    events: list[dict[str, object]] = []
    service = OfficialFactorBatchComputeService(event_emitter=events.append)
    service._eligibility_service = _Eligibility()
    service._universe_service = _Universe()
    service._load_factor_ids = lambda: {}
    service._resolve_qlib_bin_path = lambda qlib_bin_path: None
    error_meta_calls: list[tuple[object, ...]] = []
    resource_deferred_meta_calls: list[tuple[object, ...]] = []
    service._record_error_meta = lambda *args, **kwargs: error_meta_calls.append(args)
    service._record_resource_deferred_meta = (
        lambda *args, **kwargs: resource_deferred_meta_calls.append(args)
    )

    compute_calls: list[list[str]] = []

    def _compute_batch_frames(executor, batch, **kwargs):
        names = [item["factor_name"] for item in batch]
        compute_calls.append(names)
        if names != ["factor_a", "factor_b"]:
            raise AssertionError("resource gate failure must abort before later batches")
        return {
            name: FactorExecutionResult(
                factor_name=name,
                success=False,
                error=f"{RESOURCE_GATE_FAILED}: available_memory_below_minimum",
                error_type=RESOURCE_GATE_FAILED,
            )
            for name in names
        }

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc.BacktestBaseDataMemoryCache, "load_once", lambda *args, **kwargs: _BaseCache())
    monkeypatch.setattr(
        svc,
        "_resource_snapshot",
        lambda *args, **kwargs: ResourceSnapshot(100.0, 80.0, 0.0, 70000.0, 90.0),
    )
    monkeypatch.setattr(engine, "prepare_shared_context", lambda **kwargs: {"ctx": True})
    monkeypatch.setattr(engine, "compute_single_factor_metrics", lambda *args, **kwargs: {"metrics": {}})
    service._compute_batch_frames = _compute_batch_frames

    result = service.compute({
        "factor_names": [item["factor_name"] for item in factors],
        "factor_data_dir": str(tmp_path),
        "start_date": "2018-08-01",
        "end_date": "2026-04-30",
        "batch_size": 2,
        "workers": 2,
        "timeout_per_factor": 1800,
        "expected_factor_count": 4,
    })

    assert compute_calls == [["factor_a", "factor_b"]]
    assert result["success"] is False
    assert result["fail_count"] == 4
    assert result["runtime_validation"]["checks"]["resource_gate_ok"] is False
    assert result["runtime_validation"]["failure_summary"][RESOURCE_GATE_FAILED] == 4
    assert any(event["type"] == "resource_gate_abort" for event in events)
    assert error_meta_calls == []
    assert [call[0] for call in resource_deferred_meta_calls] == ["factor_a", "factor_b"]


def test_compute_drains_success_frames_incrementally(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc
    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine
    from backend.services.quantevolver import qe_eval_v2_qlib_reader as qlib_reader

    factors = [
        {"factor_name": "factor_a", "code_text": "result = 1"},
        {"factor_name": "factor_b", "code_text": "result = 1"},
        {"factor_name": "factor_c", "code_text": "result = 1"},
    ]

    class _Eligibility:
        def list_eligible_factors(self, **kwargs):
            return factors

    class _Universe:
        def metadata(self, **kwargs):
            return {"universe_key": "official", "index_policy": "st_pit_buy_eligible_reindexed_v1"}

        def build_eligible_index(self, **kwargs):
            return _base_df().index

    class _BaseCache:
        def manifest(self):
            return {"base_data_cache_policy": "load_once_readonly"}

    service = OfficialFactorBatchComputeService()
    service._eligibility_service = _Eligibility()
    service._universe_service = _Universe()
    service._load_factor_ids = lambda: {}
    service._resolve_qlib_bin_path = lambda qlib_bin_path: None
    service._record_error_meta = lambda *args, **kwargs: None
    service._write_single_atomic = lambda name, df: tmp_path / f"{name}.parquet"
    service._update_meta_atomic = lambda *args, **kwargs: None
    service._result_drain_chunk_size = lambda: 1
    close_cache_clears: list[str] = []

    live_frames: list[str] = []
    max_live_frames = 0
    metric_calls: list[str] = []

    def _compute_batch_frames(executor, batch, **kwargs):
        return {
            item["factor_name"]: FactorExecutionResult(
                factor_name=item["factor_name"],
                success=True,
                dataframe=_base_df()[["close"]].rename(columns={"close": "value"}),
            )
            for item in batch
        }

    original_drain = service._drain_success_frames

    def _tracked_drain(frames, *args, **kwargs):
        nonlocal max_live_frames
        live_frames.extend(frame[0] for frame in frames)
        max_live_frames = max(max_live_frames, len(frames))
        try:
            return original_drain(frames, *args, **kwargs)
        finally:
            live_frames.clear()

    def _metrics(name, df, ctx):
        metric_calls.append(name)
        assert len(live_frames) == 1
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    class _MetricWriter:
        def _save_metrics(self, *args, **kwargs):
            return {"inserted": 1, "skipped": 0, "errors": []}

        def _save_monthly_ic(self, *args, **kwargs):
            return None

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc.BacktestBaseDataMemoryCache, "load_once", lambda *args, **kwargs: _BaseCache())
    monkeypatch.setattr(
        svc,
        "_resource_snapshot",
        lambda *args, **kwargs: ResourceSnapshot(100.0, 80.0, 0.0, 70000.0, 90.0),
    )
    monkeypatch.setattr(engine, "prepare_shared_context", lambda **kwargs: {"ctx": True})
    monkeypatch.setattr(engine, "compute_single_factor_metrics", _metrics)
    monkeypatch.setattr(qlib_reader, "clear_close_cache", lambda: close_cache_clears.append("cleared"))
    service._compute_batch_frames = _compute_batch_frames
    service._drain_success_frames = _tracked_drain
    service._metric_writer = lambda: _MetricWriter()
    service._promote_snapshot_if_ready = lambda **kwargs: {"status": "promoted"}

    result = service.compute({
        "factor_names": [item["factor_name"] for item in factors],
        "factor_data_dir": str(tmp_path),
        "start_date": "2018-08-01",
        "end_date": "2026-04-30",
        "batch_size": 3,
        "workers": 1,
        "timeout_per_factor": 1800,
        "expected_factor_count": 3,
    })

    assert result["success"] is True
    assert result["success_count"] == 3
    assert metric_calls == ["factor_a", "factor_b", "factor_c"]
    assert max_live_frames == 1
    assert close_cache_clears == ["cleared"]


def test_compute_reuses_worker_precomputed_metrics(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc
    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    factors = [
        {"factor_name": "factor_a", "code_text": "result = 1"},
        {"factor_name": "factor_b", "code_text": "result = 1"},
    ]

    class _Eligibility:
        def list_eligible_factors(self, **kwargs):
            return factors

    class _Universe:
        def metadata(self, **kwargs):
            return {"universe_key": "official", "index_policy": "st_pit_buy_eligible_reindexed_v1"}

        def build_eligible_index(self, **kwargs):
            return _base_df().index

    class _BaseCache:
        def manifest(self):
            return {"base_data_cache_policy": "load_once_readonly"}

    events: list[dict[str, object]] = []
    service = OfficialFactorBatchComputeService(event_emitter=events.append)
    service._eligibility_service = _Eligibility()
    service._universe_service = _Universe()
    service._load_factor_ids = lambda: {}
    service._resolve_qlib_bin_path = lambda qlib_bin_path: None
    service._record_error_meta = lambda *args, **kwargs: None
    service._write_single_atomic = lambda name, df: tmp_path / f"{name}.parquet"
    service._update_meta_atomic = lambda *args, **kwargs: None

    def _compute_batch_frames(executor, batch, **kwargs):
        result = {}
        for item in batch:
            name = item["factor_name"]
            frame = _base_df()[["close"]].rename(columns={"close": "value"})
            exec_result = FactorExecutionResult(factor_name=name, success=True, dataframe=frame)
            exec_result.official_metric_result = {
                "metrics": {"full": {"factor_name": name, "monthly_ic_series": []}}
            }
            kwargs["result_handler"](name, exec_result)
        return result

    parent_metric_calls: list[str] = []

    def _parent_metrics(name, df, ctx):
        parent_metric_calls.append(name)
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    class _MetricWriter:
        def _save_metrics(self, *args, **kwargs):
            return {"inserted": 1, "skipped": 0, "errors": []}

        def _save_monthly_ic(self, *args, **kwargs):
            return None

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc.BacktestBaseDataMemoryCache, "load_once", lambda *args, **kwargs: _BaseCache())
    monkeypatch.setattr(
        svc,
        "_resource_snapshot",
        lambda *args, **kwargs: ResourceSnapshot(100.0, 80.0, 0.0, 70000.0, 90.0),
    )
    monkeypatch.setattr(engine, "prepare_shared_context", lambda **kwargs: {"ctx": True})
    monkeypatch.setattr(engine, "compute_single_factor_metrics", _parent_metrics)
    service._compute_batch_frames = _compute_batch_frames
    service._metric_writer = lambda: _MetricWriter()
    service._promote_snapshot_if_ready = lambda **kwargs: {"status": "promoted"}

    result = service.compute({
        "factor_names": [item["factor_name"] for item in factors],
        "factor_data_dir": str(tmp_path),
        "start_date": "2018-08-01",
        "end_date": "2026-04-30",
        "batch_size": 2,
        "workers": 2,
        "timeout_per_factor": 1800,
        "expected_factor_count": 2,
    })

    assert result["success"] is True
    assert parent_metric_calls == []
    assert result["db_result"]["metric_precomputed"] == 2
    assert result["db_result"]["metric_parent_computed"] == 0
    assert result["runtime_validation"]["optimization_profile"]["metric_precomputed"] == 2
    assert result["runtime_validation"]["optimization_profile"]["metric_parent_computed"] == 0


def test_compute_records_explicit_metric_precompute_fallback(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc
    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    factors = [{"factor_name": "factor_a", "code_text": "result = 1"}]

    class _Eligibility:
        def list_eligible_factors(self, **kwargs):
            return factors

    class _Universe:
        def metadata(self, **kwargs):
            return {"universe_key": "official", "index_policy": "st_pit_buy_eligible_reindexed_v1"}

        def build_eligible_index(self, **kwargs):
            return _base_df().index

    class _BaseCache:
        def manifest(self):
            return {"base_data_cache_policy": "load_once_readonly"}

    events: list[dict[str, object]] = []
    service = OfficialFactorBatchComputeService(event_emitter=events.append)
    service._eligibility_service = _Eligibility()
    service._universe_service = _Universe()
    service._load_factor_ids = lambda: {}
    service._resolve_qlib_bin_path = lambda qlib_bin_path: None
    service._record_error_meta = lambda *args, **kwargs: None
    service._write_single_atomic = lambda name, df: tmp_path / f"{name}.parquet"
    service._update_meta_atomic = lambda *args, **kwargs: None

    def _compute_batch_frames(executor, batch, **kwargs):
        name = batch[0]["factor_name"]
        exec_result = FactorExecutionResult(
            factor_name=name,
            success=True,
            dataframe=_base_df()[["close"]].rename(columns={"close": "value"}),
        )
        exec_result.official_metric_error = "RuntimeError: worker metric failed"
        kwargs["result_handler"](name, exec_result)
        return {}

    parent_metric_calls: list[str] = []

    def _parent_metrics(name, df, ctx):
        parent_metric_calls.append(name)
        return {"metrics": {"full": {"monthly_ic_series": []}}}

    class _MetricWriter:
        def _save_metrics(self, *args, **kwargs):
            return {"inserted": 1, "skipped": 0, "errors": []}

        def _save_monthly_ic(self, *args, **kwargs):
            return None

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc.BacktestBaseDataMemoryCache, "load_once", lambda *args, **kwargs: _BaseCache())
    monkeypatch.setattr(
        svc,
        "_resource_snapshot",
        lambda *args, **kwargs: ResourceSnapshot(100.0, 80.0, 0.0, 70000.0, 90.0),
    )
    monkeypatch.setattr(engine, "prepare_shared_context", lambda **kwargs: {"ctx": True})
    monkeypatch.setattr(engine, "compute_single_factor_metrics", _parent_metrics)
    service._compute_batch_frames = _compute_batch_frames
    service._metric_writer = lambda: _MetricWriter()
    service._promote_snapshot_if_ready = lambda **kwargs: {"status": "promoted"}

    result = service.compute({
        "factor_names": ["factor_a"],
        "factor_data_dir": str(tmp_path),
        "start_date": "2018-08-01",
        "end_date": "2026-04-30",
        "batch_size": 1,
        "workers": 1,
        "timeout_per_factor": 1800,
        "expected_factor_count": 1,
    })

    assert result["success"] is True
    assert parent_metric_calls == ["factor_a"]
    assert result["db_result"]["metric_parent_computed"] == 1
    assert result["db_result"]["metric_precompute_failures"] == [
        "factor_a: RuntimeError: worker metric failed"
    ]
    assert any(event["type"] == "metric_precompute_fallback" for event in events)


def _raise_empty_once_then_fail(fake_queue):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    fake_queue.get_count += 1
    if fake_queue.get_count == 1:
        raise svc.queue.Empty()
    raise AssertionError("resource gate failures must not drain killed worker queues")


def test_pending_snapshot_does_not_advance_global_cache_window(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    root = tmp_path / "factor_values"
    root.mkdir()
    meta_path = root / "_meta.json"
    meta_path.write_text(
        json.dumps({"as_of_date": "2026-04-30", "data_end": "2026-04-30", "factors": {}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(svc, "OFFICIAL_FACTOR_CACHE_ROOT", root)
    monkeypatch.setattr(svc, "OFFICIAL_FACTOR_CACHE_META_PATH", meta_path)

    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    kwargs = {
        "data_start": "2018-08-01",
        "data_end": "2026-06-30",
        "factor_data_dir": "/home/lc999/data/factor_data",
        "qlib_bin_path": "/home/lc999/data/qlib_bin",
        "universe_meta": {"universe_key": "official"},
        "base_cache_manifest": {"manifest": "ok"},
        "task_id": "task-new-window",
    }

    service._update_meta_atomic({"factor_a": {"status": "ok"}}, **kwargs)
    pending = json.loads(meta_path.read_text(encoding="utf-8"))
    assert pending["as_of_date"] == "2026-04-30"
    assert pending["data_end"] == "2026-04-30"
    assert pending["factors"]["factor_a"]["status"] == "ok"
    assert pending["pending_snapshot"]["target_end"] == "2026-06-30"

    service._update_meta_atomic({}, promote_snapshot=True, **kwargs)
    promoted = json.loads(meta_path.read_text(encoding="utf-8"))
    assert promoted["as_of_date"] == "2026-06-30"
    assert promoted["snapshot_status"] == "complete"
    assert "pending_snapshot" not in promoted


def test_checkpoint_persists_exact_retry_subset_atomically(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    checkpoint_dir = tmp_path / "checkpoints"
    monkeypatch.setattr(svc, "OFFICIAL_FACTOR_CACHE_CHECKPOINT_DIR", checkpoint_dir)
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    cfg = BatchComputeConfig(
        factor_names=["factor_a", "factor_b"],
        factor_data_dir="/home/lc999/data/factor_data",
        start_date="2018-08-01",
        end_date="2026-06-30",
        task_id="task-checkpoint",
        resumed_from_task_id="task-prior",
    )

    checkpoint = service._write_checkpoint_atomic(
        task_id="task-checkpoint",
        status="failed",
        cfg=cfg,
        requested=["factor_a", "factor_b"],
        eligible_names=["factor_a", "factor_b"],
        results=[
            {"name": "factor_a", "success": True},
            {"name": "factor_b", "success": False, "error_type": RESOURCE_GATE_FAILED},
        ],
        retry_factor_names=["factor_b"],
        db_result={"save_failures": []},
        runtime_validation={"resource_failures": [{"reason": "available_memory_below_minimum"}]},
        snapshot_promotion={"status": "not_promoted", "reason": "task_not_successful"},
        start_date="2018-08-01",
        end_date="2026-06-30",
    )

    checkpoint_path = Path(checkpoint["path"])
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint_path.exists()
    assert not list(checkpoint_dir.glob("*.tmp"))
    assert payload["retry_factor_names"] == ["factor_b"]
    assert payload["completed_factor_names"] == ["factor_a"]
    assert payload["resumed_from_task_id"] == "task-prior"


def test_error_meta_update_does_not_blank_top_level_meta(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    meta_path = tmp_path / "_meta.json"
    meta_path.write_text(json.dumps({"data_start": "2018-08-01", "data_end": "2026-04-30", "factors": {}}), encoding="utf-8")
    monkeypatch.setattr(svc, "OFFICIAL_FACTOR_CACHE_META_PATH", meta_path)

    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._active_meta_context = None
    service._record_error_meta("factor_bad", "bad code", "boom")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["data_start"] == "2018-08-01"
    assert meta["data_end"] == "2026-04-30"
    assert meta["factors"]["factor_bad"]["status"] == "error"


def test_resource_gate_deferred_meta_preserves_existing_ok_meta(monkeypatch, tmp_path):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    meta_path = tmp_path / "_meta.json"
    meta_path.write_text(
        json.dumps(
            {
                "data_start": "2018-08-01",
                "data_end": "2026-04-30",
                "factors": {
                    "factor_a": {
                        "status": "ok",
                        "computed_at": "2026-06-24T00:00:00+00:00",
                        "date_range": "2018-08-01~2026-04-30",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(svc, "OFFICIAL_FACTOR_CACHE_META_PATH", meta_path)
    events: list[dict[str, object]] = []
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._event_emitter = events.append

    service._record_resource_deferred_meta(
        "factor_a",
        "result = 1",
        f"{RESOURCE_GATE_FAILED}: available_memory_below_minimum",
    )

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["factors"]["factor_a"]["status"] == "ok"
    assert "error" not in meta["factors"]["factor_a"]
    assert events[-1]["type"] == "resource_deferred_meta_preserved"
    assert events[-1]["previous_status"] == "ok"


def test_worker_dataframe_spill_uses_temp_parquet_instead_of_queue_frame(tmp_path):
    result = FactorExecutionResult(
        factor_name="factor_a",
        success=True,
        dataframe=_base_df()[["close"]].rename(columns={"close": "value"}),
    )

    official_batch_svc._spill_worker_dataframe(result, "factor_a", str(tmp_path))

    assert result.dataframe is None
    frame_path = getattr(result, "dataframe_path")
    assert os.path.exists(frame_path)
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)

    loaded = service._consume_worker_dataframe(result)

    assert loaded["value"].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert not os.path.exists(frame_path)


def test_prepare_shared_context_does_not_retain_raw_close_df(monkeypatch):
    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    monkeypatch.setattr(engine, "read_close_prices", lambda *args, **kwargs: _base_df())

    ctx = engine.prepare_shared_context(
        qlib_bin_path=None,
        start_date="2018-08-01",
        end_date="2018-08-02",
        load_suspend_d=False,
        load_st_pit_mask=False,
    )

    assert "close_df" not in ctx
    assert list(ctx["close_unstacked"].columns) == ["000001.SZ", "000002.SZ"]
