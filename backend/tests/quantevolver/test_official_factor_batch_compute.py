from __future__ import annotations

import json
import os
import threading
import time

import pandas as pd
import pytest

from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
from backend.services.quantevolver import offline_code_text_factor_executor as executor_mod
from backend.services.quantevolver.offline_code_text_factor_executor import FactorExecutionResult
from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
from backend.services.quantevolver.official_factor_batch_compute_service import FACTOR_TIMEOUT
from backend.services.quantevolver.official_factor_batch_compute_service import RESOURCE_GATE_FAILED
from backend.services.quantevolver.official_factor_batch_compute_service import FactorResourceLimits
from backend.services.quantevolver.official_factor_batch_compute_service import OfficialFactorBatchComputeService
from backend.services.quantevolver.official_factor_batch_compute_service import ResourceGateDecision
from backend.services.quantevolver.official_factor_batch_compute_service import ResourceSnapshot


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
        "available_memory_below_minimum",
        {"reason": "available_memory_below_minimum"},
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


def test_select_batch_workers_throttles_when_available_memory_headroom_is_low():
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    service._resource_limits = FactorResourceLimits(
        soft_rss_mb=10**9,
        hard_rss_mb=10**9,
        min_available_mb=8192,
        swap_growth_hard_stop_mb=10**9,
    )

    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 70000, 90)) == 4
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 50000, 90)) == 2
    assert service._select_batch_workers(4, ResourceSnapshot(100, 80, 0, 39000, 90)) == 1


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
    service._record_error_meta = lambda *args, **kwargs: None

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


def _raise_empty_once_then_fail(fake_queue):
    from backend.services.quantevolver import official_factor_batch_compute_service as svc

    fake_queue.get_count += 1
    if fake_queue.get_count == 1:
        raise svc.queue.Empty()
    raise AssertionError("resource gate failures must not drain killed worker queues")


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
