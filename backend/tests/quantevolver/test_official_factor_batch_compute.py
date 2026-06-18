from __future__ import annotations

import json
import threading
import time

import pandas as pd

from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
from backend.services.quantevolver import offline_code_text_factor_executor as executor_mod
from backend.services.quantevolver.offline_code_text_factor_executor import FactorExecutionResult
from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
from backend.services.quantevolver.official_factor_batch_compute_service import OfficialFactorBatchComputeService


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

    result = service._compute_batch_frames(executor, batch, workers=2)

    assert result["factor_a"].success is True
    assert result["factor_b"].success is True
    assert result["factor_a"].dataframe["value"].tolist() == [10.0, 20.0, 30.0, 40.0]
    assert result["factor_b"].dataframe["value"].tolist() == [100.0, 200.0, 300.0, 400.0]


def test_batch_compute_uses_worker_threads_for_factor_values(tmp_path):
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

    result = service._compute_batch_frames(_Executor(), batch, workers=3)

    assert started.is_set()
    assert sorted(result) == ["factor_a", "factor_b", "factor_c"]
    assert sorted(calls) == ["factor_a", "factor_b", "factor_c"]


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
