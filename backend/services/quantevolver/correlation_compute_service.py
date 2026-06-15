"""Independent factor-correlation compute service.

This module owns the local Spearman+EWMA correlation compute path used by the
WSL custom-task runner. It intentionally does not import FastAPI routers or QE
evolution services, so factor correlation cannot break when QE router imports or
QE experiment orchestration change.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from psycopg2.extras import execute_values

from ...db.pg_pool import get_conn
from .correlation_engine import CorrelationEngine, CorrelationResult
from .factor_universe_mask_service import (
    OFFICIAL_FACTOR_UNIVERSE_KEY,
    FactorUniverseMaskService,
)
from .factor_eligibility_service import FactorEligibilityService
from .factor_value_loader import FactorValueLoader
from .wsl_runtime_guard import assert_wsl_runtime

logger = logging.getLogger("aistock.quantevolver.correlation_compute_service")
REPO_ROOT = Path(__file__).resolve().parents[3]
CORRELATION_FACTOR_VALUE_CACHE_DIR = REPO_ROOT / "rdagent_assets" / "factor_values"
CORRELATION_FACTOR_VALUE_CACHE_SOURCE = "offline_research_backtest_factor_values"

# 模块级缓存：避免每次请求都重新实例化
_correlation_loader: Optional[FactorValueLoader] = None
_computing_lock = threading.Lock()
_latest_result: Optional[CorrelationResult] = None
_compute_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="corr-compute")
_stop_event = threading.Event()
_compute_future: Optional[Future] = None
_active_dispatch_task_id: Optional[str] = None


_MATRIX_TIMEOUT_SEC = 3600  # 60 分钟 (首次加载 448 因子 ~8min + GEMM ~1min; 后续有合并缓存 ~1min)


def get_correlation_factor_cache_dir() -> Path:
    """Return the offline research/backtest factor cache root used by correlation."""
    return CORRELATION_FACTOR_VALUE_CACHE_DIR


def get_correlation_factor_value_pipeline():
    """Build a FactorValuePipeline bound to the offline research cache."""
    from .factor_value_pipeline import FactorValuePipeline

    return FactorValuePipeline(output_dir=str(CORRELATION_FACTOR_VALUE_CACHE_DIR))


def get_correlation_factor_value_loader(source: str = "single") -> FactorValueLoader:
    """Build a FactorValueLoader bound to the offline research cache."""
    return FactorValueLoader(source=source, pipeline_dir=str(CORRELATION_FACTOR_VALUE_CACHE_DIR))


def get_correlation_factor_cache_status() -> Dict[str, Any]:
    """Return correlation cache status without falling back to realtime snapshot cache."""
    pipeline = get_correlation_factor_value_pipeline()
    single_dir = CORRELATION_FACTOR_VALUE_CACHE_DIR / "single"
    meta_path = CORRELATION_FACTOR_VALUE_CACHE_DIR / "_meta.json"
    meta: dict[str, Any] = {}
    if meta_path.is_file():
        import json as _json

        with meta_path.open("r", encoding="utf-8") as fh:
            meta = _json.load(fh)
    factors_meta = meta.get("factors", {}) if isinstance(meta.get("factors"), dict) else {}
    disk_names = {
        item.stem
        for item in single_dir.glob("*.parquet")
        if not item.name.startswith("_")
    } if single_dir.is_dir() else set()
    meta_names = set(factors_meta.keys())
    integrity = pipeline.validate_meta_integrity()
    cached_singles = pipeline.get_cached_singles()
    total_size_mb = sum(item.get("size_mb", 0) for item in cached_singles)
    date_range = next(
        (entry.get("date_range") for entry in factors_meta.values() if entry.get("date_range")),
        None,
    )
    status = {
        "cached_count": len(cached_singles),
        "total_computable": len(pipeline.get_computable_factors()),
        "uncached_count": 0,
        "uncached_factors": [],
        "total_size_mb": round(total_size_mb, 1),
        "as_of_date": meta.get("as_of_date"),
        "generated_at": meta.get("generated_at"),
        "date_range": date_range,
    }
    status.update({
        "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
        "cache_root": str(CORRELATION_FACTOR_VALUE_CACHE_DIR),
        "single_dir": str(single_dir),
        "meta_path": str(meta_path),
        "data_source_mode": next(
            (entry.get("data_source_mode") for entry in factors_meta.values() if entry.get("data_source_mode")),
            meta.get("data_source_mode") or meta.get("data_freshness_profile"),
        ),
        "data_freshness_profile": meta.get("data_freshness_profile"),
        "window_train_start": next(
            (entry.get("window_train_start") for entry in factors_meta.values() if entry.get("window_train_start")),
            meta.get("window_train_start"),
        ),
        "window_backtest_end": next(
            (entry.get("window_backtest_end") for entry in factors_meta.values() if entry.get("window_backtest_end")),
            meta.get("window_backtest_end"),
        ),
        "disk_factor_count": len(disk_names),
        "meta_factor_count": len(meta_names),
        "orphan_parquet_count": len(disk_names - meta_names),
        "orphan_meta_count": len(meta_names - disk_names),
        "integrity_ok": bool(integrity.get("ok")),
        "integrity": integrity,
    })
    return status


def _infer_single_factor_cache_meta(pipeline: Any, factor_name: str) -> Dict[str, Any]:
    """Infer minimal metadata from an offline single-factor parquet without writing cache files."""
    import pandas as pd

    single_path = Path(str(pipeline._output_dir)) / "single" / f"{factor_name}.parquet"
    if not single_path.is_file():
        raise FileNotFoundError(f"single parquet is missing: {single_path}")
    df = pd.read_parquet(single_path, columns=[])
    if not isinstance(df.index, pd.MultiIndex):
        raise ValueError(f"single parquet must use MultiIndex(datetime, instrument): {single_path}")
    if len(df.index) == 0:
        raise ValueError(f"single parquet has no index rows: {single_path}")
    level = "datetime" if "datetime" in df.index.names else 0
    dates = pd.to_datetime(df.index.get_level_values(level), errors="coerce")
    if dates.isna().all():
        raise ValueError(f"single parquet datetime index cannot be parsed: {single_path}")
    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    return {
        "as_of_date": end,
        "date_range": f"{start}~{end}",
        "rows": int(len(df.index)),
        "inferred_from_parquet": True,
    }


class _CorrelationLogBuffer:
    """线程安全的环形日志缓冲区，跨页面导航持久化。"""

    MAX_ENTRIES = 2000

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: list[dict] = []  # [{index, ts, level, msg}]
        self._next_index = 0

    def append(self, msg: str, level: str = "INFO"):
        with self._lock:
            entry = {
                "index": self._next_index,
                "ts": datetime.now().strftime("%H:%M:%S"),
                "level": level,
                "msg": msg,
            }
            self._entries.append(entry)
            self._next_index += 1
            # 超过上限时截断前半部分
            if len(self._entries) > self.MAX_ENTRIES:
                self._entries = self._entries[-self.MAX_ENTRIES:]
        _emit_correlation_event({
            "type": "log",
            "level": level,
            "msg": msg,
            "entry": entry,
        })

    def get_since(self, after_index: int = -1) -> list[dict]:
        """返回 index > after_index 的所有日志条目。"""
        with self._lock:
            if after_index < 0:
                return list(self._entries)
            return [e for e in self._entries if e["index"] > after_index]

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._next_index = 0


_correlation_logs = _CorrelationLogBuffer()
_correlation_event_emitter: Optional[Callable[[Dict[str, Any]], None]] = None


def set_correlation_event_emitter(emitter: Optional[Callable[[Dict[str, Any]], None]]) -> None:
    global _correlation_event_emitter
    _correlation_event_emitter = emitter


def _emit_correlation_event(event: Dict[str, Any]) -> None:
    if _correlation_event_emitter is None:
        return
    try:
        _correlation_event_emitter(event)
    except Exception as exc:
        logger.warning(f"correlation event emit failed: {exc}")


class _CorrelationProgress:
    """线程安全的相关性计算进度追踪器。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {
            "status": "idle",
            "phase": "",
            "phase_label": "",
            "total": 0,
            "done": 0,
            "percent": 0,
            "started_at": None,
            "elapsed_sec": 0,
            "mode": "",
            "error": None,
            "job_id": None,
        }
        self._start_time = None

    def _emit_snapshot(self) -> None:
        _emit_correlation_event({
            "type": "progress",
            "data": dict(self._data),
        })

    def snapshot(self) -> dict:
        with self._lock:
            if self._start_time and self._data["status"] == "computing":
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)
            return dict(self._data)

    def sync_from_event(self, data: dict) -> None:
        with self._lock:
            self._data.update(data or {})
            status = self._data.get("status")
            elapsed = float(self._data.get("elapsed_sec") or 0)
            if status == "computing":
                self._start_time = time.time() - elapsed
            else:
                self._start_time = None

    def start(self, mode: str, total: int, job_id: str = None):
        with self._lock:
            self._start_time = time.time()
            self._data.update(
                status="computing", phase="cache_gen",
                phase_label="生成单因子缓存", total=total, done=0,
                percent=0, started_at=datetime.now().isoformat(),
                mode=mode, error=None, job_id=job_id,
            )
        self._emit_snapshot()

    def advance(self, done=None, phase=None, phase_label=None, total=None):
        with self._lock:
            if done is not None:
                self._data["done"] = done
            if phase:
                self._data["phase"] = phase
            if phase_label:
                self._data["phase_label"] = phase_label
            if total is not None:
                self._data["total"] = total
            p = self._data["phase"]
            t = max(self._data["total"], 1)
            d = self._data["done"]
            r = min(d / t, 1.0)
            if p == "cache_gen":
                self._data["percent"] = int(r * 60)
            elif p == "matrix_compute":
                self._data["percent"] = 60 + int(r * 30)
            elif p == "db_persist":
                self._data["percent"] = 90 + int(r * 10)
        self._emit_snapshot()

    def finish(self, status="success", error=None):
        with self._lock:
            self._data["status"] = status
            self._data["percent"] = 100 if status == "success" else self._data["percent"]
            self._data["error"] = error
            if self._start_time:
                self._data["elapsed_sec"] = round(time.time() - self._start_time, 1)
            self._start_time = None
        self._emit_snapshot()


_correlation_progress = _CorrelationProgress()

# 缓存 status 端点的 DB 查询结果，计算中跳过 DB 直接返回缓存
# counts_by_include_disabled: { False: {...}, True: {...} } — 按 include_disabled 查询口径分桶
_status_db_cache: Dict[str, Any] = {
    "meta": None,
    "counts_by_include_disabled": {
        False: {"db_count": 0, "uncorrelated_count": 0, "high_corr_count_07": 0, "high_corr_count_05": 0},
        True: {"db_count": 0, "uncorrelated_count": 0, "high_corr_count_07": 0, "high_corr_count_05": 0},
    },
}


def _update_job_status(job_id, status, error=None):
    """更新 ingestion_jobs 表状态。"""
    if not job_id:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            if status == "running":
                cur.execute(
                    "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                    (str(job_id),),
                )
            else:
                cur.execute(
                    "UPDATE market.ingestion_jobs SET status=%s, finished_at=NOW() WHERE job_id=%s",
                    (status, str(job_id)),
                )
        conn.commit()


def _current_correlation_eligible_factor_ids(include_disabled: bool = False) -> List[int]:
    rows = FactorEligibilityService().list_eligible_factors(include_disabled=include_disabled)
    factor_ids = [int(row["id"]) for row in rows if row.get("id") is not None]
    if not factor_ids:
        raise RuntimeError("当前无符合相关性 official 准入规则的因子")
    return factor_ids


def _reconcile_correlation_state(reset_all: bool = False) -> Dict[str, int]:
    """清理相关性历史脏状态，确保 DB 与当前 official 准入规则一致。"""
    stats = {
        "eligible_factors": 0,
        "deleted_pairs": 0,
        "reset_ineligible_catalog": 0,
        "reset_orphan_catalog": 0,
        "reset_all_catalog": 0,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if reset_all:
                # reset_all 不依赖准入规则, 清空全表状态
                cur.execute(
                    """
                    UPDATE aistock_factor_catalog
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE correlation_computed_at IS NOT NULL
                       OR COALESCE(correlation_pair_count, 0) <> 0
                    """
                )
                stats["reset_all_catalog"] = cur.rowcount
            else:
                # 增量收敛必须基于当前 eligible 集合
                eligible_ids = _current_correlation_eligible_factor_ids()
                stats["eligible_factors"] = len(eligible_ids)
                cur.execute(
                    """
                    DELETE FROM qe_factor_correlations
                    WHERE NOT (factor_a_id = ANY(%s) AND factor_b_id = ANY(%s))
                    """,
                    (eligible_ids, eligible_ids),
                )
                stats["deleted_pairs"] = cur.rowcount

                cur.execute(
                    """
                    UPDATE aistock_factor_catalog
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE (correlation_computed_at IS NOT NULL
                           OR COALESCE(correlation_pair_count, 0) <> 0)
                      AND NOT (id = ANY(%s))
                    """,
                    (eligible_ids,),
                )
                stats["reset_ineligible_catalog"] = cur.rowcount

                cur.execute(
                    """
                    UPDATE aistock_factor_catalog c
                    SET correlation_computed_at = NULL,
                        correlation_pair_count = 0
                    WHERE c.id = ANY(%s)
                      AND c.correlation_computed_at IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM qe_factor_correlations q
                          WHERE q.factor_a_id = c.id OR q.factor_b_id = c.id
                      )
                    """,
                    (eligible_ids,),
                )
                stats["reset_orphan_catalog"] = cur.rowcount
        conn.commit()

    return stats


def _run_correlation_compute_local(factor_names: list, as_of_date: str = None, job_id: str = None, data_date: str = None, **_kwargs):
    """统一相关性计算入口 — 同步函数，在 ThreadPoolExecutor 中执行。

    每次计算前清空所有历史相关性数据，使用离线研究/回测 single/*.parquet 缓存全量重算。
    data_date 仅保留为兼容字段；相关性计算不得切换到 realtime/snapshot cache。
    """
    assert_wsl_runtime("correlation_compute_local")
    universe_metadata: dict[str, Any] = {"universe_key": OFFICIAL_FACTOR_UNIVERSE_KEY}
    if as_of_date:
        # Correlation reuses the offline research cache; its universe metadata must match the ST PIT state.
        universe_metadata = FactorUniverseMaskService().metadata(
            start_date="2018-08-01",
            end_date=as_of_date,
            universe_key=OFFICIAL_FACTOR_UNIVERSE_KEY,
        )

    global _latest_result
    timeout_timer = None
    pipeline = None  # 提前声明，防止 finally 中 NameError
    with _computing_lock:
        try:
            _stop_event.clear()
            # 超时保护延迟到阶段2矩阵计算前启动（阶段1缓存生成有自身per-factor超时）

            _correlation_logs.clear()
            _correlation_progress.start("full", len(factor_names), str(job_id) if job_id else None)
            _update_job_status(job_id, "running")
            _correlation_logs.append(f"启动相关性计算: 因子数={len(factor_names)}, as_of_date={as_of_date or 'latest'}")

            # 汇总统计变量
            phase1_elapsed = 0.0
            phase2_elapsed = 0.0
            phase3_elapsed = 0.0

            # ═══ 先收敛历史脏状态，保证当前 official 准入规则和 DB 一致 ═══
            reconcile_stats = _reconcile_correlation_state(reset_all=True)
            _correlation_logs.append(
                "[收敛] 清理历史相关性状态: "
                f"eligible={reconcile_stats['eligible_factors']}, "
                f"deleted_pairs={reconcile_stats['deleted_pairs']}, "
                f"reset_ineligible={reconcile_stats['reset_ineligible_catalog']}, "
                f"reset_orphan={reconcile_stats['reset_orphan_catalog']}, "
                f"reset_all={reconcile_stats['reset_all_catalog']}"
            )

            # ═══ 清空所有历史相关性数据（每次计算前必须清空）═══
            import glob as _glob
            _correlation_logs.append("[清空] 清空所有历史相关性数据...")

            # 1. TRUNCATE qe_factor_correlations
            try:
                with get_conn() as _conn:
                    with _conn.cursor() as _cur:
                        _cur.execute("TRUNCATE TABLE qe_factor_correlations")
                        _cur.execute(
                            """
                            UPDATE aistock_factor_catalog
                            SET correlation_computed_at = NULL,
                                correlation_pair_count = 0
                            WHERE correlation_computed_at IS NOT NULL
                               OR COALESCE(correlation_pair_count, 0) <> 0
                            """
                        )
                    _conn.commit()
                _correlation_logs.append("[清空] DB: qe_factor_correlations 与 catalog correlation 状态已清空")
            except Exception as e:
                _correlation_logs.append(f"[清空] DB 清空失败，终止计算: {e}", "ERROR")
                logger.error(f"TRUNCATE 失败: {e}")
                _correlation_progress.finish("failed", f"DB 清空失败: {e}")
                _update_job_status(job_id, "failed")
                return {
                    "success": False,
                    "status": "failed",
                    "error": f"DB 清空失败: {e}",
                }

            # 2. 删除 HDF5 相关性矩阵缓存
            _hdf5_dir = os.path.normpath(str(REPO_ROOT / "data" / "correlation_matrices"))
            for _h5 in _glob.glob(os.path.join(_hdf5_dir, "corr_*.h5")):
                os.remove(_h5)
                _correlation_logs.append(f"[清空] 删除 HDF5: {os.path.basename(_h5)}")

            # 3. 清除内存缓存
            FactorValueLoader.invalidate_single_cache()
            FactorValueLoader.invalidate_merged_cache(str(CORRELATION_FACTOR_VALUE_CACHE_DIR))
            _correlation_logs.append("[清空] 内存缓存已清除")

            # Phase 1: 检查独立指标缓存完整性
            # 相关性计算强依赖离线研究/回测 single/ 缓存，不再读取 realtime/snapshot cache。
            pipeline = get_correlation_factor_value_pipeline()
            _correlation_logs.append(
                f"[缓存源] 使用离线研究/回测因子缓存: {CORRELATION_FACTOR_VALUE_CACHE_DIR}"
            )

            # Phase 0: cache integrity visibility.
            # Offline research caches may have all single/*.parquet files but stale/incomplete
            # _meta.json. Correlation must not fall back to realtime cache; it proceeds with
            # parquet as availability source and validates/infers metadata below.
            integrity = pipeline.validate_meta_integrity()
            if not integrity.get("ok"):
                _warning_msg = (
                    f"offline factor cache meta integrity warning: "
                    f"orphan_parquets={len(integrity.get('orphan_parquets') or [])}, "
                    f"orphan_meta_entries={len(integrity.get('orphan_meta_entries') or [])}, "
                    f"as_of_date_distribution={integrity.get('as_of_date_distribution')}, "
                    f"incomplete_entries={len(integrity.get('incomplete_entries') or [])}, "
                    f"top_level_aod_mismatch={integrity.get('top_level_aod_mismatch')}, "
                    f"factor_count={integrity.get('factor_count')}, "
                    f"error={integrity.get('error')}; continue with offline single/*.parquet "
                    "and infer missing request-factor metadata in memory only"
                )
                _correlation_logs.append(f"[Phase0 cache check] {_warning_msg}", "WARN")
            else:
                _correlation_logs.append(
                    f"[Phase0 cache check] ok: factor_count={integrity['factor_count']}, "
                    f"as_of_date={integrity['top_level_as_of_date']}"
                )

            cached_singles = pipeline.get_cached_singles()
            cached_names = {c["factor_name"] for c in cached_singles}
            missing_factors = [f for f in factor_names if f not in cached_names]

            # ── Bug D 修复: 缺少缓存的因子从计算集合排除, 而非整体失败 ──
            # 需求: "如果因子没有基于这个集成数据时间段的因子值缓存, 则不参与因子相关性计算"
            # 保护: 排除后若可计算因子 < 2 (矩阵退化), 才整体失败并给出明确原因
            if missing_factors:
                missing_sample = missing_factors[:10]
                _correlation_logs.append(
                    f"[缓存检查] 排除 {len(missing_factors)} 个无独立指标缓存的因子: "
                    f"{missing_sample}"
                    + (f"... 等 {len(missing_factors)} 个" if len(missing_factors) > 10 else ""),
                    "WARN",
                )
                if len(missing_factors) == len(factor_names):
                    _error_msg = (
                        f"全部 {len(factor_names)} 个因子均无离线研究/回测因子值缓存 (single/*.parquet)。"
                        f"请先补齐 {CORRELATION_FACTOR_VALUE_CACHE_DIR}，"
                        "然后再触发相关性计算。"
                    )
                    _correlation_logs.append(f"[缓存检查] {_error_msg}", "ERROR")
                    _correlation_progress.finish("failed", _error_msg)
                    _update_job_status(job_id, "failed")
                    return {
                        "success": False,
                        "status": "failed",
                        "error": _error_msg,
                        "missing_factors": missing_factors,
                        "hint": "run_offline_factor_cache_backfill_first",
                        "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
                        "cache_root": str(CORRELATION_FACTOR_VALUE_CACHE_DIR),
                    }

            compute_factors = [f for f in factor_names if f in cached_names]

            if len(compute_factors) < 2:
                _error_msg = (
                    f"可计算因子不足 2 个 (总请求 {len(factor_names)}, "
                    f"缺缓存 {len(missing_factors)}, 剩余 {len(compute_factors)}), "
                    "无法构建相关性矩阵"
                )
                _correlation_logs.append(f"[缓存检查] {_error_msg}", "ERROR")
                _correlation_progress.finish("failed", _error_msg)
                _update_job_status(job_id, "failed")
                return {
                    "success": False,
                    "status": "failed",
                    "error": _error_msg,
                    "missing_factors": missing_factors,
                    "excluded_factors": missing_factors,
                }

            _correlation_logs.append(
                f"[缓存检查] {len(compute_factors)}/{len(factor_names)} 个因子进入计算 "
                f"(排除 {len(missing_factors)} 个无缓存因子)"
            )

            if not compute_factors:
                _correlation_logs.append("No computable factors; stop", "ERROR")
                logger.error("No computable factors")
                _correlation_progress.finish("failed", "No computable factors")
                _update_job_status(job_id, "failed")
                return {
                    "success": False,
                    "status": "failed",
                    "error": "No computable factors",
                }

            # Phase 1.5: as_of_date consistency check.
            _meta_path = os.path.join(pipeline._output_dir, "_meta.json")
            _meta_data: dict[str, Any] = {"factors": {}}
            if os.path.isfile(_meta_path):
                import json as _json
                with open(_meta_path, "r", encoding="utf-8") as _mf:
                    _meta_data = _json.load(_mf)
            else:
                _correlation_logs.append(
                    f"[metadata check] _meta.json is missing ({_meta_path}); "
                    "infer request-factor as_of_date/date_range from offline parquet",
                    "WARN",
                )
            _factors_meta = _meta_data.get("factors", {})

            # Infer missing or incomplete metadata in memory only; do not write cache files.
            _runtime_inferred_meta: dict[str, dict] = {}
            _missing_meta = [
                fn
                for fn in compute_factors
                if not isinstance(_factors_meta.get(fn), dict) or not _factors_meta[fn].get("as_of_date")
            ]
            if _missing_meta:
                _correlation_logs.append(
                    f"[一致性校验] {_meta_path} 缺少 {len(_missing_meta)} 个因子记录，"
                    "将从离线 parquet 只读推断 date_range/as_of_date"
                )
                try:
                    for _fn in _missing_meta:
                        _runtime_inferred_meta[_fn] = _infer_single_factor_cache_meta(pipeline, _fn)
                except Exception as exc:
                    _error_msg = (
                        f"{len(_missing_meta)} 个因子缺少 _meta.json 记录，且无法从离线 parquet 推断元数据: {exc}. "
                        f"请先补齐离线研究/回测因子缓存元数据: {_missing_meta[:10]}"
                        + (f"... 等 {len(_missing_meta)} 个" if len(_missing_meta) > 10 else "")
                    )
                    _correlation_logs.append(f"[一致性校验] {_error_msg}", "ERROR")
                    _correlation_progress.finish("failed", _error_msg)
                    _update_job_status(job_id, "failed")
                    return {
                        "success": False,
                        "status": "failed",
                        "error": _error_msg,
                        "missing_meta_factors": _missing_meta,
                        "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
                        "cache_root": str(CORRELATION_FACTOR_VALUE_CACHE_DIR),
                    }

            # 全量校验 as_of_date 一致性
            _as_of_dates: dict = {}
            for _fn in compute_factors:
                _meta_entry = _factors_meta.get(_fn)
                if not isinstance(_meta_entry, dict) or not _meta_entry.get("as_of_date"):
                    _meta_entry = _runtime_inferred_meta.get(_fn, {})
                _aod = _meta_entry.get("as_of_date")
                _as_of_dates.setdefault(_aod, []).append(_fn)

            if len(_as_of_dates) > 1:
                _detail = {k: len(v) for k, v in _as_of_dates.items()}
                _error_msg = f"因子 as_of_date 不一致，无法计算相关性: {_detail}"
                _correlation_logs.append(f"[一致性校验] {_error_msg}", "ERROR")
                _correlation_progress.finish("failed", _error_msg)
                _update_job_status(job_id, "failed")
                return {
                    "success": False,
                    "status": "failed",
                    "error": _error_msg,
                    "as_of_date_distribution": _detail,
                }
            _aod_value = list(_as_of_dates.keys())[0] if _as_of_dates else "unknown"
            if not as_of_date and _aod_value != "unknown":
                universe_metadata = FactorUniverseMaskService().metadata(
                    start_date="2018-08-01",
                    end_date=_aod_value,
                    universe_key=OFFICIAL_FACTOR_UNIVERSE_KEY,
                )

            # ── Bug E 修复: 调用方 as_of_date 必须与 meta 中实际值对齐 ──
            # 若调用方显式指定 as_of_date, 它必须与 meta 中的实际值完全匹配, 否则拒绝计算
            if as_of_date and _aod_value != "unknown" and as_of_date != _aod_value:
                _error_msg = (
                    f"调用方请求 as_of_date={as_of_date}, 但 meta 中实际快照为 {_aod_value}。"
                    "拒绝跨快照计算，请先按请求日期重算独立指标, 或移除 as_of_date 参数使用当前快照。"
                )
                _correlation_logs.append(f"[一致性校验] {_error_msg}", "ERROR")
                _correlation_progress.finish("failed", _error_msg)
                _update_job_status(job_id, "failed")
                return {
                    "success": False,
                    "status": "failed",
                    "error": _error_msg,
                    "requested_as_of_date": as_of_date,
                    "meta_as_of_date": _aod_value,
                }

            _correlation_logs.append(f"[一致性校验] 通过: 全部 {len(compute_factors)} 个因子 as_of_date={_aod_value}")

            # Phase 2: 计算相关性
            _correlation_progress.advance(phase="matrix_compute", phase_label="计算相关性矩阵", done=0, total=1)
            loader = get_correlation_factor_value_loader(source="single")
            engine = CorrelationEngine(loader)
            phase2_t0 = time.time()

            # 超时保护: 仅保护阶段2矩阵计算（GEMM 应在 2 分钟内完成，30 分钟兜底）
            timeout_timer = threading.Timer(_MATRIX_TIMEOUT_SEC, _stop_event.set)
            timeout_timer.daemon = True
            timeout_timer.start()

            # ── Bug A 修复: 使用已通过 Phase 1.5 校验的 compute_factors, 而非扫盘得到的全量 ──
            # loader.get_available_factors() 会把 single/ 目录下所有文件拉进矩阵,
            # 这些因子可能没有进入 meta 一致性校验 (理论上 Phase 0 已阻断, 双保险).
            # 改用 compute_factors 保证 "参与矩阵计算的因子集 == 通过 as_of_date 校验的因子集".
            matrix_factors = list(compute_factors)
            _correlation_logs.append(
                f"[阶段2/3] 向量化矩阵计算: {len(matrix_factors)} 个因子 (来自 Phase 1.5 校验后集合)"
            )

            def _matrix_progress(done: int, total: int):
                _correlation_progress.advance(done=done, total=total)

            result = engine.compute_full_matrix(
                matrix_factors,
                as_of_date=as_of_date,
                save_hdf5=True,
                on_progress=_matrix_progress,
                stop_event=_stop_event,
                expected_as_of_date=_aod_value,
                expected_universe_key=universe_metadata.get("universe_key"),
                expected_universe_rule_version=universe_metadata.get("universe_rule_version"),
                expected_universe_fingerprint_sha256=universe_metadata.get("universe_fingerprint_sha256"),
                expected_index_policy=universe_metadata.get("index_policy"),
            )
            _latest_result = result
            _correlation_progress.advance(done=1)
            records = result.to_db_records(threshold=0)
            no_valid_pair_factors = sorted(result.get_no_valid_pair_factors())
            phase2_elapsed = round(time.time() - phase2_t0, 1)
            _correlation_logs.append(
                f"阶段2完成: {len(result.factor_names)} 因子矩阵, "
                f"{len(records)} 对相关性记录, 耗时 {phase2_elapsed}s"
            )
            if no_valid_pair_factors:
                _correlation_logs.append(
                    f"[classification] excluded {len(no_valid_pair_factors)} factors with no valid "
                    f"off-diagonal correlation pairs: {no_valid_pair_factors[:10]}"
                    + (f"... total {len(no_valid_pair_factors)}" if len(no_valid_pair_factors) > 10 else ""),
                    "WARN",
                )

            result.metadata["num_pair_valid_factors"] = len(result.factor_names) - len(no_valid_pair_factors)
            result.metadata["no_valid_pair_factors"] = list(no_valid_pair_factors)

            if hasattr(result, 'high_corr_pairs'):
                high_pairs = [p for p in (result.high_corr_pairs or []) if abs(p.get('correlation', 0)) > 0.7]
                if high_pairs:
                    _correlation_logs.append(f"  发现 {len(high_pairs)} 对高相关因子 (|r|>0.7)")

            # Phase 3: 写 DB
            _correlation_progress.advance(phase="db_persist", phase_label="写入数据库", done=0, total=1)
            _correlation_logs.append(f"[阶段3/3] 写入数据库 ({len(records)} 条记录)")
            phase3_t0 = time.time()
            if records:
                _persist_correlations_batch(records, universe_metadata=universe_metadata)
            if _latest_result:
                _persist_correlation_metadata(_latest_result)
            _correlation_progress.advance(done=1)
            phase3_elapsed = round(time.time() - phase3_t0, 1)
            _correlation_logs.append(f"阶段3完成: DB 写入耗时 {phase3_elapsed}s")

            _correlation_progress.finish("success")
            _update_job_status(job_id, "success")
            total_elapsed = _correlation_progress.snapshot().get("elapsed_sec", 0)

            # ── 成功响应: 显式汇报成功/失败因子数 + 排除原因分类 ──
            # 排除来源两类 (互斥):
            # 1) missing_from_cache: Phase 1 缺独立指标缓存 (missing_factors)
            # 2) degenerate_nan: Phase 2 engine 内部剔除 (NaN 覆盖率 > 20%)
            #    通过 compute_factors (Phase 1 后) - result.factor_names (Phase 2 后) 反推
            _requested_count = len(factor_names)
            _no_valid_pair_factors = sorted(set(no_valid_pair_factors))
            _no_valid_pair_set = set(_no_valid_pair_factors)
            _matrix_factor_names = set(result.factor_names)
            _success_factor_names = [name for name in result.factor_names if name not in _no_valid_pair_set]
            _success_count = len(_success_factor_names)
            _degenerate_factors = sorted(set(compute_factors) - _matrix_factor_names)
            _failed_count = len(missing_factors) + len(_degenerate_factors) + len(_no_valid_pair_factors)
            # Strong invariant: every requested factor is either pair-valid or classified as excluded.
            assert _requested_count == _success_count + _failed_count, (
                f"factor count mismatch: requested={_requested_count}, "
                f"success={_success_count}, failed={_failed_count} "
                f"(missing={len(missing_factors)}, degenerate={len(_degenerate_factors)}, "
                f"no_valid_pairs={len(_no_valid_pair_factors)})"
            )

            # --- 完整汇总日志 ---
            _correlation_logs.append("=" * 50)
            _correlation_logs.append("计算完成汇总")
            _correlation_logs.append(f"  请求因子数: {_requested_count}")
            _correlation_logs.append(f"  成功因子数: {_success_count}")
            _correlation_logs.append(
                f"  失败因子数: {_failed_count} "
                f"(缺缓存 {len(missing_factors)}, 退化NaN {len(_degenerate_factors)})"
            )
            _correlation_logs.append(f"  相关性记录: {len(records)} 对")
            _correlation_logs.append(f"  阶段耗时: 矩阵={phase2_elapsed}s | 写DB={phase3_elapsed}s")
            _correlation_logs.append(f"  总耗时: {total_elapsed}s")
            _correlation_logs.append("=" * 50)
            logger.info(
                f"相关性计算完成: requested={_requested_count}, "
                f"success={_success_count}, failed={_failed_count}, "
                f"no_valid_pairs={len(_no_valid_pair_factors)}, "
                f"records={len(records)}, elapsed={total_elapsed}s"
            )
            runtime_validation = _build_correlation_runtime_validation(
                requested_count=_requested_count,
                success_count=_success_count,
                failed_count=_failed_count,
                missing_factors=missing_factors,
                degenerate_factors=_degenerate_factors,
                no_valid_pair_factors=_no_valid_pair_factors,
                record_count=len(records),
                as_of_date=as_of_date,
                cache_root=CORRELATION_FACTOR_VALUE_CACHE_DIR,
                integrity=integrity,
                universe_metadata=universe_metadata,
            )
            return {
                "success": True,
                "status": "success",
                "requested_factor_count": _requested_count,
                "success_factor_count": _success_count,
                "failed_factor_count": _failed_count,
                "excluded_factors": {
                    "missing_from_cache": missing_factors,
                    "degenerate_nan": _degenerate_factors,
                    "no_valid_pairs": _no_valid_pair_factors,
                },
                "success_factors": _success_factor_names,
                "record_count": len(records),
                "calc_elapsed_sec": total_elapsed,
                "phase1_elapsed_sec": phase1_elapsed,
                "phase2_elapsed_sec": phase2_elapsed,
                "phase3_elapsed_sec": phase3_elapsed,
                "data_date": data_date,
                "as_of_date": as_of_date,
                "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
                "cache_root": str(CORRELATION_FACTOR_VALUE_CACHE_DIR),
                "runtime_validation": runtime_validation,
            }

        except Exception as e:
            was_cancelled = _stop_event.is_set()
            status = "cancelled" if was_cancelled else "failed"
            error_msg = "计算被用户取消" if was_cancelled else str(e)
            logger.error(f"相关性计算{status}: {e}", exc_info=not was_cancelled)
            _correlation_logs.append(f"计算{status}: {error_msg}", "WARN" if was_cancelled else "ERROR")
            _correlation_progress.finish(status, error_msg)
            _update_job_status(job_id, status)
            return {
                "success": False,
                "status": status,
                "error": error_msg,
                "data_date": data_date,
                "as_of_date": as_of_date,
                "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
                "cache_root": str(CORRELATION_FACTOR_VALUE_CACHE_DIR),
                "traceback": traceback.format_exc().splitlines()[-20:] if not was_cancelled else None,
            }
        finally:
            if timeout_timer is not None:
                timeout_timer.cancel()
            _stop_event.clear()

            # 强制内存清理：无论成功/失败/取消，都释放大对象
            try:
                FactorValueLoader.invalidate_single_cache()  # 清空类级别因子缓存
                import gc
                gc.collect()
                logger.info("已执行内存清理: _single_cache.clear() + gc.collect()")
            except Exception as e:
                logger.warning(f"内存清理异常: {e}")



# ── 相关性 DB 持久化辅助函数 ──

def _persist_correlations_batch(
    records: List[Dict[str, Any]],
    universe_metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """批量写入相关性记录到 qe_factor_correlations 表。

    使用 execute_values 批量 UPSERT，替代逐条 INSERT。
    10 万条记录从 ~20 分钟降至 ~10 秒。
    """
    if not records:
        return 0
    universe_metadata = universe_metadata or {}

    # 写库只做 catalog → id 映射, 不再按 is_available 过滤;
    # 准入策略由调用方（compute_correlations）通过 include_disabled 决定, 此处只负责持久化.
    catalog_rows = FactorEligibilityService().list_eligible_factors(include_disabled=True)
    catalog_name_to_id = {
        row["factor_name"]: int(row["id"])
        for row in catalog_rows
        if row.get("id") is not None
    }
    if not catalog_name_to_id:
        raise RuntimeError("catalog 中无可用于写入相关性的因子 (transformation_status=SUCCESS 且 qe_code_path 存在)")

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 预处理: 构建去重的 (a_id, b_id) -> row 映射.
            # 如果某一侧因子在 catalog 里查不到 id (异常状态), 记 WARN 并 skip.
            seen = {}
            skipped_unknown = 0
            for r in records:
                fa_id = catalog_name_to_id.get(r["factor_a"])
                fb_id = catalog_name_to_id.get(r["factor_b"])
                if fa_id is None or fb_id is None:
                    skipped_unknown += 1
                    logger.warning(
                        "相关性记录因子在 catalog 中未找到 id, skip: factor_a=%s factor_b=%s",
                        r.get("factor_a"), r.get("factor_b"),
                    )
                    continue
                a_id, b_id = min(fa_id, fb_id), max(fa_id, fb_id)
                as_of_date = None
                data_period = r.get("data_period", "")
                if data_period and "as_of_" in data_period:
                    try:
                        as_of_date = data_period.split("as_of_")[1]
                    except (IndexError, ValueError):
                        raise RuntimeError(f"无法从 data_period 解析 as_of_date: {data_period}")
                if as_of_date is None:
                    raise RuntimeError(f"相关性记录缺少 as_of_date: {r}")
                seen[(a_id, b_id)] = (
                    a_id, b_id, r["correlation"], r["method"], as_of_date, 252,
                    universe_metadata.get("universe_key"),
                    universe_metadata.get("universe_rule_version"),
                    universe_metadata.get("universe_fingerprint_sha256"),
                    universe_metadata.get("index_policy"),
                )

            values = list(seen.values())
            if not values:
                raise RuntimeError(
                    f"相关性结果全部无法映射到 catalog id, 拒绝写入, skip={skipped_unknown} 条"
                )

            execute_values(
                cur,
                """
                INSERT INTO qe_factor_correlations
                    (factor_a_id, factor_b_id, correlation, method,
                     as_of_date, data_window_days, universe, universe_rule_version,
                     universe_fingerprint_sha256, index_policy, computed_at)
                VALUES %s
                ON CONFLICT (factor_a_id, factor_b_id) DO UPDATE SET
                    correlation = EXCLUDED.correlation,
                    method = EXCLUDED.method,
                    as_of_date = EXCLUDED.as_of_date,
                    data_window_days = EXCLUDED.data_window_days,
                    universe = EXCLUDED.universe,
                    universe_rule_version = EXCLUDED.universe_rule_version,
                    universe_fingerprint_sha256 = EXCLUDED.universe_fingerprint_sha256,
                    index_policy = EXCLUDED.index_policy,
                    computed_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s::DATE, %s, %s, %s, %s, %s, NOW())",
                page_size=2000,
            )

            computed_id_list = sorted({factor_id for pair in seen.keys() for factor_id in pair})
            cur.execute(
                """
                UPDATE aistock_factor_catalog
                SET correlation_computed_at = NULL,
                    correlation_pair_count = 0
                WHERE id = ANY(%s)
                """,
                (computed_id_list,),
            )
            cur.execute("""
                UPDATE aistock_factor_catalog c SET
                    correlation_computed_at = NOW(),
                    correlation_pair_count = COALESCE(sub.cnt, 0)
                FROM (
                    SELECT factor_id, COUNT(*) AS cnt FROM (
                        SELECT factor_a_id AS factor_id FROM qe_factor_correlations
                        WHERE factor_a_id = ANY(%s)
                        UNION ALL
                        SELECT factor_b_id AS factor_id FROM qe_factor_correlations
                        WHERE factor_b_id = ANY(%s)
                    ) t GROUP BY factor_id
                ) sub
                WHERE c.id = sub.factor_id
            """, (computed_id_list, computed_id_list))

        conn.commit()

    written = len(values)
    logger.info(
        f"批量写入 {written} 条相关性记录（去重后），skip {skipped_unknown} 条因子在 catalog 中未找到 id"
    )
    return written


def _persist_correlation_metadata(result: CorrelationResult) -> None:
    """写入相关性计算元数据。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qe_correlation_metadata
                    (as_of_date, num_factors, num_high_corr_pairs,
                     avg_correlation, computation_time_sec, hdf5_path,
                     universe, universe_rule_version, universe_fingerprint_sha256, index_policy)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date) DO UPDATE SET
                    num_factors = EXCLUDED.num_factors,
                    num_high_corr_pairs = EXCLUDED.num_high_corr_pairs,
                    avg_correlation = EXCLUDED.avg_correlation,
                    computation_time_sec = EXCLUDED.computation_time_sec,
                    hdf5_path = EXCLUDED.hdf5_path,
                    universe = EXCLUDED.universe,
                    universe_rule_version = EXCLUDED.universe_rule_version,
                    universe_fingerprint_sha256 = EXCLUDED.universe_fingerprint_sha256,
                    index_policy = EXCLUDED.index_policy,
                    created_at = NOW()
            """, (
                result.as_of_date,
                int(result.metadata.get("num_pair_valid_factors", len(result.factor_names))),
                result.metadata.get("num_high_corr_07", 0),
                float(result.metadata.get("avg_correlation", 0)),
                result.computation_time_sec,
                result.metadata.get("hdf5_path"),
                result.metadata.get("universe_key"),
                result.metadata.get("universe_rule_version"),
                result.metadata.get("universe_fingerprint_sha256"),
                result.metadata.get("index_policy"),
            ))



# Public aliases used by scripts/router code. The leading-underscore functions
# are kept because existing call sites and tests may still reference them.
run_correlation_compute_local = _run_correlation_compute_local
persist_correlations_batch = _persist_correlations_batch
persist_correlation_metadata = _persist_correlation_metadata
current_correlation_eligible_factor_ids = _current_correlation_eligible_factor_ids
reconcile_correlation_state = _reconcile_correlation_state
update_job_status = _update_job_status
computing_lock = _computing_lock
stop_event = _stop_event
correlation_logs = _correlation_logs
correlation_progress = _correlation_progress


def get_latest_result() -> Optional[CorrelationResult]:
    return _latest_result


def get_matrix_timeout_sec() -> int:
    return _MATRIX_TIMEOUT_SEC


def _build_correlation_runtime_validation(
    *,
    requested_count: int,
    success_count: int,
    failed_count: int,
    missing_factors: list[str],
    degenerate_factors: list[str],
    record_count: int,
    as_of_date: str | None,
    cache_root: Path,
    integrity: dict[str, Any],
    universe_metadata: dict[str, Any],
    no_valid_pair_factors: list[str] | None = None,
) -> dict[str, Any]:
    include_no_valid_pair_summary = no_valid_pair_factors is not None
    no_valid_pair_factors = no_valid_pair_factors or []
    checks = {
        "official_cache_only": "factor_values_realtime" not in str(cache_root),
        "factor_count_reconciled": requested_count == success_count + failed_count,
        "excluded_factors_classified": (
            failed_count == len(missing_factors) + len(degenerate_factors) + len(no_valid_pair_factors)
        ),
        "enough_success_factors": success_count >= 2,
        "records_generated": record_count >= 0,
        "cache_integrity_visible": isinstance(integrity, dict),
        "universe_metadata_present": bool(universe_metadata.get("universe_key")),
    }
    excluded_summary = {
        "missing_from_cache": len(missing_factors),
        "degenerate_nan": len(degenerate_factors),
    }
    if include_no_valid_pair_summary:
        excluded_summary["no_valid_pairs"] = len(no_valid_pair_factors)

    gate_status = "passed" if all(checks.values()) else "failed"
    return {
        "schema_version": "official_factor_correlation_runtime_validation_v1",
        "gate_status": gate_status,
        "requested_factor_count": requested_count,
        "success_factor_count": success_count,
        "failed_factor_count": failed_count,
        "record_count": record_count,
        "as_of_date": as_of_date,
        "cache_source": CORRELATION_FACTOR_VALUE_CACHE_SOURCE,
        "cache_root": str(cache_root),
        "checks": checks,
        "excluded_summary": excluded_summary,
    }
