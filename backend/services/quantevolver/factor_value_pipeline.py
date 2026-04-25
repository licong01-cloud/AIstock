"""
因子值批量计算管线 — 执行已改造因子代码，收集因子值，存储为 Parquet。

Phase A: 小批量验证管线
- 从 aistock_factor_catalog 查询 transformation_status='SUCCESS' 的因子
- 读取改造后因子代码（qe_code_path 或 realtime_code_text）
- 注入 _REALTIME_LOADER + _STATIC_FACTORS_LOADER 执行环境
- 执行 calculate_XXX(instruments, start_date, end_date)
- 合并结果为 MultiIndex DataFrame，存储为 Parquet

Phase B: 单因子 Parquet 缓存（因子相关性计算用）
- compute_single_factor(): 执行单个因子 → 存 single/{name}.parquet
- compute_all_singles(): 遍历全部已改造因子 → 并发执行
- get_cached_singles(): 返回已缓存的单因子列表
"""
from __future__ import annotations

import hashlib
import json
import logging
import gc
import os
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.factor_value_pipeline")

# 因子值存储目录
_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
_REALTIME_OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "rdagent_assets", "factor_values_realtime")
_QE_FACTORS_DIR = os.path.join(_PROJECT_ROOT, "rdagent_assets", "qe_factors")


@dataclass
class FactorComputeResult:
    """单个因子的计算结果。"""
    factor_name: str
    success: bool
    num_rows: int = 0
    nan_rate: float = 0.0
    elapsed_sec: float = 0.0
    error: Optional[str] = None
    error_type: Optional[str] = None
    error_short: Optional[str] = None
    traceback_full: Optional[str] = None
    date_range: Optional[str] = None
    # 附带 meta 信息，由调用方决定何时批量写入（避免并发竞态）
    meta_entry: Optional[Dict[str, Any]] = None
    meta_as_of_date: Optional[str] = None


@dataclass
class BatchComputeResult:
    """批量因子值计算的汇总结果。"""
    total: int
    success: int
    failed: int
    factor_results: List[FactorComputeResult] = field(default_factory=list)
    output_path: Optional[str] = None
    total_elapsed_sec: float = 0.0
    merged_shape: Optional[str] = None
    warmup_timings: Optional[Dict[str, float]] = None  # 快照/缓存预热耗时

    def summary(self) -> Dict[str, Any]:
        result = {
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "success_rate": f"{self.success / self.total * 100:.1f}%" if self.total else "0%",
            "output_path": self.output_path,
            "merged_shape": self.merged_shape,
            "total_elapsed_sec": round(self.total_elapsed_sec, 1),
            "factor_results": [
                {
                    "name": r.factor_name,
                    "success": r.success,
                    "rows": r.num_rows,
                    "nan_rate": f"{r.nan_rate:.1%}",
                    "time": f"{r.elapsed_sec:.1f}s",
                    "error": r.error_short or r.error,
                }
                for r in self.factor_results
            ],
        }
        if self.warmup_timings:
            result["warmup_timings"] = self.warmup_timings
        return result



class FactorValuePipeline:
    """批量执行已改造因子代码，计算因子值并存储。

    复用 factor_code_transformer.py 的执行环境模式:
    - 注入 _REALTIME_LOADER (RealtimeFactorDataLoader)
    - 注入 _STATIC_FACTORS_LOADER (build_static_factors wrapper)
    - 线程隔离 + 超时保护
    """

    def __init__(self, output_dir: Optional[str] = None):
        self._output_dir = output_dir or _REALTIME_OUTPUT_DIR
        self._loader_instance = None
        self._static_loader_instance = None
        self._init_lock = threading.Lock()
        self._meta_lock = threading.Lock()  # 保护 _meta.json 并发读写
        # ── 快照模式 ──
        self._snapshot_static_df: Optional[pd.DataFrame] = None  # 批次内静态因子内存缓存
        self._snapshot_data_date: Optional[str] = None  # 当前快照日期

    def _ensure_loaders(self) -> None:
        """懒初始化数据加载器（线程安全）。"""
        if self._loader_instance is not None:
            return
        with self._init_lock:
            if self._loader_instance is not None:
                return

            from ...data_service.realtime_factor_data_loader import (
                RealtimeFactorDataLoader,
            )
            self._loader_instance = RealtimeFactorDataLoader()

            from ...data_service.qe_data_service import (
                build_static_factors as _build_sf,
            )

            # 捕获 pipeline 实例引用，供 _StaticFactorsLoader 快照检查
            _pipeline_ref = self

            class _StaticFactorsLoader:
                """静态因子加载器 — 磁盘缓存 + 按列读取，避免 10GB+ 内存常驻。

                首次调用: build_static_factors() → 保存为临时 parquet → 释放内存
                后续调用: pyarrow 列裁剪读取，仅加载请求的列 (~200-500MB vs 10GB)
                线程安全: _build_lock 防止多线程同时重建
                持久化: cache_key 写入 JSON 文件，服务重启后可跳过 DB 重建
                快照模式: 当 pipeline._snapshot_static_df 存在时，从内存快照切片
                """
                _TEMP_PARQUET = os.path.join(
                    os.path.dirname(__file__), "..", "..", "..",
                    "rdagent_assets", "factor_values_realtime", "_static_factors_cache.parquet",
                )
                _KEY_FILE = _TEMP_PARQUET + ".key.json"
                _build_lock = threading.Lock()

                def __init__(self):
                    self._cache_key = self._load_persisted_key()

                def _load_persisted_key(self):
                    """从磁盘加载持久化的 cache_key（服务重启后复用）。

                    - 文件不存在视为首次启动 (合法): 返回 None 让调用方走重建路径
                    - 文件存在但损坏: 严禁静默, 必须抛出, 上游决定清理或停止
                    """
                    if not (
                        os.path.isfile(self._KEY_FILE)
                        and os.path.isfile(self._TEMP_PARQUET)
                    ):
                        return None
                    with open(self._KEY_FILE, "r") as f:
                        data = json.load(f)
                    return (
                        tuple(data["instruments"]),
                        data["start_date"],
                        data["end_date"],
                    )

                def _persist_key(self, key):
                    """将 cache_key 持久化到磁盘。"""
                    try:
                        with open(self._KEY_FILE, "w") as f:
                            json.dump({
                                "instruments": list(key[0]) if isinstance(key[0], tuple) else key[0],
                                "start_date": key[1],
                                "end_date": key[2],
                            }, f)
                    except Exception as e:
                        logger.error(f"持久化 cache_key 失败: {e}", exc_info=True)
                        raise

                def load(self, instruments, start_date, end_date, columns=None):
                    # ── 快照模式：从 pipeline 内存缓存切片 ──
                    snap_df = _pipeline_ref._snapshot_static_df
                    if snap_df is not None:
                        sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
                        dates = snap_df.index.get_level_values(0)
                        mask = (dates >= sd) & (dates <= ed)
                        result = snap_df.loc[mask]
                        if result.empty:
                            raise RuntimeError(
                                f"静态因子快照切片结果为空: "
                                f"date_range={start_date}~{end_date}, "
                                f"快照日期范围={dates.min()}~{dates.max()}"
                            )
                        if columns:
                            available = [c for c in columns if c in result.columns]
                            if available:
                                return result[available]
                            logger.warning(
                                f"快照模式: 请求列 {columns} 均不在快照中 "
                                f"(快照列: {list(result.columns[:5])}...共{len(result.columns)}列)"
                            )
                            return result[[]]
                        return result

                    # ── 防护：如果快照曾被注入但被意外清除，不允许静默回退到 DB ──
                    if _pipeline_ref._snapshot_data_date is not None:
                        raise RuntimeError(
                            f"静态因子快照已被清除但 snapshot_data_date="
                            f"{_pipeline_ref._snapshot_data_date} 仍存在。"
                            "拒绝回退到 DB 查询。"
                        )

                    # ── DB 缓存模式：仅用于快照创建 ──
                    key = (
                        tuple(sorted(instruments)) if isinstance(instruments, list) else instruments,
                        str(start_date), str(end_date),
                    )
                    # 线程安全: 只有一个线程可以重建缓存
                    if self._cache_key != key:
                        with self._build_lock:
                            # double-check: 另一个线程可能已完成重建
                            if self._cache_key != key:
                                logger.info("构建静态因子数据 (DB → 临时 parquet)...")
                                df = _build_sf(instruments, start_date, end_date)
                                os.makedirs(os.path.dirname(self._TEMP_PARQUET), exist_ok=True)
                                df.to_parquet(self._TEMP_PARQUET, engine="pyarrow", compression="snappy")
                                mem_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
                                file_mb = os.path.getsize(self._TEMP_PARQUET) / (1024 ** 2)
                                logger.info(
                                    f"静态因子已落盘: {len(df)} 行 × {len(df.columns)} 列, "
                                    f"内存 {mem_mb:.0f}MB → 文件 {file_mb:.0f}MB (已释放内存)"
                                )
                                del df
                                import gc; gc.collect()
                                self._cache_key = key
                                self._persist_key(key)

                    # 按列裁剪读取 (pyarrow column pruning, 仅加载请求的列)
                    df = pd.read_parquet(self._TEMP_PARQUET, columns=columns)
                    # 按日期过滤
                    sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
                    dates = df.index.get_level_values(0)
                    mask = (dates >= sd) & (dates <= ed)
                    return df.loc[mask]

            self._static_loader_instance = _StaticFactorsLoader()
            logger.info("数据加载器初始化完成")

    def warm_caches(
        self,
        instruments: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        data_date: Optional[str] = None,
    ) -> Dict[str, float]:
        """预热数据缓存（快照模式）。

        所有因子批量计算必须使用快照模式，确保数据一致性和横向可比。
        首次调用时从 DB 加载创建快照，后续从磁盘读取。

        Parameters
        ----------
        data_date : 快照日期 (YYYYMMDD)，必填。

        Returns: {"realtime_load": sec, "static_load": sec, ...}

        Raises
        ------
        ValueError : data_date 未指定
        """
        if not data_date:
            raise ValueError(
                "data_date 参数必填。所有因子计算必须使用快照模式，"
                "确保所有因子使用相同数据计算。"
            )
        self._ensure_loaders()
        return self._warm_from_snapshot(data_date, instruments)

    def _warm_from_snapshot(
        self,
        data_date: str,
        instruments: Optional[List[str]] = None,
        snapshot_start_date: Optional[str] = None,
    ) -> Dict[str, float]:
        """从磁盘快照预热数据缓存。首次自动创建快照。

        加载后注入到 RealtimeFactorDataLoader（类级别）和 self._snapshot_static_df，
        批次内所有因子线程共享同一内存数据。
        """
        from .data_snapshot_manager import DataSnapshotManager
        from ...data_service.realtime_factor_data_loader import RealtimeFactorDataLoader

        mgr = DataSnapshotManager()
        timings: Dict[str, float] = {}

        # 1. 快照不存在 → 创建（首次执行，从 DB 加载）
        if not mgr.snapshot_exists(data_date):
            if instruments is None:
                from .evaluation_universe_service import EvaluationUniverseService
                instruments = EvaluationUniverseService().get_official_universe(as_of_date=data_date[:4] + "-" + data_date[4:6] + "-" + data_date[6:8])

            logger.info(f"[快照] 首次创建快照 {data_date}，从 DB 加载数据...")
            t0 = time.time()
            mgr.create_snapshot(data_date, instruments, start_date=snapshot_start_date)
            timings["snapshot_create"] = round(time.time() - t0, 1)
            logger.info(f"[快照] 创建完成: {timings['snapshot_create']}s")
        else:
            logger.info(f"[快照] 快照 {data_date} 已存在，跳过创建")

        # 2. 从磁盘加载到内存（批次内只加载一次）
        t0 = time.time()
        realtime_df = mgr.load_realtime(data_date)
        timings["realtime_load"] = round(time.time() - t0, 1)

        t0 = time.time()
        static_df = mgr.load_static(data_date)
        timings["static_load"] = round(time.time() - t0, 1)

        # 3. 注入到 Loader（类级别，所有实例共享）
        RealtimeFactorDataLoader.set_snapshot(realtime_df, data_date)
        self._snapshot_static_df = static_df
        self._snapshot_data_date = data_date

        logger.info(
            f"[快照] 数据已注入内存: realtime={len(realtime_df)}行 "
            f"({timings['realtime_load']}s), "
            f"static={len(static_df)}行×{len(static_df.columns)}列 "
            f"({timings['static_load']}s)"
        )
        return timings

    def clear_snapshot(self) -> None:
        """清除快照内存缓存，恢复 DB 查询模式。"""
        from ...data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
        RealtimeFactorDataLoader.clear_snapshot()
        self._snapshot_static_df = None
        self._snapshot_data_date = None
        import gc; gc.collect()

    # ── 公共接口 ──

    def get_computable_factors(
        self,
        limit: Optional[int] = None,
        factor_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """查询所有 transformation_status=SUCCESS 且有代码可执行的因子。

        Parameters
        ----------
        limit : 返回数量限制
        factor_types : 按 factor_type 过滤

        Returns
        -------
        List[Dict]: factor_name, source, qe_code_path, factor_type
        """
        conditions = ["transformation_status = 'SUCCESS'"]
        params: list = []

        if factor_types:
            placeholders = ",".join(["%s"] * len(factor_types))
            conditions.append(f"factor_type IN ({placeholders})")
            params.extend(factor_types)

        where = " AND ".join(conditions)
        sql = f"""
            SELECT factor_name, source, qe_code_path, factor_type,
                   last_transformation_at
            FROM aistock_factor_catalog
            WHERE {where}
            ORDER BY factor_name
        """
        if limit:
            sql += f" LIMIT {limit}"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

        results = []
        for row in rows:
            d = dict(zip(cols, row))
            # 确认代码文件存在
            code_path = d.get("qe_code_path")
            if code_path:
                abs_path = os.path.join(_PROJECT_ROOT, code_path)
                d["code_exists"] = os.path.isfile(abs_path)
            else:
                d["code_exists"] = False
            # 时间序列化
            if d.get("last_transformation_at") and hasattr(
                d["last_transformation_at"], "isoformat"
            ):
                d["last_transformation_at"] = d[
                    "last_transformation_at"
                ].isoformat()
            results.append(d)

        return results

    def compute_factor_values(
        self,
        factor_names: Optional[List[str]] = None,
        instruments: Optional[List[str]] = None,
        max_workers: int = 1,
        timeout_per_factor: int = 600,
        save_parquet: bool = True,
        data_date: Optional[str] = None,
        snapshot_start_date: Optional[str] = None,
        on_factor_success: Optional[callable] = None,
    ) -> BatchComputeResult:
        """批量执行因子代码，计算因子值（快照模式）。

        Parameters
        ----------
        data_date : 快照日期 (YYYYMMDD)，必填。所有因子使用相同快照数据。

        Returns
        -------
        BatchComputeResult
        """
        if not data_date:
            raise ValueError("data_date 参数必填，所有因子计算必须使用快照模式。")

        t0 = time.time()
        self._ensure_loaders()

        # 从快照 meta 读取起止日期，确保所有因子使用快照确定的时间范围
        from .data_snapshot_manager import DataSnapshotManager, _parse_data_date, DEFAULT_START_DATE
        mgr = DataSnapshotManager()
        if mgr.snapshot_exists(data_date):
            snap_meta = mgr.load_meta(data_date)
            start_date = snap_meta["start_date"]
            end_date = snap_meta["end_date"]
        else:
            end_date = _parse_data_date(data_date)
            start_date = DEFAULT_START_DATE

        if instruments is None:
            from .evaluation_universe_service import EvaluationUniverseService
            instruments = EvaluationUniverseService().get_official_universe(as_of_date=end_date)
            logger.info(f"使用官方评估股票池: {len(instruments)} 只")

        warmup_timings = self._warm_from_snapshot(data_date, instruments, snapshot_start_date=snapshot_start_date)

        try:
            return self._do_compute_factor_values(
                factor_names=factor_names,
                instruments=instruments,
                start_date=start_date,
                end_date=end_date,
                max_workers=max_workers,
                timeout_per_factor=timeout_per_factor,
                save_parquet=save_parquet,
                t0=t0,
                warmup_timings=warmup_timings,
                on_factor_success=on_factor_success,
            )
        finally:
            self.clear_snapshot()

    def _do_compute_factor_values(
        self,
        factor_names, instruments, start_date, end_date,
        max_workers, timeout_per_factor, save_parquet,
        t0, warmup_timings,
        on_factor_success=None,
    ) -> BatchComputeResult:
        """compute_factor_values 的实际执行逻辑（分离出来以便 try/finally 清理快照）。"""

        # 获取因子列表及其代码路径
        factor_infos = self._resolve_factors(factor_names)
        if not factor_infos:
            return BatchComputeResult(
                total=0, success=0, failed=0,
                total_elapsed_sec=time.time() - t0,
            )

        logger.info(
            f"开始批量因子值计算: {len(factor_infos)} 因子, "
            f"{len(instruments)} 只股票, {start_date}~{end_date}, "
            f"max_workers={max_workers}"
        )

        # 执行计算 — 串行 exec 沙箱模式
        # 基础数据（static_factors + realtime_kline）只加载一次，所有因子共享
        # 每个因子执行后立即写盘并释放 df，内存峰值固定不随因子数量增长
        all_results: List[FactorComputeResult] = []
        success_single_paths: List[str] = []  # 成功因子的 single/ 路径（用于最终合并）

        # single/ 缓存目录 — 每个因子成功后同时写入，供相关性计算复用
        single_dir = os.path.join(self._output_dir, "single")
        os.makedirs(single_dir, exist_ok=True)

        # ── 策略 B: 开工前清理陈旧快照 ──
        # 本次计算以 end_date 为权威 as_of_date, meta 中任何非该值的条目 (以及对应的 single parquet)
        # 必须清除, 否则会导致相关性计算 Phase 1.5 因多快照失败.
        # 保留增量能力: meta 中 as_of_date == end_date 的条目不动 (允许本批次只补算部分因子)
        prune_stats = self._prune_stale_snapshot(expected_as_of_date=end_date)
        if prune_stats["pruned_factors"]:
            logger.warning(
                f"[策略B] 开工前清理陈旧快照: "
                f"expected_as_of_date={end_date}, "
                f"pruned_factors={len(prune_stats['pruned_factors'])}, "
                f"pruned_as_of_dates={prune_stats['pruned_as_of_dates']}, "
                f"样例={prune_stats['pruned_factors'][:10]}"
            )
        else:
            logger.info(
                f"[策略B] 开工前检查通过: meta 中不存在其他快照 (expected={end_date})"
            )

        for i, info in enumerate(factor_infos):
            result, df = self._execute_single_factor(
                factor_name=info["factor_name"],
                code_path=info["abs_code_path"],
                instruments=instruments,
                start_date=start_date,
                end_date=end_date,
                timeout=timeout_per_factor,
            )
            all_results.append(result)
            if df is not None:
                # 立即写 single/{name}.parquet — 相关性计算直接复用
                single_path = os.path.join(single_dir, f"{info['factor_name']}.parquet")
                try:
                    df.to_parquet(single_path)
                    success_single_paths.append(single_path)
                    # 构建 meta_entry — 供 _meta.json 记录因子元数据
                    _dates = df.index.get_level_values(0)
                    result.meta_entry = {
                        "status": "ok",
                        "computed_at": datetime.now().isoformat(),
                        "rows": len(df),
                        "date_range": f"{_dates.min().strftime('%Y-%m-%d')}~{_dates.max().strftime('%Y-%m-%d')}",
                        "as_of_date": end_date,
                        "data_source_mode": "snapshot",
                    }
                    result.meta_as_of_date = end_date
                except Exception as e:
                    logger.error(f"写入 single 缓存失败: {info['factor_name']}: {e}")
                    # 标记为失败 — single/ 文件不存在会影响相关性计算
                    # 清除残留的半写文件（如果存在），避免下次扫描到不完整文件
                    if os.path.isfile(single_path):
                        os.remove(single_path)
                    result = FactorComputeResult(
                        factor_name=info["factor_name"],
                        success=False,
                        error=f"因子计算成功但缓存写入失败: {e}",
                        elapsed_sec=result.elapsed_sec,
                    )
                    all_results[-1] = result  # 替换最后一条
                    del df
                    continue

                # ── 立即写 _meta.json 单条 ──
                # 严格顺序: 先 parquet 落盘 (上一步已完成), 再 meta 落盘
                # 任何失败立即删除 parquet, 保持 disk ↔ meta 强一致
                try:
                    self.flush_meta_single(
                        factor_name=info["factor_name"],
                        meta_entry=result.meta_entry,
                        as_of_date=end_date,
                    )
                except Exception as e:
                    logger.error(
                        f"_meta.json 单条写入失败: {info['factor_name']}: {e}", exc_info=True,
                    )
                    # 严格一致性: meta 失败 → 删除 parquet + 标记因子失败
                    if os.path.isfile(single_path):
                        os.remove(single_path)
                    result = FactorComputeResult(
                        factor_name=info["factor_name"],
                        success=False,
                        error=f"因子计算成功但 meta 写入失败(parquet 已回滚): {e}",
                        elapsed_sec=result.elapsed_sec,
                    )
                    all_results[-1] = result
                    del df
                    continue

                # 回调：立即计算指标 + 入库
                if on_factor_success is not None:
                    try:
                        on_factor_success(info["factor_name"], single_path, df)
                    except Exception as cb_err:
                        logger.error(
                            f"因子回调失败: {info['factor_name']}: {cb_err}", exc_info=True,
                        )
                        # 回调失败必须显式传播到 result: 下游可识别此因子未完成独立指标入库
                        # parquet 与 meta 保留(因子值缓存有效)，但因子整体标记为失败
                        result.success = False
                        result.error = f"指标入库失败: {cb_err}"
                # 立即释放 df — 不在内存中累积
                del df
            # 每 10 个因子做一次 gc，避免中间数据累积
            if (i + 1) % 10 == 0:
                gc.collect()

        # 统计结果 — 不做合并，每个因子独立文件已在循环中写入 single/
        success_count = sum(1 for r in all_results if r.success)
        failed_count = len(all_results) - success_count

        # ── 扫尾一致性校验 ──
        # 循环内每因子已立即 flush_meta_single, 此处仅做双向一致性 reconcile:
        # 1) disk 上有 parquet 但 meta 无条目 → 删除 parquet (孤儿)
        # 2) meta 有条目但 disk 无 parquet → 移除 meta 条目 (孤儿)
        # 任何不一致直接抛出，严禁静默吞错
        reconcile_stats = self.reconcile_meta_and_single(expected_as_of_date=end_date)
        logger.info(
            f"_meta.json reconcile 完成: "
            f"removed_orphan_parquets={reconcile_stats['removed_orphan_parquets']}, "
            f"removed_orphan_meta={reconcile_stats['removed_orphan_meta']}, "
            f"final_factor_count={reconcile_stats['final_factor_count']}"
        )

        # output_path 指向 single/ 目录（而非合并文件）
        output_path = single_dir if success_single_paths else None

        elapsed = time.time() - t0
        batch_result = BatchComputeResult(
            total=len(all_results),
            success=success_count,
            failed=failed_count,
            factor_results=all_results,
            output_path=output_path,
            total_elapsed_sec=round(elapsed, 1),
            warmup_timings=warmup_timings or None,
        )

        logger.info(
            f"批量因子值计算完成: {success_count}/{len(all_results)} 成功, "
            f"耗时 {elapsed:.1f}s"
        )
        return batch_result

    # ── 内部方法 ──

    def _resolve_factors(
        self,
        factor_names: Optional[List[str]],
    ) -> List[Dict[str, str]]:
        """解析因子名到代码路径。

        Returns: [{"factor_name": ..., "abs_code_path": ...}, ...]
        """
        if factor_names:
            # 从 DB 查询指定因子的 qe_code_path
            placeholders = ",".join(["%s"] * len(factor_names))
            sql = f"""
                SELECT factor_name, qe_code_path
                FROM aistock_factor_catalog
                WHERE factor_name IN ({placeholders})
                  AND transformation_status = 'SUCCESS'
                  AND qe_code_path IS NOT NULL
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, factor_names)
                    rows = cur.fetchall()
        else:
            # 全部已改造因子
            sql = """
                SELECT factor_name, qe_code_path
                FROM aistock_factor_catalog
                WHERE transformation_status = 'SUCCESS'
                  AND qe_code_path IS NOT NULL
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()

        results = []
        for factor_name, qe_code_path in rows:
            abs_path = os.path.join(_PROJECT_ROOT, qe_code_path)
            if os.path.isfile(abs_path):
                results.append({
                    "factor_name": factor_name,
                    "abs_code_path": abs_path,
                })
            else:
                logger.warning(
                    f"因子代码文件不存在: {factor_name} -> {abs_path}"
                )

        return results

    # ── 因子执行 ──

    def _execute_single_factor(
        self,
        factor_name: str,
        code_path: str,
        instruments: List[str],
        start_date: str,
        end_date: str,
        timeout: int = 600,
    ) -> tuple[FactorComputeResult, Optional[pd.DataFrame]]:
        """在主进程中通过 exec 沙箱执行因子代码。

        数据通过 self._static_loader_instance 和 RealtimeFactorDataLoader 共享，
        文件只加载一次，所有因子共享同一份内存数据。

        Returns: (FactorComputeResult, DataFrame or None)
        """
        t0 = time.time()
        logger.info(f"开始计算因子: {factor_name}")

        # 读取代码
        try:
            with open(code_path, "r", encoding="utf-8") as f:
                code = f.read()
        except Exception as e:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=f"读取代码失败: {e}",
                error_short=f"读取代码失败: {e}",
                error_type=type(e).__name__,
                traceback_full=traceback.format_exc(),
                elapsed_sec=time.time() - t0,
            ), None

        # 构建 exec 沙箱 — 因子代码只能访问 pd/np/loader，无法污染主进程
        import types
        _patched_pd = types.ModuleType("pandas")
        _patched_pd.__dict__.update(pd.__dict__)
        # monkey-patch read_parquet 拦截静态因子路径
        _original_read_parquet = pd.read_parquet
        _snapshot_static_df = self._snapshot_static_df  # 快照模式下的内存数据

        def _patched_read_parquet(path_or_buf, **kwargs):
            path_str = str(path_or_buf)
            if "static_factors" in path_str and _snapshot_static_df is not None:
                cols = kwargs.get("columns", None)
                if cols:
                    available = [c for c in cols if c in _snapshot_static_df.columns]
                    return _snapshot_static_df[available] if available else _snapshot_static_df[[]]
                return _snapshot_static_df
            return _original_read_parquet(path_or_buf, **kwargs)

        _patched_pd.read_parquet = _patched_read_parquet

        sandbox = {
            "__name__": "__factor_pipeline__",
            "__builtins__": __builtins__,
            "pd": _patched_pd,
            "np": np,
            "_REALTIME_LOADER": self._loader_instance,
            "_STATIC_FACTORS_LOADER": self._static_loader_instance,
        }

        # 编译并执行因子代码
        # 保存沙箱注入的 loader 引用 — 因子代码可能重新定义同名变量覆盖它们
        _injected_realtime_loader = sandbox["_REALTIME_LOADER"]
        _injected_static_loader = sandbox["_STATIC_FACTORS_LOADER"]
        try:
            compiled = compile(code, f"<factor_{factor_name}>", "exec")
            exec(compiled, sandbox)
            # 恢复沙箱注入的 loader（防止因子代码顶部 import 覆盖快照版本）
            sandbox["_REALTIME_LOADER"] = _injected_realtime_loader
            sandbox["_STATIC_FACTORS_LOADER"] = _injected_static_loader
        except Exception as e:
            elapsed = time.time() - t0
            logger.warning(f"因子代码编译/定义失败: {factor_name}: {e}")
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=f"代码编译/定义失败: {e}",
                error_short=f"代码编译/定义失败: {e}",
                error_type=type(e).__name__,
                traceback_full=traceback.format_exc(),
                elapsed_sec=elapsed,
            ), None

        # 找到 calculate_xxx 函数
        calc_funcs = [k for k in sandbox if k.startswith("calculate_")]
        if not calc_funcs:
            elapsed = time.time() - t0
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error="因子代码中未找到 calculate_* 函数",
                elapsed_sec=elapsed,
            ), None

        # 执行因子计算（带超时保护，兼容主线程和子线程）
        try:
            _timed_out = False

            def _timeout_flag():
                nonlocal _timed_out
                _timed_out = True

            timer = threading.Timer(timeout, _timeout_flag)
            timer.start()
            try:
                result_df = sandbox[calc_funcs[0]](instruments, start_date, end_date)
            finally:
                timer.cancel()
            if _timed_out:
                raise TimeoutError(f"因子执行超时(>{timeout}s)")
        except Exception as e:
            elapsed = time.time() - t0
            logger.warning(f"因子执行失败: {factor_name}: {e}")
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=str(e),
                error_short=str(e)[:200],
                error_type=type(e).__name__,
                traceback_full=traceback.format_exc(),
                elapsed_sec=elapsed,
            ), None
        finally:
            # 清理沙箱，释放因子中间变量
            sandbox.clear()
            del sandbox

        elapsed = time.time() - t0

        if result_df is None:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error="因子代码返回 None",
                elapsed_sec=elapsed,
            ), None

        # 验证结果
        validation = self._validate_result(result_df, factor_name)
        if validation is not None:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=validation,
                elapsed_sec=elapsed,
            ), None

        # 提取因子列
        if isinstance(result_df, pd.DataFrame):
            factor_col = result_df.columns[0]
            series = result_df[factor_col]
        elif isinstance(result_df, pd.Series):
            series = result_df
        else:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=f"返回类型不支持: {type(result_df).__name__}",
                elapsed_sec=elapsed,
            ), None

        # 构造统一的 DataFrame
        df_out = pd.DataFrame({factor_name: series.astype("float64")})
        df_out.index = series.index
        if not isinstance(df_out.index, pd.MultiIndex):
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error="结果索引不是 MultiIndex(datetime, instrument)",
                elapsed_sec=elapsed,
            ), None

        nan_rate = float(df_out[factor_name].isna().mean())
        dates = df_out.index.get_level_values(0)
        date_range = f"{dates.min().strftime('%Y-%m-%d')}~{dates.max().strftime('%Y-%m-%d')}"

        # 释放中间数据
        del result_df

        logger.info(
            f"因子计算成功: {factor_name}, "
            f"{len(df_out)} 行, NaN率={nan_rate:.1%}, "
            f"耗时 {elapsed:.1f}s"
        )

        return FactorComputeResult(
            factor_name=factor_name,
            success=True,
            num_rows=len(df_out),
            nan_rate=round(nan_rate, 4),
            elapsed_sec=round(elapsed, 1),
            date_range=date_range,
        ), df_out

    def _validate_result(
        self,
        result: Any,
        factor_name: str,
    ) -> Optional[str]:
        """验证因子计算结果，返回错误信息或 None。"""
        if result is None:
            return "因子函数返回 None"

        if isinstance(result, pd.DataFrame):
            if result.empty:
                return "因子函数返回空 DataFrame"
            # 检查是否全 NaN
            data_cols = [
                c for c in result.columns
                if c not in ("datetime", "instrument")
            ]
            if not data_cols:
                return "返回的 DataFrame 没有有效数据列"
            if result[data_cols].isna().all().all():
                return "因子计算结果全部为 NaN"
            return None

        if isinstance(result, pd.Series):
            if result.empty:
                return "因子函数返回空 Series"
            if result.isna().all():
                return "因子计算结果全部为 NaN"
            return None

        return f"返回类型不支持: {type(result).__name__}"


    def get_cached_parquets(self) -> List[Dict[str, Any]]:
        """列出已缓存的因子值 Parquet 文件。"""
        if not os.path.isdir(self._output_dir):
            return []
        results = []
        for f in sorted(os.listdir(self._output_dir), reverse=True):
            if f.startswith("batch_") and f.endswith(".parquet"):
                fpath = os.path.join(self._output_dir, f)
                stat = os.stat(fpath)
                results.append({
                    "filename": f,
                    "path": fpath,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2),
                    "modified": datetime.fromtimestamp(
                        stat.st_mtime
                    ).isoformat(),
                })
        return results

    # ── 单因子 Parquet 缓存（因子相关性计算用） ──

    def _single_dir(self) -> str:
        """单因子缓存目录 rdagent_assets/factor_values/single/"""
        d = os.path.join(self._output_dir, "single")
        os.makedirs(d, exist_ok=True)
        return d

    def _meta_path(self) -> str:
        return os.path.join(self._output_dir, "_meta.json")

    def _load_meta(self) -> Dict[str, Any]:
        p = self._meta_path()
        with self._meta_lock:
            if os.path.isfile(p):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"_meta.json 解析失败，将重置: {e}")
                    raise RuntimeError(
                        f"_meta.json 损坏无法解析: {p}, error={e}"
                    ) from e
            return {"factors": {}}

    def _save_meta(self, meta: Dict[str, Any]) -> None:
        """原子写入 _meta.json：tmp → fsync → os.replace。

        保证任何崩溃时刻磁盘上只可能存在完整文件或旧文件，绝不会半截。
        """
        meta["generated_at"] = datetime.now().isoformat()
        meta["factor_count"] = len(meta.get("factors", {}))
        path = self._meta_path()
        tmp_path = path + ".tmp"
        with self._meta_lock:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)

    def _compute_source_hash_raw(self, code_path: str) -> str:
        with open(code_path, "r", encoding="utf-8") as f:
            raw = f.read()
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def flush_meta_batch(self, results: List[FactorComputeResult]) -> int:
        """批量写入多个因子的 meta 信息到 _meta.json（主线程调用，无竞态）。

        Returns: 成功写入的因子数量。
        """
        if not results:
            return 0
        meta = self._load_meta()
        count = 0
        as_of_date = None
        for r in results:
            if r.success and r.meta_entry:
                meta.setdefault("factors", {})[r.factor_name] = r.meta_entry
                as_of_date = r.meta_as_of_date
                count += 1
        if as_of_date:
            meta["as_of_date"] = as_of_date
        if count:
            self._save_meta(meta)
        return count

    def flush_meta_single(
        self,
        factor_name: str,
        meta_entry: Dict[str, Any],
        as_of_date: str,
    ) -> None:
        """单因子立即写入 _meta.json。

        每完成一个因子计算后立即调用，保证：
        1. 即便进程崩溃，已落盘的 single/*.parquet 在 meta 中必有对应条目；
        2. 同一因子名覆盖旧条目，不产生重复；
        3. 顶层 as_of_date 与本条目同步。

        失败必须抛出异常，严禁静默吞错。
        """
        if not factor_name or not meta_entry or not as_of_date:
            raise ValueError(
                f"flush_meta_single 参数不完整: "
                f"factor_name={factor_name!r}, "
                f"meta_entry_keys={list(meta_entry.keys()) if meta_entry else None}, "
                f"as_of_date={as_of_date!r}"
            )
        meta = self._load_meta()
        meta.setdefault("factors", {})[factor_name] = meta_entry
        meta["as_of_date"] = as_of_date
        self._save_meta(meta)

    def _prune_stale_snapshot(self, expected_as_of_date: str) -> Dict[str, Any]:
        """策略 B: 清理 meta 中非 expected_as_of_date 的条目 + 对应 single parquet。

        保留 as_of_date == expected_as_of_date 的条目不动，实现增量补算；
        非 expected 的条目无条件清除，保证 single/ + meta 单一快照。

        Returns
        -------
        {
            "expected_as_of_date": str,
            "pruned_factors": [factor_name, ...],
            "pruned_as_of_dates": {aod: count},
        }

        Raises
        ------
        FileNotFoundError : 需要删除的 parquet 不存在且 meta 条目存在（强一致性断言）
        """
        if not expected_as_of_date:
            raise ValueError("_prune_stale_snapshot 必须传入 expected_as_of_date")

        single_dir = os.path.join(self._output_dir, "single")
        meta = self._load_meta()
        factors = meta.get("factors", {})

        pruned_factors: List[str] = []
        pruned_aods: Dict[str, int] = {}

        for fn in list(factors.keys()):
            entry = factors[fn]
            aod = entry.get("as_of_date")
            if aod != expected_as_of_date:
                # 删除对应 parquet (允许不存在, 此时视为 meta 孤儿)
                pq_path = os.path.join(single_dir, f"{fn}.parquet")
                if os.path.isfile(pq_path):
                    os.remove(pq_path)
                del factors[fn]
                pruned_factors.append(fn)
                pruned_aods[aod] = pruned_aods.get(aod, 0) + 1

        if pruned_factors:
            meta["factors"] = factors
            # 顶层 as_of_date 回写为期望值 (本次计算后会变成该值, 先保证一致)
            meta["as_of_date"] = expected_as_of_date
            self._save_meta(meta)

        return {
            "expected_as_of_date": expected_as_of_date,
            "pruned_factors": pruned_factors,
            "pruned_as_of_dates": pruned_aods,
        }

    def reconcile_meta_and_single(
        self,
        expected_as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """双向对账 single/ ↔ _meta.json，保证强一致。

        1) disk 上有 parquet 但 meta 无条目 → 删除 parquet (孤儿文件)
        2) meta 有条目但 disk 无 parquet → 移除 meta 条目 (孤儿 meta)
        3) 如指定 expected_as_of_date, meta 内出现其他 as_of_date → 抛出异常
           (此阶段不应再有陈旧条目, _prune_stale_snapshot 已处理; 若存在即逻辑缺陷)

        任何 I/O 错误必须抛出, 严禁静默吞错.
        """
        single_dir = os.path.join(self._output_dir, "single")
        if not os.path.isdir(single_dir):
            raise FileNotFoundError(
                f"single 缓存目录不存在: {single_dir}, 无法 reconcile"
            )

        meta = self._load_meta()
        factors = meta.setdefault("factors", {})

        disk_names = {
            f[:-8] for f in os.listdir(single_dir)
            if f.endswith(".parquet") and not f.startswith("_")
        }
        meta_names = set(factors.keys())

        # 1) 孤儿 parquet: disk 有 meta 无
        orphan_parquets = sorted(disk_names - meta_names)
        for fn in orphan_parquets:
            pq_path = os.path.join(single_dir, f"{fn}.parquet")
            os.remove(pq_path)

        # 2) 孤儿 meta: meta 有 disk 无
        orphan_meta = sorted(meta_names - disk_names)
        for fn in orphan_meta:
            del factors[fn]

        # 3) as_of_date 一致性检查
        if expected_as_of_date is not None:
            mismatched = {
                fn: entry.get("as_of_date")
                for fn, entry in factors.items()
                if entry.get("as_of_date") != expected_as_of_date
            }
            if mismatched:
                raise RuntimeError(
                    f"reconcile 发现 as_of_date 不一致 "
                    f"(expected={expected_as_of_date}): "
                    f"count={len(mismatched)}, sample={list(mismatched.items())[:5]}"
                )
            # 顶层 as_of_date 同步为期望值
            meta["as_of_date"] = expected_as_of_date

        # 有写操作时持久化
        if orphan_parquets or orphan_meta or expected_as_of_date:
            self._save_meta(meta)

        return {
            "removed_orphan_parquets": orphan_parquets,
            "removed_orphan_meta": orphan_meta,
            "final_factor_count": len(factors),
        }

    def validate_meta_integrity(self) -> Dict[str, Any]:
        """meta 权威性自检 — 供相关性计算 Phase 0 调用。

        只读, 不修改磁盘。检查项：
        - disk ↔ meta 双向一致 (orphan_parquets / orphan_meta_entries)
        - meta 内所有因子 as_of_date 单值
        - 顶层 as_of_date 与因子记录匹配
        - 每条 meta 必含 computed_at / as_of_date / rows / date_range

        Returns
        -------
        {
            "ok": bool,
            "orphan_parquets": [...],
            "orphan_meta_entries": [...],
            "as_of_date_distribution": {aod: [factors]},
            "top_level_as_of_date": str | None,
            "top_level_aod_mismatch": bool,
            "incomplete_entries": [factor, ...],
            "factor_count": int,
            "meta_path": str,
        }
        """
        single_dir = os.path.join(self._output_dir, "single")
        meta_path = self._meta_path()

        if not os.path.isfile(meta_path):
            return {
                "ok": False,
                "error": f"_meta.json 不存在: {meta_path}",
                "orphan_parquets": [],
                "orphan_meta_entries": [],
                "as_of_date_distribution": {},
                "top_level_as_of_date": None,
                "top_level_aod_mismatch": False,
                "incomplete_entries": [],
                "factor_count": 0,
                "meta_path": meta_path,
            }

        if not os.path.isdir(single_dir):
            return {
                "ok": False,
                "error": f"single 目录不存在: {single_dir}",
                "orphan_parquets": [],
                "orphan_meta_entries": [],
                "as_of_date_distribution": {},
                "top_level_as_of_date": None,
                "top_level_aod_mismatch": False,
                "incomplete_entries": [],
                "factor_count": 0,
                "meta_path": meta_path,
            }

        meta = self._load_meta()
        factors = meta.get("factors", {})
        top_aod = meta.get("as_of_date")

        disk_names = {
            f[:-8] for f in os.listdir(single_dir)
            if f.endswith(".parquet") and not f.startswith("_")
        }
        meta_names = set(factors.keys())

        orphan_parquets = sorted(disk_names - meta_names)
        orphan_meta_entries = sorted(meta_names - disk_names)

        aod_dist: Dict[str, List[str]] = {}
        incomplete: List[str] = []
        required_fields = ("computed_at", "as_of_date", "rows", "date_range")
        for fn, entry in factors.items():
            aod = entry.get("as_of_date")
            aod_dist.setdefault(aod, []).append(fn)
            if any(not entry.get(k) for k in required_fields):
                incomplete.append(fn)

        top_mismatch = False
        if top_aod is not None and len(aod_dist) == 1:
            only_aod = next(iter(aod_dist.keys()))
            top_mismatch = (only_aod != top_aod)

        ok = (
            not orphan_parquets
            and not orphan_meta_entries
            and len(aod_dist) <= 1
            and not incomplete
            and not top_mismatch
            and len(factors) > 0
        )

        return {
            "ok": ok,
            "orphan_parquets": orphan_parquets,
            "orphan_meta_entries": orphan_meta_entries,
            "as_of_date_distribution": {k: len(v) for k, v in aod_dist.items()},
            "as_of_date_factor_sample": {
                k: v[:5] for k, v in aod_dist.items()
            },
            "top_level_as_of_date": top_aod,
            "top_level_aod_mismatch": top_mismatch,
            "incomplete_entries": incomplete,
            "factor_count": len(factors),
            "meta_path": meta_path,
        }

    def get_cached_singles(self) -> List[Dict[str, Any]]:
        """返回已缓存的单因子列表及其元数据。"""
        single_dir = os.path.join(self._output_dir, "single")
        if not os.path.isdir(single_dir):
            return []

        meta = self._load_meta()
        factors_meta = meta.get("factors", {})
        results = []

        for f in sorted(os.listdir(single_dir)):
            if not f.endswith(".parquet") or f.startswith("_"):
                continue
            factor_name = f[:-8]  # 去掉 .parquet
            fpath = os.path.join(single_dir, f)
            stat = os.stat(fpath)
            fm = factors_meta.get(factor_name, {})
            results.append({
                "factor_name": factor_name,
                "filename": f,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "computed_at": fm.get("computed_at"),
                "rows": fm.get("rows"),
                "date_range": fm.get("date_range"),
                "as_of_date": fm.get("as_of_date"),
            })

        return results

    def get_cache_status(self) -> Dict[str, Any]:
        """返回缓存状态概览。"""
        cached = self.get_cached_singles()
        cached_names = {c["factor_name"] for c in cached}

        # 查询所有可计算的因子
        computable = self.get_computable_factors()
        computable_names = {f["factor_name"] for f in computable}

        uncached = sorted(computable_names - cached_names)
        total_size_mb = sum(c.get("size_mb", 0) for c in cached)

        meta = self._load_meta()

        # 从任一因子获取 date_range（所有因子共享相同的日期范围）
        date_range = None
        for fm in meta.get("factors", {}).values():
            if fm.get("date_range"):
                date_range = fm["date_range"]
                break

        return {
            "cached_count": len(cached),
            "total_computable": len(computable),
            "uncached_count": len(uncached),
            "uncached_factors": uncached[:50],
            "total_size_mb": round(total_size_mb, 1),
            "as_of_date": meta.get("as_of_date"),
            "generated_at": meta.get("generated_at"),
            "date_range": date_range,
        }
