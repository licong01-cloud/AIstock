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

import json
import logging
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
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
_DEFAULT_OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "rdagent_assets", "factor_values")
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
    date_range: Optional[str] = None


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

    def summary(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "success_rate": f"{self.success / self.total * 100:.1f}%" if self.total else "0%",
            "output_path": self.output_path,
            "merged_shape": self.merged_shape,
            "total_elapsed_sec": round(self.total_elapsed_sec, 1),
            "factors": [
                {
                    "name": r.factor_name,
                    "success": r.success,
                    "rows": r.num_rows,
                    "nan_rate": f"{r.nan_rate:.1%}",
                    "time": f"{r.elapsed_sec:.1f}s",
                    "error": r.error,
                }
                for r in self.factor_results
            ],
        }


class FactorValuePipeline:
    """批量执行已改造因子代码，计算因子值并存储。

    复用 factor_code_transformer.py 的执行环境模式:
    - 注入 _REALTIME_LOADER (RealtimeFactorDataLoader)
    - 注入 _STATIC_FACTORS_LOADER (build_static_factors wrapper)
    - 线程隔离 + 超时保护
    """

    def __init__(self, output_dir: Optional[str] = None):
        self._output_dir = output_dir or _DEFAULT_OUTPUT_DIR
        self._loader_instance = None
        self._static_loader_instance = None
        self._init_lock = threading.Lock()
        self._meta_lock = threading.Lock()  # 保护 _meta.json 并发读写

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

            class _StaticFactorsLoader:
                """静态因子加载器 — 磁盘缓存 + 按列读取，避免 10GB+ 内存常驻。

                首次调用: build_static_factors() → 保存为临时 parquet → 释放内存
                后续调用: pyarrow 列裁剪读取，仅加载请求的列 (~200-500MB vs 10GB)
                线程安全: _build_lock 防止多线程同时重建
                持久化: cache_key 写入 JSON 文件，服务重启后可跳过 DB 重建
                """
                _TEMP_PARQUET = os.path.join(
                    os.path.dirname(__file__), "..", "..", "..",
                    "rdagent_assets", "factor_values", "_static_factors_cache.parquet",
                )
                _KEY_FILE = _TEMP_PARQUET + ".key.json"
                _build_lock = threading.Lock()

                def __init__(self):
                    self._cache_key = self._load_persisted_key()

                def _load_persisted_key(self):
                    """从磁盘加载持久化的 cache_key（服务重启后复用）。"""
                    try:
                        if os.path.isfile(self._KEY_FILE) and os.path.isfile(self._TEMP_PARQUET):
                            with open(self._KEY_FILE, "r") as f:
                                data = json.load(f)
                            # 还原为 tuple key
                            return (
                                tuple(data["instruments"]),
                                data["start_date"],
                                data["end_date"],
                            )
                    except Exception:
                        pass
                    return None

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
                        logger.warning(f"持久化 cache_key 失败: {e}")

                def load(self, instruments, start_date, end_date, columns=None):
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
    ) -> Dict[str, float]:
        """预热数据缓存：静态因子数据 + 行情数据。

        在批量计算前调用，避免首个因子因冷缓存超时。
        build_static_factors() 需 4-5 分钟（~16M 行 DB 查询），
        必须在因子超时线程（300s）外提前完成。

        Returns: {"static": elapsed_sec, "realtime": elapsed_sec}
        """
        self._ensure_loaders()

        # 使用与 compute_single_factor 相同的默认参数
        if end_date is None:
            from datetime import date
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            from datetime import date, timedelta
            d = date.today() - timedelta(days=730)
            start_date = d.strftime("%Y-%m-%d")
        if instruments is None:
            from ...data_service.qe_data_service import get_instruments_universe
            instruments = get_instruments_universe()

        timings: Dict[str, float] = {}

        # 1. 预热静态因子缓存（build_static_factors: 6 张表，~16M 行，冷缓存 260-300s）
        logger.info(f"预热静态因子缓存: {len(instruments)} 只股票, {start_date}~{end_date}")
        t0 = time.time()
        self._static_loader_instance.load(instruments, start_date, end_date)
        timings["static"] = round(time.time() - t0, 1)
        logger.info(f"静态因子缓存预热完成: {timings['static']}s")

        # 2. 预热行情数据缓存（kline_daily_raw + adj_factor，冷缓存 ~40s）
        logger.info(f"预热行情数据缓存...")
        t0 = time.time()
        self._loader_instance.load(
            instruments=instruments, start_date=start_date,
            end_date=end_date, fields=["open", "close", "high", "low", "volume", "amount", "factor"],
        )
        timings["realtime"] = round(time.time() - t0, 1)
        logger.info(f"行情数据缓存预热完成: {timings['realtime']}s")

        return timings

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
        start_date: str = "2024-12-01",
        end_date: str = "2025-12-01",
        max_workers: int = 1,
        timeout_per_factor: int = 600,
        save_parquet: bool = True,
    ) -> BatchComputeResult:
        """批量执行因子代码，计算因子值。

        Parameters
        ----------
        factor_names : 要计算的因子名列表；None 则计算所有已改造因子
        instruments : 股票池；None 则使用全 A 股
        start_date : 起始日期
        end_date : 截止日期
        max_workers : 并发线程数（建议 1-3，避免 DB 压力）
        timeout_per_factor : 每个因子的超时秒数
        save_parquet : 是否保存合并结果为 Parquet

        Returns
        -------
        BatchComputeResult
        """
        t0 = time.time()
        self._ensure_loaders()

        # 获取股票池
        if instruments is None:
            from ...data_service.qe_data_service import get_instruments_universe
            instruments = get_instruments_universe()
            logger.info(f"使用全 A 股票池: {len(instruments)} 只")

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

        # 执行计算 — 流式合并，避免 all_dfs 累积所有 DataFrame
        all_results: List[FactorComputeResult] = []
        merged_df: Optional[pd.DataFrame] = None

        def _accumulate(fname: str, df: pd.DataFrame):
            """将单因子 df 流式合并到 merged_df，立即释放原始 df。"""
            nonlocal merged_df
            if merged_df is None:
                merged_df = df
            else:
                merged_df = merged_df.join(df, how="outer")

        if max_workers <= 1:
            # 串行执行
            for info in factor_infos:
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
                    _accumulate(info["factor_name"], df)
                    del df
        else:
            # 并发执行
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for info in factor_infos:
                    fut = executor.submit(
                        self._execute_single_factor,
                        factor_name=info["factor_name"],
                        code_path=info["abs_code_path"],
                        instruments=instruments,
                        start_date=start_date,
                        end_date=end_date,
                        timeout=timeout_per_factor,
                    )
                    futures[fut] = info

                for fut in as_completed(futures):
                    info = futures[fut]
                    try:
                        result, df = fut.result()
                    except Exception as e:
                        result = FactorComputeResult(
                            factor_name=info["factor_name"],
                            success=False,
                            error=f"ThreadPool error: {e}",
                        )
                        df = None
                    all_results.append(result)
                    if df is not None:
                        _accumulate(info["factor_name"], df)
                        del df

        # 合并结果
        success_count = sum(1 for r in all_results if r.success)
        failed_count = len(all_results) - success_count

        output_path = None
        merged_shape = None

        if merged_df is not None and save_parquet:
            try:
                merged_df = merged_df.sort_index()
                output_path = self._save_parquet(merged_df, end_date)
                merged_shape = str(merged_df.shape)
                logger.info(
                    f"合并结果: {merged_df.shape}, 保存到 {output_path}"
                )
            except Exception as e:
                logger.error(f"合并/保存 Parquet 失败: {e}")
            finally:
                del merged_df
                import gc; gc.collect()

        elapsed = time.time() - t0
        batch_result = BatchComputeResult(
            total=len(all_results),
            success=success_count,
            failed=failed_count,
            factor_results=all_results,
            output_path=output_path,
            total_elapsed_sec=round(elapsed, 1),
            merged_shape=merged_shape,
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

    def _execute_single_factor(
        self,
        factor_name: str,
        code_path: str,
        instruments: List[str],
        start_date: str,
        end_date: str,
        timeout: int = 600,
    ) -> tuple[FactorComputeResult, Optional[pd.DataFrame]]:
        """执行单个因子代码并返回结果。

        复用 factor_code_transformer.py 的执行模式:
        - 注入 _REALTIME_LOADER, _STATIC_FACTORS_LOADER, pd, np
        - 线程超时保护
        - 结果验证（非空、非全 NaN）

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
                elapsed_sec=time.time() - t0,
            ), None

        # 构建执行环境
        exec_globals = {
            "__name__": "__factor_pipeline__",
            "__builtins__": __builtins__,
            "pd": pd,
            "np": np,
            "_REALTIME_LOADER": self._loader_instance,
            "_STATIC_FACTORS_LOADER": self._static_loader_instance,
        }

        # 使用线程 + 超时控制
        container: Dict[str, Any] = {"result": None, "error": None}

        def _run():
            try:
                compiled = compile(code, f"<factor_{factor_name}>", "exec")
                exec(compiled, exec_globals)

                # 查找 calculate_ 函数
                calc_func_name = f"calculate_{factor_name}"
                if calc_func_name not in exec_globals:
                    calc_funcs = [
                        k for k in exec_globals if k.startswith("calculate_")
                    ]
                    if not calc_funcs:
                        container["error"] = (
                            f"未找到 calculate_{factor_name} 函数"
                        )
                        return
                    calc_func_name = calc_funcs[0]

                container["result"] = exec_globals[calc_func_name](
                    instruments=instruments,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as e:
                container["error"] = (
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        elapsed = time.time() - t0

        if thread.is_alive():
            logger.warning(f"因子执行超时(>{timeout}s): {factor_name}")
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=f"执行超时(>{timeout}s)",
                elapsed_sec=elapsed,
            ), None

        if container["error"]:
            logger.warning(f"因子执行失败: {factor_name}: {container['error'][:200]}")
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=container["error"][:500],
                elapsed_sec=elapsed,
            ), None

        result_df = container["result"]

        # 验证结果
        validation = self._validate_result(result_df, factor_name)
        if validation is not None:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error=validation,
                elapsed_sec=elapsed,
            ), None

        # 提取因子列（结果 DataFrame 可能包含因子名列）
        if isinstance(result_df, pd.DataFrame):
            # 取第一列作为因子值
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

    def _merge_factor_dfs(
        self,
        factor_dfs: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """合并多个因子 DataFrame 为统一面板。

        输入: {factor_name: DataFrame(MultiIndex, 1列)}
        输出: DataFrame(MultiIndex, N列)
        """
        if not factor_dfs:
            return pd.DataFrame()

        dfs = list(factor_dfs.values())
        merged = dfs[0]
        for df in dfs[1:]:
            merged = merged.join(df, how="outer")

        merged = merged.sort_index()
        logger.info(f"因子合并完成: {merged.shape}")
        return merged

    def _save_parquet(
        self,
        df: pd.DataFrame,
        end_date: str,
    ) -> str:
        """保存合并的因子值为 Parquet 文件。"""
        os.makedirs(self._output_dir, exist_ok=True)
        date_tag = end_date.replace("-", "")
        filename = f"batch_{date_tag}.parquet"
        path = os.path.join(self._output_dir, filename)
        df.to_parquet(path, engine="pyarrow", compression="snappy")
        logger.info(f"因子值 Parquet 已保存: {path}")
        return path

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
                except Exception:
                    pass
            return {"factors": {}}

    def _save_meta(self, meta: Dict[str, Any]) -> None:
        meta["generated_at"] = datetime.now().isoformat()
        meta["factor_count"] = len(meta.get("factors", {}))
        with self._meta_lock:
            with open(self._meta_path(), "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)

    def compute_single_factor(
        self,
        factor_name: str,
        instruments: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        timeout: int = 600,
    ) -> FactorComputeResult:
        """执行单个因子代码 → 存为 single/{factor_name}.parquet。

        Parameters
        ----------
        factor_name : 因子名称
        instruments : 股票池；None 则使用全 A 股
        start_date : 起始日期；None 则使用 2 年前
        end_date : 截止日期；None 则使用最新交易日

        Returns
        -------
        FactorComputeResult
        """
        self._ensure_loaders()

        # 默认日期范围
        if end_date is None:
            from datetime import date
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            from datetime import date, timedelta
            d = date.today() - timedelta(days=730)
            start_date = d.strftime("%Y-%m-%d")

        if instruments is None:
            from ...data_service.qe_data_service import get_instruments_universe
            instruments = get_instruments_universe()

        # 解析因子代码路径
        factor_infos = self._resolve_factors([factor_name])
        if not factor_infos:
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                error="未找到因子代码或代码文件不存在",
            )

        info = factor_infos[0]
        result, df = self._execute_single_factor(
            factor_name=info["factor_name"],
            code_path=info["abs_code_path"],
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            timeout=timeout,
        )

        if not result.success or df is None:
            return result

        # 保存为单因子 parquet: single/{factor_name}.parquet
        single_dir = self._single_dir()
        out_path = os.path.join(single_dir, f"{factor_name}.parquet")
        # 重命名列为 value
        save_df = df.rename(columns={df.columns[0]: "value"})
        save_df.to_parquet(out_path, engine="pyarrow", compression="snappy")
        del save_df, df
        import gc; gc.collect()

        # 更新 _meta.json
        meta = self._load_meta()
        meta.setdefault("factors", {})[factor_name] = {
            "computed_at": datetime.now().isoformat(),
            "rows": result.num_rows,
            "date_range": result.date_range,
            "as_of_date": end_date,
        }
        meta["as_of_date"] = end_date
        self._save_meta(meta)

        logger.info(f"单因子缓存已保存: {out_path} ({result.num_rows} 行)")
        return result

    def extend_single_factor_cache(
        self,
        factor_name: str,
        new_end_date: str,
        instruments: Optional[List[str]] = None,
        buffer_days: int = 90,
        timeout: int = 120,
    ) -> FactorComputeResult:
        """增量扩展单因子缓存到新的截止日期。

        只计算现有缓存最后日期之后的新数据。为保证滚动窗口正确，
        增量计算时向前多加载 buffer_days 天缓冲数据，但仅保留新日期的结果。

        Parameters
        ----------
        factor_name : 因子名称
        new_end_date : 目标截止日期 (YYYY-MM-DD)
        instruments : 股票池；None 则使用全 A 股
        buffer_days : 滚动窗口缓冲天数（保证 rolling(N) 等操作正确）
        timeout : 因子执行超时秒数（增量计算应比全量快得多）
        """
        self._ensure_loaders()

        single_dir = self._single_dir()
        parquet_path = os.path.join(single_dir, f"{factor_name}.parquet")

        if not os.path.isfile(parquet_path):
            # 无缓存，需要全量计算
            return self.compute_single_factor(
                factor_name, instruments=instruments,
                end_date=new_end_date, timeout=timeout,
            )

        # 轻量日期检查：只读索引判断是否需要扩展（避免读完整文件）
        idx_only = pd.read_parquet(parquet_path, columns=[])
        last_cached_date = idx_only.index.get_level_values(0).max()
        target_date = pd.Timestamp(new_end_date)

        if target_date <= last_cached_date:
            return FactorComputeResult(
                factor_name=factor_name, success=True,
                num_rows=len(idx_only),
                date_range=f"~{last_cached_date.strftime('%Y-%m-%d')} (已覆盖)",
            )
        del idx_only

        if instruments is None:
            from ...data_service.qe_data_service import get_instruments_universe
            instruments = get_instruments_universe()

        # 计算增量：使用 buffer 天数保证滚动窗口正确
        from datetime import timedelta
        buffer_start = (last_cached_date - timedelta(days=buffer_days)).strftime("%Y-%m-%d")

        factor_infos = self._resolve_factors([factor_name])
        if not factor_infos:
            return FactorComputeResult(
                factor_name=factor_name, success=False,
                error="未找到因子代码或代码文件不存在",
            )

        t0 = time.time()
        result, df_new = self._execute_single_factor(
            factor_name=factor_infos[0]["factor_name"],
            code_path=factor_infos[0]["abs_code_path"],
            instruments=instruments,
            start_date=buffer_start,
            end_date=new_end_date,
            timeout=timeout,
        )

        if not result.success or df_new is None:
            return result

        # 只取新日期的数据（last_cached_date 之后）
        new_dates_mask = df_new.index.get_level_values(0) > last_cached_date
        df_append = df_new.loc[new_dates_mask]

        if df_append.empty:
            return FactorComputeResult(
                factor_name=factor_name, success=True,
                num_rows=0,
                date_range=f"~{last_cached_date.strftime('%Y-%m-%d')} (无新数据)",
                elapsed_sec=round(time.time() - t0, 1),
            )

        # 需要合并时才读取完整缓存文件
        existing_df = pd.read_parquet(parquet_path)
        if "value" in existing_df.columns:
            existing_df = existing_df.rename(columns={"value": factor_name})

        # 合并：existing + new
        merged = pd.concat([existing_df, df_append])
        if not merged.index.is_monotonic_increasing:
            merged = merged.sort_index()
        merged = merged[~merged.index.duplicated(keep='last')]

        # 保存
        save_df = merged.rename(columns={merged.columns[0]: "value"})
        save_df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")
        del save_df

        # 更新 meta
        all_dates = merged.index.get_level_values(0)
        date_range = f"{all_dates.min().strftime('%Y-%m-%d')}~{all_dates.max().strftime('%Y-%m-%d')}"
        num_rows = len(merged)
        new_days = df_append.index.get_level_values(0).nunique()

        # 释放大 DataFrame，防止增量循环中内存累积
        del existing_df, df_new, df_append, merged
        import gc; gc.collect()

        meta = self._load_meta()
        meta.setdefault("factors", {})[factor_name] = {
            "computed_at": datetime.now().isoformat(),
            "rows": num_rows,
            "date_range": date_range,
            "as_of_date": new_end_date,
        }
        self._save_meta(meta)

        elapsed = round(time.time() - t0, 1)
        logger.info(
            f"因子缓存增量扩展: {factor_name}, "
            f"+{new_days} 天, 总 {num_rows} 行, 耗时 {elapsed}s"
        )

        return FactorComputeResult(
            factor_name=factor_name, success=True,
            num_rows=num_rows, date_range=date_range,
            elapsed_sec=elapsed,
        )

    def compute_all_singles(
        self,
        instruments: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_workers: int = 4,
        timeout_per_factor: int = 600,
        factor_names: Optional[List[str]] = None,
    ) -> BatchComputeResult:
        """遍历全部已改造因子 → 并发执行 → 各存独立 parquet。

        Parameters
        ----------
        instruments : 股票池；None 则使用全 A 股
        start_date : 起始日期；None 则使用 2 年前
        end_date : 截止日期；None 则使用最新交易日
        max_workers : 并发线程数
        timeout_per_factor : 每个因子超时秒数
        factor_names : 指定因子列表；None 则全部已改造因子

        Returns
        -------
        BatchComputeResult
        """
        t0 = time.time()
        self._ensure_loaders()

        if end_date is None:
            from datetime import date
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            from datetime import date, timedelta
            d = date.today() - timedelta(days=730)
            start_date = d.strftime("%Y-%m-%d")
        if instruments is None:
            from ...data_service.qe_data_service import get_instruments_universe
            instruments = get_instruments_universe()

        factor_infos = self._resolve_factors(factor_names)
        if not factor_infos:
            return BatchComputeResult(total=0, success=0, failed=0,
                                     total_elapsed_sec=time.time() - t0)

        logger.info(
            f"开始全量单因子缓存生成: {len(factor_infos)} 因子, "
            f"{len(instruments)} 只股票, {start_date}~{end_date}, "
            f"max_workers={max_workers}"
        )

        all_results: List[FactorComputeResult] = []
        single_dir = self._single_dir()
        meta = self._load_meta()

        def _process_one(info: Dict[str, str]) -> FactorComputeResult:
            fname = info["factor_name"]
            result, df = self._execute_single_factor(
                factor_name=fname,
                code_path=info["abs_code_path"],
                instruments=instruments,
                start_date=start_date,
                end_date=end_date,
                timeout=timeout_per_factor,
            )
            if result.success and df is not None:
                out_path = os.path.join(single_dir, f"{fname}.parquet")
                save_df = df.rename(columns={df.columns[0]: "value"})
                save_df.to_parquet(out_path, engine="pyarrow", compression="snappy")
                del save_df
                meta.setdefault("factors", {})[fname] = {
                    "computed_at": datetime.now().isoformat(),
                    "rows": result.num_rows,
                    "date_range": result.date_range,
                    "as_of_date": end_date,
                }
            del df  # 立即释放因子 DataFrame
            return result

        if max_workers <= 1:
            for info in factor_infos:
                all_results.append(_process_one(info))
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_process_one, info): info
                    for info in factor_infos
                }
                for fut in as_completed(futures):
                    try:
                        all_results.append(fut.result())
                    except Exception as e:
                        info = futures[fut]
                        all_results.append(FactorComputeResult(
                            factor_name=info["factor_name"],
                            success=False,
                            error=f"ThreadPool error: {e}",
                        ))

        # 保存 meta
        meta["as_of_date"] = end_date
        self._save_meta(meta)

        success_count = sum(1 for r in all_results if r.success)
        elapsed = time.time() - t0

        logger.info(
            f"全量单因子缓存完成: {success_count}/{len(all_results)} 成功, "
            f"耗时 {elapsed:.1f}s"
        )

        return BatchComputeResult(
            total=len(all_results),
            success=success_count,
            failed=len(all_results) - success_count,
            factor_results=all_results,
            total_elapsed_sec=round(elapsed, 1),
        )

    def get_cached_singles(self) -> List[Dict[str, Any]]:
        """返回已缓存的单因子列表及其元数据。"""
        single_dir = os.path.join(self._output_dir, "single")
        if not os.path.isdir(single_dir):
            return []

        meta = self._load_meta()
        factors_meta = meta.get("factors", {})
        results = []

        for f in sorted(os.listdir(single_dir)):
            if not f.endswith(".parquet"):
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
