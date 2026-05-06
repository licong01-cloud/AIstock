"""
因子值加载器 — 多源懒加载因子截面数据。

数据源:
- T1: static_factors.parquet (66 因子, 3861 交易日, 4736 只股票)
- T3: Pipeline 批量计算缓存 (rdagent_assets/factor_values/batch_*.parquet)
- single: 单因子 Parquet 缓存 (rdagent_assets/factor_values/single/*.parquet)

模式:
- "parquet": 仅加载 T1 static_factors.parquet
- "auto":    T1 + T3 合并（T1 优先，T3 补充 T1 中不存在的因子）
- "single":  从 single/ 目录按需加载指定因子（因子相关性计算用）
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time as _time
from datetime import datetime
from typing import ClassVar, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("aistock.quantevolver.factor_value_loader")

# 默认路径
_DEFAULT_PARQUET = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..",
    "qlib_snapshots", "qlib_export_20251209", "static_factors.parquet",
)

_DEFAULT_PIPELINE_DIR = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..",
    "rdagent_assets", "factor_values_realtime",
)


class FactorValueLoader:
    """多源因子截面值加载器。

    支持三种模式:
    - "parquet": 仅加载 T1 static_factors.parquet (向后兼容)
    - "auto":    T1 + 最新 Pipeline 缓存合并
    - "single":  从 single/ 目录按需加载（不常驻内存）

    线程安全：使用 threading.Lock 保护一次性加载。
    加载后 DataFrame 只读（不修改），并发读取安全。
    """

    # ── 类级别单因子 DataFrame 缓存 ──
    # 所有实例共享，避免 pair 端点每次请求重新加载 parquet
    # 格式: {factor_name: (DataFrame, loaded_timestamp)}
    _single_cache: ClassVar[Dict[str, Tuple[pd.DataFrame, float]]] = {}
    _single_cache_lock: ClassVar[threading.Lock] = threading.Lock()
    _SINGLE_CACHE_TTL: ClassVar[int] = 3600  # 1 小时过期

    @classmethod
    def invalidate_single_cache(cls, factor_name: Optional[str] = None) -> None:
        """手动清除单因子缓存。factor_name=None 时清除全部。"""
        with cls._single_cache_lock:
            if factor_name:
                cls._single_cache.pop(factor_name, None)
            else:
                cls._single_cache.clear()

    @classmethod
    def invalidate_merged_cache(cls, pipeline_dir: Optional[str] = None) -> None:
        """清除合并面板缓存文件及其 sidecar。"""
        d = os.path.normpath(pipeline_dir or _DEFAULT_PIPELINE_DIR)
        merged = os.path.join(d, "single", "_merged_panel.parquet")
        sidecar = merged + ".meta.json"
        if os.path.isfile(merged):
            os.remove(merged)
            logger.info(f"已清除合并面板缓存: {merged}")
        if os.path.isfile(sidecar):
            os.remove(sidecar)
            logger.info(f"已清除合并面板 sidecar: {sidecar}")

    def __init__(
        self,
        parquet_path: Optional[str] = None,
        source: str = "auto",
        pipeline_dir: Optional[str] = None,
    ):
        self._path = os.path.normpath(parquet_path or _DEFAULT_PARQUET)
        self._source = source  # "parquet" | "auto" | "single"
        self._pipeline_dir = os.path.normpath(pipeline_dir or _DEFAULT_PIPELINE_DIR)
        self._single_dir = os.path.join(self._pipeline_dir, "single")
        self._df: Optional[pd.DataFrame] = None
        self._lock = threading.Lock()
        self._factor_columns: Optional[List[str]] = None
        self._date_min: Optional[pd.Timestamp] = None
        self._date_max: Optional[pd.Timestamp] = None
        self._t1_columns: Optional[List[str]] = None
        self._t3_columns: Optional[List[str]] = None

    # ── 公共接口 ──

    def load_factor_panel(
        self,
        factor_names: List[str],
        start_date: str,
        end_date: str,
        expected_as_of_date: Optional[str] = None,
        expected_universe_key: Optional[str] = None,
        expected_universe_fingerprint_sha256: Optional[str] = None,
        expected_index_policy: Optional[str] = None,
    ) -> pd.DataFrame:
        """返回指定因子在日期范围内的面板数据。

        Parameters
        ----------
        factor_names : 因子列名列表
        start_date : 起始日期 (YYYY-MM-DD)
        end_date : 结束日期 (YYYY-MM-DD)
        expected_as_of_date : 期望的快照日期。single 模式下必须与合并缓存的
            sidecar as_of_date 匹配, 不匹配则强制重建, 防止读到跨快照混合数据.

        Returns
        -------
        pd.DataFrame
            MultiIndex (datetime, instrument)，列为 factor_names。
        """
        if self._source == "single":
            return self._load_panel_from_singles(
                factor_names, start_date, end_date,
                expected_as_of_date=expected_as_of_date,
                expected_universe_key=expected_universe_key,
                expected_universe_fingerprint_sha256=expected_universe_fingerprint_sha256,
                expected_index_policy=expected_index_policy,
            )

        # auto / parquet 模式
        self._ensure_loaded()
        available = set(self._factor_columns)
        valid_names = [f for f in factor_names if f in available]
        if not valid_names:
            raise ValueError(f"指定的因子均不存在: {factor_names[:10]}")

        sd = pd.Timestamp(start_date)
        ed = pd.Timestamp(end_date)

        idx = self._df.index.get_level_values("datetime")
        mask = (idx >= sd) & (idx <= ed)
        panel = self._df.loc[mask, valid_names]

        logger.info(
            f"加载因子面板: {len(valid_names)} 因子, "
            f"{start_date}~{end_date}, {len(panel)} 行"
        )
        return panel

    def get_cross_section(
        self,
        factor_names: List[str],
        date: str,
        expected_as_of_date: Optional[str] = None,
        expected_universe_key: Optional[str] = None,
        expected_universe_fingerprint_sha256: Optional[str] = None,
        expected_index_policy: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取某一天的截面数据。

        Returns
        -------
        pd.DataFrame
            Index = instrument，列 = factor_names
        """
        if self._source == "single":
            panel = self._load_panel_from_singles(
                factor_names, date, date,
                expected_as_of_date=expected_as_of_date,
                expected_universe_key=expected_universe_key,
                expected_universe_fingerprint_sha256=expected_universe_fingerprint_sha256,
                expected_index_policy=expected_index_policy,
            )
            if panel.empty:
                return pd.DataFrame(columns=factor_names)
            ts = pd.Timestamp(date)
            try:
                return panel.loc[ts]
            except KeyError:
                return pd.DataFrame(columns=factor_names)

        self._ensure_loaded()
        ts = pd.Timestamp(date)
        try:
            section = self._df.loc[ts, factor_names]
        except KeyError:
            return pd.DataFrame(columns=factor_names)
        return section

    def get_available_factors(self) -> List[str]:
        """返回可用因子列名列表。"""
        if self._source == "single":
            return self._scan_single_factors()
        self._ensure_loaded()
        return list(self._factor_columns)

    def get_date_range(self) -> Tuple[str, str]:
        """返回数据的日期范围 (YYYY-MM-DD, YYYY-MM-DD)。"""
        if self._source == "single":
            dates = self._get_trading_dates_from_singles("2000-01-01", "2099-12-31")
            if not dates:
                raise ValueError("single/ 目录无因子缓存")
            return (dates[0], dates[-1])
        self._ensure_loaded()
        return (
            self._date_min.strftime("%Y-%m-%d"),
            self._date_max.strftime("%Y-%m-%d"),
        )

    def get_trading_dates(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """返回日期范围内的所有交易日列表。"""
        if self._source == "single":
            return self._get_trading_dates_from_singles(start_date, end_date)
        self._ensure_loaded()
        sd = pd.Timestamp(start_date)
        ed = pd.Timestamp(end_date)
        dates = self._df.index.get_level_values("datetime").unique()
        dates = dates[(dates >= sd) & (dates <= ed)]
        return [d.strftime("%Y-%m-%d") for d in sorted(dates)]

    def load_single_factor(
        self,
        factor_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """加载单个因子的 parquet 数据（带类级别缓存）。

        Returns
        -------
        pd.DataFrame or None
            MultiIndex(datetime, instrument), column=[factor_name]
        """
        now = _time.time()

        # 1) 尝试从缓存读取完整 DataFrame
        with self._single_cache_lock:
            cached = self._single_cache.get(factor_name)
            if cached is not None:
                cached_df, cached_at = cached
                if now - cached_at < self._SINGLE_CACHE_TTL:
                    # 缓存命中，按日期范围过滤后返回副本
                    df = cached_df
                    if start_date or end_date:
                        dates = df.index.get_level_values(0)
                        mask = pd.Series(True, index=df.index)
                        if start_date:
                            mask = mask & (dates >= pd.Timestamp(start_date))
                        if end_date:
                            mask = mask & (dates <= pd.Timestamp(end_date))
                        df = df.loc[mask]
                    return df
                else:
                    # TTL 过期
                    del self._single_cache[factor_name]

        # 2) 缓存未命中，从 parquet 加载
        fpath = os.path.join(self._single_dir, f"{factor_name}.parquet")
        if not os.path.isfile(fpath):
            return None

        try:
            df = pd.read_parquet(fpath)
        except Exception as e:
            raise RuntimeError(
                f"因子缓存文件损坏或不可读: {factor_name} ({fpath}): {e}"
            ) from e

        # 单因子 parquet 的列是 "value"，重命名为因子名
        if "value" in df.columns:
            df = df.rename(columns={"value": factor_name})

        # 3) 存入缓存（存完整 DataFrame，不含日期过滤）
        with self._single_cache_lock:
            self._single_cache[factor_name] = (df, now)

        # 4) 按日期范围过滤后返回
        if start_date or end_date:
            dates = df.index.get_level_values(0)
            mask = pd.Series(True, index=df.index)
            if start_date:
                mask = mask & (dates >= pd.Timestamp(start_date))
            if end_date:
                mask = mask & (dates <= pd.Timestamp(end_date))
            df = df.loc[mask]

        return df

    @property
    def is_loaded(self) -> bool:
        return self._df is not None

    # ── single 模式内部方法 ──

    def _scan_single_factors(self) -> List[str]:
        """扫描 single/ 目录下的 parquet 文件名，返回因子名列表。"""
        if not os.path.isdir(self._single_dir):
            return []
        return sorted([
            f[:-8]
            for f in os.listdir(self._single_dir)
            if f.endswith(".parquet") and not f.startswith("_")
        ])

    def _get_trading_dates_from_singles(self, start_date: str, end_date: str) -> List[str]:
        """从 single/ 目录的第一个 parquet 文件获取交易日列表。"""
        factors = self._scan_single_factors()
        if not factors:
            return []
        first_path = os.path.join(self._single_dir, f"{factors[0]}.parquet")
        df = pd.read_parquet(first_path, columns=[])  # 只读索引
        dates = df.index.get_level_values("datetime").unique()
        sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
        dates = dates[(dates >= sd) & (dates <= ed)]
        return [d.strftime("%Y-%m-%d") for d in sorted(dates)]

    def _load_panel_from_singles(
        self,
        factor_names: List[str],
        start_date: str,
        end_date: str,
        expected_as_of_date: Optional[str] = None,
        expected_universe_key: Optional[str] = None,
        expected_universe_fingerprint_sha256: Optional[str] = None,
        expected_index_policy: Optional[str] = None,
    ) -> pd.DataFrame:
        """从 single/ 目录按需加载指定因子，合并为面板。

        优化策略:
        1. 优先尝试读取合并面板缓存 (_merged_panel.parquet)
        2. 缓存未命中时流式加载：先建 master_index，再逐批填充 float32 数组
        3. 后续调用直接读取合并缓存

        内存优化 (v2):
        - float32 替代 float64，面板内存减半（秩相关不需要 float64 精度）
        - 流式批量填充：每批 16 因子加载→填充→释放，不累积全量 Series
        - 峰值内存 ≈ float32 面板本身（~3.4GB for 611因子），无 Series 堆积

        as_of_date 校验 (Bug C 修复):
        - 合并缓存旁侧存放 _merged_panel.meta.json, 内含生成时的 as_of_date
        - 读取前必须与调用方 expected_as_of_date 匹配, 否则强制重建
        - 缺失 sidecar 视为非法缓存, 强制重建
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        t0 = _time.time()

        # ── 1. 尝试合并面板缓存 ──
        merged_path = os.path.join(self._single_dir, "_merged_panel.parquet")
        cached_panel = self._try_read_merged_cache(
            merged_path, factor_names, start_date, end_date,
            expected_as_of_date=expected_as_of_date,
            expected_universe_key=expected_universe_key,
            expected_universe_fingerprint_sha256=expected_universe_fingerprint_sha256,
            expected_index_policy=expected_index_policy,
        )
        if cached_panel is not None:
            elapsed = round(_time.time() - t0, 1)
            logger.info(
                f"single 模式加载(合并缓存命中): "
                f"{len(cached_panel.columns)}/{len(factor_names)} 因子, "
                f"{start_date}~{end_date}, {len(cached_panel)} 行, "
                f"耗时 {elapsed}s"
            )
            return cached_panel

        # ── 2. 检查可用因子 + 构建 master_index ──
        bulk_mode = len(factor_names) > 50
        sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)

        if bulk_mode:
            logger.info(
                f"流式加载模式: {len(factor_names)} 因子, float32 + 批量填充"
            )

        # 筛选有 parquet 文件的因子
        available_names = [
            fn for fn in factor_names
            if os.path.isfile(os.path.join(self._single_dir, f"{fn}.parquet"))
        ]
        if not available_names:
            logger.warning(f"single 模式: 无可用因子文件 ({factor_names[:5]}...)")
            return pd.DataFrame()

        # 从首个 parquet 获取 master_index（同一 pipeline 生成的因子共享索引）
        t1 = _time.time()
        first_path = os.path.join(self._single_dir, f"{available_names[0]}.parquet")
        first_idx = pd.read_parquet(first_path, columns=[]).index
        date_vals = first_idx.get_level_values(0)
        master_index = first_idx[(date_vals >= sd) & (date_vals <= ed)].sort_values()
        del first_idx, date_vals

        N = len(master_index)
        K = len(available_names)
        t_idx = round(_time.time() - t1, 1)
        logger.info(f"[计时] master_index: {t_idx}s, {N} 行 × {K} 列")

        # ── 3. 预分配 float32 数组 + 流式批量填充 ──
        t2 = _time.time()
        data = np.full((N, K), np.nan, dtype=np.float32)
        loaded_names: List[str] = []
        batch_size = 16
        max_workers = min(batch_size, 8)

        def _read_as_array(fname: str) -> Tuple[str, Optional[np.ndarray]]:
            """读取单因子 parquet，返回对齐到 master_index 的 float32 数组。"""
            fpath = os.path.join(self._single_dir, f"{fname}.parquet")
            try:
                df = pd.read_parquet(fpath)
                if "value" in df.columns:
                    df = df.rename(columns={"value": fname})
                idx_dates = df.index.get_level_values(0)
                df = df.loc[(idx_dates >= sd) & (idx_dates <= ed)]
                series = df.iloc[:, 0]
                if series.index.equals(master_index):
                    return fname, series.values.astype(np.float32)
                else:
                    return fname, series.reindex(master_index).values.astype(np.float32)
            except Exception as e:
                raise RuntimeError(
                    f"因子缓存文件损坏或不可读: {fname} ({fpath}): {e}"
                ) from e

        for batch_start in range(0, K, batch_size):
            batch_names = available_names[batch_start:batch_start + batch_size]

            if len(batch_names) <= 2:
                batch_results = []
                for fn in batch_names:
                    batch_results.append(_read_as_array(fn))
            else:
                batch_results = []
                with ThreadPoolExecutor(max_workers=min(max_workers, len(batch_names))) as executor:
                    futures = {executor.submit(_read_as_array, fn): fn for fn in batch_names}
                    for future in as_completed(futures):
                        batch_results.append(future.result())

            for fname, arr in batch_results:
                if arr is not None:
                    col_idx = len(loaded_names)
                    data[:, col_idx] = arr
                    loaded_names.append(fname)
            del batch_results

        if not loaded_names:
            logger.warning(f"single 模式: 无法加载任何因子 ({factor_names[:5]}...)")
            return pd.DataFrame()

        # 裁剪未使用的列
        if len(loaded_names) < K:
            data = data[:, :len(loaded_names)]

        t_fill = round(_time.time() - t2, 1)
        logger.info(f"[计时] 流式加载+填充 {len(loaded_names)} 因子: {t_fill}s")

        merged = pd.DataFrame(data, index=master_index, columns=loaded_names)
        del data
        mem_mb = merged.memory_usage(deep=True).sum() / (1024 ** 2)
        logger.info(f"[内存] 面板: {mem_mb:.0f}MB (float32)")

        # ── 4. 保存合并缓存 ──
        # 保存失败必须抛出, 严禁静默吞错 (缓存不存在会让下次运行反复重建, 掩盖底层故障)
        if mem_mb > 3000:
            logger.warning(f"合并面板过大 ({mem_mb:.0f}MB), 跳过缓存保存")
        else:
            # 原子写 parquet: 先写临时文件再 rename, 避免读到半截 parquet
            tmp_merged = merged_path + ".tmp"
            merged.to_parquet(tmp_merged, engine="pyarrow")
            os.replace(tmp_merged, merged_path)

            # ── sidecar: 记录本次合并所用快照 as_of_date (Bug C 修复) ──
            # expected_as_of_date 可能为 None (未指定), 此时写入 None, 下次读取时
            # 若调用方指定了 expected 则自动失效, 保证强一致.
            sidecar_path = merged_path + ".meta.json"
            sidecar_data = {
                "as_of_date": expected_as_of_date,
                "factor_count": len(merged.columns),
                "factor_names": list(merged.columns),
                "date_range": f"{merged.index.get_level_values(0).min().strftime('%Y-%m-%d')}~{merged.index.get_level_values(0).max().strftime('%Y-%m-%d')}",
                "row_count": len(merged),
                "generated_at": datetime.now().isoformat(),
                "universe_key": expected_universe_key,
                "universe_fingerprint_sha256": expected_universe_fingerprint_sha256,
                "index_policy": expected_index_policy,
            }
            tmp_sidecar = sidecar_path + ".tmp"
            with open(tmp_sidecar, "w", encoding="utf-8") as _sf:
                json.dump(sidecar_data, _sf, ensure_ascii=False, indent=2)
                _sf.flush()
                os.fsync(_sf.fileno())
            os.replace(tmp_sidecar, sidecar_path)

            file_mb = os.path.getsize(merged_path) / (1024 ** 2)
            logger.info(
                f"已保存合并面板缓存: {len(merged.columns)} 因子, "
                f"{len(merged)} 行, 内存 {mem_mb:.0f}MB, "
                f"文件 {file_mb:.0f}MB, sidecar.as_of_date={expected_as_of_date}"
            )

        # 清理可能残留的 _single_cache
        if not bulk_mode and len(factor_names) > 20:
            with self._single_cache_lock:
                self._single_cache.clear()
                logger.info("已清理 _single_cache 释放内存")

        load_elapsed = round(_time.time() - t0, 1)
        logger.info(
            f"single 模式加载(float32流式): {len(loaded_names)}/{len(factor_names)} 因子, "
            f"{start_date}~{end_date}, {len(merged)} 行, "
            f"总耗时 {load_elapsed}s (索引 {t_idx}s + 加载填充 {t_fill}s)"
        )
        return merged

    def _read_single_filtered(
        self,
        factor_name: str,
        start_date: str,
        end_date: str,
    ) -> Optional[pd.DataFrame]:
        """直接读取单因子 parquet 并按日期过滤，不缓存完整 DataFrame。

        用于批量加载场景(>50因子)，避免 _single_cache 内存爆炸。
        每因子仅保留日期过滤后的数据(~5-8MB)，而非完整数据(~70MB)。

        文件不存在返回 None (上游选择是否补算); 任何解析/IO 错误必须抛出.
        """
        fpath = os.path.join(self._single_dir, f"{factor_name}.parquet")
        if not os.path.isfile(fpath):
            return None
        try:
            df = pd.read_parquet(fpath)
        except Exception as e:
            raise RuntimeError(
                f"因子缓存文件损坏或不可读: {factor_name} ({fpath}): {e}"
            ) from e
        if "value" in df.columns:
            df = df.rename(columns={"value": factor_name})
        # 立即按日期过滤，丢弃完整数据
        sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
        dates = df.index.get_level_values(0)
        mask = (dates >= sd) & (dates <= ed)
        return df.loc[mask]

    def _try_read_merged_cache(
        self,
        merged_path: str,
        factor_names: List[str],
        start_date: str,
        end_date: str,
        expected_as_of_date: Optional[str] = None,
        expected_universe_key: Optional[str] = None,
        expected_universe_fingerprint_sha256: Optional[str] = None,
        expected_index_policy: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """尝试从合并面板缓存加载。命中返回 DataFrame，否则 None。

        失效条件:
        - 缓存文件不存在
        - sidecar (_merged_panel.meta.json) 不存在, 或 as_of_date 与 expected 不匹配
        - 任何请求因子的 single parquet 比缓存文件更新（因子被重新生成）
        - 缓存日期范围不包含请求范围

        错误处理:
        - 文件不存在/sidecar 缺失 → 返回 None (正常触发重建)
        - 文件损坏/IO 错误 → 删除损坏文件并抛出, 严禁静默吞错
        """
        if not os.path.isfile(merged_path):
            return None

        # ── as_of_date sidecar 校验 (Bug C 修复) ──
        sidecar_path = merged_path + ".meta.json"
        if not os.path.isfile(sidecar_path):
            logger.info(
                f"合并缓存缺少 sidecar ({sidecar_path}), 视为非法缓存, 删除并重建"
            )
            os.remove(merged_path)
            return None

        with open(sidecar_path, "r", encoding="utf-8") as _sf:
            sidecar = json.load(_sf)
        cache_aod = sidecar.get("as_of_date")
        if expected_as_of_date is not None and cache_aod != expected_as_of_date:
            logger.info(
                f"合并缓存 as_of_date={cache_aod} 与期望 {expected_as_of_date} 不匹配, 删除并重建"
            )
            os.remove(merged_path)
            os.remove(sidecar_path)
            return None

        universe_checks = {
            "universe_key": expected_universe_key,
            "universe_fingerprint_sha256": expected_universe_fingerprint_sha256,
            "index_policy": expected_index_policy,
        }
        for key, expected in universe_checks.items():
            if expected is not None and sidecar.get(key) != expected:
                logger.info(
                    "merged factor cache %s mismatch: actual=%s expected=%s; rebuild",
                    key, sidecar.get(key), expected,
                )
                os.remove(merged_path)
                os.remove(sidecar_path)
                return None

        # 文件大小保护: 超过 1GB 的缓存文件跳过（解压后内存放大 5-8 倍）
        file_size_mb = os.path.getsize(merged_path) / (1024 ** 2)
        if file_size_mb > 1024:
            logger.warning(
                f"合并缓存文件过大 ({file_size_mb:.0f}MB > 1024MB), "
                f"跳过读取以避免内存爆炸"
            )
            return None

        merged_mtime = os.path.getmtime(merged_path)

        # 新鲜度检查: 有 single parquet 被更新 → 缓存失效
        for fname in factor_names:
            single_path = os.path.join(self._single_dir, f"{fname}.parquet")
            if not os.path.isfile(single_path):
                continue  # 因子无 parquet 文件，跳过（不影响缓存有效性）
            if os.path.getmtime(single_path) > merged_mtime:
                logger.info(f"因子 {fname} 已更新, 合并面板缓存失效")
                return None

        # 读取 + 范围/列检查: 任何读取错误必须抛出, 不再 return None
        try:
            import pyarrow.parquet as pq
            schema = pq.read_schema(merged_path)
            cached_cols = set(schema.names)
            available = [f for f in factor_names if f in cached_cols]

            if not available:
                logger.info("合并缓存无匹配因子列, 需重建")
                return None

            if len(available) < len(factor_names):
                skipped = len(factor_names) - len(available)
                logger.info(
                    f"合并缓存包含 {len(available)}/{len(factor_names)} 因子 "
                    f"(跳过 {skipped} 个不可用因子)"
                )

            # 列裁剪读取 (pyarrow column pruning, 只读需要的列)
            panel = pd.read_parquet(merged_path, columns=available)
        except Exception as e:
            # 合并缓存损坏: 删除坏文件 + 抛出, 让调用方感知
            logger.error(f"合并面板缓存损坏, 已删除: {merged_path}: {e}", exc_info=True)
            if os.path.isfile(merged_path):
                os.remove(merged_path)
            if os.path.isfile(sidecar_path):
                os.remove(sidecar_path)
            raise RuntimeError(
                f"合并面板缓存读取失败并已删除, 请重试: {e}"
            ) from e

        # 日期范围检查
        sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
        dates = panel.index.get_level_values(0)
        if dates.min() > sd or dates.max() < ed:
            logger.info(
                f"合并缓存日期范围 "
                f"[{dates.min().date()}~{dates.max().date()}] "
                f"不包含请求范围 [{start_date}~{end_date}], 需重建"
            )
            return None

        # 日期过滤
        mask = (dates >= sd) & (dates <= ed)
        panel = panel.loc[mask]

        logger.info(
            f"命中合并面板缓存: {len(factor_names)} 因子, {len(panel)} 行, "
            f"sidecar.as_of_date={cache_aod}"
        )
        return panel

    # ── auto/parquet 模式内部方法 ──

    def _ensure_loaded(self) -> None:
        """懒加载数据，线程安全，只加载一次。

        auto 模式: 先加载 T1 parquet，再合并最新的 Pipeline 缓存。
        parquet 模式: 仅加载 T1 parquet。
        single 模式: 不需要预加载（按需加载）。
        """
        if self._source == "single":
            return

        if self._df is not None:
            return
        with self._lock:
            if self._df is not None:
                return

            # 加载 T1
            if not os.path.exists(self._path):
                raise FileNotFoundError(f"Parquet 文件不存在: {self._path}")

            logger.info(f"首次加载 T1 Parquet: {self._path}")
            self._df = pd.read_parquet(self._path)

            if not isinstance(self._df.index, pd.MultiIndex):
                raise ValueError(
                    f"Parquet 数据必须为 (datetime, instrument) MultiIndex，"
                    f"当前 index: {self._df.index.names}"
                )

            if not self._df.index.is_monotonic_increasing:
                self._df = self._df.sort_index()

            self._t1_columns = list(self._df.columns)

            # auto 模式: 尝试合并 T3 Pipeline 缓存
            self._t3_columns = []
            if self._source == "auto":
                self._merge_pipeline_cache()

            self._factor_columns = list(self._df.columns)
            dates = self._df.index.get_level_values("datetime")
            self._date_min = dates.min()
            self._date_max = dates.max()

            logger.info(
                f"数据加载完成: {len(self._factor_columns)} 因子 "
                f"(T1={len(self._t1_columns)}, T3={len(self._t3_columns)}), "
                f"{len(self._df)} 行, "
                f"日期 {self._date_min.date()}~{self._date_max.date()}"
            )

    def _merge_pipeline_cache(self) -> None:
        """合并最新的 Pipeline 计算缓存到 T1 数据中。

        只补充 T1 中不存在的因子列（T3 因子）。
        """
        latest = self._find_latest_pipeline_parquet()
        if latest is None:
            logger.info("无 Pipeline 缓存可合并")
            return

        try:
            logger.info(f"加载 T3 Pipeline 缓存: {latest}")
            df_t3 = pd.read_parquet(latest)

            if not isinstance(df_t3.index, pd.MultiIndex):
                logger.warning("Pipeline 缓存索引格式不正确，跳过")
                return

            # 仅保留 T1 中不存在的列
            new_cols = [c for c in df_t3.columns if c not in self._df.columns]
            if not new_cols:
                logger.info("Pipeline 缓存中无新增因子列")
                return

            df_t3 = df_t3[new_cols]
            self._t3_columns = new_cols

            # outer join 合并
            self._df = self._df.join(df_t3, how="outer")
            if not self._df.index.is_monotonic_increasing:
                self._df = self._df.sort_index()

            logger.info(f"T3 因子合并完成: +{len(new_cols)} 列 ({new_cols[:5]}...)")

        except Exception as e:
            logger.error(f"合并 Pipeline 缓存失败: {e}")
            raise RuntimeError(f"合并 Pipeline 缓存失败: {e}") from e

    def _find_latest_pipeline_parquet(self) -> Optional[str]:
        """查找最新的 Pipeline 缓存 Parquet 文件。"""
        if not os.path.isdir(self._pipeline_dir):
            return None
        files = sorted(
            [f for f in os.listdir(self._pipeline_dir)
             if f.startswith("batch_") and f.endswith(".parquet")],
            reverse=True,
        )
        return os.path.join(self._pipeline_dir, files[0]) if files else None
