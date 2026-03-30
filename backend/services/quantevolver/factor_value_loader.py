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
    "rdagent_assets", "factor_values",
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
        """清除合并面板缓存文件。"""
        d = os.path.normpath(pipeline_dir or _DEFAULT_PIPELINE_DIR)
        merged = os.path.join(d, "single", "_merged_panel.parquet")
        if os.path.isfile(merged):
            os.remove(merged)
            logger.info(f"已清除合并面板缓存: {merged}")

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
    ) -> pd.DataFrame:
        """返回指定因子在日期范围内的面板数据。

        Parameters
        ----------
        factor_names : 因子列名列表
        start_date : 起始日期 (YYYY-MM-DD)
        end_date : 结束日期 (YYYY-MM-DD)

        Returns
        -------
        pd.DataFrame
            MultiIndex (datetime, instrument)，列为 factor_names。
        """
        if self._source == "single":
            return self._load_panel_from_singles(factor_names, start_date, end_date)

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
    ) -> pd.DataFrame:
        """获取某一天的截面数据。

        Returns
        -------
        pd.DataFrame
            Index = instrument，列 = factor_names
        """
        if self._source == "single":
            panel = self._load_panel_from_singles(factor_names, date, date)
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
        except Exception as e:
            logger.warning(f"加载单因子 {factor_name} 失败: {e}")
            return None

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
    ) -> pd.DataFrame:
        """从 single/ 目录按需加载指定因子，合并为面板。

        优化策略:
        1. 优先尝试读取合并面板缓存 (_merged_panel.parquet)
        2. 缓存未命中时并行加载，numpy 预分配合并（替代 pd.concat）
        3. 后续调用直接读取合并缓存

        内存优化:
        - 批量加载(>50因子)时绕过 _single_cache
        - numpy 预分配 + 逐列 reindex 填充，避免 pd.concat 的中间内存爆炸
        - 峰值内存 ≈ 最终 DataFrame 大小（~5GB），无 10-15GB 中间开销
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        t0 = _time.time()

        # ── 1. 尝试合并面板缓存 ──
        merged_path = os.path.join(self._single_dir, "_merged_panel.parquet")
        cached_panel = self._try_read_merged_cache(
            merged_path, factor_names, start_date, end_date,
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

        # ── 2. 并行加载各因子数据 ──
        bulk_mode = len(factor_names) > 50
        max_workers = min(32, len(factor_names))

        if bulk_mode:
            logger.info(
                f"批量加载模式: {len(factor_names)} 因子, "
                f"绕过 _single_cache 节省内存"
            )

        # 加载函数：返回 (factor_name, Series with MultiIndex)
        def _load_one(fname: str) -> Tuple[str, Optional[pd.Series]]:
            if bulk_mode:
                df = self._read_single_filtered(fname, start_date, end_date)
            else:
                df = self.load_single_factor(fname, start_date, end_date)
            if df is None or df.empty:
                return fname, None
            # 取第一列作为 Series（列名即因子名）
            return fname, df.iloc[:, 0]

        # 并行读取
        t1 = _time.time()
        factor_series: Dict[str, pd.Series] = {}

        if max_workers <= 1:
            for fname in factor_names:
                fname, series = _load_one(fname)
                if series is not None:
                    factor_series[fname] = series
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_load_one, fn): fn for fn in factor_names}
                for future in as_completed(futures):
                    try:
                        fname, series = future.result()
                        if series is not None:
                            factor_series[fname] = series
                    except Exception as e:
                        fn = futures[future]
                        logger.warning(f"并行加载因子 {fn} 失败: {e}")

        if not factor_series:
            logger.warning(f"single 模式: 无法加载任何因子 ({factor_names[:5]}...)")
            return pd.DataFrame()

        loaded_names = list(factor_series.keys())
        t_read = round(_time.time() - t1, 1)
        logger.info(f"[计时] 并行读取 {len(loaded_names)} 因子: {t_read}s")

        # ── 3. 构建 union index（替代 pd.concat 的 448-way 对齐）──
        t2 = _time.time()
        all_indexes = [s.index for s in factor_series.values()]
        master_index = all_indexes[0]
        for idx in all_indexes[1:]:
            if not master_index.equals(idx):
                master_index = master_index.union(idx)
        master_index = master_index.sort_values()
        N = len(master_index)
        K = len(loaded_names)
        t_idx = round(_time.time() - t2, 1)
        logger.info(f"[计时] union index: {t_idx}s, {N} 行 × {K} 列")

        # ── 4. 预分配 numpy 数组 + 逐列 reindex 填充 ──
        t3 = _time.time()
        data = np.full((N, K), np.nan, dtype=np.float64)

        for col_idx, fname in enumerate(loaded_names):
            series = factor_series[fname]
            if series.index.equals(master_index):
                # 索引完全一致，直接赋值（最快路径）
                data[:, col_idx] = series.values
            else:
                # 索引不一致，reindex 对齐（自动补 NaN）
                aligned = series.reindex(master_index)
                data[:, col_idx] = aligned.values

        # 释放中间数据
        del factor_series, all_indexes

        merged = pd.DataFrame(data, index=master_index, columns=loaded_names)
        del data
        t_fill = round(_time.time() - t3, 1)
        logger.info(f"[计时] numpy 预分配+填充: {t_fill}s")

        # ── 5. 保存合并缓存 ──
        try:
            estimated_mb = merged.memory_usage(deep=True).sum() / (1024 ** 2)
            if estimated_mb > 3000:
                logger.warning(
                    f"合并面板过大 ({estimated_mb:.0f}MB), 跳过缓存保存"
                )
            else:
                merged.to_parquet(merged_path, engine="pyarrow")
                file_mb = os.path.getsize(merged_path) / (1024 ** 2)
                logger.info(
                    f"已保存合并面板缓存: {len(merged.columns)} 因子, "
                    f"{len(merged)} 行, 内存 {estimated_mb:.0f}MB, "
                    f"文件 {file_mb:.0f}MB"
                )
        except Exception as e:
            logger.warning(f"保存合并面板缓存失败: {e}")

        # 清理可能残留的 _single_cache
        if not bulk_mode and len(factor_names) > 20:
            with self._single_cache_lock:
                self._single_cache.clear()
                logger.info("已清理 _single_cache 释放内存")

        load_elapsed = round(_time.time() - t0, 1)
        logger.info(
            f"single 模式加载(numpy预分配): {len(loaded_names)}/{len(factor_names)} 因子, "
            f"{start_date}~{end_date}, {len(merged)} 行, "
            f"总耗时 {load_elapsed}s (读取 {t_read}s + 索引 {t_idx}s + 填充 {t_fill}s)"
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
        """
        fpath = os.path.join(self._single_dir, f"{factor_name}.parquet")
        if not os.path.isfile(fpath):
            return None
        try:
            df = pd.read_parquet(fpath)
            if "value" in df.columns:
                df = df.rename(columns={"value": factor_name})
            # 立即按日期过滤，丢弃完整数据
            sd, ed = pd.Timestamp(start_date), pd.Timestamp(end_date)
            dates = df.index.get_level_values(0)
            mask = (dates >= sd) & (dates <= ed)
            return df.loc[mask]
        except Exception as e:
            logger.warning(f"加载因子 {factor_name} 失败: {e}")
            return None

    def _try_read_merged_cache(
        self,
        merged_path: str,
        factor_names: List[str],
        start_date: str,
        end_date: str,
    ) -> Optional[pd.DataFrame]:
        """尝试从合并面板缓存加载。命中返回 DataFrame，否则 None。

        失效条件:
        - 缓存文件不存在
        - 任何请求因子的 single parquet 比缓存文件更新（因子被重新生成）
        - 缓存日期范围不包含请求范围

        宽容策略:
        - 部分因子不在缓存中是允许的（首次加载时可能有因子加载失败）
        - 只读取缓存中存在的因子列，跳过不可用的因子
        - 调用方 (compute_full_matrix) 会处理因子数量不一致的情况
        """
        if not os.path.isfile(merged_path):
            return None

        try:
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

            # 列检查: 只读缓存中存在的因子列（宽容策略）
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
                f"命中合并面板缓存: {len(factor_names)} 因子, {len(panel)} 行"
            )
            return panel
        except Exception as e:
            logger.warning(f"读取合并面板缓存失败: {e}")
            return None

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
            logger.warning(f"合并 Pipeline 缓存失败: {e}")

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
