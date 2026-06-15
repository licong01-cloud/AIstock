"""
因子相关性计算引擎 — 截面 Spearman 秩相关 + EWMA 时序聚合。

方法论参考:
- Barra/MSCI USE4 风险模型
- Axioma Risk Model
- 截面 Spearman Rank Correlation + EWMA (half-life=125d, window=252d)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import h5py
import numpy as np
import pandas as pd
from scipy import stats

from .factor_value_loader import FactorValueLoader

logger = logging.getLogger("aistock.quantevolver.correlation_engine")

# GPU 加速探测
_USE_GPU = False
try:
    import torch
    if torch.cuda.is_available():
        # 测试实际 GEMM 是否正常工作
        _test = torch.randn(10, 10, device="cuda") @ torch.randn(10, 10, device="cuda")
        torch.cuda.synchronize()
        del _test
        _USE_GPU = True
        logger.info(f"GPU 加速已启用: {torch.cuda.get_device_name(0)}, CUDA {torch.version.cuda}")
except Exception as e:
    logger.info(f"GPU 不可用，使用 CPU BLAS: {e}")

# HDF5 存储目录
_DEFAULT_HDF5_DIR = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..",
    "data", "correlation_matrices",
)


@dataclass
class PairwiseResult:
    """两个因子的相关性结果（含时序分解）。"""
    factor_a: str
    factor_b: str
    correlation: float  # EWMA 聚合后的最终值
    daily_correlations: List[Tuple[str, float]]  # [(date, corr), ...]
    as_of_date: str
    effective_days: int
    avg_stocks_per_day: float


@dataclass
class CorrelationResult:
    """K×K 因子相关性矩阵结果。"""
    matrix: np.ndarray             # K×K 对称矩阵
    factor_names: List[str]
    as_of_date: str
    effective_window: int          # 实际使用天数
    computation_time_sec: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_pair(self, fa: str, fb: str) -> float:
        """获取两个因子的相关性值。"""
        idx_a = self.factor_names.index(fa)
        idx_b = self.factor_names.index(fb)
        return float(self.matrix[idx_a, idx_b])

    def get_high_corr_pairs(
        self,
        threshold: float = 0.7,
    ) -> List[Dict[str, Any]]:
        """获取 |相关性| > threshold 的因子对。"""
        pairs = []
        n = len(self.factor_names)
        for i in range(n):
            for j in range(i + 1, n):
                corr = float(self.matrix[i, j])
                if abs(corr) > threshold:
                    pairs.append({
                        "factor_a": self.factor_names[i],
                        "factor_b": self.factor_names[j],
                        "correlation": round(corr, 6),
                    })
        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        return pairs

    def to_hdf5(self, path: str) -> None:
        """保存到 HDF5 文件。"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with h5py.File(path, "w") as f:
            f.create_dataset("matrix", data=self.matrix, compression="gzip")
            f.attrs["factor_names"] = self.factor_names
            f.attrs["as_of_date"] = self.as_of_date
            f.attrs["effective_window"] = self.effective_window
            f.attrs["computation_time_sec"] = self.computation_time_sec
            for k, v in self.metadata.items():
                try:
                    f.attrs[k] = v
                except TypeError:
                    f.attrs[k] = str(v)
        logger.info(f"相关性矩阵已保存: {path}")

    @classmethod
    def from_hdf5(cls, path: str) -> "CorrelationResult":
        """从 HDF5 文件加载。"""
        with h5py.File(path, "r") as f:
            matrix = f["matrix"][:]
            # h5py attrs 返回 numpy 类型，需转为 Python 原生类型
            raw_names = f.attrs["factor_names"]
            factor_names = [
                s.decode("utf-8") if isinstance(s, bytes) else str(s)
                for s in raw_names
            ]
            as_of_date = str(f.attrs["as_of_date"])
            effective_window = int(f.attrs["effective_window"])
            computation_time_sec = float(f.attrs["computation_time_sec"])
            metadata = {}
            for k in f.attrs:
                if k in {"factor_names", "as_of_date",
                         "effective_window", "computation_time_sec"}:
                    continue
                v = f.attrs[k]
                # 将 numpy 类型转为 Python 原生类型
                if isinstance(v, (np.integer,)):
                    metadata[k] = int(v)
                elif isinstance(v, (np.floating,)):
                    metadata[k] = float(v)
                elif isinstance(v, np.ndarray):
                    metadata[k] = v.tolist()
                elif isinstance(v, bytes):
                    metadata[k] = v.decode("utf-8")
                else:
                    metadata[k] = v
        return cls(
            matrix=matrix,
            factor_names=factor_names,
            as_of_date=as_of_date,
            effective_window=effective_window,
            computation_time_sec=computation_time_sec,
            metadata=metadata,
        )

    def to_db_records(self, threshold: float = 0) -> List[Dict[str, Any]]:
        """转为数据库记录。threshold=0 表示全量存储所有因子对。"""
        records = []
        n = len(self.factor_names)
        for i in range(n):
            for j in range(i + 1, n):
                corr = float(self.matrix[i, j])
                if abs(corr) > threshold:
                    records.append({
                        "factor_a": self.factor_names[i],
                        "factor_b": self.factor_names[j],
                        "correlation": round(corr, 6),
                        "method": "spearman_ewma",
                        "data_period": f"252d_as_of_{self.as_of_date}",
                    })
        return records

    def get_no_valid_pair_factors(self) -> list[str]:
        """Return factor names whose off-diagonal correlations are all NaN."""
        if self.matrix.size == 0:
            return list(self.factor_names)

        no_valid_pairs: list[str] = []
        for idx, name in enumerate(self.factor_names):
            row = np.asarray(self.matrix[idx], dtype=float)
            if row.size <= 1:
                no_valid_pairs.append(name)
                continue
            off_diag = np.delete(row, idx)
            if np.all(np.isnan(off_diag)):
                no_valid_pairs.append(name)
        return no_valid_pairs


class CorrelationEngine:
    """业界标准因子相关性计算引擎。

    方法: 截面 Spearman 秩相关 + EWMA 时序聚合
    参数:
    - window: 时序窗口 (默认 252 天 = 1 年)
    - half_life: EWMA 半衰期 (默认 125 天, λ≈0.9945)
    - min_stocks: 单日截面最少股票数 (默认 100)
    - min_days: 时序最少有效天数 (默认 126)
    - winsorize_quantile: Winsorize 百分位 (默认 0.05 = 5%~95%)
    """

    def __init__(
        self,
        loader: FactorValueLoader,
        window: int = 252,
        half_life: int = 125,
        min_stocks: int = 100,
        min_days: int = 126,
        winsorize_quantile: float = 0.05,
        hdf5_dir: Optional[str] = None,
    ):
        self._loader = loader
        self._window = window
        self._half_life = half_life
        self._min_stocks = min_stocks
        self._min_days = min_days
        self._winsorize_q = winsorize_quantile
        self._hdf5_dir = os.path.normpath(hdf5_dir or _DEFAULT_HDF5_DIR)

        # EWMA 衰减因子: λ = 2^(-1/half_life)
        self._lambda = 2.0 ** (-1.0 / self._half_life)

    def compute_full_matrix(
        self,
        factor_names: List[str],
        as_of_date: Optional[str] = None,
        save_hdf5: bool = True,
        on_progress: Optional[Callable[[int, int], None]] = None,
        stop_event: Optional[threading.Event] = None,
        expected_as_of_date: Optional[str] = None,
        expected_universe_key: Optional[str] = None,
        expected_universe_rule_version: Optional[str] = None,
        expected_universe_fingerprint_sha256: Optional[str] = None,
        expected_index_policy: Optional[str] = None,
    ) -> CorrelationResult:
        """计算截至 as_of_date 的完整 K×K 相关性矩阵。

        Parameters
        ----------
        factor_names : 因子列表
        as_of_date : 截止日期 (YYYY-MM-DD)，默认数据最新日期
        save_hdf5 : 是否保存 HDF5 快照
        on_progress : 进度回调 (done, total)
        stop_event : 取消/超时事件
        expected_as_of_date : 合并缓存 sidecar 期望的快照日期，single 模式下
            传入后会触发 sidecar 校验，确保不读取跨快照混合数据。

        Returns
        -------
        CorrelationResult
        """
        t0 = time.time()
        K = len(factor_names)
        backend = "GPU" if _USE_GPU else "CPU BLAS"
        logger.info(f"开始计算 {K}×{K} 相关性矩阵, window={self._window}d, backend={backend}")

        # 确定日期范围
        if as_of_date is None:
            _, as_of_date = self._loader.get_date_range()

        # 获取窗口内的交易日列表
        all_dates = self._loader.get_trading_dates("2000-01-01", as_of_date)
        window_dates = all_dates[-self._window:] if len(all_dates) > self._window else all_dates

        if len(window_dates) < self._min_days:
            raise ValueError(
                f"有效交易日不足: {len(window_dates)} < {self._min_days}"
            )

        # 一次性加载面板数据
        panel = self._loader.load_factor_panel(
            factor_names,
            start_date=window_dates[0],
            end_date=window_dates[-1],
            expected_as_of_date=expected_as_of_date,
            expected_universe_key=expected_universe_key,
            expected_universe_fingerprint_sha256=expected_universe_fingerprint_sha256,
            expected_index_policy=expected_index_policy,
        )

        # 无条件同步 factor_names 与面板实际列顺序（修复列错位 bug）
        actual_factors = list(panel.columns)
        if len(actual_factors) != K:
            logger.warning(
                f"面板因子数 ({len(actual_factors)}) != 请求因子数 ({K}), "
                f"缺失: {set(factor_names) - set(actual_factors)}"
            )
        factor_names = actual_factors
        K = len(factor_names)

        # 因子覆盖率记录 (不再硬过滤)
        # 按机构做法: 逐日截面 Spearman + min_stocks + min_days 已能处理稀疏因子,
        # 因子级覆盖率只作为信息记录, 不用于剔除.
        # 估值类/分红类/行业类因子天然只在部分股票有值, 在日截面交集内仍可得到统计有效相关.
        total_rows = len(panel)
        if total_rows > 0:
            non_nan_ratio = panel.count() / total_rows
            low_coverage = non_nan_ratio[non_nan_ratio < 0.8]
            if len(low_coverage) > 0:
                preview = low_coverage.sort_values().head(10).to_dict()
                logger.info(
                    f"低覆盖率因子 (<80%) 共 {len(low_coverage)} 个, 保留参与计算, "
                    f"最低 10 个覆盖率: {preview}"
                )
            min_cov = non_nan_ratio.min() if len(non_nan_ratio) else 0
            if min_cov == 0:
                zero_cov = non_nan_ratio[non_nan_ratio == 0].index.tolist()
                raise ValueError(
                    f"{len(zero_cov)} 个因子在当前窗口内全为 NaN, 无法计算相关: "
                    f"{zero_cov[:10]}{'...' if len(zero_cov) > 10 else ''}"
                )

        # 逐日计算截面 Spearman 相关
        daily_corrs = []
        valid_dates = []
        stocks_per_day = []
        total_days = len(window_dates)

        for day_idx, date_str in enumerate(window_dates):
            # 协作式取消检查
            if stop_event is not None and stop_event.is_set():
                raise RuntimeError(
                    f"相关性计算被取消/超时 (已完成 {len(daily_corrs)}/{total_days} 天)"
                )

            ts = pd.Timestamp(date_str)
            try:
                section = panel.loc[ts]
            except KeyError:
                if on_progress is not None:
                    on_progress(day_idx + 1, total_days)
                continue

            if len(section) < self._min_stocks:
                if on_progress is not None:
                    on_progress(day_idx + 1, total_days)
                continue

            # Winsorize 预处理
            section_w = self._winsorize_cross_section(section)

            # 5-GEMM Spearman 相关矩阵
            corr_mat = self._cross_sectional_spearman_gemm(section_w)
            if corr_mat is not None:
                daily_corrs.append(corr_mat)
                valid_dates.append(date_str)
                stocks_per_day.append(len(section))

            if on_progress is not None:
                on_progress(day_idx + 1, total_days)

        if len(daily_corrs) < self._min_days:
            raise ValueError(
                f"有效截面天数不足: {len(daily_corrs)} < {self._min_days}"
            )

        # EWMA 时序聚合
        final_matrix = self._ewma_aggregate(daily_corrs, valid_dates)

        elapsed = time.time() - t0
        result = CorrelationResult(
            matrix=final_matrix,
            factor_names=factor_names,
            as_of_date=as_of_date,
            effective_window=len(valid_dates),
            computation_time_sec=round(elapsed, 2),
            metadata={
                "window_requested": self._window,
                "half_life": self._half_life,
                "min_stocks": self._min_stocks,
                "winsorize_quantile": self._winsorize_q,
                "avg_stocks_per_day": round(np.mean(stocks_per_day), 1),
                "date_range": f"{valid_dates[0]}~{valid_dates[-1]}",
                "universe_key": expected_universe_key,
                "universe_rule_version": expected_universe_rule_version,
                "universe_fingerprint_sha256": expected_universe_fingerprint_sha256,
                "index_policy": expected_index_policy,
                "num_high_corr_07": 0,
            },
        )
        # 修正 metadata 中的 high_corr 计数
        result.metadata["num_high_corr_07"] = len(result.get_high_corr_pairs(0.7))
        result.metadata["num_high_corr_05"] = len(result.get_high_corr_pairs(0.5))

        logger.info(
            f"相关性矩阵计算完成: {K}×{K}, "
            f"{result.effective_window} 有效天, "
            f"耗时 {elapsed:.1f}s, "
            f"|corr|>0.7 对数: {result.metadata['num_high_corr_07']}"
        )

        # 保存 HDF5
        if save_hdf5:
            hdf5_path = os.path.join(
                self._hdf5_dir, f"corr_{as_of_date.replace('-', '')}.h5"
            )
            result.to_hdf5(hdf5_path)
            result.metadata["hdf5_path"] = hdf5_path

        return result

    def compute_pairwise(
        self,
        factor_a: str,
        factor_b: str,
        as_of_date: Optional[str] = None,
    ) -> PairwiseResult:
        """计算两个因子的相关性（含每日时序分解）。"""
        if as_of_date is None:
            _, as_of_date = self._loader.get_date_range()

        all_dates = self._loader.get_trading_dates("2000-01-01", as_of_date)
        window_dates = all_dates[-self._window:] if len(all_dates) > self._window else all_dates

        panel = self._loader.load_factor_panel(
            [factor_a, factor_b],
            start_date=window_dates[0],
            end_date=window_dates[-1],
        )

        daily_corrs_scalar = []
        valid_dates = []
        stocks_per_day = []

        for date_str in window_dates:
            ts = pd.Timestamp(date_str)
            try:
                section = panel.loc[ts]
            except KeyError:
                continue
            # 去除任一因子为 NaN 的行
            valid = section.dropna()
            if len(valid) < self._min_stocks:
                continue

            a_vals = valid[factor_a].values
            b_vals = valid[factor_b].values

            # Winsorize
            a_vals = self._winsorize_array(a_vals)
            b_vals = self._winsorize_array(b_vals)

            corr, _ = stats.spearmanr(a_vals, b_vals)
            if np.isnan(corr):
                continue

            daily_corrs_scalar.append(corr)
            valid_dates.append(date_str)
            stocks_per_day.append(len(valid))

        if not daily_corrs_scalar:
            return PairwiseResult(
                factor_a=factor_a,
                factor_b=factor_b,
                correlation=0.0,
                daily_correlations=[],
                as_of_date=as_of_date,
                effective_days=0,
                avg_stocks_per_day=0.0,
            )

        # EWMA 聚合
        weights = np.array([
            self._lambda ** (len(daily_corrs_scalar) - 1 - i)
            for i in range(len(daily_corrs_scalar))
        ])
        ewma_corr = float(np.average(daily_corrs_scalar, weights=weights))

        return PairwiseResult(
            factor_a=factor_a,
            factor_b=factor_b,
            correlation=round(ewma_corr, 6),
            daily_correlations=list(zip(valid_dates, [round(c, 6) for c in daily_corrs_scalar])),
            as_of_date=as_of_date,
            effective_days=len(valid_dates),
            avg_stocks_per_day=round(np.mean(stocks_per_day), 1),
        )

    def compute_incremental(
        self,
        new_factors: List[str],
        existing_factors: List[str],
        as_of_date: Optional[str] = None,
    ) -> CorrelationResult:
        """增量计算：只算 new × (new + existing) 的子矩阵。

        当 existing_factors 为空时，等价于 compute_full_matrix(new_factors)。
        """
        all_factors = list(dict.fromkeys(existing_factors + new_factors))
        return self.compute_full_matrix(all_factors, as_of_date, save_hdf5=False)

    def get_latest_hdf5(self) -> Optional[str]:
        """获取最新的 HDF5 文件路径。"""
        if not os.path.isdir(self._hdf5_dir):
            return None
        files = sorted(
            [f for f in os.listdir(self._hdf5_dir) if f.startswith("corr_") and f.endswith(".h5")],
            reverse=True,
        )
        return os.path.join(self._hdf5_dir, files[0]) if files else None

    def get_hdf5_by_date(self, as_of_date: str) -> Optional[str]:
        """获取指定日期的 HDF5 文件路径。

        Parameters
        ----------
        as_of_date : 截止日期 (YYYY-MM-DD 或 YYYYMMDD)
        """
        fname = f"corr_{as_of_date.replace('-', '')}.h5"
        path = os.path.join(self._hdf5_dir, fname)
        return path if os.path.isfile(path) else None

    # ── 内部计算方法 ──

    def _cross_sectional_spearman(
        self,
        section: pd.DataFrame,
    ) -> Optional[np.ndarray]:
        """单日截面 Spearman 秩相关矩阵 (pandas 版，保留用于验证)。"""
        K = section.shape[1]
        valid_counts = section.count()
        usable_cols = valid_counts[valid_counts >= 30].index
        if len(usable_cols) < 2:
            return None
        sub = section[usable_cols]
        sub_corr = sub.corr(method="spearman", min_periods=30)
        sub_mat = sub_corr.values
        np.fill_diagonal(sub_mat, 1.0)
        if len(usable_cols) == K:
            return sub_mat
        corr_mat = np.full((K, K), np.nan)
        np.fill_diagonal(corr_mat, 1.0)
        col_names = list(section.columns)
        idx = np.array([col_names.index(c) for c in usable_cols])
        corr_mat[np.ix_(idx, idx)] = sub_mat
        return corr_mat

    def _cross_sectional_spearman_gemm(
        self,
        section: pd.DataFrame,
    ) -> Optional[np.ndarray]:
        """单日截面 Spearman via 5-GEMM (GPU/CPU BLAS)。

        数学等价于 pandas .corr(method='spearman', min_periods=30)：
        1. 每列独立排名 (NaN 保留)
        2. 对排名数据做 pairwise NaN Pearson = 5 次矩阵乘法

        GPU: RTX 5080 上 ~0.15ms/次 GEMM
        CPU: OpenBLAS AVX2 上 ~15ms/次 GEMM
        vs pandas nancorr: ~30 秒/天 (108K 逐对循环)
        """
        K = section.shape[1]

        valid_counts = section.count()
        usable_cols = valid_counts[valid_counts >= 30].index
        if len(usable_cols) < 2:
            return None

        sub = section[usable_cols]

        # Step 1: 每列独立排名，NaN 保留 (pandas Cython)
        ranked = sub.rank(method="average", na_option="keep")
        R = ranked.values  # (N, K_sub), float64

        # Step 2: 5-GEMM Pearson on ranks = Spearman
        nan_mask = np.isnan(R)
        M = (~nan_mask).astype(np.float64)
        X = np.where(nan_mask, 0.0, R)

        if _USE_GPU:
            sub_mat = self._gemm_pearson_gpu(X, M)
        else:
            sub_mat = self._gemm_pearson_cpu(X, M)

        if len(usable_cols) == K:
            return sub_mat

        # 嵌入完整 K×K 矩阵
        corr_mat = np.full((K, K), np.nan)
        np.fill_diagonal(corr_mat, 1.0)
        col_names = list(section.columns)
        idx = np.array([col_names.index(c) for c in usable_cols])
        corr_mat[np.ix_(idx, idx)] = sub_mat
        return corr_mat

    def _gemm_pearson_cpu(self, X: np.ndarray, M: np.ndarray) -> np.ndarray:
        """5-GEMM Pearson (CPU numpy BLAS)。"""
        N_pairs = M.T @ M
        SX = X.T @ M
        SX2 = (X ** 2).T @ M
        SXY = X.T @ X

        numerator = N_pairs * SXY - SX * SX.T
        var_x = N_pairs * SX2 - SX ** 2
        var_y = N_pairs * SX2.T - SX.T ** 2
        denominator = np.sqrt(np.maximum(var_x * var_y, 0.0))

        # 相对阈值：要求每个因子的方差 > 理论最大方差的 1%
        # rank 1..N 的方差 = N*(N^2-1)/12，对应 N*var = N^2*(N^2-1)/12
        # 这里 var_x = N*SX2 - SX^2，量级 ~ N^2 * var(rank)
        # 当大量 tied ranks 时 var 坍缩，denominator 虽 > 1e-8 但数值不稳定
        max_var = N_pairs * N_pairs * (N_pairs + 1) / 12.0  # 理论最大 N*var
        var_threshold = 0.01  # 要求方差 > 理论最大的 1%
        var_ok = (var_x > var_threshold * max_var) & (var_y > var_threshold * max_var)

        valid_pair = var_ok & (N_pairs >= 30)
        sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
        np.fill_diagonal(sub_mat, 1.0)
        return sub_mat

    def _gemm_pearson_gpu(self, X: np.ndarray, M: np.ndarray) -> np.ndarray:
        """5-GEMM Pearson (GPU PyTorch CUDA)。"""
        X_t = torch.from_numpy(X).cuda()
        M_t = torch.from_numpy(M).cuda()

        N_pairs = M_t.T @ M_t
        SX = X_t.T @ M_t
        SX2 = (X_t ** 2).T @ M_t
        SXY = X_t.T @ X_t

        numerator = N_pairs * SXY - SX * SX.T
        var_x = N_pairs * SX2 - SX ** 2
        var_y = N_pairs * SX2.T - SX.T ** 2
        denominator = torch.sqrt(torch.clamp(var_x * var_y, min=0.0))

        # 相对阈值：与 CPU 版本一致
        max_var = N_pairs * N_pairs * (N_pairs + 1) / 12.0
        var_threshold = 0.01
        var_ok = (var_x > var_threshold * max_var) & (var_y > var_threshold * max_var)

        valid_pair = var_ok & (N_pairs >= 30)
        sub_mat = torch.where(valid_pair, numerator / denominator, torch.tensor(float("nan"), device="cuda"))
        sub_mat.fill_diagonal_(1.0)

        torch.cuda.synchronize()
        return sub_mat.cpu().numpy()

    def _ewma_aggregate(
        self,
        daily_corrs: List[np.ndarray],
        dates: List[str],
    ) -> np.ndarray:
        """EWMA 时序聚合多日截面相关性矩阵（向量化）。

        w_t = λ^(T-1-t), T = 总天数
        最终 ρ̂(i,j) = Σ w_t * ρ_t(i,j) / Σ w_t (NaN-aware)
        """
        T = len(daily_corrs)

        # 权重: 最近一天权重最大, shape (T, 1, 1) 用于广播
        weights = np.array([self._lambda ** (T - 1 - t) for t in range(T)])
        weights_3d = weights[:, np.newaxis, np.newaxis]

        # 3D 数组: (T, K, K)
        corr_stack = np.stack(daily_corrs, axis=0)

        # NaN-aware 向量化加权平均
        valid_mask = ~np.isnan(corr_stack)  # (T, K, K)
        weighted_vals = np.where(valid_mask, corr_stack * weights_3d, 0.0)
        weighted_sum = np.sum(weighted_vals, axis=0)  # (K, K)
        weight_sum = np.sum(np.where(valid_mask, weights_3d, 0.0), axis=0)  # (K, K)

        # 有效天数检查
        valid_count = np.sum(valid_mask, axis=0)  # (K, K)
        min_valid = self._min_days

        result = np.where(
            (weight_sum > 0) & (valid_count >= min_valid),
            weighted_sum / weight_sum,
            np.nan,
        )
        np.fill_diagonal(result, 1.0)

        return result

    def _winsorize_cross_section(
        self,
        section: pd.DataFrame,
    ) -> pd.DataFrame:
        """截面 Winsorize: 将每列超出 [q, 1-q] 分位数的值截断。(向量化)"""
        data = section.values.copy()
        valid_counts = np.sum(~np.isnan(data), axis=0)
        q_lo = self._winsorize_q * 100
        q_hi = (1.0 - self._winsorize_q) * 100
        lo = np.nanpercentile(data, q_lo, axis=0)
        hi = np.nanpercentile(data, q_hi, axis=0)
        skip = valid_counts < 10
        lo[skip] = -np.inf
        hi[skip] = np.inf
        data = np.clip(data, lo[np.newaxis, :], hi[np.newaxis, :])
        return pd.DataFrame(data, index=section.index, columns=section.columns)

    def _winsorize_array(self, arr: np.ndarray) -> np.ndarray:
        """对单个 1D 数组做 Winsorize。"""
        lo = np.nanpercentile(arr, self._winsorize_q * 100)
        hi = np.nanpercentile(arr, (1.0 - self._winsorize_q) * 100)
        return np.clip(arr, lo, hi)
