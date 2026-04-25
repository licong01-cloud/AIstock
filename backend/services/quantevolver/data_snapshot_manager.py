"""
数据快照管理器 — 为因子 IC 计算提供基于时间点的数据快照。

核心思路：
- data_date (YYYYMMDD) 标识一个数据快照时间点
- 首次执行：从 DB 加载行情 + 静态因子数据，写入磁盘 parquet
- 后续执行：直接从磁盘读取，零 DB 访问
- 所有因子共享同一快照，保证横向可比

快照存储结构：
    rdagent_assets/factor_values/snapshots/
      20260403/
        realtime_kline.parquet      # OHLCV + adj_factor（前复权后）
        static_factors.parquet      # 7 张表合并的静态因子
        _snapshot_meta.json         # 元数据
"""
from __future__ import annotations

import gc
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("aistock.quantevolver.data_snapshot_manager")

_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
_SNAPSHOT_BASE = os.path.join(
    _PROJECT_ROOT, "rdagent_assets", "factor_values_realtime", "snapshots"
)

_REALTIME_FILE = "realtime_kline.parquet"
_STATIC_FILE = "static_factors.parquet"
_META_FILE = "_snapshot_meta.json"
_CREATING_SENTINEL = ".creating"

# 默认起始日期（覆盖完整模型训练期 + 样本外期）
DEFAULT_START_DATE = "2018-08-01"


def _validate_data_date(data_date: str) -> None:
    """校验 data_date 格式必须为 YYYYMMDD，否则抛异常。"""
    if not (len(data_date) == 8 and data_date.isdigit()):
        raise ValueError(
            f"data_date 格式错误: '{data_date}'，必须为 YYYYMMDD（如 '20260403'）"
        )
    # 校验日期合法性
    try:
        datetime.strptime(data_date, "%Y%m%d")
    except ValueError:
        raise ValueError(
            f"data_date 日期无效: '{data_date}'，无法解析为合法日期"
        )


def _parse_data_date(data_date: str) -> str:
    """YYYYMMDD → YYYY-MM-DD，格式错误直接抛异常。"""
    _validate_data_date(data_date)
    return f"{data_date[:4]}-{data_date[4:6]}-{data_date[6:8]}"


class DataSnapshotManager:
    """管理因子计算数据快照的创建、读取、列举和删除。"""

    def __init__(self, base_dir: Optional[str] = None):
        self._base = base_dir or _SNAPSHOT_BASE
        os.makedirs(self._base, exist_ok=True)

    # ── 路径工具 ──

    def _snap_dir(self, data_date: str) -> str:
        return os.path.join(self._base, data_date)

    def _realtime_path(self, data_date: str) -> str:
        return os.path.join(self._snap_dir(data_date), _REALTIME_FILE)

    def _static_path(self, data_date: str) -> str:
        return os.path.join(self._snap_dir(data_date), _STATIC_FILE)

    def _meta_path(self, data_date: str) -> str:
        return os.path.join(self._snap_dir(data_date), _META_FILE)

    def _sentinel_path(self, data_date: str) -> str:
        return os.path.join(self._snap_dir(data_date), _CREATING_SENTINEL)

    # ── 公共接口 ──

    def snapshot_exists(self, data_date: str) -> bool:
        """检查快照是否完整（两个 parquet + meta 均存在，且无创建中标志）。"""
        _validate_data_date(data_date)
        d = self._snap_dir(data_date)
        if not os.path.isdir(d):
            return False
        if os.path.isfile(self._sentinel_path(data_date)):
            return False
        return (
            os.path.isfile(self._realtime_path(data_date))
            and os.path.isfile(self._static_path(data_date))
            and os.path.isfile(self._meta_path(data_date))
        )

    def create_snapshot(
        self,
        data_date: str,
        instruments: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """从 DB 加载数据并创建磁盘快照。

        Parameters
        ----------
        data_date : 快照标识 (YYYYMMDD)
        instruments : 股票池
        start_date : 起始日期；None 则 end_date - 730 天
        end_date : 截止日期；None 则从 data_date 解析

        Returns
        -------
        dict : 快照元数据
        """
        if end_date is None:
            end_date = _parse_data_date(data_date)
        if start_date is None:
            start_date = DEFAULT_START_DATE

        snap_dir = self._snap_dir(data_date)
        sentinel = self._sentinel_path(data_date)

        # 防止并发创建
        os.makedirs(snap_dir, exist_ok=True)
        if os.path.isfile(sentinel):
            logger.warning(f"快照 {data_date} 正在由另一进程创建，等待...")
            # 简单等待：最多 600 秒
            for _ in range(120):
                time.sleep(5)
                if not os.path.isfile(sentinel):
                    if self.snapshot_exists(data_date):
                        logger.info(f"快照 {data_date} 已由其他进程创建完成")
                        return self._load_meta(data_date, raise_on_error=True)
            raise RuntimeError(f"等待快照 {data_date} 创建超时")

        # 写入哨兵
        with open(sentinel, "w") as f:
            f.write(datetime.now().isoformat())

        try:
            meta = self._do_create(data_date, instruments, start_date, end_date)
            # 创建成功，删除哨兵
            os.remove(sentinel)
            return meta
        except Exception:
            # 创建失败，清理不完整目录
            logger.error(f"快照 {data_date} 创建失败，清理目录", exc_info=True)
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    def _do_create(
        self,
        data_date: str,
        instruments: Optional[List[str]],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """实际执行快照创建（从 DB 加载 → 写 parquet）。"""
        from ...data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
        from ...data_service.qe_data_service import build_static_factors

        timings: Dict[str, float] = {}

        # instruments=None 时获取全市场股票列表
        if instruments is None:
            from .evaluation_universe_service import EvaluationUniverseService
            instruments = EvaluationUniverseService().get_official_universe(as_of_date=end_date)
            logger.info(f"[快照 {data_date}] 使用官方评估股票池: {len(instruments)} 只")

        # 1. 加载行情数据
        logger.info(
            f"[快照 {data_date}] 加载行情数据: "
            f"{'全市场' if instruments is None else f'{len(instruments)} 只股票'}, "
            f"{start_date} ~ {end_date}"
        )
        t0 = time.time()
        loader = RealtimeFactorDataLoader()
        realtime_df = loader.load(
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            fields=["open", "close", "high", "low", "volume", "amount", "factor"],
            adjust="qfq",
        )
        timings["realtime"] = round(time.time() - t0, 1)
        realtime_rows_raw = len(realtime_df)

        # ── 过滤非交易日数据 ──
        # 双重过滤确保数据准确：
        # 1. trading_calendar 过滤（排除法定节假日、调休日）
        # 2. 全市场无成交日兜底过滤（交易日历可能有误，如春节调休标记错误）
        #
        # 不处理个股停牌 — 个股 amount=0 是正常的停牌状态，保留为 NaN

        # Step 1: 从 trading_calendar 获取交易日集合
        trading_dates: Optional[set] = None
        try:
            from ...db.pg_pool import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT cal_date FROM market.trading_calendar "
                        "WHERE cal_date >= %s AND cal_date <= %s AND is_trading = true",
                        (start_date, end_date),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        raise RuntimeError(
                            f"trading_calendar 在 {start_date}~{end_date} 范围内无交易日记录，"
                            f"请检查 market.trading_calendar 表数据是否完整"
                        )
                    trading_dates = set(pd.Timestamp(r[0]) for r in rows)
                    logger.info(f"[快照 {data_date}] 交易日历: {len(trading_dates)} 个交易日")
        except Exception as e:
            # 交易日历查询失败不能静默跳过 — 会导致非交易日数据污染快照
            raise RuntimeError(f"交易日历查询失败，无法创建准确的快照: {e}") from e

        # Step 2: 用交易日历过滤 realtime
        dates_idx = realtime_df.index.get_level_values(0)
        calendar_mask = dates_idx.isin(trading_dates)
        removed_by_calendar = (~calendar_mask).sum()
        if removed_by_calendar > 0:
            non_trading_dates = sorted(set(dates_idx[~calendar_mask].unique()))
            realtime_df = realtime_df.loc[calendar_mask]
            logger.info(
                f"[快照 {data_date}] 交易日历过滤: 移除 {removed_by_calendar} 行 "
                f"({len(non_trading_dates)} 天非交易日)"
            )

        # Step 3: 全市场无成交日兜底过滤
        # 如果某天 >95% 的股票 amount=0，视为非交易日（交易日历标记错误）
        dates_idx = realtime_df.index.get_level_values(0)
        daily_zero_rate = realtime_df.groupby(dates_idx)["amount"].apply(
            lambda x: (x == 0).mean()
        )
        suspicious_days = daily_zero_rate[daily_zero_rate > 0.95].index
        if len(suspicious_days) > 0:
            mask = ~dates_idx.isin(suspicious_days)
            removed_rows = (~mask).sum()
            realtime_df = realtime_df.loc[mask]
            logger.warning(
                f"[快照 {data_date}] 全市场无成交日过滤: 移除 {len(suspicious_days)} 天 ({removed_rows} 行), "
                f"日期: {[str(d.date()) for d in sorted(suspicious_days)]}"
            )
            # 同步从 trading_dates 中移除（确保 static 也过滤）
            trading_dates -= set(suspicious_days)

        realtime_rows = len(realtime_df)
        logger.info(
            f"[快照 {data_date}] 行情数据: {realtime_rows} 行 (原始 {realtime_rows_raw}), "
            f"{realtime_df.memory_usage(deep=True).sum() / 1024**2:.0f} MB, "
            f"耗时 {timings['realtime']}s"
        )

        # 保存行情 parquet
        realtime_df.to_parquet(
            self._realtime_path(data_date),
            engine="pyarrow", compression="snappy",
        )
        del realtime_df
        gc.collect()

        # 2. 加载静态因子数据
        logger.info(f"[快照 {data_date}] 加载静态因子数据...")
        t0 = time.time()
        static_df = build_static_factors(instruments, start_date, end_date)
        timings["static"] = round(time.time() - t0, 1)
        static_rows_raw = len(static_df)

        # 对 static_factors 做同样的交易日过滤（和 realtime 保持完全一致）
        s_dates = static_df.index.get_level_values(0)
        s_mask = s_dates.isin(trading_dates)
        removed_s = (~s_mask).sum()
        if removed_s > 0:
            static_df = static_df.loc[s_mask]
            logger.info(f"[快照 {data_date}] 静态因子交易日过滤: 移除 {removed_s} 行")

        static_rows = len(static_df)
        static_cols = len(static_df.columns)
        logger.info(
            f"[快照 {data_date}] 静态因子: {static_rows} 行 (原始 {static_rows_raw}) × {static_cols} 列, "
            f"{static_df.memory_usage(deep=True).sum() / 1024**2:.0f} MB, "
            f"耗时 {timings['static']}s"
        )

        # 保存静态因子 parquet
        static_df.to_parquet(
            self._static_path(data_date),
            engine="pyarrow", compression="snappy",
        )
        del static_df
        gc.collect()

        # 3. 写入元数据
        meta = {
            "data_date": data_date,
            "start_date": start_date,
            "end_date": end_date,
            "instruments_count": len(instruments),
            "trading_days": len(trading_dates),
            "created_at": datetime.now().isoformat(),
            "realtime_rows": realtime_rows,
            "realtime_rows_raw": realtime_rows_raw,
            "static_rows": static_rows,
            "static_rows_raw": static_rows_raw,
            "static_columns": static_cols,
            "filtered_suspicious_days": [str(d.date()) for d in sorted(suspicious_days)] if len(suspicious_days) > 0 else [],
            "timings": timings,
        }
        with open(self._meta_path(data_date), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        logger.info(
            f"[快照 {data_date}] 创建完成: 行情={realtime_rows}行, "
            f"静态={static_rows}行×{static_cols}列, "
            f"耗时 realtime={timings['realtime']}s + static={timings['static']}s"
        )
        return meta

    def load_realtime(self, data_date: str) -> pd.DataFrame:
        """从磁盘加载行情快照到内存。"""
        path = self._realtime_path(data_date)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"行情快照不存在: {path}")
        t0 = time.time()
        df = pd.read_parquet(path)
        elapsed = round(time.time() - t0, 1)
        logger.info(
            f"[快照 {data_date}] 行情数据已加载: {len(df)} 行, "
            f"{df.memory_usage(deep=True).sum() / 1024**2:.0f} MB, "
            f"耗时 {elapsed}s"
        )
        return df

    def load_static(self, data_date: str) -> pd.DataFrame:
        """从磁盘加载静态因子快照到内存。"""
        path = self._static_path(data_date)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"静态因子快照不存在: {path}")
        t0 = time.time()
        df = pd.read_parquet(path)
        elapsed = round(time.time() - t0, 1)
        logger.info(
            f"[快照 {data_date}] 静态因子已加载: {len(df)} 行 × {len(df.columns)} 列, "
            f"{df.memory_usage(deep=True).sum() / 1024**2:.0f} MB, "
            f"耗时 {elapsed}s"
        )
        return df

    def load_static_columns(
        self, data_date: str, columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """按列裁剪加载静态因子快照（节省内存）。"""
        path = self._static_path(data_date)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"静态因子快照不存在: {path}")
        return pd.read_parquet(path, columns=columns)

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """列出所有可用快照及其元数据。"""
        results = []
        if not os.path.isdir(self._base):
            return results

        for name in sorted(os.listdir(self._base), reverse=True):
            d = os.path.join(self._base, name)
            if not os.path.isdir(d):
                continue
            # 跳过正在创建的
            if os.path.isfile(os.path.join(d, _CREATING_SENTINEL)):
                results.append({"data_date": name, "status": "creating"})
                continue

            meta = self._load_meta(name)
            if meta:
                meta["status"] = "ready"
                # 追加磁盘占用
                total_size = 0
                for fn in (_REALTIME_FILE, _STATIC_FILE):
                    fp = os.path.join(d, fn)
                    if os.path.isfile(fp):
                        total_size += os.path.getsize(fp)
                meta["disk_size_mb"] = round(total_size / 1024 / 1024, 1)
                results.append(meta)
            else:
                results.append({"data_date": name, "status": "incomplete"})

        return results

    def delete_snapshot(self, data_date: str) -> bool:
        """删除指定快照目录。"""
        d = self._snap_dir(data_date)
        if not os.path.isdir(d):
            return False
        shutil.rmtree(d)
        logger.info(f"快照已删除: {data_date}")
        return True

    # ── 内部工具 ──

    def _load_meta(self, data_date: str, raise_on_error: bool = False) -> Optional[Dict[str, Any]]:
        p = self._meta_path(data_date)
        if not os.path.isfile(p):
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"快照 {data_date} 元数据读取失败: {e}")
            if raise_on_error:
                raise
            return None

    def load_meta(self, data_date: str) -> Dict[str, Any]:
        """读取快照元数据（公共接口）。不存在则抛异常。"""
        meta = self._load_meta(data_date, raise_on_error=True)
        if meta is None:
            raise FileNotFoundError(f"快照 {data_date} 元数据不存在")
        return meta
