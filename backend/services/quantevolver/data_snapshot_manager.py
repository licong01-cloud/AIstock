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
    _PROJECT_ROOT, "rdagent_assets", "factor_values", "snapshots"
)

_REALTIME_FILE = "realtime_kline.parquet"
_STATIC_FILE = "static_factors.parquet"
_META_FILE = "_snapshot_meta.json"
_CREATING_SENTINEL = ".creating"

# 默认回看天数（2 年）
DEFAULT_LOOKBACK_DAYS = 730


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
            ed = datetime.strptime(end_date, "%Y-%m-%d")
            start_date = (ed - timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

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
        instruments: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """实际执行快照创建（从 DB 加载 → 写 parquet）。"""
        from ...data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
        from ...data_service.qe_data_service import build_static_factors

        timings: Dict[str, float] = {}

        # 1. 加载行情数据
        logger.info(
            f"[快照 {data_date}] 加载行情数据: {len(instruments)} 只股票, "
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
        realtime_rows = len(realtime_df)
        logger.info(
            f"[快照 {data_date}] 行情数据: {realtime_rows} 行, "
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
        static_rows = len(static_df)
        static_cols = len(static_df.columns)
        logger.info(
            f"[快照 {data_date}] 静态因子: {static_rows} 行 × {static_cols} 列, "
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
            "created_at": datetime.now().isoformat(),
            "realtime_rows": realtime_rows,
            "static_rows": static_rows,
            "static_columns": static_cols,
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
