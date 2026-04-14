from __future__ import annotations

"""Qlib 日频 Snapshot 写入工具.

负责将从数据库读取的日频前复权宽表写入指定 snapshot 目录：
- daily_pv.h5 宽表（MultiIndex: datetime, instrument）
- meta.json（时间范围、股票池大小、字段列表等）
- instruments/all.txt
- calendars/day.txt
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import json
import logging

import pandas as pd

from .config import IPO_FILTER_DAYS, QLIB_MARKET, ensure_snapshot_root

logger = logging.getLogger(__name__)


@dataclass
class SnapshotMeta:
    snapshot_id: str
    market: str
    start: str
    end: str
    instruments: int
    columns: List[str]
    generated_at: str


class SnapshotWriter:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or ensure_snapshot_root()

    def _load_list_date_map(self) -> Dict[str, pd.Timestamp]:
        """从 market.stock_basic 加载 ts_code -> list_date 映射."""
        try:
            from backend.db.pg_pool import get_conn
            sql = "SELECT ts_code, list_date FROM market.stock_basic WHERE list_date IS NOT NULL"
            with get_conn() as conn:
                df = pd.read_sql(sql, conn)
            if df.empty:
                return {}
            df["list_date"] = pd.to_datetime(df["list_date"], utc=False)
            return dict(zip(df["ts_code"].astype(str), df["list_date"]))
        except Exception as e:
            logger.warning("无法加载 stock_basic.list_date，跳过 IPO 过滤: %s", e)
            return {}

    def _normalize_dollar_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        rename_map = {c: c[1:] for c in df.columns if isinstance(c, str) and c.startswith("$")}
        if not rename_map:
            return df
        return df.rename(columns=rename_map)

    def _snapshot_path(self, snapshot_id: str) -> Path:
        path = self.root / snapshot_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_daily_full(self, snapshot_id: str, df: pd.DataFrame) -> None:
        """全量写入指定 snapshot 的日频数据.

        - 覆盖原有 daily_pv.h5（如存在）
        - 重新生成 meta.json / instruments/all.txt / calendars/day.txt
        """

        if df.empty:
            raise ValueError("write_daily_full: 输入 DataFrame 为空，无法生成 Snapshot")

        if not isinstance(df.index, pd.MultiIndex) or df.index.names != ["datetime", "instrument"]:
            raise ValueError("write_daily_full: DataFrame 索引必须为 MultiIndex[datetime, instrument]")

        snapshot_dir = self._snapshot_path(snapshot_id)

        # 排序并规范化索引 dtype，避免 Pandas 在保存带有扩展 dtype 的 MultiIndex 到 HDF5 时出错
        df = df.sort_index()

        df = self._normalize_dollar_columns(df)

        # 通过 reset_index / set_index 强制将索引各级转换为普通 numpy dtype
        tmp = df.reset_index()
        tmp["datetime"] = pd.to_datetime(tmp["datetime"], utc=False)
        tmp["instrument"] = tmp["instrument"].astype(str)

        # 强制数值列为 float，避免 HDF5 写入时出现 int64（例如 amount 变成全 0）
        if "amount" not in tmp.columns:
            tmp["amount"] = float("nan")
        for col in ["open", "high", "low", "close", "volume", "amount", "factor"]:
            if col in tmp.columns:
                tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
        df = tmp.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        h5_path = snapshot_dir / "daily_pv.h5"
        df.to_hdf(h5_path, key="data", mode="w")

        instruments_dir = snapshot_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)
        all_txt = instruments_dir / "all.txt"

        # 加载 list_date 用于 IPO 过滤：上市不满 IPO_FILTER_DAYS 天的数据不纳入
        list_date_map = self._load_list_date_map()
        ipo_delta = timedelta(days=IPO_FILTER_DAYS)

        inst_group = df.reset_index().groupby("instrument")["datetime"]
        lines: List[str] = []
        skipped_ipo = 0
        for inst, series in inst_group:
            series_sorted = series.sort_values()
            data_start = series_sorted.iloc[0]
            data_end = series_sorted.iloc[-1]

            # 计算 IPO 过滤后的 start_date: max(数据首日, list_date + IPO_FILTER_DAYS)
            list_date = list_date_map.get(str(inst))
            if list_date is not None:
                ipo_eligible = list_date + ipo_delta
                effective_start = max(data_start, ipo_eligible)
            else:
                effective_start = data_start

            # 如果过滤后 start > end，说明该股票在数据范围内全部处于 IPO 保护期
            if effective_start > data_end:
                skipped_ipo += 1
                continue

            start_dt = effective_start.strftime("%Y-%m-%d")
            end_dt = data_end.strftime("%Y-%m-%d")
            # Qlib expects instruments/all.txt as CSV with 3 columns: instrument,start,end
            lines.append(f"{inst},{start_dt},{end_dt}")

        if skipped_ipo > 0:
            logger.info(
                "[SnapshotWriter] IPO 过滤: %d 只股票因上市不满 %d 天被排除",
                skipped_ipo, IPO_FILTER_DAYS,
            )

        all_txt.write_text("\n".join(lines), encoding="utf-8")

        calendars_dir = snapshot_dir / "calendars"
        calendars_dir.mkdir(parents=True, exist_ok=True)
        day_txt = calendars_dir / "day.txt"

        unique_days = (
            df.index.get_level_values("datetime").normalize().drop_duplicates().sort_values()
        )
        day_lines = [d.strftime("%Y-%m-%d") for d in unique_days]
        day_txt.write_text("\n".join(day_lines), encoding="utf-8")

        # 生成 meta.json
        # unique_days 是 DatetimeIndex，可以直接按位置索引
        start_str = unique_days[0].strftime("%Y-%m-%d")
        end_str = unique_days[-1].strftime("%Y-%m-%d")
        # 使用本地时区时间，避免前端显示与实际时区不一致
        local_now = datetime.now(timezone.utc).astimezone()
        meta = SnapshotMeta(
            snapshot_id=snapshot_id,
            market=QLIB_MARKET,
            start=start_str,
            end=end_str,
            instruments=len(inst_group),
            columns=list(df.columns),
            generated_at=local_now.isoformat(),
        )
        meta_path = snapshot_dir / "meta.json"
        meta_path.write_text(json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8")

    def write_minute_full(self, snapshot_id: str, df: pd.DataFrame, freq: str = "1m") -> None:
        if df.empty:
            raise ValueError("write_minute_full: 输入 DataFrame 为空，无法生成 Snapshot")

        if not isinstance(df.index, pd.MultiIndex) or df.index.names != ["datetime", "instrument"]:
            raise ValueError("write_minute_full: DataFrame 索引必须为 MultiIndex[datetime, instrument]")

        snapshot_dir = self._snapshot_path(snapshot_id)

        # 排序并规范化索引 dtype，避免 Pandas 在保存带有扩展 dtype 的 MultiIndex 到 HDF5 时出错
        df = df.sort_index()

        # 通过 reset_index / set_index 强制将索引各级转换为普通 numpy dtype
        tmp = df.reset_index()
        tmp["datetime"] = pd.to_datetime(tmp["datetime"], utc=False).values  # numpy datetime64
        tmp["instrument"] = tmp["instrument"].astype("object")  # 强制 object dtype 而非 StringDtype
        # 强制将所有数值列转为 float64，避免扩展 dtype（如 Int64）导致 HDF5 写入失败
        for col in ["open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down"]:
            if col in tmp.columns:
                tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
        df = tmp.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        h5_path = snapshot_dir / "minute_1min.h5"
        df.to_hdf(h5_path, key="data", mode="w", format="fixed")

        instruments_dir = snapshot_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)
        all_txt = instruments_dir / "all.txt"

        # 加载 list_date 用于 IPO 过滤（与 write_daily_full 一致）
        list_date_map = self._load_list_date_map()
        ipo_delta = timedelta(days=IPO_FILTER_DAYS)

        inst_group = df.reset_index().groupby("instrument")["datetime"]
        lines: List[str] = []
        skipped_ipo = 0
        for inst, series in inst_group:
            series_sorted = series.sort_values()
            data_start = series_sorted.iloc[0]
            data_end = series_sorted.iloc[-1]

            list_date = list_date_map.get(str(inst))
            if list_date is not None:
                ipo_eligible = list_date + ipo_delta
                effective_start = max(data_start, ipo_eligible)
            else:
                effective_start = data_start

            if effective_start > data_end:
                skipped_ipo += 1
                continue

            start_dt = effective_start.strftime("%Y-%m-%d")
            end_dt = data_end.strftime("%Y-%m-%d")
            lines.append(f"{inst} {start_dt} {end_dt}")

        if skipped_ipo > 0:
            logger.info(
                "[SnapshotWriter] minute IPO 过滤: %d 只股票因上市不满 %d 天被排除",
                skipped_ipo, IPO_FILTER_DAYS,
            )

        all_txt.write_text("\n".join(lines), encoding="utf-8")

        calendars_dir = snapshot_dir / "calendars"
        calendars_dir.mkdir(parents=True, exist_ok=True)
        # Qlib expects calendar filenames whose stem is a valid freq string, e.g. "day", "1min".
        # Use "1min.txt" here so Freq("1min") can be parsed correctly.
        minute_txt = calendars_dir / "1min.txt"

        unique_ts = df.index.get_level_values("datetime").drop_duplicates().sort_values()
        minute_lines = [d.strftime("%Y-%m-%d %H:%M:%S") for d in unique_ts]
        minute_txt.write_text("\n".join(minute_lines), encoding="utf-8")

    # =========================================================================
    # 增量写入方法
    # =========================================================================

    def write_minute_incremental(
        self, snapshot_id: str, df_new: pd.DataFrame, freq: str = "1m"
    ) -> None:
        """增量追加分钟线数据到现有 HDF5 文件.

        如果文件不存在，则创建新文件。
        如果文件存在，则追加新数据（去重）。
        """
        if df_new.empty:
            return

        df_new = self._normalize_dollar_columns(df_new)

        snapshot_dir = self._snapshot_path(snapshot_id)
        h5_path = snapshot_dir / f"minute_{freq}.h5"

        # 数据类型标准化
        tmp = df_new.reset_index()
        tmp["datetime"] = pd.to_datetime(tmp["datetime"], utc=False).values
        tmp["instrument"] = tmp["instrument"].astype("object")
        for col in ["open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down"]:
            if col in tmp.columns:
                tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
        df_new = tmp.set_index(["datetime", "instrument"])

        if h5_path.exists():
            # 读取现有数据
            df_old = pd.read_hdf(h5_path, key="data")
            # 合并并去重（保留新数据）
            df_combined = pd.concat([df_old, df_new])
            df_combined = df_combined[~df_combined.index.duplicated(keep="last")]
            df_combined = df_combined.sort_index()
        else:
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            df_combined = df_new.sort_index()

        df_combined.to_hdf(h5_path, key="data", mode="w", format="fixed")

    # =========================================================================
    # RD-Agent 因子数据写入（daily_pv.h5 格式）
    # =========================================================================

    def write_factor_data(
        self,
        snapshot_id: str,
        df: pd.DataFrame,
        filename: str = "daily_pv.h5",
    ) -> None:
        """写入 RD-Agent 因子数据格式.

        Args:
            snapshot_id: Snapshot ID
            df: 符合 RD-Agent 格式的 DataFrame
                - Index: MultiIndex (datetime, instrument)
                - Columns: $open, $close, $high, $low, $volume, $factor
            filename: 输出文件名，默认 daily_pv.h5
        """
        if df.empty:
            return

        snap_dir = ensure_snapshot_root() / snapshot_id
        snap_dir.mkdir(parents=True, exist_ok=True)
        h5_path = snap_dir / filename

        # 确保数据格式正确
        df_out = self._normalize_dollar_columns(df.copy())

        # 确保索引名称正确
        if df_out.index.names != ["datetime", "instrument"]:
            raise ValueError(f"DataFrame index must be ['datetime', 'instrument'], got {df_out.index.names}")

        # 写入 HDF5
        df_out.to_hdf(h5_path, key="data", mode="w", format="fixed")

    def write_factor_data_incremental(
        self,
        snapshot_id: str,
        df_new: pd.DataFrame,
        filename: str = "daily_pv.h5",
    ) -> None:
        """增量写入 RD-Agent 因子数据.

        Args:
            snapshot_id: Snapshot ID
            df_new: 新增数据
            filename: 输出文件名
        """
        if df_new.empty:
            return

        snap_dir = ensure_snapshot_root() / snapshot_id
        h5_path = snap_dir / filename

        if h5_path.exists():
            df_old = pd.read_hdf(h5_path, key="data")
            df_combined = pd.concat([df_old, df_new])
            # 按 datetime + instrument 去重
            df_combined = df_combined[~df_combined.index.duplicated(keep="last")]
            df_combined = df_combined.sort_index()
        else:
            snap_dir.mkdir(parents=True, exist_ok=True)
            df_combined = df_new.sort_index()

        df_combined.to_hdf(h5_path, key="data", mode="w", format="fixed")
