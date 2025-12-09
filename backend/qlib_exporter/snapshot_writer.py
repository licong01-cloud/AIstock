from __future__ import annotations

"""Qlib 日频 Snapshot 写入工具.

负责将从数据库读取的日频前复权宽表写入指定 snapshot 目录：
- daily_pv.h5 宽表（MultiIndex: datetime, instrument）
- meta.json（时间范围、股票池大小、字段列表等）
- instruments/all.txt
- calendars/day.txt
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import json

import pandas as pd

from .config import QLIB_MARKET, ensure_snapshot_root


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

        # 通过 reset_index / set_index 强制将索引各级转换为普通 numpy dtype
        tmp = df.reset_index()
        tmp["datetime"] = pd.to_datetime(tmp["datetime"], utc=False)
        tmp["instrument"] = tmp["instrument"].astype(str)
        df = tmp.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        h5_path = snapshot_dir / "daily_pv.h5"
        df.to_hdf(h5_path, key="data", mode="w")

        instruments_dir = snapshot_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)
        all_txt = instruments_dir / "all.txt"

        inst_group = df.reset_index().groupby("instrument")["datetime"]
        lines: List[str] = []
        for inst, series in inst_group:
            series_sorted = series.sort_values()
            start_dt = series_sorted.iloc[0].strftime("%Y-%m-%d")
            end_dt = series_sorted.iloc[-1].strftime("%Y-%m-%d")
            # Qlib expects instruments/all.txt as CSV with 3 columns: instrument,start,end
            lines.append(f"{inst},{start_dt},{end_dt}")

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
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in tmp.columns:
                tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
        df = tmp.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        h5_path = snapshot_dir / "minute_1min.h5"
        df.to_hdf(h5_path, key="data", mode="w", format="fixed")

        instruments_dir = snapshot_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)
        all_txt = instruments_dir / "all.txt"

        inst_group = df.reset_index().groupby("instrument")["datetime"]
        lines: List[str] = []
        for inst, series in inst_group:
            series_sorted = series.sort_values()
            start_dt = series_sorted.iloc[0].strftime("%Y-%m-%d")
            end_dt = series_sorted.iloc[-1].strftime("%Y-%m-%d")
            lines.append(f"{inst} {start_dt} {end_dt}")

        all_txt.write_text("\n".join(lines), encoding="utf-8")

        calendars_dir = snapshot_dir / "calendars"
        calendars_dir.mkdir(parents=True, exist_ok=True)
        # Qlib expects calendar filenames whose stem is a valid freq string, e.g. "day", "1min".
        # Use "1min.txt" here so Freq("1min") can be parsed correctly.
        minute_txt = calendars_dir / "1min.txt"

        unique_ts = df.index.get_level_values("datetime").drop_duplicates().sort_values()
        minute_lines = [d.strftime("%Y-%m-%d %H:%M:%S") for d in unique_ts]
        minute_txt.write_text("\n".join(minute_lines), encoding="utf-8")

    def write_board_daily_full(self, snapshot_id: str, df: pd.DataFrame) -> None:
        """全量写入指定 snapshot 的板块日线数据.

        输出到 boards/board_daily.h5，索引要求为 MultiIndex[datetime, board]。
        """

        if df.empty:
            raise ValueError("write_board_daily_full: 输入 DataFrame 为空，无法生成板块行情")

        if not isinstance(df.index, pd.MultiIndex) or df.index.names != ["datetime", "board"]:
            raise ValueError("write_board_daily_full: DataFrame 索引必须为 MultiIndex[datetime, board]")

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        boards_dir.mkdir(parents=True, exist_ok=True)

        df = df.sort_index()

        h5_path = boards_dir / "board_daily.h5"
        df.to_hdf(h5_path, key="data", mode="w")

    def write_board_index(self, snapshot_id: str, df: pd.DataFrame) -> None:
        """写入板块索引数据到 boards/board_index.h5.

        输入 DataFrame 应包含列：trade_date, ts_code, name, idx_type, idx_count。
        """

        if df.empty:
            raise ValueError("write_board_index: 输入 DataFrame 为空")

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        boards_dir.mkdir(parents=True, exist_ok=True)

        # 确保数据类型正确
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
        for col in ["ts_code", "name", "idx_type"]:
            if col in df.columns:
                df[col] = df[col].astype("object")
        if "idx_count" in df.columns:
            df["idx_count"] = pd.to_numeric(df["idx_count"], errors="coerce").fillna(0).astype("float64")

        h5_path = boards_dir / "board_index.h5"
        df.to_hdf(h5_path, key="data", mode="w", format="fixed")

    def write_board_member(self, snapshot_id: str, df: pd.DataFrame) -> None:
        """写入板块成员数据到 boards/board_member.h5.

        输入 DataFrame 应包含列：trade_date, ts_code, con_code, con_name。
        """

        if df.empty:
            raise ValueError("write_board_member: 输入 DataFrame 为空")

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        boards_dir.mkdir(parents=True, exist_ok=True)

        # 确保数据类型正确
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
        for col in ["ts_code", "con_code", "con_name"]:
            if col in df.columns:
                df[col] = df[col].astype("object")

        h5_path = boards_dir / "board_member.h5"
        df.to_hdf(h5_path, key="data", mode="w", format="fixed")

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

        snapshot_dir = self._snapshot_path(snapshot_id)
        h5_path = snapshot_dir / f"minute_{freq}.h5"

        # 数据类型标准化
        tmp = df_new.reset_index()
        tmp["datetime"] = pd.to_datetime(tmp["datetime"], utc=False).values
        tmp["instrument"] = tmp["instrument"].astype("object")
        for col in ["open", "high", "low", "close", "volume", "amount"]:
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

    def write_board_daily_incremental(self, snapshot_id: str, df_new: pd.DataFrame) -> None:
        """增量追加板块日线数据."""
        if df_new.empty:
            return

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        h5_path = boards_dir / "board_daily.h5"

        if h5_path.exists():
            df_old = pd.read_hdf(h5_path, key="data")
            df_combined = pd.concat([df_old, df_new])
            df_combined = df_combined[~df_combined.index.duplicated(keep="last")]
            df_combined = df_combined.sort_index()
        else:
            boards_dir.mkdir(parents=True, exist_ok=True)
            df_combined = df_new.sort_index()

        df_combined.to_hdf(h5_path, key="data", mode="w", format="fixed")

    def write_board_index_incremental(self, snapshot_id: str, df_new: pd.DataFrame) -> None:
        """增量追加板块索引数据."""
        if df_new.empty:
            return

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        h5_path = boards_dir / "board_index.h5"

        # 数据类型标准化
        df_new = df_new.copy()
        df_new["trade_date"] = pd.to_datetime(df_new["trade_date"], utc=False)
        for col in ["ts_code", "name", "idx_type"]:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype("object")
        if "idx_count" in df_new.columns:
            df_new["idx_count"] = pd.to_numeric(df_new["idx_count"], errors="coerce").fillna(0).astype("float64")

        if h5_path.exists():
            df_old = pd.read_hdf(h5_path, key="data")
            df_combined = pd.concat([df_old, df_new])
            # 按 trade_date + ts_code 去重
            df_combined = df_combined.drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
            df_combined = df_combined.sort_values(["trade_date", "ts_code"])
        else:
            boards_dir.mkdir(parents=True, exist_ok=True)
            df_combined = df_new.sort_values(["trade_date", "ts_code"])

        df_combined.to_hdf(h5_path, key="data", mode="w", format="fixed")

    def write_board_member_incremental(self, snapshot_id: str, df_new: pd.DataFrame) -> None:
        """增量追加板块成员数据."""
        if df_new.empty:
            return

        snapshot_dir = self._snapshot_path(snapshot_id)
        boards_dir = snapshot_dir / "boards"
        h5_path = boards_dir / "board_member.h5"

        # 数据类型标准化
        df_new = df_new.copy()
        df_new["trade_date"] = pd.to_datetime(df_new["trade_date"], utc=False)
        for col in ["ts_code", "con_code", "con_name"]:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype("object")

        if h5_path.exists():
            df_old = pd.read_hdf(h5_path, key="data")
            df_combined = pd.concat([df_old, df_new])
            # 按 trade_date + ts_code + con_code 去重
            df_combined = df_combined.drop_duplicates(subset=["trade_date", "ts_code", "con_code"], keep="last")
            df_combined = df_combined.sort_values(["trade_date", "ts_code", "con_code"])
        else:
            boards_dir.mkdir(parents=True, exist_ok=True)
            df_combined = df_new.sort_values(["trade_date", "ts_code", "con_code"])

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
        df_out = df.copy()

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
