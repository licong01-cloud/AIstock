from __future__ import annotations

"""Qlib 数据导出协调器.

封装从 DB 读取 → 生成宽表 → 写入 Snapshot 的完整流程。
支持全量和增量导出。
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Sequence
import psycopg2
import pandas as pd

from ..db.pg_pool import get_conn
from .db_reader import DBReader
from .meta_repo import MetaRepo
from .snapshot_writer import SnapshotWriter


@dataclass
class ExportResult:
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int


def _resolve_export_codes(
    db: DBReader,
    ts_codes: Optional[Iterable[str]],
    *,
    start: date,
    end: date,
    exchanges: Optional[Sequence[str]],
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
) -> List[str]:
    if ts_codes is not None:
        codes = list(ts_codes)
        if not codes:
            raise ValueError("export: ts_codes 为空")
        return codes
    return db.get_base_ts_codes(
        start=start,
        end=end,
        exchanges=list(exchanges) if exchanges else None,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
    )


class QlibDailyExporter:
    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """执行一次日频前复权数据的全量导出.

        - 若 ts_codes 为 None，则自动读取全部 ts_code。
        - 仅导出 [start, end] 区间内的数据。
        """

        # 与因子导出保持一致：使用 Qlib 格式日线（前复权 + factor + amount）
        if ts_codes is None:
            df = self.db.load_qlib_daily_data_all(
                start,
                end,
                exchanges=list(exchanges) if exchanges else None,
                use_tushare_adj=True,
                exclude_st=exclude_st,
                exclude_delisted_or_paused=exclude_delisted_or_paused,
            )
            codes = df.index.get_level_values("instrument").unique().tolist() if not df.empty else []
        else:
            codes = list(ts_codes)
            if not codes:
                raise ValueError("export_full: ts_codes 为空，无法导出 Snapshot")
            df = self.db.load_qlib_daily_data(codes, start, end, use_tushare_adj=True)

        if df.empty:
            raise ValueError("export_full: 指定区间内无数据")

        self.writer.write_daily_full(snapshot_id, df)

        # 更新元数据
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "daily", max_dt)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="1d",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出日频数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "daily")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="1d",
                start=start, end=end, ts_codes=[], rows=0,
            )

        if ts_codes is None:
            df = self.db.load_qlib_daily_data_all(
                start, end,
                exchanges=list(exchanges) if exchanges else None,
                use_tushare_adj=True,
                exclude_st=exclude_st,
                exclude_delisted_or_paused=exclude_delisted_or_paused,
            )
        else:
            codes = list(ts_codes)
            if not codes:
                raise ValueError("export_incremental: ts_codes 为空")
            df = self.db.load_qlib_daily_data(codes, start, end, use_tushare_adj=True)

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="1d",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "daily_pv.h5")

        # 增量后重新生成辅助文件
        self._regenerate_auxiliary_files(snapshot_id)

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "daily", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="1d",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )

    def _regenerate_auxiliary_files(self, snapshot_id: str) -> None:
        """读取合并后的 daily_pv.h5，重新生成 instruments/all.txt + calendars/day.txt + meta.json."""
        import json
        import logging

        from .config import IPO_FILTER_DAYS

        _logger = logging.getLogger(__name__)

        snap_dir = self.writer.root / snapshot_id
        h5_path = snap_dir / "daily_pv.h5"
        if not h5_path.exists():
            return

        df = pd.read_hdf(str(h5_path), key="data")

        # 加载 list_date 用于 IPO 过滤（与 snapshot_writer 一致）
        list_date_map = self.writer._load_list_date_map()
        ipo_delta = timedelta(days=IPO_FILTER_DAYS)

        # instruments/all.txt
        instruments_dir = snap_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)
        grp = df.groupby(level="instrument")
        lines = []
        skipped_ipo = 0
        for inst, sub in grp:
            dts = sub.index.get_level_values("datetime")
            data_start = dts.min()
            data_end = dts.max()

            # IPO 过滤：上市不满 IPO_FILTER_DAYS 天的数据不纳入
            list_date = list_date_map.get(str(inst))
            if list_date is not None:
                ipo_eligible = list_date + ipo_delta
                effective_start = max(data_start, ipo_eligible)
            else:
                effective_start = data_start

            if effective_start > data_end:
                skipped_ipo += 1
                continue

            s = effective_start.strftime("%Y-%m-%d")
            e = data_end.strftime("%Y-%m-%d")
            lines.append(f"{inst}\t{s}\t{e}")

        if skipped_ipo > 0:
            _logger.info(
                "[exporter] IPO 过滤: %d 只股票因上市不满 %d 天被排除",
                skipped_ipo, IPO_FILTER_DAYS,
            )
        (instruments_dir / "all.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

        # calendars/day.txt
        calendars_dir = snap_dir / "calendars"
        calendars_dir.mkdir(parents=True, exist_ok=True)
        dates = sorted(df.index.get_level_values("datetime").unique())
        date_lines = [d.strftime("%Y-%m-%d") for d in dates]
        (calendars_dir / "day.txt").write_text("\n".join(date_lines) + "\n", encoding="utf-8")

        # meta.json
        meta = {
            "snapshot_id": snapshot_id,
            "market": "cn",
            "start": dates[0].strftime("%Y-%m-%d") if dates else "",
            "end": dates[-1].strftime("%Y-%m-%d") if dates else "",
            "instruments": len(grp),
            "columns": list(df.columns),
            "generated_at": datetime.now().isoformat(),
        }
        (snap_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )


class QlibMinuteExporter:
    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def _filter_by_exchange(
        self, codes: List[str], exchanges: Optional[Sequence[str]]
    ) -> List[str]:
        """按交易所过滤 ts_code."""
        if exchanges is None:
            return codes
        normalized = {e.strip().lower() for e in exchanges if e.strip()}

        def _match(code: str) -> bool:
            uc = code.upper()
            if uc.endswith(".SH"):
                return "sh" in normalized
            if uc.endswith(".SZ"):
                return "sz" in normalized
            if uc.endswith(".BJ"):
                return "bj" in normalized
            return True

        return [c for c in codes if _match(c)]

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        freq: str = "1m",
        batch_days: int = 30,
    ) -> ExportResult:
        """全量导出分钟线数据.

        使用分批加载机制，每次加载 batch_days 天的数据，避免内存溢出。

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            ts_codes: 股票代码列表（可选）
            exchanges: 交易所过滤（可选）
            freq: 频率，默认 1m
            batch_days: 每批加载天数，默认 30 天
        """
        if ts_codes is None:
            codes = self.db.get_base_ts_codes(
                start=start,
                end=end,
                exchanges=list(exchanges) if exchanges else None,
                exclude_st=exclude_st,
                exclude_delisted_or_paused=exclude_delisted_or_paused,
            )
        else:
            codes = list(ts_codes)

        if not codes:
            raise ValueError("export_full: ts_codes 为空（可能被交易所过滤条件排除），无法导出分钟 Snapshot")

        total_rows = 0
        max_dt = None
        is_first_batch = True

        # 分批加载和写入
        for batch_start, batch_end, df in self.db.load_minute_batched(
            codes, start, end, freq=freq, batch_days=batch_days
        ):
            if df.empty:
                continue

            if is_first_batch:
                # 第一批：全量写入（覆盖）
                self.writer.write_minute_full(snapshot_id, df, freq=freq)
                is_first_batch = False
            else:
                # 后续批次：增量追加
                self.writer.write_minute_incremental(snapshot_id, df, freq=freq)

            total_rows += len(df)
            batch_max_dt = df.index.get_level_values("datetime").max()
            if max_dt is None or batch_max_dt > max_dt:
                max_dt = batch_max_dt

        if total_rows == 0:
            raise ValueError("export_full: 指定区间内无分钟线数据")

        # 更新元数据
        self.meta.ensure_table()
        if max_dt:
            self.meta.upsert_last_datetime(snapshot_id, f"minute_{freq}", max_dt)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq=freq,
            start=start,
            end=end,
            ts_codes=codes,
            rows=total_rows,
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        freq: str = "1m",
        batch_days: int = 30,
    ) -> ExportResult:
        """增量导出分钟线数据.

        使用分批加载机制，从上次导出的最后时间点开始，导出到 end 日期。
        如果没有历史记录，则从 end - 7 天开始。

        Args:
            snapshot_id: Snapshot ID
            end: 结束日期
            ts_codes: 股票代码列表（可选）
            exchanges: 交易所过滤（可选）
            freq: 频率，默认 1m
            batch_days: 每批加载天数，默认 30 天
        """
        self.meta.ensure_table()

        # 获取上次导出的最后时间
        last_dt = self.meta.get_last_datetime(snapshot_id, f"minute_{freq}")
        if last_dt:
            # 从上次的下一秒开始
            start = (last_dt + timedelta(seconds=1)).date()
        else:
            # 没有历史记录，默认导出最近 7 天
            start = end - timedelta(days=7)

        if start > end:
            # 已经是最新的
            return ExportResult(
                snapshot_id=snapshot_id,
                freq=freq,
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        if ts_codes is None:
            codes = self.db.get_base_ts_codes(
                start=start,
                end=end,
                exchanges=list(exchanges) if exchanges else None,
                exclude_st=exclude_st,
                exclude_delisted_or_paused=exclude_delisted_or_paused,
            )
        else:
            codes = list(ts_codes)
            codes = self._filter_by_exchange(codes, exchanges)

        if not codes:
            raise ValueError("export_incremental: ts_codes 为空")

        total_rows = 0
        max_dt = None

        # 分批加载和写入
        for batch_start, batch_end, df in self.db.load_minute_batched(
            codes, start, end, freq=freq, batch_days=batch_days
        ):
            if df.empty:
                continue

            self.writer.write_minute_incremental(snapshot_id, df, freq=freq)

            total_rows += len(df)
            batch_max_dt = df.index.get_level_values("datetime").max()
            if max_dt is None or batch_max_dt > max_dt:
                max_dt = batch_max_dt

        # 更新元数据
        if max_dt:
            self.meta.upsert_last_datetime(snapshot_id, f"minute_{freq}", max_dt)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq=freq,
            start=start,
            end=end,
            ts_codes=codes,
            rows=total_rows,
        )


class QlibDailyBasicExporter:
    """Tushare daily_basic 指标导出协调器.

    输出文件 daily_basic.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: db_* 系列字段（估值、市值、换手率等）
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "daily_basic.h5",
    ) -> ExportResult:
        """全量导出 daily_basic 指标数据到 Snapshot.

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            exchanges: 可选，交易所过滤
            exclude_st: 是否剔除 ST 股票
            exclude_delisted_or_paused: 是否剔除退市或暂停上市股票
            filename: 输出文件名，默认为 daily_basic.h5
        """

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_daily_basic_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无 daily_basic 数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        # 更新元数据：记录该 Snapshot 的 daily_basic 最新日期
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "daily_basic", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="daily_basic",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 daily_basic 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "daily_basic")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="daily_basic",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_daily_basic_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="daily_basic",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "daily_basic.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "daily_basic", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="daily_basic",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )


class QlibMoneyflowExporter:
    """个股资金流向（moneyflow_ts）导出协调器.

    输出文件 moneyflow.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: mf_* 系列字段，_vol 为股，_amt 为元
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "moneyflow.h5",
    ) -> ExportResult:
        """全量导出 moneyflow_ts 数据到 Snapshot.

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            exchanges: 可选，交易所过滤
            exclude_st: 是否剔除 ST 股票
            exclude_delisted_or_paused: 是否剔除退市或暂停上市股票
            filename: 输出文件名，默认为 moneyflow.h5
        """

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_moneyflow_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无资金流向数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        # 更新元数据：记录该 Snapshot 的 moneyflow 最新日期
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "moneyflow", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="moneyflow",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 moneyflow 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "moneyflow")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="moneyflow",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_moneyflow_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="moneyflow",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "moneyflow.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "moneyflow", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="moneyflow",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )


class QlibBakBasicExporter:
    """Tushare bak_basic 历史股票列表数据导出协调器.

    输出文件 bak_basic.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: bb_* 系列字段（pe, float_share, total_share 等）
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "bak_basic.h5",
    ) -> ExportResult:
        """全量导出 bak_basic 指标数据到 Snapshot.

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            exchanges: 可选，交易所过滤
            exclude_st: 是否剔除 ST 股票
            exclude_delisted_or_paused: 是否剔除退市或暂停上市股票
            filename: 输出文件名，默认为 bak_basic.h5
        """

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_bak_basic_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无 bak_basic 数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        # 更新元数据：记录该 Snapshot 的 bak_basic 最新日期
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "bak_basic", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="bak_basic",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 bak_basic 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "bak_basic")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="bak_basic",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_bak_basic_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="bak_basic",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "bak_basic.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "bak_basic", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="bak_basic",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )


class QlibCyqPerfExporter:
    """Tushare cyq_perf 每日筹码及胜率数据导出协调器.

    输出文件 cyq_perf.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: cp_* 系列字段（his_low, his_high, cost_5pct, winner_rate 等）
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "cyq_perf.h5",
    ) -> ExportResult:
        """全量导出 cyq_perf 指标数据到 Snapshot.

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            exchanges: 可选，交易所过滤
            exclude_st: 是否剔除 ST 股票
            exclude_delisted_or_paused: 是否剔除退市或暂停上市股票
            filename: 输出文件名，默认为 cyq_perf.h5
        """

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_cyq_perf_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无 cyq_perf 数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        # 更新元数据：记录该 Snapshot 的 cyq_perf 最新日期
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "cyq_perf", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="cyq_perf",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 cyq_perf 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "cyq_perf")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="cyq_perf",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_cyq_perf_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="cyq_perf",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "cyq_perf.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "cyq_perf", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="cyq_perf",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )


class QlibMarginDetailExporter:
    """Tushare margin_detail 融资融券明细数据导出协调器.

    输出文件 margin_detail.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: md_* 系列字段（rzye, rqye, rzmre, rqyl, rzche, rqchl, rqmcl, rzrqye）
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "margin_detail.h5",
    ) -> ExportResult:
        """全量导出 margin_detail 融资融券明细数据到 Snapshot."""

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_margin_detail_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无 margin_detail 数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "margin_detail", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="margin_detail",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 margin_detail 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "margin_detail")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="margin_detail",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_margin_detail_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="margin_detail",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "margin_detail.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "margin_detail", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="margin_detail",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )


class QlibSectorDataExporter:
    """申万行业板块数据 (sector_data) 导出协调器.

    输出文件 sector_data.h5，供 Qlib / RD-Agent 使用：
    - Index: MultiIndex (datetime, instrument)
    - Columns: sw2_* 22 numeric columns plus l2_code_id
    """

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        filename: str = "sector_data.h5",
    ) -> ExportResult:
        """全量导出 sector_data 到 Snapshot."""

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_sector_data_panel(
            start=start,
            end=end,
            ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            raise ValueError("export_full: 指定区间内无 sector_data 数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "sector_data", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="sector_data",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: Optional[Iterable[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> ExportResult:
        """增量导出 sector_data 数据。从上次导出位置继续。"""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "sector_data")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id, freq="sector_data",
                start=start, end=end, ts_codes=[], rows=0,
            )

        codes = _resolve_export_codes(
            self.db,
            ts_codes,
            start=start, end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )
        df = self.db.load_sector_data_panel(
            start=start, end=end, ts_codes=codes,
            exchanges=list(exchanges) if exchanges else None,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id, freq="sector_data",
                start=start, end=end, ts_codes=[], rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, "sector_data.h5")

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "sector_data", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()
        return ExportResult(
            snapshot_id=snapshot_id, freq="sector_data",
            start=start, end=end,
            ts_codes=instruments, rows=int(df.shape[0]),
        )
