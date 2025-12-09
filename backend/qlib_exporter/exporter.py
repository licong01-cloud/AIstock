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


class QlibDailyExporter:
    def __init__(self, db: Optional[DBReader] = None, writer: Optional[SnapshotWriter] = None) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()

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

        if ts_codes is None:
            codes = self.db.get_all_ts_codes()
        else:
            codes = list(ts_codes)

        # 按交易所过滤 ts_code（通过后缀推断 .SH / .SZ / .BJ），若未指定则不过滤
        if exchanges is not None:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}

            def _match_exchange(code: str) -> bool:
                uc = code.upper()
                if uc.endswith(".SH"):
                    return "sh" in normalized
                if uc.endswith(".SZ"):
                    return "sz" in normalized
                if uc.endswith(".BJ"):
                    return "bj" in normalized
                # 未能识别交易所后缀时，保守起见保留
                return True

            codes = [c for c in codes if _match_exchange(c)]

        # 按 ST / 退市 / 暂停上市状态过滤股票代码
        if exclude_st or exclude_delisted_or_paused:
            excluded: set[str] = set()
            with get_conn() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    if exclude_st:
                        cur.execute("SELECT DISTINCT ts_code FROM market.stock_st")
                        excluded.update(row[0] for row in cur.fetchall())
                    if exclude_delisted_or_paused:
                        cur.execute(
                            "SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P')",
                        )
                        excluded.update(row[0] for row in cur.fetchall())

            if excluded:
                codes = [c for c in codes if c not in excluded]

        if not codes:
            raise ValueError(
                "export_full: ts_codes 为空（可能被交易所 / ST / 退市过滤条件排除），无法导出 Snapshot",
            )

        df = self.db.load_daily(codes, start, end)
        if df.empty:
            raise ValueError("export_full: 指定区间内无数据")

        self.writer.write_daily_full(snapshot_id, df)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="1d",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )


class QlibBoardExporter:
    """TDX 板块日线导出协调器."""

    def __init__(self, db: Optional[DBReader] = None, writer: Optional[SnapshotWriter] = None) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        board_codes: Optional[Iterable[str]] = None,
        idx_types: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """执行一次板块日线行情的全量导出.

        - 若 board_codes 为 None，则从 tdx_board_index 中读取全部或指定类型的板块代码。
        - 仅导出 [start, end] 区间内的数据。
        """

        if board_codes is None:
            codes = self.db.get_all_board_codes(list(idx_types) if idx_types else None)
        else:
            codes = list(board_codes)

        if not codes:
            raise ValueError("export_full: board_codes 为空，无法导出板块 Snapshot")

        df = self.db.load_board_daily(codes, start, end)
        if df.empty:
            raise ValueError("export_full: 指定区间内无板块日线数据")

        self.writer.write_board_daily_full(snapshot_id, df)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="1d_board",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
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
            codes = self.db.get_all_ts_codes_minute()
        else:
            codes = list(ts_codes)

        codes = self._filter_by_exchange(codes, exchanges)

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
            codes = self.db.get_all_ts_codes_minute()
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


class QlibBoardIndexExporter:
    """TDX 板块索引导出协调器."""

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
        idx_types: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """导出板块索引数据（tdx_board_index）到 boards/board_index.h5."""

        df = self.db.load_board_index(start, end, list(idx_types) if idx_types else None)
        if df.empty:
            raise ValueError("export_full: 指定区间内无板块索引数据")

        self.writer.write_board_index(snapshot_id, df)

        # 更新元数据
        self.meta.ensure_table()
        max_dt = df["trade_date"].max()
        self.meta.upsert_last_datetime(snapshot_id, "board_index", max_dt)

        codes = df["ts_code"].unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="board_index",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        idx_types: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """增量导出板块索引数据."""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "board_index")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="board_index",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        df = self.db.load_board_index(start, end, list(idx_types) if idx_types else None)
        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="board_index",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        self.writer.write_board_index_incremental(snapshot_id, df)

        max_dt = df["trade_date"].max()
        self.meta.upsert_last_datetime(snapshot_id, "board_index", max_dt)

        codes = df["ts_code"].unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="board_index",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )


class QlibBoardMemberExporter:
    """TDX 板块成员导出协调器."""

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
        board_codes: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """导出板块成员数据（tdx_board_member）到 boards/board_member.h5."""

        df = self.db.load_board_member(start, end, list(board_codes) if board_codes else None)
        if df.empty:
            raise ValueError("export_full: 指定区间内无板块成员数据")

        self.writer.write_board_member(snapshot_id, df)

        # 更新元数据
        self.meta.ensure_table()
        max_dt = df["trade_date"].max()
        self.meta.upsert_last_datetime(snapshot_id, "board_member", max_dt)

        codes = df["ts_code"].unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="board_member",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        board_codes: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """增量导出板块成员数据."""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "board_member")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="board_member",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        df = self.db.load_board_member(start, end, list(board_codes) if board_codes else None)
        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="board_member",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        self.writer.write_board_member_incremental(snapshot_id, df)

        max_dt = df["trade_date"].max()
        self.meta.upsert_last_datetime(snapshot_id, "board_member", max_dt)

        codes = df["ts_code"].unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="board_member",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )


class QlibBoardDailyExporter:
    """TDX 板块日线增量导出协调器."""

    def __init__(
        self,
        db: Optional[DBReader] = None,
        writer: Optional[SnapshotWriter] = None,
        meta: Optional[MetaRepo] = None,
    ) -> None:
        self.db = db or DBReader()
        self.writer = writer or SnapshotWriter()
        self.meta = meta or MetaRepo()

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        board_codes: Optional[Iterable[str]] = None,
        idx_types: Optional[Sequence[str]] = None,
    ) -> ExportResult:
        """增量导出板块日线数据."""
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "board_daily")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="1d_board",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        if board_codes is None:
            codes = self.db.get_all_board_codes(list(idx_types) if idx_types else None)
        else:
            codes = list(board_codes)

        if not codes:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="1d_board",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        df = self.db.load_board_daily(codes, start, end)
        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="1d_board",
                start=start,
                end=end,
                ts_codes=codes,
                rows=0,
            )

        self.writer.write_board_daily_incremental(snapshot_id, df)

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "board_daily", max_dt)

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="1d_board",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )


class QlibFactorExporter:
    """RD-Agent 因子数据导出协调器.

    导出 daily_pv.h5 格式的因子数据，供 RD-Agent 使用。
    
    数据格式：
    - Index: MultiIndex (datetime, instrument)
    - Columns: $open, $close, $high, $low, $volume, $factor
    - $close = 不复权价格 × 前复权因子（已复权价格）
    - $factor = 前复权因子
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
        filename: str = "daily_pv.h5",
        use_tushare_adj: bool = True,
    ) -> ExportResult:
        """全量导出因子数据.

        使用新的复权策略：
        - $close = 不复权价格 × 前复权因子
        - $factor = 前复权因子 = adj_factor / 最新adj_factor

        Args:
            snapshot_id: Snapshot ID
            start: 开始日期
            end: 结束日期
            ts_codes: 可选，指定股票代码列表
            exchanges: 可选，交易所过滤
            filename: 输出文件名
            use_tushare_adj: 是否使用 Tushare 复权因子（默认 True）

        Returns:
            ExportResult
        """
        # 使用新的 Qlib 格式数据加载方法
        if ts_codes:
            df = self.db.load_qlib_daily_data(
                list(ts_codes), start, end, use_tushare_adj=use_tushare_adj
            )
        else:
            df = self.db.load_qlib_daily_data_all(
                start, end, 
                exchanges=list(exchanges) if exchanges else None,
                use_tushare_adj=use_tushare_adj
            )

        if df.empty:
            raise ValueError("export_full: 指定区间内无因子数据")

        self.writer.write_factor_data(snapshot_id, df, filename)

        # 更新元数据
        self.meta.ensure_table()
        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "factor_data", max_dt)

        # 获取唯一 instrument 列表
        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="factor",
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
        filename: str = "daily_pv.h5",
        use_tushare_adj: bool = True,
    ) -> ExportResult:
        """增量导出因子数据.

        Args:
            snapshot_id: Snapshot ID
            end: 结束日期
            ts_codes: 可选，指定股票代码列表
            exchanges: 可选，交易所过滤
            filename: 输出文件名
            use_tushare_adj: 是否使用 Tushare 复权因子

        Returns:
            ExportResult
        """
        self.meta.ensure_table()

        last_dt = self.meta.get_last_datetime(snapshot_id, "factor_data")
        if last_dt:
            start = (last_dt + timedelta(days=1)).date()
        else:
            start = end - timedelta(days=30)

        if start > end:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="factor",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        # 使用新的 Qlib 格式数据加载方法
        if ts_codes:
            df = self.db.load_qlib_daily_data(
                list(ts_codes), start, end, use_tushare_adj=use_tushare_adj
            )
        else:
            df = self.db.load_qlib_daily_data_all(
                start, end,
                exchanges=list(exchanges) if exchanges else None,
                use_tushare_adj=use_tushare_adj
            )

        if df.empty:
            return ExportResult(
                snapshot_id=snapshot_id,
                freq="factor",
                start=start,
                end=end,
                ts_codes=[],
                rows=0,
            )

        self.writer.write_factor_data_incremental(snapshot_id, df, filename)

        max_dt = df.index.get_level_values("datetime").max()
        self.meta.upsert_last_datetime(snapshot_id, "factor_data", max_dt)

        instruments = df.index.get_level_values("instrument").unique().tolist()

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="factor",
            start=start,
            end=end,
            ts_codes=instruments,
            rows=int(df.shape[0]),
        )
