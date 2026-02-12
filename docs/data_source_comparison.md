# daily_pv.h5 和 CSV 文件导出数据源对比分析

## 导出流程对比

### CSV 文件导出流程

**文件**: `backend/qlib_exporter/router.py`

```python
_db_reader = DBReader()

def _export_daily_to_csv_for_dump_bin(
    snapshot_id: str,
    start: date,
    end: date,
    exchanges: List[str],
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
) -> Path:
    """从 DB 导出日线宽表为 CSV，供 dump_bin.py 使用.

    CSV 结构：date,symbol,open,high,low,close,volume,amount
    """
    # 从 DB 读取数据
    df = _db_reader.load_qlib_daily_data_all(
        start=start,
        end=end,
        exchanges=list(exchanges) if exchanges else None,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
    )

    # 将 Qlib 宽表转换成 dump_bin.py 期望的 CSV 结构
    df_reset = df.reset_index()
    df_reset["date"] = df_reset["datetime"].dt.date.astype(str)
    df_reset["symbol"] = df_reset["instrument"].astype(str)
    df_csv = df_reset[csv_cols]

    # 按股票拆分为多个 CSV 文件
    for symbol, g in df_csv.groupby("symbol"):
        csv_path = csv_dir / f"{symbol}.csv"
        g.to_csv(csv_path, index=False)

    return csv_dir
```

**关键点**:
- 使用 `_db_reader.load_qlib_daily_data_all` 从数据库读取数据
- 数据源：`market.kline_daily_raw` 表（不复权日线）
- 支持过滤条件：`exclude_st`、`exclude_delisted_or_paused`、`exchanges`

### daily_pv.h5 导出流程

**文件**: `backend/qlib_exporter/exporter.py`

```python
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

        return ExportResult(
            snapshot_id=snapshot_id,
            freq="1d",
            start=start,
            end=end,
            ts_codes=codes,
            rows=int(df.shape[0]),
        )
```

**关键点**:
- 使用 `self.db.load_qlib_daily_data_all` 从数据库读取数据
- 数据源：`market.kline_daily_raw` 表（不复权日线）
- 支持过滤条件：`exclude_st`、`exclude_delisted_or_paused`、`exchanges`
- 使用 `use_tushare_adj=True` 获取 Tushare 复权因子

## 数据源对比

### DBReader 数据源

**文件**: `backend/qlib_exporter/db_reader.py`

```python
class DBReader:
    """封装针对前复权日线表和分钟线表的读取逻辑."""

    def load_qlib_daily_data_all(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载全部股票的 Qlib 格式日线数据.

        Returns:
            DataFrame
                - Index: MultiIndex (datetime, instrument)
                - Columns: open, high, low, close, volume, amount, factor
        """
        # SQL 查询
        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        # 按交易所过滤
        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e and e.strip()}
            if "sh" in normalized:
                conditions.append("ts_code LIKE '%.SH'")
            if "sz" in normalized:
                conditions.append("ts_code LIKE '%.SZ'")
            if "bj" in normalized:
                conditions.append("ts_code LIKE '%.BJ'")

        # 排除 ST 股票
        if exclude_st:
            conditions.append("name NOT LIKE '%ST%'")

        # 排除退市或暂停上市股票
        if exclude_delisted_or_paused:
            conditions.append("(list_status NOT IN ('D', 'P'))")

        # 执行 SQL 查询
        sql = f"""
            SELECT
                trade_date as datetime,
                ts_code as instrument,
                open_li as open,
                high_li as high,
                low_li as low,
                close_li as close,
                volume_hand as volume,
                amount_li as amount
            FROM market.kline_daily_raw
            WHERE {' AND '.join(conditions)}
            ORDER BY trade_date, ts_code
        """

        df = pd.read_sql(sql, conn)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["instrument"] = df["instrument"].apply(self._normalize_ts_code)
        df = df.set_index(["datetime", "instrument"])
        return df
```

**关键点**:
- 数据源：`market.kline_daily_raw` 表
- 查询字段：`trade_date`, `ts_code`, `open_li`, `high_li`, `low_li`, `close_li`, `volume_hand`, `amount_li`
- 支持过滤条件：`exchanges`、`exclude_st`、`exclude_delisted_or_paused`

## 数据源一致性分析

### 相同点

1. **数据源相同**
   - CSV 文件导出：使用 `_db_reader.load_qlib_daily_data_all`
   - daily_pv.h5 导出：使用 `self.db.load_qlib_daily_data_all`
   - 两者都从 `market.kline_daily_raw` 表读取数据

2. **DBReader 实例相同**
   - CSV 文件导出：使用全局 `_db_reader = DBReader()`
   - daily_pv.h5 导出：使用 `self.db = db or DBReader()`
   - 两者都使用同一个 DBReader 类

3. **过滤条件相同**
   - CSV 文件导出：支持 `exclude_st`、`exclude_delisted_or_paused`、`exchanges`
   - daily_pv.h5 导出：支持 `exclude_st`、`exclude_delisted_or_paused`、`exchanges`
   - 两者都支持相同的过滤条件

4. **数据格式相同**
   - CSV 文件导出：返回 MultiIndex (datetime, instrument) 的 DataFrame
   - daily_pv.h5 导出：返回 MultiIndex (datetime, instrument) 的 DataFrame
   - 两者都返回相同格式的数据

### 不同点

1. **复权因子处理**
   - CSV 文件导出：不包含复权因子
   - daily_pv.h5 导出：使用 `use_tushare_adj=True` 获取 Tushare 复权因子
   - daily_pv.h5 包含 `factor` 字段，CSV 文件不包含

2. **输出格式**
   - CSV 文件导出：输出为 CSV 文件（每只股票一个文件）
   - daily_pv.h5 导出：输出为 HDF5 文件（宽表格式）

3. **日期对齐**
   - CSV 文件导出：无日期对齐，直接输出所有数据
   - daily_pv.h5 导出：无日期对齐，直接输出所有数据
   - bin 文件（从 CSV 转换）：使用 dump_bin.py 的 `data_merge_calendar` 方法进行日期对齐

## 结论

### 数据源一致性

**daily_pv.h5 和 CSV 文件使用相同的数据源**：
- 都使用 DBReader 从 `market.kline_daily_raw` 表读取数据
- 都支持相同的过滤条件（`exclude_st`、`exclude_delisted_or_paused`、`exchanges`）
- 都返回相同格式的数据（MultiIndex DataFrame）

### 日期缺失原因

**日期缺失不是数据源问题**，而是数据处理方式不同：

1. **daily_pv.h5 和 CSV 文件**
   - 直接输出所有数据，无日期对齐
   - 如果某只股票在某交易日没有数据，该股票在该日期就不会出现

2. **bin 文件（从 CSV 转换）**
   - 使用 dump_bin.py 的 `data_merge_calendar` 方法
   - 根据 `calendars/day.txt` 对齐数据
   - 只保留日历中存在的日期
   - 如果股票在某交易日没有数据，填充 NaN

### 建议

1. **统一数据处理方式**
   - 建议在导出 daily_pv.h5 时也使用日历对齐
   - 确保日期完整性

2. **文档说明**
   - 在文档中说明两种数据格式的差异
   - 说明日期缺失的原因和处理方式

3. **数据质量检查**
   - 定期检查数据质量
   - 确保数据完整性
