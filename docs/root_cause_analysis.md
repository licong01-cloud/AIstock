# daily_pv.h5 和 bin 文件数据缺失根本原因分析

## 问题背景

- daily_pv.h5 缺失 70 个日期（2010-03-01 到 2014-07-15）
- bin 文件包含这 70 个日期
- 两者都使用相同的数据源（market.kline_daily_raw 表）
- 两者都使用相同的 DBReader.load_qlib_daily_data_all() 方法

## 导出流程对比

### daily_pv.h5 导出流程

**文件**: `backend/qlib_exporter/exporter.py`

```python
class QlibDailyExporter:
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
- 调用 `self.db.load_qlib_daily_data_all()` 获取数据
- 直接写入 DataFrame，无日期过滤
- `write_daily_full()` 生成 calendars/day.txt

### bin 文件导出流程

**文件**: `backend/qlib_exporter/router.py`

```python
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
    df = _db_reader.load_qlib_daily_data_all(
        start=start,
        end=end,
        exchanges=list(exchanges) if exchanges else None,
        use_tushare_adj=True,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
    )

    if df.empty:
        raise HTTPException(status_code=400, detail="指定区间内无可导出的日线数据（可能被过滤条件排除）")

    # 将 Qlib 宽表转换成 dump_bin.py 期望的 CSV 结构
    df_reset = df.reset_index()
    df_reset["date"] = df_reset["datetime"].dt.date.astype(str)
    df_reset["symbol"] = df_reset["instrument"].astype(str)
    df_csv = df_reset[csv_cols]

    # 为兼容 dump_bin.py dump_all 的行为，这里按 symbol 拆分为多个文件：每只股票一个 CSV。
    for symbol, g in df_csv.groupby("symbol"):
        csv_path = csv_dir / f"{symbol}.csv"
        g.to_csv(csv_path, index=False)

    return csv_dir
```

**关键点**:
- 调用 `_db_reader.load_qlib_daily_data_all()` 获取数据
- 直接写入 CSV 文件，无日期过滤
- dump_bin.py 的 `data_merge_calendar()` 方法根据 calendars/day.txt 对齐数据

## 根本原因分析

### calendars/day.txt 的生成逻辑

**文件**: `backend/qlib_exporter/snapshot_writer.py`

```python
def write_daily_full(self, snapshot_id: str, df: pd.DataFrame) -> None:
    """全量写入指定 snapshot 的日频数据.

    - 覆盖原有 daily_pv.h5（如存在）
    - 重新生成 meta.json / instruments/all.txt / calendars/day.txt
    """

    # ... 写入 daily_pv.h5 ...

    # 生成 calendars/day.txt
    calendars_dir = snapshot_dir / "calendars"
    calendars_dir.mkdir(parents=True, exist_ok=True)
    day_txt = calendars_dir / "day.txt"

    unique_days = (
        df.index.get_level_values("datetime").normalize().drop_duplicates().sort_values()
    )
    day_lines = [d.strftime("%Y-%m-%d") for d in unique_days]
    day_txt.write_text("\n".join(day_lines), encoding="utf-8")
```

**关键点**:
- calendars/day.txt 是从 DataFrame 的 datetime 索引生成的
- 只包含 DataFrame 中存在的日期
- 如果某只股票在某交易日没有数据，该日期就不会出现在 calendars/day.txt 中

### dump_bin.py 的日期对齐逻辑

**文件**: `RD-Agent-main/scripts/dump_bin.py`

```python
def data_merge_calendar(self, df: pd.DataFrame, calendars_list: List[pd.Timestamp]) -> pd.DataFrame:
    # calendars
    calendars_df = pd.DataFrame(data=calendars_list, columns=[self.date_field_name])
    calendars_df[self.date_field_name] = calendars_df[self.date_field_name].astype("datetime64[ns]")
    cal_df = calendars_df[
        (calendars_df[self.date_field_name] >= df[self.date_field_name].min())
        & (calendars_df[self.date_field_name] <= df[self.date_field_name].max())
    ]
    # align index
    cal_df.set_index(self.date_field_name, inplace=True)
    df.set_index(self.date_field_name, inplace=True)
    r_df = df.reindex(cal_df.index)
    return r_df

def _data_to_bin(self, df: pd.DataFrame, calendar_list: List[pd.Timestamp], features_dir: Path):
    if df.empty:
        logger.warning(f"{features_dir.name} data is None or empty")
        return
    if not calendar_list:
        logger.warning("calendar_list is empty")
        return
    # align index
    _df = self.data_merge_calendar(df, calendar_list)
    if _df.empty:
        logger.warning(f"{features_dir.name} data is not in calendars")
        return
    # used when creating a bin file
    date_index = self.get_datetime_index(_df, calendar_list)
    for field in self.get_dump_fields(_df.columns):
        bin_path = features_dir.joinpath(f"{field.lower()}.{self.freq}{self.DUMP_FILE_SUFFIX}")
        if field not in _df.columns:
            continue
        if bin_path.exists() and self._mode == self.UPDATE_MODE:
            # update
            with bin_path.open("ab") as fp:
                np.array(_df[field]).astype("<f").tofile(fp)
        else:
            # append; self._mode == self.ALL_MODE or not bin_path.exists()
            np.hstack([date_index, _df[field]]).astype("<f").tofile(str(bin_path.resolve()))
```

**关键点**:
- dump_bin.py 使用 `data_merge_calendar()` 方法，根据 calendars/day.txt 对齐数据
- 只保留日历中存在的日期
- 如果股票在某交易日没有数据，填充 NaN

## 根本原因

### daily_pv.h5 的 calendars/day.txt

daily_pv.h5 的 calendars/day.txt 是从 DataFrame 的 datetime 索引生成的，只包含 DataFrame 中存在的日期。

**关键问题**：
- 如果某只股票在某交易日没有数据，该日期就不会出现在 DataFrame 中
- 该日期也不会出现在 calendars/day.txt 中
- calendars/day.txt 只包含有数据的日期

### bin 文件的 calendars/day.txt

bin 文件的 calendars/day.txt 是从 CSV 文件生成的，CSV 文件是从 DB 导出的。

**关键问题**：
- CSV 文件包含所有有数据的日期
- dump_bin.py 使用 `data_merge_calendar()` 方法，根据 calendars/day.txt 对齐数据
- 如果股票在某交易日没有数据，填充 NaN

### 数据缺失的根本原因

**daily_pv.h5 的 calendars/day.txt 只包含有数据的日期**，而 bin 文件的 calendars/day.txt 包含所有交易日。

**具体原因**：
1. daily_pv.h5 导出时，calendars/day.txt 是从 DataFrame 的 datetime 索引生成的
2. 如果某只股票在某交易日没有数据，该日期就不会出现在 DataFrame 中
3. 该日期也不会出现在 calendars/day.txt 中
4. bin 文件导出时，CSV 文件包含所有有数据的日期
5. dump_bin.py 使用 `data_merge_calendar()` 方法，根据 calendars/day.txt 对齐数据
6. 如果股票在某交易日没有数据，填充 NaN

### 为什么 daily_pv.h5 缺失了 70 个日期？

**原因**：
- daily_pv.h5 的 calendars/day.txt 只包含有数据的日期
- 这 70 个日期在 daily_pv.h5 的 DataFrame 中没有数据（某些股票在这些日期没有数据）
- bin 文件的 calendars/day.txt 包含所有交易日
- dump_bin.py 的 `data_merge_calendar()` 方法根据 calendars/day.txt 对齐数据
- 如果股票在某交易日没有数据，填充 NaN

### 为什么某些股票在某些日期没有数据？

**原因**：
- 股票停牌
- 股票未上市
- 数据缺失
- 其他原因

## 结论

### 数据缺失的根本原因

**daily_pv.h5 的 calendars/day.txt 只包含有数据的日期**，而 bin 文件的 calendars/day.txt 包含所有交易日。

**具体原因**：
1. daily_pv.h5 导出时，calendars/day.txt 是从 DataFrame 的 datetime 索引生成的
2. 如果某只股票在某交易日没有数据，该日期就不会出现在 DataFrame 中
3. 该日期也不会出现在 calendars/day.txt 中
4. bin 文件导出时，CSV 文件包含所有有数据的日期
5. dump_bin.py 的 `data_merge_calendar()` 方法根据 calendars/day.txt 对齐数据
6. 如果股票在某交易日没有数据，填充 NaN

### 解决方案

1. **统一 calendars/day.txt 的生成逻辑**
   - 建议在导出 daily_pv.h5 时，也从数据库的交易日历表（market.trading_calendar）读取所有交易日
   - 确保 calendars/day.txt 包含所有交易日

2. **数据填充**
   - 如果股票在某交易日没有数据，填充 NaN
   - 确保日期完整性

3. **文档说明**
   - 在文档中说明两种数据格式的差异
   - 说明日期缺失的原因和处理方式
