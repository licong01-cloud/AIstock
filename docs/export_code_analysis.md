# daily_pv.h5 和 bin 文件导出代码分析

## 导出流程对比

### daily_pv.h5 导出流程

**文件**: `backend/qlib_exporter/snapshot_writer.py`

```python
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

    # ... 生成 instruments/all.txt 和 calendars/day.txt
```

**关键点**:
- 直接写入 DataFrame，**没有日期过滤**
- calendars/day.txt 是从 DataFrame 的 datetime 索引生成的
- 包含所有数据，不管股票是否在该日期有数据

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
    # 从 DB 读取数据
    df = _db_reader.load_qlib_daily_data_all(
        start=start,
        end=end,
        exchanges=exchanges,
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
- 从 DB 导出 CSV 时，**只导出有数据的股票和日期**
- dump_bin.py 使用 `data_merge_calendar` 方法，**根据 calendars/day.txt 对齐数据**
- 只保留日历中存在的日期
- 如果股票在某日期没有数据，会填充 NaN

## 日期缺失的根本原因

### daily_pv.h5 的日期缺失原因

daily_pv.h5 的日期缺失**不是由导出程序导致的**，而是由**数据源本身决定的**：

1. **数据源限制**: daily_pv.h5 的数据来自 DBReader.load_qlib_daily_data_all，该方法从数据库读取数据
2. **数据过滤**: DBReader 可以根据以下条件过滤数据：
   - `exclude_st`: 排除 ST 股票
   - `exclude_delisted_or_paused`: 排除退市或停牌股票
   - 交易所过滤
3. **数据完整性**: 如果某只股票在某交易日没有数据（可能原因：停牌、未上市、数据缺失），该股票在该日期就不会出现在 daily_pv.h5 中

### bin 文件的日期完整性

bin 文件的日期完整性**更好**，原因如下：

1. **日历对齐**: dump_bin.py 使用 `data_merge_calendar` 方法，根据 calendars/day.txt 对齐数据
2. **数据填充**: 如果股票在某交易日没有数据，会填充 NaN，但该日期仍然存在
3. **日历文件**: calendars/day.txt 包含所有交易日，确保日期完整性

## 数据质量分析

### 采样日期分析

采样了 5 个日期（2010-03-01, 2010-07-01, 2011-05-09, 2012-01-10, 2014-07-15）和 4 只股票（000001.SZ, 000002.SZ, 600000.SH, 600036.SH）：

**关键发现**:
- **成交量为0**: 0 次
- **成交额为0**: 0 次
- **价格为0**: 0 次
- **NaN值**: 部分股票在某些日期为 NaN（如 000001.SZ 在 2010-03-01）

**结论**:
- bin 文件数据质量正常，无成交量为0或价格为0的情况
- NaN 值表示该股票在该日期没有数据（可能原因：停牌、未上市、数据缺失）
- 数据完整性良好

## 总结

### daily_pv.h5 和 bin 文件的差异

| 特性 | daily_pv.h5 | bin 文件 |
|------|-------------|---------|
| 导出方式 | 直接写入 DataFrame | CSV + dump_bin.py |
| 日期对齐 | 无 | 有（根据 calendars/day.txt） |
| 数据填充 | 无数据则不包含该日期 | 填充 NaN |
| 日期完整性 | 依赖数据源 | 完整（基于日历） |
| 数据质量 | 正常 | 正常 |

### 日期缺失的原因

1. **daily_pv.h5 日期缺失**: 数据源限制，某些股票在某些交易日没有数据
2. **bin 文件日期完整**: 使用日历对齐，确保日期完整性

### 建议

1. **统一导出逻辑**: 建议在导出 daily_pv.h5 时也使用日历对齐，确保日期完整性
2. **数据质量检查**: 定期检查数据质量，确保数据完整性
3. **文档说明**: 在文档中说明两种数据格式的差异和使用场景
