# 本地数据管理与数据缺口检查改动记录

## 一、后端统一到 8001 的数据缺口检查接口

### 1. 目标

- 新程序只依赖 8001 端口，不再依赖旧的 9000 端口和 `tdx_backend.py`。
- 将原本在 `tdx_backend.py` 中实现的 `/api/data-stats/gaps` 能力，在新后端中完整实现一份。

### 2. 文件

- `next_app/backend/routers/ingestion.py`

### 3. 新增接口：GET `/api/data-stats/gaps`

在现有的 `list_data_stats` 之后，`Trading calendar helper` 注释之前，新增：

```python
@router.get("/data-stats/gaps")
async def get_data_gaps(
    data_kind: str = Query(..., description="数据集标识，对应 market.data_stats_config.data_kind"),
    start_date: Optional[str] = Query(
        default=None,
        description="可选覆盖起始日期(YYYY-MM-DD)，默认使用 data_stats.min_date",
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="可选覆盖结束日期(YYYY-MM-DD)，默认使用 data_stats.max_date",
    ),
) -> Dict[str, Any]:
    """
    计算指定 data_kind 在本地交易日历上的缺失日期段，并压缩为连续区间返回。
    完全基于新程序的连接池 / 数据表，不依赖 tdx_backend 或 9000 端口。
    """

    # 1) 从 data_stats_config 读取表名和日期列
    cfg_rows = _fetchall(
        """
        SELECT data_kind, table_name, date_column
          FROM market.data_stats_config
         WHERE data_kind = %s AND enabled
        """,
        (data_kind,),
    )
    if not cfg_rows:
        raise HTTPException(status_code=404, detail="unknown or disabled data_kind")
    cfg = cfg_rows[0]
    table_name = str(cfg.get("table_name") or "").strip()
    date_column = str(cfg.get("date_column") or "").strip()
    if not table_name or not date_column:
        raise HTTPException(status_code=400, detail="invalid data_stats_config for this data_kind")

    # 2) 确定检查区间：显式 start/end 优先，否则使用 data_stats 的 min/max
    start: Optional[dt.date]
    end: Optional[dt.date]
    if start_date and end_date:
        try:
            start = dt.date.fromisoformat(start_date)
            end = dt.date.fromisoformat(end_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid start_date or end_date format")
    elif start_date or end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date must be both provided or omitted")
    else:
        stats_rows = _fetchall(
            """
            SELECT min_date, max_date
              FROM market.data_stats
             WHERE data_kind = %s
            """,
            (data_kind,),
        )
        if not stats_rows:
            raise HTTPException(
                status_code=400,
                detail="no data_stats entry for this data_kind; run /api/data-stats/refresh first",
            )
        row = stats_rows[0]
        start = row.get("min_date")
        end = row.get("max_date")
    if start is None or end is None:
        raise HTTPException(status_code=400, detail="min_date/max_date is NULL for this data_kind; cannot check gaps")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date is after end_date")

    # 3) 读取交易日历上的所有交易日
    cal_rows = _fetchall(
        """
        SELECT cal_date
          FROM market.trading_calendar
         WHERE is_trading = TRUE
           AND cal_date BETWEEN %s AND %s
         ORDER BY cal_date
        """,
        (start, end),
    )
    if not cal_rows:
        raise HTTPException(
            status_code=400,
            detail="no trading_calendar rows in range; please sync calendar via /api/calendar/sync first",
        )
    trading_days: List[dt.date] = [r["cal_date"] for r in cal_rows]

    # 4) 统计业务表中实际出现过数据的交易日期集合
    sql = f"""
        SELECT DISTINCT {date_column}::date AS d
          FROM {table_name}
         WHERE {date_column} >= %s AND {date_column} <= %s
         ORDER BY d
    """
    data_rows = _fetchall(sql, (start, end))
    data_days = {r["d"] for r in data_rows}

    # 5) 求差集并压缩为连续缺失区间
    missing_days: List[dt.date] = [d for d in trading_days if d not in data_days]
    missing_ranges: List[Dict[str, Any]] = []
    cur_start: Optional[dt.date] = None
    cur_end: Optional[dt.date] = None
    for d in missing_days:
        if cur_start is None:
            cur_start = d
            cur_end = d
        elif (d - cur_end).days == 1:
            cur_end = d
        else:
            days_span = (cur_end - cur_start).days + 1
            missing_ranges.append(
                {"start": cur_start.isoformat(), "end": cur_end.isoformat(), "days": days_span}
            )
            cur_start = d
            cur_end = d
    if cur_start is not None and cur_end is not None:
        days_span = (cur_end - cur_start).days + 1
        missing_ranges.append(
            {"start": cur_start.isoformat(), "end": cur_end.isoformat(), "days": days_span}
        )

    total_trading = len(trading_days)
    total_missing = len(missing_days)
    return {
        "data_kind": data_kind,
        "table_name": table_name,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "total_trading_days": total_trading,
        "covered_days": total_trading - total_missing,
        "missing_days": total_missing,
        "missing_ranges": missing_ranges,
    }
```

**效果：**

- 前端 `local-data` 页面通过 `NEXT_PUBLIC_TDX_BACKEND_BASE` 连接 8001。
- `/api/data-stats/gaps` 在 8001 直接提供，不再依赖 9000 端口的旧程序。

---

## 二、前端数据看板“补齐到最新交易日”逻辑改动

### 1. 目标

- 在“数据看板”点击「补齐到最新交易日」时：
  - 自动跳到“增量”标签页；
  - 自动填好：
    - 覆盖起始日期 = 数据看板里该数据集的 `min_date`；
    - 目标日期 = 最新交易日；
  - 一键提交增量任务即可补齐 `min_date ~ 最新交易日` 区间内的数据。

### 2. 文件

- `next_app/frontend/src/app/local-data/page.tsx`

### 3. 具体改动

#### 3.1 修改 LocalDataPage 的 `handleFillLatestFromStats`

原实现：

```ts
const handleFillLatestFromStats = useCallback(
  (kind: string, latestTradingDay: string) => {
    const lower = (kind || "").toLowerCase();
    let dataSource: DataSource = "TDX";
    let dataset: string | undefined;
    if (lower === "kline_daily_qfq") {
      dataset = "kline_daily_qfq";
    } else if (lower === "kline_daily_raw") {
      dataset = "kline_daily_raw";
    } else if (lower === "kline_minute_raw") {
      dataset = "kline_minute_raw";
    } else if (
      lower === "tdx_board_index" ||
      lower === "tdx_board_member" ||
      lower === "tdx_board_daily"
    ) {
      dataSource = "Tushare";
      dataset = "tdx_board_all";
    } else {
      return;
    }
    setIncrementalPrefill({
      dataSource,
      dataset,
      targetDate: latestTradingDay,
      startDate: null,
    });
    setActiveTab("incremental");
  },
  [],
);
```

修改为：

```ts
const handleFillLatestFromStats = useCallback(
  (kind: string, latestTradingDay: string, minDate?: string | null) => {
    const lower = (kind || "").toLowerCase();
    let dataSource: DataSource = "TDX";
    let dataset: string | undefined;
    if (lower === "kline_daily_qfq") {
      dataset = "kline_daily_qfq";
    } else if (lower === "kline_daily_raw") {
      dataset = "kline_daily_raw";
    } else if (lower === "kline_minute_raw") {
      dataset = "kline_minute_raw";
    } else if (
      lower === "tdx_board_index" ||
      lower === "tdx_board_member" ||
      lower === "tdx_board_daily"
    ) {
      dataSource = "Tushare";
      dataset = "tdx_board_all";
    } else {
      return;
    }
    setIncrementalPrefill({
      dataSource,
      dataset,
      targetDate: latestTradingDay,
      // 起始日期：使用数据看板里的 min_date
      startDate: minDate || null,
    });
    setActiveTab("incremental");
  },
  [],
);
```

#### 3.2 修改 `DataStatsTab` 的 `onFillLatest` 类型签名

原定义：

```ts
function DataStatsTab({
  onFillLatest,
}: {
  onFillLatest?: (kind: string, latestTradingDay: string) => void;
}) {
```

修改为：

```ts
function DataStatsTab({
  onFillLatest,
}: {
  onFillLatest?: (
    kind: string,
    latestTradingDay: string,
    minDate?: string | null,
  ) => void;
}) {
```

#### 3.3 修改 `handleFillLatestClick`，传入 `minDate`

原实现：

```ts
const handleFillLatestClick = useCallback(
  async (kind: string) => {
    if (!onFillLatest) return;
    try {
      const data: any = await backendRequest(
        "GET",
        "/api/trading/latest-day",
      );
      const latest = data?.latest_trading_day;
      if (!latest) {
        setError("无法获取最新交易日，请先同步交易日历。");
        return;
      }
      onFillLatest(kind, String(latest));
    } catch (e: any) {
      setError(e?.message || "获取最新交易日失败");
    }
  },
  [onFillLatest],
);
```

修改为：

```ts
const handleFillLatestClick = useCallback(
  async (kind: string, minDate?: string | null) => {
    if (!onFillLatest) return;
    try {
      const data: any = await backendRequest(
        "GET",
        "/api/trading/latest-day",
      );
      const latest = data?.latest_trading_day;
      if (!latest) {
        setError("无法获取最新交易日，请先同步交易日历。");
        return;
      }
      onFillLatest(kind, String(latest), minDate ?? null);
    } catch (e: any) {
      setError(e?.message || "获取最新交易日失败");
    }
  },
  [onFillLatest],
);
```

#### 3.4 在数据行中传入 `min_date`

在 `items.map((it) => { ... })` 中，为每一行增加：

```ts
const kind = String(
  it.data_kind || it.kind || "",
);
const minDateStr =
  it.min_date || it.date_min || it.start_date || null;
const canFillLatest = [
  "kline_daily_qfq",
  "kline_daily_raw",
  "kline_minute_raw",
  "tdx_board_index",
  "tdx_board_member",
  "tdx_board_daily",
].includes(kind);
```

然后将“补齐到最新交易日”按钮的 `onClick` 从：

```tsx
onClick={() => handleFillLatestClick(kind)}
```

修改为：

```tsx
onClick={() => handleFillLatestClick(kind, minDateStr)}
```

---

## 三、后续隔离方案建议：新程序独立目录与 Conda 环境

### 1. 项目结构建议

- 旧程序（保留原有结构）

  ```text
  aiagents-stock/
    tdx_backend.py
    ...
  ```

- 新程序（完全独立）

  ```text
  aiagents-stock-next/   # 或其他新名字
    next_app/
      backend/
      frontend/
    requirements.txt / pyproject.toml
    .env / .env.local
  ```

### 2. 独立 Conda 环境

```bash
conda create -n aiagents-next python=3.11
conda activate aiagents-next
pip install -r requirements.txt
```

- 仅安装新程序所需依赖，不引入旧 9000 端口服务的历史依赖。

### 3. 运行约定

- 新程序：

  ```bash
  # 后端
  uvicorn next_app.backend.main:app --host 0.0.0.0 --port 8001

  # 前端
  NEXT_PUBLIC_TDX_BACKEND_BASE=http://127.0.0.1:8001
  npm run dev
  ```

- 文档中明确：新 UI / 调度控制台 **只连 8001**，不再直接调用 9000 端口。
- 如需保留旧工具，单独说明如何在原仓库 + 原环境中启动 9000，仅在需要旧 UI/脚本时手动启用。
