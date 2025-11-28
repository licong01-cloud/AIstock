# TDX 行情与成交入库优化设计文档

## 1. 背景与目标

本设计文档旨在统一和优化本地 TimescaleDB 中以下数据集的入库策略：

- 日 K：`market.kline_daily_qfq`（前复权）、`market.kline_daily_raw`（未复权）
- 分钟 K：`market.kline_minute_raw`（1 分钟线）
- 高频聚合：`app.ts_lstm_trade_agg`（5 分钟成交聚合）

主要目标：

- 充分利用 TDX API 的 **按时间区间 / 全量** 批量接口，减少 HTTP 次数和数据冗余；
- 统一使用 **数据库批量写入（execute_values）**，避免逐行 INSERT；
- 为大数据集引入 **受控并行度（workers=1/2/4/8）**，提升端到端吞吐；
- 明确 **进度条语义**，对有股票代码维度的任务按“每只股票完成”更新进度；
- 支持分钟线在策略升级时 **TRUNCATE + 重新初始化** 的高效重建方式。

不变约束：

- 结果必须保持确定性，不牺牲数据正确性；
- 兼容现有调度与前端监控能力；
- 控制表（jobs/runs/tasks/state/errors）语义保持不变。

---

## 2. TDX API 使用策略

### 2.1 K 线相关接口

- **`GET /api/kline-history`**
  - 按时间范围获取 K 线：
    - 参数：`code`, `type`, `start_date`, `end_date`, `limit (<=800)`
  - 适用场景：
    - 日 K / 分钟 K **增量或中短期补数**（例如最近几个月~5 年）。
  - 优点：
    - 仅返回所需时间段，避免全历史传输；
    - 对于 5 年日 K（约 1250 条），最多 2～3 段即可拿全。

- **`GET /api/kline-all`**
  - 返回指定股票在某个周期的全部历史 K 线（天/周/月自动前复权）。
  - 与 `/api/kline-all/ths`（同花顺前复权日/周/月）、`/api/kline-all/tdx`（通达信原始 K 线）构成“全量系列”。
  - 适用场景：
    - **全量初始化** 或 **跨年大范围补数**（例如上市以来分钟线）。
  - 典型用法：
    - `/api/kline-all/tdx?code=600519&type=day&limit=366` → 最近一年的未复权日 K；
    - `/api/kline-all/tdx?code=600519&type=minute1&limit=240*250` → 约 1 年的一分钟 K。

**选型原则：**

- **增量 / 中短区间（含补 5 年日 K）**：优先使用 `GET /api/kline-history`；必要时按时间拆段，多次调用。
- **全量 / 初始化 / 超长区间**：优先使用 `GET /api/kline-all[...]`，客户端根据日期做本地过滤。

### 2.2 成交明细相关接口

- `GET /api/trade-history`：单日、分页，需 `start`/`count` 循环。
- `GET /api/minute-trade-all`：单日、一次性返回全天全部分时成交明细。
- `GET /api/trade-history/full`：上市以来全部成交明细，可配合 `before`/`limit` 截断。

**选型原则（针对 trade_agg_5m）：**

- 主路径继续使用 `GET /api/minute-trade-all`：
  - 服务端等价于帮我们封装了多次 `/api/trade-history` 分页；
  - 客户端每个交易日只需 1 次请求。
- `/api/trade-history` / `/api/trade-history/full` 保留为特殊分析用途，不作为全市场聚合 ingestion 的默认路径。

---

## 3. 各数据集策略

### 3.1 日 K：`kline_daily_qfq` / `kline_daily_raw`

#### 3.1.1 初始化（init 模式）

- `kline_daily_qfq`：
  - 使用 `GET /api/kline-all` 或 `GET /api/kline-all/ths?code=XXX&type=day`。
- `kline_daily_raw`：
  - 使用 `GET /api/kline-all/tdx?code=XXX&type=day`（现有脚本已如此）。

**本地处理：**

- 每只股票：
  1. 调用 `/api/kline-all[...]` 获得时间正序列表；
  2. 利用 `_to_date(Time/Date)` 转为 `YYYY-MM-DD`；
  3. 按 `[start_date, end_date]` 本地过滤；
  4. 结果排序后一次性传入 `upsert_kline_daily` / `upsert_kline_daily_raw`；
  5. 以 `execute_values` 一批插入/更新（详见第 4 节）。

#### 3.1.2 增量（incremental 模式，含补 5 年日 K）

- 统一使用 `GET /api/kline-history`：
  - `GET /api/kline-history?code=XXX&type=day&start_date=YYYYMMDD&end_date=YYYYMMDD`
- 时间区间：
  - `start_date`：
    - 若用户提供 `--start-date`，以此为准；
    - 否则从 `ingestion_state.last_success_date+1` 开始。
  - `end_date`：
    - 为目标交易日（当日或指定日期）。
- 若预估条数 > 800：
  - 按年度或按 800 条窗口拆为多个子区间，多次 `/kline-history` 调用；
  - 合并并去重（基于 `trade_date`）后再统一 upsert。

> 补 5 年日 K（≈1250 条/股）属于“中等区间增量”，仍按上述 `/api/kline-history` 策略处理，而不是 `/api/kline-all`。

---

### 3.2 分钟 K：`kline_minute_raw`（1 分钟线）

#### 3.2.1 初始化（init：从零或大范围重建）

- HTTP 选型：
  - 首选：`GET /api/kline-all/tdx?code=XXX&type=minute1`；
  - 如需限制：`limit ≈ 240 * 250 * 年数`。

- DB 策略：
  - 若不需要与旧数据合并（推荐新策略）：
    1. 运行前执行 `TRUNCATE TABLE market.kline_minute_raw`；
    2. 仅使用 **纯 `INSERT ... VALUES %s`**（无 `ON CONFLICT`）批量写入；
    3. 在会话内可设置 `synchronous_commit=off`, `work_mem='256MB'` 等批量调优。

- 本地处理流程：

  1. 调用 `/api/kline-all/tdx?type=minute1` 拿到该股票全部分钟 K；
  2. 根据时间戳推导 `trade_date`，按日分组为 `{date -> [bars...]}`；
  3. 对每个 `trade_date` 调用现有的 `upsert_minute` 改造版：
     - init 场景下改为 `INSERT` 无冲突；
     - 若需要保留旧数据的补数场景，则保留 `ON CONFLICT`.

#### 3.2.2 增量（incremental：小区间补数）

- 对于仅补最近几个月~1 年的分钟 K，可选：
  - `GET /api/kline-history?code=XXX&type=minute1&start_date=YYYYMMDD&end_date=YYYYMMDD`；
  - 若超过 800 条，按日期或时间窗口拆分；
  - 逻辑同日 K 增量：合并多段后本地按日分组批量 upsert.

- 对于需要补多年分钟线（例如 5 年以上）：
  - 实际上已接近全量重建，性能和实现上更合理的做法是走 **init 路径**：
    - TRUNCATE + `/api/kline-all/tdx` + 纯批量 INSERT.

---

### 3.3 高频聚合：`trade_agg_5m`

- HTTP 端：
  - 继续使用 `GET /api/minute-trade-all?code=XXX&date=YYYYMMDD`；
  - 每只股票、每个交易日一次调用，获取当天全部成交明细.

- 聚合逻辑（已存在，保持）：
  - `_aggregate_trades(trades, ts_code, freq_minutes)`：
    - 按 `bucket_start_time` 聚合为 5 分钟桶；
    - 计算买卖量、OFI、实现波动率、平均单笔等特征；
    - 返回一批待写入行 `values`.

- DB 端：
  - 批量写入 `app.ts_lstm_trade_agg`：
    - 使用 `pgx.execute_values(cur, sql, values, page_size=2000)`；
    - 通过 `ON CONFLICT(symbol, bucket_start_time, freq)` upsert.

---

## 4. 数据库批量写入策略

### 4.1 已有批量写入模式

- 日 K（QFQ/RAW）：
  - `upsert_kline_daily` / `upsert_kline_daily_raw` / `upsert_daily` / `upsert_daily_raw`：
    - 所有行聚合到 `values: List[Tuple[...]]`；
    - 单次 `pgx.execute_values(cur, sql, values)`.

- 分钟 K：
  - `upsert_minute`：
    - 以“**一只股票的一天全部分钟线**”为一个批次.

- 高频聚合：
  - `ingest_trade_agg.py`：
    - 一只股票一个交易日的所有聚合桶统一写入 `execute_values`；
    - 设置 `page_size=2000` 控制单批大小.

- 控制表：
  - `ingestion_jobs` / `ingestion_runs` / `ingestion_state` / `ingestion_checkpoints` / `ingestion_errors`：
    - 行数很少，继续保留逐行 `INSERT/UPDATE`，不做批量优化.

### 4.2 新策略要求

- 对分钟线初始化：
  - 可选 pure INSERT 模式：不使用 `ON CONFLICT`，完全由 TRUNCATE 保证唯一性.
- 其它场景：
  - 保持 `ON CONFLICT`，支持补数与重跑；
  - 始终通过 `execute_values` 批量执行.

---

## 5. 并行度（workers）设计

### 5.1 CLI 参数

在以下脚本中统一引入 `--workers` 参数：

- `scripts/ingest_full_daily.py`
- `scripts/ingest_full_daily_raw.py`
- `scripts/ingest_full_minute.py`
- `scripts/ingest_incremental.py`（影响日 K / 分钟 K 增量）
- （可选）`scripts/rebuild_adjusted_daily.py` 已有 `--workers`，作为参考实现；
- （可选）`scripts/ingest_trade_agg.py` 可后续增加.

参数规范：

```text
--workers INT, default=1, choices=[1,2,4,8]
1 表示串行，多于 1 表示在股票维度并行.
```

### 5.2 实现方式

- `workers=1`：
  - 保持现有行为，逐只股票串行处理；
- `workers>1`：
  - 使用 `ThreadPoolExecutor(max_workers=workers)`；
  - 每个 worker 负责一部分股票：
    - 在 worker 内部新建 DB 连接（autocommit）；
    - 使用相同的 HTTP 请求函数和批量 upsert 函数；
  - 任务维度为 `ts_code`（股票维度），保证单只股票的数据在一个线程内顺序处理，保持确定性.

### 5.3 调度器与前端

- **后端调度器 `backend/ingestion/tdx_scheduler.py`**：
  - `TDXScheduler._default_ingestion_args` 中：
    - 若 `options.get("workers")` 存在，将其透传为 CLI 参数 `--workers N`；
  - 数据集覆盖：日 K init/incremental、分钟 K init、trade_agg_5m（若支持）。

- **前端 `frontend/src/app/local-data/page.tsx`**：
  - 在以下数据集的 init/incremental 表单中增加并行度下拉框：
    - TDX：`kline_daily_qfq`, `kline_daily_raw`, `kline_minute_raw`, `trade_agg_5m`；
  - 选项：`1, 2, 4, 8`，默认 `1`；
  - 提交时在 payload 的 `options` 中增加 `workers: number` 字段，供调度器透传.

---

## 6. 进度条与统计策略

### 6.1 任务分类

- **有股票代码维度（symbol-based）的任务：**
  - 日 K：`kline_daily_qfq`, `kline_daily_raw`；
  - 分钟 K：`kline_minute_raw`；
  - 高频聚合：`trade_agg_5m`；
  - 板块行情：`tdx_board_*`（若按 ts_code 迭代）。

- **无股票代码维度（non-symbol）任务：**
  - 交易日历同步：`calendar_sync`；
  - 某些全局任务，如只依据日期维度或配置项执行的脚本.

### 6.2 symbol-based 任务的进度计算

**目标：** 进度条在 UI 上反映“对多少只股票处理完”的比例.

实现要点：

1. 脚本启动时：
   - 统计 `total_codes = len(codes)`；
   - 初始化 stats：`success_codes = 0`, `failed_codes = 0`, `inserted_rows = 0`；
   - 调用 `update_job_summary(conn, job_id, {"total_codes": total_codes, ...})` 记录到 `ingestion_jobs.summary`.

2. 每处理完一只股票（无论成败）时：
   - 成功：
     - `success_codes += 1`；
     - `inserted_rows += inserted_for_this_symbol`；
     - `update_job_summary(conn, job_id, {"inserted_rows": inserted, "success_codes": 1})`；
   - 失败：
     - `failed_codes += 1`；
     - `update_job_summary(conn, job_id, {"failed_codes": 1})`；
   - 同时调用 `complete_task(...)` 更新 `ingestion_job_tasks.progress` 为 100%.

3. 后端 `/api/ingestion/job/{id}` 在返回状态时：
   - 根据 `summary` 中的 counters 计算：

    ```python
    done = success_codes + failed_codes
    total = max(total_codes, 1)
    progress = round(100 * done / total, 2)
    ```

   - 将 `progress` 返回给前端，前端进度条绑定该字段.

这样，对于 symbol-based 任务，进度条约等于“**已经完成的股票占总股票数的比例**”.

### 6.3 non-symbol 任务的进度

对如下任务，不以股票数作为进度单位：

- 交易日历同步：
  - 以处理的日期总数为依据，或简单采用“开始=0，结束=100”的 two-state 进度；
- 纯配置/统计任务：
  - 同样采用 coarse-grained 进度即可。

前端可以通过 `dataset` / `data_kind` 判断当前是否 symbol-based：

- 若是，则显示“总股票数/已完成数”等细粒度信息；
- 否则，只展示任务状态与粗粒度进度。

---

## 7. 实施步骤计划

### 7.1 设计文档（当前步骤）

- 在 `doc/tdx_ingestion_design.md` 编写本设计（已完成）。

### 7.2 代码实现步骤

1. **更新增量脚本 `scripts/ingest_incremental.py`**
   - 调整 `fetch_daily` / `fetch_daily_raw` 使用 `/api/kline-history`，并支持长区间分段；
   - 调整分钟增量逻辑（如适用），引入基于 `/api/kline-history` 或 `/api/kline-all/tdx` 的按区间拉取；
   - 引入 `--workers` 参数，在股票维度实现可选的多线程并发；
   - 保持 `upsert_*` 批量逻辑不变。

2. **更新全量日 K 脚本**
   - `scripts/ingest_full_daily.py`：
     - 如有需要，切换到 `/api/kline-all` 或 `/api/kline-all/ths`；
   - `scripts/ingest_full_daily_raw.py`：
     - 已使用 `/api/kline-all/tdx`，仅对 workers 和进度更新一并对齐。

3. **更新分钟线初始化脚本 `scripts/ingest_full_minute.py`**
   - 支持使用 `/api/kline-all/tdx?type=minute1` 做单股全量获取；
   - 若指定 `--truncate`，则：
     - 先 TRUNCATE `market.kline_minute_raw`；
     - init 模式下采用纯 INSERT 批量写，不用 `ON CONFLICT`；
   - 可选引入 `--workers` 控制股票维度并发；
   - 调整进度统计为“每完成一只股票，成功/失败计数+1”。

4. **（可选）对 `scripts/ingest_trade_agg.py` 增加 workers 参数**
   - 复用 `ThreadPoolExecutor` 模式；
   - 每个 worker 独享 DB 连接和 HTTP 调用。

5. **调度器 `backend/ingestion/tdx_scheduler.py` 更新**
   - 在 `_default_ingestion_args` 中：
     - 对日 K init/incremental、分钟 init、trade_agg_5m，检查 `options["workers"]` 并透传到 CLI。

6. **前端 `frontend/src/app/local-data/page.tsx` 更新**
   - 在 Init 与 Incremental Tab 中：
     - 针对 TDX 的日 K / 分钟 K / `trade_agg_5m` 增加“并行度”下拉框（1/2/4/8，默认 1）；
     - 提交请求时在 `options` 中附带 `workers` 字段。

7. **进度计算与展示校验**
   - 确认所有 symbol-based 脚本在每处理完一只 ts_code 后调用 `update_job_summary` 累加 `success_codes` / `failed_codes`，并将 `inserted_rows` 增量写入；
   - 在 `/api/ingestion/job/{id}` 的实现中，确保 progress = done / total；
   - 前端 Jobs Tab 仍使用 `jobStatus.progress` 驱动进度条，无需额外改动逻辑，仅解释语义为“按股票数进度”。

### 7.3 联合测试计划

1. **单脚本本地测试（命令行）**
   - 选择少量股票 + 较短日期区间，分别对：
     - 日 K init / incremental；
     - 分钟 K init / incremental；
     - trade_agg_5m init / incremental；
   - 使用不同 workers（1/2/4/8）运行，观察：
     - 运行是否成功；
     - DB 中行数与时间范围是否匹配；
     - `ingestion_jobs.summary` 中统计字段递增是否正确。

2. **通过后端 API + 前端 UI 测试**
   - 从 Local Data 页面提交各类任务：
     - 验证并行度下拉框是否生效；
     - Jobs 面板进度条是否按“每只股票完成”推进；
     - Data Stats Tab 中 `symbol_count` 与 trading days 覆盖是否合理。

3. **性能观察**
   - 对比：
     - 旧版按日调用 `/api/minute` 的分钟 init vs 新版 `/api/kline-all/tdx` + TRUNCATE 的 init；
     - 旧版日 K 增量 vs `/api/kline-history` 分段方案；
   - 记录总耗时、HTTP 请求次数、DB 写入速率等指标。

---

本设计文档将作为后续所有改动的蓝本：先按上述步骤逐项实现，再进行统一回归与性能测试。
