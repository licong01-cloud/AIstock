# 官方离线因子全量计算 WSL 内存缓存设计方案

日期：2026-06-15  
状态：设计交付稿，后续代码实现必须逐条通过本文验收门禁后才允许声明完成或合入。  
适用范围：因子库官方独立指标计算、官方因子值缓存生成、因子相关性计算、QE 回测因子缓存复用。

## 1. 背景与结论

当前因子库已经把官方独立指标计算和相关性计算的入口提交到 WSL / compute-node dispatch，但官方全量因子值缓存生成仍依赖历史手工脚本和逐因子文件读取模式，没有形成“WSL 中一次加载回测底层数据、分批生成因子值、同批计算独立指标、释放批内内存”的官方产品化链路。

本方案确认：官方离线因子计算必须先形成详细设计，再进入 BUG / Issue 开发流程。后续实现不得自行降级为手工 backfill、不得在 Windows 执行重计算、不得回退到 `factor_values_realtime` / DB realtime snapshot、不得使用改造后的实时因子代码替代原始回测因子代码。

## 2. 设计依据与允许 API

### 2.1 已读取的规范和既有设计

- `docs/codex_project_memory.md`：确认根仓库是 runtime/sync baseline，非琐碎工作使用独立 worktree，生产服务重启和 DB/DDL 需要显式授权。
- `docs/standards/aistock_development_standard_v1.5_20260523.md`：确认设计方案必须落在 `docs/architecture/`，必须包含背景、范围、现状差距、目标架构、失败模式、测试方案、验收标准，并在实现合入前执行 DESIGN-COMPLIANCE-001 矩阵复核。
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`：确认后续实现必须走 BUG / GitHub Issue、声明 `allowed_write_scope`、按 closure requirements 和验证证据关闭。
- `docs/architecture/factor_independent_metrics_single_writer_design_20260427.md`：确认独立指标写入源是官方指标服务，不允许脚本、分类、评级形成第二写源。
- `docs/architecture/factor_st_pit_official_metrics_cache_design_20260506.md` 与 `docs/architecture/factor_cache_st_pit_universe_enforcement_20260513.md`：确认官方因子缓存必须携带 ST PIT universe metadata、index policy、coverage semantics。
- `docs/architecture/factor_correlation_compute_independence_design_20260501.md`：确认相关性计算应独立于 QE router，并由 WSL runner 调用 service。

### 2.2 当前代码中允许复用的 API / 组件

- `DispatchService.create_and_submit_task(...)`：用于 Windows 控制面提交 WSL / compute-node 任务，后续新增 `official_factor_full_compute` 也应复用该调度模型。
- `backend/scripts/run_official_evaluation_wsl.py`、`backend/scripts/run_correlation_compute_wsl.py`：作为 WSL custom task runner 模式参考，但必须补齐 WSL-only guard。
- `FactorOfficialEvaluationService._save_metrics()` 与 `_save_monthly_ic()`：后续仍作为官方指标写入语义的来源，不改变指标公式和表职责。
- `qe_eval_v2_metric_engine.prepare_shared_context()` 与 `compute_single_factor_metrics()`：后续批处理指标计算应复用这些函数或等价内部逻辑，避免重写指标算法。
- `FactorUniverseMaskService`：继续作为 ST PIT eligible index、mask、metadata 的权威来源。
- `FactorValueLoader(source="single", pipeline_dir=...)` 与 `CorrelationEngine.compute_full_matrix(...)`：相关性继续复用 single cache + Spearman/EWMA 算法。
- `ConfigComposer` 生成回测因子代码时使用的 `code_text` 权威语义：官方离线因子源码应与 QE 回测一致，不能改用 realtime transformation 产物。

### 2.3 禁止复用或必须降级的路径

- `DataSnapshotManager` / `RealtimeFactorDataLoader` / `FactorValuePipeline` 的 realtime snapshot 链路不得作为官方离线主链路。
- `rdagent_assets/factor_values_realtime` 不得作为官方独立指标、相关性或 QE 回测缓存来源。
- `scripts/backfill_factor_cache.py` 不得继续作为人工主流程；后续只能作为历史迁移/诊断工具，并必须加 WSL-only 防护。
- 官方离线准入不得要求 `qe_code_path IS NOT NULL` 或 `transformation_status='SUCCESS'`。

## 3. 已确认现状

### 3.1 已符合的部分

- 官方独立指标 API 入口调用 `FactorOfficialEvaluationService.compute()`，当前会提交 `official_evaluation` dispatch 任务。
- WSL runner `backend/scripts/run_official_evaluation_wsl.py` 会在节点侧调用 `_compute_local()`。
- 相关性入口 `_run_correlation_compute_via_dispatch()` 会提交 `correlation_compute` dispatch 任务。
- WSL runner `backend/scripts/run_correlation_compute_wsl.py` 会在节点侧调用 `correlation_compute_service.run_correlation_compute_local()`。
- 相关性当前已经把缓存根目录固定到 `rdagent_assets/factor_values`，不应再使用 `factor_values_realtime`。

### 3.2 当前偏差

- `_compute_local()`、相关性 local runner、`backfill_factor_cache.py` 缺少 WSL-only fail-fast 防护；函数仍可能被 Windows 直接调用。
- `scripts/backfill_factor_cache.py` 是手工脚本，不是 UI / API 产品化官方任务。
- `backfill_factor_cache.py` 当前按因子 subprocess 执行，每个因子自己读取 h5/parquet/bin 文件；主进程不持有底层数据内存缓存，无法避免重复文件读取。
- `FactorOfficialEvaluationService._compute_local()` 当前只逐因子读取 `rdagent_assets/factor_values/single/*.parquet` 并逐因子计算指标，不负责生成因子值，也不是批量 panel 指标计算。
- 旧 `FactorValuePipeline` 接近“基础数据一次加载后共享”，但其数据源是 `DataSnapshotManager` / `RealtimeFactorDataLoader` / `factor_values_realtime`，不符合官方离线回测数据源要求。
- 旧准入逻辑仍存在依赖 `transformation_status`、`qe_code_path` 的风险；官方离线计算应以 catalog 原始 `code_text` 为权威源码。

## 4. 设计目标

1. 官方全量因子值生成、独立指标计算、相关性计算都必须在 WSL / compute-node 环境执行，Windows FastAPI 只做任务提交、状态展示和结果读取。
2. 官方离线数据源只允许使用回测数据集：`factor_data_dir` 下的 h5/parquet 文件、Qlib bin 文件、ST PIT universe 数据；禁止读取任何实时行情 DB snapshot 或 `factor_values_realtime`。
3. 官方离线因子源码只使用 `aistock_factor_catalog.code_text` 原始回测因子代码；改造后的 `realtime_code_text`、`qe_code_path` 只用于模拟盘、选股、荐股等实时/准实时模块，不参与官方离线指标和相关性主链路。
4. 官方唯一因子值缓存为 `rdagent_assets/factor_values/single/*.parquet` 加 `_meta.json`；独立指标、相关性、QE 回测复用同一份缓存。
5. WSL worker 启动后一次性加载底层 bin/h5/parquet 基础数据到内存缓存，因子代码后续读取从内存适配层获得数据，不再每个因子反复读文件。
6. 因子值按批次生成；每批因子值在内存中保留，用于同批独立指标计算；批完成后写入因子值缓存、指标入库并释放批内因子值。
7. 相关性计算继续使用同一份 `single/*.parquet`，可按相关性算法需要加载 float32 panel，但必须在 WSL 运行并受内存门禁控制。
8. 未来回测数据集更新到新日期时，只需要触发同一套官方全量重算或增量重算任务，生成带新 `as_of_date` / 数据指纹的同目录缓存。

## 5. 非目标

- 不修改独立指标计算公式、窗口定义、评级规则、月度 IC 派生逻辑；本方案只改变官方数据源、执行环境、缓存生成与批处理方式。
- 不修改因子原始 `code_text` 内容，不要求因子作者为离线指标改造代码。
- 不把 `factor_values_realtime` 继续作为官方缓存；该目录只允许作为历史兼容/迁移目录。
- 不新增生产 DB DDL，除非后续 Issue 明确提出并通过 DB 门禁。
- 不由 Codex 重启生产后端、前端或 WSL 生产服务；运行时激活由用户确认。

## 6. 术语与权威边界

| 名称 | 权威含义 | 禁止混用 |
| --- | --- | --- |
| official offline factor cache | `rdagent_assets/factor_values/single/*.parquet` + `_meta.json` | 不得等同于 `factor_values_realtime` |
| data_date / as_of_date | 官方离线数据截止日，例如 `2026-04-30` | 不得称为实时快照 |
| DataSnapshotManager snapshot | 历史 DB/realtime snapshot 机制 | 不参与官方离线主链路 |
| factor_data_dir | 回测 h5/parquet 底层数据目录 | 不得由实时 DB 替代 |
| qlib_bin | 回测收益、价格、交易日等基础数据 | 不得由实时行情替代 |
| code_text | 官方离线因子源码 | 不得用 realtime_code_text/qe_code_path 替代 |

## 7. 目标架构

```text
因子库 UI / 调度 API
  -> Windows FastAPI control plane
  -> DispatchService.create_and_submit_task(task_type=official_factor_full_compute)
  -> WSL / compute-node runner
      -> WSL-only preflight
      -> Factor catalog code_text resolver
      -> Backtest base data memory cache
      -> Batch factor code executor
      -> Atomic single parquet writer + _meta writer
      -> Existing qe_eval_v2 metric engine / writer
      -> batch memory release
  -> rdagent_assets/factor_values/single/*.parquet
  -> aistock_factor_metrics / aistock_factor_monthly_ic

因子相关性 UI / 调度 API
  -> Windows FastAPI control plane
  -> DispatchService.create_and_submit_task(task_type=correlation_compute)
  -> WSL runner
      -> WSL-only preflight
      -> FactorValueLoader(source=single, pipeline_dir=rdagent_assets/factor_values)
      -> CorrelationEngine Spearman/EWMA
      -> qe_factor_correlations / qe_correlation_metadata
```

## 8. 组件设计

### 7.1 Windows 控制面

新增或扩展服务只负责提交任务，不执行计算：

- `backend/services/quantevolver/official_factor_full_compute_dispatch_service.py`
- `backend/routers/quantevolver.py` 增加“全量因子独立指标计算”API 或扩展现有 official-evaluation API 的 mode。
- `frontend/src/app/quantevolver/components/FactorList.tsx` 增加 UI 操作入口、进度、内存门禁状态、失败列表和重试入口。

控制面禁止：

- 直接调用 `_compute_local()` 做重计算。
- shell out 到 WSL 本地路径执行脚本。
- 读取或写入 worker workspace 文件作为业务计算结果。

### 7.2 WSL runner

新增 runner：

```text
backend/scripts/run_official_factor_full_compute_wsl.py
```

职责：

1. 解析 dispatch payload。
2. 调用统一 WSL-only guard。
3. 加载 `.env`、节点配置和任务参数。
4. 调用官方全量离线计算 service。
5. 按 JSON lines 输出结构化事件、进度、内存统计和最终结果。
6. 任意 import、preflight、数据源、内存门禁失败都返回结构化 JSON error。

### 7.3 WSL-only guard

新增公共防护：

```text
backend/services/quantevolver/wsl_runtime_guard.py
```

建议接口：

```python
def assert_wsl_runtime(operation: str) -> None: ...
def is_wsl_runtime() -> bool: ...
```

必须接入：

- `run_official_factor_full_compute_wsl.py`
- `run_official_evaluation_wsl.py`
- `run_correlation_compute_wsl.py`
- `backfill_factor_cache.py` 主入口，作为历史工具也不得在 Windows 执行重计算。
- `FactorOfficialEvaluationService._compute_local()` 和 `correlation_compute_service.run_correlation_compute_local()` 的防线入口。

### 7.4 基础数据内存缓存

新增模块：

```text
backend/services/quantevolver/backtest_base_data_memory_cache.py
```

职责：

- 从 `factor_data_dir` 一次性读取官方允许的 h5/parquet 文件。
- 从 qlib bin 读取 close、calendar、可交易性等指标计算所需数据，或复用 `qe_eval_v2_metric_engine.prepare_shared_context()` 的结果。
- 生成只读缓存对象，不允许因子代码修改原始 DataFrame。
- 记录文件大小、读取耗时、DataFrame shape、内存估算、数据窗口、源文件 mtime/hash。
- 未知文件读取、越权路径读取、实时 DB 访问一律 fail-fast。

官方允许基础文件初始集合：

- `daily_pv.h5`
- `daily_basic.h5`
- `moneyflow.h5`
- `bak_basic.h5`
- `cyq_perf.h5`
- `sector_data.h5`
- `margin_detail.h5`
- `static_factors.parquet`
- Qlib bin close/calendar/suspend/ST PIT 相关数据

### 7.5 原始因子代码执行适配层

新增模块：

```text
backend/services/quantevolver/offline_code_text_factor_executor.py
```

职责：

- 输入 catalog `code_text`、factor name、window、ST PIT eligible index、基础数据缓存。
- 通过受控执行环境运行原始因子代码。
- 对 `pd.read_hdf`、`pd.read_parquet`、常见相对路径读取进行内存重定向。
- 不修改因子代码内容；只改变运行环境中的文件读取提供者。
- 因子执行结果必须标准化为 MultiIndex `(datetime, instrument)` + 单列 `value`。
- 重复 index、日期范围不足、全空、schema 错误均 fail-fast 或标记该因子失败。

如果第一阶段为了安全必须使用 subprocess，必须在 WSL Linux 下通过可控 fork / 只读共享 / memmap 方案证明不会复制全量基础数据；不得回到每因子独立读取文件的模式。

### 7.6 批处理调度器

新增模块：

```text
backend/services/quantevolver/official_factor_batch_compute_service.py
```

职责：

1. 解析 factor list。
2. 按可用内存、历史耗时、因子数据源复杂度规划 batch。
3. 每批生成因子值。
4. 每批写入 single cache 和 `_meta.json`。
5. 每批调用官方独立指标计算逻辑。
6. 每批释放 factor panel、临时 DataFrame、loader 单因子 cache。
7. 输出 task event：`preflight`、`base_cache_loaded`、`batch_started`、`factor_done`、`metrics_done`、`batch_released`、`correlation_ready`、`failed`、`success`。

推荐伪代码：

```text
assert_wsl_runtime()
preflight_paths_and_catalog()
base_cache = BacktestBaseDataMemoryCache.load_once()
metrics_ctx = qe_eval_v2_metric_engine.prepare_shared_context(...)
for batch in planner.plan(factors, memory_budget):
    memory_guard.before_batch(batch)
    factor_frames = executor.compute_batch(batch, base_cache)
    cache_writer.write_singles_atomic(factor_frames, metadata)
    metrics = metric_adapter.compute_batch(factor_frames, metrics_ctx)
    metrics_writer.save(metrics)
    release(factor_frames)
    FactorValueLoader.invalidate_single_cache()
    gc.collect()
    memory_guard.after_batch()
release(metrics_ctx, base_cache)
```

### 7.7 独立指标计算适配

不得改动指标公式。允许新增适配层，把“批内因子值 DataFrame”喂给现有 `qe_eval_v2_metric_engine.compute_single_factor_metrics()` 或等价内部函数。

要求：

- `prepare_shared_context()` 仍只执行一次。
- 每批指标计算可并行，但共享 context 必须只读。
- 指标入库仍使用官方单写入器，保持 `aistock_factor_metrics`、`aistock_factor_monthly_ic` 的现有权威语义。
- 不再通过 `FactorValueLoader.load_single_factor()` 逐因子读 parquet 作为主路径；parquet 是落盘审计和后续相关性/QE 复用，不是同批指标计算的输入源。

### 7.8 相关性计算

相关性计算继续使用 `rdagent_assets/factor_values/single`，但必须补齐以下约束：

- runner 和 local service 加 WSL-only guard。
- 禁止任何 `factor_values_realtime` fallback。
- status API 文案从“snapshot”改为“official offline factor cache / as_of_date”，避免与 DataSnapshotManager snapshot 混淆。
- 全量计算前校验请求因子数、缓存因子数、as_of_date、universe metadata、index_policy。
- 对低覆盖/全 NaN 因子给出分类失败原因，不能只显示“成功 105 个”。

## 9. 缓存数据契约

每个 `single/{factor_name}.parquet` 必须满足：

- MultiIndex：`datetime`, `instrument`
- 单列：`value` 或 factor name，写入时统一为 `value`
- 日期覆盖：`2018-08-01 ~ as_of_date`，允许仅在 metadata 中声明的 warmup 缺口
- index policy：`st_pit_buy_eligible_reindexed_v1`
- universe：`shsz_st_pit_active_v1`

`_meta.json` 必须记录：

```json
{
  "schema_version": "official_factor_cache_v2",
  "source_system": "official_offline_backtest_factor_data",
  "as_of_date": "2026-04-30",
  "data_start": "2018-08-01",
  "data_end": "2026-04-30",
  "factor_data_dir": "...",
  "factor_data_dir_fingerprint": "...",
  "qlib_bin_path": "...",
  "qlib_bin_fingerprint": "...",
  "universe_key": "shsz_st_pit_active_v1",
  "universe_rule_version": "...",
  "universe_fingerprint_sha256": "...",
  "index_policy": "st_pit_buy_eligible_reindexed_v1",
  "base_data_cache_policy": "load_once_readonly",
  "factors": {
    "factor_name": {
      "status": "ok",
      "code_hash": "...",
      "rows": 0,
      "nan_rate": 0.0,
      "date_range": "2018-08-01~2026-04-30",
      "computed_at": "...",
      "batch_id": "..."
    }
  }
}
```

缺失 metadata、metadata 与 parquet 不一致、as_of_date 不一致、universe 指纹不一致、code hash 不一致时不得命中缓存。

## 10. 内存和并行策略

当前 WSL 目标资源：内存约 64GB，Swap 256GB，但设计目标是尽量不使用 Swap。

### 9.1 预算

| 项 | 建议值 |
| --- | --- |
| WSL job soft RSS limit | 45GB - 48GB |
| WSL job hard RSS stop | 52GB - 55GB |
| minimum available memory before new batch | 8GB |
| swap growth warning | 512MB |
| swap growth hard stop | 1GB |
| initial factor batch size | 16 |
| stable factor batch size | 24 - 32 |
| factor generation workers | 1 - 2 |
| metrics workers | 2 - 4 |

### 9.2 原则

- 基础数据缓存只加载一次，尽量只读共享。
- 不允许每个因子 worker 各自加载完整 h5/parquet 文件。
- 不允许盲目使用多进程导致 pandas DataFrame 被复制。
- 优先使用线程或单进程 batch；需要多进程时必须证明 Linux copy-on-write 没有因写操作触发大规模复制。
- 对大 DataFrame 尽量转 float32、category、列式数组或只读 memmap，但不得牺牲指标一致性。
- 每批完成必须清理 factor panel、临时 Series/DataFrame、FactorValueLoader `_single_cache`。

### 9.3 内存门禁事件

任务日志必须输出：

- WSL total/available memory
- RSS before/after base cache
- RSS before/after each batch
- swap used before/after each batch
- batch factor count、row count、panel memory estimate
- hard stop reason

## 11. QE 回测复用规则

- 官方 full cache 覆盖 `2018-08-01 ~ 2026-04-30` 时，回测时间段若是其子集，QE 回测完全可以复用同一份 `single/*.parquet`。
- QE 回测实际使用 PIT 股票池时，应在读取缓存时按回测窗口和 PIT eligible index 过滤/重建索引，不需要另建一份因子值缓存。
- 如果未来回测数据集刷新到更新日期，应生成新 `as_of_date` cache；旧 cache 保留或按策略归档，但不得与新任务混用。
- 回测缓存命中必须校验 code hash、数据窗口覆盖、universe metadata、index policy、factor_data_dir/qlib_bin 指纹。

## 12. `factor_values_realtime` 降级策略

`rdagent_assets/factor_values_realtime` 后续定位：历史兼容/迁移目录。

要求：

1. 官方离线因子值、独立指标、相关性、QE 回测不得读取该目录。
2. 所有新代码不得新增对该目录的官方依赖。
3. 若保留旧服务用于模拟盘/选股，应明确命名为 realtime / live-only，不得出现在 official offline 路径。
4. 静态验收必须 grep 证明 official/correlation/backtest 相关路径无 `factor_values_realtime` fallback。

## 13. 用户界面设计要求

因子库页面新增“全量因子独立指标计算”能力：

- 展示数据窗口：`2018-08-01 ~ as_of_date`。
- 展示数据源：`factor_data_dir + qlib_bin + ST PIT universe`。
- 展示执行节点：默认 `wsl2-5080` 或用户选择的 WSL compute node。
- 展示 batch size、worker、内存 soft/hard limit、swap hard stop。
- 展示进度：总因子数、已生成缓存、已完成指标、失败因子、当前 batch、RSS、available memory、swap growth。
- 提供失败因子重试入口，但重试仍必须走 WSL dispatch。
- 相关性计算按钮应说明“使用官方离线因子缓存”，不再显示 realtime snapshot 语义。

## 14. 失败模式与处理

| 失败模式 | 处理 |
| --- | --- |
| 非 WSL 环境执行重计算 | fail-fast，返回 `wsl_runtime_required` |
| factor_data_dir 缺失 | fail-fast，不回退 DB |
| qlib_bin 缺失 | fail-fast，不回退实时行情 |
| catalog `code_text` 缺失 | 该因子失败，记录 `missing_code_text` |
| 因子访问未知文件 | 该因子失败，记录 path，不允许真实文件读取 |
| 因子结果 schema 错误 | 该因子失败，记录 schema 错误 |
| batch 内存超过 soft limit | 降低后续 batch size 或暂停新 batch |
| batch 内存超过 hard limit / swap hard stop | 任务 failed，保留已成功因子缓存和指标审计 |
| `_meta.json` 写失败 | 删除对应 parquet，保持 cache/meta 一致 |
| 指标写入失败 | 因子值缓存保留但指标状态 failed，禁止报告全绿 |
| 相关性只有部分因子成功 | 汇报 excluded 分类，不得显示“全量成功” |

## 15. 开发阶段拆分

### Phase 0：设计冻结和 Issue 登记

- 本文合入或作为后续 Issue 的批准设计输入。
- 使用 `scripts/aistock_issue_workflow.py submit-bug --create-fix-worktree` 登记 T3/P1 或 P0 级 BUG/Issue。
- `allowed_write_scope` 必须包含后续实现文件和测试文件。

### Phase 1：WSL-only 和控制面门禁

- 新增 WSL guard。
- official evaluation / correlation / legacy backfill 入口接入 guard。
- 新增 dispatch task type `official_factor_full_compute`。
- UI/API 只能提交任务，不本地计算。

### Phase 2：基础数据内存缓存

- 新增 backtest base data cache。
- 实现一次性读取、只读缓存、文件访问白名单、内存统计。
- 单元测试证明同一任务中每个基础文件只读取一次。

### Phase 3：原始 code_text 执行器

- 实现原始因子代码读取适配，不改 factor code。
- 移除官方离线链路对 `transformation_status` / `qe_code_path` 的依赖。
- 单因子和小 batch smoke 通过。

### Phase 4：批量因子值生成 + 指标同批计算

- 实现 batch planner。
- 每批生成 factor frames、写 single cache、计算指标、释放内存。
- 指标公式和写入器保持现有 qe_eval_v2 语义。

### Phase 5：相关性和 QE 复用一致性

- 相关性状态和执行路径完成 official cache 语义统一。
- QE 回测缓存读取校验同一份 official cache。
- 移除或隔离官方链路上的历史 realtime/snapshot 分支。

### Phase 6：UI 与全链路验证

- 因子库 UI 加全量计算入口和进度展示。
- WSL 2 因子、16 因子、全量 575 因子验证。
- 相关性全量验证。
- QE 子窗口缓存命中验证。

## 16. 验收门禁

后续任何实现 PR 必须逐项提交验收矩阵，缺一项即不得合入。

| Gate | 验收项 | 证明方式 | 阻塞条件 |
| --- | --- | --- | --- |
| G0 | DESIGN-COMPLIANCE-001 矩阵完整 | PR 附 `design_item -> implementation_refs -> evidence -> status` | 任一设计项缺实现或缺证据 |
| G1 | WSL-only | Windows 下调用 runner/local compute 返回 `wsl_runtime_required`；WSL 下正常进入 preflight | Windows 可执行重计算 |
| G2 | 禁止 realtime cache | grep official/correlation/backtest 路径无 `factor_values_realtime` fallback；单测覆盖 | 仍有 fallback 或自动读取 |
| G3 | 原始 code_text 权威 | 单测证明官方离线不要求 `qe_code_path` / `transformation_status` | 仍因未转化而排除因子 |
| G4 | 基础数据只读一次 | monkeypatch `pd.read_hdf/read_parquet` 计数；多因子 batch 中文件读取不随因子数线性增长 | 每因子重复全量读文件 |
| G5 | batch 内存释放 | 16 因子 batch 后 RSS 回落或不持续线性增长，`_single_cache` 清空 | RSS 随 batch 无界增长 |
| G6 | 指标公式不变 | 选定 2 因子新旧指标结果在容忍误差内一致 | IC/RankIC/收益字段明显漂移且无解释 |
| G7 | cache/meta 原子一致 | 模拟 meta 写失败时 parquet 回滚；meta/parquet 校验通过 | 出现 orphan parquet 被视为成功 |
| G8 | 相关性使用同一 cache | 相关性输入 factor count 等于 official cache 可用因子集合，excluded 有分类原因 | 只显示 105 个且无明确原因 |
| G9 | QE 子窗口复用 | 指定回测子区间命中官方 cache，并按 PIT index 过滤 | 子区间无法命中或股票池错配 |
| G10 | UI 真实业务路径 | Browser/API smoke：UI 提交 dispatch、显示 WSL 节点、内存、batch、失败列表 | UI mock 成功或调用本地计算 |
| G11 | 生产门禁 | 明确 DB DDL/runtime/dependency gate；未授权不重启生产服务 | 未声明生产影响 |
| G12 | 全量验收 | WSL 全量 575 enabled factors 生成 cache、指标完成、相关性完成 | 全量不足且无失败分类 |

## 17. 设计验收索引

| Design ID | 设计条款 | 实现时必须证明 |
| --- | --- | --- |
| DAI-001 | 官方重计算 WSL-only | guard + Windows fail-fast + WSL smoke |
| DAI-002 | 官方数据源仅 factor_data_dir/qlib_bin/ST PIT | 无 DB realtime / DataSnapshotManager 依赖 |
| DAI-003 | 官方源码仅 code_text | 不依赖 qe_code_path/realtime_code_text |
| DAI-004 | 单一 official factor cache | 独立指标/相关性/QE 指向 `rdagent_assets/factor_values` |
| DAI-005 | 基础数据一次性内存缓存 | 读文件计数和内存日志 |
| DAI-006 | batch factor values + batch metrics | 每批生成、指标、释放事件完整 |
| DAI-007 | 指标公式不变 | 回归对比和现有测试通过 |
| DAI-008 | 相关性无 realtime fallback | 静态 grep + WSL task evidence |
| DAI-009 | QE 回测可复用官方 cache | 子窗口命中和 PIT 过滤验证 |
| DAI-010 | `factor_values_realtime` 降级 | 只剩 live-only/legacy 明确路径 |
| DAI-011 | UI 不 mock | Browser/API 真实 dispatch 验证 |
| DAI-012 | 内存不打爆 WSL | RSS/swap 门禁证据 |

## 18. 测试方案

### 17.1 静态测试

- `python -m py_compile` 覆盖新增 runner/service。
- grep 禁止模式：
  - official offline 路径出现 `factor_values_realtime`
  - official offline 路径出现 `RealtimeFactorDataLoader`
  - official offline 准入要求 `qe_code_path IS NOT NULL`
  - runner 绕过 dispatch 直接在 Windows 执行

### 17.2 单元测试

建议新增：

- `backend/tests/quantevolver/test_wsl_runtime_guard.py`
- `backend/tests/quantevolver/test_official_offline_factor_data_cache.py`
- `backend/tests/quantevolver/test_official_code_text_factor_executor.py`
- `backend/tests/quantevolver/test_official_factor_batch_compute.py`
- `backend/tests/quantevolver/test_official_cache_contract.py`
- `backend/tests/quantevolver/test_correlation_official_cache_only.py`

### 17.3 WSL smoke

分三档：

1. 2 因子 smoke：验证 code_text、基础数据缓存、single cache、指标入库。
2. 16 因子 batch：验证 batch 内存、并行、释放、失败分类。
3. 全量因子：验证 575 enabled factors，输出成功/失败列表，失败必须有类型和日志。

### 17.4 相关性验证

- 使用同一份 official cache 触发全量相关性。
- 校验请求因子数、缓存因子数、成功因子数、excluded 分类。
- 校验 `qe_factor_correlations` 和 `qe_correlation_metadata` 的 as_of_date / universe metadata。

### 17.5 QE 回测复用验证

- 选择官方窗口子集回测。
- 验证 cache hit，不重新生成同因子同窗口基础缓存。
- 验证 PIT 股票池过滤后回测结果可运行。

### 17.6 UI 验证

- Browser 打开因子库页面。
- 点击“全量因子独立指标计算”。
- 确认 payload 提交到 dispatch task。
- 确认 UI 展示 WSL 节点、batch、RSS、swap、成功/失败因子、cache path。
- 点击相关性计算，确认使用 official cache，不显示 realtime snapshot 术语。

## 19. 可合入 Main 的完成标准

代码实现 PR 必须同时满足：

1. 本文 DAI-001 至 DAI-012 全部 `status=passed`。
2. G0 至 G12 全部门禁通过，或用户明确批准设计变更后更新本文。
3. 全量计算在 WSL 完成，成功/失败因子都有可审计记录。
4. 相关性不再只因 cache 数量不足而显示部分成功；若有失败，必须分类解释。
5. UI 真实业务路径可触发官方 WSL task。
6. 无生产 DB DDL，或 DDL 已按流程声明并验证。
7. 未经用户授权，不重启生产 backend/frontend/TDX。
8. PR 描述包含设计验收矩阵、测试证据、生产门禁状态。

## 20. 后续 Issue 建议

建议登记一个 T3/P1 BUG 或设计驱动 Issue：

标题：官方全量因子独立指标计算必须在 WSL 使用回测 code_text 与底层数据内存缓存分批生成因子值并计算指标

建议 `allowed_write_scope`：

- `backend/scripts/run_official_factor_full_compute_wsl.py`
- `backend/scripts/run_official_evaluation_wsl.py`
- `backend/scripts/run_correlation_compute_wsl.py`
- `backend/services/quantevolver/official_factor_full_compute_dispatch_service.py`
- `backend/services/quantevolver/official_factor_batch_compute_service.py`
- `backend/services/quantevolver/backtest_base_data_memory_cache.py`
- `backend/services/quantevolver/offline_code_text_factor_executor.py`
- `backend/services/quantevolver/wsl_runtime_guard.py`
- `backend/services/quantevolver/factor_official_evaluation_service.py`
- `backend/services/quantevolver/correlation_compute_service.py`
- `backend/services/quantevolver/factor_value_loader.py`
- `backend/services/quantevolver/factor_eligibility_service.py`
- `backend/routers/quantevolver.py`
- `frontend/src/app/quantevolver/components/FactorList.tsx`
- `scripts/backfill_factor_cache.py`
- `backend/tests/quantevolver/`
- `backend/tests/test_correlation_compute_independence.py`
- `backend/tests/test_factor_cache_wsl_env.py`

## 21. 设计自检结论

本设计满足用户明确要求：

- 先设计再开发。
- WSL 环境执行，不在 Windows 重计算。
- 官方离线链路使用回测 bin/h5/parquet 和 qlib bin，不使用实时 DB snapshot。
- 使用原始 `code_text`，不使用改造后的 realtime 因子代码。
- 一份因子值缓存供独立指标、相关性、QE 回测复用。
- 基础数据一次性加载到内存，因子值按 batch 生成、计算指标、释放。
- 64GB WSL 下设置 RSS/swap 门禁，避免 swap。
- 后续实现必须用设计验收矩阵证明完整符合，不允许简化版/子集版/POC 冒充完成。
