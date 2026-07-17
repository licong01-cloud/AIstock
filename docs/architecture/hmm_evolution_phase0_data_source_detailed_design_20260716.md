# HMM 演进系统 Phase 0 数据源详细设计

> 版本：v2.2（2026-07-17 controlled external acceptance 版）<br>
> 状态：实现、单元/contract CI、Prediction Store 零副本与受控只读 DB/PIT sector integration 均已验收。<br>
> 设计权威：总体蓝图 `hmm_evolution_and_risk_management_system_design_20260716.md` v1.4。<br>
> 运行权威：`backend/services/hmm_data_source/README.md`。

本文替代 2026-07-16 初稿中的伪 async DB、旧 market 表、隐式 latest snapshot、无
manifest pickle、硬编码 QE endpoint、建用户/DDL 部署器及未经测量的验收结论。旧代码片段
不再是实现依据。

## 1. 目标与非目标

Phase 0 只交付统一只读数据源和可信 artifact cache：

- 回测：读取明确 QE `task_id/loop_name` 的 `pred.pkl` / `label.pkl`。
- 实时分析：读取上一完成交易日的 canonical market 数据和显式 candidate provider。
- 数据源切换不改变上层评估接口。
- 不写 QE、StrategyPackage、Paper、模拟盘或交易状态。
- 不创建 `hmm_evolution.*` / `hmm_risk.*`；这些属于 Phase 1/2 独立 Python bootstrap。
- 不启动服务，不接实盘/模拟盘，不自动替换生产 HMM。

## 2. 模块结构

```text
backend/services/hmm_data_source/
├── base.py
├── models.py
├── exceptions.py
├── db_repository.py
├── realtime_source.py
├── backtest_source.py
├── prediction_store_resolver.py
├── artifact_manifest.py
└── cache_manager.py
```

`HMMDataSourceInterface` 保持五个方法：`get_predictions`、`get_labels`、
`get_sector_mapping`、`validate_date_range`、`get_available_date_range`。

## 3. BacktestDataSource

### 3.1 权威身份

`base_loop_ref` 必须是两个安全 segment：`<task_id>/<loop_name>`。任务 node 来自
`qe_evolution_tasks.node_id` 的只读查询；禁止固定 localhost、固定 node 或猜测 workspace。

### 3.2 Artifact 获取

默认策略为 `prediction_store_first`：

1. 将 `<task_id>/LoopN` 映射为 Prediction Store run key `<task_id>_LN`。
2. 调用 `ModelStoreService.resolve_archive_manifest()`，验证 task/loop identity、SHA256、
   size 和实际 blob；HMM 额外要求目标 artifact 的 `collection_status=available`、
   `parser_status=parsed`、正数 row count。
3. 命中后直接读取 content-addressed blob，不写入 HMM artifact cache，并保存
   `source=prediction_store`、URI、SHA、row count、`zero_copy=true` 的运行时 source receipt。
4. manifest 不存在或目标 artifact 未登记时才允许 workspace fallback；manifest 或 blob
   已存在但损坏时 fail loud，不允许以 fallback 掩盖完整性错误。
5. workspace fallback 从 `qe_current_recorder.json`，必要时
   `qe_extracted_recorder.json` 解析 `experiment_id/recorder_id`。
6. 在下载 pickle 前读取远端 sidecar / HMM manifest / QE completion payload；manifest
   必须给出 artifact 名、schema version、SHA256、size、row count、`quality_status=ok`。
7. 只调用仓库已有的 `download_workspace_file_bytes()` 下载白名单内容；下载 bytes 与
   远端 SHA/size 不一致时拒绝缓存，反序列化后 row count 不一致时清除 entry 并失败。

白名单固定为 `pred.pkl`、`label.pkl`。读取 manifest 是信任验证，不授权下载配置文件。

### 3.3 Client 生命周期

- 外部注入 client：调用方拥有并关闭。
- 内部按 task node 创建 client：`BacktestDataSource.aclose()` / async context manager 关闭。

## 4. Artifact cache

Prediction Store 命中不创建额外副本。仅 workspace fallback 使用
`tmp/hmm_evolution_cache/<sha256(loop_ref)>/`；每个 artifact 有独立
`*.manifest.json`，本地 manifest 绑定远端 provenance。

安全要求：

- loop/artifact 名称校验和 repo cache-root containment；
- cache root、entry、artifact、manifest、clear/eviction 目标拒绝 reparse point；
- artifact/manifest 使用临时文件 + `os.replace` 原子发布；
- 同进程锁 + 独占 lock-file 跨进程互斥；
- 强制 SHA/size/row count/TTL 校验，不能关闭 checksum；
- 单文件和总容量上限；超限按最旧 entry 淘汰；
- `test_fixture` provenance 默认拒绝，仅测试显式开启；
- 缺 manifest、损坏、过期、半写均视为 cache miss/fail closed。

## 5. RealtimeDataSource

### 5.1 候选身份

实时预测必须提供显式 `candidate_id` 和已登记 provider。兼容字段
`snapshot_id="latest"` 不得解析为预测来源；无 provider 时返回明确错误。

### 5.2 Canonical DB

DB pool 是同步 psycopg2 context manager。`HMMDataRepository` 使用 `get_conn()`，异步
service 通过 `asyncio.to_thread()` 调用，不使用 `async with` 伪装异步。

只读表与字段：

- `market.kline_daily_raw(ts_code, trade_date, close_li)`；
- `market.trading_calendar(trade_date, is_trading)`；
- `market.sw_index_member(ts_code, l2_code, in_date, out_date)`。

所有查询绑定显式 `as_of_date`。最新日是 as-of 之前的完成交易日；horizon 使用交易日
序列，禁止 `CURRENT_DATE` 和自然日减法。

## 6. 配置

`DataSourceConfig` 支持：

- backtest：`base_loop_ref`、`artifact_source_preference`、`label_horizon_days`、
  `cache_dir`、`max_artifact_bytes`、`max_cache_bytes`、`cache_ttl_seconds`；
- realtime：`candidate_id`、`as_of_date`、`lag_days`、`max_query_days`。

## 7. 验证架构

### 7.1 普通 PR CI

`nox -s hmm_data_source_backend`：

- compileall 全模块；
- Backtest/Realtime/repository/cache/isolation/config 测试；
- integration 默认排除；
- branch-aware coverage 下限 70%；
- 保存 coverage XML、JUnit 与慢测时长。

`ci_change_classifier.py` 将 HMM service/test 路径映射到该 session。

### 7.2 受控 integration

`nox -s hmm_data_source_readonly_integration` 需要：

- `AISTOCK_HMM_READONLY_INTEGRATION=1`；
- `HMM_TEST_QE_LOOP_REF=<task>/<loop>`；
- `HMM_TEST_AS_OF_DATE=YYYY-MM-DD`。

session 将 Prediction Store root 绑定到 canonical repo，backtest 使用
`prediction_store_only`；DB repository factory 使用 `REPEATABLE READ` 且强制
`transaction_read_only=on`。只执行 QE artifact 读取和 DB SELECT，不运行 DML/DDL。
缺任一坐标直接拒绝，不允许硬编码旧 task 或“全部 skip 也算通过”。

此外保留不访问 DB/workspace 的 Prediction Store-only smoke：显式设置
`artifact_source_preference=prediction_store_only`，断言真实 loop 可读取、source receipt
为 `zero_copy=true`，且 HMM cache 中没有生成对应 artifact。

## 8. 部署与生产门禁

Phase 0 不需要数据库部署。`scripts/deploy_hmm_data_source.py` 默认只输出 plan/verify，唯一
apply 是显式确认后创建 repo `tmp/` 下的 cache 目录。它不连接 DB、不建用户、不 GRANT、
不建 schema、不改 `.gitignore`。

生产门禁：

- `production_ddl_gate=noop`；
- `production_backend_dependency_gate=noop`；
- `production_frontend_dependency_gate=noop`。

Phase 1 schema 必须单独交付幂等 Python bootstrap、完整 `COMMENT ON`、开发库复跑证据和
明确 production DDL approval。

## 9. 实现证据与当前状态

| 范围 | BUG / PR | 状态 |
|---|---|---|
| QE workspace client/node 契约 | BUG-688 / #2260 | 已合入 |
| 同步 DB、canonical schema、candidate identity | BUG-689 / #2266 | 已合入 |
| remote manifest/cache 信任边界 | BUG-690 / #2270 | 已合入 |
| 专用测试/coverage/CI 路由 | BUG-691 / #2273 | 已合入 |
| unsafe deploy helper/文档收敛 | BUG-692 | 本次修复 |
| Prediction Store 零副本复用 | Phase 0 收尾 PR | 本次实现；真实 prediction smoke 通过 |
| controlled external acceptance | 本验收 PR | 高收益 QE Loop8 + read-only DB/PIT sector；4 passed |

最终 receipt 使用 `qe_20260706_013235_bbd4/Loop8` 和 `as_of_date=2026-07-17`：
Prediction Store 2,260,161 rows、zero-copy/no HMM cache；DB completed date
`2026-07-16`，PIT mapping 5,864 symbols / 131 L2 codes，transaction read-only。
Phase 0 已 externally accepted，Phase 1 implementation unlocked。

## 10. Phase 1 接入原则

Phase 1 只能消费 Phase 0 返回的标准化只读视图，并保存可重放的 source manifest、candidate
manifest、evaluation spec、evaluator version 和 input hash。不得绕过 cache provenance、
candidate identity 或 as-of 边界，也不得把 top-3 推荐变成生产门禁。
