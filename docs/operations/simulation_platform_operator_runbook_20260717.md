# 模拟盘平台只读诊断 Operator Runbook

状态：BUG-687 source contract。适用于 LocalSIM 与 MiniQMT 模拟盘；上位权威为
`docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md`。

## 1. 安全边界

本 runbook 只使用 GET API。执行这些命令不会启动行情 feed、修改运行状态、重放订单、修复数据库、
调用 broker，也不会改变任何 binding 或执行算法。

禁止把以下动作作为诊断步骤：

- “先重启看看”；
- 手工修改或删除 durable fact；
- 调用 scheduler `start/stop/tick`；
- 调用 operator command、下单、撤单或 broker reconcile 写路径；
- 增加 RBAC、审批、人工 acknowledge、confirm-run 或 execution gate。

告警是当前事实的只读快照。合法数据、连接或调度恢复后，下一次 GET 会自动不再返回对应告警；业务恢复不等待人工 acknowledge。

## 2. Canonical API 与查询契约

平台聚合端点：

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?trade_date=2026-07-17"
```

支持组合定向查询：`trade_date`、`binding_id`、`run_id`、`runtime_id`、`plan_id`。`limit` 范围为 1..100。
不带 identity 时默认读取当前有效交易日；只带 `runtime_id` 时即使 daily run 尚未创建，也读取 canonical MiniQMT quote health。
无 `trade_date/run_id` 的跨日扫描最多检查 500 条 run；超限返回
`SIMULATION_PLATFORM_DIAGNOSTIC_SCAN_TRUNCATED`，要求缩小查询，不返回不完整假结果。

响应 schema 为 `simulation_platform_diagnostics_v1`，固定包含：

- `overall_health`；
- `layers.process/lifecycle/bindings/backends/durability/business`；
- `metrics`；
- `alerts`；
- `runbook` 与 `side_effect_contract`。

所有层和告警均明确 `execution_gate=false`；alerts 明确 `acknowledge_required=false`。

## 3. 固定排查顺序

### 3.1 Process

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/scheduler/status"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?trade_date=2026-07-17"
```

正常：`scheduler_loop_health.status=HEALTHY`，process 为 `HEALTHY`，active market phase 的 tick lag 不超过 `2 * interval_seconds`。

异常：

- `SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION`：top-level loop 仍在失败，即使 thread 仍存活也必须 BLOCKED；
- `SIMULATION_SCHEDULER_PROCESS_INACTIVE`：进程内 scheduler 未运行；只有 active market phase 才形成当前 critical alert；
- `SIMULATION_SCHEDULER_TICK_LAG_EXCEEDED`：active market phase 的 last tick 超过两倍 scheduler interval。

自动恢复：下一次成功 tick 清除 active loop failure/tick-lag alert，同时保留累计计数。若成功 tick 后仍保持 active failure，属于代码或状态投影 Bug。

### 3.2 Lifecycle

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?trade_date=2026-07-17"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/runs?trade_date=2026-07-17&limit=100"
```

正常：每个 run 是明确的 `IN_PROGRESS` 或 terminal 状态；`FAILED_RETRYABLE/FAILED_TERMINAL` 不得被 scheduler alive/no-op window 覆盖成绿色。

异常 reason：`SIMULATION_QUERY_HAS_BLOCKING_RUNS`、`SIMULATION_PLATFORM_DIAGNOSTIC_READBACK_FAILED`。
单 binding 失败不得阻断其他独立 binding 的诊断或调度。数据/连接恢复后由既有 scheduler cadence 自动重试；无需人工开闸。

### 3.3 Binding

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?trade_date=2026-07-17&binding_id=<binding_id>"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?run_id=<run_id>"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?plan_id=<plan_id>"
```

核对 frozen `trade_date/binding_id/run_id/runtime_id/plan_id`、backend、run status、last stage、updated time。
`SIMULATION_BINDING_BLOCKED` 只属于对应 identity。`SIMULATION_RETIRED_ROUTE_CALLED` 表示产品路径仍调用已退役 route，必须代码修复，禁止兼容 no-op 或回退旧路径。

### 3.4 Data / Backend

LocalSIM：读取 `backends[backend=local_sim]`、对应 business/durability 层。

MiniQMT：

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/platform-diagnostics?runtime_id=<runtime_id>"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/miniqmt/quote-diagnostics?runtime_id=<runtime_id>&limit=100"
```

MiniQMT canonical health 必须来自 quote diagnostics 的 durable health、subscription、writer、controller、gateway 与 OMS 联合投影；legacy `/monitor/miniqmt/status` 不参与模拟盘健康判定。
`FAILED/DEGRADED` 形成 `MINIQMT_QUOTE_PROGRESS` 告警；durable health 恢复并通过 readback 后自动解除。

### 3.5 Durable facts

LocalSIM 核对：

- `local_sim_persistence_v1/v2`；
- `local_sim_projection_outbox_v1`；
- terminal/readback failure；
- economic/projection generation 与 run identity。

阈值：`PENDING` 持续超过 120 秒、`PROJECTION_RETRYABLE`、readback failure 形成 durability warning；terminal failure 形成 BLOCKED。`PROJECTED + PERSISTED` 后自动解除。不得重写经济事实或重复 projection side effect。

MiniQMT 核对：batch/runtime identity 唯一；`results` cardinality 等于 `total`；`succeeded + failed + pending == total`；top-level 与 durable batch counts 一致。以下 reason 必须代码修复，不能过滤、归零或 padding：

- `SIMULATION_PLATFORM_RUNTIME_IDENTITY_CONFLICT`；
- `SIMULATION_PLATFORM_DURABLE_BATCH_CARDINALITY_MISMATCH`；
- `SIMULATION_PLATFORM_DURABLE_BATCH_COUNT_MISMATCH`；
- `SIMULATION_PLATFORM_BUSINESS_COUNT_CONFLICT`。

### 3.6 Broker / Reconcile

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/runs/<run_id>"
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/execution-parents?binding_id=<binding_id>&trade_date=2026-07-17&limit=100"
```

仅检查 parent/algo/child/order/trade/reconcile 链和 mismatch count。不要从 runbook 调用 broker reconcile 写路径。
MiniQMT pending algo、submitted child 和 reconcile mismatch 均从 durable fact 精确计数；terminal run 仍有 active/pending work 返回
`SIMULATION_TERMINAL_RUN_HAS_ACTIVE_WORK`。

### 3.7 TCA

```powershell
curl.exe -sS "http://127.0.0.1:8001/api/v1/simulation-runtime/execution-parents/<parent_id>/tca?revision=<revision>&snapshot_kind=<kind>"
```

核对 evidence/markout/TCA completeness。TCA capture failure 是 observation-only，不能回滚或改写已经确定的 broker execution，也不能冒充执行成功。

## 4. Metrics 与 cardinality

metric labels 只允许：`backend/control_revision/status/reason_code/market_phase/source`。
禁止 `run/order/symbol/package/strategy/binding/runtime/plan` 等高基数 label。单次响应最多 256 个 series；超限 typed fail loud，不裁剪后报成功。

平台至少投影：scheduler success/failure/tick lag、binding/run status、LocalSIM active/partial/residual/bar lag/transaction failure/outbox backlog、MiniQMT callback age/recent normalized/rejected/pending/submitted/reconcile mismatch、invalid payload、false-green prevention、durable readback mismatch。

## 5. Alerts 与自动恢复

单次响应最多返回 100 条 alert；`observed_count/returned_count/truncated` 必须同时出现。高基数 identity 允许出现在 alert/diagnostics body，但不得进入 metric labels。

告警只通知当前事实，不修改业务：

- scheduler stopped/loop failure/tick lag；
- active binding failure；
- LocalSIM causal bar lag超过 120 秒；
- LocalSIM outbox/readback/terminal failure；
- MiniQMT quote health failed/degraded；
- active algo 在收盘后仍无 terminal classification；
- retired route 被调用。

恢复判定始终是重新读取当前事实。禁止人工 acknowledge 后才恢复业务。

## 6. 交付状态分离

以下状态必须分别报告，不能互相替代：source merge、CI、production DDL、production dependency/config、restart、binding DML、runtime readback、正常交易日 LocalSIM、正常交易日 MiniQMT、broker/reconcile/TCA。

BUG-687 source PR 不新增 DB object、dependency 或生产配置，不执行 DDL/DML，不调用 broker，不重启服务。合入不等于 runtime 已激活；用户重启后再做本 runbook 的生产只读 readback。
