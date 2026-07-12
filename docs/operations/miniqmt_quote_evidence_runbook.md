# MiniQMT quote evidence operator runbook

本文覆盖 P1-D 的 durable quote evidence、markout、auction observation、metrics、只读诊断，以及 P1-E 的 `B0_QUOTE_V2` assignment/action/child/markout readback。它不创建或修改 binding/config，不启用 ingress，不调用或重放 broker，不修改 `LEGACY_B0`，也不构成 LIVE 或 `ADAPTIVE_IS_L1` 操作手册。

## 当前自动边界

- `MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED=false` 是默认状态；P1-D/P1-E 代码合入不改变它。switch=false 时只允许已有 durable active `B0_QUOTE_V2` runtime 自动恢复为 `DRAINING`，不得接纳新 assignment。
- `production_ddl_gate=pending` 前，不能启用 quote ingress；代码合入、单元测试通过或本地 SQL 文件存在都不是 production DDL 已应用的证据。
- P1-D 没有审批、RBAC、人工 acknowledge、confirm-run 或“手工恢复成功”状态。数据、连接或生命周期条件恢复后，下一个合法 lifecycle tick 自动再次尝试；历史 evidence 永不原地修改。
- durable ack 只在 repository 事务提交并 post-commit readback 核对 `event_id/type/source/hash/created_at` 后成立。日志、outbox 入队、metrics 或 API `ok=true` 都不能替代它。

## 只读诊断

以下端点不会构造 subscriber、scheduler、gateway 或 broker client，不会写 event、自动 repair 或重连：

```text
GET /simulation-runtime/miniqmt/quote-diagnostics?runtime_id=<runtime_id>&symbol=<optional>&cursor=<optional>&limit=1..500
GET /simulation-runtime/miniqmt/quote-evidence?runtime_id=<runtime_id>&market_data_id=<id>|evidence_id=<id>&include_archived=false&cursor=<optional>&limit=1..500
```

`quote-diagnostics` 的 `ok=true` 仅说明查询完成。必须单独查看 `health.status`、`production_ddl_gate`、outbox backlog、persist failure、最近 reason/stage、markout coverage 和分页 cursor。无 durable health row 时返回 `UNKNOWN`，不得将其解释为 healthy。

`quote-evidence` 的每条 `durable_receipt.durable_ack`、`readback_verified` 与 `link_complete` 独立判断；`link_complete=false` 时必须读取 `missing_links`，字段缺失和 identity 无法 readback 分别显式报告。空 records 只是没有匹配的 evidence，不是 action/child/trade/markout 闭环成功。两个 endpoint 都把 `(sequence,event_id)` cursor 下推到 repository，禁止先加载完整 runtime journal 再内存分页。诊断响应不输出 account id、secret 或完整 raw callback；symbol 和受限 hash/link 仅在该 runtime 的分页查询中出现。

## 自动故障处理语义

| 条件 | 可观察 reason/stage | 自动语义 | 禁止操作 |
|---|---|---|---|
| high-priority evidence outbox 满 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL` / `PERSIST` | 对应未来 B0_QUOTE_V2 symbol 保持 fail-closed；health/cadence 使用独立低优先级 slot；已发生 child receipt 使用显式 reserve；无关 symbol 可继续 | 丢弃 action/child/markout、让 health/cadence 挤占、提交 broker |
| transient PostgreSQL SQLSTATE | `...EVIDENCE_PERSIST_FAILED` / `PERSIST` | 仅 `08000/08001/08003/08006/40001/40P01/55P03/57P01` 使用显式有界指数退避 | retry schema/CHECK/FK/hash 冲突；回退 JSON 或内存成功 |
| idempotency conflict | `...EVIDENCE_IDEMPOTENCY_CONFLICT` / `PERSIST` | coordinator 进入 FAILED，保留两侧 hash context | 覆盖 event、换 event type/source、宣称 durable ack |
| target 后无可证明首个 mark quote | `...MARK_WINDOW_EXPIRED` 或 `...MARKOUT_QUOTE_UNAVAILABLE` / `MARKOUT` | 追加 stable `UNAVAILABLE` mark | 等待午休后、下一交易日或选择更晚 quote |
| history/generation/restart gap | `...MARKOUT_HISTORY_UNAVAILABLE` / `MARKOUT` | 追加 stable `UNAVAILABLE`，不猜测首个 quote | 用重启后的 latest quote 替代 |
| continuous segment 结束 | `...MARKET_SESSION_ENDED` / `MARKOUT` | 追加 stable `UNAVAILABLE` | 跨午休、收盘或交易日取 mark |
| auction 原始字段不可用 | `...CLOSING_AUCTION_CAPABILITY_UNAVAILABLE` / `ELIGIBILITY` | `OBSERVE_ONLY` unavailable evidence；continuous 不受阻塞 | 从 last/pre-close/L1-L5/limit/15:00 合成 auction 字段 |

持久化 retry 达到配置上限后 health=`FAILED`；这不是需要人工确认的状态。后续合法 lifecycle epoch 可以自动建立新的 writer，但不得把旧 outbox 内容替换为新 evidence 或改变既有 market/action/trade identity。

## DDL gate 与 production readback

P1-D 只允许 operator 在明确授权后运行下列两份 SQL；应用启动、诊断 API 和 Codex 均不得自动执行：

```text
backend/migrations/miniqmt_quote_ingress_event_types_20260712.sql
backend/migrations/miniqmt_quote_ingress_event_types_20260712.rollback.sql
```

forward migration 只扩展 `ck_miniqmt_event_type` 与 `ck_miniqmt_event_source`。它在 transaction 中做 exact-old/exact-target preflight、锁表后二次 preflight、target no-op 与 `pg_get_constraintdef` readback；不新建表、列、索引、role 或数据。

生产 readback 必须记录 constraint names/OIDs、validated flags、canonical definition/hash、old/new event/source row counts、unknown-value count、migration/rollback file SHA-256、查询时间和 DB identity。只有 readback exact target 且 unknown count 为零时，才可报告 `production_ddl_gate=applied_and_verified`。未获授权或尚未执行时固定报告 `pending`。

rollback 只在 exact target schema 且五个新 event type 和 `quote_ingress` source 的行数都为零时允许执行；任何新行存在都必须拒绝并记录 type/source counts 与 min/max sequence。rollback 不删除 evidence，不切换 revision，也不改变 active parent。

## 留存、指标与告警

- action/reject/child/protection/markout evidence 与 trade/child anchors 不按普通 sequence prune；至少 active 90 天且关联 trade 的 60/300/900 秒 mark 全部 terminal 前保持 active。满足条件后只能 soft archive，`include_archived=true` 仍须重建同一 identity 闭包；任何 pending mark anchor 都不受 count prune。
- `QUOTE_OBSERVED` cadence 和 `QUOTE_INGRESS_HEALTH` aggregate 在 active 14 天后可 soft archive；archived row 只能通过 `include_archived=true` 的只读 readback 查询，绝不 delete。
- metrics 的 labels 仅限 market、capture type、state、reason code、stage、horizon、source method、quantile 等有界值；严禁 runtime/binding/parent/child/trade/market-data/symbol/account id labels。
- 观察 metric/alert 永远 `observation_only=true`、`execution_gate=false`、`requires_acknowledge=false`。告警不是 broker gate、审批或人工任务。

## P1-D / P1-E 边界

P1-D 提供 evidence contract、repository/readback、migration、markout selector、auction manifest、metrics、只读 diagnostics 和本 runbook。它不接通 action submit/cancel 或 broker/reconcile，也不激活真实 SIM ingress。

P1-E 才负责冻结并显式绑定 `B0_QUOTE_V2`、真实 SIM parity、action durable-ack-before-submit 的运行接线与 Phase 0B production evidence。BUG-599/600/604/614 与 `LEGACY_B0` 的既有业务路径不属于 P1-D 改动范围。

## P1-E 只读核验

按 binding/date 生成 Phase 0B v2 export 时，只允许显式版本 `miniqmt_execution_tca_evidence_v2`。查询必须在一个 TCA-owned read snapshot 内先解析有限 parent/runtime IDs，再读取对应 runtime journal；不得全库 JSONB 扫描。v1 默认输出保持不变。

核验顺序固定如下：

1. `TCA_PARENT.lineage` 的 `binding_id/binding_hash/runtime_id` 与 `PARENT_ASSIGNMENT` 一一对应，且同 parent 只有一个 `control_revision/revision_id/assignment_id`。
2. `CONTROL_REVISION` 的 policy/adapter/code/schema hashes 与 assignment、`ACTION_INPUT`、`CHILD_RECEIPT`、markout 完全一致；任一 hash set 不唯一即 `quote_control_complete=false`。
3. `ACTION_EVENT.action_evidence_id/action_market_data_id` 必须命中 durable `ACTION_INPUT`；它之后只能出现同 deterministic action 对应的一个 `CHILD_EVENT`。
4. `CHILD_RECEIPT.source_child_event_id` 反向命中 child event，并保留 action evidence、anchor market data 和 receipt 自身 market data；append-only child event 不回填 receipt ID。
5. 每个 authoritative trade anchor 必须有 60/300/900 秒 terminal markout；captured 与 unavailable 都是明确结果，缺 horizon 不是零收益或成功。
6. 查看 manifest 的 `missing_link_count/duplicate_child_count/revision_conflict_count/hash_conflict_count/identity_conflict_count`、五档/age/cadence/markout coverage；`quote_control_complete=true` 才表示该 export 的链路完整，不表示 ingress 已启用或真实 SIM 已运行。

pending action 自动恢复只读取原 `action_evidence_candidate`、durable receipt、deterministic child 与 broker/reconcile fact。诊断或 runbook 不调用 submit/cancel、不重放 action、不修改 runtime event。parity violation 使对应 revision 停止接纳新 parent且不切回 LEGACY；assignment/persistence fault 只影响对应 runtime/symbol，均不需要人工 acknowledge、approval 或 RBAC。
