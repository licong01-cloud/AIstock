# Paper Trading v2 基线对齐报告

> 日期：2026-04-27  
> 目的：核对 `docs/architecture/paper_trading_v2_gap_closure_detailed_design_20260427.md` 与当前代码、历史设计文档的匹配关系，明确哪些内容已经实现、哪些是下一阶段设计、哪些旧文档语义被新设计覆盖。  
> 约束：本次只读检查和文档对齐，不修改资产、不重启 8001、不调整业务代码。

## 1. 对齐结论

`docs/architecture/paper_trading_v2_gap_closure_detailed_design_20260427.md` 与当前代码基础总体相符，但必须按“已实现基线 + 下一阶段缺口”理解：

- 文档中列为“已实现基线”的 StrategyPackage、Selection Center、Paper v2 持久化、session 框架、V25 core/adapter、Paper v2 UI 入口，当前代码均存在对应实现。
- 文档中列为“建议新增/补充表”的 `paper_v2.runtime_profile`、`paper_v2.runtime_profile_version`、`paper_v2.runtime_config_activation`、`paper_v2.config_change_audit` 当前尚未实现，是下一阶段开发目标。
- V25 当前已有 core、Paper adapter、能力声明和测试；但 Paper v2 真实 V25 回放/实时运行仍受 `day_features` 提供器缺口约束。当前代码要求 `market_context.day_features`，但 Paper v2 market data provider 尚未生成该字段。
- UI 当前已有策略包、选股、组合、运行控制台、账本、绩效、模型/HMM 页面；但 `sessionCapabilities()` 只在 API client 中存在，页面尚未使用它来禁用不支持的模式，这是设计文档 Phase 3 的正确缺口。
- 早期文档中“`minute_execution_policy` 是策略包固定内容”的说法仍与 manifest v1 代码兼容；最新设计将其解释为 QE backtest default / lineage。当前 Paper v2 实际执行策略已经通过 `validated_execution_policy` 和 `paper_v2.execution_policy_activation` 解析，不允许 runtime_config 直接覆盖算法。

因此，新设计方案不是把未实现内容误认为已完成，而是正确区分了当前实现和下一阶段开发缺口。需要在后续开发中继续保持这种边界。

## 2. 当前程序实现基线

### 2.1 StrategyPackage Center

代码入口：

- `backend/routers/strategy_packages.py`
- `backend/services/strategy_package/service.py`
- `backend/services/strategy_package/repository.py`
- `backend/services/strategy_package/qe_source_resolver.py`
- `backend/services/strategy_package/selection_artifact.py`
- `backend/services/strategy_package/execution_policy.py`
- `backend/services/strategy_package/model_state.py`

当前已实现：

- 从 QE 单次实验和 QE evolution loop 创建 StrategyPackage。
- frozen manifest JSON + `manifest_sha256` 持久化。
- package status 流转和 status events。
- 禁止静默替换 manifest。
- QE source 下拉只列未打包来源，并包含年化、IC、RankIC、最大回撤等展示指标。
- `strategy_pkg.validated_execution_policy`：保存 backtest-validated minute execution policy，支持 enable/disable paper。
- `strategy_pkg.model_state` 和 `strategy_pkg.model_retrain_job`：模型 stale 提示与人工确认重训 job skeleton。
- `strategy_pkg.selection_score_artifact`：权威 live/latest-data selection artifact；diagnostic backtest artifact 与权威 runtime 分离。

与新设计关系：

- 符合“策略包冻结研究 alpha 资产”的基础要求。
- 仍保留 manifest v1 的 `minute_execution_policy` 字段；当前 Paper v2 会在缺少显式 policy 时导入/创建 `manifest_default_execution_policy`。这与新设计兼容，但后续应在 API/UI 上明确它是 source backtest default，不是不可变运行配置。

### 2.2 Selection Center

代码入口：

- `backend/routers/selection_center.py`
- `backend/services/selection_center/service.py`
- `backend/services/selection_center/runtime_profile.py`
- `backend/services/selection_center/tradability.py`
- `backend/services/selection_center/hmm_runtime.py`
- `backend/services/selection_center/industry_provider.py`

当前已实现：

- `single_package`、`intersection`、`union`、`weighted_fusion`。
- `runtime_profile` 规范化，支持 TopK、停牌过滤、行业黑名单、HMM。
- TopK 后端约束为 1-50。
- `suspend_d` 停牌过滤、行业黑名单过滤均支持补位和 excluded trace。
- HMM 启用时要求 snapshot + preset + coefficients，不做中性系数兜底。
- Selection artifact 可按 `selection_artifact_config.auto_generate=true` 自动生成权威 live inference artifact。
- 已有 selection run 可再次聚合。
- 单策略包 selection run 可创建 Paper v2 portfolio；多策略包 selection-to-paper 明确 fail-fast。
- 选股结果支持加入自选池，要求 reference price，保留 source trace。

与新设计关系：

- 当前实现已经支持“多策略包优先用于选股研究，暂不直接进入 Paper v2 执行”。
- 当前 `selection_artifact_runtime_hash()` 只 hash 分数生产配置，不把 TopK、HMM、黑名单、停牌过滤纳入 artifact lookup hash；这与“原始分数 artifact 与运行筛选配置分离”一致。
- 缺口是 runtime profile 还只是 run-level JSON 快照，没有一等版本表和 activation/audit 表。

### 2.3 Paper Trading v2 持久化与日级运行

代码入口：

- `backend/routers/paper_trading_v2.py`
- `backend/services/paper_trading_v2/service.py`
- `backend/services/paper_trading_v2/day_runner.py`
- `backend/services/paper_trading_v2/readiness.py`
- `backend/services/paper_trading_v2/replay.py`
- `backend/services/paper_trading_v2/repository.py`
- `backend/services/paper_trading_v2/market_data.py`
- `backend/migrations/trading_core_v2_schema.sql`
- `backend/db/init_trading_core_v2_schema.py`

当前已实现：

- `paper_v2.portfolio` 冻结 package、manifest hash、initial cash、start date、data source、fee/risk/execution policy snapshot。
- `paper_v2.run`、orders、order events、fills、cash ledger、positions、daily snapshots、run events、errors。
- `paper_v2.execution_policy_activation`：支持按 trade_date 激活 backtest-validated policy。
- readiness、run-day、historical replay、reset replay。
- reset 需要确认文本匹配 portfolio_id，写入 `paper_v2.reset_audit`。
- market data provider 明确支持 `DB_HISTORICAL` 和 `TDX_REALTIME`，无静默 source fallback。
- 缺交易日历、分钟线、pre_close、limit、suspend 状态等会 fail-fast。

与新设计关系：

- 当前已经具备 Paper v2 核心账本和可追溯基础。
- 当前 `runtime_config` 会写入 run/session，但不是 profile version/activation 形式；这正是新设计 Phase 2 的补齐目标。
- 当前 Paper service 仍可基于 manifest minute policy 自动创建 default validated policy；后续应确保 UI/文档明确其来源，不把它解释为 manifest 运行时锁定。

### 2.4 Paper v2 会话、实时与追赶模式

代码入口：

- `backend/services/paper_trading_v2/session.py`
- `backend/services/paper_trading_v2/live_session.py`
- `backend/services/paper_trading_v2/scheduler.py`
- `backend/services/paper_trading_v2/market_data.py`

当前已实现：

- `REPLAY_ONLY`、`LIVE_ONLY`、`CATCHUP_THEN_LIVE` session model/API。
- `auto_switch_to_live=true` 会把 replay request 规范化成 `CATCHUP_THEN_LIVE`。
- source roles：historical 只允许 `DB_HISTORICAL`，live 只允许 `TDX_REALTIME`。
- session lifecycle：create/list/detail/progress/tick/pause/resume/stop。
- live tick 通过 `order_execution_state` 增量处理，重复 tick 防重复成交。
- 无新分钟线时进入 `LIVE_WAITING_FOR_BAR`，不是成功也不是失败。
- session scheduler 调用同一个 tick path，默认非自动启动。

与新设计关系：

- 与实时/回放 session 设计基本一致。
- 仍需 UI 使用 `session-capabilities` 做按钮禁用与原因展示。
- 仍需用真实数据对 `CATCHUP_THEN_LIVE`、V25 live、账本收益做端到端验证。

### 2.5 V25 / Trading Core 分钟执行

代码入口：

- `backend/execution_algos/v25_core.py`
- `backend/execution_algos/v25_two_stage_algo.py`
- `backend/services/trading_core/execution_algo_capabilities.py`
- `backend/services/trading_core/execution_algo_adapter.py`
- `backend/services/trading_core/minute_execution.py`
- `backend/tests/trading_core/test_v25_execution_contract.py`

当前已实现：

- V25 core 独立于 Paper/QE/DB/API 对象。
- capability 已拆分 historical/live：V25 historical 240 bars；live supported；live min start bars 1；plan horizon 240。
- V25 adapter 区分市场业务状态和数据/资产错误。
- V25 可在 one observed bar realtime 场景生成/持久化 plan。
- `MinuteExecutionEngine` 支持 incremental execution 和 market-aware `NO_FILL` 事件。
- 缺 `day_features` 时 fail-fast；仅 `allow_default_day_features` 可作为显式诊断开关，但不能进入权威 Paper v2。

当前关键缺口：

- Paper v2 market data provider 当前没有生成 `market_context.day_features`。因此 V25 code path 已具备，但真实 Paper v2 V25 replay/live 仍需 `V25DayFeatureProvider` 才能权威运行。

### 2.6 Paper v2 UI

代码入口：

- `frontend/src/app/paper-v2/*`
- `frontend/src/lib/paper-v2/api.ts`
- `frontend/src/lib/paper-v2/types.ts`
- `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`
- `frontend/src/app/Sidebar.tsx`

当前已实现：

- `/paper-v2` 独立路由和 Sidebar 导航。
- 策略包中心：QE source 下拉、指标展示、创建 package、status 操作、policy/model state 展示。
- 选股中心：单包/多包、TopK 20 默认/50 上限、停牌过滤、行业黑名单、HMM 下拉、历史 run 展示、加入自选池、多 run 聚合。
- 组合中心：单策略包创建组合，配置初始资金、runtime selection config、HMM/黑名单/停牌过滤、validated policy、replay/live/auto-switch。
- 运行控制台：readiness、run-day、replay/reset、live session、scheduler、policy activation、events/errors。
- 账本/绩效页面：orders/fills/cash/positions/snapshots/performance。

当前缺口：

- `paperV2Api.sessionCapabilities()` 已存在，但页面尚未调用它来禁用不支持的 session mode/source/algo 组合。
- UI 目前仍把 runtime config 作为 JSON/runtime payload 传递，尚未接入一等 runtime profile version/activation/audit。

## 3. 历史文档与最新设计的语义关系

| 文档 | 当前状态 | 与最新设计关系 |
| --- | --- | --- |
| `docs/contracts/strategy_package_manifest_v1.md` | 仍是 manifest v1 合同；要求 `minute_execution_policy` | 作为历史兼容合同保留。最新设计将该字段解释为 QE backtest default / lineage，Paper v2 当前执行策略由 validated policy 决定。 |
| `docs/adr/0001-ai-stock-trading-core-direction.md` | 早期架构 ADR；A2 写明分钟执行策略是策略包一部分 | 不直接删除。新设计是后续修正：策略包冻结研究 alpha 资产，运行策略由 Paper v2 runtime/validated policy 动态选择。 |
| `docs/architecture/trading_core_v2.md` | Trading Core v2 基础架构 | 仍有效；其中 `minute_execution_policy` 语义需按最新 gap closure 设计兼容解释。 |
| `docs/architecture/paper_trading_v2_top_level_design.md` | 顶层设计 | 主流程、OMS/Ledger/fail-fast 仍有效；策略包冻结边界由最新设计修正。 |
| `docs/architecture/paper_trading_v2_runtime_profile_execution_policy_design.md` | runtime profile/validated policy 设计 | 与最新设计方向一致；其中多项已经实现，但 runtime profile 一等版本/audit 表尚未实现。 |
| `docs/architecture/paper_trading_v2_realtime_replay_session_design.md` | replay/live/catchup session 设计 | 大部分已经落地；早期“live 未完成”的记忆已被后续 V25 core/session 实现更新覆盖。 |
| `docs/architecture/minute_execution_algo_standard_contract.md` | 日内分钟策略开发规范 | 作为 V25 和后续 V26/其他算法的权威实现规范继续有效。 |
| `docs/architecture/paper_trading_v2_gap_closure_detailed_design_20260427.md` | 最新下一阶段设计 | 与当前代码相符：已实现项均有代码基础，未实现项明确列为 Phase 1-5 后续开发。 |

## 4. 新设计与代码的一致性检查

| 新设计要求 | 当前代码状态 | 对齐结论 |
| --- | --- | --- |
| StrategyPackage 不因 status/runtime config 改 hash | package/status event 分离，测试覆盖 manifest 替换拒绝 | 已对齐 |
| HMM、黑名单、TopK、停牌过滤不锁定到 manifest | Selection/Paper runtime_config 支持这些项；UI 明确“运行时配置” | 已对齐 |
| 执行策略必须 backtest validated | `strategy_pkg.validated_execution_policy` + Paper policy activation；raw override 被拒绝 | 已对齐 |
| 多策略包优先用于选股，不能直接 Paper 执行 | service 和 UI 均明确阻断 | 已对齐 |
| replay reset 可执行但必须确认和审计 | `reset_portfolio` + `paper_v2.reset_audit` | 已对齐 |
| V25 live 不要求开盘已有 240 根 | capability/test 支持 live 1 bar + persisted plan | 代码已对齐，但真实运行仍缺 day_features provider |
| `day_features` 不可默认兜底 | V25 adapter 缺 day_features fail-fast | 已对齐；provider 待实现 |
| runtime profile version/audit | 当前只有 JSON 快照，没有 profile/version/activation/audit 表 | 未实现，正是下一阶段 Phase 2 |
| UI 必须使用 session capability 禁用 unsupported mode | API client 存在，页面未使用 | 未实现，正是下一阶段 Phase 3 |
| 后台 UI 全流程验证 | Playwright spec 存在，但最新 V25 value validation 仍需补充 | 部分实现，需下一阶段验证 |

## 5. 本次基线验证

执行命令：

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
```

结果：

```text
125 passed in 6.97s
```

未启动或重启 8001；未修改资产；未运行会写入数据库资产的脚本。

## 6. 下一阶段开发边界

后续应按最新设计继续开发，但先不改资产：

1. 先补 runtime profile/version/activation/audit framework，不修改 manifest、validated policy、model/HMM assets。
2. 补 UI 与 backend capability 对齐：页面必须调用 `session-capabilities`，不再只依赖后端失败后展示错误。
3. 补 `V25DayFeatureProvider`，必须复用/对齐 QE V25 特征语义，不能用默认值或零填充。
4. 用 8011/8012 + 3011/3012 做 UI 后台测试，不重启 8001。
5. 对真实 Paper v2 V25 回放/实时收益验证单独建测试组合和测试资产；如果需要修改 DB asset row、模型权重、HMM snapshot、validated policy，必须先做影响评估并单独确认。
