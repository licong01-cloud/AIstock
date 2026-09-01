# Advisory P0-D Historical Forward Replay F2 详细设计

> 状态：`IMPLEMENTED_VALIDATED_READY_FOR_REVIEW`
> 日期：2026-08-23
> 父蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` P0-D/H0
> 功能等级：F2

## 1. Background / 背景

P0-D 已完成 exact meta-label bundle、生产 challenger 推理、自然 observation、同 policy 成熟结算和独立指标 API/UI。自然 future OOS 必须按真实交易日积累，但它不能成为每次代码修复、模型替换或特征演进的二十交易日阻塞门槛。

现有 Historical Range 已冻结 2026-05-15 至 2026-07-16 的 44 个连续决策交易日。P0-D exact bundle 所绑定 policy-dataset 冻结请求的 `data_cutoff` 为 2026-03-10，因此在模型和阈值未读取该窗口结果的前提下，这段数据可以作为历史 out-of-time 虚拟前向验证。历史结果不能写入生产 forward observation/evaluation 表，也不能冒充自然 future OOS。

本功能在历史 artifact 和显式 as-of 数据之上复用正式 P0-D 推理与 `replay_shadow_portfolio()`，用虚拟交易时钟快速完成候选重排、持仓继承、退出、成本和指标验证。它不建设第二套回测业务逻辑。

## 2. Scope / 范围

- 读取显式指定的 Historical Range candidate artifacts，逐日恢复 Selection Top40 与 Top20 候选身份。
- 加载显式指定的 P0-D descriptor、exact bundle 和冻结 shadow/cost policy；禁止扫描 latest。
- 每个决策日只读取不晚于该日的正式特征输入，生成 Top20 `entry_priority_rank`。
- 用决策窗口加后续 rank-context tail 构造虚拟时钟；只有到达冻结 maturity watermark 后才结算对应 observation。
- 复用 `replay_shadow_portfolio()` 与正式 P0-D policy/cost，输出完成 episode 胜率、净收益、超额收益、最大回撤、换手和覆盖率。
- 结果写入独立 repo-external content-addressed artifact；exact retry 返回同一结果，身份变化产生新 artifact 或显式冲突。
- 明确输出 `HISTORICAL_OUT_OF_TIME`、`HISTORICAL_REPLAY` 或 `NATURAL_FORWARD` 之外的证据分类；本功能绝不产生 `NATURAL_FORWARD`。
- 支持代表日/小窗口与完整窗口两种批量运行，不改变实盘单日调度。

允许修改：

- `backend/services/advisory_model_first/historical_forward_replay.py`（新增）
- `backend/services/advisory_model_first/meta_label_bundle.py`
- `backend/services/advisory_model_first/model_inference.py`
- `backend/services/advisory_forward/evaluation.py`
- `backend/services/advisory_historical_range/model_challenger.py`
- `backend/services/advisory_historical_range/fullstack_comparison.py`
- `backend/services/advisory_historical_range/wsl_model_scorer.py`
- `scripts/advisory_p0d_historical_forward_replay.py`（新增）
- `backend/tests/advisory_model_first/test_historical_forward_replay.py`（新增）
- `backend/tests/advisory_historical_range/test_model_challenger.py`
- `backend/tests/advisory_historical_range/test_wsl_model_scorer.py`
- `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`（新增）
- `tests/aistock_validation/catalog/file_ownership.yaml`（仅登记 CLI 与直接测试归属）
- 本设计与父蓝图当前状态

## 3. Non-goals / 非目标

- 不向 `app.advisory_forward_*` 生产表写 observation、evaluation 或 outcome。
- 不把历史回放结果标记为自然 future OOS，不与生产 API 的自然样本数相加。
- 不使用固定 1/3/5/20 日单股收益替代冻结 Top5 shadow policy。
- 不重新训练、调参、自动激活模型，不创建通用 ModelOps、缓存或调度平台。
- 不回填 descriptor 接入前生产日期，不改变 Selection、Program Top20、Paper、模拟盘或 QMT。
- 不因尾部 rank context 不足而假定退出；尾部 observation 保持显式 censored/unresolved。

## 4. Architecture / 架构

```text
explicit Historical Range candidate refs
  -> exact Top40 rankings + Top20 Selection candidate group
  -> exact P0-D descriptor/bundle loader
  -> production-equivalent feature builder/scorer at decision cutoff
  -> per-day entry_priority_rank artifact

decision days + rank-context tail + explicit bounded market data
  -> HistoricalForwardReplayRequestV1
  -> replay_shadow_portfolio (shared production policy kernel)
  -> daily portfolio + completed/active episodes + metrics
  -> HistoricalForwardReplayArtifactV1 (repo-external only)
```

历史 orchestrator 只负责组装输入和推进虚拟时钟。候选特征、模型评分、rank 合同、组合 transition、交易成本和 benchmark 计算必须调用现有正式实现。

## 5. Contracts / 合同

### 5.1 Request identity

`HistoricalForwardReplayRequestV1` 至少绑定：

- schema/producer version；
- parent range run、每个 candidate artifact semantic hash；
- Program/package/descriptor/bundle/manifest；
- shadow policy、cost policy 及 SHA256；
- ordered decision dates、ordered context dates、explicit as-of watermark；
- exact policy-dataset request identity 与冻结 data cutoff；
- market input SHA256、code implementation SHA256。

任何身份变化必须改变 request/artifact hash；不得覆盖既有 artifact。

### 5.2 Day input

每个 context day 必须包含连续 Selection Top40，rank 为 1..40 且 symbol 唯一。每个 decision day 必须另有完整 Top20 P0-D priority，rank 为 1..20 且与 Top40 前20同一 symbol set。tail context 只服务已有持仓退出，不产生新入场。

### 5.3 PIT and virtual clock

- 特征源每次调用绑定 `decision_as_of_trade_date=D`，不得看到 D 后数据。
- 市场 outcome 数据允许一次批量读取到显式 replay watermark，但在进入业务内核前按 watermark 切片。
- observation 只有在其 maturity 不晚于 replay watermark 时进入 due 集合。
- future-poison 行、未来 rank context 或未来 priority 不能改变较早 watermark 的 artifact hash 和指标。

### 5.4 Evidence classification

- 全部 decision dates 严格晚于 exact bundle 所绑定 policy-dataset request 的 `data_cutoff`，且用户未用该窗口调参时，标记 `HISTORICAL_OUT_OF_TIME`。cutoff 禁止由操作者手填。
- 其他合法历史窗口标记 `HISTORICAL_REPLAY`。
- 本功能禁止输出 `NATURAL_FORWARD`。
- 每份报告必须同时显示模型训练截止、决策窗口、tail watermark 和分类理由。

### 5.5 Metrics

指标直接取自 shared policy replay，并至少包含：

- completed episode count/hit rate；
- mean/median net return bps；
- mean daily net/excess return；
- cumulative net/excess return；
- maximum drawdown；
- average turnover；
- decision observation count、due count、resolved count、coverage；
- active/censored episode count。

零 completed episode 时胜率为 `null`，禁止显示 0%。

### 5.6 Persistence isolation

artifact store 只接受绝对 repo-external root，路径必须位于该 root 下。发布使用不可变 create/link/readback；同 hash 不同内容 fail closed。模块不得导入生产 forward repository 写接口。

## 6. CLI / 操作合同

CLI 必须显式接收 parent range run、historical artifact root、model root、Program、binding version、window usage 和 output root。descriptor、bundle、policy、cost、policy-dataset data cutoff、决策日、tail 与 replay watermark 必须从这些显式身份和不可变父 artifact 精确解析；允许 decision start/end 缩小窗口，但不得扫描 latest、猜测 Program、手填训练截止日或指定一个与 binding 不一致的 bundle。

模式：

- `validate-inputs`：只读验证日期、artifact、descriptor、bundle、policy 和市场覆盖。
- `run`：逐日评分并写独立 artifact/state，可 exact resume。
- `report`：读取已完成 artifact 输出 JSON/Markdown 摘要，不重新评分。

## 7. Failure Semantics / 失败语义

- Top40/Top20、日期连续性、descriptor/bundle/policy、特征、市场或 benchmark 缺失均 typed fail closed。
- 单日评分失败停止后续决策日；已完成 immutable day artifact 保留，可修复后 exact resume。
- context tail 不足时拒绝启动，不伪造退出；完整 tail 后仍未退出的 episode 才报告 active/censored，并降低 coverage。
- production database 只允许只读 market/feature 查询；发现写事务或生产 forward repository 依赖时测试失败。

## 8. Verification Plan / 验证方案

- 合同：Top40、Top20、日期连续、tail-only、policy/cost/hash、证据分类。
- 同核：相同 fixture 的历史 replay 与直接 `replay_shadow_portfolio()` daily/episodes/metrics 完全一致。
- 防泄漏：D 日 scorer 只收到 D cutoff；watermark 后 market/rank/priority poison 不改变结果。
- 隔离：禁止导入/调用生产 forward persistence；artifact root escape 与 overwrite 失败。
- 幂等：相同 request exact retry 返回同 hash；descriptor/bundle/market/code变化改变 identity。
- 小窗口：代表日至少覆盖入场、持仓、替换预算、rank exit、stop、time stop、无入场和 active/censored。
- 真实窗口：运行最大可完整成熟的决策前缀加20日 tail；44日父窗口中后20日只作为 rank-context，不伪造成新的成熟决策 observation。报告性能，不把收益阈值设为功能成功门禁。

## 9. Risks / 风险

| 风险 | 控制 |
|---|---|
| 历史结果冒充自然OOS | 类型与存储物理隔离，报告强制显示证据分类 |
| 批量frame泄露未来数据 | scorer逐日cutoff，replay按watermark切片，future-poison测试 |
| 重写简化收益逻辑 | 强制调用shared policy replay，禁止固定持有期替代 |
| 44日尾部无法成熟 | decision窗口与rank-context tail分离，尾部显式censored |
| 反复使用同一窗口调参 | artifact记录使用分类；一旦用于选择，后续降级为HISTORICAL_REPLAY |
| 历史任务工程化扩张 | 只实现P0-D直接验证所需适配器、artifact和CLI |

## 10. Design Acceptance Index

| ID | requirement |
|---|---|
| F-901 | exact Historical Range Top40/Top20 输入并逐日 PIT P0-D 评分 |
| F-902 | decision window、rank-context tail 与 virtual maturity watermark 分离 |
| F-903 | 复用正式 meta-label scorer 和 shared shadow policy，不实现简化收益 |
| F-904 | future-poison 不改变较早 watermark 结果 |
| F-905 | 历史 artifact 与生产 forward persistence 物理和代码隔离 |
| F-906 | exact descriptor/bundle/policy/cost/market/code identity 与幂等 artifact |
| F-907 | 输出完整 episode、收益、回撤、换手、coverage 与 censored 指标 |
| F-908 | HISTORICAL_OUT_OF_TIME/HISTORICAL_REPLAY 分类明确且禁止 NATURAL_FORWARD |
| F-909 | 代表日、小窗口和真实成熟窗口可执行，失败可见并 exact resume |
| F-910 | 单日与批量同业务内核，实盘调度和生产表零变化 |
| F-911 | Feature Workflow、定向测试和 DESIGN-COMPLIANCE-001 审核通过 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-901 | `backend/services/advisory_historical_range/model_challenger.py`; `scripts/advisory_p0d_historical_forward_replay.py` | `backend/tests/advisory_historical_range/test_model_challenger.py`; 24/24 real day artifacts under `F:/Dev/AIstock_model_artifacts/advisory_p0d_historical_forward_replay_20260823/day-artifacts` | pass | none |
| F-902 | `backend/services/advisory_model_first/historical_forward_replay.py`; `scripts/advisory_p0d_historical_forward_replay.py` | `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`; real 24 decision + 20 tail | pass | none |
| F-903 | `backend/services/advisory_model_first/meta_label_bundle.py`; `backend/services/advisory_model_first/model_inference.py`; `backend/services/advisory_model_first/historical_forward_replay.py` | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; report `F:/Dev/AIstock_model_artifacts/advisory_p0d_historical_forward_replay_20260823/report_fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9.md` | pass | none |
| F-904 | `backend/services/advisory_model_first/historical_forward_replay.py` bounded market source and `_bounded_frame` | `backend/tests/advisory_model_first/test_historical_forward_replay.py` future-poison test | pass | none |
| F-905 | `backend/services/advisory_model_first/historical_forward_replay.py:HistoricalForwardReplayArtifactStore`; repo-external CLI output | `backend/tests/advisory_model_first/test_historical_forward_replay.py` immutable store test | pass | none |
| F-906 | request/artifact hash validators; scorer code identity; canonical publish-readback | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`; final real hash `fbf072f0...` repeated exactly | pass | none |
| F-907 | `backend/services/advisory_model_first/historical_forward_replay.py:HistoricalForwardReplayArtifactV1` | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; artifact `F:/Dev/AIstock_model_artifacts/advisory_p0d_historical_forward_replay_20260823/p0d-historical-forward/fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9.json` | pass | none |
| F-908 | request evidence classification; `scripts/advisory_p0d_historical_forward_replay.py` report renderer | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`; real artifact classification `HISTORICAL_OUT_OF_TIME` | pass | none |
| F-909 | CLI validate/run/report and resumable day state | `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`; 24/24 real run and 4-day cold-start exact-resume probe | pass | none |
| F-910 | shared `replay_shadow_portfolio`; `backend/services/advisory_forward/evaluation.py` row builder | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; `backend/tests/advisory_model_first/test_forward_model_evaluation.py` | pass | none |
| F-911 | F2 design, targeted gates and compliance review | `backend/tests/scripts/test_advisory_p0d_historical_forward_replay.py`; feature validator PASS; targeted pytest `61 passed, 2 skipped`; L0/L2/catalog validation、Ruff 与 diff-check pass | pass | none |

## 12. Implementation Plan / 实施方案

1. 建立纯计算 request/day/artifact 合同和 shared-policy replay builder。
2. 扩展 Historical Model Challenger，使其按显式 model role 加载 exact P0-D bundle并复用正式 scorer。
3. 建立独立 content-addressed artifact store 与 CLI/state exact resume。
4. 完成 fixture、未来毒化、幂等、隔离、tail/censoring 和同核测试。
5. 用冻结 v6 输入运行最大可成熟历史子窗口，输出第一份 P0-D 历史 out-of-time 报告。
6. 重复设计与代码审核，修复至全部验收项 pass 后创建 PR。

## 13. Real Validation Result / 真实验证结果

权威 44 日父窗口为 `2026-05-15..2026-07-16`。按冻结 policy 的20交易日 maturity 要求，最大可成熟子集为24个决策日 `2026-05-15..2026-06-17`，其后20日仅作持仓退出所需 rank-context；最终 market watermark 为 `2026-07-17`。24/24 observation 已结算，coverage 为100%。

权威 artifact：`fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9`。

| 指标 | P0-D | matched Selection Top5 | P0-D lift/difference |
|---|---:|---:|---:|
| completed episode | 30 | 26 | +4 |
| hit rate | 36.67% | 26.92% | +9.74pp |
| mean episode net return | -352.59 bps | -345.35 bps | -7.24 bps |
| mean daily net return | -69.46 bps | -59.17 bps | -10.30 bps |
| cumulative net return | -19.45% | -16.90% | -2.54pp |
| maximum drawdown | -22.54% | -16.90% | -5.63pp |
| mean turnover | 40.00% | 34.67% | +5.33pp |

结论：历史回放功能与胜率统计正常，P0-D 提高了命中率，但收益幅度、累计收益、回撤和换手均劣于同策略 Selection Top5，不能据此激活。该窗口在本次结果用于模型判断后已被消费；未来若据此修改特征、阈值或模型，重跑必须分类为 `HISTORICAL_REPLAY`，不得继续宣称新的历史 OOT。

## 14. Rollout / Rollback

- Rollout：仅合入源码、测试、设计和 CLI；默认不运行，不新增 scheduler、API、数据库对象或生产 binding。操作者显式提供冻结身份后，产物只写绝对 repo-external root。
- Runtime activation：无。合入后不要求后端重启；如未来把 CLI 接入服务或调度，必须另立设计和授权。
- Rollback：回退本 PR 源码即可移除入口；既有 content-addressed 历史 artifact 保持只读，不影响生产 forward 事实。是否清理实验 root 必须单独授权。
- Data rollback：无 DDL/DML，无生产数据回滚动作。

## 15. Production Gates

| 动作 | 状态 | 授权 |
|---|---|---|
| 源码、测试、repo-external历史artifact | implementation authorized | 当前任务 |
| DEV/production DDL | none | 不需要 |
| production DML | prohibited | 不需要 |
| 后端重启 | none | 不需要 |
| 模型激活/descriptor切换 | prohibited | 不属于本功能 |
| PR合入 | pending user authorization | 实现审核后 |

## 16. Completion Definition

- F-901 至 F-911 均有直接实现与测试证据，无 pending gap。
- 真实历史窗口至少产生一份可读取、可 exact retry 的 P0-D artifact 和报告。
- 报告明确区分历史 out-of-time 与自然 future OOS；tail完整且 observation 100% resolved，未伪造 censored 样本。
- 无生产 DDL/DML、forward 表写入、模型激活或后端重启。
- DESIGN-COMPLIANCE-001 四项逐项通过：无简化/子集/POC、无静默错误或伪成功、无业务逻辑漂移、无未经确认的门禁或人工审批。
