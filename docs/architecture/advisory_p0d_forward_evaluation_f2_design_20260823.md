# Advisory P0-D Forward Evaluation F2 详细设计

> Feature tier: F2
> 状态：`SOURCE_AND_DEV_VALIDATED_MERGE_READY_AWAITING_USER`
> 日期：2026-08-23
> 父蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` P0-A/P0-D

## 1. Background / 背景

P0-D 已完成真实 meta-label bundle、精确 descriptor、在线 Top20 入场优先级重排和每日 challenger observation。现有 observation 冻结 `maturity_trade_date`，但到期后没有消费者按训练时的 Top5 shadow policy 形成真实 outcome/episode label，也没有独立于 baseline Program Episode 的模型胜率、收益、回撤、换手和覆盖率指标。

直接把每日 Top5 当作五只独立股票并计算固定20日收益会改变已经批准的业务口径：训练评价包含持仓继承、每日替换预算、Selection exit rank、止盈止损、time stop、交易成本和 benchmark。因此本功能必须复用同一 `AdvisoryListTransitionEngine` 与冻结 policy，不能交付简化收益统计。

## 2. Scope / 范围

- 每次 P0-D forward observation 冻结完整 `shadow_policy`、`cost_policy` 及各自 SHA256；只保存决策时已知合同，不保存未来行情。
- 结算候选只包括 `EXPERIMENTAL_SHADOW + meta_label_take_skip_confidence` 且 `maturity_trade_date <= as_of_trade_date` 的 observation。
- 复用每日 persisted Selection Run 的连续 Top40 作为退出排名上下文；P0-D observation 的 Top20 `entry_priority_rank` 仅决定新入场顺序。
- 按相同 program、descriptor、bundle、shadow policy、cost policy 分隔连续 challenger epoch；descriptor 切换后启动新 epoch，不继承旧模型持仓。
- 只查询 epoch 首个 target 至本次成熟水位日之间的权威交易日历、`market.kline_daily_raw`、`market.adj_factor`、`market.stk_limit`、`market.suspend_d` 与 `market.index_daily`，禁止读取水位日以后数据。
- 复用 `replay_shadow_portfolio()` 和 `AdvisoryListTransitionEngine` 生成 daily portfolio、episode 与聚合 metrics；连续日期或 Top40/Top20/市场证据缺失时 typed fail closed。
- 在 PostgreSQL 中写入不可变 evaluation snapshot 和逐 observation outcome；exact retry 返回既有事实，源数据或业务结果变化产生显式 conflict。
- API/UI 展示模型证据状态、成熟样本数、已退出 episode 数、hit rate、平均日净收益/超额收益、最大回撤、平均换手和覆盖率；零成熟样本不显示伪胜率。
- scheduler 每次 tick 对每个 Program 最多推进一个最新成熟水位；结算失败不阻断 baseline publish，也不自动训练或激活模型。

允许修改：

- `backend/services/advisory_model_first/meta_label_bundle.py`
- `backend/services/advisory_model_first/model_inference.py`
- `backend/services/advisory_forward/models.py`
- `backend/services/advisory_forward/evaluation.py`（新增）
- `backend/services/advisory_forward/repository.py`
- `backend/services/advisory_forward/service.py`
- `backend/routers/advisory.py`
- `backend/db/migrations/add_advisory_forward_model_evaluation_20260823.sql`
- `backend/db/migrations/add_advisory_forward_model_evaluation_20260823.rollback.sql`
- 对应 backend/frontend 定向测试
- `frontend/src/lib/api/advisory.ts`
- `frontend/src/app/paper-v2/advisory/page.tsx`
- 本设计与父蓝图当前状态

## 3. Non-goals / 非目标

- 不回填 descriptor 接入前的历史日期，不用冻结 test、Historical Range 或 baseline episode 冒充自然前向样本。
- 不改变 Selection 正式排序、Program Top20、正式 recommendation list、Paper/QMT/模拟盘或账户状态。
- 不自动重训、自动激活、创建通用 ModelOps/缓存/调度/证据平台。
- 不用固定持有期收益替代 shadow policy，不把 active/open-mark episode 纳入成熟胜率。
- 不扫描 latest bundle，不跨 descriptor、bundle、Program 或 policy 合并指标。
- 不自动执行 DEV/生产 DDL，不控制或重启后端；DDL、合入、生产迁移与重启分别报告。

## 4. Architecture / 架构

```text
P0-D forward observation (decision-time frozen only)
  + exact descriptor/bundle
  + shadow_policy + cost_policy hashes
  + Top20 entry_priority_rank
  + maturity_trade_date
                 |
                 | maturity <= explicit as_of
                 v
AdvisoryForwardModelEvaluationService
  |- load contiguous same-epoch observations
  |- load each persisted Selection Run Top40
  |- query DB market rows only through maturity watermark
  |- future-poison / date / identity / completeness guards
                 |
                 v
replay_shadow_portfolio (shared transition engine)
                 |
                 +--> immutable evaluation snapshot
                 +--> immutable per-observation mature outcome
                 +--> API/UI model-only metrics

baseline Program list / episode / metrics remain untouched
```

evaluation 每次从 epoch 起点确定性重放到最新成熟水位。该规模只包含自然前向天数与 Top40，不建立常驻缓存；结果由 observation roster、Selection payload、market input、calendar、policy、cost 和代码 schema 共同绑定。已写成功事实不可更新。

## 5. Contracts / 合同

### 5.1 Observation contract v2

P0-D `prediction_payload_json` 新增：

- `shadow_policy`: 冻结 `AdvisoryTransitionPolicyV1` 可验证 payload。
- `shadow_policy_sha256`: 必须等于该 payload canonical hash。
- `cost_policy`: 冻结 `AdvisoryPolicyCostV1` payload。
- `cost_policy_sha256`: 必须等于 cost policy canonical hash。
- `evaluation_contract_version=advisory_forward_model_evaluation_v1`。

旧 quality-reranker observation 不要求这些字段，也不会进入本结算器。缺少任一 P0-D evaluation identity 的新 observation 为 typed invalid，不使用默认成本或默认 policy。

### 5.2 Epoch and rank contract

- epoch identity：`program_id + model_descriptor_sha256 + bundle_id + shadow_policy_sha256 + cost_policy_sha256`。
- 每个 included observation 必须有一个 persisted Selection Run，且同一 decision/target、连续 Selection rank 1..40。
- observation candidates 必须为连续 Selection Top20 的一一映射，`entry_priority_rank` 为 1..20，`selection_exit_rank == selection_effective_rank`。
- `rankings` 使用 Selection Top40 的 rank/score；`entry_priorities` 只覆盖 Top20。已持仓股票跌出 Top20但仍在Top40时继续按 Selection exit rank 判定。
- 从 epoch 首日到 watermark 的目标交易日必须连续存在成功 observation；缺日、旧 descriptor、typed unavailable 或失败 observation 均阻断该水位，不跳日。

### 5.3 Market/PIT contract

- 查询参数明确绑定 `start_target_trade_date..maturity_watermark_trade_date`；repository/source API 不接受无 end date 调用。
- 股票价格使用 `kline_daily_raw + adj_factor` 的同口径前复权 open/high/low/close；涨跌停使用 `stk_limit`，停牌使用 `suspend_d`。
- benchmark instrument 来自冻结 cost policy，当前为 `000300.SH`，读取 `market.index_daily.open`。
- 交易日历只读取显式闭区间。所有 source rows 进入 canonical market-input hash；watermark 后新增或修改数据不得改变已有 evaluation。
- 必需价格、adj factor、benchmark、Top40 或日期连续性缺失返回 typed `WAITING_DATA/FAILED`，不能删股票、补零或沿用上一日价格。

### 5.4 Persistence

新增两张表：

1. `app.advisory_forward_model_evaluation`
   - 每个 epoch/last mature observation 一条不可变 snapshot。
   - 保存水位、成熟 observation 数、portfolio metrics、coverage、roster/market/result hash 和 compact daily/episode payload。
2. `app.advisory_forward_model_observation_outcome`
   - 每个 matured observation 一条不可变 label。
   - 保存该 observation 当日触发的 entered/exited episode、净收益、hit rate、coverage 与 exact evaluation snapshot。

insert 前锁定 observation；已有 payload hash 相同为 exact retry，不同则 conflict。不得 UPDATE 成功结果。表与 baseline `advisory_episode_return`、`advisory_program_metric_snapshot` 完全隔离。
两张事实表同时安装数据库级 `BEFORE UPDATE OR DELETE` 拒绝触发器；不可变性不只依赖 repository 调用约定。迁移 readback 必须校验表、列、索引、全部约束、触发器、函数与新列注释。

### 5.5 Metrics

- `completed_episode_hit_rate`：仅完整退出 episode 中 `net_return_bps > 0` 的比例。
- `mean_daily_net_return_bps` / `mean_daily_net_excess_return_bps`：同一 shadow portfolio 每日 open-to-open、含冻结买卖成本。
- `maximum_drawdown`：portfolio cumulative NAV 的最差回撤。
- `mean_turnover_fraction`：每日 ENTER+EXIT 除以 target_count。
- `coverage`：已产生不可变 outcome 的成熟 observation / 到期 observation；没有到期 observation 时状态为 `EVIDENCE_IMMATURE` 且指标为空。
- active episode 不进入 completed hit rate；`NO_ENTRY` 是真实 outcome，记录但不伪造0收益或胜负。

## 6. API / UI

新增：

```text
GET /api/v1/advisory/programs/{program_id}/forward-model-metrics
```

返回：`EVIDENCE_IMMATURE | READY | WAITING_DATA | FAILED`、epoch identity、first/last target、last maturity、due/matured observation counts、portfolio metrics 和最近失败 reason。现有 forward detail 增加该 observation 的 `model_outcome`（没有成熟结果时为 `null`）。
指标 API 只投影摘要列与 `metrics_json`，不得把累计 daily/episode replay evidence 返回给页面。

荐股中心“每日前向”区域展示独立的“模型前向效果”卡片。它不得使用 baseline leaderboard metrics；未成熟显示等待日期与样本数，不显示 `0%`。

## 7. Scheduler / Failure Semantics

- `run_once()` 在既有 target-open settlement 后、当日 publication 前推进模型 evaluation；pending 查询每 Program 最多返回一条最早未结 observation，每 Program/epoch 只处理最新可成熟水位一次，避免单一 Program 占满批次。
- evaluation `WAITING_DATA/FAILED` 作为独立结果加入 `results`，不把 baseline Program 加入 blocked set，也不阻断 publication/settlement。
- typed reason 至少包括：
  - `ADVISORY_FORWARD_MODEL_EVIDENCE_IMMATURE`
  - `ADVISORY_FORWARD_MODEL_EVALUATION_CONTRACT_INVALID`
  - `ADVISORY_FORWARD_MODEL_EVALUATION_SEQUENCE_INCOMPLETE`
  - `ADVISORY_FORWARD_MODEL_EVALUATION_MARKET_DATA_UNAVAILABLE`
  - `ADVISORY_FORWARD_MODEL_EVALUATION_IDENTITY_CONFLICT`
- 未知异常保留日志和结构化失败；不写空成功 snapshot。

## 8. Risks / 风险与控制

- **未来数据泄露**：所有行情、交易日历和 benchmark 查询都要求显式 `start..as_of` 闭区间；evaluation identity、market-input hash 与 future-poison 测试共同约束，水位后的行不得进入结果。
- **策略口径漂移**：不重新实现简化 Top5 收益统计；直接复用 `replay_shadow_portfolio()` / `AdvisoryListTransitionEngine`，并把 shadow/cost policy 及 hash 冻结到 observation。
- **descriptor 切换污染**：连续 epoch 绑定 descriptor、bundle、policy 和 cost identity；新 descriptor 不得向旧 epoch 注入新入场优先级，旧持仓只允许用后续 Selection Top40 完成退出。
- **部分成熟导致伪胜率**：仅完整退出 episode 进入 hit rate；active/censored observation 保持 `WAITING_DATA`，`NO_ENTRY` 只记录事实而不伪造收益。
- **不可变事实漂移**：evaluation/outcome 只允许 insert；exact retry 返回既有事实，roster、market 或 result payload 漂移均显式冲突。
- **运行期影响 baseline**：每个 Program 每 tick 最多推进一个水位；evaluation 失败独立可见，不进入 baseline blocked set。
- **迁移和激活次序**：DEV DDL 已独立验证；production DDL、PR 合入和后端重启仍分别等待用户授权，且 production DDL 必须先于新源码重启。

## 9. Verification Plan / 验证方案

- 合同：policy/cost/hash、epoch、Top40、Top20 priority、连续交易日、maturity/as-of 边界。
- 纯计算：同一 fixture 与离线 `replay_shadow_portfolio` 完全同结果；止盈、止损、rank exit、time stop、停牌/涨跌停和成本。
- 未来毒化：向 watermark 后插入任意价格/observation，不改变结果 hash。
- 幂等：exact retry不新增；市场/roster/result变化产生 conflict。
- DB repository：migration readback、insert/query、outcome unique、无 UPDATE 成功事实。
- 服务：未成熟不查询未来行情；成熟每 tick bounded；失败不阻断 baseline。
- API/UI：零成熟不显示伪胜率；READY展示真实模型指标；baseline metrics不混入。
- 真实 DEV：迁移经单独 DEV gate 后，用受控自然 forward rows执行 readback；当前无自然成熟 P0-D observation时只允许 `EVIDENCE_IMMATURE`。

## 10. Production Gates

| 动作 | 状态 | 授权 |
|---|---|---|
| 源码、migration 文件、测试、PR | 自动执行 | 已授权 |
| DEV DDL apply/readback | passed | 2026-08-23：显式 DEV migration apply/retry/rollback/reapply 通过；真实 DEV `stk_limit` 为空时验证 typed fail-closed，不伪造可交易性 |
| production DDL | 禁止自动执行 | 用户指定生产目标并授权 |
| 合入 PR | pending | 用户确认 |
| 后端重启 | pending | 用户执行 |
| 历史回填 | prohibited | 不属于本功能 |
| 自动模型训练/激活 | prohibited | 不属于本功能 |

## 11. Rollout / Rollback

1. 合入源码和 migration 不自动修改数据库或启动 scheduler。
2. DEV migration/readback通过后，由用户分别授权 production DDL、合入与重启。
3. 重启后先验证 runtime SHA、migration readback、API `EVIDENCE_IMMATURE` 和 baseline业务不变。
4. 首条自然 P0-D observation 到期后，scheduler 写第一条 immutable evaluation；读取API/UI与数据库hash。
5. rollback 停止新 evaluation 调用并回滚源码；只有 evaluation 表为空时才允许执行 rollback DDL，非空时 fail closed且保留事实。

## 12. Design Acceptance Index

| ID | requirement |
|---|---|
| F-801 | P0-D observation冻结 exact shadow policy、cost policy及hash，publication payload不含未来行情 |
| F-802 | 结算只处理 maturity不晚于显式as-of的meta-label observation，watermark后数据不能改变结果 |
| F-803 | persisted Selection Top40提供退出rank，P0-D Top20 priority只控制入场；每日序列必须连续完整 |
| F-804 | evaluator复用同一 transition/shadow portfolio policy，覆盖持仓继承、替换预算、止盈止损、rank/time exit和成本 |
| F-805 | 股票/benchmark/停牌/涨跌停/交易日历均来自显式闭区间数据库查询，缺失typed fail closed |
| F-806 | evaluation snapshot与per-observation outcome不可变、exact retry、identity冲突可见，且不污染baseline表 |
| F-807 | 指标只使用完整退出episode；零成熟、NO_ENTRY和active/censored状态不伪造胜率 |
| F-808 | scheduler bounded推进，evaluation失败不阻断baseline publication/target-open settlement |
| F-809 | API/UI分开展示P0-D模型效果与baseline Program metrics，不使用mock或旧模型结果 |
| F-810 | migration遵循DEV-first、production DDL/合入/重启分离授权，无历史回填或自动训练激活 |
| F-811 | F2 validator、定向测试、未来毒化、幂等、API/UI和DESIGN-COMPLIANCE-001审核通过后才满足合入条件 |

## 13. Implementation Plan

1. 扩展 meta-label bundle/runtime prediction，冻结 policy/cost evaluation contract。
2. 增加 migration、models、repository immutable persistence。
3. 实现 DB market source、rank reconstruction、连续 epoch replay、outcome/metrics构造。
4. 将 bounded evaluation 接入 scheduler与API，扩展荐股页面。
5. 完成定向测试、DEV gate、F2 validator和多轮审核修复，停在 `MERGE_READY_AWAITING_USER`。

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-801 | `backend/services/advisory_model_first/meta_label_bundle.py`; `backend/services/advisory_model_first/model_inference.py` | `backend/tests/advisory_model_first/test_meta_label_bundle.py`; `backend/tests/advisory_model_first/test_meta_label_runtime_inference.py` | pass | none |
| F-802 | `backend/services/advisory_forward/evaluation.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation.py::test_forward_model_evaluation_ignores_market_rows_after_explicit_watermark` | pass | none |
| F-803 | `backend/services/advisory_forward/evaluation.py`; `backend/services/advisory_forward/repository.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation.py` | pass | none |
| F-804 | `backend/services/advisory_model_first/shadow_portfolio_policy.py`; `backend/services/advisory_forward/evaluation.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation.py::test_forward_model_evaluation_replays_exact_top5_policy_and_builds_mature_outcomes` | pass | none |
| F-805 | `backend/services/advisory_forward/evaluation.py`; `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation.py`; `backend/tests/advisory_model_first/test_forward_model_evaluation_dev_db.py` | pass | none |
| F-806 | `backend/db/migrations/add_advisory_forward_model_evaluation_20260823.sql`; `backend/services/advisory_forward/repository.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation_repository.py`; `backend/tests/advisory_model_first/test_forward_model_evaluation_dev_db.py` | pass | none |
| F-807 | `backend/services/advisory_forward/evaluation.py` | `backend/tests/advisory_model_first/test_forward_model_evaluation.py::test_forward_model_evaluation_replays_exact_top5_policy_and_builds_mature_outcomes` | pass | none |
| F-808 | `backend/services/advisory_forward/service.py`; distinct-Program pending query | `backend/tests/advisory_model_first/test_forward_model_evaluation.py`; `backend/tests/advisory_model_first/test_forward_model_evaluation_repository.py` | pass | none |
| F-809 | `backend/routers/advisory.py`; `frontend/src/lib/api/advisory.ts`; `frontend/src/app/paper-v2/advisory/page.tsx` | `backend/tests/advisory_model_first/test_forward_api.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | pass | none |
| F-810 | `backend/db/migrations/add_advisory_forward_model_evaluation_20260823.sql`; rollback migration | `backend/tests/advisory_model_first/test_forward_model_evaluation_dev_db.py`; `AISTOCK_RUN_ADVISORY_FORWARD_EVALUATION_DEV_DB=1 python -m pytest backend/tests/advisory_model_first/test_forward_model_evaluation_dev_db.py -q` | pass | none |
| F-811 | changed-file boundary and review record | `python -m nox -s advisory_modeling_backend`; `python -m nox -s platform_api_backend`; `python -m nox -s validation_module_registry_l0`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0d_forward_evaluation_f2_design_20260823.md --tier F2` | pass | none |

## 15. DESIGN-COMPLIANCE-001

1. 禁止简化：不使用固定20日Top5收益替代连续shadow policy；完整Top40 exit context和真实数据库行情进入实现。
2. 禁止静默错误：身份、日期、序列、行情、benchmark、policy和payload冲突全部typed fail closed。
3. 禁止业务漂移：只新增独立challenger评价；Selection、baseline Program、正式名单和交易边界不变。
4. 禁止私增门禁：不新增角色、审批、收益阈值或自动激活；DEV DDL、生产DDL、合入和重启仅保留既有授权边界。
