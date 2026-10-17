# AIstock Advisory N2 Entry / Exit 辅助诊断 F2 详细设计 v1.0

- 日期：2026-09-02
- Feature tier：F2
- 当前状态：`SOURCE_IMPLEMENTED_LOCAL_VERIFIED_FORMAL_RUN_PENDING`
- 业务归属：Selection Center / Advisory / Model-first Research
- 目标合同：`RISK_MANAGED_ADVISORY`
- 研究类型：`ORACLE_DIAGNOSTIC`
- 证据用途：`NAVIGATION_ONLY`
- 蓝图映射：F-182、F-183、F-184、F-185、F-188、F-189

## 1. Background / 当前事实

1. N1 已证明 Top20 内存在很高的 clairvoyant 排序空间，但当前冻结信息集的简单 ranker 未达到确认门槛；N2-A 又证明当前父包的不同腿存在信息差异，但固定 IC 组合未稳定胜过 LSTM。
2. N2-B v2 两存续 StrategyPackage 同窗审计已经以独立 request 启动；它只诊断候选 Alpha，不回答 T+1 开盘后是否应买入，也不回答持仓何时退出。
3. M4 已提供真实 `entry_gap_q10/q50/q90` 预测。其 executable 二分类标签在 8120 行中仅 4 个负例，不能继续作为 Entry Guard 的主要可学习目标。
4. 现有 `PriceGuard` 提供规则买卖保护，但包含 `size_multiplier/target_weight` 等执行语义；N2 当前只允许固定等权槽位的 `SKIP/WAITING`、现金或显式补位，不允许动态资金权重。
5. 现有 `build_policy_episode_labels` 与 `replay_shadow_portfolio` 已按冻结 transition policy、成本、停牌和涨跌停生成真实 baseline episode。本任务必须复用该模拟器身份，不能另造一套简化收益口径。
6. P0-H/P0-K 的 liability/holding 相关性只证明当前 policy 下的持有负担可预测，不能作为 Exit label、Exit oracle 或可部署 Exit 模型的证据。
7. 全部诊断只消费 development 历史窗口；sealed holdout 保持未读。历史回放结果只可导航，不能支持激活。

## 2. Goal and scope

本 F2 交付提供 N2 Entry/Exit 辅助诊断的可复用、PIT 严格、无仓位语义的源码核心：

1. `AdvisoryIncrementalValueLabelV1`：统一表达某层动作相对冻结 baseline 动作的增量净价值，并绑定 baseline policy、intervention policy、cost policy、shadow simulator 与 evidence level。
2. `AdvisoryEntryGuardDecisionV1`：只消费 T 日冻结信号与 T+1 当时可见 open/current，产生 `ACCEPT/REDUCE/SKIP/WAITING`；输出不含数量、权重或资金分配。
3. Entry 增量标签：把 Entry Guard 决策与同一 baseline episode label 配对；`SKIP` 对应固定槽位现金，`ACCEPT` 沿用 baseline，`REDUCE` 仅为非数值谨慎提示，不伪造半仓收益。
4. Exit-label oracle：内部调用现有 baseline episode builder，随后在每个持有 review 时点比较“请求在下一可交易开盘退出”与“继续冻结 baseline policy”的净价值；停牌、跌停、缺价和右删失均返回 typed 状态。
5. 证据边界：历史回放、sealed holdout confirmation、prospective OOS 分级；历史回放不得声明 `ACTIVATION_EVIDENCE`。
6. 干预支持度：每个角色预注册最低干预次数、干预交易日比例、regime 覆盖和 block 长度；不足时只输出 `EXPLORATORY_ONLY`。
7. 正式 N2 数据运行、bundle 与 registry 追加在源码合入后执行；源码交付与实验结果分别报告，不把测试夹具冒充真实诊断结果。

## 3. Non-goals

- 不训练 Entry、Exit、仓位或组合模型，不选择模型 family、loss、seed、阈值或特征。
- 不把 `REDUCE` 映射为 0.5 仓位、数量或任何数值资金语义。
- 不把 `SKIP` 静默替换为第 6 名；现金 arm 与显式 replacement arm 必须是不同 policy hash。
- 不把 liability、holding days、胜率或单一相关系数当作 Exit PASS 条件。
- 不读取 sealed holdout，不回填 prospective OOS，不发布 production bundle，不接入 Selection 写入、Paper、QMT、下单或 API 展示。
- 不建设 OPE 平台、通用 simulator、scheduler、缓存、ModelOps、审批或历史归档系统。
- 不执行后端启动/停止/重启、DDL/DML、依赖安装或生产激活。

## 4. Architecture

```text
frozen baseline policy + cost policy + Top20 rankings + canonical market/suspend
                                  |
                   existing policy episode simulator
                                  |
                    baseline episode labels/paths
                         /                   \
        M4 frozen T signal + T+1 open       held review decision T
                      |                              |
          typed Entry Guard decision        next executable open arm
                      |                              |
          paired Entry incremental label    paired Exit incremental label
                         \                   /
                  shared action-value contract
                                  |
           intervention support + evidence-level gate
                                  |
        development-only oracle/navigation artifact (later run)
```

代码边界：

- `action_value_contracts.py`：共享 enums、增量价值 label、evidence 与 intervention support；
- `entry_guard_decision.py`：冻结 Entry policy/signal/market observation 与纯函数决策；
- `incremental_value_labels.py`：Entry 决策与 baseline episode 的严格配对；
- `exit_label_oracle.py`：复用 baseline simulator 并构造 Exit 两臂 oracle；
- 不修改生产 `PriceGuard`、Selection、Advisory API、数据库 schema 或 runtime binding。

## 5. Shared incremental-value contract

### 5.1 Identity

每条 `AdvisoryIncrementalValueLabelV1` 必须冻结：

- `role=ENTRY_GUARD|EXIT`；
- `objective_contract=RISK_MANAGED_ADVISORY`；
- decision date、target/effective action date、instrument、episode id；
- baseline action、intervention action 与 typed label status；
- baseline/action net value bps 及其差值；不可评价时三者均为空；
- `baseline_policy_sha256`、`intervention_policy_sha256`、`cost_policy_sha256`、`shadow_simulator_sha256`；
- `evidence_level`、`decision_use`、`sealed_holdout_accessed`；
- `information_start/end` 与不可用原因；
- canonical self hash。

数值约束：`incremental_net_value_bps = action_net_value_bps - baseline_net_value_bps`。任一 policy hash、日期、动作或数值变化都产生新 label hash。

### 5.2 Evidence levels

- `HISTORICAL_REPLAY`：只允许 `NAVIGATION_ONLY` 或 `DIRECTION_GATE`，且 `sealed_holdout_accessed=false`；
- `SEALED_HOLDOUT_CONFIRMATION`：必须显式 `sealed_holdout_accessed=true`，但本任务不生成此类 receipt；
- `PROSPECTIVE_OOS`：由自然前向观察生成，本任务不回填；
- 任一 historical label 声称 `ACTIVATION_EVIDENCE` 必须 fail closed。

### 5.3 Intervention support

`AdvisoryActionInterventionSupportV1` 按 role/policy hash 统计：总决策、干预数、干预日期数、覆盖交易日比例、regime 分布、block 长度和有效 block 数。最小干预数、最小日期比例、每个必需 regime 的最小日期数和最小有效 block 数均在运行 request 中预注册；任一不足只返回 `EXPLORATORY_ONLY`，不得用事后阈值升级。

## 6. Entry Guard contract

### 6.1 Frozen inputs

`EntryGuardFrozenSignalV1` 只含 T 日可见内容：

- decision date、下一交易日 target date、instrument、selection rank；
- raw reference price；
- M4 `entry_gap_q10/q50/q90`；
- 冻结 `max_acceptable_gap_bps` 与 `max_buy_price`（若 mode 需要）；
- M4/binding/feature/policy hash 与信息截止时间。

`EntryGuardMarketObservationV1` 只含 T+1 当时可见内容：open/current、limit up/down、suspend status、observation timestamp。schema `extra=forbid`；`close/high/low/future_return/label` 不能进入对象。

### 6.2 Policy arms

- `NO_GUARD`：可交易且有价格时接受，用作无保护 baseline；
- `FIXED_GAP_3`：最大开盘 gap 300 bps；
- `FIXED_GAP_5`：最大开盘 gap 500 bps；
- `FROZEN_DYNAMIC`：使用 T 日冻结的 max gap / max buy price；
- yellow boundary 只产生 `REDUCE` 提示，不带 multiplier；
- near-limit、超过 max gap 或 max buy price产生 `SKIP`；停牌或价格未到产生 `WAITING`。

policy 固定 `target_slot_count=5`、`allow_dynamic_position=false`、`silent_replacement=false`。显式 replacement arm 必须使用另一个 policy hash，并由后续 formal pipeline 单独报告。

### 6.3 Paired Entry label

输入 baseline episode 必须来自同一 shadow simulator，且 episode 的 shadow/cost hash 与 request 完全一致：

- `ACCEPT`：action value 等于 baseline value，incremental 为 0；
- `SKIP`：action value 为现金 0 bps，incremental 为 `-baseline value`；
- `REDUCE`：`NON_NUMERIC_ADVICE_ONLY`，不构造伪半仓收益；
- `WAITING` 或 baseline 未成熟：typed unavailable/censored，不填 0。

## 7. Exit-label oracle contract

1. 公共入口必须调用现有 `build_policy_episode_labels` 生成 baseline，禁止接受来历不明的“最佳退出”表代替 baseline simulator。
2. 对 `MATURED` baseline episode，从 entry decision 后每个合法 review decision 构造动作 arm；同一 entry price、buy cost、sell cost 与 policy hash 被两臂复用。
3. action arm 请求在 review T 后的第一可交易开盘退出；停牌、缺价和一字跌停继续扫描至下一可交易日，并记录 deferred trading days。超过 data cutoff 则 `CENSORED_RIGHT_BOUNDARY`。
4. baseline value 使用冻结 policy 的原 effective exit/price；action value 使用第一可交易 exit/price。两者均按同一买卖成本计算完整 episode net bps。
5. oracle preferred action 只按增量值选择 `HOLD` 或 `EXIT_NEXT_OPEN`；它必须标记 `FUTURE_INFORMATION_CEILING/NOT_DEPLOYABLE`。执行延迟独立记录，不伪装为当日成交。
6. baseline 未成熟、policy/cost hash 不一致、calendar 不连续、行情重复/缺失或 action date 超界均 typed fail/unavailable；不得填 0 或删除样本。

## 8. PIT and leakage controls

- Entry decision 的 T 日 signal 在注入 T+1 close/high/low/future return poison 后必须完全不变；这些字段因 schema 禁止而无法进入决策。
- T+1 open/current 仅用于 Entry action，不回写 T 日 feature 或 Selection rank。
- Exit 未来路径只用于 label/oracle；不得进入 candidate feature、runtime bundle 或生产 decision。
- 同一 baseline/action 两臂使用相同 instrument、episode、policy、cost、price basis 和 simulator identity。
- 历史窗口消费写 registry 后不得冒充 sealed confirmation；本源码任务不读取任何 sealed 路径。

## 9. Error contract

至少包含以下 reason codes：

- `ADVISORY_ACTION_VALUE_POLICY_MISMATCH`
- `ADVISORY_ACTION_VALUE_NUMERIC_MISMATCH`
- `ADVISORY_EVIDENCE_LEVEL_VIOLATION`
- `ADVISORY_ENTRY_GUARD_CLOCK_MISMATCH`
- `ADVISORY_ENTRY_GUARD_INPUT_UNAVAILABLE`
- `ADVISORY_ENTRY_GUARD_DYNAMIC_POSITION_FORBIDDEN`
- `ADVISORY_ENTRY_LABEL_PAIR_MISSING`
- `ADVISORY_EXIT_BASELINE_UNAVAILABLE`
- `ADVISORY_EXIT_ACTION_CENSORED`
- `ADVISORY_EXIT_MARKET_DATA_INVALID`

可恢复的数据缺失返回 typed label status；身份、时钟、hash、数值恒等式或证据边界错误抛出 `AdvisoryModelFirstError`。禁止 broad exception 后继续成功。

## 10. Implementation Plan

1. 实现共享 action-value、evidence 与 intervention-support contracts。
2. 实现 Entry Guard policy/signal/observation/decision 与固定 3%/5%/动态 arm。
3. 实现 Entry paired labels，验证 policy hash、现金空槽、REDUCE 非数值语义。
4. 实现 Exit oracle，内部复用 baseline episode builder，并覆盖 suspend/limit-down/censoring。
5. 增加四个直接测试文件并运行 Advisory modeling 最小 nox 计划。
6. 更新蓝图 Acceptance Matrix 的源码状态；formal N2 artifact/registry 仍保持 `PENDING_REAL_RUN`。
7. 多轮审核 correctness、PIT/leakage、error/fallback、scope 和 DESIGN-COMPLIANCE；通过后提交、PR、CI、合入。
8. 源码合入后才冻结 formal N2 Entry/Exit request；真实结果单独报告，不回写本设计的源码验收。

## 11. Verification Plan

### 11.1 Contracts

- unknown field、非法 enum、非 SHA256、policy mismatch、增量恒等式错误均拒绝；
- historical + activation evidence、historical + sealed accessed 的组合拒绝；
- insufficient intervention support 只能是 exploratory。

### 11.2 Entry Guard

- 固定 3%/5% 与 dynamic policy 在边界上产生确定动作；
- 缺价/停牌为 WAITING，近涨停/超 gap/超 max buy 为 SKIP；
- future close/high/low poison 无法进入 schema；
- SKIP 形成现金空槽且不补第 6 名；decision 无 weight/quantity/multiplier 字段；
- REDUCE 不生成数值增量 label。

### 11.3 Exit oracle

- action 与 baseline 复用同一 simulator/policy/cost；
- 立即退出避免后续亏损时增量为正，过早退出错过收益时为负；
- 停牌和一字跌停推迟至第一可交易开盘；无可交易点时右删失；
- liability/holding 列即使存在也不参与 oracle preferred action；
- baseline 未成熟不产生伪数值。

### 11.4 Local and delivery gates

- direct tests；
- changed-file ruff/format/compile；
- `python -m nox -s advisory_modeling_backend`；
- `python scripts/aistock_feature_workflow.py validate --design <this-file> --tier F2`；
- `git diff --check` 与 exact scope；
- production DDL/frontend/backend dependency gates 均为 noop；无 runtime activation。

## 12. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-182 | 每个角色预注册干预次数、交易日比例、regime 与 block 阈值；不足只可探索 |
| F-183 | 所有 Entry/Exit 标签表达相对冻结 baseline 的增量净价值并绑定 policy/simulator/cost identity |
| F-184 | Entry 只消费 T 冻结信息与 T+1 open/current；SKIP 可留现金且无静默补位、动态权重或数量；REDUCE 不伪造半仓收益 |
| F-185 | Exit baseline 来自现有 policy simulator；第一可交易开盘、停牌、跌停和删失均 typed；liability/holding 不充当 Exit 证据 |
| F-188 | historical、sealed holdout 与 prospective OOS 证据不能互相冒充；源码交付不冒充 formal experiment |
| F-189 | Entry/Exit contract 明确拒绝动态资金仓位、自动下单和交易执行输入 |

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-182 | `backend/services/advisory_model_first/action_value_contracts.py`; `backend/services/advisory_model_first/incremental_value_labels.py` | `backend/tests/advisory_model_first/test_incremental_value_labels.py`; `backend/tests/advisory_model_first/test_evidence_level_boundaries.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-183 | `backend/services/advisory_model_first/action_value_contracts.py`; `backend/services/advisory_model_first/incremental_value_labels.py` | `backend/tests/advisory_model_first/test_incremental_value_labels.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-184 | `backend/services/advisory_model_first/entry_guard_decision.py`; `backend/services/advisory_model_first/incremental_value_labels.py` | `backend/tests/advisory_model_first/test_entry_guard_decision.py`; `backend/tests/advisory_model_first/test_incremental_value_labels.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-185 | `backend/services/advisory_model_first/exit_label_oracle.py` | `backend/tests/advisory_model_first/test_exit_label_oracle.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-188 | `backend/services/advisory_model_first/action_value_contracts.py`; N2 builders historical-only gate | `backend/tests/advisory_model_first/test_evidence_level_boundaries.py`; `backend/tests/advisory_model_first/test_incremental_value_labels.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-189 | Entry/Exit Pydantic contracts with `extra=forbid` | `backend/tests/advisory_model_first/test_entry_guard_decision.py`; `backend/tests/advisory_model_first/test_exit_label_oracle.py` | IMPLEMENTED_LOCAL_VERIFIED_NO_POSITION_OUTPUT | none |

## 14. Risks and controls

| Risk | Control |
|---|---|
| hindsight oracle 被误当可部署 | hard-coded future ceiling / not deployable evidence state |
| REDUCE 偷渡仓位 | schema 无 multiplier/weight/quantity；数值 label 禁止 |
| 两臂使用不同 policy/cost | exact SHA256 equality before every label |
| 停牌/跌停被删除 | typed defer/censoring，保留样本与原因 |
| historical 污染 sealed | evidence-level hard gate；本任务无 sealed path |
| 新建通用平台 | 只实现 Advisory task-local contracts/builders |
| 测试夹具冒充真实收益 | Acceptance Matrix 明确 formal run pending |

## 15. Rollout / rollback

- 本功能先作为 development-only offline module，无 API、DB、scheduler 或 active binding。
- 回滚只需回退新增离线模块与测试；不会改变现有 Selection、PriceGuard、Advisory runtime 或数据库状态。
- formal request 一旦冻结即绑定源码 commit；代码错误可 exact retry，同一经济结果后不得更换 arm/阈值重试。

## 16. Production gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- backend restart：不需要；本交付没有运行时路由或 active binding。
- DB DDL/DML：无。

## 17. DESIGN-COMPLIANCE-001

合入前逐项证明：

1. 未用静态规则、mock、局部样本或伪收益冒充模型/实验成功；规则 arm 只作为明确 baseline。
2. 无静默 fallback；身份、时钟、数据和证据错误 fail closed，正常停牌/缺失 typed 保留。
3. 未把业务逻辑移入脚本、测试或文档；正式逻辑位于 `backend/services/advisory_model_first`。
4. 未新增人工审批、动态仓位、生产运行时或未授权门禁；formal 结果由既定 registry/route 解释。
