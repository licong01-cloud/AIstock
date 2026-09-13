# 波动收缩后区间突破择时研究设计

> 版本：v1.1；日期：2026-09-14；Feature tier：F1（`position_timing` 单模块离线研究）
> 状态：`ENGINEERING_VERIFIED_FORMAL_REPLAY_PENDING`
> 任务：`PT-NEXT-019 / VOLATILITY_CONTRACTION_BREAKOUT_V1`
> 所属蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md)
> 前序方法队列：[自选股与持仓股形态择时研究设计](position_timing_pattern_strategy_design_20260911.md) §7
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`

## 1. Background / 目标与研究问题

终极目标仍是为用户已经选定的持仓股和显式自选股提供能够增加成本后收益、降低错误交易或改善风险收益比的明确择时建议。本项不扩展选股范围，只验证一个已在前序方法队列登记、且与 `PT-NEXT-018` 的低点/均线回踩机制不同的问题：股票经历自身历史上的低波动与量能收缩后，收盘突破此前价格区间，等待该信号再买入是否优于同股在评价起点立即买入并持有。

本设计把“可能有效的经验形态”翻译为一个可复现、因果、可证伪的日频政策；不宣称技术形态天然有效，也不把工程测试通过当成超额收益。历史回放是主要验证方式，不等待最新交易日、HMM、模型训练或实盘样本。结果无论 `SUPPORTED`、`NEGATIVE` 或 `INCONCLUSIVE` 都形成不可变证据；研究结果不作为源码合入门禁。

`PT-NEXT-018` 的 64 只评价股已经用于另一种形态假设。新机制在读取其结果前已存在于详细设计 §7 的后续队列，但该人口不是全新 sealed holdout，所以本项固定标为 `EXPLORATORY_PREDECLARED_MECHANISM_REUSED_EVALUATION_POPULATION`。该披露限制外推强度，不阻止历史研究，也不要求另找股票或等待未来数据。

## 2. Scope / 范围与 Non-goals

本项交付一个纯规则信号、一个连续账户政策、一个正式主比较、不可变 `prepare → run → inspect → exact retry` 管线、直接测试和蓝图结果回填。运行人口固定为前序 r4 request 的 64 只评价股；原 64 只训练股只用于固定的开发烟测与实现反例，不用于调参或筛选正式结果。

明确不做：模型训练、阈值网格、策略投票、HMM/QE/Agent/新闻融合、分钟信号、API、页面、卡片、提醒、scheduler、worker、数据库表、自动交易、运行模型或 serving 接线。不修改共享 guard 默认值，不读 active profile，不抓取数据，不做 DDL/DML，不重建或激活数据集，不控制进程。

写入范围限于：

- `backend/services/position_timing/`；
- `backend/tests/position_timing/`；
- 本设计、主择时蓝图和必要的同模块形态设计进度说明。

研究 artifact 只写 timing-owned `research/volatility_contraction_breakout_v1/requests|bundles`。不写 N0、QE、Selection、Advisory、Watchlist、Paper、MiniQMT 的 registry/current/table/artifact，也不产出订单。既有模块不得反向依赖本研究文件。

## 3. Architecture / 最小架构与复用

```text
r4 DailyCandidate + 冻结公司行动/停牌/配股 authority
                          ↓
      复用 PT-NEXT-018 source-only 完整性与共同账户政策
                          ↓
  volatility_contraction_breakout.py：纯特征和单一规则
                          ↓
  pattern_research.py：最小规则注入点，旧默认行为不变
                          ↓
  既有 T+1 daily_fill / 现金库存 / 风险退出 / 逐腿成本
                          ↓
  volatility_contraction_breakout_research.py
       prepare / run / inspect / immutable exact retry
```

| 基础能力 | 复用方式 |
|---|---|
| 日频源与全局交易日 | `action_value_data.DailyCandidate`；显式 r4 candidate 与 manifest identity |
| 复权与可比成交量 | 沿用 `pattern_strategy.pattern_feature_frame` 的 `raw × factor` 和公司行动数量映射，不前填停牌日 |
| 公司行动与停牌 | `CorporateActionBook`、`SuspensionSnapshotBook` 及前序冻结 snapshot；不读取数据库 |
| 配股 | `pattern_rights_issue` 的 typed authority 与统一 `NEVER_SUBSCRIBE`；factor 不推断账户认购 |
| 成交、涨跌停、费用 | `pattern_research.replay_full_policy_symbol` 最终调用既有 `daily_fill/apply_fill` 与 componentized cost policy |
| 风险退出 | 只用既有冻结 `risk_exit_plan`；关闭 P0 的加速放量补充退出，隔离入场机制 |
| 不可变持久化 | `PositionTimingArtifactStore._publish_immutable`、现有文件引用/hash、原子目录发布和 inspect 模式 |

只对 `replay_full_policy_symbol` 增加两个可选参数：入场观察函数和是否启用 P0 补充退出。二者默认值保持当前 P0 行为；旧调用与旧 artifact 语义不变。新规则传入自己的观察函数、选择即时 `E0` 买入，并关闭 P0 补充退出。这里不增加通用策略平台、插件注册表或第二套回放引擎。

## 4. Contracts / 冻结策略、时钟与来源

### 4.1 信号与动作

所有滚动窗口使用 candidate 的全局交易日索引，不删除停牌日压缩时钟。调整价为 `raw OHLC × factor`；成交、现金、涨跌停与费用继续使用原始人民币价格。T 日数据只在 T 日 20:00（Asia/Shanghai）决策时可用，动作目标固定为 T+1，不能用 T 日收盘信号假设 T 日成交。

固定参数：

| 参数 | v1 值 |
|---|---:|
| 区间上沿窗口 | 20 个全局交易日，严格为 T-20..T-1 的调整后最高价 |
| ATR | 14 日简单平均 true range |
| 历史波动基准 | `ATR14 / adjusted_close` 的此前 252 个有效比较值 |
| 低波动阈值 | 30% 分位；阈值窗口截止 T-2，待比较值为 T-1 |
| 成交量收缩 | T-5..T-1 的可比成交量均值 `<` T-20..T-1 均值 |
| 突破 | T 日调整后收盘 `>` T-20..T-1 调整后最高价 |
| 买入规模 | 既有参考资本下最大合法预算数量，实际成交仍由 T+1 guard/涨跌停/停牌/现金约束 |
| 持仓退出 | 仅冻结 `risk_exit_plan`；无形态止盈、模型退出或人工回填 |
| 成本主情景 | 1 个父订单，逐腿 componentized fee |
| 成本敏感性 | 2、3 个父订单，仅作诊断，不增加正式假设 |

令 `v_t = ATR14_t / adjusted_close_t`。T 日信号同时满足：

1. T 日、T-1 及所有所需窗口的 PIT、价格、factor、成交量均可用；
2. `v_(T-1) <= quantile_0.30(v_(T-253)..v_(T-2))`；
3. `mean(volume_(T-5)..volume_(T-1)) < mean(volume_(T-20)..volume_(T-1))`；
4. `adjusted_close_T > max(adjusted_high_(T-20)..adjusted_high_(T-1))`。

信号日只产生一次 T+1 OPEN 尝试。T+1 无法成交时按真实 fill 状态记账，不假设成交、不延长旧建议；之后只有再次形成新的 20 日收盘突破才会再次尝试。持仓期间忽略新的 OPEN 信号，风险退出后未来新信号可重新入场。信号输入不可用与“条件不满足”分别写入 `signal_observations`，不得把 unknown 当成无信号。

### 4.2 人口、来源与账户政策

candidate 显式冻结为：

`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260912-r4-candidate`

根 manifest 文件 SHA256 为 `ed8375696030ca95b4a1f30167c2ac956e69b8276ba981babd301682dcea78de`，dataset identity 为 `1db13b2129409c2ee4aabd8bc83c3f5e5eee1fde2a859a3cb2a722d885dd5c49`。父 source authority 来自最终 PT-NEXT-018 request `2a8cdf4d74dd023a1c1cba415c968a089cfc5759d016eebdcfb38617d550cb6f`；只读取其 request/source coverage，不读取父 receipt、比较值或模型输出来形成当前规格。

正式范围仍是 2018-08-01..2026-08-31，评价人口严格等于父 request 的 64 只 `evaluation_symbols`。公司行动 snapshot、停牌 snapshot、三条 `RIGHTS_ISSUE` authority 与 `NEVER_SUBSCRIBE` policy identity 全部逐项沿用并重新校验文件/hash/scope；新 request 绑定当前代码、规则、成本、人口、candidate 和全部 source authority hash。任何输入漂移均在读取当前策略收益前 typed fail closed，不能回退 active profile、网络、数据库、换股、缩短历史或放宽 10 bps factor-action 阈值。

配股参与继续为 `NEVER_SUBSCRIBE`：不认购、不注入认购现金、不部分认购、不产生配股数量、到账或可卖股份。candidate factor 只用于特征价格连续性，不能推断账户已经认购。该共同政策同时作用于候选与 comparator。

### 4.3 Estimand 与统计分类

每股从同一评价起点建立 100,000 元现金 sleeve：

- 候选政策 `VCB_V1`：等待冻结信号后在 T+1 买入；持仓仅使用冻结风险退出；
- 主 comparator `ALWAYS_OPEN_RISK_MANAGED`：评价起点首次可执行的 T+1 买入，使用与候选完全相同的冻结风险退出；风险卖出后在下一次合法决策立即计划重新买入；
- `BUY_AND_HOLD` 路径保留为完整政策表现诊断，但因退出语义不同，不进入入场择时的正式主结论；默认 `FROZEN_L1` 路径仅供旧P0调用，新研究不使用它。

主 estimand 是各全局交易日先对有效股票 sleeve 求均值后的 `VCB_V1 - ALWAYS_OPEN_RISK_MANAGED` 日度成本后增量 bps。两条路径的股票、资金、公司行动、风险退出和成交语义完全相同，唯一政策差异是现金时等待VCB信号还是始终尽快入场；因此主结论才可归因于入场择时。报告期累计值、毛值、逐腿费用、暴露、最大回撤、signal/fill 计数均为解释字段。主情景采用 95% 的 25-session circular block bootstrap；一个 family 只含这一个正式比较，所以 family-wise 与 nominal 区间相同。

分类冻结为：区间下界 `> 0` 为 `SUPPORTED`，上界 `< 0` 为 `NEGATIVE`，其余为 `INCONCLUSIVE`。`economic_threshold_bps=0.0`；`power_status` 独立记录，未预注册同 estimand 尺度时为 `NOT_COMPUTABLE`，不以 MDE 阻止研究或合入。只有主比较 `SUPPORTED` 时 `selected_trial_count=1`，否则为 0；该字段不自动发布 serving。

2/3 父订单情景沿同一股票、信号和路径重放，只标成本敏感性。若主情景 `SUPPORTED` 而任一敏感性区间下界不再为正，增加 `COST_ASSUMPTION_SENSITIVE`，不改变主分类、不形成运行门禁。

## 5. Immutable Artifact / 不可变产物

CLI 保持四步：

```text
prepare --timing-root --repository-root --parent-pattern-request
run --request
inspect --bundle
run --request  # exact retry
```

`prepare` 只读取父 request、source-only identity 与 candidate/source字段，冻结规则和输入后写 `<request_sha256>.json`；不得读取父 receipt 或当前策略收益。`run` 要求源码 commit 与 request 相同，输出 request、receipt、coverage、signal observations、sleeve days、fills 和三种成本情景摘要。bundle manifest 递归绑定全部文件。相同 request 已存在时必须 inspect 后返回 `ALREADY_MATERIALIZED`；源码、数据、政策或规格变化生成新 request，禁止覆盖历史 artifact。

receipt 至少包含：source/candidate/policy/rule/code hash、人口和日期、各类 signal/fill/coverage 计数、主比较及成本敏感性、effect/power、尝试数、selected 数和所有写入隔离布尔值。coverage 不完整时比较结论强制为 `INCONCLUSIVE`，同时保留原统计值与 typed reason；不删除问题股票来制造覆盖。

## 6. Implementation Plan / 单一实施块

本任务只有一个实施块，内部按依赖顺序执行，不拆成多个产品阶段或审批：

1. 冻结本文 F1 设计并通过 validator；
2. 实现纯特征/信号和现有回放的最小默认不变扩展；
3. 实现专用不可变研究管线与直接反例；
4. 在固定开发股做不读正式比较值的功能烟测；
5. 代码提交后以该 clean commit 执行正式 `prepare → run → inspect → exact retry`；
6. 将真实结果回填本文与主蓝图，执行两轮不同关注点的审核、修复和直接回归；
7. 提交文档证据，创建 PR，等待适用 CI，通过后合入。

计算耗时不是压缩样本或改阈值的理由。第一轮审核聚焦 PIT/复权/停牌/公司行动/配股/成交/成本/账户连续性；第二轮聚焦人口复用披露、统计单位、尝试计数、artifact 身份、隔离与结论措辞。发现实现缺陷用新 commit、新 request 和不可变 supersession 修复；不得改写已生成结果。

## 7. Verification Plan / 验证方案

直接测试至少覆盖：

- 所有窗口严格止于允许时点，改变 T+1 或未来值不改变 T 信号；
- 除权不产生伪突破，停牌/缺 factor/零价/缺量返回 typed unavailable；
- 低波动、量缩和区间突破四项均为必要条件，边界 `<=`/`<`/`>` 与设计一致；
- 回放旧默认参数与改动前等价；新回调只在现金路径触发，即时 T+1，补充退出关闭但风险退出保留；
- BUY_AND_HOLD 与候选共享 source、账户政策、公司行动和逐腿成本；1/2/3 父订单按真实路径计费；
- prepare 不读取父 receipt，r4/candidate/snapshot/rights/policy/人口漂移 fail closed；
- bundle 篡改、源码 commit 漂移、request 漂移、重复 run 均有确定行为；
- registry/current/card/alert/order/database/runtime/serving 写入全为 false；无其他模块 import 本研究。

本地只运行直接测试、changed-file/scope 检查、ruff/compile、F1/F2 validator 与必要的 position_timing 回归；广泛重复覆盖交给 PR CI/Nightly，不为“多轮”机械重复同一套日志。正式收益 bundle、inspect 和 exact retry 是业务证据，不替代代码测试。

当前工程读回：新增纯策略与专用研究管线各一个文件，只对既有 `replay_full_policy_symbol` 增加默认不变的入场观察函数和补充退出开关；新增两个直接测试文件，并在既有 pattern 测试中补默认等价反例。直接测试33项通过，完整 `backend/tests/position_timing` 在项目 `AIstock` Python环境为315项通过，ruff与compile通过。真实r4 source-only预检得到64只评价股、364/364个material factor interval已绑定、unbound=0、三条配股参与政策均冻结；固定开发股前2只产生19个市场匹配、9个现金态可行动信号和4,816行连续sleeve。上述只证明工程路径和来源闭合，不是正式收益结论。

## 8. Design Acceptance Index

| design_item | acceptance requirement |
|---|---|
| F-001 | 单一研究问题、终极目标、探索性人口复用和不夸大收益明确 |
| F-002 | 信号公式、PIT时钟、动作、退出和所有参数一次冻结 |
| F-003 | r4 candidate、父 source authority、配股共同政策和完整 coverage 绑定 |
| F-004 | 同股连续账户主 comparator、成本和统计分类闭合 |
| F-005 | 只增加最小规则注入点，既有默认行为不变且不复制回放基础 |
| F-006 | immutable prepare/run/inspect/exact retry 与结果身份闭合 |
| F-007 | HMM/QE/Agent/minute/runtime/DB/订单及其他模块隔离明确 |
| F-008 | 直接测试、两轮审核、证据回填和合入标准明确 |

## 9. Design Acceptance Matrix / 设计验收矩阵

`ENGINEERING_VERIFIED` 表示代码、直接测试和当前source-only预检闭合，不表示正式回放已经完成、策略取得收益或运行功能已经发布。正式bundle完成后还必须逐行补入artifact证据；不得引用本矩阵声称策略有效。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文§1、§2、§4.3；`volatility_contraction_breakout_research.py` | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py`；父队列§7 | ENGINEERING_VERIFIED | none |
| F-002 | `volatility_contraction_breakout.py` | `backend/tests/position_timing/test_volatility_contraction_breakout.py` | ENGINEERING_VERIFIED | none |
| F-003 | `volatility_contraction_breakout_research.py::_source_contract/prepare_request` | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py`；真实source-only预检364/364、rights=3 | ENGINEERING_VERIFIED | none |
| F-004 | `volatility_contraction_breakout_research.py::_comparison/run_request` | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py` | ENGINEERING_VERIFIED | none |
| F-005 | `pattern_research.py::replay_full_policy_symbol` 两个默认不变参数 | `backend/tests/position_timing/test_pattern_research.py` 默认等价与即时T+1反例 | ENGINEERING_VERIFIED | none |
| F-006 | `volatility_contraction_breakout_research.py::_publish_bundle/inspect_bundle/run_request` | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py` 篡改与exact retry反例 | ENGINEERING_VERIFIED | none |
| F-007 | 本文§2、§3；新文件仅导入`position_timing` | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py` 写入布尔值；合入前scope/import扫描 | ENGINEERING_VERIFIED | none |
| F-008 | 本文§6、§7、§10 | `backend/tests/position_timing/test_volatility_contraction_breakout_research.py`；完整position_timing回归315 passed；`scripts/aistock_feature_workflow.py` F1 validator；PR CI待当前PR | ENGINEERING_VERIFIED | none |

## 10. Risks and Compliance / 风险与符合性

主要风险是阈值任意、信号稀少、复用人口的数据窥探、长期持币造成风格暴露差异、公司行动形成伪突破，以及把正点估计误称有效。处置是一次冻结而不搜索、完整 signal/coverage 计数、明确探索性结果类、连续账户与暴露诊断、共享 source authority 和区间结论。后续是否研究新参数或融合因子只能依据失败机制另立独立规格，不能在本 bundle 上追加赢家搜索。

DESIGN-COMPLIANCE-001：①交付完整冻结策略、研究管线和真实历史结果，不以 POC、烟测或工程通过冒充完整；②unknown、no-signal、no-fill、coverage 缺口和篡改均显式，禁止静默填零；③不改变用户要求、共享 L1/L1a、账户政策或已批准边界，任何范围变化先回填设计；④不增加 MDE、最新日期、HMM、sealed holdout、人工审批或收益门禁，只有来源/因果/身份错误阻止对应错误计算。

## 11. Production Gates / 生产边界与合入标准

合入要求是设计 validator、直接测试、changed-file/scope、静态检查、bundle inspect/exact retry 和逐条验收矩阵真实闭合；策略收益方向不是合入条件。`production_ddl_gate=noop`、DML/dependency/client/install/runtime/restart/activation 均为 noop。源码合入后不需要后端重启，因为本项没有 API 或运行态接线；如未来另行接 serving，后端重启仍由用户执行。
