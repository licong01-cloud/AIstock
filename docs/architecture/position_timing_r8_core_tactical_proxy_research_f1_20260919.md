# PT-NEXT-023：R8-only 核心仓／战术仓与代理筛选研究设计

> 版本：v1.0；日期：2026-09-19；Feature tier：F1  
> 状态：DESIGN_FROZEN_IMPLEMENTATION_IN_PROGRESS；仅离线研究，不上线  
> 主蓝图：[F2 §9.25](position_timing_advice_f2_redesign_20260903.md)  
> 父研究：[PT-NEXT-022](position_timing_fundamental_screen_research_f1_20260919.md)  
> 唯一开发权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`

`DESIGN_VERIFIED` 只表示设计闭合，不表示代码、正式回放或超额收益已经完成。本研究使用已经被观察过的 R8 历史，全部结果均为 exploratory；阴性结果不触发自动调参，正结果也不自动进入在线卡片。

## 1. Background / 目标、已知证据与研究问题（F-001）

终极目标仍是为特定持仓股、自选股找到成本后超过同股长期持有的择时方法。PT-NEXT-022 的正式 R8 P1 证据显示：T1/T2 全市场合成收益约 5.75%/5.43%，同股 `BUY_AND_HOLD` 为 142.03%；逐股终值中位超额约 -27 个百分点。平均有仓天数约 42.3%，有仓时条件暴露约 95%，平均总暴露仅约 40.5%；平均择时费用约 11.8/13.0 万元，而 BH 约 0.15 万元。现有 T2 的“70/30”仅是一次减仓比例，普通趋势和风险信号仍会整仓退出，并非持续核心仓。

本轮只回答三个问题：

1. 在同一股票、同一 1,000 万元独立现金账户、同一起点下，将普通信号限制为战术仓调整，能否改善相对 BH 的成本后结果？
2. R8 内已有的估值／流动性代理和 `bak_basic` 日快照增长代理，能否识别更适合该类择时的股票范围？
3. 改善若存在，来自股票范围的 BH 提升，还是择时相对同股 BH 的增量？二者必须分开报告。

本轮不训练模型、不接 QE/HMM/Agent、不扩充数据源，也不完成 PT-NEXT-022 严格 P2/P3 财务 PIT。模型动作价值研究保留为后续独立任务，不与本轮六项假设混合。

## 2. Scope / Non-goals / 范围、隔离与不做事项（F-002）

只允许修改：

- `backend/services/position_timing/` 内本研究新增模块；
- `backend/tests/position_timing/` 内两个直接测试文件；
- 本 F1 与主 F2 蓝图进度条目；
- timing-owned、repo-external 新 artifact namespace。

不得修改 QE、HMM、Selection、Advisory、Paper、MiniQMT、local_data、数据库、R8 candidate/profile、在线卡片/API/UI、scheduler、worker 服务、N0 或任一 current/registry。不得启动、停止或重启服务；本研究不需要后端重启。旧 PT-NEXT-022 request/bundle、代码入口及 hash 语义不得改写。

研究只读：

`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate`

冻结 R8 身份沿用 PT-NEXT-022：manifest 文件 SHA256 `07db01d8...ebe1b18`、canonical SHA256 `6bb6096a...c39283`。正式 request 必须重新绑定实际消费的 `daily_basic.h5` 与 `bak_basic.h5` path/hash/size；不得查询数据库或网络补洞。

## 3. 三个嵌套筛选范围（F-003）

决策截点仍为 T 日 20:00，所有日快照统一滞后一全局交易日使用。缺失为 `UNKNOWN`，不能填 0、向未来填充或用当前股票属性回补历史。筛选只决定每个 `(screen_id,symbol)` 的首次入选；入选后形成连续账户，不因后续退池而删除或重置。

### U0 `SIZE_50_500B_V1`

完全等于 PT-NEXT-022 P1：T−1 `db_total_mv` 在 `[500000, 5000000]` 万元，且当日 PIT 买入资格有效、共同技术特征 ready。

### U1 `VALUE_LIQUIDITY_PROXY_V1`

U0 且 T−1 以下值全部已知并成立：

```text
0.5 <= db_turnover_rate_f <= 15.0
0 < db_pe_ttm <= 60.0
0 < db_pb <= 8.0
```

该组只称估值／流动性代理，不称质量、成长或财务 PIT。阈值在任何新收益读取前冻结，不按回放结果改变。

### U2 `BAK_GROWTH_PROXY_V1`

U1 且 T−1 以下 `bak_basic` 日快照字段全部已知并成立：

```text
bb_rev_yoy > 0
bb_profit_yoy > 0
bb_gpr > 0
bb_npr > 0
```

该组明确命名为 `BAK_BASIC_DAILY_SNAPSHOT_PROXY`。它不具备单季度、公告版本、历史修订可见性和现金流／ROE契约，不得称“业绩加速”、严格财务 PIT、P2/P3 或 Tushare 财报等价实现。PT-NEXT-022 P2/P3 继续保持 `FINANCIAL_PIT_INPUT_NOT_DELIVERED`。

输入分布预检只读得：`daily_basic` 8,124,082 行、`bak_basic` 7,923,740 行；物理交集上 U1/U2 约占 U0 的 58.8%/31.9%。这些是 source-only 计数，不是正式人口、收益或支持证据。

## 4. 两个冻结策略（F-004）

共同账户为 1,000 万元独立现金、无杠杆、无追加资金、现金利息 0、无跨股共享。Qlib 复权单位、逐腿组件费用、board lot、T+1、停牌、方向性涨跌停、终点可清算状态与 PT-NEXT-022 相同。

两策略在首次入选后的 T+1 与 BH 使用相同未加 price guard 的满额买入计划；若受停牌／涨停等真实限制未成交，则两账户按同一日重新尝试。这样比较的是持仓后的战术增量，不再把初始长期现金混入核心仓实验。成功首次买入后的虚拟单位记为 `full_units_anchor`；每次完全恢复战术仓后用实际恢复后的单位重置 anchor，不从 factor 反推真实券商股份。

### S1 `CORE_80_TACTICAL_20_V1`

- `core_floor = 80% × full_units_anchor`。
- R0 加速放量从 false→true且当前持仓净盈利、冻结 6%风险信号或连续两日趋势破坏，均只能卖出 floor 以上最多 20%单位；普通信号不得把账户卖空。
- 已在 floor 时不重复卖出；成交失败不消耗状态。
- 风险信号清除、趋势 `C>SMA60>SMA60_lag5`、`C>SMA20`且 R0=false 后，T+1用本账户可用现金尝试一次恢复；未成交由新决策日重算。

### S2 `CORE_70_STAGED_15X2_V1`

- `core_floor = 70% × full_units_anchor`。
- 第一级：R0 false→true且当前持仓净盈利，卖出 anchor 的 15%。
- 第二级：第一级成交后的后续决策日，R6 exhaustion 成立，且调整收盘价至少高于第一级成交时决策参考 `1 × ATR14`，再卖出 anchor 的 15%。同一日不得两次减仓。
- 冻结 6%风险信号或连续两日趋势破坏可一次性卖出尚未卖出的战术单位，但不得低于 70% floor。
- 风险清除、趋势成立、`C>SMA20`且 R0=false 后，用本账户现金恢复；成功后重置两级状态和 anchor。

两策略都不是在线安全退出规则；核心 floor 是本轮待检验的研究机制。在线 `rule_default` 不因本研究改变。partial sell 按执行日 raw 等价数量和合法手数取整，因取整只能停在 floor 之上，绝不能跌破 floor。恢复 BUY 只使用实际卖出释放的现金；费用可能令恢复后单位少于原 anchor，这是自然复投结果。

## 5. 假设、基准与统计（F-005）

唯一正式 family 为 `3 screens × 2 policies = 6` 项，每项仅比较同筛选组、同股、同起点的策略与 `BUY_AND_HOLD`。五个指数池和对应价格指数沿用 PT-NEXT-022作为诊断视图，不扩展 family。全市场和逐股继续展示沪深300背景；指数不是选胜者基准。

主要经济目标：

```text
terminal_liquidatable_nav(Timing) - terminal_liquidatable_nav(BH)
```

同时报告配对日收益差、逐股中位数／分位数／胜率、费用、换手、平均暴露、有仓比例、floor 违规计数、满仓／一级减仓／floor 状态天数。失败归因必须至少包含：

- `missed_upside_contribution`：BH 日收益为正日的 Timing−BH 累计贡献；
- `avoided_downside_contribution`：BH 日收益非正日的 Timing−BH 累计贡献；
- `incremental_fee_drag`：策略费用减 BH 费用，占初始本金；
- 各 authority 的计划、成交、阻断次数和恢复间隔。

沿用 25 交易日 block、5,000 次 bootstrap、seed `20260919`；六项共用日期抽样，报告 nominal 95% 与 Bonferroni family-wise 区间，经济阈值 0 bps。`SUPPORTED/NEGATIVE/INCONCLUSIVE` 与 `power_status` 分开；结果无论正负都不改变本轮规格或工程合入，不自动 serving。

选股效应与择时效应分开：每个 screen 的 BH 结果描述股票范围差异；每个 screen 内 Timing−BH 才是择时增量。U2 BH 较好不能写成 U2 择时有效。

## 6. Architecture / 架构与不可变产物（F-006）

新增三个轻量模块：

| 文件 | 职责 |
|---|---|
| `backend/services/position_timing/r8_proxy_screen.py` | fail-closed 打开 `daily_basic`/`bak_basic`、T−1三组四态与首次入选 |
| `backend/services/position_timing/core_tactical_timing.py` | S1/S2纯状态机，复用既有 Account/execute/费用/特征/guard |
| `backend/services/position_timing/core_tactical_benchmark.py` | 独立 prepare/run/inspect/exact retry、父唯一写者并行回放、六格报告 |

不得修改旧 `fundamental_screen.py`、`fundamental_timing.py`、`fundamental_timing_benchmark.py` 的业务合同。新代码可只读复用其纯函数和低层数据 reader，但 request 必须列出所有实际执行源码引用并在 run 时重验。

artifact namespace：

`<timing-root>/research/core_tactical_proxy_v1/`

request 在读取新策略收益前冻结 candidate、全部输入文件、源码 commit/hash、环境、screen/policy/cost/execution/statistical contract、人口／日历、source audit 和 side-effect false。chunk、bundle、receipt、manifest 首写不可变；父进程按 canonical symbol 顺序唯一 seal，worker 不写共享文件；失败 chunk 不 seal；exact retry 必须复用同一身份或返回 `ALREADY_MATERIALIZED`。

正式输出：`source_audit.json`、`screen_audit.json`、`stocks.parquet`、`pool_daily.parquet`、`fills.parquet`、`attribution.parquet`、`report.json`、`receipt.json`、`manifest.json`。不保存无界原始 payload。

## 7. Implementation Plan / 实施步骤（F-007）

本轮只分三个连续块，不拆成微阶段：

1. **合同与纯实现**：完成 F1、screen reader、S1/S2状态机和最小 direct tests；用输入分布与合成账户验证，不读取新收益。
2. **确定性管线与回放**：实现独立 benchmark，运行定向测试、固定样本 1-vs-8，然后在 WSL R8 执行 `prepare → run → inspect → exact retry`。
3. **解释与交付**：完成六项横向结果和归因，更新 F1/F2，进行多轮设计／代码／产物审核，运行 feature validator、diff/scope/CI；源码提交、PR、合入和清理按授权状态分别报告。

若 source hash、索引唯一性、候选身份、日期范围、PIT、factor/restatement 或 worker determinism 失败，只停止受影响计算并报告 typed error；这属于输入正确性，不是收益门禁。不得换股、缩期、放宽限制或回选阈值。

## 8. Verification Plan / 测试与验收（F-008）

直接测试集中到：

- `test_r8_proxy_screen.py`：双文件身份、字段／键／日期、T−1、边界、UNKNOWN、三组嵌套、旧P1不变；
- `test_core_tactical_timing.py`：共同初始买入、80/70 floor、15%两级、同日单向、no-fill状态、恢复现金、手数取整、费用、T+1、停牌／涨跌停、终值、1-vs-N。

最终验收：

1. F1 validator 与主 F2 validator 通过；Design Acceptance Matrix 无未批准 gap。
2. direct tests、相关 position_timing 小矩阵、compile/Ruff、`git diff --check`、scope/ownership通过。
3. 32股固定样本 1-vs-8逐symbol hash一致；不得按收益抽样。
4. 正式 R8 三组人口与缺失计数闭合；floor violation=0；现金／单位／费用守恒。
5. 六项结果、BH范围差异、归因、指数视图完整；缺失不删股、不缩 family。
6. inspect 与 exact retry通过；DB/network/live market/runtime/process control 均为 false。

## 9. Risks / Production Gates / Rollback / DESIGN-COMPLIANCE-001（F-009）

生产 gate 全部为 noop：无 DDL/DML、依赖安装、candidate/profile 激活、服务控制、在线发布或 backend restart。本轮回滚只回退源码／文档提交；repo-external immutable artifact 保留审计，不覆盖或删除。

- `bak_basic` 不是严格财报 PIT：名称、报告和 UI（若未来使用）必须保留 proxy 限定；本轮不上线。
- 核心 floor 可能降低避跌能力：如实报告回撤与 downside contribution，不能为改善回撤后改 floor。
- 三级嵌套筛选的入选日期不同：只在各自共同起点比较 Timing/BH；跨组 BH 仅描述，不能当纯因果效应。
- R8历史已多次被观察：全部结论 exploratory，不称 sealed holdout。
- 牛市长期持有漂移强：这不是缩短历史或只选熊市的理由。

DESIGN-COMPLIANCE-001：

1. **禁止简化交付**：六项、三组、两策略、完整R8、真实执行限制和不可变回放须全部交付；source-only或小样本不得冒充正式结果。
2. **禁止静默错误**：缺失、未知、未成交、不可清算、floor取整和代理字段限制全部typed并计数。
3. **禁止改变业务逻辑**：本轮只新增离线研究，不修改在线规则、旧研究、共享默认值或其他模块。
4. **禁止私增门禁审批**：收益、功效、最低样本、人工复核不阻止研发或合入；只有身份／因果／计算正确性错误停止无效计算。

## 10. Design Acceptance Index

| design_item | 验收条款 |
|---|---|
| F-001 | 目标、父证据、低暴露／高费用问题与三个研究问题 |
| F-002 | R8-only、position_timing隔离及零DB/runtime/其他模块写入 |
| F-003 | U0/U1/U2、T−1、UNKNOWN及bak proxy非PIT边界 |
| F-004 | S1/S2核心floor、分步减仓、恢复与账户守恒 |
| F-005 | 六项family、同股BH、选股／择时拆分及归因 |
| F-006 | 三模块、独立namespace、不可变并行产物 |
| F-007 | 三个连续实施块、不等新交易日、不按收益改规格 |
| F-008 | 直接测试、并行等价、正式回放与验收条件 |
| F-009 | 风险、生产gate和DESIGN-COMPLIANCE-001 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1；`backend/services/position_timing/core_tactical_benchmark.py` | artifact: `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/fundamental_timing_v1/bundles/52218b6026c85073f5c0b11fa14dbaa8bb2c97f15a53ecb67c6186f5a7ba1bbd/report.json`；new `report.json` attribution | DESIGN_VERIFIED | none |
| F-002 | §2；三个 `backend/services/position_timing/core_tactical_*.py`/`r8_proxy_screen.py` | test: `backend/tests/position_timing/test_core_tactical_timing.py::test_offline_side_effect_contract_is_false`；receipt flags | DESIGN_VERIFIED | none |
| F-003 | §3；`backend/services/position_timing/r8_proxy_screen.py` | test: `backend/tests/position_timing/test_r8_proxy_screen.py`；artifact: `source_audit.json` | DESIGN_VERIFIED | none |
| F-004 | §4；`backend/services/position_timing/core_tactical_timing.py` | test: `backend/tests/position_timing/test_core_tactical_timing.py`；artifact: `fills.parquet` floor audit | DESIGN_VERIFIED | none |
| F-005 | §5；`backend/services/position_timing/core_tactical_benchmark.py` | artifact: `report.json`/`attribution.parquet` six-family evidence | DESIGN_VERIFIED | none |
| F-006 | §6；`backend/services/position_timing/core_tactical_benchmark.py` | artifact: `request.json`/chunk manifests/`receipt.json`/`manifest.json` | DESIGN_VERIFIED | none |
| F-007 | §7；三个实现模块 | test: `backend/tests/position_timing/test_core_tactical_timing.py::test_prepare_freezes_contract_before_outcomes`；F1 validation receipt | DESIGN_VERIFIED | none |
| F-008 | §8 | test: `backend/tests/position_timing/test_r8_proxy_screen.py backend/tests/position_timing/test_core_tactical_timing.py`；artifact: 1-vs-8/inspect/exact retry | DESIGN_VERIFIED | none |
| F-009 | §9 | artifact: `receipt.json` side-effect flags；validation receipt: final DESIGN-COMPLIANCE-001 review | DESIGN_VERIFIED | none |
