# 择时策略全市场与核心指数股票池基准验证设计

> 版本：v1.1；日期：2026-09-15；Feature tier：F1（`position_timing` 单模块离线研究）  
> 状态：`IMPLEMENTED_SOURCE_PREFLIGHT_BLOCKED`  
> 任务：`PT-NEXT-020 / PATTERN_UNIVERSE_BENCHMARK_V1`  
> 所属蓝图：[持仓与自选池择时建议系统](position_timing_advice_f2_redesign_20260903.md)  
> 父研究：[自选股与持仓股形态择时研究](position_timing_pattern_strategy_design_20260911.md)  
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`

## 1. Background / 目标与研究问题

终极目标仍是为用户已经选定的持仓股和显式自选股提供能够增加成本后收益、降低错误交易或改善风险收益比的明确择时建议。`PT-NEXT-018` 已在 64 只评价股上完成底部企稳、回踩确认和放量加速退出的冻结策略回放，但九项比较全部为 `INCONCLUSIVE`、`selected_trial_count=0`。在引入 QE 选股之前，本任务先回答该冻结策略在更广泛 A 股人口及不同市值/板块股票池中是否存在稳定的同股择时增量，避免把未来 QE 的选股收益误归因于择时。

本任务不是对旧结果的追加调参，也不把扩样冒充首次未见时间验证。研究者已经观察过相同市场日期，故结果固定标记为 `EXPLORATORY_CROSS_SYMBOL_EXTERNAL_VALIDITY_NOT_TEMPORAL_HOLDOUT`。无论结论为 `SUPPORTED`、`NEGATIVE` 或 `INCONCLUSIVE`，都形成不可变证据；收益方向不阻止工程合入，也不自动发布 serving。

主研究问题只有一个：冻结 R0/P 完整策略在六个 candidate-bound PIT 股票池上，相对同股直接长期持有的逐腿成本后日均财富增量是否为正。沪深300只回答市场背景问题。父研究中未入选的 Q 与 Enhanced 不在本轮扩样中重复运行，避免无助于主问题的三倍计算与 57 个模型加载。

截至 2026-09-15，管线实现和 r5 candidate/配股/复权重述 reader 已完成；r5 文件侧审计确认三条配股事件及 `300506.SZ`、`688109.SH` 两条重述序列身份闭合。生产源只读快照仍在收益读取前被 `002352.SZ/2024-11-07` 与 `600989.SH/2024-07-24` 的 `market.dividend` 同期经济值冲突阻断。因此尚无 PT-NEXT-020 正式 request、收益 bundle 或策略结论；该状态是数据 authority 缺口，不是 `NEGATIVE` 或 `INCONCLUSIVE` 研究结果，也不得通过删股、缩短历史或放宽 10 bps 阈值绕过。

## 2. Scope / 范围与 Non-goals

本任务目标交付一个 F1 设计、一条 timing-owned 不可变 `prepare → run → inspect → exact retry` 管线、全市场及五个核心指数池真实历史回放、直接测试、结果回填和主蓝图进度同步。源 authority 不闭合时允许先合入 fail-closed 管线，但不得把未执行的正式回放写成已完成。

写入范围限于：

- `backend/services/position_timing/`；
- `backend/tests/position_timing/`；
- 本设计与 `position_timing` 主蓝图；
- 如确认现有实现缺陷，允许写同一模块 BUG/Issue 工作流元数据，但不得修改其他模块源码。

明确不做：QE/Selection/Advisory/Watchlist/Paper/MiniQMT 源码修改，QE 模型训练，HMM/Agent/新闻融合，分钟信号，API、页面、卡片、提醒、worker、scheduler、数据库表、DDL/DML、数据抓取、candidate 重建或激活、自动交易、registry/current/serving 写入。本任务不把全市场独立 sleeve 汇总宣称为可直接实盘的 5,144 股组合，也不新建通用回测平台、任务平台或监控平台。

## 3. Architecture / 最小架构与允许复用的 API

```text
r5 candidate manifest + candidate-bound PIT sidecars + 000300.SH 日线
                              ↓
     source-only factor/corporate-action/rights/suspension preflight
                              ↓
       同一 symbol 只读取/构建特征一次，按相关 PIT pool 分别回放账户
                              ↓
                 R0/P 主路径 + 同股 BUY_AND_HOLD
                              ↓
     分块 Parquet → 日级聚合 → 区间/暴露/现金拖累/回撤/费用
                              ↓
     immutable request / coverage / receipt / manifest / exact retry
```

允许直接复用的现有接口：

| 能力 | 权威实现与复用方式 |
|---|---|
| candidate 日线与全局交易日 | `action_value_data.DailyCandidate.open/bars/coverage`；只读明确 r5 路径 |
| R0/P 连续路径 | `pattern_research.replay_full_policy_symbol`；候选与 `BUY_AND_HOLD` 共享 source、资金、公司行动和逐腿费用 |
| 形态特征 | `pattern_strategy.pattern_feature_frame`；不复制第二套形态公式 |
| 父研究谱系 | `pattern_research.inspect_pattern_bundle` 递归核验父 bundle 与 manifest，只绑定产生 R0/P 的冻结请求和代码身份，不读取父结果来重选规则 |
| 公司行动 | `CorporateActionBook`、`freeze_corporate_action_snapshot` 与 `apply_pattern_corporate_action_policy`；只读源并写 timing-owned snapshot |
| 停牌 | r5 candidate 的 `suspend_d_daily_candidate_v2`，由 `DailyCandidate.bars` 显式映射为 `is_suspended`；不另建第二套 snapshot |
| 配股 | `pattern_rights_issue.open_rights_issue_authority` 与统一 `NEVER_SUBSCRIBE` policy；factor 不推断账户认购 |
| 复权重述 | `pattern_adj_factor_restatement.open_adj_factor_restatement_authority/audit_candidate_adj_factor_restatement`；只证明 r5 数据源修订，不把 factor 变化解释为账户已发生公司行动 |
| 不可变发布 | `PositionTimingArtifactStore._publish_immutable`、`file_reference`、`canonical_sha256` 与现有原子目录发布/inspect 方式 |

本任务不新增“从父 pattern bundle 恢复全部 walk-forward 模型”的 loader。父 bundle 只通过既有 inspect 递归验真并绑定 canonical manifest 与 request 身份；缺失、额外文件或 hash 漂移仍由父 inspect fail closed。

## 4. Contracts / 冻结输入、人口与时钟

### 4.1 candidate 与父证据

candidate 显式固定为：

`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260915-r5-candidate`

根 manifest 文件 SHA256 为 `7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1`，dataset identity 为 `59b92120a4fb52fdde8a3db57337eb9af3810d28881861db7d9e3d028987407f`。父 request 为 `014547fa46994c7b01b809580a8db5d4cce449c723661454bb3a29e6f0d9eeb2`；父 bundle manifest canonical SHA256 为 `7481afad8bcb6bde45cfc4fbe90fc53ac14719ef048aa464d91498dce0da9541`。父结果值不用于生成本任务规格，只绑定产生冻结 R0/P 的 source、policy 与代码身份。

配股 authority canonical SHA256 保持 `4a7cdb79e968f33a000f2e9b81196986349cff26100688794b87f6a1f454f10c`，共同参与政策 canonical SHA256 保持 `4c00bef92adf0fe242531748f2b4fe38aa35208a79c2d990cef68bf7d52eb60a`。r5 复权重述 authority canonical SHA256 为 `c40f3c991ac31b570e7a739bb1898a59f12e202f2e96e9bcd8399211e5323edd`；其文件 SHA256 为 `1639b06a1e43998273ac21e6d5593671c0be934fd24714f8ebfe920bfe3a96ad`，审计 SHA256 为 `48b78cc7237fc02d4c0d840f3ef63c7cb9ddc6ef46325330b8b7acc3b2dd32ac`。研究范围为 candidate 的 `2018-08-01..2026-08-31` 共 1,961 个全局 session；R0/P 的可用起点继续使用父合同的 756-session 初始边界，不等待新交易日。

### 4.2 六个股票池

股票池身份全部来自 r5 manifest 的 `st_pit_manifest.index_membership_sidecars`；六个 sidecar 的内容身份相对 r4 未变化：

| pool_id | sidecar SHA256 | intervals | historical symbols | membership start |
|---|---|---:|---:|---|
| `stock_universe` | `8979d78a7c3b322a3d9696f21e880dcd79c9a0bff0aa59d160f6a1f957640392` | 5,449 | 5,144 | 2018-08-01 |
| `csi300` | `c3158b8205e6cfd1af265c756a0be81df3fc4b28e95f22e2f854991695508807` | 591 | 539 | 2018-08-01 |
| `csi500` | `bf6f74990d55cf880c179731ab9ff8080c09a4383e6652eb06f21ab353dc24f8` | 1,300 | 1,134 | 2018-08-01 |
| `csi1000` | `3821b96179a4e8fd07727347dbd8b9b91ac120e0a76fc8097ab1e46dcbffae79` | 2,629 | 2,341 | 2018-08-01 |
| `star50` | `5eeba8833a5b2be3523815718d0cbae2656a316e6da11980193cfd434c58ff39` | 127 | 123 | 2020-08-04 |
| `star100` | `7906e7ea7fc5ad9caa602a6b11166454d855e5ad84c96c5cd62758618ceddc54` | 191 | 184 | 2023-08-07 |

reader 必须核验 manifest 的 path/hash/size、三列 schema、股票代码、日期顺序、无重叠且区间包含于 candidate 日历。指数 sidecar 表示历史指数成员，不自行承诺 ST/退市等全市场 PIT 过滤；正式逐日有效人口必须取 `index_membership_active AND stock_universe_pit_active`，不得要求一个指数区间被单个全市场区间完整包住，也不得把指数 sidecar 单独解释成可交易人口。不得使用当前成分股回填历史，不读取 active profile，也不在数据库成员和 candidate sidecar 之间静默切源。

每股 sleeve 从该股票首次进入对应 PIT pool 且达到策略可评价时钟的日期开始纳入聚合；此前日期为 `NOT_YET_IN_POPULATION`，不得当作零收益。离开指数后不得新开仓；已经持有的 candidate 与 `BUY_AND_HOLD` 都继续沿相同公司行动、可交易和退出语义演化，不因指数调出增加强制退出。重新进入后恢复新开仓资格。该处理把指数成员用作当时可选人口，不把指数定期换样混入择时动作。

### 4.3 策略、基准与终端

主要候选固定为父研究的 R0/P 完整策略。主要 comparator 为同股 `BUY_AND_HOLD`：每个 sleeve 在首次合法执行日以相同 100,000 元现金尽量买入并长期持有。两者都使用 raw 人民币成交、相同 board lot、停牌/涨跌停、公司行动、配股政策和 componentized cost policy。

研究终点必须明确区分终值计价与真实终端清仓。实现先核对现有 `replay_full_policy_symbol` 是否在共同 terminal 执行卖出、顺延最多 5 个全局交易日并收取卖出腿费用；如果只 mark-to-market，则登记同模块 BUG 并新增对称的 `TERMINAL_LIQUIDATED` 口径。旧 artifact 保留原样；新研究只使用修正后且候选/基准完全相同的终端语义。任一路径无法在最大顺延期内清仓时均记 `UNAVAILABLE_AT_HORIZON`，不得用最后价格伪造成交。

沪深300 `000300.SH` 使用 r5 `index_context` 的 close-to-close 价格指数，按每个股票池有效评价日期归一化，只标 `MARKET_CONTEXT_PRICE_INDEX_NOT_INVESTABLE_TOTAL_RETURN`。跑赢它不足以证明择时有增量。

### 4.4 父诊断与成本边界

父 Q/Enhanced 已在 PT-NEXT-018 中留下不可变诊断且没有入选。本任务只读取父 bundle 身份，不把未入选路线扩到全市场重新比较，也不据新股票池重训、重选或改变调度。六项 `P_MINUS_BUY_AND_HOLD` 是唯一 family；池间差异与沪深300背景仅为 `diagnostic_only`。所有结果保持 `selected_trial_count=0`，本任务不发布模型或规则。

本任务只运行一个父订单的冻结主成本情景，逐腿应用当前 componentized cost policy。2/3 父订单与每腿额外 5/10 bps 已在父研究中用于诊断；若在本轮对六个股票池重新执行完整连续账户，会把全市场回放放大约五倍且不改变主要问题的 estimand，因此明确不纳入 PT-NEXT-020，不以近似费用重算冒充路径敏感性。后续若成本政策或券商口径变化，必须以独立、预注册的新 request 重放，不得覆盖本轮主情景，也不得据此删除股票。

## 5. Estimand / 收益聚合与统计分类

对每个 pool、每个有效 sleeve 日先计算：

```text
timing_increment_t,i
= wealth_change(R0/P, t, i) - wealth_change(BUY_AND_HOLD, t, i)
```

再在同一全局交易日对当日有效 sleeves 等权平均，并对日序列做 25-session circular moving-block bootstrap。六个 pool 的 `P_MINUS_BUY_AND_HOLD` 使用同一个六假设 family，family-wise 区间采用 Bonferroni `99.1666667%`；同时报告 nominal 95% 区间。阈值为成本后的 `economic_threshold_bps=0.0`：family-wise lower `>0` 为 `SUPPORTED`，upper `<0` 为 `NEGATIVE`，其余为 `INCONCLUSIVE`。`power_status` 独立记录；无同 estimand 预注册尺度时为 `NOT_COMPUTABLE`，不以 MDE 阻止研究或合入。

股票池结果重叠且共享市场日期，池间排序和差值只作异质性诊断，禁止把表现最佳的池反向定义成策略适用范围。当前时间已被既有研究观察，任何正区间也只能称“在冻结策略及扩展历史人口中获得探索性横截面支持”，不能称独立时间外证实。

每个 pool 至少报告：策略与 buy-and-hold 的累计/年化收益、日均成本后增量、nominal/family-wise 区间、沪深300同期收益、最大回撤、波动率、Sharpe、平均暴露、现金比例、现金拖累、换手、逐腿费用、等待错失上涨、提前退出后继续上涨、有效股票/日期、未成交、unknown、公司行动及终端状态。池级绝对曲线是等权有效 sleeve 研究指数，不宣称真实有限资金组合。

## 6. Immutable Artifact / 不可变产物

artifact 只写：

`<timing_root>/research/pattern_universe_benchmark_v1/requests|bundles|source_diagnostics`

CLI：

```text
prepare --timing-root --repository-root --parent-pattern-bundle --candidate-root
run --request
inspect --bundle
run --request
```

`prepare` 在任何候选收益读取前冻结源码 commit、candidate、六个 sidecar、父 bundle、策略/模型/成本/统计、公司行动、停牌、配股和复权重述身份。复权重述 authority 必须与配股 authority 绑定同一 r5 manifest/revision，且 `300506.SZ`、`688109.SH` 修订 seam 审计通过；它只说明 candidate factor 源修订，不推断账户行动。source preflight 对全部候选股票检查大于 10 bps 的 material factor interval；覆盖不足时只写 content-addressed `source_diagnostics` 并 typed fail，不生成可被误读为完整的收益 bundle。

`run` 按 canonical symbol 顺序分块执行；staging 只允许位于 timing-owned root，按 request hash 与 chunk ordinal 命名。单个 symbol 只读一次源数据并只构建一次冻结 features，但成员资格会改变新开仓 eligibility，因此必须为该 symbol 所属的每个 PIT pool 分别维护和回放连续账户，禁止把全市场已执行路径事后贴上指数池标签。R0/P 与同股基准在同一 pool path 内使用相同冻结 bars/features/membership。分块文件原子写入并记录 symbol range、row count 与 SHA256；失败重试只能复用 hash 完全相同的已完成 chunk，不能跳过失败股票。最终 bundle manifest 递归绑定 request、coverage、receipt、日级聚合、pool summary、symbol diagnostics、fills 摘要及分块明细，并在顶层 inspect 时递归复核外部分块内容。

相同 request 已有合法 bundle 时，`run` 先完整 inspect 再返回 `ALREADY_MATERIALIZED`。代码、输入、合同或 BUG 修复变化必须形成新 request；已经读取收益的失败/错误 bundle 不得覆盖、删除或冒充 technical exact retry。

所有 receipt 必须明确：`registry/current/serving_model/card/alert/order/database/runtime` 写入均为 false。changed-file scope 与调用图审计必须证明源码修改只位于 position_timing、直接测试、BUG 与本文档范围；不虚构对父 artifact、QE、Selection、Advisory、Watchlist、Paper 或 MiniQMT 全目录做前后 hash 快照。它们不属于本管线写入路径，request/receipt 绑定实际读取的父 artifact 与 candidate 身份。

## 7. Implementation Plan / 单一长任务实施块

本任务不拆成多个产品阶段，只按依赖顺序连续执行：

1. 冻结本 F1 设计并通过 validator，同时把主蓝图后续优先级改为 PT-NEXT-020；
2. 实现 sidecar/父模型不可变 reader、source-only 全市场 preflight、分块回放与 receipt；
3. 用小型 fixture 和父 64 股做旧路径数值/行为等价验证；
4. 在 clean source commit 上执行全市场 source preflight；当前已确认两条 `market.dividend` 经济值冲突，新数据缺口输出精确 handoff，不在本模块补数据；
5. source 完整后执行正式 `prepare → run → inspect → exact retry`；
6. 回填真实结果和 hash，执行三轮不同关注点的审核、修复与回归；
7. 通过 F1/F2 validator、最小本地 gate、PR CI 后直接合入；backend restart 若由 runtime catalog 推断为 required，合入后等待用户重启再做只读验收。

耗时、样本量、负结果、宽区间或 MDE 不是缩短历史、换股、调整参数、阻止工程合入或等待未来数据的理由。

## 8. Verification Plan / 验证方案

直接测试至少覆盖：

- 六个 sidecar 的 manifest/path/hash/size/schema/order/overlap/calendar 校验，且指数逐日人口与 `stock_universe` PIT 取交集；当前成分股、未知 pool 和漂移文件 fail closed；
- 首次成员日期前不进入分母，成员调出后不新开仓但已有持仓继续，重新调入后恢复买入资格；
- 同一 symbol 在多池只读取/构建特征一次，但按 pool 独立回放账户；直接反例证明调出期间不得沿用全市场路径新开仓；
- 小 fixture 上分块与非分块输出完全一致，重试只复用同 hash chunk，缺块/篡改/重复 symbol fail closed；
- R0/P 与父实现在同一 64 股、同 source、同 terminal 语义下数值一致；如 terminal BUG 被确认，旧口径和新 `TERMINAL_LIQUIDATED` 口径明确分开；
- BUY_AND_HOLD 与候选共享交易、公司行动、配股、成本及 terminal；无卖出能力不能伪造终值；
- 父 bundle 由既有 inspect 递归验真，canonical manifest/request 漂移 fail closed；本轮不加载或运行 Q/Enhanced；
- 六项主比较 family-wise 区间、95% nominal、池间 diagnostic、有效 sleeve 分母和沪深300 context 语义正确；
- coverage 不完整只能得到 typed incomplete/`INCONCLUSIVE`，不能通过删股恢复 `SUPPORTED`；
- prepare 不读取本任务收益，r5 candidate/配股/复权重述 authority 及其审计均 hash-bound，bundle 不可变，inspect 能发现任意篡改，exact retry 不写第二份结果；
- N0、QE、Selection、Advisory、Watchlist、Paper、MiniQMT、registry/current/card/alert/order/DB/runtime/serving 写入均为 false。

本地最终 gate：changed-file compile/ruff、直接测试、一次完整 `python -m nox -s position_timing_backend`、`git diff --check`、scope/ownership 检查、F1 validator、主蓝图 F2 validator，以及 DESIGN-COMPLIANCE-001 逐条复核。失败后只重跑失败 nodeid/`--lf`，稳定后再运行一次完整矩阵，禁止机械重复同一日志冒充多轮审核。

## 9. Design Acceptance Index

| design_item | acceptance requirement |
|---|---|
| F-001 | 单一外部有效性问题、终极目标、探索性限制和不按收益阻碍工程明确 |
| F-002 | r5 candidate、父 bundle、日期、六个 sidecar、配股、复权重述与共同政策 hash 全部冻结 |
| F-003 | PIT 动态成员、有效 sleeve 分母、调出后持仓与再入场语义无幸存者偏差 |
| F-004 | R0/P 与同股 BUY_AND_HOLD 是唯一主基准，沪深300只作价格指数背景 |
| F-005 | 父 artifact 只读且递归验真；未入选 Q/Enhanced 不在本轮扩样或进入主策略选择 |
| F-006 | terminal、逐腿成本、公司行动、停牌、涨跌停和配股账户语义对称闭合 |
| F-007 | 六项主 family、nominal/family-wise 区间、诊断和结论措辞冻结 |
| F-008 | 单读多池、分块有界内存、immutable prepare/run/inspect/exact retry 闭合 |
| F-009 | 写入只限 position_timing，QE/其他模块/DB/runtime/serving 零写入 |
| F-010 | 直接测试、三轮审核、设计符合性、PR/CI/合入与用户重启边界明确 |

## 10. Design Acceptance Matrix / 设计验收矩阵

`DESIGN_VERIFIED` 只表示本轮规格闭合，不表示代码、全市场 source coverage、收益证据或 runtime 已完成。实现和正式回放后必须以实际代码、测试、bundle 与 hash 更新本矩阵。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文 §1、§5 | `python -m pytest backend/tests/position_timing/test_pattern_universe_benchmark.py::test_research_question_and_exploratory_scope_are_frozen -q` | DESIGN_VERIFIED | none |
| F-002 | 本文 §4.1、§4.2 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_frozen_authorities_match_r5_candidate`、`::test_r5_candidate_parent_and_authority_identities_are_frozen` | DESIGN_VERIFIED | none |
| F-003 | 本文 §4.2、§5 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_pit_membership_projection_preserves_population_clock` | DESIGN_VERIFIED | none |
| F-004 | 本文 §4.3、§5 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_same_stock_buy_and_hold_and_market_context_are_distinct` | DESIGN_VERIFIED | none |
| F-005 | 本文 §3、§4.4 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_parent_artifact_identity_is_hash_bound_and_read_only` | DESIGN_VERIFIED | none |
| F-006 | 本文 §4.3、§4.4 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_candidate_and_comparator_share_terminal_and_accounting_contracts` | DESIGN_VERIFIED | none |
| F-007 | 本文 §5 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_six_pool_familywise_classification_is_frozen` | DESIGN_VERIFIED | none |
| F-008 | 本文 §3、§6 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_chunk_manifest_and_exact_retry_are_immutable` | DESIGN_VERIFIED | none |
| F-009 | 本文 §2、§6 | `backend/tests/position_timing/test_pattern_universe_benchmark.py::test_receipt_declares_all_external_writes_false` | DESIGN_VERIFIED | none |
| F-010 | 本文 §7、§8、§11 | `python -m nox -s position_timing_backend` 与 F1/F2 validator receipt | DESIGN_VERIFIED | none |

## 11. Risks and Production Gates / 风险与生产边界

主要风险是：全市场新配股/复权区间未闭合；动态成分分母把未进入股票当零收益；长期持有的终端费用遗漏；重叠指数池被误当独立样本；全市场内存无界；按最好股票池回选；把沪深300相对收益、coverage 或工程通过包装成择时 alpha。处置分别是收益前 source audit、typed 状态、有界分块、同一六假设 family、diagnostic-only 池间比较和不可变证据措辞。

当前 source blocker 的只读证据为：`002352.SZ/2024-11-07` 同一实施日包含同一 `2024-06-30` 期间的 `cash_div=1.4` 与 `0.4`，另有 `2024-10-11` 身份的 `1.0`；`600989.SH/2024-07-24` 同一 `2023-12-31` 期间存在 `cash_div=0.3158` 与 `0.265` 且 `base_share` 不同。position_timing 无权判断其为修订、合计或独立分配，必须由 local_data authority 修复或给出可验证分类；此前不得生成收益 request。

DESIGN-COMPLIANCE-001：①不以 128 股烟测、部分股票池或 source coverage 冒充完整交付；②unknown/no-fill/source gap/terminal unavailable/分块失败均显式保留；③不修改冻结策略、父模型、用户目标、成本或其他模块；④不增加 MDE、最新日期、HMM、人工审批或收益方向门禁，只有来源、因果和身份错误阻止对应错误计算。

`production_ddl_gate=noop`，DML、dependency、client install、dataset activation、订单和 serving 均为 noop。代码位于 `backend/services/position_timing/`，即使只供离线 fresh process 使用，也以 runtime target catalog 的最终判定为准；若 `backend_restart_required=true`，源码合入后状态保持 `pending_user_restart`，后端重启只由用户执行。
