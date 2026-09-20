# PT-NEXT-028：独立股票筛选 Alpha 基线实验详细设计

> 版本：v1.0；日期：2026-09-21；Feature tier：F1  
> 状态：`IMPLEMENTED_REPLAY_VERIFIED_INCONCLUSIVE`
> 主蓝图：[择时系统 F2 蓝图](position_timing_advice_f2_redesign_20260903.md)  
> 唯一开发权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`

`DESIGN_VERIFIED` 只表示设计闭合，不表示代码、正式回放或收益结论已经完成。本实验只研究股票筛选本身，不把择时、模型训练或在线建议混入同一试验族。

## 1. Background / Goal / 研究问题（F-001）

PT-NEXT-027 已完成小市值专用 `SELL-vs-HOLD` 动作价值研究：Ridge 的回撤端点改善有支持，但收益端点仍不可分辨，且相对父 GBDT 没有联合优势。其预注册停止规则要求停止在当前14项技术信息集上继续扫描模型、阈值、恢复天数或小盘切片，后续 alpha 预算转向独立股票筛选／选股信息。

PT-NEXT-023 曾观察到 U1/U2 的长期持有合成收益高于 U0，但三个范围的首次入选日期和人口不同，只能作为描述性线索，不能证明筛选 alpha。本轮唯一问题是：在共同决策日、共同执行日、共同持有期限和同一独立账户合同下，冻结 U1/U2 是否比 U0 产生更高的成本后长期持有收益。

终极产品仍是为特定持仓股和已确认自选股提供择时建议；本轮只验证一个上游“股票适用性”信息源是否有独立价值，不建设荐股平台，不修改在线卡片。

### 1.1 Scope / 范围

范围仅包含 `position_timing` 的一个离线 R8 筛选基线 benchmark、一个直接测试文件、本 F1 及主 F2 蓝图结果回写。允许写入的运行产物仅是 timing-owned、repo-external、内容寻址的研究 artifact。

### 1.2 Non-goals / 非目标

不训练择时或选股模型，不研发新因子，不修改筛选阈值，不接 QE/HMM/Agent，不提供股票推荐，不修改在线 L1/L1a、API、UI、数据库、数据集、active profile、其他模块或进程；不把 proxy 数据包装成严格财务 PIT，也不因中间收益追加股票池、horizon 或参数。

## 2. Frozen Input / Population / Availability（F-002）

正式输入固定为 R8 candidate：

`/mnt/wsl/aistock-qe-data-v1/releases/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate`

- `qe_dataset_manifest.json` 文件 SHA256：`07db01d8c24108ebfe2b85bc42f18f33c77fb0e09451999d7520faa08ebe1b18`；
- canonical dataset SHA256：`6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283`；
- 研究人口沿用 R8 PIT `stock_universe` 的5,144只 canonical symbol，不按今天是否存续删股；
- 日历固定2018-08-01～2026-08-31，共1,961个交易日；
- `daily_basic`、`bak_basic`、factor、restatement、停牌、涨跌停与指数文件继续逐项绑定不可变身份。

每个决策 T 只使用上一全局交易日 T−1 的筛选字段和 T 当日已经可见的 PIT 资格。缺失保持 `UNKNOWN`，不填0、不向未来或无限向前填补。prepare 必须在读取本轮收益前完成 candidate、factor、restatement、proxy source 和代码身份审计；身份或计算正确性失败只使受影响研究 fail closed，不构成收益、样本量或审批门禁。

## 3. Frozen Screens / 不新增阈值（F-003）

直接只读复用 `r8_proxy_screen.py` 的三个嵌套范围及其 hash，不重写公式：

| screen | 冻结定义 | 限制 |
|---|---|---|
| U0 `SIZE_50_500B_V1` | T−1 总市值50～500亿元 | 基础可投资范围 |
| U1 `VALUE_LIQUIDITY_PROXY_V1` | U0＋换手率0.5～15、0<PE_TTM≤60、0<PB≤8 | 估值／流动性代理 |
| U2 `BAK_GROWTH_PROXY_V1` | U1＋`rev_yoy/profit_yoy/gpr/npr`均为正 | 日快照增长／盈利代理 |

`bak_basic` 必须始终标记为 `BAK_BASIC_DAILY_SNAPSHOT_PROXY_NOT_FINANCIAL_PIT`。它不满足 PT-NEXT-022 P2/P3 的季度、披露时间、修订版本、ROE和现金流合同；本轮不得将 U2 表述为严格业绩加速或质量因子。既有阈值已经在历史研究中被观察，全部结论最多为 exploratory，不称 sealed holdout。

## 4. Common Cohorts / Account / Execution（F-004）

### 4.1 共同决策日

取 R8 每个自然月的第一个全局交易日为决策日 T。每个 T 同时评价 U0/U1/U2，不使用各股票首次入选日，也不因某组信号晚出现而改变其他组起点。每个 `(symbol,T)` 只有一份市场路径，screen 只是冻结成员标记。

### 4.2 独立账户

- 每个 `(symbol,T)` 使用独立500万元现金账户，无杠杆、追加资金、现金利息或跨股票调拨；
- T 屏幕通过后，于 T+1 收盘提交一次全额买入意图；只服从现金、board lot、停牌、方向性涨停及数据完整性，不加追价 guard；
- T+1 未成交时该事件保持现金、收益为0，不以后续日期补买，不删除该事件；
- 价格使用 raw close 执行，持仓使用 Qlib 虚拟复权单位；`adjusted OHLC=raw×factor`、下游 factor=1，不模拟券商账户级分红送转或配股清算；
- 费用复用 componentized parent-order cost policy；无市场冲击。

主估值不伪造终值卖出：在固定 horizon 的 nominal close 计算持仓 mark-to-market，并扣除一笔按该时点名义金额估算的卖出费用，字段明确为 `net_mtm_after_estimated_exit_cost`。另报告该日及随后最多5个交易日的真实 `SELL` 可执行状态和最早可卖延迟；不得把估值写成已成交。价格未知时事件为 `TERMINAL_VALUATION_UNKNOWN`，不能填0或删除。

## 5. Horizons / Outcomes / Risk Diagnostics（F-005）

持有期从实际 T+1 买入日计：

- **唯一正式端点**：120个交易日后的成本后净 MTM 收益；
- **诊断端点**：20、60、252个交易日，回答信号衰减，不参与 `SUPPORTED/NEGATIVE` 分类，不据此选择新持有期；
- 每个 horizon 报入选、UNKNOWN、T+1成交、现金未成交、终值可估值、终值不可估值和可卖延迟计数；
- 每个事件报告成本后收益、期间最大回撤、费用、持仓天数、执行和终值 typed 状态；
- 沪深300同期收益只作全市场背景，不进入三项正式选胜 family，不替代同范围比较。

若某月某 screen 的任一已买入账户终值不可估值，该 screen-month-horizon 为 `COHORT_VALUATION_INCOMPLETE`，不进入均值且必须在覆盖报告中计数；T+1合法未成交的现金账户收益0仍进入。这避免只保留成功买入或终值完整的有利样本。

## 6. Comparators / Formal Family / Statistics（F-006）

每个可评价月先等权计算各 screen 的120日事件收益均值，再形成三个共同月份差：

1. `U1_MINUS_U0_120D`；
2. `U2_MINUS_U0_120D`；
3. `U2_MINUS_U1_120D`。

三项是唯一正式 family。对共同月份差做12个月 circular block bootstrap，5,000次，seed=`20260921`；同时报告 nominal 95% 和 Bonferroni 双侧 `1−0.05/3` 区间。经济阈值冻结0 bps：family-wise lower>0 为 `SUPPORTED`，upper<0 为 `NEGATIVE`，其余为 `INCONCLUSIVE`；`power_status` 独立记录，未冻结 Oracle 尺度时为 `NOT_COMPUTABLE`。

有效区间至少需要两个完整的冻结 block，即24个共同完整月份。少于24个月时不得缩短 block、切换 IID bootstrap 或输出退化区间，统一报告 `INCONCLUSIVE + UNDERPOWERED / INSUFFICIENT_COMMON_MONTHS_FOR_FROZEN_BLOCK_BOOTSTRAP`。这是推断可计算性约束，不改变股票池、阈值、horizon 或运行资格。

20/60/252日、逐股胜率、MDD、指数、年份和股票池切片全部 `diagnostic_only=true`，不得反向选择 screen、阈值、horizon 或子人口。由于三个 screen 和完整 R8 已被历史研究观察，即使正式端点有正下界，结论仍是 exploratory，不自动进入在线选股或择时。

## 7. Architecture / Artifacts / Exact Retry（F-007）

只新增一个轻量 benchmark 与一个直接测试文件：

| 文件 | 职责 |
|---|---|
| `backend/services/position_timing/selection_alpha_benchmark.py` | prepare/run/inspect/verify-parallel、共同月度事件、统计与不可变产物 |
| `backend/tests/position_timing/test_selection_alpha_benchmark.py` | 合同、因果、账户、覆盖、统计、身份及副作用测试 |

复用 `r8_proxy_screen`、`DailyCandidate`、`pattern_close_cash_replay`、factor/restatement审计和既有 immutable publish/check helpers；不新建平台、scheduler、worker服务、数据库表、API或页面。WSL原生ext4固定8个 `spawn` 进程、128股chunk、最多16个在途任务，worker纯计算、父进程按 canonical symbol 顺序唯一写入。

artifact namespace：

`<timing-root>/research/selection_alpha_baseline_v1/`

正式 bundle 至少包含 `request.json`、`source_audit.json`、`events.parquet`、`cohort_monthly.parquet`、`report.json`、`receipt.json`、`manifest.json`。request 绑定代码、candidate、screen/cost/execution/statistical contract及所有输入 hash；chunk和bundle首写不可变。相同输入 exact retry 返回相同身份或 `ALREADY_MATERIALIZED`，不得覆盖历史 artifact。

## 8. Causality / Leakage / Boundaries（F-008）

- screen 只读取 T−1 数据；T+1 close 只能在 T 决策后成交；
- horizon 价格只用于事后标签／评价，不进入 T 的 screen 或买入决定；
- 没有 Oracle、模型训练、标签回选、测试收益特征、未来成分股回填或当前存续股过滤；
- source preflight 前 `outcomes_read=false`，run 完整复核后才标 `RUN_AFTER_FULL_SOURCE_PREFLIGHT`；
- 不读取数据库、实时行情或网络API，不修改 R8、active profile、QE/HMM/Selection/Advisory/Paper/MiniQMT、N0、timing registry/current、card/event/alert/order或服务进程；
- 研究仓库 GitHub 协作网络与行情／数据网络分开记录。

## 9. Implementation Plan / 三个连续块（F-009）

1. **合同与纯实现**：冻结本文，新增 cohort-event 纯函数、不可变 request 和定向测试；先用手算小样本验证现金、费用、限制和horizon语义。
2. **pilot 与正式回放**：固定 canonical 前64股 pilot 和前8股1-vs-8等价，只验证正确性和吞吐、不按收益改规格；随后在 WSL/R8 完成全5,144股 `prepare → run → inspect → exact retry`。
3. **审核与交付**：核查完整人口、月度覆盖、三项family、诊断边界和副作用；多轮修复后更新F1/F2，运行直接测试、L0、classifier、CI并按授权合入清理。

三个块是一项连续任务，不等待最新交易日，不把约16小时当必须耗尽的时长；确定性工作提前完成即可提前交付。

## 10. Verification Plan / Acceptance（F-010）

直接测试至少覆盖：

1. 每月共同决策日、T−1筛选、T+1执行和20/60/120/252成熟边界；
2. U2⊆U1⊆U0、UNKNOWN不当FAIL、screen定义/hash不被复制改写；
3. 500万元现金守恒、board lot、最低佣金、涨停阻买、停牌、买入失败保持现金；
4. raw执行/adjusted估值、终值估值不冒充卖出、跌停可卖延迟、最大回撤；
5. 不可估值使整个月screen cohort typed incomplete，不做幸存者筛选；
6. 三项120日family精确、诊断horizon不进入family、bootstrap分类与无Oracle功效状态；
7. request/source/chunk/bundle hash、代码漂移、路径逃逸、exact retry及8进程等价；
8. `database/network/live_market/runtime/process/other_module_write=false`。

最终还需 Ruff/compile、相关小矩阵、F1/F2 validator、`git diff --check`、classifier `workflow_gate=passed/unexecuted_test_files=[]` 和 CI 全绿。测试通过只证明冻结实现符合合同，不保证数据PIT绝对完备或实盘盈利。

## 11. Risks / Stop Rule / Production Gates（F-011）

主要风险是：U1/U2已经被观察、proxy并非严格财务PIT、嵌套screen差值不是单因子因果效应、120日cohort重叠、牛市长期持有漂移、最低佣金与不可成交对小账户的影响。通过共同月份、块bootstrap、独立账户、typed覆盖和exploratory限定披露，不通过增加模型或门禁掩盖。

若三项均无正的family-wise下界，停止扫描现有 U0/U1/U2 阈值和代理字段组合；下一步应提出一个真正独立的信息源或等待严格财务PIT，而不是调整PE/PB/增长阈值。若出现支持，也只冻结为 PT-NEXT-029 的“筛选后BH vs 同screen冻结择时”新假设，不追认为在线alpha、不在本轮实现组合。

生产 gate 全部 noop：无 DDL/DML、依赖安装、dataset/profile 激活、在线发布、服务控制或后端重启。本任务只写 timing-owned repo-external immutable artifact；回滚源码不删除历史证据。

## 12. Design Acceptance Index

| design_item | 验收条款 |
|---|---|
| F-001 | 独立筛选alpha问题与PT-NEXT-027停止规则 |
| F-002 | R8身份、PIT人口、T−1可见性与source preflight |
| F-003 | 冻结U0/U1/U2且proxy不冒充财务PIT |
| F-004 | 共同月度cohort、500万元账户和T+1真实买入限制 |
| F-005 | 120日正式端点、三诊断horizon与终值MTM边界 |
| F-006 | 三项正式family、共同月份和12月块bootstrap |
| F-007 | 一个benchmark、一个测试、8进程与不可变artifact |
| F-008 | 无未来泄漏、无DB/network/runtime/其他模块副作用 |
| F-009 | 三个连续实施块、pilot不调参与正式全量回放 |
| F-010 | 直接测试、validator、classifier、CI与验收合同 |
| F-011 | 风险、停止规则、生产gate与回滚 |

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1；主蓝图§9.31 | `backend/tests/position_timing/test_selection_alpha_benchmark.py` | IMPLEMENTED_VERIFIED | none |
| F-002 | §2；`selection_alpha_benchmark.prepare/load_request` | `backend/tests/position_timing/test_selection_alpha_benchmark.py` | IMPLEMENTED_VERIFIED | none |
| F-003 | §3；`r8_proxy_screen.py` | `backend/tests/position_timing/test_selection_alpha_benchmark.py` | IMPLEMENTED_VERIFIED | none |
| F-004 | §4；`selection_alpha_benchmark._replay_symbol_task` | `backend/tests/position_timing/test_selection_alpha_benchmark.py` | IMPLEMENTED_VERIFIED | none |
| F-005 | §5；event/report schema | `backend/tests/position_timing/test_selection_alpha_benchmark.py` | IMPLEMENTED_VERIFIED | none |
| F-006 | §6；`selection_alpha_benchmark._bootstrap_monthly/_build_report` | `backend/tests/position_timing/test_selection_alpha_benchmark.py`；正式 `report.json` | IMPLEMENTED_VERIFIED | none |
| F-007 | §7；benchmark/chunk/bundle | `backend/tests/position_timing/test_selection_alpha_benchmark.py`；artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/selection_alpha_baseline_v1/bundles/4c69de0dc4b9a6d8973deb4deeab70156bf7e6fcd77668ae8f4d4167aea831b0/manifest.json` | IMPLEMENTED_VERIFIED | none |
| F-008 | §8；request/receipt side-effect fields | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/selection_alpha_baseline_v1/bundles/4c69de0dc4b9a6d8973deb4deeab70156bf7e6fcd77668ae8f4d4167aea831b0/receipt.json` | IMPLEMENTED_VERIFIED | none |
| F-009 | §9 | `backend/tests/position_timing/test_selection_alpha_benchmark.py`；artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/selection_alpha_baseline_v1/bundles/4c69de0dc4b9a6d8973deb4deeab70156bf7e6fcd77668ae8f4d4167aea831b0/manifest.json` | IMPLEMENTED_VERIFIED | none |
| F-010 | §10 | `python -m pytest backend/tests/position_timing/test_selection_alpha_benchmark.py -q`；31项相关小矩阵通过 | IMPLEMENTED_VERIFIED | none |
| F-011 | §11 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/selection_alpha_baseline_v1/bundles/4c69de0dc4b9a6d8973deb4deeab70156bf7e6fcd77668ae8f4d4167aea831b0/report.json` | IMPLEMENTED_VERIFIED | none |

## 14. Initial Review

第一轮事实审查确认 R8 的冻结身份是 `qe_dataset_manifest.json`，不是部署状态文件 `direct_monthly_state.json`；WSL manifest SHA 与既有合同一致。第二轮方法审查将“首次入选连续账户”改为共同月度cohort，并把终值卖出与MTM估值分离，避免用不可成交日伪造现金。第三轮范围审查把正式family限制为三个120日screen差，其他horizon、风险和指数只作诊断，不新增模型、平台或跨模块依赖。

## 15. Formal Replay / Review Result

最终源码提交为 `fe3864ae4c342917ae3f01630be61439dedb9338`，正式 request 为 `4c69de0dc4b9a6d8973deb4deeab70156bf7e6fcd77668ae8f4d4167aea831b0`，bundle manifest SHA256 为 `6c3baa61d2e5af59ea3d67d65936441881c0af2cc321e97d504369bf6edbc1e4`。WSL 原生 ext4、R8、8进程完成5,144股、97个决策月、41个chunk和776,584条事件；64股串并行逐股结果 `EXACT`，audit SHA256 为 `6b5bcc4505d74d15aa4f53905a3da6abe601f712289fea0fbceca651527b8b38`。独立 inspect 为 `VERIFIED`，exact retry 为 `ALREADY_MATERIALIZED`。

正式120日三项结果均没有支持：

| comparison | 共同完整月 | 点估计 / family-wise区间 | 结论 |
|---|---:|---:|---|
| U1−U0 | 13 | 不输出退化区间 | `INCONCLUSIVE / UNDERPOWERED` |
| U2−U0 | 12 | 描述性点估计约+60.82 bps；不输出退化区间 | `INCONCLUSIVE / UNDERPOWERED` |
| U2−U1 | 26 | +81.16 bps / [-15.90,+174.58] bps | `INCONCLUSIVE` |

首个完整bundle暴露一个统计实现缺陷：当共同月恰为12、冻结block也为12时，circular block只会循环排列同一组月份，区间错误塌缩为点。该bundle不可作为支持证据；修复不缩短block、不切换IID bootstrap，而是要求至少24个共同完整月，并以新代码、新request和新bundle全量重放。最终 `result_class=EXPLORATORY_SELECTION_ALPHA_NO_SUPPORTED_SCREEN`、`selected_for_live=0`。

覆盖而非买入能力是当前主要瓶颈：120日U0/U1/U2成交率约98.73%/99.14%/99.19%，但U0的97个月中只有13个月完整，78个月因至少一个已入选事件终值不可估而整体typed incomplete；U1/U2完整月为27/48。不得删除退市、停牌或缺终值股票来制造更窄区间。按停止规则，不再扫描现有U0/U1/U2阈值或proxy字段组合；下一优先级先设计不依赖幸存者删除的退市／终止上市终值authority或引入真正独立且严格PIT的信息源，再决定是否开启新的选股假设。`bak_basic`仍只是日快照代理，不能被本次正点估计包装成业绩增长alpha。

request/receipt记录 `database/network/live_market/runtime/process/other_module_write=false`，`outcomes_read`仅发生在完整source preflight之后；没有模型训练、在线发布、服务控制或后端重启。
