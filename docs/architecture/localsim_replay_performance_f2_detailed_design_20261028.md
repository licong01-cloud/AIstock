# LocalSIM 连续离线回放性能 F2 详细设计

日期：2026-10-28。Feature tier：F2；风险：T3。状态：设计及实现中。

用户授权：按批量行情准备、重复计算优化、独立账户并行的顺序开发，完成多轮审核、定向回归、L0及最终HEAD CI后可以直接提交合入。当前181日研究继续使用其冻结工作树，不热更、停止、迁移或重置。

上位设计：[双模式设计](localsim_human_operation_dual_mode_f2_detailed_design_20261026.md)、[既有择时接入](localsim_position_timing_integration_f2_detailed_design_20261024.md)。唯一开发权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`。

## 1. Background / 背景

现行研究以日为循环、以账户为内循环，行情在同日13账户间共享。CandidateReader仍逐股逐字段重复扫描日历、映射bin、解析报价跨度及停牌文件，并逐日重算累计内容pin。账户循环重复定位可见快照、构造完整历史前缀、复制账本和估值。现有约450秒/13账户日是测量基线，不是优化后承诺。

2026-10-28只读抽样：97股、22,989根分钟bar，独立新Reader行情准备及校验约55.2秒；未覆盖运行进程累计历史pin，不能当成精确线上阶段占比。2–3倍是首轮工程目标，经济效果不属于性能验收。

## 2. Scope / 范围与写入清单

独立工作树：`F:/Dev/AIstock_worktrees/localsim-replay-performance-20261028`；分支：`feat/localsim-replay-performance-20261028`；初始origin/main：`84c580d14a4d0b3d41d2efc7ae62bbcc0b3919f4`。

先登记以下精确 allowed_write_scope；扩展先更新本节：

- `docs/architecture/localsim_replay_performance_f2_detailed_design_20261028.md`
- `docs/analysis/localsim_replay_performance_implementation_acceptance_20261028.md`
- `backend/services/simulation_execution/localsim/research_performance.py`
- `backend/services/simulation_execution/localsim/dual_mode_research.py`
- `backend/services/simulation_execution/localsim/human_portfolio.py`
- `backend/services/simulation_execution/localsim/native_research.py`
- `backend/services/simulation_execution/localsim/timing.py`
- `backend/services/simulation_execution/localsim/timing_portfolio.py`
- `backend/tests/simulation_execution/test_localsim_replay_performance.py`
- `backend/tests/simulation_execution/test_localsim_dual_mode_comparison.py`
- `backend/tests/simulation_runtime/test_daily_trading_context.py`??????DEV??????????????????/???????

任务记录、基准输入引用及回执仅写`X:/AIstock_temp/localsim-replay-performance-20261028/`；WSL使用`/mnt/x/AIstock_temp/localsim-replay-performance-20261028/`。不复制大型数据或模型。

### 2.1 共享接口归属

原双模式设计§11规定：缺正式公共接口时交对应owner，不跨模块修改。目前CandidateReader无批量准备接口。需要其owner提供§5.1的最小无策略读取合同，或者用户明确扩大本任务写入范围。未获得授权前不修改`backend/services/position_timing/`、数据模块或比较器；LocalSIM其余优化继续独立实施。共享内核规则、阈值和研究人口始终不在修改范围。

用户随后明确选择“共享接口仍交择时窗口实现”。因此本PR交付LocalSIM消费接缝、计算优化、账户并行及各自完整验收；共享Reader的真实批量读取实现和其提速验收由owner独立PR交付。F-002按消费侧已完成范围核验，外部生产能力不伪报完成；缺接口时显式不可用的行为是正式合同，不是逐日静默降级。整体跨模块批量优化仍保持待owner交付状态。

## 3. Non-goals / 边界

不改TWAP默认政策、费用、OMS、撮合、现金、持仓、T+1、停牌及方向性涨跌停语义；不创建第二套账户或撮合。不改选股算法、QE、Advisory、HMM或StrategyPackage资产；不训练、不补数、不写DB、不抓行情、不控制用户backend/worker/scheduler。人工提醒不等于成交。不把批量物理读取变成跨日资格或账户结果复用。

## 4. Architecture / 架构

1. 主协调器独占既有study-writer锁，先完成整批来源检查。
2. 有界准备workspace按交易日窗口预取只读物理输入；缓存键至少绑定candidate完整SHA、组件SHA、股票、频率及区间。日语义仍交唯一CandidateReader.market。
3. 当日股票池、投影、持仓并集及C/D/T单独绑定；持仓只取各账户前一已提交日。未来窗口输入不能进入当前核心。
4. 优化后的账户计算仍调用唯一择时evaluate、原生TWAP和原生账户链。在同一轮询、派发和观察时刻复用只读市场信息与可见快照。
5. 账户任务可以在有界进程池独立计算；同一账户的日序列、SELL/BUY顺序及反馈序列保持不变。
6. 只有父协调器按原账户次序执行原生经济事务和不可变日journal。子进程不写账户产物；未来账户不能越过前缀失败先提交。

计算批次与事务粒度分离：多日准备、逐事件状态推进、完整日原子提交。不能以月底才落盘换性能或丢失失败前缀。

## 5. Contracts / 接口与等价合同

### 5.1 最小共享批量读取合同

建议在现有CandidateReader增加显式有界准备能力，保留现有构造和market/values/verify_pins：

- `prepare_window(start, end, symbols)`：复用日历日期偏移、只读映射、报价跨度索引和停牌表索引；不应用PIT选股，不生成价格偏好或交易信号。
- `market(symbol, day)`继续承担所有逐日检查，返回与原接口完全相同的MarketView、bar、来源身份、价格精度、可见时间和异常语义。
- 预取未实际消费的股票/日期不能制造新的账户日缺价失败；必要字段在实际消费时仍严格校验。不得因预取而要求不必要的备用SELL输入。
- `verify_pins()`的内容身份保证保持；不得用mtime/size代替SHA，不静默换active/profile/数据库。
- 有界窗口释放、句柄上限、内存预算可观察；只读数组不得泄露可变共享状态。

公共实现缺口单列，不在LocalSIM复制bin读取、停牌投影、原始价格恢复或择时规则。共享核心进一步优化不可变bar哈希或前缀计算时，也由其owner证明输入身份、输出和修订检测等价。

### 5.2 LocalSIM计算选项

旧CLI默认仍使用既有串行路径。新增显式离线性能选项：batch_days、account_workers及优化开关；合法范围与并发预算严格校验，未知值报错。这些是计算资源参数，不改变交易政策或既有研究人口；写独立操作回执，经济结果不掺计时/资源噪声。同一冻结request在不同合法资源参数下必须生成相同经济事实及exact retry结果。

源码身份包含新增性能模块；旧冻结研究继续由旧工作树运行，不能拿新源码恢复旧前缀。新进程加载、后端运行态和经济支持分别报告。

### 5.3 计算与提交分离

抽取唯一账户日计算函数，串行和并行均复用它。任务仅携带已验证的当日市场、manifest、费用、账户初态及计划引用。结果携带实际原生成交/现金事实、订单、事件、执行状态和原账户结果；不可把计划或时间槽填成成交。

结果经原生ledger重放与现有经济协调器验证后提交。不得通过另一套FeeModel/NAV公式制造等价结果。轻量估值继续调用原生account_snapshot，只避免复制不参与估值的成交历史。

### 5.4 因果、失败和恢复

核心只看到已完成且已可见的分钟prefix；轮询、TTL、计划修订、撤销、硬期限和14:57终点不改变。缓存命中不能跳过当日身份或输入修订检查。资金和可卖量仍按已确认成交更新。

并行worker异常按既有账户顺序归因；只保留相同已提交日前缀。中止仅回收本任务持有的计算子进程；不触碰服务进程。重试读取原生journal，exact retry返回同一不可变结果或ALREADY_MATERIALIZED，不重复成交。

## 6. Design Acceptance Index / 设计验收索引

- F-001：保持旧TWAP、旧默认串行入口和既有账户行为。
- F-002：完成有界共享批量准备的消费接缝与缺接口失败合同；真实共享实现由用户指定owner独立交付，消费侧不复制读取或策略算法。
- F-003：日股票池、PIT、C/D/T、数据及价格身份隔离。
- F-004：可见prefix、strict-after、轮询/TTL及共同终点等价。
- F-005：可见账户定位和快照复用，不共享账户状态。
- F-006：轻量估值仍使用原生账本及相同NAV/MDD。
- F-007：唯一账户日计算与唯一原生经济事务。
- F-008：有界账户进程池，按原顺序提交。
- F-009：失败前缀、部分成交、取消及恢复不重复成交。
- F-010：同request不同资源选项经济事实一致，exact retry不重算。
- F-011：参数、进程、缓存和内存预算可追踪且fail closed。
- F-012：真实冻结文件短窗口逐账户、逐日、逐订单比较与分阶段计时。
- F-013：完整人口和旧产物保护，性能提升不充当收益证明。
- F-014：多轮审核、定向回归、L0、CI与合入状态分别报告。

## 7. Implementation Plan / 长任务阶段

1. 建立基线、精确写入范围及共享接口合同；核对正在运行研究只读状态。
2. 先消费批量准备接口，验证输入/市场对象与旧Reader一致；共享接口归属不明时仅进行非依赖步骤，不复制另一Reader。
3. 优化可见账户定位、同一观察时刻的输入复用及估值快照；保持唯一核心和账本。
4. 抽取纯计算任务和序列化原生事实边界，再接有界多进程与原顺序事务提交。
5. 每阶段执行直接测试；进行实现审核、故障/因果审核及结果/性能审核，修复后只重跑相关失败点，稳定后执行最终定向矩阵。
6. WSL直接消费冻结输入，分别测基线、优化串行及有界并行；验证恢复与exact retry。没有真实证据的提速仅标估算。
7. 完成F2矩阵、L0、当前HEAD CI；完整交付经授权PR合入。后端重启与正式运行态由用户控制，本轮不热更活动回放。

## 8. Verification Plan / 验证方案

直接测试覆盖：旧默认路径；真实共享核心调用；准备/逐日读取市场对象一致；未来bar不可见；同日不同账户独立；轻量估值含持仓、费用、部分成交；串行/并行完整订单及经济事实；worker故障的有序前缀；重试、取消、恢复与exact retry；未知参数/缺接口/缺价/身份漂移；无生产DB、行情网络和服务控制。

真实验证固定同一包、初态、费用、输入、PIT及日期，短窗口至少覆盖连续多日和真实换仓；既有181日/13账户及288产物不覆盖。比较全部计划、订单/成交数量价格时点、费用、持仓现金、逐分钟NAV、终值/MDD、等待/状态/原因和完整人口；操作身份差异与业务差异分开。记录冷/热缓存、CPU预算、wall time、RSS、磁盘及阶段耗时。

原生OMS的event_time、order.updated_at、TradingRuleDecision.created_at和执行状态created_at/updated_at使用处理机器的wall clock，不是Fill.trade_time。独立重算对照保留这些原始字段，但单独报告处理时钟及其派生state_hash差异；state及TradingRuleDecision仍须通过自身哈希验证。成交trade_time、人工dispatch_at、订单created_at、反馈available_at、全部价格数量和经济事实不得排除。exact retry必须读取相同已发布原始文件及SHA，不使用上述对照归一化来掩盖收据漂移。

## 9. Risks / 风险与失败模式

主要风险是过早暴露未来输入、过期账户视图、浮点/费用漂移、并行提交破坏失败前缀、缓存掩盖文件修订及跨模块复制算法。分别以当时可见prefix、不可变视图、原生账本、单协调器、内容pin和owner公共接口约束。内存和启动开销可能使小任务并行更慢，按实测报告，不自动回退并谎称并行成功。

## 10. Production Gates / 生产门禁

`production_ddl_gate=noop`；DB读写、数据集修改、QE训练、生产账户/回放写入、行情网络及用户服务进程控制均为false。本任务计算子进程仅为离线计算，不是第二套调度服务。源码合入、离线激活、用户后端重启、正式账户启用和经济支持分别报告。

### Rollout / Rollback / 发布与回滚

离线性能选项显式启用，旧默认串行路径保留。新源码创建新研究或正式接受的源码身份；运行中的旧研究继续用原工作树。回滚只选择旧执行选项或旧源码新研究，不覆盖已发布产物、篡改断点或把新源码接到旧FAILED前缀。后端重启由用户执行；纯离线CLI验证不依赖服务重启。

## 11. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | dual_mode_research.py?native_research.py | backend/tests/simulation_execution/test_localsim_dual_mode_comparison.py | verified | - |
| F-002 | research_performance.py??5.1???? | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified_consumer_stage | approved_by_user????????????????????Reader??????????owner???? |
| F-003 | dual_mode_research.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-004 | human_portfolio.py???timing.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-005 | research_performance.py?human_portfolio.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-006 | native_research.py?human_portfolio.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-007 | dual_mode_research.py?timing_portfolio.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-008 | research_performance.py?dual_mode_research.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-009 | dual_mode_research.py?timing_portfolio.py | backend/tests/simulation_execution/test_localsim_replay_performance.py?artifact: X:/AIstock_temp/localsim-replay-performance-20261028/real-continuation-receipt.json | verified | - |
| F-010 | dual_mode_research.py | artifact: X:/AIstock_temp/localsim-replay-performance-20261028/real-validation-receipt-v2.json | verified | - |
| F-011 | research_performance.py | backend/tests/simulation_execution/test_localsim_replay_performance.py | verified | - |
| F-012 | docs/analysis/localsim_replay_performance_implementation_acceptance_20261028.md | artifact: X:/AIstock_temp/localsim-replay-performance-20261028/real-validation-receipt-v2.json | verified | - |
| F-013 | ????3/?8????? | artifact: X:/AIstock_temp/localsim-replay-performance-20261028/real-validation-receipt-v2.json | verified | - |
| F-014 | ?????6??????? | artifact: X:/AIstock_temp/localsim-replay-performance-20261028/final-directed-receipt.json | verified_local_pr_gate | - |

## 12. DESIGN-COMPLIANCE-001 / 逐项复核

1. 不把未完成共享接口或未完成真实验证声明为全部完成。
2. 不吞错误、填缺价或以空结果代替完整人口；线程/进程失败准确传播。
3. 不改变价格、成交代理、费用、账本、轮询、PIT或经济语义。
4. 不增设未授权审批；只在历史明确跨模块边界实际冲突时确认精确写入范围，其他已授权步骤继续执行。
