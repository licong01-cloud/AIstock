# 多 Alpha 策略演进蓝图：板块轮动、正交信号、关系状态与长期趋势

- 文档类型：F2 因子研发与实验分析蓝图 / 历史批次 `Gate-0` 开发记录（`develop-factor`）
- 主线：板块轮动（sector rotation）——让模型显式理解板块归属、轮动速度、成员参与度与板块内结构
- 初版日期：2026-07-10
- 当前版本：v6.6（策略演进最佳新策略包仍是唯一 P0；继承 v6.5 的 MA-E16 10 completed/2 failed、收益台账与运行态事实，不把本次规划修订冒充新实验结果；立即先恢复 Loop3/7 并完成 12-arm matched 归因，随后以重训可恢复成分/模型年龄、四格 sector oracle、基准相对/Brinson 三联诊断确定后续投向；两层板块、跨板块 lead-lag、右尾目标、条件门控与动态退出按诊断结果升为 P1 主线，participation gap/leadership exhaustion 改为并行 P1 而非 P0 主导；论文只提供候选机制，不替代 A 股真实 QE 证据；基础设施继续 maintenance-only，2026-08-12）
- 面向：Codex 因子研发 → Tier2/IC 审核 → QE 对照实验
- 关联：`develop-factor`、`analyze-factor-library`、#1939/#1940/#1941/#1943（`l2_code_id` 链路）、原 F1–F4 规格
- 多 Alpha 基础研发详细设计：`docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`
- F-014 Phase 2 实现级设计：`docs/architecture/qe_long_trend_evaluation_phase2_compute_cas_f2_design_20260722.md`

> **修订记录（BUG-989，2026-08-06）——数据面零数据库不变量（强制）：QE/多 Alpha 的训练、预测、回测和组合计算数据面不得访问数据库。数据库只用于控制面和结果面。所有计算输入来自具有版本、cutoff 和哈希的冻结 bin/H5/Parquet/sidecar 文件；缺失时 fail closed。**
>
> 本文中出现的 `market.sw_index_member` / `sw_index_classify` 只描述既有冻结数据集的历史构建来源，不构成当前或未来实验访问数据库的授权。QE/多 Alpha 运行期只允许读取冻结文件（行业 provider 为文件专用 `SectorDataIndustryIdProvider`，原 `SwIndexMemberIndustryIdProvider` 运行期 DB 查询设计已废止；第 1 节 R4 对照为历史实验事实，予以保留）。禁止运行期回退数据库、在线补齐、静默降级或以当前快照替代 PIT。
>
> 关联缺口（已解除，2026-08-06）：停牌输入不再依赖数据库——已构建版本化冻结候选数据集 `suspend_d_daily_candidate_20180801_20260630`（`suspend_d.parquet` + `manifest.json`，含逐交易日完整性回执，离线构建期只读导出自 `market.suspend_d`，`suspend_type='S'`，仅 sh/sz、剔除 BJ），作为冻结 bin 目录的同级 sidecar 同步至全部计算节点；新装配由 `qe_build_frozen_suspend_filter.py` 在计算节点按 `qe_frozen_build_spec.json` 的 suspend 钉（dataset_id/cutoff/universe key/SHA256）重建 `qe_suspend_filter.json`，钉/身份/日期覆盖/字段不符一律 fail closed，无数据库回退。原 `qe_suspend_filter_offline_dataset_gap` 阻断随之解除。

> **v6.1 历史活动快照（2026-08-10，已由 v6.2/v6.3/v6.4/v6.5/v6.6 取代）：**MA-E14 与 MA-E15 均已取得终态真实结果。MA-E14 的 H95/C85 regime overlay 在 full/early/late 三个窗口都降低 CAGR，当时阈值调优分支结束；MA-E15 的 9/9 Loop 显示压缩扩张三因子、增加 breadth 的四因子以及 LSTM 之间存在可继续研究的信号差异，但 `prepare_factors.py` 的 outer-union 令四因子实验额外纳入 1,511 行观测，不能据此归因 breadth 的真实增量。另经源码与任务配置复核，MA-E15 全部使用 V25_1 分钟执行，仍受开放 BUG-852（Issue #2692）的 10D 日频特征零填充问题影响，因此 IC/RankIC、Prediction Store 面板和 Top-K 信号诊断仍是有效证据，CAGR/Sharpe/MDD/Calmar/turnover 只能标记为 `EXECUTION_TRUTH_LIMITED_BY_BUG852`，不能作为新执行策略的真实对照。BUG-1006 是当时唯一阻断下一轮 matched 因子归因的 QE BUG；MA-E16 通过统一使用已验证的日频 `CLOSE_PRICE` 执行绕开 BUG-852，不等待或冒充修复 V25_1。该段只保留 v6.1 当时事实，不构成活动 backlog；当前状态与顺序以紧随其后的 v6.6、第 2.5、9.9.2～9.9.4、12、13、15、17 节为准。

> **v6.2 历史活动快照（2026-08-11 05:30，已由 v6.3 取代）：**当时 MA-E16 为 9 completed、2 failed、Loop12 running；该段只保留任务重叠、Windows/WSL 换页根因与 BUG-1014 最小修复边界，不再表示当前实验状态。
>
> **v6.3 历史活动权威（2026-08-11，已由 v6.4 取代）：**BUG-1006 已合入并由用户重启加载；MA-E16 权威 task `qe_20260810_224522_b0d9` 于 05:45 +08:00 终态为 `failed`，12 个 Loop 全部 terminal，其中 10 completed、2 failed，05:45 后未发现新的演进任务。10 个成功 arm 均使用相同 2026-06-30 冻结文件、`2,192,858` 行 matched panel、相同日期窗口与 label SHA256；四个可比较 matched pair 的 CE4-B 相对 CE3 CAGR 均为正，但 IC/RankIC 混合且 MDD 在 4 组中 3 组恶化，因此 breadth 当前主角色只能定位为 `CONDITIONING_STATE`，portfolio conversion 是待验证用途，不能写成已验证的 `DIRECT_ALPHA`，也不能在缺失两 arm 时做正式 blend/LOO。Loop3 已完成训练与预测，在日频 `qrun_limit.py` 读取 MLflow ICIR 时遇到空指标竞态；BUG-1022/Issue #3282 修复后必须显式以 `retry_mode=backtest_only` 复用完整模型参数重新执行预测/回测，禁止 `auto` 回退全量训练。Loop7 在训练中因 stop/SIGTERM `-15` 退出，不是 Alpha 失败；BUG-1014 新资源信封运行态验证后只对该 arm 显式执行 `retry_mode=full_train`，不得把中间参数当完成模型。重复 task `qe_20260810_221723_14ab` 保持停止，永不恢复。BUG-1014 的 RD-Agent PR #18 已以 `da3270f8…` 合入，AIstock PR #3273 已以 `49ccc1f3…` 合入并同步 root；RD immutable release/deploy/用户重启/identity 与 backend-main 用户重启/identity 均未执行。BUG-1022 已登记并进入 source fix，实验恢复继续等待对应 source merge 与用户运行态验证。本轮不启动新实验。
>
> **v6.4 历史活动权威（2026-08-12，已由 v6.5 取代）：**MA-E16 事实与 Alpha 解释不变，Loop3/7 尚未恢复。BUG-1022 日频 runner、BUG-1023 durable orchestrator 空闲扫描和 BUG-1024 background reconciler/GET 写状态修复已分别以 `a7bc06a3…`、`d8e80b9f…`、`3a6e3290…` 合入。BUG-1026 的单观察器、60 秒远端探测、变化才写状态修复已通过 AIstock PR #3296 以 `e2fdaaab…` 合入；BUG-1030 的 RD-Agent 有界初始 tail、`Last-Event-ID/after_cursor` 和不重放 cursor 修复已通过 RD-Agent PR #19 以 `795973e1…` 合入，AIstock 跨仓记录 PR #3300 以 `aee31e67…` 合入；BUG-1031 的单上游 broker、游标续传、内存/订阅者背压、页面隐藏零流量及 `qe-live-0..4.jsonl` 五个 1 GiB 环形槽修复已通过 PR #3303 以 `573e5380…` 合入。该段只记录当时 source-merged/runtime-pending 状态，不再代表当前运行态与 Issue 状态。
>
> **v6.5 进度与收益权威快照（2026-08-12，进度事实继续有效，活动顺序由 v6.6 取代）：**只读实时回读确认 MA-E16 `qe_20260810_224522_b0d9` 仍是最新 QE 演进 task，状态仍为 `failed`、10 completed/2 failed，Loop3/7 未恢复，重复 task `qe_20260810_221723_14ab` 仍为 `paused` 且永不恢复。8001 `/runtime-identity` 为 `0e0afed26732ea5eb61988b516bd0b49becfead5`，并包含 BUG-1014/1022/1023/1024/1026/1031 的 AIstock 修复；WSL `127.0.0.1:9000` 与远端 `192.168.50.215:9000` 当前 `/health` 均为 `ok`。BUG-1026、BUG-1031 已为 `verified` 并关闭，BUG-1030 为非运行态 `fixed` 并关闭；BUG-1014/1022/1023/1024 的登记仍为 `open/pending_user_restart`，与已经加载的 source 事实不一致，剩余工作仅是逐 BUG post-restart receipt/close-sync，不是新的基础设施研发。该流程收尾应与策略恢复并行且不得继续占用策略主线；立即顺序仍是 Loop3 `backtest_only`、Loop7 `full_train`，再完成 12-arm matched 归因。该快照只证明当时进度，不把后续规划写成已执行结果。
>
> **v6.6 活动规划权威（2026-08-12）：**本次修订不改变 v6.5 的 task、Loop、收益、运行身份或 BUG 状态，也不执行实验。完成 MA-E16 12-arm matched 归因后，P0 不再直接串行扫描更多横截面专家，而是执行三个可证伪诊断：①在同文件数据、同特征/标签/策略/成本下比较静态切分、模型年龄切片与因果滚动/扩展重训，量化可由新近成熟数据恢复的退化成分及剩余 calendar-regime 弱化，而不把单次恢复冒充唯一因果解释；②运行 reality/oracle × sector/stock 四格与 hard/soft 两层上界，定位瓶颈在板块选择还是板块内排序；③保留绝对收益，同时补充冻结 benchmark 下的主动收益、beta、tracking error、IR 与 Brinson allocation/selection。诊断结果分别触发适应/条件门控、两层板块与 lead-lag、右尾/LTR/hazard/meta-labeling 或横截面 Alpha 分支；任何论文方法先作为 matched canary，不升级为已验证 Alpha，也不触发底座重写、数据库数据面、UI、Archive 或历史工程。
>
> **未来关系实验的数据边界：**任何 HIST、动态图、概念图、回撤 hazard 或新 Alpha trial 都只能消费实验启动前已经生成并钉住版本/cutoff/hash 的 H5/Parquet/bin/sidecar。实验、composer、worker 与 qrun 不得查询 `market.*`、连接数据库补齐关系或在缺文件时降级。专用数据集构建流程与 QE 实验运行严格分离；是否建设新的冻结数据集只按未来策略价值另行评估，不成为当前实验前置。

---

## 1. 背景与已确认事实

策略目标是捕捉板块轮动 alpha：不仅识别“哪个板块在领涨”，还要识别轮动是否扩散到多数成员、成员是否协同、板块是否正在进入或退出领涨区，以及板块内哪些股票具备稳定的相对强度。

当前基础能力如下：

- GATs 关系模型已接入真实申万 L2 行业信息；模型侧可以显式利用板块归属。
- 导出侧已在 `sector_data.h5` 的 22 个 `sw2_*` 数值字段之外增加稳定的 `l2_code_id`。未知值为 `-1`；QE 运行只读取冻结文件中的 PIT 编码及其 dataset identity，不追溯或查询其上游数据库表。
- `sw2_*` 是“个股当日所属板块的指数聚合值”，按 PIT 归属展开到个股；`l2_code_id` 是离散分组键，不是连续特征。
- 方向 A 的签名 fallback 邻接偏置在实验 `qe_20260710_005329_4b05` 的指定配置中未观察到可辨识增量（off≈industry_bias，0.0930 vs 0.0927）。该结果不能外推否定所有邻接设计，但足以说明后续主线不再依赖字段签名猜测同业关系。
- 真实申万 L2 二值同业邻接已完成 R4 历史对照，并非“尚未验证”。在 `qe_20260713_195926_11e3` 中，seed 7 的 off/industry-bias RankIC 为 `0.105816/0.095048`，seed 17 为 `0.103728/0.100237`；两颗种子均未改善 RankIC，组合收益表现混合。MA-E13G 的三种子 matched-seed 复核进一步显示：industry-bias 相对 embedding-only 的平均 RankIC 从 `0.10461` 降至 `0.10088`，但 CAGR 从 `54.73%` 升至 `64.11%`、primary information-ratio 从 `1.401` 升至 `1.652`。因此静态行业关系不再归类为直接 Alpha 增强，而归类为 `RELATION_PRIOR/PORTFOLIO_REGULARIZER`；当前复现必须使用文件专用 `SectorDataIndustryIdProvider`，不得恢复历史运行期 DB provider。

本次修订同时纳入因子库 MCP 的去重与统一指标证据。关键结论是：原 F1–F4 不能作为四个全新的同优先级因子直接开发。

| 原编号 | 原设计 | 统一状态 | 当前证据与处置 |
|---|---|---|---|
| F1 | `m_sector_rs_rank_20d` 板块相对强度排名 | `BASELINE` | 与既有行业动量/行业反转族同源。对收益做 percentile rank 是单调变换，本身不产生正交性。保留为研究基线，不作为首批新增因子；新增研发改为“板块排名速度”。 |
| F2 | `m_sector_breadth_ma20` 板块内成员站上均线比例 | `BASELINE` | raw level 作为基线；当前英文泛化 MCP 搜索不足以证明无同族资产，Stage 0 仍须用精确名、中文描述、公式和相关簇查重。A2 breadth thrust 作为待证伪的 `NEW` 主候选，而非已证明独有。 |
| F3 | `m_sector_flow_rotation_10d` 板块资金流加速 | `NEGATIVE_CONTROL` | 与现有 `m_sw2_net_vol_momentum` 等高度相邻；既有 out-sample 1d 证据弱。继续保留为负对照和资金流窗口/状态研究样本，弱结果不阻止入库研究、变体分析或后续互证。 |
| F4 | `m_stock_sector_leadership_20d` 个股 20 日动量减板块动量 | `REUSE` | 经济公式意图已由 `m_stock_vs_industry_mom_20d` 覆盖，并与 `m_mom_residual_20d` 进入同一高相关簇；catalog 资产的 PIT 口径缺陷作为独立数据缺口记录。F-006 repair source 同步与重算可与原公式、B2 persistence 和代理样本研究并行，分别记录口径与损失，不因缺口停止方向。 |

关键策略约束保持不变：

- 标签不做板块中性化；主标签保持与目标 QE 实验一致的裸 h20 前向收益。
- 因子内部可以使用行业相对值、残差、板块内排名等结构，但不能把“因子使用相对值”和“标签板块中性化”混为一谈。
- 正交性、模型增量和单因子 IC 都是可并行观察的证据维度；同公式、反向或单调变换应显式标注谱系与重复度，用于互证而不是据此终止研究方向。

## 2. Scope / 范围

### 2.1 目标

1. 建立一个可扩展的板块轮动候选池，不把交付数量固定为四个。
2. 首批开发 5–10 个口径明确的候选，以 h20 快筛、统一指标、双层相关性和模型消融形成多维证据；任何单项弱结果、数据缺口或相关性结果都不淘汰候选或研发方向。
3. 同时覆盖四类互补信息：
   - 板块间状态：强度、排名速度、波动压缩；
   - 板块参与度：价格广度、换手广度；
   - 板块内部结构：等权参与、残差协同性、领导持续性；
   - 负对照：已知低成功率的板块资金流加速。
4. 通过 G12、显式板块因子和 `l2` embedding 的受控消融，确认增量来自哪里。
5. 通过的因子与 RDAgent/QE/Qlib、因子库和未来实时加载链保持同公式、同 PIT、同编码语义。
6. 按“低成本组合验证、长周期标签/模型、两层板块选股、多腿组合、PIT 关系模型、概念多关系图”组织长期上涨趋势 alpha 研究。顺序是资源优先级建议，不是串行门禁；各方向可在 QE 内并行，基线和实验卡只用于可复算说明。

候选在研发前和研发后使用统一状态，不用“计划开发”“已完成”“可用”混写：

| 状态 | 含义 |
|---|---|
| `NEW` | 公式已冻结，准备新开发。 |
| `BASELINE` | 只作比较基线，不默认新增可用因子。 |
| `REUSE` | 复用已有资产，只补缺失的 h20/相关性/模型证据。 |
| `NEGATIVE_CONTROL` | 负对照；无论结果强弱都保留，用于互证、损失分析与新假设。 |
| `EXPLORATORY` | 数据、公式或证据尚不完整的探索项；同步记录数据获取、代理实验和补证计划。 |
| `OBSERVED_STRONG/OBSERVED_MIXED/OBSERVED_WEAK/RELATED` | 仅描述当前 trial 的观测结果，不是准入、淘汰、停止或方向处置。 |

### 2.2 唯一硬边界与非阻断研究原则

本蓝图唯一硬边界是 **QE-only 隔离**：所有实验、评价、缓存、CAS、数据库表、API/MCP/UI 展示和任何写入都只能属于 QE；不得读取、修改、调用或影响 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage、生产交易及其他非 QE 模块的服务、表、缓存、任务、数据语义或运行状态。除这一隔离边界外，本蓝图不设置任何研究门禁。

- 数据缺失、覆盖不足、PIT 证据不完整、执行原因不可验证、平台功能未完成或某项统计结果偏弱时，统一记录缺口范围、可用部分、可能损失或偏差和可互证证据；这些情况不阻止实验、不淘汰候选。数据获取或替代实验仅在真实策略运行期间按未来价值并行评估，不自动形成前置任务。
- 预注册、冻结口径、HAC/bootstrap、DSR/PBO、相关性、成本容量、oracle 和 F-014 都是分析工具，用于说明结果可信度、适用范围和损失，不是准入条件或停止开关。
- 同公式、反向、窗口变体和高相关候选可以研究，但需显式记录谱系、差异和重复度，避免把重复结果误称为独立发现；记录本身不限制继续实验。
- 任一指标族只要可计算即可立即用于科研分析。不可计算的指标族保持 `MISSING/PARTIAL/NOT_VERIFIABLE`，但不自动启动历史数据补充、物化或替代验证，也不影响其他指标族和实验路线。
- 任何“弱、负、混合、重复、未完成”结论都只限定于当前 trial、数据版本、样本和公式，不得外推为方向级终止。
- task 状态只描述编排整体状态，不代表其全部 Loop 的研究结论。新策略 trial 的成功 Loop 由现有 writer 自然保存最小比较结果，父 task 为 `failed/partial/cancelled` 时不得覆盖已成功结果；既有历史 Loop 只读保留，不再要求补录 append-only 总账、聚合或互证。
- 失败 Loop 按模型、因子集、种子、数据、训练、预测、回测或归档阶段单独记录；它不覆盖同 task 的成功 Loop，也不把“无结果”伪装成负 Alpha 结果。成功 Loop、失败 Loop、缺失制品与任务编排异常分别报告。
- 未经用户在未来任务中专门要求并确认，不再新增研究门禁、阻断状态、淘汰规则或 `PASS/KILL/GO/STOP` 类决策语义。
- **策略演进最高优先级**：任何时刻只要现有 QE/Multi-Alpha 能完成目标 trial，就必须先运行策略候选；基础架构、数据完善、诊断和文档工作不得排在策略源码与真实实验之前。
- **基础架构封板**：P0-1～P0-4 视为满足当前演进要求。只有真实策略 trial 出现可复现、无安全替代路径的阻断性故障时，才允许建立最小 BUG 修复；不得预建通用平台能力。
- **终止历史工程**：不再执行旧任务/Loop 的 Archive 补录、历史补账、批量物化、rematerialize、证据完整化或 UI 完善。既有历史事实只读保留，不删除、不改写，也不再成为 active/pending 工作。
- **新 trial 最小证据例外**：新策略实验必须自然产出比较优劣所需的配置快照、输入身份、回测指标、成本/风险结果和失败原因；这属于当前 trial 输出，不属于历史归档工程。除现有 writer 的正常落盘外，不为此新增通用 schema、Archive、UI 或审批流程。

### 2.3 2026-07-11 `Gate-0` 历史批次记录

`Gate-0` 仅是 2026-07-11 前置批次的历史名称，不再表示当前研究门禁。该批交付了 F2 设计、candidate bundle、通用 h20 快筛、RD-Agent/AIstock h20 companion 指标契约和 F4/R2 tracked repair source 的 PIT 修复；A1–A6/B1/B2/N1、双代码资产、成本容量、QE 消融和其他工作当时被列为后续项。历史 receipt 和原状态保留用于追溯，但不能继续解释为当前研究方向的放行条件。

### 2.4 v4 路线补全边界

第 2.3、11.1、Phase G0-A–G0-C 与验收矩阵保留 `Gate-0` 当时的历史 receipt，不把历史记录改写成当前状态。v4.2–v4.6 形成了 F-014、长期标签、oracle、R8M 和任务级多样性方案；v4.3 确立的 QE-only 隔离继续作为唯一硬边界。

v4.7 明确撤销 v4.6 及本文其他章节中的所有研究门禁、阻断、淘汰、`PASS/MARGINAL/KILL`、`GO/STOP`、promotion-gate 和“数据就绪后才允许研究”语义。F-014 改为指标族独立证据状态；计算、CAS、数仓、API/MCP/UI 和历史补算可以并行交付，任一可计算结果立即可用于科研分析，平台完整度不阻断期限、模型、R8M、R8B2、R8C、两层或关系方向。oracle 负结果只描述当前 trial；概念 PIT 未就绪时并行推进数据获取、代理/部分样本实验和损失分析。历史文档或关联设计若与本条冲突，以 v4.7 为准，并在后续同步修订对应文件。

v4.8 对 QE 数仓重新执行 Loop 级审计：6 个顶层 `failed` 任务内部共有 50 个成功 Loop；R2–R5 的 8 个任务共 67/67 个成功 Loop，其中多数 task identity 先前未进入本文总账。v4.8 将这些结果恢复为正式研究证据，撤销“task failed 等于实验无结果”、TCN 证据弱、LTR/正交模态无继续价值、R7 简单融合失败即可停止其他融合、oracle/F-014/数据准备先后关系控制研究许可等错误外推。后续只按信息增益和资源安排顺序，不设置方向准入条件。

v4.9 恢复 LSTM 在 Type-B 上升趋势研究中的主模型地位。QE 历史实验证明 LSTM 曾取得当前可核验单 Alpha 记录中的最高收益：R16A `qe_20260609_025155_87ee` 最优 Loop CAGR `112.11%`，R9A `qe_20260603_000500_8936` 最优 Loop CAGR `108.29%`，R17A `qe_20260609_154050_0451` 最优 Loop CAGR `107.67%`。这些历史 trial 使用 h20、旧股票池、不同因子集和 Alpha158/策略配置，因此作为“LSTM 值得优先投入”的架构先验，不直接冒充 Type-B 长期趋势收益结论。当前同口径 R8B 已 6/6 完成，进一步支持把 LSTM 从“第一挑战者”提升为 Type-B 长期趋势第一主模型；LGBM 是结构化强基线，TCN 是高优先级并行对照，三者按任务与证据互补研究，不因模型名设置淘汰关系。

v5.0 将 R8A 12/12 与 R8B 6/6 的最终预测、标签、参数、指标和交易摘要作为完整证据集重新分析。三种子日截面 rank ensemble 对 LGBM 的 RankIC 增量约 `0.00046–0.00065`，对 LSTM 的增量约 `0.00220–0.00248`；但 Top50 对每日未来收益 Top1% 右尾事件的捕获率仅约 `1.29%–1.91%`，2026H1 多数组合进一步降至 `0.51%–1.18%`。高 RankIC 与低主升浪捕获并存，说明当前 G14-FP 更擅长广义收益排序，板块状态识别、右尾目标、信号到策略转换和 episode 级评价都应继续研究。该结论不淘汰任何模型或方向；R9S 复用预测检验换手/持仓转换，F-014 最小捕获核、四格 sector oracle、R8C 组合层融合、LSTM/TCN/LTR 和右尾因子研究按资源并行。

v5.1 启动 F-014 任务2并形成 Phase 1 / 工作流 A 的首版 QE-only 核心。v5.2 对实现逐项审计并补齐不能留给后续平台层的计算语义：快照内容 hash、QE workspace 文件绑定、不可伪造的 full-overlap receipt、feature/outcome 窗口隔离、feature 截止日后预测拒绝进入原实验、close/high-low 路径分层、ordered stage 与右删失生存、首日已有持仓左删失、position 自身 as-of 与 outcome 扩展分离、完整进出场 evidence bridge、一对一 trade 归属、indicator/trade 数量和时点矛盾 fail-fast、直接阻断损失、authoritative portfolio report、板块主导切换及所有缺口的指标族局部状态/data action。Phase 1 不连接 CAS、资源状态机、三表、API/MCP/UI 或历史 R8 task；这些 Phase 2–5 平台能力继续单列，不影响现有科研并行，也不得把本次源代码交付写成 F-014 平台整体完成。

v5.3 将 R9S/R10 结果、R11 实时进度和 BUG-730 控制面修复写入蓝图。R9S 两个 backtest-only task 已 24/24 完成；R10 已取得 48 个成功 Loop：LambdaMART 6/6、LGBM 12/12、TCN 12/12、LSTM 10/12、hold10/20 6/6、hold30 2/3。QE Archive source-status 已显示 R10 LGBM、LSTM、TCN 的任务源记录全部归档，R10E3 仍为 2/3 部分归档。R11A `qe_20260717_134109_97e1` 复用 G14-FP h60 预测比较 hold10/20/30，状态快照为 4 completed、4 running、1 pending；R11B `qe_20260717_134306_6354` 串行训练 EfficientGATs h40/h60，状态快照为 2 completed、Loop3 running。R11B 已取得的两个 h40 seed RankIC/CAGR/Calmar 分别为 `0.10527/59.34%/5.14` 与 `0.10073/38.00%/2.63`，只作为运行中证据，不提前形成图模型结论。BUG-730 已通过 PR #2347 合并，修复 MCP 单 Loop rerun 对 `node_id/node_parallelism` 的保留与透传；最近一次用户重启发生在该合入之前，因此运行时加载仍待下一次重启，当前 R11 任务已使用正确节点并发配置继续运行。现有 GAT 后续继续研究板块层时序动态图、长期趋势头、未来 5/10/20 日回撤 hazard、板块风险预算和动态退出，不重复静态二值同行业边或简单 prediction average。

v5.4 完成 R10/R11 的 Loop 级收口。R10 现为 51/51：LambdaMART 6/6、LGBM 12/12、TCN 12/12、LSTM 12/12、hold10/20 6/6、hold30 3/3；LSTM `G15-FPL+h60` 三种子平均 RankIC/CAGR/Sharpe 为 `0.10206/67.55%/1.806`，hold30 三种子平均 CAGR/Sharpe/换手为 `54.21%/1.566/5.995`。R11A `qe_20260717_134109_97e1` 的 Loop5–8 已确认训练、预测、回测、prediction-store 制品均成功，失败仅发生在 `results_only` 状态重算；BUG-741 通过 PR #2391 修复 `RealDictCursor` 字典行解析后，以同一公开恢复入口完成 9/9，不重训、不直接改库。R11A hold10/20/30 三种子平均 CAGR 分别为 `58.44%/59.18%/49.30%`：hold20 在 2/3 种子上高于 hold10，但平均 Sharpe 略低、回撤略深；hold30 明显弱化，说明固定持仓不应再单独承担退出建模。R11B `qe_20260717_134306_6354` 已 6/6 完成：h40/h60 平均 RankIC 为 `0.10461/0.10156`，CAGR 为 `54.73%/55.51%`，Sharpe 为 `1.588/1.677`，最大回撤约 `-17.02%/-16.95%`，年化换手为 `5.78/11.17`。h60 没有形成清晰的信号或收益优势，却使换手接近翻倍；h40 因此是当前 EfficientGATs 更高效的后续关系/板块风险研究锚点。R11A 9/9 与 R11B 6/6 已通过 backfill run `qear_bf_f9115c130b94464089b45de3f26c5fdf` 全量进入 QE Archive，均为 `fully_archived`。下一轮已启动 R12G canary `qe_20260718_040323_9a4a` 和 R12P 四腿组合 `macb_453ca2d0c5b21b40_20240701_20260629_20260717T201644965348Z`；前者为 WSL 单 Loop 图训练，后者在远端 CPU 以最多 4 并行执行。R12P 首次提交 `macb_dee194ec754a8991_20240701_20260629_20260717T200639105333Z` 因人为设置 `min_date_coverage=0.95` 高于 h60 标签成熟后的 `423/483=87.58%` 而立即失败，保留为配置记录；正式运行恢复平台既有 `0.8` 口径。这些结论和运行状态只描述当前 trial，图模型、动态退出、sector gating、portfolio fusion、LSTM/TCN 和其他关系结构继续研究。

v5.5 对多 Alpha QE 平台做了基于现有代码的能力审计，并将基础研发 P0-1～P0-4 提升为后续第一优先级。结论是：`MultiAlphaCombineBacktestService`、组合器、Prediction Store、远端 pred-backtest、scheme/LOO、场景回放、Archive、`combine_ui_adapter.py` 以及已复用的 `LoopDetailPanel`/`EvolutionTrajectory` 已经构成可继续研究的 Tier-1 底座；但异步提交仍由 FastAPI 进程内 `daemon=True` 线程持有，父子任务、远端 `task_id/loop_id`、节点占用和事件没有形成完整的数据库持久状态，后端重启后不能保证自动重新接管；当前只有整组 retry，没有暂停/恢复/取消和子任务级 `backtest_only/results_only` 恢复；UI 缺少沿用单 Alpha 自动演进页面的正式创建器及完整子任务运行网格。因此不能把平台描述为“未来只需新增模型和因子、无需任何程序研发”。后续不另建多 Alpha v2，不重写组合算法，而是在现有 run/service/router/UI adapter 上增量完成：P0-1 持久化编排与重启接管、P0-2 生命周期控制和子任务恢复、P0-3 QE 自动演进同风格创建器、P0-4 子任务状态/日志/恢复可见性。详细契约见 `multi_alpha_qe_evolution_foundation_f2_design_20260718.md`。

v5.5 同步核对了重启后的当前运行事实：R12G `qe_20260718_040323_9a4a` 已 1/1 完成，RankIC/CAGR/Sharpe/最大回撤/年化换手约为 `0.10569/62.12%/1.8108/-15.78%/5.72`，说明 cooperative execution canary 没有破坏该 Loop 的可训练性；R12P 最新 run `macb_453ca2d0c5b21b40_20240701_20260629_20260718T092728399999Z` 在 baseline 子任务达到 3600 秒后以 `combine_backtest_scheme_timeout` 失败。该失败发生在组合编排/子任务执行层，没有形成可用的四腿 Alpha 比较结果，不能解释为组合方向或任何 Alpha 腿失败；它直接说明子任务状态持久化、单子任务重试、超时后的远端状态核对和后端重启恢复应先于继续批量组合实验完善。

v5.6 对多 Alpha P0 F2 详细设计执行逐项语义审计并完成修订：删除设计矩阵中未经逐项确认的 `APPROVED_BY_USER` 状态，以 `DESIGN_READY/DESIGN_VERIFIED` 只表达设计完整性，代码/DDL/测试仍明确未实现；将 legacy `stop` 固定为现有单 Alpha cancel/kill 兼容语义，pause 只停止新 child 派发；删除 child 的伪远端 pause 状态并补齐 `not_computable`；parent 聚合只按结构化成功/失败事实，不判断“研究价值”；节点容量覆盖现有 QE active execution 来源并按 remote identity 去重；状态 transition 与 DB event 同事务；Archive capture 初始化失败必须 health/event/UI 可见；schema 缺失只让 multi-alpha worker/写接口结构化不可用，FastAPI 和非 QE 模块继续运行；`/quantevolver/evolution` 固定为规范 UI 入口，旧多 Alpha URL 只做兼容映射，并恢复同 viewport screenshot/golden 视觉验收。以上均是实现语义修正，不新增科研门禁或审批。

v5.8 根据正式设计审核修订 F-014 Phase 2：恢复 `indicators_normal_{freq}_obj.pkl` 的 `amount/deal_amount/ffr` 权威语义；增加 execution-environment identity、无秘密 pickle parser、派发前 `run_evaluation` control row、原子 FIFO claim、独立 `qelt:<evaluation_id>` resource session、AIstock/RD 双端 startup recovery、按 family 冻结的 CAS required 集合，以及 registration/worker/published 三阶段 receipt。normal Loop 在注册/提交评价后立即继续 `read_exp_res` 并释放 reservation，不等待 CPU 评价或 CAS。Phase 2 只把三表中的 control row 前移用于重启恢复，metric/artifact 两表仍在 Phase 3；这属于平台完整性修正，不增加研究门禁、准入或淘汰逻辑。

v5.9 按当前 Git、数据库和运行时事实更新 Phase 2：AIstock PR #2630/#2643 与 RD-Agent PR #7 已合入，DEV/生产 `qe_archive.run_evaluation` migration 已 applied/readback，8001/9000 Phase 2 routes 与 AIstock reconciler 已激活。2026-07-23 12:23，R8B `qe_20260715_104922_001d / Loop4` 的首次真实 `long_trend_only` canary 创建了唯一 control/resource row，但在 RD durable job 创建前因合法零字节 bundle 文件被旧 verifier 误拒绝而以 `QELT_BUNDLE_INVALID` 失败；没有 worker、attempt、terminal receipt、CAS、训练、回测或非 QE 副作用。BUG-837/RD-Agent PR #8 修复 bundle 校验，BUG-838/AIstock PR #2654补充同 evaluation ID 的确定性恢复和真实 delivery 状态，两者尚未合入或加载。R8–R11 的 12 task / 108 Loop 均已 Archive completed 且 `research_valid=true`：57 个保留 workspace 的 Loop 可在修复部署后直接进入 parser，51 个已清理 workspace 的 Loop 只需复用 Prediction Store 与冻结配置执行 `backtest_only/results_only rematerialize`，不重新训练。缺失制品、partial catalog 与不可验证 execution cause 继续形成局部状态和数据动作，不淘汰模型或研究方向。

v5.10 记录修复部署后的真实 F-014 运行证据：RD-Agent PR #8 和 AIstock PR #2654/#2668/#2670/#2672/#2674 已合入并加载。R8B `qe_20260715_104922_001d` 已按单 CPU 槽完成 6/6 evaluation，每个 Loop 都只有一个 RD job/attempt、worker terminal 和专属 CAS publish，合计产出 12,024 项指标、13,241,712 行 signal observations、13,726 行 holding episodes 与 2,898 个 portfolio trading days。六个 Loop 的 `signal_path/portfolio_result/sector_regime` 均为 `COMPUTED`，`position_episode/order_fill` 均为 `COMPUTED_WITH_LIMITATIONS`，`execution_cause` 均为 `NOT_VERIFIABLE`；局部限制只形成数据补取和损失分析，不取消任何已算证据或研究方向。用户完成 8001 重启后，Loop4 相同请求回放仍返回同一 evaluation/job/attempt，attempt 数量为 1；R8B 6 个原 Loop 与 Archive 的状态/配置/指标保持不变。生产保留四条历史平台失败 row 与六条 `partial + CAS published` row。当时计划下一批评价其余 51 个保留 workspace Loop，再对 51 个已清理 workspace Loop 做不重训的 `backtest_only/results_only rematerialize`；该计划已由 v5.24 明确终止，第 2.6 节只保留冻结资产清单。

v5.11 将 R12P 从“只有编排失败证据”更新为“append-only 历史结果恢复进行中”。P0-1～P0-4 基础平台完成后，R12P 复用原四腿 prediction，在 `results_only`/reference/remote-result collection 路径上逐 child 创建 successor recovery run，不重训、不覆盖原失败 run，也不直接修改历史 child。BUG-864（PR #2736，close-sync #2737）修复已完成远端结果回收与终态 reservation 竞争；BUG-865（PR #2740，close-sync #2741）补齐累计 successor 对 inline `reference_result/derived_result` 的复用；BUG-867（PR #2744，close-sync #2746）沿不可变 `source_child_id` ancestry 找回累计恢复中丢失的原始 attempt，循环、错配或缺失 lineage 继续显式报错。2026-07-26 重启后，当前 successor run `macb_recovery_e45023af793b9a29d2acfc8738560b4f5addfc1c81e3fff5b064d9ef2cdc243c` 为 `partial_recovered`：21 个 child 中 3 个 `succeeded`、18 个 `not_recovered`；已成功的是 LGBM baseline、`scheme:equal` 和 equal LOO drop-GAT。重启后预检确认其余 8 个 equal/orthogonality-aware child 可精确恢复，10 个 `ic_weighted/risk_parity` child 因原始成功 attempt/result 不存在而保留 `not_recovered`。缺口只描述历史证据覆盖，不淘汰相应权重方法或研究方向；后续先完成 8 项恢复并分析 equal、orthogonality-aware 与任务级 LOO，再启动低成本板块回撤预警 overlay。

v5.12 记录 R12P append-only 恢复和首次完整可比结果。最终 successor `macb_recovery_ce14e0a572673ff88ab22b6a4089bb7276f1cec676c19d535315bffc47e448de` 为 `partial_recovered`，但研究可用 child 已达到预期的 11/21：1 个 LGBM baseline、2 个 scheme 和 8 个 LOO；其余 10 个 `ic_weighted/risk_parity` child 继续保留 `not_recovered`，不伪造结果。LGBM baseline CAGR/Sharpe/Calmar/最大回撤/换手为 `53.57%/1.602/2.649/-20.22%/5.975`；四腿 equal 为 `74.47%/1.922/3.554/-20.95%/6.521`，orthogonality-aware 为 `72.12%/1.893/3.302/-21.84%/6.506`。equal 相对 baseline 的 CAGR、Sharpe、Calmar 分别提高约 `20.90` 个百分点、`0.3204`、`0.9051`，而最大回撤只加深约 `0.73` 个百分点；当前 trial 证明跨标签、跨任务四腿 portfolio fusion 可以产生组合层增益，并撤销“R7 两腿简单融合失败可外推到其他融合”的错误解释。LOO 显示 LGBM 是两个 scheme 中最稳定的核心贡献腿；GAT 与 LSTM 在 equal 下提供收益/Calmar 补充，TCN 在 equal 下小幅增加 CAGR/Calmar 但拖累 Sharpe，orthogonality-aware 下 GAT/LSTM/TCN 多数风险效率边际为中性或负。该归因用于下一轮板块风险条件化和组合权重研究，不淘汰任何模型或任务。正式 sector-risk overlay adapter 尚未实现，现进入基于现有 QE 策略/数据/评价链的 F2 设计与实现，不允许用 HMM 新买入过滤冒充动态减仓、退出和重入。

v5.24 纠正“平台完整度先于策略演进”的执行偏移。Multi-Alpha P0-1～P0-4 已满足当前策略研发所需的提交、运行、恢复和结果读取能力，封板为 maintenance-only。用户明确终止 F-014 UI、Phase 5 平台认证、其余 R8B/历史 Loop 物化、历史 Archive 补录、总账固化和其他历史证据完整化任务；既有记录只读保留。当时主动主线切换为 `MA-E01 Sector-Conditioned Multi-Alpha Strategy Package v1`：复用现有 LGBM/LSTM/TCN/EfficientGATs prediction，以四腿 equal 为基线，开发板块状态条件化权重、有界减仓、退出和重入，并以真实 QE run 比较 CAGR、Sharpe、Calmar、最大回撤、换手、成本、false early-exit、重入延迟和趋势捕获。源码能力由 `afe5634d` 合入；2026-07-31 提交 `none/entry_gate/bounded_de_risk/exit_reentry` 四个正式 policy run，每个严格规划 LGBM baseline + equal 两个 child，共 8 个真实 CPU pred-backtest child。该段记录当时尚未终态的事实，现已由 v6.0 和第 2.5 节替代。

v6.0 以 MA-E07～MA-E13 终态结果替代 v5.24 的运行中快照。四腿 soft de-risk 与 drop-TCN 三腿形成全窗口 Pareto 候选；G18 新增腿明显退化；近期一年窗口仅取得约 `12%` CAGR，确认首要瓶颈是 regime drift；三种子 industry-bias 结果把静态行业关系从直接 Alpha 重分类为关系先验/组合正则。活动研究不再按 `MA-E01→MA-E02` 的单线编号排队，而按 `POLICY/ALPHA/RELATION/LABEL` 四个稳定 track 并行组织，真实策略结果始终优先。新增 Alpha 首批聚焦波幅压缩扩张、大单—小单参与差、领导耗竭与多模型分歧；动态图、右尾标签和基本面扩散随后推进。基础设施、UI、Archive、历史补账和平台完整化边界不变。

v6.1 将 MA-E14/MA-E15 纳入终态总账并修正活动顺序。MA-E14 结束当前 H95/C85 threshold-overlay 分支；MA-E15 的 CE3/CE4-B/LSTM 信号结果保留，但因生成脚本 outer-union 改变观测总体，breadth 增量结论撤回为待复验，其 V25_1 组合指标另受开放 BUG-852 限制。BUG-1006 以 opt-in 的显式参考因子交集恢复 matched trial，修复 public compose 的策略参数路由并保留无契约历史语义；运行态生效后以日频 `CLOSE_PRICE` 直接执行 MA-E16。该最小修复由真实 Alpha trial 触发，符合 maintenance-only 边界，不恢复任何平台、UI、Archive 或历史工程。

### 2.5 当前执行总账（截至 2026-08-12）

本表是阅读本文时判断“已完成/待执行/仅设计”的首要入口。历史 Gate-0 receipt 不因后续进度而删除，但当前状态以本表、对应实验 task/run 和第 15 节验收矩阵为准。

实时事实的最后核对时间为 2026-08-12：8001 返回 MA-E16 仍为 10/12、最新 task 列表中没有晚于 2026-08-10 22:45 创建的演进任务；backend-main 当前身份为 `0e0afed2…`；两个 RD-Agent 节点健康。健康只证明 API 可达，不替代 commit identity；具体 BUG 的关闭状态以 task card/registry 与 post-restart receipt 为准。

| 工作流 | 当前状态 | 权威证据 | 当前结论 / 并行下一步 |
|---|---|---|---|
| QE 数据快照与申万 L2 键 | `COMPLETED_FOR_RESEARCH` | `dataset_as_of=2026-06-30`；R6 共 30 个 Loop 成功完成；`l2_code_id` 被 GAT embedding 和板块因子实际消费 | 足以继续 QE 研究；不代表非 QE 交易运行时或未来概念 PIT 数据已经就绪。 |
| 三个板块候选入因子库 | `RESEARCH_AVAILABLE` | catalog `1525/1528/1532`；统一指标批次 `049b25d8-1893-4369-a820-925f0e6b78d8`；每因子 583 个相关性配对 | 可用于 QE；catalog 的 `asset_status/transformation_status` 仍为 `pending/PENDING`，`realtime_code_text` 为空，不得宣称荐股、模拟盘或生产实时可用。 |
| R6 LGBM 因子析因 | `COMPLETED` | `qe_20260714_104829_a9ca`，5 个因子集 × 3 seeds，15/15 Loop 完成 | `G14-FP` 是风险收益与信号强度较均衡的 h20 锚点；`G15-FPL` RankIC 更高但收益转换未同步提高。 |
| R6 GAT 因子析因 | `COMPLETED` | `qe_20260714_104830_0230`，5 个因子集 × 3 seeds，15/15 Loop 完成 | `G12 + l2 embedding` 保留关系模型对照价值；新增 F/P/L 在 GAT 上未形成稳定的普遍增量。 |
| R2–R5 Loop 级补账 | `COMPLETED_LOOP_LEDGER_RECOVERED` | R2 CPU/GPU、R3 CPU/GPU、R4 CPU/GPU、R5 CPU/GPU 共 8 个 task、67/67 Loop；第 9.4.5 节 | LGBM 中板块因子稳定提升 RankIC，但资金流、广度、量价确认、领导持续性承担不同收益转化角色；GAT 对种子和关系结构敏感，不能用单一均值或单一最好 Loop 概括。 |
| 顶层失败任务有效 Loop 补账 | `TASK_FAILED_WITH_VALID_LOOPS` | 6 个顶层 `failed` task 内 50 个成功 Loop；第 9.4.5 节 | 成功的 LSTM、TCN、LGBM、LambdaMART 和单个正交模态结果全部恢复；失败 Transformer/GRU/其他 Loop 单列，不再覆盖成功结果。 |
| R7A 两腿 `equal + rank` | `COMPLETED_CURRENT_TRIAL_BELOW_BASELINE` | `macb_365aed6303e71d6e_20240701_20260629_20260714T174425343045Z` | 组合 Sharpe/Calmar 均低于 LGBM 基线；说明当前等权 rank trial 中“低重合/正交”未转化为成本后组合增益，不外推到其他融合或关系路线。 |
| R7B 两腿 `equal + zscore` | `COMPLETED_CURRENT_TRIAL_BELOW_BASELINE` | `macb_365aed6303e71d6e_20240701_20260629_20260714T190901628242Z` | CAGR 67.95%、Sharpe 1.8313、Calmar 3.5429；较 R7A 略改善，但 Sharpe/Calmar 分别落后 LGBM 基线 0.1902/0.1200。继续保存为融合方法、成本和换手互证样本。 |
| 30/40/60/120/180D 标签基础架构 | `IMPLEMENTED` | `ALLOWED_LABEL_HORIZONS`、`LongHorizonLabelMaturityPurge` 及对应测试 | 可训练长周期标签；标签期限不等于 LSTM 输入窗口或策略持仓期。 |
| F-014 长期趋势评价层 | `FROZEN_EXISTING_CAPABILITY_REUSABLE_NO_UI_HISTORY_OR_PLATFORM_COMPLETION_WORK` | Phase 1–3、R8B 6/6 CAS、Loop4 2,004 metric + 6 artifact、PR #2906 runtime contract 与 PR #2964 受控 Chromium 证据均只读保留 | 用户已终止当前 3000 visual、normal Loop 平台 smoke、其余 5 个 R8B、Phase 5、历史重评和批量物化；F-014 只作为新 trial 可选的现有评价函数复用，缺失指标不阻断下一候选。 |
| F-014 指标族证据 | `R8B_6_OF_6_THREE_COMPUTED_TWO_LIMITED_ONE_NOT_VERIFIABLE` | 第 9.6 节；6 个 evaluation/manifest；12,024 metrics；13,241,712 signals；13,726 episodes | 六个 Loop 的 `signal_path/portfolio_result/sector_regime` 已完整计算，`position_episode/order_fill` 带覆盖限制，`execution_cause` 缺直接原因证据；可用结果立即进入研究，限制项只在解释中保留，不启动历史补数。 |
| F-014 历史重评资产 | `FROZEN_NO_BACKFILL_NO_REMATERIALIZATION` | 12 task / 108 Loop 与 324 prediction/label/params blob 的既有审计结果只读保留 | 禁止执行 57 个 direct parser、51 个 results/backtest-only rematerialize、其余 5 个 R8B 或任何历史补账/物化；新策略直接复用已有 prediction。 |
| R8A 长周期 LGBM 对照 | `COMPLETED_12_OF_12` | `qe_20260715_101942_d873`；h30/h40/h60/h120 均 3/3；18 个 R8 Loop 预测资产深度分析 | 种子平均 RankIC 随 h30→h120 从 `0.09824` 升至 `0.12115`，但 2026H1 h30/h40/h60 ensemble RankIC 仅 `0.02331/0.02591/0.02916`；预测期限增强未自动形成近期主升浪捕获。 |
| R8B LSTM 长周期对照 | `COMPLETED_6_OF_6` | `qe_20260715_104922_001d`；h40、h60 均 3/3 完成 | h40 平均 RankIC/CAGR/Calmar/Sharpe/换手约 `0.10092/61.88%/3.17/1.6848/18.47`；h60 约 `0.10011/61.19%/2.98/1.7655/15.98`。该历史结果支持 LSTM 继续作为当前核心腿；后续模型训练必须服务明确的增量 Alpha 或 regime 假设。 |
| R8 预测资产深度归因 | `COMPLETED_SIGNAL_LEVEL` | 18/18 Loop 的 `pred.pkl/label.pkl/params.pkl`；2024-07-01 至 2026-06-30；第 9.6.2.1 节 | LSTM ensemble 增益高于 LGBM；LGBM/LSTM 同期限 Top50 重合约 21%；右尾捕获接近随机重合基线，且 2026H1 半导体/通信/软件主升浪显著漏捕。该证据是 signal-level；逐日 position/order 缺失范围继续显式保留，但不再执行历史补充。 |
| R9S 信号到策略转换 | `COMPLETED_24_OF_24` | LSTM `qe_20260715_190925_2cd9` 12/12；LGBM `qe_20260715_190941_b667` 12/12；均已完整归档 | LSTM h40 的 `n_drop=1` 平均 CAGR `64.70%`、换手 `6.40`，优于更高换手的 `n_drop=3`；LSTM h60 则由 `n_drop=3` 取得 CAGR `61.80%`、Sharpe `1.7445`，明显高于 `n_drop=1`。LGBM h60 偏向 `n_drop=3`，h120 偏向 `n_drop=1`；不存在跨期限统一调仓速度。 |
| R10 模型 × 因子角色 | `COMPLETED_51_OF_51` | LTR `qe_20260716_002956_b2eb` 6/6；LGBM `qe_20260716_052124_63d2` 12/12；TCN `qe_20260716_050817_4b7e` 12/12；LSTM `qe_20260716_011004_21b2` 12/12；持仓 9/9 | TCN `G13-F+h40` 平均 RankIC/CAGR `0.10838/65.19%`，`G15-FPL+h60` 为 `0.10298/60.20%`；LGBM 呈现 G15 排序增强、G13 收益转换分工；LSTM `G15-FPL+h60` 三种子平均 RankIC/CAGR/Sharpe `0.10206/67.55%/1.806`。这些结果继续用于腿角色与近期衰减诊断。 |
| R10 持仓与退出周期 | `COMPLETED_9_OF_9` | R10E2 `qe_20260716_045612_4ddd` 6/6；R10E3 `qe_20260716_083500_a8e0` 3/3 | hold10 三种子平均 CAGR/Sharpe `61.72%/1.7158`；hold30 三种子平均 `54.21%/1.566`、换手约 `5.995`。固定延长持仓稳定降低换手，但没有稳定提高收益或回撤，下一步使用趋势存活、回撤 hazard、动态退出和 false early-exit 解释。 |
| R11A 固定持仓转换 | `COMPLETED_9_OF_9_FULLY_ARCHIVED` | `qe_20260717_134109_97e1`；G14-FP h60；hold10/20/30 × seeds 123/314/2718；BUG-741/PR #2391 | hold10/20/30 平均 CAGR `58.44%/59.18%/49.30%`，Sharpe `1.738/1.708/1.496`，最大回撤 `-20.06%/-20.58%/-21.79%`。hold20 是可继续解释的固定基线，hold30 明显偏弱；下一步不是继续盲目延长，而是研究板块风险条件化的持有、减仓、退出和重入。 |
| R11B 图模型长期基线 | `COMPLETED_6_OF_6_FULLY_ARCHIVED` | `qe_20260717_134306_6354`；EfficientGATs G14-FP；h40/h60 × 3 seeds；`gpu_serial_graph=1` | h40/h60 平均 RankIC `0.10461/0.10156`、CAGR `54.73%/55.51%`、absolute Sharpe `1.588/1.677`、最大回撤 `-17.02%/-16.95%`、年化换手 `5.78/11.17`。h40 的效率明显更高；图模型现阶段价值更可能来自较浅回撤、关系状态和 sector gating，而不是单腿收益天花板。 |
| 图模型与关系研究 | `STATIC_RELATION_RECLASSIFIED_DYNAMIC_RELATION_NEXT` | R0–R6 GAT；R4 二值邻接；R11B 6/6；R12G 1/1；MA-E13G 三种子 | industry-bias 平均 RankIC 略降但 CAGR/primary information-ratio 提升，重分类为 `RELATION_PRIOR/PORTFOLIO_REGULARIZER`。下一步是只读冻结文件的 dynamic residual/flow/leadership 关系与回撤 hazard，不再做静态边直接分数融合。 |
| QE 任务与数仓状态 | `EXISTING_RESULTS_REUSABLE_NO_HISTORY_COMPLETION_WORK` | R10 51/51、R11A 9/9、R11B 6/6 与既有 prediction-store/数仓结果只读可用 | 后续实验直接引用现有 prediction 和最小 trial 输出；不再执行 Archive 补录、历史状态重算、总账固化或旧制品完整化。 |
| R12G EfficientGATs 执行 canary | `COMPLETED_1_OF_1_GPU_SERIAL_NO_TELEMETRY` | `qe_20260718_040323_9a4a`；RankIC `0.10569`、CAGR `62.12%`、Sharpe `1.8108`、最大回撤 `-15.78%`、年化换手 `5.72` | cooperative execution canary 已完成；没有调用 `nvidia-smi`、NVML 或任何 GPU/显存轮询。该结果说明新的 chunk/yield 执行方式没有使当前 Loop 丢失训练结果，图模型仍保持 WSL 1 串行。 |
| R12P 跨任务四腿组合 | `RECOVERY_COMPLETE_11_OF_21_EQUAL_AND_ORTHOGONALITY_LOO_ANALYZED` | 最终 successor `macb_recovery_ce14e0a572673ff88ab22b6a4089bb7276f1cec676c19d535315bffc47e448de`；BUG-864/#2736、BUG-865/#2740、BUG-867/#2744 | 11 个研究可用 child 已恢复：baseline 1、scheme 2、LOO 8；10 个 ic-weighted/risk-parity child 保留证据缺口。equal CAGR/Sharpe/Calmar `74.47%/1.922/3.554`，明显高于 LGBM baseline `53.57%/1.602/2.649`，也高于 orthogonality-aware `72.12%/1.893/3.302`。 |
| MA-E01 sector-risk policy | `COMPLETED_WITH_OPERATIONAL_RETRIES` | 17 条 durable run record：9 succeeded/3 failed/5 cancelled；代表性 `macb_idem_3cb2a8135fb4c30f902987a84bf36278fa5a4fce` | 正式 none/entry-gate/bounded-de-risk/exit-reentry 与 calibrated sparse de-risk 均形成真实结果；成功记录 CAGR 范围 `50.35%～77.62%`，calibrated sparse de-risk 为 `77.62%/2.040/3.606/-21.53%`（CAGR/Sharpe/Calmar/MDD）。早期失败/取消是恢复过程，不覆盖成功结果。 |
| MA-E02 Alpha LOO entry-gate | `COMPLETED_1_OF_1` | `macb_idem_eda0dc9ccbe0642022aa73bc76ad7deb13c5757b` | CAGR/Sharpe/Calmar/MDD=`74.92%/1.994/3.481/-21.52%`；完成初次策略层 Alpha LOO/entry-gate 对照。 |
| MA-E03 RRF 权重矩阵 | `COMPLETED_4_OF_4` | `MA_E03_RRF_EQ25/L30_T20/L35_T15/L40_T10` 四个 durable run | CAGR 范围 `61.52%～64.14%`；其中 L40/T10 为 `64.14%/1.798/3.259/-19.68%`。该轮未超过后续 soft de-risk Pareto，保留为权重分配对照。 |
| MA-E04 drop-TCN policy matrix | `COMPLETED_4_OF_4` | none/entry-gate/bounded-de-risk/exit-reentry 四个 durable run | CAGR 范围 `56.39%～72.14%`；entry-gate 为 `72.14%/2.017/3.533/-20.42%`。结果支持三腿/drop-TCN 继续进入 MA-E10 稳健化。 |
| MA-E05 orthogonality WSL recovery | `COMPLETED_2_VALID_AFTER_11_FAILED_ATTEMPTS` | 最终成功 entry-gate `macb_…_1d10e90c` 与 none `macb_…_c5a712fa` | 两个有效结果 CAGR `74.51%～77.54%`；entry-gate 为 `77.54%/2.035/3.606/-21.51%`。11 个失败记录保留为执行/恢复证据，不重复计为策略负结果。 |
| MA-E06 dual-node IC/risk attempt | `FAILED_2_OF_2_NO_VALID_METRICS` | `macb_idem_55bedd…`、`macb_idem_e7885…` | 两条 run 均失败，没有可报告收益；不得用 MA-E07 结果回填 MA-E06。后续 MA-E07 另立真实 run 建立 Pareto，不把本轮失败解释为策略失败。 |
| MA-E07 四腿 soft de-risk | `SUCCEEDED_FULL_WINDOW_PARETO` | `macb_idem_4a1904f03489be8992db3f650f0239dcf6dc84b9` | 2024-07-01～2026-06-29：CAGR/Sharpe/Calmar/MDD/turnover=`79.06%/2.093/3.573/-22.13%/7.0104`；当前全窗口收益锚点。 |
| MA-E08 orthogonality mid de-risk | `COMPLETED_2_OF_2_BELOW_PARETO` | C90/H70/C40 `macb_idem_dc070c0f…`；C85/H60/C30 `macb_idem_473ef860…` | CAGR 范围 `64.88%～66.54%`；较好者为 `66.54%/1.950/3.124/-21.30%`，未超过 MA-E07，保留为中等减仓强度负/边界对照。 |
| MA-E09 early/late robustness | `COMPLETED_4_VALID_2_FAILED_RETRIES` | early entry/H85-C60 与 late entry/H85-C60 四个最终成功 run | early CAGR 为 `84.08%/84.30%`，late 仅 `12.20%/11.82%`；late entry-gate 为 `12.20%/0.827/0.638/-19.13%`。该轮直接确认全窗口/早期高收益掩盖近期 regime drift。 |
| MA-E10 drop-TCN 三腿 entry-gate | `SUCCEEDED_FULL_WINDOW_PARETO` | `macb_9dbb8759f4f84c16_20240701_20260629_20260804T213425440659Z_6f8d08ae` | CAGR/Sharpe/Calmar/MDD/turnover=`75.95%/2.086/3.634/-20.90%/6.3091`；相对 MA-E07 牺牲少量收益，改善 Calmar、回撤与换手，作为稳健三腿锚点。 |
| MA-E11 编号覆盖 | `NO_DEDICATED_TASK_OR_RUN` | 2026-08-12 QE task 列表与 `limit=200` 返回的完整 140 条 combine-run 列表均未发现 `MA_E11*` | 该编号没有形成独立实验、进度或收益，不得推断为完成、失败或被其他轮次替代；活动研究从 MA-E10 直接进入 MA-E12。 |
| MA-E12 G18-SDR 新增腿 | `SUCCEEDED_BUT_DEGRADED` | `macb_idem_06623a223e94d9c84cdda714e27fae69c34eb413` | CAGR/Sharpe/Calmar/MDD=`63.92%/1.759/2.980/-21.45%`；证明机械追加因子/模型腿会稀释已有组合，不继续沿用“更多腿即增量”的假设。 |
| MA-E13G EfficientGATs industry-bias 三种子 | `COMPLETED_3_OF_3_RELATION_PRIOR` | seed123 `qe_20260807_141614_c062`；seed314/2718 `qe_20260809_035858_2d90` | 相对 matched R11B embedding-only，平均 CAGR/primary information-ratio 提升约 `9.38pp/0.251`，RankIC 下降约 `0.00374`；该 primary 指标与上一行 absolute Sharpe 分开解释。静态行业关系仅作为关系先验、风险状态或组合正则继续研究。 |
| MA-E13R drop-TCN 近期窗口 | `SUCCEEDED_LATE_WINDOW_WEAK_ALPHA` | `macb_idem_bf8128c3456de563139e841fe89146d3444e9659` | 2025-07-01～2026-06-29：CAGR/Sharpe/Calmar/MDD=`12.35%/0.831/0.719/-17.18%`；较四腿 MA-E09 的 `12.20%/0.827/0.638/-19.13%` 风险改善但收益近乎不变，首要缺口是 regime 适配与新 Alpha。 |
| MA-E14 H95/C85 regime overlay | `COMPLETED_CURRENT_POLICY_DEGRADED` | full `macb_9dbb8759f4f84c16_20240701_20260629_20260809T155633738307Z_6535682a`；early `macb_9dbb8759f4f84c16_20240701_20250630_20260809T171815780108Z_fa5a7f6c`；late `macb_9dbb8759f4f84c16_20250701_20260629_20260809T155638277938Z_c5529e19` | 相对 matched anchor 的 CAGR 在 full/early/late 分别降低约 `4.34pp/2.25pp/1.10pp`，且风险/换手没有形成补偿性改善。结束当前阈值 overlay 调优，不外推否定所有 regime 状态研究。 |
| MA-E15 压缩扩张独立专家 | `COMPLETED_9_OF_9_PANEL_CONFOUNDED_EXECUTION_TRUTH_LIMITED` | `qe_20260810_020755_38d2`；远端 LGBM Loop1～6、WSL LSTM Loop7～9；全部使用 V25_1 分钟执行 | CE3-LGBM 三种子均值 IC/RankIC=`0.03880/0.06978`；CE4-B LGBM=`0.03818/0.06814`；CE4-B LSTM=`0.03644/0.07804`，这些信号与面板诊断证据可继续使用。实际观察到的 CAGR/Sharpe/MDD/Calmar/turnover 分别为 CE3-LGBM `46.91%/1.676/-15.04%/3.121/5.249`、CE4-B LGBM `29.64%/1.176/-19.12%/1.554/6.151`、CE4-B LSTM `36.78%/1.423/-14.46%/2.539/4.781`；其中 Calmar 是逐 Loop `CAGR / abs(enhanced MDD)` 后取均值，不是顶层 API `calmar` 的均值。CE3/CE4-B 总体不同，且 V25_1 受 BUG-852 零日频特征影响，因此这些组合指标只作历史观测，既不能归因 breadth，也不能作为执行真实性基线。 |
| MA-E15 观测面板诊断 | `COMPLETED_ROOT_CAUSE_CONFIRMED` | Prediction Store Loop1～9 manifests/labels/predictions；BUG-1006/#3239 | CE3 有 `2,203,063` 行，CE4-B 有 `2,204,574` 行；CE3 index 是 CE4-B 的严格子集，额外 `1,511` 行（`0.068539%`），共同样本标签逐值一致。额外行中 `1,091` 行 h20 标签有效；LGBM 把这些额外有效行全部排入逐日 Top20，说明极小的总体差异也能显著改变组合。LSTM Loop9 只有 3 个额外样本进入 Top20，进一步表明模型与缺失填零交互不能被当作因子效应。 |
| BUG-852 V25/V25_1 分钟执行真实性 | `OPEN_BYPASSED_FOR_MA_E16_SCOPE_AUDIT_REQUIRED` | BUG-852 / Issue #2692；旧 PR #2695 未合入且已严重落后主线 | 当前 V25 基类仍向分钟执行器提供零 10D 日频特征，V25_1 继承该路径。MA-E15 的组合指标因此受限；R8A/R8B 及部分 R9～R12 也可能经过相同执行分支，后续只在引用这些历史组合指标时逐 task 核验并标记 `EXECUTION_TRUTH_LIMITED`，不得把全部历史结果一刀切判无效。IC/RankIC 与 prediction 仍可使用。MA-E16 使用日频 `CLOSE_PRICE` 避开该路径；本蓝图不把旧 PR、未合入源码或绕行写成修复。 |
| BUG-1006 matched observation panel | `MERGED_RUNTIME_VERIFIED` | BUG-1006 / Issue #3239 / PR #3243；fix commit `c76068f6…`；`qe_observation_panel_v1`；用户重启后 MA-E16 已实际按该合同装配 | 新 trial 可声明预先指定的参考因子交集；`observation_panel` 只属于生成期 data-loader contract，不进入策略 kwargs；缺配置因子、缺参考列或空面板均以稳定 reason code fail closed。未声明的旧任务继续允许已成功因子的历史 partial concat。该最小阻断 BUG 已完成源码、合入和运行态验证，当前不再是 MA-E16 前置；不扩展 UI、Archive、schema 或历史工程。 |
| MA-E16 matched CE3/CE4-B 复验 | `TERMINAL_10_COMPLETED_2_FAILED_RECOVERY_REQUIRED` | `qe_20260810_224522_b0d9`；2026-08-11 05:45 +08:00 终态；第 9.9.2～9.9.3 节矩阵 | 12 个 Loop 已全部 terminal：10 completed、Loop3/7 failed。成功 arm 均为 `2,192,858` 行相同 matched panel、相同窗口与 label SHA256，统一使用日频 `CLOSE_PRICE`、h20 裸收益、Top50/n_drop1、相同成本与 2026-06-30 文件快照。四个现有 matched pair 的 CE4-B 相对 CE3 CAGR 均提高，但 IC/RankIC 混合且 MDD 3/4 恶化；breadth 主角色暂归 `CONDITIONING_STATE`，portfolio conversion 为待验证用途。在 Loop3 `backtest_only`、Loop7 `full_train` 单 arm 恢复并完成三窗口、成本、相关性与 overlap 前，不形成独立 Alpha 或 blend/LOO 结论。 |
| BUG-1014 WSL 主机响应与 qrun 隔离 | `SOURCE_LOADED_RD_HEALTH_OK_CLOSE_SYNC_PENDING` | BUG-1014 / Issue #3266；AIstock merge `49ccc1f3…` 已包含于当前 backend identity `0e0afed2…`；RD-Agent 修复 `da3270f8…` 已进入 release `795973e1…`；两节点 `/health=ok`；任务重叠与 Windows/WSL 原生资源快照 | 根因不是图模型：GeneralPTNN LSTM 的 `n_jobs=2 + pin_memory + prefetch_factor=2` 使单 Loop 形成主进程加两个约 3 GB worker；跨 task 的 WSL 全局容量 2 允许两个本地 Loop 重叠，叠加既有 WSL swap 后触发高频换页。修复保持 batch/标签/模型/策略语义，canonical `wsl2-5080` 全局 1 槽、本地 GeneralPTNN 单进程 DataLoader、无 pin/prefetch、CPU thread 上限，远端容量 4 与两节点并行不变。源码与运行加载事实已具备；registry 仍为 `open/pending_user_restart`，只需 post-restart receipt/close-sync，不得继续扩张研发或阻断 Loop7 单 arm 恢复。 |
| 日频 qrun MLflow 指标落盘竞态 | `BUG_1022_BACKEND_SOURCE_LOADED_CLOSE_SYNC_AND_BACKTEST_ONLY_PENDING` | BUG-1022 / Issue #3282；merge `a7bc06a3…` 已包含于当前 backend identity `0e0afed2…`；MA-E16 Loop3；`scripts/qrun_limit.py`；分钟 runner 的既有 BUG-605 修复合同 | Loop3 的训练、预测与 IC/RankIC 已完成；失败来自日频 runner 缺 async metric drain barrier 与精确 empty-metric retry。运行态已加载修复，registry 仍为 `open/pending_user_restart`；先补 post-restart receipt/close-sync，并立即使用重新装配的最新 runner 显式 `retry_mode=backtest_only`。完整 params 缺失则 fail loud，禁止 fixed sleep、吞错、写零、`auto` 回退 full train 或重训整个 Loop。 |
| QE 空闲查询、观察器与实时日志阻断收口 | `PARTIAL_CLOSE_SYNC_NO_NEW_ENGINEERING` | BUG-1023/1024 source 已被 backend identity `0e0afed2…` 加载但 registry 仍 open；BUG-1026/1031 为 `verified` 且 closed；BUG-1030 为非运行态 `fixed` 且 closed；两节点 RD `/health=ok` | BUG-1023/1024 只补逐 BUG post-restart receipt/close-sync；BUG-1026/1030/1031 不再进入 backlog。60 秒探测、变化才写状态、无订阅者零日志连接/零文件写、单上游 broker 与 5×1 GiB 环形槽是已交付的稳定性边界，不启动通用监控、历史日志归档、UI 或平台研发。 |
| 多 Alpha QE 演进平台基础 | `P0_1_TO_P0_4_COMPLETE_MAINTENANCE_ONLY` | 第 4.11、9.9、Phase G0-H；已合入基础 PR 与后续阻断 BUG 修复 | 当前能力足以继续演进。只修真实 trial 暴露且无安全替代路径的阻断 BUG；禁止通用编排、UI、Archive、历史补账或 provenance 完整化研发抢占实验。 |
| 两层板块四格 oracle 与现实 top-down | `P0_DIAGNOSTIC_DESIGN_READY_NOT_RUN` | 第 9.5、9.9.2～9.9.3 节；F-018/F-039 | 由 v6.5 的 P2 条件方向提升为 MA-E16 收口后的 P0 诊断；同口径比较 reality/oracle sector × reality/oracle stock、hard Top-M/soft gating、申万 L1/L2、成本与可交易约束。oracle 永久标记未来信息上界，不冒充可部署收益，也不形成方向停止门禁。 |
| 模型年龄/重训可恢复成分与基准相对归因 | `P0_DIAGNOSTIC_DESIGN_READY_NOT_RUN` | MA-E16 LSTM Loop7～12 配置的固定 `train=2018-08-01～2022-12-31`、`valid=2023-01-01～2024-06-30`、`test=2024-07-01～2026-06-30` 与 `backtest.benchmark=null`；F-034/F-035 | 当前“近期弱化=regime drift”尚未排除 model-age、可由新近数据恢复的漂移与 beta 解释。以相同冻结文件/因子/标签/策略/成本做模型年龄切片及因果 rolling/expanding refit，再并列报告绝对收益、主动收益、beta、tracking error、IR 与 Brinson；本行仅是已预注册诊断，不是已完成结果。 |
| R8M / HIST-concept / HATS / 概念超图与高成本架构 | `P2_CONDITIONAL_NON_BLOCKING` | 第 9.6～9.8 节；F-016/F-019 | 只在现有冻结文件与空闲资源足够、且不延迟 P0 诊断/P1 candidate 时运行；动态 residual/flow/lead-lag 关系属于 P1-B，不被本行降级。概念/事件数据缺失不触发前置平台、数据库回填、历史采集或历史补算。 |

MA-E16 当前四组可比 matched pair 的事实差值如下；缺失 seed 不做组均值填补：

| Pair | ΔCAGR | ΔIC | ΔRankIC | MDD 变化 |
|---|---:|---:|---:|---:|
| LGBM / seed123 | `+2.1216pp` | `+0.00002` | `-0.00034` | 改善 `0.802pp` |
| LGBM / seed314 | `+2.6861pp` | `+0.00019` | `-0.00038` | 恶化 `1.184pp` |
| LSTM / seed314 | `+5.1712pp` | `+0.00542` | `+0.00721` | 恶化 `3.793pp` |
| LSTM / seed2718 | `+0.6313pp` | `-0.00302` | `-0.00143` | 恶化 `0.232pp` |

#### 2.5.1 截至目前的策略收益总览

本表汇总当前仍对后续演进有直接决策价值的策略/模型收益。R2～R11 的逐阶段指标继续保留在上方总账和第 9.6 节，不因本表压缩而删除。`VALID` 表示可作为相应窗口的真实策略比较；`SIGNAL_ONLY/EXECUTION_LIMITED` 表示只允许使用所列信号证据；`INCOMPLETE` 表示缺 arm 时禁止组均值、正式 blend 或 LOO。

| 阶段 / 候选 | 窗口与口径 | CAGR | Sharpe / IR | Calmar | MDD | 证据结论 |
|---|---|---:|---:|---:|---:|---|
| R12P LGBM baseline | 2024-07-01～2026-06-29，真实组合 | `53.57%` | `1.6020` | `2.6494` | `-20.22%` | `VALID`，历史单腿组合锚点。 |
| R12P 四腿 equal | 同窗口、同成本 | `74.47%` | `1.9224` | `3.5544` | `-20.95%` | `VALID`；相对 baseline 为 `+20.90pp/+0.3204/+0.9051`，证明跨任务组合存在真实互补。 |
| R12P orthogonality-aware | 同窗口、同成本 | `72.12%` | `1.8927` | `3.3017` | `-21.84%` | `VALID`；低于 equal，名称上的正交不自动产生更高经济收益。 |
| MA-E07 四腿 soft de-risk | 2024-07-01～2026-06-29 | `79.06%` | `2.093` | `3.573` | `-22.13%` | `VALID`，当前最高全窗口 CAGR 锚点。 |
| MA-E10 drop-TCN 三腿 entry-gate | 同窗口 | `75.95%` | `2.086` | `3.634` | `-20.90%` | `VALID`，当前最高 Calmar、较低回撤/换手的稳健锚点。 |
| MA-E12 G18-SDR 新增腿 | 同窗口 | `63.92%` | `1.759` | `2.980` | `-21.45%` | `VALID_NEGATIVE_CONTROL`，机械增腿退化。 |
| MA-E13G industry-bias 三种子 | matched R11B h40 | `64.11%` 均值 | primary IR `1.652` | — | — | `VALID_RELATION_PRIOR`；相对 embedding-only CAGR `+9.38pp`、IR `+0.251`，但 RankIC 从 `0.10461` 降至 `0.10088`。 |
| MA-E13R drop-TCN | 2025-07-01～2026-06-29 | `12.35%` | `0.831` | `0.719` | `-17.18%` | `VALID_LATE_WINDOW`；收益与 MA-E09 `12.20%` 接近，揭示近期 Alpha/regime drift，而非全期结果失效。 |
| MA-E14 H95/C85 overlay | full/early/late matched delta | `-4.34pp/-2.25pp/-1.10pp` | — | — | 无补偿改善 | `VALID_NEGATIVE_CONTROL`，结束当前阈值扫描。 |
| MA-E15 CE3-LGBM | 9/9 中三种子组均值，V25_1 | `46.91%` | `1.676` | `3.121` | `-15.04%` | `SIGNAL_ONLY/EXECUTION_LIMITED`；面板混杂且受 BUG-852，不能作为执行基线。 |
| MA-E15 CE4-B LGBM | 同上 | `29.64%` | `1.176` | `1.554` | `-19.12%` | 同上；不可归因 breadth。 |
| MA-E15 CE4-B LSTM | 同上 | `36.78%` | `1.423` | `2.539` | `-14.46%` | 同上；IC/RankIC/Top-K 仍有效。 |
| MA-E16 matched CE3/CE4-B | 2024-07-01～2026-06-29，日频 `CLOSE_PRICE` | 已完成 arm `30.71%～53.93%` | `1.1769～1.7465` | `1.7300～3.9707` | `-18.43%～-13.58%` | `INCOMPLETE_10_OF_12`；范围只描述成功 arm，不是组合结论，详见下一表。 |

截至当前，最佳已验证“收益最大化”候选仍是 MA-E07（CAGR `79.06%`），最佳已验证“风险收益效率”候选是 MA-E10（Calmar `3.634`、MDD `-20.90%`）。二者都是全窗口结果；近期窗口 MA-E13R 只有 `12.35%` CAGR，说明新的策略包不能只追求全窗口最高收益，必须同时改善 late-window regime 适配。MA-E16 尚不具备替换两条 Pareto 锚点的完整证据。

#### 2.5.2 MA-E16 全 12-arm 实时收益明细（2026-08-12 回读）

下表直接来自 8001 task detail。收益列统一使用 `enhanced_metrics.absolute_returns`：CAGR、Sharpe、MDD 为绝对组合口径，Calmar 按 `CAGR / abs(MDD)` 计算；IC/RankIC 使用 Loop 顶层信号指标。它们与 API 顶层 excess/primary 字段不是同一口径，禁止混算。失败 arm 保持空值，不从训练日志或相邻 seed 推断。

| Loop | 模型 / 因子组 / seed | 状态 | IC | RankIC | CAGR | Sharpe | MDD | Calmar |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | LGBM / CE3 / 123 | completed | `0.03980` | `0.06997` | `51.8049%` | `1.7423` | `-14.3832%` | `3.6018` |
| 2 | LGBM / CE3 / 314 | completed | `0.03981` | `0.06986` | `38.0450%` | `1.3593` | `-15.6922%` | `2.4245` |
| 3 | LGBM / CE3 / 2718 | failed，待 `backtest_only` | — | — | — | — | — | — |
| 4 | LGBM / CE4-B / 123 | completed | `0.03982` | `0.06964` | `53.9265%` | `1.7465` | `-13.5812%` | `3.9707` |
| 5 | LGBM / CE4-B / 314 | completed | `0.04000` | `0.06948` | `40.7311%` | `1.3831` | `-16.8761%` | `2.4135` |
| 6 | LGBM / CE4-B / 2718 | completed | `0.04005` | `0.06909` | `46.0256%` | `1.5340` | `-16.5962%` | `2.7733` |
| 7 | LSTM / CE3 / 123 | failed，待 `full_train` | — | — | — | — | — | — |
| 8 | LSTM / CE3 / 314 | completed | `0.03528` | `0.07668` | `31.3947%` | `1.2762` | `-14.6323%` | `2.1456` |
| 9 | LSTM / CE3 / 2718 | completed | `0.04025` | `0.07936` | `30.7067%` | `1.1769` | `-17.7497%` | `1.7300` |
| 10 | LSTM / CE4-B / 123 | completed | `0.03788` | `0.07881` | `37.1111%` | `1.3703` | `-17.5376%` | `2.1161` |
| 11 | LSTM / CE4-B / 314 | completed | `0.04069` | `0.08389` | `36.5659%` | `1.3134` | `-18.4253%` | `1.9845` |
| 12 | LSTM / CE4-B / 2718 | completed | `0.03724` | `0.07793` | `31.3380%` | `1.2728` | `-17.9820%` | `1.7427` |

该明细确认四个 matched pair 的方向差异，但不能填补 LGBM seed2718 CE3 和 LSTM seed123 CE3。完整 12-arm 结果仍是下一轮组合归因、固定 blend/LOO 和新 Alpha matched-control 的必要输入；这不是新增科研审批，而是避免用不平衡矩阵伪造收益结论的最小可解释性要求。

### 2.6 F-014 历史评价资产清单（冻结记录，旧执行顺序已取消）

本节只保留 v5.10 时形成的资产分组，便于解释既有结论覆盖范围；它不是 v6.1 backlog。原定的 direct parser、`backtest_only/results_only rematerialize`、其余 R8B 与批量评价顺序已由 v5.24 终止，不再执行，也不因缺失指标形成补数、补账或平台任务。

历史上无需 rematerialize、可直接进入固定 parser 的 57 个 Loop（其中 R8B 6 个已完成；其余 51 个现为冻结未执行资产）：

- R8B LSTM：6 Loop；
- R9S-LSTM：12 Loop；
- R10-TCN：12 Loop；
- R10-LSTM：12 Loop，其中 L5/L7/L9 保留 partial catalog warning；
- R10E2 LSTM fixed-hold：6 Loop，其中 L4 保留 partial catalog warning；
- R10E3 LSTM fixed-hold：3 Loop；
- R11B EfficientGATs：6 Loop。

历史上需要先做 `backtest_only/results_only rematerialize` 的 51 个 Loop（现全部冻结，不再物化）：

- R8A LGBM：12 Loop；
- R9S-LGBM：12 Loop；
- R10 LambdaMART：6 Loop；
- R10 LGBM：12 Loop；
- R11A LGBM fixed-hold：9 Loop。

旧 rematerialize 方案原本只会复用 Prediction Store 中 108/108 Loop 的 prediction、label、params 以及冻结 config/Recorder identity，不重新训练、不改写原 Archive run、不伪造原 workspace；该语义仅用于解释为何既有缺口不能被推断数据替代。现有缺失 report/positions/indicators/order/trade 与 execution cause 继续保持历史 `MISSING/PARTIAL/NOT_VERIFIABLE`，不得从日线或涨跌停状态猜测，也不得据此重启物化工程。

旧顺序曾计划在 R8B 6/6 后处理 R11B、R9S-LSTM、R10-TCN、R10-LSTM、R10E3、R10E2，再 rematerialize R8A、R11A、R9S-LGBM、R10-LGBM 和 LambdaMART；该顺序现已取消。v6.1 只复用已存在结果，直接执行第 9.9.2～9.9.3 节定义的新 candidate。

## 3. 证据口径与基线因子

因子库搜索摘要可能展示最新 `recent_1m` 记录，不能直接当作 out-sample 证据。本规格中的历史对比必须使用 `factor_library_get_metric_summary` 或明确指定 `eval_window=out_sample` 的统一指标，并同时记录 `snapshot_date`、`universe`、`return_horizon` 和 `calc_batch_id`。

首批研发前需要固定以下基线组：

| 作用 | 基线因子 | 用法 |
|---|---|---|
| 行业动量/反转基线 | `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` | 判断新板块级信号是否只是窗口或单调变换重复。 |
| 行业相对估值基线 | `m_ind_pb_rel_mom` | 检查相对价格/估值混叠及相关性红海。 |
| 个股相对行业基线 | `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_sector_momentum_spread` | 复用现有 F4 同族因子，不再换名复制。 |
| 板块资金流基线 | `m_sw2_net_vol_momentum`、`m_ind_flow_deviate`、`m_sector_mf_divergence_lg` | 作为 F3 低成功率方向的历史证据。 |

历史 1d 指标、h20 指标、QE 组合使用背景和受控消融分别回答不同问题，全部保留并互相印证；任何单一结果都注明其因果解释边界，不作为是否继续研究的条件。

2026-07-11 Gate-0 因子库 MCP 只读复核进一步确认：

| factor | eval_window | snapshot_date | universe | return_horizon | IC / RankIC | calc_batch_id | calculated_at |
|---|---|---|---|---|---|---|---|
| `m_stock_vs_industry_mom_20d` | out_sample | 2026-04-30 | `shsz_st_pit_active_v1` | 1d | -0.03802977 / -0.03668953 | `cf25429d-928c-4938-88ee-96514e65d214` | 2026-06-20T05:00:57.811464+08:00 |
| `m_mom_residual_20d` | out_sample | 2026-04-30 | `shsz_st_pit_active_v1` | 1d | -0.03886011 / -0.03851000 | `cf25429d-928c-4938-88ee-96514e65d214` | 2026-06-20T04:52:25.170443+08:00 |

查询 receipt：2026-07-11 调用 `factor_library_get`、`factor_library_get_metric_summary` 与 `factor_corr_get_clusters(min_abs_corr=0.8)`；相关性快照在 catalog 中记录为 2026-06-20。上表只用于查重和发现旧口径问题，不是 h20 验收。

- `m_stock_vs_industry_mom_20d`（manual，id=1247）仍为 `is_available=true` 但 `asset_status=pending`；其 catalog `realtime_code_text` 沿 instrument 对 `sw2_close` 做 20 日 `pct_change`，与第 4.1 节 PIT 契约冲突，因此 transformation `SUCCESS` 不能视为口径正确。
- 该因子与 `m_mom_residual_20d` 的官方指标仍只有 `return_horizon=1d`。out-sample 1d IC/RankIC 分别约为 `-0.03803/-0.03669` 与 `-0.03886/-0.03851`，形态和方向高度接近；这些历史值只支持旧 1d 口径描述，PIT 修复和 h20 重算将形成新的并列证据，不存在 PASS/FAIL 处置。
- `min_abs_corr=0.8` 的相关簇把 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_ind_pb_rel_mom` 归入同一簇；`Industry_Momentum` 与 `SW2_MOM5` 也在同一高相关簇。该证据支持 F4 `REUSE`、B2 条件增量以及 A3 相对 R1 去重，不支持换名新增。
- 英文泛化搜索 `sector breadth`/`industry momentum` 返回 0 条不能解释为“因子库不存在同族因子”；同时记录精确名称、中文描述、公式线索和相关簇搜索结果，以提高谱系分析完整度。

### 3.1 2026-06-30 快照的正式因子库证据

下表来自同一官方指标批次 `049b25d8-1893-4369-a820-925f0e6b78d8`，股票池为 `shsz_st_pit_active_v1`，`snapshot_date=2026-06-30`，h20 契约为 `T21T1`。数值是 catalog 当前可执行因子值的独立指标，不是 R6 模型归因；正负方向不得在看到 test 后临时翻转。

| 因子 | catalog id | out-sample h20 IC / RankIC | recent 6m h20 IC / RankIC | HAC ICIR（out-sample） | 相关性状态 |
|---|---:|---:|---:|---:|---|
| `m_sector_flow_price_divergence_10d_20d`（F） | 1532 | +0.01311 / +0.01217 | -0.00774 / -0.01294 | +0.05469 | 583 pairs，2026-07-12 完成 |
| `m_sector_breadth_persistence_10d_20d`（P） | 1528 | -0.00784 / -0.00976 | +0.07353 / +0.06024 | -0.02201 | 583 pairs，2026-07-12 完成 |
| `m_stock_sector_leadership_persistence_20d_10d`（L） | 1525 | -0.04285 / -0.06153 | +0.01420 / -0.01239 | -0.17459 | 583 pairs，2026-07-12 完成 |

三项均为 `is_available=true`，其含义是“研究可选择”，不是“生产实时资产完整”。截至本次复核，三项的 `asset_status=pending`、`transformation_status=PENDING` 且 `realtime_code_text` 为空；R6 成功证明 QE 离线资产可运行，不证明荐股/模拟盘/选股加载链可用。P 因子存在全 OOS 与 recent 6m 方向差异，L 因子独立 h20 为显著负向；它们只能通过预注册方向、非线性交互和组合消融解释，禁止用单次近期窗口覆写全期处置。

### 3.2 v6.0 因子库结构与新增 Alpha 证据

2026-08-09 只读运行 `scripts/analyze_factor_library.py --json`：catalog 共 788 个因子，585 个当前可用。类别数量集中在 MF/VAL/LIQ/CHIP（分别 164/159/91/91），而 CORR/QUAL/ML 仅 15/35/4；这不证明稀缺类别必然更优，但说明继续在拥挤公式族内堆数量的边际信息较低。585 个可用因子的 1d 平均绝对 IC 从 full 的 `0.01403` 降到 recent-6m 的 `0.00786`，`abs(IC)>0.02` 数量从 173 降到 37；近期衰减是因子库层面的广泛现象，不能只归因于 TCN 或单个组合器。

同一官方 h20 批次给出以下优先候选。指标只证明当前文件版本和窗口下的信号线索；是否成为新腿仍要由独立专家、matched-seed 和组合 LOO 解释。

| 候选 | full / out-sample / recent-6m h20 IC | 当前角色 | 主要限制 |
|---|---:|---|---|
| `m_intraday_range_60d_min_ratio` | `0.05566 / 0.07385 / 0.05651` | `DIRECT_ALPHA` 压缩扩张专家输入 | 独立 Top-K 收益转化弱于 IC，需要资金/breadth 确认和组合验证 |
| `Industry_Turnover_SmallBuy_Flow_Ratio_Factor` | `0.03453 / 0.06024 / 0.08714` | 新参与差因子的研究先验 | 原始 ratio 单位与分母语义不稳，必须新建单位一致公式，不直接复用原值 |
| `m_sector_mf_divergence_lg` | `0.03031 / 0.04600 / 0.07103` | `DIRECT_ALPHA/CONDITIONING_STATE` | 需与现有资金流族做相关性、规模和近期 regime 互证 |
| `m_sector_residual_cohesion_10d_60d` | `0.02314 / 0.04495 / 0.04900` | `CONDITIONING_STATE` | 独立 top-excess Sharpe 为负，禁止仅凭 IC 作为新腿 |
| `m_sector_breadth_persistence_10d_20d` | `-0.01643 / -0.00784 / +0.07353` | regime 状态/方向翻转诊断 | full/OOS 与近期符号翻转，不得固定为无条件正向 Alpha |
| `m_stock_sector_leadership_persistence_20d_10d` | `-0.04969 / -0.04285 / +0.01420` | 领导耗竭/拥挤负对照 | 近期 Pearson/Rank 方向混合，必须预定义条件交互并保留原方向 |

MA-E12 已证明，把多个看似合理的因子机械追加为 G18-SDR 第四腿会使 CAGR/Sharpe/Calmar 降至 `63.92%/1.759/2.980`。因此 v6.0 的新增信号默认先形成独立专家或条件状态，不直接扩充现有大因子集。

## 4. Architecture / 架构与统一设计原则

### 4.1 先构造板块面板，再做时序运算

F1/F3/F4 原口径中“先沿股票计算 `sw2_*` rolling/pct_change，再去重到板块”的顺序必须废止。股票发生行业变更时，该写法会把两个行业指数接在同一股票窗口中，产生跨行业伪收益。

所有板块级 `sw2_*` 计算统一采用：

1. 从当日股票记录中取 `l2_code_id` 与目标字段；过滤 `l2_code_id == -1`。
2. 按 `(datetime, l2_code_id)` 构造每日一个板块值；记录每个目标 `sw2_*` 字段的 `nunique(dropna=True)`。冲突组不静默取 `first`，而是标记受影响日期/板块、统计损失并使用可用一致组继续分析，同时启动源数据核查与补算。
3. 在 `(datetime, l2_code_id)` 板块面板上按 `l2_code_id` 做 `shift`、`rolling`、`Slope` 等时序计算。
4. 当日跨板块排名时，每个板块只占一个样本。
5. 按 `(datetime, l2_code_id)` 映射回个股 MultiIndex。

所有收益计算必须显式使用 `pct_change(fill_method=None)`；板块缺日、股票停牌或断点不得通过默认前向填充伪造收益。每天还必须记录有效板块数、unknown 数量、成员覆盖率和小样本板块占比。

成员聚合类因子则先在个股时序上计算成员状态，再按当日 PIT `l2_code_id` 聚合；不得用当前成分回填历史。

### 4.2 `l2_code_id` 语义与失败策略

- `l2_code` 是权威申万 L2 行业代码；`l2_code_id` 是稳定映射后的整数类别键。两者不能交替当作同一种物理字段使用。
- `l2_code_id` 只作为离散分组键。
- `-1` 必须在分组、排名和映射前排除；不得成为“未知板块”样本。
- parquet 路径若返回 float dtype，必须验证所有有限值均为整数语义后再显式转换；不得静默截断小数。
- 发现列缺失、非整数编码、板块字段同日不一致或覆盖率不足时，记录 `reason_code`、受影响指标族/样本、可用部分和补数方案；不伪造空列或全 NaN 成功结果，也不据此停止其他样本、指标或研发方向。

### 4.3 PIT、标签与信息泄露

- 因子名中的 `5d/20d/60d` 表示特征回看或变化窗口；`h20` 表示预测标签的持有期限，两者不得混称。
- `full/out_sample/recent_6m/recent_3m/recent_1m` 是评估窗口；`1d/5d/10d/20d` 是收益期限画像，两套维度必须分别记录。
- 所有 rolling 只使用当日及历史数据；特征严禁 `shift(-N)`。
- h20 标签统一为 T+1 到 T+21 的裸前向收益：`close[t+21] / close[t+1] - 1`。标签构造可使用未来价格，但只能存在于评估器，不得进入因子代码。
- 因子开发、快筛、统一指标和 QE 对照实验优先使用相同股票池、交易时点、复权口径和数据快照；不一致时保留差异并做对齐/敏感性分析。
- 20 日标签高度重叠，ICIR 同时报告 block bootstrap、Newey-West/HAC 或非重叠抽样结果；缺失某项时记录统计局限和补算计划。

### 4.4 双层评估与正交性

板块级因子映射回股票后，同一板块成员共享因子值，普通股票级 IC 会让成员数更多的板块获得更高权重。因此每个板块级候选必须同时报告：

1. 股票映射层：与模型实际输入一致的股票级 IC/RankIC 和相关性；
2. 板块原生层：按 `(datetime, l2_code_id)` 去重后的等权板块 IC/RankIC 和相关性；
3. 显著性：按时间 block 或板块 cluster 稳健的置信区间。

股票映射层和板块原生层的相关性都应报告；`|corr|=0.8` 仅作为“高相关”描述参考。一个层面低相关、另一层面高相关时，明确记录权重结构差异并继续用 partial/residual IC、模型消融和持仓重合互证，不形成准入或淘汰结论。

### 4.5 预注册与多重检验

- 每个候选在最终 out-sample 前冻结：公式、窗口、预期方向、缺失值规则和最小成员数。
- 不得在最终 out-sample 看到负 IC 后直接取负；若 train/validation 证明反向语义成立，应创建有清晰金融解释的版本，再进入 untouched test。
- 同族窗口变体作为一个 family 报告并保留 family-level 结果谱系；弱结果仍保留并可继续派生新假设，不使用“淘汰”处置。

### 4.6 G0-01：试验台账、依赖检验与选择偏差

机构和论文证据只提供研究先验，不直接证明 A 股 alpha。Harvey、Liu、Zhu 指出因子海量检验下传统 `t > 2` 不足；2026 年更新进一步强调测试依赖、原假设分布和样本选择，并建议 local FDR。Bailey 与 López de Prado 的 Deflated Sharpe Ratio（DSR）则校正多次尝试、非正态和选择偏差。对应 AIstock 规则为：

- 每次公式、窗口、符号、阈值、种子、切分或数据快照组合都分配唯一 `trial_id`；validation 后的任何修改都算新试验。
- 台账最小字段为：`trial_id`、`parent_trial_id`、`created_at_utc`、`candidate_id`、`family_id`、`formula_hash`、`code_hash`、`data_snapshot_sha256`、`label_contract`、train/validation/test 边界、`purge_days`、`embargo_days`、`expected_direction`、阈值、随机种子、状态、可用证据、数据缺口与后续方案。实际运行台账随实验 artifact 保存为 JSONL append log，或 immutable partitioned Parquet dataset + manifest；弱结果、错误和部分结果都保留，不覆盖历史行。
- 相关候选按 family 计数：`{A1,A2,A4}`、`{A3,B2,R1,R2}`、`{A5,A6}`、`{B1,N1}`。N1 无论结果如何都保留在试验台账并可继续产生互证实验。
- 至少报告候选总数、family 数、有效独立试验数估计和 HAC t 值；生成组合收益后再报告 DSR/PBO 或等价选择偏差诊断。
- `t >= 3` 与 local FDR 是统计治理参考，不能机械替换本规格的 h20 IC/RankIC 门槛。

### 4.7 G0-02/G0-03：purge、embargo 与重叠 h20 推断

- 固定 chronological train/validation/test；最终 test 只允许开启一次，禁止随机切分。
- 按标签区间精确 purge。对 `close[t+21] / close[t+1] - 1`，训练/验证边界至少移除会与后段标签重叠的 20 个信号日；若采用双向 CV/CPCV，再使用预注册 embargo。
- rolling 标准化、阈值和方向 `d` 只能由 train/validation 冻结。
- 普通 IC/RankIC 之外，必须报告 Newey-West long-run variance 调整的 ICIR，默认 `lag = h - 1 = 19`；同时用更长 lag、stationary/block bootstrap 或非重叠抽样做敏感性检查。

### 4.8 G0-04/G0-06：条件增量、信息扩散与 STATE 通道

行业动量可以解释相当部分个股动量；行业内 lead-lag 也可能来自共同信息的缓慢扩散。因此 rank、相对行业收益或 leadership 不能天然视为新 alpha：

- A3 报告相对 R1、`Industry_Momentum`、`SW2_MOM5` 和原始板块 20 日收益的控制结果；B2 报告相对 R2、`m_stock_vs_industry_mom_20d` 和 `m_mom_residual_20d` 的控制结果。
- A2/A4 报告相对 A1 和原始板块动量的相关系数、partial IC、残差 IC 或条件回归增量；暂缺控制项时保留原始结果并形成补算计划。
- A5/A6 是 `STATE`，不强迫具有固定单调方向。允许各自增加一个预注册的 `state × momentum_or_breadth` 模型交互腿，但交互不生成新的 catalog 原子因子，也不能在 test 后挑选。
- A5 必须区分 residual cohesion、原始成员离散度和普通低波，并检查高协同性是否表现为拥挤后的反转。

### 4.9 G0-05/G0-07/G0-08/G0-09：breadth、成本容量、拥挤与组合增量

- 外部 breadth 研究只能支持“成员参与值得检验”的先验，不能证明 A1/A2 在 A 股有效。A1 保持 level baseline，A2 保持唯一 thrust 主公式；advance/decline、自由流通加权等仅作为预注册 sensitivity。
- 所有候选都报告换手、实际费用、停牌/涨跌停可成交性、成交参与率和多资金规模 capacity curve。A2/A3/B1/N1 是高换手重点，A1/A5/A6 也不豁免。
- 去重不止检查平均因子值相关性，还检查 long-leg/目标持仓重合、同向换手和冲击重合、压力期相关性、尾部亏损与成本跳升。平均相关性低但尾部持仓高度重合时，标记为“不同公式、相同拥挤风险”。
- 最终采用标准是 GATs/LGBM 的 out-sample 组合增量，包括 `ΔIC`、净 Sharpe、回撤、换手、容量和多种子稳定性；单因子 IC 不能代替组合验证。

上述分析维度参考多重检验、PBO/DSR、行业/因子动量、信息扩散、离散度、真实交易成本和机构拥挤模型的一手研究。完整引用见第 18 节；外部结论在 A 股 candidate 数据上通过完整样本、部分样本、代理数据和后续补数持续验证，任一轮结果都不终止研究方向。

### 4.10 post-R6 模型与组合研究层级

后续研究按信息增量和工程成本分层，不把“换模型”当成默认答案：

1. **组合层**：先复用已归档预测做 GATs + LGBM 的 prediction fusion 与 portfolio fusion，验证关系模型是否以正交性而非单腿 RankIC 创造价值。
2. **决策层**：再建立“板块评分 → 板块内选股”的两层基线，直接检验板块轮动与板块内 leadership 是否优于一次性全市场排序。
3. **关系层**：在真实 PIT 申万 L2 归属上研究 HIST-industry、动态加权图和多关系注意力。与 R4 完全相同的二值同业邻接归入同一 trial family；新数据、新期限、新权重或新结构可作为新 trial 继续研究并明确差异。
4. **概念层**：概念成员 PIT 数据采集与 HIST-concept、HATS/多关系图、概念超图的代理/部分样本研究并行；同一股票同日属于多个概念是基础语义。PIT 不完整时明确覆盖范围、潜在未来信息偏差和补算计划，不阻止方向探索。
5. **状态层**：MASTER、IGMTF、TRA 可以独立研究关系与市场状态机制，不以其他模型先取得增量为前提。TRA 若用于 Type B，在长期趋势内部路由状态；Type A 超跌反弹与 Type B 长期趋势保持独立标签头和独立归因，组合层可以另行研究二者关系。

所有关系输入统一服从以下契约：

- 行业或概念成员关系优先使用 decision-as-of 可知的 PIT 关系。当前成分静态快照、短期样本或代理 `stock2concept` 可以作为明确标注的探索/敏感性实验，同时量化未来函数风险，并在真实 PIT 到位后并列互证；不得将代理结果冒充完整 PIT 结论。
- 动态矩阵/稀疏边记录 `as_of_date`、relation type、source version/hash、有效起止区间和 instrument mapping hash；`stock_index`、Qlib instruments 与关系矩阵行序不一致时标记受影响实验和样本，修正映射后重算，同时允许其他已对齐子集继续研究。
- 动态权重只能使用当日 cutoff 前可知的滚动收益、残差相关、资金流、leadership 或板块状态；训练/验证/测试边界分别构图，不得用全样本相似度。
- 多关系图至少分离 `industry_membership`、`sector_state/leadership` 和未来 `concept_membership`，不得把不同经济含义的边先求和再声称可解释。
- 关系模型尽量与相同因子、标签、切分、种子和训练预算的 LGBM/GATs 基线比较；不可完全对齐时报告差异与敏感性。首个 loop 的 wiring、资源和 alpha 指标均保留分析，不产生方向级晋级/淘汰结论。

### 4.11 多 Alpha QE 演进底座成熟度与增量架构

当前多 Alpha 后端已经能够复用既有单腿 prediction，完成标准化、权重组合、walk-forward、baseline、LOO、TopK/资金量/持仓策略场景回放、WSL/远端 pred-backtest、结果持久化、日志、Archive 和 StrategyPackage 输出；`combine_ui_adapter.py` 已把 run 映射为单 Alpha QE 的 task/loop 读模型，详情页已经复用 `LoopDetailPanel` 与 `EvolutionTrajectory`。因此现有平台不是一次性脚本，也不需要另起一套“多 Alpha v2”。

但“组合计算可用”不等于“演进平台完整”。现阶段仍有四个结构性缺口：

1. `submit_run()` 以 FastAPI 进程内 daemon thread 持有父任务，`ThreadPoolExecutor` 持有子任务；远端 `qe_task_id/qe_loop_id`、节点占用、子任务 attempt 和事件未形成数据库权威状态，后端重启只能查看旧行，不能可靠重新接管正在执行的 run。
2. 任务控制只有整组 retry 和终态 delete；没有语义明确的暂停、恢复、取消、单子任务重试及 `results_only` 恢复，已完成子任务可能被整组重算。
3. 多 Alpha UI 主要是列表和结果查看，没有复用单 Alpha自动演进页面的信息架构提供正式创建器；用户仍需依赖 API/MCP 或外部提示词构造完整 payload。
4. 当前日志和进度主要聚合在 run reason/workspace，缺少 baseline、各 scheme、各 LOO 子任务的节点、远端任务、attempt、耗时、状态、错误和恢复动作统一视图。

后续基础研发采用现有架构的增量扩展，不改变组合与回测业务公式：

- 保留 `strategy_pkg.multi_alpha_combine_backtest_run`、scheme/LOO 结果表、`MultiAlphaCombineBacktestService`、`MultiAlphaCombiner`、`MultiAlphaPanelBuilder`、Prediction Store、QE Archive 和现有 API；新增的 task/child/attempt/event 状态只承担可靠编排与审计。
- 将 `QEWorkspaceClient` 现有的 `create_and_run_loop/get_loop_status/kill_loop/get_workspace_file` 作为 WSL 和远端节点的统一子任务执行契约；本地与远端不再由不同生命周期所有者分别解释。
- 复用 HMM/QE 已有 PostgreSQL lease、fencing、row-version CAS 和 startup scanner 模式，但 lease 到期只释放旧 worker 并进入重新核对，不因暂时无法通信自动把 Alpha 结果判为失败。
- UI 沿用 `/quantevolver/evolution` 的任务列表、状态、拓扑、结果、轨迹、日志和操作样式；通过数据源 adapter 接入多 Alpha，不复制一套新的视觉系统，不伪造训练 IC/loss/feature importance 等不适用字段。
- 任何异常必须写入 run/child attempt/event 并由 API/UI 展示稳定 `reason_code`；不得 `except: pass`、返回空成功、自动切换重试模式或用默认节点/路径掩盖错误。
- 不新增研究门禁、方向淘汰、人工审批或发布审批。唯一边界仍为 QE-only；节点暂时满载时保持排队而不是失败，数据或制品缺失时保留已完成结果并显示补充/恢复动作。

完整表结构、状态机、API、UI 复用路径、迁移与验证方案见 `docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`。

## 5. 代码与运行时契约

当前因子研发链存在两种代码形态，本规格明确要求双产物而不是混用：

### 5.1 离线研发 `code_text`

- 用于 WSL 执行、`result.h5` 生成、h20 quick screen 和统一指标。
- 只能读取明确注入到任务 workspace 的 candidate h5/parquet 数据，不得读取 active/production 的隐式默认路径。
- 输出必须是单列 DataFrame，索引为 `MultiIndex(datetime, instrument)`，列名等于因子名，末尾 `dropna()`。
- 代码只依赖 pandas/numpy/scipy；不得 import qlib、硬编码股票或日期、写入项目目录。

### 5.2 QE loader-only `realtime_code_text`

- 函数签名固定：`def calculate_{factor_name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:`。
- QE workspace 中的行情只通过文件后端 `_REALTIME_LOADER`，静态字段只通过文件后端 `_STATIC_FACTORS_LOADER` 显式取列；两个 loader 均只能读取当前冻结数据身份。非 QE 实时 loader 不属于本蓝图，也不能被 QE 复用为数据库通道。
- 禁止文件 I/O、try-except 兜底、空值伪造、空 DataFrame 静默返回和 `$` 前缀列名。
- 输出索引名称继承 loader，禁止手写索引名称掩盖输入错误。

### 5.3 离线/实时一致性

同一因子的两种代码形态必须在冻结小窗口上完成 parity：

- 公共索引覆盖率一致；
- 非空值位置一致；
- 数值在声明容差内一致；
- `l2_code_id` 的 unknown、PIT 归属与板块映射一致。

因子 MCP 当前用于查库、指标、覆盖率、使用情况和相关性分析；可执行源码保存仍使用 manual factor API/脚本。只登记 catalog 元数据的 MCP register 与可执行资产状态分别报告，但任一状态缺口不阻止 QE 内已有资产的分析。

## 6. 候选因子池与研发批次

候选池允许扩展，5–10 个/批仅是便于组织和比较的建议。名称、公式、相关性和同族谱系用于说明新颖性与互证关系；前一版本信号弱或数据不全不阻止同族变体、下一批或交叉数据方向并行研究。

新增因子统一使用 `m_` 前缀并满足 `^[a-z][a-z0-9_]{2,80}$`；名称中的窗口后缀必须与唯一主公式一致，禁止同一名称承载可切换公式。

优先级 `A/B/C` 分别表示首批主要假设、次要/状态假设、基线或高重复风险假设；它不是 AIstock 的 P0/P1 风险等级，也不代表验收已通过。

### 6.1 Batch A：首批核心候选

| 编号 | 因子名 | 状态 | 类型 | 主数据源 | 最小历史 | 优先级 |
|---|---|---|---|---|---:|---|
| A1 | `m_sector_breadth_ma20_level` | `BASELINE` | 板块价格广度 level | close + `l2_code_id` | 20d | C |
| A2 | `m_sector_breadth_ma20_thrust_5d` | `NEW` | 板块价格广度扩散速度 | close + `l2_code_id` | 25d | A |
| A3 | `m_sector_rs_rank_velocity_20d_5d` | `NEW` | 板块排名进入速度 | `sw2_close` + `l2_code_id` | 25d | A |
| A4 | `m_sector_participation_gap_20d` | `NEW` | 典型成员与指数参与差 | close + `sw2_close` + `l2_code_id`；控制项 `db_circ_mv` | 20d | A |
| A5 | `m_sector_residual_cohesion_10d_60d` | `NEW` | 板块成员残差协同性 | close + `sw2_close` + `l2_code_id` | 60d | B |
| A6 | `m_sector_vol_compression_5d_20d` | `NEW` | 板块波动压缩状态 | `sw2_close` + `l2_code_id` | 20d | B |

#### A1 `m_sector_breadth_ma20_level`——价格广度 level 基线

- 个股时序：`ma20 = MA20(close)`；只在 `ma20.notna()` 时计算 `above_ma20[i,t] = 1(close[i,t] > ma20[i,t])`。不得先比较再直接 `.astype(float)`，否则无效 MA 会被误记为 0。
- 板块聚合：对当日有效成员取均值。
- 最小样本：有效成员数 `< 5` 或有效覆盖率 `< 0.8` 时该板块当日为 NaN。
- 输出：将板块 breadth 映射回当日成员。
- 方向：不预先锁死。高 breadth 可能表示趋势健康，也可能表示拥挤；作为 level 基线与 A2 比较。

#### A2 `m_sector_breadth_ma20_thrust_5d`——价格广度扩散速度

- 先计算 A1 的 `breadth20[s,t]`。
- 主公式：`thrust[s,t] = breadth20[s,t] - breadth20[s,t-5]`。
- 每日对有效板块做 percentile rank 后映射回成员。
- 预期方向：正；成员参与度正在扩散，比绝对 level 更贴近轮动形成，但仍可能在行情末端形成追涨信号。
- 变体分析：`m_sector_breadth_ma20_abnormal_60d = breadth20 - MA60(breadth20)` 作为独立因子/独立 trial 研究，不在一个因子名下切换公式；主公式强弱不限制该变体启动。
- 扩展分析：advance/decline、自由流通市值加权 breadth 等可作为独立 sensitivity 或 catalog 候选，分别保留公式、数据和结果谱系，不用事后只报告最优版本。

#### A3 `m_sector_rs_rank_velocity_20d_5d`——板块排名速度

附加分析：除原始相关性外，相对 R1、`Industry_Momentum`、`SW2_MOM5` 和原始板块 20 日收益报告 partial/residual IC。控制后没有稳定 h20 增量时标记为“当前样本可解释/高重合”，继续考察期限、regime、非线性和模型交互，不做方向处置。

- 在板块面板计算 `ret20[s,t] = sw2_close[s,t] / sw2_close[s,t-20] - 1`。
- 每日等权跨板块排名：`rank20[s,t] = CsRank(ret20[:,t])`。
- 主公式：`velocity[s,t] = rank20[s,t] - rank20[s,t-5]`。该值已经由两个截面分位之差归一化，主版本不再二次 rank。
- 预期方向：正；正在进入领涨区比“已经处于高位”更接近轮动速度。
- 相关性重点：与 `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` 同时检查，rank 变换不能被当作天然正交证明。

#### A4 `m_sector_participation_gap_20d`——成员参与差

附加分析：控制 A1、原始板块动量、板块权重集中度、SIZE 与有效成员数；若 gap 主要重述少数权重股效应，将该暴露作为经济解释和后续去偏/交互实验输入，而不是停止研究。

`db_circ_mv` 只用于 SIZE/集中度诊断和条件回归，不进入 A4 主公式；若后续改用权威指数成分权重，必须作为新 trial 冻结数据源与时点，不得用事后当前权重回填历史。

- 个股 20 日收益：`stock_ret20[i,t]`。
- 当日按 PIT 成员聚合：`member_median20[s,t] = median_i(stock_ret20[i,t])`。
- 板块指数 20 日收益在板块面板上计算：`sector_ret20[s,t]`。
- 主公式：`gap[s,t] = member_median20[s,t] - sector_ret20[s,t]`，跨板块 rank 后映射回成员。
- 预期方向：正；中位成员也参与上涨，说明轮动不是少数权重股拉动。
- 风险：可能混入小盘风格，必须额外报告与 SIZE/市值因子的相关性。

#### A5 `m_sector_residual_cohesion_10d_60d`——成员残差协同性

附加分析：同时与原始成员离散度、市场/板块波动和既有 VOL/low-vol 因子做条件比较；`state × momentum/breadth` 的多个明确命名交互可作为独立 trial 研究，并报告尝试总数与谱系。

- 个股日收益 `stock_ret1[i,t]` 必须在单一 instrument 的连续价格序列上由 close 执行 `pct_change(fill_method=None)`；板块日收益 `sector_ret1[s,t]` 必须在 4.1 的板块面板上由 `sw2_close` 执行同一计算。两者都不使用预填充收益列。
- 日残差：`resid[i,t] = stock_ret1[i,t] - sector_ret1[s,t]`。
- 当日板块离散度：`mad[s,t] = median_i(abs(resid[i,t] - median_i(resid[i,t])))`。
- 主公式：`cohesion[s,t] = -log(MA10(mad[s,t]) / MA60(mad[s,t]))`；分母为 0 或样本不足时置 NaN，不使用任意 epsilon 掩盖异常。
- 每日跨板块 rank 后映射回成员。
- 经济含义：高值表示近期成员残差相对长期收敛。它是状态特征，本身不预设涨跌方向；方向由 train/validation 冻结。
- 风险：可能退化为板块低波风格，必须检查与波动率因子及 A6 的相关性。

#### A6 `m_sector_vol_compression_5d_20d`——板块波动压缩

附加分析：与 A5、既有 VOL/low-vol 因子比较，并把不同交互、方向和公式作为独立 trial 保存；测试期出现差异时记录为新假设并在后续样本互证，不覆盖原结果。

- 在板块面板以 `pct_change(fill_method=None)` 计算行业日收益 `sector_ret1`。
- 冻结定义：`RVw[s,t] = rolling_std(sector_ret1[s], window=w, min_periods=w, ddof=1)`，其中 `w ∈ {5, 20}`；不得在实现时替换为 RMS、平方和或年化波动。
- 主公式：`compression[s,t] = -log(RV5[s,t] / RV20[s,t])`；任一窗口样本不足、`RV5 <= 0` 或 `RV20 <= 0` 时置 NaN。
- 每日跨板块 rank 后映射回成员。
- 方向：作为原子状态信号，不在因子内部预先乘动量；h20 方向由 train/validation 冻结。
- 研究假设：检验短长波动比在板块层是否提供区别于简单行业动量的 STATE 信息；该迁移尚未获得 A 股 h20 证据，必须允许无效或条件性结论。

### 6.2 Batch B：扩展候选

Batch B 与 Batch A 可按数据和资源情况并行启动；Batch A 的结果用于补充解释，不构成启动条件。

| 编号 | 因子名 | 状态 | 类型 | 最小历史 | 优先级 |
|---|---|---|---|---:|---|
| B1 | `m_sector_turnover_breadth_accel_5d` | `EXPLORATORY` | 自由流通换手异常广度 | 65d | B |
| B2 | `m_stock_sector_leadership_persistence_20d_10d` | `EXPLORATORY` | 板块内领导持续性 | 30d | C |

#### B1 `m_sector_turnover_breadth_accel_5d`——自由流通换手异常广度

附加分析：作为高换手重点样本，在 A 股 T+1、停牌、涨跌停和实际费用口径下报告多资金规模/参与率的净结果与 capacity curve；任一交易数据缺口单独记录并用可用口径分析。

- 数据：`db_turnover_rate_f` + `l2_code_id`。
- 个股异常：`x = log1p(db_turnover_rate_f)`，`z60 = (x - MA60(x)) / STD60(x)`；只在 60 日均值/标准差有效且标准差大于 0 时计算 `hot[i,t] = 1(z60[i,t] > 1)`，否则保持 NaN。
- 板块参与率：`turn_breadth[s,t] = mean_i(hot[i,t])`。
- 主公式：`turn_breadth[s,t] - turn_breadth[s,t-5]`，跨板块 rank 后映射。
- 预期方向：正；关注度从少数个股向更多成员扩散。
- 风险：极端换手可能是出货；必须检查非线性和与换手率 Top 因子的相关性。

#### B2 `m_stock_sector_leadership_persistence_20d_10d`——板块内领导持续性

附加分析：控制 R2、`m_stock_vs_industry_mom_20d` 和 `m_mom_residual_20d` 后报告 partial/residual IC；行业切换重置与不重置版本可作为语义 sensitivity 分开研究并标明 PIT 风险。

- 先按 membership-safe 板块面板得到 20 日板块收益，计算 `lead20 = stock_ret20 - sector_ret20`。
- 每日做板块内 percentile rank：`q20[i,t] = rank_within_sector(lead20[i,t])`。
- 主公式：`MA10(1(q20 >= 0.8))`，表示最近 10 个有效交易日持续位于板块前 20% 的比例。
- 10 日 rolling 必须按 instrument 的连续行业 spell 计算；`l2_code_id` 变化时重置，禁止把上一行业的领导状态带入新行业。
- 目的：识别持续龙头，而不是复制单一 20 日端点残差。
- 方向：不得沿用原 F4 的正向假设；在 train/validation 冻结后再进入 h20 test。
- 相关性画像：与 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_sector_momentum_spread` 分层报告；`|corr| >= 0.8` 标记为高重合并继续开展 residual/期限/regime/组合互证。

### 6.3 复用基线与 negative control

#### R1 原 F1 行业强度基线

`m_sector_rs_rank_20d` 与现有 `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` 高度同族，默认作为公式/窗口研究变体而不重复创建 catalog 资产；如需独立 20 日口径，可以随时建立新 trial，显式记录与 A3 rank velocity 及既有因子的谱系、相关性和实现差异。

#### R2 原 F4 个股相对行业基线

`m_stock_sector_leadership_20d`、现有 `m_stock_vs_industry_mom_20d`/`m_mom_residual_20d`、反向版本、近义因子和 B2 都可研究；先读取可执行源码并记录 `sw2_*` 收益/rolling 的板块面板口径。口径不合规时旧指标标为受影响证据，同时修复、重算并用新旧口径互证，不停止任何结构版本。

Gate-0 已修复 tracked regeneration source `scripts/p1_new_factors.py` 中的 F4/R2 offline 公式：先构造唯一 `(datetime,l2_code_id)` 面板、沿板块自身时序计算 20 日收益，再按当日 membership 映射回股票；unknown 不回退，板块日值冲突 fail-fast。因本批不写生产 DB，catalog 中既有 offline/realtime 资产与历史指标仍未替换；后续同步必须同时生成双代码形态、做 parity、重新计算 h20，并把旧 1d 指标标记为旧口径证据。

#### N1 `m_sector_flow_rotation_10d` negative control

- 板块面板上计算 `flow = sw2_mf_net_amt / sw2_amount`；`sw2_amount == 0` 时置 NaN。
- 加速：`MA10(flow) - MA10(flow).shift(10)`；每日跨板块 rank。
- 仅执行离线代码验证和 h20/1d quick screen。
- h20 强弱均保留到 catalog/全量/窗口变体研究计划中；负对照的弱或负结果本身是有效科研证据。
- 与现有板块资金流因子的双层相关性、partial IC、近期窗口和成本画像并行计算，用于解释而非决定是否继续。

## 7. 数据前置与 train/serve parity

### 7.1 Gate-0 历史快照与当前 QE 数据状态

下表前三行保留 2026-07-11 Gate-0 的历史 receipt；最后一行是 2026-07-15 的当前 QE 研究状态。不得再把旧 2026-04-28 candidate 误读为 R6/R7 实际使用的数据。

| 数据位置 | `sector_data.h5` | `static_factors.parquet` | 结论 |
|---|---|---|---|
| active `factor_implementation_source_data` | 22 个 `sw2_*` 字段，无 `l2_code_id` | 122 列，无 `l2_code_id` | 可用于不依赖离散行业键的指标、代理/损失实验；依赖 `l2_code_id` 的分析标记缺口并安排补导。 |
| candidate `factor_implementation_source_data_20260428_candidate` | 23 列，含 `l2_code_id` | 120 个数据列，无 `l2_code_id` | `sector_data.h5` 可支持板块层分析；static 缺键的指标族单独标记并与新 bundle 互证。 |
| Gate-0 隔离产物 `gate0_sector_factor_candidate_20260711` | 复用上述 23 列 candidate | 121 个数据列；旧 candidate 的 120 个数据列全部保留并新增 `l2_code_id=int16` | 已完成物理/schema/指纹验证；仍为 gitignored candidate，未 promotion。 |
| 当前 QE `factor_data`（2018-08-01 至 2026-06-30） | 23 列，含 `l2_code_id` | 123 个数据列，含 `l2_code_id=int16` | WSL 副本已被 R6/R7/R8B 实际读取，同身份远端副本已被 R8A 12/12 Loop 完整使用；满足当前 QE 研究，不外推为非 QE 运行时 readiness。 |

2026-07-11 Gate-0 实测审计：candidate `sector_data.h5` 共 7,334,829 行、1,876 个交易日、4,691 只股票，日期为 2018-08-01 至 2026-04-28，131 个已知板块，源表 `l2_code_id` 覆盖率 100%。旧生成器会把所有列统一转为 `float32` 且遗漏 `margin_detail.h5`，因此旧 candidate bundle 不具备离散类别键语义。修复后隔离生成产物为 7,304,119 行、4,691 只股票、1,876 日：7,303,993 行为已知板块，126 行显式为 unknown `-1`，known coverage 为 99.99827494595858%，取值范围 `[-1,133]`，共 131 个已知板块；旧 120 个数据列全部保留，共同字段 dtype 无变化，只新增 `l2_code_id=int16`。`static_factors.parquet` SHA-256 为 `FE91FA9C519F4FD501D5E979F03B604C66F3904387B48C0E982D8366747D60A6`；schema JSON/CSV SHA-256 分别为 `04252DD8E8941CDD8018885B1BBBE95F4C606FBAEE49C61BAB6E1986DFFF5DFE`、`D193BDBF4B003291B5FD708A1D420FF14E6526C3473F5E786F869889B81B6FD6`。产物仍在任务 worktree 的 gitignored 目录，未修改旧 candidate、active 或数据库。

输出以唯一的 `daily_basic` 索引为左连接基表：sector 有 7,334,829 个唯一键，daily-basic 有 7,304,119 个唯一键，交集 7,303,993；因此丢弃 30,836 个 sector-only keys，并将 126 个 daily-basic-only keys 的 `l2_code_id` 写为 `-1`，净行数差为 `30,836 - 126 = 30,710`。这不是随机丢行，必须随 snapshot receipt 保留。

上述历史批次 candidate 是截至 2026-04-28 的冻结快照，只用于解释当时的研究背景。当前官方指标和 R6/R7 使用 2026-06-30 快照；任何 hN 评价按交易日历反推 `last_evaluable_signal_date`，未成熟尾部保留为 inference/backtest 特征并在 IC/期限分析中标记未成熟。长周期 h30–h180 由 `LongHorizonLabelMaturityPurge` 对每个 learning segment 屏蔽最后 `horizon + 1` 个交易日标签，但 inference frame 保留；F-014 evaluator 使用右删失描述未成熟样本。

### 7.2 数据状态、缺口与获取行动

1. `[COMPLETED]` `generate_static_factors_bundle.py` 已保持连续因子 `float32`，并对 `l2_code_id` 校验整数/范围、缺失 `-1` 和有符号 `int16/int32`。
2. `[COMPLETED]` Gate-0 隔离 candidate 生成、schema/指纹/覆盖/unknown receipt 已完成；历史产物保持不可变。
3. `[COMPLETED_FOR_QE]` 2018-08-01 至 2026-06-30 的 WSL QE bundle 已部署并由 R6/R7 实验验证；sector、price、basic 和 static 使用同一快照。
4. `[COMPLETED_FOR_OFFICIAL_METRICS]` 三个 R6 板块因子的 2026-06-30 官方独立指标和基本相关性已持久化。
5. `[OUTSIDE_QE_SCOPE]` 自动 transformation/review、`realtime_code_text`、离线/实时 parity 和统一 runtime `industry_code_map` 不属于本蓝图；QE loader/离线资产状态不写入或影响荐股、模拟盘及其他模块。
6. `[QE_ONLY_HARD_BOUNDARY]` 本蓝图的 candidate、factor、实验和评价不进入 production/paper/live，也不触发这些模块的任何代码、数据或状态变化。

GAT embedding 在研究期使用实验冻结 mapping；进入任何非 QE 运行时前，必须统一 embedding、导出与实时侧 `industry_code_map`，并验证 unknown、新增行业和重启后的映射稳定性。

## 8. 研发流程

### Stage 0：数据画像、查重与研究台账

1. 记录现有数据字段、覆盖、PIT 口径和缺口；缺口同步形成获取/补算方案，并以可用子集或明确标注的代理数据继续研究。
2. 为公式运行建立 append-only `trial_id` 台账；公式、窗口、方向、阈值、种子、切分及失败版本均计入，按 `{A1,A2,A4}`、`{A3,B2,R1,R2}`、`{A5,A6}`、`{B1,N1}` 记录相关候选族，N1 的全部结果永久保留。
3. 用因子 MCP 对名称、描述、公式和同族因子定向搜索；搜索摘要与明确窗口指标一并保存，搜索缺口不阻止开发。
4. 对复用基线读取代码与 out-sample 指标；同名、近义、反向和重复版本都可研究，但需记录谱系。A2/A3/A4/B2 的 partial/residual IC 控制集随实验保存，可后补。
5. 为每个候选维护实验卡：公式、字段、窗口、方向假设、最小成员数、缺失值规则、主要相关性对照、成本/容量重点和 STATE 交互（如适用）；冻结文件缺少必需字段时，该候选明确记为当前不可计算并 fail closed，不触发数据库补齐、历史采集或平台建设。

研究治理采用“保留完整审计、允许多路径并行”的方式。负对照、高相关、复用和基线候选都写入 append-only ledger、计入 family trial count，并保留弱结果、错误和部分结果。确定性重复、单调变换或方向副本标注谱系后可作为复现、符号、期限或模型敏感性样本；novelty screen、purge/HAC、多重检验和组合分析均是解释维度，不决定候选能否继续。

### Stage 1：离线执行与双周期快筛

1. 在任务隔离 workspace 生成离线 `code_text` 和 `result.h5`。
2. 检查索引、列名、日期、股票数、板块覆盖、unknown 处理和非空率。
3. 主快筛使用与目标实验一致的 h20 裸标签；1d 只作短周期诊断。
4. h20 快筛可使用 `quick_ic_screen.py --horizon 20 --split-manifest split.json <workspace>`。manifest 记录 `trial_id/split_id/split_role/signal_start/signal_end/label_horizon_days/purge_days/embargo_days/expected_direction/data_snapshot_sha256`；脚本输出 manifest SHA-256、`label_source_end` 和 `last_evaluable_signal_date`。省略 manifest、方向或部分字段时仍保留结果，并明确标记其数据身份和统计限制。
5. chronological train/validation/test、purge、embargo、随机/滚动切分都可以作为独立 trial；每种切分与标准化/阈值/方向配置分别归档，避免覆写。
6. h20 重叠日收益尽量同时报告普通 ICIR、Bartlett lag=19 Newey-West HAC ICIR、stationary/block bootstrap 或非重叠抽样；退化或样本不足显式为空，并列出补算计划。
7. validation 后改变公式、窗口、方向、阈值或样本切分时新建 `trial_id`，旧结果保留用于比较；这是一致性记录，不是研究许可条件。

以下 h20 数值区间保留为历史批次的描述性分层，当前只用于比较和组织后续分析，不具有准入、淘汰或停止语义：

train/validation 记录预期方向 `d ∈ {-1, +1}`。下表使用方向调整后的 `d * IC_h20` 与 `d * RankIC_h20`；反向结果作为独立经济假设和 trial 实际验证，不做自动翻符号。

| 条件 | 描述标签 | 后续分析 |
|---|---|---|
| `d * IC_h20 >= 0.015` 且 `d * RankIC_h20 >= 0.015` | `OBSERVED_STRONG` | 继续统一指标、相关性、成本、regime 和模型互证。 |
| `d * IC_h20 >= 0` 且 `d * RankIC_h20 >= 0`，并且（`d * IC_h20 >= 0.005` 或 `d * RankIC_h20 >= 0.010`） | `OBSERVED_MIXED` | 分析方向、期限、样本、非线性和数据损失，可并行建立多个独立修订 trial。 |
| 其余情况（含结果与预期方向相反） | `OBSERVED_WEAK_OR_OPPOSITE` | 保留并验证反向经济语义、不同期限、regime 与代理数据，不阻止入库、全量或变体研究。 |

N1 无论结果如何都继续作为负对照；1d 与 h20 方向不一致时保留双方向画像并做持有期与金融语义诊断，反向版本作为独立 trial 验证。

### Stage 2：可执行入库与统一指标

1. 通过 manual factor API/脚本保存离线源码和 `asset_path`。
2. 生成 loader-only `realtime_code_text`，完成离线/实时 parity。
3. 计算统一指标，至少覆盖 `full`、`out_sample`、`recent_6m`、`recent_3m`、`recent_1m`。
4. RD-Agent 指标结果保持既有 1d 行与 legacy `rank_ic_20d` 兼容，并在同一结果增加 exact nullable contract：`h20_return_horizon=T21T1`、`h20_ic_mean`、`h20_ic_std`、`h20_rank_ic_mean`、`h20_rank_ic_std`、`h20_icir`、`h20_rank_icir`、`h20_icir_hac`、`h20_rank_icir_hac`、`h20_ic_positive_ratio`、`h20_n_obs`、`h20_hac_lag=19`；positive ratio 与 n_obs 按 raw Pearson IC 日序列统计，`return_horizon=1d` 与 h20 字段并列解释。
   - legacy 行键 `return_horizon=1d` 表示持久化主记录兼容；RD 内部计算 key `20d` 表示持有期；区间 label `T21T1` 表示 T+1 入场到 T+21 出场。三者语义不同，不得互相覆写或据字符串推断唯一键。
   - RD 官方 naive std/ICIR 使用 NumPy population std（`ddof=0`）。quick screen 为保持旧 1d 输出继续保留 legacy `icir/rank_icir`（`ddof=1`），同时显式输出与 RD 对齐的 `ic_std_ddof0`、`icir_ddof0`、`rank_ic_std_ddof0`、`rank_icir_ddof0`；正式重叠 h20 推断优先读取 HAC 字段。不得把两种 naive ICIR 混为同一数值口径。
5. 执行 LLM 分类和增量相关性；记录 catalog、metrics、classification、correlation 的完整性 receipt。
6. AIstock 历史批次只提交 additive schema/upsert/router/MCP 字段支持，当时未应用生产 DDL、未写生产指标行；该事实不影响 QE 研究。
7. writer authority 保持不变：official evaluation writer 是唯一允许落 `aistock_factor_metrics` 的路径；`rdagent_factor_metrics_sync` 仅保留并测试兼容 SQL/旧 payload normalization，task/loop 非官方落表继续明确禁用，不得因 h20 字段就绕过。
   - 旧 payload 完全不含 h20 keys 时，presence flag 为 false，冲突更新必须保留已有 h20 值；新 contract 即使显式携带 `None`，presence flag 仍为 true，可正确清除本次已退化/不足的旧值。不得用简单 `COALESCE` 混淆“字段缺席”和“显式空值”。

### Stage 3：双层相关性与互证

- 股票映射层和板块原生层都报告与基线/Top 因子的相关性；`|corr|=0.8` 仅作高相关描述。
- 同族高相关候选全部保留结果，用 h20 稳定性、覆盖、模型增量、期限和 regime 差异解释各自价值。
- 除原始相关性外执行 partial/residual IC；控制后无稳定增量时标注当前信息可解释程度，并继续非线性、期限和组合互证。
- 沿用 Stage 1 冻结方向 `d`，定义 `IC_d = d * IC_h20`、`RankIC_d = d * RankIC_h20`、`ICIR_d = d * ICIR_h20`；不得在 Stage 2/3 重新选择符号或覆写 `d`。
- out-sample h20 的 `IC_d=0.02`、`RankIC_d=0.02`、block/HAC `ICIR_d=0.3` 仅作为历史参考线；连续数值、置信区间、模型增量和不同期限完整报告，不产生通过/淘汰结论。
- full、out-sample 与近期窗口的 `IC_d`、`RankIC_d` 正负和方向一致性全部报告；漂移进入 regime、反转和数据差异分析，不作为淘汰条件。
- QE archive 共现、独立指标、相关性和受控消融互相补证；任一证据不完整时记录限制并继续其余分析。
- 按候选族报告有效独立试验数、HAC t/ICIR、local FDR 或等价多重检验结果；组合/策略结果另外报告 DSR 与 PBO，禁止以单次最佳 Sharpe 代替。
- 除因子值相关性外，报告目标持仓/long-leg 重合、同向换手与冲击重合、压力期相关性、尾部亏损及成本跳升；平均相关性低但尾部重合高时标记“不同公式、相同拥挤风险”。
- 在多资金规模和成交参与率下报告换手、冲击、净 Sharpe、净回撤与 capacity curve；目标规模下净增量消失描述当前容量损失，并可继续研究其他规模、执行或信号表达。

### Stage 4：结果归因与下一批

对所有强、弱、混合和部分结果记录：数据覆盖、PIT 对齐、同族重复、方向漂移、短长周期冲突、板块规模偏置、波动/市值暴露或噪声可能性。Batch B、窗口变体、交叉数据和新模型可并行启动；每个新实验写清它要解释或互证的现象即可。

## 9. QE 对照实验设计

最终目标不是“单因子 IC 排名”，而是确认显式板块因子和关系 embedding 对 G12 的独立增量。

### 9.1 GATs 2×2 消融

保持裸 h20 标签、数据切分、随机种子、训练预算和评价指标完全一致：

1. G12，关闭 `l2` embedding；
2. G12 + 通过因子，关闭 `l2` embedding；
3. G12，开启 `l2` embedding；
4. G12 + 通过因子，开启 `l2` embedding。

GATs 继续使用 1-parallel，防止并行资源争用污染比较。不得只比较第 1 组和第 4 组，否则无法区分因子贡献、embedding 贡献和交互贡献。

### 9.2 LGBM 对照

LGBM 比较 G12 与 G12 + 各候选因子。`l2_code_id` categorical feature 作为可区分的实验腿记录，和 GATs embedding 并列互证。

A5/A6 作为 STATE 信号时，可各自增加且仅增加一个在 test 前冻结的 `state × momentum/breadth` 交互实验腿，用于判断条件组合增量。交互只属于模型消融，不新增 catalog 原子因子，也不得在测试后从多个交互中择优。

### 9.3 结果分析维度

报告 h20 IC/RankIC、naive/HAC ICIR、bootstrap 区间、CAGR、DSR、PBO、Sharpe、最大回撤、换手、成本和容量曲线，并分训练/验证/测试及主要市场 regime。GATs 2×2 与 LGBM 比较 OOS ΔIC、净 Sharpe、回撤、换手、容量和多种子稳定性；稳定、混合、负向和缺失结果均进入科研分析与 Tier2/IC 记录，不形成研究方向门禁。

### 9.4 GATs + LGBM 组合验证（历史阶段优先级）

该方向不新增训练架构，先回答“GATs 单腿即使不超过 LGBM，是否能改善组合”。历史归档 `qe_20260709_055708_fe49_L2`（GATs）与 `qe_20260708_030408_80cd_L1`（LGBM）可作管线 canary；既有分析给出的日截面 rank 相关约 `0.595`、Top25 重合约 `6.9%` 只作为待复核先验，不是最终验收证据。正式结论必须使用 R6 或后续同数据快照、同因子集、同 seed、同 split 的配对预测。

至少比较：

1. 两个单腿；
2. validation 冻结权重的 rank/prediction fusion；
3. 独立下单后在组合层合并的 portfolio fusion；
4. 在相同总持仓数、总风险预算和成本模型下的 sector-exposure constrained sensitivity。

融合权重和归一化方法在每个 trial 的 validation 口径中记录，最终 test 不在同一 trial 内事后改权；新权重、新风险预算或新融合层作为新 trial 追加。报告预测 rank 相关、Top-K/行业暴露/换手重合、边际贡献、净 CAGR/Sharpe/Calmar/最大回撤、容量和 leave-one-leg-out。若当前组合不改善扣费后风险收益或改善只来自扩大风险预算，只记录该 trial 的收益转化与预算归因；GATs 单腿、portfolio fusion、跨标签融合和关系模型仍可按新假设继续研究。

#### 9.4.1 2026-07-14 历史 prediction-fusion canary receipt

本 canary 只验证历史预测资产的可对齐性、信号正交性和 prediction fusion 管线，不创建 QE 实验、不重新训练模型、不执行组合回测，也不构成 F-013 的正式晋级证据。输入腿固定为 `qe_20260709_055708_fe49_L2`（GATs）与 `qe_20260708_030408_80cd_L1`（LGBM）：两份 `pred.pkl` 各有 `2,260,161` 行、`443` 个共同预测日、`5,120` 个 instrument，预测窗口为 `2024-07-01` 至 `2026-04-28`。

正交性复核得到日截面预测 rank 相关 `0.594975`，Top25 Jaccard `0.036607`；后者等价于每天平均约重合 `1.77/25` 只股票，或以单腿 Top25 为分母约 `7.1%`。因此两腿存在显著选股差异，但低重合本身不证明组合收益提高。

h20 标签只纳入已经成熟的信号日。两腿共有 `2,154,168` 个预测/标签对、`424` 个成熟交易日和 `5,116` 个 instrument，评价窗口截止 `2026-03-31`；两份 label artifact 在全部共同样本上 `max_abs_diff=0`。预测窗口尾部尚未成熟的 19 个信号日没有进入 IC、RankIC 或 Top25 标签统计。

在读取结果前冻结两种等权方案：主方案为 `equal + rank`，敏感性方案为 `equal + zscore`，两腿权重均为 `0.5/0.5`。两腿场景中的 `orthogonality_aware` 会退化为相同的 `0.5/0.5`，因此不重复；`ic_weighted` 与 `risk_parity` 必须留到正式 R6 validation 窗口估权，禁止用本 canary 全段 OOS 选择权重。

| 方案 | h20 RankIC | h20 IC | RankIC 正向率 | Top25 h20 标签均值 |
|---|---:|---:|---:|---:|
| GATs 单腿 | 0.102045 | 0.055060 | 77.59% | 0.053662 |
| LGBM 单腿 | 0.113758 | 0.077744 | 88.21% | 0.072899 |
| `equal + rank` | 0.119018 | 0.065213 | 84.20% | 0.058810 |
| `equal + zscore` | 0.121489 | 0.074162 | 84.20% | 0.069291 |

相对 LGBM，`equal + rank` 与 `equal + zscore` 的平均 RankIC 分别增加约 `0.005260`（`+4.6%`）和 `0.007731`（`+6.8%`），但两者的 IC、RankIC 正向率和 Top25 h20 标签均值均未全面超过 LGBM；其中 `equal + zscore` 的 Top25 标签均值仍比 LGBM 低约 `5.0%`。当前结论因此冻结为：**prediction fusion 显示排序增量与正交性，但尚未证明头部收益转换或成本后组合增量**。后续正式判断仍需 R6 同因子、同 seed、同 split、同数据快照的配对预测，并完成固定总风险预算的 portfolio fusion、成本、回撤、容量和 leave-one-leg-out 回测。

执行时 R6 CPU/GPU 节点均有在途任务，本 canary 当时因资源排队没有提交 combine-backtest。该历史资源状态不是未来研究限制，也不记录为组合回测失败。

#### 9.4.2 R6 正式同口径因子析因结果（2026-07-14）

R6 已在 `dataset_as_of=2026-06-30`、`filtered_pool_20260630`、h20 裸标签、Alpha158 关闭、相同切分、V25_1_SMALL_CAP 和 seeds `123/314/2718` 下完成。CPU 任务 `qe_20260714_104829_a9ca` 与 GPU 任务 `qe_20260714_104830_0230` 均为 15/15 Loop completed。下表使用三种子均值；Sharpe、最大回撤和 CAGR 使用 absolute portfolio 口径，不能与单 Loop 的 excess-return `information_ratio/max_drawdown` 混用。

LGBM：

| 因子集 | Loops | RankIC 均值 ± σ | IC 均值 | CAGR | Sharpe | 最大回撤 | Top20 h20 | 年化换手 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| G12 | 1/6/11 | 0.08799 ± 0.00033 | 0.05236 | 0.6107 | 1.7406 | -19.78% | 0.05174 | 18.92 |
| G13-F | 2/7/12 | 0.08848 ± 0.00017 | 0.05402 | 0.7030 | 1.9184 | -19.45% | 0.05507 | 19.78 |
| G14-FP | 3/8/13 | 0.09185 ± 0.00023 | 0.05630 | 0.6900 | 1.9214 | -21.12% | 0.05981 | 19.37 |
| G14-FL | 4/9/14 | 0.09101 ± 0.00050 | 0.05379 | 0.6586 | 1.8382 | -19.65% | 0.05542 | 19.32 |
| G15-FPL | 5/10/15 | 0.09405 ± 0.00068 | 0.05591 | 0.6761 | 1.8625 | -20.39% | 0.06004 | 19.12 |

EfficientGATs（`l2_code_id` embedding on_dim8，binary adjacency off）：

| 因子集 | Loops | RankIC 均值 ± σ | IC 均值 | CAGR | Sharpe | 最大回撤 | Top20 h20 | 年化换手 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| G12 | 1/6/11 | 0.09450 ± 0.00402 | 0.04984 | 0.5455 | 1.5812 | -16.96% | 0.05169 | 16.88 |
| G13-F | 2/7/12 | 0.09245 ± 0.00398 | 0.04486 | 0.4626 | 1.4619 | -18.45% | 0.03816 | 15.55 |
| G14-FP | 3/8/13 | 0.08590 ± 0.00905 | 0.04670 | 0.6012 | 1.7143 | -16.65% | 0.05420 | 16.37 |
| G14-FL | 4/9/14 | 0.09207 ± 0.00947 | 0.04561 | 0.5848 | 1.6819 | -16.70% | 0.05180 | 15.92 |
| G15-FPL | 5/10/15 | 0.08404 ± 0.00551 | 0.04030 | 0.5278 | 1.5643 | -16.34% | 0.04341 | 14.39 |

R6 结论冻结如下：

1. F/P/L 不是“加入越多越好”。LGBM 的 G15-FPL 取得最高 RankIC，但 CAGR/Sharpe 低于 G14-FP；L 因子改善排序不等于改善组合转换。
2. G14-FP 在 LGBM 上兼顾较高 IC/RankIC、Top20 和 Sharpe，且种子 RankIC 标准差仅 0.00023，因此作为 R7 h20 锚点；这不是宣称其所有指标均为五组最优。
3. GAT 的 G12 保留关系模型对照价值，但不同因子集的 RankIC 标准差明显高于 LGBM；新增 F/P/L 未形成跨种子、跨指标一致增量。单个高值 Loop 作为新假设线索，扩容、ensemble、动态关系和板块层模型分别通过新 trial 观察资源与稳健性。
4. R6 仍只评价 h20。它证明板块因子可被模型使用，但不能证明已捕获 60–180 日主升浪或可以进入生产。

#### 9.4.3 R7A 正式两腿组合结果（2026-07-15）

正式 run `macb_365aed6303e71d6e_20240701_20260629_20260714T174425343045Z` 使用 LGBM G14-FP h20（R6 Loops 3/8/13）和 GAT G12 embedding h20（R6 Loops 1/6/11）的三种子预测，OOS 为 2024-07-01 至 2026-06-29，Top50、`equal 0.5/0.5 + rank`、固定同一回测模板；状态为 `succeeded`。

| 方案 | CAGR | Sharpe | Calmar | 最大回撤 | Top20 h20 | Top20 命中率 | 年化换手 | 相对 LGBM |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| LGBM seed-ensemble baseline | — | 2.0215 | 3.6629 | — | — | — | — | 基线；Sharpe/Calmar 由 run 的正式 delta 反推 |
| R7A `equal + rank` | 0.664839 | 1.8052 | 3.3445 | -19.88% | 0.060834 | 59.05% | 17.82 | Sharpe -0.2163；Calmar -0.3184 |

R7A 当前 trial 标记为 `COMPLETED_CURRENT_TRIAL_BELOW_BASELINE`：组合管线已经完成真实回测，但等权 rank 未改善 LGBM 的风险调整后收益。历史 canary 的 RankIC 增量、较低预测相关和较低 TopK 重合与正式组合结果共同保留，用于解释“排序差异为何未转化为组合增益”；该结果不否定 GAT、portfolio fusion、跨标签组合或其他风险预算实验。

#### 9.4.4 R7B 正式结果与后续研究线索（2026-07-15）

R7B 严格执行 R7A 的单变量敏感性：seed ensemble、OOS、Top50、固定等权和 baseline 全部不变，仅将 `normalize_method: rank -> zscore`。正式 run 为 `macb_365aed6303e71d6e_20240701_20260629_20260714T190901628242Z`，状态 `succeeded`；OOS 为 2024-07-01 至 2026-06-29，LGBM G14-FP h20 与 GAT G12 embedding h20 各使用三种子 ensemble，权重固定 `0.5/0.5`。

| 方案 | CAGR | Sharpe | Calmar | 最大回撤 | TopK h20 收益 | TopK 命中率 | 换手 | 相对 LGBM Sharpe / Calmar |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| R7A `equal + rank` | 66.48% | 1.8052 | 3.3445 | -19.88% | 6.0834% | 59.05% | 17.82 | -0.2163 / -0.3184 |
| R7B `equal + zscore` | 67.95% | 1.8313 | 3.5429 | -19.18% | 6.3768% | 58.63% | 23.54 | -0.1902 / -0.1200 |

R7B 相对 R7A 的 CAGR、Sharpe、Calmar 和最大回撤分别改善约 1.47 个百分点、0.0261、0.1984 和 0.70 个百分点，但 TopK 命中率下降约 0.42 个百分点，换手增加约 5.72。R7B 的 Sharpe/Calmar 仍低于同口径 LGBM baseline，因此当前 trial 标记为 `COMPLETED_CURRENT_TRIAL_BELOW_BASELINE`。`ic_weighted/risk_parity`、portfolio fusion、跨标签融合和 LOO 仍是可研究假设；是否立即执行只按科研价值和资源排序，不受 R7B 结果阻断。

#### 9.4.5 Loop 级历史补账与被错误停止方向复核（2026-07-15）

QE 数仓复核采用 Loop 为最小研究证据单元。task 的 `failed` 只表示至少一个编排分支失败；只要某个 Loop 已完成并有可查询指标，就必须保留、分析并进入后续实验设计。本次首先恢复 6 个顶层失败任务中的 50 个成功 Loop：

| task | task 状态 | 成功 / 总 Loop | 已恢复的主要证据 | 修正后的解释 |
|---|---|---:|---|---|
| `qe_20260708_030423_dc46` R1 GPU | `failed` | 10/12 | TCN 4 组平均 RankIC/CAGR/Calmar 约 `0.1090/83.48%/6.99`；LSTM 4 组约 `0.1124/79.69%/6.59`；ALSTM 两组 RankIC 接近 0；仅 GRU2 两组失败 | TCN 与 LSTM 都是强 h20 证据，TCN 不应因父 task 失败或早期单次结果被降级；ALSTM 弱、GRU2 无结果只限定当前配置。 |
| `qe_20260704_021700_3b56` Phase0 Trend GPU | `failed` | 6/12 | LSTM h5/h10/h20 RankIC `0.0669/0.0836/0.0981`，CAGR `69.08%/72.90%/80.01%`；TCN h5/h10/h20 RankIC `0.0635/0.0848/0.0825`；6 个失败 Loop 均为该卡 Transformer 配置 | LSTM 对 horizon 呈清晰增强；TCN h10 与 R1 h20 均有价值；Transformer 失败不是 LSTM/TCN 失败，也不是其他 Transformer/关系架构的结论。 |
| `qe_20260624_011723_6df8` P2 LTR Probe | `failed` | 2/3 | LambdaMART 反转组 RankIC `-0.0644`，基本面组 `0.0105`；资金流组无结果 | 当前 h20/factor-group/LambdaMART 配置弱或方向可能错配；右尾长期排序、方向校验、G14-FP 与 h40/h60 LTR 仍是独立研究假设。 |
| `qe_20260622_035024_e194` P1 OrthoModality | `failed` | 1/12 | shareholder-concentration TCN h5/seed2027 RankIC `0.0607`、CAGR `61.76%`、Calmar `5.55`；其余 11 个 Loop 无成功指标 | 单个有效结果不足以概括模态，但已证明该模态不是“无信号”；持有人集中度与资金流加速需在当前基础架构重跑多期限/多种子。 |
| `qe_20260621_233558_38af` OrthoProbe | `failed` | 4/8 | 两组 LGBM RankIC `0.1197–0.1334`、CAGR `54.87%–75.85%`；其中较低 RankIC 的 flow 组反而平均 CAGR/Calmar 更高 | 资金流/情绪与估值模态具有不同的排序和收益转化角色，可作为 Type B 交叉数据或多腿候选；不能按父 task 状态删除。 |
| `qe_20260603_000503_c360` R9B LGBM | `failed` | 27/28 | RankIC `0.1122–0.1304`、CAGR `35.00%–89.21%`；仅 Loop2 失败 | 这是有效的 h20 反转/换手型强基线和 Type A 对照，不直接冒充 Type B，但必须用于正交性、标签任务差异和组合风险比较。 |

R2–R5 的 8 个任务均为顶层 `completed`，67/67 个 Loop 成功，但此前只有 R4 GPU 的 task identity 被明确写入本蓝图。补账后的因子与模型结论如下：

| 批次 | task / Loop | 关键数值 | Loop 级结论 |
|---|---|---|---|
| R2 CPU | `qe_20260713_015733_fc68`，8/8 | G12→G17 平均 RankIC `0.08265→0.09027`，平均 CAGR `65.99%→67.77%` | 五个板块因子对 LGBM 的排序增量在两超参、两种子上稳定，收益转换多数为正但不是每组都正。 |
| R2 GPU | `qe_20260713_015732_1db4`，8/8 | RankIC `0.06963–0.08547`，CAGR `33.41%–74.36%` | G17 与 embedding/关系配置存在明显交互，不存在统一的“加板块因子必增益”结论。 |
| R3 CPU | `qe_20260713_104758_46fa`，8/8 | flow+volume 平均 RankIC/CAGR `0.08701/69.31%`；breadth `0.08755/64.85%`；leadership `0.08579/61.65%`；四因子 `0.08817/66.73%` | 资金流背离+量价确认更偏收益转化，breadth 更偏排序，leadership 单独较弱；不能只选一个总分赢家。 |
| R3 GPU | `qe_20260713_104817_6207`，6/6 | G17 相对 G12 在 seed7 增强、seed17 接近、seed2025 变弱 | GAT 板块因子效果具有种子/优化路径敏感性，适合做 ensemble、关系结构和板块层归因，不宜用一个种子宣称天花板。 |
| R4 CPU | `qe_20260713_195906_93b5`，4/4 | G12→G17 平均 RankIC `0.08785→0.09558`，平均 CAGR `65.14%→64.12%` | 信号提升稳定但组合收益转换混合，说明模型分数到调仓/成本的桥接是独立研究问题。 |
| R4 GPU | `qe_20260713_195926_11e3`，4/4 | off→binary industry-bias 平均 RankIC `0.10477→0.09764`，平均 CAGR约 `57.51%→57.83%` | 当前二值行业偏置削弱排序、收益近中性；只约束该边定义，不否定 GAT、HIST、动态关系或两层板块模型。 |
| R5 CPU | `qe_20260713_230009_b072`，20/20 | 单加 flow 平均 RankIC/CAGR `0.08827/68.26%`；persistence `0.09102/65.10%`；LOO/交互结果进一步显示 flow 对收益转换重要 | `F=资金流背离` 与 `P=广度持续性` 分工明确；breadth level、volume、leadership 仍保留为状态/交互变量，不因单腿收益较弱删除。 |
| R5 GPU | `qe_20260713_230038_ab62`，9/9 | RankIC `0.07294–0.09532`，CAGR `41.34%–60.60%` | GAT 对 embedding、因子集和种子敏感，研究重点转向稳健 ensemble、动态关系和板块决策层，而不是宣布模型无效。 |

R6 在上述补账后应解释为“因子角色分化”而不是单一胜者：LGBM G12/G13-F/G14-FP/G14-FL/G15-FPL 三种子平均 RankIC 约为 `0.08799/0.08848/0.09185/0.09101/0.09405`，平均 CAGR 约为 `61.07%/70.30%/69.00%/65.86%/67.61%`。因此 G14-FP 继续作为均衡 h20 锚点，G13-F 恢复为收益转化候选腿，G15-FPL 作为排序增强候选；三者进入长期标签、持有周期和组合层对照，而不是互相淘汰。

### 9.5 两层板块轮动模型

不可部署的四格 oracle 上界与真实 hard/soft top-down 工程并行研究，用来回答“板块选择”和“板块内选股”各自还有多少可提取空间；oracle 是诊断尺，不是启动真实模型的前置许可：

| 板块选择 | 板块内选股 | 用途 |
|---|---|---|
| reality | reality | 当前可实现的一层/两层基线 |
| oracle | reality | 隔离板块预测层上界 |
| reality | oracle | 隔离板块内排序层上界 |
| oracle | oracle | 整条层次结构的理论上界 |

oracle 使用未来收益构造研究上界，始终标记 `QE_ONLY_FUTURE_INFORMATION_CEILING`，与现实模型结果分栏解释。Top-M、评价 horizon、Top-K、PIT 成员、可交易约束、信号到成交规则、成本和每格相对 reality/reality 的经济增量都随 trial 保存；置信区间和阈值用于量化空间，不产生 `GO/STOP` 决策。soft-gating 上界与 hard Top-M 同步运行。F-014 任一可用指标族都可立即补充 oracle 分析，缺失指标族记录补算计划。

真实 top-down 对照与 oracle 可并行建立：

1. 板块层使用等权板块面板及 A1–A6 可得因子，对申万 L2 板块做 20/40/60 日趋势、广度、资金与状态评分，输出 hard Top-M 与连续 soft-gating 分数；
2. 个股层在板块条件下使用长期趋势因子、板块内 leadership 和流动性/可交易性选择股票；soft gate 对全市场保留非零候选权重，不把层次模型简化为硬过滤；
3. 行业不做标签中性化，但组合层记录单板块上限、板块集中度、轮动成本和涨跌停/停牌造成的捕获损失；
4. 与相同候选池、Top-K、风险和成本预算的一层 LGBM、GATs、简单“板块动量 + 板块内动量”以及 oracle 四格比较。

板块层和个股层分别归档分数、入选原因和未入选原因。若 oracle-sector/reality-stock 较高但现实板块层无增量，当前瓶颈可能在板块预测；若 reality-sector/oracle-stock 较高但现实个股层无增量，当前瓶颈可能在板块内排序；若 oracle/oracle 上界也偏低，只说明当前样本、板块定义、horizon、Top-M/Top-K 和成本口径下的上界有限，继续记录新数据、新板块体系、新因子或新结构的验证方案，不停止两层研究方向。

### 9.6 长期上涨趋势专用评价

h20 继续作为当前模型对照的统一信号标签，但它不能单独代表“连续上涨数月、捕获右尾大行情”的策略目标。post-R6 结果逐步补充与 `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` Phase 8 可对齐的指标：

- 20/40/60/120/180 个交易日收益画像和按日期聚类的置信区间；
- `+30%/+50%/+70%` 有序目标的 Recall@20/50、到达概率与 time-to-hit calibration；Recall@100 使用独立深池 profile 并与 Recall@20/50 并列，profile 尚未实现时记录数据/实现计划，不影响其他指标和实验；
- trend-stage survival、右删失、趋势失效、峰前回撤、MFE/MAE、trend capture ratio 与 false early-exit rate；
- 最近 1 年、最近 6 个月及科技抱团/板块集中等预注册 regime 切片，但不得用切片结果反向选择全期公式；
- 持仓超过 30 日不设硬约束，由信号存活、趋势失效和成本后收益决定。

Type A 超跌反弹与 Type B 长期趋势保持独立因子选择、标签头、调仓和退出逻辑；旧多 Alpha 腿只能作为组合相关性/风险基线，不能作为 Type B 演进母体。

F-014 的可实施详细设计已更新至父设计 v1.22，并继续引用 `qe_long_trend_evaluation_phase2_compute_cas_f2_design_20260722.md`。它只读复用现有 Recorder/prediction pointer 和 `qe_archive.run` 父身份，不扩展通用 Prediction Store 或既有 Archive 通用 writer/schema；使用 feature/outcome 双快照、execution environment identity、extension-only 历史价格校验和右删失；逐信号/episode 明细进入 QE-only CAS Parquet。Phase 2 control/worker/CAS 与 Phase 3 metric/artifact/API/MCP 已完成。PR #2906 source/CI、生产 DDL 和当时的 8001 runtime contract 已闭合；固定 R8B Loop4 的 historical preview GET 已真实返回 8/11 可用与 3 个 typed gap，且数据库前后不变。PR #2964 的受控 Chromium 已验证业务检索选择器、无人工 ID/JSON 与 Top-K 真实覆盖，并产出成功截图。自 v5.24 起，normal Loop 平台 smoke、完整 root/runtime SHA 对齐、3000 runtime visual、其余 5 个 R8B、Phase 5 中断恢复与批量历史重评均由用户终止，不再是活动缺口；既有可用结果只读复用，任何新策略 candidate 都不等待 F-014 完整化。

F-014 历史上按三个工作流建设：`计算/统计/可成交性`、`CAS/状态/三表/幂等恢复`、`API/MCP/UI/历史补算`。v5.24 后前两条只复用既有能力，第三条已终止；不再继续推进平台交付。既有结果仍按指标族分别解释：

| 指标族 | 典型来源 | 独立状态与科研用途 |
|---|---|---|
| `signal_path` | prediction + 价格路径 | 可计算 hN、Recall、barrier、MFE/MAE 时立即分析；缺价格段记录覆盖损失和补数方案。 |
| `position_episode` | position 快照 | 可计算持仓 capture、false early-exit 时立即分析；缺失不影响 signal path。 |
| `portfolio_result` | report/portfolio indicator | 可计算成本后收益、回撤、换手时立即分析；缺失不影响信号与持仓研究。 |
| `order_fill` | indicator object 的 `amount/deal_amount/ffr` 等 | 可区分 attempted/filled/partial/zero-fill 时立即分析；缺字段时研究其他指标族。 |
| `execution_cause` | reason code、订单/队列轨迹或可复算规则 | 证据不足时仅把具体原因标为 `NOT_VERIFIABLE`，并设计采集或互证方案；不阻断成交状态、长期捕获或任何实验。 |
| `platform_surface` | QE CAS/三表/API/MCP/UI/历史补算 | 只读记录既有工程完成度；UI 与历史批量补算缺失不再形成待办，也不影响已有计算结果用于科研分析。 |

每个指标族使用 `AVAILABLE/PARTIAL/MISSING/NOT_VERIFIABLE/NOT_APPLICABLE`，并同时记录覆盖率、缺口、可用部分和损失/偏差。脚本、CAS、后端、UI 与历史补算的既有状态互不替代；缺失面不再启动补齐工作，也不作为策略实验前置条件。

F-014 同时增加信号→成交桥接：理论机会层以 `T+1 qfq close` 衡量可预测机会，实际捕获层优先使用经 reconciliation 的 order/trade artifact，区分 `filled_t1`、`delayed_fill`、`blocked_limit_up`、`blocked_suspension`、`never_filled` 及 `entry_delay_days`，并报告入场损失的 MFE/barrier winner；退出侧对称分析跌停、停牌和延迟退出。日线触及涨跌停不等于订单必然未成交；没有订单/队列证据时仅将精确原因标记 `NOT_VERIFIABLE`，继续使用 position、report、indicator object 和价格路径做可支持的捕获、成交状态、损失区间与互证分析。

#### 9.6.1 长周期实验的四条独立轴

长期趋势实验优先采用正交设计分别观察以下四条轴；若一个 trial 同时改变多条轴，必须记录完整组合并用补充消融解释归因，而不是因此禁止实验：

1. `label_horizon`：未来 30/40/60/120/180 个交易日收益目标；决定模型学习什么。
2. `step_len/lookback`：LSTM/TCN 向后观察多少个交易日；决定模型看到什么历史。
3. 决策/调仓周期：多久重新排序、补仓、减仓或替换持仓；决定长期信号如何转化为交易频率和成本。
4. 持仓/退出逻辑：由信号存活、趋势失效、成本后收益和组合约束决定；不机械等同于标签期限，也不预设“必须持有 N 日”。

当前 QE 已支持 `1/3/5/10/20/30/40/60/120/180` 标签，并对 `horizon >= 30` 的 learning frame 应用 `LongHorizonLabelMaturityPurge`。R8A 先固定模型和因子，只比较标签期限：

| 项目 | 冻结值 |
|---|---|
| 基线 | 复用 R6 LGBM G14-FP h20 三种子结果，不重复训练 |
| 新标签 | h30、h40、h60、h120；h180 暂不进入首批 |
| 模型/因子 | `LGBModel_conservative_v1` / G14-FP |
| seeds | 123、314、2718 |
| 规模 | 12 个 CPU Loop；远端 CPU 按已验证资源上限执行 |
| 固定项 | 2026-06-30 数据、股票池、split、Alpha158 off、Top50、V25、费用与 ST PIT 策略 |
| 主评价 | 各自 hN IC/RankIC + HAC/block/non-overlap、TopK hN、absolute CAGR/Sharpe/回撤/换手 |
| 长期评价 | F-014 的 +30/+50/+70 Recall、time-to-hit、MFE/MAE、survival、capture/false-exit 和 regime 切片 |

h30 是 h20→h40 的插值控制，h40/h60 是两月至一季度趋势目标，h120 是半年趋势探针。h120 会从每个 learning segment 屏蔽最后 121 个交易日标签，有效样本和近期成熟标签显著减少；统一共同成熟截止日、各期限最大可用截止日和原始 IC 同时报告。R8A 现有指标和既有 F-014 指标族都是有效科研证据；未实现的指标只保持不可用/受限说明，不启动补充建设，也不限制期限分析。

R8A 已完成，task 为 `qe_20260715_101942_d873`，名称为 `R8A-CPU Type-B G14-FP 长周期标签 h30-h120 4horizon x 3seed LGBM`。12 个 Loop 固定分配到 `rdagent-node1`，并行度 4；h30/h40/h60/h120 均为 3/3 成功。任务使用 `filtered_pool_20260630`、G14-FP 14 因子、Alpha158 off、`score_weighted_topk_v2`、Top50、`V25_1_SMALL_CAP`、同一数据 split、费用和 QE-only ST PIT，所有 Loop 均已归档。

R8A 的种子平均 RankIC 随 h30/h40/h60/h120 依次为 `0.09824/0.10290/0.11119/0.12115`，呈期限上升；三种子日截面 rank ensemble 后为 `0.09870/0.10340/0.11170/0.12179`。但统一共同成熟区间截至 2025-12-24 时四个期限为 `0.11635/0.11925/0.12360/0.12179`，h120 不再单向领先；2026H1 已成熟的 h30/h40/h60 仅为 `0.02331/0.02591/0.02916`。因此期限拉长提高了全样本平均排序，却没有解决近期市场状态偏移与主升浪漏捕；后续同时分析预测目标、板块状态、决策周期、持仓存活和退出逻辑。

#### 9.6.2 R8B LSTM 主模型证据与其他模型并行顺序

R8B 不读取 R8A 中途结果，而是提前预注册 h40/h60 两个业务优先期限作为并行 canary；每个期限使用 seeds `123/314/2718`，首轮固定 `step_len=20`，共 6 个 `gpu_parallel_standard` Loop。这样可在 R8A 完成前利用独立 GPU 节点缩短等待时间，同时仍能在事后按同期限、同种子比较 LGBM 与 LSTM 的模型归纳偏置。R8B2 可独立研究 `step_len=20/40/60`；R8B 仍按原卡归档，二者并列比较，不以 R8B 结果作为 R8B2 的研究许可条件。

R8B 已完成，task 为 `qe_20260715_104922_001d`，名称为 `R8B-GPU Type-B G14-FP LSTM h40-h60 2horizon x 3seed canary`。模型固定 `__seed_LSTM_10D_hs64_d02__`，使用 `TimeSeries`/`TSDatasetH`、`step_len=20`；G14-FP 14 因子、数据快照、股票池、split、Alpha158、策略、Top50、执行算法、费用和 QE-only ST PIT 与 R8A 完全一致。6 个 Loop 分配到 `wsl2-5080`，并行度 2，h40 与 h60 均 3/3 成功。h40 LSTM 平均 RankIC/CAGR/Calmar/Sharpe/年化换手约 `0.10092/61.88%/3.17/1.6848/18.47`；h60 约 `0.10011/61.19%/2.98/1.7655/15.98`。对比同期限已完成的 LGBM 结果，LSTM 的 RankIC 未形成单向优势，但收益转化、风险调整后表现和换手呈现不同结构，足以支持继续研究时序模型与策略周期匹配，不把 RankIC 单项当作模型排序。

##### 9.6.2.1 R8A/R8B 三种子、分段、板块与右尾深度归因

18 个 Loop 均有可读的 `pred.pkl`、`label.pkl` 和 `params.pkl`，每个预测/标签约 220.7 万行、4,641 只股票，覆盖 2024-07-01 至 2026-06-30。主 ensemble 采用每日截面 percentile rank 后三种子等权平均；z-score ensemble 作为敏感性对照，两者结论近似。成熟标签覆盖率达到 80% 的最后日期分别为 h30 `2026-05-15`、h40 `2026-04-28`、h60 `2026-03-30`、h120 `2025-12-24`；跨期限共同成熟截止为 `2025-12-24`。

| 模型/期限 | 种子平均 RankIC | rank ensemble | ensemble 增量 | 2024H2 | 2025 | 2026H1 |
|---|---:|---:|---:|---:|---:|---:|
| LGBM h30 | 0.09824 | 0.09870 | +0.00046 | 0.13252 | 0.10766 | 0.02331 |
| LGBM h40 | 0.10290 | 0.10340 | +0.00050 | 0.12918 | 0.11405 | 0.02591 |
| LGBM h60 | 0.11119 | 0.11170 | +0.00051 | 0.11672 | 0.12779 | 0.02916 |
| LGBM h120 | 0.12115 | 0.12179 | +0.00065 | 0.10071 | 0.13287 | 未成熟 |
| LSTM h40 | 0.10092 | 0.10339 | +0.00248 | 0.07775 | 0.13587 | 0.04089 |
| LSTM h60 | 0.10011 | 0.10231 | +0.00220 | 0.05087 | 0.14670 | 0.02311 |

LGBM 种子间每日 rank 相关约 `0.981–0.983`、Top50 重合约 `79%–84%`，ensemble 主要提供稳定化；LSTM 种子相关约 `0.917–0.920`、Top50 重合约 `53%–56%`，ensemble 的信息增益明显更高。LSTM h40/h60 ensemble 的 Top50 平均前向标签相对单种子均值分别增加约 `0.00387/0.00636`，因此跨种子集成应作为 LSTM 主线的正式研究项。

每日未来收益 Top1% 定义为 signal-level 右尾事件，ensemble Top50 捕获率为：LGBM h30/h40/h60/h120 `1.49%/1.57%/1.29%/1.91%`，LSTM h40/h60 `1.33%/1.38%`；约 4,600 只股票下 Top50 的随机重合基线约 `1.1%`。2026H1 的 LGBM h30/h40/h60 降至 `1.18%/1.10%/0.51%`，LSTM h40/h60 为 `0.78%/0.91%`。这些是按信号日计数的重叠标签事件，不是去重后的独立上涨 episode；F-014 将补充 episode 合并、首次识别、MFE/MAE、time-to-hit 和持续捕获分析。

近期代表性漏捕为 `688146.SH 中船特气`：2026-03 多个 h60 信号日的前向标签约 `+6.67` 至 `+8.56`，LGBM ensemble 排名约 `1,229–3,399`，LSTM 约 `2,117–2,907`，远离 Top50，表明该类损失首先发生在信号排序层。`688585.SH 上纬新材` 的部分 h120 信号排名约 `52–119`，则更接近 TopK 边界，R9S 的持仓缓冲、降低替换和后续动态 TopK 可用于量化这类转换损失。个案标签是权威 `label.pkl` 的前向复权收益，不直接冒充已实现策略收益。

2026H1 的信号 Top50 主要集中于环境治理、房地产开发、生物制品、光伏设备、普钢、医疗器械和化学制品；右尾漏捕则持续集中于半导体、通信设备、软件开发、IT 服务、通用设备、汽车零部件和电池。LSTM 的平均板块 HHI 约 `0.062–0.068`，高于 LGBM 的 `0.044–0.049`，说明 LSTM 更依赖板块状态，但当前板块状态与科技主升浪仍存在错配。两层 sector oracle、soft gating、板块趋势/宽度/资金流加速和板块内龙头因子因此获得更高信息增益优先级。

LGBM 从 h30 到 h120 的板块原生重要性由 `21.74%` 降至 `12.63%`，基本面由 `17.79%` 升至 `23.89%`，`Price_Deviation_Historical_High` 由 `27.50%` 升至 `37.30%`，表现为慢价格状态与基本面排序增强。LSTM h40/h60 的板块原生重要性约 `38%`、板块相关合计约 `52%`，但因子重要性种子波动更大。LightGBM gain 与 PyTorch correlation 是不同归因方法，原始权重不作跨模型绝对比较；后续以 G13-F/G14-FP/G15-FPL 消融、板块暴露和右尾捕获共同解释因子角色。

同期限 LGBM/LSTM 每日 rank 相关为 h40 `0.721`、h60 `0.709`，Top50 重合仅约 `20.8%/21.2%`。这支持继续做固定风险预算的 portfolio-level R8C 与任务级 LOO，但不预设简单 prediction fusion 会改善。现有 `run_position/run_order` 为空，`run_trade` 缺少可重建逐日仓位的 quantity；因此上述板块暴露是精确的 signal Top50 暴露，累计 `run_symbol_summary` 只作持有倾向旁证。缺失的逐日 position/order 和成交原因进入 F-014 数据获取与互证，不撤销 signal-level 结论，也不停止任何实验方向。

历史 LSTM 证据不能继续从蓝图中缺席：R16A `qe_20260609_025155_87ee` 最优 Loop CAGR/RankIC/Sharpe/Calmar 为 `112.11%/0.12222/2.5193/7.7118`；R9A `qe_20260603_000500_8936` 最优 Loop为 `108.29%/0.12809/2.5632/7.9914`；R17A `qe_20260609_154050_0451` 最优 CAGR 为 `107.67%`。这些 trial 使用 h20、旧股票池、不同因子组合并包含明显的反转/换手信息，不能直接回答当前 Type-B 长期趋势目标，但它们明确证明 LSTM 不是待恢复的边缘候选，而是已有强收益转化能力、必须优先迁移和重新定标的主模型架构。

以下仅是资源与信息增益优先级，不是模型研究门禁：

1. LSTM：Type-B 长期趋势第一主模型。优先建立 R8B2 的 `step_len=20/40/60`、`label horizon × decision/rebalance cycle × exit/holding logic`、G13-F/G14-FP/G15-FPL 因子角色、跨种子集成和 R8C 长期腿组合；R8M 首个共享/迁移表示也优先采用 LSTM encoder。
2. TCN：高优先级并行时序对照，不再错误概括为弱模型，也不与 LSTM 争夺先后许可。R1 四组 TCN 平均 RankIC/CAGR/Calmar 约 `0.1090/83.48%/6.99`；建立 G14-FP h40/h60 多种子对照，并扩展 G13-F/G15-FPL 因子角色，用差异说明结构互补性。
3. LGBM 策略转换：R8A 的 RankIC/CAGR 反向分化优先形成“标签期限 × 调仓周期 × 退出逻辑”对照，区分模型预测不足与策略转换不足。
4. GAT/关系模型：R7B 当前 prediction-fusion 未超过 LGBM，R2–R6 又显示种子与关系结构敏感；h40/h60、portfolio fusion、板块决策层、HIST 和动态边继续作为不同 trial。
5. LambdaMART/LTR：旧 P2 task 仅完成两个 h20 因子组且资金流组失败，不能称为“LTR 已死”。新研究卡检查预测方向和 ranking label，再比较 G14-FP/G13-F 的 h40/h60 与右尾 barrier/NDCG 类目标。
6. ALSTM/GRU2/时间维 Transformer：当前 ALSTM 两 Loop 弱、GRU2 两 Loop 失败、Phase0 Transformer 六 Loop 失败，分别记录为当前配置证据；修复 wiring、改变任务定义或关系结构后仍可重访。CatBoost/XGBoost/TabPFN 同样可按计算成本和假设独立研究。

#### 9.6.3 R8M 多期限共享表示与迁移假设

R8M 是 R8 之外的独立研究卡，不追加或重写 R8A/R8B，也不因“共享表示”自动成为新 Alpha 腿。它检验长 horizon 样本减少时共享/迁移是否有净增量，不预设正迁移：

1. 独立训练 h20/h40/h60/h120；
2. 共享 encoder + 各期限独立 head；
3. h20 预训练 + 冻结 encoder，只训练长期 head；
4. h20 预训练 + 全量 fine-tune。

每个 head 使用自身 maturity mask、purge 和 loss denominator，不用未成熟标签补 0。实验同步输出 transfer matrix、leave-one-head-out、per-head 样本量与梯度余弦/冲突诊断；PCGrad 或动态 loss weighting 可作为独立对照，并注明是否观察到梯度冲突。设计、wiring canary、完整多种子矩阵和 F-014 各指标族可并行；IC 迁移、长期捕获、可成交性损失和固定风险预算组合增量分别分析，不存在单一裁决门。

#### 9.6.4 多腿组合的比较框架

首轮 Type B 多 Alpha 以三条腿作为便于归因的起始规模，不构成上限或准入门禁。可研究更多腿；每条腿记录其经济任务、因子谱系和标签关系，以解释重复与互补：

| 角色 | 首选候选 | 主要比较证据 |
|---|---|---|
| 中短期趋势锚点 | LGBM G14-FP h20 | R6 已完成；R7 baseline |
| 收益转化候选 | LGBM G13-F h20/h40/h60 | R5/R6 显示资金流背离的 RankIC 不最高但 CAGR 较强；与 G14-FP/G15-FPL 比较预测、持仓和 P&L 重合 |
| 长周期趋势腿 | LSTM 为第一主模型；R8A/R8B/R8M 与 TCN 的 h40/h60 LGBM/LSTM/TCN | F-014 可得的长期指标、决策/退出周期、相对 h20 的任务级差异、模型间互补及数据缺口 |
| 排序目标探针 | 重开后的 h40/h60 LambdaMART/LTR | 方向校验、NDCG/右尾捕获、与回归模型的 prediction/TopK/P&L 重合 |
| 板块决策/关系腿 | 两层板块→个股模型；HIST-industry | 板块 Recall、板块内排序、同风险预算净收益与回撤的并列证据 |
| 交叉数据候选 | shareholder concentration、flow acceleration、基本面/资金流组合 | 旧失败 task 中的成功 Loop、覆盖损失、多期限和多种子互证 |

h30、h40、h60、h120 不是天然独立的四条腿，模型名不同也不自动构成经济上独立的任务。每个组合 trial 报告每日 prediction rank 相关、TopK 与实际持仓重合、入场时点重合、板块暴露/集中度重合、P&L 相关、`+30%/+50%/+70%` 右尾事件重合、换手/成本重合，以及固定总风险预算下的 leave-one-leg-out。相关性、LOO 或成本结果只解释当前组合的互补、冗余和损失，不否决腿或研究方向；可据此继续调整权重、任务、标签、退出逻辑或新增腿。R7B 只描述当前 0.5/0.5、rank/zscore、Top50 和现有成本下的 prediction-fusion，不否定 portfolio fusion、跨标签组合或关系模型的板块选择价值。

#### 9.6.5 R9S/R10 策略转换、模型与因子角色结果（2026-07-17）

R9S 固定复用 R8 prediction，仅改变每日替换预算。结果证明标签期限与调仓速度之间不存在单调关系：

| 模型/期限 | 策略 | RankIC | CAGR | Sharpe | 最大回撤 | 年化换手 |
|---|---|---:|---:|---:|---:|---:|
| LSTM h40 | `n_drop=1` | 0.10092 | 64.70% | 1.6685 | -20.71% | 6.40 |
| LSTM h40 | `n_drop=3` | 0.10092 | 62.57% | 1.6649 | -19.50% | 13.11 |
| LSTM h60 | `n_drop=1` | 0.10011 | 50.77% | 1.4993 | -20.45% | 6.12 |
| LSTM h60 | `n_drop=3` | 0.10011 | 61.80% | 1.7445 | -18.84% | 11.53 |
| LGBM h60 | `n_drop=1/3` | 0.11119 | 53.35% / 57.47% | 1.5877 / 1.7215 | -21.19% / -20.44% | 6.00 / 12.22 |
| LGBM h120 | `n_drop=1/3` | 0.12115 | 44.66% / 41.39% | 1.3863 / 1.3352 | -20.40% / -21.51% | 6.39 / 12.92 |

R10 将 G13-F/G15-FPL 放入 LGBM、TCN、LSTM 的 h40/h60 同口径矩阵，并增加 LambdaMART 与固定持仓周期。最终为 51/51 个成功 Loop，关键三种子均值如下：

| 模型/期限 | 因子角色 | RankIC | CAGR | Sharpe | 解释 |
|---|---|---:|---:|---:|---|
| LGBM h40 | G13-F | 0.09992 | 50.36% | 1.5077 | 收益转换弱于 G15 |
| LGBM h40 | G15-FPL | 0.10469 | 54.36% | 1.5877 | 排序和收益均有小幅增量 |
| LGBM h60 | G13-F | 0.10976 | 56.21% | 1.6941 | RankIC 略低但收益转换更强 |
| LGBM h60 | G15-FPL | 0.11185 | 54.74% | 1.6583 | 排序增强未完全转化为收益 |
| TCN h40 | G13-F | 0.10838 | 65.19% | 1.7410 | 当前 TCN h40 的强组合 |
| TCN h40 | G15-FPL | 0.10188 | 62.51% | 1.6955 | 低于 G13-F，但仍有独立模型证据 |
| TCN h60 | G13-F | 0.10260 | 55.05% | 1.6422 | h60 转换一般 |
| TCN h60 | G15-FPL | 0.10298 | 60.20% | 1.7182 | 板块宽度/领导信息在 h60 转化更好 |
| LSTM h40 | G13-F | 0.10648 | 61.38% | 1.6361 | 三种子完成，继续支持 LSTM 主线 |
| LSTM h60 | G15-FPL | 0.10206 | 67.55% | 1.8060 | 三种子完整，收益较强但 RankIC 存在种子分散，继续进入动态退出和组合层研究 |

R10E2 的 hold10 三种子平均 CAGR/Sharpe/换手为 `61.72%/1.7158/11.57`；R10E3 的 hold30 三种子平均为 `54.21%/1.566/5.995`。固定持仓越长只稳定降低换手，没有稳定提高收益或回撤，因此后续把“继续持有”和“提前减仓”改为可学习的趋势存活、回撤 hazard 和动态退出问题。

LambdaMART h40/h60 的平均 RankIC 为 `-0.0241/0.0097`，但 h40 平均 CAGR 仍为 `55.38%`。该不一致进入 score 方向、group/ranking label、NDCG、暴露与策略转换诊断；它描述当前配置，不构成 LTR 方向处置。

#### 9.6.6 R11 固定持仓与 EfficientGATs 长周期结果（2026-07-18）

R11A 使用同一组 G14-FP h60 seed prediction，只改变 `hold_thresh`，因此 RankIC 在同 seed 内完全一致，策略差异来自持仓和交易转换。Loop5–8 的原始训练、预测与回测均已成功，`results_only` 首次恢复只在任务状态汇总处触发 BUG-741；修复后沿同一公开恢复入口完成，不重训、不改变预测。三种子结果如下：

| hold | 平均 RankIC | 平均 CAGR | 平均 Sharpe | 平均最大回撤 | matched-seed 解释 |
|---:|---:|---:|---:|---:|---|
| 10 | 0.11119 | 58.44% | 1.738 | -20.06% | seed314 最强；整体收益和风险效率均衡 |
| 20 | 0.11119 | 59.18% | 1.708 | -20.58% | seed123/2718 高于 hold10、seed314 低于 hold10；平均 CAGR 仅高约 0.74 个百分点，不构成固定期限单向优势 |
| 30 | 0.11119 | 49.30% | 1.496 | -21.79% | 三种子整体弱化，尤其 seed2718 CAGR 降至 42.73% |

R11A 不支持“持有越久越能捕获主升浪”的简单假设，也不要求放弃长持有。更有信息增益的下一步是以 hold10/20 为两个现实基线，让板块趋势存活、宽度/资金/领导扩散、波动拥挤和回撤 hazard 决定是否继续持有、减仓、退出和重入，并使用 F-014 的 episode/MFE/MAE/false early-exit 指标解释损失来源。

R11B 的 EfficientGATs G14-FP 结果如下：

| 标签期限 | 平均 RankIC | 平均 CAGR | 平均 Sharpe | 平均最大回撤 | 年化换手 |
|---:|---:|---:|---:|---:|---:|
| h40 | 0.10461 | 54.73% | 1.588 | -17.02% | 5.78 |
| h60 | 0.10156 | 55.51% | 1.677 | -16.95% | 11.17 |

h60 的 CAGR/Sharpe 仅小幅变化，RankIC 下降且换手接近 h40 的两倍；当前 EfficientGATs 后续默认以 h40 作为效率锚点。与 LSTM/TCN/LGBM 相比，图模型不是当前最强单腿，但回撤较浅、换手较低，继续支持把它用于关系状态、板块风险预算、sector gating 和 portfolio-level diversification。R7 只证伪当前简单 prediction fusion；R11B 不应被重新压回同类等权平均，而应进入板块层条件化和组合风险分配。

#### 9.6.7 R12P 跨任务四腿组合与任务级 LOO（2026-07-26）

R12P 复用 LGBM G14-FP h60、LSTM G15-FPL h60、TCN G13-F h40 与 EfficientGATs G14-FP h40 的 12 个已归档 seed prediction，OOS 为 2024-07-01 至 2026-06-29，Top50、zscore、walk-forward 60/20 和相同交易配置保持不变。原失败 run 与全部恢复 successor 均保留；最终 `results_only` successor 没有重新训练模型。

| 组合 | CAGR | Sharpe | Calmar | 最大回撤 | 年化换手 | Top20 收益 / 命中率 |
|---|---:|---:|---:|---:|---:|---:|
| LGBM baseline | 53.57% | 1.6020 | 2.6494 | -20.22% | 5.9747 | 11.10% / 57.86% |
| 四腿 equal | 74.47% | 1.9224 | 3.5544 | -20.95% | 6.5213 | 14.30% / 61.50% |
| 四腿 orthogonality-aware | 72.12% | 1.8927 | 3.3017 | -21.84% | 6.5064 | 14.12% / 61.31% |

equal 在当前 trial 中同时高于 baseline 和 orthogonality-aware。相对 baseline，equal 的 CAGR 增加约 `20.90` 个百分点，Sharpe 增加 `0.3204`，Calmar 增加 `0.9051`，最大回撤加深约 `0.73` 个百分点，年化换手增加约 `0.55`。这证明当前跨任务组合的经济信息可以互补，但不表示固定等权在其他时期、资金规模或新腿集合中必然最优。

LOO 的 `marginal_*` 定义为“完整组合指标减去 drop-one 指标”；正值表示该腿提高完整组合指标，负值表示移除该腿后指标反而提高：

| scheme | 腿 | ΔCAGR | ΔSharpe | ΔCalmar | 当前归因 |
|---|---|---:|---:|---:|---|
| equal | LGBM h60 | +9.13pp | +0.2527 | +0.7322 | 两种 scheme 中最稳定的核心贡献腿 |
| equal | LSTM h60 | +3.23pp | +0.0681 | +0.3417 | 提供收益与风险效率补充 |
| equal | GAT h40 | +2.89pp | +0.0073 | +0.2376 | Sharpe 近中性，但改善 CAGR/Calmar |
| equal | TCN h40 | +0.77pp | -0.0329 | +0.0676 | 收益小幅正贡献、Sharpe 轻微拖累 |
| orthogonality-aware | LGBM h60 | +4.34pp | +0.1666 | +0.3531 | 仍为主要正贡献腿 |
| orthogonality-aware | GAT h40 | +0.13pp | -0.0082 | -0.1068 | 当前权重下接近中性并拖累风险效率 |
| orthogonality-aware | LSTM h60 | -0.21pp | -0.0007 | -0.0813 | 当前权重下轻微拖累 |
| orthogonality-aware | TCN h40 | -0.35pp | -0.0312 | -0.1636 | 当前权重下拖累最明显 |

这些结果支持保留 GAT 的组合层/板块风险角色，而不是把它解释成最强单腿；同时说明“正交权重”名称本身不保证经济收益优于等权。下一轮不重复简单 prediction fusion，转向 sector-risk overlay、动态退出和图模型 sector gating；LSTM/TCN 的长期标签与因子角色仍可并行研究。

### 9.7 PIT 关系模型路线

1. **HIST-industry canary**：只消费实验启动前由专用数据集流程生成、带 dataset identity/cutoff/hash 的逐日 PIT `stock2concept`/稀疏成员文件，补齐 composer 分支、`stock_index` 对齐、每日截面 batch、fit/predict 与回测契约。composer、worker 和 qrun 禁止访问 `market.sw_index_member` 或任何数据库表；缺文件或 identity 不匹配时 fail closed。第一轮只验证 wiring、资源和同口径基线；禁止静态行业快照。
2. **动态加权/多关系图**：分别测试只用历史窗口构造的 residual co-movement、leadership、flow/state 与 cross-sector lead-lag 权重；边类型分头处理并做逐关系消融。R4 的真码二值 `industry_bias` 作为已完成负/混合对照保留。
3. **板块层时序动态图与回撤 hazard**：板块节点使用相对强弱、宽度、资金流、领导扩散、残差协同、波动与拥挤的历史窗口状态；股票通过逐日 PIT 行业边连接板块节点。至少并列长期趋势头与未来 `5/10/20` 日板块回撤 hazard，回撤事件同时记录 `-5%/-8%/-10%` 首次触达、峰谷回撤、是否先创新高及回撤后修复，区分正常整理与趋势失效。
4. **sector gating 与动态退出**：图模型输出优先作为板块风险预算和持仓状态，不再默认与逐股 prediction 做等权平均。LSTM/TCN/LGBM 负责板块内个股排序；板块 hazard 上升时允许有界减仓或覆盖固定 hold threshold，风险消退后保留可解释的重入。评价同时报告预警提前天数、避免回撤、false early-exit、post-exit MFE、重入延迟、趋势捕获率、换手和成本。
5. **HATS/层次关系**：可以与单关系基线并行；关系数量和各自结果用于解释模型实际学到的结构，不作为启动条件。
6. **MASTER/IGMTF/TRA/MIGA**：MASTER 成本较高，保持结果触发后的 P2 条件方向；IGMTF 可作低成本关系 canary。TRA/MIGA 只在固定权重/LOO 和状态切片证明专家存在条件互补后，作为 P1 learned-routing 机制先验运行严格 OOF canary。它们不是彼此的等价实现，既有时间维 Transformer 结果仅作对照。
7. **DoubleAdapt/Proceed/在线适应**：先用同合同 fixed/expanding/rolling refit 量化静态模型时间衰减、模型版本归属、重训频率和冻结期；只有简单因果重训显示恢复空间后，才运行 DoubleAdapt/Proceed matched canary。F-014 regime 切片用于解释价值，不是立项许可；论文结果不替代本地证据。

### 9.8 概念板块与超图路线

概念关系频繁新增、同一股票同时属于多个概念。数据设计目标是记录 `concept_id`、`instrument`、`in_date`、`out_date`、source/version、采集/公告可用时点和变更原因；板块标识使用稳定 ID，开放区间和同日多成员关系尽量可复算。只有已经存在、版本化且 PIT 可证明的完整/部分/代理文件才能进入下列实验；缺文件时只登记候选数据规格与预期价值，任何新数据集构建须另立明确授权的 candidate-only 任务，不得在 QE 运行期访问数据库，也不得启动历史采集/回填平台或延迟 P0/P1：

1. HIST-concept，对比 HIST-industry，验证细粒度题材关系是否增加信号；
2. industry + concept 多关系 HATS/GAT，做逐关系和交互消融；
3. 概念超图或多头关系注意力，直接表示一股多概念，不复制样本、不把多个概念压成单码；
4. 概念层板块评分 → 概念内龙头选择的两层模型。

概念数据集未入库时统一标记 `DATA_ACQUISITION_IN_PROGRESS`，同时记录已取得的时间段、缺失成员变化、代理数据适用范围、可能泄漏和误差。`sina_board_daily` 或当前成分列表可以作为明确标注的代理/敏感性实验，但不能冒充完整 PIT 历史；代理结果与真实 PIT 结果未来并列互证，不阻止概念方向继续研究。

### 9.9 并行研究优先级与资源建议

截至 2026-07-26，R9S 24/24、R10 51/51、R11A 9/9、R11B 6/6、R12G 1/1 均已完成，R11 的 15 个 Loop 已 fully archived；R12P 原正式 run 的编排失败证据完整保留，append-only recovery 已完成 11 个研究可用 child，equal/orthogonality-aware 和八条 LOO 已形成首次完整比较，10 个原始结果缺口继续保留。后续分为“已完成基础研发”和“可并行科研方向”；基础研发用于提高任务可靠性和操作效率，不构成模型、因子或研究方向的许可门槛，科研仍可在 QE-only 范围按资源继续。

#### 9.9.1 已完成基础研发：多 Alpha P0-1～P0-4

截至 2026-07-22，P0-1 durable orchestration、P0-2 lifecycle/recovery、P0-3 规范 QE 创建器和 P0-4 child/attempt observability 已全部合入并完成生产 DDL、服务重启、真实 API/SSE/readback 与 CLI Playwright 运行态验证。实现 PR 为 #2464、#2509、#2580、RD-Agent #6 和 #2593；运行态 close-sync PR #2606 已合入。以下四项保留为架构事实和回归基线，不再列为待开发任务。

1. **P0-1 持久化父子任务编排与后端重启接管**：在现有 `multi_alpha_combine_backtest_run`、service、router、Prediction Store 和 `QEWorkspaceClient` 上增量实现 task/run/child/attempt/event 权威状态；状态 transition 与 event 同一 DB transaction，用 lease/fencing/CAS 防止重复派发，持久化远端 `qe_task_id/qe_loop_id`，后端重启后先核对远端状态再继续派发或汇总。节点容量统一读取现有 QE active execution 来源并按 remote identity 去重；schema/worker 不可用只影响 multi-alpha 写能力，FastAPI 和非 QE 模块继续运行。删除每个 run 一个 daemon thread 的生命周期所有权，但不重写组合、权重、LOO 和 pred-backtest 业务算法。
2. **P0-2 暂停/恢复/取消与子任务级恢复**：暂停只停止派发新子任务并允许当前子任务完成；恢复继续未完成子任务；取消通过既有 `QEWorkspaceClient.kill_loop` 终止在途子任务并保留成功结果；legacy `stop` 委托 cancel/kill，保持现有单 Alpha 停止语义，禁止改成 pause。为 baseline/scheme/LOO 提供 `backtest_only`、`results_only` 和 `rematerialize_and_backtest` 明确模式；缺少所选模式需要的资产时返回稳定错误，不静默切换为其他模式，也不整组覆盖已成功结果。
3. **P0-3 复用单 Alpha QE 自动演进页面的正式创建器**：`/quantevolver/evolution` 是规范入口，不建设独立视觉版本；抽取并复用现有任务列表、创建 Dialog、节点选择、状态 Badge、结果/轨迹/日志组件，旧多 Alpha URL 只做兼容映射。创建器覆盖当前后端完整 request：腿及 seed prediction、日期、scheme、normalize、walk-forward、baseline、TopK、资金量、持仓/调仓参数、节点、节点并行和 timeout；不伪造训练字段，不增加人工审批或研究准入步骤；以同 viewport screenshot/golden 证明单 Alpha 前后及多 Alpha 同区域未引入新视觉语言。
4. **P0-4 子任务运行网格、日志与恢复可见性**：在现有 QE 详情布局中展示每个 baseline/scheme/LOO 的 child key、attempt、节点、远端 task/loop、状态、阶段、耗时、heartbeat、错误、制品和可执行恢复动作；复用 `LogsPanel`、`LoopDetailPanel`、`EvolutionTrajectory` 与 `combine_ui_adapter.py`，DB event 为权威、workspace/远端日志为明细。通信不确定显示 `RECONCILING/REMOTE_STATE_UNKNOWN`，不得显示成功或直接失败。

详细设计和逐文件实施顺序见 `docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`。P0-1～P0-4 的完成标准是可靠性与 UI 生命周期能力，不是任何 Alpha 方向的 go/stop 条件。

#### 9.9.2 v6.6 科研架构与方向优先级

当前总体结构固定为：`冻结文件数据 -> DIRECT_ALPHA 专家 -> CONDITIONING_STATE/RELATION_PRIOR -> 条件化组合 -> PORTFOLIO_POLICY`。同一个因子或模型在一个 trial 中必须声明主角色，避免把风险状态硬塞成直接预测腿。

| 角色 | 负责内容 | 当前示例 | 不允许的替代解释 |
|---|---|---|---|
| `DIRECT_ALPHA` | 个股或板块的未来收益排序 | LGBM/LSTM/GAT prediction、压缩扩张、参与差、领导耗竭 | 单纯降低回撤不等于产生新 Alpha |
| `CONDITIONING_STATE` | regime、breadth、拥挤、流动性、残差协同 | breadth persistence、residual cohesion、flow-price divergence | 状态指标不能因短窗口高 IC 自动升级为独立腿 |
| `RELATION_PRIOR` | 关系传播、图正则、风险状态 | `l2_code_id` embedding、industry-bias、动态图 | 静态邻接不再直接冒充排序增量 |
| `PORTFOLIO_POLICY` | 权重、入场、减仓、退出、重入 | entry-gate、bounded de-risk、dynamic n-drop | 组合改善不反推基础预测 RankIC 已提高 |
| `NEGATIVE_CONTROL` | 方向、冗余和 regime 互证 | 资金流加速、领导持续性原方向、失败新增腿 | 负结果不伪装为缺失，也不外推终止整个方向 |

**v6.1 历史归因修正，v6.6 继续有效。**MA-E15 不是“breadth 已证伪”。CE4-B 比 CE3 多出的 `m_sector_volume_confirmed_breadth_5d_20d` 同时改变了 outer-union 观测总体；Qlib 后续 Fillna 又把缺失基线特征变成零。共同样本标签一致只能排除标签漂移，不能排除模型对额外零填充样本的排序响应。因此 CE3/CE4-B 的信号差异只能标记为 `CONFOUNDED_BY_PANEL`。同时，MA-E15 的九个 Loop 均使用 V25_1 分钟执行，开放 BUG-852 令其 10D 日频执行特征为零；这不使 Prediction Store 的标签、预测、IC/RankIC 和 Top-K 面板诊断失效，但使组合收益、风险和换手只能标记为 `EXECUTION_TRUTH_LIMITED_BY_BUG852`。MA-E16 使用 `qe_observation_panel_v1`，预先固定参考因子 `m_intraday_range_60d_min_ratio`、`m_turnover_acceleration`、`m_sector_mf_divergence_lg` 的非空交集；候选 breadth 可以在该固定索引内缺失并沿用既有 loader 填充，但不得新增观测行。为获得可解释的组合结果，MA-E16 全部改用既有日频 `CLOSE_PRICE` 执行，不等待或声称修复 BUG-852。

**v6.6 优先级纠偏。**v6.5 已把近期弱化认定为首要问题，却把直接识别该问题来源的滚动重训、四格 sector oracle、基准相对归因与 learned gating 放在横截面新专家之后。v6.6 不推翻任何候选公式，只把“先识别瓶颈、再选择算法”设为当前资源顺序。P0 只包含 MA-E16 收口与以下三项诊断；诊断是科研任务，不是人工审批、平台门禁或方向停止开关。

**P0-0：MA-E16 12-arm matched matrix 收口。**MA-E14 H95/C85 overlay 在 full/early/late 三个窗口均降低 CAGR，当前阈值扫描分支结束；regime、breadth、残差协同和资金背离继续作为 `CONDITIONING_STATE`，但不再搜索静态阈值。`m_intraday_range_60d_min_ratio` 的官方 h20 IC 在 full/out-sample/recent-6m 为 `0.05566/0.07385/0.05651`，h20 RankIC 为 `0.05652/0.08320/0.06919`，HAC companion 同方向。MA-E16 当前 10/12 仅说明 breadth 的组合增量具有模型条件性并伴随回撤代价；先恢复 Loop3 `backtest_only` 与 Loop7 `full_train`，完成三窗口、成本、prediction correlation、Top50 overlap 与 matched blend/LOO，成功 10 arm 不重训，重复 task 永不恢复。

**P0-D1：model age、重训可恢复成分与 calendar regime 可证伪诊断。**MA-E16 本地 LSTM Loop7～12 的真实配置均为 `train=2018-08-01～2022-12-31`、`valid=2023-01-01～2024-06-30`、`test=2024-07-01～2026-06-30`；这证明近期弱化尚未排除模型年龄解释，但不证明滚动重训必然有效。先以低成本 LGBM、再以被选中的 LSTM cadence，在相同冻结文件、因子、h20 标签、观测面板、策略、成本与测试窗口上比较固定切分、模型年龄切片、expanding 与 rolling refit；候选 cadence 在验证区预注册，预测日只使用当时已成熟标签并保持 h20 purge/embargo。20/60/120 交易日 cadence 只作为预注册候选，不得用测试窗口选择。结果只量化“可由新近成熟数据恢复的成分”与“相同年龄/不同 calendar regime 下的剩余退化”，不能凭单次 refit 唯一归因；若简单重训没有稳定恢复，再把 DoubleAdapt/Proceed 作为独立 matched canary，而不是直接建设在线学习平台。

**P0-D2：四格 sector oracle 与现实两层模型。**将 F-018 从 P2 提升为 P0 诊断：同一候选池、Top-K、风险、成本和可交易约束下比较 reality/oracle sector × reality/oracle stock、hard Top-M 与 soft gating，并并列保存板块 Recall、板块内排序、右尾板块捕获与组合结果。申万 L2 是主 taxonomy；只有冻结文件含可验证的 PIT L1 映射时才增加 L1 敏感性，否则记 `NOT_COMPUTABLE`，禁止数据库补齐。oracle 永久标记 `QE_ONLY_FUTURE_INFORMATION_CEILING`；高上界只定位投入方向，不冒充现实收益，低上界也只约束当前 taxonomy/horizon/Top-M/Top-K trial。

**P0-D3：benchmark-relative 与 Brinson 归因。**MA-E16 配置虽定义 `000300.SH` alias，实际 `backtest.benchmark=null`，因此既有 CAGR/Sharpe/MDD/Calmar 是 absolute-return 口径。v6.6 保留全部绝对指标，并为后续候选预注册与投资域相容的冻结 benchmark identity，报告主动收益、beta、tracking error、information ratio，以及板块 allocation、板块内 selection 与 interaction；同时提供冻结可投资池等权对照，避免单一市值指数错配。benchmark 或逐日 PIT 板块权重不在冻结文件时明确 `NOT_COMPUTABLE`，不得访问数据库、在线补齐或用当前权重回填历史。

**P1-A：因果重训、适应与条件门控。**只有 P0-D1 显示存在跨模型/窗口稳定的“新近成熟数据可恢复成分”后，才从简单 rolling/expanding baseline 递进到 DoubleAdapt/Proceed；只有 matched 固定权重/LOO 与状态切片证明专家存在条件互补后，才研究 TRA/MIGA 类 learned routing。第一版 gate 可作为独立 OOF meta-model：输入仅含训练/验证期 out-of-fold 腿预测和冻结 regime/sector state，输出有界动态权重；测试标签、测试期表现和未来状态不得参与拟合。当前底座“未原生提供 learned gate”记录为实现缺口，但独立 canary 能验证假设前不重写 combine 平台、不要求联合训练。

**P1-B：两层板块、跨板块 lead-lag 与动态关系。**若 P0-D2 显示 sector/oracle 上界显著，优先建立板块连续分数 → 板块内选股的现实 soft top-down，对照一层模型和简单板块动量。`sector_residual_cohesion_break_v1` 从成员相对板块的残差协同水平、斜率和破裂速度形成 `CONDITIONING_STATE`；`dynamic_residual_flow_relation_v1` 只从冻结文件构造 rolling residual co-movement、flow/state、leadership 与 cross-sector lead-lag，作为 `RELATION_PRIOR` 逐关系消融。`pit_fundamental_diffusion_v1` 仅在冻结 H5/Parquet 含可验证 as-of/PIT manifest 时检验营收/利润加速度的板块扩散 breadth，并控制规模暴露；缺文件即 fail closed。图/关系输出可进入未来 5/10/20 日回撤 hazard，不把静态邻接直接融合成 Alpha 分数。

**P1-C：右尾目标、meta-labeling 与动态退出。**现有 Top50 对未来收益 Top1% 的捕获接近随机基线，说明继续只优化全截面 RankIC 不能自动解决长期主升浪漏捕。先比较方向已校验的 h40/h60 LambdaMART/LTR、NDCG@tail、Top-decile/分位目标与收益阈值/time-to-hit/先发生回撤 hazard；未成熟尾部保持 censored，不补零，主 h20 裸收益标签保持独立身份。Meta-labeling 只使用严格 OOF 主模型候选和决策时已可见的 state/score/cost/position-path 特征学习 take/skip/size/continue-hold；未来 MFE/MAE、time-to-hit 和 barrier outcome 只能构造标签、样本权重或评价，不能作为当时输入。它可以改善 precision、仓位和退出，但不能把主模型未召回的股票凭空加入候选，因此不得冒充右尾召回模型。

**P1-D：横截面新 Alpha 专家并行队列。**现有 `Industry_Turnover_SmallBuy_Flow_Ratio_Factor` 的 h20 IC 在 full/out-sample/recent-6m 为 `0.03453/0.06024/0.08714`，但原始 volume/amount 存在单位、规模与分母风险。`sector_participation_gap_v2` 保留为 `DIRECT_ALPHA`：逐 PIT 行业汇总 `large_net_amt=(large+extra_large buy amt)-(large+extra_large sell amt)`、`small_net_amt=small buy amt-small sell amt` 与总成交额；总成交额非正或输入缺失时该行业日为 missing，禁止填零。`gap_t=CSRank(large_net_amt/amount)-CSRank(small_net_amt/amount)`，最终信号为 `mean_5(gap)-mean_20(gap)`，方向预注册为 gap 加速为正。`m_stock_sector_leadership_persistence_20d_10d` 的 full/out-sample h20 IC 为 `-0.04969/-0.04285` 且近期方向混合；`leadership_exhaustion_v1` 继续使用“领导持续性 × breadth 减速 × 资金背离恶化”耗竭方向，原始方向另立 `NEGATIVE_CONTROL`，若用于退出则另立 `leadership_exhaustion_state_v1`。两者保持 matched control、公式/数据 hash 与三种子证据，但由 v6.5 的 P0 横截面候选改为不阻塞 P0 诊断的 P1，并不得用测试结果改方向。

**P1-E：模型分歧、预测正交与条件组合。**MA-E15 中 CE3-LGBM 与 CE4-B LSTM 的日截面 rank correlation 约 `0.68～0.73`、Top50 overlap 约 `7.6%～10%`；同一 CE4-B 的 LGBM/LSTM rank correlation 约 `0.67～0.72`、Top50 overlap 约 `3.8%～5.2%`，LSTM 三种子相关约 `0.969～0.991`，但这些数值仍受面板差异影响。MA-E16 收口后先做同面板固定权重 blend 与任务级 LOO，再研究动态分歧权重。预测正交专家继续使用裸前向收益主标签，只在严格 OOF 预测上增加相关惩罚或 residual head，主/辅助结果分开报告。低 correlation、低 Top50 overlap、模型名或 horizon 都不自动授予独立腿身份；神经网络初筛保留 seeds `123/314/2718`，只有进入最终候选且种子区间仍宽时才扩展到至少 5 个预注册 seed 或 bootstrap ensemble，不为所有探索默认扩大算力。

**P2 条件方向。**HIST-concept、HATS、超图、公告/新闻事件、供应链/股东网络、跨资产敏感度、R8M、MASTER/IGMTF 与 CausalStock 类关系发现继续开放，但只消费已经存在、版本化且 PIT 可证明的文件资产。缺文件时记录为未来数据候选，不启动前置平台、数据库回填、历史采集/补算或 UI 工作；MASTER、CausalStock 或论文基准上的成功不外推为 AIstock A 股增量。

**继续终止。**历史 Loop 总账补齐、Archive 补录、旧制品物化、F-014 Phase 5、通用 UI/schema/provenance 完整化及没有明确增量假设的 TCN/Transformer/更多腿扫描，不重新进入活动 backlog。

#### 9.9.3 策略优先执行合同（v6.6）

1. 当前全窗口 Pareto 锚点为 MA-E07 四腿 soft de-risk 与 MA-E10 drop-TCN 三腿；近期 regime 锚点为 MA-E13R。R12P 继续是历史组合基线，不再是唯一活动比较入口。
2. 每个候选并列记录 full（2024-07-01～2026-06-29）、early（2024-07-01～2025-06-30）和 late（2025-07-01～2026-06-29）窗口。窗口差异用于解释适用范围，不形成研究准入门禁；不得只用全窗口掩盖近期失效。
3. 训练型候选初筛默认使用 seeds `123/314/2718` 的 matched-seed 证据。复用 prediction 的策略候选可以先运行低成本单次真实回测，但不得把单次策略改善外推成模型稳定性结论；进入最终策略包的神经网络候选若三种子区间仍宽，必须预注册至少 5 个 seed 或等价 bootstrap ensemble 后再形成稳定性结论，传统模型和已被证伪的探索不机械扩种子。
4. 新信号按“独立因子/专家表现、同模型 matched change、独立腿表现、组合 LOO/相关性”顺序自然记录。该顺序是归因方法，不是 PASS/KILL gate；任一阶段的弱结果都按当前公式、窗口和角色保存。
5. 主预测标签保持裸前向收益。辅助 right-tail、hazard 或 residual head 必须另立身份和指标，不覆盖主标签、不利用测试窗口选择方向。
6. 所有训练、预测、回测、composer 和 worker 数据输入只来自钉住的 H5/Parquet/bin/sidecar；数据库只保留控制面和结果面。缺文件、hash 漂移、PIT 覆盖不足或运行期 DB 访问一律 fail closed。
7. 每轮自然保存版本化 candidate、冻结输入/预测身份、真实 run identity、收益、回撤、换手、成本、Top-K、退出/重入和失败原因；不得扩张为历史 Archive、全量证据补齐或平台完整化项目。
8. WSL 优先承担 `gpu_serial_graph=1` 的图/关系训练及已验证的 GPU 模型；远端节点优先承担 LGBM、组合与窗口回测。MA-E16 的单 task 内部把 `rdagent-node1` LGBM 设为 `node_parallelism=3`、`wsl2-5080` LSTM 设为 1，但当时合入前运行态的跨 task WSL 全局硬容量仍为 2，两个 task 因而各取得一个本地槽并发生 Loop7 重叠。BUG-1014 生效后的恢复和未来任务才使用全局 WSL 硬容量 1、本地 GeneralPTNN 单进程 DataLoader、无 pin/prefetch worker 复制和最多 4 个 BLAS/OpenMP 线程；远端硬上限保持 4，两节点仍可并行。该合同是主机安全边界，不是人工研究审批；工作目录、recorder、cache 与内存必须隔离，不得为了占满算力越过资源合同。
9. 不新增通用 UI、schema、Archive writer、历史解析器或历史物化任务。只有真实 trial 暴露、可复现且直接阻止产生结果、又无安全替代路径的 BUG 才能暂停实验；修复后恢复原 candidate。
10. 后端启动、停止、重启仍归用户所有。后端重启不应终止已启动的外部 QE worker；本蓝图及任何实验授权都不产生进程控制权限。
11. matched 因子实验必须在 `model_params` 显式声明 `qe_observation_panel_v1`、稳定 `panel_id`、`reference_factor_intersection` 和参考因子列表，并使用 `disable_alpha158=true`。composer 必须把 `observation_panel` 解释为生成期 data-loader contract 并从策略 kwargs 中排除；public in-memory compose、CE3/CE4 两种因子数和缺失结果路径必须走同一合同。所有 trial 的参考索引定义必须相同；缺配置因子、缺参考列、空交集或声明拼写错误均 fail closed。未声明该契约的历史 task 不改写、不回填，并继续允许已成功因子的历史 partial concat。
12. MA-E16 的 12 个 Loop 统一使用 `execution_algo=CLOSE_PRICE`、`runtime_mode=day`、`backtest_freq=day`、Top50/n_drop1 和同一成本设置；禁止混入 V25/V25_1 或分钟执行，以免开放 BUG-852 污染 matched Alpha 的组合归因。BUG-852 保持独立开放，不因本轮绕行而关闭。
13. 下一执行顺序固定为：对仍未收口的 BUG-1014/1022/1023/1024 逐项补 post-restart receipt/close-sync；这四项 source 已由当前 backend/RD 运行链加载，收尾不得扩张为新研发，也不得串行阻塞已经具备条件的策略恢复 -> Loop3 显式 `retry_mode=backtest_only`，复用完整 params 重新执行预测/回测且禁止 `auto` 回退训练 -> Loop7 显式 `retry_mode=full_train` 单 arm 重跑，禁止复用 SIGTERM 中间参数 -> 分析完整 12-arm 同面板 CE3/CE4-B 双模型三种子及 full/early/late、换手、成本、prediction rank correlation、Top50 overlap，并在存在互补证据时执行固定权重 blend + LOO -> 预注册并执行 P0-D1 model-age/refit、P0-D2 四格 sector oracle、P0-D3 benchmark/Brinson 三联诊断；三项可在资源合同内并行，但不得共用测试标签调参 -> 根据结果选择 P1-A 适应/门控、P1-B 两层板块/lead-lag、P1-C 右尾/退出或 P1-D/E 横截面/正交组合分支。`sector_participation_gap_v2` 与 `leadership_exhaustion_v1` 可在不延迟 P0 诊断时以 matched LGBM trial 并行运行，不再是唯一 P0。BUG-1026/1030/1031 已关闭，不得重新列为实验前置；重复 task 永不恢复，成功 10 arm 不重训；P2 不延迟 P0/P1；BUG-852 只在未来需要分钟执行真实性时独立处理，不阻断日频归因。
14. 运行态静默验收只验证本轮已修阻断：无 UI/无订阅者时，上游 QE 日志连接、文件写入和数据库工作均为零；有活动 task 时，节点健康/派发观察器按变化写状态，正常探测最多按设计的 60 秒节流，心跳不落数据库；五个环形日志槽总上限为 5 GiB。该验收不是新监控平台、历史归档或实验准入阶段。
15. 旧 `evolution.log` 保持只读且本轮不删除。仅在相关源码全部合入、用户部署/重启、identity/business smoke 通过、零订阅者、精确路径与 size/mtime 未变化后，才可由用户另行授权删除唯一候选文件；不得通配、递归或顺带清理其他日志。
16. P0 三联诊断共享同一冻结数据 identity、投资域、费用、可交易约束、full/early/late 窗口和预注册 test；滚动重训的模型 vintage、可用标签截止、训练窗口与 cadence，oracle 的 reality/oracle 身份，benchmark 的指数/等权构造和板块权重 hash 都随 trial 保存。任一输入不可证明即 `NOT_COMPUTABLE`，不得静默改口径。
17. DoubleAdapt、Proceed、TRA、MIGA、MASTER、meta-labeling、LambdaRankIC/LTR 和 CausalStock 只形成机制先验。实现顺序必须由三联诊断触发，并先运行最简单 matched baseline；论文指标、海外/其他指数结果或单篇最新预印本不能替代 AIstock 的 PIT、成本后、样本外和多种子证据。
18. 当前 combine 层的冻结 prediction 仍可通过独立 OOF meta-model 验证 learned gating，无需先联合训练或重写底座。只有 canary 在相同总风险、成本、窗口和策略级选择偏差校正后稳定优于固定权重，才评估最小通用化实现；否则保留为负结果，不新增平台工作。

#### 9.9.4 v6.6 DESIGN-COMPLIANCE-001 文档审核

- `no_simplified_delivery`：当前总账仍逐项覆盖 MA-E01～MA-E16；MA-E11 无独立 task/run、MA-E16 10 completed/2 failed、12-arm 明细和两个失败原因均不改写。v6.6 明确把 staleness、sector oracle、benchmark/Brinson、适应/门控、右尾/meta-labeling 分别列为尚未运行的设计，不把论文综述、配置审计、四个 matched pair 或已有绝对收益冒充新实验完成。
- `no_silent_error`：Loop3 空指标竞态、Loop7 SIGTERM、面板额外 `1,511` 行、V25_1 限制、资源换页、重复 task、空转 DB/SSE 与日志放大继续保留。新增诊断把“staleness vs regime”“absolute vs active return”“sector selection vs within-sector selection”分栏；缺 benchmark、PIT 权重、matured label、OOF prediction 或 taxonomy 时显式 `NOT_COMPUTABLE`，禁止 DB 回退和口径替换。
- `no_business_semantic_drift`：裸 h20 主标签、batch 4096、既有模型结构、QE-only 文件数据面、Top50/n_drop1、成本和成功 10 arm 均不改写；benchmark-relative/Brinson 是 companion 评价，不覆盖绝对收益；right-tail/meta-label/gate 均另立身份，不能利用测试窗口选择方向或改变既有 run 解释。Loop3 仍强制 `backtest_only`，Loop7 仍强制 `full_train` 单 arm，重复 task 不恢复。
- `no_unrequested_gate_or_approval`：P0 三联诊断是最高信息增益的资源顺序，不是人工审批、promotion gate 或方向停止规则；论文方法是否进入 canary 由可证伪结果触发，不恢复 P0-1～P0-4、F-014、UI、Archive、BUG-852、历史完整度或平台建设门禁，也不产生进程控制、DDL/DML、依赖安装或生产激活权限。

#### 9.9.5 v5.24 科研方向快照（历史，不代表当前排期）

以下条目保留 v5.24 当时措辞；其中“当前”“只在 MA-E01 期间”和“尚未终态”均只指 2026-07-31 快照，已由第 2.5、9.9.2～9.9.4 节替代。

1. **P0：MA-E01 Sector-Conditioned Multi-Alpha Strategy Package v1（当时唯一主动主线）**：复用现有 LGBM/LSTM/TCN/EfficientGATs prediction 和 R12P 四腿 equal 基线，不重训主模型；实现板块状态条件化权重、有界减仓、退出与重入。必须提交真实 QE trial，对照 LGBM baseline、equal 与 orthogonality-aware，输出 CAGR、Sharpe、Calmar、最大回撤、换手、成本、false early-exit、重入延迟和趋势捕获。该条仅记录 v5.24 当时排期。
2. **P1：MA-E02 动态关系与 sector gating**：只允许在 MA-E01 已提交且真实实验处于运行/等待期间并行准备；复用 R11B `l2 embedding only` 的 h40/h60 基线，研究 dynamic residual co-movement、flow/state、leadership 多关系与未来 5/10/20 日回撤 hazard。若会延迟下一次 MA-E01 候选提交，则立即让位于策略主线。
3. **REFERENCE：EfficientGATs 单 Loop 执行行为 canary（已完成）**：`qe_20260718_040323_9a4a` 复用 R11B h40 seed123 的模型、因子、标签、数据与策略，显式启用 attention query chunk 和 cooperative yield，保持 `gpu_serial_graph=1`、`resource_telemetry_enabled=false`；1/1 完成，RankIC `0.10569`、CAGR `62.12%`、Sharpe `1.8108`。该结果只证明现有执行方式可完成训练和回测，不是新的策略演进结果。
4. **FROZEN：F-014 平台/UI/历史完整化**：Phase 1–3、既有 R8B 6/6 CAS、Loop4 2,004 metric + 6 artifact、API/MCP 与已完成 UI 证据只读保留。用户已终止当前 3000 visual、normal Loop 平台 smoke、其余 5 个 R8B、Phase 5、历史补算/物化与 Archive 补账；这些事项不得重新进入主动优先级，也不得阻断 MA-E01。
5. **PARALLEL-ONLY：跨任务 portfolio fusion 与任务级 LOO（R12P 当前 trial 已完成）**：最终 successor 已恢复 baseline、equal、orthogonality-aware 和对应八条 LOO；equal 当前优于 baseline 与 orthogonality-aware，LGBM 是最稳定贡献腿，GAT/LSTM/TCN 的边际随 scheme 变化。10 个 ic-weighted/risk-parity child 仅保留历史证据缺口；只有在 MA-E01 运行期间且不延迟下一 candidate 时，才可另立新 trial。
6. **PARALLEL-ONLY：右尾信号、四格 sector oracle 与现实两层模型**：围绕半导体、通信、软件、IT 服务等漏捕板块，比较 reality/oracle 板块层 × reality/oracle 个股层、hard Top-M 与 soft gating；只在 MA-E01 运行期间使用空闲资源，不得成为其前置。
7. **PARALLEL-ONLY：LSTM/TCN Type-B 继续演进**：LSTM R8B2 与 TCN 多种子矩阵保留为后续候选；只在 MA-E01 运行期间使用空闲资源。LSTM/TCN 按 `gpu_parallel_standard` 最多 2 Loop，不能与 `gpu_serial_graph` 图训练同时提高并发。
8. **TERMINATED：历史 Loop 总账补齐与制品固化**：既有事实和制品只读保留，不执行历史 Archive 补录、状态重算、旧 Loop 物化、证据补齐或总账固化。新策略 trial 仍须随运行自然保存比较所需的最小配置、输入身份、指标、成本、风险和失败原因；该最小输出不构成历史工程。
9. **PARALLEL-ONLY：R8M 与迁移对照**：保留独立/共享/迁移候选设计；只在 MA-E01 运行期间使用空闲资源。
10. **PARALLEL-ONLY：LTR/排序目标与正交数据**：保留 score/group/label 诊断和正交数据候选；不在 MA-E01 提交前启动。
11. **PARALLEL-ONLY：HIST/HATS/MASTER/IGMTF/TRA/概念多关系**：方向保持开放，但仅在策略实验运行期间使用空闲资源，不先建设数据或关系平台。
12. **PARALLEL-ONLY：ALSTM/GRU2/Transformer 弱/失败配置复核**：只读保存现有证据；只有 MA-E01 运行期间有空闲资源且具备明确策略假设时才可另立新卡。

低成本、复用预测的实验先于新架构训练；同一研究阶段不得为了等待一个节点而擅自提升图模型并行度。

- GATs/HIST/大截面关系模型归为 `gpu_serial_graph`，默认 1-parallel；资源 canary 只决定是否提高并发，不决定该研究是否允许执行，未提高并发时继续串行运行。
- LSTM/TCN 等已验证可并行的模型归为 `gpu_parallel_standard`，并行上限由调度器按模型类判断；回测可与下一 loop 训练重叠，但必须隔离 recorder、工作目录和 GPU/CPU/内存配额。
- 远端 CPU 可并行执行 LGBM/融合回测；共享 factor cache 原子写、每因子锁、损坏检测/重建和 MLflow recorder 隔离决定安全并发数。并发证据不足时降为串行或较低并发继续实验，不把基础架构缺口变成研究阻断。
- 后端重启不应终止已启动的外部 QE worker；新增调度逻辑必须继续满足任务状态可恢复、运行进程不被重复启动、结果只归档一次的契约。

#### 9.9.6 策略优先执行合同（v5.24 历史快照）

1. 当前比较锚点固定为 R12P：LGBM baseline 的 CAGR/Sharpe/Calmar 为 `53.57%/1.602/2.649`，四腿 equal 为 `74.47%/1.922/3.554`，orthogonality-aware 为 `72.12%/1.893/3.302`。MA-E01 必须在同口径真实 QE run 中比较，不能只报告离线函数或局部样例。
2. 每轮迭代必须形成版本化 strategy candidate、冻结输入/预测身份、真实 run identity，以及收益、回撤、换手、成本、退出/重入和失败原因。运行自然产生的这些最小输出属于策略比较结果，不得扩张为历史 Archive、全量证据补齐或平台完整化项目。
3. 不新增通用 UI、schema、Archive writer、历史解析器或历史物化任务。只有 MA-E01 真实提交或运行暴露出的、可复现且直接阻止策略产生结果的 BUG，才允许暂停策略主线修复；修复后立即恢复同一 candidate。
4. 动态关系、PIT 数据、诊断和其他未来有价值的工作，只能在真实实验已经提交并处于运行/排队期间利用空闲资源并行推进；不得把“准备更完整”设为下一 candidate 的前置条件。
5. MA-E01 源码已在既有 `afe5634d` 中合入；当时已提交四个正式 policy run 且尚未终态。该状态已由 v6.0 终态总账替代；本条只保留“queued/running、代码或测试均不等于验证完成”的历史原则。

#### 9.9.7 v5.24 DESIGN-COMPLIANCE-001 文档审核（历史）

- `no_simplified_delivery`：蓝图只把 MA-E01 标为设计就绪、源码/真实 run 待执行，没有以文档、测试、平台或旧结果冒充新策略完成。
- `no_silent_error`：R12P 的 10 个未恢复 child、F-014 指标限制和 provenance 缺口继续显式保留；终止历史工程不改写任何缺失为成功。
- `no_business_semantic_drift`：R12P baseline/equal/orthogonality-aware 数值、四腿来源、QE-only 边界和现有组合公式不变；MA-E01 另立版本化 candidate 做真实对照。
- `no_unrequested_gate_or_approval`：P0-1～P0-4 和 F-014 完整度不再是前置；唯一允许暂停主线的是实际 trial 的可复现阻断 BUG，未新增人工审批或 research-ready 状态。

## 10. 风险与控制

1. **重复因子风险**：F1/F4 已有大量同族或精确重复。通过公式级去重和双层相关性阻止换名复制。
2. **1d/h20 错配**：现有快筛和 MCP 主摘要偏 1d。1d 与 h20 分栏解释；h20 暂缺时保留 1d 证据、缺口和补算计划，不输出 PASS/FAIL。
3. **PIT 行业切换污染**：板块面板口径与其他代理口径并列保存；逐日 PIT 结果作为主解释，代理结果用于测量污染和损失。
4. **大板块权重偏置**：同时报告等权板块层结果，不能只用股票映射层 IC。
5. **F3 低成功率**：N1 只作 negative control，禁止因“资金流叙事合理”跳过快筛。
6. **多重检验**：窗口、符号和公式在最终 test 前冻结；同族变体按 family 管理。
7. **成员样本不足**：成员数 `< 5` 或覆盖率 `< 0.8` 的板块日单独分层并与全样本并列，量化纳入/排除造成的损失。
8. **低波/规模暴露**：A4/A5/A6 报告 SIZE、VOL 与行业成员数暴露；缺失项进入后续补算。
9. **离线/实时漂移**：双代码 parity、loader、转换提示、MCP 和 active 状态分别记录；本蓝图只消费 QE 侧证据。
10. **运行状态混淆**：代码合并、candidate 数据、QE 实验和非 QE 运行状态分别报告。
11. **重叠标签虚高**：h20 普通标准误、lag=19 HAC、block/non-overlap sensitivity 并列报告；暂缺项注明不确定性。
12. **回测选择偏差**：trial 台账、多重检验、DSR/PBO、全部种子和最佳种子分别展示；不完整记录触发补证而不是方向淘汰。
13. **成本、容量与拥挤**：毛/净收益、不同资金规模、压力期持仓和冲击重合共同解释当前 trial 的可实现性与损失。
14. **非 QE 副作用**：唯一硬边界要求所有研究动作只在 QE；非 QE DDL、DB、promotion、重启和实时启用不属于本蓝图。
15. **关系身份混淆**：embedding、二值邻接、HIST 概念聚合和动态权重图分别归档并逐层消融，不把任一结果外推到其他关系机制。
16. **静态关系未来函数**：当前行业/概念成员快照回填历史会系统性泄漏；所有关系模型只接受逐日 PIT 关系和可复核 mapping hash。
17. **h20 目标错配**：只优化 h20 RankIC 可能继续偏向短周期反转或较早止盈；将第 9.6 节的 60–180 日、右尾、存活和捕获率作为并列证据，解释长期趋势能力与损失。
18. **融合伪增量**：固定与非固定总风险、不同成本假设、leave-one-leg-out、暴露和换手重合并列报告，区分真实增量和预算变化。
19. **概念多重成员膨胀**：复制一股多概念样本会改变权重和统计量；未来概念模型使用稀疏多热关系/超边，并在聚合后还原到唯一股票决策行。
20. **并行制品竞争**：共享 factor cache 或 recorder 的非原子写可能产生损坏或错读；锁、临时文件原子替换、制品 hash 和失败后的定向重建结果随并行实验记录，不因单次异常停止研究方向。
21. **F-014 平台口径分裂**：纯计算核、CAS、DB、API/MCP/UI 分别记录版本、输入身份和结果 hash；平台尚未集成时，可计算结果仍用于科研分析，并显式标注存储/展示状态，后续按同一 identity 对齐。
22. **日线触板等同不可成交**：仅凭 high/low 触及涨跌停推断订单结果会夸大或低估捕获；trade artifact 为最高权威，无订单/队列证据时保守标记 `NOT_VERIFIABLE`，入场和退出阻断对称报告。
23. **oracle 事后解释**：同时保存运行前假设和运行后解释；四格、soft gate、成本/可交易约束、连续增量和置信区间共同展示，不输出方向级停止结论。
24. **多期限负迁移**：共享表示不保证优于独立训练；per-head maturity/purge、transfer matrix、LOO 和梯度冲突诊断用于定位正负迁移，修正算法作为独立 trial 记录。
25. **期限即独立腿的错误外推**：h20/h60 或 LGBM/LSTM 可能仍消费相同经济信息；用入场、持仓、板块、P&L、右尾事件、成本和固定风险预算 LOO 判定增量，不按模型名或 horizon 自动授予腿身份。
26. **全窗口掩盖近期失效**：全窗口高 CAGR 可能集中来自早期 regime；full/early/late 并列报告，近期弱点不得被全期均值覆盖，也不得用近期窗口事后重选全期公式。
27. **状态变量冒充直接 Alpha**：breadth、residual cohesion、industry-bias 或 hazard 改善回撤不等于提高收益排序；必须按第 9.9.2 节角色分层归因。
28. **参与差原始比值失真**：行业成交额与个股/板块小单 volume 的单位、规模和零分母可制造伪信号；新公式使用单位一致 share/rank、明确聚合层级并对异常 fail closed。
29. **新关系数据面回退数据库**：HIST/动态图/概念关系缺文件时访问 `market.*` 会破坏可复现与节点一致性；DB-poison、静态扫描和 dataset identity/hash 随新 trial 验证。
30. **新增腿稀释组合**：因子单项 IC、低预测相关或新模型名都不证明组合增量；MA-E12 作为失败对照，新候选保存独立腿、matched change 与固定风险预算 LOO。
31. **空闲控制面数据库风暴**：无活动 task 或无状态变化时重复 claim、mirror、health/readback 会放大 PostgreSQL、WMI 与主机负载；后台观察器必须单实例、按变化写入，并把非紧急远端探测节流到至少 60 秒，正常心跳只保留在内存。
32. **日志消费者扇出与隐藏页面空转**：多个 UI/API 消费者各自连接 RD-Agent、断线从零重放或页面隐藏后继续轮询会重复网络、CPU、内存与文件 I/O；每 task/node 只允许一个上游 broker，cursor 必须可续传，最后订阅者离开后立即停止上游工作。
33. **日志文件无界增长与误清理**：重复写入单个 `evolution.log` 可耗尽磁盘，而通配清理会破坏运行证据；新日志只写五个 1 GiB 环形槽，旧文件保持只读，任何删除必须在运行态验证后由用户对精确文件另行授权。
34. **model staleness 冒充 regime drift**：固定训练截止后跨两年测试会把模型年龄、特征漂移和真实市场状态变化混在一起；以同数据/特征/标签/策略/成本的模型年龄 × calendar regime 切片和因果 rolling/expanding refit 量化可恢复成分，预测时仅使用已成熟标签，cadence 不在测试集选择，单次恢复不冒充唯一因果识别。
35. **绝对收益与主动收益混淆**：牛市 beta 可抬高全窗口 CAGR，弱市中的正绝对收益也可能对应较强主动收益；绝对与 benchmark-relative 指标并列，benchmark identity、构造权重和板块映射钉住，Brinson 只在逐日 PIT 权重可证明时计算。
36. **learned gate 泄漏与共同漏捕**：用测试收益训练门控会制造动态权重伪增量；即使 gate 严格 OOF，若全部基础腿共同漏掉右尾股票也无法创造新 Alpha。先固定权重/LOO 和 oracle 定位，再用训练/验证期 OOF prediction 拟合有界 gate，测试期只推理。
37. **右尾标签稀疏、重叠与删失**：Top1%、barrier、MFE/MAE、time-to-hit 与 hazard 容易产生类不平衡、重叠事件和未成熟尾部；按 episode、purge/embargo、censored likelihood、precision/recall/calibration 和成本后收益共同报告，不补零、不只报 NDCG/RankIC。
38. **meta-labeling 冒充候选生成**：二级 take/skip/size 模型只能过滤或缩放主模型已提出的候选，不能提高主模型从未召回股票的 recall；候选生成、右尾召回、meta-label precision 和退出收益分别归因。
39. **taxonomy 单一与事后概念回填**：申万 L2 粒度可能放大分组噪声，概念成员又可能被供应商事后修订；先在已冻结、PIT 可证明的 L1/L2 之间做敏感性，概念/题材仅使用版本化有效区间文件，缺失不触发数据库或历史采集平台。
40. **策略层选择偏差未落地**：蓝图已要求 DSR/PBO，但 MA-E01～MA-E16 的全部政策/权重/窗口尝试尚未形成统一策略级 receipt；既有 trial 只消费目前可读台账并把缺失列为结论限制，禁止为此启动历史补账/物化。未来前瞻保存完整尝试族、失败/放弃配置、有效独立试验数和策略收益序列；配置数或独立子期不足以支持 PBO/CSCV 时显式 `NOT_COMPUTABLE`，改用预注册 walk-forward、blocked bootstrap/DSR 或适用等价诊断，不只对最终赢家计算。

## 11. 验收与交付物

### 11.1 历史 `Gate-0` 批次交付物

- 融合一手机构/论文实施推论、F-001–F-012 与 L0–L5 验证的 F2 规格；`Gate-0` 只作为历史批次名称；
- 隔离 candidate bundle、schema、指纹、行列/覆盖/unknown/freshness receipt，旧 candidate/active 不变；
- `quick_ic_screen.py` 的 horizon、冻结方向、split manifest、HAC 和 exact label 契约及单测；
- RD-Agent → AIstock 的 exact h20 companion contract、nullable additive migration/official writer/router/MCP 代码与定向测试；RD task/loop 非官方 writer 仍禁用；
- F4/R2 tracked repair source 的 PIT 板块面板修复、冲突 fail-fast 与单测；catalog 双代码同步和指标重算后置；
- 两仓独立 PR/验证证据，以及 merge、DDL、DB、promotion、QE、runtime 状态的分离报告。

### 11.2 后续 G0-D：数据与接口

- candidate `sector_data.h5` / `static_factors.parquet` 的 schema、指纹与 `l2_code_id` receipt；
- 生成器对 `l2_code_id` 的整数 dtype、`-1` unknown、source/semantic schema 和覆盖率定向测试；
- transformation/review 对 `l2_code_id` 的兼容性 receipt；
- 离线/实时代码 parity 结果；
- unknown、PIT 行业切换、最小成员数和板块字段一致性测试。

### 11.3 后续 G0-D：因子研发

- Batch A 与 Batch B 候选代码可按资源并行开发，分别保留公式、数据和实验身份；
- R1/R2 复用基线的 h20 重评估，不新增重复 catalog 项；
- N1 negative control 的快筛、损失分析和互证 receipt；负结果保留并形成新假设；
- 每个候选的实验卡、h20/1d 快筛、统一指标、双层相关性、分类与当前证据摘要；
- append-only trial ledger、候选族有效试验数、purge/embargo 记录、HAC/block 推断和 partial/residual IC receipt；
- 多资金规模/参与率成本容量曲线，以及持仓、换手、冲击与尾部拥挤审计。

### 11.4 后续 G0-D：因子库完整性

以下字段是历史候选完整性说明，不是 v6.0 平台待办。已有值用于解释，缺失值保持 `pending/MISSING` 并随候选报告，不触发数据库回填、非 QE parity、历史证据补齐或平台建设，也不影响文件数据面上的 QE 研究：

- `aistock_factor_catalog`：`is_available=true`，`asset_path` 指向实际可执行源码；
- `aistock_factor_metrics`：官方窗口齐全，并有明确 h20 companion fields；生产 DDL 与生产回填未执行前必须标记 pending；
- `qe_factor_classification`：至少一条有效分类；
- `qe_factor_correlations`：股票映射层和板块原生层的增量相关性 receipt；
- 空代码、占位实现或仅元数据记录必须标注真实状态；已有数据和其他实现仍可继续分析。

### 11.5 后续 G0-D：模型验证与状态报告

- GATs 2×2 消融和 LGBM 对照结果；
- Tier2/IC 审核结论与未满足项；
- 分别报告：文档/代码合并状态、candidate 数据状态、active promotion 状态、QE 实验状态、模拟盘/实时状态；
- 未完成 h20 指标、数据 promotion 或 train/serve mapping 统一时，不得宣称板块轮动能力已生产就绪。

### 11.6 post-R6 研究交付物

- 历史 GATs/LGBM prediction-fusion canary receipt：预测/标签逐行对齐、正交性、Top25 重合、预冻结等权 rank/zscore 与信号级 h20 结果已完成；
- R6 正式同数据/同 split/同 seeds 的 LGBM/GAT 5 因子集 × 3 seeds 共 30 个 Loop 已完成；任务、因子集聚合和失败归因见第 9.4.2 节；
- R7A `equal + rank` 与 R7B `equal + zscore` 正式组合回测均已成功执行但未超过 LGBM Sharpe/Calmar，状态为 `COMPLETED_CURRENT_TRIAL_BELOW_BASELINE`；它们继续作为 portfolio fusion、LOO、容量、风险预算和新权重研究的互证样本；
- reality/oracle 四格、soft-gating、真实板块评分、板块 Recall、板块内排序、集中度/轮动成本及一层模型对照可并行交付；
- R8A/R8B/R8C 实验卡：R8A LGBM 长周期标签扫描 `qe_20260715_101942_d873` 已 12/12 完成，R8B LSTM canary `qe_20260715_104922_001d` 已 6/6 完成；R8C 和更多腿可并行设计/执行，并持续吸收 F-014 新增指标族；
- R9S 两个 backtest-only task 已 24/24 完成，形成 h40/h60/h120 下 `n_drop=1/3` 的期限相关策略转换证据；
- R10 已完成 51/51：LGBM/TCN/LTR/LSTM 与 hold10/20/30 均为完整三种子证据；LSTM `G15-FPL+h60` 和 hold30 最终均值已写入第 9.6.5 节；
- R11A 已 9/9，hold20 平均 CAGR 略高于 hold10 但 Sharpe/回撤未同步改善，hold30 明显弱化；R11B 已 6/6，h40 在相近回撤下以约一半年化换手取得接近 h60 的收益。15 个 Loop 均 fully archived；
- GAT h20/h40/h60 已证明与 LGBM 存在排序差异和较浅回撤，但二值同行业边无稳定增量；下一交付转为 EfficientGATs cooperative-execution canary、板块时序动态图、回撤 hazard、sector gating 和动态退出，不重复静态边或简单 prediction average；
- 与 Advisory Phase 8 对齐的 Phase 1 计算能力已覆盖 20–180 日、MFE/MAE、有序目标、time-to-hit、删失调整的 stage survival、右删失、episode capture/false-exit，以及信号→成交/退出阻断分层；Phase 2 control/worker/CAS 与 Phase 3 单 canary 明细表/API/MCP 已闭合，PR #2875 Loop/Archive UI slice 已通过第三审、源码 CI、合入及用户重启后的后端 API 运行态回读；
- F-014 详细设计：父设计已更新至 v1.22，Phase 2 从属设计保持有效；当前为 `EXISTING_CAPABILITY_FROZEN_STRATEGY_REUSE_ONLY`，其余 5 个 R8B、normal Loop 平台 smoke、当前 3000 runtime visual、历史物化与 Phase 5 均由用户终止，不再列为演进前置；
- R8M 独立设计卡：独立训练、共享多头、冻结迁移、全量微调四臂；per-head maturity/purge、transfer matrix、LOO、梯度冲突和 F-014 各指标族并列分析；
- HIST-industry 的逐日 PIT relation artifact、mapping hash、`stock_index` 对齐测试、composer/fit/predict canary 与资源 receipt；
- 动态/多关系图的逐关系消融；概念 PIT 数据获取、代理/部分样本、缺失损失和模型研究并行推进；
- 每个方向独立的实验卡、当前 trial 归因、资源类、并行策略和归档状态；单次最好 loop 只作样本，不外推为方向结论。

## 12. Design Acceptance Index / Research Evidence Index（不构成研究门禁）

下列条目保留稳定 ID，用于关联实现、测试和结果，不是科研准入或淘汰门禁；“代码存在”“指标可算”“平台完整”和“非 QE 运行”继续作为不同事实记录。

| ID | 设计要求 | 验收口径 |
|---|---|---|
| F-001 | 研究治理与试验台账 | 研究来源可追溯；每次公式/窗口/方向/阈值/种子/切分及失败版本有唯一 `trial_id`；候选族多重检验、purge/embargo 与最终 test 一次性开启规则明确。 |
| F-002 | candidate bundle 离散行业键 | `l2_code_id` 连接缺失为 `-1`，保留有符号整数 dtype，schema 为 `sector_data_raw/categorical_id`，生成覆盖率 receipt，且不覆盖 active/旧 candidate。 |
| F-003 | 通用 horizon 快筛 | `quick_ic_screen.py --horizon N` 的标签目标为 `close[t+N+1]/close[t+1]-1`；默认 1d 向后兼容；h20 提供 lag=19 HAC companion 指标；差异、缺失和 split 限制随结果报告。 |
| F-004 | RD-Agent h20 统一指标 | 保留既有 1d 与 legacy `rank_ic_20d`；同一指标记录增加 `h20_return_horizon=T21T1`、IC/RankIC mean/std、naive/HAC ICIR、raw Pearson positive ratio/n_obs 与 lag=19 共 12 个 nullable 字段，API 可序列化。 |
| F-005 | AIstock h20 持久化与查询契约 | additive schema/upsert/router/MCP 暴露 F-004 字段；旧记录/旧客户端兼容；DDL 和回填状态独立记录，不影响已有 QE 研究。 |
| F-006 | F4/R2 PIT 安全 repair source | tracked regeneration source 中的 `sw2_close` 先按 `(datetime,l2_code_id)` 构造唯一板块面板，再按板块时序计算；冲突 fail-fast，旧指标明确失效且后续需双代码同步/h20 重算，不新建近义因子。 |
| F-007 | 因子代码双形态与失败策略 | offline `code_text` 与 loader-only `realtime_code_text` 数值 parity；缺字段、重复板块值、unknown 或行业切换不静默回退。 |
| F-008 | 谱系、相关性与条件增量 | MCP 定向搜索、股票映射层/板块原生层相关性、partial/residual IC 和既有 R1/R2 代码审计均留证；无增量或高相关仅标注当前 trial 的谱系与证据。 |
| F-009 | 稳健性、成本容量与拥挤 | h20 HAC/block 推断、多重检验、DSR/PBO、真实 A 股约束下成本/容量曲线及持仓/尾部拥挤审计齐全。 |
| F-010 | QE 组合增量 | GATs 2×2、LGBM 对照、equal-sector/stock-mapped、多种子 OOS 增量；A5/A6 与其他 STATE 交互 trial 分别记录。 |
| F-011 | 零隐式生产副作用 | 本批不写生产 DB、不应用生产 DDL、不 promotion active、不重启服务、不启动 QE/模拟盘/实时交易。 |
| F-012 | 验证与状态分离 | 定向单测、F2 设计校验、diff 检查和 receipt 通过；合并、candidate、DDL、promotion、实验、运行时状态分别报告。 |
| F-013 | 组合与两层决策增量 | 同口径 GATs+LGBM prediction/portfolio fusion 和板块→个股两层基线；冻结权重/风险预算，报告正交性、成本、容量、暴露与 leave-one-leg-out；R7 结论只覆盖已运行的简单 prediction-fusion，不外推否定其他组合层。 |
| F-014 | 长期趋势目标一致性 | h20 对照之外，按指标族报告 20–180 日、有序右尾目标、MFE/MAE、time-to-hit、生存、右删失、捕获率、假退出、order fill 和 execution cause；任一族可算即用于研究，缺失族给出数据行动计划；全部能力仅属于 QE。 |
| F-015 | PIT 关系模型 | HIST-industry、动态加权图和多关系图只消费逐日 PIT 关系；mapping/stock_index fail-fast；R4 二值真码邻接不重复；新架构先通过 composer/资源 canary。 |
| F-016 | 概念多关系数据与互证 | 概念成员记录多成员、有效区间、可用时点与 source version；完整 PIT、部分样本、代理实验和 HIST-concept/HATS/超图并行，分别标注限制与损失。 |
| F-017 | 研究调度与制品隔离 | 模型类决定串并行；共享 cache 原子写/锁/损坏重建和 recorder 隔离先验收；后端重启不终止或重复启动已运行 worker。 |
| F-018 | 两层 oracle 上界 | reality/oracle 四格与 soft-gating 保存 Top-M/horizon/Top-K/PIT/可交易/成本口径，输出连续增量、置信区间、损失和限制；结果永久标记为未来信息上界，不产生 GO/STOP。 |
| F-019 | 多期限迁移完整性 | R8M 独立训练、共享多头、冻结迁移、全量微调四臂；per-head maturity/purge、transfer matrix、LOO、梯度冲突与 F-014 可用指标族并列分析。 |
| F-020 | 任务级 Alpha 比较 | 不同 horizon/模型名不自动视为独立腿；预测/持仓/入场/板块/P&L/右尾事件/成本重合和固定风险预算 LOO 用于解释互补、冗余与损失，不形成准入门。 |
| F-021 | 当前 trial 最小证据不可丢失 | task 与 Loop 状态分离；新策略 trial 随运行自然保存比较所需的配置、输入身份、指标、成本、风险和失败原因，父 task 失败不得覆盖已成功结果。既有历史事实只读保留，但不再要求历史 Archive 补录、旧 Loop 物化、状态重算或总账固化。 |
| F-022 | 多 Alpha QE 演进底座 P0 | 已有 task/run/child/attempt/event、状态/event 原子性、重启接管、统一节点占用、暂停/恢复/取消、`not_computable`、子任务恢复、QE 创建器及运行网格满足策略演进需求，现为 maintenance-only；只有真实策略 trial 暴露的可复现阻断 BUG 才允许扩展或修复。 |
| F-023 | 策略演进优先与基础设施封板 | 每轮必须产出版本化 strategy candidate 和真实 QE run；只修复真实 trial 暴露且无安全替代路径的阻断 BUG。UI、历史补账/物化、平台完整化不得抢占策略主线。 |
| F-024 | Alpha/状态/关系/策略角色分离 | 每个候选声明 `DIRECT_ALPHA`、`CONDITIONING_STATE`、`RELATION_PRIOR`、`PORTFOLIO_POLICY` 或 `NEGATIVE_CONTROL` 主角色；信号、关系和组合改善分别归因，不互相冒充。 |
| F-025 | 多窗口与 matched-seed 真实性 | 活动候选并列报告 full/early/late；训练型初筛默认使用 seeds 123/314/2718；单次回测、单因子 IC、单最好 seed 和全窗口结果不得覆盖近期失效。最终神经网络候选若三种子区间仍宽，预注册至少 5 seeds 或等价 bootstrap ensemble；不对全部探索机械扩种子。 |
| F-026 | QE 数据面零数据库扩展 | 新 Alpha、HIST、动态图、hazard 与概念关系只消费钉住的 H5/Parquet/bin/sidecar；实验/composer/worker/qrun 不访问数据库、不在线补齐、不回退。 |
| F-027 | v6.6 活动演进顺序 | BUG-1014/1022/1023/1024 receipt/close-sync 并行收尾 -> Loop3 显式 `backtest_only` -> Loop7 显式 `full_train` 单 arm -> 完整 12-arm matched CE3/CE4-B 归因与条件固定权重 blend/LOO -> model-age/refit、四格 sector oracle、benchmark/Brinson 三联诊断 -> 按结果触发适应/门控、两层板块/lead-lag、右尾/退出或横截面/正交分支。participation gap/leadership exhaustion 为不阻塞诊断的 P1；P2 不延迟 P0/P1。 |
| F-028 | matched observation panel | 训练型因子归因 trial 在 `model_params` 预先声明 `qe_observation_panel_v1` 和参考因子交集；composer 将其作为生成期 loader contract，禁止透传策略 kwargs；同组 candidate 使用完全相同的观测索引。缺配置因子、缺参考列、空面板或无效声明 fail closed；历史无契约 task 不改写并保留成功因子 partial concat。 |
| F-029 | QE WSL 主机安全封装 | 所有 task 共享一个本地 WSL 训练槽；本地 GeneralPTNN 使用 `n_jobs=0`、`pin_memory=false`、无 `prefetch_factor`、最多 4 个 BLAS/OpenMP 线程；普通 composer 与 multi-alpha 的 prepare/qrun/read 均清除数据库及非控制面 secret 环境变量。远端硬容量 4、Prediction Store 控制面、冻结文件路径和 batch/模型/策略语义保持不变；unsafe override fail closed。 |
| F-030 | 日频 runner 可靠恢复 | 日频 `qrun_limit.py` 必须在 `qlib.init` 后、`task_train` 前安装与分钟 runner 同语义的 async metric drain barrier 和精确 empty-metric retry；只重试已知落盘竞态，writer 死亡、超时、真实缺制品或其他错误 fail loud。Loop3 使用新装配 runner 显式执行 `retry_mode=backtest_only`，完整 params 缺失 fail loud，禁止 `auto` 回退训练。 |
| F-031 | 空闲控制面静默 | 无活动 task/状态变化时不得形成 2 秒级数据库、远端 API 或状态事件风暴；观察器单实例，正常探测至少 60 秒节流，状态/错误变化才持久化，正常心跳留在内存。 |
| F-032 | QE 实时日志单上游与零 UI 空转 | 每 task/node 只有一个 RD-Agent 上游，由 AIstock broker 共享给 evolution/experiment/multi-node 消费者；cursor 续传且 stale/conflict fail closed；无订阅者、页面隐藏或面板折叠时上游、文件和 DB 工作为零。 |
| F-033 | 五槽日志边界与精确清理 | 新实时日志仅写 `qe-live-0.jsonl`～`qe-live-4.jsonl`，每槽最大 1 GiB；旧 `evolution.log` 只读。历史文件删除不属于本变更，必须在 source/runtime/frontend 验证后由用户对精确路径另行授权。 |
| F-034 | model age、重训可恢复成分与 calendar regime 分离 | 相同冻结数据、因子、h20 标签、观测面板、策略、成本和测试窗口下比较固定切分、模型年龄切片、expanding 与 rolling refit；每个 vintage 保存训练窗口、已成熟标签截止、purge/embargo、cadence 与模型 hash。cadence 仅在训练/验证区预注册，测试期不重选；单次恢复不作唯一因果归因。 |
| F-035 | benchmark-relative 与 Brinson companion | 绝对收益继续为主账之一；冻结 benchmark identity、可投资池等权对照、逐日 PIT 板块权重和映射 hash 可证明时，并列输出主动收益、beta、tracking error、IR、allocation/selection/interaction。缺输入为 `NOT_COMPUTABLE`，禁止 DB 或当前权重回填。 |
| F-036 | 右尾目标与 meta-labeling 角色分离 | h40/h60 LTR、NDCG@tail、Top-decile/分位、收益阈值、time-to-hit、MFE/MAE 与 drawdown hazard 各自持有标签/删失/指标身份；meta-label 只在严格 OOF 主候选上学习 take/skip/size/continue-hold，不冒充候选召回。 |
| F-037 | OOF learned conditional gate canary | gate 只消费训练/验证期 OOF 腿预测和因果可见的冻结 state，输出有界权重；测试期只推理。固定权重、LOO、总风险、成本和窗口同口径；canary 形成稳定增量前不联合训练、不重写 combine 底座。 |
| F-038 | 策略级选择偏差治理 | MA-E01～后续策略配置按 family 保存全部尝试、失败/放弃项、有效独立试验数与收益序列；walk-forward、CSCV/PBO、DSR 或适用等价诊断作用于策略选择层，不只计算最终赢家，也不把诊断变成人工研究门禁。 |
| F-039 | sector taxonomy 敏感性 | 申万 L2 为主；冻结文件存在可证明 PIT L1 映射时增加 L1 对照，保存 taxonomy/version/membership hash 与板块规模分布。概念/题材仍为 P2 文件候选，禁止当前成员回填历史、数据库补齐或历史采集平台前置。 |

## 13. Implementation Plan / 实施计划

v6.6 当前执行顺序覆盖以下历史 Phase 顺序：MA-E16 已终态为 10 completed/2 failed，成功 arm 与重复 task 均不再运行；2026-08-12 v6.5 实时快照仍是本文最新进度证据，本次只改规划、不冒充新 task。BUG-1014/1022/1023/1024 的 source 已被当时运行链加载，剩余逐 BUG post-restart receipt/close-sync 与 Loop3/7 恢复并行收口；BUG-1026/1030/1031 已关闭，不再是前置。Loop3 只执行显式 `backtest_only`，Loop7 只执行显式 `full_train` 单 arm，随后完成 matched 归因和 P0 三联诊断，再按结果选择 P1 分支。全部继续使用日频 `CLOSE_PRICE`，不恢复 MA-E14 阈值 overlay，也不把 MA-E15 或未经逐 task 核验的旧 V25/V25_1 组合指标当作执行基线。F-014、通用 UI、历史补账/物化、监控/日志平台和底座完整化继续不实施。

### Phase G0-A：研究设计与证据记录

1. 把第 4.6–4.9 节分析方法、候选族和研究来源写入本规格。
2. 运行 `aistock_feature_workflow.py validate --tier F2` 记录设计结构状态；未完成项形成实现计划，不限制 QE 研究。

### Phase G0-B：数据与评估器前置能力

1. RD-Agent：修复 candidate bundle 的 `l2_code_id` dtype/unknown/schema/receipt，并在隔离目录生成新 bundle。
2. AIstock：给 `quick_ic_screen.py` 增加通用 horizon、精确 T+1→T+N+1 标签、split manifest/receipt、冻结方向及 HAC 指标。
3. RD-Agent：给统一指标引擎与 SOTA API 增加 F-004 companion fields。
4. AIstock：增加 F-005 additive DB/ingest/router/MCP 契约，但不执行生产迁移。

### Phase G0-C：PIT 基线修复与证据

1. 修复 F4/R2 可执行资产源的板块时序语义并增加切换/唯一性测试。
2. 对两个仓库分别运行最小定向测试、lint/compile/diff check；生成 candidate receipt。
3. 更新本矩阵为真实状态，列明数据、实现和平台缺口；各仓库状态分开记录。

### Phase G0-D：后续因子研发（不属于本次前置实现）

A1–A6、Batch B 和其他候选均可在 QE-only 范围按资源并行使用 `develop-factor` 与因子库 MCP 研发；F-001–F-006 的完成度、candidate 状态、数据缺口和 Batch A 结果作为上下文记录，不是启动条件。每个候选保留实验卡、快筛、统一指标、相关性、分类、当前证据和后续数据/实验计划。

### Phase G0-E：post-R6 归因与长期趋势闭环

1. `[COMPLETED_LOOP_LEDGER_RECOVERED]` R2–R5 的 67/67 个 Loop、6 个顶层失败 task 中的 50 个成功 Loop 和 R6 的 30 个 Loop 已写入第 9.4.5 节；后续聚合以 Loop 为最小证据单元。
2. `[COMPLETED_CURRENT_TRIAL_BELOW_BASELINE]` R7A `equal + rank` 与 R7B `equal + zscore` 均已完成；继续用于 `ic_weighted/risk_parity`、portfolio fusion、跨标签融合、LOO、成本与风险预算互证，不形成停止规则。
3. `[R8A_R8B_COMPLETED]` R8A 已完成 12/12，R8B 已完成 6/6；18 个 Loop 的 prediction/label/params 完整可读，三种子、共同成熟区间、分段、板块、右尾和因子归因已写入第 9.6.2.1 节。
4. `[R9S_COMPLETED_24_OF_24]` 两个 backtest-only task 已完成；h40/h60/h120 的最佳替换速度不同，结果已写入第 9.6.5 节。
5. `[R10_COMPLETED_51_OF_51]` LGBM/TCN/LTR/LSTM 与 hold10/20/30 全部完成；LSTM `G15-FPL+h60` 三种子与 hold30 第三种子已补齐。成功结果进入研究总账，模型、标签、因子角色与策略转换分开解释。
6. `[R11_COMPLETED_15_OF_15_FULLY_ARCHIVED]` R11A 9/9、R11B 6/6；R11A Loop5–8 的训练/预测/回测制品成功，BUG-741 修复状态重算后以 results-only 恢复。backfill run `qear_bf_f9115c130b94464089b45de3f26c5fdf` 已 15/15 写入，两个 task 均 fully archived。matched-seed 持仓与图模型长期基线结论写入第 9.6.6 节。
7. `[F014_EXISTING_CAPABILITY_FROZEN]` Phase 1–3、Loop4 单 canary、PR #2875/#2906/#2964 与既有只读回读证据保持完成；当前 3000 visual、其余 Phase 4/5 和历史完整化均由用户终止，不再是待补项。
8. `[GRAPH_LONG_HORIZON_BASELINE_COMPLETE]` GAT h20、embedding、二值行业边、R7 简单融合及 R11B h40/h60 三种子证据已齐；h40 为当前效率锚点，后续追加动态板块关系、趋势/回撤双头、sector gating 和动态退出。
9. `[R12G_COMPLETE_R12P_RECOVERY_COMPLETE]` EfficientGATs cooperative-execution canary `qe_20260718_040323_9a4a` 已 1/1 完成；跨任务四腿 portfolio fusion 已通过 append-only successor 恢复形成 baseline、equal、orthogonality-aware 和八条 LOO，最终研究可用 child 为 11/21。低成本板块回撤 overlay 已进入 F2 设计/实现，右尾/板块状态研究、四格 oracle、LSTM R8B2 和 TCN G14-FP 长期限对照继续设计。
10. `[MA_E07_TO_E13_TERMINAL]` MA-E07 四腿与 MA-E10 三腿形成全窗口 Pareto；MA-E12 新增腿退化；MA-E13G 将静态行业关系重分类为关系先验；MA-E13R 暴露近期 regime 弱 Alpha。真实 run/指标见第 2.5 与 9.9.2 节。
11. `[MA_E14_TERMINAL]` H95/C85 regime overlay 在 full/early/late 均降低 CAGR，当前阈值 overlay 分支结束；regime 状态只保留为未来风险正则/动态权重输入，不继续参数扫描。
12. `[MA_E15_TERMINAL_CONFOUNDED_EXECUTION_LIMITED]` `qe_20260810_020755_38d2` 已 9/9 完成；CE3/CE4-B 的预测、IC/RankIC 与 Top-K 面板诊断可读，但 outer-union 引入 1,511 个额外观测，breadth 因子归因无效；全部 Loop 的 V25_1 分钟执行另受开放 BUG-852 零日频特征影响，组合收益/风险/换手不作为执行真实性基线。BUG-1006 只修复 matched observation panel，不关闭 BUG-852。
13. `[MA_E16_TERMINAL_10_OF_12_RECOVER_1_RERUN_1]` MA-E16 `qe_20260810_224522_b0d9` 使用同一 2026-06-30 文件快照、CE3 reference panel、LGBM/LSTM × CE3/CE4-B × seeds 123/314/2718，于 05:45 终态为 10 completed/2 failed。Loop3 训练/预测已完成，待日频 runner 独立 BUG 修复后显式 `retry_mode=backtest_only`，完整 params 缺失 fail loud；Loop7 是 stop/SIGTERM `-15`，待 BUG-1014 用户重启验证后显式 `retry_mode=full_train` 只补跑该 arm。成功 10 arm 不重训，重复 task 永不恢复。四组现有 matched pair 的 CAGR 均提高但 IC/RankIC 混合、MDD 3/4 恶化，breadth 主角色暂归 `CONDITIONING_STATE`，portfolio conversion 为待验证用途；完整矩阵、三窗口、成本与互补性分析后才做固定 blend/LOO。
14. `[V6_6_DIAGNOSTIC_REPRIORITIZED_DESIGN_ONLY]` MA-E16 收口后依次预注册 P0-D1 staleness、P0-D2 四格 sector oracle、P0-D3 benchmark/Brinson；可在主机安全合同内并行运行。诊断结果分别触发适应/条件门控、两层板块/lead-lag、右尾/退出或横截面/正交分支；participation gap、leadership exhaustion 为并行 P1。该条仅为设计与排期，当前没有新 run、指标、收益或运行态证据。

### Phase G0-F：PIT 关系模型 canary

1. HIST-industry/动态图只消费已钉住 identity/cutoff/hash 的逐日 relation 文件；composer、worker、qrun 不访问数据库，缺文件或 mapping/stock-index 不一致时 fail closed。
2. 在同因子/标签/切分下比较 embedding-only、industry-bias 和 dynamic residual/flow/leadership；先做关系/风险状态输出，再决定是否需要独立 Alpha head，全部使用 seeds 123/314/2718。
3. 动态关系优先于 HIST-concept、MASTER/IGMTF/TRA；逐关系消融、近期窗口与组合风险贡献并列记录，不用单次 RankIC 决定方向。

### Phase G0-G：概念 PIT 数据与多关系扩展

1. 概念/事件/供应链关系降为 P2 条件方向；仅当版本化 PIT 文件已经存在时评估，不启动数据库回填、历史采集平台或回放系统建设。
2. HIST-concept、industry+concept 多关系、超图和概念层两层选股可按空闲资源运行；完整/部分/代理数据分别标注，不能延迟 P0/P1 candidate。
3. 结果只限定当前文件版本、覆盖与结构；无增量时保留证据，但不自动触发数据完善工程。

### Phase G0-H：多 Alpha QE 演进底座 P0-1～P0-4

`[P0_1_TO_P0_4_MERGED_RUNTIME_VERIFIED]` 该 Phase 已于 2026-07-22 完成。实施与运行证据以 `docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md` 的稳定 201～218 验收条目为权威；完成事实不构成科研准入条件。

1. **P0-1 durable orchestration**：在现有 combine-backtest 表和服务上增加 first-class task、child、attempt、event 与 run lease/fencing/CAS；权威状态和 event 同事务；将 `QEWorkspaceClient` 作为 WSL/远端统一执行契约；统一读取现有 QE active execution 来源并按 remote identity 去重；启动 scanner/worker 在后端重启后核对并接管，不重复提交远端 loop。schema 缺失只让 multi-alpha worker/写接口结构化不可用，不得阻止整个 FastAPI 或非 QE 模块启动。
2. **P0-2 lifecycle/recovery**：提供 run 的 pause/resume/cancel；legacy stop 委托 cancel/kill 并保持现有单 Alpha 终止语义。提供 child 的 `backtest_only/results_only/rematerialize_and_backtest` retry；所有动作保留 lineage、attempt、远端 ID 和已成功结果，不把未知远端状态静默变成失败，也不创建伪 child pause 状态。
3. **P0-3 create/composer UI**：以 `/quantevolver/evolution` 为规范入口，抽取并复用单 Alpha QE 自动演进页面的 shell、task list、create dialog、node selector、status/action 组件；多 Alpha 表单覆盖现有 request 全字段，旧 `/multi-alpha/combine-backtest` 路由只做兼容映射并复用同一组件/DOM/样式。
4. **P0-4 child observability**：在现有 QE 详情布局增加 child/attempt grid、DB event + workspace/remote log、restart reconciliation 状态和单子任务操作；复用 `LoopDetailPanel`、`EvolutionTrajectory`、`LogsPanel` 和现有 combine diagnostics，不增加另一套页面风格。
5. backend repository/service/router/startup、additive migration、frontend adapter/components、API/UI/DB 定向测试、restart E2E、并发/取消/恢复/历史回填验证均已完成；P0-3/P0-4 CLI Playwright 为 `8 passed`，父/从属 F2 validator 均通过。后续修改继续禁止简化版、静默错误、隐式 fallback、指标伪造和未经用户确认的门禁/审批。

### Phase G0-I：v6.6 P0 三联诊断与结果触发型演进

1. `[PENDING_AFTER_MA_E16_RECOVERY]` model age / refit：先运行同合同 LGBM fixed/model-age-slice/expanding/rolling，再只对验证区选定 cadence 的 LSTM 执行 matched 多种子；每个 prediction vintage 钉住可用标签截止、窗口、cadence、模型和数据 hash，并把可恢复成分与相同年龄/不同 calendar regime 的剩余退化分栏。
2. `[PENDING_AFTER_MA_E16_RECOVERY]` sector oracle：运行 reality/oracle 四格、hard/soft 两层、L2 主 taxonomy 与可用时的 L1 敏感性；保存 sector recall、within-sector rank、右尾捕获、成本、暴露与不可部署标记。
3. `[PENDING_AFTER_MA_E16_RECOVERY]` benchmark/Brinson：对 MA-E07、MA-E10、MA-E13R、完整 MA-E16 和后续候选同时保留 absolute 与 active 口径；benchmark/sector weight 输入缺失时 `NOT_COMPUTABLE`，不建设 DB/历史回填链路。
4. `[RESULT_TRIGGERED_NOT_PRECOMMITTED]` 若 staleness 可恢复，比较简单重训与 DoubleAdapt/Proceed；若 sector oracle 显示上界，推进现实两层/lead-lag；若右尾召回仍低，推进 LTR/tail/hazard；若候选已召回但转化/退出弱，再推进 meta-labeling。TRA/MIGA gate 仅在现有腿出现条件互补后运行，MASTER/概念/新闻关系继续 P2。
5. `[NO_PLATFORM_EXPANSION]` 上述 trial 复用现有文件数据面、双节点调度、结果面和最小自然输出；不新增 UI、Archive、schema、历史物化、数据库数据面、在线补齐或基础设施完整化任务。

## 14. Verification Plan / 验证计划

### 14.1 Business oracle / 业务判定真值

1. 标签真值：horizon=N 与 `close[t+N+1]/close[t+1]-1` 逐点对比并保存差异；h20 的最后可评估信号日由交易日历反推，未成熟尾部作为 censored 数据单独分析，不用 0 或失败伪造。
2. PIT 真值：个股切换行业后，F4/R2 使用“当前行业自身的历史面板”，不能把个股切换前后的两个行业价格串接；同一板块日出现冲突值必须报错。
3. 数据真值：static 输出以 daily-basic 键为基表，`l2_code_id` 为 signed integer，daily-basic-only 键为 `-1`，旧 120 个数据列和 dtype 不回归。
4. 指标真值：RD 计算 key `20d`、区间 label `T21T1` 与 legacy DB row `return_horizon=1d` 三者并存；12 个 h20 字段从 RD engine 经 official writer、router 到 MCP 不丢失，旧 payload 全部补空而不报错。
5. 权威与副作用真值：只有 official evaluation writer 可落表；RD task/loop writer 继续禁用。本批任何测试都不得连接/写生产 DB、应用 DDL、promotion 或重启 runtime。
6. 长期评价真值：hN IC/RankIC 与 hN 标签对齐情况和业务捕获分别报告；F-014 每个可计算指标族立即形成科研证据，缺失族给出补数/补算方案，平台完整度不控制期限或模型研究。
7. 可成交性真值：理论 `T+1 close_qfq` 机会与实际成交分层并存；reconciled trade artifact 优先于日线涨跌停推断，无订单/队列证据时为 `NOT_VERIFIABLE`，买入和退出阻断对称。
8. oracle 真值：未来信息只构造不可部署上界；四格、soft-gating、成本/可交易口径、连续增量和置信区间完整保存，不输出 GO/STOP。
9. staleness 真值：每个 prediction 的训练/验证窗口、模型 vintage、可用标签截止和 purge/embargo 可追溯；rolling/expanding 只能使用预测时已成熟数据，test 不选择 cadence、窗口、超参数或是否采用适应模型。
10. benchmark 真值：绝对收益不被覆盖；active return、beta、tracking error、IR 与 Brinson 绑定同一冻结 benchmark、逐日权重、taxonomy 和持仓。输入缺失时显式 `NOT_COMPUTABLE`，不能用当前成分/权重回填历史。
11. tail/meta/gate 真值：右尾 candidate recall、meta-label precision/size/exit 与 gate 组合增量分层；主模型未召回的股票不能由 meta-label 计作召回，gate 只消费 OOF prediction 和因果可见 state，测试期只推理。
12. 选择偏差真值：策略 family 的全部尝试、失败、放弃和参数身份可回读；DSR/PBO/CSCV 或等价诊断使用完整策略收益序列，不能只把最终赢家或成功 trial 送入校正。

### 14.2 L0–L5 验证映射

| level | 本批/后续验证 | 状态口径 |
|---|---|---|
| L0 | 文档章节、exact field list、SQL named-parameter 与 schema contract、F2 validator、diff check | 记录完成、失败和缺口，不作为研究许可。 |
| L1 | quick horizon/direction/manifest/HAC、bundle dtype/unknown/schema、F4 PIT/conflict、RD h20 engine/API 单测 | 记录各项结果、受影响范围与后续修复。 |
| L2 | 隔离的 nullable schema/upsert 参数、official summary positional mapping、router/MCP emit/旧 payload 回归 | 记录平台实现状态；不影响已有 QE 研究结果。 |
| L3 | 真实 2026-06-30 数据、官方指标/相关性与 QE 因子加载 | 三个因子独立指标和 R6 文件数据面加载已完成；非 QE realtime parity 的历史 `pending` 只作限制记录，不是本蓝图待办。 |
| L4 | GATs/LGBM、融合/两层 oracle 与真实模型、staleness walk-forward、benchmark/Brinson、长期趋势/右尾/meta-label、条件 gate、R8M、PIT 关系模型、成本容量、拥挤、策略级 DSR/PBO 与多种子业务流 | R2–R7、R8A 12/12、R8B 6/6 及预测资产深度归因是既有证据；v6.6 新项均为待运行设计，按自身角色、窗口、LOO、成本、风险和选择偏差自然记录结果，不再补历史 Loop 总账或 F-014 平台面。 |
| L5 | 非 QE 生产 DDL/回填、candidate → active、paper/live | 本蓝图不执行、不接入；QE-only 是唯一硬边界。 |

新增/修改业务逻辑的覆盖率目标为 line ≥ 80%、branch ≥ 70%；优先由定向 pytest coverage/CI 记录。若因嵌入式因子代码或外部引擎边界无法可靠计量，必须用上述 business oracle 分支测试补证并在矩阵记录明确例外，不得以全仓平均覆盖率掩盖关键路径。

### 14.3 具体命令与证据

| 层级 | 验证 | 预期证据 |
|---|---|---|
| Design | `python scripts/aistock_feature_workflow.py validate --design ... --tier F2` | F2 PASS，design item 与 matrix 行数一致。 |
| Candidate unit | 生成器 dtype/unknown/schema 测试 | `l2_code_id` 为 int16/int32；NaN→`-1`；非整数/越界 fail-fast；receipt 字段齐全。 |
| Candidate artifact | 在新隔离目录生成 bundle | 行数、日期、股票、板块、known coverage、`-1`、schema 与文件指纹 receipt；active 未改变。 |
| Quick screen unit | horizon=1/20 标签和 HAC 边界测试 | 默认 1d 不变；h20 精确 T+1→T+21；lag=19；不足/退化返回空而非伪值。 |
| RD metrics unit | 引擎/API 序列化测试 | legacy 字段不变，h20 companion fields 数值定义和 nullable 行为正确。 |
| AIstock contract | schema/upsert/router/MCP 定向测试 | 新字段往返，旧 payload 兼容；不连接/修改生产库。 |
| F4 PIT unit | 多行业、多日期、行业切换与重复值测试 | 板块收益仅按板块时序计算；切换不串组；板块日值冲突 fail-fast。 |
| Fusion/two-layer | 同快照预测对齐、四格 oracle/soft gate、风险预算、组合与分层归因 | oracle 永久标记不可部署；阈值预注册；单腿/融合/两层基线可复算；任务级重合与 leave-one-leg-out、暴露和成本齐全。 |
| Model-age/refit walk-forward | fixed/model-age-slice/expanding/rolling refit 的同合同对照；每个 vintage 的 matured-label cutoff、purge/embargo、cadence、模型/数据 hash | 不改变测试总体和策略；cadence 仅用训练/验证选择；可恢复成分、相同年龄/不同 calendar regime、late-window、成本和算力开销分栏，单次恢复不作唯一因果归因。 |
| Benchmark attribution | 冻结 benchmark 与等权可投资池对照；active return/beta/tracking error/IR；PIT sector allocation/selection/interaction | absolute 指标保留；benchmark/taxonomy/weight hash 可追溯；缺输入显式 `NOT_COMPUTABLE`，无 DB/当前权重回填。 |
| Long-trend | 20–180 日、有序 barrier、MFE/MAE、生存/右删失、信号→成交/退出阻断测试 | 与 Advisory Phase 8 日期/标签契约一致；Type A/B 不共用标签头；末端未成熟样本不伪装失败；日线触板不伪装订单真值。 |
| Tail/meta/gate | tail recall/NDCG/calibration、meta-label precision/size/continue-hold、OOF gate 与固定权重/LOO | 候选召回、过滤/仓位、退出和组合权重分别归因；测试标签不进入拟合；共同漏捕不冒充 gate 可解决。 |
| Strategy selection bias | 完整 strategy family ledger、失败/放弃配置、有效独立试验数、walk-forward/CSCV/PBO/DSR | 不只计算最终赢家；结果用于限制结论强度，不形成新增人工审批或研究停止门禁。 |
| Multi-horizon transfer | 独立/共享/冻结迁移/全量微调、per-head mask/purge、transfer matrix/LOO/gradient conflict | 不补零未成熟标签；共享负迁移可见；PCGrad 只在观察到冲突后另立对照；最终裁决引用同一 F-014 vintage。 |
| Relational canary | PIT relation/mapping/stock_index/composer/fit-predict/resource 测试 | 静态快照被拒绝；错位 loud fail；首 loop 完整归档；R4 二值邻接不重复冒充新实验。 |
| Parallel artifact | cache 原子写/锁/损坏重建、recorder 隔离与 restart recovery | 并行任务不互相读到半文件、不覆盖归档；后端重启不终止或重复启动 worker。 |
| Targeted coverage | quick screen + shared h20 contract 的 line/branch coverage | 29 tests；combined coverage 92%，shared contract 100%；F4 嵌入代码以 oracle 两分支补证。 |
| Repository | compile/lint/diff/targeted pytest | 两仓各自通过；已知基线告警与本次新增问题分离。 |
| Baseline authority audit | `test_factor_metrics_authority_static.py` | 14 passed/2 failed；失败均在未修改的 origin/main 文件：4 个既有硬编码本地路径，以及测试引用已不存在的 `MultiAlphaGroupEditor.tsx`。不作为本批成功证据，也不归因于本改动。 |
| External status | 真实数据 E2E、QE 控制/结果面、非 QE 生产状态 | QE 数据与既有 R6/R7、R8A/R8B receipt 只读保留；F-014 未覆盖面冻结为历史限制，非 QE UI/promotion/paper/live 不属于本蓝图动作。 |

## 15. Design Acceptance Matrix / 设计验收矩阵

v6.6 解释规则：F-001～F-023 的历史 `PLANNED/OPEN/PENDING` 只保留设计和证据状态，不代表当前排期。F-024～F-033 定义角色、窗口、数据边界、matched observation panel 与已交付运行安全合同；F-034～F-039 定义 staleness、benchmark/Brinson、右尾/meta-label、OOF gate、策略级选择偏差和 taxonomy 敏感性。新项的设计矩阵保持 `DESIGN_READY_*`，实验是否运行、是否产出结果在第 2.5/17 节独立记账，不冒充结果，也不是人工审批门禁。历史补账、通用 UI、监控/日志平台和平台完整化继续终止。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文 2.3、4.6–4.9、8、18 | validation-receipt: ledger 字段/存储契约、候选族与研究来源已冻结；本批未运行候选公式 | VERIFIED | 无 |
| F-002 | RD-Agent `tools/generate_static_factors_bundle.py` | validation-receipt: 9 项 unit；7,304,119 行 candidate receipt；parquet/schema SHA-256 | VERIFIED | 无 |
| F-003 | AIstock `scripts/quick_ic_screen.py` | validation-receipt: horizon/label/direction/manifest/HAC/CLI 单测 20 passed；与 shared contract 合计 coverage 92% | VERIFIED | 无 |
| F-004 | RD-Agent metrics engine 与 SOTA API | validation-receipt: h20 engine/API 2 passed；与 bundle 合计 11 passed | VERIFIED | 无 |
| F-005 | AIstock migration、`factor_metrics_contract.py`、official writer、routers/MCP | validation-receipt: contract 9 passed；contract+MCP/emit 51 passed；official batch 26 passed/1 skipped | VERIFIED | 无 |
| F-006 | AIstock `scripts/p1_new_factors.py` F4/R2 tracked repair source | validation-receipt: PIT 当前行业历史与冲突 fail-fast 2 passed；旧 catalog 口径失效已记录 | VERIFIED | 无 |
| F-007 | 后续候选 asset/realtime loader | validation-receipt: 三个候选已登记并由 R6 QE 离线执行；catalog asset/transformation 状态复核 | APPROVED_BY_USER: PARTIAL_QE_ASSET_READY | catalog 1525/1528/1532 `is_available=true` 且 QE 可运行；`asset_status/transformation_status=pending/PENDING`、`realtime_code_text` 为空，离线/实时 parity 与荐股/模拟盘加载仍未验收。 |
| F-008 | 因子库 MCP + Stage 0/3 | validation-receipt: 2026-06-30 官方 h20 批次；每因子 583 个基本相关性 pair；第 3.1 节 | APPROVED_BY_USER: PARTIAL_OFFICIAL_METRICS_CORR_COMPLETE | 独立指标和基本相关性已完成；板块原生层、partial/residual IC 和分类/谱系分析可并行补充，不影响继续研究。 |
| F-009 | Stage 1/3 + portfolio evaluator | validation-receipt: R6/R7A/R7B 已有成本后回测；HAC/bootstrap/DSR/PBO/cost/capacity/crowding 设计与 L4 oracle | APPROVED_BY_USER: PARTIAL_QE_BACKTEST_COMPLETE | 已有统一回测；新 trial 只自然输出可得的成本、容量与拥挤证据，不启动独立平台补齐项目。 |
| F-010 | QE GATs/LGBM experiment specs | validation-receipt: R4 二值邻接、R6 30 Loop、multi-seed OOS 与第 9.4.2 节 | APPROVED_BY_USER: PARTIAL_R6_COMPLETE | 2×2/真码邻接和 R6 LGBM/GAT 多种子已完成；历史未覆盖的两层/完整 L4 项不再作为活动验收，只有服务 v6.0 明确候选时才按最小范围复用。 |
| F-011 | 两仓隔离 worktree 与第 17 节 | validation-receipt: active/旧 candidate/production DB/DDL/runtime 均未修改 | VERIFIED | 无 |
| F-012 | 两仓定向测试、lint/compile/diff 与 F2 validation | validation-receipt: AIstock 99 passed/1 skipped；RD-Agent 11 passed；F2 PASS；authority 14 passed/2 个 origin/main 既有失败已分离；PR/merge 分离 | VERIFIED | 无 |
| F-013 | 本文 4.10、9.4–9.5、11.6、Phase G0-E | validation-receipt: R7A/R7B、R12P baseline/equal/orthogonality-aware 与八条 LOO、MA-E07～MA-E13 真实组合回测 | APPROVED_BY_USER: PORTFOLIO_FUSION_AND_LOO_COMPLETE_CONTINUING | 组合和 LOO 路径已形成真实结果；MA-E12 证明新增腿可退化，后续按候选角色和 full/early/late 继续归因。 |
| F-014 | 本文 9.6、11.6、Phase G0-E；父设计 v1.22 与 Phase 2 从属设计；Advisory Phase 8 | validation-receipt: 既有 R8B 6/6、Loop4 2,004+6；PR #2906/#2964 既有证据 | APPROVED_BY_USER: FROZEN_EXISTING_CAPABILITY_REUSABLE | 用户已终止 normal Loop 平台 smoke、runtime visual、其余 5 个 R8B、Phase 5 和批量历史重评；这些不是待交付项，既有能力只供新 trial 可选复用。 |
| F-015 | 本文 4.10、9.7、9.9.2、Phase G0-F | validation-receipt: R4、R11B、MA-E13G 三种子；冻结关系文件、mapping/stock-index fail-closed；动态 residual/flow/leadership 设计 | APPROVED_BY_USER: STATIC_RELATION_RECLASSIFIED_DYNAMIC_READY | industry-bias 为关系先验/组合正则而非直接 Alpha；动态关系是 P1，禁止实验运行期 DB provider。 |
| F-016 | 本文 4.10、9.8、9.9.2、Phase G0-G | validation-receipt: P2 文件身份/覆盖/损失设计；未来实验结果另立真实 run receipt | DESIGN_READY_P2_CONDITIONAL | 无 |
| F-017 | 本文 9.9、Phase G0-F、14.3 | validation-receipt: 模型资源分类、cache/recorder 隔离、MA-E07～MA-E13 双节点真实运行与 restart recovery | APPROVED_BY_USER: DUAL_NODE_EXECUTION_REUSABLE | WSL 图训练保持 serial；远端承担 CPU/组合。并发由现有调度与资源证据决定，不新增平台工作。 |
| F-018 | 本文 2.5、9.5、9.9.2～9.9.3、14 | validation-receipt: 四格 oracle、soft-gating、连续增量/置信区间、PIT/成本/可交易同口径、不可部署标记与 L1/L2 可用性处理；真实 oracle 尚未运行，实验状态在第 2.5/17 节独立记录 | DESIGN_READY_P0_DIAGNOSTIC | 无 |
| F-019 | 本文 9.6.3、9.9、14 | validation-receipt: R8M 四臂、per-head maturity/purge、transfer matrix、LOO、梯度冲突和 F-014 评价 | DESIGN_READY_P2_CONDITIONAL | 无 |
| F-020 | 本文 9.6.4、9.9、14 | validation-receipt: R12P 与 MA-E07～MA-E13 的预测/组合/LOO/窗口结果 | APPROVED_BY_USER: TASK_LEVEL_ALPHA_COMPARISON_ACTIVE | 基础任务级比较已完成；新专家继续记录独立腿与组合 LOO，不以模型名或低相关自动授予腿身份。 |
| F-021 | 本文 2.2、2.4、2.5、9.4.5、9.6.5–9.6.7、9.9.3、Phase G0-E | validation-receipt: 既有事实只读保留；新 trial 最小输出合同 | APPROVED_BY_USER: FROZEN_HISTORY_CURRENT_TRIAL_MINIMUM_ONLY | 禁止历史 Archive 补录、旧 Loop 物化、状态重算和总账固化；新 trial 只自然保存比较所需最小结果。 |
| F-022 | 本文 4.11、9.9.1、9.9.3、Phase G0-H；`multi_alpha_qe_evolution_foundation_f2_design_20260718.md` | validation-receipt: P0-1～P0-4 与后续真实运行/阻断 BUG 修复 | APPROVED_BY_USER: P0_1_TO_P0_4_COMPLETE_MAINTENANCE_ONLY | 当前基础满足策略演进；只修真实 trial 暴露且无安全替代路径的阻断 BUG。 |
| F-023 | 本文 2.2、2.5、9.9.2–9.9.4、Phase G0-E；`qe_sector_risk_overlay_f2_design_20260726.md` | validation-receipt: MA-E07～MA-E15 终态指标、三窗口 overlay、Prediction Store 面板诊断与 V25_1 执行源码复核 | APPROVED_BY_USER: MA_E15_COMPLETE_SIGNAL_VALID_EXECUTION_LIMITED | MA-E14 当前 overlay 退化；MA-E15 的信号与面板诊断完整，但因面板混杂不可做 breadth 因果归因，且组合指标受 BUG-852 限制；BUG-1006 是恢复 matched 因子归因的最小阻断修复。 |
| F-024 | 本文 9.9.2 | validation-receipt: 角色表与逐候选 role/归因输出合同 | DESIGN_READY_ROLE_SEPARATION | 无 |
| F-025 | 本文 2.5、9.9.2～9.9.3 | validation-receipt: MA-E13G matched-seed 先例与 full/early/late + seeds 123/314/2718 输出合同；最终神经网络候选的条件扩 seed/ensemble 规则；当前不重跑历史模型 | DESIGN_READY_MULTI_WINDOW_MATCHED_SEED | 无 |
| F-026 | 本文顶部不变量、9.7、9.9.3、Phase G0-F/G | validation-receipt: DB-poison/静态扫描、dataset identity/hash、双节点文件一致性 | APPROVED_BY_USER: ZERO_DB_RUNTIME_INVARIANT_ACTIVE | BUG-989 已建立现有链路证据；所有新方向继承并在自身 trial 显式验证，不得回退数据库。 |
| F-027 | 本文 2.5、9.9.2–9.9.3、Phase G0-E/I | validation-receipt: MA-E16 10/12 终态、12-arm 收益明细、两个失败原因、四组 matched pair 的收益/风险权衡；先恢复、再三联诊断、按结果触发 P1 的 v6.6 顺序；Loop3/7 与新诊断尚未执行，PR/运行态/实验状态在第 17 节独立记录 | DESIGN_READY_NEXT_EVOLUTION_SEQUENCE_V6_6 | 无 |
| F-028 | `config_composer.py`、生成的 `prepare_factors.py`、BUG-1006、本文 9.9.2～9.9.3 | validation-receipt: canonical contract、matched CE3/CE4 同索引、public in-memory ScoreWeighted/CLOSE_PRICE compose、策略 kwargs 隔离、legacy partial concat、缺结果/缺列/空面板 fail-closed 聚焦测试；合入与运行态不属于设计验收状态 | VERIFIED | 无 |
| F-029 | `config_composer.py`、`qe_active_execution_capacity.py`、`qe_subprocess_env.py`、`remote_dispatch.py`、`combine_backtest.py`、RD-Agent `qe_evolution_api.py`、BUG-1014、本文 2.5/9.9.3/17 | validation-receipt: 双 task Loop7 重叠时间、WSL 主进程+2 worker RSS、Windows pages/sec、WSL swap、canonical 全局容量 1/远端容量 4、GeneralPTNN 单进程 loader、线程 env、完整 PG*/secret/injection 清洗、remote_env pre-serialization reject；AI merge `49ccc1f3…` 已包含于 backend identity `0e0afed2…`；RD release `795973e1…` 已部署记录；两节点健康 | APPROVED_BY_USER: SOURCE_LOADED_CLOSE_SYNC_PENDING | BUG-1014 post-restart receipt/close-sync；不得阻断 Loop7 单 arm 恢复 |
| F-030 | `scripts/qrun_limit.py`、`test_qrun_mlflow_metric_retry.py`、分钟 runner 既有 BUG-605 合同、BUG-1022、本文 2.5/9.9.3 | validation-receipt: Loop3 训练/预测已完成，empty ICIR 发生在异步 FileStore 读取；BUG-1022 merge `a7bc06a3…` 已包含于 backend identity `0e0afed2…`，日频/分钟 runner 共用合同的双路径测试已建立 | APPROVED_BY_USER: SOURCE_LOADED_CLOSE_SYNC_PENDING | BUG-1022 post-restart receipt/close-sync 与 Loop3 `backtest_only` receipt |
| F-031 | BUG-1023/1024/1026；durable orchestrator、reconcilers、node health/dispatch observers；本文 9.9.3/10/17 | validation-receipt: BUG-1023/1024 merges `d8e80b9f…`/`3a6e3290…` 已包含于 backend identity `0e0afed2…`；BUG-1026 `verified/closed`；单观察器、60 秒探测、变化才写状态测试与 CI | APPROVED_BY_USER: PARTIAL_CLOSE_SYNC | BUG-1023/1024 只补 post-restart receipt/close-sync；BUG-1026 不再是前置 |
| F-032 | RD-Agent BUG-1030 PR #19；AIstock BUG-1030 PR #3300；BUG-1031 PR #3303；QE log broker/client/routes/frontend lifecycle | validation-receipt: RD merge `795973e1…`、AIstock merge `aee31e67…`、broker/frontend merge `573e5380…`；两节点 `/health=ok`；BUG-1030 `fixed/closed`、BUG-1031 `verified/closed` | APPROVED_BY_USER: RUNTIME_CLOSED | 无；健康不替代未来 release identity，但本批 Issue 不重新打开为实验前置 |
| F-033 | `qe_log_store.py`、BUG-1031、本文 9.9.3/10/17 | validation-receipt: merge `573e5380…`；五个 1 GiB 环形槽、首事件前零文件写入、旧文件只读 tail 回退；BUG-1031 `verified/closed` | APPROVED_BY_USER: RUNTIME_VERIFIED_CLEANUP_SEPARATE | 本文不执行日志删除；任何遗留文件清理仍是精确路径、独立授权动作 |
| F-034 | MA-E16 Loop7～12 `conf.yaml`、本文 2.5/9.9.2～9.9.3/12/14、Phase G0-I | validation-receipt: 固定 train/valid/test 与 benchmark-null 配置只读审计；fixed/model-age-slice/expanding/rolling、matured-label cutoff、purge/embargo、cadence/hash 合同；当前没有 refit run 或因果结论 | DESIGN_READY_STALENESS_DIAGNOSTIC | 无 |
| F-035 | 本文 2.5/9.9.2～9.9.3/12/14、Brinson 来源 | validation-receipt: absolute/active companion 字段、冻结 benchmark/等权对照/PIT sector weight identity、NOT_COMPUTABLE 处理；既有 MA-E01～E16 多为 absolute 口径，尚未生成 Brinson 结果 | DESIGN_READY_BENCHMARK_ATTRIBUTION | 无 |
| F-036 | 本文 9.6/9.9.2～9.9.3/12/14、Advisory Phase 8 | validation-receipt: tail label/episode/censoring、NDCG/recall/calibration、OOF meta-label take/skip/size/continue-hold 与主候选召回分层；新 LTR/tail/meta-label 尚无 run | DESIGN_READY_TAIL_META_ROLE_SEPARATION | 无 |
| F-037 | 本文 9.9.2～9.9.3/12/14、TRA/MIGA 来源 | validation-receipt: OOF prediction/state、bounded gate、固定权重/LOO、同风险/成本/窗口、测试期只推理与无平台重写合同；learned gate 尚未实现或验证 | DESIGN_READY_CONDITIONAL_GATE_CANARY | 无 |
| F-038 | 本文 4.6/9.9.3/12/14、DSR/PBO 来源 | validation-receipt: 完整 strategy family ledger、失败/放弃项、有效独立试验数、walk-forward/CSCV/PBO/DSR 适用性与结果；既有 MA-E01～E16 只消费当前可读台账并标注缺失，未来 trial 前瞻完整记录；样本不足时 `NOT_COMPUTABLE`，不启动历史平台项目 | DESIGN_READY_STRATEGY_SELECTION_BIAS | 无 |
| F-039 | 本文 9.5/9.8/9.9.2/12/14、Phase G0-G/I | validation-receipt: L2 主 taxonomy、可用时的 PIT L1、membership/version/hash、板块规模分布与 NOT_COMPUTABLE；L1/概念仅消费已有可证明冻结文件，缺失不触发 DB/历史采集/回填 | DESIGN_READY_TAXONOMY_SENSITIVITY | 无 |

## 16. Rollout / Rollback / 发布回滚

- Gate-0/运行时状态：基础代码、2026-06-30 QE 数据、长标签支持和 R7 combine-backtest 修复已经分别完成；合并、数据部署、实验成功和生产启用仍是不同事实。
- v5.0 文档 rollout：继承 v4.7 的无研究门禁原则与 v4.8/v4.9 的 Loop 级补账和 LSTM 主线；补齐 R8A 12/12、R8B 6/6、18 个 Loop 的预测资产、三种子 ensemble、分段稳定性、板块暴露、右尾漏捕和因子归因，并重排 F-014、R9S、右尾/板块状态、四格 oracle、R8C 与 R8B2 的资源优先级。本文只更新蓝图，不修改代码、DB、数据或运行时；任务2代码另立工作树和 PR。
- v5.2 F-014 Phase 1 rollout：本 changeset 交付完整性审计后的纯计算、严格 QE reader、entry/exit evidence bridge 和定向 tests；没有 route、startup、scheduler、DDL、DB、CAS 或 UI 连接，默认不会自动运行。完整 F-014 继续按 architecture v1.5 的 Phase 2–5 交付；source PR/merge、真实 R8 评价启动和平台接入保持分离，文档不预写尚未发生的 GitHub 状态。
- v5.3 研究进度 rollout：补入 R9S 24/24、R10 48 个成功 Loop、R10 source archive 修复状态、R11A/R11B 运行进度和 BUG-730 源码合入/运行时待加载事实；将图模型从静态 h20 关系预测扩展为正在执行的 h40/h60 基线，以及后续板块时序动态图、回撤 hazard、sector gating 和动态退出。本文只更新蓝图，不写 DB、不启动或中断实验、不重启服务、不启动 F-014 evaluation，也不触发非 QE 模块。
- v5.4 研究进度 rollout：补入 R10 51/51、R11A 9/9、R11B 6/6、BUG-741/PR #2391 的 results-only 恢复事实和 R11 Archive 15/15；增加 hold10/20/30 matched-seed、EfficientGATs h40/h60 完整均值及 cooperative-execution canary、sector-risk overlay、sector gating 和跨任务 portfolio fusion 顺序。R12G `qe_20260718_040323_9a4a` 与 R12P `macb_453ca2d0c5b21b40_20240701_20260629_20260717T201644965348Z` 已启动；文档与实验均保持 QE-only，不触发非 QE 模块。
- v5.5 多 Alpha foundation rollout：补入 R12G 1/1 完成与 R12P 最新 run 的 orchestration timeout 事实；新增第 4.11、9.9.1、Phase G0-H、F-022 和 `multi_alpha_qe_evolution_foundation_f2_design_20260718.md`，把 durable orchestration、lifecycle/recovery、QE 同风格创建器和 child observability 列为基础研发 P0-1～P0-4。本 changeset 只更新设计文档，不执行 DDL、不修改代码、不创建/恢复实验、不写 DB、不重启服务；未来实现不在 DDL 前额外导出数据库。
- v5.7 基础研发收口与 F-014 Phase 2 rollout：P0-1～P0-4 已通过 #2464/#2509/#2580/RD-Agent #6/#2593 合入并完成 DDL、重启、API/SSE/readback 和 CLI Playwright 运行验收，#2606 已将状态 close-sync 为 runtime verified。下一正式开发任务为 `qe_long_trend_evaluation_phase2_compute_cas_f2_design_20260722.md` 定义的真实 resolver、node worker、资源恢复、专属 CAS 与 compact receipt；本次文档更新不修改代码、DB、数据或运行时，不创建实验，也不增加科研门禁。
- v5.8 F-014 Phase 2 formal-review rollout：修复 indicator `_obj.pkl` 权威、normal adapter 时序、AIstock control ledger、qelt resource identity、environment binding、CAS required/去重和 pickle trust boundary。Phase 2 实现将新增一张 `run_evaluation` control migration；metric/artifact 两表仍属 Phase 3。当前只更新设计，不执行 DDL、DB 写入、服务重启、CAS 写入或实验。
- v5.9 F-014 Phase 2 current-state rollout：记录 AIstock #2630/#2643、RD-Agent #7、DEV/生产 control DDL 与 8001/9000 route 已完成；记录 R8B Loop4 首次真实 canary 的 pre-job `QELT_BUNDLE_INVALID`、BUG-837/838 修复状态、108 Loop 的 57 direct + 51 rematerialize 分类，以及成功 worker/CAS/restart 证据仍待后续运行。本 changeset 只更新当前事实，不执行 DDL、DB 写入、服务重启、canary retry 或非 QE 动作。
- v5.10 F-014 Phase 2 real-canary rollout：记录 RD-Agent #8 与 AIstock #2654/#2668/#2670/#2672/#2674 已合入加载；R8B 6/6 已按单 CPU 槽完成唯一 worker attempt、六族评价和 CAS manifest 发布，后端重启后 Loop4 相同请求幂等回放未增加 attempt，原 QE task/Loop/Archive 不变。下一执行顺序为其余 51 个 direct Loop 单槽评价、51 个 Loop 的不重训 rematerialize、Phase 3–5 明细表/API/MCP/UI；顺序只用于资源与操作效率，不构成研究门禁。本 changeset 只更新文档，不执行 DDL、服务重启、提交合入或非 QE 动作。
- v5.11 R12P recovery rollout：记录 BUG-864/#2736、BUG-865/#2740、BUG-867/#2744 及对应 close-sync 已合入并在 2026-07-26 重启后加载；当前 successor `macb_recovery_e45023af793b9a29d2acfc8738560b4f5addfc1c81e3fff5b064d9ef2cdc243c` 为 3 succeeded + 18 not_recovered，其中 8 项已确认可精确恢复、10 项保留原始证据缺口。下一顺序为完成 8 项 append-only 恢复、分析 equal/orthogonality-aware 与任务级 LOO、再启动板块回撤预警 overlay；顺序用于避免重复分析，不构成科研门禁。本 changeset 只更新蓝图，不执行恢复、实验、DB、DDL、服务重启或非 QE 动作。
- v5.12 R12P final-analysis rollout：记录最终 successor 11/21、baseline/equal/orthogonality-aware 指标和八条任务级 LOO；equal 当前 trial 相对 LGBM baseline 的 CAGR/Sharpe/Calmar 增量为约 `+20.90pp/+0.3204/+0.9051`，LGBM 是最稳定贡献腿，其他腿的边际依 scheme 变化。sector-risk overlay 正式进入 F2 设计/实现，不使用 HMM 新买入过滤冒充动态退出。本 changeset 只更新蓝图，不执行新实验、DB、DDL、服务重启或非 QE 动作。
- v5.13 F-014 Phase 3 single-canary rollout：记录用户授权后对 R8B Loop4 `qelt_89331d…` 执行 existing-CAS 物化，在当前 `8001` 连接的 `aistock` DB 写入 2,004 metric + 6 artifact；detail/quality API 与 bounded MCP readback 非空，exact replay 保持 row version、更新时间、主键范围、摘要和 evaluation 数量不变，且 `ready_for_node=false`。本次无 DDL、训练、回测、worker 提交、服务启停或批量补算；其余 5 个 R8B 与 Phase 4–5 单列。
- v5.14 F-014 Phase 4 Loop/Archive UI rollout：记录 Loop 长期趋势页签、唯一幂等操作、DB 状态恢复、五期限/三 barrier/episode/execution/sector/artifact 展示，以及 Archive 同 outcome snapshot 的 run/model/seed/factor-set 对比；mock Playwright 2/2、当前 8001 live Archive 只读 smoke 1/1。源码只增加 bounded query/filter 与 UI，不执行 DDL/DML、评价 POST、服务启停、训练、回测或批量补算；任务创建 profile、历史输入预览和 Phase 5 继续单列。
- v5.15 F-014 PR #2875 formal-review correction：撤销 Phase 4 `verified` 结论，记录 catalog command key/3014 两个 P0、Loop as-of/family failure evidence/coverage/censoring 缺口、Archive execution-quality 与 horizon 下推缺口，以及 Static gate/CI verdict 失败。修复顺序与父设计 v1.12 一致；本次只修订文档，不修改功能源码、DB、运行时或研究结果。
- v5.16 F-014 PR #2875 third-review rollout：记录 v1.12 阻断全部关闭，并纠正 execution status 与 evidence quality 的语义混用；source/CI verified 与当前 8001 旧 OpenAPI、其余 Phase 4/5、历史物化分别记录。本次不执行 DDL/DML、服务启停、评价或批量补算。
- v5.17 F-014 runtime readback rollout：记录 PR #2875 已以 `5413d799…` 合入、root/branch/worktree 清理完成，以及用户重启后 8001 OpenAPI、固定 R8B Loop4 evaluation list/detail/Archive、`rank_ic + horizon=60` 与 3000 HTTP 只读回读。旧 receipt 对新增 evidence-level 过滤返回空集；浏览器运行时无可用实例，所以前端逐控件可视验收仍单列 pending。本次不执行 DDL/DML、依赖安装、服务启停、评价 POST 或批量补算。
- v5.18 F-014 Phase 4 follow-up source：新增 nullable task profile migration 三件套与 init mirror；标准/fork/strategy/custom 新 task identity 使用同一注册 profile，既有 task 禁止变化；四条 builder 与 executor 在节点权威 root 上激活 normal postprocess；独立只读预检列出 dataset/Recorder/artifact 可用性与 data action，UI 明示 technical readiness 非研究门禁并始终保留创建入口。正式审核已补齐正常 catalog 下 order/trade 显式缺失行和 summary task profile projection。因 init mirror 触发 changed-file HIGH guardrail，AC 文案仅澄清既有显式 TWAP 分支，不改变执行代码、默认值或配置。聚焦后端更新为 92 passed；修复 commit `934bd422…` 已推送到 PR #2906。fresh CI、生产 DDL、运行时、live browser、其余 R8B 和 Phase 5 均未执行。
- v5.19 F-014 Phase 4 follow-up post-merge sync：PR #2906 required checks 15/15，通过 squash merge `bba48911342a3bee51e4339d597d9b2a5b85d71a` 合入；root `main` 已同步包含该 merge。source merge/CI/root sync 已完成；production DDL、runtime activation、real Recorder preview、live browser、其余 5 个 R8B 与 Phase 5 均保持独立 pending，source worktree/branch 保留等待单独清理授权。
- v5.20 F-014 Phase 4 production DDL rollout：PR #2935 已以 `005e0e8a…` 合入 post-merge 状态同步；DEV 重新完成 forward/readback/reapply/guarded rollback 并恢复零残留，生产 `127.0.0.1:5432/aistock` 已应用 task-profile migration。重连 readback 确认 nullable `text`、精确 column comment、114 条既有 task 全部为 `NULL`，业务 DML 0 行。runtime activation、real Recorder preview、live browser、其余 5 个 R8B 与 Phase 5 继续独立 pending。
- v5.21 F-014 Phase 4 runtime preview rollout：截至 2026-07-30 该轮核验时，8001/3000 进程启动于 2026-07-30 11:37 +08:00；8001 health/OpenAPI 已加载 profile/preview 合同。固定 task `qe_20260715_104922_001d` Loop 4 对真实 snapshot 的 GET 返回 200、8/11 输入可用，feature identity/orders/trades typed missing，`ready_for_node=false`、`research_gate=false`、7 个 side-effect flags 全 false；DB before/after 保持 17 evaluation/2,004 metric/6 artifact。3000 `/qe-archive` HTTP 200，但 browser runtime 列表为空，逐控件 visual 与 normal Loop smoke 继续 pending。
- v5.22 F-014 current-process correction（历史）：2026-07-31 当时的只读复核确认 8001/3000 进程均启动于 01:55 +08:00；该重启由该次文档任务之外的操作完成。该进程时间与接口快照不代表 v6.0 当前运行态，也不恢复已终止的 visual/normal Loop smoke。
- v5.23 F-014 human-search acceptance：PR #2964 将 Run/Task/outcome snapshot/L2 sector 改为业务检索选择器，canonical ID 仅作隐藏 value；操作员视图移除 raw JSON/人工 ID 输入，Top-K 展示真实完整/缺失覆盖而不以首条历史缺失冒充整体 missing。AIstock CI `30598333361` 的类型、Lint、四组 QE 后端门禁和 Playwright Chromium 全部通过，artifact `frontend-ui-evidence-30598333361` 含成功截图与 HTML report。当前 3000 runtime visual、normal Loop、完整 runtime/root SHA 对齐、其余 5 个 R8B 与 Phase 5 继续独立 pending；本轮未执行 DDL/DML、服务启停、评价 POST 或历史回填。
- v5.24 strategy-evolution-first rollout（历史）：用户终止 v5.23 及更早版本仍列作未来动作的 UI visual、normal Loop 平台 smoke、其余 R8B、Phase 5、历史补算/物化、Archive 补录和总账固化；这些边界继续有效。该版本提交四个 MA-E01 policy run、共 8 个 child；当时结果仍在运行，现已由 v6.0 的 MA-E07～MA-E13 终态总账替代。
- v6.0 regime-and-orthogonal-alpha rollout：更新 MA-E07～MA-E13 终态指标，确认 MA-E07/MA-E10 为全窗口 Pareto、MA-E12 新增腿退化、MA-E13G 静态关系属于正则角色、MA-E13R 近期 Alpha 偏弱。活动方向改为三腿 regime policy、压缩扩张/参与差/领导耗竭独立专家、多模型分歧动态权重和动态关系 hazard；基本面扩散/右尾多任务为 P1，概念/高成本图架构为 P2。本文变更仅更新设计与活动快照，不提交实验、不修改代码/数据/DB/依赖/运行时。
- v6.1 matched-observation rollout：记录 MA-E14 三个真实窗口 run 退化、MA-E15 9/9 双节点结果、CE3/CE4-B 额外 1,511 行及其 Top20 影响；补充 MA-E15 全部 V25_1 组合指标受开放 BUG-852 限制，信号层与执行层分开记账。BUG-1006 以 opt-in `qe_observation_panel_v1` 固定参考因子交集，排除策略 kwargs 泄漏，matched 缺结果/缺列/空面板 fail closed，并保持无契约历史 task 的 partial concat。MA-E16 预注册为两节点 12 Loop、统一日频 `CLOSE_PRICE`，绕开但不关闭 BUG-852。源码/测试/文档作为同一 BUG PR 交付；PR 合入、用户重启、MA-E16 提交和实验结果仍分别报告。本次不执行 DDL/DML、依赖安装、服务启停、数据切换、历史补账或非 QE 动作。
- v6.2 MA-E16 runtime-resource rollout：记录权威 task `qe_20260810_224522_b0d9` 的 9 completed/2 failed/Loop12 running 快照、重复 task 的暂停边界和 Loop7 重叠窗口；以 Windows 原生 pages/sec、WSL 进程树/RSS/swap 证明根因是 GeneralPTNN 多进程 loader 与跨 task 全局容量 2 共同放大的换页压力，不是图模型。BUG-1014/#3266 将 WSL 全局容量收紧为 1，本地 GeneralPTNN 改为单进程无 pin/prefetch 并设置线程上限，普通 composer prepare/qrun/read 复用统一凭据清洗；远端 4 槽、batch/标签/模型/策略和当前在途实验保持不变。源码合入、用户重启、运行态验证与失败 Loop 重试分别报告；本次无 DDL/DML、依赖安装、数据切换、历史补账或后端/实验进程控制。
- v6.3 MA-E16 terminal-and-recovery rollout：把权威 task 更新为 05:45 终态 10 completed/2 failed，保留两个失败原因及成功 arm 的同面板/同窗口/同 label SHA 证据；记录四组现有 matched pair 的 CAGR 正增量、IC/RankIC 混合和 MDD 3/4 恶化，不把不完整结果写成 breadth 独立 Alpha 或 blend/LOO 结论。BUG-1014 PR #3273 先安全同步、CI 与合入，再由用户重启验证；日频 qrun MLflow 空指标竞态独立登记，Loop3 强制 `backtest_only`、Loop7 强制 `full_train` 单 arm。新增 `sector_participation_gap_v2`、`leadership_exhaustion_v1`、`sector_residual_cohesion_break_v1`、`dynamic_residual_flow_relation_v1`、`pit_fundamental_diffusion_v1` 的角色与顺序，但本轮不启动实验。无 DDL/DML、依赖安装、数据切换、历史补账或后端/实验进程控制。
- v6.3 BUG-1014 source-merge / BUG-1022 source-fix rollout：RD-Agent PR #18 已以 `da3270f8…` 合入，AIstock PR #3273 已以 `49ccc1f3…` 合入并同步 root；两种运行态仍完全 pending。日频 runner 竞态登记为 BUG-1022/Issue #3282，只复用分钟 runner 已验证的 bounded async-write barrier 与 exact empty-metric retry，并要求两 runner 同一测试矩阵。本轮不执行 RD release/deploy、任何用户重启、Loop3/7 恢复或新演进实验。
- v6.4 runtime-quieting / bounded-live-log rollout：BUG-1022/1023/1024/1026 source 已分别以 `a7bc06a3…`/`d8e80b9f…`/`3a6e3290…`/`e2fdaaab…` 合入；RD-Agent BUG-1030 source 以 `795973e1…` 合入，AIstock 跨仓记录以 `aee31e67…` 合入；BUG-1031 broker/frontend source 以 `573e5380…` 合入。以上仅是实际实验暴露的主机/数据库/日志阻断修复，不新增监控或日志平台；与 BUG-1014 一样仍保持 runtime pending。本轮未部署/重启、未恢复 Loop3/7、未启动新实验、未删除旧日志。
- v6.5 current-progress-and-return-ledger rollout：2026-08-12 实时回读确认 MA-E16 仍为最新演进 task、10 completed/2 failed、Loop3/7 未恢复；记录 backend identity `0e0afed2…` 已包含 AIstock 侧相关修复、两个 RD-Agent 节点 `/health=ok`，并把 BUG-1026/1030/1031 closed 与 BUG-1014/1022/1023/1024 close-sync pending 分开。新增 R12P～MA-E16 收益总览和 MA-E16 全 12-arm absolute-return 明细；失败 arm 保留空值，MA-E15 继续标记执行真实性受限，MA-E16 继续标记不完整。本 changeset 只更新蓝图，不执行实验、DB/DDL/DML、依赖、进程控制、日志删除或其他平台工作。
- v6.6 bottleneck-identification-and-evolution rollout：继承 v6.5 的全部进度、收益和运行态事实，不制造新 task 或新结果；把 MA-E16 收口后的 P0 改为 model-age/refit、四格 sector oracle、benchmark/Brinson 三联诊断，并按结果触发 DoubleAdapt/Proceed、TRA/MIGA gate、两层板块/lead-lag、LTR/right-tail/hazard/meta-labeling 或横截面/正交分支。F-018 升为 P0 诊断，participation gap/leadership exhaustion 改为并行 P1；新增 F-034～F-039 的证据合同、策略级选择偏差和 taxonomy 敏感性。论文只作先验；设计矩阵为 `DESIGN_READY_*`，实验状态明确为尚未运行并在总账独立记录。本 changeset 只更新蓝图，不执行实验、DB/DDL/DML、依赖、进程控制、数据切换、客户端安装、日志删除或平台工作。
- v5.6 多 Alpha foundation design audit：修正未经逐项确认的批准标记、stop/pause 语义、child `not_computable` 和聚合规则、跨 QE 路径容量统计、状态/event 原子性、Archive 静默初始化、schema 的 QE-scoped failure 以及规范 UI 入口/逐像素视觉验收。该修订仍只更新设计文档，不执行 DDL、不修改运行代码、不创建/恢复实验、不写 DB、不重启服务。
- Schema rollout：现有 factor h20 指标已可用；F-014 Phase 2 control table 与 Phase 3 metric/artifact 表已在当前 runtime DB ready/readback，Loop4 已形成非空数据；未来目标特定 DDL 仍独立授权，依赖既有每日备份，不新增导出门禁。
- Data rollout：R8–R12 当前继续冻结 2026-06-30 QE 快照；任何新快照另立 dataset identity 并保留上一版本回滚，不影响非 QE PIT/模拟盘数据。
- Rollback：文档按 PR revert；未来 evaluator/schema 可停止新写入并保留历史 receipt，数据回切上一版本；任何回滚不得删除试验台账、预测或评价制品。
- Runtime rollback：本文不触发运行时动作；未来 F-014/R8 实现必须另写启动前检查、QE-only zero-impact 与恢复步骤。

## 17. Production Gates / QE-only 唯一硬边界与实施状态（兼容工作流标题，不定义科研门禁）

| 项目 | 当前状态 | 说明 |
|---|---|---|
| source merge | BUG1014_1022_1023_1024_1026_1030_1031_SOURCE_MERGED | BUG-1006 已合入并由 MA-E16 实际消费；BUG-1014 两仓 source 与 BUG-1022/1023/1024/1026/1030/1031 source 均已合入。当前 backend identity `0e0afed2…` 已包含 AIstock 侧相关修复；RD release/健康、frontend 与逐 BUG close-sync 继续分项报告。 |
| QE dataset | VERIFIED_20260630_FILE_ONLY | 当前 QE 快照与冻结 sidecar 已支持 R6～MA-E16，MA-E16 的 12 arm 均复用同一 identity；未来数据切换继续要求版本化 identity/hash 和上一版本回滚，实验运行期禁止数据库访问。 |
| 唯一硬边界 | QE_ONLY_ZERO_NON_QE_IMPACT | 所有实验、评价、缓存、CAS、表、API/MCP/UI 和写入仅限 QE；不读取、修改、调用或影响任何非 QE 模块。 |
| factor asset | RESEARCH_AVAILABLE_IN_QE | catalog 1525/1528/1532 可供 QE；本蓝图不接入荐股、模拟盘或生产交易。 |
| QE schema | EXISTING_SCHEMA_SUFFICIENT_NO_HISTORY_MATERIALIZATION | Multi-Alpha P0 与 F-014 既有表满足当前演进；Loop4 的 2,004 metric + 6 artifact 只读保留。不再物化其余 5 个 R8B，也不为历史完整化新增 schema。 |
| QE research writes | NEW_TRIAL_MINIMUM_OUTPUT_ONLY | 既有 R9S～MA-E16 结果只读复用；后续 trial 仅写入运行自然产生的版本化 candidate、输入身份、指标、成本、风险和失败原因，不执行历史补账、状态重算或总账固化。 |
| frontend/backend dependency | noop | 本批无依赖或 lockfile 变化。 |
| production DDL/DML | noop | 本批无 migration、DDL、DML 或数据库写入。 |
| candidate snapshot | VERIFIED_QE_20260630 | R6～MA-E16 使用该快照及对应冻结 sidecar；MA-E16 不覆盖数据集，其他快照必须另立 identity。 |
| QE experiment | MA_E16_TERMINAL_10_COMPLETED_2_FAILED_NO_NEW_TASK | MA-E14/MA-E15 已终态；MA-E16 于 2026-08-11 05:45 终态为 10 completed、Loop3/7 failed。2026-08-12 实时列表仍无更新的演进 task；成功结果与失败空值均已写入第 2.5 节。 |
| strategy evolution priority | RECOVER_MATCHED_MATRIX_THEN_P0_DIAGNOSTIC_TRIAD_AND_RESULT_TRIGGERED_P1 | 唯一 P0 是最佳新策略包演进。BUG-1014/1022/1023/1024 只补 receipt/close-sync，不再开发；立即 Loop3 `backtest_only`、Loop7 `full_train` 单 arm并分析完整同面板 CE3/CE4-B 双模型三种子。其后执行 model-age/refit、四格 sector oracle、benchmark/Brinson 三联诊断，再按结果触发适应/门控、两层板块/lead-lag、右尾/退出或横截面/正交 P1；participation gap/leadership exhaustion 不阻塞诊断。 |
| multi-alpha platform | P0_1_TO_P0_4_COMPLETE_MAINTENANCE_ONLY_RUNTIME_QUIETING | durable orchestration、控制/恢复、创建器、运行网格和重启 readback 已满足演进；BUG-1014/1023/1024/1026/1030/1031 只收口真实 trial 暴露的主机资源、空闲查询、重复日志连接与无界文件阻断，不恢复通用平台研发。 |
| service/runtime restart | USER_RESTART_COMPLETED_RUNTIME_SOURCE_LOADED_CLOSE_SYNC_PARTIAL | 当前 backend identity 为 `0e0afed2…`；WSL/远端 RD `/health=ok`。BUG-1026/1031 已 `verified/closed`，BUG-1030 已 `fixed/closed`；BUG-1014/1022/1023/1024 仍缺逐 BUG receipt/close-sync。本文未启动、停止或重启任何用户进程，健康不冒充 commit identity。 |
| live log retention / cleanup | FIVE_BY_ONE_GIB_RUNTIME_VERIFIED_CLEANUP_SEPARATE | BUG-1031 已 `verified/closed`，五个 1 GiB 环形槽与零订阅者零上游/零写入合同生效。本文不删除旧日志；任何遗留文件清理仍须精确路径、size/mtime 回读与独立授权，且不得阻断实验。 |
| F-014 CAS | R8B_6_OF_6_PUBLISHED_AND_HASH_VERIFIED | 根目录为 `F:\Dev\AIstock\rdagent_assets\long_trend_evaluation_store`，与 Prediction Store 分离且不在 `E:`；R8B 六个 manifest 及 required artifacts 已完成 hash/size/row-count 回读。 |
| paper/live trading | NOT_ENABLED | 不属于本规格自动动作。 |

## 18. Research Sources / 一手研究来源

以下来源只用于形成 Gate-0 研究先验与统计治理，不能替代 A 股、PIT、成本后样本外证据：

- [Harvey, Liu & Zhu, “…and the Cross-Section of Expected Returns”](https://www.nber.org/papers/w20592)：因子动物园、多重检验与更高发现阈值。
- [Harvey, Sancetta & Zhao, “What Threshold Should be Applied to Tests of Factor Models?” (2026)](https://www.nber.org/papers/w34898)：依赖检验、原假设分布、样本选择与 local FDR；`t≈3` 仅作治理参考。
- [Bailey et al., Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253) 与 [Bailey & López de Prado, Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)：PBO/DSR 与选择偏差治理。
- [Research Affiliates, A Backtesting Protocol in the Era of Machine Learning](https://www.researchaffiliates.com/insights/journal-papers/702-a-backtesting-protocol-in-the-era-of-machine-learning)：其受保护测试集、经济逻辑与可复制协议形成本文预注册/untouched test 的实施推论，并非论文直接规定 AIstock 字段。
- [López de Prado, K-Fold CV with Purging & Embargo / CPCV 方法索引](https://www.quantresearch.org/Innovations.htm)：形成 h20 split manifest、purge 与 embargo 契约的实施依据。
- [Newey & West](https://www.nber.org/papers/t0055) 与 [Politis & Romano stationary bootstrap](https://doi.org/10.1080/01621459.1994.10476870)：重叠 h20 的自相关稳健推断和时间序列重采样。
- [Moskowitz & Grinblatt, Do Industries Explain Momentum?](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00146)、[Ehsani & Linnainmaa, Factor Momentum](https://www.nber.org/papers/w25551) 与 [Hou, Industry Information Diffusion](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=463005)：行业动量、因子持续性和信息扩散的控制基线。
- [Campbell & Lettau, Dispersion and Volatility](https://www.nber.org/papers/w7144) 与 [Barberis, Shleifer & Wurgler, Comovement](https://www.nber.org/papers/w8895)：区分行业/个股离散度、波动与非基本面共振。
- [Frazzini, Israel & Moskowitz, Trading Costs](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3229719)：机构级交易成本、冲击和规模依赖。
- [Zaremba et al., Herding for profits: Market breadth and the cross-section of global equity returns](https://www.sciencedirect.com/science/article/pii/S0264999319312982)：论文使用上涨股减下跌股类 breadth，只支持“成员参与值得检验”的先验，不直接验证 MA20 breadth。
- [MSCI Integrated Factor Crowding Model](https://www.msci.com/research-and-insights/paper/msci-integrated-factor-crowding-model) 与 [Lazo-Paz, Moneta & Chincarini](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4618248)：其多维拥挤框架形成本文持仓、资金流、成本与尾部风险联合审计的实施推论，不声称复刻 MSCI 模型。
- [Novy-Marx, Backtesting Strategies Based on Multiple Signals](https://www.nber.org/papers/w21329) 与 [Gu, Kelly & Xiu, Empirical Asset Pricing via Machine Learning](https://www.nber.org/papers/w25398)：组合信号的选择偏差、非线性交互与模型增量。
- [Shin, 2026 preprint](https://arxiv.org/abs/2606.19550)：测试资产构造可能改变模型排名；仅作为前沿敏感性提示，不作为已确立结论。
- [DoubleAdapt: A Meta-learning Approach to Incremental Learning for Stock Trend Forecasting](https://arxiv.org/abs/2306.09862) 与 [Proceed: Proactive Model Adaptation Against Concept Drift](https://arxiv.org/abs/2412.08435)：分别提供股票增量适应和带预测反馈延迟的时间序列漂移机制先验；AIstock 必须先与简单 rolling/expanding refit matched 对照，不能把论文结果直接写成 A 股收益。
- [Temporal Routing Adaptor (TRA)](https://arxiv.org/abs/2106.12950) 与 [MIGA: Mixture-of-Experts with Group Aggregation](https://arxiv.org/abs/2410.02241)：支持多交易模式/风格专家与 learned routing 的研究假设；只在现有腿显示条件互补后运行严格 OOF gate canary，不能解决所有腿共同漏捕的股票。
- [MASTER: Market-Guided Stock Transformer](https://arxiv.org/abs/2312.15235)：为跨时股票关系和随市场变化的特征有效性提供结构先验；成本与目标并不直接对准当前右尾/板块层诊断，因此保持结果触发后的 P2/P1 条件 canary，而不是 P0 架构重写。
- [Meta-Labeling: Theory and Framework](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4032018)：为 take/skip、position sizing 与 false-positive 过滤提供方法先验；本文明确其只能处理主模型已提出的候选，不能冒充右尾召回或新 Alpha 生成。
- [LambdaRankIC: Directly Optimizing Rank IC for Financial Prediction](https://arxiv.org/abs/2605.00501)：2026 年直接优化全排序 RankIC 的候选方法；当前瓶颈是右尾捕获与收益转换，故仅作全排序对照，不以“最新”替代 NDCG@tail、Top-decile、barrier/hazard 目标。
- [CausalStock, NeurIPS 2024](https://proceedings.neurips.cc/paper_files/paper/2024/hash/54d689d58fe54c92aee2d732fc49fca8-Abstract-Conference.html)：为新闻驱动、多股票 lag-dependent causal relation 提供先验；需要可证明的 PIT 新闻/事件文件，当前不得触发数据库回填、历史采集平台或概念关系前置建设。
- [Brinson, Hood & Beebower, Determinants of Portfolio Performance](https://rpc.cfainstitute.org/research/financial-analysts-journal/1995/determinants-of-portfolio-performance)：提供 allocation/selection/interaction 归因框架；本文只借其拆分板块主动配置与板块内选股，不外推其养老金样本结论，也不替代绝对收益、成本和可交易性评价。

内部设计锚点（用于约束 AIstock 实现，不替代一手论文证据）：

- `docs/analysis/p2_relational_model_hist_master_feasibility_20260708.md`：HIST/MASTER/IGMTF/TRA 的早期接入评估；其中静态关系快照建议已由本规格第 4.10 节取代。
- `docs/architecture/qe_efficient_gats_l2_industry_embedding_f1_design_20260710.md`：真实 PIT 申万 L2 provider、embedding 与 industry bias 的同源契约。
- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` Phase 8：Type B 长期趋势的多期限、有序目标、生存、MFE/MAE 与捕获率口径。
- `docs/analysis/sector_rotation_factors_batch_e_plan_20260711.md`：当前板块因子批次的后续候选与执行衔接。
