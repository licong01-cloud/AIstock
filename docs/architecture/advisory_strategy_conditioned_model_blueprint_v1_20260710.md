# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v3.20

> 初始日期：2026-07-10
> 修订日期：2026-09-01
> 文档类型：F2 顶层架构蓝图，`docs-fast-update`
> 当前状态：`P0_RESEARCH_FAMILY_FROZEN_N1_FORMAL_COMPLETE_N2A_MERGED_N2B_V1_ABORTED_NO_EVIDENCE_V2_SOURCE_READY_FORMAL_RUN_PENDING`
> 当前能力基线：Top5、收益/周期、价格范围和页面/API 四类组件已由真实模型实现；两个 ENABLED Program 均已形成真实每日 `PUBLISHED` 推荐、target-open settlement 和 active episode。P0-D exact bundle 已通过安全 descriptor rotation 接入并完成真实在线 shadow 推理；自动成熟结算/指标闭环已随 PR #3697 合入。自然 future OOS 仍按交易日积累，同时新增历史虚拟前向验证以解除每次模型演进必须等待20个自然交易日的阻塞，但两类证据严格分开
> 当前源码/运行时：P0-A/P0-B/P0-C/P0-D、descriptor rotation/maturity修复、forward evaluation、历史虚拟前向、P0-E至P0-L Stage A和N0控制面均已进入`main`。N1真实开发窗口bundle `74827d03...`、两条registry记录和N2路线均已闭合，源码PR #4014已合入merge commit `cfc490a1...`并完成清理。N2-A formal clean compute commit为`30a8a74e...`、不可变bundle为`6784df1a...`；源码PR #4048已合入merge commit `55d376b3...`并完成worktree/local/remote branch精确清理。生产descriptor仍指向P0-D exact bundle；N1/N2-A均为不可部署诊断，不修改baseline、Selection或运行时
> P0-J权威结果：正式request `advselpriorresreq_3a50e2f6fd9cf43cb1f6ad3e`在首条outer path的inner block 3形成完全平坦的decreasing-isotonic prior，按预登记条件以`ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE`停止；evidence-only bundle为`eb8ade9b...`，exact retry返回同identity，零trial、无winner/PBO/Stage B。该结果证明rank-to-return单调关系跨时间分区不稳定，不证明Selection全局无效
> P0-K权威结果：源码、PR/CI、合入和正式 Stage A 均已完成。request `advselgatereq_943f9e551d5fee35e57340cc`完成`168/168`，bundle为`fee9b561...`，结果`NEGATIVE_STOP_NOT_ADVANCED`且未激活。168条trial全部选择`0.4`、拒绝数均为0，策略与Selection恒等；liability日Spearman约`0.254589`，但约束选择器没有让信号进入决策。`PBO=1.0`来自六个arm的block分数完全相同和固定tie-break，不按普通过拟合解释
> P0-L权威结果：BUG-1251修复后的正式request `advp0lreq_b86425d3b5ce508904fa01b0`生成evidence-only bundle `4476afeb...`。第一条outer path的identity control精确复现P0-G但无真实干预；gain `12/8/4/1`分别产生`33/71/85/85`次实际entry变化并把OOF换手从`0.276692`降至`0.272180/0.272180/0.269173/0.269173`，均低于P0-D预算`0.299248`，但cash day从`1`增至`2`、active-slot coverage从`0.999248`降至`0.998496/0.997744`，不满足冻结完整性合同，以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`在`0/168`停止。结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner、无可计算PBO、无Stage B、无激活；exact retry返回同一bundle identity
> 最近一次已核验的生产前向状态：截至 2026-08-28 12:55，scheduler为running/thread_alive且无last_error；第一个Program有4条P0-D observation、最早成熟日为2026-09-22，第二个Program尚无模型observation。自然证据继续积累，不回填且不阻塞离线模型演进
> 当前模型质量：M5A/M5B/M5C均不建议激活；P0-D虽在CPCV相对matched Selection Top5提升`3.6556 bps`、path win rate `64.29%`，但历史虚拟前向累计净收益、回撤和换手均劣于Selection。P0-E至P0-L没有一个满足既定Stage A晋级合同。当前有真实工程能力、局部预测信号和局部指标改善，但没有已证明可稳定替换Selection的荐股主模型
> 演进结果：P0-D历史虚拟前向证明二分类概率不能稳定表达收益幅度；P0-E outcome weighting同样负向停止。P0-F/P0-G保留收益提升但未满足换手。P0-H把相对P0-D换手压低`0.022708`并改善MDD `0.010688`，但return head日Spearman仅`0.041731`、PBO `0.90`；P0-I/P0-J证明grouped rank与Selection rank收益先验跨分区不稳定。P0-K证明liability信号虽稳定，但绝对阈值策略退化为Selection identity；P0-L进一步证明在冻结P0-G anchor、最大位移1和每日一次相邻交换的动作空间内，真实干预会降低coverage并增加cash day，冻结可行集合为空。P0-D至P0-L研究族已经事实收敛并正式冻结，不再派生P0-M或继续同数据、同候选、同特征和同模型族的局部变体
> 历史验证执行方向：44 日 A/B/C v6 golden 已冻结；P0-D 历史虚拟前向复用正式 scorer 与 shared policy kernel，24决策日+20日tail的权威 artifact 为 `fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9`。该窗口现已被 P0-D 质量判断消费，不得在后续调模后继续标为新的 OOT
> N1权威结果：canonical PIT覆盖5067只股票/5077段；386个决策日、19300条Top50标签、382个可评价日。全市场Top5赢家的Top20/40/50平均召回仅`0.8808%/1.6062%/1.7617%`；Top20内perfect Top5成本后增量为`1314.27 bps/五槽日`，95%区间`[1136.61,1516.57]`，oracle为`HIGH/DIRECTION_GATE`。固定Ridge cross-fit增量为`98.83 bps/五槽日`，95%区间`[1.18,193.73]`，MDE `136.01 bps`，为`INCONCLUSIVE/EXPLORATORY`。综合typed结果为`INCONCLUSIVE__THEORETICAL_HIGH__LEARNABILITY_HIGH`、`direction_ready=false`；不得据此激活ranker或跳过N2
> N2-A权威结果：正式request `advalpha3req_e7295a31a4e1953e9048cec5`、bundle `6784df1a...`覆盖386日和1,710,301条共同signal outcome；父包与N1 ranking精确parity。LSTM/FUND/父包RankIC分别为`0.11677/0.05628/0.12284`，Top5 H20净超额均值为`397.89/245.73/446.52 bps`。父包相对FUND的RankIC与Top5增量区间均大于0，但相对LSTM的RankIC增量`+0.00607`区间`[-0.00223,0.01484]`、Top5增量`+48.63 bps`区间`[-103.99,186.53]`均跨0；现有Alpha主要由LSTM贡献，FUND较弱但不同源，固定IC组合尚无确认性证据优于LSTM。父包Top50召回虽为随机期望`1.75×`，绝对值仍仅`1.7617%`。结果为0-trial `ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`，sealed holdout未读，不支持调权、激活或生产替换
> 当前双轨目标：自然future OOS与H0同核执行优化继续独立运行；模型研究已完成N2-A现有父包信号归因。N2-B v1因第三包含经BUG-1302证实的PIT违规因子而退出，未发布bundle、未登记registry、未形成研究证据；v2以新lineage只扩展两条仍合格独立旧包的同窗预测审计，并在一个边界明确的辅助工作包中推进Entry Guard、Exit-label与QE上游无证据准备。候选来源和动作空间诊断齐全后只选择一条N3主线。Alpha/排名与Risk-managed Advisory使用独立目标合同、标签、激活状态和展示语义；动态资金仓位不在当前授权范围
> 最终决策者：用户人工决定是否买入；系统不下单、不形成交易执行输入

## 0. 权威边界与本次纠偏

本文档是 AIstock 荐股模型研发的当前顶层权威。用户最新明确要求优先于历史设计、历史实现顺序和旧任务状态。

本次修订纠正过去近三周的严重优先级偏移：此前开发把 Phase 1R 历史范围任务、Source Catalog、逐窗口哈希、capture/build/SEALED、CAS、lease/fencing、source revision、历史固化和完整证据链放在模型训练之前，导致真实模型和用户可见荐股能力均未实现。该顺序自本修订起废止。

M0-M5C 已完成模型组件、固定日期推理和三轮负面质量实验。自2026-08-16优先级纠偏起，后续工作分为互不阻断的模型/前向主线和历史验证执行线：

1. P0-A 建立每天自然向前运行的 baseline publish、challenger observation 和 episode 跟踪，不回填旧日期。
2. P0-B 同期解除单一目标常量，用 Program active binding 动态解析 exact bundle。
3. P0-C 直接读取现有 QE H5/Parquet/Qlib Bin 和目标策略包预测 PKL，构造 Top5 shadow policy episode 标签与 purged rolling/CPCV 评价。
4. P0-D 在 WSL Conda 训练真实 meta-label 模型并进入 challenger 前向发布；正式预测只读取数据库 decision-cutoff 输入。
5. P0-E 复用P0-C连续净超额收益，在每条CPCV path内用train-only幅度统计训练收益感知meta-label；模型选择只用冻结validation，已消费历史窗口只作回放诊断。
6. P0-F 复用P0-C连续净超额收益，使用train-only median/MAD和固定Huber regression直接学习policy utility；真实Stage A因换手门禁失败已负向终止。
7. P0-G只改变训练label并完成真实Stage A；相对P0-D仍因换手`+0.004096`负向停止，禁止Stage B、replay、runtime和结果后调参。
8. P0-H把收益和换手负担分头学习，在outer-train nested OOF模型输出上约束实际entry priority；outer validation只评价，不参与price、rounds或transform拟合。
9. P0-I只替换P0-H收益头为policy-aligned grouped rank head，同日预测百分位进入原output constraint；P0-H的liability、候选、policy、cost、CPCV和advancement保持不变。
10. P0-J不延续P0-I grouped-rank目标；它在每个inner-train内拟合Selection rank的非增单调收益先验，以Huber学习残差，并用同一outer-train的nested OOF解析式可靠度系数收缩残差输出，再进入原liability/output constraint。
11. P0-K停止收益排序建模，仅训练liability head；在inner OOF选择预冻结物理holding-day阈值过滤高换手ENTER候选，eligible集合内保持Selection顺序。
12. P0-L冻结P0-G收益anchor与P0-K liability head，用relative liability rank执行有界局部ENTER重排；源码、BUG-1251修复和正式Stage A均已完成，结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`，不进入Stage B。
13. P0-D至P0-L作为一个共享开发数据、候选、特征和模型族的研究族正式冻结；负面结果与已消费窗口保持原结论，不派生P0-M，不以结果后阈值、gain、family、seed或新loss改判。
14. 新主线先建立最小JSONL trial registry、可由其生成的单页活动路线和父包预测延伸可行性spike，再在开发窗口执行分层clairvoyant oracle与固定cross-fitted learnability audit；oracle、探索和模型trial分类计数，sealed holdout不参与诊断。
15. Tier 1完成后，N2辅助工作包先对当前父包的LSTM腿、FUNDGROWTH腿和exact IC加权组合执行一次固定三臂共同窗口Alpha信号审计，Top25/Top50不得重复计作信号，结果只用于导航。随后Entry Guard MVE与Exit-label oracle补齐Entry/Exit动作空间诊断；同一时刻只允许其中一个进入candidate训练/confirmation。QE上游alpha MVE准备件不产生研究证据、不计作实验线。候选来源与Entry/Exit诊断齐全后再按四象限只选择一条模型主线。前向residual成熟后执行P1-A，两个以上兼容包具备独立bundle后执行P1-B，长期趋势包就绪后执行P2。
16. H0以已冻结44日A/B/C结果作为golden baseline实施实盘单日/历史批量双执行形态；它只优化调度、数据读取、工作区复用和重复raw计算，不改变逐日业务逻辑，也不决定模型方向。
17. 上述真实功能、新模型路线和H0均不自动解禁历史补账、历史归档、ModelOps、旧任务清理、动态资金仓位或通用数据/缓存平台；这些任务仍必须由用户针对具体目标重新确认。

以下内容不再是模型训练、模型推理、页面展示或模型启用的前置条件；H0 也不得借此恢复无界历史平台建设：

- Historical Range/Phase 1R 全链路 DML。
- 为模型训练新建 Source Catalog，或对所有来源无差别执行逐日、逐窗口全量内容哈希。H0 可以复用既有冻结 catalog，并把校验优化为批次 full seal、chunk revision token、逐日实际读取 receipt 和异常时 full rehash。
- retrospective observation/capture/label bridge。
- 新建 SEALED base snapshot、CAS publish、blob reference、invalidation 或 GC。
- 把 checkpoint、lease、fencing、recovery successor 或 exact retry 证据闭合作为首次模型训练前置；H0 仅保留长回放自身必需的逐日 checkpoint、heartbeat 和 exact resume。
- 旧 batch、旧 artifact root、orphan build 和历史 operation 的处理。
- 2/3/5 年全部窗口、全部消融、全部种子和统计检验完成后才允许首次训练。

已有 Phase 0A、Phase 1、Phase 1R、Phase 0B 源码和历史事实保持可审计，不删除、不迁移、不归档。H0 只允许在证明直接消除当前历史回放重复计算时复用或定向重构其执行边界，不恢复已废止的训练前置路径。

### 0.1 H0 授权边界

H0 的权威详细设计为
`docs/architecture/advisory_live_daily_historical_batch_shared_kernel_f2_detailed_design_20260816.md`。

该授权只包含：

- 实盘 `LiveDailyExecutor` 保持每日单日运行；历史 `HistoricalBatchExecutor` 接受冻结日期区间并在持久 worker 中分块处理。
- 两种执行器共同调用唯一的 StrategyPackage 日信号、Selection/HMM/risk/tradability 和 Advisory list transition 语义；禁止复制第二套回测选股算法。
- 静态模型/因子工作区按内容寻址复用；日期动态数据、PIT 视图、结果和 receipt 仍逐日隔离。
- A/B 仅在 raw-affecting identity 完全相同时共享不可变 raw Alpha artifact，之后分别执行各自增强逻辑。
- source validation 从“每日两次全量扫描”改为批次 seal、chunk token、逐日读 receipt 和异常 full rehash；日期 cutoff、availability、source revision 和未来毒化测试不得削弱。

该授权不包含修改、覆盖或回写已冻结v6代码身份/结果，不包含把历史批量结果写入实盘binding，也不包含多进程并发提速承诺，
更不包含通用缓存平台、通用回测引擎或生产调度平台。已冻结v6保持原code-release和golden evidence只读。

### 0.2 从属设计处置

`docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` 中以下内容已被本蓝图替代，不能继续作为开发或运行依据：

- 用完整 Historical Range/Phase 1R bridge 生成训练输入。
- 以新 SEALED base snapshot 作为 WSL 训练硬前置。
- Batch B 对既有 Advisory capture/build/snapshot 表执行生产 DML。
- 先完成完整 2/3/5 年矩阵、三种子、全量 bootstrap，再进行首次真实训练。
- 没有正式 capability readiness 时禁止显示明确标记的实验性真实模型结果。

该详细设计中仍可复用的内容仅限：SHORT_REBOUND 风格定义、候选特征公式、时间切分原则、WSL 环境约束、LightGBM LambdaRank 历史对照和错误可见性。旧5日标签只保留历史结果；P0-C Top5 shadow-policy episode标签继续作为冻结排名基线，新的Ranking、Entry与Exit研究分别使用§6.6至§6.10定义的角色化增量价值标签。后续代码不得继续调用其已废止的 Batch B 历史证据编排路径。

## 1. Background / 业务目标与当前差距

当前策略包能够产生有序候选。已经完成的模型组件可以输出 Top5、收益/周期和价格区间；两个生产 Program 已按交易日形成真实 `PUBLISHED` baseline、target-open settlement 和 active episode，目标多Alpha Program的P0-D challenger也已通过exact descriptor进入在线shadow。当前缺口不再是“模型或发布链路不存在”，而是尚无成熟自然future OOS证明模型可以稳定提高荐股净收益，且P0-D至P0-L均未取得可晋级的主模型。后续不再寻找一个同时承担候选召回、排名、买入、仓位、退出和风险的单一Selection替代模型，而以瓶颈诊断驱动的角色分离决策栈推进：

1. **前向运行能力**：对每个 ENABLED Program 按交易日持久化基线推荐、模型 challenger、outcome/价格区间和 episode 结果。
2. **上游alpha与候选召回**：StrategyPackage/QE负责把潜在赢家送入可交易候选池；Top20/40/50赢家召回不足时，资源优先转向上游，而不是继续修改下游loss。
3. **角色分离能力**：Top20排名、Entry Guard、固定槽位下的现金/空槽、Exit和组合风险分别拥有独立决策时钟、标签、shadow、验收和回滚；目标架构不是六条并行工作线，正式路线最多一主线一辅线。
4. **双目标合同**：`ALPHA_RANKING`只评价固定动作空间内的成本后超额收益；`RISK_MANAGED_ADVISORY`评价Entry/Exit/现金暴露带来的绝对收益、MDD和尾部风险。两类结果不折算成一个加权总分，实验立项时冻结归属，按合同独立激活和展示。
5. **增量价值口径**：各层标签衡量相对当前冻结基线动作的增量价值并绑定policy hash；历史可确定性重放两臂时使用配对shadow-policy模拟，只有未来日志仅观察单臂时才评估OPE估计器。
6. **用户可见能力**：页面同时展示基线与实验模型的当期建议、目标合同、状态、价格/周期范围和前向表现，不把模型输出写回 Selection、Paper 或模拟盘，也不把超额收益合同描述为绝对收益承诺。

长期趋势策略另行完成长期重排、生存、time-to-hit 和大行情捕获模型，不阻断短反弹主线。

截至 2026-08-31：

| 功能 | 状态 | 完成口径 |
|---|---|---|
| SHORT_REBOUND Top20→Top5 | `PURE_RERANKER_RESEARCH_COMPLETE_NOT_ACTIVATED` | M5A 已完成 45 个 booster 和一次冻结 test；winner 平均 5 日超额收益 `0.0071894`，低于 selection rank 的 `0.0085591`，95% block-bootstrap lift 区间跨 0。纯重排保留为历史基线，不再是唯一质量主线 |
| 预期收益与持股周期 | `M5B_REAL_CALIBRATION_COMPLETE_NOT_ACTIVATED` | 最终 request `advoutcal_ec16422ad1a97040583e5273` 生成 v2 bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` 并通过 exact retry；8/10 binary head 可校准、2 个五日 head 因排序反转明确保持 `UNCALIBRATED`，逐 head solver/版本/迭代/收敛证据完整，holding 仍独立 `UNCALIBRATED`。冻结 test 未显示足以支持激活的校准改善，现行 M3 v1 binding 保持不变 |
| 买入/止盈/止损区间 | `M4_POINT_INFERENCE_VERIFIED` | 四头真实模型、decision-cutoff 未复权投影、dividend PIT 输入、exact binding 和风险边界已贯通；目标多 Alpha Program 的固定日期按需 GET 对 20/20 候选返回完整价格范围 |
| 荐股页面模型展示 | `FORWARD_API_AND_PAGE_SOURCE_COMPLETE` | 页面/API 可展示 Top5、五期限收益、概率、MFE/MAE、持股、价格范围和前向状态；runtime已有真实发布与episode，目标多Alpha Program的P0-D challenger已接入exact descriptor并保持实验shadow |
| 每日前向发布与 episode | `FORWARD_RUNNING_NATURAL_OOS_IMMATURE` | 两个 ENABLED Program 均已形成真实发布、target-open settlement和active episode；截至2026-08-28第一个Program有4条P0-D observation、最早成熟日为2026-09-22，第二个Program尚无模型observation。当前没有成熟自然模型outcome |
| 多 Program 模型分发 | `DYNAMIC_BINDING_VERIFIED_ONE_P0D_PACKAGE` | active binding动态解析已完成；目标多Alpha Program绑定P0-D exact bundle `e555903e...`，单Alpha无bundle时基线继续且模型typed unavailable。P0-E至P0-L均未接入descriptor |
| LONG_TREND 专家 | `DEFERRED_UNTIL_PACKAGE_READY` | 对应长期趋势包形成稳定输入后训练和接入 |
| P0-D至P0-L研究族 | `FROZEN_NO_ACTIVATABLE_WINNER` | 同一P0-C开发数据/候选/feature schema/CORE家族上的九轮自适应研究已事实收敛；旧结果、合同和消费窗口不改写，不派生P0-M |
| 新模型演进路线 | `N1_FORMAL_COMPLETE_N2A_FORMAL_COMPLETE_N2B_V2_FORMAL_PENDING` | N0 completion为`9b460c29...`；N1 bundle为`74827d03...`且PR #4014已合入。N2-A bundle `6784df1a...`已完成、inspect/exact retry通过、registry新增一条0-trial导航记录；route仍为`N2_ENTRY_EXIT_QE_PREPARATION`。N2-B v1在无bundle/registry/研究结果的状态下因独立PIT安全事件退出；BUG-1302已验证退役问题包，v2固定审计另外两条合格独立包并绑定排除receipt。现有父包相对FUND有稳定增量，但相对LSTM没有确认性增量，Top50绝对召回仍低；继续完成独立包与Entry/Exit诊断，不激活、不改权重、不读sealed holdout |

历史表、schema、证据链、报告、任务状态、artifact 数量和测试数量均不得计入上述功能完成度。

### 1.1 当前进度口径

为避免把“代码存在”“单点可调用”“持续运行”和“模型有效”混为同一完成状态，本蓝图固定使用以下独立维度，不再汇总成单一百分比：

| 维度 | 当前进度 | 准确含义 |
|---|---:|---|
| 模型组件实现 | `4/4` | Top5、收益/周期、价格范围、页面/API 均有真实模型实现 |
| 固定日期按需推理 | `TARGET_MULTI_ALPHA_VERIFIED` | 仅目标多 Alpha Program 在 `2026-07-16` 的 persisted replay 上验证；不是生产前向运行 |
| 每日前向发布 | `RUNNING / REAL PUBLISHED` | 两个 ENABLED Program 已形成真实发布和target-open settlement；截至2026-08-28 scheduler运行且无last_error |
| episode 前向跟踪 | `0 MATURE MODEL OUTCOMES` | 第一个Program最早模型成熟日为2026-09-22，第二个Program尚无模型observation；任何open-mark均不得冒充成熟future OOS |
| 多 Program 模型覆盖 | `DYNAMIC RESOLVER, 1 P0D-CONFIGURED PACKAGE` | P0-D exact descriptor已作用于目标多Alpha Program；无bundle的单Alpha typed unavailable不阻断基线 |
| 模型质量升级 | `0 ACTIVATED SELECTOR CHALLENGERS` | M5A/M5B/M5C及P0-D至P0-L均未证明可以替换Selection；P0-D只作为experimental shadow，M4继续提供价格范围而非选股alpha |
| 长期趋势模型 | `NOT_STARTED` | 长期趋势原生多 Alpha 父包尚未形成可训练输入 |
| 旧研究族状态 | `P0-D..P0-L FROZEN` | 研究事实完整但无可激活winner；不以同族新变体继续消耗相同开发证据 |
| 新路线实现状态 | `N0 COMPLETE / N1 COMPLETE / N2-A FORMAL COMPLETE / N2-B V2 SOURCE READY` | N2-A已闭合当前父包三臂Alpha归因，但没有产生模型trial或可激活winner；N2-B v1未形成证据，v2只保留两条通过生命周期与PIT门禁的独立包，正式审计尚未运行。必须补齐该候选来源诊断、Entry/Exit动作空间和QE准备边界，才能进入N3唯一主线 |

PR #3346 已于 2026-08-12 合入 `main`，merge commit 为 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`；P0-D PR #3368 已于 2026-08-13 合入 `458199cd902323e006ac23d3767c908637068fa8`，后续通过descriptor rotation作为experimental shadow接入。P0-L源码PR #3959、BUG-1251修复PR #3967和close-sync PR #3969均已合入；P0-E至P0-L均未激活，M4 v1 price-range binding保持不变。源码合入、descriptor接入、运行时加载、模型激活和自然OOS成熟继续分别报告。

### 1.2 真实训练与实验数据总表

下表只记录真实冻结输入、真实模型或真实校准运行；mock、fixture、历史证据平台和测试数量不计入模型实验：

| 阶段 | 冻结输入与产物 | 样本/模型 | 资源 | 冻结 test 或真实运行结果 | 当前结论 |
|---|---|---|---|---|---|
| M0 可训练矩阵 | request `advmreq_ac5959aa8dc14a25e3b8c139` | 406 decision dates；8120 个 Top20 候选；6960 行冻结 features | 文件只读构建 | 训练/验证/test 的父输入身份已冻结，基础行情截止 `2026-06-30`，候选共同范围 `2024-07-04..2026-03-10` | 可训练输入已完成，不再扩建历史数据平台 |
| M1 首个 reranker | bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629` | train 3818 行/191 日；validation 1139 行/57 日；test 1599 行/80 日；Top5 400 行 | 128.921 秒；RSS 2,262,388,736 bytes | model Top5 5日平均超额 `-0.0002833`、命中率 `0.5025`、NDCG@5 `0.26570`；selection rank 为 `0.0085591`，HMM 为 `0.0040167`，随机为 `0.0055652` | 真实模型已接入 shadow，但原始质量明显低于基线 |
| M3 outcome/holding | bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` | 46 个 LightGBM heads；test 1600 行/80 日；1/3/5/10/20 日 horizon | 108.128 秒；RSS 655,581,184 bytes | 五期限预测零 NaN；5日正超额 head AUC `0.53469`、Brier `0.25186`；holding accuracy `0.36261`、bucket-day MAE `10.1374`、range coverage `0.75031` | 功能和运行时已贯通；原始概率与周期分布保持实验/未校准语义 |
| M4 entry/risk ranges | request `advprreq_2d826a7b2704137bf3a60d9d`；bundle `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3` | 4 个 heads；test 1600 行/80 日；1599 个 executable gap；8120 行中仅 4 个 binary 负例 | 13.85 秒；RSS 491,802,624 bytes | q10-q90 coverage `0.727955`，零 quantile crossing；真实多 Alpha readback 20/20 返回买入、止盈、止损和移动保护范围 | M4 v1 exact binding已激活并完成固定日期推理，但尚无每日forward coverage；binary必须保持`UNCALIBRATED` |
| M5A Top5 tournament | train request `advm5train_a64594d6f22f618a4afef84a`；test request `advm5test_818fe5a6c8ee323d2fbf25d4`；bundle `1757b24b854f8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | 45 trials、37 candidates；winner 为 5-seed `EXPANDING_ALL__LAMBDARANK_NDCG5__MW_0.75`；test 400 个 Top5/80 日 | tournament 18.925 秒、RSS 409,169,920 bytes；test 2.131 秒、RSS 342,462,464 bytes | winner 5日平均超额 `0.0071894`、命中率 `0.5425`、NDCG@5 `0.34150`；selection rank 为 `0.0085591`、`0.5375`、`0.32399`；均值 lift `-0.0013696`，95% block-bootstrap `[-0.0093061, 0.0053392]` | 相比 M1/HMM/随机有改善，但未证明超过现行 selection rank，不激活 |
| M5B outcome calibration | request `advoutcal_ec16422ad1a97040583e5273`；bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` | validation 940 feature-covered/1000 labels；test 1600/1600；8/10 binary heads calibrated，2 个 h5 heads 因 order reversal 保持 raw | 11.659 秒；RSS 399,200,256 bytes | 8 个 calibrated binary head 的 test Brier/logloss/ECE 均未优于 raw；收益区间名义 coverage 平均绝对偏差 `0.00984 -> 0.03129`；path upper `0.01432 -> 0.01394` | artifact 完整、exact retry 一致，但总体质量不支持激活；M3 v1 binding 不变 |
| M5C entry-gap calibration | request `advprcal_7cb766fe38898e12a008a328`；bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead` | validation 940 feature-covered/1000 eligible；test 1599/1599；central-80 CQR | 2.679 秒；RSS 395,853,824 bytes | validation coverage `0.810638` 导致 `delta=0`；test raw/calibrated coverage 均 `0.727955`，mean width 均 `0.0122280` | 全局常数校准无法修正 validation→test 漂移，`activation_recommended=false`；源码已合入但不激活 |
| 生产前向基线 | 两个 ENABLED Program；真实baseline publish、target-open settlement和episode持续运行 | 截至2026-08-28第一个Program有4条P0-D observation，第二个Program尚无模型observation | scheduler运行且thread_alive，last_error为空 | 第一个Program最早模型成熟日为2026-09-22；当前无成熟自然model outcome | 每日发布、target-open和episode闭环已验证；继续自然积累，open mark不冒充成熟胜率 |
| P0-C policy dataset | bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd` | 386 candidate days；7,720 candidates；7,716 matured labels；28/28 READY CPCV paths | 28.9 秒；RSS 1.72GB | take 4,199 / skip 3,517；holding median 6 days；Selection rank buckets 均约 54% take rate | policy-aligned 标签和评价输入已完成；PR #3367 已合入 `49973d6e` |
| P0-D meta-label | final-source request v2 `advmetareq_0451bd4cb1f8cc7add8b9956`；bundle `e555903ec928fd39ea09180133401a6490a4e6d5440e3ef63642909e1329e03a` | 2 families × 3 seeds × 28 paths = 168；winner `FAMILY_CORE_HMM/20260817` | 332.182 秒；RSS 2.91GB；exact retry 3.529 秒；与旧 bundle 12/12 功能 identity hashes 一致 | winner `19.4357 bps` vs Selection `15.7801 bps`，lift `+3.6556 bps`，path win rate `64.29%`，PBO `0.40`，AUC `0.5142` | PR #3368 已合入 `458199cd`；exact descriptor已接入目标多Alpha Program，保持`EXPERIMENTAL_SHADOW/UNCALIBRATED`，不替换baseline |
| P0-D 历史虚拟前向 | artifact `fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9`；父run `ahrr_e46883bcdf217a14d8e7a0abf01aeb18` | 44日context；24个成熟decision；20日tail；24/24 resolved | 冷日评分约15–20秒；恢复后完整复跑约18.5秒；最终24日 artifact exact retry 同hash | P0-D hit rate `36.67%` vs Selection `26.92%`，但累计净收益 `-19.45%` vs `-16.90%`，最大回撤 `-22.54%` vs `-16.90%`，平均换手 `40.00%` vs `34.67%` | 功能验证通过但模型质量不支持激活；窗口已消费，后续调模重跑降级为 `HISTORICAL_REPLAY` |
| P0-E outcome-weighted meta-label | final-source request `advmetareq_4d2393bcb776cf7d6a3aace2`；bundle `cb9e61e9c54d89263f76f2f2bcefb515070c96908aa2bca790c064fd339fb270` | 2 families × 3 seeds × 28 paths = 168；winner `FAMILY_CORE_HMM/20260817` | 346.325秒；RSS 2.74GB；exact retry 3.3秒 | `18.9626 bps` vs P0-D `19.4357 bps`，lift `-0.4731 bps`，path win `35.71%`，PBO `0.8143` | `research_improvement=false`；负面实验，不激活、不追加调参；rebase前后功能hash一致 |
| P0-E HISTORICAL_REPLAY | artifact `6bba37f8804af38f4357c3939a380cca3be2bc915a62149108518b6d4948dba4` | 同24决策日+20日tail；24/24 resolved；证据已消费 | 首日约31秒、后续约15–20秒；exact retry同hash | hit `23.08%`；累计 `-13.39%` vs Selection `-16.90%` / P0-D `-19.45%`；回撤 `-14.92%`；换手 `29.71%` | 幅度结构在该窗口改善但未被CPCV确认，只作诊断，不晋级 |
| P0-F continuous policy utility | request `advutilityreq_9e8036ee4a8ea785b6ba8742`；bundle `ff336ead...` | 3 arms × 2 families × 3 seeds × 28 paths；advancement arm为Huber utility | PBO `0.30`；完整168个candidate trial-path | 相对P0-D收益`+2.612087 bps`、path win`53.57%`、MDD`+0.001286`，相对Selection`+5.578137 bps`；换手`+0.007419` | 仅换手门槛失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-G turnover-constrained utility | request `advturnutilityreq_369c2d23fb1d30e1ce801506`；bundle `433ff217...` | 2 families × 3 seeds × 28 paths = 168 | winner `20.937724 bps`；PBO `0.40` | 相对P0-D收益`+2.191621 bps`、path win`53.57%`、MDD`+0.002908`，相对Selection`+5.157670 bps`；换手仍`+0.004096` | 仅换手门槛失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-H dual-head output constraint | request `advdualheadreq_027b41bd7b996fd25eab7b54`；bundle `82afdb81...` | 2 families × 3 seeds × 28 paths = 168 | winner `18.419750 bps`；PBO `0.90` | 相对P0-D收益`-0.326353 bps`、path win`46.43%`；MDD改善`0.010688`、换手改善`-0.022708`；return head日Spearman仅`0.041731` | 收益和path-win失败，`NEGATIVE_STOP_NOT_ADVANCED`，不激活 |
| P0-I grouped-rank return head | request `advgroupedrankreq_314c84d158a42be1b87e71d8`；evidence-only bundle `2378358c...` | 前10条path完成60个trial-path；第11条path首个family/seed停止 | 810.577秒；RSS 2.96GB；PBO不可计算 | 冻结8档price均不能满足exact P0-D OOF换手预算；已完成trial的return日Spearman均值`-0.002229`、NDCG@5 `0.385322`，liability Spearman `0.236089` | `NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner/Stage B/激活 |
| P0-J Selection-prior residual | request `advselpriorresreq_3a50e2f6fd9cf43cb1f6ad3e`；evidence-only bundle `eb8ade9b...` | 第一条path的inner block 3停止；0 trial | 213.867秒；RSS 2.89GB；PBO不可计算 | decreasing-isotonic Selection rank prior完全退化为`-1.5357 bps`平线，reason `ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE` | `NEGATIVE_STOP_INCOMPLETE_CPCV`；证明rank-to-return单调关系跨分区不稳定，不证明Selection全局无效 |
| P0-K Selection-preserving liability gate | request `advselgatereq_943f9e551d5fee35e57340cc`；bundle `fee9b561...` | 2 families × 3 seeds × 28 paths = 168 | 1173.362秒；RSS 2.93GB；PBO `1.0` | liability日Spearman`0.254589`；168个trial均选阈值`0.4`且零拒绝，完全等同Selection；相对P0-D收益`-2.966049 bps`、path win`32.14%`、MDD`-0.004162`、换手`-0.068009` | `NEGATIVE_STOP_NOT_ADVANCED`；PBO来自相同arm和固定tie-break，不按普通过拟合解释 |
| P0-L P0-G-anchored liability local reranker | request `advp0lreq_b86425d3b5ce508904fa01b0`；evidence-only bundle `4476afeb...` | BUG-1251修复后在第一条path、首个family/seed选择阶段停止；0/168 | 320.195秒；RSS 2.86GB；PBO不可计算 | identity control无干预；gain `12/8/4/1`产生`33/71/85/85`次entry变化并降低换手，但cash day由1增至2、coverage下降，全部非零档不满足完整性 | `ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`；`NEGATIVE_STOP_INCOMPLETE_CPCV`，无winner/Stage B/激活 |
| N1 Tier-1 oracle + fixed learnability | request `advn1req_d8116a79d72d9c1813485053`；bundle `74827d037128b9a4716afdf3a17221fda8c18b0af885be33c9f6978699283348` | canonical PIT 5067只/5077段；386 decision days；19300条Top50；7720条Top20 OOF；28 READY paths，每行7个OOF；382日可评价 | 397.922秒；peak RSS 2,584,461,312 bytes；exact delivery retry为duplicate no-op | Top20/40/50赢家召回`0.8808%/1.6062%/1.7617%`；perfect Top5 lift `1314.27 bps`，95%区间`[1136.61,1516.57]`；固定Ridge lift `98.83 bps`，95%区间`[1.18,193.73]`，MDE `136.01 bps` | oracle=`HIGH/CONTROL_READY/DIRECTION_GATE`；learnability=`INCONCLUSIVE/EXPLORATORY/NAVIGATION_ONLY`；综合`INCONCLUSIVE__THEORETICAL_HIGH__LEARNABILITY_HIGH`、`direction_ready=false`；不部署、不激活，route进入N2 |
| N2-A StrategyPackage三臂Alpha审计 | request `advalpha3req_e7295a31a4e1953e9048cec5`；bundle `6784df1abe1dcbb802220d03db70674638eda18b5001c2e022fc099c6bb3e9cd` | 386日；共同signal outcome 1,710,301行；LSTM/FUND/父包各19,300条Top50；父包与N1 ranking按keys和`1e-12`分数精确一致 | 70.861秒；peak RSS 2,588,618,752 bytes；inspect=`VALID`；exact retry=`EXISTING_BUNDLE`且registry duplicate no-op | LSTM/FUND/父包RankIC=`0.11677/0.05628/0.12284`；Top5 H20净超额=`397.89/245.73/446.52 bps`；父包-LSTM RankIC增量`+0.00607`、95%区间`[-0.00223,0.01484]`，Top5增量`+48.63 bps`、区间`[-103.99,186.53]`；父包Top50召回`1.7617%`、随机lift `1.75×` | `EXPLORATORY/ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`、0 trial；Alpha主要来自LSTM，FUND较弱但不同源，固定组合未确认优于LSTM；不调权、不激活，route保持N2 |
| H0 v6 golden | report `docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md`；artifact `F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json` | 44个decision dates，`2026-05-15..2026-07-16`；A/B/C三臂；C修复后独立重跑44/44 | result hash `500d96e0...`；contract hash `652eef96...`；artifact file SHA256 `9c59219d...` | 市场全窗沪深300`-4.40%`；在28个matched交易日上，HMM/risk B5相对A5的3/5/10日平均收益改善`2.43%/3.17%/3.48%`且配对95%区间不跨0，Episode胜率`35.42%` vs `27.45%`；M5A C5相对A5各期限收益改善区间均跨0，Episode胜率`18.37%` | HMM/risk存在该窗口局部相对效果但绝对收益仍负；M5A无稳健增量。PR #3558已合入`1d1fc932`，结果只作H0不可变行为oracle |

### 1.3 方向一致性复核

当前实现保留了正确的工程边界，但目标架构尚未完成：

- 训练只读取 QE H5/Parquet/Qlib Bin 和既有预测产物，正式推理才读取数据库当前行情；M5C 不读取数据库历史训练数据。
- M1、M3、M4、M5A、M5B、M5C 均执行真实 WSL 运行，没有用规则、随机、mock 或缩样本冒充完成。
- Selection、StrategyPackage、Paper、模拟盘和 QE 资产业务逻辑未被 M5C 修改；模型失败或未激活不阻断现有荐股基线。
- 没有新增角色、审批、二次策略包准入、ModelOps、历史补账、旧 batch/root 处理或通用缓存平台；H0 是用户明确批准的现有历史验证执行优化，不改变该边界。
- 真实负面实验结果均保留，没有把 artifact 发布、源码合入或 API 可返回误写为模型效果成功。
- 当前每日执行器和active-binding动态解析已经运行；P0-D exact bundle已作为独立experimental shadow接入目标多Alpha Program，但不替换baseline list。当前模型通道与baseline发布分离，不能把baseline episode或open mark指标记作成熟P0-D效果。

模型/前向链路当前最小剩余事实是继续自然积累成熟future OOS；每日发布、target-open episode、动态binding和P0-D shadow descriptor均已关闭功能缺口。P0-D至P0-L的离线研究族已完成且无可激活winner，“继续同族下一模型”已从正式路线删除。新的模型演进方向已经收敛为§5、§6和§9中的oracle+learnability瓶颈诊断与角色分离路线；与其并列的H0只优化已授权历史验证执行形态。

## 2. Scope / 当前实施范围

本蓝图当前只覆盖直接产生真实模型和荐股能力的工作：

- 读取已有 QE H5/Parquet/Qlib Bin 基础数据和已有模型预测 PKL，并核对可用于训练的字段。
- 从现有文件构造候选级训练矩阵、标签和时间切分。
- WSL Conda 中复现既有LightGBM排序、分类、分位数和生存模型；新候选模型只在oracle+learnability分流后按预注册角色、信息集和lineage训练，不预先固定为P0同族或单一模型家族。
- 对开发窗口产生cross-fitted诊断；主线、特征、阈值、policy和目标合同冻结后，才在独立sealed holdout执行一次方向确认。
- 在正式预测时从数据库读取当前/实时行情、行业、资金、HMM、停牌、ST和可交易性输入。
- 使用同一特征定义完成训练文件与数据库预测输入的 schema parity。
- 每个 Advisory Program 独立运行；一个 Program 绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- ENABLED Program 按交易日自动执行基线 review，持久化 `PUBLISHED` list version 和 episode；模型存在时同时持久化 challenger observation，模型不存在时基线照常发布并返回 typed unavailable。
- P0-D历史meta-label继续以冻结review policy下的episode净收益输出`take/skip/confidence`和Top5研究shortlist；新研究按角色输出Ranking、Entry或Exit动作及相对冻结基线动作的增量价值，均不形成自动下单或动态资金仓位。
- train/validation 内使用 purged rolling/CPCV 或样本规模允许的等价时序重采样，报告 trial 选择偏差；已经读取的冻结 80 日 test 不再用于方向、参数或阈值选择。
- 在荐股页面展示 Top5、收益范围、持股周期和价格区间。
- 模型不可用、字段缺失和版本不兼容时错误可见，但不得阻断现有规则荐股基线。
- 历史验证使用批量执行器连续处理冻结日期区间，复用静态工作区、区间读取和 raw Alpha artifact；每个交易日仍保持独立 decision cutoff、业务语义 hash、结果、receipt 和 checkpoint。
- 实盘单日执行器与历史批量执行器共享同一逐日业务内核；允许运行信封不同，不允许候选、排序、增强、名单生命周期或 outcome 口径分叉。
- P0-D至P0-L研究族冻结为历史事实；最小JSONL trial registry和由其生成的单页路线只索引实验身份、分类、消费窗口、decision use和既有receipt，不复制证据或建设治理平台。
- 父包预测延伸先执行可行性spike，区分“冻结模型可直接推理新窗口”“只有历史预测且运行资产不足”“必须重训并形成新StrategyPackage identity”三种状态。
- 综合oracle按Tier 1至Tier 3逐层放行；每层同时区分clairvoyant/action上限与固定cross-fitted learnability结果，全部只使用开发窗口，不读取sealed holdout。
- Entry Guard与Exit-label组成一个边界明确的辅助工作包，但分别使用独立目标合同、decision clock、policy hash和增量价值标签；同一时刻只允许其中一个进入candidate训练/confirmation。条件化上游alpha仅准备无研究证据的输入/预算。动态资金权重不在当前范围，第一版只允许固定等权槽位中的`SKIP`和现金/空槽结果。

## 3. Non-goals / 明确禁止

以下任务持续禁止进入当前主线。每日前向发布、模型 challenger observation、episode 跟踪和边界明确的 H0 历史批量执行优化均不属于无界历史证据平台、归档或 ModelOps，明确不在禁止范围内：

- 与当前已授权回放无关的历史数据证据链建设、历史补账、历史归档和旧任务修复。
- 新建通用 Source Catalog、全历史 lineage、source revision union 或历史 correction E2E；H0 仅复用当前冻结 catalog，并定向减少重复全量扫描。
- 新建通用 observation/capture/label/snapshot 数据平台。
- 为训练重新执行多年 Historical Range/Phase 1R 业务任务；H0 只服务独立历史验证，不向模型训练回灌业务结果。
- 为训练向 Advisory 历史业务表写入候选、列表、Outcome、Summary 或 bridge DML。
- 处理旧 PARTIAL/RUNNING batch、旧 root、orphan artifact 或遗留状态。
- 自动重训、通用 ModelOps、自动模型激活、canary 发布平台、通用漂移治理、通用缓存平台和灾备。仅允许当前 P0 所需的 Advisory 收盘后执行器、基线/challenger 前向对照，以及 H0 所需的任务内内容寻址工作区和 PIT 区间读取。
- 把六层决策栈拆成六个并行项目，或同时运行多于一条模型主线和一条独立辅助线。
- 在oracle、learnability、特征筛选或探索阶段读取新的sealed holdout；holdout只允许主线冻结后执行一次方向确认，随后立即降级为已消费历史证据。
- 动态资金权重、组合仓位优化、自动下单或交易执行输入；Entry Guard产生`SKIP`和固定槽位空缺不等于资金仓位模型。
- 为trial registry、活动路线、oracle或holdout另建UI、审批、角色、数据库平台、证据仓库或通用研究调度系统。
- 新增角色、审批、授权流、人工放行、策略包二次准入或运行时 package preflight。
- 改动 Selection、Paper、模拟盘、QMT 或策略包既有业务逻辑。
- 使用 QE 回测组合净值、Paper/模拟盘结果作为训练输入。由 QE 文件行情按冻结 Advisory review policy 确定性模拟的 episode 结果是正式监督标签，不是回测结果跨模块耦合。
- 用 mock、随机输出、规则排序或静态 JSON 冒充真实模型预测。

不禁止读取 QE 实验中已有的 H5/Parquet/Qlib Bin 基础数据，以及 Prediction Store 中的 `pred.pkl`、各腿 seed 预测、`combined_prediction.pkl`、权重和必要模型产物。基础行情、行业、资金和因子历史数据必须来自 H5/Parquet/Qlib Bin；预测、模型参数和其它非基础行情产物允许使用 PKL。所有读取必须只读，不修改 QE 实验资产，也不把 QE 组合回测净值、交易或持仓结果当作模型监督信号。

## 4. 数据权威与防泄漏边界

### 4.1 模型训练数据

模型训练默认且优先使用已有 QE H5/Parquet/Qlib Bin 基础数据和已有 QE 预测 PKL，不从生产数据库重新构建多年历史数据。

允许的训练输入包括：

- 已有 QE H5/Parquet/Qlib Bin 中的日线、复权、成交量、资金、估值、行业、指数、停牌、涨跌停和因子特征。
- 仅使用上述基础数据按本蓝图标签公式派生的收益、MFE/MAE、周期和价格标签；只有 QE `label.pkl` 与目标标签定义逐字段一致时才可直接复用，否则只作为对照，不得偷换标签语义。
- Prediction Store 中目标父包精确 roster 引用的各腿 `pred.pkl`、模型元数据、历史 seed ensemble、`combined_prediction.pkl` 和逐日 weight。允许直接读取 PKL，不要求为形式统一复制成 Parquet。
- 当前父包正式 runtime 每腿只运行代表 seed，并使用 `frozen_backtest_terminal_weights`。首模历史候选必须按两个代表 seed、当前 zscore、terminal weights、raw Top25 和 Program target_count=20 确定性重建；完整 38-seed ensemble、逐日 weight 和 `combined_prediction.pkl` 只作不进入首模特征的显式分布诊断。
- 不得把不同父包、不同演进实验、不同 roster 的“最新腿”临时拼成训练输入，也不得把代表模型结果复制为多 seed 后生成伪离散度。
- Qlib 分钟 Bin；只在明确训练盘中成交概率或价格路径模型时使用，不作为 Top5、收益、持股周期或日线级价格范围模型的前置条件。

训练过程只需记录直接保证模型可加载和特征一致性的最小信息：

```text
training_run_id
experiment_id and hypothesis_family_id
study_type and objective_contract
input_file_paths
input_schema_version
feature_names and dtypes
label_definition_version
train/validation/test date ranges
consumed_windows and decision_use
model_family and parameters
model_file_path and sha256
code_commit
WSL environment identity
```

禁止为训练输入额外生成逐日 source receipt、逐分区 revision chain、capture membership、SEALED manifest、CAS publication 或历史 correction 记录。

#### 4.1.1 2026-08-07 已验证输入清单

| 输入 | 已验证范围/内容 | 当前用途 |
|---|---|---|
| WSL `/home/lc999/data/qlib_bin` | 日线 `2018-08-01..2026-06-30`；包含 OHLCV、复权、`limit_up/down`、涨跌停价和 `prev_close` | 首模基础特征、标签、HMM重训和日线价格范围 |
| WSL H5/Parquet candidate | `/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2`；日线、基本面、资金、行业、筹码和静态因子 | 首模候选级特征；训练不在 Windows 读取 |
| WSL `/home/lc999/data/qlib_minute_bin` | `2024-01-02 09:30..2026-06-30 15:00`，约33GB，含分钟OHLCV与涨跌停字段 | 仅后续盘中路径模型 |
| Prediction Store | 当前目标父包精确 roster 为 LSTM 33 seed + FUNDGROWTH 5 seed；两个 runtime 代表 seed 与完整 38 个 `pred.pkl` 均存在 | 代表 seed 用于 runtime-equivalent 候选；完整 ensemble 仅作诊断 |
| combine workspace | 406 日 `combined_prediction.pkl`、逐日权重和组合因子文件存在 | 已回测 walk-forward 组合参考，不作为当前 runtime 候选权威 |
| suspend sidecar | `suspend_d_daily_candidate_20180801_20260630/suspend_d.parquet` | 历史停牌状态 |
| 沪深300 | 日线 Bin 内 `000300.SH` 可读 | benchmark和HMM超额收益 |

当前基础数据能够启动真实训练。目标多 Alpha 各腿共同预测范围实际形成 406 个 decision dates：`2024-07-04..2026-03-10`，因此依赖各腿预测的第二阶段模型样本不得超出该共同范围。基础行情和 H5/Parquet 截止 `2026-06-30`，用于完成 `2026-03-10` 候选的未来标签，不代表候选或 HMM 连续状态可以延伸到 `2026-06-30`。任何需要未来收益的训练头都必须按各自 horizon 剔除或右删失尾部未成熟样本；不得把无未来结果的候选当作完整标签。

当前活跃日线 Bin 未包含中证500、中证1000、创业板指或科创50。它们不是首个 Top20→Top5、收益、周期、日线价格范围或现有 HMM 架构的阻断输入；只有后续模型合同明确使用且实验证明有必要时才补充，不预建通用指数库。

M0 已实现 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`：代表 seed + current zscore + terminal weights 先生成 raw Top25，再取 Program target_count 前20。它同时绑定 `decision_as_of_trade_date` 和下一交易日 `target_trade_date`；正式特征只能读取前者 cutoff。真实文件得到 406 日、8120 个候选且每日深度固定 20；combined/ensemble 只作为诊断，不进入 runtime-equivalent 候选语义。M1 bundle、M3 outcome bundle 和 M4A price-range bundle 均已生成，后续阶段继续精确绑定这些身份。

#### 4.1.2 研究登记、开发窗口与sealed holdout

研究登记使用一个只追加JSONL权威索引；Parquet或单页路线只能由该索引派生。每条记录至少包含：

```text
experiment_id
study_type = ORACLE_DIAGNOSTIC | LEARNABILITY_AUDIT | EXPLORATORY_SCREEN | CANDIDATE_MODEL | CONFIRMATION | ACTIVATION
hypothesis_family_id and parent_lineage
unique_variable
objective_contract = ALPHA_RANKING | RISK_MANAGED_ADVISORY
dataset/schema/policy identities
generated/evaluated/selected trial counts
consumed_windows
result_class
decision_use = NAVIGATION_ONLY | DIRECTION_GATE | ACTIVATION_EVIDENCE
evidence_refs
```

- registry只索引既有request、bundle、report和receipt，不复制完整结果，不替代QE warehouse、Prediction Store或模型artifact。
- oracle诊断、learnability audit和候选模型trial分类计数；DSR/PBO只消费与其统计定义匹配的模型/策略trial，不把一个oracle指标机械计为独立模型trial。
- 探索性结果可以调整下一步研究优先级，但不能单独关闭整个方向、声称稳定效果或作为激活证据；机器校验必须拒绝`NAVIGATION_ONLY`被引用为`ACTIVATION_EVIDENCE`。
- 开发窗口允许反复用于预注册oracle、learnability和探索，但每次使用都登记为已消费；新的sealed holdout使用不同dataset/window identity，并拒绝所有oracle、learnability、特征筛选和调参请求。
- sealed holdout只在主线candidate、模型、特征、阈值、policy和目标合同全部冻结后执行一次方向确认；读回后立即登记为已消费，失败不得返回同一frontier重选候选。
- holdout隔离以明确日期分区、dataset identity、命令访问拒绝和消费receipt实现；不为此建设新数据平台。

父包预测延伸spike必须在承诺新holdout前给出三态结果：

1. 冻结父模型和完整runtime assets可以对`2026-03-10`之后的PIT特征直接推理；新prediction artifact使用新日期/输入identity，但父模型identity不变。
2. 只有历史`pred.pkl`可用，冻结运行资产或因子输入不足，不能生成新窗口；报告精确缺口，不以最后一日预测外推。
3. 必须重训父模型；这形成新模型和新StrategyPackage lineage，不能冒充旧父包自然延伸，训练/确认时间边界需重新冻结。

### 4.2 正式预测数据

只有正式执行 Advisory 模型预测时才读取数据库中的实际当前/实时数据：

- 当前策略包生成的候选、Alpha rank/score 和原生多 Alpha component evidence。
- 数据库中的当前/实时行情、复权、资金、估值、行业和交易状态。
- 本轮新训练模型产生的当前 HMM预测，以及当前risk policy、ST、停牌、涨跌停和股票池输入。
- 当前 Program、binding 和模型配置。

正式预测不得读取 QE H5/Parquet/Qlib Bin 作为当前行情替代，也不得用训练文件中最后一日数据冒充实时数据。历史 PKL只用于模型训练和离线评价；正式预测的 Alpha/多 Alpha分数必须来自该 Program 当次实际候选和父包组件输出。Advisory 的 `target_trade_date` 与 `decision_as_of_trade_date` 必须分别保存，任何正式数据库特征的 business date 都不得晚于 decision cutoff。

### 4.3 训练与预测 schema parity

训练和预测必须调用同一个特征公式注册表，并分别通过：

```text
QEFileFeatureSource -> SharedAdvisoryFeatureBuilder
DatabaseRealtimeFeatureSource -> SharedAdvisoryFeatureBuilder
```

必须核对特征名、dtype、缺失值语义、单位、复权口径和顺序。schema 不兼容时该模型预测明确失败，现有规则荐股继续运行；禁止静默丢列、补零、换特征或返回基线排名冒充模型结果。

### 4.4 最小防泄漏规则

防止未来数据泄漏只保留直接影响模型正确性的规则：

- 时间切分按交易日执行，训练、验证和测试不得随机混合日期。
- 特征时间必须不晚于预测决策时点。
- 标签只能使用决策时点之后的收益或路径。
- 相邻标签窗口按需要设置 purge/embargo。
- scaler、缺失值统计、行业编码和任何拟合转换只能在训练区间拟合。
- 训练期模型选择不得读取最终测试集结果。

这些规则由训练代码和定向测试验证，不建设独立证据平台。

### 4.5 HMM重新训练与预测边界

HMM是行业状态先验、候选特征和对照基线，不是 Top5 模型的主要监督目标。新模型不得读取旧HMM模型、旧状态序列、旧系数或旧训练结果作为训练输入；这些历史产物仅用于结果对照。

- 保留现有行业级两状态 Gaussian HMM、因果 forward-filter 和状态解释架构。
- 使用当前 QE H5/Parquet/Qlib Bin 中的行业收益、相对沪深300收益、行业成交量和真实 `$limit_up` 比例，从头拟合新HMM。
- 首个 holdout 只在 train 区间拟合 HMM 参数和 observation transform，validation/test 固定参数逐日 forward-filter；状态按 excess-return 均值确定性规范为 BEAR/BULL，不得用 `date.today()`隐式决定窗口。
- 禁止用涨幅大于9.8%的近似值替代 Bin 中已存在的真实涨停标记，以免ST、创业板和科创板语义错误。
- 当前数据库版 `SectorHMMTrainer` 的算法可复用，但训练数据适配必须改为文件读取；正式预测加载与 holdout 相同的参数和文件截止 posterior，并仅追加数据库 decision-cutoff 后续观测执行因果预测。若重拟合 HMM，必须同步重建特征和重训 reranker，不能单独替换。
- HMM重训或预测失败必须显式记录，不得静默复用旧HMM系数，也不得阻断不依赖HMM的现有规则荐股。

## 5. Architecture / 唯一目标架构

### 5.1 训练链路

```text
existing QE H5/Parquet/Qlib Bin base data
  + exact parent-roster prediction PKL
  -> read-only schema and coverage inspection
  -> QEFileFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> candidate groups + frozen review-policy episode labels
  -> development-window candidate/rank oracle + fixed cross-fitted learnability audit
  -> bounded Entry/Exit action-space diagnostics when Tier 1 identity is complete
  -> combined typed route selects exactly one main research line
  -> purged rolling/CPCV train-validation paths + untouched sealed holdout/forward stream
  -> WSL Conda real model training for the selected role
  -> validation predictions, intervention support and family-level trial-selection-bias diagnostics
  -> one frontier candidate selected from inner-train only
  -> one-time sealed-holdout direction confirmation when eligible
  -> model file + minimal load manifest
  -> historical research report + append-only registry row; no reuse of consumed windows for selection
```

该链路不创建 Historical Range batch，不写生产数据库，不进入 Phase 1R bridge，也不依赖 Source Catalog 或 SEALED snapshot。

### 5.2 正式荐股预测链路

```text
existing admitted StrategyPackage
  -> current single-Alpha or native multi-Alpha candidate Top20
  -> persisted baseline Advisory review and bounded Top20 list
  -> active binding resolves exact package/style/model bundle
  -> fresh-trained HMM prediction + current regime/tradability semantics
  -> DatabaseRealtimeFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> loaded role-specific WSL-trained bundle when available
  -> optional Top20 rank + Entry Guard + Exit decision, each on its own clock
  -> fixed-slot cash/empty-slot outcome; no dynamic capital weights
  -> return/holding/price models and risk context when available
  -> persisted daily challenger observation + forward outcome/episode maturation
  -> Advisory API
  -> Advisory page
```

模型服务位于 Advisory 消费层，不反写 Selection、StrategyPackage、Paper 或模拟盘。多个 Program 独立执行；运行时不得继续依赖单一 `PROGRAM_ID/PACKAGE_ID/MANIFEST_SHA256` 常量，而应由 Program active binding 精确解析 bundle。没有 bundle 的 Program 仍发布原始基线并显式返回模型不可用。bundle 必须匹配 package/manifest/style/schema，参数不得因显示风格相同而自动跨包共享，候选、排名、列表、observation 和 episode 不能跨 Program 混合。只有后续 matched 实验证明多个策略包兼容时，才能设计显式 compatible-set 的共享模型。

### 5.3 角色分离决策栈与双目标合同

目标架构包含六层，但工作计划最多保持一条主实验线和一条不依赖主线结果的辅助线：

| 层 | 责任 | 决策时钟 | 首要验收 |
|---|---|---|---|
| 上游alpha/候选召回 | 把未来潜在赢家送入Top20/40/50 | T日收盘及以前 | 可交易赢家召回、成本后候选流质量 |
| Top20排名 | 在冻结候选池内提高优先级 | T日收盘后 | Top5/Top10增量净超额、真实干预支持度 |
| Entry Guard | T+1实际价格到达后决定是否仍值得买 | T日冻结阈值；T+1只读权威open/current | 相对无保护基线的增量价值、追高损失、现金暴露 |
| 固定槽位现金 | 对`SKIP/WAITING`保留空槽而不强制补位 | 与Entry Guard一致 | 空槽/补位matched frontier；不输出资金权重 |
| Exit | 每个持有决策时点判断继续持有或退出 | 持仓后每日as-of | 退出相对继续基线policy的剩余净价值、MDD和尾部损失 |
| 组合风险 | 约束regime、beta、集中度和尾部风险 | 与对应Advisory动作同clock | 风险改善及机会损失；当前仅研究overlay，不形成资金仓位 |

双目标合同在实验创建时冻结，结果后不得改判：

| objective_contract | 评价口径 | 合法动作 | 激活/展示 |
|---|---|---|---|
| `ALPHA_RANKING` | 相对Selection与benchmark的成本后超额收益 | 冻结候选、槽位和Entry/Exit基线下的排名变化 | 只声明排名alpha，不承诺绝对收益 |
| `RISK_MANAGED_ADVISORY` | 绝对收益、MDD、尾部风险及相对原Advisory policy增量 | Entry Guard、Exit和固定槽位现金/空槽 | 与alpha合同独立激活；动态资金权重仍未授权 |

同一模型在两个合同下的状态分别记录；页面/API必须显示`objective_contract`、基线policy和证据等级。禁止选择结果更好看的合同事后归类，禁止把两个合同压成一个加权总分。

### 5.4 基线连续性

- 模型通道失败时保留当前规则荐股结果。
- 页面必须明确区分 `rule_default`、`experimental_model` 和后续 `validated_model`。
- 禁止把规则结果填入 model 字段。
- 禁止因模型缺失阻断单 Alpha或原生多 Alpha现有荐股。

### 5.5 实盘单日与历史批量双执行形态

H0 将“业务语义”和“执行拓扑”分离：

```text
LiveDailyExecutor ───────────────┐
                                ├─> authoritative day business composition
HistoricalBatchExecutor ────────┘     -> StrategyPackage day signal
                                      -> HMM/risk/tradability
                                      -> Selection projection
                                      -> AdvisoryListTransitionEngine
                                      -> day semantic result + receipt
```

- 实盘每天只传入一个交易日和当前正式 Program/binding，保持现有 after-close、target-open 和发布语义。
- 历史执行器传入冻结 date plan、source catalog 和 research identity，以默认5日 chunk 在同一 worker 内顺序处理；chunk 可调，但不改变日期计划。
- 共享业务层只接受日级 `AsOfDataView`，无权访问批量源的未裁剪数据。历史批量读取可以减少数据库往返，但每个视图必须同时执行 `business_date <= decision_date`、`available_at <= decision_timestamp` 和冻结 revision 约束。
- 静态模型、因子代码和配置在 package/manifest/model/factor/runtime identity 不变时整批复用；日期动态数据、HMM coefficient、风险事实、候选、名单状态和 receipt 不跨日复用。
- 批量模式不意味着把多个交易日合并为一个决策。每个交易日仍产生独立业务语义 hash、typed failure、artifact 和 checkpoint。
- 整个 artifact 的 batch/worker/timing 字段可以不同；逐日候选顺序、分数、stage trace、source refs、HMM/risk结果和名单动作必须与相同输入的单日执行语义一致。

详细组件、缓存键、失败恢复和等价性合同见 H0 详细设计。任何实现若需要另写 Selection、HMM、risk 或 list transition 算法，视为设计违例。

## 6. 模型功能

### 6.1 已冻结的SHORT_REBOUND selection/meta-label研究族

现行Selection继续产生生产基线候选和Top20顺序。P0-D至P0-L曾在固定P0-C数据、候选、feature schema v2和CORE/CORE_HMM家族内依次研究坏候选过滤、连续收益、换手约束、liability和局部重排；下列合同保留为历史事实和运行兼容边界，不再作为下一模型主线：

- group：同一 Program、package、`decision_as_of_trade_date/target_trade_date` 的 runtime-equivalent 候选 Top20。
- 输入：selection score/rank、父包腿分数与腿间分歧、HMM regime、行业状态、流动性、拥挤度、波动、可交易性和决策时可见的价格/资金特征；未来 episode path、MFE/MAE 和退出原因只能构造标签或评价，不能进入当时特征。
- 冻结 policy：生产 baseline 继续使用当前 Top20 Program policy。Top5 challenger 另建显式 `model_shadow_review_policy`，只把 `target_count/rank_enter_threshold` 固定为5，保留当前 `rank_exit_threshold=40`、确认天数、止盈止损、移动保护、20日 time stop、每日替换预算、entry/exit price basis 和成本口径。两者分别持有 hash；shadow policy 只用于研究标签和虚拟 episode，不修改生产 Top20 policy。任一政策变化产生新标签/model identity，不能静默复用旧模型。
- 候选级标签：对 target day 的每个候选独立建立反事实 entry，在既有预测文件中重建后续每日至少 `rank_exit_threshold=40` 的排名并继续追踪已持有 symbol，按同一 review policy 模拟 `rank_exit`、stop loss、trailing take profit 和 time stop，得到 realized episode net excess return、是否优于 skip/cash 以及 confidence target。只有 Top20 而没有后续 Top40/held-symbol rank 时不得伪造 rank-exit 标签。
- 输出：每个候选的 `take_probability`、`skip_probability`、`advisory_model_confidence`、Top5 challenger 和可解释 reason；系统不自动下单，也不生成资金仓位。
- 对照：selection 原始前5、现有 M5A reranker、HMM前5、meta-label Top5、随机5和候选20等权。

现有 M1/M5A 5日 LambdaRank 保留为已完成历史基线，不重写其结果。多期限复合 relevance 只可作为独立 matched 对照，不能用已消费的冻结 test 决定权重。

### 6.1.1 评价、研究族与选择偏差

- 当前 406 个 decision dates 和单一 80 日 test 无法证明约 `0.001` 级 lift；该 test 已被读取，后续只保留历史报告。
- 新模型的参数、阈值和候选 family 仅在 train/validation 的 purged rolling/CPCV 路径选择；purge 至少覆盖 review policy 的最长标签窗口，当前默认最长 20 个交易日。
- CPCV/PBO 或适用等价诊断用于报告 45 trials 及后续 family 的选择偏差，不是人工审批门禁，也不制造新的独立 OOS。样本或策略路径不足时显式 `NOT_COMPUTABLE`。
- P0-D至P0-L共享开发窗口且后续假设受前轮结果影响，全部归入同一`hypothesis_family_id`；单轮CPCV/PBO不得冒充跨轮独立OOS，未来DSR/PBO必须同时报告原始trial数、有效trial口径和相关性假设。
- 真正未来 OOS 以每日 challenger observation 及其成熟 episode 为主；不能因前向天数暂少就回头调已消费 test。
- 候选级 meta-label 指标只衡量 take/skip；最终 Top5 challenger 必须另行按冻结的 shadow portfolio policy 逐日组合重放，包含 target count、replacement budget、持仓继承和现金状态，不能用独立候选收益冒充组合 episode 收益。

### 6.1.2 P0-F 连续 policy utility 排序（历史冻结合同）

P0-D/P0-E证明继续调整binary take/skip loss不能稳定修复收益幅度。P0-F当时只改变一个实验变量：以P0-C `MATURED net_excess_return_bps`为连续目标，直接预测候选在冻结shadow policy下的净超额收益，并按预测utility重排exact Selection Top20。

- objective固定为LightGBM Huber regression，`alpha=0.90`；family仍只有CORE/CORE_HMM，seed和28-path CPCV roster不变。
- 每条path仅用train rows拟合median/MAD可逆仿射变换；不clipping、不winsorize，不使用validation或历史回放拟合transform。
- 每日排序键固定为预测policy净超额收益降序、Selection rank升序、instrument升序；entry priority由utility决定，selection exit和全部policy transition保持不变。
- winner仍由shared shadow portfolio的validation `mean_daily_net_excess_return_bps`选择，不由MAE、Spearman、PBO或历史回放选择。
- 冻结晋级合同要求同时超过exact P0-D和Selection，28-path对P0-D胜率大于50%，且配对平均回撤、换手不恶化。真实Stage A仅换手失败，已按合同完整负向终止，没有增加family、target transform、rank guard或blend。
- 因冻结CPCV合同未通过，新显式model role、runtime、descriptor和Stage B均未实现；禁止把regression score映射后伪装成take probability。
- 当前24决策日窗口已消费；P0-F未获准进入`HISTORICAL_REPLAY`，自然future OOS继续只积累既有P0-D challenger证据。

权威详细设计：`docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md`。

### 6.2 预期收益与持股周期

该组件已经使用同一历史候选矩阵训练并完成固定日期推理，作为各决策角色的辅助预测，不再被描述为当前第二主线：

- 多期限净收益分位数。
- 正收益概率。
- 信号存活概率和建议持股周期范围。
- MFE/MAE 与回撤风险。

第一版优先使用 LightGBM quantile/classification 和离散生存模型。它可以与 reranker 共用特征文件，但输出头和评价指标独立。

### 6.3 买入、止盈和止损区间

日线级价格范围已经完成；下一步仅在§6.7 Entry Guard证明需要时消费这些输出，分钟路径模型仍是条件增量：

- 第一版使用日线 Bin 的 OHLC、复权、波动、跳空、真实涨跌停价及收益/MFE/MAE模型，生成明确标记的日线级买入、止盈、保护和止损参考区间。
- 分钟 Bin 只用于后续明确批准的盘中买入时点、成交概率、事件先后和动态路径模型；不得让33GB分钟数据加载阻断第一版价格范围。
- 正式静态区间预测只需要数据库最新日线、昨收和涨跌停；只有输出盘中自适应区间时才读取数据库实时分钟行情。

输出必须是范围，不是保证价格；硬止损和行业黑名单不能被模型覆盖。

M4 executable 二分类头在 8120 行中只有 4 个负例，当前定义下不可学习。该头保持 `UNCALIBRATED` 并停止重复校准；下一轮只能先重审标签语义，或直接退役该二分类输出，连续 entry-gap quantile 和风险边界不受影响。

### 6.3.1 区间在线校准

M5B/M5C 已证明 validation 上拟合的全局常数无法处理 test 分布漂移。Adaptive Conformal/rolling calibration 仅作为前向标签成熟后的 matched 候选：按 Program、模型版本和区间头消费历史已成熟 residual，比较 raw/static/rolling coverage 与 width；没有成熟 observation 时保持原始 `UNCALIBRATED`，不得以规则区间冒充校准结果。

### 6.4 训练资源边界

- 所有训练只在WSL Conda环境运行，Windows只负责触发、读取结果和正式在线推理。
- 单个训练进程峰值内存必须低于8GB；基础数据按日期、股票和列投影分批读取，不同时全量加载多个H5或33GB分钟Bin。
- 允许把本次训练所需切片写成临时Parquet并结合内存缓存；禁止为此建设SQLite历史证据库、通用缓存平台或长期数据固化链。
- 首个真实训练目标在小时级完成。超过目标时先定位I/O、特征构建或训练瓶颈并做批处理优化，不得转向额外基础设施研发。

### 6.5 LONG_TREND

长期趋势策略包形成稳定候选文件后，复用同一文件训练和实时数据库预测架构，独立训练20至180日重排、有序收益、生存、time-to-hit和趋势捕获模型。LONG_TREND与SHORT_REBOUND不得共用同一个标签头。

### 6.6 分层oracle与learnability双诊断

oracle只用于定位瓶颈，不进入模型或运行时。所有层使用同一PIT股票池、上市年限、ST、停牌、涨跌停、成本、benchmark、winner label、执行价和冻结review policy，并登记开发窗口消费。

| Tier | 最小内容 | 放行语义 |
|---|---|---|
| Tier 1 | 全可交易股票赢家在Top20/40/50的召回；Top20内perfect Top5排序；rank分桶和成本后上限 | 天级诊断；先回答候选召回与排名动作空间 |
| Tier 2 | perfect Entry Guard、perfect Exit及其相对冻结基线policy的增量价值 | Tier 1身份与候选/排名诊断闭合后运行，回答风险管理动作是否存在独立空间 |
| Tier 3 | 固定等权槽位下允许空槽、保留现金和向后补位的frontier | 不包含动态资金权重；只有Tier 2需要现金动作时运行 |

每个Tier同时报告两种证据：

1. `CLAIRVOYANT_ACTION_CEILING`：使用未来结果测量候选池和动作空间的理论上限，永远标记不可部署。
2. `FIXED_CROSSFITTED_LEARNABILITY`：冻结一个简单模型族、超参、特征集、cross-fitting和判定线，只使用当时可见信息估计当前信息集能否逼近该机会；该审计不是新一轮模型搜索。

四象限处置固定为：

- 理论低、可学习低：该层降级，检查上游召回、股票池或policy动作空间。
- 理论高、可学习低：结论仅为“当前信息集+冻结简单模型不可学”；先扩信息集，再另立新lineage比较模型族，禁止先轮换loss或模型家族。
- 理论高、可学习高：允许创建一项确认性candidate，不能直接激活。
- 理论低、可学习异常高：先排查泄漏、标签、基线和评价错误，不据此晋级。

判定线使用成本、容量折损、最小经济收益和block/cluster置信下界共同定义，不预设全局固定bps。oracle与learnability不得读取sealed holdout。

### 6.7 Entry Guard独立能力

M4预测次日开盘gap分布和价格区间，但尚未回答“实际开盘价格到达后是否仍值得买”。Entry Guard冻结Selection顺序和现有Exit规则，只改变entry action：

```text
T close: reference_price + predicted_gap_q10/q50/q90 + max_acceptable_gap + max_buy_price
T+1 open/current: ACCEPT | REDUCE | SKIP | WAITING
```

- 至少比较无保护、固定3%、固定5%和冻结动态阈值；固定阈值是透明基线，不是所有策略/regime的默认生产规则。
- `REDUCE`仅是面向用户的谨慎建议状态，本阶段不输出数量、权重或资金分配；任何数值仓位语义需另行扩权。
- 首版`SKIP`不自动用第6名补位，固定槽位空缺和现金是合法研究结果；另行比较保留现金与向后补位，不形成动态仓位。
- 标签是“按实际可执行价进入”相对“冻结基线动作/跳过”的增量净价值，绑定entry policy hash、成本、价格基础和可交易性。
- 主要评价执行净收益、相对无保护基线收益、MDD、尾部损失、追高区间alpha、错过alpha、fillable rate、现金暴露、换手和真实干预支持度；胜率只作辅助。

### 6.8 Exit-label oracle与独立Exit能力

P0-H/P0-K的liability Spearman约0.25只证明当前policy下的持有/换手负担可预测，不证明最佳退出时机可预测。Exit首步是标签与action oracle，不直接训练正式模型：

- 在每个持有决策时点构造“下一可交易时点退出”相对“继续执行冻结baseline exit policy”的剩余净价值；同一shadow-policy模拟器生成两臂并绑定policy hash。
- 处理T+1、停牌、跌停、成本、删失、rank-drop、stop loss、trailing take profit和time stop；未来路径只作标签和评价。
- oracle通过后首版保持日频，候选输出`HOLD/REDUCE/EXIT_NEXT_OPEN/WAITING`、预测剩余收益、下行风险、time-to-hit、保护价和reason；`REDUCE`不携带数值仓位或自动交易语义。
- 主要评价利润回吐、避免亏损、过早退出机会损失、whipsaw、持有期、MDD、尾部损失、动作次数和跨regime覆盖。

### 6.9 上游alpha分流与角色化信号组合

若N1证明Top40/50赢家召回或成本后候选流不足，且N3综合分流确认上游为当前主瓶颈，主线转入QE/StrategyPackage上游alpha MVE，而不是继续修改Advisory下游模型。准备件可以并行完成数据身份、表达式grammar/operator白名单、生成预算、资源和registry schema；候选生成、IC/收益评价和结果驱动提示词调整必须等待分流。生成后的正结果还必须检查与既有因子/已知市场效应的重合、时间衰减和经济归因，避免把重复暴露或非线性复刻误报为新alpha。

P0系列输出不因存在局部信号而自动成为ensemble成员。进入组合前必须按角色分类并证明：独立时段残差相关较低、逐种子与种子平均相关结构稳定、LOO有边际增量、成本后仍存在、跨regime稳定，并附特征/SHAP归因及与已知效应的重叠检查。return/rank可以进入alpha组合；liability进入risk/Exit；M4 gap进入Entry Guard；HMM进入regime/risk。禁止把不同决策时钟的输出任意加成一个总分。

### 6.10 Frontier、确认与证据使用

未来实验使用`frontier -> candidate -> confirmation -> activation`四层合同：

1. frontier仅在inner-train按预注册网格报告收益、换手、现金、coverage和MDD的完整Pareto集合；PIT、泄漏、身份或可交易语义错误仍立即失败。
2. candidate只能依据inner-train和预定义经济效用选择一次；所有探索arm计入对应研究族trial数。
3. confirmation只评价冻结candidate。经济失败后整个frontier和窗口已消费，不得返回重选；只有代码/数据身份错误且未利用经济结果重新选点时，才允许同candidate exact retry。
4. activation独立使用对应objective contract、安全边界和prospective evidence；研究frontier不得自动放宽既有生产合同。

确认性实验预注册最低干预次数、干预交易日比例和regime分布，阈值由MDE及block/cluster有效样本量推导，不使用任意固定数字。探索性结果可导航，不可关闭方向、声称稳定效果或支持激活。

## 7. 荐股产品行为

### 7.1 多策略包

- 一个 Advisory Program 绑定一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。
- 同时支持多个 Program 独立运行。
- 不恢复页面内手工多策略包融合。
- 策略包进入系统时已经完成准入；Advisory 不做二次资产、因子、模型或可执行性验证。
- Program active binding 是模型 bundle 解析的唯一运行身份；单 Alpha 和原生多 Alpha 使用相同解析流程，一个 Program 缺模型不得影响其它 Program。
- 当前仅目标多 Alpha Program 有模型 bundle；“支持多个 Program 基线荐股”和“多个 Program 都有模型覆盖”必须分开报告。

### 7.2 实验模型展示

真实模型完成后可以立即在研究页面显示，状态固定为：

```text
EXPERIMENTAL_SHADOW
training_source = QE_FILE
calibration_state = UNCALIBRATED or PARTIAL
objective_contract = ALPHA_RANKING or RISK_MANAGED_ADVISORY
evidence_level = HISTORICAL_REPLAY or SEALED_HOLDOUT or PROSPECTIVE_OOS
```

实验状态不得描述为已校准概率或确定性收益，但不能因为尚未完成正式OOS、全量统计检验或ModelOps而隐藏真实模型排名。

### 7.3 Top5与每日列表

- Top5 shortlist是模型研究输出，不自动把现有 `target_count=20` 缩成5。
- 模型启用前不覆盖 `selection_effective_rank`。
- 每日活跃列表必须保持有界，显式记录 `ENTER/HOLD/EXIT/WATCH`；不得将每日候选简单并集。
- Entry Guard的`SKIP/WAITING`可以在固定等权槽位中形成空槽和现金，但不得输出动态资金权重；“保留现金”和“向后补位”作为独立matched arm报告。
- 是否把正式 active target 从20迁移到5，由用户在看到真实影子模型结果后单独确认。

### 7.4 每日前向发布与 episode

- `daily_after_close` 必须对应真实执行器，而不是仅保存配置。交易日 D 收盘且 Selection 输入就绪后，执行器为每个 ENABLED Program 生成 `decision_as_of_trade_date=D`、`target_trade_date=next_trading_day(D)` 的 baseline/challenger 推荐。不得读取 target 日行情或假设 target 日已经成交。
- `PUBLISHED` recommendation 与 episode entry 分阶段：D 收盘先发布目标日建议；到 target 日获得权威 `next_open_executable` 后，才按 Program 的 entry price basis 建立 episode。target 日价格缺失时保持 `WAITING_DATA`，不得回退 signal close 或伪造进入价格。
- 基线 list 始终来自该 Program 的 StrategyPackage/Selection 结果。模型存在时追加独立 challenger observation，不改变 `selection_effective_rank`、target count 或正式 episode 的当前基线语义。
- challenger 至少保存 Program/binding/package/model identity、decision/target 双日期、候选、take/skip/confidence、Top5、outcome/价格范围和 typed status。不得把按需 GET 响应冒充已持久化前向事实。
- 新角色challenger还必须保存`objective_contract`、role、baseline policy hash、decision clock、action、增量价值语义和evidence level；一个合同下验证不得改变另一个合同状态。
- baseline episode 仅由正式 Program review policy 推进；模型 challenger 的虚拟 episode 必须使用独立身份和冻结的 Top5 `model_shadow_review_policy` 模拟，不能混入基线排行榜或 Paper/模拟盘持仓。
- 运行失败必须记录具体 Program、日期、阶段和 reason；一个 Program 失败不阻断其它 Program，不能静默跳过并继续显示旧日期为最新结果。
- 本功能无资金、无下单、无 QMT 输入，是荐股研究的前向质量评价，不属于实盘交易执行。

## 8. Contracts / 最小实现合同

当前只允许实现直接支撑真实模型的合同：

| 合同 | 必需内容 |
|---|---|
| `QETrainingDatasetDescriptorV1` | H5/Parquet/Qlib Bin基础数据路径、Prediction Store PKL引用、目标父包roster、schema、日期、features、labels和split |
| `AdvisoryFeatureSchemaV1` | 训练/预测共享的特征名、dtype、单位和缺失值语义 |
| `AdvisoryTrainingRequestV1` | model family、参数、seed、WSL环境、输入和输出路径 |
| `AdvisoryModelBundleV1` | 模型文件、feature schema、label version、训练区间、指标和SHA256 |
| `AdvisoryPredictionV1` | program/package、decision/target双日期、候选、model score/rank、Top5、状态和reason |
| `AdvisoryOutcomePredictionV1` | 收益分位数、正收益概率、周期、MFE/MAE和状态 |
| `AdvisoryPriceRangePredictionV1` | entry/take-profit/protection/stop范围、单位、置信状态和reason |
| `AdvisoryForwardObservationV1` | Program/binding/package/model、decision/target双日期、baseline/challenger、Top5、预测、状态和成熟截止日 |
| `AdvisoryPolicyEpisodeLabelV1` | baseline或Top5 shadow policy/hash、entry/exit basis、成本、退出原因、持有期、净收益/超额收益、label maturity和censoring |
| `AdvisoryModelBindingResolutionV1` | active Program binding 到 exact package/style/schema/model bundle 的确定性解析；无 bundle 为 typed unavailable |
| `AdvisoryDaySemanticResultV1` | 与执行拓扑无关的逐日候选、stage trace、名单动作、source refs 和业务语义 hash；实盘单日与历史批量共同生成 |
| `AdvisoryPITAsOfViewV1` | 只暴露一个 decision date 可见的数据视图，绑定 cutoff、availability policy、source catalog/revision 和视图 hash |
| `AdvisoryHistoricalBatchExecutionV1` | 冻结 date plan、chunk policy、workspace identity、逐日 checkpoint 和 typed failure；不含新的选股算法 |
| `AdvisorySemanticParityReceiptV1` | 单日与批量在相同业务输入下的逐字段/逐 hash 等价结果，排除 batch/worker/timing 等运行信封 |
| `AdvisoryResearchTrialRegistryV1` | append-only JSONL研究索引；study type、family/lineage、唯一变量、目标合同、trial计数、消费窗口、decision use和证据引用 |
| `AdvisoryObjectiveContractV1` | `ALPHA_RANKING`或`RISK_MANAGED_ADVISORY`、冻结基线、评价指标、合法动作、展示和独立激活状态 |
| `AdvisoryOracleMiniContractV1` | 开发窗口、PIT股票池、winner/成本/容量/benchmark/执行价/review policy、Tier和不可部署标记；拒绝sealed holdout |
| `AdvisoryLearnabilityAuditV1` | 冻结简单模型、特征、超参、cross-fitting、判定线和四象限结果；失败不外推为全局不可学 |
| `AdvisoryIncrementalValueLabelV1` | role、两臂action、同一shadow-policy模拟器、policy hash、成本、价格基础、成熟/删失和增量价值 |
| `AdvisorySealedHoldoutContractV1` | 独立dataset/window identity、允许一次confirmation、访问拒绝、消费receipt和禁止回选 |
| `AdvisoryEntryGuardDecisionV1` | T日冻结阈值/价格，T+1权威open/current下的ACCEPT/REDUCE/SKIP/WAITING、固定槽位现金和reason |
| `AdvisoryExitDecisionV1` | 持有as-of时点的HOLD/REDUCE/EXIT_NEXT_OPEN/WAITING、剩余价值/风险、baseline policy和reason |

若现有 Program/list/episode/API 能承载字段，优先复用；challenger 与基线身份无法无歧义共存时才允许增加最小 Advisory 专用存储。H0 优先复用现有 Historical Range batch/run/day/artifact 合同，性能与校验 telemetry 先写任务 artifact。禁止为未来通用性预建模型注册中心、训练调度表、血缘仓库、审批表、第二套历史状态机或新 artifact 平台。

## 9. Implementation Plan / 历史进展与当前唯一后续顺序

### M0：QE文件可训练性核对

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED`。基础数据、Prediction Store、涨跌停、分钟边界、冻结 request 合同和 runtime-equivalent candidate coverage 已核实并实现；正式 request 为 `advmreq_ac5959aa8dc14a25e3b8c139`，真实文件训练覆盖 406 日、8120 个 Top20 候选。

- 绑定当前目标父包精确 roster、两个代表 seed/model SHA、zscore、terminal weights、raw Top25、Program target_count 和 runtime semantics hash。
- 完整 38 seed、逐日权重和 `combined_prediction.pkl` 只生成显式对照诊断，不进入在线 feature，也不替代 current runtime candidate。
- 冻结 decision/target 双日期和 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`，与正式预测入口保持一致。
- 绑定当前 QE H5/Parquet/Qlib Bin 基础数据，不复制或转换合法预测PKL。
- 只读核对日期范围、候选/特征/标签、缺失率和时间切分可行性。
- 选择覆盖最完整的一个真实训练范围。
- 冻结5日标签、最多5日延迟退出和10交易日 purge，显式排除各 horizon 尾部未成熟或跨 split 边界样本。
- 若首模合同必需字段缺失，继续只读搜索已有QE H5/Parquet/Qlib Bin或Prediction Store；仍不存在时报告精确阻断并停止M1，禁止静默简化特征或转向历史数据库证据工程。

完成判定：产生一个可由WSL读取的真实训练请求，不要求新数据库DML或新历史snapshot。

### M1：首个真实Top20→Top5模型

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED_EXPERIMENTAL_SHADOW`。WSL 真实训练生成 bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`，80 日 test 均有 Top5，峰值 RSS 约 2.11GB，总耗时约 129 秒。真实 test 平均超额收益低于原始排名与随机对照，因此 M1 完成时未激活；M2 随后为验证完整真实链路发布了该 exact shadow binding。运行时可用不改变其负面质量结论，也不得被隐藏或描述为优化成功。

- 实现QE文件reader、共享FeatureBuilder和WSL launcher/trainer。
- 使用当前文件数据从头训练HMM，固定状态映射并生成可从文件 cutoff 连续追加数据库观测的 posterior；旧HMM结果只进入对照报告。
- 在WSL训练真实LightGBM LambdaRank。
- 在时间留出集生成逐日Top5和基线对照。
- 保存可加载模型文件和最小manifest。

完成判定：真实reranker与本轮重新拟合的HMM均生成可加载模型、连续续推状态和非空留出预测；旧HMM产物未作为输入；Top5留出预测非空且无未来数据泄漏。shadow 使用与 holdout 相同的 HMM/reranker 参数，不单独 refit HMM。HMM失败不影响现有规则荐股，但不能把HMM增强对照标记为完成。

### M2：固定日期按需影子推理与页面

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。PR #3225 已合入；重启后运行时 commit 为 `41504a205b9372a4e709587dc2310fd8143c6c6d`。目标多 Alpha Program 在 `decision=2026-07-15 / target=2026-07-16` 返回 20 个真实候选、5 个模型 shortlist、exact bundle 和 11 个显式 HMM unavailable；单 Alpha Program 返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`。该 GET 依赖既有 REPLAY list/review，只证明单点推理；它没有生成 `PUBLISHED` list、调度执行记录或 episode。

- 实现数据库 decision-cutoff 特征source；目标交易日数据不得进入特征。
- 对当前单 Alpha或原生多 Alpha候选执行模型推理。
- API返回真实model score/rank/Top5。
- 页面展示`EXPERIMENTAL_SHADOW`结果和基线对照。

完成判定：从 persisted replay 候选、Advisory decision/target context 和数据库 decision-cutoff 行情到页面 readback 贯通；不修改 Selection、Paper 或模拟盘。该判定仅关闭 M2 单点推理，不关闭每日前向发布。

### M3：预期收益与持股周期

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。详细设计为 `docs/architecture/advisory_model_first_m3_outcome_holding_period_f2_design_20260809.md`；M3A bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` 含 46 个真实 LightGBM 模型、1600 行/80 日零 NaN test 预测，训练 108 秒、峰值 RSS 655,581,184 bytes。M3B 随 PR #3234 合入，merge commit 为 `84362027da8f6e87ec5b627a5b7df15b88c5763b`；outcome binding 已存在。运行时 commit `0ab6dec36c6bc05f7d9655de63b07bbd5353dfd2` 对固定日期的 20 个 persisted 候选完成推理，耗时 33.236 秒，五个 horizon 均非空；尚无每日 forward observation 或成熟 outcome。

- 在现有QE文件上训练收益分位数、正收益概率和周期模型。
- 接入同一Advisory预测和页面。

### M4：买入、止盈和止损范围

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`M4_POINT_INFERENCE_VERIFIED_NOT_FORWARD_RUNTIME`。详细设计为 `docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`。M4A request `advprreq_2d826a7b2704137bf3a60d9d` 在 WSL `rdagent-gpu` 生成 bundle `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3`：4 个真实 LightGBM heads、1600 行/80 日 test 预测、总耗时 13.85 秒、峰值 RSS 491,802,624 bytes。三个开盘缺口分位数零单调违例，q10-q90 test coverage 为 0.72795。可执行标签共 8120 行但只有 4 个权威负例，test 仅 1 个负例，因此 binary 输出保持 `UNCALIBRATED/EXPERIMENTAL_SHADOW`。M4B 源码、`market.dividend` 数据同步、exact binding 和用户重启均完成；2026-08-11 固定日期 readback 的 20/20 候选返回完整范围，但尚无每日发布或前向 coverage。

- 先使用日线Bin完成真实日线级价格范围模型。
- 盘中路径模型作为后续独立增量，只在用户确认需要时读取现有分钟Bin。
- 接入价格转换和硬风险边界。

### M5：模型质量迭代

历史优先级：`COMPLETED_NEGATIVE_RESEARCH`。

状态：`M5A_M5B_M5C_REAL_RESEARCH_COMPLETE_ZERO_ACTIVATION_RECOMMENDATIONS`。M5A、M5B、M5C 的真实 bundle 和负面质量结果见 §1.2；PR #3346 已合入 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`，但三个研究 bundle 均不激活。冻结 80 日 test 已消费，不得围绕这些结果继续调参。

- M5A 首先改善 Top20→Top5：当前 M1 test `mean_excess_return_5=-0.0002833`，明显低于 `selection_rank_top5=0.0085591`，不得把“模型已运行”误写为“模型排序有效”。
- M5A 在现有 406 日/8120 候选、103 特征和冻结 test 上做有限窗口、种子、模型配置及 selection-prior 混合比较；所有选择只使用 train/validation，test 只做一次最终报告。
- M5A 真实冻结 test 的 winner 平均 5 日超额收益为 `0.0071894`，原始 selection rank 为 `0.0085591`，lift 为 `-0.0013696`，95% moving-block bootstrap 区间为 `[-0.0093061, 0.0053392]`。结果说明 M5A 修复了 M1 明显为负的问题，但没有证明优于原始 selection，当前不替换已激活 binding，也不得围绕 test 继续调参。
- M5B 再处理 M3 概率校准与 quantile coverage；M5C 处理 M4 entry-gap quantile coverage。M4 executable 负例仅 4 条，禁止伪造二分类校准。
- 3/5 年实验只有在同一目标父包的合法既有预测确实存在时才执行，不能用其它实验或新模型回填；不得为 M5 新建历史证据、缓存或 ModelOps 平台。

### P0-A：每日基线发布与前向 observation

优先级：`COMPLETED_FORWARD_RUNTIME`。

状态：`SOURCE_MERGED_RUNTIME_PUBLISH_AND_SETTLEMENT_VERIFIED_FORWARD_ACCUMULATING`。

运行时证据：两个ENABLED Program均有decision `2026-08-13/14/17` 的PUBLISHED run；target `2026-08-14/17` 已SETTLED并各形成20个active episode。target `2026-08-18` 在开盘前保持WAITING_DATA，继续按同一时钟自然结算。

任务列表：

1. 设计并实现 Advisory 专用收盘后执行器，读取交易日和 ENABLED Program，调用既有正式 review 服务；不启动通用 scheduler 平台。
2. 每个 Program/target trade date 幂等产生一个 `PUBLISHED` baseline recommendation；D收盘发布与target日开盘后的 episode transition 使用同一 date context，但分阶段执行，复用现有 bounded `ENTER/HOLD/EXIT/WATCH` 语义。
3. 对有模型的 Program 在同次运行中生成并持久化 challenger observation；没有模型时写入 typed unavailable，不阻断 baseline publish。
4. 保存 decision/target、Program/binding/package/model、baseline/challenger Top5、outcome、价格范围和 maturity date；禁止只保存 GET 临时响应。
5. 页面与 API 分别展示 baseline 最新发布、challenger 最新发布、成熟 forward metrics 和明确失败状态。
6. 完成两个现有 ENABLED Program 的单日真实发布 readback；后续交易日由执行器自然积累，不回填 2026-07-17 以来历史缺口。

完成判定：两个 Program 均存在同一 target 日的真实 `PUBLISHED` recommendation；目标多 Alpha Program 存在同日模型 observation；单 Alpha Program 在无 bundle 时 baseline 成功且模型状态明确不可用。target 日开盘数据到达后，至少一个 episode 被创建，或所有未进入均有可解释的 `WAITING_DATA/NOT_ENTERED`；D收盘发布不得因尚无target open而伪造episode。服务重启和首次生产调度仍由用户分别确认。

### P0-B：动态 Program/package bundle 分发

优先级：`COMPLETED_DYNAMIC_BINDING`。

状态：`SOURCE_MERGED_DYNAMIC_BINDING_RUNTIME_FORWARD_VERIFIED`。

任务列表：

1. 用 Program active binding 替代 `target_binding.py` 中单一 Program/package/manifest 的运行时常量判断。
2. 通过 exact package/manifest/style/schema/model identity 查找 bundle，不扫描 latest、不跨 Program 共享状态。
3. 单 Alpha、原生多 Alpha 使用相同解析合同；不存在兼容 bundle 时返回 typed unavailable。
4. 保持已合入目标多 Alpha bundle 的字节级加载语义和现有 point readback。
5. 不为测试单 Alpha 包伪造模型；该包只有完成独立真实训练后才增加 bundle。

完成判定：目标多 Alpha point/forward 推理不回归，单 Alpha baseline 不受阻，新增真实 bundle 后无需修改源码常量即可被精确解析。

### P0-C：review-policy episode 标签与稳健评价

优先级：`COMPLETED_POLICY_DATASET`。

状态：`SOURCE_MERGED_REAL_FILE_DATASET_VERIFIED`。

任务列表：

1. 从现有 QE 文件价格和目标父包既有预测 PKL 确定性重放冻结的 Top5 `model_shadow_review_policy`；每个后续交易日至少重建 Top40，并对已持有但跌出 Top40 的 symbol 保留可判定的缺席 rank 语义，生成 entry、rank exit、stop loss、trailing take profit、time stop、成本和 benchmark 对齐后的 episode label。生产 Top20 policy 只作独立 baseline 对照，不被修改。
2. 明确 censoring、label maturity、停牌/涨跌停和缺价语义；不得读取 Paper、模拟盘或生产历史 episode 作为训练输入。
3. 生成 purged rolling/CPCV train-validation paths，purge/embargo 覆盖最长20日政策窗口。
4. 保存全部候选 family/trial 的 validation 路径结果；计算 PBO/等价选择偏差，不能计算时写 `NOT_COMPUTABLE`。
5. 候选级 take/skip 标签与 Top5 shadow portfolio 评价分开：后者逐日执行 target count、daily replacement budget、持仓继承和现金状态。
6. 已消费的80日 test只作为历史对照，不参与阈值、特征、窗口或模型选择。

完成判定：相同 request 确定性得到相同 episode label；边界日无未来泄漏；一个固定 policy 的 selection baseline 与 M5A 历史模型可在同一政策收益口径比较。

### P0-D：SHORT_REBOUND meta-label 模型

优先级：`COMPLETED_EXPERIMENTAL_SHADOW_NOT_ACTIVATED`。

状态：`DESCRIPTOR_ACTIVE_FORWARD_EVALUATION_MERGED_HISTORICAL_FORWARD_VALIDATED_MODEL_NOT_ACTIVATED`。

PR #3368 已合入 `458199cd902323e006ac23d3767c908637068fa8`；后续安全 descriptor rotation、exact runtime contract 与 maturity 修复也已合入并由用户重启。P0-D exact bundle 已作为 `EXPERIMENTAL_MODEL/UNCALIBRATED` challenger 接入，baseline和正式Program policy仍未被替换。

任务列表：

1. 训练真实 LightGBM take/skip/confidence 模型，消费 selection、策略腿分歧、HMM regime、行业、流动性、拥挤和可交易性特征。
2. 严格使用 P0-C 的 policy episode 标签与 train-validation paths；不使用未来 MFE/MAE/path 作为特征。
3. 同时报 selection Top5、M5A reranker、meta-label Top5、HMM、随机和 Top20 等权的政策净收益、hit rate、drawdown、turnover 和 coverage。
4. 固定模型后进入 P0-A challenger 前向发布；前向样本不足时只标记 `EVIDENCE_IMMATURE`，不回看旧 test 调参。
5. 仅当 validation 多路径与后续前向证据支持时，才提出新的 bundle 激活建议；系统不自动激活。
6. observation 到期后按同一 Top5 shadow policy、Selection Top40 exit context、冻结成本和数据库权威行情形成不可变 outcome/episode label与模型指标；固定持有期Top5收益、baseline Program Episode和open mark均不得替代。
7. 历史虚拟前向使用与正式推理相同的 exact bundle/scorer、同一 shadow policy 和数据库 PIT 行情，以批量虚拟时钟快速验证；输出与自然 future OOS 物理隔离。已用于模型判断的窗口必须标记已消费，禁止经调模后继续宣称新的历史 OOT。

完成判定：真实 WSL bundle、可重复 validation、PBO/选择偏差说明、每日 challenger observation、历史虚拟前向和 typed runtime 状态齐全；效果不佳也如实完成实验，不回到平台工程。当前 P0-D 已完成能力验证，但模型质量结论为不激活。

### P0-F：SHORT_REBOUND 连续 policy utility ranker

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md`。

Stage A源码已由PR #3758合入；停牌语义BUG-1180/1181已修复、完成重启验证并关闭。真实v2训练bundle为
`ff336eadb131cb6a3d431a846de4e9949ad984da1dcc4d9c231aa313886ebc10`：相对exact P0-D平均收益提高`2.612087 bps`、path win rate `53.57%`、相对Selection提高`5.578137 bps`、配对平均回撤改善`0.001286`，但配对平均换手增加`0.007419`。因此六项advancement只有换手失败，正式结论为`NEGATIVE_STOP_NOT_ADVANCED`；Stage B、runtime、descriptor和历史回放均禁止继续。

任务列表：

1. 复用exact P0-C dataset、feature schema和28 READY CPCV paths；不修改candidate、label、policy或成本。
2. 在每条path内以train-only median/MAD标准化连续`net_excess_return_bps`，训练固定Huber CORE/CORE_HMM × 3 seeds。
3. 预测值逆变换为`predicted_policy_net_excess_return_bps`，以确定性排序生成Selection Top20 entry priority；退出继续使用Selection Top40 rank。
4. 使用shared shadow portfolio评价并与exact P0-D/P0-E、Selection、HMM、random和Candidate20比较；candidate MAE/Spearman只作诊断。
5. 预注册advancement：对P0-D平均收益lift>0、path win>50%、对Selection lift>0，且配对平均回撤和换手不恶化。
6. advancement失败即发布负面离线结果并停止，不开发runtime/replay、不追加调参；这是一项完整实验终止，不是partial。
7. advancement通过才实现`policy_utility_ranker_with_meta_label_confidence`；entry由utility决定，概率继续来自exact P0-D binary model。
8. 历史回放只能标为`HISTORICAL_REPLAY`且不改变winner；自然future OOS继续按交易日形成并由用户决定是否激活。

完成判定：Stage A真实WSL 168 trial-path、不可变bundle、exact retry、PBO/paired comparison/advancement receipt齐全。若advancement通过，Stage B还需显式role、旧role兼容、isolated descriptor和历史回放；无论结果如何均不自动激活。

### P0-G：SHORT_REBOUND turnover-constrained policy utility

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0g_turnover_constrained_utility_f2_design_20260825.md`。

任务列表：

1. 复用exact P0-C dataset、P0-F feature schema v2、28 READY CPCV paths和shared shadow policy；不修改candidate、exit、policy或成本。
2. 用7716行成熟candidate episode的`holding_trading_days`构造与组合每日换手同单位的`2/(target_count*holding_days)`换手负担；3行涨停未入场和1行右删失不填默认label，其所在日期不参与constraint calibration；该未来字段只作label，禁止进入feature。
3. 每条outer path只在train blocks计算utility/liability MAD、固定shadow-price候选和exact P0-D train turnover budget，选择第一个满足预算的最小影子价格；validation不参与。
4. 用`net_excess_return_bps - shadow_price * turnover_liability`训练固定Huber CORE/CORE_HMM×3 seeds×28 paths，entry按预测adjusted utility排序，Selection Top40 exit保持不变。
5. 使用shared policy评价原始组合净收益、回撤和换手；train constraint、candidate loss、PBO、winner和advancement分别报告。
6. 复用P0-F六项advancement并要求每path constraint可行；任一失败完整负向停止，不追加price roster、family、seed、rank guard或blend。
7. 只有Stage A通过后才允许另行批准Stage B；P0-D概率、P0-F和P0-G bps score不得混淆。

完成判定：已完成真实WSL 168/168 trial-path、不可变bundle `433ff217...`、exact retry、资源/PBO/constraint/paired/advancement receipts；唯一失败门槛为相对P0-D换手`+0.004096`，已负向停止且未激活。

### P0-H：SHORT_REBOUND dual-head output-constrained utility

优先级：`HISTORICAL_COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`。

权威详细设计：
`docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md`。

任务列表：

1. 复用exact P0-C、feature schema v2、28 outer CPCV paths和shared policy；不修改candidate、exit、policy或cost。
2. 分别训练return bps与`2/(5*holding_days)` liability fraction/day两个Huber head，future holding只作label，预测按冻结policy物理边界clip。
3. 每条outer path在保留train blocks内执行nested inner OOF、二次purge/embargo和block-reset shared evaluation；outer validation不参与拟合。
4. exact P0-D预算也使用同一inner OOF dates和固定winner model spec重建；每个family/seed/path从固定8个multiplier选择第一个满足预算的最小price。
5. 两头rounds取inner best-iteration中位数，在full outer train refit后只评价一次outer validation；固定CORE/CORE_HMM×3 seeds×28 paths。
6. 沿用P0-F/P0-G六项advancement；P0-F/P0-G和head diagnostics/PBO只作诊断，失败禁止Stage B和结果后调参。
7. Stage A只生成immutable dual-head bundle和完整receipts，零DDL/DML、零runtime/descriptor/activation。

完成判定：已完成完整nested OOF双头Stage A、真实WSL 168/168 trial-path、不可变bundle `82afdb81...`、exact retry和资源/constraint/paired/advancement receipts；收益与path-win门槛失败，已负向停止且未激活。

### P0-I：SHORT_REBOUND grouped-rank return head with output constraint

优先级：`HISTORICAL_COMPLETED_INCOMPLETE_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV`。

权威详细设计：
`docs/architecture/advisory_p0i_grouped_rank_return_head_f2_design_20260826.md`。

任务列表：

1. 精确复用P0-H的P0-C数据、feature schema v2、28条outer CPCV path、shared policy、cost、liability label/Huber head和exact P0-D OOF换手预算。
2. 唯一新变量是return head：在每个decision date的成熟policy episode候选中，将`net_excess_return_bps`确定性映射为0..4 ordinal relevance，训练固定`rank_xendcg`。
3. score date不读取label；将同日20只候选的raw model score转为确定性`[0,1]`百分位，再与liability预测组合并在train-only OOF选择最小可行price。
4. outer validation只作shared policy评价；不参与relevance规则、类别词表、rounds、price、score transform、family、seed或winner拟合。
5. 固定CORE/CORE_HMM×P0-H相同3 seeds×28 paths=168；不增加LambdaRank对照、blend、label-gain搜索、price roster或历史回放选择。
6. 沿用P0-H相对exact P0-D的六项advancement；P0-H paired、rank diagnostics和PBO只作诊断。
7. Stage A仅生成immutable grouped-rank dual-head bundle和完整receipts，零DDL/DML、零runtime/descriptor/activation；完整负向结果也作为有效实验合入。

完成判定：真实WSL在完成前10条path的60个trial-path后，第11条path的CORE/20260813在冻结8档price上均不能满足exact P0-D OOF换手预算，按预登记规则立即`NEGATIVE_STOP_INCOMPLETE_CPCV`。不可变evidence-only bundle为`2378358...`，exact retry复用同一identity；无winner/model/PBO/advancement，不得扩展price roster或继续Stage B。已完成的60项return daily Spearman均值为`-0.002229`、NDCG@5为`0.385322`，说明该grouped-rank收益头未形成稳定收益排序信号；liability Spearman `0.236089`继续证明原liability机制有效。

### P0-J：SHORT_REBOUND Selection-prior residual return with OOF reliability shrinkage

优先级：`HISTORICAL_COMPLETED_INCOMPLETE_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV`（source PR #3811；fix PR #3821；bundle `eb8ade9b...`）。

权威详细设计：
`docs/architecture/advisory_p0j_selection_prior_residual_return_f2_design_20260826.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、P0-H liability head和exact P0-D OOF换手预算；P0-I只作为失败诊断，不作为模型或门槛reference。
2. 在每个inner-train成熟样本内，按`selection_effective_rank=1..20`计算收益中位数，并以样本数加权的非增isotonic回归形成train-only Selection收益先验曲线；score date只按rank查曲线，不读label。
3. 以`net_excess_return_bps - selection_prior_bps(rank)`为唯一return label，训练与P0-H同参数的Huber CORE/CORE_HMM残差头。
4. 聚合六fold成熟inner OOF残差预测后，使用预冻结零截距解析系数`alpha=clip(sum(pred*actual_residual)/sum(pred^2),0,1)`；零方差或非正可靠度显式得到`alpha=0`及typed receipt，不搜索weight、不读取outer validation。
5. 形成`anchored_return_bps=selection_prior_bps + alpha*predicted_residual_bps`，再与P0-H liability预测及固定8档price进入相同output constraint；Selection Top40 exit和停牌/涨停处理不变。
6. 固定CORE/CORE_HMM×原3 seeds×28 paths=168；沿用实际代码权威`build_policy_utility_advancement_receipt`的六项门槛，P0-H/P0-I和prior/residual diagnostics只作诊断。
7. Stage A只生成immutable request/bundle和完整receipts，零API/UI/DDL/DML/runtime/descriptor/activation；负向结果禁止同结果后调参。

完成判定：正式Stage A在首条outer path的inner block 3发现Selection rank中位数高度非单调，decreasing-isotonic prior完全平坦为`-1.5357 bps`，按预登记规则以`ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE`立即`NEGATIVE_STOP_INCOMPLETE_CPCV`。evidence-only bundle `eb8ade9b...`和exact retry identity均已验证；零trial、无winner/PBO/Stage B，禁止扩展prior、alpha、family、seed或继续调参。

### P0-K：SHORT_REBOUND Selection-preserving liability gate

优先级：`COMPLETED_VALID_NEGATIVE`。

状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED_NOT_ACTIVATED`。

权威详细设计：
`docs/architecture/advisory_p0k_selection_preserving_liability_gate_f2_design_20260828.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、P0-H liability label/Huber head和exact P0-D OOF换手预算。
2. 不训练或组合任何return、rank、prior、residual或confidence head；唯一模型输出为`turnover_liability_fraction_per_day`。
3. 冻结1/2/3/5/10/20日物理holding roster，对应最大liability `0.4/0.2/0.13333333333333333/0.08/0.04/0.02`；每个family/seed/path只在自己的inner OOF选择满足完整性和exact P0-D预算的最宽松阈值，并冻结P0-H相同的385/386约束日期identity。
4. gate只过滤新的ENTER候选；held candidate继续由Top40/review policy退出。eligible候选内严格按Selection rank和instrument tie-break排序，不允许模型细粒度重排。
5. 每日过滤后必须保留至少5个candidate，active-slot coverage和cash-day不得劣于matched Selection；禁止少荐股或空仓冒充低换手。
6. 固定CORE/CORE_HMM×3 seeds×28 paths=168；outer validation不参与threshold、rounds或roster拟合，168项完整后才按每个family/seed的28-path平均主指标选择winner，并沿用实际代码权威六项advancement。
7. Stage A只生成immutable request/bundle和完整receipts，零API/UI/DDL/DML/runtime/descriptor/activation；负向结果禁止同结果后调参。

完成判定：正式 request `advselgatereq_943f9e551d5fee35e57340cc`在合入源码上完成真实WSL `168/168`和exact retry，生成bundle `fee9b561...`。winner为CORE/20260813，liability日Spearman `0.254589`，但全部168条trial均在最宽`0.4`阈值停止、拒绝数为0、主指标与Selection逐位相同。相对P0-D收益`-2.966049 bps`、path win`32.14%`、MDD差`-0.004162`，仅换手改善`-0.068009`；结果为`NEGATIVE_STOP_NOT_ADVANCED`。`PBO=1.0`由六个arm策略分数完全相同导致，不作为普通PBO过拟合结论。P0-K不进入Stage B/runtime/replay，不扩展绝对阈值。

### P0-L：SHORT_REBOUND P0-G-anchored liability local reranker

优先级：`COMPLETED_VALID_NEGATIVE_RESEARCH_FAMILY_FROZEN`。

状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV_NOT_ACTIVATED`。

权威详细设计：
`docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md`。

任务列表：

1. 精确复用P0-C数据、feature schema v2、28条outer CPCV path、shared policy/cost、exact P0-D、P0-G `433ff217...`和P0-H/P0-K liability证据。
2. 固定P0-G winner `FAMILY_TURNOVER_CONSTRAINED_CORE/20260817`为唯一收益anchor；不改变P0-G family、seed、objective、rounds或shadow-price selector合同，inner/path-local price仅在各自train边界机械重算，不训练新return head。
3. 训练P0-K同合同的liability CORE/CORE_HMM×3 seeds，并在同一nested OOF中生成anchor rank和liability rank。
4. 冻结relative liability gain roster `(12,8,4,1)`；只考察anchor `(1,2)..(5,6)`，每个日期最多执行一组稳定相邻swap，每个候选相对anchor最多移动一位，只改变ENTER priority。
5. identity control必须精确复现P0-G但不得成为candidate winner；每条path只在自身outer-train OOF选择最保守、至少产生一次真实ENTER差异且满足exact P0-D换手预算的干预档。
6. winner必须形成真实priority/entry干预；Top20、active-slot和cash不得劣化。少于两个唯一block-score vector时PBO显式标为不可解释且不输出伪数值，但PBO保持diagnostic-only，不私增第七项advancement。
7. 固定2 family×3 seed×28 path=168，outer validation只评价；沿用相对P0-D/Selection收益、path win、MDD、换手和完整性的六项advancement，P0-G只作paired诊断。
8. Stage A仅生成immutable request/bundle和receipts；零API/UI/DDL/DML/runtime/descriptor/activation，只有`ADVANCED_TO_STAGE_B`才另行设计后续。

完成判定：P0-L设计PR #3951、源码PR #3959、BUG-1251 anchor日期职责修复PR #3967和close-sync PR #3969均已合入。BUG修复后的正式request `advp0lreq_b86425d3b5ce508904fa01b0`生成evidence-only bundle `4476afeb...`。identity control精确复现P0-G但无真实干预；gain `12/8/4/1`分别产生`33/71/85/85`次实际entry变化，且换手均低于P0-D OOF预算，但四个非零档均使cash day从`1`增至`2`并降低active-slot coverage，按冻结合同均非完整候选。Stage A在第一条outer path、首个family/seed的train-only selector以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`停止，结果为`NEGATIVE_STOP_INCOMPLETE_CPCV`、`0/168`、无winner、无可计算PBO、无Stage B、无runtime/descriptor/activation；exact retry复用同一bundle identity。P0-L不再是待实现任务，P0-D至P0-L研究族按本蓝图冻结。

### N0：最小研究控制面与父包预测延伸spike

优先级：`COMPLETED_CONTROL_PREREQUISITE`。

状态：`FORMAL_COMPLETE`。

1. 建立只追加JSONL trial registry和由其生成的单页活动路线；首批回填仅登记P0-D至P0-L、已消费80日test、24决策日历史回放和44日H0窗口的既有身份与结论，不重算结果、不补建证据平台。
2. 对目标父包执行只读/离线prediction extension spike，输出`FROZEN_MODEL_CAN_INFER`、`HISTORICAL_PREDICTION_ONLY`或`RETRAIN_NEW_LINEAGE_REQUIRED`三态及资源估计。
3. 冻结开发窗口与未来sealed holdout身份；holdout访问拒绝在任何oracle或learnability执行前生效。
4. N0不生成新因子、候选模型、IC或收益证据，不改变生产descriptor、Selection或运行时。

当前事实：独立F2详细设计为`docs/architecture/advisory_n0_research_control_f2_detailed_design_20260830.md`。正式root为`F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/`，completion semantic SHA256为`9b460c294472daddd02375f0e68752dd741ab43617b9d0af1b6213c0b3ee9a9`，父包状态为`FROZEN_MODEL_CAN_INFER`。window contract固定P0-C开发窗口与唯一`SEALED_UNCONSUMED` holdout，N1结束后registry为16条、SHA256 `8d7ae5cd...`，派生route SHA256 `1b8164ce...`并指向N2。旧candidate root只保留为历史候选，不再作为当前状态依据。

完成判定：已满足。registry可机器区分study/objective/decision use并生成无互斥状态的路线页；父包spike给出可复核三态；sealed holdout对开发命令不可见。N0不再是后续任务。

### N1：Tier 1候选/排名oracle与固定learnability audit

优先级：`COMPLETED_DIAGNOSTIC`。

状态：`FORMAL_COMPLETE_DIRECTION_INCONCLUSIVE_SOURCE_PR_PENDING`。

1. 仅在开发窗口计算全可交易赢家的Top20/40/50召回、Top20 perfect Top5、rank分桶、成本与容量折损后的clairvoyant上限。
2. 使用预注册的单一简单模型族、超参、特征和cross-fitting生成learnability结果；不得搜索模型族、loss、阈值或特征组合。
3. 同时报告真实干预支持度、block/cluster置信区间和四象限分流；判定线由MDE、成本、容量和最小经济收益推导。
4. oracle与learnability均登记为已消费开发诊断，不得进入sealed holdout、模型特征或运行时。

当前事实：F2详细设计为`docs/architecture/advisory_n1_tier1_oracle_learnability_f2_detailed_design_20260831.md`。正式request `advn1req_d8116a79d72d9c1813485053`绑定compute commit `f6f2edb93b8df8078795332c75db3505b9e9c5bb`，不可变bundle为`74827d03...`。PIT、成本、容量、H20时钟、全市场winner、Top50和N1标签区间CPCV均已闭合；oracle与learnability分别登记，sealed holdout未读，runtime/DDL/restart均为noop。交付恢复commit `c64f052b...`只处理Windows/WSL路径别名和route恢复，不改变bundle或结果。

结果：全市场赢家Top20/40/50召回极低，明确暴露上游候选召回瓶颈；Top20内clairvoyant空间很高。固定Ridge虽有`98.83 bps`正point lift，但95%下界`1.18 bps`未越过5 bps经济门槛且MDE高于point，不能证明当前信息集可学习。typed结果保留point象限但为`INCONCLUSIVE`，不得把理论上限当可部署模型，也不得把探索性正point当方向确认。

完成判定：已满足；N1科学诊断、bundle、registry和route完整。源码PR/合入是独立工程状态，不改变N1结果。下一步必须进入N2，不重复N1或派生新的同信息集Ridge/loss变体。

### N2：Entry Guard、Exit-label与QE上游准备

优先级：`AFTER_N1_BEFORE_N3_AUXILIARY_DIAGNOSTICS`。

状态：`N2A_FORMAL_COMPLETE_SOURCE_MERGED_N2B_V1_ABORTED_NO_EVIDENCE_V2_SOURCE_READY_FORMAL_RUN_PENDING`。

- N2-A 的权威详细设计为`docs/architecture/advisory_strategy_package_three_arm_alpha_audit_f2_detailed_design_20260831.md`。它固定比较`LSTM_ONLY`、`FUNDGROWTH_ONLY`和`IC_WEIGHTED_PARENT`，复用N1 development window、canonical PIT、H20 outcome、成本和benchmark，在共同预测交集上报告full-universe IC/RankIC、Top20/40/50赢家召回、Top5、oracle、腿间相关和组合配对边际增量。
- N2-A是零模型trial的`ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`。Top25与Top50引用同一prediction identity，只能作为候选深度/政策参数，不能冒充两个Alpha；季度只作预冻结sensitivity，不允许结果后选择窗口、权重或arm。
- N2-A不替代Entry Guard或Exit-label诊断，也不提前生成QE新Alpha。它先回答现有父包的Alpha由哪条腿贡献、组合是否优于最佳单腿，再把结果作为N3上游StrategyPackage/QE分流的输入之一。
- N2-A正式request `advalpha3req_e7295a31a4e1953e9048cec5`绑定clean compute commit `30a8a74e...`，bundle `6784df1a...`已通过manifest inspect和exact retry；registry总数由16增至17，route hash刷新为`db910830...`且next task保持`N2_ENTRY_EXIT_QE_PREPARATION`。正式运行trial=0、sealed holdout未读、runtime/DDL/DML均为noop。
- N2-A源码PR #4048已合入merge commit `55d376b3...`，任务worktree、本地分支和远端分支均已精确清理；源码交付不改变formal bundle identity、registry结果或生产运行时。
- 正式结果显示：LSTM/FUND/父包RankIC为`0.11677/0.05628/0.12284`，Top5 H20净超额为`397.89/245.73/446.52 bps`；父包相对FUND的RankIC与Top5增量95%区间均大于0，但相对LSTM的两项区间均跨0。LSTM与FUND score相关仅约`0.18`，父包与LSTM约`0.90`；因此FUND包含不同信息但较弱，固定IC组合尚未证明稳定优于LSTM。父包Top50召回`1.7617%`虽为随机期望`1.75×`，绝对候选召回仍低，继续支持扩大候选源而非在同一信息集更换loss。
- 2026-08-31只读清单原有19个registry包、7个未退役包；当前Top25/Top50父包共享prediction SHA256 `e0c571f...`，仍只算一个组合信号。除当前两腿外，当时识别出的三条独立旧单Alpha均无现成Prediction Store历史预测，但单日frozen-runtime可产性曾逐一通过。该可产性只说明artifact可执行，不证明PIT安全、共同窗口Alpha或当前生命周期合格。
- N2-B v1冻结request `advpkgareq_ef3d0abb...`原计划比较父包与`pkg_378eb9...`、`pkg_5a5ccb...`、`pkg_b668f8...`。运行约6小时后，第三包使用的`neg_vol_adjusted_momentum`被独立追加不变性检查证实存在PIT因果违规，进程退出且未发布bundle、未追加registry、未形成任何可用于导航或激活的研究证据。该失败只消费工程运行，不得解释为第三包收益差或据此选择剩余包。
- BUG-1302已在DEV完成因子隔离、策略包退役和重启后验证：`pkg_b668f8...`当前为`RETIRED`，旧因子禁用且不可rehab；PIT正确替代`neg_vol_adjusted_momentum_pit_v2`为独立新因子identity，正式评级D、保持禁用、当前没有StrategyPackage引用。退役包不得继续进入v2，也不得把替代因子静默注入旧包重写其身份。
- N2-B v2权威详细设计为`docs/architecture/advisory_independent_strategy_package_alpha_audit_f2_detailed_design_20260831.md` v1.6。新request/schema/experiment identity固定绑定已验证BUG-1302 exclusion receipt、被排除包id/status/manifest和违规因子名；仅比较`CURRENT_IC_PARENT`、`pkg_378eb9...`与`pkg_5a5ccb...`三臂。两个存续包共享57因子closure，执行386个primary group run、3个causality diagnostic和1个file-backed parity，共390个执行单元；每个冻结模型只加载一次。所有三组pairwise比较复用相同PIT/H20/cost审计，仍为0-trial `ORACLE_DIAGNOSTIC/NAVIGATION_ONLY`。
- v2不是对v1 request的续跑或修改：必须从合入后的新源码冻结新request和独立artifact root；`resource_max_wall_seconds=null`，墙钟只记录、不自动终止，RSS/temp、COW输入隔离、decision-date投影、逐值/dtype/index/hash parity、精确计算计数和sealed holdout拒绝继续fail closed。`pkg_378eb9...`与`pkg_5a5ccb...`的原生label、窗口、执行和成本不一致，原生Sharpe/RankIC只作inventory，正式结论只能来自共同窗口输出。

- N1的候选/排名身份和typed结果闭合后，一个边界明确的辅助工作包可以并行包含：Entry Guard最小matched研究闭环，以及Exit增量标签/oracle；二者分别冻结Selection/下游动作、目标合同和policy hash，同一时刻只允许一个进入candidate训练/confirmation。
- Entry Guard比较无保护、固定阈值、动态阈值、现金/空槽和补位，不输出资金权重；Exit先验证“现在退出相对继续基线policy”的动作空间和信息可学性，不把liability预测直接当Exit模型。
- QE上游只准备数据身份、表达式空间、生成预算、资源和registry；该无证据准备可与N0/N1并行且不计作实验线，N3分流前不生成或评价alpha候选。
- 同一时间最多一条主线和一个独立辅助工作包；N2不得演变为三个并行模型项目。

完成判定：Entry和Exit分别绑定决策时钟、objective contract、policy hash和增量价值标签；所有结果为独立shadow，未形成动态仓位、下单或Selection写入。

### N3：四象限分流后的唯一模型主线

优先级：`AFTER_N1_AND_REQUIRED_N2_DIAGNOSTICS`。

状态：`WAITING_N1_AND_REQUIRED_N2_DIAGNOSTICS`。

- Top40/50赢家召回或候选流不足：进入QE/StrategyPackage上游alpha MVE。
- 召回充足且Top20理论、learnability均高：进入包含新信息的Top20 ranker；禁止继续P0同信息集loss/model轮换。
- 排名空间低而Entry/Exit空间高：只推进对应Entry或Exit主线。
- 全部空间低：审查PIT股票池、候选生成和review policy动作空间，不以复杂模型掩盖低上限。

每条主线建立新hypothesis lineage和frontier合同，只能从inner-train选择一次candidate；confirmation失败后不得回选。

### N4：信号组合、重训窗口与prospective activation

优先级：`AFTER_ONE_ROLE_HAS_CONFIRMED_INCREMENTAL_VALUE`。

状态：`WAITING_CONFIRMED_SIGNAL`。

只有一个角色形成独立确认增量后，才执行逐种子/种子平均稳定性、残差正交、LOO、成本后组合和模块factorial ablation。随后比较expanding、不同rolling window和regime-weighted重训；不预设三个月周期，不建立自动重训平台。历史回放、一次sealed holdout和自然prospective OOS分级报告，激活按objective contract独立决定。

### H0：实盘单日与历史批量同核执行

优先级：`H0_AFTER_CURRENT_V6_GOLDEN_FREEZE_PARALLEL_WITH_FORWARD_MATURITY`。

状态：`USER_APPROVED_GOLDEN_FROZEN_IMPLEMENTATION_READY_NOT_STARTED`。

详细设计：
`docs/architecture/advisory_live_daily_historical_batch_shared_kernel_f2_detailed_design_20260816.md`。

任务列表：

1. 以已完成44日A/B/C回放的冻结输入、逐日业务产物和可用性能收据作为golden baseline；不得修改、覆盖或回写其code-release和旧run。
2. 将实盘单日和历史批量执行器收敛到唯一的日信号、Selection/HMM/risk/tradability 与 list transition 业务组合，禁止复制回测专用选股算法。
3. 历史执行采用持久 worker 和默认5日 chunk；静态工作区按内容 identity 复用，日期动态目录、PIT 视图、结果和 checkpoint 逐日隔离。
4. 批量读取必须通过 `AdvisoryPITAsOfViewV1` 向日内核授权；增加未来行/未来修订毒化测试，证明较早日期语义 hash 不变。
5. 当 A/B 的 raw-affecting identity 完全相同时只计算一次 raw Alpha，并发布不可变共享 artifact；B 的 HMM/risk/tradability 从 raw 后分叉，任何 raw identity 差异都拒绝共享。
6. source validation 改为批次 full seal、chunk 前后 revision token、逐日实际读取 receipt；无可靠 token 或发现漂移时执行 full rehash，禁止仅以日期 cutoff 取代 revision/PIT 校验。
7. 在单 worker、零额外并发条件下先完成语义等价和资源基准；只有内存、I/O 和失败注入验收后才评估并发，不能以并发掩盖重复工作。

完成判定：代表日与完整 golden 窗口的逐日业务语义全部等价；未来毒化、缺 revision、chunk 中断、exact resume、缓存污染和 A/B raw 共享反例测试通过；性能收据分阶段报告 workspace、source validation、raw inference、overlay、publish 的耗时/I/O/RSS。性能目标是验证指标而非业务成功门禁，未达到目标时如实保留结果，不改变语义换取速度。

### P1-A：outcome/price rolling adaptive calibration

优先级：`P1_AFTER_FORWARD_LABELS_MATURE`。

状态：`WAITING_FORWARD_LABELS`。

- 比较 raw、M5B/M5C static 和 rolling/adaptive conformal 的 coverage、width、Brier/ECE 及分 regime 稳定性。
- 校准状态按 Program/model/head 隔离；无成熟 residual 时保持 raw/uncalibrated。
- M4 executable binary 头停止重复校准，先完成标签重审；连续 gap quantile 独立保留。

### P1-B：策略条件化共享实验

优先级：`P1_WHEN_AT_LEAST_TWO_COMPATIBLE_PACKAGES_HAVE_REAL_BUNDLES`。

状态：`NOT_READY`。

- 先以多个 Program 独立 bundle 为基线，再做 strategy-style/regime conditioning 的 pooled/multi-task matched 实验。
- compatible set、共享参数和 leave-one-package-out 泛化必须显式记录；不得因样本少默认合并异质策略包。
- 只有 matched 结果优于独立模型且无明显负迁移时才提出共享 bundle，不形成自动门禁。

### P2：长期趋势专家

优先级：`P2_WHEN_PACKAGE_READY`。

- 使用长期趋势包对应的现有 QE 文件训练独立模型，并复用 P0-A/P0-B 的前向发布和动态 bundle 分发。
- 标签必须按长期趋势自己的 review policy、生存/time-to-hit 和20至180日目标构造，不复制短反弹5日或20日标签。

### 用户未来单独确认后才可恢复的可选任务

以下任务不属于当前路线；P0-A 的前向运行只保存今后自然产生的业务事实，H0 也只优化已授权回放执行，不解禁任何历史补账：

- 历史证据、历史数据固化和归档。
- Phase 1R完整历史链E2E。
- 新建通用 Source Catalog、SEALED、CAS、invalidation和GC平台；H0 仅复用现有 catalog/CAS 并定向优化重复校验和 raw artifact 共享。
- 自动训练、ModelOps、漂移监控、灾备和治理后台。
- 旧batch/root/orphan清理或修复。

## 10. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-101 | 基础历史数据只读取现有QE H5/Parquet/Qlib Bin；预测/模型产物允许PKL；训练不读取生产数据库历史数据 |
| F-102 | 真实训练仅在WSL Conda执行，Windows不训练 |
| F-103 | 首模是实际LightGBM模型，不是mock、规则或随机结果 |
| F-104 | M1/M3/M4历史切分按既有合同保留；P0-C新policy标签按最长20日及实际退出窗口重算purge/embargo，不照搬旧5日/25日边界 |
| F-105 | 训练与正式预测共享同一逐列FeatureBuilder/schema、公式、单位、missing和decision cutoff |
| F-106 | 正式预测区分decision/target双日期，只读取数据库decision cutoff行情和实际Program输入 |
| F-107 | 真实模型输出Top5并与原始/HMM/随机/等权基线比较 |
| F-108 | 页面可展示明确标记的`EXPERIMENTAL_SHADOW`真实结果 |
| F-109 | 模型失败不阻断现有荐股，不用基线冒充模型；bundle只按exact shadow binding加载，不扫描latest |
| F-110 | 多个Program的基线荐股独立运行；当前模型覆盖仅限目标多Alpha Program，单Alpha typed unavailable不得冒充模型支持完成 |
| F-111 | 收益、周期和价格范围均来自真实模型；规则只作为单独标记的对照或硬风险边界，不能替代模型结果或伪造概率 |
| F-112 | Selection、Paper、模拟盘、QE资产和策略包业务逻辑零写入 |
| F-113 | 不新增角色、审批、package二次准入或未经确认的门禁 |
| F-114 | 不处理旧历史任务、旧root、归档和非阻碍性平台工作 |
| F-115 | 进度分开报告组件实现、固定日期推理、每日发布、episode、Program模型覆盖和质量净增量；不再汇总为易误读的总体百分比 |
| F-116 | 原生多Alpha首模按当前代表seed、zscore和terminal weights重建runtime-equivalent候选；完整seed/逐日权重/combined只作诊断，不跨实验拼腿或制造伪seed特征 |
| F-117 | 历史涨跌停直接读取日线/分钟Bin中的状态、价格和昨收，不重复建设`stk_limit`训练文件 |
| F-118 | HMM按当前文件数据从头拟合、确定性规范状态并保存可续推posterior；shadow不单独refit HMM，旧模型/状态/系数只作对照 |
| F-119 | 分钟Bin不阻断Top5、收益、周期或首版日线价格范围；只有盘中路径模型明确需要时才消费 |
| F-120 | 首模只要求沪深300；其它宽基指数在模型合同明确需要前不补充、不阻断 |
| F-121 | WSL训练峰值内存低于8GB并以小时级完成首模；采用列/日期/候选分批与临时Parquet，不建设新缓存或证据平台 |
| F-122 | M5A 所有窗口、种子、模型配置和融合权重只由 train/validation 选择；冻结 80 日 test 仅在 winner 固定后评价一次 |
| F-123 | M5A 必须同时报告模型、selection rank、HMM、随机和 Top20 等权基线；质量差如实保留，不用 hidden fallback 或 test 调参 |
| F-124 | selection-prior 混合是显式模型合同并保存权重，不把规则基线冒充模型；多个 Program 仍按 exact package/style binding 隔离 |
| F-125 | M3 概率校准只使用 validation 拟合并独立报告 discrimination 与 calibration；M4 二分类负例不足时保持 `UNCALIBRATED` |
| F-126 | M3/M4 quantile 校准不得读取 test 标签或实时未来行情；模型发布、binding、重启和 deployed readback继续分开报告 |
| F-127 | ENABLED Program 的 `daily_after_close` 对应真实 Advisory 执行器，按交易日幂等产生 `PUBLISHED` baseline list，不回填旧缺口 |
| F-128 | baseline、model challenger 和 replay 身份分离；按需 GET、源码合入、bundle激活、每日发布和前向成熟分别报告 |
| F-129 | challenger observation 持久化 Program/binding/package/model、decision/target、Top5、outcome/价格区间、状态和成熟截止日 |
| F-130 | Program active binding 动态解析 exact bundle；移除单一 Program/package 运行常量限制，无 bundle 时基线继续且模型 typed unavailable |
| F-131 | 历史P0-D SHORT_REBOUND meta-label只学习take/skip/confidence，不自动下单或生成资金仓位，也不冒充候选召回或未来角色模型 |
| F-132 | meta-label 使用独立冻结的Top5 shadow policy模拟episode净收益，绑定policy hash、价格基础、成本、退出原因、成熟与删失语义；生产Top20 policy不变 |
| F-133 | 模型选择只使用 purged rolling/CPCV train-validation；冻结80日test不再选择；PBO/等价诊断不可计算时显式记录而不形成审批门禁 |
| F-134 | 真正未来OOS来自每日 challenger observation 和成熟 episode；前向样本少标记证据未成熟，不回看冻结test调参 |
| F-135 | Adaptive conformal 只在前向 residual 成熟后做 matched 实验；M4近单类 executable binary 停止重复校准并先重审标签 |
| F-136 | 跨包共享只在至少两个兼容包有独立真实bundle后做 matched/LOO 研究，不默认共享、不掩盖负迁移 |
| F-137 | 每日前向研究无资金、无下单、无QMT/Paper/模拟盘写入；不新增角色、审批、策略包二次准入或历史证据平台 |
| F-138 | rank-exit标签必须从既有预测文件重建后续至少Top40并处理held-symbol缺席；只有Top20时不得伪造完整review-policy标签 |
| F-139 | 候选级take/skip标签与Top5 shadow portfolio评价分离；后者真实执行target count、replacement budget、持仓继承和现金状态 |
| F-140 | D收盘发布绑定decision=D/target=下一交易日且不读取target行情；episode只在target日权威next-open到达后进入，缺价保持WAITING_DATA且不回退signal close |
| F-141 | 实盘单日与历史批量仅执行拓扑不同，共享唯一逐日业务语义；禁止第二套回测选股、HMM、risk或名单算法 |
| F-142 | 历史批量日内核只能读取绑定decision cutoff、availability和source revision的PIT AsOfDataView；未来数据毒化不得改变较早日期结果 |
| F-143 | 静态工作区按package/manifest/model/factor/runtime内容identity复用，日期动态数据和结果逐日隔离；缓存错误不得静默重建为成功 |
| F-144 | A/B只在raw-affecting identity完全相同时共享不可变raw Alpha artifact；HMM/risk/tradability在raw之后分叉，identity差异必须拒绝共享 |
| F-145 | source validation使用批次full seal、chunk revision token、逐日读取receipt和异常full rehash；不得用单纯日期过滤替代revision校验 |
| F-146 | 每个历史交易日独立artifact、typed failure和checkpoint；raw可预计算，顺序相关的list/episode transition在首个未完成日停止并exact resume |
| F-147 | 当前v6冻结为golden baseline；代表日和完整窗口以业务语义hash验证batch与single-day等价，运行信封字段单独比较 |
| F-148 | 首版使用单持久worker和默认5日chunk，先验证内存/I/O/恢复再评估并发；并发不是业务完成或性能验收的替代 |
| F-149 | H0不修改实盘binding、Program发布语义、策略包状态、Selection/Paper/QMT，也不修改、覆盖或回写已冻结v6的code-release与结果 |
| F-150 | 性能receipt分解workspace/source/raw/overlay/publish耗时、I/O、RSS和cache hit；目标未达不得删减PIT、typed failure或业务逻辑 |
| F-151 | P0-D descriptor 接入不得覆盖现有 M1 文件；已存在 binding 只允许 expected-current hash CAS 原子切换，切换前保存不可变快照并支持同契约精确回滚 |
| F-152 | P0-F唯一实验变量为连续policy utility entry priority；P0-C label/feature、Selection candidate/exit、shadow policy和成本不变 |
| F-153 | P0-F每path仅用train median/MAD执行可逆连续label transform，不clipping；非有限或zero-scale fail closed |
| F-154 | P0-F固定Huber CORE/CORE_HMM×3 seeds×28 paths，winner由shared-policy收益选择，不做结果后family/transform/rank guard搜索 |
| F-155 | P0-F advancement必须同时超过P0-D和Selection、path win>50%，且配对平均回撤和换手不恶化；失败完整终止Stage B |
| F-156 | P0-F candidate regression诊断、PBO、shared-policy winner和advancement分别报告，不互相冒充 |
| F-157 | P0-F conditional runtime使用显式role；utility只决定entry rank，take/skip/confidence继续来自exact P0-D binary model |
| F-158 | P0-F历史回放只在advancement通过后运行并固定为HISTORICAL_REPLAY；自然future OOS不回填且独立确认 |
| F-159 | P0-F Stage A/B均无DDL/DML和默认生产激活；descriptor、restart、activation和cleanup保持独立用户授权 |
| F-160 | P0-F真实Stage A换手门禁失败后完整终止；P0-G不得修改P0-F参数、rank guard、blend或运行时来规避负向结论 |
| F-161 | P0-G换手负担与shared evaluator同单位，holding/exit/future return只作label/evaluation；非成熟行不填默认值且只允许同日期集合的P0-G/P0-D约束校准 |
| F-162 | P0-G影子价格候选固定，只在每条outer CPCV path的train blocks选择最小可行值；validation和历史回放不得参与 |
| F-163 | P0-G不连续train block之间强制空仓重置，purge/embargo和label span不得跨validation继承portfolio state |
| F-164 | P0-G固定Huber CORE/CORE_HMM×3 seeds×28 paths，entry只按adjusted utility变化，Selection Top40 exit、policy和成本保持不变 |
| F-165 | P0-G沿用收益、path win、Selection、回撤、换手和完整性六项advancement；失败停止Stage B且不自动激活 |
| F-166 | P0-G Stage A生成不可变request/bundle、constraint/PBO/paired/advancement/resource/exact-retry receipt，零DDL/DML和零运行时写入 |
| F-167 | P0-H以nested OOF双头output constraint分离return/liability并复用exact P0-D预算；真实负向结果证明约束有效但return泛化不足，未激活 |
| F-168 | P0-I只以grouped-rank替换return head；冻结price不可行时按预登记停止，不扩展roster、不补跑、不生成winner或runtime bundle |
| F-169 | P0-J以inner-train Selection单调收益先验加OOF解析收缩残差替换from-scratch return；liability、业务逻辑、exact P0-D预算和六项门槛不变 |
| F-170 | P0-K删除return head，只用train-only OOF liability gate过滤高换手ENTER候选；eligible集合保持Selection顺序，完整性、业务逻辑、exact P0-D预算和六项门槛不变 |
| F-171 | P0-L固定P0-G收益anchor，只用relative liability rank执行最大位移1/每日一次的相邻ENTER重排；identity control不得成为winner，机制必须产生真实干预且保持shared policy与六项门槛 |
| F-172 | P0-D至P0-L属于同一共享开发研究族并永久冻结；不派生P0-M、不回选旧frontier、不结果后扩展阈值/family/seed/loss |
| F-173 | 六层决策栈是目标架构而非六条工作线；正式研究最多一条主线和一条结果独立的辅助线 |
| F-174 | 每项新研究预注册`ALPHA_RANKING`或`RISK_MANAGED_ADVISORY`，标签、指标、激活状态和页面展示按合同隔离，不折算为一个加权总分 |
| F-175 | 最小append-only JSONL registry区分study type、family/lineage、唯一变量、objective、trial计数、消费窗口、decision use和证据引用；单页路线由其派生，不建设治理平台 |
| F-176 | 父包预测延伸spike输出冻结模型可推理、仅历史预测或需重训新lineage三态；禁止外推最后一日预测或把重训冒充旧模型延伸 |
| F-177 | 每层同时报告clairvoyant/action ceiling与固定cross-fitted learnability；理论上限不可部署，learnability失败只约束当前信息集和冻结简单模型 |
| F-178 | oracle mini-contract冻结PIT股票池、上市年限、ST/停牌/涨跌停、winner、成本、容量、benchmark、执行价和policy；oracle/learnability不得读取sealed holdout |
| F-179 | sealed holdout只在主线完全冻结后执行一次confirmation并立即登记消费；失败不得返回同一frontier重选，exact retry仅限未利用经济结果的代码/数据身份修复 |
| F-180 | frontier只在inner-train报告完整收益/换手/现金/coverage/MDD Pareto集合并选择一次candidate；activation合同独立且不得结果后放宽 |
| F-181 | 探索性结果可用于研究导航，但不能单独关闭方向、声称稳定效果或支持激活；`decision_use`错误引用必须机器拒绝 |
| F-182 | 确认性实验预注册最低干预次数、干预交易日比例和regime覆盖；阈值由MDE与block/cluster有效样本量推导，恒等策略或稀疏高方差不冒充有效 |
| F-183 | Ranking、Entry、Exit和Risk标签均表达相对冻结基线动作的增量价值并绑定policy hash；历史两臂可重放时使用同一shadow simulator，不强制引入OPE平台 |
| F-184 | Entry Guard只消费T日冻结信息和T+1当时可见权威open/current；`SKIP`可形成固定槽位现金/空槽，但不生成动态资金权重 |
| F-185 | Exit先验证“现在退出相对继续baseline policy”的label/oracle；liability或holding相关性不得冒充Exit可预测性 |
| F-186 | QE alpha MVE准备件可与N0/N1并行；N3正式分流前禁止生成/评价候选、读取IC/收益或按结果调整提示词与搜索预算 |
| F-187 | 组合只接纳角色一致且具备独立时段残差、逐种子/种子平均稳定性、LOO边际、成本后、跨regime和经济归因/已知效应重叠检查的信号；不同决策时钟不得任意加总 |
| F-188 | 历史批量回放、一次sealed holdout和自然prospective OOS分级使用，任何一类不得冒充另一类；只有对应objective contract的前向证据可支持激活 |
| F-189 | 动态资金仓位、自动下单和交易执行输入不在当前授权；固定等权槽位的`SKIP/WAITING`不构成仓位模型 |
| F-190 | 重训周期通过expanding、不同rolling window和regime-weighted对照选择；不预设三个月、不建设自动重训平台 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | `backend/services/advisory_model_first/qe_file_source.py` | `backend/tests/advisory_model_first/test_qe_file_source.py`; M1/M3/M4 frozen artifacts | implemented_verified | none |
| F-102 | Windows launcher + `scripts/wsl/advisory_*_train.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/training_log.json`; M3/M4同类receipt | implemented_verified | none |
| F-103 | `reranker_training.py`, `outcome_training.py`, `price_range_training.py` | `backend/tests/advisory_model_first/test_price_range_training.py`; `backend/tests/advisory_model_first/test_outcome_training.py`; artifact: M1 `model.txt` | implemented_verified | none |
| F-104 | historical `time_split.py`/`outcome_split.py`; `policy_cpcv.py` P0-C policy split | `backend/tests/advisory_model_first/test_outcome_split.py`; `backend/tests/advisory_model_first/test_policy_cpcv.py`; artifact: M1/M3/M4 `split.json` and P0-C 28 READY paths | IMPLEMENTED_HISTORICAL_AND_POLICY_SPLIT_VERIFIED | none |
| F-105 | `shared_feature_builder.py`, `feature_schema_v1.py` | `backend/tests/advisory_model_first/test_shared_feature_builder.py`; `backend/tests/advisory_model_first/test_realtime_feature_source.py` | implemented_verified | none |
| F-106 | `realtime_feature_source.py` | `backend/tests/advisory_model_first/test_realtime_feature_source.py`; artifact: deployed model-shadow readback for decision `2026-07-15`, target `2026-07-16` | implemented_verified | none |
| F-107 | `reranker_training.py` baseline comparison | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/baseline_comparison.json`; `backend/tests/advisory_model_first/test_candidate_and_contracts.py` | implemented_verified | none |
| F-108 | `backend/routers/advisory.py`; `frontend/src/app/paper-v2/advisory/page.tsx` | `backend/tests/advisory_model_first/test_model_shadow_api.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`; runtime route HTTP 200 | implemented_verified | none |
| F-109 | `model_inference.py`, exact binding loaders | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_model_shadow_api.py`; artifact: typed single Alpha unavailable readback | implemented_verified | none |
| F-110 | Program-level baseline composition + `model_binding_resolution.py` dynamic exact-bundle resolver | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; runtime: target multi-Alpha exact P0-D + single-Alpha typed unavailable | IMPLEMENTED_DYNAMIC_MODEL_COVERAGE_VERIFIED | none |
| F-111 | M3 46 heads; M4 4 heads | `backend/tests/advisory_model_first/test_outcome_training.py`; `backend/tests/advisory_model_first/test_price_range_training.py`; M3/M4 `metrics.json` artifacts | implemented_verified | none |
| F-112 | Advisory-only model-first modules | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-113 | blueprint §§3,14 and model-first error contracts | artifact: `docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`; F2 validator receipts | implemented_verified | none |
| F-114 | model-first import and changed-file boundaries | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-115 | blueprint 分层进度 ledger | artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`; latest verified runtime readback 2026-08-28 12:55；v6/P0-D至P0-L report/result identities | APPROVED_BY_USER_CURRENT_TRUTH_REFRESHED_20260830 | none |
| F-116 | `candidate_group.py`, `prediction_source.py`, `qe_file_source.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; artifact: M1 406-date/8120-row request | implemented_verified | none |
| F-117 | Qlib daily/limit fields and price-range labels | `backend/tests/advisory_model_first/test_labels.py`; `backend/tests/advisory_model_first/test_price_range_labels.py` | implemented_verified | none |
| F-118 | `fresh_hmm.py` | `backend/tests/advisory_model_first/test_fresh_hmm.py`; artifact: M1 `fresh_hmm_models.json` | implemented_verified | none |
| F-119 | M1/M3/M4 daily input contracts | `backend/tests/advisory_model_first/test_qe_file_source.py`; `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-120 | QE `000300.SH` daily Bin | `backend/tests/advisory_model_first/test_qe_file_source.py`; artifact: M1 `training_request.json` benchmark identity | implemented_verified | none |
| F-121 | WSL pipelines and receipts | artifact: M1/M3/M4 bundle `training_log.json`; M1 peak ~2.11GB/129s, M3 ~0.66GB/108s, M4 ~0.49GB/13.85s | implemented_verified | none |
| F-122 | M5A train/test dual request and winner policy | `backend/tests/advisory_model_first/test_quality_contracts.py`; artifact: `quality_runs/advm5train_a64594d6f22f618a4afef84a/winner_receipt.json`; `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_once_receipt.json` | implemented_real_trained_verified | none |
| F-123 | M5A shared baseline evaluator；冻结 test 结果低于 selection rank 的事实见 §9 | `backend/tests/advisory_model_first/test_quality_pipeline.py`; artifact: `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_report.json` | implemented_real_evaluated_verified | none |
| F-124 | M5A ensemble and selection-prior policy；现行 M1 binding 保留的运行状态见 §9 | `backend/tests/advisory_model_first/test_quality_scoring.py`; `backend/tests/advisory_model_first/test_quality_bundle.py`; artifact: bundle `1757b24b854cf8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | implemented_bundle_verified | none |
| F-125 | M5B validation-only probability calibration；8 个正斜率 head 发布 calibrated，2 个排序反转 head 明确 uncalibrated；逐 head solver/版本/迭代/收敛证据 fail-closed | `backend/services/advisory_model_first/outcome_calibration.py`; artifact: `outcome_calibration_runs/advoutcal_ec16422ad1a97040583e5273/outcome_calibration_receipt.json`; `backend/tests/advisory_model_first/test_outcome_calibration.py` | real_calibration_verified | none |
| F-126 | M5B outcome quantile calibration和 M5C entry-gap coverage calibration均只使用 validation 拟合；M5C test 零缺失、`delta=0` 且质量未改善 | `outcome_calibration.py`; `price_range_calibration.py`; artifact: M5C bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead`; `test_price_range_calibration.py` | m5b_m5c_real_calibration_negative_quality_verified_not_activated | none |
| F-127 | P0-A Advisory after-close runner + existing review service | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_scheduler.py`; artifact: two persisted 2026-08-14 forward runs | implemented_runtime_publication_verified | none |
| F-128 | baseline/challenger/replay identity contract | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_api.py`; artifact: multi Alpha EXPERIMENTAL_SHADOW and single Alpha typed UNAVAILABLE readback | implemented_runtime_identity_verified | none |
| F-129 | `AdvisoryForwardObservationV1` | `backend/tests/advisory_model_first/test_forward_postgres.py`; `backend/tests/advisory_model_first/test_forward_api.py`; artifact: persisted forward/model observation readback | implemented_runtime_observation_verified | none |
| F-130 | `AdvisoryModelBindingResolutionV1` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; artifact: exact multi Alpha descriptor and package-without-bundle typed unavailable | implemented_dynamic_binding_verified | none |
| F-131 | policy-aligned meta-label take/skip/confidence | `backend/tests/advisory_model_first/test_meta_label_training.py`; `backend/tests/advisory_model_first/test_meta_label_bundle.py`; artifact: final-source bundle `e555903e...` | implemented_real_wsl_verified_experimental_not_activated | none |
| F-132 | `AdvisoryPolicyEpisodeLabelV1` + existing review transition semantics | `backend/tests/advisory_model_first/test_policy_episode_labels.py`; `backend/tests/advisory_model_first/test_policy_dataset_bundle.py`; artifact: P0-C bundle `81e2c9ba...` | implemented_real_file_dataset_verified | none |
| F-133 | purged rolling/CPCV + PBO/equivalent report | `backend/tests/advisory_model_first/test_policy_cpcv.py`; `backend/tests/advisory_model_first/test_policy_pbo.py`; artifact: P0-C 28 paths and P0-D 168 trial-path rows/70 PBO partitions | implemented_real_cpcv_pbo_verified | none |
| F-134 | daily forward observations and matured policy episodes | `backend/tests/advisory_model_first/test_forward_publication.py`; `backend/tests/advisory_model_first/test_forward_postgres.py`; runtime: eachProgram 3 PUBLISHED runs, 2 SETTLED target dates and 20 active episodes as of 2026-08-18 | APPROVED_BY_USER_FORWARD_RUNNING_EVIDENCE_IMMATURE | approved_by_user: wait for natural model episode/outcome maturity; current OPEN_MARK_TO_MARKET metrics are not mature OOS and are not backfilled |
| F-135 | rolling/adaptive calibration matched study | `backend/tests/advisory_model_first/test_adaptive_calibration.py` (target path) | APPROVED_BY_USER_P1A_DESIGN_READY_WAITING_NATURAL_FORWARD_LABELS | none |
| F-136 | compatible-set pooled/multi-task experiment | `backend/tests/advisory_model_first/test_strategy_conditioned_pooling.py` (target path) | APPROVED_BY_USER_P1B_DIRECTION_READY_WAITING_COMPATIBLE_PACKAGE_DATA | none |
| F-137 | Advisory-only forward boundaries | `backend/tests/advisory_model_first/test_forward_boundaries.py`; `backend/tests/advisory_model_first/test_meta_label_boundaries.py`; artifact: Advisory forward boundary readback | implemented_runtime_boundary_verified | none |
| F-138 | P0-C Top40/held-symbol rank reconstruction | `backend/tests/advisory_model_first/test_policy_rank_source.py`; artifact: P0-C `candidate_rankings.parquet` | implemented_real_file_reconstruction_verified | none |
| F-139 | candidate meta-label evaluator + shadow portfolio policy simulator | `backend/tests/advisory_model_first/test_policy_episode_labels.py`; `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py`; artifact: P0-D matched baseline report | implemented_real_policy_simulation_verified_not_activated | none |
| F-140 | after-close publication and target-open episode clock | `backend/tests/advisory_model_first/test_forward_date_clock.py`; `backend/tests/advisory_model_first/test_forward_recovery.py`; runtime: target 2026-08-14/17 SETTLED, target 2026-08-18 pre-open WAITING_DATA | APPROVED_BY_USER_RUNTIME_TARGET_OPEN_SETTLEMENT_VERIFIED | approved_by_user: no target-open fallback; each future target remains WAITING_DATA until authoritative open arrives |
| F-141 | H0 shared day business composition + two executors | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-142 | `AdvisoryPITAsOfViewV1` + historical batch source | `backend/tests/advisory_execution/test_pit_asof_view.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-143 | content-addressed runtime workspace session | `backend/tests/strategy_package/test_runtime_workspace_session.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-144 | immutable raw Alpha day artifact reuse | `backend/tests/advisory_historical_range/test_raw_alpha_reuse.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-145 | batch/chunk/day source validation policy | `backend/tests/advisory_historical_range/test_batch_source_validation.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-146 | day checkpoint + ordered list transition recovery | `backend/tests/advisory_historical_range/test_batch_recovery.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-147 | semantic hash oracle + current v6 golden receipt | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path); report `docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md`; artifact `comparison_result_v6.json`, result hash `500d96e0...`, contract `652eef96...`, 44 dates | APPROVED_BY_USER_GOLDEN_FROZEN_IMPLEMENTATION_ORACLE_READY | approved_by_user: batch semantic normalization and parity implementation remain pending |
| F-148 | single-worker chunk policy + resource receipt | `backend/tests/advisory_historical_range/test_batch_resource_policy.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-149 | execution boundary and no-live-activation assertions | `backend/tests/advisory_execution/test_execution_boundaries.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-150 | stage telemetry and performance report | `backend/tests/advisory_historical_range/test_batch_telemetry.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-151 | `model_binding_resolution.py`; descriptor operator CLI | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; P0-D runtime F2 design；runtime descriptor `f98f2ded... -> e555903e...` | SOURCE_MERGED_RUNTIME_VERIFIED | none |
| F-152 | P0-F design §§2-5；`policy_utility_pipeline.py` | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; artifact `ff336ead...` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-153 | `policy_utility_training.py` | `backend/tests/advisory_model_first/test_policy_utility_training.py`; artifact `ff336ead...` train-only transform receipt | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-154 | P0-F request/contracts/training/pipeline | `backend/tests/advisory_model_first/test_policy_utility_contracts.py`; `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; 168 trial-path receipt | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-155 | P0-F advancement receipt | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; artifact `ff336ead...` six-metric advancement result | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-156 | P0-F candidate/portfolio metrics and PBO | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; `backend/tests/advisory_model_first/test_policy_pbo.py`; artifact `ff336ead...` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-157 | conditional resolver/inference/bundle role | P0-F design §6; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0f_suspension_v2_20260824/policy_utility_bundles/ff336eadb131cb6a3d431a846de4e9949ad984da1dcc4d9c231aa313886ebc10/advancement_receipt.json` | NOT_IMPLEMENTED_BY_FROZEN_NEGATIVE_STOP | approved_by_user: Stage A failure prohibited Stage B runtime role, descriptor and activation implementation |
| F-158 | conditional historical replay union | `backend/tests/advisory_model_first/test_historical_forward_replay.py`; P0-F negative advancement receipt | NOT_RUN_BY_FROZEN_NEGATIVE_STOP | approved_by_user: historical replay was conditional on advancement and therefore was correctly not executed |
| F-159 | P0-F Stage A boundary assertions | `backend/tests/advisory_model_first/test_meta_label_boundaries.py`; `backend/tests/advisory_model_first/test_policy_utility_bundle.py`; P0-F negative receipt | STAGE_A_BOUNDARY_VERIFIED_STAGE_B_PROHIBITED | none |
| F-160 | P0-F negative receipt；P0-G contracts | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-161 | P0-G liability label/training | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-162 | P0-G train-only price selection | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-163 | P0-G split/block-reset pipeline | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-164 | P0-G contracts/training/pipeline | `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-165 | P0-G advancement/stage guard | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-166 | P0-G immutable bundle/exact retry/boundary | `backend/tests/advisory_model_first/test_turnover_constrained_utility_bundle.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-167 | P0-H dual-head contracts/training/pipeline/bundle | `backend/tests/advisory_model_first/test_dual_head_output_constraint_contracts.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-168 | P0-I grouped-rank contracts/training/pipeline/bundle | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_contracts.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py` | STAGE_A_INCOMPLETE_STOP_VERIFIED | none |
| F-169 | P0-J F2 detailed design and target implementation | `backend/tests/advisory_model_first/test_selection_prior_residual_contracts.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py`; artifact `eb8ade9b...` | STAGE_A_INCOMPLETE_STOP_VERIFIED | none |
| F-170 | P0-K contracts/training/pipeline/bundle/request+WSL CLI；F2 detailed design | `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_bundle.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/advancement_receipt.json` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-171 | P0-L F2 design；contracts/training/pipeline/bundle/request+WSL CLI；BUG-1251 date-role fix；formal Stage A | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_contracts.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py`; artifact `4476afeb...`记录非零local-rerank降低换手但违反冻结coverage/cash完整性、无winner/PBO/Stage B；PR #3959/#3967/#3969 | STAGE_A_INCOMPLETE_STOP_VERIFIED_NOT_ACTIVATED | none |
| F-172 | §0、§6.1、§9 P0研究族冻结 | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py`; `backend/tests/advisory_model_first/test_research_control_cli.py`; formal route SHA256 `1b8164ce...` | FORMAL_ROUTE_VERIFIED_FAMILY_FROZEN | none |
| F-173 | §5.3决策栈；§9 N2/N3单主线规则 | `backend/tests/advisory_model_first/test_research_control_cli.py`; N1 route `N2_ENTRY_EXIT_QE_PREPARATION` | N1_TO_N2_TRANSITION_VERIFIED_FUTURE_ACTIVATION_PENDING | approved_by_user: N2/N3 activation transitions remain future scope |
| F-174 | `AdvisoryObjectiveContractV1`；§5.3、§7.2 | target: `backend/tests/advisory_model_first/test_objective_contracts.py`; UI/API objective-label test | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: contract implementation and contract-specific activation/display remain future work |
| F-175 | `AdvisoryResearchTrialRegistryV1`；§4.1.2、§9 N0 | `backend/tests/advisory_model_first/test_research_trial_registry.py`; `backend/tests/advisory_model_first/test_research_control_cli.py`; formal registry SHA256 `8d7ae5cd...` | IMPLEMENTED_FORMAL_VERIFIED | none |
| F-176 | §4.1.2 parent prediction extension；§9 N0 | `backend/tests/advisory_model_first/test_parent_prediction_extension.py`; formal N0 completion `9b460c29...` | IMPLEMENTED_FORMAL_VERIFIED | none |
| F-177 | `AdvisoryLearnabilityAuditV1`；§6.6 | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py`; N1 receipt `1e47d33d...` | IMPLEMENTED_FORMAL_VERIFIED_EXPLORATORY_INCONCLUSIVE | none |
| F-178 | `AdvisoryOracleMiniContractV1`；§6.6 | `backend/tests/advisory_model_first/test_oracle_mini_contract.py`; `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py`; N1 receipt `28d4330d...` | IMPLEMENTED_FORMAL_CONTROL_READY | none |
| F-179 | `AdvisoryResearchWindowContractV1`；§4.1.2、§6.10 | `backend/tests/advisory_model_first/test_research_window_guard.py`; formal N0 window；N1 manifest `sealed_holdout_accessed=false` | IMPLEMENTED_FORMAL_VERIFIED_SEALED_UNCONSUMED | none |
| F-180 | §6.10 frontier/candidate/confirmation/activation | target: `backend/tests/advisory_model_first/test_research_frontier_contract.py` | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: one-selection and exact-retry implementation belongs to the routed experiment |
| F-181 | registry `decision_use`；§4.1.2、§6.10 | `backend/tests/advisory_model_first/test_research_control_contracts.py`; `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-182 | intervention-support receipt；§6.7、§6.8、§6.10 | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py`; N1 oracle/learnability receipts | IMPLEMENTED_FOR_N1_FUTURE_ROLES_PENDING | approved_by_user: N2 roles must preregister their own support thresholds and cannot reuse N1 counts |
| F-183 | `AdvisoryIncrementalValueLabelV1`；§6.7/§6.8 | target: `backend/tests/advisory_model_first/test_incremental_value_labels.py`; policy-hash invalidation test | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: direct paired-label implementation deferred to N2 |
| F-184 | `AdvisoryEntryGuardDecisionV1`；§6.7 | target: `backend/tests/advisory_model_first/test_entry_guard_decision.py`; T+1 poison/no-replacement cash arm | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: existing M4/PriceGuard bridge deferred to N2 and has no position sizing |
| F-185 | `AdvisoryExitDecisionV1`；§6.8 | target: `backend/tests/advisory_model_first/test_exit_label_oracle.py`; suspend/limit/censoring cases | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: exit-label viability must be measured in N2 and is not assumed here |
| F-186 | §6.9、§9 N2 QE preparation boundary | target: `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: preparation only; alpha generation/evaluation waits for N3 formal route |
| F-187 | §6.9 signal admission/combination | `backend/tests/test_multi_alpha_orthogonality.py`; `backend/tests/test_multi_alpha_combine_backtest.py`; target role/seed stability and attribution-overlap tests | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: no P0 signal is admitted automatically; role/seed/economic-attribution evidence waits for N4 |
| F-188 | §4.1.2、§6.10 evidence levels | target: `backend/tests/advisory_model_first/test_evidence_level_boundaries.py`; forward observation maturity tests | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: sealed holdout/prospective receipts do not exist yet and cannot be backfilled |
| F-189 | §2、§3、§5.3 no-position boundary | target: `backend/tests/advisory_model_first/test_no_dynamic_position_boundary.py` | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: fixed-slot cash only; dynamic position scope remains unauthorized |
| F-190 | §9 N4 retraining-window comparison | target: `backend/tests/advisory_model_first/test_retraining_window_contract.py` | DESIGN_READY_NOT_IMPLEMENTED | approved_by_user: waits for a confirmed signal; no automatic retraining platform is authorized |

## 12. Verification Plan

### 12.1 训练验证

- QE H5/Parquet/Qlib Bin基础数据和目标父包Prediction Store PKL真实可读。
- feature/label schema和日期范围读回一致。
- 精确roster中的每条腿和seed均能解析，腿间对齐不跨Program或父包。
- 日线Bin的`limit_up/down`、`up/down_limit_price`和`prev_close`可读。
- HMM由本轮文件数据重新拟合，历史HMM产物未进入训练输入。
- 既有WSL/Conda/LightGBM训练身份和日志可见；新主线若采用其它预注册模型族，同样保存可复现环境、参数、资源和真实训练日志。
- policy episode label 与实际 review transition 对固定样本逐事件一致，policy hash、价格基础、成本和退出原因完整。
- purged rolling/CPCV 的 train/validation 标签窗口无交叉；已消费80日test不进入任何选择输入。
- 模型文件可重新加载并对相同输入产生确定性预测。
- 历史P0-D meta-label的take/skip/confidence非空；新主线按role验证Ranking、Entry或Exit输出，逐日group、Program/package、objective contract和policy边界正确。
- 全部模型trial/family的validation结果可读，PBO/DSR或适用等价诊断为数值或明确`NOT_COMPUTABLE`；oracle与learnability按study type独立登记，不机械混入模型trial计数。
- 峰值内存、各阶段耗时和临时文件规模可见，首模不读取全量分钟Bin。

### 12.2 推理验证

- 数据库当前/实时输入可以生成与训练相同schema。
- 两个 ENABLED Program 各自产生同日 baseline `PUBLISHED` list；一个失败不阻断另一个。
- D收盘发布的decision/target交易日正确，响应和持久化均不含target日行情；target open到达前不创建带伪价格的episode。
- 目标多 Alpha 解析 exact bundle 并持久化 challenger；单 Alpha 无 bundle 时 baseline 成功、模型 typed unavailable。
- 定时运行、手动同日重试和进程重启后的同日重试保持幂等，不产生重复 list/observation/episode transition。
- 模型不可用、字段缺失和版本冲突均有typed reason和有效后台日志。
- 不写Selection、Paper、模拟盘、QMT或QE实验文件。
- forward observation 到期后按同一 policy 形成 outcome/episode label；未成熟项保持 censored/pending，不补零。

### 12.3 页面验证

- 页面展示真实API结果，不使用fixture或静态mock。
- 明确区分规则、实验模型和后续已验证模型。
- Top5、收益/周期、价格区间按已实现能力逐项出现，不等待全部模型完成。
- baseline 与 challenger、REPLAY 与 PUBLISHED、latest success 与 latest failure 分层展示，零 episode 不显示伪造胜率。
- 桌面和移动viewport无重叠、无静默网络错误。

### 12.4 H0 单日/批量等价与性能验证

- 使用已冻结v6的package/manifest/code-release/source catalog/date plan、逐日raw/candidate/list/outcome语义和可用阶段耗时，作为不可变golden evidence。
- 单交易日分别经现有 single-day 路径与新 batch size=1 路径执行；候选、分数、rank、stage trace、source refs、名单动作和业务语义 hash 必须一致。
- 代表日覆盖5月/6月/7月、ST、停牌、行业映射缺失、HMM排除、无候选和模型特征缺失；再对完整44日窗口比较逐日语义。
- 在 D+1 及以后数据中注入价格、行业、ST、HMM或修订变化，D 日结果必须不变；绕过 `AsOfDataView` 的直接 batch frame 访问测试必须失败。
- 对 workspace cache key、source catalog、runtime semantics 或 raw-affecting config 任一字段做单变量变更，错误复用必须被拒绝。
- 在 chunk 内第3日注入可重试和不可重试失败，验证前两日已提交、顺序 list transition 不跨越失败日、修复后从同一 identity exact resume。
- A/B raw identity相同时只产生一个 raw artifact；改变任一 raw-affecting字段后产生不同identity且不得共享。overlay 输出保持两臂独立。
- 记录单日、batch size=1、batch size=5 的 workspace/source/raw/overlay/publish耗时、实际读写字节、峰值RSS、GPU利用和cache hit；不以增加并发作为首轮优化。

### 12.5 Oracle、learnability与新主线验证

- registry相同`experiment_id`的经济字段不可覆盖；study type、objective contract、decision use和消费窗口缺失时拒绝登记。
- 单页路线只能从registry生成；手工修改路线不得改变研究状态。
- 父包预测spike对真实目标identity输出三态之一，并证明冻结模型、输入和新prediction artifact边界；需要重训时必须产生新lineage。
- Tier 1赢家召回使用decision-date PIT可交易股票池，上市年限、ST、停牌、涨跌停、成本、benchmark和winner标签逐字段固定。
- clairvoyant结果带`FUTURE_INFORMATION_CEILING/NOT_DEPLOYABLE`；任何将其写入模型特征、bundle或runtime的路径测试失败。
- learnability只运行一个冻结简单模型合同，cross-fitting无标签窗口交叉；改变family、超参、特征或阈值产生新experiment identity。
- oracle和learnability命令对sealed holdout日期/identity fail closed；confirmation读取后写消费receipt，第二次选择或同frontier回选失败。
- frontier保留完整Pareto点；candidate选择只读取inner-train。经济失败后只允许原candidate的非经济exact retry，不允许换点。
- Entry Guard的T日决定在T+1未来收盘/high/low毒化下不变；`SKIP`形成固定槽位现金但无资金权重或第6名静默补位。
- Exit标签两臂使用同一policy simulator和policy hash；停牌、跌停、WAITING和删失均有typed结果，liability指标不作为Exit PASS条件。
- intervention-support receipt包含动作数、覆盖交易日、block/cluster有效样本和regime分布；恒等策略或不足功效只标记探索/证据不足。
- 组合验证分别报告逐种子、种子平均、独立时段残差、LOO、成本和regime；不同role/clock无法通过同一分数组合接口。
- 历史回放、sealed holdout和prospective OOS使用不同evidence level；`NAVIGATION_ONLY`与错误objective contract不得进入activation引用。

### 12.6 DESIGN-COMPLIANCE-001

每次合入前逐项证明：

1. 没有用简化、mock、规则或静态结果冒充真实模型。
2. 没有静默错误或无日志fallback。
3. 没有改变Selection、Paper、模拟盘、策略包或荐股基线语义。
4. 没有新增未经用户确认的门禁、审批、角色或无界历史工程；trial registry、路线、oracle和holdout保持最小任务内控制，H0保持在用户明确批准的执行优化范围内。

## 13. Rollout / Rollback

### 13.1 Rollout

下一阶段发布顺序固定为：

1. `COMPLETED`：P0-A/P0-B 详细设计、源码、真实发布和动态 bundle 解析已完成。
2. `COMPLETED`：用户重启后的两个 ENABLED Program 真实发布、target-open settlement和episode readback已完成；后续由日调度自然积累。
3. `COMPLETED`：P0-C file-based policy episode标签与purged rolling/CPCV评价已合入。
4. `COMPLETED_DESCRIPTOR_ACTIVE`：P0-D meta-label bundle已真实训练并作为独立experimental challenger接入；不自动替换baseline。
5. `COMPLETED_SOURCE_RUNTIME_AWAITING_MATURE_OOS`：自然observation成熟结算与独立模型指标源码、DDL、合入、用户重启和运行readback均已完成；截至2026-08-28首个Program有4条P0-D observation，最早成熟日为2026-09-22，真实自然胜率继续等待首批样本到期。
6. `COMPLETED_GOLDEN_FROZEN`：v6 A/B/C、outcome、统计和报告已完成；44日逐日结果是H0不可变oracle，不回写或重算。
7. `PARALLEL_H0`：继续关闭会改变Historical Range候选、day terminal receipt、outcome timeline/hash或父子lease语义的正确性BUG，并在独立revision实施H0；先通过batch size=1与代表日等价，再运行完整44日和性能基准。H0不决定模型方向。
8. `COMPLETED_VALID_NEGATIVE`：P0-K源码、正式168/168和exact retry已完成；liability预测存在但绝对阈值策略恒等，禁止Stage B和结果后阈值扩展。
9. `COMPLETED_VALID_NEGATIVE`：P0-L设计、源码、BUG-1251修复、正式Stage A和exact retry均已完成；冻结非零局部重排档全部因coverage/cash完整性不可行而在`0/168`停止，禁止StageB和结果后扩展gain/位移/每日swap。
10. `FAMILY_FROZEN`：P0-D至P0-L没有可激活winner，正式冻结为一个研究族；不创建P0-M或继续同数据、同候选、同特征和同模型族的局部派生。
11. `COMPLETED_N0`：正式registry/路线、父包prediction spike、window contract和sealed holdout访问边界已闭合；不产生模型收益证据。
12. `COMPLETED_N1_INCONCLUSIVE`：开发窗口Tier 1 oracle与固定cross-fitted learnability已完成；oracle为高上限，learnability欠功效且综合`direction_ready=false`，不激活、不提前选主线。
13. `NEXT_N2_AUXILIARY`：现有父包LSTM/FUND/IC加权组合的固定三臂共同窗口Alpha审计已完成；下一主任务是完成绑定BUG-1302排除证据的N2-B v2两存续包共同窗口审计，同时在同一边界明确的辅助工作包中完成Entry Guard MVE与Exit-label oracle。同一时刻只允许一个进入candidate训练/confirmation；QE alpha只做无证据准备，不得在N3前生成或评价候选。
14. `ROUTED_MAIN`：综合N1与所需N2诊断后，只选择上游alpha、Top20新信息ranker、Entry或Exit中的一条主线；一个角色确认增量前不组合。
15. `CONDITIONAL`：确认信号后运行种子/正交/LOO/重训窗口与prospective activation；前向标签成熟后运行P1-A，至少两个兼容策略包具备独立bundle后运行P1-B，LONG_TREND包就绪后运行P2。

源码合入、WSL训练、模型文件生成、后端重启、模型加载和页面可见是独立状态，不得合并声明完成。

### 13.2 Rollback

- 关闭目标 Program 的 challenger 配置后保留现有 `selection_effective_rank`、baseline publish 和 `rule_default`。
- 模型加载或推理失败时只关闭模型通道，不停止现有荐股。
- 执行器失败不删除已发布事实；修复后按同一 Program/date 幂等重试，不回填未授权历史日期。
- H0 回滚只切回既有逐日 historical executor；已完成的 batch day artifact 和 checkpoint 保持审计可读，不删除、不覆盖，也不影响 LiveDailyExecutor。
- 不修改或回滚Selection、Paper、模拟盘、StrategyPackage或QE资产。
- 不删除训练文件、模型文件或已产生的预测；只停止继续使用有问题的模型版本。
- P0-A 会产生正常 Advisory 业务写入；这不是一次性修复 DML。若详细设计证明需要最小 DDL，则迁移与回滚另行设计并按 DEV-first 执行。

## 14. Production Gates / 正确性检查与生产影响（无新增业务门禁）

本蓝图不新增业务审批、人工确认门禁或运行门禁。以下仅是普通输入校验和错误可见性要求，不形成独立状态机、审批步骤或运行阻断层：

- 训练文件真实存在且schema可读。
- WSL训练环境真实可用。
- feature schema与模型兼容。
- 正式预测所需数据库字段可用。
- 模型文件可加载且预测结果合法。

输入正确时这些检查必须自动通过；失败必须输出具体reason和日志，不能要求人工审批放行。

默认生产影响：

```text
production_ddl_gate = noop
production_dml_gate = noop for training; daily Advisory publication is normal runtime business write
production_backend_dependency_gate = noop unless WSL dependency is proven missing
production_frontend_dependency_gate = noop unless UI implementation introduces a real dependency
runtime_activation_and_backend_restart = separate user-confirmed actions
historical_batch_activation = separate user-confirmed action after source merge; never activate against or overwrite the frozen v6 run identity
```

训练过程不启动、停止或重启用户服务。模型源代码合入、WSL训练完成、模型文件生成、后端加载、页面可见和模型启用必须分别报告，不能合并成一个完成状态。

## 15. 风险与直接处置

| 风险 | 直接处置 |
|---|---|
| QE文件不含目标候选或合同必需特征 | 搜索其它已有QE H5/Parquet/Qlib Bin或Prediction Store PKL；仍缺失则报告精确阻断，禁止静默删列或启动历史证据工程 |
| QE文件标签口径不兼容 | 仅用文件内价格派生标签，或明确阻断对应模型；不读生产历史库补造 |
| 多Alpha预测被跨实验拼接 | 只接受目标父包精确roster、seed和权重；不使用“最新腿”替换 |
| 旧HMM结果污染新模型 | 当前文件数据重新拟合；旧模型、状态和系数仅进入对照报告 |
| 分钟数据拖慢首模 | M1/M3和首版M4不读取分钟Bin；盘中路径作为用户另行确认的增量 |
| 为首模补建宽基指数库 | 只使用现有沪深300；额外指数不作为前置条件 |
| H5固定格式或大Parquet造成内存超限 | 候选/日期/列投影、分批读取和临时Parquet；峰值内存低于8GB，不建设新缓存平台 |
| 训练/预测特征不一致 | 共享FeatureBuilder和schema parity测试，预测失败显式可见 |
| 首模效果不佳 | 保留真实结果并迭代特征/窗口；不回到基础设施扩建 |
| 正式预测缺实时字段 | 只补实际缺失的数据库查询或适配，不扩建通用数据平台 |
| 模型输出被理解为确定结论 | 页面标记实验状态、区间和不确定性 |
| 模型失败影响基线 | 模型通道隔离，规则荐股继续运行 |
| ENABLED但调度未执行 | 页面/API分开显示配置状态和最近成功/失败日期；P0-A以真实PUBLISHED/readback关闭缺口 |
| challenger污染baseline | 分开身份和存储，禁止改写selection rank、baseline list或正式episode |
| 5日标签与20日运营错配 | P0-C按冻结review policy生成episode标签；旧5日reranker只作历史对照 |
| CPCV被误当新OOS | 仅用于train/validation路径和选择偏差；真正OOS只来自未来forward observation |
| adaptive calibration过早 | 没有成熟residual时保持uncalibrated，不用规则或旧test补造 |
| 跨包共享造成负迁移 | 先完成独立bundle，再做compatible-set matched/LOO实验，不默认共享 |
| H0再次扩张为历史闭合平台 | 只接受直接减少当前回放重复工作且受 F-141 至 F-150 约束的变更；历史补账、归档、通用缓存/调度/ModelOps仍停止 |
| 批量读取把未来行暴露给业务内核 | 日内核只接受 `AdvisoryPITAsOfViewV1`，禁止传入原始batch frame；未来毒化与访问边界测试必须通过 |
| 静态工作区复用造成跨日污染 | cache key绑定完整内容identity，工作区只读；日期数据和输出位于独立sandbox，identity冲突或写入静态区立即失败 |
| A/B raw共享混淆唯一变量 | raw key显式包含所有raw-affecting字段；overlay配置不进入raw key但在分叉后独立hash，反例测试验证拒绝错误共享 |
| 减少双重全量校验削弱数据漂移检测 | 保留批次full seal、chunk前后token和逐日read receipt；无token或token变化时强制full rehash |
| chunk失败跨越名单顺序依赖 | raw预计算与有状态list transition分层；后者在首个未完成日停止，只从最后成功checkpoint exact resume |
| 追求并发重现资源争用 | 首版单worker、默认5日chunk；先消除重复I/O和工作区重建，性能/内存收据通过后才评估并发 |
| P0-K绝对阈值形成恒等策略 | 已由P0-L改用relative liability rank和真实priority/entry干预验证；不再扩展P0-K绝对阈值 |
| P0-L非零局部干预降低coverage并增加cash day | 按冻结合同以`ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE`终止；不在结果后扩大gain、位移、每日swap或回退identity冒充成功，P0研究族整体冻结 |
| hindsight oracle被误当可学习模型 | 每个Tier同时报告不可部署clairvoyant ceiling与固定cross-fitted learnability；理论高/可学习低时先扩信息集，不继续换loss |
| oracle/learnability污染sealed holdout | 开发命令按dataset/window identity拒绝holdout；主线冻结后只允许一次confirmation并立即写消费receipt |
| trial registry和路线膨胀为治理平台 | 只追加JSONL索引既有artifact，路线由其生成；禁止UI、审批、数据库平台、证据复制和通用调度 |
| 双目标合同结果后改判 | experiment创建时冻结objective contract，按合同保存标签、指标、激活和页面状态；禁止加权总分和事后换合同 |
| learnability audit演变为新模型搜索 | 单一冻结family/超参/特征/cross-fitting/判定线；任何变化创建新experiment并计入trial |
| 完美上限高但干预证据稀疏 | 确认性实验预注册MDE推导的动作数、日期比例和regime覆盖，使用block/cluster推断；不足只作探索 |
| liability被误当Exit alpha | Exit先构造退出相对继续policy的直接增量标签和oracle；liability仅作候选risk特征 |
| 多弱信号伪独立 | N2-A先在同一PIT/window/outcome上检查两腿score/result相关和组合配对边际；后续组合再检查独立时段残差、逐种子/种子平均相关、LOO、成本和regime，不同role/clock不得任意加总 |
| 相同预测或不同取样区间被当作不同Alpha | prediction identity相同的Top25/Top50只算一个信号；横向主结论固定共同窗口与共同预测交集，各包原生Sharpe只作inventory，季度只作描述性sensitivity |
| 父包预测延伸改变模型身份 | spike区分冻结模型推理、历史预测不足和重训新lineage；禁止用新模型补出的预测冒充旧包自然OOS |
| 空槽现金被扩展为未授权仓位 | 当前只允许固定等权槽位`SKIP/WAITING`；动态资金权重、组合仓位和交易输入需用户另行扩权 |

## 16. 当前下一步

M0-M5C的代码、真实WSL实验和固定日期推理已形成当前基线；M5A/M5B/M5C均不激活。P0-A/P0-B已在两个ENABLED Program上持续形成真实`PUBLISHED` run，P0-C/P0-D、forward evaluation和P0-D exact shadow descriptor已合入并运行；v6 44日对照和P0-D/P0-E历史回放已冻结。P0-D至P0-L研究族保持冻结且无可激活winner。N0已正式完成；N1已证明Top20内部存在高clairvoyant空间，但固定当前信息集Ridge尚无确认性learnability证据，同时全市场赢家Top20/40/50召回仅约`0.88%/1.61%/1.76%`。因此下一阶段不能继续P0同族或直接激活ranker，必须进入N2补齐Entry/Exit动作空间并准备上游alpha分流。

下一工作严格按以下顺序执行；自然前向、模型研究与H0历史执行互不冒充，模型研究最多一条主线和一条独立辅助线：

1. **保持旧研究族冻结（已持续满足）**：P0-D至P0-L保持已完成负向/未激活结论，不创建P0-M，不放宽旧合同，不重用已消费窗口声称新OOS。
2. **N1源码交付（已完成）**：正式bundle/registry/route未改写；PR #4014已合入`cfc490a1...`并完成精确清理，未触发后端重启、DDL或descriptor变更。
3. **N2-A三臂Alpha审计（已完成）**：正式bundle `6784df1a...`闭合当前LSTM/FUND/父包归因，0 trial、零调权、零sealed读取；结论是父包显著优于FUND但未确认优于LSTM，候选召回仍低。
4. **N2-B v2两存续包同窗审计（当前主任务）**：F2详细设计v1.6固定比较父包、`pkg_378eb9...`和`pkg_5a5ccb...`，并绑定BUG-1302 receipt及`pkg_b668f8...`的`RETIRED`身份；旧v1 request/artifact不得续跑、改写或冒充v2。一次读取完整区间，在单一批任务内按386个T的PIT股票池和共享57因子closure执行386个primary group run、3个causality diagnostic及1个file-backed parity，每个冻结模型只加载一次。COW输入、WSL local temp和factor完成后的decision-date result投影不得改变数值，并须由真实file-backed全量reference逐值/dtype/index/hash证明。总墙钟仅作telemetry，RSS/temp/PIT/parity仍fail closed；运行期间每30分钟轻量检查。生成Prediction Store后复用相同PIT/H20/cost审计并报告三组pairwise比较；禁止静默替换违规因子、裁剪factor输入/中间计算、union-wide后置PIT、直接横比原生指标、引入任何`RETIRED`包、结果后选包或窗口。
5. **继续N2 Entry/Exit辅助诊断（唯一辅助工作包）**：分别冻结Entry Guard与Exit增量标签、decision clock、policy hash和动作空间；先完成clairvoyant/learnability诊断，再决定其中是否有一个进入candidate训练。二者都不得形成动态仓位、下单或Selection写入。
6. **并行完成QE上游无证据准备**：只冻结数据身份、表达式/operator白名单、生成预算、原创性/衰减约束和registry lineage；N3前不生成、筛选或评价新alpha候选。
7. **按路由选择唯一主线**：综合N1、N2-A、旧包同窗扩展和Entry/Exit结果，若候选召回仍低则优先QE/StrategyPackage上游alpha；只有召回改善且新信息learnability确认后才开发Top20 ranker；Entry/Exit空间更高则只攻对应层。
8. **确认后才组合**：一个角色在新lineage形成确认增量后，才执行种子稳定性、正交、LOO、成本后组合、重训窗口对照和prospective activation；frontier只选点一次。
9. **继续自然前向与H0**：自然observation/outcome按交易日形成且不回填；H0在独立revision按冻结v6完成同核、未来毒化、恢复和性能验收，但不决定模型方向。
10. **只修阻碍演进的正确性BUG**：不恢复历史证据固化、归档、通用平台或旧任务清理。P1-A、P1-B和LONG_TREND仍按各自真实输入条件启动。

当前无需等待动态资金仓位授权即可执行N2-B v2两存续包同窗扩展、N2 Entry Guard固定槽位现金研究、Exit-label oracle和QE无证据准备；只有把空槽/现金扩展为动态资金权重、组合仓位或交易执行输入时，才需用户另行扩权。继续禁止未授权历史补账/归档、旧batch/root清理、通用缓存/调度/ModelOps、角色审批和额外门禁。Historical Range仅按H0详细设计执行已授权验证优化。
