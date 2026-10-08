# HMM Evolution Phase 2：L2资金流监督增量完整详细设计

> 版本：v1.1；初始日期：2026-10-07；状态更新：2026-10-08；owner：HMM；tier：F2。
> 当前状态：APPROVED_BY_USER_EXECUTED_BELOW_BINDING_MBE。既有D1～D6未变；源码已交付，获授权的正式双fresh-process共2fit已完成，结果详见§14。原DEV事务回滚验证已全部回滚，未持久化安装迁移或导入正式新run；新候选surface/capability/advisory均NOT_AVAILABLE。tail、生产DDL/DML、runtime activation、依赖及进程控制未执行。以下源码阶段及批准前历史记录保留，不作为当前待训练指令。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md`当前L2主线；直接基线：`hmm_evolution_phase2_rotation_l2_p0_detailed_design_20260922.md` v1.6。本文只展开“已有L2资金流成果的单一模型增量”，不替代C-010/A5二十维正式HMM合同。
> 初始review base：`81d43fe8202f9e09e43f62f7cf5adad157ca7622`。该时点P1产品状态文档PR #5653为OPEN；本提案不将该快照外推为实时PR状态，不改其两个文件。

## 1. Background、目标与范围

终极目标保持不变：预测申万二级行业未来相对强弱、识别独立风险，为后续QE、荐股及模拟盘提供经过相应场景验证的可选辅助。计算与评价覆盖正式L2目录；UI默认前10/后10、可配置且总数不超过30。不是让所有行业通过三态结构证明，也不是增加实验记录、归档或训练平台。

2026-10-07实时只读核验：已有资金流研究产品surface为`AVAILABLE_EXPERIMENTAL`，capability为`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`，advisory为`NOT_AVAILABLE`，forward为`NOT_STARTED`，tail未读。原10D基线有368个预测日、358个成熟评价日，mean Rank IC=`0.02973212294698707`，HAC95%区间为`[-0.005748738480015222,0.06521298437398935]`。这证明有可用的研究产品和正点估计，不证明独立预测能力或收益改善。

基线身份：run=`2657778e7c4c3e376874d7290897dc42d99f3ee05a9fa7bc8c36a2cddf817505`；model=`60f19b3c31c2fe8b8c94638d056aefc9f0264fb0e7859a939bfa2431229de38e`；input=`8739376d8001f0b886ee7ac33bda4df061f1ce9110139fe965b3a88100c19ebf`。不因本提案覆盖、重新注册或重建旧结果。原HMM/R1及risk/persistence终态保持，不重新实验以补历史记录。

### 1.1 唯一假设与完整交付

唯一假设：现有固定信号只使用20日资金流强度的5日变化，未利用强度水平；在同源、同PIT口径下，用固定的共享线性模型学习“强度水平＋变化”的联合排序，可能改善未来10D相对强弱预测。当前没有证据保证改善；监督学习也不能创造输入中不存在的信息。

本包完整范围为：受限文件输入→两项特征→固定监督模型→双process因果样本外预测→同窗口/同人口基线比较→真实研究产品适配。实施、评价和发布作为一个功能包交付，不把adapter、测试、API等拆成独立研发阶段。低维是正式提案的模型定义，不冒充二十维HMM简化实现。

### 1.2 Non-goals与授权边界

- 不研发第二候选，不搜索feature/window/horizon/alpha/seed，不反转验证后发现的符号，不把失败模型改参数后重跑。
- 不修改QE、Selection、Advisory、Paper、行业黑名单、数据生产、active profile或共享数据。QE实验后置，由QE窗口执行。
- 不读取`2026-04-01`及之后的tail outcome，不把已消费的development重新命名为untouched holdout。
- 不复活退役B3/P6链，不重做旧资金流、R1、risk或persistence实验；不新建registry、feature store、调度器或证据平台。
- 不安装依赖，不修改Conda AIstock；数据库仅限用户单独批准的既有`aistock_dev`事务回滚式迁移及测试行写入/回读，不持久化。其余DDL/DML、runtime activation或进程控制不执行。cleanup仅限本任务工作树和分支，须走正式清理流程。源码合入、实际fit和生产动作分别报告。

## 2. Architecture、Contracts与现有实现复用

1. 沿正式active-profile resolver冻结显式request；只读取本合同必要列/日期和authority，不自动跟随运行中的profile变化。
2. 复用`rotation_l2_input.py`的正式PIT/security/suspend/provider-absence、单位和行业聚合规则，构造有界训练与预测输入。原builder固定development边界，实施时须增加独立显式范围入口；不能偷改原基线边界或调用旧全family builder。
3. 新增薄模型模块`rotation_l2_moneyflow_supervised.py`和对应CLI。参数固定，共享L2模型；不是每行业挑seed或拼模型。
4. child先拟合训练数据，再封闭全部232日预测；评价阶段才接入预测区间标签。parent按冻结request重验authority、参数、输出和双process一致性。
5. 复用现存L2表、repository、read API和热力图。产品validator增加精确的新合同分支；原资金流/HMM分支不放宽、不代签，不新建产品表。

特征窗口和rank/state的公共纯计算只在HMM内部按实际调用抽取复用，不复制基线实现或留下兼容代理；此类机械抽取必须保持旧数值/hash回归。必要的前端类型/说明变化限于现有HMM页面，不修改跨业务消费者。

本模型明确为`model_type=pooled_ridge_moneyflow_rank`，不是HMM posterior，也不是risk model。推荐版本`hmm_risk_rotation_l2_moneyflow_supervised_v1`。model-contract hash绑定D1～D3全部公式；model-parameter hash另绑定两项系数、截距和训练输入身份；evaluation-contract hash绑定D4；run_id绑定上述身份与输入、mapping、quote authority和source commit。

输入逻辑视图分为受限feature、训练outcome和预测区间outcome；fit/score child不得接收预测区间outcome payload或调用其reader。训练标签reader上界固定2025-04-15；预测区间outcome由评价阶段在双方预测封闭后读取，不通过同一宽表读入后“自觉不用”。同一只读源文件可以被不同受限reader引用，不要求复制源文件或创建第二数据集。

## 3. D1：共享输入、行业人口和读取合同（已批准）

沿直接基线D1，正式目录为131个申万文本L2代码；整数ID仅是共享release内连接键，不重编号。逐日结构集合`S(t)`、截至t-1具备25日有效特征的`E(t)`及可评价集合`M(t)`分别报告。保持逐行业每日有效成员至少1、有效成员/正式期望成员≥0.90、有效成交额为有限正数。真实停牌从分子分母同时排除；合法provider absence保留在期望分母；未知漏采、单位错误、identity漂移均fail closed。

正常全停牌、无resolved成员、合法历史不足或官方停发为行业typed unavailable，不因它们要求整模型全部行业通过。131目录行始终保留。停发只约束quote，不清空真实moneyflow；quote unavailable行业不能伪装成有官方L2预测。未来outcome是否可用不能决定历史feature eligibility。

本次只读解析的active profile为`20260928-v15-unified-moneyflow1`，release=`qe_hmm_full_v2_20260831`，cutoff=`2026-08-31`，manifest=`2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc`，profile文件SHA=`56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f`。这是本次设计观察，不是硬编码月份/路径；正式preflight再次经正式resolver核验，并冻结显式request。若批准时的输入身份与执行时不同，报告差异并等待输入绑定更新，不自行切换或回退release。任何与旧基线不同之处也须披露，不只替换manifest/hash。

所需共享membership从`2024-07-01`覆盖，故本提案不假设2023年训练PIT可读。日历metadata读回SHA=`ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`。只做了日历算术，没有扫描新训练面板，不能称训练输入已PASS。

必要数据仍为股票净资金流/amount、官方SW L2涨跌幅、CSI300 close和直接基线的全部PIT/identity/quote/suspend/provider authority。限定语义源范围`2024-08-13..2026-03-31`；feature不读取decision日。table HDF按列/日期选读；只能整表解码且会访问tail outcome的reader必须阻断，不得“先全读再过滤”。不复制源数据、不补数、不查询运行时市场数据库。

preflight一次冻结有界输入，两个process复用；拒绝每process重新全历史扫描。用本次同一输入重评分确定性基线以配对，不依赖已消失的旧临时请求或恢复历史账本。若所需文件/authority缺失，报告精确需求，不能临时新增数据准备任务或缩短窗口。

## 4. D2：两项特征和监督标签（已批准）

沿基线的当日有效contributors及CNY单位：

```text
F(s,u) = fsum(net_mf_amount_cny(i,u))
A(s,u) = fsum(amount_cny(i,u))
I20(s,t) = sum[F(s,u), u=t-20..t-1] / sum[A(s,u), u=t-20..t-1]
delta(s,t) = I20(s,t) - I20(s,t-5)
rank_scale(v; U) = (average_rank_U(v)-1)/(|U|-1)-0.5
x1(s,t) = rank_scale(I20(s,t); E(t))
x2(s,t) = rank_scale(delta(s,t); E(t))
```

偏移都是canonical open sessions，feature需要恰25个source日。每一天使用其当时成员，禁止t-1成员回填历史。按canonical日期/行业/股票排序后用确定性聚合。两个feature按当日完整`E(t)`精确average rank；相等值不按代码拆tie。`|E(t)|<2`为整日typed unavailable，不默认neutral。全部值相等时rank=真实0，不把它误判缺数。

标签沿基线10D官方close-to-close相对收益，只有正式quote路径完整才可计算：

```text
y(s,t) = product[1+sw2_pct_change(s,u)/100, u=t+1..t+10]-1
         - (CSI300_close(t+10)/CSI300_close(t)-1)
T(t) = E(t) ∩ {行业s的完整10D outcome在训练截止前成熟且正式可读}
y_train(s,t) = rank_scale(y(s,t); T(t))
```

这是用户已批准的新训练损失合同；不会修改基线的原始收益标签。target rank改变的是损失权重、丢弃收益幅度信息，不改变评价的Rank IC或以真实收益计算的spread定义。`|T(t)|<2`时该日不进入fit，记录原因/日期，不平移训练窗；若全部训练日无样本，则typed训练数据不足。官方停发合法无标签，不伪造；应有报价漏采或非有限值仍batch fail closed。

训练target rank只在训练标签面计算。预测排序必须仍对完整当时`E(t)`输出；禁止按未来标签可用性重算feature rank、筛预测或改变训练后评分。

## 5. D3：固定训练、purge和模型（已批准）

### 5.1 日历账本

| 用途 | 精确范围/数量 | 因果边界 |
|---|---|---|
| 最早feature source | 2024-08-13 | 2024-09-19之前25个open sessions |
| 训练decision | 2024-09-19..2025-03-31，126日 | 只使用各t截至t-1特征 |
| 训练最迟outcome | 2025-04-15 | 2025-03-31之后10个open sessions |
| 标签隔离decision | 2025-04-01..2025-04-15，10日 | 不进入预测评价，不作为第二训练窗 |
| 样本外预测 | 2025-04-16..2026-03-31，232日 | 固定模型，在每个t仅用截至t-1特征 |
| 成熟评价decision | 2025-04-16..2026-03-17，222日 | 所有outcome≤2026-03-31 |
| 最后10个预测日 | 2026-03-18..2026-03-31 | 保留预测、label为outcome_not_mature |

126是先验固定的约半年低维共享训练预算，不来自本候选结果、不机械继承252/504窗。实际有样本训练日数另报；合法NA不能被改成数据缺陷，也不能偷偷改日期账本。无滚动重训，无最终全development refit，禁止使用232日评价结果早停、选参、换方向或重排训练段。

设有合法训练样本的日期数为`D`，该日`N_t=|T(t)|`，每行`w(s,t)=1/(D*N_t)`，使每个实际训练日等权、全部权重和为1。采用float64：

```text
minimize over b, beta:
  sum[w(s,t)*(y_train(s,t)-b-beta1*x1(s,t)-beta2*x2(s,t))**2]
  + 0.01*(beta1**2+beta2**2)
Ridge(alpha=0.01, fit_intercept=True, solver="svd", positive=False)
```

alpha=0.01是在已固定rank尺度和总权重1上的已批准先验收缩量，不从评价结果反推；不可把权重改成总和样本数而沿用alpha。无额外z-score/特征裁剪/缺失插补，无超参搜索或自动符号约束。SVD不使用随机seed；request写`seed=not_applicable`，不能假报seed42。固定现有环境版本、同host单线程；不安装/升级依赖。

原始预测`p=b+beta1*x1+beta2*x2`；产品score为当日`E(t)`内`rank_scale(p; E(t))`。state复用原L2 top/bottom20%公式和边界tie整体neutral规则。全p相等可形成真实零score/neutral，IC另报无方差；不以自然tie强制模型失败或偷偷回退delta。任何参数/原始预测非有限，typed停止，不调alpha或重新拟合。

每个child只fit一次；双fresh-process计划总fit=2。parent逐项匹配输入、训练date/sample集合、特征/标签hash、参数和预测canonical payload。路径、created_at和process index不进入需要相等的业务hash；canonical规则沿仓库，拒绝NaN。不承诺跨host/BLAS版本bitwise等同。

## 6. D4：样本外价值比较与证据（已批准）

仅对232个预测日运行本模型和同输入的原delta基线；原基线重评分为0 fit。双方feature资格/目录必须相同，任何区别报告typed合同错误，不挑成功行业。不能直接用原358日`0.029732...`与新222日IC作增量结论。

每日评价集合`M(t)`取双方合法预测与成熟正式outcome的交集；另报候选全人口结果、交集结果以及被排除原因/数量。Rank IC用精确average rank的Spearman；零方差或N<2为不可计算，不填0。当前提案保留binding mean daily Rank IC=`0.02`、coverage=`0.90`及既有充分性算法，来源为现行产品先验量级，不宣称该数由收益目标或当前结果推导。

报告两个固定时间块：`2025-04-16..2025-09-30`和`2025-10-01..2026-03-31`。采用原基线D4相同的人口/覆盖/评价充分性规则应用于新总体及这两个块；效果门只在总体，块IC不增加全正/显著性AND门。末端未成熟日保留，不每块再删除10天。

必须输出：每日双方Rank IC及配对差`IC_candidate-IC_baseline`、均值、HAC lag=9诊断区间、IC/spread符号诊断、完整日历和三个coverage分母、组数和收益幅度。HAC按真实open-session间隔，不把缺评价日压缩成相邻日，不用131行业当131份独立时间样本。

spread仍为展示state下真实`y`的trending均值减fading均值；不是组合收益，不包含换手、费用或容量。任一组为空为typed不可评价。candidate spread与baseline用各自冻结state，另列可比较的配对日期；不得以spread不满意临时升为第二推广门或改变state阈值。

达到0.02只是研究效果资格；配对增量点估计/区间决定建议是否有继续价值，不自动变成新增审批门。若候选效果不足，终止本候选并如实报告“未达到现行研究效果量级”；若配对增量≤0，报告“未观察到排序改善”。两者不能混称：低于0.02也可能相对基线有所改善，高于0.02也可能没有增量。均不宣称所有资金流模型无效，也不自动搜索新候选。

本设计及其选窗是看过既有development结果后的研发决策，`selection_basis=RETROSPECTIVE_DEVELOPMENT_SELECTED`。即使预测逐日因果、训练与评价严格分开，该区间仍非独立最终holdout；HAC区间未校正整个研发历史的选择偏差。不得报告forward-confirmed、QE增益或真实净收益。

## 7. D5：产品、身份和诚实状态（已批准）

沿现有正交状态：输入/执行完整性、effect、research surface、capability、forward、advisory分别存储。证据不足为`EVIDENCE_INSUFFICIENT`，效果低于0.02为`BELOW_BINDING_MBE`；合法研究回放仍可作真实产品验证。符合研究效果后才可具有`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`，不自动advisory升级；真实writer/readback/API/UI未验证前surface不得为`AVAILABLE_EXPERIMENTAL`。

本批总是`tail_accessed=false`、`forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`。未做forward功效核算则不标“功效不足”；全null/无真实预测不能通过research surface。UI本体展示确切预测区间、训练截止、历史选择局限、效果状态与IC区间，不能遮盖效果不佳。

新模型身份和基线严格分离，不借旧product-validation record推广新model/run；不覆盖旧行。推荐目录payload为232×131=`30,392`行，这只是planned数量，不声称可用行数量或已经导入。

原repository只识别原资金流及已准入HMM合同，不能将Ridge输出塞进原`moneyflow_intensity_delta_5d_rank`字段。新version分支必须同时校验model/evaluation/parameter/input身份及`planned_fits=2`；原零fit路径继续严格零fit。新解释字段为：`raw_prediction`、`intercept`、`moneyflow_level_linear_term`、`moneyflow_delta_linear_term`、`average_rank_score`、`daily_rank_group`、`model_parameter_sha256`。三项线性量按既有浮点校验策略重构raw prediction；rank score单独与封闭截面重算核对，不假称线性贡献加和等于rank score，不制造SHAP/零贡献。

沿现有same-key幂等/不同payload冲突、131完整日期原子revision、writer/readback和无mock API/UI合同。新run的产品验证记录写现有文件store并绑定当前run/model/input/row身份，不绑环境变量、后端当前commit或服务重启。生产写入/运行时选择仍需独立具体授权；不因一条研究记录控制服务。

源码复审发现现存L2表的availability、validation和run_summary CHECK只接受原零fit分支。新增`backend/db/migrations/extend_hmm_risk_rotation_l2_supervised_20261007.sql`迁移源码，保留数据库已安装的旧谓词和约束注释，只对本精确supervised version增加两fit/参数身份/解释字段分支；不新建表、不影响其他模块、不给未知version开放入口。新run真实写入前必须先在既有DEV事务回滚验证，再另行获得目标DDL/DML授权。2026-10-07用户明确批准的既有`aistock_dev`回滚验证已通过：临时执行迁移、旧版131行及新版131行writer/readback、幂等写入和非法合同拒绝，随后全部回滚。验证结束后原约束定义/注释与48,208行均恢复，测试run残留为0；不能因此声称DEV或production已持久化安装此迁移、导入正式新run或完成真实API/UI验收。

## 8. D6：预算、终止与实施方案（已批准）

一个功能包只包含三类实际工作，不设计微阶段：

1. 源码与聚焦测试：有界文件输入、唯一模型与CLI、精确产品分支和直接测试；最多三轮审核修复，无finding可以提前结束。按actual changed-files→ownership→module registry→所需最小test plan，不改全局CI/nox/test plan迁就任务。
2. 用户批准并源码交付后：一次有界file-only preflight、两个fresh children各1 fit、同窗口基线0 fit、配对评价。输出一份紧凑parent结果、必需的参数和真实预测payload；不保存逐股票大日志、源拷贝或重建旧证据。
3. 如需验证真实产品：沿已有L2导入与API/UI合同申请具体DEV/production写入目标；复用表和页面，不再重做基础架构。没有这项授权时交付有效离线结果及可导入payload，不冒报生产完成。

本段保留原实施预算，不承诺为凑10小时持续运行。用户此前批准源码合入后的2-fit已完成；当前2/2 fits，源码门禁与模型结果分别核算，实际终态见§14，不再重复该实验。

终止条件：D1～D6未批准停止于本文；批准后输入或执行失败保存typed reason并停止；两process不一致不得选择其中成功者；有效评价完成即停止该唯一候选。源码BUG登记独立BUG按现有流程修复，不能以BUG名义改模型合同。没有改善不自动生产写入、重训、读取tail或开启第二候选。

## 9. Verification Plan与审核

定向新测试计划：`backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py`；直接回归复用`test_rotation_l2_input.py`、`test_rotation_l2.py`、`test_rotation_l2_prediction.py`。以下均是实施计划，不是本次测试已通过声明：

- source范围、稀疏ID/正式文本行业、PIT成员变化、单位、正常停牌/停发、合法NA和未知缺失分离；DB和tail reader poison。
- 两项rank/精确tie、单成员、N<2、全同值；手算I20/delta；修改t日及未来源不改变t的feature。
- 126/10/232/222日历账本；训练最后outcome≤首次预测as-of；训练标签乱序规范化，未来标签不能改变参数/预测；预测不按outcome资格过滤。
- 日等权loss与alpha尺度、SVD参数回读、截距/两项原始贡献重构、rank与state；无CV/grid/retry/最终refit，失败不回退基线。
- 同日同人口配对评价、成熟/停发label、HAC真实日期间隔、不能使用旧358日指标；块效果不新增AND门。
- 双process参数/payload一致性、child共同篡改不得过parent、输出碰撞/parent finalization typed失败、总fit实际计数2。
- 新version产品validator精确允许，未知version拒绝；旧资金流0fit/HMM/state身份原样回归；完整日期原子写入、冲突拒绝和readback。
- 单独授权后，在既有`aistock_dev`实跑迁移与writer/readback，旧版及新版各131行、新版含合法unavailable行，非法预算/模型/参数身份由数据库拒绝；无fit，全部回滚并核对原约束定义/注释、行数及测试run零残留。此测试默认skip，不作为CI自动访问数据库的入口。
- 新run真实API/UI及前10/后10、总数30/31、不可用/tie/IC区间显示；文件研究记录无需环境修改或重启。

源码阶段运行精确pytest、Ruff、py_compile、diff及按ownership生成的最小HMM slice、registry/L0；广回归优先CI。源码是否runtime impact=backend必须实际重算，不预先宣称none；如果影响后端导入链，补fresh-process router/health/HMM依赖证据，不自行进程控制。

### 9.1 DESIGN-COMPLIANCE-001逐项

| 项 | 本文设计结论 |
|---|---|
| 禁止简化版/占位交付 | 一个正式定义的独立低维候选，包含评价与实际产品适配；不冒充C-010/A5或预测已确认 |
| 禁止静默错误 | 数据/identity/执行失败显式typed；合法停牌/停发不伪造；无默认1.0、neutral fallback或隐藏缩窗 |
| 禁止业务语义迁移 | 只做L2；基线/hash/旧HMM/独立risk及其终态保留，新损失/模型/窗已获明确批准，收益/风险能力不混称 |
| 禁止未经批准门禁审批 | MBE/coverage沿现有尺度提案；无显著性AND、全行业三态门、CPU/内存门或额外人工审批，D1～D6已批准，其余授权边界保持 |

## 10. Design Acceptance Index与设计验收矩阵

F-001=D1正式文件输入及人口；F-002=D2特征/标签；F-003=D3固定训练与模型；F-004=D4同口径价值；F-005=D5真实产品与状态；F-006=D6预算/停止/边界。

下表是源码及单独授权DEV合同验证矩阵，不以设计结构PASS冒充正式模型、持久化数据库迁移或真实产品完成。F-005的既有DEV回滚验证阻断已解决；前端新增测试仍须本PR CI通过，正式模型及真实API/UI验收仍按§8的授权边界单独报告。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3；rotation_l2_input.py有界读取 | backend/tests/hmm_risk/test_rotation_l2_input.py及test_rotation_l2_moneyflow_supervised.py；固定HDF非目标日期numeric-read poison通过 | verified_source | 无 |
| F-002 | §4；rotation_l2_moneyflow_supervised.py | backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py；rank/权重/未来标签拒绝通过 | verified_source | 无 |
| F-003 | §5；run_rotation_l2_moneyflow_supervised.py | backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py；真实双fresh child、正常方程零fit复核通过 | verified_source | 无 |
| F-004 | §6；共用显式窗口evaluator | backend/tests/hmm_risk/test_rotation_l2.py及test_rotation_l2_moneyflow_supervised.py；同人口配对与222成熟日通过 | verified_source | 无 |
| F-005 | §7；精确repository/UI分支及HMM自有表CHECK迁移源码 | backend/tests/hmm_risk/test_rotation_l2_prediction.py；test_rotation_l2_moneyflow_supervised.py::test_existing_dev_migration_writer_and_rollback实跑通过且全部回滚；frontend/tests/hmm-risk/hmm-risk.spec.ts已添加、执行待CI | verified_source_and_dev | 无 |
| F-006 | §8～9；CLI/parent/实际ownership | backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py；unsafe output零写入、finalization失败记录、fit预算拒绝通过 | verified_source | 无 |

## 11. Risks、发布/回滚与Production gates

风险：两项同源特征可能无足够信息、相关性高或监督拟合无改善；126日训练可能不代表后续机制；单次冻结模型不能自适应；222成熟日和10D重叠的功效有限；rank target丢弃幅度；研发已查看旧development，不能声称独立无偏确认。以上是结果解释与下一用户决策依据，不自动增加研究门或偷偷换模型。

发布不替换现存baseline/risk。只有新run实际完成产品闭合才可选它；若效果不足则保留真实研究结果、旧run继续使用，不覆写旧数据。回滚是显式选择旧已验证run，不是产生预测失败时悄悄fallback。新source影响runtime时用户控制backend-main重启；仅记录研究结果不能成为重启理由。

当前文档同步：production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；runtime_activation=noop；process_control=false；dataset_write=false；active_profile_write=false；本轮新增fits=0，既有正式fits=2；tail_accessed=false。`dev_rollback_validation=passed`、`dev_persistent_change=false`：仅既有`aistock_dev`的临时迁移/262行测试写入已执行并全部回滚。未持久化DEV/production迁移、正式数据写入或激活，不由历史回滚验证产生权限。

## 12. 本次设计审核、批准与实施状态

用户明确批准一次整包D1～D6：同源L2人口和受限输入、level+delta rank特征/rank训练标签、126日训练/10日隔离/SVD Ridge alpha=0.01/双process2fit、同窗口基线及0.02效果口径、独立新version产品身份、禁止搜索/tail/生产动作的停止条件。批准依据是用户本轮明确批准合同及源码/合入后训练授权，不是此前的“开始下一步”。

设计提交`4f9767282`阶段（2026-10-07）完成两轮作者设计审修：第一轮核算共享membership与训练可行范围，弃用不受现有PIT支持的早期252日假设，明确126/10/232/222账本及同窗口比较；修正profile SHA抄录及F2表格/section格式。第二轮核对sample-weight/alpha尺度、训练label截止、预测不接收评价label、原始贡献与rank非线性投影、效果达标与配对改善的不同语义、旧产品零fit约束和新模型2fit分支。该阶段未发现文档阻断finding；这不是独立外部审核或源码审核结论，后续源码复审发现的数据库实际阻断见§12.1。

设计提交`4f9767282`阶段F2检查实际PASS：6项索引/6行矩阵、warnings=0。严格UTF-8解码通过、replacement character为false；新文件用`git diff --no-index --check -- NUL <本文件>`检查无空白错误，普通`git diff --check`也通过。Git提示LF将按仓库配置转换CRLF是格式提示，不当作业务结果。该阶段只有本文件新增，模型/数据/tail/DB/process零操作；此历史PASS不能替代当前源码验收。

F2 validator只核文档结构和验收索引，不等同用户批准、源码审核或正式模型验收。D1～D6为APPROVED_BY_USER，源码已交付、正式2/2fit效果不足，真实新run产品验收未执行。下面保存2026-10-07源码阶段记录，其中“未运行/fit=0”指当时快照；当前只以§14为执行终态，不另建历史证据档案。

### 12.1 源码实施与当前审核状态（2026-10-07）

第一轮修正固定HDF block顺序假设、有界benchmark数值读取、输出位置保护、父流程failure receipt和逐行summary重复大payload；第二轮修正参数/fit计数类型、单线程环境完整性、共同篡改参数拒绝、effect/capability与指标一致性、新旧version隔离及保留旧错误文案。最后发现L2数据库CHECK不能接受新两fit合同，补齐精确迁移源码并如实等待DEV授权。用户补充仅既有`aistock_dev`事务回滚授权后，补齐真实数据库合同测试及约束注释保留，完成下述回滚验证。

最终第三轮作者审核修正三项阻断finding：新版available行必须同时满足structural/feature资格，unavailable行不得声称feature资格；run/child在数据集输出路径被拒绝后不得再写失败收据；产品reader和acceptance不得仅靠重算hash接受错误长度系数或布尔截距。对应7项新增用例修复前全部RED、修复后7 passed。共用参数校验避免复制数值合同；输出位置未获完整授权时只报告typed stderr，不向受保护目录写任何文件。再次核对D1～D6、旧baseline数值/hash、模型2fit及零fit旧分支、合法NA、无tail、无生产写入、无额外门禁，未发现剩余源码阻断；PR CI仍需通过才合入。这是作者多轮审修，不冒称独立外部审核或真实新模型验收。

前两轮实际验证：新模型定向17 passed；HMM八个直接测试文件91 passed（两条既存Pydantic warning）；registry为8 passed；L0 blocking=0；Ruff、py_compile、diff检查及fresh-process router/health/HMM import通过。变更的三份HMM TS/TSX strict type check通过，无emit/依赖安装；ESLint退出0，但有独立worktree依赖解析警告。最终源码提交`18a3376d5`保存全部11文件，干净状态同步main后HEAD为`5a9451216c34cbe5f3c0d172fd36059548e3e010`；重新运行同一八文件HMM slice为98 passed、1 skipped（DEV专项默认跳过）、两条既存Pydantic warning，registry 8 passed、L0 blocking=0，fresh-process router/health/HMM完整导入通过。首次slice命令误写不存在的test_api.py，未运行测试；校正为test_rotation_l2_api.py后的上述结果才是有效证据。DEV专项再次1 passed且全部回滚。Playwright本地未运行：不安装依赖或控制用户服务，交由本PR CI执行，不能写成已经通过。Nox无RTK专用wrapper，采用原生命令；pytest/git/ruff使用RTK。

业务变更仅限HMM及本文；ownership无未映射文件，所需plan为l0、validation_catalog_integrity、hmm_risk_pr_slice、hmm_risk_ui。迁移路径由通用platform.db规则匹配，SQL实际仅扩展HMM自有表的三个CHECK，不操作其他模块表或数据；不能把目录匹配称为HMM专用ownership规则。runtime impact=backend，target_ids=[backend-main]，catalog_error=null；新模型的runtime生效仍等待用户重启后核验，不影响独立离线训练。main已在干净提交后安全同步，origin/main...HEAD仍只有上述11个文件。除单独授权的DEV回滚验证外，没有正式数据preflight、正式fit、持久化数据库变更、tail、activation或服务控制；源码清理只按本轮精确授权在实际合入后执行。

DEV验证前F2曾为FAIL：F-005待授权，未覆盖或伪造此历史结果。2026-10-07用户明确补充仅既有`aistock_dev`事务回滚式迁移及写入/回读验证授权后，实际命令为（只在本次测试进程设置授权开关，不绑定业务runtime或实验记录）：

```powershell
$env:AISTOCK_HMM_DEV_ROLLBACK_AUTHORIZED='1'
python -m pytest backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py::test_existing_dev_migration_writer_and_rollback -q -s -p no:cacheprovider
```

该测试1 passed；连接前校验显式DEV配置，连接后核对`current_database()=aistock_dev`。旧CHECK拒绝新版行的RED成立，临时迁移后旧版/新版各131行writer/readback及新版幂等写入通过，新版保留1行合法unavailable；非法planned_fits、completed_fits、model_contract_hash、参数hash及旧版非零fit共5类均被DB拒绝。`Ridge.fit`设为poison，fit=0。最终事务`ROLLED_BACK`，行数`48,208→48,208`，全部约束定义/注释恢复，测试run残留=0。受验迁移文件SHA256=`8956e2d1ba986858beaf810f9c66bf58d3a35721de00c138eec2b2b1358547c4`。首次执行存在局部未注册pytest marker warning，已移除该标记并保留默认skip的明确授权开关，复验1 passed、无warning；未修改全局pytest/CI配置。

回滚验证后的定向默认测试为17 passed、1 skipped（DEV测试未获本次进程开关时跳过）；F2实际PASS、6项索引/6行矩阵、warnings=0；Ruff check/format、py_compile、git diff --check通过；L0 blocking=0。L0的4条MEDIUM均为前端测试fixture内JSON匹配的RAW_JSON_UI提示，不是产品页面原始JSON展示；未将非阻断扫描提示写成不存在。上述结果不替代尚未运行的前端CI或真实新run产品验收。

源码实施已提交并安全同步main，多轮作者审修及本地源码门禁通过，下一步创建功能PR并等待CI。DEV回滚验证不等同正式模型数据、真实API/UI、持久化DDL/DML或生产完成；源码合入以全绿CI和实时PR状态为准。本轮不启动正式训练。

## 13. 技术依据

[scikit-learn 1.8 Ridge正式接口](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.Ridge.html)：采用加权平方误差加L2收缩，显式SVD、截距和sample_weight；SVD路径不使用随机seed。实施必须核对现有数值环境及输出，而非安装最新版本。

## 14. 正式实验终态与本轮只读同步（2026-10-08）

执行工作树`F:/Dev/AIstock_worktrees/validation-hmm-l2-moneyflow-supervised-20261008`，固定源码`1dc3ad9d81bac4bb7edcf3023331e4b1addb89ec`；结果`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_supervised_20261008_1dc3ad9d/run/acceptance.json`。file-only preflight及双process各1fit完成，总2/2，参数与预测严格相同；父进程正常方程/authority读回为零额外fit。输入generation=20260928-v15-unified-moneyflow1、manifest=2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc，与批准合同闭合。

232日/30,392目录行：30,160 available、232合法quote-unavailable，后者均801011.SI；126训练日、222成熟评价日，coverage pass share=1.0。执行COMPLETED、效果BELOW_BINDING_MBE；这不是未知数据缺口、正常停牌/停发或数值失败造成的终态。

| 同222日口径 | 候选 | 原delta基线 |
|---|---|---|
| mean daily Rank IC | 0.018100248031013167 | 0.01804921719709577 |
| 展示状态真实10D spread | 0.0013824402023636126 | 0.001929789977978202 |

候选减基线IC均值=0.00005103083391739742，HAC95%区间[-0.005580388888226639,0.005682450556061434]。两块候选IC约0.03301/0.00178；块差不能证明机制时变。结论：两项同源特征的监督拟合未产生可信增量，候选低于既有0.02量级；不是所有非线性模型、价格信息或HMM均已被证伪。原358日0.0297321不能与本222日横比。

model hash=518fbb2ac7cf1b574d1a2fafe9545f442cafe2ad5ec05a872d0435133cc69aa9；参数hash=cf572c524548fb3705917fdd074aa84fa86a8db8351c17497d26119c4a80e3aa；双process预测hash=199db281e8c5a1307e6c073426c0d7904d2853f12e7fd06fa9308f9f2a3e1b1c。只引用既有结果，不复制输入/预测或重建历史证据。

该候选已经结束：不改alpha、窗口、符号、特征或阈值重跑；原基线及产品未覆盖。surface/capability/advisory=NOT_AVAILABLE、forward=NOT_STARTED、tail未读、数据库/数据集/runtime动作=0。本轮仅文档同步，新增fit=0；新互补信息提案另有精确合同，不从此批准自动继承。
