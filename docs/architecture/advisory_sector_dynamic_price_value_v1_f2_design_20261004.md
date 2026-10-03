# Advisory 动态板块条件价格价值 M1 F2详细设计 v1

2026-10-04；R2连续任务的第四个候选，研究类型EXPLORATORY_SCREEN/NAVIGATION_ONLY，非生产模型。详细设计先合入，随后独立源码树实现。单候选失败继续下一真正新增信息设计，不结束48小时任务。

## 1. Background / 当前事实与假设

R2设计#5395、三模型源码#5404已合入且自身官方清理完成。M2/M3/M4共11真实fit+1索引，各完整81D/1620原候选/100估值日四臂，真实模型TAKE81/89/78，日净增量均不达标。不是旧R1的TAKE4支持不足；不回选控制、不继续同13D换算法。经济确认/ENTRY_VALUE启用仍0。

H-SECTOR-DYNAMIC-2：在同12D+价格条件、同标签及共同监督上，加入D可见的板块强弱/波动与个股相对偏离，能否产生相对matched及原Top5的成本后增量。这是信息块比较，不是因子挖掘、上游包训练、HMM重训或新退出策略。

## 2. Scope / 精确文件与所有权

设计树`F:/Dev/AIstock_worktrees/advisory-sector-dynamic-value-design-20261004` / `feat/advisory-sector-dynamic-value-design-20261004`只写本设计及蓝图；合入后创建自己的源码树。允许源码精确范围：

- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_source_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/services/advisory_model_first/economic_price_campaign_evaluation_v2.py（仅提取现有完整四臂纯公共helper，不改变旧数学/政策/阈值）
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_source_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_price_campaign_evaluation_v2.py（提取helper的精确回归）
- docs/architecture/advisory_sector_dynamic_price_value_v1_f2_design_20261004.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

源码树登记：`F:/Dev/AIstock_worktrees/advisory-sector-price-value-v1-20261004` / `feat/advisory-sector-price-value-v1-20261004`，基于已合入#5411的最新origin/main，仅上列精确文件可写。

仅Advisory消费者/纯模型。临时runner/log/cache在X任务目录；新源输入和研究在`F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004`独立hash目录。不覆盖任何旧plan/输入/模型/结果，不改公共QE/Selection/数据生产者。

## 3. Non-goals / 安全边界

不写数据库/DDL/DML，不激活profile/重建股票池/候选，不安装依赖，不控制backend/worker/QE或他人进程；后端重启user-owned。本轮离线无需重启。不读sealed/新holdout、不申请旧失败补证，不研发分钟买卖点、执行/订单/仓位，不宣布真实收益或激活。

不以当前股票行业成员回填历史、不以名称或字符串猜行业码/指数码、不把当前映射或恢复证据声称历史原生receipt。源条件缺失只暂停M1，继续本轮其它有经济解释的新信息设计。

## 4. Architecture / 最小共核

复用现存Advisory只读profile/池消费者、严格IndustryPitResolver、授权冻结父输入及value标签，R2非执行JSON树导出/推理、全局价格支持和durable stages/registry。新三个叶模块分别负责：纯模型/plan及价格函数、来源/行业映射/日行情计算、登记/准备/拟合/评价编排。现有R2评价仅把公共完整四臂计算提成函数，旧调用默认合同不变；不复制第二个平台或每模型一套组合模拟器。

纯查询可单行/批量/完整tick集，接受固定D信息及假设价格，不读收益/真实T后路径。T观察只把真实raw open代入D冻结函数，不能修改D特征或用T HLC分类可用性。

## 5. Contracts / 来源、版本与PIT

### 5.1 身份与行业码映射

显式profile path/sha，当前`20260928-v15-unified-moneyflow1` / `qe_hmm_full_v2_20260831`，sha56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f。父prepared、12D feature、同政策labels及严格分类snapshot均绑定原manifest/plan/profile/code/hash、原键和D/T；保持原386D/7720候选、池外14及原RECOVERED_LIMITED/native UNPROVEN，不用当前池删旧候选。

权威结构映射来自一次只读`market.sw_index_classify`查询：level=L2/src=SW2021，共134唯一industry_code/index_code pair；与[官方SW2021分类表](https://tushare.pro/document/2?doc_id=181)精确代码对照及profile code_map验证。使用明确行业码→指数码，而非名称匹配；profile市场码采用其实际.SI标识。官方标准表是结构身份参照，不是成员时序/原生捕获收据。snapshot记录真实读取时间，绝不倒填known_from/捕获时间。

这是一份**版本化结构码转换**，不是时间变化的业务feature。必须证明taxonomy_version=SW2021、显式代码一一映射、历史D分类源也声明相同版本；无法证明该版本内稳定关系或有一对多/源冲突，则对应映射不能用于因果研究，M1暂停或逐行UNKNOWN，不能从当前表自动推定历史行业。134结构码与131市场可报价码不同不等于丢失股票，未发布行情的码正常UNKNOWN。

公司D分类仍只用原严格AS_PUBLISHED_PIT可知3729行；3977知晓未知不得借映射转为已知。D分类known_from≤D、唯一身份/来源bundle/profile匹配是硬合同；不得用专门4股指数成分证据作为全市场分类证明。静态映射的验证不提升父研究native证据等级。

### 5.2 冻结板块行情

profile sector_context_pins绑定`components/factor_h5_static_candidate_v2/sector_data.h5`，sha575ad57d52567857a028ff71809ea776894dce3f00421182ecf9ebdb4e3402f3；表3339006行，l2_code_id/sw2_close。只读分块提取日期/id/close，并对同D/id重复报价一致性核验，得到板块日报价；**不消费H5 instrument对应的历史行业赋值**。公司所属板块来自上一节严格D分类，经结构map→profile numeric id连接。

2024-07-04/05训练源5706行/130 id，重复日/id报价冲突0，仅证明读法可行，不证明完整窗口/PIT/经济通过。正式prepare先核定精确H5/profile/code_map/bundle hash，最多100000行一块；读后复核字节身份。缺quote、不发布、正常稀疏或不足warmup保留候选UNKNOWN；源明确声明的null/未分类sentinel仍UNKNOWN，不擅自变可用。与已证明namespace冲突的positive id、同D/id矛盾、非法已知值和hash漂移硬失败。禁止前填/零填价格、选择性删日期或把不完整窗口当连续有效。

### 5.3 固定新信息块

新增三个字段均为D及过去：sector_ret5（当D板块close/5交易日前close−1）；sector_vol20（20个完整单日simple return的sample std，ddof1，需21个完整交易日close）；relative_ret5_sector（原ret5−sector_ret5，比例单位一致）。第三项是有经济解释的派生交互，只有前两项增加独立信息，不声称三个正交alpha。

日历来自同冻结窗口的交易日合同，最后一日恰好D。无21个完整既存交易日时warmup UNKNOWN，不临时补当前行情；必须按完整日历重索引后检验21个原始close，不能丢失交易日后把21条稀疏quote当21日。只读D及之前值，字段不能使用后续收益/label status/maturity判断是否可用。strict分类缺失、任一新字段缺失时两模型共同UNKNOWN，原12D仍保留，不能把可选行业缺失升级为整个基础业务失败。

## 6. Model / 固定四fit与matched

两臂均固定GradientBoostingRegressor（sklearn1.8.0/scipy1.16.3）：200 trees、lr.05、max_depth3、min_leaf30、subsample1、seed20261004、无early-stop/调参。各有gross Y均值及path L q.1两头；matched只用13D（12D+g）；candidate用16D（12D+三个新字段+g），同监督/标签/价格支持。分类码/id不作为数值模型变量。

共同监督为原12D完整、三新字段AVAILABLE、合法gap、同VALUE_REVIEW_5_V1成熟Y/L且实际label_information_end未越train boundary；最低100监督行/20D，不足不fit。global支持仍从原train无标签12D合法价格观察、T≤train_end构建，100bps桶/30观察/5D与gap2.5～97.5%及支持洞，**不按收益或分类成功筛支持**。预测是否可估只用D输入/支持，不能用未来label可用性。

窗口沿父train2024-07-04～2025-05-30、val2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02，label截止2026-03-10；这些属于plan，不是公共默认硬编码。test不训练/校准/选点，val只诊断不反调。每次fit前持久STARTED，partial禁止隐式重试；四fit一次、无seed或超参网格，整个R2最多15fit加M4一个索引（原11fit不重置）。

树只导出公开结构非执行JSON，不pickle/joblib，float32分裂语义与原预测parity；predict每个合法价格节点，扣buy.95/sell5.95bps一次，期望net>0且下行参考≤800bps，允许多段/空集/UNKNOWN，不假设越低越安全。不得把路径q10当校准成功率或最优盘中买点。

## 7. Evaluation / 完整四臂

同原Top5 baseline、±300bps规则、matched、candidate；完整81D/1620候选/共同100估值日，原Top40复评/VALUE_REVIEW_5_V1/固定五槽/现金0。市场可执行但模型UNKNOWN仅研究控制保留原动作，其贡献分列不算模型TAKE；市场/法规未知所有臂不进入，不补Top6。

M1开发导航门沿R2事前合同：减baseline及matched日均net均≥5bps；两者实际进入差异≥12决策日及≥15%原决策日；真实模型TAKE≥30episode；相对两者MDD恶化≤200bps、最差5%日均值恶化≤20bps。block5/reps2000/seed20261004区间仅开发导航，不独立显著性。端点/held-mark/未结算任一不证，经济BLOCKED且不输出净收益指标。M1失败只结束M1，不把来源PASS当效果或整体停止。

原候选/部分行业可知产生大量UNKNOWN控制时必须解释真实干预/贡献，不把全组合回报都归给新信息。正结果只制定未消费独立确认设计，不读sealed、不挑结果较好合同/控制、不生产binding。与旧R1或三路线的不同训练人口不做跨run信息增量归因。

## 8. Implementation Plan / 本轮接续

先结构映射、严格分类、日行情/日历可识别性检查→本设计三轮自审/F2校验/独立PR合入→最小三个源码叶及公共评价helper/定向测试、重复审核修复→清洁源码提交、来源专属小snapshot及四fit计划预登记→公开QE三路径空闲时prepare/train/evaluate→蓝图真实结果、源码PR/必需CI/合入/自己官方清理。M1不成立继续D可见Selection状态动态等真正新信息的独立设计，不能无预算盲加实验。

源检查/工程预算最多6h，fit≤30min、2线程、RSS/新工件各≤2GiB；最多7720候选/500000价行、H5块100000行。长实验30min检查，短实验完成即接续。总任务截止2026-10-06 02:36，预算不是要求睡满；有其它有价值路线时不能因一个失败/CI等待结束。

## 9. Verification Plan / 多轮审核与最小测试

来源：精确版本/显式代码映射、一对多/行业未来时钟/未知id/报价矛盾/hash漂移拒绝；索引无未来归属，H5重复报价只用同D/id；缺交易日、normal缺失、不发布、warmup保留原keys/UNKNOWN。计算：21完整日、D cutoff、单位/ddof、未来行毒化不改D输出，新字段可选但其身份矛盾不能默认。

学习：共享监督/实际maturity、test poison不改变fit/支持、global支持无标签且不按分类成功筛选；13/16维各两树JSON parity及4物理fit、控制不含新块；纯节点/批量/多段/tick一致。工程：不可变stage/registry/partial拒绝、15累计fit不能重置、QE未知暂停fit；完整四臂公共helper保留旧算法，端点/持仓未证不输出metrics、UNKNOWN不算模型TAKE。只跑最小相关矩阵，广回归交CI，不重跑旧负研究。

## 10. Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-697 | 新动态板块信息而非同13D再换模型，负candidate不结束整轮 |
| F-698 | 版本化结构crosswalk/严格D行业分开，知晓未知/旧native限制不升级 |
| F-699 | profile/H5/map/源hash绑定，日/id去重与D完整日历，缺失保留/冲突硬失败 |
| F-700 | 同政策标签、共同监督、maturity、global支持无标签、test不fit |
| F-701 | 固定13/16 matched与4fit、JSON parity、纯价集/成本/tick/风险 |
| F-702 | 完整四臂/真实TAKE/UNKNOWN、冻结经济与风险门、开发/确认隔离 |
| F-703 | 原子登记/部分fit不重试、15累计预算、QE互斥及资源 |
| F-704 | 精确Advisory范围、多轮自审修复、合入/清理与用户重启分离 |

## 11. Design Acceptance Matrix

设计#5411已合入。源码仅上列Advisory文件，已执行三轮自审修复及定向测试。正式预登记`advsectorvalue_d22f697febf9d36f501e1216`，完整prepare已于2026-10-04 04:12通过：7720原候选全部保留，50180日/id报价重复冲突0，新增块AVAILABLE3505/UNKNOWN分类或映射3991/warmup224；原严格可知3729=3505+224，池外14仍UNKNOWN，原3977知晓未知不解锁。SW2021的134结构pair与官方表精确一致，profile行情131码命名空间包括合法id=0。源读取没有数据库写入、sealed消费或原生证据升级。

2026-10-04 04:13拟合前公开QE检查：single running0、custom_evo running0、multi-alpha running2（macb_idem_d1b438d28cd03452c4ad60f2398abe210e809598 / macb_idem_7860ec944ad54176cbe1d102ccabdc89d078b50e）。遵守本轮互斥合同，M1正式fit保持0；工程可交付、研究待闲时继续，不操作其它任务或把研究待运行当经济通过。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-697 | economic_sector_price_value_v1.py；§1/8 | test_economic_sector_price_value_v1.py；artifact: 独立plan | SOURCE_VERIFIED_RESEARCH_PENDING | approved_by_user: 只交付离线研究消费者，正式fit待QE空闲，不代表经济完成 |
| F-698 | economic_sector_price_source_v1.py | test_economic_sector_price_source_v1.py；artifact: 原prepare源摘要 | VERIFIED | approved_by_user: 非历史捕获receipt，native UNPROVEN不变 |
| F-699 | economic_sector_price_source_v1.py | test_economic_sector_price_source_v1.py；artifact: 7720完整prepare | VERIFIED | none |
| F-700 | economic_sector_price_value_v1.py | test_economic_sector_price_value_v1.py | VERIFIED | none |
| F-701 | economic_sector_price_value_v1.py | test_economic_sector_price_value_v1.py（实际sklearn/JSON、13/16维、4次单测fit） | SOURCE_VERIFIED_RESEARCH_PENDING | approved_by_user: 单测fit不计正式研究trial，M1正式fit0 |
| F-702 | economic_price_campaign_evaluation_v2.py；§7 | test_economic_price_campaign_evaluation_v2.py（真实shadow完整四臂/持仓不证明阻断） | SOURCE_VERIFIED_RESEARCH_PENDING | approved_by_user: 81D正式研究未运行，不宣称收益/激活 |
| F-703 | economic_sector_price_pipeline_v1.py | test_economic_sector_price_pipeline_v1.py；artifact: 登记/prepare | VERIFIED | none |
| F-704 | §2/3/8/9/13 | artifact: 精确范围/三轮自审及无runtime/DB操作 | VERIFIED | none |

## 12. Risks / 结论边界

结构码版本验证不是股票成员PIT，不能用当前所属行业/指数成分解锁原UNKNOWN；行业历史报价可能稀疏，模型输入支持不足不全局删股票。静态映射若不能被版本事实证实稳定，不能训练本M1。三个字段有派生重叠，开发期多候选存在研究选择偏差；即便正结果也不是独立确认或经济收益承诺。低覆盖时UNKNOWN研究控制可能主导组合，因此真实TAKE及相对matched不可省。

## 13. Rollout / Rollback / Production Gates

仅独立离线研究，不生产family/API/UI/绑定，backend_restart_required=false，DDL/DML/依赖/数据激活/运行activation=noop。错误停止M1并保存真实状态，不覆盖旧工件；后续生产角色独立确认和产品设计，若需要重启由用户。

DESIGN-COMPLIANCE-001逐项：①设计/源码/输入/开发导航/确认/启用分报，不以partial完成业务；②UNKNOWN及身份/时钟/无已证明端点fail closed不默认成功；③新信息独立lineage、预算及参数结果前固定，旧负结果不改判；④不新增数据/行业窗口强制依赖、平台/UI/自然等待门禁，不越界修其它模块。

## 14. 三轮设计自审

方法轮核对新块只有两个独立信息量、派生relative不能冒称第三alpha，同模型matched隔离新增信息且不比较不同旧人口。时钟/源轮将结构码映射与成员可知分开、当前读取时间不倒填，补完整交易日重索引/21原报价不可稀疏跳日，保留原严格分类UNKNOWN。工程/边界轮修订声明的未知sentinel与unexpected id冲突区别、提取旧四臂helper而非复制平台、累计15fit不重置，明确SOURCE预检不等于全窗/经济/生产完成。本窗口自审不是独立外审；尚无M1收益或拟合结果用于修订。

源码三轮自审：第一轮核对真实合同类型与价格集合、多段/支持洞，修复profile数字id=0须保留为合法id而非缺失；第二轮检查21完整日与实际maturity、可选缺失和共同mask，NaT成熟时钟硬拒绝；第三轮核对仅提取旧四臂公共计算、真实shadow定向回归及held-mark阻断，累计预算固定15且不可由调用参数放宽。单测初次两处预期/空值类型错误已针对性修订；最终11项PASS。源码提交后才预登记并运行，结果不用于调参。
