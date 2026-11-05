# 申万L2成员损失：新release与Q4连续交付

> v1.1；2026-11-06；F2；父蓝图§1.27。用户“开始执行合并的连续任务”批准本包方向和Q4四次拟合预算；源码、预检、正式研究及QE采用分开报告。正式新增fit=4/4已完成，预算耗尽；本次只回填真实结果，不改变D1～D6。

## 1. Background / 终极目标

目标仍是申万二级行业信号改善真实股票选择的收益/损失权衡，不是通过合法性检查。已完成成员损失研究#7013与8/8 fits：实际新买同样本C−B日均亏损Rank IC=+0.045903，尚无新账户净收益证明。保持旧资产，下一包一次完成新release消费、Q4滚动和必要价值诊断；QE实验由QE窗口执行。

## 2. Scope / Architecture

复用原正式20维reader、PIT/security/停牌及价格解码、固定成员标签、Ridge核与预测核。训练身份不可随数据更新改写；每次推断独立记录显式release身份，不复制旧概率换manifest。不得每月把目录名写进源码。保留Q2/Q3历史模型，以新文件重新计算其生效区间内的预测，不延长旧模型生效期。

允许源码：`backend/services/hmm_risk/risk_l2_member_loss.py`的通用文件源及纯预测核；新增`risk_l2_member_release.py`、`scripts/hmm_risk/run_risk_l2_member_release.py`和直接测试。文档限本设计、原成员设计状态、父蓝图；不改QE、Selection、Advisory、LocalSIM、数据生产、candidate/profile、CI、nox或test plan。

接续登记精确HMM消费scope：`backend/services/hmm_risk/release_industry_binding.py`、`backend/tests/hmm_risk/test_release_industry_binding.py`；`rotation_l1_input_bundle.py::_industry_adapter`仅新增显式schema分派，`formal_state_input.py::_bounded_l2_stock_facts`仅补充读取结束时binding稳定性检查。不改任何数据准备或跨业务源码。

## 3. Contracts D1 / L2-MEMBER-RELEASE-D1：保持模型问题

仍为原B固定成员组合负收益与C成员负收益均值，公式及成员/市值/整数厘/合法NA语义逐项继承原成员设计D1/D2。20维、10日、Ridge alpha=1/SVD/seed42、日期等权、train-only scaler、raw和clip[0,1]输出不变。B/C同训练mask和scaler；原C-010成熟/有效标签仍作为共同mask条件。不是事件概率或新HMM状态模型，不引入消费阈值或1-p。

## 4. Contracts D2 / L2-MEMBER-RELEASE-D2：显式新输入

active profile只在建立request时读取一次；随后显式传入root、generation、revision、release、cutoff、manifest canonical/文件SHA、calendar、security、provider及完整C-013 authority。无latest搜索、DB补齐、旧root回落或数据集写入。运行前验证真实文件，不能仅将新identity写入旧prepared。

本次预期输入：generation=`20261030-monthly-v2-unified-correction-4388f6c6d166`，release=`qe_hmm_full_v2_20261030`，cutoff=2026-10-30；manifest=`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`，文件SHA=`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。这是本次request身份，不是源码全局月份常量。

C-013在其冻结窗口内必须真实覆盖所需as-of/context；窗口之后只允许下述显式release L2消费绑定，旧9月authority不可改日期延伸。共享行业history不自动等价于C-013，10月观测分类也不能冒充此前已知。分类覆盖不足、行情缺失和模型不可用分别报告。合法停牌、停发、空行业保留typed NA及完整131，不新增每行业全部数值的门。

消费表示澄清：旧C-013窗口内逐键保持原resolver；仅对其截止之后、同release截止以内的日期，允许显式绑定该release正式`sector_membership_spans.parquet`和`sector_code_map.json`。稀疏ID只作共享join键，以申万L2文本代码经原C-013已批准投影确定L1/L2身份，不生成或推断L3。验证manifest/组件receipt/两文件hash、同release截止、131目录、一一对应和不重叠区间；未知代码/冲突必须失败，合法无归属保留typed unavailable。新binding记录baseline C-013与release成员authority的各自身份，不改旧bundle/hash，不将它称为新完整C-013 bundle。区间来源仍为研究性effective/as-of事实，`historical_as_known=false`；禁止该分支用于forward/live冒充首次发布时间已知。训练窗截止9/30，因此本次Q4训练分类仍由原C-013决定，不引入新成员筛样本或新模型语义。

## 5. Contracts D3 / L2-MEMBER-RELEASE-D3：Q4和预算

Q4训练事实窗2023-10-01..2026-09-30，严格先限定`pos(t)+10<=pos(2026-09-30)`再读标签端点。模型季度有效区间2026-10-01..12-31；本次请求只到冻结calendar的2026-10-30，实际17预测日×131=2,227行/臂。生效区间不要求未来12月日历已经存在；请求终点必须显式存在，不能截短请求。

仅Q4 B/C×双fresh-process=4次新fit。旧Q2/Q3的8-fit预算已经耗尽，不重跑。所有共同训练标签和输入在首个fit前完成验证；attempt先于调用持久化，失败即停止，无自动重试。按原固定Conda base数值环境、单线程；不安装依赖。正式CLI要求固定clean commit，不绕过源码门禁。

## 6. Contracts D4 / L2-MEMBER-RELEASE-D4：身份与双入口

v1历史模型验证仍要求其原合同/pins；Q4使用新的执行合同而非伪造为Q3。训练release、标签、mask、scaler、参数、源码和数值环境永远保留。新推断receipt单独包含实际输入identity和model bundle identity。预测核由两种受验证的载体复用，不复制实现。

完整131行业，as-of=previous open、trade=decision，不重复滞后。输出为期望损失而非概率/warning。文件数据面不借数据库兜底；现有历史数据库快照只能验证共同核，不称当前在线验收。旧A、M0/M1、旧B/C及QE已交资产原字节不改。

## 7. Contracts D5 / L2-MEMBER-RELEASE-D5：零fit诊断与消费边界

先封存两process参数与全预测，再读评价结果；前后release差异、B/C样本覆盖、损失误差、风险排序和上行机会同时报告。10月17日中仅7日能在10/30内成熟10D标签，其余10日保持未成熟；不延长读取或缩短预测窗口。该小段不能冒充强功效或未使用holdout。

既有4–9月真实新买队列可按同数据identity重新读取事实作零fit诊断；没有10月实际队列时明确NOT_EVALUATED，不捏造账户收益。新release的历史修订差异不归因于模型提升。保留historical_as_known=false、untouched_holdout=false、economic_value_confirmed=false，不自动部署、晋升或提交QE任务。

## 8. Contracts D6 / L2-MEMBER-RELEASE-D6：连续完成与停止

一个工作包连续完成设计/源码→至少两轮作者审修→最小门禁→固定源码预检→零fit重推/Q4四fit→分析交接。数据或authority真实缺口在fit前停，并列精确字段、范围及owner；不要求笼统重建数据。不能用preflight PASS代替预测或经济效果。源码交付动作依明确权限；cleanup、服务、生产及依赖未授权。

## 9. Implementation Plan / Verification Plan

直接测试覆盖：季度有效期与请求边界分离、成熟先于端点读取、显式release与hash漂移、旧模型身份不换绑、完整131和NA、B/C同mask/scaler、先登记attempt、四fit无重试及两process不一致拒绝。保留旧成员测试与最小PIT/schema/security切片；全矩阵交CI/Nightly，不反复本地跑。

### Design Acceptance Index

F-011：D1/D3/D5唯一模型与价值问题；F-012：D2/D4身份、因果和载体；F-013：D6连续交付与真实状态。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_member_release.py：schedule/training_rows/execute_child | backend/tests/hmm_risk/test_risk_l2_member_release.py：季度/成熟/共同mask/失败计数测试；真实研究状态见§11 | VERIFIED_SOURCE | 无 |
| F-012 | 同service及release_industry_binding.py；共享reader显式分派与读取稳定性检查；本行仅source/文件载体断言 | backend/tests/hmm_risk/test_release_industry_binding.py；backend/tests/hmm_risk/test_risk_l2_member_release.py；正式文件回执见§11.1 | VERIFIED_SOURCE_FILE_ONLY | 无 |
| F-013 | scripts/hmm_risk/run_risk_l2_member_release.py：guard/clean_head/双process与不重试 | backend/tests/hmm_risk/test_risk_l2_member_release.py：attempt先行/两process预算/完整grid回读；生产状态见§11 | VERIFIED_SOURCE | 无 |

本Index没有将新数据数据库事实实读或当前在线加载列为已验证断言：D4只允许既有历史数据库快照验证共同核，不承诺本包直接上线。§11.1单列NOT_EVALUATED，不是获豁免的已完成项目；后续真实双入口消费仍须独立核验。F2源码/文件合同通过不能消除该未完成的业务状态，也不使本包成为生产采用。

## 10. Risks / Production Gates / Rollback

输入修订可能改变历史信号；C可能仅度量波动而非可避免亏损，较高亏损仍伴上涨机会。Q4模型不是11月生产采用证明。合法NA导致完整案例偏差，全部披露。回滚保持原模型/资产和消费者指针，本包不修改它们。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false。源码runtime分类以实际changed-files为准，不由离线执行推定none。

## 11. 审修、实施与真实状态

### 11.1 最新终态（取代下方开发过程的待运行状态）

固定研究HEAD=`25e5a8ffcf273271793fa4baf68a2bdaff2257fd`；输入manifest仍为§4冻结值。正式prepare_v4及file_preflight_v4 PASS，共同训练716日、89,871行；prepared canonical=`203e4beaf175e453953bd81d860c2af9031265d2589347fb8ba971663c8b6929`。双fresh-process正式4/4 fits字节一致、父进程封闭通过，acceptance canonical=`8342abe3b6c65b0a2e61e4a91cd1303174b04feaa62c25ddecea0f2fbaae52a6`。B/C各17日×131=2,227行，全部available。输出及唯一结果摘要位于`F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/`。旧8-fit和新4-fit均不得重跑。

原Q2/Q3四模型已在原生效期基于新release真实20维重新推断，四次fresh-process CLI均零fit，旧资产SHA未变。封存之后读取评价事实：4–9月125个全行业共同定义日，B/C对成员亏损Rank IC为0.186587/0.255571，C−B=+0.068984；不是原QE新买同样本99日的+0.045903口径，不能相互比较或当作收益提升。C高风险组的平均成员亏损5.8694%、上行5.1206%；低风险组为2.4902%/2.0595%。风险与机会同时增强，不能自动套禁买、降仓或1-p。

Q4仅7个成熟日，B/C Rank IC=0.664752/0.661386，C−B=−0.003366，未证明新季度C增量；另10日保持OUTCOME_NOT_MATURE。全期另有2个合法FIXED_COHORT_ENDPOINT_NA，不补价或删成员重归一化。没有QE账户净收益证据，没有同新输入的数据库事实实读，原历史双入口不冒充当前验收。QE新买队列/新输入双入口/消费者准入各自仍NOT_EVALUATED；无新QE任务、DB/数据集/profile写入或服务动作。

源码PR #7037目前范围门禁阻断：独立修复的登记记录随分支集成，PR被按BUG范围验收。独立价格编码修复PR #7049最终CI全绿但未合入；待用户授权先交付该PR，再同步主线及复验feature最终HEAD CI，不删除正式BUG记录、扩大BUG scope或修改流水线。研究执行身份保持上面的固定HEAD，不伪造绑定到后续文档提交。

结果文档第一轮自审核对原回执/文件SHA、125与7+10日算术、716/89,871训练mask和4/4计数；第二轮核对与父蓝图/原成员设计的一致性，明确全行业与真实新买口径不同、上下行机会并列、无新数据DB实读和QE采用。没有独立第三方审核，缺口不因F2 PASS消失。已批准D1～D6未修改，不为结果记录新增环境变量、DB表或重启。

### 11.2 开发审修过程（历史状态）

初始发现：共享C-013目录仅存在8月和9月，10月release内industry_l2_history_authority的end_date仍为2026-08-31；尚不能宣称10月C-013闭合，也不能断言基础行情缺失。新fit=0/4，旧8/8保持。

真实零fit开发读回`F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/source_boundary_v1.json`：142日完整request/Q4 17日；C-013边界止9/30，首个未覆盖as-of为10/08，共16个as-of。明确`development_source_uncommitted=true`、`full_file_preflight=false`，不是固定源码正式预检。10月共享组件回执同时给出83,565个factual stock-day、missing membership/unknown code/quote gap均0；该月源事实看起来已具备，下一步先由HMM核验/构造合法消费者authority绑定，不要求数据窗口笼统补数或重建。

第一轮作者审修发现并修正：原C-010合法NA状态为OUTCOME_LEGAL_NA；封闭必须由parent依据prepared重算整个请求grid和预测，不能信任child自报日期/fit；特征载体拒收outcomes或额外标签字段；旧v1模型继续原验证路径。第一轮8项直接测试和Ruff通过。这里的VERIFIED_SOURCE仅指源码合同，不能推导4-fit、全量数据或QE交付完成。

第二轮作者审修核对输入/预算/旧版隔离：边界检查细分原季度重推、Q4训练和Q4推断，不把整套基础数据判坏；Q4请求末日由parent固定，删除日期或伪造数值均不能靠两child相同而通过；失败attempt先落盘，已有attempt阻止重试。由现有原模型与原feature进行524行有界零fit回归，提取后的共同预测核与原sealed结果完全一致，旧B/C及A文件SHA未变。两轮均为作者自审，不冒称第三方审核。

初版局部验证（接续前）：`python -m nox -s hmm_risk_pr_slice -- <新旧member测试及security/provider直接文件>` 41 passed；ownership 7/7、无歧义；L0零finding；module-registry L0 8 passed；Ruff/格式/compile及hmm_risk、health、新service的fresh-process import通过；F2为3项/3行、warnings=0。未跑完整HMM或跨模块矩阵。canonical实际分类为backend、target_ids=[backend-main]、catalog_error=null，不写成none。

DESIGN-COMPLIANCE-001逐项：没有简化模型或placeholder，预测/标签保留完整131和合法NA；未用默认概率/现金或任何静默fallback；模型、标签、窗口、seed与本D1～D6一致，旧资产不覆盖；未私增行业样本量/全行业必过或生产审批门。初版源码审查时authority消费绑定尚未解决，该状态已被以下接续读回取代。正式clean-commit file-only预检、训练、新资产和QE就绪各自记录，不以局部测试/F2 PASS冒充业务验收。

2026-11-06接续：按D2的表示澄清完成HMM release L2消费绑定。真实文件开发读回`release_industry_readback_v1.json`为PASS：17日、83,565 PIT股票日全部resolved、缺失0；原C-013仍止9/30，新projection coverage至10/30。binding canonical=`d53b033790108c851d15b3cdc7cfb48aff45d2c70c6af63f2f9ddf101b29ffb7`；实际request为同目录`request_v3.json`，字节SHA=`f21087884e9b617e8372fc7b4ae46a5a2187d05b1baa4757b48250b07ac7ffb9`。旧季度重推、Q4训练和Q4预测的分类边界均通过。该读回使用未提交开发源码，仍明确full_file_preflight=false；不能将其当完整20维正式预检。

本轮两轮作者审修覆盖：显式source root/release必须与分类binding一致，旧调用不扩大新schema支持；历史resolver逐键不改、新区间仅作文本投影；稀疏ID、顺序及合法文本规范化、重复/未知ID、hash/组件缺失、错误cutoff和forward模式均有直接拒绝测试。相关矩阵曾210 passed/1 failed，原因是旧calendar隔离测试不提供新source上下文；仅新schema传入新约束后，该定点回归通过。未改旧测试、未放宽来源检查。模型/特征/预算和数据集字节均未改。

接续最终本地矩阵为**211 passed**（新分类绑定、member新旧合同及直接formal/source/date输入），4条已有合法空/pandas数值warning原样保留；L0零finding，scope/ownership为11/11且无歧义，F2 3/3 warnings=0，fresh-process在DB/network poison下导入hmm_risk、health和新依赖通过。Nox无专用RTK wrapper，直接用既有Python执行，不安装环境。未跑完整HMM或其他模块矩阵。源码PR、最终提交及固定HEAD正式预检/fit在任务回执分别报告，不在本设计预填通过。
