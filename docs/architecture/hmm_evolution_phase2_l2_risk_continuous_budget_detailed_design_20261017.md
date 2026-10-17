# HMM Phase 2：L2 连续风险预算消费价值研究

> v1.0；2026-10-17；F2；HMM owner。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md`。
> 精确合同 L2-RISK-CONTINUOUS-D1～D6：**APPROVED_BY_USER_FOR_IMPLEMENTATION_AND_ONE_STUDY**。用户本轮对明确问题回复“批准完整推荐合同”。只批准一个新消费公式和一次双 fresh-process 零-fit 研究，不授权自动采用、生产操作、合入、清理或服务控制。

## 1. Background、Goal、Scope

终极目标仍是申万二级行业的风险降低和轮动/场景增益，不以模型程序合法替代价值。已消费的104日风险概率中，103个可消费信号日出现1095次二值 warning 翻转；旧 R 的参考换手9.829859，高于无overlay B的2.330465。这只是检验连续减配的依据，不证明新规则有效。旧结果来自当时批准v17，只作假设依据；本研究正式重新读取当前v18，不给旧数字改身份。

本包一次闭合：固定模型/20维特征的v18文件输入、双进程固定参数推断、五臂参考价值比较、直接测试、两轮以上作者审修、紧凑结论。既有两日确认政策已经研究，不再运行。没有QE实验、执行引擎、API/UI、数据准备、数据集激活或历史证据归档任务。

## 2. Architecture / Contracts D1：唯一源与冻结模型

正式输入仅为generation=`20261015-v18-unified-quote1`、release=`qe_hmm_full_v2_20260831`、cutoff=`2026-08-31`、state revision=`20261015-r8-unified-quote1`，manifest identity=`f82670728cd6cde06bbc834481187794d081070689648c06b88786a7782e435b`，manifest bytes SHA=`1d3602211a7828cc5696d8eb206e49edf1bdccf447cd2a85c0a7186e02b8bf5c`。request显式给绝对普通candidate root、security/provider/PIT authority与原模型资产路径。验证既有正式reader的manifest、组件、共享ID→官方文本代码、PIT、停牌/合法缺失和quote authority。不得latest搜索、旧数据回落、改共享编号、重导数据、查询数据库、补0或前填。

模型仍为原风险模型canonical SHA=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`，从原正式acceptance/sealed/features的独立pins验证完整参数承载关系，而非接受任意自报hash。原模型训练边界、标准化参数、20项C-010/A5定义、seed、阈值均不变。源kernel只新增显式frozen release参数；旧调用默认行为和原历史合同不变。

数据和模型引用是两个不同身份：模型的训练来源保持原值，新推断/收益来源记录v18真实identity；禁止仅修改旧features manifest字段。源文件读前后稳定性、参数canonical hash、原feature definition、131官方文本代码投影均闭合。

## 3. Contracts D2：固定日期与因果边界

复用已消费历史2026-04-01..2026-08-31，共104个prediction decision日和103个收益日。全部日历从冻结文件计算并与已批准schedule校验。每个decision t的20维输入仅到prev_open(t)。收益日u消费prev_open(u)的信号（该信号自身只知更早as_of）；不使用同日收盘后信号交易前收盘。

准备特征不读取prediction窗口之外的未来收益/事件标签。两child先独立完成相同固定参数推断并封存；parent验证两份sealed与request/features/model/日期/行业相等后才构造103日收益facts，然后交给两个child比较。只取当前获准且已消费窗口，不读2026-08-31之后数据，不重建10D事件标签、不重新filter HMM，不进行任何fit。

## 4. Contracts D3：五臂、现金与同敞口

固定目录N=131；预测时刻合法概率行业集合E，warning集合W={i∈E:p_i≥0.20}。每行业不可用预算保留现金，不重新分配；不可用不等于低风险，不因未来收益再筛行业。

| arm | 当日目标行业预算 |
|---|---|
| B | i∈E时1/N，否则0 |
| R | i∈E且非warning时1/N，否则0 |
| X_R | i∈E时sum(R)/len(E)，否则0；E空时全现金 |
| C | i∈E时(1-p_i)/N，否则0 |
| X_C | i∈E时sum(C)/len(E)，否则0；E空时全现金 |

两matched仅按同日已知目标总敞口构造，不用收益调权，不声称同beta。p必须有限且∈[0,1]，warning严格等于p≥0.20；未知status、非法sentinel、未来as_of和缺行业直接typed fail closed。不得阈值搜索、概率变换、确认天数或额外杠杆。

## 5. Contracts D4：价值算术、NA与成本

复用既有`risk_l2_value_replay.arm_day/_blocks`，不复制漂移、现金、换手或财富公式。每臂首日从全现金建仓；其后单边行业换手为sum|target−上一日收益漂移后持仓|。展示gross路径和0/5/10/20bp×单边换手示意成本路径，不将示意成本称真实净收益。原C-010/A5个股聚合收益不是可交易指数/实际成交账；basis=`GROSS_SYNTHETIC_L2_REFERENCE`。

任一E行业缺失收益，当日五臂共同paired不可用，不补0，不按持仓臂各自删日期；块只作诊断，不拼接完整净值。缺失后的首个成本换手未知，不伪造重置。正常停牌/停发维持现有typed NA，不另加逐行业效果门；资本耗尽明确报告，不称程序失败。

完整报告五臂收益、最大回撤（负数）、最差日、下行平方损失、敞口/现金、换手、availability及机会成本。比较C−B、C−R、C−X_C、R−X_R，并完整列四档成本。无完整103日共同路径时`INSUFFICIENT_REFERENCE_PATH`；否则如实报告连续规则是否相对X_C有回撤改善。这个描述性结果不是正式promotion、forward confirmation或产品收益保证；不设置临时阈值制造胜出。

## 6. Contracts D5：双 fresh-process 与失败

在同一clean committed源码、现有Conda base固定数值环境与单线程参数下运行两个独立Python child。固定numpy/scipy/sklearn等版本与原模型相同，不修改AIstock环境或安装依赖。parent逐项核对schema/request/features/model/source commit/完整calendar/catalog/contract/fits=0，并要求业务payload canonical bytes严格相同；不靠自洽hash替代权威匹配，不用误差包络静默放行。

所有输出到显式repo-external普通路径，write-once，禁止候选/代码/旧资产覆盖、symlink/junction和collision。child与parent失败分别写typed failure，真实completed/unknown如实保留，不能生成成功acceptance；不将过程记录绑定部署环境变量，不为研究记录重启服务。只保留当次必要输入与紧凑结果，不整理历史证据。

## 7. Contracts D6：终止与交付边界

一次批准研究完成或合法数据/身份阻断即停止本候选。报告价值/机会成本/不确定性，不自动开新候选或替换现有risk warning，不自动合入、清理、写库、切profile或激活runtime。P1独立HMM数据库兼容缺陷另走BUG；P2新月数据待owner给唯一冻结身份后由HMM做消费预检，均不阻断本v18研究。

### 7.1 Implementation Plan

一包实现五臂比较和薄CLI：原risk模型carrier验证抽取为不读取旧numeric release的helper；原source kernel增加显式release binding，旧默认不变；复用固定风险推断并传入本次“已消费”合同，不能继承原历史独立性声明。新模块为`backend/services/hmm_risk/risk_l2_value_continuous.py`，薄CLI为`scripts/hmm_risk/replay_risk_l2_value_continuous.py`。完成聚焦测试、两轮作者审修和提交门禁后，在clean commit执行一次正式研究并给出采用/不采用建议，不另拆小阶段或新增平台。

## 8. Verification Plan / Design Acceptance Index

F-011=五臂真实价值口径；F-012=当前文件输入/因果/身份/零fit；F-013=双进程完整结果而非API/UI假升级。本表先审核定义完整性；SOURCE/EXPERIMENT状态单独记录，不以F2结构PASS推导执行成功。

### 8.1 Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_value_continuous.py；§3～§5 | test: backend/tests/hmm_risk/test_risk_l2_value_continuous.py；公式、同敞口、时序、NA、漂移/成本、机会成本 | DESIGN_REVIEW_VERIFIED | 无 |
| F-012 | backend/services/hmm_risk/frozen_l2_history_input.py；backend/services/hmm_risk/formal_state_input.py；backend/services/hmm_risk/frozen_l2_history.py；§2～§3 | test: backend/tests/hmm_risk/test_risk_l2_value_continuous.py；显式v18、原model闭合、无旧回落、fit/DB poison | DESIGN_REVIEW_VERIFIED | 无 |
| F-013 | scripts/hmm_risk/replay_risk_l2_value_continuous.py；§6～§7 | test: backend/tests/hmm_risk/test_risk_l2_value_continuous.py；child authority、双process、collision、typed失败/终态 | DESIGN_REVIEW_VERIFIED | 无 |

| 真实执行维度 | 当前状态 |
|---|---|
| 精确合同 | APPROVED_BY_USER |
| 源码/直接验证 | 作者审修通过；31项新研究测试、52项直接helper回归通过；与正式研究状态分离 |
| 正式双process研究/价值 | NOT_RUN；必须以实际结果更新，不沿用旧数值 |

最小本地门禁：changed-file Ruff/py_compile、直接新研究矩阵与受影响旧helper回归、git diff --check、ownership/module/test-plan路由、L0、F2。广域Nightly由CI执行，不重复全量HMM。代码完成至少两轮作者审核/修复，保留结果，不声称独立第三方审核。

## 9. Risks、Rollout / Rollback、Production Gates

104日历史已消费，仅是新消费公式development研究，不能重新命名holdout。连续缩权可能降低换手也可能错失收益；须与自身同敞口比较，不能靠更多现金宣称选择有效。无真实执行与成分交易账，QE场景价值留给QE owner另验，不启动QE。研究新文件不改运行产品，rollback为不采用，不删除旧资产。

production_ddl_gate=noop；production_dml_gate=noop；frontend/backend dependency=noop；dataset/profile write=false；runtime activation=false；process control=false。canonical分类按三个既有helper保守判为runtime_impact=backend、target_ids=[backend-main]、backend_restart_required=true；补全用户所有的runbook/identity/business probe及fresh-process import后pre_pr_ready=true、blocking=[]。不能人工降级为none，也不能将离线研究完成冒充后端生效。离线CLI在自己的fresh process加载提交源码，不需要控制后端；源码合入后的backend-main生效仍等待用户重启与只读验证。旧helper默认行为保留，直接回归已通过。

## 10. DESIGN-COMPLIANCE-001 与审修

1. 无简化版：五臂/完整目录日期/四成本/双process/机会成本一包，不交placeholder。
2. 无静默错误：不补0/前填/回落，不拼缺口路径，权威/时序/非有限错误typed停止。
3. 无业务逻辑迁移：本次唯一新公式已获精确批准；模型、20维、warning及产品默认行为不变。
4. 无私增门禁：不增加显著性、CPU/内存、逐行业全通过或人工审批条件；合入/生产权限独立。

第一轮作者设计审核：明确旧诊断v17与新研究v18分离；将连续规则自己的X_C纳入，避免敞口差冒充增益；保留103日共同NA口径。
第二轮作者设计审核：补齐一步消费延迟、原模型完整carrier、parent权威闭合、各成本初始建仓/缺口后未知换手、资本耗尽和零fit≠零predict，禁止研究记录环境变量和生产状态假升级。两轮无剩余设计阻断；真实源码和研究完成后更新此处状态。

第三轮作者源码审核：补充child的可用性/原因/volatility与原features逐行闭合、parent纯算术readback、outcome重复行业日期拒绝，以及原20维输入止于2026-08-28的构造测试。最终新研究矩阵31 passed；受影响原helper矩阵52 passed；changed-file Ruff、py_compile、diff check、ownership（7/7，零歧义）、validation_module_registry_l0（8 passed）、L0（零阻断）及F2通过。独立base fresh-process import指向本任务工作树，未访问数据库、fit或服务。此处仅记录源码审查结果，不代表正式双process研究或运行态验收。
