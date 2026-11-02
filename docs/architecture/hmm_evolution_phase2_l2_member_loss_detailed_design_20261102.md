# 申万L2成员损失：单候选与配对控制详细设计

> v1.0；2026-11-02；F2设计文档；父蓝图v3.00 §1.26/§7。状态：目标可行性分析及设计完成，源码/正式训练尚未执行。本次用户要求开始上一轮的目标对照与详细设计任务，未将完成文档等同于已启动8-fit研究。
> 2026-11-02接续授权：用户要求“执行文档的提交合入，开始下一步的实施”，本设计进入源码实施；实际源码、fit消耗、结果与生产状态分别记录，不将授权或文档合入记成训练完成。

## 1. Background / 目标和依据

终极目标是申万L2信号为QE及后续荐股/模拟盘带来可检验的收益增量或可接受收益代价下的损失降低。原自然新买消费的6.96bp增量不足以推广；原loss在实际新买股票上的日均亏损IC仅0.0382。不能继续靠调κ、改禁买阈值或把持有期中位数直接当新horizon。

[零fit可行性报告](../analysis/hmm_l2_member_loss_target_feasibility_20261102.md)确认：同成员/权重的组合损失B与成员损失C并不等价，96,972个可比键的平均抵消量0.804122个百分点；真实新买队列中C与个股亏损的实现关联高于A/B。这是新的可检验假设，尚不是预测或经济优势。

## 2. Scope / Non-goals

一个成员损失Ridge候选C和一个配对组合损失Ridge控制B，原20D、季度训练窗口、10日horizon和Ridge参数固定。旧A模型/预测/M0/M1不覆盖、不重训。研究不修改个股Alpha、股票池、QE策略、执行算法或其他模块。

不增加L1/大盘模型、horizon搜索、阈值/seed搜索、校准器或新消费公式；不把新值放进probability或旧preset_A。自然停牌、合法停发、单成员行业不构成全模型失败；训练标签不可用与预测输入不可用分开。只读冻结文件，不用数据库填补文件；本包不申请生产DDL/DML、API发布或日更激活。

## 3. Architecture / 复用和允许修改

沿现有正式HMM frozen reader取得共享release、PIT、security、calendar及报价身份；复用整数厘/复权前缀解码和严格前置市值事实能力。按as-of冻结成员及权重，构造配对B/C标签；同一份20D、相同共同样本和scaler输入，各拟合B/C；封存模型后读评价事实，交付完整L2预测及同口径比较。

拟实施文件限定为`backend/services/hmm_risk/risk_l2_member_loss.py`、`scripts/hmm_risk/run_risk_l2_member_loss.py`、`backend/tests/hmm_risk/test_risk_l2_member_loss.py`；必要的现有HMM共享输入函数扩展先列精确路径，不复制reader实现。原日期文件/数据库20D生成能力保持；研究采用已有文件输入及历史快照，不将历史验证冒充当前在线消费。

## 4. Contracts D1 / L2-MEMBER-LOSS-D1：唯一目标及配对控制

决策日`t`，`a=prev_open(t)`；`M[t,s]`是在a处于共享PIT股票池、正式归属L2行业s且a未停牌的股票集合。不可根据t之后是否停牌、收益或QE入选结果修改M。取截至a最新真实流通市值`v[a,i]>0`，沿原正式市值来源/无效事件语义，`w[t,i]=v[a,i]/sum(M的v)`，十日期间不再平衡。

原生价格P采用C-010现有严格解码，唯一整数厘不能恢复时显式失败。`R[t,i]=P[t+10,i]/P[t,i]−1`，交易日偏移来自同一冻结calendar；不把特征as-of、执行日和标签锚点再次滞后。R是原生价格参考收益，不宣称分红或交易费用后的持仓回报。

- 配对控制：`B[t,s]=max(0,−sum_i w[t,i]*R[t,i])`。
- 唯一候选标签：`C[t,s]=sum_i w[t,i]*max(0,−R[t,i])`。
- 诊断：`U[t,s]=sum_i w[t,i]*max(0,R[t,i])`；逐行验证`U−C=sum(wR)`与`C−B=min(C,U)`。

旧A为原逐日C-010行业收益复合路径，只读保留其冻结模型和结果。A与B有成员/再平衡/旧价格精度差异，不能把C与旧A的差异全部归因为取负部顺序；B与C才是本研究的严格配对控制。复权口径仅报告敏感性，不训练第二候选或按本轮相关大小切换正式目标。

## 5. Contracts D2 / L2-MEMBER-LOSS-D2：输入、窗口和合法NA

显式冻结release沿原季度包：manifest canonical=`590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394`，manifest文件SHA=`321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`；generation=`20260930-monthly-v2-unified-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1`，cutoff=2026-09-30。不解析别的active版本。

原准备包`F:/Dev/AIstock_runtime/hmm_l2_risk_rolling/20261026/repaired_20261028/prepare_v2`及其中feature canonical=`94be96e74fb906098b6c9691fccc33895989586330af94b2243af046baf96b62`保持只读。C-013/security/provider/calendar沿原request正式pins；不新建行业编号、映射或数据候选。完整131行业文本目录保留。

| vintage | 训练事实窗口 | 标签必须成熟至 | 预测范围 |
|---|---|---|---|
| 2026Q2 | 2023-04-01..2026-03-31 | 2026-03-31 | 2026-04-01..06-30 |
| 2026Q3 | 2023-07-01..2026-06-30 | 2026-06-30 | 2026-07-01..09-30 |

共同训练mask为原该vintage成熟/有效标签键、原20D有限特征键及B/C标签有效键的交集；B/C严格同mask，不根据任一拟合结果筛样本。零fit读回参考为Q2 88,930行/714日、Q3 89,570行/715日；这是当前输入事实，不是新增最低样本量门禁。

在任何标签价格读取之前，先按冻结日历限定`pos(t)+10<=pos(train_end)`；不得先读取越过训练截止的outcome再过滤。Q2与Q3分别截断，不因Q3后来已知而补写Q2的未成熟标签。

无成员、合法停牌端点NA、未成熟等分别记录。固定成员任一个必要端点不可定价时，整个对应B/C标签为合法NA；不补零、前填、伪造停牌报价或移除该股票后重归一化。保持成员、缺失股票及权重损失范围，披露完整案例分析的选择偏差。合法NA不阻断其他行业/日期的训练和预测，不要求每个行业历史上出现全部状态。未知缺失、重复/冲突身份、非有限必要事实与hash漂移仍fail closed，并具体定位键；不能泛化为所有缺失都合法。

预测保持2026-04-01..09-30完整125日×131=16,375行/模型，保留availability/reason。标签评价止于2026-09-30内已成熟的115日，其余10日1,310个标签保持未成熟；不为补齐标签读取10月outcome。`historical_as_known=false`、`untouched_holdout=false`，这是已消费开发历史，不能恢复未使用测试资格。

## 6. Contracts D3 / L2-MEMBER-LOSS-D3：算法和8-fit预算

20D顺序、原train-only标准化、常数列处理及日期等权沿既有loss设计：总N行、D日期、当日n_d行的权重`N/(D*n_d)`；scaler仅在该vintage共同训练mask上计算未加权均值/std(ddof=0)，B/C共享同一scaler。截距不惩罚。

唯一估计器`Ridge(alpha=1.0, fit_intercept=True, copy_X=True, max_iter=None, tol=1e-4, solver='svd', positive=False, random_state=42)`。保留raw输出，正式损失用`clip(raw,0,1)`物理投影；不以clip吞掉NaN/inf。B/C参数完全相同，不搜索更合适的alpha或窗口。

沿旧loss正式acceptance的现存Conda base数值环境：Python 3.13.5、NumPy 2.3.3、SciPy 1.16.3、scikit-learn 1.8.0、threadpoolctl 3.6.0，BLAS/OpenMP线程数1。启动前只读核对，不安装/升级依赖或改AIstock服务环境；训练子进程的线程条件只用于数值复现，结果和模型身份仍保存在文件，不绑定后端环境变量或触发重启。

每process拟合2个vintage×2个目标=4次，双fresh-process总上限**8 fits**。C是唯一新候选；B是相同样本/模型/窗口的控制，并非恢复退役训练链。旧A/M0/M1零fit。预算比旧单目标4-fit多4次，原因是避免把共同样本减少的效应误当标签改善；本设计不假称已经获旧预算覆盖。实际消耗从每次fit开始累计，失败不自动重试、不替换候选，最多8次。

## 7. Contracts D4 / L2-MEMBER-LOSS-D4：模型、资产和消费者隔离

新标识为`hmm_risk_l2_member_terminal_loss_ridge_v1`，控制为`hmm_risk_l2_fixed_cohort_portfolio_loss_ridge_v1`；独立schema输出`expected_member_loss`或`expected_fixed_cohort_portfolio_loss`、raw值、trade/as_of、availability/reason及vintage/input/model身份，不输出假概率、warning或假中性值。

载体包含目标公式/价格口径、成员/权重来源、共同mask、特征/标签/训练期/scaler/参数hash、源码和数值环境；复用现有receipt，不建新的registry/数据库表。两process比较封闭参数与输出，时间戳不混进数值identity。旧loss/M0/M1和QE已接资产的字节/hash保持。

推断核只接已验证的20D/scaler/参数，保持既有文件与数据库来源分离。用现有真实历史快照在2026-04-01/07-01各131行验证两入口同核行为，不查库填文件，也不把历史快照叫实时验收。没有新API/UI、环境变量、日更调度、服务重启或生产采用。该vintage不适用于11月在线荐股/模拟盘。

## 8. Contracts D5 / L2-MEMBER-LOSS-D5：以股票风险识别决定是否继续

先封存两process的B/C完整预测，再评价；原A只读。主研发比较在已存在的B0自然新买去重队列进行，沿已冻结股票→L2映射，将行业预测映射至实际买入股票，以原QE同release的复权close十日负收益为共同参考，不含实际成交费用。不得用未来股票亏损挑选行业、日期、股票或新消费阈值。

主指标为共同可定义日期上`Delta_IC=mean_d(Spearman(C预测,个股10日亏损)−Spearman(B预测,同一批个股10日亏损))`；同一日期两臂必须同股票。少于两股、预测常数或实际损失常数时保留undefined并列出原因，不能填0/0.5。跨seed去重的日期—股票是主视图；各seed为敏感性，不当作独立市场。

研发取舍建议为`Delta_IC>0`才继续讨论单次QE消费验证；否则结束本候选。它不是生产推广阈值或显著性要求，不新增每季度/每行业全部为正的AND门。报告原A同样本表现；即使C优于配对B，若未优于现有A，必须明确没有交付更好的现成版本，不能建议替换。

同时报告：各季度及日期覆盖；B/C各自目标的日期等权MSE/MAE及训练均值参照；全部合法行和共同交集；旧A同股票IC；高/低预测风险对应的未来亏损和正收益；前轮实际持仓损失关系及右截尾。MSE改善不能替代股票风险识别，风险与收益兼有的股票不能直接变成禁买依据。不计算一个事后“完美策略收益”，不拟合第二上行模型，不重新提交QE账户。

有限正增量、负结果、不可定义及证据不足均完整交付。无论是否继续，已消费历史和回溯分类限制保留；不能宣称收益已证实或forward-confirmed。真正账户效果由QE窗口后续按其消费合同执行。

## 9. Contracts D6 / L2-MEMBER-LOSS-D6：连续交付和终止

本次包已完成零fit目标对照与设计审修，停在可实施设计。下一实施包应连续完成HMM源码/直接测试→多轮审修及必要门禁→按授权交付源码→干净固定commit上的正式8-fit→完整预测/有界双入口→结果分析与单一后续结论，不将adapter、测试、预检拆成小研发阶段。

遇未知输入缺失/身份冲突在fit前停止并给精确键；正式fit失败即停止，保留真实消耗；达到8-fit上限不重试；效果无新增价值则结束，不自动换horizon、参数、模型或消费公式。合法市场缺失按D2逐标签处理，不作为全模型拒收理由。其他业务模块源码仍由对应owner修改。

## 10. Implementation Plan / Verification Plan

正式代码复用现有reader，scratch分析器不能直接复制成生产路径。先验证同成员/同mask标签及成熟边界，再实现一个配对执行器和独立预测schema。测试仅覆盖真实保护：PIT前置、来源alias/市值无效事件、整数厘/前缀编码、单成员恒等、停牌合法NA与未知缺失区别、完整131、train-only scaler/样本权重、8-fit计数/无重试、旧schema拒绝误用及原资产不改。

两轮作者审核分别复核数学/因果边界及接口/预算/失败路径。局部使用新增直接测试、Ruff/compile、scope/ownership、HMM最小相关切片及L0；宽矩阵交CI/Nightly，不修改CI、noxfile或test plan。正式研究另用两个fresh-process，不把单元测试fit计为正式研究，也不能把正式失败伪装为测试重试。

### Design Acceptance Index

F-011：目标、样本、预算及风险识别评价（D1/D2/D3/D5）；F-012：身份、合法NA、双入口及新旧隔离（D2/D4）；F-013：完整交付、owner边界与停止（D6）。本索引此时验收的是设计完整性，源码/实验另行记录，不以文档PASS冒充模型成功。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | 本设计§4～§6/§8；成员目标分析§1～§3 | artifact: docs/analysis/hmm_l2_member_loss_target_feasibility_20261102.md；artifact: tmp/handoff/hmm-l2-member-loss-20261102/comparison_v2.json | VERIFIED | 无 |
| F-012 | 本设计§3/§5/§7/§10 | artifact: tmp/handoff/hmm-l2-member-loss-20261102/review_v3.json；四个原生读回样本、全96,972键恒等式及完整NA计数 | VERIFIED | 无 |
| F-013 | 本设计§8～§11；父蓝图§1.26/§7 | artifact: docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md；artifact: docs/analysis/hmm_l2_member_loss_target_feasibility_20261102.md | VERIFIED | 无 |

## 11. Risks / Rollout / Rollback / Production Gates

C可能只是放大波动、无法由现有20D预测；行业级信号仍不能区分同业股票。合法停牌标签NA会产生完整案例偏差；模型不因此被宣告失败，但效果必须披露这种偏差。原生价格未包含分红费用，实际股票参考采用复权close，二者语义不同，不把代理损失当账户净损失。当前开发历史已经消费，不能用重复对照制造未使用holdout。

若候选无价值，保持原资产/接口并交负结果，不增加新的候选。发布/回滚以是否显式使用新schema控制，不删除旧成果。`production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_gates=noop`、`dataset/profile_write=false`、`runtime_activation=false`、`backend_restart_required=false`（本次文档交付）。后续源码runtime分类按实际changed-files重新计算，不继承文档的none。

## 12. 设计审修与DESIGN-COMPLIANCE-001

第一轮：核对真实分析的成员/权重、完整NA和样本量；明确B/C须同mask拟合，将原拟议单目标4-fit改为带配对控制的8-fit，避免把样本改变归因于标签。第二轮：纠正价格口径和共同可定义日期，限定主评价为实际候选股票亏损辨别；保留旧A、上行机会、已消费历史和合法停牌边界。不是独立第三方审核。

逐项符合性：完整交付指定目标对照和精确设计，未报源码/模型完成；所有NA/undefined/负面结果保留，无默认安全值；新目标/控制/预算明确列出，不改旧批准合同；只提出本研究的一个取舍规则，不加生产审批、额外显著性/覆盖门槛或全行业合取要求。设计F2验收与最终diff检查结果在交付时记录。

本轮验证：`aistock_feature_workflow.py validate --design <本文件> --tier F2 --format summary` PASS（3项/3行，warnings=0）；三份文档`git diff --check`通过。只变更本设计、目标对照报告和父蓝图；业务源码/测试/数据/profile/服务零修改，未创建PR或执行合入。
