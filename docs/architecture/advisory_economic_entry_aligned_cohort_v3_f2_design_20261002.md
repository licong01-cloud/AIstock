# Advisory 经济进入价值：一致监督集合双头 v3 F2 设计 v0.2

> 日期2026-10-02；状态DESIGN_REVIEWED_IMPLEMENTATION_PENDING。父设计：[风险口径与每日身份v2](advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md)。这是原权重复用被明确阻断后的新拟合方案，不放宽旧合同，不是exact retry，目前未执行研究训练。

## 1. Background / 已核验事实

离线经济v1源码PR #5224已合入，真实模型TAKE0，不能把7笔UNKNOWN控制收益当模型利润。风险v2设计PR #5233、标签/消费内核源码PR #5242已合入；后者merge=`ea659553937a4f96957693a836364a3333e6efbb`，27直接测试及必需CI通过，main已同步，无数据库/服务操作。

v2 prepare `adventryloss_daadbb8de554e555061df5d8`完整保留7,720候选，新风险标签AVAILABLE7,685/UNKNOWN31/NOT_ENTERED3/CENSORED1，label-end purged349。原train/validation监督eligible变化22/2，因此停止原收益权重复用，planned1/generated0/evaluated0。这个输入阻断不是新模型效果失败，不构成更改800或选择新参数的理由。

## 2. Scope / 切片与边界

本设计交付同一entry-loss目标下的一致监督集合双头、条件节点预测及一次固定历史导航。不承诺正模型，未确认不激活。完整日常D网格消费者/API/UI后续独立验收，不能把本切片当完整产品。只修改Advisory新文件；原v1七核心、v2五核心及旧工件不改。无需DB查询/写入、数据补齐、profile激活、QE训练、安装或进程控制；临时X/持久F，不读sealed。源码交付沿已有授权、多轮审核和CI；重启用户负责。

## 3. Architecture / 新模型身份

唯一必要变化：return均值与entry-loss q90两个头在同一个AVAILABLE且特征完整的监督集合重新拟合。原候选/日期、特征、label-end purge、exit policy和cost不变。UNKNOWN/CENSORED/NOT_ENTERED仍在输出/收据中，仅不用于监督拟合；不从业务股票池删除停牌股票或日期，不填零或SKIP。

新bundle明确`return_training_mode=REFIT_COMMON_ELIGIBLE`、risk metric/schema和两份新权重。原request只作来源/时钟lineage，新training request/输入身份另立并绑定v2 labels与eligible hash，不用旧request身份表示新模型拟合。

## 4. Contracts / 唯一固定候选

- eligible精确为原AVAILABLE、v2 AVAILABLE、九字段有限且未被information_end purge；train/validation预期3,117/1,507，拟合前核验/绑定精确行hash，不按结果挑样本。
- return目标仍原policy/cost下entry相对固定空槽净收益；risk只`entry_net_max_loss_open_close_bps`，不改峰谷字段，不新建第二risk目标。直接消费已发布v2标签，不重写旧标签。
- 一个candidate、两个新头：LightGBM200轮/lr0.05/depth3/leaves7/minleaf30/seed20261002/2线程，q0.9；无test early-stop、参数/阈值/seed搜索。800只作固定保守研究stop参考，期望净收益严格>0，不能自动成为用户资金预算。
- 真实拟合/新模型评价前，plan绑定v1 plan/prepared/trained、v2 plan/prepared/labels、参数/源码/LGBM版本、split/eligible、evaluation算法和shadow simulator hash。v2 generated0仍0，新候选另计1，不是exact retry或独立OOS。
- 来源、label schema、矩阵、输出维度、版本、特征顺序或policy/cost矛盾fail closed；旧工件读回仍可用。

## 5. 预测消费者 / D与T分离

所有v3调用使用唯一条件节点内核：D八特征加显式query_gap，train-only feature bounds及price-bin支持，均值和entry-loss独立输出；支持内、净值>0、risk≤预算才ACCEPTABLE，支持不足为UNKNOWN。未来日常网格复用该内核及v2每日身份/法规价合同，不复制业务判断。

历史导航先在真实T开盘条件查询，仅D特征/D可见T参考、T当前开盘/当时可交易性及CNY limits。未来退出可交易性、成熟状态、收益、后续OHLC均不得进入决策；31个未来label UNKNOWN不能提前用于prediction SKIP。不能用T价倒填D或伪造board/ST/membership。价格条件估值不是改变买价的因果最优解，不复制未来路径制造多买价训练样本，不宣称盘中最佳成交点。训练scope/每日来源身份分别绑定，当前RECOVERED_LIMITED不升级native COMPLETE。

## 6. Evaluation / 三臂与证据

三臂固定为baseline、gap±300bps规则、一个模型；Top5不补第6，UNKNOWN基线控制独立归因。复用同一既有shadow portfolio policy、成本和共同horizon，全原cohort保留。报告组合净收益/MDD、配对日lift/固定block区间、干预日分布、TAKE/SKIP/UNKNOWN、真实TAKE/control episode、错失盈利/避免亏损和支持段。

未来退出端点不可证明时保留限制；名义shadow结算不等于真实可成交利润，不能靠改其它模块模拟器制造一致性，不能事后删除失败日期。v1和新比较的执行约束不同，不声称逐样本等价。零TAKE或负增量只交付真实结论并终止本candidate，不扫描预算/loss/阈值，不关闭整个价格建议业务。episode描述不相加为组合收益；风险coverage不是胜率，收益误差分位数不是均值置信界。

配对日lift使用共同估值期全日序列，固定moving-block bootstrap block=5/reps=2000/seed=20261002，报告双侧95%描述性区间；不根据结果改变block长度或只取干预赢家日。现金benchmark=0，未提供同口径指数时明确不是指数超额。执行性UNKNOWN保留在名义组合及独立限制清单，不用移除未知episode来制造“已证明可成交的组合收益”；证据仍NAVIGATION_ONLY，不做激活判断。

## 7. Implementation Plan / 精确范围

新增`backend/services/advisory_model_first/economic_entry_aligned_contracts.py`、`economic_entry_aligned_training.py`、`economic_entry_aligned_inference.py`、`economic_entry_aligned_pipeline.py`、`economic_entry_aligned_evaluation.py`及对应叶模块最小测试；本设计、父v2设计、主蓝图。旧经济核心不改。

先多轮设计审核/合入→独立双头与唯一节点内核、定向测试→输入/eligible准备及新plan登记→一次真实拟合和固定导航→多轮源码审核/精确测试/CI/交付。API/UI精确范围后续登记，无相关model时不交付占位BUY。

## 8. Verification Plan

核验同一eligible/purge、原return目标不变、source/label/mask/hash矛盾、UNKNOWN不填值/删候选、future test毒化不改两头、单条/批量条件等价、支持域/空集/非连续集合、非法模型输出，以及真实TAKE与control归因。最小fixture、无重复快照，合成拟合不是研究证据；本地仅精确测试/lint/L0，宽回归交CI。F2 validator及DESIGN-COMPLIANCE-001通过才交付。

## 9. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-560 | 原复用阻断保留，新拟合不称exact retry |
| F-561 | 双头同eligible，原return/候选/政策/成本不改 |
| F-562 | 一个candidate、固定预算、源/model身份预登记 |
| F-563 | 唯一条件节点、D/T/PIT、支持域 |
| F-564 | 同shadow三臂、UNKNOWN归因、执行限制 |
| F-565 | 旧源码/工件不漂移，Advisory及资源/操作边界 |
| F-566 | 设计/源码/模型/APIUI/激活分开验收 |

## 10. Design Acceptance Matrix

仅设计验收，源码/真实模型尚未实施。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-560 | §1、§3～§4 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-561 | §3～§4 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-562 | §4 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-563 | §5、§8 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-564 | §6 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-565 | §2、§7 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-566 | §7～§8 | artifact: docs/architecture/advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md | DESIGN_VERIFIED | none |

## 11. Risks

可用集合略变不自动创造alpha。弱信息、gap稀疏、历史非原vintage和股票池身份缺口仍存在，历史盈利仍不能激活。动态资金仓位/分钟执行不在权限，不为归档/基础平台膨胀新增主线。

两轮本窗口设计审核完成：第一轮核验监督mask修正不是风险阈值/目标搜索、标签与预测的未来clock分离；第二轮补全固定统计规则、同shadow名义利润与可执行性证明的边界、旧reader源码不漂移、API/UI未交付状态。DESIGN-COMPLIANCE-001四项：不把离线切片当完整业务；未知/缺口显式；旧成本/政策/候选/结果不改；不引入审批平台或跨模块修复。validator仅结构证据。

## 12. Production Gates / Rollout / Rollback

design_merge=pending；source_implemented=false；model_trained=false；daily_api_ui_delivered=false；model_confirmed=false；binding_active=false；DB/profile/service操作noop；sealed未读；backend_restart_owner=user。

先设计后独立源码和唯一导航候选，失败保留新收据，不改旧数据/模型/合同。typed unknown仅影响新经济角色，不重排或重新运行Selection。后续角色版本独立回滚，不覆盖旧ENTRY_PRICE/M4；本次无生产操作，不称运行时激活完成。
