# 因子研究 P2：质量诊断与可恢复处置建议

日期：2026-09-09；版本：1.0；F1 单模块设计。依据[蓝图](factor_research_evolution_blueprint_20260908.md) §5–7 与[P1](factor_research_p1_detailed_design_20260908.md)。用户授权设计、实现、DEV、多轮审核修复、提交合入和本任务清理；生产操作和后端重启另批准。

## 1. 背景、交付目标与边界

一次交付批量质量概览、同口径窗口对照、月度描述、异常/冗余建议、依赖与改进关系记录、DEV全库盘点及重点诊断。P1 PR #4448 已合入；两表生产迁移、profile同步及runtime验证仍独立待办，不以此阻断P2开发。不自动删除、禁用、覆盖因子，不修改QE/数仓/评级/官方指标引擎，不新建表/UI/调度器。

全库指本次明确数据库目标的catalog全部行（含禁用、同名不同source），不能用DEV的10只因子冒充生产全部因子。空目录明确empty，不宣称完成生产治理。盘点完整性不等于每项都可给出有效性结论。

## 2. 实施方案：最小实现与输入

新增 `quality.py` 纯诊断与 `quality_repository.py` 只读适配，复用 `ResearchRepository.cursor`。`scripts/factor_research.py quality` 是只读新命令；不改变既有七命令。输入明确 `--target/--env-file/--as-of/--recent-start/--history-start`，可指定factor范围；默认当前目标全部catalog。输出JSON至stdout，compact summary仅输出统计。要保存研究结论，使用既有create/record，不另造写入器或自动建表。DEV操作允许，生产操作本任务不执行。

单个数据库只读事务读取既有摘要：catalog、官方metrics、monthly IC、correlations。按factor_name/id过滤，固定字段避免加载code_text/因子值；不读原始行情，不导出/冻结/全量hash，不新算指标。每条catalog记录都有结果；关联指标使用factor_catalog_id，旧行只有唯一名称时才关联；歧义保留待处理原因。并发读取的一致性仅普通数据库事务语义，不建立冻结数据集。

## 3. 质量与比较合同

官方指标保留实际ID、日期、batch、方向、股票池、引擎、coverage语义和n_trading_days。窗口比较需要同一因子的同批次、snapshot、engine、horizon、universe/rule、index_policy、方向和coverage语义；旧fingerprint只读取现存值参与口径对照，不计算任何新hash。数据窗口使用data_start/data_end，不能用calculated_at替代。缺口逐项报告，不阻断其他因子。

按用户请求的history/recent范围选择完全位于对应范围的已有指标，展示所有可比配对，不使用任意最新一行覆盖其余。重叠窗口只描述差异并标记overlapping，不声称独立验证/统计显著性；方向事先给定，仅展示direction-adjusted变化，不根据负IC翻转。coverage和样本量缺失、非有限值、无有效样本、日期不合法和重复窗口冲突分别报告。没有可比对照不自动重算。

月度表实际仅有factor_name/month_end/snapshot_date与数值、n_days，没有catalog ID、horizon、batch和universe。因此按snapshot分组输出请求范围内逐月摘要、历史/近期有效月份数与按n_days加权IC差异，但必须明确descriptive_only/knowledge_of_basis_missing，不能拼接不同snapshot或冒充严格同口径与标签成熟证明；月份必须完整落在选择窗口内。不同名称来源歧义不归因。

## 4. 冗余、依赖与处置

相关性按真实catalog ID关联，保留method/as_of/window/universe/rule/index_policy，按abs(correlation)展示每因子已有配对。负相关同样属于信息重合线索；不设新的硬阈值，不做传递聚类自动删除，不填0代表独立。表没有有效配对数，统一报告sample_support_unavailable；无记录不伪造低相关。完全相同且非空的原始expression可作为公式相同线索，不做字符串前缀猜测、复杂表达式重写或把相同公式直接认定结果完全相同。

已有 `experiment_id/best_loop_task_run_id/source_task_id` 只是来源引用，不是当前全量消费者依赖。报告这些已知引用和consumer_inventory_unverified。允许用既有record的decision/request/correction保存改进对象、替换关系、owner需求和条件；未知引用绝不自动视为无人使用。生产退出/替换待owner确认及用户授权，P2交付建议与可恢复台账，不代替业务处置。

## 5. 实际运行与恢复

先DEV只读盘点，输出目标、as-of、目录总数、reviewed/pending计数、reason分布、逐因子证据与待办。reviewed仅指成功整理观察，不代表alpha有效；reviewed+pending=目录总数。不存在指标/无有效观察为pending，部分信息缺失可reviewed并附限制。

本轮真实DEV任务用create登记后运行quality一次，result以record入库，并新进程show读回；重点因子分析与P3交接保存decision/request。不能用文件唯一记录研究历史，产物保存在repo外X盘任务目录；已完成结果登记失败重试record不重复计算。不强求发现alpha，不追加全库因子重算。生产数据未授权时，生产盘点单独记未执行。

## 6. 验证方案与精确范围

改动限定两个研究模块、既有CLI、新定向测试、DEV测试文件、nox所属计划和本设计/操作说明。本次无需DDL或新依赖，不修改其他业务源码。ownership沿factor_library.research，普通计划实际收集新测试，DEV计划保留授权保护；不放宽classifier。

测试：非有限数/无样本/错误日期；同名source歧义；同口径与不同口径窗口；重叠与不重叠；部分月份/未来月份排除；相反符号相关；空目录和全库分母；输入置换；依赖未知；CLI无副作用；DEV真实只读查询与结果入库读回。运行Ruff、compile、diffcheck、所属nox、registry L0、L0、F1验收与最终CI。多轮实质审核修复；成功计算不重复，失败节点定向重测。

## 7. P3 handoff

- FR-REQ-01 因子评价owner：候选/已有因子可比增量相关性与有效配对证据；不在研究层重写引擎。
- FR-REQ-02 QE数仓owner：只读实验/因子版本/模型/标签/股票池/时间/成本后结果关联，不触发ETL。
- FR-REQ-03 QE owner：既有趋势实验的召回/失效/退出诊断，不改模型参数或运行训练。
- FR-REQ-04 因子指标owner：月度IC的批次/股票池/期限关联能力，源结果无这些字段时只允许描述，不补造依据。

未交付接口只影响相关结论，不阻断P2独立能力。

## 8. Design Acceptance Index 与设计验收矩阵

F-201 全目标目录完整性；F-202 纯读与既有CLI兼容；F-203 口径和时间比较；F-204 月度限制；F-205 冗余和引用；F-206 持久恢复；F-207 模块边界和handoff；F-208 审核/授权/运行状态。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | backend/services/factor_research/quality.py | test: backend/tests/factor_research/test_quality.py | 已验证 | 无 |
| F-202 | backend/services/factor_research/quality_repository.py；scripts/factor_research.py | test: backend/tests/factor_research/test_quality.py | 已验证 | 无 |
| F-203 | backend/services/factor_research/quality.py | test: backend/tests/factor_research/test_quality.py | 已验证 | 无 |
| F-204 | backend/services/factor_research/quality.py | test: backend/tests/factor_research/test_quality.py | 已验证 | 无 |
| F-205 | backend/services/factor_research/quality.py；§4 | test: backend/tests/factor_research/test_quality.py | 已验证 | 用户批准范围：生产处置另授权 |
| F-206 | 复用repository.create/record/show | test: backend/tests/factor_research/test_repository_dev.py | 已验证 | 用户批准范围：生产表迁移未执行 |
| F-207 | §7 | artifact: docs/architecture/factor_research_p2_detailed_design_20260909.md | 已验证 | 无 |
| F-208 | §6/本节 | artifact: docs/operations/factor_research_quality.md | 已验证 | 用户批准范围：CI/合入/生产运行分别报告 |

## 9. 设计审核与状态

第一轮：发现monthly缺完整basis、相关性缺有效样本数，修订为描述性观察，不能冒充衰变或独立性证明。DEV仅10个catalog对象，禁止宣传生产全库完成。

第二轮：明确同名来源歧义、重复窗口冲突、全部窗口对照而非随意选择最新、未来/部分月份和重叠窗口限制；来源引用不等于实时消费者清单。不设置淘汰线或额外门禁，不自动调用正式writer。

设计逐条符合性：未缩减记录/真实DEV要求；缺失不填默认成功；不改其他模块与正式因子；不新增审批/冻结/资源限制。实现与合入证据需另更新，当前设计不是已上线。production_ddl=noop，production_dml=noop，process_control=false；runtime按最终changed files/catelog推导，backend restart owner=user。

## 10. 风险与 Production gates

DEV只有10个catalog、50条metrics、970条monthly、45对相关性；真实盘点覆盖DEV全部目录，不声称生产全库完成。所有10项都有可展示官方观察，但monthly basis、correlation sample support及当前消费者均未完整验证，不能以reviewed代替有效/无冗余结论。没有新表，没有任何生产访问。

DEV表已存在，9项DEV测试通过；production_ddl_gate=noop，production_dml_gate=noop，dependency_install=noop，client_install=noop，runtime_activation=false，process_control=false。源码runtime仍按最终catalog推导，用户完成必要重启后才验证运行状态。历史目录和因子处置不在本轮自动执行范围。

实现第一轮审核：5项RED测试暴露相关性重复口径未标冲突、孤立metric未报告、异常范围及非整数样本误接受；修复后GREEN。第二轮检查新增DEV测试必须被普通计划收集并安全跳过，修正旧8到9计数，禁止连接机制未改。不存在新增阻断业务的门禁，reason只影响对应诊断。

## 11. 最终本地验收与四项设计符合性

- 普通factor_research_backend：59 passed、9 skipped；DEV：9 passed；Windows fresh-process：2 passed；WSL Python3.10.19 quality help、模块导入和缺失观察语义通过；nox环境合同17 passed；registry8 passed；L0 blocking0；Ruff/compile/diff通过。新增定向35项包含5项RED→GREEN。
- 实際DEV研究task `921f8531-5e64-467d-9dba-e9476c8fe75f` revision4新进程读回，10/10目录、2组重点decision和4项owner需求；没有因子重算。仅DEV目标，未声称生产全库完成。
- 9个精确changed files：classifier passed，未映射代码/未收集测试均0；backend计划factor_research_backend，外部DEV计划factor_research_dev_db，保持runner_enabled=false。runtime_impact=backend，runtime_files为两个新backend模块，target=backend-main，restart owner=user；不手工降级为none。

| 审核条款 | 直接依据 | 结论 |
|---|---|---|
| 完整交付与证据 | 全目标目录包含禁用/缺指标项，真实DEV写入和新进程恢复；生产/owner依赖明确单列 | 本轮诊断能力完成，不冒充全库淘汰/P3或生产启用 |
| 不静默错误 | test_quality覆盖来源歧义、重复口径、非有限、范围与样本、月份、未来范围；既有record重试与恢复不变 | 问题显式reason，记录失败不伪造成功 |
| 不擅改业务语义 | changed files仅研究模块/CLI/测试/文档及nox一项收集；SQL仅SELECT/只读事务，write只复用研究记录 | 无QE/数仓/官方writer或可用位修改 |
| 不私增门禁审批 | 无新硬阈值、自动淘汰、冻结、hash计算、资源限制、发布流程；reason是诊断而非业务阻断 | 符合已批准边界 |

以上为本窗口顺序多轮实质审核，不冒称独立agent审核。最终PR/CI与源码merge在交付回执读回，不能用本地通过替代CI。生产迁移、profile同步和backend重启未执行。
