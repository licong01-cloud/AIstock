# 因子研究比较能力：集中增补详细设计

日期：2026-09-11；版本：1.2；F1 单研究模块设计与历史实现回执。依据[蓝图 v1.2](factor_research_evolution_blueprint_20260908.md)和[方法论 v2.1](../analysis/factor_research_methodology.md)。v1.0 完成设计，v1.1 实现比较能力（§12 保留）；v1.2 仅修订完整评价设计与 Skill，新增代码和数据评价尚待实施。以下既有接口保持兼容；后续范围按 §13，不执行正式因子晋升、QE 实验、生产数据库操作或进程控制。

## 1. 背景、目标、非目标与边界

一次集中落实“看得到存量证据 → 技术预检 → 完整独立指标与相关性、近期优先 → 角色匹配的时间外增量 → 有依据的下一步”。不新增阶段、平台、表、task_type、record_type、writer、官方评价引擎、资源限制或生产门禁。研究角色用 context_json，研究结果仍由既有 records 保存。未知和不可计算只是对应诊断状态，不阻断其他研究或业务；不能把缺少完整评价误报成整体无效。

本文前半部保留验收设计，§12 单独记录 v1.1 实现，不把设计状态追溯改写为已实现。[P1](factor_research_p1_detailed_design_20260908.md)、[P2](factor_research_p2_detailed_design_20260909.md)及[质量操作记录](../operations/factor_research_quality.md)的原始测试、DEV 任务/revision、PR 与当时生产状态不修改、不重算；当前运行事实另由实时回执核实。尤其月度 h20 空值原因不能由日期猜测。

| 已有位置 | 已核实能力 | 增补边界 |
|---|---|---|
| service.py: context | 分页返回 catalog/metrics/correlations，日期过滤含 calculated_at | 兼容追加表达式与依赖/近邻线索；不要把 calculated_at 当历史可见时点 |
| quality_repository.py | 已 SELECT expression；P2 是摘要诊断 | 参考字段映射，不改变 quality 默认输出或让其读取全库因子值 |
| runner.py: execute/evaluation_context | 先登记 attempt，再子进程计算；prepare 传日期与 instrument_hint，按组复用一次；官方基础指标按原口径输出 | 增加显式 opt-in comparison，请求缺失时路径与结果语义保持兼容；不是修复“强制全历史” |
| models.py/repository.py/service.py | 两表、context_json、六种 records、幂等/并发/attach、有限 JSON | 原 writer/恢复路径复用，不引入另一套研究账本 |
| qe_eval_v2_metric_engine.py | HOLDING_PERIODS 查表、共享价格/PIT/停牌和标签 | 只读复用，不修改官方语义；新增研究统计单独命名，不写官方 metrics |
| correlation_engine.py | compute_incremental 仍转 compute_full_matrix(existing+new) | 首版局部比较不调用该接口；正式子矩阵需求交 FR-REQ-01 owner |

## 2. 实施方案与允许的文件范围

未来代码范围拟限 `backend/services/factor_research/{service,models,runner}.py`，以及该目录下新增纯比较模块 `comparison.py` 和只读适配 `comparison_context.py`；必要的 `scripts/factor_research.py` 可选参数与 `backend/tests/factor_research/` 定向测试。仅按 actual changed files → `file_ownership.yaml` → `module_registry.yaml` → `test_plans.yaml` 确认所需计划，必要 nox 收集变更必须明确列入实现 scope。

不得修改 QE/qrun/Qlib/数仓/评级/官方 writer/生产 catalog/数据集/PIT/交易语义；不安装依赖。使用已存在的 NumPy/Pandas 线性代数与日期能力，不因参考 Pingouin 文档安装 Pingouin。v1.2 同步 Codex/Claude 两个因子入口引用唯一方法正文，代码范围待 §13 owner 交付。若实现必须改共享 owner 文件，提交具体需求，不能偷改后声称仍为本模块。

## 3. 输入与上下文接口

### 3.1 研究卡与不可混用的计数

在 tasks.context_json 保存 `method_version`、`research_role`（predictive_increment/replacement/conditional）、`hypothesis_family`、机制/用途/主要期限、`direction_source`（declared/fitted）、方向确定时点、数据字段/单位/可得时点、B/F/替换/消融引用、探索/拟合/评价安排、反证与扩样计划。沿用 task_type，不追溯补造旧字段；旧研究缺失标 unknown，旧入口继续可用。

工程 attempt、实际试过的公式/参数/方向和查看过的评价范围分别放在 attempt/decision 摘要中。失败重试不加科学检验计数，一次 attempt 的多个选择不能只计一次。结论更正使用 correction，计算完成而入库失败继续 attach，不重跑。

### 3.2 context 兼容追加

保留当前字段与分页，按本任务明确 factor/catalog ID 范围返回 expression、输入依赖列表、依赖确认方式与 unknown、候选近邻及检索依据。来源标签不等于代码实际输入；只读解析显式字段访问或已核实说明，遇动态代码记录 unknown，不执行 catalog 任意代码来“探测依赖”。重名以 id/source 区分，表达式相同只是线索，代码/输入未核实不判构造重复。

近邻来自声明范围的表达式/输入字段集合/窗口族/机制及拟合期抽样相关；实际执行范围与检索范围均回报。不是建立全仓静态分析器，也不默认加载所有 code_text 或全部因子值。可先使用调用者审核选定的候选列表；分页遗漏/元数据未知不被描述为全库去重。已有指标引用实际时间和 basis，不能按 calculated_at 构造干净 OOS。

### 3.3 run 可选 comparison 请求

现有 run 请求兼容增加一个 `comparison` 对象；无该对象时不运行新诊断、不改变既有基础指标。v1 精确计算合同包含 `research_role/horizon/baseline/candidate`、可选 `replacement_target/state`、`controls`、`value_artifacts`、`fit_windows/evaluation_windows`、全局 `knowledge_cutoff`、`direction`、`hac_maxlags`、可选 `construction/cost`。研究用途、输入选择依据、近邻检索过程、样本计划等继续留在已有 task `context_json`，不在运行请求复制第二份研究卡。

每个 `fit_windows[]` 明确 `{start,end,knowledge_cutoff:{date,phase}}`；每个 `evaluation_windows[]` 用 `fit_window_index` 精确绑定一次既有拟合，拟合知识截止严格早于对应评价窗口。全局和拟合 cutoff 的 phase 只允许 `pre_open/post_close`。首版 combiner 固定为分别拟合的横截面 rank OLS 和日期等权，不开放评价期模型搜索。所有信号名必须精确关联本次已审核候选或 repo 外稳定值产物；未引用产物、目录内产物、符号链接、重名控制角色和未绑定信号均拒绝，不增加任意代码自动执行路径。

范围仍用现有 instruments/read_start/read_end/signal_start/signal_end/cutoff；技术预检可小样本，完整评价请求须展开完整目标历史 PIT 股票池并覆盖全有效历史/最新日期，不只更换方法版本。多窗口保留边界，暖启动与标签读取范围不等于评价范围。执行依赖数组只保留本组需要的列，旧候选产物可按范围读回，不长期保留全市场大缓存，不添加 OS 以上的资源门禁。

## 4. 数值与时点规格

以下只说明实现，不复制第二份统计规范；语义以方法论 C-1～C-7 为准。

### 4.1 股票、日期、缺失与标签

使用现有 canonical v2/PIT/停牌上下文，指定样本与当日 eligible mask 相交。选样按历史可用信息，不能用当前存活或全期数据完整性过滤。排名在声明的当日截面上取 average ranks，常量/空列不可伪造排序信号。共同有效样本单独用于可比统计，但理论 PIT 分母、原始有效样本、共同样本和丢失原因均保留；不以 dropna 后行数替换覆盖分母。

复用现有标签矩阵与 HOLDING_PERIODS，成熟断言直接查入场/出场所需日历位置及可获得时点，不从 20d 名称算偏移。现有 h20 对应 T+1/T+21。完整交易日历上的日期到达与价格有效是两项观察，缺价格不当未成熟，未成熟不补零。fit 窗截止必须包含时点（或明确收盘数据已可用），盘前不得用当日收盘；训练样本剔除尚未成熟或与评价边界不符的标签。

测试日历可使用小型明确交易日序列，其中人为缺失日标明为非交易日；真实代码用当前 provider 的日历，不另抄节假日表。预测只能读取截至对应信号时点的输入；评价侧收益排名/残差不得进入预测矩阵。

### 4.2 原始/控制/近邻三个视角

先做原始 RankIC，再按声明 Z_style（规模、20 日反转、换手、波动、行业中本任务可得且适用的项），以及 Z_style 加声明近邻计算偏 RankIC。行业配置/风格择时不默认消除其目标暴露；控制项未绑定或缺失，明确该视角不可计算，不悄悄删除控制项然后沿用完整口径名称。

两次残差化使用同一个共同掩码、相同 Z、截距与 average ranks；行业哑变量留一个参照类，不对类别编码排名。NumPy 最小二乘给出矩阵秩、观测数、残差变异；秩不足/零残差/自由度不足返回对应诊断而非有限值 0。数值容差依据浮点精度和矩阵尺度，写入报告并以退化/近退化构造测试，不作为 alpha 阈值。

R²_span 仅在拟合样本报告被基准解释程度及其样本/自由度，不据高值自动判无信息。已知构造关系来自已审核代码及可得输入，统计关系不提升为构造事实。合成信号相似性按声明计算，不能替代实际增量。只算本次指定因子对的逐日 Spearman 及有效日期/股票数，不调用 compute_incremental 生成额外全矩阵，不写官方 correlations。

### 4.3 B 与 B+F 的简单时间外比较

首版主路径为预先声明的排名输入线性最小二乘（不搜索模型/正则超参数）。每个信号日先仅以当时可得的输入和 PIT 构造两方案共同输入截面，average rank 变换为 rank/(当日输入股票数+1)-0.5；不根据未来标签是否存在决定输入排名。fit 再选择截止时点已成熟且标签有效的共同训练行，标签在该训练截面同式排名居中；同日每股权重 1/n_t（n_t 为有效训练行数），使训练日期等权，B 与 B+F 分别拟合含截距的系数。两边同期限/训练样本/归一化/目标/选择规则，不能为 F 选择更容易的股票或更有利窗口。

评价日仅对当日可得输入进行同规则变换，用先前 fit 系数预测，不用当日未来收益重拟合；预测完成后才用标签有效掩码计算 IC 配对差，未来标签缺失不得改变之前的预测值或输入排名。滚动方案明确 fit/eval 日历映射和系数适用区间，单一静态方案只拟合一次。方向 declared 按事先方案应用；fitted 只在 fit 内确定并记录，不修改官方评级方向。

条件角色仅加入事先声明且时点合法的交互列（例如连续状态乘候选）；单调研究失败后不能自动试遍交互。替换角色用相同 B 其余部分和相同程序替换指定分量。等权或其他表示方式可作为另行声明的研究尝试，不在最终评价中择优报告。完全重复列即使在其他正则实现中改变预测，也不能归因于新增来源信息。

成本/状态决策指标不统一成 delta IC：状态预测可声明固定损失的日期配对差；若要求实际策略决策结果而无现成可比结果，保留 owner 待交付，不在此写第二个策略回测器。每个报告区分研究排序增量、使用方式改善与真实策略尚未验证。

### 4.4 不确定性与成本

在连续有效日期段对声明的 delta 序列使用 Bartlett 权重 HAC 均值方差，带宽 `hac_maxlags` 在评价前由研究计划声明并报告依据；不通过挑 lag 达到显著，不将标签重叠当唯一依据。输出 mean_delta、HAC standard error/t、有效日期、带宽及限制。日期有缺口按连续段分别报告，不拼接或把缺日填零；样本或方差不足时统计量为 null+reason。保留双边原始量，不保证配对一定提高检验力，不报告未成立的多重性控制。

成本只对明确已有的、同币种同资金归一化的持仓/成交权重路径计算。买入/卖出分别乘声明费率，净收益由同频毛收益扣相应费用；换手定义、初始建仓/末端持仓处理、持有/重叠批次、风险/参与程度均列明。研究只读消费路径，不生产虚构成交；无路径则 cost_status=unavailable，仍可给预测结论。非线性冲击/容量没有模型时不声称已覆盖。

对线性等效成本情景可报告盈亏平衡 bp：以买卖共同的每成交权重可变费率为未知数，使用累计算术毛收益扣除已声明固定费用，再除以总成交权重并换算 bp，注明非复利口径；不同时解两个未知买卖费率。成交权重为零或毛口径不匹配时不可计算，负值如实保留，不制造无限容量/无成本结论。低换手不自动意味着净效果更好，比较必须包含参与度和覆盖。

## 5. 输出、持久化与失败恢复

沿用 runner 的 task/attempt 产物目录和既有 JSON 写入/attach 流程，新增 `research_comparison` 结果块，不覆盖原 metrics。计算列文件复用已有候选 H5；报告只保存窗口级摘要、所需逐日差值及有限诊断，不重复保存全市场成功日志或原始数据。

结果至少包含方法/角色/基准与控制集引用、方向来源和锁定时点、实际样本/日期/成熟与缺失计数、原始/偏 RankIC、局部相关支持、各方案与配对差、HAC 配置与限制、成本可用性、信息关系与使用价值两轴、下一步。未知用 null+reason，JSON 不写 NaN；reason 是观察，不创建新的任务状态或审批。

计算请求的 comparison 身份、候选顺序及引用应纳入现有 attach 一致性校验；旧结果无该块仍按旧合同恢复，不自动补算。科学结论和执行状态分开：computed 可以伴随“证据不足”。部分诊断不可算时保留其余结果与原因；执行/持久化失败继续原失败语义，不能“比较不可用”吞掉真实异常。

记录成本区分 observed（实测）、preplanned（运行前原方案）、estimated（注明估算基础）、unknown；它与交易 cost_scenarios 是两个概念。不把计划未执行的全库运行成本写成实际节省。

## 6. 原首轮研究与后续退出

原三个问题及 v1.1 回执保留为历史：既有中长期候选复核、近重复候选替换优势、一项两融机制可行性。后续按方法论2.1先修复技术缺陷，再对技术有效候选完成全目标 PIT 股票池/全有效历史独立指标与相关性，优先分析2024年至最新日期，不再因小样本弱信号决定不扩样。两融时点未确认交数据 owner，只作可行性结论，不伪造或开启新数据同步。

每题记录预检问题、完整评价完成/缺失范围、实际成本、信息/使用价值、下一步。真实结论可以不支持或不确定；技术预检/局部比较不等于整体评价完成。没有诊断时会怎样做只能引用事前记录，不凭事后叙事估出方法收益。研究结束、工具可用、方法有效、生产可用是四种不同状态，原前三题至多给方法改进线索。

## 7. Owner handoff

- FR-REQ-01 因子评价/相关性 owner：提供指定成对子矩阵路径与有效配对/可比范围，不只是路由到当前 compute_incremental。首版局部抽样不依赖此交付。
- FR-REQ-02 QE 数仓 owner：只读实验与因子/模型/标签/股票池/时间/成本结果关联，不触发 ETL。
- FR-REQ-03 QE owner：既有趋势实验召回、失效、退出与可比策略对照；本窗口不调模型/seed、不启动训练。
- FR-REQ-04 指标 owner：月度批次/期限/股票池关联能力；不可获得则继续 descriptive_only，不猜测补齐。

ID/owner 保持 P2 原交接，不改历史 request；后续新 request/correction 链接原记录。评级方向来源标注或 catalog 元数据回填等需求只提交明确 owner，生产写入须单独授权。缺少跨模块接口只限制对应结论。

## 8. 验证方案与测试验收

| 测试 | 必须发现的错误 / 正确观察 |
|---|---|
| 标签边界 | 查表覆盖 1d/5d/10d/20d；刚好成熟/少一天/跨非交易日/盘前不可用/价格缺失；将 h20 错回退20而非21应 RED |
| 偏 RankIC | 两侧残差化、共同掩码、类别哑变量；只残差化一侧或再次 rank 残差应在已知数值用例 RED |
| 完全重复/固定单调变换 | Z 含相应基准时零残差统计量不可定义；不能报0或把已知变换叫新来源信息；严格递減方向正确 |
| 高张成残差有用 | F=X+0.01S 的已知构造不能被高 R² 自动否定；低维可识别和样本机械拟合分别处理 |
| 纯噪声/条件交互 | 低相关不自动称 alpha；无条件 IC 弱不跳过预设交互；合成结果用数值容差，真实候选不预设有效 |
| 重加权与拟合隔离 | B 与 B+F 训练预算/样本相同，拟合只读成熟标签；未来标签扰动不改变更早预测；不在评价期选择combiner |
| 差值/覆盖/HAC | 共同样本与原始覆盖同时报告；手算小序列均值/方差对照；缺日不拼接，零方差/少日期不伪造 t |
| 替换与成本 | 明确权重路径手算买卖费用及净差；重复持有与调仓区分；无路径不得报净收益；降参与不能冒充等条件优势 |
| context 与局部相关 | unknown/重名/分页/动态依赖可见，读取只限定范围；不调用 compute_full_matrix 或任意catalog代码，不写官方结果 |
| 兼容与恢复 | 无 comparison 旧 run/quality 行为不变；新块 attach 匹配、乱序拒绝、记录失败后补登记不重算；两表原事务不变 |

实现阶段测试按实际 ownership/module/test plan 执行研究模块定向测试与既有普通计划；DEV 测试由普通计划收集并安全 skip，现有 DEV 专用计划仅获准后执行，runner_enabled=false 不变。不跑无关 QE 训练、全库重算或全业务回归。构造测试只验证算法，不能替代真实读写/恢复或实际研究结果。

## 9. Design Acceptance Index 与设计验收矩阵

F-301 上下文与研究角色；F-302 范围与标签成熟；F-303 统计与信息关系；F-304 时间外增量；F-305 成本与不确定性；F-306 持久化兼容恢复；F-307 方法自检与首轮研究；F-308 owner/历史/授权边界。

下表只验收本次设计是否写全，状态统一为设计待实施，不把引用的旧源码当作新增能力已交付。未来实现另以最终 HEAD 的代码/测试/DEV 与研究证据更新矩阵，保留本次设计状态。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | 本文 §3 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，代码待实施 |
| F-302 | 本文 §4.1、§8 | artifact: docs/analysis/factor_research_methodology.md | 用户批准设计先行 | 用户批准范围：仅文档，标签测试待实施 |
| F-303 | 本文 §4.2、§8 | artifact: docs/analysis/factor_research_methodology.md | 用户批准设计先行 | 用户批准范围：仅文档，不以历史测试代替新统计证据 |
| F-304 | 本文 §4.3、§8 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，研究与正式QE证据分开 |
| F-305 | 本文 §4.4 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，不造新回测器 |
| F-306 | 本文 §5 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，复用writer/attach，无DDL |
| F-307 | 本文 §6、§8 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，真实实验未执行 |
| F-308 | 本文 §1–2、§7、§10 | artifact: docs/architecture/factor_research_comparison_supplement_20260909.md | 用户批准设计先行 | 用户批准范围：仅文档，不改其他模块与历史 |

## 10. v1.0/v1.1 发布、风险与当时 Production gates（保留；本次见 §13.4）

v1.0 文档 PR 当时为 `runtime_impact=none`；v1.1 实际修改 `backend/services/factor_research/**`，最终 runtime contract 必须按 changed files/catalog 归为 backend 并绑定 `backend-main`，不能沿用旧回执。production_ddl_gate=noop、production_dml_gate=noop、dependency_install=noop、client_install=noop、production_activation=false；源码合入后是否重启由用户单独授权，本窗口不执行。DEV 仅运行既有两表事务测试和只读 context，不建表、不改 schema。

v1.1 源码交付执行数学/因果、兼容/恢复/性能、DESIGN-COMPLIANCE-001 三轮顺序审核与修复，运行所属模块计划、DEV 既有数据库计划、UTF-8/编译/Ruff/diff、F1 校验和 L0；PR 使用最终 HEAD 的必要 CI。不执行正式因子/模型计算。合入后只清理本任务命名工作树及分支，不删研究历史或数据。源码、root 同步、运行时待重启和清理分别读回；本轮是已批准 F1 实施，不伪造 BUG/close-sync。

剩余风险是未知依赖、样本不足、研究选择偏差、现有权重路径缺失及owner未交付；以对应结论限制处理，不靠新增全局门禁解决。方法是否改善发现效率必须在后续实际研究中检验，本次设计通过不是实证。

<a id="review"></a>
## 11. v1.0 文档审核与修订记录

以下为本窗口顺序实质审核，非独立 agent/Claude 再审声明，不引用讨论中的历史测试数作为本次验证。

### 第一轮：数学、复用与文档结构

核对 runner 范围参数、context 当前字段、quality 的 expression 查询、两表/记录枚举、标签常量表，明确新增能力待实现而旧路径复用。修订蓝图过时的“新建表待核实/只有全历史”式理解，保留原验收历史。方法正文明确高张成不判无信息、半偏不混称RankIC、单调变换与构造重复分开、实际候选不预设成功。

首次 F1 校验暴露 background/non_goals/implementation_plan/verification_plan 标题及验收状态不匹配；修正文档而非放宽验证器。补充变规模截面排名归一化，以免不同股票数改变拟合尺度；F1/F2 校验均已通过。

### 第二轮：跨文档一致性、因果时间与恢复

逐项核对方法 C-1～C-7 与本文实现/测试，添加稳定链接。发现“共同有效样本”若提前包含未来标签缺失会改变预测输入排名，修正为先按当时输入排名/预测，再以标签掩码评价；测试要求未来标签扰动不能改变更早预测。盈亏平衡bp从模糊的总收益/交易量细化为单一买卖共同可变费率、固定费先扣、零成交不可算。保留真实成本与研究执行成本的不同字段。

核对范围仅三份文档，P1/P2 设计与质量操作记录相对基线内容未变；不会把8月空值自动判为尾部未成熟。无新writer/表/枚举/客户端/门禁；attach的新增可选块与旧结果兼容明确。UTF-8、相对链接、七条C编号唯一性及文档范围检查通过。

### DESIGN-COMPLIANCE-001 四项最终文档审核

| 条款 | 直接依据 | 结论与限制 |
|---|---|---|
| 禁止简化交付 | §1–10包含范围/输入/数学/恢复/测试/owner/验收；八条索引八行矩阵；没有把未知依赖从报告删除 | 文档交付完整；代码和实证待实施，不以局部样本冒充生产 |
| 禁止静默错误 | 方法C-1～C-7、§4–5/§8覆盖零残差、机械拟合、未成熟、缺失、错误恢复、时点和不确定性 | 文档没有默认0/虚假独立/虚假净收益；实现需真实RED→GREEN |
| 禁止擅改业务逻辑 | 精确三文档scope、§2/§7、原P1/P2和操作记录无diff，基础引擎收益定义保持 | 未改QE/数仓/评级/数据集/数据库/运行配置 |
| 禁止私增门禁审批 | §1/§4–6/§10的诊断与生产状态分开，无新阈值/审批/全库前置 | 仅已有文档流程；没有新资源/冻结/hash/自动晋升淘汰 |

本地无剩余阻断文档finding；F2蓝图12条/12行、F1本文8条/8行均PASS/warnings=0，diff与链接/历史保护检查通过。最终HEAD的CI及合入后状态由PR记录，不提前宣称已合入或研究方法有效。

## 12. v1.1 实现回执与实施验收

### 12.1 实现范围

- `comparison_context.py` 只对本次分页可见 catalog 行做词法字段扫描和近邻线索；`inputs` 是已观察 token，`completeness=unknown`，从不执行 catalog 代码，也不把 Jaccard/同表达式提升为等价结论。
- `comparison.py` 实现显式多 fit/evaluation 窗口、独立拟合知识截止、共享标签查表、PIT/停牌样本、原始与双侧残差偏 RankIC、指定基准局部相似性、日期等权 B 与 B+F/替换/交互 OLS、张成度、分段 HAC、成本路径和两轴待解释结果。计算结果不写官方 metrics/correlations/catalog。
- `runner.py` 仅在请求显式提供 `comparison` 时加载本次候选或声明的 repo 外稳定值文件；控制项不进入模型堆叠矩阵，未声明路径保持旧行为。`service.py` 复用原 result/attach writer，并把 comparison 请求身份、窗口、方向、控制集和结果数量纳入 readback。
- 未新增表、migration、writer、task/record 枚举、依赖、资源门禁、QE/Qlib/数仓/评级修改；没有自动晋升、淘汰或 alpha 阈值。

### 12.2 重要实现精化

拟合信号区间与“拟合时知道到哪一天”不能混为一个日期，因此 v1 每个 fit window 必须携带独立 knowledge cutoff。fit 仅使用该 cutoff 已成熟且价格有效的标签；全局 cutoff 只控制评价结果当前可知范围。runner 在把指标视图裁到 signal window 时额外保留完整 read-tail `label_calendar`，使 `signal_end < read_end` 时仍按实际入场/出场交易日判断成熟，而不是按裁短日历误判。这个精化落实 C-7，不改变既有 `HOLDING_PERIODS` 或收益公式。方向符号在候选诊断/模型输入前应用，fitted/declared 及锁定时点仅按已登记请求回报，不由评价期重选。

预测模型的评价使用真正 Spearman（预测值和目标分别排名）而非预测水平与目标排名的 Pearson；偏 RankIC 仍按 C-2 对 x/y 两侧使用同一 Z 残差化且残差不再排名。每个评价窗口同时输出声明网格、PIT、候选、共同模型输入、成熟标签的逐层数量与 closure，控制变量缺失只使相应偏相关视角不可用，不缩小 B/B+F 的模型样本。

内存路径只保留声明样本：repo 外稳定值读回后立即按 signal window 和 instruments 裁剪；模型长表只堆叠 B/F/条件 state，不把诊断 controls 重复堆叠；偏 RankIC 按单日截面逐日拼接/释放。L0 的 `MEMORY-DATAFRAME-001` 对单日 `pd.concat` 给出一条非阻断启发式告警，逐行审核确认其峰值由本次股票截面而非全历史行数决定；不为消除提示新增资源门禁或虚假分块。

成本只接受四份同日期、实数、递增且 repo 外的权重/同频算术毛收益路径，并声明 currency/capital normalization。费用按实际权重变化拆买卖，固定费用定义为每个发生交易日期的一次资本归一化收益扣减；路径超出评价窗口或日期被静默求交均报错。无路径仍返回 unavailable，不影响纯预测诊断。

### 12.3 Design Acceptance Index 实施矩阵

下表与 §9 的 v1.0 设计状态并列，不能覆盖其历史事实。状态只表示代码/测试交付，不把工具可用、真实研究结论、QE 价值或运行时激活混为一体。

| design_item_at_v1_0 | implementation_refs_at_v1_0 | test_or_evidence_at_v1_0 | status_at_v1_0 | gap_or_exception_at_v1_0 |
|---|---|---|---|---|
| F-301 | `service.py`、`comparison_context.py` | `backend/tests/factor_research/test_comparison.py::test_context_dependency_scan_is_bounded_and_does_not_claim_equivalence`；DEV context readback | verified | - |
| F-302 | `comparison.py::_last_available_price_position/_mature_dates/_coverage_report` | `backend/tests/factor_research/test_comparison.py::test_label_maturity_boundary_uses_trading_calendar_and_cutoff_phase`；h20/PIT/缺失同文件测试 | verified | - |
| F-303 | `comparison.py::_partial_rank_daily/_construction_relation` | `backend/tests/factor_research/test_comparison.py::test_partial_rank_ic_residualizes_both_sides_and_never_fills_zero`；类别/张成/单调构造同文件测试 | verified | - |
| F-304 | `comparison.py::_weighted_fit/_compute_window_result` | `backend/tests/factor_research/test_comparison.py::test_fit_uses_only_labels_mature_by_its_own_cutoff`；多窗口/Spearman/替换/交互同文件测试 | verified | - |
| F-305 | `comparison.py::_hac/_cost_report` | `backend/tests/factor_research/test_comparison.py::test_cost_paths_cannot_silently_intersect_different_dates`；HAC/实际权重同文件测试 | verified | - |
| F-306 | `runner.py`、`service.py` | `backend/tests/factor_research/test_contracts.py::test_runner_optional_comparison_uses_current_candidate_artifacts`；`backend/tests/factor_research/test_recovery.py`；`python -m nox -s factor_research_dev_db` | verified | - |
| F-307 | `backend/tests/factor_research/test_comparison.py` 构造/角色测试 | `python -m pytest backend/tests/factor_research/test_comparison.py -q` | verified | approved_by_user: 本 PR 验收研究工具；正式市场研究按 §6 独立执行，不以构造测试冒充已完成 |
| F-308 | 本文 §1–2/§7/§10/§12；全部 changed files | `python scripts/aistock_feature_workflow.py validate --design docs/architecture/factor_research_comparison_supplement_20260909.md --tier F1`；`python -m nox -s l0` | verified | - |

### 12.4 v1.1 顺序审核记录

第一轮数学与因果审核发现并修复：初稿只按全局 cutoff 屏蔽标签，可能让 fit 借用其后才成熟的收益；为每个 fit 增加独立知识截止并加入边界测试。随后发现预测 RankIC 初稿误用预测水平与收益排名的 Pearson，改为两侧排名后的 Spearman。方向、PIT/停牌和未来标签变化测试通过。

第二轮兼容、恢复与成本审核补齐：控制项不再进入模型长表；分母逐层闭合并区分未成熟/价格缺失；cost 四路径不再静默取日期交集；构造结论明确为 caller declaration + values/ranks check；attach 对新块做完整身份验证，旧请求不接受伪造新块。普通模块计划和既有 DEV 两表事务计划通过，未新增 schema。

第三轮按最终 diff 执行 §8、FEATURE-WORKFLOW-001 与 DESIGN-COMPLIANCE-001；具体最终测试数、CI、PR/merge SHA 和 cleanup 只在实际发生后由工作流回执与 PR 记录，不在提交前预填。真实市场案例仍是后续研究任务，不作为本次源码合入的虚假成功条件。

最终本地证据：`factor_research_backend` 为 93 passed、9 个 DEV 测试安全 skip，Windows fresh process 2 passed；显式 DEV 计划 9 passed；workflow nox 环境测试 17 passed；module registry 8 passed 且 ownership 14/14；F1 为 8/8、warnings=0；Ruff、py_compile、diff、L0 均通过且 blocking=0；WSL `rdagent-gpu` Python 3.10.19 fresh-process 导入通过。DEV context 对两只现有因子的 expression 缺失返回 `unknown/completeness=unknown`，没有猜测依赖。生产 DDL/DML、依赖安装、进程控制均未执行。

### 12.5 DESIGN-COMPLIANCE-001 最终实现审核

| 条款 | 最终代码依据 | 结论与边界 |
|---|---|---|
| 禁止简化/子集/POC 冒充完整 | F-301～F-306 与 F-308 均有实现和真实测试；多窗口、三角色、标签、偏相关、成本、恢复均在正式路径 | 比较工具实现完整；F-307 的真实市场研究明确是后续实证，不把“未运行研究”伪装成有效 alpha |
| 禁止静默错误或假成功 | 独立 fit cutoff、完整 label calendar、PIT 分母 closure、无穷值/非法 mask、零残差、秩亏、HAC 缺口、成本日期错配和 attach 身份均显式处理 | 不填零、不静默缩样、不把统计相似性写成构造事实、不把 computed 写成有效/可交易 |
| 禁止擅改业务语义 | changed files 仅 factor_research、所属测试/nox 与本文；复用且不修改 `HOLDING_PERIODS`/官方 writer | QE、数仓、评级、PIT、数据集、策略和生产目录均未改；新结果只进研究 record |
| 禁止私增门禁/审批 | 新校验只约束 opt-in comparison 输入与科学口径；无 comparison 的旧路径不变；无 OS 以上资源门禁 | 未新增生产 gate、阈值、自动准入/淘汰或资源限制；生产库 noop，backend 重启继续由用户决定 |

<a id="full-scope-design"></a>
## 13. v1.2 完整评价实施增补（BUG-1430；代码待实施）

### 13.1 最小改造与 owner

完整评价定义仅引用[方法论完整评价](../analysis/factor_research_methodology.md#full-evaluation)。本次只改三份文档、四个客户端入口及 BUG 元数据；不改已有 P1/P2 与 §12 历史回执。实现仍沿用两表、runner、context_json/records、官方指标引擎及 attach，不加新表/状态机/平台，不启动研究或数据导出。

| 交付项 | 实现归属与最小方案 | 本轮状态 |
|---|---|---|
| 全目标 PIT 读取与分母 | 共享读取 owner 修复 `qe_eval_v2_qlib_reader._read_instruments` 的同股后段覆盖前段，保留全部历史资格区间；研究层以请求目标网格保留缺失计数，不按已读列缩小分母 | 待单独代码交付；不改 PIT 规则/数据文件 |
| 完整评价编排 | 研究 runner/CLI 复用现有范围参数展开全目标股票与全有效历史；技术预检与完整评价在原 context_json 中说明用途，旧 run 可用于预检，不把旧样本结果重标 full | 待实施；不改变已有计算/恢复接口语义 |
| 多窗口/期限独立指标 | 指标 owner 复用现有引擎，提供实际日期明确的全期、2024起、年度、近期/月度结果和 h1/h5/h10/h20 口径；研究层汇总既有字段，缺能力不自己复制第二套引擎 | FR-REQ-04 增补请求，旧请求历史不改；缺项如实报告 |
| 候选对存量相关 | FR-REQ-01 owner 提供指定成对子矩阵及有效配对范围；本批候选×候选/全部存量可评价因子，不重算未受影响的存量×存量 | 待交付；现有局部比较不能冒充已完成 |
| 结果与解释 | 研究层复用 metrics/records/attach，保存日期/池/口径/完成与缺失项、近期优先报告和两轴结论；既有真实结果复用，错误结论另作关联更正 | 待实施/运行；不写官方 metrics/correlations/catalog |

共享 owner 变更按当前 ownership 精确登记，研究窗口不直接改 QE/数仓/评级或其他窗口代码。读取修复影响的已有研究需在后续重新评价，对比双方使用同一修复版本；评价器修复产生的变化不得归因于因子创新。近期已接触历史仍必须评价，标明 research_feedback；真实时间外拟合只用已成熟标签，不能用全期相关反向挑基准再宣称独立验证。

### 13.2 范围、输出和复用

完整评价计划在现有 context_json 中说明实际数据截止、目标 PIT 规则与覆盖起点、所需窗口/期限、存量比较目录及不可比项；不要求新 schema 或机器审批。输出请求与实际范围、每期限成熟末日、理论资格/数据/因子/配对有效数，不能用 full/recent 名称代替日期。全期包括全部有效历史，2024起重点窗口与各年/最近6/3/1个月/月度同时披露，不在两者之间择优。方法论列出的适用独立指标必须计算；缺失功能明确列为未完成项，统计上无定义的量说明原因，不伪造值。

原始方向 IC 与方向化组合分开；收益/夏普标明期限、分组、基准、毛净与权重路径。当前 h1 高值组夏普不是 h20 负向策略夏普；排序换手不是实际资金换手。不得改既有权威算法后仍沿用旧版本名称。相关性须披露当前目录中已算/缺失的因子对和各自支持范围，最近邻局部结果不等于全库比较；无有效配对不能当相关0。

数据集不重导出。已有值满足代码版本/输入/单位/日期/股票范围时直接复用，不满足则补算对应缺口；不做新冻结/全量哈希。逐日完整截面的排名/分组在分批时必须保持一致，不能先在各小股票批次算 IC 再平均。按日期/必要因子列组织有界批处理，公共价格/标签一次读取复用；不将所有候选和全库因子长驻内存，不设新的 OS 以上资源限制。报告真实时间，不承诺未实测耗时。

### 13.3 验收场景与退出

以下为后续代码必须通过的测试/实证，不是本轮已通过结果：

| 覆盖条款 | 必须发现的错误 / 交付观察 |
|---|---|
| F-302：多段 PIT 与漏股 | 同股两段资格、退市历史、晚期重新入池、缺 bin 列；原区间均保留且缺失不缩原分母；请求574/读到526不能称574已评价 |
| F-302/F-306：旧日期与近期 | 输入截止2026-08-31但实际信号止于2024-06-28，报告明确完整评价未完成；recent显示真实边界，不写成2026近期 |
| F-302/F-303：日期与期限 | 暖启动、标签刚好成熟/差一天/节假日、价格缺失分开；年度/2024起与全期均有实际范围；h20指标不误配h1组合夏普 |
| F-303：相关完整性 | 全部目标因子对已算/不可比计数与目录对应；构造重复和无配对分开；新旧子矩阵与小型全矩阵对应块一致且不算无关旧旧对 |
| F-303/F-306：复用与批处理 | 全截面直接算与批处理结果在明确数值误差内一致；不得平均股票小批次IC；补登记不重算，旧窗口结果不冒充新窗口 |
| F-307：真实完整评价 | 技术有效的既有候选完成全目标池/全有效历史指标与相关性，重点2024年至最新；不要求某个正结果；缺能力/中断报告未完成和下一步，不判无价值 |
| F-308：隔离 | 数据集、官方单写指标、生产目录、QE/数仓和运行时无未授权变动；只运行所属模块计划，不用正式QE训练作为工具测试 |

后续顺序：读取/分母修复 → 研究编排与 owner 指标/相关能力交付 → 既有候选完整评价 → 用途增量与下一轮研究方向。不能在看过评价后翻转当前因子方向并沿用原结论，方向规则继续按方法论执行。不是新增阶段或审批；共享能力缺口只限制对应交付，不阻断无关业务。完整研究可结论不支持/证据不足，但只做小样本或全量执行失败不得宣布完整评价已完成。

### 13.4 本次文档验收范围

F-302/F-303/F-306/F-307/F-308 的增补设计依据为 §13.1～13.3；代码与数据证据均待后续，不覆盖 §9/§12 的历史验收。蓝图、方法论、详细设计与四入口逐项场景审核，Skill格式、UTF-8/链接、git diff --check、workflow smoke及现有CI验证文档交付。`runtime_impact=client`、backend_restart_required=false；production_ddl/dml、依赖、进程控制、数据计算/导出均noop。合入只表示新研究要求与入口已发布，不表示读取缺陷已修复或因子已完成全量评价。

### 13.5 本窗口修订审核记录

第一轮（范围/需求）：补齐技术预检→完整独立指标/相关→用途增量；重点2024年至最新与全历史并列，旧近期窗口不能冒充当前近期。修正四入口未同步、文档把已有比较能力一概写成待实现的问题；新代码能力仍明确待交付。

第二轮（反例/一致性）：发现 C-4 尚允许只复核少量弱候选、蓝图P3表仍偏向廉价比较、示例版本仍2.0、旧生产状态可能被误读为本轮。逐项修正，旧验收章节明确历史边界；增加“缺bin不能缩分母”“不能平均股票批次IC”“全期相关选基准需标research_feedback”的验收反例。没有运行市场计算或声称这些后续代码测试已通过。

DESIGN-COMPLIANCE-001：①完整交付范围为三文档/四入口，代码/数据缺口逐项列出，不以子集冒充实现；②缺失/无定义/旧日期/读列缩减均不假报完整，不填0；③仅按用户批准调整研究要求，C-1～C-7统计公式、官方writer、PIT/交易/业务语义保持；④无新增门禁、阈值、审批、冻结/哈希/资源限制或生产动作。此为本窗口顺序审核，不声称独立agent审核；最终CI/merge状态由实际PR回执记录。

第三轮（最终差异/交付边界）：核对全期与2024起并存、全部目标股与历史资格、完整独立指标和相关性、两客户端一致入口、实际日期及owner待办；将“方向选择”明确为下一轮研究方向，避免误读为事后翻因子符号。文档审核无剩余阻断项。Skill验证2项、蓝图F2/集中设计F1结构验证均通过，workflow smoke通过；registry 8 passed，普通研究计划93 passed/9 skipped及fresh-process 2 tests通过，L0 0 findings。DEV测试安全跳过，未执行DEV/生产数据库操作；这些是文档兼容/结构证据，不是新增全范围评价功能或有效alpha证据。
