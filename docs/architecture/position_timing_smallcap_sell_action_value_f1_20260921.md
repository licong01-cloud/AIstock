# PT-NEXT-027：小市值专用 SELL-vs-HOLD 动作价值实验详细设计

**Feature tier**：F1  
**状态**：DESIGN_VERIFIED / IMPLEMENTATION_PENDING  
**日期**：2026-09-21  
**模块边界**：`backend/services/position_timing` 离线研究  
**父证据**：PT-NEXT-024、PT-NEXT-025、PT-NEXT-026  
**唯一开发权威**：`docs/standards/aistock_development_standard_v1.5_20260523.md`

## 1. Background / Goal / 唯一研究问题（F-001）

PT-NEXT-026 已证明恢复模型不是当前主要突破口：两个恢复模型相对同股长期持有只有轻微正点估计，正式区间全部跨零，而且中位 1 日恢复带来更多循环和费用；固定 5 日恢复反而是更好的诊断候选。本轮因此固定恢复侧，只回答一个问题：

> 原 PT-NEXT-024 通用 U0 卖出模型迁移到小市值股票时是否存在可利用的域错配；在相同候选事件、20%减仓、固定5日恢复、成本和成交限制下，小市值专用的 SELL-vs-HOLD 动作价值能否相对同股长期持有同时提高成本后终值并降低最大回撤？

不扫描恢复天数、卖出阈值、市值阈值、股票池、特征集合或模型超参数。Ridge 与冻结浅层 GBDT 是两个预注册函数族，不按验证或测试收益二选一。

## 2. Population / Account / Time（F-002）

- 源人口仍为 R8 的 5,144 只股票。研究人口为 `SMALL_LT_50B`：在测试窗口内首次同时满足 PIT 有效、技术特征 ready、且 T−1 `db_total_mv < 500000` 万元时登记；市值未知不填补、不入池。首次登记后连续跟踪至统一终点，避免按未来退市、ST 或后来市值反向删样本。
- 每只股票拥有独立 5,000,000 元现金账户，无杠杆、无追加资金；卖出所得现金只留在本股账户自然复投，多股之间不共享资金。
- 训练：2018-08-01～2022-12-31；验证：2023-01-01～2024-06-30；测试首次执行：2024-07-01；最后普通执行：2026-08-28；终值：2026-08-31。
- 测试中的策略与同股 BH 从相同登记日开始，使用相同初始资金、买入手数、逐腿费用、T+1、停牌、方向性涨跌停和终止清算规则。
- 研究继续不模拟市场冲击或成交参与率；`market_impact_simulated=false`。500 万元结果是无冲击的信号研究证据，不是小市值实盘容量证明。

## 3. Frozen Candidate / Recovery / Only Variable（F-003）

- 候选事件与 PT-NEXT-024/026 保持一致：冻结风险 guard、连续两日收盘低于 `SMA60-ATR20`、以及盈利状态下 R0 加速放量事件；只允许 20% 战术减仓，核心暴露下限 80%。
- 所有 Timing 路径在减仓实际成交后，从该成交后的第 5 个交易日开始尝试恢复；停牌或方向性涨停时逐日重试。恢复规则不是模型变量。
- 正式测试只有卖出 gate 不同：通用父 GBDT、小市值 Ridge、小市值 GBDT。模型预测严格大于 0 bps 才接受该次候选减仓；预测不大于 0 时本次候选 episode 被消费，不在后续日轮询同一事件。
- BH 不执行普通减仓。父 GBDT 路径只作已冻结迁移基准；不重新训练、不修改其模型 hash。

### 3.1 Non-goals / 非目标

本轮不研发选股、QE/HMM/Agent 融合、分钟方向信号、在线卡片、自动交易、实盘容量模型或账户级公司行动清算；也不通过追加股票池切片、恢复参数、特征块和模型族寻找正结果。

## 4. Causal SELL-vs-HOLD Label（F-004）

训练和验证标签必须来自小市值 cohort 中、仅依赖当时可见规则的候选 episode，不得先用通用父 GBDT 在其自身训练期筛选样本。每个候选只形成一个标签，两个臂从同一标准化 500 万元账户状态出发：

1. `SELL_FIXED5`：决策后 T+1 尝试卖出 20%，成交后第 5 个交易日起尝试用本次减仓现金恢复；执行失败按同一冻结成交规则重试。
2. `HOLD`：不执行本次减仓，原仓位继续持有。
3. 两臂在决策后第 21 个交易日按同一调整后收盘价盯市；只有完整终值可得才形成成熟标签。

`label_sell_fixed5_advantage_bps = 10000 × (wealth_sell_fixed5 - wealth_hold) / 5,000,000`

标签记录 `decision_date`、`label_window_end` 和 `label_available_at`。训练／验证只读取在各自 cutoff 前已经成熟的标签；跨边界标签进入 `IMMATURE_OR_OUTSIDE`，不得进入拟合。测试标签、受限 Oracle、未来最优卖点、测试收益与父模型输出都不得进入训练、标准化、阈值或模型选择。

## 5. Features / Models（F-005）

- 输入严格固定为 PT-NEXT-024 的 14 项日频因果市场特征，均在 T 日 20:00 决策时可得；不增加市值数值、恢复状态、分钟线、财务、HMM、新闻或 QE 特征。
- 市值仅决定 cohort 身份，不作为模型特征，避免本轮同时改变人口和信息集。
- Ridge：训练集均值／标准差标准化、闭式 L2 回归，规格沿用父研究。
- GBDT：LightGBM 4.6.0、100 轮、深度 3、7 叶、单线程确定性训练，其余参数沿用父研究；不 early-stop、不搜索。
- 两个模型共享完全相同的行、标签、特征顺序和 0 bps 决策阈值；模型、训练行、预处理、特征合同和 request 均哈希绑定。

## 6. Policies / Comparators / Formal Family（F-006）

测试期只生成四条连续账户路径：

- `BH500`：同股长期持有。
- `PARENT_GBDT_FIXED5_V1`：冻结通用父卖出模型 + 固定5日恢复。
- `SMALLCAP_SELL_RIDGE_FIXED5_V1`：小市值 Ridge 卖出 gate + 固定5日恢复。
- `SMALLCAP_SELL_GBDT_FIXED5_V1`：小市值 GBDT 卖出 gate + 固定5日恢复。

正式 family 包含 8 个端点：两个新模型分别对 BH 和父 GBDT 比较成本后终值差及 MDD 改善。按交易日聚合，25 日 circular moving-block bootstrap、5,000 次、seed `20260921`，Bonferroni `0.05/8`，经济阈值 0 bps。

- 单端点 `SUPPORTED`：校正后下界 > 0；`NEGATIVE`：校正后上界 < 0；其余 `INCONCLUSIVE`。
- `ALPHA_SUPPORTED_EXPLORATORY`：某一新模型对 BH 的终值和 MDD 均 `SUPPORTED`。
- `DOMAIN_MISMATCH_SUPPORTED_EXPLORATORY`：同一新模型对父 GBDT 的终值和 MDD 均 `SUPPORTED`。
- 其他情况保持 `INCONCLUSIVE`；正点估计、名义区间或逐股胜率不得替代校正后结论。

模型验证 MAE、标签均值、预测正值比例、逐股收益/MDD/双胜率、平均暴露、费用、候选数、模型接受／拒绝／不可用计数、完成减仓恢复周期数及实际等待天数全部报告，但只作机制诊断，不反向选择模型、阈值或人口。

## 7. Artifacts / Identity / Exact Retry（F-007）

新建 timing-owned `research/causal_smallcap_sell_value_v1`，至少封存：request、R8/source preflight、父模型引用、label audit、labels、两个模型、trial spec、causality receipt、enrollment audit、chunk manifests、逐股摘要、逐日聚合、fills、正式比较、诊断、receipt 和 manifest。

request 显式绑定干净仓库提交、R8 candidate manifest、父 bundle/模型、完整 contract、源文件和环境。bundle 内容寻址且不覆盖历史产物；exact retry 只能复用相同身份或返回 `ALREADY_MATERIALIZED`。完整 source preflight 通过前不得读取研究收益。

## 8. Causality / Fail-closed（F-008）

- 训练路径：R8 frozen source → T−1 cohort / T as-of features → causal rule candidate → two-action mature label。
- 测试路径：R8 frozen source → T−1 cohort / T as-of features → frozen candidate → frozen sell model → intent → T+1 E0 execution → fixed5 recovery → account/outcome。
- Oracle 始终 `policy_access=false`，且本轮不重新计算 Oracle。
- candidate、calendar、factor/restatement、父 bundle/model、特征顺序、训练成熟时钟、源代码、模型环境、chunk 或串并行身份漂移均 fail closed。
- 非有限特征或模型预测不得默认为 HOLD 成功样本；运行事件显式记 `MODEL_UNAVAILABLE`，正式报告计数。

## 9. Implementation Plan / 最小实施方案（F-009）

只新增两个离线研究文件和一个定向测试文件，并更新本设计和主蓝图；优先复用 R8 reader、市场特征、候选识别、账户、费用、成交、聚合和 artifact 纯实现。不新增 API、页面、路由、数据库表、scheduler、worker 平台、registry/current 或 serving adapter。

执行阶段不再细分为多个产品里程碑：设计验证 → 定向手算／因果测试 → 小样本 pilot → 串并行一致性 → prepare → WSL 8 进程全量 run → inspect → exact retry → 结果回填 → 三轮审核 → CI/PR/合入/清理。阴性或不显著结果同样视为实验完成，不追加参数扫描。

## 10. Verification Plan / 验证方案与合入标准（F-010）

直接测试至少覆盖：500万元现金守恒；规则候选不受父模型同样本筛选；SELL_FIXED5 与 HOLD 标签手算；标签成熟边界；小市值 T−1 cohort；固定5日恢复；父/新模型 gate 区分；停牌／方向性涨跌停不伪造成交；Oracle/测试列不进入特征；模型身份；正式8端点 family；无 DB/network/runtime 写入。

本地最小门：changed-file Ruff/compile、定向测试、一个相关小矩阵、`git diff --check`、F1 validator；主蓝图更新后再跑 F2 validator。正式 WSL 运行必须完成 inspect、exact retry 和有界串并行一致性。PR 以 required `CI verdict` 为合入依据。

完成前执行三轮复核：业务／因果、数值／统计、隔离／设计一致性。禁止把测试通过、Oracle、诊断正点估计或模型验证误差写成已获 alpha。

## 11. Scope / Risks / Production Gates / Stop Rule（F-011）

只允许修改 `backend/services/position_timing/causal_smallcap_sell_*`、对应定向测试及本设计／主蓝图。不得修改 QE、Selection、HMM、Advisory、Paper、MiniQMT、local_data、candidate/profile、数据库或在线 L1/L1a。

### 11.1 Risks / 风险

- 小市值方向由既有测试段诊断生成，属于 exploratory hypothesis-generated，不冒充独立 sealed holdout。
- 相同股票和相邻候选事件相关；正式推断以连续聚合账户路径和交易日 block bootstrap 为准，不把标签行数当独立样本量。
- 忽略冲击可能高估可实现收益；本轮不得据此宣称 500 万元实盘容量。
- 8 端点校正降低功效，但这是同时检验 alpha 与域错配的预注册代价，不得运行后删端点。

### 11.2 Production Gates / 生产门

生产门：DDL/DML=`NOT_APPLICABLE`；依赖=`NO_CHANGE_EXPECTED`；生产激活=`NOT_REQUESTED`；backend restart=`NOT_REQUIRED`；在线写入=`FORBIDDEN`；`selected_for_live=0`，无论结果如何都不自动发布。

若本轮没有形成校正后支持，停止在当前14项技术信息集上继续叠加择时模型，不扫描更多模型、阈值或小盘切片；下一主方向转为独立股票筛选/选股 alpha 与择时的后续组合设计。该 stop rule 是研发方向约束，不阻塞完成阴性实验或保留证据。

## 12. Design Acceptance Index

| design_item | 设计要求 |
| --- | --- |
| F-001 | 固定恢复侧，只检验小市值专用 SELL-vs-HOLD 与父模型域错配 |
| F-002 | R8 小于50亿元 T−1 cohort、每股500万元独立账户和固定时间段 |
| F-003 | 候选、20%减仓、固定5日恢复和父模型身份冻结，唯一改变卖出 gate |
| F-004 | 500万元 SELL_FIXED5 对 HOLD 成熟标签，禁止父模型同样本筛选与未来泄漏 |
| F-005 | 14项既有因果特征、Ridge/浅层GBDT、0 bps阈值、零搜索 |
| F-006 | 新模型对BH和父模型的8端点正式family及诊断边界 |
| F-007 | 独立不可变artifact、全身份绑定、preflight-before-outcomes和exact retry |
| F-008 | 训练/测试信息路径、Oracle隔离及身份漂移fail-closed |
| F-009 | position_timing内最小离线实现、WSL 8进程全量回放、不建平台 |
| F-010 | 定向测试、F1/F2、串并行、inspect、retry、三轮审核和CI |
| F-011 | 单模块隔离、零运行发布、无支持即停止当前技术信息集堆叠 |

## 13. Design Acceptance Matrix

`DESIGN_VERIFIED` 只表示设计闭合，不表示实现、收益支持或运行发布完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | §1 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-002 | §2 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-003 | §3 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-004 | §4 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-005 | §5 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-006 | §6 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-007 | §7 | artifact: `research/causal_smallcap_sell_value_v1` | DESIGN_VERIFIED | none |
| F-008 | §8 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-009 | §9 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-010 | §10 | test: `backend/tests/position_timing/test_causal_smallcap_sell_value.py` | DESIGN_VERIFIED | none |
| F-011 | §11 | artifact: `research/causal_smallcap_sell_value_v1/receipt.json` | DESIGN_VERIFIED | none |

## 14. Initial Review

1. **目标复核**：没有同时改变股票池、恢复、阈值或特征；研究问题能直接区分“小市值专用卖出模型是否优于通用父模型”与“是否超过长期持有”。
2. **因果复核**：训练候选由规则产生，不用父模型在自身训练期筛样本；标签在成熟后才进入相应 split；Oracle、测试结果和未来最优动作均无政策访问权。
3. **统计复核**：父模型不再仅靠点估计比较，而进入同一预注册8端点 family；功效代价如实接受，不据此增加门禁或减少端点。
4. **过度工程复核**：仅两个离线文件、一个测试和两份设计文档；不建设服务、注册中心、调度平台或跨模块融合。
5. **隔离复核**：只读 R8 与父 artifact，只写 timing-owned 内容寻址研究目录；DB、网络行情、运行态、进程控制和其他模块均为 false。
