# LocalSIM 10月输入与账户续接契约验收

日期：2026-11-06。交付阶段：`INPUT_AND_OPENING_CONTRACT_ONLY`，对应设计`docs/architecture/localsim_period_continuation_inputs_f2_20261106.md`的F-201—F-209。本文不作为10月账户回放或经济效果验收。

## 输入与实现

交接准备收据：`X:/AIstock_temp/position-timing-october-extension-20261106/4970f2f5eea6ae8c08608387ce262a03e93ed7d704fadc12c412229ceeca332f.json`，完整SHA `a500646e101996a5845aa53b6dcb842f7ebb2f4598fc99403942868c2071f383`。

`period_continuation.py`新增独立离线`prepare`及只读`inspect`入口。`timing_campaign.inspect_selection_history`仅增加可选`committed_artifacts`参数；默认扫描行为不变。准备request、preflight和opening schema显式使用v2，不能交给现有v1空账户runner执行。

冻结候选：`20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`，manifest完整SHA `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。沿用数据owner的`ACCEPTED_WITH_WARNINGS / 18`，质量文件完整SHA `20d3b605d92c467a648b2736286ac3b81b697dc2be3a6c12256c2c2952e79b10`。

父结果完整SHA `090b2d993ef49de12c3cd887b874329e087b921f9ea6d013d40a0f2e578de7cf`；父completion完整SHA `65fafca9bafe44834004ae367e7bd3733ff62362ae5fa864c7074f75ec4c931f`。父研究已经通过其正式request显式采用修正后的输入candidate；早期campaign locator仅保留来源记录，不重新消费。新期间读取父request和完整source-inspection共同绑定的实际候选，不以失效的早期候选路径阻断续接，也不重新解释旧结果。

## 消费字段合同

1. owner首先消费`preflight.json`的FileRef，调用`inspect_preparation`核验完整SHA及最后发布的`preparation_receipt.json`。无receipt的失败前缀不可消费；`BLOCKED_SOURCE_INPUTS`不可当作准备通过。`WAITING_PRICE_PLANS`仅表示本阶段输入和初态已就绪，不授权账户回放。
2. 交易日期严格读取`trade_dates`；从`selection_inventory`读取各日已有projection/completion、原目标顺序、UNKNOWN/NONE和逐日C/D/T，不从年初自动扩展、不重新推理。原生release逐日保留，同时比较实际runtime profile及正式PIT规则身份。
3. `initial_accounts`是13份`localsim_period_opening_account_v2`。`parent_account_id`一对一绑定相同rule/delay/operation；`parent_day`及`parent_account_receipt`可回读。`cash_cny`、`positions`承接父真实状态，`new_capital_added_cny=0`。持仓成本和股数不变，只通过既有原生结算推进T+1并重绑子账户归属。
4. 每户`opening_nav_cny`和`opening_marks_raw_cny`取父9月30日14:57原终值；不得用当日15:00价格或新候选重述旧账。`analysis_anchor`绑定原资本、父高水位、原累计MDD/费用和父结果引用。
5. 10月期间盈亏为各自`terminal_nav - opening_nav`；匹配B0净增量为两户期间净变化之差。累计收益分母保留1月资本原点；期间MDD从各自opening NAV起算，累计MDD承接父高水位和原MDD。不通过额外充值拉平账户。
6. `historical_sell_symbols`是73只历史持仓并集；`prior_only_sell_symbols`保留12只不在10月目标中的历史股票。owner生成价格计划时需要这些SELL种子，不能仅从本月目标累计SELL。
7. owner仍须独立交付历史SELL价格计划及期间/累计分析兼容。未来LocalSIM可执行期间request必须绑定本准备契约和该价集的新FileRef，不能修改v1请求绕过初态检查。本轮不调用比较器读取10月经济结果，不执行10月回放及经济exact retry。

## 审核与验证记录

第一轮审核修复了来源元数据摘要变化的误报、父研究早期未消费candidate路径的误读，以及发布前来源再核验的顺序。保留父/新source身份，比较真实语义字段；来源变化不等于价格变化，实际价格变化仍明确阻断。

第二轮审核增加最后发布的准备receipt、无receipt拒绝、FileRef再核验、逐户profile/lineage绑定、T+1回读核验及真实父NAV时钟/终点约束。没有引入新的会计、计费、成交或筛选算法。

真实首轮preflight保留在`prepared/b85416f1ecc103e328bc2b7d56b26337729f04eb7d23e52abb4ceb32923aa99f`，完整SHA `8d532b1f0e9efce2ec6a7c35e210cff7d64437d4a8438bfa0cc33c781f3670f2`，状态为`BLOCKED_SOURCE_INPUTS`。17日所需行情无缺失，阻断来自7只父期股票的分钟成交量表示差异。逐字段审计`X:/AIstock_temp/localsim-october-continuation-20261106/history-differences-r1.json`，SHA `96c1cd67ae8892218961a2974218ac613bb095eb511c17be7cac3384e4f9cf65`：价格、时间、成交额及日交易资格全部一致，只变更`volume_shares`。7只为002069.SZ、002356.SZ、300595.SZ、601002.SH、605337.SH、688161.SH、688289.SH。

第三轮审核将跨期合同明确为“初态估值和交易资格等价”，而非完整历史回放输入等价。旧成交量不是继承现金、份额、成本或NAV的计算输入，故记录全字段不等及其差异摘要，不新增容差、不替换数据，不以此阻断新月份初态。本次新研究的来源变化显式冻结；不能将该结论套用到同一研究exact retry或完整历史重放的来源等价。7处差异与候选声明的18项日/分钟成交量质量警告不是同一人口，两者分别保留。真实价格或时间/交易资格变化仍阻断。

已完成真实候选质量/PIT声明回读、父研究request/result/source-inspection身份核对、13账户末日journal及原生账本核对。完整17日行情preflight、定向测试和正式交付路径在本节后续补齐，不将预检查等同最终验收。

L0通过：`X:/AIstock_temp/localsim-october-continuation-20261106/validation/guardrails/verify_skill_l0_paths.json`，0 finding；主guardrail扫描0 blocking。RTK无nox包装，使用项目原生`python -m nox --no-venv -s l0 -- <显式路径>`；验证输出通过本任务精确Junction落X盘。

## 状态与边界

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | backend/services/simulation_execution/localsim/period_continuation.py；backend/services/simulation_execution/localsim/timing_campaign.py | backend/tests/simulation_execution/test_localsim_period_continuation.py；真实17日库存 | planned | none |
| F-202 | backend/services/simulation_execution/localsim/period_continuation.py | 身份/SHA/政策负例；真实source声明 | planned | none |
| F-203 | backend/services/simulation_execution/localsim/period_continuation.py | 13份原生父journal及账本回读 | planned | none |
| F-204 | backend/services/simulation_execution/localsim/period_continuation.py | 原生T+1/归属及篡改负例 | planned | none |
| F-205 | backend/services/simulation_execution/localsim/period_continuation.py | 原末日NAV及181日高水位读取 | planned | none |
| F-206 | backend/services/simulation_execution/localsim/period_continuation.py | 真缺价/停牌负例及真实文件预检 | planned | none |
| F-207 | backend/services/simulation_execution/localsim/period_continuation.py | 最后发布receipt、失败前缀拒绝及独立inspect | planned | none |
| F-208 | backend/services/simulation_execution/localsim/timing_campaign.py | 显式文件/旧默认库存等价及旧文件保护 | planned | none |
| F-209 | backend/tests/simulation_execution/test_localsim_period_continuation.py；docs/architecture/localsim_period_continuation_inputs_f2_20261106.md | 本任务WSL/L0/定向测试/F2/CI收据 | planned | none |

源码：开发验证中；真实17日输入：待正式离线执行；账户续接：末日核验通过、待最终文件发布；10月回放/经济exact retry：未执行；运行态：未操作；经济效果：未评价。

数据库、数据集、行情网络及backend/worker/scheduler均未操作。既有1—9月父产物、旧失败证据、原288场景和生产账户保持原样。离线准备不需要后端重启；本次不代替线上运行态加载验证。
