# LocalSIM 10月输入与账户续接契约验收

日期：2026-11-06。交付阶段：`INPUT_AND_OPENING_CONTRACT_ONLY`，对应设计`docs/architecture/localsim_period_continuation_inputs_f2_20261106.md`的F-201—F-209。本文不作为10月账户回放或经济效果验收。

## 输入与实现

交接准备收据：`X:/AIstock_temp/position-timing-october-extension-20261106/4970f2f5eea6ae8c08608387ce262a03e93ed7d704fadc12c412229ceeca332f.json`，完整SHA `a500646e101996a5845aa53b6dcb842f7ebb2f4598fc99403942868c2071f383`。

`period_continuation.py`新增独立离线`prepare`及只读`inspect`入口。`timing_campaign.inspect_selection_history`仅增加可选`committed_artifacts`参数；默认扫描行为不变。准备request、preflight和opening schema显式使用v2，不能交给现有v1空账户runner执行。

冻结候选：`20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`，manifest完整SHA `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。沿用数据owner的`ACCEPTED_WITH_WARNINGS / 18`，质量文件完整SHA `20d3b605d92c467a648b2736286ac3b81b697dc2be3a6c12256c2c2952e79b10`。

父结果完整SHA `090b2d993ef49de12c3cd887b874329e087b921f9ea6d013d40a0f2e578de7cf`；父completion完整SHA `65fafca9bafe44834004ae367e7bd3733ff62362ae5fa864c7074f75ec4c931f`。父研究已经通过其正式request显式采用修正后的输入candidate；早期campaign locator仅保留来源记录，不重新消费。新期间读取父request和完整source-inspection共同绑定的实际候选，不以失效的早期候选路径阻断续接，也不重新解释旧结果。

## 消费字段合同

1. owner首先消费`preflight.json`的FileRef，调用`inspect_preparation`核验完整SHA及最后发布的`preparation_receipt.json`。无receipt的失败前缀不可消费；`BLOCKED_SOURCE_INPUTS`不可当作准备通过。`WAITING_PRICE_PLANS`仅表示本阶段输入和初态已就绪，不授权账户回放。
2. 交易日期严格读取`trade_dates`；从`selection_inventory`读取各日已有projection/completion、原目标顺序、UNKNOWN/NONE和逐日C/D/T，不从年初自动扩展、不重新推理。原生release逐日保留，同时比较实际runtime profile及正式PIT规则身份。owner绑定价格计划时使用`preflight.selection_inventory.sha256`的完整文件SHA；准备request内部同名摘要字段为canonical内容摘要，不能混用。
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

正式准备已通过，状态`WAITING_PRICE_PLANS`；新期间必要输入阻断0。17日850目标、170只目标股票；可能持仓人口逐日累积，共2324证券日、550788根bar。父73只历史持仓的初态必要输入全部相同，其中66只完整可见前缀相同，7只成交量差异如上保留。13户分别回读181日已冻结NAV锚点，共2353账户日，重算账户日0；共同终点仍为9月30日14:57，没有充值或旧账重述。

新输入/初态产物根：`X:/AIstock_temp/localsim-october-continuation-20261106/prepared/465cb1fd201b93e9fda412ec85e8c5219752198c84a6d6a4bf708f23c0c5f706`。producer source commit为`3fbc60d54554ce8652378a943fa4caf6ce662205`。本次产物约2.07 MiB，无模型或候选复制；准备337.06秒、进程峰值RSS 704432 KiB。这是输入与收据准备耗时，不是账户回放性能。

| 文件 | 完整文件SHA256 |
|---|---|
| preflight.json | 12bdae08351b363f9da871b70e20e1a2faee4caafdad897d7146fb7e61636e56 |
| preparation_request.json | 5f25528dd2bfc0f1a5f45e3bef2911b22032e95291a3cbe932a43c4f2d81fb54 |
| selection_inventory.json | d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc |
| preparation_receipt.json | 20442e0c1a49ca6297d764b2deeea92b8bb3a7cdc09183bf1c98ffac0af20a2d |
| X:/AIstock_temp/localsim-october-continuation-20261106/execution-r2.json | 2bb5bdf032ca69e6259e0a4254a826ff4f767122812baf2886ba1a670e3df239 |
| X:/AIstock_temp/localsim-october-continuation-20261106/readback-r2.json | d840be8974b301a8c0ccc3c21c28501fe65afa43f2b77f6dd0cd08da53b3841f |

全新WSL进程完成独立`inspect`，回读状态和人口一致，59.23秒；未计算账户日，也不是经济exact retry。17项新合同测试全部通过（`pytest-final-a.xml`10项、`pytest-final-b.xml`7项），2项既有原生数量/完整批次检查回归通过（`pytest-legacy-guards.xml`）。输入损坏、错误人口、错误C、profile变化、现金/NAV伪造、来源读中变化、真缺价、合法停牌、不可消费失败前缀、旧入口等价、可见前缀、非估值差异披露均有定向证据。Ruff及diff检查通过。完整经济回放、运行态和收益验证不在本阶段。

L0通过：`X:/AIstock_temp/localsim-october-continuation-20261106/validation/guardrails/verify_skill_l0_paths.json`，0 finding；主guardrail扫描0 blocking。RTK无nox包装，使用项目原生`python -m nox --no-venv -s l0 -- <显式路径>`；验证输出通过本任务精确Junction落X盘。

## 状态与边界

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | backend/services/simulation_execution/localsim/period_continuation.py；backend/services/simulation_execution/localsim/timing_campaign.py | backend/tests/simulation_execution/test_localsim_period_continuation.py；17日850目标 | passed | none |
| F-202 | backend/services/simulation_execution/localsim/period_continuation.py | 身份/SHA/政策负例；真实quality/PIT/profile及7种release声明 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/pytest-final-b.xml | passed | none |
| F-203 | backend/services/simulation_execution/localsim/period_continuation.py | 13份原生父journal及账本回读；readback-r2.json ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/readback-r2.json | passed | none |
| F-204 | backend/services/simulation_execution/localsim/period_continuation.py | 原生T+1/归属及现金/NAV篡改拒绝 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/pytest-final-a.xml | passed | none |
| F-205 | backend/services/simulation_execution/localsim/period_continuation.py | 每户14:57 NAV、2353日原NAV及高水位读取 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/execution-r2.json | passed | none |
| F-206 | backend/services/simulation_execution/localsim/period_continuation.py | 真缺价/停牌负例；2324证券日真实输入与73只初态价格核验 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/execution-r2.json | passed | none |
| F-207 | backend/services/simulation_execution/localsim/period_continuation.py | 最后发布receipt、失败前缀拒绝及独立WSL inspect ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/pytest-final-a.xml | passed | none |
| F-208 | backend/services/simulation_execution/localsim/timing_campaign.py | 显式/旧默认等价；pytest-legacy-guards.xml；父文件保护 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/pytest-legacy-guards.xml | passed | none |
| F-209 | backend/tests/simulation_execution/test_localsim_period_continuation.py；docs/architecture/localsim_period_continuation_inputs_f2_20261106.md | X:/AIstock_temp/localsim-october-continuation-20261106/下WSL/L0/定向测试/F2收据；CI为合入前最终HEAD门禁 ; artifact: X:/AIstock_temp/localsim-october-continuation-20261106/readback-r2.json | passed | none |

本阶段输入和初态：验收通过；源码：等待PR最终HEAD CI及合入门禁，状态以PR实时检查和外部交付回执为准；10月回放/经济exact retry：未执行；运行态：未操作；经济效果：未评价。完整期间runner不在本阶段，不将本阶段验收当作可执行研究完成。CI发布门禁不由本地矩阵替代。

数据库、数据集、行情网络及backend/worker/scheduler均未操作。既有1—9月父产物、旧失败证据、原288场景和生产账户保持原样。离线准备不需要后端重启；本次不代替线上运行态加载验证。
