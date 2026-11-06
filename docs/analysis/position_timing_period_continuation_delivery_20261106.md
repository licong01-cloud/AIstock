# 择时10月续接：价格计划及期间分析交付

日期：2026-11-06。所属模块：position_timing。状态：择时侧软件与价集完成；LocalSIM独立期间账户回放及真实经济结果未执行。本文件不是收益确认。

## 1. 完成范围

对应[单模块F1设计](../architecture/position_timing_period_continuation_f1_20261106.md) F-301～F-306。

- `intraday/period_context.py::load_period_context`：消费正式v2 preflight、准备request、最后发布receipt及13份初态，校验身份/人口/零新入金/累计锚点；不复制LocalSIM账本验证。
- `intraday/price_plan_campaign.py::build/inspect`：兼容旧v1，新增历史持仓SELL种子；仍复用唯一CandidateReader和50bps构造器。
- `intraday/continuous_results.py::PeriodAnalysisSpec/analyze_period`：新增期间统计入口，旧AnalysisSpec/analyze不变。消费已核验的LocalSIM逐日账户事实，不生成成交、费用或NAV。

软件完成不等于LocalSIM已实现期间runner，也不等于10月回放已完成。当前没有10月facts、收益、回撤或producer经济exact retry；不能补造或从测试夹具读取经济结论。

## 2. 固定输入身份

LocalSIM准备根：`X:/AIstock_temp/localsim-october-continuation-20261106/prepared/465cb1fd201b93e9fda412ec85e8c5219752198c84a6d6a4bf708f23c0c5f706`。

| 输入 | 完整文件SHA256 |
|---|---|
| preflight.json | 12bdae08351b363f9da871b70e20e1a2faee4caafdad897d7146fb7e61636e56 |
| preparation_request.json | 5f25528dd2bfc0f1a5f45e3bef2911b22032e95291a3cbe932a43c4f2d81fb54 |
| selection_inventory.json | d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc |
| preparation_receipt.json | 20442e0c1a49ca6297d764b2deeea92b8bb3a7cdc09183bf1c98ffac0af20a2d |

candidate：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`；manifest完整SHA `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。

包`pkg_c8d3d51fb6d24be89cfb3a50c30c80e4`，包manifest canonical SHA `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`；父结果完整SHA `090b2d993ef49de12c3cd887b874329e087b921f9ea6d013d40a0f2e578de7cf`。

不重训、不重推理、不重新选股。原生runtime release逐日绑定保留；不把release元数据变化直接当算法变化。沿用数据owner已接受质量披露，不修改数据或扩大原始容差。

## 3. 正式10月价集

最终生产源码commit：`f1b790daece1e9d6740314af6c70712da137b7d1`。后续文档提交不改变该源码身份。

最终产物根：
`X:/AIstock_temp/position-timing-october-implementation-20261106/artifacts/intraday_v1/shared_price_plans/3746fc3359a7eff8505e140abd91f67adff983ae7770ac0a65293f09b7314c3b`

下列文件全部使用该根下的绝对路径；WSL路径为`/mnt/x/AIstock_temp/...`，不复制大型数据。

| 文件 | 完整文件SHA256 |
|---|---|
| templates.json | 245b88622a6f0a1ea9fc43f5577944bd5826f800961b05b66a2966abbdae7cde |
| policy_document.json | 10519776e781d8d72f44264cb94d6bef843bfa62a2cb15eb9309616ddb5a021a |
| reference_audit.json | 97aa561cd4de0aebd5c7972b04423706f5f2d1a0fb667ef7f8a7482f48c5aae7 |
| manifest.json | 02089ceec4ffca123e6bab8fa1352c4c0d63f41b619a8a6d722b7a2fe724bd54 |

人口：2026-10-08～10-30，17个交易日，850个当日BUY、2324个SELL，共3174条事件。13户9月末73只历史持仓每天SELL覆盖完整；其中12只本月从未入选的股票保留SELL，不能为它们生成无用BUY。UNKNOWN参考价0，consumer_status=READY。

仍为`RAW_PREVCLOSE_PREFERRED_50BPS_V1`：raw prev_close参考、BUY=[0.995R,R]、SELL=[R,1.005R]，0.01元向外舍入；盘前09:00可见是历史假设，当日09:30～14:57有效。不是价格预测、最优阈值或生产默认。新身份不覆盖旧288或旧1—9月研究。

WSL最终整批验收收据：
`X:/AIstock_temp/position-timing-october-implementation-20261106/price-acceptance-f1b790daece1e9d6740314af6c70712da137b7d1.json`
完整SHA `ffde00e87ee3ec10e4cd6aeeb40be12c7d5e9316ce1d062c63696f4c3baef6a6`。

build=28.22秒、inspect=31.00秒、exact retry=29.34秒；峰值RSS548192KiB，结束时进程FD观测6。使用已有缓存状态，未清OS缓存，不称冷缓存基准；这些是价集验证耗时，不是账户回放性能。三步manifest/templates/policy身份一致，retry=ALREADY_MATERIALIZED。

## 4. LocalSIM最小后续接缝

LocalSIM尚须在自己的模块内实现/接入独立期间runner，绑定上面同一个v2 preflight和templates；不能把准备文件交旧v1空账户runner。每户继承自己的初态，禁止充值或重置成零持仓；只跑10月17日，不重跑1—9月。

分析入口：

```text
python -m backend.services.position_timing.intraday.continuous_results analyze-period --spec <PeriodAnalysisSpec绝对路径> --spec-sha256 <完整文件SHA> --timing-root <独立择时artifact根>
```

公共Python入口：`PeriodAnalysisSpec(preflight=FileRef(...), price_templates=FileRef(...), facts=FileRef(...))`与`analyze_period(spec, timing_root)`。spec schema=`position_timing_period_analysis_spec_v1`。

LocalSIM提供一个轻量的`position_timing_period_facts_v1`投影即可，不要求更换其正式runner/result schema。完整字段见F1设计Contracts，关键为：

1. preflight和price_templates必须使用上面的完整FileRef；producer_source_commit为实际runner commit。
2. accounts必须完整13户，account_id来自v2 opening，各自days为17个按日期排序的正式账户日FileRef，不补造或删除未成交账户。
3. 逐日事实沿用既有day中的rule/delay/operation、trade_date、terminal_as_of、terminal_nav_cny、fees_cny和全部nav_observations；不需要择时读取或重放账本。
4. source_batch_verified=true，outcomes_read_stage=AFTER_COMPLETE_SOURCE_BATCH_INSPECTION，nav_observation_scope=ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS。
5. source_inspection为最终来源/结果绑定收据FileRef，stage=COMPLETE_SOURCE_BATCH_INSPECTED；绑定preflight、price_templates、candidate_manifest、trade_dates、producer_source_commit、account_days_sha256=digest(facts.accounts)、费用文件SHA及执行合同canonical SHA。**这是回放完成后的结果绑定，不是开跑前必须预知结果SHA的门禁**；原始source检查仍在读取收益前做，原始检查引用可保留在该收据额外字段中。
6. 费用文件SHA取preflight.inputs.cost_policy.sha256；执行合同SHA取digest(preparation_request.execution_contract)，均复用原政策。

择时分析器验证全部来源及day SHA后才解析新期间NAV。期间收益从每户opening NAV算；累计收益从原1000万元资本算。24项比较明确移除父期净值差；MDD消费完整NAV并承接父峰值/历史MDD，不伪装纯择时归因。

producer经济exact retry和择时报告exact retry分开。本接口目前不消费尚未定义的LocalSIM期间重试状态schema，故默认明确标REPRODUCIBILITY_UNVERIFIED/NOT_SUPPLIED，并输出暂定描述性报告；这不阻止研究或伪装成验证通过。LocalSIM自己的exact retry应独立交付。

## 5. 多轮审核与验证

- 第一轮：冻结期间/累计数学口径；补金额类型与固定analysis_contract、13户lineage、receipt-last、历史SELL来源校验。
- 第二轮：费用/执行合同来源绑定；prepare异常Reader关闭；身份漂移、缺失/未来NAV不静默补值。
- 第三轮：逐项对照F-301～F-306和DESIGN-COMPLIANCE-001，明确最终消费者收据与开跑前source检查的时序，避免循环依赖。
- 直接稳定回归：新期间及原v1价集/分析共35项通过；第二轮补强后新期间17项通过。测试文件`backend/tests/position_timing/test_intraday_period_continuation.py`；旧直接测试分别为`test_intraday_price_plans.py`与`test_intraday_continuous_results.py`。
- 本地ruff、compileall、git diff --check、L0通过；静态扫描0 finding/0 blocking。ownership全映射，无跨owner实现修改。
- classifier=targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]；required plans=l0、position_timing_backend，无DEV数据库计划。最终CI按PR HEAD另行回读，不把本地检查当CI。
- 真实WSL用已有rdagent-gpu环境完成初态读取及整批价集构造/inspect/retry。WSL已有三个研究环境均缺pytest，未安装依赖；直接测试在Windows完成，不能声称WSL pytest已通过。

精确命令和测试XML/JSON位于`X:/AIstock_temp/position-timing-october-implementation-20261106/`；源码和受控文档经独立PR/CI交付。

## 6. 完成与未完成状态分列

| 项目 | 状态 |
|---|---|
| 共享历史SELL价集、公共分析接口 | 完成并有定向验证 |
| 真实10月价集build/inspect/exact retry | VERIFIED，同不可变身份 |
| LocalSIM期间runner/消费验收 | 由LocalSIM窗口接续，本文不宣称已完成 |
| 10月13账户回放、经济exact retry | 未执行 |
| 10月实际收益/回撤及横向经济分析 | 待正式producer结果；未读取 |
| 数据库/数据集/行情网络/服务/生产账户操作 | 均未执行 |
| 后端重启/运行态激活 | 离线交付不依赖重启；未请求或验证在线加载 |

本轮feature无BUG修复，不登记虚构BUG；未触及其他模块。后续只需用户转交LocalSIM按本合同接续，不能把其未交付部分移入择时临时wrapper。

运行分类单列：当前runtime catalog对这些backend Python路径保守匹配`backend-main`，本任务没有修改catalog、扩大非运行时豁免或宣称在线进程已加载。这里的“不需重启”仅指新鲜WSL进程调用离线CLI可完成交付/消费，不代表生产运行态完成；未来若接入在线入口，重启和运行身份验证仍由用户独立授权。
