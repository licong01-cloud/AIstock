# LocalSIM 分钟择时接入包（由用户转交）

状态：`TIMING_SDK_RUNTIME_VERIFIED_RESEARCH_READY_CONSUMER_PENDING`（2026-10-24）。择时纯SDK、提醒API/组件、历史批研究及结果比较器已实施；本文件不触发或授权本窗口修改LocalSIM。阶段一PR #6611已合入且用户重启后核验通过；本次离线研究增量无需重启，不等待未来行情。

权威设计：[总蓝图](../architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)、[阶段一详细设计](../architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md)。

## 已交付输入

- 源码身份以携带本文的已合入PR/commit为准，核心入口为 `backend.services.position_timing.intraday.evaluate`；合同位于同目录 `contracts.py`，版本均v1；B0/C1/C2参数见详细设计§4与显式 `Policy`。
- 每个计划固定 `Policy(source_commit=<实际消费源码commit>, policy_id=..., allow_split=...)`，`digest(policy)`写入 `Plan.policy_sha256`，不得把合成fixture的虚拟source_commit当作已部署身份。
- 可重放输入、输出、测试与CLI证据见[实施验收记录](../analysis/position_timing_intraday_phase1_implementation_20261023.md)。功能fixture只有1个计划/10根分钟线，不是R8收益研究，所有延迟输出都是未确认成交的意图。
- 阶段一加载身份/health/五路由/只读合同核验通过，见[运行回执](https://github.com/licong01-cloud/AIstock/pull/6611#issuecomment-6356504087)；这不代表实际盘中采样、Advisory来源桥接或LocalSIM产品接入。

本包已具备可调用核心与冻结例子，后续owner接入不以模型正收益或未来交易日为前置。纯SDK导入不创建运行客户端、订单、数据库连接或全局策略注册。

### 最小调用与提醒消费

`from backend.services.position_timing.intraday import evaluate, Plan, MarketView, ExecutionView, Policy, State`

`decision, state = evaluate(plan, observed_prefix, execution_view, plan.policy, previous_state, as_of)`

模拟反馈必须给出匹配的symbol/trade_date/available_at、confirmed/reserved_quantity、cash_cny、sellable_quantity、source_ref、feedback_id、last_confirmed_batch/at。下一批仅由确认后的反馈推进；核心输出累计目标比例，owner负责其原有lot/资金/可卖量转换，不能按每次剩余量重复乘1/3。

提醒消费者导入 `frontend/src/app/position-timing/IntradayTimingPanel.tsx`，传入apiBase/planRefs/refreshSourcePlans/onStatus；不复制算法。服务实例的source_resolver协议须由来源owner实现并注入；当前未配置真实Advisory桥接时创建ADVISORY计划返回422，不信任浏览器自行核实。API路径见详细设计§5.1；无成交写回endpoint。

### 冻结功能例子

外置artifact路径见实施验收记录，均在既有position_timing artifact root的 `intraday_v1/research` 下。

- 输入：`functional_inputs/phase1_20261023_v1/spec.json`，来源明确为 `functional_fixture_only`。
- request：`requests/7685a812fb4a4e1c11e7802a5e9c5cd9319a010f81ea420400a4c7cf495fe575.json`。
- bundle：`bundles/7685a812fb4a4e1c11e7802a5e9c5cd9319a010f81ea420400a4c7cf495fe575`；manifest canonical SHA256：`06c7aadfc787e2b0ad8b54205a128ea15c8f8622169b12ed27ef4f009ee3bcb1`。
- 独立进程prepare/run/inspect/exact retry成功，retry为 `ALREADY_MATERIALIZED`，10个decision、1个opportunity、4档delay_intents。未来经济研究不得用此合成candidate冒充正式R8 manifest。

## LocalSIM窗口负责的最小范围

1. 从最新main核查LocalSIM自身设计及实现；当前TWAP-only不是任意算法注册器。更新自身上位合同，通过新的明确策略身份允许择时facade，旧TWAP release/账户/参数和退役V25规则不变。
2. 消费择时纯函数，不依赖提醒HTTP、浏览器、MiniQMT或Tick。沿用LocalSIM分钟源、增量游标、资金/持仓/费用/OMS与账户持久化，不建第二套实现。
3. 正确映射当前 `init_order/compute_step/is_complete` 与 `StepResult/StepFill`：机会先成为待执行意图，到满足后续bar/延迟/限制后才形成模拟成交；不能在信号bar收盘后回填同bar价格。
4. 处理partial确认、资金/可卖量、board-lot/T+1、停牌/方向性限价、截止取消及尾单。不能因旧residual策略而越过新计划硬限价或过期继续执行。
5. D0/1/3/5使用相同原始计划和独立模拟状态，报告实际等待；不可成交保留原因。纯延迟冻结机会流与反馈闭环分别报告。
6. 保留当前native TWAP对照；另用同成交代理/费用/时延的B0作主基准。LocalSIM费用与个人净佣分项口径不得混称一致，不从提醒次数推断父订单费次数。
7. 定向回归、L0/CI和多轮修复后交付自己的源码/PR；后端重启仍由用户操作。本需求不授权数据库/数据集变更或生产操作。

## 接口语义摘要

`evaluate(plan, observed_prefix, execution_view, policy, previous_state, as_of) -> decision,next_state` 无I/O，状态显式传入，不依赖进程全局变量。
decision只含机会、累计目标比例/可空数量、价格条件、有效期和原因；只有LocalSIM确认成交才能推进executed_quantity。
HUMAN_UNKNOWN不能用于模拟账户；同输入/同状态需与提醒核心输出一致，不要求不同成交反馈的消费者永远输出相同结果。

## 交回证据

策略版本与新release身份、old TWAP不变、三策略调用闭环、D0/1/3/5、价格/资金/partial/恢复/取消边界、费用与全计划分母、经济评价是否完成分别列明。接入功能可用不等于新策略优于TWAP。

## 已就绪的真实历史研究消费包

研究合同与命令见[研究详细设计](../architecture/position_timing_intraday_research_f2_detailed_design_20261023.md)，实际人口/诊断见[实施记录](../analysis/position_timing_intraday_research_implementation_20261023.md)。

- 外置根：`F:\Dev\AIstock_model_artifacts\position_timing_advice_v1\intraday_v1\research_studies`，WSL对应`/mnt/f/Dev/AIstock_model_artifacts/position_timing_advice_v1/intraday_v1/research_studies`。
- request/bundle身份：`1c684e78b94872878608cafc48a7a496540b9a9c8cdf62424d886154398d2062`；bundle manifest canonical SHA：`8b9128a33c15464c0784b953d658d0666fa2697608e46b073f3a3691ae81b9c1`。
- `bundles/<身份>/consumption.json`包含24原计划×三规则×四延迟的288个case、候选/费用/执行合同及12个逐日逐规则子包。文件是`{sha256,payload}`不可变信封，先校验再消费；不得直接改写其中WSL路径或身份。
- 子包提供派生计划、归一化raw市场、决策、机会和延迟意图。`EXECUTION_SLOT_ELIGIBLE`不是成交；无机会case也须保留。当前是冻结机会流延迟研究，不等于带成交反馈的分批闭环。
- plan的target_quantity=1000只是预声明目标，不是已有资金或可卖量证明。LocalSIM owner需在读取收益前冻结每个计划的真实模拟初始账户；同计划三规则和全部延迟共用该账户、目标金额及14:57终值时刻。BUY须有足够现金，SELL须有合法可卖数量；不把反复评估或提醒解释成新父订单。
- 费用使用本消费包指定的LocalSIM合计费率最低额模型：买0.000095、卖0.000595、最低5元、父订单累计。它不是用户个人净佣加规费合同。旧native TWAP独立命名，主比较使用同延迟matched B0。

返回一个有完整文件SHA的JSON，schema=`position_timing_localsim_research_results_v1`，严格字段以`intraday/research_results.py::EconomicResults/EconomicRow`为准：

1. 顶层：producer=`LOCALSIM`、真实producer_source_commit/release_identity、study_manifest_sha256、cost_policy_file_sha256、execution_contract_sha256、max_drawdown_definition=`POSITIVE_PEAK_TO_TROUGH_FRACTION`。
2. 每个适用case恰一行：case_id、initial_account_sha256、initial_nav_cny、target_quantity/target_notional_cny、filled_quantity、average_price_raw、fees_cny、terminal_as_of/terminal_nav_cny、max_drawdown、mean_wait_seconds、status、reason_code、account_receipt_sha256。
3. status限FILLED/PARTIAL/NO_FILL/UNAVAILABLE；未成交仍评价原现金/原持仓终值，不能丢行或填零收益；未知则UNAVAILABLE并说明来源。保留可回读的账户receipt，hash不代替账本真实性。
4. 待执行机会若修订/取消/条件失效不能续用；最多执行预定目标，停牌/方向性涨跌停/零成交/资金/board-lot/T+1由LocalSIM原链路决定。不模拟市场冲击但不能取消交易资格检查。

接回后由择时窗口在WSL调用：

```text
python -m backend.services.position_timing.intraday.research compare --bundle <上述bundle绝对路径> --results <LocalSIM结果绝对路径> --results-sha256 <完整文件SHA>
```

比较器先重新inspect整批输入/子包再读取账户结果，校验人口、费用、初始账户和终止时点，报告BUY/SELL/ALL、三规则×四延迟、净增量/条件成交价差/MDD降低/等待/完成率及缺失分母。没有LocalSIM结果时仍为`NOT_RUN_LOCALSIM_ADAPTER_PENDING`；不能用测试手算结果替代真实模拟评价。
