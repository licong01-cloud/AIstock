# LocalSIM 分钟择时接入包（源码合入后由用户转交）

状态：`TIMING_SDK_IMPLEMENTED_CONSUMER_PENDING`。择时纯SDK、提醒API/组件和功能回放已实施；本文件不触发或授权本窗口修改LocalSIM。源码合入后由用户转交模拟盘窗口；SDK离线接入无需等待用户后端重启，线上API尚待重启后核验。

权威设计：[总蓝图](../architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)、[阶段一详细设计](../architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md)。

## 已交付输入

- 源码身份以携带本文的已合入PR/commit为准，核心入口为 `backend.services.position_timing.intraday.evaluate`；合同位于同目录 `contracts.py`，版本均v1；B0/C1/C2参数见详细设计§4与显式 `Policy`。
- 每个计划固定 `Policy(source_commit=<实际消费源码commit>, policy_id=..., allow_split=...)`，`digest(policy)`写入 `Plan.policy_sha256`，不得把合成fixture的虚拟source_commit当作已部署身份。
- 可重放输入、输出、测试与CLI证据见[实施验收记录](../analysis/position_timing_intraday_phase1_implementation_20261023.md)。功能fixture只有1个计划/10根分钟线，不是R8收益研究，所有延迟输出都是未确认成交的意图。
- API/页面需用户后端重启后核验，当前 `runtime_verified=false`；LocalSIM/Advisory产品接入与经济效果均未确认。

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
