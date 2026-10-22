# LocalSIM 分钟择时接入需求（基础设施就绪后转交）

状态：`CONTRACT_ONLY_DO_NOT_START`。本文件不触发模拟盘窗口开发，也不宣称择时核心/API已经实现。用户要求先完成择时侧基础能力，可接入时再转交。

权威设计：[总蓝图](../architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)、[阶段一详细设计](../architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md)。

## 到时转交的输入

- 择时源码commit、公开evaluate入口及schema/policy hash，B0/C1/C2固定规则。
- 可重放的分钟前缀、session/日级交易事实、计划和执行视图，预期decision/state与四档delay_intents。
- 核心/接口测试、inspect/exact retry证据；明确哪些仅是功能证据，经济效果仍未确认。
- 本模块API/页面是否需用户重启及其状态，不能把未重启当已经启用。

上述输入尚不存在时请勿按本文开始修改LocalSIM，不需要等待模型正收益或未来行情才生成这些输入。

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
