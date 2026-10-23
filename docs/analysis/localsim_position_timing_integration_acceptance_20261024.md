# LocalSIM 分钟择时接入与离线研究验收

状态：实施/真实账户研究待运行；不得视作已合入、已激活或经济效果得到支持。

## 已冻结范围与输入

采用 `localsim_position_timing_integration_f2_detailed_design_20261024.md` 的 F-001..F-010。原 24 计划、三规则、四延迟和 288 适用 case 不变。全部新账户的起始现金为 1000 万元；SELL 研究初态通过前一交易日真实 LocalSIM 模拟买入及 T+1 形成。12 个连续离线账户选择最新加入回执中的 LightGBM＋12项因子，保留原生产账户和默认 TWAP。

WSL 整批 `research.inspect` 已确认 `VERIFIED`；manifest canonical SHA=`8b9128a33c15464c0784b953d658d0666fa2697608e46b073f3a3691ae81b9c1`，24 原计划、12 child、288 case，耗时 77.61 秒。此回执未读取经济结果，不是历史模拟验收。

## 审核及修复记录

第一轮：核对上位 TWAP authority、唯一核心依赖和因果钟。修复测试把 09:31:10 决策用于 09:31 open 的错误；严格保留未来决策拒绝。账户反馈按真实确认可见时刻保存，禁止把后一分钟确认状态重新标为更早时刻；闭环调用不得倒退到最新确认之前。新增显式离线 policy 身份，旧默认不变。

第二轮：核对现金、成交、取消、恢复、计费和 board-lot。不重新实现账本；复用现有 OMS、MinuteExecutionEngine、账本累计计费及 LocalSimEconomicCoordinator。修复 Decimal snapshot 规范化丢失金额小数位导致恢复后重复事实冲突；恢复使用原生经济事实序列化，部分成交后继续仍只收父订单最低 5 元。增加 whole-generation 原子发布、CAS、hash 链、readback 和失败前缀测试。修订数量先确认取消旧父订单，返回真实已成交及新剩余目标给 planner，不让旧意图按新数量执行。SELL 零股残余仅允许实际清仓，不把订单尾量当成持仓残股。新路径接入既有 broker record、查询及 fill callback。

直接测试目前包括：三规则真实核心、四延迟 strict-after、未来扰动、现金/可卖量/T+1、HARD、停牌、方向涨跌停、部分成交、取消/修订、累计分批/冷却、恢复/失败前缀/重复、未成交现金终值和可回读 receipt。正式最终矩阵和 CI 待执行。

## 连续账户的外部输入缺口

10-23 原生创建回执绑定的 LightGBM 包使用 TWAP 日目标；没有冻结全年分钟择时计划和到价区间来源。不能把前收盘价自动解释成用户价格偏好，更不能令 PRICE_TOUCH 永不成交后将其包装成有效策略对比。此缺口归择时计划 producer/LocalSIM 消费合同协同，非数据缺价、模型重训或数据库补数问题。可以冻结 12 个独立离线账户初态；正式全年回放须等合法输入，不能将预检标成 RUNNING。

## 运行与生产边界

仅文件输入和独立研究输出；不查询生产数据库，不执行 DDL/DML，不改数据集，不抓行情，不操作 backend/worker/scheduler，不创建生产账户或启动其回放。离线 CLI 不等待后端重启。现有生产 runtime 的加载与未来 activation 继续独立报告。

## 待补真实收据

producer commit/release、初始账户/准备/费用/执行合同、288 行结果与账户 receipt、比较器报告、exact retry、耗时/内存/磁盘，以及 PR/CI/合入状态将在真实运行后登记。源码接入、历史模拟、运行激活和经济结论分别给出。
