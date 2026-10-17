# StrategyPackage 参考价可用性合同 v1（BUG-1904）

## 边界与原生语义

共享参考价读取属于 StrategyPackage，不属于 LocalSIM、Advisory 私有业务或分钟执行。
本修复不改变评分、融合、排序、原始 TopK、股票池、UNKNOWN、manifest、冻结政策或 C/D/T。
正常股票缺失真实参考价仍为数据完整性错误；合法停牌不是模型失败或交易程序失败。
参考价是仓位数量计算使用的估值依据，不是交易日存在成交或可以执行的承诺。

## C 时钟：只读真实报价

- 优先读取 C 日真实正值收盘价，状态 `EXACT_CLOSE`，`quote_date=C`。
- 仅当 C 日无报价、且有明确全天停牌记录时，读取最近一个 `<C` 的真实收盘价。
  必须证明从该报价下一自然日至 C 的日历完整，每个开市日均有全天停牌证据。
  状态为 `SUSPENSION_LAST_OBSERVED_CLOSE`，价格仍属于实际 `quote_date`，`as_of_date=C`。
- 停牌证明消费唯一公共 `project_suspension_days` 的 `FULL_DAY_SUSPENDED`，不在消费者中
  重写 S/R 优先级。既保留原始单 S 全天记录，也接受公共权威已经明确解决的同日 S/R。
  重复、未知、矛盾事件及没有权威解决的混合事件不能构造连续停牌证明。
  部分时段停牌、缺少停牌证据、断档、缺日历、缺历史真实报价不得无条件前填。
- 公告解决的 C 日证明还要求 `disclosure_date <= C`；后日公告不提前证明 C 时点的参考价。
  权威投影是有效市场状态，仍保留 `historical_publication_time_proven=false`，不声称历史信息可得。
- 所有 SQL 的行情、停牌、日历范围均截止 C；不得在 C 参考价阶段读取 D/T 的停牌或行情。
- `UNAVAILABLE` 留在完整原始评分内，不能被补价或变成空评分。原始 TopK 必需价格缺失立即失败。
  原生过滤后递补进入有效 TopK 的缺价候选同样在共享计算边界失败，不能拖至下单阶段。
- `include_reference_price=false` 的 Advisory 原始评分路径不要求参考价，也不增加读取。

## D/T 时钟：继续使用原生 eligibility

停牌过滤由原生 `TradabilityFilter` 在冻结政策规定的 D 时钟执行。
`DbSuspendLookupProvider` 读取 D 日全部原始事件并消费同一公共投影；全天/时段停牌保持原生排除，
复牌保留，不可解决的事件显式失败。不得只读取 S 行而丢弃同日 R，亦不得以缺价猜测停牌。
共享内核包装层同样必须遵守 [provider 时钟合同 v1](strategy_package_selection_provider_clocks_v1.md)：
不能把整个 tradability 改成 C；仅 risk/PIT 行业绑定 C，停牌 eligibility 和相关排除证据保留 D。
启用过滤时，停牌候选按原生 `suspended_by_suspend_d` 排除并递补；关闭过滤时，不擅自替用户打开。
参考价状态不参与评分或改变 TopK，也不替代后续交易市场约束。
C 停牌、D 复牌时仍保留实际历史报价日期；C 正常、D 停牌则在 D 原生 eligibility 判断。
本合同不读取未来价格、不预测复牌、不回填数据库、不恢复或控制生产回放。

## 原生产物与来源身份

需要参考价的行在 `component_scores` 中标明 `reference_price_required=true` 和
`reference_price_availability`（合同版本、状态、as_of_date、quote_date、完整停牌证明日期）。
既有 `reference_price_trade_date` 保持估值截止日兼容；实际报价日期必须从 availability 读取，不得把历史价格称作 C 日新报价。
正常精确日期来源保留既有 v1 receipt。存在历史停牌参考价时使用 v2 来源 receipt，
把每股实际报价日期、停牌证明和读取截止日纳入 content/parameter hash。
`first_observed_at` 使用本次真实读取时间，不倒填历史捕获时间、不提升数据证据等级。
已存产物不可覆盖；新产物的内容及 source revision 按实际来源绑定，模型/policy 身份不偷换。
BUG-1912 的新过滤结果另绑定 provider 合同版本和实际观测日期；原始评分身份可保持，
旧 C 过滤产物不兼容新 D 过滤结果，不能因 TopK 恰好相同而复用旧后处理断点。

## 验收与交付分层

直接回归覆盖正常有价、连续全天停牌、复牌、真实缺价、部分停牌、断档、日历缺失、未来隔离、
多 alpha、价格非必需路径与原生过滤递补。以真实保存的已完成推理输入进行只读原生链验证，
核对完整评分、原始股票池、UNKNOWN、排序/TopK、实际报价日期和 eligibility 状态。
源码/CI 与新进程只读证据通过不等于生产账户回放通过。后端由用户重启；
LocalSIM owner 再验证 2026-01-20（C=2026-01-19）的已完成运行及真实共享链，
不复制共享算法、不重做已完成订单和账务。核对 301362.SZ 的实际历史报价及连续停牌证明、
D 日资格排除和原生递补，并保留旧产物；缓存/断点不得误用旧 C 过滤结果。
BUG-1911 已确认 603616.SH/2026-01-21 合法停牌且无权威 pre_close；共享链负责冻结政策下的
D 日资格过滤，停牌持仓估值、旧计划修订和原回放受控恢复由 LocalSIM owner 处理。
不得要求补造无权威来源的价格，也不得放宽正常股票及递补候选的真实缺价校验。

## BUG-1959：共享停牌权威接入与运行身份

两个公共消费者经 `selection_center/suspension_provider.py` 薄适配器调用唯一数据投影；
适配器只冻结已加载权威并生成回执，不拥有账户、评分、递补算法或业务缓存。
参考价观测及 D 日过滤阶段分别保留 C/D 用途、完整原始事件、投影、权威原文、
schema/policy、实现 SHA 和 data identity。来源 receipt 的 content hash 包含停牌证明身份。
LocalSIM 独立请求/断点身份补齐公共参考价、过滤器和薄适配器的源码指纹；进程启动时冻结，
不能因之后更新工作目录而冒称新实现已加载。新进程加载共享修改后不复用旧过滤断点，
原冻结账户、政策、manifest、股票池、C/D/T、独立结果和成功账本仍分别保持。
原 01-20 至 01-22 成功日不重新下单；01-23 是本轮原 LSTM 的运行态验收点。
