# 股票池与数据导出兼容概要设计

生成日期：2026-05-04
适用范围：未来 QE 主路径、Paper Trading v2/Selection Center 主路径、Qlib Bin/H5 数据导出策略。
明确排除：所有历史遗留版本、旧版选股、旧版模拟盘不再改造。

## 1. 结论

在未来有统一股票池的前提下，数据导出和股票池职责必须分离：

- 特征数据层只保存真实历史事实，不用删除股票历史来表达股票池。
- 股票池层负责按交易日 PIT 判断是否允许买入、是否允许持有、是否只能卖出。
- Qlib Bin 通过 `instruments/*.txt` 的多段 start/end 区间限制训练、预测、回测可选范围。
- H5/parquet 长期不能依赖“删除股票”兼容股票池，必须增加独立 universe mask 或让 loader 显式接收并应用股票池。
- 股票池功能未完成前，可以用现有程序做接近目标的 QE 回测，但只能作为过渡结论，不能替代最终 PIT 股票池审计。

## 2. 改造边界

```text
模块/路径                         是否改造  原因
-------------------------------  --------  ------------------------------------------------------------
QE 新实验/演进主路径               是        后续主要研究和回测入口，必须支持统一股票池
Paper Trading v2 / Selection      是        未来模拟盘主路径，选股前应使用同一股票池解析器
历史遗留选股                       否        用户已明确不再改造，避免扩大范围
旧版 paper_trading                 否        用户已明确不再改造，保留历史行为
交易执行层                         只兜底    继续做停牌、涨跌停、分钟成交校验，不作为股票池主入口
```

## 3. 推荐的数据分层

```text
层级              保存内容                                      是否表达股票池限制      推荐规则
----------------  --------------------------------------------  --------------------  --------------------------------------------------
DB 原始数据层      日线、分钟线、复权、涨跌停、停牌、公告、ST事件  否                    尽量保留完整真实历史
Bin 特征事实层     Qlib OHLCV/limit/suspend/factor bins          否                    不因未来 ST/退市/暂停上市删除历史
Qlib instruments  all.txt / pool_xxx.txt 多段区间                是                    训练、预测、回测候选股票池入口
H5/parquet事实层   daily_pv/static/sector/factor parquet         否                    长期保留事实数据，另配 universe mask
股票池解析层        buy/sell/hold eligibility spans              是                    唯一 PIT 可交易/可买入判断来源
策略执行层          订单可成交性、涨跌停、停牌、价格精度           只做兜底              禁止用执行失败代替选股前过滤
```

## 4. 退市、暂停上市、ST 的目标处理

关键原则：不要按导出截止日全局删除一只股票的全部历史，这会产生未来信息泄漏。

```text
事件类型          特征数据是否保留  buy_pool 处理                         sell_pool/持仓处理
----------------  ----------------  ------------------------------------  --------------------------------------------
上市未满一年      保留              list_date + 365D 前不可买入            不涉及或仅用于已有持仓处理
ST/*ST 生效       保留              从 PIT 生效日开始不可新买              已持仓进入 sell-only，按真实停牌/跌停限制卖出
ST 摘帽           保留              摘帽后可按冷却期规则重新进入候选池      已持仓恢复正常评估
暂停上市          保留              暂停期不可买入                         已持仓按真实不可交易状态处理
退市/退市整理     保留历史          退市整理/退市后不可买入                已持仓按真实可卖/不可卖状态处理
北交所/BJ/BSE     不导出到 QE Bin   固定不可买入                           主 QE 不覆盖
```

说明：

- “保留”指保留历史真实行情和特征事实，不代表允许买入。
- PIT 剔除应由股票池区间或 mask 表达，而不是从 Bin/H5 中物理删除整只股票历史。
- 退市和暂停上市不能假设一定先 ST，必须有独立 PIT 事件来源。

## 5. Bin 文件的明确导出逻辑

未来目标逻辑：

```text
项目                  目标规则
--------------------  ------------------------------------------------------------
交易所范围            只导出 SH/SZ，固定排除 BJ/BSE
历史行情              保留 SH/SZ 股票真实历史行情，包括后来 ST、暂停、退市前历史
上市前数据            不导出或不可用，按 list_date 截断
复权口径              Qlib OHLCV 使用 Qlib 兼容复权口径，limit/pre_close 保持原始价格口径
股票池限制            不在 bin 字段层删除，通过 instruments/all.txt 或 pool_xxx.txt 多段区间限制
IPO 365D              通过 instruments start_date 表达，不删除 feature bin
ST/暂停/退市          通过 instruments end_date/多段区间表达，不全局删除历史
```

对当前已有导出器的注意：如果程序仍按“导出结束日之前出现过 ST/暂停/退市记录”全局排除股票，那么它只能算过渡数据，不是最终 PIT 权威数据。最终规则应改成“特征保留 + PIT instruments 限制”。

## 6. H5/parquet 应怎样兼容未来股票池

H5/parquet 的核心问题是：部分 no-alpha 或动态因子路径可能直接读取 `combined_factors_df.parquet`、`static_factors.parquet` 或 H5，不一定经过 Qlib `instruments`。

长期推荐：H5/parquet 不按股票池导出多份文件，而是保留事实数据，并增加显式股票池约束。

```text
方案                      推荐级别  说明
------------------------  --------  ------------------------------------------------------------
事实 H5 + universe mask    推荐      H5 保留全量事实，loader 按 trade_date+instrument mask 过滤
事实 parquet + spans表     推荐      parquet 保留事实，额外提供 pool_id 的 start/end 区间
按股票池导出多份 H5        不推荐    文件膨胀、版本混乱、实验复现困难
在 H5 中永久删除 ST 历史    禁止      会把未来事件写入历史数据，造成泄漏
完全不让 H5 路径参与权威回测 过渡可用  股票池未实现前，避免 no-alpha 绕过 universe
```

推荐的 H5 配套文件：

```text
文件/表                        内容                                      用途
-----------------------------  ----------------------------------------  ----------------------------------
universe_spans.parquet          pool_id, ts_code, start_date, end_date    生成 Qlib instruments 和 H5 mask
tradable_mask.parquet           datetime, instrument, can_buy/can_sell    no-alpha/动态因子 loader 直接过滤
pool_policy_manifest.json       规则版本、事件源、生成时间、hash          实验复现和审计
excluded_reason.parquet         每日每股被排除原因                        分析 ST/退市/暂停/行业黑名单影响
```

最关键的兼容要求：

- no-alpha loader 必须从“忽略 instruments”改为“强制应用 instruments 或 universe mask”。
- `combined_factors_df.parquet` 如果继续作为模型输入，必须在训练/预测前按股票池过滤。
- H5/parquet 中可以保留后来退市或 ST 股票的历史特征，但模型不能在不可买入日期把它们作为候选样本。

## 7. 股票池功能未实现前，是否可以用现有程序完成接近目标的回测

可以，但只能选择“最接近目标、风险最低”的路径。

```text
回测路径                          是否建议  可信度      原因
--------------------------------  --------  ----------  ------------------------------------------------------------
Alpha158 + Qlib Bin + stock_pool   建议      较高        主要通过 Qlib instruments 控制候选池
动态因子 + NestedDataLoader        谨慎      中          需要确认 dynamic parquet 与 instruments 一致
no-alpha / DynamicFactorsOnly      不建议    低~中       可能直接读 parquet，绕过 Qlib instruments
旧版模拟盘选股                     不建议    不纳入      用户已明确不改造旧路径
只靠交易阶段停牌/涨跌停过滤         不建议    不足        不能修正训练/预测股票池错误
```

现阶段可执行的近似流程：

```text
步骤  操作
----  ----------------------------------------------------------------------
1     使用 SH/SZ、排除 BJ/BSE、带 IPO all.txt 的 Qlib Bin 数据集
2     使用现有 stock_pool 生成脚本生成 filtered_pool_YYYYMMDD.txt
3     QE 配置中显式传入 stock_pool，不使用默认全市场隐式配置
4     优先跑 Alpha158 或 Alpha158+动态因子 NestedDataLoader 路径
5     暂不把 no-alpha/H5-direct 结果作为权威对照结论
6     回测后审计成交失败、涨跌停、停牌、持仓数、资金利用率
```

这个流程可以接近目标，但仍有边界：

- 如果当前 Bin 导出器仍然全局排除未来 ST 股票，会比目标 PIT 池更保守，不能证明 PIT 逻辑完全正确。
- 如果动态因子 parquet 未按同一股票池过滤，模型训练样本仍可能与回测股票池不一致。
- 如果只使用一个 `filtered_pool_YYYYMMDD` 表达全回测期股票池，行业黑名单属于实验策略假设，不等同于完整历史 PIT 风控池。

## 8. 后续股票池概要设计

未来新增统一 `StockPoolResolver`，只接入 QE 主路径和 Paper Trading v2/Selection Center 主路径。

```text
组件                  职责
--------------------  ------------------------------------------------------------
StockPoolPolicy        定义基础池、IPO天数、ST规则、暂停/退市规则、行业黑名单、公告风险规则
StockPoolResolver      输入 pool_id + trade_date，输出 can_buy/can_sell/can_hold 股票集合
UniverseSpanStore      持久化 pool_id 的 PIT start/end 区间和排除原因
QlibPoolWriter         从 spans 生成 instruments/all.txt 或 pool_xxx.txt
H5MaskProvider         从 spans 生成 no-alpha/动态因子 loader 可用的 mask
ExperimentBinder       QE/Paper v2 创建任务时冻结 pool_policy_hash 和 pool_version
AuditReporter          输出每日股票池大小、排除原因分布、样本覆盖率、泄漏检查
```

选股前加载顺序：

```text
顺序  操作
----  ------------------------------------------------------------
1     读取实验冻结的 pool_id / pool_policy_hash
2     根据 trade_date 解析 PIT buy_pool / sell_pool
3     训练和预测样本先按 buy_pool/mask 过滤
4     策略 topK 只从 buy_pool 中产生新买入候选
5     已持仓股票进入 sell_pool 检查，允许真实可卖时卖出
6     执行层再做停牌、涨跌停、价格和分钟线可成交性校验
```

## 9. 实施优先级

```text
优先级  事项                                      目标
------  ----------------------------------------  ------------------------------------------------------------
P0      冻结本文规则                              避免继续用删除历史数据表达股票池
P0      QE 主路径接入 StockPoolResolver            确保训练、预测、回测候选池一致
P0      修复 no-alpha/H5 loader 股票池过滤          防止直接 parquet 绕过 instruments
P0      Bin exporter 改为事实保留 + all.txt 限制    消除未来 ST/退市/暂停上市删除历史的问题
P1      Paper Trading v2 接入同一 resolver          让未来模拟盘与 QE 股票池一致
P1      H5 mask/spans 输出                          兼容动态因子和后续 no-alpha 实验
P1      股票池审计报表                              输出每日池大小、排除原因、样本覆盖率
```

## 10. 对当前问题的直接回答

```text
问题                                            回答
----------------------------------------------  ------------------------------------------------------------
未来是否可以用多股票池统一所有新主路径选股前加载  可以，QE 和 Paper v2 主路径先做，旧路径不改
Bin 是否通过 all.txt 限制即可                    目标上是，但 all.txt 必须是 PIT 多段区间，不是单日全局删除
H5 是否也通过删除股票来限制                      不应该，长期应保留事实数据 + mask/spans 过滤
H5 在股票池未实现前怎么办                        权威回测优先不用 H5-direct；必须用时临时预过滤 parquet
现有程序能否做接近目标的回测                    可以，优先 Alpha158/Qlib Bin/stock_pool，结果标记为过渡可信
是否已经达到最终 PIT 权威                        还没有，需要 resolver、H5 mask、no-alpha loader、Bin exporter 全部对齐
```
