# 旧仓留存与计划新增资金的行业复核 · F0 功能卡

## 范围

HMM 离线研究工具为每本账户、每个决策日提供一个人工复核行业。调用方使用当日收盘持仓、逐笔买卖和已知的次日目标，按先进先出识别持有超过十个交易日的股份。行业采用决策日归属。按股数计价的输入可以汇总本金；只有席位的荐股名单不适用本接口。

```sh
python scripts/hmm_risk/review_retained_holdings.py --days review_days.parquet --stocks review_stocks.parquet --warnings review_warnings.parquet --output account_review.csv
```

- `days` 每账户日一行。字段为 `account,date,effective,nav,has_next_target,old_stock_count`，最后一项为原账中旧仓股票数量，包括预计全部卖出的股票。
- `stocks` 每账户日每只旧股一行。字段为 `account,date,stock,code,old_value,retained_value`。最后两列分别为原旧仓市值和预计按原目标留下的旧股份市值，使用决策日价格及账户净值币种。预计全卖时留存值填零。行业归属未知时保留空值。
- 可选的 `warnings` 按 `date,effective,code` 提供冻结的 `M0,M1,p0,p1`。日期分别为决策日和生效日。输出先选行业，再附两种概率和预警。缺预测保留空值。
- 输出含留存金额、占净值百分比和预计继续持有的股票。金额并列时按行业代码选择一项。原目标缺失、持股数量不全、行业未知和没有可复核旧仓分别保留状态。清单不含未来收益或损失。

## 复核次日计划新增资金

当复核问题是「已经有旧仓的行业，明天还计划买多少」，使用第二个离线入口。每本账户每天返回两行。`retained` 先选预计留存旧仓本金最大的行业，`incoming` 在预计仍留有旧仓的行业中选计划新增最多的一项。两行分别服务于持仓复核和买入计划复核，盘后准备时可以只查看所需行。

```sh
python scripts/hmm_risk/review_planned_additions.py --days days.parquet --stocks review_stocks.parquet --targets targets.parquet --warnings review_warnings.parquet --output additions.csv
```

- `days` 增加 `target_count`，由当日已知的完整原目标名单提供。`targets` 每个 `account,date,effective,stock` 一行，含决策日行业 `code`、原净值权重 `target_weight` 和该股收盘总持仓市值 `held_value`。总市值包括新旧股份；经完整原持仓确认无仓才可填零。三个输入使用相同收盘时点和币种。
- 每股新增需求为原权重乘净值再减该股总持仓市值，负数取零，各股按分汇总。行业并列时优先预计留存本金较大者，再按行业代码。同业第一只复核股票按新增需求、股票代码排序。输出包括行业预计新增总额、占净值比例、其中新开仓和已有持仓补买金额，以及第一只股票的需求。实际可成交本金还取决于现金、整手和执行约束。
- 缺目标行、缺持股市值、正需求股票行业不明分别标记 `INCOMPLETE_TARGETS`、`UNKNOWN_TARGET_HOLDINGS`、`UNKNOWN_TARGET_INDUSTRY`，对应金额留空。所有留存行业均无新增需求时，`incoming` 行标记 `NO_PLANNED_ADDITION`、需求为零。原旧仓和原目标状态沿用前述入口。冻结的 M0、M1 在行业选定后匹配，两列可分别缺失。

## 核对同一笔现金能否改变买单顺序

历史执行复核使用下面两个离线入口。第一个从同一账户同一分钟的已归档原买单找出两种现金互斥的选择；第二个核对改买以后，原策略要求清仓时应卖多少股。复核者须提供原执行顺序、整手、原始价格、累计费用与当时现金。交易后形成的成交回执用于历史验证。

```sh
python -m scripts.hmm_risk.review_competing_buys --tickets tickets.parquet --scores reconstructed.parquet --output choices.csv
python -m scripts.hmm_risk.review_competing_exits --bars exit_bars.parquet --output exit_sizes.csv
```

- `tickets` 按 `account,tick,stock` 唯一，使用上海本地时钟。`date` 为更早的决策日，`code,rank` 来自该日的行业和原股排名。`sequence` 为先卖后买账本下同分钟买单的实际顺序。`filled,attempted,remaining_before` 为真实成交、可证明的原尝试量及原母单当时剩余股数。`cash_limited` 只在实际因资金缩量时为真，`exact_attempt` 为假表示只知道尝试量的下界。`price_cents,prior_notional_cents,cash_end_cents` 单位为分，分别为原始每股价格、本订单本次之前的累计成交金额、保护其他原成交后的分钟结余。调用方提供同一时点所有获资买单与可定位的受限买单，并独立覆盖未定位记录。原费率为买入万分之 0.95、每张累计最低 5 元。
- `status` 描述是否可以从先获资的其他行业撤下一个能够独立买卖的最小单位，转给受限行业。分别核对两只股票的交易单位、原单剩余量和边际费用。`whole_status` 描述两个原尝试子单是否各自都能买足、合在一起又超出现金。只有在原排序能逐股复现原成交时，才输出反向优先后的两只股数；剩余现金继续用于先获资的原股。两项各自的 `first_daily`、`whole_first_daily` 标出每账户日最早可核实的一个选择，同刻按原获资顺序再按受限股票代码。多个可替代配对往往使用同一笔现金。
- 可选 `scores` 以 `date,effective,code,score` 对齐。较高的冻结行业分才换序，同分保留，缺分标 `UNKNOWN` 并在账户比较中沿用原顺序。正式与历史重构分应分别运行。原股排名的对照也保留未知值。
- 清仓复核使用 `account,order,tick,stock,target_quantity,original_open,revised_open,split_count,sellable_bar,volume_shares,original_sold`。逐笔原时钟、是否可卖及原始成交量均来自同一归档，开盘股数已核对 T+1。只处理原目标明确为零的三段 TWAP。还原原卖量后，用修订持仓在相同分钟重算；增加的卖量须位于原成交分钟，增量上限取该分钟成交量的 1%。缺原单一致性或容量时输出具体状态，建议股数为空。整个账户的现金和净值需继续按原流水检验。

## 设计验收索引与验证

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `scripts/hmm_risk/review_retained_holdings.py` 的 `review` 按预计留存金额排序并列出股票和净值占比 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，两本历史账 371 个有效选择与独立 FIFO 清单相符 | PASS | 无 |
| F-002 | 同一入口校验持仓完整性、保留逐日状态并按决策日及生效日匹配可选预警 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，396 个原账户日逐一覆盖 | PASS | 无 |
| F-003 | `scripts/hmm_risk/review_planned_additions.py` 的 `planned_reviews` 计算两种复核选择，返回逐股与行业的正需求和资金来源拆分 | `backend/tests/hmm_risk/test_review_planned_additions.py`，两本原账 396 天次产生 792 行，与原始目标、持仓及次日成交对照 | PASS | 无 |
| F-004 | 同一入口校验原目标行数、账户日期、市值、行业、预警和空选择，支持本地文件和标准输出 | `backend/tests/hmm_risk/test_review_planned_additions.py` 覆盖不完整目标、无新增、错误行与输出一致性；此前 396 行旧仓清单逐字节一致 | PASS | 无 |
| F-005 | `scripts/hmm_risk/review_competing_buys.py` 比较受限买单与原获资者，保留其他成交、原排序、缺分、两种单位及每日本金去重 | `backend/tests/hmm_risk/test_review_competing_buys.py`，396 个历史账户日中确认 5 个最小单位配对及 51 个完整子单配对，日内共享资金 | PASS | 无 |
| F-006 | `scripts/hmm_risk/review_competing_exits.py` 用同一清仓目标、原始时钟和成交量核对原卖量与修订股数 | `backend/tests/hmm_risk/test_review_competing_exits.py`，四张已完成清仓母单共 12 个原成交分钟与独立逐笔核算一致 | PASS | 无 |

## 生产门禁与适用方式

入口使用本地 Parquet 和 CSV。已验证范围为离线人工复核；真实使用率和账户收益仍未知。交易、服务、数据库和策略包运行配置由原消费者管理。
