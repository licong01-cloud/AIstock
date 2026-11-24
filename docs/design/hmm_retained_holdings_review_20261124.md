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

## 设计验收索引与验证

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `scripts/hmm_risk/review_retained_holdings.py` 的 `review` 按预计留存金额排序并列出股票和净值占比 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，两本历史账 371 个有效选择与独立 FIFO 清单相符 | PASS | 无 |
| F-002 | 同一入口校验持仓完整性、保留逐日状态并按决策日及生效日匹配可选预警 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，396 个原账户日逐一覆盖 | PASS | 无 |
| F-003 | `scripts/hmm_risk/review_planned_additions.py` 的 `planned_reviews` 计算两种复核选择，返回逐股与行业的正需求和资金来源拆分 | `backend/tests/hmm_risk/test_review_planned_additions.py`，两本原账 396 天次产生 792 行，与原始目标、持仓及次日成交对照 | PASS | 无 |
| F-004 | 同一入口校验原目标行数、账户日期、市值、行业、预警和空选择，支持本地文件和标准输出 | `backend/tests/hmm_risk/test_review_planned_additions.py` 覆盖不完整目标、无新增、错误行与输出一致性；此前 396 行旧仓清单逐字节一致 | PASS | 无 |

## 生产门禁与适用方式

入口使用本地 Parquet 和 CSV。已验证范围为离线人工复核；真实使用率和账户收益仍未知。交易、服务、数据库和策略包运行配置由原消费者管理。
