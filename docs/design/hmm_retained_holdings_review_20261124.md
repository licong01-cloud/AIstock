# 按预计留存本金复核旧仓 · F0 功能卡

## 范围

HMM 离线研究工具为每本账户、每个决策日提供一个人工复核行业。调用方使用当日收盘持仓、逐笔买卖和已知的次日目标，按先进先出识别持有超过十个交易日的股份。行业采用决策日归属。按股数计价的输入可以汇总本金；只有席位的荐股名单不适用本接口。

```sh
python scripts/hmm_risk/review_retained_holdings.py --days review_days.parquet --stocks review_stocks.parquet --warnings review_warnings.parquet --output account_review.csv
```

- `days` 每账户日一行。字段为 `account,date,effective,nav,has_next_target,old_stock_count`，最后一项为原账中旧仓股票数量，包括预计全部卖出的股票。
- `stocks` 每账户日每只旧股一行。字段为 `account,date,stock,code,old_value,retained_value`。最后两列分别为原旧仓市值和预计按原目标留下的旧股份市值，使用决策日价格及账户净值币种。预计全卖时留存值填零。行业归属未知时保留空值。
- 可选的 `warnings` 按 `date,effective,code` 提供冻结的 `M0,M1,p0,p1`。日期分别为决策日和生效日。输出先选行业，再附两种概率和预警。缺预测保留空值。
- 输出含留存金额、占净值百分比和预计继续持有的股票。金额并列时按行业代码选择一项。原目标缺失、持股数量不全、行业未知和没有可复核旧仓分别保留状态。清单不含未来收益或损失。

## 设计验收索引与验证

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `scripts/hmm_risk/review_retained_holdings.py` 的 `review` 按预计留存金额排序并列出股票和净值占比 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，两本历史账 371 个有效选择与独立 FIFO 清单相符 | PASS | 无 |
| F-002 | 同一入口校验持仓完整性、保留逐日状态并按决策日及生效日匹配可选预警 | `backend/tests/hmm_risk/test_review_retained_holdings.py`，396 个原账户日逐一覆盖 | PASS | 无 |

## 生产门禁与适用方式

入口使用本地 Parquet 和 CSV。已验证范围为离线人工复核；真实使用率和账户收益仍未知。交易、服务、数据库和策略包运行配置由原消费者管理。
