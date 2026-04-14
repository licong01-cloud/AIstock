# Tushare 新数据源因子研发计划

> 创建日期: 2026-04-02
> 状态: 进行中

## 一、背景

当前因子库（891个因子，558可用）主要基于以下 h5 数据集：
- `daily_pv.h5` — OHLCV 行情
- `daily_basic.h5` — PE/PB/换手率/市值等
- `moneyflow.h5` — 资金流向
- `bak_basic.h5` — 历史基本面
- `cyq_perf.h5` — 筹码分布
- `sector_data.h5` — 申万二级行业

通过 Tushare API 探索，发现 21 个可用接口（2026-04-01 验证），可扩展 6 个维度的因子研发。

## 二、数据源分类与处理范式

### 2.1 三类数据的因子化方法

| 类型 | 特征 | 处理方式 | 无数据股票 |
|------|------|---------|-----------|
| **部分覆盖型** | 固定子集有数据（融资融券~4300只） | 直接计算，覆盖外填 NaN | CSZScoreNorm → 截面均值(0) |
| **稀疏事件型** | 每天少量股票触发（涨停~96只/日） | 滚动聚合 + 指数衰减 | 填 0（无事件=中性） |
| **低频事件型** | 极稀疏高信息量（业绩预告~4只/日） | forward-fill + 长衰减(30-90天) | 填 0 |

### 2.2 通用因子工程模式

```python
# 模式A: 部分覆盖 → 直接转日频因子
factor[stock] = compute(data[stock])  # 有数据的股票
factor[uncovered] = NaN               # 无数据的股票，CSZScoreNorm处理

# 模式B: 稀疏事件 → 滚动聚合
factor = rolling_sum(event_flag, window=20)      # 20日事件次数
factor = rolling_sum(event_value * exp_decay, 20) # 带衰减的事件值

# 模式C: 低频事件 → forward-fill + 衰减
factor = event_signal * max(0, 1 - days_since / decay_window)
```

## 三、数据源详情与优先级

### P0（第一批）: 日频高覆盖

#### 3.1 margin_detail（融资融券明细）
- **接口**: `margin_detail`，积分要求 2000，单次限量 6000 行
- **查询模式**: BY_DATE（按交易日拉取）
- **覆盖**: ~4300只/日，占活跃股 >95%
- **字段**: trade_date, ts_code, rzye(融资余额), rqye(融券余额), rzmre(融资买入额), rqyl(融券余量), rzche(融资偿还额), rqchl(融券偿还量), rqmcl(融券卖出量), rzrqye(融资融券余额)
- **NULL率**: 全部 0%
- **因子方向**:
  - `md_rzye_chg_Nd`: 融资余额N日变化率（N=5,10,20）
  - `md_rzmre_ratio`: 融资买入额/融资余额（杠杆意愿）
  - `md_rqyl_chg_Nd`: 融券余量N日变化率（看空信号）
  - `md_rzrq_balance_chg`: 融资融券余额净变化
  - `md_margin_sentiment`: (融资买入-融券卖出)/(融资买入+融券卖出)

#### ~~3.2 hk_hold（北向资金持股明细）~~
- **重要**: 港交所从 2024-08-20 起停止发布日度北向持股数据，改为季度披露
- **影响**: out_sample 期间(2024-07-01~)仅有约 1.5 个月日度数据，后续为空
- **结论**: **暂不入库**，等待数据恢复或使用替代方案（moneyflow_hsgt 市场级数据仍有日度更新）
- **替代**: `moneyflow_hsgt` 提供沪深港通每日资金净流入（市场级，非个股级），可作为市场情绪因子

### P1（第二批）: 稀疏事件型

#### 3.3 limit_list_d（涨跌停统计）
- **接口**: `limit_list_d`，积分要求较高
- **查询模式**: BY_DATE
- **覆盖**: ~96只/日（仅当日有涨跌停的股票）
- **字段(18)**: trade_date, ts_code, industry, name, close, pct_chg, amount, limit_amount, float_mv, total_mv, turnover_ratio, fd_amount, first_time, last_time, open_times, up_stat, limit_times, limit(U涨停/D跌停/Z炸板)
- **因子方向**:
  - `ld_up_count_Nd`: 过去N日涨停次数
  - `ld_down_count_Nd`: 过去N日跌停次数
  - `ld_consec_up`: 当前连板天数
  - `ld_open_times_avg`: 涨停开板次数均值（情绪强度）
  - `ld_limit_turnover`: 涨停日换手率（封板资金）

#### 3.4 top_list / top_inst（龙虎榜）
- **接口**: `top_list`(每日明细) + `top_inst`(机构明细)
- **覆盖**: ~51只/日
- **因子方向**:
  - `tl_net_buy_Nd`: 龙虎榜净买入金额N日累计/流通市值
  - `tl_inst_net_Nd`: 机构席位净买入N日累计/流通市值
  - `tl_appear_count_Nd`: N日内上榜次数

#### 3.5 block_trade（大宗交易）
- **接口**: `block_trade`
- **覆盖**: ~58只/日
- **因子方向**:
  - `bt_vol_Nd`: N日大宗交易量/日均成交量
  - `bt_discount_Nd`: N日大宗交易折价率均值

#### 3.6 forecast（业绩预告）
- **接口**: `forecast`，按公告日查询
- **覆盖**: ~4只/日（极稀疏）
- **因子方向**:
  - `fc_signal_decay60`: 业绩预告方向信号(+预增/-预减) × 60日衰减
  - `fc_pchg_mid_decay60`: 预告变动幅度中值 × 60日衰减
  - `fc_surprise`: 预告利润偏离上年利润

#### 3.7 stk_holdertrade（股东增减持）
- **接口**: `stk_holdertrade`，按公告日查询
- **覆盖**: ~12只/日
- **因子方向**:
  - `ht_net_change_decay30`: 增减持净额 × 30日衰减 / 总股本
  - `ht_holder_type_signal`: 高管/机构/个人分类权重

#### 3.8 repurchase（股票回购）
- **接口**: `repurchase`
- **覆盖**: ~23只/日
- **因子方向**:
  - `rp_amount_ratio`: 回购金额/总市值 × 衰减

### P2（第三批）: 季频财务

#### 3.9 fina_indicator（财务指标）
- **接口**: `fina_indicator`，108个字段
- **查询模式**: BY_CODE（按股票逐只拉取）
- **处理**: forward-fill 至下次公告日
- **因子方向（高价值）**:
  - `fi_roe_chg`: ROE 季度环比变化
  - `fi_ocfps_to_eps`: 经营现金流/EPS 比率（盈利质量）
  - `fi_revenue_yoy_accel`: 营收同比增速的加速度
  - `fi_debt_to_assets_chg`: 杠杆率变化
  - `fi_gross_margin_chg`: 毛利率变化

#### 3.10 report_rc（分析师一致预期）
- **接口**: `report_rc`，按股票查询
- **覆盖**: 主要覆盖大中盘股
- **因子方向**:
  - `rc_eps_consensus`: 一致预期EPS
  - `rc_rating_score`: 评级量化分数（买入=5, 增持=4, ...）
  - `rc_target_premium`: 目标价/当前价溢价率

### P3（低优先级）

| 接口 | 原因 | 备注 |
|------|------|------|
| stk_holdernumber | 更新极慢（非日频） | 筹码集中度，可后续考虑 |
| pledge_stat | 按周更新 | 质押风险，信号弱 |
| share_float | 事件极稀疏 | 解禁预期，可后续考虑 |
| dividend | bak_basic 已有股息率 | 重复度高 |
| income/cashflow | 与 fina_indicator 重叠 | fina_indicator 更精炼 |
| stk_factor | 我们已有类似技术指标 | MACD/KDJ/RSI/BOLL 重复 |
| moneyflow_hsgt | 市场级非个股级 | 可作为大盘情绪因子，非个股因子 |
| stk_account | API返回空数据 | 积分不足或已停用 |

## 四、实施步骤

### 阶段1: 数据入库（P0）

| 步骤 | 数据集 | 工作内容 | 预计 |
|------|--------|---------|------|
| 1.1 | margin_detail | 建表 + DatasetSpec + 调度 + 路由 + 前端 | add-tushare-dataset skill |
| 1.2 | margin_detail | h5 导出器（db_reader + exporter + router + 前端） | qlib_exporter 模块 |
| 1.3 | margin_detail | 全量数据拉取（init，从 2018-08-01 起） | 前端操作 |
| 1.4 | margin_detail | h5 导出 → `margin_detail.h5` | 前端操作 |

### 阶段2: 因子研发（基于 margin_detail h5）

| 步骤 | 工作内容 |
|------|---------|
| 2.1 | 设计 5-8 个融资融券因子 |
| 2.2 | WSL 验证 + 入库 + 4窗口指标 + LLM分类 |
| 2.3 | IC 筛选，保留 |IC|≥0.02 的因子 |

### 阶段3: 数据入库（P1 稀疏事件型）

| 步骤 | 数据集 | 备注 |
|------|--------|------|
| 3.1 | limit_list_d | 涨跌停统计 → `limit_list.h5` |
| 3.2 | top_list + top_inst | 龙虎榜 → `top_list.h5` |
| 3.3 | block_trade | 大宗交易 → `block_trade.h5` |
| 3.4 | forecast | 业绩预告 → 需特殊 forward-fill 逻辑 |
| 3.5 | stk_holdertrade | 股东增减持 |

### 阶段4: 因子研发（P1 事件因子）

| 步骤 | 工作内容 |
|------|---------|
| 4.1 | 涨跌停 + 龙虎榜因子（滚动聚合模式） |
| 4.2 | 业绩预告 + 增减持因子（forward-fill + 衰减模式） |
| 4.3 | IC 筛选 |

### 阶段5: 数据入库 + 因子研发（P2 财务）

| 步骤 | 工作内容 |
|------|---------|
| 5.1 | fina_indicator 入库（BY_CODE 模式，108字段） |
| 5.2 | report_rc 入库 |
| 5.3 | 财务类因子研发 |

## 五、h5 文件命名与前缀规范

| h5 文件 | 列前缀 | 来源表 |
|---------|--------|--------|
| `margin_detail.h5` | `md_` | `market.margin_detail` |
| `limit_list.h5` | `ld_` | `market.limit_list_d` |
| `top_list.h5` | `tl_` | `market.top_list` + `market.top_inst` |
| `block_trade.h5` | `bt_` | `market.block_trade` |
| `fina_indicator.h5` | `fi_` | `market.fina_indicator` |

## 六、hk_hold 数据中断说明

港交所从 **2024-08-20** 起停止发布日度北向持股数据，改为季度披露。

- 日度数据可用范围: ~2017-03-17 至 2024-08-19
- out_sample 期间(2024-07-01~) 仅 ~35 个交易日有数据
- **决策: 暂缓入库**，等数据恢复或有替代方案后再评估
- 可考虑 `moneyflow_hsgt`（市场级北向资金净流入）作为临时替代

## 七、风险与注意事项

1. **Tushare 积分**: 部分接口需要较高积分（fina_indicator 需 5000 分）
2. **流控**: BY_CODE 模式拉取 fina_indicator 需要遍历 ~5000 只股票，需合理设置 batch_sleep
3. **数据回填**: margin_detail 从 2018 年起有数据，需全量回填约 1800 个交易日
4. **稀疏因子 IC**: 稀疏事件因子的 IC 通常偏低（0.01-0.02），但可能具有独特 alpha（与现有因子低相关）
5. **forward-fill 时效**: 财务数据 forward-fill 需注意公告日 vs 报告期的区别，避免前视偏差
