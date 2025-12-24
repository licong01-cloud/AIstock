# XtQuant 数据集清单

基于 XtQuant (迅投 MiniQMT) 可获取的数据集目录，包括字段、精度、实时性等信息，为 AIstock 统一数据服务层设计提供参考。

## 数据源概述

- **主要数据源**: MiniQMT 客户端
- **备用数据源**: TDX API、本地数据库、Tushare
- **数据格式**: Pandas DataFrame、NumPy arrays
- **连接方式**: 通过 xtquant 库建立与 MiniQMT 的连接

## 1. 行情数据 (Market Data)

### 1.1 K线数据
**接口**: `get_market_data()`, `download_history_data()`

| 字段名 | 数据类型 | 精度 | 说明 | 实时性 |
|--------|----------|------|------|--------|
| time | datetime | 日 | 时间戳 | 实时/历史 |
| open | float | 4位小数 | 开盘价 | 实时 |
| high | float | 4位小数 | 最高价 | 实时 |
| low | float | 4位小数 | 最低价 | 实时 |
| close | float | 4位小数 | 收盘价 | 实时 |
| volume | int | 股 | 成交量 | 实时 |
| amount | float | 2位小数 | 成交额 | 实时 |
| pre_close | float | 4位小数 | 前收盘价 | 实时 |
| suspend_flag | int | - | 停牌标记 | 实时 |

**支持周期**: 
- Level1: `tick`, `1m`, `5m`, `15m`, `30m`, `1h`, `1d`, `1w`, `1mon`, `1q`, `1hy`, `1y`
- Level2: `l2quote`, `l2order`, `l2transaction`

**复权方式**: `none`, `front`, `back`, `front_ratio`, `back_ratio`

### 1.2 分笔数据 (Tick Data)
**接口**: `get_full_tick()`

| 字段名 | 数据类型 | 精度 | 说明 | 实时性 |
|--------|----------|------|------|--------|
| time | datetime | 毫秒 | 时间戳 | 实时 |
| price | float | 4位小数 | 成交价 | 实时 |
| volume | int | 股 | 成交量 | 实时 |
| amount | float | 2位小数 | 成交额 | 实时 |
| direction | int | - | 买卖方向 | 实时 |

### 1.3 Level2 深度数据
**接口**: `get_l2_quote()`, `get_l2_order()`, `get_l2_transaction()`

| 字段名 | 数据类型 | 精度 | 说明 | 实时性 |
|--------|----------|------|------|--------|
| ask_price | float | 4位小数 | 卖价 | 实时 |
| bid_price | float | 4位小数 | 买价 | 实时 |
| ask_volume | int | 股 | 卖量 | 实时 |
| bid_volume | int | 股 | 买量 | 实时 |

## 2. 财务数据 (Financial Data)

### 2.1 基本财务报表
**接口**: `get_financial_data()`, `download_financial_data()`

**支持报表类型**:
- `report_time`: 按报告期
- `announce_time`: 按公布期

**主要财务表**:
| 表名 | 说明 | 主要字段 |
|------|------|----------|
| Balance | 资产负债表 | total_assets, total_liabilities, net_assets |
| Income | 利润表 | total_revenue, net_profit, operating_profit |
| CashFlow | 现金流量表 | operating_cash_flow, investing_cash_flow, financing_cash_flow |

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| period | string | - | 报告期 | 季度 |
| net_profit | float | 2位小数 | 净利润 | 季度 |
| total_revenue | float | 2位小数 | 营业总收入 | 季度 |
| total_assets | float | 2位小数 | 总资产 | 季度 |
| net_assets | float | 2位小数 | 净资产 | 季度 |
| eps | float | 4位小数 | 每股收益 | 季度 |
| roe | float | 4位小数 | 净资产收益率 | 季度 |

## 3. 基础信息 (Basic Information)

### 3.1 合约基础信息
**接口**: `get_instrument_detail()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| InstrumentID | string | - | 合约代码 | 静态 |
| InstrumentName | string | - | 合约名称 | 静态 |
| ExchangeID | string | - | 交易所代码 | 静态 |
| ProductClass | string | - | 产品类别 | 静态 |
| VolumeMultiple | int | - | 合约乘数 | 静态 |
| PriceTick | float | 4位小数 | 最小变动价位 | 静态 |
| CreateDate | string | - | 创建日期 | 静态 |
| OpenDate | string | - | 上市日期 | 静态 |
| ExpireDate | string | - | 到期日期 | 静态 |
| TotalShare | float | 0位小数 | 总股本 | 动态 |
| FloatShare | float | 0位小数 | 流通股本 | 动态 |

### 3.2 除权除息信息
**接口**: `get_divid_factors()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| time | datetime | 日 | 除权除息日 | 事件驱动 |
| cash_dividend | float | 4位小数 | 现金分红 | 事件驱动 |
| share_dividend | float | 4位小数 | 送股比例 | 事件驱动 |
| share_ratio | float | 4位小数 | 配股比例 | 事件驱动 |

## 4. 板块行业数据 (Sector Data)

### 4.1 板块分类
**接口**: `get_sector_list()`, `get_stock_list_in_sector()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| sector_name | string | - | 板块名称 | 月度 |
| stock_code | string | - | 成分股代码 | 月度 |
| weight | float | 4位小数 | 权重 | 月度 |

**支持板块类型**:
- 行业板块
- 概念板块
- 地域板块
- 指数成分股

### 4.2 指数权重
**接口**: `get_index_weight()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| index_code | string | - | 指数代码 | 日度 |
| constituent_code | string | - | 成分股代码 | 日度 |
| weight | float | 4位小数 | 权重 | 日度 |

## 5. 交易日历 (Trading Calendar)

### 5.1 交易日历
**接口**: `get_trading_dates()`, `get_trading_calendar()`, `get_holidays()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| date | datetime | 日 | 日期 | 年度 |
| is_trading | bool | - | 是否交易日 | 年度 |
| market | string | - | 市场代码 | 年度 |

## 6. 特色数据 (Special Data)

### 6.1 投研版特色数据
**需要投研版权限**

| 数据类型 | 接口 | 说明 | 更新频率 |
|----------|------|------|----------|
| 期货仓单 | `warehousereceipt` | 期货仓单数据 | 日度 |
| 期货席位 | `futureholderrank` | 期货持仓排名 | 日度 |
| 资金流向 | `northfinancechange1d` | 港股通资金流向 | 日度 |
| 涨跌停数据 | `stoppricedata` | 涨跌停统计 | 日度 |
| 无风险利率 | `riskfreerate` | 无风险利率 | 日度 |

### 6.2 ETF 数据
**接口**: `get_etf_info()`, `download_etf_info()`

| 字段名 | 数据类型 | 精度 | 说明 | 更新频率 |
|--------|----------|------|------|----------|
| etf_code | string | - | ETF代码 | 静态 |
| creation_unit | int | 股 | 申赎单位 | 静态 |
| cash_component | float | 2位小数 | 现金替代 | 实时 |

## 7. 数据获取优先级

为统一数据服务层设计建议的数据源优先级：

1. **实时数据**: 
   - 首选: MiniQMT (Level1/Level2)
   - 备选: TDX API
   - 兜底: 本地数据库

2. **历史数据**:
   - 首选: MiniQMT 本地缓存
   - 备选: 本地数据库
   - 兜底: Tushare

3. **特色数据**:
   - 首选: Tushare (如仅其提供)
   - 备选: MiniQMT 投研版
   - 兜底: 手动补充

## 8. 数据标准化建议

### 8.1 字段命名规范
- 统一使用小写字母和下划线: `stock_code`, `close_price`
- 时间字段统一为 `datetime` 类型
- 价格字段统一精度为4位小数
- 数量字段统一为整数

### 8.2 数据质量检查
- 价格数据合理性检查 (非负、非零)
- 成交量与成交额一致性检查
- 时间序列连续性检查
- 除权除息数据完整性检查

### 8.3 存储优化
- 按股票代码分区存储
- 按时间范围压缩存储
- 建立适当的索引策略
- 实现增量更新机制

## 9. 接口使用示例

```python
import xtquant.xtdata as xtdata

# 连接MiniQMT
xtdata.connect()

# 获取K线数据
kline_data = xtdata.get_market_data(
    field_list=['open', 'high', 'low', 'close', 'volume'],
    stock_list=['000001.SZ', '600000.SH'],
    period='1d',
    start_time='20240101',
    end_time='20241231'
)

# 获取财务数据
financial_data = xtdata.get_financial_data(
    stock_list=['000001.SZ'],
    table_list=['Balance', 'Income'],
    start_time='20240101',
    end_time='20241231'
)

# 获取交易日历
trading_dates = xtdata.get_trading_dates(
    market='SH',
    start_time='20240101',
    end_time='20241231'
)
```

## 10. 注意事项

1. **连接要求**: 使用前需启动 MiniQMT 客户端
2. **数据权限**: 某些特色数据需要投研版权限
3. **数据完整性**: 建议先检查数据完整性再使用
4. **性能考虑**: 大量历史数据获取建议分批进行
5. **实时性**: Level2 数据仅当日有效，跨日清空

此清单为 AIstock 统一数据服务层的设计提供了完整的数据源参考，确保能够满足各种策略的数据需求。
