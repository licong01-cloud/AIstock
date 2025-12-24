# 策略配置指南

> **说明**: 本文档详细说明策略配置页面中各个 JSON 配置字段的含义和配置方法。

---

## 配置字段说明

策略配置页面包含以下三个 JSON 配置文本框：

1. **策略配置 (config_json)** - 必填，策略的核心参数
2. **调度配置 (schedule_config)** - 可选，控制策略的执行时间
3. **风控配置 (risk_config)** - 可选，策略的风控参数

---

## 1. 策略配置 (config_json)

### 双均线策略 (MA_CROSS)

**配置示例（日线）**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "1d",
  "symbols": ["600519.SH", "000001.SZ"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

**配置示例（15分钟线，日内交易）**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "15m",
  "symbols": ["600519.SH"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

**字段说明**：
- `ma_short`: 短期均线周期（默认：5）
- `ma_long`: 长期均线周期（默认：20）
- `period`: 数据周期（默认：`"1d"`，支持 `"15m"`, `"1m"`, `"5m"`, `"30m"`, `"1d"` 等）
  - `"15m"`: 15分钟线（日内交易）
  - `"1d"`: 日线（日频交易）
- `symbols`: 股票代码列表（格式：`["600519.SH", "000001.SZ"]`）
  - 上海股票：`600519.SH`
  - 深圳股票：`000001.SZ`
- `position_size`: 仓位大小（0-1，默认：0.1，即10%）
- `price_type`: 价格类型（`"LIMIT"` 限价 或 `"MARKET"` 市价）

**完整示例（日线）**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "1d",
  "symbols": [
    "600519.SH",
    "000001.SZ",
    "600036.SH",
    "000002.SZ"
  ],
  "position_size": 0.15,
  "price_type": "LIMIT"
}
```

**完整示例（15分钟线，日内交易）**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "15m",
  "symbols": ["600519.SH"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

### 趋势跟踪策略 (TREND_FOLLOWING)

**配置示例**：
```json
{
  "ma_period": 20,
  "volume_ratio": 1.5,
  "symbols": ["600519.SH", "000001.SZ"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

**字段说明**：
- `ma_period`: 均线周期（默认：20）
- `volume_ratio`: 成交量放大倍数（默认：1.5，即成交量需要是均量的1.5倍才买入）
- `symbols`: 股票代码列表
- `position_size`: 仓位大小（0-1，默认：0.1）
- `price_type`: 价格类型（`"LIMIT"` 或 `"MARKET"`）

**完整示例**：
```json
{
  "ma_period": 30,
  "volume_ratio": 2.0,
  "symbols": [
    "600519.SH",
    "000001.SZ"
  ],
  "position_size": 0.2,
  "price_type": "MARKET"
}
```

---

## 2. 调度配置 (schedule_config)

**配置示例**：
```json
{
  "type": "daily",
  "time": "09:30"
}
```

**字段说明**：
- `type`: 调度类型
  - `"daily"` - 每日执行（需要 `time` 字段）
  - `"hourly"` - 每小时执行（需要 `time` 字段）
  - `"minute"` - 每分钟执行（需要 `interval` 字段）
- `time`: 执行时间（格式：`"HH:MM"`，如 `"09:30"`）
- `interval`: 执行间隔（分钟，仅用于 `minute` 类型）

**配置示例**：

#### 每日执行（推荐）
```json
{
  "type": "daily",
  "time": "09:30"
}
```
说明：每个交易日 09:30 执行

#### 每日多次执行
```json
{
  "type": "daily",
  "time": "14:30"
}
```
说明：每个交易日 14:30 执行

#### 每小时执行
```json
{
  "type": "hourly",
  "time": ":00"
}
```
说明：每小时的第0分钟执行（如 09:00, 10:00, 11:00...）

#### 每分钟执行（测试用）
```json
{
  "type": "minute",
  "interval": 5
}
```
说明：每5分钟执行一次（仅用于测试）

**不配置调度**：
如果不需要自动调度，可以留空或填写 `{}`，策略只能通过 API 手动触发。

---

## 3. 风控配置 (risk_config)

**配置示例**：
```json
{
  "max_position_pct": 0.2,
  "max_total_position_pct": 0.8,
  "max_risk_per_trade": 0.02
}
```

**字段说明**：
- `max_position_pct`: 单个股票最大仓位比例（0-1，默认：0.2，即20%）
- `max_total_position_pct`: 总仓位最大比例（0-1，默认：0.8，即80%）
- `max_risk_per_trade`: 单笔交易最大风险比例（0-1，默认：0.02，即2%）

**完整示例**：
```json
{
  "max_position_pct": 0.15,
  "max_total_position_pct": 0.75,
  "max_risk_per_trade": 0.01
}
```

**不配置风控**：
如果不需要额外风控配置，可以留空或填写 `{}`，将使用基础风控（资金检查、持仓检查）。

---

## 完整配置示例

### 示例1：双均线策略（每日执行，日线）

**策略配置**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "1d",
  "symbols": ["600519.SH", "000001.SZ"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

**调度配置**：
```json
{
  "type": "daily",
  "time": "09:30"
}
```

**风控配置**：
```json
{
  "max_position_pct": 0.2,
  "max_total_position_pct": 0.8
}
```

### 示例2：双均线策略（15分钟线，日内交易）

**策略配置**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "15m",
  "symbols": ["600519.SH"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

**调度配置**：
```json
{
  "type": "minute",
  "interval": 15
}
```
说明：每15分钟执行一次，适合日内交易

**风控配置**：
```json
{
  "max_position_pct": 0.2
}
```

### 示例3：趋势跟踪策略（每小时执行）

**策略配置**：
```json
{
  "ma_period": 20,
  "volume_ratio": 1.5,
  "symbols": ["600519.SH"],
  "position_size": 0.15,
  "price_type": "MARKET"
}
```

**调度配置**：
```json
{
  "type": "hourly",
  "time": ":00"
}
```

**风控配置**：
```json
{}
```

---

## 配置步骤

### 方式1：通过前端UI配置（推荐）

1. 访问 `http://localhost:3000/qmt/strategies`
2. 点击"新建策略"
3. 填写基本信息：
   - 策略ID：`ma_cross_001`
   - 策略名称：`双均线策略`
   - 策略类型：选择 `双均线策略`
   - 描述：`MA5/MA20交叉策略`
4. 在"策略配置 (JSON)"文本框中粘贴配置：
   ```json
   {
     "ma_short": 5,
     "ma_long": 20,
     "symbols": ["600519.SH", "000001.SZ"],
     "position_size": 0.1,
     "price_type": "LIMIT"
   }
   ```
5. 在"调度配置 (JSON)"文本框中粘贴配置：
   ```json
   {
     "type": "daily",
     "time": "09:30"
   }
   ```
6. 在"风控配置 (JSON)"文本框中粘贴配置（可选）：
   ```json
   {
     "max_position_pct": 0.2
   }
   ```
7. 点击"保存"

### 方式2：通过API配置

```bash
curl -X POST "http://127.0.0.1:8001/api/v1/strategies/config" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "ma_cross_001",
    "strategy_name": "双均线策略",
    "strategy_type": "MA_CROSS",
    "description": "MA5/MA20交叉策略",
    "enabled": true,
    "config_json": {
      "ma_short": 5,
      "ma_long": 20,
      "symbols": ["600519.SH", "000001.SZ"],
      "position_size": 0.1,
      "price_type": "LIMIT"
    },
    "schedule_config": {
      "type": "daily",
      "time": "09:30"
    },
    "risk_config": {
      "max_position_pct": 0.2
    }
  }'
```

---

## 常见问题

### Q1: 股票代码格式是什么？
A: 格式为 `{代码}.{市场}`，例如：
- 上海：`600519.SH`
- 深圳：`000001.SZ`

### Q2: 如何添加多个股票？
A: 在 `symbols` 数组中添加多个股票代码：
```json
{
  "symbols": ["600519.SH", "000001.SZ", "600036.SH"]
}
```

### Q3: 仓位大小如何设置？
A: `position_size` 是 0-1 之间的数字：
- `0.1` = 10% 仓位
- `0.2` = 20% 仓位
- `0.5` = 50% 仓位

### Q4: 调度时间如何设置？
A: 
- 每日执行：`{"type": "daily", "time": "09:30"}`
- 每小时执行：`{"type": "hourly", "time": ":00"}`
- 每5分钟执行：`{"type": "minute", "interval": 5}`

### Q5: JSON 格式错误怎么办？
A: 
- 确保使用双引号 `"`，不要使用单引号 `'`
- 确保最后一个字段后没有逗号
- 可以使用在线 JSON 验证工具检查格式

### Q6: 可以不配置调度吗？
A: 可以，留空或填写 `{}`，策略只能通过 API 手动触发。

---

## 配置模板

### 双均线策略模板

```json
{
  "ma_short": 5,
  "ma_long": 20,
  "symbols": ["600519.SH"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

### 趋势跟踪策略模板

```json
{
  "ma_period": 20,
  "volume_ratio": 1.5,
  "symbols": ["600519.SH"],
  "position_size": 0.1,
  "price_type": "LIMIT"
}
```

### 调度配置模板

```json
{
  "type": "daily",
  "time": "09:30"
}
```

### 风控配置模板

```json
{
  "max_position_pct": 0.2,
  "max_total_position_pct": 0.8,
  "max_risk_per_trade": 0.02
}
```

---

**提示**: 不需要手写脚本，直接在文本框中粘贴 JSON 配置即可。建议先使用简单的配置测试，确认无误后再添加更多股票或调整参数。

