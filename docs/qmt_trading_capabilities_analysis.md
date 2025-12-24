# QMT/xtquant 股票交易功能分析

## 概述

基于 `xtquant` 文档分析，AIstock 可以通过 `xtquant` 实现完整的股票交易功能。`xtquant` 提供了丰富的交易 API，支持股票买卖、撤单、查询等核心功能。

## 已实现功能

### 1. 查询功能 ✅
- ✅ **资金查询** (`query_stock_asset`) - 已实现
- ✅ **持仓查询** (`query_stock_positions`) - 已实现
- ✅ **委托查询** (`query_stock_orders`) - 已实现
- ✅ **成交查询** (`query_stock_trades`) - 已实现

## 可实现的交易功能

### 2. 股票下单功能 ⭐

#### 2.1 同步下单
```python
order_stock(account, stock_code, order_type, order_volume, price_type, price, strategy_name, order_remark)
```

**参数说明：**
- `account`: StockAccount 资金账号
- `stock_code`: 证券代码，如 `'600000.SH'` 或 `'000001.SZ'`
- `order_type`: 委托类型
  - `xtconstant.STOCK_BUY` (23) - 买入
  - `xtconstant.STOCK_SELL` (24) - 卖出
- `order_volume`: 委托数量（股为单位）
- `price_type`: 报价类型
  - `xtconstant.FIX_PRICE` - 限价
  - `xtconstant.LATEST_PRICE` - 最新价
  - `xtconstant.MARKET_PEER_PRICE_FIRST` - 对手方最优价格委托
  - `xtconstant.MARKET_MINE_PRICE_FIRST` - 本方最优价格委托
  - `xtconstant.MARKET_SH_CONVERT_5_CANCEL` - 最优五档即时成交剩余撤销（上交所）
  - `xtconstant.MARKET_SZ_INSTBUSI_RESTCANCEL` - 即时成交剩余撤销委托（深交所）
- `price`: 委托价格（限价时填写，市价时填 0）
- `strategy_name`: 策略名称（可选）
- `order_remark`: 委托备注（可选）

**返回值：**
- 成功：订单编号（大于 0 的正整数）
- 失败：-1

#### 2.2 异步下单
```python
order_stock_async(account, stock_code, order_type, order_volume, price_type, price, strategy_name, order_remark)
```

**特点：**
- 返回下单请求序号 `seq`
- 通过回调 `on_order_stock_async_response` 接收下单结果
- 适合高频交易场景

### 3. 撤单功能 ⭐

#### 3.1 根据订单编号撤单
```python
cancel_order_stock(account, order_id)  # 同步
cancel_order_stock_async(account, order_id)  # 异步
```

#### 3.2 根据柜台合同编号撤单
```python
cancel_order_stock_sysid(account, market, order_sysid)  # 同步
cancel_order_stock_sysid_async(account, market, order_sysid)  # 异步
```

**参数说明：**
- `order_id`: 下单接口返回的订单编号
- `market`: 交易市场
  - `xtconstant.SH_MARKET` (0) - 上海
  - `xtconstant.SZ_MARKET` (1) - 深圳
- `order_sysid`: 券商柜台返回的合同编号

**返回值：**
- 成功：0
- 失败：-1

### 4. 其他功能

#### 4.1 订阅/反订阅账号信息
```python
subscribe(account)      # 订阅，接收资金、委托、成交、持仓变动推送
unsubscribe(account)    # 反订阅
```

#### 4.2 资金划拨
```python
fund_transfer(account, transfer_direction, price)
```

#### 4.3 新股申购（可选）
```python
query_new_purchase_limit(account)  # 查询新股申购额度
query_ipo_data()                   # 查询新股信息
```

#### 4.4 银证转账（可选）
```python
bank_transfer_in(account, bank_no, bank_account, bank_pwd, amount)   # 银行转证券
bank_transfer_out(account, bank_no, bank_account, bank_pwd, amount)  # 证券转银行
query_bank_info(account)           # 查询银行信息
query_bank_transfer_stream(...)   # 查询转账流水
```

## 实现建议

### 阶段一：核心交易功能（推荐优先实现）

1. **股票买入/卖出下单**
   - 支持限价单和市价单
   - 支持上交所和深交所
   - 前端提供下单表单（股票代码、数量、价格类型、价格）

2. **撤单功能**
   - 在委托列表中显示"撤单"按钮
   - 支持根据订单编号撤单

3. **委托状态实时更新**
   - 利用订阅功能接收委托状态推送
   - 自动更新委托列表状态

### 阶段二：增强功能（可选）

1. **批量下单**
   - 支持多只股票批量下单
   - 支持条件单（价格触发）

2. **智能算法下单**
   - `smart_algo_order_async()` - 智能算法下单
   - `query_smart_algo_task()` - 查询智能算法任务

3. **风控功能**
   - 单笔最大金额限制
   - 单日交易限额
   - 持仓集中度控制

### 阶段三：高级功能（可选）

1. **新股申购**
2. **银证转账**
3. **资金划拨**

## 技术实现要点

### 1. 后端 API 设计

**新增接口：**
- `POST /api/v1/qmt/order` - 下单
  - 请求体：`{stock_code, order_type, order_volume, price_type, price, strategy_name?, order_remark?}`
  - 返回：`{success, order_id, message}`

- `POST /api/v1/qmt/cancel` - 撤单
  - 请求体：`{order_id}` 或 `{market, order_sysid}`
  - 返回：`{success, message}`

### 2. 前端 UI 设计

**下单表单：**
- 股票代码输入（自动补全）
- 买卖方向选择（买入/卖出）
- 数量输入
- 价格类型选择（限价/市价）
- 价格输入（限价时显示）
- 策略名称/备注（可选）

**委托列表增强：**
- 显示撤单按钮（仅可撤委托）
- 实时更新委托状态
- 显示委托详情（价格、数量、成交情况）

### 3. 错误处理

- 下单失败：返回错误码和错误信息
- 撤单失败：提示失败原因
- 连接断开：自动重连或提示用户

### 4. 安全考虑

- **模拟盘优先**：初期仅在模拟盘实现
- **风控检查**：下单前检查资金、持仓、限额
- **操作确认**：重要操作（下单、撤单）需要用户确认
- **日志记录**：记录所有交易操作

## 代码示例

### 后端下单接口示例

```python
@router.post("/order", summary="股票下单")
async def place_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        stock_code = payload.get("stock_code")
        order_type = payload.get("order_type")  # 23=买, 24=卖
        order_volume = payload.get("order_volume")
        price_type = payload.get("price_type")
        price = payload.get("price", 0.0)
        strategy_name = payload.get("strategy_name", "")
        order_remark = payload.get("order_remark", "")
        
        order_id = _client.place_order(
            stock_code=stock_code,
            order_type=order_type,
            order_volume=order_volume,
            price_type=price_type,
            price=price,
            strategy_name=strategy_name,
            order_remark=order_remark,
        )
        
        if order_id > 0:
            return {"success": True, "order_id": order_id, "message": "下单成功"}
        else:
            return {"success": False, "order_id": -1, "message": "下单失败"}
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e))
```

### 前端下单表单示例

```typescript
// 下单表单组件
const [stockCode, setStockCode] = useState("");
const [orderType, setOrderType] = useState<23 | 24>(23); // 买入
const [orderVolume, setOrderVolume] = useState(100);
const [priceType, setPriceType] = useState("FIX_PRICE"); // 限价
const [price, setPrice] = useState(0);

async function handlePlaceOrder() {
  const res = await fetch(`${API_BASE}/qmt/order`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      stock_code: stockCode,
      order_type: orderType,
      order_volume: orderVolume,
      price_type: priceType,
      price: priceType === "FIX_PRICE" ? price : 0,
    }),
  });
  const data = await res.json();
  if (data.success) {
    alert(`下单成功，订单编号：${data.order_id}`);
  } else {
    alert(`下单失败：${data.message}`);
  }
}
```

## 总结

**xtquant 完全支持在 AIstock 侧实现股票交易功能**，包括：

✅ **核心功能**：
- 股票买入/卖出下单（限价/市价）
- 撤单功能
- 委托/成交/持仓/资金查询（已实现）

✅ **可选功能**：
- 订阅推送（实时更新）
- 批量下单
- 智能算法下单
- 新股申购
- 银证转账

**建议实现顺序**：
1. 先实现下单和撤单功能（核心）
2. 再添加订阅推送（实时更新）
3. 最后考虑高级功能（新股、转账等）

**安全建议**：
- 初期仅在模拟盘实现
- 添加风控检查
- 重要操作需要确认
- 记录操作日志

