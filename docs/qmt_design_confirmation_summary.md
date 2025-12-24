# QMT 交易系统设计方案确认总结

> **日期**: 2024-12-20  
> **状态**: ✅ 所有需求已明确，设计方案已完成

---

## 一、需求确认清单

### ✅ 1. 调度库选择
- **使用**: `schedule` 库（与现有程序 `tdx_scheduler.py` 一致）
- **参考**: `backend/ingestion/tdx_scheduler.py`

### ✅ 2. 策略执行方式
- **API 触发**: 通过 `/api/v1/strategies/execute` 手动触发 ✅
- **定时任务**: 使用 `schedule` 库实现 ✅
- **事件驱动**: 未来实现 ⏳

### ✅ 3. 策略实现优先级
- **优先实现**: 
  1. 双均线策略（`MACrossStrategy`）
  2. 趋势跟踪策略（`TrendFollowingStrategy`）
- **后续实现**: 网格交易策略等其他策略

### ✅ 4. 策略基类
- **需要实现**: 是 ✅
- **价值**: 统一接口、代码复用、类型安全、易于扩展、测试友好
- **参考文档**: `docs/strategy_base_class_explanation.md`

### ✅ 5. 策略管理UI
- **需要实现**: 是 ✅
- **功能**: 策略配置管理、启用/禁用、参数修改、执行记录查询

### ✅ 6. 数据库持久化
- **需要实现**: 是 ✅
- **持久化内容**:
  - 策略配置（`app.strategy_config`）
  - 交易意图（`app.trade_intent`）- 用于幂等性保护
  - 策略执行记录（`app.strategy_execution`）
  - 策略性能统计（`app.strategy_performance`）- 可选

### ✅ 7. 策略配置管理
- **方案**: 策略配置固化到数据库 ✅
- **优势**: 
  - 动态修改参数（无需重启）
  - 版本管理和回滚
  - 多策略实例管理
  - 策略启用/禁用控制

### ✅ 8. 未来演进
- **需要演进到完整框架**: 是 ✅
- **未来集成**: RDagent 多因子策略（本地运行）
- **现阶段**: 仅实现基础策略，RDagent 集成暂不开发

---

## 二、核心设计决策

### 1. 架构设计
- **当前**: 直连方式（AIstock 进程内 import xtquant）
- **未来**: 可演进到独立 Gateway 服务
- **封装**: 所有 xtquant 调用集中在 `qmt_client.py`

### 2. 幂等性保护
- **方案**: 数据库持久化（`app.trade_intent` 表）
- **幂等键格式**: `{strategy_id}:{date}:{symbol}:{side}:{hash}`
- **检查机制**: 执行前查询数据库，防止重复下单

### 3. 风控方案
- **当前**: 基础风控（资金检查、持仓检查）
- **未来**: 可扩展为完整风控系统（集中度、回撤等）

### 4. 策略执行流程
```
策略调度 → 信号生成 → 创建交易意图（数据库） → 风控检查 → QMT下单 → 更新交易意图状态
```

### 5. 数据库设计
- **Schema**: `app`（与现有量化模型表一致）
- **核心表**:
  - `app.strategy_config` - 策略配置
  - `app.trade_intent` - 交易意图（幂等性）
  - `app.strategy_execution` - 执行记录
  - `app.strategy_performance` - 性能统计（可选）

---

## 三、实现计划

### 阶段一：数据库与核心组件 ⏳
- [ ] 创建数据库表结构（`backend/db/init_qmt_schema.py`）
- [ ] 实现 `SimpleStrategyExecutor` 类
- [ ] 实现 `RiskControlService` 类
- [ ] 实现策略基类 `BaseStrategy`
- [ ] 实现策略执行 API (`/api/v1/strategies/execute`)

### 阶段二：策略实现 ⏳
- [ ] 双均线策略 (`MACrossStrategy`)
- [ ] 趋势跟踪策略 (`TrendFollowingStrategy`)
- [ ] 策略调度器（使用 `schedule` 库）

### 阶段三：策略管理 ⏳
- [ ] 策略管理 API（CRUD）
- [ ] 策略管理 UI（前端页面）
- [ ] 策略执行记录查询

### 阶段四：增强功能 ⏳
- [ ] 事件驱动策略执行
- [ ] 策略回测功能
- [ ] 高级风控
- [ ] 策略性能统计
- [ ] RDagent 多因子策略集成

---

## 四、关键文件清单

### 后端文件
```
backend/
├── infra/
│   ├── qmt_client.py              # ✅ 已有
│   ├── strategy_executor.py       # ⏳ 待实现
│   └── risk_control.py            # ⏳ 待实现
├── routers/
│   ├── qmt.py                     # ✅ 已有
│   └── strategies.py              # ⏳ 待实现
├── strategies/                    # ⏳ 待创建
│   ├── __init__.py
│   ├── base_strategy.py           # ⏳ 待实现
│   ├── ma_cross_strategy.py       # ⏳ 待实现
│   └── trend_following_strategy.py # ⏳ 待实现
├── schedulers/
│   └── strategy_scheduler.py      # ⏳ 待实现
└── db/
    ├── init_qmt_schema.py         # ⏳ 待实现
    └── qmt_dao.py                 # ⏳ 待实现（可选）
```

### 前端文件
```
frontend/
└── src/app/qmt/
    ├── positions/
    │   └── page.tsx               # ✅ 已有
    └── strategies/
        └── page.tsx               # ⏳ 待实现
```

### 文档文件
```
docs/
├── qmt_trading_system_design.md           # ✅ 已完成
├── strategy_base_class_explanation.md      # ✅ 已完成
└── qmt_design_confirmation_summary.md     # ✅ 本文档
```

---

## 五、技术细节确认

### 1. 股票代码格式
- **统一格式**: `{code}.{market}`（如 `600519.SH`, `000001.SZ`）
- **自动转换**: 前端/策略可以输入 `600519`，后端自动转换为 `600519.SH`

### 2. 价格类型映射
| 前端/策略 | xtquant 常量值 | 说明 |
|----------|---------------|------|
| `LIMIT` | 11 | 限价 |
| `MARKET` | 5 | 最新价 |
| `MARKET_PEER_PRICE_FIRST` | 44 | 对手方最优 |
| `MARKET_MINE_PRICE_FIRST` | 45 | 本方最优 |

### 3. 委托类型
| 值 | 说明 |
|---|------|
| 23 | 买入 (`STOCK_BUY`) |
| 24 | 卖出 (`STOCK_SELL`) |

### 4. 交易意图状态
| 状态 | 说明 |
|------|------|
| `PENDING` | 待执行 |
| `EXECUTING` | 执行中 |
| `EXECUTED` | 已执行 |
| `FAILED` | 执行失败 |
| `CANCELLED` | 已取消 |

---

## 六、下一步行动

1. **确认设计方案** ✅（本文档）
2. **开始实现** ⏳
   - 先实现数据库表结构
   - 再实现核心组件（执行器、风控）
   - 最后实现策略和管理功能

---

## 七、参考资料

- **主设计方案**: `docs/qmt_trading_system_design.md`
- **策略基类说明**: `docs/strategy_base_class_explanation.md`
- **集成备忘录**: `docs/xtquant_miniqmt_integration_memo.md`
- **安装配置指南**: `docs/qmt_sim_xtquant_setup.md`

---

**确认人**: AI Assistant  
**确认时间**: 2024-12-20  
**状态**: ✅ 所有需求已明确，可以开始实现

