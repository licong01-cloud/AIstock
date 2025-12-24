# AIstock × miniQMT（xtquant）对接备忘录（直连起步，后期可演进 Gateway）

> 适用范围：Windows 本机部署 miniQMT；AIstock 负责策略运行与管理；miniQMT 负责（模拟/实盘）执行。  
> 本文目标：把“怎么连、怎么部署、怎么选架构、怎么演进、有哪些坑”一次性讲清楚，便于后续落地开发与运维。

---

## 1. 背景与现状（基于当前仓库的代码扫描）

### 1.1 本仓库中与 miniQMT 相关的两套实现

- **监测模块（monitor）使用的 `miniqmt_interface.py`**
  - 现状：**预留/模拟实现**（大量 TODO，生成模拟订单号，未真实调用 `xtquant`）。
  - 对外：后端提供 `/monitor/miniqmt/status|connect|disconnect`；`monitor_service.py` 在触发条件后调用 `execute_strategy_signal()`。

- **旧项目目录 `aiagents-stock-main` 中的 AI 盯盘（smart-monitor）引擎**
  - `aiagents-stock-main/smart_monitor_qmt.py`：存在真实 `xtquant` 调用（`xttrader.XtQuantTrader()`、`order_stock()`、`query_stock_positions()` 等）。
  - 新后端 `backend/smart_monitor_engine.py`：会尝试**动态加载旧引擎**，失败则回退 stub（即 AI 盯盘可能处于“可用/不可用/模拟”三种状态）。

### 1.2 当前一键启动脚本实际启动什么

根目录 `start_all_ai_stock.bat` 启动：
- TDX Go Backend（`tdx-api-main/web`，默认设端口 `TDX_HTTP_PORT=19080`）
- AIstock 后端（FastAPI：`uvicorn backend.main:app --port 8001`）
- AIstock 前端（Next.js：`frontend` 目录 `npm run dev`）

> 启动脚本本身不会额外启动旧项目服务；但新后端存在“动态导入旧引擎”的桥接逻辑。

---

## 2. xtquant 是什么：链接方式与部署位置

### 2.1 xtquant 不是独立服务

- **xtquant 通常是 Python 库（包）**，在 Python 进程里 `import` 后使用。
- **miniQMT 是独立的 Windows 客户端/交易端**（需要安装、登录、保持运行）。
- 常见形态：AIstock（Python）通过 xtquant 与同机 miniQMT 交互，完成：
  - 查询资金/持仓/当日委托成交
  - 下单/撤单
  - 订阅回报（依版本/实现）

### 2.2 xtquant 应该部署在哪里

在“miniQMT 部署在 AIstock 运行的本机”的前提下：
- **xtquant 应安装在同一台 Windows 机器的 Python 环境中**（与 miniQMT 客户端同机）。
- 不建议放到 WSL/Linux 来跑 xtquant（生态/进程交互不匹配，稳定性差）。

### 2.3 AIstock 直连 xtquant 时，重启会影响什么

- **AIstock 重启**：
  - 进程退出 -> 连接对象/订阅回调消失 -> **xtquant 连接需要重建**
  - 重启期间的回报可能丢失（若只靠内存回调收取）
  - 若无“幂等与对账”，有重复下单风险
- **miniQMT 客户端通常不受影响**：不会因为 AIstock 重启而退出（除非你把 miniQMT 当子进程跟随关闭）。
- **已提交的委托不会自动撤销**：订单继续在券商/柜台侧排队或成交。

---

## 3. 目标架构：AIstock 管策略，miniQMT 只执行

### 3.1 角色划分（建议）

- **AIstock（策略编排层）**
  - 策略运行与管理：启停、调度、参数、版本、回测/实盘模式切换
  - 特征计算、模型推理（机器学习策略）、组合/仓位生成
  - 风控与合规策略：限仓、频率、白名单、黑名单、交易时段、冷却期、最大回撤/单笔损失等
  - 生成“下单意图（Trade Intent）”并落库（或发送到执行层）
  - 对前端提供：信号、意图、订单、成交、持仓、执行状态展示

- **miniQMT + xtquant（执行层能力）**
  - 负责连接、下单、撤单、查资金/持仓/委托/成交、回报采集
  - 不建议承载策略逻辑（尤其是 ML 特征/推理/组合优化）

> 机器学习策略建议在 AIstock 侧运行：ML 的“特征+推理+组合+风控”属于策略域逻辑，更需要版本化与可回放。

---

## 4. 两种架构选型：直连 vs 独立 Gateway

### 4.1 架构 A：直连（AIstock 进程内 import xtquant）

**结构**
- FastAPI / 策略引擎进程内直接调用 xtquant（但要集中封装，见第 5 章）
- miniQMT 客户端独立运行

**适用**
- 初期以模拟盘为主
- 日频策略（每天少量交易）
- 实盘更偏“选股 + 买卖点建议 + 人工下单”，自动下单需求弱

### 4.2 架构 B：独立 Gateway（本机常驻执行服务）

**结构**
- AIstock 产出下单意图（intent）-> 通过 HTTP/队列/DB 任务表交给 Gateway
- Gateway 常驻，内部 import xtquant，连接 miniQMT，负责执行与回报
- AIstock 只做编排与展示，不直接碰 xtquant

**适用**
- 后期要自动实盘、长时间稳定运行
- 多策略/多任务并发执行，需要统一串行化与风控
- 需要稳定接收回报，不能因 AIstock 重启丢事件

### 4.3 优劣势对比（摘要）

| 方案 | 优势 | 劣势 |
|---|---|---|
| 直连 | 部署简单、链路短、开发快 | 重启断连、回报易丢、并发与一致性更难、后期扩展成本更高 |
| 独立 Gateway | 隔离更强、连接更稳、回报不易丢、统一风控/幂等/对账、多策略/多账户更易扩展 | 组件增多、协议/状态机更规范、运维复杂度上升 |

---

## 5. 初期“直连”也要做的：逻辑 Gateway（模块/类封装）

> 核心原则：**策略代码不直接 import xtquant**；xtquant 只出现在一个地方。  
> 这样后期改为“独立 Gateway 服务”时，策略侧基本不改，只替换实现。

### 5.1 建议的抽象接口（AIstock 内部统一协议）

建议定义一套稳定的“执行接口”，策略只依赖这个接口：
- `health()`：连接状态 / 模式（SIM/LIVE）/ 账户信息摘要 / 最近错误
- `sync_account()`、`sync_positions()`、`sync_orders()`、`sync_fills()`：对账用
- `place_order(intent)`：提交订单
- `cancel_order(broker_order_id)`：撤单

并定义“下单意图（Trade Intent）”数据结构（建议落库）：
- `client_order_id`：幂等键（必须）
- `strategy_id` / `run_id`：策略与批次
- `symbol`：统一格式（如 `600519.SH`）
- `side`：BUY/SELL
- `qty` 或 `target_value/target_weight`（二选一，推荐先 qty）
- `order_type`：MARKET/LIMIT
- `limit_price`（限价单）
- `time_in_force`（可选）
- `reason`/`signal`：信号摘要与追溯
- `status`：NEW/SENT/ACKED/FILLED/REJECTED/CANCELED/ERROR

### 5.2 并发模型：强制串行执行 xtquant 调用

直连阶段也建议：
- **单线程 worker + 队列** 执行所有 xtquant 调用
- FastAPI 接口/策略线程只把任务丢给 worker，并等待结果或返回 task id

原因：
- 避免多线程下单/查询导致状态混乱
- 便于复用到后期独立 Gateway（同样是一个 worker）

### 5.3 幂等与防重复下单（直连阶段必须做）

至少做到：
- 每个 intent 写入 DB（或持久化存储）后再执行
- `client_order_id` 唯一（例如：`{strategy}:{trade_date}:{symbol}:{side}:{seq}`）
- `place_order` 前先查 DB：如果同 `client_order_id` 已有 `broker_order_id`，直接返回，不再下单

### 5.4 启动对账（重启恢复）

AIstock 启动后（或每天开盘前）执行一次：
- 拉取：资金、持仓、当日委托、当日成交
- 回填 DB，把本地状态与真实账户对齐

> 日频策略尤其适合：“每天一次对账 + 一次下单批次”，复杂度低但可靠性高。

---

## 6. 后期演进：从“逻辑 Gateway”到“独立 Gateway 服务”

### 6.1 演进目标

保持策略侧接口不变，将实现从：
- `XtQuantGateway`（同进程直连）
替换为：
- `RemoteGatewayClient`（HTTP/队列）
并新增：
- `GatewayService`（常驻进程，内部仍是 XtQuantGateway + worker）

### 6.2 通信选项（同机也适用）

优先级建议：
- **DB 任务表（最少组件）**：AIstock 写 intent；Gateway 轮询执行；状态回写
- **Redis/RabbitMQ 等队列（更实时）**：intent 队列 + 回报事件流；DB 仍做最终账本
- **HTTP/gRPC**：实现简单但需要额外处理重试/幂等/回报回推

### 6.3 独立服务的“最小边界”

建议先把独立服务边界控制在：
- **只做交易执行与对账**（place/cancel/sync/health）
- 行情（实时 tick）可以后置：除非你需要 tick 级策略，否则优先使用现有 TDX/DB/Qlib 数据体系做历史与分钟级特征。

---

## 7. 针对你的使用场景的建议（模拟盘为主 + 实盘手工）

### 7.1 现在更合适的做法

- **实盘：先不自动下单**  
  AIstock 输出：选股结果、买卖点、建议仓位/数量、风险提示、订单清单（可复制/导出），由人工在 miniQMT 完成交易。

- **模拟盘：可以自动下单（用于闭环验证）**  
  直连或轻量逻辑 gateway 即可；关键是把 intent/order/fill/position 全部落库，形成可复盘的闭环。

### 7.2 什么时候应该升级到独立 Gateway

满足任一条件就值得升级：
- 计划开启实盘自动下单
- 策略数量增多/并发增多
- AIstock 后端需要频繁升级重启，但不希望影响执行与回报
- 需要更严格的审计、告警、风控隔离

---

## 8. 落地清单（建议优先级）

### 8.1 直连阶段（建议必须做）
- [ ] 定义 `TradeIntent` + `OrderStatus` 状态机（落库）
- [ ] 实现逻辑 gateway：`TradeGateway` 接口 + `SimGateway` + `XtQuantGateway`
- [ ] xtquant 调用集中在 `XtQuantGateway`，并强制单 worker 串行化
- [ ] 幂等：`client_order_id` 唯一 + 执行前查重
- [ ] 启动/每日对账：资金/持仓/委托/成交回填 DB
- [ ] 前端展示：信号、意图、订单、成交、持仓、手工执行清单导出

### 8.2 演进阶段（独立 Gateway）
- [ ] 将 `XtQuantGateway` 移入常驻进程服务
- [ ] AIstock 改为 `RemoteGatewayClient`（HTTP/队列/DB任务表）
- [ ] 增加心跳、告警、权限控制（token/IP 白名单）
- [ ] 回报不丢：回报落库 + 重启补拉对账

---

## 9. 附：关于“我是否熟悉 xtquant 的所有用法”

- 我对 **典型接入路径与工程化关键点**（连接生命周期、下单/撤单、查询资金/持仓/委托/成交、回报处理、幂等与对账、并发与重启恢复）是熟悉的，能够设计并落地一套可演进架构。
- 但 **xtquant 的具体 API 细节**会随版本/券商封装不同而变化（类名、常量、回调字段、返回对象属性等）。  
  在你正式接入实盘前，仍建议用你本机安装的 xtquant 版本与官方示例做一次 API 对表核对，确保实现与当前版本一致。


