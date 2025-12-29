# AIstock 研发进度对照 Checklist (严格遵循 4 份核心设计文档)

## 0. 核心参考文档 (唯一事实来源)
1. **顶层架构**: `f:\Dev\RD-Agent-main\docs\2025-12-23_RD-Agent_AIstock_TopLevel_Architecture_Design.md`
2. **阶段二总设计 (AIstock Phase2 唯一入口)**: `f:\Dev\RD-Agent-main\docs\2025-12-26_Phase2_Detail_Design_RD-Agent_AIstock_v2.md`
3. **数据服务层详细设计 (唯一接口规范)**: `f:\Dev\RD-Agent-main\docs\2025-12-24_DataServiceLayer_Detail_Design_RD-Agent_AIstock.md`
4. **阶段三详细设计 (执行层+选股+预览)**: `f:\Dev\RD-Agent-main\docs\2025-12-26_Phase3_Detail_Design_RD-Agent_AIstock_v1.md`

---

## 1. 数据服务层 (Data Service Layer) [全量功能清单]

### 1.1 在线行情视图 (规范文档-3) [部分完成]
- [x] **Snapshot 接口**: `get_realtime_snapshot` 支持 xtquant/tdx 切换。
- [x] **报价流接口**: `stream_quotes` (QuoteBatch 迭代器).
- [x] **历史窗口视图**: `get_history_window` (MultiIndex 规范化 DataFrame).
- [ ] **[缺失] 交易日历与规则视图**: `get_trading_calendar`, `get_trading_period` (规范文档-3.4).

### 1.2 账户与持仓视图 (规范文档-4) [未完成]
- [ ] **[缺失] 账户状态接口**: `get_portfolio_state` (Cash/Equity/Positions).
- [ ] **[缺失] 订单状态接口**: `get_open_orders` (Order 列表).
- [ ] **[缺失] 成交历史接口**: `get_trades` (Trade 列表).

### 1.3 Qlib Runtime 集成 (规范文档-6.4) [未完成]
- [ ] **[缺失] 固定版本集成**: AIstock 环境内安装并配置 `qlib`.
- [ ] **[缺失] 自定义 DataProvider**: 映射 `get_history_window` 到 Qlib 数据请求.
- [ ] **[缺失] 自定义 DataHandler**: 确保数据索引、字段名、缺失值处理与 RD-Agent 侧 100% 一致.

---

## 2. 阶段二 (Phase 2): 成果资产化与推理服务 [全量功能清单]

### 2.1 成果同步与落库 (Phase2_v2-3) [部分完成]
- [x] **Catalog 同步**: 因子/策略/Loop 三大 Catalog 的 API 拉取与 DB 写入.
- [ ] **[缺失] 因子实现指针**: Catalog 中 `impl_module`, `impl_func`, `impl_version` 的展示与校验 (Phase2_v2-2.1).
- [ ] **[缺失] 资产归档标记**: 从 WSL 侧拉取成果并标记本地归档状态.

### 2.2 推理服务层 (Inference Layer) (Phase3-7.10) [未完成]
- [ ] **[缺失] 基于 Loop 的选股服务**: 输入 `loop_id` -> Qlib 推理 -> 输出候选列表.
- [ ] **[缺失] 模型推断引擎**: 加载 `model_conf` + `.pkl` 模型，执行 `predict`.
- [ ] **[缺失] 日志同步入口**: 关联 `log_dir`, 支持在 AIstock UI 查看 RD-Agent 原始回测日志.

---

## 3. 阶段三 (Phase 3): 执行迁移与策略预览 [待开始]
*(注意：必须在 Phase 2 和 Data Service 完全验收后再开始)*

- [ ] **AIstock 因子执行引擎**: `aistock/factors/engine.py` 实现.
- [ ] **策略执行引擎**: `aistock/strategies/engine.py` 实现.
- [ ] **策略预览模式**: 虚拟持仓/净值曲线/多策略对比 UI.
