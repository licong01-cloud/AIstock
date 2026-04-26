# Paper Trading v2 UI 全流程验证方案

Date: 2026-04-26
Status: active validation plan
Scope: `/paper-v2` 新 UI、StrategyPackage、Selection Center、Paper Trading v2 历史回放与 TDX 实时分钟线可用性验证
Out of scope: V25 adapter、QMT、Shadow、实盘交易、8001 生产后端重启

## 1. 验证目标

本验证不是“页面能点通”测试，而是确认模拟盘 v2 对策略研发有实际业务价值：

- StrategyPackage 必须来自权威 QE 实验或 QE 演进 Loop。
- 选股结果必须可追溯 `package_id`、`manifest_sha256`、`trade_date`、`data_source` 和运行配置。
- 历史回放必须使用分钟线、涨跌停、pre_close、交易日历、停牌数据和真实账本逻辑。
- 订单、成交、现金流水、持仓、日快照、错误事件和绩效必须落库并可在 UI 查询。
- 绩效验证以真实回放数据为准：收益可能为正也可能为负，但不能用空结果、默认价格、默认持仓或默认成功伪装策略有效。
- 缺少任何关键数据或运行时产物必须 fail-fast，并把后端错误原样透传到 UI。

## 2. 固定测试样本

首批 StrategyPackage 来源：

```text
qe_20260416_002701
qe_20260413_084216
qe_20260416_082012
```

当前日期为 2026-04-26，非交易日。当前阶段使用最近 10 个已完成交易日做 `DB_HISTORICAL` 回放验证；下一个交易日盘中再执行 `TDX_REALTIME` 主链路运行验证。

## 3. 测试环境

- 不重启或停止 8001 生产后端。
- 后端临时端口：`8011`。
- 前端临时端口：`3011`。
- 前端 API 环境变量：`NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1`。
- Playwright 使用 Chromium，默认 headless，避免占用用户鼠标和当前桌面。
- 所有 Python 命令使用：

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
```

## 4. UI 功能验证矩阵

### 4.1 StrategyPackage 中心

步骤：

1. 打开 `/paper-v2/packages`。
2. 分别输入三个 QE 实验 ID。
3. 执行“预览实验 Manifest”。
4. 执行“验证模拟盘就绪度”。
5. 执行“从单次实验创建”。
6. 对已创建策略包执行“启用选股”和“启用模拟盘”。
7. 查看指标、模型状态、执行策略、状态事件。

成功标准：

- 每个成功创建的包都有 `package_id`、冻结 manifest、`manifest_sha256`。
- UI 展示 IC、RankIC、夏普、年化收益、最大回撤等指标；缺失指标必须显式显示缺失。
- 后端拒绝不满足模拟盘条件的包时，UI 显示错误码、消息和上下文。

### 4.2 单策略包选股

步骤：

1. 打开 `/paper-v2/selection`。
2. 选择 `single_package`。
3. 选择最近 10 个已完成交易日中的一个交易日。
4. 数据源选择 `DB_HISTORICAL`。
5. 开启“剔除已确认停牌”。
6. 运行选股。
7. 查看排序候选股、目标权重和剔除/补位追踪。

成功标准：

- 候选股不为空，除非后端明确返回 valid_no_candidate 和原因。
- 已确认停牌、行业黑名单剔除必须有 trace。
- 如果信号 top 股票不可交易，必须从后续排名补位。
- 缺少 score、rank、target position、行业数据、HMM artifact 或停牌审计时必须失败。

### 4.3 多策略包聚合选股

步骤：

1. 分别为多个单策略包运行同一交易日、同一数据源的选股。
2. 在“聚合已有选股运行”中选择多个已完成单策略包运行。
3. 分别测试 `union`、`intersection`、`weighted_fusion`。
4. 查看聚合候选股和来源 trace。

成功标准：

- 多策略包聚合不冻结，不创建模拟盘执行组合。
- `weighted_fusion` 使用明确权重和 rank-normalized trace。
- 交易日或数据源不一致时后端拒绝，UI 透传错误。

## 5. 模拟盘 v2 回放验证

### 5.1 从单策略包选股创建组合

步骤：

1. 对成功的单策略包选股运行点击“从选股运行创建”。
2. 打开 `/paper-v2/portfolios` 或组合详情页。
3. 确认组合冻结字段：
   - `package_id`
   - `manifest_sha256`
   - `initial_cash`
   - `start_date`
   - `data_source`
   - `fee_policy`
   - `risk_policy`
   - `execution_policy`

成功标准：

- 组合状态可进入 `READY`。
- 多策略包聚合选股不能创建组合。
- 执行策略必须来自已回测验证策略或通过 manifest 默认策略校验。

### 5.2 最近 10 个交易日历史回放

步骤：

1. 打开组合运行控制台。
2. 设置回放起止日期为最近 10 个已完成交易日。
3. 使用默认 runtime profile：
   - `top_k=50`
   - `exclude_suspended=true`
   - `industry_blacklist=[]`
   - `hmm.enabled=false`
4. 先执行单日 readiness。
5. 执行历史回放，默认 `reject_existing`。
6. 如测试组合已存在历史账本，仅对 E2E 专用组合使用 `reset_portfolio`，并输入完整 `portfolio_id` 确认。

成功标准：

- 回放日期必须是交易日。
- 至少一个策略包在 10 日回放中产生非零订单、成交、现金流水、持仓或明确失败原因。
- 回放不能出现部分日期成功、部分日期静默跳过的假成功。
- 重复运行必须由 `reject_existing` 拒绝，除非显式确认 reset。

### 5.3 账本与绩效验证

步骤：

1. 打开组合账本页。
2. 检查订单、成交、现金流水、持仓、日快照、运行事件、错误。
3. 打开绩效页。
4. 检查总收益率、年化收益、年化波动率、夏普、最大回撤、日收益和净值曲线。

成功标准：

- 绩效来自持久化 `daily_snapshots`，不是前端临时计算假数据。
- `final_nav`、`total_return`、`daily_returns` 与账本快照一致。
- 负收益也必须如实展示；不能把亏损隐藏成“策略有效”。
- 如果缺少足够数据，绩效页必须显示 `insufficient_data_reasons`。

## 6. TDX 实时分钟线验证

当前 2026-04-26 是非交易日，因此不验证盘中实盘收益，只验证 TDX 实时分钟线通道是否可访问。

步骤：

1. 确认 TDX Go 后端 `19080` 可访问。
2. 通过后端临时端口 `8011` 调用 Paper v2 或 market-data provider 的 TDX_REALTIME minute smoke。
3. 使用高流动性股票，例如 `000001.SZ`。
4. 如果 TDX 在非交易日只返回空结果，测试必须记录明确原因，不能生成默认分钟线。

成功标准：

- 若 TDX 返回分钟线，记录 bar 数、首尾时间、价格字段完整性。
- 若 TDX 返回空，记录 `TDX_REALTIME_NO_CURRENT_SESSION_BARS` 或等价明确错误。
- 不允许静默 fallback 到 DB_HISTORICAL。

## 7. 负向 fail-fast 测试

必须覆盖：

- 不存在 QE 实验 ID。
- 未启用选股的策略包进入 Selection Center。
- 交易日历缺失或非交易日。
- 停牌审计缺失。
- 涨跌停或 pre_close 缺失。
- 分钟线缺失。
- HMM 开启但缺少 snapshot、preset、系数或行业映射。
- 多策略包聚合运行尝试创建 Paper v2 组合。
- 重复回放触发 `reject_existing`。
- reset 回放确认文本不匹配。

## 8. Playwright 自动化范围

第一阶段自动化：

- 页面加载与导航。
- 关键按钮可见性和禁用逻辑。
- 后端错误透传面板显示。
- StrategyPackage 创建、启用、选股、创建组合、readiness、replay、账本、绩效页的 UI 流程。

第二阶段自动化：

- 用临时后端 `8011` 和临时前端 `3011` 做真实 API 流程。
- 使用测试专用 portfolio，避免影响既有账本。
- 每发现一个 bug，修复后单独提交。

已落地的专用命令：

```powershell
cd F:\Dev\AIstock\frontend
$env:PAPER_V2_API_BASE='http://127.0.0.1:8011/api/v1'
$env:PAPER_V2_FRONTEND_PORT='3011'
npx playwright test --config=playwright.paper-v2.config.ts tests/paper-v2
```

说明：

- Paper v2 E2E 使用 `frontend/playwright.paper-v2.config.ts`，避免覆盖其他模块正在使用的通用 Playwright 配置。
- 临时前端通过 `PAPER_V2_API_PROXY_TARGET` 代理 `/api/v1/*` 到 8011，避免 3011 本地端口触发浏览器 CORS 预检失败。
- 当前自动化已覆盖 StrategyPackage 展示、就绪 fail-fast、Selection Center fail-fast、多策略包研究边界、组合创建 fail-fast、模型/HMM 页面和 TDX 分钟线 HTTP 通道。

## 9. 明日盘中验证

下一个交易日盘中执行：

1. 选择 `TDX_REALTIME` 数据源。
2. 对已 READY 的测试组合执行 readiness。
3. 若 readiness 通过，执行单日实时模拟。
4. 验证观察到的分钟线只使用已发生分钟，不使用未来数据。
5. 验证订单、成交、账本、运行事件和错误在盘中增量可见。

## 10. 结果汇报格式

每轮验证输出：

- 使用的策略包和 manifest hash。
- 选股日期、候选数、可交易候选数、剔除数、补位数。
- 回放日期范围、交易日数、订单数、成交数、现金流水数、日快照数。
- 总收益率、最终净值、最大回撤、夏普或数据不足原因。
- 所有失败的错误码、上下文和修复提交号。
