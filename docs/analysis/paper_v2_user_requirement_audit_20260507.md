# Paper v2 用户需求审计与架构诊断

生成时间：2026-05-07
作者：Claude（仅分析，未修改任何代码）
范围：Paper Trading v2 / StrategyPackage / Selection Center / Paper v2 前端
配套文档：`docs/analysis/paper_v2_architecture_flow_and_confirmed_defects_20260507.md`（同日由 Codex 出具的阻断点清单，本文不重复，仅在必要处引用）

本文针对用户在 2026-05-07 提出的六项核心需求和 UI 体验诉求，进行**逐项代码级取证**与**架构合规性判定**。结论先于证据；每条结论附 file:line 引用。

---

## 0. 总体结论

| 用户需求 | 结论 | 风险等级 |
| --- | --- | --- |
| ① UI 简化、命名清晰、布局合理 | 不达标 | 高 |
| ② StrategyPackage 仅冻结模型+因子，其他可在 Selection/Paper 阶段自定义 | **架构方向相反**：当前用 `backtest_contract` 把 9 项配置中的 6 项强制锁定到与 QE 一致 | **极高** |
| ③ 策略包之上自定义 HMM/黑名单/停牌/初始资金/topk/换仓数/日频/分钟执行/尾盘 | 9 项中：1 项支持，5 项被冻结，2 项缺失，1 项部分支持 | 极高 |
| ④ 强制实盘数据，禁用回测产物 | **基本达标**：信号路径已严禁 `pred.pkl`，仅接受 live inference artifact；市场数据严格走 TDX/DB 双源 | 低 |
| ⑤ 选股/模拟盘与 QE 用同一框架引擎 | **架构上不达标**：QE 在 WSL+Qlib 跑，Selection/Paper 在 AIstock Python 侧独立实现；只通过 manifest 字段做"配置一致"校验，不是同一执行栈 | 高 |
| ⑥ 模拟盘支持回放→实盘平滑转换 | **达标**：已实现 `CATCHUP_THEN_LIVE` 自动状态机 | 低 |

最关键的两个架构层级问题：

1. **配置冻结边界与用户心智模型相反**。用户希望"策略包是模型+因子的最小冻结单元，外层一切可调"；当前实现是"manifest 同时冻结 portfolio_policy / minute_execution / risk_policy / HMM / 黑名单 / tradability / stock_pool 等"，再用 `backtest_contract` 在 Paper/Selection 阶段强制驳回任何与 QE 不一致的运行时配置。这条不修，UI 简化没有意义——UI 上看到的"可改"其实改不动。
2. **"统一引擎"是字段一致性，不是执行栈一致性**。QE 用 Qlib + WSL 的 `qrun_limit_minute.py`；Paper v2 用本地 Python 的 `TargetPositionEngine` + `RebalanceEngine` + `MinuteExecutionEngine`；Selection Center 只跑 `StrategyPackageRuntime.build_signal_snapshot()` 后停止（没有持仓/订单/执行）。三套独立栈共享的只有 `StrategyPackage manifest` 这份 frozen JSON 和因子推理脚本 `strategy_package_live_inference.py`。

---

## 1. 用户需求 ①：UI 复杂、不人性化

**用户原话（2026-05-07）**：单个页面或卡片显示太多信息、命名是无意义字母组合、布局不合理。

### 1.1 命名直接暴露内部 ID

UI 中存在大量**截短的内部哈希**直接展示给用户：

- `frontend/src/app/paper-v2/packages/page.tsx:322-323` — 在"当前策略包"卡片里直接渲染 `package_id` 与 `manifest_sha256` 的截短哈希作为 chip。
- `frontend/src/app/paper-v2/page.tsx:158` — 首页"模拟盘列表"的"策略包"列直接渲染 7 字符的 `shortHash(portfolio.package_id)`，用户在表格里识别策略包必须靠记忆 7 位哈希。
- `frontend/src/app/paper-v2/selection/page.tsx:391` — 选股结果区显示 `eyebrow={run ? 'run_id ${shortHash(run.run_id)}' : '尚未运行'}`。
- 复合 source key：`packages/page.tsx:92,105-108` 用 `${source_kind}:${experiment_id}:${qe_task_id}:${qe_loop_id}` 作为下拉 value，用户复制粘贴或排查时直面这串字符。

### 1.2 状态枚举原文渲染，无中文映射

- `format.ts` 中虽然定义了 `STATUS_LABELS` 中文映射，但**仅在 `StatusBadge` 组件内部生效**。表格列、过滤器下拉、错误提示中仍然出现 `BACKTEST_APPROVED` / `SELECTION_ENABLED` / `PAPER_ENABLED` / `LEGACY_NON_ST_PIT` / `STALE_INITIAL_BACKTEST_MODEL` / `PREFLIGHTING` / `CATCHING_UP` 等英文枚举值。
- 例如 `selection/page.tsx:367` 警告条直接展示 `BLOCKED/LEGACY_NON_ST_PIT`：`"当前选择包含 BLOCKED/LEGACY_NON_ST_PIT 策略包"`。

### 1.3 单页/单卡密度过高

| 页面 | 行数 | 主要交互元素数 | 主要卡片 |
| --- | --- | --- | --- |
| `paper-v2/page.tsx`（总览） | 174 | ≈18（含 6 个下拉、3 个文本输入、8 列表格） | 4 |
| `packages/page.tsx`（策略包） | 358 | ≈13 | 4 |
| `selection/page.tsx`（选股） | 468 | ≈25+（主控制卡 12 个表单字段、4 个表格） | 6 |
| `portfolios/[id]/run-console/page.tsx`（运行控制台） | **722** | **≈45+**（20+ 表单、12+ 按钮、6 个下拉、4 个复选、3 张表格） | 6 |

具体证据：

- 总览页"运行/暂停组合"过滤卡（`page.tsx:140-151`）一个卡里塞 8 个字段（status / sortBy / sortDir / searchField / search / pageSize / minCash / maxCash），3 列网格中第三行只用了 2 格、剩 1 格空白，既不紧凑也不松弛。
- 选股页"运行配置"卡（`selection/page.tsx:315-368`）单卡 12 个表单 + 2 通知 + 1 主按钮，HMM 子区在主区下方平铺，而不是折叠。
- 运行控制台 6 个主卡（单日运行控制 / 执行策略激活 / 运行配置版本 / 运行场景启动 / 实时模拟 / 会话与时间线），每卡都自带表单 + 表格 + JsonPanel，单页滚动深度极大。

### 1.4 流程没有"步骤 1→2→3"引导

- 顶部 7 个平级 tab（`layout.tsx:8-16`：总览 / 策略包 / 选股中心 / 运行监控 / 模拟组合 / 模型与 HMM / 设置），无任何"必须先完成 X 才能进入 Y"的视觉引导。
- 总览页虽然有"流程看板"卡（`page.tsx:112-119`），但内容只是 4 个统计 MetricCard（数字），不是流程图。
- 用户从 QE source 到第一次成功跑出选股需要：进 `/packages` → 选 source → 创建 → 标记选股可用 → 跳 `/selection` → 选包 → 配置 → 运行。**至少 8 次交互、跨 2-3 个页面**（`packages/page.tsx:296`、`packages/page.tsx:299`、`selection/page.tsx:366`）。

### 1.5 错误与诊断展示

- 大量场景使用 `<JsonPanel value={...} />` 直接 dump 后端结构（如 `run-console/page.tsx:566` 的 `readiness`、`run-console/page.tsx:643` 的 capability 诊断）。这些是给开发者看的格式，不是给最终用户看的。
- `ReadinessResult.checks[]` 已经有 `check_name`/`status`/`context` 三段结构（types.ts:229-241），但前端只把 `readinessPassed` 计算成布尔值显示在 MetricCard 上（`run-console/page.tsx:111-114, 500`），失败原因被埋在折叠 JSON 里。

### 1.6 样式层证据

- `paper-v2.css` 表格 `min-width: 720px`（line 111-118），首页 8 列表格在 1366px 笔记本上必然横向滚动。
- `.pv2-card` 边框 `rgba(99,83,61,0.16)`（16% 透明度）+ 浅色 `#fffdf8` 背景，卡片之间几乎看不见分隔。
- `.pv2-muted #70685f` 与背景对比度约 3.1:1，未达 WCAG AA。
- `.pv2-metric::after` 在每个指标卡右下角加 86px 装饰圆，无信息含义但放大了视觉拥挤感。

---

## 2. 用户需求 ②：StrategyPackage 应只冻结"模型+因子"

**用户原话**：策略包只限制模型和因子的组合，其他配置都可以在模拟盘和选股中自定义选择；审计策略包是否符合模拟盘和选股的资产要求。

### 2.1 当前 manifest 实际冻结清单

`backend/services/strategy_package/qe_source_resolver.py:376-406` 的 `_build_manifest()` 与 `backend/services/strategy_package/backtest_contract.py:59-93` 的 `build_backtest_runtime_contract()` 共同决定了实际冻结边界。除了用户期望的"模型+因子"以外，还把以下字段一并冻结：

| 冻结大类 | 关键字段 | 证据 |
| --- | --- | --- |
| 模型/因子 | `model_asset` / `factor_set` / `alpha_components` / `alpha_combination_policy` | qe_source_resolver.py:386-391 |
| **portfolio_policy** | `topk` / `n_drop` | qe_source_resolver.py:398、backtest_contract.py:331-332 |
| **minute_execution_policy** | `bar_freq` / `algo_code` / `algo_config` | qe_source_resolver.py:548-562 |
| **risk_policy** | enabled / providers / hard_actions | backtest_contract.py:389-393 |
| **HMM** | enabled / model_snapshot_id / signal_preset / coefficients_path | backtest_contract.py:439-525 |
| **industry_blacklist** | enabled / values[] | backtest_contract.py:528-556 |
| **tradability** | exclude_suspended | backtest_contract.py:415-437 |
| **stock_pool / universe_policy** | universe_key | qe_source_resolver.py:397 |
| backtest_summary | IC / 年化 / 最大回撤等 | qe_source_resolver.py:567-592 |
| asset_checks | 资产校验结果 | qe_source_resolver.py:594-628 |
| alpha_mode | 单/多 alpha | qe_source_resolver.py:342, runtime.py:64-69 |
| data_split | 训练/验证/测试日期 | qe_source_resolver.py:328-333 |

### 2.2 backtest_contract 强制约束机制

`backend/services/strategy_package/backtest_contract.py` 中的 `validate_runtime_profile_matches_backtest_contract()`（line 125）和 `normalize_runtime_config_with_backtest_contract()`（line 234）在以下入口被无条件调用：

- Selection Center 单包 run：`backend/services/selection_center/service.py`（normalize 在 run 主路径中）
- Paper v2 day run：`backend/services/paper_trading_v2/day_runner.py:125-130`
- Paper v2 portfolio runtime activation：`backend/services/paper_trading_v2/service.py:645-654`

任何与 manifest 不一致的 runtime 配置都会被驳回（topk: line 406-412；exclude_suspended: line 426-434；HMM: line 502-516；industry_blacklist: line 552；execution_policy: line 109-122）。**没有 skip / override / disable 开关**——这是架构级硬约束。

### 2.3 用户期望 9 项配置的当前可调状态

| # | 配置项 | 用户期望 | 当前状态 | 证据 |
| --- | --- | --- | --- | --- |
| 1 | HMM 配置 | Selection/Paper 自由选 | 冻结，必须与 QE 完全一致 | backtest_contract.py:161-205, 439-525 |
| 2 | 行业黑名单 | 自由配 | 冻结 | backtest_contract.py:528-556 |
| 3 | 是否剔除停牌股 | 自由配 | 冻结 | backtest_contract.py:415-437 |
| 4 | 初始资金 | 自由配 | **可自由配置** | service.py:104, models.py:37 |
| 5 | TopK | 自由配 | 冻结 | backtest_contract.py:406-412, 136-147 |
| 6 | 每日换仓数量（n_drop） | 自由配 | 冻结 | qe_source_resolver.py:373,398; runtime.py:559-563 |
| 7 | 日频策略 | 自由选 | **架构缺失**：`_normalize_backtest_freq()` 仅允许 `1min`/`5min`，明确拒绝 `day`（qe_source_resolver.py:526-540） |
| 8 | 分钟线执行策略 | 自由选 | 冻结 | qe_source_resolver.py:548-562, backtest_contract.py:96-122, service.py:359-392 |
| 9 | 尾盘处理策略 | 自由配 | **完全缺失**：代码库无 `pre_close` / `tail_period` / `close_handling` 任何字段定义 | （全库未命中） |

**结论**：9 项中 1 项支持、5 项被强制冻结、2 项架构缺失、1 项（risk_policy）字段存在但同样被冻结。这与"策略包只冻结模型+因子"的设计意图**根本性相反**。

### 2.4 资产准入审计是否就位

用户期望"审计策略包是否符合模拟盘和选股的资产要求，确认可以进入"。当前 `selection_center/package_health.py` 已有健康预检，能识别 `RUNNABLE` / `LEGACY_NON_ST_PIT` / `BLOCKED`，但：

- 当前所有 4 个可选包均为 `LEGACY_NON_ST_PIT`（参见 Codex 文档 P0-1），即审计已能识别"不符合"，但**没有正向"已审计通过、可入选股/可入模拟盘"的状态机**清晰地展示给用户。
- StrategyPackage 状态字段（`BACKTEST_APPROVED` / `SELECTION_ENABLED` / `PAPER_ENABLED`）是手工标记流程，不是基于资产/合约自动审计的产物（packages/page.tsx:296 的"标记可用于选股"按钮）。

---

## 3. 用户需求 ③：策略包之上自定义 Selection / Paper 配置

直接对应 §2.3 的 9 项表格。**核心问题已包含在 §2**：架构上 backtest_contract 把这些字段都锁死，UI 上即便允许填写，提交时也会被后端拒绝。

需要在架构层做的判断（仅列出问题，不写改动建议）：

- **是否需要"软合约 + 偏离声明"机制**：保留"和 QE 一致"作为推荐缺省，但允许用户显式"偏离"，并在 portfolio/run 元数据上记录偏离项以便溯源。
- **日频策略缺位的影响**：当前 QE 实验合约设计就把 `backtest_freq` 限制在分钟线，意味着即便去掉 backtest_contract 锁，用户也无法在 Paper 里改成日频跑——日频路径在执行栈层就不存在。
- **尾盘处理**：当前完全没有概念存在，属于产品功能缺失，不只是"配置不可调"。

---

## 4. 用户需求 ④：必须使用实盘数据，禁用回测数据

**结论：基本达标**。

### 4.1 信号源严格

- `backend/services/paper_trading_v2/day_runner.py:619-623` 在读取 selection_score_artifact 时硬性校验 `metadata.source_type == AUTHORITATIVE_SELECTION_SOURCE_TYPE`（即 `live_qe_model_inference_v1`）和 `authority_scope == AUTHORITATIVE_SELECTION_SCOPE`，回测产物（`qe_mlruns_pred_pkl_v1`）会被拒绝。
- 缺 artifact 时调用 `selection_artifact_service.generate_from_live_inference()`（day_runner.py:627），强制走 live inference 链路。
- live_session.py:331-342 的 LIVE 路径同样先 `_ensure_authoritative_selection_artifact()` 再 `runtime.build_signal_snapshot()`。

### 4.2 市场数据双源严格

`backend/services/paper_trading_v2/market_data.py`：

- 仅支持 `MinuteDataSource.TDX_REALTIME` 与 `MinuteDataSource.DB_HISTORICAL`（line 35-39）。
- `_load_raw_bars_from_tdx()`（line 508-527）调用 `fetch_minute_kline_tdx()`，无缓存、无 fallback；空返回直接抛 `DataUnavailableError`（line 522-527）。
- `_load_raw_bars_from_db()`（line 529-561）直接 SQL 查 `market.kline_minute_raw`；空结果 fail-fast（line 545-549）。
- session 模式与数据源是硬绑定的：`REPLAY_ONLY` → DB（session.py:618-619, 695-696）；`LIVE_ONLY` → TDX（live_session.py:200-204）；`CATCHUP_THEN_LIVE` 同时持有两者（live_session.py:113-115）。任一不匹配抛 `SessionConfigError`。

### 4.3 readiness 数据完整性预检

`readiness.py:69-327`：
- 交易日历（line 133）
- `suspend_d` / `stk_limit` 刷新审计（line 154-158）
- 信号 artifact 生成（line 187-204）
- 每标的分钟线必须存在，否则 `DataUnavailableError`（line 304-327）

### 4.4 残留风险点

需要补强的不是"是否使用实盘数据"，而是"实盘数据完整性的提前提示"：

- 当前 readiness 在用户点击"执行单日运行/就绪检查"时才暴露缺数据，没有"打开页面就主动告知 TDX 接口当前是否可用"。
- 当 ST PIT universe 不覆盖目标交易日（参见 Codex 文档 P0-2，当前 spans 只到 2026-04-30），用户在选股配置阶段看不到这个阻断点，需要等运行后失败。

---

## 5. 用户需求 ⑤：选股/模拟盘必须与 QE 实验同一框架引擎

**结论：架构上不达标**。三套是独立执行栈，仅靠 manifest 字段做"配置一致性"校验，不是同一执行引擎。

### 5.1 三套引擎入口对照

| 系统 | 入口 | 执行环境 | 持仓/订单/执行 |
| --- | --- | --- | --- |
| QE 实验 | `backend/services/quantevolver/executors/backtest.py:27` `BacktestExecutor.submit()` → `ConfigComposer.compose_experiment_in_memory()`（line 1327）→ WSL 上跑 `qrun_limit_minute.py` → Qlib | WSL + Qlib | 全部由 Qlib 内置 `TopkDropoutStrategy` / `ScoreWeightedTopkStrategyV2`（YAML 配置）完成 |
| Selection Center | `backend/services/selection_center/service.py:79` `run_single_package()` → `runtime.build_signal_snapshot()` | AIstock 本地 Python | **不构建持仓**：仅做 score 加载 + risk_policy/tradability 过滤后返回候选股（service.py:151, 162-174） |
| Paper v2 | `backend/services/paper_trading_v2/day_runner.py:77` `run_day()` → `runtime.build_signal_snapshot()` → `TargetPositionEngine` → `RebalanceEngine` → `MinuteExecutionEngine` | AIstock 本地 Python | 由 strategy_package/runtime.py 的 `_compute_score_weighted_weights()`（line 603）、`_filter_dynamic_ndrop()`（line 551）、`_can_sell_under_hold_thresh()`（line 667）等完成 |

### 5.2 共享 vs 独立的清单

**共享**：
- `StrategyPackage manifest`（frozen JSON）
- 因子推理脚本 `scripts/strategy_package_live_inference.py` 与 `backend/inference_engine.py`（Selection 与 Paper 复用，QE 不复用——QE 走 Qlib MLflow）

**独立**：
- 持仓权重算法（Paper 在 Python，QE 在 Qlib YAML 配置）
- 动态 n_drop（Paper 在 runtime.py:551，QE 在 Qlib 内）
- 持仓锁定期 hold_thresh（仅 Paper 实现）
- 风险策略过滤（Paper 在 day_runner.py:178-191，Selection 在 service.py:162-193，**两边各自一份**）
- 行业黑名单/停牌过滤（Paper 与 Selection 各自实现）

### 5.3 backtest_contract 一致性校验的局限

`backtest_contract.py` 校验的是 9 个字段是否相等（topk / HMM / blacklist / tradability / risk_policy enabled / minute_execution algo_code / strategy_family 等），不能保证：

- 三套引擎对同一份 score 数据计算出的权重一致（Qlib 实现 vs runtime.py:603 实现）
- 三套引擎的 `enable_dynamic_ndrop` / `max_n_drop` / `min_n_drop` / `threshold_method` / `threshold_floor` 行为一致（contract 没有覆盖这些动态参数的语义校验）
- 三套引擎的数据窗口宽度一致（`backend/inference_engine.py:111-137` 与 `scripts/strategy_package_live_inference.py:80-105` 的窗口缓冲在 WSL 与本地环境可能不同）

### 5.4 风险案例

`ScoreWeightedTopkStrategyV2` 在 backtest_contract.py:28-32 定义了 marker，但 ConfigComposer 中目前查不到 QE 侧 V2 的实现路径——这意味着 Paper v2 端的 V2 权重算法**无法与 QE 端做基线一致性回归**。

---

## 6. 用户需求 ⑥：模拟盘支持回放与实盘运行，回放→实盘平滑转换

**结论：达标**。

- 三种 session mode 已实现：`REPLAY_ONLY` / `LIVE_ONLY` / `CATCHUP_THEN_LIVE`（`backend/services/paper_trading_v2/session.py:687-689`、`live_session.py:200-204`、`live_session.py:113-115`）。
- `CATCHUP_THEN_LIVE` 在 `live_session.py:111-192` 实现自动状态机：先用 DB 历史数据补齐缺失交易日（`_run_historical_catchup`），全部完成后写入 `SWITCHING_TO_LIVE`/`LIVE_INTRADAY` 状态（line 187-191），无需用户重建 portfolio。
- `auto_switch_to_live` 配置项（session.py:147-155）允许 `REPLAY_ONLY` session 在显式开启后自动转 `CATCHUP_THEN_LIVE`，避免重新建会话。
- Codex 文档（line 305）历史标注的 "LIVE_ONLY/CATCHUP_THEN_LIVE intentionally fail-fast 直到增量分钟级执行实现" 在当前代码中不再成立——`_prepare_live_run`/`_process_live_run` 已实现增量分钟执行（live_session.py:256-572）。

需要二次确认的点（不属于"是否实现"，属于"质量"）：

- 增量执行下"何时算一天结束"由 `_finalize_live_day()`（line 733-812）控制；下午 15:00 后是否自动落地、跨日衔接、停盘半日等边界条件需要 e2e 跑通。
- `tick()` 由 scheduler 触发还是由用户手动驱动两种路径都存在（scheduler.py），节奏一致性需要在产品上明确（"每日 09:30 自动启动 tick" vs "用户手工点 tick"），UI 当前两种都暴露。

---

## 7. 阻断性 / 不合理 / 缺失项汇总

| 类别 | 问题 | 严重度 |
| --- | --- | --- |
| 架构 | `backtest_contract` 强行把 6 项配置锁定为"与 QE 一致"，与"策略包只冻结模型+因子"的产品意图相反 | P0 |
| 架构 | 三套独立执行栈，"统一引擎"实际只是字段一致性校验，无法保证语义一致 | P0 |
| 功能 | 日频策略路径完全缺失（仅 1min/5min） | P1 |
| 功能 | 尾盘处理策略概念在代码中不存在 | P1 |
| 流程 | 没有"从 QE source 一键到选股结果"的原子化入口（参见 Codex 文档 P0-3） | P0 |
| 数据 | ST PIT universe spans 当前到 2026-04-30，落后于交易日（参见 Codex 文档 P0-2） | P0 |
| 数据 | 4 个可选包全部 LEGACY_NON_ST_PIT（参见 Codex 文档 P0-1） | P0 |
| 推理 | live inference 冷启动失败 30+ 次（参见 Codex 文档 P0-4） | P0 |
| 推理 | strict feature coverage 可能为 0（参见 Codex 文档 P0-5） | P0 |
| UI | 单页 20-50 个交互元素、运行控制台 722 行 45+ 个交互 | P1 |
| UI | 内部哈希、英文枚举直接展示，无中文映射或语义化别名 | P1 |
| UI | 没有"步骤 1→2→3"流程图；7 个平级 tab 无主从关系 | P1 |
| UI | 错误诊断大量用 JsonPanel 直接 dump 后端结构，非用户向 | P1 |
| UI | 冻结字段与可调字段在表单上无视觉区分，用户不知道"提交后改不动" | P1 |

---

## 8. 待用户决策的设计选择

下列项需要用户在改动前给出方向，本文不作主张：

### 8.1 配置冻结边界

A. **保留现状**：manifest 全部锁定，UI 删掉所有"可改"字段
B. **极简策略包**：manifest 仅保留 `model_asset` + `factor_set` + `alpha_combination_policy`，把 portfolio_policy / risk_policy / HMM / blacklist / tradability / minute_execution 全部移到 Paper portfolio / Selection runtime；引入"偏离 QE 基线"标记
C. **软合约**：保留当前 manifest 字段作为"推荐默认"，但允许 Paper/Selection 显式覆盖并记录偏离声明

用户描述指向 B 或 C，但 B 的影响面最大（需要修 backtest_contract 全部校验逻辑、需要重设计资产准入审计）。

### 8.2 "统一引擎"的真正含义

A. **字段一致**（当前）：用 manifest + backtest_contract 保证三套独立栈使用相同配置
B. **执行栈一致**：让 Selection/Paper 也用 Qlib（在 AIstock 本地或 WSL 跑 Qlib backtest），淘汰 runtime.py 中的独立持仓算法
C. **明确分工**：QE 用 Qlib（重在批量回测），Paper/Selection 用 Python（重在低延迟和实盘），承认是两套引擎，强化字段+输出一致性回归（让两边对同一日期跑一次，比对持仓与净值差异）

### 8.3 UI 简化方向

A. **任务向导式**：三步式向导（① 选 QE source → ② 配置 Selection → ③ 创建 Paper portfolio），所有中间状态隐藏
B. **角色拆分**：研究员视图（看到所有字段、JsonPanel）vs 操作员视图（只暴露"可改"配置 + 一键运行）
C. **重命名 + 折叠**：保留当前结构，把 JsonPanel 全部改成结构化错误卡 + 把英文枚举全部翻译 + 长卡折叠次要字段

### 8.4 日频策略与尾盘处理

A. 暂不支持，明确写入产品边界文档
B. 优先补日频（影响面小，复用 day_runner 即可）
C. 同时补日频和尾盘策略（需新增执行算法 + 新增 minute_execution_policy 子类型）

### 8.5 模块边界（与 Codex 协作）

按照已商定的"模块边界由用户显式声明"，Paper v2 / StrategyPackage / Selection Center 的修改应明确分配给我或 Codex 之一。建议本文涉及的改动尽量落在以下文件集合，且不与 QE 共用执行核心代码：
- `backend/services/strategy_package/`（除了不动 qe_source_resolver 的 manifest 字段定义部分）
- `backend/services/selection_center/`
- `backend/services/paper_trading_v2/`
- `backend/routers/{strategy_packages,selection_center,paper_trading_v2}.py`
- `frontend/src/app/paper-v2/**`
- `frontend/src/lib/paper-v2/**`

QE 执行核心 (`backend/services/quantevolver/`)、RD-Agent worker、Qlib YAML 模板按 Codex memory 的边界不动。

---

## 9. 验证回归矩阵建议（仅清单，不实施）

任何后续变更都需要覆盖以下场景，否则不算闭环：

- 单元
  - StrategyPackage manifest 仅冻结模型+因子的回归
  - Paper portfolio 创建时允许覆盖 9 项中各项的回归
  - backtest_contract 软合约下的偏离声明回归
- API
  - `POST /selection-center/runs` 在 9 项配置自由覆盖时不被驳回
  - `POST /paper-trading-v2/portfolios` 创建时各 runtime 字段独立校验
- 业务
  - QE 单次实验 → 创建包 → Paper 运行（运行时 topk/HMM/blacklist 与 QE 不一致）
  - QE 演进 loop 同上
  - REPLAY_ONLY → CATCHUP_THEN_LIVE 自动转换
- UI E2E
  - 主流程在 5 步内完成
  - 错误展示无 JsonPanel
  - 所有英文枚举有中文/语义化映射
- 一致性
  - 同一 manifest 同一日期，QE backtest 与 Paper v2 运行的 NAV 差异基线
  - Selection candidates 与 Paper target positions 在同一 runtime 下的差异基线

---

## 10. 文件证据索引

后端：

- `backend/services/strategy_package/qe_source_resolver.py:328-628`
- `backend/services/strategy_package/backtest_contract.py:28-556`
- `backend/services/strategy_package/runtime.py:49, 64-69, 551-563, 603, 667`
- `backend/services/strategy_package/live_inference.py:129-155`
- `backend/services/strategy_package/service.py:99-142, 359-392, 645-654`
- `backend/services/selection_center/service.py:79, 104, 151, 162-193`
- `backend/services/selection_center/runtime_profile.py:38-201`
- `backend/services/paper_trading_v2/day_runner.py:77, 125-130, 178-203, 436-448, 591-634`
- `backend/services/paper_trading_v2/live_session.py:60-572, 614-647, 733-812`
- `backend/services/paper_trading_v2/session.py:147-155, 607-696, 757`
- `backend/services/paper_trading_v2/replay.py`
- `backend/services/paper_trading_v2/market_data.py:35-39, 228-561`
- `backend/services/paper_trading_v2/readiness.py:69-327`
- `backend/services/paper_trading_v2/models.py:37`
- `backend/services/quantevolver/executors/backtest.py:27-49`
- `backend/services/quantevolver/runtime_contract.py:16-27`
- `backend/inference_engine.py:111-137`
- `scripts/strategy_package_live_inference.py:80-105`

前端：

- `frontend/src/app/paper-v2/layout.tsx:8-16`
- `frontend/src/app/paper-v2/page.tsx:101-171`
- `frontend/src/app/paper-v2/packages/page.tsx:92-330`
- `frontend/src/app/paper-v2/selection/page.tsx:58-468`
- `frontend/src/app/paper-v2/portfolios/page.tsx:209-292`
- `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx:111-722`
- `frontend/src/app/paper-v2/paper-v2.css:51-145`
- `frontend/src/lib/paper-v2/format.ts:22-135`
- `frontend/src/lib/paper-v2/types.ts:229-241`

参考文档：

- `docs/analysis/paper_v2_architecture_flow_and_confirmed_defects_20260507.md`（Codex 同日产出，本文未重复其阻断点清单）
- `docs/standards/aistock_development_standard_v1.1_20260504.md`
- `docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md`
- `docs/architecture/paper_v2_selection_business_flow.md`
- `docs/architecture/strategy_package_authoritative_selection_inference.md`

---

## 11. QE 配置层/执行层复用可行性与统一引擎设计意见

本节回答用户在 2026-05-07 的追问：

> 原来设计思路是 QE 使用统一配置层，组合所有配置；执行层分回测和模拟盘，区别是数据源。检查 QE 是否满足这个要求？模拟盘 v2 能否复用 QE 配置层和执行层？这种架构是否最优？回测和实盘必须统一是否最佳实践？未来公告/财报独立信号、多 alpha 架构如何接入？

### 11.1 QE 当前是否满足"统一配置 + 双执行层"

**结论：部分满足，关键的两个抽象都没真做出来。**

| 维度 | 现状 | 是否就位 | 证据 |
| --- | --- | --- | --- |
| 配置维度覆盖 | universe / portfolio / minute_execution / factor / model / risk / HMM / blacklist 全覆盖 | ✓ | `experiment_config.py:166-230`、`config_composer.py:241-324` |
| 配置输出形态 | 直接组装 Qlib YAML 文本，无中间 IR | ✗ | `config_composer.py:2223-3200+` |
| 数据源参数化 | 仅 `QLIB_DATA_PATH_WSL` / `QLIB_MINUTE_PATH_WSL` 两个全局 env，写死进 YAML；`ExperimentConfig` 中无 `data_source` 字段 | ✗ | `config_composer.py:99-100, 2816-2820` |
| Executor 抽象基类 | `BaseExecutor.submit(config, ctx) → ExecutionResult` 接口干净 | ✓ | `executors/base.py:32-43` |
| Executor 实现多样性 | 唯一实现 `BacktestExecutor`，硬编码 Qlib YAML + WSL `qrun_limit_minute.py --backtest-only` + RDAgent 提交 | ✗ | `executors/backtest.py:78-90, 127-130, 163-171` |
| Paper / Live executor 痕迹 | 全库零痕迹，无 stub / TODO / 注释 | ✗ | （全库未命中） |
| `qe_selection_service.py` 角色 | 不是执行器，是"复用 QE 训练资产做推理生成信号"的离线服务，与 selection_center 平行 | n/a | `qe_selection_service.py:1-12` |

也就是说：**"统一配置 + 可插拔执行"是讲故事时的样子，落地是"Qlib 配置生成器 + Qlib 提交器"**。配置维度统一是真的，但配置→Qlib YAML 是直通车，数据源没参数化，执行层只是个壳。

### 11.2 复用 QE 配置层+执行层做 Paper v2 的可行性

**可行，但工作量被低估。Explorer agent 估"2–3 周中等改造"偏乐观，真实成本应在 4–8 周，关键障碍如下：**

1. **必须先把配置层抽出中间 IR**。当前 `_compose_conf_yaml()` 把"语义"和"Qlib 表达"耦合（如 `port_analysis_config` 直接写 Qlib 类名）。要做"双 backend"必须先把配置抽成与执行无关的 dataclass，再分别有 `to_qlib_yaml()` / `to_paper_runtime()` 两个 emitter。这一步是真重构。

2. **数据源必须从环境级抬到配置级**。当前 `os.getenv` 全局变量意味着同一进程不能同时跑"回测+实盘"——切换数据源会破坏正在跑的回测。需要把 `data_source` 做成 `ExecutionContext` 的一部分。

3. **PaperExecutor 不是"换个 submit 实现"那么简单**。Paper v2 已有的执行栈（`day_runner` + `TargetPositionEngine` + `RebalanceEngine` + `MinuteExecutionEngine`）已经实现了一套 Python 端持仓/订单/分钟执行。要让它接受 QE IR，要么写适配器（保留现有引擎），要么放弃现有引擎全部走 Qlib（在本地或 WSL 跑非 backtest-only 模式）——后者会推翻 Paper v2 此前所有 ST PIT/分钟撮合修复的工作。

4. **Selection Center 比 Paper 干净**。Selection 没有持仓/订单概念，它就是"配置 → 信号"，作为 IR → SignalEmitter 接入比 PaperExecutor 简单一个量级。

5. **`qe_selection_service.py` 是个反面先例**。它做的就是"复用 QE 训练资产做实盘推理"，但它**没有动 QE 配置层**——它直接读训练好的模型权重和因子序列做推理。这反过来证明：当前 QE "配置层"实际上是 Qlib 配置生成器，要做实盘只能旁路它。

### 11.3 量化机构如何做"统一" —— 现实参考

这是被反复浪漫化的目标。业界共识更接近：**统一"策略逻辑"，不要统一"执行模拟"**。

#### 11.3.1 按层拆开看"是否应该统一"

| 层 | 是否应该统一 | 原因 |
| --- | --- | --- |
| 因子/特征计算 | **必须统一** | 训练-推理偏移（train-serve skew）是首要踩雷点 |
| 信号生成（model.predict、规则打分、事件信号） | **必须统一** | 同上 |
| 仓位构建（topk / 权重算法 / hold_thresh） | **应该统一** | 确保回测净值与实盘一致的 single source of truth |
| 风险/合规过滤（黑名单、停牌、ST、风控） | **应该统一** | 监管也要求 |
| 订单生成（rebalance intent） | **接口统一、实现可分** | 实盘要处理 partial fill / reject / 并发，回测不需要 |
| 撮合/成交模拟（fill 模型） | **不应强行统一** | 回测追求向量化批处理速度；实盘追求事件驱动低延迟。混在一起两边都做不好 |
| 数据访问 | **接口统一、实现分离** | 回测读历史 bar 文件、实盘读 broker tick；行为差太多，DataProvider 抽象比"换路径"更合理 |

#### 11.3.2 主流框架与机构的实际做法

| 主体 | 实际做法 | 对 AIstock 的启示 |
| --- | --- | --- |
| **WorldQuant Brain** | Alpha 用声明式 DSL 定义（Fast Expression），同一份 alpha 表达式自动驱动回测和模拟实盘，执行层独立。**最接近 AIstock 想做的事**。 | 验证了"声明式 IR + 多 backend"可行 |
| **QuantConnect / Lean** | 策略写成 C# 类，`OnData` 回调。同一类在回测和实盘运行，差异在 `IBrokerage`（broker sim vs 真实 broker）和 `IDataFeed`（历史文件 vs broker tick）。**回测也是事件驱动**，速度慢但语义对齐。 | 验证了"接口统一+实现分离"路径，代价是回测速度 |
| **Zipline (Quantopian)** | 同 Lean，事件驱动；live 版本已弃用 | 完全统一引擎在生产上不可持续 |
| **Backtrader / vectorbt / bt** | 仅回测，不做实盘 | 大部分研究框架其实选择不碰实盘 |
| **Qlib（你们正在用的）** | 本质回测引擎，加了 `OnlineSimulator` 做实盘，社区广泛吐槽 `Exchange.deal_order` 基于回测假设做的，硬接实盘出现 order ID/成交时序/撮合规则不一致 | **用 Qlib 做生产级实盘不是好主意** |
| **大型买方（Two Sigma / Citadel / Renaissance / AQR 公开资料）** | 研究端和生产端代码很少真共享。共享的是"特征定义 + 模型权重 + 风控规则"这种**声明性产物**；执行栈是两套独立优化的（研究 Python，生产常见 C++）。**靠 shadow trading 做对账**——生产系统长期 dry-run，对比研究端输出，差异超阈值报警 | 真正的"一致性"是输出层验证，不是代码层共享 |
| **卖方/银行（GS / JPM）** | 两套系统，通过 FIX 消息和风险限额做契约。研究→生产是正式 handoff 流程 | 对你们规模偏重，但"声明性合约"的思路通用 |
| **国内私募（公开技术博客中的高毅、明汯、九坤等片段）** | 核心因子和组合逻辑用类 SQL/类 DataFrame DSL；回测用自研向量化引擎；实盘用独立的事件驱动撮合系统；两边通过"日终对账"验证 | 国内中小私募也是"DSL + 双引擎 + 对账"模式 |

#### 11.3.3 共同模式：声明式 IR + Adapter 层 + 输出对账

成熟的量化系统几乎都收敛到这套架构：

```
┌─────────────────────────────────────────────────┐
│ Strategy Specification Layer (Declarative IR)   │
│  - SignalComponents (alpha 因子 / 事件信号 / 模型)│
│  - CombinationRule (单 alpha / 多 alpha / ML)    │
│  - PortfolioConstruction (topk / 权重 / 锁仓)   │
│  - RiskPolicy (黑名单 / ST PIT / 暴露上限)      │
│  - ExecutionIntent (调仓频率 / 分钟算法 / 费率)  │
└─────────────────────────────────────────────────┘
                      ↓ compile（无外部副作用）
┌─────────────────────────────────────────────────┐
│ Compute Plan / IR (DAG)                         │
│  - 不依赖具体数据源                              │
│  - 不依赖具体执行环境                            │
└─────────────────────────────────────────────────┘
        ↓                    ↓                ↓
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Backtest    │    │ Paper       │    │ Live        │
│ Adapter     │    │ Adapter     │    │ Adapter     │
│ (Qlib/向量) │    │ (Python)    │    │ (QMT/FIX)   │
│ 历史 Provider │    │ DB+TDX     │    │ broker tick │
│ 理想撮合     │    │ 实时撮合     │    │ 真实撮合     │
└─────────────┘    └─────────────┘    └─────────────┘
        ↓                    ↓                ↓
              Output Consistency Tests (Diff Layer)
                  - 同 manifest 同日 NAV/持仓 diff
                  - 超阈值报警，不超不阻断
```

关键设计原则：
- **IR 是声明式的**——只描述"做什么"，不描述"怎么做"
- **adapter 之间不通过代码共享保证一致性**，通过**对账测试**保证
- **数据 provider 是接口，不是路径参数**——实盘 provider 实现订阅/重连/补单/快照恢复；回测 provider 实现向量化批读
- **execution intent 与 execution simulation 解耦**——IR 只说"分钟级 VWAP 调仓"，至于回测里是向量化算 VWAP 还是实盘里下 VWAP 算法单，是 adapter 的事

### 11.4 给 AIstock 的设计意见

#### 11.4.1 总体判断

**目标可达，但要现实化。** "回测和实盘行为完全一致"在工程上不可达，应改为：

> 在统一的 IR 上声明策略；多个 adapter 用各自最适合的方式实现；通过 **shadow run + 输出对账** 保证一致性在可量化的容忍度内。

**不应该追求**：
- 三套引擎用同一份 Python 代码（性能、低延迟、撮合语义不可调和）
- "一行代码改完，回测+模拟盘+实盘自动同步"（这是 IR + adapter 的接口约定能做的，不是单一代码库能做的）
- 完全消除研究-生产差距（连 Two Sigma 都没做到，靠的是 dry-run 监控）

**应该追求**：
- **新增信号类型（公告/财报）只需在 IR 层加一个 SignalComponent 子类型**，三个 adapter 各自实现读取/计算路径——这正是用户描述的"接入 QE 配置层后自动流到 selection/paper/实盘"
- 多 alpha 作为 CombinationRule 的一种，与单 alpha 走同一 IR 路径
- 行为差异通过对账测试量化，差异在容忍度内即视为"统一"

#### 11.4.2 推荐的分阶段实施路径

按工程量和风险递进，**强烈建议按顺序，不要跳阶段**：

**Phase 0：止血与软合约（1–2 周）**
- 把 `backtest_contract.py` 从硬驳回改成"软合约 + 偏离声明"。Paper portfolio / Selection run 允许偏离 manifest 默认值，但偏离项必须显式记录到 portfolio metadata 用于审计
- 新增 `tests/aistock_validation/modules/qe_paper_consistency.md`：对每个 ST PIT 包，跑一次 QE 回测+ Paper v2 重放同日，比对 NAV、持仓集合、换手、费用四个核心指标，差异 > 阈值报警（**这件事是后续所有方案的基线测试，必须先有**）
- UI 上把 9 项配置真正放出来可调（`portfolios/page.tsx`、`run-console/page.tsx`），冻结字段与可调字段视觉区分

**Phase 1：定义 IR（4–6 周，但不动现有代码）**
- 新增 `backend/services/strategy_ir/`，定义 `StrategySpec` Pydantic 模型，先**只覆盖 80% 的常见场景**，其它走 escape hatch（直接传 raw config）
- 字段建议（不是最终方案，待评审）：
  ```
  StrategySpec
  ├── signal_components: list[SignalComponent]
  │   ├── AlphaFactorSignal (现有 QE 因子)
  │   ├── ModelSignal (LGB / NN / 等)
  │   ├── EventSignal (公告 / 财报 — Phase 3 加)
  │   └── ExternalSignal (escape hatch)
  ├── combination: CombinationRule
  │   ├── SingleAlpha
  │   ├── WeightedSum
  │   └── MetaLearner (多 alpha 的预留)
  ├── portfolio_construction: PortfolioSpec
  │   └── topk / weight_method / hold_thresh / n_drop / dynamic_ndrop_*
  ├── risk_policy: RiskPolicySpec
  │   └── st_pit / sector_blacklist / suspended / exposure_limits
  └── execution_intent: ExecutionSpec
      └── rebalance_freq / minute_algo / fee_model
  ```
- 写两个 emitter（**新代码，旧代码不动**）：
  - `to_qlib_yaml(StrategySpec) -> dict`：复刻当前 `ConfigComposer._compose_conf_yaml()` 的输出
  - `to_paper_runtime(StrategySpec) -> PaperRuntimeBundle`：产出 Paper v2 现有引擎需要的 dataclass
- **不切换调用方**——先证明"老配置 → IR → Qlib YAML"在所有现有 QE 实验上字节级等价，再考虑迁移

**Phase 2：DataProvider 抽象（2–3 周，与 Phase 1 并行）**
- `backend/services/data_providers/`，定义 `BarDataProvider` / `EventDataProvider` / `FundamentalDataProvider` 接口
- 三个实现：`QlibBinProvider`（回测）、`TimescaleDBProvider`（Paper 重放）、`TDXLiveProvider`（实盘）
- 把 `paper_trading_v2/market_data.py` 现有的双源逻辑包装进这个接口
- QE Phase 1 暂时不切，仍用环境变量

**Phase 3：事件信号（公告/财报）接入（3–4 周）**
- 在 IR 中新增 `EventSignal` 子类型：
  ```
  EventSignal(
      source="announcement" | "financial_report" | "research_report",
      event_type=...,
      lookback=..., decay=..., threshold=...
  )
  ```
- 在 `signal_components` 列表里和 alpha 因子并列，Combination 层用同一个权重/排序规则处理
- 三个 adapter 各自实现：
  - QlibAdapter：把 EventSignal 编译成额外的 feature column，注入 Qlib data handler
  - PaperAdapter：在 `StrategyPackageRuntime.build_signal_snapshot()` 中并入信号合成
  - LiveAdapter（未来）：订阅事件流（公告推送/财报披露 webhook）实时更新
- **关键**：Phase 1 IR 设计时就要把 EventSignal 接口预留好，不要等到 Phase 3 才补结构

**Phase 4：Multi-Alpha 接入（3–4 周）**
- IR 已经支持 `signal_components: list[...]` 和 `CombinationRule`，多 alpha 在 IR 层是自然的
- 三个 adapter 各自实现 multi-alpha 编译：
  - QlibAdapter：组合多个 alpha 的 prediction，在 Qlib 端融合
  - PaperAdapter：runtime 层并入多 score 加权
- 不需要为多 alpha 新增执行层概念

**Phase 5：迁移（6–10 周）**
- 让 Selection Center 和 Paper v2 的入口从读 manifest 改成读 IR（manifest 仍保留作为"已审计的 IR 快照"）
- QE ConfigComposer 改成"先生成 IR、再 emit YAML"，旧入口保留兼容
- 全部跑通后逐步淘汰旧路径

**Phase 6：实盘 LiveExecutor（独立项目，3–6 个月）**
- 接 QMT / xtquant
- 严格定义 broker adapter 接口（订单状态机、撤单、部分成交、reject）
- shadow trading：先在生产 dry-run 1–3 个月，每天跟 Paper v2 对账
- 对账通过后才允许真实下单

#### 11.4.3 必须先做的"对账测试"（Phase 0 内）

无论后续走 IR 路径还是其他路径，**对账测试都必须先建立**——否则任何架构改动都没有验证基线。

| 对比对 | 输入 | 输出指标 | 容忍度（建议初值） |
| --- | --- | --- | --- |
| QE 回测 vs Paper v2 重放 | 同 manifest、同日期区间、同股票池 | 日 NAV 差异、日持仓 Jaccard、日换手率、日费用 | NAV < 5bp / 持仓 ≥ 95% / 换手差 < 5% / 费用差 < 1bp |
| Selection Center vs Paper v2 信号 | 同 manifest、同日 | 候选股集合一致性、score 排名相关性 | Top-K Jaccard ≥ 90% / Spearman ≥ 0.95 |
| Paper v2 重放 vs Paper v2 实盘（未来） | 同日（实盘当晚回放对照） | NAV 差、持仓差、成交差 | shadow trading 阶段定义 |

差异超阈值不强制阻断，但 portfolio metadata 要记录"本次运行检测到 X bp NAV 偏差"用于审计追溯。这个机制比 `backtest_contract` 的字段硬比较有用得多——它捕获的是行为差，不是配置差。

#### 11.4.4 不应该做的事

- **不要把 Paper v2 现有的 Python 引擎推翻去跑 Qlib OnlineSimulator**。Qlib 实盘成熟度差，强行替换会浪费此前 ST PIT/分钟撮合的所有修复。
- **不要追求 IR 一次覆盖 100% 场景**。设计时留 `raw_config` escape hatch（直接传 dict），先覆盖 80%，剩 20% 后面补，不要 over-engineer。
- **不要在 Phase 0 没做的情况下直接进入 Phase 1**。先有对账测试，否则改 IR 时无法证明"行为没变"，回归测试会变成主观判断。
- **不要把"事件信号"做成 alpha 因子的特例**。它是独立的信号类型（更新频率、衰减、点事件特性都不同），强行套到现有 alpha 框架会污染 alpha 因子的语义。
- **不要让 IR 和 manifest 长期并存**。manifest 设计时未考虑 IR，长期共存会双源不一致。Phase 5 必须收口：manifest 退化为"IR 的冻结序列化产物"，不再独立扩展字段。

### 11.5 与已有 Codex 文档和模块边界的协调

- 本节涉及修改 QE 配置层（`config_composer.py`、`experiment_config.py`），按 `feedback_aistock_codex_alignment.md` 的模块边界规则，**这些文件主要属于 Codex 维护的 QE 共用执行核心**——具体改动归属应由用户在分配任务时显式指定，不在本文默认认领范围。
- `backend/services/strategy_package/`、`selection_center/`、`paper_trading_v2/`、`paper-v2` 前端属于本会话默认范围，Phase 0 的"软合约 + 对账测试 + UI 配置开放"可以在不动 QE 配置层的前提下完成。
- Phase 1+ 涉及 QE 配置层的真正改造前，需要用户与 Codex 协调具体分工，并保留 ST PIT 风险策略和现有 QE 回测稳定性的回归保护。

### 11.6 一句话结论

**"统一引擎"在工程上等价于"统一声明 + 多 adapter + 输出对账"，不等价于"同一份代码"。**
QE 当前的配置层有统一的雏形但绑死 Qlib YAML，要做你描述的架构需要先抽 IR；这条路成熟机构走过、可行，但工作量是季度级的。在动手前，先做"对账测试"作为不可绕过的验证基线，比争论架构方案更重要。

---

## 12. 务实复用路径：基于现有 AIstock 原生策略库（修正 §11 的工作量估算）

本节回答用户在 2026-05-07 的进一步追问：

> QE 整体架构已稳定（数百次回测验证），模拟盘还没成功跑起来。能否对统一配置层做少量修改，优先保证 QE 最小修改，让 Paper v2 尽量复用 QE 成果？不是同一份代码，而是尽量复用类、方法、策略——能否降低工作量？

**结论：可行，且工作量比 §11 的估算大幅下降。原因是 AIstock 已经有一套零 Qlib 依赖的策略库存在，但被 Paper v2 自己绕过了。**

§11 的估算偏悲观，是因为我把"QE 用的策略"等同于"Qlib 内置策略 + 必须有 Qlib 才能跑"。深入取证后情况完全不同。

### 12.1 关键发现：AIstock 已存在两套并行的策略实现

| 路径 | 位置 | Qlib 依赖 | 当前谁在用 |
| --- | --- | --- | --- |
| **AIstock 原生策略库** | `backend/rebalance_strategies/` | **零依赖** | `qe_strategies/topk_dropout_rc_qlib.py`（QE 通过 config_composer 注入） |
| **Qlib 适配版** | `qe_strategies/topk_dropout_rc_qlib.py` | 强依赖 BaseSignalStrategy/Position/Exchange | QE 默认路径（生成 Qlib YAML 时引用） |
| **Paper v2 runtime.py 内嵌实现** | `backend/services/strategy_package/runtime.py:551-679` | 零依赖 | Paper v2 自己（**和原生策略库的逻辑重复**） |

#### 12.1.1 AIstock 原生策略库（已存在的可复用代码）

`backend/rebalance_strategies/` 下的代码完全是 plain Python：

- `base_strategy.py:9-93` `BaseRebalanceStrategy`：抽象基类，签名是
  ```
  generate_orders(score_items, current_positions, portfolio_value, config,
                  signal_date, next_trade_date, portfolio_id, close_price_fn)
  → List[Dict]
  ```
  全部用 Python `Dict` / `List` / `float` / `date`，**不依赖 Qlib 任何对象**。
- `topk_dropout.py:31-152` `TopkDropoutStrategy`：纯 dict/list/set 操作。
- `topk_dropout_rc.py:25-535` `TopkDropoutWithRiskControlStrategy`：继承上面，加上止损、换手率截断、行业 HMM 调整；唯一外部依赖是 `db/pg_pool`（数据访问），**不是 Qlib**。
- `registry.py`：注册表机制，QE 的 `config_composer.py:167-171` `QE_LOCAL_STRATEGY_ROOTS` 已经把这个目录列为搜索路径。

#### 12.1.2 QE 的策略类路由能力（已存在）

`config_composer.py:2531-2571` 的策略类生成逻辑允许 QE 在生成 YAML 时引用**任意自定义类**，而不仅是 Qlib 内置类：
```
QE_LOCAL_STRATEGY_ROOTS = [
    AISTOCK_PROJECT_ROOT / "backend" / "rebalance_strategies",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies",
    AISTOCK_PROJECT_ROOT / "scripts",
]
```
也就是说 QE 已经具备"用 AIstock 自研策略类做回测"的路由机制；**只是当前生产路径默认走 `qlib.contrib.strategy.signal_strategy.TopkDropoutStrategy`**（config_composer.py:2517-2530）。

#### 12.1.3 Paper v2 的尴尬现状

`backend/services/strategy_package/runtime.py:551-679` 把 `_compute_score_weighted_weights` / `_filter_dynamic_ndrop` / `_compute_threshold` / `_can_sell_under_hold_thresh` 重新写了一遍，**和 `backend/rebalance_strategies/` 里的实现做的是同一类事**。但两者：
- 接口不同：runtime.py 用 `SelectionCandidate` dataclass，rebalance_strategies 用 `Dict`
- 调用入口不同：runtime.py 由 `TargetPositionEngine` 直接拼装，rebalance_strategies 通过 `BaseRebalanceStrategy.generate_orders()` 统一入口
- 结果是：QE 跑回测用的是 rebalance_strategies（如果 QE 用了自研类）或 Qlib 内置；Paper v2 跑模拟盘用的是 runtime.py。**同一份产品逻辑写了两遍**。

### 12.2 已部分统一的部件

下列代码当前已经是"无 Qlib 依赖、可三方共用"的状态，只是没有被强制成"唯一入口"：

| 模块 | 文件 | 当前用户 | 备注 |
| --- | --- | --- | --- |
| 风险策略接口 | `backend/services/selection_center/risk_policy.py:1-151`（`RiskDecision`、`StPitRiskDecisionProvider.evaluate`） | Paper v2、Selection Center 都在用 | QE 还在用并行的 JSON 文件 |
| 停牌/黑名单过滤 | `backend/services/selection_center/tradability.py:21-160` | Paper v2、Selection Center 都在用 | 同上 |
| 执行算法基类 | `backend/execution_algos/base_algo.py:31-79` `BaseExecutionAlgo` | Paper v2 在用 | QE 在 YAML 里另外写 inner_strategy |
| V25 执行核心算法 | `backend/execution_algos/v25_core.py:1-100` 文件头明确声明"independent from Paper v2, Qlib, databases, or API objects" | Paper v2 在用，QE 通过适配器 | **本身就是按"共享算法核心"写的** |
| 因子推理 | `scripts/strategy_package_live_inference.py` + `backend/inference_engine.py` | Paper v2、Selection Center 共享 | QE 走 Qlib MLflow，是不同路径 |

也就是说：**风险策略、tradability、执行算法、推理脚本这四类已经是共享或可共享状态**。Paper v2 没成功跑起来，主要不是"缺一个统一引擎"，而是 §0/§7 列出的那些具体阻断点（ST PIT spans、live inference 冷启动、UI 流程缺口、配置硬约束）。

### 12.3 务实方案：Paper v2 接入 AIstock 原生策略库（不抽 IR）

下列方案完全不动 QE 配置层、不抽 IR、不改 Qlib 适配；只让 Paper v2 与 QE 在 Python 类层面真正共享同一套策略实现。

#### 12.3.1 改动清单

1. **Paper v2 `TargetPositionEngine` 改为调用 `BaseRebalanceStrategy`**
   - 当前 `runtime.py` 中 `_compute_score_weighted_weights` / `_filter_dynamic_ndrop` 等私有方法可以保留作为内部工具，但持仓决策的主入口换成实例化 `TopkDropoutStrategy` / `TopkDropoutWithRiskControlStrategy`，调用 `.generate_orders()`，把返回的 `List[Dict]` 翻译成 Paper v2 的 `OrderIntent`。
   - 涉及文件：`backend/services/strategy_package/runtime.py`、`backend/services/paper_trading_v2/day_runner.py`。
   - 工作量估计：1 周。

2. **QE 默认策略改为 AIstock 自研类**（可选，但对长期一致性最关键）
   - `config_composer.py:2517-2530` 把默认 `TopkDropoutStrategy` 改成 `backend.rebalance_strategies.TopkDropoutStrategy`（QE 已有 `QE_LOCAL_STRATEGY_ROOTS` 路由，Qlib YAML 里支持 `module: "custom_strategy"`）。
   - 这一步**非强制**——只要 QE 默认 Qlib 内置策略和 AIstock 原生策略在算法上等价，就可以保留 QE 现状。但必须对账验证（见 §12.4）。
   - 工作量估计：QE 本身 0.5 周（仅替换默认 import path）+ 大量回测对账（2-4 周）。
   - **属于 Codex 维护范围**，本会话不会执行，仅在用户分配后协调。

3. **Selection Center / Paper v2 的风险策略统一入口**
   - 当前 `selection_center/risk_policy.py` 和 `tradability.py` 已经被 Paper v2 共用，进一步要做的是让 QE 也走这套接口。
   - QE 端是用 JSON 文件 (`suspend_filter.json` / `qe_event_risk_policy.json`)；改成在 Qlib custom_strategy 内部 import Selection Center 的 provider 即可（这些 provider 是 plain Python，可被 Qlib 进程调用）。
   - 工作量估计：1-2 周。
   - **属于 Codex 维护范围**。

4. **执行算法集中到 `backend/execution_algos/`**
   - 让 QE YAML 里的 `inner_strategy` 直接引用 `backend.execution_algos.*` 的类（QE 已有 custom_strategy 路由能力）。
   - V25_core.py 已是按"共享核心"写的，正好适用。
   - 工作量估计：1 周。
   - **属于 Codex 维护范围**。

5. **对账测试基础设施**（不可绕过，先做）
   - `tests/aistock_validation/modules/qe_paper_consistency.md`
   - 同 manifest 同日：QE 回测 NAV/持仓/换手 vs Paper v2 重放 NAV/持仓/换手，差异 > 阈值告警
   - 工作量估计：1-2 周。
   - **本会话默认范围内**。

#### 12.3.2 修正后的总工作量

按上面拆分：
- 落在我（Paper v2/Selection Center 模块边界内）的工作：**3-5 周**（runtime.py 接 rebalance_strategies + 对账测试 + UI 配置开放）
- 落在 Codex（QE 配置层最小改动）的工作：**4-7 周**（QE 默认策略替换 + 对账验证 + 风险策略统一入口 + 执行算法迁移）
- **总计 7-12 周**，远低于 §11 的 4-8 周/Phase + 后续若干 Phase 的季度级工作量

§11 那个"完整 IR + Adapter + 多阶段"的方案保留作为**长期目标**——上述 §12.3 都做完之后，再考虑是否值得把声明性 IR 抽出来。但短期内不需要走那条路。

### 12.4 风险与注意事项

#### 12.4.1 等价性验证不可省

把 QE 默认策略从 Qlib `TopkDropoutStrategy` 切到 `backend.rebalance_strategies.TopkDropoutStrategy` 的前提是**两者算法等价**。当前没有任何回归证据证明这点。必须：

1. 先在测试环境用一个小样本（10 只股票、半年回测）跑两版本对比 NAV/持仓/换手
2. 确认差异在容忍度内（建议 NAV < 5bp、持仓 Jaccard ≥ 95%）才能切默认
3. 旧 Qlib 版本保留作为降级选项至少 3 个月

#### 12.4.2 Qlib `Position`/`Exchange` 的语义差异

QE 端 Qlib 跑回测时，撮合是由 Qlib `Exchange.deal_order` 决定的；Paper v2 端撮合由自己的 `MinuteExecutionEngine` 决定。**即使策略类共用**，撮合差异仍可能产生 NAV 差。这部分必须靠 §12.3 第 5 点的对账测试持续监控，不能靠"代码共享"消除。

#### 12.4.3 hold_thresh / 动态 n_drop 的语义

`backend/rebalance_strategies/topk_dropout_rc.py` 与 `runtime.py:551-679` 在动态 n_drop / hold_thresh 上**实现细节不完全等价**（前者是基于换手率上限截断，后者是基于 score 差阈值动态决定）。如果 QE 当前生产用的是 Qlib 内置 `TopkDropoutStrategy`（不支持动态 n_drop），那么 QE 上根本没有动态 n_drop 行为；此时 Paper v2 的动态 n_drop 是"超出 QE 范围的特性"——对账时要明确把这种"已知差异源"标记出来，不能笼统归类为 bug。

#### 12.4.4 多 alpha 与事件信号

§11.4.2 Phase 3-4 描述的多 alpha + 事件信号接入，在 §12 务实方案下仍然适用——只是不需要先抽 IR：
- **多 alpha**：在 `BaseRebalanceStrategy.generate_orders` 接收的 `score_items` 上扩展，每个 component 一个 score column；权重融合在策略类内部完成。无需新增配置层概念。
- **事件信号**：作为新的 `score_items` 数据源（公告/财报信号生成器输出与 alpha 因子并列的 score column），同样在 `score_items` 层混入。无需改策略类。

这意味着 §12 路径不仅成本低，对未来扩展也是兼容的——抽 IR 是为了"更优雅"，不是为了"才能做"。

### 12.5 修正后的推荐路径

按照 §12.3 拆分：

| 阶段 | 内容 | 工作量 | 模块归属 | 阻塞依赖 |
| --- | --- | --- | --- | --- |
| **0** | 软合约（backtest_contract 改硬驳为偏离声明）+ UI 9 项配置开放 + 对账测试基础设施 | 2-3 周 | Paper v2/Selection Center | 无，可立即开始 |
| **1** | Paper v2 runtime.py 接入 backend/rebalance_strategies/，删除重复实现 | 1-2 周 | Paper v2 | 阶段 0 对账测试就位 |
| **2** | 修复 §0/§7 的具体阻断点（ST PIT spans 刷新、live inference 冷启动 preflight、QE source 一键选股入口、UI 简化） | 3-5 周 | Paper v2/Selection Center | 阶段 1 完成 |
| **3** | （需 Codex 协调）QE 默认策略切到 AIstock 原生类 + 风险策略统一入口 | 4-7 周 | QE 配置层 | 阶段 1 对账验证 |
| **4** | 多 alpha 与事件信号接入（在 score_items 层扩展） | 4-6 周 | Paper v2/Selection Center 主体 + 与 Codex 协调 QE 端 | 阶段 3 完成 |
| **5**（可选长期） | 抽出声明性 IR，QE/Paper/Live 三个 emitter | 8-12 周 | 跨模块 | 阶段 4 完成且确实需要 |

阶段 5 不是必须的——如果阶段 0-4 已经能稳定支持新策略接入和实盘上线，**可以永远不做**。这就是务实路径。

### 12.6 一句话修正结论

**不需要抽 IR。AIstock 已经有零 Qlib 依赖的策略库（`backend/rebalance_strategies/`）和共享的风险/执行/推理模块；Paper v2 没成功跑起来不是因为缺统一引擎，是因为它绕过了这套库自己写了一遍 + 配置硬约束 + 阻断点没修。**
**先让 Paper v2 用上这套库（1-2 周）+ 修阻断点（3-5 周）+ 软合约 + 对账测试（2-3 周），就能在不动 QE 主干的前提下基本满足"统一行为"的目标。后续 QE 默认策略迁移和事件信号扩展属于在此基础上的小步演进，不是大工程。**

> **⚠️ 12 章重要更正见 §13.1。** §12 的核心论断"AIstock 已有的策略库 QE 也能直接用"在重新核证后被推翻：QE 走 Qlib 风格策略（继承 `BaseSignalStrategy`），`backend/rebalance_strategies/` 走 plain Python 风格（继承 `BaseRebalanceStrategy`），两套接口不兼容；§12.3 第 2 项"切默认策略到原生类"实际不可行。请读者按 §13 调整结论。

---

## 13. §12 重大更正 + 开源参考合规性审计

### 13.1 §12 的核心论断需要更正

经直接核查 `config_composer.py:167-171, 444-461, 2517-2571, 4494` 与 `qe_strategies/topk_dropout_rc_qlib.py:14-95` 后确认，§12 中对"策略类共享"的判断有误，必须更正。

#### 13.1.1 `QE_LOCAL_STRATEGY_ROOTS` 的真实用途

`config_composer.py:167-171`：

```python
QE_LOCAL_STRATEGY_ROOTS = [
    AISTOCK_PROJECT_ROOT / "backend" / "rebalance_strategies",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies",
    AISTOCK_PROJECT_ROOT / "scripts",
]
```

实际用途由 `_resolve_strategy_dependency_path()`（line 444-461）和 `_resolve_strategy_dependency_code()`（line 463-479）决定：**它是用于在生成 QE workspace 时把策略源代码 `.py` 文件作为依赖打包到 WSL** 的搜索根目录，**不是 "QE 可以直接调用 AIstock 自研策略类" 的机制**。被打包的策略仍在 Qlib 框架内运行（`module: custom_strategy`）。

#### 13.1.2 QE 的实际策略路由

`config_composer.py:2516-2571`：

- **默认**（line 2517-2519，硬编码）：
  ```python
  strategy_class = "TopkDropoutStrategy"
  strategy_module = "qlib.contrib.strategy.signal_strategy"
  ```
  即 Qlib 内置类。
- **用户自选**（line 2531-2571）：从 `aistock_strategy_catalog` 数据库的 `strategy_info["source_code"]` 读源代码，写到 WSL 当作 `custom_strategy` 模块。
- **强制约束**（line 4494）：
  ```python
  valid_base_classes = {"BaseSignalStrategy", "BaseStrategy", "TopkDropoutStrategy", ...}
  ```
  即使是自研策略，也必须继承 Qlib 基类才能通过校验。

#### 13.1.3 两套并存且不兼容

| 体系 | 基类 | 主入口 | 运行环境 |
| --- | --- | --- | --- |
| Qlib 风格（QE 在用） | `qlib.contrib.strategy.signal_strategy.BaseSignalStrategy` | `generate_trade_decision(self, execute_result=None)` 依赖 `self.trade_position` / `self.trade_exchange` / `self.signal` | 必须在 Qlib backtest 框架内（WSL+Qlib） |
| Plain Python 风格（Paper v1 历史代码） | `backend.rebalance_strategies.base_strategy.BaseRebalanceStrategy` | `generate_orders(score_items, current_positions, ..., close_price_fn)` 接收/返回 `Dict`/`List` | 任何 Python 进程 |

**两者名字接近、行为接近，但接口完全不兼容**——`generate_trade_decision()` 与 `generate_orders()` 签名、返回值、调用上下文都不同。把 `backend/rebalance_strategies/` 的类塞给 QE 跑回测会被 line 4494 校验直接拒收。

#### 13.1.4 修正后的真实图景

| 共享层 | 是否真共享 | 备注 |
| --- | --- | --- |
| 因子推理 | Paper v2 / Selection 共享 `inference_engine.py` + `strategy_package_live_inference.py`；**QE 不共享**（走 Qlib MLflow） | 短期内不可合并 |
| 策略类（持仓决策） | **三套独立**：QE 用 Qlib 风格；Paper v2 自己写在 `runtime.py:551-679`；`backend/rebalance_strategies/` 闲置（仅给 Paper v1 历史代码用） | §12 复用判断**有误** |
| 风险策略 | Paper v2 / Selection 共享 `selection_center/risk_policy.py` 和 `tradability.py`（plain Python）；QE 走并行 JSON 文件 | 可共享：QE custom_strategy 内部 import Selection Center provider 可行 |
| 撮合 | **三套独立**：Qlib `Exchange` / Paper v2 `MinuteExecutionEngine` / 未来实盘 broker | 业界共识不应统一 |
| 执行算法核心 | `backend/execution_algos/v25_core.py` 文件头声明独立于 Qlib，但 QE 端 inner_strategy 各写适配 | 算法可共享，adapter 各写 |

#### 13.1.5 撤回 §12.3 第 2 项

§12.3 第 2 项写的"把 QE 默认从 `TopkDropoutStrategy` 切到 AIstock 自研类"含混了两件事：

A. **切到 AIstock 维护的 Qlib 风格类**（仍继承 `BaseSignalStrategy`）：可行，但**对实盘统一无帮助**——还是 Qlib 依赖。好处仅是把"默认策略代码 single source of truth"从 Qlib 仓库挪到 AIstock 仓库。

B. **切到 `backend/rebalance_strategies/` 里的 plain Python 类**（继承 `BaseRebalanceStrategy`）：**不可行**——QE 校验拒收（line 4494）、Qlib 调用约定不匹配、`generate_orders` 接口接不上 Qlib backtest 框架。

我在 §12 想表达的实际是 B（因为 B 才能与 Paper v2 共享代码），但 B 在当前 QE+Qlib 架构下做不到。

#### 13.1.6 Qlib 依赖在实盘的本质问题

QE 的所有策略类（默认 + 自研）都继承 `BaseSignalStrategy`，强依赖：
- `self.trade_position`：Qlib `Position` 对象
- `self.trade_exchange`：Qlib `Exchange` 对象（含 `deal_order` 理想化撮合）
- `self.trade_calendar`：Qlib 预排日历
- `self.signal`：Qlib signal loader

**这套依赖在真实实盘中不成立**：

| 维度 | Qlib 假设 | 真实实盘 | 后果 |
| --- | --- | --- | --- |
| 信号 | `signal.get_signal()` 一次性返回全市场 score | 事件驱动，按 tick/分钟更新 | 调用约定不兼容 |
| Position | 内存对象，调用即拿到准确余额 | Broker 异步推送，有延迟、可能因 reject 滞后 | 仓位状态机不同 |
| 撮合 | `Exchange.deal_order` 理想化（按 deal_price 立即全成交） | partial fill / reject / 撤单 / 排队 | 完全不同的订单状态机 |
| Calendar | 预排日历按 step 驱动 | 实时时钟 + 交易所事件（停牌/集合竞价/熔断） | 时间驱动机制不同 |
| 错误处理 | 假设市场总有交易对手 | 涨跌停/流动性不足/broker 拒单 | Qlib 没有相应代码路径 |

Qlib 的 `OnlineSimulator` 本质是"按真实日期重放历史"，**不是接 broker 的实盘**。Paper v2 现在用自己的 `MinuteExecutionEngine` 而非 Qlib OnlineSimulator，正是因为 Qlib Online 路径不到生产级——**这反而是 Paper v2 设计上正确的部分**。

**结论**：Paper v2 与 QE 在策略主干层不能共享同一份代码（除非整体抛弃 Qlib，回到 §11 的季度级工作量）。能省工作量的复用只在三个细分点：
1. **风险策略接口**（让 QE custom_strategy 内部 import Selection Center plain Python provider）
2. **执行算法核心**（让 QE inner_strategy 引用 `execution_algos/v25_core.py`）
3. **因子推理**（短期不可合并，QE 离不开 Qlib MLflow）

#### 13.1.7 §12 工作量估算的修正

§12.5 表里的 "7-12 周总计" 在数字上仍接近正确，但**理由要重新表述**：

- **不是因为** "Paper v2 接入 rebalance_strategies/ 替代 runtime.py"——这条路 QE 端用不上，Paper v2 接 rebalance_strategies/ 也没有"与 QE 同源"的好处（最多只是减少 Paper v2 内部重复实现）。
- **而是因为** Paper v2 自身的修稳定 + 修阻断点 + 软合约 + 对账测试 + UI 简化 这些事本身就是这个量级的工作。
- 修正后的 Phase 1 应改为："Paper v2 内部把 `runtime.py:551-679` 的算法整理到一个独立的 `paper_trading_v2/strategy/` 子模块"，作为 Paper v2 自身的内部清理，**不再宣称这等同于"与 QE 共享"**。
- 与 QE 真正的共享只发生在 §13.1.4 列出的三个细分点上，这是对 Codex 模块的修改，不在本会话默认范围内。

### 13.2 Paper v2 设计是否真的"参考开源社区成熟产品"

**结论：设计文档明确写了要参考 vn.py，但实现层面几乎完全没有落实。**

#### 13.2.1 设计文档的明确要求

`docs/architecture/paper_trading_v2_top_level_design.md`：

- §0/§5（line 51-52）："新的交易中心尽量参考和局部复用 vn.py：参考 vn.py 的事件驱动、对象模型、OMS、撮合、风控、网关分层"
- §8（line 431-454，整节叫"参考 vn.py 的设计与可复用点"）：明确列出五点可参考/复用的模式
  - 事件驱动 EventEngine
  - 交易对象模型（OrderData / TradeData / PositionData / AccountData）
  - Gateway 抽象 / SimBrokerAdapter
  - OMS（订单管理系统）
  - Backtesting 撮合流程
- §16 参考资料（line 892-898）列出 vn.py GitHub 与文档地址
- §15（line 721）："参考 vn.py 对象模型，定义 AIstock Order/Trade/Position/Account/Event"

`docs/architecture/paper_trading_v2_implementation_plan.md` §4.1（line 207-263）也做了同样的承诺：
- Order/Trade/Position/Account/Bar/Tick 数据类参考 vn.py 命名与字段
- 参考 vn.py EventEngine 模式
- 参考 vn.py Gateway 抽象（虽然当前只实现 SimBrokerAdapter）
- 参考 vn.py 把委托/成交/持仓/账户统一纳入 OMS

#### 13.2.2 实现层面的落实情况（核证 = 零落实）

实际代码搜索结果（`backend/services/paper_trading_v2/` + `backend/services/strategy_package/` 全目录）：

| 设计承诺 | 是否落实 | 证据 |
| --- | --- | --- |
| EventEngine（事件驱动） | ❌ | 全目录搜索 `EventEngine` / `class Event ` / `subscribe` / `publish` / `on_tick` / `on_bar` / `on_order` / `on_trade` 全部零命中 |
| OrderData / TradeData / PositionData / AccountData | ❌ | 实际命名是 `PaperPortfolio` / `PaperRun` / `OrderExecutionState` / `IntradaySnapshot` 等，与 vn.py 命名体系完全不同（`models.py:29-387`） |
| Gateway 抽象 | ❌ | 全目录搜索 `Gateway` / `BaseGateway` 零命中；`broker` / `Adapter` 也零命中 |
| SimBrokerAdapter | ❌ | 该类不存在 |
| OMS（订单管理系统） | ❌ | 没有独立 OMS 类，订单生命周期由 `PaperTradingLiveMinuteExecutor.tick()` 内联管理 |
| 策略生命周期回调（`on_init`/`on_start`/`on_tick`/`on_bar`/`on_order`/`on_trade`/`on_stop`） | ❌ | 全部零命中。Paper v2 是"由外部驱动 `tick()` / `run_day()`"的命令式编排，不是 vn.py 的"策略类响应回调"模式 |

实际架构（从 `class` 列表逆推）：

- `PaperTradingDayRunner`（day_runner.py:41）：日级批处理编排器
- `PaperTradingLiveMinuteExecutor`（live_session.py:60）：有 `tick()` 方法但由外部调度器调用
- `PaperTradingV2Repository`（repository.py:68）：存储层
- `PaperTradingV2Runner`（runner.py:59）：运行器
- `PaperTradingV2SessionScheduler`（scheduler.py:25）：调度器

是 **Repository + Service + Scheduler + 命令式 tick 轮询** 风格，更像 Web 后端服务的架构，**不是 vn.py 的事件驱动 + 网关 + OMS 风格**。

#### 13.2.3 这种偏差的代价

设计偏差对当前 Paper v2 的"跑不起来"和"难扩展实盘"是有直接关联的：

1. **事件驱动缺失**：实盘必然是事件驱动（broker 推送 tick / order update / fill），而当前架构是"调度器定时调 `tick()` 主动拉取分钟线"。从模拟盘转实盘时需要重写信号处理路径——这不是接一个新 broker 就能完成的。
2. **Gateway 抽象缺失**：现在没有 `BaseGateway`，未来接 QMT / xtquant / 真实券商时，每接一家就要在 `live_session.py` 里改 `MinuteDataSource` 枚举 + 改 `_load_raw_bars_*` 函数 + 改订单提交路径。vn.py 风格下应该是"新增 `XtQuantGateway(BaseGateway)` 类、其它代码不动"。当前架构做不到。
3. **订单状态机弱化**：vn.py 的 `OrderData.status` 是完整状态机（`SUBMITTING`/`NOTTRADED`/`PARTTRADED`/`ALLTRADED`/`CANCELLED`/`REJECTED`），AIstock 的 `OrderExecutionState`（models.py:329）字段简化，不能完整刻画实盘订单生命周期（partial fill 多次更新、撤单失败重试、reject 后状态等）。
4. **OMS 缺失**：vn.py 把订单/成交/持仓/账户统一在 OMS 里通过事件同步状态，AIstock 当前是各自直接读写 DB 表，没有内存中的"事实状态"层。实盘下并发更新（tick 推送 + 用户操作 + 风控干预同时发生）会很难保证一致性。
5. **策略生命周期回调缺失**：vn.py 策略写一遍 `on_tick`/`on_bar`/`on_trade`/`on_order` 后回测和实盘自动复用——这正好是用户期望的"配置和逻辑统一"。AIstock 当前没有这层接口，所以 QE 端 Qlib 策略和 Paper v2 端的 day_runner 各写一份。

#### 13.2.4 客观判断

这不是说 vn.py 是唯一正确答案——它是一种成熟方案，AIstock 也可以选其他方案（QuantConnect Lean 风格、NautilusTrader 风格等）。但**设计文档明确承诺要参考 vn.py 五大要点，实现却一项都没落实**，这是 design-implementation gap。

可能的原因（推测，没有证据）：
- 早期 Paper v2 实现优先满足"复用已有 Selection Center / 数据库表 / FastAPI 路由"等近便的工程结构
- 没有专门的 trading core 工程师把 vn.py 的事件驱动模型嫁接进 Web 后端
- `paper_trading_v2_implementation_plan.md` §4.1 的 "**借鉴改造**" 在执行时被默认成 "**重写**"，结果是失去了 vn.py 验证过的语义保证

#### 13.2.5 这一节对架构方案的影响

如果将来想真正接实盘（QMT / xtquant），三个选择：

A. **认账重写**：花时间把 EventEngine + Gateway + OMS 基础设施补上，回到原始设计承诺。工作量 6-10 周。风险：会动到 day_runner / live_session 主干，但有 vn.py 文档可参考。
B. **不参考 vn.py，但补齐订单状态机和 broker 抽象**：保留当前 Repository+Service 风格，单独把 `BaseBrokerAdapter` / `OrderState` 状态机做完整。工作量 3-5 周。代价：失去 vn.py 验证过的事件驱动语义，长期维护成本更高。
C. **保留模拟盘现状，接实盘时另起项目**：承认 Paper v2 是模拟盘专用，不强求统一。实盘用 vn.py 或其他成熟产品独立实现。代价：研究→实盘还是要做适配层，但 Paper v2 不阻塞。

**这个选择应当与 §11.4 / §12.5 的总体路径选择一起拍板**——如果坚持 §12 的"务实路径"（不抽 IR、Paper v2 自身演进），那么 B 选项最现实；如果接受 §11 的"长期 IR"路径，则 A 选项与 IR 抽取可以一起做（IR 抽取本身要重新整理 trading core，正好补 vn.py 抽象）。

#### 13.2.6 给用户的建议（不是决定）

短期：把这个 design-implementation gap 作为已知技术债登记到 `docs/architecture/` 下的"技术债记录"，避免后续讨论中反复发现"原来设计文档是这么写的"。

中期：在 §12 的 Phase 0-2 内，至少补两件 vn.py 风格的最低限度基础：
- `OrderState` 状态机（参照 vn.py `OrderData.status`）至少能区分 `PENDING / SUBMITTED / PARTIAL_FILLED / FILLED / CANCELED / REJECTED`
- `BaseBrokerAdapter` 接口（即使先只有一个 `SimBrokerAdapter` 实现），把 `live_session.py` 里硬编码的 broker 逻辑挪进去

这两步成本不大（1-2 周），但可以为未来接实盘留接口。比"完整重写 vn.py 那套基础设施"现实，比"不动它"对未来更友好。

长期：如果决定走 §11 的 IR 路径，把 vn.py 抽象一起补；如果走 §12 的务实路径，至少把 §13.2.6 的中期两件做掉。

---

## 14. 整体接入 vn.py + miniQMT 路径深度评估

### 14.1 用户提出的问题陈述（2026-05-07）

> 未来对接的第一个模拟盘和实盘行情是 miniQMT；所有真实成交和对账都以 miniQMT 为准；AIstock 接入实盘后本地账单/成交都不是权威数据；vn.py 已实现与 miniQMT 的对接；从 0 开发交易系统风险和工作量太大。是否复用 vn.py 的架构甚至代码是最佳选择？按这个思路是否还需要更大改动？

### 14.2 用户的三个判断逐项验证

#### 14.2.1 "miniQMT 是权威账户/成交源"——**完全正确，是金融 IT 硬约束**

任何接 broker 的实盘系统都必须把 broker 当作 source of truth，本地账本只是 cache + 审计副本。原因：
- 监管对账要求成交数据可溯源到柜台；
- broker 端有撤单失败、部分成交、reject 等异步事件，本地推断的状态可能与柜台不一致；
- 资金/持仓/可用额度只有柜台知道（融资融券、T+1、解冻等规则在柜台执行）；
- 跨日/断线/重启后必须以柜台为准重建本地视图。

这不是架构选择，是**做实盘的前置条件**。AIstock 现在 `paper_trading_v2/repository.py` 把 DB 当事实表的设计模型在接入实盘后**必须翻转**——DB 退化为快照/日志，事实在 broker 那里。

#### 14.2.2 "vn.py 已实现 miniQMT 对接"——**基本正确，但需技术验证**

社区情况（基于公开信息）：
- vn.py 主仓库 `vnpy/vnpy` 提供事件驱动核心（EventEngine、MainEngine、BaseGateway、OmsEngine、CtaTemplate）；
- miniQMT 网关由社区独立项目 `vnpy_xt` 维护（封装迅投 xtquant Python SDK）；
- 国内多家中小私募在生产使用 vnpy_xt + miniQMT 组合。

**但落地前必须验证**（不能拍胸脯）：
- `vnpy_xt` 当前维护活跃度、最近 commit 时间、issue 响应速度；
- 与 AIstock 当前 miniQMT 版本（`backend/infra/qmt_client.py:1199` 行自研客户端对应的 xtquant 版本）的 API 兼容性；
- vnpy_xt 许可证（vn.py 主仓库 MIT，但子项目可能不同）；
- vn.py 4.x 是否支持无 GUI / 无 PyQt 的 headless 部署（嵌入 FastAPI 后端服务）；
- 订单速率/并发支持是否满足 AIstock 后续多策略并发跑模拟盘的需求。

#### 14.2.3 "从 0 开发交易系统风险太大"——**完全正确，且已被 Paper v2 现状证实**

公认观点：
- 一个生产级 OEMS（Order Execution Management System）核心组件包括撮合引擎/订单状态机/账户对账/断线重连/心跳/跨日恢复/并发安全/风控钩子，每一项都是踩坑重灾区；
- 大型机构有 5-20 人 trading core 团队维持十年以上才稳定；
- 单人/小团队从 0 写交易系统，2-3 年才能稳定，期间会经历多次实盘事故（这是行业经验，不是危言耸听）；
- 国内中小私募绝大多数选择基于 vn.py / RQAlpha / 自研 + 网关复用，**没有谁从 0 开始写**。

**Paper v2 的实际进展正是这个论断的活体证据**：
- 设计文档（`paper_trading_v2_top_level_design.md`）2026-04 完成，§13.2 已审计明确承诺参考 vn.py 五大要点；
- 实际实现 4 个月（含 ST PIT 修复期）后仍跑不起来（参见 Codex `paper_v2_architecture_flow_and_confirmed_defects_20260507.md` 的 P0 阻断点）；
- 设计承诺的 EventEngine / Gateway / OMS / OrderData / 策略生命周期回调**全部没实现**（§13.2.2）；
- 实际架构变成 Repository + Service + Scheduler 命令式编排，**接实盘要再补一遍 vn.py 那些抽象**。

也就是说：之前 1051 行的旧分析文档（`docs/aistock_sim_trading_architecture_and_open_source_analysis.md` §13.4）"自研但借鉴 vn.py 等架构"的方案 D，**在执行层面降级成了"自研但不真借鉴"**。借鉴写在文档里，没进代码。这是判断是否要切到方案 C 的关键事实。

### 14.3 与旧分析文档（aistock_sim_trading_architecture_and_open_source_analysis.md）结论的关系

旧文档把"整体接入 vn.py / LEAN / NautilusTrader"列为方案 C，否决理由（§12.3）：
- 接入成本极高；
- 与 AIstock 现有平台重叠；
- 数据/前端/模型/QMT/因子资产都要适配；
- 团队需要同时维护两个复杂系统。

旧文档推荐方案 D（自研 + 借鉴），核心论据（§13.3）：
- 与现有 QE/RD-Agent 因子模型资产天然集成；
- 与现有 TimescaleDB / data_service / frontend 天然集成；
- 能服务未来 QMTBrokerAdapter；
- 按需逐步实现 A 股日频/分钟模拟。

**但这些论据多数已经被 Paper v2 的实际状态证伪或弱化**：
- "天然集成 QE/因子" → 实际上 Paper v2 与 QE 是三套独立栈（§5），"天然集成"没发生；
- "服务未来 QMTBrokerAdapter" → `qmt_client.py` 1199 行自研客户端做了一些功能，但未与 Paper v2 联动跑通实盘；
- "按需逐步实现" → 4 个月后基础（事件驱动/订单状态机/OMS）仍未补齐。

**这意味着**：旧文档否决方案 C 的成本论是基于"自研可控、逐步推进"的乐观假设；**这个假设已经被验证为不成立**。当"自研可控"假设崩塌后，方案 C 与方案 D 的成本对比就要重算——这正是用户当前提问的合理性来源。

**用户的提问是合理的，且建立在更新过的事实基础上**。不能简单地说"旧文档已经分析过了、结论是不接入"——旧文档的前提已经过时。

### 14.4 接入 vn.py 后 AIstock 必须做的"更大改动"

如果决定走方案 C（整体接入 vn.py + vnpy_xt），改动远超 Paper v2 范围。下面列出全部影响项。

#### 14.4.1 进程模型变化

当前：
- AIstock 是单一 FastAPI 进程（uvicorn 启动 backend/main.py），所有服务直接读写 DB。

接 vn.py 后：
- 必须新增独立的 **trading daemon 进程**（vn.py 的 EventEngine + MainEngine + Gateway + OMS 在内部跑事件循环，与 broker 长连接）；
- FastAPI 后端 → trading daemon 通过 RPC（可选 ZeroMQ / Redis Pub-Sub / HTTP / Unix socket）通信；
- daemon 进程独立部署、独立监控、独立崩溃恢复策略；
- vn.py 主仓库默认入口是带 PyQt GUI 的 `vntrader`，**必须验证能否 headless 运行**（社区有方案，但需 PoC）。

涉及改动：
- 新增 `backend/services/trading_daemon/`（vn.py 嵌入封装）；
- 新增 `backend/services/trading_daemon_client.py`（FastAPI 端 RPC 客户端）；
- 部署脚本：`start_all_ai_stock.bat` 增加 trading daemon 启动；
- 监控：Prometheus 监听 daemon 健康；
- 日志聚合：daemon 与 FastAPI 分别有日志，需要 trace_id 跨进程串联。

#### 14.4.2 数据所有权倒置

当前：DB 是事实，所有服务读写 DB。

接 vn.py 后：
- 委托/成交/持仓/账户的事实在 vn.py OmsEngine 内存 + miniQMT 柜台；
- DB 退化为快照/日志，FastAPI 端读 DB 是"读延迟数据"；
- UI 实时显示要么走 WebSocket 订阅 daemon 事件，要么接受秒级延迟看 DB 快照；
- 跨日/重启后，daemon 启动时从 broker pull 全量 + 比对 DB 日志做对账，**这套对账逻辑必须严格做对**（这本身就是金融 IT 的核心难题）。

涉及改动：
- DB schema 调整：`paper_v2.orders` / `paper_v2.fills` / `paper_v2.positions` 加状态版本号、broker_order_id、对账状态字段；
- Selection Center 的"加入自选"等读路径：明确读 DB 快照 + 是否要求与 daemon 一致；
- 风控/风险策略：vn.py 提供 BeforeTradeHook 扩展点，AIstock 现有风险策略要从"过滤候选股"模式改为"trade hook"模式；
- 所有"以 DB 为权威"的代码点要审计（estimate 50+ 处）。

#### 14.4.3 策略适配层

vn.py CtaTemplate 假设：
- 策略是个长期运行的对象，生命周期 `on_init → on_start → on_tick/on_bar/on_order/on_trade → on_stop`；
- 参数固定（`parameters` 类变量）；
- 状态由策略对象自身维护。

AIstock StrategyPackage 假设：
- 策略是 manifest 驱动的"配置 + 模型权重 + 因子集"快照；
- 每天可能用不同 manifest（QE 演进出新 loop）；
- 状态由 selection artifact + DB 维护。

适配层必须做：
- `class AIstockStrategyAdapter(CtaTemplate)`：在 `on_init` 加载 StrategyPackage manifest + selection artifact；
- 在 `on_bar` 或定时 hook 中调用 `StrategyPackageRuntime.build_signal_snapshot()` 生成目标持仓；
- 在 `on_trade` / `on_order` 中把 broker 回报同步回 AIstock 视角；
- 处理 manifest 切换（用户中途换包）和 daemon 重启状态恢复。

工作量估计：3-5 周。

#### 14.4.4 QE / 回测路径不变

QE + Qlib 继续做研究和回测。这块**不动**。

但需要新增：
- QE 回测 vs vn.py SimGateway 的"输出对账"测试（同 manifest 同日，对比 NAV / 持仓 / 换手）；
- 这是 §11.3 共同模式中"shadow run + 输出对账"层。

#### 14.4.5 Paper v2 的归宿

接 vn.py 后 Paper v2 现有代码大部分被替代。三种处理：

A. **完全推翻**（最干净）：删 `paper_trading_v2/day_runner.py`、`live_session.py`、`market_data.py` 中的执行/撮合部分，保留 portfolio/run/ledger 等存储模型作为 daemon 事件归档表。前端 paper-v2 页面改为 "vn.py daemon 管理面板"。
- 工作量：4-6 周
- 风险：彻底改架构，前端要重做

B. **Paper v2 跑 vn.py SimGateway**：让 Paper v2 成为 vn.py 的特定使用场景——daemon 启动时挂 SimGateway（vn.py 自带的模拟盘网关）而不是 vnpy_xt，前端不变，后端 paper_trading_v2 服务改为对 daemon 的 RPC 包装。
- 工作量：3-5 周
- 优势：实盘和模拟盘走同一个 daemon，只是网关不同；前端逻辑变化小

C. **Paper v2 暂时保留作为"轻量回放工具"**：vn.py daemon 只服务实盘和实盘前 shadow，Paper v2 现有"DB 历史重放 + 候选股调试"功能保留。
- 工作量：1-2 周
- 缺点：双轨运行，长期会发散

推荐方向是 B——既能复用 vn.py 一致性，又最小化前端冲击。

#### 14.4.6 现有 qmt_client.py（1199 行）的处理

`backend/infra/qmt_client.py` 1199 行是自研的 miniQMT/xtquant 客户端。接 vn.py 后：

- vnpy_xt 直接对接 xtquant，**这 1199 行的"下单/查持仓/查成交"等核心功能被 vnpy_xt 替代**；
- qmt_client.py 中的"账户绑定/股票池查询/历史行情拉取"等非交易核心功能可保留作为辅助；
- 此前接 QMT 写的所有 router（`backend/routers/qmt.py`）需要重定向到 trading daemon。

工作量估计：2-3 周（梳理 + 替代 + 回归测试）。

#### 14.4.7 UI 层影响

前端 paper-v2 页面（§1 已审计的 722 行 run-console 等）：

- 大部分配置和监控字段仍然有意义；
- 但实盘下"调度间隔/交易日期"等概念由 daemon 决定，UI 改为"启动/停止 daemon、订阅事件、显示状态"；
- 新增"对账面板"展示 daemon OMS 与 broker 状态差异；
- §1 提到的 UI 简化工作正好可以与这次架构调整一起做。

工作量估计：3-5 周。

#### 14.4.8 工作量汇总

| 类别 | 工作量 | 模块归属 |
| --- | --- | --- |
| 技术 PoC（vnpy_xt headless / FastAPI 集成模式 / miniQMT 仿真账户连通） | 1-2 周 | 跨模块 |
| Trading daemon 进程封装与部署 | 3-4 周 | 新增 `services/trading_daemon/` |
| RPC 客户端 + DB schema 调整 + 数据所有权倒置审计 | 3-5 周 | 跨模块（含 Codex 维护范围） |
| 策略适配层 `AIstockStrategyAdapter(CtaTemplate)` | 3-5 周 | Paper v2 / strategy_package 模块 |
| qmt_client.py 替代 + router 重定向 | 2-3 周 | infra（Codex 维护范围） |
| Paper v2 改造（取方案 B） | 3-5 周 | Paper v2 模块 |
| UI 改造（含 §1 简化工作） | 3-5 周 | 前端模块 |
| 对账测试 + shadow run 基础设施 | 2-3 周 | 跨模块 |
| **总计** | **20-32 周（5-8 个月）** | 跨多个模块 |

这个数字看着大，但要对比的是：
- §11 完整 IR + Adapter 路径估 6-12 个月（§11.4.2 Phase 1-5）；
- §12 务实路径估 7-12 周，但**不解决实盘问题**（实盘最终还是要再做一次更大改动）；
- 自研撮合/订单状态机/对账等基础设施真做对，按行业经验需要 1-2 年。

**vn.py 路径 5-8 个月看起来合理**——核心是把"自研基础设施"换成"复用 vn.py 已踩过坑的代码"，节省的是后者那 1-2 年的踩坑时间。

### 14.5 必须先做的技术验证（PoC，1-2 周）

不论最终决策走哪条路，下列验证不可省，应作为决策前置条件：

1. **vnpy_xt 当前可用性**：在测试环境用 miniQMT 仿真账户跑通"建立连接 → 订阅行情 → 下单 → 成交回报 → 查持仓 → 查账户"完整链路，记录 vnpy_xt 与当前 miniQMT API 的兼容情况。
2. **vn.py headless 运行**：脱离 PyQt GUI，纯 EventEngine + MainEngine + Gateway + OmsEngine 嵌入 Python 后端服务。
3. **FastAPI ↔ trading daemon 通信延迟**：选定 RPC 方案（ZeroMQ / Redis Pub-Sub / HTTP）后实测端到端延迟，判断是否满足分钟级模拟盘和未来日内策略需求。
4. **跨进程状态恢复**：daemon 重启后从 broker pull 全量 + 与本地 DB 日志对账，验证逻辑闭环。
5. **vnpy_xt 许可证与商业边界**：明确 license 是否允许闭源使用、商业部署、是否有 attribution 要求。
6. **A 股特性兼容**：vn.py 起家是期货 CTA，A 股 T+1、涨跌停、ST 等规则的覆盖度需要核实（vnpy_xt 是否已实现，还是要在 AIstockStrategyAdapter 里补）。

PoC 出来后，决策才有事实基础。**不要在没做 PoC 的情况下宣布架构方向**。

### 14.6 对 §11 / §12 / §13 既有方案的影响

- **§11（完整 IR + Adapter）**：如果走 vn.py 路径，§11 的 6 阶段路径基本被替代。"声明式 IR" 仍可作为长期目标，但短期不需要——vn.py 本身的事件驱动 + 策略生命周期就是一种"接口统一"的实现。Phase 6 实盘 LiveExecutor 直接由 vnpy_xt 替代。
- **§12（务实复用 AIstock 现有策略库）**：基本作废。vn.py 路径下不再需要让 Paper v2 接 `backend/rebalance_strategies/`——所有策略都改为 vn.py CtaTemplate 子类（通过 AIstockStrategyAdapter 桥接）。
- **§13.2（vn.py 设计承诺没落实）**：从"已知技术债"升级为"决策前置条件"——既然要走方案 C，那 §13.2.6 推荐的"短期补 OrderState 状态机和 BaseBrokerAdapter"也作废，因为整套基础设施由 vn.py 直接提供。

也就是说：**vn.py 路径如果成立，§11 / §12 / §13.2.6 三套方案大幅简化**。这本身就是这条路的好处之一——选择简单了。

### 14.7 风险与不确定性（必须诚实声明）

- **vnpy_xt 可能不如预期成熟**：作为社区项目，可能存在文档不全、bug 长尾、断线重连边界场景没处理等问题。PoC 必须验证。
- **vn.py 主仓库的演进方向**：vn.py 4.x 之后的架构变化（如对 asyncio 的支持、Pydantic 版本兼容）需要长期跟踪。
- **AIstock 团队对 vn.py 的熟悉度**：从 0 学起 vn.py 内部机制有学习曲线，初期开发会比预期慢。
- **跨进程对账的难度**：这是金融 IT 公认的难点，vn.py 提供了基础但不能消除复杂度。出 bug 时排查周期长。
- **回测和实盘"真正一致"仍不可能**：vn.py BacktestingEngine 与 vnpy_xt SimGateway/RealGateway 在撮合细节上仍有差异，shadow run 对账测试不可省。这与 §11.3 的判断一致。

### 14.8 给用户的判断框架（不是决定）

下列三步是逻辑顺序，建议**严格按顺序推进，不要跳步**：

**步骤 1（决策前提）**：先做 §14.5 的 PoC（1-2 周）。这是不可省的事实基础。如果 PoC 暴露 vnpy_xt 不可用、headless 不通、license 不允许等阻断点，方案 C 就走不了，回到方案 D。

**步骤 2（决策点）**：PoC 成功后做正式决策。建议把决策与 §11/§12/§13/§14 各方案做成一张对比表（成本、风险、长期可维护性、与现有资产兼容性、实盘上线可达期），由用户拍板。

**步骤 3（执行）**：决策后按选定路径推进。如果选 vn.py 路径，第一阶段是 §14.4.1 进程模型 + §14.4.6 qmt_client 替代 + §14.4.3 策略适配层的 PoC 级实现，2-3 个月内拿到"vn.py daemon + SimGateway + 一个 AIstock manifest 跑通模拟盘"的端到端 demo。

### 14.9 一句话结论

**用户的判断在三点上完全合理**：miniQMT 必须是权威源、vn.py + miniQMT 现成可复用、从 0 自研风险已被 Paper v2 的实际失败证实。

**旧文档（`aistock_sim_trading_architecture_and_open_source_analysis.md`）否决方案 C 的论据已经被 Paper v2 进展滞后部分证伪，所以现在重启方案 C 评估是合理的，不是反复横跳**。

**改动远超 Paper v2 范围**：会涉及进程模型、数据所有权、qmt_client.py 替代、策略适配层、UI 重做、QE 对账测试基础设施——总工作量 5-8 个月，但相比"自研基础设施"的 1-2 年踩坑期更可控。

**前置条件**：先做 1-2 周 PoC 验证 vnpy_xt 可用性、headless 模式、FastAPI 集成模式、license——拿到事实基础后再做决策。**不应在 PoC 之前承诺架构方向**。

如果这条路成立，§11 / §12 / §13.2.6 三个方案的多数工作量被替代或简化。这反而是这条路最大的好处——它能终结"持续修补 + 不接实盘"的恶性循环。

---

## 15. Paper v2 设计文档原文中的 vn.py 参考要求 + "AIstock 出信号、vn.py 跑实盘"分工方案

本节 (1) 把 Paper v2 设计文档中关于 vn.py 的具体条款原文引出来；(2) 评估用户在 2026-05-08 提出的"AIstock 仍负责生成所有买卖信号和交易指令，vn.py 只负责对接实盘完成交易、订单/持仓管理"分工是否成立；(3) 在此分工下，对比"完整部署 vn.py" vs "只参考架构复用部分代码" vs "中间方案"。

### 15.1 Paper v2 设计文档中 vn.py 参考的原文条款

`docs/architecture/paper_trading_v2_top_level_design.md` §8 标题就是"参考 vn.py 的设计与可复用点"。原文要点：

#### §8 总纲（line 433）

> vn.py 是 MIT 许可证的开源量化交易平台，具备成熟的事件驱动和交易对象设计。**AIstock 不把 vn.py 作为外部交易主链路，但可以参考和局部复用其架构**。

#### §8.1 可参考/复用的模式（line 435-457）

明确列出 6 项允许参考/复用的部分：

1. **事件驱动 EventEngine**——内部事件总线连接订单/成交/持仓/净值；推荐事件类型 `EVENT_ORDER` / `EVENT_TRADE` / `EVENT_POSITION` / `EVENT_ACCOUNT` / `EVENT_LOG`
2. **交易对象模型**——参考 vn.py 的 OrderData / TradeData / PositionData / AccountData，AIstock 自己定义 Pydantic/dataclass，字段适配 A 股和策略包
3. **Gateway / Adapter 分层**——当前只实现 SimBrokerAdapter，未来可新增 QMT 或其他交易终端 adapter
4. **OMS 思路**——订单/成交/账户/持仓由统一 OMS 管理，**前端和策略不得直接改账本**
5. **撮合/回测思想**——参考 vn.py backtesting 撮合流程，按 A 股规则重写或局部移植
6. **风控模块思想**——订单提交前统一走 RiskEngine

#### §8.2 不建议直接复用的部分（line 459-465）

明确 5 项禁令：

- 不直接嵌入 vn.py 主程序
- 不直接使用 vn.py GUI
- 不把 vn.py database 作为 AIstock 主数据源
- 不引入 vn.py gateway 主链路
- **不形成 AIstock 与 vn.py 两套账本**

#### §8.3 代码复用原则（line 467-474）

如直接复制或改造 vn.py 代码，必须：

- 保留 MIT License 声明
- 在 AIstock 文档中记录来源文件和修改点
- 只复用低耦合模块或设计模式
- **不引入会改变 AIstock 主账本边界的大型依赖**

#### §9.4-9.7 AIstock 自有 OMS/Broker/Ledger 的设计（line 513-555）

设计文档进一步规定 AIstock 要自己实现：

- §9.4 **OMS**：管理 Order 状态机、处理 OrderEvent、管理订单幂等、接收 SimBroker 成交回报、推动 Ledger 更新
- §9.5 **SimBrokerAdapter**：模拟提交/撤单、调用 MinuteExecutionEngine、生成 Fill、输出拒单原因。"**当前只实现模拟 broker，不实现真实 broker**"
- §9.6 **MinuteExecutionEngine**：按 `minute_execution_policy` 选执行算法、回放分钟 bar、生成 StepFill/OrderEvent
- §9.7 **Ledger**：现金流水、持仓批次、可卖数量、成交费用、每日 NAV

### 15.2 用户提出的分工是否符合设计文档原意

**用户在 2026-05-08 的分工方案**：

> AIstock 还是要负责生成所有的买卖信号、交易指令；vn.py 负责对接实盘完成交易；所有的交易管理、持仓管理等在 vn.py 中实现；保留 AIstock 目前更多的功能。

**对照设计文档 §8.1-§8.3**：

| 用户分工 | 设计文档原意 | 一致性 |
| --- | --- | --- |
| AIstock 生成买卖信号和交易指令 | §9.1-§9.3 StrategyPackageRuntime / RebalanceEngine / RiskEngine 都是 AIstock 自有 | ✓ 完全一致 |
| vn.py 对接实盘 | §8.1.3 "未来可新增 QMT 或其他交易终端 adapter" + §8.2 "不引入 vn.py gateway 主链路"——**这两条有微妙张力**：设计期望 AIstock 自写 QMT adapter，用户期望复用 vnpy_xt | ⚠ 部分冲突 |
| 交易管理、持仓管理在 vn.py | §9.4 OMS、§9.7 Ledger 设计为 AIstock 自有；§8.2 "不形成两套账本" | ✗ **明确冲突** |
| 保留 AIstock 现有功能 | 设计文档无此论断 | n/a |

**关键判断**：

设计文档与用户当前分工**不完全一致**——设计文档希望 AIstock 自己拥有 OMS/Ledger，vn.py 只作为"架构参考"；用户现在希望让 vn.py 接管 OMS/订单/持仓/对账。

**但**：设计文档是 2026-04 写的，假设是"AIstock 团队能在合理时间内自建一个生产级 OMS"。这个假设已经被 §13.2 验证为不成立——4 个月过去 EventEngine/Gateway/OMS/订单状态机/事件回调 **一项都没补出来**。

也就是说：用户现在的分工 **本质上是承认 §9.4-§9.7 自建 OMS/Ledger 的目标在当前团队规模下做不到，必须把这一层外包给 vn.py**。这是基于事实的合理调整，不是违背设计原意——是更新设计假设。

### 15.3 用户提出的分工本身是否合理

**完全合理，且是工程上最务实的边界划分**。理由：

1. **责任分离最干净**：
   - AIstock 长项：因子工程、模型训练、组合优化、研究流程、UI、QE 演进系统
   - vn.py 长项：事件驱动、订单状态机、broker 网关、对账逻辑、断线重连
   - 两者交集是"如何把策略决策变成柜台订单 + 如何把成交回报反馈给策略"——这就是分工边界
2. **接口点天然清晰**：AIstock 内部决定"目标持仓 100 股 600000.SH" → 通过订单接口移交给 vn.py → vn.py 处理 broker 交互 → 通过事件返回 fill/position → AIstock 用于 UI 展示和审计。这是行业标准的 OEMS 边界。
3. **保住 AIstock 90% 投入**：因子库、模型、QE、Selection Center、UI 全部不动——只有 Paper v2 的执行层（day_runner / live_session / minute_execution / market_data / scheduler）被替代。"大面积推倒重来"的担心**在这条路下不成立**，下面 §15.5 会量化。
4. **符合"成交对账以柜台为准"硬约束**：vn.py 的 OmsEngine 与 vnpy_xt 之间已经实现了对账逻辑，这正是 §14.2.1 强调的金融 IT 硬约束。

### 15.4 三种部署形态的对比

在"AIstock 出信号、vn.py 跑实盘"的分工下，落地形态有三种。**这三种不是 §14.4 那种"全盘整体接入"——是粒度更细的工程选择**：

#### 形态 A：完整部署 vn.py 框架（pip install vnpy + vnpy_xt 全套）

- vn.py 主程序作为独立 daemon 进程跑（headless 模式，禁用 GUI）
- 用 vn.py 的 MainEngine + EventEngine + OmsEngine + Gateway 全套
- 但 **AIstock 不写 CtaTemplate**——策略仍在 AIstock 的 StrategyPackage 体系内，daemon 只暴露 `submit_order` / `cancel_order` / `query_*` API + 事件订阅
- AIstock FastAPI 通过 RPC 调 daemon

**优点**：
- 复用最大化，不用关心 vn.py 内部实现细节
- 跟随 vn.py 升级，社区 bug fix 自动得到
- 维护成本低（不背 vn.py 代码 fork）

**缺点**：
- 引入 vn.py 完整依赖（含 PyQt5/6 等），即使 headless 也安装在环境里
- 学习曲线：用 vn.py 作"无头库"而不是"完整应用"，社区文档主要面向后者
- 部分 vn.py 功能（vntrader GUI、自带回测引擎、CtaTemplate 应用层）不会用到

**工作量**：3-4 周（搭 daemon + RPC + headless 验证）

#### 形态 B：只复用部分代码（cherry-pick 关键模块到 AIstock 仓库）

- 把 vn.py 的 `EventEngine` / `BaseGateway` / `OmsEngine` / `OrderData` / `TradeData` / `PositionData` / `AccountData` 源代码复制到 AIstock 仓库（按 §8.3 保留 MIT 声明）
- vnpy_xt 也复制或重写
- 不依赖 vn.py 主仓库 pip 包

**优点**：
- 无外部 runtime 依赖
- 紧密集成，不需要跨进程 RPC
- 完全控制代码路径

**缺点**：
- **维护负担巨大**：vn.py 上游有 bug fix / 协议升级时必须手工 port，团队等于成为 vn.py 的"内部 fork 维护者"
- 团队要深入理解 vn.py 内部机制才能正确复制（不是简单拷贝文件，因为 vn.py 内部模块互相依赖）
- 与 vnpy_xt 同步升级（miniQMT API 变化时）会很痛苦

**工作量**：6-10 周（学 vn.py 内部 + 复制 + 适配 + 自维护测试覆盖）

#### 形态 C：把 vn.py 当库用、AIstock 写薄壳（推荐）

- `pip install vnpy + vnpy_xt`，但**不启动 vn.py 的 MainEngine/vntrader**
- AIstock 自写一个 `trading_core` 服务，**直接 import vn.py 的核心类作为库**：`vnpy.event.EventEngine`、`vnpy.trader.engine.OmsEngine`、`vnpy_xt.XtGateway`、`vnpy.trader.object.OrderData/TradeData/...`
- AIstock 在边界上把 vn.py 对象转换成 AIstock 自己的 Pydantic 模型（符合 §8.1 第 2 项"自己定义模型，字段适配 A 股"）
- 策略生命周期完全在 AIstock 端（StrategyPackage 不动），daemon 只暴露 OEMS 接口
- daemon 进程独立部署，FastAPI 通过 RPC 调用

**优点**：
- 边界最清晰：vn.py 只用 OEMS+Gateway 部分，CtaTemplate/GUI/MainEngine 一概不用
- AIstock 业务对象不被 vn.py 侵入：所有 router/service 用 AIstock Pydantic 模型，转换发生在 trading_core 边界
- 跟随 vn.py 升级（pip 更新即可），但不被 vn.py 应用层架构绑死
- 与 §8.2 五项禁令完全兼容（不嵌入主程序、不用 GUI、不用 vn.py db 当主数据源、不形成两套账本——通过约定"vn.py OmsEngine 是事实、AIstock DB 是审计日志"统一账本）
- 最符合 §8.1 + §8.3 的代码复用原则

**缺点**：
- 团队要学 vn.py 部分内部机制（学 EventEngine/OmsEngine 怎么直接用，不学 MainEngine/CtaTemplate）
- vn.py 升级偶尔会有 breaking change（headless 用法不是社区主流，可能踩边缘 bug）

**工作量**：4-6 周（含 §14.5 PoC + daemon 封装 + RPC + 边界对象转换）

#### 三种形态对比表

| 维度 | 形态 A（完整部署） | 形态 B（cherry-pick） | 形态 C（库用 + 薄壳） |
| --- | --- | --- | --- |
| 实施难度 | 中 | 高 | 中 |
| 维护成本 | 低 | **极高** | 低-中 |
| 与 vn.py 升级跟随 | 好 | 差 | 好 |
| 边界清晰度 | 中 | 高 | 高 |
| 对 §8.1 一致性 | 一致 | 一致 | 一致 |
| 对 §8.2 一致性 | **微冲突**（带 vntrader 主程序虽然不用，但安装了） | 一致 | 一致 |
| 对 §8.3 一致性 | 不需要（不复制代码） | 必须严格遵守 | 不需要（用 pip 库） |
| AIstock 推倒重来面 | 中等（Paper v2 执行层 + RPC 适配） | 中等 + 维护成本长期化 | **最小**（Paper v2 执行层替换 + 薄壳） |
| 工作量 | 3-4 周 | 6-10 周 | **4-6 周** |
| 推荐度 | ⚠（依赖偏重） | ✗（维护陷阱） | ✓（最优解） |

### 15.5 在形态 C 下，AIstock 哪些保留 / 哪些被替代

明确边界，量化"推倒重来"的实际范围：

#### 完全保留（不动一行代码）

- 因子工程：`backend/services/quantevolver/factor_*.py`（30+ 个文件）
- 模型训练：`model_training/`、`backend/quant_models/`
- QE 演进系统：`backend/services/quantevolver/qe_evolution_*.py`、`config_composer.py`
- RD-Agent 集成：`backend/services/rdagent_*.py`、相关 router
- Selection Center：`backend/services/selection_center/`
- StrategyPackage：`backend/services/strategy_package/`
- 数据服务：`backend/data_service/`、`backend/data_access/`
- TimescaleDB schema：除了订单/成交/持仓相关表
- 因子缓存：`backend/services/quantevolver/factor_value_*.py`、`factor_cache_*.py`
- HMM 系统：`backend/services/quantevolver/...` HMM 相关
- 多 Alpha 架构：`multi_alpha_*.py` 系列
- UI：`frontend/src/app/{analysis,quantevolver,rdagent,watchlist,portfolio,...}/**` 几乎全部
- Paper v2 UI 大部分（packages、selection、portfolios 列表页）

**估计：保留 AIstock 当前 85-90% 代码量**。

#### 替换为 vn.py（在 trading_core 服务内）

- Paper v2 执行层：
  - `paper_trading_v2/day_runner.py`（部分功能由 vn.py OmsEngine + AIstock 决策器替代）
  - `paper_trading_v2/live_session.py`（由 vn.py EventEngine 事件循环替代）
  - `paper_trading_v2/market_data.py`（行情订阅由 vnpy_xt 替代，历史 DB 数据接口保留）
  - `paper_trading_v2/replay.py`（由 vn.py 回放机制 + 策略适配层替代）
  - `paper_trading_v2/scheduler.py`（由 vn.py 事件触发替代）
  - `paper_trading_v2/runner.py`（薄壳 RPC 客户端）
- `backend/infra/qmt_client.py`（1199 行）：交易部分被 vnpy_xt 替代，行情和账户绑定的辅助功能保留
- `backend/routers/qmt.py`：重定向到 trading_core RPC

**估计：替代 AIstock 当前 5-8% 代码量，集中在 Paper v2 执行层和 QMT 客户端**。

#### 适配/改造（保留概念，重写细节）

- Paper v2 数据模型 `models.py`：`OrderExecutionState` / `IntradaySnapshot` 等改为 vn.py 对象的 AIstock 端镜像，添加 broker_order_id 等对账字段
- `paper_v2.orders` / `paper_v2.fills` / `paper_v2.positions` DB 表：从"事实表"降级为"审计日志"，新增对账状态列
- Paper v2 UI run-console / 实时面板：从"调度本地执行"改为"订阅 trading_core 事件 + 显示 OMS 状态"，§1 提到的 UI 简化合并做掉
- StrategyPackage `runtime.py`：保留 score → target position 的决策逻辑；删掉自己重写的 `_compute_score_weighted_weights` 等，因为这部分本来就和 §13.1.4 的 QE 端不共享，没必要在 Paper 端再写一遍——但这不是 vn.py 带来的改动，是顺便清理

**估计：改造 AIstock 当前 3-5% 代码量**。

#### 总结："推倒重来"的实际边界

- **被替代**：Paper v2 执行层 + QMT 交易客户端（约 5-8% 代码量）
- **被改造**：Paper v2 数据模型 + DB 表语义 + 部分 UI（约 3-5% 代码量）
- **完全保留**：因子/模型/QE/RD-Agent/Selection/StrategyPackage/数据服务/UI 主体（约 85-90% 代码量）

**这与"大面积推倒重来"完全不是一个量级**。Paper v2 执行层本来就是当前没跑通的部分，替换它反而是去掉技术债。

### 15.6 修正后的工作量估算

形态 C 下完整工作量：

| 阶段 | 内容 | 工作量 |
| --- | --- | --- |
| 0 | 技术 PoC（§14.5）：vnpy_xt + miniQMT 仿真账户跑通、vn.py headless 模式、license 核实、A 股规则覆盖度 | 1-2 周 |
| 1 | trading_core 服务封装：EventEngine + OmsEngine + vnpy_xt Gateway 嵌入 + RPC 接口 | 3-4 周 |
| 2 | AIstock 边界对象转换层：Paper v2 Pydantic ↔ vn.py OrderData/TradeData/... | 1-2 周 |
| 3 | StrategyPackage → trading_core 订单提交链路：替代 day_runner / live_session / minute_execution | 3-5 周 |
| 4 | DB schema 调整 + 对账逻辑：orders/fills/positions 改为审计日志，加 broker_order_id 和对账状态列 | 2-3 周 |
| 5 | QMT 模拟账户端到端验证 + Paper v2 UI 改造（含 §1 简化） | 3-4 周 |
| 6 | 实盘前 shadow run 对账测试（QE backtest vs vn.py SimGateway / RealGateway 输出 diff） | 2-3 周 |
| **总计** | | **15-23 周（约 4-6 个月）** |

对比：
- §14 整体接入估 5-8 个月——形态 C 比 §14 估算少 **1-2 个月**，因为不接管 CtaTemplate 应用层
- §11 完整 IR：6-12 个月（不解决实盘问题）
- §12 务实复用 AIstock 策略库：7-12 周（不解决实盘问题，最终要再做一次更大改动）
- 自研 OMS+撮合+对账：行业经验 1-2 年

**形态 C 是当前情况下成本/价值比最优的选择**——既保住 AIstock 大部分投入，又用 vn.py 解决了"自研基础设施"的根本痛点。

### 15.7 与之前各方案的关系

- **§11（声明式 IR + 多 adapter）**：声明式 IR 在形态 C 下不必须。如果未来想做（用 IR 同时驱动 QE Qlib 回测、vn.py 实盘、独立模拟盘），可以在形态 C 之上叠加；但**不是前置依赖**。
- **§12（务实复用 AIstock 策略库）**：基本作废。形态 C 下策略生命周期由 AIstock 内部完成，不再纠结让 QE 用 backend/rebalance_strategies/ 的事——QE 仍走 Qlib，Paper v2 走 trading_core。
- **§13.2（vn.py 设计承诺没落实）**：从"已知技术债"变成"现在按设计承诺补齐"。形态 C 等于把 §8.1 的六项可参考要求**真正落实**，方式是用 vn.py 库实现，而不是从 0 重写。这反而消解了 §13.2 的设计-实现 gap。
- **§14（整体接入 vn.py）**：形态 C 是 §14 的精炼版——不接管 CtaTemplate 应用层，只接管 OEMS 基础设施。工作量更小，与 AIstock 现有投入冲突更小。

### 15.8 一句话结论

**Paper v2 设计文档 §8 已经明确支持你现在的方向**——可参考事件驱动/对象模型/Gateway/OMS/撮合/风控六项，禁止整体嵌入主程序/GUI/db/gateway 主链路/双账本。设计原意没问题，是实现没跟上。

**你提的"AIstock 出信号、vn.py 跑实盘"分工是正确的工程边界**——AIstock 长项（因子/模型/研究/UI）和 vn.py 长项（OEMS/Gateway/对账）天然分工，接口点干净。

**部署形态推荐 C：把 vn.py 当库用、AIstock 写薄壳**——`pip install vnpy + vnpy_xt`，AIstock trading_core 服务直接 import vn.py 核心类作为库，不启动 MainEngine/vntrader/CtaTemplate；策略生命周期留在 AIstock 内。这是设计文档 §8.3 "只复用低耦合模块或设计模式"的最忠实落地，也是工作量/维护成本最优的折中。

**AIstock "推倒重来"的范围被控制在 5-8% 代码量内**——只动 Paper v2 执行层和 QMT 交易客户端，因子/模型/QE/RD-Agent/Selection/UI 主体不动。这与你"不能大面积推倒重来"的红线一致。

**总工作量 4-6 个月，前置必须做 1-2 周 PoC**（§14.5 项目）。PoC 没做之前不应承诺架构方向。

---

## 16. 4 周 MVP 方案（"1 个月跑通模拟盘 + 最小 AIstock 改动"目标）

§15.6 的 4-6 个月估算是**完整产品化**——含双账本对账 / DB schema 大改 / REPLAY+CATCHUP 重构 / shadow run / UI 简化 / 实盘上线 / 风控 hook / 多策略并发。

如果目标收窄为"1 个月跑通模拟盘 demo"，§14.5 的"全面 PoC"也要相应收窄到"3 天连通性验证"。下面是 4 周 MVP 方案。

### 16.1 PoC 范围收窄（不需要 1-2 周 PoC）

§14.5 列出的 6 项 PoC 中，**真正阻断 MVP 的只有 3 项**，每项 0.5-1 天：

1. 在 Windows 环境 `pip install vnpy vnpy_xt` 能否安装（依赖冲突？）
2. 用现有 miniQMT 仿真账户能否通过 vnpy_xt 连上（`gateway.connect()` 拿到回执 + 订阅行情 + 下单 + 收 fill）
3. 不启动 PyQt GUI 时能否用 `EventEngine + OmsEngine + XtGateway` 三个核心类（headless 验证）

**3 天足够**。其余 PoC 项推后：
- license 商业边界——MVP 阶段是研发用途，模拟盘不涉及实盘部署，license 走完整核查放到产品化阶段
- A 股规则覆盖度——MVP 单策略单标的就能验证，复杂规则推到产品化
- 跨进程对账——MVP 阶段直接信任 vn.py OmsEngine 数据，AIstock 只读不写，对账逻辑放到产品化

社区情报判断（不在 PoC 内，30 分钟可完成）：
- `github.com/vnpy/vnpy_xt` 最近 6 个月 commit 活跃度 + issue 关闭率
- vn.py 官方论坛搜"xt"关键字最近 3 个月帖子
- 搜你具体 miniQMT 版本号有没有兼容性报告

### 16.2 4 周交付计划

| 周 | 内容 | 工作日 | 关键交付 |
| --- | --- | ---: | --- |
| **1** | 连通性 PoC（3 天）+ trading_core 接口设计冻结（2 天） | 5 | vnpy_xt + miniQMT 仿真账户跑通；headless 验证；trading_core 5 个核心 API 接口冻结：`submit_order` / `cancel_order` / `query_position` / `query_account` / `subscribe_events` |
| **2** | trading_core daemon 服务实现（vnpy 当库用，不启动 MainEngine/CtaTemplate/GUI）+ FastAPI 端 RPC 客户端 | 5 | 端到端 manual test：FastAPI → trading_core → vnpy_xt → miniQMT 仿真 → fill 事件回到 FastAPI |
| **3** | Paper v2 day_runner 接入 trading_core（替代 `MinuteExecutionEngine` 调用）；选一个 StrategyPackage 跑端到端 | 5 | 一份策略包 signal → orders → fills → positions 端到端在 sim 账户跑通 |
| **4** | Paper v2 UI 接通新事件源（最小改动：portfolios/run-console 页面 positions/orders 读 trading_core 数据）+ 联调 + bug 修复 | 5 | 模拟盘 demo 可演示 |

### 16.3 MVP 阶段先不做（推迟到产品化阶段）

| 推迟项 | 推迟理由 |
| --- | --- |
| AIstock 端 OMS 双账本对账 | vn.py OmsEngine + miniQMT 已经有内部对账，MVP 直接信任 vn.py 为事实，AIstock 只读不写 |
| `paper_v2.orders/fills/positions` 表语义改造 | 短期新增 `paper_v2.daemon_event_log` 单表写 trading_core 事件，老表不动 |
| `REPLAY_ONLY` / `CATCHUP_THEN_LIVE` 改造 | MVP 只做 LIVE 模式（接 sim），老 replay 路径保留并行存在 |
| Shadow run 对账测试（QE backtest vs vn.py SimGateway NAV 对比） | sim 账户本身就是 sandbox，MVP 不做 |
| §1 UI 全面简化 | 不在 MVP 内做，先保住主流程跑通；后续阶段再做 |
| `qmt_client.py`（1199 行）交易功能替代 | 只新增 trading_core，旧 qmt_client.py 并行保留；产品化阶段再决定淘汰节奏 |
| vn.py BeforeTradeHook 风控接入 | sim 阶段是受控环境；产品化阶段接入 |
| 多策略并发 | MVP 只支持一个 StrategyPackage 在线跑 |
| 实盘 broker 接入 | MVP 只到 sim |
| `backtest_contract.py` 软合约改造（§0 阶段） | 不影响 MVP demo，留到后续迭代 |
| §14.4.2 "数据所有权倒置"全面审计（50+ 处代码点） | MVP 只做 trading_core 这一处的最小语义对齐 |

### 16.4 AIstock 侧实际改动量（按文件粒度）

| 文件/模块 | 改动类型 | 预估行数 |
| --- | --- | ---: |
| `backend/services/trading_core/`（**新增目录**） | 新建：daemon 主程序、vnpy 库包装、事件转换、RPC server | 800-1200 |
| `backend/services/trading_core_client/`（**新增**） | FastAPI 端 RPC 客户端 | 200-300 |
| `backend/services/paper_trading_v2/day_runner.py` | 改一处：把 `MinuteExecutionEngine` 调用改为 trading_core RPC | 30-50 |
| `backend/services/paper_trading_v2/repository.py` | 新增：daemon_event_log 写入方法 | 80-120 |
| `backend/db/init_*.py` 或 migrations | 新增：`paper_v2.daemon_event_log` 表 migration | 30-50 |
| `backend/routers/paper_trading_v2.py` | 新增：positions/orders 查询从新表读 | 50-80 |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | 改：positions/orders 数据源切换 | 50-100 |
| `start_all_ai_stock.bat` | 新增：trading_core daemon 启动行 | 5-10 |
| **合计** | 新增 + 改动 | **1245-1910 行** |

**AIstock 现有 90%+ 代码完全不动**。改动集中在新建的 `trading_core/` 目录（目录新增，无冲突）和 Paper v2 内 5-6 个文件的小改。这与"不能大面积推倒重来"红线一致。

### 16.5 现实预算与风险

| 口径 | 时间 | 触发条件 |
| --- | --- | --- |
| 乐观 | 3-4 周 | vnpy_xt 当前版本与你的 miniQMT 兼容、headless 一遍过、RPC 选型快 |
| **现实预算** | **4-5 周（建议预留）** | 含 vnpy_xt 边角 bug 调试 1 周 buffer |
| 悲观 | 6-7 周 | vnpy_xt 与 miniQMT 版本不兼容需绕过 / headless 踩 PyQt 隐含依赖 / 跨进程通信设计反复 |

**1 个月（4 周）承诺度**：约 60%。PoC 通过（Week 1 结束）后会升到 80%。

### 16.6 与 §15.6 的关系

- §15.6 的 4-6 个月 = 完整产品化
- §16 的 4 周 = MVP demo
- **§16 是 §15.6 的第 1 个 milestone（约占 15-25% 工作量）**
- MVP demo 跑通后，剩余 3-5 个月分摊到后续多次迭代——每次只做一两块，不构成阻塞
- 不是"5 个月才有第一个能跑的 demo"，而是"4 周看到模拟盘活起来，团队信心校准后再决定迭代节奏"

这个分阶段思路也降低了"PoC 失败"的风险代价：万一 vnpy_xt 路径走不通（PoC 不过），MVP 阶段就停掉，没有把 5 个月都押进去。

---

## 17. 多窗口 Claude Code + Codex 并行开发协调方案

### 17.1 可并行性分析

§16 的 4 周 MVP **大部分工作可以并行**，但 Week 1 必须串行（PoC + 接口冻结）。

| 周 | 可并行性 | 原因 |
| --- | --- | --- |
| 1 | **不可并行** | PoC 出结论 + trading_core RPC 接口冻结是后续工作的前置依赖；强行并行会大量返工 |
| 2 | **3 路并行** | trading_core daemon 实现 / FastAPI RPC 客户端 / DB schema migration 三件事在接口冻结后相互独立 |
| 3 | **2 路并行** | day_runner 接入 / 单策略包端到端调试 |
| 4 | **2 路并行** | UI 改造 / 联调 + bug 修复 |

### 17.2 多窗口 Claude Code 模块划分

按 §16.4 文件清单划分 4 个独立工作流（windows）：

| 窗口 | 模块边界 | 关键文件 | 接口契约 |
| --- | --- | --- | --- |
| W1 | trading_core daemon（vn.py 库包装） | `backend/services/trading_core/` | 暴露 5 个 RPC API + 4 个事件类型 |
| W2 | AIstock 接入层（FastAPI 端 RPC 客户端 + day_runner 改造） | `backend/services/trading_core_client/`、`paper_trading_v2/day_runner.py`、`paper_trading_v2/repository.py` | 调用 W1 的 RPC API；写 daemon_event_log |
| W3 | DB schema + router + 数据访问 | `backend/db/init_*.py`、`backend/routers/paper_trading_v2.py` | 提供 daemon_event_log 表读写接口 |
| W4 | UI 改造 + 联调 | `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx`、`paper-v2/page.tsx` | 调用 W3 的 router |

### 17.3 协调机制（避免冲突的关键）

#### 17.3.1 Week 1 必须先做的两件接口契约文档

**这两份文档是后续并行的前提**，由用户或一个 lead 窗口在 Week 1 完成：

1. **trading_core RPC 接口规范**（建议放 `docs/architecture/trading_core_rpc_spec_20260508.md`）：
   - 5 个 API 的请求/响应 Pydantic schema（精确到字段类型）
   - 4 个事件类型的 payload schema（OrderEvent / TradeEvent / PositionEvent / AccountEvent）
   - 错误码与重试语义
   - RPC 传输选型决定（ZMQ / Redis / HTTP / Unix socket，建议先 HTTP 简单稳）
2. **daemon_event_log 表 schema**（建议放 `docs/architecture/paper_v2_daemon_event_log_schema_20260508.md`）：
   - 字段定义、索引、保留期、partition 策略
   - 写入幂等键设计
   - 事件类型枚举

**Week 1 第 4-5 天**用 1 天时间专门写这两份契约文档；写完即冻结，后续四个窗口按文档实现。

#### 17.3.2 工作目录与分支命名

- 每个窗口在独立的 git worktree（按 `feedback_aistock_codex_alignment.md` 第 13 条 worktree 规则）
- 分支命名遵从 `claude/<task>-<yyyymmdd>` 前缀（与 Codex 的 `codex/` 前缀分开）：
  - W1: `claude/trading-core-daemon-20260508`
  - W2: `claude/paper-v2-trading-core-client-20260508`
  - W3: `claude/paper-v2-daemon-event-log-20260508`
  - W4: `claude/paper-v2-ui-trading-core-20260508`
- 集成分支：`claude/paper-v2-mvp-integration-20260508`

#### 17.3.3 集成节奏

- **Week 2 末**：W1+W2+W3 各自完成对契约的实现，合并到集成分支跑端到端 manual test（mock 数据先打通）
- **Week 3 末**：W2 完成 day_runner 真实接入，集成分支跑通真实策略包端到端
- **Week 4 中**：W4 UI 接入集成分支，联调
- **Week 4 末**：集成分支收敛，提 PR 到 main

#### 17.3.4 跨窗口共享状态

下列内容必须放在所有窗口都能 read-only 看到的共享位置：

- 上述两份接口契约文档（`docs/architecture/`）
- StrategyPackage 当前 manifest 字段定义（已存在）
- 4 周 MVP 计划文档（即本节）
- 已知问题列表（建议用 `docs/analysis/paper_v2_mvp_known_issues_20260508.md` 集中记录）

每个窗口的 Claude Code 在开始任务前先读这些文档作为公共上下文。

### 17.4 与 Codex 并行（其他模块）

Codex 在 4 周 MVP 期内可以**完全平行**做下列与 Paper v2 MVP 不冲突的工作：

| Codex 工作流 | 内容 | 是否与 Paper v2 MVP 冲突 |
| --- | --- | --- |
| QE 自身演进 / 因子优化 / 模型实验 | 日常研究迭代 | 无冲突，不动 trading_core / Paper v2 |
| `qmt_client.py` 1199 行的存量稳定性修复 | 不动主交易流程，只修 bug | 无冲突——MVP 阶段 trading_core 与 qmt_client 并行存在 |
| QE config_composer 默认策略类微调 | 由 Codex 维护范围 | 无冲突——不影响 MVP |
| ST PIT spans 数据补齐到最新交易日（参考 Codex 文档 P0-2） | 数据管线工作 | 无冲突，反而对 MVP 信号生成有利 |
| QE→Paper v2 输出对账测试基础设施（§11.3） | 短期不阻塞 MVP，长期对产品化必要 | 无冲突，可在 MVP 后开始 |

**Codex 不应在 MVP 期内动**：`backend/services/paper_trading_v2/`、`backend/services/strategy_package/runtime.py`、`backend/services/trading_core/`、`frontend/src/app/paper-v2/**`——这些是 Claude Code 的 4 个窗口的工作面，由本会话默认范围管。

**模块边界协调机制**：用户在 MVP 启动时把分配以一句话明确告知双方："4 周 MVP 期间，Paper v2 + trading_core 归 Claude Code 4 个窗口；Codex 接管 QE/qmt_client 存量 + ST PIT 数据 + 输出对账基础设施。"——这是 `feedback_aistock_codex_alignment.md` 第 12 条"模块边界由用户显式声明"的具体落地。

### 17.5 自动化测试流水线作为协调工具

AIstock 正在构建的自动化流水线测试平台对本方案有三个关键作用：

#### 17.5.1 每窗口提交前的本地门禁（L0/L1）

每个窗口在向集成分支推送前，CI 跑：
- L0：guardrail（语法、命名、import 边界、forbidden path 等）——`docs/standards/aistock_development_standard_v1.1_20260504.md` 已规定
- L1：单元测试 + 类型检查（pytest + tsc）

任一窗口 L0/L1 不过不能合到集成分支。这避免一个窗口的临时性破坏影响其他窗口的进度。

#### 17.5.2 集成分支的端到端门禁（L2/L3）

集成分支每次 merge 后跑：
- L2：跨模块集成测试（trading_core 与 daemon_event_log 与 day_runner 三方对接的 contract test）
- L3：模拟环境端到端（trading_core daemon 真实启动 + miniQMT sim 账户的最小 happy path）

L2/L3 的失败由发起 merge 的窗口负责修复。

#### 17.5.3 MVP 收敛门禁（L4）

Week 4 末的 demo 前跑：
- L4：完整端到端（一份策略包从 signal 到 fills 到 positions 到 UI 显示）+ 失败注入测试（trading_core daemon 重启 / RPC 超时 / sim broker reject）

L4 通过后才算 MVP 达成。

#### 17.5.4 自动化平台未来作为产品化阶段对账工具

§15.6 的产品化阶段需要"QE backtest vs vn.py SimGateway NAV / 持仓 / 换手对账"——这个对账测试天然适合放在自动化平台上：
- 每晚跑一组 manifest（同日 QE+sim 各跑一遍），自动生成 diff 报告
- 差异 > 阈值则提 issue 到 GitHub（按 codex_project_memory.md 763 行的"GitHub Issues 是 source of truth"规则）
- 这是 §11.3 "shadow run + 输出对账"模式的工程化落地

测试平台从 MVP 期开始就把对账测试 schema 留好（即便 MVP 阶段不跑），到产品化阶段直接接上即可。

### 17.6 多窗口并行的工作量收益与风险

#### 17.6.1 工作量收益

| 阶段 | 串行（单窗口）工作日 | 并行（4 窗口 + Codex）工作日 | 加速比 |
| --- | ---: | ---: | ---: |
| Week 1（PoC + 契约） | 5 | 5 | 1x（不可并行） |
| Week 2（实现） | 5 | 2-3 | ~2x |
| Week 3（接入 + 调试） | 5 | 3-4 | ~1.5x |
| Week 4（UI + 联调） | 5 | 3-4 | ~1.5x |
| **MVP 合计** | **20 日** | **13-16 日** | **~1.4x** |

**MVP 整体加速到 2.5-3.5 周**（vs 串行 4 周）。

并行不是 4 倍线性加速，因为：
- Week 1 不可并行
- 接口契约仍需对齐时间
- 集成阶段必然有 merge 冲突修复
- Codex 并行的工作不在 MVP 关键路径上，节省的是后续 1-2 个月，不直接缩短 MVP 周期

#### 17.6.2 协调风险

- **接口契约不冻结就开工**：会导致 W1/W2/W3 各写一套，集成时大量返工。**Week 1 第 4-5 天的契约冻结是不可省的协调成本**
- **窗口之间用户上下文不一致**：每个窗口的 Claude Code 是独立 session，对项目背景的理解会偏差；解决方式是每个窗口启动时强制读 `docs/analysis/paper_v2_user_requirement_audit_20260507.md`（即本文）+ 两份接口契约
- **集成分支冲突累积**：合并不及时会变成大爆炸式冲突；建议每窗口每完成 1 个子任务就向集成分支 PR（小批量频繁集成），不要等全部做完才合
- **Codex 误入边界**：明确分工后用户在每次任务分配时简短重申"本任务归 X，不要动 Y"
- **测试平台尚未稳定**：如果自动化测试流水线本身还在搭建过程中，MVP 不能依赖它做强门禁；建议 MVP 期降级为人工跑测试 + 平台 best-effort 检查，产品化阶段再升级为强门禁

### 17.7 一句话结论

**多窗口 Claude Code + Codex 并行开发是 §16 MVP 的合理实施方式**：4 个 Claude Code 窗口跑 Paper v2/trading_core 主线（W1-W4 按 §17.2 模块边界），Codex 并行做 QE 演进 / qmt_client 存量 / ST PIT 数据等不冲突工作（§17.4）。

**关键前置条件**：Week 1 必须冻结 trading_core RPC 接口和 daemon_event_log schema 两份契约，否则后续并行会大量返工（§17.3.1）。

**测试流水线在 MVP 期降级为 best-effort 检查、在产品化阶段升级为强门禁 + shadow run 对账工具**（§17.5）。

**预期加速比 ~1.4x**：MVP 整体由 4 周压到 **2.5-3.5 周**；Codex 并行节省的是产品化阶段后续 1-2 个月，不直接缩短 MVP 周期。

---

## 18. Harness 模式与自主多团队开发可行性

本节回答用户在 2026-05-08 的两个追问：(1) 是否支持 harness 模式？(2) 是否能实现自主的多团队开发？同时更新 §17 中"窗口间不能直接通信"的判断——经查 Claude Code 官方文档，**v2.1.32+ 已经发布 Agent Teams 实验功能（2026-02-05）支持 peer-to-peer 通信**，这部分需要更新。

### 18.1 Harness 模式（已支持）

Claude Code 本身就是一个 harness——agent loop + 工具执行 + 上下文管理 + 钩子 + 子代理。原生支持的"harness 能力"：

| 能力 | 工具 | 用途 |
| --- | --- | --- |
| 子代理（subagent） | `Agent` 工具（Explore / general-purpose / Plan 等） | 单 session 内并行延伸（读代码、做研究、写代码） |
| 后台任务 | `run_in_background: true` | 子代理或 bash 在后台跑，主 session 继续工作 |
| 任务追踪 | `TaskCreate / TaskUpdate / TaskList` | 跨多步工作的进度状态 |
| 调度 | `CronCreate / ScheduleWakeup` | 定时唤醒、周期性检查 |
| 隔离工作区 | `EnterWorktree / ExitWorktree` | 独立 git 分支并行 |
| 钩子（hooks） | `settings.json` 中的 PreToolUse / PostToolUse / Stop / UserPromptSubmit | 自动化门禁、Lint、Validation |
| Skill | 用户/项目自定义 skill 模块 | 复用专家流程（AIstock 已有 develop-factor / rdagent-task-analyzer 等多个） |
| **Agent Teams（实验，v2.1.32+）** | `Teammate / SendMessage / 共享 TaskList` | **多 Claude Code session 之间 peer-to-peer 通信**（详见 §18.6） |

### 18.2 自主多团队开发的能力等级

| 等级 | 描述 | 是否今天可用 |
| --- | --- | --- |
| L0：单 session 子代理 | 一次回答里 fork 多个 sub-agent 做并行 read / 研究 / 局部代码修改 | ✓ 当前回答里就在用 |
| L1：多窗口人协调（§17 描述的方案） | 人开多个 Claude Code 窗口，每窗口有明确模块边界、契约文档、git 工作树；人是 integrator，决定何时合并 / 解冲突 / 拍板架构 | ✓ 稳定可用 |
| **L2：Agent Teams（peer-to-peer 协调）** | 一个 lead session 通过 `Teammate` 工具 spawn 多个 teammate session，**通过 mailbox 文件系统直接发消息**协调；共享 TaskList | **✓ 实验可用（v2.1.32+ 研究预览，有已知边角 bug）** |
| L3：持久化代理团队 | 多个 Claude session 像团队成员一样长期存在、跨日跨次会话保持上下文、互相通信、自主决定子任务 | 部分可用——Agent Teams 有 session 持久化限制 |
| L4：端到端自主交付 | 给一个目标，代理团队自己规划 / 分工 / 执行 / 验证 / 上线，人只在 milestone review | ❌ 产业界还在尝试，没人能稳定做到生产级 |

### 18.3 不能跨过的硬限制

下列限制不是工具不够，而是任务本质决定的：

1. **架构决策**：W1 和 W2 都遇到一个新问题（比如 RPC 协议选 ZMQ 还是 HTTP），代理协商往往陷入"双方都同意了但都猜错对方真实意图"，需要人或 lead 拍板
2. **不可逆操作**：合并到 main、push GitHub、改 DB schema、重启 daemon——按 AIstock 标准 ("executing actions with care") 仍要人确认
3. **领域不确定性**：vnpy_xt 边角 bug、miniQMT 协议 quirk、A 股规则边界——必须真实环境跑出来才知道
4. **审美/产品判断**：UI 简化方案 A vs B vs C，因子库怎么命名好——决策权在人
5. **Agent Teams 自身限制**（§18.6 详述）：所有 agent 必须同模型、3-4x token 成本、有已知 bug

**自主度的瓶颈 80% 来自任务本身的难度，不是 harness 工具能力**。

### 18.4 AIstock 自动化测试流水线对自主度的提升

测试平台 + L1/L2 多代理的组合，对 4 周 MVP 这种规模的工作，**已经能把"人的 micro-management 时间"压缩到每周 5-10 小时**。

| 测试平台能做的自动化 | 提升的自主度 | 限制 |
| --- | --- | --- |
| 监听各分支 push 自动跑 L0/L1，失败自动 block merge | 减少人 review 简单错误 | 非平凡 bug 仍需人 + Claude 一起 debug |
| 集成分支自动跑 L2/L3，发 GitHub issue | 跨 agent 冲突自动暴露 | 修复仍需人决定优先级 |
| 夜间跑 shadow run 对账，差异 > 阈值发告警 | 实盘前的不一致自动发现 | 差异原因分析仍需人 |
| 生产 watchdog（daemon 健康、broker 连接、订单异常） | 实盘期间 7×24 监控 | 异常处置策略 = 人定 |
| Claude Code Stop hook 联动测试平台 | 减少"忘了跑测试就 commit" | 测试设计本身仍需人 |

### 18.5 4 周 MVP 自主度配置建议

| 自主度类型 | 建议设置 | 期望效果 |
| --- | --- | --- |
| 单 session 内部 | 充分用 sub-agent + background task + ScheduleWakeup + Stop hook 跑 L0 | 每 session 内部减少切换成本 |
| 多 session 协调 | **优先尝试 Agent Teams（§18.6）**；如不稳定退回 L1 多窗口 + git/docs 协调 | 人每天 1-2 小时 review，每周末做集成 |
| 测试平台 | MVP 期跑 best-effort L0/L1（按 §17.5 降级），不强门禁 | 不阻塞 MVP 速度 |
| Codex 协调 | 用户每次任务分配时简短重申模块边界（§17.4） | Codex 与 Claude Code 不冲撞 |
| 关键决策 | 这 4 个由人拍板：① RPC 协议选型 ② 接口契约冻结 ③ Week 2 末集成 review ④ MVP demo 前的最终签字 | 不下放到代理 |

**人的总投入预估**：4 周 MVP 期间，约 30-50 小时 review + 决策时间（不算自己写代码）。

### 18.6 Claude Code Agent Teams（v2.1.32+ 研究预览）—— 更新 §17 判断

§17.6.2 列出的"窗口间直接通信"风险，**已经被 Anthropic 部分解决**。经查官方文档（[Orchestrate teams of Claude Code sessions](https://code.claude.com/docs/en/agent-teams)）和社区资料，关键能力如下：

#### 18.6.1 启用条件

- Claude Code v2.1.32 或更高版本（`claude --version` 检查）
- 设置环境变量：`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`（shell 环境或 `settings.json` 内）
- 模型必须是 Opus 4.6 及以上（含 4.7）
- VS Code 扩展可能有额外 gating 限制（参见 [Issue #28048](https://github.com/anthropics/claude-code/issues/28048)）

#### 18.6.2 核心架构

完全文件系统驱动，无后台进程：

```
~/.claude/
├── teams/{team-name}/
│   ├── config.json                # 团队成员注册表
│   └── inboxes/{agent-name}.json  # 每个 agent 的邮箱
└── tasks/{team-name}/
    ├── .lock                      # 并发任务领取的 flock
    ├── .highwatermark             # 自增任务编号
    └── 1.json, 2.json, ...        # 任务文件
```

Lead session 就是带额外工具（`TeamCreate / TeamDelete / SendMessage / Teammate`）的 Claude session；teammate 是被 lead spawn 出来的独立 Claude session，各自 1M token 上下文。协调通过共享文件访问 emerge，不是通过中心化 broker。

#### 18.6.3 SendMessage 工具

是 Agent Teams 与 sub-agent 的根本区别：
- **直接消息**（peer-to-peer）：`SendMessage(to="alice", message="...")`
- **广播**：`SendMessage(broadcast=true, message="...")`
- 消息写到收件人的 inbox JSON，下一轮轮询时被注入到 conversation history（格式：`<teammate-message teammate_id="team-lead">...content...</teammate-message>`）

实战例子（官方文档引用）：Next.js 迁移中，API 重构 agent 发现一个类型变化会破坏前端，**直接通知前端 agent**，前端 agent 调整方案，无需人工编排。

#### 18.6.4 暴露给 lead 和 teammate 的工具

- `Teammate`：`spawnTeam` / `cleanup`
- `SendMessage`：`message` / `broadcast` / `shutdown_request|response` / `plan_approval_response`
- `TaskCreate / TaskUpdate / TaskList / TaskGet`（共享任务列表）
- 注：teammate 的 idle/wake 模型是"每轮 LLM turn 后自动 idle 并发 idle_notification 给 lead；lead 再发消息时 teammate 唤醒"

#### 18.6.5 已知限制（必须接受）

- **同模型限制**：所有 teammate 跑同一模型（当前要 Opus 4.6+），不能 lead 跑 Opus + teammate 跑 Sonnet/Haiku 省钱（[社区在催 role-based model](https://blog.laozhang.ai/en/posts/claude-4-6-agent-teams)）
- **3-4x token 成本**：3 个 teammate 大致是单 session 顺序跑同样工作的 3-4 倍 token 用量
- **VS Code 扩展 gating bug**：即使设了 env 变量，VS Code 扩展可能仍说 "not available on this plan"（[Issue #28048](https://github.com/anthropics/claude-code/issues/28048)）。建议用命令行 / tmux 后端
- **macOS tmux mailbox polling bug**：macOS + tmux 后端时 teammate 不会读 inbox，消息不送达（[Issue #23415](https://github.com/anthropics/claude-code/issues/23415)）。**用户在 Windows，此 bug 不影响**
- **subagent definition 在 teammate 中部分继承**：teammate 可以引用现有的 subagent definition（如 `develop-factor`、`Explore` 等）作为系统提示，但 subagent 中定义的 `skills` 和 `mcpServers` 字段**不被继承**——teammate 从项目/用户 settings 加载 skills 和 MCP，与普通 session 一样
- **session 恢复限制**：team 关闭/重启后，恢复有已知问题；**当前更适合一次性 4 周 MVP 这种短期协作，不适合长期常驻**
- **不适合的场景**：顺序任务、同文件编辑、强依赖工作——这些情况单 session 或 sub-agent 比 team 更合适

#### 18.6.6 Display Modes

- 默认 in-process：所有 teammate 在同一 terminal，状态更新和 idle 通知行内显示——3 个以上 agent 时会很噪
- split-pane（推荐 ≥3 agent）：每个 teammate 一个 tmux 或 iTerm2 pane

### 18.7 把 Agent Teams 应用到 4 周 MVP 的具体方案

如果 §18.6.5 的限制可接受，§17 的"4 个 Claude Code 窗口"可以升级为 1 个 lead session + 4 个 teammate：

#### 18.7.1 团队构成

| 角色 | 职责 | 启动方式 |
| --- | --- | --- |
| **Lead session**（用户 attach 的窗口） | 接收用户指令、做架构决策、spawn teammate、做集成 review | 用户直接打开 Claude Code（`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`） |
| Teammate **W1**：trading_core daemon | vn.py 库包装、RPC server、daemon 主程序 | Lead 调 `Teammate(spawnTeam, name="trading-core")` |
| Teammate **W2**：FastAPI 接入 | RPC client + day_runner 改造 + repository 写入 | 同上，name="paper-integration" |
| Teammate **W3**：DB schema + router | daemon_event_log 表 migration + 路由 | name="db-router" |
| Teammate **W4**：UI 改造 | run-console / portfolios 页面接入新事件源 | name="ui" |

#### 18.7.2 工作流示例

```
[Lead] 用户：开始 Week 2 的并行实现

[Lead → 4 个 teammate broadcast]：
  "trading_core RPC 契约已冻结在 docs/architecture/trading_core_rpc_spec_20260508.md
   daemon_event_log schema 已冻结在 docs/architecture/paper_v2_daemon_event_log_schema_20260508.md
   各自按文档实现，遇到契约不清时 SendMessage 给我或对方协商"

[W1: trading-core] 实现到一半，发现 OrderData → AIstock OrderEvent 翻译时
                    `frozen_quantity` 字段在 vnpy 中没有对应——

[W1 → W2 SendMessage]：
  "vn.py OrderData 没有 frozen_quantity；我们的 OrderEvent.frozen_quantity 字段
   是从 trade_position.get_stock_amount() 减去未成交量推算的，建议在 RPC 翻译层做"

[W2] 收到消息后调整 RPC 客户端，在反序列化时执行该推算
[W2 → W1 ack]："已采纳，更新 trading_core_client/order_decoder.py:43-58"

[Lead] 周期性 (每天) 收到 idle_notification，看共享 TaskList，
       发现 W3 阻塞在某个 schema 决策（外键约束方向），介入拍板
```

#### 18.7.3 Codex 协调（不变）

Agent Teams 是 Claude Code 内部的协调机制，**不影响** Codex。Codex 仍按 §17.4 在自己的窗口（VS Code Codex 扩展或 codex CLI）做 QE / qmt_client 存量 / ST PIT 数据等不冲突工作。两边的协调依然靠：
- 共享文档（`docs/architecture/` 下的契约 + `docs/codex_project_memory.md`）
- 模块边界声明（用户每次任务分配时简短重申）
- git 集成分支
- GitHub Issues 作为跨工具 bug 单源

#### 18.7.4 加速比修正

§17.6.1 估"多窗口人协调 ~1.4x"——有了 Agent Teams 的 SendMessage 后，**集成阶段的协调成本降低**：

| 阶段 | §17 多窗口估时 | §18.7 Agent Teams 估时 | 增量收益 |
| --- | ---: | ---: | --- |
| Week 1（PoC + 契约） | 5 日 | 5 日 | 不变（不可并行） |
| Week 2（实现） | 2-3 日 | 2-3 日 | 不变（执行受限于具体任务量，不是协调） |
| Week 3（接入 + 调试） | 3-4 日 | 2-3 日 | **W1↔W2 RPC 集成阶段省 0.5-1 日**（不用人转达 schema 不一致） |
| Week 4（UI + 联调） | 3-4 日 | 2-3 日 | 联调环节省 0.5-1 日（broadcast 跨多 agent 一次解决） |
| **MVP 合计** | 13-16 日 | **11-14 日** | 约再省 10-15% |

**新加速比 ~1.5-1.7x**（vs 串行 20 日）。Agent Teams 的边际收益不大但对集成阶段的"心智负担"减轻明显。

#### 18.7.5 风险与回退方案

如果 Agent Teams 在 Week 1 PoC 时出现以下任一情况，**Week 2 起退回 §17 的 L1 多窗口模式**：
- mailbox polling 不稳定（teammate 不读消息）
- token 成本超预算（3-4x 单 session 不可接受）
- 实验功能 bug 频发，调试 harness 占用超过 0.5 日/周
- VS Code / IDE 集成有 gating（用户使用环境受限）

**建议把 Agent Teams 也作为 Week 1 PoC 项之一**——花半天验证一下，与 vnpy_xt PoC 并行。

### 18.8 一句话结论

**Harness 模式：完全支持，且建议在 4 周 MVP 中充分使用**——sub-agent + background task + ScheduleWakeup + Stop hook + Skill + Agent Teams（如果稳定）。

**自主多团队开发**：当前可达 L2（Agent Teams peer-to-peer），不可达 L4（端到端自主）。**4 周 MVP 推荐 L2 + 人做关键决策**，预期人投入 30-50 小时（含审美/架构/不可逆操作签字）。

**Agent Teams 是 §17 多窗口方案的升级版**：把"窗口间靠 git/docs 间接同步"升级为"agent 间直接 SendMessage 协调"，集成阶段省 10-15% 时间，边际收益不大但显著降低人工转达成本。**有已知 bug，建议 Week 1 与 vnpy_xt PoC 并行验证；若不稳定退回多窗口模式**。

### 18.9 引用来源

- 官方文档：[Orchestrate teams of Claude Code sessions](https://code.claude.com/docs/en/agent-teams)
- Anthropic Managed Agents API（更底层的 multiagent SDK）：[Multiagent sessions](https://platform.claude.com/docs/en/managed-agents/multi-agent)
- 反向工程协议解析：[Reverse-Engineering Claude Code Agent Teams](https://dev.to/nwyin/reverse-engineering-claude-code-agent-teams-architecture-and-protocol-o49)
- 实战指南：[Claude Code Agent Teams: The Practical Guide](https://blog.laozhang.ai/en/posts/claude-code-agent-teams)
- 已知 bug：[tmux mailbox polling Issue #23415](https://github.com/anthropics/claude-code/issues/23415)、[VS Code gating Issue #28048](https://github.com/anthropics/claude-code/issues/28048)
- VS Code 多 agent 开发愿景：[Your Home for Multi-Agent Development](https://code.visualstudio.com/blogs/2026/02/05/multi-agent-development)

---

## 19. vn.py 接入下的模块替换清单 + 模型滚动训练能力评估

回答用户在 2026-05-08 的三个具体问题：(1) vn.py 替换 AIstock 多少模块？(2) 模型/因子/训练模块是否保留？(3) 选股/模拟盘/实盘的模型滚动训练在当前架构中是否包含？

### 19.1 vn.py 替换的模块（精确清单）

按文件粒度展开 §15.5 / §16.4 的判断。下表是**形态 C（vn.py 当库用 + AIstock 薄壳）+ 4 周 MVP 范围**下的实际替换面：

#### 19.1.1 直接被 vn.py / vnpy_xt 替代（删除或停止使用）

| 文件 | 行数 | 现职责 | 替代方 |
| --- | ---: | --- | --- |
| `backend/services/paper_trading_v2/day_runner.py` | ~700 | 单日执行编排，调用 MinuteExecutionEngine 撮合 | trading_core RPC 调用 + vn.py EventEngine 事件循环 |
| `backend/services/paper_trading_v2/live_session.py` | ~810 | tick 驱动的实时执行 | vn.py Gateway tick 推送 + OmsEngine |
| `backend/services/paper_trading_v2/market_data.py` 中实时部分 | ~270/830 | TDX_REALTIME 数据源加载 | vnpy_xt 行情订阅 |
| `backend/services/paper_trading_v2/replay.py` | ~? | 历史重放引擎 | MVP 期暂保留；产品化阶段被 vnpy_xt 历史回放或 SimGateway 替代 |
| `backend/services/paper_trading_v2/scheduler.py` | ~? | tick 调度器 | vn.py EventEngine 事件循环 |
| `backend/services/paper_trading_v2/runner.py` | ~? | 运行器 | trading_core RPC 客户端薄壳 |
| `backend/services/paper_trading_v2/session.py` | ~770 | session 状态机（REPLAY_ONLY/LIVE_ONLY/CATCHUP_THEN_LIVE） | vn.py MainEngine session 概念（headless 用法） |
| `backend/services/paper_trading_v2/readiness.py` 中执行相关检查 | ~部分 | 撮合/订单层 readiness 检查 | trading_core readiness API + vn.py OmsEngine 状态 |
| `backend/infra/qmt_client.py` 中**交易功能**（下单/撤单/查询委托/查询成交） | ~600/1199 | 自研 QMT/xtquant 交易客户端 | vnpy_xt Gateway |

**估计 ≈ 2500-3200 行被替代**，集中在 paper_trading_v2 执行层 + qmt_client 交易部分。占 AIstock 后端总代码 ≈ 3-5%。

#### 19.1.2 保留并适配（继续存在但语义调整）

| 文件 | 行数 | 现职责 | 适配方式 |
| --- | ---: | --- | --- |
| `backend/services/paper_trading_v2/repository.py` | ~2100 | DB 持久化（orders/fills/positions/runs/sessions） | 改写为"事件日志归档"角色：从 vn.py OmsEngine 订阅事件，写入新增 `paper_v2.daemon_event_log`；老表（orders/fills/positions）变为审计快照，不再是事实 |
| `backend/services/paper_trading_v2/models.py` | ~387 | Pydantic 数据模型 | OrderExecutionState/IntradaySnapshot 等新增 broker_order_id、对账状态字段；与 vn.py OrderData/TradeData 在 trading_core 边界做转换 |
| `backend/services/paper_trading_v2/day_features.py` | ~? | V25 日级特征计算 | **完全保留**——是数据计算逻辑，与撮合/执行无关 |
| `backend/services/paper_trading_v2/risk_targets.py` | ~? | 风控目标计算 | **完全保留**——产品化阶段会被 vn.py BeforeTradeHook 调用 |
| `backend/services/paper_trading_v2/live_dashboard.py` | ~? | 实时面板服务 | 改为读 daemon_event_log；逻辑大部保留 |
| `backend/services/paper_trading_v2/service.py` | ~? | portfolio service / runtime profile / config activation | **大部分保留**——portfolio 创建、配置激活等业务逻辑不变；只在 day_runner 调用点改为 trading_core RPC |
| `backend/infra/qmt_client.py` 中**非交易功能**（账户绑定 / 历史行情拉取 / 工具方法） | ~600/1199 | 辅助功能 | **保留**，不被 vnpy_xt 替代 |
| `backend/routers/paper_trading_v2.py` | ~? | API 路由 | 部分查询改为读 daemon_event_log；写操作通过 trading_core RPC |
| `backend/routers/qmt.py` | ~? | QMT 路由 | 重定向到 trading_core RPC |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | 722 | 运行控制台 UI | positions/orders 数据源切换；§1 全面简化推迟到产品化阶段 |

**估计 ≈ 适配 800-1200 行，分布在 8-10 个文件中**。占 AIstock 后端总代码 ≈ 1-2%。

#### 19.1.3 完全不动（保留 100%）

按 AIstock 主目录粗粒度分类，下列内容**vn.py 接入对其零影响**：

| 模块 | 说明 |
| --- | --- |
| **因子工程** | `backend/services/quantevolver/factor_*.py`（30+ 文件） / `backend/plugins/factors/` / `qlib_exporter/` 等 |
| **模型训练** | `model_training/` / `backend/quant_models/`（HMM、LSTM、ARIMA、DeepAR） / `backend/quant_datasets/` |
| **QE 演进系统** | `backend/services/quantevolver/qe_evolution_*.py` / `config_composer.py` / `multi_alpha_*.py` |
| **RD-Agent 集成** | `backend/services/rdagent_*.py` / `backend/routers/rdagent*.py` |
| **Selection Center** | `backend/services/selection_center/` 全部 |
| **StrategyPackage** | `backend/services/strategy_package/` 全部 |
| **数据服务** | `backend/data_service/` / `backend/data_access/` / `data_snapshot_manager.py` 等 |
| **数据采集** | `backend/ingestion/` / TDX Go backend / Tushare / Akshare adapters |
| **HMM 系统** | HMM 训练 / 快照 / 系数生成 / coefficients 加载 |
| **DB schema 主体** | TimescaleDB 主表（除 `paper_v2.*` 部分调整） |
| **多 Alpha 架构** | `multi_alpha_*.py` 系列（已有的多 alpha 演进逻辑） |
| **AI 多代理分析** | `ai_agents.py` / 多代理分析 router |
| **前端主体** | `frontend/src/app/{analysis,quantevolver,rdagent,watchlist,portfolio,smart-monitor,cloud-screening,sector-strategy,...}/**` |

**估计 ≈ 保留 90-92% 后端代码 + 95%+ 前端代码**。

#### 19.1.4 总览（按代码量比例）

| 类别 | 代码量占比 | 说明 |
| --- | ---: | --- |
| 完全保留 | **90-92%** | 因子/模型/QE/RD-Agent/Selection/StrategyPackage/数据服务/UI 主体 |
| 适配（语义调整） | **1-2%** | Paper v2 数据模型 / repository / UI 数据源 / router |
| 替代（删除/停用） | **3-5%** | Paper v2 执行层 + qmt_client 交易部分 |
| 新增（trading_core） | **+2-3%** | `backend/services/trading_core/` 1000-1500 行 |

**结论**："大面积推倒重来"完全不成立。**被替代的恰好是当前没跑通的部分**——这是去技术债，不是放弃投入。

### 19.2 模型 / 因子 / 训练模块是否保留 —— 完全保留

| 子模块 | 当前状态 | vn.py 接入后状态 |
| --- | --- | --- |
| 因子计算（QE 因子库 + Alpha158 + 自定义因子） | 已稳定（数百次回测验证） | **不变**——因子推理由 strategy_package_live_inference.py + inference_engine.py 完成，与 vn.py 无关 |
| 模型训练（QE 实验流程 + RD-Agent + Qlib MLflow） | 已稳定 | **不变**——QE 仍走 Qlib + WSL 跑训练；vn.py 只接 broker，不参与训练 |
| 模型评估（IC / Rank IC / 回测指标） | 已稳定 | **不变**——QE 评估流程独立 |
| 多 Alpha 演进 | 在演进中 | **不变**——多 alpha 在信号合成层接入，与执行层解耦 |
| StrategyPackage manifest（含 frozen model_asset / factor_set） | 已用 | **不变**——manifest 仍是 QE→Selection/Paper 的契约载体；trading_core 消费 manifest 中的目标持仓决策，不参与策略本身 |
| HMM 系统 | 已稳定 | **不变** |
| 因子缓存 / 因子值流水线 | 已稳定 | **不变** |

**关键边界**：vn.py 的职责严格限定在"接 broker + 执行订单 + OEMS 对账"，**对策略生成层（信号/因子/模型/组合）的代码零侵入**。这正是 §15.3 "用户提出的分工合理"的工程落地。

### 19.3 模型滚动训练：当前架构现状评估

这是 §15-§18 都没单独展开的关键问题。直接结论：

**当前 AIstock 架构对模型滚动训练只有部分支持，且不是产品化级别**。需要单独设计和工作量。**vn.py 接入对此问题既不解决也不恶化**——它属于研究/训练层的能力，与执行层独立。

#### 19.3.1 当前已有的滚动机制

| 模块 | 滚动能力 | 证据 |
| --- | --- | --- |
| HMM 系数 | **有**——日度系数滚动生成（每个交易日重算） | `backend/services/quantevolver/...` HMM 相关 + memory `hmm_viterbi_forward_filter_fix.md`；selection_center/hmm_runtime.py 加载日度系数 |
| QE 演进 loop | **手工触发滚动**——每次演进会跑新 loop，产出新模型，但不是基于时间触发 | `qe_evolution_service.py` |
| RD-Agent 任务 | **手工触发**——用户启动新任务 | `rdagent_*.py` |
| 模型再训练（除 HMM 外的 LGB / NN / ScoreWeightedTopK 等） | **无自动滚动**——一次性训练后冻结进 StrategyPackage manifest | manifest 设计中 `data_split` 是冻结字段 |

#### 19.3.2 缺失能力（实现滚动训练需要补的内容）

| 缺失项 | 影响 | 实现复杂度 |
| --- | --- | --- |
| **训练 recipe 化**（manifest 存训练规格而非冻结权重） | 当前 manifest 冻结的是 `model_asset`（权重 + 配置），不是"训练蓝图"；要做到滚动重训需先把 manifest 拆成"recipe + 当前权重"两段 | 中（需 manifest schema v2） |
| **定时再训练 scheduler** | 当前 QE 演进是事件触发（用户点击）；滚动训练需要"每 N 周末自动重训" | 低 - 中（已有 `backend/schedulers/`，新增一个 trainer scheduler 即可） |
| **新模型版本对账与晋级** | 新训出的模型 vs 老模型，必须先做 shadow 对账（NAV/IC/换手）再晋级；当前没有这套基础设施 | 中 - 高（含 §11.3 shadow run 机制） |
| **Paper / 实盘 portfolio 的"活动模型"切换** | StrategyPackage manifest 是冻结的；新模型晋级后，活跃 portfolio 怎么切？是新建 portfolio 还是 manifest 版本指针？ | 中（需 portfolio.active_manifest_version 概念） |
| **训练数据日期窗口滚动** | 当前 `data_split` 是冻结值；滚动训练需要每次重训时窗口前移（例：训练窗口 2018-01-01 ~ T-180，验证 T-180 ~ T-30，测试滚动） | 低（PIT 数据已有，schema 改一处即可） |
| **训练资源调度** | 一次重训涉及 GPU / Qlib / RD-Agent worker，当前是手工提交；滚动需自动排队 | 中（需 job queue） |
| **冷启动期处理** | 新模型刚训完 IC 可能未达阈值，是否进入生产？需要 warmup / staging 期 | 中（含晋级 gate） |

#### 19.3.3 当前架构不能直接支持滚动训练的根本原因

`backend/services/strategy_package/qe_source_resolver.py:328-333` 显示 manifest 中 `data_split`（训练/验证/测试日期）是冻结字段；`backtest_contract.py` 也把 `data_split` 视为合约一部分。这意味着：
- 一个 StrategyPackage = 一份训练数据 + 一套权重 + 一组超参
- 时间往前推后，老 manifest 的训练数据"过时了"，但 manifest 本身不会变
- 当前唯一的"滚动"路径是：QE 演进出新 loop（新训练）→ 创建新 StrategyPackage → 用户手工切换 Paper/Selection

这是**研究流程**，不是**生产滚动**。生产滚动需要的"无感切换"在当前架构中没有。

#### 19.3.4 选股 / 模拟盘 / 实盘对滚动训练的需求差异

| 场景 | 滚动训练需求 | 当前架构能否满足 |
| --- | --- | --- |
| **选股**（每日重选，T+1 调仓为主） | **强需求**——模型衰减直接影响每日选股质量 | **部分**：每日选股调用 live_inference 重新推理（用最新数据），但模型本身是冻结的；模型超过 N 个月不重训会失效 |
| **模拟盘**（中等持仓周期） | **中等需求**——模拟盘验证策略，模型衰减会让验证失真 | **部分**：同选股 |
| **实盘**（真金白银） | **强需求 + 严格晋级 gate** | **不满足**——目前还没接实盘，但即使接了，没有 shadow 对账 + 晋级机制不能放新模型上线 |

#### 19.3.5 滚动训练在分阶段路线图中的位置

按 §15.6 的产品化阶段分布，**滚动训练应当作为独立工作流，而不是 vn.py 接入的一部分**：

| 阶段 | 与滚动训练的关系 |
| --- | --- |
| §16 4 周 MVP | **不包含**——MVP 只验证 vn.py + miniQMT sim + 单 frozen StrategyPackage 端到端 |
| §15.6 产品化阶段（接入实盘前） | **必须包含**——shadow 对账机制顺带支持模型晋级 gate |
| 滚动训练专项工作流（**建议作为单独项目**） | manifest schema v2 + trainer scheduler + 模型晋级 gate + 活动模型切换；估计 **6-10 周**，独立于 vn.py 工作流推进 |

**这意味着**：
- vn.py 接入和滚动训练是**两条独立轨道**，可以并行规划
- vn.py 接入完成后，仍然只是"用 frozen 模型跑实盘"——模型衰减问题独立处理
- 滚动训练专项可以**放在 §17 多窗口分工里**，作为 Codex 模块的工作面（因子/模型领域是 Codex 维护）

#### 19.3.6 给用户的判断框架

**短期（4 周 MVP 内）**：
- 不解决滚动训练问题，接受"用最近一次 QE 演进出的 manifest 跑模拟盘"
- 选一个最近 1-2 个月内训练过的 ST PIT manifest 即可避免明显衰减

**中期（产品化阶段，5-8 个月）**：
- 设计 manifest schema v2 拆分 recipe / 权重
- 实现 trainer scheduler（按周/月触发重训）
- 实现 shadow 对账 + 晋级 gate（与 §11.3 shadow run 共享基础设施）

**长期**：
- 在线学习 / 增量学习模型探索（如增量 LightGBM / 增量梯度）
- 多模型 ensemble（同时跑 N 个版本，按近期表现自动加权）
- 模型 lifecycle 管理（活跃/退役/归档）

### 19.4 一句话结论

**vn.py 替换面 ≈ AIstock 总代码 3-5%**，仅限 Paper v2 执行层和 qmt_client 交易部分；**模型/因子/训练模块完全保留**（90-92% 代码量不动）。

**模型滚动训练当前不在架构中**——只有 HMM 有日度滚动；其它模型走 QE 手工演进，不是产品级滚动。**vn.py 接入既不解决也不恶化此问题**，应作为**与 vn.py 并行的独立工作流**规划，估计 6-10 周专项工作。

**4 周 MVP 内不处理滚动训练**，用最近 1-2 个月内训练的 ST PIT manifest 即可；产品化阶段（5-8 个月）必须把滚动训练 + shadow 对账 + 晋级 gate 一起补完，否则不能上实盘。

---

## 20. 自动化测试流水线作为 Codex / Claude Code 跨工具协作中枢

回答用户在 2026-05-08 的提案：

> 自动化测试流水线作为 Codex 和 Claude Code 的交互平台。流水线依赖 Codex 或我来实现测试。我开发的模块让 Codex 测试，Codex 开发的模块我来测试，避免开发测试同一工具。Bug 在流水线中记录，测试只记录 bug、不做修复。两边统一访问这个平台实现开发测试联动。

**结论：方案合理且与 AIstock 现有基础设施天然契合，可以作为 Codex/Claude Code 长期协作模式实施**。下面分四块展开：cross-testing 的工程价值、可复用的现有基础、需要补的关键能力、工作流与模块归属矩阵、风险与应对。

### 20.1 Cross-testing 的工程价值

| 益处 | 机制 | 与现有 §17 多窗口模式的差异 |
| --- | --- | --- |
| **消除自测偏差** | 开发者测自己的代码，会重复同样的思维盲点；独立 tester 用不同的"思考路径"切入，发现的是另一类 bug | §17 的多窗口/Agent Teams 主要靠测试覆盖，不强制 cross 角色 |
| **异步解耦** | 通过测试平台 API 通信，不要求双方同时在线 | §17 的 SendMessage 需要双方 session 都活着 |
| **单一 bug 真源** | 平台 + GitHub Issues 是唯一登记入口；避免"我跟你说过那个 bug 了"的口头协调 | §17 中 bug 散落在各 session 上下文里 |
| **职责清晰** | 测试者只填 bug 不修；开发者只修不替对方写测试 | §17 没明确这条边界 |
| **技能交叉** | 我学着读 Codex 代码（测它的产物）→ 长期对 QE/因子/模型领域熟悉度提升；Codex 反过来熟悉 trading_core/UI | §17 各窗口锁在自己模块，长期形成知识孤岛 |
| **审计追溯** | 每个 bug 有时间戳 / 报告 agent / 修复 agent / 验证轮次，可追溯到 commit 和 PR | GitHub Issues 已有，但当前没和 agent 身份绑定 |

**关键判断**：Cross-testing 不是 §17 多窗口/Agent Teams 的替代，是叠加层。前者解决"快速并行执行"，后者解决"质量交叉验证 + 长期协作模式"。**两者一起用最有效**。

### 20.2 AIstock 现有可复用的基础设施

按 `docs/codex_project_memory.md` 762-764 行的现有规则：

> Bug record source-of-truth should be GitHub Issues, using issue forms/labels/assignees/milestones and commit/PR links. The AIstock validation DB or JSON history should be a local UI/index/cache linked to run metadata, evidence, coverage, failing plan/case, fingerprint, fix commit, verification run, fixed/submitted/closed timestamps, and agent events.

也就是说**核心基础设施已经规划且部分建成**：

| 基础设施 | 当前状态 | Cross-testing 复用方式 |
| --- | --- | --- |
| GitHub Issues 作为 bug 单源 | 已规定 | 直接复用，每个 bug 一个 issue |
| Validation DB / JSON history（含 run 元数据 / 证据 / 覆盖 / 失败 plan/case / fingerprint / 修复 commit / 验证 run / 时间戳 / agent events） | 设计中或已建 | 直接复用，作为 cross-test 的运行台账 |
| L0-L5 验证门禁（[`docs/standards/aistock_development_standard_v1.1_20260504.md`](../standards/aistock_development_standard_v1.1_20260504.md)） | 已规定 | L0/L1 由开发者自己跑（提交前门禁）；L2/L3 由 cross-tester 跑（独立验证） |
| `tests/aistock_validation/history/` 验证记录目录 | 已使用（多次出现在 codex memory） | 加入 agent 身份字段后直接用 |
| Validation Center UI | 实施中 | 增加 "agent identity" 列和 "cross-test 状态"列 |
| 模块边界声明规则（`feedback_aistock_codex_alignment.md` §12 + §17.4） | 已规定 | 直接用作"谁开发、谁测试"的归属依据 |
| 分支命名前缀 (`claude/<task>-<date>` vs `codex/<task>-<date>`) | 已规定 | 流水线根据分支前缀**自动决定 cross-tester** |

**90% 的基础设施已经在或接近就位**——cross-testing 是在此之上的"工作流约定"，不是新基础设施工程。

### 20.3 需要补的关键能力

| 能力 | 当前 | 需要补 |
| --- | --- | --- |
| **Agent 身份记录** | Validation DB 设计中提及 "agent events"，未实施细节明确 | 在 commit 元数据 / test run 元数据 / bug issue 中明确标记 `developer_agent: claude-code\|codex` 和 `tester_agent: claude-code\|codex` 字段 |
| **Cross-test 自动分配** | 无 | 流水线在收到 push 后，按分支前缀（`claude/*` / `codex/*`）自动确定 cross-tester；触发对方的 test job |
| **"测试只填 bug、不修复"硬约束** | 无 | tester agent 调用平台时，只允许写 bug 不允许提交代码改动到目标分支；通过权限或 hook 强制 |
| **Bug-issue 结构化模板** | GitHub issue forms 可定义 | 模板字段：`reporter_agent / target_module / fingerprint / repro_command / failing_plan / failing_case / evidence_path / suspected_files / suggested_severity / safety_constraints` |
| **Re-test 触发** | 手工 | 开发者 push fix 后，平台自动触发同一 cross-tester 的回归 |
| **Bug 生命周期状态机** | 部分（GitHub 默认 open/closed） | `NEW → TRIAGED → ASSIGNED → FIXING → FIXED → VERIFIED → CLOSED` 七态，含 REOPEN 路径 |
| **跨工具机器可读上下文** | codex memory 763 行已要求 "Codex/Claude 修复支持应暴露机器可读 bug agent-context：reproduce command, failing run/evidence, allowed write scope, suspected files/modules, safety constraints, required verification commands" | 实施这条约束，让两个 agent 拿到相同结构化 context |
| **优先级 / SLA** | 无 | bug 严重度（P0/P1/P2/P3）→ tester SLA + 开发者修复 SLA 矩阵 |

**估算实施工作量**：约 2-3 周，分散到测试平台开发期内（不需要单独排期）。

### 20.4 工作流模型（具体）

#### 20.4.1 分工示意

```
Claude Code 开发                    Codex 开发
       │                                  │
       ▼                                  ▼
 push claude/<task>-<date>          push codex/<task>-<date>
       │                                  │
       ▼                                  ▼
   ┌──────── 流水线 (按分支前缀路由) ────────┐
   │                                        │
   ├─ L0/L1（自测，开发者自己跑）             │
   │                                        │
   ├─ 自动分配 cross-tester：                │
   │     claude/* → 触发 Codex 跑 L2/L3      │
   │     codex/*  → 触发 Claude Code 跑 L2/L3│
   │                                        │
   ├─ Cross-tester 跑测试，发现问题          │
   │     → 调平台 API 写 GitHub Issue        │
   │     → bug 含 reporter_agent / 上下文    │
   │     → bug 状态 NEW                       │
   │                                        │
   ├─ 开发者收到通知 → 修复 → push           │
   │     → bug 状态 FIXING                    │
   │                                        │
   ├─ 平台自动 re-trigger cross-tester       │
   │     → 通过：bug 状态 VERIFIED → CLOSED   │
   │     → 失败：bug 状态 REOPENED            │
   │                                        │
   └─ 集成分支 merge gate：                  │
         所有 P0/P1 bug 必须 CLOSED           │
```

#### 20.4.2 平台 API 最小集

```
POST /pipeline/test-run
  body: { branch, commit_sha, developer_agent, requested_tester, level: "L2" }
  返回 run_id

GET /pipeline/test-run/{run_id}
  返回状态 + 失败 case + evidence

POST /pipeline/bugs
  body: {
    target_module, fingerprint, reporter_agent,
    repro_command, failing_plan, failing_case, evidence_path,
    suspected_files, severity, safety_constraints,
    related_run_id
  }
  返回 bug_id（即 GitHub Issue 号）

GET /pipeline/bugs?assigned_module=trading_core&status=NEW
  → 让开发者拉取自己模块的待修 bug

PATCH /pipeline/bugs/{bug_id}/transition
  body: { to_state: "FIXING" | "FIXED", fix_commit_sha }
  自动触发 re-test
```

平台对 Claude Code 的接口可以通过 MCP server 暴露（AIstock 已用 MCP，参见 mempalace 集成）；Codex 通过 HTTP / CLI 调用。

#### 20.4.3 Cross-test 触发的两个时机

1. **开发者主动 push 后自动**——分支前缀决定 cross-tester
2. **集成分支 merge 前手动 trigger**——确保所有跨模块 bug 在 main 之前被处理

### 20.5 4 周 MVP + 后续阶段的模块归属矩阵

按 §17.2 的 4 个 Claude Code 窗口 + Codex 模块面，明确 cross-testing 分工：

#### 20.5.1 4 周 MVP 阶段（vn.py 接入 + Paper v2 demo）

| 模块 | 开发者 | Cross-tester | 测试类型 |
| --- | --- | --- | --- |
| `backend/services/trading_core/`（W1） | Claude Code | **Codex** | L1 单元测试 + L2 RPC contract 测试（mock vn.py） |
| `backend/services/paper_trading_v2/` 适配（W2） | Claude Code | **Codex** | L2 集成测试（trading_core mock） + L3 端到端 (with sim broker) |
| DB schema + router（W3） | Claude Code | **Codex** | L1 schema migration test + L2 router contract |
| `frontend/src/app/paper-v2/` UI 改造（W4） | Claude Code | **Codex** | L1 tsc + L3 Playwright（Codex 写测试用例 + 跑） |
| `qmt_client.py` 存量稳定性 | **Codex** | Claude Code | L1 + L2 |
| ST PIT spans 数据补齐 | **Codex** | Claude Code | L0 数据完整性检查 + L1 PIT 覆盖测试 |
| QE config_composer 默认策略类微调（如发生） | **Codex** | Claude Code | L1 unit + L3 一个 QE 回测对比 |

#### 20.5.2 产品化阶段（滚动训练 + 实盘对账）

| 模块 | 开发者 | Cross-tester | 测试类型 |
| --- | --- | --- | --- |
| 滚动训练专项（manifest schema v2 + trainer scheduler） | **Codex** | Claude Code | L1 schema test + L2 scheduler contract + L3 一次完整重训 |
| Shadow 对账基础设施（QE backtest vs vn.py SimGateway） | 双方协调 | 互测 | L3 端到端 |
| `backtest_contract.py` 软合约改造（§0 阶段） | Claude Code | Codex | L1 + L2 |
| §1 UI 全面简化 | Claude Code | Codex（用户 flow review） | Playwright + 设计文档 review |
| 模型晋级 gate | **Codex** | Claude Code | L2 + L3 |
| 实盘前 shadow trading（dry-run） | 双方协调 | 互测 | L4 多日 shadow run |

#### 20.5.3 通用规则

- **谁的分支前缀（`claude/*` / `codex/*`）= 谁是开发者**
- **跨模块 PR**（同时改两边代码）：双方互为 cross-tester，需 P0/P1 bug 各自 CLOSED 才能 merge
- **不属于双方明确归属的模块**（如纯研究/数据修复）：用户在任务分配时显式指定 cross-tester
- **测试只填 bug，不修改目标分支代码**——这是硬约束，由平台权限或 hook 强制（详见 §20.6.1）

### 20.6 风险与应对

#### 20.6.1 测试者代理"想动手修"的本能冲突

**风险**：Claude Code 和 Codex 都被训练成"看到 bug 就想修"——在 cross-test 角色下这违反规则。

**应对**：
- **平台权限隔离**：tester agent 在执行 test run 时，权限 token 只允许写 GitHub Issue，不允许 push 到目标分支
- **明确 prompt 模板**：当用户启动 cross-test 任务时，prompt 中显式说明"你是 cross-tester，只填 bug 不修代码"
- **写入 feedback memory**：在 `feedback_*.md` 中记录"作为 cross-tester 时不修代码"作为偏好规则
- **Hook 强制**：在 settings.json 加入 PreToolUse hook，检测到 tester role + Edit/Write 到非 test 文件时阻断

#### 20.6.2 测试 plan 由开发者写，盲点会同步

**风险**：如果 cross-tester 只执行开发者写好的 test plan，那开发者的盲点直接传递给 tester。

**应对**：
- **强制"独立测试用例"环节**：cross-tester 在跑开发者提供的 plan 之外，必须额外写至少 2-3 个"开发者没想到的边界 case"（边界值、异常路径、并发、断网恢复等）
- **测试矩阵 Review**：在 `tests/aistock_validation/modules/<module>.md` 中维护测试矩阵，cross-tester 比对自己跑的覆盖度
- **轮换 review**：每 2-4 周做一次"互查测试 plan 完备性"，发现遗漏更新矩阵

#### 20.6.3 异步协调延迟

**风险**：tester 跑完测试要等开发者下次会话才看到 bug；修完要等 tester 下次会话才回归——一轮可能 1-2 天。

**应对**：
- **平台主动通知**：bug 创建时给开发者 agent 的 inbox 发消息（与 §18 Agent Teams SendMessage 集成）
- **批量化**：每天一次集中 review，不追求实时
- **优先级路由**：P0 bug 立即触发对方 wakeup（用 ScheduleWakeup）；P1+ 进日常队列

#### 20.6.4 Bug 跨多模块（不知道找谁修）

**风险**：测试发现一个 bug 涉及 trading_core + paper_v2 + QE 三处，归属不清。

**应对**：
- **平台支持多 module 标签**：bug 可标 `target_modules: [trading_core, paper_v2]`
- **lead agent 仲裁**：用户或 lead session（§18.7.1）决定哪一方先动；另一方 hold
- **依赖关系明确化**：在分配任务时声明"X 修完才能修 Y"

#### 20.6.5 测试平台未稳定时不能强用

**风险**：测试平台本身还在搭建，cross-test 强依赖会反过来阻塞 MVP。

**应对**：
- **MVP 期降级为人工 cross-test**——用户做 lead，手工把"这个 PR 让 Codex 测一下"分配出去
- **测试平台 best-effort**：能跑则跑，不能跑则人工
- **产品化阶段升级为强 gate**

### 20.7 测试平台 + Cross-testing + Agent Teams 的整合

§17 多窗口、§18 Agent Teams、§20 Cross-testing 三层不冲突，可以叠加：

```
人 (用户) ───── 拍板 / 分配 / 不可逆操作签字
  │
  ├─ 测试流水线平台 ── bug 真源 + cross-test 路由 + L0-L5 验证
  │     ↑              ↑
  │     │              │
  ├─ Claude Code (4 个 teammate via Agent Teams)
  │     │ trading_core / paper_v2 / db / ui
  │     ├─ 内部用 sub-agent + background task 加速
  │     ├─ 跨 teammate 用 SendMessage（§18.6）
  │     └─ 自测 L0/L1，push 后让 Codex 做 cross-test
  │
  └─ Codex (1-2 个 session)
        │ qe_config / qmt_client / 滚动训练专项
        ├─ 自测 L0/L1，push 后让 Claude Code 做 cross-test
        └─ 通过测试平台访问 Claude Code 模块的 bug 队列
```

三层各管一件事：
- **§17/§18 多窗口/Agent Teams**：执行加速（同时干活）
- **§20 Cross-testing**：质量交叉验证 + 长期协作（互相把关）
- **测试平台**：bug 真源 + 工作流自动化（持久协调）

### 20.8 一句话结论

**Cross-testing + 测试平台作为 Codex/Claude Code 协作中枢是合理且天然契合 AIstock 现有基础设施的方案**——`docs/codex_project_memory.md` 762-764 行规定的 GitHub Issues 单源 + Validation DB + agent events 已经是这个模式的雏形，只需要补上 agent 身份字段、cross-test 自动路由、tester 权限隔离三件事（约 2-3 周工作量）。

**与 §17 多窗口和 §18 Agent Teams 不冲突，三层叠加最有效**：多窗口/Agent Teams 解决执行加速、cross-testing 解决质量交叉验证、测试平台是底层 bug 真源和工作流自动化。

**4 周 MVP 期建议人工 cross-test**（用户当 lead，手工分配测试任务），测试平台稳定后升级为自动路由。**滚动训练等长期工作流上线后，cross-testing 的价值会被放大**——Codex 训出新模型，Claude Code 跑 shadow 对账并填模型衰减/分布漂移类 bug；反之亦然。

**最大的执行风险是 tester agent 想动手修代码**——必须通过权限隔离 + prompt 约束 + hook 三重保障落实"只填 bug、不动代码"硬约束。这条做对了，cross-testing 长期可持续；做不对，第一周就会退化成"两个开发者都改对方代码"的混乱。

---

## 21. 测试流水线现状盘点 + 与 Cross-testing 目标的差距

经实地核查 AIstock 仓库，测试流水线的成熟度**显著高于 §20.2 的初步估计**。本节给出准确盘点和到目标的实际差距。

### 21.1 已就位的基础设施（远比 §20 估计完整）

#### 21.1.1 文件 / 代码量

| 组件 | 路径 | 行数 | 用途 |
| --- | --- | ---: | --- |
| Validation 入口脚本 | `scripts/aistock_validate.py` | 787 | 本地验证执行 + 元数据/证据 sidecar 写入 |
| Nox 会话定义 | `noxfile.py` | 1029 | L0-L5 各 session 定义 |
| Pre-commit 钩子 | `.pre-commit-config.yaml` | n/a | 提交前自动检查 |
| Semgrep 守护 | `.semgrep/aistock/` 目录 | n/a | 静态规则扫描 |
| Validation backend | `backend/services/validation/` | **4059** | 完整服务层（见下） |
| Validation API router | `backend/routers/validation.py` | 513 | 28 个 GET 端点（plans / runs / findings / bugs / ui-targets / coverage / evidence） |
| Validation Center UI | `frontend/src/app/validation-center/` | 已存在 | 前端面板 |

`backend/services/validation/` 拆解：

| 模块 | 行数 | 职责 |
| --- | ---: | --- |
| `execution_runner.py` | 807 | 测试 plan 执行编排 |
| `finding_store.py` | 441 | **finding/bug 存储，已含 `assigned_agent` 字段、`agent_context` schema (`aistock_validation_agent_context_v1`)** |
| `history_store.py` | 442 | 历史 run 记录持久化 |
| `ui_target_catalog.py` | 416 | UI 测试目标目录 |
| `file_ownership.py` | 410 | **文件 ↔ 模块归属追踪** |
| `git_status_provider.py` | 393 | git 状态信息 |
| `git_activity_provider.py` | 318 | **git 活动追踪（commit 作者、修改文件等）** |
| `module_quality.py` | 308 | 模块质量度量 |
| `module_registry.py` | 199 | 模块目录读取 |
| `plan_catalog.py` | 175 | 测试 plan 目录 |
| `models.py` | 18 | Pydantic 模型 |

#### 21.1.2 配置 / 目录类资产

| 资产 | 位置 | 内容 |
| --- | --- | --- |
| 模块注册表 | `tests/aistock_validation/catalog/module_registry.yaml` | schema_version=`aistock_module_registry_v1`，含模块层级（如 `qe / qe.core / qe.single_experiment / qe.custom_evolution / qe.auto_evolution / qe.enhanced_metrics`），每模块有 `module_type / risk_level / ui_routes / api_routes / test_plans.required_on_change / test_plans.recommended` |
| 文件归属 | `tests/aistock_validation/catalog/file_ownership.yaml` | 文件 → 模块映射 |
| 测试 plan 目录 | `tests/aistock_validation/catalog/test_plans.yaml` | 命名 plan 列表 |
| UI 目标目录 | `tests/aistock_validation/catalog/ui_targets.yaml` | 路由级 UI 验证目标 |
| L 等级定义 | `tests/aistock_validation/catalog/test_levels.md` | L0-L5 完整定义（trigger / minimum evidence / claim boundary） |
| 模块测试矩阵 | `tests/aistock_validation/modules/` | 7 个：`development_guardrails / local_data_management / paper_v2_selection_center / qe / qe_archive / qe_data_completeness / validation_center` |
| 历史记录 | `tests/aistock_validation/history/<module>/` | 按模块分目录的时间戳记录（`YYYYMMDD_HHMMSS_lN_<topic>.{json,md}`） |
| 模板 | `tests/aistock_validation/templates/` | `test_case.md` / `test_run_record.md` |

#### 21.1.3 已有的 28 个 API 端点（Validation Center router）

含 `/plans` / `/runs` / `/coverage` / `/evidence` / `/ui-targets` / `/findings` / `/bugs` 全套读路径，**已经支持按 agent 过滤 findings**（`finding_store.py:115, 130-132`）：

```python
agent: str | None = None,
...
if agent:
    agent_l = agent.lower()
    items = [item for item in items if agent_l in str(item.get("assigned_agent") or "").lower()]
```

#### 21.1.4 L0-L5 等级现状（已生产化）

`test_levels.md` 已规定每级 trigger / minimum evidence / claim boundary。**Codex memory 显示 L0-L4 在最近 1 个月被多次实际运行**（如 `paper_v2_selection_center/20260506_l3_*` / `qe_archive/20260502_*_l3_*` / `qe_data_completeness/20260504_l3_*` 等）——不是设计文档，是活跃使用中的工具链。

明确禁止生产 8001 重启、要求 `pass_scope` + `business_assertion` 元数据、记录 evidence 与 coverage——这套规则已经写进 `test_levels.md` 并被遵守。

### 21.2 §20 提出的 cross-testing 三件事 vs 现状对照

§20.3 列出的三件需要补的能力，在现状下重新评估：

| §20.3 提出的需求 | 现状 | 真实差距 |
| --- | --- | --- |
| **Agent 身份记录** | `finding_store` 已有 `assigned_agent` 字段 + `agent_context` schema (`aistock_validation_agent_context_v1`) + `_finding_agent_context()` + `_bug_agent_context()` 辅助函数 + API 已支持按 agent 过滤 | **80% 就位**。差的是：① 区分 `developer_agent` vs `tester_agent`（当前只有 `assigned_agent` 单字段）；② 在 commit / test_run 元数据中也带上身份字段 |
| **Cross-test 自动分配（按分支前缀路由）** | `git_activity_provider.py` 已知道 commit 作者和分支信息；`module_registry` 知道模块归属 | **核心数据已有，路由逻辑没写**。需要新增一个 `cross_test_router` 服务（约 100-200 行），消费 git_activity_provider + file_ownership + module_registry，输出"该 push 应触发哪个 agent 做 L2/L3" |
| **测试只填 bug、不修复硬约束** | 平台层无强制；当前所有 agent 都有完整写权限 | **未就位**。需要：① settings.json 增加 tester-mode hook（PreToolUse 拦截 Edit/Write）；② Validation API 增加 tester token 限制；③ 在 cross-test 任务的 system prompt 中写入硬约束 |

#### 21.2.1 修正后的工作量

§20.3 估"约 2-3 周工作量"，现在重新估：

| 子任务 | 工作量 | 备注 |
| --- | --- | --- |
| 在 finding/bug schema 增加 `developer_agent` / `tester_agent` 双字段（兼容现有 `assigned_agent`） | 0.5-1 天 | finding_store + models 改动 |
| commit 元数据 / test run 元数据扩展 agent 身份 | 1-2 天 | 与 git_activity_provider 联动 |
| Cross-test 路由服务（按分支前缀 + 文件归属决定 cross-tester） | 2-3 天 | 新建 `backend/services/validation/cross_test_router.py` |
| Validation API 新增触发 cross-test 端点（POST /pipeline/cross-test/trigger） | 1 天 | router.py 加端点 |
| Tester-mode hook（PreToolUse 拦截非 test 文件写入） | 1-2 天 | settings.json + 一个小 Python hook 脚本 |
| Tester token / 权限隔离 | 2-3 天 | 设计 token scheme，与 GitHub fine-grained PAT 联动 |
| Bug 状态机（`NEW → TRIAGED → ASSIGNED → FIXING → FIXED → VERIFIED → CLOSED → REOPENED`）实现到 finding_store | 2-3 天 | 状态字段 + transition API |
| Re-test 触发逻辑（开发者 push fix 后自动 re-trigger 同一 cross-tester） | 1-2 天 | git_activity_provider + cross_test_router 联动 |
| MCP server 暴露给 Claude Code（让我直接调验证 API） | 1-2 天 | 类似现有 mempalace MCP |
| Validation Center UI 增加 agent 列 + cross-test 状态列 | 2-3 天 | 前端 layout.tsx + page.tsx |
| 文档与模板（`tests/aistock_validation/templates/` 增加 cross_test_record.md） | 0.5 天 | |
| **合计** | **14-22 工作日（约 3-5 周日历周，因为 Codex/Claude Code 协调有间隔）** | |

**与 §20.3 估的 2-3 周差距不大**——核心数据模型 80% 已就位，主要是补"路由 + 状态机 + 权限 + UI 适配"。

### 21.3 仍存在的短板（不在 cross-testing 范围内、但影响整体可用性）

#### 21.3.1 测试 plan 实际覆盖度

`tests/aistock_validation/modules/` 只有 7 个模块的测试矩阵。AIstock 当前模块远超 7 个（看 `module_registry.yaml` 已有 30+ 模块层级）。**模块测试矩阵覆盖率约 20-25%**——大量模块没有写过 plan，cross-test 触发后无 plan 可跑。

应对：
- MVP 阶段只在已有矩阵的模块上启用 cross-test
- 新模块开发时把"写测试矩阵"作为开发完成的一部分（cross-tester 帮忙补也算）

#### 21.3.2 自动 trigger 机制

当前看 `aistock_validate.py` 是手动入口，git push 后自动触发 cross-test 没看到现成实现。这一块需要补 GitHub Actions / git hook，估计另算 1-2 天。

#### 21.3.3 跨工具机器可读上下文

Codex memory 763 行的"machine-readable bug agent-context"（`reproduce command, failing run/evidence, allowed write scope, suspected files/modules, safety constraints, required verification commands`）部分已在 `agent_context` schema 中，但需要核实字段完整性。这是**让 cross-tester agent 一接到任务就能立即开干**的关键——如果上下文不完整，agent 要花时间问、读代码、推测，加速效果会减半。

#### 21.3.4 Codex 接入方式

Claude Code 可以通过 MCP server 自然访问 Validation API。**Codex 怎么接入需要确认**：
- 如果 Codex 本身支持 MCP（部分版本支持），同样接 MCP server 即可
- 如果不支持，Codex 只能通过 HTTP / CLI 调用，需要写一组 Codex skill 包装常用 API

#### 21.3.5 测试平台自身的稳定性

`backend/services/validation/` 4059 行也是软件，本身可能有 bug。**MVP 阶段不应把流水线的 hard gate 卡死**——按 §17.5 的判断，先 best-effort，等流水线本身稳定后再升级强 gate。

### 21.4 整体差距评估

| 维度 | 现状 | 距离"§20 cross-testing 全自动" |
| --- | --- | --- |
| L0-L5 等级定义 | ✓ 完整 | 0% 差距 |
| 模块注册表 + 层级 | ✓ 完整（30+ 模块） | 0% 差距 |
| 文件归属追踪 | ✓ 完整 | 0% 差距 |
| Finding/Bug 存储 | ✓ 含 agent_context | 10% 差距（双字段 + 状态机） |
| Validation API（read） | ✓ 28 端点 | 0% 差距 |
| Validation API（write/trigger） | ⚠ 部分 | 30% 差距（cross-test trigger 端点 + 状态转换 + re-test 触发） |
| Git 活动追踪 | ✓ | 0% 差距（已有作者/分支/文件信息） |
| **Cross-test 自动路由** | ✗ | **70% 差距** |
| **Tester 权限隔离** | ✗ | **80% 差距** |
| **Bug 状态机** | ⚠ 雏形 | 50% 差距 |
| **MCP server 暴露** | ⚠ AIstock 已有 mempalace MCP，但 validation MCP 没看到 | 60% 差距（参考 mempalace 实现一份） |
| **Codex 接入路径** | ⚠ 待确认 | 50% 差距 |
| **Validation Center UI** | ✓ 存在 | 20% 差距（增加 agent 列 + cross-test 状态视图） |
| **模块测试矩阵覆盖** | ⚠ 7/30+ | 75% 差距（写矩阵是长期任务，不是平台能力问题） |
| **自动 trigger（push → run）** | ⚠ 没看到 | 40% 差距（GitHub Action + webhook） |

**整体加权差距 ≈ 35-40%**——比 §20 估计的"接近 0% 仅需补 3 件事"更实际，但远比"需要从 0 搭"乐观。

### 21.5 建议的实施节奏

#### 21.5.1 4 周 MVP 阶段（与 vn.py 接入并行）

不依赖 cross-test 自动化——**用人工 cross-test**：
- 用户当 lead，每次 push 后口头/书面分配"这个 PR 让 Codex 测一下"
- Codex 跑测试、发现 bug 写 GitHub Issue（手工）
- 平台层不强制路由

工作量：0（用现有手工流程）。**优先把 vn.py + miniQMT MVP 跑通**。

#### 21.5.2 MVP 收尾期（Week 4 末 / 第 5 周）

并行启动 cross-testing 平台增强（不阻塞 MVP）：
- finding/bug 双 agent 字段
- Cross-test 路由服务原型
- Validation Center UI 加列

工作量：1-2 周（可由 Codex 或 Claude Code 跨模块协作做）。

#### 21.5.3 产品化阶段（与滚动训练并行）

完整启用 cross-testing：
- Tester 权限隔离 hook
- Bug 状态机
- 自动 trigger（push → run）
- MCP server for Claude Code
- Codex 接入路径敲定
- 模块测试矩阵补到 80%+ 覆盖

工作量：3-5 周，分散在 5-8 个月产品化期内。

### 21.6 一句话结论

**测试流水线的基础设施成熟度远超 §20.2 估计**——4059 行的 validation 服务 + 30+ 模块注册 + L0-L5 完整定义 + 28 个 API 端点 + agent_context schema 已就位。

**到 §20 描述的 cross-testing 全自动状态，整体差距约 35-40%**——核心数据模型 80% 完成，路由逻辑、tester 权限隔离、bug 状态机、MCP 暴露需要补；模块测试矩阵覆盖率（当前 ~20%）是最大的非平台短板。

**实施节奏建议**：4 周 MVP 期用人工 cross-test 不依赖平台增强；MVP 收尾期并行启动平台增强（1-2 周）；产品化阶段完整启用（3-5 周分散落地）。**总平台增强工作量 ≈ 4-7 周，分散执行不阻塞 MVP**。

**最大短板不是平台能力，是测试矩阵覆盖度**——这要靠每个新功能开发时附带写测试矩阵长期积累，不是一次工程能补上。Cross-testing 启用后这个短板会被加速暴露，反过来推动开发者写更全的测试 plan，形成正循环。

---

## 22. 7 个研发方向的优先级综合排序

回答用户在 2026-05-08 的问题：

> 7 个方向：(1) QE 模型 seed 不固定 (2) HMM 优化 (3) 模拟盘选股+运行 (4) 自动化测试流水线 (5) 公告/财报独立信号 (6) QE 自动演进 (7) 多 alpha 架构。
> 两个优先目标：A. QE 找到近期最佳组合 + 选股和模拟盘验证。B. QE 持续优化，半自动化演进。
> 自动化测试流水线是否最高优先级？

**直接结论：不是。按你的目标 A+B，测试流水线是 Tier 3 而不是 Tier 0**。但你的直觉有合理成分——**研究端的可复现性 / 比较框架 / IC 追踪基础设施确实是 Tier 0**，只是这部分不是 §21 那个 cross-testing 平台，而是 #1（模型 seed 控制）派生出的研究 test infra。下面展开。

### 22.1 关键重构：区分"研究 test infra" vs "工程 CI infra"

测试基础设施有两种，**对你目标的优先级完全不同**：

| 类别 | 内容 | 对目标 A 影响 | 对目标 B 影响 |
| --- | --- | --- | --- |
| **研究 test infra** | 模型 seed 控制 / 训练可复现性 / 模型间对比框架 / IC 追踪 / NAV 对账 / 因子稳定性度量 | **直接阻塞**——seed 不固定时"找到最佳组合"是伪命题（同模型跑两次得不同结果） | **直接阻塞**——演进需要可比较，不可比较则无法判断"新版是否优于旧版" |
| **工程 CI infra**（即 §21 的测试流水线 + cross-testing） | L0/L1/L2/L3 自动化 / cross-test 路由 / agent 身份 / bug 状态机 | 间接支持——MVP 单策略 demo 可用人工测试 | 间接支持——研究迭代主要靠开发者自己跑实验，不是大规模并行 |

**你说的"测试流水线最高优先级"如果指研究 test infra，是对的**——这正是 #1（seed）的本质。**如果指工程 CI infra，则不是 Tier 0**——目前 4059 行已足够支撑当前规模。

下面所有讨论按"研究 test infra"和"工程 CI infra"分别处理。

### 22.2 7 个方向对两个目标的依赖矩阵

| 方向 | 目标 A 依赖 | 目标 B 依赖 | 状态 |
| --- | --- | --- | --- |
| **#1 QE 模型 seed 不固定** | **必须先解决**（不解决"最佳组合"无意义） | **必须先解决**（不解决无法对比演进结果） | 阻塞两条路径 |
| #2 HMM 优化 | 弱依赖（用现有 HMM 即可） | 中等依赖（HMM 是组合的一部分） | 可推后 |
| **#3 模拟盘选股+运行** | **目标 A 的最终交付** | 弱依赖（演进的验证渠道之一） | 在目标 A 关键路径上 |
| #4 自动化测试流水线 | 弱依赖（人工 cross-test 即可，§21.5.1） | 中等依赖（演进多了之后批量验证有用） | 不在关键路径，但有长期价值 |
| #5 公告/财报独立信号 | 不依赖 | 中等依赖（新信号源是演进材料之一） | 后期接入 |
| **#6 QE 自动演进** | 弱依赖（A 阶段用现有 manifest） | **目标 B 的核心** | 在目标 B 关键路径上 |
| #7 多 alpha 架构 | 弱依赖（A 阶段单 alpha 即可） | 中等依赖（多 alpha 是演进维度之一） | 中后期 |

#### 22.2.1 关键观察

- **#1 同时阻塞 A 和 B**——这是真正的 Tier 0
- **#3 是目标 A 的最终交付**，#6 是目标 B 的核心——两条 Tier 1 主干
- **#4 不在任何关键路径上**——可以推迟到主干工作展开后再补强
- **#2 / #5 / #7 是可推后的支线**——不阻塞核心目标

### 22.3 #1 模型 seed 不固定 —— 必须最先解决（Tier 0）

#### 22.3.1 为什么是 Tier 0

如果同一份 QE 实验配置（同因子集 + 同模型 + 同超参 + 同数据）跑两次得到不同结果，**意味着**：

- "最佳组合"是统计随机性，不是实质性优势——目标 A 的"找到最佳"在数学上不成立
- 演进 loop 之间无法对比——目标 B 的"新版优于旧版"无法判断
- shadow run 对账（§11.3 / §15.6）会因 seed 不固定而失败——产品化阶段也走不通
- Paper v2 与 QE 的输出对账（§13.1）会因模型本身随机性而 NAV 不一致——vn.py 接入后也对不上账

#### 22.3.2 当前现状（需 Codex 进一步核实）

可能的随机性来源（按概率排序）：
1. **训练框架随机性**：LightGBM / XGBoost / NN 的初始化、数据 shuffle、采样、dropout、bagging
2. **数据切分随机性**：QE 的 train/val/test 切分如果用了 random_state 不固定
3. **多线程/GPU 非确定性**：CUDA 卷积、reduce 操作、PyTorch backend
4. **特征工程随机性**：因子生成中的归一化、缺失值填充、cross-sectional ranking 在 tie 处理上
5. **依赖版本漂移**：sklearn / lgb / torch 版本变化导致同 seed 不同结果

#### 22.3.3 修复方向（不写实现，只写设计要点）

- **统一 seed pipeline**：在 QE 配置层引入全局 `master_seed`，传播给所有子组件（数据切分 / shuffle / 模型初始化 / GPU backend）
- **确定性 GPU 设置**：`torch.backends.cudnn.deterministic=True` + `cublas_workspace_config`
- **依赖锁定**：requirements.txt 锁定 minor 版本（sklearn / lgb / torch）
- **可复现性测试**：`tests/aistock_validation/modules/qe_reproducibility.md`（**新建**）规定"同 manifest 同 seed 跑两次，NAV 差异 < 0.01bp、持仓 100% 相同"作为门禁
- **manifest 中记录 seed**：`StrategyPackage manifest` 在 `data_split` 旁加 `master_seed` 字段，纳入 frozen contract

**估算工作量**：1-2 周（属于 Codex 维护的 QE 配置层 + RD-Agent worker，不在我的默认模块范围）

### 22.4 优先级三条赛道（Tier 1-3）的详细规划

#### Tier 0：必须先做（约 1-2 周）

| 任务 | 归属 | 工作量 |
| --- | --- | --- |
| #1 QE 模型 seed 控制 + 可复现性测试 | Codex（QE 配置层 + RD-Agent worker） | 1-2 周 |

**完成判定**：同 manifest 同 seed 两次训练 NAV 差异 < 0.01bp + 持仓 100% 相同——写入 `tests/aistock_validation/modules/qe_reproducibility.md` 作为门禁。

#### Tier 1A：目标 A 主干（约 5-7 周，与 Tier 0 末段重叠开始）

| 任务 | 归属 | 工作量 |
| --- | --- | --- |
| 在 Tier 0 完成后，从近期 QE 演进中选一个验证可复现的最佳 manifest | 用户 + Codex | 0.5 周 |
| §16 vn.py + miniQMT 模拟盘 4 周 MVP | Claude Code（多窗口/Agent Teams） | 4 周 |
| Paper v2 选股 UI 调试 + 简化（最小） | Claude Code | 0.5-1 周 |
| 实地跑通一份策略包端到端 sim | Claude Code + 用户 | 1 周（含调试 buffer） |

**完成判定**：可演示的模拟盘 demo + 一份可复现 manifest 的实测 NAV/持仓记录。

#### Tier 1B：目标 B 主干（约 8-12 周，与 Tier 1A 并行）

| 任务 | 归属 | 工作量 |
| --- | --- | --- |
| 模型+因子组合**对比框架**（IC / Rank IC / 年化收益 / 回撤 / 换手 / 稳定性多维度对比） | Codex（QE 演进层） | 2-3 周 |
| #6 QE 自动演进改造（基于对比框架做候选选择） | Codex | 4-6 周 |
| 演进 loop 之间的"晋级 gate"（新 loop 必须在 N 个维度上优于 SOTA 才进入下一轮） | Codex | 1-2 周 |
| #7 多 alpha 架构稳定化（修现有 bug + 在演进框架中支持多 alpha） | Codex（多 alpha 是 Codex 维护范围） | 4-6 周（与上面并行） |

**完成判定**：用户能在 UI 上看到一个 evolution dashboard，展示当前 SOTA / 候选 / 晋级历史；演进可半自动化执行（用户点"开始演进"，系统跑 N 轮，自动按 gate 晋级）。

#### Tier 2：长期支撑能力（穿插在 Tier 1A/1B 之间，约 3-5 周分散）

| 任务 | 归属 | 工作量 | 时机 |
| --- | --- | --- | --- |
| #2 HMM 优化（信号准确度 / 系数生成稳定性 / 覆盖率 bug） | Codex | 2-3 周 | Tier 1A 末或 1B 中 |
| #4 工程 CI infra 增强（finding/bug 双 agent 字段 + cross-test 路由 + tester 权限隔离） | Claude Code 或 Codex | 1-2 周 | Tier 1A 末 |
| §21 测试矩阵补齐（重点补 trading_core / paper_v2 / qe_reproducibility 三个） | 各模块 owner（写自己模块的矩阵） | 1-2 周分散 | 与开发并行 |

#### Tier 3：可推后的扩展（Tier 1+2 完成后，约 4-8 周）

| 任务 | 归属 | 工作量 |
| --- | --- | --- |
| #5 公告/财报独立信号 R&D | Codex 或专项研究人员 | 4-6 周 |
| 完整工程 CI infra（自动 trigger / MCP server / Bug 状态机 / 模块矩阵 80%+ 覆盖） | 双方协作 | 3-5 周分散 |
| §15.6 vn.py 产品化收口（OMS 双账本对账 / DB schema 重做 / shadow run / 实盘准备） | Claude Code 主导 + Codex 协作 | 同 §15.6 |
| 滚动训练专项（§19.3） | Codex | 6-10 周 |

### 22.5 推荐的 6 个月日历表（含 Tier 划分）

```
Month 1                Month 2          Month 3          Month 4-6
├─ Week 1-2 ─────┐
│  Tier 0 #1 seed │
│  (Codex 主导)    │
└──────────────────┘
                  ├─ Week 3-7 ──────────────────────┐
                  │  Tier 1A: vn.py MVP + paper sim   │
                  │  (Claude Code 多窗口/Agent Teams)  │
                  │  目标 A 关键路径                  │
                  └────────────────────────────────────┘
                  ├─ Week 3-14 ─────────────────────────────────────────┐
                  │  Tier 1B: 对比框架 + #6 自动演进 + #7 多 alpha       │
                  │  (Codex 主导)                                        │
                  │  目标 B 关键路径                                    │
                  └────────────────────────────────────────────────────────┘
                                  ├─ Week 6-9 ───┐
                                  │ Tier 2 穿插：#2 HMM + #4 CI 增强 │
                                  └─────────────────────────────────┘
                                                        ├─ Week 14-26 ───────┐
                                                        │  Tier 3：#5 公告信号 │
                                                        │  + 产品化收口 + 滚动 │
                                                        │  (并发多轨道)        │
                                                        └────────────────────────┘
```

### 22.6 关键决策建议

#### 22.6.1 把 #1 当作"研究 test infra"，而不是普通 bug fix

模型 seed 不是孤立的 bug——它是**整个研究系统可信度的基础**。修完后会自然要求：
- 把 `master_seed` 加入 manifest（contract 升级）
- 把"可复现性"作为 QE 演进 gate 的一部分
- 把"近期 SOTA 是哪一个"作为元数据持续追踪
- 把模型对比框架建立起来（已在 Tier 1B 内）

**这一系列推进会把 #1 自然演变成 Tier 1B 的"对比框架"基础**——不是单点修复，是基础设施级别的工作。所以 Tier 0 的 1-2 周工作量应该理解为"打开一个长期工作流"。

#### 22.6.2 工程 CI infra（#4）的合理时机

不是越早越好。**当下面任一信号出现时再发力**：
- Tier 1A 完成、有 2+ 个并行开发活动同时进行（cross-testing 价值放大）
- 模块测试矩阵覆盖度从 ~20% 提升到 50%+（有东西可以自动跑）
- 实盘前期（产品化阶段）需要严格的回归和 shadow run（强 gate 价值放大）

**MVP 期保持人工 cross-test**（§21.5.1）即可，不要为了"先把 infra 搭好"而放慢 Tier 1。

#### 22.6.3 #2 #5 #7 的处理

- **#2 HMM 优化**：HMM 在选股和模拟盘中是**可选组件**，不强求 MVP 内修。但既然 HMM 系数生成有覆盖率 bug（参见 codex memory `hmm_viterbi_forward_filter_fix.md`），建议在 Tier 1A 末抽 2-3 周做掉。
- **#5 公告/财报信号**：是新的研究方向，**与目标 A/B 都不在同一时序**。建议放 Tier 3，等目标 A 演示完、目标 B 演进框架成熟后，作为新维度接入演进——这时已有对比框架可衡量"加入公告信号是否提升组合表现"。
- **#7 多 alpha**：现有架构有 bug（参见 codex memory `project_multi_alpha_progress.md` "回测未执行+UI多节点日志不可见"）。建议在 Tier 1B 内修复——多 alpha 是演进的重要维度，没多 alpha 演进会受限。

#### 22.6.4 用户需要做的关键决策

1. **Tier 0 由 Codex 接还是 Claude Code 接？** —— QE 配置层属 Codex 维护范围，但如果 Codex 当前有其它高优任务，可以考虑让 Claude Code 临时介入（但需要用户当回合明确授权跨边界）
2. **Tier 1A 和 Tier 1B 是否真的并行启动？** —— 并行需要双方 agent + 用户精力分配，串行更省心但 6 个月变成 9-10 个月。**建议并行**——目标 A 给信心，目标 B 给长期价值
3. **#4 工程 CI infra 的具体启动节点** —— 推荐 Tier 1A 末（约 Week 7-8）轻量启动 finding/bug 双字段 + cross-test 路由原型；其余推到 Tier 3
4. **#5 公告信号是否拉前？** —— 不建议。Tier 3 内启动，避免目标 B 演进框架还没建好就增加变量

### 22.7 一句话结论

**测试流水线（#4 工程 CI infra）不是 Tier 0，但模型 seed（#1 研究 test infra）是 Tier 0**——你"测试基础设施最高优先"的直觉对，但具体抓的对象需要重选。

**优先级应为**：
- **Tier 0**（1-2 周）：#1 模型 seed 修复 + 可复现性测试 → 解锁所有后续工作
- **Tier 1A**（5-7 周，目标 A 关键路径）：vn.py + miniQMT 4 周 MVP（Claude Code 主导）
- **Tier 1B**（8-12 周，目标 B 关键路径，与 1A 并行）：模型+因子对比框架 + #6 自动演进 + #7 多 alpha 修复（Codex 主导）
- **Tier 2**（穿插，3-5 周）：#2 HMM 优化 + #4 工程 CI infra 轻量启动 + 测试矩阵补齐
- **Tier 3**（4-8 周，Tier 1+2 后）：#5 公告信号 + 产品化收口 + 滚动训练 + 完整 CI infra

**6 个月日历内可以达成两个目标**：目标 A 在 Month 2 末有 demo，目标 B 在 Month 3 末有半自动演进 dashboard。**前提是 #1 在 Month 1 内修完**——这是整个计划的起点。

---

## 23. 自动化测试流水线建成后的真实收益评估

回答用户在 2026-05-08 的追问：

> 自动化流水线开发完成是否能：(1) 提升整个项目开发效率？(2) 提升代码质量？(3) 减少手工测试验证修复 bug 时间？(4) 实现多开发工具直接配合？

**直接结论：四类收益都会兑现，但量级和时机有显著差异**。下面分别量化，并与 §22 优先级建议保持一致——避免造成"是不是该提前做 #4"的认知反复。

### 23.1 四类收益的真实量级

#### 23.1.1 开发效率（25-40% 提升，但仅在特定场景）

**有效场景**：
- 多模块并行开发（4+ 个 Claude Code/Codex 窗口同时跑）：节省人工分发测试任务、人工跟踪 bug 状态、人工协调 re-test 时间——估约 **节省 30-40% 协调时间**
- 重复性回归（模块改动后的 L0/L1 自动跑）：每次改动节省 5-15 分钟手工跑测试——估约 **节省 20-30% 单次提交时间**
- 跨日异步协作（开发者下班后 cross-tester 自动跑）：把"等待对方在线"的窗口压平——估约 **加速 1-2 个工作日的迭代**

**无效或低效场景**：
- 研究探索期（写实验、调因子、看 IC 图）：研究循环主要在 notebook + Qlib MLflow，与 CI infra 关系小——**几乎无加速**
- 单人/小团队短任务：搭流水线本身的成本可能 > 节省的时间——**净负**
- 真实环境集成（vnpy_xt + miniQMT、broker 行为）：必须真实环境调试，CI 跑不出来——**0 加速**

**对 AIstock 当前**：Tier 1A（vn.py MVP，4-5 人月）+ Tier 1B（演进框架，6-8 人月）期间，工程 CI infra 帮的是 1A 末段开始（多模块集成密集），不是 1A 初段（研究 + MVP 单线）。

#### 23.1.2 代码质量（短期改善有限，长期复利显著）

**能直接提升**：
- 静态规则违反（已由 semgrep + L0 守护）：当前已经在用，**继续投入边际收益递减**
- 回归 bug（改 A 模块破坏 B 模块）：cross-testing 强制跑跨模块测试，**中等收益**——但前提是测试矩阵覆盖到位（§21.3.1 当前 20% 覆盖率）
- 接口契约违反（API schema 漂移）：自动化 contract test 抓到——**有效但小众**

**不能直接提升**：
- **设计质量**：UI 混乱（§1）、策略类碎片（§13.1）、文档与实现不一致（§13.2）——这些是设计层 bug，自动化测试发现不了
- **架构债务**：Paper v2 没用 vn.py 设计承诺、模型 seed 不固定——这些是系统层判断，要人或专门审计 agent 做
- **研究正确性**：因子未来函数泄漏、训练-推理偏移、PIT 违规——semgrep 能抓部分模式，但语义级泄漏需要专门的研究审计工具

**真实评价**：自动化流水线对代码质量的提升是**"防止变坏"远大于"主动变好"**——它降低回归风险，但不创造好设计。

#### 23.1.3 减少手工测试验证修复 bug 时间（30-50% 节省，但仅在中后期）

**节省时间的来源**：
- 每次 push 自动跑 L0/L1：开发者不用手工 `nox -s ...`——**节省 5-10 分钟/次**
- Bug 状态机自动流转：不用手工 GitHub Issue 标签维护——**节省 10-20 分钟/bug**
- 自动 re-test 触发：修完不用手工通知 cross-tester——**节省 30-60 分钟/round**
- 跨工具消息统一在 GitHub：不用在多窗口/多对话间手工同步——**节省 20-40 分钟/天**（对密集协作期）

**累计估算**：在密集开发期（Tier 1B 末 / Tier 2 / 产品化阶段），人工测试和协调时间从约 30% 降到约 15%——**净节省 15-20% 的开发者总时间**。

**但**：在 Tier 0（修 seed）和 Tier 1A 早期（vn.py PoC + MVP），手工测试占比本来就低（研究 + 集成 demo），自动化的边际节省**不到 5%**。

#### 23.1.4 多开发工具配合（是真实收益，但需要平台增强先到位）

参见 §20 / §21 cross-testing 详细分析。**核心收益**：

- 消除"自测偏差"——独立 tester 发现的是另一类 bug
- 异步解耦——不要求双方同时在线
- 单一 bug 真源——避免口头协调
- 长期知识交叉——agent 通过 cross-test 学习对方模块

**量级**：4 周 MVP 期（手工 cross-test）跟产品化期（自动化 cross-test）比较：手工 cross-test 协调成本约 20-30% 总时间；自动化后降到 5-10%，**净节省 15-20%**。

但 §21.4 显示当前到完整自动 cross-testing 还有 **35-40% 平台差距**（路由 / 权限隔离 / 状态机 / MCP / 模块矩阵覆盖），这部分本身要 **4-7 周分散工作量**。

### 23.2 ROI 曲线（投入 vs 产出）

| 阶段 | 已投入 | 边际投入 | 边际收益 | 累计收益 | 净 ROI |
| --- | --- | --- | --- | --- | --- |
| 现状（4059 行 + 7 模块矩阵 + L0-L5 已用） | ~6-8 人月 | - | - | 已收回大部分投入 | 已正 |
| 增量到 50% 自动 cross-testing（双 agent 字段 + 路由原型 + 基本 hook） | +1-2 周 | 1-2 周 | 短期：手工分发取消；长期：cross-test 习惯养成 | 中等 | 高（投入小、收益直接） |
| 增量到 80% 自动 cross-testing（状态机 + MCP + 自动 trigger + 测试矩阵补到 50%） | +3-5 周 | 3-5 周 | 协调时间从 20% 降到 10% | 大 | 中等（投入中、收益要 3-6 个月才显现） |
| 增量到 95% 自动（全部模块矩阵覆盖 + shadow run 对账自动化） | +6-10 周 | 6-10 周 | 协调时间从 10% 降到 3% | 大 | 低（投入大、收益边际下降） |

**最优投入点：50% 自动**——边际投入小、收益立即兑现、不阻塞 Tier 1 主干。这正是 §22.4 Tier 2 内的 1-2 周轻量增强。

**80% 自动**：在 Tier 1 主干结束、产品化阶段启动时投入，收益与产品化阶段的"上实盘前需要 shadow run + 强 gate"需求匹配。

**95% 自动**：可能永远不必投入。当 80% 自动够用时，剩下 15% 用人工补即可——除非团队规模扩张到 10+ 人。

### 23.3 几个常见认知陷阱（必须避免）

#### 23.3.1 "测试自动化能解决质量问题"

**不能**。自动化只能验证**已被想到的测试场景**。设计质量、需求理解、边界条件覆盖度——这些是**写测试 plan 的人**决定的，不是流水线决定的。

§21.3.1 显示 AIstock 当前模块测试矩阵覆盖率 ~20%。即使流水线 100% 自动化，未覆盖的 80% 模块仍然没人测——**自动化不会变出测试 case**。

#### 23.3.2 "测试自动化是解决拖延的银弹"

**不是**。Paper v2 跑不通不是因为缺自动化测试；是因为：
- 设计承诺未落实（§13.2 EventEngine/Gateway/OMS 全没做）
- 阻断点没修（§0/§7 ST PIT spans + live inference 冷启动）
- 配置硬约束（§2 backtest_contract 把 9 项配置锁死）

这些**自动化测试发现不了**——它们是设计/架构/产品决策，需要人/审计 agent 介入。流水线再完善也不能让 Paper v2 自动跑通。

#### 23.3.3 "先把流水线做完再做主线工作"

**最危险的陷阱**。理由：
- 流水线本身是软件，有 bug，会产生维护成本
- 没有真实模块跑测试，流水线的"价值"无法验证（§21.3.1 提到 7/30 模块矩阵覆盖率）
- 团队精力是零和——投入流水线的精力不能投入 Tier 0/1A/1B
- §22 已分析：用户两个目标（A/B）都不在流水线关键路径上

**正确的节奏**：流水线在 Tier 1A 末轻量增强，而不是先把流水线做到 95% 再开始主线。

#### 23.3.4 "更多自动化 = 更高质量"

**不是线性关系**。当回归测试覆盖到 60-70%，再加测试用例的边际价值快速下降，反而增加维护成本（test flakiness、依赖更新、虚假 fail 排查）。**追求 95% 覆盖是浪费**——80% 已经在边际收益拐点上。

### 23.4 与 §22 优先级建议的一致性

§22 把 #4 工程 CI infra 排在 Tier 2/3，**本节支持这个判断不变**。具体配合：

| 阶段 | 流水线增强动作 | 收益时机 |
| --- | --- | --- |
| Tier 0（Week 1-2，#1 seed） | 流水线**不动**——保留现有手工流程 | n/a |
| Tier 1A（Week 3-7，vn.py MVP） | 流水线**不动** + Week 7-8 末轻量增强（双 agent 字段 + 路由原型） | 增强当周 + Tier 1A 末段集成期受益 |
| Tier 1B（Week 3-14，演进框架） | 流水线**轻量增强**（同上）+ 测试矩阵补 trading_core / paper_v2 / qe_reproducibility 三个 | Tier 1B 末段密集集成时受益 |
| Tier 2（Week 6-12 穿插） | #4 工程 CI infra 1-2 周完成 50% 自动 cross-testing | 立即受益于 Tier 1A 末和 Tier 1B 中后期 |
| Tier 3（Month 4-6，产品化） | 增强到 80% 自动（状态机 + MCP + 自动 trigger）+ 模块矩阵覆盖度提到 50%+ | 产品化阶段 + 实盘前 shadow run 全程受益 |

**关键判断**：流水线增强是"穿插式投入"，不是"先做完再做别的"。Tier 1+2 总投入约 4-7 周分散执行，与主线工作并行。

### 23.5 给用户的判断

#### 23.5.1 自动化测试流水线**会**：
- 在 Tier 1B 末和产品化阶段节省 15-20% 总开发时间
- 防止回归 bug，长期保护代码质量底线
- 让 Claude Code 和 Codex 真正实现异步协作（cross-testing 落地后）
- 上实盘前提供 shadow run / 对账的强 gate

#### 23.5.2 自动化测试流水线**不会**：
- 解决 Tier 0/1A 早期的开发效率问题（那些是研究 + 集成的 bottleneck，不是 CI bottleneck）
- 提升设计质量（架构债务靠人审计，不是自动测试）
- 让 #1 模型 seed / #6 演进 / #7 多 alpha 修复变快（这些是研究/算法工作，不是 CI 加速对象）
- 替代用户的关键决策（架构、不可逆操作、产品判断）

#### 23.5.3 投入节奏建议
- **Month 1（Tier 0）**：流水线 0 投入
- **Month 2（Tier 1A 末）**：1-2 周轻量增强（50% 自动 cross-testing）
- **Month 3-4（Tier 2/产品化前期）**：3-5 周分散增强到 80%
- **Month 5-6+（产品化）**：剩余增强按需做，可能停在 80%

**总投入 4-7 周，分散在 6 个月内执行**——不集中、不阻塞、不抢占 Tier 0/1 资源。

### 23.6 一句话结论

**自动化测试流水线建成后，会兑现"开发效率提升 15-25% / 代码质量底线保护 / bug 修复协调时间节省 30-50% / 多工具异步协作落地"四类收益**——但**仅在 Tier 1 末段和产品化阶段才显现**。Tier 0 和 Tier 1A 早期的研究/MVP 工作几乎不受流水线影响。

**最大陷阱是"先把流水线做完再做主线"**——流水线本身是工具，需要业务模块跑测试才有价值；没有 Tier 1 的成果，流水线 95% 自动化也是空转。

**正确节奏**：穿插式投入 4-7 周分散在 6 个月内，与 §22 三条主干并行——这样流水线的每一步增强都能立即在 Tier 1A/1B/产品化的当期工作中兑现价值，避免投资过早收益过晚。

---

## 24. QE 修改如何同步到模拟盘 / 实盘 —— 研究-生产一致性机制

回答用户在 2026-05-08 的关键风险问题：

> 今后 QE 实验与模拟盘要尽量一致，QE 中的修改（如 seed 问题）怎样同步到模拟盘？不共用代码情况下，怎样实现 QE 修改直接同步到模拟盘？

**直接结论：seed 这个具体例子大部分是自动同步的（不需要代码同步），但其他类型的 QE 修改有真实的传播风险**——这是研究-生产 gap 的本质问题，需要分类应对，不是单一机制。下面分四块展开：seed 修复的特殊性、QE 修改的分类与传播需求、三种架构选项、分阶段推荐。

### 24.1 Seed 修复这个具体例子 —— 大部分自动同步

让我们具体分析 §22 Tier 0 的 seed 修复在 QE 和 Paper v2 之间的传播路径：

| 步骤 | QE 端发生什么 | Paper v2 端是否受影响 | 是否需要代码同步 |
| --- | --- | --- | --- |
| 1. QE 训练时设置 `master_seed=42` | LightGBM/torch/numpy random 全部固定 | n/a | n/a |
| 2. 训练产生模型权重 | 权重确定（同 seed 跑两次得相同权重） | n/a | n/a |
| 3. 权重序列化到 `model_asset`（manifest 字段） | 已发生 | Paper v2 读取该 model_asset 做推理 | **不需要**——读 manifest 即可 |
| 4. Paper v2 推理调用 inference_engine.py | n/a | 输入 = 实盘数据 + 模型权重；模型权重确定，推理过程理论上确定 | **可能需要**——见下 |
| 5. Paper v2 因子计算（与 QE 用同样因子代码？） | n/a | 用 strategy_package_live_inference.py（与 QE 共享） | **已经共享**（参见 §13.1.4） |
| 6. Paper v2 端的随机性来源（cross-sectional ranking 在 tie 上、float 累加顺序、并行任务调度） | n/a | 可能引入小幅不确定性 | **小概率需要**——为 Paper 推理也设定 seed |

**核心判断**：
- 训练期的 seed 控制属于 QE 内部修复，**模型权重作为副作用自动捕获**
- Paper v2 读 manifest + 权重 → 自动获得 QE 修复的好处
- **唯一可能需要代码改动的位置**：Paper v2 自己的随机性来源（如 tie 处理）需要也设 seed——但这是 Paper v2 内部一次性补丁，与 QE 修复无关

**结论**：seed 修复 95% 自动同步；剩 5% 是 Paper v2 自身的小补丁。这是个**好运气**——不是所有 QE 修复都这么容易传播。

### 24.2 QE 修改的分类与传播需求

把 QE 可能发生的修改按"传播难度"分四类：

#### 24.2.1 类别 A：训练期修改（自动同步，无需代码改动）

举例：
- Seed 控制（§22 #1）
- 训练超参（learning rate / max_depth / n_estimators）
- 损失函数 / early stopping 策略
- 训练数据切分窗口
- 因子归一化策略（仅在训练流程内）

**传播机制**：修改 → 重训 → 新模型权重 → 新 manifest → Paper v2 读取
- **传播延迟**：~1 个训练周期
- **代码同步成本**：0（除非 manifest schema 变了，见类别 D）

#### 24.2.2 类别 B：因子层修改（自动同步，因为已共享）

举例：
- 新增因子定义（QE 演进 + RD-Agent 产出）
- 修改因子计算逻辑
- 因子缓存策略

**传播机制**：因子代码 / 缓存机制在 `backend/services/quantevolver/factor_*.py` + `inference_engine.py`，**Paper v2 直接 import**（参见 §13.1.4）。
- **传播延迟**：immediate（commit 即生效）
- **代码同步成本**：0

#### 24.2.3 类别 C：执行层语义修改（需要双侧代码改动）

举例：
- 新增 / 修改策略类（如 `EnhancedTopkDropoutStrategy` 增加新参数）
- 修改 risk policy 语义（如 ST PIT 规则变化）
- 修改 tradability 过滤（如新的停牌判定）
- 修改持仓权重计算（如 score 加权方法新增 `entropy` 选项）
- 修改动态 n_drop 阈值算法
- 修改 hold_thresh 计算方式
- 修改最小成交单位 / 整手处理

**传播机制**：QE 端在 `qe_strategies/` 改 → Paper v2 端在 `runtime.py` / `rebalance_strategies/` / vn.py adapter 改 → 必须有人手工镜像
- **传播延迟**：取决于人执行
- **代码同步成本**：**每次改动需要在 2-3 个地方改**——这是真实风险

**这是最危险的类别**——容易出现 QE 端改了 Paper 端忘了，导致回测和实盘行为漂移。

#### 24.2.4 类别 D：契约层修改（需要 schema 升级 + 双侧代码）

举例：
- manifest schema 新增字段（如 `master_seed`、`hmm_signal_preset_v2`）
- backtest_contract 新增校验规则
- 新增 alpha 类型（multi_alpha v2、event_signal）

**传播机制**：QE 端 `qe_source_resolver.py` / `backtest_contract.py` 升级 schema → Paper v2 端 `service.py` / `runtime.py` / 相关 router 升级 reader → 老 manifest 兼容性处理
- **传播延迟**：取决于双侧升级节奏
- **代码同步成本**：**重，但有显式 schema 引导**

### 24.3 不共用代码情况下的三种架构选项

针对类别 C/D（真正需要传播的），有三种架构选择：

#### 选项 A：手工镜像 + Shadow 对账测试（成本低、风险靠测试控制）

机制：
- QE 端改完，开发者手工把同样语义在 Paper v2 端再写一遍
- Shadow 对账测试（§11.3 / §17.5）每晚跑：同 manifest 同日，QE backtest vs Paper v2 重放，差异 > 阈值告警
- 差异告警 → 人介入查找哪一侧没同步

优点：
- 不需要架构重构
- 各端代码留在自己的执行栈中（QE 在 Qlib YAML，Paper 在 plain Python / vn.py）
- 测试是"事后保护"，不阻塞当前开发节奏

缺点：
- **每次类别 C 修改需要双工**（QE + Paper 各改一次）
- 漏改的风险靠测试发现，但测试覆盖度不全时漏改会进生产
- 测试 flakiness 时容易"豁免"差异，长期会麻木

工作量：
- shadow 对账基础设施：2-3 周（已规划在 §15.6 产品化阶段）
- 每次类别 C 修改 +50-100% 时间（双工 + 验证）

#### 选项 B：抽出"策略 policy 模块"（中等成本，长期一致性最强）

机制：
- 把类别 C 的语义抽到一个 plain Python 模块 `backend/services/strategy_policy/`
- 该模块定义 `StrategyPolicy` 抽象类：`decide_targets(scores, positions, params) -> Targets`
- 没有任何 Qlib / vn.py 依赖（纯 dict / list / numpy）
- QE 端写 `qe_strategies/qlib_policy_adapter.py`：继承 Qlib `BaseSignalStrategy`，内部 delegate 到 `StrategyPolicy`
- Paper v2 端：trading_core 直接调用 `StrategyPolicy`
- 一处修改自动传播

优点：
- **类别 C 修改自动同步**——一处改两边生效
- 长期一致性最强
- 这是 §15.5 中 `backend/rebalance_strategies/` 当初想做但没做完的方向

缺点：
- 需要前期重构（拆现有 `qe_strategies/topk_dropout_rc_qlib.py` 和 Paper v2 `runtime.py:551-679`）
- 拆得不干净时反而引入耦合 bug
- 类别 D（schema 修改）仍需双侧手工——policy 模块只解决执行语义，不解决契约升级

工作量：
- 一次性重构：4-6 周（§12 务实复用 AIstock 策略库的核心工作）
- 每次类别 C 修改：1x（只改 policy 模块）

#### 选项 C：契约驱动的"blueprint"模式（最严谨、成本最高）

机制：
- QE 产出 manifest 时附带完整 `execution_blueprint` 对象（描述执行所需的所有参数 + 决策树 + 函数引用）
- blueprint schema 强类型 + 严格版本（manifest_schema_v1, v2, ...）
- Paper v2 有 `BlueprintExecutor`：根据 blueprint 版本路由到对应执行器
- QE 改语义 → blueprint 增字段 / 升版本 → Paper v2 必须支持新版本（CI 强制）
- 每个 schema 版本都有 equivalence test：blueprint 在 QE 跑一次，Paper 跑一次，输出相同

优点：
- 契约最显式
- 漏同步会被 schema CI 立即抓出来（不能模糊"也许 paper 端用旧版本就行了"）
- 类别 C + D 都覆盖

缺点：
- 工作量最大（schema 设计 + 双侧执行器 + equivalence 框架）
- 需要 manifest schema 重大重构
- 短期内严重拖慢迭代速度
- 这是 §11 完整 IR 路径的核心要素，工作量季度级

工作量：
- 基础设施：8-12 周
- 每次新功能：1-2 周（schema 设计 + 双端执行器）

### 24.4 选项对比表

| 选项 | 一次性成本 | 类别 C 单次同步成本 | 漏同步风险 | 适合阶段 |
| --- | --- | --- | --- | --- |
| A：手工镜像 + Shadow 对账 | 2-3 周（基础设施） | 每次 +50-100% 时间 | 中（靠测试发现） | **MVP 期 + 早期产品化** |
| B：策略 policy 模块 | 4-6 周（一次性重构） | 1x（不增加） | 低（一处改两边生效） | **中期产品化** |
| C：Blueprint + schema | 8-12 周 | 1-2 周/特性 | 极低 | **大规模演进期** |

### 24.5 与 §22 / §15 优先级的整合

按 §22 的 Tier 划分，推荐分阶段实施：

#### Tier 0（Week 1-2，#1 seed）—— 不需要任何特殊机制

seed 属于类别 A（训练期修改），自动通过模型权重传播。Paper v2 端可能需要的小补丁（tie 处理 setseed）属于一次性工作，1-2 天搞定。

#### Tier 1（Week 3-14，目标 A + 目标 B 主干）—— 选项 A（最低成本）

- vn.py + miniQMT MVP 不引入 policy 抽象重构
- §15.6 的 shadow 对账基础设施作为 Tier 3（产品化）启动
- 类别 C 修改在此期间靠人手工双侧改 + 在 Tier 1 末段建立 equivalence smoke test（小规模）

**关键约定**：用户 + Codex + Claude Code 在每次类别 C 修改时**显式声明"需要传播"**，并在两个地方都改 + 提对账测试用例。这条约定写入 `feedback_aistock_codex_alignment.md`。

#### Tier 2（Week 6-12 穿插）—— 启动选项 A 的 shadow 对账基础设施

- 部分 §15.6 工作前移：建立"同 manifest 同日 QE+Paper 重放"对账测试框架
- 不要求 100% 覆盖，覆盖到 trading_core / paper_v2 / runtime.py 持仓决策一致性即可
- 工作量 2-3 周，归 Codex 或双方协作

#### Tier 3（Month 4-6 产品化）—— 评估是否升级到选项 B

- 当 Tier 1B 演进框架稳定 + 类别 C 修改频率开始上升时，开始评估选项 B
- 触发条件：
  - 单月发生 >2 次类别 C 修改
  - shadow 对账多次因双工失败发出告警
  - 演进 loop 之间需要快速 A/B 比较（手工双工跟不上节奏）
- 工作量 4-6 周，归 Codex 维护范围（QE 配置层 + Paper v2 共享）

#### 长期（Month 6+）—— 选项 C 仅在演进规模持续扩大时考虑

- 如果一年内类别 C/D 修改超过 30 次、跨多个 alpha、跨多个执行算法，再考虑 blueprint 重构
- 否则维持选项 B 即可

### 24.6 常见误区与硬约束

#### 24.6.1 "QE 改完，跑一遍回测看看对就行了"——不对

QE 回测使用历史数据 + Qlib 撮合；Paper v2 使用历史/实盘数据 + 自有撮合（或 vn.py SimGateway）。**两边数据流和撮合逻辑都不同**——回测对了不代表 Paper 也对。必须双侧分别测。

#### 24.6.2 "都用 manifest 当契约就好"——不够

manifest 只能传**数据**，不能传**逻辑**。如果 QE 端策略类多了一个 `if score > threshold: skip` 的代码分支，manifest 里没有字段表达这个，Paper v2 不会自动获得这个分支。

#### 24.6.3 "shadow 对账如果通过就万事大吉"——不对

shadow 对账只测**已经发生过的历史日期**。如果差异表现只在特定市场状态（涨停潮 / 流动性危机 / 新规则生效），历史数据可能没覆盖到。**shadow 是 last line of defense，不是首要保障**。

#### 24.6.4 "手工双工太麻烦，让 AI 自动同步"——危险

让 Claude Code / Codex 跨模块自动镜像代码听起来美好，但：
- 镜像逻辑可能"看起来对"实际语义偏差（最难抓的 bug）
- agent 不理解金融语义边界（什么是 PIT、什么是 frozen、什么场景必须 fail-fast）
- 自动同步出问题时责任无法归属

**自动同步必须配合 equivalence test 强 gate** —— 没有强 gate 的自动同步等于把 bug 放大。

### 24.7 短期可立即做的硬约束（无成本，立即部署）

不论走选项 A/B/C，下面四条约定立即落实，0 工作量但消除大量风险：

1. **类别 C 修改必须显式声明"需要传播"**：用户在分配任务时，如果这个修改属于类别 C，明确说"QE 端 + Paper 端都要改"。Codex 和 Claude Code 在 commit message / PR description 中标注 `[CROSS-STACK]`。
2. **类别 C PR 必须含双侧修改 + 至少一个 equivalence smoke test**：哪怕 smoke test 只是一个最小 case（同 manifest 同日跑一份），也比无测试好。
3. **Manifest schema 升级走双 PR 模式**：先在 QE 端定义 schema v2 但不启用；Paper v2 端添加 v2 reader（兼容 v1）；两边都合后再切 QE 端默认产出 v2。这样不会出现"QE 产出 Paper 读不了"的窗口期。
4. **类别 A/B/D 修改归 Codex 维护范围声明**：用户在分配 Codex 任务时明确"这是类别 X 修改"，让 Codex 知道是否需要通知 Claude Code 跟进。

这四条写入 `feedback_aistock_codex_alignment.md` 第 14 条作为长期约束。

### 24.8 一句话结论

**"QE 修改如何同步到 Paper v2"不是单一机制问题，是按修改类别分类的传播策略**：

- **类别 A（训练期）**：自动同步，包括 seed 修复——通过模型权重和 manifest 传播，**95% 无需代码改动**
- **类别 B（因子层）**：已共享代码，自动同步
- **类别 C（执行语义）**：当前唯一需要双工的类别，**短期靠 §24.7 四条约定 + Tier 2 末 shadow 对账兜底；中期通过选项 B 抽 policy 模块根治**
- **类别 D（契约层）**：通过 manifest schema 显式版本化 + 双 PR 模式管理

**短期不需要重构架构**——选项 A（手工镜像 + shadow 对账）+ §24.7 四条约定足够支撑 Tier 0/1。**中期产品化时（Month 4-6）评估升级到选项 B**——把类别 C 的同步成本从"双工"降到"一处"。**选项 C blueprint 模式可能永远不必做**，除非演进规模超过 30+ 类别 C/D 修改/年。

**关键的硬约束**：类别 C 修改必须显式声明 + 双 PR + smoke test——这条做对了，研究-生产 gap 长期可控；做不对就会出现"回测可用、实盘失效"的事故。这是金融 IT 的硬伤，不是工具能完全替代的。

---

## 25. QE 配置层 + 执行层与 Paper v2 共用 —— "选项 B+"详细方案

回答用户在 2026-05-08 的关键架构追问：

> QE 的组合配置层和执行层是否能与 Paper v2 共用？执行层 Paper v2 使用不同的适配器切换到实盘。今后 QE 使用多 alpha 架构时，Paper v2 可以直接支持，不需要代码的同步。

**这是正确的架构目标**。本节给出比 §24 选项 B 更进一步的"选项 B+"——不仅共用执行层 policy，还共用配置层 spec——以及具体落地路径。

### 25.1 用户的目标架构

```
              ┌──────────────────────────────────────────┐
              │ StrategySpec (Pydantic 数据模型)          │
              │ - 因子组合 (alpha_components)            │
              │ - 模型定义 (model_recipe)                 │
              │ - 组合策略 (combination_rule)            │
              │ - 持仓策略 (portfolio_policy)            │
              │ - 风险策略 (risk_policy)                 │
              │ - 执行意图 (execution_intent)            │
              │ - 多 alpha 配置 (alpha_combination)      │
              └──────────────────────────────────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              ▼                 ▼                  ▼
    ┌──────────────────┐ ┌─────────────────┐ ┌──────────────────┐
    │ Strategy Engine  │ │ Strategy Engine │ │ Strategy Engine  │
    │ (shared, 共享)   │ │ (shared, 共享)  │ │ (shared, 共享)   │
    │                  │ │                 │ │                  │
    │ score→targets    │ │ score→targets   │ │ score→targets    │
    │ targets→orders   │ │ targets→orders  │ │ targets→orders   │
    └──────────────────┘ └─────────────────┘ └──────────────────┘
              ▼                 ▼                  ▼
    ┌──────────────────┐ ┌─────────────────┐ ┌──────────────────┐
    │ QE Adapter       │ │ Paper Adapter   │ │ Live Adapter     │
    │ - Qlib YAML 生成 │ │ - inference     │ │ - inference      │
    │ - WSL 提交        │ │ - SimGateway    │ │ - vnpy_xt        │
    │ - 历史撮合        │ │ - 模拟撮合       │ │ - 实盘 broker    │
    └──────────────────┘ └─────────────────┘ └──────────────────┘
```

**关键属性**：
- StrategySpec + Strategy Engine 是单一来源
- 三个 adapter 各 100-300 行薄壳
- 新增 alpha 类型 / 新组合方式 / 新策略子类——**改一处，三个 adapter 自动支持**
- 这正是 §11 IR 的精神，但范围收窄到"配置 + 执行"，不强求 IR 覆盖一切

### 25.2 为什么这是正确目标，但不是 Tier 0

**正确目标的依据**：
- 解决 §24 类别 C 的双工成本：从"每次类别 C 改动需要 +50-100% 时间"降到"1x"
- 多 alpha / 公告信号 / 滚动训练等未来工作 **天然可扩展**——只是新增 spec 字段和 engine 分支
- 与用户目标 B "持续演进 + 多 alpha"完全对齐
- 与 §15 vn.py 路径不冲突——选项 B+ 在 OEMS 之上工作；vn.py 仍是 OEMS 层

**为什么不是 Tier 0**：
- 工作量大（9-15 周一次性投入，详见 §25.4）
- Tier 0 (seed) 和 Tier 1A (vn.py MVP) 的成果都不依赖这个重构
- 急于在 MVP 之前做会拖慢"看到模拟盘跑起来"的节奏

**关键时机判断**：
- **Tier 1B 内、做 #7 多 alpha 之前必须做**——否则多 alpha 会同时落到 QE Qlib 和 Paper v2 两套实现，把双工成本永久化
- **早于多 alpha 1 个月开始**最优——给重构时间，又不耽误后续多 alpha 工作

### 25.3 落地分解：6 个工作流（按依赖排序）

| # | 工作流 | 内容 | 工作量 | 归属 |
| --- | --- | --- | --- | --- |
| 1 | **StrategySpec Pydantic 模型设计** | 设计完整的 spec schema（含多 alpha / 事件信号扩展点）；写文档 + 单测；schema 版本 v1 | 1-2 周 | 双方协作设计、用户拍板 |
| 2 | **Strategy Engine 核心实现**（plain Python，无 Qlib / vn.py 依赖） | 实现 score→targets / targets→orders 的核心逻辑；覆盖现有 Paper v2 `runtime.py:551-679` 的所有算法（topk、动态 n_drop、score 加权权重、hold_thresh）；多 alpha 组合点预留接口 | 2-3 周 | Codex（属其因子/策略维护范围） |
| 3 | **QE Adapter 实现** | `qe_strategies/qlib_adapter.py`：把 StrategySpec 转 Qlib YAML；继承 BaseSignalStrategy 时内部 delegate 到 Strategy Engine（非纯研究行为部分）；保留 Qlib 撮合 | 2-3 周 | Codex |
| 4 | **Paper v2 / Live Adapter 实现** | trading_core 调用 Strategy Engine 生成 orders；把 orders 翻译为 vn.py OrderRequest；保留 §15 vn.py 集成 | 1-2 周 | Claude Code（Paper v2 模块） |
| 5 | **现有代码迁移** | 把 `runtime.py:551-679` 算法搬进 Strategy Engine；把 `qe_strategies/topk_dropout_rc_qlib.py` 改造为 adapter；现有 manifest 加映射到 StrategySpec v1（兼容老 manifest） | 2-3 周 | 双方协作 |
| 6 | **Equivalence 测试矩阵** | 同 manifest 同日：QE adapter 跑一次 + Paper adapter 跑一次，比对 NAV/持仓/换手；建立至少 5 个 baseline 场景；接入测试流水线作为 cross-stack 强 gate | 1-2 周 | 双方协作 |

**总工作量**：9-15 周（约 2-4 个月）

**可并行性**：#1 完成后，#2/#3/#4 可三路并行；#5 依赖 #2-#4 完成；#6 依赖 #5 完成。所以日历压缩后**最快 6-8 周**（多窗口/Agent Teams + Codex 并行）。

### 25.4 ROI：何时开始最优

#### 25.4.1 不做的成本（按 §22 时间表）

| 阶段 | 不做选项 B+ 的累计代价 |
| --- | --- |
| Tier 1B 多 alpha (#7) | 多 alpha 同时落到 QE 和 Paper v2 = 双工 +50-100%（按 §24 类别 C 估算）= 2-4 周额外工作；以后每次多 alpha 改动都重复 |
| Tier 2 HMM 优化 (#2) | HMM 已在 selection_center 共享（§13.1.4），影响小 |
| Tier 3 公告/财报信号 (#5) | 新信号类型同时实现两份 = 双工 +50-100% = 2-3 周额外工作 |
| 滚动训练专项 (§19.3) | 若 manifest schema v2 + recipe 化与 StrategySpec 整合，节省 1-2 周；否则要做两次 schema 升级 |
| 实盘 adapter（产品化阶段） | 第三个 adapter 实现 = 又一次完整重写策略层 = 3-4 周额外工作 |

**累计代价**：6 个月内**至少 8-12 周**重复劳动 + 长期类别 C 同步压力。

#### 25.4.2 做选项 B+ 的成本

- 一次性 9-15 周（可压缩到 6-8 周日历）
- **早做收益更高**：每多一个未做选项 B+ 时落地的功能，迁移成本就增加

#### 25.4.3 净 ROI 判断

| 启动时机 | 净收益 | 风险 |
| --- | --- | --- |
| Tier 0 / 1A 早期（Month 1） | 推迟 MVP，收益要 2-3 个月才显现 | **不推荐**——抢占 Tier 0/1A 资源 |
| **Tier 1B 早期（Month 2 末）**——多 alpha 之前 | 一次投入避免后续多 alpha + 公告信号 + 滚动训练 + 实盘的所有重复劳动 | **推荐**——时机最优 |
| Tier 1B 末或 Tier 2（Month 3+） | 多 alpha 已落入两套，迁移成本高 | 不推荐（晚了） |
| 产品化阶段后期 | 全部双工成本已付出，收益主要在长期维护 | 仅作为"还债" |

**最优启动时机**：Month 2 末，与 Tier 1B 多 alpha 工作开始同步——但**先做 StrategySpec 设计 + Strategy Engine**（工作流 #1-#2），多 alpha (#7) 推迟 4-6 周等基础设施就绪后再做。

### 25.5 与 §15 vn.py 路径的兼容性确认

**完全兼容**。§15 选项 C 在 OEMS 层用 vn.py 库；选项 B+ 在 strategy 层用共享 Engine。两者堆叠：

```
Strategy 决策层（选项 B+，共享）
              ▼
        生成 OrderIntent
              ▼
OEMS 执行层（§15 选项 C，vn.py 库）
              ▼
       broker（vnpy_xt → miniQMT）
```

**Paper v2 day_runner 改造后的样子**：

```
load StrategySpec from manifest
  → call StrategyEngine.decide_targets(scores, current_positions)
  → call StrategyEngine.targets_to_orders(targets, current_positions, params)
  → submit OrderIntent list to trading_core RPC
        → trading_core wraps as vn.py OrderRequest
        → vnpy_xt sends to miniQMT
        → fills come back via vn.py event
        → trading_core publishes events
        → Paper v2 records to daemon_event_log
```

整个链路里 strategy 层（共享） 和 OEMS 层（vn.py） 是清晰解耦的，互相不依赖。

### 25.6 多 alpha + 公告信号场景下的"自动同步"演示

这是用户提案的核心价值——具体描绘下：

#### 25.6.1 多 alpha 接入

未来 Codex 实现多 alpha 时：

1. 在 StrategySpec 里加：
   ```python
   class StrategySpec:
       alpha_components: list[AlphaComponent]  # 已存在
       combination_rule: CombinationRule       # 新加：weighted_sum / rank_aggregation / meta_learner
   ```
2. 在 Strategy Engine 里加 CombinationRule 处理：
   ```python
   def combine_alphas(component_scores, rule):
       if rule.type == "weighted_sum": ...
       if rule.type == "rank_aggregation": ...
       if rule.type == "meta_learner": ...
   ```
3. 三个 adapter 不变——QE adapter / Paper adapter / Live adapter 都通过 Strategy Engine 自动获得多 alpha 能力
4. 改动行数：~200-400 行，**仅在共享层**

如果不做选项 B+，这同样的多 alpha 改动需要在 QE Qlib 端 + Paper v2 runtime.py 端 + Live adapter 端各实现一次，**约 600-1200 行 + 三套 equivalence 测试**。

#### 25.6.2 公告信号 (#5) 接入

类似机制：

1. StrategySpec 的 `alpha_components` 列表中接受新类型 `EventSignalComponent`
2. Strategy Engine 在 `compute_scores()` 阶段把事件信号合入分数计算
3. 三个 adapter 自动支持

#### 25.6.3 滚动训练 (§19.3) 接入

manifest schema v2（recipe + 当前权重）天然落地为 StrategySpec 的两个字段：
1. `model_recipe`（描述"如何训练"）
2. `model_asset_pointer`（指向当前权重）

trainer scheduler 周期性产出新 model_asset → manifest pointer 更新 → 三个 adapter 都自动用上新权重。

### 25.7 风险与硬约束

#### 25.7.1 风险：StrategySpec 设计不全 → 后期再加字段成本递增

应对：
- #1 工作流（spec 设计）必须包含**未来 6-12 个月所有已知扩展场景**评审：多 alpha / 公告信号 / 滚动训练 / 多种执行算法
- 留 escape hatch：`raw_extension: dict[str, Any]` 字段允许暂时塞进新字段而不立刻升级 schema
- 每个 schema 升级 (v1 → v2) 走双 PR 模式（参见 §24.7 第 3 条）

#### 25.7.2 风险：Strategy Engine 与 Qlib 撮合行为不一致

应对：
- Strategy Engine 只覆盖"决策层"（score → targets → orders）；撮合层留给 Qlib（QE adapter）和 vn.py SimGateway / 实盘 broker（Paper / Live adapter）
- equivalence 测试只比对决策层输出，不要求撮合层完全相同（撮合差异由 §11.3 shadow run 兜底）

#### 25.7.3 风险：QE adapter 改造影响 QE 现有稳定性（数百次回测验证）

应对：
- QE adapter 改造期间，**保留旧路径并行**：旧 manifest 走旧路径，新 StrategySpec manifest 走新 adapter
- 至少跑 50+ 次回测对比新旧路径输出，差异 < 0.1bp 才切默认
- 切默认后旧路径作为降级选项保留 3-6 个月

#### 25.7.4 风险：双方 agent 对 StrategySpec 字段定义理解不一致

应对：
- 字段定义文档极致显式：每个字段写"语义描述、合法值范围、与其他字段的依赖、升级路径"
- 每次类别 D 修改（schema 升级）走双 PR + 双侧 reader + equivalence test
- 用户在分配任务时明确"这次 PR 是修改 spec 还是 engine 还是 adapter"

### 25.8 推荐实施节奏（更新 §22 优先级）

把选项 B+ 整合进 §22 时间表：

```
Month 1                     Month 2          Month 3-4              Month 5-6
├─ Week 1-2 ──┐
│  Tier 0 #1 seed │
│  (Codex)         │
└──────────────────┘
                 ├─ Week 3-7 ──────────────┐
                 │ Tier 1A: vn.py MVP (Claude Code) │
                 └──────────────────────────────────┘
                 ├─ Week 3-6 ──────────┐
                 │ Tier 1B 启动:          │
                 │ #1 StrategySpec 设计   │
                 │ #2 Strategy Engine     │  ← 选项 B+ 工作流 1-2
                 │ (Codex 主导)            │
                 └────────────────────────┘
                                  ├─ Week 7-10 ──────────────┐
                                  │ #3-#5 三 adapter 实现      │
                                  │ (Codex + Claude Code 并行) │
                                  │ #6 equivalence 测试        │
                                  └────────────────────────────┘
                                                      ├─ Week 11-15 ──┐
                                                      │ #7 多 alpha     │
                                                      │ #5 公告信号 (排) │
                                                      │ → 自动支持三端    │
                                                      └─────────────────┘
                                                                       ├─ Tier 3 产品化 →
```

**关键节点变化**：
- 多 alpha (#7) 从 Tier 1B Week 3-14 推迟到 Week 11-15（重新排到选项 B+ 之后）
- 收益：以后所有新功能默认三端支持，不再有类别 C 双工

**总日历**：仍是 6 个月，但**质量上从"赶节奏快交付"变成"打基础慢交付"**——前 3 个月看似慢，后 3 个月加速。长期累积价值远高于短期完成数量。

### 25.9 一句话结论

**用户提的"QE 配置层 + 执行层共用，Paper v2 用不同 adapter 切换实盘，多 alpha 自动同步"是正确的架构目标，且与 §15 vn.py 路径完全兼容**——选项 B+ 在 strategy 层共享，vn.py 在 OEMS 层服务，两层堆叠不冲突。

**最优启动时机**：Month 2 末（Tier 1B 早期，多 alpha 工作之前）。**早做收益最高，晚做成本递增**——多 alpha 一旦双轨落地，重新合并的成本是单次实施的 2-3 倍。

**总投入**：9-15 周可压缩到日历 6-8 周（含并行）；**避免后续累计 8-12 周双工劳动 + 长期类别 C 同步压力**——净 ROI 极高。

**§22 优先级修订**：建议把 #7 多 alpha 推迟到 Week 11-15，先在 Week 3-10 完成选项 B+ 基础设施。这样多 alpha + 公告信号 + 滚动训练 + 实盘 adapter 都自动落到三端，**真正实现"QE 修改不需要代码同步到 Paper v2"的架构愿景**。

**唯一硬约束**：StrategySpec 一次设计，未来扩展场景必须前置评审；每次 schema 升级走双 PR + equivalence test 强 gate（与 §24.7 四条约定一脉相承）。这条做对了，长期一致性可保；做不对，又会回到三套实现各漂移的局面。

---

## 26. §25 方案的真实边界 —— "一套架构、自动同步、不维护两套系统"是否成立？

回答用户在 2026-05-08 的精确追问：

> 你建议的方案是否为 QE 和模拟盘使用同一套架构为目标，未来 QE 的所有修改都可以直接同步到模拟盘，不需要维护两套系统？

**答案分三档，必须诚实区分**：

| 用户预期 | §25 实际兑现 |
| --- | --- |
| "同一套架构" | **是**（在策略/研究层面） |
| "QE 所有修改自动同步" | **大部分是**（≈95%），少数例外（≈5%）必须本地修改才正确 |
| "不维护两套系统" | **不完全**——本质上是"一个共享核心 + 三个薄壳 adapter"。比当前的三套独立栈大幅改善，但不是零适配代码 |

下面分别说明这三档的边界。

### 26.1 "同一套架构"——是

§25 之后的架构状态：

| 层 | 数量 | 性质 |
| --- | ---: | --- |
| StrategySpec（配置数据模型） | **1** | 单一来源，所有 spec 字段在此定义 |
| Strategy Engine（策略决策代码） | **1** | 单一来源，所有持仓/订单决策逻辑在此 |
| 因子计算 / 模型推理 | **1**（已共享） | inference_engine.py + strategy_package_live_inference.py |
| 风险策略 / tradability | **1** | selection_center 已共享，§25 后纳入 Engine |
| 撮合 adapter | **3** | QE Qlib / Paper SimGateway / Live vnpy_xt——必须不同（环境约束） |
| 行情订阅 adapter | **2** | DB 历史 / vnpy_xt 实时——必须不同（数据源约束） |

策略和研究层是 1 套，环境适配层是 3 套薄壳——**这是工程现实下"同一套架构"的最干净形态**。

### 26.2 "所有修改自动同步"——95% 是，5% 例外

#### 26.2.1 自动同步的修改（≈95%，按 §24 类别对照）

| 类别 | 例子 | §25 后是否自动同步 |
| --- | --- | --- |
| 类别 A（训练期） | seed / 训练超参 / 损失函数 / 数据切分窗口 | **自动**——通过模型权重 |
| 类别 B（因子层） | 新因子定义 / 因子计算逻辑 / 因子缓存 | **自动**——已共享代码 |
| 类别 C（执行语义） | **多 alpha 组合规则** / 新策略子类 / 风险策略语义 / 持仓权重算法 / 动态 n_drop / hold_thresh / **公告/财报信号合成** | **自动**——改 Strategy Engine 一处，三端生效 |
| 类别 D 内 spec 字段扩展 | 新字段（如 `master_seed`、新 alpha 类型） | **自动**——adapters 通过 spec 读取 |

**这是 §25 的核心价值**——用户最关心的"演进时大量类别 C/D 修改",**真正实现一处改、三端生效**。

#### 26.2.2 必须本地修改的"5% 例外"——而且**这 5% 不应该自动同步**

下列变化天然属于 adapter 内部、不需要也不应该传播：

| 变化类型 | 例子 | 影响范围 | 为什么不应该传播 |
| --- | --- | --- | --- |
| Qlib 框架升级 | Qlib 4.x → 5.x，data_handler API 变化 | QE adapter only | Paper 不用 Qlib，传播过去无意义 |
| vn.py 框架升级 | vn.py 4.x → 5.x，OmsEngine 接口变化 | Paper / Live adapter only | QE 不用 vn.py |
| miniQMT 协议变化 | xtquant 接口字段调整 | Live adapter only | QE 和 Paper 不用 miniQMT |
| 撮合细节差异 | Qlib 的 deal_price 计算 vs vn.py 的实时 fill 推送 | 各 adapter 内部 | 撮合本来就因环境不同而不同 |
| 性能优化 | Qlib 批量化 vs vn.py 事件驱动 | 各 adapter 内部 | 性能特性不同是正确的 |
| 错误处理 | broker reject / partial fill 处理 | Live adapter only | QE 没有真实 broker，不存在该概念 |

**这 5% 的"不同"是好事**——保持 adapter 各自最优地与所在环境集成。**强行统一反而会让所有 adapter 都跑得不好**（Lean / Zipline 历史教训，参见 §11.3）。

### 26.3 "不维护两套系统"——更准确说是"维护一个核心 + 三个薄壳"

#### 26.3.1 当前 vs §25 后的对比

| 项目 | 当前（2026-05-08） | §25 后 |
| --- | --- | --- |
| 策略决策代码 | **3 套独立**（QE Qlib / Paper runtime.py / Selection Center 各一份风险/tradability） | **1 套共享 Engine** |
| 配置 schema | manifest 单源但被绕过（runtime.py 自己重写算法）| StrategySpec 单源，所有 adapter 强制读 |
| 撮合代码 | 3 套（Qlib / Paper MinuteExecution / 未来 Live） | 3 套（Qlib / vn.py SimGateway / vnpy_xt） ——**仍然 3 套但都是成熟外部库** |
| 因子推理 | 已共享 | 仍共享 |
| 风险/tradability | 2 套并行（selection vs Paper day_runner） | 1 套（在 Engine 内） |
| 总维护单元 | 7-8 个独立逻辑点 | **3 个：Engine + spec schema + 3 个薄 adapter** |

#### 26.3.2 三个薄 adapter 的真实工作量

| Adapter | 预估行数 | 维护频度 |
| --- | --- | --- |
| QE Adapter（Qlib YAML 生成 + 撮合 delegate） | 200-400 | 仅在 Qlib 升级或 spec schema 升级时改 |
| Paper Adapter（trading_core RPC + spec 读取） | 100-200 | 仅在 vn.py 升级或 spec schema 升级时改 |
| Live Adapter（同上但接 vnpy_xt） | 100-200 | 仅在 vnpy_xt 升级或 spec schema 升级时改 |

**总 adapter 代码量 < 1000 行**，对比 Strategy Engine（共享，2000-3000 行）+ 因子推理（共享）+ StrategySpec schema（共享），**adapter 占总代码量约 15-20%**。

**这 15-20% 是必要代价**，无法避免——除非选择把 QE 也跑在 vn.py 上（推翻 QE 数百次回测验证基础，§15 / §11.3 已否决）。

### 26.4 真实期望设定（必须接受）

| 用户原话 | 严格事实 | 实际体验 |
| --- | --- | --- |
| "同一套架构" | 决策/研究层 1 套；环境层 3 套薄壳 | **大部分时候像一套** |
| "QE 所有修改自动同步" | 类别 A/B/C 自动；类别 D（schema）半自动；环境层不传播且不应传播 | **95% 自动，5% 例外是合理的** |
| "不维护两套系统" | 维护 1 个核心 + 3 个薄壳，对比当前 7-8 个独立逻辑点是大幅简化 | **从"维护三个完整栈"变成"维护一个核心 + 三个轻量适配"** |

**唯一不诚实的承诺是"零 adapter"**——只要 QE 跑 Qlib + Paper 跑 vn.py + Live 跑 vnpy_xt，三个 adapter 不可避免。但相比当前 7-8 个独立逻辑点漂移，这是巨大改善。

### 26.5 多 alpha + 演进 + 实盘 切换的具体演示

用户最关心的"多 alpha 自动同步"+"切换实盘不写新代码"，§25 后的具体表现：

#### 26.5.1 Codex 实现多 alpha

```
1. Codex 在 StrategySpec 加 CombinationRule 类型: "weighted_sum" / "rank_aggregation" / "meta_learner"
2. Codex 在 Strategy Engine 加 combine_alphas() 函数处理新类型
3. PR 合并

→ QE Adapter 不变（自动通过 Engine 用上）
→ Paper Adapter 不变（自动）
→ Live Adapter 不变（自动）
→ Equivalence test 验证三端输出一致
```

**Claude Code 不需要任何代码同步**——Codex 一个 PR 解决全部。

#### 26.5.2 切换 Paper v2 → 实盘

```
1. 同一份 manifest（含 StrategySpec）
2. Paper portfolio 的 trading_core 配置从 SimGateway 切到 vnpy_xt
3. 启动

→ Strategy Engine 不变（同一份代码跑）
→ StrategySpec 不变（同一份配置）
→ 唯一变化：trading_core 内部 broker adapter 从 SimGateway → vnpy_xt
```

**没有 Paper v2 业务代码改动——切实盘是配置项变更，不是代码变更**。

#### 26.5.3 公告/财报信号接入

```
1. Codex 在 StrategySpec 增加 EventSignalComponent 类型
2. Codex 在 Strategy Engine 的 compute_scores() 加事件信号合成分支
3. PR 合并

→ 三个 adapter 自动支持
→ Selection Center 自动支持（也用 Engine）
→ 实盘自动支持（也用 Engine）
```

**新信号类型 = 一处改、五端生效**（QE 回测 + Selection + Paper + Live + 任何未来端）。

### 26.6 一句话结论

**§25 方案是"一套核心 + 三个薄壳 adapter"——比"完全同一套"略弱，比"维护多套独立系统"大幅改善**。

**用户的"QE 所有修改自动同步"诉求 95% 兑现**，剩 5% 是环境特定变化（Qlib/vn.py/miniQMT 升级），不应该传播也不需要传播。

**用户的"不维护两套系统"诉求大部分兑现**——从当前 7-8 个独立逻辑点漂移降到 1 个共享 Engine + 3 个 < 300 行的薄 adapter。**adapter 不是 bug，是必要代价**——因为 Qlib / vn.py / miniQMT 是不同的执行环境，必须各自最优集成。

**用户最关心的"多 alpha 自动同步"和"切换实盘不写新代码"，§25 后两者都成立**——多 alpha 是 spec + Engine 改动一次，自动生效到三端；切实盘是配置项变更（SimGateway → vnpy_xt），不是代码变更。

**前提是把 §25 真做对**——StrategySpec 设计要前置评审 6-12 个月扩展场景，Strategy Engine 实现要严格按 spec 走、不允许 adapter 越权直接实现策略逻辑，每次 schema 升级双 PR + equivalence test 强 gate。这三条做对了，长期"一套架构、自动同步"就成立；做不对，又会回到三套实现漂移的旧状态。

---

## 27. 整体依赖关系 + "立即统一" vs "先完善 QE" 的取舍

回答用户在 2026-05-08 的最关键问题：

> 目前所有计划中的依赖关系是怎样的？是需要现在就实现 QE 实验与模拟盘、实盘的架构统一，还是先完善 QE 的架构，再开始统一？

**直接结论：不是二选一**。大部分工作可并行；架构统一 (§25) 与 QE 完善 (#6 演进) 不互斥；但**有一个关键临界点必须把握住**：**多 alpha (#7) 落地之前，架构统一必须就绪**——否则双工成本永久化。

### 27.1 完整依赖图（按"必要前置"和"自然时序"分类）

```
┌─────────────────────────────────────────────────────┐
│ #1 模型 seed (Month 1)                              │
│ Tier 0 — 所有后续工作的最小前置                       │
└─────────────────────────────────────────────────────┘
            │ 必要前置
            ├──────────────────────────────────┐
            ▼                                  ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│ Tier 1A: vn.py MVP       │    │ Tier 1B 启动:             │
│ (Month 2-3, Claude Code) │    │ §25 工作流#1 StrategySpec │
│ 用现有 frozen manifest   │    │ 设计 (Month 2)            │
│ 不依赖 §25                │    │ + #6 对比框架基础         │
│                          │    │ (Month 2 起, Codex 主导)  │
└─────────────────────────┘    └─────────────────────────┘
       │                              │
       │ 弱依赖                       │ 必要前置
       │ (MVP 完成后接入 Engine)      │
       ▼                              ▼
┌──────────────────────────────────────────────────────┐
│ §25 工作流 #2-#6 (Month 2-4)                         │
│ Strategy Engine + 三 adapter + 现有代码迁移 + tests   │
│ 关键里程碑：选项 B+ 完成                              │
└──────────────────────────────────────────────────────┘
            │ 必要前置（关键临界点）
            ├──────────────────────────────────┐
            ▼                                  ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│ #7 多 alpha (Month 4-5) │    │ #6 自动演进 主体          │
│ → 自动落到三端           │    │ (Month 3-5, Codex)        │
│   (无 §25 则永久双工)    │    │ 演进 loop 优化 + 晋级 gate │
└─────────────────────────┘    └─────────────────────────┘
            │                              │
            └──────────┬───────────────────┘
                       ▼
        ┌──────────────────────────────────┐
        │ #5 公告/财报信号 (Month 5-6)     │
        │ 滚动训练专项 (Month 5-6)          │
        │ 实盘前 shadow run + 对账           │
        │ → 全部自动落到三端                 │
        └──────────────────────────────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ 实盘 (Month 6+)   │
              │ 配置切换非代码    │
              └──────────────────┘

[穿插，弱依赖] #2 HMM 优化 (Month 2-3 任意时机)
[穿插，弱依赖] #4 测试流水线增强 (Month 3-5，Tier 1A 末启动)
```

### 27.2 关键依赖判断（决定取舍的几条主线）

| 依赖关系 | 强度 | 含义 |
| --- | --- | --- |
| #1 seed → 所有其他 | **强** | seed 不固定时演进无法对比、对账无法做、可复现性测试无法建立 |
| §25 → #7 多 alpha | **强** | 多 alpha 一旦双轨落地，重新合并成本是单次实施 2-3 倍 |
| §25 → #5 公告信号 | **强** | 同上，新信号类型如果两套实现，永久双工 |
| §25 → 实盘 adapter | **强** | 没有 §25，实盘相当于第三套独立实现 3-4 周额外工作 |
| §25 → 滚动训练 schema v2 | **强** | manifest schema v2（recipe 化）天然属于 spec 设计一部分 |
| §16 vn.py MVP → §25 | **弱** | MVP 用现有 frozen manifest 跑，不需要 spec 完成 |
| §25 → #6 自动演进 | **弱** | 演进框架的对比基础设施（IC 比较、NAV 比较）可以独立做；只是后期演进选择新策略时需要 §25 的 spec |
| #2 HMM 优化 → 任何 | **弱** | HMM 修复在 selection_center 内部，与 §25 大体解耦 |
| #4 测试流水线 → 任何 | **弱** | 流水线增强不阻塞主线（参见 §23） |

### 27.3 为什么"先完善 QE 再统一"不对

直觉上"先把 QE 弄稳定再统一其它"听起来稳妥，但**实际不可行**：

1. **"QE 完善"是一个持续过程，不是一个里程碑**——QE 演进 (#6) 是长期任务（按 §22 估 4-6 周主体 + 持续迭代）。如果"等 QE 完善再统一"，统一永远不会启动。
2. **多 alpha (#7) 是 QE 完善的一部分**——多 alpha 落地之前必须统一，否则后续每次类别 C 修改双工。"先完善 QE" 实际上是"在双工状态下完善 QE"，长期成本极高。
3. **§25 spec 设计本身有助于澄清 QE 概念**——做 spec 设计时被迫把 QE 当前的"模型 + 因子 + 组合策略 + 风险策略"边界写清楚。这个梳理过程会**反向帮助 QE 完善**——很多 QE 现在隐含的耦合（参见 §13.1）会在 spec 设计时暴露。
4. **QE 现在已经"稳定"到足够支撑 spec 设计**——数百次回测验证、已有 manifest 体系、已有 backtest_contract 雏形。spec 设计不是从 0 抽象，是把现有 manifest + backtest_contract 重构成更严格的形态。

### 27.4 为什么"立即全面统一"也不对

直觉上"既然 §25 价值高，应该立即全力做"也不对：

1. **#1 seed 没修，无法做 equivalence test**——同 manifest 跑两次得不同结果，根本测不出 spec/engine 是否正确
2. **vn.py MVP (Tier 1A) 不依赖 §25**——可以并行启动；不应该把 vn.py MVP 推迟到 §25 完成（用户最关心的"模拟盘跑起来"会被推迟 2-3 个月）
3. **spec 设计需要时间评审**——§25.7 强调 spec 一次设计、未来扩展场景前置评审。仓促设计的 spec 会在多 alpha / 公告信号接入时反复改 schema，反而引入双 PR 流程的痛苦
4. **团队精力有上限**——同时全力推 #1 seed、§25 spec/engine、vn.py MVP、#6 演进 4 条线会失败。**必须并行 2-3 条主线，不能 4 条**

### 27.5 推荐的并行节奏（最终版本）

把 §22 / §25 的时间表合并为一张统一图，明确每个阶段同时跑哪几条线：

| 阶段 | 主线（必须做） | 副线（可以做） | 不开始 |
| --- | --- | --- | --- |
| **Month 1** | #1 seed (Codex) | 无 | §25, MVP, 多 alpha, 演进 |
| **Month 2** | Tier 1A vn.py MVP (Claude Code) + §25 工作流 #1 spec 设计 + #6 对比框架基础 (Codex) | #2 HMM 优化 (Codex 抽时间) | #7 多 alpha, #5 公告 |
| **Month 3** | Tier 1A 末段（MVP demo）+ §25 工作流 #2-#5（Engine + adapters + 迁移）(双方协作) | #4 流水线轻量增强（Tier 1A 末）+ #6 演进 loop 优化 | #7 多 alpha, #5 公告 |
| **Month 4** | §25 工作流 #6 equivalence 测试 + #7 多 alpha 开工（建立在 §25 完成的基础上）+ #6 晋级 gate | #5 公告信号设计阶段 | 滚动训练实施 |
| **Month 5** | #5 公告信号实施 + 滚动训练 schema/scheduler + 产品化前期 shadow run 基础设施 | #4 流水线进一步增强 | 实盘上线 |
| **Month 6** | 实盘前最后准备（shadow run 对账 + 风控 hook + 产品化收口） | 滚动训练晋级 gate | 真实下单（用户决定）|

**关键里程碑**：
- Month 1 末：seed 修复完成，所有其他工作的"地基"建好
- Month 3 末：vn.py 模拟盘 demo + §25 选项 B+ 完成 → 用户能看到"两个目标的雏形"
- Month 4 末：多 alpha 落地（自动支持三端）+ 演进框架可半自动化
- Month 6 末：可上实盘的状态（实盘是否真上线由用户决定）

### 27.6 唯一必须确认的关键决策

用户拍板这一条即可启动：

> **是否同意 Month 2 同时启动三条主线：vn.py MVP（Claude Code 主导）+ StrategySpec/Engine 设计（Codex 主导）+ #6 对比框架基础（Codex）？**

**如果同意**：按 §27.5 时间表推进，Month 6 内完成两个目标 + 接入实盘的状态
**如果否决某一条**：

- 否决 vn.py MVP → 推迟"看到模拟盘跑起来"2-3 个月，失去用户层面的可见进展
- 否决 §25 → 多 alpha (#7) 落地后双工成本永久化（每次类别 C 修改 +50-100% 时间）
- 否决 #6 → 目标 B "持续演进"无核心进展，6 个月后仍是手工演进

也就是说**三条都不能不做**——只能调节优先级和资源投入比例。

### 27.7 一句话结论

**"先完善 QE 再统一"不对**——QE 完善是持续过程，永远等不到"完善了"那天；多 alpha (#7) 落地前不统一会永久双工。

**"立即全面统一"也不对**——必须先修 #1 seed（否则 equivalence test 无基础）+ vn.py MVP 不依赖 §25 应该并行启动。

**正确节奏是"立即并行三条线"**：
- Month 1：#1 seed（独占）
- Month 2-3：vn.py MVP + §25 设计/实现 + #6 对比框架（三线并行）
- Month 4：§25 完成后 #7 多 alpha 开工（自动落三端）
- Month 5-6：#5 公告 + 滚动训练 + 实盘准备（全部自动支持三端）

**关键临界点**：**多 alpha (#7) 必须在 §25 完成后才开工**。这是整个计划中唯一的硬时序约束——其它工作之间都是软时序。

**唯一需要用户拍板的决策**：是否在 Month 2 同时启动三条主线。如果同意，按 §27.5 表推进；如果否决任一条，下方有相应代价。

可以从 Codex 接 Tier 0 #1 seed 任务、用户与 Codex 启动 §25 工作流 #1 spec 设计评审会议开始落地。

---

## 28. 是否先完成"模型库 + QE 实验 + 数仓"重新规划再开始后续工作？

回答用户在 2026-05-08 的进一步追问：

> 目前 QE 实验需要重新规划模型库，包括模型库、QE 实验、和数仓的数据存储分类等方式，包括 seed 问题的处理。是否这个步骤完成后再开始后续的工作最合理？

**直接答**：**部分对、部分不对**。

**对的部分**：基础重构有价值，**比 §22 Tier 0 单纯的 #1 seed 范围大**。如果模型库/数据存储确实结构性混乱，先重构再建上层是合理的。

**不对的部分**：**"完成后再开始后续"会导致后续无限推迟**——基础重构这种工作可以无限做（永远能找到"再优化一点"的空间），如果不时间盒、不并行其它工作，6-12 个月可能还在重构。

**关键是把重构本身做成"时间盒的 Tier 0+"，而不是开放式的"完美基础"工程**。

### 28.1 必须先澄清范围（用户决定）

"重新规划模型库 + QE 实验 + 数仓数据存储"是个大词。**实际工作量取决于范围**——下面三档差异巨大：

| 范围档位 | 内容 | 工作量 | 是否必要 |
| --- | --- | --- | --- |
| **A：最小范围**（仅 §25 已包含） | manifest schema v2（recipe + asset_pointer 拆分）+ #1 seed + 模型权重存储路径标准化 | 1-3 周 | 必要，已在 §25/§22 内 |
| **B：中等范围**（§25 + 数据层重构） | A + QE 实验生命周期重构（结构化命名 / 状态标准化）+ TimescaleDB 关键表 schema 重审 + 模型库版本化与索引 | 4-8 周 | **取决于现状是否结构性混乱** |
| **C：完整重构** | B + 数仓全面分类重组（因子表 / 模型表 / 实验表 / 结果表分区与命名）+ 历史数据迁移 + 工具链重做 | 12-20 周 | **慎做**——容易越做越大、推迟所有产出 |

**用户必须先确认**：你说的"重新规划"是 A、B、还是 C？

- 如果是 **A**：已经在 §22/§25 计划内，**不需要新的 Tier**——按 §27 节奏推进即可
- 如果是 **B**：需要把 Tier 0 从 1-2 周扩展为 4-8 周，下面 §28.4 给方案
- 如果是 **C**：**强烈建议拆分**——把"必须先做的"拆出来作为 B，"可以渐进做的"延后；不要做"完整重构后再开始"

**没有用户的范围确认前，我不能给出确切建议**。下面默认按 B 假设展开。

### 28.2 真正的"基础重构"判断框架

不是所有"看起来基础"的工作都值得作为前置 Tier 0。判断三条标准：

| 标准 | 是 Tier 0 | 不是 Tier 0 |
| --- | --- | --- |
| **是否阻塞下游？** | 不做就无法做下游（如 #1 seed 不修无法做对账） | 不做下游也能跑（只是不优雅） |
| **后期重构成本是否倍增？** | 是（如 manifest schema 已经有数百个 manifest 引用，越晚改越痛） | 否（局部重构，不影响上层） |
| **现状是否真的"坏"？** | 是（结构性混乱、数据丢失、工作流断裂） | 否（只是"不够好"，但能用） |

**只满足"看起来基础"但不满足上述任一标准的工作**——属于"想做但不该现在做"，留在 backlog 里渐进改进。

#### 28.2.1 套用到三类对象

**模型库**：
- 模型存储路径（如何存权重文件）→ 现状能用吗？如果能用，**Tier 1**（不阻塞）
- 模型版本化（recipe + asset_pointer 拆分）→ §25 manifest schema v2 必须做 → **Tier 0**
- 模型与因子的索引/关联→ 演进时需要查询哪些模型用了哪些因子→ **Tier 1**（不阻塞 MVP）

**QE 实验**：
- 实验命名规范（`qe_yyyymmdd_xxxx_LoopN` 已经在用）→ 已经规范，**不动**
- 实验生命周期状态机（DRAFT / RUNNING / SUCCEEDED / FAILED / ARCHIVED）→ 现有 `qe_experiment_status_scanner.py` 已实现部分→ **Tier 1**（局部修复）
- 实验结果存储和查询→ 影响演进对比框架→ **Tier 1**

**数仓 / 数据存储**：
- 因子表 / 模型表 / 实验表 / 结果表的分区与命名→ Codex memory 显示这块在持续重构（factor pipeline v2 / factor library v2 cleanup / data snapshot architecture）→ **持续中**
- TimescaleDB partition 策略→ 性能问题影响演进速度→ **Tier 1**
- 历史数据迁移→ **Tier 2**（不影响新数据）

**结论**：真正的 Tier 0 候选只有：
- **#1 seed**（已确认）
- **manifest schema v2**（recipe + asset_pointer，已在 §25 内）
- **模型权重存储路径标准化**（如果当前真的混乱）

其余"看起来基础"的工作大部分是 Tier 1 或持续改进，**不应该阻塞主线**。

### 28.3 为什么"完成后再开始后续"是危险的

#### 28.3.1 重构无止境陷阱

基础重构有个特点：**永远能再做一点**。比如：
- 模型库重构后，发现因子库也有同样问题——再重构因子库
- 因子库重构后，发现数据 ingestion 也有同样问题——再重构 ingestion
- ingestion 重构后，发现 schema 还需要再优化——再优化
- ...

每个发现都"看起来"是必须先做的，结果 6 个月后还在基础层。

#### 28.3.2 价值产出长期为零

按 §27 计划，Month 3 末就能让用户看到"模拟盘跑起来 + 演进有对比框架"——具体的产出。**如果先做基础重构 4-8 周再开始后续**，意味着：

- Month 3 末：还在基础重构，无可见产出
- Month 5 末：基础重构完成（如果不超期），开始 vn.py MVP
- Month 7 末：vn.py MVP 完成，目标 A 雏形
- Month 9 末：演进框架完成，目标 B 雏形
- Month 11+：实盘准备

**整体延迟 4-5 个月**。这期间无法验证基础重构是否真的解决了下游问题——直到下游建起来才知道。

#### 28.3.3 没有下游需求驱动的基础设计往往错

基础重构如果没有"具体下游消费场景"，设计往往脱离实际：
- 模型库 schema 设计没考虑 vn.py adapter 怎么读 → 实盘时还要改
- 数仓分类没考虑演进框架怎么查询 → 演进时还要改
- ...

**最优做法是"骨架先建、皮肉同步生长"**——基础和应用并行做，互相验证。

### 28.4 推荐方案：扩展 Tier 0 为"Tier 0+"，时间盒 4-6 周

如果用户确认范围是 B（中等范围），推荐如下：

#### 28.4.1 Tier 0+ 内容（4-6 周）

| 子任务 | 工作量 | 归属 | 是否阻塞下游 |
| --- | --- | --- | --- |
| #1 模型 seed 控制 + 可复现性测试 | 1-2 周 | Codex | 是 |
| Manifest schema v2 设计（recipe + asset_pointer 拆分） | 1-2 周 | Codex（与 §25 工作流 #1 合并） | 是 |
| 模型权重存储路径标准化（如确有混乱） | 1 周 | Codex | 弱阻塞 |
| QE 实验状态机修复（如有 bug） | 1 周 | Codex | 不阻塞 |
| 关键 TimescaleDB 表 schema audit + 必要修复 | 1-2 周 | Codex | 不阻塞 |

**总计 4-6 周**，归 Codex 主导（属其 QE/数据维护范围）。

#### 28.4.2 并行启动的内容

Tier 0+ 期间，**Claude Code 可同时启动**（不阻塞）：

- §16 vn.py MVP 的 PoC 阶段（Week 1-2 PoC，§14.5 + §16）
- Paper v2 当前阻断点修复（§0 软合约、UI 配置开放等不依赖模型库的工作）

**Codex 在 Tier 0+ 后期**（Week 4-6）启动：

- §25 spec 设计正式启动（与 manifest schema v2 设计自然合并）
- #6 对比框架基础（依赖 seed 修复，但不依赖模型库重构完整完成）

#### 28.4.3 Tier 0+ 完成后的节奏

| 时点 | 状态 |
| --- | --- |
| **Week 4-6 末** | Tier 0+ 完成；vn.py MVP PoC 完成 |
| **Week 7-10** | vn.py MVP 主体（用 v2 manifest）+ §25 Engine 实现 + #6 对比框架推进 |
| **Week 11-15** | §25 完成 + #7 多 alpha + #5 公告信号设计 |
| **Week 16-26** | 滚动训练 + 实盘准备 + 产品化收口 |

**整体日历从 §27 的 6 个月扩展为 6.5-7 个月**——多 1 个月做基础。**这个代价值得**——因为做对了基础后续不再返工。

#### 28.4.4 不应该放进 Tier 0+ 的内容

下列项明确**不放在 Tier 0+**，作为 Tier 1/2/3 内的渐进工作：

- 因子库全面 cleanup（已经持续在做，渐进推进）
- 数据 ingestion pipeline 重构（除非真的导致数据错误）
- 历史数据迁移到新 schema（兼容老 schema，逐步迁移）
- UI 全面简化（§1 工作，Tier 3）
- 测试矩阵补到 80% 覆盖（持续工作）
- 完整工程 CI infra（Tier 2/3）

**这条非常重要——一旦把 cleanup / 优化 / 完美主义的工作放进 Tier 0+，时间盒会爆炸**。

### 28.5 决策矩阵

| 用户判断现状 | 范围 | 建议 |
| --- | --- | --- |
| 模型库/数据存储**确实结构性混乱**导致下游频繁踩坑 | B（中等） | Tier 0+ 4-6 周，按 §28.4 推进 |
| 模型库/数据存储**有改进空间但能用**，不结构性影响下游 | A（最小） | 按 §27 原节奏，不扩 Tier 0；改进留 Tier 1/2 渐进做 |
| 模型库/数据存储**严重断裂**，几乎无法继续工作 | C（完整） | **必须拆分**：列出"什么是必须先修"作为 Tier 0+，其余渐进改进 |

**只有用户能判断现状属于哪一档**——我不掌握模型库 / 数据存储的具体状态。

### 28.6 给用户的关键判断要点

需要你回答两件事，我才能给精确建议：

1. **模型库/数据存储现在"坏"到什么程度？**
   - 现在做 QE 实验时，**有没有具体的报错或工作流断裂**？还是只是"觉得不够整洁"？
   - 如果有具体错误，列出来——这些是真正要修的
   - 如果只是觉得不够整洁，**留 backlog 渐进改进**，不阻塞主线
2. **重构想覆盖到哪个范围（A/B/C）？**
   - A：已在 §22/§25 内，不需要扩 Tier 0
   - B：扩展 Tier 0 为 4-6 周（§28.4 方案）
   - C：必须拆分，不要"一次性完整重构"

### 28.7 一句话结论

**"先完成基础重构再开始后续"在范围 A 下不需要、在范围 B 下需要扩 Tier 0+ 到 4-6 周、在范围 C 下必须拆分而非整体推进**。

**关键风险**：基础重构的"完美陷阱"——永远能再优化一点。**必须时间盒 4-6 周硬性结束，不论是否"完美"**——剩下的留 Tier 1/2 渐进做。

**正确做法**：把基础重构当成一个**时间盒的 Tier 0+ 阶段**（4-6 周），其后立即并行启动 vn.py MVP + §25 Engine + #6 对比框架。**整体日历从 §27 的 6 个月扩展为 6.5-7 个月**——这是值得的代价。

**不正确的做法**：开放式"基础重构"——会无限延展、价值产出长期为零、设计脱离下游实际场景。

**用户必须先确认范围**（A/B/C）和**当前真实痛点**（具体报错 vs 整洁度）才能给精确建议。如果是范围 A，不需要新动作；如果是 B，按 §28.4 推进；如果是 C，建议先列具体痛点清单，再按 §28.2 的判断框架拆分。

---

## 29. 与 Codex 同日 SOTA / StrategyPackage / 资产治理设计文档的整合分析

参考：`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`（Codex 同日产出 1495 行，当前在 worktree `codex/qe-sota-strategy-asset-doc-20260508` 分支，未合 main）

**结论先给**：Codex 设计与本文 §22-§28 大部分兼容、且在治理/生命周期方面比本文设计更细致——必须采纳。但**在"多 alpha 自动同步"这一点上存在架构分歧**：Codex 走"asset freeze + retest gate"路径，本文 §25 B+ 走"共享 Engine + 三 adapter"路径。**两条路实际不互斥，应叠加使用**——Codex 解决治理，§25 B+ 解决执行层代码共享。

### 29.1 Codex 设计核心要点

| 维度 | Codex 设计 | 创新点 |
| --- | --- | --- |
| **职责四分** | QE（探索）/ QE 数仓（永久研究事实）/ SOTA 殿堂（人工审核工作台）/ StrategyPackage（资产对象） | 把当前混在一起的概念分清 |
| **生命周期状态机** | `AUTO_CANDIDATE → REVIEW_PENDING → SOTA_APPROVED → ASSET_FROZEN → ORIGINAL_RETESTING → ORIGINAL_RETEST_PASSED → PAPER_CANDIDATE → PAPER_ENABLED → PAPER_VALIDATED → LIVE_CANDIDATE`，`RETIRED` 终态不可删 | 替代当前手工"标记"流程 |
| **Master Seed Contract** | 训练时记录 `random_seed / numpy / torch / lightgbm / xgboost / catboost` 等全部子 seed + 库版本 + 硬件 context；`seed_policy=fixed/multi_seed/random_logged/unset_legacy` | 比 §22 Tier 0 更细致 |
| **Seed Fragility Scoring** | `metric_mean/std_by_seed / sensitivity_score / rank_stability / selection_overlap_by_seed`，`seed_fragile=true` 默认禁 Paper | 比 §22 多了"种子敏感性筛选"维度 |
| **模型库 4 层** | `ModelTemplate`（家族）→ `ModelSpec`（代码 + 架构 + 超参搜索空间）→ `ModelTrial`（一次训练）→ `ModelArtifact`（权重 + 预处理 + 推理 schema） | 业界标准 MLflow / SageMaker 模式 |
| **Frozen Alpha Core** | factor_set + factor_order + factor_schema_hash + model_spec + seed + weights + preprocessor + feature_schema + train/valid/test 标识——晋级后不可改 | 形式化 §2 "策略包冻结" |
| **Runtime Variants** | TopK / 执行算法 / HMM 开关 / 风控 overlay 可变，每个变种独立 hash + 必须 validation 通过；不能改 frozen core | 形式化 §3 "策略包之上自定义" |
| **明确否决** "AlphaAssetBundle" 单独抽象 | line 71-97 拒绝并行资产治理层，主张扩展 StrategyPackage v2 一并承载 | 与 §25 B+ 在抽象层数上有分歧 |

### 29.2 与本文 §22-§28 的对照

#### 强一致（直接采纳 Codex 方案）

| 本文章节 | Codex 章节 | 一致性 |
| --- | --- | --- |
| §22 Tier 0 #1 seed | Codex Phase 4 + Master Seed Contract | **Codex 更全面** |
| §28 Tier 0+ 范围 B | Codex Phase 4 + Phase 5 | **Codex 给出具体设计** |
| §15.2 / §2 "策略包冻结模型+因子" | Codex Frozen Alpha Core | **Codex 形式化为 schema 字段** |
| §3 "策略包之上 Selection/Paper 自定义" | Codex Runtime Variant + locked core | **Codex 形式化** |
| §11.3 shadow run / 输出对账 | Codex "ORIGINAL_RETESTING" 强制门禁 | **Codex 落到 state machine** |
| §19.3 滚动训练 manifest schema v2 | Codex ModelTrial / ModelArtifact 自然支持 | **Codex 提供承载结构** |
| §26 类别 A 同步（训练期） | Codex 通过 model_artifact 传播 | **一致** |
| §22 演进对比框架 | Codex Mode A-F validation_run 矩阵 | **Codex 更细致** |

**结论**：上述 8 项**直接采纳 Codex 设计**，本文不再自己设计。

#### 弱差异（互补、可叠加）

| 议题 | 本文 | Codex |
| --- | --- | --- |
| 测试流水线 cross-testing | §20-§21 | 未涉及 |
| vn.py 接入 | §15-§16 | 未涉及 |
| Agent Teams 协调 | §18 | 未涉及 |

**两份文档互补，不冲突**。

#### 重要分歧（必须协调）

Codex line 71-97 否决了"AlphaAssetBundle"——理由是避免"SOTA / StrategyPackage / Paper 三套资产引用并存"。

但本文 §25 B+ 是另一回事——**共享 Strategy Engine + 三 adapter** 是为了让多 alpha / 公告信号 / 实盘 adapter 三端**自动同步**，**不是另起一个资产层**——Codex 文档没明确这个区分。

### 29.3 Codex 设计在哪里"够用"，在哪里"不够"

#### 够用的场景
- 单个 StrategyPackage 全生命周期管理
- 单个 manifest 在 Paper / 实盘的资产一致性
- runtime variant（topk / 算法）变体管理
- 滚动训练新 ModelArtifact 晋级
- Seed fragility 筛选 → 避免"靠运气"包

#### 不够的场景

下列 Codex 文档**没有给出明确机制**：

- **多 alpha 落地后，新 alpha 类型同步到三端的代码层**：Codex 让 SOTA 殿堂晋级新策略包，但每个新策略类型的**执行代码**（topk 计算 / 权重融合 / 候选过滤）仍在 QE Qlib / Paper v2 / 未来 Live 三处独立——Codex 治理只能 catch 已存在的 bug（通过 retest），不能避免"两边各写一份不一致"的根本问题
- **公告/财报信号合成**：Codex 假设新 alpha 通过 SOTA 流程晋级，但合成代码在 QE Qlib YAML 端 + Paper inference 端各写一次的问题没解决
- **Paper v2 → 实盘切换不写代码**：Codex 流程允许 Paper-validated 包变成 Live-candidate，但**实盘 adapter 本身**还要写——除非 trading_core 已经把 Paper SimGateway 和 Live vnpy_xt 都做成"可切换"——这是 §25 B+ + §15 vn.py 路径解决的层

#### 真实关系：双层叠加

```
Layer 1: Codex 治理层（采纳）
  ├─ SOTA 殿堂：晋级流程
  ├─ StrategyPackage v2：资产冻结 + 状态机
  ├─ Master Seed Contract：可复现性
  ├─ Model Registry 4 层：模型库治理
  └─ Validation Modes A-F：retest 门禁

Layer 2: 本文 §25 B+ 执行共享层（建议在 Codex 之上叠加）
  ├─ StrategySpec：Codex StrategyPackage v2 实际就是这个 spec
  ├─ Strategy Engine（可选）：解决多 alpha 跨栈同步
  └─ 3 Adapters（可选）：实盘切换是配置项不是代码
```

**Codex 的 StrategyPackage v2 实际上已经接近 §25 的 StrategySpec**——只是 Codex 没把它从"治理实体"显式抽象为"配置驱动的 Engine 输入"。两者关系是同一个对象的两个视角。

### 29.4 模型库设计是否最优

Codex 4 层架构（Template / Spec / Trial / Artifact）**与业界标准（MLflow / SageMaker / W&B）几乎一致**，是成熟、合理的设计。

#### 评估对照

| 业界做法 | Codex 设计 | 是否覆盖 |
| --- | --- | --- |
| MLflow registered_model + version + stage + artifact | Template + Spec + Trial + Artifact | ✓ 一致 |
| SageMaker model_package_group + model_package + artifact | 类似 | ✓ |
| Lifecycle 状态机 + 审计 | `model_lifecycle_event` 表 | ✓ |
| 模型 lineage（衍生 / 微调链） | ModelTrial → ModelArtifact 隐式承载 | ⚠ 隐式 |
| Cross-spec 标签 / 集合 | metadata 字段 | ⚠ 隐式 |
| 训练 / 推理成本追踪 | 未覆盖 | ✗ |
| A/B 测试 / 渐进 rollout | runtime variant 雏形 | ⚠ 部分 |
| 数据泄漏检测 | 未覆盖 | ✗ |
| 模型签名 / Schema 追踪 | feature_schema + prediction_schema | ✓ |

#### 可建议的微增强（非必须）

1. **显式 lineage**：ModelArtifact 加 `parent_artifact_id`，追踪"衍生自哪个权重"
2. **标签 / 集合**：ModelSpec 加 `tags: list[str]`，支持横切查询
3. **训练成本字段**：ModelTrial 加 `compute_seconds / gpu_hours / dataset_bytes`
4. **数据快照引用**：ModelTrial 引用 `data_snapshot_id`，帮助严格复现

这些是**渐进增强**，不影响 Codex 主体设计。

#### 是否有更好架构？

**没有显著更好的**。Codex 4 层是业界共识；改成 3 层（合并 Spec/Trial）会丢失"超参搜索空间 vs 单次尝试"区分；改成 5 层（加 Lineage）过度复杂。**采纳即可**。

唯一需要补的是**模型与 StrategyPackage 的关系**：当前 Codex 设计是"package 引用 ModelArtifact"，但**一个 package 可以引用多个 ModelArtifact 吗**（多模型 ensemble / 多 alpha 共用模型）——Codex 文档没明确，**值得在 spec 设计时确认**。

### 29.5 本文章节更新方向

| 章节 | 是否需要更新 | 方向 |
| --- | --- | --- |
| §22 Tier 0 #1 seed | **更新** | 从"1-2 周修 seed"扩展为"采纳 Codex Master Seed Contract + Phase 4，含 fragility scoring"，工作量 2-3 周 |
| §25 选项 B+ | **更新** | StrategySpec 引用 "StrategyPackage v2"（Codex 设计），不另起新对象；Engine 仍按 §25.3 工作流 #2 实现 |
| §28 Tier 0+ 范围 B | **更新** | 直接采纳 Codex Phase 4 + Phase 5 + Frozen Alpha Core 作为 Tier 0+ 内容；时间盒 4-6 周 |
| §19.3 滚动训练 | **更新** | 复用 Codex Model Registry + ModelTrial；schema v2 = ModelSpec / ModelArtifact 区分 |
| §27 依赖图 | **更新** | Codex 7 阶段排进时间表 |
| §15-§18 / §20-§24 / §26 | **不更新** | Codex 不涉及这些范围 |

### 29.6 推荐整合方案

#### 时间表更新

| 阶段 | §27 原节奏 | 整合 Codex 后 |
| --- | --- | --- |
| Tier 0 / 0+ | 1-2 周（仅 seed） | **4-6 周**（采纳 Codex Phase 0-5：术语 + 手工 SOTA + 资产冻结 + retest + Master Seed Contract + Model Library 4 层） |
| Tier 1A | Month 2-3 vn.py MVP | **不变** |
| Tier 1B | Month 2-3 §25 + #6 对比框架 | **更新**：Codex Phase 6 (Runtime Variants) + Phase 7 (latest-data + rolling) 合并到 Tier 1B；§25 Engine 作为补充 |
| Tier 2-3 | Month 4-6 多 alpha + 公告 + 滚动 + 实盘 | **不变**（叠加在 Codex 治理上） |

**整体日历从 6 个月调整为 6.5-7 个月**——多 1 个月落地 Codex 完整治理设计——值得。

#### 与 Codex 的分工

| 工作流 | 归属 |
| --- | --- |
| Codex Phase 0-7（治理 + 模型库 + seed + 资产冻结 + variants + latest-data） | **Codex 主导** |
| §25 B+ Strategy Engine + 3 adapters | **Claude Code 主导** |
| §15 vn.py + miniQMT 接入 | **Claude Code 主导** |
| §16 4 周 MVP | **Claude Code 多窗口 / Agent Teams** |
| §20-§21 cross-testing 平台增强 | 双方协作 |
| #6 自动演进 | **Codex 主导** |
| #7 多 alpha | Codex Phase 6 + §25 B+ 都就绪后启动 | 双方协作 |

#### 用户决策点

需要拍板：

1. **是否采纳 Codex 设计作为 Tier 0+ 的具体内容？**（强烈建议是）
2. **§25 B+ 是否仍要做？**（决定多 alpha / 公告信号是否自动跨栈同步）
   - 选 A：仅 Codex 治理 → 多 alpha / 公告每次实现两份；retest gate 兜底
   - 选 B：Codex 治理 + §25 B+ → 一次实现，三端自动同步
3. **Codex 文档是否合 main**？（建议先合让本文引用稳定）

### 29.7 一句话结论

**Codex 同日 SOTA / 资产治理设计文档质量很高，与本文 §22-§28 大部分兼容、互补，必须采纳——但不能完全替代 §25 B+ 的"共享 Engine + 三 adapter"路径，二者解决不同层次的问题**：

- **Codex 解决治理 / 生命周期 / 资产冻结 / 退役**——本文 §22 / §28 应直接采纳，把"manifest schema v2"具体化为 Codex Frozen Alpha Core + Model Registry 4 层架构
- **§25 B+ 解决多 alpha / 公告信号 / 实盘切换的代码层共享**——Codex 没解决，仍需考虑（决定权在用户）

**模型库设计**：Codex 4 层架构（Template / Spec / Trial / Artifact）是业界共识，无明显更好方案；可微增强（lineage / tags / 成本字段）但不重新设计。

**整体计划影响**：Tier 0+ 时间盒从"1-2 周"扩为"4-6 周"采纳 Codex Phase 0-5；其他章节大部分不变；整体日历从 6 个月调整为 6.5-7 个月。

**关键决策**：用户需拍板是否在 Codex 治理基础上叠加 §25 B+ 共享 Engine——如果坚持"多 alpha / 公告信号 / 实盘切换不写代码"目标，§25 B+ 必须做；如果接受"双工 + retest gate 兜底"，可只做 Codex 治理。**两个选择都合理，取决于对长期演进规模的判断**。

---

## 30. §29.6.3 决策点 #2 的明确建议：选 B，但范围因 Codex 设计而简化

用户在 2026-05-08 明确：

1. ✓ 采纳 Codex 设计作为 Tier 0+ 内容
2. ❓ 询问 §25 B+ 是否做的明确建议
3. Codex 文档由用户协调合 main

本节给出 #2 的明确建议。

### 30.1 直接建议：选 B（Codex 治理 + §25 B+ 共享 Engine）

理由按重要性排序：

#### 30.1.1 用户目标 B（持续演进 + 多 alpha）注定高频 Class C 修改

按 §22 优先级路线，下列工作流将产生大量 Class C 修改：

| 工作流 | 估算每年 Class C 修改次数 |
| --- | --- |
| #6 QE 自动演进（Tier 1B 主体）每个 loop 可能产出新策略子类 | 10-20 次/年 |
| #7 多 alpha（不同 combination_rule 实验） | 5-10 次/年 |
| #5 公告/财报信号（不同 event 类型 + 衰减策略） | 5-10 次/年 |
| 滚动训练 trial / variant 实验 | 5-10 次/年 |
| **累计** | **25-50 次/年** |

**这是 §22 路线的真实演进强度**——不是"5-10 次/年"的低活动场景。

#### 30.1.2 双工成本量化

**仅靠 Codex 治理（不做 §25 B+）的成本**：

| 项目 | 单次 | 年化（按 30 次/年中位估计） |
| --- | --- | --- |
| 双侧实现（QE + Paper） | +50-100% 时间 = 1-3 工作日/次 | 30-90 工作日 |
| Mode A-F retest 验证 | 0.5-1 工作日/次 | 15-30 工作日 |
| 漏改 incident（保守估 10%） | 1-3 工作日修复 | 3-9 工作日 |
| **累计** | | **48-129 工作日/年（约 10-26 工作周）** |

**做 §25 B+ 的一次性成本**（在 Codex 治理已经就位的基础上）：

| 工作流 | 工作量 | 备注 |
| --- | --- | --- |
| StrategySpec 设计 | **节省**——直接 reuse Codex StrategyPackage v2 manifest schema | 0-1 周 |
| Strategy Engine 核心实现 | 2-3 周 | 与 §25.3 工作流 #2 一致 |
| 3 个 adapter 实现 | 3-5 周（QE + Paper + Live 各 1-2 周） | 比 §25 原估略短，因 Codex Frozen Alpha Core 已规约接口 |
| 现有代码迁移 | 2-3 周 | 与 §25.3 工作流 #5 一致 |
| Equivalence 测试 | 1-2 周 | 与 Codex Mode A-F 矩阵复用基础设施 |
| **累计** | **8-14 周（可压缩到日历 5-7 周）** | 比 §25.3 原估 9-15 周略短 |

**ROI 对比**：

| 策略 | 第 1 年总成本 | 第 2 年起年化成本 |
| --- | ---: | ---: |
| 仅 Codex 治理（双工） | 10-26 工作周 | 10-26 工作周 |
| Codex + §25 B+ | 8-14 工作周一次性 + 1-2 工作周 minor 维护 | 1-2 工作周 |

**第 1 年即回本**；之后每年净节省 8-24 工作周。

#### 30.1.3 §25 B+ 在 Codex 设计基础上"打折"

§29.3 的洞察是：**Codex 的 StrategyPackage v2 已经接近 §25 的 StrategySpec**——Frozen Alpha Core schema 实际上就规定了 spec 内容。这意味着：

- StrategySpec 设计阶段**几乎为零**（直接 reuse Codex schema + Pydantic 化）
- Strategy Engine 仍要实现，但有 Codex Frozen Alpha Core 作为输入契约，**实现边界清晰**
- 3 个 adapter 仍要写，但 Codex 的 Validation Mode A-F 给出了 adapter 的"消费场景"——adapter 设计有现成参考

**§25 B+ 在 Codex 设计基础上的总成本下降到 8-14 周（vs 原估 9-15 周）**，并且**风险大幅降低**（spec 设计风险被 Codex 化解）。

#### 30.1.4 多 alpha (#7) 启动时机的硬约束

§27.5 已经判定：**多 alpha (#7) 必须在 §25 完成后才开工**——否则永久双工。

如果选 A（仅 Codex 治理，不做 §25 B+），多 alpha 启动时**没有共享 Engine**，意味着：
- QE Qlib 端实现一份多 alpha 组合
- Paper v2 runtime 端实现一份多 alpha 组合
- 未来 Live adapter 又一份
- Codex 治理通过 retest 验证三份一致——**但每次新的 combination_rule 都要做三份 + 三份 retest**

**这与用户"多 alpha 自动同步"的明确诉求（§25.1 / §26 / §27）冲突**。

### 30.2 风险对冲：如果对 25-50 次/年估算保守

如果用户认为实际 Class C 修改频率会更低（如 10-15 次/年），ROI 仍正但不那么显著。可以采用**渐进策略**：

#### 30.2.1 渐进策略（推荐次优方案）

```
Tier 0+ (Month 1-1.5)     : 完成 Codex Phase 0-5 治理
Tier 1A (Month 2-3)       : vn.py MVP（不依赖 §25 B+）
Tier 1B 早期 (Month 2-3)  : §25 B+ 第一阶段——StrategySpec 引用 Codex schema + Strategy Engine 核心实现
                            （只覆盖现有功能，不主动加多 alpha 能力）
Tier 1B 末 (Month 3-4)    : Strategy Engine 完成 + 现有代码迁移
                            决策点：观察 #6 演进运行 1-2 个月的实际 Class C 修改频率
                            ├── 如果 ≥ 15 次/月预期：完成 §25 B+ 全部 + 启动 #7 多 alpha
                            └── 如果 < 15 次/月：暂停 §25 B+，按双工模式做 #7 多 alpha；保留 Engine 作为 Paper 内部用
```

**渐进策略的好处**：
- 用 1-2 个月观察期决定全力推 §25 B+ 还是降级双工
- 即使最终选择降级，前期投入（Engine + 迁移）也不浪费——Paper v2 内部仍受益
- 风险下界清晰

**渐进策略的代价**：
- 决策点延后 1-2 个月，期间 #7 多 alpha 启动也延后
- 若最终决策"全力推"，比直接选 B 多花 2-4 周（决策延迟成本）

#### 30.2.2 何时选渐进策略

- 用户对 #6 演进的实际产出强度心里没底
- 团队对 §25 B+ 实施信心不足
- 想看到 1-2 个月 Codex 治理实际效果再决定

**何时直接选 B（不走渐进）**：
- 用户已确信 #6 + #7 + #5 + 实盘 4 个工作流都会落地
- 团队对架构改造有信心
- 想最快路径达成"多 alpha 不写代码"目标

### 30.3 推荐节奏（在 §29.6.1 整合时间表上落实）

按 §29.6.1 + 选 B 直接路径：

```
Month 1-1.5  Tier 0+  Codex Phase 0-5（治理 + 模型库 + seed）  [Codex 主导]
                     §25 B+ 工作流 #1-#2 启动:
                     - StrategySpec = Codex StrategyPackage v2 引用    [Codex/Claude Code 协作]
                     - Strategy Engine 设计                              [Claude Code 主导]

Month 2-3    Tier 1A  vn.py MVP                                          [Claude Code 多窗口]
             Tier 1B  §25 B+ 工作流 #2-#5 推进                            [Claude Code]
                     - Strategy Engine 实现
                     - 3 个 adapter 实现
                     - 现有代码迁移
                     #6 QE 演进对比框架                                   [Codex 主导]
                     Codex Phase 6（Runtime Variants）                   [Codex]

Month 4      §25 B+ 工作流 #6 equivalence 测试 + 整合 Codex Mode A-F      [双方协作]
             #7 多 alpha 启动（自动落三端）                                [双方协作]
             Codex Phase 7（latest-data + rolling validation）            [Codex]

Month 5-6    #5 公告/财报信号 + 滚动训练 + 实盘前 shadow                   [自动支持三端]
```

**整体日历**：6.5-7 个月，与 §29.6.1 一致。**§25 B+ 大部分嵌入 Tier 1B 内并行做**——不额外延长总日历。

### 30.4 一句话明确建议

**选 B**：Codex 治理 + §25 B+ 共享 Engine——**两层叠加**。

理由：
- 用户目标 B（演进 + 多 alpha）注定 25-50 次/年 Class C 修改频率
- 仅靠双工 + retest 第 1 年成本就 10-26 工作周；§25 B+ 一次性成本 8-14 周即回本
- Codex StrategyPackage v2 已经把 §25 B+ 的 spec 设计阶段消化掉，剩余工作量打折
- 多 alpha (#7) 是用户 Tier 1B 关键路径，必须先有 §25 B+ 才能"自动同步"

**风险对冲方案（可选）**：渐进策略（§30.2.1）—— 先做 Codex 治理 + Strategy Engine 核心，1-2 个月观察期后决定是否全力推 §25 B+。代价是决策延迟 1-2 个月。

**我的明确推荐：直接选 B 不走渐进**——因为用户已多次明确"切换实盘不写代码 / 多 alpha 自动同步"是核心诉求（§25.1 / §26 / §27），渐进策略与该诉求张力较大。

### 30.5 落地下一步

按 §30.4 选 B 路径：

1. **Codex 侧合 main**（用户已确认）—— 让 §29 引用稳定
2. **用户授权 Codex 启动 Phase 0-1**（术语对齐 + 手工 SOTA 流程）—— Codex 维护范围内可立即开始
3. **用户与 Claude Code + Codex 召开 spec 设计评审会**（StrategySpec ≅ Codex StrategyPackage v2 schema 的字段评审）—— 决定 §25 B+ 是否能直接 reuse 还是需要补字段
4. **Codex 启动 Phase 4（Master Seed Contract）+ Phase 5（Model Library）** —— Tier 0+ 主体工作
5. **Claude Code 启动 §16 vn.py MVP PoC（Week 1-2 PoC）+ §25 B+ Strategy Engine 核心设计** —— 不依赖 Tier 0+ 完成，可并行
6. **Tier 0+ Phase 0-5 完成后**（约 Month 1.5），Tier 1A/1B 全速推进

**关键节点**：Month 4 末 §25 B+ 完成 → 多 alpha #7 立即启动；Month 6 末整体可上实盘状态。

---

## 31. Codex 文档已合 main 后的启动评估 + 现阶段高优先级工作

用户在 2026-05-08 明确：
- Codex 文档已合并到 main
- 多 alpha 架构推迟（等单 alpha 探索方向成熟后再启动）
- 询问：是否还有内容需补充？现阶段高优先级工作？

### 31.1 Codex 文档启动前需要补充的内容（不阻塞，可边做边补）

Codex 文档（1495 行）已经覆盖治理 / 模型库 / Seed / 状态机 / Validation 模式等核心设计，**总体足够 Codex 启动 Phase 0-1-4 实施**。下列内容**不是阻塞项**，但建议在执行过程中或之前补：

| 补充项 | 必要性 | 何时做 | 归属 |
| --- | --- | --- | --- |
| **每 Phase 工作量估算（周/工作日）** | 中 | Phase 0-1 启动后 1 周内 | Codex（基于实际节奏估） |
| **现有 4 个 LEGACY_NON_ST_PIT 包的处理** | 低 | Phase 1 时顺便明确（标记 legacy / 推进迁移 / 退役任一） | Codex |
| **`aistock_model_catalog` → `model_registry.*` 迁移脚本** | 中 | Phase 5 实施时同步设计 | Codex |
| **SOTA 殿堂 UI 设计文档**（独立 doc） | 中 | Phase 1 末或 Phase 2 开始前 | 双方协作（Claude Code 帮 UI 部分） |
| **Validation Mode A-F 与 `tests/aistock_validation/modules/` 的对接** | 中 | Phase 3 启动时 | Codex（写新模块测试矩阵） |
| **单 package 是否支持多 ModelArtifact** 字段确认 | 低 | Phase 5 schema 实现时 | Codex（5 分钟决定即可） |
| **数仓数据存储分类**（用户在 §28 提到的范围一部分） | 不在 Codex 文档范围 | 不在 Tier 0+ 内，留 Tier 1/2 渐进做 | Codex 持续 |

**结论**：**没有阻塞性缺失**。Codex 可立即按文档进入实施阶段。上面 7 项是"边做边补"或"低优先级"。

### 31.2 Codex 现阶段可立即启动的高优先级工作

按 §29.6.1 时间表 + 用户多 alpha 推迟的指示，**Codex 应该立即开始**的工作（按优先级排序）：

#### 优先级 1：Phase 4 Master Seed Contract（最高优先级）

**理由**：这是 §22 的 Tier 0 #1 seed，**所有后续工作的最小前置**。

**内容**：
- 实施 Codex 文档 line 471-560 设计：训练时记录 `random_seed / numpy / torch / lightgbm / xgboost / catboost` 等全部子 seed + 库版本 + 硬件 context
- 添加 `seed_policy=fixed/multi_seed/random_logged/unset_legacy` 字段
- 实施 Seed Fragility Scoring（line 537-558）

**估算**：2-3 周

**可立即开始**：是。不依赖任何其他工作。

#### 优先级 2：Phase 0 + Phase 1（治理流程基础）

**理由**：低工作量、立即解锁后续 Phase。

**内容**：
- Phase 0：术语对齐 —— 停止把 QE 自动 SOTA 称为"自动加入"
- Phase 1：手工"加入 SOTA 殿堂"按钮 + lifecycle status 跟踪基础

**估算**：1-2 周（与 Phase 4 并行）

**可立即开始**：是。

#### 优先级 3：Phase 5 Model Library 4 层架构（基础设施级）

**理由**：滚动训练、Paper 资产引用、SOTA 治理都依赖此。

**内容**：
- 新增 `model_registry.model_template / model_spec / model_trial / model_artifact` 四张表
- 现有 `aistock_model_catalog` 保留作为兼容视图
- `model_lifecycle_event` 替代静默删除

**估算**：3-4 周

**可启动时机**：Phase 4 完成 50% 后启动（约 Week 2 末）。

#### 优先级 4：Phase 2 Asset Freezing

**理由**：Phase 1 完成后即可启动；Paper v2 消费冻结包需要这一步。

**内容**：模型权重 / 因子代码 / schema 复制到 protected 库 + 记录 sha256

**估算**：1-2 周

**可启动时机**：Phase 1 完成后（约 Week 3）。

#### 优先级 5：Phase 3 强制原始配置 retest

**理由**：Paper readiness 的硬门禁。

**估算**：1-2 周

**可启动时机**：Phase 2 完成后（约 Week 4-5）。

#### 暂时降级（多 alpha 推迟带来的影响）

| Phase | 原优先级 | 调整后 |
| --- | --- | --- |
| Phase 6 Runtime Variants | 中 | **降低**——单 alpha 阶段 variant 需求低；保留设计但延后实施 |
| Phase 7 latest-data + rolling-train validation | 中 | 保持——与滚动训练专项匹配 |

### 31.3 Claude Code 现阶段可立即启动的高优先级工作（与 Codex 并行，不冲突）

#### 优先级 1：vn.py + miniQMT PoC（3-5 天连通性验证）

**理由**：§16 4 周 MVP 的前置；不依赖 Codex Phase 4。

**内容**（§16.1）：
- `pip install vnpy vnpy_xt` 安装验证
- miniQMT 仿真账户连通（通过 vnpy_xt）
- vn.py headless 模式运行

**估算**：3-5 天

**可立即开始**：是。

#### 优先级 2：vn.py + Paper v2 集成 MVP（4 周）

**理由**：用户目标 A 关键路径；用现有 ST PIT manifest 跑（不依赖 Codex Phase 4 完成）。

**估算**：4 周（PoC 后立即开始）

**可启动时机**：Week 1 PoC 通过后立即。

#### 优先级 3：§25 B+ Strategy Engine 核心设计（多 alpha 推迟后降级，但保留）

**理由**：用户多 alpha 推迟后，§25 B+ 紧迫性下降，但仍是中期价值高的工作。

**调整方案**：
- **不在 Tier 0+ 全力推**——把 §25 B+ 工作流 #1 spec 设计推迟到与 Codex Phase 5 (Model Library) 同步
- **核心 Engine 设计纸面工作可以先做**——读 Codex StrategyPackage v2 schema + 设计 Engine 接口，0.5-1 周成本
- **实际实现等到 Codex Phase 4-5 完成后再启动**——避免在 Codex 治理基础未就位时盲做

**估算**：纸面设计 0.5-1 周（现阶段做），实际实现 5-7 周（Codex Phase 4-5 完成后）

#### 优先级 4：Paper v2 当前阻断点修复（不依赖 Codex 也不依赖 vn.py）

**理由**：在 vn.py MVP 期间提前修一些 §0/§7 的痛点，让 MVP 演示更顺。

**内容**：
- ST PIT spans 数据补到最新交易日（Codex P0-2，由 Codex 做）
- Live inference 冷启动 preflight（§0/§7 P0-4）
- UI 配置开放（§1 部分，不在 §31 全面简化）

**估算**：穿插 1-2 周

**可启动时机**：vn.py PoC 阶段并行做。

### 31.4 多 alpha 推迟对整体计划的影响

用户决策"多 alpha 等单 alpha 架构稳定后再启动"——这是**合理的产品判断**。对计划影响：

#### 31.4.1 §25 B+ 紧迫性下降

§30.1.1 的 Class C 修改频率估算需修正：

| 工作流 | 原估（含多 alpha） | 修正（多 alpha 推迟） |
| --- | --- | --- |
| #6 演进 | 10-20 次/年 | 10-20 次/年（不变） |
| #7 多 alpha | 5-10 次/年 | **0**（推迟） |
| #5 公告/财报信号 | 5-10 次/年 | 5-10 次/年（不变） |
| 滚动训练 trial | 5-10 次/年 | 5-10 次/年（不变） |
| **累计** | 25-50 次/年 | **20-40 次/年** |

仍然高频，但**没有"必须在 #7 之前完成"的硬时序约束**。所以：

- §25 B+ 仍值得做，但**不是 Tier 0+ 紧急工作**
- 可以等 Codex Phase 4-5 完成后再启动 §25 B+ 实施
- §25 B+ 的"防止三套漂移"价值在公告信号 + 滚动训练接入时体现

#### 31.4.2 时间表调整

| 阶段 | §29/§30 原节奏 | 多 alpha 推迟后 |
| --- | --- | --- |
| Tier 0+ Month 1-1.5 | Codex Phase 0-5 + §25 B+ 启动 | **Codex Phase 0-1-4 + §25 B+ 仅纸面设计**（不实施） |
| Tier 1A Month 2-3 | vn.py MVP + §25 B+ 实施 | vn.py MVP + Codex Phase 5 推进 |
| Tier 1B Month 3-4 | §25 B+ 完成 + #7 多 alpha 启动 | **§25 B+ 实施**（Codex Phase 5 完成后启动）+ #6 演进对比框架 + Codex Phase 6-7 |
| Tier 2-3 Month 4-6 | #7 多 alpha + #5 公告 + 滚动训练 + 实盘 | **#5 公告 + 滚动训练 + 实盘准备**（多 alpha 不在该窗口） |
| 长期 | - | 单 alpha 探索成熟后启动 #7（可能 Month 7+ 或更晚，由用户判断） |

**整体日历仍 6-7 个月**，但内容重心从"#7 多 alpha"移到"#5 公告信号 + 滚动训练 + 实盘 demo"。**用户目标 A（模拟盘 demo）和目标 B（演进对比 + 滚动）都能在 Month 6 内达成**。

### 31.5 现阶段（Week 1-2）应立即启动的工作清单

按 §31.2 + §31.3 整理：

| # | 工作 | 归属 | 工作量 | 启动时机 |
| --- | --- | --- | --- | --- |
| 1 | **Phase 4 Master Seed Contract** | **Codex** | 2-3 周 | **立即** |
| 2 | **Phase 0-1 治理流程基础**（术语对齐 + 手工 SOTA 按钮） | **Codex** | 1-2 周 | **立即**（与 #1 并行） |
| 3 | **vn.py + miniQMT PoC（3-5 天连通性）** | **Claude Code** | 3-5 天 | **立即** |
| 4 | **vn.py + Paper v2 集成 MVP** | **Claude Code 多窗口** | 4 周 | PoC 通过后立即（约 Week 1 末） |
| 5 | **§25 B+ Strategy Engine 接口纸面设计** | Claude Code | 0.5-1 周 | Week 2 起做 |
| 6 | **ST PIT spans 数据补到最新交易日**（解决 §0/§7 P0-2） | Codex | 0.5-1 周 | **立即**（与 #1 并行） |
| 7 | **Phase 5 Model Library**（4 层架构） | Codex | 3-4 周 | Week 2-3 启动 |

**整体并行 4 条主线**：Codex Phase 0-1-4 + Codex Phase 5（错峰启动）+ Claude Code vn.py MVP + Claude Code §25 纸面设计。

**在 Week 1 末可以达到的状态**：
- vn.py PoC 完成（go / no-go 信号）
- Codex Phase 0-1 完成（治理流程上线，UI 上能看到"加入 SOTA 殿堂"按钮）
- Codex Phase 4 进展过半（seed contract schema 落地）
- ST PIT 数据补完最新交易日

### 31.6 一句话结论

**Codex 文档没有阻塞性缺失，可立即按文档进入实施阶段**——7 项补充内容（§31.1）属于"边做边补"或"低优先级"，不阻塞 Phase 0-1-4 启动。

**现阶段（Week 1-2）4 条主线并行启动**：
1. Codex Phase 0-1-4（治理基础 + Master Seed Contract）—— **最高优先级**
2. Claude Code vn.py + miniQMT PoC + 集成 MVP —— **目标 A 关键路径**
3. Codex Phase 5（Model Library）+ ST PIT 数据补齐 —— **基础设施 + 数据修复**
4. Claude Code §25 B+ 纸面设计 + Paper v2 阻断点修复 —— **辅助 / 准备**

**多 alpha 推迟对计划的最大影响**：§25 B+ 紧迫性下降，可以"先纸面设计后实施"——实际编码推迟到 Codex Phase 4-5 完成后启动，节省 Tier 0+ 期间的注意力分散。**整体 6-7 个月日历不变**，但 Tier 1B/2 重心从"多 alpha"移到"公告信号 + 滚动训练 + 实盘准备"。

**当前最关键的执行约束**：Codex Phase 4 (Master Seed Contract) 是所有其他工作的解锁条件，应被视为绝对优先级，2-3 周内完成。

---

## 32. §25 B+ 具体内容 + 启动时机 + 设计阶段测试用例补齐

回答用户在 2026-05-08 的三个具体追问：

> 1. §25 B+ 的具体内容是什么？
> 2. 是否等 Codex 完善 QE 架构后，才开始与 Paper v2 的整合？
> 3. Codex 开发过程是否需要在设计阶段就在自动化流水线中添加测试用例和覆盖？

### 32.1 §25 B+ 具体内容

§25 B+ 在 Codex 治理（§29）落地后的**精确范围**——三层架构 + 七个具体交付物：

#### 32.1.1 三层结构（Codex 已提供 + §25 B+ 补充）

```
┌──────────────────────────────────────────────────┐
│ Codex StrategyPackage v2 manifest (已设计)        │
│ - Frozen Alpha Core: factor + model + seed + ... │
│ - Runtime variant: topk / 算法 / HMM 可变体        │
│ ↓ §25 B+ 直接 reuse 作为 StrategySpec 输入        │
└──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│ §25 B+ Strategy Engine (新建，纯 Python)          │
│ 输入: StrategyPackage v2 manifest + score 数据    │
│       + current_positions + market_context        │
│ 输出: List[OrderIntent]                           │
└──────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ QE Adapter   │ │ Paper Adapter│ │ Live Adapter │
│ (Qlib YAML + │ │ (trading_core│ │ (trading_core│
│ delegate)    │ │ + SimGateway)│ │ + vnpy_xt)   │
└──────────────┘ └──────────────┘ └──────────────┘
```

#### 32.1.2 Strategy Engine 内部职责（精确清单）

**Engine 必须实现**（plain Python，零 Qlib / vn.py 依赖）：

| 模块 | 内容 | 来源 |
| --- | --- | --- |
| `score_to_candidates` | 接受全市场 score → 应用 topk + risk_policy + tradability 过滤 → 输出 candidates | 现有 `selection_center/risk_policy.py` + `tradability.py` 复用 + 现有 `runtime.py` 重构 |
| `compute_weights` | 接受 candidates → 应用 score 加权（softmax / equal / rank / linear） + min/max 约束 → 输出 weights | 复用 `runtime.py:602-664` `_compute_score_weighted_weights` |
| `apply_dynamic_ndrop` | 应用动态 n_drop（threshold_method / max_n_drop / min_n_drop） | 复用 `runtime.py:551-600` `_filter_dynamic_ndrop` |
| `apply_hold_thresh` | 持仓锁定期检查（不允许卖出未达 hold_thresh 天数的持仓） | 复用 `runtime.py:667-679` `_can_sell_under_hold_thresh` |
| `targets_to_intents` | 目标持仓 vs 当前持仓 → rebalance intent 列表（Symbol + Direction + Quantity） | 现有 `paper_trading_v2/day_runner.py` 部分逻辑迁入 |
| `compute_score_combination` | 多 alpha 时应用 combination_rule（weighted_sum / rank_aggregation / meta_learner） | 多 alpha 启动时新加（即用户推迟的 #7） |

**Engine 不实现**（留给 adapter 或外部）：
- 因子/模型推理（已共享于 `inference_engine.py`）
- 撮合 / fill 模拟（QE 用 Qlib，Paper/Live 用 vn.py）
- 行情数据访问（QE 用 Qlib bin，Paper/Live 用 DB / vnpy_xt）
- 订单状态机（OEMS 层，由 vn.py 提供）

#### 32.1.3 三个 Adapter 的精确范围

| Adapter | 输入 | 输出 | 职责 | 估算行数 |
| --- | --- | --- | --- | --- |
| **QE Adapter** | Codex StrategyPackage v2 manifest | Qlib YAML + custom_strategy.py（delegate 到 Engine） | 把 manifest 转成 Qlib 配置；在 Qlib `BaseSignalStrategy.generate_trade_decision` 内部调用 Engine.decide()；保留 Qlib 撮合 | 200-400 |
| **Paper Adapter** | manifest + 当前 sim 账户状态 | OrderIntent 通过 trading_core RPC 提交给 vn.py SimGateway | 读取 manifest / inference 当日 score / 调用 Engine / 翻译为 vn.py OrderRequest / 接收 fill 事件 | 100-200 |
| **Live Adapter** | manifest + 当前实盘账户状态 | OrderIntent 通过 trading_core RPC 提交给 vnpy_xt | 同 Paper Adapter，但 trading_core 内部 broker adapter 是 vnpy_xt 而不是 SimGateway | 100-200 |

**总 Adapter 代码 < 1000 行**——对比 Engine（共享，2000-3000 行），adapter 占约 25-30%。

#### 32.1.4 §25 B+ 七个交付物（可直接用作工作分解）

| # | 交付物 | 工作量 | 依赖 |
| --- | --- | --- | --- |
| 1 | StrategySpec 接口定义（Pydantic 包装 Codex StrategyPackage v2，加 escape hatch `raw_extension` 字段） | **0.5 周**（reuse Codex schema） | Codex Phase 5 完成 |
| 2 | Strategy Engine 核心实现（`score_to_candidates / compute_weights / apply_dynamic_ndrop / apply_hold_thresh / targets_to_intents`） | 2-3 周 | #1 完成 |
| 3 | QE Adapter（Qlib YAML 生成 + custom_strategy delegate） | 2-3 周 | #2 完成 |
| 4 | Paper Adapter（trading_core RPC + SimGateway 集成） | 1-2 周 | #2 + §16 vn.py MVP |
| 5 | Live Adapter（trading_core RPC + vnpy_xt 集成） | 1-2 周 | #2 + 实盘 PoC |
| 6 | 现有代码迁移（`runtime.py:551-679` + `qe_strategies/topk_dropout_rc_qlib.py` 改造为 adapter 调用 Engine） | 2-3 周 | #2 + #3 + #4 完成 |
| 7 | Equivalence 测试矩阵（同 manifest 同日 QE backtest vs Paper vs Live 输出对账，整合 Codex Mode A-F） | 1-2 周 | #6 完成 |
| **合计** | | **9.5-15.5 周（可压缩到日历 5-7 周）** | |

### 32.2 是否等 Codex 完成 QE 架构再开始 Paper v2 整合？

**部分等，部分并行**——精确分层：

#### 32.2.1 立即可并行的工作（不等 Codex）

| Claude Code 工作 | 何时启动 | 是否依赖 Codex |
| --- | --- | --- |
| §16 vn.py + miniQMT PoC（3-5 天连通性） | **立即** | 否 |
| §16 vn.py + Paper v2 集成 MVP（4 周，用现有 manifest） | PoC 通过后 | 否（用 Codex 治理之前的 manifest 跑 demo） |
| §25 B+ 交付物 #1 StrategySpec 接口纸面设计（草拟 Engine 接口） | **立即**（Week 1-2） | 仅参考 Codex 设计文档，不依赖 Codex 代码就绪 |
| Paper v2 当前阻断点修复（live inference preflight / UI 配置开放） | 立即 | 否 |

#### 32.2.2 必须等 Codex 完成的工作

| §25 B+ 交付物 | 必须等什么 | 何时启动 |
| --- | --- | --- |
| #2 Strategy Engine 核心实现 | Codex Phase 4（Master Seed Contract）—— 否则 equivalence 测试无可复现基础 | Codex Phase 4 完成（约 Week 3） |
| #3 QE Adapter | Codex Phase 5（Model Library）—— Adapter 需要新 model_registry schema 引用 | Codex Phase 5 完成（约 Month 2 末） |
| #4-#5 Paper / Live Adapter | §16 vn.py MVP + Codex Phase 2-3（Asset Freezing + retest）—— Adapter 需要冻结资产引用 | Month 2-3 |
| #6 现有代码迁移 | #2 + #3 + #4 完成 | Month 3 |
| #7 Equivalence 测试 | #6 完成 + Codex Mode A-F 框架就绪 | Month 3-4 |

#### 32.2.3 整合时间表

```
Week 1-2:  Codex Phase 0-1-4 启动 + §25 B+ #1 纸面设计 (Claude Code) + vn.py PoC (Claude Code)
Week 3-4:  Codex Phase 4 完成 + Codex Phase 5 启动 + §25 B+ #2 Engine 核心开始 (Claude Code)
                                                    + vn.py MVP 开始 (Claude Code)
Week 5-6:  Codex Phase 5 推进 + §25 B+ #2 Engine 核心完成 + #3 QE Adapter 启动
                                  vn.py MVP 主体推进
Week 7-10: Codex Phase 5 完成 + §25 B+ #3-#4-#5 Adapter 实施
                                  vn.py MVP 完成
Week 11-14: §25 B+ #6 现有代码迁移 + #7 Equivalence 测试
                                    (与 Codex Mode A-F 框架整合)
Week 15+:  #5 公告/财报信号 + 滚动训练 + 实盘准备 (全部自动支持三端)
```

**整体日历仍 6-7 个月**，§25 B+ 大部分嵌入 Tier 1B 内并行做。

#### 32.2.4 关键判断

**不要等 Codex 全部完成再启动 Paper v2 整合**——
- vn.py MVP（§16）独立于 Codex 治理，应立即并行
- §25 B+ 纸面设计可立即做（仅参考 Codex 文档）
- §25 B+ Engine 实现可在 Codex Phase 4 完成后即启（不必等 Phase 5）

**但应等 Codex Phase 4 完成才进入 §25 B+ 实施**——
- 没有 seed 控制无法做 equivalence 测试
- equivalence 测试是 §25 B+ 价值兑现的关键

### 32.3 设计阶段就在自动化流水线添加测试用例 —— 必须做

**用户判断完全正确**——这是 AIstock 已有规则的明确要求。`tests/aistock_validation/catalog/test_levels.md` 已规定 L0-L5 的 trigger / minimum evidence / claim boundary；`docs/codex_project_memory.md` line 762-764 规定 Validation DB + GitHub Issues 是 bug 单源；`feedback_aistock_codex_alignment.md` 第 12 条规定模块开发与测试矩阵同步。

**问题是当前 7 模块测试矩阵覆盖率只有 ~20%（§21.3.1）**——意味着 Codex Phase 0-7 启动时，新功能没有现成测试矩阵可跟随。**必须在每个 Phase 设计阶段同步写测试矩阵**，否则 cross-testing 启用时无 plan 可跑（§21）。

#### 32.3.1 Codex 每个 Phase 必须配套的测试用例

下表给出**最小必备测试用例**（Codex 在 Phase 设计阶段就应当写入对应模块测试矩阵）：

| Phase | 必备测试用例 | L 等级 | 写入文件 |
| --- | --- | --- | --- |
| **Phase 0** 术语对齐 | 文档与代码的术语一致性扫描（grep 旧术语 0 命中） | L0 | `tests/aistock_validation/modules/qe_governance.md`（**新建**） |
| **Phase 1** 手工 SOTA 流程 | UI: "加入 SOTA 殿堂" 按钮可点击；后端 API 创建 `REVIEW_PENDING` 记录；老的"自动 SOTA"不再发生 | L3 | 同上 |
| **Phase 2** Asset Freezing | promote 后资产被复制到 protected 库；sha256 写入 manifest；老路径修改 manifest 应失败 | L2 | `tests/aistock_validation/modules/strategy_package_v2.md`（**新建**） |
| **Phase 3** Original-config retest | 包从 `ASSET_FROZEN` 流转到 `ORIGINAL_RETESTING` → `ORIGINAL_RETEST_PASSED`；retest 失败包不能进 `PAPER_CANDIDATE` | L3 | 同上 |
| **Phase 4** Master Seed Contract | **同 manifest 同 master_seed 跑两次 NAV 差异 < 0.01bp + 持仓 100% 相同**（核心验收）；seed_fragility_score 计算正确性 | **L4**（核心 gate） | `tests/aistock_validation/modules/qe_reproducibility.md`（**新建**） |
| **Phase 5** Model Library 4 层 | model_template / spec / trial / artifact 表 CRUD；老 catalog → 新 registry 视图迁移；lifecycle_event 替代删除 | L2 | `tests/aistock_validation/modules/model_registry.md`（**新建**） |
| **Phase 6** Runtime Variants | variant 独立 hash；不能修改 frozen core；validation 通过才进 PAPER_CANDIDATE | L3 | `tests/aistock_validation/modules/strategy_package_v2.md` |
| **Phase 7** Latest-data + Rolling validation | Mode A-F 各 mode 对同一包跑通；rolling-train 模式产出新 ModelArtifact 进 ORIGINAL_RETESTING | L4 | `tests/aistock_validation/modules/qe_validation_modes.md`（**新建**） |

**关键执行约束**：
- 每个 Phase **PR 必须含两部分**：① 实施代码 ② 对应测试矩阵 + 至少 3 个测试 case
- L4 核心 gate（Phase 4）的"两次跑 NAV 差异 < 0.01bp"是**不可妥协标准**——不通过则该 Phase 不算完成
- 模块测试矩阵新建的 4 个文件（qe_governance.md / strategy_package_v2.md / qe_reproducibility.md / model_registry.md / qe_validation_modes.md）**直接补 §21.3.1 的覆盖率短板**，让覆盖率从 7/30+ 升到 12/30+

#### 32.3.2 §25 B+ 七个交付物的测试用例

§25 B+ 也要遵守同样规则。每个交付物配套测试：

| 交付物 | 测试用例 | L 等级 | 写入文件 |
| --- | --- | --- | --- |
| #1 StrategySpec | Pydantic schema 验证；Codex StrategyPackage v2 → StrategySpec 转换无损；escape hatch 字段正常工作 | L1 | `tests/aistock_validation/modules/strategy_engine.md`（**新建**） |
| #2 Strategy Engine 核心 | 单元测试每个 Engine 函数（score_to_candidates / compute_weights / apply_dynamic_ndrop / apply_hold_thresh / targets_to_intents） | L1 | 同上 |
| #3 QE Adapter | adapter 生成的 Qlib YAML 与现有 QE 输出在小样本下 NAV 差异 < 5bp | L3 | `tests/aistock_validation/modules/qe_paper_consistency.md`（**新建**） |
| #4 Paper Adapter | trading_core 调用 Engine.decide() 后 OrderIntent 正确转为 vn.py OrderRequest；fill 事件正确回流 | L2 | 同上 |
| #5 Live Adapter | 同 Paper Adapter，但用 vnpy_xt（仿真账户） | L3 | 同上 |
| #6 现有代码迁移 | 迁移前后 Paper v2 单日跑同 manifest 输出 100% 一致 | L4 | 同上 |
| #7 Equivalence 测试矩阵 | 5+ baseline 场景：QE backtest vs Paper vs Live 同日 NAV/持仓/换手 diff 在容忍度内 | L4 | 同上 |

#### 32.3.3 与 Cross-testing 的衔接（§20-§21）

- Codex Phase 实施 → Codex 写测试矩阵（设计阶段） → Codex 写代码 → **Claude Code 作为 cross-tester 执行测试**（cross-testing 模式）
- §25 B+ 实施 → Claude Code 写测试矩阵（设计阶段） → Claude Code 写代码 → **Codex 作为 cross-tester 执行**

这正是 §20.5 模块归属矩阵的应用——**测试矩阵覆盖度从 7/30+ 升到 12/30+ 的工作恰好通过这条路径自然推进**，不需要单独的"补测试矩阵专项"。

### 32.4 一句话结论

**§25 B+ 具体内容**：在 Codex StrategyPackage v2 manifest 之上新建一个共享 Strategy Engine（plain Python，复用现有 `runtime.py` 逻辑 + `selection_center` 风险策略）+ 三个薄 adapter（QE Qlib / Paper SimGateway / Live vnpy_xt），共 7 个交付物，9.5-15.5 周（可压缩到日历 5-7 周）。

**启动时机**：**部分立即并行，部分等 Codex Phase 4 完成**——纸面设计 + vn.py MVP + Paper v2 阻断点修复立即启动；Engine 实施等 Codex Phase 4 done（约 Week 3）；Adapter 实施等 Codex Phase 5 done（约 Month 2 末）。**不要等 Codex 全部完成才启动**——会浪费 1-2 个月并行机会。

**设计阶段加测试用例**：**必须做，且不可妥协**。每个 Phase / 交付物 PR 必须含①实施代码 ②对应测试矩阵 + 至少 3 个测试 case；Phase 4 (Master Seed Contract) 的"两次跑 NAV 差异 < 0.01bp"是核心 L4 gate；这个做法直接把 §21.3.1 模块测试矩阵覆盖度从 7/30+ 推到 12/30+，为后续 cross-testing 启用打基础。

**最关键的执行 anchor**：Phase 4 (Master Seed Contract) 完成后，§25 B+ 才能真正进入 Engine 实施——这是 Tier 0+/Tier 1B 之间的硬时序分界，2-3 周内由 Codex 完成是关键。

---

## 33. Codex 在长期 feature 分支上开发，main 不受影响

回答用户在 2026-05-08 的实操问题：

> Codex 开发是否可以在现有仓库中新建一个分支，在完成整体验证前暂时不合入现有的 main 仓库，确保开发期间，main 分支可以继续执行实验？

**完全可以，且强烈推荐**——这是工程标准做法（long-running feature branch 模式）。下面给出具体分支策略 + 数据隔离 + 合入条件 + 与 Claude Code 并行的协调方式。

### 33.1 推荐的分支结构

```
main（生产稳定，实验持续运行，UI 8001 端口跑当前代码）
 │
 ├─ codex/qe-governance-integration-20260508（长期集成分支，Codex 主导）
 │   ├─ codex/qe-phase-0-terminology-20260508       (Phase 0 worktree)
 │   ├─ codex/qe-phase-1-manual-sota-flow-20260508  (Phase 1)
 │   ├─ codex/qe-phase-4-seed-contract-20260508     (Phase 4，最高优先级)
 │   ├─ codex/qe-phase-5-model-library-20260510     (Phase 5)
 │   ├─ codex/qe-phase-2-asset-freezing-20260520    (Phase 2)
 │   ├─ codex/qe-phase-3-original-retest-20260601   (Phase 3)
 │   ├─ codex/qe-phase-6-runtime-variants-20260615  (Phase 6)
 │   └─ codex/qe-phase-7-rolling-validation-20260701 (Phase 7)
 │
 ├─ claude/paper-v2-vnpy-mvp-20260508（Claude Code 主导，独立于 Codex 集成分支）
 │   ├─ claude/trading-core-daemon-20260508
 │   ├─ claude/paper-v2-trading-core-client-20260508
 │   ├─ claude/paper-v2-daemon-event-log-20260508
 │   └─ claude/paper-v2-ui-trading-core-20260508
 │
 └─ claude/strategy-engine-design-20260508（§25 B+ 纸面设计 + 后续实施）
```

**要点**：
- `main` **保持稳定**——生产 8001 / 现有实验 / UI 全部不受影响
- `codex/qe-governance-integration-*` **长期集成分支**——Codex 各 Phase 完成后合入此分支，整体验证后才合 main
- Codex 各 Phase 在各自子 worktree 开发，完成 L0-L3 测试后合入集成分支
- Claude Code 的 vn.py 工作**独立于 Codex 集成分支**（§33.5 详述）

### 33.2 Codex 集成分支的工作流

```
1. Codex 在 codex/qe-phase-X-yyyymmdd 完成 Phase X 实施
   ├── 写代码
   ├── 写测试矩阵（Codex 文档附录 A.5 强制要求）
   ├── L0 守护扫描通过
   └── L1 单测通过

2. Codex 提 PR：codex/qe-phase-X → codex/qe-governance-integration-20260508
   ├── Cross-tester（Claude Code）在集成分支上跑 L2/L3
   ├── bug 进 GitHub Issues + Validation Center
   ├── 修复迭代直至 L2/L3 通过
   └── 合入集成分支

3. 集成分支定期跑全套 L4 验证（含 Mode A-F 全模式）
   └── 失败立即修复，不允许带病演进

4. 所有 Phase 0-7 完成 + 集成分支跑通 §A.5.2 Phase 4 L4 核心 gate
   └── 准备合 main（§33.6 合入条件）

5. 合 main 后：
   ├── 用户授权重启生产 8001（按 codex_project_memory.md line 314 规则）
   ├── 跑生产数据 smoke test
   └── 监控 24 小时无异常
```

### 33.3 数据 / 数据库隔离策略

#### 33.3.1 数据库层

Codex 集成分支 Phase 4-5 涉及 schema 变更（Master Seed Contract 字段、Model Library 4 层新表）。**禁止直接动生产 DB**。隔离方式：

| 选项 | 内容 | 推荐度 |
| --- | --- | --- |
| A：使用 dev 数据库 | 在 PostgreSQL 同实例创建 `aistock_dev` 独立 DB；集成分支代码默认连 dev 库 | **推荐**——简单、可重置 |
| B：纯 additive 迁移 + 兼容 | 集成分支所有 schema 变更只增不减；老表/字段保留 | **必须做**（与 A 叠加） |
| C：feature flag 控制启用 | 新表/字段虽然存在但默认不启用；测试时显式开启 | 复杂度高；本场景不必 |

**具体做法**：
- Codex Phase 4-5 在集成分支跑测试时连 dev DB
- 生产 DB 不被 schema 变更影响（main 上的 schema 不动）
- 合 main 后再做生产 DB 迁移（**additive 优先**——不删表/字段；老 manifest 仍可用）

#### 33.3.2 工作区资产隔离

Codex Phase 2 Asset Freezing 涉及向 protected 库复制资产。生产路径与集成分支路径必须分开：

```
生产: F:\Dev\AIstock\rdagent_assets\strategy_package_runtime\          ← main 在用
集成: F:\Dev\AIstock\rdagent_assets\strategy_package_runtime_dev\      ← 集成分支用
       或
       F:\Dev\AIstock_worktrees\qe-governance-integration-*\rdagent_assets\...
```

#### 33.3.3 ST PIT 数据共享

ST PIT spans / suspend_d / stk_limit / 行情数据等**只读数据**集成分支可与 main 共享——这些不会被开发过程破坏。Codex Phase 4-5 不修改这些表。

### 33.4 现有 main 上的实验如何继续运行

main 上的工作完全不受影响：

| 项目 | main 上的状态 | 集成分支期间的影响 |
| --- | --- | --- |
| 生产 FastAPI 8001 | 跑当前 main 代码 | **零影响**——8001 不重启 |
| 已运行的 QE 实验 | 跑当前 manifest schema v1 | 零影响（集成分支用 dev DB） |
| RD-Agent worker | WSL 上跑当前代码 | 零影响 |
| Selection Center / Paper v2 现有功能 | main 代码可用 | 零影响 |
| ST PIT 数据补齐（Codex P0-2） | 直接在 main 推进 | **正常推进**——这是数据修复，不动代码 schema |
| 用户 UI 操作 | 跑 main 代码 | 零影响 |

**唯一需要协调的事**：用户要在 main 上做小补丁（如修 bug）时，Codex 集成分支需要定期 rebase main 保持同步——见 §33.5。

### 33.5 与 Claude Code 并行工作（vn.py MVP）的协调

| Claude Code 工作流 | 是否依赖 Codex 集成分支 | 处理 |
| --- | --- | --- |
| `claude/paper-v2-vnpy-mvp-20260508`（vn.py PoC + 集成 MVP） | **不依赖**——用 main 上现有 ST PIT manifest 跑 demo | Claude Code 直接在 main 基础上开分支 |
| `claude/strategy-engine-design-20260508`（§25 B+ 纸面设计） | 不依赖（仅参考 Codex 文档） | 同上 |
| §25 B+ Engine 实施 | **依赖** Codex Phase 4 完成 | 等 Codex Phase 4 合入集成分支后启动 |
| §25 B+ QE Adapter 实施 | **依赖** Codex Phase 5 完成 | 等 Codex Phase 5 合入集成分支后启动 |

**关键协调约定**：
1. **Claude Code 不主动合入 Codex 集成分支**——避免污染 Codex 工作面
2. **§25 B+ Engine/Adapter 需要 Codex 代码时**：Claude Code 单独 fetch Codex 集成分支到本地、读 schema 设计参考——但代码仍在 `claude/*` 分支提交
3. **最终合 main 时**：Codex 集成分支先合 main → Claude Code 的 §25 B+ 分支再合 main（或反序，但不能同时）
4. **如果 main 期间发生小补丁**：双方分支都要定期 rebase main 保持同步（建议每 2 周一次）

### 33.6 集成分支 → main 的合入条件（merge gate）

集成分支只在满足下列**全部条件**时才允许合 main：

| # | 条件 | 验收方式 |
| --- | --- | --- |
| 1 | Phase 0-7 全部完成（含测试矩阵） | Codex 文档附录 A.5 全表 ✓ |
| 2 | **Phase 4 L4 核心 gate 通过**：同 manifest 同 master_seed 跑两次 NAV 差异 < 0.01bp + 持仓 100% 相同 | `tests/aistock_validation/history/qe_reproducibility/...l4_seed_contract_strict.json` |
| 3 | 集成分支跑完整 L4 通过 | `nox -s aistock_validation_l4` 通过 |
| 4 | 现有 main 上的 4 个 LEGACY_NON_ST_PIT 包迁移决策已落实（标 legacy / 推进迁移 / 退役任一） | Phase 1 内决定，集成分支验证 |
| 5 | 老 manifest schema v1 兼容性测试通过（main 上现有 manifest 在新代码下仍可读） | Phase 5 测试矩阵覆盖 |
| 6 | 生产 DB 迁移脚本已写好 + 在 dev DB 上 dry-run 通过 + 有回滚 SQL | Phase 5 交付物 |
| 7 | 用户最终签字（含合并时间窗、生产 8001 重启时间窗） | 用户当回合明确确认 |

**任一条不满足，集成分支不合 main**。这是金融 IT 的硬性规则，不能因为"想快点合"而妥协。

### 33.7 现有数据 / manifest 的迁移计划

合 main 后，main 上现有的 4 个 LEGACY_NON_ST_PIT manifest 需要迁移：

| Manifest | 当前状态 | 迁移目标 | 时机 |
| --- | --- | --- | --- |
| 4 个 LEGACY_NON_ST_PIT | manifest schema v1 + ST PIT 合约缺失 | 标记 `lifecycle_status=LEGACY` + `seed_policy=unset_legacy` + `protected_asset=true` | Phase 1 末（合主 main 时） |
| 已有 selection artifact | v1 格式 | 保留作诊断；不晋级到新流程 | Phase 1 末 |
| 历史 QE 实验记录 | 现有 schema | 保留只读；新实验产出 v2 schema | 自然演进 |

**迁移本身是 additive**——老数据保留、新流程不读老数据；不存在"全库替换"风险。

### 33.8 回滚计划（万一合 main 后出问题）

虽然有 §33.6 的 7 条 merge gate，仍要准备回滚：

| 故障类型 | 回滚动作 |
| --- | --- |
| 生产 8001 启动失败 | `git revert <merge-commit>` + 重启 8001 + 监控 |
| 现有 manifest 在新代码下读取失败 | 同上 + 紧急修 v1 reader 兼容性 |
| 新增 model_registry 表 schema 错误 | DB 迁移 down script + 代码 revert |
| 演进 / 实验跑不通 | revert + 修 + 重新走 merge gate |

**revert 要快**——金融 IT 不允许"先观察一下"。出问题立即 revert，再分析根因。

### 33.9 推荐的具体启动步骤

按今天起算（Week 1 Day 1）：

```
Day 1 (用户 + Codex):
  1. 用户授权 Codex 创建集成分支:
     git checkout -b codex/qe-governance-integration-20260508
     git push -u origin codex/qe-governance-integration-20260508
  2. 用户在 GitHub 设置分支保护（PR required + L0/L1 必过）
  3. 用户决定 dev DB 命名（推荐 aistock_dev）+ 创建

Day 1 (Codex):
  4. Codex 启动 worktree: codex/qe-phase-4-seed-contract-20260508
  5. Codex 同时启动 worktree: codex/qe-phase-0-terminology-20260508
  6. Codex 同时启动 worktree: codex/qe-phase-1-manual-sota-flow-20260508

Day 1 (Claude Code，独立):
  7. Claude Code 启动 worktree: claude/paper-v2-vnpy-mvp-20260508
  8. 第一步: vn.py + miniQMT PoC（3-5 天）

Week 1-2:
  Codex Phase 0-1-4 推进; 合入集成分支后 Claude Code 跑 cross-test
  Claude Code vn.py PoC + MVP 启动

Week 3-6:
  Codex Phase 5 启动; Phase 4 完成
  Claude Code §25 B+ Engine 设计 (paper)
  Claude Code vn.py MVP 主体推进

Week 7-10:
  Codex Phase 2-3 + 6-7 推进
  Claude Code vn.py MVP 完成 + §25 B+ 实施

Week 11-15:
  集成分支跑完整 L4 + Mode A-F
  §33.6 merge gate 7 条逐一验证
  通过后合 main + 用户授权重启 8001

Week 16+:
  正式启动 #5 公告信号 / 滚动训练 / 实盘准备（基于稳定的新 main）
```

### 33.10 一句话结论

**Codex 在 `codex/qe-governance-integration-20260508` 长期集成分支开发是工程标准做法、强烈推荐**——main 上的 8001 / 实验 / 用户操作完全不受影响。

**关键操作约定**：
- 集成分支用 dev DB（`aistock_dev`）—— schema 变更不污染生产 DB
- 集成分支用独立 protected 库路径 —— 资产冻结不污染生产路径
- main 上的 4 个 LEGACY_NON_ST_PIT manifest 走 additive 迁移 —— 老数据保留不删
- 集成分支 → main 走 §33.6 7 条 merge gate（含 Phase 4 L4 核心 gate 不可妥协）
- 准备 revert SQL + revert plan（金融 IT 硬性要求）

**Claude Code vn.py MVP 工作独立**——不污染 Codex 集成分支；最终合 main 时双方分支序列合入，不能同时。

**预期合 main 时机**：Week 11-15（约 Month 3 末）—— 与 §29.6.1 / §31 时间表一致。

可以从用户授权 Codex 创建集成分支 + 设置分支保护 + 创建 dev DB 三件事开始落地。

---

## 34. 资源受限场景：使用生产 DB + 严格 additive only

回答用户在 2026-05-08 的实操约束：

> 目前可能没有足够空间或资源支持分支的数据库，是否能继续使用生产数据库？只增加表或字段，不删除现有结构？是否可以实现？

**完全可行，且有标准工程模式（expand-contract pattern）**。下面给出严格规则 + 风险控制 + 与 §33 dev DB 方案的取舍。

### 34.1 直接结论

**可以直接使用生产 DB**，前提是 Codex 集成分支严格遵守"只增不改不删"的 6 条硬规则。这是业界标准做法（Stripe / Shopify / GitHub 等大型系统升级 schema 都用这种模式）。

**风险等级**：低（但前提是规则严格执行；任一条破规则风险立即升高）。

**节省的资源**：~10-50GB（取决于 main DB 当前大小，无需复制一份）。

### 34.2 必须遵守的 6 条硬规则

| # | 规则 | 例子 |
| --- | --- | --- |
| 1 | **新表必须独立 schema 命名空间** | `model_registry.model_template` 等放 `model_registry` schema；`strategy_pkg.promotion_review` 放 `strategy_pkg` schema —— **不能新建表直接落到 `public` schema 与现有表混在一起** |
| 2 | **新字段必须 NULL 或有 DEFAULT** | `ALTER TABLE strategy_pkg.package ADD COLUMN seed_policy TEXT NULL` 或 `... DEFAULT 'unset_legacy' NOT NULL` —— **不能加 NOT NULL 无 DEFAULT** |
| 3 | **不修改 / 不删除任何现有字段或表** | 即使发现旧字段命名不好或类型不对，也只能"新增更好的字段 + 双写一段时间 + 后续合 main 时切换 reader" |
| 4 | **新表不引用现有表带 CASCADE 删除** | `FOREIGN KEY ... ON DELETE CASCADE` 禁用——即使是新表引用老表也不行；防止意外删除连锁 |
| 5 | **集成分支只创建新记录，不修改现有记录** | 集成分支测试用**新 manifest_id / package_id**，避免与生产实验冲突；现有 4 个 LEGACY_NON_ST_PIT 包**只读不改** |
| 6 | **写入新字段的查询必须显式判空** | 老代码 SELECT * 仍可工作（新字段 NULL 不影响）；新代码读新字段时检查 IS NULL → 走 fallback 路径 |

**任一条破例都会破坏隔离**——必须写入 Codex 文档作为硬约束。

### 34.3 各 Phase 的 additive 落地方案

#### Phase 4 Master Seed Contract（schema 改动）

```sql
-- 在现有 strategy_pkg.package 表加可空字段
ALTER TABLE strategy_pkg.package ADD COLUMN seed_policy TEXT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN master_seed BIGINT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN seed_sequence JSONB NULL;
-- (其他子 seed 字段同理)

-- 新建独立审计表
CREATE TABLE strategy_pkg.seed_fragility_score (
    package_id TEXT PRIMARY KEY REFERENCES strategy_pkg.package(package_id),
    metric_mean_by_seed JSONB,
    seed_sensitivity_score DOUBLE PRECISION,
    -- ... (per Codex 文档第 4 章)
    created_at TIMESTAMPTZ DEFAULT now()
);
```

**生产侧零影响**：老代码不知道新字段存在，照常工作；老 manifest 新字段为 NULL（视为 `seed_policy=unset_legacy` 默认）；`seed_fragility_score` 是新表，不影响现有查询。

#### Phase 5 Model Library 4 层（最大变更）

完全新建 `model_registry` schema，不动现有 `aistock_model_catalog`。

```sql
CREATE SCHEMA IF NOT EXISTS model_registry;

CREATE TABLE model_registry.model_template (...);
CREATE TABLE model_registry.model_spec (...);
CREATE TABLE model_registry.model_trial (...);
CREATE TABLE model_registry.model_artifact (...);
CREATE TABLE model_registry.model_lifecycle_event (...);
```

**生产侧零影响**：`aistock_model_catalog` 完全不动；集成分支测试时往 `model_registry.*` 写新数据；合 main 后新 QE 实验产出的模型走 `model_registry`，老 catalog 保留作兼容视图（per Codex 文档 line 828-868）。

#### Phase 1-2 SOTA 流程 + Asset Freezing

新增表 `strategy_pkg.promotion_review` / `strategy_pkg.package_validation_run` / `strategy_pkg.package_runtime_variant` 等（per Codex 文档第 5.4 章），在已有 `strategy_pkg` schema 下扩展。**不动 `strategy_pkg.package` 现有字段**。

#### Phase 6-7 Runtime Variants + Latest-data Validation

扩展 `strategy_pkg.package_validation_run` 表（在 Phase 5 已建好的基础上加 mode 字段）。**不动其他表**。

### 34.4 必须避免的破规则做法

| 破规则做法 | 后果 |
| --- | --- |
| `ALTER TABLE ... DROP COLUMN ...` | 生产代码崩溃 |
| `ALTER TABLE ... ALTER COLUMN ... TYPE ...` | 生产代码读到不期望的类型 |
| 给老表加 NOT NULL 无 DEFAULT 字段 | 老 INSERT 缺字段直接报错 |
| 删除现有索引 | 生产查询性能塌方 |
| 在集成分支修改现有 package 记录 | 与 main 上的实验冲突 |
| 给现有表加 CASCADE 外键 | 删除某行连锁删除 |
| 集成分支跑测试时复用生产 package_id | 与生产并发写冲突 |

### 34.5 集成分支测试的"新 ID 隔离"策略

为避免与生产实验冲突，集成分支测试必须用专属 ID 命名空间：

| 资源 | 生产命名 | 集成分支命名 |
| --- | --- | --- |
| package_id | `pkg_xxx` | `pkg_dev_xxx` 或 `pkg_test_yyyy` |
| manifest_id | `mfst_xxx` | `mfst_dev_xxx` |
| qe_task_id | `qe_yyyymmdd_xxxx` | `qe_dev_yyyymmdd_xxxx` |
| protected asset 路径 | `rdagent_assets/strategy_package_runtime/<sha>/` | `rdagent_assets/strategy_package_runtime_dev/<sha>/` |

**集成分支代码读时**：可以查所有 ID（含生产）；**写时**：只允许写 dev/test 前缀的 ID。

### 34.6 与 §33 dev DB 方案的取舍

| 维度 | dev DB（§33.3.1） | 生产 DB additive only（本节 §34） |
| --- | --- | --- |
| 磁盘占用 | +生产 DB 等量（10-50 GB） | +新表（~ 几 GB） |
| 设置成本 | 需要新建 DB + 复制数据 | 无 |
| 风险 | 几乎为零 | 低（前提是 6 条硬规则严格执行） |
| 测试数据隔离 | 物理隔离 | 逻辑隔离（ID 命名 + 新 schema） |
| 误操作恢复 | 重建 dev DB | 需要 revert SQL |
| 推荐场景 | 资源充足 | **资源受限——本节方案** |

### 34.7 必须补充到 Codex 流程的检查清单

每次 Codex Phase X PR 合入集成分支前，Cross-tester（Claude Code）按下列清单核对：

- [ ] 没有 `ALTER TABLE ... DROP COLUMN`
- [ ] 没有 `ALTER COLUMN TYPE`
- [ ] 新字段都 NULL 或有 DEFAULT
- [ ] 新表都在独立 schema（`model_registry` / `strategy_pkg` 等），不在 `public`
- [ ] 没有引入 CASCADE 外键到现有表
- [ ] 测试用 dev/test 前缀 ID
- [ ] 不修改现有记录字段值（除 Phase 1 一次性的 LEGACY 标记 + 必须用户授权）

任一条不符合 → PR 不合 → 修复 → 重审。**这条已落入 §29 / 附录 A.4 cross-testing 流程**。

### 34.8 磁盘空间预估（6 个月 Phase 0-7 累计）

| 新表 | 预估行数 | 预估大小 |
| --- | --- | --- |
| `model_registry.model_template` | 5-20 | < 1 MB |
| `model_registry.model_spec` | 50-200 | < 10 MB |
| `model_registry.model_trial`（演进 6 个月累计） | 5000-20000 | 100-500 MB |
| `model_registry.model_artifact`（仅元数据；权重文件存盘） | 5000-20000 | 50-200 MB |
| `model_registry.model_lifecycle_event` | 10000-50000 | 50-200 MB |
| `strategy_pkg.promotion_review` | 50-200 | < 10 MB |
| `strategy_pkg.package_validation_run` | 500-2000 | 50-200 MB |
| `strategy_pkg.package_runtime_variant` | 100-500 | 10-50 MB |
| `strategy_pkg.seed_fragility_score` | 50-200 | < 10 MB |
| **总计 DB 新增** | | **~300 MB - 1.2 GB** |

**注意**：模型权重文件本身（保存在文件系统而非 DB）才是大头——单个 LGB / NN 模型 10-500 MB，6 个月累计可能 **50-500 GB**。这部分存在 protected 库（`F:\Dev\AIstock\rdagent_assets\`），需要确认所在磁盘有足够空间。

**操作建议**：
- 检查 `F:\` 当前剩余空间 + 当前 `rdagent_assets/` 占用
- 如剩余 < 200 GB，应用旧权重清理策略（per Codex 文档 line 220 `RETIRED` 终态可归档冷存储）

### 34.9 一句话结论

**生产 DB + additive only 完全可行**，业界标准做法。**关键是 §34.2 的 6 条硬规则**：
1. 新表独立 schema
2. 新字段 NULL 或 DEFAULT
3. 不改不删现有结构
4. 不加 CASCADE 外键
5. 测试用新 ID 命名空间
6. 老 SELECT * 仍能工作

**与 §33 dev DB 方案相比**：节省 10-50 GB，风险接近（规则严格执行下），适合资源受限场景。

**风险防控**：每次 Codex Phase PR 合入集成分支前，Cross-tester 按 §34.7 检查清单核对。

**额外磁盘占用**：DB 约 300 MB - 1.2 GB（6 个月累计）；**模型权重文件 50-500 GB（文件系统，不在 DB）才是大头**——需检查 `F:\` 空间。

可以从用户确认磁盘空间 + Codex 创建集成分支（不需要 dev DB）+ Codex 文档增补 §34.2 6 条硬规则三件事开始落地。

