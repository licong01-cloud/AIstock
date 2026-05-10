# Paper v2 UI 简化测试矩阵

日期：2026-05-09
归属：Claude Code 工作面（前端 / `frontend/src/app/paper-v2/`）
设计依据：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §1 + audit §8.3 用户决策（A 任务向导 / B 角色拆分 / C 重命名+折叠）+ `broker_backend_switch_flow_20260509.md` §6.3（typed error UI 映射）

## 模块定位

paper v2 UI 简化覆盖：
- 单页交互元素过多（P1-C：paper-v2/page.tsx 174 行 ~18 元素；selection/page.tsx 468 行 ~25+；run-console/page.tsx 722 行 ~45+）
- 内部哈希、英文枚举直接展示（P1-D：packages/page.tsx 322-323；paper-v2/page.tsx 158；selection/page.tsx 391）
- 错误诊断大量 JsonPanel dump（P1-E：run-console/page.tsx 566/643）
- 冻结字段 vs 可调字段无视觉区分（P1-F）
- 7 个平级 tab 无主从引导（P1-G：layout.tsx 8-16）

| 维度 | 取值 |
| --- | --- |
| 模块 ID | `ui_simplification` |
| 风险等级 | medium-high（UI 错误显示影响生产可用性；元素过多影响用户决策准确性） |
| 工作面 | Claude Code（`frontend/src/app/paper-v2/` + 共享前端组件） |
| 是否触动 main | UI 改动走标准 PR；本矩阵驱动 Playwright L3 |

## L0 静态守卫

L0 trigger：paper v2 frontend 任一文件变更 / 新加 i18n key / 新加 typed error UI 映射。

- L0-G1：禁止 `JsonPanel` / 原始 JSON dump 给用户（P1-E 反模式）
- L0-G2：禁止英文枚举值直接展示（必须经 `STATUS_LABELS` / i18n 映射）
- L0-G3：禁止 `alert()` / `window.confirm()` 等浏览器原生 dialog 承载业务 actions
- L0-G4：i18n key 统一前缀 `broker_error.*`（broker 维度）/ `paper_v2.*`（业务维度）
- L0-G5：所有 typed error UI 渲染必须按 §6.3 表（页面级 / banner / 系统模态 / 内嵌 banner）

pass criteria：
- ESLint + tsc 通过
- semgrep 检查 `JsonPanel` 用法（仅允许在开发者诊断面板）
- i18n key 命名规范扫描通过

## L1 单能力

L1 trigger：单个组件 / 单个 i18n key / 单个 typed error UI 映射变更。

### L1-C1：StatusBadge 中文映射完整
- 验所有 paper v2 出现的 status 字段（`pending / running / completed / failed / cancelled / preflight_failed` 等）都有中文映射
- `STATUS_LABELS` 在列 / 下拉 / 错误条 / banner / toast 全部生效（不仅在 StatusBadge 内）
- pass：`grep "STATUS_LABELS"` 出现位置覆盖所有 status 显示场景

### L1-C2：哈希 / artifact_id / sha256 用户向显示
- packages/page.tsx 等位置不直接显示长 hash（按 P1-D）
- 使用前 8 字符 + tooltip 完整 hash 模式
- 复制按钮可拷整 hash
- pass：UI 中 ≥ 16 字符 hash 字符串 0 命中（除 tooltip / 复制源）

### L1-C3：i18n key 命名规范（broker_error.* / paper_v2.*）
- 所有 broker typed error UI 用 `broker_error.<error_class_lower>.<title|body|actions>`
- 业务字段 / 操作 / 状态用 `paper_v2.<page>.<field|action|state>`
- pass：i18n 文件中 unknown key 0 命中

### L1-C4：typed error 渲染 forbidden flags（§6.3）
- `BrokerCompatibilityMismatchError` 渲染 → `forbidOverride: true`，无"强行继续"按钮
- `MiniQMTSingletonViolation` 渲染 → `forbidRetry: true`，系统错误模态
- `BrokerMarketSourceMismatchError` 渲染 → `autoFixOption: true`，提供"自动切换"按钮
- pass：4 个错误类的 ERROR_UI_MAP 配置严格符合 §6.3

### L1-C5：error.context 字段完整渲染
- 7 个 context 字段（package_id / broker_compatible_value / target_backend_id / occupying_portfolio_name / error_id / allowed_set / given_source）
- UI 必须把每个字段渲染到对应位置（中文文案 + 实际值）
- 任一字段缺失时 → UI 显示"<字段名> 缺失"占位（不允许静默隐藏）
- pass：7 字段 100% 显示路径覆盖

### L1-C6：冻结字段 vs 可调字段视觉区分（P1-F）
- 冻结字段（如 `frozen_alpha_core` / `manifest_sha256` / `broker_compatible`）UI 显示 readonly 风格 + lock icon
- 可调字段（`topk` / `n_drop` / `threshold_overlay` 等）UI 显示 active 风格
- 提交后 readonly 字段不应被后端拒收（不再"看似可改"）
- pass：每页 readonly 字段数 + active 字段数 = 总字段数；视觉区分明显（contrast ≥ 4.5:1）

## L2 组件 / API / DB 流

L2 trigger：UI 多组件协作；后端错误 → 前端渲染。

### L2-F1：BrokerCompatibilityMismatchError 端到端渲染
- 触发后端 `BrokerCompatibilityMismatchError`（spec.broker_compatible="LocalSim_only" + portfolio.broker.backend_id="minqmt_sim"）
- API 返回 typed error JSON（含 error_class / context dict）
- 前端 ERROR_UI_MAP 路由到页面级渲染
- 显示中文标题/内文/按钮（§6.3）+ §3.6.5 文档链接
- pass：从 API 错误返回到 UI 渲染完成 ≤ 1.5s；不出现 Python traceback / 英文枚举

### L2-F2：BrokerMarketSourceMismatchError + autoFix 路径
- 触发 LocalSim 配 MINIQMT_REALTIME → typed error
- UI 显示内嵌 banner + "自动切换为允许的行情通道"按钮
- 点击 autoFix → 自动切换到 TDX_REALTIME 并重新提交
- pass：autoFix 路径成功；切换后无 ledger / portfolio 漂移

### L2-F3：MiniQMTSingletonViolation 系统模态
- 注入进程内已有 MiniQMTSimBroker → 构造第二个 → typed error
- UI 显示系统错误模态（不允许 dismiss-and-retry）
- 显示 error_id 让用户能复现到日志
- pass：模态无重试按钮；error_id 与日志 session id 一致

### L2-F4：i18n 缺 key 失败模式
- 注入未在 i18n 文件中定义的 key
- UI 显示"<key>"占位（不静默 fallback 到英文）
- pass：UI 不出现未翻译英文（除技术字段如 sha256）

## L3 模块 UI/API 回归（Playwright）

L3 trigger：UI 模块改动后 paper v2 全 UI 回归。

### L3-I1：paper v2 总览 + 7 个 tab 主从引导（P1-G）
- 主从引导：总览 / 决策 / 监控 三组（按 audit §8.3 选项）
- pass：tab 顺序符合主从结构；当前 tab 高亮；步数 ≤ 3 完成主流程

### L3-I2：selection center 一键流程（P0-C / P1-C）
- 选 package → 配置 → 一键提交 → selection 结果展示
- 元素数 < 15（vs 原 25+）
- pass：步数 ≤ 3；UI 元素 < 15；不出现"看似一键实际 8 步"

### L3-I3：run-console 错误诊断（P1-E）
- 触发 5 类典型错误（broker / selection / data / risk / execution_algo）
- 每类 UI 显示中文向用户内文 + 不出现 JsonPanel raw dump
- 开发者可点"查看技术详情"展开 raw context（默认折叠）
- pass：默认视图无 raw JSON；技术详情可选展开；错误归因清晰

### L3-I4：BrokerCompatibilityMismatchError 完整 UX 路径
- 用户尝试在 LocalSim_only 包上选 MiniQMTSim portfolio
- UI 显示页面级阻断 + 3 个 actionable（选其他包 / 改 portfolio / 升级流程）
- 点击"升级流程" → 跳转到 §3.6.5 文档
- pass：UX 路径无死胡同；用户能从根因解决；不允许"取消" 是唯一选项

### L3-I5：跨页持久化 / 刷新一致性
- 各页操作后刷新 / 重开 → 状态持久化
- 不出现"提交成功但刷新后看不到"
- pass：所有 paper v2 页 refresh 后状态一致

## Pass Criteria 汇总

| 等级 | 必须项 |
| --- | --- |
| L0 | ESLint + tsc + semgrep + i18n key 规范全绿 |
| L1 | 6 类组件级（StatusBadge / hash / i18n / typed error flags / context / 冻结视觉）单能力通过 |
| L2 | 4 类典型 typed error 端到端渲染 + i18n 缺 key 路径 |
| L3 | Playwright 主从引导 + 一键流程 + 错误诊断 + UX 路径 + 跨页持久化（5 case） |

## 失败处理预期

- L0 失败 → 阻断 UI PR；先修 lint / i18n
- L1 失败 → 该组件级未到位；先修对应组件
- L2 失败 → typed error 端到端断裂；优先修复（影响生产可用性）
- L3 失败 → UX 不达 audit §1 简化目标；阻断 release

## 与 Codex 模块的边界

| 不属于本模块（Codex / 其他范围） | 落地位置 |
| --- | --- |
| 后端 typed error 类定义（`errors.py`） | `trading_core.md`（同目录） |
| Strategy Engine 决策侧 typed error（`strategy_engine_design_20260508.md` §10.1） | `strategy_engine.md`（同目录） |
| typed error → 中文 UI 映射 source of truth（`broker_backend_switch_flow_20260509.md` §6.3） | 设计文档（已 task #17 完成）|
| QE / Codex 后台 UI（QE 实验列表 / archive） | `qe.md` / `qe_archive.md`（已有） |
| Validation Center UI 自身 | `validation_center.md`（已有） |

本模块覆盖 paper v2 UI 简化；**不**修后端 typed error 定义、**不**改后端 API schema、**不**改 QE 后台 UI。

## 取材源

- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §1 + §8.3
- `docs/architecture/broker_backend_switch_flow_20260509.md` §6.3 typed error 中文 UI 映射
- `frontend/src/app/paper-v2/` 现有页面（layout.tsx / page.tsx / packages / selection / portfolios / running 等）
- `docs/standards/cross_test_framework_template_20260508.md` §2.5.4 typed error UI 映射回引（v0.4.1）
- task #18 (B 前 3 项 UI 简化) commit history

## Deferred Scope

- audit §8.3 用户决策具体方案（A 任务向导 / B 角色拆分 / C 重命名+折叠）的最终选择 → 待用户决定后本矩阵 v1.1 增量
- 主体导航重构（顶部 nav / 左侧侧栏 / 底部 tab 三选）→ 跨整个 AIstock，不在 paper v2 范围
- 移动端响应式适配 → 明确不在本期范围（per audit "桌面端单操作员" 假设）
- 主题 / 暗色模式：不在本期范围
- 性能 / 大数据列表虚拟化：不在本期 paper v2 数据量级
