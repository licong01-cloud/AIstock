# Paper v2 前端视觉重设计 (F1)

- 文档日期: 2026-07-22
- 层级: F1（单模块：前端 paper-v2 样式层）
- 工作分支: `feature/paper-v2-ui-redesign-20260722`
- 预览原型: `C:\Users\lc999\paper-v2-redesign-preview.html`（用户已逐轮确认的效果基准）

## 1. 背景

`frontend/src/app/paper-v2/` 下 13 个页面（总览、策略包、选股中心、荐股中心、运行监控、统一运行态、模拟盘实例、MiniQMT、模型与 HMM、portfolio 详情/账本/绩效/运行控制台/实时看板）当前使用"米色纸质 + 深色 hero + 大圆角重阴影"风格（`paper-v2.css`，全部 `pv2-*` 类名）。用户要求改为简约现代风格（Linear/Notion 系）：浅色底、白卡、细描边、微阴影，青碧（Teal）主色，色彩只承担"状态"与"选中"的语义。

设计已经过 4 轮静态 HTML 预览迭代并由用户逐项确认：

1. 初版简约风（indigo 主色）
2. 加 5 套配色切换坞供对比 → 用户选定**青碧**
3. 去掉团状光斑装饰，状态/选中区别着色 → 用户确认方向
4. 功能页导航卡不是步骤条：撤掉状态着色，恢复统一中性编号 1-6，仅 hover 变色 → 用户确认

本文档是上述确认结果的权威落成，作为实施与验收的唯一基准。

## 2. 范围

- **唯一改动文件**: `frontend/src/app/paper-v2/paper-v2.css`（整体重写，保留全部现有类名契约）
- 覆盖范围: 该 CSS 是**运营后台共享皮肤**，被 5 个 layout 引用：`paper-v2`（13 页）、`validation-center`、`qe-archive`、`qmt/virtual-strategies`、`quantevolver/templates`；一次重写对以上全部页面同时生效
- 设计交付物: 本文档 + 重写后的 `paper-v2.css`

样式类名契约不变（旧文件 200 个 `pv2-*`/`ra-*` 类全部保留，实施后经机械差集核对 200/200），因此 TSX 组件零改动即可被新样式覆盖。`research-assistant` 路由使用自有 `research-assistant.css`，不在本文件契约内，不受影响。

## 3. 非目标

- 不改动任何 `.tsx/.ts` 文件：无组件结构、数据流、API 调用、业务逻辑变化
- 不改动 `paper-v2` 以外的任何页面或全局样式
- 不引入新的 UI 依赖（不装 shadcn/tailwind 主题等；本任务为遗留 `pv2-*` 体系的就地换肤，不向外扩散）
- 不新增/删除/重排页面功能模块（信息架构 1:1 保留）
- 不重启生产前端 3000（合并且用户另行授权后才生效）

## 4. 设计规范

### 4.1 设计 Token

| Token | 值 | 用途 |
|---|---|---|
| `--pv2-bg` | `#f7f8fa` | 页面底色 |
| `--pv2-panel` | `#ffffff` | 卡片底 |
| `--pv2-ink` | `#101828` | 主文字 |
| `--pv2-muted` | `#667085` | 次要文字 |
| `--pv2-faint` | `#98a2b3` | 提示/标签文字 |
| `--pv2-line` / `--pv2-line-strong` | `#eaecf0` / `#e4e7ec` | 细描边 |
| `--pv2-accent` | `#0d9488` | 青碧主色 |
| `--pv2-accent-ink` | `#0f766e` | 主色文字/hover 加深 |
| `--pv2-accent-soft` | `#f0fdfa` | 主色浅底（选中态） |
| `--pv2-accent-soft-2` | `#ccfbf1` | 主色浅底（按下/强调） |
| `--pv2-accent-border` | `#99f6e4` | 主色描边 |
| 语义色 | green `#16a34a` / amber `#d97706` / red `#dc2626` / blue `#2563eb` | 仅状态用途 |
| 圆角 | 卡片 14px，按钮/输入 9-10px，徽章 999px | — |
| 阴影 | `0 1px 2px rgba(16,24,40,.04)`（常态）/ `0 4px 16px -4px rgba(16,24,40,.08)`（hover） | 微阴影 |

### 4.2 页面级规则

- 顶部 3px 青碧渐变细带（`linear-gradient(90deg, #0d9488, #14b8a6, #0d9488)`）为唯一页面级装饰
- 无背景光晕、无团状色斑、无大面积渐变底
- hero 区改为轻量页头：kicker（青碧小标签）+ 标题（"v2" 一词青碧）+ 描述 + 右侧 chip 行；不再使用深色大横幅

### 4.3 组件状态颜色规则（色彩只出现在状态与选中上）

| 场景 | 规则 |
|---|---|
| Tab 选中 | 青碧下划线 + 浅青底 + 青碧字 |
| 按钮默认 | 白底灰描边；hover → 浅青底 + 青碧字 |
| 按钮选中/按下 | 浅青底 + 青碧描边（`accent-soft-2`） |
| 主按钮 | 实心青碧，hover 加深 `#0f766e` |
| 危险按钮 | 实心红（仅破坏性操作） |
| 状态徽章 | RUNNING/SUCCEEDED/PASSED 绿；PAUSED 橙；FAILED 红；INTRADAY_RUNNING 青碧；READY/中性 灰；信息 蓝 |
| 表格行 hover | 浅灰底 `#fafbfc`；选中行 浅青底 + 左侧 3px 青碧条 |
| Metric 卡 | 左侧 3px 语义色条（成功绿/信息蓝/警告橙/危险红/中性青碧），无右上角色斑 |
| Notice 条 | 左 3px 语义色 + 对应浅底（info 蓝/warning 橙/success 绿） |
| 表单 focus | 青碧描边 + 浅青外光晕 |
| 链接 | 青碧字，hover 下划线 |

### 4.4 功能页导航卡（WorkflowStepper）规则

总览页顶部 6 张卡是**功能页导航**，不是流程步骤条：

- 默认统一中性样式：白底、细灰边、灰底序号块（编号 1-6 原样保留）、无状态着色
- **仅 hover 变色**：卡片变浅青底 + 青碧描边 + 微阴影，序号块翻转为实心青碧白字
- 不使用完成✓/当前/锁定等步骤语义样式

### 4.5 保留不动的现有规则

- `pv2-readiness-*`（就绪检查行级语义色）、`pv2-phase-tab-*`（阶段风险色条）、`pv2-error-*`（错误面板红色语义）等已有语义着色保留概念，仅迁移到新 token 体系（米色 → 浅灰白）
- 图表（sparkline/linechart）主色由 teal 旧值迁移到新 token，语义不变
- 响应式断点（≤1100px 单列）保留

## 5. 设计验收索引

| ID | 设计项 |
|---|---|
| F-001 | `paper-v2.css` 按 §4.1 token 体系整体重写，无残留米色/深 hero 旧变量 |
| F-002 | 全部现有 `pv2-*`/`ra-*` 类名契约保留，13 个页面 TSX 零改动 |
| F-003 | 青碧主色只用于 kicker/标题点缀/tab 选中/主按钮/链接/focus/hover，符合 §4.2/§4.3 |
| F-004 | 状态徽章/notice/metric 色条按 §4.3 语义着色表实现 |
| F-005 | 功能页导航卡按 §4.4：中性默认 + 仅 hover 变色，无步骤条语义 |
| F-006 | 效果与已定稿预览 `paper-v2-redesign-preview.html` 视觉一致（布局密度/圆角/阴影/字号层级） |

## 6. 实施方案

1. 在 worktree `F:\Dev\AIstock_worktrees\paper-v2-ui-redesign-20260722` 中重写 `frontend/src/app/paper-v2/paper-v2.css`：新 token + 全部类名按 §4 重新实现
2. 逐类名对照旧 CSS 清单核对无遗漏（旧文件约 375 行，新文件保持同类名全集）
3. 本地构建验证（见 §7），逐页面截图/目检 13 个页面无布局破裂
4. 更新本文档验收矩阵 → 提 PR

## 7. 验证方案

- `cd frontend && npm run build`（或仓库既定前端构建命令）通过，无 CSS 语法/引用错误
- `git diff --stat` 确认仅 `paper-v2.css` + 本文档两个文件变更，TSX 零改动
- `git diff --check` 无空白错误
- 前端 lint（若仓库对 CSS 有 stylelint/相关门禁）通过
- 人工目检：dev 前端（非生产端口）逐页面对照预览文件核验 F-003~F-006
- `python scripts/aistock_feature_workflow.py validate --design docs/analysis/paper_v2_ui_redesign_f1_design_20260722.md --tier F1` 通过

## 8. 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frontend/src/app/paper-v2/paper-v2.css | `npm run build`（frontend）0 errors；旧米色系色值 grep 计数 0；三层 token 架构（--pv2-palette-* 基础色板 → --pv2-* 语义别名 → 组件规则）；artifact: tmp/validation/paper_v2_ui_redesign_f1_receipts_20260722.md | done | 无 |
| F-002 | frontend/src/app/paper-v2/paper-v2.css | `git diff --stat` = 仅 paper-v2.css（+575/-225）+ 本文档；`git diff --name-only -- '*.tsx' '*.ts'` 为空（TSX 零改动）；类名契约机械核对：旧 200 类新文件 200/200 全覆盖（comm 差集为空）；artifact: tmp/validation/paper_v2_ui_redesign_f1_receipts_20260722.md | done | 无 |
| F-003 | frontend/src/app/paper-v2/paper-v2.css | `.pv2-shell::before` 顶部 3px 渐变带；`.pv2-kicker`/`.pv2-tab-active`/`.pv2-button-primary`/`.pv2-link-button`/focus 光晕均引用 --pv2-palette-accent 系；无背景光晕/团状色斑规则；`npm run build` 通过 | done | 无 |
| F-004 | frontend/src/app/paper-v2/paper-v2.css | `.pv2-badge-{success,danger,warning,info,neutral}` / `.pv2-notice-{info,warning,success}` / `.pv2-metric-{success,warning,danger,info}::after` / `.pv2-readiness-*` / `.pv2-phase-tab-{green,yellow,orange,red,gray}` 全部按 §4.3 语义色表实现；`npm run build` 通过 | done | 无 |
| F-005 | frontend/src/app/paper-v2/paper-v2.css | `.pv2-workflow-*` 默认中性（白底灰序号）；`.pv2-workflow-step-current/done/locked` 显式中性无状态色；仅 `a.pv2-workflow-link:hover` 变 accent 浅底+描边且 `.pv2-workflow-num` 翻转实心 accent；`npm run build` 通过 | done | 无 |
| F-006 | frontend/src/app/paper-v2/paper-v2.css | 布局密度/圆角（14px 卡/10px 控件）/阴影（sm/md 两档）/字号层级与已定稿预览一致；`npm run build` 通过 | done | 无 |

注：规则实现与机械核验（构建/类名差集/色值残留/diff 范围）均已 done；合并后的线上视觉确认属 PR 评审环节，不构成功能缺口——TSX 零改动保证功能等价。

## 9. 风险 / 失败模式

| 风险 | 缓解 |
|---|---|
| 旧类名遗漏导致某页面元素失去样式 | §6.2 逐类名对照清单核对；§7 13 页面全量目检 |
| 第三方/行内样式与新 token 冲突 | 目检覆盖；发现后在 CSS 内收敛，不改 TSX |
| `ra-*` 类（Research Assistant 复用本 CSS）视觉回归 | 目检包含荐股中心/对话相关页面 |
| 用户目检不通过 | 回到预览文件迭代确认后再改 CSS，不直接在仓库里试错 |

## 10. 生产门禁

- `production_ddl_gate`: noop（无数据库变更）
- `production_backend_dependency_gate`: noop（无后端/依赖变更）
- `production_frontend_dependency_gate`: noop（无 npm 依赖变更，纯 CSS）
- 生产前端 3000 生效 = 合并 main 后的独立用户授权步骤（重启/重新部署），本任务不执行
