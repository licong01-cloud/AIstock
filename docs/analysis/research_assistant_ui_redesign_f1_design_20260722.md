# Research Assistant 前端视觉重设计 (F1)

- 文档日期: 2026-07-22
- 层级: F1（单模块：前端 research-assistant 样式层）
- 工作分支: `feature/ra-ui-redesign-20260722`
- 上游设计: `docs/analysis/paper_v2_ui_redesign_f1_design_20260722.md`（同一设计语言，本文档只列 RA 差异项）

## 1. 背景

`frontend/src/app/research-assistant/` 17 个子页面（chat/approvals/audit/memory/graph/mcp-tools/settings/skills/tasks/trace/workbench 等）使用独立样式文件 `research-assistant.css`（704 行，仅被本路由 layout 引用），当前为"森林绿+陶土橙+米色径向渐变底+深色大 hero"的暖重风格，与 2026-07-22 已合并的 paper-v2 青碧简约风（PR #2579）不一致。用户要求 RA 页面应用同一 UI 风格。

## 2. 范围

- **唯一改动文件**: `frontend/src/app/research-assistant/research-assistant.css`（整体重写，保留全部现有类名契约：190 个 `ra-*`/`pv2-*` 类，含文件内 legacy audit 页面的 `pv2-*` 映射块）
- 生效范围: 仅 research-assistant 路由（该 CSS 的唯一引用方）
- 设计交付物: 本文档 + 重写后的 `research-assistant.css`

## 3. 非目标

- 不改动任何 `.tsx/.ts` 文件：无组件结构、数据流、API 调用、业务逻辑变化
- 不让 RA 路由改引 `paper-v2.css`，不迁移组件类名（保持两文件独立皮肤，同一设计语言）
- 不改动 `paper-v2.css`（其中 `ra-chat-*` 副本服务于其他 layout 内嵌组件，已与 PR #2579 同为新规范，天然一致）
- 不改动 RA 路由以外的页面
- 不重启生产前端 3000

## 4. 设计规范（与 paper-v2 的差异项）

通用规范（色板/圆角/阴影/状态语义色/选中态规则）完全继承 paper-v2 F1 设计 §4，不再重复。RA 特有项：

### 4.1 Token 映射

新增 `--ra-palette-*` 基础色板层，**色值与 `--pv2-palette-*` 完全一致**（同一青碧主色 #0d9488、同一中性灰阶、同一语义色）；旧 `--ra-*` 变量名全部保留为语义别名：`--ra-forest → accent`、`--ra-forest-dark → accent-deep`、`--ra-mint → accent-soft-2`、`--ra-clay → accent`（用户头像/设置按钮跟随主色）、`--ra-amber/--ra-red/--ra-blue` 保持语义色、`--ra-radius 24px → 14px`、`--ra-shadow → 微阴影`。

### 4.2 页面级

- `.ra-shell` 去掉径向渐变底，改纯色 `#f7f8fa`，顶部同款 3px 青碧渐变细带
- `.ra-hero-shell` 深色横幅 → 轻量白卡页头（同 pv2-hero 规范），标题字号收敛
- `.ra-tabs` 胶囊选中态：浅青底 + 青碧字（替代薄荷绿胶囊）

### 4.3 RA 特有组件

- 对话气泡：AI = 浅灰白卡；用户 = 实心青碧白字；头像 AI = 浅青底青碧字，用户 = 实心琥珀（与 pv2 内 RA 副本一致）
- `.ra-chat-hero` 深色渐变 → 浅底 + 左 3px 青碧条
- 步骤点（rail 内是真步骤语义，保留状态色）：done 绿 / current 琥珀脉冲 / failed 红
- inline-decision 选项卡：默认白卡，选中 = 浅青底 + 青碧描边
- 证据卡/阻断卡/记忆节点：白卡 + 细边 + 微阴影；warning=浅琥珀底、danger=浅红底
- graph 画布：去径向渐变改浅灰底；节点描边青碧；degraded 提示浅琥珀底
- legacy `pv2-*` 映射块：同步映射到新规范（白卡/细边/语义徽章），保持对 audit 旧页面的视觉兼容
- 状态色全部沿用统一语义：成功绿 / 警告琥珀 / 危险红 / 信息蓝 / 主色青碧仅用于选中和交互

## 5. 设计验收索引

| ID | 设计项 |
|---|---|
| F-001 | `research-assistant.css` 按三层 token 架构重写，色板与 paper-v2 一致，无森林绿/陶土/米色渐变残留 |
| F-002 | 全部 190 个现有类名契约保留，RA 全部 TSX 零改动 |
| F-003 | hero/tabs/shell 按 §4.2 轻量化，与 paper-v2 页头规范一致 |
| F-004 | RA 特有组件（气泡/inline-decision/证据卡/graph）按 §4.3 实现，状态语义色与全站统一 |
| F-005 | legacy pv2-* 映射块同步新规范，audit 旧页面视觉不破裂 |
| F-006 | 构建通过且与 paper-v2 新皮肤视觉一致（密度/圆角/阴影/字号层级） |

## 6. 实施方案

1. worktree `F:\Dev\AIstock_worktrees\ra-ui-redesign-20260722` 重写 `research-assistant.css`
2. 类名契约机械差集核对（旧 190 类 → 新文件全覆盖）
3. `npm run build` + diff 范围核验
4. 回填验收矩阵 → F1 validate → PR

## 7. 验证方案

- `npm run build`（frontend）0 errors
- 类名差集 `comm` 为空；`git diff --name-only -- '*.tsx' '*.ts'` 为空
- 旧森林绿/陶土/米色色值 grep 计数 0
- `python scripts/aistock_feature_workflow.py validate --design docs/analysis/research_assistant_ui_redesign_f1_design_20260722.md --tier F1` 通过
- PR 评审时用户对照 paper-v2 页面目检 RA chat / audit / mcp-tools 页

## 8. 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frontend/src/app/research-assistant/research-assistant.css | `npm run build` 0 errors；旧色值（#1f4f3a/#14372a/#c66b42/#d7f8df/#fffaf0/#fffdf6/#eef4e9 等）grep 计数 0；三层 token 架构；artifact: tmp/validation/ra_ui_redesign_f1_receipts_20260722.md | done | 无 |
| F-002 | frontend/src/app/research-assistant/research-assistant.css | 类名契约核对：旧 190 类新文件 190/190 全覆盖（comm 差集为空）；`git diff --name-only -- '*.tsx' '*.ts'` 为空；artifact: tmp/validation/ra_ui_redesign_f1_receipts_20260722.md | done | 无 |
| F-003 | frontend/src/app/research-assistant/research-assistant.css | `.ra-shell` 纯色底+3px 细带；`.ra-hero-shell` 白卡页头；`.ra-tab-active` 浅青底青碧字；`npm run build` 通过 | done | 无 |
| F-004 | frontend/src/app/research-assistant/research-assistant.css | 气泡/头像/inline-decision/证据卡/graph/步骤点全部按 §4.3 落位；语义色引用 --ra-palette-{green,amber,red,blue}；`npm run build` 通过 | done | 无 |
| F-005 | frontend/src/app/research-assistant/research-assistant.css | legacy pv2-* 映射块（pv2-card/pv2-badge/pv2-table/pv2-readable-* 等）全部映射到新 token 体系；`npm run build` 通过 | done | 无 |
| F-006 | frontend/src/app/research-assistant/research-assistant.css | 圆角 14px/阴影 sm+md 两档/字号层级与 paper-v2 一致；`npm run build` 通过 | done | 无 |

注：规则实现与机械核验均已 done；合并后线上视觉确认属 PR 评审环节，TSX 零改动保证功能等价。

## 9. 风险 / 失败模式

| 风险 | 缓解 |
|---|---|
| 类名遗漏导致元素失样式 | 190 类机械差集核对 |
| RA 组件内联样式与新 token 冲突 | PR 评审目检；发现后在 CSS 内收敛 |
| graph 页 react-flow 第三方类名覆盖失效 | 保留原有 `.react-flow__*` 覆盖选择器并换新 token |
| 与 paper-v2.css 中 ra-chat-* 副本视觉漂移 | 两边同一色板/规则模板，本次按同一规范编写 |

## 10. 生产门禁

- `production_ddl_gate`: noop
- `production_backend_dependency_gate`: noop
- `production_frontend_dependency_gate`: noop（纯 CSS，无 npm 依赖变更）
- 生产前端 3000 生效 = 合并后独立用户授权步骤（可与 paper-v2 换肤同次重启）
