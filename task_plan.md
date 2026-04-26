# Task Plan: 因子值缓存与执行层对齐设计

## Goal
在不影响当前实验主链的前提下，设计因子值缓存体系的对齐与增强方案：统一 `source_hash_raw`，并覆盖日期范围选择、按成功进度增量补算、失败日志诊断、执行层自动缓存复用与 backfill/管理端一致性。

## Current Phase
Phase 7

## Phases

### Phase 1: Requirements & Discovery
- [x] Understand user intent
- [x] Identify constraints and requirements
- [x] Document findings in findings.md
- **Status:** complete

### Phase 2: Current-State Code Analysis
- [x] Inspect cache management router, backfill script, execution-layer cache hooks, and pipeline internals
- [x] Identify current support vs missing pieces
- [x] Document code locations and mismatches
- **Status:** complete

### Phase 3: Alignment Design
- [x] Define how to unify `source_hash_raw` across execution layer, backfill script, router, and meta consumers
- [x] Define compatibility strategy that avoids breaking running experiments
- [x] Define required meta schema adjustments
- **Status:** complete

### Phase 4: Feature Design
- [x] Design UI + API flow for selectable start/end dates
- [x] Design true incremental/resume semantics for batch cache compute
- [x] Design structured failure logging and task diagnostics flow
- [x] Design consistency between execution-layer auto-write cache and manual backfill cache
- **Status:** complete

### Phase 5: Delivery
- [x] Summarize architecture decisions and implementation order
- [x] Identify minimal-risk rollout sequence
- [x] Deliver design to user
- **Status:** complete

### Phase 6: 统一评级标准设计
- [x] 梳理规则评级 / LLM评级 / 批量脚本重算之间的冲突点
- [x] 明确唯一权威评级标准必须由单一规则引擎产出
- [x] 设计适配 Multi-Alpha 的多指标综合评分框架
- [x] 明确 LLM 仅作为补充审核与文字说明，不得单独改写正式评级
- [x] 明确所有评级输入统一从数据库读取，禁止从文件侧读取
- **Status:** complete

### Phase 7: 评级v1与管理工具栏设计
- [x] 将日频低换手与选股稳定性要求纳入评级v1
- [x] 设计唯一 UI 入口触发的评级执行流
- [x] 设计版本化规则管理、批量重评与规则说明展示
- [x] 设计数据库结构与页面展示改造方案
- **Status:** complete

## Key Questions
1. How can `source_hash_raw` be unified without invalidating currently written cache metadata or disturbing running experiments?
2. What is the minimal set of changes needed to make factor cache management usable for long-running incremental backfill and diagnostics?
3. Which current capabilities already exist in execution layer and should be preserved rather than rebuilt?

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Focus on design first, no code changes yet | User asked to start designing and current experiment is running in execution layer |
| Treat execution-layer cache path as authoritative behavior baseline | It already provides on-demand cache hit/write during experiments |
| Avoid any design that requires starting/stopping services | User preference and current session constraints |
| Do not modify execution-layer cache protocol first; instead align factor value compute/backfill chain to it | Execution layer is actively running experiments, so lower-risk direction is to converge other writers/readers to `source_hash_raw` |
| Use compatibility read fallback during migration, but only write `source_hash_raw` going forward | Prevents breaking old cache meta while converging schema |
| Add task-level checkpoint files for batch cache jobs | Required to support resume, retry_failed_only, and structured diagnostics |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Stop-hook requested MemPalace autosave but MCP mempalace tools returned internal error | 1 | Continued task work without relying on autosave |
| Initial broad PowerShell process query failed due to shell interpolation issue | 1 | Replaced with precise process query targeting `backfill_factor_cache.py` |
| planning-with-files stop hook reported `SD=.../scripts` / `/check-complete.sh` not found | 1 | Verified local skill path exists; root cause is hook command's plugin-root assumption and fragile shell fallback under current Windows installation layout |

## Notes
- Running experiment exists in execution layer, so alignment design should minimize blast radius.
- `source_hash_raw` is already used by execution-layer cache hit path; design should converge other writers/readers to that field.
- Planning files live in project root and should be updated as analysis progresses.

---

# Task Plan: Paper Trading v2 UI Implementation

## Goal
Create a new `/paper-v2` UI that is independent from legacy `/paper-trading`, aligns with StrategyPackage -> Selection Center -> Paper Trading v2 backend APIs, and exposes the full correct Paper v2 workflow with fail-fast error visibility.

## Current Phase
Paper v2 UI Phase 1: design document and implementation baseline

## Phases

### Phase 1: UI design document
- [x] Add detailed Paper v2 UI design document under `docs/architecture/`.
- [x] Document route map, API mapping, user flow, fail-fast behavior, and visual system.

### Phase 2: API/types/component foundation
- [x] Add Paper v2 frontend API client wrappers.
- [x] Add shared types and error handling.
- [x] Add common UI components for status badges, error panels, JSON traces, confirmations, cards, and tables.

### Phase 3: Core workflow pages
- [x] Add `/paper-v2` shell and Overview.
- [x] Add Packages page.
- [x] Add Selection Center page.
- [x] Add Portfolio Center and create wizard.

### Phase 4: Trading operations pages
- [x] Add portfolio detail.
- [x] Add Run Console with readiness/run-day/replay/reset.
- [x] Add Ledger views.
- [x] Add Performance report.
- [x] Add Model & HMM center.

### Phase 5: Navigation and verification
- [x] Add global Sidebar links.
- [x] Run frontend lint/type/build checks where available.
- [x] Run backend import/API smoke checks on non-8001 port if needed.
- [x] Document UI-based validation capability and limitations.

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Create new `/paper-v2` UI instead of refactoring legacy `/paper-trading` | Avoid mixing legacy paper_trading APIs with Paper v2 fail-fast workflow |
| Keep multi-package Paper execution disabled in UI | Backend intentionally requires combined package/SelectionBundle before trading aggregate selections |
| Use validated execution policy selector only | Paper-only execution config is prohibited |
| Surface backend errors with full context | Paper v2 must not hide fail-fast backend errors |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| None yet | - | - |

| PowerShell New-Item multiple path invocation failed | 1 | Re-ran directory creation with a foreach loop and -LiteralPath |

| New-Item -LiteralPath unsupported in current shell | 2 | Used System.IO.Directory.CreateDirectory for exact path creation including [portfolioId] |
| `npm run lint` entered interactive ESLint setup because the project has no ESLint config | 1 | Used `npm run build`, which completed Next compilation plus type/lint validity checks successfully |
| `npm run start -- -p 3011` initially used stale dev `.next` artifacts after a dev smoke and returned 500 | 1 | Removed generated `.next`, rebuilt cleanly, then `npm run start -- -p 3011` route smoke returned 200 for all Paper v2 routes |

---

# Task Plan: Paper v2 Selection/Package/Portfolio UI Completion

## Goal
补齐 Paper Trading v2 新 UI 与后端主链路，使 StrategyPackage 创建、权威选股、自选股票池加入、历史选股记录聚合、单策略包模拟盘启动和运行组合列表都能在 `/paper-v2` 中文页面中完成，并保持 fail-fast 与可追溯。

## Current Phase
Phase 2: 后端能力补齐

## Phases

### Phase 1: 设计文档
- [x] 落地 `docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md`
- [x] 明确 artifact 自动生成、自选加入、QE source 下拉、单包模拟盘启动边界
- **Status:** complete

### Phase 2: 后端能力补齐
- [ ] 新增 QE 未打包来源查询接口
- [ ] Selection Center 支持显式 auto_generate artifact
- [ ] Selection Center 支持选股结果加入自选股票池
- [ ] TopK 后端限制到 50
- **Status:** in_progress

### Phase 3: 前端页面补齐与中文化
- [ ] `/paper-v2/packages` 使用 QE source 下拉并展示指标
- [ ] `/paper-v2/selection` 补齐 Top20、HMM 下拉、历史记录详情、聚合按钮、自选按钮
- [ ] `/paper-v2/portfolios` 补齐单包启动、运行配置、HMM/黑名单、回放/实时模式、运行列表
- **Status:** pending

### Phase 4: 测试与 UI 验证
- [ ] 后端 pytest
- [ ] 前端 build
- [ ] 临时端口 API/UI smoke
- [ ] Playwright 或等价后台 UI 流程验证
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| UI 自动生成 selection artifact 通过 `selection_artifact_config.auto_generate=true` 显式启用 | 避免用户手动生成 artifact，同时不让后端无条件 silent fallback |
| `auto_generate` 不进入 artifact hash | 这是编排开关，不改变模型推理结果 |
| 自选加入使用 selection result 的 `reference_price` | 加入价必须与选股时点可追溯，缺价格直接失败 |
| 多策略包聚合只用于选股研究 | 当前不创建多策略包模拟盘执行 |
