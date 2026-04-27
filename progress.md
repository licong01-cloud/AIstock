# Progress Log

## Session: 2026-04-16

### Phase 7: 评级v1与管理工具栏设计
- **Status:** complete
- Actions taken:
  - 将日频低换手与选股稳定性要求纳入正式评级v1设计。
  - 明确唯一正式评级应锚定到当前 AIstock 日频多Alpha生产场景，而不是泛化学术评级。
  - 设计了版本化规则管理、UI单一入口执行流、数据库分表方案与工具栏能力。
  - 明确 v1 使用数据库现有 `turnover` 作为选股稳定性 proxy，后续可在 v2 增加更精确的篮子重合率指标。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

- **Status:** complete
- Actions taken:
  - 基于现有因子评级冲突，设计了唯一权威的统一规则评级方案。
  - 明确正式评级必须由单一规则引擎产出，LLM 只做补充审核与说明。
  - 明确所有评级输入必须统一从数据库读取，禁止任何文件侧读数参与正式评级。
  - 评估了方案合理性与局限性：方案方向正确，但规则必须版本化并定期校准。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)


### Phase 1: Requirements & Discovery
- **Status:** complete
- **Started:** 2026-04-16
- Actions taken:
  - 读取记忆与当前代码，确认用户关注的因子值缓存缺口。
  - 检查后端 router、前端入口、backfill 脚本、pipeline、执行层 prepare_factors 注入逻辑。
  - 确认执行层已具备按需自动写缓存与下次复用能力。
  - 精确识别到正在运行的 `backfill_factor_cache.py` 任务并按用户授权终止。
  - 建立 planning-with-files 所需文件。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (created)
  - `F:\Dev\AIstock\findings.md` (created)
  - `F:\Dev\AIstock\progress.md` (created)

### Phase 2: Current-State Code Analysis
- **Status:** complete
- Actions taken:
  - 确认 `start_date/end_date` 后端已支持但前端未传。
  - 确认 `--incremental` 仅停留在参数层，没有形成真实的批任务增量语义。
  - 确认存在 `extend_single_factor_cache()` 可作为真正增量方案基础。
  - 确认 pipeline 失败信息被截断，缺少结构化失败日志与前端诊断展示。
  - 确认 `source_hash_raw` 与 `source_hash` 并存，存在对齐需求。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 3: Alignment Design
- **Status:** complete
- Actions taken:
  - 明确以执行层当前缓存协议作为对齐基准，不先改 execution-layer 主逻辑。
  - 明确 `source_hash_raw` 统一策略：迁移期读兼容、写统一。
  - 明确低风险 rollout：先对齐 factor value compute/backfill/router/front-end，再增强增量与诊断。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 4: Feature Design
- **Status:** complete
- Actions taken:
  - 设计了日期范围选择的前端/API 流程。
  - 设计了真正 incremental + resume/retry_failed_only 的批任务语义。
  - 设计了结构化失败日志与任务诊断链路。
  - 设计了 execution-layer 自动缓存与 backfill 缓存的元数据一致性方案。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 5: Delivery
- **Status:** in_progress
- Actions taken:
  - 整理了交付给用户的方案摘要与实施顺序。
  - 分析了 planning-with-files stop hook 报错，确认问题属于本机 Claude skill/plugin hook 路径解析，而非 AIstock 项目代码。
  - 已落地第一轮开发：统一 `source_hash_raw` 写入、增强 backfill 批任务编排（incremental / task checkpoint / failed.ndjson / resume_task_id / retry_failed_only）、增强后端任务状态接口、增强前端缓存管理入口（日期区间 / incremental / 最近任务 / 日志 / 失败因子）。
  - 对修改后的 Python 文件执行了 `py_compile` 静态编译通过。
  - 前端 TypeScript 未能真实 type-check：当前环境里 `tsc` 仅返回“typescript 未安装”的提示，不是项目代码错误。
- Files created/modified:
  - `F:\Dev\AIstock\scripts\backfill_factor_cache.py` (rewritten)
  - `F:\Dev\AIstock\backend\services\quantevolver\factor_value_pipeline.py` (updated)
  - `F:\Dev\AIstock\backend\routers\quantevolver.py` (updated)
  - `F:\Dev\AIstock\frontend\src\app\quantevolver\components\FactorList.tsx` (updated)
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

## Test Results
| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| Find running factor cache task | Process query for `backfill_factor_cache.py` | Only real factor cache task found | Confirmed WSL PIDs 48420/66384 | ✓ |
| Stop confirmed factor cache task | Stop-Process on confirmed PIDs | Task terminated | PID 48420 stopped, 66384 already exited; no remaining process on recheck | ✓ |

## Error Log
| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-04-16 | Stop hook requested MemPalace autosave but MCP tool returned internal error | 1 | Ignored for current analysis work |
| 2026-04-16 | PowerShell variable name `$pid` conflicted with built-in `$PID` during process termination | 1 | Renamed loop variable to `$procId` |

## 5-Question Reboot Check
| Question | Answer |
|----------|--------|
| Where am I? | Phase 3: Alignment Design |
| Where am I going? | Phase 4 feature design, then delivery |
| What's the goal? | Design factor cache alignment and enhancements around `source_hash_raw`, dates, incremental resume, logging, and execution-layer reuse |
| What have I learned? | See findings.md |
| What have I done? | See above |

---
*Update after completing each phase or encountering errors*

## Session: 2026-04-26 Paper Trading v2 UI
- **Status:** started
- Actions taken:
  - User confirmed new Paper v2 UI direction.
  - Started detailed design + implementation flow.
  - Will avoid restarting production backend on port 8001; temporary ports only for verification.
- Added `docs/architecture/paper_trading_v2_ui_design.md` with route map, page wireframes, backend API mapping, fail-fast UI contract, and verification plan.

### Paper v2 UI implementation pass
- **Status:** implementation and verification complete
- Actions taken:
  - Added new `/paper-v2` route tree with Overview, Packages, Selection, Portfolios, Portfolio Detail, Run Console, Ledger, Performance, Model & HMM, and Settings pages.
  - Added shared Paper v2 frontend API wrappers, types, formatting helpers, status/error/notice/JSON/table/card/confirmation components, and visual CSS.
  - Added StrategyPackage creation controls for QE experiment and QE evolution loop sources.
  - Added dynamic multi-package Selection Center controls plus existing-run aggregation UI while keeping multi-package Paper execution blocked.
  - Added portfolio readiness/run-day/replay/reset UI, dated validated-execution-policy activation UI, full ledger UI, and performance report UI.
  - Added `/api/v1/paper-v2/portfolios/{portfolio_id}/cash-ledger` repository/router/API client support for cash-ledger traceability.
  - Added Sidebar links for Paper Trading v2 without modifying legacy `/paper-trading/*`.
  - Ran frontend build and route smoke checks on temporary port 3011.
  - Ran backend import/API smoke checks on temporary port 8011 without touching port 8001.
  - Ran relevant backend pytest suite.
- Files created/modified:
  - `docs/architecture/paper_trading_v2_ui_design.md`
  - `docs/codex_project_memory.md`
  - `backend/routers/paper_trading_v2.py`
  - `backend/services/paper_trading_v2/repository.py`
  - `frontend/src/app/Sidebar.tsx`
  - `frontend/src/app/paper-v2/**`
  - `frontend/src/components/paper-v2/**`
  - `frontend/src/lib/paper-v2/**`

## Paper v2 UI Verification Results
| Test | Result |
|------|--------|
| `npm run lint` in `frontend` | Not usable non-interactively; `next lint` prompted to create ESLint config |
| `npm run build` in `frontend` | Passed; all `/paper-v2` routes compiled. After a dev smoke dirtied `.next`, the generated `.next` directory was safely removed and a clean build also passed |
| Backend import smoke | Passed; `backend.main` imported and cash-ledger route was present |
| Temporary backend on port 8011 | Started successfully; `/openapi.json`, `/api/v1/paper-v2/portfolios`, `/api/v1/selection-center/selectable-packages`, and `/api/v1/strategy-packages` returned 200 |
| OpenAPI path check | Passed for cash-ledger, execution-policy activations, selection aggregate-runs, and strategy package creation |
| `pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/test_tushare_sync_engine.py backend/tests/test_hmm_rolling_training.py -q -p no:cacheprovider` | Passed: 94 passed |
| `npm run start -- -p 3011` route smoke | Passed after clean rebuild: `/paper-v2`, packages, selection, portfolios, detail, run-console, ledger, performance, model-hmm, settings returned 200 |
| Browser click automation | Not run; Playwright is not installed in the frontend project |

## Session: 2026-04-26 Paper v2 completion continuation
- **Status:** design completed, backend implementation starting
- Actions taken:
  - Re-read Codex project memory and Paper v2 architecture context.
  - Re-read existing planning files and confirmed previous Paper v2 UI pass left several garbled/unfinished UI paths.
  - Added detailed design document: docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md.
  - Confirmed DB has target QE experiments and existing packaged StrategyPackages for qe_20260416_002701, qe_20260413_084216, qe_20260416_082012.
- Timestamp: 2026-04-26T23:59:23

## Session: 2026-04-27 QE Config Truthfulness / No-Silent-Override Hardening
- **Status:** started
- Actions taken:
  - Activated planning-with-files for persistent tracking.
  - Re-read docs/codex_project_memory.md.
  - Captured dirty git baseline; many pre-existing modified/untracked files are present.
  - Added new task plan section for this work.
