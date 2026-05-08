# Day 1+2 worktree 4 PR 拆分状态（Phase 1 完成）

> **生成时间**：2026-05-09 晚（Phase 1 整理完成）
> **来源**：Agent Teams 实施 session lead
> **目的**：记录 Day 1+2 全部增量按 4 PR 拆分 commit 到 feature 分支的状态，供用户 review
> **关键约束**：4 PR 全部在 feature 分支 `claude/paper-v2-vnpy-mvp-20260508`，**未合 main**（按用户 2026-05-09 决策）

---

## 1. 4 PR Commit 清单

| PR | Commit | 大小 | Scope | 主要内容 |
| --- | --- | --- | --- | --- |
| **PR-A** | `b5f9e13` | 9 files, +514 / -9 | 后端基础 schema | MarketDataSource.MINIQMT_REALTIME 枚举 + portfolio.broker_backend 字段 + DB migration SQL + 25 tests |
| **PR-B** | `7500194` | 5 files, +1295 | LocalSim broker | BrokerBackend ABC + LocalSimBackend impl + 4 typed errors + 20 tests |
| **PR-C** | `f253a6b` | 9 files, +241 / -17 | 前端 UI 简化 | format.ts 工具 + CopyChip + ReadinessFailureCard + 6 paper-v2 page 改造 + 28 CSS 规则 |
| **PR-D** | `b4177d1` | 7 files, +2121 / -90 | 设计稿 + 文档增量 | Engine §3.6 + cross-test v0.5 + 5 份新设计稿（含 Codex 协调文档） |

**Total 增量**：30 files, +4171 / -116 lines

**Push 状态**：`origin/claude/paper-v2-vnpy-mvp-20260508` 已同步（Phase 1 启动前 commit ea967f8 PoC + 6448c4b handoff snapshot 已在 origin）

---

## 2. PR-A 详情：后端基础 schema（b5f9e13）

### 文件清单

| 文件 | 类型 | 备注 |
| --- | --- | --- |
| `backend/services/paper_trading_v2/market_data.py` | M | MINIQMT_REALTIME 枚举 + ALLOWED_MARKET_SOURCES + assert_broker_market_source_match |
| `backend/services/paper_trading_v2/models.py` | M | BrokerBackendId Literal + PaperPortfolio.broker_backend 默认 local_sim + cross-config model_validator |
| `backend/services/paper_trading_v2/service.py` | M | create_portfolio broker_backend 参数 + cross-config fail-fast + _validate_broker_compatibility stub |
| `backend/services/paper_trading_v2/repository.py` | M | INSERT broker_backend 列 + 后向兼容读 |
| `backend/routers/paper_trading_v2.py` | M | CreatePortfolioRequest.broker_backend；broker_backend immutable |
| `backend/db/init_trading_core_v2_schema.py` | M | fresh install 同步加 broker_backend 列 + 3 CHECK |
| `backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql` | A | DB migration（**用户授权后跑**） |
| `backend/tests/paper_trading_v2/test_market_data_broker_match.py` | A | 14 tests |
| `backend/tests/paper_trading_v2/test_portfolio_broker_backend.py` | A | 11 tests |

### 落地的 R-Q9 决策

- **R-Q9.1** BrokerBackend 抽象引入（Literal "local_sim" / "minqmt_sim"，不开 minqmt_live）
- **R-Q9.2** 多策略包绑定 LocalSim（默认 local_sim）
- **R-Q9.3** 行情通道强绑定撮合端（fail-fast cross-config）

### Pending 用户操作

- DB migration 跑：`psql -d <paper_v2_db> -f backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql`
- 8001 重启（按 feedback_no_service_start）
- broker_compatibility 字段 stub 等 OPEN-EXT-3 落地

---

## 3. PR-B 详情：LocalSim BrokerBackend（7500194）

### 文件清单

| 文件 | 类型 | 备注 |
| --- | --- | --- |
| `backend/services/paper_trading_v2/broker/__init__.py` | A | 包导出 |
| `backend/services/paper_trading_v2/broker/base.py` | A | BrokerBackend ABC + 9 Pydantic 模型（OrderHandle / OrderHandleStatus / FillEvent / BrokerAccountSnapshot / CancelAck / BrokerBindCapacity / SubscriptionHandle / MarketDataChannel + Literals） |
| `backend/services/paper_trading_v2/broker/localsim.py` | A | LocalSimBackend 复用 MinuteExecutionEngine + InMemoryLedger；同步语义 |
| `backend/services/trading_core/errors.py` | M | BrokerBackendError 基类 + BrokerSubmitError / BrokerRejectedError / BrokerConnectivityError |
| `backend/tests/paper_trading_v2/test_localsim_backend.py` | A | 20 tests |

### 关键设计

- **同步语义约定**：LocalSim submit 同步阻塞、fill_callback 在 return 前已触发、OrderHandle.status 返回时已是终态（FILLED / PARTIALLY_FILLED / REJECTED）
- **ABC docstring 提醒**：Engine 共享代码不得依赖此同步性，必须按 MiniQMTSim 异步 superset 写
- **多 portfolio 隔离**：每 portfolio 独立 LocalSim 实例（ledger / positions / handle namespace / subscriber 互不干扰）
- **typed error 路径**：context 字段含 cause / cause_code（feedback_no_silent_errors）

### 落地的 R-Q9 决策

- **R-Q9.1 D1** BrokerBackend 抽象（ABC Protocol）
- **R-Q9.2 D2** LocalSim 多包并行（每 portfolio 独立实例）
- **R-Q9.5** schema 细化（BrokerAccountSnapshot 新名 / PositionLot 复用 / SubscriptionHandle+MarketDataChannel 最小定义 / submit 同步异步分流）
- **R-Q9.6** unsubscribe_fill_callback（idempotent + silent on unknown / released / shutdown）

---

## 4. PR-C 详情：前端 UI 简化（f253a6b）

### 文件清单

| 文件 | 类型 | 备注 |
| --- | --- | --- |
| `frontend/src/lib/paper-v2/format.ts` | M | packageDisplayLabel + selectionRunLabel + 复用 STATUS_LABELS 全局 |
| `frontend/src/components/paper-v2/CopyChip.tsx` | A | hash chip + tooltip + 一键复制（navigator.clipboard 原生） |
| `frontend/src/components/paper-v2/ReadinessFailureCard.tsx` | A | 基于 ReadinessResult.checks[] 三段结构 + advanced 折叠 |
| `frontend/src/app/paper-v2/page.tsx` | M | 表格 packageDisplayLabel + dataSourceLabel |
| `frontend/src/app/paper-v2/packages/page.tsx` | M | chip 重排（display_name + 创建日期 + CopyChip） |
| `frontend/src/app/paper-v2/portfolios/page.tsx` | M | 同 page.tsx 改造 |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | M | JsonPanel(readiness/capability) → ReadinessFailureCard / CapabilityErrorList |
| `frontend/src/app/paper-v2/selection/page.tsx` | M | 警告条 / runLabel / table 中文化 |
| `frontend/src/app/paper-v2/paper-v2.css` | M | 28 新规则 |

### 验证

- ✅ tsc --noEmit 通过
- ✅ 无新依赖
- ⚠ next lint 进入交互向导（worktree 无 .eslintrc.json）— 待用户决策是否补
- ⚠ frontend dev server 重启 + 浏览器手测 — 待用户操作

### Pending（不在本 PR）

- P1-G 步骤式向导：等用户 §8.3 决策（A 任务向导 / B 角色拆分 / C 重命名+折叠）

---

## 5. PR-D 详情：设计稿 + 文档增量（b4177d1）

### 文件清单

| 文件 | 类型 | 备注 |
| --- | --- | --- |
| `docs/architecture/strategy_engine_design_20260508.md` | M | §3.6 BrokerBackend + §10.1 typed errors + §11 Mode G 9 cases + §17 R-Q9.1-9.6 + OPEN-EXT-3 |
| `docs/standards/cross_test_framework_template_20260508.md` | M | v0.4.1 → v0.5（§2.4.5/6/7 broker + §2.5.4 typed error UI 映射 + §3.5.1 LocalSim 矩阵 9 行升级 + §3.5.1.A 同步 invariant + §3.5.1.B 9 条 ABC 接口完整性） |
| `docs/architecture/portfolio_broker_backend_ui_design_20260509.md` | A | portfolio 列表 + 创建 wizard + 4 类错误页面 UI + 6 PR-UI 拆分 |
| `docs/architecture/broker_backend_switch_flow_20260509.md` | A | 3 切换意图 + §6.3 typed error 中文 UI 映射 + ERROR_UI_MAP + i18n broker_error.* |
| `docs/analysis/vnpy_connect_dry_run_design_20260509.md` | A | 3 层验证 + step5 脚本设计 + DRY_RUN=True 守卫 + CallbackTrace shape-only |
| `docs/discussion/paper_v2_dual_broker_pr_split_plan_20260509.md` | A | 4 PR + 6 phases + 8 acceptance items V1-V8 |
| `docs/discussion/claude_code_day1_deliverables_20260509.md` | A | Codex 协调文档（12 交付 + 3 OPEN-EXT + Phase 4-6 衔接） |

### 落地的 R-Q9 决策（文档侧）

R-Q9.1 / R-Q9.2 / R-Q9.3 / R-Q9.4（broker_compatible 字段语义）/ R-Q9.5（schema 细化）/ R-Q9.6（unsubscribe）全部在 §17.1 + §17.5 一致性 checklist 落地。

---

## 6. 测试 / 回归状态

| 维度 | 状态 |
| --- | --- |
| 116 全套测试 | ✅ 0 回归 |
| 新增 45 tests | ✅ 全 PASS（14 market_data + 11 portfolio_broker_backend + 20 localsim_backend） |
| tsc --noEmit | ✅ 全过 |
| 116 → 161 测试 | ✅ +45 |

---

## 7. 边界遵守对照

| 边界 | 状态 |
| --- | --- |
| 不动 Codex 工作面（quantevolver / qe_strategies / model_registry） | ✅ |
| 不动 main 业务代码 | ✅（PR-A/B/C 全在 feature 分支） |
| 不动 finding_store schema | ✅（待明天与 Codex 协商） |
| 不重启生产 8001 | ✅ |
| 不跑 DB migration | ✅（脚本备好等用户授权） |

---

## 8. 下一步（Phase 2 启动条件）

Phase 2 5 thread 并行启动：
- T1 impl-paper-v2 → Paper v2 阻断点修复（live inference preflight）
- T2 ui-simplify → UI 简化 §1 B 后续项
- T3 engine-design → 双纸面设计（MCP server + shadow run）
- T4 cross-test → 5 份新模块测试矩阵
- T5 env-poc → vn.py + Paper v2 MVP 端到端 sim

预计 overnight 完成；早 5-7 AM 写 morning_status_20260510.md。

---

## 9. 用户 review 要点

- [ ] 4 PR commit hash 是否符合预期（b5f9e13 / 7500194 / f253a6b / b4177d1）
- [ ] 边界检查无越界（§7）
- [ ] DB migration 何时跑 + 8001 重启时机
- [ ] frontend 浏览器手测窗口
- [ ] OPEN-EXT-1/2/3 跨 Codex 协调时机
- [ ] §8.x audit 决策方向
- [ ] Day 1+2 4 PR 是否合 main（推荐先合 PR-D 文档，PR-A/B/C 等用户跑过 migration + 验证后再合）

---

**Phase 1 完成**。Phase 2 启动后 morning_status 文档将更新本节末追加进展。
