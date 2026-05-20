# BUG-092 L3 验证记录：Paper v2 显式执行策略证据

- 日期：2026-05-20
- 分支：`bug/BUG-092-paper-v2-explicit-execution-policy-evidence`
- Worktree：`F:\Dev\AIstock_worktrees\bug-092-paper-v2-explicit-execution-policy-evidence`
- GitHub Issue：#111 `BUG-092`
- 本地 BUG JSON：`tests/aistock_validation/bugs/20260520_BUG-092-paper-v2-explicit-execution-policy-evidence.json`
- 生产影响：未触碰生产 backend `8001`、frontend `3000`、生产数据库、MiniQMT 运行时或任何策略资产。

## 修复目标

BUG-092 要求 Paper v2 / MiniQMTSim 不得把 StrategyPackage manifest 中的 `minute_execution_policy` 自动转换为 `BACKTEST_VALIDATED` 且 `paper_enabled=true` 的执行策略。模拟盘组合必须绑定已有、显式创建、带成功来源证据且已启用 paper 的 `ValidatedExecutionPolicy`。

## 实现范围

- `backend/services/paper_trading_v2/service.py`
  - 删除 `_ensure_default_manifest_execution_policy` 自动创建 `manifest_default_execution_policy` 的路径。
  - 新增 `_select_default_manifest_execution_policy`：仅选择已经持久化、hash 与 manifest policy 匹配的策略；未找到时 fail-fast。
  - 保留 Paper v2 后续校验：manifest hash 匹配、`paper_enabled`、`ensure_policy_can_enter_paper`、执行策略 schema 校验。
- `backend/services/strategy_package/service.py`
  - `create_execution_policy` 现在要求 `source_backtest_status` 属于成功证据集合，并规范化为大写后持久化。
- `frontend/src/app/paper-v2/portfolios/page.tsx`
  - 创建模拟盘前必须选择已验证执行策略；按钮在无策略时禁用。
  - 删除新增路径上的 raw JSON 创建结果展示，改为中文业务提示。
- `frontend/src/app/paper-v2/miniqmt-sim/page.tsx`
  - MiniQMTSim 创建组合前加载并要求选择 `paper_enabled` 执行策略。
  - 创建请求显式传入 `execution_policy.validated_execution_policy_id`。
- 测试更新
  - Paper v2 现有用例显式创建 validated execution policy，不再依赖 manifest 自动导入。
  - 新增无显式策略拒绝、失败 backtest source 拒绝等回归测试。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item / issue requirement | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| 创建 Paper portfolio 时，没有显式 validated execution policy 必须失败，且不得自动生成 `manifest_default_execution_policy` | `backend/services/paper_trading_v2/service.py`; `backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_requires_explicit_validated_policy_evidence` | focused pytest 4 passed；Paper v2 backend nox 445 passed | PASS | 无 |
| Manifest minute policy 不再自动成为 Paper/MiniQMT 执行权威 | `backend/services/paper_trading_v2/service.py::_select_default_manifest_execution_policy` | `rg` 复核无 `_ensure_default_manifest_execution_policy` 自动创建路径；guardrail_changed_files findings=0 | PASS | 无 |
| 执行策略创建必须带成功来源证据 | `backend/services/strategy_package/service.py`; `backend/tests/strategy_package/test_repository_service.py::test_strategy_package_execution_policy_requires_successful_source_evidence` | strategy_package + paper_v2 pytest 392 passed；paper_v2_backend nox 445 passed | PASS | 无 |
| 显式指定 `paper_enabled` validated execution policy 的 Paper v2 创建/运行仍可用 | Paper v2 day-runner、session、runtime-profile、broker-backend、MiniQMTSim 测试 helper 显式 seed policy | `python -m pytest backend/tests/paper_trading_v2 backend/tests/strategy_package ... -q` -> 392 passed, 1 skipped, 2 xfailed | PASS | 无 |
| 原有 raw paper-only execution override 和未知 fallback algo 仍被拒绝 | `backend/tests/paper_trading_v2/test_day_runner.py`; 现有 Paper v2 backend suite | `python -m nox -s paper_v2_backend` -> 445 passed, 1 skipped, 2 xfailed | PASS | 无 |
| Run / portfolio 路径继续持久化 validated policy id/hash/json/source evidence | `backend/services/paper_trading_v2/service.py::_paper_execution_policy_payload`; existing run persistence tests | Paper v2 backend nox 445 passed | PASS | 无 |
| Portfolio UI 必须选择 paper-enabled validated policy，不再说明“Manifest 默认导入” | `frontend/src/app/paper-v2/portfolios/page.tsx` | `npm run lint -- --file ...` PASS；changed-file guardrail findings=0 | PASS | 无 UI E2E：本分支未启动临时 3011/8012，因本次为表单约束与 backend contract 回归，已用 lint + backend contract 覆盖 |
| MiniQMTSim UI 创建组合必须选择 paper-enabled validated policy，并把 policy id 显式传给后端 | `frontend/src/app/paper-v2/miniqmt-sim/page.tsx` | `npm run lint -- --file ...` PASS；Paper v2 backend MiniQMTSim tests included in 445 passed | PASS | 无 |
| GitHub Issue 与本地 BUG JSON 保持链接和状态同步 | `tests/aistock_validation/bugs/20260520_BUG-092-paper-v2-explicit-execution-policy-evidence.json`; GitHub #111 | #111 已为 BUG-092、`status:in_progress`，完成提交后将同步为 `status:fixed-pending-review` | PASS_PENDING_COMMIT | 等待最终 commit hash 回填 |

## 执行命令与结果

### 后端 / 回归

```powershell
python -m pytest backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_requires_explicit_validated_policy_evidence backend/tests/paper_trading_v2/test_day_runner.py::test_paper_trading_day_runner_persists_full_day_path backend/tests/paper_trading_v2/test_day_runner.py::test_paper_execution_policy_activation_matching_qe_contract_is_used_for_trade_date_run backend/tests/strategy_package/test_repository_service.py::test_strategy_package_execution_policy_requires_backtest_contract_and_hash -q
# PASS: 4 passed
```

```powershell
python -m pytest backend/tests/paper_trading_v2 -q
# PASS: 223 passed, 1 skipped, 2 xfailed
```

```powershell
python -m pytest backend/tests/strategy_package backend/tests/paper_trading_v2 -q
# PASS: 391 passed, 1 skipped, 2 xfailed
```

```powershell
python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_center_creates_single_package_paper_portfolio_with_trace_link -q
# PASS: 1 passed
```

```powershell
python -m pytest backend/tests/paper_trading_v2 backend/tests/strategy_package backend/tests/selection_center/test_runtime_selection.py::test_selection_center_creates_single_package_paper_portfolio_with_trace_link -q
# PASS: 392 passed, 1 skipped, 2 xfailed
```

```powershell
python -m nox -s paper_v2_backend
# PASS: 445 passed, 1 skipped, 2 xfailed
```

### 编译 / 前端 / Guardrail

```powershell
python -m compileall backend/services/paper_trading_v2 backend/services/strategy_package backend/routers/paper_trading_v2.py backend/routers/strategy_packages.py
# PASS
```

```powershell
cd frontend
npm run lint -- --file src/app/paper-v2/portfolios/page.tsx --file src/app/paper-v2/miniqmt-sim/page.tsx
# PASS: No ESLint warnings or errors
```

```powershell
python -m nox -s validation_module_registry_l0
# PASS: 8 passed; ownership scan files=12, mapped=12, unmapped=0, ambiguous=0
```

```powershell
python -m nox -s guardrail_changed_files -- --changed-only
# PASS: files=14, findings=0, blocking=0; module ownership files=14, mapped=14
```

```powershell
git diff --check
# PASS; only LF/CRLF warnings from Git, no whitespace errors
```

### L0 流水线

```powershell
python -m nox -s l0
# PASS: successful; baseline/new non-blocking findings only, blocking=0
```

## 失败、修复与复测记录

| 失败/发现 | 原因 | 修复 | 复测 |
|---|---|---|---|
| 首次 `paper_v2_backend` 失败 | Selection Center 测试仍依赖隐式 manifest policy | 在 `backend/tests/selection_center/test_runtime_selection.py` 中显式创建 execution policy | `python -m nox -s paper_v2_backend` PASS |
| 首次前端 lint 失败 | worktree frontend 没有 `node_modules` | 在 worktree `frontend/` 执行 `npm ci --ignore-scripts --no-audit --no-fund`，只生成 ignored `frontend/node_modules/` | targeted frontend lint PASS |
| changed-file guardrail 首次出现 P2 `UI-RAWJSON-001` | Portfolio create 成功结果仍使用 `JsonPanel` | 改为中文 `NoticePanel` 业务提示，删除该页面 `JsonPanel` import | `guardrail_changed_files --changed-only` PASS，findings=0 |

## 资产安全与生产影响

- 未修改 StrategyPackage frozen manifest、模型权重、HMM snapshot、QE/RD-Agent artifact、Paper ledger 历史数据或生产 DB。
- 未启动、停止或重启生产 backend `8001`、frontend `3000`、TDX Go `19080` 或 MiniQMT。
- `frontend/node_modules/`、`.pytest_cache/`、`__pycache__/`、`tmp/` 为 ignored 本地验证产物，未纳入提交。

## 残余风险

- 本分支不实现新的执行策略证据导入 UI；它只阻止隐式 manifest 自动授权，并要求已有 validated execution policy。
- Portfolio / MiniQMTSim UI 没有做浏览器 E2E，本次以 backend contract、lint、guardrail 覆盖；后续若改动用户完整操作流，应补 `paper_v2_l3` 或 Playwright 路径。
- GitHub Issue 状态将在 commit/push 后由 `status:in_progress` 同步为 `status:fixed-pending-review`。
