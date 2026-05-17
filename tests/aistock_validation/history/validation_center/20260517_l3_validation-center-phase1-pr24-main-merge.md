# Validation Center Phase-1 PR #24 Main Merge Validation

日期：2026-05-17
模块：validation_center
级别：L3
分支：codex/validation-center-phase1-pipeline-20260515
验证目标：将 Validation Center phase-1 pipeline center 功能分支更新到最新 `origin/main` 后，验证后端 API、前端 UI、真实开发端口只读链路与合入门禁，再通过 PR 合入 `main`。

## 变更范围

- 功能分支原始交付：`1861dc0 feat(validation): implement phase1 pipeline center`。
- 本次预合并：在功能分支执行 `git merge --no-ff origin/main -m "merge: update validation center pipeline branch from main"`，无冲突。
- 本次只处理合入准备相关文件：Validation Center 后端/前端既有变更、main 最新提交、合入前验证记录、少量已存在文档/HTML 空白字符清理。
- 未修改生产数据、交易资产、StrategyPackage frozen manifest、QE/RD-Agent 实验产物、HMM snapshot、Paper ledger。

## 环境与端口

- 后端验证端口：`8012`。
- 前端验证端口：`3011` / `3012`。
- 生产后端 `8001`：未重启、未停止、未用于验证。
- 生产前端 `3000`：未重启、未停止、未用于验证。
- 启动开发后端时，首次 PowerShell/GBK 控制台启动因 `UnicodeEncodeError` 失败；随后使用 `PYTHONUTF8=1` 与 `PYTHONIOENCODING=utf-8` 启动 `8012`，验证通过。该失败没有触达生产端口。

## 执行命令与结果

```powershell
git fetch --prune origin
git -C F:\Dev\AIstock_worktrees\validation-center-phase1-pipeline-20260515 branch backup/validation-center-phase1-pre-main-merge-20260517 HEAD
git -C F:\Dev\AIstock_worktrees\validation-center-phase1-pipeline-20260515 merge --no-ff origin/main -m "merge: update validation center pipeline branch from main"
```

结果：无冲突，功能分支吸收最新 `origin/main`。

```powershell
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_center_backend
```

结果：`101 passed in 18.26s`；coverage gate 通过，`line=80.22`、`branch=60.45`，状态 `passed`。

```powershell
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_center_ui
```

结果：TypeScript `tsc --noEmit` 通过；Playwright mock UI `1 passed`。

```powershell
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0 -- scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py tests/aistock_validation/modules/validation_center.md frontend/tests/validation-center/validation-center-real-port.spec.ts
```

结果：skill metadata 通过；quality guardrail 仅报告 3 个 `MEDIUM RAW_JSON_UI` 复核项；legacy guardrail 命中 1 个 baseline P1，`blocking=0`；session 成功。

```powershell
$env:PYTHONUTF8='1'; $env:PYTHONIOENCODING='utf-8'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012
```

结果：开发后端 `8012` 可用；`GET /api/v1/validation/health` 返回 `status=success`，`production_8001_touched=false`。

```powershell
$env:PYTHONUTF8='1'; $env:PYTHONIOENCODING='utf-8'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_center_live_readonly -- 8012
```

结果：只读 smoke 通过，`endpoints=39`、`failures=0`。

```powershell
$env:PYTHONUTF8='1'; $env:PYTHONIOENCODING='utf-8'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_center_real_port_ui -- 8012 3012
```

结果：真实开发端口 UI smoke 通过；TypeScript 通过；Playwright real-port UI `1 passed`。

```powershell
git diff --check
```

结果：无输出。`origin/main...HEAD` 范围内原有 phase-1 文档/HTML 空白字符已清理。

## 证据文件

- `tmp/validation/coverage/validation_center_backend_snapshot.json`
- `tmp/validation/coverage/validation_center_backend.xml`
- `tmp/validation/coverage/validation_center_backend.json`
- `tmp/validation/validation_center/readonly_smoke.json`
- `tmp/validation/validation_center/readonly_smoke_evidence.json`
- `tmp/validation/validation_center/ui_real_port_smoke.json`
- `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`
- `tmp/validation/guardrails/l0_paths.json`
- `tmp/validation/guardrails/l0_paths.md`

## 业务断言

- Validation Center phase-1 后端只读 API、pipeline card 数据、Issue workflow、merge gate、pipeline tests、features、GitHub/legacy debt/automation summaries 的单元/集成测试通过。
- `/validation-center` 前端页面可通过 mocked API 和真实开发端口访问关键面板，不依赖生产 `3000` 或 `8001`。
- live read-only smoke 只发送 GET 请求，未写业务 DB，未启动任意 shell，未触达生产 `8001`。
- 本次合入只推进 PR #24 的代码合并；运行中的生产服务需要单独重启/刷新后才会加载新代码。

## 遗留风险

- 本次未运行 controlled runner live POST smoke；原因是 phase-1 pipeline center 本次合入目标为只读中心页面与合入门禁展示，runner 行为已由 `validation_center_backend` 覆盖，且本次合入不需要新增 runner 归档记录。
- 本次未重启生产 `8001` / `3000`；合并后当前已运行服务不会自动热加载新功能。
