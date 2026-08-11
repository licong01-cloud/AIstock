# AIstock Issue Workflow Quickstart

示例触发语句：`按规范修复 BUG-112`。

生产门禁分别报告 `production_ddl_gate`、`production_frontend_dependency_gate` 和 `production_backend_dependency_gate`；不适用时为 `noop`。

本文件只提供命令入口，不定义第二套规范。规则语义以 `aistock_development_standard_v1.5_20260523.md` 为唯一权威，任务分流以 `docs/standards/README.md` 和当前 lane 为准。

## 直接入口

已有 BUG：

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

已有 workflow 状态或新窗口恢复：

```powershell
python scripts/aistock_issue_workflow.py resume --bug-id BUG-XXX
```

新 BUG（先声明精确文件范围）：

```powershell
python scripts/aistock_issue_workflow.py submit-bug `
  --title "<title>" --module <module> --severity P1 `
  --description "<description>" --expected "<expected>" --actual "<actual>" `
  --reproduce-command "<command>" `
  --changed-file <path> --create-github --create-fix-worktree --apply
```

需要新增文件时使用重复的 `--added-file <path>`；范围变化先更新 BUG 登记，再继续编辑。

## Doctor 只用于诊断

普通任务直接使用 `run`、`resume`、`submit-bug` 或当前 lane，不把 `doctor` 当作通用前置门禁。仅在以下情况运行一次：

- 客户端/bootstrap 状态未知；
- workflow/client 代码刚变更；
- 恢复状态 stale 或 conflict；
- 用户明确要求诊断。

```powershell
python scripts/aistock_issue_workflow.py doctor
```

按具体失败项修复；不要循环运行全量 doctor。当前 lane 客户端校验使用：

```powershell
python scripts/aistock_issue_workflow.py verify-clients --workflow-only --selected-lane <lane>
```

## 实现与本地验证

在返回的 task worktree 中工作，只修改 `allowed_write_scope`。本地保留最小安全门禁：

1. changed-file lint/compile；
2. 直接 fix-point test 或 contract smoke；
3. `git diff --check`；
4. scope/ownership check；
5. production gates 状态。

失败后先重跑失败 nodeid 或 `pytest --lf`；行为稳定后只运行一次相关小矩阵。宽模块、UI/API/business-flow、LLM drift 或跨模块回归交给 Validation Center、CI 或 Nightly。

workflow/client 文件变更增加：

```powershell
python scripts/aistock_issue_workflow.py workflow-smoke --bug-id BUG-XXX --changed-file <path> --module validation
```

## PR 前与 PR

```powershell
python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr `
  --validation-evidence "<command> -> passed" --push --create-pr
```

只有用户明确要求 commit/push/PR 时才执行相应写入选项。

## 合入与 aftercare

BUG PR 优先使用 `merge-finalizer` 或 `aistock-merge-aftercare` lane。授权按动作和目标独立，但可以在一条用户指令中打包；若该指令明确列出 merge、精确 cleanup targets、具体 production target/migration，合入后直接继续已授权动作，不再二次询问，并分别记录状态。裸 merge 授权仅覆盖源码合入和必要 BUG/metadata 同步；以下动作不得由裸 merge 推导：

- source worktree/branch cleanup；
- production DDL/DML；
- frontend/backend dependency installation；
- client/frontend/runtime activation；
- backend start/stop/restart；
- 文件、worktree 或 branch 删除。

生产 DDL/DML 仅在 DEV receipt 通过且不可变 merge commit 已确认后，对授权目标执行 preflight、apply 和回读。未获对应授权时报告 `noop` 或 `pending`，不要推导权限。runtime BUG 在用户重启并完成 `post-restart-verify` 前保持 `fixed_source_pending_user_restart`，Issue 保持打开。

## 安全批处理

仅当 module、risk、scope、验证链与生产 gate 相容时才使用 batch；否则记录 split reason：

```powershell
python scripts/aistock_issue_workflow.py start-batch --bug-id BUG-AAA --bug-id BUG-BBB
python scripts/aistock_issue_workflow.py finish-batch --batch-id <BATCH-ID> --plan-only
```

每个 BUG 仍保留独立验收、证据、commit 映射与 GitHub closure。

## 停止条件

- 实际改动超出登记 scope；
- 必需测试或 CI 失败；
- PR 不可合入或来源 worktree 不干净；
- runtime identity、DDL、依赖或生产授权缺失；
- 发现并发修改、未知 dirty 文件或需要未授权的进程/删除操作。

最终报告分别列出 branch、commit、PR、changed files、验证、BUG/Issue、merge、close-sync、root sync、cleanup、production gates、client/runtime 激活，以及 runtime/DB 是否被触碰。
