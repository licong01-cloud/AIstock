# AIstock backend-main 用户重启与只读验证 Runbook

## 适用范围

本 runbook 只描述 `docs/standards/aistock_runtime_targets_v1.yaml` 中 `backend-main` 的用户操作边界。
BUG、PR、CI、合入、aftercare 或 close-sync 均不授予 Codex、Claude Code、CI 或其他窗口启动、停止、重启用户后端的权限。

## 前置条件

1. 目标源码 PR 已合入 `main`，记录完整 merge SHA。
2. 生产 checkout 已安全同步到该 merge SHA，且工作树无未解释修改。
3. `production_ddl_gate`、frontend/backend dependency gate 分别完成或明确为 `noop`。
4. 用户明确决定本次重启的目标是 `backend-main`；任何其他服务不在本 runbook 范围内。

## 用户操作

用户使用现有 AIstock 后端启动方式重启 `backend-main`。本仓库 workflow 只输出目标、预期 identity 和只读验证命令，不执行进程控制。

## 重启后只读验证

在仓库最新 `main` 上执行：

```powershell
python scripts/aistock_issue_workflow.py post-restart-verify `
  --bug-id BUG-NNN `
  --target backend-main `
  --expected-identity <MERGE_SHA>
```

必须同时满足：

1. health、identity 和 BUG 指定的 business smoke 均返回 HTTP 2xx；
2. `GET http://127.0.0.1:8001/api/v1/runtime-identity` 的 `merge_commit` 等于本次合入 SHA；
3. business smoke 回读的是该 BUG 要求的持久化业务合同，不以日志消失代替；
4. receipt 的 `runtime_identity_match=true`、`post_restart_effective_gate=passed`。

任一条件失败时保持 GitHub Issue 打开并停止 close-sync；不得通过再次重启、修改 receipt、降级 probe 或手工标记成功绕过失败。

## 状态报告

分别报告 source merge、生产源码同步、用户重启、post-restart receipt、close-sync、数据库变化和依赖 gate。数据库 DDL/DML、模型训练、selection、model/READY 不由本 runbook 授权。
