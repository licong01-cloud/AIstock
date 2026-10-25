# HMM database Risk L2 流水线交付与用户重启验证

## 范围与依据

本 runbook 对应 runtime catalog 的 `backend-main / hmm_database_risk_l2_readback`。
仅为 `backend/services/hmm_risk/risk_l2_database_inference.py` 提供缺省引用，不改变其 backend 分类。
BUG 中显式指定的引用优先，错误或空引用继续阻断。混入其他运行态源码时不套用本路由。

identity 使用现有 `GET /api/v1/runtime-identity`：返回后端导入时冻结的完整 Git SHA，必须与本次合入 SHA 一致。
business smoke 使用现有只读 `GET /api/v1/hmm-risk/risk-l2/overview`，默认绑定已持久化 run：
`88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26`。
该 run 也用于 BUG-2065、BUG-2068 的只读验收引用；它不是本次 BUG-2082 的新训练或发布结果。
数据不存在、返回空对象、身份不一致或语义不完整均不能通过，不自动生成替代 run。

## 合入前的配置预检

HMM 窗口在自己的工作树安全同步包含 BUG-2084 的主线后，使用 canonical 脚本：

```powershell
python F:/Dev/AIstock/scripts/aistock_issue_workflow.py finish --bug-id BUG-2082 --plan-only
```

应消除 operator_runbook_ref、identity_ref、business_smoke_ref 三项缺失。
`runtime_impact=backend`、`target_ids=[backend-main]`、`backend_restart_required=true` 不变。
已有源码与真实只读预检证据保留；无需重新训练。缺失的 fresh-process 证据等其他门禁仍然阻断。
`pre_pr_ready=true` 仅表示交付配置及源码证据就绪，不代表合入、用户重启或业务验收完成。

## 合入及用户重启

1. HMM 源码 PR 合入后记录完整 merge SHA，并安全同步运行 checkout。
2. DDL、依赖 gate 分别完成或明确 noop。本 runbook 不授权任何 DDL/DML。
3. 用户自行重启 backend-main。本窗口、CI 与工作流均不执行服务控制。
4. 用户确认重启完成后，在最新主线执行只读验证，将实际 merge SHA 传给 `--expected-identity`：

```powershell
python F:/Dev/AIstock/scripts/aistock_issue_workflow.py post-restart-verify `
  --bug-id BUG-2082 --target backend-main --expected-identity $hmmMergeSha
```

命令要求 health、冻结运行身份和业务语义同时成立。HTTP 200 本身不能通过业务验收。
现有 `hmm_risk_l2_overview` validator 校验请求 run_id、model/input/acceptance/row 哈希、
131 行业平衡计数、PIT 日期关系、`tail_accessed=false` 与 `AVAILABLE_EXPERIMENTAL` 正式表面回执。
只有 digest 绑定的 receipt 给出 `runtime_identity_match=true` 且 `post_restart_effective_gate=passed`，
才可以继续相应 close-sync。失败时保留 Issue 打开，不切换端点或伪造 receipt。

## 验收界限

上述 smoke 证明重启后的 backend-main 可以回读现有 HMM Risk L2 持久化业务合同；
它不重新执行 database inference，不证明 BUG-2082 的数据库输入修复已在一次新业务计算中生效。
BUG-2082 已有的只读数据库预检与源码测试是独立证据；后续正式业务验收应保留真实输入、
authority、模型及预检 receipt 的身份绑定，由 HMM 窗口按原合同处理，不能以本次配置解阻代替。
不训练、不写库、不启动 QE、不激活 authority，不更改模型或业务合同。
