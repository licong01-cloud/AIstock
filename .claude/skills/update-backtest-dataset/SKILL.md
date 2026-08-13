---
name: update-backtest-dataset
description: Operate the durable, candidate-only AIstock monthly QE backtest dataset release workflow, including monthly update, NO_OP, re-attestation, catalog/status, resource blockers and signoff. Never activates or overwrites production without separate explicit authorization.
---

# 月度更新 AIstock QE 回测数据集

这是 thin pointer，不复制第二套流程或策略。

1. 定位当前 repository root。
2. 完整读取并遵循 `../../../.codex/skills/update-backtest-dataset/SKILL.md`。
3. 按该 Skill 的路由只读取一个或多个直接 references。
4. 人类操作步骤、一次性 runtime 准备、`--preflight`、Worker 授权和签收命令使用
   `../../../docs/operations/qe_backtest_dataset_monthly_update_runbook.md`。

当前文件不保存 cutoff、candidate、Worker、production 或审批状态；以 live profile、control catalog 和 receipt 为准。
真实数据执行、进程控制、production activation、DB mutation、依赖安装和 cleanup 始终分别授权。
