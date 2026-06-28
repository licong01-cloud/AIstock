# Tier2 审查结论 — combine-backtest 远端派发 Phase 1

- 审查人:Claude(战略 session)
- 日期:2026-06-28
- 范围:Codex Phase 1 两仓库改动(AIstock `feature/combine-remote-dispatch-phase1-20260628` + RDAgent `feature/qe-workspace-artifact-store-20260628`)
- 权威设计:`docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`(已在 main)

## 总评:✅ 通过(本地实现层),F-007 阻塞为真实且 Codex 处理正确

实现质量高,自审 handoff 诚实完整(F-007 标 blocked、F-005 标 not_in_phase1、明确"不得合入声明"、L4 阻塞证据具体)。代码符合设计 F-001~F-011 的 Phase 1 范围。同意 Codex 判断:**远端部署 WAS 前不得合入 main**。

## 已核验的正确点(实质审查,非仅信自报)
1. **F-006 零回归**:`combine_backtest.py:_executor_for_node` 本地分支返回原 `ShellPredBacktestExecutor`;`is_remote_compute_node` 基于 `infra.compute_nodes` 注册判定,非字符串猜测。本地路径行为不变。✅
2. **方法接缝真实存在**:`QEWorkspaceClient.get_workspace_file`(:290)/`get_loop_status`(:111)/`create_and_run_loop` 均存在,无 AttributeError 风险。✅
3. **远端 loop_id 一致性**:远端 `loop_id = f"Loop{loop_index}"`(qe_evolution_api.py),Codex 小文件上传路径用 `Loop{loop_index}/{name}`(remote_dispatch.py:345)——一致。✅
4. **RDAgent WAS 端点健壮**:sha256 正则校验 + 不匹配 400 + 删临时文件 + 原子 rename + 独立 store env(`QE_WORKSPACE_ARTIFACT_STORE`)。✅
5. **no-silent-error**:全链路 reason_code 显式(node/path/artifact/sync/remote failure/timeout/result invalid),失败 loud 带上下文。✅
6. **路径解耦 fail-loud**:`_require_remote_linux_path` 拒绝 Windows 路径 / `/mnt/` 前缀,防本机路径发远端。✅
7. **测试**:本地复跑 `test_multi_alpha_remote_dispatch.py` = 19 passed(与自报一致);55 passed 全量;coverage 77.13%。✅

## ⚠️ L4 必须验证的集成风险(批准附带条件)
**[R-1 高] workspace 根目录 env 不统一**:
- 小文件经 `qe_file_sync` 上传,落 `QE_WORKSPACE_ROOT`(qe_file_sync_api.py:32)下 `{task_id}/Loop{i}/`。
- loop 执行 cwd 由 `QE_WORKSPACE_WSL`(qe_evolution_api.py:57)决定。
- WAS artifact store 用第三个 env `QE_WORKSPACE_ARTIFACT_STORE`。
- **风险**:若远端 `QE_WORKSPACE_ROOT` ≠ `QE_WORKSPACE_WSL`,上传的小文件(conf.yaml/pred.pkl/qrun 脚本)不会出现在 loop 的执行 cwd → 远端 run 找不到文件跑空/跑错。
- **L4 验收必须确认**:远端 215 上这两个 env 指向同一目录;否则 Codex 需在 wsl_command 里显式 cd 到上传目录,或统一 env。

**[R-2 中] symlink 落点**:`_remote_wsl_command` 用 `ln -sfn artifact_path combined_factors_df.parquet`(相对 cwd)。同样依赖 cwd = 上传小文件的目录。与 R-1 同源,L4 一并验证。

**[R-3 低] F-007 数值对账容差**:L4 真实远端 run 与本地基线对账,需明确容差(浮点/环境差异);完全逐位相等不现实,应给出可接受阈值(如 CAGR/Sharpe 相对误差 <1e-6 或说明环境性差异来源)。

## 阻塞项(Codex 已正确标注)
- **F-007 blocked**:远端 215 未部署本轮 RDAgent WAS 代码(`GET /artifacts/{sha}` 返回 404)。需先部署/重启远端 RDAgent → 再做真实 combine run 数值对账。
- **F-005 not_in_phase1**:容量守卫统一属 Phase 2,本轮不做(符合设计)。

## 放行路径(给用户的下一步)
1. **部署远端 WAS**:把 RDAgent `feature/qe-workspace-artifact-store-20260628` 部署到 192.168.50.215 并重启其 9000 服务(用户操作,我不启动服务)。同时确认远端 `QE_WORKSPACE_ROOT`/`QE_WORKSPACE_WSL`/`QE_WORKSPACE_ARTIFACT_STORE` 三 env 配置(R-1)。
2. **Codex 做 F-007 真实远端 run**:端到端跑一次,数值对账本地基线;过程中验证 R-1/R-2/R-3。
3. F-007 verified 后 → 两仓库各开 PR → 我终审 → 合入。

## 结论
本地实现 **Tier2 通过**;F-007 真实远端验证 + R-1/R-2/R-3 是合入前必须闭环的硬门。Codex 未提交/未开 PR 的处置正确,无需返工,等远端部署后继续。
