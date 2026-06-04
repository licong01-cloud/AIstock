# 2026-06-05 双策略包模拟盘重启后准备验收

## 范围

- 目标交易日：2026-06-05。
- 验收目标：确认 AIstock LocalSim 多策略包与 MiniQMT 多策略包具备明早无值守模拟盘验证条件。
- 操作边界：未重启/停止 backend、frontend、TDX；未触发 simulation scheduler tick；仅做只读 API/DB 验收、TDX/QMT smoke 和本地 pytest。

## 关键结论

- `paper_v2.simulation_release_binding` 对 2026-06-05 有 4 条有效绑定：`local_sim=2`、`minqmt_sim=2`。
- 四条绑定对应 release 均带完整 `execution_policy.policy_json`，不再是 id-only release。
- LocalSim 使用显式 `V25_1_SMALL_CAP` policy snapshot；MiniQMT 使用 `SNIPER_MINIQMT` vn.py-style policy snapshot。
- `paper_v2.simulation_daily_run` 在 2026-06-05 当前为 0 条，未提前污染明日 run。
- QMT API：`enabled=true`、`connected=true`、`mode=SIM`、账号 `62266303`。
- TDX API：`/api/health` healthy，`/api/server-status` connected，`/api/quote?code=000001` 返回 1 条行情。
- Scheduler：`autostart=true`、`default_submit=true`、provider ready；同日 LocalSim 数据源策略为 `TDX_REALTIME`，MiniQMT 为 `MINIQMT_REALTIME`。

## Runtime 注记

- 重启后 runtime loaded commit 为 `2f76d1cd`，当前 `origin/main` 为 `3e7cdcfc`。
- 差异文件仅为 issue workflow skill/docs/scripts/test：`.codex/skills/fix-aistock-issue/SKILL.md`、`backend/tests/scripts/test_aistock_issue_workflow.py`、`docs/standards/aistock_issue_workflow_quickstart.md`、`scripts/aistock_issue_workflow.py`。
- `loaded_source_matches_disk=true`，本次模拟盘相关 backend source 未发现未加载差异；该 commit mismatch 不作为明早模拟盘阻断。

## 验证命令

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/research-assistant/health
Invoke-RestMethod http://127.0.0.1:8001/api/v1/simulation-runtime/scheduler/status
Invoke-RestMethod http://127.0.0.1:8001/api/v1/qmt/status
Invoke-RestMethod http://127.0.0.1:19080/api/health
Invoke-RestMethod http://127.0.0.1:19080/api/server-status
Invoke-RestMethod 'http://127.0.0.1:19080/api/quote?code=000001'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest backend/tests/simulation_runtime/test_strategy_runtime_release.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_localsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_production_context_provider_rejects_stale_portfolio_policy_when_release_policy_is_vnpy_id_only backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_production_context_provider_uses_runtime_release_policy_snapshot_over_portfolio_default backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_production_context_provider_uses_tdx_realtime_for_same_day_localsim -q
```

## 结果

- Pytest：37 passed in 1.47s。
- Post-restart log scan：20:29:18 后无 `ERROR`、无 `SESSION_LOCK_TIMEOUT`、无 `RuntimeConfigInvalidError`；仅有 QMT connected、simulation scheduler started、HMM scheduler dependency warning。

## 证据文件

- `tmp/validation/tomorrow_dual_strategy_prep_20260604/12_post_restart_readiness_check.json`
- `tmp/validation/tomorrow_dual_strategy_prep_20260604/13_post_restart_log_scan.json`
- `tmp/validation/tomorrow_dual_strategy_prep_20260604/14_post_restart_targeted_pytest_result.json`

## 生产门禁

- production_ddl_gate: noop，本轮验收未执行 DDL。
- production_backend_dependency_gate: noop。
- production_frontend_dependency_gate: noop。
- production runtime touched by Codex: false。
- scheduler tick triggered by Codex: false。

## 明早执行窗口

- 08:50-09:10：只读检查 backend/QMT/TDX/scheduler/active bindings。
- 09:10-09:20：观察 selection evidence 生成。
- 09:20-09:25：观察 execution plan 生成。
- 09:25-15:00：观察 LocalSim submit/reconcile 与 MiniQMT managed orders/reconcile。

## 20:42-20:49 追加复核

- 端口进程快照：backend `8001` 为 `python` PID `27356`，启动时间 `2026-06-04T20:29:01+08:00`；TDX `19080` 为 `web` PID `13384`；frontend `3000` 为 `node` PID `109784`。
- 只读 API/DB 复核结论：`overall_status=ready_for_tomorrow_unattended_dual_strategy_simulation`，阻断项为空。
- Paper v2 session scheduler：`running=true`、`auto_run.env_enabled=true`。
- Simulation runtime scheduler：context provider `ready=true`、`autostart=true`、`default_submit=true`；LocalSim 同日数据源策略为 `TDX_REALTIME`，MiniQMT 数据源策略为 `MINIQMT_REALTIME`。
- QMT：`connected=true`、`mode=SIM`；TDX health/server-status/quote smoke 均成功。
- DB：2026-06-05 有效绑定仍为 4 条，`local_sim=2`、`minqmt_sim=2`；四条绑定均带完整 `execution_policy.policy_json`；2026-06-05 `simulation_daily_run` 仍为 0。
- 分钟线：本地分钟库 ready through `2026-06-04`；明日盘中同日分钟线按策略从 TDX 实时源取数，不要求 DB 盘中已有 `2026-06-05` 分钟线。
- Post-restart log scan：`20:29:01` 后无 `ERROR`、无 `SESSION_LOCK_TIMEOUT`、无 `RuntimeConfigInvalidError`、无 tick failed 阻断日志。
- Targeted pytest 复跑：37 passed in 1.65s。

追加证据：

- `tmp/validation/tomorrow_dual_strategy_prep_20260604/15_followup_process_ports_readonly.json`
- `tmp/validation/tomorrow_dual_strategy_prep_20260604/16_followup_post_restart_readiness_check.json`
- `tmp/validation/tomorrow_dual_strategy_prep_20260604/17_followup_post_restart_log_scan.json`
- `tmp/validation/tomorrow_dual_strategy_prep_20260604/18_followup_targeted_pytest_result.json`
