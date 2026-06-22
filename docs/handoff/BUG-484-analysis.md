# BUG-484 analysis — unattended pre-run binding failure durable audit

## 独立根因结论

- SimulationLifecycleScheduler.run_once() 在逐 binding 调用 _run_binding() 时，会捕获 DataUnavailableError / RuntimeConfigInvalidError。
- 当 aise_on_error=False（无值守 background scheduler 默认路径）时，当前代码只返回一个内存态 SimulationSchedulerBindingResult(status= FAILED)，其中包含 	ype/message/context，但没有创建或更新 paper_v2.simulation_daily_run。
- _run_binding() 的 durable run 创建发生在 selection evidence 生成后，由 SimulationLifecycleOrchestrator._create_or_load_run() 完成；如果失败发生在 context_provider.load_context()、selection、freshness validation 或 plan build 之前，就可能完全没有 durable run / plan / evidence。
- 因此 active binding 在 pre-run 失败后会消失式跳过：本 tick 返回里有 transient error，但 API/UI/监控只读 daily runs 时看不到当天失败原因，违反 no-silent-error。

## durable 手段盘点

- 现有最贴合模型是 paper_v2.simulation_daily_run：它已经被 SimulationRuntimeOpsService.list_runs/get_run_detail 投影到 API/UI/监控，并且有 (strategy_id, binding_id, trade_date) 唯一语义。
- SimulationRuntimeRepository.save_simulation_daily_run() 与 InMemorySimulationRuntimeRepository.save_simulation_daily_run() 都会先按 (strategy_id, binding_id, trade_date) 查 existing；这天然支持同一 binding 当日重复 tick 去重。
- update_simulation_daily_run() 会合并 un_payload_json，适合写入 pre_run_failure / submit_failure / last_stage 这类审计字段。
- 不需要新建孤立审计表；使用 deterministic simulation_daily_run_identity_v1（同 lifecycle _create_or_load_run()，但 trade_date 来自 scheduler 参数）创建 FAILED_RETRYABLE daily run，后续故障消除后同一 binding/date 可复用该 run 继续生成 selection/plan。

## 修复方案

- 在 un_once() 的 aise_on_error=False except 分支中调用 durable helper，先拿 untime_release，再创建或更新 deterministic daily run。
- 写入内容包含 eason_code、inding_id、strategy_id、roker_backend、	rade_date、data_source、error type/message/context、roker_called=False、submitted_intents=0、ailed_intents=0。
- 同一 binding 当日重复失败只更新同一 run 的 pre_run_failure.last_observed_at 与 observed_count，不插入重复行，防止 30 秒 tick 刷屏。
- 保持 per-binding 隔离：失败 binding 返回 FAILED_RETRYABLE result，其他 binding 继续执行；aise_on_error=True 仍原样抛出。
- 不改变 MiniQMT 执行、终态、EOD fresh reconcile 或 submit_result_gate 语义；仅在 shared pre-run catch 层补 durable audit。

## L16 关联结论

- Issue 描述的 L16 现象（active binding、缺 2026-06-22 durable run/plan/selection evidence、portfolio stuck at 2026-06-17）与该 silent pre-run 失败路径一致。
- 由于历史失败没有 durable row，无法从当前 DB/API 反推出当时 exact exception；这正是 BUG-484 的根因。
- 修复后，如果 L16 或同类 binding 再在 context/selection/plan 前失败，simulation_daily_run.run_payload_json.pre_run_failure 与 API errors[] 会暴露具体 reason_code/context，运营可见且可审计。

## 与 issue 描述差异

- 无实质分歧。
- 补充：选用 FAILED_RETRYABLE 而非 terminal failure，因为该阶段没有 broker side effect，修复数据/配置后应允许同日后续 tick 复用同一 run 继续推进。
