# QE GPU Phase Pipeline 与资源遥测 F2 设计

> 文档状态：`implementation_verified_local`  
> Feature tier：`F2`  
> 日期：2026-07-13  
> 范围：QuantEvolver custom-evo 调度、EfficientGATs GPU 生命周期、QE runner、QE Archive、只读查询 API/MCP。

## Background

当前 custom-evo 的 `node_parallelism` 信号量覆盖“提交、训练、预测、回测、完成处理”整个 Loop。GPU 模型完成预测并进入 CPU 分钟回测后，后续 Loop 仍不能开始训练。另一方面，EfficientGATs 的 resident tensor 在 `predict()` 返回前没有确定性释放与释放后校验；仅调用 `torch.cuda.empty_cache()` 不能证明本地引用已经消失。

2026-07-13 的运行审计显示，单个 G17/EfficientGATs Loop 在 RTX 5080 16GB 上训练/预测可占用约 9.3GB 显存，而 CPU 回测阶段远长于训练阶段。两个 resident 训练不应并行，但“一个 Loop CPU 回测 + 下一个 Loop GPU 训练”具有明显价值。

本功能同时补齐阶段级资源事实：GPU 峰值显存、GPU 利用率、进程树 RSS/VmHWM、阶段时间和 resident fallback，固化到 QE 数仓供后续容量规划与实验复核使用。

## Scope

### 目标

1. 对显式启用的 custom-evo 任务提供模型感知阶段流水线：每节点总 Loop 并发仍由 `node_parallelism` 控制；GAT 类模型以独占租约串行训练，其他模型以共享租约保持原并发能力；前一独占 Loop 的 GPU 阶段确认释放后，后一 Loop 可开始训练，前一 Loop 继续 CPU 回测。
2. EfficientGATs 在 fit/predict 的所有正常、streaming fallback 和异常路径上确定性清理 resident tensor，并对释放后的 CUDA allocated/reserved 做阈值校验。
3. runner 以固定采样周期采集进程树和 GPU 资源，按阶段聚合并通过带单次运行 token 的结构化事件写入 QE Archive。
4. 提供按 run/task/loop 查询阶段资源事实的后端只读 API 和 QE MCP 工具。

### Non-Goals

- 不允许 GAT/EfficientGATs 与任何其他 GPU 训练阶段同时运行。
- 不改变模型、标签、因子、策略、交易成本、回测结果或已有 `node_parallelism` 默认语义。
- 不从日志文本推断阶段，不把“GPU 利用率低”当作释放证明。
- 不中断、迁移或重新配置 2026-07-13 已在运行的 QE 实验。
- 不在运行时自动执行 DDL，不在本任务中应用生产 migration、重启服务或启动新实验。
- 第一版不提供前端图表；结构化 API/MCP 已形成后续 UI 可直接消费的稳定契约。

## Current Gaps

1. `submit_custom_evo_all_loops()` 和 selected-loop 路径都用一个 Semaphore 包住完整 Loop。
2. DB slot 只理解 Loop active/terminal，不理解 GPU phase released。
3. EfficientGATs `fit()` 的 `empty_cache()` 执行时 resident local 仍然存活；`predict()` 返回前没有 finally 清理。
4. runner 没有稳定的 phase event、资源采样器和幂等上传契约。
5. QE Archive 的 `run_metric`/`run_model_training_metric` 不适合表达每阶段起止、峰值、fallback 与 release proof。

## Architecture

目标架构由四个边界清晰的组件组成：custom-evo 双资源调度器负责并发安全，runner resource helper 负责采样和阶段事件，EfficientGATs 负责可证明的 CUDA 生命周期释放，QE Archive session/phase 表负责运行态协调与历史事实的唯一持久化。调度器只消费已认证的结构化事件；模型和 runner 不直接修改调度状态。

## Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-001 | EfficientGATs 在 fit/predict 的正常、fallback、异常路径确定性删除 resident 引用、同步 CUDA、执行 GC/empty-cache，并输出结构化释放证明。 |
| F-002 | 阶段流水线使用“每节点总 Loop 槽位 + 模型感知共享/独占 GPU phase gate”双资源模型；GAT 独占训练，其他模型保持共享并行。 |
| F-003 | 只有已认证、顺序合法且显存释放校验通过的 `gpu_phase_released` 事件才能提前释放 GPU 租约；否则按该模型策略保持租约到 Loop terminal。 |
| F-004 | all-loops、selected-loops、retry/rerun 复用同一阶段等待器和失败语义，不形成旁路。 |
| F-005 | 新能力显式 opt-in、默认关闭；未启用任务保持现有 whole-loop `node_parallelism` 行为和业务结果。 |
| F-006 | runner 按 bootstrap/train/predict/backtest/finalize 阶段采集 GPU 峰值、GPU utilization、进程树 RSS/VmHWM、时间和 CUDA allocated/reserved。 |
| F-007 | resident requested/active/fallback/release proof 以 reason code 和结构化字段固化，不允许静默 fallback。 |
| F-008 | QE Archive 使用专用 session/phase 表，事件幂等、可在 archive run 创建前写入，并在归档后绑定 run_id。 |
| F-009 | phase webhook 使用每运行随机 token 的 SHA-256 校验、任务/Loop/节点绑定和单调 sequence；重放不得倒退阶段。 |
| F-010 | 后端只读 API 与 QE MCP 支持 run_id、task_id、loop_index、source_run_key 过滤和有界 limit。 |
| F-011 | 停止、超时、回调失败、后端重启和不支持阶段契约均 fail-closed：不提前释放 GPU 槽位，不伪造成功，不中止原实验。 |
| F-012 | migration、代码合入、生产 DDL、服务重启、运行激活分别汇报并可独立回滚。 |
| F-013 | 模型训练策略只分 `exclusive`/`parallel` 两类：GAT/EfficientGATs 必须为 `exclusive`，其他模型默认 `parallel`；允许非 GAT 模型通过 catalog `model_config.gpu_training_policy` 显式收紧为 `exclusive`。 |
| F-014 | 每节点使用共享/独占 GPU phase gate：`exclusive` 与任何训练互斥，多个 `parallel` 共享租约可按现有 `node_parallelism` 并行；等待中的独占租约优先，避免 GAT 饥饿。 |
| F-015 | 模型策略写入 Loop config 与 resource session；非法策略或把已知 GAT 声明为 `parallel` 必须 fail-fast，不允许静默绕过独占门。 |
| F-016 | 后端重启不得终止已提交到 QE Workspace 的实验；GPU 租约必须以数据库原子预留为跨进程真值，runner 本地 outbox 在后端恢复后按 sequence 幂等重放，启动扫描器立即对账 terminal session。 |

## Contracts

### 1. 任务配置

custom-evo task 新增任务级配置：

```json
{
  "phase_pipeline_enabled": true,
  "resource_telemetry_enabled": true,
  "node_parallelism": {"wsl2-5080": 2}
}
```

- `phase_pipeline_enabled` 默认 `false`。
- `resource_telemetry_enabled` 默认 `false`；启用 phase pipeline 时必须同时为 `true`。
- `node_parallelism` 仍是节点上的总 active Loop 上限；要产生重叠必须至少为 2。
- GPU phase gate 不提供独立数字并发参数：`parallel` 数量继续由既有 `node_parallelism` 控制，`exclusive` 固定为独占。
- backtest-only Loop 不占 GPU phase slot；full-train Loop 必须先获得 GPU phase slot。
- 仅 runner 明确支持 release contract 的模型可以提前释放。EfficientGATs 可在真实释放证明后解除独占租约；原生 GAT 保持独占到 Loop terminal。非 GAT `parallel` 模型没有通用预测完成回调，因此共享租约保持到 terminal，但多个共享 Loop 仍可并行。

模型 catalog 可在 `model_config` 中声明：

```json
{
  "gpu_training_policy": "exclusive"
}
```

已知 `GATs`/`EfficientGATs` 根据规范化后的 `model_config.class`、`model_name` 或 `model_type` 强制解析为 `exclusive`，不得覆盖为 `parallel`。未声明的其他模型解析为 `parallel`，保持 LSTM/TCN/Transformer 等既有并发语义。

### 2. 阶段状态机

```text
created -> bootstrap -> train -> predict -> gpu_phase_released -> backtest -> finalize
                                    |                                |
                                    +-> release_rejected             +-> completed
                                                                     +-> failed
```

事件必须带 `sequence_no`，同一 session 只接受单调递增。`gpu_phase_released` 还必须带：

- `release_check_passed=true`
- `cuda_allocated_bytes_after`
- `cuda_reserved_bytes_after`
- `release_baseline_reserved_bytes`
- `release_tolerance_bytes`
- `reason_code=QE_GPU_PHASE_RELEASE_CONFIRMED`

若释放阈值未通过，runner 写 `release_rejected`，调度器继续持有 GPU slot 直到 Loop terminal。

### 3. 双资源调度

```text
loop coroutine
  -> acquire total_loop_semaphore(node)
  -> resolve model gpu_training_policy
  -> if full_train: acquire model-aware gpu phase lease(node, policy)
       exclusive: wait until no exclusive/shared lease
       parallel:  wait until no exclusive/waiting-exclusive lease
       atomically check conflicts + insert reserved resource session under node advisory transaction lock
  -> submit loop
  -> wait for authenticated gpu_phase_released OR terminal
  -> release gpu phase lease exactly once
  -> keep total_loop_semaphore until terminal + result processing
```

默认关闭时仍执行原 whole-loop `node_parallelism` 路径。流水线开启后，第二个 coroutine 可以先占总 Loop 槽位并等待模型感知租约。多个 `parallel` Loop 可直接提交；`exclusive` Loop 会等待所有共享租约结束，且其等待期间阻止新的共享租约插队。进程内 gate 在同一事件循环的所有 scheduler 实例间共享，只负责公平排队；数据库在节点级 transaction advisory lock 下把冲突检查与 resource session 插入合并为一次原子预留，负责跨 scheduler、跨进程和重启后的安全。只有 `gpu_phase_released_at` 或真实 Loop terminal 才解除持久化冲突；`release_rejected`、未带释放证明的 `backtest/finalize` 均继续占用。DB `node_parallelism` 继续限制 active Loop 总数，不替代 GPU phase gate。

### 4. Runner phase/session 事件

runner 使用工作区内的 `qe_runtime_resource.py`：

- 启动 daemon sampler，默认每 1 秒采样一次。
- 进程资源按当前 PID 及递归子进程聚合；`rss_peak_bytes` 取阶段内样本峰值，`vm_hwm_bytes` 取 `/proc/<pid>/status` 可得的最大 HWM。
- GPU 资源同时记录设备级 utilization/memory.used 与当前进程组 used memory；CUDA 可用时记录 PyTorch allocated/reserved/peak。
- phase transition 先按单调 sequence 原子落本地 `qe_runtime_resource.json` outbox，再上传结构化事件；服务端已接受但响应丢失时，重复上传由 event hash 判定为幂等成功。
- 上传失败写 `qe_runtime_resource_upload_failure.json` 并输出 error reason code。后台 sampler 在实验继续运行期间定时重试，下一 phase 也会按 sequence 从最早未确认事件重放；恢复后写结构化 recovered marker。普通 telemetry 上传失败不改变实验结果；流水线所需 release 事件在服务端确认前调度保持串行。
- runner terminal 阶段提供有界 final drain；超过窗口仍不可达时不得伪造上传成功，Loop terminal 对账可安全解除调度占用，本地 outbox 保留完整未上传事实。
- runner `finally` 必须结束 sampler 并提交 terminal/finalize aggregate。

### 5. EfficientGATs release contract

- `fit()` 使用 try/finally，resident train/valid tensors 在 finally 中显式删除；该阶段只记录训练资源，不宣称 GPU 已释放，因为随后仍需预测。
- `predict()` 统一包裹 resident 和 streaming 路径；输出 Series 已转 CPU 后，显式删除 test resident/batches，`torch.cuda.synchronize()`、`gc.collect()`、`torch.cuda.empty_cache()`。
- release 校验以进入 fit 前清理后的 reserved baseline 加固定 tolerance 为上限；阈值参数有保守默认值并可作为模型参数透传。
- release proof 由模型调用 runner helper 生成；未加载 helper 时仅在未启用 phase pipeline 的旧工作区兼容。若 pipeline env 已启用但 helper 缺失，必须输出 `QE_GPU_PHASE_HELPER_MISSING` 并且不能写 release success。
- 模型权重不移到 CPU，保持 qlib 保存/加载契约；释放对象是 resident datasets、临时 batch、prediction CUDA tensor 和 allocator cache。

## Persistence Design

新增两个 additive 表，均位于 `qe_archive`：

```text
qe_archive.run_resource_session
  session_id PK                  # 每次真实提交唯一
  source_run_key                 # qe_<task>_L<index>
  attempt_no
  task_id, loop_id, loop_index, node_id
  archive_run_id NULL FK qe_archive.run
  token_sha256
  phase_pipeline_enabled
  gpu_training_policy             # exclusive / parallel
  current_phase, last_sequence_no, status
  gpu_phase_released_at
  created_at, updated_at, completed_at
  UNIQUE(source_run_key, attempt_no)

qe_archive.run_resource_phase
  id BIGSERIAL PK
  session_id FK session
  source_run_key
  sequence_no
  phase, phase_status
  started_at, ended_at, duration_seconds
  sample_count
  process_rss_peak_bytes, process_vm_hwm_peak_bytes
  gpu_device_index, gpu_name
  gpu_memory_used_peak_bytes, gpu_process_memory_peak_bytes
  gpu_utilization_avg_pct, gpu_utilization_peak_pct
  cuda_allocated_peak_bytes, cuda_reserved_peak_bytes
  cuda_allocated_end_bytes, cuda_reserved_end_bytes
  resident_requested, resident_active, resident_fallback
  fallback_reason_code, release_check_passed, reason_code
  metadata JSONB
  UNIQUE(session_id, sequence_no)
```

不把这些多维阶段记录展开为大量 `run_metric`，避免字段语义丢失与冗余。每次 retry/rerun 使用新的 `session_id/attempt_no`，避免覆盖旧资源事实。archive ingestion 创建 `qe_archive.run` 后，按 task/loop/attempt 绑定 `archive_run_id`；session/phase 历史只保存一份。

## API and MCP

### 写入 webhook

`POST /api/v1/quantevolver/evolution/webhook/loop-resource-phase`

Header `X-QE-Resource-Token` 必填。服务端只保存 token SHA-256；raw token 只通过 worker 工作区内权限为 `0600` 的单运行 secret 文件传递，不进入 command、日志或数据库。payload 必须与 session 的 task/loop/node 绑定，sequence 单调，phase transition 合法。重复的同 sequence 同 payload 返回幂等成功；不同 payload 返回冲突。

### 只读查询

`GET /api/v1/qe-archive/resource-phases`

过滤参数：`run_id`、`task_id`、`loop_index`、`source_run_key`、`limit<=200`。返回 session 摘要和按 sequence 排序的 phase rows。

MCP 新增只读工具 `qe_archive_query_resource_phases`，参数和 API 一致，默认 limit 20；工具清单标记 `read_only/direct`。

## Failure Modes and Reason Codes

| reason_code | 行为 |
|---|---|
| `QE_GPU_PHASE_RELEASE_CONFIRMED` | 释放校验通过，可释放 GPU slot。 |
| `QE_GPU_PHASE_RELEASE_THRESHOLD_EXCEEDED` | 保持 GPU slot 到 Loop terminal，实验继续。 |
| `QE_GPU_PHASE_CONTRACT_UNSUPPORTED` | 不提前放行，按 whole-loop 串行。 |
| `QE_GPU_PARALLEL_LEASE_TERMINAL_RELEASE` | parallel 模型按既有 whole-loop 边界释放共享租约，不视为降级或失败。 |
| `QE_GPU_TRAINING_POLICY_INVALID` | catalog 策略值非法，创建/提交前 fail-fast。 |
| `QE_GPU_TRAINING_POLICY_CONFLICT` | 已知 GAT 被声明为 parallel，拒绝提交。 |
| `QE_GPU_PHASE_HELPER_MISSING` | pipeline 开启时拒绝 release success。 |
| `QE_RESOURCE_EVENT_AUTH_FAILED` | HTTP 403，不更新阶段。 |
| `QE_RESOURCE_EVENT_SEQUENCE_CONFLICT` | HTTP 409，不覆盖历史事件。 |
| `QE_RESOURCE_EVENT_PHASE_INVALID` | HTTP 409，不允许状态倒退或越权释放。 |
| `QE_RESOURCE_EVENT_UPLOAD_FAILED` | 本地 marker + error log；调度保持串行。 |
| `QE_RESOURCE_EVENT_UPLOAD_RECOVERED` | 后端恢复后 outbox 已按 sequence 全部重放并确认。 |
| `QE_GPU_PHASE_LEASE_BUSY` | 持久化租约冲突；释放进程内 gate 后排队重试，不提交冲突训练。 |
| `QE_RESOURCE_SESSION_SCHEMA_NOT_READY` | 创建/启动 pipeline task 时 fail-fast，提示待执行 DDL；运行时不自动建表。 |

## Implementation Plan

### Phase 3：GPU 阶段流水线

1. 修复 EfficientGATs resident 生命周期并增加 release proof 单测。
2. 新增 session 初始化、结构化 phase webhook 与认证/sequence 校验。
3. 扩展 custom-evo request/task snapshot 和 ExecutionContext/composer env。
4. 抽取统一 phase-aware loop runner，接入 all/selected/retry/rerun 调度路径。
5. 验证默认关闭不变、开启后 train 串行/backtest 可重叠、release 失败保持串行。
6. 增加模型能力解析和共享/独占 gate；验证 GAT 独占、LSTM/TCN 共享并行及混合模型互斥。

### Phase 5：QE 数仓资源遥测

1. 新增 runner sampler、阶段聚合和本地 marker。
2. 新增 archive migration/init schema/repository 查询与 archive run 绑定。
3. 新增只读 API、MCP wrapper、manifest 和有界响应测试。
4. 完成 migration 静态/DEV-DB 可执行性检查；生产 DDL 保持 pending。

## Verification Plan

### L0/L1

- EfficientGATs resident/streaming/failure 三类 predict 路径均执行 cleanup；release pass/fail reason code 可断言。
- phase transition、token hash、sequence idempotency/conflict、非法 release 的服务单测。
- scheduler fake loop：Loop1 release 后 Loop2 才 submit；Loop1 terminal 前总槽位不释放；release rejected/unsupported 时 Loop2 等待 terminal。
- 两个 scheduler 实例共享同一进程 gate；并发数据库预留通过节点级 transaction lock 保证 exclusive/parallel 冲突检查与 session 插入原子化。
- composer 断言仅 opt-in 命令包含 phase env/token，默认命令无变化。
- runner sampler 用 fake psutil/nvidia-smi/torch 验证峰值、均值、HWM、采样失败 reason code、outbox sequence、回调中断后恢复重放与 terminal flush。
- repository/API/MCP 的过滤、limit、排序和空结果。
- 模拟后端回调中断：运行任务不失败，`release_rejected` 不放行；恢复后 outbox 顺序重放，terminal DB 写失败也以真实 Loop terminal 安全释放本地 gate。

### L2/L3

- DEV-DB apply/readback/rollback 仅在明确 DDL 授权后执行。
- fake callback server 验证工作区 helper 的 HTTP payload、认证头、失败 marker 和重试幂等。
- qlib 可用环境运行 EfficientGATs 小数据 fit/predict，确认 prediction parity 与 release 后 reserved 阈值。

### L4/CI

- `git diff --check`、ruff/py_compile、目标 pytest、F2 feature validator、DESIGN-COMPLIANCE-001。
- 广泛 QE/API/MCP 回归交 Validation Center/CI；当前正在运行实验不作为验证对象。

## Production Gates

```text
source_merge_gate = pending
production_ddl_gate = pending
production_dependency_gate = noop
production_dml_gate = noop
production_restart_gate = pending_after_merge
runtime_activation_gate = pending_after_ddl_and_restart
current_running_experiment = untouched
```

## Rollout / Rollback

- Rollout 顺序：source merge -> 显式执行 additive migration -> 服务重启 -> 先以 `phase_pipeline_enabled=false` 验证 telemetry -> 单节点两 Loop opt-in canary -> 扩大使用。
- 回滚首先关闭 task-level `phase_pipeline_enabled`，立即恢复 whole-loop 串行；telemetry 表和历史记录保留。
- 若代码回滚，新表为 additive，不影响旧 QE 运行；禁止删除历史 resource rows 作为普通回滚步骤。
- 不修改已提交或正在运行 Loop 的 command/env；新能力只影响部署后新提交的 Loop。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `aistock_models/aistock_models/efficient_gats.py` | `test_qe_config_truth.py` cleanup/release proof cases | verified | none |
| F-002 | `qe_evolution_service.py` node total-loop semaphore + GPU-phase semaphore | `test_qe_config_truth.py` phase-aware semaphore cases | verified | none |
| F-003 | `qe_resource_phase_service.py` strict release event + scheduler waiter | release pass/reject/terminal cases | verified | none |
| F-004 | `_submit_custom_evo_loop_unified` and manual retry resource-session path | custom-evo mutation routes and scheduler regressions | verified | none |
| F-005 | custom-evo request/task snapshot flags and config composer | default-off and opt-in composer/scheduler cases | verified | none |
| F-006 | `scripts/qe_runtime_resource.py`, `qrun_limit.py`, `qrun_limit_minute.py` | `test_qe_runtime_resource.py` aggregate/terminal cases | verified | none |
| F-007 | model/helper structured resident and reason-code events | cleanup, fallback, release rejection and upload-failure cases | verified | none |
| F-008 | additive migration, init schema, archive binding and resource service | archive schema/repository + service idempotency cases | verified | none |
| F-009 | phase webhook + per-run secret file + SHA-256 token service | auth/identity/sequence/conflict API and service cases | verified | none |
| F-010 | QE Archive router, MCP module/server and manifest | bounded filter/query and manifest cases | verified | none |
| F-011 | scheduler fail-closed waiter and helper failure markers | rejected/unsupported/interrupted/upload-failure cases | verified | none |
| F-012 | migration/rollback files and task-level opt-in gates | feature validator, this matrix and delivery gate report | verified | none |
| F-013 | `qe_gpu_training_policy.py` resolver and Loop config snapshot | GAT/LSTM/TCN/explicit/invalid policy cases | verified | none |
| F-014 | `ModelAwareGPUPhaseGate` and scheduler policy-specific lease acquisition | shared/shared, exclusive/shared, writer-priority and idempotent release cases | verified | none |
| F-015 | resource session `gpu_training_policy`, migration constraint and scheduler fail-fast path | schema/service/scheduler integration tests | verified | none |
| F-016 | `qe_resource_phase_service.py` atomic reservation/reconciliation、process-shared gate、runner outbox replay、startup scanners | atomic conflict、cross-scheduler gate、upload recovery、terminal reconciliation tests | verified | none |

## Local Verification Results

- `ruff check`：全部变更 Python 文件通过。
- `py_compile`：全部变更运行时代码通过。
- 资源服务、模型策略、共享/独占 gate、runner、API、执行器、配置、归档、调度、QE MCP 与 manifest 最终合并矩阵：`292 passed, 25 skipped`。
- 后端重启安全专项：节点级显式事务原子预留、`release_rejected` 持续占位、跨 scheduler 共享 gate、runner outbox 恢复重放、terminal 对账、shutdown 不调用远端 `kill_loop` 均有直接回归证据。
- 密钥边界专项：实际上传文件保留 token，但命令、任务快照和 `ExecutionResult` 返回值不含 raw token；专项回归通过。
- `git diff --check`、F2 feature validator 与 rebase 到最新 `origin/main` 后的最小回归均通过。
- DEV/生产数据库 migration apply/readback/rollback 未执行；依据本设计必须等待明确 DDL 授权。

## DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：Phase 3、Phase 5 与模型感知增量的全部 DAI 项均有实现和证据。
- [x] `no_silent_error`：fallback、release rejected、upload failure、schema not ready 均有 reason code。
- [x] `no_business_semantic_drift`：默认关闭，模型输出/策略/回测口径不变。
- [x] `no_unrequested_gate_or_approval`：只增加自动资源安全校验，不增加人工审批。
- [x] all/selected/retry/rerun 没有阶段调度旁路。
- [x] backend restart 不调用远端 `kill_loop`；持久化租约、outbox replay 与启动 terminal reconciliation 覆盖进程内状态丢失。
- [x] migration、merge、生产 DDL、重启、runtime activation 分离汇报。
