# QE 公共实验消费者身份与资源隔离 F1 详细设计

> Feature tier：`F1`。本功能只扩展 QE 公共控制面与既有执行容量合同，不修改 Advisory、Selection、Paper、QMT 或策略业务代码。

## 1. 背景与目标

QE 已统一登记 single、custom/strategy/auto evolution 与 Multi-Alpha 运行，但现有登记只区分 `source_type`（UI/MCP/scheduler/agent）和 `purpose`（research/validation）。Advisory 通过 QE 公共入口训练模型时，无法与 QE 主线研究任务形成稳定的业务消费者身份，也无法在共享节点上应用更保守的资源预算。

本功能增加独立的 `consumer_id`，使 Advisory 能复用 QE 的公开 API/MCP、统一登记、数据集绑定、配置生成、执行预留和历史读取，同时不改变 QE 主线默认行为。

### 1.1 与既有设计的权威关系

本设计是 `qe_unified_experiment_registry_selective_warehouse_f2_design_20260907.md` 的窄范围后续扩展。旧设计 2.3 中“不接入 Advisory”仍约束其原始三批交付，不再约束本次经用户明确要求新增的 QE 公共消费者适配；旧设计的统一登记、选择性入仓、数据集绑定和运行可见性合同均保持不变。本设计不授权修改 Advisory 业务代码。

## 2. 范围

- 支持两个消费者：`qe_mainline`、`advisory`。
- 未传 `consumer_id` 的现有 UI、MCP、scheduler、agent 和历史配置统一解析为 `qe_mainline`。
- `consumer_id` 写入 `_qe_run_registration`，随 experiment/task/loop 和 Multi-Alpha parent/child 继承，并可在现有历史 API/MCP 中筛选。
- `consumer_id` 是控制面元数据：必须持久化和回读，但不得进入模型、策略构造器或最终 Qlib YAML。
- Advisory 必须通过 QE 公开 API/MCP 显式提交 `consumer_id=advisory`；不得调用 QE 内部数据库或直接调用 `ConfigComposer`。
- 复用现有 `QEWorkspaceSubmissionCoordinator`。`qe_mainline` 保持节点原容量；`advisory` 的有效容量上限为 1，因此仅在节点无其他活动 reservation 时入场，且同一节点最多一个 Advisory 执行。已运行任务不抢占。
- WSL 既有硬容量 1 不变；远端既有硬容量 4 不变；本功能不提高并发。

## 3. 非目标

- 不新增数据库表、列、事件流、队列或调度 daemon。
- 不修改 Advisory 代码，不替 Advisory 创建私有 QE 入口。
- 不改变模型、因子、标签、seed、数据切分、股票池、Top-K、成本、分钟 TWAP 或退出逻辑。
- 不承诺抢占运行中的 Advisory 任务，也不把业务优先级实现成新的审批门禁。
- 不修改、恢复或重跑失败任务 `qe_20260907_204335_1fb0`；后续只能创建全新 identity 的 exact-retry。
- 不执行实验、Archive 写入、DDL/DML、依赖安装、数据集写入或任何进程控制。

## 4. 核心合同

### 4.1 身份

`consumer_id` 与 `source_type`、`purpose` 正交：

- `consumer_id` 回答“哪个业务消费者使用 QE”；
- `source_type` 回答“通过什么客户端入口创建”；
- `purpose` 回答“research 还是 validation”。

未知 consumer 必须以稳定 `qe_run_consumer_id_invalid` loud fail；不得静默映射为主线。旧记录缺字段时只读投影为 `qe_mainline`。

### 4.2 资源隔离

容量预留仍以 `infra.qe_execution_reservation` 为唯一活动槽权威，不增加第二状态源：

- `qe_mainline`：`effective_capacity = existing_node_capacity`；
- `advisory`：`effective_capacity = min(existing_node_capacity, 1)`；
- duplicate replay 保持既有幂等语义，不因消费者预算被误判为新任务；
- waiting 结果显式回读 `consumer_id`、节点物理容量和消费者有效容量，不伪造 submitted。

该合同意味着 Advisory 只在节点当前无活动 QE reservation 时启动。启动后，远端主线仍可使用剩余物理槽；WSL 单槽则自然串行。它不停止、杀死或抢占任何运行中任务。

### 4.3 入口与历史

- 单实验 UI/API 与 MCP pending-create 接受可选 `consumer_id`。
- custom_evo MCP/API 接受可选 `consumer_id`，并在计划 task/loop 中固化。
- Multi-Alpha 使用同一 `GenerateConfigRequest.consumer_id` 并继承到 parent registration 和 node submission。
- 其他未显式扩展的现有调用仍由默认值进入 `qe_mainline`，语义不变。
- `GET /quantevolver/experiments` 与 `qe_experiment_list` 增加 `consumer_id` 筛选；摘要直接显示该字段，不要求人工输入内部 ID 或 JSON。

## 5. Design Acceptance Index

| item | requirement |
| --- | --- |
| F-001 | 注册合同区分 `consumer_id`、`source_type`、`purpose`；仅允许 `qe_mainline`/`advisory`，旧调用默认主线，未知值 loud fail。 |
| F-002 | single、custom_evo 和 Multi-Alpha 的公开创建路径能持久化 consumer，并由 task/loop/parent/child 继承。 |
| F-003 | consumer 元数据可在历史摘要/API/MCP 筛选，且不进入 Qlib strategy kwargs 或 YAML。 |
| F-004 | Advisory 使用既有 reservation 权威且每节点有效容量为 1；主线容量不变；duplicate replay、waiting 和 fail-closed 语义不变。 |
| F-005 | 无 consumer 的 legacy/unregistered 与当前主线配置在模型、因子、标签、数据切分、股票池、seed、策略和执行参数上前后等价。 |
| F-006 | 不修改 Advisory 或其他非 QE 模块，不新增 DDL、后台进程、审批门禁或实验副作用。 |

## 6. 实施方案

1. 在 `qe_run_registry.py` 增加 consumer 常量、规范化函数和注册字段；计划 loop 继承父 consumer。
2. 在 QE 请求模型和 MCP 的 single/custom_evo 创建工具中提供可选 consumer；服务端验证并固化，客户端不能提交 provider path、workspace path 等 server-owned 字段。
3. 在 `ExecutionContext`/`QEWorkspaceSubmissionSource` 传递控制面 consumer；Coordinator 计算消费者有效容量后调用原 reservation repository。
4. Multi-Alpha parent registration 与 node submission从同一 registration 读取 consumer。
5. 在现有历史 SQL 和摘要投影中增加 consumer 筛选/显示，不新增历史服务。

## 7. 验证方案

- 注册单元测试：默认主线、显式 Advisory、非法值、计划 loop 继承。
- 历史测试：摘要与参数化 consumer 筛选；旧记录投影为主线。
- 容量测试：Advisory 容量 1、主线容量不变、节点已有活动任务时 Advisory waiting、duplicate replay 不回退。
- 路径测试：single、custom_evo、Multi-Alpha 的 consumer 传播；legacy 未注册路径保持等价。
- 配置隔离测试：`consumer_id` 不进入策略 kwargs/YAML。
- 静态门：changed Python ruff/py_compile、`git diff --check`、ownership/scope、F1 validator 和相关 QE 小矩阵。

## 8. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `qe_run_registry.py` consumer contract | `backend/tests/quantevolver/test_qe_registered_submission.py` | PASS | none |
| F-002 | QE API/MCP/evolution/Multi-Alpha adapters | `backend/tests/mcp/test_domain_modules.py`; `backend/tests/unified_engine/test_qe_runtime_first_pending_routes.py`; `backend/tests/unified_engine/test_custom_evo_mutation_routes.py` | PASS | none |
| F-003 | history filter/summary, QE history UI and ConfigComposer boundary | `backend/tests/quantevolver/test_qe_experiment_history_contract.py`; `backend/tests/quantevolver/test_qe_registered_submission.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts` | PASS | none |
| F-004 | `qe_active_execution_capacity.py` | `backend/tests/multi_alpha/test_qe_submission_coordinator.py` | PASS | none |
| F-005 | defaulted request/registration and existing path regressions | `backend/tests/quantevolver/test_qe_registered_submission.py`; `backend/tests/unified_engine/test_qe_runtime_first_pending_routes.py` | PASS | none |
| F-006 | changed-file and production-gate audit | `python -m nox -s validation_module_registry_l0`; `python -m nox -s guardrail_changed_files` | PASS | none |

## Risks / Failure Modes / 风险

- 若调用方遗漏 `consumer_id`，任务按兼容合同归入 `qe_mainline`；Advisory 必须在其公开 API/MCP 适配层显式传入 `advisory`，历史筛选可审计该事实。
- 若持久化记录包含未知 consumer，读取与调度均以 `qe_run_consumer_id_invalid` 失败，不静默归类。
- Advisory 可能在主线持续占用节点时长期等待；这是保护主线吞吐的显式结果，不引入抢占或隐藏队列。
- 旧记录只在读取投影中默认显示 `qe_mainline`，不反向改写历史 JSONB。

## 9. 发布、回滚与生产边界

- Source merge、用户后端重启、post-restart 验证和实验 exact-retry 分开报告。
- 回滚只 revert 本 Feature 的 source merge；不删除登记记录、不恢复未登记执行、不改数据集。
- `production_ddl_gate=noop`；`production_dml_gate=noop`；`dependency_install=noop`；`dataset_write_count=0`。
- `backend_restart_owner=user`；本任务不启动、停止或重启 Backend、WSL API、远端 API 或实验。

## Production Gates / 生产门禁

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `dependency_install=noop`
- `dataset_write_count=0`
- Source merge、用户后端重启、post-restart 验证与 Advisory exact-retry 分别报告，不合并成一个完成状态。

## 10. DESIGN-COMPLIANCE-001 预审

| 检查项 | 设计结论 | 直接依据 |
| --- | --- | --- |
| 禁止简化/子集/POC/mock-only | 待实现后逐项验收 | 身份、入口、容量、历史、配置隔离均列入 F-001～F-006。 |
| 禁止静默错误/伪成功 | 设计满足 | 未知 consumer loud fail；容量不足返回 waiting；不伪造 submitted。 |
| 禁止未经确认改变业务逻辑 | 设计满足 | 默认主线语义及研究/策略参数完全不变；Advisory 仅显式 opt-in。 |
| 禁止私增门禁/审批 | 设计满足 | consumer 是路由与资源合同，不是收益准入或人工审批。 |
