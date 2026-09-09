# AIstock Advisory / QE matched-policy canary F2 详细设计 v1.0

- 日期：2026-09-09
- Feature tier：F2
- 业务归属：Selection Center / Advisory
- 当前状态：`SOURCE_IMPLEMENTED_LOCAL_VERIFIED_QE_PROFILE_BLOCKED`
- 上游依赖：QE 公共任务与 loop 配置查询已经可用；实际新任务提交等待 `QE_PROFILE_RUNTIME_READY`
- 关联蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` F-242、F-245、§6.9、§16

## 1. 背景与目标

在不修改 QE 公共源码、不提交训练或回测的前提下，交付 Advisory 自有的 Q-CANARY 消费边界：

1. 读取 QE 公共 API 已返回的 task summary 和 loop config JSON，构造不可执行的 exact-retry preflight receipt。
2. 对原失败任务 `qe_20260907_204335_1fb0` 保持只读；新任务必须使用新 identity。
3. 冻结模型、因子、seed、label、数据切分、Top50/n_drop、风险策略、执行算法和分钟口径；允许新的 registry/task identity 与合同一致的新 profile generation。
4. 在 seed123/314/2718 都形成完成结果后，用相同 PIT、Top5 review policy、成本和父 policy 生成配对 seed-sensitivity 报告。
5. 输出只作 `NAVIGATION_ONLY` candidate evidence，不激活模型、不修改 StrategyPackage、Selection、Paper 或交易输入。

## 2. Scope / 范围与非目标

允许修改：

```text
backend/services/advisory_model_first/qe_advisory_matched_canary.py
backend/tests/advisory_model_first/test_qe_advisory_matched_canary.py
scripts/advisory_qe_matched_canary.py
docs/architecture/advisory_qe_matched_canary_f2_detailed_design_20260909.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

禁止修改 `backend/services/quantevolver/**`、QE MCP/router/registry/profile/dataset/composer。实现不得访问数据库、网络、WSL、worker，不得创建、恢复或停止 QE 任务。

## 3. Architecture / 架构

实现保持单向依赖：QE 公共 API 响应文件 → Advisory 纯函数投影与校验 → 不可执行 preflight receipt；真实三 seed 结果文件 → Advisory matched-policy 纯函数 → navigation-only report。核心模块不导入 QE 内部包，不访问 API、数据库、文件系统或进程；薄 CLI 只读取用户指定的本地 JSON 并输出单个 JSON。preflight 的 ready 只代表“允许 QE 窗口创建新任务”，不代表任务已经创建、分发或完成。

## 4. Contracts / 接口契约

### 4.1 QE public-response adapter

- 输入只接受公开 task summary 和 loop config JSON；支持 API envelope 或其 `data` 内容。
- 原任务必须为 `failed`，两条 loop 的 `experiment_id` 必须为空，seed roster 必须为 `314/2718`。
- registration、task、loop index 必须一致；unknown/missing/duplicate loop 一律 typed fail closed。
- shared contract hash 排除 seed、旧 task/run identity、profile generation 和 seed 对应 anchor index；其余实验语义必须一致。
- `QE_PROFILE_RUNTIME_READY` 之前只返回 `BLOCKED_QE_PROFILE_NOT_READY`；ready 后也只返回 `READY_FOR_NEW_QE_TASK_CREATION`，不执行提交。
- 不能只证明两条 loop 彼此相同；每条 loop 还必须逐项匹配原失败实验的冻结模型、三因子、seed、label、数据切分、Top50/n_drop、risk policy、分钟TWAP、rolling/vintage、release/cutoff/universe合同，防止两条 loop 同时漂移后伪装成exact retry。

### 4.2 matched-policy observation/report

- 每个 seed 必须绑定完成的 QE experiment、相同 release/cutoff/universe、相同 Advisory policy/cost/PIT identity。
- candidate 与 parent 必须在同一个 Top5 policy 下评价，`target_count=5`，无资金权重或静默补位。
- 固定 seed roster 为 `123/314/2718`；缺 seed、重复 seed、identity drift、非完成实验、sealed holdout 读取均拒绝。
- 报告同时保留 QE Top50 结果与 Advisory Top5 相对父 policy 的净增量，禁止用 QE CAGR 代替荐股结论。
- 汇总 mean/std/min/max、正增量 seed 数、candidate/parent coverage/turnover/MDD、QE Top50风险和实际干预日支持；不内置事后激活阈值。

## 5. 错误语义

| reason_code | 含义 |
|---|---|
| `ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID` | QE public response 缺字段或结构非法 |
| `ADVISORY_QE_CANARY_SOURCE_TASK_INVALID` | 原任务状态、loop roster 或 experiment identity 不符合 exact-retry 前提 |
| `ADVISORY_QE_CANARY_CONTRACT_DRIFT` | 两条 loop 或三 seed 的冻结语义漂移 |
| `ADVISORY_QE_CANARY_PROFILE_NOT_READY` | 尚未收到精确 marker `QE_PROFILE_RUNTIME_READY` |
| `ADVISORY_QE_CANARY_OBSERVATION_INVALID` | 结果未完成、缺 seed、重复 seed或指标非法 |
| `ADVISORY_QE_CANARY_POLICY_MISMATCH` | candidate/parent 或跨 seed policy/cost/PIT 不一致 |
| `ADVISORY_QE_CANARY_SEALED_ACCESS_FORBIDDEN` | 输入声称读取 sealed holdout |

## 6. Implementation Plan / 实施方案

1. 在 Advisory 自有模块定义冻结 source snapshot、preflight receipt、matched observation/report 和稳定 reason code。
2. 通过薄 CLI 消费本地保存的 QE public response，不实现 HTTP client、QE task creator 或数据库读取。
3. 用定向测试覆盖单边/双边合同漂移、旧任务身份、profile gate、三 seed 角色隔离、干预支持和 sealed holdout 禁止。
4. 更新蓝图 F-242/F-245 的源码能力状态，但保持真实 seed 输出、经济验证、merge、runtime 与 experiment submission 为独立未完成状态。

## 7. Verification Plan / 验证方案

- API envelope/direct payload 正常投影；缺 registration、task/loop mismatch、额外/重复 seed 拒绝。
- 原失败记录只读、`experiment_id=null`、new task required；profile blocked/ready 两态均不提交。
- 仅 seed 与合法 control identity 不同的两条 loop 具有相同 shared contract hash；模型、因子、窗口、TopK、风险或执行漂移均拒绝。
- 三 seed matched report 的配对增量与统计量正确；QE Top50 与 Advisory Top5 指标保持分栏。
- policy/cost/PIT/release/universe/sealed drift fail closed。
- 源码扫描证明无 `quantevolver` import、HTTP/DB/subprocess/process-control 或任务提交调用。
- 运行定向 pytest、compile、`git diff --check`、F2 validator 和 `DESIGN-COMPLIANCE-001` 四项复核。

## 8. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-242 | QE public-response consumer、candidate provenance、universe/budget/execution parity、profile typed block、原失败任务只读 |
| F-245 | seed123/314/2718 的 QE/Advisory matched-policy 报告、seed sensitivity、角色口径隔离、navigation-only |

## 9. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-242 | `backend/services/advisory_model_first/qe_advisory_matched_canary.py`；`scripts/advisory_qe_matched_canary.py` | `python -m pytest backend/tests/advisory_model_first/test_qe_advisory_matched_canary.py -q`；24 passed | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: QE profile尚未ready，本轮不提交实验，原失败任务未修改 |
| F-245 | `backend/services/advisory_model_first/qe_advisory_matched_canary.py`；`scripts/advisory_qe_matched_canary.py` | `backend/tests/advisory_model_first/test_qe_advisory_matched_canary.py`覆盖三seed统计、角色隔离、policy/release/sealed/support drift | REPORT_CONTRACT_IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: 真实seed314/2718输出尚不存在，只交付消费与报告能力，不构成经济证据 |

## 10. Rollout / rollback / 发布回滚

- 当前只形成独立 Advisory 源码候选；未获单独授权不得合入。
- source merge 后不自动重启、不激活模型、不提交实验；若后端运行时需要消费新接口，仍由用户执行重启并单独验证源码/运行时一致性。
- 回滚仅 revert 本 Feature 的 Advisory 文件与蓝图状态，不触碰原 QE 失败记录、后续 QE task、registry、dataset profile 或数据库。
- QE ready 后的 exact retry 是新的外部阶段：必须先登记新 identity，再由 QE 公共入口分发；不由本 CLI 执行。

## 11. Risks / Failure Modes / 风险

- 两条 loop 同时漂移：逐条匹配冻结源合同，不能只比较 loop 间 hash。
- QE Top50收益被误当成 Advisory Top5收益：报告强制分栏并只计算 Top5 相对父 policy 的配对增量。
- 稀疏或零干预形成伪结论：逐 seed 记录干预日数量和覆盖率，不把偶然变化隐藏在平均收益里。
- sealed holdout 污染：任何 observation 声明访问 sealed 均 typed fail closed。
- ready marker 被误解为 submitted：receipt 永久带 `qe_task_submission_performed=false`，且核心模块无网络/QE runtime依赖。
- 治理膨胀：不增加平台、表、后台进程、审批流或 Archive 写入，只交付纯合同与薄 CLI。

## 12. Production Gates / 生产门禁

```text
backend_restart = noop
production_ddl_gate = noop
production_dml_gate = noop
dependency_install = noop
runtime_activation = noop
qe_task_submission = blocked_until_QE_PROFILE_RUNTIME_READY
```

## 13. DESIGN-COMPLIANCE-001

1. 不把 preflight/report 源码完成称为 canary 经济验证完成；真实 seed 输出缺口保持显式。
2. 缺字段、identity drift、profile block、sealed access 与 policy mismatch 全部 typed fail closed。
3. 不改变 QE、Selection、StrategyPackage 或运行时逻辑；只消费公开合同并生成 Advisory 报告。
4. 不新增人工审批或结果门槛；唯一运行阻断来自已声明的 `QE_PROFILE_RUNTIME_READY` 外部状态。
