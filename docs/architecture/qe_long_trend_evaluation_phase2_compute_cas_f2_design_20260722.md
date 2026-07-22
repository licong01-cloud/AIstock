# QE 长期趋势评价 F-014 Phase 2：计算节点、专属 CAS 与恢复编排 F2 设计

- 文档类型：F2 跨仓库从属实现设计
- 日期：2026-07-22
- 状态：`IMPLEMENTED_LOCAL_VERIFIED_NOT_ACTIVATED`
- 父级权威：`docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md` v1.6
- 上位研究蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v5.7
- 代码范围：AIstock QuantEvolver / QE Workspace Client / QE Resource Phase / RD-Agent QE Workspace
- 唯一硬边界：QE-only；不得读取、修改或触发 Selection、Advisory、Paper、模拟盘、荐股、QMT、StrategyPackage 或其他非 QE 链路

本设计不增加研究准入、淘汰、审批、确认或方向停止逻辑。输入、制品或数据缺失时，系统保存真实缺口、已完成指标族、影响范围和补取方案；缺失项不能取消其他科研结果，也不能阻止其他 QE 实验并行执行。

## 0. Decision Summary / 决策摘要

Phase 1 已经交付纯契约、严格 QE 数据读取和长期评价计算核。Phase 2 不重写这些公式，也不提前实现 Phase 3–5 的指标明细表、公共 API、MCP 和 UI，而是完整交付以下六个计算平台能力：

1. 将冻结的 evaluator 源码、profile、数据身份和 Recorder 输入身份绑定到 normal Loop 与 `long_trend_only` 两种入口；
2. 在 QE 节点解析真实 Qlib Recorder 与显式数据快照，不在 Windows FastAPI 中加载 H5；
3. 由节点外部进程执行同一 Phase 1 evaluator，并持久化 job/attempt/terminal receipt；
4. 在派发前把评价身份和任务状态写入 Phase 2 权威 `qe_archive.run_evaluation` 控制记录，使 AIstock 重启后可以枚举并恢复，而不是依赖进程内状态或扫描工作目录；
5. 将逐信号、逐 episode 和紧凑回执原子发布到独立的 `QELongTrendArtifactStore`，不重复保存可由这两类明细和指标 JSON 推导的副本；
6. 为每次评价创建独立 CPU postprocess resource session，复用现有认证 sequence/outbox 语义，支持 AIstock/RD-Agent 重启后的 inspect/reconcile/collect，不重复计算或覆盖成功制品。

Phase 2 的完成不会宣称 F-014 平台整体完成。Phase 2 只提前交付三张评价表中的 `qe_archive.run_evaluation` 控制表；`run_evaluation_metric/run_evaluation_artifact`、公共 API、MCP、UI、批量历史回填和完整 E2E 仍分别属于父设计 Phase 3–5。Phase 2 产出的真实 receipt 与 CAS 制品可以立即用于科研分析和后续联调。

### 0.1 Implementation-time design corrections / 实现期设计修订

实现审计确认并修正了四个原设计中必须明确的时序/平台事实：

1. normal adapter 运行时 `qe_archive.run` 尚未必然创建，因此 `run_evaluation` 先以权威 `parent_task_id + parent_loop_index` 建立 durable parent，`run_id` 允许为空；Archive 完成后只允许通过 task/Loop/source/run-type 核对做一次性 CAS 绑定。不得创建占位 Archive run 或影子父表。
2. normal 与 historical 的 evaluator identity/worker context 统一使用稳定的 `qe_task_loop:<task_id>:LoopN` 父身份；真实 Archive `run_id` 只作为控制表 FK。这样 Archive 创建时点不会让同输入产生第二个 evaluation。
3. `catalog_digest` 只覆盖冻结 Recorder 投影和权威 artifact manifest hash；workspace 中后续新增的 qelt 目录、日志、mtime 或无关文件不得改变 evaluation identity。
4. control-row 创建前 AIstock 不可达时，adapter 同时保存 Loop 内 typed pending receipt 和专属无密钥 pending index。RD startup replayer 只枚举该专属 index，校验 task/Loop/descriptor/adapter/pending hashes 后固定重放 adapter；不扫描 Archive 或全部 workspace，不执行请求提供的任意命令。

以上是实现正确性修订，不是科研准入、审批或方向淘汰门禁。

## 1. Background / 背景

F-014 Phase 1 已证明长期趋势指标、删失、episode、portfolio 和 entry/exit evidence bridge 可以在纯计算层保持一致语义；当前真正缺少的是把这些能力接入真实 QE Recorder、不可变数据快照、节点外部进程和可复算制品。Phase 2 因此只解决“真实输入如何被准确解析、计算如何在节点运行、制品如何原子保存、重启后如何继续”四个平台问题，不改变任何科研公式。

## 2. Current Code Truth / 当前代码事实

### 2.1 已完成的 Phase 1

| 现有文件 | 当前职责 | Phase 2 复用方式 |
|---|---|---|
| `backend/services/quantevolver/long_trend_evaluation_contract.py` | profile、reason code、family status、snapshot identity、evaluation identity | 原样作为唯一语义权威；不复制公式或重定义状态 |
| `backend/services/quantevolver/long_trend_data_reader.py` | 只允许 `daily_pv.h5/sector_data.h5/meta.json`，验证内容 hash、快照与重叠 parity | 只在 QE 节点调用；Windows 后端不得调用 |
| `backend/services/quantevolver/long_trend_evaluation.py` | signal、episode、portfolio、entry/exit evidence、sector、统计推断 | worker 将真实已解析对象传入；不增加静默 fallback |
| `backend/tests/unified_engine/test_qe_long_trend_contract_reader.py` | 契约、reader 与快照 oracle | Phase 2 回归基础 |
| `backend/tests/unified_engine/test_qe_long_trend_evaluation_core.py` | 计算与 evidence bridge oracle | Phase 2 不改变现有期望 |

### 2.2 可复用的平台代码

| 能力 | 现有入口 | 设计结论 |
|---|---|---|
| QE 节点提交与身份 | `qe_workspace_client.py::QEWorkspaceClient` | 扩展 typed evaluation job 方法，不另建 HTTP client |
| 节点环境/数据身份 | `get_execution_environment()`、`get_dataset_identity()` | 直接复用并把环境 snapshot/hash 绑定到 evaluation/request/attempt/CAS；不从 AIstock 本机猜测远端身份 |
| 完整文件目录 | `list_workspace_files()`、`stat_workspace_file()` | resolver 只消费 catalog 声明的文件，不猜常见路径 |
| 文件下载 | `download_workspace_file_bytes()` | 小型 JSON/pickle 可复用；大 Parquet 新增 streaming 方法，禁止全量 bytes 常驻内存 |
| Recorder 解析 | `results_only_retry.py::collect_results_only_artifacts()` | 提取 `qe_current_recorder.json` 和精确 mlruns 前缀的规则抽成共享 resolver；不复制第二套猜测逻辑 |
| 通用预测 CAS | `model_store/artifact_store.py::PredictionArtifactStore` | 仅借鉴 blob hash、临时文件和原子 manifest；namespace、root、URI、allowlist 完全独立 |
| 资源阶段 | `qe_resource_phase_service.py::QEResourcePhaseService` | 增加独立 `qelt:<evaluation_id>` source key、`created -> long_trend_eval` 合法 phase 与 CPU postprocess 元数据；不恢复 GPU 监测 |
| 统一节点容量 | `qe_active_execution_capacity.py` | normal Loop 提交透传 postprocess descriptor；不把 CPU 评价计为 GPU Loop，不改变现有模型并发上限 |
| QE Archive | `qe_archive.run`、父设计 `run_evaluation` schema | Phase 2 只读父 run identity并写专属评价控制记录；不写通用 metric/artifact 表，评价指标/制品明细表留在 Phase 3 |
| RD-Agent QE API | `rdagent/app/api_endpoints/qe_evolution_api.py` | 在现有 QE Workspace router 下增加 loop-owned evaluation job；不建设第二个服务 |
| RD 文件目录/数据身份 | `qe_workspace_catalog.py`、`qe_dataset_identity.py` | 扩展 evaluation 子目录 catalog 与显式 snapshot root 解析，保持 allowlist/path containment |

### 2.3 实现前基线缺口（本分支已按第 17 节验收矩阵实现）

1. Phase 1 engine 只接受已经解析好的 DataFrame/对象，没有真实 Recorder resolver；
2. normal Loop submission 没有冻结的长期评价 postprocess descriptor；
3. RD-Agent 没有 `evaluation_id` 级 job、attempt、process identity、receipt 和取消语义；
4. `QEResourcePhaseService` 不认识 `long_trend_eval`；
5. 没有 QE 长期评价专属 CAS、schema manifest、streaming collect 与 conflict 检测；
6. `read_exp_res.py` 没有挂载 compact receipt；
7. 后端重启后没有可枚举的 evaluation 控制记录和 inspect/reconcile/collect 服务；
8. 当前 normal Loop 命令固定为 `qrun -> read_exp_res.py`，没有“评价 worker receipt 生成后再挂载”的确定时序；
9. 现有执行环境 identity 已包含 Python、Qlib、安装包和执行器文件 hash，但 F-014 尚未绑定该 identity。

## 3. Scope / 范围

### 3.1 本阶段完整交付

- normal Loop 的显式 profile opt-in postprocess descriptor；
- 已完成 Loop 的内部 `long_trend_only` 服务入口，供 Phase 3 API 直接复用；
- evaluator deployment bundle、源码 hash 和 bundle allowlist；
- QE Recorder、workspace catalog、dataset identity 与 snapshot parity resolver；
- RD-Agent loop-owned evaluation job API、文件 spool、外部进程、attempt 和 terminal receipt；
- `qe_archive.run_evaluation` 控制表、派发前 queued 记录和 AIstock 重启枚举；
- CPU 单槽原子 claim、独立 resource phase/outbox 事件与恢复；
- 独立 `QELongTrendArtifactStore`、streaming collect、原子 manifest 和内容冲突检测；
- normal Loop registration、worker terminal/compact 与 CAS published compact 三阶段契约，以及 `read_exp_res.py` 的受限 registration JSON 挂载适配器；
- normal/historical 同核一致性、重启恢复、重复提交、部分输入、CAS 中断和内存释放测试；
- AIstock 与 RD-Agent 两仓库的定向测试及 QE-only 隔离审计。

### 3.2 本阶段不实现

- 不创建 `run_evaluation_metric/run_evaluation_artifact` 两张明细表；Phase 2 必须创建父设计已有的 `run_evaluation` 控制表，不另建临时/影子状态表；
- 不新增公共 FastAPI route、MCP tool 或 frontend 页面；
- 不执行 R8–R11 全量历史补算；只允许一个已完成 Loop 的显式 canary；
- 不注册周期 cron 或平行 global scheduler，不扫描全部 Archive；只在现有 QE runtime startup reconciliation 中枚举 `run_evaluation` 非终态行，并由 RD QE API startup dispatcher 发现其本地 queued job；
- 不改变训练、标签、因子、回测、策略、TopK、成本或执行公式；
- 不修改通用 Prediction Store、`qe_archive.run_metric/run_artifact/raw_payload` writer；
- 不模拟订单队列、撤单、涨跌停成交或分钟级执行；
- 不增加 GPU/NVML/`nvidia-smi`/显存轮询。

## 4. Non-Goals / 非目标

1. 不以 Phase 2 平台状态判断任何模型、因子、期限、板块或 Alpha 方向是否继续；
2. 不把 evaluator 变成训练器、回测器、交易模拟器或研究审批器；
3. 不为缺失 order/trade/position 生成伪制品、默认值或推测结果；
4. 不建设与 QE Workspace、QE Archive、resource phase 或 Prediction Store 并行的通用平台；
5. 不在设计阶段执行代码、DDL、依赖安装、服务重启、实验、数据库写入或 CAS 写入。

## 5. Design Acceptance Index / 设计验收索引

| ID | 验收项 |
|---|---|
| F-014 | 本文是父级 F-014 的 Phase 2 从属实现，不重定义 Phase 1 公式或 Phase 3–5 范围。 |
| F-301 | Phase 2 只接收 QE task/Loop/run、QE workspace 和 QE dataset identity；非 QE 来源在计算或 CAS 写入前结构化拒绝。 |
| F-302 | normal 与 `long_trend_only` 使用相同 profile、bundle、resolver、engine、artifact schema 和 terminal receipt。 |
| F-303 | evaluator bundle 按文件 allowlist、单文件 hash、bundle hash 和 source SHA 固化；节点不得执行请求指定的任意路径或命令。 |
| F-304 | Recorder resolver 先读取权威 recorder ref 与完整/部分 catalog；完整 catalog 中不存在的文件不再猜测，部分 catalog 显式记录限制。 |
| F-305 | feature/outcome snapshot 均使用节点返回的 immutable identity；不得把当前默认数据冒充指定 snapshot。 |
| F-306 | Windows FastAPI 不加载 H5、不反序列化百万行 Parquet；计算只在 QE 节点，收集使用 streaming 与 hash 校验。 |
| F-307 | 六个 family 独立解析与计算；缺 pred/position/report/indicator/sector 只影响依赖族并形成 data action plan。 |
| F-308 | RD evaluation job 具有稳定 evaluation/job/attempt/request/process identity，重复提交同 identity 幂等，内容冲突 fail-fast。 |
| F-309 | 节点 CPU postprocess 默认单槽 FIFO；排队不失败，不抢占模型训练，不触发 GPU 监测。 |
| F-310 | `long_trend_eval` phase 使用现有 token、sequence、source binding 和 outbox 语义；非法事件不能推进状态。 |
| F-311 | AIstock 后端重启后通过 inspect/reconcile 恢复已有 job；不重复启动仍存活进程，不重复发布成功 CAS。 |
| F-312 | 专属 CAS 使用独立 root/URI/schema allowlist，blob 临时写入、fsync/hash 后发布，manifest 原子替换。 |
| F-313 | 同 evaluation identity 的成功 manifest 不可覆盖；相同内容返回已有结果，不同内容返回 `QELT_DUPLICATE_IDENTITY_CONFLICT`。 |
| F-314 | receipt 与大明细分离；`read_exp_res.py` 只挂载 registration JSON，不等待 worker/CAS、不内联 Parquet 或复制全量数据。 |
| F-315 | terminal receipt 保存 family/platform 状态、资源、输入/输出 hash、限制和 data action；不把平台失败写成研究失败。 |
| F-316 | typed cancel 只终止指定 evaluation attempt，并保留已成功 artifact；不调用训练 Loop kill，不修改回测结果。 |
| F-317 | normal/historical parity、重复请求、重启、CAS/下载中断、partial catalog、missing artifact 和 memory release 均有直接测试。 |
| F-318 | AIstock/RD-Agent ownership、import、route 和 runtime 回归证明非 QE 模块、通用 Prediction Store 和现有 Loop 语义零变化。 |
| F-319 | Phase 2 在远端派发前持久化 `run_evaluation` 控制记录；重启恢复只枚举该专属表的非终态行，不靠目录扫描或内存对象。 |
| F-320 | 节点 execution-environment snapshot/hash 进入 evaluation identity、request、attempt receipt 与 CAS manifest；依赖能力不满足时形成结构化平台状态，不替换环境或改变科研方向。 |
| F-321 | Recorder resolver 从冻结 backtest frequency 解析 `indicators_normal_{freq}.pkl` 与 `indicators_normal_{freq}_obj.pkl`；后者是 `amount/deal_amount/ffr` 权威来源，二者不得互相替代。 |
| F-322 | normal Loop 严格执行 `qrun -> register/submit -> read_exp_res`；registration/worker/published receipt 分阶段同 identity，原 Loop及时释放 reservation，评价失败不改写训练/回测终态。 |

## 6. End-to-End Architecture / 端到端架构

```text
AIstock explicit QE caller
  ├─ normal Loop: ConfigComposer + frozen postprocess descriptor
  │    qrun -> fixed register/submit adapter -> registration receipt -> read_exp_res
  └─ historical: QELongTrendPhase2Service.prepare_long_trend_only(...)
        │
        ├─ validate qe_archive.run + task/Loop + dataset/workspace identity
        ├─ build frozen evaluator bundle/environment/input/evaluation identity
        ├─ insert/update qe_archive.run_evaluation control row (queued)
        ├─ create dedicated qelt:<evaluation_id> resource session
        ▼
QEWorkspaceClient typed evaluation methods
        ▼
RD-Agent existing /api/v1/qe_workspace router
        ├─ persist request + secret separately
        ├─ enqueue loop-owned evaluation job
        ├─ one CPU postprocess slot claims attempt
        └─ external subprocess runs frozen evaluator bundle
              ├─ exact Recorder/catalog resolver
              ├─ strict feature/outcome dataset resolver
              ├─ Phase 1 QELongTrendEvaluationEngine
              └─ staged core Parquet + worker compact receipt + terminal manifest
        │
        ├─ authenticated monotonic resource phase events
        └─ durable job/status/artifact catalog inspection
        ▼
AIstock streaming collector
        ├─ verify catalog, size, sha256 and schema hash
        ├─ publish to dedicated QELongTrendArtifactStore
        ├─ build CAS published compact receipt
        └─ CAS/update run_evaluation with fencing and immutable hashes
```

Phase 2 不让 RD-Agent 直接写 PostgreSQL。AIstock 新增最小 `QELongTrendEvaluationControlRepository`，只拥有父设计已经定义的 `qe_archive.run_evaluation` 控制表；它在远端 POST 前写 `queued`，并持久化 evaluation/request/environment/resource/node/CAS 生命周期。Phase 3 在同一 `evaluation_id` 上新增 metric/artifact repository，不迁移、不复制控制状态，也不创建影子 schema。

## 7. Frozen Configuration and Identity / 冻结配置与身份

### 7.1 ExperimentConfig 与 ConfigComposer

仅在调用方显式设置时增加：

```json
{
  "long_trend_evaluation": {
    "enabled": true,
    "profile_id": "qe_long_trend_v1",
    "profile_sha256": "<sha256>",
    "evaluator_version": "qelt_core_v1",
    "evaluator_source_sha256": "<sha256>",
    "execution_environment_snapshot_id": "<qeenv id>",
    "execution_environment_manifest_sha256": "<sha256>",
    "feature_snapshot_id": "<immutable id>",
    "feature_manifest_sha256": "<sha256>",
    "outcome_snapshot_id": "<immutable id>",
    "outcome_manifest_sha256": "<sha256>",
    "backtest_freq": "<frozen Recorder frequency>",
    "mode": "normal_postprocess"
  }
}
```

默认不存在该对象，不写 `enabled=false` 伪配置，不插入伪资源阶段。profile 内容从注册表生成，调用方不能覆盖 horizon、barrier、slice、entry 或 terminal 公式。`backtest_freq` 从冻结的 QE request/config 和 Recorder catalog 双向核对，二者不一致时保存冲突证据，不猜测 `1day` 或 `1min`。environment identity 必须来自节点 `get_execution_environment()`，调用方不能自填。

### 7.2 evaluator deployment bundle

AIstock 新增 `long_trend_evaluation_bundle.py`，bundle 保留可导入的 Python package 相对布局，只允许打包：

- `backend/__init__.py`
- `backend/services/__init__.py`
- `backend/services/quantevolver/__init__.py`
- `backend/services/quantevolver/long_trend_evaluation_contract.py`
- `backend/services/quantevolver/long_trend_data_reader.py`
- `backend/services/quantevolver/long_trend_evaluation.py`
- `backend/services/quantevolver/qe_dataset_contract.py`
- `backend/services/quantevolver/long_trend_pickle_parser_entry.py`
- `backend/services/quantevolver/long_trend_worker_entry.py`
- `bundle_manifest.json`

manifest 保存每个文件相对路径、SHA-256、size、bundle schema、Python ABI、evaluator source SHA、`execution_environment_snapshot_id` 与 `execution_environment_manifest_sha256`。环境 manifest 已由现有 QE execution-environment contract 覆盖 Python、Qlib、已安装包和 executor file set；worker 另外检查 NumPy、Pandas、PyTables/HDF、Parquet engine 的实际 import capability，并把版本/能力摘要写入 attempt receipt。能力缺失形成结构化 platform failure 和获取/修复建议，不改用另一 Python、另一节点 checkout 或替代序列化格式，也不据此淘汰研究方向。

RD-Agent 仅把 bundle allowlist 文件写入 `evaluation_dir/runtime/`，拒绝绝对路径、`..`、符号链接、额外文件和 hash 不符。RD-Agent 将 `evaluation_dir/runtime` 作为唯一新增 `PYTHONPATH`，并固定执行 `python -m backend.services.quantevolver.long_trend_worker_entry`；request 不携带 shell command。这样远端执行的是已哈希的 AIstock 权威 Phase 1 源码，不依赖 WSL `/mnt/f/Dev/AIstock` 或远端机上某个可漂移 checkout。

### 7.3 identity

父设计 `evaluation_id` 增加节点 execution-environment manifest hash，使相同研究输入在不同运行环境下不会错误共用同一不可变结果：

```text
evaluation_id = sha256(
  "qe_task_loop:" + task_id + ":Loop" + loop_index
  + profile_sha256 + evaluator_source_sha256
  + execution_environment_manifest_sha256
  + canonical(feature_dataset_manifest_sha256 or "<NULL>")
  + canonical(outcome_dataset_manifest_sha256 or "<NULL>")
  + input_manifest_sha256
)
```

Phase 2 节点身份：

```text
job_id       = qelt_job_<sha256(evaluation_id + node_id)>
request_sha  = sha256(canonical request without secret/token)
attempt_id   = qelt_attempt_<evaluation_id hash>_<attempt_no>
bundle_sha   = sha256(canonical bundle manifest)
execution_identity_sha = sha256(bundle_sha + execution_environment_manifest_sha256)
```

真实 `qe_archive.run_id`、resource token、callback credential、本地绝对路径和 PID 不进入 evaluation identity。真实 run 通过 `run_evaluation.run_id` 一次性绑定；process identity 只进入 attempt receipt，用于 kill/reconcile fencing。request、job、attempt、worker terminal receipt、CAS manifest 与 `run_evaluation` 必须保存同一 environment snapshot/hash；发现不同环境时创建不同 `evaluation_id`，不得覆盖或伪装成同 identity retry。

## 8. Real Artifact and Dataset Resolver / 真实制品与数据解析

### 8.1 Recorder authority

resolver 必须先读取 `qe_current_recorder.json`，得到 exact experiment/recorder ID，再拼接：

```text
mlruns/{experiment_id}/{recorder_id}/artifacts/
```

允许的 Recorder 输入：

- `pred.pkl`
- `label.pkl` 或 catalog 明确存在的 `sig_analysis/label.pkl`
- `params.pkl`/`params_pkl`
- `portfolio_analysis/report_normal_1day.pkl`
- `portfolio_analysis/positions_normal_1day.pkl`
- `portfolio_analysis/indicators_normal_{backtest_freq}.pkl`：仅作为 portfolio/indicator 汇总输入；
- `portfolio_analysis/indicators_normal_{backtest_freq}_obj.pkl`：`amount/deal_amount/ffr` 与订单对象的权威输入；
- catalog 明确标注且 schema 可识别的 order/trade evidence

`backtest_freq` 必须来自冻结配置并与 catalog 实际路径一致。`indicators_normal_{freq}.pkl` 与 `_obj.pkl` schema 分别验证，汇总文件不得代替 `_obj.pkl` 生成 attempt/deal/ffr，`_obj.pkl` 也不得冒充组合 report。不得猜测未出现在 complete catalog 的 order/trade 文件名。complete catalog 中缺失映射为 family-local missing；partial catalog 保存 `catalog_completeness=partial`、warnings 和尝试过的权威路径，不把“未列出”冒充“确定不存在”。

### 8.2 输入反序列化

- pickle 只接受 `qe_current_recorder.json` 指向的 exact experiment/recorder 前缀，且该 recorder、workspace、task/Loop、`qe_archive.run` 必须属于同一 QE source identity；request 不接受调用方提供任意 pickle path/URI；
- workspace containment、catalog type、size、SHA-256 和 allowlist 全部通过后，RD supervisor 才启动固定 `long_trend_pickle_parser_entry` 子进程；
- supervisor 独占 resource token/callback credential。parser/evaluator 子进程使用最小环境变量、独立工作目录和输入只读/输出 staging allowlist，不获得数据库连接、callback secret、Prediction Store credential 或任意 shell command；
- prediction/label/position/report/indicator 分别执行 schema validator；
- 单项解析失败形成该族 reason 和冲突摘要，其他族继续；
- raw pickle 不复制进长期评价 CAS，只记录受信来源身份、原始相对路径、size、sha256、catalog completeness、parser process identity 与 parser receipt；
- order/trade/position 数量或日期冲突时不得回退到日线 high/low 猜成交。

上述信任边界只覆盖 AIstock 创建并由同一 QE Loop Recorder 归档的制品，不扩展为外部 pickle 上传能力。parser 异常、超时或非零退出必须形成 `QELT_PICKLE_PARSER_FAILED`，保留其他已经规范化的指标族结果；不得在 supervisor 进程内重试 `pickle.load` 作为隐藏 fallback。

### 8.3 数据快照

feature 与 outcome 均通过节点 `dataset-identity` 读取。请求只允许节点配置 root 内的显式 `data_root_uri`，返回 identity 必须与请求 snapshot/manifest hash 相同。worker 使用 `long_trend_data_reader.py` 再次验证：

- `meta.json/daily_pv.h5/sector_data.h5` allowlist；
- 文件内容 hash 与 manifest；
- feature 全重叠区间 qfq OHLC parity；
- outcome 只能为 same snapshot 或 verified extension；
- evaluation as-of 不超出 outcome end date。

identity 不完整时保存局部状态和补数方案，不自动改用 `/home/lc999/data/factor_data`、当前默认数据或生产 Selection PIT。

## 9. RD-Agent Evaluation Job Contract / 节点评价任务契约

### 9.1 路由

在现有 `/api/v1/qe_workspace` router 下增加：

```text
POST /tasks/{task_id}/loops/{loop_id}/long-trend-evaluations
GET  /tasks/{task_id}/loops/{loop_id}/long-trend-evaluations/{evaluation_id}
GET  /tasks/{task_id}/loops/{loop_id}/long-trend-evaluations/{evaluation_id}/artifacts
GET  /tasks/{task_id}/loops/{loop_id}/long-trend-evaluations/{evaluation_id}/artifacts/{path}
POST /tasks/{task_id}/loops/{loop_id}/long-trend-evaluations/{evaluation_id}/cancel-intents
```

它们属于 loop 子资源，不是新的实验服务。status/artifact GET 只读，artifact path 继续执行 workspace containment。

### 9.2 节点持久目录

```text
<loop_dir>/long_trend_evaluations/<evaluation_id>/
  request.json
  job.json
  attempts/<attempt_no>/
    claim.json
    process_identity.json
    secret.json            # 0600，不进入 catalog/receipt
    stdout.log
    status.json
    staging/
    artifacts/
    terminal_receipt.json
```

`request.json/job.json/claim.json/status.json/terminal_receipt.json` 使用临时文件 + fsync + replace；`claim.json` 使用 `O_CREAT|O_EXCL` 建立。成功 attempt immutable；失败 retry 新增 attempt，不删除旧日志和 receipt。

### 9.3 CPU 单槽与队列

- 每节点一个 `long_trend_eval` OS slot lock，由 supervisor 从 claim 前持有到 terminal receipt fsync 完成，不能只保护队列扫描；
- POST 原子写 queued job 后返回，不因槽位占用返回研究失败；
- dispatcher 在 OS lock 内按 `created_at/evaluation_id` 排序，使用独占 `claim.json` 将唯一 queued job 原子推进为 starting；同 attempt 已有 claim 时只 inspect，不创建第二进程；
- 显式 submission、inspect、AIstock reconcile 和 RD QE API startup spool recovery 都可唤醒 node dispatcher；startup 只发现既有 queued/running job，不创建新 evaluation，不注册周期 cron；
- 外部 subprocess 与 RD API 生命周期解耦，API 重启不杀 worker；
- stale running 必须核对 PID、start time、command hash、evaluation/request/environment identity 与 claim owner，再决定仍运行、已终止或可重试；PID 不匹配不能发送信号；
- worker supervisor 与 evaluator 分离：supervisor 持有 slot/resource secret并负责 outbox，evaluator/parser 不获得 secret；
- 不读取 GPU/NVML，不调用 `nvidia-smi`，不轮询桌面资源。

### 9.4 typed cancel

cancel intent 必须携带 expected `attempt_id/process_identity/request_sha`。匹配后只终止 evaluator subprocess；若 terminal receipt 已存在返回 already-terminal。已发布成功 CAS 不删除，正在 staging 的临时文件保留为失败证据或由同 attempt 清理，不触碰训练/回测进程。

## 10. AIstock Phase 2 Service / AIstock 编排服务

新增 `QELongTrendPhase2Service`，提供内部 typed 方法：

```text
prepare_normal_postprocess(...)
prepare_long_trend_only(...)
submit(...)
inspect(...)
reconcile(...)
collect_and_publish(...)
cancel_attempt(...)
```

服务职责：

1. 从 `qe_archive.run` 只读验证 `source_system/run_type/task_id/loop_index`；
2. 获取 QE node environment、dataset 和 workspace identity；
3. 构建 profile/bundle/input manifest/evaluation identity；
4. 在同一事务创建或幂等读取 `run_evaluation(status=queued)`，再创建独立 resource session；DB 事务提交前不得远端 POST；
5. 使用 control row fencing 将 queued 推进为 submitting，提交 node job并保存 job/request/attempt identity；
6. normal 模式在 RD 接受 job 后立即返回 registration receipt，`read_exp_res.py` 只挂载该生命周期指针；historical 模式同样返回 queued，由相同 reconcile 继续；
7. inspect terminal status 后 streaming collect；
8. 原子发布 CAS、生成 published compact receipt，并以 owner/fencing/row_version CAS 更新 control row。

Phase 2 不增加用户可调用的 public 创建/查询 API，但在现有 `quantevolver_evolution.py` QE webhook 信任边界下增加一个 authenticated internal postprocess-registration endpoint，供 normal Loop adapter 在 qrun 产出真实 Recorder 后调用。Phase 3 的用户 POST/GET API直接调用同一 service，不再实现第二套编排。

### 10.1 `run_evaluation` Phase 2 控制记录

Phase 2 migration 创建父设计已经定义的 `qe_archive.run_evaluation`，本阶段至少持久化：

| 字段组 | 字段 | 语义 |
|---|---|---|
| parent/research identity | `evaluation_id/parent_task_id/parent_loop_index/run_id/evaluation_type/profile_id/profile_sha256/evaluator_version/evaluator_source_sha256` | 稳定 task/Loop evaluator 父身份；`run_id` 在 Archive 行生成后一次性验证绑定，可暂为空 |
| execution identity | `execution_environment_snapshot_id/execution_environment_manifest_sha256/bundle_sha256/input_manifest_sha256` | 可复算运行环境、bundle 与输入身份 |
| dataset identity | `feature_dataset_snapshot_id/feature_dataset_manifest_sha256/outcome_dataset_snapshot_id/outcome_dataset_manifest_sha256` | feature/outcome 快照；缺失使用父设计 typed null |
| remote identity | `node_id/job_id/request_sha/current_attempt_id/resource_session_id` | 节点 job、attempt 与独立资源会话 |
| lifecycle | `status/worker_terminal_sha256/artifact_store_run_key/artifact_manifest_sha256/reason_code/reason_json` | queued 至 terminal/CAS 生命周期；平台状态不改写 family 结论 |
| evidence | `family_status_json/platform_delivery_status_json/data_action_plan_json/stats_json` | 已算科研证据、平台交付和补数方案 |
| recovery CAS | `owner_id/fencing_token/lease_expires_at/row_version` | AIstock recovery worker 的 claim、lease、fencing 与乐观 CAS |
| time | `created_at/started_at/completed_at/updated_at` | 生命周期时间 |

repository 使用 `FOR UPDATE SKIP LOCKED` claim 非终态 row；heartbeat、remote identity、receipt、CAS 和 terminal 更新必须匹配 `owner_id + fencing_token + row_version`。lease 过期只允许新 owner claim/reconcile，不能直接把远端未知任务标失败。相同 evaluation identity 和相同 request 返回已有 row；相同 identity 不同 request/environment/bundle 返回结构化 conflict。

Phase 2 migration 必须同时提供 forward/preflight/guarded rollback 与 schema/comment/readback 测试。设计修订不执行 DDL；未来 DEV 与生产应用分别遵循现有授权边界，不要求数据库导出、额外备份或研究审批。Phase 3 只新增 `run_evaluation_metric/run_evaluation_artifact`，继续引用本表主键。

### 10.2 normal 与 historical 的精确时序

normal opt-in 命令由 ConfigComposer 固定为：

```text
qrun_limit*.py
  -> long_trend_postprocess_adapter.py register-with-AIstock-and-submit
  -> read_exp_res.py attach registration_receipt JSON
```

qrun 前的 descriptor 只冻结 profile/evaluator/environment/dataset/frequency 和父 task/Loop 身份；最终 Recorder artifact path/hash 必须等 qrun 完成后由 adapter 从 exact recorder ref/catalog 取得。adapter 使用原 Loop resource webhook token调用 internal registration endpoint并提交 recorder ref/catalog digest，不自行生成 evaluation identity、不直接 POST RD evaluation job。AIstock 重新读取/核对 RD catalog，构建 input/evaluation identity，在数据库事务中写 queued control row和独立 qelt resource session，提交事务后才调用 RD typed job API，并把 evaluation/job identity 返回 adapter。

adapter 只能消费冻结 descriptor，不接受 shell command、任意 path、profile override 或数据 root override。AIstock control row提交并收到 RD durable queued/submitted receipt 后，adapter 原子写 `qe_long_trend_registration_v1` 并立即返回；不等待 CPU slot、worker terminal 或 Windows CAS。因此原 Loop 可以继续 `read_exp_res.py`、结束进程并释放既有 execution reservation，下一 Loop 的 GPU 训练不被长期评价占位。

worker terminal 后生成 `qe_long_trend_worker_compact_v1`，其 `artifact_manifest_uri/hash` 使用 typed null，`platform_delivery_status.cas=awaiting_collect`。AIstock collector 发布 CAS 后生成同 `evaluation_id/worker_terminal_sha256` 的 `qe_long_trend_published_compact_v1`，补齐 CAS URI/hash并更新 control row。registration、worker、published 三种 receipt 的 schema/stage 分开，不能覆盖或互相冒充；科研指标以 worker/published 为准，registration 只证明任务身份和已提交状态。

评价 worker 的 succeeded/partial/failed/cancelled 由独立 supervisor/control row 保存，normal adapter 不等待也不改写它。评价平台失败不改变 qrun/read_exp_res 的原始退出码、训练/回测状态或既有指标。若 AIstock webhook 在 control row 创建前不可达，adapter 原子保存 `postprocess_registration_pending.json`，并在 `.qe_long_trend_registration_pending/` 写入不含 token/secret 的 task/Loop、descriptor、adapter 与 pending receipt hash index；descriptor、Recorder ref/catalog 与父 resource secret 仍保留在该 Loop 的权威文件中。`read_exp_res.py` 只挂载 typed pending observation 后继续；RD startup replayer 仅枚举专属 index，校验全部 hash 后在精确父 Loop 固定重放 adapter，不扫描 Archive 或全部 workspace。若 control row 已存在，后续只 reconcile，不重复提交。adapter 自身在任何异常分支都必须持久化 control-row reason 或本地 typed failure observation，禁止空成功。historical `long_trend_only` 不执行 qrun/read_exp_res，直接使用相同 submit/inspect/collect/publish 方法。

## 11. Resource Phase and Recovery / 资源阶段与恢复

### 11.1 状态转换

每个 evaluation 创建独立 resource session；control repository 在同一个 PostgreSQL transaction 内写入 `source_run_key="qelt:<evaluation_id>"` 的 `run_resource_session` 与 queued control row。`task_id/loop_index` 仍绑定父 QE Loop，`phase_pipeline_enabled=false`，不调用普通 Loop `create_session`、不预留 GPU、不续接或重开原训练/回测 resource session。扩展 transition：

```text
created -> long_trend_eval -> finalize
long_trend_eval -> finalize|completed|failed|cancelled
```

normal 与 historical 都走该独立状态机。不得为了满足 transition 伪造 bootstrap/backtest 事件，也不得把 evaluation retry attempt 记成原训练 attempt。现有普通 Loop 状态机、GPU lease 和 terminal row 保持不变。

事件 metadata：`evaluation_id/job_id/attempt_id/node_id/cpu_seconds/rss_peak_bytes/read_bytes/output_rows/artifact_bytes/catalog_completeness`。资源数字是 worker 自身 receipt，不做系统级轮询，也不参与研究 go/stop。

### 11.2 认证与 outbox

- 复用 resource session token SHA、单调 sequence 和 `qelt:<evaluation_id>` source binding；
- secret 单独写入 0600 文件，request/manifest/log 不包含明文；
- worker 本地 outbox 保存未送达事件；重复 sequence 幂等，payload hash 不同则 conflict；
- AIstock 不可达时 worker 继续完成计算和 terminal receipt；恢复后重放事件。

### 11.3 reconcile 决策

| 观察 | 行为 |
|---|---|
| node job running 且 process identity 匹配 | 接管观察，不重启 |
| node terminal + CAS 未发布 | streaming collect，不重算 |
| CAS manifest 已成功且 hash 匹配 | 返回已有结果 |
| node terminal receipt 存在但 artifact 缺失 | platform CAS failed/partial，保留 family receipt并允许只重传 |
| running manifest 但进程不存在 | attempt failed，新增 retry attempt，不覆盖旧证据 |
| 同 identity request/bundle 不同 | fail-fast conflict |
| node/网络暂不可达 | `REMOTE_STATE_UNKNOWN`，不伪造失败 |

AIstock startup reconciliation 只 claim `run_evaluation` 非终态行并执行上述决策；RD startup dispatcher 只扫描 `<loop_dir>/long_trend_evaluations` 既有 job，registration replayer 只枚举 `.qe_long_trend_registration_pending/` 专属 index。两端都不得扫描非 QE 表、Archive 全量或全部 workspace，不得创建新研究任务或重跑已成功 family。queued job 与 control-row 创建前的 typed pending registration 均可在服务重启后按冻结身份继续，不依赖下一个用户请求。

## 12. Dedicated QELongTrendArtifactStore / 专属 CAS

### 12.1 root 与 URI

- env：`QE_LONG_TREND_ARTIFACT_STORE_ROOT`
- DEV/test 默认：`<repo_root>/rdagent_assets/long_trend_evaluation_store`
- production：必须由部署环境显式配置稳定绝对 root；不得因进程 cwd、worktree 或 checkout 改变实际 store
- URI：`aistock-qe-long-trend://evaluations/<evaluation_id>/<artifact_type>`
- 禁止 E: HDD；允许显式 F: SSD root；不得与 Prediction Store root 相同。

root resolve、权限、与 Prediction Store `samefile` 检查失败只把 CAS platform delivery 标为 failed/awaiting_repair；节点 worker terminal receipt 和已算 family 继续保留，不把存储配置问题变成研究方向淘汰条件。

### 12.2 artifact allowlist

| artifact_type | schema |
|---|---|
| `worker_compact_receipt` | `qe_long_trend_worker_compact_v1` JSON |
| `published_compact_receipt` | `qe_long_trend_published_compact_v1` JSON |
| `signal_observations` | `qe_long_trend_signal_observation_v1` Parquet |
| `holding_episodes` | `qe_long_trend_holding_episode_v1` Parquet |
| `worker_terminal_receipt` | `qe_long_trend_worker_terminal_v1` JSON |

execution evidence 已作为规范化列进入 `signal_observations/holding_episodes`；sector/family metrics 和 data actions 进入 `worker_terminal_receipt`，Phase 3 再写 `run_evaluation_metric`。不新增可由两份核心 Parquet 和 terminal JSON 无损推导的重复 Parquet。每项保存 sha256、schema_sha256、size、row_count、columns/dtypes、evaluation/input/bundle/environment identity、attempt 和 source node。未知 artifact type 拒绝，不变成通用文件仓库。

required 集合按 worker 的真实 family status 冻结，不能由 collector 临时猜测：

| 条件 | 必需制品 | 允许显式不产出 |
|---|---|---|
| 任意 terminal attempt | `worker_terminal_receipt`、`worker_compact_receipt` | 无；即使 task failed/cancelled 也必须有结构化 terminal evidence |
| `signal_path` 为 `COMPUTED/COMPUTED_WITH_LIMITATIONS` | `signal_observations`，包括合法零行但 schema 完整的 Parquet | 当该族 `NOT_COMPUTABLE` 时 manifest 记录 typed absence/reason |
| `position_episode` 为 `COMPUTED/COMPUTED_WITH_LIMITATIONS` | `holding_episodes`，包括合法零行但 schema 完整的 Parquet | 当该族 `NOT_COMPUTABLE` 时 manifest 记录 typed absence/reason |
| AIstock 完成所有当前适用制品校验 | `published_compact_receipt` | CAS 未发布时不存在，control row 保持 awaiting/partial，不影响 worker family status |

`portfolio_result/order_fill/execution_cause/sector_regime` 的计算结果保存在 terminal metrics/family JSON；其输入不足不得要求一个不存在的派生 Parquet，也不得阻止其他适用制品发布。

### 12.3 streaming 与原子发布

1. QE client 新增 async streaming download，逐块更新 SHA-256 并写 CAS `tmp/`；
2. size/hash/schema 与 node catalog 不一致时删除临时文件并返回结构化错误；Parquet 校验只读取 footer/schema/row-group metadata，不在 Windows 反序列化全量行；
3. blob 路径按内容 hash，已存在 blob 必须 size/hash 一致；
4. 按 worker terminal receipt 冻结的 required matrix 校验所有适用项和 typed absence 后写 staging manifest；
5. 持有 evaluation 级跨进程 OS lock 后二次比较已有 manifest；POSIX 使用文件 fsync + atomic replace + directory fsync，Windows 使用 `MoveFileExW(REPLACE_EXISTING|WRITE_THROUGH)`，相同内容幂等、不同内容冲突；
6. manifest 成功后才把 CAS platform status 标为 published；
7. retry 只重传缺失 blob或重建 published compact receipt，不重新计算成功 family；
8. published receipt/manifest 与 control row 使用同一 owner/fencing/row_version CAS；DB 更新失败保留已发布 immutable manifest并进入 reconcile，不删除 CAS。

## 13. Compact Receipt Contract / 紧凑回执

`read_exp_res.py` 只挂载 normal registration receipt：

```json
{
  "schema_version": "qe_long_trend_registration_v1",
  "receipt_stage": "registered",
  "evaluation_id": "qelt_...",
  "profile_id": "qe_long_trend_v1",
  "job_id": "qelt_job_...",
  "request_sha": "...",
  "evaluation_asof": "YYYY-MM-DD",
  "task_status": "queued|submitted|registration_pending",
  "platform_delivery_status": {"worker": "queued|submitted|registration_pending", "cas": "awaiting_worker"},
  "artifact_manifest_uri": {"typed_null": "awaiting_cas_publish"},
  "artifact_manifest_sha256": {"typed_null": "awaiting_cas_publish"},
  "worker_terminal_sha256": {"typed_null": "awaiting_worker_terminal"}
}
```

registration receipt 只含任务身份、提交状态和 typed pending 字段，不含尚未计算的 family/headline/maturity/execution/data-action。所有 dict 有大小上限，禁止内联逐股票、逐日、逐 episode、日志或秘密。receipt 缺失时 `read_exp_res.py` 不创建空成功对象，只保留原有 QE 指标并写结构化 `long_trend_registration_status=missing` 与已知 registration reason；不得伪造 evaluation success。

worker terminal 生成 `qe_long_trend_worker_compact_v1`，开始包含 family/headline/maturity/execution/data-action summary；AIstock CAS 发布后生成 `qe_long_trend_published_compact_v1`：`receipt_stage=cas_published`，保留相同 `evaluation_id/worker_terminal_sha256/family_status/headline_metrics`，补入不可变 `artifact_manifest_uri/sha256`。worker/published receipt 进入专属 CAS 与 control row，不回写覆盖 registration receipt，也不要求重跑 `read_exp_res.py`。Phase 3 API/MCP/UI 以 published receipt 为平台交付权威，同时展示 registration/worker receipt hash用于核对。

## 14. Failure Semantics / 失败语义

新增/沿用 reason：

| reason_code | 语义 |
|---|---|
| `QELT_BUNDLE_INVALID` | bundle 文件、hash、allowlist 或 ABI 不一致 |
| `QELT_NODE_CAPABILITY_UNAVAILABLE` | RD 节点尚未声明 long-trend v1 capability；只影响显式评价请求 |
| `QELT_EXECUTION_ENVIRONMENT_MISMATCH` | environment snapshot/hash/依赖能力与冻结 request 不一致 |
| `QELT_RECORDER_REF_MISSING` | 无权威 recorder ref；依赖 Recorder 的族局部受限 |
| `QELT_WORKSPACE_CATALOG_PARTIAL` | catalog 部分可见；保存限制与尝试路径 |
| `QELT_INDICATOR_FREQUENCY_CONFLICT` | 冻结 backtest frequency 与 Recorder indicator 路径不一致 |
| `QELT_INDICATOR_OBJECT_MISSING` | `_obj.pkl` 不可得；order_fill 局部受限且不得用汇总文件替代 |
| `QELT_PICKLE_PARSER_FAILED` | 隔离 parser 异常/超时/非零退出；保留其他已规范化 family |
| `QELT_CONTROL_STATE_CONFLICT` | run_evaluation owner/fencing/row_version 或 immutable identity 冲突 |
| `QELT_NODE_JOB_IDENTITY_CONFLICT` | 同 evaluation/job identity 请求内容不同 |
| `QELT_NODE_PROCESS_IDENTITY_CONFLICT` | cancel/reconcile 的进程身份不匹配 |
| `QELT_NODE_STATE_UNKNOWN` | 节点暂不可达；不假失败 |
| `QELT_ARTIFACT_STREAM_INTERRUPTED` | 下载中断；可只重传 |
| `QELT_ARTIFACT_HASH_MISMATCH` | node catalog 与接收内容不一致 |
| `QELT_ARTIFACT_SCHEMA_MISMATCH` | Parquet/JSON schema 与注册版本不一致 |
| `QELT_CAS_MANIFEST_CONFLICT` | 成功 identity 下出现不同 manifest |
| `QELT_RESOURCE_EVENT_INVALID` | token/sequence/binding 非法 |

禁止 `except: pass`、空 dict 成功、0 填补缺失、当前数据替换指定 snapshot、complete catalog 后继续猜文件、CAS 失败后回写训练/回测失败，以及因任何 family 缺失取消其他 family。

## 15. File-Level Implementation Plan / 逐文件实施计划

### 15.1 AIstock 新增

- `backend/services/quantevolver/long_trend_evaluation_bundle.py`
- `backend/services/quantevolver/long_trend_artifact_resolver.py`
- `backend/services/quantevolver/long_trend_artifact_store.py`
- `backend/services/quantevolver/long_trend_evaluation_control_repository.py`
- `backend/services/quantevolver/long_trend_evaluation_phase2.py`
- `backend/services/quantevolver/long_trend_pickle_parser_entry.py`
- `backend/services/quantevolver/long_trend_worker_entry.py`
- `backend/services/quantevolver/templates/long_trend_postprocess_adapter.py`
- `backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.sql`
- `backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.preflight.sql`
- `backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.rollback.sql`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_control_repository.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`

### 15.2 AIstock 修改

- `experiment_config.py`：opt-in profile descriptor；默认无字段
- `config_composer.py`：冻结 bundle/profile/environment/snapshot/frequency descriptor，并将 normal 命令固定为 qrun → adapter → read_exp_res
- `qe_workspace_client.py`：typed submit/inspect/catalog/stream/cancel methods
- `qe_active_execution_capacity.py`：透传 postprocess descriptor，不改变 GPU reservation
- `qe_resource_phase_service.py`：加入显式 qelt source key、`created -> long_trend_eval` transition 和 CPU metadata validation
- `results_only_retry.py`：抽取共享 recorder ref/path resolver；保持现有 results-only 语义
- `templates/read_exp_res.py`：只挂载 registration receipt；不等待或伪造 worker/CAS published receipt
- `backend/routers/quantevolver_evolution.py`：增加沿用 resource webhook token/source binding 的 internal postprocess registration；不暴露用户创建 API
- `backend/main.py`/现有 QE runtime lifecycle：只注册 run_evaluation 非终态 startup reconciliation，不创建周期 scheduler
- CI ownership catalog：登记新增 QE backend 与跨仓库测试路径

### 15.3 RD-Agent 新增

- `rdagent/app/api_endpoints/qe_long_trend_evaluation.py`
- `rdagent/app/api_endpoints/qe_long_trend_registration_replayer.py`
- `rdagent/app/api_endpoints/qe_long_trend_worker.py`
- `test/app/test_qe_long_trend_evaluation_api.py`
- `test/app/test_qe_long_trend_worker_recovery.py`

### 15.4 RD-Agent 修改

- `qe_evolution_api.py`：include loop-owned evaluation router / optional postprocess descriptor
- `qe_workspace_catalog.py`：evaluation 子目录 artifact catalog，secret 排除
- `qe_dataset_identity.py`：显式 registered root/snapshot identity 对齐，不扫描未授权目录
- `qe_environment_identity.py`：在现有 immutable manifest 中声明 `qe_long_trend_evaluation_v1` capability，不增加资源轮询
- QE API startup lifecycle：只恢复既有 evaluation spool/slot，不创建新 job

Phase 2 不修改 frontend、MCP、Selection/Advisory/Paper/模拟盘/QMT/StrategyPackage 或通用 Prediction Store。允许的基础注册仅为专属 migration、AIstock QE startup reconciliation 和 RD QE startup spool recovery；ownership/import/route 审计必须证明这些入口不能调用非 QE 模块。

## 16. Implementation Sequence / 开发顺序

1. **P2-A control + identity**：`run_evaluation` migration/repository、environment/bundle/input identity、claim/lease/fencing 与 opt-in config；
2. **P2-B resolver + parser**：exact Recorder/catalog/frequency/snapshot resolver、`_obj.pkl` 权威映射、无秘密 parser subprocess 与 family-local inventory；
3. **P2-C node job**：typed API、atomic spool/claim、CPU 单槽、external subprocess、status/cancel/startup recovery；
4. **P2-D CAS**：family-aware required matrix、streaming/footer validation、atomic publish、conflict/retry；
5. **P2-E resource + dual receipt**：独立 qelt resource session/outbox、normal adapter、worker/published compact receipt 与 historical parity；
6. **P2-F cross-repo canary**：一个 deterministic fixture 与一个已完成 Loop 的显式 `long_trend_only` canary，不启动训练。

这些是工程实施顺序，不是研究门禁。P2-A～P2-E 可在文件所有权不冲突时并行开发；缺失真实 artifact 时使用完整 fixture 验证 wiring，同时记录真实数据补取任务，不能用 fixture 冒充真实 canary。

## 17. Verification Plan / 验证计划

### 17.1 单元与合同

- bundle path/hash/ABI/environment/依赖能力/额外文件拒绝；
- evaluation identity、environment hash、typed null、normal/historical parity；
- complete/partial catalog、exact recorder path、frozen frequency、summary vs `_obj.pkl` schema、missing optional family；
- parser 无 resource/DB/Prediction secret、任意 path 拒绝、parser crash/timeout family-local receipt；
- snapshot same/verified-extension/mismatch/missing identity；
- `run_evaluation` migration/preflight/guarded rollback、queued-before-POST、claim/lease/fencing/row-version CAS；
- artifact allowlist、family-aware required/typed absence、stream interruption、hash/footer/schema mismatch、atomic manifest、duplicate conflict；
- `created -> long_trend_eval`、qelt source binding、sequence replay/conflict、secret 不出现在 parser/evaluator/log/manifest；
- cancel process fencing、stale PID、already-terminal；
- worker/published compact receipt schema、typed CAS null、大小与字段 allowlist。
- internal registration webhook token/source binding、queued-before-RD-POST、AIstock unreachable pending file 与 exact Loop recovery。

### 17.2 集成

1. `run_evaluation=queued` 事务提交 → RD API submit → queued → atomic claim → running → terminal；
2. normal 严格执行 `qrun -> adapter register/submit -> read_exp_res`，证明原 Loop reservation及时释放；历史 `long_trend_only` 对同输入产生相同 family metrics 和核心 artifact hash；
3. evaluator/platform failure 不改变 qrun/read_exp_res 原始退出码、Loop 回测结果或原 resource session terminal；
4. AIstock 在 submit 前、submit 后、worker terminal 后、CAS publish 后四个注入式重启点均从 control row inspect/collect，不产生第二 worker；
5. RD API 在 queued claim 前、starting 后、running 中三个重启点从 job/claim/process manifest 恢复；
6. CAS publish 中断后只重传适用缺失 blob或重建 published receipt；
7. node 暂不可达返回 unknown，不假失败；
8. 真实已完成 Loop canary：不训练、不回测，只生成 F-014 control row/receipt/CAS；
9. H5/Parquet 内存 receipt 证明 chunk 释放，内存不随 Loop 数累积。

### 17.3 零影响审计

- import allowlist 不出现非 QE module；
- route diff 只允许 RD `/qe_workspace/**` 子资源和 AIstock 既有 QE webhook 下的 authenticated internal registration；AIstock Phase 2 无用户 public create/query route；
- DB schema diff 只允许 `qe_archive.run_evaluation`、约束/索引/comment；不得出现 metric/artifact 或非 QE 表；
- Prediction Store manifest/hash 回归不变；
- 普通未启用 profile 的 Loop request、runner、phase sequence 与 enhanced metrics 不变；
- 启用 profile 的 normal Loop 仅新增独立 qelt resource session和受限 worker receipt，不改原 Loop terminal/metrics；
- 不出现 GPU telemetry/NVML/`nvidia-smi`；
- DESIGN-COMPLIANCE-001 四项逐条复核。

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-014 | parent Phase 2 scope linkage | `backend/tests/scripts/test_aistock_feature_workflow.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_long_trend_evaluation_phase2_compute_cas_f2_design_20260722.md --tier F2`; source implementation is locally verified while merge/DDL/runtime/canary remain separately reported operational facts | implemented_local_verified | none |
| F-301 | `backend/services/quantevolver/long_trend_evaluation_phase2.py` QE identity validation | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py` | implemented_local_verified | none |
| F-302 | shared bundle/resolver/engine/store | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; real canary remains an activation receipt rather than a source-design exception | implemented_local_verified | none |
| F-303 | bundle builder + environment-bound RD allowlist extractor | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; RD evaluation API/capability tests | implemented_local_verified | none |
| F-304 | exact recorder/catalog/frequency resolver + isolated parser | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; RD `test/app/test_qe_long_trend_evaluation_api.py` | implemented_local_verified | none |
| F-305 | dataset identity + strict reader | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; existing `backend/tests/unified_engine/test_qe_long_trend_contract_reader.py`; real dataset canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-306 | QE worker + streaming client | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py`; real worker canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-307 | Phase 1 family-local engine wrapper | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; existing `backend/tests/unified_engine/test_qe_long_trend_evaluation_core.py` | implemented_local_verified | none |
| F-308 | RD request/job/claim/attempt/process manifests | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_evaluation_api.py` | implemented_local_verified | none |
| F-309 | RD FIFO CPU slot held for worker lifetime | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py`; multi-job canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-310 | independent `qelt:<evaluation_id>` resource phase/outbox | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; `backend/tests/unified_engine/test_qe_resource_phase_service.py`; DDL/runtime state is reported separately | implemented_local_verified | none |
| F-311 | control-row inspect/reconcile/collect | `backend/tests/unified_engine/test_qe_long_trend_phase2_control_repository.py`; `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py`; restart canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-312 | dedicated long-trend CAS + family-aware required matrix | `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py` | implemented_local_verified | none |
| F-313 | immutable success manifest | `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py` | implemented_local_verified | none |
| F-314 | registration/worker/published staged receipt adapter | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; real normal Loop canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-315 | worker terminal receipt | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_evaluation_api.py` | implemented_local_verified | none |
| F-316 | typed cancel | validation-receipt: RD-Agent `python -m pytest -q test/app/test_qe_long_trend_evaluation_api.py`（与 recovery 文件合计 12 passed）；live-PID canary is separately tracked as activation evidence | implemented_local_verified | none |
| F-317 | cross-repo parity/restart/failure/memory matrix | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py`; DDL、两端重启与真实已完成 Loop canary 尚未执行且不冒充本地测试证据 | implemented_local_verified | none |
| F-318 | ownership/import/route/schema diff | `python scripts/aistock_module_ownership_scan.py --changed-only --include-untracked --fail-on-unmapped --fail-on-ambiguous`; `python -m nox -s validation_catalog_integrity validation_module_registry_l0`; 32 个初始变更文件与后续 BUG-829 文件均重新执行目录路由 | implemented_local_verified | none |
| F-319 | `long_trend_evaluation_control_repository.py` + Phase 2 migration | `backend/tests/unified_engine/test_qe_long_trend_phase2_control_repository.py`; restart injection tests；DDL 未应用并作为生产状态单独报告 | implemented_local_verified | none |
| F-320 | execution-environment identity binding | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; RD `test/app/test_qe_long_trend_evaluation_api.py`; deployment restart state is separately reported | implemented_local_verified | none |
| F-321 | frequency-aware summary/`_obj.pkl` resolver | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; `backend/tests/unified_engine/test_qe_long_trend_evaluation_core.py` | implemented_local_verified | none |
| F-322 | normal nonblocking adapter + internal registration + staged receipts | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD pending replay tests；real normal Loop canary is separately tracked as activation evidence | implemented_local_verified | none |

## 19. Rollout and Rollback / 发布与回滚

### 19.1 Rollout

1. AIstock 与 RD-Agent 分别使用独立 feature branch/PR；
2. RD 先交付 API/worker/capability；AIstock 在提交前读取 immutable environment capability，旧 RD 返回结构化 `QELT_NODE_CAPABILITY_UNAVAILABLE`，只影响显式评价请求，不影响普通 Loop；
3. AIstock 合入 control migration/repository、typed client/orchestrator/adapter；未启用 profile 时 normal Loop 无命令或状态变化；
4. 未来执行实现时先在现有 DEV DB 应用/readback Phase 2 control migration；生产 DDL、AIstock backend 与 WSL/远端 QE API 重启分别由用户明确授权，不以数据库导出或额外备份为前置条件；
5. 重启后先跑 deterministic fixture，再显式选择一个已完成 Loop 执行 `long_trend_only` canary；
6. canary 只写 `run_evaluation` 控制行、QE Phase 2 job/resource event 和专属 CAS，不写 metric/artifact 表。

### 19.2 Rollback

- profile 默认关闭；停止提交新 evaluation job 即停止新增计算；
- 已运行 node worker 不因 AIstock rollback 被强杀，先 inspect/collect/cancel typed attempt；
- CAS 内容按 hash immutable，rollback 不删除历史 receipt；
- 回滚 AIstock/RD code 不修改训练、回测或 Archive 既有结果；
- control migration rollback 仅在 `run_evaluation` 为空且无引用时允许；已有评价证据时保留 additive table并回滚 writer/route，禁止删除历史 control/receipt。修复采用 forward migration。

## 20. Risks and Mitigations / 风险与应对

| 风险 | 影响 | 应对 |
|---|---|---|
| 历史 Recorder catalog 不完整 | 某些 family 无法解析 | 保存 partial catalog、尝试路径和 data action；其他 family 继续 |
| remote job 与 backend 状态短暂分离 | 重复启动或错误终态 | job/attempt/process identity + inspect/reconcile，不可达标记 unknown |
| queued job 在 AIstock/RD 重启窗口丢失 | 永久排队或重复 worker | queued-before-POST control row + node spool/claim + 双端 startup reconciliation |
| H5/Parquet 内存峰值 | 节点卡顿或 OOM | 单 CPU 槽、signal-date chunk、streaming collect、父对象及时释放 |
| CAS 中途失败 | 结果已算但未发布 | 保留 terminal receipt，只重传缺失 blob，不重算成功 family |
| evaluator bundle 漂移 | 同 identity 结果不可复算 | source/bundle/file hash 进入 identity，冲突 fail-fast |
| Python/Qlib/数据依赖环境漂移 | 同源码同数据产生不同结果 | execution-environment snapshot/hash 进入 evaluation identity、request、attempt、CAS |
| pickle 制品越权执行 | parser 读取秘密或任意文件 | exact QE provenance + fixed parser subprocess + supervisor 独占 secret + input/output allowlist |
| registration/worker/published receipt 混用 | read_exp_res 阻塞原 Loop或伪造科研结果/已发布状态 | 三种 schema/stage 分离；registration 无指标，worker CAS 字段 typed null，published 由 AIstock CAS 后生成且互不覆盖 |
| order/trade/position 证据不足 | execution cause 无法判断 | 明确 `NOT_VERIFIABLE`，不以日线猜测，不影响 signal/sector/portfolio 已有证据 |
| CPU 评价影响 GPU 训练 | 桌面或节点资源争用 | 默认单槽、队列、只记录自身资源；不抢占、不终止、不提高图模型并发 |
| 跨仓库版本错配 | API/worker 合同错误 | RD 先合入，AIstock typed client 后合入，双方 schema/hash contract test |

## 21. Production Gates / 实施事实（不定义科研门禁）

| 项目 | 状态 |
|---|---|
| design | `IMPLEMENTED_LOCAL_VERIFIED_NOT_ACTIVATED`（F2 validator 23/23、路由本地测试与跨仓库定向测试通过） |
| Phase 1 core | `MERGED_VERIFIED` |
| AIstock Phase 2 source | `IMPLEMENTED_LOCAL_NOT_MERGED` |
| RD-Agent Phase 2 source | `IMPLEMENTED_LOCAL_NOT_MERGED` |
| production_ddl_gate | `pending`（forward/preflight/guarded rollback 已开发并做静态/合同测试，未应用 DEV 或生产 DDL） |
| production_frontend_dependency_gate | `noop` |
| production_backend_dependency_gate | `noop` |
| runtime | `UNTOUCHED_BY_IMPLEMENTATION` |
| DB/data/experiments | `UNTOUCHED_BY_IMPLEMENTATION` |
| research gates/approvals | `NONE_ADDED` |

## 22. DESIGN-COMPLIANCE-001 Review / 设计符合性复核

- [x] **禁止简化交付**：Phase 2 同时覆盖 queued-before-POST 控制记录、normal/historical 两入口、真实 resolver、节点原子 claim、双端恢复、专属 CAS、三阶段 receipt、cancel 和跨仓库测试；fixture 不能冒充真实 canary，Phase 2 也不冒充 F-014 全平台完成。
- [x] **禁止静默错误**：所有 control/environment/identity/catalog/frequency/parser/worker/stream/CAS/resource 异常都有结构化状态/reason；required artifact 按 family 冻结；禁止空 dict、0 填补、当前数据替换指定 snapshot及 `except: pass`。
- [x] **禁止业务逻辑偏移**：恢复 `indicators_normal_{freq}_obj.pkl` 的 amount/deal_amount/ffr 权威语义；Phase 1 profile、公式、family status、训练/回测终态和 evidence authority 原样复用，不新增冗余派生 Parquet。
- [x] **禁止私增门禁审批**：唯一硬边界是 QE-only 零影响；数据、制品、成熟度、显著性和平台状态只作为科研证据与补取计划，不形成研究许可、淘汰或方向停止状态。
