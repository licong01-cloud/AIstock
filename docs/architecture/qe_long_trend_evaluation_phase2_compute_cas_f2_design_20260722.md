# QE 长期趋势评价 F-014 Phase 2：计算节点、专属 CAS 与恢复编排 F2 设计

- 文档类型：F2 跨仓库从属实现设计
- 日期：2026-07-22
- 状态：`DESIGN_READY_CODE_NOT_STARTED`
- 父级权威：`docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md` v1.5
- 上位研究蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v5.7
- 代码范围：AIstock QuantEvolver / QE Workspace Client / QE Resource Phase / RD-Agent QE Workspace
- 唯一硬边界：QE-only；不得读取、修改或触发 Selection、Advisory、Paper、模拟盘、荐股、QMT、StrategyPackage 或其他非 QE 链路

本设计不增加研究准入、淘汰、审批、确认或方向停止逻辑。输入、制品或数据缺失时，系统保存真实缺口、已完成指标族、影响范围和补取方案；缺失项不能取消其他科研结果，也不能阻止其他 QE 实验并行执行。

## 0. Decision Summary / 决策摘要

Phase 1 已经交付纯契约、严格 QE 数据读取和长期评价计算核。Phase 2 不重写这些公式，也不提前实现 Phase 3–5 的数据库、公共 API、MCP 和 UI，而是完整交付以下五个计算平台能力：

1. 将冻结的 evaluator 源码、profile、数据身份和 Recorder 输入身份绑定到 normal Loop 与 `long_trend_only` 两种入口；
2. 在 QE 节点解析真实 Qlib Recorder 与显式数据快照，不在 Windows FastAPI 中加载 H5；
3. 由节点外部进程执行同一 Phase 1 evaluator，并持久化 job/attempt/terminal receipt；
4. 将逐信号、逐 episode、执行证据和板块明细原子发布到独立的 `QELongTrendArtifactStore`；
5. 复用现有 QE resource session 的认证 sequence/outbox 语义，支持后端重启后的 inspect/reconcile/collect，不重复计算或覆盖成功制品。

Phase 2 的完成不会宣称 F-014 平台整体完成。三张评价表、公共 API、MCP、UI、批量历史回填和完整 E2E 仍分别属于父设计 Phase 3–5；但 Phase 2 产出的真实 receipt 与 CAS 制品可以立即用于科研分析和后续联调。

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
| 节点环境/数据身份 | `get_execution_environment()`、`get_dataset_identity()` | 直接复用；不从 AIstock 本机猜测远端身份 |
| 完整文件目录 | `list_workspace_files()`、`stat_workspace_file()` | resolver 只消费 catalog 声明的文件，不猜常见路径 |
| 文件下载 | `download_workspace_file_bytes()` | 小型 JSON/pickle 可复用；大 Parquet 新增 streaming 方法，禁止全量 bytes 常驻内存 |
| Recorder 解析 | `results_only_retry.py::collect_results_only_artifacts()` | 提取 `qe_current_recorder.json` 和精确 mlruns 前缀的规则抽成共享 resolver；不复制第二套猜测逻辑 |
| 通用预测 CAS | `model_store/artifact_store.py::PredictionArtifactStore` | 仅借鉴 blob hash、临时文件和原子 manifest；namespace、root、URI、allowlist 完全独立 |
| 资源阶段 | `qe_resource_phase_service.py::QEResourcePhaseService` | 增加 `long_trend_eval` 合法 phase 与 CPU postprocess 元数据；不恢复 GPU 监测 |
| 统一节点容量 | `qe_active_execution_capacity.py` | normal Loop 提交透传 postprocess descriptor；不把 CPU 评价计为 GPU Loop，不改变现有模型并发上限 |
| QE Archive | `qe_archive.run`、reproducibility manifest | Phase 2 只读父 run identity；不写通用 metric/artifact 表 |
| RD-Agent QE API | `rdagent/app/api_endpoints/qe_evolution_api.py` | 在现有 QE Workspace router 下增加 loop-owned evaluation job；不建设第二个服务 |
| RD 文件目录/数据身份 | `qe_workspace_catalog.py`、`qe_dataset_identity.py` | 扩展 evaluation 子目录 catalog 与显式 snapshot root 解析，保持 allowlist/path containment |

### 2.3 当前缺口

1. Phase 1 engine 只接受已经解析好的 DataFrame/对象，没有真实 Recorder resolver；
2. normal Loop submission 没有冻结的长期评价 postprocess descriptor；
3. RD-Agent 没有 `evaluation_id` 级 job、attempt、process identity、receipt 和取消语义；
4. `QEResourcePhaseService` 不认识 `long_trend_eval`；
5. 没有 QE 长期评价专属 CAS、schema manifest、streaming collect 与 conflict 检测；
6. `read_exp_res.py` 没有挂载 compact receipt；
7. 后端重启后没有 evaluation inspect/reconcile/collect 服务。

## 3. Scope / 范围

### 3.1 本阶段完整交付

- normal Loop 的显式 profile opt-in postprocess descriptor；
- 已完成 Loop 的内部 `long_trend_only` 服务入口，供 Phase 3 API 直接复用；
- evaluator deployment bundle、源码 hash 和 bundle allowlist；
- QE Recorder、workspace catalog、dataset identity 与 snapshot parity resolver；
- RD-Agent loop-owned evaluation job API、文件 spool、外部进程、attempt 和 terminal receipt；
- CPU 单槽排队、resource phase/outbox 事件与恢复；
- 独立 `QELongTrendArtifactStore`、streaming collect、原子 manifest 和内容冲突检测；
- compact receipt 注入 `enhanced_metrics` 的适配器；
- normal/historical 同核一致性、重启恢复、重复提交、部分输入、CAS 中断和内存释放测试；
- AIstock 与 RD-Agent 两仓库的定向测试及 QE-only 隔离审计。

### 3.2 本阶段不实现

- 不创建 `run_evaluation/run_evaluation_metric/run_evaluation_artifact` 三张表；
- 不新增公共 FastAPI route、MCP tool 或 frontend 页面；
- 不执行 R8–R11 全量历史补算；只允许一个已完成 Loop 的显式 canary；
- 不注册 startup/cron/global scheduler，不扫描全部 Archive；
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
| F-314 | compact receipt 与大明细分离；`read_exp_res.py` 只挂载受限 JSON，不内联 Parquet 或复制全量数据。 |
| F-315 | terminal receipt 保存 family/platform 状态、资源、输入/输出 hash、限制和 data action；不把平台失败写成研究失败。 |
| F-316 | typed cancel 只终止指定 evaluation attempt，并保留已成功 artifact；不调用训练 Loop kill，不修改回测结果。 |
| F-317 | normal/historical parity、重复请求、重启、CAS/下载中断、partial catalog、missing artifact 和 memory release 均有直接测试。 |
| F-318 | AIstock/RD-Agent ownership、import、route 和 runtime 回归证明非 QE 模块、通用 Prediction Store 和现有 Loop 语义零变化。 |

## 6. End-to-End Architecture / 端到端架构

```text
AIstock explicit QE caller
  ├─ normal Loop: ConfigComposer + submission payload postprocess descriptor
  └─ historical: QELongTrendPhase2Service.prepare_long_trend_only(...)
        │
        ├─ validate qe_archive.run + task/Loop + dataset/workspace identity
        ├─ build frozen evaluator bundle and evaluation identity
        ├─ create existing QE resource session (CPU postprocess semantics)
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
              └─ staged Parquet + compact receipt + terminal manifest
        │
        ├─ authenticated monotonic resource phase events
        └─ durable job/status/artifact catalog inspection
        ▼
AIstock streaming collector
        ├─ verify catalog, size, sha256 and schema hash
        ├─ publish to dedicated QELongTrendArtifactStore
        └─ attach compact receipt only
```

Phase 2 不让 RD-Agent 直接写 PostgreSQL。Phase 3 repository 将消费同一 terminal receipt 和 CAS manifest，因此 Phase 2 不需要临时表或未来再迁移的影子 schema。

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
    "feature_snapshot_id": "<immutable id>",
    "feature_manifest_sha256": "<sha256>",
    "outcome_snapshot_id": "<immutable id>",
    "outcome_manifest_sha256": "<sha256>",
    "mode": "normal_postprocess"
  }
}
```

默认不存在该对象，不写 `enabled=false` 伪配置，不插入伪资源阶段。profile 内容从注册表生成，调用方不能覆盖 horizon、barrier、slice、entry 或 terminal 公式。

### 7.2 evaluator deployment bundle

AIstock 新增 `long_trend_evaluation_bundle.py`，bundle 保留可导入的 Python package 相对布局，只允许打包：

- `backend/__init__.py`
- `backend/services/__init__.py`
- `backend/services/quantevolver/__init__.py`
- `backend/services/quantevolver/long_trend_evaluation_contract.py`
- `backend/services/quantevolver/long_trend_data_reader.py`
- `backend/services/quantevolver/long_trend_evaluation.py`
- `backend/services/quantevolver/qe_dataset_contract.py`
- `backend/services/quantevolver/long_trend_worker_entry.py`
- `bundle_manifest.json`

manifest 保存每个文件相对路径、SHA-256、size、bundle schema、Python ABI 与 evaluator source SHA。RD-Agent 仅把这些 allowlist 文件写入 `evaluation_dir/runtime/`，拒绝绝对路径、`..`、符号链接、额外文件和 hash 不符。RD-Agent 将 `evaluation_dir/runtime` 作为唯一新增 `PYTHONPATH`，并固定执行 `python -m backend.services.quantevolver.long_trend_worker_entry`；request 不携带 shell command。这样远端执行的是已哈希的 AIstock 权威 Phase 1 源码，不依赖 WSL `/mnt/f/Dev/AIstock` 或远端机上某个可漂移 checkout。

### 7.3 identity

沿用父设计 `evaluation_id`。Phase 2 增加：

```text
job_id       = qelt_job_<sha256(evaluation_id + node_id)>
request_sha  = sha256(canonical request without secret/token)
attempt_id   = qelt_attempt_<evaluation_id hash>_<attempt_no>
bundle_sha   = sha256(canonical bundle manifest)
```

resource token、callback credential、本地绝对路径和 PID 不进入 evaluation identity。process identity 只进入 attempt receipt，用于 kill/reconcile fencing。

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
- `portfolio_analysis/indicators_normal_1day.pkl`
- catalog 明确标注且 schema 可识别的 order/trade evidence

不得猜测未出现在 complete catalog 的 order/trade 文件名。complete catalog 中缺失映射为 family-local missing；partial catalog 保存 `catalog_completeness=partial`、warnings 和尝试过的权威路径，不把“未列出”冒充“确定不存在”。

### 8.2 输入反序列化

- pickle 只在已通过 workspace containment、size/hash 和 allowlist 后由 QE worker 读取；
- prediction/label/position/report/indicator 分别执行 schema validator；
- 单项解析失败形成该族 reason 和冲突摘要，其他族继续；
- raw pickle 不复制进长期评价 CAS，只记录原始路径、size、sha256、catalog completeness 与 parser receipt；
- order/trade/position 数量或日期冲突时不得回退到日线 high/low 猜成交。

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
    process_identity.json
    secret.json            # 0600，不进入 catalog/receipt
    stdout.log
    status.json
    staging/
    artifacts/
    terminal_receipt.json
```

`request.json/job.json/status.json/terminal_receipt.json` 使用临时文件 + fsync + replace。成功 attempt immutable；失败 retry 新增 attempt，不删除旧日志和 receipt。

### 9.3 CPU 单槽与队列

- 每节点一个 `long_trend_eval` OS file lock；
- POST 原子写 queued job 后返回，不因槽位占用返回研究失败；
- 显式 submission、inspect 或 AIstock reconcile 可唤醒 node dispatcher；不注册周期 cron；
- dispatcher 按 `created_at/evaluation_id` FIFO claim；
- 外部 subprocess 与 RD API 生命周期解耦，API 重启不杀 worker；
- stale running 必须核对 PID、start time、command hash 和 evaluation identity，再决定仍运行、已终止或可重试；
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
4. 创建既有 resource session 并提交 node job；
5. inspect terminal status 后 streaming collect；
6. 原子发布 CAS 并返回 compact receipt。

Phase 2 不增加 public router。Phase 3 的 POST/GET API 直接调用这些方法，不再实现第二套编排。

## 11. Resource Phase and Recovery / 资源阶段与恢复

### 11.1 状态转换

扩展现有 transition：

```text
backtest -> long_trend_eval -> finalize
bootstrap -> long_trend_eval -> finalize        # historical long_trend_only
long_trend_eval -> finalize|completed|failed|cancelled
```

事件 metadata：`evaluation_id/job_id/attempt_id/node_id/cpu_seconds/rss_peak_bytes/read_bytes/output_rows/artifact_bytes/catalog_completeness`。资源数字是 worker 自身 receipt，不做系统级轮询，也不参与研究 go/stop。

### 11.2 认证与 outbox

- 复用 resource session token SHA、单调 sequence 和 source binding；
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

## 12. Dedicated QELongTrendArtifactStore / 专属 CAS

### 12.1 root 与 URI

- env：`QE_LONG_TREND_ARTIFACT_STORE_ROOT`
- 默认：`<repo_root>/rdagent_assets/long_trend_evaluation_store`
- URI：`aistock-qe-long-trend://evaluations/<evaluation_id>/<artifact_type>`
- 禁止 E: HDD；允许显式 F: SSD root；不得与 Prediction Store root 相同。

### 12.2 artifact allowlist

| artifact_type | schema |
|---|---|
| `compact_receipt` | `qe_long_trend_receipt_v1` JSON |
| `signal_observations` | `qe_long_trend_signal_observation_v1` Parquet |
| `holding_episodes` | `qe_long_trend_holding_episode_v1` Parquet |
| `execution_evidence` | `qe_long_trend_execution_evidence_v1` Parquet |
| `sector_metrics` | `qe_long_trend_sector_metric_v1` Parquet |
| `family_metrics` | `qe_long_trend_family_metric_v1` Parquet |
| `data_action_plan` | `qe_long_trend_data_action_v1` JSON |
| `worker_terminal_receipt` | `qe_long_trend_worker_terminal_v1` JSON |

每项保存 sha256、schema_sha256、size、row_count、columns/dtypes、evaluation/input/bundle identity、attempt 和 source node。未知 artifact type 拒绝，不变成通用文件仓库。

### 12.3 streaming 与原子发布

1. QE client 新增 async streaming download，逐块更新 SHA-256 并写 CAS `tmp/`；
2. size/hash/schema 与 node catalog 不一致时删除临时文件并返回结构化错误；
3. blob 路径按内容 hash，已存在 blob 必须 size/hash 一致；
4. 所有 required manifest items 验证完成后写 staging manifest；
5. fsync 文件与目录后原子 replace evaluation manifest；
6. manifest 成功后才把 CAS platform status 标为 published；
7. retry 只重传缺失 blob，不重新计算成功 family。

## 13. Compact Receipt Contract / 紧凑回执

`read_exp_res.py` 只挂载：

```json
{
  "schema_version": "qe_long_trend_receipt_v1",
  "evaluation_id": "qelt_...",
  "profile_id": "qe_long_trend_v1",
  "evaluation_asof": "YYYY-MM-DD",
  "task_status": "succeeded|partial|failed|cancelled",
  "family_status": {},
  "platform_delivery_status": {},
  "headline_metrics": {},
  "maturity_summary": {},
  "execution_coverage": {},
  "data_action_summary": {},
  "artifact_manifest_uri": "aistock-qe-long-trend://...",
  "artifact_manifest_sha256": "...",
  "worker_terminal_sha256": "..."
}
```

headline 只含父设计注册的标量；所有 dict 有大小上限，禁止内联逐股票、逐日、逐 episode、日志或秘密。receipt 缺失时 `read_exp_res.py` 不创建空成功对象。

## 14. Failure Semantics / 失败语义

新增/沿用 reason：

| reason_code | 语义 |
|---|---|
| `QELT_BUNDLE_INVALID` | bundle 文件、hash、allowlist 或 ABI 不一致 |
| `QELT_RECORDER_REF_MISSING` | 无权威 recorder ref；依赖 Recorder 的族局部受限 |
| `QELT_WORKSPACE_CATALOG_PARTIAL` | catalog 部分可见；保存限制与尝试路径 |
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
- `backend/services/quantevolver/long_trend_evaluation_phase2.py`
- `backend/services/quantevolver/long_trend_worker_entry.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py`
- `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`

### 15.2 AIstock 修改

- `experiment_config.py`：opt-in profile descriptor；默认无字段
- `config_composer.py`：冻结 bundle/profile/snapshot descriptor并为 normal Loop 透传 postprocess job
- `qe_workspace_client.py`：typed submit/inspect/catalog/stream/cancel methods
- `qe_active_execution_capacity.py`：透传 postprocess descriptor，不改变 GPU reservation
- `qe_resource_phase_service.py`：加入 `long_trend_eval` transition 和 CPU metadata validation
- `results_only_retry.py`：抽取共享 recorder ref/path resolver；保持现有 results-only 语义
- `templates/read_exp_res.py`：只挂载 compact receipt
- CI ownership catalog：登记新增 QE backend 与跨仓库测试路径

### 15.3 RD-Agent 新增

- `rdagent/app/api_endpoints/qe_long_trend_evaluation.py`
- `rdagent/app/api_endpoints/qe_long_trend_worker.py`
- `test/app/test_qe_long_trend_evaluation_api.py`
- `test/app/test_qe_long_trend_worker_recovery.py`

### 15.4 RD-Agent 修改

- `qe_evolution_api.py`：include loop-owned evaluation router / optional postprocess descriptor
- `qe_workspace_catalog.py`：evaluation 子目录 artifact catalog，secret 排除
- `qe_dataset_identity.py`：显式 registered root/snapshot identity 对齐，不扫描未授权目录

Phase 2 不修改 frontend、MCP、migration、`backend/main.py` startup、Selection/Advisory/Paper/模拟盘/QMT/StrategyPackage 或通用 Prediction Store。

## 16. Implementation Sequence / 开发顺序

1. **P2-A bundle + identity**：bundle builder、allowlist、hash、opt-in config 与 normal/historical parity fixture；
2. **P2-B resolver**：Recorder/catalog/snapshot resolver，family-local input inventory；
3. **P2-C node job**：typed API、spool、CPU 单槽、external subprocess、status/cancel/recovery；
4. **P2-D CAS**：streaming collect、schema validation、atomic publish、conflict/retry；
5. **P2-E resource + compact receipt**：resource phase/outbox、read_exp_res adapter、normal/historical integration；
6. **P2-F cross-repo canary**：一个 deterministic fixture 与一个已完成 Loop 的显式 `long_trend_only` canary，不启动训练。

这些是工程实施顺序，不是研究门禁。P2-A～P2-E 可在文件所有权不冲突时并行开发；缺失真实 artifact 时使用完整 fixture 验证 wiring，同时记录真实数据补取任务，不能用 fixture 冒充真实 canary。

## 17. Verification Plan / 验证计划

### 17.1 单元与合同

- bundle path/hash/ABI/额外文件拒绝；
- evaluation identity、typed null、normal/historical parity；
- complete/partial catalog、exact recorder path、missing optional family；
- snapshot same/verified-extension/mismatch/missing identity；
- artifact allowlist、stream interruption、hash/schema mismatch、atomic manifest、duplicate conflict；
- resource transition、sequence replay/conflict、secret 不出现在日志/manifest；
- cancel process fencing、stale PID、already-terminal；
- compact receipt 大小与字段 allowlist。

### 17.2 集成

1. RD API submit → queued → running → terminal；
2. normal postprocess 与历史 `long_trend_only` 对同输入产生相同 family metrics 和 artifact hash；
3. AIstock 重启后 inspect/collect，不产生第二 worker；
4. RD API 重启后从 job/attempt manifest 恢复；
5. CAS publish 中断后只重传；
6. node 暂不可达返回 unknown，不假失败；
7. 真实已完成 Loop canary：不训练、不回测，只生成 F-014 receipt/CAS；
8. H5/Parquet 内存 receipt 证明 chunk 释放，内存不随 Loop 数累积。

### 17.3 零影响审计

- import allowlist 不出现非 QE module；
- route diff 只在 RD `/qe_workspace/**`，AIstock Phase 2 无 public route；
- DB schema diff 为零；
- Prediction Store manifest/hash 回归不变；
- 普通未启用 profile 的 Loop request、runner、phase sequence 与 enhanced metrics 不变；
- 不出现 GPU telemetry/NVML/`nvidia-smi`；
- DESIGN-COMPLIANCE-001 四项逐条复核。

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-014 | parent Phase 2 scope linkage | `backend/tests/scripts/test_aistock_feature_workflow.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_long_trend_evaluation_phase2_compute_cas_f2_design_20260722.md --tier F2` | DESIGN_READY | none |
| F-301 | `backend/services/quantevolver/long_trend_evaluation_phase2.py` QE identity validation | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py` | DESIGN_READY | none |
| F-302 | shared bundle/resolver/engine/store | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py` | DESIGN_READY | none |
| F-303 | bundle builder + RD allowlist extractor | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_long_trend_evaluation_api.py` | DESIGN_READY | none |
| F-304 | shared recorder/catalog resolver | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py` | DESIGN_READY | none |
| F-305 | dataset identity + strict reader | `backend/tests/unified_engine/test_qe_long_trend_phase2_bundle_resolver.py`; existing `backend/tests/unified_engine/test_qe_long_trend_contract_reader.py` | DESIGN_READY | none |
| F-306 | QE worker + streaming client | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_long_trend_worker_recovery.py` | DESIGN_READY | none |
| F-307 | Phase 1 family-local engine wrapper | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; existing `backend/tests/unified_engine/test_qe_long_trend_evaluation_core.py` | DESIGN_READY | none |
| F-308 | RD job/attempt manifests | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_evaluation_api.py` | DESIGN_READY | none |
| F-309 | RD FIFO CPU slot | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py` | DESIGN_READY | none |
| F-310 | resource phase transition/outbox | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py` | DESIGN_READY | none |
| F-311 | inspect/reconcile/collect | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_long_trend_worker_recovery.py` | DESIGN_READY | none |
| F-312 | dedicated long-trend CAS | `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py` | DESIGN_READY | none |
| F-313 | immutable success manifest | `backend/tests/unified_engine/test_qe_long_trend_phase2_artifact_store.py` | DESIGN_READY | none |
| F-314 | compact receipt adapter | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py` | DESIGN_READY | none |
| F-315 | worker terminal receipt | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_long_trend_evaluation_api.py` | DESIGN_READY | none |
| F-316 | typed cancel | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; RD `test/app/test_qe_long_trend_worker_recovery.py` | DESIGN_READY | none |
| F-317 | cross-repo test matrix | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; `F:/Dev/RD-Agent-main/test/app/test_qe_long_trend_worker_recovery.py`; real canary receipt | DESIGN_READY | none |
| F-318 | ownership/import/route/schema diff | `backend/tests/unified_engine/test_qe_long_trend_phase2_orchestration.py`; CI ownership receipt | DESIGN_READY | none |

## 19. Rollout and Rollback / 发布与回滚

### 19.1 Rollout

1. AIstock 与 RD-Agent 分别使用独立 feature branch/PR；
2. 先合并节点 API/worker，再合并 AIstock typed client/orchestrator；未启用 profile 时均无运行变化；
3. 本阶段无 DDL和依赖；代码合入后由用户授权并执行 AIstock backend、WSL/远端 QE API 重启；
4. 重启后先跑 deterministic fixture，再显式选择一个已完成 Loop 执行 `long_trend_only` canary；
5. canary 只产生 QE Phase 2 job、resource event 和专属 CAS，不写三张评价表。

### 19.2 Rollback

- profile 默认关闭；停止提交新 evaluation job 即停止新增计算；
- 已运行 node worker 不因 AIstock rollback 被强杀，先 inspect/collect/cancel typed attempt；
- CAS 内容按 hash immutable，rollback 不删除历史 receipt；
- 回滚 AIstock/RD code 不修改训练、回测或 Archive 既有结果；
- 不执行数据库回滚，因为 Phase 2 无 schema 变化。

## 20. Risks and Mitigations / 风险与应对

| 风险 | 影响 | 应对 |
|---|---|---|
| 历史 Recorder catalog 不完整 | 某些 family 无法解析 | 保存 partial catalog、尝试路径和 data action；其他 family 继续 |
| remote job 与 backend 状态短暂分离 | 重复启动或错误终态 | job/attempt/process identity + inspect/reconcile，不可达标记 unknown |
| H5/Parquet 内存峰值 | 节点卡顿或 OOM | 单 CPU 槽、signal-date chunk、streaming collect、父对象及时释放 |
| CAS 中途失败 | 结果已算但未发布 | 保留 terminal receipt，只重传缺失 blob，不重算成功 family |
| evaluator bundle 漂移 | 同 identity 结果不可复算 | source/bundle/file hash 进入 identity，冲突 fail-fast |
| order/trade/position 证据不足 | execution cause 无法判断 | 明确 `NOT_VERIFIABLE`，不以日线猜测，不影响 signal/sector/portfolio 已有证据 |
| CPU 评价影响 GPU 训练 | 桌面或节点资源争用 | 默认单槽、队列、只记录自身资源；不抢占、不终止、不提高图模型并发 |
| 跨仓库版本错配 | API/worker 合同错误 | RD 先合入，AIstock typed client 后合入，双方 schema/hash contract test |

## 21. Production Gates / 实施事实（不定义科研门禁）

| 项目 | 状态 |
|---|---|
| design | `DESIGN_READY_CODE_NOT_STARTED` |
| Phase 1 core | `MERGED_VERIFIED` |
| AIstock Phase 2 source | `NOT_STARTED` |
| RD-Agent Phase 2 source | `NOT_STARTED` |
| production_ddl_gate | `noop` |
| production_frontend_dependency_gate | `noop` |
| production_backend_dependency_gate | `noop` |
| runtime | `UNTOUCHED_BY_DESIGN` |
| DB/data/experiments | `UNTOUCHED_BY_DESIGN` |
| research gates/approvals | `NONE_ADDED` |

## 22. DESIGN-COMPLIANCE-001 Review / 设计符合性复核

- [x] **禁止简化交付**：Phase 2 同时覆盖 normal/historical 两入口、真实 resolver、节点 job、资源恢复、专属 CAS、compact receipt、cancel 和跨仓库测试；fixture 不能冒充真实 canary，Phase 2 也不冒充 F-014 全平台完成。
- [x] **禁止静默错误**：所有 identity、catalog、snapshot、parser、worker、stream、CAS 和 resource 异常都有结构化状态/reason；禁止空 dict、0 填补、当前数据替换指定 snapshot及 `except: pass`。
- [x] **禁止业务逻辑偏移**：Phase 1 profile、公式、family status 和 evidence authority 原样复用；不改变训练、标签、因子、回测、策略、成本、TopK 或执行语义。
- [x] **禁止私增门禁审批**：唯一硬边界是 QE-only 零影响；数据、制品、成熟度、显著性和平台状态只作为科研证据与补取计划，不形成研究许可、淘汰或方向停止状态。
