# HMM 演进系统 Phase 1 离线评估实验室实现级详细设计

> **版本**：v1.7
> **日期**：2026-07-17
> **状态**：P1-A 已验收；P1-B 已实现；P1-C API/UI/worker 源码及 BUG-742～BUG-748 审计硬化已实现；真实 QE 10-case、性能、实机截图与 runtime activation 仍待外部验收
> **设计权威**：总体蓝图 `hmm_evolution_and_risk_management_system_design_20260716.md` v1.8
> **上游运行契约**：`hmm_evolution_phase0_data_source_detailed_design_20260716.md` v2.2
> **隔离约束**：`HMM_EVOLUTION_ISOLATION_CONSTRAINTS.md` v2.1
> **Feature tier**：F2
> **Design Acceptance Index**：复用总体蓝图 `F-006`～`F-010`

本文是总体蓝图 Phase 1 的从属实现设计，不建立第二套架构。总体蓝图定义阶段边界与
跨阶段约束；本文定义 Phase 1 的数据身份、schema、状态机、离线指标、推荐公式、API、
UI、失败语义、验证和发布门禁。两者冲突时，先修订总体蓝图，再实现代码。

---

## 1. Background（背景与现状）

Phase 0 已完成 QE Prediction Store 零副本读取、可信 workspace fallback、canonical
market 只读访问、显式 candidate identity、缓存安全和专用 CI。受控外部验收使用
`qe_20260706_013235_bbd4/Loop8`，`pred.pkl` / `label.pkl` 各 2,260,161 行；该 loop
的标签 horizon 为 20 个交易日，因此只能作为 `label_horizon_days=20` 的证据，不能被
命名为 `label_10d`。

`scripts/diagnostics/hmm_offline_diagnostic.py::compute_replacements` 最初证明了可行的离线
诊断路径：将 raw score 与 HMM coefficient 调整后的 score 分别排序，比较 TopK
entered/dropped 集合，并使用 label 或 market forward return 衡量替换质量。P1-B 已将该脚本
迁到唯一 pure evaluator：pred/label 复用 Phase 0 BacktestDataSource 缓存，行情收益通过 canonical
只读 repository 获取，label horizon 显式保留，QE config 不下载，意外转换/合同错误 fail loud。
该脚本仍只是研究诊断入口，不是第二套 evaluator 或 API service。

Phase 1 要把其中的纯计算语义抽取成可重放、可批量执行、可取消、可审计的研究服务，
并允许研究人员只读查看 QE 实验的全部资产以及数据库中的最新共同完成行情水位，同时保持
以下边界：

- pred/label 继续消费 Phase 0 标准化输入；其它 QE 实验资产通过独立只读 reader 访问；
- 只写 `hmm_evolution.*`；
- top-3 仅为研究推荐，最终有效性由 QE 终审；
- 不生成、替换或修改任何生产 HMM、QE、Selection、Paper、QMT 状态。

## 2. Scope（批准范围）

Phase 1 v1 包含：

1. 注册和审计预计算 HMM coefficient 候选；
2. 通过只读 asset catalog/reader 查看 QE task/loop 的全部实验资产；
3. 在固定 QE loop、日期窗口、universe、TopK 和显式 label horizon 上执行离线评估；
4. 显式读取 canonical DB 的最新共同完成行情水位或指定 as-of，计算 10 个交易日 forward return；
5. 批量评估 10+ 候选，复用同一份 pred/label 输入；
6. 持久化候选、评估、批次、状态、heartbeat、错误和推荐证据；
7. 提供真实 API、中文 UI、HMM 研究工作台导航、固定证据区和独立详情页；
8. 提供单评估、批评估、失败重试、取消和结果复用语义；
9. 通过独立 Python bootstrap 创建 `hmm_evolution.*`，代码合入不等于生产 DDL 已执行。

## 3. Non-goals（非目标与硬边界）

- 不训练 HMM；训练与滚动训练属于 Phase 3。
- 不自动生成缺失 coefficient artifact；缺失即结构化失败。
- 不修改、删除、清理、终止或重跑 QE task/loop；只读资产接口不得调用 QE client 的
  create/kill/cleanup 类方法。
- QE 配置、日志、模型参数、报告和其它资产可以只读查看与取证，但不得自动执行、导入为生产
  配置或绕过 parser/trust contract 直接参与计算。
- 不修改 Phase 0 已验收的 pred/label 自动反序列化白名单；Phase 1 的全资产 reader 是独立
  read-only evidence contract。
- 不修改 `model_train_configs`、`model_train_snapshots`、`strategy_packages`、
  `paper_v2.*`、Selection、Advisory、MiniQMT 或 QMT。
- 不调用 `selection_center/hmm_risk_gate.py`；该模块输出 `can_buy=False`，不是纯分析契约。
- 不自动提交 QE、不自动淘汰候选、不自动替换生产 snapshot。
- 不把 top-3、coverage、历史一致性或推荐分数升级为研究停止条件或生产门禁。
- 不使用 FastAPI `BackgroundTasks` 伪装 durable worker；进程重启后任务状态必须可恢复。
- 不启用生产 worker、生产 schema、8001/3000/19080 或任何 scheduler。
- 不新增裸 `.sql` 文件，不在业务 service 内隐式建表。

## 4. DESIGN-COMPLIANCE-001 控制

| 控制 | Phase 1 设计要求 |
|---|---|
| `no_simplified_delivery` | schema、repository、状态机、纯 evaluator、scorer、API、UI 和验证证据必须按 F-006～F-010 逐项完成后才可报告 Phase 1 完成。 |
| `no_silent_error` | 缺 artifact、hash 不符、horizon 不符、无共同日期、DB 数据不足、lease 丢失、取消、图表加载失败和 polling 超时均返回稳定 reason code，不返回中性成功、永久 loading 或空白。 |
| `no_business_semantic_drift` | 结果仅为研究分析；不产生 `RiskDecision`、`can_buy`、订单、配置变更或生产 snapshot 变更。 |
| `no_unrequested_gate_or_approval` | 推荐公式不包含淘汰阈值；所有成功候选均展示，证据不足者标记为未排名，不宣告方向无效。 |

### 4.1 禁止简化版交付

- 禁止把只建 schema、只写 repository、只做 backend、只画静态页面或只跑 mock 视为 Phase 1 完成。
- 禁止使用 POC、placeholder scorer、硬编码 top-3、静态成功响应或临时 JSON 代替真实 evaluator/worker/API/UI。
- 禁止省略 QE 全资产只读 reader、latest-common market watermark、durable 状态机、结构化错误或
  Design Acceptance Matrix 中任一承诺项。
- 分阶段 PR 可以只交付 P1-A/P1-B/P1-C 中明确的一段，但只能报告该设计子集完成，不得提前
  宣称 Phase 1 整体完成。

### 4.2 门禁与审批最小化

- 只有生产 DDL 和首次生产 runtime activation 属于基础设施变更，需要明确操作授权。
- candidate 注册、QE 资产只读查看、离线评估、批处理、重试、取消和 top-3 查看不新增审批流。
- 并发上限、路径 containment、manifest 校验、lease/fencing、完整性校验属于技术安全约束，
  不是研究方向门禁。
- runtime flag 是部署配置和 kill switch，不得实现成多层产品审批或人工签字链。
- coverage、推荐分数、历史一致性和数据新鲜度只作为证据展示，不设置未经设计批准的通过阈值。

### 4.3 本轮设计审核结论

| 审核项 | 原设计风险 | v1.5 处置 |
|---|---|---|
| QE 资产读取 | 只覆盖 pred/label 与两类 coefficient，低于研究所需只读范围 | 增加全资产 reader；inspection 范围与 computational trust 分离；真实 node complete catalog 为 F-006 验收 |
| 最新行情 | 只有显式 date 字段，容易被实现成 `date.today()` 动态漂移 | 增加 `latest_common_completed`，入队解析并固化 watermark/PIT coverage |
| 过度门禁 | strict-full 默认、approved-local 命名和多段 activation 容易演化成审批链 | 默认共同日期证据模式；configured source；只保留 DDL/首次 activation 操作授权 |
| 静默错误 | neutral fallback、缺失指标重加权或图表加载失败可能只在日志/context 内可见 | 强制 evidence_quality/warnings、固定错误区、reason code 和 terminal state；未知异常 fail，不允许空集合假成功 |
| 简化交付 | 原表有原则但缺少具体禁止清单 | 明确禁止 schema/backend/mock/static/placeholder/POC 冒充 Phase 1 完成 |
| Phase 0 对齐 | 扩大 QE 读取可能误改 Phase 0 whitelist | 全资产 reader 独立实现；Phase 0 pred/label 信任、cache、zero-copy 契约保持不变 |
| UI 业务语义 | “热力图/热度”可能被误解为交易吸引力，三阶段 tab 可能被静态占位 | 状态、置信度、severity 分离；Phase 1 只激活真实演进页，风险/训练页按后续验收真实注册 |
| Legacy UI 泄漏 | 直接复用 `/paper-v2/model-hmm` 会带入生产写入语义和 `pv2-*` 视觉 | 新建 HMM research shell；禁止 Paper v2 组件、抽屉式列表和 raw JSON 主视图 |

### 4.4 2026-07-18 全面设计合规复核

| P0 控制 | 设计级结论 | 证据与剩余边界 |
|---|---|---|
| `no_simplified_delivery` | PASS | F-006～F-010 仍逐项验收；P1-C 必须包含真实 API、worker CLI、完整 UI 状态、10-case 与性能证据；演示 HTML、静态页面和未实现 tab 明确不算完成。 |
| `no_silent_error` | PASS | API/repository、polling、component/renderer、empty/degraded/stale/timeout 均有显式状态与 reason code；禁止 console-only、raw context-only、永久 loading 和空集合假成功。 |
| `no_business_semantic_drift` | PASS | top-3 仍为 QE 终审前研究推荐；horizon、candidate、watermark 和 evidence identity 保持；不调用 risk gate、不写生产 snapshot、不产生 can_buy/订单。 |
| `no_unrequested_gate_or_approval` | PASS | 仅生产 DDL 与首次 runtime activation 需要操作授权；候选、评估、批处理、重试、取消、证据查看不新增审批链或淘汰阈值。 |

本结论只表示 v1.5 **设计文本**已消除已知 P0 缺口，不表示 F-010 实现完成。F-010 在真实代码、
API/UI、E2E、benchmark 和外部证据回填前继续保持 `approved_by_user_for_implementation`。

## 5. Architecture（架构）

```text
frontend /hmm-evolution
        │
        ▼
backend/routers/hmm_evolution.py
        │ request/response + structured error mapping
        ▼
HMMEvolutionService
   ├── CandidateRegistryService
   ├── BatchOrchestrator
   ├── HMMOfflineEvaluator              # pure calculation boundary
   ├── HMMRecommendationScorer          # batch-relative, versioned
   └── HMMEvolutionRepository           # only hmm_evolution.* writes
        │
        ├── BacktestDataSource           # Phase 0 pred/label, read-only
        ├── QEExperimentAssetReader       # all QE assets, read-only evidence
        ├── HMMMarketReturnRepository    # canonical market SELECT only
        ├── CandidateArtifactResolver    # precomputed coefficient, no generation
        └── PostgreSQL hmm_evolution.*

operator-started HMMEvolutionWorker
        └── claim batch/evaluation with lease + fencing + heartbeat
```

### 5.1 进程边界

- API 只登记请求、读取结果和设置 cancel flag；不在请求线程执行 10 分钟计算。
- Worker 是独立、人工启用的进程，不是定时 scheduler。默认不随 backend startup 启动。
- Repository 使用仓库同步 `get_conn()`；异步 API 通过线程执行器调用同步 repository，禁止
  `async with get_conn()`。
- Worker 与 API 共享 Pydantic contract 和 repository，不共享内存状态作为权威状态。

### 5.2 写入 allowlist

Phase 1 代码允许的 DB DML 目标固定为：

- `hmm_evolution.candidate`
- `hmm_evolution.offline_evaluation`
- `hmm_evolution.batch_test_run`
- `hmm_evolution.batch_test_item`

任何其它 schema/table 写入均 fail closed。market/QE 访问只允许 SELECT。

### 5.3 QE 实验全资产只读边界

`QEExperimentAssetReader` 复用现有 `QEWorkspaceClient.get_workspace_file()`、
`download_workspace_file_bytes()` 和 Prediction Store / QE archive manifest 查询能力，但只暴露
list/read/stat，不暴露 create/run/kill/cleanup/delete。

当前 AIstock `QEWorkspaceClient` 只有“已知相对路径读取”，没有完整 workspace list/stat client
binding。P1-A 必须补齐只读 `list_workspace_files()` / `stat_workspace_file()` 契约；若远端节点尚无
对应 endpoint，则需在 QE node 侧同步增加安全、非递归穿透的只读 manifest/list API。只聚合
Prediction Store manifest 或猜测常见文件名时必须标记 `catalog_completeness=partial`，不得宣称
“全部资产已可见”。F-006 只有在真实 node 上验证 `catalog_completeness=complete` 后才算实现。

读取范围包括但不限于：pred/label、HMM coefficient、模型参数、metrics、portfolio report、
日志、文本/JSON/YAML 配置、recorder metadata 和其它 task/loop 产物。读取范围宽，计算信任
范围仍必须显式：

- `inspection_only`：任何安全相对路径资产可只读查看、hash 和作为证据引用，不自动反序列化或执行；
- `trusted_computational_input`：只有具备 SHA/size/content type/schema/parser receipt 的已声明资产
  才能进入 evaluator；
- Prediction Store 命中优先零副本；缺失时可读 workspace；已存在但损坏的 manifest/blob 不得
  以 workspace fallback 掩盖；
- workspace 资产没有可信 manifest 时仍可 inspection-only 读取，但必须标记
  `trust_level=unverified_evidence`，不得进入评分计算；
- 读取 bytes 默认在内存或受控临时流中完成，不为每个实验固化额外永久副本；
- 所有访问记录 task/loop/path/source/SHA/size/trust level，API 不返回 token、密码或本机绝对路径。
- 资产目录枚举必须通过 node API，Windows backend 不直接扫描、挂载或复制 QE worker filesystem。

## 6. Contracts：候选身份与 artifact 契约

### 6.1 候选来源

Phase 1 v1 支持三类 coefficient 来源：

1. `existing_snapshot_coefficients`
   - 请求提供 `snapshot_id` 和明确 `coefficient_artifact_name`；
   - 只读查询 snapshot 的 `status/model_path/config_id`；
   - snapshot 必须处于 completed/ready 类状态；
   - 只读取已存在 coefficient JSON，不调用生成接口，不读取或复制训练配置。
2. `configured_local_coefficients`
   - 请求提供 `root_alias` 和规范化相对路径；
   - `root_alias` 来自部署配置 `HMM_EVOLUTION_ARTIFACT_ROOTS_JSON`；
   - 拒绝绝对路径、`..`、反斜杠逃逸、symlink/reparse 穿透和未配置 root。
3. `qe_experiment_coefficients`
   - 请求提供 task/loop 和规范化 asset path；
   - 通过 `QEExperimentAssetReader` 只读获取；
   - 只有 trusted manifest 与 coefficient parser contract 均通过时才可登记为 candidate；
   - inspection-only/unverified asset 可以查看，但不能进入 evaluator。

上述扩展不改变 Phase 0 的 pred/label whitelist；它是 Phase 1 单独的只读 asset contract。

### 6.2 coefficient payload 最低契约

Artifact 必须是 UTF-8 JSON object，并至少包含：

```json
{
  "daily_coefficients": {
    "2026-01-05": {"801010.SI": 1.0}
  },
  "stock_sector_map": {
    "000001.SZ": "801780.SI"
  }
}
```

校验要求：

- 日期 key 必须是合法 ISO date 且严格递增规范化；
- sector code 和 symbol 为非空字符串；
- coefficient 必须为 finite 且大于 0；
- 至少一个日期、一个 sector 和一个 symbol mapping；
- 所有统计只从已验证 payload 推导；
- legacy artifact 没有 schema version 时登记为
  `detected_format=hmm_sector_coefficients_legacy_v1`，不得伪造原始字段。

### 6.3 candidate manifest

持久化 `artifact_manifest` 使用 `hmm_candidate_manifest_v1`：

```json
{
  "schema_version": "hmm_candidate_manifest_v1",
  "artifact_type": "hmm_sector_coefficients",
  "source_type": "existing_snapshot_coefficients",
  "source_ref": {
    "snapshot_id": "...",
    "config_id": "...",
    "artifact_name": "coefficients_preset_A_....json"
  },
  "artifact_uri": "snapshot://<snapshot_id>/<artifact_name>",
  "artifact_sha256": "<64 hex>",
  "size_bytes": 0,
  "detected_format": "hmm_sector_coefficients_legacy_v1",
  "coverage": {
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "date_count": 0,
    "sector_count_min": 0,
    "sector_count_max": 0,
    "stock_sector_map_count": 0
  },
  "coefficient_stats": {"min": 0.0, "max": 0.0},
  "algorithm_version": "score_times_sector_coefficient_v1"
}
```

API 默认不返回本机绝对路径，只返回 stable URI、hash 和统计。

### 6.4 candidate ID 与幂等

候选身份不包含 display name、描述和 lifecycle：

```text
identity_payload = {
  artifact_sha256,
  artifact_type,
  detected_format,
  algorithm_version
}
manifest_hash = sha256(canonical_json(full_manifest))
candidate_id = "hmmc_" + sha256(canonical_json(identity_payload))[0:24]
```

相同内容从不同合法来源登记时返回同一 candidate，并保留首次来源为权威 provenance；新来源
只可追加到 `source_ref.aliases`，不能改写 artifact hash。display name 可修改，但不改变 ID。

### 6.5 lifecycle

`research_only -> retired`；artifact 后续 hash/路径失配时可进入 `invalid`。

- `retired`/`invalid` 候选仍可读取历史结果；
- 默认新批次拒绝使用 `retired`/`invalid`，但这只是输入状态校验，不宣告研究方向无效；
- 不物理删除 candidate 或历史 evaluation。

## 7. Contracts：evaluation spec、source manifest 与 hash

### 7.1 `hmm_evaluation_spec_v1`

```json
{
  "schema_version": "hmm_evaluation_spec_v1",
  "base_loop_ref": "<task_id>/LoopN",
  "window_start": "YYYY-MM-DD",
  "window_end": "YYYY-MM-DD",
  "as_of": {"policy": "latest_common_completed", "requested_date": null},
  "label_horizon_days": 20,
  "universe": {"type": "prediction_artifact_all"},
  "topk": 50,
  "date_coverage_policy": "batch_common_intersection_with_evidence",
  "missing_sector_policy": "neutral_with_evidence",
  "market_forward_return": {
    "mode": "required",
    "horizon_trading_days": 10
  },
  "sort_policy": "score_desc_symbol_asc_v1",
  "metric_version": "hmm_replacement_metrics_v1",
  "recommendation_version": "hmm_recommendation_v1"
}
```

v1 universe 只支持 `prediction_artifact_all`。后续自定义股票池必须记录有序 symbol list hash，
不得只记录显示名称。`as_of.policy` 支持 `explicit` 和 `latest_common_completed`；后者在请求入队
时解析一次并冻结为 `resolved_as_of_date`，不得在 worker 执行时再次漂移。

### 7.2 label horizon

- 请求的 `label_horizon_days` 必须与 Phase 0 归一化 label 中唯一 horizon 完全一致；
- label 出现多个 horizon 或与请求不符时失败；
- DB 和 API 的通用字段名为 `net_label_return`，同时保存 `label_horizon_days`；
- 仅当 horizon=10 时，UI/导出才可显示别名 `net_label_10d`；
- h20 数据显示为“净标签收益（20 交易日）”，不得误标为 10 日。

### 7.3 source manifest

`hmm_evaluation_source_manifest_v1` 必须包含：

- pred/label 与其它被引用 QE assets 的 source、URI、SHA256、size、row count（如适用）、
  trust level、zero-copy/fallback 决策；
- base loop、task、loop、实际日期范围和 label horizon；
- 排序后的 universe hash、symbol count；
- candidate ID、candidate manifest hash、artifact SHA；
- 启用 DB 10 日收益时的 requested policy、resolved as-of、各数据集 max date、共同完成水位、
  calendar range、price row count、字段名
  `market.kline_daily_raw.close_li` 和只读 transaction receipt；
- `warnings`、`evidence_quality` 和所有 neutral/missing/degraded 计数；
- 不包含密码、token、原始绝对路径或配置文件正文；配置正文只通过受控 asset content API 展示。

### 7.4 canonical hash

所有 hash 使用 UTF-8 canonical JSON：key 排序、无多余空白、日期 ISO、enum 固定小写、
禁止 NaN/Infinity。

```text
evaluation_spec_hash = sha256(canonical_json(evaluation_spec))
source_manifest_hash = sha256(canonical_json(source_manifest))
logical_evaluation_key = sha256({
  candidate_manifest_hash,
  source_manifest_hash,
  evaluation_spec_hash,
  evaluator_version
})
input_hash = logical_evaluation_key
```

结果另保存 `result_hash`，对有限浮点使用 12 位有效数字的十进制定长字符串后再 canonicalize。

## 8. Contracts：离线 evaluator

### 8.1 唯一计算路径

新模块 `backend/services/hmm_evolution/evaluator.py` 抽取
`compute_replacements` 的纯计算语义。诊断脚本后续改为调用该模块，不保留第二套实现。
Phase 1 service 不复制诊断脚本的 DB 连接、下载、文件写入或报告生成代码。

### 8.2 排序与 TopK

对每个交易日：

1. 取 finite raw score；
2. 通过 `stock_sector_map` 找 sector；
3. 通过当日 `daily_coefficients` 找 coefficient；
4. `adjusted_score = raw_score * coefficient`；
5. raw 和 adjusted 均按 `(score DESC, symbol ASC)` 排序；
6. `entered = adjusted_topk - raw_topk`，`dropped = raw_topk - adjusted_topk`；
7. entered/dropped symbol 均按字典序输出，保证跨平台可重放。

原脚本的 `mergesort` 只保证输入顺序稳定，不能消除 artifact 输入顺序差异；v1 使用 symbol
作为显式 tie-break。非并列 fixture 必须与旧诊断结果完全一致，并列 fixture 以本文为权威。

### 8.3 缺失 sector/coefficient

`missing_sector_policy=neutral_with_evidence` 时 coefficient 使用 1.0，但必须记录：

- missing sector symbol count/ratio；
- missing coefficient sector count/ratio；
- neutral fallback replacement count。

任何 neutral fallback 都必须使 `evidence_quality=degraded`，写入结构化 `warnings` 并在 API/UI
主视图可见，不能只藏在调试 JSON。若整个窗口 sector mapping 为空或 coefficient 与 prediction
无任何共同日期，评估失败，不能返回中性成功。

`batch_common_intersection_with_evidence` 先计算同批候选、pred 和 label 的共同日期集合，再用
同一集合评估全部候选；丢弃日期、原始覆盖率和共同覆盖率全部记录。`strict_full` 作为调用方
显式选择的复现模式保留，但不是默认研究门禁。

### 8.4 指标定义

逐日先计算：

```text
daily_net_label = mean(entered valid label) - mean(dropped valid label)
daily_net_db_10d = mean(entered valid db_ret_10d) - mean(dropped valid db_ret_10d)
```

只有 entered 与 dropped 两侧都至少一个有效值时，该日才是 comparable day。聚合指标：

- `net_label_return`：所有 label comparable days 的 `daily_net_label` 等权均值；
- `net_db_10d`：所有 DB comparable days 的 `daily_net_db_10d` 等权均值；
- `positive_net_label_day_ratio`：label comparable days 中 `daily_net_label > 0` 的比例；
- `replacement_count`：entered + dropped 行数总和；
- `changed_day_count`：replacement_count > 0 的日期数；
- `label_comparable_day_count` / `db_comparable_day_count`；
- `label_day_coverage_ratio = label_comparable_day_count / changed_day_count`；
- `db_day_coverage_ratio = db_comparable_day_count / changed_day_count`；
- `primary_coverage_ratio`：推荐公式按 label coverage 使用；
- sector fallback、coefficient min/max、daily replacement summary。
- `evidence_quality=complete|degraded|insufficient` 和结构化 `warnings`。

若 `changed_day_count=0`，评估可以 `succeeded`，但 efficacy 指标为 null、推荐分数为 null，
UI 显示“该候选在此窗口未改变 TopK”，不得显示为收益 0 或通过门禁。

### 8.5 DB 10 日 forward return

- 仅使用 `market.trading_calendar` 的交易日序列和
  `market.kline_daily_raw(ts_code, trade_date, close_li)`；
- `T+10` 是同一 symbol 的第 10 个后续交易日，不是自然日；
- 查询必须批量执行，禁止逐 symbol/date round trip；
- `mode=required` 时 DB 不可用或覆盖不足导致零 comparable day，评估失败；
- `mode=disabled` 时不查询 DB，`net_db_10d=null`，source manifest 明确记录 disabled；
- 不提供 silent best-effort 模式。

最新行情读取使用 `latest_common_completed`：分别解析 `market.trading_calendar`、
`market.kline_daily_raw` 以及本次显式启用的其它日频数据集最大完成日期，选择共同完成水位；
`market.sw_index_member` 按该日期验证 PIT 映射覆盖率，不伪造日频 max date。该日期在请求入队
时固化为 `resolved_as_of_date` 并进入 input hash。禁止直接使用
`date.today()`、`CURRENT_DATE` 或 worker 开始执行时的动态“最新”。Phase 1 v1 读取日线最新
共同完成数据，不把盘中未完成 bar 当成完整日线。

### 8.6 结果体积

DB 保存聚合指标、逐日摘要和最多 100 条按
`abs(adjusted_rank - raw_rank) DESC, date ASC, symbol ASC` 选取的 deterministic sample。
不持久化全部 replacement rows；完整结果可由 manifest 重放，避免把数百万行分析明细塞入 DB。

## 9. Contracts：推荐公式 `hmm_recommendation_v1`

推荐只在同一 batch 内相对计算，不写入 `offline_evaluation`，而写入
`batch_test_item`。这避免同一个评估结果在不同 cohort 中携带错误排名。

### 9.1 percentile

对每个非 null 指标按升序计算 average rank：

```text
n = 1: percentile = 0.5
n > 1: percentile = (average_rank - 1) / (n - 1)
```

### 9.2 权重与分数

| 指标 | 权重 | 方向 |
|---|---:|---|
| `net_label_return` | 0.45 | 越高越好 |
| `net_db_10d` | 0.30 | 越高越好 |
| `positive_net_label_day_ratio` | 0.15 | 越高越好 |
| `primary_coverage_ratio` | 0.10 | 越高越好 |

```text
available_weight = sum(weight of non-null metrics)
recommendation_score = 100 * sum(weight * percentile) / available_weight
evidence_confidence = available_weight
```

至少 `net_label_return` 或 `net_db_10d` 有一个非 null 才生成 score；只有 coverage 的候选
保持未排名，但仍完整展示。缺失指标按剩余权重归一化，不填 0、不假装失败；只要发生权重
重归一化，`evidence_quality` 至少为 degraded，排行榜必须显示缺失组件和实际 available weight。

### 9.3 排名与 top-3

排序键：

1. `recommendation_score DESC NULLS LAST`
2. `evidence_confidence DESC`
3. `net_db_10d DESC NULLS LAST`
4. `candidate_id ASC`

取前 3 个有 score 的成功候选作为 top-3；不足 3 个就返回实际数量。未入选候选不标记
rejected，只标记 `is_top3=false`。公式、权重、percentile、组件值和版本全部持久化。

## 10. DB Contracts（数据库详细设计）

### 10.1 bootstrap

目标文件：`backend/db/init_hmm_evolution_schema.py`。

- `SCHEMA_VERSION = "hmm_evolution_v1"`；
- DDL 列表在一个 transaction 内按序执行；
- 全部 schema/table/column 有 `COMMENT ON`；
- `CREATE ... IF NOT EXISTS` 只用于首次 bootstrap；已有对象结构不符时 verify 失败，禁止
  用 IF NOT EXISTS 掩盖 drift；
- `iter_ddl()` 和 expected columns/constraints 暴露给测试；
- service 不调用 bootstrap；
- 不创建 role、不 GRANT、不连接生产环境默认地址。

### 10.2 `hmm_evolution.schema_version`

| 字段 | 类型 | 约束/语义 |
|---|---|---|
| `version` | TEXT | PK |
| `description` | TEXT | NOT NULL |
| `applied_at` | TIMESTAMPTZ | NOT NULL default clock timestamp |

### 10.3 `hmm_evolution.candidate`

| 字段 | 类型 | 约束/语义 |
|---|---|---|
| `candidate_id` | TEXT | PK，server-derived |
| `manifest_hash` | CHAR(64) | UNIQUE, NOT NULL |
| `display_name` | TEXT | NOT NULL |
| `description` | TEXT | nullable |
| `source_type` | TEXT | supported three-value CHECK |
| `source_ref` | JSONB | NOT NULL |
| `artifact_manifest` | JSONB | NOT NULL |
| `algorithm_version` | TEXT | NOT NULL |
| `lifecycle_status` | TEXT | research_only/retired/invalid CHECK |
| `invalid_reason_code` | TEXT | nullable |
| `invalid_context` | JSONB | nullable |
| `created_by` | TEXT | NOT NULL |
| `row_version` | BIGINT | NOT NULL default 1 |
| `created_at/updated_at` | TIMESTAMPTZ | NOT NULL |
| `retired_at` | TIMESTAMPTZ | nullable |

索引：`lifecycle_status, created_at DESC`；JSONB 不建通用 GIN，避免无边界索引成本。

### 10.4 `hmm_evolution.offline_evaluation`

| 字段组 | 关键字段 |
|---|---|
| identity | `eval_id` PK、`logical_evaluation_key` CHAR(64)、`run_generation` INT、UNIQUE(logical key, generation) |
| input | `candidate_id` FK、`base_loop_ref`、`source_manifest`/hash、`candidate_manifest_hash`、`evaluation_spec`/hash、`evaluator_version`、`input_hash` |
| window | `as_of_date`、`window_start/end`、`label_horizon_days`、`universe_id/hash`、`topk` |
| state | `status`、`attempt_count`、`owner_id`、`fencing_token`、`lease_expires_at`、`heartbeat_at`、`cancel_requested_at`、`row_version` |
| counts | `trading_days_count`、`changed_day_count`、label/DB comparable days、`replacement_count` |
| metrics | `primary_coverage_ratio`、`net_label_return`、`net_db_10d`、`positive_net_label_day_ratio`、`evidence_quality`、`warnings_json`、`metrics_json`、`result_hash` |
| error | `error_code`、`reason_code`、`error_message`、`error_context` |
| time | `queued_at`、`started_at`、`completed_at`、`created_at`、`updated_at` |

状态 CHECK：`queued/running/succeeded/failed/cancelled/timed_out`。所有 ratio 为 0..1；
日期、topk、generation、count 有合法 CHECK。索引覆盖 `(status, lease_expires_at)`、
`(candidate_id, created_at DESC)`、`input_hash`。

### 10.5 `hmm_evolution.batch_test_run`

| 字段组 | 关键字段 |
|---|---|
| identity | `batch_id` PK、`request_hash` UNIQUE、`idempotency_key` nullable UNIQUE |
| retry | `retry_of_batch_id` self FK、`retry_generation` INT |
| state | `status`、`owner_id`、`fencing_token`、`lease_expires_at`、`heartbeat_at`、`cancel_requested_at/by`、`row_version` |
| summary | candidate/queued/running/succeeded/failed/cancelled/timed_out count |
| recommendation | `recommendation_spec`/hash、`recommendation_version` |
| error | `error_code`、`reason_code`、`error_context` |
| audit | `created_by`、`created_at`、`started_at`、`completed_at`、`updated_at` |

状态 CHECK：`queued/running/cancel_requested/completed/partial_failed/failed/cancelled/timed_out`。

### 10.6 `hmm_evolution.batch_test_item`

| 字段 | 类型 | 语义 |
|---|---|---|
| `batch_id` | TEXT FK | composite PK |
| `candidate_id` | TEXT FK | composite PK |
| `eval_id` | TEXT FK | NOT NULL |
| `ordinal` | INT | request order；UNIQUE(batch, ordinal) |
| `item_status` | TEXT | pending/waiting_shared/reused/queued/running/succeeded/failed/cancelled/timed_out |
| `recommendation_score` | DOUBLE | nullable |
| `evidence_confidence` | DOUBLE | nullable 0..1 |
| `recommendation_rank` | INT | nullable |
| `is_top3` | BOOL | NOT NULL default false |
| `recommendation_components` | JSONB | nullable |
| error snapshot | code/reason/context | item-readable failure |
| timestamps | created/updated/completed | audit |

`recommendation_*` 属于 batch item，不属于 evaluation。

## 11. Repository 与 transaction 契约

目标模块：

```text
backend/services/hmm_evolution/
├── models.py
├── errors.py
├── qe_asset_reader.py
├── candidate_artifact.py
├── evaluator.py
├── scorer.py
├── repository.py
├── service.py
└── worker.py
```

Repository 必须提供：

- `register_candidate()` / `get_candidate()` / `list_candidates()` / `retire_candidate()`；
- `create_or_get_evaluation()`；
- `create_or_get_batch()` / `get_batch()` / `list_batches()`；
- `claim_batch()` / `claim_evaluation()`，使用 `FOR UPDATE SKIP LOCKED`；
- `heartbeat_*()`，带 `fencing_token + row_version` compare-and-set；
- `complete_evaluation()` / `fail_evaluation()` / `cancel_evaluation()`；
- `request_batch_cancel()` / `recompute_batch_state()` / `apply_recommendation()`；
- `create_retry_batch()`，只重建 failed/cancelled/timed_out item。

Repository 和 service 禁止裸 `except:`，禁止捕获 `Exception` 后返回空 dict/list、默认 0、默认
neutral 或成功状态。边界层如需捕获未知异常，必须保留 exception chaining、映射稳定 reason
code、记录 trace id，并将任务置为 failed；cleanup 次级失败作为 warning 附加，不能覆盖主错误。

每次 transaction 只完成一个状态变化和必要的相邻行更新。外部 artifact/market I/O 不得
持有 DB transaction。

## 12. 状态机、lease、幂等与取消

### 12.1 batch 状态机

```text
queued ──claim──> running ──all success──> completed
  │                  │  ├──mixed terminal──> partial_failed
  │                  │  ├──all failure─────> failed
  │                  │  ├──lease expired───> timed_out
  │                  │  └──cancel request──> cancel_requested ──> cancelled/partial_failed
  └──cancel request──────────────────────────────────────────────> cancelled
```

### 12.2 evaluation 状态机

```text
queued -> running -> succeeded
                  -> failed
                  -> cancelled
                  -> timed_out
```

禁止 terminal 状态原地重置。显式 retry 创建 `run_generation+1` 的新 evaluation 和新 batch，
保留旧失败记录。

### 12.3 lease 与 fencing

- 默认 heartbeat 15 秒、lease 90 秒；operator 可配置，但 lease 必须至少为 heartbeat 的 3 倍；
- claim 时递增 `fencing_token`；
- heartbeat/complete/fail 必须匹配 owner + token + row_version；
- token 失配返回 `hmm_evolution_stale_fencing_token`，旧 worker 不得提交结果；
- worker 不自动无限重试；lease 超时进入 timed_out，由显式 retry API 处理。

### 12.4 request 幂等

- `request_hash` 由排序后的 candidate IDs、evaluation spec、recommendation spec 计算；
- 相同请求返回原 batch，响应标记 `idempotent_replay=true`；
- 相同 `Idempotency-Key` 对应不同 request hash 返回 HTTP 409；
- batch candidate IDs 必须去重，原始展示顺序记录在 ordinal，不影响 request hash。

### 12.5 shared evaluation

创建 batch 时：

- 已有 succeeded evaluation：item=`reused`，不重算；
- 已有 queued/running evaluation：item=`waiting_shared`，等待同一结果；
- 已有 failed/cancelled/timed_out：普通请求复用其终态并提示显式 retry；
- retry endpoint 创建下一 generation。

取消 batch 只取消其 item。仅当某 running evaluation 没有其它 active batch item 引用时，才设置
evaluation cancel flag；否则该 evaluation 继续，当前 item 进入 cancelled。

## 13. Worker 执行与性能设计

### 13.1 输入复用

同一 batch 的 pred/label 只加载一次为 `BatchInputBundle`，候选之间共享只读 DataFrame。
Prediction Store 命中继续零副本读取原 artifact，不创建 Phase 1 永久副本。

### 13.2 并发

- 默认候选并发 2，部署配置可调整 1..4；这是资源保护，不是研究方向门禁或审批；
- 同一 worker 内共享 input bundle，禁止为每个 candidate 复制完整 pred/label；
- market forward return 对 batch 的 replacement symbol/date 并集批量查询并按 candidate 切分；
- 不使用无界 process pool。

### 13.3 取消检查点

Worker 在以下边界检查 cancel/lease：输入加载后、candidate artifact 校验后、每 20 个交易日、
DB enrichment 前后、结果提交前。取消不会留下 succeeded 假状态。

### 13.4 性能 receipt

每次 benchmark 记录：硬件、Python/pandas 版本、base loop、行数、日期数、candidate 数、
artifact source、冷/热缓存、各阶段耗时、峰值 RSS、并发和结果 hash。

验收基准：

- 单候选在标准验收基准上冷输入 <10 分钟；
- 10 候选共享输入且受限并发 <30 分钟；
- 未在指定基准和硬件测量时只能报告 pending，不能用小 fixture 宣称性能完成。

## 14. API Contracts

Router prefix：`/api/v1/hmm-evolution`。

### 14.1 candidate

| 方法 | 路径 | 语义 |
|---|---|---|
| POST | `/candidates/preview` | 只读解析、hash、coverage，零 DB 写 |
| POST | `/candidates` | 登记候选，201 或幂等 200 |
| GET | `/candidates` | 按 lifecycle/source 分页 |
| GET | `/candidates/{candidate_id}` | manifest、coverage、历史摘要 |
| POST | `/candidates/{candidate_id}/retire` | 软退役，不删除历史 |

### 14.2 QE asset read-only

| 方法 | 路径 | 语义 |
|---|---|---|
| GET | `/qe-assets/{task_id}/{loop_name}` | 列出全部可见实验资产与 trust level |
| GET | `/qe-assets/{task_id}/{loop_name}/stat` | 指定安全相对路径的 metadata/hash |
| GET | `/qe-assets/{task_id}/{loop_name}/content` | 受大小限制的 text/JSON 或原始 bytes stream；只读 |

asset API 必须拒绝绝对路径、路径穿越和 QE mutation method；大文件使用 stream/Range，不整文件
读入 API 进程内存。配置资产可查看但不提供“应用/执行/导入”按钮。

`/content` 是受控只读传输 contract，不授权 UI 直接 dump JSON/YAML。前端必须先根据 media type、
schema id 和 trust level 选择专用摘要/表格；未知结构化格式只展示 metadata 和“不支持可视化”。
禁止因为缺少 renderer 就把原始 JSON 作为默认 fallback。

### 14.3 evaluation/batch

| 方法 | 路径 | 语义 |
|---|---|---|
| POST | `/evaluate` | 单候选；内部创建一项 batch；返回 202 |
| POST | `/batch` | 1..50 个去重候选；返回 202 |
| GET | `/evaluations/{eval_id}` | 状态、指标、daily summary、错误 |
| GET | `/batches` | 分页历史 |
| GET | `/batches/{batch_id}` | batch、items、top-3、progress |
| POST | `/batches/{batch_id}/cancel` | 幂等取消请求 |
| POST | `/batches/{batch_id}/retry-failed` | 新 batch/generation，只重试失败项 |

### 14.4 response envelope

成功：

```json
{"status": "ok", "data": {}, "trace_id": "..."}
```

失败：

```json
{
  "error_code": "HMM_EVOLUTION_ERROR",
  "reason_code": "hmm_evolution_label_horizon_mismatch",
  "message": "标签 horizon 与请求不一致",
  "context": {},
  "trace_id": "..."
}
```

### 14.5 HTTP / reason code

| HTTP | reason code 示例 |
|---:|---|
| 400 | invalid_spec / duplicate_candidate / unsupported_source / unsafe_asset_path |
| 404 | candidate_not_found / batch_not_found / evaluation_not_found |
| 409 | idempotency_conflict / invalid_state_transition / retry_not_available |
| 422 | artifact_manifest_invalid / label_horizon_mismatch / no_common_dates |
| 503 | schema_unavailable / source_unavailable / qe_asset_unavailable / market_data_unavailable |
| 504 | evaluation_timed_out |

稳定 reason code 统一以 `hmm_evolution_` 开头。`context` 不返回秘密、绝对路径或大 payload。

## 15. UI Contracts

### 15.1 用户确认的最终信息架构

2026-07-18 用户确认：

1. 保留“演进实验室 / 板块风险 / 滚动训练”三个一级页签；
2. 接受状态热力图、页面内今日预警和下方固定详情区；
3. 最终 HMM 主入口默认进入“板块风险”热力图。

最终路由与阶段激活：

| 路由 | 职责 | 当前阶段交付 |
|---|---|---|
| `/hmm` | Phase 2 完成后重定向 `/hmm-risk` | P1-C 不注册误导性默认入口 |
| `/hmm-evolution` | 候选、评估、批次、排行榜、top-3 | P1-C 必须完整交付 |
| `/hmm-evolution/batches/[batchId]` | 批次状态、项目结果、失败与推荐证据 | P1-C 必须完整交付 |
| `/hmm-evolution/evaluations/[evalId]` | 单次评估的结构化证据详情 | P1-C 必须完整交付 |
| `/hmm-risk` | L1/L2 状态热力图、预警、固定详情、状态分布 | 总体蓝图 Phase 2 验收通过后 |
| `/hmm-research-training` | 窗口、时效性、研究训练任务、隔离边界 | 总体蓝图 Phase 3 验收通过后 |

P1-C 可以建立可扩展的 `HMMResearchNavigation` 外壳，但只展示已通过真实 API/UI 验收的
“演进实验室”入口。风险和滚动训练不得以 disabled tab、静态截图、mock 数据、空路由或“敬请期待”
冒充可用功能；后续 Phase 验收通过后再注册对应页签。最终三个页签启用后，点击 HMM 主导航默认进入
`/hmm-risk`。

### 15.2 演进实验室总览

1. **候选库**：名称、来源、coverage、hash 短码、lifecycle；
2. **QE 资产浏览**：task/loop 资产目录、来源、hash、trust level 和 schema-aware 摘要；
3. **新建评估**：base loop、日期/as-of policy、label horizon、TopK、DB 10 日收益模式、候选多选；
4. **运行中批次**：进度、heartbeat、耗时、取消和 item 级状态；
5. **历史排行榜**：推荐分数、证据置信度、净标签收益（动态 horizon）、Net DB 10D、正值日比例、coverage；
6. **top-3 研究推荐**：明确标注“研究推荐，需 QE 终审”，未入选候选仍完整展示；
7. **证据质量**：watermark、candidate/source/evaluator version、degraded warnings 在主视图可见。

### 15.3 固定证据区、独立详情页与结构化资产查看

- 主视图使用中文指标卡、表格、进度、逐日折线和固定证据区；详情页保持页面路由，不使用抽屉、
  侧滑列表或悬浮 JSON 面板。
- manifest/spec/hash/error context 必须转成明确字段分组，例如“输入身份 / 数据水位 / 计算版本 /
  证据质量 / 失败原因”；hash 可复制但不得把整个 payload dump 到页面。
- QE JSON/YAML 资产不得直接渲染原始文件。已知 schema 使用字段摘要、表格或专用可视化；未知 schema
  仅显示 filename、media type、size、hash、trust level 和“不支持可视化”，不得 raw dump 兜底。
- 普通 text/log 资产只允许在独立资产检查页做大小受限、明确标注来源的只读文本查看；不能混入
  候选、评估或推荐主视图，也不能提供执行、导入或应用动作。
- `changed_day_count=0` 显示“未改变 TopK”，不显示绿色 0 收益或成功门禁；h20 动态显示
  “20 交易日”，不得误写成 10 日收益。

### 15.4 加载、空态、降级、失败和轮询

| 状态 | 页面行为 |
|---|---|
| loading | 显示有界 skeleton 和正在加载的资源名；超过客户端阈值进入 timed-out，不无限旋转 |
| empty | 说明“无候选/无批次/无结果”的具体原因与下一步，不返回空白成功 |
| degraded | 主视图显示 warning、受影响指标、裁剪范围和可用证据；不只写技术 context |
| failed/timed_out | 固定错误区显示 reason code、中文解释、trace id、失败阶段和可重试条件 |
| stale | 显示数据水位、期望水位和陈旧范围；不得自动使用旧结果冒充最新 |
| terminal | 停止 polling，保持最终审计状态；成功、partial_failed、failed、cancelled 明确区分 |

- polling 初始 3 秒，60 秒后退避到 10 秒，terminal 后停止；本阶段不引入 WebSocket。
- API/renderer Promise rejection 必须进入可见 failed 状态；禁止仅 `console.error`、吞异常后保留 loading、
  返回 `{}`/`[]` 或静态成功。
- P1-C 不需要热力图 renderer；未来共享 chart loader 必须具备
  `renderer_loading/renderer_failed/renderer_ready` 显式状态，不能沿用现有 console-only 动态加载模式。

### 15.5 视觉与交互规范

- 使用 shadcn-compatible tokens、清晰的 card/table/badge/button/form 边界和适合研究数据的密度；
- 采用用户确认的浅色研究工作台：neutral surface、深绿色 primary、绿色/灰色/琥珀色状态色，
  红色与青色只作为 HIGH/OPPORTUNITY 的 severity accent；正式实现使用
  `--hmm-state-trending`、`--hmm-state-neutral`、`--hmm-state-fading`、
  `--hmm-severity-high`、`--hmm-severity-opportunity` 等语义 token，不在页面散落硬编码色值；
- 禁止复用 `paper-v2.css`、`pv2-*`、`frontend/src/components/paper-v2/*` 或
  `/paper-v2/model-hmm` 页面布局；
- 禁止抽屉式列表、右侧滑出详情、raw JSON 文件直显；长内容进入独立详情页；
- 颜色不能是唯一状态载体，badge、文字和 aria label 必须同步；
- UI 不出现“通过生产门禁”“允许交易”“替换生产模型”等未获批准的动作或暗示。

### 15.6 客户端文件

目标：

```text
frontend/src/app/hmm-evolution/
frontend/src/components/hmm-research/HMMResearchNavigation.tsx
frontend/src/components/hmm-research/EvidencePanel.tsx
frontend/src/components/hmm-research/VisibleErrorState.tsx
frontend/src/components/hmm-evolution/
frontend/src/lib/hmm-research/contracts.ts
frontend/src/lib/hmm-evolution/api.ts
frontend/src/lib/navigation/nav-groups.ts
```

共享组件只承载研究信息和可见失败状态，不得带入交易动作、生产 snapshot 写入或 Paper v2 API contract。

## 16. Failure Modes（失败模式与处置）

| 失败 | 处置 |
|---|---|
| candidate artifact 缺失/hash 改变 | candidate 标 invalid，评估失败；不生成替代 artifact |
| Prediction Store manifest 损坏 | 继承 Phase 0 fail loud，不 fallback 掩盖 |
| QE 任意资产路径不安全或读取失败 | 400/503 + reason code；不得返回空内容假成功 |
| QE asset 只有 unverified evidence | 可查看；不得进入 evaluator/scorer |
| label horizon 不一致 | 422；不重命名、不截断 |
| coefficient/pred 无共同日期 | 422 no_common_dates |
| 共同日期集合为空 | 422 coefficient_date_coverage_empty |
| 共同日期裁剪 | succeeded/degraded + 主视图 warning，不静默 |
| market DB 不可用且 mode=required | failed + market_data_unavailable |
| 无 TopK 替换 | succeeded + unranked，不伪造 0 收益 |
| worker 崩溃/lease 过期 | timed_out；旧 fencing token 不得提交 |
| 部分 candidate 失败 | batch=partial_failed；成功结果仍可读 |
| cancel 与 complete 竞争 | CAS/fencing 决定唯一 terminal 状态 |
| schema 未部署 | API 503 schema_unavailable；不得隐式 DDL |
| 推荐指标缺失 | 按可用权重归一化；无 efficacy 指标则 unranked |
| 未知异常 | failed + chained exception/trace/reason；不得返回 `{}`、`[]` 或默认 neutral |
| UI API 返回成功但业务集合为空 | 显式 empty reason 与查询条件；不得显示完成卡或空白排行榜 |
| chart/component 动态加载失败 | visible failed + reason code + retry；不得只写 console 或永久 loading |
| JSON/YAML 无专用 renderer | 仅显示 metadata/trust/hash 和不支持可视化；不得 raw dump 兜底 |
| 风险/训练 Phase 未验收 | 不注册对应 tab/route；不得静态占位、假开关或 mock 页面冒充功能 |

## 17. Verification Plan（验证方案）

### 17.1 schema/repository

- bootstrap DDL/comment/constraint/index snapshot test；
- 在临时开发 DB transaction 中连续执行两次并 verify；
- drift 检测必须失败；
- repository 状态迁移、CAS、fencing、idempotency、shared evaluation、retry 和取消测试；
- SQL write-target guard 断言仅 `hmm_evolution.*`。
- QE asset reader 断言只调用 list/read/stat，mutation method 通过 fake client 明确 fail；真实 node
  integration 必须证明 catalog completeness=complete，partial manifest 不得冒充全资产；
- config/log/model/report 等多类型资产均可 inspection-only 读取，未经信任不得进入 evaluator。

### 17.2 evaluator/scorer

- 非并列 fixture 与旧 `compute_replacements` entered/dropped/daily net 对齐；
- 并列 fixture 验证 symbol tie-break；
- label horizon 10/20、混合 horizon、NaN/Infinity、缺 mapping、缺 date、无替换；
- common-date intersection、degraded warning 主动暴露、strict explicit mode；
- DB 10 交易日使用交易日历，不使用自然日；
- day-weighted 聚合、coverage denominator、结果 hash；
- scorer percentile、singleton、并列、缺失、top-3 少于 3、无淘汰副作用。

### 17.3 API/UI

- Pydantic request/response contract 与 reason code 测试；
- 202/idempotent 200/409 conflict/取消/retry；
- UI 使用契约级 test server 驱动真实 API client，覆盖中文文案、动态 horizon、QE asset schema-aware
  view、固定证据区、degraded warning、empty/failed/stale/timed-out 和 terminal polling；
- 断言 P1-C 仅注册已完成的演进页；未完成的 `/hmm-risk`、`/hmm-research-training` 不得出现
  disabled tab、空路由、静态占位或 mock-only 页面；
- changed-file guard 断言无 `paper-v2.css`、`pv2-*`、`frontend/src/components/paper-v2/*`、
  抽屉式列表和 raw JSON 主视图；
- component rejection/renderer load error 必须进入 `VisibleErrorState`，reason code、中文解释、trace id
  和 retry condition 可见；不得依赖 console 或无限 loading；
- keyboard、focus、aria label 和非颜色状态标识测试；长详情通过独立路由访问，不使用 drawer；
- 安全验证端口使用 `backend.validation_app` 扩展或专用 test app，不启 8001/3000/19080；
- UI E2E/截图交给 Validation Center，不能用静态 grep 代替。

### 17.4 外部验收

- 使用 Phase 0 已验收高收益 loop 作为输入复用证明；h20 标签按 h20 展示；
- 至少 10 个历史 HMM case 与 QE 结果做方向/排序对照；差异仅记录为 evidence；
- 记录 performance receipt、Prediction Store zero-copy、QE 全资产只读 access receipt、DB transaction read-only 与 latest-common watermark；
- 任何 production DDL、worker activation 或服务运行未获批时明确报告 pending。

#### 17.4.1 P1-A 外部验收回执（2026-07-17）

- **真实 dev PostgreSQL**：仅连接 `5433/aistock_dev`，8 路并发验证 candidate、
  evaluation、batch 的唯一创建与幂等返回；验证 batch/evaluation 单 worker claim、
  heartbeat、row_version CAS、fencing token 拒绝旧写、完成态重算和 lease 超时。
  `backend/tests/hmm_evolution/test_repository_dev_postgres.py` 结果为 `1 passed`，
  测试数据按唯一 ID 精确删除。
- **repository 事务边界**：默认连接固定为
  `get_conn(autocommit=False, manage_transaction=True)`，禁止多语句 create/batch/state
  transition 在 autocommit 下产生半提交。
- **生产 schema**：经用户授权后已执行并 verify
  `hmm_evolution_v1`；回执为 5 张表、115 列、41 个约束、7 个非约束索引，业务表为空。
  bootstrap 原子性与 PostgreSQL constraint normalization 修复见 BUG-729 / PR #2344。
- **真实 QE 资产复用**：选择近期高收益
  `qe_20260706_013235_bbd4/Loop8`（年化收益
  `0.4939623331722296`，horizon=20，status=completed）。RD-Agent PR #4 增加精确
  `GET .../files` 只读路由；用合并后的路由对真实 node workspace 扫描得到 221 个
  唯一相对路径、`catalog_completeness=complete`、无绝对路径和重定向。
- **零副本证据**：AIstock reader 原位读取既有
  `pred.pkl`（18,159,009 bytes，
  SHA256=`bc82351d405b5f370eaef50ce3245d237508f1861806bc31ffdd63b62451cfef`）
  与 `label.pkl`（18,159,035 bytes，
  SHA256=`451a11242af6cc9834760704411403986344850522fe59b882c6387b0ce8a0f3`），
  只生成内存 read receipt，不创建新的 artifact 副本。
- **运行态边界**：上述证据完成 P1-A contract/数据验收；现有 9000 进程仍按正常部署周期
  更新，未因本验收重启。8001/3000/19080 未由本任务启动，worker/API/UI activation
  继续为 pending。

### 17.5 DESIGN-COMPLIANCE-001

每个实现 PR 逐项检查：

- `no_simplified_delivery`：是否交付 PR 承诺的完整 API/UI/worker/benchmark 子集；是否存在
  backend-only、mock-only、静态页面、死 tab、placeholder 或演示文件进入正式实现；
- `no_silent_error`：是否存在 bare/broad except、空集合假成功、中性 fallback 隐藏、
  console-only error、永久 loading、静态成功或 warning 只藏技术 context/raw JSON；
- `no_business_semantic_drift`：是否保持 state/confidence/severity/score/horizon 身份，是否触碰
  QE/Paper/Selection/QMT/生产 snapshot，是否把热力图解释成交易热度或买卖建议；
- `no_unrequested_gate_or_approval`：是否新增本文未批准的阈值、审批、确认链或研究淘汰规则；
  runtime switch 仍仅是部署开关，不是每次评估的产品审批。

四项必须分别给出实现引用与验证证据；任何一项为 gap 时，不得请求合入或标记 F-010 verified。

## 18. Implementation Plan（实施方案）

### P1-A：schema、candidate registry 与 durable state machine

交付 F-006/F-008：

- Python bootstrap + comments + verify；
- Pydantic models/errors；
- QE 全资产只读 reader、candidate artifact preview/registry；
- repository、batch/evaluation/item 状态机、lease/fencing/idempotency/retry/cancel；
- worker skeleton，不启生产 runtime。

当前状态：P1-A 源码与外部验收完成；P1-B 与 P1-C 源码完成，P1-C 审计硬化已覆盖共享输入并发、lease recovery、QE 权威节点、内容安全、全资产浏览、UI 状态机与异常可观测性；
`production_ddl_gate=applied_and_verified`；
`runtime_activation_gate=pending`。这不代表整个 Phase 1 已通过外部验收；真实 QE 10-case、10 候选性能、实机页面截图和首次 runtime activation 仍未完成。

### P1-B：evaluator 与 recommendation scorer

交付 F-007/F-009：

- 从诊断脚本抽取 pure evaluator；
- Phase 0 source manifest adapter；
- DB 10 交易日 return repository；
- metrics/result hash/daily evidence；
- `hmm_recommendation_v1`；
- 旧诊断脚本改用同一 pure evaluator。

### P1-C：API、UI 与外部验收

交付 F-010：

- candidate/evaluation/batch API；
- worker CLI 与受控启用方式；
- `/hmm-evolution` 页面、批次/评估详情、固定证据区和 HMM research navigation shell；
- schema-aware QE asset view、完整 loading/empty/degraded/failed/stale/terminal 状态；
- 禁止 Paper v2 依赖、抽屉式列表、raw JSON 主视图和未实现风险/训练 tab；
- 真实 API/UI 证据、10-case 对照和性能验收；
- 生产 runtime 首次启用需要一次操作授权；启用后正常研究操作不再增加审批门。

每个 PR 必须只承诺自身可验证子集，并更新总体蓝图 Design Acceptance Matrix 的真实引用。

## 19. Rollout / Rollback（发布与回滚）

### 19.1 rollout 顺序

1. 合入 P1-A 代码但不执行生产 DDL；
2. 在开发/验证 DB 显式 bootstrap、复跑和 drift verify；
3. 获得生产 DDL 操作授权后执行并保存 receipt；
4. 合入 P1-B，使用人工 worker CLI 做受控 benchmark；
5. 合入 P1-C，先在安全验证 app/UI 完成演进页、证据区、错误状态和导航激活边界验收；
6. 获得一次生产 runtime activation 操作授权后按部署配置启用；API/worker/nav 状态分别记录，
   但不得衍生三套产品审批流。

### 19.2 activation flags

- `HMM_EVOLUTION_RUNTIME_MODE=disabled|api_only|api_worker`，默认 disabled；
- `NEXT_PUBLIC_HMM_EVOLUTION_ENABLED=false` 默认；
- artifact roots 为空时 candidate local source 不可用，不回退任意路径。

上述配置是 deployment switch/kill switch，不是候选注册、评估或推荐的业务审批条件。

### 19.3 rollback

- 先关闭 UI/API/worker flags；
- 回滚代码不 DROP schema、不删历史结果；
- candidate 使用 retired/invalid，evaluation 保留 immutable audit；
- schema migration 只能 forward-fix；需要破坏性 DDL 时另立审批；
- 回滚不得修改生产 HMM、QE、Paper 或交易记录。

## 20. Risks（风险与缓解）

| 风险 | 缓解 |
|---|---|
| 同一输入被重复加载导致内存爆炸 | batch 级 input bundle、受限并发、性能 RSS receipt |
| 旧诊断与新 evaluator 分叉 | 诊断脚本改为调用 pure evaluator；oracle tests |
| label horizon 被误命名 | generic net_label_return + explicit horizon + UI 动态标签 |
| batch-relative rank 污染 reusable evaluation | rank/score 只存在 batch item |
| artifact 路径漂移 | stable URI + SHA；每次执行前复核；失配 invalid |
| worker 重启产生重复提交 | DB lease、fencing、row_version CAS |
| partial failure 被整体掩盖 | partial_failed + item error；成功结果保留 |
| 推荐公式变成淘汰门禁 | 无阈值；所有候选展示；top-3 明示 QE 终审 |
| Phase 1 越权读写生产状态 | source resolver 只读；repository write allowlist；副作用测试 |
| schema/code/runtime 状态混淆 | 分开报告 merge、DDL、activation、external acceptance |
| Paper v2 视觉/生产语义泄漏 | changed-file import/class guard + 独立 HMM research shell |
| raw JSON 或抽屉替代业务视图 | schema-aware fields、固定证据区、独立详情页；未知结构不直接展示 |
| 未实现 Phase tab 被静态占位 | 导航按验收项注册；P1-C 只激活演进页 |
| UI 错误被 console/permanent loading 吞掉 | VisibleErrorState + reason code + terminal polling tests |

## 21. Production Gates（生产门禁）

| 门禁 | 本设计 PR | P1-A | P1-B | P1-C |
|---|---|---|---|---|
| `production_ddl_gate` | noop | applied_and_verified（hmm_evolution_v1：5 tables / 115 columns / 41 constraints / 7 indexes） | noop | noop |
| `production_backend_dependency_gate` | noop | noop | noop | noop |
| `production_frontend_dependency_gate` | noop | noop | noop | noop |
| `runtime_activation_gate` | noop | pending | pending | pending，首次生产启用需一次操作授权；不新增业务审批流 |
| `data_write_gate` | docs only | 仅 hmm_evolution.* | 仅评估结果 | 同左 |
| `service_start_gate` | noop | 禁止自动启动 | 禁止自动启动 | 安全验证与生产启动分离 |
| `design_compliance_gate` | F2 validator | DESIGN-COMPLIANCE-001 | 同左 | 同左 + UI evidence |

## 22. Design Acceptance Index（设计验收索引）

- **F-006 Phase 1 独立候选注册表**：QE 全资产只读 reader、内容寻址 candidate、可信
  coefficient manifest、research-only lifecycle、只写 `hmm_evolution.*`。
- **F-007 Phase 1 评估可重放**：显式 horizon、QE asset trust receipt、latest-common watermark、
  source/candidate/spec/evaluator hash、稳定排序、交易日 forward return 和 result hash。
- **F-008 Phase 1 批处理状态机**：batch/evaluation/item 分层、幂等、lease、fencing、heartbeat、
  取消、超时、重试、shared result 和 partial failure。
- **F-009 Phase 1 推荐语义**：`hmm_recommendation_v1` 版本化、batch-relative、无淘汰阈值、
  top-3 仅为 QE 终审前研究推荐。
- **F-010 Phase 1 API/UI**：真实 QE asset/candidate/evaluation/batch API、中文演进实验室、
  HMM research navigation shell、动态 horizon、schema-aware asset view、主视图 degraded warning、
  固定证据区、独立详情页和可见终止错误；禁止 Paper v2、抽屉式列表、raw JSON 主视图和未实现 tab。

## 23. Design Acceptance Matrix（设计验收矩阵）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-006 | 本文 §5.3、§6、§10、§11；`backend/services/hmm_evolution/{qe_asset_reader,candidate_artifact,models,errors,repository,service}.py`、`backend/services/quantevolver/qe_workspace_client.py`、`backend/db/init_hmm_evolution_schema.py`；RD-Agent PR #4 `qe_workspace_catalog.py` + exact `GET .../files` route | `python -m pytest backend/tests/hmm_evolution/test_qe_asset_reader.py backend/tests/hmm_evolution/test_candidate_artifact.py backend/tests/hmm_evolution/test_repository_integration.py -q`；真实 `qe_20260706_013235_bbd4/Loop8`：221 unique relative assets、complete catalog、pred/label read receipts、zero extra copy；生产 schema verify receipt见 §17.4.1 | verified | 无 |
| F-007 | 本文 §7、§8；`backend/services/hmm_evolution/{evaluator,input_adapter,market_repository,source_manifest,executor}.py`；`backend/services/hmm_data_source/{backtest_source,cache_manager}.py`；`scripts/diagnostics/hmm_offline_diagnostic.py` | `backend/tests/hmm_evolution/test_{evaluator,input_adapter,market_repository,source_manifest,executor,legacy_oracle,legacy_diagnostic}.py`；非并列旧诊断 oracle、显式 tie-break、h10/h20/mixed horizon、latest-common/read-only transaction、交易日 forward return、coverage/warning、deterministic result hash；BUG-736/BUG-737 回归覆盖无硬编码凭据、无宽泛异常吞错、无 QE config 下载、Phase 0 缓存复用与 canonical market repository；HMM/Data Source matrix：178 passed / 8 skipped；新核心模块 line coverage 86.76%、branch coverage 70.31%；真实 dev PostgreSQL：空行情 fail-loud + forward-return SQL/只读事务 smoke 1 passed | verified | 无 |
| F-008 | 本文 §10～§13；`backend/services/hmm_evolution/{repository,service,worker,input_adapter,executor,models,errors}.py`；BUG-742/BUG-743 | `python -m pytest backend/tests/hmm_evolution/test_worker.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_repository_integration.py -q`：共享 input bundle 单次加载、`candidate_concurrency=1..4` 有界并发、serialized heartbeat/fencing、worker-cycle batch recompute、每轮 lease reaper；既有 dev PostgreSQL 8-worker receipt | approved_by_user_implementation_complete_external_acceptance_pending | 用户明确批准先完成审计修复并合入、外部验收另行执行；仍需 dev PostgreSQL 真实双候选并发、进程中断后 lease recovery 和 10 候选耗时 receipt，未标记 verified |
| F-009 | 本文 §9；`backend/services/hmm_evolution/scorer.py`、`repository.py::_apply_recommendations_with_cursor()` | `backend/tests/hmm_evolution/test_scorer.py`、`test_repository_integration.py::test_batch_recommendations_persist_only_on_batch_items`；singleton/percentile/tie/missing renormalization/coverage-only unranked/stable top-3/no evaluation-table write；无淘汰阈值、无新增审批，排名只持久化到 batch item | verified | 无 |
| F-010 | 本文 §14、§15；`backend/routers/hmm_evolution.py`、`backend/services/hmm_evolution/{runtime,asset_content_policy}.py`、`scripts/hmm_evolution_worker.py`、`frontend/src/{app/hmm-evolution,components/hmm-evolution,components/hmm-research,lib/hmm-evolution,lib/hmm-research}`；BUG-744～BUG-748 | `python -m pytest backend/tests/hmm_evolution/test_api.py backend/tests/hmm_evolution/test_qe_workspace_client_catalog.py backend/tests/hmm_evolution/test_frontend_contract.py -q`；`frontend/tests/hmm-evolution/hmm-evolution.spec.ts`：QE 权威节点、text-only bounded content + redaction、221+ 资产分页搜索、schema-aware 摘要、bounded polling/stale/degraded、daily_summary fail-loud、SVG 曲线、session idempotency | approved_by_user_implementation_complete_external_acceptance_pending | 用户明确批准先完成审计修复并合入、外部验收另行执行；真实 API 页面截图、完整 Playwright、10-case、性能 benchmark 和首次 runtime activation 仍待补，未宣称 F-010 verified |

## 24. 设计结论

P1-A 的 QE 全资产只读 reader、candidate identity、schema 和 durable state machine 已完成
源码、真实 dev PostgreSQL、生产 schema 与真实 QE workspace 外部验收。P1-B 已实现 pure
evaluator、Phase 0 source manifest adapter、latest-common/交易日收益只读 repository、durable
executor、batch-relative recommendation scorer，并通过 BUG-736/BUG-737 完成旧诊断唯一计算
路径迁移；F-007/F-009 已验证。P1-C 的 API/UI/worker 源码及 BUG-742～BUG-748 审计硬化已完成，
但受控 10-case、10 候选性能、真实页面/Playwright 和首次 runtime activation 仍待外部验收。
生产 worker/API/UI 均未启用，不得把当前状态表述为整个 Phase 1 完成或已具备生产运行状态。

## 25. 变更记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v1.7 | 2026-07-18 | 回填 BUG-742～BUG-748 审计修复：有界并发共享输入、lease reaper、QE 权威节点、内容安全、全资产 schema-aware 浏览、UI fail-loud 状态机和 idempotency；F-008/F-010 标记为源码完成但外部验收待补，不提前宣称 Phase 1 完成 |
| v1.6 | 2026-07-18 | 回填 P1-C API/UI/worker 实现路径、本地 contract/TypeScript/Next build 证据和仍待完成的真实 UI/10-case/性能外部验收；将 F-006/F-008 测试证据改为 feature validator 可核验命令 |
| v1.5 | 2026-07-18 | 固化用户确认的三页签最终信息架构和风险热力图默认首页；Phase 1 只激活真实演进页；以固定证据区/独立详情页替代抽屉和 raw JSON；补 UI 状态机、失败语义、legacy guard、可访问性和四项 DESIGN-COMPLIANCE-001 审核 |
| v1.4 | 2026-07-17 | 回填 P1-B evaluator/scorer、旧诊断唯一计算路径迁移、真实验证证据和 P1-C 剩余范围 |
