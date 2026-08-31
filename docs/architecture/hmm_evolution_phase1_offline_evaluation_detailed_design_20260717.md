# HMM 演进系统 Phase 1 离线评估实验室实现级详细设计

> **版本**：v2.8
> **日期**：2026-07-17
> **修订日期**：2026-07-22
> **状态**：P1-A/P1-B/P1-C 与 2026-07-22 Phase 1 收官外部验收全部完成：benchmark purpose 隔离下 zerocopy 1c/10c 与 fallback（Loop1 + h10 spec，用户裁决）cold/warm 全 matrix 分阶段 receipt、真实 UI/Playwright 18 场景、worker 31.6 分钟 bounded soak（idle/claim/success/failure/retry/timeout），证据见 §17.4.6；同日经独立授权完成 production `hmm_evolution_v3` 单事务 DDL、exact verify 和 worker restart。Gate truth：`hmm_evolution_v1/v2/v3` production `applied_and_verified`；`production_ddl_gate=applied_verified`、`production_runtime_activation_gate=applied_verified`、dependency gates `noop`（§21）
> **设计权威**：总体蓝图 `hmm_evolution_and_risk_management_system_design_20260716.md` v2.8
> **上游运行契约**：`hmm_evolution_phase0_data_source_detailed_design_20260716.md` v2.2
> **隔离约束**：`HMM_EVOLUTION_ISOLATION_CONSTRAINTS.md` v2.1
> **Feature tier**：F2
> **Design Acceptance Index**：复用总体蓝图 `F-006`～`F-010A`

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

Phase 1 要把其中的纯计算语义抽取成内容可校验重放、可批量执行、可取消、可审计的研究服务，
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
9. 提供独立长期运行的自动评估 worker service，只消费 API 已登记的 durable queued work；
10. 通过独立 Python bootstrap 创建/升级 `hmm_evolution.*`，代码合入不等于生产 DDL 已执行。

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
- 不在代码合入时自动启用生产 worker、生产 schema、8001/3000/19080 或任何 scheduler；首次 worker service activation 仍是独立运行操作。
- 自动评估 worker 不创建 batch、不按日历触发研究、不训练 HMM，也不复用 Phase 3 rolling-training scheduler。
- 不新增裸 `.sql` 文件，不在业务 service 内隐式建表。

## 4. DESIGN-COMPLIANCE-001 控制

| 控制 | Phase 1 设计要求 |
|---|---|
| `no_simplified_delivery` | schema、repository、状态机、纯 evaluator、scorer、API、UI、自动评估 worker service 和验证证据必须按 F-006～F-010A 逐项完成后才可报告 Phase 1 完成。 |
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

- 只有生产 DDL 和首次生产 runtime activation 属于基础设施变更，需要明确操作授权；2026-07-18 用户已批准自动评估 worker 的代码设计与实现，但代码合入不等于进程已启动。
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

本结论只表示 v1.8 **设计文本**已消除已知 P0 缺口，不表示 F-010/F-010A 外部验收完成。对应项在真实代码、
API/UI、E2E、benchmark 和外部证据回填前继续保持已批准但不得提前宣称 verified 的状态。

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

deployment-started HMMEvolutionWorkerService
        └── poll durable queue -> HMMEvolutionWorker
                              └── claim batch/evaluation with lease + fencing + heartbeat
```

### 5.1 进程边界

- API 只校验轻量 request/candidate identity 并持久化 `preparation_queued` batch receipt；不读取 QE/market artifact。worker claim 为 `preparing` 后在 lease/heartbeat 下冻结一次输入，并原子创建 evaluation/item；不在请求线程执行长耗时 preparation 或评估。
- Worker 是独立进程，不是 FastAPI background task 或定时 scheduler。部署启动后自动轮询已有 durable queue，默认不随 backend startup 隐式启动。
- Worker service 只消费 API 已创建的 `preparation_queued` receipt 与其物化后的 queued evaluation；不得自行发起用户意图、选择候选、修改推荐公式或触发 Phase 3 训练。
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

### 7.1 `hmm_evaluation_spec_v2` 与 legacy v1

```json
{
  "schema_version": "hmm_evaluation_spec_v2",
  "base_loop_ref": "<task_id>/LoopN",
  "window_start": "YYYY-MM-DD",
  "window_end": "YYYY-MM-DD",
  "as_of": {"policy": "latest_common_completed", "requested_date": null},
  "label_horizon_days": 20,
  "universe": {"type": "source_loop_stock_pool_st_pit"},
  "topk": 50,
  "date_coverage_policy": "batch_common_intersection_with_evidence",
  "missing_sector_policy": "neutral_with_evidence",
  "market_forward_return": {
    "mode": "required",
    "horizon_trading_days": 10
  },
  "sort_policy": "score_desc_symbol_asc_v1",
  "metric_version": "hmm_replacement_metrics_v2",
  "recommendation_version": "hmm_recommendation_v1"
}
```

新评估只接受 v2。排名输入必须是“源 QE loop 实际使用的 PIT 股票池区间”与该 QE 数据集绑定的
不可变 ST-PIT eligible spans 的逐日交集，不能直接对 `pred.pkl` 全量 symbol 排名。股票池从
completed loop 的持久化 `config_json` 解析，读取 AIstock 本地交付给 QE 的同名 pool 文件，并冻结
文件 SHA256；ST-PIT 使用 `shsz_st_pit_qe_dataset_<dataset_contract_id>`，冻结 rule version、scope、
source fingerprint、index policy 和 coverage semantics。任一日交集后的 symbol 数小于 TopK 必须失败。

`hmm_evaluation_spec_v1` / `prediction_artifact_all` 仅用于只读展示已存在的历史 evaluation，不允许
创建或重试；旧结果保留其原始 hash，不用 v2 evaluator 覆写。未来选股荐股、模拟盘和生产消费者继续使用滚动 live namespace（当前为
`shsz_st_pit_active_v1`）；历史研究使用不可变 QE dataset namespace。两者执行同一权威 ST-PIT
规则，但不得混用快照或让历史结果随 live universe 漂移。

completed legacy loop 若历史上没有 immutable dataset namespace，只允许一次受控兼容路径：读取
`shsz_st_pit_active_v1` 对应的冻结 runtime artifact，并在 source manifest 标记
`binding_mode=legacy_frozen_runtime_artifact_v1`。兼容前必须同时满足 source loop=`completed`、
pool artifact `ready=true/dirty=false`、pool 文件 SHA、ST-PIT rule version、config hash、source
fingerprint、coverage semantics 与冻结 receipt 完全一致；任一项缺失或变化即 fail loud。该例外不允许
把当前 live namespace 静默当成历史权威，也不允许回写 QE/Paper/生产配置。

对冻结 ST-PIT runtime artifact 产生前、且经 repo immutable legacy manifest 单独批准的历史 loop，
BUG-798 增加更窄的 cross-loop compatibility 路径：预测、标签、stock pool 和 config 仍来自原 source
loop；ST-PIT 只能读取 receipt 固定的 donor task/loop 下 `qe_event_risk_policy.json`，并逐项校验 donor
identity、artifact SHA256/size、原 loop canonical config hash、原 pool SHA256、coverage、span count、
rule version、scope 和 source fingerprint。source manifest 必须如实标记
`binding_mode=legacy_allowlisted_compatibility_artifact_v1`、donor identity 及
`coverage_semantics=allowlisted_cross_loop_immutable_artifact_v1`，不得声称旧 loop 原运行时使用过该
artifact。未登记 loop、任一 receipt 漂移或窗口超出覆盖范围仍 fail loud；该路径不查询 live ST 状态、
不重建近似快照、不扩大 Phase 0 pred/label 下载白名单。

`as_of.policy` 支持 `explicit` 和 `latest_common_completed`；API 先写 durable receipt，worker 在
`preparing` lease 内解析一次并冻结为 `resolved_as_of_date`。后续 evaluation replay 必须核对冻结
manifest 的内容 hash，不能在执行阶段接受相同计数但不同值的漂移。

### 7.2 label horizon

- 请求的 `label_horizon_days` 必须与 Phase 0 归一化 label 中唯一 horizon 完全一致；
- label 出现多个 horizon 或与请求不符时失败；
- DB 和 API 的通用字段名为 `net_label_return`，同时保存 `label_horizon_days`；
- 仅当 horizon=10 时，UI/导出才可显示别名 `net_label_10d`；
- h20 数据显示为“净标签收益（20 交易日）”，不得误标为 10 日。

### 7.3 source manifest

新评估必须使用 `hmm_evaluation_source_manifest_v3`；v3 在 v2 universe/asset identity 上新增强制的
market calculator version 与 return/missing content hash。legacy v1/v2 manifest 仅用于历史只读审计：

- pred/label 与其它被引用 QE assets 的 source、URI、SHA256、size、row count（如适用）、
  trust level、zero-copy/fallback 决策；
- base loop、task、loop、实际日期范围和 label horizon；
- 源 loop、stock pool name/filename/SHA256/interval count，以及逐日 eligible-pair hash、symbol count、
  过滤前后 row count；
- QE dataset contract ID、不可变 ST-PIT universe key、rule version、scope、source fingerprint、
  index policy 和 coverage semantics；
- candidate ID、candidate manifest hash、artifact SHA；
- 启用 DB 10 日收益时的 requested policy、resolved as-of、各数据集 max date、共同完成水位、
  calendar range、price row count、return row count、missing-return count/reason counts、字段名
  `market.kline_daily_raw.close_li`、`market_return_calculator_version`、按日期/股票排序后的
  return+missing-evidence content SHA256 和只读 transaction receipt；
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
market return content hash 使用数据库显式 `DOUBLE PRECISION` 计算后的 IEEE-754 hex 值，并包含
`trade_date/symbol/horizon/label_date` 与完整 missing evidence。Phase 1 不为行情创建额外永久副本，
因此保证的是同一来源可访问时的内容校验重放与 drift fail-loud；不得写成“无限期离线可重建”。
缺少新 calculator version/content hash 的既有 market-required 结果标为
`known_invalid/legacy_integer_division_market_returns`，只读审计且不得复用或参与新推荐。

### 7.5 execution purpose 与 benchmark 隔离契约（2026-07-21 用户批准，选项 1A）

同一 `logical_evaluation_key` 可以按 `execution_purpose` 区分两类执行：

- `evaluation`（默认）：普通研究执行，行为与既有语义完全一致；
- `benchmark`：研究验收专用真实重算执行，仍走唯一 API → durable queue → claim → lease →
  fencing → input adapter → evaluator → persist 执行链，禁止任何旁路执行器。

隔离规则（均为强制）：

1. `execution_purpose` 与 `benchmark_id` 持久化在 `offline_evaluation` 与 `batch_test_run`，
   可审计；`benchmark_id` 为 server/CLI 生成的验收标识，普通执行为 null。
2. 普通 `create_or_get` 只查询 `execution_purpose='evaluation'` 的最新 generation；
   benchmark 代际对普通 submission 完全不可见，**不得成为普通 submission 的复用来源**。
3. benchmark submission 对已有 succeeded（普通或 benchmark）logical key 创建
   `max(run_generation)+1` 新代际并真实重算；`run_generation` 序列在两个 purpose 间共享，
   保证 `UNIQUE(logical_evaluation_key, run_generation)` 不冲突。
4. benchmark 失败只终态化其自身代际，**不得遮蔽历史普通 succeeded evaluation**；
   普通查询仍然只返回普通 purpose 的最新代际。
5. `retry-failed` 必须继承原 batch 的 `execution_purpose` 与 `benchmark_id`；
   普通 retry 与 benchmark retry 不得串用（普通 retry 不会重试 benchmark item，反之亦然）。
6. 普通 submission 行为、logical key、request 幂等和 shared evaluation 语义保持不变；
   相同普通请求仍返回原 batch 并标记 `idempotent_replay=true`。
7. 禁止 reset/delete 任何历史 evaluation；禁止通过修改 candidate/spec/hash 制造新
   logical key 伪造“同口径重跑”。
8. 正式 UI 不提供“强制重跑”按钮；benchmark 入口仅为研究验收 API/CLI 参数，
   前端表单不暴露 `execution_purpose`。

benchmark 代际在 API/UI 中可只读查看，其 `execution_purpose=benchmark` 与 `benchmark_id`
必须显式展示，不得与普通评估结果混淆；benchmark batch 内仍按 `hmm_recommendation_v1`
计算 batch-relative 分数，但 benchmark 结果不构成新的研究推荐证据基线。

## 8. Contracts：离线 evaluator

### 8.1 唯一计算路径

新模块 `backend/services/hmm_evolution/evaluator.py` 抽取
`compute_replacements` 的纯计算语义。诊断脚本后续改为调用该模块，不保留第二套实现。
Phase 1 service 不复制诊断脚本的 DB 连接、下载、文件写入或报告生成代码。

### 8.2 排序与 TopK

进入排序前，input adapter 必须先应用 §7.1 的源 loop stock pool ∩ immutable QE ST-PIT 逐日掩码。
被 base pool 排除、已按 PIT 规则进入 ST/退市风险不可买区间或不属于源 loop universe 的股票，
即使存在于 pred/label artifact，也不得参与 raw/adjusted TopK、调入或调出计算。

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
daily_net_label = mean(all entered labels) - mean(all dropped labels)
daily_net_db_10d = mean(all entered db_ret_10d) - mean(all dropped db_ret_10d)
```

只有 entered 与 dropped 两侧的每一只预期股票都有对应收益时，该日才是 comparable day。禁止先
过滤 null 再用剩余子集计算局部均值。逐日必须记录 `calculation_status`：无调仓为
`no_adjustment`，证据齐全为 `computed`，任一预期收益缺失为 `incomplete_evidence`。聚合指标：

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
- 每条缺失收益证据的 date、symbol、entered/dropped side、label/market 类型、horizon、所需起止日期
  和稳定 reason code；
- `evidence_quality=complete|degraded|insufficient` 和结构化 `warnings`。

若 `changed_day_count=0`，评估可以 `succeeded`，但 efficacy 指标为 null、推荐分数为 null，
UI 显示“该候选在此窗口未改变 TopK”，不得显示为收益 0 或通过门禁。

### 8.5 DB 10 日 forward return

- 仅使用 `market.trading_calendar` 的交易日序列和
  `market.kline_daily_raw(ts_code, trade_date, close_li)`；
- `T+10` 是同一 symbol 的第 10 个后续交易日，不是自然日；
- 查询必须批量执行，禁止逐 symbol/date round trip；
- DB 必须为缺失 pair 返回稳定原因：`forward_horizon_not_completed`、`start_price_missing` 或
  `horizon_price_missing`，不能只返回一个缺行计数；
- `mode=required` 时 DB 不可用或覆盖不足导致零 comparable day，评估失败；
- `mode=disabled` 时不查询 DB，`net_db_10d=null`，source manifest 明确记录 disabled；
- 不提供 silent best-effort 模式。

最新行情读取使用 `latest_common_completed`：分别解析 `market.trading_calendar`、
`market.kline_daily_raw` 以及本次显式启用的其它日频数据集最大完成日期，选择共同完成水位；
`market.sw_index_member` 按该日期验证 PIT 映射覆盖率，不伪造日频 max date。该日期在请求入队
时固化为 `resolved_as_of_date` 并进入 input hash。禁止直接使用
`date.today()`、`CURRENT_DATE` 或 worker 开始执行时的动态“最新”。Phase 1 v2 读取日线最新
共同完成数据，不把盘中未完成 bar 当成完整日线。

### 8.6 结果体积

DB 保存聚合指标、逐日摘要、全部 `incomplete_return_evidence`，以及最多 100 条按
`abs(adjusted_rank - raw_rank) DESC, date ASC, symbol ASC` 选取的 deterministic sample。
不持久化全部正常 replacement rows；但缺失证据不得采样或截断，否则 UI 无法解释具体未计算原因。
完整正常结果可由 manifest 重放，避免把数百万行分析明细塞入 DB。

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
metric_availability_ratio = available_weight
```

至少 `net_label_return` 或 `net_db_10d` 有一个非 null 才生成 score；只有 coverage 的候选
保持未排名，但仍完整展示。缺失指标按剩余权重归一化，不填 0、不假装失败；只要发生权重
重归一化，`evidence_quality` 至少为 degraded，排行榜必须显示缺失组件和实际 available weight。
该值只表示公式中可用权重占比，不表示统计置信度、样本可靠性或结果正确概率。数据库历史列名
`evidence_confidence` 可暂作内部兼容存储，但 API/UI 只能暴露 `metric_availability_ratio`。

### 9.3 排名与 top-3

排序键：

1. `recommendation_score DESC NULLS LAST`
2. `metric_availability_ratio DESC`
3. `net_db_10d DESC NULLS LAST`
4. `candidate_id ASC`

取前 3 个有 score 的成功候选作为 top-3；不足 3 个就返回实际数量。未入选候选不标记
rejected，只标记 `is_top3=false`。公式、权重、percentile、组件值和版本全部持久化。

## 10. DB Contracts（数据库详细设计）

### 10.1 bootstrap

目标文件：`backend/db/init_hmm_evolution_schema.py`。

- `SCHEMA_VERSION = "hmm_evolution_v2"`；v1 已部署，v2 forward migration 增加 durable request payload 与 preparation 状态；
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
| identity | `batch_id` PK、`request_hash` UNIQUE、`idempotency_key` nullable UNIQUE、`request_payload` JSONB |
| retry | `retry_of_batch_id` self FK、`retry_generation` INT |
| state | `status`、`owner_id`、`fencing_token`、`lease_expires_at`、`heartbeat_at`、`cancel_requested_at/by`、`row_version` |
| summary | candidate/queued/running/succeeded/failed/cancelled/timed_out count |
| recommendation | `recommendation_spec`/hash、`recommendation_version` |
| error | `error_code`、`reason_code`、`error_context` |
| audit | `created_by`、`created_at`、`started_at`、`completed_at`、`updated_at` |

状态 CHECK：`preparation_queued/preparing/queued/running/cancel_requested/completed/partial_failed/failed/cancelled/timed_out`。

### 10.6 `hmm_evolution.batch_test_item`

| 字段 | 类型 | 语义 |
|---|---|---|
| `batch_id` | TEXT FK | composite PK |
| `candidate_id` | TEXT FK | composite PK |
| `eval_id` | TEXT FK | NOT NULL |
| `ordinal` | INT | request order；UNIQUE(batch, ordinal) |
| `item_status` | TEXT | pending/waiting_shared/reused/queued/running/succeeded/failed/cancelled/timed_out |
| `recommendation_score` | DOUBLE | nullable |
| `evidence_confidence` | DOUBLE | legacy internal storage；API/UI 名称固定为 `metric_availability_ratio`，nullable 0..1 |
| `recommendation_rank` | INT | nullable |
| `is_top3` | BOOL | NOT NULL default false |
| `recommendation_components` | JSONB | nullable |
| error snapshot | code/reason/context | item-readable failure |
| timestamps | created/updated/completed | audit |

`recommendation_*` 属于 batch item，不属于 evaluation。

### 10.7 `hmm_evolution_v3`：execution purpose 列（2026-07-21 批准）

`offline_evaluation` 与 `batch_test_run` 各增加两个最小必要列：

| 字段 | 类型 | 约束/语义 |
|---|---|---|
| `execution_purpose` | TEXT | NOT NULL DEFAULT `'evaluation'`，CHECK IN (`evaluation`,`benchmark`)；普通执行为 `evaluation`，验收真实重算为 `benchmark` |
| `benchmark_id` | TEXT | nullable；`benchmark` purpose 必填，普通执行必须为 null（CHECK 保证一致） |

既有行由 DEFAULT 回填为 `evaluation`/null，不改变任何历史身份；`logical_evaluation_key`、
`UNIQUE(logical_evaluation_key, run_generation)`、`request_hash` 幂等键全部保持不变。

### 10.8 `hmm_evolution.performance_receipt`（2026-07-21 批准，选项 B）

batch/evaluation 两级可审计性能回执；**只能由真实执行阶段写入**，未采集字段为 null/unknown，
禁止伪造 0；terminal evaluation 的 receipt 原子 finalize；failed/timed_out 保留 partial receipt
与失败阶段。完整 receipt 不写入 `metrics_json` 或 `source_manifest`。

| 字段 | 类型 | 约束/语义 |
|---|---|---|
| `receipt_id` | TEXT | PK，`hmpr_` 前缀 |
| `receipt_level` | TEXT | CHECK IN (`batch`,`evaluation`) |
| `batch_id` | TEXT FK | 所属 batch |
| `eval_id` | TEXT FK | evaluation 级必填，batch 级为 null（CHECK 与 level 一致） |
| `execution_purpose` | TEXT | CHECK IN (`evaluation`,`benchmark`)，与所属执行一致 |
| `benchmark_id` | TEXT | nullable；benchmark 必填 |
| `schema_version` | TEXT | receipt payload 版本，当前 `hmm_performance_receipt_v1` |
| `receipt_status` | TEXT | CHECK IN (`partial`,`final`)；崩溃/超时保留 `partial` |
| `cache_state` | TEXT | CHECK IN (`cold`,`warm`,`mixed`,`unknown`)；顶层证据推导，见 §13.4 |
| `cache_evidence` | JSONB | 逐 artifact：`cold_miss/warm_hit/zero_copy_bypass/fallback_download/unknown` + cache root identity |
| `stage_timings` | JSONB | 每阶段 `started_at/completed_at/duration_ms`；阶段清单见 §13.4 |
| `runtime_identity` | JSONB | Python/pandas 版本、进程并发、owner、host、pid |
| `hardware_identity` | JSONB | OS、CPU、物理内存等只读采集 |
| `input_identity` | JSONB | candidate 数、pred/label/selected 行数、日期数、QE task/loop、pred/label SHA256、universe hash、market content hash |
| `peak_rss_bytes` | BIGINT | nullable；执行进程采样峰值，未采集为 null |
| `request_to_terminal_ms` | BIGINT | nullable；请求到终态总耗时 |
| `result_hash` | CHAR(64) | nullable；与 evaluation result_hash 一致 |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT clock_timestamp() |
| `finalized_at` | TIMESTAMPTZ | nullable；终态原子 finalize 时间 |
| `row_version` | BIGINT | NOT NULL DEFAULT 1；每阶段 CAS 更新 |

约束：每个 `(receipt_level, batch_id, eval_id)` 唯一（一个执行一条 receipt）；UNIQUE 部分索引
`(batch_id) WHERE receipt_level='batch'` 与 `(eval_id) WHERE receipt_level='evaluation'`。
全部 schema/table/column 带 COMMENT，bootstrap 幂等且 exact verify。

### 10.9 `hmm_evolution.worker_runtime_status`（2026-07-21 批准）

durable worker 运行态；只查询 batch/evaluation 表不足以证明 idle worker 存活，故独立持久化。

| 字段 | 类型 | 约束/语义 |
|---|---|---|
| `owner_id` | TEXT | PK；一个 worker 进程一个 owner |
| `host` | TEXT | NOT NULL |
| `pid` | INTEGER | NOT NULL |
| `started_at` | TIMESTAMPTZ | NOT NULL |
| `last_poll_at` | TIMESTAMPTZ | nullable；每轮 poll 心跳（含 idle） |
| `last_claimed_batch_id` | TEXT | nullable |
| `last_terminal_batch_id` | TEXT | nullable |
| `consecutive_failure_count` | INTEGER | NOT NULL DEFAULT 0 |
| `runtime_status` | TEXT | CHECK IN (`running`,`stopped`)；崩溃不得伪写 stopped |
| `shutdown_at` | TIMESTAMPTZ | nullable；仅 SIGINT/SIGTERM 正常收敛时写入 |
| `exit_code` | INTEGER | nullable；仅正常退出时写入 |
| `updated_at` | TIMESTAMPTZ | NOT NULL |
| `row_version` | BIGINT | NOT NULL DEFAULT 1，CAS |

支持多 worker，**不增加单实例锁**；status API 只读，按 `last_poll_at`/`updated_at` freshness
推导 `healthy/stale/stopped/unknown`：stale 行不得显示 healthy，崩溃行由 freshness 推导 stale，
API 不得仅凭进程内变量返回成功。只写 `hmm_evolution` research schema。

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
- `create_or_get_submission()` / `claim_batch_preparation()` / `heartbeat_batch_preparation()` /
  `materialize_prepared_batch()` / `fail_batch_preparation()`；
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
preparation_queued ──claim──> preparing ──atomic materialize──> queued
       │                         │                                  │
       └──cancel──────────────> cancelled                           ├──claim──> running ──all success──> completed
                                 └──explicit failure──────────────> failed

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
- evaluation worker 不自动无限重试；evaluation lease 超时进入 timed_out，由显式 retry API 处理。
- preparation lease 过期表示输入尚未物化，安全回到 `preparation_queued`，不能生成空 batch 成功或
  把 receipt 标成 evaluation timed_out；任何准备错误必须落为 batch failed + reason/context。

### 12.4 request 幂等

- `request_hash` 由 submission schema、保持展示顺序的 candidate IDs、evaluation spec、recommendation spec/version 与 created_by 计算；
- 相同请求返回原 batch，响应标记 `idempotent_replay=true`；
- 相同 `Idempotency-Key` 对应不同 request hash 返回 HTTP 409；
- batch candidate IDs 必须去重，原始展示顺序记录在 ordinal，不影响 request hash。

### 12.5 shared evaluation

worker 完成 input freeze 并原子物化 batch 时：

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

每次 benchmark/普通执行在 `hmm_evolution.performance_receipt`（§10.8）记录：硬件身份、
OS、Python/pandas 版本、进程并发、base loop、pred/label/selected 行数、日期数、candidate 数、
QE task/loop 身份、pred/label SHA256、universe hash、market content hash、result hash、
逐 artifact cache evidence、峰值 RSS 和结果 hash。耗时必须至少拆为：API receipt persist、
preparation queue wait、QE/source load、universe resolve、market freeze、evaluation queue wait、
compute、result persist，以及 request-to-terminal 总耗时；只报 `started_at → completed_at`
不足以验收 API 目标。

**写入责任**（每阶段真实写入，CAS `row_version` 递增）：

- API 进程：batch receipt 创建（`receipt_status=partial`）+ `api_receipt_persist` 阶段实测；
- preparation worker：`qe_source_load`、`universe_resolve`、`market_freeze` 与 preparation 完成时间；
- evaluation worker：`evaluation_queue_wait`（`queued_at→claim`）、`compute`、`result_persist`、
  峰值 RSS 采样；terminal 时原子 `finalize`（`receipt_status=final`、`finalized_at`、
  `request_to_terminal_ms`）；
- failed/timed_out/cancelled：保留 `partial` receipt 与已完成的最后阶段，禁止补写未执行阶段。

**cache evidence 语义（2026-07-21 用户批准）**：

逐 artifact 状态（`cache_evidence` 数组元素）：

| 状态 | 语义 |
|---|---|
| `cold_miss` | 应用层 cache root 中不存在，本次真实下载/读取填充 |
| `warm_hit` | 应用层 cache root 命中既有可信 artifact |
| `zero_copy_bypass` | Prediction Store 零副本原位读取，绕过应用层 cache；**不得标为 warm hit** |
| `fallback_download` | store 缺失后 workspace 下载（无论冷热） |
| `unknown` | 无法从证据证明 |

顶层 `cache_state` ∈ `cold/warm/mixed/unknown`，由逐 artifact 证据推导，禁止只给 batch 一个
无证据标签。规则：

- 全部 `zero_copy_bypass` 时顶层为 `unknown`，并注明 `application cache bypassed / OS page
  cache not measured`；OS page cache 未测量时不得声称 OS 级 cold/warm；
- cold 验收必须使用新的 task-scoped cache root（`tmp/hmm_evolution_acceptance/<benchmark_id>/cold-cache`），
  先扫描 reparse point；禁止删除共享 `tmp/hmm_evolution_cache`、Prediction Store 或 QE workspace；
- `cold≈warm` 可以是事实，不作为失败，但必须有逐 artifact 证据；
- 若标准基准全部 zero-copy，必须同时报告：(1) production-normal zero-copy benchmark；
  (2) 选择已批准、可验证的 workspace-fallback loop，用同 candidate + 同 spec 在 task-scoped
  cache root 上实测应用层 cold/warm；不得为获得差异修改业务数据或候选身份。

验收基准：

- 单候选在标准验收基准上冷输入 <10 分钟；
- 10 候选共享输入且受限并发 <30 分钟；
- 未在指定基准和硬件测量时只能报告 pending，不能用小 fixture 宣称性能完成。

2026-07-19 旧同步 preparation 路径的事实记录（非通过回执）：API 返回 202 前约 66～69 秒；
batch `created_at → completed_at` 为 Loop1 146.638s、Loop2 222.352s、Loop3 304.339s、
Loop4 377.703s、Loop5 450.539s、Loop6 522.748s、Loop7 585.680s、Loop8 663.290s、
Loop9 10-candidate batch 990.449s、Loop10 125.938s。Loop8 request-to-terminal 超过 12 分钟；
Loop9 item 195.781s 不能作为单候选测试。BUG-775 改造后必须重新跑同一标准基准，旧数据只用于
指出口径问题，不能证明新路径达标。

### 13.5 自动评估 worker service 生命周期

- 入口必须是显式 `--serve`，与人工 `--once` / `--drain` 共用同一 runtime、repository、lease 和 fencing 契约；禁止无参数隐式进入无限循环。
- service 启动前必须加载 canonical `.env`，并要求 `HMM_EVOLUTION_RUNTIME_MODE=api_worker`；显式进程环境优先于 `.env`，配置缺失或非法时 fail closed 并退出非零。
- queue 非空时连续处理有界 worker slice；queue 为空时使用可中断等待，poll interval 必须有上下界，禁止 busy loop。
- 每轮先回收过期 preparation lease，再优先 claim 一个 `preparation_queued` receipt；准备完成后下一轮
  才 claim queued evaluation。API 和 evaluation worker 不得各自重复读取同一批输入。
- `SIGINT`/`SIGTERM` 只停止接纳下一轮 claim；当前已 claim slice 继续通过既有 checkpoint、heartbeat 和 terminalization 完成，然后进程退出。
- `HMMEvolutionError` 或未知基础设施异常不得被吞掉或伪装为 idle；进程结构化记录 reason code/exception chain 后非零退出，由外部 supervisor 按部署策略重启。
- 自动运行不增加候选审批、评估审批、top-3 阈值或研究停止条件；`api_worker` 仍是 deployment switch/kill switch，而不是产品审批。
- service 不写健康 JSON 文件、不以进程内计数作为权威任务状态；任务真相仍只来自 `hmm_evolution.*`。运行健康通过进程退出码、结构化日志和 API durable status 交叉核对。

### 13.6 durable worker runtime status 与监督（2026-07-21 用户批准）

- worker 进程在启动、每轮 poll（含 idle）、claim、terminal 和正常退出时写
  `hmm_evolution.worker_runtime_status`（§10.9）；心跳间隔不超过 poll interval。
- `SIGINT`/`SIGTERM` 正常收敛后写 `runtime_status=stopped`、`shutdown_at`、`exit_code=0`；
  进程崩溃不得伪写 stopped，由 API 按 freshness 推导 `stale`。
- 连续 worker 级失败递增 `consecutive_failure_count`，成功 claim 后归零；HMMEvolutionError
  与未知异常仍按 §13.5 fail-loud 非零退出。
- 只读 status API（§14.6）返回 durable 行 + 推导健康态：`healthy`（fresh heartbeat）、
  `stale`（心跳超阈值且未正常 stopped）、`stopped`（显式 shutdown）、`unknown`（无 durable 行或
  证据不足）；stale 行不得显示 healthy，API 不得仅凭进程内变量返回成功。
- 日志保持结构化 stdout（含 owner_id/batch_id/reason_code）；本阶段不实现自有
  RotatingFileHandler，日志收集由部署侧负责。
- 不安装 Windows Service、Scheduled Task、开机自启或 watchdog 自动重启；正式服务化安装仍需
  用户单独授权。

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
| POST | `/evaluate` | 单候选；快速持久化 `preparation_queued` batch receipt；返回 202/幂等 200，不读取 QE/market asset |
| POST | `/batch` | 1..50 个去重候选；快速持久化同一 durable receipt，前端按 batch_id 轮询 preparation/evaluation 状态 |
| GET | `/evaluations/{eval_id}` | 状态、指标、daily summary、错误 |
| GET | `/batches` | 分页历史 |
| GET | `/batches/{batch_id}` | batch、items、top-3、progress |
| POST | `/batches/{batch_id}/cancel` | 幂等取消请求 |
| POST | `/batches/{batch_id}/retry-failed` | 新 batch/generation，只重试失败项；继承原 `execution_purpose`/`benchmark_id`，普通与 benchmark 不串用 |

`/evaluate` 与 `/batch` 请求体接受可选 `execution_purpose`（默认 `evaluation`）与
`benchmark_id`（purpose 为 `benchmark` 时必填且必须服务端可审计生成或校验）；前端表单不暴露
该参数，benchmark 仅用于研究验收 API/CLI。`GET /evaluations/{eval_id}` 与
`GET /batches/{batch_id}` 响应附带对应 `performance_receipt`（batch 级与 evaluation 级），
包含 `cache_state/cache_evidence/stage_timings/peak_rss_bytes/request_to_terminal_ms`；
无 receipt 的历史行显式返回 null 并标注 `receipt_unavailable`，不得伪造字段。

### 14.6 worker status（只读）

| 方法 | 路径 | 语义 |
|---|---|---|
| GET | `/workers` | 列出 durable worker runtime status 行，附 freshness 推导的 `health=healthy/stale/stopped/unknown`、freshness 阈值与依据时间戳；只读，不凭进程内变量返回成功 |

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
5. **历史排行榜**：推荐分数、指标可用比例、净标签收益（动态 horizon）、Net DB 10D、正值日比例、coverage；不得把 available weight 称为置信度；
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
- 逐日 `replacement_count=0` 必须显示“当日无调整”；changed day 的完整收益缺失必须显示
  “证据缺失”并在结构化表中列出 symbol、调入/调出、所需日期和原因，不能统一显示“未计算”。
- HMM 模块自己的研究导航不能替代应用全局左侧导航；`/hmm-evolution` 及其 batch/evaluation 子路由
  必须始终保留全局左侧导航。

### 15.4 加载、空态、降级、失败和轮询

| 状态 | 页面行为 |
|---|---|
| loading | 显示有界 skeleton 和正在加载的资源名；超过客户端阈值进入 timed-out，不无限旋转 |
| preparation_queued/preparing | 明确显示“等待输入冻结/正在冻结输入”，evaluation 列表可暂为空；不得把 receipt 当作已完成输入校验 |
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
| preparation worker 崩溃/lease 过期 | receipt 回到 preparation_queued 并保留审计；不得创建空 evaluation 或假成功 |
| 旧整数除法行情结果 | known-invalid/view-only；Net DB 10D 与推荐不可复用，详情页显式解释 |
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

### 17.4.2 2026-07-19 Phase 1 worker runtime activation receipt

- **运行环境**：Windows 独立 Python 进程显式运行
  `scripts/hmm_evolution_worker.py --serve --owner-id service-aistock-recovery --poll-seconds 5`；
  worker 未嵌入 FastAPI startup，也未注册 Phase 3 scheduler。
- **API 与请求**：重启后的 `/api/v1/health` 返回 200；以候选
  `hmmc_51125769a3e34f2a8dee4888`、`qe_20260705_004409_4437/Loop10`、
  `hmm_evaluation_spec_v2`、TopK 46、label horizon 20 提交，HTTP 202，trace
  `66b589b23be549e5b53fb9728614cb6a`。
- **durable queue**：batch `hmmb_4a5f9f9b7c064c5287829c35c23f1177` 被 worker 自动认领；
  evaluation `hmme_7bc7478f392548b2952507530c42d7a8` 在约 121 秒后 succeeded，
  1 succeeded / 0 failed / 0 timeout，243 个交易日、217 个 changed days。
- **冻结 universe 身份**：`filtered_pool_20260428 ∩ shsz_st_pit_active_v1`，4497 symbols、
  5161 ST-PIT spans；binding mode 为 `legacy_frozen_runtime_artifact_v1`，风险策略 artifact
  `qe_event_risk_policy.json` SHA256 为
  `8f1a09a0e6e9fba0e5f9e0eb62ad2af02a91ea1059cbdec0257f287c556d4942`。
- **用户点名日期复核**：2025-02-17、2025-03-18、2025-04-03、2025-04-11 均产生
  entered/dropped、label 和 market return 指标，不再显示为“未计算”。
- **显式降级而非静默成功**：结果 `evidence_quality=degraded`，结构化记录 3 条 label artifact
  缺失和 4 条 market horizon price 缺失；primary coverage 98.62%。该 receipt 证明 BUG-772
  和首次 worker activation 通过，不将剩余数据证据缺口伪装为 complete。
- **后续状态**：真实 10-case、10 候选、pre-ST-PIT 兼容重放及进程中断 fail-closed/显式 retry
  已在后续 receipt 完成；严格冷热缓存分段 timing/RSS、长期服务监督和真实 UI/Playwright 仍待完成，
  不得据此宣称整个 Phase 1 verified。

### 17.4.3 2026-07-20 schema v2 DEV-first 生产 DDL receipt

- **DEV 实施**：`127.0.0.1:5433/aistock_dev` 幂等执行 `hmm_evolution_v2` bootstrap，
  exact column/constraint/index/comment verify 通过；真实 PostgreSQL repository/market integration
  `2 passed`。
- **生产 preflight**：目标固定为 `127.0.0.1:5432/aistock`；迁移前只有 v1 version row，
  `request_payload` 不存在，24 个 completed batch、1 个 timed_out batch，活动 batch 为 0。
- **生产实施与回读**：单事务升级后 v2 version row 从 0→1，`request_payload JSONB NOT NULL
  DEFAULT '{}'`、`preparation_queued/preparing` 状态约束及全部 schema/table/column comments
  exact verify 通过。
- **业务数据不变量**：迁移前后 candidate/evaluation/batch/item 分别为 10/44/25/44 行；
  忽略预期新增列后的逐表 SHA-256 完全一致，历史 batch 状态保持 24 completed / 1 timed_out；
  repository read smoke 通过。
- **运行态边界**：未重启 API、worker、8001/3000/19080 或其它生产服务；DDL applied 不等于
  v2 runtime 已激活，必须在后续显式重启后另行验证新 durable preparation receipt。

### 17.4.4 2026-07-21 schema v2 worker 与 10-case receipt

- **runtime**：独立 worker PID 74728 以 5 秒 polling 运行，加载 BUG-788/BUG-789 后代码；验证结束时
  活动 batch 为 0，worker 日志无 ERROR/Traceback/CRITICAL。
- **BUG-788**：retry batch `hmmb_0926ae50211f4952bde206425b4b48f0` completed；evaluation
  `hmme_e585d6adc32742f9b8fb375a3f571db3` 55.398 秒完成，market transaction read-only，
  `write_relations=[]`，未再出现 QueryCanceled。
- **10-case**：`qe_20260705_004409_4437/Loop1`～`Loop10` 同口径 v2 evaluation 全部 succeeded，
  每例 243 个交易日、69.3～99.3 秒；pred/label `zero_copy=true`、`fallback=false`，market content
  hash 一致。Loop2 evidence complete；其余 partial label/market evidence 以结构化 degraded warning
  显式保留。
- **10 候选**：既有 batch `hmmb_e2ac69e2e21a474e9044afa34a8f580b` 10/10 succeeded，约
  12 分 37 秒；仍需后续刷新严格冷热缓存分段 receipt，不影响其已低于 30 分钟的事实记录。
- **BUG-798 后续**：BUG-800 修复 path-free compatibility receipt，BUG-801 保证单项 preparation
  失败终态化后 worker 继续服务，BUG-804 将动态 acquisition `fallback` 从 immutable artifact identity
  比对中移除，但保留 source/URI/SHA256/size/row_count/selected_row_count/zero_copy 强校验。
- **pre-ST-PIT 真实回执**：batch `hmmb_66db955297e6440283097e6fdfb927ac` 在
  `qe_20260502_131502_9b54/Loop1`、2024-07-01～2026-04-27 窗口上 9/9 succeeded，约 24 分 4 秒；
  pred SHA256 `24ca37fc573f57b0c1759501af7b0b17e4cf02c8fbf97144e49c73696a694da6`、label SHA256
  `cf258ca77dd03f512e1587ac7a3a72903e431f6ba6400a53c47b92d832a55ec6`、allowlisted donor
  `qe_20260705_004409_4437/Loop10` 的 ST-PIT artifact SHA256
  `8f1a09a0e6e9fba0e5f9e0eb62ad2af02a91ea1059cbdec0257f287c556d4942` 均被 manifest 固化；
  market transaction 为 read-only、`write_relations=[]`，9 个结果均 `result_validity=valid`。

### 17.4.5 2026-07-21 worker 进程中断与显式 retry receipt

- **权威语义**：本节按 §12.3 执行；evaluation lease 过期必须进入 `timed_out`，旧 terminal row
  不原地复活，也不自动无限重试。恢复动作必须通过 `/batches/{batch_id}/retry-failed` 创建新
  `run_generation`。总体蓝图中旧“lease recovery=自动重新认领”措辞不符合本详细设计，已同步修正。
- **受控中断**：batch `hmmb_39fe2314e09041a9a056467a87d4fb46` 含 3 个真实候选；evaluation
  `hmme_94e3084d31e84745ac8a049c4d4c47ac` 已为 `running`、owner
  `service-aistock-post-bug804`、`fencing_token=1`、`attempt_count=1` 后中断唯一 worker PID 73948。
  新 worker PID 37024 以 owner `service-aistock-lease-recovery-20260721` 启动；90 秒 lease 到期后，
  两个旧 evaluation 明确终态化为 `timed_out`，token/attempt 未被伪造递增，第三项由新 worker
  succeeded；原 batch 为 `partial_failed`（1 succeeded / 2 timed_out）。
- **显式恢复**：retry batch `hmmb_9e1d0eaf43d1432bb1cbbbba53cca5b6` 的
  `retry_of_batch_id` 指向原 batch、`retry_generation=2`，只包含两个 timed_out 候选；两项均
  succeeded、`result_validity=valid`，总耗时约 4 分 50 秒，旧超时记录保持不可变。
- **运行边界**：只中断/重启 HMM Evolution worker；API、前端、TDX、QE/Paper/Selection 配置均未
  重启或修改，无 DDL。该回执验收 fail-closed lease/fencing + explicit retry，不把它误报为自动接管。
- **仍未完成**：当前 API receipt 未记录 §13.4 要求的全部阶段 timing 与 peak RSS，故严格冷/热缓存
  benchmark 仍 pending；本窗口没有可用浏览器实例，真实页面截图/Playwright 仍 pending，不能用
  mock 或源码 contract 测试替代。

### 17.4.6 2026-07-22 Phase 1 收官验收 receipt（性能 benchmark / 真实 UI / worker soak）

- **验收环境与边界**：worktree `hmm-phase1-performance-ui-supervision-20260721`（branch
  `feature/hmm-phase1-performance-ui-supervision-20260721`）；backend 8011 + frontend 3011 +
  DEV `aistock_dev`（127.0.0.1:5433）；task-scoped cache root `F:/Dev/hmm_acceptance_20260721/`。
  生产端口 8001/3000/19080 未启动/停止/重启；生产 HMM Evolution worker 未触碰；生产 DB
  全程只读、零 DML/DDL；`hmm_evolution_v3` DDL 仅 DEV 执行，生产 v3 `pending`（未授权、未执行）。
- **DEV 数据写入事实**（如实记录，替代早期"DEV 只写 hmm_evolution.*"的绝对表述）：
  初始约束仅允许 DEV `hmm_evolution.*` 验收写入；随后获得战略 session 逐项明确授权
  （`GO DEV SEED FULL`，2026-07-21 NEED-HUMAN 批准追加 `model_train_configs` 与
  `qe_experiments`；2026-07-22 "Loop1 + h10 spec"裁决追加 fallback loop 引用行），由
  `scripts/dev_db/seed_hmm_benchmark_reference_20260721.py` 向 DEV 种子 benchmark 引用数据。
  production source 全程只读（`default_transaction_read_only=on`）；仅 DEV DML（insert-only、
  ON CONFLICT DO NOTHING、无 TRUNCATE/DELETE/DROP/DDL）。实际写入的 canonical relations 与
  行数（2026-07-22 DEV 只读回数，`current_database()=aistock_dev` 守卫）：
  `market.kline_daily_raw` 2,796,553 行（trade_date ≥ 2024-06-03）、`market.sw_index_member`
  7,053 行（全表 PIT spans）、`public.model_train_configs` 10 行、
  `public.model_train_snapshots` 10 行、`infra.compute_nodes` 2 行（wsl2-5080、rdagent-node1）、
  `public.qe_experiments` 4 行（2026-07-21 授权 benchmark 2 行 + 2026-07-22 裁决 fallback 2 行，
  均 NULL task_id/round_id）、`public.qe_evolution_tasks` 2 行、`public.qe_evolution_loops` 2 行。
  `infra.compute_nodes` 遥测列按批准的差异规则处理：稳定列必须一致（divergent=list+STOP），
  volatile 列（last_heartbeat/updated_at/status）可从只读 source 刷新并前后对照报告，DEV-owned
  列（current_task_id/metrics_snapshot）永不从生产复制、新行置中性值。生产无任何 DML/DDL；
  仓库未提交任何数据 dump/CSV/parquet（seed 仅含 WHERE 口径与行数估计，不含数据本体）。
- **fallback loop 裁决登记（用户 2026-07-22 批准）**：canonical loop
  `qe_20260705_004409_4437/Loop10` 的 workspace 无任何远端 manifest（三探针全 404），且不在
  legacy 白名单，manifest trust gate 按设计 fail-closed（`DateRangeError`，非代码缺陷）；设计
  明确不扩 pred/label 白名单。用户裁决"Loop1 + h10 spec"：cold/warm benchmark 使用唯一已批准
  legacy loop `qe_20260502_131502_9b54/Loop1`，spec 与 canonical 完全一致、仅
  `label_horizon_days=10`（匹配该 loop LABEL0 真实 h10 语义，mlruns param `label_horizon=10`
  佐证；label.pkl 无 horizon 列，由 spec 解释）。偏离"同 spec"仅此一项且被语义强制；cold/warm
  测的是同一条 workspace 下载代码路径；生产 pre-ST-PIT 9/9 h10 端到端已证（§17.4.4）。
- **benchmark 矩阵**（全部 batch `completed`、receipt `final`、两级分阶段 stage 齐全、
  cold/warm `result_hash` 完全一致 `2a50dd3c…` → 同口径确定性重算）：

  | benchmark | batch | batch rtt | eval rtt（compute） | peak RSS | cache 证据 |
  |---|---|---|---|---|---|
  | `bench_zerocopy_1c_20260721` | `hmmb_484121afc751408a8a9c1ba5f8da8e01` | 154,942ms | 97,098ms（96,656ms） | 830,435,328B | zero_copy_bypass×2 → 顶层 `unknown` |
  | `bench_zerocopy_10c_20260721` | `hmmb_3a5d2b2ead104689a6423cbfd1f61699` | 851,507ms | 152,581–784,008ms | max 1,392,398,336B | zero_copy_bypass×2；11 receipts（1 batch + 10 eval）、0 orphan |
  | `bench_cold_1c_20260721`（fallback spec） | `hmmb_cc38ea9f441341baa820acfde27817aa` | 130,213ms（含 qe_source_load 17,093ms 真实 16.4MB+24.6MB 下载入 `cache_cold1` 并落盘 manifest） | 68,424ms | 834,228,224B | fallback_download×2 → 顶层 `cold` |
  | `bench_warm_1c_20260721`（fallback spec） | `hmmb_556e3c3371544d3f88be9e36b47fc33b` | 143,863ms | 92,496ms | 833,007,616B | warm_hit×2（两级）→ 顶层 `warm` |

  两级 receipt 语义实测一致：cold run 的 evaluation 级 receipt 显示 `warm_hit` 是 per-stage
  真实（preparation 下载后 compute 读热缓存）；benchmark 的 cold/warm 身份由 batch 级 receipt
  承载（§13.4 已登记语义）。失败 batch 保留 `partial` receipt 为设计内 orphan，验收断言只针对
  completed batch。
- **真实 UI/Playwright 验收**：`frontend/tests/hmm-evolution/hmm-evolution-real-acceptance.spec.ts`
  18 场景全部通过（无 route mock、无 fixture；`page.on('request')` 生产端口 8001/3000/19080
  守卫先行失败）；backend 8011 / frontend 3011（playwright webServer 以
  `NEXT_PUBLIC_API_BASE=127.0.0.1:8011` 启动，覆盖指向生产的 `.env.local`）；18 张全页截图归档
  `tmp/hmm_ui_acceptance_20260722/`（3 页：演进实验室/批次详情/评估详情；含 10 候选 completed
  批次、失败批次、retry generation 2、终态轮询停止、h20/h10 动态 horizon、243 交易日逐日表、
  不存在 evalId 的 VisibleErrorState、导航边界与可访问性）。验收中发现的独立问题并已修复：
  `Sidebar.tsx` 告警轮询使用 `NEXT_PUBLIC_TDX_BACKEND_BASE`，未覆盖时 `.env.local` 生产值生效
  会向 8001 发只读 GET；`playwright.config.ts` webServer env 增加该变量覆盖（未形成生产写入，
  守卫先行失败）。
- **worker 长期监督 soak**：owner `hmm-dev-soak-20260722`，2026-07-21 19:10:42→19:42:19 UTC
  （31.6 分钟 bounded，`--serve --poll-seconds 5`）：idle 阶段 5 秒 cadence poll 持续推进
  `worker_runtime_status.row_version`；claim+success（`hmmb_b4e01bb2…` completed 1/1）；
  failure（`hmmb_ea62605f…` `hmm_evolution_source_unavailable`，`consecutive_failure_count`
  1→2）；显式 retry（`hmmb_30b1413c…` `retry_generation=2` 确定性再失败，链上
  `retry_of_batch_id` 正确）；timeout（kill 运行中 worker，evaluation lease 19:32:12 过期后
  1 秒内被重启 worker 的 `mark_expired_leases_timed_out` 终态化为 `timed_out`、reason
  `hmm_evolution_evaluation_timed_out`，batch `hmmb_ed0a1a93…` 派生 `timed_out`）；同 owner-id
  重启干净 re-upsert（新 pid 47956）；dedup 副证：同 candidate+canonical spec 的
  `hmmb_f2bea8a4…` 即时 completed（§7.5 logical key 复用语义）。bounded soak 结束为强制终止
  （本环境无法向 harness 持有 PID 发 SIGTERM）；crash-safety 已由 timeout 场景证明。

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

当前状态：P1-A/P1-B/P1-C 源码、审计硬化与外部验收全部完成；覆盖共享输入并发、lease/fencing fail-closed + explicit retry、QE 权威节点、内容安全、全资产浏览、UI 状态机与异常可观测性；
`hmm_evolution_v1/v2` 的 `production_ddl_gate=applied_and_verified`（2026-07-20，DEV/production exact verify 与受保护数据摘要校验通过，历史事实）；
v2 `runtime_activation_gate=applied_and_verified`。2026-07-21 独立 Windows worker 已加载 schema v2 与 BUG-800/BUG-801/BUG-804 后代码；Loop1～Loop10、pre-ST-PIT 9 候选和进程中断后的显式 retry receipt 均成功，活动队列归零。既有 10 候选与 pre-ST-PIT 9 候选 batch 均在 30 分钟内完成；严格冷热缓存分阶段 timing/RSS、长期服务监督和实机页面/Playwright 已由 §17.4.6 收官验收补齐。
2026-07-22 production v3 更新：收官验收引入的 `hmm_evolution_v3`（`execution_purpose`/`benchmark_id` 列、`performance_receipt` 与 `worker_runtime_status` 表及配套约束/索引/COMMENT/schema_version row）已在 DEV 和 production `applied_and_verified`。Production 在活动 batch 为 0 时执行单事务 bootstrap + transaction 内 `verify_schema`，独立只读 exact verify 通过，17 candidate、97 evaluation、53 batch、98 item 的受保护内容摘要不变；随后经独立授权重启 worker，owner `service-aistock-hmm-v3-20260722` 的 `/workers` 状态为 healthy/running、连续失败 0、活动 batch 0。代码合入、DDL 与 runtime activation 仍分别记录。

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
- 显式 `--serve` 自动评估 worker service、可中断 idle polling、信号收敛和 fail-loud 进程退出；
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
6. 合入自动评估 worker service，但保持 runtime activation 状态独立；
7. 获得一次生产 runtime activation 操作授权后按部署配置启动独立 worker service；API/worker/nav 状态分别记录，
   但不得衍生三套产品审批流。

### 19.2 activation flags

- `HMM_EVOLUTION_RUNTIME_MODE=disabled|api_only|api_worker`，默认 disabled；
- HMM 演进页面与左侧导航无前端 env gate，构建后默认可见；后端 runtime mode 只控制真实 API/worker 能力，
  页面必须以可见错误说明 runtime disabled，不能隐藏功能入口；
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
| `production_ddl_gate` | noop | v1/v2 production `applied_and_verified`（2026-07-20 历史事实，DEV-first receipt 见 §17.4.3）；v3 DEV/production `applied_and_verified`（2026-07-22，单事务 DDL + exact verify） | noop | 同 P1-A：v1/v2/v3 已应用并验证；DDL 与 runtime activation 分别留证 |
| `production_backend_dependency_gate` | noop | noop | noop | noop |
| `production_frontend_dependency_gate` | noop | noop | noop | noop |
| `production_runtime_activation_gate` | noop | v2：旧 schema worker 已受控 activation（历史事实）；v3：`applied_verified`（2026-07-22 独立授权重启，durable worker healthy/running） | 同左 | 不新增业务审批流 |
| `data_write_gate` | docs only | 仅 hmm_evolution.*（runtime 写 allowlist） | 仅评估结果 | 同左 |
| `service_start_gate` | noop | 禁止隐式启动 | 禁止隐式启动 | 独立 service 显式启动；不得挂入 FastAPI startup，安全验证与生产启动分离 |
| `design_compliance_gate` | F2 validator | DESIGN-COMPLIANCE-001 | 同左 | 同左 + UI evidence |

2026-07-20 v2 DDL 事实与数据不变量见 §17.4.3：v1/v2 生产 schema `applied_and_verified` 为历史事实；v2 API/worker 已受控 activation。
2026-07-22 v3 事实：v3 DEV `applied_and_verified`（§17.4.6 全部 benchmark/UI/soak 证据均在 DEV v3 schema 上产生）；production 获得 `GO PRODUCTION DDL HMM EVOLUTION V3` 后完成单事务 DDL 与 exact verify，随后获得独立 worker restart 授权并完成 durable healthy/running 回读。`production_ddl_gate=applied_verified`、`production_runtime_activation_gate=applied_verified`，dependency gates `noop`；未启用 Phase 2/3 scheduler，也未接入 QE/Paper 生产链。

## 22. Design Acceptance Index（设计验收索引）

- **F-006 Phase 1 独立候选注册表**：QE 全资产只读 reader、内容寻址 candidate、可信
  coefficient manifest、research-only lifecycle、只写 `hmm_evolution.*`。
- **F-007 Phase 1 内容校验重放**：显式 horizon、QE asset trust receipt、latest-common watermark、
  source/candidate/spec/evaluator hash、版本化行情值/缺失证据 content hash、稳定排序、交易日 forward return 和 result hash；不夸大为永久离线重建。
- **F-008 Phase 1 批处理状态机**：durable receipt、preparation_queued/preparing 输入冻结、batch/evaluation/item 分层、幂等、lease、fencing、heartbeat、
  取消、超时、重试、shared result 和 partial failure。
- **F-009 Phase 1 推荐语义**：`hmm_recommendation_v1` 版本化、batch-relative、无淘汰阈值、
  top-3 仅为 QE 终审前研究推荐。
- **F-010 Phase 1 API/UI**：真实 QE asset/candidate/evaluation/batch API、中文演进实验室、
  HMM research navigation shell、动态 horizon、schema-aware asset view、主视图 degraded warning、
  固定证据区、独立详情页和可见终止错误；禁止 Paper v2、抽屉式列表、raw JSON 主视图和未实现 tab。
- **F-010A Phase 1 自动评估 worker service**：显式 `--serve` 独立进程自动消费既有 durable queue；
  canonical env、poll bounds、idle wait、signal shutdown、lease 超时 fail-closed、显式 retry 和
  fail-loud exit 完整；不创建任务、不嵌入 FastAPI、不接入 Phase 3 scheduler。

## 23. Design Acceptance Matrix（设计验收矩阵）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-006 | 本文 §5.3、§6、§10、§11；`backend/services/hmm_evolution/{qe_asset_reader,candidate_artifact,models,errors,repository,service}.py`、`backend/services/quantevolver/qe_workspace_client.py`、`backend/db/init_hmm_evolution_schema.py`；RD-Agent PR #4 `qe_workspace_catalog.py` + exact `GET .../files` route | `python -m pytest backend/tests/hmm_evolution/test_qe_asset_reader.py backend/tests/hmm_evolution/test_candidate_artifact.py backend/tests/hmm_evolution/test_repository_integration.py -q`；真实 `qe_20260706_013235_bbd4/Loop8`：221 unique relative assets、complete catalog、pred/label read receipts、zero extra copy；生产 schema verify receipt 见 §17.4.1/§17.4.3 | verified | 无 |
| F-007 | 本文 §7、§8；`evaluator.py`、`input_adapter.py`、`market_repository.py`、`source_manifest.py`、`universe.py`；BUG-772～BUG-774、BUG-788、BUG-798、BUG-800、BUG-804 | `python -m pytest backend/tests/hmm_evolution/test_market_repository.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_universe.py -q`；Loop1～Loop10 market hash/read-only/zero-copy receipt；§17.4.4 pre-ST-PIT batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded，donor/artifact/source/market hash 回读通过 | verified | 无 |
| F-008 | 本文 §7.5、§10～§13；`repository.py`、`service.py`、`worker.py`、`input_adapter.py`、`models.py`；BUG-775、BUG-801；2026-07-21 新增登记：`execution_purpose`/`benchmark_id` 隔离（选项 1A）、`performance_receipt` 分阶段采集 | `python -m pytest backend/tests/hmm_evolution/test_service.py backend/tests/hmm_evolution/test_repository_integration.py backend/tests/hmm_evolution/test_schema_and_state_machine.py -q`；10 候选 10/10 succeeded；§17.4.5 中断 batch 1 succeeded/2 timed_out，retry generation 2 为 2/2 succeeded；§17.4.6 benchmark purpose 隔离下 zerocopy 1c/10c、fallback cold/warm 全 matrix 真实 receipt（分阶段 timing + peak RSS + per-artifact cache evidence + result_hash 确定性） | verified | 无 |
| F-009 | 本文 §9；`scorer.py`、`repository.py::_apply_recommendations_with_cursor()`；BUG-776 | `python -m pytest backend/tests/hmm_evolution/test_scorer.py backend/tests/hmm_evolution/test_repository_integration.py -q`；metric availability、percentile/tie/missing renormalization/stable top-3；历史污染推荐 known-invalid/view-only | verified | 无 |
| F-010 | 本文 §14、§15；真实 API/UI 路径；BUG-744～BUG-748、BUG-770～BUG-772、BUG-788/BUG-789；2026-07-21 新增登记：evaluation/batch 响应附 `performance_receipt`（§14.3）、worker status 只读端点（§14.6）、UI receipt 展示（cache_state/peak RSS/阶段耗时） | `python -m pytest backend/tests/hmm_evolution/test_api.py backend/tests/hmm_evolution/test_qe_workspace_client_catalog.py backend/tests/hmm_evolution/test_frontend_contract.py -q`；2026-07-21 Loop1～Loop10 同口径 evaluation 全部 succeeded，单例 69.3～99.3 秒，degraded evidence 显式；§17.4.6 真实 UI/Playwright 18 场景（8011/3011，无 mock，生产端口守卫）全过，18 张截图归档 | verified | 无 |
| F-010A | 本文 §5.1、§13.5、§13.6、§17.4.2、§17.4.5、§17.4.6、§18～§21；`worker_service.py`、`hmm_evolution_worker.py --serve`、`worker_runtime_status` durable 监督（2026-07-21 登记）；只读 `/workers` status API | `python -m pytest backend/tests/hmm_evolution/test_worker_service.py backend/tests/hmm_evolution/test_worker_cli.py -q`：22 passed；中断旧 PID 73948 后新 PID 37024 保持服务，过期 evaluation 明确 timed_out，显式 retry 2/2 succeeded，活动队列归零；§17.4.6 31.6 分钟 bounded soak：idle/claim/success/failure/retry/timeout 六类事件真实发生并被 durable 监督记录 | verified | 无 |

## 24. 设计结论

P1-A 的 QE 全资产只读 reader、candidate identity 与 schema v2 已完成源码、真实 DEV PostgreSQL、
生产 schema、真实 QE workspace 与重启后 runtime 外部验收；schema v3 已在 DEV
`applied_and_verified`，production 仍为 `pending`。P1-B 已实现 pure evaluator、Phase 0 source
manifest adapter、latest-common/交易日收益只读 repository、durable executor、batch-relative
recommendation scorer，并通过 BUG-736/BUG-737 完成旧诊断唯一计算路径迁移；BUG-773/BUG-774
修复了行情收益整数除法并将重放收紧为 content-verified/fail-on-drift，旧 market-required receipt
只读无效。P1-C 的 API/UI/worker 源码及审计硬化、受控 10-case、10/9 候选性能、pre-ST-PIT
compatibility、进程中断 fail-closed 与显式 retry、严格冷热缓存分阶段 timing/RSS、durable worker
监督和真实页面/Playwright 18 场景均已完成外部验收。因此 F-006～F-010A 在“实现 + DEV 外部验收”
边界内全部 verified；2026-07-22 后续独立 deployment receipt 又证明 production schema v3 已应用且 production worker 已加载新版本。以上仍不表示 Phase 2/3 已启用，或 HMM 已接入 QE/Paper 生产链。

## 25. 变更记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v2.8 | 2026-07-22 | 回填 production v3 deployment receipt：经精确授权完成单事务 DDL 与独立 exact verify，17 candidate、97 evaluation、53 batch、98 item 的受保护内容摘要不变；随后经独立授权重启 HMM worker，durable `/workers` 状态为 healthy/running、连续失败 0、活动 batch 0。将 `production_ddl_gate` 与 `production_runtime_activation_gate` 分别更新为 `applied_verified`；dependency gates 保持 `noop`，Phase 2/3 与 QE/Paper 接入仍未启用。 |
| v2.7 | 2026-07-22 | BUG-821：将设计权威引用更新到总体蓝图 v2.7，并修正 §24 的旧验收缺口结论，使其与顶部状态、§17.4.6 和 F-006～F-010A 验收矩阵一致；继续明确 Phase 1 verified 仅覆盖实现与 DEV 外部验收，production schema v3、production runtime activation 以及 Phase 2/3 均未据此完成。 |
| v2.6 | 2026-07-22 | 按只读复核 NEED-FIX 修正 gate truth 与 DEV 写入事实：`hmm_evolution_v1/v2` production `applied_and_verified` 保留为历史事实；明确 `hmm_evolution_v3`（`execution_purpose`/`benchmark_id` 列、`performance_receipt`、`worker_runtime_status`）DEV `applied_and_verified`、production `pending`（未授权、未执行），`production_ddl_gate=pending`、`production_runtime_activation_gate=pending`、dependency gates `noop`（§21、§18 P1-A 状态）；§17.4.6 以授权时间线 + 八张 canonical relations 精确行数 + compute_nodes 遥测差异规则如实记录 DEV 种子写入，替代"DEV 只写 hmm_evolution.*"的绝对表述；登记 v3 生产 DDL 须单独获得 `GO PRODUCTION DDL HMM EVOLUTION V3`。 |
| v2.5 | 2026-07-22 | 回填 Phase 1 收官外部验收（§17.4.6）：登记用户"Loop1 + h10 spec"fallback 裁决（canonical Loop10 workspace 无远端 manifest、trust gate 按设计 fail-closed、不扩白名单）；zerocopy 1c/10c 与 fallback cold/warm 全 benchmark matrix（分阶段 timing、peak RSS、per-artifact cache evidence、cold/warm result_hash 一致）；真实 UI/Playwright 18 场景（含 Sidebar `NEXT_PUBLIC_TDX_BACKEND_BASE` 覆盖修复）；worker 31.6 分钟 bounded soak 六类事件；F-008/F-010/F-010A 标记 verified。 |
| v2.4 | 2026-07-21 | 登记用户批准的 Phase 1 收官裁决：execution purpose 隔离（§7.5，选项 1A，benchmark 对 succeeded logical key 开 max+1 generation、普通 create_or_get 仅见 evaluation purpose、retry 继承 purpose 不串用）；`hmm_evolution.performance_receipt`（§10.8，选项 B，batch/evaluation 两级、分阶段 CAS 写入、失败保留 partial）；逐 artifact cache evidence 语义（§13.4，zero_copy_bypass 不得标 warm、全 zero-copy 顶层 unknown、task-scoped cold cache root）；durable `worker_runtime_status` 与只读 `/workers` API（§10.9/§13.6/§14.6，freshness 推导 healthy/stale/stopped/unknown，不装系统服务）；schema v3 bootstrap DEV-first，生产 DDL 独立 pending。 |
| v2.3 | 2026-07-21 | 回填 BUG-800/BUG-801/BUG-804 后 pre-ST-PIT 9/9 真实回执；明确 worker 崩溃时 evaluation lease 过期按批准状态机进入 timed_out，显式 retry 创建 generation 2 并 2/2 succeeded，不自动复活旧终态；保留严格冷热缓存、长期监督与真实 UI/Playwright 缺口。 |
| v2.2 | 2026-07-21 | BUG-798：为唯一批准的 pre-ST-PIT legacy loop 增加 donor identity 与 config/pool/artifact/coverage 全量固定的 cross-loop immutable compatibility receipt；新增 binding/coverage provenance，未登记或漂移继续 fail loud，不读取 live ST、不扩 pred/label 白名单。 |
| v2.1 | 2026-07-20 | 回填 `hmm_evolution_v2` DEV-first 生产 DDL：exact schema/comment verify、受保护数据摘要不变与 repository read smoke；将 runtime 状态纠正为“v2 后尚未重启”，不把旧 worker receipt 误作新代码已加载 |
| v2.0 | 2026-07-19 | 对齐 BUG-773～BUG-777：显式浮点行情收益、版本化 return/missing content hash、known-invalid 历史结果、durable preparation receipt/状态机、`metric_availability_ratio`、真实性能分段口径、legacy ST-PIT 兼容约束、移除前端 env gate；schema v2 生产 DDL/重启独立 pending |
| v1.9 | 2026-07-19 | 回填 BUG-772/PR #2471 与首次 Windows worker runtime activation：真实 Loop10 v2 API 202、冻结 ST-PIT universe、约 121 秒 queue terminal receipt和显式 degraded evidence；保留 10-case、10 候选、lease recovery 与 UI/Playwright 缺口 |
| v1.8 | 2026-07-18 | 批准 Phase 1 自动评估 worker service：显式 `--serve` 独立消费 durable queue，固化 canonical env、poll bounds、信号收敛、fail-loud 退出和与 Phase 3 scheduler 的隔离；新增 F-010A |
| v1.7 | 2026-07-18 | 回填 BUG-742～BUG-748 审计修复：有界并发共享输入、lease reaper、QE 权威节点、内容安全、全资产 schema-aware 浏览、UI fail-loud 状态机和 idempotency；F-008/F-010 标记为源码完成但外部验收待补，不提前宣称 Phase 1 完成 |
| v1.6 | 2026-07-18 | 回填 P1-C API/UI/worker 实现路径、本地 contract/TypeScript/Next build 证据和仍待完成的真实 UI/10-case/性能外部验收；将 F-006/F-008 测试证据改为 feature validator 可核验命令 |
| v1.5 | 2026-07-18 | 固化用户确认的三页签最终信息架构和风险热力图默认首页；Phase 1 只激活真实演进页；以固定证据区/独立详情页替代抽屉和 raw JSON；补 UI 状态机、失败语义、legacy guard、可访问性和四项 DESIGN-COMPLIANCE-001 审核 |
| v1.4 | 2026-07-17 | 回填 P1-B evaluator/scorer、旧诊断唯一计算路径迁移、真实验证证据和 P1-C 剩余范围 |
