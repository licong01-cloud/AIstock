# HMM 演进系统 Phase 1 离线评估实验室实现级详细设计

> **版本**：v1.1
> **日期**：2026-07-17
> **状态**：implementation-ready design；Phase 1 代码尚未实现
> **设计权威**：总体蓝图 `hmm_evolution_and_risk_management_system_design_20260716.md` v1.5
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

当前 `scripts/diagnostics/hmm_offline_diagnostic.py::compute_replacements` 已证明一条可行
的离线诊断路径：将 raw score 与 HMM coefficient 调整后的 score 分别排序，比较 TopK
entered/dropped 集合，并使用 label 或 market forward return 衡量替换质量。但该脚本仍是
人工诊断工具，包含本机 DB 配置、临时 I/O、输入顺序依赖和宽泛异常处理，不能直接成为
API service。

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
7. 提供真实 API、中文 UI、导航入口和高级调试抽屉；
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
| `no_silent_error` | 缺 artifact、hash 不符、horizon 不符、无共同日期、DB 数据不足、lease 丢失和取消均返回稳定 reason code，不返回中性成功。 |
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

| 审核项 | 原设计风险 | v1.1 处置 |
|---|---|---|
| QE 资产读取 | 只覆盖 pred/label 与两类 coefficient，低于研究所需只读范围 | 增加全资产 reader；inspection 范围与 computational trust 分离；真实 node complete catalog 为 F-006 验收 |
| 最新行情 | 只有显式 date 字段，容易被实现成 `date.today()` 动态漂移 | 增加 `latest_common_completed`，入队解析并固化 watermark/PIT coverage |
| 过度门禁 | strict-full 默认、approved-local 命名和多段 activation 容易演化成审批链 | 默认共同日期证据模式；configured source；只保留 DDL/首次 activation 操作授权 |
| 静默错误 | neutral fallback 和缺失指标重加权可能只在 JSON 内可见 | 强制 evidence_quality/warnings，主 UI badge，未知异常 fail，不允许空集合假成功 |
| 简化交付 | 原表有原则但缺少具体禁止清单 | 明确禁止 schema/backend/mock/static/placeholder/POC 冒充 Phase 1 完成 |
| Phase 0 对齐 | 扩大 QE 读取可能误改 Phase 0 whitelist | 全资产 reader 独立实现；Phase 0 pred/label 信任、cache、zero-copy 契约保持不变 |

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

### 15.1 页面与导航

- `/hmm-evolution`：实验室总览；
- `/hmm-evolution/batches/[batchId]`：批次详情；
- `/hmm-evolution/evaluations/[evalId]`：评估证据详情；
- 导航放在 `QuantEvolver` 分组，标签“🧭 HMM 演进实验室”，不放入 Paper Trading v2。

### 15.2 总览结构

1. 候选库：名称、来源、coverage、hash 短码、lifecycle；
2. QE 资产浏览：task/loop 资产目录、来源、hash、trust level 和受控内容预览；
3. 新建评估：base loop、日期/as-of policy、label horizon、TopK、DB 10 日收益模式、候选多选；
4. 运行中批次：进度、heartbeat、耗时、取消；
5. 历史排行榜：推荐分数、证据置信度、净标签收益（动态 horizon）、Net DB 10D、正值日比例、coverage；
6. top-3 卡片明确标注“研究推荐，需 QE 终审”。

### 15.3 详情与错误

- 主视图使用中文指标卡、表格和逐日折线；
- 原始 manifest/spec/hash/error context 放高级调试抽屉；
- `changed_day_count=0` 显示“未改变 TopK”，不显示绿色 0 收益；
- h20 标签动态显示 20 交易日；
- failed/timed_out 显示稳定 reason code、中文解释、可重试条件；
- degraded evidence、neutral fallback、缺失指标重加权和共同日期裁剪必须在主表显示 warning badge；
- polling 初始 3 秒，60 秒后退避到 10 秒，terminal 后停止；不引入 WebSocket。

### 15.4 客户端文件

目标：

```text
frontend/src/app/hmm-evolution/
frontend/src/components/hmm-evolution/
frontend/src/lib/hmm-evolution/api.ts
frontend/src/lib/navigation/nav-groups.ts
```

不复用 legacy Paper v2 页面依赖；共享通用基础组件时不得带入交易动作。

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
- UI 真实 API mock server contract test、中文文案、动态 horizon、QE asset browser、degraded warning、空状态、失败状态；
- 安全验证端口使用 `backend.validation_app` 扩展或专用 test app，不启 8001/3000/19080；
- UI E2E/截图交给 Validation Center，不能用静态 grep 代替。

### 17.4 外部验收

- 使用 Phase 0 已验收高收益 loop 作为输入复用证明；h20 标签按 h20 展示；
- 至少 10 个历史 HMM case 与 QE 结果做方向/排序对照；差异仅记录为 evidence；
- 记录 performance receipt、Prediction Store zero-copy、QE 全资产只读 access receipt、DB transaction read-only 与 latest-common watermark；
- 任何 production DDL、worker activation 或服务运行未获批时明确报告 pending。

### 17.5 DESIGN-COMPLIANCE-001

每个实现 PR 逐项检查：

- 是否交付该 PR 承诺的完整设计子集；
- 是否存在 bare/broad except、空集合假成功、中性 fallback 隐藏、静态成功或 warning 只藏调试 JSON；
- 是否触碰 QE/Paper/Selection/QMT/生产 snapshot；
- 是否新增本文未批准的阈值、审批或研究淘汰规则。

## 18. Implementation Plan（实施方案）

### P1-A：schema、candidate registry 与 durable state machine

交付 F-006/F-008：

- Python bootstrap + comments + verify；
- Pydantic models/errors；
- QE 全资产只读 reader、candidate artifact preview/registry；
- repository、batch/evaluation/item 状态机、lease/fencing/idempotency/retry/cancel；
- worker skeleton，不启生产 runtime。

生产状态：代码可合入；`production_ddl_gate=pending`；`runtime_activation_gate=pending`。

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
- `/hmm-evolution` 页面、详情、导航；
- 真实 API/UI 证据、10-case 对照和性能验收；
- 生产 runtime 首次启用需要一次操作授权；启用后正常研究操作不再增加审批门。

每个 PR 必须只承诺自身可验证子集，并更新总体蓝图 Design Acceptance Matrix 的真实引用。

## 19. Rollout / Rollback（发布与回滚）

### 19.1 rollout 顺序

1. 合入 P1-A 代码但不执行生产 DDL；
2. 在开发/验证 DB 显式 bootstrap、复跑和 drift verify；
3. 获得生产 DDL 操作授权后执行并保存 receipt；
4. 合入 P1-B，使用人工 worker CLI 做受控 benchmark；
5. 合入 P1-C，先在安全验证 app/UI 完成验收；
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

## 21. Production Gates（生产门禁）

| 门禁 | 本设计 PR | P1-A | P1-B | P1-C |
|---|---|---|---|---|
| `production_ddl_gate` | noop | pending，需生产 DDL 操作授权 | noop | noop |
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
- **F-010 Phase 1 API/UI**：真实 QE asset/candidate/evaluation/batch API、中文实验室、动态
  horizon、主视图 degraded warning、可读错误、高级调试抽屉和 QuantEvolver 导航入口。

## 23. Design Acceptance Matrix（设计验收矩阵）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-006 | 本文 §5.3、§6、§10、§11；目标 QE asset reader、`init_hmm_evolution_schema.py`、candidate registry/repository | 目标：真实 node complete catalog/all-asset read-only/mutation-refusal/manifest/path/hash/lifecycle/bootstrap/comment/write-allowlist tests | approved_by_user_for_implementation | 实现证据由 P1-A PR 回填 |
| F-007 | 本文 §7、§8；目标 `hmm_evolution/evaluator.py` | 目标：旧诊断 oracle、asset trust、latest-common watermark、tie/horizon/calendar/coverage/replay hash tests | approved_by_user_for_implementation | 实现证据由 P1-B PR 回填 |
| F-008 | 本文 §10～§13；目标 batch/evaluation/item repository + worker | 目标：idempotency/lease/fencing/heartbeat/cancel/retry/shared/partial failure tests | approved_by_user_for_implementation | 实现证据由 P1-A/P1-B PR 回填 |
| F-009 | 本文 §9；目标 `hmm_evolution/scorer.py` | 目标：percentile/weight/missing/tie/top-3/no-side-effect tests | approved_by_user_for_implementation | 实现证据由 P1-B PR 回填 |
| F-010 | 本文 §14、§15；目标 QE asset/candidate/evaluation/batch router、API client、页面、导航 | 目标：API contract、真实 UI、asset browser、中文/动态 horizon/degraded warning/error/截图证据 | approved_by_user_for_implementation | 实现证据由 P1-C PR 回填 |

## 24. 设计结论

Phase 1 可以按 P1-A → P1-B → P1-C 开始实现。最优先是 P1-A，因为 QE 全资产只读 reader、
candidate identity、schema 和 durable state machine 是 evaluator/API/UI 的共同地基。当前只
完成设计，不代表 schema 已部署、worker 已启用、API/UI 已运行或 Phase 1 已验收。
