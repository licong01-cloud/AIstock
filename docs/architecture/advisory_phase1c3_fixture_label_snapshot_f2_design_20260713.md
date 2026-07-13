# AIstock Advisory Phase 1C-3 Fixture Label And Snapshot F2 Design

## 0. Background / 文档定位与背景

本文是以下权威父级设计在 Phase 1C 的第三个可实施切片：

- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
- `docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
- `docs/architecture/advisory_phase1c_capture_foundation_f2_design_20260713.md`
- `docs/architecture/advisory_phase1c2_fixture_source_revision_selector_f2_design_20260713.md`

Batch B 的实现级子设计由以下文档冻结：

- `docs/architecture/advisory_phase1c3_batch_b_label_capture_revision_selector_f2_design_20260713.md`

Phase 1C-1 已提供 capture batch、gap、canonical signal、observation version、lineage、
stage/candidate 和 fixture observation writer。Phase 1C-2 已提供 explicit source
requirement、exact source resolution、terminal-first observation selector、capture membership
seal 和 source revision v2 cutoff evidence。

本切片完整目标是在 fixture/local 环境交付 outcome label、PIT universe raw outcome、build/attempt、
deterministic Parquet、真实本地 CAS 和 SEALED snapshot golden；当前 Batch A 已合入，Batch B-D
仍按本文和子阶段设计实施。它不接入真实 observer、
日常调度、荐股页面、Selection、Paper、模拟盘、QMT、模型训练或交易执行。
本切片任何后续阶段若引入模型训练，训练进程只能在 WSL 的项目 Conda 环境运行，禁止在
Windows 环境执行训练；Batch B 本身不包含训练代码或训练环境运行门禁。

## 1. Feature Classification

- Tier：`F2`
- Delivery type：分批实现；Batch A 已合入，Batch B-D 保持设计/待实现状态
- Predecessors：Phase 1C-1、Phase 1C-2 和 Phase 1C-3 Batch A 已合入
- Runtime activation：`noop`
- Database mutation：`noop`
- Approval/role/authorization/manual gate：不存在，也不在后续实现范围

## 2. Architecture / 设计结论与总体架构

Phase 1C-3 使用以下单一业务链：

```text
COMPLETE capture batch membership
  -> one selected observation version per canonical signal
  -> alpha_raw maximum legal INCLUDED candidate set
  -> frozen LabelPolicyBundle + exact label SourceRevisionSet
  -> one shared OutcomeEngine
       -> candidate label revisions
       -> PIT eligible universe raw outcomes
       -> minimal calculation evidence blobs
  -> separate COMPLETE label-capture batch
       -> references source observation-capture receipt
       -> seals policy/source/label/evidence memberships
  -> terminal-first SelectedLabelMapping
  -> frozen BuildRequest + COMPLETE observation/label capture set
  -> DatasetBuild / operation-scoped BuildAttempt
  -> deterministic attempt files
  -> full verify receipt
  -> create-if-absent local CAS promotion
  -> manifest-content-addressed SEALED snapshot
```

必须同时成立：

1. 候选与 universe 不复制两套收益、成本、benchmark、terminal 或 censor 公式。
2. 每个 projection 每行只有一个 value 或 event，不使用宽 JSON 混装多 projection。
3. `T -> E -> S -> X_h` 只由冻结 calendar/policy 计算，`h=1` 的 `X_1=S`。
4. PENDING、MATURED、RIGHT_CENSORED、UNAVAILABLE 使用 append-only revision，禁止
   UPDATE 和 predecessor fallback。
5. 缺 price、adj factor、tradability、cost、benchmark 或 settlement 时只影响依赖该
   source 的 projection，不以 0 或其他 projection 代替。
6. fixture snapshot 必须由真实 Parquet bytes、真实 SHA、真实 schema/readback 和真实本地
   filesystem CAS 产生；不接受 mock-only、内存文件或只比较预期 JSON 的成功证明。
7. snapshot 只有在 durable promotion 已完成、selected version 唯一、cross-reference 闭合后
   才能插入 `SEALED` final row。

## 3. 当前事实与缺口

### 3.1 已存在并必须复用

- `CapturePlan`、`CaptureMembership`、capture batch lease/fencing/CAS。
- `canonical_signal_id_for_plan()` 和 immutable observation/version/lineage/stage/candidate。
- `SelectedObservationMapping` 的 terminal-first EXACT/LATEST policy。
- `SourceRequirementSet`、`SourceResolutionReceipt`、`SourceRevisionSet` 和 exact member hashes。
- `alpha_raw` INCLUDED candidate 的 symbol、rank、score、component evidence 和 stage hash。

### 3.2 当前不存在

- label policy bundle 和统一 outcome engine。
- outcome label revision、label selector 和 selected label mapping repository。
- PIT universe raw outcome 与 calculation evidence schema。
- dataset build、attempt、checkpoint、file set 和 build event repository。
- deterministic Parquet writer/verifier。
- local filesystem CAS、promotion receipt、manifest 和 final snapshot repository。
- 对应 label/build/snapshot additive migration。

因此 Phase 1C-3 不得被实现成只计算几个收益字段的纯函数，也不得把静态 fixture JSON
描述为 snapshot 完成。

## 4. Scope

### 4.1 In Scope

- 冻结并校验 v1 label/calendar/price/cost/benchmark/terminal/barrier/cash-return policy。
- 计算 T/E/S/X_h、projection maturity 和 source closure。
- 使用同一 OutcomeEngine 生成 candidate 和 universe raw outcomes。
- append-only label revision、exact/latest terminal-first selector、immutable mapping repository。
- calculation evidence bundle 的真实本地 CAS bytes 和 content hash。
- fixture/local build、attempt、lease/fencing、checkpoint、terminate/recover 状态机。
- deterministic Parquet materialize、全量 verify、local CAS promote 和 DB seal golden。
- additive label/build/snapshot migration 与 rollback 设计；只允许 DEV/test rollback-only 验证。
- 单 Alpha、原生多 Alpha、空候选、terminal/censor、缺 source、成本/benchmark 和 crash
  recovery 的正反向 golden。

### 4.2 Non-Goals

- 不实现 source availability observer；属于 Phase 1D。
- 不扫描生产市场库或执行真实历史回填；属于 Phase 1E/1G/1H。
- 不配置生产 durable dataset store，不发布首个生产 snapshot；属于 Phase 1I。
- 不启动 Phase 0B 分析或任何模型训练。
- 不改变 StrategyPackage、Selection Center、Paper、模拟盘、QMT 或 Advisory list 语义。
- 不创建 HTTP API、UI、MCP、scheduler、role、grant、approval 或人工 override。
- 不允许运行进程执行 DDL。
- 不设计 GC 物理删除；Phase 1C-3 只验证 reservation/ref/seal 的必要闭包。完整
  invalidation/GC runtime 留在 Phase 1I。

## 4.3 Parent Consistency

| 父级规则 | Phase 1C-3 落点 |
|---|---|
| A1-008 label revision/terminal-first | §10 append-only chain 与 selector |
| A1-010 content-addressed snapshot/CAS | §12-§14 build、promotion、seal |
| A1-011 candidate/universe 分离 | §9 owner 与 raw universe outcome |
| A1-017 stable signal/version selection | §6、§10、§13 selected mappings |
| A1-018 T/E/S/X_h | §7 calendar/maturity |
| A1-019 common cost/benchmark/outcome | §8 one OutcomeEngine |
| A1-020 attempt/fencing/generation | §11-§12 state machine/base admission |
| 父级 §10.2 logical label row | §6.5、§10、§16.1 mandatory header/payload one-to-one physical refinement |
| 父级 outcome acceptance item 029 | §7-§9 与 calculation evidence |
| 父级 build/CAS acceptance item 030 | §11-§16 build/CAS/invalidation/schema |

本切片不改变父级 ID、状态名、projection、maturity 或 snapshot hash 公式。若实现发现父级
字段不足，必须先回到设计原位修订并重新验证，不能在代码中增加未记录的 fallback 或 mode。
§16.1 的拆表只细化父级单条 logical label 的 PostgreSQL 物理布局；父级全部字段、唯一
revision chain、selector 和 snapshot membership 语义仍由一次 mandatory join 完整提供。

## 5. 权威数据与隔离

| 数据 | Phase 1C-3 权威来源 | 禁止来源 |
|---|---|---|
| signal/candidate | COMPLETE capture membership 中选择的 observation/alpha_raw stage | 当前 Selection 查询、荐股列表、人工选择 |
| outcome price/path | exact label SourceRevisionSet 指向的历史行情 slice | 当前价、Paper 成交、回测结果 |
| calendar | frozen calendar version/hash/slice | 系统当前日期推断 |
| adjustment/corporate action | exact adj/corporate-action source members | 未复权 fallback、当前复权表 latest |
| tradability | exact suspend/limit/pre-close source members | “有价格即可成交”假设 |
| cost/cash | frozen versioned policy | 零成本默认值、隐式追加资本 |
| benchmark | T cutoff 冻结 universe constituents/weights | E/X_h 后重新选成分或重加权 |
| terminal/censor | frozen terminal policy + exact event/settlement source | 默认零收益、删除样本 |

所有输入都使用显式 immutable object、完整 hash 和 exact source revision。fixture/local
repository 是真实契约 oracle，不从数据库 mutable latest row 补值。

## 6. Contracts / 核心类型与契约

### 6.1 `LabelPolicyBundle`

```text
label_policy_bundle_id/hash
label_policy_id/hash/schema_version
phase1_handoff_bundle_hash/handoff_readiness_hash
admission_scope_id/hash
audit_target_id
package_id/manifest_sha256/alpha_mode
style_family
style_assignment_policy_id/hash/decided_at
calendar_version/hash
price_policy_hash
adjustment_policy_hash
entry_execution_policy_hash
cost_policy_hash
benchmark_policy_hash
cash_return_policy_hash
terminal_return_policy_hash
barrier_policy_hash
corporate_action_policy_hash
symbol_normalization_policy_hash
horizons[]                         # sorted unique h >= 1
projections_by_horizon[]
candidate_reference_notional
benchmark_portfolio_notional
currency = CNY
price_unit = yuan
storage_scale = li_to_yuan_1000
research_only = true
execution_prohibited = true
```

所有 policy 必须有非空 schema/version/hash。v1 可显式使用
`CASH_RETURN_ZERO_V1`，但不能省略 `cash_return_policy_hash`。bundle canonical hash 排除
created_at、运行 worker、URI 和 attempt。registry policy definition 可以跨 target 复用，但
resolved bundle 必须绑定 Phase 0A handoff 已冻结的 target/package/style/handoff identity；
style 不从包名、候选走势或未来收益临时推断。超跌反弹与长期趋势可以
冻结不同 horizon、barrier、terminal 和 projection matrix，不能在 outcome engine 内按包名
隐藏分支。

### 6.2 `OutcomeOwner`

```text
owner_type = CANDIDATE | UNIVERSE
owner_key
canonical_signal_id
observation_version_id nullable for UNIVERSE
candidate_stage_evidence_id nullable for UNIVERSE
symbol
decision_as_of_trade_date
universe_layer
evidence_scope = RETROSPECTIVE_RESEARCH_ONLY
```

Candidate owner 必须属于 selected observation 的最大合法 `alpha_raw` stage，且
`membership_status=INCLUDED`。Universe owner 必须属于 T cutoff 冻结的 PIT eligible
universe；它不会进入 candidate、荐股名单或 Selection。

### 6.3 `OutcomeCalculationRequest`

```text
owner
label_policy_bundle_id/hash
horizon_trading_days
projection
projection_schema_version
T/E/S/X_h and scheduled maturity
label_as_of_ts
label_source_revision_set_id/hash
entry/exit/path/adj/corporate-action/tradability slices
benchmark constituent/weight/source bundle when required
terminal/censor/settlement bundle when applicable
```

请求不接受任意 SQL、current reader、默认 source 或未冻结 dict。候选与 universe 都调用
同一个 `OutcomeEngine.calculate(request)`。

### 6.4 `OutcomeCalculationResult`

```text
maturity_status = PENDING | MATURED | RIGHT_CENSORED | UNAVAILABLE
outcome_event_status = NONE | TERMINAL | BARRIER
projection_value_decimal nullable
projection_event_code nullable
entry_status
entry/exit/path/barrier/terminal/censor evidence
candidate/benchmark cashflow details when required
source_closed_at/event_closed_at/failure_observed_at
missing_source_receipt_hash nullable
calculation_evidence_bundle
projection_payload_hash
reason_codes[]
```

状态和 payload 使用 projection-specific validator。无关字段必须为 NULL；缺失必需字段
必须返回稳定 reason code，不能抛裸 `KeyError`、空列表或部分成功。

### 6.5 `LabelAppendRequest` 与 `OutcomeLabelVersion`

`LabelAppendRequest` 是调用方可重试的语义请求，必须先计算
`label_append_request_hash`：

```text
label_key_hash
expected predecessor version id/hash/revision nullable-together
policy/source revision identities
projection semantic payload
calculation_evidence_sha256/size_bytes/store_backend_hash
label_append_request_hash
```

canonical request 明确排除 repository 控制或物理定位字段：`computed_at`、
`label_version_id`、`label_revision_no`、首次创建它的 capture batch id、evidence URI、attempt、
worker 和运行时间。`calculation_evidence_uri` 只是 locator；语义身份使用 evidence SHA、size 和
store backend identity，因此同一 evidence bytes 更换 URI 仍是同一请求。

repository 接受请求后才产生 `OutcomeLabelVersion`：

```text
label_key_hash = hash(canonical_signal_id, symbol, label_policy_hash,
                      horizon, projection)
label_version_id = advlabel_<label_content_hash[:24]>
label_revision_no
supersedes_label_version_id
label_append_request_hash
canonical_signal_id/observation_version_id/candidate_stage_evidence_id
symbol/decision_as_of_trade_date
policy/source revision identities
OutcomeCalculationResult payload
created_by_capture_batch_id
computed_at
label_content_hash
```

`computed_at` 由 PostgreSQL 在首次插入事务中使用 `clock_timestamp()` 产生，不由调用方
提供。它不进入经济 label key 或 append request hash，但进入 version evidence；
`label_content_hash` 同时覆盖 append request hash、repository 分配的 revision/predecessor、
`computed_at`、首次 creator batch、semantic payload 和 evidence content identity，明确排除
locator URI。snapshot selector 只按冻结 `label_as_of_ts` 解析 terminal。

### 6.6 `SelectedLabelMapping`

```text
selection_policy = EXACT_REVISION_V1 | LATEST_ELIGIBLE_REVISION_V1
selector_request_hash/policy_hash
label_key_hash
requested_label_as_of_ts
required maturity/event/projection capability
expected observation_version_id/candidate_stage_evidence_id
expected label_source_revision_set_hash
terminal label version id/hash/revision
selection_status = SELECTED | UNAVAILABLE | CONFLICT
reason_codes[]
selected_mapping_id/hash
```

它使用强类型 immutable repository oracle。same ID/same hash 幂等；same ID/different hash
或 same hash/different ID 是 conflict，禁止覆盖。

### 6.7 `LabelCaptureRequest`

Observation capture batch 完成后已经不可变，Phase 1C-3 禁止重新打开并追加 label。每次
label 计算或成熟修订使用一个新的 capture batch state-machine instance：

```text
schema_version = advisory_phase1_capture_batch_v2
capture_purpose = LABEL_CAPTURE_V1       # 进入 request payload/hash，不增加可变 mode
source_observation_capture_batch_id/receipt_hash/membership_hash
sorted selected observation mapping ids/hashes
label_policy_bundle_id/hash
label_source_revision_set_id/hash
label_as_of_ts
planned label keys/projections/horizons/count hash
```

现有 `advisory_phase1_capture_batch_v1` canonical payload/hash 绝对不修改，并始终解释为
`OBSERVATION_CAPTURE_V1`。实现增加显式 tagged-union request：v1 只接受既有 observation
字段；v2 必须携带 `capture_purpose`，本切片只允许 `LABEL_CAPTURE_V1`。repository 按 payload
schema version 反序列化，禁止给 v1 注入新默认字段后重算 hash。这样新 label batch 具有独立
request identity，又不会改变任何历史 observation batch/receipt。

代码形态必须保留现有 `CaptureBatchRequest` 作为 v1 类型和既有 import surface，新增
`LabelCaptureBatchRequestV2`，并令 batch/repository 内部只接受这两个类型的 discriminated
union。v1 继续要求至少一个 `CapturePlan` 并写 `advisory_capture_plan`；v2 不携带也不创建新的
CapturePlan row，只以 source observation receipt、历史 plan set hash 和 selected mapping set
证明来源。`_load_locked()` 先读新增 discriminator 再选择强类型 parser，不能先按 v1 parse
失败后 fallback 到 v2。

每个 label batch 只覆盖一个 admission scope。它引用 source observation batch 和历史
`CapturePlan` identity，但绝不复用、重建或修改 `TraceCaptureBinding`。source observation
batch 必须为 COMPLETE 且 receipt/membership hash 精确匹配；多个 scope 通过多个独立 label
batch 处理，任一 scope conflict 不阻断其他 scope。

LABEL_CAPTURE 复用现有 capture batch lease/fencing/CAS，不建立第二套 batch 状态机。RUNNING
期间按 evidence role 追加 source observation capture、selected observation mapping、policy
bundle、label source revision、label version、calculation evidence 和 selected label mapping；
COMPLETE 时封 membership count/hash/receipt。PENDING 后成熟或 source correction 必须创建
新的 label-capture batch，引用旧 label revision但不得修改旧 observation/label batch。

### 6.8 `LabelCaptureBinding` 与共享链路隔离

`LabelCaptureBinding` 是 Advisory Phase 1C-3 自有契约，定义在计划新增的
`backend/services/advisory_phase1/label_capture.py`，不得定义在或导入到 `stage_trace.py`：

```text
schema_version = advisory_phase1_label_capture_binding_v1
capture_batch_id/current_fencing_token          # 新 label batch 自身身份
source_observation_capture_batch_id/receipt_hash/membership_hash
source_capture_plan_set_count/hash
source_trace_binding_hash                       # 历史 provenance，只读
source_control_binding_event_hash               # 历史 provenance，只读
phase1_handoff_bundle_hash/handoff_readiness_hash
admission_scope_id/hash
selected_observation_mapping_set_count/hash
label_policy_bundle_id/hash
label_source_revision_set_id/hash
binding_hash
```

`binding_hash` 覆盖以上字段，但历史 trace/control hash 只证明来源，不赋予 label batch 当前
trace admission 状态。label capture 不调用 `PostgresTraceAdmissionValidator`，不查询也不重验
当前 `TRACE_CAPTURE enabled`；只验证历史 source observation batch 已 COMPLETE，且 request、
receipt、membership、binding 和 mapping hashes 精确一致。这样离线标签构建不会因为运行期
trace control 已禁用而阻断，也不会把荐股证据要求下沉到 Selection/Paper/模拟盘。

v2 capture request canonical payload 使用该 binding 的语义字段，但排除新 label
`capture_batch_id`、`current_fencing_token` 和 `binding_hash`；因此显式 recovery batch 可以获得
相同 request hash，同时每个 batch 仍有独立 binding identity。source observation/control/
plan/mapping/policy/source-revision hashes 均不得排除。

现有 `TraceCaptureBinding`、`Phase1TraceCaptureService`、
`backend/services/advisory_phase1/stage_trace.py` 的字段、canonical hash、admission 和行为全部
冻结。实现只能在 `capture_foundation.py` 的 batch repository 增加 tagged-union dispatch：

- v1 request/binding 仍严格走现有 `OBSERVATION_CAPTURE_V1` 路径，bytes/hash/SQL insert/readback
  完全不变；
- v2 `LABEL_CAPTURE_V1` 只接受 `LabelCaptureBinding`，不得转换为 `TraceCaptureBinding`；
- Batch B 只完成 domain model 与 `InMemoryCaptureBatchRepository` dispatch；PostgreSQL
  discriminator、v2 insert/readback 和 `_load_locked()` raw payload dispatch 必须与 Batch C
  additive migration 同批交付，禁止在 Batch B 创建等待 DDL 的半上线 SQL path；
- `backend/services/simulation_runtime/selection.py` 及其共享消费者保持零修改，并用 import/
  contract regression 证明行为不变。

## 7. 时间轴与成熟算法

### 7.1 Calendar projection

```text
T = decision_as_of_trade_date
E = next_trading_day(T)
S = next_trading_day(E)
X_h = shift_trading_days(E, h)      # 不包含 E
```

- `h >= 1`；`h=1 -> X_1=S`。
- GAP_1D 的 scheduled maturity 是 E open source 首次闭合时间，horizon 固定 0。
- 其他 projection 的 schedule 固定为 X_h policy deadline。
- terminal/barrier/censor 可以提前 event-close，但不得改写 scheduled maturity。
- calendar slice hash 必须闭合 T 至 max(X_h)；节假日、周末、临时休市 fixture 必须覆盖。

### 7.2 Maturity state

| 状态 | 必填 | 禁止解释 |
|---|---|---|
| PENDING | scheduled maturity；尚未闭合原因 | 不可消费，不等于 0 |
| MATURED | source_closed_at、完整 projection payload | terminal payoff 完整时仍进入 winner/loser 分母 |
| RIGHT_CENSORED | event_closed_at、censor date/reason、observed days | 只供 survival/hazard，不作固定期限收益 |
| UNAVAILABLE | failure_observed_at、missing source receipt、reason | 不删除、不伪造 value |

`outcome_event_status` 与 maturity 正交。`TERMINAL + MATURED`、`TERMINAL + UNAVAILABLE`
都合法，分别表示结算闭合和结算缺失。

### 7.3 Projection closure matrix

| Projection | 必需 source/closure | 独立失败边界 |
|---|---|---|
| GAP_1D | decision close、E entry quote、adj factor | 不依赖 X_h/cost/benchmark |
| RETURN_GROSS | entry、exit、adjustment、corporate action | 不依赖 cost/benchmark |
| RETURN_NET_ABSOLUTE | gross closure、candidate cashflow/cost | 缺 cost 仅阻断 net |
| RETURN_NET_EXCESS | net absolute、X_h benchmark | terminal candidate 不令 benchmark 提前成熟 |
| PATH_MFE/MAE | E 至 X_h 完整可用 path | path order 不可证明时 UNAVAILABLE |
| EXECUTABLE_MFE/MAE | S 至 X_h tradable path | 与 E 日不可卖 touch 分离 |
| BARRIER | E touch + S 起 barrier order | 同日双触发为 ORDER_AMBIGUOUS |
| SURVIVAL | terminal/censor policy 与 event source | censor 不作固定期限收益 |

## 8. 统一 OutcomeEngine

### 8.1 Price 与 adjustment

```text
raw_yuan = raw_li / 1000
normalized_price = raw_yuan * adj_factor
gross_return = normalized_exit / normalized_entry - 1
```

- raw price 用于成交现金流，normalized price 用于 total-return/path 比较。
- adj factor 缺失不得退回 raw return。
- entry 晚于 open 且无分钟证据时，包含入场日极值的 path/barrier projection
  UNAVAILABLE；其他不依赖 intraday order 的 projection 可独立闭合。
- exit basis=open 时不得使用退出后的 X_h high/low。

### 8.2 Entry 与 T+1

- v1 entry basis 来自 frozen policy，默认语义可为 `NEXT_OPEN_EXECUTABLE_V1`，但不是
  无条件使用 open。
- E 日停牌、无报价、不可成交涨跌停或 source 缺失产生明确 entry status。
- E 日不可卖；E 日 barrier touch 只进入 `entry_day_path_touch_*`。
- v1 不顺延寻找下一买点，也不估计分钟 fill probability。

### 8.3 Barrier

- executable barrier 从 S 开始。
- 日线同日触及 target 和 stop，或分钟同 timestamp 双触发，返回
  `ORDER_AMBIGUOUS`；不得事后选择有利顺序。
- `time_to_executable_hit_trading_days` 从 E 计数但只引用 S 起可执行事件。

### 8.4 Candidate fixed-capital cashflow

```text
Q0 = max lot multiple satisfying
     Q0 * buy_execution_price + buy_fee(Q0) <= candidate_reference_notional
entry_cash = Q0 * buy_execution_price + buy_fee
residual_cash = reference_notional - entry_cash
exit_cash = Qh * sell_execution_price - sell_fee + corporate_action_cashflows
terminal_value = residual_cash * (1 + cash_return_rate) + exit_cash
r_net_absolute = terminal_value / reference_notional - 1
```

- rights subscription 只能从 residual cash 扣除；需要外部注资则 UNAVAILABLE。
- slippage/impact 进入 execution price，不重复扣费。
- 保存 Q0/Qh、notional、逐项 fee、residual/exit cash 和 cost breakdown hash。

### 8.5 Benchmark

- v1 benchmark 固定为 `PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1`。
- T cutoff 冻结 constituents、weights 和 hash。
- 每个 allocation 独立计算 lot、最低佣金、不可执行留现金；E 日状态不能导致剔除后
  重加权。
- benchmark 与 candidate 共用 entry、exit、cost、cash、terminal、corporate-action
  实现。
- 缺 benchmark 只使 net excess/所需 benchmark projection UNAVAILABLE。

### 8.6 Terminal 与 censor

- terminal policy 冻结退市、吸收式停牌、长期停牌、结算和 last-valid-price 规则。
- terminal payoff 完整：`MATURED + TERMINAL`。
- payoff source 缺失：`UNAVAILABLE + TERMINAL`。
- policy 判定 censor：`RIGHT_CENSORED`，保存 informative/non-informative assumption。
- 禁止默认零收益或从 denominator 删除 terminal 样本。

### 8.7 Calculation evidence

每个 result 生成 canonical evidence bundle：

```text
owner identity
policy bundle hash
source revision set id/hash
entry/exit/path/adj/tradability/calendar slices
corporate-action/terminal/censor slices
benchmark constituent/allocation rows when required
cashflow/cost breakdown
formula/schema/writer version
result payload hash
```

bundle 以 canonical JSON/Arrow IPC 中确定的一种 v1 格式写入真实 local CAS，保存 URI、
SHA256 和 size。相同输入必须产生相同 bytes/hash。

## 9. Candidate Label 与 Universe Raw Outcome

### 9.1 Candidate enumeration

对每个 selected observation：

1. 解析唯一 `alpha_raw` stage evidence。
2. 取最大合法深度的 INCLUDED candidate；rank 必须 1..N 连续，symbol 唯一。
3. 为每个 candidate、horizon、projection 生成 label key。
4. 不因 Top5、Selection effective、Advisory list 或人工选择过滤。
5. 合法空候选日产生 observation coverage，不产生 candidate label。

### 9.2 Universe outcomes

- Universe 在 T cutoff 由 frozen universe policy 与 exact source revision 枚举。
- 使用相同 OutcomeEngine 和 policy bundle。
- 每行一个 projection，owner_type=UNIVERSE。
- raw outcome 进入 Parquet，不在 app DB 复制千万级明细。
- winner definition 不写死在 raw outcome；30%/50%/70% 等阈值由 Phase 0B 的
  versioned registry 派生。
- PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE 和 event counts 均进入 coverage。

## 10. Label Revision 与 Selector

### 10.1 Append-only repository

同一 `label_key_hash`：

- revision 1 无 predecessor。
- revision n 必须引用 n-1 的 exact version id/hash。
- 同一个 `label_append_request_hash` 重跑返回旧 revision 和首次持久化的权威 locator。
- payload/source/status 改变必须追加 revision。
- fork、cycle、revision gap、ID/hash collision 全部 conflict。
- 禁止 `ON CONFLICT DO UPDATE`。

允许状态转换继承父级矩阵：

```text
PENDING -> PENDING | MATURED | RIGHT_CENSORED | UNAVAILABLE
MATURED -> MATURED | UNAVAILABLE
RIGHT_CENSORED -> RIGHT_CENSORED | MATURED | UNAVAILABLE
UNAVAILABLE -> UNAVAILABLE | MATURED | RIGHT_CENSORED
```

每次非幂等变化必须引用新的 source revision/event/receipt。

单次 append transaction 必须按固定顺序执行：

1. 对 `label_key_hash` 获取 transaction-scoped advisory lock。
2. 先按全局唯一 `label_append_request_hash` 查找；命中后完整 readback 比较 semantic request、
   evidence SHA/size/backend 和 label key，全部一致才返回既有 version。URI 不参与比较，返回
   首次持久化的权威 URI；任何 hash collision 都返回 conflict。
3. 未命中时锁定该 key 的当前 terminal revision，验证调用方给出的 expected predecessor
   id/hash/revision 和状态转换；revision 由 repository 分配，禁止调用方猜测。
4. 数据库使用 `clock_timestamp()` 产生首次 `computed_at`，生成 label content hash/version id。
5. 同一事务插入 unpartitioned authority header 和恰好一行 partitioned payload；任何一侧
   缺失、date/version 不匹配或重复 payload 都使事务失败。

因此并发的相同语义请求收敛为同一 label version；不同 label batch 的 retry 可在各自
membership 中引用该既有 version，不会因为 creator batch、运行时间或 locator URI 不同而
制造伪 revision。

label repository 成功追加或幂等命中后，必须在当前 RUNNING `LABEL_CAPTURE_V1` batch 中
登记对应 label/evidence membership。batch 在所有 planned keys 都产生 label version 或
显式 gap 后才能 COMPLETE；部分写入后失败的 batch 不进入 build。

### 10.2 Terminal-first selector

1. 先验证完整 label chain。
2. 只保留 `computed_at <= label_as_of_ts`。
3. 解析唯一最大 revision terminal。
4. 验证 source revision、observation/stage membership 和 requested capability。
5. EXACT policy 要求显式 version 就是 terminal；LATEST 返回 terminal。
6. terminal 不满足时返回 UNAVAILABLE/CONFLICT，不回退旧 MATURED。

## 11. Build Request 与 Identity

### 11.1 `FixtureDatasetBuildRequest`

必须冻结：

```text
Phase 0A/handoff/admission scope hashes
sorted COMPLETE observation and label capture batch ids/receipt/membership hashes
date range
selected observation mappings
label policy bundle/horizons/projection matrix
selected label mappings or selector requests
universe/benchmark/cost/calendar/symbol policy hashes
query registry and snapshot source revision set
required composite capability matrix
builder/code/writer/schema/partition/compression config
requested_source_cutoff/label_as_of_ts
optional full base snapshot identity
```

只给 base id、不带 content/manifest hash 无效。attempt、lease、URI、worker 和运行时间不
进入 build request hash。

### 11.2 Identity

```text
capture_set_hash = hash(sorted COMPLETE capture ids/receipts/membership hashes)
logical_build_key = hash(build_request_hash, capture_set_hash,
                         snapshot_source_revision_set_hash)
build_id = advbuild_<hash(logical_build_key, generation)[:24]>
manifest_core_hash = hash(complete logical files, selected versions,
                          source/capture/base/capability/schema/count hashes)
snapshot_content_hash = manifest_core_hash
snapshot_id = advsnap_<snapshot_content_hash[:24]>
```

相同完整输入命中相同 logical key。ABORTED generation 的受控重做只增加 generation；
semantic/source/policy 改变必须形成新 logical key。

### 11.3 Base snapshot admission

非空 base identity 必须同时提供 id、content hash、manifest hash 和 source/capture/policy
compatibility hashes。admission 必须证明：

- base 是 SEALED，manifest/file/blob hashes 完整。
- base 和它实际复用的 base chain 无 append-only invalidation。
- schema/query/policy/symbol/calendar/evidence scope compatible，且无 base cycle。
- 只有 partition source/content hash 未变化的 blob 可复用。
- child manifest 展开完整 logical file set，不能把 delta 当完整 dataset。

Phase 1C-3 实现最小 `SnapshotInvalidationOracle` 和 append-only invalidation row，只用于
fixture base/read fail-closed；生产 invalidation/consumer epoch/GC CLI 仍属于 Phase 1I。
base invalidated 或 identity 不完整时直接拒绝，不退回 full rebuild 伪装相同 request。

## 12. Build 与 Attempt 状态机

### 12.1 Build

```text
lifecycle = ACTIVE | SEALED | FAILED_TERMINAL | ABORTED
checkpoint = REQUESTED | MATERIALIZED | VERIFIED | PROMOTED | SEALED
```

- checkpoint 只能单向前进。
- 同 logical key 只允许一个 ACTIVE generation。
- 每个 checkpoint 固定 attempt id、receipt hash 和 file/manifest set hash，写入后不可替换。
- 普通 attempt failure 不回退 checkpoint，也不能自行终止 build。

### 12.2 Attempt

```text
operation = MATERIALIZE | VERIFY | PROMOTE | SEAL | RECOVER
state = ACTIVE | SUCCEEDED | FAILED | EXPIRED | ABORTED
lease owner/token/fencing token/expiry
expected build row version/checkpoint
operation request hash
```

每个命令创建独立 attempt；命令之间不保持 DB transaction、exported snapshot 或 lease。
所有写入校验 current attempt、未过期 lease、fencing 和 expected row version。

### 12.3 Recover 与 terminate

- stale attempt 只能追加 EXPIRED 后由新 fencing token 建 RECOVER attempt。
- old token 永久失效，禁止继续登记 file/checkpoint。
- checkpoint 前失败可重试同 operation。
- 已固定 checkpoint 损坏时只能 `terminate-build` CAS 到 ABORTED/FAILED_TERMINAL。
- ABORTED 可引用 termination receipt 创建 next generation；FAILED_TERMINAL 必须修正语义
  形成新 logical key。
- 不使用 last-writer-wins。

## 13. Deterministic Parquet

### 13.1 Logical roles

```text
canonical_signals/
observation_versions/
selected_observations/
lineage/
stage_summaries/
stage_candidates/
outcome_labels/horizon=H/
selected_labels/horizon=H/
outcome_source_evidence/owner_type=CANDIDATE|UNIVERSE/horizon=H/
universe_outcomes/horizon=H/
gaps/
source_revisions/source_revision_set.parquet
schemas/*.json
```

每个 data role 按 decision year/month 分区；selected mapping 和 source descriptor 可为
小型全局文件。

### 13.2 Writer

- PyArrow lazy import，只在 fixture CLI/test path 初始化。
- 固定 schema、列顺序、decimal/timezone、sort key、compression 和 writer version。
- 去除随机 UUID、写入时间、临时路径和非业务 metadata。
- 每个 partition 全量计算 canonical content hash、file SHA、rows、min/max keys。
- 相同输入必须 byte-for-byte 相同；writer version 变化进入 build identity。

MATERIALIZE 使用一个有时限的 `REPEATABLE READ READ ONLY` coordinator transaction，
通过 server-side cursor/`fetchmany` 或 COPY 按日期分区批量读取 COMPLETE observation/label
capture memberships；禁止 N×symbol×date 查询。并行 reader 只能在该 transaction 存活期
导入同一 exported snapshot。全部 DB rows 写入 staging 并 flush 后关闭 transaction，后续
VERIFY/PROMOTE 不再读取 mutable source。超时、source hash 变化或内存预算超限使 attempt
显式失败，不能拆成多个不一致视图继续成功。

### 13.3 Verifier

VERIFY 不读取 mutable source，只读取 attempt files 和冻结 descriptors，并全量验证：

- file SHA/size/schema/rows/sort/partition content hash。
- selected observation 每个 canonical signal 恰好一个 version。
- selected label 每个 requested key 恰好一个 terminal legal version。
- label observation/stage/symbol membership cross-reference。
- outcome evidence owner/hash/source closure。
- candidate/universe 使用相同 policy/formula versions。
- counts、maturity/event summaries 和 capability rows。

抽样只能作为附加诊断，不能代替全量 verify。

## 14. Local Filesystem CAS 与 Seal

### 14.1 `LocalFilesystemDatasetStore`

Phase 1C-3 使用真实本地 filesystem adapter：

- root 必须由测试/开发配置显式提供并位于 repo 之外。
- staging 与 blob root 必须同卷。
- blob path=`blobs/sha256/<prefix>/<sha>`。
- create-if-absent；目标存在时逐 byte/size/hash 比较，不覆盖。
- 文件 close 后执行文件 flush，promotion 后 flush parent directory。
- 测试必须真实断开并重新打开文件验证，不以 in-memory fake 代替。
- store contract 冻结 filesystem/volume identity、atomic/durability mode、projected bytes、
  reserved bytes 和 min-free bytes；materialize 前必须满足
  `free >= projected + reserved + min_free`，容量未知或不足时在写文件前失败。

该 adapter 只证明协议和 golden，不产生生产 store receipt，也不宣称 Phase 1I 已完成。

### 14.2 Promotion

1. MATERIALIZE 写 attempt-scoped staging 并 flush。
2. VERIFY 生成 immutable verify receipt。
3. PROMOTE create-if-absent 写 CAS blobs。
4. 由 complete logical file set/selected versions/capabilities 生成 manifest core 和 snapshot id。
5. 原子发布 manifest + promotion receipt，目标存在时完整比较。
6. 短 DB/local control transaction 固定 PROMOTED checkpoint。

### 14.3 Seal

单一 transaction/oracle 操作同时固定：

- final SEALED snapshot header。
- complete file rows。
- selected observation/label mappings。
- blob refs。
- build-to-snapshot mapping 和 SEALED event。
- build checkpoint/lifecycle。

seal 前必须确认 promotion receipt/manifest/blob hashes、selected version 唯一、cross-reference、
composite capability 和 source/capture hashes。客户端超时重试按完整 snapshot content hash
幂等返回。

## 15. Composite Capability

不能用若干单维 true 推断完整能力。每个 capability row 的 exact key 至少包含：

```text
admission_scope_id
audit_target_id
canonical_signal_scope_hash/phase0a_signal_context_hash
oos_interval_id/evidence_scope
stage
label_policy_hash/horizon/projection
universe_policy_hash/universe_layer
capability_status/reason_codes/counts/content_hash
```

Phase 1C-3 fixture snapshot 可以声明已真实验证的 Phase 1 capability，但必须固定：

```text
MODEL_TRAINING_READY = false
RUNTIME_ADVISORY_READY = false
TRADING_EXECUTION_READY = false
```

缺 candidate label 不必阻断空候选 signal coverage；缺 universe outcome 只阻断对应
universe/projection capability。BLOCKED identity conflict 阻断对应 scope，不能伪装 PARTIAL。

## 16. Additive Schema Plan

后续实现新增一对 migration/rollback，建议命名：

```text
backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_<date>.sql
backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_<date>.rollback.sql
```

新增表：

```text
app.advisory_outcome_label
app.advisory_outcome_label_payload
app.advisory_dataset_build
app.advisory_dataset_build_attempt
app.advisory_dataset_attempt_file
app.advisory_dataset_build_event
app.advisory_dataset_build_gap
app.advisory_dataset_snapshot
app.advisory_dataset_snapshot_file
app.advisory_dataset_snapshot_observation
app.advisory_dataset_snapshot_label
app.advisory_dataset_snapshot_invalidation
app.advisory_dataset_blob
app.advisory_dataset_snapshot_blob_ref
```

扩展既有表：

```text
app.advisory_capture_batch
  capture_request_schema_version TEXT NOT NULL
    DEFAULT 'advisory_phase1_capture_batch_v1'
  capture_purpose TEXT NOT NULL
    DEFAULT 'OBSERVATION_CAPTURE_V1'
```

两列不进入历史 v1 canonical payload/hash。CHECK 只允许
`advisory_phase1_capture_batch_v1 + OBSERVATION_CAPTURE_V1` 或
`advisory_phase1_capture_batch_v2 + LABEL_CAPTURE_V1`。既有 observation insert 可以继续省略
两列并获得完全相同的 v1 行为；label insert 必须显式写 v2/purpose。label 行现有
`control_binding_event_hash` 只记录 source batch 的历史 provenance，label validator 不据此
要求当前 control event 仍为 enabled。

DB closure 同时要求 `request_payload_jsonb.schema_version` 等于 discriminator；v2 payload 的
`capture_purpose` 和 `binding_jsonb.schema_version` 必须分别为 `LABEL_CAPTURE_V1` 与
`advisory_phase1_label_capture_binding_v1`，v1 则保持既有 trace binding schema。现有
`verify_advisory_capture_batch_transition()` 必须把新增两列加入 immutable comparison；禁止在
PLANNED/RUNNING 或 terminal row 上切换 purpose/schema。

### 16.1 Physical column closure

父级 §10.2 的一个逻辑 label version 由 unpartitioned authority header 与 partitioned payload
一对一组成，禁止用一个通用 outcome JSONB 代替。拆表只解决 PostgreSQL 分区唯一约束，
不产生第二套 label authority；repository 任何读写都必须验证完整 join。

`app.advisory_outcome_label` 是全局 authority header：

```text
identity:
  label_version_id PK, label_key_hash, label_revision_no,
  supersedes_label_version_id, label_append_request_hash,
  canonical_signal_id, observation_version_id,
  candidate_stage_evidence_id, symbol, decision_as_of_trade_date
selector/projection:
  label_policy_id/hash, projection, projection_schema_version,
  horizon_trading_days, intended_entry_trade_date,
  earliest_sell_eligible_trade_date, exit_trade_date
status/source visibility:
  maturity_status, outcome_event_status, entry_status
  label_source_revision_set_id/hash, projection_payload_hash,
  calculation_evidence_sha256/size_bytes/store_backend_hash
audit:
  label_content_hash, created_by_capture_batch_id, computed_at
```

Header 约束至少包含：

- `UNIQUE(label_key_hash,label_revision_no)`；
- `UNIQUE(label_append_request_hash)`；
- `UNIQUE(supersedes_label_version_id)`，且 predecessor FK 指向同 header；
- `UNIQUE(label_content_hash)`；
- `UNIQUE(label_version_id,decision_as_of_trade_date)`，供 payload composite FK 使用。

`app.advisory_outcome_label_payload` 存放宽 projection/price/cashflow/evidence 字段：

```text
identity:
  label_version_id, decision_as_of_trade_date,
  projection, projection_schema_version, horizon_trading_days
clock/status detail:
  entry_ts, exit_ts, scheduled_maturity_ts, event_closed_at,
  source_closed_at, failure_observed_at, missing_source_receipt_hash,
  maturity_status, outcome_event_status, entry_status
projection payload:
  projection_value_decimal, projection_event_code, projection_payload_hash
price/cashflow:
  raw-yuan/adj entry and exit fields, Q0/Qh, notional/fees,
  entry_cash/residual_cash/exit_cash, benchmark gross/net return
path/event:
  entry-day touch, executable barrier, terminal, censor,
  last-valid-price, settlement and observed holding fields
evidence:
  all policy hashes, label source revision id/hash, source-slice hashes,
  calculation_evidence_uri/sha256/size_bytes/store_backend_hash,
  cost breakdown hash
```

Payload 使用 `PRIMARY KEY(decision_as_of_trade_date,label_version_id)`，并以
`(label_version_id,decision_as_of_trade_date)` composite FK 引用 header。它按
`decision_as_of_trade_date` 月分区，因此所有 partitioned unique/PK 都包含 partition key。
projection-specific CHECK 保证 value/event、maturity 和不相关 NULL 字段闭合；header/payload
重复的 date、projection/status/source/evidence hash 必须一致。header 的 calculation evidence
`(store_backend_hash,sha256)` 以 composite FK 引用 `advisory_dataset_blob`，size 通过 deferred
closure trigger 与 blob header 精确一致。

同一 transaction 先插 header 再插 payload。两侧 DEFERRABLE constraint trigger 在 commit
时验证每个新 header 恰好有一行匹配 payload，且 version/date/hash/status closure 一致；payload
PK 保证不会有第二行。header 或 payload 任一 insert 失败都回滚，禁止暴露半条 label。
header 与 payload 均 no-update/no-delete。只有 payload 预建 golden 使用的 2026-06、2026-07、
2026-08 月分区；unpartitioned header 不建月分区。

`app.advisory_dataset_build`：

```text
build_id PK, logical_build_key_sha256, build_generation, predecessor_build_id
build_request_hash, snapshot_source_revision_set_hash, capture_set_hash
handoff/admission/query hashes, date_start/date_end
base snapshot id/content/manifest nullable-together
builder/code/writer/partition/compression identities
lifecycle_status, checkpoint, current_fencing_token, current_attempt_id
materialized/verified/promoted/sealed attempt and receipt/file-set fields
sealed_snapshot_id, termination fields, row_version, timestamps
UNIQUE(logical_build_key_sha256,build_generation)
```

同 logical key 只有一个 ACTIVE generation；checkpoint 对应 attempt/receipt/set fields 必须
按状态 nullable-together，填充后不可替换。

`app.advisory_dataset_build_attempt`：

```text
attempt_id PK, build_id FK, attempt_no, operation, attempt_state
lease_owner_id/token/fencing_token/acquired/heartbeat/expires
expected_build_row_version/expected_checkpoint
staging_uri, operation_request_hash, error_code/hash, started/finished
UNIQUE(build_id,attempt_no)
```

`app.advisory_dataset_attempt_file`：

```text
attempt_id/fencing_token, logical_path/role, partition_key_hash/ordinal
staging_uri, sha256, size_bytes, row_count, schema_fingerprint
partition_content_hash, min/max decision date/sort key
compression/writer_version
PRIMARY KEY(attempt_id,logical_path)
UNIQUE(attempt_id,logical_role,partition_key_hash,ordinal)
```

`app.advisory_dataset_snapshot` 只允许 INSERT `snapshot_state=SEALED`：

```text
snapshot_id PK, snapshot_content_hash UNIQUE
manifest_core_sha256/manifest_sha256 UNIQUE, promotion receipt uri/hash
build_id UNIQUE, snapshot_schema_version
source/capture/base/handoff/admission/query identities
builder/code/writer/partition identities
dataset_capability_manifest/hash, schema fingerprint
file_count/row_count/total_bytes, label maturity/event summary, sealed_at
```

Mapping tables：

```text
snapshot_file:
  snapshot_id/logical_path/role/partition/ordinal/content_uri/SHA/size/rows/schema/content hash,
  store_backend_hash/blob_sha256
snapshot_observation:
  snapshot_id/canonical_signal_id/observation_version_id/evidence scope/OOS/selector hash
snapshot_label:
  snapshot_id/label_key_hash/label_version_id/canonical_signal_id/
  observation_version_id/candidate_stage_evidence_id/symbol/selector hash
```

唯一键严格继承父级 §13.5：每 snapshot/canonical signal 一个 observation，每
snapshot/label key 一个 label；label membership 必须指向同 snapshot selected observation。

其余表：

```text
snapshot_invalidation:
  invalidation_id, snapshot/manifest, invalidated_at/by, reason/hash,
  request/content hash, replacement snapshot nullable
dataset_blob:
  store_backend_hash, blob_sha256, size_bytes, first_seen_at
snapshot_blob_ref:
  snapshot_id/logical_path/logical_role/partition_key_hash/ordinal,
  store_backend_hash/blob_sha256/ref_content_hash
build_event:
  event_id/build/attempt/fencing/event_type/time/actor/payload/reasons
build_gap:
  gap_id/capture/canonical signal/target/program/package/date/capability/
  gap class/evidence scope/missing hashes/reasons/content hash
```

`app.advisory_dataset_blob` 使用
`PRIMARY KEY(store_backend_hash,blob_sha256)`；exact retry 必须 readback 比较 size 和 backend
identity，same key/different size 为 conflict。blob header no-update/no-delete。

`app.advisory_dataset_snapshot_blob_ref` 必须满足：

- `PRIMARY KEY(snapshot_id,logical_path)`；
- `UNIQUE(snapshot_id,logical_role,partition_key_hash,ordinal)`；
- `UNIQUE(ref_content_hash)`，其 canonical payload 覆盖 snapshot、path、role、partition、ordinal
  和 blob identity；
- FK `snapshot_id` -> snapshot，composite FK `(store_backend_hash,blob_sha256)` -> dataset_blob；
- `snapshot_file` 提供
  `UNIQUE(snapshot_id,logical_path,store_backend_hash,blob_sha256)`，并以 composite FK 指向
  dataset_blob；blob ref 再以同四列 composite FK 指向 snapshot_file，强制 logical path、store
  backend 和 blob SHA 完全一致；
- no-update/no-delete，exact retry 只能完整 readback 后返回既有 ref。

### 16.2 Trigger/index/partition closure

- migration 的 FK 拓扑顺序固定为：capture discriminator -> dataset blob -> label header ->
  label payload parent/partitions -> build/attempt -> snapshot -> snapshot file/mappings -> blob ref。
  后建 deferred cross-table trigger；不得依赖临时 `NOT VALID` FK 或禁用 trigger 才能完成 apply。
- label header、label payload、attempt file、snapshot/file/mapping/invalidation、blob header、
  blob ref、build event、build gap 全部 no-update/no-delete。
- build/attempt 只允许 §12 状态变化，trigger 必须验证 expected row/current fencing。
- label header/payload 一对一、predecessor/revision fork，以及 build event、attempt file 的
  ordinal/file-set fork 在 DB 层拒绝。
- Phase 1C-3 migration 只预建 golden 固定使用的 `2026-06-01`、`2026-07-01`、
  `2026-08-01` 三个月分区；运行时缺分区直接失败。Phase 1D 容量计划确定真实历史范围后，
  Phase 1F release migration 才预建完整生产分区，Phase 1C-3 不猜测多年范围。上述分区只
  属于 `advisory_outcome_label_payload`，全局 label header 不分区。
- 索引覆盖 label key/revision、canonical signal/symbol/horizon/projection/maturity/source time，
  build logical key/lifecycle/checkpoint、attempt lease expiry 和 snapshot sealed time。
- repository 使用 insert-do-nothing 后完整 readback 比较；禁止 evidence `DO UPDATE`。

本切片只实现 base/read admission 所需的最小 append-only snapshot invalidation，不提前实现
consumer epoch、hold 或 GC 物理删除 DML；schema 若为 seal/blob-ref 外键闭合所需，只能创建
最小 append-only header，不得把未实现状态机标为完成。

数据库约束必须包含：

- label global predecessor single-chain、append-request idempotency、revision gap/fork rejection、
  header/payload exactly-one closure 和 no update/delete。
- build lifecycle/checkpoint/attempt lease/fencing transition。
- attempt file immutable identity 和 file-set uniqueness。
- final snapshot 只能 INSERT 为 SEALED，禁止 update/delete。
- snapshot observation/label 唯一选择和 membership cross-reference。
- blob header/ref composite FK、snapshot file/ref identity closure 和两表 no update/delete。
- base snapshot chain/invalidation fail-closed。
- immutable repository exact retry 比较完整 persisted rows。
- 不创建 role、approval、authorization、grant/revoke 或 runtime DDL function。

rollback 只允许在 DEV/test 且不存在 label/build/snapshot evidence 时执行；生产迁移是否应用是
独立部署事实，不是业务审批。不得要求每次 DDL 前创建全库备份。

## 17. Implementation Plan / 实施批次

### 17.1 Frozen shared boundary

后续实现允许修改 Advisory 自有的 `capture_foundation.py` tagged-union repository，但必须保持
v1 default/serialization/hash/SQL 行为不变。以下共享文件是零修改边界：

```text
backend/services/advisory_phase1/stage_trace.py
backend/services/simulation_runtime/selection.py
```

`label_capture.py`/`label_builder.py` 禁止 import `PostgresTraceAdmissionValidator`，也禁止经
间接 helper 查询当前 TRACE_CAPTURE control。修改上述共享文件或加入 current-control 检查
会直接构成未满足 F-001/F-011 的实现，不得以兼容层、fallback 或跳过测试描述为完成。该
边界是开发范围与自动测试断言，不是运行时门禁、角色或人工审批。

### 17.2 Batch A：Policy、calendar 与 pure OutcomeEngine

- `label_policy.py`
- `outcome_engine.py`
- canonical calculation-evidence byte codec/hash + real create-if-absent local blob primitive
- candidate/universe 共用公式 golden

退出条件：T/E/S/X_h、maturity、price/adj、entry、barrier、cost、benchmark、terminal/censor
全部正反向 fixture 通过；不存在默认值或双实现。

### 17.3 Batch B：Label capture、revision、selector 与 universe rows

实施级契约、允许修改范围、WSL-only 后续训练约束和 12 项子阶段验收矩阵见：

`docs/architecture/advisory_phase1c3_batch_b_label_capture_revision_selector_f2_design_20260713.md`

- `label_capture.py` with Advisory-owned `LabelCaptureBinding`
- `label_builder.py`
- `LabelAppendRequest` canonical codec/hash
- unpartitioned authority header + partitioned payload logical models and append-only in-memory repository/oracle
- SelectedLabelMapping repository
- candidate enumeration、universe raw outcome、coverage summaries
- `capture_foundation.py` v1/v2 domain + in-memory dispatch；PostgreSQL SQL path 保持 v1 原样

本分支已完成实现并完成本地验证：历史 source observation receipt 精确校验且不重验 current
TRACE_CAPTURE enabled；append request 并发幂等、header/payload logical exactly-one、revision/
selector/source membership、multi-alpha/empty candidate 和 universe denominator raw evidence 闭合。
该状态不包含 PostgreSQL physical repository、migration 或任何 runtime wiring。

### 17.4 Batch C：Build/attempt 与 additive schema

- `dataset_build.py`
- build/attempt/file/event/snapshot repositories
- additive migration/rollback，含 capture v1/v2 discriminator、label header/payload 和 blob/ref
- `PostgresCaptureBatchRepository` v2 create/recover/`_load_locked()` dispatch 与 migration 同批实现
- PostgreSQL label authority header/payload repository
- DEV-DB rollback-only L4

退出条件：合法输入可自动走 REQUESTED -> MATERIALIZED -> VERIFIED；stale fencing、非法
transition、fork、update/delete 均拒绝。

### 17.5 Batch D：Parquet、CAS、promotion 与 SEALED golden

- `snapshot_writer.py`
- deterministic PyArrow writer/verifier
- real local filesystem CAS adapter
- manifest/promotion/seal receipts

退出条件：相同 fixture 两次产生相同 bytes/file SHA/manifest/snapshot ID；crash/retry 不
消费半成品；合法完整 fixture 能到 SEALED。

四个 batch 可以分别提交，但不得把 A/B 的纯函数或 C 的 schema 存在描述为 Phase 1C-3
完整交付。只有 D 的正向 SEALED golden 和全矩阵通过后，Phase 1C-3 才完成。

## 18. Error Contract

稳定 reason family：

```text
ADVISORY_PHASE1C3_POLICY_INVALID
ADVISORY_PHASE1C3_CALENDAR_INVALID
ADVISORY_PHASE1C3_SOURCE_INCOMPLETE
ADVISORY_PHASE1C3_ENTRY_UNAVAILABLE
ADVISORY_PHASE1C3_PATH_ORDER_UNAVAILABLE
ADVISORY_PHASE1C3_BARRIER_ORDER_AMBIGUOUS
ADVISORY_PHASE1C3_COST_UNAVAILABLE
ADVISORY_PHASE1C3_BENCHMARK_UNAVAILABLE
ADVISORY_PHASE1C3_TERMINAL_SETTLEMENT_UNAVAILABLE
ADVISORY_PHASE1C3_LABEL_CAPTURE_BINDING_CONFLICT
ADVISORY_PHASE1C3_LABEL_APPEND_REQUEST_CONFLICT
ADVISORY_PHASE1C3_LABEL_CHAIN_CONFLICT
ADVISORY_PHASE1C3_LABEL_PAYLOAD_CLOSURE_CONFLICT
ADVISORY_PHASE1C3_LABEL_SELECTION_CONFLICT
ADVISORY_PHASE1C3_BUILD_STATE_CONFLICT
ADVISORY_PHASE1C3_ATTEMPT_FENCED
ADVISORY_PHASE1C3_FILE_SET_MISMATCH
ADVISORY_PHASE1C3_CAS_CONTENT_CONFLICT
ADVISORY_PHASE1C3_BLOB_REF_CONFLICT
ADVISORY_PHASE1C3_SNAPSHOT_SEAL_INVALID
```

错误必须包含 scope/owner/key/hash/checkpoint 等非敏感 context。禁止裸异常、空成功、
自动重试另一 revision、零值 fallback、旧 MATURED fallback 或把 conflict 降级 PARTIAL。

## 19. Verification Plan

### 19.1 L0

- 禁止 import/call Selection、Paper、simulation、QMT、broker、order 和 runtime scheduler。
- `stage_trace.py` 与 `simulation_runtime/selection.py` 必须保持 zero diff；label modules 禁止
  import/call `PostgresTraceAdmissionValidator` 或 current TRACE_CAPTURE control reader。
- migration 无 role/approval/auth/grant/revoke/runtime DDL。
- schema/model/hash/SQL parameter parity。
- Ruff、py_compile、diff check、F2 feature workflow validation。

### 19.2 L1 Pure/fixture

- 周末/节假日/临时休市、h=1 和多 horizon 的 T/E/S/X_h。
- li-to-yuan、adj、corporate action、entry/exit basis 和 path order。
- E touch 与 S executable barrier 分离；same-bar 双触发 ambiguous。
- candidate fixed-capital、lot、最低佣金、residual cash、terminal cashflow。
- benchmark frozen constituents/weights、不可执行留现金且不重加权。
- terminal/censor/settlement 和 maturity/event 正交。
- candidate/universe 相同输入调用同 engine 产生相同 projection semantics。
- label transition、retry、fork、terminal-first no-fallback。
- 既有 observation capture v1 request/binding serialized bytes、payload/hash、receipt golden 和
  repository insert/readback 完全不变。
- 现有 `Phase1TraceCaptureService` enabled/disabled v1 contract smoke 继续通过，证明
  capture-foundation union 没有改变 Selection/Paper 所依赖的 trace path。
- v2 `LabelCaptureBinding` 使用新 batch id/token，精确引用历史 source receipt/membership/
  trace/control provenance；current TRACE_CAPTURE enabled/disabled 两种状态产生相同 label
  admission 结果，历史 hash 不匹配则稳定拒绝。
- v1 仍至少写一个既有 CapturePlan；v2 不创建 CapturePlan row，load/readback 必须依
  discriminator 一次解析成功，禁止 parse-error fallback。
- 相同 LabelAppendRequest 的串行 retry、并发 retry 和不同 evidence URI 均收敛到一个 version，
  新 label batch membership 可引用既有 version；不同 semantic/evidence content 才追加 revision。
- 单 Alpha、原生多 Alpha各腿不同 window 不影响 outcome common identity。

### 19.3 L2 DB rollback-only

- migration apply twice、schema/readback、rollback twice。
- migration 后既有 capture rows discriminator 均为 v1/OBSERVATION，旧 insert path 省略新列仍
  产生 v1；v2 label insert 必须显式 discriminator 且不触发 trace admission validator。
- label header 的 global PK/unique/index 可创建；payload 三个月 partition、composite FK 和
  projection CHECK 可创建。
- header-only、payload-only、wrong date/version/hash closure 均在 commit 拒绝；合法 header+
  payload 同事务自动通过，并能由 repository 完整 join/readback。
- label/build/attempt/file/snapshot append-only 与非法 transition 拒绝。
- `label_append_request_hash` 并发唯一、expected predecessor、revision gap/fork 和 content
  collision 拒绝。
- blob header/ref no-update/no-delete、composite FK、logical path/role uniqueness、snapshot
  file/ref identity closure；合法 blob + file + ref 自动通过。
- legal positive path 可到 VERIFIED/PROMOTED/SEALED，不存在正常数据永远过不了的约束。
- stale fencing、duplicate terminal、selected membership mismatch、same ID different hash 拒绝。

### 19.4 L2 File/CAS golden

- 真实 Parquet schema/readback、全量 rows/sort/hash。
- 两次相同输入 byte-identical。
- blob exists same bytes 幂等；same path/hash different bytes conflict。
- crash at materialize/verify/promote/seal 各 checkpoint 后恢复。
- published not sealed 不可读；sealed manifest/blob/mapping 全部闭合。
- old observation/new label membership 混用、两个 selected version、缺 composite capability 拒绝。
- base reuse 仅复用未变化 partition，invalidated/incompatible/cyclic base 拒绝。

### 19.5 Coverage

OutcomeEngine、label revision/selector、build/attempt state、file/CAS/seal 关键分支目标 100%；
模块总体 branch coverage 不低于 85%。共享消费者广泛回归委派 CI/nightly，本地不扩展到
Paper/模拟盘业务 E2E。

## 20. 自动不变量与可满足性

本切片只保留五组自动数据不变量，不存在审批层：

| 不变量 | 合法输入通过条件 | 正向证明 |
|---|---|---|
| Identity/source closure | 历史 source batch COMPLETE 且 receipt/membership/binding、selected observation、policy、source revision、owner 全 hash 匹配；不重验 current trace control | 单/多 Alpha 与 trace enabled/disabled 等价 fixture |
| Label closure | append request 幂等、header/payload 恰好一对一，projection 所需 source 闭合或明确 PENDING/CENSOR/UNAVAILABLE | 并发 retry、DB positive path 与全 maturity matrix |
| Build ownership | current attempt、lease、fencing、row version、checkpoint、base validity 匹配 | 正常单向 build |
| File/CAS integrity | complete file set、SHA、schema、sort、durable promotion 匹配 | real Parquet/CAS golden |
| Seal closure | selected versions 唯一、membership/capability、blob header/file/ref 闭合 | SEALED E2E golden |

数据准确、source 完整、policy 匹配、容量足够且无并发冲突时，合法 fixture 必须自动完成
`label -> build -> materialize -> verify -> promote -> seal`。任何导致所有合法输入永远失败的
CHECK/trigger/service predicate 都是 P0 缺陷，禁止通过 bypass 或人工 override 规避。

## 21. Design Acceptance Index

- F-001：Phase 1C-3 完整复用 Phase 1C-1/1C-2 identity、source、capture 和 selector；
  既有 capture v1 serialized bytes/payload/hash/behavior 不变，label 使用 Advisory-owned
  `LabelCaptureBinding` 与显式 v2 tagged request，不重用 TraceCaptureBinding、不重验 current
  trace admission，也不建立 mutable latest 或第二套 canonical signal/batch 状态机。
- F-002：T/E/S/X_h、h=1、maturity、terminal/censor 和 projection closure 精确定义。
- F-003：candidate 与 universe 使用同一个 OutcomeEngine，price/adj/entry/barrier 语义一致。
- F-004：fixed-capital cost/cashflow 与 frozen benchmark 完整且不存在零成本/重加权 fallback。
- F-005：LabelAppendRequest 并发幂等、全局 authority header/分区 payload 一对一、label
  revision、状态转换、terminal-first selector 和 immutable mapping 完整闭合。
- F-006：全 alpha_raw candidate 与 PIT universe raw outcome 分离；label 使用独立 COMPLETE
  capture batch，空候选和 coverage 不丢失。
- F-007：build request/key/generation、attempt/lease/fencing/checkpoint/terminate/recover 状态机闭合。
- F-008：deterministic Parquet、真实 local CAS、immutable blob header/ref、promotion、
  manifest-content snapshot 和 seal 闭合。
- F-009：additive schema/rollback、PostgreSQL-compatible global/header partition/payload keys、
  append-only/deferred closure trigger 和 DB positive path 可实施且可满足。
- F-010：四批实现边界明确，任何子集不得被描述为 Phase 1C-3 完成。
- F-011：`stage_trace.py` 与 `simulation_runtime/selection.py` 零修改；无 Selection/Paper/模拟盘/
  QMT/runtime 接线，无训练、生产 observer 或生产 store 激活。
- F-012：无审批、角色、授权、人工门禁、静默错误、简化版或业务逻辑偏移。

## 22. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `label_capture.py`; `capture_foundation.py` v1/v2 in-memory request union; frozen `stage_trace.py` | v1 capture regression; v2 tagged request/parser/recovery; historic provenance fixtures | batch_b_verified | none |
| F-002 | `backend/services/advisory_phase1/label_policy.py`; `backend/services/advisory_phase1/outcome_engine.py` | content-bound policy/calendar identity, T/E/S/X_h, horizon/terminal/censor and no-default negative fixtures; branch coverage: label policy 89%, outcome engine 86% | batch_a_verified | none |
| F-003 | one `OutcomeEngine` for CANDIDATE/UNIVERSE with canonical request revalidation and exact SourceRevisionSet/member binding | candidate/universe parity, owner-symbol/source drift and unsafe-model-copy rejection fixtures | batch_a_verified | none |
| F-004 | fixed-capital cashflow, per-share corporate action processing and exact rational equal-weight benchmark in `outcome_engine.py` | lot/minimum-fee/residual-cash, per-current-share action, unbuyable/zero-lot cash retention and unequal-weight rejection fixtures | batch_a_verified | none |
| F-005 | `label_builder.py` LabelAppendRequest, logical header/payload, in-memory repository and terminal-first selector | serial/concurrent/URI retry, stale predecessor, transition, exact/no-fallback and mapping collision tests | batch_b_verified | none |
| F-006 | `label_builder.py` alpha_raw candidate enumerator, LABEL_CAPTURE_V1 builder and universe raw rows | single/multi-alpha/empty, source/policy/duplicate universe, coverage, real-CAS COMPLETE/gap tests | batch_b_verified | none |
| F-007 | planned build/attempt repositories | state reachability, lease/fencing/recover/terminate tests | design_ready | none |
| F-008 | Batch A local atomic create-if-absent primitive in `backend/services/advisory_phase1/calculation_evidence.py`; planned Batch D `snapshot_writer.py` and blob/ref repositories | exact retry/conflict, repo-external root, hardlink atomic publish, staging cleanup and file/directory durability fixtures with 85% branch coverage; planned byte-identical Parquet/blob/ref/SEALED golden | design_ready | none |
| F-009 | planned Phase 1C-3 migration/rollback with capture discriminator and label header/payload | apply/readback, PostgreSQL constraint creation, positive path, illegal mutation and rollback L4 | design_ready | none |
| F-010 | §17 four implementation batches; Batch A/B implementation refs are recorded by F-001..F-006/F-008 | Batch B direct label suite 37 passed; capture/label combined suite 48 passed; Advisory Phase 1 regression 173 passed/3 skipped; pure branch coverage: label capture 85.71%, label builder 85.21%; Batch C/D exits remain defined by §17 | batch_b_verified | none |
| F-011 | Advisory Phase 1 package boundary plus §17 frozen shared files | Batch B zero-diff/import scan; Advisory Phase 1 regression; runtime remains noop | batch_b_verified | none |
| F-012 | §2/§4/§18/§20 plus Batch B changed-file scan | no approval/auth/current-control gate, no silent default, no shared runtime wiring; DESIGN-COMPLIANCE Batch B review | batch_b_verified | none |

## 23. Production Gates / 生产与运行状态

- 本文档本身不执行 DDL/DML，不启动或重启服务。
- 后续 migration 只在明确的 DEV/test rollback-only 任务中验证；生产应用另行记录状态。
- 不新增依赖安装；若实现需要 PyArrow 版本变化，必须在 Batch D 设计内显式更新 lock 并
  执行 Windows dependency smoke，不能假设 WSL 环境等于 Windows。
- Batch B 不包含模型训练；未来任何模型训练只允许 WSL/Conda，Windows 训练禁止。若需要
  实现运行时环境拒绝机制，必须先形成独立设计并经用户确认。
- runtime observer、capture dispatcher、label scheduler、dataset store activation 均为 noop。
- `production_ddl_gate`: `noop` for Batch B pure/in-memory code task。
- `production_dml_gate`: `noop`。
- `production_frontend_dependency_gate`: `noop`。
- `production_backend_dependency_gate`: `noop`。

这些是发布状态记录，不是审批或人工门禁。

## 24. Risks

| 风险 | 后果 | 设计控制 |
|---|---|---|
| 把 E 当 X_1 | T+1 收益错位 | 单一 calendar projector + off-by-one golden |
| 只标 Top5 | 重排训练样本偏差 | alpha_raw full-depth enumeration |
| candidate/universe 双公式 | recall denominator 不可比较 | one OutcomeEngine |
| 缺成本/benchmark 填 0 | 收益标签失真 | projection-local UNAVAILABLE |
| terminal 样本删除 | survivorship bias | maturity/event 正交并保留分母 |
| selector 回退旧 MATURED | source correction 被隐藏 | terminal-first no-fallback |
| label 复用 TraceCaptureBinding/current control | 离线标签阻断共享 Selection/Paper | 独立 LabelCaptureBinding + frozen shared files |
| 调用方时间戳/URI 进入 retry identity | 同一请求产生伪 revision | LabelAppendRequest hash 排除 repository/locator 字段 |
| partitioned label 承担全局 unique | PostgreSQL migration 无法创建 | unpartitioned authority header + partitioned payload |
| header/payload 分步成功 | selector 读取半条 label | same transaction + deferred exactly-one trigger |
| checkpoint 可回退或换文件 | snapshot 不可复现 | fixed attempt/file-set CAS |
| 向 COMPLETE observation batch 追加 label | 破坏 capture receipt | 独立 LABEL_CAPTURE_V1 batch |
| base identity/失效未校验 | 复用错误数据 | exact base chain/invalidation admission |
| 多事务分段读取 DB | Parquet 内部不一致 | one bounded coordinator snapshot transaction |
| Parquet 只抽样验证 | 坏行进入 snapshot | full readback/hash/cross-reference |
| CAS overwrite | 相同 hash 不同 bytes | create-if-absent + byte comparison |
| blob/ref 仅有逻辑描述无 FK | seal 指向不存在或错 backend blob | composite PK/FK + immutable file/ref closure |
| 单维 capability 拼接 | 错误宣称 ready | exact composite capability row |
| 子批次状态被误报为整体完成 | 交付范围与事实不符 | Batch D SEALED golden 后才完成 |

## 25. DESIGN-COMPLIANCE-001

- [x] 与父级 Phase 1 outcome/label/build/snapshot 语义一致。
- [x] 复用 Phase 1C-1/1C-2 identity、source、capture、selector，没有重复 authority。
- [x] label capture 使用独立 binding，仅校验历史 COMPLETE source evidence，不依赖 current
  TRACE_CAPTURE enabled，且共享 stage_trace/Selection 文件为零修改边界。
- [x] T/E/S/X_h、price/adj、entry、barrier、cost、benchmark、terminal/censor 完整。
- [x] candidate/universe 共用 engine，full candidate 和 denominator 边界明确。
- [x] label revision、build attempt、Parquet、CAS、promotion、seal 状态可达。
- [x] label global identity/header 与 partition payload、blob header/ref 的 PostgreSQL key/FK/
  deferred closure 可创建且具有合法正向事务。
- [x] 正常完整输入有自动正向路径，不存在无法满足的检查组合。
- [x] 不包含 simplified、POC、placeholder、mock-only success 或 silent fallback。
- [x] 不包含 Selection、Paper、模拟盘、QMT、训练或运行时激活。
- [x] 不包含审批、角色、授权、人工确认或额外业务门禁。
- [x] 设计任务不执行生产 DDL/DML、依赖安装或服务重启。

## 26. Exit Criteria

本文当前状态表示 Batch A 已合入，Batch B 已在独立 worktree 完成实现和本地验证，仍待代码审查、
提交和合入；Batch C/D 仍按各自批次边界等待后续实施：

- F-001 至 F-012 均为已验证或 `design_ready`，且无未批准 gap。
- F2 feature workflow validation 通过。
- 父级 identity、label、build、snapshot、isolation 和 no-approval 条款无冲突。
- 所有 code-time 参数决策均有 frozen policy/hash 位置，不留“实现时自行决定”。
- 明确只有最终 SEALED golden 才能宣称 Phase 1C-3 完成。

它不表示 Phase 1、Phase 0B、模型训练或荐股模型能力已经完成。
