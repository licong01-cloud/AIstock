# MiniQMT 统一执行内核 K1 Contracts / Registry F2 详细设计

> 权威关系：本文是 [`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md) 的 K1 下位实施合同；模拟盘唯一上位权威仍是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。冲突时依次以上位蓝图、整体内核蓝图、本文为准。本文不得改写 StrategyPackage admission、Selection/Target、`B0_QUOTE_V2`、OMS、Gateway 或唯一 broker route 的 owner。
>
> Feature tier：`F2`。文档状态：`design_ready`；实现状态：`not_started`。
>
> 基线：`origin/main@ef1da755e16193e67c0508b2d752c8f42e4b1207`，日期 2026-07-22。
>
> 交付边界：本文只定义 K1 contracts、code-owned registry、deterministic context、current-three manifests、pinned vn.py compatibility lock/receipt 和 import boundary。本文不实施产品代码、不执行 DDL/DML、不修改生产配置、不调用 broker、不启动、停止或重启服务。

## 0. Implementation Decision / 实施决策

K1 不切换现有 runtime，而是在现有唯一 `MiniQMTExecutionRuntime` 内建立下一阶段可直接消费的严格合同层：

1. contracts 使用 strict、frozen、`extra=forbid` 的 Pydantic v2 model 与同一个 canonical codec；任何 writer/readback 使用同一 schema；
2. registry 是代码拥有、封闭集合、一次构建成功或整体失败的 immutable snapshot，不扫描目录、不加载 entry point、不接受文件路径或用户上传插件；
3. manifest 与 factory binding 分离：`implementation_ref` 只用于审计，实际 factory 只能由 composition root 显式传入 callable；
4. current-three 在 K1 生成 manifest/compatibility receipt，但产品 runtime 继续使用现有 `VNPY_STYLE_ASSETS/create_vnpy_style_core`；K3 parity 完成前不得切换；
5. plugin config、kernel controls 与历史混合 `algo_config` 的投影必须可证明且无损；K1 只做 shadow comparison，不拒绝或改变现有 production run；
6. 时间、ID 与随机 draw 全部由 immutable event/session identity 派生；禁止 `datetime.now/utcnow`、`uuid4` 和 process-global random；
7. capability 校验只证明执行技术兼容性，不读取或二次校验策略包，不新增审批、RBAC、人工 acknowledge、人工恢复或 enable gate；
8. K1 没有 broker side effect、数据库 migration 或平行 runtime，也没有 LEGACY/minute/default-algo fallback。

## 1. Background and Current-State Evidence / 背景与当前事实

### 1.1 定向代码事实

| current artifact | observed contract/debt | K1 treatment |
| --- | --- | --- |
| `backend/execution_algos/vnpy_style/registry.py` | `VNPY_STYLE_ASSETS` 硬编码 core class；`str/int/float` 宽松转换；未知 config 可穿透 | 新 registry 严格构建；旧 registry 在 K3 前保留且不作为 V2 authority |
| `backend/execution_algos/vnpy_style/models.py` | action 使用 `uuid4`，order/trade DTO 使用 wall clock default | K1 定义 deterministic DTO/context；K3 才迁移生产调用 |
| `backend/execution_algos/vnpy_style/base.py` | algo/order/action ID、log time 使用进程随机/时间；空 `vt_orderid` 可无 receipt 返回 | K1 明确 deterministic identity 与 durable diagnostic contract |
| `best_limit_core.py` | 无 provider 时调用 process-global `random.uniform` | K1 固定 hash-based draw；K3 迁移前仍走现有注入 seam |
| `runtime.py` | runtime 直接按 `algo_code` 查 registry、构造 core 并在内存缓存 | K1 不改行为；K2/K3 才由 kernel/registry 替换具体分支 |
| `client.py::_timer_iterations` | `timer_iterations` 与算法参数混在 `algo_config`，同步 for-loop 模拟 timer | K1 仅记录 legacy projection；真实 timer 在 K2/K3 实施并退役该 helper |
| `b0_quote_v2.py::_vnpy_asset_manifest` | build manifest 从旧 `VNPY_STYLE_ASSETS` 筛 current three | K1 shadow comparison；K3 后改为 V2 registry snapshot |

基线 AIstock 文件 SHA-256 固定为：

| file | sha256 |
| --- | --- |
| `attribution.py` | `c8654322cfcc176a70b65a655bc678b0262149165b42dc68e7d9d158b16537d8` |
| `base.py` | `2329dfe9585cd682d01aab0ae4e91138e2f6cb881acf3e8a4c4456b7fce07395` |
| `models.py` | `4d515c9457f04f9fd26a0c0b90616c5e922a5fc7d74c25e6d4b3cae667a13644` |
| `registry.py` | `89924503eda9b81ada1bd263dec7af549e8475b7c2b519695989d959dcdca7f8` |
| `sniper_core.py` | `94873e9f6a306d39c66f7327b3b389e2587dac8f7adf8824600c91478ddfad49` |
| `best_limit_core.py` | `852850690851b0e4a0b8e611dbdf131710ed1dd8caa3442bd5dfc450efaa6a7c` |
| `twap_lite_core.py` | `6ab2ee3c19b507fcd8060bc73701c5b47fe834e058bf6808f3ced9cee60f4173` |

这些 hash 是 K1 characterization 输入，不是永久版本号；任何源文件变化必须更新行为 characterization、manifest version/hash 和直接 parity evidence，禁止只更新 hash 让测试通过。

### 1.2 Pinned upstream lock

唯一 upstream baseline 为 `vnpy/vnpy_algotrading@4133987530eb28f3538d1983545d81c4f83d7d59`。K1 lock 必须包含以下已核对 SHA-256：

| upstream file | sha256 |
| --- | --- |
| `vnpy_algotrading/engine.py` | `2c73e1c093cabcd5768954f1129451877a82afd204790fb07e4f305b64c5e68d` |
| `vnpy_algotrading/template.py` | `b21fa36a8a2c347ab92379df1cd9f81ec69bc922233ec4096d75dbbade7454b8` |
| `vnpy_algotrading/base.py` | `8416653d8cf61ab45e26b593eea06417dd6fa21b331bba6c60a2bbb8bccf8f93` |
| `vnpy_algotrading/algos/sniper_algo.py` | `fbf84d2c61f8200079fe1f8da3b3412a036e5a7ffb6c601f9e4614ad110c8c76` |
| `vnpy_algotrading/algos/best_limit_algo.py` | `b35227b932a160c2f786d3202283b61656d9f16631fb42f596a9d376765617e9` |
| `vnpy_algotrading/algos/twap_algo.py` | `aeabb067ef79d48182f357b8d4736f8a90f6a4ecb77bc82506a3244575a6cd0f` |

CI 不联网下载 upstream。上述 lock、精确 surface descriptor 和本地 characterization vectors 是 CI authority；升级 commit 或任一 source hash 必须作为独立 compatibility migration，不允许跟随 `latest`。

## 2. Scope and Non-Goals / 范围与非目标

### 2.1 K1 范围

- strict V2 DTO、canonical JSON/hash、typed error hierarchy；
- immutable code-owned plugin registry 与 deterministic snapshot；
- `DeterministicExecutionContextV1` 和 ID/draw helpers；
- current-three config/state/market-data/event manifests；
- pinned `VnpyCompatibilitySurfaceV1`、requirement/receipt 和 source lock；
- import/static boundary 与 shadow parity；
- ownership/test routing、直接测试和 K1 implementation slicing。

### 2.2 非目标

- 不实现 durable event ingress/delivery/repository/outbox、ExchangeSessionClock 或 DDL；这些属于 K2；
- 不把 Sniper/BestLimit/TWAP 生产调用切换到 V2 plugin；这些属于 K3；
- 不实现完整 façade runtime、Iceberg、Stop；分别属于 K4/K5；
- 不删除旧 registry/core/client helper；只有 K3/K6 在 parity 和真实证据闭合后退役；
- 不改变 signal、selection、target、side、quantity、A 股手数/T+1/涨跌停/停牌、B0 quote、OMS 或 Gateway 语义；
- 不新增人工门禁、审批、RBAC、acknowledge、confirm-run、人工 recovery、默认业务 fallback 或第二 broker route。

## 3. Architecture, Target Files and Dependency Direction / 架构、目标文件与依赖

### 3.1 K1 目标文件

```text
backend/services/miniqmt_execution_runtime/
  plugin_contracts.py          # strict DTO/enums only
  plugin_canonical.py          # one canonical JSON/hash authority
  plugin_registry.py           # immutable snapshot/build validation
  deterministic_context.py     # logical time/ID/hash-draw only

backend/execution_algos/
  vnpy_style/plugin_manifests.py  # current-three manifest/schema descriptors
  vnpy_compat/
    __init__.py
    locked_surface.py          # pinned code-owned surface/source lock
    receipts.py                # pure compatibility comparison

backend/tests/miniqmt_execution_runtime/
  test_algo_plugin_contracts.py
  test_algo_plugin_registry.py
  test_deterministic_execution_context.py
  test_current_three_plugin_manifests.py
  test_vnpy_compatibility_receipts.py
  test_plugin_import_boundaries.py
```

K1 不新增 repository、router、scheduler、gateway、OMS、DB migration 或 runtime switch 文件。

### 3.2 Import rule

允许依赖方向：

```text
plugin_contracts <- plugin_canonical
plugin_contracts <- deterministic_context
plugin_contracts + deterministic_context <- plugin_registry
plugin_contracts <- current-three manifest descriptors
plugin_contracts <- vnpy_compat locked surface/receipts
composition root -> registry + explicit factory bindings
```

插件/manifest/compat 模块禁止导入：

- `simulation_runtime` scheduler/client/bridge；
- MiniQMT runtime/repository/gateway/OMS 实现；
- `xtquant`、QMT 网络 client、FastAPI/router；
- StrategyPackage、Selection、Target/Rebalance、model、QE、factor、DB pool/repository；
- wall-clock、UUID 或 process-global random helper。

`test_plugin_import_boundaries.py` 必须同时做 AST import scan 与 isolated import；禁止靠注释关键字或 monkeypatch-only 测试假通过。导入违规是 build failure，不是运行期开关。

## 4. Strict Model and Canonical Codec / 严格模型与哈希

### 4.1 Common model policy

所有 K1 model 继承内部 `FrozenStrictModel`：

```python
model_config = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    validate_default=True,
)
```

补充规则：

- identity 必须为非空、首尾无空白的 `str`；不得 `str(value)`；
- `bool` 不得作为 `int/float`；数量为严格正整数或模型明确允许的零；
- money/price/rate 使用 canonical decimal string，validator 通过 `Decimal` 证明 finite、scale/range 后保存规范字符串；禁止 hash binary float；
- datetime 必须 timezone-aware，规范为 UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`；naive、offset ambiguity 拒绝；
- mapping 只允许字符串 key；duplicate key、非 JSON 类型、NaN/Infinity、bytes、set/tuple carrier 拒绝；tuple 仅存在于 typed model 内，dump 后为 JSON array；
- enum 只接受精确值，不大小写猜测；legacy normalization 只能在独立 projection 中执行并保留原值/hash；
- error context 先经过 JSON-safe evidence codec；malformed value 只进入 evidence，不进入 set/sort/Counter，错误构造器不得二次抛异常。

### 4.2 Canonical JSON V1

`canonical_json_bytes_v1(value)` 固定为：

1. 先用目标 strict model `model_dump(mode="json", exclude_none=False)`；
2. 递归拒绝非 `null/bool/int/str/list/object`；decimal/datetime 已在 model 层规范为 string；
3. object key 按 Unicode code point 升序；array 保留业务顺序；声明为 set-semantics 的 manifest 字段在 model validator 中先去重并按 enum/string 升序转 tuple；
4. UTF-8、`ensure_ascii=False`、无 BOM、无空白、分隔符 `,`/`:`；
5. hash 使用 lowercase SHA-256 hex。

域分离公式：

```text
hash_v1(domain, payload) = sha256(utf8(domain) + 0x00 + canonical_json_bytes_v1(payload))
```

同 identity/hash 的重复对象幂等；同 identity 任一 closure 字段不同抛 `MiniQMTPluginIdentityConflict`。禁止自动补字段、排序业务 ordered effects、覆盖旧对象或返回默认成功。

## 5. K1 Contract Schemas / 契约 schema

### 5.1 `ExecutionAlgoPluginManifestV2`

| field | exact contract |
| --- | --- |
| `schema_version` | literal `execution_algo_plugin_manifest_v2` |
| `plugin_id` | lowercase dotted ID；current three 见 §8 |
| `algo_code` | exact uppercase code |
| `plugin_version` | strict SemVer without leading `v` |
| `provider` | `AISTOCK_DERIVED` 或 `VNPY_COMPAT` |
| `implementation_ref` | audit-only `python.module:ClassName`；不得动态 import |
| `config_schema_version/config_schema/config_schema_sha256` | strict JSON schema 与 `hash_v1("miniqmt_plugin_config_schema_v1", schema)` |
| `state_schema_version/state_schema/state_schema_sha256` | 完整 durable state schema 与对应 hash |
| `subscribed_event_types` | 去重排序的 `ALGO_START/TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/RECONCILE/EOD/OPERATOR` 子集；必须含 `ALGO_START` |
| `market_data_requirements` | 去重排序 `MarketDataRequirementV1`；按当前 side/session 生效 |
| `required_facade_methods` | pinned surface method names；去重排序 |
| `required_facade_object_fields` | DTO name -> exact field tuple |
| `supported_sides` | 非空子集 `BUY/SELL` |
| `supported_order_types` | K1 current-three 只能为 `LIMIT` |
| `supported_broker_backends` | K1 只能含 `minqmt_sim` |
| `restart_policy` | literal `DURABLE_RESTORE` |
| `source_attribution` | `SourceAttributionV1`，含 repo/commit/files+hash/license/AIstock source hashes |
| `compatibility_requirement` | `VnpyCompatibilityRequirementV1` |
| `behavior_characterization_sha256` | current core trace vectors hash |
| `behavior_contract_sha256` | 下述 behavior closure hash |
| `manifest_sha256` | 下述完整 manifest hash |

```text
behavior_contract_sha256 = hash_v1(
  "miniqmt_plugin_behavior_contract_v2",
  plugin_id/version + config/state schema hashes + events + requirements +
  sides/order/backend + source attribution + compatibility requirement +
  behavior_characterization_sha256
)

manifest_sha256 = hash_v1(
  "execution_algo_plugin_manifest_v2",
  all fields except manifest_sha256
)
```

`plugin_id + plugin_version` 唯一；同 key 只有完整 manifest hash 相同才幂等。相同 `algo_code` 只能有一个 active registration；多版本可留在 registry history 供 state restore，但创建新 instance 只使用 snapshot 指定版本，不自动选 latest。

### 5.2 `MarketDataRequirementV1` 与 `GatewayCapabilityCatalogV1`

`MarketDataRequirementV1` 字段：

- `capability`: `L1_BID/L1_ASK/DEPTH_5_BID/DEPTH_5_ASK/LAST_PRICE/LIMIT_UP_DOWN/SESSION_PHASE/TRADE_STATS/AUCTION_NATIVE`；
- `required_fields`: capability 内实际被该算法消费的精确字段；`L1_BID/L1_ASK` 只允许 `price/volume` 非空子集；
- `applicable_sides`: `BUY/SELL` 非空子集；避免 BUY 因仅 SELL 所需盘口被过度阻断；
- `event_types`: 必须是 manifest subscriptions 子集；
- `session_phases`: `OPEN_AUCTION/CONTINUOUS_AM/LUNCH_BREAK/CONTINUOUS_PM/CLOSE_AUCTION/CLOSED` 子集；
- `absence_disposition`: `WAIT_FOR_NEXT_VALID_EVENT` 或 `TERMINAL_AT_SESSION_BOUNDARY`；
- `requirement_sha256`: 其余字段的 domain-separated hash。

`MarketDataViewV2` 的 `L1_BID/L1_ASK` projection 保留该侧真实 price 和 volume，但 requirement 只检查 `required_fields`。Sniper 的对手盘要求 price+volume；BestLimit/TWAP 只要求其实际消费的 price，不能因未消费的 volume 暂缺额外阻断。任何已提供字段仍由 B0 authority 按既有 schema 校验，非法值不得当作 missing。`AUCTION_NATIVE` 只接受 source 原生 auction payload，禁止从普通 quote、last price、minute bar、旧缓存或 timer 合成。

`GatewayCapabilityCatalogV1` 字段为 `schema_version/route_id/quote_source/gateway_backend/order_types/market_data_capabilities/session_phases/idempotent_submit_by_client_ref/exact_order_id_cancel/catalog_sha256`。它是代码/adapter capability fact，不是策略包校验或人工门禁。K1 registry 只生成 compatibility receipt；K2 algo creation 才消费 receipt。static unsupported、current observation missing、supplied invalid 三种状态不得合并。

### 5.3 `RuntimeEventEnvelopeV2` 与 `AlgoEventDeliveryV1`

K1 model 必须逐字段实现整体蓝图 §5.2/§5.3：

- event：`schema_version,event_id,event_key_sha256,runtime_id,sequence,event_type,event_time_utc,monotonic_ns,source,symbol,payload_schema_version,payload,payload_sha256,source_identity,correlation`；
- delivery：`schema_version,delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,algo_delivery_sequence,previous_delivery_id,status,attempt_count,lease_owner,lease_expires_at,transition_id,last_error_json,created_at_utc,updated_at_utc`；
- event/source/payload schema/source identity 使用整体蓝图固定 composite table；model validator 不允许任意 enum 交叉组合；
- `ALGO_START` 必须 sequence 1 delivery 且 source identity closure 含 parent/plugin/config hash；
- delivery predecessor/sequence 是 K2 repository constraint；K1 仅保证 DTO 无法表达负数、空 identity 或非法状态组合。

### 5.4 State/start/transition contracts

`AlgoStateSnapshotV2` 精确字段：`schema_version,algo_instance_id,plugin_id,plugin_version,plugin_manifest_sha256,state_schema_version,transition_sequence,last_applied_delivery_sequence,last_applied_delivery_id,last_closed_delivery_sequence,state,state_sha256,last_applied_event_id,updated_at_utc`。state 先通过 manifest state schema，再计算 `hash_v1("execution_algo_state_v2", state)`；schema/hash/identity mismatch typed fail loud。

`AlgoStartContextV1` 精确字段：

- runtime/algo/parent/strategy-slot identity；
- symbol、side、canonical price/quantity、board-lot/volume-increment frozen facts；
- plugin manifest/config payload/hash；
- exact `ALGO_START` event/delivery identity；
- `DeterministicExecutionContextV1`；
- immutable contract/account/market capability projection及各自 hash；
- frozen execution plan/release/policy identity，仅作为执行输入 lineage，不含策略包内容、模型代码或 alpha signal。

`AlgoInitializationV1` 与 `AlgoTransitionV1` 均包含：`next_state,broker_commands,timer_mutations,diagnostic_observations,terminal_outcome,effect_set_sha256`。initialization 还必须引用 exact start event/delivery。ordered effect array 不排序；ordinal 是 identity 的一部分。

`BrokerCommandV2`：`schema_version,command_type(SUBMIT_LIMIT|CANCEL_ORDER),runtime_id,algo_instance_id,parent_intent_id,transition_id,ordinal,local_vt_orderid,symbol,side,order_type,price_decimal,quantity,owned_broker_order_id,reason_code,metadata,payload_sha256,command_id`。`SUBMIT_LIMIT` 禁止 broker order ID；`CANCEL_ORDER` 必须引用该 algo durable-owned local/broker mapping。K1 model 不调用 Gateway。

`TimerMutationV1`：`schema_version,mutation_type(UPSERT_ONE_SHOT|CANCEL),algo_instance_id,transition_id,ordinal,timer_name,schedule_epoch,due_at_exchange_utc,catch_up_policy,payload,payload_sha256,schedule_id,timer_occurrence_id`。CANCEL 不得伪造 due；UPSERT 必须在 session authority 可表达的边界内。

`DiagnosticObservationV1`：`schema_version,observation_id,runtime_id,algo_instance_id,event_id,transition_id,ordinal,severity(INFO|WARNING|ERROR),reason_code,message,context,context_sha256,observed_at_logical_utc`。空返回、missing contract/tick、rounded-zero、unsupported surface 和 schema failure 都必须有 typed reason；log 不能替代 failure receipt。

`effect_set_sha256` 按 `next_state_sha256 + ordered command/timer/diagnostic IDs + terminal_outcome` 计算；同 transition 输入必须得到同一 hash。K2 才规定与 repository/outbox 的原子提交。

## 6. Deterministic Execution Context / 确定性

`DeterministicExecutionContextV1` 字段：

| field | authority |
| --- | --- |
| `schema_version` | `deterministic_execution_context_v1` |
| `runtime_id/algo_instance_id/event_id/delivery_id` | durable identity |
| `plugin_manifest_sha256` | registry snapshot |
| `transition_sequence` | delivery predecessor closure |
| `logical_time_utc` | event/session authority；不是 arrival wall clock |
| `exchange_trade_date/session_epoch/session_phase` | ExchangeSessionClock projection |
| `input_projection_sha256` | state/event/read-only services closure |
| `context_sha256` | 其余完整字段 hash |

纯函数：

```text
derive_id(kind, ordinal, business_payload_sha256) =
  kind_prefix + hash_v1("miniqmt_deterministic_id_v1", context_sha256 + kind + ordinal + business_payload_sha256)

draw_u53(draw_ordinal) =
  first_53_bits(hash_v1("miniqmt_plugin_draw_v1", context_sha256 + draw_ordinal)) / 2^53
```

BestLimit 的等价 draw 为 `int(min_volume + (max_volume - min_volume) * draw_u53(n))`，每个 draw ordinal 写入 next state；retry/restart 使用相同 ordinal 得到相同 quantity。禁止用 `random.Random` 版本行为、全局 seed、进程缓存或重新抽取。action/local order/timer/diagnostic ID 均使用 `derive_id`；logical timestamp 只来自 context。

ordinal 必须从当前 transition 的 effect collector 按调用顺序分配并落入 state/receipt。duplicate ordinal、跳号、同 ordinal 不同 payload 是 terminal deterministic conflict。

## 7. Registry and Failure Semantics / Registry 与失败语义

### 7.1 Construction

```python
build_plugin_registry_v2(
    registrations: tuple[PluginRegistrationV2, ...],
    gateway_catalog: GatewayCapabilityCatalogV1,
    compatibility_surface: VnpyCompatibilitySurfaceV1,
) -> PluginRegistrySnapshotV1
```

`PluginRegistrationV2 = manifest + explicit factory callable + config/state validator callable + compatibility requirement`。factory callable 必须由 composition root 代码引用，不能从 `implementation_ref` import。

构建顺序固定：strict parse → schema/hash closure → source/behavior closure → factory/interface signature → compatibility receipt → gateway capability closure → duplicate/conflict closure → 按 `(algo_code,plugin_id,plugin_version)` 排序冻结 snapshot。任何一项失败均不发布 partial snapshot。

`PluginRegistrySnapshotV1` 包含 `schema_version,registrations,compatibility_receipts,gateway_catalog_sha256,registry_sha256`；`registry_sha256` 不含构建时间、进程 ID 或内存地址。同一输入在不同进程/顺序下必须一致。

禁止：目录扫描、namespace package scanning、`importlib` 动态字符串、Python entry point、配置文件任意 module/class、上传 zip/wheel、热 reload、自动选最高版本、未注册 fallback、捕获异常后跳过坏插件。

### 7.2 Typed failures

| reason code | exact condition | effect |
| --- | --- | --- |
| `MINIQMT_PLUGIN_MANIFEST_SCHEMA_INVALID` | type/extra/enum/field invalid | registry build fails；zero publication |
| `MINIQMT_PLUGIN_MANIFEST_HASH_CONFLICT` | schema/behavior/manifest hash mismatch | registry build fails |
| `MINIQMT_PLUGIN_REGISTRATION_CONFLICT` | duplicate key/algo active owner/factory mismatch | registry build fails |
| `MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID` | config invalid/unknown/type coercion required | caller receives typed failure；K1 shadow only |
| `MINIQMT_PLUGIN_STATE_SCHEMA_INVALID` | state unknown/missing/hash/version invalid | no restore/default state |
| `MINIQMT_PLUGIN_CAPABILITY_UNSUPPORTED` | static catalog cannot satisfy manifest | compatibility receipt failed；不发布 active registration |
| `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED` | method/signature/DTO/source/characterization mismatch | failed receipt；不发布 active registration |
| `MINIQMT_PLUGIN_DETERMINISM_CONFLICT` | same input different state/effect/ID/draw | terminal test/runtime contract failure |

error context 至少包含 registry build identity、plugin/algo/version、stage、field/path、expected/actual type/hash、source/gateway receipt identity；context 必须可 JSON 序列化。不得 `except Exception: pass`、只写日志、返回空 registry、固定 True receipt 或使用旧 algorithm 作为 fallback。

## 8. Current-Three Manifest Matrix / 三种算法

### 8.1 Common attribution and source mode

三个 plugin 均为 `provider=AISTOCK_DERIVED`、`plugin_version=2.0.0`、`restart_policy=DURABLE_RESTORE`、`supported_sides=[BUY,SELL]`、`supported_order_types=[LIMIT]`、`supported_broker_backends=[minqmt_sim]`。它们引用 §1.2 pinned source lock，并使用 `DERIVED_SOURCE_EXACT_CHARACTERIZATION` compatibility mode；K1 不把 derived core 虚称为任意第三方 plugin 通用兼容。

### 8.2 Manifest facts

| algo | plugin/state | strict plugin config | subscriptions | side-specific market data | state-specific fields |
| --- | --- | --- | --- | --- | --- |
| `SNIPER_MINIQMT` | `aistock.vnpy.sniper` / `sniper_state_v2` | `price_mode` 只能为 `LIMIT_TRIGGER_BY_BEST_QUOTE`，default 同值；无未知字段 | `ALGO_START,TICK,ORDER,TRADE,SESSION,EOD` | BUY=`L1_ASK(price,volume)`；SELL=`L1_BID(price,volume)`；continuous only | `vt_orderid` |
| `BEST_LIMIT_MINIQMT` | `aistock.vnpy.best_limit` / `best_limit_state_v2` | required strict positive int `min_volume,max_volume`，且 max>=min；无未知字段 | `ALGO_START,TICK,ORDER,TRADE,SESSION,EOD` | BUY=`L1_BID(price)`；SELL=`L1_ASK(price)`；continuous only | `vt_orderid,order_price_decimal,next_draw_ordinal` |
| `TWAP_LITE_MINIQMT` | `aistock.vnpy.twap_lite` / `twap_lite_state_v2` | required strict positive int `time,interval`，time>=interval；aliases 只由 legacy projection 处理 | `ALGO_START,TICK,TIMER,ORDER,TRADE,SESSION,EOD` | BUY=`L1_ASK(price)`；SELL=`L1_BID(price)`；continuous only | `order_volume,timer_count,total_count,last_market_data_id` |

TWAP 虽然 `on_tick` 不发单，仍消费 TICK 更新 exact `last_market_data_id`，TIMER 只能读取该 durable latest view，不得读取进程缓存或 minute bar。session/EOD 用于午休、收盘 residual 自动闭合；不存在人工恢复。

common state 必须包含 `algo_name,algo_code,symbol,side,offset,limit_price_decimal,parent_quantity,min_volume,volume_increment,status,traded_quantity,traded_price_decimal,active_orders,parameters,variables,last_tick_lineage,finished_reason`。`active_orders` 按 local order ID 规范排序，每项含 command/broker mapping status；禁止只保存当前 `audit_metadata()` 的子集后在重启时猜测。

### 8.3 Legacy `algo_config` projection

现有输入把算法参数与 adapter/runtime controls 混在一个 object。K1 新增 pure `LegacyVnpyPolicyProjectionV1`，只用于 shadow comparison：

| category | keys |
| --- | --- |
| plugin config | Sniper `price_mode`；BestLimit `min_volume,max_volume`；TWAP `time,interval`；TWAP aliases `duration_seconds,interval_seconds` 规范到 canonical key且冲突时报错 |
| legacy timer driver | `timer_iterations`；保留 raw/hash，但不进入 plugin config；K2/K3 由真实 timer 替代 |
| kernel order controls | `time_in_force_seconds,max_cancel_replace,marketable_limit_cross_ticks,marketable_limit_protection_band_pct,price_tick`；保留为 separate projection |

alias 同时存在且值不同、未知 key、bool-as-number、空白/非有限值必须在 shadow receipt 中显式报告。K1 不据此阻断当前 run；K3 切换前必须完成对所有 active release/policy 的 read-only inventory 与 parity，未知真实字段不得删除、默认或静默忽略。该 inventory 是兼容性证据，不是人工审批门禁。

## 9. vn.py Compatibility Surface and Receipt / 兼容面

### 9.1 Exact methods

`VnpyCompatibilitySurfaceV1` 固定：

| surface | exact return/error contract |
| --- | --- |
| `send_order(algo,direction,price,volume,order_type,offset)->str` | collector 生成 typed `SUBMIT_LIMIT` 与 deterministic local ID；missing contract/rounded zero 返回 upstream-compatible empty string并同时生成 durable typed diagnostic；不调用 Gateway |
| `cancel_order(algo,vt_orderid)->None` | 仅生成 owned exact cancel；unknown ownership typed fail loud |
| `get_tick(algo)->TickData|None` | 从当前 immutable market view 投影；None 必须伴随 durable wait/diagnostic |
| `get_contract(algo)->ContractData|None` | 从 frozen contract projection；缺失返回 None + diagnostic，不猜 exchange/min_volume |
| `write_log(msg,algo=None)->None` | 生成 bounded diagnostic；不能代替 error/receipt |
| `put_algo_event(algo,data)->None` | 收集 strict parameter/variable/status projection；不启动 EventEngine |
| `update_tick/update_timer/update_order/update_trade` | 只由 exact delivery route 调用；顺序等于 per-algo delivery sequence |

required object fields：

- `TickData`: `vt_symbol,datetime,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1`；
- `OrderData`: `vt_orderid,status,traded,price,is_active()`；
- `TradeData`: `vt_orderid,vt_tradeid,price,volume,datetime`；
- `ContractData`: `symbol,exchange,gateway_name,min_volume,pricetick`；
- enums: `Direction.LONG/SHORT,Offset.NONE,OrderType.LIMIT,AlgoStatus.RUNNING/PAUSED/STOPPED/FINISHED`。

未声明 attribute/callback、dynamic `__getattr__`、no-op default 或 unsupported order type 必须抛 `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED`。K1 receipt 证明 surface/requirement 闭合；K4 才实现 transition-scoped façade collector。

### 9.2 `VnpyCompatibilityRequirementV1` / receipt

requirement 字段：`schema_version,mode,upstream_repo,upstream_commit,source_files_and_hashes,required_method_signatures,required_object_fields,required_enum_values,characterization_sha256,requirement_sha256`。

receipt 字段：`schema_version,plugin_id,plugin_version,manifest_sha256,requirement_sha256,surface_sha256,source_lock_sha256,method_signature_sha256,object_field_sha256,characterization_sha256,gateway_capability_catalog_sha256,status(PASSED|FAILED),ordered_failures,receipt_sha256`。receipt 不含 wall clock；相同输入必须 byte-identical。FAILED receipt 保留全部 bounded failures，但对应 registration 不进入 active registry snapshot；不得只记录第一个错误后把其余 registration 当成功。

## 10. Implementation Plan and K1 Slices / 实施方案与开发切片

### K1-A — strict contracts/canonical/determinism（2–3 人日）

- 实现 §4-§6 model、codec、error 与 deterministic helpers；
- 先写 malformed type/hash/identity/time/decimal/draw RED tests；
- 不接 runtime/repository。

### K1-B — registry/current-three manifests（3–4 人日）

- 实现 immutable registry、source/behavior closure、current-three schema/manifest；
- 实现 legacy policy shadow projection；
- 对旧 registry metadata/config/trace 做 direct parity；
- 不改变 `VNPY_STYLE_ASSETS` 产品调用。

### K1-C — compatibility/import boundary（2–3 人日）

- 实现 locked surface、requirement/receipt；
- AST + isolated import negative matrix；
- registry order-independence、partial-build failure、pinned source/DTO/signature characterization；
- 完成 K1 compliance/readback receipt。

总计 7–10 人日、预计 1–2 PR。若拆为两 PR，K1-A 只提供不可被产品 runtime 导入的合同模块；K1-B/C 合并前不得宣称 registry/current-three 完成。

## 11. Verification Plan and Routing / 验证方案

### 11.1 Direct tests

- strict model：null/empty/whitespace/object/list/number/bool identity、extra、enum、nonfinite、naive datetime、decimal scale、JSON key collision；
- canonical/hash：dict insertion order、process/restart、manifest set order、ordered effects、same identity/different closure；
- registry：registration input permutation、duplicate key、same algo active conflict、factory/signature mismatch、bad source/hash、unsupported capability、FAILED receipt、zero partial publication；
- deterministic：same context retry/restart、different transition/ordinal、BestLimit u53 boundary、duplicate/skip ordinal、no wall-clock/uuid/global random imports；
- manifests：三算法 config 正反 matrix、side-specific capability、TWAP tick/timer、complete state schema、source/behavior hash；
- legacy projection：aliases same/different、adapter controls preserved、unknown key visible、no runtime effect；
- compatibility：pinned method signatures、DTO fields、enum、source hashes、empty-return diagnostic、unknown attribute/no-op forbidden；
- import：StrategyPackage/Selection/model/QE/DB/xtquant/FastAPI/runtime/repository/gateway/OMS 禁止依赖。

### 11.2 Existing characterization reused

- `test_miniqmt_vnpy_algo_parity_sniper.py`
- `test_miniqmt_vnpy_algo_parity_best_limit.py`
- `test_miniqmt_vnpy_algo_parity_twap.py`
- `test_miniqmt_vnpy_algo_restart_recovery.py`
- `test_miniqmt_vnpy_algo_import_boundary.py`
- `backend/tests/trading_core/test_vnpy_style_execution_assets.py`

K1 implementation只运行 changed files 经 `file_ownership.yaml -> module_registry.yaml -> test_plans.yaml` 实际选择的模块：

- `backend/services/miniqmt_execution_runtime/**` 与其测试：`miniqmt_execution_runtime` → `l0 + miniqmt_execution_runtime_l2`；
- `backend/execution_algos/**` 与 execution asset test：primary `paper_v2`、impact `miniqmt_execution_runtime/simulation_runtime/qe.core/strategy_package`；只在 classifier 因真实 changed files/共享合同选中时运行对应 required plan，不手工扩大到无关前后端；
- docs-only K1 design PR：只运行 `l0`、module registry/catalog checks 与两个 F2 design validators，不运行产品 L2。

任何 unmapped/ambiguous owner、计划未实际选择、直接测试只测 helper 不走 production construction seam，都不得标记 K1 ready。

### 11.3 Coverage and evidence

K1 新 Python 文件 statement/branch coverage 不低于所属 critical module 现行阈值；contracts、registry conflict、canonical error、deterministic draw 和 compatibility failure 必须有直接 branch coverage。PR receipt 分别记录 RED、GREEN、classifier、actual sessions、F2 validator、DESIGN-COMPLIANCE-001；不以 mock-only 或 CI job skipped 作为通过。

## 12. Risks and Failure Modes / 风险与失败模式

| risk/failure | required response |
| --- | --- |
| strict schema 误把历史混合 config 当 plugin config | K1 shadow receipt 保留 raw/hash 和分类；K3 前 read-only inventory/parity；不得在 K1 阻断 run、删除字段或放宽 schema |
| manifest hash 更新掩盖行为变化 | source hash、schema hash、characterization hash、plugin version 必须共同闭合；行为变化必须新增/更新 RED/GREEN vectors |
| registry 重新形成 hard-coded algorithm branch | composition root 只提交 registration tuple；kernel 不按 algo_code 分支；新增算法只增 registration/plugin/tests |
| capability 过度门禁 | `applicable_sides + required_fields` 精确到真实消费字段；static unsupported/current missing/supplied invalid 分层 |
| deterministic helper 仍依赖 Python random/wall clock | AST denylist + multi-process/restart exact vectors；hash-based u53 与 logical time 是唯一 authority |
| compatibility lock 自证循环 | pinned upstream commit/file hash、surface descriptor、characterization vectors 三方闭合；CI 不联网且升级独立评审 |
| K1 shadow 被误当生产切换 | K1 无 runtime import/wiring；progress 明确 design/source 与 K2/K3/runtime 状态分离 |
| typed error builder 因 malformed value 二次失败 | JSON-safe evidence codec 先行；非法 carrier 不进入 hash/set/sort；直接 arbitrary-JSON negative matrix |

以上失败均不得通过默认值、静默跳过、旧算法 fallback、人工 acknowledge 或全局 stop gate 掩盖。

## 13. Rollout, Rollback and Production Gates / 发布、回滚与生产门禁

K1 rollout：

1. 合入 strict modules/manifests/tests，但不接入 runtime construction；
2. build/shadow comparison 只在测试和只读 diagnostics seam 使用，不改变 run、parent、child 或 broker；
3. K2/K3 各自再完成 durable schema/dispatcher 与 runtime switch；
4. K3 切换前必须证明 current-three before/after trace、active release config projection、restart/timer/ID parity；
5. 不设置永久 feature flag、人工审批或并行产品 route。

K1 rollback 是 source revert：因没有 DB、配置、runtime switch 或 broker side effect，不需要 DDL/DML/数据 repair。若 shadow comparison 发现 drift，保留 typed evidence并修正 contract/实现，现有 runtime 继续按原 authority 运行；禁止通过放宽 schema、忽略字段或 fallback 掩盖。

状态必须分开报告：

- `source_merge`: K1 PR 独立状态；
- `close_sync`: feature design/implementation 不登记 BUG 时为 `not_applicable`；
- `production_ddl_gate=noop`；
- `production_dml_gate=noop`；
- backend/frontend dependency gate `noop`；
- production config/binding/broker/restart/runtime observation `noop` for K1 design；
- K1 source merge 不等于 K2/K3 implemented 或 production runtime activated。

### 13.1 Production Gates / 生产门禁

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_config_gate=noop`
- `production_binding_gate=noop`
- `production_broker_gate=noop`
- `service_restart=noop`
- `runtime_observation=noop_for_design`

## 14. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-053` | K1 模块/依赖/import boundary 固定，插件不能越权到信号、runtime owner、DB 或 broker |
| `F-054` | strict DTO、canonical JSON/hash、identity/type/time/decimal/error evidence contract 可直接实现且 writer/readback 同 schema |
| `F-055` | code-owned immutable registry、factory binding、duplicate/conflict/partial-publication 语义完整，无动态加载或默认插件 |
| `F-056` | deterministic logical time、ID、ordinal 与 hash-based random 在 retry/restart 下 byte-stable，无 wall clock/UUID/global random |
| `F-057` | Sniper/BestLimit/TWAP current-three manifest/config/state/event/capability/source 与 legacy projection 完整且不改变现有 runtime |
| `F-058` | pinned vn.py source/method/DTO/enum/return/error compatibility lock 与 immutable receipt 精确，无通用兼容夸大或 no-op |
| `F-059` | K1 direct/negative/parity/import tests、changed-file ownership routing、coverage 与 failure evidence 达到 critical module 实施条件 |
| `F-060` | K1 rollout/rollback、K2/K3/K4 边界、无 fallback/人工门禁/平行 route 及生产状态分离完整 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-053` | §3 target modules/dependency/import denylist | target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` AST + isolated import | design_ready | none |
| `F-054` | §4-§5 target `backend/services/miniqmt_execution_runtime/plugin_contracts.py` and `plugin_canonical.py` | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` strict/canonical/error matrix | design_ready | none |
| `F-055` | §7 target `backend/services/miniqmt_execution_runtime/plugin_registry.py` | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py` permutation/conflict/zero-partial matrix | design_ready | none |
| `F-056` | §6 target `backend/services/miniqmt_execution_runtime/deterministic_context.py` | target `backend/tests/miniqmt_execution_runtime/test_deterministic_execution_context.py` retry/restart/draw/ordinal matrix | design_ready | none |
| `F-057` | §1、§8 target `backend/execution_algos/vnpy_style/plugin_manifests.py` | target `backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py` + existing current-three parity/restart test paths | design_ready | none |
| `F-058` | §1.2、§9 target `backend/execution_algos/vnpy_compat/locked_surface.py` and `receipts.py` | target `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py` pinned source/signature/DTO/error characterization | design_ready | none |
| `F-059` | §11 ownership/catalog/test plans and coverage | command: `python -m nox -s l0`；target plan `miniqmt_execution_runtime_l2` only when implementation changed files select it | design_ready | none |
| `F-060` | §2、§10、§12-§13 state-separated rollout/rollback/gates | artifact: `docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`；command: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md --tier F2` | design_ready | none |

## 16. DESIGN-COMPLIANCE-001 / 正式复核

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | DTO/registry/determinism/current-three/source receipt/import/test/rollout 全部定义；没有把三个 helper 或 mock registry 宣称为统一架构完成 |
| no silent error/fake success | pass | schema/hash/identity/config/source/capability/determinism 全部 typed fail loud；FAILED receipt 不发布；无 exception pass、空 registry、固定 True ACK |
| no business semantic drift | pass | K1 shadow-only；signal/target/side/quantity/A股规则/B0/OMS/Gateway/唯一 broker route 不变；current-three parity 是切换前置证据 |
| no unauthorized gate/approval | pass | registry/capability 是自动技术合同；无 RBAC、审批、acknowledge、confirm-run、人工恢复、永久 enable flag |
| no fallback/parallel route | pass | 无 LEGACY/minute/default-algo fallback，不启动第二 vn.py EventEngine/OMS/Gateway；旧 runtime 在 K3 前仍是唯一产品 authority |
| no nondeterministic hidden state | pass | logical time、identity、ordinal、draw 明确派生；禁止 wall clock/UUID/global random/process cache |
| production state separated | pass | design/source/DDL/DML/dependency/config/binding/broker/restart/runtime 分开；本设计所有生产 gate 为 noop |

## 17. Definition of Done / K1 完成定义

只有同时满足以下条件，K1 implementation 才能从 `design_ready` 更新为 `implemented_verified`：

1. `F-053..F-060` 均有最终 HEAD 对应的 source/test receipt；
2. strict writer/readback schema、canonical/hash 与 error evidence negative matrix 全绿；
3. registry 对 registration 顺序无关、冲突全拒绝、任一失败零 partial publication；
4. current-three manifests/config/state/source/trace 与旧实现 parity，legacy controls 无损可见；
5. deterministic ID/time/draw 在 retry/restart/不同进程重放一致；
6. pinned vn.py source/signature/DTO/enum/return/error receipt 全闭合；
7. import denylist 和真实 construction seam 测试通过，无动态加载/平行 runtime；
8. changed-file classifier 无 unmapped/ambiguous，实际 required sessions 与 coverage 通过；
9. DESIGN-COMPLIANCE-001 逐项复核通过；
10. source merge、close-sync、生产 gates 和 runtime 状态分别报告；不得把 K1 合入写成 K2/K3/K4 或生产激活完成。
