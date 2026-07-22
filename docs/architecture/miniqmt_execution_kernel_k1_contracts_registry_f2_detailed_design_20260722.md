# MiniQMT 统一执行内核 K1 Contracts / Registry F2 详细设计

> 权威关系：本文是 [`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md) 的 K1 下位实施合同；模拟盘唯一上位权威仍是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。冲突时依次以上位蓝图、整体内核蓝图、本文为准。本文不得改写 StrategyPackage admission、Selection/Target、`B0_QUOTE_V2`、OMS、Gateway 或唯一 broker route 的 owner。
>
> Feature tier：`F2`。文档状态：`design_ready`；实现状态：`K1-A implemented_verified`，`K1-B/K1-C not_started`，K1 overall `in_progress`。
>
> 当前实现基线：`origin/main@b03f368a6bfe188219478eca542b0a4c8bde3c76`，审核补修日期 2026-07-23。
>
> 交付边界：本文只定义 K1 contracts、code-owned plugin catalog/registry construction、deterministic context、current-three manifests、pinned vn.py compatibility lock/receipt 和 import boundary。本文不实施产品代码、不执行 DDL/DML、不修改生产配置、不调用 broker、不启动、停止或重启服务。

## 0. Implementation Decision / 实施决策

K1 不切换现有 runtime，而是在现有唯一 `MiniQMTExecutionRuntime` 内建立下一阶段可直接消费的严格合同层：

1. contracts 使用 strict、frozen、`extra=forbid` 的 Pydantic v2 model 与同一个 canonical codec；任何 writer/readback 使用同一 schema；
2. plugin catalog 是 route-independent、代码拥有、封闭集合、一次构建成功或整体失败的 deeply immutable snapshot，不扫描目录、不加载 entry point、不接受文件路径或用户上传插件；
3. serializable manifest/registration descriptor 与 process-local factory binding 分离：`implementation_ref` 只用于审计，实际 factory 只能由 composition root 显式传入 callable，callable 不进入 durable hash；新实例由 hash-covered creation binding 选择 exact plugin key；
4. current-three 在 K1 生成 manifest/compatibility receipt，但产品 runtime 继续使用现有 `VNPY_STYLE_ASSETS/create_vnpy_style_core`；K3 parity 完成前不得切换；
5. plugin config、kernel controls 与历史混合 `algo_config` 的投影必须可证明且无损；K1 只做 shadow comparison，不拒绝或改变现有 production run；
6. 时间、ID 与随机 draw 全部由 immutable event/session identity 派生；禁止 `datetime.now/utcnow`、`uuid4` 和 process-global random；
7. capability 校验是 per-plugin/per-route receipt，只证明执行技术兼容性；FAILED receipt 不改变 catalog、不影响其它 plugin，不读取或二次校验策略包，不新增审批、RBAC、人工 acknowledge、人工恢复或 enable gate；
8. K1 没有 broker side effect、数据库 migration 或平行 runtime，也没有 LEGACY/minute/default-algo fallback。

## 1. Background and Current-State Evidence / 背景与当前事实

### 1.1 定向代码事实

| current artifact | observed contract/debt | K1 treatment |
| --- | --- | --- |
| `backend/execution_algos/vnpy_style/registry.py` | `VNPY_STYLE_ASSETS` 硬编码 core class；`str/int/float` 宽松转换；未知 config 可穿透 | 新 plugin catalog 严格构建；旧 registry 在 K3 前保留且不作为 V2 authority |
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
- route-independent immutable code-owned plugin catalog 与 deterministic snapshot；
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
plugin_canonical                         # dependency leaf
plugin_contracts -> plugin_canonical + jsonschema
deterministic_context -> plugin_contracts + plugin_canonical
plugin_registry -> plugin_contracts + plugin_canonical + deterministic_context
current-three manifest descriptors -> plugin_contracts + plugin_canonical
vnpy_compat locked surface/receipts -> plugin_contracts + plugin_canonical
composition root -> plugin_registry + explicit process bindings + creation bindings
```

`jsonschema` 只用于校验 code-owned manifest 的 config/state JSON Schema 定义以及 writer/readback instance；它是仓库已锁定的后端依赖，不提供网络、动态插件加载、默认值注入或运行时 gate。schema `$ref/$dynamicRef` 只允许当前 document 内可解析到现存目标的 `#`/`#/...` local JSON pointer，外部 URI、anchor alias 或 missing target 在 validator 运行前拒绝，禁止隐式网络 retrieval。校验必须先 `check_schema`，再汇总确定性排序后的 instance violations；不得只 hash schema 而不执行 schema。

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

`frozen=True` 只冻结 model attribute，不足以冻结嵌套 `dict/list`。因此任何进入 identity/hash/snapshot 的 JSON carrier 禁止保存为裸 `dict/list`，必须先转换为递归不可变 `FrozenJsonValueV1`：

```text
FrozenJsonScalarV1 = null | strict bool | strict int | strict string
FrozenJsonArrayV1  = tuple[FrozenJsonValueV1, ...]
FrozenJsonMemberV1 = frozen(key: strict string, value: FrozenJsonValueV1)
FrozenJsonObjectV1 = tuple[FrozenJsonMemberV1, ...]  # key Unicode code-point sorted
FrozenJsonValueV1  = FrozenJsonScalarV1 | FrozenJsonArrayV1 | FrozenJsonObjectV1
```

`FrozenJsonValueV1` 字段使用 Pydantic `BeforeValidator(freeze_json_v1)` 和 explicit serializer；`freeze_json_v1()` 在 strict type validation 前深拷贝并递归校验，不能依赖 Pydantic 把 list/dict 自动转 tuple。三个公开 marker constructor 自身也必须递归 freeze、校验 member type/key、拒绝 duplicate key 并规范排序；不得把“已经是 marker”当作可信输入后原样保留 caller-owned nested value。`thaw_json_v1()` 每次返回新的普通 JSON view，调用方修改 view 不得反向修改 model。`config_schema/state_schema/payload/state/source_attribution/context/metadata/correlation/ordered_failures` 等字段均存 `FrozenJsonValueV1` 或具体 frozen submodel。禁止用 `MappingProxyType` 包住仍可变的 nested value，也禁止把 caller 原始 dict 引用留在 private attribute。

补充规则：

- identity 必须为非空、首尾无空白的 `str`；不得 `str(value)`；
- `bool` 不得作为 `int/float`；数量为严格正整数或模型明确允许的零；
- money/price/rate 使用 canonical decimal string，validator 通过 `Decimal` 证明 finite、scale/range 后保存规范字符串；禁止 hash binary float；
- datetime 必须 timezone-aware，规范为 UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`；naive、offset ambiguity 拒绝；
- 外部 mapping 只允许字符串 key；duplicate key、非 JSON 类型、NaN/Infinity、bytes、set/tuple carrier 拒绝；内部 tuple 只由 `freeze_json_v1()` 生成，canonical serializer 根据 frozen object/array 类型分别输出 JSON object/array；
- enum 只接受精确值，不大小写猜测；legacy normalization 只能在独立 projection 中执行并保留原值/hash；
- error context 先经过 JSON-safe evidence codec；malformed value 只进入 evidence，不进入 set/sort/Counter，错误构造器不得二次抛异常。异常对象的 `__str__`、自定义 Mapping/BaseModel evidence renderer 再次失败时必须输出原类型与 `*_render_error_type`，不得覆盖 primary failure 或返回空 context。

### 4.2 Canonical JSON V1

`canonical_json_bytes_v1(value)` 固定为：

1. 目标 strict model 的具体 serializer 把 frozen submodel/FrozenJsonValueV1 投影为新 JSON value；不得使用通用 `default=str`、object repr 或 caller-owned mapping；
2. 递归拒绝非 `null/bool/int/str/list/object`；decimal/datetime 已在 model 层规范为 string；
3. object key 按 Unicode code point 升序；array 保留业务顺序；声明为 set-semantics 的 manifest 字段在 model validator 中先去重并按 enum/string 升序转 tuple；
4. UTF-8、`ensure_ascii=False`、无 BOM、无空白、分隔符 `,`/`:`；
5. hash 使用 lowercase SHA-256 hex。

域分离函数同时保留 raw digest 与 hex 表达，禁止从 hex 字符串的 ASCII bytes 重新解释 bit：

```text
digest_bytes_v1(domain, payload) =
  sha256(utf8(domain) + 0x00 + canonical_json_bytes_v1(payload)).digest()  # exact 32 bytes

hash_hex_v1(domain, payload) = lowercase_hex(digest_bytes_v1(domain, payload))  # exact 64 chars
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
| `config_schema_version/config_schema/config_schema_sha256` | strict frozen JSON schema 与 `hash_hex_v1("miniqmt_plugin_config_schema_v1", schema)` |
| `state_schema_version/state_schema/state_schema_sha256` | 完整 durable frozen state schema 与 `hash_hex_v1("miniqmt_plugin_state_schema_v1", schema)` |
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

```json
{
  "behavior_hash_domain": "miniqmt_plugin_behavior_contract_v2",
  "behavior_hash_payload_exact_keys": [
    "plugin_id", "algo_code", "plugin_version", "provider", "implementation_ref",
    "config_schema_version", "config_schema_sha256", "state_schema_version", "state_schema_sha256",
    "subscribed_event_types", "market_data_requirements", "required_facade_methods",
    "required_facade_object_fields", "supported_sides", "supported_order_types",
    "supported_broker_backends", "restart_policy", "source_attribution",
    "compatibility_requirement", "behavior_characterization_sha256"
  ],
  "manifest_hash_domain": "execution_algo_plugin_manifest_v2",
  "manifest_hash_payload": "exact manifest object with manifest_sha256 omitted and every other field retained"
}
```

`behavior_contract_sha256 = hash_hex_v1(behavior_hash_domain, object containing exactly behavior_hash_payload_exact_keys)`；`manifest_sha256 = hash_hex_v1(manifest_hash_domain, manifest_hash_payload)`。字段使用 keyed JSON object，不允许字符串拼接或依赖 Python field declaration order。

`plugin_id + plugin_version` 唯一；同 key 只有完整 manifest hash 相同才幂等。相同 `algo_code` 可以登记多个历史可恢复版本，但 code-owned catalog 必须且只能用一个 hash-covered `creation_binding` 指定新实例的 exact PluginKey；历史 restore 使用 durable snapshot key，不自动选 latest、不把旧版本删除，也不通过 Gateway capability 改写 creation binding。

### 5.2 `MarketDataRequirementV1` 与 `GatewayCapabilityCatalogV1`

`MarketDataRequirementV1` 字段：

- `capability`: `L1_BID/L1_ASK/DEPTH_5_BID/DEPTH_5_ASK/LAST_PRICE/LIMIT_UP_DOWN/SESSION_PHASE/TRADE_STATS/AUCTION_NATIVE`；
- `required_fields`: capability 内实际被该算法消费的精确字段；`L1_BID/L1_ASK` 只允许 `price/volume` 非空子集；
- `applicable_sides`: `BUY/SELL` 非空子集；避免 BUY 因仅 SELL 所需盘口被过度阻断；
- `event_types`: 必须是 manifest subscriptions 子集；
- `session_phases`: `OPEN_AUCTION/CONTINUOUS_AM/LUNCH_BREAK/CONTINUOUS_PM/CLOSE_AUCTION/CLOSED` 子集；
- `absence_disposition`: `WAIT_FOR_NEXT_VALID_EVENT` 或 `TERMINAL_AT_SESSION_BOUNDARY`；
- `requirement_sha256`: `hash_hex_v1("miniqmt_market_data_requirement_v1", exact preceding fields)`。

`MarketDataViewV2` 的 `L1_BID/L1_ASK` projection 保留该侧真实 price 和 volume，但 requirement 只检查 `required_fields`。Sniper 的对手盘要求 price+volume；BestLimit/TWAP 只要求其实际消费的 price，不能因未消费的 volume 暂缺额外阻断。任何已提供字段仍由 B0 authority 按既有 schema 校验，非法值不得当作 missing。`AUCTION_NATIVE` 只接受 source 原生 auction payload，禁止从普通 quote、last price、minute bar、旧缓存或 timer 合成。

`GatewayCapabilityCatalogV1` 字段为 `schema_version/route_id/quote_source/gateway_backend/order_types/market_data_capabilities/session_phases/idempotent_submit_by_client_ref/exact_order_id_cancel/catalog_sha256`；`catalog_sha256 = hash_hex_v1("miniqmt_gateway_capability_catalog_v1", exact preceding fields)`。它是代码/adapter capability fact，不是策略包校验或人工门禁，也不进入 plugin catalog hash。K1 route evaluator 生成 per-plugin/per-route `PluginRouteCompatibilityReceiptV1`；K2 algo creation 才消费该 receipt。static unsupported、current observation missing、supplied invalid 三种状态不得合并。

### 5.3 `RuntimeEventEnvelopeV2` 与 `AlgoEventDeliveryV1`

K1 model 必须逐字段实现整体蓝图 §5.2/§5.3：

- event：`schema_version,event_id,event_key_sha256,runtime_id,sequence,event_type,event_time_utc,monotonic_ns,source,symbol,payload_schema_version,payload,payload_sha256,source_identity,correlation`；
- delivery：`schema_version,delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,algo_delivery_sequence,previous_delivery_id,status,attempt_count,lease_owner,lease_expires_at,transition_id,last_error_json,created_at_utc,updated_at_utc`；
- event/source/payload schema/source identity 使用整体蓝图固定 composite table；每种 event 的 source identity key set 必须与表中 exact set 相等，missing/extra 都拒绝，model validator 不允许任意 enum 交叉组合；
- `ALGO_START` 必须 sequence 1 delivery；source identity exact closure 为 `algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,algo_code,plugin_id,plugin_version,plugin_manifest_sha256,plugin_config_sha256`，并重新计算整体蓝图 §5.5 的 `mqalgo_*`；
- `delivery_id = "mqdelivery_" + hash_hex_v1("miniqmt_algo_event_delivery_identity_v1", {event_id,algo_instance_id,plugin_manifest_sha256})`；writer 与 readback model validator 使用同一公式，不接受 caller-supplied alias；
- delivery predecessor/sequence 是 K2 repository constraint；K1 仅保证 DTO 无法表达负数、空 identity 或非法状态组合。

### 5.4 State/start/transition contracts

`AlgoStateSnapshotV2` 精确字段：`schema_version,algo_instance_id,plugin_id,plugin_version,plugin_manifest_sha256,state_schema_version,transition_sequence,last_applied_delivery_sequence,last_applied_delivery_id,last_closed_delivery_sequence,state,state_sha256,last_applied_event_id,updated_at_utc`。authoritative writer 必须同时接收 exact manifest 与 `DeterministicExecutionContextV1`：先 `check_schema`，再按 manifest state schema 校验 state、deep-freeze 并计算 `hash_hex_v1("execution_algo_state_v2", state)`；readback 必须调用同一 `validate_against_authority_v1(manifest, context)` 重新证明 plugin/schema/algo/hash/time closure。`updated_at_utc` 只能取当前 context logical time，不接受 caller timestamp，不读取 commit wall clock。

`AlgoStartContextV1` 精确字段：

- runtime/algo/parent/strategy-slot identity；
- symbol、side、canonical price/quantity、board-lot/volume-increment frozen facts；
- plugin manifest/config payload/hash；config 必须通过 manifest config schema，不能只验证 hash；
- exact `ALGO_START` event/delivery identity；
- `DeterministicExecutionContextV1`；
- immutable contract/account/market capability projection及各自 hash；
- frozen execution plan/release/policy identity，仅作为执行输入 lineage，不含策略包内容、模型代码或 alpha signal。

`AlgoInitializationV1` 与 `AlgoTransitionV1` 均包含：`next_state,broker_commands,timer_mutations,diagnostic_observations,terminal_outcome,effect_set_sha256`。initialization 还必须引用 exact start event/delivery。ordered effect array 不排序；ordinal 是 identity 的一部分。

`BrokerCommandV2`：`schema_version,command_type(SUBMIT_LIMIT|CANCEL_ORDER),runtime_id,algo_instance_id,parent_intent_id,transition_id,ordinal,local_vt_orderid,symbol,side,order_type,price_decimal,quantity,owned_broker_order_id,reason_code,metadata,payload_sha256,command_id`。`SUBMIT_LIMIT.local_vt_orderid = "mqlocalorder_" + hash_hex_v1("miniqmt_local_order_identity_v1", {runtime_id,algo_instance_id,parent_intent_id,transition_id,ordinal,symbol,side,order_type})`；`command_id = "mqcommand_" + hash_hex_v1("miniqmt_broker_command_identity_v2", exact business payload)`。writer 生成、readback 重算；相同 ID/different payload 必须在 effect hash 前拒绝。`SUBMIT_LIMIT` 禁止 broker order ID；`CANCEL_ORDER` 必须引用该 algo durable-owned local/broker mapping。K1 model 不调用 Gateway。

`TimerMutationV1`：`schema_version,mutation_type(UPSERT_ONE_SHOT|CANCEL),algo_instance_id,transition_id,ordinal,timer_name,schedule_epoch,due_at_exchange_utc,catch_up_policy,payload,payload_sha256,schedule_id,timer_occurrence_id`。`schedule_id = "mqtimersched_" + hash_hex_v1("miniqmt_timer_schedule_identity_v1", {algo_instance_id,timer_name,schedule_epoch})`；`timer_occurrence_id = "mqtimerocc_" + hash_hex_v1("miniqmt_timer_occurrence_identity_v1", {schedule_id,due_at_exchange_utc})`。effect array 中的 timer mutation identity 使用 `"mqtimermut_" + hash_hex_v1("miniqmt_timer_mutation_identity_v1", exact mutation payload)`，因此同 schedule/due 但 payload/ordinal 漂移不会共享 effect hash。CANCEL 不得伪造 due；UPSERT 必须在 session authority 可表达的边界内。

`DiagnosticObservationV1`：`schema_version,observation_id,runtime_id,algo_instance_id,event_id,transition_id,ordinal,severity(INFO|WARNING|ERROR),reason_code,message,context,context_sha256,observed_at_logical_utc`。`observation_id = "mqdiag_" + hash_hex_v1("miniqmt_diagnostic_observation_identity_v1", exact preceding fields with observation_id omitted)`；writer 从 deterministic context 取得 runtime/algo/event/logical time，readback 重算 identity，并用 `validate_against_context_v1` 证明 authority closure。空返回、missing contract/tick、rounded-zero、unsupported surface 和 schema failure 都必须有 typed reason；log 不能替代 failure receipt。

`effect_set_sha256 = hash_hex_v1("miniqmt_algo_effect_set_v1", {"next_state_sha256":...,"ordered_command_ids":[...],"ordered_timer_mutation_ids":[...],"ordered_diagnostic_observation_ids":[...],"terminal_outcome":null|...})`。五个 key 必须全部存在，且每个 ID 已由对应 DTO 的完整 business closure 重算通过；ordered arrays 保留 ordinal 顺序且不得 canonical sort。同 transition 输入必须得到同一 hash；same ID/different payload 不得形成相同 effect hash。K2 才规定与 repository/outbox 的原子提交。

## 6. Deterministic Execution Context / 确定性

`DeterministicExecutionContextV1` 字段：

| field | authority |
| --- | --- |
| `schema_version` | `deterministic_execution_context_v1` |
| `runtime_id/algo_instance_id/event_id/delivery_id` | durable identity |
| `plugin_manifest_sha256` | plugin catalog snapshot |
| `transition_sequence` | delivery predecessor closure |
| `logical_time_utc` | event/session authority；不是 arrival wall clock |
| `exchange_trade_date/session_epoch/session_phase` | ExchangeSessionClock projection |
| `input_projection_sha256` | state/event/read-only services closure |
| `context_sha256` | 其余完整字段 hash |

`context_sha256 = hash_hex_v1("miniqmt_deterministic_execution_context_v1", exact context object with context_sha256 omitted)`；所有 identity 字段、logical time/session 和 input projection hash 必须保留，禁止省略 null field 或加入 process/arrival time。

纯函数：

```text
derive_id(kind, ordinal, business_payload_sha256) =
  kind_prefix + hash_hex_v1(
    "miniqmt_deterministic_id_v1",
    {"context_sha256": context_sha256,
     "kind": kind,
     "ordinal": strict_nonnegative_int,
     "business_payload_sha256": business_payload_sha256}
  )

draw_u53(draw_ordinal) =
  ((int.from_bytes(
      digest_bytes_v1(
        "miniqmt_plugin_draw_v1",
        {"context_sha256": context_sha256, "draw_ordinal": strict_nonnegative_int}
      )[0:7], "big"
    ) >> 3) / 2^53)
```

BestLimit 的等价 draw 为 `int(min_volume + (max_volume - min_volume) * draw_u53(n))`，每个 draw ordinal 写入 next state；retry/restart 使用相同 ordinal 得到相同 quantity。禁止用 `random.Random` 版本行为、全局 seed、进程缓存或重新抽取。插件内部 action identity 使用上述 `derive_id`；具备可独立 readback exact fields 的 algo/delivery/local order/command/timer/diagnostic DTO 使用 §5 的字段闭包专用公式并由 model validator 重算，两类公式不得混用或接受 caller alias。所有 logical timestamp 只来自 context。

ordinal 必须从当前 transition 的 effect collector 按调用顺序分配并落入 state/receipt。duplicate ordinal、跳号、同 ordinal 不同 payload 是 terminal deterministic conflict。

## 7. Registry and Failure Semantics / Registry 与失败语义

### 7.1 Construction

```python
build_plugin_catalog_v2(
    descriptors: tuple[PluginRegistrationDescriptorV2, ...],
    creation_bindings: tuple[PluginCreationBindingV1, ...],
    process_bindings: PluginProcessBindingsV2,
    compatibility_surface: VnpyCompatibilitySurfaceV1,
) -> PluginCatalogRuntimeV2

evaluate_plugin_route_compatibility_v1(
    *,
    catalog_snapshot: PluginCatalogSnapshotV1,
    plugin_key: PluginKeyV1,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> PluginRouteCompatibilityReceiptV1
```

四类对象严格分层：

1. `PluginKeyV1 = plugin_id + plugin_version + manifest_sha256`；
2. `PluginRegistrationDescriptorV2 = manifest + factory_binding_id + factory_signature_sha256 + config_validator_binding_id + config_validator_signature_sha256 + state_codec_binding_id + state_codec_signature_sha256`，仅含可 canonicalize 的 frozen data；
3. `PluginProcessBindingsV2` 是 process-local sealed mapping：binding ID -> explicit callable。constructor 必须先复制 caller mapping，再以不可变 mapping view 保存 exact callable references；caller 后续修改原 dict 不得换掉已验证 binding。callable 只能由 composition root 代码引用，不能从 `implementation_ref` import；该 mapping 不进入任何 durable/canonical hash，但 callable 的 `__module__ + __qualname__` 必须等于 descriptor/manifest exact implementation ref，normalized `inspect.signature` 必须等于 descriptor signature hash，`inspect.getsourcefile` 的 repo-relative path/hash 必须与 `SourceAttributionV1.aistock_files` 闭合；只验证同 signature 而接受另一函数属于 `MINIQMT_PLUGIN_BINDING_INVALID`；
4. `PluginCreationBindingV1 = algo_code + exact PluginKeyV1`，决定新实例版本并进入 catalog hash。历史 restore 始终使用 instance snapshot 的 frozen PluginKeyV1，不查询 creation binding。

current-three `creation_bindings` 固定为：

```text
SNIPER_MINIQMT     -> aistock.vnpy.sniper / 2.0.0 / registered manifest_sha256
BEST_LIMIT_MINIQMT -> aistock.vnpy.best_limit / 2.0.0 / registered manifest_sha256
TWAP_LITE_MINIQMT  -> aistock.vnpy.twap_lite / 2.0.0 / registered manifest_sha256
```

manifest hash 在 implementation 构建时由注册对象计算，上述 binding 必须引用同一 descriptor 的实际 hash，不允许独立复制 hash 字符串。每个 active `algo_code` 必须且只能有一个 creation binding；binding 缺失、指向不存在/历史-only descriptor、algo_code 不一致均为 catalog build conflict。禁止自动 latest、数据库热选版本或 kernel 的具体算法分支。

catalog 构建顺序固定：strict parse/deep-freeze → schema/hash closure → source/behavior closure → process binding existence/signature closure → pinned vn.py compatibility receipt → duplicate/version/creation-binding closure → 按 `(algo_code,plugin_id,plugin_version,manifest_sha256)` 排序冻结 snapshot。schema/source/binding/identity 任一失败均不发布 catalog，并抛出携带完整 `PluginCatalogBuildFailureReceiptV1` 的 `PluginCatalogBuildError`。

`PluginCatalogBuildFailureReceiptV1` 字段为 `schema_version,build_input_sha256,ordered_descriptor_keys,ordered_failures,failure_set_sha256,receipt_sha256`；`failure_set_sha256 = hash_hex_v1("miniqmt_plugin_catalog_failure_set_v1", ordered_failures)`，`receipt_sha256 = hash_hex_v1("miniqmt_plugin_catalog_build_failure_receipt_v1", exact preceding fields)`。它不含 wall clock、callable repr/地址或 Gateway catalog。exception 是该 receipt 的唯一返回载体，调用方不得以空 catalog 或上一个 catalog 假成功。ordered failures 使用 stable stage/plugin/field/reason/context hash 排序并保留全部 bounded failures。

`PluginCatalogSnapshotV1` 只包含 `schema_version,registration_descriptors,pinned_compatibility_receipts,creation_bindings,catalog_sha256`；`catalog_sha256 = hash_hex_v1("miniqmt_plugin_catalog_snapshot_v1", exact preceding fields)`。`PluginCatalogRuntimeV2` 仅在 snapshot 成功后将其与已验证的 process bindings 组合；snapshot/canonical hash 从不包含 callable、构建时间、进程 ID 或内存地址。同一 descriptor/binding 输入以任意输入顺序、不同进程构建必须得到 byte-identical snapshot。

Gateway capability 不参与 plugin catalog 构建和 `catalog_sha256`。`evaluate_plugin_route_compatibility_v1` 在 algo 创建前按 exact plugin key + exact gateway catalog 生成：

```text
PluginRouteCompatibilityReceiptV1 =
  schema_version
  plugin_key / algo_code
  plugin_manifest_sha256 / catalog_sha256
  gateway_capability_catalog_sha256
  required/supported order types
  required/supported market capabilities with side/field/session detail
  status = PASSED | FAILED
  ordered_failures
  receipt_sha256
```

`receipt_sha256 = hash_hex_v1("miniqmt_plugin_route_compatibility_receipt_v1", exact preceding fields)`。FAILED route receipt 只拒绝该 plugin/route 的 algo 创建并保留 `broker_called=false`，catalog 和其它 plugin/route 继续有效。它不是人工审批，也不能写回或移除 catalog registration。

禁止：目录扫描、namespace package scanning、`importlib` 动态字符串、Python entry point、配置文件任意 module/class、上传 zip/wheel、热 reload、自动选最高版本、未注册 fallback、捕获异常后跳过坏插件。

### 7.2 Typed failures

| reason code | exact condition | effect |
| --- | --- | --- |
| `MINIQMT_PLUGIN_MANIFEST_SCHEMA_INVALID` | type/extra/enum/field invalid | catalog build fails with aggregate receipt；zero publication |
| `MINIQMT_PLUGIN_MANIFEST_HASH_CONFLICT` | schema/behavior/manifest hash mismatch | catalog build fails with aggregate receipt |
| `MINIQMT_PLUGIN_REGISTRATION_CONFLICT` | duplicate key/creation binding/factory descriptor mismatch | catalog build fails with aggregate receipt |
| `MINIQMT_PLUGIN_BINDING_INVALID` | process binding missing、callable signature mismatch 或 callable repr 被用于 hash | catalog build fails with aggregate receipt |
| `MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID` | config invalid/unknown/type coercion required | caller receives typed failure；K1 shadow only |
| `MINIQMT_PLUGIN_STATE_SCHEMA_INVALID` | state unknown/missing/hash/version invalid | no restore/default state |
| `MINIQMT_PLUGIN_CAPABILITY_UNSUPPORTED` | exact route catalog cannot satisfy exact plugin requirement | route receipt FAILED；只拒绝该 algo/route，plugin catalog 不变 |
| `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED` | method/signature/DTO/source/characterization mismatch | catalog build failure receipt；不发布 catalog |
| `MINIQMT_PLUGIN_DETERMINISM_CONFLICT` | same input different state/effect/ID/draw | terminal test/runtime contract failure |

error context 至少包含 catalog build identity、plugin/algo/version、stage、field/path、expected/actual type/hash 和适用的 source/binding/route receipt identity；context 必须可 JSON 序列化。不得 `except Exception: pass`、只写日志、返回空 catalog、复用上次 catalog、固定 True receipt 或使用旧 algorithm 作为 fallback。

## 8. Current-Three Manifest Matrix / 三种算法

### 8.1 Common attribution and source mode

三个 plugin 均为 `provider=AISTOCK_DERIVED`、`plugin_version=2.0.0`、`restart_policy=DURABLE_RESTORE`、`supported_sides=[BUY,SELL]`、`supported_order_types=[LIMIT]`、`supported_broker_backends=[minqmt_sim]`。它们引用 §1.2 pinned source lock，并使用 `DERIVED_SOURCE_EXACT_CHARACTERIZATION` compatibility mode；K1 不把 derived core 虚称为任意第三方 plugin 通用兼容。

`SourceAttributionV1` exact schema 为：`schema_version=source_attribution_v1`、`upstream_repo`、`upstream_commit`（40-char lowercase git SHA）、`upstream_files`（按 path 排序的 frozen `{path,sha256}` tuple）、`upstream_license`、`upstream_copyright`、`aistock_asset_version`、`aistock_files`（按 path 排序的 frozen `{path,sha256}` tuple）、`derivation_summary`、`attribution_sha256`。`attribution_sha256 = hash_hex_v1("miniqmt_source_attribution_v1", exact preceding fields)`；空 file set、重复 path、非 lowercase SHA 或 source map 与 manifest `implementation_ref` 不闭合均拒绝。

### 8.2 Manifest facts

| algo | plugin/state | strict plugin config | subscriptions | side-specific market data | state-specific fields |
| --- | --- | --- | --- | --- | --- |
| `SNIPER_MINIQMT` | `aistock.vnpy.sniper` / `sniper_state_v2` | `price_mode` 只能为 `LIMIT_TRIGGER_BY_BEST_QUOTE`，default 同值；无未知字段 | `ALGO_START,TICK,ORDER,TRADE,SESSION,EOD` | BUY=`L1_ASK(price,volume)`；SELL=`L1_BID(price,volume)`；continuous only | `vt_orderid` |
| `BEST_LIMIT_MINIQMT` | `aistock.vnpy.best_limit` / `best_limit_state_v2` | required strict positive int shares `min_volume,max_volume`，且 max>=min；无未知字段 | `ALGO_START,TICK,ORDER,TRADE,SESSION,EOD` | BUY=`L1_BID(price)`；SELL=`L1_ASK(price)`；continuous only | `vt_orderid,order_price_decimal,next_draw_ordinal` |
| `TWAP_LITE_MINIQMT` | `aistock.vnpy.twap_lite` / `twap_lite_state_v2` | required strict positive int seconds `time,interval`，time>=interval；aliases 只由 legacy projection 记录，不自动改写已冻结 policy | `ALGO_START,TICK,TIMER,ORDER,TRADE,SESSION,EOD` | BUY=`L1_ASK(price)`；SELL=`L1_BID(price)`；continuous only | §8.4 exact timer state |

TWAP 虽然 `on_tick` 不发单，仍消费 TICK 更新 exact `last_market_data_id`，TIMER 只能读取该 durable latest view，不得读取进程缓存或 minute bar。session/EOD 用于午休、收盘 residual 自动闭合；不存在人工恢复。

### 8.3 Exact common state schema

三个 state schema 共享以下 exact frozen fields：

| field | type/invariant |
| --- | --- |
| `algo_name` | non-empty deterministic string；等于 algo instance frozen local name |
| `algo_code/symbol/side/offset` | exact manifest algo code；recognized A-share symbol；`BUY|SELL`；literal `NONE` |
| `limit_price_decimal` | positive canonical decimal string，必须按 frozen contract `pricetick` 可整除，不硬编码 0.01 |
| `parent_quantity/min_volume/volume_increment` | strict positive integer shares；quantity closure 使用同一 board-lot authority |
| `status` | `PAUSED/RUNNING/STOPPED/FINISHED`；与 instance status 的映射由 parent contract 固定 |
| `traded_quantity` | strict integer，`0 <= traded_quantity <= parent_quantity` |
| `traded_price_decimal` | non-negative canonical decimal string；traded=0 时只能为 `0`，traded>0 时必须 positive |
| `active_orders` | 按 `local_vt_orderid` 升序的 `tuple[PluginActiveOrderStateV1,...]`；ID 唯一 |
| `parameters/variables` | `FrozenJsonObjectV1`；字段集必须与 manifest state schema exact，不接受 unknown/default |
| `last_tick_lineage` | `MarketDataLineageRefV1 | null` |
| `finished_reason` | `FINISHED` 时 non-empty reason；`PAUSED/RUNNING/STOPPED` 为 null，STOPPED reason 由对应 event/diagnostic receipt 保存，不伪造 finished reason |

`PluginActiveOrderStateV1` exact fields：`local_vt_orderid,submit_command_id,broker_order_id|null,status,requested_price_decimal,requested_quantity,cumulative_filled_quantity,last_order_event_id|null,last_trade_event_id|null,mapping_sha256`；`mapping_sha256 = hash_hex_v1("miniqmt_plugin_active_order_state_v1", exact preceding fields)`。status 固定为 `PENDING_DISPATCH/SUBMITTED/PARTIALLY_FILLED/CANCEL_PENDING/CANCELLED/FILLED/REJECTED/OUTCOME_UNKNOWN`；filled quantity 必须在 `[0,requested_quantity]`；broker ID 只能由 durable Gateway/OMS receipt 提供，禁止从 local ID 猜测。inactive order 不留在 `active_orders`，但其历史继续由 command/child/order/trade facts 重建。

`MarketDataLineageRefV1` exact fields：`market_data_id,event_id,payload_sha256,generation,sequence,exchange_time_utc,session_phase`。同 identity/hash conflict 拒绝；不得只保存裸 tick payload 或进程缓存地址。

Sniper `vt_orderid: string|null`；非 null 时必须引用唯一 active order。BestLimit 增加 `vt_orderid:string|null`、`order_price_decimal:string|null`、`next_draw_ordinal:int>=0`；无 active order 时前两者同时为 null，有 active order 时两者与该 order 完全一致。禁止只保存当前 `audit_metadata()` 子集后在重启时猜测。

### 8.4 TWAP exact units/session/restart state

`twap_lite_state_v2` 在 common state 外固定：

| field | exact contract |
| --- | --- |
| `duration_seconds` | 等于 canonical config `time`，strict positive integer exchange-active seconds |
| `interval_seconds` | 等于 canonical config `interval`，strict positive integer exchange-active seconds，且 `duration_seconds >= interval_seconds` |
| `order_volume` | strict non-negative integer shares；按现有 TWAP rounding 语义和 frozen board-lot rule 计算且不超过 parent quantity；为 0 时产生 durable `TWAP_SLICE_VOLUME_ROUNDED_ZERO` diagnostic、零 broker command，并在 duration/EOD 形成明确 residual，不伪造最小下单量 |
| `active_elapsed_seconds` | `[0,duration_seconds]`；由 APPLIED TIMER occurrence 推进 |
| `interval_elapsed_seconds` | `[0,interval_seconds-1]`；每次 slice 后归零 |
| `last_timer_occurrence_id` | 已应用 TIMER 时 non-empty；初始化为 null；同 occurrence 不重复累计 |
| `last_market_data_lineage` | `MarketDataLineageRefV1|null`；TIMER 只能读取该 durable lineage |

只有 `session_phase=CONTINUOUS_AM|CONTINUOUS_PM` 且 exact `timer_occurrence_id` 首次 APPLIED 的一秒 TIMER 才同时推进两个 elapsed counter。`OPEN_AUCTION/LUNCH_BREAK/CLOSE_AUCTION/CLOSED` 不累计 duration/interval；跨午休 due 由 ExchangeSessionClock 顺延到 PM 下一 exchange-active second，不执行 catch-up burst。`active_elapsed_seconds == duration_seconds` 或 EOD 时按 parent residual contract 终结；restart 从 snapshot + last timer occurrence 继续，不重放已计时秒、不读取 wall clock。

legacy `timer_count/total_count` 迁移分别映射为 `interval_elapsed_seconds/active_elapsed_seconds`，但必须与 durable TIMER/session evidence 和 config range 闭合；active order 由 snapshot IDs 与 durable command/child/OMS facts exact join。缺 evidence、计数越界或 identity 冲突生成 typed migration failure，不归零、不重建空 state。迁移 receipt 固定 old/new schema/hash、consumed evidence IDs、field mapping 和 receipt hash。

### 8.5 Legacy `algo_config` projection

现有输入把算法参数与 adapter/runtime controls 混在一个 object。K1 新增 pure `LegacyVnpyPolicyProjectionV1`，只用于 shadow comparison：

| category | keys |
| --- | --- |
| plugin config | Sniper `price_mode`；BestLimit `min_volume,max_volume`；TWAP canonical `time,interval`，单位均为 exchange-active seconds |
| legacy timer driver | `timer_iterations`；保留 raw/hash，但不进入 plugin config；K2/K3 由真实 timer 替代 |
| kernel order controls | `time_in_force_seconds,max_cancel_replace,marketable_limit_cross_ticks,marketable_limit_protection_band_pct,price_tick`；保留为 separate projection |

TWAP `duration_seconds/interval_seconds` 是现有 registry 曾声明的 legacy aliases，但当前 default merge 可能使 alias-only 输入仍采用默认 `time/interval`。shadow receipt 必须同时保存 raw config/hash、`legacy_effective_config` 和 `candidate_canonical_config`：alias 与 canonical 同时存在且值不同立即 conflict；alias-only 若会改变 legacy effective behavior，标记 `DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION`，K1/K3 都不得静默重解释已冻结 release。未知 key、bool-as-number、空白/非有限值同样显式报告。K1 不据此阻断当前 run；K3 切换前完成所有 active release/policy 的 read-only inventory 与 parity，未知真实字段不得删除、默认或静默忽略。该 inventory 是兼容性证据，不是人工审批门禁。

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

requirement 字段：`schema_version,mode,upstream_repo,upstream_commit,source_files_and_hashes,required_method_signatures,required_object_fields,required_enum_values,characterization_sha256,requirement_sha256`；`requirement_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_requirement_v1", exact preceding fields)`。

pinned compatibility receipt 字段：`schema_version,plugin_id,plugin_version,manifest_sha256,requirement_sha256,surface_sha256,source_lock_sha256,method_signature_sha256,object_field_sha256,characterization_sha256,status(PASSED|FAILED),ordered_failures,receipt_sha256`。它不含 Gateway catalog、wall clock 或 process binding callable；相同输入必须 byte-identical。任一 pinned compatibility FAILED 进入 `PluginCatalogBuildFailureReceiptV1` 并阻止整个 code catalog 发布，因为这表示代码/source 合同损坏，而不是当前 route 不支持。

Gateway capability 使用 §7.1 独立 `PluginRouteCompatibilityReceiptV1`。该 receipt FAILED 只拒绝 exact plugin/route algo 创建，不改变 pinned compatibility receipt、plugin catalog 或其它 plugin；不得把 route failure 伪装为 catalog build failure，也不得只记录第一个错误后把其余 route/plugin 当成功。

## 10. Implementation Plan and K1 Slices / 实施方案与开发切片

### K1-A — strict contracts/canonical/determinism（2–3 人日）

- 实现 §4-§6 model、recursive FrozenJson、codec、raw digest/hex hash、error 与 deterministic helpers；
- 先写 malformed type/hash/identity/time/decimal/draw、caller-input/returned-view nested mutation RED tests；
- 不接 runtime/repository。

当前实现状态：`implemented_verified`。实现位于 `backend/services/miniqmt_execution_runtime/plugin_canonical.py`、`plugin_contracts.py`、`deterministic_context.py`；直接证据位于 `test_algo_plugin_contracts.py` 和 `test_deterministic_execution_context.py`。初始 RED 因三个目标模块不存在产生 2 个 collection error；正式审核补充的 marker mutation、error renderer、exact source identity、same-command-ID/different-payload effect 与 schema/time authority matrix 为 5 failed。最终 GREEN 为 67 个 strict/hash/event/schema/state/effect/determinism 直接用例；changed-file classifier 为 `targeted_ci_required`、`unmapped_code_files=[]`，只选择 `miniqmt_execution_runtime_l2`，该计划 473 passed、1 skipped。当前 line+branch coverage：canonical 94%、contracts 85%、determinism 97%。schema violation evidence 最多消费并呈现 32 项且显式标记 truncation；通用 `derive_id_v1` 仅保留插件内部 `ACTION` kind，持久化 DTO 不存在可竞争的第二 identity authority。新模块未从现有 package `__init__`、runtime、repository、Gateway 或 OMS 接线；`source_merge=pending_pr_update`，不能写成 K1 complete 或 production activated。

### K1-B — registry/current-three manifests（3–4 人日）

- 实现 route-independent immutable plugin catalog、serializable descriptor/process binding split、creation bindings、aggregate build failure receipt 和 per-route compatibility receipt；
- 实现 source/behavior closure、current-three exact config/state schema/manifest；
- 实现 legacy policy shadow projection；
- 对旧 registry metadata/config/trace 做 direct parity；
- 不改变 `VNPY_STYLE_ASSETS` 产品调用。

### K1-C — compatibility/import boundary（2–3 人日）

- 实现 locked surface、requirement/receipt；
- AST + isolated import negative matrix；
- catalog order-independence、partial-build failure、pinned source/DTO/signature characterization；
- 完成 K1 compliance/readback receipt。

总计 7–10 人日、预计 1–2 PR。若拆为两 PR，K1-A 只提供不可被产品 runtime 导入的合同模块；K1-B/C 合并前不得宣称 plugin catalog/current-three 完成。

## 11. Verification Plan and Routing / 验证方案

### 11.1 Direct tests

- strict model：null/empty/whitespace/object/list/number/bool identity、extra、enum、nonfinite、naive datetime、decimal scale、JSON key collision、JSON Schema definition/config/state instance；
- canonical/hash：dict insertion order、process/restart、manifest set order、ordered effects、same identity/different closure、raw digest/hex distinction、exact keyed payload vectors、algo/delivery/local-order/command/timer/diagnostic readback recomputation；
- deep immutability：构建后修改 caller 原始 nested dict/list、修改 thawed/readback view、复用 mutable default，以及直接构造公开 marker 后修改 caller nested value，均不能改变 frozen object/hash；
- error evidence：异常 `__str__` 或 Mapping renderer 再次失败时输出 primary type 与 render error type，不能二次抛异常；
- catalog：descriptor input permutation、duplicate key、missing/duplicate creation binding、historical restore key、factory binding module/qualname/signature/source mismatch、caller binding dict mutation 不换掉 sealed callable、callable 不可进入 hash、bad source/hash、aggregate FAILED receipt、zero partial publication；
- route compatibility：一个 unsupported plugin/route 只产生 FAILED route receipt，current-three 其它 registration/snapshot/hash 不变；不同 route receipt 不改 catalog；
- deterministic：same context retry/restart、different transition/ordinal、BestLimit raw-digest u53 exact vectors、duplicate/skip ordinal、no wall-clock/uuid/global random imports；
- manifests：三算法 config 正反 matrix、side/required-field capability、complete active-order/lineage/state schema、TWAP exchange-active seconds/午休不累计/EOD/restart、source/behavior hash；
- legacy projection：canonical/alias same/different/alias-only drift、legacy effective vs candidate config、adapter controls preserved、unknown key visible、no runtime effect；
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
| catalog 重新形成 hard-coded algorithm branch | composition root 只提交 descriptor/process binding/creation-binding tuples；kernel 不按 algo_code 分支；新增算法只增 registration/plugin/tests |
| route capability 变成全局门禁 | Gateway catalog 不进入 plugin catalog build/hash；FAILED route receipt 仅拒绝 exact algo/route，其它 plugin 不受影响 |
| frozen model 嵌套值仍可修改 | 所有 hash carrier 使用 recursive FrozenJson/frozen submodel；caller input 与 thawed view mutation direct tests |
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
| `F-054` | strict DTO、recursive deep immutability、canonical raw-digest/hex hash、identity/type/time/decimal/error evidence contract 可直接实现且 writer/readback 同 schema |
| `F-055` | route-independent code-owned immutable catalog、descriptor/callable 分层、creation binding、aggregate build failure 与 per-route compatibility 语义完整 |
| `F-056` | deterministic logical time、exact keyed ID/effect hash、ordinal 与 raw-digest u53 random 在 retry/restart 下 byte-stable，无 wall clock/UUID/global random |
| `F-057` | Sniper/BestLimit/TWAP current-three exact manifest/config/state/event/capability/source、TWAP exchange-active seconds 与 legacy projection 完整且不改变现有 runtime |
| `F-058` | pinned vn.py source/method/DTO/enum/return/error compatibility lock 与 immutable receipt 精确，无通用兼容夸大或 no-op |
| `F-059` | K1 direct/negative/parity/import tests、changed-file ownership routing、coverage 与 failure evidence 达到 critical module 实施条件 |
| `F-060` | K1 rollout/rollback、K2/K3/K4 边界、无 fallback/人工门禁/平行 route 及生产状态分离完整 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-053` | §3 target modules/dependency/import denylist；K1-A dependency direction 保持，`plugin_contracts` 仅新增仓库已锁定 `jsonschema` 用于 schema authority，不连接 runtime/network/DB | changed-file classifier `targeted_ci_required`、`unmapped_code_files=[]`；target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` AST + isolated import 仍属 K1-C | design_ready | none |
| `F-054` | `plugin_contracts.py` and `plugin_canonical.py` strict models、public-marker-safe recursive FrozenJson、JSON Schema definition/instance authority、canonical decimal/time/JSON/raw digest+hex、typed JSON-safe error evidence、writer/readback hash closure | `backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` malformed identity/type/extra/hash/set permutation/duplicate JSON key/exact composite event/delivery/state/config/initialization/effect/deep-mutation/broken-renderer/bounded-schema-evidence/JSON readback matrix；direct total 67 passed；canonical 94%、contracts 85% line+branch | implemented_verified | none |
| `F-055` | §7 target `backend/services/miniqmt_execution_runtime/plugin_registry.py` | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py` descriptor/binding/creation/route-isolation/aggregate-failure matrix | design_ready | none |
| `F-056` | `deterministic_context.py`、`DeterministicExecutionContextV1` 与 contract DTO exact algo/delivery/local-order/command/timer/diagnostic/effect identity closure、raw-digest u53、BestLimit quantity、strict ordinal sequence | `backend/tests/miniqmt_execution_runtime/test_deterministic_execution_context.py` logical-time/hash/retry/readback/raw-digest/different ordinal/invalid coercion/range；`backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` same-ID/different-payload、authority-time/readback identity matrix；determinism 97% line+branch | implemented_verified | none |
| `F-057` | §1、§8 target `backend/execution_algos/vnpy_style/plugin_manifests.py` | target `backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py` exact active-order/lineage/TWAP session/legacy drift + existing parity/restart paths | design_ready | none |
| `F-058` | §1.2、§9 target `backend/execution_algos/vnpy_compat/locked_surface.py` and `receipts.py` | target `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py` pinned source/signature/DTO/error characterization | design_ready | none |
| `F-059` | §11 ownership/catalog/test plans and coverage | direct = 67 passed；`python -m nox -s miniqmt_execution_runtime_l2` = 473 passed/1 skipped；classifier `targeted_ci_required`/unmapped empty；coverage canonical/contracts/determinism = 94/85/97 | design_ready | none |
| `F-060` | §2、§10、§12-§13 state-separated rollout/rollback/gates | artifact: `docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`；command: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md --tier F2`；DESIGN-COMPLIANCE-001 | design_ready | none |

## 16. DESIGN-COMPLIANCE-001 / 正式复核

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | K1-A 不只生成 helper：writer/readback 均闭合 public marker、schema definition/instance、event/algo/delivery/effect identity 与 context time；K1-B/C 继续明确 not_started，没有把 K1-A 宣称为统一架构完成 |
| no silent error/fake success | pass | same identity/different business payload、invalid schema/state/config/source key、logical-time drift 和 malformed renderer 全部直接拒绝；error evidence 保留 renderer failure type，无 exception pass、空 state/config、固定 True ACK |
| no business semantic drift | pass | K1 shadow-only；signal/target/side/quantity/A股规则/B0/OMS/Gateway/唯一 broker route 不变；TWAP time/interval 固定 exchange-active seconds、午休不累计，legacy alias-only drift 不自动重解释；current-three parity 是切换前置证据 |
| no unauthorized gate/approval | pass | plugin catalog 与 per-route capability 分离，单 plugin/route failure 不阻止其它 plugin；无 RBAC、审批、acknowledge、confirm-run、人工恢复、永久 enable flag |
| no fallback/parallel route | pass | 无 LEGACY/minute/default-algo fallback，不启动第二 vn.py EventEngine/OMS/Gateway；旧 runtime 在 K3 前仍是唯一产品 authority |
| no nondeterministic hidden state | pass | algo/delivery/local-order/command/timer/diagnostic/effect identity 与 logical time 均从 exact persisted/context fields 重算；ordinal/draw 明确派生，禁止 wall clock/UUID/global random/process cache |
| production state separated | pass | design/source/DDL/DML/dependency/config/binding/broker/restart/runtime 分开；本设计所有生产 gate 为 noop |

## 17. Definition of Done / K1 完成定义

只有同时满足以下条件，K1 implementation 才能从 `design_ready` 更新为 `implemented_verified`：

1. `F-053..F-060` 均有最终 HEAD 对应的 source/test receipt；
2. strict writer/readback schema、recursive deep immutability、canonical raw-digest/hex hash 与 error evidence negative matrix 全绿；
3. plugin catalog 对 descriptor 顺序无关、callable 不进入 hash、creation binding exact、code/source 冲突 aggregate fail 且零 partial publication；
4. per-route unsupported 只拒绝 exact plugin/route，其它 plugin/catalog hash 不变；
5. current-three manifests/config/state/source/trace 与旧实现 parity，TWAP exchange-active seconds/午休/EOD/restart 与 legacy controls 无损可见；
6. deterministic ID/time/draw 在 retry/restart/不同进程重放一致；
7. pinned vn.py source/signature/DTO/enum/return/error receipt 全闭合；
8. import denylist 和真实 construction seam 测试通过，无动态加载/平行 runtime；
9. changed-file classifier 无 unmapped/ambiguous，实际 required sessions 与 coverage 通过；
10. DESIGN-COMPLIANCE-001 逐项复核通过；
11. source merge、close-sync、生产 gates 和 runtime 状态分别报告；不得把 K1 合入写成 K2/K3/K4 或生产激活完成。
