# MiniQMT 统一执行内核 K5 Iceberg/Stop Plugin 扩展 F2 详细设计

> Feature tier：`F2`。文档状态：`implementation_pr_open_pending_required_ci`；K5 implementation=`implemented_verified_local_pr_2978_pending_required_ci`，source merge=`pending_pr_2978`。
>
> 上位唯一实现蓝图：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一上位蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 已合入前置：K1、K2、K3、K4 overall 均为 `implemented_verified + merged`。K4-B 已通过 PR #2953 / merge `cbb5f12871f41d1fd529a9a98e8811484eac8ba0` 合入；状态同步后 `main@e594583c44b118649a0aa6a0fb9e519d7b3b1ad1` 是本文设计基线。
>
> 本文只细化父蓝图已经批准的 K5：将 K4 已固定源码、已正向表征但尚未注册的 Iceberg/Stop 接入现有 K1 catalog、K4 通用 façade-backed adapter 和 K2 shadow invocation。本文不修改 execution kernel，不切换产品 runtime，不实现 K6 per-command product authority、dependent-BUY、旧 route 退役或生产迁移。

## 0. Executive Decision / 核心结论

K5 用两个事件形态明显不同的上游算法证明 MiniQMT execution kernel 对具体算法无感知：Iceberg 由 exchange-active `TIMER` 驱动并读取同一事务中 sequence-cutoff 的 native B0 quote；Stop 由 native B0 `TICK` 触发且只允许一次触发。两者都通过 K1 manifest/catalog、K4 exact façade 和现有 K2 durable shadow seam 运行，不在 kernel、scheduler、client、repository 或 broker route 增加 algo-code 分支。

固定决策如下：

1. 新增两个 code-owned plugin：`aistock.vnpy.iceberg@1.0.0 / ICEBERG` 与 `aistock.vnpy.stop@1.0.0 / STOP`，provider 固定为 `VNPY_COMPAT`。
2. 两个 factory 只构造既有 `VnpyFacadeBackedPluginAdapterV1`，携带 K4 已验证的 exact algorithm binding；不实现第二套算法、不 import installed/latest vn.py。
3. K5 manifest/config/state/compatibility 必须逐字段复用 K4 requirement、V2 characterization receipt、K1-C pinned surface 和 K4 state envelope；不得手写一套宽松兼容合同。
4. K4 conformance authority 只做一次窄幅泛化：抽取一个纯 evaluator，由原 K4 current-three writer/readback与 K5 exact full-five writer/readback共同调用。既有 V2 receipt/set schema/hash 不变，不创建 K5 平行 receipt。
5. K4 public writer 仍只接受 exact current-three；K5 public writer 只接受 exact current-three + Iceberg + Stop，拒绝 caller supplied subset、extra plugin、mode、disposition 或 arbitrary expected facts。
6. K5 catalog 是真实 K1 catalog/creation binding 的 code-owned shadow composition，但产品 root 不消费它；“registered”不等于产品激活。
7. K5 完整保留 Iceberg 一次 callback 可能产生多条 command 的 trace；现有 K2 V1 materializer 只允许 exact 单 command broker-neutral shadow。多 command 不丢弃、不复用第一条 receipt、不假成功，留给 K6 generic per-command authority。
8. K5 不新增数据库 schema、DDL/DML、配置开关、binding、RBAC、人工 acknowledge、审批或 enable gate。任何失败自动可见；catalog/authority build失败阻止整项K5 shadow candidate但不影响现有产品route，合法catalog上的route failure只隔离exact plugin/route。
9. 通过直接、negative、fresh-process、restart、DEV PostgreSQL shadow、coverage、changed-files routing 与 F2 验收后，K5 才可标记 `implemented_verified`；source merge、生产 gate、服务重启和 runtime activation继续分开报告。

## 1. Scope / 范围

### 1.1 In scope

- 两个 strict `ExecutionAlgoPluginManifestV2`、`PluginRegistrationDescriptorV2`、`PluginCreationBindingV1`、K1 compatibility receipt 和 K4 conformance receipt。
- 两个 explicit factory、config validator、state codec；factory 返回现有 `VnpyFacadeBackedPluginAdapterV1`。
- K5 exact full-five shadow catalog composition与 sealed full-five conformance authority。
- Iceberg `ALGO_START/TIMER/ORDER/TRADE/COMMAND_OUTCOME/RECONCILE/SESSION/EOD` 生命周期。
- Stop `ALGO_START/TICK/ORDER/TRADE/COMMAND_OUTCOME/RECONCILE/SESSION/EOD` 生命周期。
- K2 creation/delivery public seam 的真实 shadow integration、restart/readback 和 broker-neutral evidence。
- 对 K4 conformance builder 的最小公共 evaluator 抽取，使 V2 schema 可表达既有 pure current-three 与 K5 façade-backed 两种已批准 binding disposition。
- 直接测试、negative matrix、DEV PostgreSQL、coverage、classifier/F2、rollout/rollback和生产状态分离。

### 1.2 Non-goals

- 不修改 K2 repository schema、migration、scheduler、client、event ingress、timer owner、outbox dispatcher、OMS 或 Gateway；仅允许在既有通用 K2 invocation error renderer 中保留 K4 typed façade primary reason/context，不增加算法分支、route 或状态机。
- 不替换 current-three K3 factory/binding，不把 Sniper/BestLimit/TWAP 改为 façade-backed 产品实现。
- 不实现 K6 generic per-command product command-authority aggregate、writer/readback、projection/materializer、restart/reconcile。
- 不实现 dependent-BUY durable coordinator，不退役 legacy helper/product route，不触发真实 broker。
- 不修改策略信号、选股、策略包准入、资产校验、Target、side、quantity、board-lot、price 或 B0_QUOTE_V2 业务语义。
- 不合成 auction、last price、limit-up/down、L1 depth、generation、sequence 或普通 quote fallback。
- 不增加人工门禁、审批、RBAC、manual acknowledge、manual recovery、全局 stop gate 或“等待 K5 验证”产品开关。
- 不得以 mock-only、固定 PASSED、默认 config、previous/latest catalog、installed package 或 legacy route 代替真实权威。

## 2. Background, Current Facts, Ownership and Gap / 背景、当前事实、所有权与缺口

### 2.1 Existing authority reuse

| Fact | Unique owner | K5 rule |
| --- | --- | --- |
| manifest/config/event/capability/state schema | K1 `ExecutionAlgoPluginManifestV2` | 只创建两个 strict manifest，不扩展 top-level schema |
| descriptor/catalog/build failure | K1-B `PluginRegistrationDescriptorV2/PluginCatalogSnapshotV1` | exact full catalog、zero-partial publication |
| creation identity / process callable binding | K1-B `PluginCreationBindingV1` + `PluginProcessBindingsV2` | creation只闭合algo/plugin key；live callable独立process-local，fresh process重算 |
| pinned method/DTO/enum/source | K1-C V2 locked surface/compatibility receipt | repo-owned bytes，无网络/installed fallback |
| Iceberg/Stop exact source behavior | K4 source manifest、V2 characterization、algorithm binding | K5 逐 hash 复用，不重新解释 upstream |
| façade/DTO/effect/state envelope | K4 `VnpyFacadeBackedPluginAdapterV1` 与 V2 conformance | K5 不复制 adapter/runtime owner |
| event/delivery/transition/timer/repository | K2 durable kernel | K5 仅调用已有 public seam |
| product command authority/cutover | K6 | K5 不预占、不伪造 positive product receipt |

### 2.2 Signal/execution isolation

K5 输入只来自冻结的 execution plan、algo instance、manifest/catalog、K2 event/delivery/state/mapping和 native B0 market-data projection。K5 不读取 signal、ranking、model、strategy-package assets 或 selection artifact，也不做策略包二次完整性校验。缺少运行时必要 quote/contract/route事实时只产生 typed wait/failure；不得返回信号层补值、重新选股或改变 target。

### 2.3 Verified design gap and bounded correction

现有 K4 `build_vnpy_facade_conformance_set_v2()` 将 descriptor algo set 硬编码为 `BEST_LIMIT_MINIQMT/SNIPER_MINIQMT/TWAP_LITE_MINIQMT`，并固定输出 `PURE_PLUGIN_SHADOW_CONFORMANCE + NOT_APPLICABLE_PURE_PLUGIN`。因此父蓝图原“只新增 plugin files”不足以直接实施 K5。

K5 只允许以下修正：

- 从现有 builder 提取 `_evaluate_vnpy_facade_conformance_v2` 纯 evaluator；它不接受 caller supplied expected set/disposition，而从 strict descriptor provider、exact creation binding、algorithm binding与 K4 characterization authority推导。
- `AISTOCK_DERIVED` + existing current-three pure binding只允许 `PURE_PLUGIN_SHADOW_CONFORMANCE/NOT_APPLICABLE_PURE_PLUGIN`。
- `VNPY_COMPAT` + exact `VnpyFacadeBackedPluginAdapterV1` binding只允许 `FACADE_BACKED_ADAPTER/SHADOW_ONLY_K2_V1`。
- 其他 provider、binding type、plugin/algorithm mismatch、missing/extra/duplicate authority全部 typed fail；不发布 partial set。
- 原 `build_vnpy_facade_conformance_set_v2()` 与原 validator继续固定 exact current-three，对外结果和 hash 不变。
- 新 K5 writer/validator固定 exact full-five，并复用同一 evaluator、`VnpyFacadeConformanceReceiptV2`、`VnpyFacadeConformanceBuildItemV2`、`VnpyFacadeConformanceSetV2` 与 `VnpyFacadeConformanceAuthorityV2`。

该改动不触及 kernel durable schema/state machine、DB 或 route，不建立第三种 disposition，也不允许任意 plugin set；它是 K5 使用已批准 K4 adapter 的必要闭合。若既有通用 invocation wrapper 会把 K4 typed failure 降格成通用异常，允许在同一公共入口做一次 reason/context 保真修复，但不得按 Iceberg/Stop 分支。

## 3. Architecture and Planned Module Boundary / 架构与计划模块边界

K5 计划生产文件固定为：

- `backend/execution_algos/vnpy_compat/k5_plugin_manifests.py`：两个 code-owned manifest、strict config/state validator和 exact source attribution。
- `backend/execution_algos/vnpy_compat/k5_binding_authority.py`：Iceberg/Stop 两项 code-owned、recursive-immutable `VnpyFacadeAlgorithmBindingV2` canonical payload literal与唯一pure strict reader；不保存PASSED结论，不执行I/O或fresh-process构建。
- `backend/execution_algos/vnpy_compat/k5_plugin_factories.py`：两个 explicit factory和 process callable binding；只通过`k5_binding_authority.py`读取exact V2 binding并返回 K4 adapter。
- `backend/services/miniqmt_execution_runtime/k5_shadow_catalog.py`：exact full-five descriptor/compatibility/catalog/creation/conformance composition root；它只编排K1/K4 pure authorities，不拥有kernel/repository/Gateway，产品 runtime不 import。
- `backend/execution_algos/vnpy_compat/facade_characterization.py`：仅抽取§2.3 pure evaluator并增加 exact K5 public writer/readback；不改变K4 source vectors或current-three输出。
- `backend/execution_algos/vnpy_compat/facade_adapter.py`：仅补齐config-only factory到pinned source setting的显式representation bridge，以及无active child时从`NOT_APPLICABLE`推导既有`CLEAN`终态闭包；不改变Iceberg/Stop pinned比较符、方向、数量、quote或command语义。
- `backend/services/miniqmt_execution_runtime/kernel_delivery.py`：仅修复通用invocation error renderer对既有typed façade reason/context的保真；不得出现`ICEBERG`/`STOP`分支或新门禁。
- `backend/services/miniqmt_execution_runtime/plugin_registry.py`：把既有pure `ExecutionAlgoPluginV2` runtime-checkable Protocol从kernel implementation module提升到catalog/process-binding权威，使conformance与kernel共用同一SPI且不形成反向kernel import；方法集合与运行语义完全不变。

两个package `__init__.py`保持不变；K5通过上列exact module refs显式导入，防止package import扩大公开面或自动构建shadow catalog。

禁止修改：

- 除上一条列明的`kernel_delivery.py`通用error-renderer保真修复外，禁止修改`backend/services/miniqmt_execution_runtime/kernel*.py`、repository/migration、scheduler、client、Gateway与broker代码；
- `backend/execution_algos/vnpy_style/*` current-three production behavior；
- `backend/execution_algos/vnpy_compat/__init__.py`、`backend/services/miniqmt_execution_runtime/__init__.py` package import surface；
- selection、strategy package、Target、LocalSIM或产品 binding/config。

若实现必须修改上述禁止文件，K5 立即停止并先修订父蓝图/详细设计；不得用 algo-code branch 或临时 bypass 扩大 write scope。

## 4. Exact Plugin, Manifest and Source Contracts / 精确插件、Manifest 与源码契约

### 4.1 Fixed identities

| algo_code | plugin_id | version | provider | config schema version | state schema version | implementation_ref |
| --- | --- | --- | --- | --- | --- | --- |
| `ICEBERG` | `aistock.vnpy.iceberg` | `1.0.0` | `VNPY_COMPAT` | `iceberg_characterization_config_v1` | `iceberg_facade_state_v1` | `backend.execution_algos.vnpy_compat.k5_plugin_factories:create_iceberg_plugin_v1` |
| `STOP` | `aistock.vnpy.stop` | `1.0.0` | `VNPY_COMPAT` | `stop_characterization_config_v1` | `stop_facade_state_v1` | `backend.execution_algos.vnpy_compat.k5_plugin_factories:create_stop_plugin_v1` |

两个 manifest 都固定：

- `order_types={LIMIT}`；direction/offset/member映射复用K4 exact enum authority。
- `supported_sides={BUY,SELL}`、`supported_broker_backends={minqmt_sim}`、`restart_policy=DURABLE_RESTORE`。
- compatibility requirement/source lock/method/object/enum hash来自 K1-C generator；K5不能传入 caller hash。
- `behavior_characterization_sha256` 等于对应 K4 V2 PASSED characterization receipt hash。
- `source_attribution` 同时包含 exact upstream path/hash 与 K5 manifest/binding-authority/factory/state-codec canonical-LF path/hash；binding literal任意字节漂移都必须改变manifest/source identity并在full-five candidate发布前被fresh authority comparison拒绝。
- source/readback对CRLF/LF使用仓库统一 canonical-LF authority；upstream pinned bytes hash仍按 exact bytes authority，不混用。
- manifest build失败进入现有aggregate build failure；不发布旧/partial catalog。

Pinned source facts：

| algo | path | bytes SHA-256 |
| --- | --- | --- |
| Iceberg | `vnpy_algotrading/algos/iceberg_algo.py` | `9019cd20e4288b1642f7bc5f1508244eb9ccb419a2a888f69040fd9c5c6a2c21` |
| Stop | `vnpy_algotrading/algos/stop_algo.py` | `18a758b2d86b0704b00ce385f3517061e21dee57178c3abfd10271091e8db090` |

### 4.2 Iceberg config/event/capability contract

`config_schema`固定`additionalProperties=false`，只允许：

- `display_volume`：required、finite、non-bool、`>=0`，无 default/alias/coercion。为同时保持K1 canonical JSON禁止binary float与upstream允许fractional number的事实，durable carrier仅允许strict non-negative integer，或非整数值的canonical non-negative decimal string；整数不得以字符串重复表示，binary float不得进入durable authority。process-local source-setting bridge只在调用pinned class前把已验证carrier转换为float，不改变durable config identity。exact `round_to` 后为0时保持 pinned empty-send/diagnostic语义。
- `interval`：required，strict non-bool integer，`>=0`，无 default/alias/coercion；单位为 exchange-active TIMER occurrence。`0`与`1`都按每次 occurrence 检查，不擅自收紧。

required façade methods固定为`cancel_order,get_tick,put_algo_event,send_order,update_order,update_timer,update_trade,write_log`；subscriptions固定`ALGO_START,COMMAND_OUTCOME,EOD,ORDER,RECONCILE,SESSION,TIMER,TRADE`；market capabilities固定 native `L1_ASK/L1_BID`和K4 contract projection所需字段。Iceberg不订阅业务TICK callback；TIMER通过K2 read seam读取同session且`tick.sequence < timer.delivery.sequence`的最后一项 eligible native B0 TICK。

### 4.3 Stop config/event/capability contract

`config_schema`固定`additionalProperties=false`，只允许：

- `price_add`：required，finite signed、non-bool canonical decimal string，无 default/alias/coercion；不得把float/int静默转成字符串，不因方向偏好禁止负值。

required façade methods固定为`put_algo_event,send_order,update_order,update_tick,update_trade,write_log`；subscriptions固定`ALGO_START,COMMAND_OUTCOME,EOD,ORDER,RECONCILE,SESSION,TICK,TRADE`；market capabilities固定 native `LAST_PRICE/LIMIT_UP_DOWN`。Stop只消费continuous AM/PM B0 TICK，不从L1、分钟线、pre_close或普通quote合成last/limit字段。

### 4.4 Config writer/readback parity

config validator先strict model/schema readback，再执行上述跨字段/数值合同；factory只接收validator返回的recursive immutable canonical payload。写入manifest的schema hash、process validator ref/signature/body hash和运行时validator必须由同一builder重建。非法类型、额外字段、missing、NaN/Infinity、bool-as-number、别名、默认值或hash-correct drift均在factory构造前typed fail。

### 4.5 Exact market-data requirements

以下四项必须使用existing `MarketDataRequirementV1.create()`生成并按`requirement_sha256`排序；全部固定`session_phases={CONTINUOUS_AM,CONTINUOUS_PM}`、`absence_disposition=WAIT_FOR_NEXT_VALID_EVENT`：

| plugin | capability | required_fields | applicable_sides | event_types |
| --- | --- | --- | --- | --- |
| Iceberg | `L1_ASK` | `price` | `BUY` | `TIMER` |
| Iceberg | `L1_BID` | `price` | `SELL` | `TIMER` |
| Stop | `LAST_PRICE` | `last_price` | `BUY,SELL` | `TICK` |
| Stop | `LIMIT_UP_DOWN` | `limit_down,limit_up` | `BUY,SELL` | `TICK` |

`event_types`表示callback在哪类event上需要该capability，因此必须是manifest subscription子集；它不表示quote lineage的原始event type。Iceberg的requirement在`TIMER`生效，TIMER read seam仍从K2 runtime的durable native B0 event authority读取prior TICK；不得为满足schema给Iceberg增加TICK业务callback。Stop requirement在`TICK`生效并直接消费同一projection。EOD未满足的数据需求由既有EOD terminal语义闭合，不修改absence disposition或增加超时门禁。

### 4.6 K4 source disposition 与 K5 shadow registration 的精确关系

K4 source manifest中Iceberg/Stop的`VnpyFacadeRegistrationDispositionV1.CHARACTERIZATION_ONLY_K5`必须原样保留，K5不得修改K4 source manifest、source identity、characterization vector或hash。该值是K4 pinned-source的来源与阶段归属元数据，不是当前K1 catalog registration状态，也不与K5 shadow registration矛盾。

K5 registration只由K1 `PluginRegistrationDescriptorV2`、`PluginCreationBindingV1`和exact process binding证明。full-five evaluator只接受以下唯一桥接：

```text
K4 source disposition = CHARACTERIZATION_ONLY_K5
+ descriptor provider = VNPY_COMPAT
+ exact K1 registration/creation binding
+ exact code-owned V2 binding literal == fresh K4 V2 binding
=> FACADE_BACKED_ADAPTER / SHADOW_ONLY_K2_V1
```

任一项缺失、重复、provider不符、source disposition被改写或binding不相等都进入ordered typed failure并使full-five candidate zero publication。不得新增disposition enum、把`CHARACTERIZATION_ONLY_K5`重写为“registered”，也不得据此激活产品route。

## 5. Catalog, Creation Binding and Conformance / Catalog、创建绑定与一致性

### 5.1 Exact full-five composition

`build_k5_shadow_catalog_runtime_v1()`是唯一composition root，不接受plugin list或mode参数。它按现有current-three code-owned builder读取三项descriptor/binding/compatibility receipt，再增加两项K5事实，最终强制：

```text
algo_codes = BEST_LIMIT_MINIQMT, ICEBERG, SNIPER_MINIQMT, STOP, TWAP_LITE_MINIQMT
plugin_ids = aistock.vnpy.best_limit, aistock.vnpy.iceberg,
             aistock.vnpy.sniper, aistock.vnpy.stop, aistock.vnpy.twap_lite
```

所有集合按K1 canonical sort；algo/plugin/implementation/binding/source identity必须唯一。missing、extra、duplicate、wrong provider、stale K1 receipt、creation callable drift或catalog hash drift均 aggregate fail，zero publication；不得退回current-three catalog并报告K5成功。

### 5.2 Creation identity and process binding separation

durable descriptor只保存refs/signatures/source hashes；`PluginCreationBindingV1`只保存exact `algo_code + plugin_key`，不得携带callable。factory/config validator/state codec只存在于process-local `PluginProcessBindingsV2`。factory现有SPI只有canonical config参数，因此不得依赖调用方注入、可变global registry、前次build对象或每次调用时隐式启动fresh process。Iceberg/Stop exact `VnpyFacadeAlgorithmBindingV2`由`k5_binding_authority.py`保存为code-owned immutable canonical payload literal，并由下列pure reader唯一提供：

```text
k5_facade_algorithm_bindings_v2() -> tuple[VnpyFacadeAlgorithmBindingV2, ...]
k5_binding_for_algo_v2(algo_code: str) -> VnpyFacadeAlgorithmBindingV2
```

reader必须strict构造既有`VnpyFacadeAlgorithmBindingV2`、强制exact `{ICEBERG,STOP}`集合、唯一algo identity、canonical order和递归不可变；missing/extra/duplicate/hash-correct field drift均fail loud。它不读取文件、网络、环境变量、subprocess、installed package或previous/latest authority，不创建平行DTO，也不携带PASSED布尔值。manifest的`behavior_characterization_sha256`取exact literal binding内的`characterization_receipt_sha256`；§5.4要求它在candidate发布前与fresh sealed authority逐字段相等，而不是把literal当静态成功证据。

fresh process必须从 repo-owned refs加载并重算：

| plugin | binding_id | callable_ref |
| --- | --- | --- |
| Iceberg | `aistock.vnpy.iceberg.factory` | `backend.execution_algos.vnpy_compat.k5_plugin_factories:create_iceberg_plugin_v1` |
| Iceberg | `aistock.vnpy.iceberg.config_validator` | `backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_iceberg_config_v1` |
| Iceberg | `aistock.vnpy.iceberg.state_codec` | `backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_iceberg_state_v1` |
| Stop | `aistock.vnpy.stop.factory` | `backend.execution_algos.vnpy_compat.k5_plugin_factories:create_stop_plugin_v1` |
| Stop | `aistock.vnpy.stop.config_validator` | `backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_stop_config_v1` |
| Stop | `aistock.vnpy.stop.state_codec` | `backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_stop_state_v1` |

factory exact signature为`(canonical_plugin_config: Mapping[str, Any]) -> VnpyFacadeBackedPluginAdapterV1`；config/state validator exact signature与K1现有validator SPI一致：`(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any`，返回值必须是`freeze_json_v1`后的recursive immutable canonical payload。descriptor中的signature hash是实现稳定后由code-owned canonical signature重算并固化的exact值，writer/readback与fresh process必须一致；不得填占位hash或运行时接受任意签名。

- callable module/qualname/signature/canonical-LF source hash；
- exact manifest/plugin/algo/version；
- code-owned K5 binding literal及其K4 algorithm binding/source execution/characterization hash；
- factory probe config和返回adapter exact type。

mutation、monkeypatch、wrong adapter、wrong algorithm class或stale source均产生现有binding failure，且不发布partial catalog。

### 5.3 Full-five conformance writer/readback

K5 conformance writer要求：

- exact full-five catalog与exact K4 façade/source/characterization authority；
- 对每项descriptor从其exact K4 characterization requirement/artifact取得首个canonical vector config作为唯一factory probe config，先走catalog-bound config validator，再调用catalog-bound factory；不得使用调用方config或默认setting；
- current-three factory probe必须返回exact `ExecutionAlgoPluginV2` pure implementation并保持pure/not-applicable；Iceberg/Stop probe必须返回exact `VnpyFacadeBackedPluginAdapterV1`且其manifest/class/V2 algorithm binding闭合，才可标记façade-backed/shadow-only；
- 每项route receipt从同一 strict gateway catalog现场重建；B0 source与conditional exact-cancel语义不变；
- 每项K1 compatibility、K4 algorithm binding、source execution set、characterization receipt、state/terminal mapping逐字段闭合；
- 五项均PASSED才发布一个V2 set和sealed authority；任何失败保留完整ordered failures并zero publication。

readback先strict schema/hash，再用当前repo bytes、catalog、gateway和characterization重新执行同一pure evaluator，逐字段比较set/build item/receipt/disposition/hash。hash-correct伪造、missing/extra/duplicate receipt或caller supplied PASSED均拒绝。

### 5.4 Exact full-five binding/composition build sequence

唯一合法构建顺序固定如下，任何步骤失败均保留ordered typed evidence并且zero publication：

1. 使用既有K1/K3 builder重建current-three descriptor、requirement、creation与process binding，不修改其bytes/hash。
2. 调用既有`build_vnpy_facade_characterization_authority_fresh_process_v2`，从repo-owned pinned bytes生成sealed五算法K4 characterization authority；不得以in-process缓存或literal替代。
3. 调用既有`build_vnpy_facade_algorithm_bindings_v2`，只从sealed authority选出exact Iceberg/Stop V2 bindings。
4. strict读取`k5_binding_authority.py`两项code-owned literals，逐模型/逐字段要求与步骤3的fresh bindings byte-identical；同时闭合class/source execution/state/terminal mapping/characterization receipt hash。missing、extra、duplicate或任何drift均失败。
5. K5 manifest使用已通过步骤4 equality的literal binding `characterization_receipt_sha256`，再构建exact full-five K1 catalog、creation bindings与process bindings；不得先发布catalog后补验binding。
6. factory保持现有config-only SPI，仅strict读取code-owned literal、pinned class和state/terminal mappings构造adapter；不读取composition-local对象或隐藏mutable state。
7. full-five conformance factory probe对返回adapter再次比较manifest、algorithm class、literal binding与步骤2/3 fresh authority；随后执行writer/readback。五项全部闭合后才发布sealed full-five candidate。

步骤2/3是外部源码的fresh事实，步骤4/6提供config-only factory可获得的稳定code-owned binding；两者缺一不可。该链没有固定PASSED、默认/previous/latest fallback，也不改变K1 factory SPI。

## 6. Implementation Plan: Factory, Adapter and State Codec / 实施方案：Factory、Adapter 与状态编码

### 6.1 Factory construction

两个factory执行顺序固定：

1. strict读取exact manifest与canonical config；
2. 通过`k5_binding_for_algo_v2` strict读取code-owned exact `VnpyFacadeAlgorithmBindingV2`；其与fresh K4 authority的逐字段相等已经由§5.4 candidate构建闭合，factory仍独立校验binding/class/manifest identity；
3. 从K4 repo-owned pinned loader取得exact `IcebergAlgo`或`StopAlgo` class；
4. 通过algo-specific pure source-setting builder把已验证canonical config映射为pinned constructor setting：Stop canonical decimal string与Iceberg fractional decimal string只在此process-local边界转换为float，integer与interval保持原值；不得default、clip、round或改变durable config；
5. 构造既有`VnpyFacadeBackedPluginAdapterV1`，传入exact state/terminal mappings与上述source-setting builder；
6. 对返回对象的manifest、class、binding、codec refs做独立readback后返回。

factory不得缓存前一次成功对象、吞异常、返回pure plugin替代、加载installed package或直接构造broker/Gateway/repository。

### 6.2 State envelope authority

两算法复用`VnpyFacadeStateEnvelopeV1`，不创建K5 durable state DTO。manifest `state_schema`必须结构性覆盖该envelope，并锁定algorithm-specific parameter/variable key set；state codec执行strict model readback后验证runtime/algo/plugin/manifest/binding/symbol/side/quantity、active-order/mapping、market lineage和state mapping hash。

Iceberg exact state：

- parameters恰好`display_volume,interval`；variables恰好`timer_count,vt_orderid`。
- `interval in {0,1}`时callback后`timer_count=0`；`interval>1`时`0<=timer_count<interval`。
- `vt_orderid`是pinned Iceberg当前“可见委托指针”，不等价于envelope内全部`ordered_active_orders`。非空时必须唯一命中当前同owner、同parent、nonterminal mapping；但更早的cancel-pending mapping可以同时存在。
- pinned `on_timer`在发出cancel后立即把`vt_orderid`清空，因此空指针可与零个或多个仍未收到terminal ORDER callback的cancel-pending/nonterminal mappings并存；state codec不得把这种合法after-state误判为corruption。
- cancel ACK前的后续eligible TIMER可按pinned source再次submit，新`vt_orderid`可与旧cancel-pending mappings并存。codec必须按owner/identity排序并完整保留所有active mappings，不能改为“只要任一active mapping存在就禁止submit”。
- 旧order的迟到terminal ORDER callback会经pinned `on_order`无条件清空当前`vt_orderid`；即使此时另一个更新的mapping仍nonterminal，该after-state也必须原样可持久化、恢复并由后续TIMER按pinned逻辑推进，不得擅自把指针恢复到最新mapping或吞掉callback。

Stop exact state：

- parameters恰好`price_add`；variables恰好`vt_orderid,order_status`。
- `vt_orderid`一旦非空，后续TICK不得重新触发或替换该id；restart后同样成立。
- `order_status`必须是K4 exact upstream/member mapping；不得以任意字符串或默认状态代替。

所有state extract/restore都重算envelope hash；malformed、unknown key、missing key、wrong type、wrong owner、active mapping drift或hash drift typed fail。last-good durable state继续由K2事务拥有，codec失败不能覆盖。

## 7. Iceberg Exact Lifecycle / Iceberg 精确生命周期

### 7.1 Initialize and TIMER

- `ALGO_START`构造一次upstream instance并冻结initial envelope；retry/restart从durable state恢复，不重复initialize side effect。
- 每个eligible TIMER先由K2 exchange clock拥有的durable event/delivery排序；午休、auction、closed阶段不生成可计数occurrence，K5不读取wall clock。
- TIMER transition读取同runtime/session/symbol、严格早于delivery sequence的latest native B0 TICK；later tick不能改变retry输入。
- 无eligible tick时`get_tick()->None`并保留exact zero-command/diagnostic trace；不使用stale quote、previous session、分钟线或默认price。
- `timer_count`按pinned source先加1；未到interval则只更新state；到达时重置0并只检查upstream当前`vt_orderid`指针，不把`ordered_active_orders`中其它cancel-pending mapping升级为额外submit门禁。

### 7.2 Submit, cancel and cumulative callbacks

- `vt_orderid`为空时，visible slice=`min(parent residual, display_volume)`，再经K4 exact round/contract authority；rounded zero保持empty-return而非假提交。旧cancel-pending mapping存在本身不得阻止该pinned submit分支。
- `vt_orderid`非空时，BUY仅在native ask满足pinned条件、SELL仅在native bid满足pinned条件时，对该current owned `local_vt_orderid`生成cancel并在同一after-state立即清空指针；不得调用broker cancel-all，也不得等待cancel ACK后才清空。
- cancel ACK前下一eligible TIMER允许生成新的current order；old cancel-pending与new current mapping都必须保留。旧order迟到terminal callback按pinned `on_order`清空当时的current pointer，即使它指向new order；不得用mapping identity guard“修正”该上游行为。
- Iceberg `on_timer`最多只撤销其唯一`vt_orderid`；若terminal/EOD host lifecycle调用inherited `cancel_all()`并产生多条cancel，则按frozen local id排序并完整保留ordinal/effect/trace。K2 V1不能表达时明确拒绝materialization，但不丢trace、不报告callback失败为成功。
- ORDER更新active/terminal与cumulative traded事实；TRADE在pinned exact条件`traded >= target_volume`时finish。partial `<`不终结，equal与over均命中source条件；若K2现有cumulative/overfill invariant把overfill判为非法，则必须在进入plugin transition前typed拒绝，不能把Iceberg比较符静默改为`==`。ORDER/TRADE与mapping/child/OMS identity必须闭合，迟到/重复callback按K2去重和sequence合同处理。
- EOD按现有K2 residual/terminal规则处理，不制造补单；SESSION午休/PM只改变clock eligibility，不重置state或产生catch-up burst。

## 8. Stop Exact Lifecycle / Stop 精确生命周期

### 8.1 Native trigger

- `ALGO_START`构造一次instance并冻结initial state；restart严格恢复`vt_orderid/order_status`。
- 只有native B0 continuous `TICK`触发。BUY条件=`last_price>=stop limit_price`；SELL条件=`last_price<=stop limit_price`。
- 未触发时只写exact state/diagnostic trace；缺失、非法或非native last/limit事实进入现有wait/failure disposition，不合成。
- BUY order price=`min(limit_price + price_add, limit_up)`（limit_up存在时）；SELL=`max(limit_price - price_add, limit_down)`（limit_down存在时），保持pinned source与existing positive-price/OMS preflight。

### 8.2 Exactly-once and terminal

- trigger成功生成command后，deterministic local id立即进入after-state；同一或后续TICK看到非空`vt_orderid`不得再触发。
- crash发生在state/effect transaction前时整笔回滚并由同event deterministic replay；commit后重放由K2 event/delivery/state identity去重，不重复command。
- ORDER只更新exact `order_status`；TRADE只在pinned exact条件`traded == target_volume`时finish，不能改为`>=`或因partial fill提前终结。equal命中；over不命中source终止分支，若K2现有cumulative/overfill invariant判定该输入非法，必须在进入plugin transition前typed拒绝并保留source characterization中的`==`，不得静默归一化为target。
- rejected/unknown/callback-before-ACK等outcome保持K2 authority；K5不自行re-arm、fallback到新order或把broker unknown改成reject/success。
- EOD与terminal状态由现有K2处理；没有人工恢复/acknowledge。

## 9. Shadow Invocation, Multi-command and Product Boundary / Shadow 调用与产品边界

K5 integration必须走真实K2 creation/delivery public seam，使用sealed full-five catalog与conformance authority：

- `ALGO_START`、TIMER/TICK、ORDER/TRADE输入均是strict durable carrier；
- single command可进入existing K2 V1 broker-neutral shadow materializer，必须`dispatch_attempt=0`、`broker_called=false`；
- zero command保留exact transition/state/diagnostic；
- multi-command保留完整K4 collector trace并产生typed `product materialization unavailable` evidence，不截断或复用第一条receipt；
- 不进入dispatcher、Gateway、broker，不创建产品binding，不修改existing product composition root。

K5 shadow composition只供测试、离线诊断和后续K6输入；product runtime import graph必须证明未引用`backend/services/miniqmt_execution_runtime/k5_shadow_catalog.py`。K6必须先实现新的versioned per-command authority并拒绝K4/K5 V1 product receipt，才可设计产品激活。

## 10. Transactions, Retry, Restart and Concurrency / 事务、重试、恢复与并发

- K2继续是single writer；K5没有repository writer、线程、task、process或锁owner。
- event→delivery→before state→transition→effect/command→after state沿用K2同事务；factory/conformance构建不进入该事务。
- adapter/codec/conformance都是pure or process-local；同输入在retry/restart/fresh process下hash、state、trace、command identity一致。
- per-algo predecessor、lease/fence/CAS、stale worker、claim/reclaim、callback dedupe均沿用K2。K5不得捕获后改写为成功。
- same-symbol/multi-slot按runtime/algo identity隔离；Stop exactly-once与Iceberg timer_count不能共享全局状态。
- full-five catalog build并发只允许code-owned immutable结果；一个plugin binding失败使本次candidate zero publication，但不破坏已经运行的产品current-three route。
- active failure在下一次同authority完整成功build后自动清除；last failure保留。没有人工ack或无限重试。

## 11. Risks, Failure Modes, Diagnostics, Metrics and Retention / 风险、失败模式、诊断、指标与保留

K5不计划新增异常基类或reason code；下表固定复用现有typed authority。若实施发现下表无法精确表达新的失败语义，必须先修订本文和父蓝图，不得临时使用通用`ValueError`、日志后继续或新增未设计的fallback。context必须JSON-safe、repo-relative、bounded并包含可获得的plugin/algo/runtime/event/delivery/state/catalog/conformance identity。

| failure stage | exact existing reason authority |
| --- | --- |
| manifest/config/state schema | `MINIQMT_PLUGIN_MANIFEST_SCHEMA_INVALID`、`MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID`、`MINIQMT_PLUGIN_STATE_SCHEMA_INVALID` |
| registration/catalog/process binding | `MINIQMT_PLUGIN_REGISTRATION_CONFLICT`、`MINIQMT_PLUGIN_MANIFEST_HASH_CONFLICT`、`MINIQMT_PLUGIN_BINDING_INVALID` |
| pinned compatibility/source | existing `MINIQMT_VNPY_COMPAT_*` reason family |
| façade binding/state/market | `MINIQMT_VNPY_FACADE_BINDING_INVALID`、`MINIQMT_VNPY_FACADE_STATE_MAPPING_INVALID`、`MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID`、`MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE` |
| conformance receipt/authority | `MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID`、`MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID`、`MINIQMT_VNPY_FACADE_CONFORMANCE_DRIFT` |
| route capability | `MINIQMT_PLUGIN_CAPABILITY_UNSUPPORTED`、`MINIQMT_PLUGIN_ROUTE_COMPATIBILITY_RECEIPT_INVALID`、`MINIQMT_GATEWAY_CAPABILITY_CATALOG_INVALID` |
| K2 V1 multi-command boundary | `MINIQMT_VNPY_FACADE_MULTI_COMMAND_PRODUCT_AUTHORITY_UNAVAILABLE` |

必须显式区分：

- manifest/config/state/creation binding invalid；
- pinned source/characterization/algorithm binding invalid；
- catalog aggregate build failure与per-route compatibility failure；
- market-data temporarily unavailable、unsupported capability与invalid observation；
- adapter transition failure、multi-command V1 materialization unsupported、K2 persistence/readback failure。

Metrics只使用既有低基数标签：`algo_code,event_type,outcome,reason_family`；禁止plugin instance、symbol、order、runtime、event id作为label。durable evidence和diagnostics retention沿用K2/K4，不新增高基数时序表或告警后端。alerts自动出现/清除，无RBAC、审批或acknowledge。

## 12. Verification Plan / 验证计划

### 12.1 Direct production-seam tests

计划测试文件：

- `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_postgres.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_scope_boundaries.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`

核心正向矩阵：

- exact two manifests/config/state/source attribution和fresh-process creation binding；code-owned Iceberg/Stop V2 binding literal与fresh sealed K4 binding逐字段相等、fresh-process deterministic且factory无隐藏mutable authority；
- exact full-five catalog与conformance writer/readback；K4 current-three set byte-identical不变；`CHARACTERIZATION_ONLY_K5 + VNPY_COMPAT + exact K1 registration + exact V2 binding`桥接唯一且不改K4 source manifest；
- Iceberg interval 0/1/>1、rounded-zero、missing/ready tick、submit/cancel、cancel-pending期间resubmit、old/new mapping并存、旧terminal callback清空新pointer、ORDER/TRADE `< / == / >`、午休/PM、restart、多command trace；
- Stop BUY/SELL trigger/not-trigger、signed price_add、limit bounds、exactly-once、ORDER/TRADE `< / == / >`、restart；
- K2 real creation/delivery/transition shadow，single-command materialization broker-neutral，zero/multi-command semantics；
- same-symbol multi-slot、retry、stale worker、duplicate/late callback和fresh-process deterministic readback。

### 12.2 Negative and corruption matrix

必须直接覆盖：

- missing/extra/duplicate plugin/descriptor/binding/receipt、wrong provider、algo/plugin/source mismatch；
- illegal config type/default/alias/NaN/Infinity/bool、malformed state key/type/hash/owner；
- K4 current-three builder收到full-five、K5 builder收到partial/extra set；
- code-owned V2 binding literal missing/extra/duplicate、field/hash/class/source/characterization drift、与fresh binding不相等、factory读取环境/global/cache或隐式fresh-process；
- K4 source disposition被改写、K1 registration缺失或`CHARACTERIZATION_ONLY_K5`被错误当作产品激活；
- caller forged disposition/PASSED、hash-correct receipt/catalog/state drift；
- installed/latest/previous catalog fallback尝试；
- Stop repeated trigger/re-arm/将`==`改为`>=`、Iceberg wrong TIMER cutoff/auction/lunch count/stale quote/把空pointer等同无active mapping/用任一active mapping阻止pinned resubmit；
- multi-command truncation/first-receipt reuse/fake materialization；
- product root import K5 catalog、kernel/client/scheduler/repository出现algo-code branch；
- exception renderer二次异常、unbounded/mutable/absolute-path evidence。

### 12.3 DEV PostgreSQL and no-broker proof

DEV测试只使用现有DEV配置和disposable schema，不执行新DDL：

- 通过真实K2 repository public seam提交Iceberg/Stop ALGO_START与后续event；
- 独立连接readback event/delivery/state/effect/mapping/outbox identity；
- restart重建同一state/command trace；
- 断言`dispatch_attempt=0/broker_called=false`且Gateway/broker seam调用次数为0；
- transaction failure/commit/readback failure保持显式，不返回假成功。

### 12.4 Coverage and changed-files route

- K5新增/实质修改生产模块line coverage `>=80%`、branch coverage `>=70%`；不得用skip/xfail排除新合同。
- 严格按changed files → `file_ownership.yaml` → `module_registry.yaml` → `test_plans.yaml`选择测试。
- 预期primary plan为`miniqmt_execution_runtime_l2`；只有实际ownership/shared-contract映射要求时才运行Paper或其他计划。
- 运行`python -m nox -s l0`、`python -m nox -s validation_module_registry_l0`、Ruff check/format、py_compile、`git diff --check`。
- 运行本文、父蓝图、统一蓝图三个F2 validator；每个design item必须与矩阵一一对应且`warnings=0`。

## 13. Rollout, Rollback, Production Gates and State Separation / 交付、回滚、生产门禁与状态分离

### 13.1 Source rollout

推荐一个feature PR交付exact manifests/factories/catalog/conformance refactor/state codecs/tests及三份设计状态更新。只有直接/DEV/L2/coverage/F2/CI和DESIGN-COMPLIANCE-001闭合后可标记K5=`implemented_verified`。合入后单独同步source state并安全aftercare；不因merge自动激活产品。

### 13.2 Rollback

在K6尚未消费K5时，source rollback删除K5 manifests/factories/shadow composition/tests并恢复K4 conformance helper内部组织；K1-K4 durable事实、current-three product route和DB不变。若K6已依赖，必须先回滚K6 consumer再回滚K5。无DDL rollback、订单补偿、配置切换或服务控制。

### 13.3 Independent state report

每次交付分别报告：

- `source_merge`
- `close_sync=not_applicable_feature`
- `root_sync`
- `cleanup`
- `production_ddl_gate`
- `production_dml_gate`
- backend/frontend dependency gates
- config/binding/broker gates
- `service_restart`
- `runtime_activation`

本地implementation已完成定向/DEV/coverage/模块计划验证；PR、required CI与source merge尚未发生。全部生产与运行门禁仍为`noop`。

## 14. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-091` | K5 current facts、K1-K4复用、K5/K6边界、信号/执行隔离和禁止kernel算法分支/状态机/product-route改动完整 |
| `F-092` | Iceberg/Stop exact plugin identity、manifest、config、source attribution、event/capability合同及K4 `CHARACTERIZATION_ONLY_K5`到K1 shadow registration桥接可直接实施 |
| `F-093` | K4 conformance hardcode缺口以前后一致的单一pure evaluator闭合，source disposition保持不变，无平行schema/receipt/任意plugin-set authority |
| `F-094` | code-owned exact V2 binding literal、fresh sealed K4 binding equality与exact full-five descriptor/catalog/compatibility/creation/conformance composition、zero-partial publication和fresh-process readback完整 |
| `F-095` | config-only factory的唯一binding authority、constructor、strict config/state codec、restore/active-order/lineage closure完整，Iceberg pointer与cancel-pending mappings不混同 |
| `F-096` | Iceberg exchange-active TIMER、sequence-cutoff native B0 quote、cancel-pending resubmit/late callback、exact `traded >= target`、restart/multi-command语义精确 |
| `F-097` | Stop native TICK、signed price_add、limit bound、exactly-once trigger、exact `traded == target`、ORDER/TRADE/restart语义精确 |
| `F-098` | K2 shadow invocation、transaction/retry/concurrency/failure/diagnostics/metrics/retention完整且无人工门禁 |
| `F-099` | direct/negative/DEV PostgreSQL/fresh-process/coverage/changed-files routing/F2验收可执行 |
| `F-100` | source rollout/rollback、K6 prerequisite与source/production/runtime状态分离完整 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-091` | §0–§3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_scope_boundaries.py`、`test_plugin_import_boundaries.py::test_real_k1_a_and_k1_c_modules_pass_ast_and_isolated_import`通过 | implemented_verified_local | none |
| `F-092` | §4 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py` final=`4 passed` | implemented_verified_local | none |
| `F-093` | §2.3、§4.6、§5.3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py` full-five writer/readback/factory probe matrix通过 | implemented_verified_local | none |
| `F-094` | §5 | `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_catalog.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`通过 | implemented_verified_local | none |
| `F-095` | §5.2、§5.4、§6 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`=`4 passed`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-096` | §7 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py` final=`15 passed`，Iceberg完整向量闭合 | implemented_verified_local | none |
| `F-097` | §8 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py` final=`15 passed`，Stop完整向量闭合 | implemented_verified_local | none |
| `F-098` | §9–§11 | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_postgres.py -q`=`1 passed`；public K2 transition在lifecycle direct覆盖 | implemented_verified_local | none |
| `F-099` | §12 | coverage aggregate=`38 passed` + manifest focused=`4 passed`；`python -m nox -s miniqmt_execution_runtime_l2`=`1127 passed,31 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；四模块line/branch均达标 | implemented_verified_local | none |
| `F-100` | §13 | artifact: `docs/architecture/miniqmt_execution_kernel_k5_iceberg_stop_plugins_f2_detailed_design_20260731.md`、父蓝图、统一蓝图；PR #2978 已开放，required CI/merge/aftercare 与生产/runtime状态继续分离 | implemented_verified_local | none |

## 16. Formal Design Review and DESIGN-COMPLIANCE-001 / 正式设计审核

### 16.1 Review findings closed in this revision

1. **K4 builder并非真正支持K5**：已在§2.3限定抽取单一pure evaluator；原K4 writer保持exact current-three，K5 writer固定exact full-five，禁止平行receipt和任意caller mode。
2. **“registered”可能被误读为产品激活**：已在§5和§9明确registered只发生于code-owned K1 shadow catalog/creation binding，产品composition root不引用。
3. **state schema可能退化为宽松dict**：已在§6要求复用strict `VnpyFacadeStateEnvelopeV1`并锁定两算法parameter/variable和active mapping closure。
4. **Iceberg TIMER可能读取later/stale/auction quote**：已固定same session、strict sequence cutoff、continuous exchange-active occurrence与无fallback。
5. **Stop可能在reject/restart后被擅自re-arm**：已固定non-empty deterministic `vt_orderid`后的no-retrigger和K2 outcome authority。
6. **multi-command可能被V1静默压缩**：已固定保留完整trace、typed拒绝V1 materialization，产品aggregate只归K6。
7. **catalog失败可能回退current-three并假报K5成功**：已固定full-five zero-partial publication和no previous catalog fallback。
8. **范围可能扩入kernel/DDL/产品门禁**：已在§1.2、§3、§13设为明确non-goal和no-diff验收，不新增审批或运行gate。
9. **shadow catalog放入algorithm package或package export会扩大import surface**：已把composition root固定在service层exact module，两个package `__init__.py`保持no-diff且产品root不得引用。
10. **Iceberg `vt_orderid`被误当作全部active-order集合**：已按pinned cancel后立即清空与template active-order延迟移除语义，允许空pointer与cancel-pending mappings并存、cancel ACK前resubmit、old/new mapping并存及旧terminal callback清空新pointer；禁止用额外active-order门禁“修正”上游。
11. **config-only factory没有可获得的V2 binding authority**：已新增code-owned immutable `k5_binding_authority.py`与§5.4 fresh equality闭包；factory只读literal，full-five publication必须先以fresh sealed K4 authority逐字段验证，消除隐藏global、per-call subprocess和静态PASSED。
12. **Iceberg/Stop terminal比较符可能被“达到”模糊化**：已分别固定Iceberg `traded >= target_volume`与Stop `traded == target_volume`，并要求`< / == / >`直接向量和K2 overfill typed rejection边界，禁止静默归一化或互换运算符。
13. **K4 `CHARACTERIZATION_ONLY_K5`与K5 registration关系不明**：已固定其为不可改写的source provenance/lifecycle metadata，并以exact K1 descriptor/creation + VNPY_COMPAT + code-owned/fresh V2 binding建立唯一shadow bridge，不新增enum或产品激活语义。
14. **Iceberg `display_volume`的上游number语义与K1禁用binary float冲突**：已在§4.2固定唯一durable carrier为strict integer或非整数canonical decimal string；factory只在process-local pinned-source边界转换为float，拒绝binary float与整数字符串双重identity。
15. **conformance只检查callable identity而未执行factory**：已在§5.3固定writer/readback都以canonical vector config调用catalog-bound validator与factory两次，验证无共享实例、pure/current-three与façade/K5 exact返回类型及binding闭包；失败不得发布PASSED set。
16. **K5公开binding/factory/config/state错误可能泄漏裸`ValueError/RuntimeError`或触发二次异常**：公开K5 authority现统一使用`MiniQMTPluginContractError`或既有`VnpyFacadeContractError`，context必须JSON-safe并保留primary reason；kernel invocation只做通用typed reason/context保真。
17. **ACTIVE状态在最后一个child终结后无法进入terminal**：K4 adapter原把输入`NOT_APPLICABLE`原样用于terminal mapping，而K2只有在after-state无active child时才可形成`CLEAN`。现固定仅在after-state确实无active child时推导既有`CLEAN`；`CANCEL_PENDING/OUTCOME_UNKNOWN`不变，避免假终结或永久ACTIVE。
18. **详细设计原绝对禁止任何kernel文件修改，与通用error renderer保真修复冲突**：已在§1.2/§3把唯一例外精确限定为`kernel_delivery.py`现有公共invocation错误保真；scope test继续扫描全部`kernel*.py/client.py`，禁止Iceberg/Stop算法分支、route或门禁。
19. **conformance probe曾直接读取adapter私有字段**：adapter现提供只读`conformance_runtime_binding_readback_v1()`，只返回immutable binding carrier与class ref；probe不再依赖私有attribute或composition-local cache，wrong object在readback前先typed分类。
20. **pure factory probe为检查`ExecutionAlgoPluginV2`反向import `kernel_delivery`，违反K1 import-boundary**：未放宽denylist；将原Protocol原样迁至既有`plugin_registry.py` process-binding authority，kernel保持re-export兼容，conformance与kernel共用同一runtime-checkable SPI，direct import-boundary与registry矩阵证明无第二Protocol或产品依赖。
21. **新增模块总coverage达标但branch coverage不足**：未使用pragma/skip/降低阈值；删除JSON Schema之后重复且不可达的config数值检查，保留schema唯一authority，并补public literal-drift、hash-correct readback drift、wrong-manifest与non-object negative paths。最终四个K5新模块line/branch均达到`>=80/>=70`。

### 16.2 Mandatory review result

| DESIGN-COMPLIANCE-001 item | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC/placeholder/mock-only completion | pass | §4–§12已由真实manifest/catalog、code-owned/fresh binding、config-only factory、adapter/K2 shadow与DEV PostgreSQL证据闭合；状态为本地verified、PR/CI pending，不冒充已合入 |
| no silent error/fake success/fallback | pass | zero-partial catalog、typed failure、strict readback、multi-command显式拒绝、no installed/latest/previous/default fallback |
| no business semantic drift | pass | §6–§8逐项固定Iceberg cancel-pending/pointer/late callback及`>=`、Stop `==`的pinned语义；信号/target/side/quantity/B0/OMS/Gateway/product route不变 |
| no unauthorized gate/approval | pass | 无RBAC、manual ack/recovery、enable/stop gate；所有K5限制都是数据/identity/transaction正确性合同 |

## 17. Phase and Production State / 阶段与生产状态

- K1/K2/K3/K4 overall：`implemented_verified + merged`。
- K5 detailed design：`implementation_pr_open_pending_required_ci`。
- K5 implementation：`implemented_verified_local_pr_2978_pending_required_ci`。
- K6：`not_started`。
- Product runtime：未切换，现有产品route不变。
- `design_source_merge=merged_pr_2968`；merge commit=`1e739dce8a5a18d9e9e4c16027801a7a81e34384`。
- `implementation_source_merge=pending_pr_2978`；实现主体 commit=`09e0755b`。
- `close_sync=not_applicable_feature`。
- `production_ddl_gate=noop`。
- `production_dml_gate=noop`。
- backend/frontend dependency gates：`noop`。
- config/binding/broker gates：`noop`。
- `service_restart=noop`。
- `runtime_activation=noop`。
