# MiniQMT 统一执行内核 K4 vn.py Compatibility Façade F2 详细设计

> Feature tier：`F2`。文档状态：`implementation_verified_source_pending_user_authorization`；K4-A=`implemented_verified_contract_slice + merged`、K4-B=`implemented_verified_source_pending_user_authorization`。
>
> 设计交付：PR #2861，merge `8250b64ff3c2deb04eb3594f1ae3fba3acd1e6ce`，`source_merge=merged_pr_2861`。
>
> 2026-07-29 正式修订复核：K4/父蓝图/统一蓝图 F2 validator 分别为 `10/10`、`48/48`、`90/90`，均 `warnings=0`；DESIGN-COMPLIANCE-001逐项通过。该结果仅表示K4 shadow设计可实施，不表示K4代码、K5/K6、产品command authority或runtime activation已完成。
>
> 2026-07-31 K4-B正式审核补修已完成本地direct、DEV PostgreSQL、MiniQMT/Paper、coverage、L0、module registry与F2闭环；PR #2953补修HEAD `534ed8fb` 的required CI run `30573150209`全绿。K4-B=`implemented_verified_source_pending_user_authorization`。该状态不表示source已合入、产品runtime已切换或生产gate已执行。
>
> 上位唯一实现蓝图：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一上位蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 已合入前置：K1、K2、K3 overall 均为 `implemented_verified + merged`。K3-B 已通过 PR #2848 / merge `38434e10d530edd883fa75f904de5b025158f918` 合入，状态同步已通过 PR #2858 / merge `70ec6aaec28aa20755d73ecfe1a027f8ea94dbad` 合入。K4-A pure façade/adapter、strict contracts、six-source identity、observation-only characterization writer与fail-closed publication boundary已通过 PR #2883 / merge `527b2f4a58d3fe84f85c0b1f4ba2fe375d181dda` 合入；K4-B exact pinned-source executor、positive characterization/conformance、optional K2 invocation与read-only repository seam已实现并完成本地验收，source merge仍待PR，产品 runtime 未切换。
>
> 本文只细化父蓝图已经批准的 K4：initialize/transition-scoped `VnpyAlgoEngineFacadeV1`、通用façade-backed adapter和existing K2 optional invocation、精确 method/DTO/enum/state/effect 映射、每个已注册算法的 façade conformance receipt，以及 current-three + Iceberg/Stop source-compatible characterization。本文不增加算法、策略、产品 route、runtime owner、OMS、Gateway、数据库表、人工门禁或审批。

## 0. Executive Decision / 核心结论

K4 在 K1-C 双 upstream V2 authority、K1-B plugin catalog/route receipt、K2 durable kernel 和 K3 current-three pure plugins 之间补齐唯一缺失的 vn.py source-compatible façade。K4 的输出是代码所有、可重算、shadow-only 的 compatibility/conformance 事实，不是第二个执行平台，也不是产品运行开关。

固定决策如下：

1. `VnpyAlgoEngineFacadeV1` 只在单次 `initialize/transition` 内存在；它读取 immutable K2/K3 输入并只向 transition-local collector 追加 existing K1/K2 effects。
2. façade 不直接调用 Gateway、broker、repository、HTTP、DB、wall clock、global random、UUID、EventEngine 或 OMS；K2 继续拥有事务、dispatch、callback、reconcile、timer、fencing 和 retry。
3. K1-C `VnpyCompatibilityRequirementV2/VnpyCompatibilityReceiptV2/LockedSurfaceV2` 继续是 method/DTO/enum/pinned-source 唯一 authority；K4 不复制、不降级、不改写 K1 V2 receipt。
4. K4-A增加observation-only `VnpyFacadeConformanceReceiptV1`记录尚不可执行的闭合失败；K4-B增加source-authoritative `VnpyFacadeConformanceReceiptV2`证明“当前 AIstock façade 实现 + exact K1 receipt + exact manifest + exact characterization”闭合。两者都不替代K1 receipt，也不创建第二plugin catalog；只有V2可成为K4-B positive authority。
5. current-three 继续使用 K3 已合入的 exact pure plugin/factory/binding；K4 不替换其 factory，不创建平行 current-three route。K4 只对三个现有注册 plugin 生成 conformance receipt，并复用 K3 parity trace 作为行为证据。
6. Iceberg/Stop 在 K4 只进入 pinned source-compatible characterization；不创建 manifest、factory、plugin、creation binding、algo instance 或 broker command。K5 才拥有它们的插件新增与运行验收。
7. K4 完成后仍为 shadow-only；K6 继续独占 dependent-BUY durable coordinator、旧 helper 退役、route cutover、生产迁移和真实 SIM 验收。
8. K4-A 已合入的 V1 characterization/conformance/input carriers 永久保持 observation-only/fail-closed；K4-B 不改变其历史语义，而是增加 exact full-input/full-trace V2 authority。任何 V1 receipt、caller supplied result 或 hash-correct copy 都不能升级为 K4-B PASSED。
9. K4-B 只交付 shadow source execution 与 existing K2 optional seam；不会注册 Iceberg/Stop、不会切换 current-three factory、不会创建产品 command authority，也不会执行 broker/DDL/DML/runtime activation。

### 0.1 Background / 背景

K1 已冻结 plugin/manifest、双 upstream method/DTO/enum/source authority和immutable compatibility receipt；K2 已提供durable event/delivery/transition/mapping/outbox/timer/reconcile内核；K3 已把current-three迁入同一pure plugin SPI并完成committed-fact parity。当前唯一未闭合的是父蓝图§8要求的initialize/transition-scoped vn.py façade及其existing K2调用接缝：它必须把pinned调用面映射到现有K1/K2 effect，而不能让upstream算法直接拥有EventEngine、OMS、Gateway或broker。K4只关闭该缺口。

## 1. Scope / 范围

### 1.1 In scope

- 实现父蓝图 §8 已锁定的六个 `AlgoEngine` façade methods：
  - `send_order(algo,direction,price,volume,order_type,offset)->str`
  - `cancel_order(algo,vt_orderid)->None`
  - `get_tick(algo)->TickData|None`
  - `get_contract(algo)->ContractData|None`
  - `write_log(msg,algo=None)->None`
  - `put_algo_event(algo,data)->None`
- 保持 pinned `AlgoTemplate.update_tick/update_timer/update_order/update_trade` 的参数、调用顺序、return/error 行为。
- 对 `TickData/OrderData/TradeData/ContractData` 和 `Direction/Offset/OrderType/AlgoStatus` 建立 exact manifest-driven projection；构造 `ContractData/OrderData` 所必需的 selected `Exchange/Status` member只作为K4 DTO mapping，从K1已固定的exact `constant.py` bytes AST提取并进入mapping hash，不扩展plugin可请求的K1 enum capability。
- K4-A保留每个current-three plugin的V1 observation receipt；K4-B生成每个已注册plugin的exact PASSED `VnpyFacadeConformanceReceiptV2`并形成单一`VnpyFacadeConformanceSetV2` derived view，V1不得升级或混用。
- 对同一 pinned `vnpy_algotrading` commit 的 Sniper、BestLimit、TWAP、Iceberg、Stop 建立 source-compatible characterization evidence。
- current-three characterization 复用 K3 `CurrentThreeParityTraceV1`；Iceberg/Stop 只生成 characterization-only receipt，不注册或执行产品 plugin。
- 定义直接测试、negative matrix、fresh-process/restart determinism、coverage、changed-files routing、DESIGN-COMPLIANCE-001 和生产状态分离。

### 1.2 Non-goals / 非目标

- 不新增、实现或注册 Iceberg/Stop plugin、manifest、factory、state codec 或 creation binding；这些属于 K5。
- 不修改 current-three 的 K3 factory/class/binding，不以 façade-backed 替代路径重新实现 Sniper/BestLimit/TWAP。
- 不实现 K6 dependent-BUY coordinator、旧 helper 退役、产品唯一 route 切换、binding migration、production rollout 或真实 broker 验收。
- 不新增第二套 `MainEngine/EventEngine/OmsEngine/BaseGateway`，不运行 upstream `AlgoEngine` 或其事件循环。
- 不扩大 K1-C exact method list、DTO field list或 enum list；需要新 surface 时必须另行修改上位蓝图和 K1 authority，K4 不自行兼容。
- 不修改 Selection、策略信号、策略包准入、资产检查、Target、方向、数量、B0_QUOTE_V2、OMS、A股 board-lot 或 Gateway 业务语义。
- 不新增数据库表、列、migration、DDL/DML、生产配置、binding、RBAC、人工 acknowledge、审批或 enable gate。
- 不以 installed/latest vn.py、网络下载、previous receipt、旧 catalog、legacy route、默认 DTO、普通 quote、分钟线或 mock-only fixture 作为 fallback。

## 2. Current Facts and Ownership / 当前事实与所有权

### 2.1 Existing authorities

| 事实 | 唯一 owner | K4 使用方式 |
| --- | --- | --- |
| plugin manifest/config/state/event/capability | K1 contracts + K1-B catalog | strict readback；不复制 schema |
| pinned method/DTO/enum/source authority | K1-C `LockedSurfaceV2` + `VnpyCompatibilityReceiptV2` | 作为 K4 conformance 的前置输入 |
| route capability | `GatewayCapabilityCatalogV1` + `PluginRouteCompatibilityReceiptV1` | `send_order/cancel_order/get_*` 只消费 strict-valid receipt |
| event/delivery/transition/effect identity | K1/K2 carriers | façade collector 只调用 existing writer |
| transaction/CAS/fence/retry/reconcile | K2 durable kernel | K4 不实现第二 owner |
| current-three behavior/state | K3 exact plugins + parity receipts | K4 只生成 façade conformance evidence |
| product runtime route | current legacy product route，未来由 K6 一次切换 | K4 不接线 |

### 2.2 Signal/execution isolation

K4 输入只能来自已经冻结的 execution plan、algo instance、K2 event/delivery/projection 和 K1 manifest/receipt。K4 不读取 selection signal、strategy package asset、model code、portfolio target generator 或 candidate ranking；也不重新校验策略包完整性。缺少运行所需 market/contract/route projection 时只生成 typed wait/failure evidence，不回到信号层补值。

### 2.3 Module boundaries

计划生产文件固定为：

- `backend/execution_algos/vnpy_compat/facade_contracts.py`：K4 strict carriers、hash/readback、failure/receipt。
- `backend/execution_algos/vnpy_compat/facade_projection.py`：manifest-driven DTO/enum/event projection pure functions。
- `backend/execution_algos/vnpy_compat/facade.py`：transition-scoped `VnpyAlgoEngineFacadeV1` 和 collector adapter。
- `backend/execution_algos/vnpy_compat/facade_adapter.py`：通用 `VnpyFacadeBackedPluginAdapterV1`，拥有 initialize/restore/callback/state-extract/freeze 的唯一 source-compatible host seam；K4 不在 catalog 注册该 adapter。
- `backend/execution_algos/vnpy_compat/facade_characterization.py`：repo-owned pinned source characterization builder/readback。
- `backend/execution_algos/vnpy_compat/facade_source_execution.py`：单进程pure pinned-source loader/trace executor；不得import subprocess/multiprocessing/socket/repository，也不得拥有process lifecycle。
- `backend/execution_algos/vnpy_compat/pinned_source/facade_source_manifest.json`：五个算法及 `round_to` helper 的 exact source identity。
- `backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json`：五算法full executable input/expected trace artifact及六项K3 committed-fact material；不含产品binding或broker事实。该文件不得放入K1 exact `pinned_source/` 路径，否则会破坏K1固定source path set。
- `backend/execution_algos/vnpy_compat/pinned_source/vnpy_algotrading/algos/iceberg_algo.py` 与 `stop_algo.py`：同一 pinned commit 的只读 source authority。
- `backend/execution_algos/vnpy_compat/pinned_source/vnpy_core/vnpy/trader/utility.py`：只供 AST 提取 pinned `round_to`；禁止 import/execute 整个 utility module。
- `backend/services/miniqmt_execution_runtime/plugin_registry.py`：只增加从 existing `PluginCatalogSnapshotV1`（由 `build_plugin_catalog_v2` 生成）派生 conformance set 的显式 seam；不改变现有 catalog schema/hash/publication。
- `backend/services/miniqmt_execution_runtime/vnpy_facade_characterization_runner.py`：K4-B离线fresh-process orchestration与malformed/timeout evidence owner；不由product runtime/package import自动启动，不访问DB/Gateway/broker。
- `backend/services/miniqmt_execution_runtime/kernel_creation.py` 与 `kernel_delivery.py`：增加显式、可选、capability-based 的 façade invocation seam；只有 exact `VnpyFacadeBackedPluginAdapterV1` + exact PASSED conformance receipt 才进入该 seam，现有 pure `ExecutionAlgoPluginV2` 调用签名、current-three binding 和产品 routing 保持不变。
- `backend/services/miniqmt_execution_runtime/kernel_repository_event_delivery.py`与`kernel_repository_k2b.py`：只增加对existing durable runtime-event/delivery/ALGO_START/TICK facts的bounded same-cursor read-only authority query，供TIMER lineage和creation authority重建；不增加表、列、writer、lock order或第二repository owner。

禁止创建第二个 repository、dispatcher、scheduler、route evaluator、plugin registry、Gateway adapter 或 runtime controller。execution-algos目录继续通过现有import-boundary验证；fresh process只能由上列services runner显式拥有，防止为了characterization放宽插件/算法模块的side-effect边界。

## 3. Pinned Source Authority / 固定源码权威

### 3.1 Reused K1-C authority

K4 必须读取现有 repo-owned `source_manifest.json`、`surface_contract.json` 和 `VnpyCompatibilityReceiptV2`。固定 upstream：

- `VNPY_ALGOTRADING=https://github.com/vnpy/vnpy_algotrading@4133987530eb28f3538d1983545d81c4f83d7d59`
- `VNPY_CORE=https://github.com/vnpy/vnpy@4.0.0@1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9`

K4 不修改 K1-C `vnpy_pinned_source_manifest_v2` 的 exact 八 source union，也不产生 V3/V1 fallback。K1 receipt 的 source/method/object/surface/characterization component hash 公式保持不变。

### 3.2 K4 façade source manifest

父蓝图要求 current-three + Iceberg/Stop source-compatible characterization，并要求 `send_order` 保持 pinned rounded-zero return semantics。为避免把未注册算法或 helper 塞入 K1 plugin catalog，K4 新增职责单一的 `VnpyFacadeSourceManifestV1`：它只证明五个 algorithm source bytes和一个 `round_to` helper source bytes，不证明 API/DTO surface，也不发布 plugin。

Exact algorithm source set：

| algo_code | source path | bytes | SHA-256 | K4 disposition |
| --- | --- | ---: | --- | --- |
| `SNIPER_MINIQMT` | `vnpy_algotrading/algos/sniper_algo.py` | `2186` | `fbf84d2c61f8200079fe1f8da3b3412a036e5a7ffb6c601f9e4614ad110c8c76` | registered current-three conformance |
| `BEST_LIMIT_MINIQMT` | `vnpy_algotrading/algos/best_limit_algo.py` | `3560` | `b35227b932a160c2f786d3202283b61656d9f16631fb42f596a9d376765617e9` | registered current-three conformance |
| `TWAP_LITE_MINIQMT` | `vnpy_algotrading/algos/twap_algo.py` | `2532` | `aeabb067ef79d48182f357b8d4736f8a90f6a4ecb77bc82506a3244575a6cd0f` | registered current-three conformance |
| `ICEBERG` | `vnpy_algotrading/algos/iceberg_algo.py` | `3228` | `9019cd20e4288b1642f7bc5f1508244eb9ccb419a2a888f69040fd9c5c6a2c21` | characterization only；K5 owns registration |
| `STOP` | `vnpy_algotrading/algos/stop_algo.py` | `2631` | `18a758b2d86b0704b00ce385f3517061e21dee57178c3abfd10271091e8db090` | characterization only；K5 owns registration |

Iceberg/Stop 的 upstream audit URL 固定为相同 commit 下的 raw source；implementation/CI 只读取 repo-owned bytes，不联网。

Exact helper source set：

| helper | upstream namespace | source path | bytes | SHA-256 | execution rule |
| --- | --- | --- | ---: | --- | --- |
| `round_to` | `VNPY_CORE` | `vnpy/trader/utility.py` | `32957` | `9bce3f6e18c84668b0ffadd717f0b6fd4ca2b454dc748dad6572af78c850608d` | 只AST提取lines 111–118等价函数体并字符化；禁止import整个module及其talib/path side effects |

`round_to` exact characterization为：`decimal_value=Decimal(str(value))`、`decimal_target=Decimal(str(target))`、`float(int(round(decimal_value/decimal_target))*decimal_target)`。K4 writer必须先证明target是finite positive contract `min_volume`，再执行该公式；不得替换为floor/ceil、自定义lot rounding或浮点 `%`。

`VnpyFacadeSourceV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_source_v1
source_role=ALGORITHM|HELPER
algo_code_or_helper_name, upstream_namespace
upstream_repo, upstream_commit, source_path, source_size, source_sha256
registration_disposition=REGISTERED_CURRENT_THREE|CHARACTERIZATION_ONLY_K5|FACADE_HELPER_ONLY
source_identity_sha256
```

`source_identity_sha256 = hash_hex_v1("miniqmt_vnpy_facade_source_identity_v1", exact preceding fields)`。

`VnpyFacadeSourceManifestV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_source_manifest_v1
ordered_upstream_authority_sha256
ordered_sources
manifest_sha256
```

sources 按 `(source_role,registration_disposition,algo_code_or_helper_name,source_path)` 排序；必须恰好为上表五个algorithm和一个helper，拒绝 missing/extra/duplicate/traversal/backslash/drive path。ordered upstream authority必须分别等于 K1-C source manifest 内 `VNPY_ALGOTRADING/VNPY_CORE` authority hash并按namespace排序。`manifest_sha256 = hash_hex_v1("miniqmt_vnpy_facade_source_manifest_v1", exact preceding fields)`。

## 4. Architecture / 架构

```text
PluginCatalogSnapshotV1 + K1 VnpyCompatibilityReceiptV2
                 + strict PluginRouteCompatibilityReceiptV1
                 + K2 delivery/event/projection/state/mapping facts
                                      |
                                      v
          sealed VnpyFacadeConformanceAuthorityV2 readback
                                      |
                                      v
        K2 optional invocation -> VnpyFacadeBackedPluginAdapterV1
                                      |
                                      v
          initialize/transition-scoped VnpyAlgoEngineFacadeV1
                    + immutable projection view
                    + transition-local effect collector
                                      |
                                      v
         existing AlgoTransitionV1 / BrokerCommandV2 /
         TimerMutationV1 / DiagnosticObservationV1
                                      |
                                      v
                  existing K2 transaction/materializer/outbox
```

K4 不向该图添加 runtime、repository 或 broker owner。façade 返回后，其对象、projection object 和 collector 全部失效；下一 delivery 从 durable state/mapping/event 重建。

### 4.1 Exact façade contract and implementation binding

`facade_contract_sha256` 不是自由字符串或文档版本号。K4 必须由下列 code-owned strict carriers 构造唯一 contract。

`VnpyFacadeImplementationBindingV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_implementation_binding_v1
component_name
callable_ref, callable_signature_sha256
repo_relative_source_path, canonical_lf_source_size, canonical_lf_source_sha256
binding_sha256
```

`component_name` exact set 为 `facade.create,facade.send_order,facade.cancel_order,facade.get_tick,facade.get_contract,facade.write_log,facade.put_algo_event,collector.create,collector.freeze,adapter.initialize,adapter.restore,adapter.transition,adapter.extract_state,projection.tick,projection.order,projection.trade,projection.contract,projection.enum,helper.round_to,characterization.deterministic_uniform,state_mapping.build,state_mapping.readback,source_manifest.build,source_manifest.readback,characterization.module_binding.build,characterization.module_binding.readback,characterization.build,characterization.readback,conformance.build,conformance.readback`；按 component name 排序且唯一。`binding_sha256 = hash_hex_v1("miniqmt_vnpy_facade_implementation_binding_v1", exact preceding fields)`。writer/readback必须从 live callable ref/signature和repo-owned canonical-LF source bytes独立重算；source/callable漂移不能通过重算caller supplied hash接纳。

`VnpyFacadeMethodContractV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_method_contract_v1
surface_owner, method_name
pinned_surface_ref_kind=K1_METHOD_REQUIREMENT|PINNED_TEMPLATE_HELPER
pinned_surface_ref_sha256
ordered_invocation_phases
ordered_required_authority_refs
return_disposition, empty_return_disposition
ordered_effect_types, ordered_reason_codes
implementation_binding_sha256
method_contract_sha256
```

六个AlgoEngine methods与四个callback methods引用K1 exact `method_requirement_sha256`；`start/buy/sell/cancel_all/finish/pause/resume/put_event` helper引用pinned template source identity + exact AST signature/body hash组成的`PINNED_TEMPLATE_HELPER` ref。各自一项并按 `(surface_owner,method_name)` 排序且唯一。`method_contract_sha256 = hash_hex_v1("miniqmt_vnpy_facade_method_contract_v1", exact preceding fields)`。

`VnpyFacadeContractV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_contract_v1
requirement_sha256, surface_sha256, method_signature_sha256, object_field_sha256
ordered_implementation_bindings, implementation_binding_set_sha256
ordered_method_contracts, method_contract_set_sha256
dto_mapping_set_sha256, state_mapping_set_sha256, terminal_mapping_set_sha256
isolated_module_binding_set_sha256
facade_contract_sha256
```

`VnpyFacadeIsolatedModuleBindingV1` exact fields为`schema_version,module_name,export_name,binding_owner=K1_PINNED_SOURCE|K4_FACADE_IMPLEMENTATION|K4_DTO_PROJECTION|K4_ENUM_PROJECTION|K4_PINNED_HELPER_IMPLEMENTATION|K4_DETERMINISTIC_INPUT_ADAPTER,binding_ref,binding_source_identity_sha256_or_implementation_binding_sha256,binding_sha256`。exact set必须包含`vnpy_algotrading.base.AlgoStatus`、`vnpy_algotrading.template.AlgoTemplate`、`vnpy.trader.engine.BaseEngine -> VnpyAlgoEngineFacadeV1`、`vnpy.trader.utility.round_to -> helper.round_to`、`random.uniform -> characterization.deterministic_uniform`、四个§7 DTO class及算法实际访问的selected enum owners/members；按`(module_name,export_name)`排序且唯一。不得暴露`MainEngine/EventEngine/OmsEngine/Gateway`，不得使用installed module、stub、MagicMock或`sys.modules`既有对象。binding/set domains分别为`miniqmt_vnpy_facade_isolated_module_binding_v1`与`miniqmt_vnpy_facade_isolated_module_binding_set_v1`，writer/readback从K1 pinned source identity、K4 live implementation binding和DTO/enum mapping独立重建。

`helper.round_to`必须由§3.2 exact utility AST body生成实际 callable，signature/return和canonical-LF source binding进入implementation hash；禁止import整个`utility.py`。`characterization.deterministic_uniform(a,b)`必须为source-isolated harness专用、signature兼容的transition-local callable：从当前vector的`explicit_deterministic_inputs.ordered_uniform_draws`按call ordinal消费一个exact u53 integer，计算`u=u53_integer/2**53`，验证finite bounds后按pinned Python `a + (b-a) * u`公式产生结果，并把ordinal/bounds/u53/result写入actual call trace；missing/extra draw、bound drift或重复消费均FAILED。它不得读取`random` module state，且不进入产品K3 path；K3继续使用既有deterministic u53 authority。

implementation/method两个 set hash分别使用 `miniqmt_vnpy_facade_implementation_binding_set_v1` 与 `miniqmt_vnpy_facade_method_contract_set_v1`，覆盖 ordered full payloads；`facade_contract_sha256 = hash_hex_v1("miniqmt_vnpy_facade_contract_v1", exact preceding fields)`。该 contract同时绑定 K1 pinned surface、K4 semantic mapping、isolated import owner和实际实现 source/binding，禁止固定常量、仅凭测试向量自证或 previous contract fallback。

## 5. Initialization/Transition Construction Contract / 初始化与单次 transition 构造合同

### 5.1 Shared authority input

`VnpyFacadeAuthorityInputV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_authority_input_v1
plugin_catalog_snapshot: PluginCatalogSnapshotV1
gateway_capability_catalog: GatewayCapabilityCatalogV1
plugin_key
manifest: ExecutionAlgoPluginManifestV2
pinned_compatibility_receipt: VnpyCompatibilityReceiptV2
route_compatibility_receipt: PluginRouteCompatibilityReceiptV1
facade_conformance_receipt: VnpyFacadeConformanceReceiptV1
facade_conformance_set: VnpyFacadeConformanceSetV1
authority_input_sha256
```

`authority_input_sha256 = hash_hex_v1("miniqmt_vnpy_facade_authority_input_v1", exact preceding full canonical payloads)`。writer/readback固定按以下顺序执行：strict-readback catalog；从 catalog 解析 exact descriptor/manifest/K1 receipt；strict-readback gateway catalog；调用 `route_receipt.validate_against_authority_v1(catalog_snapshot,gateway_catalog)`；从 conformance set按 plugin key解析唯一 receipt并使用§11相同authority重建。任一 status 非 PASSED、missing/extra/duplicate、hash-correct authority drift或plugin/route/catalog/gateway identity冲突均 typed fail，不能用“上游已验证”作为不可复核布尔标志。runtime adapter invocation还必须要求conformance receipt的`runtime_binding_disposition=FACADE_BACKED_ADAPTER`；`PURE_PLUGIN_SHADOW_CONFORMANCE`只计K4证据，不能调用adapter seam。K4/K5 shadow root还必须要求`command_authority_disposition=SHADOW_ONLY_K2_V1`；当前V1 receipt不能表示产品command authority，未来K6产品root必须拒绝以任何K4/K5 V1 receipt作为activation proof。

该 V1 input只保留K4-A structural/lifecycle负例，不得作为K4-B positive invocation。K4-B增加`VnpyFacadeConformanceAuthorityValidationReceiptV2`与sealed process-local `VnpyFacadeConformanceAuthorityV2`：validation receipt exact fields为`schema_version,conformance_set_v2_sha256,source_executor_binding_sha256,ordered_source_execution_set_sha256s,validation_input_sha256,status=PASSED|FAILED,ordered_failures,receipt_sha256`，domain为`miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2`；authority object只能由§11 `validate_vnpy_facade_conformance_set_against_authority_v2`创建，保存strict V2 set/receipt/binding和validation receipt，不可由caller mapping、bool或裸hash构造。

`VnpyFacadeAuthorityInputV2` exact fields为V1 catalog/gateway/plugin/manifest/K1/route字段加`facade_conformance_receipt_v2,facade_conformance_set_v2,conformance_authority_validation_receipt_v2,source_executor_binding_sha256,source_execution_set_sha256,authority_input_sha256`，schema/domain均为`miniqmt_vnpy_facade_authority_input_v2`。创建时必须从sealed authority object解析唯一plugin receipt并复核live catalog/gateway/route、descriptor/process factory、adapter/algorithm binding；不能直接接受上述receipt参数。source execution与五算法rebuild在construction root创建sealed authority时完成，不得在K2数据库事务内启动subprocess；每次invocation仍strict-readback全部carrier和live callable binding，发现process binding漂移立即typed fail并使本次effect为零。

### 5.2 Initialization input

`VnpyFacadeInitializationInputV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_initialization_input_v1
start_event: RuntimeEventEnvelopeV2
start_delivery: AlgoEventDeliveryV1
start_context: AlgoStartContextV1
authority_input: VnpyFacadeAuthorityInputV1
transition_id, transition_sequence=1
input_sha256
```

`input_sha256 = hash_hex_v1("miniqmt_vnpy_facade_initialization_input_v1", exact preceding full canonical payloads)`。`start_event`必须是 exact `ALGO_START`；event/delivery/context/runtime/algo/plugin/config/plan/release/policy、contract/account/capability projection和logical time必须逐项闭合。adapter只在该路径执行 pinned algorithm constructor一次，然后执行 pinned `start()` 一次；constructor/start产生的 `put_event/write_log` 按调用顺序进入同一 initialization collector。initial retry从相同 input重建并产生 byte-identical state/effects，不能再次提交不同 command。

K4-B positive seam使用`VnpyFacadeInitializationInputV2`：除`authority_input`必须是V2外字段和业务语义与V1一致，schema/domain升级为`miniqmt_vnpy_facade_initialization_input_v2`。`transition_id`必须由existing `algo_transition_id_v1(delivery_id,event_id,runtime_id,algo_instance_id,transition_sequence=1)`重建，不能由caller、UUID或尚不存在的transition row提供。V1 input不自动升级为V2。

### 5.3 Transition input

`VnpyFacadeTransitionInputV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_transition_input_v1
runtime_event: RuntimeEventEnvelopeV2
delivery: AlgoDeliveryPersistenceV1
algo_instance: ExecutionAlgoInstancePersistenceV2
manifest: ExecutionAlgoPluginManifestV2
authority_input: VnpyFacadeAuthorityInputV1
before_state: AlgoStateSnapshotV2
read_only_services: AlgoReadOnlyServicesV1
command_lifecycle_projection: KernelCommandLifecycleProjectionV1
ordered_active_mappings: tuple[ExecutionCommandChildMappingV1,...]
deterministic_context: DeterministicExecutionContextV1
transition_sequence
input_sha256
```

`input_sha256 = hash_hex_v1("miniqmt_vnpy_facade_transition_input_v1", exact preceding full canonical payloads)`。writer/readback必须先分别 strict-validate existing carriers，再验证 runtime/algo/plugin/event/delivery/sequence/route、`read_only_services.execution_projection_set`、deterministic context和lifecycle projection closure。active mappings按 `local_vt_orderid` 排序且parent/runtime/algo/symbol/side必须一致；mapping集合必须与lifecycle projection active local-id集合完全相等。使用 `AlgoReadOnlyServicesV1` 而不是只有refs的 `ExecutionProjectionSetV1`，因此 `get_tick/get_contract` 消费的是hash-closed immutable payload，不允许从ref猜值。

K4-A V1 transition carrier存在一个已确认的前置/完成事实混淆：它从`delivery.transition_id`取得collector identity并要求该值非null，但existing K2 `apply_claimed_delivery_atomic`是在调用plugin之后才物化transition；真实callback执行时locked delivery必须是`CLAIMED + transition_id=null`。K4-B禁止通过提前写transition row、伪造非null field或放松K2事务解决该冲突，固定新增`VnpyFacadeTransitionInputV2`：

```text
schema_version=miniqmt_vnpy_facade_transition_input_v2
runtime_event, claimed_delivery, algo_instance, manifest
authority_input: VnpyFacadeAuthorityInputV2
before_state, read_only_services, command_lifecycle_projection
ordered_active_mappings, deterministic_context
transition_id, transition_sequence, input_sha256
```

`claimed_delivery`必须为exact `AlgoDeliveryPersistenceV1`，status=`CLAIMED`、`transition_id/failure_receipt_id/skip_receipt_id/closed_at_utc`均null，lease owner/worker/incarnation/epoch/fence/expiry和row_version与当前`apply_claimed_delivery_atomic` locked CAS参数逐项相等。`transition_id`必须等于existing `algo_transition_id_v1(...)`对该delivery/event/runtime/algo/sequence的结果；materializer随后必须生成同一identity。其它V1 owner/state/mapping/service closure全部保留并切换到V2 hash domain。adapter/collector/facade从input的`transition_id`读取identity，绝不能再从pre-call delivery完成字段读取。retry持有same delivery+fence时重建byte-identical input；new lease epoch仍保持相同transition identity但由K2 CAS拒绝stale writer。

### 5.4 Single K2 invocation seam

K4交付通用 `VnpyFacadeBackedPluginAdapterV1`，它实现existing `ExecutionAlgoPluginV2`并额外提供 exact `initialize_with_facade(input)` 与 `transition_with_facade(input)`。K4-B只在existing `invoke_plugin_initialize_v1/invoke_plugin_transition_v1` 增加一个显式 optional façade input：

```python
invoke_plugin_initialize_v1(
    *, plugin, expected_manifest, start_context,
    facade_input: VnpyFacadeInitializationInputV2 | None = None,
) -> AlgoInitializationV1

invoke_plugin_transition_v1(
    *, plugin, expected_manifest, state_codec, state, event,
    services, deterministic_context,
    facade_input: VnpyFacadeTransitionInputV2 | None = None,
) -> AlgoTransitionV1
```

`VnpyFacadeBackedPluginAdapterV1`新增`initialize_with_facade_v2/transition_with_facade_v2`并只接受上述V2 carrier；K4-A既有V1方法与V1 tests继续保留但不能发布positive receipt。两个existing invoke函数仍是唯一SPI入口，返回值继续走原`AlgoInitializationV1/AlgoTransitionV1`完整closure validation；V2 method不得绕过existing result validator或materializer。

- ordinary pure plugin + `facade_input=None` 继续走现有调用，签名和结果验证不变；
- exact `VnpyFacadeBackedPluginAdapterV1` 必须携带与§4.1 binding byte-identical的adapter identity、`FACADE_BACKED_ADAPTER` receipt，并且必须收到对应 initialization/transition input；
- adapter为满足existing SPI而暴露的base `initialize/transition`若被绕过K2 seam直接调用，必须以`MINIQMT_VNPY_FACADE_BINDING_INVALID`失败并产生zero effect；禁止把facade input暂存在singleton、ContextVar、thread-local或adapter mutable field后再调用base method；
- ordinary plugin收到facade input、façade-backed adapter缺少input、adapter/conformance/manifest不一致均以 `MINIQMT_VNPY_FACADE_BINDING_INVALID` fail loud，zero callback/effect；
- 不按algo code分支、不动态wrap ordinary plugin、不使用`hasattr`/duck-typing fallback、不替换current-three K3 factory；
- K5使用existing manifest fields：`implementation_ref`指向其factory、`compatibility_requirement`等于K4 characterization requirement、`behavior_characterization_sha256`等于exact characterization receipt hash、`state_schema`包含§10.2 envelope；其factory只返回携带exact algorithm binding的通用adapter并新增state codec/tests，因此K5无需修改kernel或增加manifest top-level field；K4不预注册Iceberg/Stop。

creation coordinator从其现有 exact catalog/gateway/route/start facts构建initialization input；delivery worker在同一locked read范围内从durable ALGO_START event、catalog runtime、configured strict gateway catalog、locked algo/delivery/state/mapping/lifecycle facts构建transition input。该seam不创建第二 runtime、repository、dispatcher或route。

construction root必须显式向existing `KernelAlgoCreationCoordinatorV1` 与 `KernelDeliveryWorkerV1` 注入同一strict gateway/conformance authority；不得模块全局查找。authority为`None`时ordinary pure plugin照常运行；若resolved plugin为façade-backed adapter则缺authority立即typed fail。authority存在也不能切换ordinary plugin。K4 shadow harness显式注入；产品root直到K6仍不注入/不激活façade-backed binding。

K4-B把该optional依赖的实际类型收口为sealed `VnpyFacadeConformanceAuthorityV2 | None`，不再向runtime注入裸V1/V2 set。`KernelAlgoCreationCoordinatorV1.__init__`保持现有三个必填参数并只增加keyword-only optional authority；`KernelDeliveryWorkerV1.__init__`增加同一optional authority及仅在其非null时必填的strict gateway catalog。ordinary product construction继续传`None`，因此函数调用、factory、current-three行为和对象graph不改变；测试/未来K5 shadow root显式传同一authority object。禁止环境变量、配置bool、algo-code allowlist、global singleton、manual acknowledge或“set存在即切换全部plugin”。

K4 当前 product catalog 中不存在 `FACADE_BACKED_ADAPTER` descriptor：current-three必须保持K3 pure binding，Iceberg/Stop由K5拥有。K4 direct/integration test可以使用由同一production manifest/receipt/adapter constructors构建、全量strict-readback的test-only closed candidate来覆盖optional invocation，但该candidate不得写入code-owned current catalog、creation binding或durable DB，也不能单独作为K4完成证据。K4完成证据必须同时包含真实current-three catalog的`PURE_PLUGIN_SHADOW_CONFORMANCE`、真实pinned Iceberg/Stop characterization及optional seam的production-constructor正反测试；第一个真实registered shadow adapter由K5交付并复用同一seam。文档和receipt必须把`test_only_candidate=true`留在测试证据上下文且不进入产品hash/status。

### 5.4.1 K2 V1 command-authority boundary

K4/K5 均保持 shadow-only，不能把 existing K2 command projection 能力虚报为 façade-backed 产品提交已经闭合。当前 `ExecutionProjectionSetV1` 对每个 `projection_type` 只允许一个 ref，而一次 upstream callback 可以通过 `cancel_all()` 产生多个有序 CANCEL command；现有 `OMSPreflightProjectionReceiptV1` 和 `MiniQMTRiskDecisionReceiptV1` 又分别绑定单个 child/command owner。因此 K4 不允许采用以下简化：复用第一条 command 的 receipt、把多条 command 合成一条、丢弃后续 command、为每条 command 写同 type duplicate ref、固定 PASS，或在 façade 内绕过 K2 preflight。

K4 的 exact 交付边界固定为：

- source-isolated characterization 与 pure collector 必须完整保留同一次 callback 的全部 ordered commands，包括 `cancel_all()` 多命令；ordinal、identity、state/effect trace均参与 receipt，不能只测第一条；
- K2 broker-neutral shadow materialization 只对 existing V1 projection set 能精确证明的单 command transition执行，并且 authority 必须来自同一 committed legacy fact/characterization vector的 exact route/OMS/risk/kill receipt；这些 receipt 只证明 shadow parity，`dispatch_attempt=0`、`broker_called=false`；
- 若 initialization/transition 产生多 command，K4 shadow seam仍返回完整 `AlgoTransitionV1` 供 pure conformance 比较，但不得调用 existing materializer并不得发布“product compatible” receipt；必须产生 `MINIQMT_VNPY_FACADE_MULTI_COMMAND_PRODUCT_AUTHORITY_UNAVAILABLE` 的 typed shadow observation，且该 observation不能删减或改写原 command set；
- K4/K5 的 `VnpyFacadeConformanceReceiptV1/V2` 必须将 `command_authority_disposition` 固定为`NOT_APPLICABLE_PURE_PLUGIN|SHADOW_ONLY_K2_V1`；V1永不PASSED，V2也只证明 façade/source/state/effect compatibility，不等于产品 command-authority receipt，不能作为 runtime activation 条件的替代证据；
- K6 在产品 cutover 前必须以独立 F2 详细设计关闭 generic per-command authority aggregate、writer/readback、transition projection hash、materializer、同步reject返回语义和restart/reconcile链；不得通过双跑algorithm callback、回写第一次probe state或先返回local id再把OMS reject伪装成成功。K6 可以一次性扩展 generic kernel seam，但不得按 algo code 分支；该F2必须定义独立versioned product command-authority receipt和activation readback，不能只给K4 V1增加enum/hash。K4/K5 不预先发明该 carrier、DB schema或产品 writer。

这是现有 K2 V1 carrier 的技术事实边界，不是人工审批、enable flag或永久 stop gate。K4 implementation可以在该边界内完整完成；产品 façade-backed route 仍由 K6 独占，未完成 K6 时不得宣称可运行产品命令。

### 5.5 Façade construction

Exact constructor：

```python
VnpyAlgoEngineFacadeV1.create(
    invocation_input: VnpyFacadeInitializationInputV1 | VnpyFacadeTransitionInputV1 |
                      VnpyFacadeInitializationInputV2 | VnpyFacadeTransitionInputV2,
    effect_collector: VnpyFacadeEffectCollectorV1,
) -> VnpyAlgoEngineFacadeV1
```

V1分支仅保留K4-A observation/lifecycle证据；K4-B positive adapter调用必须走V2。V2 transition collector owner必须等于`invocation_input.transition_id`，不得读取`claimed_delivery.transition_id`。

`create()` 必须验证：

1. §5.1全部K1/catalog/gateway/route/conformance authority为 exact PASSED；
2. manifest `required_facade_methods/object_fields/order_types/market_data_requirements` 与调用面一致；
3. initialization 或 before state、active mappings、actual projection payload和K2 owner/version/hash完整闭合；
4. collector尚未冻结且transition identity/sequence与input完全相同；
5. invocation phase与adapter method严格一致，INITIALIZE不得携带predecessor mapping，TRANSITION不得重跑constructor/start。

任一失败不构造 façade、不调用 algorithm callback、不生成 effect。不得使用 previous conformance、默认 projection 或 legacy product helper。

### 5.6 Collector lifecycle

`VnpyFacadeEffectCollectorV1` 不是新业务 effect authority；它只按调用顺序收集 existing effect constructors 的输入。exact seam为：

```python
VnpyFacadeEffectCollectorV1.create(
    deterministic_context, parent_intent_id, transition_id
) -> VnpyFacadeEffectCollectorV1

collector.freeze(
    next_state: AlgoStateSnapshotV2,
    terminal_outcome: TerminalOutcomeV1 | None,
) -> AlgoTransitionV1
```

- ordinal 从 `0` 连续递增，禁止 caller 注入、跳号或复用；
- `send_order/cancel_order` 生成 existing `BrokerCommandV2`；
- `write_log/put_algo_event/missing get_*` 生成 existing `DiagnosticObservationV1`，payload 使用 §10 strict schema；
- 不生成 TIMER；timer仍只来自plugin lifecycle的existing `TimerMutationV1`和K2 ExchangeSessionClock；
- `freeze()`先strict-readback `next_state`并验证algo/plugin/event/delivery/transition sequence/logical time及manifest state schema，再用existing `AlgoTransitionV1` constructor闭合state/effects/terminal outcome；
- freeze 后任何调用 typed fail；同 input重试必须byte-identical；same identity/different effect terminal conflict；
- collector不持久化、不dispatch、不ACK。

## 6. Exact Façade Method Semantics / 精确方法语义

### 6.1 `send_order`

输入必须满足 pinned signature；`direction/order_type/offset` 必须是 §7 exact enum projection。`price` 必须为 finite positive number并 canonicalize为 existing decimal price；`volume` 接受 pinned `float` surface，但必须是 finite、positive且非 `bool`。façade使用§3.2 exact pinned `round_to(volume,contract.min_volume)`；除该公式外禁止任何隐式四舍五入。rounded结果为zero时走下述upstream-compatible空字符串路径；非zero结果必须是exact integral A-share shares，否则typed fail。字符串、NaN、Inf全部拒绝。

处理顺序固定：

1. manifest 声明 `send_order`、order type、side 和 required market capability；
2. route receipt 对 exact order/capability 为 PASSED；
3. contract projection 存在且 symbol/exchange/gateway/min_volume/pricetick 与 owner/hash闭合；
4. volume只执行pinned `round_to`；façade不执行、不复制也不猜测 OMS/board-lot/risk/kill-switch判断；
5. collector 以 next ordinal 调用 `BrokerCommandV2.create(SUBMIT_LIMIT,...)`；K4 shadow materialization只有在§5.4.1所述existing K2 authority与该单command owner精确闭合时才继续；
6. 返回该 command 的 deterministic `local_vt_orderid`。existing K2 materializer继续拥有projection校验与durable commit，K4不把preflight结果转换成另一套façade业务状态。

missing contract 或 pinned `round_to` 产生zero时，按 upstream返回 `""`，同时必须追加stable typed diagnostic，`broker_called=false`、zero broker command。existing K2 OMS reject不是rounded-zero；在K4 shadow中必须保留原reject receipt并阻止materialization，不能转换为空字符串成功、PASS或另一条command。`SHADOW_ONLY_K2_V1` conformance只覆盖valid-command return及reject可见性，不得宣称已证明upstream同步reject返回；K6必须按§5.4.1关闭该产品语义并生成独立versioned product authority receipt后才可激活。其它invalid/capability/identity冲突直接typed fail。façade不调用Gateway。

### 6.2 `cancel_order`

`vt_orderid` 必须命中 transition input 中唯一 active mapping，且 runtime/algo/parent/child/submit command/broker identity 与 state closure 完整。unknown、duplicate、inactive、cross-owner 或缺 broker identity 均 typed fail；不得只写日志后返回。

合法调用以 next ordinal 创建 existing `BrokerCommandV2(CANCEL_ORDER)`，引用 exact owned local/broker mapping。它不创建第二 child、不改变 mapping、不调用 Gateway，返回 `None`。

### 6.3 `get_tick`

K4-B positive path只从 `VnpyFacadeTransitionInputV2.read_only_services.market_data_projection` 构造 §7 `TickData` view，且projection ref/payload/hash必须闭合；V1仅保留K4-A observation tests。对 current-three及K5计划算法，quote authority只能是native `B0_QUOTE_V2` continuous L1；不得从minute、process cache、ordinary quote、另一侧盘口、last price或auction合成continuous fields。

- `TICK` delivery：market projection必须与当前 `RuntimeEventEnvelopeV2` strict TICK payload、source event、generation、control/assignment和B0 lineage byte-identical；
- `TIMER` delivery：existing K2 read-only query从同一 `algo_instance_id` 的已持久化、`APPLIED` prior delivery反查最新eligible TICK；必须同时满足 `delivery.algo_delivery_sequence < timer_delivery.algo_delivery_sequence`、`event.sequence < timer_event.sequence`、same runtime/algo/symbol/session epoch与native B0 source。projection ref的 `source_event_id/payload_sha256/logical_at_utc` 必须指向该exact event，并增加 `ConsumedLineageRefV1(MARKET_DATA)`；
- query固定使用existing `uq_miniqmt_k2_delivery_sequence(algo_instance_id,algo_delivery_sequence)`和event owner key，`ORDER BY delivery.algo_delivery_sequence DESC,event.sequence DESC,event.event_id DESC LIMIT 1`。timer delivery/event sequence构成immutable双cutoff，因此retry/restart不因随后quote到达而改变；latest prior candidate若cross-session/cross-phase/cross-symbol、B0 payload自身eligibility/freshness非READY、non-APPLIED或非B0则直接按unavailable处理，不继续向更旧事件回退；不使用wall-clock重新计算TTL。duplicate owner、future sequence或carrier/readback drift均typed fail；
- 其它callback只有manifest明确声明需要current market projection且input builder提供上述exact lineage时才可读取，否则按missing处理。

没有当前合法 view 时返回 `None`，并追加 `MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE` diagnostic，包含 runtime/algo/event/delivery/symbol、required fields、missing fields、market-data lineage 和 reason。不得读取 process cache fallback。

### 6.4 `get_contract`

初始化时从 `AlgoStartContextV1.contract_projection`、normalized A-share symbol和§5.1 strict route/gateway authority构造 `VnpyFacadeContractViewV1`，并作为§10 state envelope必需字段进入首个durable `AlgoStateSnapshotV2`。exact fields为 `schema_version,runtime_id,algo_instance_id,symbol,exchange_member,gateway_name,min_volume,volume_increment,pricetick_decimal,contract_projection_sha256,gateway_catalog_sha256,route_receipt_sha256,contract_view_sha256`；hash domain为 `miniqmt_vnpy_facade_contract_view_v1`。

`exchange_member`只允许 normalized symbol suffix exact table `.SH->SSE,.SZ->SZSE,.BJ->BSE`，table及selected member value expression从K1 pinned `constant.py` AST重算并进入 `dto_mapping_set_sha256`；`gateway_name`必须等于strict gateway route/backend mapping，不能使用任意caller string。后续transition只从strict-readback state envelope返回同一view，并与current catalog/gateway/route hashes复核。缺失或漂移返回 `None` + `MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE` diagnostic；不得猜exchange、gateway、min_volume或pricetick。

### 6.5 `write_log`

`msg` 必须是严格字符串；pinned surface允许空字符串，因此K4不得额外拒绝或改写空消息。UTF-8 canonical 后最大内联2048字符；超限保留前2048字符并在同一 diagnostic payload 中记录 original length 和 full-message SHA-256，不能静默截断或把截断文本冒充完整原文。`algo` 若非 null 必须等于当前 transition owner。日志只产生 bounded diagnostic，不能代替 exception、failure receipt、command receipt 或 callback evidence。

### 6.6 `put_algo_event`

只接受 §10 `VnpyAlgoProjectionObservationV1` 可严格重建的 parameter/variable/status payload；拒绝 unknown field、mutable object、callable、NaN/Inf、非字符串 key 和 owner drift。追加 existing diagnostic observation，不启动 EventEngine，不广播产品 UI，不持久化第二份 algo state。

### 6.7 Template helpers and callback methods

`buy/sell/cancel_order/cancel_all/get_tick/get_contract/write_log/put_event/finish/pause/resume` 保持 pinned `AlgoTemplate` 调用关系；它们不是新增 façade surface。`cancel_all` 必须按 frozen active mapping 的 `local_vt_orderid` 排序展开，禁止裸 broker cancel-all。

`buy/sell` 在algo status非RUNNING时由K4 adapter在调用pinned helper前后核对返回值；exact空返回必须追加 `MINIQMT_VNPY_FACADE_ALGO_NOT_RUNNING` diagnostic，zero broker command。该路径不能只依赖AlgoEngine method，因为pinned helper在调用engine前已经返回。missing contract和rounded-zero继续使用各自reason，三类empty return不得合并成成功或空ACK。

`update_tick/update_timer/update_order/update_trade` 只能由 exact K2 delivery route 调用一次，顺序等于 per-algo delivery sequence。callback 不得直接调用 repository/Gateway，也不得由 tick 推断 ORDER/TRADE。

## 7. DTO and Enum Projection / DTO 与枚举映射

### 7.1 General rules

- 只实现 K1-C `required_object_fields/required_enum_values` 和 exact manifest 声明字段；没有 dynamic `__getattr__`。
- projection object transition-local、不可跨 delivery 缓存；source facts immutable。
- 输入先 strict readback，再映射；不得先构造默认 DTO 后补字段。
- 对同一 source payload 映射结果 byte-identical；mapping table进入 `dto_mapping_sha256`。
- enum 使用 explicit table；未知值 fail loud，不按字符串名称猜测。
- `Exchange/Status`只允许构造required DTO所需的selected member mapping，来源必须是K1 pinned core source AST；它们不进入plugin可请求enum capability，也不能用installed vn.py enum补值。

K4实际向pinned algorithm暴露四个transition-local frozen projection class，而不是用可变dict或带大量默认值的installed DTO。class exact field union由五个§12.1 requirements生成：`TickData={vt_symbol,datetime,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,last_price,limit_up,limit_down}`、`OrderData={vt_orderid,status,traded,price,is_active()}`、`TradeData={vt_orderid,vt_tradeid,price,volume,datetime}`、`ContractData={symbol,exchange,gateway_name,min_volume,pricetick}`。每个algo只允许访问其requirement声明子集；所有attribute在construction时显式提供，除了pinned明确nullable的`TradeData.datetime`外无默认值。unknown attribute没有`__getattr__`，mutation失败；`OrderData.is_active()`使用本节exact Status table。isolated source import中的四个class name解析为这些实际projection classes，不能换成MagicMock或宽松SimpleNamespace。

### 7.2 Exact mapping table

| upstream object/field | AIstock source | rule |
| --- | --- | --- |
| `TickData.vt_symbol` | frozen symbol + exchange projection | exact `symbol.exchange`；不猜 exchange |
| `TickData.datetime` | exact selected TICK source event logical timestamp | Asia/Shanghai aware；TIMER读取时仍使用source TICK time，不替换为timer/wall clock |
| `bid_price_1/bid_volume_1/ask_price_1/ask_volume_1` | native B0 L1 lineage | exact decimal/strict non-negative volume；required side missing返回 `None` + diagnostic |
| `last_price/limit_up/limit_down` | manifest-declared native field | K4 仅 characterization Stop；K5 未注册前不声明产品可用 |
| `OrderData.vt_orderid` | mapping `local_vt_orderid` | 不以 broker id 替代 |
| `OrderData.status/traded/price/is_active()` | strict ORDER/RECONCILE event + durable mapping | status member从K1 pinned `Status` AST selected table映射；cumulative/price和active predicate共用K3/K2 callback authority |
| `TradeData.vt_orderid` | exact mapping local id | callback mapping必须唯一 |
| `TradeData.vt_tradeid/price/volume/datetime` | strict TRADE event | trade identity唯一；时间来自 broker event authority |
| `ContractData.symbol/exchange/gateway_name/min_volume/pricetick` | durable `VnpyFacadeContractViewV1` + current strict route authority | 全部必需；exchange使用`.SH/.SZ/.BJ` exact selected member table；缺失返回 `None` + diagnostic |
| `Direction.LONG/SHORT` | existing BUY/SELL | exact two-way table；不支持 NET |
| `Offset.NONE` | existing A-share offset authority | K4不引入 OPEN/CLOSE |
| `OrderType.LIMIT` | existing `SUBMIT_LIMIT` | 其它 order type unsupported |
| `AlgoStatus.RUNNING/PAUSED/STOPPED/FINISHED` | existing algo lifecycle projection | 不新增产品状态；K2 terminal truth优先 |

`NormalizedOrderStatusV1 -> vn.py Status` exact table为 `ACCEPTED->NOTTRADED,PARTIALLY_FILLED->PARTTRADED,FILLED->ALLTRADED,CANCELLED->CANCELLED,REJECTED->REJECTED`；`is_active()`只在`NOTTRADED/PARTTRADED`为true。K2没有strict ORDER callback时不得合成`SUBMITTING`。`ExecutionAlgoPersistenceStatusV2`不反向覆盖upstream object status：adapter先保存pinned AlgoStatus，materializer再依据existing `terminal_outcome`映射持久化status；FAILED由existing failure transition拥有，不能伪造一个upstream status继续callback。

`VnpyFacadeDtoMappingV1` exact fields为 `schema_version,object_name,field_name,source_projection_type,source_field_path,conversion_rule,missing_disposition,allowed_enum_mapping,mapping_sha256`；按 `(object_name,field_name)` 排序且唯一。`mapping_sha256 = hash_hex_v1("miniqmt_vnpy_facade_dto_mapping_v1", exact preceding fields)`。

`dto_mapping_set_sha256 = hash_hex_v1("miniqmt_vnpy_facade_dto_mapping_set_v1", ordered full mapping payloads)`。

## 8. Event-to-Callback Routing / 事件到回调

| K2 event | façade/pinned callback | required source |
| --- | --- | --- |
| `TICK` | `update_tick(TickData)` | exact current MarketDataView + B0 lineage |
| `TIMER` | `update_timer()` | committed K2 timer occurrence；无 DTO |
| `ORDER` | `update_order(OrderData)` | strict callback mapping update + ORDER payload |
| `TRADE` | `update_trade(TradeData)` | strict callback mapping update + TRADE payload |
| `SESSION/EOD/COMMAND_OUTCOME/OMS_RECONCILE/ALGO_START/RUNTIME_STOPPED` | no invented upstream callback | 由 existing plugin lifecycle处理；若 manifest 要求未声明 callback则显式 no-callback receipt，不调用 no-op method |

`no-callback` 是 routing fact，不是静默丢事件：transition receipt 必须保留 event/delivery identity，并由 existing K2/K3 lifecycle产生对应 state/diagnostic/terminal effect。K4 不把非 upstream callback 事件强塞进 `update_timer`。

## 9. Effect and Identity Closure / Effect 与身份闭包

K4 不定义新 broker command identity。所有 submit/cancel 必须通过 existing `BrokerCommandV2.create()`：

```text
local_vt_orderid = existing miniqmt_local_order_identity_v1 formula
command_id = existing miniqmt_broker_command_identity_v2 formula
child_order_id = existing miniqmt_kernel_child_order_identity_v1 formula
mapping_id = existing miniqmt_command_child_mapping_identity_v1 formula
```

façade call ordinal等于 `AlgoTransitionV1` effect ordinal。effect freeze 后，K2 materializer 继续原子创建 transition、command、mapping、child、outbox；callback/reconcile 继续通过 exact mapping 更新。K4 不持有 broker id、不返回 broker ACK、不重写 quantity/price/status。

完整可重建链保持：

```text
runtime event -> delivery -> transition -> façade call ordinal
-> BrokerCommandV2 -> mapping -> child -> outbox attempt
-> broker/callback/reconcile receipt -> later event/delivery/transition
```

## 10. Durable State Adapter and Projection Observation / Durable 状态适配与参数变量观测

### 10.1 Algorithm/state mapping authority

`VnpyFacadeStateFieldMappingV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_state_field_mapping_v1
algo_code, source_identity_sha256
attribute_name, state_path, field_role=BASE|PARAMETER|VARIABLE|ACTIVE_ORDER
value_type, nullable, mutable_container_disposition
constructor_disposition=INITIALIZE_ONLY|RESTORE_FROM_STATE
mapping_sha256
```

writer从pinned algorithm/template AST的constructor assignments、`default_setting`、`variables`和base fields构造；按 `(algo_code,field_role,attribute_name,state_path)` 排序且唯一。`mapping_sha256` domain为 `miniqmt_vnpy_facade_state_field_mapping_v1`；set domain为 `miniqmt_vnpy_facade_state_mapping_set_v1`。missing/extra/duplicate attribute、类型/nullability drift、未声明mutable container或source assignment drift均使characterization/conformance FAILED。

`VnpyFacadeTerminalMappingV1` exact fields为 `schema_version,algo_code,algo_status_member,trigger_event_type,traded_relation=FULL|RESIDUAL|ANY,required_active_child_closure,terminal_outcome_or_none,reason_code,mapping_sha256`；按 `(algo_code,algo_status_member,trigger_event_type,traded_relation,required_active_child_closure)` 排序且唯一，set domain为`miniqmt_vnpy_facade_terminal_mapping_set_v1`。它必须同时满足pinned helper behavior和existing K2 terminal/active-child authority，不能由adapter临时推断。

`VnpyFacadeAlgorithmBindingV1` exact fields为 `schema_version,algo_code,source_identity_sha256,class_ref,constructor_signature_sha256,constructor_body_sha256,state_mapping_set_sha256,terminal_mapping_set_sha256,characterization_receipt_sha256,adapter_contract_sha256,binding_sha256`；constructor hashes从exact algorithm source AST重算，binding domain为 `miniqmt_vnpy_facade_algorithm_binding_v1`。V1仅为K4-A structural evidence且不能进入positive conformance；K4-B以§11定义的V2 binding额外绑定source execution authority。K4为五个算法生成binding evidence；current-three只用于shadow conformance，Iceberg/Stop为`CHARACTERIZATION_ONLY_K5`，均不进入现有catalog。K5未来process factory返回的adapter必须携带byte-identical V2 binding；manifest继续使用existing top-level schema并通过`compatibility_requirement/behavior_characterization_sha256/state_schema/implementation_ref`与该binding闭合，不能新增字段或自行发明另一套restore规则。

### 10.2 Durable state envelope and lifecycle

façade-backed plugin的manifest state schema必须包含exact `VnpyFacadeStateEnvelopeV1`：

```text
schema_version=miniqmt_vnpy_facade_state_envelope_v1
runtime_id, algo_instance_id
plugin_id, plugin_version, plugin_manifest_sha256
algorithm_binding_sha256
algo_name, symbol
direction_member, offset_member, limit_price_decimal, target_volume_decimal
status_member, traded_volume_decimal, traded_price_decimal
contract_view: VnpyFacadeContractViewV1
ordered_active_orders
ordered_parameters
ordered_variables
state_mapping_set_sha256
state_envelope_sha256
```

active-order entries由exact durable mapping + callback authority构造，包含 local/broker/command/child identity、symbol/side、price、requested/cumulative/remaining quantity、K2 `CurrentThreeActiveOrderStatusV3`、pending command、last order/trade/command-outcome/reconcile、terminal ORDER facts和exact native market-data lineage，按 `local_vt_orderid` 排序且唯一。market-data lineage exact fields为`market_data_id,event_id,payload_sha256,generation,sequence,exchange_time_utc,session_phase`；只能从同一 immutable `MARKET_DATA` projection ref及byte-identical B0 TICK payload重建，`COMMAND_PENDING`不得保存空dict、合成generation或普通quote fallback。parameter/variable entries使用§10.3 bounded immutable codec并与state mapping exact key set相等。`state_envelope_sha256 = hash_hex_v1("miniqmt_vnpy_facade_state_envelope_v1", exact preceding fields)`。

`algo_name`固定等于deterministic `algo_instance_id`，`vt_symbol`固定等于normalized A-share `symbol`；禁止使用upstream process counter、wall clock或随机suffix。direction/offset/price/volume从frozen parent/algo facts映射，target/traded/active-order quantities必须为exact integral shares并与K2 algo row闭合。

生命周期固定：

1. **INITIALIZE**：执行pinned constructor一次、`start()`一次，收集constructor/start effects，抽取完整state envelope，按manifest state schema验证后生成sequence=1的`AlgoStateSnapshotV2`；
2. **RESTORE**：每次delivery使用exact class `__new__`，设置transition-local façade，再按state mapping逐字段hydrate base/parameter/variable/active-order；`COMMAND_PENDING|OUTCOME_UNKNOWN|TERMINAL_TRADE_PENDING`不恢复成upstream `active_orders`，避免pre-ACK或已收到terminal ORDER的child被伪装为upstream broker-active order；禁止重跑constructor/start、pickle、copy process object或默认补字段；
3. **CALLBACK**：TICK/TIMER/ORDER/TRADE只执行§8映射的一个 `update_*`，ORDER/TRADE先经existing strict callback payload reader验证；COMMAND_OUTCOME/RECONCILE/SESSION/EOD不伪造upstream callback，由同一active-order lifecycle authority推进pending/outcome/terminal-trade-pending/reconcile closure并产生显式diagnostic；callback前后object identity和owner必须不变；
4. **EXTRACT**：逐字段读取exact mapping，拒绝unknown instance attribute（仅Python runtime内部声明的固定allowlist除外）、callable/mutable alias/NaN/Inf/owner drift；active order必须与post-callback mapping closure相等。terminal ORDER observed cumulative领先TRADE时保存`TERMINAL_TRADE_PENDING`，只在exact TRADE追平或authoritative RECONCILE闭合后删除；callback与mapping各自保留其原生identity namespace，禁止互相覆盖；
5. **FREEZE**：生成下一`AlgoStateSnapshotV2`并调用§5.6 collector freeze；status与terminal outcome、traded/remaining、active child closure必须满足existing K2 state/transition authority。

该state envelope是唯一durable algorithm truth。Python algorithm/façade/DTO object不跨delivery保存；retry/restart必须由相同snapshot重建byte-identical after-state/effect。

upstream `FINISHED/STOPPED` 是algorithm object state，不自动等于K2 terminal。若active child closure为`CANCEL_PENDING|OUTCOME_UNKNOWN`或仍有active mapping，adapter保存该status、生成所需owned cancel/diagnostic但`terminal_outcome=None`；后续只处理ORDER/TRADE/COMMAND_OUTCOME/OMS_RECONCILE/SESSION/EOD closure，不再处理TICK/TIMER业务动作。只有mapping/child/outbox closure为CLEAN且§10.1 terminal mapping命中时才返回FILLED/CANCELLED/EXPIRED_WITH_RESIDUAL；FAILED继续由existing failure transition拥有。callback乱序不能提前terminal或重复cancel。

### 10.3 Projection observation

`VnpyAlgoProjectionObservationV1` exact fields：

```text
schema_version=miniqmt_vnpy_algo_projection_observation_v1
runtime_id, algo_instance_id, event_id, delivery_id, transition_sequence
plugin_id, plugin_version, plugin_manifest_sha256
algo_status
ordered_parameters
ordered_variables
observation_sha256
```

parameter/variable entries exact fields为 `name,value,value_type,value_sha256`，按 name 排序且唯一；value 使用 K1 bounded JSON-safe immutable codec，最多 64 entries、最大深度8、单字符串2048。超限或 unsupported type fail loud，不截断成成功。`observation_sha256 = hash_hex_v1("miniqmt_vnpy_algo_projection_observation_v1", exact preceding fields)`。

enum value不得依赖Python `str()`；固定编码为 `{enum_owner,member,pinned_value_expression}` 并由§7 selected mapping验证。Decimal/float交易数值先验证finite，再使用existing canonical decimal string；datetime使用canonical UTC。未列入state/DTO mapping的object、enum、datetime或container直接失败。

该 observation 必须由同一transition已抽取的§10.2 envelope派生，parameter/variable/status与envelope逐字段相等；它进入 existing `DiagnosticObservationV1` payload，不是第二份 durable algo state，不得参与交易决策或 UI 成功判定。`put_algo_event(algo,data)` supplied data若与adapter重建的exact projection不等则typed fail，不能把caller data反写state。

## 11. Façade Conformance Receipt / Façade 符合性回执

### 11.1 Failure carrier

`VnpyFacadeConformanceFailureV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_conformance_failure_v1
field_path, reason_code, context, context_sha256
```

context 复用 K1 bounded JSON-safe immutable codec。failure identity/sort key为 `(field_path,reason_code,context_sha256)`。最多保留255个真实failure；超限时第256项必须是唯一 marker：

```text
field_path=__failure_set__
reason_code=MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED
context={omitted_count,omitted_failure_set_sha256}
```

omitted hash domain为 `miniqmt_vnpy_facade_omitted_failure_set_v1`。禁止 first-error shortcut、空 FAILED、异常 renderer 覆盖 primary failure。

### 11.2 Per-plugin receipt

`VnpyFacadeConformanceReceiptV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_conformance_receipt_v1
plugin_id, plugin_version, algo_code, manifest_sha256
runtime_binding_disposition=PURE_PLUGIN_SHADOW_CONFORMANCE|FACADE_BACKED_ADAPTER
command_authority_disposition=NOT_APPLICABLE_PURE_PLUGIN|SHADOW_ONLY_K2_V1
pinned_compatibility_receipt_sha256
requirement_sha256, surface_sha256, source_lock_sha256
method_signature_sha256, object_field_sha256, characterization_sha256
facade_contract_sha256, implementation_binding_set_sha256
method_contract_set_sha256, dto_mapping_set_sha256, state_mapping_set_sha256, terminal_mapping_set_sha256
isolated_module_binding_set_sha256
facade_source_manifest_sha256, algorithm_characterization_receipt_sha256, algorithm_binding_sha256
status=PASSED|FAILED
ordered_failures
receipt_sha256
```

所有 K1 component 必须逐字段等于 exact supplied `VnpyCompatibilityReceiptV2`；manifest/plugin identity必须等于 existing `PluginCatalogSnapshotV1` descriptor。全部K4 implementation/method/DTO/state/terminal/isolated-module contract component必须逐字段等于§4.1 `VnpyFacadeContractV1`，`facade_source_manifest_sha256`必须同时闭合五算法和pinned `round_to` helper，algorithm characterization和§10.1 algorithm binding必须引用同algo source/state/terminal mapping/adapter contract且binding内characterization hash等于receipt字段。`runtime_binding_disposition`必须由existing sealed process factory使用characterization receipt中的canonical config真实构造一次broker-neutral plugin后，从exact class identity + adapter binding重建；factory exception、config drift或其它type均FAILED，不能用`hasattr`/caller flag分类。current-three现有pure factory固定为`PURE_PLUGIN_SHADOW_CONFORMANCE + NOT_APPLICABLE_PURE_PLUGIN`；只有K5 future factory真实返回exact adapter时才能为`FACADE_BACKED_ADAPTER + SHADOW_ONLY_K2_V1`。schema和authority validator必须拒绝任何其它command disposition；K6不能通过hash-correct改写把V1升级为产品事实。`receipt_sha256 = hash_hex_v1("miniqmt_vnpy_facade_conformance_receipt_v1", exact preceding fields)`。

PASSED 必须无 failure；FAILED 必须至少一项 failure/marker。writer/readback从exact catalog descriptor、K1 receipt、live K4 implementation binding、K4 method/state/DTO contract、source manifest和characterization receipt独立重建，不接受caller-supplied status/failure/hash。实现source或callable发生漂移时必须FAILED；只更新expected hash后重建PASSED属于无效自证。

### 11.3 Conformance set

`VnpyFacadeConformanceSetV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_conformance_set_v1
plugin_catalog_sha256
facade_contract_sha256
dto_mapping_set_sha256
state_mapping_set_sha256, terminal_mapping_set_sha256
isolated_module_binding_set_sha256
facade_source_manifest_sha256
ordered_receipts
build_input_sha256
receipt_set_sha256
```

每个`VnpyFacadeConformanceBuildItemV1` exact fields为 `schema_version,plugin_key,registration_descriptor_full_payload,pinned_compatibility_receipt_sha256,algorithm_characterization_receipt_sha256,algorithm_binding_sha256,runtime_binding_disposition,command_authority_disposition,build_item_sha256`，按plugin key排序且唯一；item domain为`miniqmt_vnpy_facade_conformance_build_item_v1`。`build_input_sha256 = hash_hex_v1("miniqmt_vnpy_facade_conformance_build_input_v1", {plugin_catalog_sha256,facade_contract_sha256,dto_mapping_set_sha256,state_mapping_set_sha256,terminal_mapping_set_sha256,isolated_module_binding_set_sha256,facade_source_manifest_sha256,ordered full build item payloads})`；`receipt_set_sha256 = hash_hex_v1("miniqmt_vnpy_facade_conformance_set_v1", exact preceding fields)`。任何字段都不得省略或用count代替full identities。

V1 structural set仍要求对existing catalog中每个`required_facade_methods`非空的registered descriptor恰好一个receipt并按plugin key排序，但K4-A不能发布该set。current-three K4 completion改由下述V2 set要求三项均PASSED；missing/extra/duplicate/key drift全部失败且不发布partial set。Iceberg/Stop未注册，因此不进入任一set。

该 set 是 existing catalog 的 derived conformance view，不是第二 plugin catalog：不能增加/删除/重排 descriptor，不能决定 route，不持久化 DB，也不能作为当前产品 runtime 的人工 run gate。K4/K5 shadow façade-backed seam只能显式消费exact PASS + `FACADE_BACKED_ADAPTER + SHADOW_ONLY_K2_V1` receipt；current-three `PURE_PLUGIN_SHADOW_CONFORMANCE + NOT_APPLICABLE_PURE_PLUGIN`不得进入seam，当前K3 binding不改变。产品root必须等待K6独立F2定义并生成新的versioned product command-authority receipt；所有K4/K5 V1 receipt均必须被product activation seam拒绝，不能只改hash或沿用shadow status。

K4-B positive publication不复用上述V1 set。它固定增加 `VnpyFacadeAlgorithmBindingV2`、`VnpyFacadeConformanceReceiptV2`、`VnpyFacadeConformanceBuildItemV2` 与 `VnpyFacadeConformanceSetV2`：各自以对应 `..._v2` schema/domain覆盖V1全部业务身份，并额外绑定 `source_executor_binding_sha256/source_execution_set_sha256/algorithm_characterization_receipt_v2_sha256`。V2 algorithm binding的class/constructor/state/terminal/adapter fields与V1逐字段一致，但`characterization_receipt_sha256`必须指向exact PASSED V2 receipt；禁止V1/V2 hash alias。V2 conformance receipt的K1、catalog、manifest、K4 live binding、source manifest、V2 characterization和algorithm binding必须由同一builder现场重建；V2 set仍只包含existing catalog中三个current-three descriptors，Iceberg/Stop不进入set。

K4-B writer/readback唯一入口固定为：

```python
build_vnpy_facade_algorithm_bindings_v2(
    *, characterization_authority_v2,
    facade_contract, source_manifest
) -> tuple[VnpyFacadeAlgorithmBindingV2, ...]

build_vnpy_facade_conformance_set_v2(
    *, catalog_runtime, gateway_catalog, facade_contract, source_manifest,
    characterization_authority_v2, algorithm_bindings_v2
) -> VnpyFacadeConformanceSetV2

validate_vnpy_facade_conformance_set_against_authority_v2(
    *, conformance_set, catalog_runtime, gateway_catalog,
    facade_contract, source_manifest, requirements, ordered_vectors,
    expected_trace_authorities
) -> VnpyFacadeConformanceSetV2
```

最后一个入口必须重新执行§12 V2 source authority，再重建receipt/binding/build-item/set并做完整payload equality；只重算supplied hash、只验count/keys或信任此前PASSED均不合格。任一算法失败时 `partial_set=None`，不得保留previous V1/V2 set。current-three的`runtime_binding_disposition`仍固定为`PURE_PLUGIN_SHADOW_CONFORMANCE`，所以V2 set本身不会把它们切到façade adapter；K5未来注册的adapter由K5自己的catalog/conformance rebuild产生，不在K4-B伪造。

## 12. Source-Compatible Characterization / 源码兼容性表征

### 12.1 Characterization receipt

`VnpyFacadeAlgorithmCharacterizationReceiptV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_algorithm_characterization_receipt_v1
algo_code, source_identity_sha256, facade_source_manifest_sha256
characterization_requirement_sha256
canonical_factory_probe_config, factory_probe_config_sha256
facade_contract_sha256, implementation_binding_set_sha256
dto_mapping_set_sha256, state_mapping_set_sha256, terminal_mapping_set_sha256
isolated_module_binding_set_sha256
ordered_vector_ids, vector_set_sha256
status=PASSED|FAILED
ordered_failures
receipt_sha256
```

`factory_probe_config_sha256 = hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", canonical_factory_probe_config)`；该config固定等于该algo canonical vector顺序中的第一项config并通过exact manifest/requirement schema；Iceberg/Stop允许full vector set覆盖多个合法config，但不得把多config压缩成一个默认值或遗漏其余vector，完整config集合由`vector_set_sha256 = hash_hex_v1("miniqmt_vnpy_facade_characterization_vector_set_v1", ordered full vector payloads)`绑定。receipt domain为 `miniqmt_vnpy_facade_algorithm_characterization_receipt_v1`。

K4-A只提供严格 V1 observation comparison writer：`executed_vector_results` 是非权威观察输入，即使逐字段等于 expected，也必须生成 `FAILED + MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE`；它不能生成 algorithm binding 或 conformance set。K4-B必须通过独立V2 writer在内部执行§12.1 exact pinned-source harness，并由V2 readback重新执行同一authority后，才允许生成PASSED V2 receipt、V2 algorithm binding和V2 conformance set。禁止把caller mapping、test fixture、hash-correct carrier或expected→actual回填升级为source execution authority。

上述 V1 carrier 的语义在 K4-B 后也不改变。K4-B 不得给 `build_vnpy_facade_characterization_receipt_v1(...,executed_vector_results=...)` 增加任何 PASSED 分支，也不得原地改变 `VnpyFacadeAlgorithmCharacterizationReceiptV1`、`VnpyFacadeConformanceReceiptV1/SetV1` 或 `VnpyFacadeAuthorityInputV1` 的 hash domain。V1 永久表示 K4-A observation/fail-closed 证据；K4-B source-authoritative 路径固定使用下列 V2/V1-versioned 新 carriers，V1/V2 不做自动转换：

```text
VnpyFacadeSourceExecutorBindingV1
  schema_version=miniqmt_vnpy_facade_source_executor_binding_v1
  executor_ref, executor_signature_sha256, executor_source_sha256
  facade_source_manifest_sha256, facade_contract_sha256
  implementation_binding_set_sha256, isolated_module_binding_set_sha256
  dto_mapping_set_sha256, state_mapping_set_sha256, terminal_mapping_set_sha256
  supported_algo_codes, binding_sha256

VnpyFacadeCharacterizationVectorV2
  schema_version=miniqmt_vnpy_facade_characterization_vector_v2
  vector_id, scenario_id, step_ordinal, predecessor_vector_id_or_INIT
  algo_code, side, invocation_phase=INITIALIZE|TRANSITION
  canonical_config, deterministic_context
  start_context_or_null, runtime_event_or_null, read_only_services_or_null
  before_state_or_null, ordered_active_mappings
  explicit_deterministic_inputs
  expected_trace_authority_ref
  expected_ordered_facade_calls, expected_ordered_effects
  expected_after_state, expected_terminal_outcome
  vector_sha256

VnpyFacadeExecutedVectorResultV1
  schema_version=miniqmt_vnpy_facade_executed_vector_result_v1
  vector_id, vector_sha256, scenario_id, step_ordinal
  source_executor_binding_sha256, source_identity_sha256
  invocation_status=COMPLETED|FAILED
  actual_ordered_facade_calls, actual_ordered_effects
  actual_after_state_or_null, actual_terminal_outcome
  consumed_deterministic_inputs, ordered_execution_failures
  result_sha256

VnpyFacadeSourceExecutionSetV1
  schema_version=miniqmt_vnpy_facade_source_execution_set_v1
  algo_code, characterization_requirement_sha256
  source_executor_binding_sha256, facade_source_manifest_sha256
  facade_contract_sha256, vector_set_sha256
  ordered_results, ordered_failures, status=PASSED|FAILED
  execution_set_sha256

VnpyFacadeAlgorithmCharacterizationReceiptV2
  schema_version=miniqmt_vnpy_facade_algorithm_characterization_receipt_v2
  <V1 identity/config/contract/mapping/source fields except status/failures/hash>
  source_executor_binding_sha256, source_execution_set_sha256
  ordered_vector_ids, vector_set_sha256
  status=PASSED|FAILED, ordered_failures, receipt_sha256
```

上述`expected/actual_ordered_facade_calls`与`expected/actual_ordered_effects`不是开放dict。`VnpyFacadeTraceCallV1` exact fields为`schema_version,ordinal,method_name,normalized_arguments,return_disposition=VALUE|NONE|EMPTY_STRING|RAISED,normalized_return_or_null,ordered_diagnostic_reason_codes,call_sha256`；ordinal从0连续，method_name只允许§6六方法与§6.7 exact template helper/callback，arguments/return必须按§4 method contract的structured parameter/return schema逐字段验证。`VnpyFacadeTraceEffectV1` exact fields为`schema_version,ordinal,effect_kind=BROKER_COMMAND|TIMER_MUTATION|DIAGNOSTIC,carrier_schema_version,carrier_full_payload,carrier_identity,carrier_sha256,effect_sha256`；full payload必须strict readback为existing `BrokerCommandV2/TimerMutationV1/DiagnosticObservationV1`，ordinal与collector ordinal相等。call/effect domains分别为`miniqmt_vnpy_facade_trace_call_v1`与`miniqmt_vnpy_facade_trace_effect_v1`。unknown key/type、仅给hash不带carrier、carrier hash-correct但identity/owner漂移或用diagnostic替代command均失败。expected/actual after state必须是full strict `VnpyFacadeStateEnvelopeV1`，terminal必须是existing `TerminalOutcomeV1 | None`；禁止bool equality、count-only或JSON shape-only comparison。

`VnpyFacadeSourceExecutorBindingV1.binding_sha256`、V2 vector、executed result、execution set与V2 receipt分别使用 domains `miniqmt_vnpy_facade_source_executor_binding_v1`、`miniqmt_vnpy_facade_characterization_vector_v2`、`miniqmt_vnpy_facade_executed_vector_result_v1`、`miniqmt_vnpy_facade_source_execution_set_v1` 和 `miniqmt_vnpy_facade_algorithm_characterization_receipt_v2`，均覆盖除自身 hash 外的 exact preceding full canonical payload。`supported_algo_codes`固定为排序且唯一的五算法集合；executor binding writer/readback必须从 live callable ref/signature、canonical-LF implementation source、K4-A live implementation binding和exact source manifest重建，不能接受caller-supplied binding hash。

V2 vector 是可执行输入而不是仅有 hash 的说明：INITIALIZE 必须携带 exact `VnpyFacadeCharacterizationStartContextV2` 且 transition-only fields 为 null；TRANSITION 必须携带 exact `RuntimeEventEnvelopeV2/AlgoReadOnlyServicesV1/VnpyFacadeStateEnvelopeV1/ordered_active_mappings` 且 start context 为 null。`scenario_id + step_ordinal`从0连续；首项 predecessor=`INIT`，后续项必须引用同scenario前一项vector id，并要求 `before_state == predecessor actual_after_state == expected predecessor after_state`。跨scenario、跳步、hash-only state、缺full event/projection、从actual输出反写expected均直接 FAILED。expected trace authority ref exact fields为 `authority_kind=K3_COMMITTED_PARITY|K4_PINNED_CHARACTERIZATION,authority_identity_sha256,source_snapshot_sha256_or_null,parity_input_sha256_or_null,parity_receipt_sha256_or_null,ref_sha256`：current-three 三项必须引用同一次 strict K3 source snapshot/input/PASSED receipt；Iceberg/Stop固定为K4 code-owned pinned vector artifact且K3字段为null。该ref只证明expected来源，不能替代source execution。

`VnpyFacadeCharacterizationManifestViewV1` exact fields为`schema_version,algo_code,registration_disposition,real_plugin_key_or_null,real_manifest_sha256_or_null,required_facade_methods,required_object_fields,required_enum_members,order_types,market_data_capabilities,state_schema_sha256,characterization_requirement_sha256,view_sha256`；current-three逐字段派生自real K1 manifest且real identity必填，Iceberg/Stop逐字段派生自§12 requirement且real identity必须null。`VnpyFacadeCharacterizationStartContextV2` exact fields为`schema_version,vector_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,symbol,side,limit_price_decimal,parent_quantity,contract_projection,deterministic_context,manifest_view,canonical_config,context_sha256`，所有identity由vector/scenario deterministic hash生成，不用UUID。两者分别使用`miniqmt_vnpy_facade_characterization_manifest_view_v1`和`miniqmt_vnpy_facade_characterization_start_context_v2` domain。

Iceberg/Stop source execution不得为满足现有`AlgoStartContextV1/AlgoStateSnapshotV2`类型而创建假的`ExecutionAlgoPluginManifestV2`。K4-B新增source-only `_create_characterization_v2(manifest_view,characterization_context,trace_collector)`，复用同一`VnpyAlgoEngineFacadeV1`六方法、DTO projection、round_to和existing effect constructors，但只freeze为`VnpyFacadeSourceStateEnvelopeV1`与trace carrier，不创建durable `VnpyFacadeStateEnvelopeV1`、`AlgoInitializationV1/AlgoTransitionV1`、catalog descriptor、plugin factory或durable state；source envelope只绑定algo/source/manifest-view identity，禁止伪造plugin/binding/route identity。`VnpyFacadeTraceCollectorV2`是process-local single-use collector，只持有deterministic context、parent/vector identity、next ordinal、ordered strict call/effect和frozen flag；append仍调用existing command/diagnostic constructors，`freeze_result_v1`只能生成一项`VnpyFacadeExecutedVectorResultV1`，freeze后调用typed fail。current-three也走同一source executor view；只有K2 optional invocation才使用real manifest与V2 runtime inputs。该source-only seam按exact class/type分派，不能duck-type到产品SPI或形成第二algorithm implementation。

`VnpyFacadeExecutedVectorResultV1`完整保存actual call/effect/state/terminal与已消费deterministic input，而非只保存相等布尔值。任何source exception也必须生成一项`FAILED` result并保留已发生的partial ordered trace及bounded JSON-safe exception evidence；exception renderer失败不得覆盖primary failure。execution set按 `(scenario_id,step_ordinal,vector_id)`稳定排序且每个required vector恰好一项result；missing/extra/duplicate、FAILED result、actual/expected任一逐字段差异、draw少用/多用或scenario predecessor drift均使set FAILED。PASSED set必须无failure、所有result为COMPLETED并逐字段等于expected。failure仍复用§11.1的255+marker规则；不得 first-error shortcut。

K4-B public authority固定为：

```python
run_vnpy_facade_source_execution_sets_v1(
    *, source_manifest, facade_contract, requirements,
    ordered_vectors, expected_trace_authorities
) -> VnpyFacadeCharacterizationAuthorityV2

build_vnpy_facade_characterization_authority_v2(
    *, source_manifest, facade_contract, requirements,
    ordered_vectors, expected_trace_authorities
) -> VnpyFacadeCharacterizationAuthorityV2

validate_vnpy_facade_characterization_authority_v2(
    *, receipts, source_manifest, facade_contract, requirements,
    ordered_vectors, expected_trace_authorities
) -> VnpyFacadeCharacterizationAuthorityV2
```

`VnpyFacadeCharacterizationAuthorityV2`是sealed process-local object，exact持有source executor binding、五个source execution sets、五个V2 receipts与一个authority hash；只能由上列services runner/build/readback创建，不提供mapping/model_validate公共构造，也不跨process持久化。三个入口都不接受 `executed_vector_results/status/failures/receipt_sha256`。writer通过services runner创建fresh spawned interpreter执行repo-owned pinned algorithm/template/base methods；readback必须在新的fresh interpreter重新执行全部五算法并与supplied receipt逐字段比较。worker输入只允许repo-relative file identity与canonical JSON，使用one-way `multiprocessing.Pipe`先读carrier再join，单worker timeout=120秒、carrier上限8 MiB；禁止Queue join/read deadlock、absolute worktree path、installed/latest package、network、wall clock、PID、UUID、global random或previous receipt。spawn/timeout/malformed carrier/worker exit均产生typed aggregate FAILED和zero binding/conformance publication，不能切换到in-process helper或expected→actual fallback。source executor binding同时绑定semantic artifact hash和canonical-LF file SHA-256。该隔离用于确定性和副作用所有权，不新增网络安全产品、RBAC或人工门禁。

`VnpyFacadeCharacterizationRequirementV1` exact fields为 `schema_version,algo_code,registration_disposition,source_identity_sha256,config_schema_version,config_schema,config_schema_sha256,config_validation_contract_sha256,ordered_required_methods,ordered_required_object_fields,ordered_required_enum_members,ordered_event_types,ordered_market_data_capabilities,state_mapping_set_sha256,requirement_sha256`，config schema使用existing strict local-only JSON Schema authority，额外跨字段/rounding规则以`miniqmt_vnpy_facade_config_validation_contract_v1` hash固定，requirement hash domain为 `miniqmt_vnpy_facade_characterization_requirement_v1`。current-three requirement必须与existing manifest/K1 receipt/config schema及live config-validator behavior逐字段相等；Iceberg/Stop requirement由exact algorithm/template AST访问集和pinned `default_setting`生成并固定为`CHARACTERIZATION_ONLY_K5`，其object/enum facts仍从K1 pinned core bytes/LockedSurface验证，不接受手写字段类型、隐式default或installed package。Stop的`last_price/limit_up/limit_down`和Iceberg TIMER L1 fields必须显式进入该requirement。K5未来manifest的config schema、compatibility requirement和behavior characterization以及process config-validator必须逐字段引用/实现该K4 requirement/receipt后才能由K1 generator生成正式compatibility receipt；K4 requirement本身不能注册plugin或替代K1 receipt。

Iceberg characterization config schema固定`additionalProperties=false`并要求显式`display_volume`与`interval`：`display_volume`为finite nonnegative、non-bool number，rounded-zero继续走§6.1 exact empty-return diagnostic；`interval`为strict integer `>=0`、单位exchange-active TIMER occurrences，`0/1`均按pinned source表现为每个occurrence检查，不擅自收紧。Stop schema固定`additionalProperties=false`并要求显式`price_add`为finite signed、non-bool canonical decimal；K4不因方向偏好额外禁止negative值，最终order price仍通过existing positive-price/OMS authority。两者均不使用upstream `default_setting`静默补值；default_setting只用于证明字段集合和source behavior。K5不得放宽/收紧这些schema或改变单位。

每个 `VnpyFacadeCharacterizationVectorV1` exact fields：

```text
schema_version, vector_id, algo_code, side, invocation_phase
canonical_config, before_state_sha256_or_INIT
event_type, event_payload_sha256, projection_set_sha256
authority_input_sha256, source_market_data_event_id
explicit_deterministic_inputs
expected_ordered_facade_calls
expected_ordered_effects
expected_after_state_sha256, expected_terminal_outcome
vector_sha256
```

`explicit_deterministic_inputs`不是开放dict，而是`VnpyFacadeDeterministicInputsV1`：exact fields为`schema_version=miniqmt_vnpy_facade_deterministic_inputs_v1,ordered_uniform_draws,inputs_sha256`。每个`VnpyFacadeUniformDrawV1` exact fields为`schema_version,ordinal,u53_integer,draw_sha256`，ordinal从0连续、`u53_integer`为strict integer且`0<=value<2**53`，draw/set domains分别为`miniqmt_vnpy_facade_uniform_draw_v1`和`miniqmt_vnpy_facade_deterministic_inputs_v1`。没有random调用的vector必须是空tuple；实际调用数少于或多于tuple长度均FAILED，禁止忽略剩余draw或自动补draw。

无 wall clock、PID、absolute path、installed package或网络输入。BestLimit upstream `uniform` 只通过§4.1 exact `characterization.deterministic_uniform` binding消费 `explicit_deterministic_inputs.ordered_uniform_draws`；产品 K3 继续使用 K1 deterministic u53 authority，K4 不恢复 global random。TWAP和engine的`round_to`只通过同一§4.1 `helper.round_to` binding执行，不加载utility module其它代码。

characterization harness必须执行exact repo-owned algorithm/template/base source methods，而不是复制算法逻辑。isolated module graph必须从§4.1 `VnpyFacadeIsolatedModuleBindingV1` exact set构建：`vnpy.trader.object/constant`解析为K4实际DTO/enum projection classes并在启动前由K1 `LockedSurfaceV2`逐字段/member验证；`vnpy.trader.engine.BaseEngine`唯一解析为实际`VnpyAlgoEngineFacadeV1` class，且同module不得提供`MainEngine`或其它owner；`vnpy.trader.utility`只暴露exact `helper.round_to`，`random`只暴露transition-local `deterministic_uniform`；relative template/base解析为repo-owned pinned source module。order/timer/diagnostic使用actual K1/K2 constructors。禁止dummy object、MagicMock、固定return、helper-only visitor、installed package、复用污染的`sys.modules`或手写第二份algorithm branch作为PASSED证据。source-isolated execution不发布factory/catalog，不启动EventEngine/Gateway/repository；expected trace必须来自独立pinned source/K3 committed parity vector，而不是从actual输出回填。

K4不得为此放宽K1对AIstock-owned plugin/module的global-random与dynamic-import禁令：只有source identity/bytes逐项等于§3 exact pinned BestLimit source时，characterization loader才可按§4.1绑定其既有`from random import uniform`到deterministic adapter；任一source drift、其它module请求`random`或K4 production code直接import `random`仍由import boundary拒绝。该例外只属于离线shadow characterization，不是网络安全功能或产品runtime fallback。

### 12.2 Current-three

Sniper/BestLimit/TWAP 必须：

- 从 existing K1 current-three manifest/receipt 和 K3 committed parity vector读取 input；
- 将 pinned source method-call trace映射为 §6 façade calls；
- 将 K3 `CurrentThreeParityTraceV1` 映射为 expected effects/state；
- 同时覆盖INITIALIZE constructor/start与TRANSITION restore/callback/extract/freeze，证明call ordinal、price、quantity、reason、cancel ownership、timer semantics、state和terminal outcome一致；
- 任一 drift产生 FAILED receipt，不能用 transport suppression掩盖 business drift。

不创建或调用第二 current-three plugin factory。

K4-B增加唯一repo-owned vector artifact `backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json`，schema固定为`miniqmt_vnpy_facade_characterization_vector_artifact_v2`，字段为`schema_version,k3_source_commit_sha,k3_contract_binding_sha256,ordered_k3_expected_trace_materials,ordered_vectors,vector_set_sha256,artifact_sha256`；`k3_source_commit_sha`固定指向生成expected trace所使用的已合入K3 source commit，而不是会随merge变化的K4当前HEAD。canonical-LF file hash与semantic artifact hash分别进入source executor binding。它不是expected-only fixture：三个current-three scenario必须由现有K3 production constructors在DEV disposable PostgreSQL的committed-fact path生成六项BUY/SELL `CurrentThreeShadowSourceSnapshotV1/CurrentThreeParityInputV1/CurrentThreeParityReceiptV1` material，并把exact full input与expected kernel trace写入vector artifact；K4-B readback再使用当前code-owned K3 factory、state codec、parity constructors从artifact full input重新运行kernel trace，并强制每个current-three vector的`expected_trace_authority_ref`逐字段等于同algo/side material identity。K3 manifest/factory/process binding、source snapshot、parity input/receipt/ref任一漂移都使current-three characterization FAILED；不得只信artifact hash，也不得读取生产数据库或第二factory。该artifact属于K4 characterization authority而非K1 pinned source lock，避免向K1 exact path set私增文件。

current-three每个algo至少包含一条INITIALIZE scenario和覆盖其全部manifest event subscriptions、BUY/SELL、active-order callback、terminal/retry边界的TRANSITION scenario；TWAP必须覆盖AM→午休零计时→PM恢复、无catch-up burst与EOD residual，BestLimit必须覆盖exact deterministic u53 draw，Sniper必须覆盖price/cancel ownership。transport duplicate observation保持K3既有语义，只能从expected与actual两侧同源消除，不能用来忽略business effect/state差异。

### 12.3 Iceberg/Stop characterization-only boundary

K4 必须使用 §3 repo-owned exact source bytes和 source-isolated harness验证：

- Iceberg需要INITIALIZE constructor/start和`TIMER/ORDER/TRADE`；TIMER `get_tick`必须使用§6.3同session、sequence cutoff前的durable TICK lineage，覆盖missing/stale返回None、visible slice submit及owned cancel；
- Stop 只需要 `TICK/ORDER/TRADE`、`last_price/limit_up/limit_down`、single trigger submit和terminal state；
- 两者必须使用§10 state mapping执行constructor-once、restore-without-constructor、callback-once和after-state extraction；constructor `put_event`、not-running empty return、finish/cancel_all均进入expected call/effect trace；
- source访问的 façade method/DTO/enum 必须全部属于父蓝图已批准 surface；unsupported surface直接 FAILED；
- harness broker_called=false，不创建 K2 algo、delivery、mapping、outbox或 DB row；
- characterization PASSED 只说明 K4 façade surface足够，不表示 K5 plugin已实现、注册、可运行或产品启用。

Iceberg/Stop vectors与current-three共用同一V2 artifact但`expected_trace_authority_ref.authority_kind=K4_PINNED_CHARACTERIZATION`；其expected full inputs/effects由本设计的exact scenario matrix和pinned AST access set生成，readback必须验证vector覆盖全部required method/object/enum/event/config分支，不能由actual trace反向生成。Iceberg至少覆盖：display_volume rounded-zero、interval 0/1/>1、TIMER missing/READY tick、visible slice、owned cancel、ORDER/TRADE cumulative、finish/cancel_all、午休/PM same-phase cutoff和restart。Stop至少覆盖：BUY/SELL trigger/not-trigger、price_add signed、last/limit-up/limit-down边界、single trigger幂等、ORDER/TRADE terminal及restart。每个required branch必须同时有positive与malformed/authority-drift negative；没有完整矩阵不得发布PASSED。

## 13. Error Semantics / 错误语义

固定 stable reason codes：

- `MINIQMT_VNPY_FACADE_SOURCE_INVALID`
- `MINIQMT_VNPY_FACADE_CONTRACT_INVALID`
- `MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID`
- `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED`
- `MINIQMT_VNPY_FACADE_TRANSITION_INPUT_INVALID`
- `MINIQMT_VNPY_FACADE_INITIALIZATION_INPUT_INVALID`
- `MINIQMT_VNPY_FACADE_BINDING_INVALID`
- `MINIQMT_VNPY_FACADE_STATE_INVALID`
- `MINIQMT_VNPY_FACADE_ORDER_INPUT_INVALID`
- `MINIQMT_VNPY_FACADE_ORDER_OWNERSHIP_INVALID`
- `MINIQMT_VNPY_FACADE_ALGO_NOT_RUNNING`
- `MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE`
- `MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE`
- `MINIQMT_VNPY_FACADE_EFFECT_CONFLICT`
- `MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED`
- `MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE`
- `MINIQMT_VNPY_FACADE_V1_AUTHORITY_NOT_EXECUTABLE`
- `MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID`
- `MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED`
- `MINIQMT_VNPY_FACADE_EXECUTION_RESULT_INVALID`
- `MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID`
- `MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID`
- `MINIQMT_VNPY_FACADE_CLAIMED_DELIVERY_INVALID`
- `MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID`
- `MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED`

Typed exception context 至少包含适用的 plugin/runtime/algo/event/delivery/transition/method/field/expected/actual/source/receipt identity。错误 evidence 必须 JSON-safe、repo-relative、稳定排序且有界。不得 `except Exception: pass`、固定 PASSED、空 ACK、空 diagnostic、默认值成功或 error-to-log-only。

runtime invocation中的K4 typed error必须继承existing `KernelPluginInvocationError`（或由唯一无损adapter保留reason/message/context/broker_called/exception chain），使existing invoke/materializer不把它覆盖成generic plugin failure；code-owned conformance/characterization build failure使用独立aggregate error且`partial_set=None`。renderer自身失败不能覆盖primary reason。

V1 positive invocation、V1/V2 receipt混用、caller supplied execution result或未由sealed V2 authority创建的runtime input固定分类为`MINIQMT_VNPY_FACADE_V1_AUTHORITY_NOT_EXECUTABLE|...CONFORMANCE_AUTHORITY_INVALID`，不能误报成static capability unsupported。spawn timeout/exit、malformed child carrier、source exception、actual trace mismatch分别保留source executor/execution result reason与vector/scenario/step identity；外层不得catch-all重写成generic conformance failure。repository zero TICK是正常unavailable diagnostic；ALGO_START缺失、owner/cutoff/carrier冲突、claimed delivery完成字段提前出现则是typed corruption failure，二者不得合并。

missing tick/contract 的 upstream-compatible `None` 和 not-running/missing-contract/rounded-zero order 的 `""` 不是静默失败：必须同时生成各自 exact typed diagnostic；其它 corruption/ownership/capability/state/binding failure 必须抛出 typed error。

## 14. Concurrency, Retry and Restart / 并发、重试与重启

- façade/collector/object projection均为 transition-local；禁止 singleton、thread-local隐藏状态或跨 delivery cache。
- 同 delivery retry从相同 durable input重建，effect/receipt byte-identical；same identity/different payload terminal conflict。
- K2 lease/fence/CAS继续唯一拥有 writer并发；K4不实现锁、retry loop或 DB writer。
- callback-before-ACK、late callback、same-symbol/multi-slot通过 existing mapping/event sequence隔离；façade只消费 exact owner facts。
- process restart只从 durable state envelope、mapping/event、ALGO_START authority、catalog/gateway/receipt重建；adapter通过`__new__`+exact mapping hydrate，不序列化Python algorithm/façade object，也不重跑constructor/start。
- TIMER market-data lookup使用immutable timer event sequence cutoff；later TICK、process cache和retry时当前quote不能改变selected source event。source event缺失或不再strict-readable时fail loud，不选择次新event。
- characterization fresh-process输出必须与本进程 byte-identical；absolute worktree path不得进入 hash。
- failure不回退 previous conformance set、旧 receipt、legacy helper或 current product route。
- source executor按algo_code稳定顺序一次只运行一个fresh spawned worker；每个worker最多120秒、最多接收该algo artifact中的exact required vector数，不接受caller扩容或无限队列。timeout/abnormal exit产生完整typed FAILED并继续收集其余algo的有界failure，最终zero publication；该资源边界不改变任何交易业务状态，也不是人工审批。
- source execution scenario内部严格串行；不同scenario从INIT重新构造，不能共享algorithm/façade/collector/module/global random状态。worker返回后父进程先验证carrier/schema/hash/count，再比较expected；malformed或超限输出在反序列化后、业务比较前fail loud。
- sealed conformance authority在process内immutable；fork/spawn/restart后必须重新执行V2 validator，不能跨process pickle/live-cache复用。K2 transition事务只消费已验证authority和full V2 input，不等待source executor，避免在DB lock内启动subprocess。

## 15. Repository, Migration and Transactions / Repository 与迁移

K4不新增durable DB schema、DDL/DML、repository owner或writer。所有交易事实继续通过K2 existing transaction bundle持久化；§5.4.1无法由V1 projection精确表达的多command transition不得进入该bundle。K4-B只允许在existing K2 repository增加以下两个bounded read-only seam：

```python
read_facade_algo_start_event_v1(
    *, runtime_id: str, algo_instance_id: str
) -> VnpyFacadeRepositoryEventReadV1

read_facade_latest_prior_tick_v1(
    *, runtime_id: str, algo_instance_id: str,
    timer_delivery_sequence: int, timer_event_sequence: int,
    exchange_trade_date: str, session_epoch: str, session_phase: str
) -> VnpyFacadeRepositoryEventReadV1 | None
```

`VnpyFacadeRepositoryEventReadV1` exact fields为`schema_version,read_kind=ALGO_START|LATEST_PRIOR_TICK,runtime_id,algo_instance_id,cutoff_delivery_sequence_or_null,cutoff_event_sequence_or_null,event,delivery,read_sha256`；hash domain为`miniqmt_vnpy_facade_repository_event_read_v1`。`VnpyFacadeRepositoryReadSetV1` exact fields为`schema_version,request_sha256,algo_start_read,latest_prior_tick_read_or_null,read_set_sha256`，绑定本次runtime/algo/current event+delivery双cutoff、trade date/session epoch/phase及两个full read payload。两者均strict immutable，且不得解释为第二份event事实。

ALGO_START seam通过`execution_algo_event_delivery`的`algo_instance_id + algo_delivery_sequence=1`唯一键连接`execution_runtime_event(runtime_id,event_id)`，要求delivery/event均为exact owner、delivery为APPLIED且event type/source/schema为`ALGO_START/MINIQMT_EXECUTION_KERNEL/miniqmt_algo_start_v1`。TIMER seam固定使用：

```sql
SELECT event.payload, delivery.carrier_json
FROM qmt_strategy.execution_algo_event_delivery AS delivery
JOIN qmt_strategy.execution_runtime_event AS event
  ON event.runtime_id=delivery.runtime_id AND event.event_id=delivery.event_id
WHERE delivery.runtime_id=:runtime_id
  AND delivery.algo_instance_id=:algo_instance_id
  AND delivery.status='APPLIED'
  AND delivery.algo_delivery_sequence < :timer_delivery_sequence
  AND event.sequence < :timer_event_sequence
  AND event.event_contract_version='KERNEL_V2'
  AND event.event_type='TICK' AND event.source='B0_QUOTE_V2'
  AND event.payload_schema_version='miniqmt_market_data_view_v2'
ORDER BY delivery.algo_delivery_sequence DESC,event.sequence DESC,event.event_id DESC
LIMIT 1
```

该查询由existing `uq_miniqmt_k2_delivery_sequence`与event owner/index支持，不按JSON symbol全表扫描。选中后必须strict-readback full event/delivery，再验证event symbol、B0 market-data identity/hash、`exchange_trade_date/session_epoch/session_phase`与current timer deterministic context完全一致；latest candidate不合格时返回explicit unavailable且不得向更旧TICK fallback。这里不发明新的wall-clock TTL：freshness只消费B0 payload自身已冻结的eligibility/freshness事实与same phase；缺字段、非READY或phase漂移均unavailable。

真实delivery路径不能通过另开连接在callback期间读取这两个事实。K4-B为existing `apply_claimed_delivery_atomic`增加keyword-only `facade_read_request: VnpyFacadeRepositoryReadRequestV1 | None=None`；repository在同一cursor、同一locked transaction中调用私有`_read_facade_*_with_cursor`并把`VnpyFacadeRepositoryReadSetV1 | None`作为bundle builder最后一个参数。ordinary plugin固定传None且忽略该参数；只有exact façade-backed adapter可传非null request。request owner/cutoff必须等于当前locked event/delivery/algo，否则在callback前typed fail。public两个read方法只用于独立readback/diagnostics/DEV测试，并使用相同pure projection authority；事务commit后以固定cutoff独立重读必须byte-identical。zero/multiple/corrupt ALGO_START typed fail，TICK zero按explicit unavailable，carrier/owner/hash conflict typed fail。不得截断集合、读取process cache、改变lock order或新增未索引全表扫描。

K4 conformance/characterization receipts是 code-owned immutable build artifacts，由 fresh-process可重算；不写生产表，不触发 broker。新增read-only query必须用existing DEV PostgreSQL disposable data证明retry/restart selection稳定、later TICK不改变cutoff、cross-runtime/symbol/session排除且数据库零写入。

如果实现发现必须新增数据库字段、修改 K2/K3 durable schema或变更 product binding，说明本设计假设失效，必须停止并回到父蓝图重新审查，不能在 K4 PR 中顺手扩大 scope。

## 16. Diagnostics, Metrics and Retention / 诊断与保留

K4 不新建 metrics backend、alert、operator acknowledge或审批流。只允许复用 existing K2 diagnostics：

- `miniqmt_vnpy_facade_source_execution_total{algo_code,status,reason_code}`：每个五算法V2 build attempt一次；
- `miniqmt_vnpy_facade_conformance_build_total{status,reason_code}`：complete set build/rebuild一次；
- `miniqmt_vnpy_facade_runtime_invocation_total{phase,outcome,reason_code}`：只有future façade-backed shadow invocation记录，K4 current product保持零；
- `miniqmt_vnpy_facade_repository_read_total{read_kind,outcome}`：`ALGO_START|LATEST_PRIOR_TICK`与`FOUND|UNAVAILABLE|INVALID`；
- `miniqmt_vnpy_facade_active_failure{stage,reason_code}`：当前process最后一次build/readback是否仍失败，成功full rebuild自动清零；
- read-only diagnostics固定返回source manifest/executor/vector/execution-set/characterization/conformance hashes、五算法status、bounded failures、last failure和runtime invocation count；identity/symbol只在existing受限分页detail中出现。

完整 failures 保留在 immutable receipt；source/vector/conformance code-owned artifacts随Git历史保留，K2 runtime diagnostic observation沿用existing K2 retention，不新增DB retention/prune job。metrics label exact allowlist只有上列枚举，禁止 symbol、order id、runtime/algo/plugin id、event/delivery/transition id、source hash进入label。

K4-B shadow构建失败使对应CI/direct build失败并且conformance set不发布；它不停止current product pure-plugin route。未来K5 shadow root若active failure持续一个完整build周期，复用existing K2 critical diagnostic channel发出`MINIQMT_VNPY_FACADE_CONFORMANCE_ACTIVE_FAILURE`；成功full rebuild自动清除active alert并保留last failure，不需要人工ack。repository read INVALID记录error并终止该delivery；TICK UNAVAILABLE只产生既定diagnostic并等待后续真实event。不得新增邮件/短信、RBAC、operator确认或手工恢复步骤。

## 17. Risks / 风险

| risk | prevention and evidence |
| --- | --- |
| façade 被实现成第二 runtime 或直接 broker adapter | import/static guard 禁止 EventEngine/OMS/Gateway/repository/broker ownership；所有命令只经 existing `BrokerCommandV2` collector |
| K2 pure plugin SPI无法向façade提供完整事实 | K4-B explicit optional invocation input + exact adapter protocol；ordinary plugin/current-three path byte-identical；K4 adapter fixture不冒充registered plugin，K5提供首个真实registered shadow adapter并只消费既有seam |
| K2 V1 projection set无法逐command闭合多command authority | §5.4.1明确K4/K5 shadow边界：collector/characterization保留全部command，existing materializer只接收exact单command shadow authority；禁止复用receipt或丢command；generic aggregate由K6独立F2 cutover关闭 |
| route receipt被当成“已验证”布尔事实 | authority input必须携带catalog+gateway并调用existing authority validator；ALGO_START/row/projection hash闭合 |
| constructor每次restore重放或状态丢失 | INITIALIZE constructor/start once；TRANSITION `__new__`+exact mapping hydrate/extract；unknown field/state schema/retry direct tests |
| TIMER通过process latest quote使Iceberg漂移 | durable TICK sequence cutoff、same session、B0 freshness和consumed lineage；later quote不能改变retry input |
| façade contract hash未绑定实际实现 | method/state/DTO contract + live callable/signature/canonical-LF source binding共同进入hash，writer/readback独立重算 |
| current-three 被切到平行 façade factory | exact K3 factory/class/binding readback与static negative test；K4不得修改creation bindings |
| Iceberg/Stop characterization 被误报为 plugin 完成 | source disposition固定 `CHARACTERIZATION_ONLY_K5`；catalog/creation/K2 row zero-publication直接测试 |
| K1 receipt 与实际 façade implementation自证循环 | K1 receipt、K4 contract/mapping/source/characterization是独立输入，conformance writer/readback逐项重建，不接受caller hash/status |
| DTO missing field被默认值掩盖 | manifest-driven exact field set；先strict readback再构造，missing返回typed None/diagnostic或fail，不先建默认DTO |
| order ownership或callback alias错误导致跨algo影响 | exact active mapping与K2 callback authority；unknown/duplicate/cross-owner terminal fail，zero command |
| retry/restart重复effect | transition-local collector、deterministic ordinal、existing K2 identity/CAS/fence；fresh-process hash parity |
| failure evidence高基数或无界 | bounded immutable failure set与固定低基数metrics；完整identity只留receipt context |
| K4实施顺手修改K5/K6/生产状态 | changed-file review、F-081/F-088/F-090、DESIGN-COMPLIANCE；发现DB/binding/cutover需求立即停止并回到父蓝图 |

## 18. Verification Plan / 验证方案

### 18.1 Direct contract tests

- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_contracts.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_algo_engine_facade.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_projection.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_state_adapter.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_kernel_invocation.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_receipts.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_characterization.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_repository_postgres.py`
- `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py`

覆盖strict schema/extra/type/identity/hash、recursive immutability、source set、method/implementation binding、DTO/enum/state mapping、initialization+six methods、callback routing、ordinal、freeze、bounded failure/truncation、writer/readback、fresh-process parity。

K4-B必须先建立以下真实RED再实现：V1 caller observation即使expected==actual仍不能PASSED；V1 transition要求pre-call delivery.transition_id导致真实CLAIMED路径失败；V1 vector缺full event/projection/before-state不能执行；source executor/result/set/receipt V2不存在；latest-prior-TICK尚无同事务read seam；V1/V2 authority混用可构造structural carrier但不能获得sealed authority。GREEN必须通过public writer/readback/invoke/repository seam，不得只测private comparator、AST visitor或mock object。

### 18.2 Negative matrix

必须直接覆盖：

- K1 receipt FAILED/missing/duplicate/component drift；
- catalog/manifest/plugin key/hash drift；route receipt只有structural-valid但catalog/gateway authority drift；
- initialization/transition input缺actual projection payload、delivery/algo/mapping/lifecycle owner drift；ordinary plugin误收facade input、facade adapter缺input或binding drift；
- 单command shadow authority与command/child/route/OMS/risk/kill owner drift；`cancel_all()`多command被截断、合并、复用receipt、duplicate-type ref或错误送入V1 materializer；
- algorithm/helper source missing/extra/duplicate/traversal/size/hash/decode/AST drift；`round_to` body/name/signature/Decimal-round-int-multiply-return drift；
- unsupported method/field/enum/order type/callback；
- bool/NaN/Inf/fractional quantity、invalid price、unknown/cross-owner cancel；
- missing/stale/synthetic quote、contract/gateway/min_volume/pricetick缺失；
- callback mapping alias/owner/status/cumulative/trade identity conflict；
- effect ordinal gap/duplicate、post-freeze call、same id/different effect；
- constructor在restore/retry被重跑、unknown/missing state field、mutable alias、state schema/hash/terminal drift、empty-return diagnostic缺失；
- TIMER source TICK future/cross-runtime/cross-symbol/cross-session/stale/duplicate/corrupt，later TICK不得改变immutable cutoff选择；
- failure >255、malformed failure context、renderer secondary failure；
- receipt hash-correct但authority-inconsistent readback；
- V1 observation/receipt/set/input被作为V2 executable authority；V2 vector缺full input、scenario ordinal/predecessor drift、actual回填expected、execution set missing/extra/duplicate；
- source executor live ref/signature/source drift、spawn timeout/exit/malformed carrier、partial trace、result/set hash drift、V2 receipt缺execution-set binding；
- 不得接受pre-call CLAIMED delivery携带transition_id/receipt/closed fact、lease owner/epoch/fence/row-version drift、input transition identity与`algo_transition_id_v1`不等或materializer生成不同identity；
- conformance authority绕过sealed builder、V2 set混入V1 receipt、runtime直接注入裸set/hash、process restart复用旧sealed object；
- absolute path、wall clock、global random、UUID、installed/latest/network fallback；
- isolated module binding missing/extra/duplicate、BaseEngine指向stub/installed class、暴露MainEngine、加载完整utility、恢复global random、uniform draw ordinal/bounds漂移或复用预置`sys.modules`；
- Iceberg/Stop被错误加入catalog/creation binding/K2 runtime的static negative test。

### 18.3 Integration and parity

- current-three：reuse K3 committed parity vectors，证明 façade call/effect trace与existing K3 outcome一致；不调用第二 factory。
- current-three vector artifact：DEV committed-fact generator→repo-owned full-input artifact→fresh K3 factory replay→pinned source executor四段逐项闭合；artifact/hash与live K3 trace任一方单独修改都必须FAILED。
- K2 shadow seam：真实 creation/delivery invocation覆盖pure-plugin unchanged path；façade-backed optional path使用§5.4 production-constructor closed candidate且明确标记test-only、不进入catalog。单command使用真实`BrokerCommandV2/AlgoTransitionV1` constructors及exact existing projection authority验证command/mapping/materializer identity；多command完整保留collector trace并明确拒绝V1 materialization；全部`dispatch_attempt=0`、`broker_called=false`，不运行dispatcher。该fixture测试不能单独满足K4 completion。
- DEV PostgreSQL：existing schema中验证ALGO_START/TIMER source TICK bounded read-only query、同cursor/locked transaction carrier、public independent readback、retry/restart cutoff、AM→PM phase排除、latest-invalid不向旧event fallback与zero DB write；对所有相关表比较transaction前后row count和row payload hash，不执行migration。
- source-isolated Iceberg/Stop characterization：真实 repo-owned bytes，zero catalog publication、zero DB/broker effect。
- restart/fresh-process：receipt/source/vector/mapping hash byte-identical。
- standard package import：不自动注册算法、不加载legacy adapter、不启动runtime/EventEngine/OMS/Gateway。
- import owner：`facade_source_execution.py`通过existing algorithm import-boundary且无process/network/file-write owner；services runner普通import不spawn，不被`backend.execution_algos.vnpy_compat` package eager import。

### 18.4 Coverage and routing

- K4核心 `facade_contracts.py/facade_projection.py/facade.py/facade_adapter.py/facade_characterization.py` 各自 line `>=80%`、branch `>=70%`；新增source executor/V2 authority模块同样各自达到该阈值。touched `kernel_creation.py/kernel_delivery.py/kernel_repository_event_delivery.py/kernel_repository_k2b.py` optional invocation/read-only query分支必须有直接正反测试。
- changed files必须全部经 `file_ownership.yaml -> module_registry.yaml -> test_plans.yaml` 映射，`unmapped_code_files=[]`。
- 预计只选择 `miniqmt_execution_runtime_l2` 和因 `backend/execution_algos/**` shared ownership而真实依赖的 `paper_v2_backend`；无 shared contract diff时不得额外运行其它模块。
- F2 validators：本文、父执行内核蓝图、统一模拟盘蓝图全部通过且 warnings=0。

## 19. Implementation Plan / 实施方案

K4 implementation固定最多两个 source PR，不扩大到 K5/K6：

### K4-A — pure façade/adapter, contracts and source authority

- `facade_contracts.py/facade_projection.py/facade.py/facade_adapter.py/facade_characterization.py` 的pure、zero-DB/Gateway实现；
- 五算法+`round_to` helper source manifest、Iceberg/Stop与utility repo-owned source bytes；
- implementation/method/DTO/state mapping contract、algorithm-binding/characterization/conformance strict carriers与hash formula；
- observation-only characterization writer/readback、failure/truncation、fresh-process determinism；raw caller observation始终FAILED，algorithm binding/conformance publication在K4-B exact executor到位前fail loud且zero partial publication；
- actual live callable/signature/source binding positive/negative；不修改K2 creation/delivery/repository，不调用broker。

K4-A implementation receipt（2026-07-30）：

- 五个pure核心文件、K1 delegated-path closure、repo-owned Iceberg/Stop/utility bytes和`facade_source_manifest.json`已实现；source manifest hash=`e0284a6d0e92938626d5a00bd16a325a31ccba35d0158847025af97b0ece51ea`；
- current-three三个K1 requirement的per-plugin characterization hash保持各自独立；K4 shared requirement只绑定三者共同的`source_lock/method_signature/object_field` component和有序plugin requirement hashes，domain=`miniqmt_vnpy_facade_shared_k1_requirement_v1`，shared surface domain=`miniqmt_vnpy_facade_shared_k1_surface_v1`；不得把任一plugin-specific K1 surface冒充共享façade authority；
- review-fix live façade contract hash=`59f9aed06e24aaefb1aebe753ddc493279bcd10cd2b4606bc71ec45af1040444`，implementation/method/DTO/state/terminal/isolated sets分别为`c861d972.../fa718f99.../e4a6ddb1.../9824f5ef.../8a224095.../600aab67...`；writer/readback均重建同一pinned/live authority；
- K4 direct矩阵=`58 passed`；五核心line/branch分别为`92.04/79.09`、`86.67/76.87`、`83.33/71.43`、`92.45/71.08`、`97.45/94.64`；full PR classifier=`targeted_ci_required`、changed files=`21`、ownership=`21/21 mapped`、`unmapped_code_files=[]`，只选择MiniQMT/Paper；MiniQMT=`1047 passed,29 skipped`，Paper=`1050 passed,2 skipped,2 xfailed`；L0 blocking=`0`，module registry=`8 passed`、catalog=`14/14 mapped`；
- K4-A仍是pure/shadow-only。它没有修改K2 creation/delivery/repository、current-three factory/binding或产品route；K4-A observation receipt均显式FAILED且不会发布algorithm binding/conformance set。K4-B exact pinned-source executor、optional invocation、read-only repository seam、current-three parity以及Iceberg/Stop完整characterization已实现并验证，但产品root仍未注入sealed authority；不得把K4 source验证解释为runtime activation。
- source HEAD=`117c96f9945d4ddd96a09d6ebfe741626a67c51f`已通过PR #2883，以普通merge commit `527b2f4a58d3fe84f85c0b1f4ba2fe375d181dda`进入main；source merge与K4-B/产品runtime状态继续分离。

### K4-B — existing K2 optional invocation and shadow integration

- `facade_contracts.py`增加V2 full-input/full-trace/source-execution/characterization/conformance/authority/input strict carriers；V1 observation carriers和hash domain保持不变且永不PASSED；
- 新增pure `facade_source_execution.py`从exact pinned modules执行单worker内五算法scenario；新增services-owned `vnpy_facade_characterization_runner.py`负责fresh process、timeout和malformed carrier；`facade_characterization.py`只编排requirements/vectors/V2 receipts/bindings/conformance，不能复制algorithm branch或拥有process lifecycle；
- 新增唯一`facade_characterization_vectors_v2.json`及strict artifact writer/readback；current-three expected由existing K3 committed-fact production constructors生成并由live K3 factory重放，Iceberg/Stop保持characterization-only；
- `facade_adapter.py/facade.py`增加V2 positive path并从V2 input读取precomputed transition identity；V1 path不被暗中升级；
- `kernel_creation.py/kernel_delivery.py`只增加keyword-only optional sealed authority/input，ordinary pure path逐行语义不变；exact class/binding分派，不按algo code、`hasattr`或配置flag分支；
- `kernel_repository_event_delivery.py/kernel_repository_k2b.py`增加同cursor read request/read set与两个public independent readback；不新增表/列/index/migration，不改变writer、lock order、CAS/fence或transaction ownership；
- 单command仅做broker-neutral existing materializer shadow closure，多command完整保留trace并明确不materialize；所有测试`dispatch_attempt=0/broker_called=false`；
- direct/DEV PostgreSQL/L2/Paper/coverage/F2/DESIGN-COMPLIANCE全部闭合后才能标记K4-B implemented；不修改 current-three binding，不注册Iceberg/Stop，不切产品 route。

K4-B实现状态（2026-07-31）：`implemented_verified_source_pending_user_authorization`。正式审核发现并修复K3 manifest/process binding使用工作树原始EOL导致Windows/Linux identity分裂、异常reason renderer二次失败、无标记message截断/绝对路径残片，以及characterization preflight失败未进入active diagnostics的问题。AIstock-owned source attribution与binding readback现统一canonical-LF；repo-owned artifact仍包含`81`个full-input/full-trace vectors与`6`项K3 BUY/SELL committed-parity material，重新使用当前production K3 constructors闭合，semantic artifact hash=`37dc70e54a4576ceb909d07df7d599ce93737948920d0182b1f4d81572f3b758`、vector-set hash=`4a3117fa3865e0b7f759f61102ea58a426c5d45844d1a821fa5161197f506cf2`、canonical-LF file hash=`ec7bc3c740fab18277c698a80a1b985b7e474d038bc37230c6f8911abbac9396`、live K3 binding hash=`123b3349bfdf79edd739cd355f6667cf98d18f2826b1480fb354c3dad3f1bedb`。本地非DB direct=`202 passed,1 skipped`、DEV PostgreSQL=`2 passed`、MiniQMT=`1099 passed,30 skipped`、Paper=`1050 passed,2 skipped,2 xfailed`；八个K4核心模块line均`>=80%`且branch均`>=70%`，最低分别为`80.06%/70.00%`。PR classifier=`targeted_ci_required`且`unmapped_code_files=[]`；required CI run `30573150209`全绿，source merge与产品runtime状态继续分离。

任一 PR 不得以“后续补齐”为由省略本切片的 writer/readback、negative、restart或failure evidence。

## 20. Rollout, Rollback and Production Gates / 发布、回滚与生产状态

K4 source merge后仍不部署/激活产品 route。Rollback只回退 K4 code-owned façade/conformance/characterization文件到最后一个 schema-compatible main；不删除 K1/K2/K3 durable facts，不重写算法状态或 broker facts。

K4-B使用additive Python V2 carriers/artifact/API，无数据库migration。rollback固定删除/回退V2 source executor、vector artifact、V2 exports、optional invocation/read request参数和对应测试，同时保留已合入V1 observation-only API及其永久FAILED语义；不得把V2 artifact转写成V1 PASSED或回填previous set。因为产品root不注入sealed V2 authority，source rollback不需要config/binding/DML、服务重启或订单补偿。若未来K5已依赖V2，必须先按K5/K6独立状态回退其shadow binding，不能在K4 rollback中删除未知使用者。

```text
design_source_merge=merged_pr_2861
k4a_source_merge=merged_pr_2883
k4b_design_source_merge=merged_pr_2914
k4b_design_merge_commit=15c38cb8d3a2c3a1710a29526ac6ac07ef580238
k4b_code_source_merge=pending_user_authorization
close_sync=not_applicable_feature
production_ddl_gate=noop
production_dml_gate=noop
production_backend_dependency_gate=noop
production_frontend_dependency_gate=noop
production_config_gate=noop
production_binding_gate=noop
broker_gate=noop
service_restart=noop
runtime_activation=noop
product_runtime=not_switched
K4_overall=implemented_verified_pending_source_merge
K4-A=implemented_verified_contract_slice_merged
K4-B=implemented_verified_source_pending_user_authorization
K5=not_started
K6=not_started
```

上述状态是交付事实，不进入每日模拟盘业务路径，也不是人工审批门禁。

## 21. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-081` | K4 current facts、K1/K2/K3/K5/K6边界、信号/执行隔离、唯一runtime/OMS/Gateway/route事实完整 |
| `F-082` | K1-C authority复用与五算法+`round_to` helper source manifest exact，Iceberg/Stop只表征不注册、utility只AST提取不执行 |
| `F-083` | initialization/transition authority input、K2 optional invocation、通用adapter、collector lifecycle、ordinal/freeze/retry/restart合同可直接实施；K2 V1单/多command shadow边界精确，K5 shadow无需修改kernel，产品aggregate仍由K6拥有 |
| `F-084` | 六个AlgoEngine方法、template helper与四个callback的shadow return/error/diagnostic/zero-command语义精确，含not-running/missing/rounded-zero空返回；product OMS同步reject语义不由K4伪证并由K6 disposition拥有 |
| `F-085` | Tick/Order/Trade/Contract DTO、selected Exchange/Status enum与event routing逐字段映射；TIMER durable TICK cutoff及missing/unsupported语义精确且无合成/fallback |
| `F-086` | durable state envelope、constructor-once/restore/extract/freeze、active-child terminal mapping与façade effect复用existing K1/K2 identity；单command shadow链可完整重建，多command trace完整但不伪装V1 materialization，且zero direct broker |
| `F-087` | implementation/method/state/terminal/DTO/isolated-module contract、runtime binding disposition、command authority disposition、conformance failure/receipt/set exact schema/hash/truncation/writer/readback/zero-partial publication完整 |
| `F-088` | current-three复用K3 parity，Iceberg/Stop source-isolated characterization-only，Iceberg TIMER lineage、K5边界与确定性输入完整 |
| `F-089` | typed error、concurrency/retry/restart、diagnostics/cardinality/retention、无人工门禁与无previous/latest fallback完整 |
| `F-090` | direct/negative/integration/fresh-process/coverage/routing/F2/rollout/rollback/生产状态分离可执行 |

## 22. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-081` | §0–§2；父蓝图§2、§5、§8、K4/K5/K6；K1/K2/K3 detailed designs | `backend/tests/miniqmt_execution_runtime/test_vnpy_k4_scope_boundaries.py`与full changed-file review证明信号、资产、方向、数量、B0、OMS、Gateway、current-three binding及产品route未改 | implemented_verified | none |
| `F-082` | §3、§12.1；existing K1-C source manifest/receipt；K4 façade source manifest与V2 executor binding | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py`：81 vectors、fresh process、writer/readback、five-algorithm source execution | implemented_verified | none |
| `F-083` | §4–§5；V2 authority/init/transition；`facade_contracts.py/facade.py/facade_adapter.py`与K2 optional seam | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_kernel_invocation.py`、`backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`：CLAIMED delivery、precomputed transition identity、ordinary path unchanged、direct SPI bypass | implemented_verified | none |
| `F-084` | §5.4.1、§6；K1 detailed §9.1；pinned engine/template signatures | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`：six-method/callback/diagnostic full actual trace与source-only state | implemented_verified | none |
| `F-085` | §6.3–§8、§15；`facade_projection.py`与existing K2 read-only query | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_repository_postgres.py` DEV PostgreSQL=`2 passed`，覆盖same-transaction ALGO_START/TIMER prior-TICK、phase/freshness、independent readback与zero-write | implemented_verified | none |
| `F-086` | §5.4.1、§5.6、§9–§10、§15；existing state/command/transition/materializer | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_lifecycle.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_kernel_invocation.py`、`backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`：single/multi-command、callback/late trade/restore与materializer identity | implemented_verified | none |
| `F-087` | §4.1、§5.1、§11–§12；V1 fail-closed + V2 source/characterization/conformance authority | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py`：sealed authority、fresh writer/readback、V1/V2隔离、bounded failure与zero-partial publication | implemented_verified | none |
| `F-088` | §3.2、§6.3、§10、§12；K3 parity与唯一V2 vector artifact | artifact: `backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json`；`backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py::test_source_attribution_hash_is_checkout_eol_independent`、`backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py::test_registry_callable_source_hash_is_checkout_eol_independent`证明checkout EOL不改变source identity；`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`覆盖current-three与Iceberg/Stop | implemented_verified | none |
| `F-089` | §13–§17 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_trace_collector_preserves_primary_failure_when_reason_code_property_breaks`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_exception_summary_sanitizes_before_bounded_truncation`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py::test_k3_preflight_failure_records_active_characterization_failure`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_diagnostics.py::test_characterization_success_does_not_clear_active_failure_before_conformance`证明primary failure、omitted hash和active/last failure闭合 | implemented_verified | none |
| `F-090` | §18–§20 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_repository_postgres.py` DEV=`2 passed`；非DB direct=`202 passed,1 skipped`；coverage八核心line/branch均`>=80/>=70`；`python -m nox -s miniqmt_execution_runtime_l2`=`1099 passed,30 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；`python -m nox -s l0`、`python -m nox -s validation_module_registry_l0`与三份F2 validator通过 | implemented_verified | none |

## 23. DESIGN-COMPLIANCE-001 / 正式设计复核

2026-07-30 K4-B实现正式复核逐项核对生产代码、真实public writer/readback/invocation/repository seams、direct/DEV/L2/Paper/coverage证据和K4/K5/K6边界。实现中发现并修复artifact K3 live binding/receipt漂移、active-order price/quantity/lineage closure、terminal ORDER晚到TRADE、callback/outcome次序、standard import boundary与repository public signature authority；以下PASS表示当前source达到implemented-verified，不表示已merge或产品runtime已激活。

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | K4-B定义full-input/full-actual-trace source executor、fresh readback、V2 authority、真实K2 transaction seam、DEV PostgreSQL与五算法矩阵；K4-A/K4-B状态分开，caller observation和test-only adapter candidate均不能冒充完成 |
| no silent error | pass | V1永不PASSED；spawn/timeout/malformed carrier、source exception、actual drift、claimed delivery/repository corruption均typed fail且zero publication/effect；合法TICK unavailable仍有typed diagnostic，无固定PASSED、假ACK、catch-all覆盖或fallback |
| no business semantic drift | pass | Iceberg TIMER读取durable sequence-cutoff B0 TICK并保持upstream callback；K4不修改current-three K3 factory/behavior、price/quantity/side/timer、A股规则、Selection/Target/B0/OMS/Gateway或产品route |
| no unauthorized gates | pass | sealed conformance authority、capability、same-session/fence/strict readback都是局部确定性技术合同；不新增RBAC、审批、人工ack、package二次准入、配置enable flag、人工恢复或日常run gate |
| no parallel runtime/route | pass | 无第二MainEngine/EventEngine/OMS/Gateway/repository/dispatcher；current-three无第二factory；K6仍独占cutover |
| exact authority reuse | pass | K1/K3/K4 source与catalog/gateway/route均现场重建；V1 fail-closed与V2 executable authority严格隔离；V2 receipt绑定full execution set且readback重新执行；K4/K5 shadow receipt不能替代K6 product authority |
| transaction/restart correctness | pass | pre-call CLAIMED delivery保持transition_id null，V2单独以existing identity函数重建；repository event+delivery在existing cursor/lock/CAS内读取，fixed cutoff独立readback；subprocess不在DB transaction内启动，retry/restart byte-identical |
| K5/K6 boundary | pass | K4交付通用adapter与shadow invocation但不注册Iceberg/Stop；K5只新增plugin/manifest/binding/tests且shadow验收无需改kernel；generic per-command product authority aggregate、dependent-BUY与route cutover均由K6独立F2拥有；无DDL/config/production binding/restart/runtime activation |
| production state separation | pass | design/source/merge/DDL/dependency/config/binding/broker/restart/runtime分别记录；当前全部production/runtime gates为noop |

## 24. Definition of Done / K4 完成定义

K4 implementation只有同时满足以下条件才可标记 `implemented_verified`：

1. `F-081..F-090` 全部具有真实 code/test receipt；
2. current-three三个registered pure plugin均有exact PASSED V2 `PURE_PLUGIN_SHADOW_CONFORMANCE + NOT_APPLICABLE_PURE_PLUGIN` receipt，full source-execution set、live implementation binding、fresh writer/readback和set closure通过；全部V1 receipt仍FAILED；K4不得宣称已有registered façade-backed adapter；
3. Iceberg/Stop V2 exact source characterization PASSED，包含full actual trace、constructor-once/state restore和Iceberg TIMER durable TICK lineage，但catalog/creation binding/K2 runtime仍为零；
4. V2 initialization/transition optional invocation、CLAIMED pre-call identity、façade六方法、DTO/enum/state mapping、callback routing、ordinal/freeze/retry/restart全部通过production-constructor direct tests；test-only adapter candidate不进入catalog/DB且不作为单独完成证据；
5. effect只通过existing K1/K2 constructors进入transition；单command shadow authority可独立重建，多command collector trace完整且V1 materialization明确拒绝；existing repository same-transaction read set有DEV PostgreSQL zero-write与fixed-cutoff independent readback证据；zero direct Gateway/broker；
6. current-three factory/binding和业务行为不变，K3 parity继续通过；
7. malformed/corrupt/unsupported/ownership/capability/state/binding/authority/failure truncation全部fail loud且无fallback；
8. core line/branch coverage、changed-file routing、F2 validators、DESIGN-COMPLIANCE-001和required CI通过；
9. K4 source状态、K5/K6、产品route、DDL/config/binding/restart/runtime activation分别报告；
10. 产品 runtime 仍为 `not_switched`，不因 K4 source merge自动重启或激活。

当前K4-B source已满足1–10的代码与验证条件，PR #2953 required CI已通过；source merge仍待用户明确授权，合入后仍不得自动执行服务重启或runtime activation。
