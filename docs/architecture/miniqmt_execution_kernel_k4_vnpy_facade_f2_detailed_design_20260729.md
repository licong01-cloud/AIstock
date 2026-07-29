# MiniQMT 统一执行内核 K4 vn.py Compatibility Façade F2 详细设计

> Feature tier：`F2`。文档状态：`design_ready_for_implementation`。
>
> 上位唯一实现蓝图：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一上位蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 已合入前置：K1、K2、K3 overall 均为 `implemented_verified + merged`。K3-B 已通过 PR #2848 / merge `38434e10d530edd883fa75f904de5b025158f918` 合入，状态同步已通过 PR #2858 / merge `70ec6aaec28aa20755d73ecfe1a027f8ea94dbad` 合入。K4/K6 当前均为 `not_started`，产品 runtime 未切换。
>
> 本文只细化父蓝图已经批准的 K4：transition-scoped `VnpyAlgoEngineFacadeV1`、精确 method/DTO/enum/effect 映射、每个已注册算法的 façade conformance receipt，以及 current-three + Iceberg/Stop source-compatible characterization。本文不增加算法、策略、产品 route、runtime owner、OMS、Gateway、数据库表、人工门禁或审批。

## 0. Executive Decision / 核心结论

K4 在 K1-C 双 upstream V2 authority、K1-B plugin catalog/route receipt、K2 durable kernel 和 K3 current-three pure plugins 之间补齐唯一缺失的 vn.py source-compatible façade。K4 的输出是代码所有、可重算、shadow-only 的 compatibility/conformance 事实，不是第二个执行平台，也不是产品运行开关。

固定决策如下：

1. `VnpyAlgoEngineFacadeV1` 只在单次 `initialize/transition` 内存在；它读取 immutable K2/K3 输入并只向 transition-local collector 追加 existing K1/K2 effects。
2. façade 不直接调用 Gateway、broker、repository、HTTP、DB、wall clock、global random、UUID、EventEngine 或 OMS；K2 继续拥有事务、dispatch、callback、reconcile、timer、fencing 和 retry。
3. K1-C `VnpyCompatibilityRequirementV2/VnpyCompatibilityReceiptV2/LockedSurfaceV2` 继续是 method/DTO/enum/pinned-source 唯一 authority；K4 不复制、不降级、不改写 K1 V2 receipt。
4. K4 增加 `VnpyFacadeConformanceReceiptV1`，只证明“当前 AIstock façade 实现 + exact K1 receipt + exact manifest + exact characterization”闭合。它不替代 K1 receipt，也不创建第二个 plugin catalog。
5. current-three 继续使用 K3 已合入的 exact pure plugin/factory/binding；K4 不替换其 factory，不创建平行 current-three route。K4 只对三个现有注册 plugin 生成 conformance receipt，并复用 K3 parity trace 作为行为证据。
6. Iceberg/Stop 在 K4 只进入 pinned source-compatible characterization；不创建 manifest、factory、plugin、creation binding、algo instance 或 broker command。K5 才拥有它们的插件新增与运行验收。
7. K4 完成后仍为 shadow-only；K6 继续独占 dependent-BUY durable coordinator、旧 helper 退役、route cutover、生产迁移和真实 SIM 验收。

### 0.1 Background / 背景

K1 已冻结 plugin/manifest、双 upstream method/DTO/enum/source authority和immutable compatibility receipt；K2 已提供durable event/delivery/transition/mapping/outbox/timer/reconcile内核；K3 已把current-three迁入同一pure plugin SPI并完成committed-fact parity。当前唯一未闭合的是父蓝图§8要求的transition-scoped vn.py façade：它必须把pinned调用面映射到现有K1/K2 effect，而不能让upstream算法直接拥有EventEngine、OMS、Gateway或broker。K4只关闭该缺口。

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
- 对 `TickData/OrderData/TradeData/ContractData` 和 `Direction/Offset/OrderType/AlgoStatus` 建立 exact manifest-driven projection。
- 生成 current-three 每个已注册 plugin 的 `VnpyFacadeConformanceReceiptV1`，并形成单一 `VnpyFacadeConformanceSetV1` derived view。
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
- `backend/execution_algos/vnpy_compat/facade_characterization.py`：repo-owned pinned source characterization builder/readback。
- `backend/execution_algos/vnpy_compat/pinned_source/facade_source_manifest.json`：五个算法及 `round_to` helper 的 exact source identity。
- `backend/execution_algos/vnpy_compat/pinned_source/vnpy_algotrading/algos/iceberg_algo.py` 与 `stop_algo.py`：同一 pinned commit 的只读 source authority。
- `backend/execution_algos/vnpy_compat/pinned_source/vnpy_core/vnpy/trader/utility.py`：只供 AST 提取 pinned `round_to`；禁止 import/execute 整个 utility module。
- `backend/services/miniqmt_execution_runtime/plugin_registry.py`：只增加从 existing `PluginCatalogSnapshotV1`（由 `build_plugin_catalog_v2` 生成）派生 conformance set 的显式 seam；不改变现有 catalog schema/hash/publication。
- `backend/services/miniqmt_execution_runtime/kernel_delivery.py`：只增加 façade conformance readback 的 shadow integration seam；不改变 current-three binding 或产品 routing。

禁止创建第二个 repository、dispatcher、scheduler、route evaluator、plugin registry、Gateway adapter 或 runtime controller。

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
                    VnpyFacadeConformanceSetV1 readback
                                      |
                                      v
            transition-scoped VnpyAlgoEngineFacadeV1
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

## 5. Transition-Scoped Construction Contract / 单次 transition 构造合同

### 5.1 `VnpyFacadeTransitionInputV1`

该 carrier 只组合 existing canonical payload，不复制其业务 schema：

```text
schema_version=miniqmt_vnpy_facade_transition_input_v1
runtime_event: RuntimeEventEnvelopeV2
delivery: AlgoEventDeliveryV1
algo_instance: ExecutionAlgoInstancePersistenceV2
manifest: ExecutionAlgoPluginManifestV2
pinned_compatibility_receipt: VnpyCompatibilityReceiptV2
route_compatibility_receipt: PluginRouteCompatibilityReceiptV1
before_state: AlgoStateSnapshotV2
execution_projections: ExecutionProjectionSetV1
ordered_active_mappings: tuple[ExecutionCommandChildMappingV1,...]
transition_sequence
input_sha256
```

`input_sha256 = hash_hex_v1("miniqmt_vnpy_facade_transition_input_v1", canonical refs/full payload required by each existing readback)`。writer/readback 必须先分别 strict-validate existing carriers，再验证 runtime/algo/plugin/event/delivery/sequence/route owner closure。active mappings 按 `local_vt_orderid` 排序且 parent/runtime/algo/symbol/side 必须一致。

### 5.2 Construction

Exact constructor：

```python
VnpyAlgoEngineFacadeV1.create(
    transition_input: VnpyFacadeTransitionInputV1,
    effect_collector: VnpyFacadeEffectCollectorV1,
) -> VnpyAlgoEngineFacadeV1
```

`create()` 必须验证：

1. K1 pinned receipt 为 `PASSED` 且与 manifest component/hash exact；
2. route receipt 已使用 exact catalog snapshot + strict gateway catalog 完成 authority readback，且 status 为 `PASSED`；
3. manifest `required_facade_methods/object_fields/order_types/market_data_requirements` 与调用面一致；
4. before state、active mappings、projection set 与 K2 durable owner/version/hash 完整闭合；
5. collector 尚未冻结且 transition identity/sequence 与 input 完全相同。

任一失败不构造 façade、不调用 algorithm callback、不生成 effect。不得使用 previous conformance、默认 projection 或 legacy product helper。

### 5.3 Collector lifecycle

`VnpyFacadeEffectCollectorV1` 不是新业务 effect authority；它只按调用顺序收集 existing effect constructors 的输入，并在 freeze 时生成一个 existing `AlgoTransitionV1`。

- ordinal 从 `0` 连续递增，禁止 caller 注入、跳号或复用；
- `send_order/cancel_order` 生成 existing `BrokerCommandV2`；
- `write_log/put_algo_event/missing get_*` 生成 existing `DiagnosticObservationV1`，payload 使用 §10 strict schema；
- 不生成 TIMER；timer 仍只来自 plugin `TimerMutationV1` 和 K2 ExchangeSessionClock；
- freeze 后任何调用 typed fail；
- 同 input 重试必须 byte-identical；same identity/different effect terminal conflict；
- collector 不持久化、不 dispatch、不 ACK。

## 6. Exact Façade Method Semantics / 精确方法语义

### 6.1 `send_order`

输入必须满足 pinned signature；`direction/order_type/offset` 必须是 §7 exact enum projection。`price` 必须为 finite positive number并 canonicalize为 existing decimal price；`volume` 接受 pinned `float` surface，但必须是 finite、positive且非 `bool`。façade使用§3.2 exact pinned `round_to(volume,contract.min_volume)`；除该公式外禁止任何隐式四舍五入。rounded结果为zero时走下述upstream-compatible空字符串路径；非zero结果必须是exact integral A-share shares，否则typed fail。字符串、NaN、Inf全部拒绝。

处理顺序固定：

1. manifest 声明 `send_order`、order type、side 和 required market capability；
2. route receipt 对 exact order/capability 为 PASSED；
3. contract projection 存在且 symbol/exchange/gateway/min_volume/pricetick 与 owner/hash闭合；
4. volume只执行pinned `round_to`，其结果与price进入existing OMS/board-lot preflight；façade不另写board-lot规则；
5. collector 以 next ordinal 调用 `BrokerCommandV2.create(SUBMIT_LIMIT,...)`；
6. 返回该 command 的 deterministic `local_vt_orderid`。

missing contract 或 pinned `round_to` 产生zero时，按 upstream返回 `""`，同时必须追加stable typed diagnostic，`broker_called=false`、zero broker command。OMS reject不是rounded-zero，必须保留其existing typed result，不能转换为空字符串成功。其它invalid/capability/identity冲突直接typed fail。façade不调用Gateway。

### 6.2 `cancel_order`

`vt_orderid` 必须命中 transition input 中唯一 active mapping，且 runtime/algo/parent/child/submit command/broker identity 与 state closure 完整。unknown、duplicate、inactive、cross-owner 或缺 broker identity 均 typed fail；不得只写日志后返回。

合法调用以 next ordinal 创建 existing `BrokerCommandV2(CANCEL_ORDER)`，引用 exact owned local/broker mapping。它不创建第二 child、不改变 mapping、不调用 Gateway，返回 `None`。

### 6.3 `get_tick`

只从当前 `RuntimeEventEnvelopeV2` 中 schema=`miniqmt_market_data_view_v2` 的 immutable payload及 `ExecutionProjectionSetV1.market_data_projection` 构造 §7 `TickData` view。对 current-three，quote authority 只能是 native `B0_QUOTE_V2` continuous L1；不得从 minute、last known、cache、ordinary quote 或 auction 合成 continuous fields。

没有当前合法 view 时返回 `None`，并追加 `MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE` diagnostic，包含 runtime/algo/event/delivery/symbol、required fields、missing fields、market-data lineage 和 reason。不得读取 process cache fallback。

### 6.4 `get_contract`

只从 frozen symbol/contract projection、board-lot authority 和 strict route/gateway receipt 构造 §7 `ContractData` view。缺失返回 `None` + `MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE` diagnostic；不得猜 exchange、gateway、min_volume 或 pricetick。

### 6.5 `write_log`

`msg` 必须是非空字符串，UTF-8 canonical 后最大 2048 字符；超限保留前 2048 字符并在同一 diagnostic payload 中记录 original length 和 full-message SHA-256，不能静默截断。`algo` 若非 null 必须等于当前 transition owner。日志只产生 bounded diagnostic，不能代替 exception、failure receipt、command receipt 或 callback evidence。

### 6.6 `put_algo_event`

只接受 §10 `VnpyAlgoProjectionObservationV1` 可严格重建的 parameter/variable/status payload；拒绝 unknown field、mutable object、callable、NaN/Inf、非字符串 key 和 owner drift。追加 existing diagnostic observation，不启动 EventEngine，不广播产品 UI，不持久化第二份 algo state。

### 6.7 Template helpers and callback methods

`buy/sell/cancel_order/cancel_all/get_tick/get_contract/write_log/put_event/finish/pause/resume` 保持 pinned `AlgoTemplate` 调用关系；它们不是新增 façade surface。`cancel_all` 必须按 frozen active mapping 的 `local_vt_orderid` 排序展开，禁止裸 broker cancel-all。

`update_tick/update_timer/update_order/update_trade` 只能由 exact K2 delivery route 调用一次，顺序等于 per-algo delivery sequence。callback 不得直接调用 repository/Gateway，也不得由 tick 推断 ORDER/TRADE。

## 7. DTO and Enum Projection / DTO 与枚举映射

### 7.1 General rules

- 只实现 K1-C `required_object_fields/required_enum_values` 和 exact manifest 声明字段；没有 dynamic `__getattr__`。
- projection object transition-local、不可跨 delivery 缓存；source facts immutable。
- 输入先 strict readback，再映射；不得先构造默认 DTO 后补字段。
- 对同一 source payload 映射结果 byte-identical；mapping table进入 `dto_mapping_sha256`。
- enum 使用 explicit table；未知值 fail loud，不按字符串名称猜测。

### 7.2 Exact mapping table

| upstream object/field | AIstock source | rule |
| --- | --- | --- |
| `TickData.vt_symbol` | frozen symbol + exchange projection | exact `symbol.exchange`；不猜 exchange |
| `TickData.datetime` | market-data event logical timestamp | Asia/Shanghai aware；不得用 wall clock |
| `bid_price_1/bid_volume_1/ask_price_1/ask_volume_1` | native B0 L1 lineage | exact decimal/strict non-negative volume；required side missing返回 `None` + diagnostic |
| `last_price/limit_up/limit_down` | manifest-declared native field | K4 仅 characterization Stop；K5 未注册前不声明产品可用 |
| `OrderData.vt_orderid` | mapping `local_vt_orderid` | 不以 broker id 替代 |
| `OrderData.status/traded/price/is_active()` | strict ORDER/RECONCILE event + durable mapping | status/cumulative/price 共用 K3/K2 callback authority |
| `TradeData.vt_orderid` | exact mapping local id | callback mapping必须唯一 |
| `TradeData.vt_tradeid/price/volume/datetime` | strict TRADE event | trade identity唯一；时间来自 broker event authority |
| `ContractData.symbol/exchange/gateway_name/min_volume/pricetick` | frozen contract + route capability projection | 全部必需；缺失返回 `None` + diagnostic |
| `Direction.LONG/SHORT` | existing BUY/SELL | exact two-way table；不支持 NET |
| `Offset.NONE` | existing A-share offset authority | K4不引入 OPEN/CLOSE |
| `OrderType.LIMIT` | existing `SUBMIT_LIMIT` | 其它 order type unsupported |
| `AlgoStatus.RUNNING/PAUSED/STOPPED/FINISHED` | existing algo lifecycle projection | 不新增产品状态；K2 terminal truth优先 |

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

## 10. Projection Observation Contract / 参数变量状态观测

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

该 observation 进入 existing `DiagnosticObservationV1` payload，不是第二份 durable algo state，不得参与交易决策或 UI 成功判定。

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
pinned_compatibility_receipt_sha256
requirement_sha256, surface_sha256, source_lock_sha256
method_signature_sha256, object_field_sha256, characterization_sha256
facade_contract_sha256, dto_mapping_set_sha256
facade_source_manifest_sha256, algorithm_characterization_sha256
status=PASSED|FAILED
ordered_failures
receipt_sha256
```

所有 K1 component 必须逐字段等于 exact supplied `VnpyCompatibilityReceiptV2`；manifest/plugin identity必须等于 existing `PluginCatalogSnapshotV1` descriptor。`facade_source_manifest_sha256`必须同时闭合五算法和pinned `round_to` helper。`receipt_sha256 = hash_hex_v1("miniqmt_vnpy_facade_conformance_receipt_v1", exact preceding fields)`。

PASSED 必须无 failure；FAILED 必须至少一项 failure/marker。writer/readback 从 exact catalog descriptor、K1 receipt、K4 contract/mapping/source manifest和characterization receipt重建，不接受 caller-supplied status/failure/hash。

### 11.3 Conformance set

`VnpyFacadeConformanceSetV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_conformance_set_v1
plugin_catalog_sha256
facade_contract_sha256
dto_mapping_set_sha256
facade_source_manifest_sha256
ordered_receipts
build_input_sha256
receipt_set_sha256
```

对 existing catalog 中每个 `required_facade_methods` 非空的 registered descriptor恰好一个 receipt，按 plugin key 排序；missing/extra/duplicate/key drift全部失败且不发布 partial set。current-three K4 completion要求三项均 PASSED。Iceberg/Stop未注册，因此不进入该 set。

该 set 是 existing catalog 的 derived conformance view，不是第二 plugin catalog：不能增加/删除/重排 descriptor，不能决定 route，不持久化 DB，也不能作为当前产品 runtime 的人工 run gate。K4 façade-backed creation future seam 只能显式消费 exact PASS receipt；当前 K3 binding不改变。

## 12. Source-Compatible Characterization / 源码兼容性表征

### 12.1 Characterization receipt

`VnpyFacadeAlgorithmCharacterizationReceiptV1` exact fields：

```text
schema_version=miniqmt_vnpy_facade_algorithm_characterization_receipt_v1
algo_code, source_identity_sha256, facade_source_manifest_sha256
facade_contract_sha256, dto_mapping_set_sha256
ordered_vector_ids, vector_set_sha256
status=PASSED|FAILED
ordered_failures
receipt_sha256
```

`vector_set_sha256 = hash_hex_v1("miniqmt_vnpy_facade_characterization_vector_set_v1", ordered full vector payloads)`；receipt domain为 `miniqmt_vnpy_facade_algorithm_characterization_receipt_v1`。

每个 `VnpyFacadeCharacterizationVectorV1` exact fields：

```text
schema_version, vector_id, algo_code, side
canonical_config, before_state_sha256
event_type, event_payload_sha256, projection_set_sha256
explicit_deterministic_inputs
expected_ordered_facade_calls
expected_ordered_effects
expected_after_state_sha256, expected_terminal_outcome
vector_sha256
```

无 wall clock、PID、absolute path、installed package或网络输入。BestLimit upstream `uniform` 只在 characterization harness 中由 `explicit_deterministic_inputs` 提供可重放 draw；产品 K3 继续使用 K1 deterministic u53 authority，K4 不恢复 global random。

### 12.2 Current-three

Sniper/BestLimit/TWAP 必须：

- 从 existing K1 current-three manifest/receipt 和 K3 committed parity vector读取 input；
- 将 pinned source method-call trace映射为 §6 façade calls；
- 将 K3 `CurrentThreeParityTraceV1` 映射为 expected effects/state；
- 证明 call ordinal、price、quantity、reason、cancel ownership、timer semantics、state和terminal outcome一致；
- 任一 drift产生 FAILED receipt，不能用 transport suppression掩盖 business drift。

不创建或调用第二 current-three plugin factory。

### 12.3 Iceberg/Stop characterization-only boundary

K4 必须使用 §3 repo-owned exact source bytes和 source-isolated harness验证：

- Iceberg 只需要 `TIMER/ORDER/TRADE`、`get_tick`、owned cancel、visible slice submit、parameter/variable/status projection；
- Stop 只需要 `TICK/ORDER/TRADE`、`last_price/limit_up/limit_down`、single trigger submit和terminal state；
- source访问的 façade method/DTO/enum 必须全部属于父蓝图已批准 surface；unsupported surface直接 FAILED；
- harness broker_called=false，不创建 K2 algo、delivery、mapping、outbox或 DB row；
- characterization PASSED 只说明 K4 façade surface足够，不表示 K5 plugin已实现、注册、可运行或产品启用。

## 13. Error Semantics / 错误语义

固定 stable reason codes：

- `MINIQMT_VNPY_FACADE_SOURCE_INVALID`
- `MINIQMT_VNPY_FACADE_CONTRACT_INVALID`
- `MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID`
- `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED`
- `MINIQMT_VNPY_FACADE_TRANSITION_INPUT_INVALID`
- `MINIQMT_VNPY_FACADE_ORDER_INPUT_INVALID`
- `MINIQMT_VNPY_FACADE_ORDER_OWNERSHIP_INVALID`
- `MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE`
- `MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE`
- `MINIQMT_VNPY_FACADE_EFFECT_CONFLICT`
- `MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED`
- `MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID`
- `MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED`

Typed exception context 至少包含适用的 plugin/runtime/algo/event/delivery/transition/method/field/expected/actual/source/receipt identity。错误 evidence 必须 JSON-safe、repo-relative、稳定排序且有界。不得 `except Exception: pass`、固定 PASSED、空 ACK、空 diagnostic、默认值成功或 error-to-log-only。

missing tick/contract 的 upstream-compatible `None` 和 missing/rounded-zero order 的 `""` 不是静默失败：必须同时生成 exact typed diagnostic；其它 corruption/ownership/capability failure 必须抛出 typed error。

## 14. Concurrency, Retry and Restart / 并发、重试与重启

- façade/collector/object projection均为 transition-local；禁止 singleton、thread-local隐藏状态或跨 delivery cache。
- 同 delivery retry从相同 durable input重建，effect/receipt byte-identical；same identity/different payload terminal conflict。
- K2 lease/fence/CAS继续唯一拥有 writer并发；K4不实现锁、retry loop或 DB writer。
- callback-before-ACK、late callback、same-symbol/multi-slot通过 existing mapping/event sequence隔离；façade只消费 exact owner facts。
- process restart只从 durable state/mapping/event/catalog/receipt重建，不序列化 Python algorithm/façade object。
- characterization fresh-process输出必须与本进程 byte-identical；absolute worktree path不得进入 hash。
- failure不回退 previous conformance set、旧 receipt、legacy helper或 current product route。

## 15. Repository, Migration and Transactions / Repository 与迁移

K4 不新增 durable DB schema、DDL、DML或 repository。所有交易事实继续通过 K2 existing transaction bundle持久化。K4 conformance/characterization receipts是 code-owned immutable build artifacts，由 fresh-process可重算；不写生产表，不触发 broker。

如果实现发现必须新增数据库字段、修改 K2/K3 durable schema或变更 product binding，说明本设计假设失效，必须停止并回到父蓝图重新审查，不能在 K4 PR 中顺手扩大 scope。

## 16. Diagnostics, Metrics and Retention / 诊断与保留

K4 不新建 metrics backend、alert、operator acknowledge或审批流。只允许复用 existing K2 diagnostics：

- code-owned conformance build status：registered count、PASSED/FAILED count、failure reason count；
- characterization status：五个固定 algo 的 source/vector/receipt hash；固定低基数标签仅为 `algo_code,status,reason_code`；
- runtime diagnostics只有 façade 真正被未来 façade-backed plugin调用时才记录 method/reason；current product runtime在 K4阶段不接线。

完整 failures 保留在 immutable receipt；metrics不包含 symbol、order id、runtime id等高基数标签。无人工 acknowledge；成功 rebuild 自动清除 active failure但保留 last failure receipt。

## 17. Risks / 风险

| risk | prevention and evidence |
| --- | --- |
| façade 被实现成第二 runtime 或直接 broker adapter | import/static guard 禁止 EventEngine/OMS/Gateway/repository/broker ownership；所有命令只经 existing `BrokerCommandV2` collector |
| current-three 被切到平行 façade factory | exact K3 factory/class/binding readback与static negative test；K4不得修改creation bindings |
| Iceberg/Stop characterization 被误报为 plugin 完成 | source disposition固定 `CHARACTERIZATION_ONLY_K5`；catalog/creation/K2 row zero-publication直接测试 |
| K1 receipt 与实际 façade implementation自证循环 | K1 receipt、K4 contract/mapping/source/characterization是独立输入，conformance writer/readback逐项重建，不接受caller hash/status |
| DTO missing field被默认值掩盖 | manifest-driven exact field set；先strict readback再构造，missing返回typed None/diagnostic或fail，不先建默认DTO |
| order ownership或callback alias错误导致跨algo影响 | exact active mapping与K2 callback authority；unknown/duplicate/cross-owner terminal fail，zero command |
| retry/restart重复effect | transition-local collector、deterministic ordinal、existing K2 identity/CAS/fence；fresh-process hash parity |
| failure evidence高基数或无界 | bounded immutable failure set与固定低基数metrics；完整identity只留receipt context |
| K4实施顺手修改K5/K6/生产状态 | changed-file review、F-081/F-088/F-090、DESIGN-COMPLIANCE；发现DB/binding/cutover需求立即停止并回到父蓝图 |

## 18. Verification Plan / 验证方案

### 17.1 Direct contract tests

- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_contracts.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_algo_engine_facade.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_projection.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_receipts.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_characterization.py`

覆盖 strict schema/extra/type/identity/hash、recursive immutability、source set、method signature、DTO/enum mapping、six methods、callback routing、ordinal、freeze、bounded failure/truncation、writer/readback、fresh-process parity。

### 17.2 Negative matrix

必须直接覆盖：

- K1 receipt FAILED/missing/duplicate/component drift；
- catalog/manifest/plugin key/hash drift；
- algorithm/helper source missing/extra/duplicate/traversal/size/hash/decode/AST drift；`round_to` body/name/signature/Decimal-round-int-multiply-return drift；
- unsupported method/field/enum/order type/callback；
- bool/NaN/Inf/fractional quantity、invalid price、unknown/cross-owner cancel；
- missing/stale/synthetic quote、contract/gateway/min_volume/pricetick缺失；
- callback mapping alias/owner/status/cumulative/trade identity conflict；
- effect ordinal gap/duplicate、post-freeze call、same id/different effect；
- failure >255、malformed failure context、renderer secondary failure；
- receipt hash-correct但authority-inconsistent readback；
- absolute path、wall clock、global random、UUID、installed/latest/network fallback；
- Iceberg/Stop被错误加入catalog/creation binding/K2 runtime的static negative test。

### 17.3 Integration and parity

- current-three：reuse K3 committed parity vectors，证明 façade call/effect trace与existing K3 outcome一致；不调用第二 factory。
- K2 shadow seam：用真实 `BrokerCommandV2/AlgoTransitionV1` constructors验证 command/mapping/materializer identity；broker_called=false，不运行 dispatcher。
- source-isolated Iceberg/Stop characterization：真实 repo-owned bytes，zero catalog publication、zero DB/broker effect。
- restart/fresh-process：receipt/source/vector/mapping hash byte-identical。
- standard package import：不自动注册算法、不加载legacy adapter、不启动runtime/EventEngine/OMS/Gateway。

### 17.4 Coverage and routing

- K4核心 `facade_contracts.py/facade_projection.py/facade.py/facade_characterization.py` 各自 line `>=80%`、branch `>=70%`。
- changed files必须全部经 `file_ownership.yaml -> module_registry.yaml -> test_plans.yaml` 映射，`unmapped_code_files=[]`。
- 预计只选择 `miniqmt_execution_runtime_l2` 和因 `backend/execution_algos/**` shared ownership而真实依赖的 `paper_v2_backend`；无 shared contract diff时不得额外运行其它模块。
- F2 validators：本文、父执行内核蓝图、统一模拟盘蓝图全部通过且 warnings=0。

## 19. Implementation Plan / 实施方案

K4 implementation固定最多两个 source PR，不扩大到 K5/K6：

### K4-A — contracts, source authority and conformance receipts

- `facade_contracts.py`、五算法+`round_to` helper source manifest、Iceberg/Stop与utility repo-owned source bytes；
- DTO mapping contract、characterization vector/receipt、conformance receipt/set；
- writer/readback、failure/truncation、fresh-process determinism；
- 不接 K2 delivery、不调用 broker。

### K4-B — transition-scoped façade and shadow integration

- `facade_projection.py/facade.py`；
- existing K1/K2/K3 public seam integration；
- current-three conformance/parity、Iceberg/Stop characterization-only；
- direct/L2/coverage/F2/DESIGN-COMPLIANCE；
- 不修改 current-three binding，不切产品 route。

任一 PR 不得以“后续补齐”为由省略本切片的 writer/readback、negative、restart或failure evidence。

## 20. Rollout, Rollback and Production Gates / 发布、回滚与生产状态

K4 source merge后仍不部署/激活产品 route。Rollback只回退 K4 code-owned façade/conformance/characterization文件到最后一个 schema-compatible main；不删除 K1/K2/K3 durable facts，不重写算法状态或 broker facts。

```text
source_merge=pending_design_pr
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
K5=not_started
K6=not_started
```

上述状态是交付事实，不进入每日模拟盘业务路径，也不是人工审批门禁。

## 21. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-081` | K4 current facts、K1/K2/K3/K5/K6边界、信号/执行隔离、唯一runtime/OMS/Gateway/route事实完整 |
| `F-082` | K1-C authority复用与五算法+`round_to` helper source manifest exact，Iceberg/Stop只表征不注册、utility只AST提取不执行 |
| `F-083` | transition-scoped façade/input/collector生命周期、ordinal/freeze/retry/restart合同可直接实施 |
| `F-084` | 六个AlgoEngine方法、template helper与四个callback的return/error/diagnostic/zero-command语义精确 |
| `F-085` | Tick/Order/Trade/Contract DTO、enum与event routing逐字段映射、missing/unsupported语义精确且无合成/fallback |
| `F-086` | façade effect复用existing K1/K2 command/transition/mapping/child/outbox identity，完整链可重建且zero direct broker |
| `F-087` | conformance failure/receipt/set exact schema/hash/truncation/writer/readback/zero-partial publication完整 |
| `F-088` | current-three复用K3 parity，Iceberg/Stop source-isolated characterization-only，K5边界与确定性输入完整 |
| `F-089` | typed error、concurrency/retry/restart、diagnostics/cardinality/retention、无人工门禁与无previous/latest fallback完整 |
| `F-090` | direct/negative/integration/fresh-process/coverage/routing/F2/rollout/rollback/生产状态分离可执行 |

## 22. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-081` | §0–§2；父蓝图§2、§5、§8、K4/K5/K6；K1/K2/K3 detailed designs | `backend/tests/miniqmt_execution_runtime/test_vnpy_k4_scope_boundaries.py`核对K4不修改信号、资产、方向、数量、B0、OMS、Gateway、current-three binding与产品route | design_ready | none |
| `F-082` | §3；existing K1-C source manifest/receipt；planned façade source manifest | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_characterization.py` exact five-source positive/negative与source bytes hash/readback | design_ready | none |
| `F-083` | §4–§5；planned `facade_contracts.py/facade.py` | `backend/tests/miniqmt_execution_runtime/test_vnpy_algo_engine_facade.py` create/freeze/ordinal/retry/restart/post-freeze matrix | design_ready | none |
| `F-084` | §6；K1 detailed §9.1；pinned engine/template signatures | `backend/tests/miniqmt_execution_runtime/test_vnpy_algo_engine_facade.py` six-method + template/callback positive/negative；missing `None/""`同时有typed diagnostic | design_ready | none |
| `F-085` | §7–§8；planned `facade_projection.py` | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_projection.py` DTO/enum/field/owner/lineage/unsupported/missing/synthetic quote matrix | design_ready | none |
| `F-086` | §5.3、§9；existing `BrokerCommandV2/AlgoTransitionV1`与K2 materializer | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_k2_shadow_integration.py` direct constructors、mapping identity、dispatch attempt=0、broker_called=false | design_ready | none |
| `F-087` | §11；planned `facade_contracts.py` + explicit derived-set seam | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_receipts.py` writer-readback、hash-correct authority drift、failure 255/256/500、zero partial set | design_ready | none |
| `F-088` | §3.2、§12；K3 `CurrentThreeParityTraceV1`；planned characterization builder | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_characterization.py` current-three real parity vectors与Iceberg/Stop repo-owned source-isolated zero registration/DB/broker | design_ready | none |
| `F-089` | §13–§17 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_failure_restart.py` malformed/error-renderer、same-ID conflict、multi-slot ownership、fresh-process、low-cardinality diagnostics | design_ready | none |
| `F-090` | §18–§20 | `python -m nox -s miniqmt_execution_runtime_l2`；`python -m nox -s paper_v2_backend`；core line>=80%/branch>=70%；classifier `unmapped_code_files=[]`；三份F2 validator与DESIGN-COMPLIANCE | design_ready | none |

## 23. DESIGN-COMPLIANCE-001 / 正式设计复核

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | 六方法、四DTO、四callback、current-three和Iceberg/Stop表征、receipt/readback/restart/negative/coverage均有实施合同；mock-only不计完成证据 |
| no silent error | pass | upstream-compatible `None/""`必须同时有typed diagnostic；其它invalid/ownership/capability/corruption均typed fail；无空FAILED、固定PASSED、假ACK或exception swallowing |
| no business semantic drift | pass | K4不修改current-three K3 factory/behavior、price/quantity/side/timer、A股规则、Selection/Target/B0/OMS/Gateway或产品route |
| no unauthorized gates | pass | conformance是父蓝图已批准的代码完整性receipt，仅约束未来façade-backed binding；不是RBAC、审批、人工ack、package二次校验或日常run gate |
| no parallel runtime/route | pass | 无第二MainEngine/EventEngine/OMS/Gateway/repository/dispatcher；current-three无第二factory；K6仍独占cutover |
| exact authority reuse | pass | K1-C V2 source/method/object/surface receipt保持唯一；K4 algorithm source manifest只承担五算法表征，不复制API/DTO authority |
| K5/K6 boundary | pass | Iceberg/Stop仅characterization；无manifest/plugin/binding；无dependent-BUY、DDL/config/binding/restart/runtime activation |
| production state separation | pass | design/source/merge/DDL/dependency/config/binding/broker/restart/runtime分别记录；当前全部production/runtime gates为noop |

## 24. Definition of Done / K4 完成定义

K4 implementation只有同时满足以下条件才可标记 `implemented_verified`：

1. `F-081..F-090` 全部具有真实 code/test receipt；
2. current-three 三个 registered plugin均有 exact PASSED conformance receipt，writer/readback和set closure通过；
3. Iceberg/Stop exact source characterization PASSED，但 catalog/creation binding/K2 runtime仍为零；
4. façade六方法、DTO/enum mapping、callback routing、ordinal/freeze/retry/restart全部直接测试通过；
5. effect只通过existing K1/K2 constructors进入transition，zero direct DB/Gateway/broker；
6. current-three factory/binding和业务行为不变，K3 parity继续通过；
7. malformed/corrupt/unsupported/ownership/capability/failure truncation全部fail loud且无fallback；
8. core line/branch coverage、changed-file routing、F2 validators、DESIGN-COMPLIANCE-001和required CI通过；
9. K4 source状态、K5/K6、产品route、DDL/config/binding/restart/runtime activation分别报告；
10. 产品 runtime 仍为 `not_switched`，不因 K4 source merge自动重启或激活。
