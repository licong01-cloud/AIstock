# MiniQMT 统一执行内核 K3 Current-Three Runtime Migration F2 详细设计

> Feature tier：`F2`。文档状态：`design_ready_for_review`；PR #2816；`source_merge=pending_user_review`。
>
> 上位唯一架构：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一总蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 已合入前置：K1 overall、K2 overall 均为 `implemented_verified + merged`；K2-D final source `82c69fbf7e7245e0af76262ddc7b7f59ce7d996b` 已通过 PR #2804 / merge `fc4170faa10847c0b58aa8088b4a8b6d0ca26b29` 合入。K3/K4 implementation 均为 `not_started`，产品 runtime 未切换。
>
> 本设计阶段不实施 K3 代码，不执行 DDL/DML，不修改生产配置或 binding，不调用 broker，不启动、停止或重启服务，不激活 runtime。

## 0. Implementation Decision / 实施决策

K3 将 `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT` 迁移为三个真实、side-effect-free 的 `ExecutionAlgoPluginV2`，直接消费 K1 strict contracts 和 K2 durable event/delivery/state/outbox/timer authority。三个算法不得继续由 kernel、client、B0 adapter 或 runtime 通过 `algo_code` 分支解释业务行为。

K3 固定为两个实现切片：

1. **K3-A current-three pure plugins**：实现三插件、process factory、exact manifest/source/version 更新、state/config codec 闭包和直接行为测试；保持 shadow-only，不实例化产品 runtime，不调用 Gateway。
2. **K3-B parity/inventory/shadow orchestration**：以同一 immutable event stream 同时运行 legacy characterization oracle 与 K2 plugin delivery，生成不可变 parity receipt 和 legacy state/policy inventory；两侧均 `broker_called=false`。K3-B 不创建第二个产品 route，不进行生产 cutover。

K3 不使用永久 feature flag、双 broker submit、LEGACY fallback、默认 plugin、人工 acknowledge、审批、RBAC 或 per-run enable gate。旧 `MiniQMTExecutionRuntime._ensure_vnpy_core/_handle_vnpy_actions` 产品调用的原子退役属于 K6；K3 只保证 K2 kernel 内没有 current-three 算法特判，并提供算法迁移所需的 exact parity、state inventory、restart 和 no-broker-duplicate 证据。现有 event-loop dependent-BUY 卖出回款协调器不是算法策略，它尚无 K2 durable carrier，必须作为 K6 cutover 前的独立 execution-coordination 合同实现；K3 不把它复制进三个 plugin，也不把缺失合同伪报为 cutover-ready。

## 1. Background and Current-State Evidence / 背景与当前事实

### 1.1 现有产品路径

当前产品路径仍为：

```text
ExecutionPlan / parent intent
  -> MiniQMTExecutionRuntime.create_vnpy_algo_instance
  -> create_vnpy_style_core
  -> core.update_tick/update_timer/update_order/update_trade
  -> VnpyAction[]
  -> MiniQMTExecutionRuntime._handle_vnpy_actions
  -> direct submit_child_order / gateway.cancel_child_order
  -> legacy event/state metadata write
```

定向代码事实：

| current owner | current behavior | K3 required closure |
| --- | --- | --- |
| `runtime.py::_ensure_vnpy_core` | 从 `metadata.vnpy_algo_state` 重建 process-memory core，并对字段执行宽松 `int/float/setattr` 恢复 | K3 plugin 只能从 `AlgoStateSnapshotV2` 经同一 strict state codec restore；malformed state typed fail-loud |
| `runtime.py::_dispatch_tick_to_vnpy_algos/_dispatch_timer_to_vnpy_algos` | 按 runtime list + algo family 分支调用 core | K2 exact event owner routing 和 per-algo delivery 是唯一新路径 |
| `runtime.py::_handle_vnpy_actions` | state 写入与 submit/cancel side effect 分属不同边界 | K3 只返回 `AlgoTransitionV1`；K2 materializer/outbox transaction 持有全部 effect |
| `runtime.py::_defer_dependent_buy_action_if_needed/_try_release_deferred_buys_after_sell_trade` | 在 legacy algo metadata 保存待释放 BUY，SELL trade 后读取 `qmt_strategy_ledger.virtual_account.cash` 并直接 submit，SELL terminal/EOD 时显式阻断或 residual | 这是跨 parent/slot 的 execution coordination，不属于 Sniper/BestLimit/TWAP；K3 只做 zero-write inventory 和边界测试，K6 cutover 前必须有独立 durable contract/repository/ACCOUNT-TRADE-EOD event closure |
| `runtime.py::record_order_event/record_trade_event` | callback 直接更新 core，再递归处理 action | K2 callback ingress→delivery→transition→outbox 是唯一新路径 |
| `b0_quote_v2.py` | 将 normalized quote 投影成 `VnpyTick` 后调用具体 runtime branch | K3 只消费同一 B0 `MarketDataViewV2`/lineage；B0 不知道 algo code |
| `plugin_manifests.py` | current-three descriptor 的 process factory 仍解析为 legacy core class | K3 factory 必须返回真实 `ExecutionAlgoPluginV2`，descriptor/process binding/readback 共用同一 authority |

### 1.2 已有可复用 authority

K3 必须直接复用而不得复制：

- K1 `ExecutionAlgoPluginManifestV2`、`AlgoStartContextV1`、`AlgoStateSnapshotV2`、`RuntimeEventEnvelopeV2`、`AlgoReadOnlyServicesV1`、`AlgoTransitionV1`、`BrokerCommandV2`、`TimerMutationV1`、`DiagnosticObservationV1`；
- K1 current-three exact config schema、v2 state characterization/source authority、source attribution、market-data requirements、`LegacyVnpyPolicyProjectionV1`、deterministic context 和 `best_limit_quantity_v1`；K3 v3 state只通过§5.1.1显式版本演进，不原地改变v2 readback；
- K2 `KernelAlgoCreationCoordinatorV1`、`KernelIngressCoordinatorV1`、`KernelDeliveryWorkerV1`、`materialize_*_transition_v1`、`ExchangeSessionClockV1`、`KernelOutboxDispatcherV1/RecoveryV1/ReconcilerV1` 和唯一 PostgreSQL repository façade；
- B0 `MarketDataViewV2` 与 native continuous L1 lineage；
- 现有 OMS/Gateway、board-lot、price tick、T+1、limit/suspend、cash/risk/admission 既有决定；
- 现有 dependent-BUY 的业务语义只作为 legacy observation authority 保留。K2 当前 `OMSPreflightDecisionV1` 仅有 `PASS/REJECT`，`KernelProjectionTypeV1`、`AlgoTransitionV1`、mapping/outbox 和 repository 均没有 durable deferred-command owner，因此 K3 不得宣称可直接复用一个不存在的 K2 authority。

K3 不读取 StrategyPackage、模型代码、alpha signal、选股 artifact、回测结果或包内资产，也不执行第二次策略包完整性校验。运行时仅验证当前 transition 所需的 event/state/market-data/OMS/Gateway/repository contracts。

### 1.3 Knowledge graph and live-source boundary

当前 worktree 不含 `.understand-anything/knowledge-graph.json`，因此本设计使用 exact-symbol `rg` 定向核对 live source；没有把缺失或陈旧图谱当作 authority，也没有扩大到无关模块扫描。

## 2. Scope and Non-Goals / 范围与非目标

### 2.1 K3 范围

- 三个 current-three plugin 的 initialize/restore/transition 完整实现；
- plugin manifest/version/source/factory binding 的 exact writer/readback 更新；
- Sniper、BestLimit、TWAP Lite 的 TICK/ORDER/TRADE/SESSION/EOD，及 TWAP TIMER 行为；
- active order、traded quantity、market-data lineage、timer occurrence、diagnostic 和 terminal outcome 闭包；
- legacy policy/state read-only inventory；
- dependent-BUY legacy coordinator 的 read-only inventory、算法/协调器职责分界和 K6 cutover blocker evidence；
- before/after normalized trace parity 与 immutable receipt；
- restart/replay、duplicate/out-of-order、same-symbol multi-slot、N=1/N>1 和 failure isolation；
- K6 可消费的算法 cutover/rollback evidence，以及未闭合 execution-coordination prerequisite 的显式清单；
- direct tests、coverage、changed-files routing、F2 validator 和 DESIGN-COMPLIANCE-001。

### 2.2 非目标

- 不实现 K4 `VnpyAlgoEngineFacadeV1` 或扩大 pinned vn.py method surface；
- 不实现 Iceberg/Stop；
- 不在 K3 合入时激活产品 runtime；
- 不删除 legacy product helper；其原子退役属于 K6；
- 不新增数据库表、列、CHECK、index 或 migration；K3 使用已合入 K2 schema；
- 不把 dependent-BUY 的 required cash、sell dependency、defer/release/block/EOD 状态塞入三个算法 state、diagnostic metadata 或普通 OMS `REJECT`；这些载体都不是 durable coordinator authority；
- 不执行生产 DDL/DML、配置/binding 变更、broker 调用或服务控制；
- 不改变 Selection/Target、side、quantity、asset、A 股规则、B0、OMS、Gateway 或 broker route；
- parity evidence 必须由真实 legacy oracle、production plugin/K2 public seams、完整 positive/negative vectors 和 strict writer/readback 生成；任何缺失证据均保持失败可见。

### 2.3 K3/K4/K6 边界

| stage | owns | explicitly does not own |
| --- | --- | --- |
| K3 | current-three native pure plugin behavior、parity、inventory；dependent-BUY legacy coordinator zero-write inventory | vn.py public façade、dependent-BUY durable coordinator、生产 cutover、legacy retirement |
| K4 | pinned vn.py API/DTO/return/error façade compatibility | 第二 runtime/EventEngine/OMS/Gateway、算法业务语义重写 |
| K6 | dependent-BUY durable execution-coordination 详细设计/实现、production DDL/config/restart 独立授权、唯一产品 route cutover、legacy helper retirement、真实 SIM observation | 新算法逻辑、把协调器下沉进 plugin 或临时双路 fallback |

## 3. Target Architecture and Files / 目标架构与文件

### 3.1 唯一 K3 调用链

```text
K2 RuntimeEventEnvelopeV2
  -> exact owner AlgoEventDeliveryV1
  -> KernelDeliveryWorkerV1
  -> catalog descriptor + process binding
  -> current-three ExecutionAlgoPluginV2
  -> AlgoTransitionV1
  -> K2 materialize_applied/failure/skip_transition_v1
  -> state + timer + diagnostics + command mapping/outbox (one transaction)
  -> K2 dispatcher/reconciler
```

K3 plugin 不持有 repository、connection、Gateway、OMS、clock、scheduler、B0 controller、HTTP client、thread、task 或 mutable process-global state。每次 initialize/transition 使用 transition-scoped plugin object；相同 immutable inputs 必须得到 byte-identical state/effect hashes。

上述调用链只拥有单个 parent algo 的 state/effect。SELL proceeds 对多个 BUY parent 的释放属于 kernel 外层 execution coordinator；在 K6 durable coordinator 合入前，产品 route 不得切换到 K2/K3。算法 plugin 不能读取其他 algo state、策略 ledger 或全局现金，也不能根据 `SELL_PROCEEDS_REQUIRED` 自行重试。

### 3.2 目标文件

K3-A production target：

```text
backend/execution_algos/vnpy_style/plugin_base.py
backend/execution_algos/vnpy_style/sniper_plugin.py
backend/execution_algos/vnpy_style/best_limit_plugin.py
backend/execution_algos/vnpy_style/twap_lite_plugin.py
backend/execution_algos/vnpy_style/plugin_factories.py
backend/execution_algos/vnpy_style/plugin_manifests.py
backend/services/miniqmt_execution_runtime/plugin_contracts.py # additive public transition-id authority only
backend/services/miniqmt_execution_runtime/kernel_materializer.py # v3 state-to-mapping closure only
```

`plugin_registry.py`必须保持generic且预计零产品diff；K3通过其现有public catalog/binding API验收3.0.0 manifests。若direct RED证明generic registry合同自身有缺陷，必须单独登记BUG或先更新本设计的明确write scope，不能以“顺手补丁”扩大K3。

K3-B production/validation target：

```text
backend/services/miniqmt_execution_runtime/kernel_current_three_parity.py
backend/services/miniqmt_execution_runtime/kernel_current_three_inventory.py
scripts/miniqmt_current_three_inventory.py
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py
backend/tests/miniqmt_execution_runtime/test_current_three_legacy_inventory.py
```

`sniper_core.py/best_limit_core.py/twap_lite_core.py` 在 K3 仅作为 committed characterization oracle，由 tests/parity harness source-isolated 调用；产品 K2 plugin factory 不得返回这些 core，K3 不删除其 attribution/pinned behavior evidence。

### 3.3 Dependency direction

```text
current-three plugins -> K1 plugin contracts + deterministic helpers only
plugin factories -> current-three plugins + frozen descriptor input
K2 delivery -> plugin registry/process binding
parity harness -> legacy characterization oracle + K2 public seams
dependent-BUY inventory -> legacy runtime/repository read-only evidence only

plugins -X-> runtime/client/B0/repository/OMS/Gateway/StrategyPackage/xtquant
kernel -X-> current-three algo_code branches
plugins -X-> dependent-BUY dependency graph / qmt_strategy ledger / cross-parent release
```

Import-boundary tests必须拒绝 direct/alias/dynamic/transitive 导入越权 owner；不得为 K3 新增第二份 denylist authority。

### 3.4 Dependent-BUY cutover prerequisite / 依赖买单切换前置

定向事实表明 legacy 产品语义是：BUY 因同批 SELL 回款不足而 defer；只有 SELL trade 已持久化且 `qmt_strategy_ledger.virtual_account.cash` 的 fresh authoritative readback 足够时才能 release；partial/unfilled/cancelled、ledger authority missing 和 EOD 均必须形成明确 reason/status，且不得依据 runtime JSON 估算现金。该语义由 BUG-528 的真实产品路径持有，不是任何 current-three 算法的 price/quantity/timer 规则。

K2 当前不能无损表达这条链：

1. `OMSPreflightProjectionReceiptV1.decision` 只有 `PASS|REJECT`，没有 durable `WAITING_FOR_CAPITAL`；
2. `AlgoTransitionV1` 只有 broker commands、timer mutations 和 diagnostics，没有 deferred execution effect；
3. command mapping/outbox 只接受准备调用 broker 的 SUBMIT/CANCEL，不能用 PENDING outbox 冒充“尚未获准创建的 BUY command”；
4. diagnostic observation 不是业务 state，不能负责 restart/replay/release；
5. 三个 plugin 都是单 parent owner，不能读取其他 SELL parent、全局现金或 reserved cash。

因此 K6 在产品切换前必须先提交独立 F2 详细设计并实现唯一 `DependentBuyCoordinationV1` authority，至少闭合：dependency identity（runtime/strategy/BUY parent/ordered SELL parents）、required/reserved/released cash、authoritative ledger projection identity/hash、defer/release/block/EOD state machine、SELL TRADE/ORDER terminal/ACCOUNT/EOD event lineage、single-writer/CAS/fence、restart recovery、同 cash 不能重复释放、release 后才创建正常 K2 command/outbox、typed failure、readback、retention/diagnostics、migration/rollback 和 direct/DEV PostgreSQL tests。该 authority 必须位于 execution coordination 层，不得修改算法 config/state schema，不得重做策略包校验、signal/selection、OMS/risk/admission，也不得以人工确认或 fallback 代替自动事件驱动恢复。

K3 的完成仅证明“current-three algorithm migration implemented_verified”；`K3 implemented_verified`、`K6 cutover_ready` 与 `product runtime switched` 必须分开报告。缺少上述 coordinator 时 K6 必须 fail closed 并保持现有唯一 legacy 产品 route，但这不是新增日常业务门禁，而是防止丢失已存在资金因果语义的 source cutover 前置条件。

## 4. Plugin Identity, Version and Binding / 插件身份与绑定

### 4.1 K3 plugin keys

K3 current-three durable keys 固定为：

```text
SNIPER_MINIQMT     -> aistock.vnpy.sniper / 3.0.0
BEST_LIMIT_MINIQMT -> aistock.vnpy.best_limit / 3.0.0
TWAP_LITE_MINIQMT  -> aistock.vnpy.twap_lite / 3.0.0
```

`plugin_id` 保持不变；从 shadow descriptor/core binding 切换为可执行 pure plugin 是 major executable-contract change，因此 `plugin_version` 必须从 `2.0.0` 提升为 `3.0.0`。三个 `state_schema_version` 同步提升为 `sniper_state_v3`、`best_limit_state_v3`、`twap_lite_state_v3`：v2 active-order carrier强制非空broker order ID，无法表达SUBMIT已原子materialize但broker尚未ACK的真实状态；沿用v2将迫使重复下单或伪造broker identity。v3只解决pre-ACK/mapping closure及TWAP terminal-pending状态，不改变price/quantity/timer业务规则，也不覆盖或回写v2 state。

### 4.2 Manifest and source authority

每个 manifest：

- `implementation_ref` 指向 exact plugin class；
- `aistock_files` 覆盖 plugin class、shared plugin base/factory 和实际消费的 deterministic helper；
- upstream repo/commit/source/license 与 K1-C pinned authority不变；
- config/market-data/subscription/capability schema不变；state采用§5.1.1定义的v3 exact schema；
- manifest hash、descriptor hash、catalog hash、creation binding hash 和 route compatibility receipt全部重算；
- `2.0.0` 与 `3.0.0` 不得共享 manifest/catalog identity。

### 4.3 Durable descriptor and process binding

Durable descriptor 与 live callable 继续分层：

```text
manifest.implementation_ref = exact plugin class ref
factory_binding_id          = aistock.vnpy.<algo>.factory.v3
factory_callable_ref        = plugin_factories:create_<algo>_plugin_v3
config_validator_binding_id = existing current-three strict config authority
state_codec_binding_id      = aistock.vnpy.<algo>.state.v3
```

factory exact contract为 `factory(canonical_plugin_config) -> ExecutionAlgoPluginV2`。返回对象必须持有与 descriptor byte-identical 的 manifest，并真实实现 `initialize/restore_state/transition`。factory ref/signature/source drift 产生 `MINIQMT_K3_PLUGIN_BINDING_INVALID`，阻止整个 current-three catalog publication；不得返回 legacy core、adapter、fixture 或 partial catalog。

### 4.4 Construction dependency closure

为避免 manifest/factory/plugin 循环导入，文件依赖固定为：

```text
plugin classes -> K1 contracts/deterministic helpers only
plugin_manifests.py -> frozen schema/source/ref/signature literals only
plugin_factories.py -> plugin classes + plugin_manifests.current_three_manifests_v3
plugin_registry.py caller -> descriptors from plugin_manifests
                           + live bindings from plugin_factories
```

`plugin_manifests.py` 不导入 plugin class或live factory；descriptor只保存class/factory的冻结ref/signature/source facts。三个external factory仍只有一个参数，内部按algo code从 `current_three_manifests_v3()` strict取得唯一manifest，再调用 `PluginClass(manifest=manifest, canonical_config=config)`。plugin constructor本身不公开为catalog factory。missing/duplicate manifest、class/manifest algo mismatch、config output drift、factory返回非protocol对象均在catalog build阶段aggregate fail，零catalog publication。

## 5. Contracts and Common State/Event Semantics / 契约与公共状态事件语义

### 5.0 Transition/effect construction authority

`BrokerCommandV2`、`TimerMutationV1`和`DiagnosticObservationV1`都要求先持有exact `transition_id`，而`AlgoTransitionReceiptV1`随后又会校验同一identity。K3不得在每个plugin复制私有hash公式，也不得填placeholder再由materializer改写。

K3-A必须把K1既有公式提升为唯一public pure helper：

```text
algo_transition_id_v1(
  delivery_id,
  event_id,
  runtime_id,
  algo_instance_id,
  transition_sequence,
) = "mqtransition_" + hash_hex_v1(
  "miniqmt_algo_transition_identity_v1",
  exact preceding identity payload,
)
```

`AlgoTransitionReceiptV1.create`、三个plugin/shared plugin base、tests和K2 materializer预检全部调用该helper；原inline公式删除，禁止第二实现。plugin构造顺序固定为：strict input/state readback → next state → public transition ID → ordered commands/timers/diagnostics → `effect_set_sha256` → `AlgoTransitionV1`。ordinal在各effect类别内由业务表规定且identity唯一；same transition retry得到byte-identical ID/effect set。helper参数类型/空值、caller-supplied wrong ID、same identity/different payload和writer/readback drift均需直接负测。

### 5.1 Initialize

三个 plugin 的 `initialize(AlgoStartContextV1)` 必须：

1. strict-readback manifest、plugin config、frozen parent/slot/symbol/side/quantity、contract/account/market refs；
2. 创建 K1 exact common state：`RUNNING`、`traded_quantity=0`、`traded_price_decimal=0`、空 active orders、null market lineage；
3. 使用 frozen `pricetick/min_volume/volume_increment`，不硬编码 `0.01/100`；
4. 只返回 `AlgoInitializationV1`，不写 DB、不调用 broker；
5. Sniper/BestLimit 初始 command/timer为空；TWAP 创建唯一下一 exchange-active second one-shot timer；
6. 任一 config/context/state closure失败生成 deterministic initialization failure receipt，零 state/effect commit、零 broker side effect。

### 5.1.1 V3 active-order and mapping closure

三个v3 state共用唯一 `CurrentThreeActiveOrderStateV3`：

```text
local_vt_orderid, submit_command_id
broker_order_id|null
symbol, side
status=COMMAND_PENDING|SUBMITTED|PARTIALLY_FILLED|CANCEL_PENDING|OUTCOME_UNKNOWN
requested_price_decimal, requested_quantity
cumulative_filled_quantity, remaining_quantity
last_order_event_id|null, last_trade_event_id|null
market_data_lineage
mapping_payload_sha256
```

`mapping_payload_sha256 = hash_hex_v1("miniqmt_plugin_active_order_state_v3", exact preceding fields)`。state items按local id排序且唯一；`vt_orderid`/BestLimit order price继续精确引用唯一item。`COMMAND_PENDING`必须broker ID为null、cumulative fill为0、remaining=requested；其它状态必须有exact broker ID。terminal broker mapping不得留在active state。

SUBMIT transition在同一K2 transaction中同时形成v3 `COMMAND_PENDING` state、`ExecutionCommandChildMappingV1.RESERVED`和`BrokerCommandOutboxV1.PENDING`，三者local/command/parent/symbol/side/price/quantity必须byte-exact。后续ACK/ORDER/TRADE/reconcile event才把state推进为broker-identified状态；禁止根据local ID猜broker ID。`kernel_materializer.py`必须使用K2 mapping projection authority逐item验证：

- new SUBMIT item ↔ same-transition RESERVED mapping/outbox；
- existing COMMAND_PENDING item ↔ durable RESERVED/DISPATCHING，或尚未有ORDER/RECONCILE delivery应用到plugin state的BROKER_ACCEPTED mapping；后者保留显式state lag且仍禁止新command；
- broker-identified item ↔ durable BROKER_ACCEPTED mapping和exact broker ID；
- terminal mapping不得留在state，active mapping不得被state静默丢弃；
- same local/command identity任一payload drift终止整个transition transaction。

因此下一TICK/TIMER看到COMMAND_PENDING时不得再SUBMIT。需要cancel但尚无broker ID时写`K3_COMMAND_PENDING_CANCEL_WAIT`并等待真实callback/reconcile；有broker ID后才生成exact CANCEL。EOD/expiry同样保留STOPPED nonterminal state直至pending submit获得可取消/terminal事实，不得用空cancel或假terminal越过broker uncertainty。

### 5.2 TICK

- plugin 只接收 manifest 声明一侧的 native continuous L1 fields；
- 合法 TICK 总是更新 `last_tick_lineage`；TWAP 同步更新 `last_market_data_lineage`；
- 当前 observation 暂缺时 state保持不变，写 `WAITING_FOR_MARKET_DATA` diagnostic且零 command；这不是新 algo status或人工门禁；
- 已提供但非法/冲突 observation在 plugin 前失败，delivery不假 ACK；
- OPEN_AUCTION/CLOSE_AUCTION/LUNCH_BREAK/CLOSED 不触发 current-three submit；禁止从 minute/last/cache/另一侧报价合成字段。

### 5.3 ORDER and TRADE

- 事件必须通过 K2 command-child mapping 唯一命中 `local_vt_orderid`；missing/multiple/cross-parent/cross-slot typed terminal conflict；
- ORDER 只更新broker/status projection，不增加parent traded quantity。nonterminal ORDER累计量小于当前state时视为stale并写diagnostic；大于已应用TRADE总量时保留原active cumulative并写`K3_ORDER_AHEAD_OF_TRADE_PENDING`，raw durable event保留observed value，不能提前计成交或丢弃异常；terminal order从active tuple移除，历史留在 command/child/order facts；
- TRADE 按 exact trade identity一次应用，使用canonical Decimal计算`new_notional = previous_traded_price * previous_traded_quantity + trade_price * trade_quantity`和加权`traded_price_decimal`，同步推进命中active order的cumulative/remaining；top-level traded quantity单调不降且不超过 parent，duplicate trade不重复累计；
- cumulative fill、active child fill sum、parent traded和remaining quantity必须闭合；
- traded达到parent quantity时 terminal outcome=`FILLED`，active orders必须已CLEAN；否则先生成exact cancel commands并等待callback；
- callback transition不得直接调用 Gateway或递归执行另一插件方法。

### 5.4 SESSION and EOD

- lunch/非连续阶段不累计 TWAP timer、不提交 child；state不靠wall clock自动改变；
- EOD 若无 active child，未完成数量以 `EXPIRED_WITH_RESIDUAL`终结；
- EOD 若有 active child，第一 transition 取消 timers、按 local id排序对已有broker ID的child生成 exact CANCEL commands；COMMAND_PENDING只记录等待事实；state进入 `STOPPED` + `K3_EOD_CANCEL_PENDING` diagnostic，terminal outcome为null；
- 全部 child terminal后的 ORDER/TRADE callback transition 才返回 `EXPIRED_WITH_RESIDUAL`；
- CANCEL outcome unknown 经 K2 reconcile后仍无法闭合时走既有 `FAILED + OUTCOME_UNKNOWN` active-child contract，不伪报 residual terminal；
- 不新增人工 reconcile、acknowledge 或无限等待状态。

## 6. Sniper Exact Behavior / Sniper 精确行为

### 6.1 BUY

- 无 active order且`ask_price_1 <= limit_price`时提交；
- quantity=`min(parent_remaining, strict ask_volume_1)`；quantity<=0时零 command并写 visible diagnostic；
- price始终为 frozen limit price，不使用 ask price代替；
- 有 active order时，任何下一合法 TICK只生成该 order的exact CANCEL并返回，不同一transition re-submit。

### 6.2 SELL

- 无 active order且`bid_price_1 >= limit_price`时提交；
- quantity=`min(parent_remaining, strict bid_volume_1)`；
- price始终为 frozen limit price；
- active order cancellation与BUY相同。

### 6.3 State and terminal

`vt_orderid` 为null或唯一 active order id；active child requested price必须等于 frozen limit price。ORDER terminal清空该字段。TRADE partial写 `SNIPER_TRADE_PARTIAL` diagnostic；full fill只在quantity闭合并且active child CLEAN后终结。

## 7. BestLimit Exact Behavior / BestLimit 精确行为

### 7.1 Quote and replace

- BUY无active order时以 `bid_price_1` 提交；SELL以 `ask_price_1` 提交；
- active order price等于当前same-side best price时零 command；COMMAND_PENDING/CANCEL_PENDING/OUTCOME_UNKNOWN一律不re-submit；
- price变化时只生成 exact CANCEL；replacement 等 terminal ORDER callback后由下一合法 TICK生成；
- 不将对手盘、last price、minute bar或旧cache作为best price。

### 7.2 Deterministic quantity

```text
draw_quantity = best_limit_quantity_v1(
  context=DeterministicExecutionContextV1,
  draw_ordinal=state.next_draw_ordinal,
  min_volume=config.min_volume,
  max_volume=config.max_volume,
)
submit_quantity = min(draw_quantity, parent_remaining)
```

只有 materialized SUBMIT command 才把 `next_draw_ordinal` 加一；WAIT/CANCEL/duplicate/retry不消耗ordinal。retry/restart对同 transition/ordinal得到相同quantity、command id和effect hash；禁止调用 `_default_uniform`、global random或重新抽签。

### 7.3 State

`vt_orderid/order_price_decimal` 同时为null或同时引用唯一 active order；requested price、symbol、side、parent、market lineage全部闭合。partial/full fill语义与§5.3一致。

## 8. TWAP Lite Exact Behavior / TWAP 精确行为

### 8.1 Timer schedule

- `duration_seconds=config.time`、`interval_seconds=config.interval`，均为 strict exchange-active seconds；
- initialize 创建 due于下一 exchange-active second的 `TWAP_ACTIVE_SECOND` one-shot；
- 每个首次 APPLIED TIMER occurrence令 `active_elapsed_seconds += 1`、`interval_elapsed_seconds += 1`，并安排下一 active second；
- 午休、auction、closed阶段无 occurrence，PM顺延且无catch-up burst；
- duplicate/replayed occurrence以 `last_timer_occurrence_id` 去重，不重复计时或发单。

### 8.2 Duration boundary

与现有 core 顺序一致：TIMER 先推进active elapsed；当其达到duration时立即停止进一步slice。无active child则state=`FINISHED`并返回 `EXPIRED_WITH_RESIDUAL`；有active child则state=`STOPPED`、terminal outcome=null，并按§5.4两阶段EOD/expiry closure取消或等待COMMAND_PENDING callback。v3 state codec必须允许`active_elapsed_seconds==duration && status=STOPPED && active_orders非空`，只允许无active child时为`FINISHED`；不得在duration边界额外提交最后一单。

### 8.3 Slice boundary

当interval尚未达到时零 broker command。达到时：

1. `interval_elapsed_seconds` 归零；
2. 读取 state中与common `last_tick_lineage` exact相等的 durable latest view；
3. 无合法view时写 `TWAP_WAITING_FOR_TICK` diagnostic并继续下一timer；
4. 若存在COMMAND_PENDING/CANCEL_PENDING/OUTCOME_UNKNOWN，写visible wait diagnostic且本次不生成新slice；否则按local id排序对全部active orders生成CANCEL；
5. `slice_quantity=min(order_volume,parent_remaining)`；为0时写 `TWAP_SLICE_VOLUME_ROUNDED_ZERO`，零SUBMIT；
6. BUY仅在`ask_price_1 <= limit_price`时以limit price提交；SELL仅在`bid_price_1 >= limit_price`时以limit price提交；
7. CANCEL与SUBMIT可在同一 transition 形成有序commands，ordinal固定为全部CANCEL在前、唯一SUBMIT在后；K2 outbox依序处理，不绕过unknown/callback语义。

### 8.4 Restart

restore必须保留duration/interval/order_volume、两个elapsed counters、last timer occurrence、last market lineage、active orders和traded quantity。restart从下一durable due继续，不重放已应用second，不读取wall clock，不用legacy `timer_iterations`补时间。

## 9. Legacy Policy and State Inventory / 历史投影

### 9.1 `CurrentThreeLegacyStateInventoryV1`

K3-B新增read-only strict carrier：

```text
schema_version=miniqmt_current_three_legacy_state_inventory_v1
runtime_id, trade_date, legacy_algo_instance_id
parent_intent_id, strategy_slot_id, symbol, side, target_quantity
algo_code
legacy_metadata_sha256, legacy_state_sha256|null
ordered_child_fact_refs, child_fact_set_sha256
ordered_order_event_refs, order_event_set_sha256
ordered_trade_event_refs, trade_event_set_sha256
legacy_policy_projection_receipt_sha256
candidate_plugin_key|null
candidate_plugin_config_sha256|null
candidate_state_schema_version|null
candidate_state_sha256|null
dependent_buy_coordination_ref|null
ordered_failures
disposition
observation_only=true
runtime_effect_applied=false
inventory_sha256
```

`disposition`只允许：

- `TERMINAL_NO_WRITE`：legacy instance已terminal，只生成candidate evidence；
- `ACTIVE_LEGACY_OWNER`：active/open-child继续旧owner至terminal；
- `SESSION_BOUNDARY_ELIGIBLE`：无active command/child且所有legacy state/timer evidence闭合，仅表示K6可迁移，不在K3写入；
- `INVALID_VISIBLE`：配置/state/identity/evidence不闭合。

`inventory_sha256 = hash_hex_v1("miniqmt_current_three_legacy_state_inventory_v1", exact preceding fields)`。failures使用K1 bounded immutable evidence authority，最多256项，截断保留前255项和唯一omitted-set marker/hash。K3不修改legacy row、不伪造ALGO_START/delivery/transition/outbox，也不自动把active instance改成3.0.0。存在 dependent-BUY metadata/contract/history 时 `dependent_buy_coordination_ref` 必须存在；不存在时必须为null，禁止用空object表示“已检查”。

`ordered_child_fact_refs/ordered_order_event_refs/ordered_trade_event_refs` 的每个ref exact fields为 `identity,payload_sha256,logical_time_utc`，分别按identity排序；ref hash domain为 `miniqmt_current_three_legacy_evidence_ref_v1`，set hash domain分别为 `miniqmt_current_three_legacy_child_fact_set_v1`、`miniqmt_current_three_legacy_order_event_set_v1`、`miniqmt_current_three_legacy_trade_event_set_v1`。同identity不同hash/time立即`INVALID_VISIBLE`，不能保留任意一个版本。

### 9.2 `CurrentThreeDependentBuyInventoryV1`

该 carrier 只刻画 legacy coordinator，不是新 runtime state：

```text
schema_version=miniqmt_current_three_dependent_buy_inventory_v1
runtime_id, buy_algo_instance_id, buy_parent_intent_id
strategy_id|null
ordered_sell_parent_intent_ids
required_cash_decimal|null
observed_status|null
observed_reason_code|null
normalized_status=DEFERRED_WAITING_SELL_PROCEEDS|RELEASED_SUBMITTED|BLOCKED_SELL_PROCEEDS_UNAVAILABLE|EOD_RESIDUAL|INVALID_VISIBLE
raw_metadata_sha256
dependent_buy_contract_sha256|null
dependent_buy_action_sha256|null
ledger_authority_source|null
ledger_observation_context_sha256|null
released_child_order_id|null
ordered_trigger_event_refs
trigger_event_set_sha256
ordered_failures
evidence_completeness=COMPLETE|HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE|INVALID_VISIBLE
observation_only=true
runtime_effect_applied=false
coordination_ref_sha256
```

observation identity domain为`miniqmt_current_three_dependent_buy_inventory_identity_v1`，覆盖runtime/BUY algo/BUY parent/raw metadata hash；有效的strategy/SELL parents属于payload closure而不是构造invalid evidence的前置。`trigger_event_set_sha256`使用`miniqmt_current_three_dependent_buy_trigger_event_set_v1`覆盖ordered event identity/hash/time refs；`coordination_ref_sha256 = hash_hex_v1("miniqmt_current_three_dependent_buy_inventory_v1", exact preceding fields)`。raw metadata、contract、action和ledger observation context分别canonical-hash，不能把当前ledger readback冒充历史release时刻的authority。

conditional closure：

- `COMPLETE`要求strategy、非空SELL parents、positive canonical required cash和合法status/reason闭合；
- DEFERRED不能携带released child；RELEASED必须有released child、action和`ledger_authority_source=qmt_strategy_ledger.virtual_account.cash`；BLOCKED/EOD不得携带released child；
- 现有legacy history没有historical ledger projection identity/hash时，RELEASED/DEFERRED可保留真实business status，但必须标记`HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE`并加入typed failure；不能生成假的projection ref；
- raw字段不能规范化、alias冲突、缺失关键action/contract、同trigger identity不同hash/time时，normalized status必须`INVALID_VISIBLE`、completeness必须`INVALID_VISIBLE`且ordered failures非空；strategy/required cash允许null以保证failure receipt本身可构造；
- `COMPLETE`不得有failure，另外两种completeness必须至少一个failure；failure使用同一K1 bounded/truncation authority。

writer/readback必须从同一read-only repeatable-read snapshot独立重算；不得调用 `_try_release_deferred_buys_after_sell_trade`、不得写legacy metadata、不得提交BUY、不得把`INVALID_VISIBLE`降级为“无依赖”。inventory的存在只说明K6需要迁移对应协调事实，不改变K3 plugin parity结果。

### 9.3 `CurrentThreeLegacyInventorySetV1`

read-only inventory CLI输出集合级strict carrier：

```text
schema_version=miniqmt_current_three_legacy_inventory_set_v1
repository_commit_sha
trade_date
observed_at_database_utc
ordered_inventory_items
inventory_item_set_sha256
total_count
counts_by_disposition
set_sha256
```

items按`(runtime_id,legacy_algo_instance_id)`排序且identity唯一；`inventory_item_set_sha256 = hash_hex_v1("miniqmt_current_three_legacy_inventory_item_set_v1", ordered item identity/hash refs)`；`set_sha256 = hash_hex_v1("miniqmt_current_three_legacy_inventory_set_v1", exact preceding fields)`。`observed_at_database_utc`来自同一read-only repeatable-read transaction的database timestamp，不参与业务cutover identity之外的猜测。writer/readback重算item、counts和set hash。

`scripts/miniqmt_current_three_inventory.py` 只允许 `--read-only --output <explicit-path>`，拒绝缺失output、repo外未授权输入overlay和任何write mode；输出是K3/K6验证artifact，不写业务数据库、不改变release/policy/binding。K6执行前必须从目标数据库重新生成fresh inventory；旧artifact不能作为默认或latest fallback。

### 9.4 Policy projection

必须直接消费 `LegacyVnpyPolicyProjectionV1`：

- `NO_DRIFT/ALIAS_EQUIVALENT` 可形成candidate config；
- `DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION/CONFLICT/INVALID_INPUT_VISIBLE` 只形成visible evidence；
- unknown/control字段继续保留在adapter projection，不进入plugin config；
- inventory不是run admission、人工审批或策略包二次校验。

## 10. Parity Contract / 行为等价合同

### 10.1 `CurrentThreeParityInputV1` and event refs

`CurrentThreeParityEventRefV1` exact fields：

```text
schema_version=miniqmt_current_three_parity_event_ref_v1
step_ordinal
event_id, event_type, event_source
event_payload_sha256
logical_time_utc
market_data_projection_id|null, market_data_projection_sha256|null
account_projection_id|null, account_projection_sha256|null
contract_projection_id|null, contract_projection_sha256|null
event_ref_sha256
```

`step_ordinal`从0严格连续；projection identity/hash必须同时null或同时存在；`event_ref_sha256 = hash_hex_v1("miniqmt_current_three_parity_event_ref_v1", exact preceding fields)`。

`CurrentThreeParityInputV1` exact fields：

```text
schema_version=miniqmt_current_three_parity_input_v1
algo_code
runtime_id, parent_intent_id, strategy_slot_id
symbol, side, target_quantity
limit_price_decimal, pricetick_decimal
min_volume, volume_increment
plugin_config, plugin_config_sha256
legacy_policy_projection_receipt_sha256
execution_coordination_scope=ALGO_LOCAL_ONLY
ordered_event_refs, event_set_sha256
input_sha256
```

`event_set_sha256 = hash_hex_v1("miniqmt_current_three_parity_event_set_v1", ordered event identity/hash refs)`；`input_sha256 = hash_hex_v1("miniqmt_current_three_parity_input_v1", exact preceding fields)`。writer/readback strict重算config、coordination scope、event refs/set和input hash；相同input identity任一business field/hash变化立即conflict。K3 parity只能声明`ALGO_LOCAL_ONLY`，禁止用普通PASSED receipt暗示 dependent-BUY cross-parent coordinator 已等价。

### 10.2 Trace normalization

Legacy oracle和K3 plugin必须消费同一个 `CurrentThreeParityInputV1`：frozen parent/slot/contract/config、ordered immutable events、projection refs、logical time和deterministic context。两侧都禁止Gateway/broker call。

Normalized trace逐step记录：

```text
step_ordinal, event_type, event_payload_sha256, logical_time_utc
state_status, traded_quantity, remaining_quantity
algo_specific_state_projection
ordered_business_effects
ordered_timer_effects
ordered_diagnostic_reason_codes
terminal_outcome|null
```

business effect exact fields为`kind,side,symbol,canonical_price,quantity,cancel_target_ordinal,reason_code,market_data_lineage_sha256`。legacy/K3不同ID格式不直接比较字符串，但必须分别证明event→state/effect owner关系和本侧identity/hash readback；不得通过删除price/quantity/reason/timer/lineage字段伪造等价。

每个 normalized trace step 增加 `step_sha256 = hash_hex_v1("miniqmt_current_three_parity_trace_step_v1", exact preceding step fields)`；`trace_sha256 = hash_hex_v1("miniqmt_current_three_parity_trace_v1", {algo_code,side,ordered_step_ordinal_and_hash_refs})`。step ordinal必须与event ref一一对应，除ALGO_START初始化step外不得增删或重排。legacy/kernel writer使用同一normalizer schema但独立构造，不允许一侧直接复用另一侧输出。

### 10.3 `CurrentThreeParityReceiptV1`

```text
schema_version=miniqmt_current_three_parity_receipt_v1
algo_code
legacy_source_attribution_sha256
plugin_id, plugin_version, plugin_manifest_sha256
plugin_config_sha256
parity_input_sha256
execution_coordination_scope=ALGO_LOCAL_ONLY
ordered_event_refs, event_set_sha256
legacy_trace_sha256
kernel_trace_sha256
ordered_differences
status=PASSED|FAILED
broker_called=false
receipt_sha256
```

PASSED必须differences为空；FAILED必须至少一个difference。difference exact fields为`step_ordinal,field_path,legacy_value_sha256,kernel_value_sha256,reason_code,context_sha256`，按tuple稳定排序，最多256项并使用K1 truncation authority。`receipt_sha256 = hash_hex_v1("miniqmt_current_three_parity_receipt_v1", exact preceding fields)`；writer/readback重算完整receipt。任何 malformed trace/identity/hash产生typed failure，不能返回空PASSED。receipt writer必须验证input的`execution_coordination_scope=ALGO_LOCAL_ONLY`；cross-parent defer/release事件若混入算法trace，必须以`MINIQMT_K3_PARITY_SCOPE_INVALID`拒绝，不能由normalizer删除后判PASSED。

### 10.4 Mandatory parity vectors

Sniper：BUY/SELL未穿价、穿价、depth quantity、active cancel、partial/full fill、duplicate callback、EOD。

BestLimit：BUY/SELL初次quote、same-price no-op、price-change cancel、deterministic draw ordinals、partial/full fill、restart/replay。

TWAP：TICK只更新view、每秒timer、interval slice、午休零累计、PM恢复、duration边界无末单、missing view、zero slice、partial/full fill、restart、EOD active-child closure。

Cross-cut：invalid vs missing quote、board lot/pricetick、same-symbol multi-slot、N=1/N>1、out-of-order event、plugin exception、DB failure zero ACK、no duplicate broker effect；dependent-BUY case必须分别证明(1)算法候选effect的ALGO_LOCAL parity，(2)legacy coordinator inventory非空且不会被plugin消费，禁止将两者合成一个虚假PASSED。

## 11. Transactions, Concurrency and Failure Semantics / 事务与失败

K3不新增transaction owner。所有production transition继续由K2处理：

1. event ingress transaction；
2. per-algo delivery claim/fence；
3. pure plugin invocation；
4. state/timer/diagnostic/command mapping/outbox single commit；
5. external Gateway call outside transaction；
6. ACK/reject/unknown/reconcile transaction。

这六步只适用于已经通过现有 admission 并可合法形成broker command的单algo effect。`SELL_PROCEEDS_REQUIRED`不得被转换为PENDING outbox、timer retry或plugin diagnostic；dependent-BUY durable transaction由K6新增协调合同单独设计，且release成功后才进入上述正常K2 command transaction。

同algo predecessor N未APPLIED/terminal closure时N+1不得执行；不同algo/slot可并行。duplicate delivery重用同transition/effect hashes；same ID/different payload terminal conflict。

Typed K3 reasons至少包括：

```text
MINIQMT_K3_PLUGIN_BINDING_INVALID
MINIQMT_K3_PLUGIN_CONFIG_INVALID
MINIQMT_K3_PLUGIN_STATE_INVALID
MINIQMT_K3_EVENT_UNSUPPORTED
MINIQMT_K3_EVENT_PAYLOAD_INVALID
MINIQMT_K3_MARKET_DATA_LINEAGE_INVALID
MINIQMT_K3_TIMER_LINEAGE_INVALID
MINIQMT_K3_PARITY_DRIFT
MINIQMT_K3_PARITY_SCOPE_INVALID
MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID
MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID
MINIQMT_K3_DEPENDENT_BUY_COORDINATOR_NOT_MIGRATED
MINIQMT_K3_ACTIVE_LEGACY_CUTOVER_FORBIDDEN
```

异常context必须JSON-safe、有界并包含runtime/algo/plugin/event/delivery/transition identity和stage。renderer失败保留primary reason并追加renderer type；禁止 `except Exception: pass`、空结果、默认state、固定ACK或fallback到legacy core。

## 12. Diagnostics, Metrics and Retention / 诊断

- 使用现有 K2 diagnostic observation/repository；K3不新增生产schema；
- 低基数metrics：`algo_code`、`event_type`、`outcome`、`reason_family`、`parity_status`、`inventory_disposition`、`coordination_inventory_status`；
- runtime/algo/event/command IDs只进入diagnostics/log，不进入metric labels；
- parity receipt为CI/validation immutable artifact；若K6正常交易日shadow observation需要持久化，只能引用existing diagnostic evidence和artifact URI/hash，不把大trace塞入metrics；
- K2既有event/transition/outbox/diagnostic retention不变；
- alerts自动clear，不新增人工acknowledge或发布审批。

## 13. Implementation Plan and Slices / 实施方案与切片

### K3-A — Pure plugins, v3 state and bindings（6–9 人日）

- 三个plugin class、shared pure helpers和factories；
- manifest 3.0.0/source/descriptor/process binding/catalog closure；
- v3 state/active-order codec、public transition-id helper和materializer mapping closure；
- initialize/restore/TICK/ORDER/TRADE/SESSION/EOD与TWAP TIMER；
- import boundary、direct behavior、state/config/factory negative matrix；
- 不接产品runtime、不调用Gateway/broker、不修改legacy product path。

### K3-B — Parity, inventory and shadow orchestration（4–6 人日）

- parity input/trace/receipt writer-readback；
- legacy policy/state及dependent-BUY coordinator read-only inventory；
- K2 public creation/ingress/delivery/materializer/outbox seam的broker-neutral shadow run；
- restart/replay/multi-slot/concurrency/failure/equivalence矩阵；
- K6 cutover evidence清单；
- 不执行production cutover。

两个PR都必须独立通过DESIGN-COMPLIANCE-001。K3-A不能被声明为K3 complete；只有K3-B parity/inventory/restart闭合后K3 overall才可标记`implemented_verified`，但产品runtime仍需保持`not_switched`直到K6。

## 14. Verification Plan / 验证方案

### 14.1 Direct tests

```text
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py
backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py
backend/tests/miniqmt_execution_runtime/test_current_three_legacy_inventory.py
backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py
backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py
backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py
backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py
```

每个测试必须调用production plugin factory、K2 public invocation/materializer或parity/inventory writer/readback，并同时覆盖真实成功与失败路径；脱离production seam或不验证业务结果的测试不计入完成证据。

### 14.2 K2 integration tests

- ALGO_START→initial state/timer/command；
- TICK/ORDER/TRADE/TIMER/SESSION/EOD exact routing；
- state/effect transaction rollback；
- SUBMIT→COMMAND_PENDING→ACK/callback/reconcile→broker-identified active state；pending期间tick/timer/EOD不重复submit且不伪造cancel；
- callback-before-ACK、duplicate/out-of-order、stale delivery；
- current-three command→mapping→child→broker callback identity chain；
- no broker duplicate；
- same-symbol multi-slot和bad-plugin isolation。
- dependent-BUY算法候选effect与cross-parent coordinator inventory分离；K3 plugin不得读取ledger/其他parent，也不得制造defer/release state。

### 14.3 Coverage and routing

- 新增/修改 production modules line coverage `>=80%`、branch coverage `>=70%`；
- 不以omit/pragma/skip/xfail排除新K3逻辑；
- changed files严格经`file_ownership.yaml -> module_registry.yaml -> test_plans.yaml`路由；
- 预计K3-A选择`miniqmt_execution_runtime_l2`和`paper_v2_backend`；若共享simulation runtime contract实际变化，再选择`simulation_core_l2`；
- frontend/Go/QE未改则不运行；
- PR classifier必须`unmapped_code_files=[]`且CI实际执行被选择plans。

### 14.4 Acceptance matrix

必须分别证明：

- exact config/state/source/manifest/binding；
- Sniper/BestLimit/TWAP每条业务分支；
- TWAP AM/lunch/PM/EOD/restart；
- parity receipt正负/截断/readback；
- active legacy inventory zero-write；
- no direct broker/import/runtime owner；
- crash/retry/concurrency/no-duplicate；
- signal/selection/package isolation；
- production gates和runtime状态分离。

## 15. Risks, Rollout and Rollback / 风险、发布与回滚

### 15.1 Risks and mandatory mitigations

| risk | mandatory mitigation |
| --- | --- |
| pure plugin再次持有legacy mutable core | factory/return-type/import boundary直接测试；每transition新对象；state只来自strict snapshot |
| parity normalizer隐藏真实差异 | price/quantity/reason/timer/lineage全部进入normalized effect；difference writer/readback和negative vectors |
| dependent-BUY被遗漏或错误塞进算法state | §3.4证明K2 carrier缺口；K3只生成zero-write coordinator inventory；K6切换前必须实现独立durable coordinator，plugins禁止ledger/cross-parent访问 |
| K3被误用为临时产品route | K3-A/K3-B均shadow-only且`broker_called=false`；产品cutover仅K6执行 |
| active legacy instance被in-place升级 | zero-write inventory disposition；active/open-child固定legacy owner至terminal |
| TWAP用wall clock或午休catch-up | K2 ExchangeSessionClock one-shot occurrence；AM/lunch/PM/EOD/restart直接测试 |
| BestLimit restart重新抽签 | raw-digest u53 + persisted ordinal；retry/restart同effect hash |
| EOD有active child却伪报terminal | STOPPED + cancel pending两阶段closure；unknown进入既有FAILED contract |
| pre-ACK active order无法表示 | 三个state schema升v3；COMMAND_PENDING + RESERVED mapping + PENDING outbox同事务闭合；broker ID只来自ACK/callback/reconcile |
| ORDER cumulative先于TRADE造成死锁或双计 | ORDER不增加成交，ahead事实留在durable event+diagnostic；TRADE唯一推进top-level/active cumulative；EOD reconcile显式闭合 |
| 新控制演变为人工门禁 | parity/inventory仅开发/运维证据；无RBAC、审批、acknowledge或per-run switch |

### 15.2 K3 source rollout

1. 合入K3-A pure plugins，不改变产品route；
2. 合入K3-B parity/inventory，不改变产品route；
3. 在CI/DEV disposable schema运行完整parity、restart和K2 integration；
4. K3 source完成只表示current-three算法迁移证据ready，不表示dependent-BUY coordinator或K6 cutover ready，也不表示production DDL、配置、重启或runtime activation；
5. K6先以独立F2设计和代码闭合§3.4 durable coordinator，再在独立授权下执行唯一route切换并退役legacy product calls；两项不能以一次状态声明合并。

不得以永久feature flag、双broker route、默认legacy fallback或人工确认作为rollout。

### 15.3 Source rollback

- K3尚未产品激活时，source rollback只回退K3 plugin/parity代码到最后一个schema-compatible main；
- K2 additive schema和durable facts不删除；
- K3 parity/inventory artifacts保留为历史证据；
- 若未来K6已创建3.0.0 active algo，则不能把active state交给2.0.0 legacy owner；必须从durable facts drain/reconcile至terminal，或部署兼容3.0.0的最后版本；
- 不回写plan、side、quantity、quote/control revision、child/broker facts。

## 16. Production Gates / 生产门禁

本设计阶段：

```text
source_merge=pending_user_review_pr_2816
close_sync=not_applicable_feature_design
production_ddl_gate=noop
production_dml_gate=noop
production_backend_dependency_gate=noop
production_frontend_dependency_gate=noop
production_config_gate=noop
binding_gate=noop
broker_gate=noop
service_restart=noop
runtime_activation=noop
```

这些状态是交付记录，不进入每日模拟盘业务路径，也不是人工审批门禁。

## 17. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-071` | current legacy side-effect chain、dependent-BUY coordinator carrier缺口、K2/K3/K4/K6边界、信号/执行隔离和唯一route事实完整 |
| `F-072` | current-three plugin identity/version/source/manifest/factory/catalog、v3 state和public transition identity writer-readback可直接实施，零partial catalog |
| `F-073` | Sniper BUY/SELL/active-cancel/depth/fill/EOD exact行为与state lineage完整 |
| `F-074` | BestLimit quote/replace/deterministic draw ordinal/restart exact且无global random |
| `F-075` | TWAP exchange-active TIMER、午休、PM、duration、slice、missing view、EOD/restart语义完整 |
| `F-076` | pre-ACK COMMAND_PENDING、ORDER/TRADE因果、active-order/mapping/outbox、traded quantity/terminal/unknown-cancel transaction与identity closure完整 |
| `F-077` | legacy policy/state/dependent-BUY read-only inventory和ALGO_LOCAL immutable parity receipt schema/hash/truncation/readback完整 |
| `F-078` | K2 public seam shadow orchestration无algo branch、无direct broker、无第二runtime/route/fallback，且不把cross-parent coordination塞入plugin |
| `F-079` | typed failure、diagnostics/metrics/retention、concurrency/retry/rollback和无人工门禁完整 |
| `F-080` | direct/parity/restart/integration测试、coverage、changed-file routing、K6 coordinator cutover prerequisite和生产状态分离可执行 |

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-071` | §1–§3.4；`runtime.py::_ensure_vnpy_core/_handle_vnpy_actions/_defer_dependent_buy_action_if_needed/_try_release_deferred_buys_after_sell_trade`定向事实；父蓝图K3/K6 | artifact: `docs/architecture/miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`；target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` route/import/ledger-cross-parent static tests | design_ready | none |
| `F-072` | §3–§5.1.1；target current-three plugin/factory/manifest/catalog、v3 state与public transition-ID modules | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py`、`backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py`、`backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py` | design_ready | none |
| `F-073` | §5–§6 Sniper exact transition table | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py` Sniper positive/negative/restart vectors | design_ready | none |
| `F-074` | §5、§7；`best_limit_quantity_v1`唯一draw authority | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py` deterministic ordinal/retry/restart/price-change vectors | design_ready | none |
| `F-075` | §5、§8；K2 `ExchangeSessionClockV1`/timer occurrence | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py` AM/lunch/PM/duration/EOD/restart vectors | design_ready | none |
| `F-076` | §5.1.1、§5.3–§5.4、§11；K2 callback mapping/materializer/outbox | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py` pre-ACK pending、callback-before-ACK、ORDER-ahead-of-TRADE、fill closure、EOD cancel/unknown/no-duplicate cases | design_ready | none |
| `F-077` | §9–§10 strict policy/state/dependent-BUY inventory与ALGO_LOCAL parity carriers | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_legacy_inventory.py` positive/negative/truncation/readback/scope separation | design_ready | none |
| `F-078` | §3–§3.4、§13；K2 creation/ingress/delivery/materializer public seams | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py`、`backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` shadow/no-branch/no-broker/no-ledger/no-cross-parent tests | design_ready | none |
| `F-079` | §11–§12、§15；typed failure/diagnostics/rollback | target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py` malformed input、plugin-local isolation、DB failure、source rollback cases | design_ready | none |
| `F-080` | §14–§16；ownership/classifier/F2/coverage/gates和K6 prerequisite | target commands `python -m nox -s miniqmt_execution_runtime_l2`、`python -m nox -s paper_v2_backend`及真实依赖plans；line>=80/branch>=70，`unmapped_code_files=[]` | design_ready | none |

## 19. DESIGN-COMPLIANCE-001 / 正式复核

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | 三算法全部event/state/effect/timer/restart/parity/inventory路径均有实施合同；K3-A不被冒充K3 complete；dependent-BUY未被忽略或伪装进普通state/outbox |
| no silent error | pass | malformed config/state/event/binding/parity/inventory均typed fail-loud；无空PASSED、默认state、固定ACK或exception swallowing |
| no business semantic drift | pass | Sniper/BestLimit/TWAP既有price/quantity/reason/timer顺序、A股规则、Selection/Target/B0/OMS/Gateway authority不变；dependent-BUY现有ledger-cash因果语义被列为K6显式前置而非删除；任何算法差异由FAILED parity receipt显式暴露 |
| no unauthorized gates | pass | parity/inventory是开发证据而非run gate；无RBAC、审批、acknowledge、人工恢复、永久enable flag或package二次校验 |
| no parallel route/fallback | pass | K3 shadow两侧均broker_called=false；产品route退役仅由K6一次完成，不双submit、不fallback到legacy |
| production state separation | pass | design/source/DDL/config/binding/restart/runtime observation分别记录；本设计全部production/runtime gates为noop |

## 20. Definition of Done / K3 完成定义

K3 implementation只有同时满足以下条件才可标记`implemented_verified`：

1. F-071..F-080全部具有真实code/test receipt；
2. 三个factory返回真实`ExecutionAlgoPluginV2`，catalog零partial publication；v3 state能无伪造地表示pre-ACK COMMAND_PENDING并与mapping/outbox闭合；
3. current-three ALGO_LOCAL mandatory parity vectors全部PASSED，任一drift保持FAILED而非被normalize隐藏；dependent-BUY coordinator仅形成strict inventory，不计入算法PASSED；
4. TWAP上午/午休/下午/duration/EOD/restart使用真实K2 timer semantics；
5. event→delivery→transition→command→mapping→child→callback链可重建且不重复broker effect；
6. active legacy state只inventory、不in-place cutover；
7. line/branch coverage、changed-file routing、F2 validators、DESIGN-COMPLIANCE-001和required CI通过；
8. K3 source state、K6 dependent-BUY coordinator、production DDL/config/restart/runtime activation明确分离。

即使K3 source已合入，产品runtime仍为`not_switched`。K6必须先完成§3.4独立durable coordinator设计/实现及其DEV/CI验收，再在独立授权和真实SIM observation下完成唯一route cutover；两者缺一不可。
