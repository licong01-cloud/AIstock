# 分钟级交易时机建议与 LocalSIM 执行适配 F2 蓝图

> 版本：v1.0；日期：2026-10-23；Feature tier：F2。
> 状态：`DESIGN_REVIEWED_AWAITING_USER_CONFIRMATION`；本次交付仅为蓝图，详细设计、代码、实验及上线均未执行。
> 源码核查基线：`029e8e4a8a9b18446edca7f439bf333152a5565b`（本任务独立工作树起点）。
> 唯一开发规范：`../standards/aistock_development_standard_v1.5_20260523.md`。
> 范围：独立分钟择时核心、LocalSIM 分钟模拟执行适配、荐股可嵌入提醒；不接实盘，不适配 MiniQMT。

## 1. Background / Scope / Non-goals：目标与边界

### 1.1 用户目标

对用户已确定的持仓股、自选股或荐股名单中的**明确交易计划**，基于当时已经可见的分钟线，给出等待、执行机会、价格条件和分批建议。对 LocalSIM 输出模拟执行决策；对人工使用只展示和提醒，不创建交易订单。

目标是相对简单执行基准改善成本后结果，并评估对收益与回撤的影响；不承诺最优分钟点、必然成交或超额收益。不使用“提醒发生即成交”的口径包装有效性。只有股票名称而没有 BUY/SELL 方向时，可展示行情观察，但不擅自推断交易方向。数量未知时只能给机会提醒，不能生成模拟数量或声称知道剩余仓位。

### 1.2 文档权威与旧研究关系

- 本文是[持仓与自选池总蓝图](position_timing_advice_f2_redesign_20260903.md)的新增日内执行辅助分支，不改写日频方向策略、PT-NEXT 历史实验、个人费用合同或已冻结 artifact。
- [模拟平台上位蓝图](simulation_platform_unified_authoritative_blueprint_20260715.md)及[LocalSIM 分层设计](localsim_legacy_retirement_and_layer_isolation_f2_detailed_design_20260830.md)仍管理 LocalSIM。它们当前限定 TWAP-only；本文提出未来扩展需求，**不覆盖现行限制，也不授权本窗口修改 LocalSIM**。
- 用户已明确本分支只使用分钟线和 LocalSIM。此前关于 MiniQMT 插件/Tick 适配的讨论不作为本项目实现依据；不恢复已退役的 V25 执行算法。
- 原日频研究主要比较同股账户择时与长期持有；本分支主问题是在同一计划下如何执行。两类估计目标不同，不将本分支执行价差并入旧日频 alpha，也不以旧日频阴性结果否决分钟研究。

### 1.3 最小范围

包括：已完成分钟线输入、计划及版本、纯决策核心、历史回放、两种消费接口、最小状态展示、D0/D1/D3/D5 模拟响应对照。基础功能与策略研究相对独立；功能可用不要求先训练模型或先取得显著正收益。

不包括：实盘、MiniQMT/QMT、Tick/盘口/排队优化、自动人工账户交易、重新选股、QE/HMM 融合、多 Agent 融合、新数据库/OMS/账本/调度平台/通用插件市场、监控平台、全历史原始 payload 留存。不是把上述内容改名后放进适配器。

## 2. Architecture：现有基础、缺口与复用

### 2.1 核查事实，不把存在文件等同于上线

以下路径均相对本仓库，符号与源码提交用于定位；本次没有运行策略、查询数据库或确认生产模块激活状态。

| 资产 | 已核实的源码语义 | 复用/缺口 |
|---|---|---|
| `backend/data_service/tdx_adapter.py::fetch_minute_kline_tdx` | 调用 `/api/kline-all/tdx`，显式 start/end date；价格厘转元，volume 为手；校验时间/OHLCV/重复 | 可复用读取；不能据此保证末根已完成或盘中更新时延 |
| `tdx-api-main/web/server_api_extended.go::handleGetKlineAllTDX` | 日期有界调用 `GetKlineUntil`，过滤目标范围 | 不新增抓取链路，不把 API 名称中的 all 误读为每次返回全历史 |
| `backend/services/simulation_data/minute_provider.py` | `load_observed_intraday` / `load_new_bars`；TDX 无静默历史 fallback，volume 手转股 | LocalSIM 复用本 owner 的数据适配；择时端只借用已验证纯转换或稳定读接口，不改 producer |
| `backend/services/simulation_execution/localsim/runtime.py::LocalSimBackend` | 分钟执行、增量状态、OMS/ledger；支持 TDX、历史 DB、冻结文件输入 | 复用 LocalSIM，不另建账户与撮合系统；本轮历史研究优先冻结文件，不要求生产 DB |
| `backend/services/simulation_execution/localsim/planning.py::LocalSimPlanner` | `assert_plan_uses_twap` 确实拒绝非 TWAP；`attach_causality_cursor` 冻结执行游标 | 当前不能零改动切新算法；由 LocalSIM owner 实施显式策略扩展 |
| `backend/services/trading_core/execution_algo_adapter.py`、`minute_execution.py` | `create_state/compute_step` 将 `StepResult` 转 `StepFill`，由现有引擎应用成交 | 这不是纯“建议”接口。新 adapter 要区分决策时间与后续成交时间，不能把机会直接转成交 |
| `backend/execution_algos/base_algo.py`、`twap_algo.py` | `init_order/compute_step/is_complete`；现 TWAP 使用当前 bar close，逐步更新状态 | 只参考协议并保留旧行为；不能认为算法名 TWAP 证明均匀覆盖整日或使用下一分钟 open |
| `backend/services/trading_core/ledger.py::FeeModel` | 当前基础公式为合计买/卖费率后取最低费用，不是净佣最低额加各项规费 | 复用账本、使用明确 cost identity；不宣称与择时个人费用数值一致 |
| `backend/routers/position_timing.py` | 已有 cards、alerts/poll、alerts/claim API | 可复用提醒展示和幂等思想；旧日频冻结触发不是新的分钟信号 |
| `backend/services/position_timing/minute_execution_pipeline.py` | 旧 L4b-1 为冻结卡片与分钟 candidate 的离线窗口反事实；无在线适配 | 可参考不可变研究产物方式，不改其合同或冒充本项目已完成 |
| `backend/services/advisory_model_first/generic_daily_price_api_service_v1.py` | 固定五交易日价集，`NAVIGATION_ONLY`、`economic_confirmation=false` | 价格字段是可选输入，不自动变成硬限制、卖价模型或获利保证 |

2026-10-23 约01:02（北京时间）前序只读检查：本地 TDX health 为 healthy；000001.SZ、600000.SH、688981.SH 的2026-10-22请求各返回240根、09:31至15:00。**只是夜间连通/已结束交易日返回事实，不是盘中逐分钟新鲜度验收**；本蓝图不把它当数据 SLA，也不据此要求等待下一交易日才可研发。

### 2.2 一核心、两个适配层

```text
明确交易计划 + 可选价格约束 + 已完成/已可见分钟线
                         ↓
        position_timing 独立纯分钟决策核心
       WAIT / OPPORTUNITY / EXPIRED / UNAVAILABLE
                  ↙                    ↘
       LocalSIM 分钟适配器          提醒 API + 可嵌入组件
       延迟/成交模拟/确认反馈        页面状态/弹窗，无交易权限
                  ↓
        LocalSIM 既有账户与绩效
```

核心建议置于 `backend/services/position_timing/intraday/`（拟新增），不单独部署。核心不导入 FastAPI、TDX、数据库、荐股仓库或 LocalSIM 运行对象；只消费显式输入、输出新状态与决策。模型推理以后通过显式函数/权重对象注入，不改变消费接口。最初只使用少量固定实现，不建热加载平台。

依赖方向：LocalSIM adapter → 纯择时核心；荐股组件 → 择时 HTTP API；择时 HTTP service → 自有计划/状态和既有只读数据能力。核心不反向依赖消费者。接口能力不等于必须建立两个 HTTP 服务，LocalSIM 优先进程内调用，提醒使用 HTTP。

### 2.3 模块 owner 与写入边界

| Owner | 后续职责 | 禁止混入 |
|---|---|---|
| position_timing | 核心/自身 API/自有组件/冻结参数与研究报告 | 修改 LocalSIM 代码或表、荐股原名单、QE/HMM/N0、共享 guard 默认值 |
| LocalSIM 窗口 | 自身上位设计更新、TWAP-only 扩展、注册与策略适配、延迟成交、状态/账户反馈 | 静默解除旧限制、覆盖历史 release、恢复 V25、调用提醒 API 充当成交 |
| Advisory 窗口 | 传递原计划及价集来源、挂载组件、同步计划撤销/修订 | 复制分钟判断逻辑、重排名单、将提醒当真实交易 |
| 数据窗口 | 既有 TDX/冻结文件缺口修复需求 | 择时窗口临时补库、改 dataset 或生成假的分钟数据 |

LocalSIM 接口扩展未交付时，核心回放和提醒仍可先验收；整体“两接口交付”不能提前报完成。不得为绕过依赖另建模拟账本。各窗口任务由用户转发，本次不通过协调通道启动跨模块开发。

## 3. Contracts：数据、决策和两种消费合同

### 3.1 最小共同输入输出

命名为蓝图提案，第一阶段详细设计将其固化为实际 schema；不声称这些 API 已存在。

| 对象 | 最小内容与含义 |
|---|---|
| Plan | `plan_id/revision/source_ref`、股票、BUY/SELL、计划时间/有效期、目标规模（模拟必填）、允许分批、价格条件、父计划身份 |
| ObservedBars | 交易所时区、bar 起止、OHLCV/amount 单位、`available_at/received_at` 或历史可见时间假设、数据源/快照身份 |
| ExecutionView | 模拟确认成交量、待执行保留量、资金/可卖数量及来源；人工无反馈为 UNKNOWN，不填0冒充没成交 |
| Policy | 策略代码/版本、显式参数/hash；未来模型权重/特征/训练截止 hash；不读取可变“最新最好模型” |
| Decision | `decision_id/plan_revision/as_of/policy_hash/input_ref`、状态、建议批次/规模、价格条件、有效期、typed reason |

每个计划固定一个有效策略版本；换策略/修改计划形成新 revision，旧 revision 不再发新机会。方向与总规模是上游 authority，核心不得放大或反转。`decision_id` 来自计划版本、策略、所用输入/状态身份与有效决策时钟事件（含到期事件）；相同输入 exact retry 结果相同。并发修订应基于预期 revision 检查，旧响应不能重新激活被取消计划。计划生命周期 ACTIVE/CANCELLED/EXPIRED/COMPLETED 与机会状态分开；COMPLETED 只由明确完成反馈确定，不能由提醒次数确定。机会边沿身份与决策身份分开，时间刷新不能使持续成立的同一机会每分钟重新弹窗。

机会不等于完成：只有 LocalSIM 确认成交/取消反馈改变模拟 executed/reserved 状态。人工端在未知成交状态下不连续推送依赖“上一批已买入”的下一批；可持续显示当前机会，用户修改计划或提供明确进度后再推进，第一版不建设真实账户成交采集系统。

### 3.2 因果与分钟数据

1. 核心只见已完成分钟记录；有效可见时间至少为 `max(bar_end, source_available_at（如有）, received_at)`，不得晚于 decision_as_of。没有source发布时刻不能猜成0，线上以实际收到时间及闭合规则计算；不能把时间戳等于当前分钟当成闭合证明。1分钟聚合为5分钟时，只使用完整的已结束5分钟组，午休/缺bar不补造。
2. 历史文件可以完整载入适配器，但传给核心的只能是当时前缀；全日最高最低、未来成交量曲线、未来bar不得进入当时特征或状态。回放的 available_at 若没有原始采集证据，显式标为假设，不声称重建真实历史网络延迟。
3. TDX 元/厘、手/股和时间戳由明确适配合同转换一次。提醒可执行价格使用 raw CNY；使用复权特征时记录转换版本，不将旧 Qlib 虚拟复权单位直接作为 LocalSIM 股票数量。
4. 新鲜度基于交易时段和最近应完成 bar；午休/休市不计为应到而缺失的交易bar。缺少必要字段返回 typed unavailable，正常尚无新bar为 WAIT，不以0/昨天价格伪装当前机会。策略及新鲜度数值在详细设计冻结，不继承旧报价5分钟阈值作为默认正确值。
5. 同一bar修订不得重写已发出的历史决策。记录 revision/conflict 和所用输入身份；影响既往因果一致性时停止该计划的新提醒并显示原因。历史研究以新request重算，不覆盖旧产物。
6. 历史冻结文件、TDX 当日观察各自保留来源；不可用时不静默切源。盘中短采样只验证数据到达和末根语义，不成为历史研发准入条件。

### 3.3 价格语义

`price_constraint_mode` 明确为 NONE、REFERENCE、PREFERRED 或 HARD；价集可多段，保留空洞，携带 raw坐标/生效日期/来源版本。NONE 是预先选择的研究/产品模式，不是缺数据时的 fallback。选定依赖价集的模式而输入缺失，返回 UNAVAILABLE；核心不静默忽略。

五交易日建议价集合是条件化的研究导航，不是当日分布区间，也不证明盘中任意时点买入后仍保持原模型经济效果。融合实验固定评价终止日及相对原计划日的horizon定义；实际晚买可使剩余持有时间缩短，不自动将终止日后移以凑足原持有时长。买入模型不能镜像成卖出模型。HARD 约束到期也不自动突破；参考价不自动升格为硬限制。

### 3.4 LocalSIM 分钟执行接口

向 LocalSIM 窗口提供一个纯函数入口和薄策略 facade，目标映射现有 `init_order/compute_step/is_complete`，不使用 MiniQMT 协议。入口示意：`evaluate(plan, observed_prefix, execution_view, policy, as_of) -> decision, next_state`。

当前 `StepResult/StepFill` 代表模拟子成交，**不能把本分钟刚产生的 OPPORTUNITY 直接包装为本分钟成交**。LocalSIM adapter 保存待执行决策，到满足延迟与后续可成交条件的bar才生成自己的 StepFill；成交价格和合法数量由 LocalSIM 现有执行/风险/账本链路确认，核心不提前增加 executed_quantity。重试/恢复必须保存未完成批次与执行游标，避免重复成交或丢掉剩余量。

请求给 LocalSIM owner 的最小变更集合：

- 更新其 TWAP-only 上位合同，在新 release 中显式允许新的策略身份；旧 TWAP 参数、release、历史账户不变，旧退役算法仍拒绝。
- 在自身注册/capability/policy 路径接入新 facade；若现有 adapter 缺少待执行/确认反馈能力，只扩展必要桥接，不造第二账本或调度器。
- 保留交易日、停牌、方向性涨跌停、board-lot/T+1、现金与卖出可用量；不把现有尾盘 residual 路径变成无条件越过硬限价/截止时间的兜底。
- 基线和候选使用同一明确成交代理与费用合同；正常不可成交保留原因，原始全部计划纳入报告。

这些变更未交付前状态是 `LOCALSIM_INTEGRATION_NOT_IMPLEMENTED`，不是“已经可直接替换”。基础核心可独立交付，但不能把独立回放假称 LocalSIM 产品接入。

### 3.5 荐股可嵌入接口与提醒

建议的 HTTP 面：PUT `/position-timing/intraday/plans/{plan_id}` 创建/修订自有计划；POST 同计划 `/evaluate` 进行幂等计算并推进 timing-owned 状态；GET 同计划 `/status`；GET `/position-timing/intraday/alerts` 只读当前机会。提醒claim复用现有POST机制并独立namespace。路径在详细设计与现有 router 合并确认；无模拟下单/切换执行模式 endpoint。GET 不推进决策游标、不写事件或假装送达；POST evaluate只写自有状态/有界证据，不创建订单。相同计划版本、输入与时钟事件的重试不重复推进。

荐股端最少改动：传入原 list/item/plan revision 与已有价集引用；挂载一个 timing-owned 组件；收到原计划取消/修订时同步更新，页面每次刷新机会前通过原有计划读取能力核对来源revision。来源无法核实时保留旧内容作历史并停止新弹窗，不继续消费无法确认有效的旧计划。普通股票名单不自动生成目标仓位。组件显示中文状态、数据时间、原因、区间、有效期；缺数据保留可见状态，不影响原荐股名单。

优先页面可见时每分钟触发幂等evaluate并读取状态，服务端对同股票/日期使用有界短缓存和请求合并，减少每个标签页重复拉上游；不新建常驻worker/SSE/调度平台。缓存还绑定数据源及bar边界，不跨日期/来源复用，也不承诺跨进程全局请求去重。仅处理当前计划范围，过期计划不持续取行情。页面关闭不保证提醒，重新打开先同步原计划revision再评估当前状态，不补弹历史机会。

复用现有提醒幂等方式，但新分钟机会独立namespace。claim 仅代表获得本轮弹窗资格，不叫“用户已看到”；领取后崩溃仍可在状态列表看到机会。持久事件幂等，UI 尽力去重而非承诺严格 exactly-once。重复轮询不重复生成同一机会；刷新输入不等于新机会，失效/重新进入的边沿及节流定义在详细设计冻结。

### 3.6 存储与隔离

计划/决策/提醒只写 timing-owned namespace，研究 request/bundle/receipt 使用既有外置 artifact root 下的新版本子目录，复用已有存储/锁，不新增全局registry。只保存有界输入摘要、所用窗口引用、价格时间、状态、策略身份，不无限留存原始行情响应。仅有hash而无可重取的源不足以完整重算：冻结文件保留URI/hash，盘中保留实际使用的有界归一化窗口或既有可回读版本引用，不假称事后修订行情等于当时输入。LocalSIM 成交/费用/账户留在其自身体系，不写入人工提醒账户。无跨模块表写入、无修改 active profile、无覆盖旧产物。

## 4. Research / Verification Plan：研究与正确性分开

### 4.1 基础先行，模型可替换

第一阶段以规则实现接口闭环：B0 简单基准、C1 到价机会、C2 有限等待/分批。规则公式、等待窗口、分批数与必要阈值由第一阶段详细设计一次明确，不在本蓝图拍定“最优值”；首次实现与验证不做大规模搜索。没有价集的计划走显式 NONE，C1在这类计划上为不适用，不填造无限价集来假装完成到价策略；B0和不依赖价集的C2仍可验证。研究价集效果时使用同一批有合法价集的计划，并报告价集缺失/未知的原始人口比例，不根据结果过滤人口。

第二阶段先诊断收益来源/延迟损失，再选择一次小规模的机制研究：回落企稳、趋势确认或冲高减速，买卖侧分别评价。只有明确问题需要预测时才引入轻量线性/树模型；模型不是架构上线前置。研究轨与交付轨共享数据合同、版本和回放器，不要求每次换模型重写页面或消费接口。

### 4.2 基线、估计目标与费用

保留两种基线且分开命名：

- `LOCALSIM_NATIVE_TWAP`：当前冻结 LocalSIM 策略原样运行，只作产品现状对照；不能冒称它采用本方案的新成交代理。
- `MATCHED_SIMPLE_EXECUTION`：和候选共享原计划、方向、规模、起点、截止时间、成交代理、费用、约束及延迟；主结论用它隔离时机策略贡献。简单基准不读取未来bar来挑买卖价。

先报告执行改善，再看相同终止日的账户净结果/MDD。未执行资金留在原账户，未卖出仍按原持仓计算终值；共同估值缺失显式 unavailable，不只统计成功成交。全市场/指数池分层只在当轮明确人口与PIT依据后展示，不自动复跑旧日频全市场矩阵，不把指数收益当成执行价差基准。

LocalSIM 按自身冻结费用模型记账，不在本窗口覆盖默认值。个人净佣最低5元+规费的口径可作为独立敏感性分析，但必须有单独 cost-policy identity，不把 LocalSIM 合计费率最低额冒充同一合同。父计划/子批次/费用计费父订单映射明确；不因多次提醒暗中增加或减少最低佣金次数。不模拟市场冲击仍需披露，不能把不考虑冲击等同于没有成交限制或已经证明容量。

### 4.3 D0/D1/D3/D5 延迟合同

四个场景使用相同策略和独立模拟状态。D0=零额外人工延迟，D1/D3/D5=提醒可见后增加1/3/5分钟；**D0不等于即时保证成交，也不是严格收益上界**。首轮D1作为主要产品观察口径，其余完整报告，不能事后挑最优延迟。

定义：`eligible_at = advice_available_at + human_delay`。`advice_available_at` 包含数据发布/获取、计算及提示可见时间；历史缺真实记录时使用显式冻结的发布/展示时延假设，不能仅用bar标签代替。

仅有分钟OHLC时，拟采用 `NEXT_ELIGIBLE_BAR_OPEN` 作为统一成交代理：选起始时刻严格晚于 eligible_at 的第一根可用执行bar开盘价；实际bar为结束标记时先由交易时段映射起点，不能拿午休后一根机械减一分钟推导连续交易。执行器等bar可观测后记录模拟结果，但不能把此bar的完整OHLCV先提供给信号核心，再声称提前在其开盘成交。

例如结束标记09:31的bar于09:31:05收到，提醒于09:31:10可见：D0最早使用09:32开始、09:33结束的bar open；D1最早使用09:33开始的bar open，D3/D5同理。这里另外包含分钟网格取整等待，报告实际等待秒数，不把D0解释为零总等待。strict-after的边界与时延假设由详细设计冻结，不能按结果切换为同bar close。

计划/硬限价/交易资格不满足时记录未成交或在有效期内继续等待，截止后不跨日携带。等待中的决策遇到计划取消/修订或自身有效期结束立即失效，不因“已经排队”而继续执行。午休计入真实等待时间，但不能在闭市中成交；当日最后一根信号可能没有后续执行bar，应过期。分钟成交代理不能证明排队、精确盘口和实际成交概率，涨跌停/停牌/零成交使用明确保守规则，不因当分钟low/high触及价格就确定成交。

开盘代理的价格约束按该执行bar的open及当时有效规则判断，不能用该bar后来回落/冲高选择更有利价格。若成交模拟使用整分钟成交量限制数量或判断无成交，该信息只属于事后区间成交模型，需标明其不证明开盘瞬间可成交；不得反馈为开盘前特征。LocalSIM如果不能表达这个分离，应由其owner补最小adapter，不改用同bar close掩盖差异。

延迟隔离实验使用同一冻结机会流和四份独立响应状态，避免成交差异反过来改变机会流；该实验只估计响应延迟影响。完整闭环实验另报：不同成交反馈可改变后续策略状态/机会，仍从同一原始计划、市场数据和策略起跑。二者不能混成一张“纯延迟效应”表。

### 4.4 研究有效性与最小测试

- 同计划配对报告：买/卖价差（方向统一）、金额加权与等权指标、净收益、完成率、等待、未完成机会成本、提醒频次；账户MDD与逐股分布区别报告，未成交/未知均保留分母。
- 每次研究request同时冻结计划人口及其生成时点、源dataset/分钟覆盖与PIT身份、价集/策略/费用、时延/成交/终止合同、源码版本及比较family。历史研究沿用的R8/R7身份不自动成为本分支默认输入；使用当轮明确可读且验证覆盖的冻结文件，不依赖可变active profile、未来导出或生产数据库。缺口只影响相应实验，不能用删股/缩期/补0改善结果。
- 统计单位尊重同股票/交易日相关性，不把相关的1分钟点当独立样本；正式候选、端点、family、时间切分和费用在读取收益前冻结。重复使用过的历史测试区间标探索性，不伪装未见测试。
- 模型特征仅使用前缀；标签允许未来但只在训练/评价模块，训练截止时标签需成熟，跨边界重叠做purge。验证/测试不参与标准化拟合、调参或策略阈值回选。
- Oracle 只作明确约束下事后诊断，不进入运行特征或策略状态，不作为能实现最优收益的承诺。未来扰动测试要求改变t之后数据不改变截至t的决策；合法后续成交/收益可以变化。
- 最小业务测试覆盖：闭合bar/未来扰动、时间/单位、计划取消和旧revision、价格多段/未知、延迟跨午休/收盘、限制与无成交、部分成交/重试、提醒不写订单、模型缺失显式失败、同输入双适配决策一致。不是新增一套全平台回归或大批内部断言。
- 模型结果支持与否如实记录，不加MDE准入、sealed holdout审批、双人确认或自然日等待门禁。基础功能可以带实验性标识交付，未证实收益不能宣传已获利。

## 5. Implementation Plan：仅两个阶段

### 第一阶段：可用闭环与接口交付

下一步编写**一份紧凑的F2详细设计**：固化最小schema、规则参数、时间/成交代理、提醒状态转换和各owner文件清单。并行工作面是同一阶段，不再拆为多轮独立大项目：

| 工作面 | 责任与交付 | 不等待什么 |
|---|---|---|
| 核心与回放 | position_timing：纯核心、显式基准规则、冻结文件前缀决策回放、差异报告 | 训练模型、未来行情、LocalSIM生产接入 |
| LocalSIM接入 | LocalSIM窗口：上位合同和显式策略扩展、因果adapter、D0/1/3/5、原账户/费用回归 | 模型证明正收益；但不绕过本模块输入/账户正确性 |
| 提醒接入 | position_timing API/组件；Advisory窗口完成最小挂载与计划生命周期同步 | LocalSIM成交、后台通知平台、实盘接口 |

阶段完成须分别有核心、LocalSIM和提醒接入的真实功能证据。允许独立发布已完成的工作面，但不能把它叫完整阶段交付。核心单独回放先证明机会流与因果性，不自行建立账本来冒充LocalSIM经济结果；账户级延迟收益使用接入后的既有LocalSIM离线链路，无需等待生产自然日。盘中只读采样验证数据时效，页面功能可先用可控历史时钟验证，不假称实际盘中验收。

### 第二阶段：连续研究和有证据的能力替换

在稳定接口上开展有限的规则/模型研究，先回答时机优势能否承受延迟及错失机会，再决定下一候选。每轮一项主要变化，参数与模型权重版本化；不以研究轮数为进度，不全自动扫描直到出现正结果。规则和研究模型可并存展示且明确标签，不自动将最优历史结果替换当前策略。

### 不增加的东西

不建独立微服务、消息总线、数据湖、特征平台、独立训练调度、额外账本或全局模型registry。不要求日频/荐股/QE/HMM先成功才能研究本问题；不在本次开发跨模块代码。若发现真实公共缺陷，提交明确输入/输出/失败复现需求交owner，不在择时内临时补造数据或绕过合同。

## 6. Rollout / Rollback / Risks / Production Gates 与验收

### 6.1 状态与发布

区分 `design_verified`、`source_verified`、`integration_verified`、`runtime_verified` 和 `economic_evidence`；不以F2静态校验代替实现或获利。基础API/组件可先发布；LocalSIM只能在其窗口完成显式扩展后对新实例选择新策略。旧TWAP行为和历史release不覆盖，切回已知策略使用新的明确计划/版本，不暗中改变在途执行。

回滚：停止新计划引用新策略，保留旧研究和提醒记录；在途模拟计划由LocalSIM owner按既有生命周期处理，不把清理文件当取消订单。提醒故障只使本组件UNAVAILABLE，不影响原荐股页面。

本次 `code_change=false; experiment_started=false; database_read=false; database_write=false; dataset_mutation=false; runtime_action=false; backend_restart=noop`。仅文档和源码读取、Git元数据操作、文档校验；没有本次行情采样。未来若需后端重启由用户操作；不提前申请或执行生产迁移。

### 6.2 风险与处置

| 风险 | 最小处置 |
|---|---|
| LocalSIM实际是TWAP-only，误称可直接替换 | 将其上位设计/代码扩展写成owner任务，核心/提醒可继续 |
| StepFill与机会混淆、同bar前视 | 待执行队列/游标及确认反馈由LocalSIM持有；前缀核心与后续执行bar隔离 |
| 分钟未闭合、发布迟到、历史重述 | 明确时间/版本/新鲜度；受影响计划显式状态，不伪造成功 |
| 费用或收盘/open代理变化造成虚假改善 | 保留native对照，主结论用matched合同；费用身份单独记录 |
| 低完成率看似“成交价更好” | 全计划分母、未成交机会成本与共同终值，不能只报成交子集 |
| 价格模型与持有期不匹配 | 保留价集来源/持有期，融合后单独验证，不作旧效果迁移承诺 |
| 人工没有成交反馈仍连续推批 | UNKNOWN独立，提醒不推进账户；只展示当前机会与有效期 |

### 6.3 Design Acceptance Index

以下编号仅属于本蓝图，不替换总蓝图原F-ID。当前验收对象是**设计文件**，不是尚未开发的功能。

| ID | 蓝图必须明确的内容 |
|---|---|
| F-001 | 只有LocalSIM与提醒；不接MiniQMT/实盘/Tick，日频研究不被改写 |
| F-002 | 现状证据准确；TWAP-only、StepFill和费用差异可追溯 |
| F-003 | 一纯核心双适配、输入显式、核心无消费者反向依赖 |
| F-004 | 计划版本、方向规模、模拟/人工状态隔离与重试 |
| F-005 | 已完成bar、可见时间、单位、来源与未来信息隔离 |
| F-006 | LocalSIM按owner扩展，复用原账户且不伪造同bar成交 |
| F-007 | 荐股最小挂载，提醒无交易权限与送达保证混淆 |
| F-008 | 价集多段/坐标/持有期及可选模式明确 |
| F-009 | native/matched基线、费用、D0/1/3/5和全计划评价 |
| F-010 | 基础交付独立于模型有效性，研究冻结/Oracle/泄漏测试 |
| F-011 | 仅两阶段，无新平台/额外审批；跨模块修改交owner |
| F-012 | 发布状态、回滚、设计验收与实现证据分离 |

### 6.4 Design Acceptance Matrix

`DESIGN_VERIFIED` 仅表示条款已在设计中闭合；`gap_or_exception=none` 仅针对本次设计范围。所有新接口/核心/策略/实验仍未实施；§5中的跨模块交付是未来实施任务，不以本矩阵掩盖。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文§1 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-002 | 本文§2.1；LocalSimPlanner/ExecutionAlgoAdapter/FeeModel | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-003 | 本文§2.2/§3.1 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-004 | 本文§3.1/§3.4/§3.5 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-005 | 本文§3.2/§4.3/§4.4 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-006 | 本文§3.4 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-007 | 本文§3.5/§3.6 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-008 | 本文§3.3 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-009 | 本文§4.2/§4.3 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-010 | 本文§4.1/§4.4 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-011 | 本文§2.3/§5 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |
| F-012 | 本文§6.1/§6.2/§6.4 | artifact:docs/analysis/position_timing_intraday_blueprint_review_20261023.md | DESIGN_VERIFIED | none |

本蓝图确认后才进入第一阶段详细设计和开发；本次不启动实验，也不声称任一候选优于现有执行策略。
