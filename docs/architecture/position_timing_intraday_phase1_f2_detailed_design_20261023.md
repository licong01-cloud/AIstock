# 分钟择时阶段一 F2 详细设计：独立核心、提醒接口与延后消费接入

> 版本：v1.1；日期：2026-10-23；状态：`SOURCE_IMPLEMENTED_RUNTIME_PENDING`；Feature tier：F2。
> 上位：[分钟择时与LocalSIM蓝图](position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)。本模块核心/API/页面/决策回放已实施并有本地功能证据；LocalSIM与Advisory产品接入仍交owner。
> 源码交付不等于生产已加载、模型训练或经济有效。后端重启及重启后运行核验未执行；未查询数据库、抓取行情、修改数据集或控制用户服务。
> 原设计基线：`c8a6ad045fd38c3b57540b1d57fd3bd9e1a48b21`；实施起点：`cb303549a6ced76a1a311b48b490085de2aedc2e`。实际变更与验证见[实施验收记录](../analysis/position_timing_intraday_phase1_implementation_20261023.md)。
> 唯一开发权威：`../standards/aistock_development_standard_v1.5_20260523.md`。

## 1. Background / Scope / Non-goals

### 1.1 本阶段交付与实施顺序

本窗口先实现：纯分钟决策核心、三种固定规则、TDX与冻结文件输入边界、timing-owned计划/状态/API、可嵌入提醒组件、历史前缀回放及可转交的LocalSIM接口包。先挂载本模块现有页面，验证提醒闭环，不等待荐股页面或LocalSIM产品接入。

LocalSIM接入仍属于总蓝图第一阶段的消费接入工作面，**延后交给LocalSIM窗口**；未接入时不能宣称整个阶段完成。Advisory页面挂载也由对应窗口修改。本文件只约定消费合同，不修改它们的代码或上位设计，不解除当前TWAP-only，也不注册新执行算法。

总体仍为两个阶段：第一阶段基础闭环，第二阶段连续研究。以下工作包不是新增项目阶段。模型训练/预测研究、QE/HMM、多Agent、实盘、MiniQMT、Tick、盘口、真实成交采集、订单/账本/新调度平台均不在本次实现范围。

### 1.2 能力声明

首版为 `RULE_BASED_INTRADAY_ASSISTANCE`、`economic_confirmation=false`、`orders_created=false`。可显示规则型价格条件和机会，不把均线/近期价格区间称为模型预测分布；不承诺最佳买卖点或改善收益。规则有效性与API上线分别验收，历史回放先行，盘中实测不阻塞开发。

## 2. Architecture / 复用与精确实施文件清单

### 2.1 结构

```text
MANUAL计划 / 源模块只读计划桥接 / 历史冻结计划
                     ↓
          IntradayPlanV1 + PolicyV1
                     ↓
TDX/冻结文件 → NormalizedMinutePrefix → evaluate（纯函数）
                     ↓
      状态与机会意图（不是StepFill、不是订单）
         ↙                             ↘
 timing API + React组件          后续LocalSIM owner薄适配
  页面和弹窗                   既有成交/费用/账本/延迟验证
```

同一输入、policy、前态、执行视图和时钟得到同一决策。核心不得导入TDX、FastAPI、DB、Advisory或LocalSIM运行对象。HTTP只支持人工建议；未来LocalSIM用进程内函数，不通过HTTP把“提醒”转换为交易。

### 2.2 本轮已实施文件范围

| 文件 | 职责 |
|---|---|
| `backend/services/position_timing/intraday/__init__.py` | 仅公开纯合同/核心入口，不在import时装载数据/模型/注册策略 |
| 同目录 `contracts.py` | 严格schema、reason枚举、版本化固定policy、canonical序列化 |
| 同目录 `core.py` | 三种规则、批次意图、机会与生命周期状态转换，无I/O |
| 同目录 `inputs.py` | TDX/冻结文件、全局日历与日级交易事实只读适配，单位/前缀/来源校验 |
| 同目录 `store.py` | 本模块namespace、锁、不可变transition及原子head，不改旧event parser |
| 同目录 `service.py` | API应用逻辑、来源校验、幂等批评估、轻量缓存、claim |
| 同目录 `replay.py` | prepare/run/inspect/exact retry的决策/延迟意图回放，无账户撮合 |
| `backend/routers/position_timing.py` | 追加intraday路由，保留旧接口行为，不改backend main |
| `frontend/src/app/position-timing/IntradayTimingPanel.tsx` | 自包含、可导入的API/提醒组件；沿用本模块ownership路径，props注入源刷新回调，不充当后端权威resolver |
| `frontend/src/app/position-timing/page.tsx` | 挂载新组件；旧日频卡片/提醒不改逻辑 |
| `backend/tests/position_timing/test_intraday_core.py` | 因果、规则、状态及可执行性边界 |
| `backend/tests/position_timing/test_intraday_api_store.py` | 接口、CAS/恢复/claim和无交易副作用 |
| `backend/tests/position_timing/test_intraday_replay.py` | 冻结身份、前缀/延迟意图、inspect/retry |
| `frontend/tests/position-timing/position-timing.spec.ts` | 在现有CI已执行的Playwright文件中追加分钟提醒业务测试，复用runner |

实施核查确认：现有ownership精确覆盖`frontend/src/app/position-timing/**`和`frontend/tests/position-timing/**`，CI前端计划执行既有`position-timing.spec.ts`。因此组件保持可导入但放在本模块目录，测试追加既有文件，不要求其他窗口扩展catalog。此项为文件布局收敛，不改变接口/产品范围。`package.json`已有Playwright；不增加Jest/Vitest或重复fixture、内部实现断言。

### 2.3 已核实复用点

- `tdx_adapter.fetch_minute_kline_tdx` 已按单日有界读取；仍需本模块闭合bar/新鲜度判定，不能继承旧报价5分钟阈值。
- 全局 `TradingCalendarStatusService` / `simulation_data.trading_calendar.TradeCalendarProvider` 提供交易日，不能把交易日列表误当交易时段列表。时段由输入中的显式session snapshot约束，缺失不按工作日猜。
- 线上实际复用 `DailyTradingContextProvider.load_supporting_facts/load_stk_limit_authority_attempt` 的只读入口，验证股票和日期；不加载含当日收盘/历史fallback的日频快照，不调用旧日频materialize来“顺便”取数据。缺限价行返回未知，不伪造合法无限价状态。
- `PositionTimingArtifactStore` 提供canonical、no-replace、atomic-replace与文件锁范式。新namespace单独解析新schema，不把新事件塞进只支持旧三类事件的 `_validated_event_payload`。
- LocalSIM真实owner为 `simulation_execution/localsim`；`paper_trading_v2/broker/localsim.py`仅兼容导出。`LocalSimPlanner.assert_plan_uses_twap` 和 `ExecutionAlgoAdapter.compute_step → StepFill` 的约束不变；`FeeModel`不等于个人分项费用。

## 3. Contracts：字段、时钟与数据输入

### 3.1 固定合同

| 对象/schema | 字段与规则 |
|---|---|
| `position_timing_intraday_plan_v1` | `plan_id`、正整数`revision`、`source_kind/ref/revision`、canonical symbol、BUY/SELL、target_trade_date、valid_from/until、lifecycle、policy_id/hash、price_constraint、可空target_quantity、quantity_source_ref；日期内有效，不允许悄悄跨日 |
| `position_timing_intraday_market_v1` | session/calendar snapshot及hash、日级limit/suspend/reference来源与available_at、当日已完成bars；bar含start/end、OHLC原币元、volume_shares、可空amount_cny、received_at、源版本 |
| `position_timing_intraday_execution_view_v1` | consumer_kind、symbol/trade_date/available_at、confirmed/reserved quantity、cash/sellable及身份、last_confirmed_batch、last_confirmed_at、feedback_id；HUMAN_UNKNOWN字段为null，模拟输入才要求完整；身份匹配且available_at不晚于决策 |
| `position_timing_intraday_state_v1` | plan/policy身份、last_processed_bar、时钟事件、批次、机会编号、last_condition、last_decision_ref；不能把alert_count当executed_quantity |
| `position_timing_intraday_decision_v1` | 决策身份、输入/state hash、as_of、WAIT/OPPORTUNITY/EXPIRED/UNAVAILABLE、batch_id、建议比例/可空数量、原因、data_as_of、valid_until、source_status、execution_status |

金额/价格为Decimal字符串，数量为非负整数股，时间带时区并canonical为UTC，UI转Asia/Shanghai。字段extra禁止；空值是UNKNOWN而不是0。HUMAN可没有target_quantity，只显示比例/机会；不得由资金500万元等历史实验默认值补造数量。BUY/SELL必须由用户或来源计划明确给出。

policy快照显式保存schema、算法版本/源码身份、所有参数、session与时延合同，不靠未来共享默认值重建。canonical沿用本模块排序JSON/UTF-8/SHA256范式，Decimal规范化、禁止NaN/Inf；内容hash不含自身hash字段。金额/数量不为负；bar价格为正、OHLC顺序合法、成交量有限且非负；最新bar为零量时标`NO_OBSERVED_TRADE`、不发机会，不能将前收填值当有效成交。类型失败与真实缺失分别保留原因。

source_kind首版支持 `MANUAL`（本模块计划为authority）与 `FROZEN_REPLAY`（只允许CLI）。`ADVISORY`的只读resolver协议提供，但未配置真实resolver时返回 `SOURCE_ADAPTER_NOT_CONFIGURED`，不信任浏览器自行声明的“来源已验证”。未来挂载由荐股窗口接入其现有名单/计划读取能力，无直接跨模块表写入。价格来自用户时标USER_SUPPLIED，来自模型时保留原policy/model hash，不能混称模型结果。

计划状态ACTIVE/CANCELLED/EXPIRED/COMPLETED：到期可以由GET派生显示，真正状态推进由POST处理；COMPLETED仅模拟确认完成或用户明确修改计划为完成，不因弹窗次数设置。服务器分配revision；同revision同内容重试成功，旧expected_revision更新返回409。价格/数量/策略/有效期改变均新revision，原revision不可改写，原批次和未执行建议立即失效。

### 3.2 时钟与session v1

HTTP evaluate只用服务器时钟；请求不能传过去/未来as_of来解锁历史价。CLI才使用冻结模拟时钟。日历复用全局服务的只读快照；历史运行用request内的冻结日历，不连接生产DB。

首版只在显式 `CN_A_CONTINUOUS_WINDOW_V1` 研究/提醒窗口运行：09:30～11:30、13:00～14:57（当地时间），不建集合竞价策略。该窗口是产品范围，不声明适用于所有未来交易制度；每个来源的bar标记方式和交易日须被snapshot验证。股票停牌事实独立，ST不等于停牌，方向性涨跌停分别处理。

决策窗口为左闭右开；恰好11:30或14:57不能发新机会/claim。bar本身的结束时刻可等于session结束，但该bar收齐时已经闭市，不能用来回填闭市前机会。午休前未执行的本批意图按有效期终止，不自动跨午休恢复；午后可显示同批当前条件，但不补造第二次弹窗。

1分钟bar采用结束标记；规范化明确start/end且两者属于同一session。09:31表示09:30～09:31，13:01表示13:00～13:01；不跨午休合成bar。闭合接纳规则v1为 `end <= server_received_at - 5s`，排除最新未完成bar；这是保守运行假设，不承诺数据源再无修订。已接纳bar内容变化时该计划 `INPUT_REVISION_CONFLICT`，不覆写旧证据；可用新计划revision明确重建，非人工审批流程。

有效可见时刻=`max(bar_end,source_available_at如有,received_at)`，并要求≤decision_as_of；不能把整日响应统一贴早上received_at。线上逐次保留实际接纳时间；历史无原采集时间时按bar_end+5s、模拟展示轮询相位再+5s构造（默认每分钟xx:10），标 `ASSUMED_VISIBILITY`。未来盘中采样可形成新版本时延合同，不修改原实验。

最新应到bar为当前session中满足end≤now−5s的最后一格；允许落后最多1个有效交易格，超过为 `MINUTE_STALE`。开盘尚无bar为WAITING_FOR_BAR，午休为SESSION_BREAK，不继续弹窗；缺格不补0。重复同bar同payload无动作，乱序按时间规范后校验，冲突重复fail closed。策略用最近5根时必须同session连续有效，不能把昨天或午休前的数据凑足5根。

### 3.3 输入来源与性能边界

TDX调用只针对计划股票/当天，单请求10秒timeout，服务批评估最多50个计划、去重股票后4并发、批预算15秒；超额未处理项返回 `DEFERRED_BUDGET`，响应明确partial，不丢项。资源上限是分页/批大小不是股票研究准入或最低成交额门禁。首版每个页面可见时60秒触发一批evaluate，无常驻worker、SSE或额外scheduler。

响应保留输入顺序与逐项处理状态；页面下一轮将上轮DEFERRED项排在已完成项之前，其余轮转，不能每轮固定从前50项开始。超过50项的页面列表用同一轮转队列分批，不静默丢弃尾部计划；长期失败源也不独占队首。队列仅页面内有界ID列表，不是新后台任务库。预算耗尽后不继续启动排队请求，在途请求受原timeout约束且不得提交已超预算的迟到状态。

同进程同source/symbol/date/day-context短缓存TTL=5秒并合并在途请求；新bar边界不复用过期缓存，不跨日/源复用；不承诺多进程全局去重。失败不缓存成空数据。网络获取在文件锁外；写入前重新核对计划revision、状态和源版本。

TDX adapter已转价格为元，volume仍为手；本层一次转股并核对可用VolumeShares，不能对同值重复乘100。amount缺失不补0，三种首版规则不依赖amount。raw价格与输入limit统一；历史复权分钟必须有明确raw还原合同，不能把调整后价格直接与raw涨跌停比较。错误数据不会触发从TDX切历史文件或日频价格fallback。

日级事实包含当时可见的参考价/上下限（可有合法无涨跌幅状态）、停牌及来源时间。线上通过既有只读loader/纯计算获取；历史输入随request绑定snapshot。未知和“无价格限制”分开，缺必要事实显示 `TRADABILITY_UNKNOWN`，不因缺输入阻止其他有效计划。既有交易日/停牌/涨跌停数据有缺口时交数据窗口，不补库。

## 4. Rule policy v1：有限且可重放，不声称最优

### 4.1 共同定义

每个计划选择一个policy，不组合投票。规则从新接纳的已完成bar逐格推进，c_t为该bar close，m5为同session最近5根close的Decimal算术均值。没有特征训练、隐含校准或回测选参数。

纯入口固定为 `evaluate(plan, observed_prefix, execution_view, policy, previous_state, as_of) -> decision,next_state`；初始化状态显式传空，禁止进程全局状态。线上首次打开或漏过数轮时，历史前缀只供指标预热；以实际本次as_of及最新合格bar判断当前机会，不把旧bar重新当作当时已观察并补发提醒。离线则逐个冻结的轮询as_of重放，同样不能给尚未开始的计划发历史机会。C2等待起点是该批首次实际可评估时钟事件，不是下载文件里的第一根bar。

价格模式：NONE完全不使用价集；REFERENCE只展示；PREFERRED早期偏好满足价集，显式时间预算到期可放弃偏好；HARD任何分支均须满足价集。区间闭区间、可多段不合并空洞，raw元、tick合法，保留来源/valid_until。已选依赖价集但缺失/过期为UNAVAILABLE，禁止降为NONE。实际成交是否仍符合价格由后续执行器再检查，当前close在价集不证明未来开盘仍可成交。

进入OPPORTUNITY前还要满足：计划/源有效、时段有效、新鲜数据、必要交易事实可用；当前close处于BUY上限/SELL下限时标方向阻挡，不声称可执行。分钟线无法证明盘口排队或瞬间流动性；本层只做保守机会筛选，不输出成功成交。

### 4.2 三个策略

| policy_id | 完整首版规则 | 批次 |
|---|---|---|
| `FIRST_OBSERVABLE_V1`（B0） | valid_from后第一次可评估且满足共同约束的bar发机会；HARD必须满足，PREFERRED/REFERENCE仅展示并标 `PREFERENCE_NOT_USED_BY_BASELINE`；不等待近期形态 | 1批 |
| `PRICE_TOUCH_V1`（C1） | 要求PREFERRED或HARD价集；c_t落任一合法区间才发机会，直到有效期结束；不根据bar low/high触价假定当前可成交 | 1批；NONE/REFERENCE配置不适用 |
| `BOUNDED_REVERSION_V1`（C2） | 5根连续bar可用时，BUY满足`c_t <= m5 且 c_t > c_(t-1)`；SELL满足`c_t >= m5 且 c_t < c_(t-1)`，并满足PREFERRED/HARD。首个可评估格起等待15个交易分钟，到期可走本策略 `TIME_BUDGET_RELEASE`，无需形态但仍须共同约束和HARD；PREFERRED可放弃并明确原因 | allow_split=false为1批；true最多3批 |

C2等待时钟按session格推进，午休暂停；人工延迟时钟按墙钟计时，二者不得混用。时间预算到期且5根不足可以走明确的TIME_BUDGET_RELEASE，但最新可见bar必须有效；这不是换用B0或静默fallback。截止valid_until是更高约束，不为凑足15分钟顺延到次日。

C2分批目标累计比例为1/3、2/3、1（序列化为整数分子/分母，不用二进制浮点近似）。模拟端以原目标数量计算各批累计目标，adapter按既有board-lot/资金/可卖量转换，不能用“每次剩余量的1/3”造成尾数收敛不完。若不足合法最小量，允许依现有规则合并至合法批次并披露，不造零量成交。纯核心只输出累计目标比例/可空数量，不自建board-lot实现。

下一批须上一批已由消费端确认完成/合法终结且距确认至少5个交易分钟，再开始本批机会判定与15分钟预算。部分成交不能直接当一批完成；是否合法缩量由LocalSIM自身账户规则决定并反馈。HUMAN_UNKNOWN只展示当前批次及剩余静态计划，不因为已弹窗就自动推进；用户以明确剩余规模修订计划形成新revision即可继续，无额外批准流程，不把原总量自动当成剩余量。

### 4.3 机会与提醒状态机

- WAIT→OPPORTUNITY：新bar满足当前批规则。机会身份=`hash(plan_id,revision,policy_hash,batch_id)`；首版同批在同revision内只有一个弹窗资格。失效后再次成立只更新列表，不重复弹窗；需要不同提醒节奏后续另立版本，不暗改当前结果。
- OPPORTUNITY可随每个新bar重新验证条件；新数据不满足则WAIT，计划取消/到期则终止，数据失效则UNAVAILABLE。C2一旦进入TIME_BUDGET_RELEASE，该批此后保持时间预算分支，不回退到形态分支。
- `valid_until=min(plan.valid_until, session.end, decision_as_of+90s, 所依赖价集的valid_until)`；NONE/REFERENCE不以价集期限约束算法。只有持续新鲜且条件仍成立才刷新有效期。GET/claim都按当前时钟检查，`now >= valid_until`即失效，不依赖后台先写EXPIRED。
- 消息领取只表示 `ALERT_EMISSION_AUTHORIZED`，不表示用户看见或成交；先claim再toast，页面崩溃不补造送达。列表始终显示有效机会；后续刷新不产生第二条领取记录。
- 每分钟变更的是decision_id（包含input/state/时钟），不是新的机会identity。重复读取、同请求retry及多标签页不得产生重复事件；UI尽力去重，不承诺物理显示exactly-once。

延迟模拟若等待期间机会失效/取消，待执行意图作废；持续OPPORTUNITY可刷新90秒有效期，但不重置首次advice_available_at来规避D3/D5。同批意图一旦取消，不因重新成立而自动复活或生成第二次延迟意图；列表可以恢复可见，但没有第二次弹窗。新revision或确认后的下一批才有新的机会身份。这个保守合同可能降低D3/D5完成率，须如实计入结果，不用事后持续条件补造提醒。

## 5. API / State / UI Contracts

### 5.1 对外接口

现有router下追加，完整前缀为 `/api/v1/position-timing/intraday`。HTTP不开放模拟成交写回或SIM执行开关；纯SDK的消费反馈只供后续LocalSIM进程内使用。

| 接口 | 输入 / 结果 |
|---|---|
| PUT `/plans/{plan_id}` | expected_revision（创建为0）、计划payload、idempotency_key；同key同payload返回原revision，不同payload为409；source resolver未配置不可伪造来源 |
| POST `/evaluations` | 1～50个plan_id/revision、idempotency_key；服务器取时钟/行情，返回逐项状态、输入/决策hash、budget summary；仅写timing-owned状态 |
| GET `/plans/{plan_id}/status` | 只读状态与有效性派生；404区别于无机会；不推进游标、不获取新行情 |
| GET `/alerts?plan_ids=...` | 分页读取当前有效机会和不可用原因；来源失效/过期不返回可领取机会 |
| POST `/alerts/{opportunity_id}/claim` | expected_revision、decision_id、idempotency_key；重新检查当前源、状态、有效期；第一次authorized=true，其余already_authorized，不声称delivered |

方案从蓝图的逐计划evaluate细化为有界批评估，减少页面对TDX重复请求；没有增加第二业务能力。路由错误：422 schema/不支持组合，409 revision/幂等内容冲突，404不存在；正常尚无bar/价不合适/停牌等作为200内typed状态，源I/O异常或预算未完成逐项返回UNAVAILABLE/DEFERRED，禁止整批空success。错误日志不含密钥、凭证或外部原始payload。

source validation在evaluate开始及claim前执行：MANUAL核本模块revision；未来ADVISORY由注入resolver核当前ref/revision/lifecycle。不得仅接受浏览器 `source_valid=true`。评估resolver预算包含在15秒内，claim最多等待5秒且共用4个有界worker槽；无法核实则 `SOURCE_UNVERIFIED` 并保留历史显示，不更改源模块名单。已有系统访问控制沿用，不建设新的审批/RBAC系统，也不绕过已有鉴权。

GET只报告最后核验来源的时间与已知状态，不调用外部resolver，因此不保证发现尚未观察到的源撤销；领取时必须重新核验。过期/未知状态不能标成“当前已核实”。每个计划提交使用实际输入完成后的as_of，不能用请求开始时间接纳后来才收到的bar；批响应包含逐项evaluated_at，不声称50项是同一市场瞬时快照。

### 5.2 幂等、存储与崩溃恢复

根目录沿用 `POSITION_TIMING_ARTIFACT_ROOT`，新数据仅在 `intraday_v1/`：inputs、transitions（含不可变plan版本）、heads、requests（幂等预留）、claims、batches、locks及research。路径由经过约束的ID或内容hash生成，拒绝路径分隔符/越界，不接受客户端传任意磁盘路径。

每计划锁保护CAS；锁持有期间不访问网络。获取锁最多1秒，超时返回 `BUSY_RETRYABLE`；不能直接继承无限等待而锁住API。复用既有Windows/Linux锁与原子写范式，在本模块实现有界等待，不修改共享工具默认值。

每次计划状态提交先no-replace写有界input及immutable transition（含before/after state、decision、request身份），最后atomic-replace单一head指向已写完整transition。head是计划状态的唯一提交点；崩溃遗留未被head链引用文件不算已提交状态事件。重试按idempotency索引/可回读链返回既存结果，不重新触发提醒；恢复只读取已提交链，hash不闭合为typed conflict，不“修复”成初始状态。领取记录使用下面的独立no-replace提交点，不误称由head提交。

批评估不是跨计划事务。先no-replace写批key与规范化完整请求身份，冲突时在任何逐项写入前返回409；每项以`(batch_key,plan_id,revision)`幂等提交，最终no-replace保存有界批响应receipt。已完成批重试返回原响应（包括DEFERRED项），新轮询须新key；批响应落盘前崩溃则读取已提交项，仅重算未提交项，使用恢复时真实时钟，明确各项evaluated_at。并发同key的批响应以首次成功落盘者为准，所有调用回读该响应；同key异请求409。UI按当前时钟处理旧响应有效期，不因重试成功重新弹过期机会。不引入全局事务协调器。

claim在相同计划锁内，检查head后写独立no-replace领取记录；即使响应丢失也只一条授权。它不改executed_quantity。来源网络校验先在锁外完成，锁内核对校验时的revision；来源并发变化超过可观察边界属于轮询局限，不能承诺跨系统分布式事务。若校验已过期，不接受领取。

请求响应有原始evaluated_at；重试不得把旧结果换个当前时间冒充新计算。新的轮询使用新key。状态输入保留实际所用归一化窗口（当日最多240根且只接纳有效前缀）和摘要/hash；研究冻结源用URI/hash，既有完整源可回读则不重复保存。无无界逐次原始响应、无全局registry、无数据库DDL/DML。

### 5.3 可嵌入组件

组件props只包含 `apiBase`、计划ref列表、源计划刷新回调、`onStatus`；不读取荐股内部store，不复制算法。首版挂在现有position-timing页，MANUAL计划由显式股票/方向/有效期/数量可选字段创建，不把旧watchlist自动变成买单。

可见时每分钟先刷新来源ref、同步必要新revision，再调用批evaluate，读取状态/claim并toast；同一个页面的所有卡片共用一轮请求。隐藏时停止轮询；重新显示先同步源版本，不补弹过期机会。跨标签页由claim去重。若props新旧计划改变，取消旧请求的UI应用，按revision丢弃迟到响应。

显示规则名称、分钟数据截至时间、建议比例/价格条件、原因、有效期和“仅建议/未确认成交”。缺数据和源失效有明确中文状态，原荐股页面不因此报整体错误。不要显示伪胜率、Oracle最优价或未经校准的模型置信度。优先复用现有组件/token/toast，不新增页面框架/通知服务；浏览器通知不是第一版交付条件。

## 6. Replay / Verification Plan：先决策回放，后账户经济验证

### 6.1 本窗口CLI

`python -m backend.services.position_timing.intraday.replay prepare --spec <json> --timing-root <root>` 冻结request；随后run、inspect，再以同request exact retry。spec显式指定计划文件、归一化分钟/日级事实/session快照、原candidate manifest引用/hash、policy、日期和可见/轮询假设。不自动找最新active profile，不抓取或导出数据。

v1离线输入是冻结JSONL/Parquet的明确映射列，不要求新建数据集：inputs.py只在本研究产物目录建立必要的不可变规范化视图，原candidate只读；不能利用规范化偷偷补缺失值。缺raw还原、单位、日级事实或身份时输出source failure，不能等行情来补研究。开始读取可用于收益计算的未来执行bar之前，完成整个spec的source身份和覆盖检查；请求中的策略输入永远只读前缀。

run输出 `decisions`、`opportunities`、`delay_intents` 和receipt，不生成虚构账户NAV。inspect重算request/policy/input/output hash、行数、时间因果、计划保留与原因分布；exact retry同产物或ALREADY_MATERIALIZED。Python源码身份采用UTF8_SOURCE_LF_V1（只统一Git LF/CRLF换行），数据文件仍校验原始字节SHA，不放宽数据身份。receipt区分 `decision_replay_complete` 与 `economic_evaluation=NOT_RUN_LOCALSIM_ADAPTER_PENDING`。

首次验证用明确冻结的少量计划窗口覆盖买卖、午休、收盘、缺bar、暂停/限价、计划变更及partial反馈模拟；fixture用于功能而不是获利证据。先不启动新的全市场策略搜索。真实历史人口与研究窗口在实施后的首个request中按可用文件显式固定，不事后按收益删股缩期。

### 6.2 四档延迟及执行意图

默认 `visibility_delay_seconds=5`，模拟轮询相位为每分钟xx:10，人工延迟分别0/60/180/300秒；latency参数进policy/spec/hash，不在运行中自动校准。上游输入在xx:10之后才可见则等下一轮。实际线上时延记录替代默认假设时另立request。

`eligible_at = advice_available_at + human_delay`；仅使用开始时刻严格晚于eligible_at、严格早于当前有效valid_until且属于有效session的执行bar。例：09:31结束的bar于09:31:05可见、09:31:10提醒，则D0最早09:32开盘（结束标记09:33），D1最早09:33开盘。D1/3/5若超出首个90秒有效期，必须有等待期间连续重新验证的同机会续期，不能拿首次状态外推；没有续期就记过期。报告实际秒数，D0不是零总延迟或收益上界。

本窗口delay_intents只输出计划/机会/场景、eligible_at、所需下一执行slot、取消/过期等，不执行成交或计算账户收益。待LocalSIM接入后，消费同一合同按 `NEXT_ELIGIBLE_BAR_OPEN_V1` 产生模拟成交；价格/数量仍检查原风险、资金、可卖量和硬限价。完整执行bar的OHLCV若用于事后成交近似不得提前给核心；触价不证明成交，零量/限制/缺数据不能默认成功。

同机会流的延迟敏感性诊断与LocalSIM反馈闭环分开：前者冻结机会，不让D0成交重写D5机会；后者各场景独立账户/状态，成交不同可改变下一批时间。不能把二者混为只改变延迟的净效应。低延迟未产生机会或晚延迟未成交也在原计划分母中。

### 6.3 后续LocalSIM经济验证合同（本次不执行）

保留 `LOCALSIM_NATIVE_TWAP` 原行为对照和 `MATCHED_SIMPLE_EXECUTION` 同成交代理基准；后者即B0语义，和候选共享人口、计划、价格硬约束、费用、时延、估值/终止日期。不能把原TWAP收盘代理与新open代理之差全部归因于择时。C1只有合法价集的人口可比，同时报告原始缺价集比例。

LocalSIM沿用自身冻结费用/父订单身份，分批不得重复或漏收最低费用；个人分项费用只做独立带hash的敏感性说明。无冲击假设不等于无成交约束。不改当前TWAP账户或政策默认值，不为此建立第二套公司行动清算。

主观察D1，D0/3/5完整报告；至少包含全计划完成率、等待秒数、买卖方向统一的价差、未成交机会成本、费用后共同终值差、MDD、提醒数。配对估计按股票/日期相关性分组，预先固定端点/family，不只用成交子集；研究数据被反复查看时标探索性。任何统计支持都不是上线前置，也不能把仅功能可用说成收益已确认。

### 6.4 最小验证矩阵

| 集合 | 业务必须证明的行为 |
|---|---|
| Core | B0/C1/C2手算正反例、m5/15格/分批时间、未知数量、价格多段、已结束bar、未来后缀扰动不改截至t决策 |
| Time/Input | 手/股一次转换、raw价坐标、source时间/迟到、5秒接纳、缺格/午休/14:57边界、已消费bar修订、TDX失败不换源 |
| API/Store | 同key重试、409、预算partial不丢计划、CAS取消竞态、写transition后崩溃、claim响应丢失、GET无写入、无订单/跨模块表写 |
| UI | 隐藏暂停/恢复先同步、迟到旧revision丢弃、无量只显示比例、过期不弹、claim失败状态可见 |
| Replay | 逐格与批载前缀一致、四场景独立、时间例子、未成交/缺口计数、manifest/inspect/retry不可变 |
| Deferred LocalSIM | 状态保存/恢复、partial确认、lot/T+1/资金/limit/suspend、费用/尾单、native回归、与HTTP同输入核心输出一致 |

只跑改动点最小测试、L0/changed-file和F2文档校验，模块必要回归交CI；不重复全平台套件。未来模型引入再补标签成熟、purge、训练截止和fit泄漏测试，本版不假装已经训练。Oracle仅事后上界诊断，不能传入本版核心输入。

## 7. Implementation Plan / 延后接入与转交条件

本窗口实现按一条连续任务推进：contracts/core/input/store/service → API/组件 → 决策回放/最小回归/多轮修复。不是七个独立阶段，不设置逐文件审批。完成后交付源码、合同版本/hash、历史功能回放和API/UI证据，并如实报告需要的用户后端重启。

然后才将[LocalSIM接入需求](../handoff/position_timing_intraday_localsim_integration_request_20261023.md)交给模拟盘窗口。转交所需是**实际可调用的纯函数与固定schema、策略参数、可重放例子**，不是等待模型盈利。LocalSIM窗口负责其TWAP-only上位合同、注册/adapter、延迟/成交反馈和自身回归；本窗口不绕过owner先改它。

荐股窗口在组件/API可用后仅挂载并提供resolver；在此之前本模块页面独立可用。source_verified、core/API可用、consumer_integrated、runtime_verified和economic_evidence各自报告；两个消费者都验收前不称完整双接口产品完成。

## 8. Risks / Rollout / Rollback / Production Gates

| 风险 | 处置 |
|---|---|
| 规则参数未经经验校准 | 明确v1可复现工程起点，冻结后研究，不宣传最优/预测能力 |
| 分钟晚到/修订 | 5秒与1格是显式假设，实际采样单独报告；typed失效不覆盖历史 |
| 模拟适配尚不存在 | 标PENDING，不阻核心/提醒开发，不伪造账户收益 |
| 信号有效期与延迟冲突 | 等待中每分钟重新验证同机会，延迟起点不重置，取消失效即不执行 |
| 多标签页/崩溃 | CAS与claim幂等；持续状态列表可见，不承诺用户送达 |
| 依赖源计划未核实 | 无resolver不能冒充ADVISORY来源；MANUAL仍可独立使用 |

源码上线仅挂载新namespace和本模块页面，不改旧日频cards/alerts、原策略/模型路由、共享默认值或其他模块。未配置模型不会加载模型，第一版无需模型资产。回滚关闭新页面入口/不再创建新revision，保留不可变证据；不覆盖历史策略和在途LocalSIM实例。

本轮实现与验证：database/dataset/用户runtime/process均noop；仅在独立worktree安装既有lockfile声明的前端测试依赖、运行隔离Playwright前端和冻结功能fixture回放。无训练/账户收益实验，无生产配置修改或数据库DDL。backend重启始终用户执行。

## 9. Design Acceptance Index

本表验收本模块实施范围。`SOURCE_VERIFIED`表示源码与本地功能证据闭合，不表示生产已加载或有收益；`CONTRACT_VERIFIED`用于用户明确延后至owner的消费/经济合同，不伪称这些消费者已经接入。LocalSIM/Advisory接入、后端重启后核验及经济评价分别保持待完成。

| ID | 验收设计条款 |
|---|---|
| F-101 | 先独立核心/提醒，LocalSIM延后owner接入，不扩展到MiniQMT/实盘 |
| F-102 | 精确文件范围、纯核心与已有基础复用 |
| F-103 | 严格输入、来源resolver、人工未知状态、计划revision |
| F-104 | bar闭合/可见时间、时段/单位/新鲜度和来源失效 |
| F-105 | 三个固定规则、累计分批比例、15格/5格和缺价集语义 |
| F-106 | 机会/decision分离、失效更新、领取不等于成交 |
| F-107 | API写读分离、批预算、错误语义与无交易能力 |
| F-108 | 原子head、CAS、幂等、崩溃恢复与有界证据 |
| F-109 | 可嵌入组件、先本页、源同步、旧revision与过期显示 |
| F-110 | 离线前缀回放、冻结输入及inspect/exact retry，不建账本 |
| F-111 | D0/1/3/5时钟和失效；延迟诊断与反馈闭环区分 |
| F-112 | native/matched基准、费用、全计划经济评价延后 |
| F-113 | 最小业务测试与未来泄漏防护 |
| F-114 | 转交条件、实现/接入/运行/经济状态分离及无额外审批 |

### 9.1 Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | intraday/__init__.py、service.py；本文§1/§7 | backend/tests/position_timing/test_isolation.py | SOURCE_VERIFIED | none |
| F-102 | backend/services/position_timing/intraday/；本文§2 | backend/tests/position_timing/test_intraday_core.py | SOURCE_VERIFIED | none |
| F-103 | intraday/contracts.py、service.py | backend/tests/position_timing/test_intraday_api_store.py | SOURCE_VERIFIED | none |
| F-104 | intraday/inputs.py、core.py | backend/tests/position_timing/test_intraday_core.py | SOURCE_VERIFIED | none |
| F-105 | intraday/contracts.py、core.py | backend/tests/position_timing/test_intraday_core.py | SOURCE_VERIFIED | none |
| F-106 | intraday/core.py、service.py | backend/tests/position_timing/test_intraday_api_store.py | SOURCE_VERIFIED | none |
| F-107 | backend/routers/position_timing.py、intraday/service.py | backend/tests/position_timing/test_intraday_api_store.py | SOURCE_VERIFIED | none |
| F-108 | intraday/store.py、service.py | backend/tests/position_timing/test_intraday_api_store.py | SOURCE_VERIFIED | none |
| F-109 | frontend/src/app/position-timing/IntradayTimingPanel.tsx、page.tsx | frontend/tests/position-timing/position-timing.spec.ts | SOURCE_VERIFIED | none |
| F-110 | intraday/replay.py、inputs.py | backend/tests/position_timing/test_intraday_replay.py；artifact:docs/analysis/position_timing_intraday_phase1_implementation_20261023.md | SOURCE_VERIFIED | none |
| F-111 | intraday/replay.py | backend/tests/position_timing/test_intraday_replay.py | SOURCE_VERIFIED | none |
| F-112 | 本文§6.3；LocalSIM延后接入需求 | artifact:docs/handoff/position_timing_intraday_localsim_integration_request_20261023.md | CONTRACT_VERIFIED | none |
| F-113 | 三个intraday测试、既有API/隔离测试与前端用例 | artifact:docs/analysis/position_timing_intraday_phase1_implementation_20261023.md | SOURCE_VERIFIED | none |
| F-114 | 本文§7/§8及转交包 | artifact:docs/handoff/position_timing_intraday_localsim_integration_request_20261023.md | CONTRACT_VERIFIED | none |
