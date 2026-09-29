# Advisory ENTRY_PRICE 独立价格角色 F2 详细设计 v1.2

> 日期：2026-09-28；Feature tier：F2；业务归属：Selection Center / Advisory。
> 状态：SOURCE_IMPLEMENTED_COORDINATE_V2_LOCAL_VERIFIED_CI_PENDING（2026-09-29）。provider-compatible v2 坐标源码及定向回归已完成；尚未合入、确认模型或激活生产角色。文中的生产验收条件继续有效。
> 父级：[策略条件化模型蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) §5.3.1、§5.6、§16。
> 配套：[一次性历史确认](advisory_entry_price_confirmation_f2_design_20260928.md)、[绑定及每日运行](advisory_entry_price_delivery_f2_design_20260928.md)。

## 1. Background / 当前事实与目标

已有 v3 三分位头 `30e8a75b4b321a0be31ea6b8530c5bbc0afd2f81d28c2977e74e57226ddf1081`、v4 校准包 `508fedfeb48a168792d6650b06cb197556b72f54c48d97a7fa9f6b2075de437f`，名义 coverage 为0.8，模型输出为次交易日有效开盘 gap 的 q10/q50/q90。80日/1600行历史连续空间 coverage=0.733125、业务tick投影 coverage=0.81375；这些结果已消费。rolling-20D CQR selected=0，不能回选为本次候选。

2026-09-28只读核查发现：自然价格预测和结算目录仍仅有2026-09-15一日/20行；日常 P0-D 的22条 observation 属于另一模型，不能计作价格确认。已有 API 返回 PRICE_RANGE_UNAVAILABLE。未核验的其他节点不能被算入样本总量。

代码根因有三层：`model_inference.py::_model_shadow` 在 meta-label 分支直接返回价格 unavailable；`_price_range_shadow` 要求 M3 成功；`price_range_inference.py::_project_candidate` 算出entry后仍因缺 holding/path 整行失败。V1候选合同也要求 TP/protective/SL 全部存在。只改UI或只调用旧binding发布函数都不能交付独立买入区间。

目标：在同一Program的合法候选和D截止特征具备时，独立提供T日开盘参考区间；M3或Ranking模型不可用不能抹去已算出的entry。价格coverage不等于收益胜率，区间上界不是自动拒买阈值，中位数不命名为最佳买价。

## 2. Scope / 完整交付边界

本切片交付一个ENTRY_PRICE角色、版本化API、独立推理、UI及与每日/历史共用的纯价格内核。完整四角色价格信封仍可经legacy通道输出；单独entry是本次明确批准的交付单元，不能记为止盈止损或全荐股模型完成。

只支持已确认的 exact package、style、policy、feature schema 和候选来源。当前v4仅属于 `pkg_ma_8ec5e389fa2c5e484a1ac7e9`。全市场/单指数/多指数的接口应兼容，但新股票池必须有对应确认范围；后置过滤不自动获得跨池有效性。

## 3. Non-goals / 模块与操作边界

不改变QE、Selection、StrategyPackage、Paper、Execution、Position Timing源码；不训练新Alpha、不扩池、不改排名、不添加资金权重/订单/分钟特征。不改v3/v4模型、校准量或历史artifact；不把本次版本升级当成新模型证据。不新增数据库schema、外部服务或通用角色平台。

## 4. Architecture / 角色与纯函数边界

调用顺序为：冻结候选及D/T身份 → ENTRY_PRICE专属binding解析 → 加载v4三个头及原特征schema → 只读D截止特征 → q10/q50/q90及原校准 → 同核价格投影 → 版本化entry输出。

Ranking与M3按原路径运行，结果与entry按symbol关联；其模型失败不短路entry。候选来源或必需特征本身缺失仍阻断entry，因为独立角色不创造候选或替代父包输入。父Alpha score等103特征中的实际依赖保留，冻结模型的parent/outcome ID仍作为训练来源身份校验；独立运行不要求执行M1排序或M3推理。

已拆出 `score_entry_price_bundle` 作为无M3依赖的接口：输入 LoadedAdvisoryPriceRangeBundle、feature matrix、D/T、逐股PIT context，返回与输入候选同数、同序的entry结果。它与原 `score_price_range_bundle` 共用 `predict_entry_quantiles`、`project_entry_price`，旧路径继续原M3投影并保持V1成功/失败语义。`_entry_band`和法规投影复用，历史与每日不复制公式。

`load_frozen_price_range_bundle(...)` 已有显式加载入口，继续校验 v4 package、manifest、style、parent/outcome来源ID及所有member hash。来源ID不取自当前Ranking descriptor；不得把P0-D bundle ID冒充v4训练父模型。旧 `load_exact_price_range_bundle` 与 `publish_price_range_binding` 是包级V1路径，不能用于Program级新角色的发布。

## 5. Contracts / 新合同与旧版兼容

### 5.1 API及版本

保留 `GET /api/v1/advisory/programs/{program_id}/model-shadow?target_trade_date=...` 默认V1行为；已实现显式查询参数 `price_contract=entry-v2`，未指定时仍按legacy-v1。新版UI显式请求entry-v2并传入 `entry_list_version_id`，消费者可继续使用原接口。新分支以附加 `entry_price` 字段返回 `advisory_entry_price_envelope_v2`，旧 `price_range` 不变。未知版本返回422。

顶层Ranking `status=MODEL_UNAVAILABLE` 不代表新价格子信封不可用，UI按子信封自身状态展示。新entry GET只读已发布prediction，不即时计算或发布预测、binding、数据库记录；每日持久化由配套delivery通道负责。显式历史list只读原binding下唯一匹配的冻结结果，不把当前模型套到旧荐股单。旧Ranking GET计算语义不变。

### 5.2 V2信封

必须字段：`schema_version`、`role=ENTRY_PRICE`、`objective_contract=RISK_MANAGED_ADVISORY`、`evidence_state`、`availability_status`、program/binding/package/manifest/style/policy/universe/schema身份、D/T、role binding hash、price bundle/member identity、`training_lineage={parent_bundle_id,outcome_bundle_id}`、`price_basis=UNADJUSTED_CNY_DECISION_CLOSE`、nominal coverage、calibration state、candidate_count、available_count、unavailable_count、candidates及reason。

`evidence_state` 为 `EXPERIMENTAL` 或 `CONFIRMED_PRICE_DISTRIBUTION`；后者只证明确认范围中的预测分布，不声明交易收益。`availability_status` 为 AVAILABLE/PARTIAL/UNAVAILABLE：全部候选entry可用为AVAILABLE、部分可用为PARTIAL、全不可用为UNAVAILABLE。身份整体失败时可空数组；已识别合法候选后的逐股失败必须保留行。零候选单独返回 `NO_CANDIDATES` reason，不把0/0视为AVAILABLE。

每行：symbol、D截止真实reference price/date、target_raw_price_multiplier、tick_size、法规范围、`entry_price={status,raw_range,calibrated_range,calibration,reason_code,message}`。range包含condition=NEXT_TRADING_DAY_VALID_OPEN和正有限low/mid/high，单调且合法tick。entry不可用时所有数值为null且reason必填。校准量0也如实标记，不代表coverage达标。

另外三个角色固定为独立子对象：`take_profit`、`protective`、`stop_loss`，各自包含status、payload、source identity、reason。仅当既有完整投影成功并且M3来源/候选/政策完全匹配时映射；没有匹配来源则UNAVAILABLE/OUTCOME_ROLE_UNAVAILABLE，不能拿规则百分比填充。ENTRY_PRICE顶层availability仅统计entry，另设`auxiliary_availability`描述辅助角色，不能把entry可用误展示为全部能力可用。

身份字段的必填规则按availability分支：AVAILABLE/PARTIAL必须完整；UNAVAILABLE且未解析到binding/model时允许对应identity为null并给出精确reason，不得编造ID，candidates为空且计数为0。合法roster已确定时保留逐股失败行及已知身份。所有嵌套合同extra=forbid。接口源码已实现，但未合入/重启，不能声称运行中后端已具备新参数。

### 5.3 PIT、正常缺失与金额

D为决策交易日，T为下一交易日；特征行和可见时间均≤D截止，目标日结果只在独立settlement读取。正常停牌造成D缺收盘时，保留候选并使用已验证的更早最后收盘，记录真实价格日；零合法历史价时逐股UNAVAILABLE。不能删股票、补零、用T价、或从未来标签推断是否停牌。

先遵循v4标签及价格投影的既有定义，再做tick/法规投影。连续评价坐标为 `open_T / (decision_raw_close * target_raw_price_multiplier) - 1`，multiplier仅来自D时已公告且有效的公司行动。训练标签是否与此坐标一致必须在开发数据上做parity审计；不一致时记为模型/标签问题，不能在holdout中偷偷改公式。CNY与`open_li/1000`转换只在数据adapter一次完成。

## 6. Implementation Plan / 精确源码范围

实施前从最新origin/main创建Advisory工作树。复用已存在的 `PostgresRealtimeFeatureSource.load`、`build_advisory_feature_matrix`、`load_frozen_price_range_bundle`、`resolve_regulatory_price_range`；仅在以下明确文件范围实现：

- 新增 `backend/services/advisory_model_first/entry_price_contracts.py`、`entry_price_service.py`。
- 修改 `price_range_inference.py`、`model_inference.py`、`price_range_runtime_bundle.py`（均在同目录）。
- 修改 `backend/routers/advisory.py`、`frontend/src/lib/api/advisory.ts`、`frontend/src/app/paper-v2/advisory/page.tsx`。
- 对应定向测试新增 `backend/tests/advisory_model_first/test_entry_price_contracts.py`、`test_entry_price_service.py`；复用`test_price_range_inference.py`、`test_model_inference.py`和`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`。

顺序：新合同和纯entry scorer → 注入式独立service → version参数与UI → legacy兼容 → 配套confirmation读取同核输出。Program级binding实现范围由delivery设计独占，禁止在此临时写包级binding。输入adapter若需扩展先在本设计登记精确Advisory路径。

## 7. Verification Plan / 结果验证

核心验收：P0-D存在但无M3时entry成功；Ranking未绑定但已有合法候选时entry成功；M3异常仍保留entry及独立错误；父包/候选/必要特征失效时如实失败。新旧路径在完整输入下q10/q50/q90、校准及entry投影逐值一致。

再覆盖未来行情毒化不改变预测；正常停牌、更早close、除权、ST、涨跌停、未知缺行；重复symbol与identity mismatch；GET零写入；跨Program/股票池拒绝；V1默认输出兼容。UI通过真实schema fixture验证独立展示，不写源码字符串快照测试。

审核按合同/PIT、集成/兼容两轮分别执行，修复后复审受影响条款；本地定向测试与F2校验，CI运行对应Advisory计划，避免重复全模块回归。设计合入检查 `git diff --check` 和F2结构，不把文档通过写成业务通过。

## 8. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-510 | ENTRY_PRICE不依赖M3/Ranking推理成功，但保持候选与实际特征依赖 |
| F-511 | V2显式请求、V1默认语义不变、角色可用性独立 |
| F-512 | 冻结v4来源身份保持，不能把当前P0-D替作训练parent |
| F-513 | 单日/批量共用纯entry scorer、PIT及金额投影同核 |
| F-514 | 正常缺失保留、候选唯一、异常逐角色可见，无规则替填 |
| F-515 | UI展示参考区间和证据含义，不承诺收益/成交或附加交易动作 |
| F-516 | 确认前只具备源码/实验能力，源码合入不发布binding |

## 9. Design Acceptance Matrix

本矩阵保持稳定验收ID；SOURCE_VERIFIED只表示源码合同和定向回归通过，不等于模型确认或生产完成。UI类型检查与隔离浏览器用例均通过，CI仍需最终通过。真实开发数据坐标审计已由 v2 生产器闭合；窗口资格、正式确认、生产binding与运行读回仍未完成。已批准源码/历史回归先行，不豁免这些生产条件。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-510 | §4；entry_price_service.py | backend/tests/advisory_model_first/test_entry_price_service.py | SOURCE_VERIFIED | none |
| F-511 | §5；entry_price_contracts.py/API | backend/tests/advisory_model_first/test_entry_price_contracts.py；test_forward_api.py | SOURCE_VERIFIED | none |
| F-512 | §4；entry_price_service.py/既有bundle loader | backend/tests/advisory_model_first/test_entry_price_service.py | SOURCE_VERIFIED | none |
| F-513 | §5.3；price_range_inference.py | backend/tests/advisory_model_first/test_entry_price_service.py；test_daily_price_envelope_pit.py | SOURCE_VERIFIED | none |
| F-514 | §5.2～§5.3 | backend/tests/advisory_model_first/test_entry_price_service.py | SOURCE_VERIFIED | none |
| F-515 | §5.1；advisory/page.tsx | frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts：independent entry用例，隔离端口3312/无后端，1 passed | SOURCE_VERIFIED | none |
| F-516 | §10～§12；配套delivery设计 | backend/tests/advisory_model_first/test_entry_price_role_binding.py | SOURCE_VERIFIED | none |

## 10. Rollout / Rollback

代码交付后保持ENTRY_PRICE未绑定；旧用户沿用V1，新UI识别角色未配置原因。源码合入后由用户重启加载；确认通过后显式发布binding，再读回V2。确认实验可在重启前从任务源码离线完成。回滚只撤回ENTRY_PRICE指针或UI版本选择，不轮换Ranking、不删除bundle。

## 11. Risks / 直接处置

解耦运行依赖不等于训练来源独立，保留manifest来源校验；schema跨版本不允许静默转换失败；辅助角色错误不提升为整个Program失败；包级旧binding不能复用为Program角色指针。v4价格模型不合格时本次功能仍未完成生产交付，必须报告待改模型及已完成源码。

## 12. Production Gates / 完成状态

本次源码的DB、训练、binding、重启均为noop。实现限Advisory，后端重启由用户执行。DESIGN-COMPLIANCE-001逐项核对：本次角色源码不冒充生产完整交付；不吞错；不改原候选/排名/规则基线；不新增审批平台。数值确认标准只在配套confirmation合同定义一处。

## 13. 2026-09-29 实施审核

合同/PIT与集成/恢复多轮审核后修复：零候选不调用特征/模型；自然capture只选PUBLISHED荐股单；GET不刷新日历文件；原子发布时复查角色CAS/开盘边界；历史结果逐项核对完整scope。既有136项定向回归通过；BUG-1623追加的投影、确认、角色、历史回放和日常交付矩阵为108项通过。三份前端改动TypeScript诊断为0。CI未映射Advisory浏览器用例，因此使用现有lockfile匹配依赖和Playwright自有临时前端单独验证对应一条用例：1 passed，未启动/连接用户后端。

训练标签代码使用Qlib复权open/close比，运行时使用raw close及D可见公司行动投影。BUG-1623把新数值语义登记为`advisory_entry_price_core_v2`：读取D日raw close与D日`market.adj_factor`，按已在D日可见且T日实施的公告计算理论除权参考价，使用A股0.01元tick作ROUND_HALF_UP，再把目标factor按供应商四位精度投影；不读取T日行情或T日factor。旧v1合同和artifact保持不可变。

只在已消费validation的1,000行执行v2坐标审计：可用1,000、缺失0，最大绝对gap差`1.1347649842008423e-7`，低于预注册`1e-6`；3个公司行动行的差分别为`5.6567e-8`、`4.2966e-9`、`4.8889e-9`。该PASS只关闭数值坐标前置项，不是模型效果或收益证据，未读取sealed holdout。新回放请求必须显式绑定v2生产器身份；旧请求仅允许读取已完成artifact，不能以新语义重新计算。

v2现在可以进入历史功能回归及正式确认的输入审核，但正式执行仍受QE不并行、连续合格窗口、完整vintage/消费材料约束；未满足时不得发布ENTRY_PRICE binding。
