# Advisory 荐股工作台详细设计

日期：2026-10-28。分级：F2，Advisory 只读业务视图、API 与前端联动。设计依据：用户确认的卡片首页、策略详情和当日推荐三张效果图，以及当前发布/结算/收益合同。用户已授权本设计经多轮审核后合入，再开发、审核、验证并合入代码；用户后端重启仍由用户执行。

交付状态：详细设计已在 PR #6776 独立合入。实现经三轮自审修订、后端直接测试及合成异常/真实只读结果浏览器验证；以下矩阵对应源码交付。用户后端重启、运行身份核验与经济效果仍分开报告。

## 1. 背景与范围

旧页面将任务管理、当前跟踪池、名单版本、收益和多个研究面板放在同一长页。最早创建的多 Alpha 父包默认被选中；其最新有效名单停在 10 月 9 日，输入截止 10 月 8 日，后续请求因逐 seed 冻结资产缺失而失败。旧成功状态与行情估值更新不能证明推荐发布正常。

2026-10-28 再核查 LSTM 最新发布目标日为 10 月 28 日：HOLD/ACTIVE 10 条、WATCH/PENDING_TARGET_OPEN_ENTRY 11 条、WATCH/OUTSIDE_ENTRY_THRESHOLD 19 条、WAITING/PENDING_TARGET_OPEN_REVIEW 9 条。这是核查快照，不能硬编码成页面数据。当前发布候选、进入阈值、已跟踪股票具有不同含义，范围数量不应强截成配置 TopK。

本次交付总览、当日推荐、持续跟踪、历史记录、调整记录、效果统计、模型观察、运行数据状态和股票详情。原任务配置/手工复评/历史研究入口保留到独立管理页。首页默认五个启用任务，数量以真实配置为准；允许查看暂停、草稿和归档任务。

### 1.1 非目标与边界

- 不改模型、排名、选股人口、目标数量、买卖规则、成本口径、发布/结算调度；不触发拟合、QE、回放、数据修复或真实下单。
- 不恢复缺失冻结资产，不执行生产 DDL/DML，不替代数据 owner 的每日就绪判断。
- 不删除历史记录，不把参考收益称为账户收益。重排序保持影子观察，不替换原生推荐。
- 2026-10-28用户追加授权：在全局左侧导航添加“荐股”链接直达新总览；其它导航项、分组行为和模块不变。正式工作台内容仍在原导航右侧。
- 保留旧管理能力，但日常只读工作台不挂载旧页面，不因浏览触发实时模型推理。

## 2. 视觉与信息架构

沿用已确认效果图的浅灰底、白色卡片、细边框、蓝色主操作和清晰留白，使用 shadcn-compatible tokens 与独立组件，不扩散旧 Paper v2 全局样式。

视觉参考：[总览](../design/advisory_workbench_20261028/overview.png)、[历史与股票详情](../design/advisory_workbench_20261028/history.png)、[当日推荐](../design/advisory_workbench_20261028/daily.png)。图片中的证券/数量为示意，不作为业务数据或规则来源。

### 2.1 总览

`/paper-v2/advisory` 默认总览，不自动选中任何策略。卡片身份是 Program 与当前绑定，不用策略名称作为关联主键。显示名称、模型类别、启用状态和发布运行状态；首次已发布目标日与启用日分别展示；最新目标 T、输入 D、发布时刻、估值日独立展示。

卡片主指标：跟踪中、已结束有效样本、已结束胜率、平均费用前参考收益；附加止盈/止损、待结算/缺失数量。无样本显示“暂无已结束样本”，不能将空值转成 0。默认异常优先、再按最新目标排序，不以小样本胜率自动排名。搜索名称、状态、统计来源与日期区间可筛选。

### 2.2 当日推荐

详情头部显示策略名称、推荐目标日、输入截止日、最新读取时间、刷新和返回总览。日期由服务端正式交易日历给出，上海时区解释；浏览历史日期明确为“所选交易日”，不称“今日”。默认所选目标为今天（交易日）或下一个正式交易日，不自动回退旧名单。

四个数量卡：当日荐股范围、新加入、继续推荐、今日移出。主表为范围内股票，支持全部/新加入/继续推荐筛选，显示排名、名称与证券代码、变化、连续推荐天数、跟踪进度、参考价/日期、参考收益类型。范围外老股票与今日移出单独列示；阈值外观察候选单独折叠，不计入推荐数量。

无所选日有效名单时展示原因和“查看最近有效名单”，只有用户点击才改变目标日期。禁止以旧名单填满当前日区域。

### 2.3 其他分栏与股票抽屉

- 持续跟踪：未结束或待结算 episode，区分首次入选日、有效进入日、当日名单成员身份和估值日期。
- 历史记录：所有荐股过程可筛选；默认已结束，可切全部状态。相同股票重复进入按 episode_id 分开。股票、进入日期、退出原因、来源和绑定过滤使用服务端分页。
- 调整记录：完整名单版本分页，点击切换所选名单；另展示实际复评决策及日期、进入/保留/退出原因。
- 效果统计：已结束和浮动收益分开，展示样本、覆盖、平均/中位数、胜率、平均跟踪天数、止盈止损及单次最差回撤；注明费用前/参考价格。不得推出组合累计收益、年化或账户回撤。
- 模型观察：只在用户进入时 GET 既存 `reranking-shadow` 结果；展示捕获天数、成熟天数、配对差值及原生/重排名单。没有发布、未配置、缺失或失败均显示原因；不调用旧实时 `model-shadow`。
- 股票抽屉：选择荐股过程，展示推荐→进入确认→逐日决策→退出触发→退出确认→结算。当前名单与最新跟踪结果分别标明日期；历史名单中的后续信息明确“截至当前”，不冒充历史当时已知。
- 任务管理：独立 `/paper-v2/advisory/manage` 入口保留既有配置、手工复评与历史研究能力。日常页面所有读取为 GET，不自动触发管理动作。

状态颜色：新加入蓝、继续推荐青、等待橙、结束灰；止盈/止损/排名退出等有中文标签和图标。收益红涨绿跌；亏损股票也可继续推荐，不能用收益方向决定成员身份。键盘可操作、清晰焦点、错误用 alert，移动窄屏横向表格滚动并保持页面边界。

## 3. 业务合同

### 3.1 名单成员与变化

只读投影依赖已保存的 list_version/item、前向发布/结算和 review decision，不重新计算排名。

| 原始事实 | 当日成员身份 | 跟踪说明 |
|---|---|---|
| ENTER / HOLD 且属于有效已发布名单 | IN_SCOPE | 进入/跟踪状态另看 episode 和当日结算 |
| 前向 WATCH + PENDING_TARGET_OPEN_ENTRY | IN_SCOPE | 待进入确认；不能当作已成交或已产生收益 |
| WATCH + OUTSIDE_ENTRY_THRESHOLD | OBSERVATION | 观察候选，范围外 |
| WAITING + PENDING_TARGET_OPEN_REVIEW | OUTSIDE_PENDING | 名单外仍跟踪，待复评，不等同退出 |
| EXIT | OUTSIDE_EXIT | 有退出建议，是否完成另看有效价格与结算 |
| 未识别组合/重复证券/跨绑定不一致 | UNKNOWN | 可见问题，不猜测成员身份 |

新加入/继续推荐来自同一绑定下当前与上次已发布名单的集合差/交集，不直接等同 ENTER/HOLD。移出来自上次范围内减当前范围内，退出原因仅引用真实决策。没有可靠前序链时变化和连续天数标为未知；正式首份名单且不存在更早发布时可判首次。连续天数按正式相邻交易日的有效名单连续成员记录计算，缺日或换绑定终止可证明链并标记覆盖不足，不能以前 20 条假装全历史。

成员计数恒等式：范围内 = 新加入 + 继续推荐 + 变化未知。前序身份可证明时，上次范围 − 移出 + 新加入 = 当次范围。未知不隐藏。集合变更与等待进入/退出可同时成立。

### 3.2 时钟、运行和数据

配置状态（启用/暂停/草稿/归档）、发布状态（已发布/等待数据/资产阻断/失败/无当前名单）和结算状态独立。最新失败不能被旧 last_review_status=SUCCEEDED 覆盖。区分发布失败与已发布后的结算失败。资产缺失使用结构化 reason/context 识别，不一律显示等待数据。

起始日期用整个已发布集合的 MIN(target date)，绑定起点另列；不得从当前分页计算。完整缺口仅在已知交易日历、正式起点和所选截止日范围计算，返回范围及总数；日历未知不推算周末。

复用 `DailyIntegrityService.day/readiness`：按正式 package_id、数据日期与现有依赖投影返回 all_sync_complete 与 consumer_ready/prediction_ready。它们与名单是否已发布不同。就绪合同缺表、服务不可用或依赖无法解析时显示“未能确认”及原因，不回退到小时、行数或其它标记。这里只读展示，不改变调度或新增人工门禁。

### 3.3 收益与来源

复用 `program_performance.episode_performance/performance_metrics`。胜率分母只含 SETTLED 的有效费用前参考收益；浮动与已结束分开，待退出/缺价格不能算赢。single episode drawdown 只命名“单次最差回撤”。统计范围按入选日期过滤；收益估值截止是当前读取时点，不能称历史 as-of 绩效。

来源以 episode 的首次关联 review/前向记录和不可变证据判断 FORWARD / REPLAY / UNKNOWN；不因当前存在同股票前向记录就改写历史来源。默认 ALL 明示混合，允许分别过滤；当前/历史绑定分别可选，不能漏掉未知绑定。旧数据身份不足时保留 UNKNOWN。

## 4. Architecture / 只读实现架构

新增 `backend/services/advisory_workbench/`：repository 在短只读事务内读取轻量名单、分页 episode/decision、日历和运行摘要；projection 负责成员/变化/状态；service 复用既有绩效和数据就绪能力。API 在既有 Advisory router 下接入，不触碰发布与交易引擎。

前端新增 `frontend/src/components/advisory/workbench/` 和 `frontend/src/lib/api/advisoryWorkbench.ts`。旧页面移到管理组件，原入口渲染新工作台。状态包含 program、tab、target_date、filters、page；请求带 AbortController 和 generation guard，禁止快速切换后旧结果覆盖新选择。刷新失败保留最后成功内容并明确标记；首读失败不能显示“0 个任务”。定时/窗口重新聚焦刷新仅在可见页面进行，用户翻页期间不悄然更换数据集。

### 4.1 查询与版本一致性

每次返回 `schema_version/program_id/binding_version_id/target_date/list_version_id/generated_at/snapshot_id`；分页还返回 `total_count/limit/offset`。snapshot_id 覆盖请求业务范围、列表/episode/decision/forward 的版本水位、统计时钟和数据价格版本。单次查询在一致只读事务内完成；同一分页会话后续请求携带 snapshot_id，数据变更返回 409 SNAPSHOT_CHANGED 并提示刷新，不默默拼接版本。

历史 episode 与名单版本在数据库 LIMIT/OFFSET，稳定排序日期 DESC、身份 ASC；limit 仅允许 20/50/100，上限 100。episode 收益计算只对当前页明细或服务端统计输入进行，浏览器不下载全部历史。总览不得返回 forward 的大 publication/selection JSON；所有 API 显式白名单投影。

交易日历、源错误及版本不一致是可见状态。前端只对已验证响应做展示，不把 null 转零或 rank 缺失改成入选排名。

### 4.2 API 契约

所有新增接口 GET，前缀 `/api/v1/advisory/workbench`。参数错误 422、对象不存在 404、快照冲突 409、源查询失败 503；结构化错误包含 reason_code 和中文说明，日志保留内部身份。

| 资源 | 参数与输出 |
|---|---|
| `/overview` | status、source、start/end；返回任务卡片、交易日上下文、汇总状态；不逐卡装载巨型推理数据 |
| `/programs/{id}` | target_date、source、start/end、binding；返回身份、运行摘要、收益口径、日历缺口、名单汇总与日期选项 |
| `/programs/{id}/daily` | target_date、group/change、q、limit/offset、snapshot_id；范围内、移出、名单外待复评、观察及未知各有计数 |
| `/programs/{id}/episodes` | source、binding、status、q、start/end、exit_reason、limit/offset、snapshot_id；页内 episode 参考收益与状态 |
| `/programs/{id}/episodes/{episode_id}` | 身份约束、snapshot_id；完整过程头与分页事件，不能串包或合并重复入选 |
| `/programs/{id}/versions` | 日期过滤、limit/offset、snapshot_id；全量总数与真实首日 |
| `/programs/{id}/decisions` | 日期、股票、episode、limit/offset、snapshot_id；逐日决策 |
| `/programs/{id}/readiness` | data_date；调用共享消费者合同，结果不可用明确返回 unavailable，不假装通过 |

模型观察复用现有持久化 `reranking-shadow` GET。列表内尚未创建 episode 的新股详情由已发布条目提供，不能造 episode。不能为每个股票在页面加载时查询一次模型或价格。

## 5. Design Acceptance Index

- F-001：已确认卡片总览，默认启用任务、异常可见、筛选与无自动选包。
- F-002：正式起始日、D/T/生成/估值时钟、完整缺口与上海时间。
- F-003：三维成员合同、WATCH/WAITING 真实投影与数量恒等式。
- F-004：当日主表、新加入/继续、移出/观察分区、未知与无当前名单。
- F-005：持续跟踪与历史服务端分页、重复入选身份、筛选绑定。
- F-006：股票抽屉与事件分页，退出触发/确认/结算分离。
- F-007：费用前参考收益、有效分母、来源/绑定/区间过滤、空值与单次回撤。
- F-008：独立运行/发布/结算状态及每日共享就绪展示。
- F-009：只读持久化重排序观察，旧管理和历史研究入口可达。
- F-010：稳定快照分页、并发取消、刷新失败可见、定时读取生命周期。
- F-011：真实仓库/数据库读取、白名单轻量 API、超时与 SQL 只读。
- F-012：视觉一致、响应式、键盘操作、业务中文，无 raw JSON 业务流。
- F-013：多轮审核、直接后端/浏览器证据、五任务实读及发布边界。
- F-014：全局左侧导航“荐股”入口、当前页高亮及直接跳转新总览（用户追加授权）。
- F-015：统一时间、前向来源、当前绑定/历史分段、共同日期与各自样本；费用/基准/UNKNOWN可追溯。
- F-016：主页完整指标表、净超额柱图、月度热图、参考净值和收益回撤图，懒加载且风格一致。
- F-017：有限现金、五批次重叠的标准化参考组合；净值/组合回撤与单次过程统计分别命名。
- F-018：历史名单仅从详情进入，所有保存版本分页、精确版本查看，与当前荐股共用列表。
- F-019：累计涨跌仅对应本次连续荐股期间，继续不重置、结束冻结、重新入选分段、缺证据未知。
- F-020：轻量首屏、范围批量读取、有界缓存与并发、历史页不扫描行情，性能预算和规模验证。
- F-021：后端合同、浏览器视觉/交互、真实只读与修订失效证据；源码与用户重启后的加载分态。

## 6. 实施方案与允许修改文件

设计阶段仅本文件、`docs/analysis/advisory_workbench_design_review_20261028.md`、本次三张效果图 `docs/design/advisory_workbench_20261028/*.png`、原 Advisory 蓝图当前队列的链接与边界说明。

代码阶段从设计合入的最新 main 新建独立 worktree。文件范围：`backend/services/advisory_workbench/{__init__,repository,projection,service}.py`、`backend/routers/advisory.py`、`backend/tests/advisory_workbench/test_*.py`；`frontend/src/app/paper-v2/advisory/page.tsx`、`frontend/src/app/paper-v2/advisory/manage/page.tsx`、`frontend/src/components/advisory/AdvisoryManagement.tsx`、`frontend/src/components/advisory/workbench/{Workbench,Overview,ProgramDetail,DailyList,EpisodeTable,StockDrawer,Performance,ModelObservation,shared}.tsx`、同目录 `workbench.module.css`、`frontend/src/lib/api/advisoryWorkbench.ts`、`frontend/tests/paper-v2/advisory-workbench.spec.ts`、既有荐股 UI 测试仅管理入口兼容调整、本设计与 `docs/operations/advisory_workbench_delivery_20261028.md`。精确文件清单以实施收据记录；超出范围先在设计登记原因，不自动修改其它 owner 模块。

顺序：设计三轮审核修订→独立设计 PR 合入→只读 API/直接测试→前端组件→真实数据读取与浏览器验收→语义/失败/范围三轮自审修复→代码 PR/CI 合入。无未经验证的阶段冒充整体完成。

实施文件细化：新增路由叶 `backend/routers/advisory_workbench.py`，由既有 Advisory router include，避免继续扩充旧巨型路由；其余语义与授权范围不变。页面专属测试以精确文件 `backend/tests/advisory_workbench/test_projection.py`、`test_service.py`、`test_api.py`、`test_repository.py` 登记。仅本次隔离工作树 `tmp/advisory-workbench/` 保存验证输入/输出及临时 PR 正文。

追加允许范围：`frontend/src/lib/navigation/nav-groups.ts` 与 `frontend/src/app/Sidebar.tsx`，仅新增荐股链接所需图标和注册；F-014由同一工作台浏览器测试验证，不改变其它导航。

页面容器细化：`frontend/src/app/paper-v2/layout.tsx` 仅在新工作台根路径去除旧交易应用大横幅，避免重复标题；管理页与其它 Paper 页面保持原容器。开发复用已安装 frontend/node_modules 的目录联接，不安装新依赖。


验收持久化文件细化：`docs/operations/evidence/advisory_workbench_20261028/receipt.json`、`overview.png`、`daily.png`、`history-detail.png`。截图来自 3012 隔离前端对真实只读响应的呈现，不是生产运行激活证据。模型类别仅取正式 manifest 的 model_type，缺失显示待核验，不以名称反推。

## 7. 验证方案与结果验证

提交前门检细化：新测试必须被 CI 实际执行，追加精确注册文件 `tests/aistock_validation/catalog/file_ownership.yaml`、`module_registry.yaml`、`test_plans.yaml`，`backend/services/validation/plan_catalog.py`、`noxfile.py`、`frontend/tests/advisory-mocked.config.ts`、`.github/workflows/test.yml`、`backend/tests/scripts/test_ci_change_classifier.py`。仅增加本工作台测试归属、直接执行计划及对应隔离前端匹配，不恢复业务流水线、不运行用户启动脚本。

后端直接测试覆盖前向 WATCH 进入、阈值外 WATCH、范围外 WAITING、集合变化和缺前序、换绑定、同股票多 episode、无数据/null、结算前后、来源未知、>20 版本真实首日、交易日缺口、分页过滤与快照变更、跨包身份、参数/错误和只读路径。

真实数据验证使用现有 DEV/生产只读 SELECT 与 GET，不写入业务表；数据库事务强制 read only。验证程序连接真实数据库，逐项核对五任务的卡片/日名单/历史/日期，与底层已保存记录一致；保存脱敏计数和状态收据，不保存推理大 payload。

浏览器直接矩阵覆盖总览→策略→当日/历史/抽屉、筛选分页、空值、阻断旧日期、失败刷新、并发切换、模型懒加载、无 POST 和移动端。真实读取与合成异常测试分别标明，不以 mock 覆盖宣称真实经济验收。生产端口只读，测试端口使用 3011/3012；不启动/停止用户 backend。

执行 feature validator、changed-file Ruff/TypeScript/ESLint、直接 pytest、Playwright、diff/scope 检查。广域回归由现有 CI/Validation Center 承担，未执行模块明确记录。

## 8. Design Acceptance Matrix（实现验收）

14 项按最终代码和直接证据核对；原设计阶段审核留存在独立设计 PR。PASS 仅指源码和隔离验证，运行激活仍按第 9 节由用户执行。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frontend/src/components/advisory/workbench/Overview.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified | 无 |
| F-002 | backend/services/advisory_workbench/projection.py | backend/tests/advisory_workbench/test_projection.py | verified | 无 |
| F-003 | backend/services/advisory_workbench/projection.py | backend/tests/advisory_workbench/test_projection.py | verified | 无 |
| F-004 | frontend/src/components/advisory/workbench/DailyList.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified | 无 |
| F-005 | backend/services/advisory_workbench/repository.py; frontend/src/components/advisory/workbench/EpisodeTable.tsx | backend/tests/advisory_workbench/test_repository.py; frontend/tests/paper-v2/advisory-workbench.spec.ts | verified | 无 |
| F-006 | frontend/src/components/advisory/workbench/StockDrawer.tsx; backend/services/advisory_workbench/service.py | backend/tests/advisory_workbench/test_service.py; frontend/tests/paper-v2/advisory-workbench.spec.ts | verified | 无 |
| F-007 | backend/services/advisory_workbench/service.py; frontend/src/components/advisory/workbench/Performance.tsx | backend/tests/advisory_historical_range/test_program_performance.py; artifact:docs/operations/evidence/advisory_workbench_20261028/receipt.json | verified | 无 |
| F-008 | frontend/src/components/advisory/workbench/ProgramDetail.tsx; backend/services/advisory_workbench/service.py | artifact:docs/operations/evidence/advisory_workbench_20261028/receipt.json | verified | 无 |
| F-009 | frontend/src/components/advisory/workbench/ModelObservation.tsx; frontend/src/app/paper-v2/advisory/manage/page.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts; frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts | verified | 无 |
| F-010 | frontend/src/components/advisory/workbench/shared.tsx; backend/services/advisory_workbench/service.py | frontend/tests/paper-v2/advisory-workbench.spec.ts; backend/tests/advisory_workbench/test_service.py | verified | 无 |
| F-011 | backend/routers/advisory_workbench.py; backend/services/advisory_workbench/repository.py | backend/tests/advisory_workbench/test_api.py; artifact:docs/operations/evidence/advisory_workbench_20261028/receipt.json | verified | 无 |
| F-012 | frontend/src/components/advisory/workbench/workbench.module.css; frontend/src/components/advisory/workbench/StockDrawer.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts; artifact:docs/operations/evidence/advisory_workbench_20261028/daily.png | verified | 无 |
| F-013 | docs/operations/advisory_workbench_delivery_20261028.md | artifact:docs/operations/evidence/advisory_workbench_20261028/receipt.json | verified | 无 |
| F-014 | frontend/src/lib/navigation/nav-groups.ts; frontend/src/app/Sidebar.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified | 无 |

## 9. 发布、回滚、风险和生产门禁

`production_ddl_gate=noop`，`production_dml=not_requested`，`backend_restart_owner=user`。不新增依赖。源码合入、root 同步、前端激活、后端重启、重启后验收和经济效果各自报告。源码交付可完成但运行激活仍等待用户重启；不得伪造在线页面截图。

## 10. 2026-11-09已批准增量：横向比较与详情历史名单

用户批准上一轮完整设计并修订：历史名单只在详情进入，主页不加历史入口；名单表与当前荐股相同；累计涨跌只计算荐股期间；页面统一、直观、美观。新隔离树/精确文件范围和机器证据见`X:/AIstock_temp/ADVISORY/comparison-history-20261109-v1/task.json`，本节是本轮唯一设计载体。旧已交付F-001～014保持原证据。新增叶文件`backend/services/advisory_workbench/comparison_tracking.py`承载按所选截止的过程统计；`backend/tests/advisory_workbench/test_api.py`同步只读路由数量与参数合同断言。其余精确路径均已在收据登记，无其他owner模块变更。

### 10.1 主比较与完整指标

主页先轻量显示启用包的配置、发布状态和最新名单，再加载比较。时间支持20/60/120交易日、本年、全部、自定义，默认60；点击应用才更新。默认前向、当前绑定、共同日期；各自样本、回放和全绑定显式切换，跨绑定按版本标注。所选终日也是评价截止，不能看其后的价格。未满5TD、未发布、缺价及UNKNOWN保留分母和原因；合法不可执行入场为CASH固定槽，不按剩余股票重新分配。

主评价对象是每个目标T当天正式已发布名单的IN_SCOPE固定原槽，不是只有已经进入的episode，更不重跑TopK或模型。若同包同日多个正式版本且没有唯一身份，保留冲突；名单/条目跨包或绑定、重复证券均不静默拼接。共同集要求所选各臂与沪深300都完整成熟；无共同日期显示原因，不暗删包或换窗。各自样本仅描述，不宣称独立优胜。

固定T开盘至T+4收盘，沿既有买0.95/卖5.95bps，滑点默认显式0bps，均为参考假设；沪深300默认000300.SH同窗毛价格收益，明确不含指数交易成本。股价比较复用既有adj_factor/停牌/涨跌停合同；缺来源不补。净超额=策略净收益减指数毛价格收益。Top50→Top10研究、100bps目标和V2独立，不消费或改写其工件。

完整提供5TD均值/中位数/胜率/最差/下尾10%/对指数净超额、描述性相关修正区间；已结束过程毛/净均值/中位数/胜率/平均盈利亏损/盈亏比/利润因子/持有期/退出原因，未结束浮动/缺估值/日期，原日期/成熟/有效/未知/现金/版本覆盖。主表简洁、更多指标展开；默认同窗净超额排序，不创建综合评分或自动激活。样本相关和短窗支持限制明确展示。

口径细化：过程统计沿原进入/退出价格基准的未复权参考价格；固定5TD和参考组合使用既有调整因子。均为当前权威记录的只读重算，截止后行情/进入/退出收益不参与，不声称历史当时已知的数据版本。跨口径明确标注，不改写历史记录。全部历史从所选来源/绑定的首个保存版本读取，不以固定五年截断；版本/条目/行情观察点限额超出时明确拒绝，不能静默截短。

### 10.2 标准化参考组合

这是统一研究展示，不是真实账户或Paper Trading执行。每臂及指数从共同比较起点1单位现金开始；每天开盘新增一批，预算不超过前日NAV20%和可用现金，按当日IN_SCOPE固定原槽等权，使用归一化份额；每批固定第5个市场session收盘退出，收入次日可用，不加杠杆。无名单预算留现金且记缺发布；UNKNOWN入场/持仓/退出价格不当现金，之后不可证明的净值保持null。停牌/一字涨停入场沿原合同为空现金槽；持仓无法退出时不得假售，保留未知。股数以入场adj_factor固定，估值使用同源调整比例；价格/分红口径按同一既有调整来源，不声称历史首次可得性。含费用和日终浮动，给区间收益、组合MDD、现金比例、实际投入和费用拖累；不得累乘重叠批次均值，不得拿单次最差回撤替代组合MDD。

### 10.3 历史名单及累计涨跌修订

详情新增“历史名单”，原“历史记录”改为“跟踪历史”，调整记录只保留决策；主页不增加历史名单按钮。所有保存版本可分页20/50/100、按目标日期/来源/状态筛选；正式前向和REPLAY分开。通过list_version_id读取精确名单，不再仅按日期猜版本；支持同日多版本、全部历史绑定、直达链接和返回恢复页码。

历史明细复用当前DailyList及QuoteProvider/RecommendationValue，保留同样的股票、最新价、今日涨跌、首次推荐、累计涨跌、排名和变化列。最新价明确为当前行情；累计涨跌是本次连续推荐首次生效日开盘→移出生效日开盘，继续推荐不重置、移出冻结、再次入选另起段。未结束只使用有连续名单证明覆盖的估值日；缺日期、重复版本或跨绑定中断保留UNKNOWN，不能将停更之后的涨跌计进推荐期。与5TD含费/调整后的策略评价分开，沿原表注明累计涨跌为未复权价格变化、不含费用、不计入胜率。

追加精确叶文件`frontend/src/components/advisory/LiveQuotes.tsx`，仅让既有批量推荐分段响应同时显示“截至名单目标日的连续推荐天数”，避免历史表为补这一列再扫描全历史或逐股查询；其他行情合同不变。

同一术语补齐到`frontend/src/components/advisory/workbench/StockDrawer.tsx`的累计涨跌标签。概况“最新名单范围”批量核对最新版本的IN_SCOPE，包含待确认进入，不能复用ACTIVE计数；历史目录的保存版ACTIVE计数命名为“当时跟踪数”，完整荐股范围在打开名单后展示。

### 10.4 性能与接口

现有overview仅作兼容；新增brief模式批量读取配置/最新名单/首日/运行摘要，不读episode大payload或全历史行情hash。比较拆成摘要与曲线，后者在图表可见时请求；相同参数复用有界TTL缓存、同请求去重、并发最多2，数据修订刷新必须失效，响应携带源身份和生成时刻。统计按请求范围批量读取名单/股价/调整/停牌/限价/指数，不逐股查。缓存不依赖MAX(trade_date)冒充行情内容身份，短TTL与强制刷新重新读取真实源。图表轻量SVG，不新增依赖；降采样只影响绘图，完整统计及极值保留。每条线至多800点，曲线响应目标300KB以内。

历史元数据分页不调用全历史市场watermark，不读取全部名单条目。详情单版只取该版/必要前序，收益由已存在可见股票批量接口加载。浏览器取消过时请求，选择应用后统一刷新，滚动/行情高度变化不重查名单；固定骨架高度和可见性加载避免卡顿。

拟验收预算：默认5包近60交易日，brief与历史分页P95≤500ms、比较摘要P95≤1.5s；首屏LCP P75≤2.5s、INP P75≤200ms；10包×5年/10万episode规模验证响应体、工作量随范围有界、页面持续可交互。实际测量须分冷/热、API/浏览器与样本数；未达标直接优化并复核，不将一次读取当P95或靠减少正确性检查伪达标。

### 10.5 实施与验收

顺序P0设计/精确scope→P1纯评价/有限资金组合与批量源→P2详情历史与累计合同→P3同风格图表/表格/时间筛选→P4直接合同、现有DEV只读验证/真实源对账、浏览器视觉和规模/性能→P5当前HEAD三轮自审和授权源码交付。任何DDL/DML仍无授权且本设计不需要；0新金融fit、模型/生产榜单/源资产/服务写入。后端重启继续用户执行。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-015 | backend/services/advisory_workbench/comparison_service.py | backend/tests/advisory_workbench/test_comparison_service.py | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-016 | frontend/src/components/advisory/workbench/Comparison.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-017 | backend/services/advisory_workbench/comparison_portfolio.py | backend/tests/advisory_workbench/test_comparison_portfolio.py | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-018 | backend/services/advisory_workbench/history.py; frontend/src/components/advisory/workbench/HistoricalLists.tsx | backend/tests/advisory_workbench/test_history.py | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-019 | backend/services/advisory_workbench/recommendations.py | backend/tests/advisory_workbench/test_history.py | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-020 | backend/services/advisory_workbench/brief.py; backend/services/advisory_workbench/comparison_cache.py | backend/tests/advisory_workbench/test_comparison_repository.py; frontend/tests/paper-v2/advisory-workbench.spec.ts | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |
| F-021 | docs/architecture/advisory_recommendation_workbench_f2_design_20261028.md | frontend/tests/paper-v2/advisory-workbench.spec.ts | design_approved_by_user | 用户已批准完整设计；本PR冻结规格，实现验收在代码PR核实 |

回滚通过正常代码回退恢复旧入口，不删业务数据或配置，不回退数据 owner 的就绪合同。旧管理组件保持可用以支持已有操作，但不作为新工作台失败时的静默跳转。

风险：旧 episode 来源不足→UNKNOWN；daily integrity 尚未迁移/激活→显示不可用；父包资产缺失→卡片阻断；并发写入→409刷新；昂贵统计→小请求、只读超时和明显失败；当前估值用于历史 episode→标明当前估值日期；列表成员超过配置目标→保留实际发布集合，不截断或重新排名。

四项设计符合性：完整交付按14项验收；错误和UNKNOWN可见；不改变推荐/交易语义；不引入额外审批或生产门禁。用户授权继续覆盖设计与代码两次合入，无需重复询问。

本设计PR仅冻结2026-11-09用户已批准的F-015～F-021规格；表内测试路径为实施计划，不声称本设计PR已完成代码或经济验收。源码合入、用户重启和后续运行验收继续分别报告。
