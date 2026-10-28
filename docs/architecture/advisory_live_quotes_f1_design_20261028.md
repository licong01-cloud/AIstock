# Advisory 最新价格与参考涨跌 F1 设计

## 背景

用户要求所有荐股股票列表显示TDX最新价格，交易时段更新实时行情，非交易日自动取最近收盘价，并检查大部分参考收益为空的问题。2026-10-28只读核对：四个有当前名单的任务共90行，其中53行没有episode、等待进入确认；其余37行有过程收益。各包抽查已有episode收益均可用。空值主要来自把“推荐后的价格变化”与“进入后的过程收益”放在同一列，不能伪造进入确认来填补。

## 范围

覆盖工作台当日所有分组、持续跟踪、历史记录、逐日调整、模型股票名单、股票抽屉，以及管理入口各股票表格。非股票列表（名单版本、逐日评价汇总等）不加无意义价格列。属于Advisory单模块只读展示扩展；复用现有TDX HTTP与交易日历，不修改调度、模型、交易或共享数据生产。

## 边界

原Top50/模型/名单、episode进入退出、参考收益结算及胜率统计不变。实时行情不能作为历史PIT、模型输入就绪、成交确认或数据库补数。无DDL/DML、无拟合、无服务操作。旧已结束收益不随当前价格变化。历史页面的新价格列明确为“当前”，不冒充所选历史日价格。

## Design Acceptance Index

- F-001：TDX专用只读报价合同，单位元、批量身份校验、有限值正价，缺失和来源错误可见。
- F-002：正式交易日历+上海时钟选实时/午间/最近收盘；非交易日不取昨收字段代替最近收盘，收盘价使用TDX原始日K及其真实日期。
- F-003：工作台和管理入口的全部股票列表、抽屉一致显示价格/状态/读取时间，历史名单不改变报价时点。
- F-004：当日列表明确“较推荐参考价涨跌”=(最新价/原输入日参考收盘价−1)，无episode也可展示；说明是未复权价格变化、不计入过程收益或胜率，跨除权不解释成总回报。
- F-005：过程收益维持原逻辑；空值显示待进入/待结算/价格缺失原因，不补零。抽屉同时区分报价涨跌与过程收益。
- F-006：可见股票注册去重、50只分批、分页/切包取消过期结果；15秒可见刷新、后台标签暂停；失败后不把旧值当成新行情。
- F-007：直接合同测试、真实TDX独立进程读回和CI UI/类型检查；审核全部需求，无占位交付。

## 实施方案

新增GET `/advisory/workbench/quotes?symbols=...`，最多100个规范证券代码；返回按代码映射的price、kind、trade_date、observed_at、reason_code、source=TDX、price_basis=RAW_CNY。不持有数据库事务等待外部行情。服务缓存有界且区分日历日/时段，失败短缓存，实时约10秒、收盘约5分钟。TDX批量报价每次50只；日K使用`/api/kline-all/tdx?type=day&limit=2`，保留真实日期并剔除尚未收盘/未来条目。日K读取有限并发、请求超时和批次总等待上限；单股失败隔离。日历不可读则明确不可用，不猜周末。

前端共享QuoteProvider注册当前挂载股票，合并去重批次，QuoteCell和ReferenceChange消费同一结果。请求为GET；后台服务调用TDX查询用其原有POST批量协议，不执行任何采集任务。TDX实时协议没有完整交易日期，读取时刻与源交易时刻分开，不能伪造源时间；日K则显示原始bar日期。零成交/停牌不自动标成正在交易；无可靠价显示原因。

当日参考涨跌基价仍来自既存已发布名单的原输入日close，报价早于基价日期时不计算。过程收益不被该显示数覆盖；接口/数据库return_bps保持原意。新加入或待进入记录明确“未确认进入”，观察候选不冒充已持有。错误行独立展示，页面原名单/分页不因报价失败消失。

允许文件：`backend/services/advisory_workbench/quotes.py`、`backend/routers/advisory_workbench.py`、`backend/tests/advisory_workbench/test_quotes.py`、现有`test_api.py`；`frontend/src/components/advisory/LiveQuotes.tsx`、工作台6个股票展示组件及Workbench、AdvisoryManagement、`frontend/tests/paper-v2/advisory-workbench.spec.ts`、本设计和交付证据。发现新文件需求先更新此清单。

实现清点补充（仍属用户“所有股票列表”范围）：管理入口嵌入的`frontend/src/app/paper-v2/advisory/historical-range/HistoricalRangeResearchView.tsx`两张证券表，以及`frontend/src/components/advisory/EconomicEntryValueCard.tsx`、`SectorEntryPriceCard.tsx`的证券卡片也纳入允许文件；仅增加当前报价展示，不触发或修改其研究/模型操作。

对应原管理页回归`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`与历史展示回归`frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts`允许增加只读报价fixture及表格列断言，不放宽其原有业务断言。

验证清点发现历史展示spec尚未纳入现有Advisory隔离UI计划。允许对`frontend/tests/advisory-mocked.config.ts`及`tests/aistock_validation/catalog/{file_ownership,module_registry,test_plans}.yaml`增加该spec的准确归属和执行映射；复用既有runner，不启动业务backend、不增加新流水线或更改工作流政策。

## 验证方案

纯合同覆盖休市、盘前、午间、收盘、停牌/零价、代码重复/越界、单位、批次部分失败与日期错配；路由只读及参数限额。前端回归新增股票无episode有价格涨跌、旧收益不被覆盖、失败可见、所有股票表头、切页不串股。真实TDX用数只实际证券独立进程读回，不启动测试后端、不操作生产服务；CI负责较宽UI和相邻回归。设计先经独立PR合入，代码验收完成后再合入。

## 风险

报价与本地日线更新时间不同；明确显示来源和日期。跨除权价格变化不是含分红总回报。TDX返回无值或日历失败保持不可用。盘中请求成功只证明读取到TDX快照，读取时间不等于交易所逐笔时间。服务缓存和浏览器定时器不能修改不可变名单身份。

## 生产门禁

用户已授权开发及多轮审核后提交合入；后端重启由用户执行。数据库及数据集变更noop。源码合入、运行加载和真实业务验收分别报告。

## 设计验收矩阵

下列verified指源码逐项审核、直接合同及真实TDX独立读回；完整UI执行由PR CI负责，必须通过后才合入，当前尚不声明生产运行验收完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | quotes.py、quotes路由 | backend/tests/advisory_workbench/test_quotes.py | verified（源码审核/直接合同） | 无 |
| F-002 | quotes.py时钟/TDX日K | backend/tests/advisory_workbench/test_quotes.py | verified（源码审核/直接合同） | 无 |
| F-003 | LiveQuotes和全部股票组件 | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified（源码审核/直接合同） | 无 |
| F-004 | DailyList、ReferenceChange | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified（源码审核/直接合同） | 无 |
| F-005 | EpisodeTable、StockDrawer | frontend/tests/paper-v2/advisory-workbench.spec.ts | verified（源码审核/直接合同） | 无 |
| F-006 | QuoteProvider、后端缓存 | backend/tests/advisory_workbench/test_quotes.py、frontend/tests/paper-v2/advisory-workbench.spec.ts | verified（源码审核/直接合同） | 无 |
| F-007 | docs/operations/evidence/advisory_live_quotes_20261028 | backend/tests/advisory_workbench/test_quotes.py; artifact:docs/operations/evidence/advisory_live_quotes_20261028/real_read.json | verified（源码审核/直接合同） | 无 |

## 审核记录

第一轮：根据真实90行/53行无episode事实修正收益定义，保留过程收益统计；避免以最新行情伪造进入或退出。
第二轮：扩充到管理入口所有股票表、模型观察和抽屉；日K必须走TDX raw端点，不能使用默认同花顺前复权接口；添加源时钟、非交易日和失败过期语义。

第三轮实现审核：扩展管理页嵌入证券表；未成交快照不伪造源交易日期；批次超时清除未完成批次旧值；行情错误不隐藏原名单；已结束收益日期使用实际退出日。直接后端合同30项及补充未成交反例1项通过，真实TDX三股日K/快照价格一致；TypeScript、changed-file lint和L0通过（既有管理页两条Hook提示保留）。UI回归交PR CI，运行加载待用户重启。

第四轮：新纳入执行的历史展示spec仍访问工作台改版前路径；改为现有`/paper-v2/advisory/manage?view=...`管理入口，保留全部历史研究业务断言，继续执行完整UI回归。
