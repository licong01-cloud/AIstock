# 荐股连续推荐与实时涨跌 F1 详细设计

## 背景

2026-10-29 只读核查发现：普通四包最新名单共90条在荐记录，53条被旧UI标为“待进入”；其中15条已经存在与名单、binding、证券严格对应的ENTER决策，原不可变名单未回写episode_id。其余记录也不能因未进入内部跟踪池而被解释为尚未推荐。用户授权按本设计开发、多轮审核修复后提交合入；后端重启由用户执行。

## 范围与边界

F1，Advisory工作台单模块。名单展示以连续推荐段为单位；跟踪池episode仅用于策略过程复盘。主表列为股票、推荐状态、首次生效日、起始基准价、最新价、累计涨跌（实时）、今日涨跌；排序、连续推荐天数、名单分组和分页继续保留。

非目标：不改选股、每天替换额度、20股跟踪池、成交假设、胜率、模型、数据集、生产账本；不运行拟合、预测、QE或数据准备；不写库、不增加DDL、不操作服务。此处涨跌为价格观察，不是实际成交收益，不扣成本，也不按建议买入价假设成交。

允许修改文件：本设计；backend/services/advisory_workbench/{recommendations,repository,service,quotes}.py；backend/routers/advisory_workbench.py；backend/tests/advisory_workbench/{test_recommendations,test_quotes,test_api,test_service}.py；frontend/src/lib/api/advisoryWorkbench.ts；frontend/src/components/advisory/LiveQuotes.tsx；frontend/src/components/advisory/workbench/{DailyList,StockDrawer,ProgramDetail,EpisodeTable,shared}.tsx；frontend/tests/paper-v2/advisory-workbench.spec.ts。证据置于docs/operations/evidence/advisory_realtime_returns_20261029/。实际新增文件逐一记录，不扩大到数据生产。

## Design Acceptance Index

- F-001：名单身份、连续推荐段、binding隔离、移出及重新加入。
- F-002：首次生效日开盘基准、实时报价、今日涨跌、缺失与价格口径说明。
- F-003：结束冻结、历史视图、未来/缺失交易日、停牌与部分失败。
- F-004：不可变名单后续ENTER的严格只读关联，不伪造跟踪状态。
- F-005：主列表与抽屉呈现推荐状态和实时涨跌，策略过程统计分离。
- F-006：可见行15秒刷新、隐藏暂停、分页取消、身份变化清除旧值。
- F-007：真实只读读回、分层验证和重启门禁。

## 实施方案

### 连续推荐段与时钟

使用program、binding、首次名单ID、证券作为段身份。按已发布版本的previous_list_version_id及官方交易日逐日追溯。连续在荐不重设起始价；明确不在荐后重新加入建立新段。binding变化建立新身份，不借用旧包价格。首份名单必须明确无前版本；缺前版本、重复版本/证券、缺交易日或跨交易日缺名单均显示连续性待核验，不猜测起点。

起始日为首份名单的trade_date（生效日），基准为当日真实未复权开盘价。盘前显示“开盘基准尚未形成”，推荐状态仍是新加入。优先读取本地同日raw日线；若同日有效TDX快照有成交，用TDX Open建立只读显示基准。停牌/无成交不借用昨日开盘。查看历史名单时继续追踪后续版本，首份明确移出的生效日开盘价为结束参考价；到达该开盘时点后冻结。未来移出不提前冻结；已结束缺精确结束开盘价则显示缺失，不改用最新价。

### 收益和价格口径

累计涨跌（实时）=(当前价或结束参考价 / 起始基准价 - 1)×100%；今日涨跌=(当前价 / TDX昨收 - 1)×100%。两者不同：前者跨本段，后者只覆盖报价交易日。休市显示最近收盘日涨跌并标明日期。

本轮主指标明确为**未复权价格涨跌**，所有输入统一RAW_CNY，显示“未复权·价格变化”，并常驻说明跨除权/分红不代表投资总回报。不得将未复权价格变化命名为含分红累计收益或真实持仓收益。复权总回报不在本轮范围；这避免在当天复权因子尚未交付时前填因子或把猜测当作确认。已有策略过程收益计算保持原口径，单独显示。

TDX live读取K.Close/K.Open/K.Last并统一除以1000；昨收不可用不补零。非开盘时使用TDX raw日K，获取选定收盘日及其前一交易日收盘；相邻日期缺失不冒充昨收。行情早于起始日、暂停/无成交且无有效日期、时钟不足均不给本段涨跌。基准和结束价附来源、日期、口径。每个空值均附稳定reason_code。

### API与仓储

增加只读GET /advisory/workbench/programs/{program_id}/returns，输入1—100个规范symbols和必填list_version_id。名单ID必须属于该program且为发布版本；返回advisory_display_quotes_v1报价信封及每股recommendation字段，字段包括段身份、首次日、结束日、base_price、end_price、return_bps、reason_code；报价附open_price、previous_close、previous_close_date、today_return_bps。名单外证券仍可返回报价，但本段状态NOT_RECOMMENDED。

先在短只读快照批量获取版本、名单、日历、精确日期价格，再释放数据库事务获取TDX。最多读取4096个版本，超限明确报错；每次只计算可见symbol，不按股循环SQL。报价仍复用已有有界缓存与超时。固定名单ID可跨15秒读取，后续版本可使段结束；不得用会随估值变化的snapshot_id拒绝正常行情刷新。

后续episode关联仅接受program+list_version_id+binding+symbol的唯一ENTER决策，并核对episode的program、symbol和binding；重复不同episode显式标记冲突，不能按symbol取最新episode。保留原名单和原账本不写回。

### UI

QuoteProvider增加名单上下文，继续复用可见证券注册、15秒轮询、焦点恢复、隐藏取消及批量机制。上下文变化清空旧结果；请求失败不能留下旧实时涨跌。列表和抽屉使用同一个返回计算结果，避免公式漂移。主列表显示新加入/继续推荐/已移出，不再展示“待进入”。缺episode说明只在“策略过程复盘”中出现，不能阻止名单涨跌。

已结束段用结束价冻结累计涨跌；最新价与今日涨跌可继续观察证券行情，明确区别。策略过程记录继续保留已有收盘估值/结束收益及胜率，文字注明其不等于名单累计涨跌。

## 验证方案

直接合同：连续/重新加入/binding切换、未知前链/交易日缺口/重复身份、历史段结束、未来移出、精确价格缺失、同日TDX开盘、盘前/停牌、单位、昨收日期、结束冻结、严格ENTER关联及冲突反例。API只允许GET和有限symbols，数据库事务不跨TDX网络。前端覆盖新加入无episode也有涨跌、主表无“待进入”、两种涨跌不同、结束冻结说明、切页和失败清空。

本地执行最小直接后端合同、TypeScript/修改文件lint、diff检查；PR CI执行完整工作台及关联UI。真实TDX/生产库独立进程只读读回验证价格与身份，不启动后端。多轮审核分别核对金融时间语义、身份隔离、前端异步与错误路径、完整设计验收。

## 风险与生产门禁

未复权跨除权价格变化有机械跳变，常驻口径说明及每格标记，不混入策略胜率。TDX获取时间不等于交易所逐笔时间，保留原始时间和交易日。历史资料缺失时如实显示，不补数。源码合入、运行加载、真实UI验收分开报告；由用户重启后再读回API和UI。数据库/数据集/模型变更均为noop。

## 设计验收矩阵

此独立设计PR的verified仅指设计合同审核完成，以下实现与测试路径为实施定位和验收命令，不表示代码已存在或运行验收已通过。实现PR必须替换成实际验证收据，不能用本矩阵宣称功能完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | recommendations.py段投影 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-002 | recommendations.py、quotes.py | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-003 | recommendations.py历史结束时钟 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-004 | repository.py、service.py严格关联 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-005 | DailyList、StockDrawer、共享组件 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-006 | QuoteProvider上下文与取消 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |
| F-007 | 只读API、合同测试及生产门禁 | artifact:docs/architecture/advisory_realtime_returns_f1_design_20261029.md | verified（设计合同审核） | 无 |

## 审核记录

第一轮：将名单生命周期与跟踪池分开，明确连续推荐、重新加入、结束冻结；禁止按股票代码借用其他binding或不同时间段的episode。

第二轮：将指标明确为未复权价格变化，避免当天复权因子缺失时前填制造收益；明确盘前、停牌、历史缺口和TDX失败的空值原因。补充只读事务不得跨网络、固定名单身份刷新和源码/运行验收分离。
