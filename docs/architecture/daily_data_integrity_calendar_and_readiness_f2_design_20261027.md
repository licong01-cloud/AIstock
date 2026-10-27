# 每日数据完整性台账、交易日日历与共享就绪 F2 详细设计

日期：2026-10-27。设计先经PR #6773合入；实现任务为Feature F2 / Issue #6774。

第14节保留原设计阶段的docs-only审核记录。当前实现与源码/DEV验收见第12、15节；生产授权、迁移、用户重启和运行态验收仍独立报告，不能由源码通过推断已上线。本轮未执行生产写入、采集/补录/导出/预测、数据激活、依赖安装或用户服务控制。

## 1. Background / 背景与现有实现

本设计承接[每日同步自治控制平面](data_sync_autonomous_control_plane_design_20260519.md)及[共享月更运行手册](../operations/monthly_unified_dataset_release_v2_runbook.md)。继续使用既有调度器、采集器、审计和重试设施，补齐事实修订追踪、统一解释、交易日历史台账及消费者绑定；不新增第二套同步平台。

2026-10-27核对的源码入口如下。定位以符号为准，不把历史文档中的行号、股票总数或发布时间表当作运行配置。

| 已有入口 | 已有能力与本设计的使用边界 |
|---|---|
| `backend/services/data_refresh_audit.py::DataRefreshAuditRepository` | dataset/date/source审计、`record_success`、`record_failure`、`require_success`；成功记录不是全部字段完整证明，新增修订匹配合同后统一解释 |
| `backend/services/data_sync_targets.py` | target、attempt、租约、发布时间规则、迟到尾窗；复用，不新建retry queue |
| `backend/ingestion/tdx_scheduler.py::TDXScheduler` | 采集调度、重试及`register_completion_callback`；通知仅是唤醒提示，不是预测成功标志 |
| `audit_backed_data_health.py::AuditBackedDataHealthChecker` | 审计优先健康检查；与API/日历共享同一判断，不维护独立成功算法 |
| `backend/routers/local_data.py`、`local_data_management.py` | 正式本地数据管理API/service；新增只读日历和统一就绪投影 |
| `frontend/src/app/local-data/page.tsx` | 现有今日任务卡按任务执行状态展示；不能直接复用为事实日期日历 |
| `advisory_historical_range/inference_inputs.py::InferenceInputSession` | 输入会话、来源身份和有界分区；复用数据绑定思想，不假设它已经实现跨连接一致性快照 |
| `dataset_release/monthly_postgres_source.py`及既有SOURCE snapshot | 单一月更operation、增量和历史修订；读取同一台账并匹配实际SOURCE输入，不复制另一套规则 |

现有完成回调主要按最新`trade_date`选择审计行；仅修订旧日期可能不改变通知水位。现有前端状态读取有静默捕获失败路径。二者必须随本设计修正，但本设计不宣称所有历史故障均由它们造成。

### 1.1 与旧设计的衔接

旧设计的“审计是权威、任务/缓存不是权威、一次实质验收”原则不变。本设计是其交易日修订与消费合同扩展。重叠处以本设计为后续实施依据，特别修正：

- 到截止时间仍为空不等于已证明provider真实无记录。
- 重试告警截止时间不等于永久停止补采；已知可恢复迟到目标继续自动追赶。
- 后台重试失败但已验收事实未变化，不覆盖其有效结论。
- 最大成功日期不证明其之前全部日期、股票、字段完整。
- 新台账是既有审计的版本化汇总，不与审计并列成为可人工选择的第二权威。

## 2. Scope / 范围、目标与非目标

目标是由数据准备模块持有一份可修订的交易日完整性结论，日历、健康检查、月更、荐股、HMM、QE、因子及LocalSIM共同读取。程序化可恢复故障自动闭环，无法确认或无法安全自动修复时明确显示原因。

事实覆盖与买入资格分离：按正式数据源/证券合同保存事实，不能因ST、退市整理、退出指数或PIT出池隐藏已有真实行情；B股等范围外证券按现有正式合同排除，不擅自扩展市场。PIT独立版本化且只有一个全局默认绑定。改变上市满一年/半年等资格参数，不应要求重新导出早已存在的完整行情。

### 2.1 Non-goals / 明确边界

- 不修改模型、因子公式、策略、股票池语义、覆盖阈值、执行安全检查或既有实验/账户/断点。
- 不建设清算系统，不依赖LLM、逐公告语义搜索或每天人工填写来源/授权/审批资产。
- 不承诺未知软件缺陷、权限/硬盘故障和无修订通知的上游历史改写都能自动发现并修复。
- 不重复全历史检查，不每日复制整库、重导全量数据或计算全库hash。
- 不以台账绿色替代真实文件发布、模型输入预检或执行时必要的报价校验。
- 不新增调度器、消息中间件、独立重试队列或新的手工“改绿”入口。

## 3. 日期、范围与判定合同

### 3.1 四个必须分开的时钟

`fact_date`是事实归属日；`due_at`是该事实应发布/入库的时限；`observed_at/validated_at`是真实观察/验收时间；`decision_at`是消费者决策时点。交易日用DATE，时点用带时区时间戳；策略按`Asia/Shanghai`及正式交易日历解释，前端不得通过UTC日期转换造成错日。

例如10月26日融资融券在27日到达，修复的是26日台账；10月份补7月份分钟线，更新7月份格子。不得将补录日当事实日，或倒填最初可知/抓取时间。跨午夜、周末、节假日重试仍绑定原事实日。

`due_at`是应到/告警期限，不是禁止提前使用真实数据的时钟门槛。若数据已真实发布、范围与字段完整、时间合同满足，则可提前验收；不强制等待22:00/23:00。用于历史决策的发布时间无法证明时保留UNKNOWN，不能用当前抓取时间倒证历史PIT。

收盘预测与次日盘前执行是不同使用合同：D日数据可用于既定时点预测，次日涨跌停/停牌输入按次日合同就绪；不把次日尚未发布的执行字段变成D日所有预测的前置条件。

### 3.2 义务清单与合法空值

以当前DatasetSpec、正式启用的同步目录、共享发布时间规则生成义务清单。生产业务数据、按需研究包、统计缓存、测试任务分开；不要求所有数据库表每天有新增行。参考/稀疏事件类检查“截至何时、何范围成功读取”，不能用最后事件日期判断过期。

义务含`dataset/fact_date/scope_version/rule_version/required_fields/legal_na_rules/dependencies/due_at`。`scope_version`描述证券/市场、历史窗口、频率、单位和来源合同，不含PIT买入资格。配置由数据owner维护，消费者只声明已有业务依赖，不能提交忽略缺失、删股或降低规则的参数。

预期证券分母必须独立于本次已返回记录，使用事实日有效的证券/上市终止边界及正式provider范围；不能按当前上市清单倒推历史，也不能“源只返回这些股票，所以这些就是全集”。真实停牌、不适用按合同单独计数。

每日义务清单保留生成时的版本；停用一次调度不等于可以删除历史未完成义务。正式目录调整以新合同和生效范围表达，不得通过移除失败数据集将旧日染绿。

| 类别 | 验收范围与合法情况 |
|---|---|
| 日线/分钟原始行情 | 实际应有事实的证券日期；字段有限性、唯一键、时标、OHLC关系和单位。分钟使用既定会话及真实停牌区间，不仅看240/241行；零成交量不是自动错误，不制造bar |
| `adj_factor` | 正式范围内正且有限的因子、尾部覆盖及已知历史修订；历史重述影响范围进入失效链，不能仅追加新日掩盖拼接 |
| `daily_basic` | 沿用现有必需字段合同，分别记录各字段有效/合法NA/缺失；市值正数、其他字段按其定义。股息率等可空字段不得默认补0，也不能只用总条数或95%解释为无缺口 |
| moneyflow | 所需字段、单位、历史source-specific代码和适用区间；正式absence与未知缺口分开；不根据因子或当前代码猜历史事实 |
| 指数/行业/PIT派生 | 历史分类版本、membership、报价availability、个股聚合资金流独立解释；停发不等于membership消失，ID覆盖不等于数值完整 |
| 停牌/ST/涨跌停/全局PIT | 使用共享权威解析与正式规则；合法稀疏记录、全天/盘中/同日S/R/跨日区间分别处理，未知不能按全天停牌豁免；不新增逐事件公告归档门禁 |
| 融资融券/ETF等迟到源 | 真实事实日与共享发布延迟；部分到达不是完整发布，超过短尾窗仍保留未闭合目标 |
| 公告/财务/参考目录 | 按正式同步目录检查截至时点的源读取、分页及适用范围；合法无新增可完成；一次性研究包不自动变为所有日预测必需输入 |

合法NA必须来自已有明确合同或可核验事实，不以重试次数决定。`UNKNOWN`、未检查、查询失败与缺失数量0不同。不因本设计私自调整QE/HMM既有覆盖阈值或填值合同。

### 3.3 两个结果，一份权威

`all_sync_complete(D)`表示D日义务清单全部验收闭合，未解释必需缺口为0；T+1事实尚未到期时仍是正常等待，不把D-1行伪充D日已完整。

`consumer_ready(request)`从同一结果按策略包/数据字段/窗口/时点/全局PIT投影。荐股的`prediction_ready`是它的别名，而非另一套规则。某包不用迟到融资融券时可以预测，日历仍如实显示该日未全部完整；使用它的包必须等到所需事实满足。数据就绪不是预测完成，更不是允许成交。

交易日绿色还要求义务目录解析完整且实际义务数大于0；空清单、漏注册的数据集或空消费者依赖不能利用`all([])=true`变成ready。明确无需某输入的情况由既有正式合同表达，不以解析失败替代。

## 4. Architecture / 唯一状态链与自动修复

```text
既有调度/受管回补
  -> 数据事务提交 + 修订标记 + 持久待验目标
  -> 既有worker执行目标范围验收
  -> 同版本审计 + 交易日汇总
  -> 现有API/日历/通知/消费者/月更
```

### 4.1 写入与修订

所有正式生产写入口（Python、Go TDX、历史导入、批准的修复SQL）须在同一数据事务中完成：实际改变事实、增加受影响分区`data_revision`、登记/重启持久`VALIDATE_ONLY`目标。范围至少为dataset/date，可携带股票/字段和派生影响区间；不按行执行昂贵全表触发器。

增加修订只针对真实变化；网络失败、无变化upsert、缓存刷新不增加数据修订。已有合法数据不得被NULL/NaN覆盖。有限值更正仅按已授权的权威源修订/确定性转换规则处理，不能取最大最小值或自动选择冲突来源。

现有`DataSyncTargetRepository.upsert_target`自行创建连接；实施须补充可参与调用方事务的内部写入接口，不能假设已有`conn`参数。Go与批准SQL使用同一事务协议，迁移必须在现有DEV验证。绕过协议的无记录旁路DML不在准确性承诺内，正式修复脚本不得遗漏影响标记。

### 4.2 单次验收与CAS发布

1. 短一致性只读快照取得分区修订R、范围版本和事实，执行目标范围检查。
2. 单独写事务比较当前R、规则和范围仍相等，才发布`validated_revision=R`及结果；若变化则合并重验，旧worker结果不能覆盖新修订。
3. 成功审计、结果摘要和对应target收敛需原子发布；交易日汇总引用确定的验证结果。
4. 事实已变化但汇总尚未重算时，API/消费者读取必须比较修订并返回待验，不能短暂显示旧绿灯。
5. 发布相同验证结果不产生新修订/通知。并发target使用既有租约，并增加attempt/fencing与expected revision约束。

这不是长事务包住采集或模型推理。现有读入检查若已覆盖必需条款就复用，补不足的字段；日终检查优先读小型审计，不重复扫描行情。

租约领取须返回持久`lease_token/attempt_id`，续租、状态结束与验收发布都比较同一token及所验revision；过期worker即使数据revision未变也不能发布。长作业根据进度/heartbeat识别是否失联，不能仅超过固定两小时就认定数据缺失并重拉。

### 4.3 自愈动作与告警分离

| 失败类别 | 自动动作 | 无法自动闭合时 |
|---|---|---|
| 临时网络/限流/连接/事务冲突 | 有限快速重试后退避，数据源共享限流；保留断点 | 持续按计划追赶并去重告警，不伪写成功 |
| 作业中断/部分提交/遗漏分页 | 恢复原target、页/股票断点；已验内容复用 | 明确失败范围，不重做全部 |
| 已知缺行/无效字段/代码或单位适配 | 精确真实数据补取、fill-only或正式修订；重新读回 | 保留精确缺口和原因 |
| 派生数据脏/依赖修订 | 先闭合依赖，再重算受影响日期/股票及必要rolling预热闭包 | 不把未知聚合结果发布为有效 |
| 上游迟到/部分发布 | `WAIT_UPSTREAM`并持续低频补采；deadline只升级告警 | 空响应/重试耗尽仍不等于已证明absence |
| 新协议/来源冲突/磁盘或权限故障 | 隔离故障范围，保留原始错误和进度 | 需要工程处理；不自动改代码、删任意数据或重启服务 |

自动修复生产表/字段/动作范围在上线授权中明确，不每天逐股新增人工确认；超出范围的新写入仍按现行DEV及具体生产授权执行。

告警、任务状态与事实可用性分离。原已验事实不变时，补采失败不会撤销其结论；实际发现缺口、修订或派生过期时立即失效。`final_blocked`不能使已知迟到目标永久失去自动追赶资格：超期可继续`retry`并带已告警标志；不可安全修复的错误保持明确阻断。告警解除由真实复验驱动，点击确认不改变数据状态。

## 5. Contracts / 数据库与持久化设计

物理事实仍在原市场表，`dataset_date_refresh_audit`仍是分区验收入口；不并列新增一张可手工签发ready的分区权威表。

### 5.1 扩展现有audit

拟增加列：`data_revision bigint`、`validated_revision bigint nullable`、`validation_status`、`validation_id`、`scope_version`、`rule_version`。revision与时间戳不同，不能依赖时钟先后防止并发覆盖。已有provider/source行继续表达采集观察；对共享物理分区由数据owner写规范`data_source=canonical_db`验收行，并在metadata保留真实来源引用，不能把它说成新增行情供应商。

共享就绪只读取规范行的匹配修订结果；不能从多个source记录中挑一个绿色。旧`status/quality_status`保留兼容，但`success`单独不再足以判ready。所有既有health/API/`require_success`适配器使用同一解释器，不能留下一条旧成功捷径。

当前audit upsert会保护旧success免受后续failed覆盖。实施须区分：provider抓取失败而事实未变时保留有效结果；规范分区实际复验失败或修订变脏时，必须用带修订比较的专用事务写入使旧结论失效，不能被此保护条件吞掉。`require_success(data_source=...)`保留原来源限制，但来源success也必须满足共享物理分区验收；不能静默丢弃消费者的来源要求。

metadata包含计数定义、合法NA分类、`gap_groups`、来源/attempt引用、影响范围和检查覆盖。缺失单位分别是记录、证券日、字段单元和分钟键，不混算。大范围缺口按证券/连续日期或分钟区间/字段集合压缩，支持精确展开分页；不将百万条逐bar错误复制到每次汇总。

每条检查在同一种key/field单位下将预期集合分成`VALID / LEGAL_NA / MISSING / INVALID / UNVERIFIED`五个互斥类别；分母、有效数及缺口数必须可对账，不能将字段单元与证券日相加。事件类合法空集合仍须有成功源读取及非空义务定义，不能靠0=0通过未执行检查。

计数未观察时为null；`data_revision/validated_revision`为非负整数且后者允许null；只有实际执行且规则/范围匹配才能令二者相等。`validation_id`标识确定的一次结果；`daily_integrity_id`标识确定的一版汇总。时间为ISO8601带时区，事实日为`YYYY-MM-DD`，不使用字符串化NaN或默认0表示未知。

### 5.2 交易日版本化汇总

拟新增轻量表`market.data_daily_integrity_revisions`：主键`(fact_date, contract_version, revision)`；每版记录`daily_integrity_id`、`as_of`、当前状态、分项validation引用与必要计数快照、`all_sync_complete`、完成方式、首次/最近完成时点及变化原因。最新版本视图供日历/消费者使用，历史版保留真实当时结论，不保存完整行情快照。

汇总是可重建投影；如与分区修订不一致，以分区验收/修订状态为准并重建汇总，不允许人工选择另一版本解除数据错误。月度统计取每个交易日一版最新有效结果，不重复计数修订版本。

首次补验历史只写真实`validated_at`，不能伪称过去当天已验。规则升级保留旧版本含义，仅重新验证受影响范围；未覆盖新条款的旧绿色不能自动解释为新合同已满足。

### 5.3 持久待验目标

复用`data_sync_targets`，在`target_scope.operation`区分`FETCH`、`VALIDATE_ONLY`、既有派生刷新。VALIDATE_ONLY身份绑定dataset/date/scope而非每次revision，metadata合并最新desired revision。旧attempt结束时若desired revision已变，不能标为reconciled。生产写入和此目标登记同事务，避免提交后崩溃漏通知。

既有回调改用受管提交/待验修订变化唤醒，不只看MAX(trade_date)。历史补录、范围修订、依赖失效都进入同一链。不新建broker或另一套定时器。

影响范围按数据依赖闭包计算：补D日严格前置市值可能影响后续消费者；成员/历史代码区间修订及factor重述按真实生效区间传播；rolling派生还包括必要后续窗口，不能只刷新补录日自身。复用既有表达式/字段依赖，不自行重训业务模型；无法确定闭包时只明确阻断受影响派生/消费者，不能伪缩范围，也不拖住无关数据集。

## 6. 状态机与日历颜色

分项状态：`UNVERIFIED / NOT_DUE / WAIT_UPSTREAM / VERIFYING / VALID / INCOMPLETE / ERROR / NOT_APPLICABLE`。真实缺口进入INCOMPLETE，无法完成检查为UNVERIFIED或ERROR，不能用缺口0冒充检查成功。

交易日状态按同一函数聚合：

| 状态 | 颜色/文字 | 条件 |
|---|---|---|
| `NOT_TRADING` | 浅灰/休市 | 正式日历非交易日，不计完整率；自然日事件仍按其合同处理 |
| `UNVERIFIED` | 灰/待核验 | 缺有效义务清单或验收结果，数量可未知 |
| `IN_PROGRESS` | 蓝/同步或等待 | 尚无已确认缺口，正在采集/验收或正常等待发布 |
| `PARTIAL` | 黄/部分完成 | 有可定位的未闭合缺口/超期未到；自动补采中仍显示真实缺口 |
| `ERROR` | 红/检查异常 | 检查失败、未知冲突或需要处理的系统故障，不能确认完整性 |
| `COMPLETE` | 绿/完整 | 所有义务满足、匹配当前修订，无未解释必需缺口 |

`COMPLETE`再按`completion_kind`显示：`ON_TIME`绿色；`RECOVERED`青绿色并有“补齐”标记；`HISTORICAL_VERIFIED`绿色加“历史补验”；`SOURCE_REVISED`绿色加“已修订”。RECOVERED表示真实确认缺口后复验恢复，不因一次网络重试就宣称修复过数据。再次出现问题必须退出绿色。

聚合时：非交易日先单独返回；否则ERROR优先于PARTIAL，PARTIAL优先于普通处理中，未知清单/未验范围不能被多数成功覆盖。任务running/failed及缓存stale仅附属展示，不得覆盖仍有效的事实结论。

未来交易日从正式日历只读生成灰色“未到期”展示，不写验收成功；当日尚在正常发布窗口内时显示蓝色等待。首次历史补验若没有旧时点记录，`observed_on_time=null`，不能从当前完整反推其当时准时完整。

## 7. API / UI / MCP合同

以下为拟新增API，均复用`/api/v1/local-data`路由及同一service；GET不采集、不写库、不自动扫描行情。CLI/MCP如提供入口只做此服务薄封装。

| 只读API | 返回 |
|---|---|
| `GET /integrity/calendar?month=YYYY-MM` | 正式交易日历、本月各日状态/revision、计数、补齐标记、更新时间及全月汇总 |
| `GET /integrity/days/YYYY-MM-DD` | 当日义务、各数据集验收、合法NA、缺口、自动修复与发布包含情况 |
| `GET /integrity/days/YYYY-MM-DD/gaps?dataset=...&cursor=...&limit=...` | 存储缺口组的有界分页及计数单位，不在请求中重查整表 |
| `GET /readiness?data_date=...&consumer=...&binding_id=...` | 按正式消费者/策略包身份解析的共享就绪；不得传ignore/override/默认行业等绕过参数 |

响应包括`schema_version`、`as_of`、`fact_date`、`contract_version`、`daily_integrity_id`、`revision`、`state`、`completion_kind`、`all_sync_complete`、分项counts及源状态。消费者结果另含请求窗口、所需分区修订、全局PIT绑定和`consumer_ready`，荐股兼容名为`prediction_ready`。这些字段是版本信息，不是人工审批token。

汇总数量必须来自完整义务集合，不能对limit=30/50的展示页计数冒充总数。缺口分页游标绑定validation/revision，期间状态变化返回明确版本已变，调用方从该版本首页重取，不能拼接不同版本。ETag/增量刷新须包含有效修订及待验水位；不能因汇总尚未重建返回304隐藏已提交的变更。

格式/日期非法返回400；无验收记录的合法日期返回200+UNVERIFIED而非空成功；非交易日返回NOT_TRADING。数据库/服务读取失败返回503及可诊断错误，不制造绿色。数据本身PARTIAL/ERROR是业务状态，正常返回200及原因，不能把HTTP200当完整。

### 7.1 页面

在本地数据管理增加“交易日完整性”月历及日期详情组件，遵循项目组件与主题token；不继续把全部逻辑堆入现有巨型page或扩散legacy样式，不为日历引入新依赖。

- 月份切换、今天定位、状态图例、交易日/完整/补齐/部分/异常/待验统计；颜色配文字/图标并支持键盘和屏幕阅读。
- 点击日期显示dataset、事实日、应到时间、已验修订、有效/缺失/合法NA/未知数量、字段与证券缺口、next_retry_at和修复结果。
- 分别显示`source_complete`和当前release的`release_contains_revision`；当前release截止日之外显示“未覆盖”，不能解释为源数据缺失。
- 不提供“强制成功/手工改绿”按钮；若保留重试操作，复用现有受控任务接口。
- 可见页面按现有刷新模式查询小型台账，后台页面暂停；请求失败展示最后更新时间及“无法获取最新状态”，不能静默保留绿灯当当前结论。
- 统计缓存过期独立提示，不影响台账颜色；GET与页面加载不触发校验任务。

状态接口故障是页面级“读取失败”，不把所有交易日的持久状态批量改成ERROR。旧release缺少源revision pin时，`release_contains_revision=null`并显示“未建立对应关系”，不能直接宣称已包含或缺数据；复用已有有效组件读回建立关系，不为填此栏全量重验。

## 8. 消费者、PIT与实际读取

### 8.1 荐股

共享服务从已准入策略包、实际特征及窗口定义解析依赖；存量包补足确定性依赖映射，不让用户每日填写。无法解析的包明确UNKNOWN，只影响该包，不拖住其他已知包。

新预测按既定decision_at绑定输入交易日、所需历史窗口、数据修订和全局PIT，不能使用未来数据、当日市值代替严格前置值或沿用未知数据。PIT只确定实际选股范围，不裁原始行情；持仓报价目录独立覆盖真实事实。

使用既有输入会话取得本次一致输入。多连接/进程须共享同一短一致性快照或已捕获的输入分区，不能假设每个连接独立读取最新表也一致。获取阶段版本变化则刷新绑定；模型运算不持有长数据库事务。

通知重复投递按消费者/包/决策时点/输入修订幂等处理；数据ready、任务已受理、预测成功分别记录。修订后的是否再次预测遵循既有任务策略，不能自动覆盖旧荐股或重复下单。

自动通知路径先按`consumer/策略包/正式决策槽位`查已有任务，再冻结首次输入修订。仅新增输入revision不得绕过此槽位创建第二个正式预测。未绑定输入前可以重取最新修订；已冻结任务的重试/更正按既有消费者合同处理。只有业务合同明确允许修正版时才走其正式更正流程，手动历史业务入口的既有语义不由数据服务重定义。

### 8.2 模拟盘/HMM/QE/因子

共享源完整性结论相同，但合法NA、前置窗口和必需字段按其已批准合同投影。LocalSIM盘前执行、持仓报价、HMM预热与QE冻结训练不可被统一成“所有表当天非空”。台账不替代消费者自身真实缺价报错、模型业务验证、账户接续或历史输入冻结。

旧任务保留原绑定。全局默认PIT切换影响新任务，不能借台账改绑历史任务。所有模块只读共享状态，不能维护自己的默认PIT或写完整性结论。

## 9. 月度更新与源/文件分离

1. 月更计划读取目标月逐日台账及已知历史修订；日历颜色是入口，实际使用确定的分项revision引用。
2. SOURCE在既有一致快照内核对修订与实际输入，纳入截至封存时已知的修复。不能先看月历全绿，稍后读另一版本数据库冒充相同输入。
3. 只验新增月和明确历史改动、必要依赖闭包，其他历史结果复用。原operation的有效checkpoint和已完成组件保留，不因日历状态变化无条件全量导出。
4. 封存后新事实不静默改变旧SOURCE或文件；走既有受管修订/组件更正，已发布内容及旧任务不覆盖。
5. `source_month_complete`、文件物化完成、节点一致、生产激活分别报告；源绿色不等于Bin/H5/Parquet已修复。发布元数据记录实际包含的源revision，原子激活仍按现有权限。
6. 已授权暂缓项如margin_detail可按现有规则继续不依赖它的工作，保留PARTIAL/暂缓事实，不把该月宣称全部完整；不能新增审批或放宽消费者必需输入。

## 10. 性能、故障隔离与历史初始化

- 新增检查以当前增量、已知失败键和依赖闭包为界；分钟按交易日时间边界和正确股票参数类型裁剪chunk，聚合读回一次，不逐股票重复MAX全历史。
- 大范围缺口压缩存储；日历为轻量索引查询，详情/缺口分页；没有校验变化不反复增加状态版本。
- 快速重试有上限，迟到目标退避持续运行并有数据源级配额；不因同一上游失败同时启动全市场重拉。持久租约、heartbeat及revision fencing防止重启重复提交。
- 日结汇总或月更worker异常不得让整个后端启动失败；单独暴露不健康，不发布假ready，其余不依赖它的已验功能继续可用。
- 首次上线优先本月及当前月更范围；有匹配规则/范围的既有有效结果可继承，其他历史标待验，不为日历全绿启动全部历史扫描。
- 对无变更通知的上游回溯修订，按已有独立核对能力发现后登记影响范围；不能承诺仅看新增日即可发现一切历史改写，不将全历史日检变为预测门禁。
- 性能需在DEV报告目标日期读取量、查询计划/耗时、重试次数和聚合耗时；日历目标为索引摘要秒级读取，不能虚构生产时效达标。运行进度可见且带原生错误。

## 11. Implementation Plan / 实施拆分与生产边界

| 阶段 | 实施内容/主要位置 | 阶段验证与禁止事项 |
|---|---|---|
| P1 合同与已知缺陷 | 统一DatasetSpec/发布规则/字段/NA；`tushare_sync_engine`、`tdx_scheduler`、health解释器 | T01-T06；未知不填0，字段按合同，不抄旧文档固定股票数/时刻 |
| P2 持久修订与自愈 | audit迁移、target事务入口、Go/SQL影响标记、共享`daily_integrity` service/repository | T07-T13；现有DEV，失败事务不留下版本/target，旧worker不能写ready |
| P3 日历API/UI | local_data service/router、独立calendar/detail组件及既有请求层 | T14-T17；GET不写入、不扫描大表、前端不另算成功 |
| P4 消费与月更 | 既有完成回调、输入绑定适配、monthly SOURCE/修订引用 | T18-T22；不训练、不自动改实验/账户/激活、不增新调度器 |
| P5 合并交付 | 故障注入、真实目标日只读验收、范围/生产状态报告 | T23-T24；功能逐项真实验收后集中安排用户重启，不以文档合入冒充上线 |

### 11.1 Production gates / 权限与rollout/rollback

生产目标和操作见[日度完整性上线手册](../operations/daily_data_integrity_calendar_runbook.md)。迁移已在现有DEV验证，不新建数据库、不要求生产备份/导出；生产DDL/DML仍须具体授权，源码合入不替代生产授权。源码交付的验证范围与上线后真实目标日读回分开，不把用户重启作为再次开发或重新导出数据的理由。

迁移采用additive列/表，旧记录初始化为UNVERIFIED，不默认ready。上线前一次性完成writer与reader兼容：某writer未覆盖时其分区不得声明新版修订已受管，不能出现新页面和旧消费者各选一套成功判定。生产启用、初始补验、消费者读回分开报告，不设置固定等待若干天/手工收据签发后才能使用的额外门禁。

出现问题时停止新台账发布/新增预测触发，保留事实、旧输入和已完成任务；回滚不能退回“仅job success即ready”的不安全解释。依赖新迁移的代码不可先卸载列/表，版本回滚/恢复按正式流程执行。所有后端重启仍由用户执行。

## 12. Design Acceptance Index 与 Verification Plan

以下保留原稳定条款，矩阵已替换为实际实现及可执行验证入口。`source_verified`仅指源码/DEV交付通过，不代表生产运行验收通过；第15节和上线手册分别列出生产授权、应用、读回、运行态状态。

### 12.1 Design Acceptance Matrix / 开发验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | data_integrity/catalog.py::Obligation.due_at、writes.py::fact_day | backend/tests/test_daily_data_integrity_api.py::test_tplus1_publication_updates_original_fact_date；backend/tests/test_daily_data_integrity_inputs.py::test_writer_normalizes_actual_fact_day | source_verified | 无 |
| F-002 | data_integrity/catalog.py::obligations、validation.py::_scan_contract | backend/tests/test_daily_data_integrity_api.py::test_catalogue_nonempty_ignores_maintenance_and_keeps_disabled_history；backend/tests/test_daily_data_integrity.py | source_verified | 无 |
| F-003 | data_integrity/validation.py::_raw_scheduler/_traded；共享security_source_identity；未变更买入资格 | backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py::test_raw_completion_excludes_old_b_quotes_but_preserves_a_share_gaps；backend/tests/data_service/test_security_source_identity.py | source_verified | 无 |
| F-004 | data_integrity/validation.py::daily_basic_result、contracts.py::finite_number；共享停牌minute端点合同 | backend/tests/test_daily_data_integrity.py::test_daily_basic_admission_ratio_is_not_zero_gap_certification；backend/tests/test_daily_data_integrity.py::test_shared_suspension_minutes_keep_real_unknown_and_regular_boundaries | source_verified | 无 |
| F-005 | data_integrity/validation.py；adj_factor_history_reconciler.py；sector_data_builder.py::sector_fact_comparison_sql | backend/tests/services/test_adj_factor_history_reconciler.py；backend/tests/services/test_sector_data_builder.py；backend/tests/data_service/test_security_source_identity.py | source_verified | 无 |
| F-006 | tdx_scheduler.py::_reconcile_recovered_final_targets；data_integrity/provider.py/worker.py | backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py::test_final_target_recovery_sweep_repairs_historical_recovered_family；backend/tests/test_daily_data_integrity_dev.py::test_fetch_retry_cannot_be_postponed_by_repeated_validation | source_verified | 无 |
| F-007 | migration marker、data_integrity/writes.py；Go raw_rows.go::markRawIntegrity | backend/tests/test_daily_data_integrity_dev.py::test_atomic_rollback_and_historical_date_rearm；go test -count=1 -run TestDEVRawWriteReadbackAndIdempotence .（web目录，现有DEV） | source_verified | 无 |
| F-008 | data_integrity/repository.py::claim/heartbeat/publish | backend/tests/test_daily_data_integrity_dev.py::test_expired_worker_cannot_publish_even_same_revision；backend/tests/test_daily_data_integrity_dev.py::test_rule_upgrade_fences_old_claim_even_without_new_facts | source_verified | 无 |
| F-009 | data_refresh_audit.py；contracts.py::effective_partition；writer的实际变化RETURNING | backend/tests/test_dataset_refresh_audit.py::test_refresh_failure_cannot_downgrade_usable_success_evidence；backend/tests/test_daily_data_integrity_api.py::test_unknown_day_and_committed_dirty_revision_not_old_green | source_verified | 无 |
| F-010 | TushareSyncEngine、Go、受管导入和repair SQL共用同事务协议，清单见上线手册 | backend/tests/test_tushare_sync_engine.py；backend/tests/scripts/test_ingest_tushare_daily_basic.py；backend/tests/test_daily_data_integrity_dev.py；go test -count=1 -run TestDEVRawWriteReadbackAndIdempotence . | source_verified | 无 |
| F-011 | tdx_scheduler.py::_dispatch_completion_callbacks；worker.py持久target恢复 | backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py::test_historical_revision_wakes_callback_without_advancing_latest_day；backend/tests/test_daily_data_integrity_dev.py::test_atomic_rollback_and_historical_date_rearm | source_verified | 无 |
| F-012 | data_refresh_audit.py、audit_backed_data_health.py、data_integrity/contracts.py | backend/tests/test_daily_data_integrity_inputs.py::test_monthly_provider_green_cannot_override_dirty_canonical_revision；backend/tests/test_dataset_refresh_audit.py | source_verified | 无 |
| F-013 | worker.py::rebuild_day；repository.py::append_day；contracts.py::completion_kind | backend/tests/test_daily_data_integrity.py::test_completion_distinguishes_historical_verification_revision_and_actual_recovery；backend/tests/test_daily_data_integrity_api.py::test_unknown_day_and_committed_dirty_revision_not_old_green | source_verified | 无 |
| F-014 | local_data.py只读四接口、service.py | backend/tests/test_daily_data_integrity_api.py::test_readonly_routes_distinguish_bad_request_failure_and_revision_change；frontend/tests/local-data/daily-integrity-calendar.spec.ts | source_verified | 无 |
| F-015 | DailyIntegrityCalendar.tsx、独立theme CSS | frontend/tests/local-data/daily-integrity-calendar.spec.ts；validation-receipt: valjob_20261027_120040_28f995e9（2通过） | source_verified | 无 |
| F-016 | UI AbortController、visibilitychange、view_watermark与读取错误提示 | frontend/tests/local-data/daily-integrity-calendar.spec.ts；backend/tests/test_daily_data_integrity_api.py::test_release_read_failure_never_reports_proven_stale_files | source_verified | 无 |
| F-017 | data_integrity/release.py；monthly_candidate_finalizer.py | backend/tests/test_daily_data_integrity_inputs.py::test_release_inclusion_is_not_inferred_from_cutoff_or_source_green；backend/tests/dataset_release/test_monthly_candidate_finalizer.py::test_finalizer_pins_captured_source_revisions_without_reading_live_db | source_verified | 无 |
| F-018 | data_integrity/consumers.py；service.py::readiness | backend/tests/test_daily_data_integrity.py::test_consumer_not_blocked_by_unused_delayed_dataset_and_unknown_not_ready；backend/tests/test_daily_data_integrity_inputs.py::test_dependencies_use_frozen_factor_bytes_and_actual_window | source_verified | 无 |
| F-019 | pg_pool.py::capture_read_snapshot；InferenceInputSession；inference_engine.py；源修订命名空间cache | backend/tests/test_daily_data_integrity_dev.py::test_actual_nested_readers_and_revision_use_one_short_dev_snapshot；backend/tests/test_daily_data_integrity_inputs.py；validation-receipt: valjob_20261027_114509_99ba600e（564通过） | source_verified | 无 |
| F-020 | 既有completion callback单航班/重放；仅通知，不新增任务/订单/预测写入口，消费者决策槽位合同保留 | backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py::test_completion_change_dispatches_once_and_explicit_request_recovers；backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py::test_single_flight_preserves_watermark_and_requests_arriving_in_flight | source_verified | 无 |
| F-021 | 共享canonical audit解释；正式全局PIT resolver；旧文件输入不改绑；已批准未知预热保留 | backend/tests/hmm_data_source；validation-receipt: valjob_20261027_120149_0fdb0cef（324通过、1跳过、6 integration未执行）；backend/tests/advisory_historical_range | source_verified | 无 |
| F-022 | monthly_postgres_source.py同SOURCE捕获；source_authority.py；finalizer pins/inherit | backend/tests/dataset_release/test_monthly_postgres_source.py::test_frozen_bundle_pins_formal_source_stage_receipt；backend/tests/dataset_release/test_monthly_candidate_finalizer.py；backend/tests/dataset_release/test_monthly_build_resume.py | source_verified | 无 |
| F-023 | service.py仅小型索引；原分钟精确日期chunk查询；30秒批量去抖；非致命monthly启动 | backend/tests/test_daily_data_integrity_dev.py::test_minute_batches_invalidate_immediately_but_coalesce_physical_checks；backend/tests/test_daily_data_integrity_inputs.py::test_optional_monthly_worker_configuration_failure_does_not_abort_api_startup；DEV元数据读回0.033秒见§15 | source_verified | 无 |
| F-024 | additive迁移及正式上线/回滚手册；生产状态不由源码合入推断 | backend/tests/test_daily_data_integrity_dev.py（6通过）；artifact: docs/operations/daily_data_integrity_calendar_runbook.md（精确目标、hash、待授权步骤；未宣称生产已应用） | source_verified | 无 |

功能开发阶段必须把每项替换为真实实现位置、可执行测试与真实读回，禁止把拟测试名、文档检查或静态成功当成已完成。测试正常路径与反例并重；真实验收不启动未授权的荐股/模拟盘/QE实验，不为取证终止生产任务。

## 13. Risks / 风险与取舍

- 完整性结论只保证指定规则/范围/修订的已执行检查，不保证上游永远正确。未知保持未知，避免“零误报零漏报”的不实承诺。
- 受管writer遗漏是主要漏检风险，必须逐入口验收；直接SQL旁路不应被默认为已覆盖。
- 依赖推导无法只从任意模型二进制自动可靠完成；复用已准入包及实际输入定义，未知包单独显式处理。
- 规则升级不应引发全部历史重扫；明确所需日期/字段闭包，未验部分如实显示。
- 源/文件/模型绑定状态不能互相代替；新字段修复可能要求业务owner重新算因子或训练，数据owner不承诺收益改善。

## 14. 文档审核与设计交付验收

本节只记录文档审核，不提升§12的功能状态。采用本窗口连续多轮自审，不冒称独立专家、多代理或外部业务验收。每轮实际发现、修订与复核在提交前补充。

### 14.1 审核记录

第一轮（业务日期与状态）：完成初稿逐条对照需求及现有writer/reader的自审。修订§3：due_at不是固定等待门槛、历史证券分母不能来自当前清单或返回子集、空义务/空依赖不能自动ready；修订§6：未来未到期与未知历史分开，历史首次补验不能伪造准时完成；补齐§12.1矩阵章节。复核相应日期与空集合反例通过。

第二轮（事务、历史修订与消费竞态）：逐段核对audit upsert、target事务入口及完成回调。修订§5.1：既有success保护不能吞掉真实复验失败，同时保留原来源限制；修订§7：分页/ETag固定有效修订、全量计数不能来自展示截断；修订§7.1：接口失败不批量写红、旧release缺pin显示未知而非武断缺失；修订§8.1：同一正式预测槽位不因新revision重复创建。按旧attempt晚完成、历史回补不改变最新行情日、并发分页及通知重放顺序复核，通过设计级推演，不冒充执行测试。

第三轮（可开发性与完整性交付）：检查类型、计数、依赖闭包、实施/验收对应及权限。修订§3.2：暂停调度不删除历史义务；修订§4.2：持久lease token与heartbeat判定；修订§5：缺失计数null语义、五类互斥集合、历史修订向严格前置/rolling派生传播。已执行文档结构/相对链接/24项唯一索引检查及`git diff --check`，通过；F2完整功能检查保留预期FAIL，24项pending未改成已实现。

三轮完成后，按DESIGN-COMPLIANCE-001复核：本轮交付完整设计而非功能子集；错误/未知与合法NA明确；PIT、策略、窗口、阈值及冻结任务语义不改；未新增日常审批、全历史检查或固定等待日门槛。设计内容审核通过，可提交docs-only PR；CI及merge状态以GitHub实际结果为准。

### 14.2 DESIGN-COMPLIANCE-001

逐项审核完整设计覆盖、显式失败、业务语义不漂移、无私增审批/发布阻断。docs-only检查运行`git diff --check`及F2结构/索引检查；F2完整功能检查会因24项pending、尚无实现证据及未闭合功能差距返回FAIL，这表示功能尚未实现，不能通过填写虚假verified或改验证器变为PASS。文档可合入条件是内容审核通过、引用/结构正确、差异仅约定两份文档及docs CI通过，不是功能验收通过。

## 15. 2026-10-27实现审核与交付状态

Feature Issue：[#6774](https://github.com/licong01-cloud/AIstock/issues/6774)。实现分支`feature/daily-integrity-calendar-20261027`，从设计合入commit `6d3c42330a6ac81e8fe1fa1774d1f4874d1e7cb7`开始。验收矩阵的源码通过与生产完成是不同概念；原第14节描述历史设计交付，不覆盖本节实测结果。

### 15.1 连续自审与修订

没有使用多代理或冒称独立专家审核。本窗口逐轮自审，宽回归交现有Validation Center执行：

1. 事务与并发：修复autocommit默认值、同事务审计/目标、实际变化RETURNING、R相同但lease已过期及规则升级的旧worker发布；实际DEV注入rollback/并发写入并读回。成功/失败attempt与canonical verdict同事务发布。
2. 业务日期与事实语义：补T+1原日期、reference/event成功空扫、历史alias、正式停牌以及行业停发的独立合同；不从当前选股池或provider返回子集推完整分母。`daily_basic`保留既有采集准入比例，同时逐必需字段显示残余缺口，不把95%称为无缺口；可空估值仍保留NA。
3. 消费一致性与发布：修复多连接/跨调用cache混用、旧分区开始受管后的cache失效；实际DEV证明一个输入快照读R1期间独立写事务已提交R2，当前读不混入R2，下次捕获可见R2。旧冻结文件/任务不改绑；来源已补、文件未含、文件读失败与截止日之外分别显示。
4. 性能与故障隔离：分钟每批事实立即变脏，但验证目标按30秒短去抖合并；不为每只股票重复全日扫描。修复反复验收把FETCH的next_retry_at向后推、导致永远不执行的饥饿风险。日历只读取小审计/队列/交易日历和manifest元数据；可选monthly worker配置错误明确ERROR但不阻断整个API启动。
5. UI与测试接入：完成主题/键盘/未知计数/补齐标记/分页/后台暂停；接口失败标明旧读回，不把全部日子写红。修复测试运行器X盘父目录及旧假cursor/CAS夹具，补齐本模块UI计划注册；没有放宽断言、PIT、覆盖阈值或修改流水线规则。
6. 正式CI复审：首轮全路径扫描发现两个旧分钟导入脚本的硬编码凭据，已移除并复用共享数据库配置；补直接执行的project-root导入路径及main守卫。价格/单位/文件解析合同不变，新增测试证明导入模块不联网、不写库；不把凭据内容复制进日志或交付报告。

### 15.2 实测结果及限制

| 验证 | 实际结果 |
|---|---|
| 日常同步完整回归 | `valjob_20261027_120037_4819a38a`：1025通过、1跳过；末轮新增单点的正整数revision/冲突停牌回归另通过 |
| 历史消费者输入 | `valjob_20261027_114509_99ba600e`：564通过、2跳过 |
| HMM数据源兼容 | `valjob_20261027_120149_0fdb0cef`：324通过、1跳过、6 integration未运行；非正式训练/预测 |
| 月更/Qlib | `valjob_20261027_120039_ad3d3ce5`：2127通过、86跳过，1个旧CAS夹具失败；补齐夹具后该node的legacy/有pins两分支均通过。最终commit仍须正式CI，不把该整轮记录改成全绿 |
| 日历UI | `valjob_20261027_120040_28f995e9`：2通过，隔离3012、mock API；没有操作用户backend |
| 现有DEV | 最新migration重复应用通过，6项事务/租约/缓存/退避/去抖测试通过；Go `TestDEVRawWriteReadbackAndIdempotence`真实DEV通过（临时事实表、marker事务回滚） |
| 只读元数据性能 | DEV 72个日历键、3个启用schedule、0个新分区/汇总：0.033秒含连接；canonical查询使用`idx_dataset_integrity_canonical_date`。这是小规模DEV数据，不冒称生产吞吐达标 |
| 静态/注册 | changed-file Ruff、Python编译、原生已安装TypeScript编译、L0、module ownership和catalog完整性通过；RTK的tsc包装器输出不可信，已改用本地真实编译器，未安装依赖 |

受管writer是既有正式入口，不包括任意绕过协议的手工SQL。`sector_data`严格复用已激活producer的现有聚合总体与数值合同，不借本功能改行业资金流计算公式；raw事实分母仍与买入PIT独立。消费者依赖无法确定、历史首次公开时点不明、真实上游缺失均保留UNKNOWN/等待，不自动生成NA豁免。

### 15.3 DESIGN-COMPLIANCE-001与运行边界

1. 禁止简化交付：24项均指向实际源码/可执行验证；不是仅有日历mock，实际DEV迁移、事务并发和Go写入均已验证。生产授权、应用及重启后真实日验收不冒充完成。
2. 禁止静默错误：未知计数为null、读取异常503、版本分页409；事实变脏立即退出旧绿灯；空结果须有正式扫描，失败不按无记录处理；不会保证任意未知程序错误自行修好。
3. 禁止改变业务逻辑：没有更改模型、因子、策略、买入PIT、NA/预热豁免、历史任务绑定或交易账户；数据ready不授予执行权限。
4. 禁止私增门禁：没有新增调度器/队列/环境变量/人工“改绿”流程，没有逐公告/LLM步骤、全史重扫或大型数据源hash相等门禁。生产DDL的具体授权沿用现行安全边界。

生产授权=`pending`、生产DDL/DML=`not_applied`、运行态验收=`pending`；数据集构建/激活/训练/实验=`false`。源码PR/CI/merge以GitHub最终状态为准，上线顺序和回滚见运行手册。Issue在真实上线验收前保持开放。
