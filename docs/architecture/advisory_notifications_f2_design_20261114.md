# 荐股自动通知 F2 设计

## 背景

用户授权先交付自动通知代码，复用现有通知服务且不影响其他模块。通知与荐股名单发布、价格结算、收益评价分别计状态。

2026-11-20用户将目标更新为“发送荐股详情，使用飞书表格，可以点击打开”，并要求先验证消息和表格。原生飞书测试表已完成创建、写入、读回、目标群只读授权、群消息发送，用户已确认能够打开并看到测试记录；群历史读取权限也已生效。当前增量见文末§飞书详情表，取代下方仅完成提示的旧目标；旧回执和已发消息保留。

2026-11-16 用户明确调整飞书范围：**只通知荐股任务完成，不提供细节**，并确认“全部荐股任务完成后，每天一条”。本增量在当日全部启用的 `daily_after_close` Program 成功发布后发送一条简短通知；不把部分完成、UNKNOWN 或未发布当作完成，不向飞书发送逐包名单、指标、汇总明细或超时提醒。原邮件及其他模块通知保持原合同。本增量任务为 `X:/AIstock_temp/ADVISORY/feishu-completion-20261116/task.json`，新代码与配置生效、飞书 API 接受和群内实际收到分别验收。文末首次交付的生产门禁是历史记录，既有表迁移与旧运行态验收以原通知任务回执为准，本增量不新增迁移。

## 范围与非目标

实现每包发布通知、21:45 当日汇总、22:00 未发布提醒，复用已有邮件与钉钉/飞书配置；不增加前端页面、独立调度器或新凭证。只实现源码和现有 DEV 数据库验证，不发送真实消息、不执行生产迁移、不启用开关、不重启、不改名单或训练。任务与精确写范围：`X:/AIstock_temp/ADVISORY/notifications-20261114/task.json`。

## 架构与契约

- 原 `AdvisoryForwardScheduler` 在本轮业务服务返回、释放业务锁后调用通知服务。原业务结果保持独立，通知异常只记录通知状态；GET 状态只读内存，不发消息、不读队列。
- `AdvisoryNotificationTransport.send_external_message` 是新增明确通道入口，继承已有 NotificationService 的兼容接口并共享现有实例的配置，复用 SMTP/requests 依赖；仅严格发送路径处理真实 SMTP/机器人回执，页面提示不算外部发送成功。根目录旧服务文件保持与 main 一致，不修改任何旧调用者或旧全局开关。
- 仅查询当天启用且 `daily_after_close` 的 Program 和当天持久化 forward publication；日历决定目标交易日。最多 100 包，超过上限明确失败，不截断成功。
- `app.advisory_notification_delivery` 是独立持久化队列，不写监控/组合通知表。发布事件绑定 program/binding/目标日/list version；汇总、超时各按决策日唯一。每通道独立唯一键，入队 `ON CONFLICT DO NOTHING`，原始消息不随重试改写。
- 状态为 PENDING/SENDING/SENT/FAILED/EXPIRED。`FOR UPDATE SKIP LOCKED` 短事务领取，租约 120 秒，token 防止旧 worker 覆写新领取；网络调用期间不持有数据库事务。失败最多 6 次，指数退避；SMTP 已接受但记录前进程退出可能重复，因此为至少一次交付，不承诺外部严格恰好一次。
- 每轮最多发送 4 个通道事件；单通道请求有超时。只发现当前决策日，不补发过去日期；到目标交易日 09:30 失效，过期保留 EXPIRED。周末不新建事件，可处理未过期队列。
- 新开关 `AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED` 默认 false。关闭时不构造仓储/传输、不查库、不发送；原邮件/Webhook 开关不改。启用时只使用现有已配置通道，未配置返回 NOT_CONFIGURED。
- 链接使用 `AISTOCK_ADVISORY_NOTIFICATION_BASE_URL`（默认 http://localhost:3000）和真实 `/paper-v2/advisory?program_id=...`。消息只包含名单身份、时间、状态和链接，不存收件地址、密码或机器人 URL。异常只存类型/固定代码，不存异常正文。

## Design Acceptance Index

- F-001：共享严格发送入口与旧通知兼容。
- F-002：真实持久化发布发现、单包/汇总/超时语义与 UNKNOWN。
- F-003：通道去重、并发领取、租约、重试、过期、崩溃恢复。
- F-004：业务隔离、默认关闭、GET 无副作用、有限工作量。
- F-005：现有 DEV 的迁移、幂等、真实 SQL 状态机验证及生产边界。
- F-006：当日全部启用荐股任务完成才生成一条飞书完成事件，正文仅日期与完成提示；每日稳定去重，缺失、部分完成及无任务不报成功。
- F-007：荐股专用飞书配置与可选签名，复用严格发送器，绝不修改共享通知配置；真实业务码、超时与请求大小校验。
- F-008：复用原队列表与租约/重试；完成专用路由与旧明细 Webhook 队列双向隔离，切换开关后不能错投旧消息，无新增 DDL。
- F-009：配置缺失、等待完成、事件生成及实际发送状态分别可查；不新增调度器、页面或 GET 副作用，不控制服务。
- F-010：原通知兼容性、真实 DEV 路由 SQL、签名及任务完成边界验收；源码交付与机器人配置、用户重启、真实发送分别报告。
- F-011：正式名单身份冻结、所有策略包详情、与页面相同荐股期间口径、UNKNOWN和价格时点可见。
- F-012：原生飞书电子表格、按策略分工作表、目标群可打开，禁止公开共享或用本机链接替代。
- F-013：复用原调度器/队列/发送器，每日一条可点击表格链接，其他通知隔离。
- F-014：远端文档状态持久化、租约校验、重试复用已建文档；不确定创建/发送不盲目重复。
- F-015：来源/权限/平台错误明确失败；有界读取与发布性能，不影响页面加载。
- F-016：真实平台及人类打开验证、直接/既有DEV检查、多轮审核和源码/运行分态。
- F-017：手机群消息按策略显示全量在荐、已移出、退出建议及待复核股票的分页表格，不再TopN预览或省略策略包，仍可打开电脑完整表格。
- F-018：电脑表格默认进入策略总览，保留各包完整分组、19列详情、数据时点和解释；总览显示名单状态与数据完整度。
- F-019：卡片和表格从同一冻结快照产生，原排名、未知与不适用语义不变；分页只影响可见行数，完整数据保留，不能用摘要或截断替代，且不产生新行情查询或收益口径。
- F-020：复用原专用发送器、签名、大小上限和队列租约/每日唯一键；发卡前完成本地构造校验，明确失败不伪造已发送，兼容其他通知。
- F-021：专用卡片以真实紧凑UTF-8字节发送和计量20KB限制；全量无法容纳时明确报错，不静默删行、删包或拆成多条通知；实际群原始JSON和手机分页另验证。

## 2026-11-16 飞书完成通知增量合同

| 配置键 | 默认值 | 用途 |
|---|---|---|
| `AISTOCK_ADVISORY_FEISHU_ENABLED` | `false` | 仅控制荐股飞书完成通知；仍受原荐股通知总开关控制 |
| `AISTOCK_ADVISORY_FEISHU_WEBHOOK_URL` | 空 | 专用飞书机器人 HTTPS 地址，不改旧全局 `WEBHOOK_*` |
| `AISTOCK_ADVISORY_FEISHU_SIGNING_SECRET` | 空 | 可选签名密钥；机器人开启签名校验时必须配置 |

专用配置启用时，Advisory 发送器复制共享配置后仅覆盖本实例的 Webhook 配置，原共享实例和邮件配置不变。未启用时保持原配置引用，邮件与旧钉钉行为不变；即使其他模块已全局启用飞书，Advisory 也不借此发送旧详细消息，飞书完成通知仅由荐股专用开关启用。专用地址缺失或非法时明确 `NOT_CONFIGURED`，不回退其他机器人。专用地址只接受飞书或 Lark 官方 HTTPS 自定义机器人端点，日志、队列和回执不保存地址或签名密钥。

完成事件仅从当天持久化发布记录生成：当前启用任务非空、每个任务均为 `PUBLISHED`，program/binding/list/目标日与有时区的发布时间完整且一致，发布时间不晚于观察时钟。所有条件满足后不等待 21:45，按既有调度周期发送“YYYY-MM-DD 荐股任务已完成。”。每日唯一事件键不随重试或后续名单版本变化；正文不含包名、数量、持仓、收益、状态明细或链接。成功只表示荐股名单发布完成，不表示成交、盈利或群内已读。

事件沿用原 `channel=webhook`、`kind=SUMMARY` 及原消息 schema，新增固定 `delivery_route=advisory_feishu_completion_v1` 和源名单集合摘要。领取 SQL 按 Webhook 路由过滤：新模式只领取完成事件，旧模式只领取没有新路由的旧事件；邮件不受该过滤限制。未匹配的旧事件不发送，仍按原截止规则保留过期状态，不删除或改写原消息。无需迁移、补跑、重建队列或写生产测试行。发送时按秒级当前时间生成可选 HMAC-SHA256/Base64 签名；消息请求体按真实 JSON 编码检查 20KB 上限，失败不伪造 SENT。固定短正文无需明细分片。

验收使用禁止外部网络的直接测试与既有 DEV：全完成/部分完成/UNKNOWN/无任务/非交易日、错误目标及未来发布时间、每日去重、专用配置隔离、签名、业务拒绝、请求大小及路由切换。DEV 只使用原表，测试行回滚或按精确测试键清除；不创建数据库，不写生产。旧邮件和旧 Webhook 合同相关小矩阵复核；真实飞书测试和后端重启不由源码交付推导。

## 实施方案

增加严格共享入口、独立队列迁移/仓储、通知编排，并接入既有调度器。业务发布不依赖队列表存在，队列不可用时明确报通知 FAILED。源码按现有授权交付，运行验收单独报告。

## 验证方案

直接合同测试覆盖旧入口、SMTP SSL/TLS/拒收/失败、Webhook 业务失败、消息生成、时间与日历、配置、重试和调度异常隔离。现有 DEV 用显式连接执行新表 DDL、幂等迁移与真实入队/并发/token/租约/状态机，测试事务回滚且不创建数据库；所有外部发送使用阻断网络的测试替身。最后 lint/编译、相关小矩阵、F2 validate、scope/diff 检查；CI 覆盖相关模块。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/advisory_forward/notification_transport.py | backend/tests/advisory_model_first/test_advisory_notifications.py | passed | 无 |
| F-002 | backend/services/advisory_forward/notifications.py | backend/tests/advisory_model_first/test_advisory_notifications.py | passed | 无 |
| F-003 | backend/services/advisory_forward/notification_repository.py | backend/tests/advisory_model_first/test_advisory_notifications_pg.py | passed | 无 |
| F-004 | backend/services/advisory_forward/scheduler.py | backend/tests/advisory_model_first/test_forward_scheduler.py | passed | 无 |
| F-005 | backend/db/migrations/add_advisory_notifications_20261114.sql | backend/tests/advisory_model_first/test_advisory_notifications_pg.py | passed | 无 |
| F-006 | backend/services/advisory_forward/notifications.py | backend/tests/advisory_model_first/test_advisory_notifications.py | passed | 无 |
| F-007 | backend/services/advisory_forward/notification_transport.py | backend/tests/advisory_model_first/test_advisory_notifications.py | passed | 无 |
| F-008 | backend/services/advisory_forward/notification_repository.py | backend/tests/advisory_model_first/test_advisory_notifications_pg.py | passed | 无 |
| F-009 | backend/services/advisory_forward/notifications.py | backend/tests/advisory_model_first/test_advisory_notifications.py | passed | 无 |
| F-010 | backend/tests/advisory_model_first/test_advisory_notifications.py | backend/tests/advisory_model_first/test_advisory_notifications_pg.py; backend/tests/advisory_model_first/test_forward_scheduler.py | passed | 无 |
| F-011 | backend/services/advisory_forward/notification_details.py | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py; artifact: X:/AIstock_temp/ADVISORY/feishu-detail-sheet-20261120/daily_detail_test_receipt.json | source_passed | none |
| F-012 | backend/services/advisory_forward/feishu_sheets.py | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py; artifact: X:/AIstock_temp/ADVISORY/feishu-detail-sheet-20261120/provider_receipt.json | source_provider_passed | none |
| F-013 | backend/services/advisory_forward/notifications.py; backend/services/advisory_forward/notification_transport.py | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py; backend/tests/advisory_model_first/test_advisory_notifications.py | source_passed | none |
| F-014 | backend/services/advisory_forward/notification_repository.py; backend/services/advisory_forward/feishu_sheets.py | backend/tests/advisory_model_first/test_advisory_notifications_pg.py; backend/tests/advisory_model_first/test_advisory_feishu_sheets.py | source_dev_passed | none |
| F-015 | backend/services/advisory_forward/notification_details.py; backend/services/advisory_forward/feishu_sheets.py | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py | source_passed | none |
| F-016 | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py; scripts/advisory_feishu_details.py | artifact: X:/AIstock_temp/ADVISORY/feishu-detail-sheet-20261120/task.json; artifact: X:/AIstock_temp/ADVISORY/feishu-detail-sheet-20261120/daily_detail_test_receipt.json | source_dev_provider_passed | none |
| F-017 | backend/services/advisory_forward/notification_presentation.py | backend/tests/advisory_model_first/test_advisory_notification_presentation.py; artifact: X:/AIstock_temp/ADVISORY/feishu-mobile-tables-20261120/provider_test_receipt.json | source_provider_passed | none |
| F-018 | backend/services/advisory_forward/notification_details.py | backend/tests/advisory_model_first/test_advisory_notification_presentation.py; artifact: X:/AIstock_temp/ADVISORY/feishu-dual-view-20261120/provider_test_receipt.json | source_provider_passed | none |
| F-019 | backend/services/advisory_forward/notification_presentation.py | backend/tests/advisory_model_first/test_advisory_notification_presentation.py; artifact: X:/AIstock_temp/ADVISORY/feishu-mobile-tables-20261120/provider_test_receipt.json | source_provider_passed | none |
| F-020 | backend/services/advisory_forward/notification_transport.py; backend/services/advisory_forward/feishu_sheets.py | backend/tests/advisory_model_first/test_advisory_feishu_sheets.py; backend/tests/advisory_model_first/test_advisory_notification_presentation.py; backend/tests/advisory_model_first/test_advisory_notifications.py | source_passed | none |
| F-021 | backend/services/advisory_forward/notification_presentation.py; backend/services/advisory_forward/notification_transport.py | backend/tests/advisory_model_first/test_advisory_notification_presentation.py; artifact: X:/AIstock_temp/ADVISORY/feishu-mobile-tables-20261120/provider_test_receipt.json | source_provider_passed | none |

## 风险

外部交付无法与数据库原子提交，极端崩溃可重复。共享旧接口兼容行为不作为荐股成功依据。超时/配置错误不允许伪造 SENT；过期及最终失败保留。队列无级联外键，不因业务名单删除而丢失发送证据。

## 发布与回滚

先交付默认关闭的源码。后续独立授权生产迁移、用户重启、启用指定通道和真实通知验证。回滚关闭荐股通知开关，保留发送记录；不改其他通知全局开关，不删除表。

## 生产门禁

现有 aistock_dev 已通过幂等迁移、真实 SQL 去重/重试/token fencing/SKIP LOCKED/租约恢复/过期以及日历快照验证；只有新队列表持久化，测试行零残留。生产 DDL/DML 未授权/未执行，真实通知未授权/未发送，运行启用未执行，后端重启归用户。代码验收通过不等于运行通知已经启用。

本地验收：39 passed（含 4 项真实 DEV 测试）；原监控 Markdown/interactive 消息、原组合邮件调用与原 fallback 合同兼容。最终版本审核及 CI 状态写入任务记录。

2026-11-16 完成通知增量：70 项直接/调度/真实 DEV 测试通过，其中 5 项真实 DEV 验证原队列状态机与新旧路由隔离，DEV 测试行零残留、schema fingerprint 不变。Ruff、L0、文件归属和设计符合性通过，当前 Agent 多轮自审不冒充独立外审。源码与配置、运行、真实群发送分别验收；本增量无生产 DDL/DML、真实发送或服务操作。缺少机器人配置或尚未完成用户重启时不宣称飞书已启用。

## 2026-11-20 飞书详情表：Scope / Implementation Plan

保持当日全部启用任务发布后每日一条。新增荐股专用`AISTOCK_ADVISORY_FEISHU_DETAILS_ENABLED`，默认false；开启后以详情表路由代替本实例完成提示路由，旧邮件/其它模块仍保持。配置沿用现有`AISTOCK_ADVISORY_FEISHU_APP_ID/APP_SECRET/CHAT_ID`及Webhook，凭证仅从本机环境读取、不入表/消息/日志。源码交付后按本次授权配置本地开关，用户负责后端重启。

精确源码写范围：本文；`advisory_forward`下新增`feishu_sheets.py`、`notification_details.py`及既有`notifications.py`、`notification_repository.py`、`notification_transport.py`；新增直接测试`backend/tests/advisory_model_first/test_advisory_feishu_sheets.py`；既有`test_advisory_notifications_pg.py`；新增显式运维验证CLI`scripts/advisory_feishu_details.py`及其现有CI映射`scripts/ci_change_classifier.py`。单一任务记录为`X:/AIstock_temp/ADVISORY/feishu-detail-sheet-20261120/task.json`，不新建说明文档或数据库表。

1. 从已有发布快照发现全部完成后，按program/binding/target/list version精确捕获详情。复用HistoryService的保存版本投影及RecommendationReturns的连续荐股期间估值，分批捕获所有列表分组，不只取第一页或Top10。每包上限500行、最多100包，超限明确失败而非截断。来源变化/缺件失败；行情暂不可用仍保留原名单和UNKNOWN字段。原始快照连同hash在首次入队固定，后续重试不重新取数或改变股票/价格。
2. 每个策略一个工作表，中文列与页面对应：目标日、策略包、分组、排名、股票代码/名称、推荐状态、连续推荐、首次生效日、起始基准价、行情快照价、荐股期间累计涨跌、今日涨跌、行情日期/获取时点、缺失原因。累计涨跌只使用现有连续荐股期间值，不转成账户/扣费收益；新入选尚无开盘基准时保留未知。全部内容为发送时冻结快照，打开表格不会调用本机行情或改变数据。
3. 使用飞书原生电子表格API创建工作簿、查询/创建各工作表、写入并读回数据；给配置的目标群添加并核对view权限，不公开共享。表头冻结并统一中文字段，保留代码前导零，防止字符串以公式执行。写入成功不代表共享或发送成功，均分别记录。
4. 原队列表JSON payload新增不可变`detail_snapshot`和仅详情路由可写的`sheet_delivery_state`。首次创建前记录create_started，成功后持久化文档ID和链接。创建响应不确定则保留明确异常状态，禁止再次创建；平台明确拒绝可按原队列退避。工作表/数据写入和共享按保存文档及确定性名称恢复，不能重复创建整本表。每次外部步骤前核对并续约领取token，过期/旧worker禁止继续。发送前标记message_started；不确定发送不会盲目再发。原队列每通道去重、失效时间及最多6次尝试继续有效。
5. 复用原严格Webhook发送器发送简短摘要及可点击的原生表格URL，每天只发送一个包含全部策略的工作簿链接。新旧路由互斥领取，切换模式不补发旧详细/旧完成消息；没有内容、权限错误、业务码失败均不能标SENT。
6. 直接测试、现有DEV真实JSON状态/token fence/并发/租约检查，真实已发布名单的显式测试（不写正式队列）及多轮审核后交付源码。最小本地lint/F2/scope/diff、精确HEAD CI通过后合入。源码、配置、用户重启、自然详细发送分别验收。

## 详情表 Risks / Verification Plan

无需新DDL。DEV仅复用原队列表并回滚测试键，生产不手工插入/修改队列。平台创建/消息接口没有与本地事务跨服务原子提交的保证，因此不确定结果优先防重复并明确失败，不伪造恰好一次；已知消息失败可安全退避。测试群与实际目标群一致、测试记录显式标注，不混入正式荐股。网络调用有10秒超时，单条详情有总时间及输出大小限制；没有新增页面请求、定时器或服务。通知失败不影响荐股发布。

平台先验：测试表`https://my.feishu.cn/sheets/AnCHswkXchSm70tpfE6c73YanLd`已经用户明确确认可打开并看到TEST001，保存证据与历史权限错误见provider_receipt.json；此证明独立于正式自动详情交付，不将测试表当正式当日结果。

内容验收补充：直接使用现有QuoteService的`000001.SZ`证券身份合同，不自建另一套代码格式。无效源身份或缺少推荐期间对象明确失败，不能跳过查询后以整表UNKNOWN冒充完整详情。直接测试必须验证真实代码格式会实际进入共享查询，且连续天数、首次生效日、基准价及已有涨跌值逐字段透传；真实已发布名单验收同时统计各字段覆盖率和缺失原因。用户反馈的测试内容错误只通过显式CLI修复已发送且标注测试的同一文档，保留旧快照摘要、不发送第二条消息；正式队列中的冻结快照不开放此修复入口。两种通知模式共用原每日事件键，切换不会产生第二条日通知。

源码/平台验收：91项直接与既有DEV测试通过（其中7项真实DEV，测试事务回滚），Ruff、文件归属、guardrail及F2符合性检查通过。真实测试工作簿`https://my.feishu.cn/sheets/CmuysP2kqhZBdKtPLC8cXNdenEe`包含4个策略包244行，所有数据已读回、目标群view权限已读回、唯一测试消息已从群历史核对。用户发现首版内容全UNKNOWN后，定位并修复代码格式错误；同文档修复后87只在荐股票的连续天数/首次生效日/基准价/行情价/累计涨跌/今日涨跌均87/87有值，86行明确非本次推荐的期间字段显示不适用。原生TEST001表已由用户确认打开，修复后详情内容用户已明确确认“字段已有数据，显示正常”。源码CI/合入、用户重启和自然事件自动发送分别记录在任务回执，不由平台API成功推导运行已生效。

## 2026-11-20 手机摘要与电脑详情：Scope / Plan / Verification

本增量沿用F2设计，在已完成的原生表格通知上提供两个展示层。每天仍在全部启用任务发布后发一条消息，不为手机/电脑拆成两次通知。手机使用纵向飞书卡片，展示决策日/目标日、各包在荐及新入选/持续/移出/待核验数量、原排名前N只的名称代码和推荐期间摘要，底部按钮打开同一个原生工作簿。默认N=3，可按本次用户回复选择5；多包/长文本触及消息预算时明确标注展示包数，其余完整信息仍在表格中，不把摘要称为完整名单。

电脑端新增第一张“策略总览”，展示全部策略的在荐、变更、观察/待复核/待退出数量、原名单行数及有效报价/推荐期间收益覆盖数，连同原“使用说明”和各包完整19列工作表。总览用于查看名单变化和数据完整度，策略收益横向比较仍沿用主页固定5TD口径；不将各段长度不同的累计涨跌聚合成策略排名。价格与收益仍是捕获时冻结值。

精确源码范围：本文；新增`backend/services/advisory_forward/notification_presentation.py`；既有`notification_details.py`、`feishu_sheets.py`、`notification_transport.py`、`notifications.py`；既有`backend/tests/advisory_model_first/test_advisory_feishu_sheets.py`；新增`test_advisory_notification_presentation.py`；既有`scripts/advisory_feishu_details.py`。任务记录`X:/AIstock_temp/ADVISORY/feishu-dual-view-20261120/task.json`。

两层只格式化同一持久化快照，不新增数据集、排行、行情访问或页面请求。预览只取已发布在荐股票，排序保持原rank，缺少rank不捏造排名；UNKNOWN、未形成开盘基准、不适用分别保留或用有依据的中文提示。构造卡片和体积校验发生在message_started之前；共享严格HTTP/业务码验收与可选签名，不回退文本伪装成功。未发送的旧断点文档补齐总览并复用已有工作表；已接受消息不重发，已正式发送的旧快照不改写。无新DDL、开关、依赖、服务或后端进程操作。

验收覆盖：摘要TopN与全量表格一致、原排名/分组/未知/数量与边界、超长/多包消息预算、URL和快照校验、旧断点恢复、发卡前失败不留下不确定发送标记、签名/真实拒绝/超时/旧通知兼容；随后进行标注测试的手机卡片、桌面总览及人类点击验收。实际群历史证明送达，手机显示不能由API返回的旧客户端兼容文案推导。多轮当前Agent审核及直接合同检查后，经精确HEAD CI交付源码；用户负责后端重启，自然任务通知另验收。

双层展示源码/平台证据：101项直接与既有通知兼容测试通过，9个精确文件scope、Ruff、ownership、guardrail通过。真实平台发送一条标注测试的手机摘要卡片（3759字节），在配置群历史中核对唯一消息；卡片内容摘要与已保存发送状态一致，按钮指向原生工作簿。原工作簿新增策略总览置于index 0，全部6张工作表逐格读回、244行明细及原冻结hash保留、目标群view权限读回通过，无新增行情读取或正式队列写入。手机实际显示/按钮的人类反馈、源码CI/合入、后端用户重启及自然卡片发送均在本轮任务记录分态，不将API成功冒充手机或自然事件验收。

## 2026-11-20 手机全量表格合同（替代TopN摘要）

用户已要求手机显示每个包全部推荐股票和退出清单，本条替代此前F-017的Top3/Top5与超长时减少展示包数的方案。每个策略一个原生表格，三列为“股票”“状态”“累计涨跌”，股票列保留原排名、名称与证券代码。表格先放全部在荐股票，再放已移出/退出建议，最后放其他范围外待复核；手机每页10行，完整rows随同一卡持久化，翻页不会触发行情请求。普通观察候选仍在电脑明细中，已移出的观察股票则进入手机退出相关范围。标题和每行状态区分已移出、退出建议、待复核，不将名单变化当成交完成。

按平台原生表格组件预算最多5个策略表，保留每日一条；当前4个运行包的已验收样本为87只在荐、71只已移出和16只其他待复核，共174行，不漏包、不截断。专用卡片改用紧凑UTF-8实际请求体计算并发送，沿用20KB发送上限并留18KB内容预算；若未来完整内容超过单卡能力，返回固定错误码，不退回摘要、不遗漏数据、不自动扩展多条通知授权。原有文本、邮件及其他模块发送路径保持。

精确写范围：本文、`notification_presentation.py`、`notification_transport.py`、`test_advisory_notification_presentation.py`和`scripts/advisory_feishu_details.py`。任务回执为`X:/AIstock_temp/ADVISORY/feishu-mobile-tables-20261120/task.json`。先完成全量覆盖/状态/排序/分页/Unicode字节与实际wire一致/旧通道兼容的直接检查和多轮审核，再用同一历史冻结快照发送一条明确标注测试的全量表格卡片；读群历史时请求原始卡片JSON，逐包逐行核对。人类手机分页显示、源码CI/合入、用户重启和自然发送分别验收。无新的数据读取、训练、数据库更改、服务控制、审批或开关。

本轮源码/平台验收：103项直接与旧通道兼容测试、Ruff、5文件scope、ownership、guardrail和quality L0通过。群内唯一全量测试卡片的原始JSON已读回，四表分别44/44/41/45行，174行的内容、顺序、列和page_size=10与发送内容逐项一致；原冻结hash与电脑完整明细保持。真实卡片为14891个UTF-8字节，ASCII转义会膨胀到22140字节，因此对专用卡片执行实际紧凑UTF-8发送，字节门禁和wire测试一致，原文本和其他通道不改。手机实际分页反馈、源码CI/合入和重启后自然事件证据仍按任务记录各自更新，不用API成功替代人类或运行验收。
