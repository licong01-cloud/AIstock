# 荐股自动通知 F2 设计

## 背景

用户授权先交付自动通知代码，复用现有通知服务且不影响其他模块。通知与荐股名单发布、价格结算、收益评价分别计状态。

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

## 风险

外部交付无法与数据库原子提交，极端崩溃可重复。共享旧接口兼容行为不作为荐股成功依据。超时/配置错误不允许伪造 SENT；过期及最终失败保留。队列无级联外键，不因业务名单删除而丢失发送证据。

## 发布与回滚

先交付默认关闭的源码。后续独立授权生产迁移、用户重启、启用指定通道和真实通知验证。回滚关闭荐股通知开关，保留发送记录；不改其他通知全局开关，不删除表。

## 生产门禁

现有 aistock_dev 已通过幂等迁移、真实 SQL 去重/重试/token fencing/SKIP LOCKED/租约恢复/过期以及日历快照验证；只有新队列表持久化，测试行零残留。生产 DDL/DML 未授权/未执行，真实通知未授权/未发送，运行启用未执行，后端重启归用户。代码验收通过不等于运行通知已经启用。

本地验收：39 passed（含 4 项真实 DEV 测试）；原监控 Markdown/interactive 消息、原组合邮件调用与原 fallback 合同兼容。最终版本审核及 CI 状态写入任务记录。

2026-11-16 完成通知增量：70 项直接/调度/真实 DEV 测试通过，其中 5 项真实 DEV 验证原队列状态机与新旧路由隔离，DEV 测试行零残留、schema fingerprint 不变。Ruff、L0、文件归属和设计符合性通过，当前 Agent 多轮自审不冒充独立外审。源码与配置、运行、真实群发送分别验收；本增量无生产 DDL/DML、真实发送或服务操作。缺少机器人配置或尚未完成用户重启时不宣称飞书已启用。
