# 荐股自动通知 F2 设计

## 背景

用户授权先交付自动通知代码，复用现有通知服务且不影响其他模块。通知与荐股名单发布、价格结算、收益评价分别计状态。

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

## 风险

外部交付无法与数据库原子提交，极端崩溃可重复。共享旧接口兼容行为不作为荐股成功依据。超时/配置错误不允许伪造 SENT；过期及最终失败保留。队列无级联外键，不因业务名单删除而丢失发送证据。

## 发布与回滚

先交付默认关闭的源码。后续独立授权生产迁移、用户重启、启用指定通道和真实通知验证。回滚关闭荐股通知开关，保留发送记录；不改其他通知全局开关，不删除表。

## 生产门禁

现有 aistock_dev 已通过幂等迁移、真实 SQL 去重/重试/token fencing/SKIP LOCKED/租约恢复/过期以及日历快照验证；只有新队列表持久化，测试行零残留。生产 DDL/DML 未授权/未执行，真实通知未授权/未发送，运行启用未执行，后端重启归用户。代码验收通过不等于运行通知已经启用。

本地验收：39 passed（含 4 项真实 DEV 测试）；原监控 Markdown/interactive 消息、原组合邮件调用与原 fallback 合同兼容。最终版本审核及 CI 状态写入任务记录。
