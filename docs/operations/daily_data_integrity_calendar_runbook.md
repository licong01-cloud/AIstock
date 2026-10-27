# 每日数据完整性台账上线与消费手册

关联：[F2设计及24项源码验收](../architecture/daily_data_integrity_calendar_and_readiness_f2_design_20261027.md)、[Feature #6774](https://github.com/licong01-cloud/AIstock/issues/6774)。这不是另一套发布/审批平台。

## 1. 本次交付和未执行项

本次实现事实分区revision、原子待验目标、共享worker、交易日版本化状态、日历/详情/缺口分页、策略包依赖就绪、短一致性输入和月更SOURCE修订pins。仅使用原调度器/线程池、audit和data_sync_targets。

状态独立记录：源码/DEV验证已完成；生产授权尚未取得、生产DDL尚未应用、运行态尚未验收。未启动采集、导出、正式预测、训练或实验，未操作用户服务。不能据此通知业务窗口“生产已经就绪”。

## 2. 精确迁移及顺序

迁移文件：`backend/db/migrations/add_daily_data_integrity_v1.sql`。

文件字节SHA256：`6c92fbe5f8982a5887040696f59b74453009894200d1823493241cb5da0c8750`。执行前必须从已合入commit重新核对；如git换行转换导致字节不同，报告实际字节hash并重新确认精确生产输入，不能套用本hash。

生产目标仅为Windows PostgreSQL `127.0.0.1:5432/aistock` 的以下元数据对象：

- `market.dataset_date_refresh_audit`新增6列：data_revision、validated_revision、validation_status、validation_id、scope_version、rule_version；旧行默认待验，不默认成功。
- `idx_dataset_integrity_canonical_date`部分索引。
- 新表`market.data_daily_integrity_revisions`及字段注释。
- 函数`market.mark_data_integrity_changed(text,date,text,text)`，由既有受管writer在同一事实事务调用；没有行级trigger。

迁移无行情表DML，价格/财务/资金流/行业事实修改行数为0；不要求生产备份、快照或重导。锁超时5秒、语句超时60秒；失败回滚并报告，不擅自加大超时或中断其他任务。

上线顺序：

1. 确认最终merge commit包含文件，复核文件hash、现有DEV重复迁移及6项事务结果。
2. 取得上述具体生产目标/迁移授权后，通过现有正式数据库运维连接执行该SQL；不把源码合入授权当DDL授权。
3. 分别读回6列的类型/default/comment、新表主键/约束/注释、部分索引和函数；确认未修改行情事实。
4. 用户集中更新/重启FastAPI后端和实际负责入库的TDX Go后端。不能只重启FastAPI便称Go writer协议已生效；旧Go二进制不具备新marker。worker-scheduler按现有启动方式随实际后端版本核对，不私自控制进程。
5. 只读确认新四接口及真实目标日状态、实际writer提交后的R/待验目标、canonical复验、日历和消费者读回；未完成任一步保留其真实状态。

新旧schema兼容期间：无迁移的writer继续原流程，但新的完整性接口明确503，不能用旧job success冒充新台账。迁移后原provider观察不是canonical验收。历史没有相匹配结果的日期显示“待核验”；不为了页面全绿启动全部历史扫描。

## 3. 自动动作与性能界限

- 实际事实改变才增加R；无变化重试/last_seen刷新不增加R。修改与R、VALIDATE_ONLY目标同事务，失败全部回滚。
- 分区脏立即可见；原始分钟批次使用30秒去抖合并验收目标，不阻止数据使用、不创建额外来源审批。单批最多10项、45秒软预算，单SQL30秒上限；运行中lease保护/过期fencing使旧结果不能覆盖新事实。
- 正式调度种子仅当前和上一交易日，加持久化的精确历史修复目标。不新增全史自检；源端回溯改写仍由既有独立核对发现后标记影响区间。
- 已知网络、迟到、部分发布等沿原FETCH/派生任务重试。告警截止不是永久放弃；已过短尾窗的已知目标继续低频追赶。重复验证不把FETCH时间不断向后推。
- 未知协议、冲突、磁盘/权限等保留错误，不自动改代码、删文件、造bar、填零或重启服务。

`daily_basic`既有95%采集准入不改变；日历的“全部无缺口”另逐必需字段对账，剩余缺项如实显示。可空估值、合法0成交、明确停牌/预热/行业停发按原合同保留，不制造成功。

## 4. 受管写入口清单

| 路径/入口 | 行为 |
|---|---|
| TushareSyncEngine普通/镜像/事件/参考/财务采集 | 实际变化RETURNING；镜像完全一致不重写；事件按真实ann_date；成功空扫登记原事实日 |
| TDX `web/raw_rows.go` | 新增及精度补空的真实日期，同Go事务调用marker；无变化重放不增R，冲突严格回滚 |
| adj_factor_history_reconciler | 按已验证stage差异记录真实历史日期，不新增每日全史审计 |
| sector_data_builder / sector_data_repair | 实际派生变化/依赖失效进同一链，沿用现有行业聚合合同 |
| ingest_tushare_daily_basic/adj_factor/moneyflow_ts | 实际变化返回日期，非有限/空值不覆盖已有合法值 |
| ingest_full_minute、import_minutes_to_db、import_minutes_gap | 共用changed_values，价格事实/修订/目标同事务；不因PIT裁行情 |
| backfill_tushare_daily_basic_fields、backfill_raw_volume_precision | 原受控补空/精度合同不变，补录日期原子失效 |
| repair_stk_limit_pre_close、repair_historical_daily_fact_absence、repair_local_data_daily_sync、repair_local_sync_incident | 正式生成SQL保留具体目标/来源边界，并在同事务标记实际影响日期；不增加生产DML权限 |
| sync_eastmoney_anns_metadata、sync_cninfo_anns_metadata | 分页完成/失败分别记录，语义变化才增R，真实抓取时间不倒填 |

任意临时旁路SQL、旧二进制或未登记外部写库程序不能被声称自动受管。后续新增正式writer必须接同一协议，不维护第二份成功状态；这是源码入库契约，不是每日人工审批。

## 5. 页面和只读消费

入口`/local-data`的“交易日数据完整性”组件独立于任务卡。日期颜色使用服务端聚合，点击看字段/证券/计数单位、原事实日、应到时间、revision及重试。未知为未知；HTTP200不代表数据完整，503不批量修改持久日状态。

共享接口（既有`/api/v1/local-data`前缀）：

- `GET /integrity/calendar?month=YYYY-MM`
- `GET /integrity/days/YYYY-MM-DD`
- `GET /integrity/days/YYYY-MM-DD/gaps?dataset=...&limit=100&cursor=...`
- `GET /readiness?data_date=...&consumer=SELECTION&binding_id=<正式策略包ID>`

`all_sync_complete`是该日正式义务全闭合；`consumer_ready/prediction_ready`只按已批准包的必需输入和窗口投影。不用迟到融资融券的包不因它等待；无法确定依赖/模型序列窗口时明确UNKNOWN，不假装支持。HMM/QE等非策略包authority不猜测私有ID，保留已有各自正式输入预检。

global PIT使用官方resolver，台账不另建默认PIT。数据ready不是任务已受理、预测成功或交易许可；本功能不创建任务、订单、账户，也不覆盖历史结果。源捕获用同一短RR只读事务，嵌套连接共用且不能提前提交；模型计算在事务结束后执行。跨进程使用已捕获输入而不是各自重读最新数据库。

## 6. 月更和文件覆盖

SOURCE在既有同快照读取canonical结论及修订，精确历史修复同快照取pins；finalizer登记`reports/source_integrity_revisions.json`及其manifest hash。封存后的新增事实走现有修订流程，不能改变旧SOURCE/已完成组件或旧任务绑定。

日历源绿色不等于Bin/H5已包含。活动release无pins时显示“未建立对应关系”；截止日之外显示“未覆盖”；已知pin不一致明确显示不匹配；文件不可读单独提示，不能伪造源缺口。只读日历不扫描Bin/H5，不因为补此显示重新全量导出或加16MiB私有门槛。

## 7. 上线后验收、回滚与后续协作

验收清单：实际版本加载；四只读接口；当前/上一交易日自动待验；一例真实已受管提交的R和target一致；迟到补入更新原日期；复验状态与健康检查一致；月更SOURCE pins和真实文件发布分别读回。没有被授权启动预测/实验来作验收，不恢复LocalSIM断点或修改账本。

发现问题先停止本功能新增状态发布/新通知，保留事实、旧输入、有效任务和原生错误；修复后按revision重验。不要删除revision表或回滚成“job success即ready”；依赖这些列的代码尚在运行时不能先卸载schema。后端进程控制仍由用户执行。

Feature #6774在生产迁移、用户重启和真实目标日读回完成前保持开放。源码合入、DEV验证、production authorization、production apply、production readback、runtime verification六项分开更新。
