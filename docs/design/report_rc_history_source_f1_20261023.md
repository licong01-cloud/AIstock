# report_rc 历史原始面板 F1 设计

## 背景

用户批准范围为 2023-07-01..2025-09-30 的历史原始面板。展示接口缺 quarter 的去重不满足研究；现有 DEV/生产均无 report_rc 表，无同名运行采集任务。原两次探测无原始数据，不视为已交付。

## 范围与非目标

仅共享数据准备、原始持久化、版本化 Parquet、读取与质量检查。复用 Tushare HTTP 传输、限流、显式数据库目标的原始接入方式。通用日频 upsert 会损失重复及修订，故采用与已有 tushare_*_raw 相同的独立原始接入模式；不注册会自动去重的展示 DatasetSpec，不改 Advisory 展示接口、不新增默认每日调度或 UI。

不改 Qlib/H5/profile，不读取收益，不训练，不下载 PDF，不调用 LLM，不恢复其他采集任务，不控制服务。生产写入单独授权。

## Design Acceptance Index

- F-001 固定自然日范围、字段完整、原始重复/冲突/多预测期不丢失。
- F-002 非 vintage 时钟：report_date、provider create_time、真实 captured_at 分离；不推定首次公开时间。
- F-003 分页、有限重试、单写者、断点续采；失败与空响应分离，不将截断报告完整。
- F-004 现有 DEV 两张原始表持久化及幂等读回，生产未授权不写。
- F-005 原始/标准化 Parquet、schema、manifest、校验与独立读取。
- F-006 同机构同 quarter 配对可追溯；预测期滚动/首次/无前值/冲突/未知分开，EPS 股本口径未知不冒称可比。
- F-007 原 Top50 仅身份覆盖，不重建人口，无清单明确交接。

## 实施方案

本任务允许文件清单：backend/services/report_rc_data.py、backend/services/report_rc_data_delivery.py、scripts/prepare_report_rc_history.py、scripts/export_report_rc_history.py、backend/db/migrations/add_report_rc_raw_v1.sql、backend/tests/test_report_rc_data.py、本设计文档，以及仅登记上述测试/新文件所属边界的noxfile.py和tests/aistock_validation/catalog/file_ownership.yaml。不得扩展到业务消费者或修改其他流水线任务。

`backend/services/report_rc_data.py` 为 source-only producer；`scripts/prepare_report_rc_history.py` 为显式范围/目标 CLI；迁移只新增 capture/raw 表，不改变既有表。采集按自然日，单页 3000，满页继续 offset，重复页报错；每页最多3次，30秒超时，退避1/2秒，120请求/分钟、4线程。每页保存已成功响应和实际采集时间，完成日期才写完成收据。重启仅复用合同/hash一致的成功页；失败页不冒充空日。

原始记录按批次/日期/页/行保存重复；标准化不做机构同义词合并、不同 quarter 合并或冲突胜出选择。相同 org 文本仅 trim；eps 单位为供应商文档的元/股，币种字段未提供、实际股本口径 UNKNOWN；供应商没有报告ID时保存 NULL，不制造ID并声称供应商原生ID。内部 lineage key 与供应商ID明确分离。

采集合同 v1 固定不改；标准化版本 `report_rc_preserve_occurrences_v2` 将缺失或非标准 quarter 显式标记 `FORECAST_PERIOD_UNKNOWN`，保留原始字符串且不作为普通季度配对。已完成原始数据不重新采集；初次文件验收发现的这一质量问题仅产生独立 `delivery-v2`，`delivery-v1` 保留且原始 Parquet 字节完全一致。

本轮官网文档复核：当日21:15、次日09:15更新两次，与旧 skill 的19..22描述有变化。这只是供应商更新计划，不是每条报告历史首次公开时间，不用于倒填 available_at。原两包Top50只按既存身份/日期列核对“报告日不晚于D”的输入支持，仍不声称D时可得或形成信号。

## 验证方案

RED→GREEN：满页续页、重复页、HTTP/协议错误、schema不符、失败续采、真实空日、合同漂移、重复/冲突/季度滚动/NaN、文件独立读取及 DEV 幂等。真实全范围逐日收据闭环。可证明的是当前供应商重建快照，不是历史首次发布 vintage。

## 风险与失败模式

供应商分页期间变动、晚修及历史删除无法恢复为历史 vintage；明确 NON_VINTAGE，收集窗口内多页存在一致性限制。原始重复保留；不可比口径不输出经济修正信号。无记录仅指本次源端成功空响应，不代表历史从未有报告。原始机构字符串不代表已解决法人别名。

## 生产门禁

仅现有 aistock_dev 执行 DDL/DML；连接不允许 dotenv 覆盖显式目标。aistock 生产 target、SQL、DEV 结果需独立授权。无依赖安装，无重启需求，不切数据集。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/report_rc_data.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_temp/DATA_PREPARATION/hmm-classification-report-rc-20261023/report-rc-final-readback.json | verified | 无 |
| F-002 | backend/services/report_rc_data.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2/quality.json | verified | 无 |
| F-003 | backend/services/report_rc_data.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2/date_coverage.parquet | verified | 无 |
| F-004 | backend/db/migrations/add_report_rc_raw_v1.sql | artifact: X:/AIstock_temp/DATA_PREPARATION/hmm-classification-report-rc-20261023/dev-validation-548991.json | verified | 无 |
| F-005 | backend/services/report_rc_data_delivery.py；scripts/export_report_rc_history.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_temp/DATA_PREPARATION/hmm-classification-report-rc-20261023/report-rc-final-readback.json | verified | 无 |
| F-006 | backend/services/report_rc_data_delivery.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2/examples.json | verified | 无 |
| F-007 | backend/services/report_rc_data_delivery.py | backend/tests/test_report_rc_data.py；artifact: X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2/original_population_input_coverage.parquet | verified | 无 |

## 实际交付与设计合规复核

- Feature Issue：#6640；F1。无默认调度或展示去重变更，无新增依赖。
- 数据目录：`X:/AIstock_dataset_candidates/research_source_candidates/report_rc/20230701-20250930-capture-20261023-v1/delivery-v2`。
- manifest SHA256：`4d1bd7f24ef29295eae450f47c664a84b2e36efbc4e0ebdc141619e0acbc4222`。
- 实物统计：823日、548991行；16次请求失败均恢复，最终失败日期0、成功空日31。4128行EPS未知、2169行预测期未知保留；真实重复/冲突均0，不伪造真实样例。原52400身份行不变，其中30500行处于采集范围，未读收益。DEV全量精确读回、重复新增0；生产写入仍需独立授权。
- DESIGN-COMPLIANCE-001：逐项设计均由真实采集、文件和现有DEV证据验证；不是仅mock成功；无未批准的裁剪/替代；未知时钟/股本/别名按用户要求保留并明确限制，未冒称历史PIT或生产已应用。
- Advisory负责后续as-of、信号定义、F1及有界实验。本交付只证明当前供应商历史原始快照可重读，历史PIT尚未成立。
