# 共享月更 v2 实现验收矩阵

对应[详细设计](monthly_unified_dataset_release_v2_f2_design_20260921.md)。2026-10-26按既存真实回执修订；本矩阵不是每月新增的发布门禁。本轮没有重导数据、扫描全历史或启动实验。

精确身份、路径、hash和限制见[验收对账](../handoff/monthly_release_acceptance_reconciliation_20261026.md)：

- E1：原9月operation的六阶段checkpoint、READY及11项真实consumer result。
- E2：当前修订successor的正式激活、双节点fresh-process及运行API读回。
- E3：当前closure、派生资产及11项准备合同身份读回。
- E4：原95个DEV profile及生产默认PIT的独立只读读回。

E1绑定原manifest `522e8e35…f410`，E2/E3绑定当前manifest `afa9158b…5337`，不得混用。E3的 `READY_FOR_NODE_READBACK` contract不是实际consumer smoke。`verified`只代表对应条款的列明证据，不能等同于整个Feature完成、全量数据无缺口或所有业务实验已成功。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `monthly_unified.py`、`monthly_immutable_deploy.py`、`monthly_profile_candidate.py` | `backend/tests/dataset_release/test_monthly_unified_v2.py`；unified/profile/create-exclusive/CAS测试；E1真实发布、E2旧release保留 | in_progress | baseline不可变/COW证据仍需归并；目录存在不等于字节未污染 |
| F-002 | `control_service.py`、`monthly_dataset_releases.py`、CLI/MCP | `backend/tests/dataset_release/test_monthly_unified_v2.py`；router/CLI/MCP幂等测试；E1同一durable operation | in_progress | 真实operation已闭环；尚缺三入口同key真实对照，不再声称尚无闭月operation |
| F-003 | `monthly_unified.py`、`monthly_worker_runtime.py` | `backend/tests/dataset_release/test_monthly_worker_runtime_cli.py`；unified/runtime CLI测试；E1跨attempt恢复 | in_progress | 恢复已有真实证据，指定进程故障/取消边界演练仍未闭合；不终止生产worker补证 |
| F-004 | `monthly_postgres_source.py`、`monthly_source_audit.py` | `backend/tests/dataset_release/test_monthly_postgres_source.py`；postgres source测试；E1 change-scope及pins | in_progress | 真实scope已封存；后续历史修复与规则变化依赖闭包的回执仍需归并，不重扫全历史 |
| F-005 | `monthly_snapshot.py`、`monthly_source_producer.py` | `backend/tests/dataset_release/test_monthly_snapshot.py`；snapshot/source producer测试；E1 SOURCE consistent_input_set_complete | in_progress | 同视图SOURCE已执行；尚缺生产只读并发修订/快照失效演练证据 |
| F-006 | source audit、postgres source、shared components | `backend/tests/dataset_release/test_monthly_source_audit.py`；日历/生命周期/六池测试；E1对应gate | in_progress | 真实gate已执行；后继事实目录修复的完整分母与例外尚待归并，六池PASS不代替全部字段验收 |
| F-007 | `monthly_source_audit.py`、正式分钟会话规范 | `backend/tests/dataset_release/test_monthly_source_audit.py`；minute/停牌/日分钟OHLCV及amount测试；E1 minute_price | in_progress | 真实增量月已检查；后继历史分钟修复/有限值差异按精确回执继承；按既定时间戳规范，不按240/241行数造bar |
| F-008 | source audit、postgres source | `backend/tests/dataset_release/test_monthly_source_audit.py`；`test_daily_basic_checks_rows_and_all_required_fields`及分页测试；E1字段gate | in_progress | 真实字段gate已执行；延迟字段、nullable及历史补值回执尚待完整归并，不承诺所有上游空值可自动恢复 |
| F-009 | source audit、postgres source、build bridge | `backend/tests/dataset_release/test_monthly_source_audit.py`；adj tail/rebase/restatement及invalidation测试；E1 adj gate | in_progress | 尚缺真实历史重述选择性重建演练，普通月尾PASS不代替 |
| F-010 | `monthly_repair_journal.py`、`monthly_source_producer.py` | `backend/tests/dataset_release/test_monthly_repair_journal.py`；repair journal测试及既有精确DEV/授权生产/readback回执 | in_progress | 全部受管writer影响范围的归并仍需完成；不自动执行生产DML |
| F-011 | build bridge、incremental baseline、有界Qlib writer | `backend/tests/dataset_release/test_monthly_build_bridge.py`；四种action/calendar前缀/mixed planner测试；E1 BUILD | in_progress | 真实BUILD已完成；增量/全量等价及全部失败attempt计入的性能证据未闭合，不重导当前数据集补证 |
| F-012 | source audit、shared components、fact catalog/PIT producer | `backend/tests/dataset_release/test_monthly_shared_components.py`；生命周期/provider隔离测试；E1及后继事实目录修复 | in_progress | 物理目录/PIT分离已有修复；后继全事实范围与验收分母尚待归并，sentinel不能代替完整覆盖 |
| F-013 | factor materializer、build bridge、mature build runner | `backend/tests/dataset_release/test_monthly_build_bridge.py`；增量/选择性重建/burn-in/历史失效测试；E1 BUILD | in_progress | 真实factor已交付；尚缺真实增量/全量值对照及慢频/rolling恢复证据 |
| F-014 | shared components、consumer layout、SW L2 quote policy | `backend/tests/dataset_release/test_monthly_shared_components.py`；shared component/layout测试；E1 HMM读回、E3 sector pins | in_progress | 共享资产已交付；历史行业、availability及字段一致性仍需归并，ID覆盖不等于数值字段完整 |
| F-015 | build executor、supervised scope、immutable/remote deploy | `backend/tests/dataset_release/test_monthly_remote_deploy.py`；remote deploy/build恢复与COW测试；E1三端transfer/registration；E2修订部署读回 | verified | none |
| F-016 | official adapters、profile candidate、local validation、profile contract | `backend/tests/dataset_release/test_monthly_local_validation.py`；local validation/profile测试；E1 LOCAL_VALIDATE；E2/E3当前B/C/E/P与继承pins | in_progress | 当前身份链和三端manifest一致；不把E3准备合同提升为当前11项业务smoke，后继范围归并见F-025 |
| F-017 | HMM derive/production/consumer probe | `backend/tests/dataset_release/test_monthly_hmm_consumer_probe.py`；authority/producer/probe测试；E1 DERIVE真实71053行、fit=0、节点548日；E3资产manifest匹配 | in_progress | 正式生成及原输入节点预检已执行；当前修订的推理继承/重建依据尚待归并，不只改hash或代替HMM业务验收 |
| F-018 | QE consumer/node probes、active resolver、Composer | `backend/tests/dataset_release/test_monthly_qe_consumer_probe.py`；QE/node probe测试；E1 single/custom/multi-alpha/P10/P11真实result PASS | in_progress | 原五类smoke确已执行，含P11物化及Qlib日/分钟读取；不是当前所有修订后实验/完整物理覆盖验收 |
| F-019 | shared consumer probe、active task binding、factor dispatch | `backend/tests/dataset_release/test_monthly_shared_consumer_probe.py`；shared consumer/binding/dispatch测试；E1 factor真实节点读回 | in_progress | 真实文件消费已执行；cache命名空间和旧任务冻结身份的独立证据尚待归并，不启动因子评价补证 |
| F-020 | shared consumer probe、managed consumer task、数据准备入口 | `backend/tests/dataset_release/test_monthly_shared_consumer_probe.py`；shared consumer/managed task测试；E1 Selection/Advisory/择时/统一回测真实result | in_progress | 原输入probe已执行；独立authority/历史任务接续由owner确认，不自动修改旧request或账户 |
| F-021 | active task binding、managed consumer task、dispatch | `backend/tests/dataset_release/test_monthly_unified_v2.py`；resolve-once/retry/remote submit失败测试；E2旧binding未改 | in_progress | 真实排队跨切换演练未闭合，不提交生产任务或重绑旧任务制造验收 |
| F-022 | runtime release registration、remote deploy、RD-Agent identity | `backend/tests/dataset_release/test_monthly_remote_deploy.py`；registry/remote deploy测试；E1登记、E2现有API读新manifest | in_progress | API已可读当前release；固定API进程身份跨新增registry前后对照尚缺，fresh process不能代替 |
| F-023 | `monthly_unified.py`、monthly release router | `backend/tests/dataset_release/test_monthly_unified_v2.py`；CAS/intent/ALREADY_APPLIED测试；E2精确授权、正式CAS、ACTIVATED_VERIFIED及双节点API | verified | none |
| F-024 | monthly unified、retention、task binding | `backend/tests/dataset_release/test_monthly_unified_v2.py`；rollback/retention测试；E2旧release/binding保留 | in_progress | 真实回滚演练及跨dispatch/实验/训练完整引用聚合未闭合，禁止自动删旧release，不切生产补演练 |
| F-025 | consumer validation/registry、node probes | `backend/tests/dataset_release/test_monthly_consumer_validation.py`；正式组合测试；E1精确11项真实result、unresolved=0；E2节点身份 | in_progress | 原scope不替代后继修复范围；当前11项准备合同不是完整consumer readback；不重做已验证历史 |
| F-026 | official adapters、remote deploy、monthly unified | `backend/tests/dataset_release/test_monthly_unified_v2.py`；telemetry测试；E1成功阶段合计4.512446小时；BUG-1891局部性能实测 | in_progress | 合计不含早前失败/暂停/修复，不是本月总时长或下月10小时SLA；真实整链增量性能仍待验收，telemetry不作准入 |
| F-027 | CLI/worker/supervisor、backend lifespan、API/MCP、Skill/runbook | `backend/tests/dataset_release/test_monthly_worker_runtime_cli.py`；worker组合/薄入口测试；E1真实run恢复至READY及激活 | in_progress | 后端真实演练已完成；UI/CLI/MCP统一入口与恢复展示证据尚待归并，不再发起构建证明历史 |
| F-028 | monthly runtime、SW L2 quote policy、HMM loader | `backend/tests/dataset_release/test_monthly_runtime.py`；跨年cutoff/动态taxonomy/availability/hash漂移测试 | in_progress | 下一月份taxonomy不变/变更两类演练尚未发生，不虚构未来输入或硬编码131 |
| F-029 | profile v4 resolver、remote deploy、node probe runner | `backend/tests/dataset_release/test_monthly_remote_deploy.py`；v3/v4/registry测试；E1部署；E2当前v4及双节点fresh/运行API complete=true | verified | none |
| F-030 | monthly unified ready/activation receipts | `backend/tests/dataset_release/test_monthly_unified_v2.py`；六阶段持久回执测试；E1完整六阶段及正式READY；E2激活/节点分态 | verified | none |

## 当前结论及完整Feature检查

4项由真实证据闭合，26项保持 `in_progress`。这是Feature验收状态，**不是数据覆盖率、代码完成百分比或当前release未激活**。记录归并、故障/性能及下一月演练不等于当前业务需要重导、补行情或重启。

```text
python scripts/aistock_feature_workflow.py validate --design docs/architecture/monthly_unified_dataset_release_v2_f2_design_20260921.md --acceptance docs/architecture/monthly_unified_dataset_release_v2_acceptance_20260921.md --tier F2
```

完整Feature检查仍应FAIL（存在明确未验收条款），不得豁免为功能PASS。本次仅交付验收记录修订，docs-fast按格式/链接及 `git diff --check` 验证。后续复用已完成范围，不为关闭Issue重跑全历史或增加审批。
