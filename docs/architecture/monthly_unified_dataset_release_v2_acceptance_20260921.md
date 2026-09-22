# 共享月更 v2 实现验收矩阵

对应[详细设计](monthly_unified_dataset_release_v2_f2_design_20260921.md)。本文件是后续完整开发的工作清单，不是当前功能已通过的回执。所有行初始pending；不得因设计文档合入而改为verified。`implementation_refs`是拟实现责任位置，`test_or_evidence`是计划验证场景，实施时替换为实际代码/测试/回执。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | dataset_release计划与封存 | 单一successor和旧文件不变验证 | pending | 尚未开发验收 |
| F-002 | MonthlyReleaseService与薄入口 | API/CLI/MCP请求返回同operation | pending | 尚未开发验收 |
| F-003 | release状态store与锁 | 双writer、重复请求、批次取消/恢复 | pending | 尚未开发验收 |
| F-004 | source影响范围adapter | 历史补录、新尾部、范围不全的组件重建 | pending | 尚未开发验收 |
| F-005 | snapshot读取adapter | 并发修改和中途崩溃不混版本 | pending | 尚未开发验收 |
| F-006 | 日历/lifecycle/六池validator | 正常与缺日、退市历史population | pending | 尚未开发验收 |
| F-007 | 分钟与日线parity validator | 交易会话、停牌和量额单位差异 | pending | 尚未开发验收 |
| F-008 | daily_basic统一字段检查器 | 两种入口相同字段/分母/分页规则 | pending | 尚未开发验收 |
| F-009 | adj invalidation adapter | 四类变化及尾部增长无误全重建 | pending | 尚未开发验收 |
| F-010 | local_data repair协调adapter | DEV、授权、apply、readback独立 | pending | 尚未开发验收 |
| F-011 | `monthly_build_bridge.py`、`monthly_incremental_baseline.py`、既有 bounded Qlib writer | 四种 action、predecessor manifest/CAS/Merkle 闭合、首迁单次组件重建及后续增量/选择性计划测试 | in_progress | 增量 baseline 与正式 mixed planner 已接通；仍待真实闭月 candidate 验证 calendar 严格前缀、增量等价和性能后转为 verified |
| F-012 | PIT lifecycle物化 | 新旧证券完整观测历史和选股隔离 | pending | 尚未开发验收 |
| F-013 | factor rolling/aggregate writer | 月界对照与历史修订后效 | pending | 尚未开发验收 |
| F-014 | `monthly_shared_components.py`、`monthly_consumer_layout.py`、`sw_l2_quote_policy.py` | `test_monthly_shared_components.py`、`test_monthly_consumer_layout.py`：共享sidecar、QE/HMM发布目录、benchmark/meta/coverage、hardlink零重复数据发布 | in_progress | sealed SOURCE与共享consumer layout生产器已实现；仍待真实闭月 candidate 回放和三节点 readback 后转为 verified |
| F-015 | `monthly_build_executor.py`、`monthly_mature_build_runner.py`、`monthly_supervised_scope.py`、`monthly_immutable_deploy.py` | create-exclusive staging、原子发布、attempt独立supervisor、跨卷私有复制、hardlink别名保留、同字节resume与漂移拒绝测试 | in_progress | 私有构建、无重复大文件部署和失败恢复合同已闭合；仍待正式node1 transport及真实闭月三节点回执后转为verified |
| F-016 | `monthly_official_adapters.py`、`monthly_worker.py`、`monthly_immutable_deploy.py`、`monthly_profile_candidate.py`、`monthly_local_validation.py`、`profile_contract.py` | `test_monthly_local_validation.py`、`test_monthly_profile_candidate.py`及dataset-release全量回归：candidate-local C/E、11 consumer contracts、manifest/derived pins、predecessor lineage、共享consumer requirements | in_progress | B/C/E/P及文件型LOCAL_VALIDATE身份已实现；仍待真实R/C readback和三节点闭月回放后转为verified |
| F-017 | `monthly_hmm_derive.py`、`monthly_hmm_consumer_probe.py`、`monthly_official_adapters.py` | `test_monthly_official_adapters.py`、`test_monthly_hmm_consumer_probe.py`：正式file-only producer适配、冻结model/config/script hash、新B/C绑定、正式frozen-input loader读回、无fit/no-DB、完整日期grid及同字节resume | in_progress | 数据侧正式派生、登记与file-only输入消费合同已实现；仍待正式模型authority配置、真实闭月产物及节点readback后转为verified |
| F-018 | `monthly_qe_consumer_probe.py`、QE active-profile resolver、Composer coefficient reader | `test_monthly_qe_consumer_probe.py`：single/custom/multi-alpha实际universe binding、P10三重身份和共享系数读取、P11实际物化过滤股票池、无训练/实验副作用 | in_progress | 五类QE file-only probe已实现；仍待正式probe registry组装、真实闭月candidate及WSL/node1 readback后转为verified |
| F-019 | `monthly_shared_consumer_probe.py`、factor-research consumer binding | `test_monthly_shared_consumer_probe.py`：factor/day/manifest/stock-pool精确文件绑定与可读性 | in_progress | 因子研发数据文件probe已实现；正式计算任务的resolve-once cache identity及旧任务复现仍待接入 |
| F-020 | `monthly_shared_consumer_probe.py`、active profile consumer resolver | `test_monthly_shared_consumer_probe.py`：Selection/Advisory/择时/统一回测精确组件binding及真实JSON/text/Parquet/HDF sentinel reader | in_progress | 共享文件binding probe已实现；各业务新建任务入口的resolve-once持久化及真实节点readback尚未闭合 |
| F-021 | dispatch/task冻结binding | 排队跨切换、retry不重新resolve | pending | 尚未开发验收 |
| F-022 | RD-Agent registry/identity | 已运行API识别新release，无逐月重启 | pending | 尚未开发验收 |
| F-023 | authorization/CAS/activation | 双切换、崩溃、读回、伪授权拒绝 | pending | 尚未开发验收 |
| F-024 | rollback与引用清单 | 并发安全回滚和旧任务引用保留 | pending | 尚未开发验收 |
| F-025 | `monthly_local_validation.py`、`monthly_consumer_validation.py`、`monthly_consumer_registry.py`、各consumer probes、`monthly_official_adapters.py` | 对应dataset-release测试：完整六池scope、不可变精确11 controller preflight注册、v4 binding/node registration/manifest闭合、side-effect fail-closed | in_progress | 11项code-owned controller preflight与本地文件读取已实现，且未包装为node PASS；WSL/node1远程执行及真实三节点闭月readback尚未接入，未达到verified |
| F-026 | telemetry/分区/传输 | 真实读算写传hash成本，无资源准入 | pending | 尚未开发验收 |
| F-027 | 产品入口/Skill/runbook | 一次真实run及恢复到ready/切换 | pending | 尚未开发验收 |
| F-028 | 动态日期/目录/model合同 | 非固定计数的新月份与新行业 | pending | 尚未开发验收 |
| F-029 | v4兼容与双仓部署 | 读端先行、legacy显式保留 | pending | 尚未开发验收 |
| F-030 | release总回执 | 各生产动作和功能状态分别读回 | pending | 尚未开发验收 |

## 后续开发验收命令

```text
python scripts/aistock_feature_workflow.py validate --design docs/architecture/monthly_unified_dataset_release_v2_f2_design_20260921.md --acceptance docs/architecture/monthly_unified_dataset_release_v2_acceptance_20260921.md --tier F2
```

当前预期为FAIL（尚无实现证据），不是设计语法豁免或生产准入。后续填入真实证据后必须PASS；不能使用设计审核记录替代功能测试。
