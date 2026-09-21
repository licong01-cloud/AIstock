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
| F-011 | direct增量planner/Qlib writer | 四种action、calendar header及恢复 | pending | 尚未开发验收 |
| F-012 | PIT lifecycle物化 | 新旧证券完整观测历史和选股隔离 | pending | 尚未开发验收 |
| F-013 | factor rolling/aggregate writer | 月界对照与历史修订后效 | pending | 尚未开发验收 |
| F-014 | shared sector/context producer | PIT全池、停发、market总量定义 | pending | 尚未开发验收 |
| F-015 | private write/seal/deploy adapter | hardlink隔离、跨卷、失败恢复 | pending | 尚未开发验收 |
| F-016 | manifest/E/P/R/C contracts | 无环身份及三端同字节 | pending | 尚未开发验收 |
| F-017 | HMM-owned producer/登记适配 | 冻结模型、新B/C、无fit、完整日期 | pending | 尚未开发验收 |
| F-018 | QE创建与Composer reader | P10/P11真实binding和物化股票池 | pending | 尚未开发验收 |
| F-019 | 因子正式计算/cache入口 | 新identity隔离与旧任务复现 | pending | 尚未开发验收 |
| F-020 | Selection/Advisory/择时/统一回测adapter | 各真实新建任务入口读取验证 | pending | 尚未开发验收 |
| F-021 | dispatch/task冻结binding | 排队跨切换、retry不重新resolve | pending | 尚未开发验收 |
| F-022 | RD-Agent registry/identity | 已运行API识别新release，无逐月重启 | pending | 尚未开发验收 |
| F-023 | authorization/CAS/activation | 双切换、崩溃、读回、伪授权拒绝 | pending | 尚未开发验收 |
| F-024 | rollback与引用清单 | 并发安全回滚和旧任务引用保留 | pending | 尚未开发验收 |
| F-025 | coverage/consumer验证编排 | 完整scope非抽样假成功，typed例外 | pending | 尚未开发验收 |
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
