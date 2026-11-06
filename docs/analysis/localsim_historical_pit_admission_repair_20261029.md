# LocalSIM 历史 PIT 准入失败的终态修复

2026-10-29，BUG-2133 / Issue #6869。补齐 BUG-2103 未覆盖的历史日边界；不修改公共 PIT、选股、择时、账户本金、成交、费用或账本。

## 根因与最终行为

LSTM、TCN 的 2026-10-26 live 日任务冻结租约止于 10-23，在交易前以 `CANONICAL_PIT_RUNTIME_INVALID` 失败。后续日已按逐日新发布正常运行，但历史恢复扫描对没有经济 outbox 的旧失败反复 claim/clear，既无法恢复该日，也没有正式终态。

只对当前调度器扫描到的已过去业务日，沿用既有零副作用判定：状态为可重试的 canonical PIT 交易前失败、无选股/计划、明确未调用 broker、提交数为整数零、无订单/成交/经济持久化证据。再核对原 binding/release/package/manifest 哈希、冻结 lease 与选股投影；只有原租约确实不覆盖失败日才允许终态。

符合条件的日任务保留原失败记录及原发布，转为 `FAILED_TERMINAL`，附 `LOCALSIM_HISTORICAL_PIT_ADMISSION_MISSED` 审计。它表示该日确实没有完成交易，不能伪装成成功或收益完整。后续扫描为幂等 no-op，不再无效重试；不冻结新历史 lease、不产生后继历史订单、不追加成交、资金或持仓。

冻结 source plan、非 canonical 发布、当日/未来日、显式历史调度、有计划/评分/经济证据和未知副作用均保持原路径。正常当日 PIT 续接继续使用 BUG-2103 的公共 freeze 与不可变发布。MiniQMT 及退役算法不在修改范围。

## 验收和审核

| 条款 | 实现 | 证据 | 状态 |
|---|---|---|---|
| 原失败可复现 | 历史失败 sweep | `pit-before.xml`：原 fix-point 失败，返回空结果且旧状态未闭合 | 通过 |
| 明确失败、无历史补单 | `_terminalize_missed_localsim_pit_admission` | `pit-after.xml`：同一节点通过；发布/绑定/计划数量不变，原失败保留 | 通过 |
| 幂等与安全边界 | 终态 carrier、原严格副作用检查 | `pit-matrix.xml`：26 项通过，含 current-day、frozen plan、未知副作用与哈希漂移 | 通过 |
| 当前日与旧 TWAP 不回归 | 原 daily PIT 用例 | 同一矩阵保留原生及迁移政策、禁用续接、原 TWAP 与发布中断恢复 | 通过 |
| 真实历史记录 | 独立 Python + 生产只读连接 + 内存 update capture | 见外部 `pit-fresh-readonly-r1.json`，不执行生产 DML | 单独记录 |
| L0、CI、合入、运行加载 | canonical BUG 流水线 | 分别读取正式收据；通过直接测试不代表后台已经加载 | 分别确认 |

证据根目录：`X:/AIstock_temp/localsim-reliability-followup-20261029`。

审核一：验证终态仍为失败，未抹去漏交易日；审核二：验证完整冻结身份和零副作用条件，没有以延长租约或补做交易修复历史；审核三：复核 current-day、source plan 和 unknown-effect 隔离、幂等扫描与 CI 范围。均为本窗口自审，不冒称独立审查。

DESIGN-COMPLIANCE-001：完整实现历史无效重试边界；不吞错或伪造成功；不改变交易/账本/PIT 语义；不新增审批或行情补数要求。

## 合入与用户重启

该修改属于 `backend-main` 调度器，必须由用户重启加载。合入、运行加载、历史终态读回是独立状态。重启后先核对 runtime identity，再确认 10-26 两个原日任务保留原 `pre_run_failure`、出现上述终态审计且未新增订单/成交，并检查当日账户和调度继续正常。不能只用健康接口 200 代替业务验收。

本修复不修饰此前策略收益，也不证明人工操作模式经济有效。本轮不执行生产数据库写入、数据集修改、生产回放或服务控制。
