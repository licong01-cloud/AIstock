---
name: update-backtest-dataset
description: Operate the direct, candidate-only AIstock monthly QE/HMM dataset update workflow. Use for monthly PIT refresh, database gap completion, Qlib daily/minute and H5/static/index/sector candidate rebuild, status, validation and signoff. Never overwrites or activates production without separate explicit authorization.
---

# AIstock QE/HMM 月度数据集直接更新

## 唯一目标

每月把唯一权威 PIT 股票池和所需市场数据更新到“上一个月最后一个已完成交易日”，生成新的独立 candidate，完成一次结构/抽样/QE-HMM smoke 后停止。

本 Skill 不再使用或恢复：

- source-freeze；
- 全历史源数据内容哈希；
- publish 前全历史 source recheck；
- source-drift waiting/re-attestation；
- revision ledger、fingerprint、Merkle root 或 checksum 形式的等价全量扫描；
- 资源准入、pressure ladder、重复真实 sample 或新增人工审批点。

## 当前 2026-08-31 更新合同

7 月分钟线在旧候选导出后完成过数据库补录，因此：

- 现有 2026-07-31 数据集只读保留；
- 新候选必须重新导出分钟组件并追加 8 月数据；
- 有完整补录影响清单时只重建受影响股票/日期；
- 没有完整清单时重建整个分钟组件；
- 其他组件只追加 8 月尾部或选择性重建真实失效分区；
- 不得因为分钟组件重建而全量重导所有无关组件。

## 每月固定顺序

1. `status`：确认没有活动的旧构建；只读查看当前候选状态。
2. PIT：更新/readback `aistock_equity_pit_canonical_v2` 到目标 cutoff。
3. 数据：确认数据库补数和目标月数据已可用；分钟缺口优先 TDX，其次 Tushare。
4. 行业：复用或生成同 cutoff 的 industry/P3A full authority。
5. 计划：基于 cutoff、PIT、补录影响范围和旧候选组件选择 `REUSE / INCREMENTAL / SELECTIVE_REBUILD / COMPONENT_REBUILD`。
6. 构建：只写新的 repo-external candidate 目录，不覆盖旧候选或 production。
7. 验收：一次全量结构检查、分层数值抽样和 QE/HMM producer smoke。
8. 停止：报告 candidate 路径和未解决真实缺口；不进入训练、消费者迁移或生产激活。

普通入口保持：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 events --run-id <run_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 receipt --run-id <run_id>
```

在 BUG-1322 direct-build 源码合入并完成 `worker-scheduler` runtime readback 前，不得重新提交 `monthly`；旧 runtime 仍会启动已停用的 source-freeze。

## 不可突破边界

- 始终 candidate-only；不覆盖、不原地追加现有 candidate，不切 production pointer/symlink。
- 月更读取数据库和 provider；数据库修复、DDL/DML、生产激活、node1、依赖安装和后端重启仍是独立动作。
- PIT 固定使用 `aistock_equity_pit_canonical_v2 / shsz_a_252td_st_delist_asof_v2`，保留252交易日IPO暖机和历史退市生命周期。
- moneyflow 固定股/元单位；static 固定121列和 `l2_code_id int16/-1`；指数固定12只；HMM benchmark 固定 `000300.SH`。
- `stk_limit` 缺失使用既有版本化A股规则计算器，不填零、不填 NaN、不直接标记不可交易。
- 分钟缺口固定 TDX 优先、Tushare 次级，只补真实缺键；无法补齐时报告精确股票/日期。
- 不保留八年全市场 DataFrame；SQL、写入和验证按股票/月流式分批。
- CPU、内存、commit headroom、swap、预测磁盘和其他模块负载只记录 telemetry，不参与任务 admission、等待、取消或终态判断。
- 只有 OS/DB/WSL/文件系统实际失败、必要数据缺失、PIT/唯一键/数值错误或候选写入失败可以结束 attempt。

## 验收

不执行全量数据内容哈希或全量逐值比较。必须确认：

- cutoff、PIT、股票生命周期、唯一键和分母闭合；
- daily/minute/H5/static/index/sector 文件与日期范围；
- 7 月补录和 8 月新增分钟数据的物理覆盖；
- ST、涨跌停、复权、QFQ、moneyflow、12指数和行业数据分层抽样；
- 股票池、PIT、Qlib instruments、H5/static一致；
- QE/HMM producer smoke PASS；
- production writes、pointer changes和旧候选覆盖为零。

详细步骤见 `references/monthly-workflow.md` 和 `docs/operations/qe_backtest_dataset_monthly_update_runbook.md`。
