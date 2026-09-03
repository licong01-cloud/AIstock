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

本轮已生成一个失败但可恢复的 canonical v2 candidate，其中 daily/minute/index 已 PASS。不得为了重新证明流程
而重导这三个组件；只在新目录重建 BUG-1336 改变合同的 factor/static/sector。完成首个 v2 candidate 后再评估
按月尾部追加，不得为增量复用重新引入冻结、哈希或复杂 lineage。

## 每月固定顺序

1. `status`：确认没有活动的旧构建；只读查看当前候选状态。
2. PIT：更新/readback `aistock_equity_pit_canonical_v2` 到目标 cutoff。
3. 数据：确认数据库补数和目标月数据已可用；分钟缺口优先 TDX，其次 Tushare。
4. 行业：复用或生成同 cutoff 的股票行业分类 PIT candidate。月更 sector 字段按分类 PIT 映射申万 L2
   published 日线；申万指数成员进出 authority 仅供成分研究，不是股票 sector 字段或月更发布前置。
5. 计划：基于 cutoff、PIT、补录影响范围和旧候选组件选择 `REUSE / INCREMENTAL / SELECTIVE_REBUILD / COMPONENT_REBUILD`。
6. 构建：只写新的 repo-external candidate 目录，不覆盖旧候选或 production。
7. 验收：一次全量结构检查、分层数值抽样和 QE/HMM producer smoke。
8. 停止：报告 candidate 路径和未解决真实缺口；不进入训练、消费者迁移或生产激活。

普通入口保持：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

BUG-1322 已永久禁用旧 v2 source-freeze 提交路径。BUG-1323 合入后，上述 `monthly --candidate-only` 命令在当前进程中直接顺序执行 daily、minute、factor/static/sector 和 12-index 四个组件，状态写入新 candidate 自身的 `direct_monthly_state.json`；它不依赖 backend 或 worker-scheduler 重启。

BUG-1336 起，v2 的 `monthly/status` 不再打开旧 control store 或加载旧 profile 的 resource/source-freeze
合同，也不要求更早 validated baseline。恢复失败候选时只根据小型 component metadata 重新打开合同已改变的
组件；2026-08-31 现有候选因此复用已 PASS 的 daily/minute/index，只重建新的
`factor_h5_static_candidate_v2`。

同一天对同一 cutoff 重复执行会读取该状态，只跳过已经 `PASS` 的组件。没有状态文件的非空目录默认拒绝采用；仅允许采用由本轮已授权直接分钟导出产生、且只含 `components/logs/reports/work` 标准子目录的精确候选路径。

## 不可突破边界

- 始终 candidate-only；不覆盖、不原地追加现有 candidate，不切 production pointer/symlink。
- 月更读取数据库和 provider；数据库修复、DDL/DML、生产激活、node1、依赖安装和后端重启仍是独立动作。
- PIT 固定使用 `aistock_equity_pit_canonical_v2 / shsz_a_252td_st_delist_asof_v2`，保留252交易日IPO暖机和历史退市生命周期。
- moneyflow 固定股/元单位；static 固定121列和 `l2_code_id int16/-1`；指数固定12只；HMM benchmark 固定 `000300.SH`。
- sector published 日线与 sector moneyflow 分别生成：资金流缺失不得清空同日已有的 `sw2_pct_change/pe/pb`；
  股票缺少官方行业指数成员进出记录不得阻断分类 PIT 到 published L2 指数的投影。
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
- QE/HMM producer contract smoke PASS：验证 Qlib/H5/index 可读取且 sector 代表字段非空；不得用 85% 覆盖率、
  IC、信号日期数或回测收益作为数据发布门禁。
- production writes、pointer changes和旧候选覆盖为零。

详细步骤见 `references/monthly-workflow.md` 和 `docs/operations/qe_backtest_dataset_monthly_update_runbook.md`。
