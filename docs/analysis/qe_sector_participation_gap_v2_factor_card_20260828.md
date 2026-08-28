# QE `m_sector_participation_gap_v2` 文件型因子卡

- 状态：`FILE_SMOKE_AND_UNIT_TEST_CANDIDATE`
- 日期：2026-08-28
- 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.10，P1-D / A-01
- 角色：`DIRECT_ALPHA`
- 数据面：冻结 `sector_data.h5` + `static_factors.parquet`
- 本阶段：源码与聚焦验证；不入库、不运行正式 QE、不形成策略包结论

## 1. 经济假设

当申万 L2 板块的大单及特大单净参与强于小单净参与，并且该差值的短期均值相对长期均值加速时，板块后续中期趋势更可能持续。该假设针对板块参与结构，不以当前成员快照回填历史。

## 2. 公式

逐 PIT 行业、逐交易日：

```text
large_net_amt = (large_buy + extra_large_buy)
              - (large_sell + extra_large_sell)
small_net_amt = small_buy - small_sell
large_ratio   = large_net_amt / amount
small_ratio   = small_net_amt / amount
gap_t         = CSRank_sector(large_ratio) - CSRank_sector(small_ratio)
factor_t      = mean_5(gap_t) - mean_20(gap_t)
```

方向在任何快筛前固定为：`factor_t` 越大，预测后续收益越高。

## 3. 文件与 PIT 契约

1. `sector_data.h5` 只提供冻结行业日聚合值；`static_factors.parquet.l2_code_id` 提供逐股票逐日 PIT 行业键。
2. 相同行业日重复到成员股票的行业值必须完全一致；不一致立即失败，不取均值掩盖数据错误。
3. 截面 rank 在唯一行业集合上计算，禁止按成员数量重复加权。
4. `amount <= 0`、任一金额字段缺失或无合法 L2 时为 missing，不填零。
5. 5/20 日 rolling 要求窗口内完整观测；未来数据只可用于后续 label/evaluation，不进入本因子。
6. 输出为单列 `float32` DataFrame，MultiIndex 精确为 `(datetime, instrument)`。

## 4. 实现范围

- 源码：`scripts/qe_alpha_candidates/sector_rotation/m_sector_participation_gap_v2.py`
- 测试：`backend/tests/quantevolver/test_sector_participation_gap_v2.py`
- 文件归属：候选源码位于非运行时 `scripts/**` 研究工具边界，不修改 `.gitignore` 或后端运行时代码。
- CLI 强制显式 `--data-dir` 与 `--output-path`，运行产物必须位于 repo/worktree 外。
- CLI 可用 `--start-date/--end-date` 做有界文件快筛；读取器按 HDF 行日期二分定位，并自动向前补足 19 个交易日，禁止因窗口截断改变 20 日 rolling 历史值。
- 不新增 `rdagent_assets/manual_factors` 或 `rdagent_assets/qe_factors` 集成副本；注册/I1 接入由未来独立任务完成。
- 不修改 factor catalog、composer、remote dispatch、数据集或数据库。

## 5. 当前验证合同

1. 行业信号按 PIT 成员一致映射；
2. 非正分母和缺失不填零；
3. 行业重复值不一致 fail loud；
4. L2 身份非法 fail loud；
5. 全量输入与按日期截断输入的历史结果完全一致；
6. 缺必需列 fail loud；
7. CLI 输出路径解析后必须位于 repo/worktree 外；
8. 日期窗口有界读取与全量计算在相同区间完全一致；反向日期窗口 fail loud；
9. 非有限金额或 L2 身份 fail loud，不进入 rank；
10. 语法、ruff、聚焦 pytest、`git diff --check` 通过。

## 6. 后续但不在本阶段

数据准备窗口完成正式 signoff 后：

1. 在正式 signoff 的文件快照上重新执行 repo 外 factor workspace 计算；
2. 运行 h20 full/out-sample/recent-6m/recent-3m 信号快筛；
3. 对规模、换手、波动和现有资金流腿做残差/相关性分析；
4. 只有文件/PIT/方向/coverage 证据完整时进入 matched CE3+candidate、三种子、单腿/blend/LOO；
5. catalog、指标和分类写入必须另获授权。

## 7. 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| A01-01 大单含特大单、小单独立、amount 分母 | factor constants/calculate_factor | 聚焦数值 fixture | IMPLEMENTED | 无 |
| A01-02 唯一行业截面 rank | sector-day consistency/groupby | 成员映射测试 | IMPLEMENTED | 无 |
| A01-03 PIT L2 映射 | `l2_code_id` daily mapping | 截断等价测试 | IMPLEMENTED | 真实文件待后续 |
| A01-04 缺失与非法输入 fail loud | column/type/consistency checks | negative tests | IMPLEMENTED | 无 |
| A01-05 单列 QE/Qlib 输出合同 | `FACTOR_NAME`, float32, MultiIndex | output-contract assertions | IMPLEMENTED | 无 |
| A01-06 零数据库数据面 | 仅 H5/Parquet imports | 静态审核 | IMPLEMENTED | 无 |
| A01-07 真实快筛与 matched QE | 后续独立任务 | 当前不执行 | DEFERRED | 等待数据 signoff |
| A01-08 大文件资源边界 | HDF 行二分、有界窗口、19 日预热、Parquet 日期过滤 | 有界读取等价测试 | IMPLEMENTED | 全历史仍按显式请求读取 |

## 8. 文件冒烟回执（非正式收益门禁）

- 只读输入：`factor_implementation_source_data_debug_20260630_candidate`；输出仅写入 WSL `/tmp/qe_factor_candidates/m_sector_participation_gap_v2/result.h5`。
- 结果：成功，31,875 行、327 个交易日、99 只股票，日期为 2018-08-28 至 2019-12-31；索引无重复、数值全部有限。
- 限制：该调试快照不覆盖蓝图要求的 2024H2 至 2025H2，因此本回执只证明真实 H5/Parquet 读取与输出合同，不提供 h20 IC、收益或晋级结论。
- 较新的 `F:\Dev\RD-Agent-state\factor_data` 覆盖到 2026-04-30，但固定 HDF 为 826 万行且尚非数据准备窗口本轮正式 signoff；等待期不以高内存全量加载替代正式筛选，也不把旧窗口结果伪装成当前证据。
- 对上述较新文件根执行 2024-07-01 至 2025-12-31 有界读取时，`static_factors.parquet` 被确认不含 `l2_code_id`，因此在 h20 计算前 fail closed；源码已将此检查前移到 HDF 大文件加载之前。正式筛选必须等待数据准备窗口交付并签署包含 PIT `l2_code_id` 的一致快照，禁止从数据库或当前成员表回填。
- 资源审查：guardrail 的 `ALGO-COMPLEXITY-001` 为 4 条 P2 非阻断提示；已通过列级底层读取、有界日期窗口和 schema-first 预检处理。对当前缺列文件根复验为 0.8 秒、最大 RSS 约 122MB、零 swap，并在 HDF 大表读取前退出。

## 9. DESIGN-COMPLIANCE-001

1. 未把 fixture、单测或源码写成 Alpha 已验证；真实快筛和 matched QE 明确延期。
2. 缺列、非法 L2、行业值不一致均显式失败；缺失/非正分母不填零。
3. 公式、方向、角色与父蓝图 A-01 保持一致，不修改策略、执行或数据业务语义。
4. 未新增收益门禁、人工审批、catalog、生产激活或数据库写入。
