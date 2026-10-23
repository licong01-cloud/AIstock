# 旧多 Alpha 包逐 seed 原资产恢复与 Advisory 接续

关联：BUG-2076 / [#6634](https://github.com/licong01-cloud/AIstock/issues/6634)；原 BUG-1804 / [#5742](https://github.com/licong01-cloud/AIstock/issues/5742)，原防复制修复 [#5745](https://github.com/licong01-cloud/AIstock/pull/5745)。本次是已冻结旧包的资产恢复入口修复，不重新运行原实验。

## 当前结论与缺失清单

对象 `pkg_ma_8ec5e389fa2c5e484a1ac7e9`，版本 `1.0.0`，冻结 SHA256 `f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016`。本轮开始与结束读回一致。生产包仍为 `SELECTION_ENABLED`，不表示逐 seed 资产完整。

2026-10-23 本轮读取的 backend runtime identity 为 `dba51ff094c402d1eea4fa655b10030e1f260d55`。Advisory program `advp_3126dd77f9774d94850f37ad012f640f` 仍绑定旧包，状态 `ENABLED`，最后成功复评交易日仍为 `2026-10-09`。本轮没有发起复评或修改程序。

| 核验对象 | LSTM 33 seed | FUND 5 seed |
|---|---|---|
| 原权重真实字节与原 prediction-store manifest | 33/33 通过，33 个不同 SHA256 | 5/5 通过，5 个不同 SHA256 |
| 冻结来源与 task/loop/experiment 坐标 | 33/33 一致 | 5/5 一致 |
| 原因子引用和顺序 | 每 seed 26 因子，与旧包一致 | 每 seed 7 因子，与旧包一致 |
| 原 `conf.yaml` | 33 份不可读 | 5/5 通过 |
| 原工作区模型代码 | 33 份 `model.py` 不可读；旧包主模型的一份冻结代码另已核验 | 5/5 所需 `qe_custom_loaders.py` 通过；不要求不存在的 `model.py` |
| 原权重配对的已拟合预处理 | 33 份未取得；不能重新拟合替代 | 5/5 原配对 `dataset` 通过 |
| schema 与真实模型加载 | 缺原配置/预处理，不能确认；旧包的 20 Alpha158 别名 schema 本身可读 | 5/5 独立 WSL 进程通过：LGB、7 动态因子、0 Alpha158、特征数差 0 |
| 完整 seed 资产闭包 | 0/33 | 5/5 |

旧包已登记的 37 份资产（33 因子代码、2 主模型权重、1 模型代码、1 schema）全部通过物理 SHA256 与长度核验。单份主 LSTM 代码的存在，不能证明另外 32 个 seed 的训练代码相同；也不能替代其原拟合预处理。

38 项可移植回执、原权重 SHA256、来源、每步状态及 DEV 回执见 [资产核验 JSON](evidence/strategy_package_seed_asset_recovery_20261023.json)。原始完整节点错误及逐项日志保留于 `F:/Dev/AIstock/tmp/handoff/strategy-package-seed-recovery-20261023/`；该临时目录不是生产资产库。

本轮通过正式 QE workspace API 对 `wsl2-5080` 与 `rdagent-node1` 的原坐标读取 LSTM `conf.yaml`、`model.py` 和 `mlruns-params`，返回缺失/404。集中 prediction-store 的对应 manifest 保有权重/预测/标签，但不含这些训练闭包文件。这是当前可访问来源的缺口，不等同于断言所有历史备份都不存在。

## 版本化恢复路径

入口：`scripts/recover_multi_alpha_seed_assets.py`；实现：`SeedAssetRecoveryService`。默认只读取生产来源并写独立本地 staging CAS；`--apply-dev` 只连接 `.env` 的 `TDX_DB_DEV_*` 指向的现有 DEV。脚本没有生产写入或激活选项，不创建数据库、不修改数据集、不训练、不控制服务。

1. 固定旧包 SHA、原 leg/seed 顺序与来源坐标，核对每 seed 原权重的来源 manifest、task/loop、SHA256 和长度。主 seed 还必须匹配旧包权重。拒绝借用别的 seed 的权重 URI。
2. 保留旧包因子 SHA 与顺序；读取原配置，核对 Alpha158 schema；按精确权重 SHA 恢复原已拟合预处理；冻结所需代码并运行既有自包含加载检查。缺失按 seed 返回明确失败，不以空值、复制或重新拟合补齐。
3. 全部 seed 通过后才构建新 `pkg_sr_<digest>` / 新版本 `DRAFT`。保留 `mean_by_trade_date_instrument`、rank 标准化、原 IC 权重政策、terminal weights、全部 406 条 per-window weights、源预测引用与历史策略上下文。旧包不就地更新。
4. 新版本记录旧包 ID/SHA、语义摘要、核验收据摘要，移除旧 admission 收据，不宣称历史收益因本次恢复重新得到验证。再次检查冻结过程中资产未改变，再执行父包自包含检查。
5. DEV 保存前重新核对旧包身份、候选及收据完整性、CAS 字节和版本冲突；保存后读回，重复同一版本必须幂等。任何 seed 缺失均不写新包。

示例（先使用新的独立输出目录）：

```powershell
python scripts/recover_multi_alpha_seed_assets.py `
  --package-id pkg_ma_8ec5e389fa2c5e484a1ac7e9 `
  --version 1.0.1-seed-assets `
  --source-root F:/Dev/AIstock `
  --output F:/Dev/AIstock/tmp/handoff/strategy-package-seed-recovery-20261023/recheck `
  --apply-dev
```

退出 0 表示完整候选通过；退出 2 表示已产出缺失收据但候选不完整。不要将“不完整”重标为恢复成功。补回原文件后可以重做资产核验，不需要提交训练或 QE 实验。

原重新晋升入口仍执行 `require_execution_algo_active`，`V25_1_SMALL_CAP` 仍返回 `V25_EXECUTION_ALGO_RETIRED`。恢复入口只修复历史资产闭包，不调用晋升、策略迁移、Selection enable 或执行准入。V25 退役约束没有移除；之后任何执行路径仍须通过既有检查。

## DEV 与验证状态

真实 38 seed 审计结果为 5 完整、33 阻断；以此计划调用 DEV 持久化返回 `seed_recovery_incomplete`。DEV 包数量前后均为 6，没有写入真实候选。

另在既有 `aistock_dev` 使用明确标注的合成治理 fixture 验证成功路径：原包、新 DRAFT、38 条模型权重与 2 条因子资产、幂等回读、原包不变均通过；整个事务回滚，包数量仍为 6。此 fixture 验证数据库和治理链路，不是生产 LSTM 加载证据。

该 DEV 当前缺少已发布的 `strategy_pkg_multi_alpha_combine_source_type_20260629.sql` 约束升级；测试在同一 DEV 事务内显式应用原 migration 并一起回滚。后续在 DEV 实际登记恢复包前，应显式补齐此既有 migration。生产旧包已支持该 source type；本修复不引入新 schema，生产 DDL gate 为 noop。

定向测试覆盖 38 seed 语义/顺序、旧身份不变、幂等、缺失不部分入库、复制 seed URI、原归档哈希变更、来源漂移、因子顺序漂移、重复 roster、候选/收据篡改、CAS 损坏、backfill 保持阻断，以及 V25 退役检查。广泛 `paper_v2_backend` 回归交 CI/Nightly；该计划不需要 backend/frontend 服务，不能用它重启用户服务。

## Advisory 接续与独立交付状态

| 状态 | 本轮结果 |
|---|---|
| 模块内恢复入口与缺失诊断 | 修复 PR 交付；没有把部分恢复称为成功 |
| 原资产核验 | 38 项有回执；FUND 5 完整，LSTM 33 阻断 |
| DEV 治理链路 | 合成成功路径通过并回滚；真实候选不入库 |
| 生产资产写入/新包登记 | 未执行，另列授权与回执 |
| Selection/Advisory 激活与 program 换绑 | 未执行 |
| API 源码生效 | 未合入/未重启；CLI 使用任务 worktree 新进程验证 |
| V25 执行资格 | 仍退役，不因资产恢复改变 |

下一步先按 JSON 的 33 个 task/loop 坐标，从原始留存或可信归档补回 `conf.yaml`、模型代码及与已确认权重 SHA 配对的原 `dataset`。不要复制主 seed、生成新配置冒充旧配置、用同结构模型重训或重新拟合预处理。若原训练闭包不可找回，则旧 LSTM leg 的等价恢复不能成立，旧父包继续保持阻断；新训练只能是另立研究版本，不能标记为本包恢复。

全部原资产验证通过、DEV 新版本验证通过，并取得生产资产写入/登记授权后，按新 ID/SHA 发布完整资产闭包。Advisory 窗口再按现有 Selection 准入与 program 版本流程接续新包，保留旧历史账本与失败原因，记录新 package-set hash。当前旧包不可仅靠清空错误、换标签、复用主 seed 或提前换绑恢复成功复评。

## DESIGN-COMPLIANCE-001 逐项审查

| 要求 | 实现/证据 | 结论 |
|---|---|---|
| 不简化交付 | 全 roster 核验、完整候选才持久化；真实缺口与 DEV fixture 分开 | 源码路径完整；旧包实际恢复未完成，缺口明确 |
| 不静默错误 | 每 seed/每阶段原因、38 项回执、失败退出码与 no-write | 通过 |
| 不改变业务语义 | 原身份保留，语义摘要覆盖 seed/IC 权重/策略上下文；V25 测试 | 通过 |
| 不私增审批 | 恢复审计和 DEV 无新增人工步骤；生产写入/激活沿用本次用户边界 | 通过 |
