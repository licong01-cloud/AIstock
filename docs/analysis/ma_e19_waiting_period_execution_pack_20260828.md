# MA-E19 数据等待期执行包：九臂信号分析、D2/D3 预注册与三臂恢复卡

- 文档类型：QE-only `docs-fast-new` 执行包
- 日期：2026-08-30
- 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.11
- 执行方案：`docs/analysis/ma_e19_p0_triad_and_alpha_execution_plan_20260824.md` v1.2
- 当前任务：`qe_20260825_031740_2457`
- 当前状态：`MA_E19R2_PARTIAL_9_OF_12_WAITING_DATASET_SIGNOFF_TOOLING_ACTIVE`
- 唯一目标：在不提交实验、不修改数据集和不访问数据库数据面的前提下，把数据就绪后的下一步压缩为可直接执行的最小工作包。

## 1. 边界

本执行包只做：

1. 对 MA-E19R2 Loop1～9 的现有 API/制品指标做只读综合分析；
2. 预注册 WP-D2 四格 Sector Oracle 和 WP-D3 Benchmark-relative/Brinson；
3. 固定数据就绪后的 2026H1 三臂恢复任务卡；
4. 固定旧九臂与新数据身份的等价性判定。

本执行包不做：

- 不提交、恢复、暂停或取消任何 QE task；
- 不构建、发布、分发或激活数据集；
- 不读取数据库作为训练、预测、因子或回测输入；
- 不控制 AIstock、WSL、node1、RD-Agent 或 worker 进程；
- 不执行 DDL/DML、依赖安装、因子入库、Archive、UI 或历史补账；
- 不把九臂部分证据写成 D1 完成或 refit 胜负结论。

## 2. 当前事实冻结

实时回读时间为 2026-08-30。任务 `qe_20260825_031740_2457` 状态为 `failed`，Loop1～9 `completed`，Loop10～12 `failed`；没有发现更新的 completed D1 task。BUG-1191 / Issue #3793 已完成 source/runtime/close-sync 并关闭，但该事实不等于数据 candidate 已签核或激活。

回测数据集 durable workflow 的最新只读状态：profile `qe_hmm_full_v2`、submission `dss_cdc7ee95f703cb1cbd8a4faf9e8cee40`、`submission_state=BLOCKED_CONTRACT`、`run_id/run_state/outcome=null`、worker `healthy/IDLE`、`production_activation=not_requested`。因此当前仍禁止提交依赖新数据的正式 QE task；worker 空闲不能替代 terminal receipt、catalog readback、candidate signoff 或 activation。

固定合同：LGBM、CE3、h20、seed 123、21 交易日 purge、Top50/n_drop1、`score_weighted_topk_v2`、`TWAP/1min`、node1 串行度 1、零数据库数据面。

| Loop | vintage | refit | IC | RankIC | ICIR | RankICIR | Top20 return | Top20 hit | CAGR | Sharpe | MDD | avg turnover |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2024H2 | fixed_anchor | 0.042189 | 0.061977 | 0.569173 | 1.081619 | 0.038877 | 0.519937 | 0.668782 | 1.6428 | -0.132306 | 0.031477 |
| 2 | 2024H2 | expanding | 0.042189 | 0.061977 | 0.569173 | 1.081619 | 0.038877 | 0.519937 | 0.698250 | 1.6924 | -0.132409 | 0.031524 |
| 3 | 2024H2 | rolling | 0.038772 | 0.063763 | 0.492898 | 0.877955 | 0.044753 | 0.543768 | 0.706443 | 1.7715 | -0.120752 | 0.030530 |
| 4 | 2025H1 | fixed | 0.056825 | 0.082008 | 0.822282 | 1.601191 | 0.042075 | 0.608749 | 0.787443 | 2.4090 | -0.164632 | 0.032204 |
| 5 | 2025H1 | expanding | 0.055204 | 0.091033 | 0.681022 | 1.214013 | 0.043123 | 0.630207 | 0.633286 | 2.2649 | -0.136446 | 0.030909 |
| 6 | 2025H1 | rolling | 0.055822 | 0.089877 | 0.634987 | 0.895959 | 0.028121 | 0.599955 | 0.507454 | 2.1114 | -0.124145 | 0.030595 |
| 7 | 2025H2 | fixed | 0.040600 | 0.075623 | 0.982394 | 1.953336 | 0.040987 | 0.554560 | 0.539578 | 2.8203 | -0.071074 | 0.031423 |
| 8 | 2025H2 | expanding | 0.038181 | 0.075221 | 0.924208 | 1.787982 | 0.037328 | 0.544465 | 0.813042 | 4.0588 | -0.057811 | 0.031819 |
| 9 | 2025H2 | rolling | 0.030187 | 0.070244 | 0.734957 | 1.364215 | 0.039700 | 0.581461 | 0.675109 | 3.5437 | -0.053348 | 0.031391 |

每臂的 `execution_manifest_sha256` 均已回读且互不相同，这是配置/窗口身份差异的正常结果，不是同输入重放证明。未来 determinism 证明必须在相同 arm 输入下另行重放。

## 3. 九臂综合分析

### 3.1 vintage 内相对 fixed 的增量

| vintage | refit | ΔIC | ΔRankIC | ΔCAGR | ΔSharpe | ΔMDD（正值为改善） |
|---|---|---:|---:|---:|---:|---:|
| 2024H2 | expanding | 0.000000 | 0.000000 | +0.029468 | +0.0496 | -0.000103 |
| 2024H2 | rolling | -0.003417 | +0.001786 | +0.037661 | +0.1287 | +0.011554 |
| 2025H1 | expanding | -0.001621 | +0.009025 | -0.154157 | -0.1441 | +0.028186 |
| 2025H1 | rolling | -0.001003 | +0.007869 | -0.279989 | -0.2976 | +0.040487 |
| 2025H2 | expanding | -0.002419 | -0.000402 | +0.273464 | +1.2385 | +0.013263 |
| 2025H2 | rolling | -0.010413 | -0.005379 | +0.135531 | +0.7234 | +0.017726 |

### 3.2 可以确认的结论

1. expanding/rolling 没有形成跨三个已完成 vintage 的同方向 IC/RankIC 改善。
2. 2025H1 的 expanding/rolling 明显降低 CAGR/Sharpe，但改善 MDD；2025H2 则明显提高 CAGR/Sharpe 并改善 MDD，portfolio 增量存在显著时变。
3. 2024H2 fixed_anchor 与 expanding 的 signal 指标完全相同，而 portfolio 指标不同；该差异不应被解释为训练窗口增量，应继续作为确定性与执行转换诊断。
4. rolling 在 2025H2 的 RankIC 低于 fixed，但风险调整收益更高，说明“全截面排序质量”和“组合转换”不能合并判断。
5. 当前数据最多支持 `PARTIAL_SIGNAL_DOES_NOT_YET_SUPPORT_STABLE_REFIT_RECOVERY`；不能启动 DoubleAdapt/Proceed，也不能宣布 refit 无效。

### 3.3 当前不可确认的结论

- 2026H1 是否恢复近期弱化；
- expanding/rolling 是否达到至少 3/4 vintage 同方向改善；
- 模型年龄衰减曲线是否支持固定 cadence；
- sector selection、within-sector ranking、beta、allocation、selection 或成本中哪一项是主瓶颈；
- 新数据身份下旧九臂是否可无条件复用。

## 4. 数据就绪门禁

数据准备窗口必须提供一个可引用的 signoff，至少包含：

1. immutable candidate/dataset identity、cutoff、manifest SHA；
2. WSL 与 node1 的 `instruments/all.txt`、day/1min calendars、1min feature root 哈希；
3. 2026H1 全投资域的 minute instrument coverage 与缺失分类；
4. PIT universe、停牌、涨跌停、pre-close、factor 和交易单位合同；
5. candidate signoff、node1 distribution、active activation 三个独立状态；
6. QE subprocess 零数据库数据面证明。

截至 2026-08-30，上述 signoff 尚未取得。BUG-1191 的 verified/closed、worker `healthy/IDLE` 和 `BLOCKED_CONTRACT` submission 均不能满足本节门禁；本执行包不创建新的 dataset intent，不构建、发布、分发或激活数据。

仅观察到某个 active `all.txt` 的结束日期变化，不等于上述门禁完成。

## 5. 旧九臂等价性判定

数据 identity 改变后，优先执行文件级/语义级等价性检查，不直接重复训练：

1. 从九个 arm 保存的 dataset/universe receipt 回读旧 identity；
2. 对每个 test window 比较旧/新 `all.txt` 的逐股票有效区间交集；
3. 比较 daily/minute calendar、实际可交易股票日集合和 reference-factor observation panel；
4. 比较 prediction 输入行 identity，而不是只比较文件总行数；
5. 输出逐 arm `SEMANTIC_EQUIVALENT` 或 `RERUN_REQUIRED`。

判定：

- 九臂全部 `SEMANTIC_EQUIVALENT`：只运行新的 2026H1 三臂；
- 任一臂 `RERUN_REQUIRED` 或证据缺失：以新 dataset identity 完整重跑 12 臂；
- 禁止把“窗口早于旧 all.txt 截止日”单独作为等价证明。

### 5.1 文件型审计工具合同

实现入口固定为 `scripts/qe_alpha_candidates/sector_rotation/ma_e19_semantic_equivalence_audit.py`，输入为两个 `qe_ma_e19_arm_set_manifest_v1` JSON 文件，输出为一个 `qe_ma_e19_semantic_equivalence_receipt_v1` JSON 文件。工具只使用 Python 标准库，不导入 backend、不访问 API/数据库、不启动任务或进程。

每个 manifest 必须：

1. 自身 `manifest_sha256` 与去除该字段后的 canonical JSON 匹配；
2. 精确包含 2024H2、2025H1、2025H2 × fixed/expanding/rolling 九个 arm；2024H2 的 `fixed_anchor` 规范化为 `fixed`，不得出现重复规范键；
3. 每臂包含 train/valid/test 窗口和 `dataset/calendar/universe/tradability/factor/label/prediction/order/strategy` 九个组件；
4. 每个组件包含非空 `identity`、真实来源 `source_sha256` 和窗口/配置语义 `semantic_sha256`；来源 SHA 可因 immutable release 改变，但 `semantic_sha256` 必须逐组件相等才能复用；
5. candidate manifest 额外包含非空且为 64 位 SHA256 的 `candidate_signoff_sha256`、`catalog_readback_sha256`、`node_distribution_sha256`、`active_activation_sha256`。缺任一项为 `NOT_COMPUTABLE`，不得把 worker healthy/IDLE 或 source/runtime receipt 代替数据证据。

输出规则：

- 输入 schema、manifest hash、九臂集合、窗口、组件或 release evidence 缺失/非法：整体 `NOT_COMPUTABLE`，返回稳定 `reason_code`，退出码 2；
- 输入完整且任一臂窗口或组件 `semantic_sha256` 不同：该臂及整体 `RERUN_REQUIRED`，退出码 1；
- 九臂窗口与九组件语义全部相同：整体 `SEMANTIC_EQUIVALENT`，退出码 0；
- `identity/source_sha256` 变化只进入 provenance differences，不被静默忽略，也不在语义相同的情况下伪造重跑要求；
- receipt 必须按规范键排序、包含两侧 manifest SHA、逐臂结果、计数、稳定 reason codes 和自校验 `receipt_sha256`；同输入重复运行必须逐字节相同。输出写入显式路径并原子替换，不扫描目录、不覆盖输入文件、不创建数据或实验制品。

## 6. MA-E19R 2026H1 三臂恢复任务卡（仅预注册，不提交）

任务身份在实际提交前重新查询并生成；禁止恢复旧 task。

| arm | refit | train | valid | test/backtest | rolling_train_days |
|---|---|---|---|---|---:|
| R-10 | fixed | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2026-01-05～2026-06-29 | 0 |
| R-11 | expanding | 2018-08-01～2025-05-08 | 2025-06-10～2025-12-02 | 2026-01-05～2026-06-29 | 0 |
| R-12 | rolling | 2022-03-23～2025-05-08 | 2025-06-10～2025-12-02 | 2026-01-05～2026-06-29 | 756 |

固定项：LGBM、CE3、h20、seed 123、120D cadence、21 交易日 purge、相同 observation panel、Top50/n_drop1、`TWAP/1min`、TailTWAPWithLimitStrategy、相同费用/风险/停牌合同、node1 并行度 1。

提交前条件：数据门禁完成、九臂等价性结论完成、node1 空闲、无 task id 冲突、BUG-1133/1175/1178 运行身份可回读。

输出：signal/portfolio/execution 三层 receipt、prediction SHA、order/fill identity、tie-break version、deterministic replay、Top20/Top50/tail recall、turnover/cost。

## 7. WP-D2 四格 Sector Oracle 预注册

### 7.1 输入

- D1R 最终冻结 prediction、label、holdings 与 dataset identity；
- PIT 申万 L2 taxonomy 与逐日成员 hash；
- 同一 observation panel、benchmark、TWAP/1min 执行合同；
- oracle 只在评价期构造并永久标记 `QE_ONLY_FUTURE_INFORMATION_CEILING`。

### 7.2 四格

| cell | sector | stock | 解释 |
|---|---|---|---|
| D2-RR | reality | reality | 可部署基线 |
| D2-OR | oracle | reality | sector selection ceiling，不可部署 |
| D2-RO | reality | oracle | within-sector ranking ceiling，不可部署 |
| D2-OO | oracle | oracle | 总体上界，不可部署 |

每格同时运行 hard Top-M 与 soft continuous gating，但不在评价期调阈值。所有格必须使用相同 test dates、成本、投资域和执行合同。

### 7.3 输出与触发

- sector Recall/NDCG、右尾板块捕获；
- within-sector RankIC、股票 tail recall；
- TWAP portfolio、成本、换手、板块主动暴露和 bootstrap/HAC 区间；
- sector ceiling 高：触发两层 soft gating 与 cross-sector lead-lag；
- stock ceiling 高：触发 participation/leadership/cohesion/tail target；
- 两者都低：优先组合转换与执行诊断；
- 两者都高：触发两层联合路线。

完成标识：`P0_D2_END_STATUS=FOUR_CELL_COMPUTABLE`。

## 8. WP-D3 Benchmark-relative / Brinson 预注册

### 8.1 输入

- 与 D1R/D2 完全相同的 holdings、日期、成本和 TWAP portfolio；
- 冻结 benchmark identity；
- 冻结可投资股票池等权对照；
- PIT taxonomy、逐日 benchmark/portfolio 权重及 hash。

### 8.2 输出

- absolute return、active return、beta、tracking error、IR；
- Brinson allocation、selection、interaction；
- 按 vintage/refit 统一分解，不允许改变样本总体；
- 缺 benchmark、逐日权重或 taxonomy 时为 `NOT_COMPUTABLE`，不以当前成分回填。

结果触发：allocation 主导进入两层板块/P1-F；selection 主导进入股票层正交 Alpha/right-tail；beta 主导下调历史绝对 CAGR 的 Alpha 解释；成本主导进入动态退出/换手预算。

完成标识：`P0_D3_END_STATUS=ABSOLUTE_ACTIVE_BRINSON_RECONCILED`。

## 9. 等待期可继续的 Alpha 工作

只允许日频文件型候选的源码、fixture、PIT 截断和单元测试，不做 catalog/数据库写入或正式 QE：

1. `sector_participation_gap_v2`：优先实现；大单/小单参与差的 5D-20D 加速，主角色 `DIRECT_ALPHA`；
2. `dynamic_residual_flow_relation_v1`：只准备关系构造与防泄漏测试，正式角色先为 `RELATION_PRIOR`；
3. tail/trend-persistence：只稳定 label/weight/evaluation 合同，不在当前数据身份上训练；
4. leadership/cohesion：先作为 `CONDITIONING_STATE`，不冒充直接 Alpha；
5. `pit_fundamental_diffusion_v1`：保持 `DEFERRED_PIT_INPUT_NOT_READY`。

### 9.1 `sector_participation_gap_v2` 当前执行回执

- 当前 `origin/main` 已包含文件型候选源码 `scripts/qe_alpha_candidates/sector_rotation/m_sector_participation_gap_v2.py`、因子卡和 13 项聚焦测试；未提交 catalog、数据库或正式 QE。
- 旧调试文件快照的 H5/Parquet 真实计算通过，但仅覆盖 2018-08-28 至 2019-12-31，因此不形成收益结论。
- 当前 `F:\Dev\RD-Agent-state\factor_data` 的行业 HDF 覆盖到 2026-04-30，但配套 `static_factors.parquet` 不含 PIT `l2_code_id`；2024H2～2025H2 h20 快筛在标签计算前 fail closed。
- 候选读取器已支持日期有界 HDF 切片、19 交易日 rolling 预热、Parquet 日期过滤和 repo 外输出，避免为快筛加载 826 万行全历史；缺 `l2_code_id` 的 schema 检查先于 HDF 大文件加载。
- 下一动作仍是等待数据准备窗口提供含 PIT `l2_code_id` 的一致快照 signoff；禁止数据库回填、当前成员快照回填或用旧调试窗口替代正式证据。

## 10. 恢复顺序与结束状态

1. 数据 signoff/activation 完成；
2. 九臂旧/新 identity 等价性判定；
3. 三臂或完整 12 臂新任务；
4. D1 synthesis；
5. D2；
6. D3；
7. 按结果触发一个最小 P1 canary；
8. 新 Alpha 先快筛，再 matched QE/blend/LOO。

当前结束状态：

`WAITING_PACK_STATUS=READY_WITHOUT_EXPERIMENT_SUBMISSION`

当前长任务状态：

`QE_LT8H_01_STATUS=IN_PROGRESS_DATASET_BLOCKED_TOOLING_ACTIVE`

该标识只表示等待期分析和预注册已准备，不表示数据、D1、D2、D3、新 Alpha 或策略包已经完成。

## 11. 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| W-01 九臂真实状态与指标 | §2～§3 | 8001 summary/full API readback | COMPLETE | 2026H1 三臂缺失 |
| W-02 不提前裁决 D1 | §3 | 3 vintage delta 与停止条件 | COMPLETE | 等待 2026H1 |
| W-03 数据 signoff 与 active 分离 | §4 | WSL 只读观察不冒充双节点签核 | COMPLETE | 正式 signoff 待数据窗口 |
| W-04 旧九臂等价性 | §5～§5.1 | 标准库 CLI、21 项聚焦测试、三态/退出码、自校验 receipt、未知字段与大小写 SHA fail closed、精确 non-runtime/ownership 登记 | SOURCE_IMPLEMENTED_TESTED | 等待新 manifest 才能形成最终裁决；未提交实验 |
| W-05 三臂任务卡 | §6 | 窗口/固定项/提交前条件 | DESIGN_READY | 不提交 task |
| W-06 D2 四格 | §7 | 四格、oracle 标记、输出与触发 | TOOLING_ACTIVE_EXPERIMENT_PENDING | 等待 D1R 才能形成真实实验结论 |
| W-07 D3 Brinson | §8 | 输入一致性、分解和缺失语义 | TOOLING_ACTIVE_EXPERIMENT_PENDING | 等待 D1R 才能形成真实归因结论 |
| W-08 日频 Alpha 边界 | §9～§9.1 | 文件型、无 DB、无正式 QE；A-01 源码/13 tests/旧文件冒烟 | SOURCE_READY_DATA_BLOCKED | 等待含 PIT `l2_code_id` 的正式 signoff |
| W-09 动作边界 | 全文 | process/DB/dataset/experiment 均 noop | COMPLETE | 无 |
