# PT-NEXT-025：冻结择时模型的市值分层与全市场迁移实验详细设计

> 版本：v1.1；日期：2026-09-20；Feature tier：F1；状态：`IMPLEMENTED_RESEARCH_COMPLETE_INCONCLUSIVE`。
> 本轮由用户明确要求启动。它回答“取消 50～500 亿元市值限制，或改用大于 500 亿元股票后，既有择时结论是否改善”，不重新训练模型、不搜索新阈值。
> 主蓝图：`position_timing_advice_f2_redesign_20260903.md`；父研究：`position_timing_causal_oracle_minute_research_f1_20260919.md`。唯一开发权威为 `docs/standards/aistock_development_standard_v1.5_20260523.md`。

## 1. Background / 目标与可证伪问题（F-001）

终极目标仍是让特定自选股或持仓股的独立现金账户，在真实可执行约束和成本后，获得高于同股长期持有的终值并降低最大回撤。PT-NEXT-024 在 T−1 市值 50～500 亿元的 U0 人口中，冻结 GBDT 相对同股买入持有（BH）得到约 +52.5 bps 组合收益点估计和约 +1.397 个百分点最大回撤改善，但两个校正区间均跨零，结论为 `INCONCLUSIVE`，不得称为超额收益。

本轮只检验一个机制问题：该点估计是否受 U0 市值筛选驱动，冻结模型能否迁移到大市值和不设市值上限的 PIT 人口。结果允许为正、负或不可分辨；阴性结果同样完成实验，不能以继续搜索阈值替代。

PT-NEXT-024 原计划提出的“恢复时机/继续持有价值”新目标顺延为 PT-NEXT-026，不取消。本轮是用户在看到市值筛选疑问后作出的显式优先级调整。

## 2. Scope / Non-goals / 隔离边界（F-002）

实现仅限 `backend/services/position_timing/`、其定向测试、本设计与主蓝图进度更新；只写 timing-owned 新研究 namespace。只读 R8 candidate 和 PT-NEXT-024 父 bundle，不修改数据、数据库、profile、registry/current、在线卡片或其他模块。

不修改 QE、HMM、Selection、Advisory、Paper、MiniQMT、local_data；不新增数据源、服务、路由、调度器、worker 平台或运行时模型；不启动、停止或重启服务。研究在 WSL 中使用本地 8 个纯计算进程；数据库、外部市场网络和运行时动作均为 false。

本轮不重新训练 Ridge/GBDT，不新增模型，不调阈值，不重复分钟代理、Oracle 或全训练回放。执行视图固定为日频收盘 `DAILY_CLOSE`（E0），因为唯一自变量是股票池；PT-NEXT-024 已单独完成分钟执行敏感性，重复它不会回答本轮问题。

## 3. Frozen Inputs / 不可变输入（F-003）

| 输入 | 冻结身份 |
| --- | --- |
| R8 WSL candidate | `/mnt/wsl/aistock-qe-data-v1/releases/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate` |
| candidate manifest 文件 SHA256 | `07db01d8c24108ebfe2b85bc42f18f33c77fb0e09451999d7520faa08ebe1b18` |
| candidate canonical SHA256 | `6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283` |
| PT-NEXT-024 父 bundle id | `dfe8a85496a96f35b03d5a380f001645a999878f70461715adc73a9cde9854e6` |
| 父 manifest canonical SHA256 | `2ad1f15097f617473ea0f2b620e30922913f76ba48c98ea445ea661854bf2ec0` |
| Ridge model SHA256 | `aac2246286a863e02ecc985236c1ff6d52e22bf99277c996671cb5543ea4c450` |
| GBDT model SHA256 | `52b27c56494b4e72add859dbfd1555a72531aa96586ad2a8e5001280a9c2503a` |

适配器必须校验父 manifest 全部文件、父 request/candidate、模型规格与模型哈希；不允许从当前 active profile 推断父产物。prepare 在读取新收益前固定代码、环境、candidate、父 bundle、日线数据和 PIT sidecar 身份。任何漂移 fail closed。

## 4. Population / 四个首次入池人口（F-004）

所有人口均在公共测试窗口内按当时可知信息登记：T−1 `db_total_mv`、当日 `pit_active`、共同技术特征 `ready`。首次满足后账户固定保留到统一终点，不因之后市值、ST 或退市状态变化而删除；交易限制仍逐日生效。每股独立初始现金 1,000 万元，无杠杆、无追加资金、无跨股资金调拨。

| pool_id | 首次入池条件 | 用途 |
| --- | --- | --- |
| `U0_BRIDGE_50_500B` | 50 亿 ≤ T−1 总市值 ≤ 500 亿 | 精确复现父研究 E0，控制实现漂移，不计新假设 |
| `LARGE_GT_500B` | T−1 总市值 > 500 亿 | 正式迁移假设 |
| `SMALL_LT_50B` | T−1 总市值 < 50 亿 | 解释全市场差异的诊断，不计正式假设 |
| `ALL_PIT` | PIT 有效且技术特征 ready，不设市值条件 | 正式迁移假设；市值缺失者仍可进入并单列 size unknown |

市值单位沿用 R8 `daily_basic.h5::db_total_mv` 的万元；50 亿和 500 亿边界分别为 500,000 和 5,000,000 万元。三个市值分层要求有限值，缺失不得填充；`ALL_PIT` 不因市值缺失被删除。U0 对任何从首次测试决策到首次入池间出现的市值未知继续沿用父研究的 `ENROLLMENT_UNKNOWN` 语义，以保证桥接一致。

## 5. Strategy / Comparators / Execution（F-005）

加载父 `models.json`，原样运行 `BH100`、`CYCLE5_UNFILTERED_V1`、`CYCLE5_RIDGE_V1`、`CYCLE5_GBDT_V1`。正式比较只使用冻结 GBDT 与同股同起点 BH；其余策略均为机制诊断，不参与选择。模型只判断固定 20% 战术减仓是否执行；成功减仓后第 5 个后续全局交易日恢复，参数、14 项特征顺序、阈值 0 bps、费用、手数、T+1、停牌和方向性涨跌停处理全部不变。

时间窗口保持 PT-NEXT-024：训练模型的历史身份不变，公共回放首次执行日 2024-07-01、最后普通决策日 2026-08-27、最后普通执行日 2026-08-28、统一终点 2026-08-31。执行只用 E0 的 T+1 日收盘代理；它不证明真实收盘集合竞价成交，也不模拟市场冲击。

## 6. Estimands / Statistics / Claims（F-006）

正式 family 预注册为 2 个股票池 × 2 个端点，共 4 个端点：

1. `ALL_PIT`：GBDT − BH 的成本后组合终值差，以及组合最大回撤改善；
2. `LARGE_GT_500B`：同样两个端点。

组合路径为各独立账户 NAV 等额合成，所有账户从各自首次入池日起进入相应事后 cohort；报告逐股分布，不能只看大股票数量主导的总现金差。使用与父研究一致的 25 交易日移动块 bootstrap、5,000 次、固定 seed；名义区间 alpha=0.05，family-wise 区间使用 Bonferroni alpha=0.05/4。端点分类分别为 `SUPPORTED`（校正后下界 > 0）、`NEGATIVE`（校正后上界 < 0）或 `INCONCLUSIVE`；两个端点都支持才可记 `JOINT_SUPPORTED_EXPLORATORY`，仍不得自动上线。

U0、SMALL、Ridge、无过滤策略、年度/市场状态和逐股切片全部标为 diagnostic-only，不得从这些结果反向挑股票池或模型。父研究已观察事实计入研究谱系，但不把旧八端点和本轮四端点伪装成同一个未观察 family；receipt 同时披露历史试验，不声称本轮 test 是首次未见。

## 7. Outputs / Comparison Archive（F-007）

新 namespace 为 `research/causal_timing_universe_transport_v1`。至少封存：request、source audit、父模型引用、trial spec、逐池 enrollment audit、逐股汇总、逐池日路径、成交、比较报告、容量诊断、receipt 和 manifest。所有文件由 manifest 逐项哈希；exact retry 必须返回同一身份或 `ALREADY_MATERIALIZED`，不得覆盖旧产物。

报告必须在四个池内并列 BH、无过滤、Ridge、GBDT 的：账户数、完整估值数、合成收益、合成 MDD、逐股收益/MDD 中位数、GBDT 相对 BH 的逐股双胜率、平均暴露、条件暴露、换手、费用、模型接受/拒绝/不可用次数。另输出四端点正式区间和结论、U0 桥接差异、size unknown 数量、入池日期分布与异常状态。

容量只做披露：至少报告父订单名义金额分布、明确 `market_impact_simulated=false`。仅当 R8 内存在已冻结且可验证的日成交额字段时，才增加订单金额/成交额诊断；不得为获得漂亮容量而新增流动性过滤或静默估算。若无权威成交额，状态为 `CAPACITY_NOT_EVALUATED_NO_AUTHORITATIVE_TURNOVER_NOTIONAL`。

“数仓”按用户语境解释为完整结果归档和可比较分析表，不是新增数据库或数据仓库：本轮禁止 DDL/DML，文档和不可变 parquet/json artifact 是唯一交付载体。

## 8. Bridge / Causality / Fail-closed（F-008）

U0 桥接必须与父 bundle 的 E0 同入口账户逐股结果和组合路径一致；允许的差异只来自新文件列顺序等非业务表示，比较采用规范化内容 hash 与最大数值差。任何账户、终值、MDD、入池或成交业务差异均 fail closed，不能继续读取正式 ALL/LARGE 收益。

每一人口的 candidate 与 BH 使用同一入池 ordinal、同一执行和终止清算。模型权重、预处理量、阈值和特征顺序来自父模型；回放期间不能调用 fit。特征继续由每个决策时点及之前数据构建；T−1 市值通过 lag 后使用。不得用期末市值、未来成分、最终存活名单或结果回选人口。

prepare 完成全部输入身份检查后仍记录 `outcomes_read=false`；run 在 source preflight、父模型验证和 U0 bridge 输入闭合后才记录 `outcomes_read=true`。非正/非有限因子、candidate 重述、代码漂移、缺少父文件或模型 hash 漂移均 fail closed。

## 9. Implementation Plan / 最小实现计划（F-009）

只新增一个离线 benchmark 模块与一个定向测试文件，复用现有 `DailyCandidate`、R8 source reader、特征实现、父模型 validator、`replay_policy`、费用/成交和 artifact helper；不复制交易算法，不改 PT-NEXT-024 的旧合同或产物。必要的少量冻结常量可放在新模块内，不建立第二套研究平台。

CLI 仅含 `prepare`、`run`、`inspect`、`verify-parallel`。正式顺序为 prepare → 小样本串并行一致性 → 8 进程全量 run → inspect → exact retry。所有正式结果在 WSL R8 路径执行。

## 10. Verification Plan / 审核、验证与合入（F-010）

测试覆盖四个人口边界、T−1、市值缺失、首次入池固定、父模型 hash、禁止 fit、U0 bridge fail-closed、四端点校正、artifact identity、exact retry、串并行一致和零数据库/运行时边界。避免为内部实现细节堆叠重复测试。

完成后执行定向 pytest、ruff/compile、`git diff --check`、F1 validator、主 F2 validator和当前 required CI。至少进行三轮复核：业务/因果、统计/数值、隔离/文档一致性。通过后按用户既有授权提交、PR、CI、合入与 close-sync；无需后端重启。研究阴性或不可分辨不阻塞研发交付，也不允许改参数追求阳性。

## 11. Risks / Production Gates / Rollback（F-011）

- 全市场包含小盘、退市和市值缺失股票，结果更接近人口迁移而非可容量化组合；无冲击假设必须显式保留。
- 冻结模型在新人口可能特征分布漂移；报告模型不可用、预测和触发分布，不用自动重训掩盖漂移。
- 首次入池 cohort 的合成路径不是在窗口起点一次买齐的现实基金组合；它用于同股择时与 BH 因果比较，不用于宣称可直接配置全市场。
- R8 历史和本轮问题均已被研究者观察；结论属于探索性迁移证据，不是 sealed holdout 或实盘保证。
- 无运行时发布。回滚方式是停止引用新 bundle；保留不可变证据，不删除父产物，不改变在线卡片。

Production Gates：数据库 DDL/DML=`NOT_APPLICABLE`；生产激活=`NOT_REQUESTED`；backend restart=`NOT_REQUIRED`；在线业务写入=`FORBIDDEN`；selected_for_live 固定为 0。

## 12. Design Acceptance Index

| design_item | 设计要求 |
| --- | --- |
| F-001 | 以迁移检验解释 U0 结果，不重新训练或扫描参数 |
| F-002 | position_timing 隔离、纯离线、E0-only、零 DB/服务动作 |
| F-003 | R8、父 bundle 与两模型不可变身份 |
| F-004 | U0/LARGE/SMALL/ALL 四个 T−1 首次入池人口 |
| F-005 | 同账户、同策略、同 BH、同成本与执行合同 |
| F-006 | 两池四端点正式 family 与诊断边界 |
| F-007 | 全结果比较、容量限制与不可变归档 |
| F-008 | U0 精确桥接、PIT 因果与 fail-closed |
| F-009 | 单适配器、单测试文件、复用旧实现的最小架构 |
| F-010 | 多轮审核、验证、CI、合入和无需重启 |
| F-011 | 风险、结论限制、零生产发布与回滚 |

## 13. Design Acceptance Matrix

`IMPLEMENTATION_VERIFIED` 只表示冻结实现、回放和不可变证据闭合，不表示 alpha、未来收益或运行发布获得支持。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | §1、§5、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/report.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED_INCONCLUSIVE | none |
| F-002 | §2、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/receipt.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-003 | §3、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/requests/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-004 | §4、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/enrollment_audit.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-005 | §5、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/report.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-006 | §6、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/report.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED_INCONCLUSIVE | none |
| F-007 | §7、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/manifest.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-008 | §8、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/bridge_audit.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED_EXACT | none |
| F-009 | §9 | `backend/services/position_timing/causal_timing_universe_benchmark.py`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED | none |
| F-010 | §10、§15 | test: `python -m pytest backend/tests/position_timing/test_causal_timing_universe_benchmark.py -q`；artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/manifest.json` | IMPLEMENTATION_VERIFIED | none |
| F-011 | §11、§15 | artifact: `/home/lc999/data/position_timing_artifacts/position_timing_advice_v1/research/causal_timing_universe_transport_v1/bundles/9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd/receipt.json`；`backend/tests/position_timing/test_causal_timing_universe_benchmark.py` | IMPLEMENTATION_VERIFIED_NO_RUNTIME_RELEASE | none |

## 14. 初始设计审核记录

1. 目标审核：将“去掉市值限制”冻结为人口迁移问题，避免同时重训模型导致无法归因；保留 U0 精确桥接。
2. 过度工程审核：删除重复训练、分钟执行、Oracle、新数据源和平台化组件；只保留一个离线适配器、四个人口和一个正式 family。
3. 因果与统计审核：ALL 不要求市值已知，三个市值层不填充未知；T−1、首次入池固定；正式四端点校正与诊断结果分离。
4. 隔离审核：新 artifact namespace、父产物只读、零 DB/在线/进程控制；“数仓”明确为文件化研究归档。

## 15. Implementation and Formal Results / 实现与正式结果

### 15.1 不可变身份与完整性

实现提交为 `ad687ed8d`，只新增 `causal_timing_universe_benchmark.py`、一个定向测试文件和本文档；后续提交仅回填结果与蓝图。正式 request/bundle ID 为 `9ac167a77f02dad65b216a26178c4ecbf96e0b0f5b5d14f3bc74069ce6efa5dd`，manifest canonical SHA256 为 `a711da8be0905ef1d799ce1a3678c47e4b693eb5d5ac446b8dd66d146ce8e4fd`，receipt canonical SHA256 为 `5dda77320c9e6840a8acabd7260f927fe71e5ea117f2862c50e885aeff522aee`。inspect 返回 `VERIFIED`，相同 request exact retry 返回 `ALREADY_MATERIALIZED` 且 manifest identity 不变。

R8 源人口 5,144 股、81 个 chunk、chunk 人口合计 5,144。ALL 入池 5,076 股、未入池 68；LARGE 入池 594；SMALL 入池 3,135；U0 入池 3,638、`ENROLLMENT_UNKNOWN` 566、未入池 940，与父研究一致。四池独立按各自条件首次入池，股票可在窗口不同时间进入多个诊断池：U0∩LARGE=385、U0∩SMALL=2,006、LARGE∩SMALL=20；因此 SMALL 结果不能当作 ALL 增量的互斥归因。

8 股串并行结果为 `EXACT`。U0 全人口桥接中 14,552 个四政策账户摘要的终值、MDD、费用、换手、暴露与条件暴露全部零差异，405,176 条成交 canonical 相同，四条组合 NAV 路径逐点零差异。父产物曾从成交表重建模型拒绝数，而拒绝动作不会产生成交，故父诊断列低估；新实现从 replay state 记录真实拒绝／不可用数并在 bridge audit 显式列为预期诊断差异，不改变策略、成交或收益。

### 15.2 横向比较

以下均为 2024-06-28 现金基准日至 2026-08-31 统一终点、每股独立 1,000 万元账户的合成路径；“改善”均为 GBDT − BH。SMALL 与 U0 是诊断，正式 family 仅 ALL 和 LARGE。

| 股票池 | 账户 | BH收益 | GBDT收益 | 终值改善 | BH MDD | GBDT MDD | MDD改善 | 逐股双胜率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ALL_PIT | 5,076 | 71.3554% | 71.9923% | +63.69 bps | -24.8809% | -23.5912% | +1.2896pp | 51.44% |
| LARGE_GT_500B | 594 | 23.9701% | 24.0520% | +8.19 bps | -19.6421% | -18.9116% | +0.7304pp | 49.66% |
| U0_BRIDGE_50_500B | 3,638 | 42.3406% | 42.8656% | +52.50 bps | -24.1642% | -22.7672% | +1.3970pp | 54.65% |
| SMALL_LT_50B | 3,135 | 81.8292% | 82.6664% | +83.72 bps | -27.9971% | -26.6004% | +1.3968pp | 51.87% |

无过滤 CYCLE5 在四池都落后 BH，说明仅加快固定 5 日恢复仍不能解决动作选择。Ridge 相对 BH 的点估计在 ALL/SMALL 为约 +38.26/+54.05 bps，在 LARGE/U0 为约 -50.63/-29.68 bps；不能把 GBDT 的正点估计简化为所有函数族一致。GBDT 平均暴露相对 BH 只降低约 1.58～2.01 个百分点，却显著增加换手和费用；它不是靠长期大幅空仓获得全部 MDD 改善，但仍未建立可分辨 alpha。

### 15.3 正式统计结论与解释

| 正式比较 | 终值点估计 | family-wise终值区间 | MDD点估计 | family-wise MDD区间 | 结论 |
| --- | ---: | ---: | ---: | ---: | --- |
| ALL GBDT − BH | +63.69 bps | [-1046.98,+499.97] bps | +128.96 bps | [-13.87,+229.64] bps | INCONCLUSIVE |
| LARGE GBDT − BH | +8.19 bps | [-299.34,+217.11] bps | +73.04 bps | [-13.48,+163.20] bps | INCONCLUSIVE |

ALL 的名义 MDD 区间下界仅 +0.21 bps，但四端点校正后跨零；终值区间也明显跨零。取消 50～500 亿元限制把收益点估计从约 +52.50 bps 提高至 +63.69 bps，但没有改变证据分类，且 MDD改善点估计略降。大于 500 亿元池的收益优势几乎消失。SMALL 诊断点估计较高，但人口与 U0/LARGE 非互斥、同一 R8 已被反复观察、且没有市场冲击／成交额容量模型，不能据此回选“小盘股阈值”或宣称小盘 alpha。

容量披露记录 1,394,249 个已成交父订单，名义额中位数约 237.37 万元、95 分位约 999.81 万元、最大约 3.045 亿元；R8 没有在本轮冻结权威日成交额，状态为 `CAPACITY_NOT_EVALUATED_NO_AUTHORITATIVE_TURNOVER_NOTIONAL`，`market_impact_simulated=false`。这对 SMALL 结果尤其重要，但不反向修改账户或过滤股票。

最终结论仍为 `EXPLORATORY_UNIVERSE_TRANSPORT/INCONCLUSIVE`，`selected_for_live=0`。市值限制不是当前证据不足的主要工程阻塞；继续扫描市值阈值只会扩大选择偏差。下一优先项恢复为 PT-NEXT-026：围绕“何时恢复／继续持有价值”冻结一个更高信噪比的单一因果目标，沿用 Ridge 校准和浅层 GBDT 参考，不引入新模型族、分钟方向模型或 QE/HMM 融合。SMALL 只保留为未来容量证据具备后的外部有效性诊断，不作为下一轮选股规则。
