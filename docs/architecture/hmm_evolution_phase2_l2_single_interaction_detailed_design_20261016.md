# HMM Phase 2：L2 单交互项匹配对照详细设计

版本：1.0；日期：2026-10-16；任务类型：F2。
授权：用户在 P0→P1→P2 连续任务规划后要求“开始执行长任务”，并另行批准本任务审核通过后提交、推送和创建 PR。本合同只展开该规划中的一个交互项和 4-fit 预算；不自动合入、不清理、不扩大生产或进程权限。

## 1. 背景与目标

父蓝图为 `hmm_evolution_and_risk_management_system_design_20260716.md`。最终目标仍是申万二级行业的轮动预测和风险控制，为真实消费场景提供可验证的增益；不能用研究记录、合法性检查或一级行业页面代替效果。

已有四特征 raw-return Ridge 在旧面板上显示小幅正 spread，但相对旧对照的增量区间不确定。本轮仅检验一个假设：资金流增量与正相对动量的交互是否提供加性模型遗漏的信息。该机制未被证明。本轮是 `RETROSPECTIVE_DEVELOPMENT_RESEARCH`，不产生独立 forward 确认、不覆盖旧结果。

P0 的既有订阅保持 enabled，原 durable job 不重复执行；公共 `coefficient_readiness_tick` 的生产接线仍由共享调度器 owner 处理。P1 复用已有风险终态：回撤改善伴随收益机会成本及较高换手，只适合作为风险优先研究候选，不能宣称净收益增强。

## 2. Scope / 非目标

本包一次完成精确合同、最小源码、定向测试、多轮作者审核及有限研究；不为 helper、receipt 或每个指标单独拆阶段。

允许源码范围：

- `backend/services/hmm_risk/rotation_l2_moneyflow_supervised.py`：抽取现有训练数据/标签读取纯函数，不改变旧调用行为。
- `backend/services/hmm_risk/rotation_l2_moneyflow_price_supervised.py`：抽取现有价格视图验证，不放宽旧 terminal pins。
- `backend/services/hmm_risk/rotation_l2_interaction.py`：新匹配对照研究。
- `scripts/hmm_risk/run_rotation_l2_interaction.py`：file-only CLI。
- `backend/tests/hmm_risk/test_rotation_l2_interaction.py`：直接合同测试。
- 本详细设计与父蓝图当前状态。

不修改 QE、Selection、Advisory、数据生产、候选文件、scheduler、CI/nox/test plan、生产订阅和旧模型。无依赖安装、数据库读写、runtime activation、进程控制、QE 实验、完整 HMM 训练或参数搜索。合入与 cleanup 仍为独立动作。

## 3. 架构与数据契约（D1）

正式 `rotation_l2_input` 文件 reader → 同一训练输入/价格视图 → 两个 fresh process（每个按固定顺序训练 control、interaction）→ 父进程零-fit 重算参数与预测一致性 → 首次读取评估期标签 → 匹配效果与参考路径分析。

本次输入固定为：

| 字段 | 冻结值 |
| --- | --- |
| generation | `20261015-v18-unified-quote1` |
| release / cutoff | `qe_hmm_full_v2_20260831` / `2026-08-31` |
| manifest identity | `f82670728cd6cde06bbc834481187794d081070689648c06b88786a7782e435b` |
| profile byte SHA256 | `807ce03afc9838e5d1c6e71e3a46c34b41f4b77032bf42abee0dbb2e30aa0e23` |
| manifest byte SHA256 | `1d3602211a7828cc5696d8eb206e49edf1bdccf447cd2a85c0a7186e02b8bf5c` |
| sector data byte SHA256 | `575ad57d52567857a028ff71809ea776894dce3f00421182ecf9ebdb4e3402f3` |
| moneyflow byte SHA256 | `88467d29634f9f1ddbbdaba6a9ee8f759d68b6f109960a77023ab957b6daab60` |
| 共享 code-map canonical digest | `9f24562fdb5db8f1bba82ab33cabbe3df478dfdff5a0039d292c92ff929ec4a8` |
| reader mapping_hash（131 个 member-backed 文本代码 digest） | `ed23788ae6684bdd559d3a0a9088b9b0d9fd7dde37cc8a37932c582b0c632cb1` |
| quote authority canonical digest | `796c0036b1b1aea5592eb6f3be48a84c9aab26a3ad1b120c33dc205fb06fff97` |
| security identity canonical digest | `21a47581be3aec0f6d7f41de4f863b549aa227f1a8026e4398ce4a5c20095a87` |
| provider absence canonical digest | `494a9b3ff725792592cf668505691e57a000ef10f160015dc3c45c28552565ba` |

显式 profile 路径为 `F:/Dev/RD-Agent-state/dataset_profiles/active_dataset_profile.json`，候选根为 `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20261015-r8-unified-quote1-candidate`。运行前与现存 active 重新核对；漂移即停止，不自动换 release。所有 source 文件由 reader 根据正式 manifest 验 pin，不创建 HMM 私有行业编号、不读数据库、不回退旧目录。

行业身份为 131 个目录申万 L2 文本代码；稀疏 ID 只用于共享 release 连接。停牌、停发报价与真实 provider absence 沿用既有 typed NA 语义；保留完整分母，禁止补零、前填、默认 neutral、删行业或造价格。两个训练臂的 eligibility 必须相同，不能用 outcome 决定特征资格。

## 4. 日期与因果边界（D2）

| 项目 | 精确账本 |
| --- | --- |
| 最早特征来源 | `2024-08-13`，首个训练日之前 25 个交易日 |
| 训练决策 | `2024-09-19..2025-03-31`，126 日 |
| 训练标签成熟截止 | `2025-04-15` |
| purge | 10 个交易日 |
| 预测决策 | `2025-04-16..2026-03-31`，232 日 |
| 成熟 10D 标签 | 222 日，最后成熟决策 `2026-03-17` |
| 未成熟尾端 | 最后 10 个决策日明确 outcome_not_mature |
| 分块 | first=`2025-04-16..2025-09-30`；second=`2025-10-01..2026-03-31` |

每个决策 t 的特征仅使用 t−1 及更早数据。两个子进程仅接收训练标签截止日以内的标签和 t−1 特征视图；评估标签由父进程在两份 process 的参数、身份、预测和 fit 预算完整验证后读取。禁止解析 `2026-04-01` 及以后的 outcome。读取文件哈希不等于把文件内晚期标签加载给模型。

该历史窗口已参与既有研究，不是未消费 holdout。任何正结果只支持继续研究，不能给 forward/advisory/default promotion。

## 5. 模型、公式与预算（D3）

control 使用原四列：`moneyflow_level_rank`、`moneyflow_delta_rank`、`relative_momentum_rank`、`relative_downside_rank`。资金流使用原 25 日来源、20 日 intensity 与 5 日 delta；价格使用原 20 日窗口；横截面 exact average rank 缩放到 [−0.5,0.5]。资金流在 E0 中 rank，价格在 E_plus 中 rank，均早于 outcome 过滤。

interaction 仅追加：

`moneyflow_momentum_interaction = moneyflow_delta_rank * max(relative_momentum_rank, 0)`

不重新 rank 交互、不增加 cutoff、阈值、lags、正则网格、交互组合或其他模型类。两个臂用完全相同的日期、行业、raw 10D 相对收益 y 和日期等权 `1/(usable_dates*date_samples)`；不再对 y 做 rank。Ridge 固定 `alpha=0.01, fit_intercept=true, solver=svd, positive=false`；seed 不适用，无 refit。

严格预算：两臂 × 两 fresh process = **4 fits**，每个进程 control→interaction 各 1 fit。重复进程用于数值复现，不是另一个候选。任何 fit/身份/数值失败即结束，不重试、换 seed、增加 fit 或调公式。父进程只重算矩阵、stationary gradient 与预测，不能调用 fit。

固定 Windows Conda base：Python 3.13.5、NumPy 2.3.3、SciPy 1.16.3、scikit-learn 1.8.0、threadpoolctl 3.6.0；六个数值线程变量均为 1。环境变量仅控制数值线程，不承载研究记录或运行资产。子进程 `PYTHONPATH` 显式指向本任务 worktree。

## 6. 评价、价值与终态（D4）

先报告各臂 native coverage、合法 NA、成熟度，再在共同可评估人口比较：daily Rank IC、top/bottom 实际 10D spread、相对差的 Newey–West HAC（lag=9、95%区间）及两个预注册分块。delta 仅为零-fit 解释对照；不得把 eligibility 差异当成模型改进。

同时复用既有 `rotation_l2_reference_value.cohort_reference_path`：t−1 及以前事实生成决策 t 信号，按 t 收盘参考价进入，仅从 t+1 起计持有收益，10 个隔离 sleeve、每 sleeve 等权 trending 组、无杠杆、退出规则与现金保持原合同；0/5/10/20bp 单边假设成本。它是既有 close-reference 合同，不是次日实际成交模型。报告收益、MDD、换手和机会成本；它不是个股成交或真实净收益。持有的官方行业指数发生合法停发导致路径不可估值时，必须报告 `INSUFFICIENT_REFERENCE_PATH`，不造价格、不自动缩窗或删除行业。

价值解释不是新的 AND promotion 门：共同 daily IC 增量均值非正记 `NOT_SUPPORTED_BY_CURRENT_EVIDENCE`；均值正而 95% 区间跨零记 `POSITIVE_INCREMENT_UNCONFIRMED`；增量区间下界大于零记 `RETROSPECTIVE_INCREMENT_OBSERVED`。若共同人口不足或 IC 不可计算记 `EVIDENCE_INSUFFICIENT`。spread、风险、成本可反对收益价值，必须一起解释，不因单个指标正就宣称可采用。

本包终态无论正负均 `forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`、`production_promoted=false`。无增量即结束，不自动开启第三模型；正增量也只能提出版本化可选研究建议，不能替换现有 QE 系数或日度订阅。

## 7. 进程、产物与失败契约（D5）

输入、两个 child 和 compact acceptance 使用 canonical 内容 hash 与 source commit。CLI 限定普通绝对路径、repo/dataset 外输出、新目录 exclusive-create；禁止覆盖旧结果或写入候选。正式执行要求 source clean 且与输入 source commit 一致。child 输出包含每臂 fit 计数，父进程分别验证相同臂跨进程 bitwise、一致训练样本和预测目录；哈希自洽不能替代业务重算。

typed failure 持久化到合法输出旁的独立 sibling 文件；已知完成的 fit 与未知状态明确区分，不能把 child 崩溃伪装为 0 fit。未验证输出 authority 时不得写 failure receipt。记录只在文件中，不新增环境变量、数据库表、后台服务或重启依赖。

## 8. 实施方案与验证（D6）

### Verification Plan / 验证方案

先 RED→GREEN 直接测试：交互精确公式/维度、两臂相同训练 y/weight/population、t−1 边界、未知/漂移输入拒绝、参数/预测自重哈希篡改拒绝、两进程差异、4-fit 预算、标签读取顺序、合法停发路径、不覆盖/typed failure 和 CLI fresh-process。纯函数提取必须保留旧调用结果。

本地只跑 changed-file lint/compile、直接测试、受提取影响的最小原合同测试、ownership/module registry、F2 validator、diff check；广泛 HMM 回归交 CI/Nightly，不重复全矩阵。代码最多三轮作者审修：因果/数学、身份/失败、价值/交付；无 finding 可提前结束，三轮后仍阻断则如实停交。

### Design Acceptance Index

- F-001 / D1：正式文件数据身份与 L2 分母。
- F-002 / D2：126/10/232/222 日账本与标签隔离。
- F-003 / D3：单交互、匹配 raw-return 训练、4 fits 与数值环境。
- F-004 / D4：共同人口效果、参考路径与诚实价值结论。
- F-005 / D5：canonical/业务 readback、fresh process、文件失败与权限。
- F-006 / D6：定向验证、审修、边界及交付。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `rotation_l2_interaction.py::validate_input` | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py::test_self_hashed_old_release_or_quote_drift_rejected` | verified | 无 |
| F-002 | `rotation_l2_interaction.py::training_matrix` | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py::test_evaluation_outcomes_cannot_enter_fit_input`；`test_t_minus_one_price_boundary` | verified | 无 |
| F-003 | `rotation_l2_interaction.py::run_process` | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py::test_matched_training_has_identical_y_population_and_weight`；`test_paired_child_is_two_fits_and_parent_zero_fit_readback` | verified | 无 |
| F-004 | `rotation_l2_interaction.py::close_processes` | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py::test_hac_evidence_uses_real_lower_field`；`test_legal_held_quote_na_is_not_valued_or_filled` | verified | 无 |
| F-005 | `run_rotation_l2_interaction.py` | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py::test_cli_rejects_child_drift_before_any_outcome_read`；`test_cli_unsafe_destination_cannot_write_failure_or_train` | verified | 无 |
| F-006 | 本节、定向测试与审修结论 | `pytest backend/tests/hmm_risk/test_rotation_l2_interaction.py`；`nox -s validation_module_registry_l0`；`nox -s l0` | verified | 无 |

矩阵验收对象为实现合同，不是模型效果。源码首个提交前正式研究为 0/4；提交后正式 file-only preflight 和 4/4 fits 的结果见§12，不以测试或 F2 PASS 替代。

## 9. 风险、发布与回滚

历史标签选择偏差、只有一个训练窗口、小幅因子增量及不确定区间必须披露。新 release 真实修正可能改变训练面板，因此不能直接用旧模型绝对指标归因交互；必须同源匹配对照。旧价格 validator 的 v15 pins 不得放宽，新研究建立自己的明确身份。

源码提交/PR 与研究结果、合入、生产采用分别报告。研究输出为新版本文件；无生产发布动作，无运行态回滚需要；停止只是不继续该候选，不删除旧结果。

## 10. Production gates / DESIGN-COMPLIANCE-001

- production_ddl_gate=noop；production_dml_gate=noop；dependency gates=noop。
- database_read=false；database_write=false；dataset_write=false；active_profile_write=false；runtime_action=false。
- 后端重启权限=false；离线研究本身不需要后端重启。实际 changed-files 分类为 `runtime_impact=backend, target_ids=[backend-main]`（共享 HMM helper 为 backend 源码）；已以 AIstock Python fresh-process 导入 `backend.routers.hmm_risk`、`backend.routers.health` 及三个变更模块并核对其绝对路径。源码持久化基础为 git；合入后的后端加载仍待用户重启和只读身份/业务验证，本任务不执行这些动作，也不声明 runtime 已生效。
- 无简化版：完整两臂、共同人口、参考路径与明确终态，不以 mock 测试冒充实验。
- 无静默错误：缺失/漂移/非有限/停发均 typed，完整分母，禁止填值。
- 无业务漂移：旧模型/默认/行业身份/状态/窗口/成本合同保留，只增加已规划的一个交互研究。
- 无私增审批或门禁：统计区间用于证据分级，不捆绑生产功能，不新增 headroom、额外 AND 或人工批准点。

## 11. 实现审核与执行状态

完成三轮作者自审（非独立第三方）：第一轮核对匹配 raw-return 目标、日期等权、唯一交互和父进程零-fit，纠正 HAC 区间字段读取；第二轮核对身份、标签隔离和失败输出，区分共享 code-map digest 与 131 个 member-backed digest，增加 active profile 漂移提前拒绝；第三轮核对参考成交时点、合法停发不可估值、价值分级和权限，明确原参考路径不是次日实际成交，未发现剩余阻断 finding。

本地结果：新直接测试 18 passed；受纯函数提取直接影响的既有小矩阵 12 passed；Ruff、format、py_compile、ownership（7/7）、module registry（8 passed）、L0（0 blocking）、F2（6 items / 6 rows / 0 warnings）及 diff check 通过。AIstock 环境 fresh-process 导入与路径核验通过。广泛 HMM 回归交正式 CI，不重复完整矩阵。

源码提交前一次零-fit 预检因本合同摘要误用停止（数据未修改、fits=0），已按正式 reader 的 member-backed digest 修正。该段记录原审核状态；提交后实际研究见§12，backend runtime 仍未因本研究改变。

## 12. 正式研究结果与取舍（2026-10-16）

执行源码为干净提交 `e77f5fcde0f0c1f2109d2364720fe1ff58a3a67a`（已安全合并当时最新 main）；两个进程全部完成，4/4 fits，逐臂 bitwise 一致、父进程参数/目标函数/预测零-fit 回读通过，随后才读取截至2026-03-31的评估事实。冻结 active profile 的字节 SHA 执行后仍未变化。

正式 file-only preflight=PASS；131目录均保留，每臂输出30,392行，130个合格行业形成222个成熟评价日的共同人口。`801011.SI` 的232行保持正式 `hmm_risk_rotation_l2_quote_unavailable`，未补值、删目录或阻断整个研究；最后10个预测日保持标签未成熟。两臂训练同为126日、16,380行。

| 研究臂 | mean daily Rank IC | 95% HAC区间 | mean 10D spread（收益率） |
| --- | --- | --- | --- |
| 四特征 control | 0.0247737229 | [-0.0205049752, 0.0700524210] | 0.0003824485 |
| 五特征 interaction | 0.0249539548 | [-0.0204378791, 0.0703457886] | 0.0004027028 |
| 零-fit delta参照 | 0.0180492172 | [-0.0218827588, 0.0579811932] | 0.0019297900 |

interaction−control 的共同人口 IC 增量为 `0.0001802318507448995`，95% HAC区间 `[-0.0003295643230118229, 0.000690028024501622]`；spread增量 `0.000020254331215041453`，区间 `[-0.0002627586562279175, 0.0003032673186580004]`。两个预注册分块的 IC 增量均微正但区间跨零；不能把它称为已确认的增量或非平稳机制证据。

四臂×四成本的16条参考路径均232/232日可估值、无NA。0bp时 control/interaction 累计参考收益分别26.774867%/26.694367%，MDD幅度7.500668%/7.614303%，累计双边换手52.719653/52.807840（初始净值倍数）。interaction相对control在0/5/10/20bp下收益分别少0.080500/0.078550/0.076646/0.072976个百分点，MDD幅度分别更大0.113636/0.108607/0.091249/0.084395个百分点。四份配对日收益HAC区间均跨零。成本只是既有指数收盘参考假设，不是个股实际成交净收益或QE实验。

正式统计终态为 `POSITIVE_INCREMENT_UNCONFIRMED`；业务取舍是**没有证据支持采用该交互项**。不推导所有交互/非线性模型无效，不追加拟合、调参、第三模型或生产替换。`forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`、`production_promoted=false`。

正式结果：`F:/Dev/AIstock_runtime/hmm_l2_interaction/20261016_e77f5fcde/run/acceptance.json`（271085 bytes）；canonical acceptance SHA=`28e095a60b81870305cf3bfe0442338ab2336655053ee891b47b12252ea0ad9d`，文件字节SHA=`80a0b340e94be430a1593f6d9ac2e0ae639aab9db44e8ba4a4da6f8bb8a6dea3`；input hash=`62e61be7f523b2bb8d643736c693e59692a6a1337dc115dfb05aa41a71215b21`，outcome hash=`59a8c01107aad25146a39983cef8c634da7210394e8827c07e28c8c696ed6327`。canonical hash 与文件字节 SHA 是不同身份，不混写。

control model hash=`7396b08e882fab92eb0acf21cfa3b49c68f7dd156807bf42c4af5e0466e79150`；interaction model hash=`21dc3423fe07b6c5b2dd390bc8ff71562910f6657395e21f35217695e6a1fbb1`。两个进程和最终acceptance均绑定各自模型，不把新研究身份用于旧QE资产或订阅。

后续只交付源码/结论PR，不自动合入、cleanup、生产写入或重启。P0共享调度公共接线仍由对应owner闭合；P1风险优先研究判断不变。本研究已达到有限假设的终止条件，不因预留任务时长继续消耗历史窗口。
