# LSTM-VALUE-1：冻结模型的重排价值诊断（F1）

2026-11-01，Advisory/model_first，用户批准执行长任务。当前版本为实施合同，真实运行和验收状态以末尾矩阵、结果报告和不可变工件为准。

## Background / Goal / Feature Card

唯一目标是帮助精确LSTM包获得有价值的Top50→Top10重排。P19专属SPO+在已消费开发窗相对原生有正增量，但尚无独立确认；P20已冻结第3折并捕获首个新批次。本任务利用现成工件回答编码可比性、改变选择的机制和历史改善的依赖性，交付可复用离线工具与一个具体处理决定，不把工程完成当经济成功。

主对照为原分数Top10，固定T开盘至T+4收盘、十槽、买0.95/卖5.95bps及UNKNOWN/CASH。生产Top20持仓政策不改。精确包、manifest/run/source沿[P19](advisory_lstm_specialist_spo_v1_f1_design_20261027.md)和[P20](advisory_lstm_spo_shadow_v1_f2_design_20261027.md)，不将结论泛化至所有LSTM或其它包。

## Scope / Inputs / 时钟与角色

精确scope位于`X:/AIstock_temp/ADVISORY/lstm-value-20261101/scope.json`。新增离线脚本、直接测试、本设计和结果报告；只补新脚本的精确ownership/CI route，不改训练器、后台/API/调度、数据集、策略包或当前模型文件。

输入角色由预注册protocol绑定绝对路径、字节SHA256及size；解码前/运行后复核，拒绝角色错配、hash漂移、重复身份、错包或错预测日期。只接受规划inventory列明的P19 recovery-v2 archive和当前固定模型/release/prediction；不由目录里最新文件替代。

- 历史特征：原305D/15,250行；三折模型与原持出预测126D/6,300行。
- 训练资格：每折原train_finance。先投影D/H，再解码H≤该折截止的已消费标签；用原training资格复原KEY并核对training_keys_sha256，禁止调用fit。不能把截止前全部行都叫实际训练样本。
- 经济诊断：仅P19 evaluated/panel、daily、finance和report的已消费开发结果；三臂共同完整主分母102D，原126D及未知/未成熟行全保留。
- 当前输入：既存D=2026-10-30/T=2026-11-02预测的50行inputs/predictions。payload `224d0203a0bb5c09db908f65b3e5fcc00845fbaff050f0865bf4bdce32ad8e14`，冻结模型文件`8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813`、release `877bfb0f0826487504ffbd5fcc92947497b226e864e10297ed1718392eb0b74e`。无当前收益入口，禁止当前数据进入历史角色。

不读新的raw收益、2026压力期、outer、sealed或当前未成熟结果。历史再分析标DEVELOPMENT_REUSE_ONLY；当前仅1D，不检验总体漂移显著性。无可追溯既存行业映射时NOT_AVAILABLE，不派新采集任务。

## Architecture / Implementation Plan

单个operator脚本提供freeze/run的文件型CLI。复用`economic_entry_pipeline.publish_stage/read_stage`的preregistered/prepared/evaluated，不发布trained阶段。冻结输入和源码identity后才能解码；已完成同一protocol再次run只验证及复用工件，不重算；冲突拒绝。

1. 原编码是8字段＋8missing bits、原median/scale、按包/D对有效行中心化。逐字段贡献求和必须还原原decision_score；未知行保留原槽，不用默认分数伪成功。父分数两字段和六个行情字段分开汇总，但相关字段贡献不是独立信息或因果归因。
2. 当前/每折历史输出完整人口、原/模型rank、保留/换入/换出和贡献。预测分数及其差不能标成收益率。复用权威predict作核对，历史仍走原fold日期约束，当前仍走精确冻结模型约束。
3. 对每组10×40边界对计算gap和六个已知连续行情字段的单坐标翻转距离（按原scale）。group-centering公共均值在同组差中抵消；零系数、tie、缺失非可导和no_rank_advice单列。派生parent_score_z/rank不作独立可实现扰动。边界量明确ALGEBRAIC_SENSITIVITY_ONLY，不是行情误差、概率或新交易政策。
4. 训练/持出/当前分别报告缺失、固定分位数、训练范围外情况和贡献尺度。训练keys不符即拒绝训练可比性证明，不静默换用更宽参照。
5. 历史经济只复用原三臂共同成熟mask；按原fold/季度与全日期报告，核对逐股权重差乘旧净值/10到原增量。新增正负贡献分离、日期/股票集中度、单日与连续原日期固定5D块影响诊断。缺失保留，影响诊断不改变主分母，不删极值后重报收益，不把重叠5TD或同股票重复出现当独立样本。
6. 记录全部诊断投影计数、0fit与无新政策搜索；产出处理决定。只有真实实现错误才转最小BUG；有实质不同可证伪机制才设计唯一候选及预算，否则KEEP_FROZEN_OBSERVATION或NO_ACTIONABLE_INCREMENT。没有任何分支自动训练、改榜或激活。

主要输出为input_profile.json、score_contributions.parquet、boundary_review.parquet、historical_dependence.json、stock_dependence.parquet、decision.json和运行receipt。工件均在新的X根、create-only；不改P19/P20旧工件。schema显式区分诊断与经济确认。

## Verification Plan

先合成直接合同测试，再真实新进程运行。覆盖贡献/中心化/missing/tie、解析边界与独立数值扰动、角色/时钟/hash/人口拒绝、不可变发布复用与冲突。用调用拦截证明未调用fit、金融源或服务执行；不能只相信receipt里写0。

真实验收要求训练KEY匹配三折、15,250输入/6,300历史预测/50当前行、126D/102主分母、逐股增量对账、原模型/release/预测hash不变，未知和原负结果可见。诊断不得反向修改原研究分类。

R1审算术与真实输入；R2审开发复用/分母/重叠/因果/搜索边界；R3审scope/无副作用/全部设计项与真实输出。均是本窗口自审。最小本地lint/compile、直接测试、scope/ownership、L0、F1及diff；CI覆盖新测试，宽模块回归交CI/Nightly。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-581 | 精确已消费archive/当前角色绑定、时钟/来源hash、完整人口与训练KEY |
| F-582 | 原16维编码贡献还原、原rank/tie/unknown与换入换出解释 |
| F-583 | 当前/历史画像与解析边界，派生坐标/缺失/零斜率的真实含义 |
| F-584 | 原126D/102D主分母、历史逐股对账、影响/集中度诊断及开发复用标识 |
| F-585 | 不可变文件CLI、幂等/冲突、零拟合/当前无收益/无后台副作用 |
| F-586 | 多轮审核、真实工件、唯一处理决定、经济与交付分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-581 | scripts/advisory_lstm_rank_value_review.py | backend/tests/advisory_model_first/test_lstm_rank_value_review.py | PLANNED | 实施中 |
| F-582 | scripts/advisory_lstm_rank_value_review.py | backend/tests/advisory_model_first/test_lstm_rank_value_review.py | PLANNED | 实施中 |
| F-583 | scripts/advisory_lstm_rank_value_review.py | backend/tests/advisory_model_first/test_lstm_rank_value_review.py | PLANNED | 实施中 |
| F-584 | scripts/advisory_lstm_rank_value_review.py | artifact: X:/AIstock_temp/ADVISORY/lstm-value-20261101/study/evaluated/historical_dependence.json | PLANNED | 尚未运行 |
| F-585 | scripts/advisory_lstm_rank_value_review.py | backend/tests/advisory_model_first/test_lstm_rank_value_review.py | PLANNED | 实施中 |
| F-586 | docs/analysis/advisory_lstm_rank_value_review_20261101.md | artifact: X:/AIstock_temp/ADVISORY/lstm-value-20261101/review_receipt.md | PLANNED | 尚未交付 |

## Non-goals / Risks / Production Gates / Rollback

0新fit、0参数/阈值/seed或新政策收益搜索；累计248＋1不变。未提出新的有效性门槛或等待条件。研报/现金流/价格/Exit/其它包暂停。NON_VINTAGE、开发复用、多次选择、未知完整组偏差及当前样本仅1D全部保留；解释能力不等于新增alpha。

生产DDL/DML、依赖、客户端、数据集、服务和QE均noop；backend_restart_required=false，因为没有后台运行源码或配置修改。P20原模型和已保存预测冻结。回退只需停止使用离线工具，保留所有不可变工件；不需要数据库或进程操作。
