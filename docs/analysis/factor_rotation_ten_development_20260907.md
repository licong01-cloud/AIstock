# 十因子研发：设计、真实评估与入库交付

## 背景

F1 feature。用户批准十个因子研发并入库，明确不限于指数。已确认前三个方向使用沪深300/中证500/中证1000 PIT分层、沪深300基准；第四个原SW1广度名额改为下行半方差变化。实际为3个指数相关、7个非指数候选。

选题时生产库789条目录、584个可用：MF118、VAL91、LIQ83、VOL68、CHIP67、MOM60、STAT23、TECH23、SIZE20、CORR14、QUAL14、ML3。这是当时快照。补充关系变化、下行流动性和日内/隔夜结构；不因财务因子少就引入未验证的财务PIT。

研发完成不等于十个独立alpha成立。真实评估发现指数分层信号弱，部分非指数信号高度重复；保留原始研究结果，不按结果反复翻转方向或调参。2026年7—8月已参与选题，不再宣称为未接触的OOS。

## 范围与非目标

worktree：F:/Dev/AIstock_worktrees/factor-rotation-ten-20260907；branch：feature/factor-rotation-ten-20260907；起始基线33f5327b100989eebcffb53f3f3544505eaed6af。

精确写入范围：

- scripts/qe_alpha_candidates/rotation_liquidity_factors.py
- scripts/qe_alpha_candidates/rotation_index_factors.py
- scripts/qe_alpha_candidates/prepare_rotation_index_context.py
- scripts/qe_alpha_candidates/export_rotation_memberships.py
- scripts/qe_alpha_candidates/run_rotation_liquidity_precheck.py
- scripts/qe_alpha_candidates/run_rotation_ten_research.py
- scripts/qe_alpha_candidates/run_rotation_ten_correlations.py
- scripts/qe_alpha_candidates/repackage_rotation_ten_sources.py
- scripts/qe_alpha_candidates/validate_rotation_ten_library_dev.py
- backend/services/quantevolver/backtest_base_data_memory_cache.py
- backend/services/quantevolver/official_factor_batch_compute_service.py
- backend/services/quantevolver/factor_analyst.py
- backend/tests/quantevolver/test_rotation_liquidity_factors.py
- backend/tests/quantevolver/test_rotation_index_factors.py
- noxfile.py、tests/aistock_validation/catalog/file_ownership.yaml：接入已有qe_read_backend计划。
- docs/standards/aistock_runtime_targets_v1.yaml：只精确登记本批九个离线脚本，三个后端文件仍归backend-main。
- 本文。

仅新增本批十个明确名称的目录/指标/分类及新相关性。不禁用m_*、不删除弱因子、不改旧相关性缓存，不运行QE策略实验，不改行情/数据集/模型标签/阈值/评级/全局模型绑定。不安装依赖、不执行DDL、不控制进程。DEV resume只认本任务receipt的十个ID并先比对旧源码和资产，不能覆盖其他因子。

## 实施方案

### 输入与消费合同

输入根：X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260905-candidate/components。

- 基础因子文件factor_h5_static_candidate_v2/daily_pv.h5；Qlib日线daily_bin_candidate。
- 指数index_context/index_daily.h5，仅000300.SH、000905.SH、000852.SH。
- 已批准core_index_membership_pit的4542条区间只读提取（592/1307/2643），按半开区间生成独立index_factor_context.h5：3529800行、3037只历史成员、1961交易日。
- 无CSI2000 PIT；不把A500发布前回溯行情当as-known，不把缺乏股票映射的SW1面板替代股票行业因子。
- 正式指标复用aistock_equity_pit_canonical_v2、PIT/停牌及h1/h20合同，benchmark不进入股票分母。

新增文件仅在X:/AIstock_factor_research/rotation-ten-20260907；原数据集未修改、日线/分钟线未重新导出。三个指数因子需要显式supplemental_data_dir，不声称旧RD-Agent工作区无补充文件即可运行。官方批计算新增可选参数；默认不变，补充目录不能覆盖daily_pv。另七个只需现有daily_pv。

### 公式

以下短名加m_前缀即目录名称。收盘t最早供t+1使用。共用交易日轴、不跨股票、不填缺失。条件求和的0仅为已观测非下跌日的零贡献；无下跌样本时条件均值不可用。

| 短名 | 公式 |
|---|---|
| pit_size_bucket_relative_strength_20d | 所属PIT指数20日收益减沪深300同期收益 |
| pit_size_rotation_acceleration_5d_20d | 所属指数5日收益/5减20日收益/20，再减沪深300同量 |
| pit_index_beta_break_20d_60d | 股票对所属指数20日beta减60日beta，不拼接换桶前后的历史 |
| rebound_participation_asymmetry_20d | 20日成交额加权涨跌方向减等日权涨跌方向 |
| downside_illiquidity_recovery_5d_20d | 前20日与后5日下跌条件下绝对收益/成交额均值比的对数 |
| volume_confirmed_drawdown_repair_20d | 相对五日前已知20日峰值的回撤修复比例乘5/20日成交额比 |
| return_volume_coupling_break_10d_60d | 收益与成交额对数变化的10日相关减60日相关 |
| overnight_intraday_repair_divergence_20d | 20日日内减隔夜对数收益均值除以日收益波动 |
| downside_range_absorption_20d | 下跌日下影线/日振幅乘成交额/此前20日均额，取20日条件均值 |
| downside_semivariance_shift_10d_60d | 下行平方收益占总平方收益之比：60日减10日 |

指数收益先在完整指数轴计算再按PIT映射，避免进入日伪收益。前两个同桶股票值相同，仅三桶暴露，不是数千独立个股信号。每个catalog保存独立源码，不夹带其他公式。已用官方执行器证明独立化与既有全历史输出完全相等。跨Python unparse只在元组括号等格式上不同：保留已评估源码字节，仅允许AST完全一致的格式差异。

### 评估和入库

qe_eval_v2五窗口full/out_sample/recent_6m/recent_3m/recent_1m及h1/h20：50条指标、970条月度IC。覆盖率保留引擎nonwarmup口径，不冒充全市场完整分母。不可用JSON诊断用null，不填0。

相关性复用现有Spearman/EWMA，252日/half-life125；每批32旧加10新，释放旧panel，只保存涉及新因子的对，不覆盖旧矩阵。对584旧因子得到5875对：5865有限、10个no_valid_pairs。quality_structure_composite记录degenerate_nan；dynamic_pe_inv_momentum_breakout对新因子10对均无有效相关性，未伪造0。

DEV实际写入/回读十条目录、50指标、970月度、10分类；另复用正式writer写入并逐值回读45个新/新相关性对。DEV没有完整生产因子目录，45对不能冒充生产5875对入库完成。

首次分类失败：DEV无factor_analyst绑定，默认deepseek-v4-pro复杂响应无法解析，规则也未识别一个独立公式；生产实际绑定deepseek/deepseek-chat。增加显式单次llm_model参数，用相同模型复验10/10成功。未修改任何全局模型配置，不在失败后自动换模型。LLM只分类/描述，正式评级不由LLM或本脚本伪造。分类标签不代表真实数据依赖，例如参与不对称被LLM归为CHIP，但数据依然只有OHLC/amount，不声称使用了筹码分布数据。

## 真实结果与风险

out_sample=2024-07-01..2026-08-31，原始方向；max相关为最近252日对旧库的绝对值。负RankIC不等于已授权翻转策略方向；未证明成本后收益或组合增量效果。

| 简称 | RankIC h1 | RankIC h20 | 最大绝对相关 | 解释 |
|---|---:|---:|---:|---|
| relative_strength | -0.00783 | -0.01146 | 0.092 | 低重复但弱，仅三桶暴露 |
| rotation_acceleration | -0.00097 | -0.00437 | 0.085 | 很弱，不列优先alpha |
| beta_break | -0.00593 | -0.02433 | 0.241 | 中期关系变化观察项 |
| participation | -0.02774 | -0.03258 | 0.988 | 与m_up_down_vol_asymmetry近重复 |
| illiquidity_recovery | -0.02397 | -0.03006 | 0.395 | 较低重复的研究候选 |
| drawdown_repair | -0.03052 | -0.03868 | 0.862 | 与MomentumReversal_5D_20V高度重复 |
| coupling_break | -0.01936 | -0.02288 | 0.844 | 与CORD10高度重复 |
| intraday_divergence | -0.03565 | -0.07413 | 0.624 | 本批较值得研究的中期信号 |
| range_absorption | -0.01134 | -0.02133 | 0.450 | 下行承接观察项 |
| semivariance_shift | -0.00983 | -0.01109 | 0.803 | 弱且与波动动量重叠 |

优先研究日内/隔夜分化、下跌冲击恢复，再看下行承接及beta变化；不自动启用选股或交易，不为凑数量宣称十个都独立有效。

## 验证方案与审核记录

按changed files→file_ownership→module_registry→test_plans，运行已有qe_read_backend及直接测试。Windows用例覆盖独立公式、prefix/未来扰动、顺序、股票隔离、单位缩放、缺失、不混桶、独立源码、跨Python格式、逐次模型覆盖不污染配置。

- 初期29→41测试，扩展后60通过；模型与DEV相关性新增3项，相关文件17项通过。
- qe_read_backend已347 passed/1 skipped（最后三项新增前）；最终HEAD测试单独记录，不冒用旧HEAD。
- WSL Ubuntu rdagent-gpu Python3.10.19完成真实全历史计算；独立源码和原输出逐值完全一致，依表顺序为3372619/3372619/3107348/7823697/7544204/6908142/7537948/7825964/7538286/7542407行。
- 多轮修正：执行器标准列value；换桶指数收益；NaN JSON诊断；独立源码避免其他公式干扰分类；跨Python unparse；DEV分类模型与生产一致。
- 真实artifact根X:/AIstock_factor_research/rotation-ten-20260907/evaluation含metrics.json、correlations.json、源码/H5、source-specialization-proof.json、dev-library-specialized-validation.json和dev-correlation-validation.json，不冒充CI fixture。

## Design Acceptance Index / 设计验收矩阵

矩阵判断源码与DEV交付。生产应用必须经过源码合入，未执行前不宣称用户目标全部完成。

- F-001：已批准3+7公式。
- F-002：PIT、无未来数据、缺失不填。
- F-003：真实全历史及五窗口h1/h20。
- F-004：增量相关性及显式不可用。
- F-005：入库代码和DEV指标/分类/相关性回读。
- F-006：默认消费不变及显式补充目录。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_alpha_candidates/rotation_index_factors.py、scripts/qe_alpha_candidates/rotation_liquidity_factors.py | backend/tests/quantevolver/test_rotation_index_factors.py；backend/tests/quantevolver/test_rotation_liquidity_factors.py | pass | 无 |
| F-002 | scripts/qe_alpha_candidates/prepare_rotation_index_context.py | backend/tests/quantevolver/test_rotation_index_factors.py | pass | 无 |
| F-003 | scripts/qe_alpha_candidates/run_rotation_ten_research.py | artifact: X:/AIstock_factor_research/rotation-ten-20260907/evaluation/metrics.json | pass | 无 |
| F-004 | scripts/qe_alpha_candidates/run_rotation_ten_correlations.py | artifact: X:/AIstock_factor_research/rotation-ten-20260907/evaluation/correlations.json | pass | 无 |
| F-005 | scripts/qe_alpha_candidates/validate_rotation_ten_library_dev.py | artifact: X:/AIstock_factor_research/rotation-ten-20260907/evaluation/dev-library-specialized-validation.json；artifact: X:/AIstock_factor_research/rotation-ten-20260907/evaluation/dev-correlation-validation.json | pass | 无 |
| F-006 | backend/services/quantevolver/backtest_base_data_memory_cache.py、backend/services/quantevolver/official_factor_batch_compute_service.py | backend/tests/quantevolver/test_rotation_index_factors.py；backend/tests/quantevolver/test_official_factor_batch_compute.py | pass | 无 |

## DESIGN-COMPLIANCE-001

1. 不简化交付：十公式真实计算、官方指标/相关性/DEV回读完成；生产未执行，不说已经入生产库。三个指数因子仍需显式补充目录。
2. 不静默错误：弱信号、高相关、degenerate_nan/no_valid_pairs、初次LLM失败均公开；最终不是规则fallback冒充LLM。
3. 不改变业务逻辑：3+7来自用户确认；不改数据集、策略、标签、评级、默认批处理和模型配置；负方向只报告，不事后翻转。
4. 不私增门禁：不新增IC/资源/冻结/大型哈希任务；仅既有DEV-first、审核、生产授权顺序。弱因子不触发股票删除或旧因子禁用。

## 生产门禁与后续步骤

- DEV DML：本批十个ID通过；生产DML=not_started；DDL=noop。
- 既有顺序：源码审核→用户授权合入→merge commit确认→production目标preflight→十条目录/50指标/970月度/10分类及新相关性→回读。目前无本批合入授权，不自行merge。
- DEV脚本拒绝production。生产复用正式服务及DEV验证内容，不复制DEV IDs、不覆盖旧代码、不伪造无效相关性。生产5875对仍需按执行时catalog映射写入。
- runtime按实际文件推导backend/backend-main，restart owner=user。源码合入后长期后端加载新参数需用户重启；离线fresh process无需重启。
- production_ddl_gate=noop；dependency install=noop；dataset mutation/export/activation=noop；backend/worker/scheduler process control=noop。
- 不需重导出数据集。生产入库、持久源码资产和API分类/指标回读完成后才声明十因子已加入生产库。

最终L0审核触及factor_analyst既有四处异常直接返回None及一处except/pass；本轮仅补充明确的不可用告警（记录异常类型、不输出凭证），保留可选统计语义，没有新增阻断或改变正常路径。新增失败路径测试证明不伪造指标、不泄露连接详情。

提交前最终审核：qe_read_backend 355 passed/1 skipped；此后仅不可用告警变更，相关文件20项定向测试通过；Ruff、Windows与WSL Python3.10 py_compile、git diff --check通过。模块ownership 18/18 mapped，validation_module_registry_l0 8 passed；L0质量检查0 findings，基线扫描25项、blocking=0；F1设计6项验证PASS。正式本地逐项审核无新增阻断发现。PR/CI、最终HEAD及生产应用是独立状态，以实时回执为准，不将此记录冒充最终HEAD测试。
