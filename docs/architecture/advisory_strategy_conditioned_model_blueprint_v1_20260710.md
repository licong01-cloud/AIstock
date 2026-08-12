# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v3.0

> 初始日期：2026-07-10
> 修订日期：2026-08-13
> 文档类型：F2 顶层架构蓝图，`docs-fast-update`
> 当前状态：`P0A_P0D_SOURCE_COMPLETE_REAL_META_LABEL_TRAINED_PENDING_MERGES_AND_FORWARD_ACTIVATION`
> 当前能力基线：Top5、收益/周期、价格范围和页面/API 四类组件已由真实模型实现；但只完成固定历史日期的按需影子推理，尚未形成任何每日 `PUBLISHED` 推荐、前向模型 observation 或持仓 episode。因此不再用 `4/5 = 80%` 表示总体可用进度
> 当前源码/运行时：M0-M5C 源码均已进入 `main`；PR #3346 已于 2026-08-12 squash 合入为 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`。M2/M3/M4 仅对目标多 Alpha Program 的 `decision=2026-07-15 / target=2026-07-16` 完成按需 GET readback；这不是每日发布或前向运行验证
> 当前生产前向状态：两个 Program 自 2026-07-17 为 `ENABLED` 且配置 `daily_after_close`，但截至 2026-08-12 均为 `last_review_trade_date=null`、`entered_episode_count=0`、`active_count=0`、`metric_status=NO_EPISODES`、无 `PUBLISHED` list version；现有 `2026-07-16` 记录均为 `REPLAY`
> 当前模型质量：M5A/M5B/M5C 三项旧实验均不建议激活；P0-D policy-aligned meta-label 已完成 168 个 CPCV path-trials，winner 相对 matched Selection Top5 提升 `3.6556 bps`、path win rate `64.29%`，但 PBO `0.40` 且 AUC `0.5142`，仅为未激活 experimental challenger
> 演进方向：先建立无资金、无下单的每日前向发布与 champion/challenger 跟踪，再把纯 Top20 重排主线调整为 policy-aligned meta-label `take/skip/confidence`；CPCV/PBO、自适应 conformal 和跨包共享均为结果驱动的研究方法，不是新增审批或运行门禁
> 唯一当前目标：使用已有 QE H5/Parquet/Qlib Bin 基础数据和已有预测 PKL，在 WSL 完成真实模型训练，并把真实模型预测接入荐股功能
> 最终决策者：用户人工决定是否买入；系统不下单、不形成交易执行输入

## 0. 权威边界与本次纠偏

本文档是 AIstock 荐股模型研发的当前顶层权威。用户最新明确要求优先于历史设计、历史实现顺序和旧任务状态。

本次修订纠正过去近三周的严重优先级偏移：此前开发把 Phase 1R 历史范围任务、Source Catalog、逐窗口哈希、capture/build/SEALED、CAS、lease/fencing、source revision、历史固化和完整证据链放在模型训练之前，导致真实模型和用户可见荐股能力均未实现。该顺序自本修订起废止。

M0-M5C 已完成模型组件、固定日期推理和三轮负面质量实验。自 v3.0 起，后续工作只按以下权威顺序：

1. P0-A 建立每天自然向前运行的 baseline publish、challenger observation 和 episode 跟踪，不回填旧日期。
2. P0-B 同期解除单一目标常量，用 Program active binding 动态解析 exact bundle。
3. P0-C 直接读取现有 QE H5/Parquet/Qlib Bin 和目标策略包预测 PKL，构造 Top5 shadow policy episode 标签与 purged rolling/CPCV 评价。
4. P0-D 在 WSL Conda 训练真实 meta-label 模型并进入 challenger 前向发布；正式预测只读取数据库 decision-cutoff 输入。
5. 前向 residual 自然成熟后执行 P1-A，自有两个以上兼容包的独立 bundle 后执行 P1-B；长期趋势包就绪后执行 P2。
6. 上述真实功能不自动解禁历史证据、历史固化、归档、ModelOps 或通用平台；这些任务必须由用户针对具体目标重新确认。

以下内容不再是模型训练、模型推理、页面展示或模型启用的前置条件：

- Historical Range/Phase 1R 全链路 DML。
- Source Catalog 和逐日、逐来源、逐窗口内容哈希。
- retrospective observation/capture/label bridge。
- 新建 SEALED base snapshot、CAS publish、blob reference、invalidation 或 GC。
- checkpoint、lease、fencing、recovery successor 或 exact retry 证据闭合。
- 旧 batch、旧 artifact root、orphan build 和历史 operation 的处理。
- 2/3/5 年全部窗口、全部消融、全部种子和统计检验完成后才允许首次训练。

已有 Phase 0A、Phase 1、Phase 1R、Phase 0B 源码和历史事实保持只读，不删除、不修复、不迁移、不归档，也不再消耗主线研发资源。

### 0.1 从属设计处置

`docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` 中以下内容已被本蓝图替代，不能继续作为开发或运行依据：

- 用完整 Historical Range/Phase 1R bridge 生成训练输入。
- 以新 SEALED base snapshot 作为 WSL 训练硬前置。
- Batch B 对既有 Advisory capture/build/snapshot 表执行生产 DML。
- 先完成完整 2/3/5 年矩阵、三种子、全量 bootstrap，再进行首次真实训练。
- 没有正式 capability readiness 时禁止显示明确标记的实验性真实模型结果。

该详细设计中仍可复用的内容仅限：SHORT_REBOUND 风格定义、候选特征公式、时间切分原则、WSL 环境约束、LightGBM LambdaRank 历史对照和错误可见性。旧5日标签只保留历史结果；新主线使用本蓝图 P0-C 的 Top5 shadow policy episode 标签。后续代码不得继续调用其已废止的 Batch B 历史证据编排路径。

## 1. Background / 业务目标与当前差距

当前策略包能够产生有序候选。已经完成的模型组件可以对固定历史日期输出 Top5、收益/周期和价格区间，但生产 Program 从未按日发布一期正式推荐，也没有任何 episode 或真实前向结果。后续目标必须同时完成两类互不冒充的能力：

1. **前向运行能力**：对每个 ENABLED Program 按交易日持久化基线推荐、模型 challenger、outcome/价格区间和 episode 结果。
2. **模型增量能力**：主模型保留 selection rank 的方向和候选召回，二级模型学习 `take/skip/confidence`，减少坏候选进入 Top5；纯重排只作为对照。
3. **运营口径一致**：训练标签和最终评价直接复用冻结的 Advisory review policy，反映真实止盈、止损、移动保护、rank exit 和 time stop 后的 episode 净收益。
4. **用户可见能力**：页面同时展示基线与实验模型的当期建议、状态、价格/周期范围和前向表现，不把模型输出写回 Selection、Paper 或模拟盘。

长期趋势策略另行完成长期重排、生存、time-to-hit 和大行情捕获模型，不阻断短反弹主线。

截至 2026-08-12：

| 功能 | 状态 | 完成口径 |
|---|---|---|
| SHORT_REBOUND Top20→Top5 | `PURE_RERANKER_RESEARCH_COMPLETE_NOT_ACTIVATED` | M5A 已完成 45 个 booster 和一次冻结 test；winner 平均 5 日超额收益 `0.0071894`，低于 selection rank 的 `0.0085591`，95% block-bootstrap lift 区间跨 0。纯重排保留为历史基线，不再是唯一质量主线 |
| 预期收益与持股周期 | `M5B_REAL_CALIBRATION_COMPLETE_NOT_ACTIVATED` | 最终 request `advoutcal_ec16422ad1a97040583e5273` 生成 v2 bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` 并通过 exact retry；8/10 binary head 可校准、2 个五日 head 因排序反转明确保持 `UNCALIBRATED`，逐 head solver/版本/迭代/收敛证据完整，holding 仍独立 `UNCALIBRATED`。冻结 test 未显示足以支持激活的校准改善，现行 M3 v1 binding 保持不变 |
| 买入/止盈/止损区间 | `M4_POINT_INFERENCE_VERIFIED` | 四头真实模型、decision-cutoff 未复权投影、dividend PIT 输入、exact binding 和风险边界已贯通；目标多 Alpha Program 的固定日期按需 GET 对 20/20 候选返回完整价格范围 |
| 荐股页面模型展示 | `SOURCE_COMPLETE_POINT_READBACK_VERIFIED` | 页面源码可展示 Top5、五期限收益、概率、MFE/MAE、持股与价格范围；HTTP 和固定历史日期 readback 已验证，但没有每日前向发布数据 |
| 每日前向发布与 episode | `NOT_IMPLEMENTED_ZERO_FORWARD_DAYS` | 两个 ENABLED Program 均无 review、无 `PUBLISHED` list、无 episode；`daily_after_close` 目前只是配置字段，没有 Advisory 自动执行器 |
| 多 Program 模型分发 | `TARGET_MULTI_ALPHA_ONLY` | 当前模型服务通过常量绑定一个目标 Program/package/manifest；第二个 ENABLED 单 Alpha Program 明确返回 `MODEL_UNAVAILABLE`，尚未实现按 active binding 动态加载 |
| LONG_TREND 专家 | `DEFERRED_UNTIL_PACKAGE_READY` | 对应长期趋势包形成稳定输入后训练和接入 |

历史表、schema、证据链、报告、任务状态、artifact 数量和测试数量均不得计入上述功能完成度。

### 1.1 当前进度口径

为避免把“代码存在”“单点可调用”“持续运行”和“模型有效”混为同一完成状态，本蓝图固定使用以下独立维度，不再汇总成单一百分比：

| 维度 | 当前进度 | 准确含义 |
|---|---:|---|
| 模型组件实现 | `4/4` | Top5、收益/周期、价格范围、页面/API 均有真实模型实现 |
| 固定日期按需推理 | `TARGET_MULTI_ALPHA_VERIFIED` | 仅目标多 Alpha Program 在 `2026-07-16` 的 persisted replay 上验证；不是生产前向运行 |
| 每日前向发布 | `0 PUBLISHED DAYS` | 两个 ENABLED Program 均无正式发布日期和 review 状态 |
| episode 前向跟踪 | `0 EPISODES` | 无 active/closed episode，暂无真正未来 OOS 指标 |
| 多 Program 模型覆盖 | `1 TARGET PROGRAM` | 单 Alpha Program 尚无动态 bundle；基线荐股不应因此受阻 |
| 模型质量升级 | `0/3 建议激活` | M5A、M5B、M5C 都已完成真实冻结 test，但均未满足替换现行基线的研究结论；不是代码失败，也不能冒充质量成功 |
| 长期趋势模型 | `NOT_STARTED` | 长期趋势原生多 Alpha 父包尚未形成可训练输入 |

PR #3346 已于 2026-08-12 合入 `main`，merge commit 为 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`。M5C bundle 未激活，M4 v1 price-range binding 保持不变；源码合入不等于 bundle 激活、每日发布或前向验证。

### 1.2 真实训练与实验数据总表

下表只记录真实冻结输入、真实模型或真实校准运行；mock、fixture、历史证据平台和测试数量不计入模型实验：

| 阶段 | 冻结输入与产物 | 样本/模型 | 资源 | 冻结 test 或真实运行结果 | 当前结论 |
|---|---|---|---|---|---|
| M0 可训练矩阵 | request `advmreq_ac5959aa8dc14a25e3b8c139` | 406 decision dates；8120 个 Top20 候选；6960 行冻结 features | 文件只读构建 | 训练/验证/test 的父输入身份已冻结，基础行情截止 `2026-06-30`，候选共同范围 `2024-07-04..2026-03-10` | 可训练输入已完成，不再扩建历史数据平台 |
| M1 首个 reranker | bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629` | train 3818 行/191 日；validation 1139 行/57 日；test 1599 行/80 日；Top5 400 行 | 128.921 秒；RSS 2,262,388,736 bytes | model Top5 5日平均超额 `-0.0002833`、命中率 `0.5025`、NDCG@5 `0.26570`；selection rank 为 `0.0085591`，HMM 为 `0.0040167`，随机为 `0.0055652` | 真实模型已接入 shadow，但原始质量明显低于基线 |
| M3 outcome/holding | bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` | 46 个 LightGBM heads；test 1600 行/80 日；1/3/5/10/20 日 horizon | 108.128 秒；RSS 655,581,184 bytes | 五期限预测零 NaN；5日正超额 head AUC `0.53469`、Brier `0.25186`；holding accuracy `0.36261`、bucket-day MAE `10.1374`、range coverage `0.75031` | 功能和运行时已贯通；原始概率与周期分布保持实验/未校准语义 |
| M4 entry/risk ranges | request `advprreq_2d826a7b2704137bf3a60d9d`；bundle `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3` | 4 个 heads；test 1600 行/80 日；1599 个 executable gap；8120 行中仅 4 个 binary 负例 | 13.85 秒；RSS 491,802,624 bytes | q10-q90 coverage `0.727955`，零 quantile crossing；真实多 Alpha readback 20/20 返回买入、止盈、止损和移动保护范围 | M4 v1 当前继续激活；binary 必须保持 `UNCALIBRATED` |
| M5A Top5 tournament | train request `advm5train_a64594d6f22f618a4afef84a`；test request `advm5test_818fe5a6c8ee323d2fbf25d4`；bundle `1757b24b854f8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | 45 trials、37 candidates；winner 为 5-seed `EXPANDING_ALL__LAMBDARANK_NDCG5__MW_0.75`；test 400 个 Top5/80 日 | tournament 18.925 秒、RSS 409,169,920 bytes；test 2.131 秒、RSS 342,462,464 bytes | winner 5日平均超额 `0.0071894`、命中率 `0.5425`、NDCG@5 `0.34150`；selection rank 为 `0.0085591`、`0.5375`、`0.32399`；均值 lift `-0.0013696`，95% block-bootstrap `[-0.0093061, 0.0053392]` | 相比 M1/HMM/随机有改善，但未证明超过现行 selection rank，不激活 |
| M5B outcome calibration | request `advoutcal_ec16422ad1a97040583e5273`；bundle `a2dea5157f1b768dff42ea844f7dc5a2d31563652967a6535adf89b228bd5533` | validation 940 feature-covered/1000 labels；test 1600/1600；8/10 binary heads calibrated，2 个 h5 heads 因 order reversal 保持 raw | 11.659 秒；RSS 399,200,256 bytes | 8 个 calibrated binary head 的 test Brier/logloss/ECE 均未优于 raw；收益区间名义 coverage 平均绝对偏差 `0.00984 -> 0.03129`；path upper `0.01432 -> 0.01394` | artifact 完整、exact retry 一致，但总体质量不支持激活；M3 v1 binding 不变 |
| M5C entry-gap calibration | request `advprcal_7cb766fe38898e12a008a328`；bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead` | validation 940 feature-covered/1000 eligible；test 1599/1599；central-80 CQR | 2.679 秒；RSS 395,853,824 bytes | validation coverage `0.810638` 导致 `delta=0`；test raw/calibrated coverage 均 `0.727955`，mean width 均 `0.0122280` | 全局常数校准无法修正 validation→test 漂移，`activation_recommended=false`；源码已合入但不激活 |
| 生产前向基线 | 两个 ENABLED Program；各有一条 `2026-07-16` REPLAY | `PUBLISHED=0`；review=0；episode=0 | 非训练项 | `NO_EPISODES`，无 win rate、无 latest recommendation、无 forward outcome | 生产前向闭环尚未开始，不能据此判断模型未来质量 |
| P0-C policy dataset | bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd` | 386 candidate days；7,720 candidates；7,716 matured labels；28/28 READY CPCV paths | 28.9 秒；RSS 1.72GB | take 4,199 / skip 3,517；holding median 6 days；Selection rank buckets 均约 54% take rate | policy-aligned 标签和评价输入已完成；PR #3367 未合入 |
| P0-D meta-label | request `advmetareq_cf90fa2c84d77352c5f8898b`；bundle `20d662860e053c70fb817fe7d0a3f28d09790d2a17cb6b9a8a51c41b492713c8` | 2 families × 3 seeds × 28 paths = 168；winner `FAMILY_CORE_HMM/20260817` | 312.222 秒；RSS 2.80GB；exact retry 4.578 秒 | winner `19.4357 bps` vs Selection `15.7801 bps`，lift `+3.6556 bps`，path win rate `64.29%`，PBO `0.40`，AUC `0.5142` | 真实模型已训练，保持 `EXPERIMENTAL_MODEL/UNCALIBRATED/NOT_ACTIVATED`；源码待 PR/合入 |

### 1.3 方向一致性复核

当前实现保留了正确的工程边界，但目标架构尚未完成：

- 训练只读取 QE H5/Parquet/Qlib Bin 和既有预测产物，正式推理才读取数据库当前行情；M5C 不读取数据库历史训练数据。
- M1、M3、M4、M5A、M5B、M5C 均执行真实 WSL 运行，没有用规则、随机、mock 或缩样本冒充完成。
- Selection、StrategyPackage、Paper、模拟盘和 QE 资产业务逻辑未被 M5C 修改；模型失败或未激活不阻断现有荐股基线。
- 没有新增角色、审批、二次策略包准入、ModelOps、历史证据、旧 batch/root 处理或通用缓存平台。
- 真实负面实验结果均保留，没有把 artifact 发布、源码合入或 API 可返回误写为模型效果成功。
- 当前模型推理是独立 GET 侧路，并由静态 target binding 限定单一 Program；Program review 不消费模型 Top5，`daily_after_close` 也没有执行器。这两项是待实现功能，不再描述为已完成的“策略条件化运行时”。

下一阶段的最小必要架构工作只有两项：每日前向发布/episode 跟踪，以及按 Program active binding 动态解析 bundle。它们直接产生模型未来质量证据，不属于被禁止的历史证据平台。模型方向同时从纯重排转向 policy-aligned meta-label；M5A/M5B/M5C 暴露的 selection-prior、分布漂移和近乎单类标签问题作为对照事实保留。

## 2. Scope / 当前实施范围

本蓝图当前只覆盖直接产生真实模型和荐股能力的工作：

- 读取已有 QE H5/Parquet/Qlib Bin 基础数据和已有模型预测 PKL，并核对可用于训练的字段。
- 从现有文件构造候选级训练矩阵、标签和时间切分。
- WSL Conda 中训练 LightGBM 排序、分类、分位数和生存模型。
- 对真实留出区间产生预测并与现有候选排序对比。
- 在正式预测时从数据库读取当前/实时行情、行业、资金、HMM、停牌、ST和可交易性输入。
- 使用同一特征定义完成训练文件与数据库预测输入的 schema parity。
- 每个 Advisory Program 独立运行；一个 Program 绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- ENABLED Program 按交易日自动执行基线 review，持久化 `PUBLISHED` list version 和 episode；模型存在时同时持久化 challenger observation，模型不存在时基线照常发布并返回 typed unavailable。
- 训练以冻结 review policy 下的 episode 净收益构造 meta-label，模型只输出 `take/skip/confidence` 和 Top5 研究 shortlist，不形成自动下单或资金仓位。
- train/validation 内使用 purged rolling/CPCV 或样本规模允许的等价时序重采样，报告 trial 选择偏差；已经读取的冻结 80 日 test 不再用于方向、参数或阈值选择。
- 在荐股页面展示 Top5、收益范围、持股周期和价格区间。
- 模型不可用、字段缺失和版本不兼容时错误可见，但不得阻断现有规则荐股基线。

## 3. Non-goals / 明确禁止

以下任务持续禁止进入当前主线。每日前向发布、模型 challenger observation 和 episode 跟踪是当前核心产品功能，不属于历史证据平台、归档或 ModelOps，明确不在禁止范围内：

- 历史数据证据链建设、历史数据固化、历史归档和旧任务修复。
- Source Catalog、逐窗口 hash、全历史 lineage、source revision union 或历史 correction E2E。
- 新建通用 observation/capture/label/snapshot 数据平台。
- 为训练重新执行多年 Historical Range/Phase 1R 业务任务。
- 为训练向 Advisory 历史业务表写入候选、列表、Outcome、Summary 或 bridge DML。
- 处理旧 PARTIAL/RUNNING batch、旧 root、orphan artifact 或遗留状态。
- 自动重训、通用 ModelOps、自动模型激活、canary 发布平台、通用漂移治理和灾备。仅允许当前 P0 所需的 Advisory 收盘后执行器和基线/challenger 前向对照。
- 新增角色、审批、授权流、人工放行、策略包二次准入或运行时 package preflight。
- 改动 Selection、Paper、模拟盘、QMT 或策略包既有业务逻辑。
- 使用 QE 回测组合净值、Paper/模拟盘结果作为训练输入。由 QE 文件行情按冻结 Advisory review policy 确定性模拟的 episode 结果是正式监督标签，不是回测结果跨模块耦合。
- 用 mock、随机输出、规则排序或静态 JSON 冒充真实模型预测。

不禁止读取 QE 实验中已有的 H5/Parquet/Qlib Bin 基础数据，以及 Prediction Store 中的 `pred.pkl`、各腿 seed 预测、`combined_prediction.pkl`、权重和必要模型产物。基础行情、行业、资金和因子历史数据必须来自 H5/Parquet/Qlib Bin；预测、模型参数和其它非基础行情产物允许使用 PKL。所有读取必须只读，不修改 QE 实验资产，也不把 QE 组合回测净值、交易或持仓结果当作模型监督信号。

## 4. 数据权威与防泄漏边界

### 4.1 模型训练数据

模型训练默认且优先使用已有 QE H5/Parquet/Qlib Bin 基础数据和已有 QE 预测 PKL，不从生产数据库重新构建多年历史数据。

允许的训练输入包括：

- 已有 QE H5/Parquet/Qlib Bin 中的日线、复权、成交量、资金、估值、行业、指数、停牌、涨跌停和因子特征。
- 仅使用上述基础数据按本蓝图标签公式派生的收益、MFE/MAE、周期和价格标签；只有 QE `label.pkl` 与目标标签定义逐字段一致时才可直接复用，否则只作为对照，不得偷换标签语义。
- Prediction Store 中目标父包精确 roster 引用的各腿 `pred.pkl`、模型元数据、历史 seed ensemble、`combined_prediction.pkl` 和逐日 weight。允许直接读取 PKL，不要求为形式统一复制成 Parquet。
- 当前父包正式 runtime 每腿只运行代表 seed，并使用 `frozen_backtest_terminal_weights`。首模历史候选必须按两个代表 seed、当前 zscore、terminal weights、raw Top25 和 Program target_count=20 确定性重建；完整 38-seed ensemble、逐日 weight 和 `combined_prediction.pkl` 只作不进入首模特征的显式分布诊断。
- 不得把不同父包、不同演进实验、不同 roster 的“最新腿”临时拼成训练输入，也不得把代表模型结果复制为多 seed 后生成伪离散度。
- Qlib 分钟 Bin；只在明确训练盘中成交概率或价格路径模型时使用，不作为 Top5、收益、持股周期或日线级价格范围模型的前置条件。

训练过程只需记录直接保证模型可加载和特征一致性的最小信息：

```text
training_run_id
input_file_paths
input_schema_version
feature_names and dtypes
label_definition_version
train/validation/test date ranges
model_family and parameters
model_file_path and sha256
code_commit
WSL environment identity
```

禁止为训练输入额外生成逐日 source receipt、逐分区 revision chain、capture membership、SEALED manifest、CAS publication 或历史 correction 记录。

#### 4.1.1 2026-08-07 已验证输入清单

| 输入 | 已验证范围/内容 | 当前用途 |
|---|---|---|
| WSL `/home/lc999/data/qlib_bin` | 日线 `2018-08-01..2026-06-30`；包含 OHLCV、复权、`limit_up/down`、涨跌停价和 `prev_close` | 首模基础特征、标签、HMM重训和日线价格范围 |
| WSL H5/Parquet candidate | `/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2`；日线、基本面、资金、行业、筹码和静态因子 | 首模候选级特征；训练不在 Windows 读取 |
| WSL `/home/lc999/data/qlib_minute_bin` | `2024-01-02 09:30..2026-06-30 15:00`，约33GB，含分钟OHLCV与涨跌停字段 | 仅后续盘中路径模型 |
| Prediction Store | 当前目标父包精确 roster 为 LSTM 33 seed + FUNDGROWTH 5 seed；两个 runtime 代表 seed 与完整 38 个 `pred.pkl` 均存在 | 代表 seed 用于 runtime-equivalent 候选；完整 ensemble 仅作诊断 |
| combine workspace | 406 日 `combined_prediction.pkl`、逐日权重和组合因子文件存在 | 已回测 walk-forward 组合参考，不作为当前 runtime 候选权威 |
| suspend sidecar | `suspend_d_daily_candidate_20180801_20260630/suspend_d.parquet` | 历史停牌状态 |
| 沪深300 | 日线 Bin 内 `000300.SH` 可读 | benchmark和HMM超额收益 |

当前基础数据能够启动真实训练。目标多 Alpha 各腿共同预测范围实际形成 406 个 decision dates：`2024-07-04..2026-03-10`，因此依赖各腿预测的第二阶段模型样本不得超出该共同范围。基础行情和 H5/Parquet 截止 `2026-06-30`，用于完成 `2026-03-10` 候选的未来标签，不代表候选或 HMM 连续状态可以延伸到 `2026-06-30`。任何需要未来收益的训练头都必须按各自 horizon 剔除或右删失尾部未成熟样本；不得把无未来结果的候选当作完整标签。

当前活跃日线 Bin 未包含中证500、中证1000、创业板指或科创50。它们不是首个 Top20→Top5、收益、周期、日线价格范围或现有 HMM 架构的阻断输入；只有后续模型合同明确使用且实验证明有必要时才补充，不预建通用指数库。

M0 已实现 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`：代表 seed + current zscore + terminal weights 先生成 raw Top25，再取 Program target_count 前20。它同时绑定 `decision_as_of_trade_date` 和下一交易日 `target_trade_date`；正式特征只能读取前者 cutoff。真实文件得到 406 日、8120 个候选且每日深度固定 20；combined/ensemble 只作为诊断，不进入 runtime-equivalent 候选语义。M1 bundle、M3 outcome bundle 和 M4A price-range bundle 均已生成，后续阶段继续精确绑定这些身份。

### 4.2 正式预测数据

只有正式执行 Advisory 模型预测时才读取数据库中的实际当前/实时数据：

- 当前策略包生成的候选、Alpha rank/score 和原生多 Alpha component evidence。
- 数据库中的当前/实时行情、复权、资金、估值、行业和交易状态。
- 本轮新训练模型产生的当前 HMM预测，以及当前risk policy、ST、停牌、涨跌停和股票池输入。
- 当前 Program、binding 和模型配置。

正式预测不得读取 QE H5/Parquet/Qlib Bin 作为当前行情替代，也不得用训练文件中最后一日数据冒充实时数据。历史 PKL只用于模型训练和离线评价；正式预测的 Alpha/多 Alpha分数必须来自该 Program 当次实际候选和父包组件输出。Advisory 的 `target_trade_date` 与 `decision_as_of_trade_date` 必须分别保存，任何正式数据库特征的 business date 都不得晚于 decision cutoff。

### 4.3 训练与预测 schema parity

训练和预测必须调用同一个特征公式注册表，并分别通过：

```text
QEFileFeatureSource -> SharedAdvisoryFeatureBuilder
DatabaseRealtimeFeatureSource -> SharedAdvisoryFeatureBuilder
```

必须核对特征名、dtype、缺失值语义、单位、复权口径和顺序。schema 不兼容时该模型预测明确失败，现有规则荐股继续运行；禁止静默丢列、补零、换特征或返回基线排名冒充模型结果。

### 4.4 最小防泄漏规则

防止未来数据泄漏只保留直接影响模型正确性的规则：

- 时间切分按交易日执行，训练、验证和测试不得随机混合日期。
- 特征时间必须不晚于预测决策时点。
- 标签只能使用决策时点之后的收益或路径。
- 相邻标签窗口按需要设置 purge/embargo。
- scaler、缺失值统计、行业编码和任何拟合转换只能在训练区间拟合。
- 训练期模型选择不得读取最终测试集结果。

这些规则由训练代码和定向测试验证，不建设独立证据平台。

### 4.5 HMM重新训练与预测边界

HMM是行业状态先验、候选特征和对照基线，不是 Top5 模型的主要监督目标。新模型不得读取旧HMM模型、旧状态序列、旧系数或旧训练结果作为训练输入；这些历史产物仅用于结果对照。

- 保留现有行业级两状态 Gaussian HMM、因果 forward-filter 和状态解释架构。
- 使用当前 QE H5/Parquet/Qlib Bin 中的行业收益、相对沪深300收益、行业成交量和真实 `$limit_up` 比例，从头拟合新HMM。
- 首个 holdout 只在 train 区间拟合 HMM 参数和 observation transform，validation/test 固定参数逐日 forward-filter；状态按 excess-return 均值确定性规范为 BEAR/BULL，不得用 `date.today()`隐式决定窗口。
- 禁止用涨幅大于9.8%的近似值替代 Bin 中已存在的真实涨停标记，以免ST、创业板和科创板语义错误。
- 当前数据库版 `SectorHMMTrainer` 的算法可复用，但训练数据适配必须改为文件读取；正式预测加载与 holdout 相同的参数和文件截止 posterior，并仅追加数据库 decision-cutoff 后续观测执行因果预测。若重拟合 HMM，必须同步重建特征和重训 reranker，不能单独替换。
- HMM重训或预测失败必须显式记录，不得静默复用旧HMM系数，也不得阻断不依赖HMM的现有规则荐股。

## 5. Architecture / 唯一目标架构

### 5.1 训练链路

```text
existing QE H5/Parquet/Qlib Bin base data
  + exact parent-roster prediction PKL
  -> read-only schema and coverage inspection
  -> QEFileFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> candidate groups + frozen review-policy episode labels
  -> purged rolling/CPCV train-validation paths + untouched future forward stream
  -> WSL Conda real model training
  -> validation predictions and trial-selection-bias diagnostics
  -> model file + minimal load manifest
  -> historical research report; no reuse of the consumed 80-day test for selection
```

该链路不创建 Historical Range batch，不写生产数据库，不进入 Phase 1R bridge，也不依赖 Source Catalog 或 SEALED snapshot。

### 5.2 正式荐股预测链路

```text
existing admitted StrategyPackage
  -> current single-Alpha or native multi-Alpha candidate Top20
  -> persisted baseline Advisory review and bounded Top20 list
  -> active binding resolves exact package/style/model bundle
  -> fresh-trained HMM prediction + current regime/tradability semantics
  -> DatabaseRealtimeFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> loaded WSL-trained meta-label bundle when available
  -> take/skip/confidence + Top5 challenger; pure reranker remains baseline comparison
  -> return/holding/price models when available
  -> persisted daily challenger observation + forward outcome/episode maturation
  -> Advisory API
  -> Advisory page
```

模型服务位于 Advisory 消费层，不反写 Selection、StrategyPackage、Paper 或模拟盘。多个 Program 独立执行；运行时不得继续依赖单一 `PROGRAM_ID/PACKAGE_ID/MANIFEST_SHA256` 常量，而应由 Program active binding 精确解析 bundle。没有 bundle 的 Program 仍发布原始基线并显式返回模型不可用。bundle 必须匹配 package/manifest/style/schema，参数不得因显示风格相同而自动跨包共享，候选、排名、列表、observation 和 episode 不能跨 Program 混合。只有后续 matched 实验证明多个策略包兼容时，才能设计显式 compatible-set 的共享模型。

### 5.3 基线连续性

- 模型通道失败时保留当前规则荐股结果。
- 页面必须明确区分 `rule_default`、`experimental_model` 和后续 `validated_model`。
- 禁止把规则结果填入 model 字段。
- 禁止因模型缺失阻断单 Alpha或原生多 Alpha现有荐股。

## 6. 模型功能

### 6.1 SHORT_REBOUND selection + policy-aligned meta-label

现行 selection rank 继续负责方向、候选召回和 Top20 顺序。下一模型主线不再要求在同一信号内纯重排战胜 selection，而是学习基础策略未直接优化的坏候选过滤与置信度：

- group：同一 Program、package、`decision_as_of_trade_date/target_trade_date` 的 runtime-equivalent 候选 Top20。
- 输入：selection score/rank、父包腿分数与腿间分歧、HMM regime、行业状态、流动性、拥挤度、波动、可交易性和决策时可见的价格/资金特征；未来 episode path、MFE/MAE 和退出原因只能构造标签或评价，不能进入当时特征。
- 冻结 policy：生产 baseline 继续使用当前 Top20 Program policy。Top5 challenger 另建显式 `model_shadow_review_policy`，只把 `target_count/rank_enter_threshold` 固定为5，保留当前 `rank_exit_threshold=40`、确认天数、止盈止损、移动保护、20日 time stop、每日替换预算、entry/exit price basis 和成本口径。两者分别持有 hash；shadow policy 只用于研究标签和虚拟 episode，不修改生产 Top20 policy。任一政策变化产生新标签/model identity，不能静默复用旧模型。
- 候选级标签：对 target day 的每个候选独立建立反事实 entry，在既有预测文件中重建后续每日至少 `rank_exit_threshold=40` 的排名并继续追踪已持有 symbol，按同一 review policy 模拟 `rank_exit`、stop loss、trailing take profit 和 time stop，得到 realized episode net excess return、是否优于 skip/cash 以及 confidence target。只有 Top20 而没有后续 Top40/held-symbol rank 时不得伪造 rank-exit 标签。
- 输出：每个候选的 `take_probability`、`skip_probability`、`advisory_model_confidence`、Top5 challenger 和可解释 reason；系统不自动下单，也不生成资金仓位。
- 对照：selection 原始前5、现有 M5A reranker、HMM前5、meta-label Top5、随机5和候选20等权。

现有 M1/M5A 5日 LambdaRank 保留为已完成历史基线，不重写其结果。多期限复合 relevance 只可作为独立 matched 对照，不能用已消费的冻结 test 决定权重。

### 6.1.1 评价与选择偏差

- 当前 406 个 decision dates 和单一 80 日 test 无法证明约 `0.001` 级 lift；该 test 已被读取，后续只保留历史报告。
- 新模型的参数、阈值和候选 family 仅在 train/validation 的 purged rolling/CPCV 路径选择；purge 至少覆盖 review policy 的最长标签窗口，当前默认最长 20 个交易日。
- CPCV/PBO 或适用等价诊断用于报告 45 trials 及后续 family 的选择偏差，不是人工审批门禁，也不制造新的独立 OOS。样本或策略路径不足时显式 `NOT_COMPUTABLE`。
- 真正未来 OOS 以每日 challenger observation 及其成熟 episode 为主；不能因前向天数暂少就回头调已消费 test。
- 候选级 meta-label 指标只衡量 take/skip；最终 Top5 challenger 必须另行按冻结的 shadow portfolio policy 逐日组合重放，包含 target count、replacement budget、持仓继承和现金状态，不能用独立候选收益冒充组合 episode 收益。

### 6.2 预期收益与持股周期

第二优先级使用同一候选训练矩阵，训练：

- 多期限净收益分位数。
- 正收益概率。
- 信号存活概率和建议持股周期范围。
- MFE/MAE 与回撤风险。

第一版优先使用 LightGBM quantile/classification 和离散生存模型。它可以与 reranker 共用特征文件，但输出头和评价指标独立。

### 6.3 买入、止盈和止损区间

第三优先级先完成日线级价格范围，再决定是否增加盘中路径模型：

- 第一版使用日线 Bin 的 OHLC、复权、波动、跳空、真实涨跌停价及收益/MFE/MAE模型，生成明确标记的日线级买入、止盈、保护和止损参考区间。
- 分钟 Bin 只用于后续明确批准的盘中买入时点、成交概率、事件先后和动态路径模型；不得让33GB分钟数据加载阻断第一版价格范围。
- 正式静态区间预测只需要数据库最新日线、昨收和涨跌停；只有输出盘中自适应区间时才读取数据库实时分钟行情。

输出必须是范围，不是保证价格；硬止损和行业黑名单不能被模型覆盖。

M4 executable 二分类头在 8120 行中只有 4 个负例，当前定义下不可学习。该头保持 `UNCALIBRATED` 并停止重复校准；下一轮只能先重审标签语义，或直接退役该二分类输出，连续 entry-gap quantile 和风险边界不受影响。

### 6.3.1 区间在线校准

M5B/M5C 已证明 validation 上拟合的全局常数无法处理 test 分布漂移。Adaptive Conformal/rolling calibration 仅作为前向标签成熟后的 matched 候选：按 Program、模型版本和区间头消费历史已成熟 residual，比较 raw/static/rolling coverage 与 width；没有成熟 observation 时保持原始 `UNCALIBRATED`，不得以规则区间冒充校准结果。

### 6.4 训练资源边界

- 所有训练只在WSL Conda环境运行，Windows只负责触发、读取结果和正式在线推理。
- 单个训练进程峰值内存必须低于8GB；基础数据按日期、股票和列投影分批读取，不同时全量加载多个H5或33GB分钟Bin。
- 允许把本次训练所需切片写成临时Parquet并结合内存缓存；禁止为此建设SQLite历史证据库、通用缓存平台或长期数据固化链。
- 首个真实训练目标在小时级完成。超过目标时先定位I/O、特征构建或训练瓶颈并做批处理优化，不得转向额外基础设施研发。

### 6.5 LONG_TREND

长期趋势策略包形成稳定候选文件后，复用同一文件训练和实时数据库预测架构，独立训练20至180日重排、有序收益、生存、time-to-hit和趋势捕获模型。LONG_TREND与SHORT_REBOUND不得共用同一个标签头。

## 7. 荐股产品行为

### 7.1 多策略包

- 一个 Advisory Program 绑定一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。
- 同时支持多个 Program 独立运行。
- 不恢复页面内手工多策略包融合。
- 策略包进入系统时已经完成准入；Advisory 不做二次资产、因子、模型或可执行性验证。
- Program active binding 是模型 bundle 解析的唯一运行身份；单 Alpha 和原生多 Alpha 使用相同解析流程，一个 Program 缺模型不得影响其它 Program。
- 当前仅目标多 Alpha Program 有模型 bundle；“支持多个 Program 基线荐股”和“多个 Program 都有模型覆盖”必须分开报告。

### 7.2 实验模型展示

真实模型完成后可以立即在研究页面显示，状态固定为：

```text
EXPERIMENTAL_SHADOW
training_source = QE_FILE
calibration_state = UNCALIBRATED or PARTIAL
```

实验状态不得描述为已校准概率或确定性收益，但不能因为尚未完成正式OOS、全量统计检验或ModelOps而隐藏真实模型排名。

### 7.3 Top5与每日列表

- Top5 shortlist是模型研究输出，不自动把现有 `target_count=20` 缩成5。
- 模型启用前不覆盖 `selection_effective_rank`。
- 每日活跃列表必须保持有界，显式记录 `ENTER/HOLD/EXIT/WATCH`；不得将每日候选简单并集。
- 是否把正式 active target 从20迁移到5，由用户在看到真实影子模型结果后单独确认。

### 7.4 每日前向发布与 episode

- `daily_after_close` 必须对应真实执行器，而不是仅保存配置。交易日 D 收盘且 Selection 输入就绪后，执行器为每个 ENABLED Program 生成 `decision_as_of_trade_date=D`、`target_trade_date=next_trading_day(D)` 的 baseline/challenger 推荐。不得读取 target 日行情或假设 target 日已经成交。
- `PUBLISHED` recommendation 与 episode entry 分阶段：D 收盘先发布目标日建议；到 target 日获得权威 `next_open_executable` 后，才按 Program 的 entry price basis 建立 episode。target 日价格缺失时保持 `WAITING_DATA`，不得回退 signal close 或伪造进入价格。
- 基线 list 始终来自该 Program 的 StrategyPackage/Selection 结果。模型存在时追加独立 challenger observation，不改变 `selection_effective_rank`、target count 或正式 episode 的当前基线语义。
- challenger 至少保存 Program/binding/package/model identity、decision/target 双日期、候选、take/skip/confidence、Top5、outcome/价格范围和 typed status。不得把按需 GET 响应冒充已持久化前向事实。
- baseline episode 仅由正式 Program review policy 推进；模型 challenger 的虚拟 episode 必须使用独立身份和冻结的 Top5 `model_shadow_review_policy` 模拟，不能混入基线排行榜或 Paper/模拟盘持仓。
- 运行失败必须记录具体 Program、日期、阶段和 reason；一个 Program 失败不阻断其它 Program，不能静默跳过并继续显示旧日期为最新结果。
- 本功能无资金、无下单、无 QMT 输入，是荐股研究的前向质量评价，不属于实盘交易执行。

## 8. Contracts / 最小实现合同

当前只允许实现直接支撑真实模型的合同：

| 合同 | 必需内容 |
|---|---|
| `QETrainingDatasetDescriptorV1` | H5/Parquet/Qlib Bin基础数据路径、Prediction Store PKL引用、目标父包roster、schema、日期、features、labels和split |
| `AdvisoryFeatureSchemaV1` | 训练/预测共享的特征名、dtype、单位和缺失值语义 |
| `AdvisoryTrainingRequestV1` | model family、参数、seed、WSL环境、输入和输出路径 |
| `AdvisoryModelBundleV1` | 模型文件、feature schema、label version、训练区间、指标和SHA256 |
| `AdvisoryPredictionV1` | program/package、decision/target双日期、候选、model score/rank、Top5、状态和reason |
| `AdvisoryOutcomePredictionV1` | 收益分位数、正收益概率、周期、MFE/MAE和状态 |
| `AdvisoryPriceRangePredictionV1` | entry/take-profit/protection/stop范围、单位、置信状态和reason |
| `AdvisoryForwardObservationV1` | Program/binding/package/model、decision/target双日期、baseline/challenger、Top5、预测、状态和成熟截止日 |
| `AdvisoryPolicyEpisodeLabelV1` | baseline或Top5 shadow policy/hash、entry/exit basis、成本、退出原因、持有期、净收益/超额收益、label maturity和censoring |
| `AdvisoryModelBindingResolutionV1` | active Program binding 到 exact package/style/schema/model bundle 的确定性解析；无 bundle 为 typed unavailable |

若现有 Program/list/episode/API 能承载字段，优先复用；challenger 与基线身份无法无歧义共存时才允许增加最小 Advisory 专用存储。禁止为未来通用性预建模型注册中心、训练调度表、血缘仓库、审批表、历史状态机或新 artifact 平台。

## 9. Implementation Plan / 唯一后续顺序

### M0：QE文件可训练性核对

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED`。基础数据、Prediction Store、涨跌停、分钟边界、冻结 request 合同和 runtime-equivalent candidate coverage 已核实并实现；正式 request 为 `advmreq_ac5959aa8dc14a25e3b8c139`，真实文件训练覆盖 406 日、8120 个 Top20 候选。

- 绑定当前目标父包精确 roster、两个代表 seed/model SHA、zscore、terminal weights、raw Top25、Program target_count 和 runtime semantics hash。
- 完整 38 seed、逐日权重和 `combined_prediction.pkl` 只生成显式对照诊断，不进入在线 feature，也不替代 current runtime candidate。
- 冻结 decision/target 双日期和 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`，与正式预测入口保持一致。
- 绑定当前 QE H5/Parquet/Qlib Bin 基础数据，不复制或转换合法预测PKL。
- 只读核对日期范围、候选/特征/标签、缺失率和时间切分可行性。
- 选择覆盖最完整的一个真实训练范围。
- 冻结5日标签、最多5日延迟退出和10交易日 purge，显式排除各 horizon 尾部未成熟或跨 split 边界样本。
- 若首模合同必需字段缺失，继续只读搜索已有QE H5/Parquet/Qlib Bin或Prediction Store；仍不存在时报告精确阻断并停止M1，禁止静默简化特征或转向历史数据库证据工程。

完成判定：产生一个可由WSL读取的真实训练请求，不要求新数据库DML或新历史snapshot。

### M1：首个真实Top20→Top5模型

历史优先级：`COMPLETED_BASELINE`。

状态：`COMPLETED_EXPERIMENTAL_SHADOW`。WSL 真实训练生成 bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`，80 日 test 均有 Top5，峰值 RSS 约 2.11GB，总耗时约 129 秒。真实 test 平均超额收益低于原始排名与随机对照，因此 M1 完成时未激活；M2 随后为验证完整真实链路发布了该 exact shadow binding。运行时可用不改变其负面质量结论，也不得被隐藏或描述为优化成功。

- 实现QE文件reader、共享FeatureBuilder和WSL launcher/trainer。
- 使用当前文件数据从头训练HMM，固定状态映射并生成可从文件 cutoff 连续追加数据库观测的 posterior；旧HMM结果只进入对照报告。
- 在WSL训练真实LightGBM LambdaRank。
- 在时间留出集生成逐日Top5和基线对照。
- 保存可加载模型文件和最小manifest。

完成判定：真实reranker与本轮重新拟合的HMM均生成可加载模型、连续续推状态和非空留出预测；旧HMM产物未作为输入；Top5留出预测非空且无未来数据泄漏。shadow 使用与 holdout 相同的 HMM/reranker 参数，不单独 refit HMM。HMM失败不影响现有规则荐股，但不能把HMM增强对照标记为完成。

### M2：固定日期按需影子推理与页面

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。PR #3225 已合入；重启后运行时 commit 为 `41504a205b9372a4e709587dc2310fd8143c6c6d`。目标多 Alpha Program 在 `decision=2026-07-15 / target=2026-07-16` 返回 20 个真实候选、5 个模型 shortlist、exact bundle 和 11 个显式 HMM unavailable；单 Alpha Program 返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`。该 GET 依赖既有 REPLAY list/review，只证明单点推理；它没有生成 `PUBLISHED` list、调度执行记录或 episode。

- 实现数据库 decision-cutoff 特征source；目标交易日数据不得进入特征。
- 对当前单 Alpha或原生多 Alpha候选执行模型推理。
- API返回真实model score/rank/Top5。
- 页面展示`EXPERIMENTAL_SHADOW`结果和基线对照。

完成判定：从 persisted replay 候选、Advisory decision/target context 和数据库 decision-cutoff 行情到页面 readback 贯通；不修改 Selection、Paper 或模拟盘。该判定仅关闭 M2 单点推理，不关闭每日前向发布。

### M3：预期收益与持股周期

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`COMPLETED_POINT_INFERENCE_NOT_FORWARD_RUNTIME`。详细设计为 `docs/architecture/advisory_model_first_m3_outcome_holding_period_f2_design_20260809.md`；M3A bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` 含 46 个真实 LightGBM 模型、1600 行/80 日零 NaN test 预测，训练 108 秒、峰值 RSS 655,581,184 bytes。M3B 随 PR #3234 合入，merge commit 为 `84362027da8f6e87ec5b627a5b7df15b88c5763b`；outcome binding 已存在。运行时 commit `0ab6dec36c6bc05f7d9655de63b07bbd5353dfd2` 对固定日期的 20 个 persisted 候选完成推理，耗时 33.236 秒，五个 horizon 均非空；尚无每日 forward observation 或成熟 outcome。

- 在现有QE文件上训练收益分位数、正收益概率和周期模型。
- 接入同一Advisory预测和页面。

### M4：买入、止盈和止损范围

历史优先级：`COMPLETED_POINT_INFERENCE`。

状态：`M4_POINT_INFERENCE_VERIFIED_NOT_FORWARD_RUNTIME`。详细设计为 `docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`。M4A request `advprreq_2d826a7b2704137bf3a60d9d` 在 WSL `rdagent-gpu` 生成 bundle `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3`：4 个真实 LightGBM heads、1600 行/80 日 test 预测、总耗时 13.85 秒、峰值 RSS 491,802,624 bytes。三个开盘缺口分位数零单调违例，q10-q90 test coverage 为 0.72795。可执行标签共 8120 行但只有 4 个权威负例，test 仅 1 个负例，因此 binary 输出保持 `UNCALIBRATED/EXPERIMENTAL_SHADOW`。M4B 源码、`market.dividend` 数据同步、exact binding 和用户重启均完成；2026-08-11 固定日期 readback 的 20/20 候选返回完整范围，但尚无每日发布或前向 coverage。

- 先使用日线Bin完成真实日线级价格范围模型。
- 盘中路径模型作为后续独立增量，只在用户确认需要时读取现有分钟Bin。
- 接入价格转换和硬风险边界。

### M5：模型质量迭代

历史优先级：`COMPLETED_NEGATIVE_RESEARCH`。

状态：`M5A_M5B_M5C_REAL_RESEARCH_COMPLETE_ZERO_ACTIVATION_RECOMMENDATIONS`。M5A、M5B、M5C 的真实 bundle 和负面质量结果见 §1.2；PR #3346 已合入 `034ccd36dd94441ec8c0fe0f94010d6874b8b799`，但三个研究 bundle 均不激活。冻结 80 日 test 已消费，不得围绕这些结果继续调参。

- M5A 首先改善 Top20→Top5：当前 M1 test `mean_excess_return_5=-0.0002833`，明显低于 `selection_rank_top5=0.0085591`，不得把“模型已运行”误写为“模型排序有效”。
- M5A 在现有 406 日/8120 候选、103 特征和冻结 test 上做有限窗口、种子、模型配置及 selection-prior 混合比较；所有选择只使用 train/validation，test 只做一次最终报告。
- M5A 真实冻结 test 的 winner 平均 5 日超额收益为 `0.0071894`，原始 selection rank 为 `0.0085591`，lift 为 `-0.0013696`，95% moving-block bootstrap 区间为 `[-0.0093061, 0.0053392]`。结果说明 M5A 修复了 M1 明显为负的问题，但没有证明优于原始 selection，当前不替换已激活 binding，也不得围绕 test 继续调参。
- M5B 再处理 M3 概率校准与 quantile coverage；M5C 处理 M4 entry-gap quantile coverage。M4 executable 负例仅 4 条，禁止伪造二分类校准。
- 3/5 年实验只有在同一目标父包的合法既有预测确实存在时才执行，不能用其它实验或新模型回填；不得为 M5 新建历史证据、缓存或 ModelOps 平台。

### P0-A：每日基线发布与前向 observation

优先级：`P0_NOW`。

状态：`NOT_IMPLEMENTED`。

任务列表：

1. 设计并实现 Advisory 专用收盘后执行器，读取交易日和 ENABLED Program，调用既有正式 review 服务；不启动通用 scheduler 平台。
2. 每个 Program/target trade date 幂等产生一个 `PUBLISHED` baseline recommendation；D收盘发布与target日开盘后的 episode transition 使用同一 date context，但分阶段执行，复用现有 bounded `ENTER/HOLD/EXIT/WATCH` 语义。
3. 对有模型的 Program 在同次运行中生成并持久化 challenger observation；没有模型时写入 typed unavailable，不阻断 baseline publish。
4. 保存 decision/target、Program/binding/package/model、baseline/challenger Top5、outcome、价格范围和 maturity date；禁止只保存 GET 临时响应。
5. 页面与 API 分别展示 baseline 最新发布、challenger 最新发布、成熟 forward metrics 和明确失败状态。
6. 完成两个现有 ENABLED Program 的单日真实发布 readback；后续交易日由执行器自然积累，不回填 2026-07-17 以来历史缺口。

完成判定：两个 Program 均存在同一 target 日的真实 `PUBLISHED` recommendation；目标多 Alpha Program 存在同日模型 observation；单 Alpha Program 在无 bundle 时 baseline 成功且模型状态明确不可用。target 日开盘数据到达后，至少一个 episode 被创建，或所有未进入均有可解释的 `WAITING_DATA/NOT_ENTERED`；D收盘发布不得因尚无target open而伪造episode。服务重启和首次生产调度仍由用户分别确认。

### P0-B：动态 Program/package bundle 分发

优先级：`P0_NOW_PARALLEL_WITH_P0_A`。

状态：`STATIC_TARGET_BINDING_ONLY`。

任务列表：

1. 用 Program active binding 替代 `target_binding.py` 中单一 Program/package/manifest 的运行时常量判断。
2. 通过 exact package/manifest/style/schema/model identity 查找 bundle，不扫描 latest、不跨 Program 共享状态。
3. 单 Alpha、原生多 Alpha 使用相同解析合同；不存在兼容 bundle 时返回 typed unavailable。
4. 保持已合入目标多 Alpha bundle 的字节级加载语义和现有 point readback。
5. 不为测试单 Alpha 包伪造模型；该包只有完成独立真实训练后才增加 bundle。

完成判定：目标多 Alpha point/forward 推理不回归，单 Alpha baseline 不受阻，新增真实 bundle 后无需修改源码常量即可被精确解析。

### P0-C：review-policy episode 标签与稳健评价

优先级：`P0_AFTER_P0_A_DESIGN_CAN_RUN_IN_PARALLEL_WITH_FORWARD_ACCUMULATION`。

状态：`NOT_IMPLEMENTED`。

任务列表：

1. 从现有 QE 文件价格和目标父包既有预测 PKL 确定性重放冻结的 Top5 `model_shadow_review_policy`；每个后续交易日至少重建 Top40，并对已持有但跌出 Top40 的 symbol 保留可判定的缺席 rank 语义，生成 entry、rank exit、stop loss、trailing take profit、time stop、成本和 benchmark 对齐后的 episode label。生产 Top20 policy 只作独立 baseline 对照，不被修改。
2. 明确 censoring、label maturity、停牌/涨跌停和缺价语义；不得读取 Paper、模拟盘或生产历史 episode 作为训练输入。
3. 生成 purged rolling/CPCV train-validation paths，purge/embargo 覆盖最长20日政策窗口。
4. 保存全部候选 family/trial 的 validation 路径结果；计算 PBO/等价选择偏差，不能计算时写 `NOT_COMPUTABLE`。
5. 候选级 take/skip 标签与 Top5 shadow portfolio 评价分开：后者逐日执行 target count、daily replacement budget、持仓继承和现金状态。
6. 已消费的80日 test只作为历史对照，不参与阈值、特征、窗口或模型选择。

完成判定：相同 request 确定性得到相同 episode label；边界日无未来泄漏；一个固定 policy 的 selection baseline 与 M5A 历史模型可在同一政策收益口径比较。

### P0-D：SHORT_REBOUND meta-label 模型

优先级：`P0_AFTER_P0_C`。

状态：`NOT_STARTED`。

任务列表：

1. 训练真实 LightGBM take/skip/confidence 模型，消费 selection、策略腿分歧、HMM regime、行业、流动性、拥挤和可交易性特征。
2. 严格使用 P0-C 的 policy episode 标签与 train-validation paths；不使用未来 MFE/MAE/path 作为特征。
3. 同时报 selection Top5、M5A reranker、meta-label Top5、HMM、随机和 Top20 等权的政策净收益、hit rate、drawdown、turnover 和 coverage。
4. 固定模型后进入 P0-A challenger 前向发布；前向样本不足时只标记 `EVIDENCE_IMMATURE`，不回看旧 test 调参。
5. 仅当 validation 多路径与后续前向证据支持时，才提出新的 bundle 激活建议；系统不自动激活。

完成判定：真实 WSL bundle、可重复 validation、PBO/选择偏差说明、每日 challenger observation 和 typed runtime 状态齐全；效果不佳也如实完成实验，不回到平台工程。

### P1-A：outcome/price rolling adaptive calibration

优先级：`P1_AFTER_FORWARD_LABELS_MATURE`。

状态：`WAITING_FORWARD_LABELS`。

- 比较 raw、M5B/M5C static 和 rolling/adaptive conformal 的 coverage、width、Brier/ECE 及分 regime 稳定性。
- 校准状态按 Program/model/head 隔离；无成熟 residual 时保持 raw/uncalibrated。
- M4 executable binary 头停止重复校准，先完成标签重审；连续 gap quantile 独立保留。

### P1-B：策略条件化共享实验

优先级：`P1_WHEN_AT_LEAST_TWO_COMPATIBLE_PACKAGES_HAVE_REAL_BUNDLES`。

状态：`NOT_READY`。

- 先以多个 Program 独立 bundle 为基线，再做 strategy-style/regime conditioning 的 pooled/multi-task matched 实验。
- compatible set、共享参数和 leave-one-package-out 泛化必须显式记录；不得因样本少默认合并异质策略包。
- 只有 matched 结果优于独立模型且无明显负迁移时才提出共享 bundle，不形成自动门禁。

### P2：长期趋势专家

优先级：`P2_WHEN_PACKAGE_READY`。

- 使用长期趋势包对应的现有 QE 文件训练独立模型，并复用 P0-A/P0-B 的前向发布和动态 bundle 分发。
- 标签必须按长期趋势自己的 review policy、生存/time-to-hit 和20至180日目标构造，不复制短反弹5日或20日标签。

### 用户未来单独确认后才可恢复的可选任务

以下任务不属于当前路线；P0-A 的前向运行只保存今后自然产生的业务事实，不解禁任何历史补账：

- 历史证据、历史数据固化和归档。
- Phase 1R完整历史链E2E。
- Source Catalog、SEALED、CAS、invalidation和GC增强。
- 自动训练、ModelOps、漂移监控、灾备和治理后台。
- 旧batch/root/orphan清理或修复。

## 10. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-101 | 基础历史数据只读取现有QE H5/Parquet/Qlib Bin；预测/模型产物允许PKL；训练不读取生产数据库历史数据 |
| F-102 | 真实训练仅在WSL Conda执行，Windows不训练 |
| F-103 | 首模是实际LightGBM模型，不是mock、规则或随机结果 |
| F-104 | M1/M3/M4历史切分按既有合同保留；P0-C新policy标签按最长20日及实际退出窗口重算purge/embargo，不照搬旧5日/25日边界 |
| F-105 | 训练与正式预测共享同一逐列FeatureBuilder/schema、公式、单位、missing和decision cutoff |
| F-106 | 正式预测区分decision/target双日期，只读取数据库decision cutoff行情和实际Program输入 |
| F-107 | 真实模型输出Top5并与原始/HMM/随机/等权基线比较 |
| F-108 | 页面可展示明确标记的`EXPERIMENTAL_SHADOW`真实结果 |
| F-109 | 模型失败不阻断现有荐股，不用基线冒充模型；bundle只按exact shadow binding加载，不扫描latest |
| F-110 | 多个Program的基线荐股独立运行；当前模型覆盖仅限目标多Alpha Program，单Alpha typed unavailable不得冒充模型支持完成 |
| F-111 | 收益、周期和价格范围均来自真实模型；规则只作为单独标记的对照或硬风险边界，不能替代模型结果或伪造概率 |
| F-112 | Selection、Paper、模拟盘、QE资产和策略包业务逻辑零写入 |
| F-113 | 不新增角色、审批、package二次准入或未经确认的门禁 |
| F-114 | 不处理旧历史任务、旧root、归档和非阻碍性平台工作 |
| F-115 | 进度分开报告组件实现、固定日期推理、每日发布、episode、Program模型覆盖和质量净增量；不再汇总为易误读的总体百分比 |
| F-116 | 原生多Alpha首模按当前代表seed、zscore和terminal weights重建runtime-equivalent候选；完整seed/逐日权重/combined只作诊断，不跨实验拼腿或制造伪seed特征 |
| F-117 | 历史涨跌停直接读取日线/分钟Bin中的状态、价格和昨收，不重复建设`stk_limit`训练文件 |
| F-118 | HMM按当前文件数据从头拟合、确定性规范状态并保存可续推posterior；shadow不单独refit HMM，旧模型/状态/系数只作对照 |
| F-119 | 分钟Bin不阻断Top5、收益、周期或首版日线价格范围；只有盘中路径模型明确需要时才消费 |
| F-120 | 首模只要求沪深300；其它宽基指数在模型合同明确需要前不补充、不阻断 |
| F-121 | WSL训练峰值内存低于8GB并以小时级完成首模；采用列/日期/候选分批与临时Parquet，不建设新缓存或证据平台 |
| F-122 | M5A 所有窗口、种子、模型配置和融合权重只由 train/validation 选择；冻结 80 日 test 仅在 winner 固定后评价一次 |
| F-123 | M5A 必须同时报告模型、selection rank、HMM、随机和 Top20 等权基线；质量差如实保留，不用 hidden fallback 或 test 调参 |
| F-124 | selection-prior 混合是显式模型合同并保存权重，不把规则基线冒充模型；多个 Program 仍按 exact package/style binding 隔离 |
| F-125 | M3 概率校准只使用 validation 拟合并独立报告 discrimination 与 calibration；M4 二分类负例不足时保持 `UNCALIBRATED` |
| F-126 | M3/M4 quantile 校准不得读取 test 标签或实时未来行情；模型发布、binding、重启和 deployed readback继续分开报告 |
| F-127 | ENABLED Program 的 `daily_after_close` 对应真实 Advisory 执行器，按交易日幂等产生 `PUBLISHED` baseline list，不回填旧缺口 |
| F-128 | baseline、model challenger 和 replay 身份分离；按需 GET、源码合入、bundle激活、每日发布和前向成熟分别报告 |
| F-129 | challenger observation 持久化 Program/binding/package/model、decision/target、Top5、outcome/价格区间、状态和成熟截止日 |
| F-130 | Program active binding 动态解析 exact bundle；移除单一 Program/package 运行常量限制，无 bundle 时基线继续且模型 typed unavailable |
| F-131 | SHORT_REBOUND meta-label 只学习 take/skip/confidence，不自动下单或生成资金仓位，也不冒充候选召回模型 |
| F-132 | meta-label 使用独立冻结的Top5 shadow policy模拟episode净收益，绑定policy hash、价格基础、成本、退出原因、成熟与删失语义；生产Top20 policy不变 |
| F-133 | 模型选择只使用 purged rolling/CPCV train-validation；冻结80日test不再选择；PBO/等价诊断不可计算时显式记录而不形成审批门禁 |
| F-134 | 真正未来OOS来自每日 challenger observation 和成熟 episode；前向样本少标记证据未成熟，不回看冻结test调参 |
| F-135 | Adaptive conformal 只在前向 residual 成熟后做 matched 实验；M4近单类 executable binary 停止重复校准并先重审标签 |
| F-136 | 跨包共享只在至少两个兼容包有独立真实bundle后做 matched/LOO 研究，不默认共享、不掩盖负迁移 |
| F-137 | 每日前向研究无资金、无下单、无QMT/Paper/模拟盘写入；不新增角色、审批、策略包二次准入或历史证据平台 |
| F-138 | rank-exit标签必须从既有预测文件重建后续至少Top40并处理held-symbol缺席；只有Top20时不得伪造完整review-policy标签 |
| F-139 | 候选级take/skip标签与Top5 shadow portfolio评价分离；后者真实执行target count、replacement budget、持仓继承和现金状态 |
| F-140 | D收盘发布绑定decision=D/target=下一交易日且不读取target行情；episode只在target日权威next-open到达后进入，缺价保持WAITING_DATA且不回退signal close |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | `backend/services/advisory_model_first/qe_file_source.py` | `backend/tests/advisory_model_first/test_qe_file_source.py`; M1/M3/M4 frozen artifacts | implemented_verified | none |
| F-102 | Windows launcher + `scripts/wsl/advisory_*_train.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/training_log.json`; M3/M4同类receipt | implemented_verified | none |
| F-103 | `reranker_training.py`, `outcome_training.py`, `price_range_training.py` | `backend/tests/advisory_model_first/test_price_range_training.py`; `backend/tests/advisory_model_first/test_outcome_training.py`; artifact: M1 `model.txt` | implemented_verified | none |
| F-104 | historical `time_split.py`/`outcome_split.py`; future P0-C policy split | `backend/tests/advisory_model_first/test_outcome_split.py`; artifact: M1/M3/M4 `split.json` | APPROVED_BY_USER_HISTORICAL_VERIFIED_POLICY_SPLIT_DESIGN_READY | none |
| F-105 | `shared_feature_builder.py`, `feature_schema_v1.py` | `backend/tests/advisory_model_first/test_shared_feature_builder.py`; `backend/tests/advisory_model_first/test_realtime_feature_source.py` | implemented_verified | none |
| F-106 | `realtime_feature_source.py` | `backend/tests/advisory_model_first/test_realtime_feature_source.py`; artifact: deployed model-shadow readback for decision `2026-07-15`, target `2026-07-16` | implemented_verified | none |
| F-107 | `reranker_training.py` baseline comparison | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/baseline_comparison.json`; `backend/tests/advisory_model_first/test_candidate_and_contracts.py` | implemented_verified | none |
| F-108 | `backend/routers/advisory.py`; `frontend/src/app/paper-v2/advisory/page.tsx` | `backend/tests/advisory_model_first/test_model_shadow_api.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`; runtime route HTTP 200 | implemented_verified | none |
| F-109 | `model_inference.py`, exact binding loaders | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_model_shadow_api.py`; artifact: typed single Alpha unavailable readback | implemented_verified | none |
| F-110 | Program-level Advisory baseline composition；模型仍静态绑定单一目标 | `backend/tests/advisory_model_first/test_model_inference.py`; artifact: runtime API current-state receipt 2026-08-12 | APPROVED_BY_USER_BASELINE_VERIFIED_DYNAMIC_MODEL_COVERAGE_DESIGN_READY | none |
| F-111 | M3 46 heads; M4 4 heads | `backend/tests/advisory_model_first/test_outcome_training.py`; `backend/tests/advisory_model_first/test_price_range_training.py`; M3/M4 `metrics.json` artifacts | implemented_verified | none |
| F-112 | Advisory-only model-first modules | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-113 | blueprint §§3,14 and model-first error contracts | artifact: `docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`; F2 validator receipts | implemented_verified | none |
| F-114 | model-first import and changed-file boundaries | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-115 | blueprint 分层进度 ledger | artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`; validation-receipt: runtime API current-state 2026-08-12 | APPROVED_BY_USER_CURRENT_TRUTH_VERIFIED | none |
| F-116 | `candidate_group.py`, `prediction_source.py`, `qe_file_source.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; artifact: M1 406-date/8120-row request | implemented_verified | none |
| F-117 | Qlib daily/limit fields and price-range labels | `backend/tests/advisory_model_first/test_labels.py`; `backend/tests/advisory_model_first/test_price_range_labels.py` | implemented_verified | none |
| F-118 | `fresh_hmm.py` | `backend/tests/advisory_model_first/test_fresh_hmm.py`; artifact: M1 `fresh_hmm_models.json` | implemented_verified | none |
| F-119 | M1/M3/M4 daily input contracts | `backend/tests/advisory_model_first/test_qe_file_source.py`; `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | implemented_verified | none |
| F-120 | QE `000300.SH` daily Bin | `backend/tests/advisory_model_first/test_qe_file_source.py`; artifact: M1 `training_request.json` benchmark identity | implemented_verified | none |
| F-121 | WSL pipelines and receipts | artifact: M1/M3/M4 bundle `training_log.json`; M1 peak ~2.11GB/129s, M3 ~0.66GB/108s, M4 ~0.49GB/13.85s | implemented_verified | none |
| F-122 | M5A train/test dual request and winner policy | `backend/tests/advisory_model_first/test_quality_contracts.py`; artifact: `quality_runs/advm5train_a64594d6f22f618a4afef84a/winner_receipt.json`; `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_once_receipt.json` | implemented_real_trained_verified | none |
| F-123 | M5A shared baseline evaluator；冻结 test 结果低于 selection rank 的事实见 §9 | `backend/tests/advisory_model_first/test_quality_evaluation.py`; artifact: `quality_evaluations/advm5test_818fe5a6c8ee323d2fbf25d4/test_report.json` | implemented_real_evaluated_verified | none |
| F-124 | M5A ensemble and selection-prior policy；现行 M1 binding 保留的运行状态见 §9 | `backend/tests/advisory_model_first/test_quality_scoring.py`; `test_quality_runtime_bundle.py`; artifact: bundle `1757b24b854cf8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb` | implemented_bundle_verified | none |
| F-125 | M5B validation-only probability calibration；8 个正斜率 head 发布 calibrated，2 个排序反转 head 明确 uncalibrated；逐 head solver/版本/迭代/收敛证据 fail-closed | `backend/services/advisory_model_first/outcome_calibration.py`; artifact: `outcome_calibration_runs/advoutcal_ec16422ad1a97040583e5273/outcome_calibration_receipt.json`; `backend/tests/advisory_model_first/test_outcome_calibration.py` | real_calibration_verified | none |
| F-126 | M5B outcome quantile calibration和 M5C entry-gap coverage calibration均只使用 validation 拟合；M5C test 零缺失、`delta=0` 且质量未改善 | `outcome_calibration.py`; `price_range_calibration.py`; artifact: M5C bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead`; `test_price_range_calibration.py` | m5b_m5c_real_calibration_negative_quality_verified_not_activated | none |
| F-127 | P0-A Advisory after-close runner + existing review service | P0-A/B task branch；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | production forward activation not executed |
| F-128 | baseline/challenger/replay identity contract | P0-A/B task branch；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | production forward activation not executed |
| F-129 | `AdvisoryForwardObservationV1` | P0-A/B task branch；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | DDL/DML/runtime not executed |
| F-130 | `AdvisoryModelBindingResolutionV1` | P0-A/B task branch；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | descriptor write/runtime activation not executed |
| F-131 | policy-aligned meta-label take/skip/confidence | `meta_label_training.py`; final bundle `20d66286...` | REAL_WSL_TRAINED_EXPERIMENTAL_NOT_ACTIVATED | source PR/merge pending |
| F-132 | `AdvisoryPolicyEpisodeLabelV1` + existing review transition semantics | P0-C bundle `81e2c9ba...`; PR #3367 | REAL_FILE_DATASET_COMPLETE_PR_OPEN_NOT_MERGED | none |
| F-133 | purged rolling/CPCV + PBO/equivalent report | P0-C 28 paths；P0-D 168 trial-path rows/70 PBO partitions | REAL_CPCV_AND_PBO_COMPLETE | PBO 0.40 must remain visible |
| F-134 | daily forward observations and matured policy episodes | `backend/tests/advisory_model_first/test_forward_maturity.py` (target path) | APPROVED_BY_USER_FORWARD_OOS_DESIGN_READY_PENDING_IMPLEMENTATION | none |
| F-135 | rolling/adaptive calibration matched study | `backend/tests/advisory_model_first/test_adaptive_calibration.py` (target path) | APPROVED_BY_USER_P1A_DESIGN_READY_WAITING_NATURAL_FORWARD_LABELS | none |
| F-136 | compatible-set pooled/multi-task experiment | `backend/tests/advisory_model_first/test_strategy_conditioned_pooling.py` (target path) | APPROVED_BY_USER_P1B_DIRECTION_READY_WAITING_COMPATIBLE_PACKAGE_DATA | none |
| F-137 | Advisory-only forward boundaries | P0-A/B boundary tests；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | runtime not activated |
| F-138 | P0-C Top40/held-symbol rank reconstruction | `policy_rank_source.py`; P0-C bundle `candidate_rankings.parquet` | REAL_FILE_RECONSTRUCTION_COMPLETE | source PR #3367 not merged |
| F-139 | candidate meta-label evaluator + shadow portfolio policy simulator | `policy_episode_labels.py`; `shadow_portfolio_policy.py`; P0-D matched baselines | REAL_POLICY_SIMULATION_COMPLETE | source PRs not merged |
| F-140 | after-close publication and target-open episode clock | P0-A/B task branch；PR #3366 | SOURCE_AND_TESTS_COMPLETE_PR_OPEN_NOT_MERGED | production scheduler not activated |

## 12. Verification Plan

### 12.1 训练验证

- QE H5/Parquet/Qlib Bin基础数据和目标父包Prediction Store PKL真实可读。
- feature/label schema和日期范围读回一致。
- 精确roster中的每条腿和seed均能解析，腿间对齐不跨Program或父包。
- 日线Bin的`limit_up/down`、`up/down_limit_price`和`prev_close`可读。
- HMM由本轮文件数据重新拟合，历史HMM产物未进入训练输入。
- WSL身份、Conda环境和LightGBM真实训练日志可见。
- policy episode label 与实际 review transition 对固定样本逐事件一致，policy hash、价格基础、成本和退出原因完整。
- purged rolling/CPCV 的 train/validation 标签窗口无交叉；已消费80日test不进入任何选择输入。
- 模型文件可重新加载并对相同输入产生确定性预测。
- meta-label take/skip/confidence 非空，逐日 group 和 Program/package 边界正确。
- 全部 trial/family 的 validation 结果可读，PBO/等价诊断为数值或明确 `NOT_COMPUTABLE`。
- 峰值内存、各阶段耗时和临时文件规模可见，首模不读取全量分钟Bin。

### 12.2 推理验证

- 数据库当前/实时输入可以生成与训练相同schema。
- 两个 ENABLED Program 各自产生同日 baseline `PUBLISHED` list；一个失败不阻断另一个。
- D收盘发布的decision/target交易日正确，响应和持久化均不含target日行情；target open到达前不创建带伪价格的episode。
- 目标多 Alpha 解析 exact bundle 并持久化 challenger；单 Alpha 无 bundle 时 baseline 成功、模型 typed unavailable。
- 定时运行、手动同日重试和进程重启后的同日重试保持幂等，不产生重复 list/observation/episode transition。
- 模型不可用、字段缺失和版本冲突均有typed reason和有效后台日志。
- 不写Selection、Paper、模拟盘、QMT或QE实验文件。
- forward observation 到期后按同一 policy 形成 outcome/episode label；未成熟项保持 censored/pending，不补零。

### 12.3 页面验证

- 页面展示真实API结果，不使用fixture或静态mock。
- 明确区分规则、实验模型和后续已验证模型。
- Top5、收益/周期、价格区间按已实现能力逐项出现，不等待全部模型完成。
- baseline 与 challenger、REPLAY 与 PUBLISHED、latest success 与 latest failure 分层展示，零 episode 不显示伪造胜率。
- 桌面和移动viewport无重叠、无静默网络错误。

### 12.4 DESIGN-COMPLIANCE-001

每次合入前逐项证明：

1. 没有用简化、mock、规则或静态结果冒充真实模型。
2. 没有静默错误或无日志fallback。
3. 没有改变Selection、Paper、模拟盘、策略包或荐股基线语义。
4. 没有新增未经用户确认的门禁、审批、角色或历史工程。

## 13. Rollout / Rollback

### 13.1 Rollout

下一阶段发布顺序固定为：

1. 完成 P0-A/P0-B 详细设计，复用现有 review/list/episode，确定 challenger 最小持久化和动态 bundle 解析。
2. 合入 P0-A/P0-B 源码；若需要最小 DDL，遵守 DEV-first 和生产具体授权，不扩大为通用平台。
3. 用户单独重启后，对两个 ENABLED Program 执行一个交易日的真实发布 readback，随后由日调度自然积累。
4. 并行完成 P0-C file-based policy episode 标签和 purged rolling/CPCV 评价。
5. 在 WSL 训练 P0-D meta-label bundle，先进入 challenger 前向发布，不自动替换 baseline。
6. 前向标签成熟后再运行 P1-A；兼容策略包具备独立 bundle 后再运行 P1-B。

源码合入、WSL训练、模型文件生成、后端重启、模型加载和页面可见是独立状态，不得合并声明完成。

### 13.2 Rollback

- 关闭目标 Program 的 challenger 配置后保留现有 `selection_effective_rank`、baseline publish 和 `rule_default`。
- 模型加载或推理失败时只关闭模型通道，不停止现有荐股。
- 执行器失败不删除已发布事实；修复后按同一 Program/date 幂等重试，不回填未授权历史日期。
- 不修改或回滚Selection、Paper、模拟盘、StrategyPackage或QE资产。
- 不删除训练文件、模型文件或已产生的预测；只停止继续使用有问题的模型版本。
- P0-A 会产生正常 Advisory 业务写入；这不是一次性修复 DML。若详细设计证明需要最小 DDL，则迁移与回滚另行设计并按 DEV-first 执行。

## 14. Production Gates / 正确性检查与生产影响（无新增业务门禁）

本蓝图不新增业务审批、人工确认门禁或运行门禁。以下仅是普通输入校验和错误可见性要求，不形成独立状态机、审批步骤或运行阻断层：

- 训练文件真实存在且schema可读。
- WSL训练环境真实可用。
- feature schema与模型兼容。
- 正式预测所需数据库字段可用。
- 模型文件可加载且预测结果合法。

输入正确时这些检查必须自动通过；失败必须输出具体reason和日志，不能要求人工审批放行。

默认生产影响：

```text
production_ddl_gate = noop
production_dml_gate = noop for training; daily Advisory publication is normal runtime business write
production_backend_dependency_gate = noop unless WSL dependency is proven missing
production_frontend_dependency_gate = noop unless UI implementation introduces a real dependency
runtime_activation_and_backend_restart = separate user-confirmed actions
```

训练过程不启动、停止或重启用户服务。模型源代码合入、WSL训练完成、模型文件生成、后端加载、页面可见和模型启用必须分别报告，不能合并成一个完成状态。

## 15. 风险与直接处置

| 风险 | 直接处置 |
|---|---|
| QE文件不含目标候选或合同必需特征 | 搜索其它已有QE H5/Parquet/Qlib Bin或Prediction Store PKL；仍缺失则报告精确阻断，禁止静默删列或启动历史证据工程 |
| QE文件标签口径不兼容 | 仅用文件内价格派生标签，或明确阻断对应模型；不读生产历史库补造 |
| 多Alpha预测被跨实验拼接 | 只接受目标父包精确roster、seed和权重；不使用“最新腿”替换 |
| 旧HMM结果污染新模型 | 当前文件数据重新拟合；旧模型、状态和系数仅进入对照报告 |
| 分钟数据拖慢首模 | M1/M3和首版M4不读取分钟Bin；盘中路径作为用户另行确认的增量 |
| 为首模补建宽基指数库 | 只使用现有沪深300；额外指数不作为前置条件 |
| H5固定格式或大Parquet造成内存超限 | 候选/日期/列投影、分批读取和临时Parquet；峰值内存低于8GB，不建设新缓存平台 |
| 训练/预测特征不一致 | 共享FeatureBuilder和schema parity测试，预测失败显式可见 |
| 首模效果不佳 | 保留真实结果并迭代特征/窗口；不回到基础设施扩建 |
| 正式预测缺实时字段 | 只补实际缺失的数据库查询或适配，不扩建通用数据平台 |
| 模型输出被理解为确定结论 | 页面标记实验状态、区间和不确定性 |
| 模型失败影响基线 | 模型通道隔离，规则荐股继续运行 |
| ENABLED但调度未执行 | 页面/API分开显示配置状态和最近成功/失败日期；P0-A以真实PUBLISHED/readback关闭缺口 |
| challenger污染baseline | 分开身份和存储，禁止改写selection rank、baseline list或正式episode |
| 5日标签与20日运营错配 | P0-C按冻结review policy生成episode标签；旧5日reranker只作历史对照 |
| CPCV被误当新OOS | 仅用于train/validation路径和选择偏差；真正OOS只来自未来forward observation |
| adaptive calibration过早 | 没有成熟residual时保持uncalibrated，不用规则或旧test补造 |
| 跨包共享造成负迁移 | 先完成独立bundle，再做compatible-set matched/LOO实验，不默认共享 |
| 再次转向历史闭合 | 以本蓝图F-114和P0-A至P0-D功能清单立即停止该任务 |

## 16. 当前下一步

M0-M5C 的代码、真实 WSL 实验和固定日期推理已形成当前基线；PR #3346 已合入，但 M5A/M5B/M5C 均不激活。生产事实仍是两个 ENABLED Program、零 PUBLISHED 天数、零 episode、零模型前向结果，当前模型只覆盖目标多 Alpha Program。

下一主线严格按以下顺序执行：

1. **P0-A 每日前向发布**：先写详细设计，复用既有 review/list/episode，形成真实 baseline publish 和 challenger observation；不补历史日期。
2. **P0-B 动态 bundle 分发**：与 P0-A 同阶段设计和开发，解除静态 target 常量，仅按 Program active binding 精确解析。
3. **P0-C policy episode 标签与稳健评价**：直接使用 QE 文件和冻结 review policy，在 forward 自然积累期间并行完成；不建设历史平台。
4. **P0-D meta-label 模型**：真实 WSL 训练 take/skip/confidence，以政策净收益和未来 forward evidence 评价；纯 reranker 保留对照。
5. **P1-A adaptive calibration**：只有 forward residual 成熟后执行；M4近单类binary先重审，不再重复静态校准。
6. **P1-B 跨包条件化共享**：至少两个兼容策略包拥有独立真实 bundle 后再做 matched 实验。
7. **P2 LONG_TREND**：策略包就绪后复用前向与动态分发基础，训练独立长期标签和模型。

继续禁止 Historical Range、历史证据、旧 batch/root 清理、通用缓存、ModelOps、角色审批和额外门禁。任何后续任务若不能直接关闭 P0-A至P0-D、P1或P2 的一个明确用户/模型能力，不进入当前主线。
