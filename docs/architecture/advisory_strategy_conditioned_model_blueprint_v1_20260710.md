# AIstock 荐股策略条件化模型体系 F2 架构蓝图 v2.5

> 初始日期：2026-07-10
> 修订日期：2026-08-08
> 文档类型：F2 顶层架构蓝图，`docs-fast-update`
> 当前状态：`MODEL_FIRST_M1_TRAINED_M2_NEXT`
> 当前用户可见功能进度：短反弹真实功能 `0/4`；长期趋势真实功能 `0/1`；离线模型里程碑 M0/M1 `2/2`，尚未计入用户可见功能
> 规划进度：M0/M1 源码与正式 WSL 训练已完成；406 日 runtime-equivalent Top20、110 个 fresh HMM、80 日 test Top5 和原子 bundle 已验收；模型 test 质量低于原始排名与随机对照，保持未激活实验影子状态；下一项直接进入 M2 数据库实时特征、API 和页面 readback
> 唯一当前目标：使用已有 QE H5/Parquet/Qlib Bin 基础数据和已有预测 PKL，在 WSL 完成真实模型训练，并把真实模型预测接入荐股功能
> 最终决策者：用户人工决定是否买入；系统不下单、不形成交易执行输入

## 0. 权威边界与本次纠偏

本文档是 AIstock 荐股模型研发的当前顶层权威。用户最新明确要求优先于历史设计、历史实现顺序和旧任务状态。

本次修订纠正过去近三周的严重优先级偏移：此前开发把 Phase 1R 历史范围任务、Source Catalog、逐窗口哈希、capture/build/SEALED、CAS、lease/fencing、source revision、历史固化和完整证据链放在模型训练之前，导致真实模型和用户可见荐股能力均未实现。该顺序自本修订起废止。

从本修订起，所有后续工作必须遵循以下权威顺序：

1. 读取现有 QE H5/Parquet/Qlib Bin 基础数据和目标策略包已有预测 PKL。
2. 在 WSL Conda 环境完成真实模型训练和真实留出集预测。
3. 将模型推理接入 Advisory；正式预测只读取数据库中的当前/实时行情和相关实际输入。
4. 在荐股页面显示真实模型输出。
5. 依次完成预期收益、持股周期、买入、止盈和止损区间模型。
6. 只有上述真实功能全部完成后，用户重新确认仍有必要时，才可讨论历史证据、历史固化、归档、ModelOps 或通用平台。

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

该详细设计中仍可复用的内容仅限：SHORT_REBOUND 风格定义、候选特征公式、标签公式、时间切分原则、WSL 环境约束、LightGBM LambdaRank 方向、基线对照和错误可见性。后续代码不得继续调用其已废止的 Batch B 历史证据编排路径。

## 1. Background / 业务目标与当前差距

当前策略包能够产生有序候选，但候选前 20 名的原始排序与后续实际收益关系较弱。荐股模型必须完成四项用户可感知功能：

1. 将每个策略包的候选 Top20 重排为质量更高的 Top5 shortlist。
2. 输出预期收益范围、正收益概率和建议持股周期范围。
3. 输出买入、止盈、移动保护和止损参考区间。
4. 在荐股页面展示真实模型结果、模型状态和必要解释。

长期趋势策略另行完成长期重排、生存、time-to-hit 和大行情捕获模型，不阻断短反弹主线。

截至 2026-08-07：

| 功能 | 状态 | 完成口径 |
|---|---|---|
| SHORT_REBOUND Top20→Top5 | `MODEL_TRAINED_NOT_USER_VISIBLE` | WSL 真实训练与留出集预测已存在；Advisory 推理和页面 readback 待 M2 |
| 预期收益与持股周期 | `NOT_IMPLEMENTED` | 真实模型输出分位数、概率和周期范围 |
| 买入/止盈/止损区间 | `NOT_IMPLEMENTED` | 真实模型和价格转换层输出范围 |
| 荐股页面模型展示 | `NOT_IMPLEMENTED` | 页面读取真实预测，不是 mock、规则冒充或静态示例 |
| LONG_TREND 专家 | `DEFERRED_UNTIL_PACKAGE_READY` | 对应长期趋势包形成稳定输入后训练和接入 |

历史表、schema、证据链、报告、任务状态、artifact 数量和测试数量均不得计入上述功能完成度。

## 2. Scope / 当前实施范围

本蓝图当前只覆盖直接产生真实模型和荐股能力的工作：

- 读取已有 QE H5/Parquet/Qlib Bin 基础数据和已有模型预测 PKL，并核对可用于训练的字段。
- 从现有文件构造候选级训练矩阵、标签和时间切分。
- WSL Conda 中训练 LightGBM 排序、分类、分位数和生存模型。
- 对真实留出区间产生预测并与现有候选排序对比。
- 在正式预测时从数据库读取当前/实时行情、行业、资金、HMM、停牌、ST和可交易性输入。
- 使用同一特征定义完成训练文件与数据库预测输入的 schema parity。
- 每个 Advisory Program 独立运行；一个 Program 绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- 在荐股页面展示 Top5、收益范围、持股周期和价格区间。
- 模型不可用、字段缺失和版本不兼容时错误可见，但不得阻断现有规则荐股基线。

## 3. Non-goals / 明确禁止

在短反弹四项真实功能完成前，禁止实施或恢复以下任务：

- 历史数据证据链建设、历史数据固化、历史归档和旧任务修复。
- Source Catalog、逐窗口 hash、全历史 lineage、source revision union 或历史 correction E2E。
- 新建通用 observation/capture/label/snapshot 数据平台。
- 为训练重新执行多年 Historical Range/Phase 1R 业务任务。
- 为训练向 Advisory 历史业务表写入候选、列表、Outcome、Summary 或 bridge DML。
- 处理旧 PARTIAL/RUNNING batch、旧 root、orphan artifact 或遗留状态。
- 自动调度、自动重训、ModelOps、champion/challenger、canary、漂移治理和灾备。
- 新增角色、审批、授权流、人工放行、策略包二次准入或运行时 package preflight。
- 改动 Selection、Paper、模拟盘、QMT 或策略包既有业务逻辑。
- 使用 QE 回测收益、组合净值、交易、持仓、Paper/模拟盘结果作为训练特征或标签。
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

M0 已实现 `OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2`：代表 seed + current zscore + terminal weights 先生成 raw Top25，再取 Program target_count 前20。它同时绑定 `decision_as_of_trade_date` 和下一交易日 `target_trade_date`；正式特征只能读取前者 cutoff。真实文件 smoke 已得到 406 日、8120 个候选且每日深度固定 20；combined/ensemble 只作为诊断，不进入 runtime-equivalent 候选语义。正式训练 bundle 尚未生成，因此 M1 仍不得标记完成。

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
  -> candidate groups + labels + date split
  -> WSL Conda real model training
  -> validation/test predictions
  -> model file + minimal load manifest
  -> historical holdout comparison report
```

该链路不创建 Historical Range batch，不写生产数据库，不进入 Phase 1R bridge，也不依赖 Source Catalog 或 SEALED snapshot。

### 5.2 正式荐股预测链路

```text
existing admitted StrategyPackage
  -> current single-Alpha or native multi-Alpha candidate Top20
  -> fresh-trained HMM prediction + current risk/tradability semantics
  -> DatabaseRealtimeFeatureSource
  -> SharedAdvisoryFeatureBuilder
  -> loaded WSL-trained model bundle
  -> model score/rank and Top5 shortlist
  -> return/holding/price models when available
  -> Advisory API
  -> Advisory page
```

模型服务位于 Advisory 消费层，不反写 Selection、StrategyPackage、Paper 或模拟盘。多个 Program 独立执行；bundle 必须匹配 package/manifest/style/schema，模型参数不得因显示风格相同而自动跨包共享，候选、排名、列表和状态也不能跨 Program 混合。未来只有基于多个包共同训练并在设计中显式声明兼容集合的新 bundle 才能共享。

### 5.3 基线连续性

- 模型通道失败时保留当前规则荐股结果。
- 页面必须明确区分 `rule_default`、`experimental_model` 和后续 `validated_model`。
- 禁止把规则结果填入 model 字段。
- 禁止因模型缺失阻断单 Alpha或原生多 Alpha现有荐股。

## 6. 模型功能

### 6.1 SHORT_REBOUND Top20→Top5

第一优先级模型固定为当前超跌反弹原生多 Alpha 包的 LightGBM LambdaRank：

- group：同一策略包、同一 `decision_as_of_trade_date/target_trade_date` 的 runtime-equivalent 候选 Top20。
- 输入：父包当前两腿代表模型的 raw/normalized score、leg rank、terminal weight、combined rank/score，以及逐列冻结的行情、量价、资金、估值、筹码、行业和市场特征；完整 seed ensemble 的一致度/离散度只作诊断，不进入在线特征。本轮重新训练的 HMM只作为辅助特征和独立对照，不替代主特征模型。
- 标签：文件数据构造的 5 日可执行风险调整超额收益 relevance；延迟退出时股票、benchmark和MFE/MAE使用同一实际退出日，10日purge覆盖最长窗口。
- 输出：`advisory_model_score`、`advisory_model_rank`、Top5和主要特征贡献。
- 对照：原始前5、HMM前5、模型前5、随机5和候选20等权。

首次真实训练只要求选择一个覆盖最完整的现有文件时间范围和一个固定 trainer seed，跑通真实训练、留出预测、模型加载和 Advisory 影子推理。它是正式的首个基线里程碑，不得冒充最终模型结论或 formal forward OOS。

当前合法预测覆盖内的不同时间窗口、额外种子、完整消融和bootstrap属于首模跑通后的模型质量迭代，不得阻塞首次训练与页面实验展示；3/5年窗口只有存在同一目标父包的真实历史预测时才允许执行。

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

若现有API、文件或表能够承载这些字段，优先复用；只有真实功能无法落地时才允许增加最小DDL/API。禁止为未来通用性预建模型注册中心、训练调度表、审批表或历史证据表。

## 9. Implementation Plan / 唯一后续顺序

### M0：QE文件可训练性核对

优先级：`P0_NOW`。

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

优先级：`P0_NOW`。

状态：`COMPLETED_EXPERIMENTAL_SHADOW`。WSL 真实训练生成 bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`，80 日 test 均有 Top5，峰值 RSS 约 2.11GB，总耗时约 129 秒。真实 test 平均超额收益低于原始排名与随机对照，因此 bundle 未激活；该负面质量结论不回溯否定训练功能完成，也不得被隐藏或描述为优化成功。

- 实现QE文件reader、共享FeatureBuilder和WSL launcher/trainer。
- 使用当前文件数据从头训练HMM，固定状态映射并生成可从文件 cutoff 连续追加数据库观测的 posterior；旧HMM结果只进入对照报告。
- 在WSL训练真实LightGBM LambdaRank。
- 在时间留出集生成逐日Top5和基线对照。
- 保存可加载模型文件和最小manifest。

完成判定：真实reranker与本轮重新拟合的HMM均生成可加载模型、连续续推状态和非空留出预测；旧HMM产物未作为输入；Top5留出预测非空且无未来数据泄漏。shadow 使用与 holdout 相同的 HMM/reranker 参数，不单独 refit HMM。HMM失败不影响现有规则荐股，但不能把HMM增强对照标记为完成。

### M2：真实荐股影子推理与页面

优先级：`P0_NEXT`。

- 实现数据库 decision-cutoff 特征source；目标交易日数据不得进入特征。
- 对当前单 Alpha或原生多 Alpha候选执行模型推理。
- API返回真实model score/rank/Top5。
- 页面展示`EXPERIMENTAL_SHADOW`结果和基线对照。

完成判定：从真实策略包候选、Advisory decision/target context 和数据库 decision-cutoff 行情到页面readback贯通；不修改Selection、Paper或模拟盘。

### M3：预期收益与持股周期

优先级：`P1`。

- 在现有QE文件上训练收益分位数、正收益概率和周期模型。
- 接入同一Advisory预测和页面。

### M4：买入、止盈和止损范围

优先级：`P1`。

- 先使用日线Bin完成真实日线级价格范围模型。
- 盘中路径模型作为后续独立增量，只在用户确认需要时读取现有分钟Bin。
- 接入价格转换和硬风险边界。

### M5：模型质量迭代

优先级：`P2_AFTER_FUNCTION`。

- 在当前约两年的各腿预测覆盖内比较滚动、扩展和不同起点窗口；3/5年只有在同一目标父包的合法历史预测确实存在时才执行，不能用其它实验或新模型回填。
- 增加额外种子、消融、bootstrap和概率校准。
- 它们影响模型质量结论，不回溯阻断M1/M2真实功能。

### M6：长期趋势专家

优先级：`P2_WHEN_PACKAGE_READY`。

- 使用长期趋势包对应的现有QE文件训练独立模型。

### 完成后再确认的可选任务

以下任务不属于当前路线，只有短反弹`4/4`完成后由用户重新确认：

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
| F-104 | 训练/验证/测试按decision date执行246/10/60/10/80切分，10日purge覆盖最长退出窗口且无未来数据泄漏 |
| F-105 | 训练与正式预测共享同一逐列FeatureBuilder/schema、公式、单位、missing和decision cutoff |
| F-106 | 正式预测区分decision/target双日期，只读取数据库decision cutoff行情和实际Program输入 |
| F-107 | 真实模型输出Top5并与原始/HMM/随机/等权基线比较 |
| F-108 | 页面可展示明确标记的`EXPERIMENTAL_SHADOW`真实结果 |
| F-109 | 模型失败不阻断现有荐股，不用基线冒充模型；bundle只按exact shadow binding加载，不扫描latest |
| F-110 | 多个Program独立运行，支持单Alpha和原生多Alpha父包 |
| F-111 | 收益、周期和价格范围均来自真实模型；规则只作为单独标记的对照或硬风险边界，不能替代模型结果或伪造概率 |
| F-112 | Selection、Paper、模拟盘、QE资产和策略包业务逻辑零写入 |
| F-113 | 不新增角色、审批、package二次准入或未经确认的门禁 |
| F-114 | 不处理旧历史任务、旧root、归档和非阻碍性平台工作 |
| F-115 | 只有真实功能计入进度；基础设施和证据不计入`N/4` |
| F-116 | 原生多Alpha首模按当前代表seed、zscore和terminal weights重建runtime-equivalent候选；完整seed/逐日权重/combined只作诊断，不跨实验拼腿或制造伪seed特征 |
| F-117 | 历史涨跌停直接读取日线/分钟Bin中的状态、价格和昨收，不重复建设`stk_limit`训练文件 |
| F-118 | HMM按当前文件数据从头拟合、确定性规范状态并保存可续推posterior；shadow不单独refit HMM，旧模型/状态/系数只作对照 |
| F-119 | 分钟Bin不阻断Top5、收益、周期或首版日线价格范围；只有盘中路径模型明确需要时才消费 |
| F-120 | 首模只要求沪深300；其它宽基指数在模型合同明确需要前不补充、不阻断 |
| F-121 | WSL训练峰值内存低于8GB并以小时级完成首模；采用列/日期/候选分批与临时Parquet，不建设新缓存或证据平台 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | planned: `backend/services/advisory_model_first/qe_file_source.py` | planned: `backend/tests/advisory_modeling/test_qe_file_source.py` | design_ready | none |
| F-102 | planned: `backend/services/advisory_model_first/wsl_training.py` | planned: `backend/tests/advisory_modeling/test_wsl_training.py` | design_ready | none |
| F-103 | planned: `backend/services/advisory_model_first/short_rebound_trainer.py` | planned: `backend/tests/advisory_modeling/test_short_rebound_training.py` | design_ready | none |
| F-104 | planned: `backend/services/advisory_model_first/time_split.py` | planned: `backend/tests/advisory_modeling/test_time_split.py` | design_ready | none |
| F-105 | planned: `backend/services/advisory_model_first/shared_feature_builder.py` | planned: `backend/tests/advisory_modeling/test_feature_source_parity.py` | design_ready | none |
| F-106 | planned: `backend/services/advisory_model_first/realtime_feature_source.py` | planned: `backend/tests/advisory_modeling/test_database_realtime_source.py` | design_ready | none |
| F-107 | planned: `backend/services/advisory_model_first/reranker_evaluation.py` | planned: `backend/tests/advisory_modeling/test_reranker_evaluation.py` | design_ready | none |
| F-108 | planned: `backend/routers/advisory.py`、`frontend/src/app/paper-v2/advisory/page.tsx` | planned: `frontend/tests/advisory-model-shadow.spec.ts` | design_ready | none |
| F-109 | planned: `backend/services/advisory_model_first/model_inference.py` | planned: `backend/tests/advisory_modeling/test_inference_failures.py` | design_ready | none |
| F-110 | planned: Program级推理composition | planned: `backend/tests/advisory_modeling/test_program_isolation.py` | design_ready | none |
| F-111 | planned: M3/M4模型头 | planned: `backend/tests/advisory_modeling/test_outcome_and_price_models.py` | design_ready | none |
| F-112 | planned: Advisory消费层边界 | planned: `backend/tests/advisory_modeling/test_protected_module_isolation.py` | design_ready | none |
| F-113 | 本蓝图§3、§12.4、§14 | planned: `backend/tests/advisory_modeling/test_no_unapproved_gates.py` | design_ready | none |
| F-114 | 本蓝图§0、§3、§9 | planned: `backend/tests/advisory_modeling/test_no_historical_dependencies.py` | design_ready | none |
| F-115 | 本蓝图§1、§9 | artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-116 | planned: `backend/services/advisory_model_first/qe_file_source.py` + existing `ModelStoreService` | planned: `backend/tests/advisory_modeling/test_qe_exact_roster_prediction_source.py` | design_ready | none |
| F-117 | existing Qlib Bin fields + planned file source | planned: `backend/tests/advisory_modeling/test_qe_limit_bin_source.py` | design_ready | none |
| F-118 | existing `backend/quant_models/hmm/sector_hmm.py` + planned file adapter | planned: `backend/tests/advisory_modeling/test_hmm_fresh_file_fit.py` | design_ready | none |
| F-119 | planned: M1/M3/M4 model inputs | planned: `backend/tests/advisory_modeling/test_minute_data_phase_boundary.py` | design_ready | none |
| F-120 | existing `000300.SH` Qlib day Bin | planned: `backend/tests/advisory_modeling/test_benchmark_index_minimum.py` | design_ready | none |
| F-121 | planned: WSL launcher/reader/trainer | planned: `backend/tests/advisory_modeling/test_training_resource_budget.py` | design_ready | none |

## 12. Verification Plan

### 12.1 训练验证

- QE H5/Parquet/Qlib Bin基础数据和目标父包Prediction Store PKL真实可读。
- feature/label schema和日期范围读回一致。
- 精确roster中的每条腿和seed均能解析，腿间对齐不跨Program或父包。
- 日线Bin的`limit_up/down`、`up/down_limit_price`和`prev_close`可读。
- HMM由本轮文件数据重新拟合，历史HMM产物未进入训练输入。
- WSL身份、Conda环境和LightGBM真实训练日志可见。
- 训练、验证和测试日期无交叉。
- 模型文件可重新加载并对相同输入产生确定性预测。
- 留出集Top5非空，逐日group边界正确。
- 峰值内存、各阶段耗时和临时文件规模可见，首模不读取全量分钟Bin。

### 12.2 推理验证

- 数据库当前/实时输入可以生成与训练相同schema。
- 单Alpha和原生多Alpha各自独立推理。
- 模型不可用、字段缺失和版本冲突均有typed reason和有效后台日志。
- 不写Selection、Paper、模拟盘、QMT或QE实验文件。

### 12.3 页面验证

- 页面展示真实API结果，不使用fixture或静态mock。
- 明确区分规则、实验模型和后续已验证模型。
- Top5、收益/周期、价格区间按已实现能力逐项出现，不等待全部模型完成。
- 桌面和移动viewport无重叠、无静默网络错误。

### 12.4 DESIGN-COMPLIANCE-001

每次合入前逐项证明：

1. 没有用简化、mock、规则或静态结果冒充真实模型。
2. 没有静默错误或无日志fallback。
3. 没有改变Selection、Paper、模拟盘、策略包或荐股基线语义。
4. 没有新增未经用户确认的门禁、审批、角色或历史工程。

## 13. Rollout / Rollback

### 13.1 Rollout

发布顺序固定为：

1. 合入QE文件reader、共享FeatureBuilder和WSL trainer。
2. 在WSL完成真实训练并生成可加载模型文件。
3. 合入数据库实时FeatureSource和Advisory影子推理API。
4. 后端经用户单独确认后重启加载新源码；模型文件激活单独报告。
5. 页面先展示`EXPERIMENTAL_SHADOW` Top5，再随M3/M4逐项增加收益、周期和价格范围。

源码合入、WSL训练、模型文件生成、后端重启、模型加载和页面可见是独立状态，不得合并声明完成。

### 13.2 Rollback

- 关闭目标Program的模型配置后恢复现有`selection_effective_rank`和`rule_default`。
- 模型加载或推理失败时只关闭模型通道，不停止现有荐股。
- 不修改或回滚Selection、Paper、模拟盘、StrategyPackage或QE资产。
- 不删除训练文件、模型文件或已产生的预测；只停止继续使用有问题的模型版本。
- 本阶段默认无DDL/DML，因此回滚不执行数据库反向迁移。

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
production_dml_gate = noop for training
production_backend_dependency_gate = noop unless WSL dependency is proven missing
production_frontend_dependency_gate = noop unless UI implementation introduces a real dependency
runtime_activation = separate user-confirmed action
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
| 再次转向历史闭合 | 以本蓝图F-114和`N/4`功能进度立即停止该任务 |

## 16. 当前下一步

“QE文件训练与数据库实时推理垂直切片”详细设计已经形成：

`docs/architecture/advisory_model_first_qe_file_training_realtime_inference_f2_design_20260808.md`

该设计已经：

1. 绑定目标父包精确roster、两腿代表seed/model SHA、current zscore、terminal weights和runtime semantics；完整seed、合成预测和逐日权重只作诊断。
2. 定位目标QE H5/Parquet/Qlib Bin和实际schema，确认基础数据只读文件来源。
3. 明确逐列首模特征、decision/target双日期、完整标签公式、10日purge和时间切分。
4. 明确HMM文件输入、状态规范化、因果预测、文件截止posterior续推和旧HMM仅对照边界。
5. 明确首版日线价格范围不依赖分钟Bin，盘中路径属于后续增量。
6. 明确WSL真实训练命令、资源上限和模型输出。
7. 明确数据库实时FeatureSource与共享FeatureBuilder。
8. 明确真实影子推理API和页面readback。
9. 明确禁止Historical Range、Source Catalog、SEALED、历史DML和旧任务处理。

下一项任务直接进入 M2：实现数据库 decision-cutoff FeatureSource、exact bundle loader、只读影子推理 API 和 Advisory 页面 readback，并展示真实 baseline 与 `EXPERIMENTAL_SHADOW` 状态。不得在 M2 前插入其它基础设施、证据、历史固化或治理任务；模型质量优化按 M5 排序，不得用规则或基线替代真实模型输出。
