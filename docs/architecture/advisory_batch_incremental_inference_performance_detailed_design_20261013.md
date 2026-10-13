# Advisory 历史批量与日频增量推理性能详细设计

> 日期：2026-10-13；版本：v1.2
> 用户已确认优化方向，并要求尽量在一至两个长任务内完成。
> 当前任务：用户于2026-10-13授权源码开发、多轮审核修复及满足合入条件后的提交合入；后端重启仍由用户执行。
> 当前状态：LT1_SCOPE_SOURCE_VALIDATED_CI_BLOCKED；本轮归属源码的真实对照已完成，公共CI测试覆盖仍未闭合；不代表完整LT1/LT2、合入或运行态激活。
> 原分析源码基线：`a500e52b95b2ab812af74c9dd65479d42fca61ec`；独立源码工作树已安全同步至`9e9e0842c`后继续实施。
> 实现涉及共享推理契约，按 F2/T3 管理；本文同时跟踪设计、源码、性能和运行态，四者不得混报。

## 1. Scope / 目标、继承关系与边界

目标是消除重复数据读取、物化和计算，使历史回放真正批量执行，使实盘日频仅更新新增及修订数据。已批准 StrategyPackage、模型权重、特征、股票池、排序、TopK、成本和逐日业务逻辑保持不变。

本文件是[实盘单日与历史批量同核主设计](advisory_live_daily_historical_batch_shared_kernel_f2_detailed_design_20260816.md)的推理性能补充，不另建业务内核、回测引擎、缓存平台或调度系统。名单和退出、状态恢复、业务发布继续由既有权威组件处理；不重复实现主设计全部 H0。

[现行蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)将 H0 设为条件性休眠。本次仅处理用户明确批准的首日约29分钟及重复 I/O 问题，不恢复旧44日研究、历史证据固化、归档、UI或其他休眠工作。性能验证使用代表性输入，不再为旧负向模型寻找新经济证据。

范围限制：

- QE 已批准的策略包直接消费，不新增策略包准入、收益门槛、重训或重新审核 QE 实验。
- Advisory 窗口不修改 QE 公共代码、数据准备、Selection、LocalSIM/Paper 或其他 owner 的源码；共享文件按具体函数与消费者影响确定范围，不按文件可访问性扩大权限。
- 正常数据源沿用既有数据库日频记录和已交付身份；不补数据、不改 active profile、不重建候选、不写数据库或执行 DDL。
- 不参与分钟线执行算法研发，不调整资金或订单逻辑，不控制现有实验或服务进程。
- 主设计 §9.2 的“大文件显式 copy”可选项在本次范围内禁用：不支持共享引用时明确报告原因，不回退到复制完整输入。必要的不同 schema 转换是一次性生成不同表示，不是按日期或策略包复制同一文件。

## 2. Background / 当前事实与问题依据

2026-10-13已取得的首日记录：两个包的 `2026-01-05` 信号使用 `2025-12-31` 截止数据，历史输入从 `2024-11-05` 开始；股票池4913，实际评分4907。两包各读取1,392,716行行情及同规模合并基础数据，行情、基础数据和股票池摘要分别相同。对应 signal artifact 为 `ssa_6dd268749c194d5bb6153307a60d503b`、`ssa_803a06219d3147e8a89916b58135a5e8`。

从调度提交至 artifact 保存约28分42秒、28分54秒；没有分阶段计时，不能称为“神经网络 forward 用了29分钟”，也不能据此精确分摊数据库、因子或 GPU 耗时。这是历史观察，不是对当前回放进度的声明。

| 已核对位置 | 事实与优化点 |
|---|---|
| `backend/inference_engine.py::_run_inference_impl` | 读取历史/基础数据、生成 HDF/Parquet、逐因子执行，再做模型预测；序列分支对多个日期重复计算重叠历史前缀 |
| `_apply_saved_qe_infer_processors` / `_saved_qe_step_len` | 两处读取相同冻结 dataset，分别取 processor 和序列长度 |
| `_compute_alpha158_last_day_only` | 回归、相关、`xs`、`groupby.apply` 等可复用分组或等价向量化；须先证明公式等价 |
| `backend/data_service/cache.py::SelectionDataCache` | 当前缓存是进程内、日级、按条数控制，读写复制 DataFrame；历史只读入口关闭它，不能直接打开当作批量缓存 |
| `backend/services/strategy_package/live_inference.py` | WSL provider 每日新进程；预处理文件再次物化/复制；基础 HDF 多 alias 分别写入。权重 asset 原生路径已有硬链接，不应误称全部权重都在复制 |
| `selection_artifact.py::prepare_from_live_inference_dates` | 已接受日期列表，并在循环前 prepare workspace；缺口是循环内仍单日 provider.run，不是完全没有批量接口 |
| `backend/data_service/api.py` / `backend/qlib_exporter/db_reader.py` | 当前路径存在重复复权因子读取及合并；公共修复由对应 owner 处理 |
| `multi_alpha_live.py::weights_for_apply_date` | 区分冻结混合权重与原生滚动 IC 权重；历史只读入口目前仅接受 manifest 冻结权重，不得为提速改权重模式 |
| Selection / simulation scheduler | 已有 artifact 复用和同键 in-flight 去重；新方案补共享输入及批量计算，不重复建设这些机制 |

原首日超时与后台推理继续完成是另一类正确性问题，由 BUG-1869 所属 owner 处理。本设计不抢改该工作树，不把延长超时当作性能改善。

## 3. Implementation Plan / 只安排两个长任务

两个任务按交付目标组织，内部步骤、代码审核和定向验证连续执行，不拆成十几个审批阶段。长任务数量不等于 PR 数量；不同 owner 的必要交付不能合成越界大 PR。

| 长任务 | 连续工作清单 | 完成条件 |
|---|---|---|
| LT1：去重、复用与历史批量 | 加轻量阶段计时；共用原始输入和必要格式；消除预处理/模型重复装载；等价优化 Alpha158及共同因子；接通持久推理 session 与日期分块；多轮审核、修复、语义/资源/性能验证 | 代表窗口真实使用批量路径，结构性去重成立；全评分、TopK、权重和逐日业务语义等价；清楚报告冷/热耗时、资源峰值和未完成 owner 项 |
| LT2：日频增量与整体收尾 | 在 LT1 上增加新增日/迟到修订/池变更处理；原生权重更新复用；去除重复查询和摘要扫描；仅针对剩余热点调线程/forward batch/CPU-GPU 搬运；完成日频、恢复及跨消费者回归与交付 | 连续日及修订重算与独立单日一致；缓存有界、无未来信息、无重复大文件复制；相关源码按范围交付，合入与用户重启后的激活状态分开报告 |

优先完成 LT1 的端到端路径，而不是先优化全部微小热点。资源调优可在没有收益时明确结束，不为耗满工时持续试参。若 LT1 已覆盖 LT2 的必要增量并完成全部验收，可合为一个长任务；默认两个便于降低组合风险。

不作未经测量的工时或分钟数承诺。跨 owner 未交付仅标记对应优化项缺口，Advisory 自身工作继续；不能因此宣布端到端优化全部完成。后端重启仍由用户执行，不能通过多拆任务绕开模块边界。

### 3.1 当前实施进展与尚未完成项

截至本版提交前的核对快照，源码在独立工作树中，未创建PR、未合入，后端运行态不变；用户已授权提交合入，提交状态以Git交付记录为准。已实施的Advisory归属改动：

- 推理入口加入连续阶段计时、行数/评分数、资源加载/命中/保留字节、兼容格式生成计数及Linux RSS/GPU峰值；计时不参与原输入或policy hash。
- 同一显式历史session加载冻结模型和infer processor，逐次隔离可变网络/processor状态。processor只保留已拟合子集，不保留训练DataFrame、不执行fit；不能安全复用的资产显式采用原加载方式，不阻断已批准包。
- 已验证20项现有Alpha158公式的有序因果tail计算，复用instrument索引和中间数组，不再对每个序列日复制全历史前缀。未知特征或不满足等价前提的输入继续使用原算法，不改变特征定义或原索引约定。
- 已知原生因子adapter的六个基础HDF别名引用同一实体；不同PV schema保留一次必要转换。写保护/写入检测只覆盖本调用新建的临时文件，不修改正式资产或因子代码。
- CLI新增显式只读历史日期计划，复用同一engine/session；逐日原子保存完整评分，失败保留已完成日期、停止后续日期并明确FAILED，不执行正式名单或账户发布。
- Windows/WSL任务临时及相关库cache显式落X盘；计算叶模块在Python3.10节点可导入，原业务类型导出仍保持身份一致。

实测使用既有冻结LSTM包`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、4913只股票池和原46维/20步序列，2026-01-05首日使用2025-12-31 cutoff、1,392,716行输入。两日旧算法/优化对照均结束，4907只有效评分、完整排序、input_context及source receipt除观测时间以外的字段完全相同。实际只读产物位置：`X:/AIstock_temp/advisory-perf-20261013/lstm-actual-2day-r3/`。

| 指标 | 2026-01-05首日 | 2026-01-06续日 |
|---|---|---|
| 旧算法耗时 | 1629.21秒 / 27.15分钟 | 1598.97秒 / 26.65分钟 |
| 优化耗时 | 417.62秒 / 6.96分钟 | 421.64秒 / 7.03分钟 |
| 实测提速 / 完整评分 | 3.90倍 / 4907只完全一致 | 3.79倍 / 4907只完全一致 |
| 优化读取 / 动态因子 / Alpha158 | 237.55 / 112.11 / 29.14秒 | 207.59 / 137.28 / 40.05秒 |
| 模型加载阶段 / session资源 | 5.55秒；2次加载（模型+processor） | 0.01秒；0次加载、2次命中 |

两日内核累计旧算法3229.10秒、优化839.56秒；含WSL启动的墙钟分别3236.29秒、844.63秒，约3.83倍。首日Alpha158旧算法1172.17秒、优化29.14秒；优化forward为0.15秒，不能再把总耗时称作GPU forward耗时。优化首日进程峰值RSS约8.14GiB，对照约9.33GiB。对照为同一宿主顺序执行，非后台负载完全恒定的理想实验。

最终源码的状态隔离/临时路径和性能收据绑定随后有定向修订，不能将r3冒充最终文件绑定。串行计划已全部结束：最终LSTM两日只读复验及TCN一日旧/新对照分别位于`X:/AIstock_temp/advisory-perf-20261013/lstm-final-2day/`、`X:/AIstock_temp/advisory-perf-20261013/tcn-final-firstday/`。两次探针开始/结束的七个推理源码文件SHA完全一致，提交前再次核对也一致。

| 最终源码验证 | 原基准 / 最终优化 | 正确性及身份结论 |
|---|---|---|
| LSTM 2026-01-05 | r3旧算法1629.21秒 / 最终内核380.16秒（6.34分钟） | 4907只完整评分/排序、input_context、除first_observed_at以外的全部source receipt字段精确一致 |
| LSTM 2026-01-06 | r3旧算法1598.97秒 / 最终内核419.32秒（6.99分钟） | 同上；资源0次加载、2次命中 |
| LSTM两日含WSL启动墙钟 | r3旧算法3236.29秒 / 最终804.41秒（13.41分钟），约4.02倍 | 最终探针仅运行optimized，其progress中的score_equivalence仍是PENDING；提交前另与既存r3 reference逐日完整比较PASS，不能伪称该探针独立生成了reference |
| TCN 2026-01-05含WSL启动墙钟 | 1542.90秒（25.71分钟） / 403.48秒（6.72分钟），约3.82倍 | 同一探针旧/新全评分及输入身份EXACT_PASS；两路径各4907只评分 |

最终LSTM首日/续日行情及基础数据读取阶段为200.35/231.30秒，动态因子112.50/121.65秒，Alpha158为31.33/28.98秒，forward为0.136/0.020秒。TCN内核397.17秒，其中读取199.73秒、动态因子121.41秒、兼容格式等准备27.03秒、Alpha158为32.23秒、forward为0.61秒。读取阶段包含现有数据组装，不能将它全部称为纯SQL时间；尚未测量实际SELECT次数。

实际冻结LSTM/TCN权重的3次资源复用smoke也在各自独立WSL进程完成，合成输入评分与原加载精确相同，各1次加载+2次命中。最终真实LSTM两日session共2次加载（模型+processor）、2次命中；模型参数没有重训。最终LSTM进程累计峰值RSS首日约8.13GiB、续日约9.40GiB，TCN约8.11GiB；资源模板缓存预算不是全进程内存上限，也不能用单日峰值证明长窗口无增长。上述验证均不是实盘收益或正式业务发布证明。

提交前最终最小矩阵一次运行37个独立用例全部通过：原严格评分/只读合同、精确公式/未来毒化、模型和processor隔离、资源身份/逐出/同键single-flight、别名写保护及失败日期顺序。此前各修复点另有定向复验，不能将重复执行累加成新增用例。变更Python文件ruff通过。pytest未安装在WSL环境，不安装依赖，节点通过真实加载及推理验证。此前发现的Python3.10业务类型提前导入、硬链接ctime误报、Windows只读测试清理问题已修复并复验；这些失败不能被记为原先已通过。

仍未完成且不得冒充PASS：原始行情/基础数据跨日期和跨包分区复用、StrategyPackage原生WSL provider接入日期计划、公共复权重复读取消除、共同动态因子缓存、日频新增/修订传播、多腿原生权重/正式业务消费顺序回归。部分属于§8其他owner，Advisory不得越界修改；本次源码范围可以独立审核交付，但LT1整体及LT2完成状态须分别保留，不能称为全部优化完成。

交付检查发现公共CI覆盖缺口：F2校验中`backend/tests/scripts/test_strategy_package_live_inference.py`未被现有选中nox计划实际覆盖，报`ci_classifier_unexecuted_test_blocked`。GitHub精确文件名检索未返回相关Issue，本分支未找到PR；已向用户给出流水线owner精确修复请求，将该文件纳入既有`advisory_historical_range_backend`的真实pytest collection。此项未解决前，不报告满足合入要求，不省略changed-file、不改名/转移ownership、不由Advisory修改公共CI源码。真实性能验证继续，不为此停掉旧实验或操控服务。

### 3.2 长窗口进一步优化的优先顺序

两日结果表明本轮已去除约二十分钟的Alpha重复前缀开销，但历史日期计划目前只复用模型资源，仍逐日读取约139万行行情及同规模基础数据、生成本日格式并重算动态因子。它不是§4/§5要求的完整跨日数据与特征批量。按每交易日7分钟作线性规划估算，126/252个交易日分别约14.7/29.4小时每包；两包串行约29.4/58.8小时，尚不含正式名单、结算和发布，也不是长窗口实测。

下一轮沿用LT1/LT2，不新建平台或扩张到其他owner：

| 优先级 | 优化项目 | 实施及正确性要求 |
|---|---|---|
| 1 | 跨日、跨包原始输入一次读取，后续只取新增/修订分区 | 现有只读公开接口优先；原始未复权数据与逐日复权视图分离。不能将区间末日的前复权价格直接切片给早期日期；股票池、availability、单位及原日视图摘要保持一致。缺少公开接口时只提出最小需求，不修改数据/QE模块 |
| 2 | 共同有限窗口特征一次计算、多日期/模型复用 | 已有20步序列相邻日重叠19步，可复用精确共同结果；仅在代码/参数/输入/人口及历史初始化口径一致时共享。LSTM/TCN有18项动态因子源码SHA相同，是复用候选而非已证明结果可互换 |
| 3 | 兼容格式按实际输入身份复用，减少逐因子重新读HDF和result.h5往返 | 现有alias已不复制实体，但每个因子仍各自读取、计算、写读结果。通过原生adapter最小接口或Advisory归属的安全执行能力接通；未知因子仍只拿本日合法视图，不拿含未来行的共享buffer |
| 4 | 原生日期列表真正接入一个session及分块评分 | 现有SelectionScoreArtifact入口虽接受日期列表，内部仍逐日provider.run并启动进程；StrategyPackage owner仅需批量消费者接入，Advisory不代改。GPU forward不足1秒，不先投入模型改造或盲目增加并发 |
| 5 | 在多日期分块基准中核对总RSS、物理I/O和续日耗时，再收尾日频增量 | 缓存不能按日期保留完整重叠窗口；使用有界工作块及X盘单实体引用。逐日业务状态/退出/结算仍顺序推进，不强行向量化有持仓依赖的状态转换 |

动态因子不能一律对全区间算一次后切片。例如LSTM的`m_tech_atr_ratio_14d`使用`ewm(span=14, min_periods=10)`，当前单日warm-up起点会移动；从固定总区间起点计算可能改变初始化与原有评分。此类因子须保持原起点语义，证明等价后复用，不能截短、近似或修改因子代码。复用共同因子也不共享不同包的已拟合processor/模型状态。

结构目标是总耗时从“交易日数×全窗口读取/重算”转为“一次准备+增量/分块计算+逐日业务转换”。具体半年/一年耗时必须在接通后测量；本轮不承诺未经验证的秒级续日SLA，不缩池/减特征/放宽PIT来换速度。

## 4. Architecture / 数据、文件与缓存设计

### 4.1 缓存层级与身份

优先复用既有 artifact/asset store 和任务生命周期，仅新增完成该推理链需要的内部对象，不建 Redis、UI、审批或长期证据平台。

| 层级 | 可复用内容 | 身份和共享边界 |
|---|---|---|
| 原始输入 | 行情、复权因子、日历及声明需要的基础列；按日期/股票分区 | 已交付 source/profile/revision、节点、字段/schema/单位/复权口径、日期/股票范围；相同输入跨包复用，池语义在消费者侧仍独立 |
| 兼容格式 | 因子真正要求的规范化 PV、基础 HDF或 Parquet | 原始输入身份、schema 与转换代码；不同 schema 各生成一次，相同 alias 指向同一实体 |
| 共同特征 | Alpha158及同代码/参数/输入的动态因子 | 原始视图身份、PIT 边界、特征代码/参数、缺失和历史初始化语义；涉及横截面时必须含实际人口身份 |
| 冻结模型资源 | 模型、训练时 infer processor、feature order、step_len | package/model/preprocessor/code/运行环境及 device/dtype；不共享可变隐藏状态或不兼容 processor |
| 原始评分 | 单日完整合法股票池的评分 | 特征、模型、cutoff、股票池、normalization 和全部 raw-affecting 配置；与账户/名单动作分离 |

使用稳定摘要，不复用当前进程内 Python `hash()` 作为跨进程身份。原始列投影可以复用同一上层完整对象的子集，不能因列顺序、单位或复权不同误命中。多个池共享原始行情不代表可以共同归一化或排序。

身份检查用于保证缓存命中正确，不构成新的 StrategyPackage 准入合同。缓存缺失是正常 miss；输入错误、摘要冲突或污染不能伪装成命中、空成功或调用当前配置回填。

### 4.2 一次物化，多次引用

原始数据采用不可变分区，历史 warm-up 重叠部分只物化一次；日期视图引用已有分区，不再按日复制整段历史。需要的格式表示按内容身份唯一生成，同一基础 HDF 的兼容名称采用路径映射或验证过的链接。

模型和 processor 优先直接只读引用现有 asset。已有权重硬链接保留，删除重复 processor 物化和请求内复制。跨 F/X 盘不能硬链接：使用消费者能够访问的正式引用或只读链接，不降级 `copy2`。链接不天然只读，消费者必须不能写共享输入；写模式或输入被改变应显式失败，独立保存输出。

标准输出不是大文件缓存。小配置、任务描述及必要的结果文件可以生成；禁止重复生成相同完整行情、基础数据、模型、processor 文件。允许按真正不同的因子 schema 生成必要表示，但应记录次数和字节量。

### 4.3 原子构建、容量和并发

- 同键 single-flight 覆盖读取、计算、发布全流程，不只保护 dictionary 的 get/put；消费者等待同一构建，失败可重试，不共享半成品。
- 临时构建写入任务拥有的 staging，完成后原子发布；使用原子 manifest 引用多文件组合。取消/崩溃只留下不可命中的未完成项，不覆盖已完成对象。
- 内存按字节预算、生命周期和 LRU 控制；有引用对象不逐出。磁盘同样有最大容量，不为184日保留184个完整重叠窗口。
- 预算包含 DataFrame、转换缓冲、模型和 worker 全部内存；遵守现有任务资源上限，分块或只读 mmap/列式文件溢出，不默认提高配额。
- DataFrame 层面标记“只读”不够。共享 backing 由内部 provider 持有；需要改列/标准化的消费者只取最小隔离工作块，禁止就地改共享对象，也不在锁内复制全表。
- 不依赖系统日期使历史缓存失效；由版本、实际日期区间、修订和资源生命周期决定复用。

### 4.4 Windows / WSL 临时目录

所有新增临时和可重建缓存放在任务拥有的 X 盘目录，例如 Windows `X:\AIstock_temp\advisory_inference\<task_id>`，WSL 通过对应 `/mnt/x/AIstock_temp/advisory_inference/<task_id>` 映射。正式已有 F 盘 asset 保持原地，不复制到 X，不改动正式资产。

Windows 显式配置 `TEMP/TMP` 及相关库临时路径；每个 WSL 子进程显式传入 `TMPDIR` 和 cache root，不能假设继承 Windows 设置。验证解析后的 Windows 最终路径、WSL mount 和 reparse target 不落 C 盘；X 不可用时报告可操作错误，不静默使用默认临时目录。存量 WSL 磁盘的位置与本次新增临时路径分别报告，不迁移 WSL 或重设其他窗口配置。

## 5. Contracts / 历史批量计算与因果语义

### 5.1 读取整个计划，消费单日视图

批次规划 union 股票及 warm-up/输出范围，只读取所需列，按预算分块。一次准备并不等于因子可访问未来：因子及业务内核只接收指定 cutoff 的视图；大批底层 buffer 不对外暴露。

决策日 D 的 cutoff 沿用现有交易日/时钟规则；例如交易日推理以 D-1 收盘信息评分。不能机械按自然日减一，也不能改为使用 D 收盘。停牌、上市不足历史、未知字段继续遵守原合同，不删除日期、缩小池或缩短 warm-up 来提速。

前复权必须按每个历史 cutoff 的既有规则生成；不能先用今天的复权价格算全年，再切出历史信号。输入的 availability/revision 和股票池 PIT 沿用正式已交付语义，不能把“全区间已在缓存”当作消费授权。

### 5.2 特征只算必要部分

Alpha158 等可证明因果等价的时序特征一次顺序计算/增量更新，再构造每个决策日的模型序列，不反复执行 `history <= day` 全前缀。一次建立 instrument 索引、共享中间统计；回归/相关/滚动等价向量化，保持排序、min_periods、NaN、ddof、单位和列次序。

不能统一把所有因子视作有限 rolling window。逐因子保留 rolling、expanding、EWM、历史初始化和原入口重置语义；全局归一化、rank、依赖历史端点或未知行为的因子，不得未经证明批量替换。未优化项继续显式使用正确的既有单日算法，计入耗时与缺口，不冒充全部已去重，也不阻断批准包正常消费。

跨包复用共同特征只限相同代码、参数、输入和人口语义。不同包的冻结 infer processor 和横截面标准化独立应用；不对测试/未来区间执行 fit 或 fit_transform，不为优化重训模型。

### 5.3 持久 session 与 forward 分块

一个批量 session 复用模型、processor、feature order、step_len 和基础输入。首先保证同包跨日复用；模型多种子/多腿按预算分组，不能为了“全量批量”把所有模型同时装入 GPU。

模型仍消费训练时规定的完整序列。可以对多个日期/股票组装样本后按 batch size 前向计算，但不改成只输入新增一行，不跨日无依据延续 LSTM hidden state，不改变 TCN padding、填充或 eval 模式。每个样本仅含自己的历史视图，跨样本不互相使用信息。

已有 `prepare_from_live_inference_dates(...)` 保持调用兼容；先从已批准的消费者接入实际批量 provider，而不是另建同名业务算法。当前已实现本任务的`InferenceResourceSession`及CLI `--historical-plan`；它们不是StrategyPackage原生provider已经接通的证明。原始分区和修订增量接口仍待其owner实施，不能当作已有API调用。

全池评分可以批量预计算；名单、账户状态和任何费用/交易/收益结算仍按交易日顺序经既有入口执行。首个未完成日之后不得提交依赖它的业务状态；后续已算评分只是未消费缓存。恢复复用评分，但不能重复发布 artifact、名单转换或交易动作。

拟新增的内部接口职责限定如下；具体符号由各 owner 在登记实现范围时确定，不新增公开 HTTP endpoint 或修改现有请求默认语义：

| 内部契约 | 输入 | 输出 / 生命周期 |
|---|---|---|
| 原始分区 provider | 已有 source/revision、日期/股票/字段范围及预算 | 不可变分区引用；取得单日视图时应用其 cutoff、人口和复权语义 |
| inference session | 已批准模型/processor资产引用、环境与device身份 | 已加载资源句柄；受任务预算约束，close只释放自身内存/临时引用 |
| score-dates | session、排序后的日期计划、单日视图provider、样本块上限 | 日期独立的完整 raw scores、输入引用与阶段计时；不直接发布业务状态 |
| incremental update | 新增/修订分区及原特征定义 | 新内容身份和精确受影响区间；旧缓存只失效、不伪装更新或改写正式产物 |

数据库只读输入使用既有短事务/一致性语义，任务不得为整个回放持有长事务。每次结果显式关联自己的 input/model/processor/policy 身份；共享原始评分与各消费者正式 artifact 身份不是同一对象。

## 6. 日频增量与权重

### 6.1 新增日、修订和股票池变化

日频获取新增交易日分区及明确修订的历史分区，再计算受影响部分。无修订时复用历史原始输入和可复用特征；新股/新增成分股补齐模型所需既存历史，移出股票不参与当前排名，但不删除历史分区。

修订影响范围由公式决定：有限 rolling 更新至受影响最大窗口；expanding/EWM 或历史初始化受影响可能需要从变更点重算后续整段，不能统一“只重算最近N日”。复权变化单独考虑价格变换及受影响特征；缺可靠修订标识时不得无限复用老输入，沿用现有可验证只读读取，说明该来源仍有必要读取成本。

每日仍评分完整合法股票池，重新计算原有横截面 normalization/rank 和 TopK。不能只更新昨日 Top20/50，不能遗漏新入池股票。新增/修订缓存 miss 触发正常计算，不人为升级为新准入门禁。

### 6.2 三类“权重”分别处理

| 权重类别 | 历史批量 | 实盘日频 |
|---|---|---|
| 模型训练参数 | 从批准包加载一次并复用，不训练新权重 | 同模型身份复用，版本变更正常换资源 |
| manifest 冻结 Alpha/seed 混合权重 | 一次解析后用于所有合法日期，不按结果重新优化 | 保持不变，不把冻结模式改成滚动 |
| 原生滚动混合权重 / 每日目标股票权重 | 滚动模式仅按原合同的逐日成熟标签窗口计算；股票目标权重按各日评分和原 TopK/组合策略生成 | 只更新新增成熟标签及移出窗口的统计，但结果仍对齐原算法；目标股票权重每日重算 |

滚动权重的 cutoff、settlement lag、标签持有期和 lookback 不变；未来未成熟收益不能参与历史权重。增量 sum/count 的浮点次序差异要验证，必要时保留原逐日计算。当前历史只读入口不支持原生 rolling 模式的接口缺口由 StrategyPackage owner 处理，不设为现有冻结包回放的前置，也不私自注入 weight_history。

目标股票权重不等于账户订单数量。现金、既有持仓、成交/停牌、手续费和执行价依赖逐日状态，由对应 owner 的原有逻辑计算；不能一次批量生成整个区间的订单。

## 7. 必要查询、摘要与资源调优

所需数据字段从现有因子依赖合同取得；未知依赖保留完整正确输入，不能按名称猜字段、漏数据或偷偷置默认值。复权重复查询/merge 和 reference price 的重复读取优先复用相同口径的原始视图；不同单位、原始/复权价格不能互代。

输入在规范排序的不可变实体形成时计算摘要，后续引用内容身份；不要每个消费者再 sort/copy/hash 全量表。对可变数据库或来源版本变化仍按既有合同重新验证，不删原本必要的完整性/事务检查。逻辑日视图摘要算法不变，不能用不同的新摘要直接替代旧 policy/input hash。

正常路径 legacy signals 目前已经批量 INSERT，不是逐行 SQL。是否可省略该持久化必须由 owner 完成真实消费者依赖审计；本任务不直接删写入、建新表或将研究缓存写入生产业务库。历史只读依然 `persist_signals=False`、不维护 universe。

先去重复再调线程、日期/股票 forward batch、CPU/GPU搬运。全局 `os.chdir` 及任意因子写结果要求串行隔离或独立 worker，不能直接多线程运行。并发增长须有真实资源收益且不影响 QE 正常实验，不停止其他任务，不拿宿主空闲内存瞬时值当稳定配额。

## 8. 实现责任与范围登记

以下是当前实现位置与后续 owner 边界，用户的开发授权不改变其他模块归属。

当前任务精确允许文件：

- `docs/architecture/advisory_batch_incremental_inference_performance_detailed_design_20261013.md`
- `backend/inference_engine.py`（Advisory归属的只读入口、特征执行与资源复用；既有公开单日默认语义不变）
- `backend/services/advisory_historical_range/alpha158_batch.py`
- `backend/services/advisory_historical_range/__init__.py`（保留原业务类型导出，计算叶模块不触发业务类型的Python版本依赖）
- `backend/services/advisory_historical_range/inference_session.py`
- `backend/services/advisory_historical_range/factor_input_materialization.py`（本推理入口生成的兼容输入去重；不修改StrategyPackage生成器）
- `scripts/strategy_package_live_inference.py`
- `backend/services/advisory_historical_range/performance_probe.py`（独立只读实际包对照，不提交业务artifact或训练）
- `backend/tests/advisory_historical_range/test_alpha158_batch_parity.py`
- `backend/tests/advisory_historical_range/test_inference_cache_lifecycle.py`
- `backend/tests/advisory_historical_range/test_inference_incremental_parity.py`
- `backend/tests/advisory_historical_range/test_inference_stage_counters.py`
- `backend/tests/advisory_historical_range/test_inference_partition_reuse.py`
- `backend/tests/scripts/test_strategy_package_live_inference.py`

其他 owner的精确文件不在此清单；后续若需要新增Advisory接入文件，先登记到本文再实施。尚未实施的文件不为占位创建。

| 范围 | 现有位置 / owner依据 | 实施约束 |
|---|---|---|
| Advisory 历史编排和只读推理入口 | `backend/services/advisory_historical_range/**`、`backend/inference_engine.py`、`scripts/strategy_package_live_inference.py`；catalog `advisory_historical_range_foundation` | 登记精确文件/函数后实施。虽然 inference_engine 当前 primary owner 是 Advisory，其共享函数仍必须报告其他消费者影响；不能凭归属忽略公共行为 |
| 包资源、WSL provider、原生混合权重与 artifact 接口 | `backend/services/strategy_package/live_inference.py`、`selection_artifact.py`、`multi_alpha_live.py`、`package_asset_store.py`；StrategyPackage owner | 交付最小接口和数据复用修改，保持旧单日调用兼容；Advisory 不自行修改 |
| 公共数据读取、复权、共享 data cache | `backend/data_service/cache.py`、`api.py`、`timescaledb_adapter.py`；数据服务 owner；`backend/qlib_exporter/db_reader.py` 为 qlib_data | 仅提交重复读取/字段依赖需求及复现，不改数据准备或 QE profile |
| Selection及模拟执行 | `backend/services/selection_center/service.py`、`backend/services/simulation_runtime/**` 及 Paper消费者 | owner 完成接入/恢复回归；不替代当前 artifact/in-flight 去重，不改订单或账户算法 |
| QE 公共源码、因子库和训练 | `backend/services/quantevolver/**` 等 | 本方案无修改、训练、版本发布或数据激活动作 |

若同一 shared infer 函数同时服务多个消费者，允许在它的归属任务中提交一次公共优化，而非复制算法到各模块；但须由 owner 接受范围，并只验证实际受影响契约。未取得边界内实现权限的项目保持明确交接，不由 Advisory 越界补齐。

## 9. Verification Plan / 验证方案、指标与合入标准

### 9.1 正确性定向矩阵

以相同数据/模型身份对照旧单日路径和新批量/增量路径，保留以下不可删减场景，不用大规模实现快照替代行为测试：

1. 普通日、首日 warm-up、停牌/缺数据、新入池/移出、多指数池；股票唯一性、PIT人口、列序/单位/missing 和完整评分集合一致。
2. batch size=1 与多日分块、跨块边界、不同实际 step_len、LSTM/TCN和多腿/seed；冻结 processor 无 fit、无状态串扰。
3. D 后未来行毒化及“模型拿不到底层未来 buffer”，逐日复权/availability变化；D信号不受未来变化影响。
4. 输入/代码/schema/processor身份变化、同键并发、半成品、损坏缓存、共享输入写入、容量逐出；无错命中或污染传播。
5. 新增日、迟到修订、EWM/expanding传播、复权修订；对照正确单日全量重算，不使用任意截短近似。
6. 中断恢复、重复请求、缓存重建及不完整日期：评分可重用，正式名单/状态/账户转换不重复、不越过未完成日。使用任务测试模拟，不控制生产进程。
7. Windows 与 WSL 临时路径解析、跨盘引用、alias写保护；无 C 盘新临时产物、重复完整输入或隐藏 copy fallback。

离散输出（股票集合、排序/tie-break、TopK、权重政策、状态/原因及名单动作）须一致；浮点差异逐列报告，采用现有 dtype/算法合理误差并在对照前声明，不能结果后放宽到掩盖排名变化。不能通过宽容阈值把业务 hash 不一致写成等价。

Advisory 本地只跑修改点最小合同及直接失败节点；跨消费者回归按真实共享契约边由 owner/既有 CI 执行，避免各窗口重复完整模块套件。不新增 UI 验收或数据库写入作为本次性能前置。

### 9.2 性能测量与去重标准

记录已有任务中的轻量 timing/counters，不建 dashboard、归档台账或收益确认平台：

- end-to-end wall、首日/续日、有效股票-日期吞吐和日级耗时；清楚区分排队、准备、读取、因子、processor、forward、序列编排和已有发布耗时。
- SELECT次数/行数、物理读取/写入字节、格式生成次数、模型/dataset加载次数、缓存命中/等待/失效原因、峰值 RSS/GPU内存及缓存大小。
- 冷缓存、同进程热缓存、跨请求可复用缓存分别测；固定身份、窗口、worker和已有硬件，报告后台负载。不能把两个同时运行的样本当成同条件独立基准。

结构性标准：同版本原始分区每个构建周期读取/物化一次；同必要 schema 每个输入身份生成一次；大输入/模型/processor没有按包或日期的物理副本；同 session/model 只装载一次；批量共同特征不重复扫描可复用历史前缀。容量逐出、合法修订、不同 schema 或不可复用语义的重算要列明，不隐藏例外。

已有主设计性能指标使用它自己的44日 golden，不能挪用到本次新包数据宣称完成。当前已取得§3.1的真实分段和两日基准，剩余跨日读取/动态因子优化仍须测量；以结构性去重及同条件实测判断，不事后制造绝对分钟 SLA。不把历史读窗重叠比例当成墙钟加速倍数。

性能完成至少需要代表性连续窗口确有 end-to-end/续日改善、无资源上限退化，且全部业务等价通过。若结构去重通过但整体未改善，则继续定位剩余热点，报告性能目标未完成，不以理论估算、延长 timeout、减少股票/特征或测试通过冒充性能 PASS。

独立只读对照允许显式`performance-reference`执行方式，仅恢复本次修改前的Alpha逐日前缀、原兼容格式和两次dataset装载，以同一代码/数据环境测量；不作为业务降级或另一套策略。正式默认仍是优化路径，参考模式须显式标识且不允许写库。模型缓存保留冻结模板、隔离逐次调用状态；保留预算不冒充总体RSS/GPU峰值，仍需实际测量工作缓冲及调用副本。

### 9.3 审核与交付

每个长任务内部连续执行：第一轮检查范围、重复工作及设计条款；第二轮检查 PIT/复权/序列/可变状态与反例；第三轮对最终 diff、真实验证和性能数据复核。发现问题先修再审，最终轮不得遗留影响正确性或范围的未决项；没有假定只审三轮就自动通过。

源码PR运行对应Feature/BUG正式入口、设计校验和changed-file gate。是否合入沿用具体源码任务授权，不因文档完成推导生产或进程权限。用户已要求无需细拆阶段，不为每个优化项私增审批。归属源码PR可以在其实现/验证通过后交付，但完整计划的未完成项不得以源码合入替代验收。

## 10. Design Acceptance Index / 设计验收索引

以下F编号仅在本文内编号，不替换蓝图的对应原条款。矩阵分别说明当前源码和完整计划；用户批准的是优化范围、LT1/LT2顺序及owner边界，不代表跨模块权限、未实施项豁免或运行验收。

| 条款 | 要求 | 设计位置 |
|---|---|---|
| F-001 | 同一业务语义、无新策略包门槛、模块范围不扩大 | §1、§5、§8 |
| F-002 | 原始输入和兼容格式去重、不重复复制大文件 | §4 |
| F-003 | 冻结模型/processor复用及真正批量 | §5 |
| F-004 | 日频增量及合法修订，无任意截断历史 | §6 |
| F-005 | 权重不变、标签成熟、状态顺序 | §5.3、§6.2 |
| F-006 | PIT、容量、原子缓存和X盘临时 | §4、§5、§9 |
| F-007 | 两长任务、多轮审核及真实性能改善 | §3、§9 |

### 10.1 Design Acceptance Matrix / 设计验收矩阵

`test_or_evidence`区分已执行位置和后续target。实现不能只覆盖现有测试而遗漏§9.1新契约；设计结构校验PASS不证明未实现的owner项、完整LT1/LT2或运行态PASS。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/inference_engine.py`、CLI及§8范围；原包/权重/池/排序语义保持 | `backend/tests/test_inference_strict_scoring_alignment.py`、最终LSTM两日/TCN首日4907只全评分及输入身份精确对照 | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATED_CI_PENDING | 本轮归属源码等价已验证，公共CI覆盖未闭合；不代表自然前向激活或完整LT1/LT2完成 |
| F-002 | `factor_input_materialization.py`、原位冻结asset引用 | `backend/tests/advisory_historical_range/test_inference_partition_reuse.py`及真实1实体/6alias计数 | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATING | 格式别名已去重；原始分区及跨包provider尚未接通，完整条款未完成 |
| F-003 | `inference_session.py`、`alpha158_batch.py`、CLI日期计划 | `test_inference_cache_lifecycle.py`、`test_alpha158_batch_parity.py`、`backend/tests/scripts/test_strategy_package_live_inference.py`及实际资源加载/命中 | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATING | 归属session已实现；原生provider/consumer接入属其他owner，仍待实施 |
| F-004 | §6；LT2消费者及来源依赖owner | target: `backend/tests/advisory_historical_range/test_inference_incremental_parity.py` | APPROVED_BY_USER_LT2_PLANNED_NOT_IMPLEMENTED | 日频新增/修订/EWM/复权传播尚未实施，不在本轮源码完成声明内 |
| F-005 | §5.3、§6.2；原权重/名单/状态owner；CLI只输出raw评分 | `backend/tests/scripts/test_strategy_package_live_inference.py`日期失败/顺序；target: `test_batch_signal_consumption_order.py` | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATING | raw日期消费合同已测；正式名单/多腿原生权重回归待owner接入，不改账户执行 |
| F-006 | `inference_session.py`、X临时路径、兼容输入保护 | `backend/tests/test_inference_engine_historical_readonly.py`、`test_inference_cache_lifecycle.py`、`test_alpha158_batch_parity.py`及真实WSL加载 | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATING | 当前保留预算只约束模板，不是总RSS硬上限；跨分区修订/总worker预算待对应实现 |
| F-007 | §3.1、§3.2、`performance_probe.py`、阶段计时 | `backend/tests/advisory_historical_range/test_inference_stage_counters.py`；最终LSTM两日/TCN首日产物及提交前37项最小矩阵 | APPROVED_BY_USER_SCOPE_IMPLEMENTATION_VALIDATED_CI_PENDING | 最终源码绑定PASS，LSTM约4.02倍/TCN约3.82倍；公共CI覆盖待owner修复，长窗口、原生消费者及LT1整体未完成 |

## 11. Rollout / Rollback / 发布、回退与源码验收

维持现有单日公开调用兼容，批量优化以明确任务/session接入；同一调用不能暗改包、日期或业务政策。新路径失败可在新的明确尝试中采用既有正确单日路径，但必须报告实际执行方式，不能静默回退后称批量性能已通过；错误数据不能通过回退变成成功。

不主动启动、终止或恢复已有正式 run，不覆盖旧输出。共享缓存可按明确任务生命周期回收，但不删除正式产物、其他窗口工作区或唯一输入。后端源码合入与用户重启、运行态身份读回分别完成；standalone只读性能测试不以等待实盘新交易日为前置。

本次包括已登记源码、测试和本文：`production_ddl_gate=noop`、数据库写入/数据激活/依赖安装/后端重启/实验训练均`noop`；独立只读实际包预测已执行。交付检查包括设计条款、最终源码定向测试、`git diff --check`、真实全评分/身份一致性和性能。合入后已有backend进程不会自动加载新源码，等待用户重启后另做只读读回，不把standalone验证称作生产启用或收益确认。

DESIGN-COMPLIANCE-001逐项结论：不把待实现设计冒充全部完成；失败/缺口和浮点差异显式报告；原包/模型/业务政策不变；不新增策略准入或细分人工审批，原模块所有权和用户重启权限保持不变。

## 12. Risks / 风险与直接处置

| 风险 | 直接处置 |
|---|---|
| 全区间特征、今天前复权或未成熟标签污染历史日期 | 日视图能力隔离、逐日复权/标签合同及未来毒化；不能以快路径为由改变 |
| 动态因子/processor写共享文件、全局CWD并发串扰 | 最小隔离工作块、静态输入写保护；受控串行/worker，不盲目线程化 |
| 缓存越积越大、旧修订误命中 | 字节预算、原子发布、修订失效和精确传播，报告不可复用例外 |
| owner交接把两个长任务变成全平台工程 | 只交付本次推理链最小接口和直接回归，现有产品边界不变，不创建新平台/台账/UI |

## 13. Production Gates / 本次源码及后续实现的生产影响

本次数据库、依赖、数据激活、进程控制、模型训练及运行态激活均 `noop`。后续源码合入依具体任务授权；公共owner交付不意味着Advisory获得该模块修改权。后端重启仍由用户执行；重启未完成只影响对应运行态验证，不阻断不依赖重启的只读开发/历史验证。

变更记录：v1.0 将用户确认的优化方向组织为 LT1/LT2，并明确主设计继承、真实基准缺口、批量/增量/权重边界与实际 owner；自审补齐拟新增接口、短事务、独立评分/业务artifact及可验证的设计矩阵。

v1.1：实施Advisory归属的计时、冻结资源复用、精确Alpha158、HDF alias去重、只读日期计划和X临时路径；补充真实首日对照及未完成项，修订原“仅文档/源码均待实施”的过时状态，保留完整LT1/LT2和其他owner边界。

v1.2：更新最终源码绑定的LSTM两日及TCN首日实际对照、37项最终定向矩阵和公共CI剩余缺口；按真实热点明确跨日/跨包输入、共同特征、因子文件往返和原生批量接入优先级，补充EWM移动起点及长窗口内存限制，不将七分钟单日改善冒充完整批量/增量完成。
