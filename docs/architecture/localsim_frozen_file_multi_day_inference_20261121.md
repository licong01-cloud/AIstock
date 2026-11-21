# LocalSIM 冻结文件回放的跨日推理会话

## 背景与范围

F2 接入，延续 [原 LocalSIM 公共内核设计](localsim_shared_selection_signal_kernel_integration_20261014.md) 的跨日接口。本次让 LocalSIM 冻结文件回放复用原生推理 CLI 已有的多日会话和原始数据增量读取。原生策略包准备保留每种子的资产、因子顺序和模型，源计划保留每天的决策日、信息截止和目标日。

## 边界

本次处理选股推理。动态因子公式、跨日因子输出复用、Advisory 入口、TWAP 时段与撮合、训练、实时账户、资金和账本沿用各自当前合同。数据库段沿用按日输入会话。明确声明日级 PIT 的原生工作区沿用逐日处理。

## 架构与契约

1. 冻结文件路径上的原生 LocalSIM 释放按当前日和相邻下一计划交易日，最多向现有 CLI 提交两日。下一日须具有相同文件绑定、独立 PIT 人口和适用的运行时租约。模块采用各日源计划的 C/D/T。CLI 继续启用历史只读。
2. 首日使用现有原生工作区。次日的同一批冻结种子通过显式 PIT 请求提供该日成员。CLI 使用原生包资产和数值算法，在一个消费者会话内缓存原始输入。各日窗口、成员、计算结果和来源回执仍独立。
3. 预计算成绩留在 LocalSIM 自己的 node 目录。后续当天的原生准备在同一进程中以释放、源计划、冻结文件、种子、模型、处理器、因子代码和工作区语义身份查找已完成的原始批次。读取时核查冻结组件内容、批次索引 SHA、种子路径和 SHA、消费者和时钟、当前日 PIT 人口及完整输入回执。随后继续当天现有的融合、过滤、TopK 和每日证据事务。进程内索引上限为 32 个日期及种子组，进程重启后的已提交证据使用现有断点。
4. 下一日预检缺失输入，或 WSL 在首日合法前缀之后的下一日原始计算报错，首日仍按原合同计算及验证全部种子。后一天随自身请求重新走原生准备，错误记录进入首日推理元数据。当前日计算、索引完整性、来源或身份错误沿原异常路径报告。

## Design Acceptance Index

| design_item | requirement |
|---|---|
| F-001 | 两个连续冻结文件交易日在同一个原生只读 WSL 节点处理并使用增量读取 |
| F-002 | 每日 C/D/T、成员、种子、分数及实际输入回执与原生逐日执行等价 |
| F-003 | 已完成原始结果仅由同一释放、源计划和工作区语义身份消费，文件篡改失败 |
| F-004 | 保持逐日融合和证据提交，保留首日合法前缀，按日处理数据库切换或显式 PIT |

## 实施方案

工作树写入范围为本设计、LocalSIM 原生选股服务、LocalSIM 推理适配器和现有选择推理定向测试。复用共享 WSL 历史请求、输入源计划和原数值流程。无需引入依赖、配置或数据库迁移。

## 验证方案

先验证多日请求形态、下一交易日的新工作区消费、跨日 PIT 独立、完成前缀、隔离、索引篡改和文件到数据库边界。然后使用已登记多 Alpha 策略包及 2026-01-05、2026-01-06 两日的冻结文件，以只读进程比较多日与逐日推理。基准使用同一模型、处理器、源计划、冻结绑定和原生融合入口。新生成的工件放在独立 X 盘范围，生产数据连接只读，原证据和生产账本保留。

真实验证在候选源码起新进程，最多两个相邻日期及一个策略包的同一 Alpha 种子，节点具有进程超时。预检确认两个日期均为 DATASET_FILES，实际文件绑定固定，验证源码与回执绑定。分数、排序、人口或输入回执差异均保留证据。性能记录实际节点次数、增量读、耗时和日级内容核对。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | LocalSimSharedInferenceProvider | artifact:X:/AIstock_temp/LOCALSIM/multi-alpha-20261120/prefetch-frozen-final-fund-UPDATED3-20261121T054923Z.json | passed | 无 |
| F-002 | 原生工作区和每日来源回执校验 | artifact:X:/AIstock_temp/LOCALSIM/multi-alpha-20261120/prefetch-frozen-final-fund-UPDATED3-20261121T054923Z.json | passed | 无 |
| F-003 | LocalSIM 原始结果校验 | pytest backend/tests/simulation_runtime/test_localsim_selection_inference.py | passed | 无 |
| F-004 | 原每日选股与证据入口 | pytest backend/tests/simulation_runtime/test_localsim_selection_inference.py backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py | passed | 无 |

首个真实分支验证为 UPDATED3 FUND，覆盖 1 月 5 日和 6 日的全部 6 个种子请求。次日增读 7 个原始分区，复用同一进程产生的 node 工件。两日分数、成员和来源回执内容分别等同于各日已有串行结果。回执 SHA256 为 d1007fc3273cec9b33e40e1114b3af5866bdadd622cfaabd6368e746b0e334d0。

## 风险与发布回滚

首日推理返回可能等到次日预计算完成，后续请求可节省节点启动与文件读取。节点请求窗口限定两日，保留批次总体耗时。源文件或节点输出校验失败时沿现有异常传播。源码回滚使用本次变更的前版本，未提交预计算属于临时工件，已提交选股证据与账本仍由现有消费者管理。

## 生产门禁

DDL、依赖与训练门禁均为 noop。现有 backend-main 用户重启合同适用。完成源码与只读验证后再执行运行核验，数据、账户和执行状态依现有授权管理。
