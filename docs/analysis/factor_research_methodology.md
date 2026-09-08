# 因子研究方法论

版本 1.0；依据：因子演进蓝图与 P1 详细设计。本文是研究方法唯一正文，不取代项目开发标准，也不授予生产写入、实验启动或进程控制权限。

## 目的

Codex/Claude Code 提出假设、编写受审查代码、解释对照；AIstock 提供已有数据、确定性指标工具和研究记录。以有用途、有增量、可维护的因子库为目标，不以新因子数量为目标。已有因子的诊断、改进和有依据的退出建议同样是研究成果。

## 每轮研究

1. 先用 `scripts/factor_research.py list/show/context` 查相关研究及已有因子，避免重复试错。历史事实来自数据库记录，不根据聊天印象推测。
2. 写研究卡：问题、用途/持有期、机制或文献依据、预期方向、数据字段/单位/可用时间、相近基准、对照、削弱假设的结果、反馈/独立评价窗口、方法版本。
3. 选择足够区分假设的少量候选；不强制批量大小、新字段数或固定 IC 线。先检查与已有因子的公式/用途重叠，再安排必要计算。
4. 审核候选实现后运行本任务子进程；已有产物优先复用。任务与尝试登记后才能开始新计算；数据库提交失败保存结果，用 attach 补登记，不重跑。
5. 分层解读：实现正确 → 信号表现 → 组合信息增量 → 真实策略表现。独立指标不能证明策略盈利；相关性未计算不能宣称全库去重；无有效样本不能填零。
6. 登记条件性结论、未解决问题与下一步。已计划尝试有结果/真实失败、数据库读回、下一步明确即可结束；没有发现 alpha 也可完成一轮。

研究卡可以是 tasks.context_json，尝试/反馈/结论保存在 records。新窗口通过状态、因子名、问题关键词发现任务，读取当前快照和最近记录。方法改版不自动改写过去结论。

## 评价与因果时间

- 候选特征不得读取未来价格/未来成员关系；截断前缀与跨股票边界测试用于检查实现，不是每次导出追加的全量数据验收。
- 标明数据截止日、暖启动、信号评价期和成熟标签范围。共享引擎 h1 是 T+1 到 T+2，h20 是 T+1 到 T+21，不另造收益公式。
- 已参与提示词改写、候选选择或参数选择的窗口标记 `research_feedback`。数据库字段名 out_sample 不自动等于未触碰的独立样本外；不能反复看结果后仍宣传独立验证。
- 保留数据缺失、计算失败、标签未成熟、信号弱和不可比较的差别。评价器已有报告保留，未知原因写未知，不编造停牌或补零。
- 因子方向与机制事先明确。查看结果后变更方向或公式属于新尝试，不覆盖旧结果。
- LLM 共识不是有效性证据。跨模型或跨数据版本的策略差异无法直接归因于因子；需 QE owner 提供可比对照。

## 保留并修正的技术规范

- 命名沿用小写字母开头、字母数字下划线，语义清楚；`m_` 为既有手工因子惯例，不通过改名覆盖旧因子。
- 结果为单列 DataFrame，列名等于因子名；MultiIndex 精确为 `datetime/instrument`，日期为日频无时区市场日期，索引唯一。
- rolling、shift、pct_change 必须按股票时间序列计算；可先 unstack instrument，再按时间轴向量化。不得在多股长表直接 rolling 导致串股。
- 缺失值可保留 NaN；不得使用 dropna 缩小理论覆盖分母，或用 0/前填掩盖缺失。Inf 是计算异常，需诊断；暖启动说明与数据缺失说明分开。
- 数据字段和单位以当前实际数据接口/组件说明为准；不沿用旧 skill 固定文件列数、旧路径、旧截止日期。显式选择已有数据，不重新导出/冻结/做全量哈希。
- P1 新评价显式 canonical v2、保留停牌/PIT；不进入旧 snapshot bootstrap。其他 authority 缺口交 owner，不关闭过滤或改业务规则。
- 候选采用已有依赖与受审查脚本，不导入 qrun/backend 主入口，不联网/写业务 DB、不自动安装。独立进程并非安全沙箱，不运行任意不可信代码。

## 存量质量维护

先对照原用途、长期与近期可比窗口、样本量、数据质量和已有同族因子，再判断观察、改进、替换或退出建议。单月 IC 下降不能自动证明永久失效；高相关也可能有数据依赖、持有期或成本差异。反向高相关需识别方向等价，不能自动翻转。

旧经验表可作为有日期/样本/用途的研究记录，不是永久成功/失败名单。低 IC、高相关不自动改 `is_available`；活跃消费者和历史实验引用未确认时不覆盖同名因子或删除旧版本。需要实际处置时走既有 owner 和精确授权。

## 工具与结果恢复

CLI 每个子命令通过 `--help` 查看参数；显式 `--env-file` 和 `--target dev|production` 选择现有配置。目标选择不是写生产授权。帮助不连 DB，create/record/run/attach 支持 `--dry-run`。

最小请求：

```json
{"task_id":"调用者生成UUID","record_id":"调用者生成UUID","summary":"开始研究",
 "task":{"title":"趋势内回撤","objective":"是否提供不同于已有趋势因子的排序信息",
 "task_type":"new_factor","factor_names":[],"context_json":{"method_version":"1.0","purpose":"research_feedback"}}}
```

更新请求提供 expected_revision 和本次新 record_id，payload 保存研究/执行/结论/需求的紧凑摘要。相同 record_id 重试必须提交相同请求；版本冲突先读最新进度，不覆盖其他窗口的更新。correction 引用同任务旧记录；不删除历史。

`run` 输入包含 task_id、开始记录 record_id、expected_revision、attempt_id、method_version、canonical universe_key、data_dir、qlib_bin_path、artifact_root、instruments、read_start/signal_start/signal_end/read_end/cutoff、candidates（factor_name/script）及可选 timeout_seconds。同一批次共享相同输入上下文；不同输入分开调用，不持有全局大缓存。

候选脚本参数为 `--data-dir --output --start-date --end-date --instruments`；instruments 是 JSON 名称数组。脚本应在读取时限制日期/股票/字段，仅输出 `--output` 指定的 H5 key=data。CLI 复制已审核脚本到本次稳定产物目录，依次执行并评价，不依赖之后会清理的任务工作树。

结果完整文件和 attach.json 在 repo-external task/attempt 下；数据库记录它们的引用和摘要。`computed_not_recorded` 表示已算但登记失败，修复实际问题后使用 attach；未知 attempt 先查看实际进程/产物，不自动重启。相同 attempt_id 不重复执行，重新计算需另建 attempt 并关联原尝试。

## 正式交付与跨模块边界

`factor-research` 组织研究；`develop-factor` 保留兼容入口和正式交付引导，二者引用本文。正式交付仅复用[因子研究交付操作](../operations/factor_research_delivery.md)，研究候选不调用 catalog save/full-pipeline。

QE、QE 数仓、HMM、Selection/Paper/Advisory 的代码/配置需求交专用窗口，记录输入输出、owner、交付引用与验收，不跨界修改或启动实验。不得为了本任务停止其他工作、增加资源硬阈值或建复杂平台。
