# 因子演进实施计划与 P1 详细设计

日期：2026-09-08；版本：1.1；设计级别：F2；当前修订交付：文档。用户授权文档合入后开始 P1 代码实施，生产迁移/激活仍单独授权。

上位设计：[因子研发与质量演进蓝图](factor_research_evolution_blueprint_20260908.md)，已合入 PR #4435。本设计细化其 P1，不改变三阶段范围、不迁移 QE 业务语义、不引入新平台。文档合入只确认设计交付，后续源码、DEV、生产应用分别报告与授权。

<a id="plan"></a>
## 1. 背景与整体实施方案

目标是让 Codex/Claude Code 通过 AIstock 的共享工具进行有依据的研究，并从数据库恢复历史和进度；同时逐步维护少而精、用途明确的因子库。最快落地方式是三个交付包，不再拆出独立“平台建设”“通用自动化”“全历史迁移”等前置阶段。

| 阶段/优先级 | 一个完整交付包 | 主要复用与协作 | 结束条件 |
|---|---|---|---|
| P1 最高 | 独立方法论、客户端薄入口、研究数据库记录/恢复、候选执行和基础评价、两类真实案例 | 现有 PostgreSQL 连接、因子指标引擎、只读 catalog；不等待 QE 新开发 | 代码与 DEV 验证通过；获准生产应用后可跨窗口恢复；研究不依赖对话，不要求发现有效 alpha |
| P2 次高 | 存量质量概览、衰变/异常/冗余诊断、改进/替换/退出建议与获准处置 | 现有官方指标、月度 IC、相关性；依赖消费者变更交 owner | 全库范围有已处理/待处理清单，建议可解释，旧实验/活跃业务不被破坏 |
| P3 按需 | 可比 QE 结果与研究关联、组合增量、必要趋势诊断、持续维护触发 | 向 QE/QE 数仓窗口提需求，复用已交付公开入口 | 能区别数据/模型/配置差异与因子贡献；无结果不伪造验证；不接管训练与数仓源码 |

P1 尽量作为一个实现 PR 完成文档、工具、数据库 migration 和测试；源码合入、DEV 验证、生产迁移不是新的业务阶段，但不能合并为一个虚假的完成状态。只有真实所有权冲突才拆 owner PR。P2 可在 P1 稳定后推进，P3 的明确接口需求可提前交接，不等所有窗口空闲或所有 PR 合入。

每阶段用具体完成项、数据库记录和剩余工作表示进度，不按生成因子数或没有分母的百分比衡量。旧历史补录不作为新研究启动前提。

## 2. P1 范围与非目标

### 2.1 必交付

1. 独立方法论文档，包含新因子、现有因子诊断/改进两种任务；Codex/Claude 入口引用同一正文。
2. 最小研究任务与记录数据库模型；创建、发现、读取、进度更新、记录尝试/结论、关联外部需求和断点恢复。
3. 一个 CLI 入口及少量共享 Python 模块：只读上下文、候选执行、共享评价、结果登记。
4. 不入生产 catalog 的候选研究空间，使用现有数据和指标口径，不复制正式指标写入器。
5. 一个新研究案例、一个存量诊断案例；数据库真实验证和新进程恢复。

### 2.2 非目标

不开发 UI、HTTP/MCP 三套入口、守护进程、scheduler、向量库、知识图谱、模型路由、自动研究总控或新实验数仓。不修改 QE/qrun、QE 数仓、HMM、Selection、Paper、Advisory、Research Assistant/Research Pipeline 代码，不运行完整 QE 训练/回测。P1 不自动淘汰/启用正式因子。

不新增数据集导出、冻结、全量哈希、备份、数据库快照、资源阈值或未经用户批准的门禁。必要的返回格式、幂等与事务规则是蓝图已要求的数据正确性实现，不扩展为人工审批或全任务阻断。

<a id="discovery"></a>
## 3. 已核实能力与设计决策

以下为本次源码核对，不是运行时/数据库已部署证明；本轮未查询生产数据库或执行代码计算。

| 源码/实际符号 | 核对结果 | P1 决策 |
|---|---|---|
| `backend/db/pg_pool.py:get_conn(*, autocommit=True, manage_transaction=False)` | 复用 TDX_DB 环境配置，支持管理事务 | 显式目标配置后调用；写事务用 `get_conn(autocommit=False, manage_transaction=True)`，不改共享连接模块 |
| `backend/db/init_research_pipeline_schema.py` | experiment.pipeline_type 限于 `hmm_research/event_signal_research`，包含 stage/promotion 等语义 | 不扩大其约束、不伪装类型；避免跨模块迁移 |
| `backend/db/init_research_assistant_schema_20260521.py` | 任务关联模型 profile、审批状态及 assistant 事件体系 | 不直接写其他模块任务；仅借鉴 task+record 的轻量模式 |
| `backend/services/rdagent_candidate_service.py:cache_task_loops/get_task_loops` | RD-Agent 实验缓存/同步，查询路径可能刷新写入 | 不作为只读通用历史入口，不伪造 RD-Agent task |
| `backend/services/quantevolver/factor_official_evaluation_service.py:compute` | 提交正式计算并最终写 metrics/monthly IC | 不能用于候选无副作用试算，不新增官方单写源 |
| `backend/services/quantevolver/qe_eval_v2_metric_engine.py:prepare_shared_context`、`compute_single_factor_metrics` | 可准备共享上下文，再输入单列因子计算；准备阶段调用 PIT ensure，只有已核实的 canonical v2 路径纯读，旧 snapshot 路径可能 bootstrap；单因子计算不自行入官方指标表 | P1 新评价使用当前 canonical v2 的只读路径，显式路径/日期/股票池，研究输出另存；不把准备函数整体称为无副作用 |
| `backend/services/quantevolver/correlation_engine.py:compute_pairwise/compute_incremental` | pairwise 无有效天时返回零及 `effective_days=0`；incremental 目前调用合并集合的全矩阵 | 不把无有效配对的零认作独立；不承诺名称带 incremental 就只算新旧对；P1 只读已有可比相关性，需新计算时向 owner 提精确需求 |
| `backend/services/quantevolver/offline_code_text_factor_executor.py` | 使用进程内 pandas 写入重定向，结果归一化可能取首列/保留最后重复行 | P1 新候选不接入此宽松路径；使用独立进程及明确单列产物，不修改旧 executor |

参考：[官方指标单写源设计](factor_independent_metrics_single_writer_design_20260427.md)。本设计复用公式与计算，不继承旧研究 skill 中固定 IC 淘汰线、强制新字段和永久失败黑名单。研究评价的 `calc_engine=qe_eval_v2` 仅说明计算公式来源；结果必须同时标记 `scope=research_candidate`，不能冒充正式因子评级或已批准策略。

### 3.1 现有函数契约

```python
prepare_shared_context(
    qlib_bin_path=None, start_date=None, end_date=None,
    instrument_hint=None, load_suspend_d=True,
    load_st_pit_mask=True, universe_key=OFFICIAL_FACTOR_UNIVERSE_KEY,
) -> dict
compute_single_factor_metrics(fname, factor_df, ctx) -> dict
```

调用时不依赖默认数据路径，不能将两个布尔参数关闭来绕过既有停牌/PIT 语义。现有 h1 为 T+1 到 T+2，h20 为 T+1 到 T+21；使用共享引擎的成熟标签与报告，不另写收益公式。无可用数据时返回真实错误，不回落旧数据、默认行业、空股票池或补零。

P1 新评价绑定当前数据集的 `aistock_equity_pit_canonical_v2`：已核实 `FactorUniverseMaskService.ensure_ready` 对该 key 调用 `get_status_readonly`，load_spans 为 SELECT，停牌读取也是 SELECT。旧默认 snapshot key 会调用 `ensure_immutable_dataset_snapshot(bootstrap_if_missing=True)`，不能原样用于本研究的无写入准备。因此不省略 universe_key，不自动使用旧 key；旧研究指标仍可只读查看。需要其他 authority 的新计算时，先交接只读入口需求给其 owner，不关闭 PIT、不自行 bootstrap，也不改变其他模块可用股票池。

<a id="method"></a>
## 4. 方法论与客户端架构

### 4.1 唯一方法正文

拟新增 `docs/analysis/factor_research_methodology.md`，在 P1 实现包中正式交付，版本从 `1.0` 开始。正文定义：问题/用途、文献与现有因子对照、预先假设、有限候选、实现/信号/组合/策略四层评价、条件性结论、下一轮决策。

研究卡最少字段：研究问题、用途/周期、机制依据、预期方向、可用字段、相近基准、主要对照、什么结果削弱假设。没有强制因子数、新字段数或统一 IC 生死线。

研究经验有适用条件，不将单次 h1 失败上升为整个方向失效。研发反馈窗口不能继续称为全新独立测试；方向和评价器改动需另记尝试，不看结果后悄悄翻转。实际策略收益与独立指标的分组诊断不是同一概念。

存量诊断卡沿用同一模板，另记原用途、长期与近期可比窗口、样本成熟度、与同族因子的差异及当前消费者引用是否已确认。低近期 IC、高相关或反向相关单项均不直接判定应淘汰；先区分数据/执行故障、用途不匹配与条件性衰变。P1 可输出诊断和建议，P2 才集中形成全库质量维护与获准处置能力，避免先研发大量新因子再考虑重复。

### 4.2 客户端入口

拟新增 repo-owned `.codex/skills/factor-research/SKILL.md` 与 `.claude/commands/factor-research.md` 两个薄入口，引用同一方法正文和 CLI `--help`。名称为规划，当前不存在该新工具。不修改 AGENTS.md，不重装所有 profile，不复制个人 skill 内容为第二份权威。

流程为：查数据库中的相关任务 → 创建/继续任务 → 读取必要上下文 → 形成研究卡 → 执行/登记 → 解读结果 → 更新下一步。`factor-research` 是研究组织入口，`develop-factor` 保留原名称作为研发兼容/正式交付入口；二者引用同一方法正文，不维护两个评价政策。正式交付沿用现有工具，不由研究 CLI 复制入库写入器。

### 4.3 现有 skill 的升级与复用清单

当前 `develop-factor` 位于客户端 profile，非 repo 已跟踪权威。本轮只读核对表明：其所依托的 `ManualFactorService.save_factor` 已保存源码、catalog 和 asset_path，并调用分类；官方指标与评级也有现有服务。不能把这些既有能力重新开发一遍，也不能把旧 skill 中所有命令视为当前有效。

| 内容 | P1 处理 |
|---|---|
| 命名、单列 MultiIndex 输出、无未来信息、按股票隔离、量纲/字段、合理向量化 | 保留为方法正文中的技术规范；旧模板的长表 rolling/pct_change 需明确分股，示例不得串股 |
| catalog/源码关联、官方独立指标、分类/评级、相关性工具 | 复用既有能力；只核对精确调用/返回，不改业务源码；分别报告成功和缺失 |
| `/factors/manual/save` | 旧说明已漂移；当前实际入库路由为 `/api/v1/quantevolver/factors/manual` |
| `batch-compute-metrics-unified` | 当前为兼容入口，转官方 writer；正式交付说明优先指向 `/api/v1/quantevolver/official-evaluation/compute` |
| 固定数据根、旧截止日期、旧 RD-Agent writer 描述 | 改为显式实际输入与现行单写源；不回落旧数据；不在 skill 放凭据 |
| dropna 和缺失 | 区分输出格式与评价覆盖；保留缺失/暖启动说明，不通过删行伪造完整覆盖 |
| 永久 P_fail、每批必须 5–10 个、固定 IC/相关性自动淘汰线 | 改为有用途/样本/周期条件的经验；不自动改 is_available，不因固定阈值淘汰 |
| 历史窗口反复用于研发筛选 | 标注 research_feedback，不能继续声称独立样本外验证 |

P1 增加 repo-owned `.codex/skills/develop-factor/SKILL.md` 与 `.claude/commands/develop-factor.md` 兼容薄入口，以及 `docs/operations/factor_research_delivery.md` 作为唯一正式交付操作参考。该参考只引用既有业务入口，列出保存、官方评价、分类/评级、相关性各自副作用与结果读回；不提供通配 SQL 淘汰、直接覆盖同名因子或绕过正式 writer 的备用路径。

个人 profile 同名入口的替换交现有单一 client-sync owner，按明确目标和现有流程处理；先确认现有同步工具支持这些业务入口，不支持则交 owner 补齐，不在研究模块私建安装器。不得扫描/覆盖所有用户配置或修改其他活动窗口。实施验收分别报告 repo 入口完成与客户端同步状态，未同步不能声称所有窗口已切换。旧入口用户继续使用熟悉名称时也应读同一方法论，不允许旧硬阈值悄悄重新生效。

研究候选始终不调用 save/full-pipeline。正式入库/更新及因子可用位变更只有明确批准后执行；LLM 分类返回空、指标失败或相关性无有效样本都不能被“保存成功”掩盖。同名保存当前会覆盖代码，因此交付前明确是新因子还是获准版本更新，保护活跃消费者和历史引用；能力缺口提交 owner，不在 P1 重写业务。

方法论可独立演进，任务/尝试记录具体版本与代码引用。更新方法不自动重算旧研究，也不改变旧结论的原始适用条件。研究事实不写入用户个人模型记忆充当数据库替代品。

<a id="database"></a>
## 5. 数据库详细契约

### 5.1 两张小表，不建设通用平台

拟在既有数据库的 `public` 命名空间新增 `factor_research_tasks`、`factor_research_records`，不创建新数据库或 schema。名称属于本设计方案，不代表已存在。只保存摘要与引用；不引用其他业务表的级联外键，因子名称/版本作为逻辑引用，避免因旧业务记录变化丢失研究。

**factor_research_tasks：当前可直接查询的任务快照**

| 字段 | 类型/语义 |
|---|---|
| task_id | UUID 主键；客户端在提交前产生，响应丢失可按 ID 查询 |
| title, objective | TEXT；标题和研究问题 |
| task_type | TEXT；`new_factor/diagnosis/improvement/redundancy` 四类用途，不是审批级别 |
| status | TEXT；`active/paused/completed`；结束不等于找到 alpha |
| phase, completed_summary, next_action, blocking_reason | TEXT；具体进展，阻碍可空；无百分比字段 |
| factor_names | TEXT[]；可为空，通过名称发现相关历史 |
| context_json | JSONB 对象；研究卡、用途/周期、方法版本、数据/基准引用和负责人标识 |
| revision | BIGINT 从 1 递增；防止并发窗口静默覆盖进展 |
| created_at, updated_at | TIMESTAMPTZ，数据库时间；显示时才转换时区 |

**factor_research_records：每次实质研究更新的紧凑记录**

| 字段 | 类型/语义 |
|---|---|
| record_id | UUID 主键，提交前产生，兼作幂等请求 ID |
| task_id | UUID，指向本模块 tasks，ON DELETE RESTRICT |
| task_revision | BIGINT；该次事务成功后任务的 revision，`UNIQUE(task_id, task_revision)` |
| record_type | TEXT；`created/progress/attempt/result/decision/request/correction` |
| attempt_id | UUID 可空；串联同一次计算开始/结束的摘要，不创建另一套任务队列 |
| related_record_id | UUID 可空；指向同任务的前序尝试/被更正结论；实现时验证同任务 |
| summary | TEXT；短说明，不存完整日志/聊天 |
| payload_json | JSONB 对象；本次请求、研究快照、代码/结果/需求引用，见 §5.2 |
| created_at | TIMESTAMPTZ；顺序以 task_revision 判定，不以客户端时钟判定 |

必要索引：tasks 的 `(status, updated_at, task_id)`；factor_names 的 GIN；records 的 `(task_id, task_revision DESC)`；以及主键/唯一键。对 `record_type='attempt'` 的开始记录增加 `(task_id, attempt_id)` 部分唯一索引，并要求此类 attempt_id 非空，防止不同 record_id 重复启动同一计算；result 等后续记录不受此唯一索引限制。任务问题查询初期参数化 ILIKE，不新增搜索服务。SQL 仅基本主键、外键、唯一性、revision 正值及字段形状/状态校验，不增加 hash 列、trigger、审批表或平台级门禁。

任务快照供恢复直接读取，records 供解释历史，不通过重放事件日志才能恢复；不记录每次工具调用。普通 CLI 不提供删除记录命令，更正新增记录，不抹掉原结论。

### 5.2 payload 与真实状态

- `request`：用户提交的规范化请求体，供幂等比较；使用 JSON 值比较，不计算内容哈希。
- `research`：假设、相近基准、预期方向、主要对照、用途、方法版本、`research_feedback/independent_evaluation` 等真实窗口用途。
- `execution`：实际计算任务/尝试标识、环境、已完成/进行中/失败/未知状态、错误类型；不凭窗口退出猜测进程已停止。
- `artifacts`：代码版本或持久文件、候选值/指标文件、数据库指标批次/日期；摘要必须能找到原结果，不能只指向将被清理的临时工作树。
- `decision`：保留/观察/改进/替换建议/退出建议、依据及适用条件。建议不更新正式 `is_available`。
- `external_request`：需求描述、负责窗口、所需输入输出、验证条件、Issue/PR/交付引用、当前交接状态；P1 记录需求，不开发另一套项目管理。

JSON 非有限数值不得转成零。指标的 null 保留并附共享引擎实际报告；没有原因则记为未知，不造原因。只保存非秘密的配置位置/目标别名，禁止 DSN 密码、令牌或完整环境变量。

### 5.3 事务、幂等和并发

创建：同一事务 INSERT task revision=1 与 created record；任务已存在且原请求相同则读回，不创建第二项；同 ID 不同请求返回 `request_conflict`。

更新：先按 record_id 查已提交结果；请求相同则幂等返回，即使客户端持有旧 revision；请求不同返回冲突。不存在则在一个短事务执行 `UPDATE ... WHERE task_id=%s AND revision=%s RETURNING revision`，再 INSERT record。无匹配行时区分不存在和版本已变化，返回当前 revision/进度供重新读取，不自动覆盖。record 写入失败则 task 更新一起回滚。

两个相同请求并发时，后到请求可能命中唯一冲突，也可能先看到 revision 变化；两条失败路径均结束当前事务，再按 record_id 查询已提交记录并比较原请求，相同即读回。不存在同请求记录才返回实际版本冲突。不同请求竞争任务 revision 时，后者重读并由研究者合并实际进度；不全局加锁、不影响其他任务。同 task/attempt_id 使用新的 record_id 重复请求返回 `attempt_exists`，不能绕开唯一性再次执行。

写请求响应明确返回 `applied=true/replayed=false` 或 `applied=false/replayed=true`；只有首次成功创建 attempt 且 applied=true 才可启动计算。规范化请求仅统一 JSON 表示，不改业务值；request 中保留调用者提交的 expected_revision，不将服务器生成时间/最终 revision 加入比较。同 ID 重试提交原请求即可，无需改写旧 revision。

读取用 `get_conn(autocommit=False, manage_transaction=True)` 后在查询前 `SET TRANSACTION READ ONLY`，只查询这两张表或已确认只读的业务来源；参数化 SQL，分页稳定排序。不调用会刷新/写缓存的“查询”服务。

连接目标由既有 TDX_DB 配置提供，CLI 显式指定 `--env-file <已有配置文件>`；不打印秘密，不因为未找到 DEV 配置静默连接默认生产。`--help` 不加载配置、不连接 DB、不建表。此为目标选择语义，不能当作新的生产授权。

<a id="cli"></a>
## 6. CLI 与候选执行契约

拟新增 `scripts/factor_research.py`，使用既有 Python 环境，不新增依赖。以下为设计中的新命令，不声称当前可运行：

| 命令 | 参数与作用 | 副作用 |
|---|---|---|
| `create` | `--input task.json`，含预生成 task_id/record_id | 写两张研究表 |
| `list` | `--status/--factor/--query`，`--limit` 默认 20，提供分页游标 | 只读发现任务，不需旧聊天中的 ID |
| `show` | `--task-id`，任务快照+最近记录；`--before-revision` 翻页历史 | 只读，返回恢复信息 |
| `record` | `--input record.json`，含 expected_revision 与完整本次请求 | 原子记录进展/尝试/结果/更正/需求 |
| `context` | 显式因子/任务与评价时间条件 | 只读读取 catalog/已有指标及相关性摘要，零条返回真实 unavailable |
| `run` | `--input attempt.json`，指定 task、代码、数据与产物根；使用运行 CLI 的已有 Python 环境 | 仅本任务候选计算、研究记录，不启动服务或 QE |
| `attach` | `--input result.json`，已有 attempt_id 与结果引用 | 登记已完成产物，不重跑、不改官方指标 |

共有 `--format summary|json`，JSON 顶层 `ok/task_id/record_id/revision/applied/replayed/result/error`，不适用字段为 null。exit 0 表示请求完成而非因子有效；输入/版本冲突、外部执行/数据库失败返回非零和原因，不再包装成静态成功。`list` 空列表合法，指标缺失与零值不同。分页是输出控制，不是业务门禁。

`attempt.json` 必须明确 task_id、开始记录 record_id、expected_revision、attempt_id、候选名与脚本、方法版本、已有数据路径/截止日、数据读取范围与信号评价范围、PIT universe_key、样本股票、产物根。数据读取范围包含必要暖启动与已存在的成熟标签期；不得读取超出所声明截止日的数据。只登记这些现有引用，不重新冻结或计算数据集哈希。结果同时记录请求范围、实际可评价范围和引擎报告；尾部标签尚未成熟不补值、不冒充完整窗口。

### 6.1 run 的具体流程

1. Codex 先完成研究卡和审查过的候选脚本。脚本使用显式 `--data-dir/--output`，只写指定候选结果；原数据只读，不修改已有正式因子源码。
2. run 先登记 attempt，保存预生成 attempt_id、代码引用、输入和计划输出位置；数据库不可用时不启动新计算，已有计算结果可用 attach 恢复。该失败只作用于当前请求。
3. CLI 由使用者在选定的已有 Windows 或 WSL Python 环境启动，候选子进程使用同一 `sys.executable`；路径必须属于该环境，WSL 显式传入当前任务 worktree 和数据的 `/mnt/...` 路径。不在一次调用中跨 Windows/WSL 调度、隐式加载 canonical root、改造远端执行器或 backend/worker 进程中 exec/monkeypatch。P1 默认顺序执行一个小批次，不建设调度器，不把顺序设为其他模块的资源限制。
4. 脚本输出单列 `factor_name`、MultiIndex(datetime, instrument) 的 result.h5 或明确指定的等价 Parquet；adapter 检查本产物的名称、索引唯一性和数值形状，不自动取首列或保留最后一条冲突记录。检查限于本候选输出，不扫描全数据集。
5. 同批候选共享一次 `prepare_shared_context`；依次调用 `compute_single_factor_metrics`。共享上下文位于独立 CLI 研究进程，结束后释放，不修改服务缓存。显式传入 `instrument_hint/start_date/end_date`，准备阶段只读所需股票和日期，而不是载入全市场后再取 40 股。候选脚本子进程已退出后再评价，避免同时保留两份大面板。使用独立解释器进程只隔离 Python 状态，不宣称它是安全沙箱。
6. 保存候选值与共享引擎原结果，另写紧凑研究摘要；明确 `research_candidate`、样本范围和标签口径。记录结果并读回后报告完成。

共享上下文只在数据根、截止/读取范围、股票集合、universe_key 和评价口径一致的候选之间复用；条件不同则顺序准备各自上下文并释放旧对象，不用首个候选的上下文替代所有候选。比较的是显式参数，不增加文件哈希或全量复验。只比较事先指定的相同信号评价范围；输入保留暖启动数据不等于把暖启动期间计入评价。

适配器先保留 prepare 生成的前瞻收益，再对传给单因子计算的上下文视图做同一信号日期切片：`close_unstacked/fwd_ret_mats/dates/st_pit_eligible_mask` 及 factor_df 同步切片，`data_start/data_end` 描述实际评价区间；原输入范围和 authority metadata 单独保留。不得先裁掉标签所需的未来价格再重新计算收益。此处只做输入适配，不改公式/标签或共享引擎源码；定向测试同时比较手工选择的同一上下文调用结果，避免暖启动和尾部标签价格进入覆盖率分母。

新生成代码应是已审核的受信研究代码，不允许网络/DB 写入、任意清理、业务配置变更或导入业务入口造成隐式副作用。执行命令不执行用户代码中的任意“安装/修复环境”建议；发现依赖问题交回处理，不自动安装。输入目录与输出目录必须分离；不复制完整数据、不创建生产指针，不使用已存在输出目录覆盖上一次尝试。

### 6.2 长任务与异常恢复

任务产物根由调用方明确给出 repo-external task root，推荐复用现有 X 盘研究根下的独立 task/attempt 子目录；目录选择不是固定磁盘门禁，不迁移其他数据。记录中保存可跨会话找到的稳定引用。

run 在本次输出目录保存轻量 `execution.json/result.json` 作为进程间结果交换；数据库仍是研究历史权威，文件仅承担窗口/写库中断时的恢复输入。计算成功但数据库提交失败返回 `computed_not_recorded` 和准确路径，不重新计算。

响应丢失时先 show/读 record_id，再用 attach 补登记。结果登记遇到其他窗口先更新 revision 时也保留已算产物并报告 `computed_not_recorded`；读取新进度后以新记录请求 attach，不能通过重跑计算解决进度冲突。已有 attempt 状态未知时检查其实际任务/结果，不启动第二个同 ID 计算；需要重新计算则创建新 attempt 并关联旧尝试。避免重复的是同一次动作，不限制提出新研究。

如果两个客户端同时请求同一 attempt，只有成功记录新 attempt 的调用方执行；幂等读回方只返回已有状态，不再次启动。中断在“已登记但尚未启动”之间时保留未确认状态，按实际结果处理，不自动循环补跑。

### 6.3 P1 基础评价与相关性边界

正式 `FactorOfficialEvaluationService.compute()` 不用于候选试算。P1 复用纯引擎，不写 catalog、metrics、monthly IC、classification、ratings、correlations 或 QE 数仓表；研究摘要保存在本模块 records，完整输出在研究产物根。

现有因子诊断读取实际官方指标及批次/日期，不触发重算；新因子相关性未计算时明确缺口，不能声称全库去重通过。已有相关性记录没有有效样本信息或口径不一致时，不猜测可比性。需要新旧增量相关性接口的需求在 §9 提交给 owner，不在 P1 偷改全矩阵实现；这不阻断研究记录及基础评价交付，也不允许在缺少增量证据时推广候选。

<a id="files"></a>
## 7. 拟实施文件范围与模块所有权

本次 v1.1 修订只修改本文，归属 `docs.architecture`。下表为后续 P1 实施包规划，不是文档 PR 已经改动的文件，也不自动授权其他 owner 的代码。

| 规划位置 | 职责 |
|---|---|
| `docs/analysis/factor_research_methodology.md` | 独立方法正文 |
| `.codex/skills/factor-research/SKILL.md`、`.claude/commands/factor-research.md` | 客户端薄入口，走现有受控流程 |
| `.codex/skills/develop-factor/SKILL.md`、`.claude/commands/develop-factor.md` | 保留旧名称的兼容/交付入口，引用同一方法，不修改 profile 文件 |
| `docs/operations/factor_research_delivery.md` | 复用现有因子库工具的单一操作参考，不另造 writer |
| `backend/services/factor_research/models.py` | 明确请求/记录形状、错误，不另建框架 |
| `backend/services/factor_research/repository.py` | 两表的短事务、查询和幂等 |
| `backend/services/factor_research/service.py` | 请求编排、候选记录、只读上下文 |
| `backend/services/factor_research/runner.py` | 独立进程运行、共享指标函数适配、产物登记 |
| `scripts/factor_research.py` | 单一 CLI；必要包 `__init__.py` 随实现添加 |
| `backend/migrations/factor_research_p1_20260908.sql` 及精确 preflight/rollback 伴随文件 | 仅本模块两表 migration，不在 import/启动时建表 |
| `backend/tests/factor_research/` | 请求、事务、恢复、隔离与计算契约测试 |

归属建议挂在现有 `factor_library`，不是创建新的大型业务模块。新增路径的 ownership/test plan 精确登记由相应 workflow owner 执行或在已批准范围内补充；仅新文件未被现有 catalog 覆盖时补登记，不宽泛调整 `quantevolver/**` 的所有权。

实现登记采用 `factor_library.research` 验证子边界，只匹配本 P1 新路径，避免把研究 DEV 测试扩散到所有现有 factor_library 改动。它只是现有 catalog 中的测试路由，不增加业务服务/平台；普通 CI 与真实 DEV 计划分开，其他因子/QE 文件所有权不变。

2026-09-08 用户补充授权：将 `docs/standards/aistock_runtime_targets_v1.yaml` 加入精确写入范围，仅把 `scripts/factor_research.py` 登记为独立 CLI 的 non-runtime source。不改变五个 backend 新文件的 catalog 推断、任何 runtime target 或通配规则，不推导进程操作权限。

实际实现按 changed files → ownership → module registry → test plans 选择最小验证；P1 不直接修改 `backend/services/quantevolver/**`、`qe_archive/**`、`research_assistant/**`、`research_pipeline/**`、对应 routers 和它们的 migration。发现缺口提需求。

runtime_impact 按最终 catalog 推导，不因 CLI-only 擅自降级 backend Python；若要求 backend-main 重启由用户执行。尽量通过独立 CLI 实现，避免引入必须常驻的新服务；客户端同步仅精确受影响入口，沿现有流程处理。

<a id="migration"></a>
## 8. DEV、生产发布与回滚

本设计不执行数据库查询/迁移。实施前只读核对既有 DEV/生产目标和两表是否存在、schema 是否匹配。以显式目标配置运行精确 migration；不创建新测试数据库，不备份导出，不重启其他模块。

DEV 顺序：preflight → 建表 → 真实创建/更新/并发/失败恢复/发现任务测试 → 新 Python 进程读回 → 保留与清理精确测试记录的计划验证。rollback 仅在两表为空且无人依赖时可撤销本次表；有研究历史时保留数据、采用兼容修复，不自动 DROP。

生产只有在用户明确授权这两张表和相应 migration 后执行。读回表结构、最小写入/恢复样例，分别报告 migration、记录写入与读取；不将源 PR 合入当作 DDL/DML 授权。任何生产演示记录均属于明确授权的本模块目标，不写其他业务表。

源码/DEV 完成与生产应用分开。生产未授权时可交付代码和验证，不宣称 P1 已正式启用；不得因此阻断无关开发。代码回退保留研究表与记录，旧业务不依赖新 CLI，停止使用新工具即可避免影响，不做数据删除型回滚。

<a id="handoff"></a>
## 9. 跨窗口需求与不影响业务的约束

| 可提前交接的需求（规划标识） | 给谁 | 输出/验收 | 是否阻断 P1 |
|---|---|---|---|
| FR-REQ-01 候选与既有因子的可比增量相关性 | 因子评价/相关性 owner | 输入候选值、基准与窗口，返回有效样本/口径/不可计算原因；不能内部全库重算却称纯增量 | 不阻断记录/执行/基础评价；未交付不能声称完成信息增量验证 |
| FR-REQ-02 可比 QE 实验结果读取 | QE 数仓窗口 | 实验/特征/模型/标签/股票池/时间关联、成本后结果及用途标记；只读查询不触发 ETL | P3 按需交付，不阻断 P1/P2 独立工作 |
| FR-REQ-03 趋势候选召回/失效/退出诊断 | QE 窗口 | 与当前批准实验一致的诊断，明确标签成熟与比较条件 | 仅对应策略评价需要；不得由本窗口改标签或启动训练 |

需求标识尚未登记为 Issue，不声称 owner 已接单。后续在研究 records 中记录明确需求、负责窗口、交付引用和验证结果，不依赖聊天确认。

所有研究使用 task-scoped 代码/输出和短数据库事务；只读 SQL 显式列、条件与分页，不长期持有事务等待计算。P1 两个样例顺序执行，先确定数据读取范围，不把独立进程误当成内存配额。发现有实际争用时只调整本任务方式或协商执行时段，不停止其他实验，不新增资源硬门禁。

生产数据路径、PIT、活跃策略和因子可用位均不变。能力需要跨模块代码改动时必须提交专用窗口，不能以“顺手修复”为理由越界。只运行所属模块及真实依赖的定向测试，不要求所有业务停止开发。

<a id="validation"></a>
## 10. 验证方案与两个真实案例

### 10.1 P1 实现测试矩阵

| 场景 | 验证方法 | 必须观察的结果 |
|---|---|---|
| 请求幂等 | 同请求重复、响应丢失、相同 ID 不同请求 | 不重复写；不同请求明确冲突 |
| 并发进度 | 两个 DEV 连接同时更新同 revision | 只一项成功，另一项重读；无静默丢历史 |
| 事务失败 | task 更新后模拟 record 写失败 | 两表一致回滚 |
| 任务恢复 | 新进程只用 list/query/factor 查找，不给旧聊天 ID | 找到研究、实际记录与下一步 |
| 历史更正 | correction 关联旧结论 | 原记录仍在，当前结论可解释 |
| 计算/入库断点 | 实际结果已落盘后模拟 DB 不可用，再 attach | 不重复执行候选；恢复真实 result 引用 |
| 同 attempt 并发启动 | 两客户端以相同/不同 record_id 请求同 attempt，覆盖 revision 竞争和唯一冲突两条路径 | 相同请求幂等读回；新 record_id 返回 attempt_exists；只 applied=true 方启动 |
| CLI 副作用 | --help、list/show/context、未知配置、未知任务，以及 canonical v2 准备与误传旧 snapshot key | 不建表/写官方指标/触发 ETL/bootstrap；错误真实，不重建 PIT |
| 候选形状/时序 | 多列、重复索引、空值、截断前缀、跨股票边界 | 不静默取首列/最后重复；无未来或串股票计算 |
| 共享指标一致 | 同一候选/上下文直接调用引擎与 adapter 对照，另测不同 universe/范围的连续请求与尾部未成熟标签 | 数值一致；不复用错误上下文；暖启动不计入信号评价；标签/窗口/覆盖元数据保留 |
| 既有业务保护 | 精确检查本任务输出和目标表；测试中阻止其他写入路径 | 不改数据集、catalog、metrics、QE/数仓配置 |
| 新旧入口一致 | repo 两个名称及 Codex/Claude 入口的引用检查，结合现行 API/服务源码 | 同一方法/交付参考；旧名称仍可发现；不含失效路由、旧 writer、凭据、固定自动淘汰规则 |

数据库测试在既有 DEV 真库执行，不以 sqlite/mock 代替；mock 只用于确定性失败注入。Windows 当前 Python 与已存在的 WSL Python 3.10 环境 fresh-process 验证；缺少依赖报告，不自行安装。不声明模拟测试证明全部生产行为。

### 10.2 真实案例

案例 A：一个趋势内回撤或突破承接假设。使用现有日线与已有 PIT/停牌数据，先以 40 只股票、约 300 个交易日做工具闭环样例，覆盖所需暖启动和成熟标签。这是本次功能验证样本，不是全市场 alpha 有效性结论，也不是永久股票数门禁。只输出本研究候选，不重新导出数据。

案例 B：一个已有因子的衰变/冗余诊断。读取已有官方指标和可比历史摘要，区分样本不足、周期变化、数据异常与失效；可形成“继续观察/需求补齐”的真实结论。不更改正式可用位。样本名字实施时从可用数据中选，不把某只股票写成特例。

两个案例均要求实际创建任务、登记尝试/结果/结论，并在新的客户端会话通过 DB 查询恢复；无可用指标时不能伪造诊断成功，换为有真实数据的案例或报告具体未完成项。完整全市场研究、正式新增因子与策略验证作为后续获准任务，不能以这两个小样本替代。

### 10.3 本轮文档验证

仅本文，初版 docs-fast-new，v1.1 为 docs-fast-update，归属 `docs.architecture`。至少两轮实质审核修订；UTF-8、引用/锚点、`git diff --check`；复用 `python scripts/aistock_feature_workflow.py validate --design docs/architecture/factor_research_p1_detailed_design_20260908.md --tier F2`。最终 HEAD 的 CI verdict 通过后按用户授权合入。文档结构通过不是实施或数据库验证通过。

<a id="risks"></a>
## 11. 风险、剩余确认与快速落地约束

数据库实际部署、DEV 目标、既有只读账号能力和精确数据根尚未本轮运行验证；实施前核实，不编造当前状态。新 schema 与 CLI 是本次明确方案，不能在实现时再自由扩成复杂平台。

本设计将共享评价副作用、Research Pipeline 跨模块耦合、RD-Agent/Assistant 语义不匹配和旧 executor 的宽松归一化显式隔离；并非宣称这些既有模块已修复。需要改动时走 §9 owner 需求，不在本窗口补丁旧业务。

普通 Python 子进程并非文件权限沙箱，不承诺任意不可信代码无法写外部路径；P1 限定审查过的研究代码与既有授权环境。发现需要额外隔离设施先提方案，不悄悄升级生产环境或增加平台门禁。

P1 工作按“一次实现、必要失败节点重测、最终小矩阵验收”组织；两轮审核关注实质发现，不反复重跑成功的大计算。不设置固定多轮数据信心门槛、机器资源阈值或必须盈利的结束条件。

<a id="acceptance"></a>
## 12. Design Acceptance Index 与设计验收矩阵

F-101 三阶段快速落地；F-102 独立方法与客户端入口；F-103 复用决策与副作用识别；F-104 两表任务/历史；F-105 幂等与并发恢复；F-106 CLI 发现与只读；F-107 候选执行与纯指标；F-108 结果恢复与历史保护；F-109 精确所有权/需求交接；F-110 DEV/生产/回滚；F-111 两类真实案例与测试；F-112 无复杂平台/新门禁及真实交付状态。

用户批准先详细设计再实施；下表已更新为 P1 实施验收，源码/DEV 验证、CI、生产应用分别报告。历史设计 PR 的结构通过不作为实现验收证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | §1–2，独立 CLI 与研究模块 | artifact: docs/architecture/factor_research_p1_detailed_design_20260908.md#plan | 已验证 P1 范围 | 无 |
| F-102 | docs/analysis/factor_research_methodology.md；两个名称的 repo skills/commands | artifact: docs/operations/factor_research_delivery.md | 正文与入口审核通过 | 用户批准范围：profile 同步交单一 owner，未安装 |
| F-103 | backend/services/factor_research/runner.py、service.py | test: backend/tests/factor_research/fresh_process_smoke.py | 已验证 | 无 |
| F-104 | backend/migrations/factor_research_p1_20260908.sql；repository.py | test: backend/tests/factor_research/test_repository_dev.py | DEV 两表/23 列及注释读回通过 | 用户批准范围：生产应用另授权 |
| F-105 | backend/services/factor_research/repository.py | test: backend/tests/factor_research/test_repository_dev.py | DEV 幂等/并发/回滚测试通过 | 无 |
| F-106 | scripts/factor_research.py；service.py | test: backend/tests/factor_research/test_contracts.py | 七命令已实现；真实案例新进程读回通过 | 无 |
| F-107 | backend/services/factor_research/runner.py | test: backend/tests/factor_research/fresh_process_smoke.py | Windows/WSL 子进程和指标测试通过 | 用户批准范围：仅候选基础评价，相关性增量属于后续 owner 需求 |
| F-108 | backend/services/factor_research/service.py | test: backend/tests/factor_research/test_recovery.py；backend/tests/factor_research/test_repository_dev.py | 文件/数据库登记失败可恢复，已验证不重算 | 无 |
| F-109 | 精确 ownership/module/test plan/nox/runtime catalog；§9 handoff | test: backend/tests/factor_research/test_contracts.py::test_runtime_registration_is_exact_cli_only；artifact: tmp/handoff/factor-research-p1/ci-selection-all.json | 本地测试路由和 runtime 精确登记验证通过 | 无 |
| F-110 | 三个精确 SQL 文件；repository.py | test: backend/tests/factor_research/test_repository_dev.py | DEV 验证通过 | 用户批准范围：生产另授权；有历史保留，不执行删除型回滚 |
| F-111 | backend/tests/factor_research/dev_cases.py | artifact: X:/AIstock_factor_research/p1-validation-20260908/cases-7cfe6914-a26b-4735-9be0-b267e81adff8.json | 两案例入 DEV 并读回通过 | 用户批准范围：仅功能验证；40 股无停牌样本不证明全市场 alpha |
| F-112 | 本文边界、方法正文和全部 changed files | artifact: docs/architecture/factor_research_p1_detailed_design_20260908.md#risks | 已验证未引入复杂平台或新业务门禁；准确记录交付状态 | 无 |

## 13. Production gates 与本次状态

`production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_install=noop`、`client_install=noop`、`runtime_impact=none`、`production_activation=false`、`process_control=false`。本轮仅文档无需重启。未来代码实际 runtime contract 按 catalog 推导，不能沿用本轮 none。

用户已授权本修订审核、提交、PR 与合入，随后开始 P1 代码实施。本文 PR 本身仅文档；后续实现使用新工作树，生产迁移、因子入生产、服务重启与清理不由设计合入推导。主仓同步与工作树清理分别报告，不删除其他窗口文件。

## 14. 审核记录

第一轮：对照实际共享指标/连接代码审核执行和存储契约，发现并修订：同 attempt 不同请求 ID 可重复启动的漏洞；相同请求并发可能先命中 revision 冲突而非唯一冲突；Windows/WSL 跨环境执行边界不明确；计算已完成但进度竞争导致重复计算的风险。补充 attempt 唯一性、applied/replayed 响应、两条并发读回路径、同解释器子进程及 attach 恢复。明确读取前限制样本范围、候选子进程退出后再评价，不加资源门禁。

第二轮：重新逐项对照蓝图及本文各 12 项验收要求和数据/业务边界，发现并修订共享上下文复用条件缺失、暖启动与信号评价范围容易混淆、存量诊断入口容易只看近期 IC 三项问题。明确参数分组复用、成熟标签与范围报告，并补充存量诊断卡及对应测试。

最终依赖复核：进一步跟读 PIT ensure/load_spans/status 及单因子对齐代码，发现旧 snapshot 准备存在 bootstrap 分支，且仅裁因子序列仍会被引擎重对齐到完整价格日期。修订为显式使用已核实纯读的 canonical v2，并细化先算既有标签再同步切片上下文的适配规则和验证。结构校验发现审核文字引用上位编号被当作新增验收项，改为正文引用，不修改工具或豁免校验。最终本文设计范围内无剩余阻断 findings；这些为本窗口顺序实质审核，不宣称独立 agent/其他业务窗口已经审核。

### DESIGN-COMPLIANCE-001 四项逐条结果

| 条款 | 直接审核依据 | 本次结论 |
|---|---|---|
| 禁止未获准简化交付 | §1–2、§5、§10、§12 保留 P1 真实数据库写入/恢复与两类案例，不以文档或小样本冒充上线/alpha 有效性 | 设计符合；代码与 DEV/生产能力未执行 |
| 禁止静默错误 | §3、§5.3、§6 明确未知相关性不填零、请求冲突、不同请求去重、结果登记失败可恢复、时序范围不偷换 | 设计符合；实现必须以 §10 的真实测试证明 |
| 禁止擅改业务逻辑 | §2、§7、§9 禁止越界修改 QE/数仓等模块，不写官方因子状态/指标，不将现有有副作用服务当只读入口 | 设计符合；本次实际 changed files 仅本文 |
| 禁止新增门禁审批 | §2、§6、§8–9、§11 不新增冻结/哈希、资源阈值、固定淘汰线或审批平台；数据库幂等仅实现已批准历史正确性 | 设计符合；未修改任何运行配置或工作流规则 |

F2 结构校验结果为 12 项索引、12 行矩阵、warnings=0；最终文件检查及 commit-bound CI 以本设计 PR 为准。结构验证不代替实施和数据库验收。

### P1 实施检查点（2026-09-08，尚未合入）

v1.1 设计通过 PR #4442 合入，merge commit 为 `72b66f33c3acc03eab3a18184b551cd51430f128`。后续实现位于独立分支 `feature/factor-research-p1-implementation-20260908`；上文“仅文档”状态记录设计 PR，§12 矩阵已更新为实施证据；两者均不代表生产启用。

- 已实现独立方法正文、研究入口、保留旧名称的交付入口及同源 Claude commands；未安装或覆盖活动 profile。
- 已实现两表研究历史、七个 CLI 子命令、候选子进程、既有指标引擎适配、只读因子库上下文及 attach 恢复。不写官方因子状态或指标，不修改 QE/数仓业务。
- 既有 DEV `aistock_dev:5433` 已执行本模块迁移，新增两表、23 列及注释；七个数据库测试及一个真实 DEV 失败恢复测试通过。生产仅只读查询，未执行 DDL/DML。空表回滚脚本尚未实际执行，不删除已保存研究历史来制造回滚证据。
- Windows 定向测试 19 passed，内置 fresh-process 测试 2 passed；WSL Python 3.10 fresh-process 测试 2 passed。WSL 缺少 pytest，未安装，也不宣称 WSL pytest 已执行。
- 真实案例 A：DEV task `87f6f91c-e17e-4148-ac39-b65c83c7705b`，40 股票候选计算及结果记录完成。DEV 停牌源为空，样本通过生产只读查询确认窗口内无停牌；因此该案例不证明停牌覆盖或全市场有效性。
- 真实案例 B：DEV task `6f156fcf-d015-4777-806d-f46b41817a92`，读取既有因子 `m_downside_semivariance_shift_10d_60d` 的实际指标并记录诊断上下文；未修改其可用状态，不将诊断记录当作淘汰结论。
- 产物仅在 `X:/AIstock_factor_research/p1-validation-20260908`；未重导出或修改现有数据集。模块 registry 测试 8 passed，L0 blocking=0，py_compile 和 diff 检查通过。

两轮实现审核修复了未知请求字段被忽略、空候选结果可 attach、读取已不存在指标列等真实问题，并验证因果截断、跨股票隔离、并发幂等和失败恢复。四项设计复核：未以功能样本代替正式研究交付；错误和恢复状态明确；没有跨模块业务修改；没有新增资源、冻结或自动淘汰门禁。但完整实施验收仍未结束，不能以这些局部结果声称合入就绪。

此前将 `unexecuted_test_blocked` 归因于分类器的判断不正确。经流水线窗口复核，现行规范要求普通计划实际收集新增测试，DEV 计划另外执行；根因是本 P1 nox/测试配置遗漏。没有登记流水线 BUG，也没有修改分类器。

### P1 测试计划修复与复核（2026-09-08）

第一轮：DEV 文件使用模块级 `AISTOCK_DEV_DB_E2E=1` 授权保护，普通计划明确覆盖为 `0` 并清空 DEV env-file，只收集并跳过数据库测试；专用计划仍 `runner_enabled=false`，调用时要求显式 DEV env-file 并启用授权。增加测试证明继承的授权值不会穿透普通计划、未授权执行不会连接数据库、专用计划缺 env-file 不会伪造通过。全量 26 个 changed files 的 classifier 为 `targeted_ci_required/workflow_gate=passed`，未覆盖测试和未映射代码均为 0。

第二轮：修复测试重复加载 noxfile 导致的注册告警；发现并通过 RED→GREEN 修复“计算已完成但 attach.json 写入失败”遗漏。现在返回 `computed_not_recorded`、真实 result_path 及 attachment 请求，文件没有落成时 attach_path 为 null，不要求重新计算。

最终本地小矩阵：普通计划 23 passed、8 skipped（DEV 测试被收集但不连接数据库）；DEV 专用计划 8 passed；Windows/WSL fresh-process 各 2 passed；nox 环境合同 17 passed；registry 8 passed；Ruff、py_compile 通过，L0 blocking=0。DEV 新进程确认案例 A revision=4/completed，案例 B revision=2/active，23 列均有注释。未重跑真实案例计算，未改生产数据库、数据集或进程。

四项 DESIGN-COMPLIANCE-001：完整 P1 能力有真实数据库与候选证据，但不将未完成 runtime/PR 状态声称完整交付；错误与恢复不静默；未修改业务模块或评价语义；新增的测试授权隔离实现现行 CI 数据库安全规范，不是新增业务门禁。以上均为本窗口顺序实质审核，不冒称其他 agent 已审核。

当时剩余精确项为新 CLI 缺少 runtime 登记。用户随后明确授权加入精确文件范围，完成情况见下一节；旧 unknown 不再代表当前分类结果。

### 授权范围补充后的最终本地复核（2026-09-08）

仅在 `docs/standards/aistock_runtime_targets_v1.yaml` 的 `non_runtime_source_paths` 增加 `scripts/factor_research.py` 一行。对比原 catalog 的完整 YAML 结构，除该条目外完全一致；未更改 classifier、target、probe、通配规则或后端登记。

第一轮补充回归先 RED（没有登记），增加精确条目后 GREEN。测试同时证明新 CLI 为 none、五个 backend 文件仍为 backend-main、未登记相似脚本仍为 unknown。第二轮重新计算全部 27 个实际 changed files：`runtime_impact=backend`、`target_ids=[backend-main]`，observed impacts 包含 backend/client/database/none；原 unknown 已消除。整体不是 none，后端重启 owner 仍为 user。源码分类不等于运行身份验证；本任务为 feature，空 BUG record 不能作为其正式重启 receipt。

本轮验证：普通计划 24 passed、8 skipped；Windows fresh-process 2 passed；runtime catalog 精确登记/拒绝别名等定向测试 4 passed；registry 8 passed；Ruff、diff 检查通过；L0 blocking=0。全部 changed files 的测试路由为 passed，未覆盖测试和未映射代码均为 0。前轮 DEV 8 passed 和 WSL 2 passed 的代码行为未被本次 catalog 登记改变，本轮未重复数据库写入或真实案例计算。

DESIGN-COMPLIANCE-001 四项补充审核：仅关闭已获准的精确登记缺口，不把源码/样本冒充上线；没有吞错或默认分类；完整 catalog 对照证明未调整其他业务/后端语义；未新增业务门禁或扩大授权。F-109 的本地缺口已关闭。提交/PR、最终 HEAD 的完整 CI、客户端同步、生产两表迁移和用户重启仍为独立待执行事项，不使用设计 PR 的 CI 代替实现 PR 证据。

用户随后授权本 P1 实现提交、创建 PR 与合入。交付遵循已批准的设计范围，最终 source HEAD/merge SHA/CI 由实现 PR 提供；上文“尚未提交/尚未创建 PR”均为对应检查点的历史状态。此次源码授权不包含生产两表 migration/DML、profile 安装、清理或进程操作。

### v1.1 复用与 skill 修订审核

第一轮：对照已核实旧 skill 与现行路由/ManualFactorService，补充保留的技术规范、实际保存/官方评价入口、历史经验的条件化处理和双入口单方法职责，消除“新建 skill 等于重做因子库”的歧义。

第二轮：复核客户端归属与副作用，补充“现有同步工具未支持业务入口则交 owner”以防私造安装器；明确保存成功不代表分类/指标全部成功、同名保存会覆盖代码，研究流程不调用正式写入口；修正 v1.1 文档更新与随后代码实施的授权/状态表述。按四项 DESIGN-COMPLIANCE-001 复核：未缩减数据库闭环交付，未保留静默成功或固定淘汰规则，未跨模块改业务，未增加门禁。本文设计范围无剩余阻断 finding；仅为本窗口顺序审核，不冒称独立复核。
