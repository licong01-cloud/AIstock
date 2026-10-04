# AIstock 全仓代码审核与整改报告

日期：2026-10-05。版本：v1.1，完成两轮反证与报告修订。

源码快照：`876316279cfb3118664192ea0abc9333b4980c68`。所有源码行号、代码规模和静态结论均对应该快照，不代表其他窗口未合入的工作。

本报告只评估现状并提出整改，不修改开发标准，不创建新审批或门禁，不表示业务缺陷已经修复。本次授权交付为报告提交、CI、合入及本报告工作树清理。

## 1. 总体结论

目前不能把整个系统评价为“代码质量已经通过全面验收”。语法与基础名称检查相对稳定，但仍有成功状态校验不足、读取接口写入副作用、行情逐标的状态不完整、夜间测试合同漂移，以及测试债务和巨型模块维护成本等问题。

本次整理 **12 项主要整改项：4 项已确认 P1，1 项条件性 P1 风险，7 项 P2**。未确认新的 P0；这不是不存在 P0 的保证，也不能把规则扫描标记的 P0 候选全部当作真实严重缺陷。

优先顺序：先修正远端同步成功合同、读取写入边界及行情来源准确性，同时恢复夜间反馈可信度；再精简重复测试、降低复杂度和消除高价值 lint 问题。不要再用全仓重复测试、新平台或更严格审批来代替具体修复。

已有 PIT、候选身份、企业行为、持久化事务等防线仍有价值。整改应保留这些防线，删除重复劳动，而不是把缺数补零、失败降级成成功或放宽验收。

## 2. 审核范围、方法与边界

### 2.1 覆盖层次

| 层次 | 本次实际执行 | 不能推出的结论 |
| --- | --- | --- |
| 第一方 tracked 程序清单 | Python、TS/TSX、JS/JSX、Go、PowerShell、BAT、SQL 的清单与规模扫描 | 每一行均被人工逐行审阅 |
| 非测试 Python | AST 解析、危险异常/超时模式与函数跨度检查 | 类型正确、业务数值正确或运行依赖齐全 |
| Python Ruff | backend、scripts、tools、debug_tools、factors_sota、aistock_models 及指定根入口的既有配置检查 | 所有语言都编译成功 |
| 全仓 guardrail | 现有 baseline 模式，保留候选并人工反证 | 每个正则命中都是真 BUG；blocking=0 表示全仓合格 |
| 模块测试债务 | 现有 ownership-aware 有效代码统计 | 超比例就说明每个测试无价值 |
| 关键业务链 | API→service→持久化、远端同步确认、行情来源、离线 HMM、前端错误呈现等定点审阅 | 实际生产 profile、数据、收益与运行态已验收 |
| 现成夜间结果 | GitHub 最近五次 Nightly run 及最近两次 job 状态、最新失败日志摘要 | 重新执行全业务矩阵或修改业务任务 |

第一方清单排除已标识的 backup、缓存、文档和 pinned vendored 源码；不展开第三方库。该清单未统计 shell 脚本、notebook 和 YAML 为程序文件，CI YAML 单独定点审阅；权威预算脚本的统计后缀包含 `.sh`，两种清单不能混用。

未进行生产 API 探测、数据库读写、浏览器业务验收、Go/前端完整编译、依赖安装、训练、实验或服务控制。没有读取全量历史 BUG JSON、遍历所有 worktree，亦没有扫描秘密凭据内容。

### 2.2 依据

唯一规范依据是 [开发标准 v1.5](../standards/aistock_development_standard_v1.5_20260523.md) 与 [路由索引](../standards/README.md)。主要对照 fail-closed、错误不得静默、读写权限边界、运行身份与业务结果分离、验证适用性及测试预算原则。报告不是新的规范副本。

RTK 用于支持的高输出命令；结构化 GitHub JSON 与 AST/统计输出保留原格式，避免过滤丢失证据。没有将 RTK 使用率、扫描总数或全仓 lint 引入新的发布阻断。

## 3. 基线数据

### 3.1 静态检查

| 指标 | 结果 | 解读 |
| --- | ---: | --- |
| 第一方 tracked 程序文件 | 4,281 | 其中非测试文件 2,805，按路径启发式分类 |
| 程序物理行数 | 生产 1,389,195；测试 486,358 | 包含注释/空行；不能用来代替有效代码预算 |
| 非测试 Python AST 语法错误 | 0 | 未执行源码；不能等同完整编译或类型检查 |
| Ruff 诊断 | 830 | 使用既有配置，包含测试；不是 830 个业务 BUG |
| Ruff F821 未定义名称 | 0 | 只覆盖上述 Ruff 输入集 |
| 单语句广义异常 handler 候选 | 445 | 需区分显式错误结果、可选能力与真实静默失败 |
| 直接 requests/subprocess 无 timeout 候选 | 205 | 包括本地 Git 调用；调用者/包装器可能另有边界 |
| Guardrail findings | 4,268，扫描文件 6,467 | 正则候选，不能当作确认缺陷数 |

Ruff 分布：F401 189、F811 5、F541 182、F841 39、E701 25、E402 311、E401 22、E702 23、E741 29、E722 4、E731 1。相加为 830。

Guardrail 原始严重度标签为 P0 277、P1 139、P2 3,852；其中有 fail-closed 文案、受控 Popen、已传 timeout 的调用等误报。本次 baseline 使用 `--fail-on-severity NONE`，其 blocking=0 仅表示没有让历史候选阻断此次扫描。

### 3.2 权威测试预算

`scripts/aistock_validation_budget.py` 按 tracked 文件、真实归属、有效代码行统计：生产 1,259,312；测试 426,780；测试/生产为 **33.89%**。75 个模块桶中，51 个超出 30%，其中包含 12 个无生产分母的 test-only 桶；即另有 39 个有生产分母的超预算桶。

| 模块 | 生产有效行 | 测试有效行 | 比例 |
| --- | ---: | ---: | ---: |
| qe.active_dataset_universe | 2,149 | 4,206 | 195.72% |
| qe.archive_handlers | 2,122 | 3,308 | 155.89% |
| hmm.evolution | 11,608 | 6,775 | 58.36% |
| qe.long_trend_evaluation | 15,361 | 6,985 | 45.47% |
| strategy_package | 28,272 | 12,738 | 45.06% |
| hmm.risk | 30,001 | 13,474 | 44.91% |
| qe.sector_risk_overlay | 11,313 | 4,017 | 35.51% |
| qlib_data | 108,807 | 37,976 | 34.90% |
| advisory.model_first | 106,290 | 34,632 | 32.58% |
| qe.core | 74,551 | 23,706 | 31.80% |
| validation.workflow_automation | 44,867 | 13,533 | 30.16% |
| advisory.historical_range | 69,922 | 20,760 | 29.69% |
| factor_library.research | 4,051 | 1,163 | 28.71% |
| position_timing | 45,081 | 11,266 | 24.99% |

这是当前快照结果，不应沿用之前窗口“已降到 30% 内”的旧结论。统计范围、归属及后续增长都可能改变比例。不能迁移归属、加入无效生产代码、排除测试 helper 或提高上限来伪造达标。

## 4. 主要发现与整改要求

### R01 — P1：远端同步缺失成功字段仍被接受，可能形成假完成

证据等级：**已确认，隔离复现**。

位置：[factor_cache_remote_sync_service.py](../../backend/services/quantevolver/factor_cache_remote_sync_service.py)，202、230、270、680 行附近。

上传与 metadata 更新仅拒绝 `payload.get("ok") is False`；空字典、缺失 ok 或非布尔成功值没有被拒绝。`upload_sync_bundle` 再返回 `ok=True`，上层无异常即写 `status=completed`。请求有 timeout 和 HTTP 状态检查，缺陷不是“忘记设置 timeout”，而是响应语义验证不足。

实际抽取原方法、替换 HTTP 与上传为隔离替身：metadata 响应 `{}`、上传响应 `{}` 时，返回 `ok=true, uploaded_count=1, uploaded=[{}], meta={}`。没有网络、文件上传、数据库或 backend import。

影响：错误代理响应、服务端合同漂移或损坏响应可能被当作同步完成。尚未验证生产节点是否发生过这种响应，不能据此宣称已有数据损坏。

所有者：QE 因子缓存/节点同步。修复应要求明确布尔成功、与请求绑定的必要身份/大小或 hash 合同；缺失、错误、部分成功必须明确失败。先复用已有返回合同，再决定是否需要轻量只读回读，避免无条件重复传输或全量重算。

最小验收：正常响应、空字典、缺失 ok、非布尔 ok、身份不符、上传失败、metadata 失败；只有全部满足合同才允许完成。不得修改业务数据来满足测试。

### R02 — P1：模型状态 GET 存在持久化写入副作用

证据等级：**已确认，原方法隔离复现**。

位置：[strategy_packages.py](../../backend/routers/strategy_packages.py) 847 行；[strategy_package/service.py](../../backend/services/strategy_package/service.py) 638–644 行；[model_state.py](../../backend/services/strategy_package/model_state.py) 98 行起。

`GET /{package_id}/model-state` 调用 `get_model_state`，每次读取都对计算结果执行 `upsert_model_state`。替身 repository 复现一次读取对应一次 upsert。任意 `as_of_date` 参与全局持久状态计算；历史日期查询可能覆盖当前陈旧度，这是由代码可推导的进一步风险，未操作数据库验证。

影响：只读验收、前端加载或预览可能写库；历史/当前查询的顺序可能影响持久状态，读写权限与审计意图不清。当前前端确实调用该 GET，不能仅按命名当作只读探针。

所有者：StrategyPackage/模型状态。整改为读取计算只读 view，持久刷新放显式写入口并遵守既有权限；若现有设计确实要求惰性写入，应明确迁移兼容方案，不能让只读验证继续调用带写入副作用的接口。

最小验收：GET/预览不 upsert；历史查询不覆盖当前状态；显式刷新正确写入；保留既有 retraining/failed/stale 判定。真实事务验证只能使用已有 DEV，生产不写。

### R03 — P1：部分实时行情缺失被错误标成实时来源

证据等级：**已确认，隔离复现**。

位置：[rdagent_selection_service.py](../../backend/services/rdagent_selection_service.py) 349–411 行。

代码使用批次级 `quote_source`。只要快照非空就整体设为 miniqmt/tdx；缺失标的被 continue，历史行情仅在整批 fallback 时填入。输出各标的仍共用该实时来源。

复现输入 A/B 两个标的、快照仅有 A、历史 B=20：结果 A=11、B=null，批次来源 miniqmt。随后输出 B 同样带 miniqmt 标签，无法区分“无实时行情”和“真实实时数据”。

影响：展示与消费者错误理解来源；已有历史值也不进入明确的逐标的处理。这里不能武断要求全部自动 fallback，更不能用补零伪造行情。

所有者：RD-Agent 选股展示/行情接入。整改为逐标的来源、时间和 unavailable reason；按既有展示合同显式选择历史参考或缺失状态，交易消费者仍 fail-closed。保留异常原因，不吞掉数据源失败。

最小验收：完整快照、部分缺失、全部缺失、空值/NaN、数据源异常；逐标的价格与来源一致，不把缺失当实时成功。

### R04 — 条件性 P1：旧 SectorHMM 缺特征补零与滚动暖启动语义

证据等级：**源码条件已确认；当前生产启用状态未验证**。

位置：[sector_hmm.py](../../backend/quant_models/hmm/sector_hmm.py) 343–361、789–807、905 行；[topk_dropout_rc.py](../../backend/rebalance_strategies/topk_dropout_rc.py) 335–353 行。

旧路径在缺少涨停统计时设置 `lu_ratio=0.0`，实时推断还固定为 0；20 日均值允许不足 20 日的窗口。缺失统计与真实零值被混合，训练和实时输入可能不同。warm-up 是否允许短窗口必须由模型合同确认，不能仅凭函数名定为 BUG。

该路径由 `enable_sector_hmm` 显式开关触发，不等于当前 hmm_risk 的 V16/G2C 主链。未查生产开关，不能宣称现行 HMM 已错误交易。

所有者：HMM 与使用旧开关的 QE 策略。先确认仍有合法消费者；退役则精确移除消费者及旧测试，仍使用则定义缺失/暖启动合同并拒绝不兼容模型。不能把零填充包装成性能优化，也不要重跑训练来掩盖输入问题。

最小验收：真实零值与缺失区别、训练/推断特征一致、warm-up 边界和旧开关兼容；不启动实验。

### R05 — P2：Go 辅助服务器初始化错误被返回为成功

证据等级：**错误分支已确认；当前主服务使用情况未证实**。

位置：[extend/codes-server.go](../../tdx-api-main/extend/codes-server.go) 13–17 行；已找到 example/CodesHTTP 调用，未找到主服务使用该入口的证据。

`tdx.DialCodes` 返回错误时，`ListenCodesHTTP` 返回 nil，服务未启动却向调用者报告无错误。整改应返回原错误并由调用者记录失败；若只是未使用示例，应标明边界或退役，不升级为生产紧急事故。

最小验收：初始化失败向上返回非 nil；成功路径不变。没有为本报告运行 Go 构建或下载依赖。

### R06 — P2：Go 外部 HTTP 路径缺少整体截止时间/响应体上限

证据等级：**局部调用配置已确认，运行影响依赖调用条件**。

位置：[client_bj_code.go](../../tdx-api-main/client_bj_code.go) 38–59 行；[extend/codes-server.go](../../tdx-api-main/extend/codes-server.go) 43–53 行；extend/spider-ths.go 122 行。

使用 `http.DefaultClient`，可见请求没有 context deadline；CodesHTTP 对响应体执行无大小上限的 `io.ReadAll`。本仓检索未见统一设置 DefaultClient Timeout，不能因此排除外部启动器可能修改全局 client。[Go 官方 Client 文档](https://pkg.go.dev/net/http#Client) 说明零 Timeout 没有该整体超时边界；这不等于 TCP 所有阶段都没有超时。

所有者：TDX 数据接入。使用已有配置的有限超时/context cancellation，以及适合代码列表的响应体上限；错误明确返回。不要为此修改全局网络代理或无限提高超时。

最小验收：慢响应、未结束响应体、超大响应体、正常列表；使用本地替身，不访问业务数据库。

### R07 — P2：前端将请求失败变成空列表/未加载状态

证据等级：**源码已确认，UI 实际行为未浏览器复验**。

位置：[packages/page.tsx](../../frontend/src/app/paper-v2/packages/page.tsx) 300–306、687 行附近。

policies/events 的失败被 `.catch(() => [])` 吞掉，modelState/deleteDependencies 失败转 null。外层 catch 无法收到这些请求失败，UI 易混淆“服务错误”与“确实没有数据/尚未获取”。

所有者：StrategyPackage 前端。为独立区域保留 error/loading/empty 状态，局部失败不必阻止其他区域展示，但不能显示成正常空数据。不得用加重全页门禁替代错误呈现。

最小验收：区域成功、合法空结果、HTTP 错误、网络失败；保留区域错误及重试入口，操作按钮不能依赖未知状态冒进。

### R08 — P1：Nightly 长期红灯包含可修复的合同漂移，削弱反馈价值

证据等级：**GitHub 当前运行记录及失败日志已确认**。

最近五次 run：37157241682（失败）、37074934461（失败）、36938718209（成功）、36788300918（失败）、36642320288（失败）。这是五次样本，不是长期失败率估计；最新运行基于 `d6cc7ee7…`，与本报告快照不同。

最新两次 runner preflight、DR snapshot、DR validate、汇总/自动 BUG 步骤通过，Nightly L3 失败，code intelligence/paper live 被跳过；这些跳过不能当作对应验证通过，也不能只凭 skipped 判定全部由同一依赖条件造成。不能把汇总成功当作夜间业务通过，也不能把日志中脚本文本的“conda not on PATH”误当实际环境错误。

最新失败摘要：

- validation_workflow_automation：`test_ci_failure_issue_summary.py::test_nightly_workflow_manual_dispatch_can_skip_dr_and_live` 仍要求旧 DR 条件字串，当前工作流已让 Nightly L3 与 DR 调度独立。应更新行为契约，而非回滚工作流以满足旧字串。
- `test_aistock_validation_budget.py::test_workflow_automation_has_an_honest_production_denominator`：实际比例 0.3016248，超过 0.3；这是当前测试债务，不应提高上限或伪造分母。
- hmm_risk_ui：alert 定位匹配两个元素导致 strict-mode 失败，需按业务语义缩小定位，不删验收断言。
- validation_catalog_integrity 会话失败；本次不从会话名推断所有子因果，需在归属任务中查看精确断言。

所有者：流水线、Validation Center 与 HMM UI 各自边界。优先复用已有 [#5306](https://github.com/licong01-cloud/AIstock/issues/5306)、[#5424](https://github.com/licong01-cloud/AIstock/issues/5424) 核对范围，若不覆盖再登记；不要重复创建同一失败 BUG。

最小验收：定向失败测试恢复；下一次自然 Nightly 对应会话通过，汇总准确区分 infrastructure/test/business。不要为了核验触发 DR、paper live 或额外数据库任务。

### R09 — P2：测试预算仍未完成，且归属桶影响判断

证据等级：**现有权威脚本当前统计已确认**。

详见第 3.2 节。workflow_automation 只超约 73 有效行，可通过去重复小修收敛；QE 小生产模块、HMM evolution 和 qlib 的超额较大，需要业务所有者判断保留的真实合同。

所有者：各模块及验证基础设施。按“退役/重复/结构实现细节/现行高风险合同”分类，再参数化或删除前两类。保留 PIT、单位、身份、缺失 fail-closed、终态持久化、恢复与并发的直接回归。

test-only 桶先核对真实代码归属；不能为降低比例将测试悄悄挪给别的模块。MiniQMT 暂停开发、LocalSIM 优先级较低，不能让其历史债务抢占 QE/HMM/Advisory/因子/择时/数据任务，也不能无边界整仓删测试。

最小验收：受影响模块预算与精确测试计划；保留合同列表及删除理由；不机械重跑低相关全矩阵。30% 是当前项目要求，不是普适质量定律。

### R10 — P2：巨型文件/函数放大维护、上下文与回归成本

证据等级：**物理规模已确认，不是圈复杂度或缺陷率推断**。

| 文件或函数 | 当前规模 |
| --- | ---: |
| scripts/aistock_issue_workflow.py | 23,607 行 |
| simulation_runtime/scheduler.py | 16,318 行 |
| research_assistant/service.py | 10,883 行 |
| routers/quantevolver.py | 9,216 行 |
| quantevolver/qe_evolution_service.py | 8,631 行 |
| quantevolver/config_composer.py | 7,537 行 |
| config_composer._compose_conf_yaml | 单函数跨度 1,290 行 |
| advisory selection_liability_gate pipeline run 函数 | 单函数跨度 809 行 |
| position_timing.pattern_research.run_pattern_request | 单函数跨度 802 行 |

所有者：对应模块。优先在后续真实修改时按稳定行为边界提取纯函数/阶段，不同时大重构全部模块；用短 context pack、函数定位和 direct-neighbor 测试减少反复全文件读取。

最小验收：外部输入输出、持久化与错误传播不变；只补提取边界的必要回归。不得通过新增框架、设计审批或全仓门禁“治理”复杂度。

### R11 — P2：基础 lint 债务多，修复需避免机械改动导入语义

证据等级：**830 条当前诊断已确认；具体业务影响未逐项判定**。

E402 311 中部分可能是有意的环境初始化/延迟导入；F401/F541 多属清理债务；F811 中四项是 fixture 名称遮蔽导入，一项重复 os 导入，不能称作五个重复业务函数。E722 裸 except 值得优先人工核对。

所有者：对应源码所有者。先消除会吞失败的裸 except 与危险 unused state，再在修改邻域清理简单问题。仅对确需初始化的导入保留局部解释，避免全仓 autofix 改变导入顺序/注册副作用。

最小验收：changed-files lint 和直接消费者导入；不要把全仓 830 条历史 lint 突然设成阻挡所有窗口的新门禁。

### R12 — P2：规则扫描候选质量不足，容易制造重复调查成本

证据等级：**多个误报已沿调用链反证**。

Guardrail 对 factor-cache requests 调用误报超时缺失，实际传入 timeout；部分 Popen 已有 communicate(timeout) 与所有者级清理。AST 候选中的 27 个 async time 调用全为 time.time/monotonic，不是 sleep 或阻塞 I/O；已从本报告缺陷统计剔除。

所有者：Validation/guardrail。复用 AST/现有包装契约识别显式 timeout 和受控进程；基线继续用于趋势与人工分流，不新增全仓发布门禁。先改善精确率，再讨论扩展规则。

最小验收：已受控调用不报错、真正无界请求仍检出；规则命中数与已确认问题数分别呈现。

## 5. 模块定点评估与尚未验证项

| 领域 | 本次重点 | 评价与后续所有者 |
| --- | --- | --- |
| QE/RD-Agent | 远端缓存 ack、行情部分缺失、配置组合复杂度 | R01/R03 优先；不参与实验和 profile 激活 |
| HMM | 旧 sector_hmm 输入边界、当前中立 contracts、现成 UI 失败 | R04 先确认消费者；不能将旧链问题归到 V16/G2C |
| Advisory | 现成问题/大型 pipeline 与输入校验相关检索 | 有身份/选择防线；新报 #5446 交所属窗口，未复现其全部模型流程 |
| 多 alpha | durable_repository 事务入口及身份/唯一性错误分支 | 发现明确事务与错误合同；未执行并发/恢复，不给全面通过结论 |
| 因子研发 | 当前预算、现成 aftercare 状态与缓存发布顺序 | 研究预算 28.71%；旧 Issue OPEN 不代表源码尚未修复 |
| 择时策略 | 企业行为 snapshot identity/范围/数值 fail-closed | 当前预算 24.99%；PIT/企业行为保护不能为删测试而削弱 |
| 数据准备 | candidate_validator 历史 lineage/reuse/lookup 合同与现成月更问题 | 明确缺失/身份拒绝逻辑值得保留；不做生产月更或 DEV 写库 |
| StrategyPackage/前端 | GET 写状态、区域错误降级 | R02/R07；浏览器验收交独立隔离环境 |
| TDX Go | 错误向上传递、HTTP 资源边界 | R05/R06；辅助示例与主服务要区分 |
| LocalSIM/MiniQMT | 大文件/测试预算与现成运行态遗留 | 低优先维护；未逐行审核 scheduler 或执行行情/订单流程 |
| 流水线/验证 | Nightly、测试预算、规则误报、超大工作流实现 | R08/R09/R10/R12；不得牵连业务窗口或提高不相关测试量 |

已读到的正确防线只是定点正面证据，不意味着这些模块没有其他缺陷。Python 全仓扫描比 TS/Go/SQL 的本次人工深度更高；未做 SQL 注入全路径证明、前端类型全构建、交易状态空间枚举和生产数据校验。后续应按风险与变更范围继续审阅，而不是以本报告替代这些验证。

## 6. 已登记问题与交接原则

本次只读获取 GitHub 当前 Issue/Nightly 摘要，没有全量 JSON/历史 worktree 扫描。已有 BUG 优先复用，源码合入、运行验收、Issue 关闭和清理分别判断。

- Nightly 相关 #5306、HMM UI #5424：核对当前失败是否仍在原登记范围，归属窗口修复测试合同。
- Advisory 新报 #5446：当前有独立业务窗口处理线索，不在本报告分支修改。
- QE 月更/候选遗留如 #5439、#5378、#5315、#4988：以所属窗口的候选、身份与真实验收证据为准，不根据标题推断全部仍是源码缺陷。
- 因子相关 #5197 的旧“计算前清空”问题：当前源码先校验再发布；Issue OPEN 不能证明旧实现仍存在。应交因子窗口判断正式验收/关闭，不重复修复历史代码。
- LocalSIM #4379 等运行终态遗留：不绕过终态验证，不因为清理目标就操作 scheduler 或生产数据库。

R01–R07 由所属窗口核对已有登记后按独立根因登记/修复。R08–R12 按工作流与各模块归属处理；质量债务不必拆成每条 lint 一个 BUG。报告发现编号 Rxx 不是正式 BUG 编号。

## 7. 整改顺序与效率要求

1. **第一批：P1 成功语义与读写边界。** R01、R02、R03 分模块精确修复；R08 与既有 Nightly BUG 联动。R04 先确认旧开关使用，再决定修复或退役。来源未知时不声称生产已受损。
2. **第二批：反馈可信度与小额预算修复。** 消除 workflow 30.16% 超额和旧字串断言，恢复自然夜间结果；为局部 UI 失败显示真实原因。不得新建 CI 数据库、安装依赖或盲目重跑全仓矩阵。
3. **第三批：重点模块测试债务。** QE/HMM/Advisory/数据准备优先，各业务所有者给出删除依据与保留合同；因子/择时已经较低，不再机械删减。共享 fixtures 去重复可以做，但不得藏到生产分母。
4. **持续邻域改进：** 修复 Go 有界请求、危险异常与源代码重复；按实际开发触点拆解巨型函数，改善 scanner 精确率。MiniQMT 暂停、LocalSIM 低优先，不抢主线资源。

每项使用“复现→精确修复→直接消费者/合同验证→复审→当前 HEAD CI→合入→独立运行/清理状态”闭环；仅更改文档时走 docs-fast，不跑 DEV/业务矩阵。跨模块问题协调所有者，但不为协调另建永久审批层。

无法凭静态扫描给出可信的“效率必然提升 50%”或整改工时。可衡量指标为：业务开发实际时间、相关测试执行时间、CI 运行/排队/网络分段、重复验证次数与 Nightly 相同根因复发次数。使用现有收据和运行时间，不新增手工填表门禁。

## 8. 本报告的复核、修订与交付证据

### 第一轮：反证与事实修订

剔除或降级：将全部 guardrail P0 当真实 P0、将 time.time 当阻塞 I/O、已带 timeout 的 factor-cache 调用、受控 Popen、fixture 遮蔽当业务函数重复、Issue OPEN 当源码未修复、脚本回显文案当 runner 环境失败。旧 HMM 暖启动语义与生产启用状态保留为待确认。

### 第二轮：最小复现与报告一致性

在本任务 ignored scratch 中通过 AST 抽取原方法、隔离替身执行三项证明，未导入 backend 或接触真实网络/数据库：

```text
R01: empty_remote_ack_accepted -> ok=true, uploaded_count=1, uploaded=[{}], meta={}
R02: model_state_read_upsert_calls -> 1
R03: partial_quote -> source=miniqmt, prices={A:11.0, B:null}, historical_B=20.0
```

这证明特定代码路径的行为，不证明真实服务器已经返回畸形响应、生产数据库被本次修改或策略收益有误。关键输出在本文持久化，成功原始扫描日志与辅助脚本不是长期权威证据。

执行的只读命令类别：tracked inventory/AST + Ruff（无缓存）、现有 validation budget、现有 guardrail baseline、GitHub Nightly/jobs/log 摘要、原方法隔离替身。报告交付检查为 UTF-8/链接/路径/数字与 finding 分级一致性、`git diff --check`；不额外要求业务测试或 DEV。

第二轮机械复核：12 个 finding 编号连续，4 个已确认 P1/1 个条件性 P1/7 个 P2 与摘要相符；13 个本地链接存在且位于仓库内；模块表的每个生产/测试行数和比例逐项匹配冻结预算 JSON；Ruff 分类相加等于 830；唯一 tracked 改动为本报告。另回读当前源码确认 Nightly 旧断言仍存在、模型状态空值 UI 文案及 Go 请求无 deadline，不仅依靠历史日志推断现状。

DESIGN-COMPLIANCE-001：本任务完整交付的是审核与整改报告而非源码修复；未将子集扫描称为全程序形式验证；未隐藏失败/伪造成功；未迁移业务逻辑；未增加未经授权的审批或门禁。

生产 DDL/DML、依赖安装、进程控制、训练/实验、运行激活：全部未执行。仅本报告路径进入提交；临时计划/扫描脚本在 ignored `tmp/handoff/repository-review`，持久证据随报告/PR 保存后交官方 cleanup 精确清理。

版本记录：v1.0 固定源码快照、12 项分级发现与模块交接；v1.1 第二轮核对数字/链接/原方法复现与当前断言，修正 skipped 因果推断并补齐交付证据。后续整改另走所属模块源码流程，不能把本报告合入当作所有缺陷已解决。
