# AIstock 项目开发规范 v1.0

> 版本：1.0
> 更新日期：2026-05-04
> 状态：人类可读规范源，待用户审阅后作为 AIstock 项目开发规范基线
> 当前文件：`docs/standards/aistock_development_standard_v1.0_20260504.md`
> 同步机器版本：`docs/standards/aistock_development_standard_v1.0_20260504.yaml`
> 历史归档目录：`docs/standards/archive/`

## 1. 文档定位

本文是 AIstock 的唯一项目级开发规范源。AIstock 不再拆分“Python 开发规范”和“量化/交易开发规范”，因为本项目的 Python、数据、前端、实验、回测、Paper Trading、未来实盘前置验证高度耦合，普通工程规范必须和量化业务红线一起执行。

规范文件体系如下：

| 文件 | 角色 | 权威性 | 说明 |
|---|---|---:|---|
| `docs/standards/aistock_development_standard_v1.0_20260504.md` | 人类可读规范源 | 最高 | 定义必须遵守的项目开发、量化、测试和提交流程。 |
| `docs/standards/aistock_development_standard_v1.0_20260504.yaml` | 机器可读同步版本 | 派生 | 由本文抽取规则，供 guardrail scanner、nox、Validation Center、Codex/Claude Code 使用。 |
| `docs/architecture/aistock_development_standards_and_guardrails_20260504.md` | 落地设计 | 低于规范源 | 说明如何把规范接入 baseline、changed-files gate、agent 修复和验证中心。 |
| `docs/codex_project_memory.md` | Codex 项目记忆 | 运行约束 | 记录当前规范位置、执行要求和阶段进展。 |

如出现冲突，以本文为准；机器 YAML 和架构设计必须在同一提交中向本文对齐。

## 2. 版本、归档和变更规则

1. `docs/standards/` 根目录只保留当前选定生效的人类规范 MD 和对应机器 YAML。
2. 历史版本必须移动到 `docs/standards/archive/`，不得在根目录堆叠多个旧版规范。
3. 文件名必须包含版本号和更新日期：`aistock_development_standard_vX.Y_YYYYMMDD.md`。
4. 每次修改规范必须创建新版本文件，不直接覆盖历史版本；例如从 `v1.0_20260504` 升级为 `v1.1_20260510`。
5. 机器 YAML 必须与人类规范同版本、同日期、同提交更新。
6. 历史版本只允许补充“已归档说明”或修正明显错别字，不允许改变历史规范含义。
7. 任何新增 P0/P1 红线必须同步增加机器规则，或在 YAML 中明确标注 `manual_review_only` 及原因。
8. 规范变更本身需要通过 L0 文档读取、YAML 解析、规则引用一致性和 guardrail smoke 验证。

## 3. 适用范围

本规范适用于 AIstock 全仓：

- FastAPI backend：`backend/routers`、`backend/services`、`backend/data_service`、`backend/db`、`backend/infra`。
- Next.js frontend：`frontend/src`、`frontend/tests`。
- 数据与导出：Tushare、本地 DB、Qlib H5/Bin、因子库、模型库、HMM、QMT/TDX 接入。
- QE / RD-Agent / 自动演进 / 自定义演进 / 实验归档 / 数仓。
- Paper Trading v2、Selection Center、StrategyPackage、未来实盘前置验证。
- 脚本、测试流水线、nox、Codex/Claude Code agent 工作流。
- 文档、设计方案、分析报告、测试证据和发布记录。

## 4. 风险等级

| 等级 | 含义 | 新增/修改代码处理 | 历史代码处理 |
|---|---|---|---|
| P0 | 可能导致错误交易、错误回测结论、资产破坏、生产干扰、数据不可追溯 | 阻断提交，必须修复或明确用户授权 | 建立 backlog，优先治理；若影响当前功能必须先隔离 |
| P1 | 影响可复现、可维护、数据完整、DB 可读性或长期稳定性 | 默认阻断；特殊情况需有例外记录和到期时间 | 分模块治理，新增代码不得扩大 |
| P2 | 可维护性、性能、UI 可读性、覆盖率不足等质量风险 | 允许 warning 或按模块阻断 | 纳入质量趋势和重构计划 |
| P3 | 命名、注释、局部风格等低风险问题 | 建议修复 | 逐步改进 |

## 5. 最高优先级红线

以下红线适用于所有代码和脚本：

1. 不得直接或间接干扰生产端口 `8001`，除非用户明确要求生产重启或生产操作。
2. Windows 侧 backend 不得直接读取 WSL 或远端 worker workspace 文件；必须通过 API 或 AIstock-owned artifact store。
3. 不得用空数组、默认值、`None`、`True`、默认价格、默认资金、默认持仓伪装成功。
4. 不得在交易、Paper Trading、QE 回测、HMM、执行算法中静默降级业务逻辑。
5. 不得修改 StrategyPackage frozen manifest、模型权重、HMM snapshot、QE/RD-Agent artifact、Paper ledger 等受保护资产，除非任务明确要求并有验证证据。
6. 新增 DB 表和字段必须有 PostgreSQL comment，说明业务含义、单位、来源和质量语义。
7. QE 实验和 loop 进入 `complete` 状态前，必须能够说明 required sections 是否完整；不完整必须是 `partial/failed`。
8. 所有可测试功能在提交前必须经过对应自动化测试流水线验证。
9. 设计方案必须放在 `docs/architecture/`；规范必须放在 `docs/standards/`；分析证据必须放在 `docs/analysis/` 或 `tests/aistock_validation/history/`。
10. 不得未经用户同意执行“简化版开发”并声称满足完整需求。

## 6. 机器可读规则映射

本节中的规则 ID 必须与 `docs/standards/aistock_development_standard_v1.0_20260504.yaml` 保持一致。

<a id="rule-arch-wsl-001"></a>
### 6.1 [ARCH-WSL-001] 禁止 Windows 侧直接访问 WSL/远端 workspace

- 严重等级：P0。
- 禁止范围：backend、frontend、scripts、nox 中的运行时代码。
- 禁止写法：`\\wsl$`、`\\wsl.localhost`、`/mnt/f/...`、直接拼 worker workspace 路径读取产物。
- 正确方式：worker API、AIstock-owned artifact store、已入库 payload、带 manifest 的 artifact URI。
- 例外：仅限文档说明、测试 fixture、明确的 worker 侧脚本；例外必须在规则中排除。

<a id="rule-prod-port-001"></a>
### 6.2 [PROD-PORT-001] 禁止测试和工具重启/杀死生产 backend 8001

- 严重等级：P0。
- 开发和验证必须使用 backend `8011/8012`、frontend `3011/3012`。
- 测试脚本不得包含针对 `8001` 的 kill、restart、uvicorn 启动、Stop-Process、taskkill。
- 如确需生产重启，只能由用户明确授权并单独执行。

<a id="rule-err-fallback-001"></a>
### 6.3 [ERR-FALLBACK-001] 禁止 broad exception 伪装成功

禁止：

```python
try:
    run_business_job()
except Exception:
    return []
```

要求：

- 捕获异常后必须返回结构化错误、记录 `partial/failed`、或重新抛出业务异常。
- Parser 可以汇总错误，但最终状态必须明确，不能静默丢字段。
- UI 必须显示失败原因或缺失字段，不得把空值展示为完整成功。

<a id="rule-trading-fallback-001"></a>
### 6.4 [TRADING-FALLBACK-001] 禁止交易/回测/HMM 静默降级

- Paper Trading v2 缺分钟线、pre_close、limit、suspend、Torch/context、HMM coefficient 时必须 fail-fast。
- V25 或分钟执行算法不得自动降级为 TWAP 或日频回测。
- HMM 缺 coefficient 不得使用中性系数伪装成功。
- Qlib execution price basis 不匹配时不得默认 `factor=1`。
- 如有降级模式，必须显式配置、UI 可见、日志可审计、测试覆盖，并确认不会改变业务结论。

<a id="rule-config-hardcode-001"></a>
### 6.5 [CONFIG-HARDCODE-001] 禁止运行时代码硬编码路径和密钥

- 路径、端口、worker 地址、artifact root、数据库连接、API key 必须来自配置、环境变量、DB catalog、manifest 或用户请求。
- 禁止硬编码 `F:\Dev\AIstock`、`C:\Users\...`、生产端口、token、password、secret。
- 示例、文档、测试 fixture 可以出现路径样例，但不得进入运行时默认值。

<a id="rule-qe-artifact-001"></a>
### 6.6 [QE-ARTIFACT-001] QE artifact 必须有 manifest

QE/RD-Agent/Qlib 产物不得只保存文件路径。manifest 至少应包含：

- `artifact_id`、`artifact_type`、`uri`、`storage_tier`。
- `sha256`、`size_bytes`、`row_count`、`schema_version`。
- `created_at`、`source_task_id`、`source_loop_index`、`producer`。
- `quality_status`、`missing_sections`、`cleanup_eligible`。

数仓归档不得依赖 QE runtime DB 或 worker workspace 在未来仍然存在。

<a id="rule-ui-rawjson-001"></a>
### 6.7 [UI-RAWJSON-001] 操作员 UI 不得以 raw JSON 作为主要业务视图

- UI 应使用中文业务标签、表格、卡片、图表、错误态和缺失原因。
- JSON 可以作为高级调试抽屉，但不能作为普通操作员的主要界面。
- 按钮 disabled 必须展示原因；后端错误必须透传为可读提示。

<a id="rule-resource-timeout-001"></a>
### 6.8 [RESOURCE-TIMEOUT-001] 外部调用和子进程必须有 timeout/生命周期控制

- HTTP 请求、subprocess、DB 长查询、文件批处理应有 timeout、取消、日志路径、退出码和资源释放。
- 长任务必须有 heartbeat、状态持久化和幂等执行策略。
- 大文件和大 DataFrame 必须采用 chunk/batch 或明确内存边界。

<a id="rule-db-comment-001"></a>
### 6.9 [DB-COMMENT-001] 新增 DB 表和字段必须有 comment

- 每张新表必须有 `COMMENT ON TABLE`。
- 每个新字段必须有 `COMMENT ON COLUMN`。
- comment 应包含业务语义、单位、来源、可空含义、质量语义或枚举含义。
- JSONB 字段 comment 必须说明 schema/version/source/quality 约束。
- DDL 不得由业务 service 隐式执行。

## 7. Python 后端工程规范

### 7.1 分层边界

- router 只负责 API contract、确认文本、参数校验、HTTP 错误映射。
- service 负责业务编排和状态转换。
- repository/data access 负责持久化和查询，不承载业务决策。
- schema/bootstrap/migration 管理 DDL，业务请求不得偷偷建表或改 schema。
- 高风险服务必须支持依赖注入，便于测试替换 DB、HTTP client、clock、worker client。

### 7.2 类型和数据结构

- 新增 API request/response 使用 Pydantic model 或明确 TypedDict/dataclass。
- 核心业务不得长期传递无 schema 的大 dict。
- JSONB payload 必须带 `schema_version`、`source`、`quality_status`。
- 时间字段必须说明时区和口径；金额字段必须说明币种；收益率字段必须说明是否百分比。
- 函数、变量、模块使用 `snake_case`；类使用 `PascalCase`；常量使用 `UPPER_SNAKE_CASE`。

### 7.3 错误处理

- 业务失败必须 fail-fast 或进入可审计的 `partial/failed` 状态。
- catch broad exception 必须带上下文日志和结构化错误。
- 不得因环境错误改变业务语义，例如缺分钟线改用日频、缺模型改用缓存、缺因子改用 0。
- 错误信息应包含：错误码、触发条件、输入摘要、缺失字段、建议修复方式。

### 7.4 配置和环境

- 配置优先级必须清晰：显式请求 > 环境变量 > DB catalog > 默认配置。
- 默认值进入 QE/Paper/回测执行前必须展开成 effective config 并持久化。
- 新增配置必须说明有效范围、单位、默认来源和是否会影响历史结果解释。

### 7.5 资源和并发

- 子进程必须记录命令摘要、环境白名单、日志路径、退出码。
- 长任务必须支持 timeout、cancel、heartbeat、状态持久化和幂等重试。
- 全局 cache 必须有 max size、TTL、clear 或生命周期说明。
- 后台 scheduler 默认不得在开发端口推进生产可见状态，除非明确 feature flag 开启。

## 8. 数据库和数据管线规范

1. DB schema 变更必须有 migration/bootstrap 文件和测试。
2. 新表/字段必须有 comment，并由测试或 smoke 检查覆盖。
3. 数据管线必须记录 source、as_of、trade_date、row_count、hash/quality、refresh status。
4. 数据修复必须可审计，不能用默认 0 或空值覆盖真实缺失。
5. Tushare、TDX、QMT、Qlib、HMM 数据必须明确 PIT 口径。
6. 删除或清理数据前必须区分业务资产和临时缓存，受保护资产不得当垃圾删除。
7. 大型明细优先 artifact store + manifest，DB 存结构化索引和分析必要字段。

## 9. QE / RD-Agent / 数仓规范

### 9.1 实验创建阶段必须记录

- experiment/task id、loop index 规划、实验类型、用户说明、创建时间、创建人/agent。
- requested config 和 effective config。
- 模型类型、模型版本、超参数、随机种子、label horizon、数据频率、训练/验证/回测区间。
- 因子列表、因子版本、因子分类、因子独立指标、相关性矩阵版本。
- 策略参数：topk、n_drop、持仓数量、每日换股数量、最短持股时间、初始资金、成本、benchmark、停牌/涨跌停处理。

### 9.2 实验执行阶段必须记录

- worker 节点、开始/结束时间、状态转换、heartbeat、失败原因。
- 训练曲线、valid metric、early stop、best iteration、模型 artifact。
- 特征/因子权重或 attribution；LGB/LSTM 优先实现最小可验证路径，其他模型保留 schema。
- 订单意图、子单、成交、未成交原因、tail-substitute 候选、最终成交方向和金额。
- 成本扣费路径、现金变动、持仓变动和 report cost 对账。

### 9.3 实验完成阶段必须记录

- 期初资产、期末资产、绝对收益、超额收益、with/without cost、最大回撤、Sharpe、turnover、benchmark。
- 收益曲线、回撤曲线、IC/RankIC 曲线、成本曲线。
- position summary、holding audit、stock trades、symbol summary、execution events。
- artifact manifest 和缺失字段说明。
- backtest-only loop 必须记录训练来源：source task、source loop、model artifact、训练指标引用。

### 9.4 数仓独立性

- QE runtime DB 和 worker workspace 未来可以清理，数仓必须独立回答历史实验详情。
- QE DB 可重复保存小型结构化指标，用于 UI 展示、重试、恢复；大型明细不要塞入 QE DB 大 JSON。
- 数仓应保存分析所需结构化指标，并持有 artifact manifest 与必要 parquet/文件资产。
- 历史补录和新增 loop 补录必须通过 API 完成，不依赖手工脚本或 Windows 侧直读 worker 文件。

## 10. Qlib / HMM / 因子和模型规范

- Qlib stock export 对 QE 默认使用 SH/SZ；BJ/BSE 排除策略必须明确。
- feature 数据与可交易股票池 eligibility 必须分离，不能因未来 ST/暂停/退市删除历史事实。
- Qlib adjusted price 与 raw limit/pre_close 比较时必须显式转换口径。
- HMM snapshot、coefficient artifact、model registry 必须可追溯 as_of 和 effective date。
- 因子入库必须有版本、分类、独立指标、相关性信息和数据覆盖质量。
- 模型库必须记录模型类型、训练数据、超参数、artifact、指标和可复现配置。

## 11. Paper Trading v2 / Selection Center / StrategyPackage 规范

- StrategyPackage frozen manifest 和 manifest hash 不得静默修改。
- Selection Center live/paper selection 不得直接使用 QE 回测 pred.pkl 作为权威信号。
- Paper Trading v2 日运行必须校验交易日历、suspend_d、stk_limit、minute bars、pre_close、limit、模型上下文。
- 缺少关键市场数据或模型上下文必须 fail-fast。
- 回放、catch-up、live session 必须有独立状态、session lock、幂等 run、cancel/timeout。
- 现金、持仓、订单、成交、NAV、错误事件必须持久化并可对账。

## 12. 前端 UI 规范

- UI 必须调用真实 backend 能力，不得用 mock/静态成功伪装完整功能。
- 普通操作界面应展示中文业务标签、状态、指标、图表和缺失原因。
- raw JSON 只能放高级调试区，默认不作为主要视图。
- 按钮 disabled 必须有可读原因；失败必须展示后端错误码和修复建议。
- UI E2E 必须监听 pageerror、console error、requestfailed 和非预期 4xx/5xx。
- 前端验证使用 3011/3012，不得影响生产前端或后端。

## 13. 测试、覆盖率和提交规范

### 13.1 设计阶段测试要求

每个新功能设计文档必须包含：

- 业务目标和 false-success 风险。
- L0/L1/L2/L3/L4/L5 测试范围。
- API/DB/UI/log/business oracle 验证方式。
- 长运行任务的 nightly/后台验证策略。
- 覆盖率目标和不适合自动化的人工确认项。

### 13.2 提交前最低验证

新增或修改代码提交前至少执行：

```powershell
python -m compileall <changed-python-paths>
python -m pytest <targeted-tests> -q -p no:cacheprovider
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
```

涉及 UI/API/DB/QE/Paper/HMM/数据管线的变更，还必须执行对应 nox session、API/DB smoke、UI E2E 或 run evidence。

### 13.3 覆盖率目标

- 新增/修改 Python 代码 line coverage 目标 >= 80%。
- 新增/修改 Python 代码 branch coverage 目标 >= 70%。
- QE 数据完整性、warehouse/archive、交易执行、成本/ledger、HMM、cleanup gate 必须有 L1/L2 和业务 oracle。
- 长回测/长实验可先做 fast gate，再进入 nightly/L4/L5；未通过前不得标记生产可用。

## 14. Agent 开发和 Bug 修复规范

- Codex/Claude Code 必须先读取项目记忆和相关设计/规范。
- 只修改当前任务相关文件，不处理其他窗口的 dirty workspace。
- 不得回滚用户或其他工具修改。
- 修复 bug 必须先复现，再修改，再跑失败用例和周边回归。
- Bug 记录应包含描述、触发条件、严重等级、复现命令、疑似文件、修复 commit、验证记录。
- Agent 可读取机器 YAML 和 Validation Center 只读上下文；不得直接执行任意 SQL/shell 或读取 worker workspace。
- 若规则与任务冲突，必须向用户说明并取得明确授权。

## 15. 文档归属规范

| 类型 | 目录 |
|---|---|
| 项目规范 | `docs/standards/` |
| 历史规范版本 | `docs/standards/archive/` |
| 架构和设计方案 | `docs/architecture/` |
| 分析报告和调查结论 | `docs/analysis/` |
| 自动化验证历史记录 | `tests/aistock_validation/history/` |
| 测试矩阵和测试模块说明 | `tests/aistock_validation/modules/` |
| 临时运行产物 | `tmp/` 或被 `.gitignore` 排除的运行目录 |

规范不得散落在 `docs/architecture`；架构设计也不得复制粘贴规范正文，只能引用本规范。

## 16. 历史代码治理策略

AIstock 经历过多工具探索式开发，历史代码存在大量不符合规范的情况。治理原则：

1. 不做一次性全仓大重排，避免引入行为风险。
2. baseline findings 作为历史技术债，不等于立即全部阻断。
3. changed-files 中新增 P0/P1 必须优先阻断。
4. 高风险模块先治理：QE、Paper Trading v2、Selection Center、HMM、Qlib 数据、DB schema、资产清理。
5. 每次治理必须有回归测试和 run evidence。
6. 历史例外必须有 owner、原因、到期时间和复查计划。

## 17. 当前第一阶段暂不做

- 不对全仓历史代码一次性 Black 格式化。
- 不强制历史代码立刻补满类型标注。
- 不把所有 P2/P3 历史问题作为第一版阻断项。
- 不直接启用 QE 实时生产 hook。
- 不清理 QE workspace 或 QE DB 历史记录。
- 不直接做 LLM agent 自动演进。
- 不一次性实现所有模型 attribution；先保留 schema 和 LGB/LSTM 最小路径。
- 不引入新的 NoSQL/复杂存储系统作为第一阶段依赖。

## 18. 一致性验证要求

本规范与机器 YAML 必须满足：

1. YAML `source_standard` 指向当前 MD 文件。
2. YAML `source_version` 等于本文版本。
3. YAML 中每条 enabled rule 必须有 `standard_ref`。
4. YAML 中每个 rule_id 必须在本文出现。
5. 人类规范新增 P0/P1 红线时，必须同步新增机器规则或标注手工审查项。
6. scanner、测试、nox 命令应默认读取当前 YAML，而不是历史归档版本。

## 19. 参考基线

- PEP 8 / PEP 257 / PEP 484：Python 命名、docstring、类型标注基础。
- Google Python Style Guide：异常、全局状态、线程/并发、可维护性建议。
- pytest / nox / Playwright：自动化验证基础。
- Qlib Recorder：Experiment -> Recorder/run 和 artifact 思想。
- QuantConnect LEAN / NautilusTrader：backtest/live 边界、事件驱动、执行测试。
- Freqtrade / Hummingbot：backtest summary、dry-run/live、connector/controller/executor 分层。
