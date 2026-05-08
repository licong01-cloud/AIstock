# Codex App 多 Agent 并行开发交接包：QE/HMM 热修复与治理整改

日期：2026-05-08
状态：交接包
目标：让 Codex App 在后续多 Agent 模式下，可以不依赖本窗口上下文，独立完成并行开发、测试、审查和合入准备。

## 0. 使用方式

### 0.1 App 与 CLI 关系

本机 CLI 检测结果：`codex-cli 0.128.0`，本地命令支持：

```powershell
codex app [PATH]
codex app-server [OPTIONS]
codex fork
codex resume
codex cloud
```

这说明 CLI 可以启动 Codex Desktop App 或 app-server，但不应假设 CLI 可以强制配置 App 内所有 agent 的项目规范。项目规范的权威来源应放在仓库文档中，并由每个 App agent 在任务提示中显式读取。

推荐做法：

1. CLI 负责准备本交接包、设计文档、测试矩阵、分支命名和任务提示。
2. Codex App 负责打开对应 worktree，并行启动多个 agent。
3. 每个 agent 使用独立 branch/worktree，按本交接包限制自己的文件范围。
4. PM/Integrator agent 只做集成和审查，不与 worker 同时改同一文件。
5. 合入 `main` 前仍由一个主控窗口做最终验证和用户确认。

### 0.2 App 启动示例

```powershell
codex app F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508
```

后续真正实现时，每个 agent 应打开自己的 worktree，例如：

```powershell
codex app F:\Dev\AIstock_worktrees\qe-backtest-recorder-isolation-20260508
codex app F:\Dev\AIstock_worktrees\qe-score-weighted-capacity-v1-20260508
codex app F:\Dev\AIstock_worktrees\qe-governance-integration-20260508
```

如果某个任务必须同时修改 RD-Agent 外部仓库，应单独说明 `--add-dir F:\Dev\RD-Agent-main` 或在 App 中同时加入该目录；不要在 AIstock 分支里假装已经修改 worker 侧代码。

## 1. 所有 Agent 必读文档

每个 agent 开始前必须读取：

1. `docs/codex_project_memory.md`
2. `docs/standards/aistock_development_standard_v1.1_20260504.md`
3. `docs/operations/qe_hmm_experiment_infra_issues_20260508.md`
4. `docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md`
5. `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`
6. 与任务相关的代码文件和既有测试文件

禁止只读本交接包就直接改代码。

## 2. 全局开发规范

- 不修改 `AGENTS.md`。
- 不在 `F:\Dev\AIstock` 生产 root 直接开发；每个任务必须用独立 worktree。
- 不重启、kill 或 reload 生产 backend `8001`，除非用户单独明确授权。
- 不修改受保护资产：StrategyPackage frozen manifest、模型权重、HMM snapshot、QE/RD-Agent artifact、Paper ledger、validated policy。
- 不使用 `git reset --hard`、`git checkout -- .`、`git clean -fd` 等破坏性命令。
- 不把临时脚本放根目录；临时诊断脚本放 `debug_tools/`，正式可复用脚本放 `scripts/` 并补测试。
- 新 DB 表/字段必须有 PostgreSQL comment。
- QE/HMM/交易路径不允许 silent fallback；缺关键数据必须 fail-fast。
- 每个 agent 返回时必须列出：来源文档、修改文件、测试命令、验证结果、残留风险、是否触碰生产 `8001`。

## 3. 分支和角色总览

| Agent | 分支建议 | 任务 | 是否可优先合 `main` | 主要文件范围 |
| --- | --- | --- | --- | --- |
| PM/Integrator | `codex/qe-hmm-hotfix-integration-20260508` | 集成、审查、合并顺序、最终 gate | 只做集成 | docs、测试记录、冲突解决 |
| Agent A | `codex/qe-backtest-recorder-isolation-20260508` | backtest-only recorder 隔离 bugfix | 是，P0 | `scripts/qrun_limit*.py`、QE worker/API 调度相关、QE tests |
| Agent B | `codex/qe-score-weighted-capacity-v1-20260508` | 新建容量参数化策略资产和 UI/DB 可选 | 可独立上线，若有代码需合 `main` | strategy catalog、config composer、frontend QE UI、StrategyPackage contract |
| Agent C | `codex/qe-hmm-hotfix-validation-20260508` | 自动化测试、business oracle、验证流水线 | 随 A/B 合入 | `backend/tests/`、`tests/aistock_validation/`、debug_tools |
| Agent D | `codex/qe-governance-integration-20260508` | 长期治理详细实施规划和阶段 gate | 否，长期分支 | model registry、SOTA、StrategyPackage 设计/迁移 docs |
| Agent E | `codex/qe-hotfix-review-20260508` | 独立 code review 和回归审计 | 不直接写业务代码 | review notes、validation records |

## 4. Agent A：backtest-only recorder 隔离 bugfix

### 4.1 目标

修复 backtest-only 并行 loop 共享或 symlink 写入 source `mlruns` 的问题。source recorder 只读，target recorder 必须 loop-local、非 symlink、非同 realpath。

### 4.2 允许文件范围

AIstock：

```text
scripts/qrun_limit.py
scripts/qrun_limit_minute.py
backend/services/quantevolver/qe_evolution_service.py
backend/services/quantevolver/qe_workspace_client.py
backend/routers/quantevolver*.py
backend/tests/**/test_*qe*recorder*.py
backend/tests/**/test_*backtest_only*.py
tests/aistock_validation/history/qe/*.md
```

外部 RD-Agent 如确需修改，必须单独说明并使用独立分支：

```text
F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py
F:/Dev/RD-Agent-main/**/qe_workspace*相关测试
```

不得修改策略资产、模型库 schema、Paper v2 业务逻辑。

### 4.3 必做实现点

- 增加 source/target `mlruns` realpath 检查。
- backtest-only 模式下不允许 target `mlruns` 是 symlink。
- source `mlruns` 只用于读取模型参数，target `MLFLOW_TRACKING_URI` 只指向 loop-local 目录。
- 写入 `qe_recorder_isolation.json`。
- `qe_current_recorder.json` 指向本次 target recorder。
- malformed metric retry 只能在 isolation passed 后做，且不能吞掉真实错误。

### 4.4 必跑测试

最少：

```powershell
python -m py_compile scripts/qrun_limit.py scripts/qrun_limit_minute.py
pytest backend/tests -q -p no:cacheprovider -k "backtest_only or recorder or qrun"
git diff --check
```

如果有 RD-Agent 侧修改，必须增加 worker 侧测试或最小复现脚本，并说明未重启生产 `8001`。

### 4.5 可复制给 App Agent 的提示

```text
你是 Agent A，负责 AIstock QE backtest-only recorder isolation P0 bugfix。请先阅读 docs/codex_project_memory.md、docs/standards/aistock_development_standard_v1.1_20260504.md、docs/operations/qe_hmm_experiment_infra_issues_20260508.md、docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md、tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md。你不独占代码库，其他 agent 会并行工作；不要修改策略资产、模型库或 Paper v2 文件。目标是保证 backtest-only target recorder loop-local、非 symlink、不与 source mlruns 同 realpath，并写入 qe_recorder_isolation.json。请先找出当前 symlink/params 传递/MLFLOW_TRACKING_URI 的真实代码路径，再实现 fail-fast 和测试。不要重启生产 8001，不要修改受保护资产。完成后提交分支 codex/qe-backtest-recorder-isolation-20260508，并按交接格式报告来源、文件、测试、风险。
```

## 5. Agent B：容量参数化 ScoreWeighted V2 新策略资产

### 5.1 目标

新增可在 DB/UI 选择的容量参数化策略，不改变旧 `score_weighted_topk_v2`。新策略用于 HMM/QE 验证时避免固定 5M cap 抑制权重效果。

### 5.2 允许文件范围

```text
scripts/register_score_weighted_strategy_v2_capacity_v1.py
scripts/score_weighted_strategy_v2_capacity_v1.py 或等价资产源文件
scripts/qe_suspend_filter_score_weighted_strategy_capacity_v1.py（如果需要）
backend/db/migrations/*score_weighted*capacity*.sql（如需）
backend/services/quantevolver/config_composer.py
backend/services/strategy_package/backtest_contract.py
backend/services/strategy_package/runtime.py（只在确需识别新 strategy_id 时）
frontend/src/app/quantevolver/evolution/**
backend/tests/**/test_*strategy*capacity*.py
frontend/tests/**
tests/aistock_validation/history/qe/*.md
```

不得修改旧 `score_weighted_strategy_v2.py` 的默认行为，不得回写历史 StrategyPackage manifest。

### 5.3 必做实现点

- 新策略文件、新 class、新 strategy_id。
- 默认 `max_single_order_value=1000000000.0`，并在 param_schema 中暴露。
- `max_weight`、`max_position_ratio`、`max_single_order_value` 必须进入 requested config 和 effective config。
- UI 选择新策略后可编辑容量参数。
- 旧策略显示 legacy/capacity-constrained 提示。
- StrategyPackage/Paper contract 能识别新 strategy_id，但旧 package 缺字段仍走 legacy 5M default。

### 5.4 必跑测试

```powershell
python -m py_compile scripts/register_score_weighted_strategy_v2_capacity_v1.py backend/services/quantevolver/config_composer.py backend/services/strategy_package/backtest_contract.py
pytest backend/tests -q -p no:cacheprovider -k "strategy and (capacity or score_weighted or package)"
cd frontend; npm run typecheck 或 npm run build
cd ..; git diff --check
```

如果只做 DB/asset 注册而不改代码，也必须提供：

- dry-run SQL 或注册脚本输出；
- strategy catalog 查询结果；
- QE UI 能读到新 strategy_id 的截图或 API smoke；
- 旧 strategy_id hash/source/default_config 未改变的证明。

### 5.5 可复制给 App Agent 的提示

```text
你是 Agent B，负责新增 score_weighted_topk_v2_capacity_v1 策略资产。请先阅读 docs/codex_project_memory.md、docs/standards/aistock_development_standard_v1.1_20260504.md、docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md、tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md。你不独占代码库，Agent A 会并行修 recorder bug；不要修改 backtest-only recorder 逻辑。你的核心约束是：新建策略文件和新 strategy_id，不直接改旧 score_weighted_topk_v2 行为。让 DB/UI 可选择新策略，并暴露 max_single_order_value/max_weight/max_position_ratio。旧策略必须保留 legacy_5m_cap 语义。不要修改历史 StrategyPackage manifest，不要重启生产 8001。完成后提交分支 codex/qe-score-weighted-capacity-v1-20260508，并按交接格式报告。
```

## 6. Agent C：自动化测试与验证流水线

Agent C 负责把两个热修复变成可由 CI/本地自动执行的测试和验证 gate，避免“跑通一次 QE”但缺乏回归保护。

允许文件范围：

```text
backend/tests/**
frontend/tests/**
tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md
tests/aistock_validation/history/qe/*.md
debug_tools/qe/**
```

Agent C 可读 Agent A/B 分支 patch，但不得直接改 A/B 业务实现文件，除非 PM/Integrator 明确转交。

可复制提示：

```text
你是 Agent C，负责 QE/HMM 热修复测试和验证流水线。请先阅读 docs/codex_project_memory.md、docs/standards/aistock_development_standard_v1.1_20260504.md、docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md、tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md。你不负责实现业务逻辑，不要修改 Agent A/B 正在改的文件。请为 recorder isolation 和 capacity strategy 新策略设计并实现/补充测试，覆盖 fail-fast、非 symlink、same realpath rejection、parallel target isolation、旧策略不变、新策略参数可见、Paper contract 兼容等。不要重启生产 8001，不要修改受保护资产。完成后提交分支 codex/qe-hmm-hotfix-validation-20260508，并报告测试覆盖缺口。
```

## 7. Agent D：长期治理分支设计落地

Agent D 基于主治理方案和本补充设计，拆分 SOTA/StrategyPackage/模型库/seed/数仓的长期实施任务，使用独立集成分支，不影响生产。

允许文件范围：

```text
docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md
docs/architecture/*model_registry*.md
docs/architecture/*sota*.md
docs/operations/*handoff*.md
backend/db/migrations/*（仅设计或后续授权）
```

可复制提示：

```text
你是 Agent D，负责长期 QE asset governance 设计落地，不做两个热修复的代码实现。请先阅读 docs/codex_project_memory.md、docs/standards/aistock_development_standard_v1.1_20260504.md、docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md、docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md。目标是在 codex/qe-governance-integration-20260508 中细化 Phase 0-7，包括 seed contract、模型库四层结构、SOTA 手工晋级、StrategyPackage 增强、原始配置复测、runtime variant、数仓归档。必须遵守生产 DB additive-only 规则，不修改现有生产记录，不重启生产 8001。完成后输出阶段计划、DDL 草案、测试 gate 和与 Claude Code 的协作边界。
```

## 8. Agent E：独立审查

Agent E 在 A/B/C 完成后做 code review 和业务回归审计，重点发现 false success、silent fallback、生产资产污染、旧策略语义漂移。

审查清单：

- backtest-only target `mlruns` 是否可能仍指向 source。
- malformed metric 重试是否可能掩盖隔离失败。
- source model artifact 是否只读。
- 新策略是否确实新 strategy_id + 新 source file。
- 旧 `score_weighted_topk_v2` 默认和 source hash 是否不变。
- UI 是否暴露容量参数，而不是只显示 `initial_cash`。
- StrategyPackage 旧 manifest 是否仍兼容。
- 测试是否在不触碰生产 `8001` 的情况下完成。

可复制提示：

```text
你是 Agent E，负责独立 review，不直接实现功能。请阅读相关设计和 Agent A/B/C 的 diff，按 code review 心态输出发现，优先列出 P0/P1 bug、行为回归、缺测试和生产风险。发现必须带文件/行号。不要修改业务代码，除非 PM/Integrator 单独要求。不要重启生产 8001。
```

## 9. 统一交接回复格式

每个 agent 完成后必须按以下格式回复：

```text
分支：
提交：
读取的文档：
修改文件：
未修改但检查过的关键文件：
测试命令和结果：
业务验证结果：
生产 8001 是否触碰：否/是（必须说明）
受保护资产是否触碰：否/是（必须说明）
DB 是否写入：否/是（schema/table/record id）
残留风险：
建议下一步：
```

## 10. 合入顺序建议

1. Agent A recorder isolation：P0，验证通过后可单独合 `main`。
2. Agent C 针对 A 的测试：随 A 合入或先后合入。
3. Agent B capacity strategy：若只需资产注册，可生产资产操作；若代码改动，验证后合 `main`。
4. Agent C 针对 B 的测试：随 B 合入。
5. Agent D 长期治理：进 `codex/qe-governance-integration-20260508`，不急于合 `main`。
6. Agent E review：每个合入前执行。

## 11. 明确禁止

- 禁止两个 worker 同时编辑同一文件后让 PM 盲目合并。
- 禁止在测试中删除或重写真实 QE workspace、source `mlruns`、HMM snapshot、model weights。
- 禁止以“兼容旧实验”为理由继续 target symlink 写 source `mlruns`。
- 禁止直接修改旧策略文件来实现容量参数化。
- 禁止把 long-term governance 与 P0 bugfix 混在一个大 PR。
- 禁止任何 agent 自行决定重启生产 `8001`。
