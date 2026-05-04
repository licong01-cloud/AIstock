# AIstock Legacy / Dead Code 只读预盘点

> 日期：2026-05-04
> 状态：只读预盘点；不是删除清单，不作为自动清理依据
> 文档位置：`docs/analysis/aistock_legacy_dead_code_prebaseline_20260504.md`
> 关联方案：`docs/architecture/aistock_development_standard_v1_2_calibration_plan_20260504.md`

## 1. 目的

本预盘点用于支持开发规范 v1.2 的历史遗留代码治理设计。当前仓库经历过多轮探索式开发和多工具并行修改，存在大量旧脚本、旧设计文档、根目录历史文件、疑似一次性诊断代码。本文只做只读统计和候选方向，不删除、不移动、不重命名任何文件。

## 2. 盘点边界

- 统计来源：`git ls-files` 的 tracked 文件列表。
- 未扫描运行时引用图、动态 import、DB scheduler、前端路由实际访问量。
- 当前工作区存在其他窗口的未提交修改、删除和未跟踪文件；本文不处理这些改动，也不把它们作为删除结论。
- 结论只表示“需要治理/需要复核”，不是“可以删除”。

## 3. 只读命令

```powershell
$files = git ls-files
$total = $files.Count
$py = ($files | Where-Object { $_ -like '*.py' }).Count
$rootFiles = $files | Where-Object { $_ -notmatch '/' }
$rootMd = $rootFiles | Where-Object { $_ -like '*.md' }
$docsRootMd = $files | Where-Object { $_ -match '^docs/[^/]+\.md$' }
$scriptsPy = $files | Where-Object { $_ -match '^scripts/.*\.py$' }
$debugLikeScripts = $scriptsPy | Where-Object { $_ -match '(?i)(debug|diagnos|tmp|one_off|test_|verify|v\d+|mini|simple)' }
```

## 4. 初步统计

| 指标 | 数量 | 解释 |
|---|---:|---|
| tracked files | 2,397 | Git 已跟踪文件总量。 |
| tracked Python files | 1,043 | Python 代码和脚本规模较大。 |
| backend Python files | 464 | 后端核心治理应分模块推进。 |
| frontend `ts/tsx` files | 178 | 前端规则应通过 tsc/Playwright 分层治理。 |
| root-level tracked files | 47 | 根目录历史文件较多，存在治理空间。 |
| root-level tracked Markdown | 8 | 历史根目录文档需要迁移或归档评审。 |
| `docs/*.md` root docs | 116 | 大量旧分析/设计文档未按 v1.1 目录规范归类。 |
| `scripts/**/*.py` | 220 | 正式脚本和一次性脚本混杂风险较高。 |
| debug-like scripts in `scripts/` | 34 | 可能应评估迁移到 `debug_tools/`、归档或转正式脚本。 |

## 5. 根目录历史文件样例

以下是 tracked 根目录文件样例，不能直接判定为垃圾文件，但说明根目录需要单独治理：

```text
RD-Agent_Qlib_多因子备忘录.md
RDAGENT_LLM_CONFIG_DEPLOYMENT_GUIDE.md
__init__.py
ai_agents.py
analysis_current_implementation_problems.md
app_pg.py
config.py
create_catalog_tables.sql
create_tables.py
data_source_manager.py
deepseek_client.py
export_index_tdx_to_qlib_bin.py
findings.md
miniqmt_interface.py
monitor_scheduler.py
monitor_service.py
network_optimizer.py
notification_service.py
pg_monitor_repo.py
pg_portfolio_db.py
pg_smart_monitor_repo.py
pg_watchlist_repo.py
portfolio_manager.py
portfolio_scheduler.py
progress.md
run_migration.py
sector_strategy_agents.py
sector_strategy_data.py
sector_strategy_db.py
sector_strategy_engine.py
sector_strategy_markdown.py
sector_strategy_pdf.py
sector_strategy_scheduler.py
sector_strategy_ui.py
stock_data.py
task_plan.md
```

治理建议：

- 明确是否仍被当前 backend/frontend/scheduler 引用。
- 仍在用的迁移到合适模块或保留为稳定入口并补说明。
- 已废弃的进入 `delete_candidate`，经过验证后单独清理。
- `findings.md`、`progress.md`、`task_plan.md` 如为 agent 临时文件，应纳入 planning 文件治理规则，避免污染根目录。

## 6. `docs/*.md` 历史文档样例

当前 tracked `docs/*.md` 根目录文档较多，v1.1 规范要求后续设计、分析、运维、用户指南分别进入专门目录。

样例：

```text
docs/2025-12-24_DataServiceLayer_Implementation_Design_AIstock.md
docs/QE_Analysis_and_Design_v1.md
docs/QE_v3_implementation_plan.md
docs/RD-Agent_Workspace_Loop_Factor关联关系分析.md
docs/alpha158_memory_optimization_analysis.md
docs/backend_test_report.md
docs/bin_data_quality_analysis.md
docs/final_analysis_report.md
docs/qmt_design_confirmation_summary.md
docs/rd_agent_prompt_pack_implementation_progress.md
```

治理建议：

- 不在普通功能提交中批量移动旧文档，避免巨大 diff。
- 先按主题建立文档索引：architecture、analysis、operations、user_guides、releases、archive。
- 已被新文档取代的旧文档标记 `legacy_readonly` 或 `delete_candidate`。
- 迁移时保留重定向说明或索引，避免 agent 找不到历史上下文。

## 7. `scripts/` 中疑似一次性脚本样例

以下脚本名称具备 debug、diagnosis、test、verify、mini、v24/v25 等特征，需复核是否仍为正式业务脚本：

```text
scripts/_backfill_v2_deterministic.py
scripts/_dry_grade_v2_overfit.py
scripts/_inspect_v2_samples.py
scripts/_probe_backtest_tables.py
scripts/_scan_rule_b_v2.py
scripts/_smoke_test_factor_analyst_v2.py
scripts/_test_classify_rules.py
scripts/_test_pipeline_10_factors.py
scripts/diagnostics/hmm_qe_candidate_attribution.py
scripts/diagnostics/hmm_sector_factor_overlay_diagnostic.py
scripts/paper_v2_live_validation.py
scripts/qe_evolution_diagnostic.py
scripts/qe_qlib_minute_gap_diagnosis.py
scripts/qlib_v25_limit_state_smoke.py
scripts/smoke_test_10D_models.py
scripts/v24_mini_backtest.py
scripts/verify_hmm_covariance_fix.py
scripts/verify_hmm_direct.py
scripts/verify_hmm_qlib.py
scripts/verify_hmm_simple.py
scripts/verify_hmm_wsl.py
```

治理建议：

- `scripts/diagnostics/**` 可以作为过渡位置，但 v1.2 需要明确与 `debug_tools/` 的关系。
- 若脚本被 nox、scheduler、API 或长期流程调用，应转为正式脚本：参数化、fail-fast、测试覆盖、文档说明。
- 若脚本只是一次性验证，应迁移到 `debug_tools/<module>/<date_or_issue>/` 或归档/删除。
- 删除前必须检查引用和运行证据，不允许按文件名自动删除。

## 8. 风险分级建议

| 候选类型 | 风险 | 建议动作 |
|---|---:|---|
| 根目录 `.py` 业务模块 | 高 | 先做引用关系和入口确认，不能直接删。 |
| `docs/*.md` 旧分析/设计文档 | 中 | 建索引、归档、迁移；低风险但数量大。 |
| `scripts/*test*/*verify*/*mini*` | 中 | 判断是否被流水线调用；无引用再迁移或删除。 |
| 受保护资产、模型、ledger、artifact | 极高 | 不进入 dead-code 自动清理。 |
| untracked 大文件/临时输出 | 中 | 需单独盘点；不纳入本 tracked baseline。 |

## 9. 后续执行建议

1. 建立 `dead_code_inventory` 机器 JSON schema，但第一版只读输出。
2. 扫描引用关系：Python import、`rg` 字符串引用、nox、pytest、scheduler、router、frontend API client。
3. 每个候选项输出 `confidence`、`evidence`、`risk`、`recommended_action`。
4. 先治理根目录污染和 `docs/*.md` 旧文档，因为业务风险较低但数量大。
5. 对 QE/Paper/Qlib/HMM 脚本逐个模块确认，不做批量删除。
6. 每次清理单独提交，只提交清理相关文件，并附验证证据。

## 10. 当前不执行

- 不删除文件。
- 不移动文件。
- 不修改 active standard。
- 不生成阻断门禁。
- 不处理其他窗口的 dirty worktree。
