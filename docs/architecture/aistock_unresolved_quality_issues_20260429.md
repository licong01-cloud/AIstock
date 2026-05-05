# AIstock 未解决质量问题与脚本目录扫描清单（2026-04-29）

本文件用于下次继续检查确认。来源为 2026-04-27 至 2026-04-29 的高优先级审计、独立指标单写入设计、legacy 指标脚本盘点，以及 2026-04-29 对当前工作区的只读扫描。

> 约束：本次只做文档更新和只读扫描，没有删除、移动、归档任何文件。

## 1. 关联文档

- `docs/architecture/aistock_high_priority_issue_audit_20260427.md`
- `docs/architecture/factor_independent_metrics_single_writer_design_20260427.md`
- `docs/architecture/factor_legacy_metric_scripts_inventory_20260428.md`
- `docs/architecture/qe_remote_execution_capability_audit_20260427.md`
- `analysis_current_implementation_problems.md`
- `docs/api_implementation_issues.md`

## 2. 当前扫描方法

扫描命令原则：

```powershell
git ls-files --cached --others --exclude-standard
```

含义：

- 只统计 git 已跟踪文件和未被 `.gitignore` 忽略的未跟踪文件。
- 不统计已经被 git 忽略的目录。
- 后端/前端运行时引用扫描范围：`backend/routers`、`backend/services`、`backend/data_service`、`backend/infra`、`backend/execution_algos`、`backend/db`、`backend/qlib_exporter`、`backend/main.py`、`frontend/src`。
- 候选文件范围：`scripts/`、`backend/scripts/`、`backend/tests/`、`tests/`、`frontend/tests/`、`configs/`、根目录脚本/配置文件。
- “未被后端/前端调用”仅代表在上述运行时代码中没有发现直接字符串引用或 import/call 迹象；不等于一定无价值，仍需按类型判断。

本次只读扫描结果：

- 非忽略文件总数：2402
- 被扫描的后端/前端运行时源码文件：450
- 脚本/测试/配置候选文件：437
- 被运行时代码直接引用候选：39
- 未被运行时代码直接引用候选：398
- 未跟踪候选文件：33

按类型统计：

| 类型 | 候选数 | 未被运行时直接引用 |
| --- | ---: | ---: |
| diagnostic_analysis | 100 | 98 |
| misc_script | 107 | 93 |
| migration_backfill_admin | 46 | 43 |
| patch_fix_oneoff | 38 | 38 |
| research_or_runtime_helper | 48 | 32 |
| test | 97 | 93 |
| config | 1 | 1 |

## 3. 已基本完成或已明显改善的问题

以下问题在当前工作区已看到明显修复迹象，但仍需合并前复核：

- `backend/services/manual_factor_service.py` 已改为通过 `FactorOfficialEvaluationService` 和 `FactorRatingService` 产出正式指标/评级；原 `_save_metrics_to_db()` 只保留 legacy no-op 注释。
- `backend/services/quantevolver/factor_analyst.py` 已将 LLM 评级职责降级为解释/分类，正式评级从 `qe_factor_official_ratings.official_grade` 读取。
- `backend/services/quantevolver/qe_evolution_agents.py`、`backend/services/quantevolver/multi_alpha_selector.py`、`backend/services/quantevolver/portfolio_architect.py` 已大量改为读取 `official_grade` / `official_score`，旧 `classification.grade` 作为权威评级的风险明显下降。
- 前端 QE compose、factor deletion、correlation detail、MultiAlphaGroupEditor 等页面已出现 `official_grade` 字段替代旧 `grade` 的改动；但仍有 `.bak` 和少量 legacy 字段需要清理或兼容标注。

## 4. 仍需最高优先级跟进的问题（P0）

### P0-1 本地/WSL/用户路径硬编码仍大量存在

风险：运行环境绑定到本机路径，远程节点、CI、其他开发机或生产部署会失败；也会使文档、脚本、迁移混杂真实机器路径。

当前证据：

- `configs/execution_algos/v25_two_stage.yaml:4`
- `configs/execution_algos/v25_two_stage.yaml:5`
- `backend/init_catalog_db.py:1050`
- `backend/db/migrations/add_v25_execution_algo.sql:27`
- `backend/infra/wsl_qlib_runner.py:11`
- `backend/infra/wsl_qlib_runner.py:17`
- `backend/routers/quantevolver.py:3382`
- `scripts/add_v25_to_catalog.py:11`
- `scripts/add_v25_to_db.py:15`
- `scripts/batch_develop_factors_v2.py:19`
- `scripts/optimize_timeout_factors.py:277`
- `scripts/hmm_dynamic_offline_experiments.py:790`

建议：

- 运行时代码只允许从配置中心、环境变量、StrategyPackage asset registry 或 compute node 配置读取路径。
- 研究/诊断脚本如必须保留本机路径，应移动到 `scripts/diagnostics/` 或 `scripts/legacy/` 并加显式“仅本机诊断，不参与生产运行”说明。
- 对 `F:/Dev`、`C:/Users`、`/mnt/f`、`/home/lc999`、`rdagent-gpu` 增加静态扫描守卫，运行时代码出现时阻断。

### P0-2 V24/V25 模型资产仍有硬编码入口

风险：Paper v2 / QE 运行可能依赖本机 `/home/lc999/data/rl_models/...`，资产不可迁移、不可追踪、不可校验。

当前证据：

- `configs/execution_algos/v25_two_stage.yaml:4`
- `configs/execution_algos/v25_two_stage.yaml:5`
- `backend/init_catalog_db.py:1050`
- `backend/db/migrations/add_v25_execution_algo.sql:27`
- `scripts/v25_verify.py:24`
- `scripts/v25_minute_test.py:37`
- `scripts/verify_v25_minute_execution.py:34`
- `scripts/v24_mini_backtest.py:71`

建议：

- 统一通过 `backend/services/strategy_package/model_asset_resolver.py` 或 asset registry 解析模型文件。
- catalog/migration 中只保留资产 logical id / manifest hash，不写本机绝对路径。
- V24/V25 临时验证脚本归入 `scripts/diagnostics/v25/` 或归档，避免和产品入口混淆。

### P0-3 legacy 指标脚本仍能直接写权威指标表

风险：`aistock_factor_metrics` / `aistock_factor_monthly_ic` / `qe_factor_classification` 被非统一入口写入，破坏“独立指标单写入”设计，导致 UI、LLM、QE 读取到不一致指标。

当前高风险候选：

- `backend/scripts/batch_factor_metrics_sync.py:32`
- `backend/scripts/restore_task_metrics.py:57`
- `scripts/batch_develop_factors_v2.py:987`
- `scripts/batch_develop_factors_v2.py:992`
- `scripts/optimize_timeout_factors.py:369`
- `scripts/optimize_timeout_factors.py:374`
- `scripts/backfill_monthly_ic_v2.py:132`
- `scripts/sync_classification_ic_mirror.py:44`
- `scripts/p1c_batch_update_holding_period_class.py:68`
- `scripts/clear_ratings_for_v2.py:61`
- `scripts/_factor_cleanup_execute.py:78`
- `scripts/_factor_cluster_compute.py:204`

建议：

- `aistock_factor_metrics` 正式写入口只保留 `backend/services/quantevolver/factor_official_evaluation_service.py`。
- `qe_factor_official_ratings` 正式评级写入口只保留 `backend/services/quantevolver/factor_rating_service.py`。
- 其他脚本改为 diagnostic-only、dry-run、或调用正式 service/API；默认禁止直接 DELETE/INSERT/UPDATE 权威表。
- 对脚本目录增加静态检测：凡出现 `INSERT INTO aistock_factor_metrics`、`DELETE FROM aistock_factor_metrics`、`UPDATE qe_factor_classification` 等，需要白名单。

### P0-4 `.codex_tmp`、临时补丁、测试资产未忽略，污染工作区

风险：临时探针、补丁、模型 `.pt`、CSV/MD 报告、启动脚本进入 git status，后续提交容易误带入仓库。

当前证据：

- `.codex_tmp/`
- `.claude_tmp_patch_factor_cache.py`
- `backend/tests/.tmp_model_asset_resolver/`
- `scripts/qrun_limit_minute.py.backup`

建议：

- `.codex_tmp/`、`.claude_tmp_*`、`backend/tests/.tmp_model_asset_resolver/`、`*.backup` 加入 `.gitignore`。
- 已生成但有价值的报告移动到 `docs/analysis/`；无价值探针归档或删除。
- 测试生成模型/缓存必须使用 pytest `tmp_path`，不能留在仓库路径。

### P0-5 后端/前端仍有 legacy 字段兼容残留，需要显式标注

风险：虽然正式评级链路已经改善，但部分代码仍用 legacy 字段别名或兼容字段，后续维护者容易误认为旧字段仍是权威来源。

当前证据：

- `backend/services/quantevolver/portfolio_architect.py:354` 将 `m.ic_mean AS ic_value` 作为内部输出别名。
- `backend/services/quantevolver/factor_analyst.py:2092` 仍在 upsert `qe_factor_classification.ic_value/sharpe_value/ann_ret_value` 等 legacy 字段。
- `backend/services/quantevolver/deletion_candidate_service.py` 仍对前端兼容 `v2_grade` 类字段。
- `frontend/src/app/quantevolver/compose/page.tsx.bak` 仍包含旧 `ic_value` / `ann_ret_value` 显示逻辑。

建议：

- legacy 字段保留时统一命名为 `legacy_*` 或在代码注释中标注“只读兼容，不作为权威评级”。
- 删除或归档 `.bak` 页面，避免扫描和维护误判。
- 前端展示层只显示 `official_grade`、`official_score`、`ind_*`；旧字段只放 debug 区。

## 5. 脚本/文件分类与处理建议

### A. 必须保留：产品运行时直接引用脚本

这些文件被后端/前端运行时代码直接引用，不能作为无用脚本清理：

- `scripts/backfill_factor_cache.py`
- `scripts/download_anns_pdf.py`
- `scripts/generate_stock_pool.py`
- `scripts/hmm_train_script.py`
- `scripts/precompute_hmm_coefficients.py`
- `scripts/qrun_limit.py`
- `scripts/qrun_limit_minute.py`
- `scripts/qe_suspend_filter.py`
- `scripts/qe_suspend_filter_strategy.py`
- `scripts/qe_suspend_filter_score_weighted_strategy.py`
- `scripts/tail_twap_strategy.py`
- `scripts/tail_twap_v24_strategy.py`
- `scripts/tail_twap_v25_strategy.py`
- `scripts/close_execution_strategy.py`
- `scripts/strategy_package_live_inference.py`

建议：

- 保留在 `scripts/` 或迁移为更清晰的 runtime helper 目录，但迁移必须同步修改引用。
- 对这些脚本补充入口说明、依赖环境、输入输出、失败策略。
- 特别检查其中是否还有本机路径默认值。

### B. 建议保留但迁移：研究/实验脚本

这些脚本可能仍有研究价值，但不应和产品运行脚本混在同一层级：

- `scripts/hmm_dynamic_offline_experiments.py`
- `scripts/hmm_dynamic_tuning_experiments.py`
- `scripts/hmm_dynamic_tuning_pass3_experiments.py`
- `scripts/hmm_horizon_v2_train.py`
- `scripts/hmm_horizon_v2_compare.py`
- `scripts/train_and_register_hmm.py`
- `scripts/v24_mini_backtest.py`
- `scripts/v25_mini_backtest.py`
- `scripts/v25_verify.py`
- `scripts/v25_verify_final.py`
- `scripts/verify_v25_integration.py`
- `scripts/verify_v25_minute_execution.py`
- `scripts/v24_v25_test.py`
- `scripts/v24_v25_real_test.py`
- `scripts/v25_minute_test.py`
- `scripts/v25_minute_test_final.py`

建议：

- 迁移到 `research/`、`scripts/research/` 或 `scripts/diagnostics/hmm_v25/`。
- 对已经形成结论的实验，保留 `docs/analysis/*.md`，脚本只保留可复现实验入口。
- 如果脚本依赖固定模型路径、固定 qlib 路径，应改为参数化或标记本机限定。

### C. 建议归档或删除：一次性 patch/fix 脚本

这些脚本大多是一次性修复、代码替换、页面补丁、DB 修复入口，继续留在根目录或 scripts 根目录会误导维护者：

- `.claude_tmp_patch_factor_cache.py`
- `fix_db.py`
- `fix_pool_error.py`
- `fix_template_service.py`
- `replace_script.py`
- `modify_strategy.py`
- `debug_pkl.py`
- `scripts/patch_evolution_page.py`
- `scripts/patch_evolution_page_v2.py`
- `scripts/patch_train_oracle_gap.py`
- `scripts/patch_v10_oracle_gap.py`
- `scripts/patch_hmm_workspace.py`
- `scripts/backup_and_replace_all_txt.py`
- `scripts/_factor_cleanup_execute.py`
- `scripts/_factor_cleanup_rollback.py`

建议：

- 如修复已经进入正式代码，脚本删除或移入 `scripts/legacy/oneoff/`。
- 带 DELETE/UPDATE 的脚本默认不能放在 scripts 根目录。
- 对有回滚价值的脚本，只保留到文档或 migration 记录中。

### D. 建议整理：诊断/分析脚本

这些脚本数量最多，大多没有运行时引用，很多还带本机路径：

- `scripts/_inspect_*`
- `scripts/_scan_*`
- `scripts/_check_*`
- `scripts/diagnose_*`
- `scripts/verify_*`
- `scripts/compare_*`
- `scripts/analyze_*`
- `backend/scripts/diagnose_missing_factors.py`
- `backend/scripts/validate_data_service.py`
- `backend/scripts/compare_xtquant_tdx_realtime.py`

建议：

- 对仍有复用价值的，统一放到 `scripts/diagnostics/`，加 README 和参数说明。
- 对只为某次问题定位产生的，若结论已在 `docs/analysis/`，脚本可归档或删除。
- 对 `_inspect_*` / `_check_*` 这类临时 DB 探针，优先清理，避免长期污染。

### E. 正式测试应保留，脚本式测试应迁移

保留价值较高：

- `backend/tests/paper_trading_v2/*`
- `backend/tests/selection_center/*`
- `backend/tests/strategy_package/*`
- `backend/tests/trading_core/*`
- `backend/tests/test_factor_metrics_authority_static.py`
- `backend/tests/test_tushare_sync_engine.py`
- `backend/tests/test_hmm_rolling_training.py`

需要整理：

- `scripts/test_*.py`
- `scripts/test_*.sh`
- `scripts/*_test.py`
- 临时测试产物目录 `backend/tests/.tmp_model_asset_resolver/`

建议：

- 正式测试保留在 `backend/tests`、`tests`、`frontend/tests`，纳入 pytest/Playwright/nox。
- `scripts/test_*` 如果是产品回归测试，应迁入正式测试目录；如果只是人工验证，归入 diagnostics。
- 测试产生的模型、cache、日志必须进入临时目录并被忽略。

### F. 管理/迁移/回填脚本需分级

高价值但需保护的脚本：

- `scripts/create_*_table.py`
- `scripts/seed_dataset_refresh_audit.py`
- `scripts/create_suspend_d_table.py`
- `scripts/ingest_tushare_adj_factor.py`
- `backend/db/migrations/*`

高风险需改造或归档的脚本：

- `backend/scripts/batch_factor_metrics_sync.py`
- `backend/scripts/restore_task_metrics.py`
- `scripts/backfill_monthly_ic_v2.py`
- `scripts/sync_classification_ic_mirror.py`
- `scripts/p1c_batch_update_holding_period_class.py`
- `scripts/batch_develop_factors_v2.py`
- `scripts/optimize_timeout_factors.py`
- `scripts/migrate_factor_rating_v2.py`
- `scripts/clear_ratings_for_v2.py`

建议：

- DDL/迁移脚本进入 `backend/db/migrations` 或明确的 `scripts/admin/`。
- 回填脚本必须 dry-run 默认、显式确认、记录审计输出。
- 指标/评级类回填必须调用统一 service/API，不允许散落 SQL 写表。

## 6. 建议的清理优先级

### P0 立即处理

- `.codex_tmp/`、`.claude_tmp_*`、`backend/tests/.tmp_model_asset_resolver/`、`*.backup` 加入 ignore 或移出仓库候选区。
- 禁止非官方入口写 `aistock_factor_metrics`、`aistock_factor_monthly_ic`、`qe_factor_official_ratings`。
- 处理 V24/V25 模型路径硬编码，统一到 asset resolver / registry。
- 删除或归档 `frontend/src/app/quantevolver/compose/page.tsx.bak`。

### P1 分批整理

- 将 HMM/V25 实验脚本迁到 research/diagnostics 目录。
- 将 `_inspect_*`、`_check_*`、`diagnose_*`、`verify_*` 分为“保留复用”和“一次性归档”。
- 将 `scripts/test_*` 中的正式回归测试迁到 `backend/tests` 或 `frontend/tests`。
- 给运行时引用脚本补充 README、参数和失败策略。

### P2 长期维护

- 建立 `scripts/README.md`，说明 `runtime/`、`admin/`、`diagnostics/`、`research/`、`legacy/` 的边界。
- 在 nox 或静态检查中加入路径硬编码、权威表写入、`.bak/.backup` 文件检测。
- 对脚本增加 owner/status 字段：`runtime`、`admin`、`diagnostic`、`research`、`legacy`、`delete_candidate`。

## 7. 下次复查建议命令

```powershell
# 非忽略文件清单
git ls-files --cached --others --exclude-standard

# 查临时污染
git status -sb
rg -n "\.codex_tmp|\.claude_tmp|\.backup|\.bak" .

# 查本机路径硬编码
rg -n "F:/Dev|F:\\Dev|C:/Users|C:\\Users|/mnt/f|/home/lc999|rdagent-gpu" backend scripts configs frontend/src

# 查权威指标表直接写入
rg -n "INSERT INTO aistock_factor_metrics|DELETE FROM aistock_factor_metrics|UPDATE aistock_factor_metrics|INSERT INTO qe_factor_official_ratings|UPDATE qe_factor_classification|DELETE FROM qe_factor_classification" backend scripts

# 查旧评级/旧指标字段
rg -n "classification\.grade|v2_grade|ic_value|sharpe_value|ann_ret_value|compute_factor_metrics_unified" backend frontend/src scripts
```
