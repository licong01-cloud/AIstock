# Live Inference 运行时事件采集审计 (2026-05-10)

> 状态：审计文档，仅诊断，不改 `live_inference.py`。
> 范围：`backend/services/strategy_package/live_inference.py` 一文件
> D1 边界依据：drawer 0939d7d1720ed9d728630b5b — `live_inference.py` 是 Claude Code 工作面
> Created: 2026-05-10
> Author: impl-paper-v2

## §1 文件概览

- **路径**: `backend/services/strategy_package/live_inference.py`
- **行数**: 1835 行
- **大小**: 77.8 KB
- **主要类与函数**:
  - 数据类: `QEExperimentRuntimeSource`、`PreparedInferenceWorkspace`、`LiveInferenceResult`、`StaticLoaderFeatureResolution`、`FactorOrderResolution`、`LiveInferencePreflightCheck`、`LiveInferencePreflightResult`
  - 错误类: `LiveInferencePreflightError`（继承自 `StrategyPackageValidationError`）
  - 核心类:
    - `QEExperimentRuntimeAssetResolver` — 解析 QE 资产、执行 5 项冷启动 preflight、`prepare_workspace`、从 node API 物化 mlruns 工件、构建 `factor_order.json` / `manifest.json` / `strategy_package_factor_entry.py`
    - `LocalStrategyPackageInferenceProvider` — 本进程内调用 `backend.inference_engine.InferenceEngine().run_inference(...)`
    - `WslStrategyPackageInferenceProvider` — 通过 `wsl -d <distro>` 调用 `scripts/strategy_package_live_inference.py`，从 stdout JSON 文件读取 scores
- **当前调用链入口**: 上游 selection_artifact / service.py（在同目录）通过 `require_preflight_or_raise` + `prepare_workspace` + `*InferenceProvider.run` 三段式驱动；`run_inference` 的最终调用方是 Selection Center 的 `generate_from_live_inference`。
- **当前数据落表情况一句总结**: **零落表** — 整个文件没有任何 `INSERT` / `UPDATE` 语句，没有 `logger`/`logging` 导入，仅向上层返回 `LiveInferenceResult(scores, metadata)`；workspace 里写出的 `manifest.json`/`factor_order.json` 是磁盘缓存而非数据库工件。是否落 `paper_v2.*` 完全由调用方决定。

## §2 应采集但未落表的运行时事件（主表）

| 事件名 | 位置 cite | 当前流向 | 采集成本 | 建议落地位置 | DW 价值 |
|---|---|---|---|---|---|
| Preflight 5-check 聚合结果（passed + 每项 status/message/suggestion/context） | `live_inference.py:502-755`（`preflight_for_strategy_package`）+ `:139-173`（`to_dict`） | returned-not-persisted（`LiveInferencePreflightResult.to_dict()` 仅在失败时被 `LiveInferencePreflightError.context["preflight"]` 携带，PASS 全量结果完全丢弃） | moderate（新表 `paper_v2_live_inference_preflight`，1 行/checkpoint+5 行/check 或单行 JSONB） | 新表 `paper_v2_live_inference_preflight`（trade_date, package_id, manifest_sha256, passed, blocked_check, checks_jsonb, captured_at） | HIGH |
| Preflight check #1 `qe_source` 解析详情（experiment_id / qe_task_id / qe_loop_id / status） | `live_inference.py:559-600` | in-memory only（PASS 仅作为 `context` 字典挂在 `LiveInferencePreflightCheck` 上，未持久化） | trivial（落进上面的 checks_jsonb） | `paper_v2_live_inference_preflight.checks_jsonb` | HIGH |
| Preflight check #2 `qe_node` 解析（execution_node_id, asset_workspace_path） | `live_inference.py:602-630` | in-memory only | trivial | 同上 | HIGH |
| Preflight check #3/#4/#5 `conf_yaml` / `factor_source` / `model_params`（含 sampled_factors、factor_names_count、model_params_path、candidate_count） | `live_inference.py:632-753` | in-memory only | trivial | 同上 | HIGH |
| Materialize-from-node 下载列表（conf.yaml / factors/<N>.py / model.py / mlruns params.tar.gz） | `live_inference.py:1061-1156`（`_materialize_runtime_source_from_node` 内 `_download_workspace_file` 多次调用） | in-memory only（下载成功无任何留痕，失败仅抛 `DataUnavailableError`，无逐文件成功记录） | moderate（新表 `paper_v2_qe_asset_download_log`：experiment_id / qe_task_id / qe_loop_id / rel_path / bytes / latency_ms / outcome） | 新表 `paper_v2_qe_asset_download_log` | MEDIUM |
| Mlruns params 来源回退路径（`download_mlruns_params` 失败 → `_copy_cached_mlruns_params` 命中本地缓存） | `live_inference.py:1136-1156`（params_tar try/except 分支）+ `:1235-1271`（`_copy_cached_mlruns_params`） | in-memory only（fallback 命中静默成功，调用方完全看不见“今天用的是缓存而不是节点”） | trivial（在 manifest.diagnostics 加 `model_params_origin` 字段或独立列） | `paper_v2_strategy_package_runtime.model_params_origin`（"node_api" / "local_cache_fallback"） | HIGH |
| Mlruns params candidate_count（params.pkl 多份候选时取 mtime 最新者） | `live_inference.py:1534-1568`（`_resolve_model_params_path`） | returned-not-persisted（仅出现在 `manifest.json` 磁盘文件 + `LiveInferencePreflightCheck.context`，DB 无记录） | trivial（写入上面的新表/JSONB） | `paper_v2_live_inference_preflight.checks_jsonb.model_params.candidate_count` | MEDIUM |
| Factor order 解析来源（`qe_static_dataloader` / `qe_experiments.factor_names` / `*_after_missing_static_loader` / `*_after_empty_static_loader`） | `live_inference.py:1283-1358`（`_build_factor_order` → `dynamic_factor_source`）+ `:953-979`（写入 `factor_order.json`） | log-only（写到磁盘 `factor_order.json` 但不入 DB；`warnings` 列表仅磁盘） | trivial（落 `paper_v2_strategy_package_runtime.factor_order_source`） | `paper_v2_strategy_package_runtime.factor_order_source` + `factor_order_warnings_jsonb` | HIGH |
| `static_loader_unreadable_configs` / `missing_configs`（schema artifact 不可读 → 抛 `DataUnavailableError`） | `live_inference.py:1296-1320`、`:1407-1414`、`:1100-1113` | returned-not-persisted（仅出现在错误上下文，错误若被上层吞掉就丢失） | trivial（落 warnings_jsonb） | 同 factor_order_warnings_jsonb | MEDIUM |
| Factor 文件存在性 sample 检查（preflight 阶段抽 3 个 factor 检查 .py 是否存在） | `live_inference.py:683-721` | in-memory only | trivial（落 checks_jsonb） | `paper_v2_live_inference_preflight.checks_jsonb.factor_source` | LOW |
| 完整 factor 文件解析失败列表（`_resolve_factor_files` 抛错时的 `missing_factors`） | `live_inference.py:1514-1532` | returned-not-persisted（错误上下文）/ in-memory（成功路径丢失 factor_source_dir 与 factor 数量） | trivial | `paper_v2_strategy_package_runtime.factor_files_count` | MEDIUM |
| Workspace 准备清单（manifest.json 内的 diagnostics 块：qe_experiment_id / source_workspace_path / qe_task_id / qe_loop_id / execution_node_id / factor_source_dir / model_source_path / model_candidate_count） | `live_inference.py:1006-1023`（写盘）+ `:1025-1038`（返回 `PreparedInferenceWorkspace`） | log-only（仅落到 `workspace_path/manifest.json` 磁盘缓存，DB 无） | trivial（多数字段已可由 `paper_v2_strategy_package` 补列） | `paper_v2_strategy_package_runtime`（每次 prepare_workspace 一行：cache_key / model_candidate_count / dynamic_factor_source / model_source_path / execution_node_id_resolved / cached_or_node） | HIGH |
| 推理输入快照（trade_date、cutoff_date、workspace_path、`AISTOCK_STRICT_INFERENCE` 标志） | `live_inference.py:1690-1728`（Local provider）/ `:1749-1815`（WSL provider） | in-memory only（`InferenceEngine.run_inference` 的入参完全不持久化） | trivial（在 `paper_v2_inference_run` 表加 strict_inference + cutoff_date + workspace_cache_key） | `paper_v2_inference_run.input_snapshot_jsonb`（包含 trade_date/cutoff_date/strict_inference/backend/manifest_sha256） | HIGH |
| 推理 raw scores DataFrame（multiindex datetime/instrument → score）的统计形态（行数、score 分布、有效/无效行数） | `live_inference.py:373-428`（`_score_rows_from_frame` 内 `pd.notna+isfinite` 过滤前） | in-memory only（仅 `_score_rows_from_frame` 内部用一次，过滤后丢失原始统计） | moderate（需要在 `_score_rows_from_frame` 之前加采集，或让 caller 持久化原始 DataFrame 摘要） | 新表 `paper_v2_inference_score_summary`（trade_date, package_id, n_rows, score_min, score_max, score_mean, score_std, n_invalid, n_finite） | HIGH |
| 推理 ranked rows（symbol/score/rank 全表） | `live_inference.py:419-428`（`_score_rows_from_frame` 输出） | returned-not-persisted（作为 `LiveInferenceResult.scores` 返回，落表完全由 caller 决定，本文件不保证） | trivial（caller 已落 `paper_v2_selection`，但需补 raw score 列） | `paper_v2_selection.score`（确认 raw QE score 是否落表，非 normalized rank） | HIGH |
| 推理后端类型 metadata（`inference_backend` ∈ {local, wsl}, `wsl_distro`, `wsl_conda_env`） | `live_inference.py:1727`（local）/ `:1813-1814`（wsl） | returned-not-persisted（在 `LiveInferenceResult.metadata` 里返回，caller 是否落表未知） | trivial | `paper_v2_inference_run.backend` + `wsl_distro` + `wsl_conda_env` | MEDIUM |
| 推理延迟分解（preflight耗时 / materialize-from-node 耗时 / prepare_workspace 耗时 / model load+inference 耗时 / postprocess 耗时） | **整个文件都没有计时**（`subprocess.run` `:1778-1786` 仅有 `timeout` 上限，不记录实际耗时；其余函数无 `time.perf_counter`） | **完全未采集** | moderate（需要在 `preflight_for_strategy_package` / `_materialize_runtime_source_from_node` / `prepare_workspace` / `*InferenceProvider.run` 入口出口加 `perf_counter`，约 5-8 个埋点） | `paper_v2_inference_run.timing_jsonb`（preflight_ms, materialize_ms, prepare_ms, inference_ms, postprocess_ms, total_ms） | HIGH |
| WSL 子进程退出信号（returncode / stdout_tail / stderr_tail） | `live_inference.py:1778-1805` | returned-not-persisted（仅在 returncode != 0 时通过 `DataUnavailableError.context` 携带，成功路径完全丢弃 stdout/stderr） | trivial（成功路径也截一段 stderr_tail 落表） | `paper_v2_inference_run.wsl_returncode` + `wsl_stderr_tail`（截尾 4KB） | MEDIUM |
| WSL inference output JSON 的额外 metadata（来自 `payload["metadata"]`，可能包含 model_id、qlib provider URI、git sha 等） | `live_inference.py:1806-1815` | returned-not-persisted（与 backend 标记合并后透传给 caller） | trivial | `paper_v2_inference_run.subprocess_metadata_jsonb` | HIGH |
| Typed errors（`DataUnavailableError` / `StrategyPackageValidationError` / `LiveInferencePreflightError` / `QEWorkspaceFileNotFound`）的 error_code + context | 散布全文：`:178-184`（`LiveInferencePreflightError.error_code = "LIVE_INFERENCE_PREFLIGHT_FAILED"`），其余从 `trading_core.errors` 与 `qe_workspace_client` 引入 | in-memory only（异常向上抛，能否落表完全看 caller 是否在 try/except 里写 DB） | moderate（需要在 caller 加 `paper_v2_inference_error` 表 + 装饰器，本文件单独无法采集） | 新表 `paper_v2_inference_error`（trade_date, package_id, error_code, error_class, message, context_jsonb, stage） | HIGH |
| Workspace 缓存命中/未命中（`_reset_cache_dir` 每次都 rmtree 并重建，意味着 prepare_workspace 永远是冷启动） | `live_inference.py:1177-1187`（`_reset_cache_dir`） | **未采集且无缓存语义**（实际行为是“每次重建”，不是 hit/miss；这点本身值得落表给 DW） | trivial | `paper_v2_strategy_package_runtime.cache_was_reset`（永远 true，但落表后 DW 可看出 prepare_workspace 调用频度） | LOW |
| `_copy_cached_mlruns_params` 候选搜索结果（扫描 `cache_root/*/*/manifest.json` 找匹配 experiment_id 的 params.pkl 候选数） | `live_inference.py:1235-1271` | in-memory only（`candidates` 列表在函数返回后丢弃，仅 bool 结果保留） | trivial | `paper_v2_strategy_package_runtime.fallback_candidate_count` | LOW |
| Calibration / drift 指标 | **整个文件不计算任何漂移指标**（无 PSI / KS / 历史 score 分布对比） | not_computed | blocking（需新增 inference 历史 store + 在线对比作业） | 独立 ML 监控管道（不在 paper_v2 范畴） | MEDIUM |

## §3 按采集成本分组

### 3.1 trivial — 立即可补（仅需在已经写盘的 `manifest.json` / `factor_order.json` 数据基础上加 1 张 PG 表 + 由 caller 调用一次 INSERT）

- Preflight check #1~#5 全部 `context` 字段（事件 #2~#4、#9）
- `model_params_origin`（node_api vs local_cache_fallback，事件 #6）
- `factor_order_source` + `warnings`（事件 #8、#9）
- `_resolve_factor_files` 成功路径的 factor 数量（事件 #11）
- `manifest.json` diagnostics 块全字段映射到 PG（事件 #12）
- 推理输入快照 + backend metadata（事件 #13、#16）
- WSL stdout/stderr 截尾（事件 #18）
- WSL subprocess metadata 透传（事件 #19）
- Cache 重建标志 + fallback 候选数（事件 #21、#22）
- Preflight 主结果落表（事件 #1，建表是 moderate；填字段是 trivial）

### 3.2 moderate — 需 schema / 新表 / 修改流程

- 新表 `paper_v2_live_inference_preflight`（事件 #1）
- 新表 `paper_v2_qe_asset_download_log`（事件 #5，需在 `_download_workspace_file` 加 hook）
- 新表 `paper_v2_inference_score_summary`（事件 #14，需在 `_score_rows_from_frame` 过滤前采集）
- 推理延迟埋点（事件 #17，需在多处加 `perf_counter`）
- 新表 `paper_v2_inference_error`（事件 #20，需 caller 配合）

### 3.3 blocking — 需跨系统协调

- Calibration / drift 指标（事件 #23，需要历史 score store + 调度器，独立于 paper_v2 范畴）
- 真正的 model_id 持久化追溯：本文件层面只能拿到 `params.pkl` 路径与 `model_candidate_count`，无法独立给出 model_id（需要 QE node API 在 `download_mlruns_params` 时一并返回 run_id / model_uri，或在 `_resolve_model_params_path` 后调用 mlruns 反解；属于跨系统）

## §4 与 A2 capture gaps 的关系

> A2 / A1 / B1 文档与本文档同批次产出，写本文时尚未着陆。预期对照点（待 A2 落地后由作者交叉引用）：

- **A2 BLOCKING 已覆盖（预期）**: Preflight 主结果落表、推理 raw scores 落表、typed error 落表 — 这三项 A2 应已列入新表设计，本文档只补充 `live_inference.py` 内部的字段细节而不是要求另起新表。
- **`live_inference.py` 独有，A2 可能漏掉的项**:
  - 事件 #6 `model_params_origin`（node_api vs local_cache_fallback）— 这是 `_copy_cached_mlruns_params` 静默 fallback 的关键审计字段，A2 若只看 `paper_v2_selection` / `paper_v2_inference_run` 上层难以发现。
  - 事件 #8 `factor_order_source` 4 种取值 + warnings — A2 看不到 `_build_factor_order` 内部的 schema 回退路径。
  - 事件 #14 推理 raw scores 的统计形态（mean/std/n_invalid）— 这是落 `paper_v2_selection` 之前的中间态，A2 通常只采终态。
  - 事件 #17 推理延迟分解（5 段 perf_counter）— 全文件无任何计时，A2 在外层只能拿到端到端总耗时。
- **建议**: A2 / B1 落地后引用本文档 §2 第 #6/#8/#14/#17 行作为补强字段；如未覆盖请在下一轮 paper_v2 schema 演进中补上。
- **交叉引用占位**: 待 A2 (`docs/analysis/A2_capture_gaps_*.md`) 与 B1 (`docs/analysis/B1_*.md`) 着陆后回填具体路径与小节号。

## §5 审计结论

1. **可观测性评级**: **D（差）** — `live_inference.py` 是整条 paper_v2 推理链上可观测性最薄弱的一环：**无 logger 导入、无计时埋点、无 DB 写入，所有运行时事件要么 in-memory、要么仅磁盘 manifest.json、要么靠 caller 接住返回值**。一旦 caller 不落表（或日志被截断），昨天到底用了哪个 model_id、哪份 params.pkl（node 还是本地缓存）、preflight 五项各是什么 status，事后无法重建。
2. **最关键补齐项 #1（HIGH）**: **Preflight 5-check 完整结果必须落 `paper_v2_live_inference_preflight`**（事件 #1+#9），含 PASS 路径。当前 PASS 路径完整结果在 `LiveInferencePreflightResult` 返回后立即被 GC，30+ 历史冷启动失败的根因复盘只剩异常 message。
3. **最关键补齐项 #2（HIGH）**: **`model_params_origin` 字段必须落表**（事件 #6）— `_copy_cached_mlruns_params` 是 `download_mlruns_params` 抛错时的静默 fallback，今天用的是远端 node 还是本地缓存的 params.pkl，**目前从 DB 完全看不出来**，这是模型可重现性的硬缺口。
4. 次关键：**推理延迟 5 段埋点**（事件 #17）与 **factor_order_source**（事件 #8）—— 都是 trivial 成本，DW 价值 HIGH，建议同批补齐。
5. **不在本文件解决的 blocking 项**: 真正的 model_id（mlflow run_id）追溯需要跨系统改动 QE node API；calibration/drift 监控属于独立管道。
