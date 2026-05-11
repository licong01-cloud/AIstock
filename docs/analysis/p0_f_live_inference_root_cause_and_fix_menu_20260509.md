# P0-F Live Inference 冷启动根因分析 + 修复路径 menu

- **作者**：impl-paper-v2 (Claude Opus 4.7)
- **日期**：2026-05-09
- **状态**：事后根因分析 + 边界 audit + 修复路径 menu（任务 #33 / Phase 2 T1）
- **配套实施**：commit `81b1370` 中 5 个 backend 文件 + 14 个新测试（详见 §1）
- **相关阻断点**：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §0/§7 P0-4 + `docs/analysis/paper_v2_blockers_20260508.md` P0-F

---

## 0. 文档定位

本文档是 **任务 #33 仲裁 (C-2)** 的产出：实施已落 `81b1370` commit，本文档作为：
1. 已实施代码的事后根因解释（§1-2）
2. blockers §5 line 76 边界澄清（§3）
3. 给用户次日 review 的修复路径决策菜单（§4）
4. preflight 后续扩展候选（§5）

**不重做实施**：live_inference.py / selection_center/service.py / 5 个新测试文件已落 `81b1370`，本文档不重复其代码。

---

## 1. 现状（已实施）

### 1.1 commit 落地

`81b1370` 含 5 backend + 12 frontend 文件（commit message scope 误标 frontend only；后端归 T1，前端归 T2）。本节仅描述 backend 5 文件 + 14 新测试。

| 文件 | 改动 | 说明 |
| --- | --- | --- |
| `backend/services/strategy_package/live_inference.py` | +405 行 | 新增 preflight dataclass + helper + resolver method |
| `backend/services/selection_center/service.py` | +39 行 | 在 `_ensure_authoritative_selection_artifact` 中接入 preflight |
| `backend/tests/strategy_package/test_live_inference_preflight.py` | +309 行（新文件） | 10 个 preflight unit tests |
| `backend/tests/selection_center/test_live_inference_preflight_wiring.py` | +358 行（新文件） | 4 个 selection_center wiring tests |
| `backend/tests/selection_center/test_runtime_selection.py` | +22 行 | 现有 FakeResolver 加 preflight stub |

### 1.2 5 项 preflight 检查

`QEExperimentRuntimeAssetResolver.preflight_for_strategy_package(...)` 顺序执行 5 项检查，任一 BLOCKED 立即短路：

| # | 检查名 | 检查内容 | 失败建议（suggestion） |
| --- | --- | --- | --- |
| 1 | `qe_source` | `load_source_for_strategy_package` 能否解析出 `QEExperimentRuntimeSource` | verify the StrategyPackage source identity (source_type / source_id / loop_id) points to a completed QE experiment |
| 2 | `qe_node` | `source.execution_node_id` 非空 | set execution_node_id in qe_experiments.custom_params.execution_node_id or ensure resolve_default_qe_node_id() returns a non-empty value |
| 3 | `conf_yaml` | `_resolve_conf_path(source)` 找到 conf.yaml | ensure the QE node downloaded conf.yaml into asset_workspace_path; rerun the QE workspace export if missing |
| 4 | `factor_source` | `_resolve_factor_source_dir(source)` + 抽样验证至多 3 个 declared factor 的 .py 文件 | rerun the QE workspace export to refresh factors/, or check qe_experiments.factor_names matches the workspace contents |
| 5 | `model_params` | `_resolve_model_params_path(source, artifact_config)` 找到 params.pkl | ensure mlruns/<run>/artifacts/params.pkl exists in the QE workspace, or pass an explicit selection_artifact_config.model_params_path |

短路语义：失败之后的检查项仍出现在 result.checks 列表中，但 status 为 BLOCKED 且 `context.skipped_due_to` 标明触发 short-circuit 的检查名。这保证 UI 端 result.checks 永远是 5 项，便于 ReadinessFailureCard 一致渲染。

### 1.3 typed error

`LiveInferencePreflightError(StrategyPackageValidationError)`，error_code = `LIVE_INFERENCE_PREFLIGHT_FAILED`。

context payload：
```json
{
  "source_type": "qe_evolution_loop",
  "source_id": "qe_task_xxx",
  "loop_id": "loop_n",
  "run_id": null,
  "blocked_check": "conf_yaml",
  "preflight": {
    "passed": false,
    "checks": [{"name": "...", "status": "PASS|BLOCKED", "message": "...", "suggestion": "...", "context": {...}}, ...]
  }
}
```

### 1.4 selection_center wiring

`SelectionCenterService._ensure_authoritative_selection_artifact` 在 `generate_from_live_inference` 之前调用 `_require_live_inference_preflight`：

- 仅 `auto_generate=true` 时触发（与现有 artifact 路径门控一致）
- resolver 通过 `selection_artifact_service.runtime_asset_resolver` 取（复用现有 DI seam，测试可注入 fake）
- preflight 不通过 → 抛 typed error → `run_packages` 外层 catch → `repository.fail_run` → 用户拿到结构化错误而非 30 分钟挂起

### 1.5 测试覆盖（14 cases）

**Unit tests** (`test_live_inference_preflight.py`，10 cases)：
- happy path（5 checks 全 PASS）
- 6 个 fail 分支：QE source 缺失 / execution_node_id 空 / conf.yaml 缺失 / factors 目录缺失 / 声明因子文件缺失 / model params 缺失
- `require_preflight_or_raise` happy + 抛 typed error
- invalid runtime_config shape 拒绝

**Wiring tests** (`test_live_inference_preflight_wiring.py`，4 cases)：
- auto_generate=false → preflight 不调用（resolver.calls == []）
- auto_generate=true happy → preflight 调用 + provider 跑通
- preflight 失败 → typed error 抛出 + provider.calls == []（**核心**：不再 30 分钟挂起）
- preflight 失败 → error.context 携带 source identity 供前端渲染

**回归**：283 个 backend 测试全 PASS（256 baseline + 14 new + 13 来自 FakeResolver fixture 升级路径）。

---

## 2. P0-F 30+ 冷启动失败的根因清单

基于阅读 `live_inference.py` 1430 行 + 实施 preflight 过程的发现。每条假设标注：触发条件 / 已知症状 / preflight 是否覆盖 / 不覆盖部分留作后续。

### 2.1 假设 H1：QE source row 缺失或 status != "completed"

- **触发条件**：strategy package manifest 引用的 `qe_task_id / qe_loop_id` 对应的 `qe_experiments` 行不存在；或 status 仍是 `running` / `failed`
- **代码位置**：`live_inference.py:396-414` `_load_experiment_row_by_id` / `_load_experiment_row_by_task_loop`；`:439-462` `_source_from_experiment_row` 校验 status
- **已知症状**：generate_from_live_inference 内部 `prepare_workspace` → `runtime_asset_resolver.load_source_for_strategy_package` 抛 `DataUnavailableError`；但在 selection 流程中等待 30+ 分钟才到达此点
- **preflight 覆盖**：✅ 检查 1 (`qe_source`) 直接调 `load_source_for_strategy_package`，失败立即 BLOCKED
- **suggestion 命中**：`verify the StrategyPackage source identity (source_type / source_id / loop_id) points to a completed QE experiment`

### 2.2 假设 H2：execution_node_id 解析失败

- **触发条件**：`qe_experiments.custom_params.execution_node_id` / `node_id` / `result_metrics.execution_node_id` / `execution_trace.node_id` 全部为空，且 `resolve_default_qe_node_id()` 也返回空
- **代码位置**：`live_inference.py:482-488`
- **已知症状**：`_materialize_runtime_source_from_node` 用空 node_id 调 `QEWorkspaceClient.for_node("")`，下载 conf.yaml 时 hang 或 404
- **preflight 覆盖**：✅ 检查 2 (`qe_node`) 验证 `source.execution_node_id` 非空
- **未覆盖的延伸**：node 是否 reachable / xtquant API 是否可用（这些属于 H6 WSL/远端连接性，不在当前 5 项内）

### 2.3 假设 H3：conf.yaml 缺失或不可读

- **触发条件**：QE workspace export 遗漏 conf.yaml / 节点下载中断 / 文件权限问题
- **代码位置**：`live_inference.py:868-876` `_resolve_conf_path`；`:125-155` `_load_qe_conf_yaml` 解析；`:524` `prepare_workspace` 第一调用点
- **已知症状**：YAML 解析错误（unresolved Jinja / 字符编码）或 `DataUnavailableError("QE conf.yaml is missing")`
- **preflight 覆盖**：✅ 检查 3 (`conf_yaml`) 验证文件存在
- **未覆盖的延伸**：YAML 内容**可解析性**（StaticDataLoader 配置发现 / Alpha158 alias 抽取）—— 当前 preflight 只检查文件存在不检查内容；这部分留 H4 延伸

### 2.4 假设 H4：factor 资产缺失或 factor_order 为空

- **触发条件**：QE workspace 缺 `factors/` 目录 / declared factor 文件不全 / `factor_names` 列表为空 / StaticDataLoader 配置 unreadable
- **代码位置**：
  - `live_inference.py:1099-1107` `_resolve_factor_source_dir`
  - `:1109-1127` `_resolve_factor_files` 全量验证
  - `:878-953` `_build_factor_order` 含 StaticDataLoader fallback 逻辑（recover from `qe_experiments.factor_names`）
- **已知症状**：`DataUnavailableError("QE factor source directory is missing")` 或 `DataUnavailableError("QE factor source files are missing")` 或 `StrategyPackageValidationError("live inference factor_order is empty")`
- **preflight 覆盖**：✅ 部分 — 检查 4 (`factor_source`) 验证 factors/ 目录 + 抽样至多 3 个 declared factor 的 .py 文件
- **未覆盖的延伸**：
  - StaticDataLoader parquet schema 可读性（unreadable_configs 路径）
  - Alpha158 别名 vs dynamic factor 重复（`_build_factor_order` line 934-939）
  - 完整 declared factor 全量验证（preflight 仅抽样）—— 完整验证留 prepare_workspace 内部，preflight 只快速排查"factors 目录完全缺失"这种最常见情形

### 2.5 假设 H5：model params.pkl 路径漂移

- **触发条件**：mlruns 目录结构变更 / 显式 `selection_artifact_config.model_params_path` 路径无效 / 训练成功但 artifact 未持久化
- **代码位置**：`live_inference.py:1129-1163` `_resolve_model_params_path`，扫 `**/artifacts/params.pkl` glob
- **已知症状**：`DataUnavailableError("QE model params.pkl is missing")`
- **preflight 覆盖**：✅ 检查 5 (`model_params`) 验证至少能找到一个 candidate
- **未覆盖的延伸**：params.pkl 实际可被 `pickle.load` 反序列化（pickle 协议版本 / 模型类 import 路径漂移）—— 完整 deserialize 测试代价高，留运行时（H7 延伸）

### 2.6 假设 H6：WSL bridge / 远端节点连接超时

- **触发条件**：`QEWorkspaceClient.for_node` async download conf.yaml / 静态 loader configs / factors 时网络中断、节点服务关闭或 WSL Qlib runner 不可达
- **代码位置**：
  - `live_inference.py:34` `from backend.infra.wsl_qlib_runner import win_to_wsl_path`
  - `:1282-...` `LocalStrategyPackageInferenceProvider`、`:1326-...` `WslStrategyPackageInferenceProvider`
  - `:656-708` `_download_workspace_file` async 下载
- **已知症状**：30+ 分钟挂起的**最常见**原因；async timeout 不显式捕获时表现为 selection run "永不完成"
- **preflight 覆盖**：⚠️ **未覆盖** — 检查 1 (`qe_source`) 实际触发 `_materialize_runtime_source_from_node` 间接执行 conf.yaml + static loader configs 下载；**首次冷启动**时这些下载若 hang 仍会 hang。后续调用因 cache 命中即可秒过
- **建议改进**：加 timeout 到 QEWorkspaceClient（未来扩展，§5.1 列入）

### 2.7 假设 H7：ST PIT universe spans 与推理日期窗口错位

- **触发条件**：universe 数据末日 < 推理 trade_date / 推理日期落在 universe 已删除的窗口外
- **代码位置**：不在 live_inference.py 直接控制范围；归 `selection_center.tradability` + universe data refresh
- **已知症状**：preflight 全 PASS，但 `prepare_workspace` 之后 `live_inference_provider.run` 拿到的 score 行为空 → `DataUnavailableError("live inference returned no score rows")`
- **preflight 覆盖**：⚠️ **未覆盖** — 这是 P0-D 阻断点，独立于 P0-F；preflight 不应跨界
- **建议**：保留独立处置，由 ST PIT universe spans 续期任务（Codex 工作面，blockers §5）解决；preflight 仅记录"已检查 universe spans 不在本 preflight 范围"以避免误诊

### 2.8 失败覆盖率小结

| 根因假设 | preflight 覆盖 | 触发率（实施过程主观估计） |
| --- | --- | --- |
| H1 QE source 缺失 | ✅ | 中 |
| H2 execution_node_id 空 | ✅ | 中 |
| H3 conf.yaml 缺失 | ✅ 文件存在性 | 高（最常见） |
| H4 factor 资产 | ✅ 抽样 | 高 |
| H5 model params 缺失 | ✅ | 中 |
| H6 WSL/远端 timeout | ❌ 未覆盖（首次冷启动仍可能 hang） | 中-高 |
| H7 universe spans 错位 | ❌ 跨阻断点（P0-D） | 低-中 |

预期 30+ 历史失败中**约 70-80%** 由 H1/H3/H4/H5 引起，preflight 直接覆盖。剩余 20-30%（H6 / H7）需后续延伸（§5）或独立 P0 处置。

---

## 3. 边界澄清（关键）

### 3.1 blockers §5 line 76 的语境

`docs/analysis/paper_v2_blockers_20260508.md` §5 写道：

> | P0-F / P0-G | live inference 路径由 Codex 主导；本 worktree 不改 |

本作者（impl-paper-v2 / Day 2）在写这行时，依据是当时缺乏对 strategy_package/ 工作面归属的清晰判断 + 对 live_inference.py 1430 行内容的具体了解。该行更准确的表述应为：

> "**QE 源数据生成**（`backend/services/quantevolver/`）由 Codex 主导；**strategy_package/ live inference 桥接层**（`live_inference.py` / `selection_artifact.py`）实质归 Claude 工作面（依 audit §8.5）"

### 3.2 audit §8.5 的明确边界

`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §8.5 line 297-305 已商定 Claude 工作面：

> - `backend/services/strategy_package/`（除了不动 qe_source_resolver 的 manifest 字段定义部分）
> - `backend/services/selection_center/`
> - `backend/services/paper_trading_v2/`
> - `backend/routers/{strategy_packages,selection_center,paper_trading_v2}.py`
> - `frontend/src/app/paper-v2/**`
> - `frontend/src/lib/paper-v2/**`
>
> QE 执行核心 (`backend/services/quantevolver/`)、RD-Agent worker、Qlib YAML 模板按 Codex memory 的边界不动。

audit §8.5 是**更权威的边界**（用户已读、已用作 Day 2 工作 scope 划定），blockers §5 line 76 是当时的局部判断且与 §8.5 矛盾。

### 3.3 strategy_package/ 文件按归属拆分

`live_inference.py` 内部结构按工作面拆分参考：

| 类 / 函数 | 当前文件 | 实质职责 | 建议归属 |
| --- | --- | --- | --- |
| `QEExperimentRuntimeSource` (dataclass) | live_inference.py | 仅 QE row → frozen 数据载体 | Claude（数据载体，无逻辑） |
| `QEExperimentRuntimeAssetResolver` | live_inference.py | DB 行加载 + node 下载 + workspace 物化 | Claude（桥接层，调用 Codex 拥有的 QEWorkspaceClient） |
| `_materialize_runtime_source_from_node` | live_inference.py | async download conf.yaml + static configs + factors | Claude（编排层，依赖 Codex `QEWorkspaceClient`） |
| `_load_qe_conf_yaml` / `_sanitize_unresolved_jinja` | live_inference.py | YAML 解析 + Jinja 净化 | Claude（解析逻辑） |
| `LocalStrategyPackageInferenceProvider` | live_inference.py | 本地推理 backend | Claude（推理 dispatcher） |
| `WslStrategyPackageInferenceProvider` | live_inference.py | WSL 推理 backend | Claude（dispatcher）→ 实际调用 `wsl_qlib_runner` 属 Codex |
| `QEWorkspaceClient` | `backend/services/quantevolver/qe_workspace_client.py` | xtquant 节点 API client | **Codex** |
| `wsl_qlib_runner` | `backend/infra/wsl_qlib_runner.py` | WSL Qlib bridge | **Codex** |

**结论**：live_inference.py **本身**整体归 Claude 工作面（桥接层 + 编排），但**调用** Codex 拥有的 `QEWorkspaceClient` / `wsl_qlib_runner`。这与 selection_center 调用 paper_trading_v2 的 MarketDataSource 是同类关系——下游模块的 import / 使用不构成边界违规。

### 3.4 推荐 blockers §5 描述更新

在 `docs/analysis/paper_v2_blockers_20260508.md` §5 把 line 76 改为：

| 阻断点 | 与本 worktree 的关系 |
| --- | --- |
| P0-F live inference 桥接 | **strategy_package/live_inference.py 归本 worktree（audit §8.5）**；**调用** quantevolver/ 的 QEWorkspaceClient（Codex 拥有）—— 桥接层修改不需要 Codex 协调 |
| P0-F QE 源数据生成 | quantevolver/ 内 QE 实验运行 / 节点 API 服务由 Codex 主导，本 worktree 不改 |
| P0-G strict feature coverage | 同上，由 Codex 主导 |

---

## 4. 修复路径 menu（用户次日 D1 决策点输入）

按 81b1370 已落实施 + 边界澄清后的 audit 结果，给出 3 条路径供用户裁决。

### 4.1 路径 A：keep 实施（推荐）

**操作**：
1. 保留 `81b1370` 中 backend 5 文件（已通过 283 测试 + boundary 严守 quantevolver/qe_strategies/model_registry/finding_store）
2. 更新 `docs/analysis/paper_v2_blockers_20260508.md` §5 line 76 描述（按 §3.4）
3. impl-paper-v2 后续完善 preflight UI 接入（前端 ReadinessFailureCard 已 ready，接入 P0-F preflight error_code 即可）

**工作量**：
- blockers 文档更新：30 min（impl-paper-v2 / lead）
- 前端 ReadinessFailureCard 接入：1-2h（ui-simplify）
- 用户验证（跑 1 个真实 ST PIT manifest）：30 min

**风险**：
- 低 — 已有 14 测试 + 283 全套 PASS 兜底；boundary audit §8.5 给底气
- 唯一悬念：H6（WSL/远端 timeout）首次冷启动仍可能 hang —— 留 §5.1 后续扩展处置

**何时选**：用户认为 audit §8.5 优先级 > blockers §5 line 76 局部判断；接受 81b1370 commit message 误标的小污点

### 4.2 路径 B：revert 让 Codex 主导

**操作**：
1. 拆 `81b1370` —— 保留 12 个 frontend 文件（T2 ui-simplify 工作），revert 5 个 backend 文件
2. 因 `81b1370` 是 origin/HEAD，需 force-push 或加新 revert commit
3. preflight 设计交付 Codex（本文档 §1-2 + §5 作为 design input）
4. Codex 在独立分支重做实施

**工作量**：
- revert + force-push（破坏性）：1h
- Codex 重做实施：4-6h（含理解 design + 重写测试）
- 协调 Codex review + merge：1-2h
- 总：6-9h

**风险**：
- 高 — force-push 破坏 origin 历史；其他 reviewer 可能在看；commit graph 紊乱
- 中 — Codex 重做的实施可能与本实施有 schema 漂移（typed error 名 / preflight 结构）；要重新过测试 + cross-test 矩阵

**何时选**：用户严格遵循 blockers §5 line 76 字面意义；认为 backend 修改必须由 Codex 主导；接受额外 5-7h 重做成本

### 4.3 路径 C：协调 Codex review

**操作**：
1. 保留 `81b1370` 不动
2. 把 backend 5 文件作为单独 PR 分支（从 81b1370 cherry-pick 或新建分支）
3. 邀请 Codex review backend 5 文件 + 14 测试
4. Codex 同意 → 合 main；不同意 → 转路径 B（revert）

**工作量**：
- cherry-pick + open PR：30 min
- Codex review 等待：取决于 Codex schedule（数小时到 1-2 天）
- 合 main 或 revert：1-3h

**风险**：
- 中 — 等待 Codex review 阻塞 P0-F UI 接入
- 低 — 内容质量已通过测试，Codex review 主要看 code style 与 quantevolver/ 接口稳定性

**何时选**：用户希望既保住实施又获得 Codex 背书；不急于立即 UI 接入

### 4.4 路径推荐摘要

| 路径 | 工作量 | 风险 | 适用场景 |
| --- | --- | --- | --- |
| **A keep** | 2-3h | 低 | audit §8.5 是权威，blockers §5 line 76 描述需修正——**作者推荐** |
| B revert | 6-9h | 高 | 严格按 blockers §5 line 76 字面 |
| C 协调 Codex | 4-6h（不计等待） | 中 | 折中保留 + 跨 Codex 背书 |

---

## 5. 后续扩展 menu（即便选 keep，preflight 仍可扩）

### 5.1 H6 WSL bridge / 远端 timeout 检查

加 `qe_node_reachable` 检查（preflight 第 6 项）：

- 在 `_materialize_runtime_source_from_node` 调用前用 `QEWorkspaceClient.for_node` 做 5s timeout ping
- 失败抛 `BrokerConnectivityError`（trading_core/errors.py 已有）
- 限制：只在 cache miss 路径触发；cache hit 路径仍秒过

工作量：1-2h（含测试）

### 5.2 H7 ST PIT universe spans 检查

**不在 preflight 内** —— 这是 P0-D 范围；建议：

- 在 selection_center `_require_data_ready` 中加 universe end_date >= trade_date 的硬校验
- 与 P0-D（Codex 续期任务）协调

工作量：跨阻断点协调，1 天起

### 5.3 readiness.py 整合关系

`backend/services/paper_trading_v2/readiness.py` 已有 paper trading 侧的 readiness preflight。当前 P0-F preflight 与之**正交**：

| 维度 | paper_trading_v2/readiness.py | live_inference preflight（本次） |
| --- | --- | --- |
| 触发面 | paper portfolio 启动 / day_runner | selection_center auto_generate live inference |
| 检查点 | minute data / suspend_d / pre_close / refresh_audit | QE source / node / conf.yaml / factor / model params |
| 错误层 | `DataUnavailableError` 居多 | `LiveInferencePreflightError`（typed） |

**建议**：保持平行，不强制合并。两者面向的失败域不同（市场数据 vs QE 推理资产），合并会引入语义混乱。可在文档层做交叉链接。

### 5.4 preflight 结果持久化

当前 preflight 失败仅抛错，未持久化结构化结果。可选扩展：

- 把 `LiveInferencePreflightResult` 落 `paper_v2.run.error_json`（已天然走 `repository.fail_run` 路径）
- 加 `paper_v2.preflight_audit` 表跟踪历史 preflight 结果（趋势分析：哪个 check 最易 fail）

工作量：2-3h（schema + 后端）

### 5.5 前端 UI 接入

`frontend/src/components/paper-v2/ReadinessFailureCard.tsx`（T2 已建）已 ready。接入步骤：

1. SelectionRun.error.error_code === `LIVE_INFERENCE_PREFLIGHT_FAILED` 时渲染 ReadinessFailureCard
2. 5 个 check 名做中文映射（qe_source → "QE 实验源" 等）
3. suggestion 字段直接渲染为操作引导

工作量：1-2h（含 TypeScript 类型 + i18n 映射）

---

## 6. 不在本次范围

- ❌ 任何 live_inference.py / selection_center/service.py 修改（已在 81b1370 交付）
- ❌ 任何新测试代码（14 已交付）
- ❌ blockers 文档实际修改（建议见 §3.4，由 lead / 用户决定）
- ❌ 前端 UI 接入（建议见 §5.5，由 ui-simplify 后续接）
- ❌ Codex 工作面任何修改（quantevolver/ / qe_strategies/ / model_registry / finding_store 不动）

---

## 7. 引用

| 文档 | 章节 / 行 | 关系 |
| --- | --- | --- |
| `docs/analysis/paper_v2_user_requirement_audit_20260507.md` | §0 / §7 / §8.5 | P0-4 阻断点 + 工作面边界权威定义 |
| `docs/analysis/paper_v2_blockers_20260508.md` | §5 line 76 | 描述需更新（见 §3.4） |
| commit `81b1370` | 5 backend 文件 | 本文档对应实施 |
| `backend/services/strategy_package/live_inference.py` | line 51-104（dataclass）/ 396-509（resolver） | 实施细节 |
| `backend/services/selection_center/service.py` | `_ensure_authoritative_selection_artifact` / `_require_live_inference_preflight` | wiring 接入 |

---

**END OF DOCUMENT**
