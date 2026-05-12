# 执行算法 Model Artifact Resolution 全量修复设计

**作者**: Strategy session (Claude Code 战略)
**日期**: 2026-05-12
**状态**: APPROVED by user, 派 Codex CLI 执行 (与 Codex App Task 18 并行不冲突)
**目标**: 所有 paper-v2 可用的执行算法 model artifact 在 AIstock backend 都可解析

## §1 问题诊断

### 1.1 当前症状

用户报告: 新 QE 实验完成后添加策略包报错
```
DATA_UNAVAILABLE: V25_1_SMALL_CAP early_model_path is not accessible from AIstock backend
```

### 1.2 根因

**execution_algorithm_catalog** (DB 表) 注册了 4 个算法:
- V19_PLAN
- V24_PLAN  
- V25_TWO_STAGE
- V25_1_SMALL_CAP (2026 新加)

**AIstock model cache** (`rdagent_assets/model_cache/execution/`) 当前仅含:
- V24_PLAN/ ✅
- V25_TWO_STAGE/ ✅ (含 v25_early_net_joint_fixed.pt + v25_late_net_joint_fixed.pt)
- **V25_1_SMALL_CAP/** ❌ **目录不存在**

**Resolver 行为** (`backend/services/strategy_package/model_asset_resolver.py:329-351`):
- 查 `cache_root/<ALGO_CODE>/<filename>`
- V25_1_SMALL_CAP 没有 cache 目录 → `_find_existing_source` 返回 None → 抛 DataUnavailableError

### 1.3 关键事实

**V25_1_SMALL_CAP 共享 V25_TWO_STAGE 的模型**:
- migration `add_v25_1_small_cap_execution_algo.sql` 中 default_config:
  - `early_model_path = /home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt`
  - `late_model_path = /home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt`
- 与 V25_TWO_STAGE default_config 完全相同的 model paths
- 二者只差 wrapper 逻辑 (V25_1 加 board-aware bucket schedule), 神经网络一致

**结论**: 不需要训练新模型, 仅需让 resolver 知道 V25_1_SMALL_CAP 可以复用 V25 模型。

## §2 修复方案 (分两阶段)

### 阶段 A — 短期立即 unblock (30 min, Codex CLI 执行)

#### A.1 复制 model 文件到 V25_1_SMALL_CAP cache 目录

```bash
# Windows (git-bash 内)
mkdir -p "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_1_SMALL_CAP"

# 从 V25_TWO_STAGE cache 复制 (注意: 仅复制原始 .pt, 不复制 hashed 版本)
cp "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_early_net_joint_fixed.pt" \
   "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_1_SMALL_CAP/v25_early_net_joint_fixed.pt"

cp "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_late_net_joint_fixed.pt" \
   "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_1_SMALL_CAP/v25_late_net_joint_fixed.pt"

# Verify
ls -lh "F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_1_SMALL_CAP/"
```

#### A.2 验证 resolver 可解析

```bash
cd F:/Dev/AIstock
conda run -n AIstock python -c "
from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from pathlib import Path
r = ModelAssetResolver()
for algo in ['V25_TWO_STAGE', 'V25_1_SMALL_CAP']:
    for fname in ['v25_early_net_joint_fixed.pt', 'v25_late_net_joint_fixed.pt']:
        original = f'/home/lc999/data/rl_models/v25/{fname}'
        candidates = r._candidate_paths(original, algo_code=algo)
        found = next((c for c in candidates if c.exists()), None)
        print(f'{algo} {fname}: {\"OK \" + str(found) if found else \"MISSING\"}')
"
```

预期输出:
```
V25_TWO_STAGE v25_early_net_joint_fixed.pt: OK F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_early_net_joint_fixed.pt
V25_TWO_STAGE v25_late_net_joint_fixed.pt: OK F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_late_net_joint_fixed.pt
V25_1_SMALL_CAP v25_early_net_joint_fixed.pt: OK F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_1_SMALL_CAP\v25_early_net_joint_fixed.pt
V25_1_SMALL_CAP v25_late_net_joint_fixed.pt: OK F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_1_SMALL_CAP\v25_late_net_joint_fixed.pt
```

#### A.3 流水线验证 (短期 fix 仍走流水线)

```bash
cd F:/Dev/AIstock
conda run -n AIstock python -m pytest \
  backend/tests/strategy_package/test_model_asset_resolver.py \
  backend/tests/trading_core/test_execution_algo_capabilities.py \
  -q -p no:cacheprovider

conda run -n AIstock python -m nox -s paper_v2_backend
```

预期: 全 PASS, 0 新 fail。

#### A.4 不需要 commit (仅添加 cache 文件)

`rdagent_assets/model_cache/execution/` 在 .gitignore (因为是 generated artifacts), 复制后不在 git 跟踪范围。**不动 git history**, 即 main 不变。

### 阶段 B — 长期架构修复 (R7 Sprint, Codex App 实施)

#### B.1 根因 (architecture)

5 个独立 architecture 缺陷:

1. **Algo registration 与 model artifact 准备解耦**
   - DB migration 注册算法 (`add_v25_1_small_cap_execution_algo.sql`) 跑过, 但 model artifact 准备没自动化
   - Migration 应该 self-validate: model paths 在 cache root 可访问
   - 当前 fail-fast at runtime (resolver), 应该 fail-early at migration apply

2. **Algo alias / asset sharing 概念缺失**
   - V25_1_SMALL_CAP **实际** 共享 V25_TWO_STAGE 的 model
   - 但 cache 目录按 algo_code 分隔, 同 model 文件复制多份
   - 应有 `asset_namespace` 概念 (V25_1_SMALL_CAP → asset_namespace=V25_TWO_STAGE)

3. **Cross-platform path 自动 mapping**
   - QE 实验在 Linux 跑, model 在 `/home/lc999/data/rl_models/v25/`
   - AIstock backend 跑 Windows, 需要 `F:/Dev/AIstock/rdagent_assets/model_cache/execution/<ALGO>/`
   - 当前 manual cp 或 resolver fallback, 应有自动 sync (rsync / WSL UNC / WSL bridge)

4. **新 QE 实验完成后 asset sync 自动化缺失**
   - QE 训练完毕后 model export, 但 export 到 AIstock cache 是 manual step
   - 应该 RD-Agent 跑完 emit `qe.experiment.completed` event → AIstock listen → 自动 sync model

5. **execution_algorithm_catalog 与 model artifact 检查脱节**
   - catalog 表注册算法 + default_config (含 model paths)
   - 但没有 health check API "list missing model artifacts for all enabled algos"
   - 应该有 `/api/v1/execution-algos/health` endpoint 主动列出缺失

#### B.2 修复 design (5 tracks, R7 Sprint scope)

##### Track 1: Migration Self-Validation
- 修改 `execution_algorithm_catalog` migration 模板, 加 post-INSERT validator
- 新算法 INSERT 后, 自动:
  - 解析 default_config 中 model paths
  - 调 `ModelAssetResolver.resolve_runtime_asset(...)` for each path
  - 如缺失, RAISE `EXECUTION_ALGO_ARTIFACT_MISSING` (migration apply fail)
- Files:
  - `backend/services/execution_algo_catalog/validators.py` (新文件)
  - migration template helper (在 `backend/db/utils/` 加)

##### Track 2: Algo Alias / Asset Namespace
- `execution_algorithm_catalog` 表加 `asset_namespace TEXT` 字段
- 当 `asset_namespace IS NOT NULL` 时, resolver 用 `asset_namespace` 而非 `algo_code` 作为 cache 目录
- 数据迁移: V25_1_SMALL_CAP.asset_namespace = 'V25_TWO_STAGE'
- Files:
  - `backend/db/migrations/add_execution_algo_asset_namespace_20260513.sql`
  - `backend/services/strategy_package/model_asset_resolver.py` (Asset namespace lookup)
  - `backend/tests/strategy_package/test_model_asset_resolver_alias.py`

##### Track 3: Cross-Platform Model Sync
- 新工具 `scripts/sync_qe_models_to_aistock_cache.py`:
  - 输入: WSL/Linux model directory
  - 输出: AIstock cache (per algo_code, with sidecar)
  - 自动检测 Windows Native vs WSL UNC path
  - 支持 nightly cron + on-demand
- Files:
  - `scripts/sync_qe_models_to_aistock_cache.py`
  - `backend/tests/scripts/test_sync_qe_models.py`
  - `docs/operations/qe_model_sync_sop_20260513.md`

##### Track 4: QE Experiment Completion → AIstock Auto-Sync
- RD-Agent QE 跑完 emit `qe.experiment.completed` event (已有 emit hook 类似机制)
- AIstock backend listen via outbox: `paper_v2.outbox_event` (现有架构)
- Listener: 自动 sync model artifacts (调 Track 3 工具)
- Files:
  - `backend/services/qe_archive/listeners/qe_experiment_completed_listener.py`
  - `backend/tests/qe_archive/test_qe_experiment_listener.py`

##### Track 5: Health Check API
- 新 endpoint `GET /api/v1/execution-algos/health`
- 列出所有 enabled algos + cache health (per algo: missing/cached/ok)
- 用于 monitoring + UAT
- Files:
  - `backend/routers/execution_algo_health.py`
  - `frontend/src/app/admin/execution-algos/health/page.tsx`
  - `backend/tests/routers/test_execution_algo_health.py`

#### B.3 长期 ROI

- **零 manual cp**: 新算法注册自动准备 model artifact
- **零 runtime DataUnavailable**: 早期 fail (migration), 不在用户操作时崩
- **Cross-platform 透明**: WSL/Windows 自动 mapping
- **所有 enabled algo 都可用**: paper-v2 可放心用任何 enabled algo

#### B.4 工作量预估 (Codex App)

- Track 1 (Migration validation): 1-2 天
- Track 2 (Asset namespace): 1-2 天 (含 migration + resolver + tests)
- Track 3 (Cross-platform sync): 1-2 天
- Track 4 (Auto-sync listener): 2-3 天 (含 listener + 测试 + 集成)
- Track 5 (Health API): 1-2 天
- **总计**: 6-10 天 (Codex App 主导)

## §3 执行 Plan (Codex CLI 接收本 doc)

### Phase 0: Codex CLI 接收任务 (~10 min)

Codex CLI 读本 doc, 计划执行 Phase A 立即修复 (短期), 并接受 Phase B 作为 future Codex App R7 Sprint 工作。

### Phase A — Codex CLI 立即执行 (~30 min)

1. **A.1**: 创建 V25_1_SMALL_CAP cache 目录 + 复制 V25 model 文件
2. **A.2**: 跑 resolver smoke 验证
3. **A.3**: 跑 pytest + nox 流水线验证
4. **A.4**: 输出执行日志到 `docs/operations/v25_1_small_cap_model_artifact_fix_20260512.md`

### Phase A 验证标准

- [ ] V25_1_SMALL_CAP cache 目录存在 + 2 个 .pt 文件
- [ ] resolver smoke OK 输出
- [ ] pytest test_model_asset_resolver + test_execution_algo_capabilities 全 PASS
- [ ] nox paper_v2_backend 全 PASS
- [ ] 用户 UI 重新添加 V25_1_SMALL_CAP 策略包 不再报 DATA_UNAVAILABLE

### Phase A 不需要做的事

- 不 commit 到 git (cache 文件 .gitignored)
- 不动 main HEAD
- 不 cherry-pick (无 code/doc 改动 in repo, 仅本 design doc 在 main)
- 不动 prod DB
- 不停止 Codex App Task 18 (event loop hotfix)

### Phase B — Codex App 后续 R7 Sprint 实施 (待 Task 18 完成后)

按 §2.2 5 tracks 顺序实施:
1. Track 1 (Migration self-validation) — 防止再发
2. Track 2 (Asset namespace) — 数据建模
3. Track 5 (Health API) — 可观测性
4. Track 3 (Cross-platform sync) — 工具基础
5. Track 4 (Auto-sync listener) — 端到端自动化

每 track 经流水线 baseline 验证 + cherry-pick to main。

## §4 流水线验证

### Phase A 流水线 (Codex CLI 执行)

```bash
cd F:/Dev/AIstock
conda run -n AIstock python -m pytest \
  backend/tests/strategy_package/test_model_asset_resolver.py \
  backend/tests/strategy_package/test_qe_source_resolver.py \
  backend/tests/strategy_package/test_repository_service.py \
  backend/tests/trading_core/test_execution_algo_capabilities.py \
  -q -p no:cacheprovider

conda run -n AIstock python -m nox -s paper_v2_backend
conda run -n AIstock python -m nox -s qe_data_contract_backend
```

预期 PASS, 无新 fail。

### Phase B 流水线 (Codex App, R7 Sprint)

- 每 track 单独 nox session 验证
- 完整 Stage 6 baseline 跑 (含新 track 修改)
- paper-v2 跑 baseline GREEN 后 cherry-pick to main

## §5 与现有 Codex Work 关系

| 现有 task | 与本 design 关系 |
|---|---|
| Task 18 (event loop hotfix, in_progress) | **不冲突**, Codex CLI Phase A 是独立工作 (cache 文件 + smoke 验证, 不动代码) |
| Task 25 (新 QE → paper-enabled pipeline) | **应纳入 Track 3+4** 范围 (Codex App R7 Sprint 实施) |
| Task 119 (GitHub Issues 集成) | **不冲突**, 后续 issue tracker 可 file P1 issue "EXECUTION_ALGO_ARTIFACT_MISSING for new algo" |
| Task 20 (5-缺陷 RCA + 重构) | **平行**, 那是 async/sync 架构, 本 design 是 model artifact 架构 |

## §6 关键决策点 (User APPROVED)

| # | 决策 | 用户选择 |
|---|---|---|
| 1 | 短期方案 A (复制 model 文件) | ✅ 执行 |
| 2 | 长期方案 B (R7 Sprint 5 tracks) | ✅ 执行 |
| 3 | Codex CLI 接管 Phase A | ✅ 与 Codex App Task 18 并行不冲突 |
| 4 | 流水线验证后合 main | ✅ Phase B 走流水线, Phase A 无 code 改动 |
| 5 | 所有执行算法可被 paper-v2 使用 | ✅ Phase B 5 tracks 完成后达成 |

## §7 总结

| 维度 | Phase A (短期) | Phase B (长期) |
|---|---|---|
| 工作量 | 30 min | 6-10 天 |
| 执行者 | Codex CLI | Codex App R7 Sprint |
| 流水线验证 | 现有 pytest + nox | 5 tracks 各自 |
| 合并 main | 无 code 改动 (仅 cache 文件) | 每 track cherry-pick |
| 用户立即可做 | ✅ 添加 V25_1_SMALL_CAP 策略包 不再报错 | — |
| 长期消除 | — | 任何新算法注册自动 ready |

完成后, **所有 enabled 执行算法都可被 paper-v2 使用**, 用户决策成立。
