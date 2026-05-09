# enable_paper() 错误处理路径审计 (T7) — 2026-05-10

> 状态：audit-only，不修代码 / schema / migration。
> 承诺自：[REVIEW] reply Q3 给 Codex（drawer 9571f8049df2c2ed4604b8d6 + 069c7576bb083edf271f2f10）。
> Codex retest gate 严格化背景：drawer ec4bc625899d771c5862c1c9 + d6507dfd46b02d6592e2c617。
> Created: 2026-05-10. Branch: claude/paper-v2-vnpy-mvp-20260508

## 范围 / 方法

- 仓库内对 `enable_paper(`（带左括号）做全量 grep，捕获所有调用点。
- 对 `def enable_paper` 做单独 grep 定位定义。
- 对常见近义符号 `paper_enable / activate_paper / paper_activate / start_paper / enable_paper_trading` 做扫描，确认无其他入口。
- 对 retest gate / governance-eligibility 相关关键字 (`retest`, `original_fixed_weight`, `paper_candidate`, `paper_ready`, `governance_eligibility`, `is_paper_eligible`, `assess_eligibility`) 做仓库级扫描，验证 Codex 严格化后端代码是否落地到本 worktree。
- 仅 Read：governance 文件 (`service.py / repository.py / runtime_variant.py / validation_run.py / validation_stability.py / package_asset.py`) 不做任何修改。

## §1 enable_paper() 调用点清单

### 定义点

| path:LINE | 类型 | 说明 |
|---|---|---|
| `backend/services/strategy_package/service.py:342` | 唯一定义 | `StrategyPackageService.enable_paper(self, package_id: str) -> StrategyPackageRecord` — wrapper around `transition_status(to_status=PackageStatus.PAPER_ENABLED, reason="enable_paper")` |

近义符号扫描：仓库内**没有**任何 `paper_enable`(函数) / `activate_paper` / `paper_activate` / `start_paper` / `enable_paper_trading` 定义；只有 schema 字段 `paper_enabled`（DDL/policy 行 boolean）和 fixture 函数 `make_paper_enabled_manifest`（不是 enable_paper 同义符号）。也即：**Paper v2 只有一个 `enable_paper` 入口**。

### 调用点（生产 + 测试）

| path:LINE | module | caller function | trigger |
|---|---|---|---|
| `backend/routers/strategy_packages.py:453` | `backend.routers.strategy_packages` | `enable_strategy_package_paper(package_id)` | HTTP POST `/strategy-packages/{package_id}/enable-paper` (FastAPI endpoint, `backend/routers/strategy_packages.py:450` `@router.post("/{package_id}/enable-paper")`) |
| `backend/tests/strategy_package/test_repository_service.py:24` | tests | `test_strategy_package_repository_persists_frozen_manifest_and_status_flow` | pytest 单元测试（in-memory repo, 验证 happy-path 状态流转） |
| `backend/tests/strategy_package/test_repository_service.py:46` | tests | `test_enable_paper_does_not_validate_manifest_minute_runtime_asset` | pytest 单元测试（验证不再校验 manifest 内嵌 minute runtime） |

`enable_paper(` **生产调用点只有 1 个**（HTTP 路由）。

调用图：

```
HTTP POST /strategy-packages/{id}/enable-paper          (routers/strategy_packages.py:450)
  └─ StrategyPackageService.enable_paper(package_id)     (services/strategy_package/service.py:342)
      └─ self.transition_status(... PAPER_ENABLED, reason="enable_paper")  (service.py:343-347)
          ├─ if to_status == PAPER_ENABLED:
          │     record = self.repository.get(package_id)
          │     self.validator.validate_manifest_identity_for_paper_trading(record.current_manifest())   (service.py:326)
          └─ self.repository.transition_status(...)        (repository.py:188 PG / repository.py:667 InMemory)
                ├─ get(package_id)
                ├─ if record.package_status not in allowed_from:
                │     raise InvalidStateTransitionError                       (repository.py:199 / :678)
                └─ UPDATE strategy_pkg.package SET package_status=...  WHERE current_status=...
                  └─ if cur.rowcount != 1: raise InvalidStateTransitionError("compare-and-set race")  (repository.py:218)
```

`allowed_from(PAPER_ENABLED) = {BACKTEST_APPROVED, SELECTION_ENABLED}`（`service.py:40`）。

## §2 每个调用点错误处理审计

### 2.1 HTTP 路由 `backend/routers/strategy_packages.py:450-456`

```python
@router.post("/{package_id}/enable-paper")
def enable_strategy_package_paper(package_id: str) -> dict[str, Any]:
    try:
        record = StrategyPackageService().enable_paper(package_id)
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)
```

- **Error handling shape**: `try: ... except TradingCoreError: _raise_http(exc)`
- **Exception types caught**: `TradingCoreError`（domain 基类，覆盖 `StrategyPackageValidationError` / `DataUnavailableError` / `UnsupportedFeatureError` / `InvalidStateTransitionError` / `ExecutionAlgoError`，见 `backend/services/trading_core/errors.py:12-46` —— 全部继承 `TradingCoreError`）。
- **Log level on error**: 无显式 log（依赖 FastAPI/uvicorn 默认日志输出 HTTPException）。
- **Re-raise behavior**: `_raise_http()` (`backend/routers/strategy_packages.py:84-90`) 将 `TradingCoreError` 转为 `HTTPException(400/404/422, detail=exc.to_dict())` —— 总是 raise，绝不 return。`raise ... from exc` 保留原因链。
- **非 `TradingCoreError` 异常**：未被 except 捕获，直接逃出 → FastAPI 默认 500。
- **Verdict**: **fail-fast**。任何 domain 错误（包括状态机非法迁移）都会变成 4xx HTTP 响应，前端可观测；非典型异常变成 500。**没有 silent-swallow**。

### 2.2 测试调用点（test_repository_service.py:24, :46）

裸 `service.enable_paper(...)`，无 try/except，异常会直接让 pytest 报 fail。**Verdict**: fail-fast (测试上下文)。

### 2.3 定义体审计 — `backend/services/strategy_package/service.py:342-347`

```python
def enable_paper(self, package_id: str) -> StrategyPackageRecord:
    return self.transition_status(
        package_id=package_id,
        to_status=PackageStatus.PAPER_ENABLED,
        reason="enable_paper",
    )
```

下钻 `transition_status` (`service.py:306-333`)：

```python
def transition_status(self, *, package_id, to_status, reason, context=None) -> StrategyPackageRecord:
    allowed = STATUS_TRANSITIONS.get(to_status)
    if not allowed:
        raise StrategyPackageValidationError("unsupported strategy package target status", context=...)
    if to_status == PackageStatus.PAPER_ENABLED:
        record = self.repository.get(package_id)
        # ... 注释说明：Paper v2 不校验 manifest 内嵌的 V24/V25 runtime asset
        self.validator.validate_manifest_identity_for_paper_trading(record.current_manifest())
    return self.repository.transition_status(
        package_id=package_id, to_status=to_status,
        allowed_from=allowed, reason=reason, context=context or {},
    )
```

- **可能抛出的异常**:
  - `StrategyPackageValidationError`（来自 `validator.validate_manifest`、`validate_manifest_identity_for_paper_trading`、子调用 `validate_manifest`）— 见 `validators.py:25-73`
  - `InvalidStateTransitionError`（来自 `repository.transition_status`，原因有两类：当前 status ∉ allowed_from（`repository.py:199` / `:678`）；compare-and-set 竞态（`repository.py:218`））
  - 数据库 / 事务异常（`psycopg2` 错误）会逃出（不被 service 层捕获），HTTP 层也不会归类为 `TradingCoreError` → 500（实际行为合理：DB 异常应可观测）
- **`enable_paper` 自身没有 try/except，所有异常透传**。
- **校验不变量（through `validate_manifest_identity_for_paper_trading`, `validators.py:51-73`）**:
  1. `manifest.asset_checks` 全 passed（`validators.py:25-31`）
  2. `manifest.manifest_sha256` 与重算结果一致（`validators.py:32-42`，frozen_manifest 完整性）
  3. `manifest.package_status ∈ {BACKTEST_APPROVED, SELECTION_ENABLED, PAPER_ENABLED}`（`validators.py:62-73`）
  4. **不**校验 `validation_status`、**不**校验 `paper_candidate`、**不**调用 governance-eligibility 端点 / 内嵌函数。
- **状态机不变量（`STATUS_TRANSITIONS`, `service.py:36-54`）**: `PAPER_ENABLED` 只接受 `from ∈ {BACKTEST_APPROVED, SELECTION_ENABLED}`，repository 层用 SQL 的 `WHERE package_status = %s` + `rowcount != 1` 形成 compare-and-set，杜绝 lost-update。
- **依赖 governance-eligibility 接口？**: **否**。`enable_paper` 完全在进程内推算（manifest 完整性 + 状态机），既不调用 HTTP `GET /strategy-packages/{id}/governance-eligibility`，也不调用任何 `is_paper_eligible / assess_eligibility / governance.eligibility` 函数（仓库级 grep 0 命中，§4 详述）。

**定义体 Verdict**：fail-fast；但**校验半径明显小于 Codex 严格化后的预期**（缺 `validation_status` / retest / paper_candidate 校验，§3 §4 详述）。

## §3 风险登记

| Risk | 调用点 | 影响 | 严重度 |
|---|---|---|---|
| R1 | `service.py:342` `enable_paper` 不校验 `validation_status` | 一个 `BACKTEST_APPROVED` 但其 `original_fixed_weight` retest 未通过的包，可在 Codex 严格化未到达本分支前被推进到 `PAPER_ENABLED`，绕过 retest gate | **HIGH**（按 Codex Phase 3 设计意图；本分支尚无相应 schema 字段，故现实影响 = "策略风险未被门禁拦截"，而非 silent-swallow） |
| R2 | `enable_paper` 完全没调用 governance-eligibility 接口 / 内嵌函数 | 未来 Codex 落 `validation_run` / `validation_stability` 表后，`enable_paper` 不会自动门禁；需要后续显式 wiring | **HIGH**（如果不修，Phase 3 验收时仍可能漏拦截） |
| R3 | router 422 / 400 区分粒度有限：`StrategyPackageValidationError` / `InvalidStateTransitionError` 都映射到 400 | 前端难以单测 "retest gate fail" vs "manifest sha mismatch" vs "status race"。仅文案区分 | **LOW** |
| R4 | `enable_paper` 路径**未触发** `validate_for_paper_trading`（仅触发 `validate_manifest_identity_for_paper_trading`）。execution-policy 校验在 `enable_execution_policy_for_paper`（独立端点 `service.py:400-417`）和 paper_trading_v2 runner / readiness 入口 (`day_runner.py:106,135`、`live_session.py:270,278`、`readiness.py:115-116`)。 | 设计上是 OK 的（执行策略晚于 manifest 冻结），**但**意味着仅有 `enable_paper` 通过不代表能跑起来；如果用户/UI 把 PAPER_ENABLED 当作 "可以跑" 的信号，会有惊讶。 | **LOW**（属于设计选择；T1 的 silent-cache 修复涵盖一阶问题；T7 不重复） |
| R5 | router 层无 access-log/audit-log（仅 status_event 记录在 DB） | 失败请求只在 4xx 响应里有信息，平台运维侧难以告警 | **LOW** |

**没有 BLOCKING 级风险**——`enable_paper` 没有发现 81b1370 风格的 silent-cache-fallback / `except: log+continue`。**没有发现 silent-swallow 反模式**。

T7 的 §1+§2 合计：

- # call sites (生产) = 1 (`routers/strategy_packages.py:453`)
- # call sites (测试) = 2 (`test_repository_service.py:24, :46`)
- # fail-fast = 3 (router + 2 tests)
- # silent-swallow = 0
- # mixed = 0

定义体本身亦为 fail-fast。

## §4 与 Codex retest gate 严格化的兼容性

### 4.1 retest gate 代码搜寻结果

仓库级扫描：
- `Grep "retest"` → 命中**仅在 docs**（`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`、`docs/architecture/strategy_engine_design_20260508.md`、`docs/analysis/paper_v2_user_requirement_audit_20260507.md`）。`backend/` Python 源码 0 命中。
- `Grep "paper_candidate"` → 命中**仅在 docs**（设计文档），**`backend/` 0 命中**。
- `Grep "original_fixed_weight"` → 命中**仅在 docs**。
- `Grep "governance.eligibility|governance_eligibility|is_paper_eligible|assess_eligibility"` → 仓库级 **0 命中**。
- `backend/services/validation/` 目录列出（`__init__.py`, `execution_runner.py`, `file_ownership.py`, `finding_store.py`, `git_activity_provider.py`, `git_status_provider.py`, `history_store.py`, `models.py`, `module_quality.py`, `module_registry.py`, `plan_catalog.py`, `ui_target_catalog.py`）— **没有 `validation_run.py` / `validation_stability.py` / `retest_gate.py`** 等 Codex 设计文档中提到的模块。
- `backend/services/strategy_package/` 目录列出 — **没有 `runtime_variant.py` / `package_asset.py`** 等 Codex governance workspace 文件。

**结论**：Codex strict retest gate 在本 worktree (`claude/paper-v2-vnpy-mvp-20260508`) 中 **代码尚未落地**，仅在设计文档（`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` Phase 3 章节，~L1713）中说明。drawer `ec4bc625899d771c5862c1c9` + `d6507dfd46b02d6592e2c617` 描述的 "scan ALL validation runs (not just latest 100)" 应该是在 Codex 工作空间分支，本分支看不到。

### 4.2 enable_paper() 与 Codex eligibility 的实际接线

- **是否调用 governance-eligibility 端点 (`GET /strategy-packages/{id}/governance-eligibility`)?** 否（仓库级 grep 端点路径 0 命中）。
- **是否调用等价进程内函数（`is_paper_eligible(package_id)` / `governance.assess_eligibility(...)` 等）?** 否（0 命中）。
- **是否直接读 validation 表？** 否（`enable_paper` / `transition_status` SQL 中只触及 `strategy_pkg.package` 和 `strategy_pkg.package_status_event`）。
- **当前路径只校验：** manifest 完整性 + manifest 内嵌 `package_status` 枚举 + DB 中当前 `package_status` 的状态机迁移合法性。

故：**eligibility 端点返回 `paper_ready=false` 时，本分支 `enable_paper` 不会响应**——根本就没问。

### 4.3 失败模式预测（基于代码阅读，非实跑测试）

| Scenario | Codex 严格化预期 | 当前 `enable_paper` 实际行为 | 兼容性 |
|---|---|---|---|
| A. PASSED `original_fixed_weight` retest 缺失 | 应 raise typed error / HTTP 4xx | **不检查**。如果 `package_status ∈ {BACKTEST_APPROVED, SELECTION_ENABLED}` 则继续推进到 PAPER_ENABLED → HTTP 200 | **不兼容（gap）** |
| B. `validation_status != VALIDATION_PASSED` | 应 raise | **不检查**`validation_status`（manifest 中也没有此字段被消费）→ 200 | **不兼容（gap）** |
| C. `paper_candidate=false` | 应 raise | **不检查**`paper_candidate`（schema 内无此列）→ 200 | **不兼容（gap）** |
| D. `frozen_manifest_sha256` 不匹配 | 应 raise | **检查**！`validators.py:32-42` 重算 `compute_manifest_sha256(manifest)` 并与 `manifest.manifest_sha256` 比对，不匹配 → `StrategyPackageValidationError` → HTTP 400 | **兼容** |
| E. 状态机非法迁移（如从 DRAFT 直接 PAPER_ENABLED） | 应 raise | **检查**！`STATUS_TRANSITIONS[PAPER_ENABLED]={BACKTEST_APPROVED,SELECTION_ENABLED}` + repository 层 SQL compare-and-set → `InvalidStateTransitionError` → HTTP 400 | **兼容** |
| F. compare-and-set 竞态 | 应 raise | `repository.py:218` `cur.rowcount != 1` → `InvalidStateTransitionError` → HTTP 400 | **兼容** |
| G. manifest `asset_checks` 任一项 failed | 应 raise | `validators.py:25-31` raise → 400 | **兼容** |

**整体兼容性裁定：partial（部分兼容）**——manifest 完整性 / 状态机 / 资产检查全部 fail-fast 且 typed-error；但 retest / validation_status / paper_candidate 这三个 Codex Phase 3 引入的 gate **完全不接线**。这不是 silent-swallow 漏洞（不存在 except 吞错的代码），而是 **"未实现 / 未 wire"**——一旦 Codex Phase 3 schema 落到主线，必须有后续 PR 把 `enable_paper` 路径接上 eligibility 校验，否则 retest gate 会被绕过。

### 4.4 关键观察：T1 修复 vs T7 审计的关系

- T1 (`c7dee33`) 修的是 `_copy_cached_mlruns_params` 的 silent-cache fallback，属 paper_trading_v2 runtime 范畴。
- T7 审 `enable_paper` 状态推进函数的错误路径——**两者错误形态不同**。`enable_paper` **不存在** silent-cache 反模式（没有 except、没有 log+return-None、没有 fallback to default）。原 [REVIEW] Q3 的担心是 81b1370 "类同 silent-cache" 风格——本审计排除该担心：**`enable_paper` 路径整体 fail-fast，无需 T1 同款修复**。

## §5 修复建议（T8 候选）

无 BLOCKING 级风险。下面列出 HIGH 风险对应的 T8 后续动作（待 Codex Phase 3 schema 落主线后立即跟进）：

### T8-A. enable_paper 接 governance-eligibility（HIGH，配合 Codex Phase 3）

- 文件：`backend/services/strategy_package/service.py`（`enable_paper` 或 `transition_status` 中 `to_status == PAPER_ENABLED` 分支）
- 改动形状：在 `validate_manifest_identity_for_paper_trading` 之后、`repository.transition_status` 之前，新增对 governance-eligibility 的强检查：

  ```python
  # 伪代码（待 Codex schema 落主线后实现）
  eligibility = self.governance.assess_paper_eligibility(package_id)
  if not eligibility.paper_ready:
      raise StrategyPackageValidationError(
          "paper eligibility gate failed",
          context={
              "package_id": package_id,
              "validation_status": eligibility.validation_status,
              "paper_candidate": eligibility.paper_candidate,
              "retest_status": eligibility.retest_status,
              "missing_runs": eligibility.missing_runs,
          },
      )
  ```

- 估时：1-2 工作日（设接口已存在）+ 0.5 工作日单元测试覆盖 §4.3 的 A/B/C 三 scenario。
- Workspace 归属：D1（Paper v2 wiring）+ 依赖 Codex eligibility 接口（先决条件）。

### T8-B. 不变量回归测试（LOW-effort，可立即开工）

无需等 Codex schema，可立刻在本 worktree 加：

- 测试 1：`test_enable_paper_rejects_manifest_sha_mismatch` — 显式构造 sha256 错配的 manifest，确认 raise `StrategyPackageValidationError`（锁住 §4.3 D 当前行为）。
- 测试 2：`test_enable_paper_rejects_invalid_status_transition` — 从 DRAFT 直接调 enable_paper，确认 raise `InvalidStateTransitionError`（锁住 §4.3 E 当前行为）。
- 文件：`backend/tests/strategy_package/test_repository_service.py`（已存在 happy-path，缺这两条 fail-fast 反向测试）
- 估时：~1 小时
- Workspace 归属：D1（test-only）

### T8-C. router 错误细分（LOW，可选）

`StrategyPackageValidationError` 与 `InvalidStateTransitionError` 都被 `_raise_http` 映射到 400，前端难以分辨 retest fail vs status race。可在 `_raise_http` 中给 `InvalidStateTransitionError` 单独分配 409 (Conflict)。估时 0.5 小时。Workspace D1。

---

**闭环**：T7 排除了 `enable_paper` 存在 81b1370 风格 silent-swallow 的疑虑（无 BLOCKING 风险）。但发现了与 Codex 严格化 retest gate 的**接线缺口**（§4.3 A/B/C 三 scenario 全部 gap）；这是 "未实现"，不是 "实现错"，需要在 Codex Phase 3 schema 落主线后用 T8-A 补齐。建议立即实施 T8-B（不变量测试），锁住当前 fail-fast 行为不被未来重构破坏。
