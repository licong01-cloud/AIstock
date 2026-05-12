# paper-v2 DISPATCH — 5-Layer Verify Codex 9f31ac8 Sentinel Endpoint

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-12 06:50 (北京时间)
**Type**: Type C audit (dual-party verify, BLOCKER 9:30)
**SLA**: ≤ 45 min (~07:35 deliver, 9:30 LocalSim 模拟盘 ETA 紧)

## 上下文

Codex Task 8 deliver (drawer `85bc32aa`, commit `9f31ac8`):
- 新 endpoint `POST /api/v1/paper-v2/coldstart-sanity/sentinel-order`
- 新文件 `backend/services/paper_trading_v2/coldstart_sentinel.py`
- LocalSim-only (hard reject miniqmt_sim/live)
- run_id `sanity-*` prefix validation
- package_id exact match (无 fallback)
- 409 InvalidStateTransitionError / 503 daemon unavailable mapping
- capture field preflight (intended_price/fill_market_context/created_at/updated_at)
- sanity script Phase 2 update: package_id payload + `--sentinel-package-id` default first `--package-id` + required_capture_columns preflight + cleanup paper_v2.run + deterministic paper_v2.portfolio
- runbook update (LocalSim-only caveat + package_id + OpenAPI deploy check + capture DDL dependency)
- branch-local SQL `add_paper_v2_capture_fields_20260510.sql` (cross-branch dep ref)
- **branch-local 兼容**: sentinel portfolio insert 用 `execution_policy/runtime_config/metadata` 存 broker_backend (避免 branch schema 缺列)

Codex 自验:
- 53 tests passed (sentinel + sanity)
- 116 tests passed (broader regression)
- 0 P1, 8 P2 findings
- py_compile + git diff --check PASS

时间紧 (9:30 = 2h44min from now), SLA 严格 45 min。

## 任务 — 5-Layer Audit

### L1 — Static safety + Endpoint contract

读 `backend/routers/paper_trading_v2.py` (新增 endpoint) + `backend/services/paper_trading_v2/coldstart_sentinel.py`:

- [ ] 5-guard chain 完整 + 严格先于业务逻辑:
  - `broker_backend != local_sim` → 400/422 hard reject (no DB / API call)
  - `run_id` 不以 `sanity-` 开头 → 400 reject
  - `package_id` 不在 enable_paper=true 列表 → 404/409 reject (no fallback to latest)
  - 当前时间在 9:30-11:30 + 13:00-15:00 CST → 409/423 reject (避免污染真实交易)
  - daemon process down → 503 reject
- [ ] capture column preflight: 调用 endpoint 前 SELECT `\d paper_v2.fills`, 确认 4 列存在 (intended_price + fill_market_context + created_at + updated_at)
- [ ] response schema 含: fill_id + run_id + intended_price + fill_market_context + created_at + updated_at + routing_class + outbox_event_id
- [ ] error responses 含 typed error 名称 (`InvalidStateTransitionError` / 等)
- [ ] no prod DB connect during endpoint (LocalSim broker in-process matching)

### L2 — Tests rerun

```bash
git fetch origin codex/qe-governance-integration-20260509
git checkout 9f31ac8 -- backend/routers/paper_trading_v2.py backend/services/paper_trading_v2/coldstart_sentinel.py backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py
python -m pytest backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py -v
```

预期: **20+ passed** (Codex 报告 sentinel + sanity 共 53 passed)

覆盖度检查:
- LocalSim happy path
- miniqmt_sim reject
- miniqmt_live reject
- run_id format invalid
- package_id not in enabled set
- 交易时段 reject
- daemon down (503)
- capture column missing (preflight fail)
- governance gate disabled (409)

### L3 — Sanity script Phase 2 integration

读 `scripts/paper_v2_coldstart_sanity.py` Phase 2 修改:

- [ ] Phase 2 调用 `POST /api/v1/paper-v2/coldstart-sanity/sentinel-order` (不是 mock)
- [ ] `--sentinel-package-id` 参数默认值为第一个 `--package-id` (Codex 报告)
- [ ] required_capture_columns preflight 在 Phase 1 (preflight) 加入
- [ ] cleanup Phase 4 含 paper_v2.run + deterministic paper_v2.portfolio (Codex 报告)
- [ ] 30 tests rerun PASS (与原 30 一致或更多)

### L4 — Runbook §7.4 + §8.5 alignment

读 `docs/operations/r6_prod_apply_runbook_20260511.md`:

- [ ] §7.4 加入 sentinel endpoint deploy 检查 step (OpenAPI deploy check)
- [ ] §8.5 移除 endpoint absent caveat
- [ ] capture DDL dependency 在 runbook 明示 (paper_v2.fills 4 列 prerequisite)
- [ ] LocalSim-only caveat 明示 (miniqmt 路径不支持)
- [ ] cleanup scope 列出 (sentinel run + portfolio + fills + outbox + governance evidence row)
- [ ] §11 E2E wrapper README 与新 endpoint 对齐

### L5 — Branch-local fallback 评估

Codex 报告: branch-local 兼容方案存 `broker_backend=local_sim` 在 `execution_policy/runtime_config/metadata`, 而非直接 INSERT `paper_v2.portfolio.broker_backend`。理由: branch schema 缺该列。

- [ ] **理由合理性**: branch-local DDL 是 R5 合 main 已含, 但 codex branch base `a72411d` 前于 R5 merge — 所以确实 codex branch 该列缺。Codex 的 metadata fallback 合理。
- [ ] **R6 merge 后自动失效**: R6 merge 时, main 上 `paper_v2.portfolio.broker_backend` 列已存在 (R5 中加入)。sentinel portfolio insert 路径应自动切回正路径 (直接 INSERT broker_backend 列)。验证 9f31ac8 中 sentinel insert 逻辑是否检测 schema state:
  - 选项 A: 总是用 metadata (post-R6 也 fallback) — sub-optimal
  - 选项 B: 检测 column 存在 → 直接 INSERT, 否则 fallback — 理想
  - 选项 C: 仅 branch-local 期间 fallback, R6 merge 后 metadata 字段未使用 — 取决于 sentinel_cleanup 行为
- [ ] 如选项 A, 标 P2 finding (tech debt)
- [ ] 如选项 B, 标 PASS
- [ ] 如选项 C, 标 PASS (R6 merge 后 fallback path dead code, 后续 cleanup task)

### 输出 verify doc

`docs/cross_tool/20260512_paper_v2_VERIFY_codex_sentinel_endpoint.md`:
- 5 layers PASS/FAIL/PARTIAL
- L5 fallback 选项分类 + 评估
- L4 runbook alignment 8 fields check
- 23+ tests rerun result
- verdict: READY / READY-WITH-CAVEATS / BLOCKED
- 9:30 LocalSim 模拟盘 GO: YES / NO

### deliver drawer

```
[VERIFY] paper-v2 5-layer verify Codex 9f31ac8 sentinel endpoint
commit: <new>
branch: claude/paper-v2-baseline-post-r5-20260511 (continue)
L1 Static + 5-guard: PASS/FAIL
L2 Tests (23+): PASS/FAIL
L3 sanity script Phase 2: PASS/FAIL
L4 Runbook §7.4+§8.5: PASS/FAIL (X/8 fields aligned)
L5 Branch-local fallback: PASS / PARTIAL / FAIL (选项 A/B/C)
9:30 LocalSim GO: YES / NO
doc: docs/cross_tool/20260512_paper_v2_VERIFY_codex_sentinel_endpoint.md
```

## Do NOT

- ❌ 不要 INSERT dev DB (read-only verify)
- ❌ 不要 connect prod DB
- ❌ 不要 fix Codex 代码 (报告 finding 即可)
- ❌ 不要 merge codex branch
- ❌ 不要启 prod backend / daemon

## References

- Codex deliver: drawer `85bc32aa`, commit `9f31ac8`
- main HEAD: `568c16c` (R5 + 2 verify docs + 2 baseline docs + sanity verify + runbook)
- 9:30 时间预算: 2h44min from 06:50
- LocalSim mock-only (实盘暂不开发, miniqmt PR-005 延后)
