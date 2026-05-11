# [DISPATCH] Pipeline Stage 7 — 流水线完整性补全（4 子任务）

**from**: claude_code_strategy
**to**: 3 个团队 + 新建 frontend worktree
**date**: 2026-05-11 (drafted, dispatch when Phase 3 全绿 + Stage 6 baseline GREEN)
**verdict**: DISPATCH-PREP（4 子任务）

## Summary

用户 2026-05-11 明确：现有流水线不够全面，merge main 前必须补 4 个 gap（UI E2E / cross-module E2E flow / 深度数据验证 / DR 验证）。Stage 7 在 Stage 6 现有 sessions 全绿后启动，估 5-9 天。

## 4 子任务概览

| # | 任务 | 团队 | Worktree | 估时 | 依赖 |
|---|---|---|---|---|---|
| **7.1** | 4 frontend 页面 + Playwright tests | frontend-pipeline-pages | **新建** `claude/frontend-pipeline-pages-20260511` | 2-3 days | 独立, 立即可并行 |
| **7.2** | Cross-module E2E flow test (paper v2 full lifecycle) | dw-foundation | `claude/dw-foundation-20260510` | 2-3 days | T14b/c r3 Codex review PASS |
| **7.3** | Deep data quality assertions | pipeline-foundation | `claude/pipeline-foundation-20260510` | 1-2 days | 独立, 立即可并行 |
| **7.4** | DR validation session | pipeline-foundation | `claude/pipeline-foundation-20260510` (顺接 7.3) | 0.5-1 day | 7.3 完成后顺接 |

## 触发条件

- ✅ Phase 3 三方绿（含 dw r3 review PASS）
- ✅ Stage 6 现有 sessions baseline GREEN
- ✅ 用户授权 Stage 7 + 接受 merge 推迟（已确认 2026-05-11）

## Stage 7.1 — Frontend 4 新页面 + Playwright tests

**Worktree**: `claude/frontend-pipeline-pages-20260511`（基于 origin/main 新建）

**详情**: docs/cross_tool/20260511_strategy_DISPATCH_stage_7_1_frontend_pages.md（见独立子 dispatch doc）

**4 个页面**:

### 7.1.a qe_archive UI
- 路径: `frontend/src/app/qe-archive/`
- 功能:
  - 列表页 `/qe-archive`: 显示最近 paper_v2_run 归档（22 张表 join）
  - 详情页 `/qe-archive/[run_id]`: 单 run 完整 archive 数据展示（fills / positions / daily_snapshots / cash_ledger / events）
  - 过滤: 按 portfolio_id / status / trade_date 范围
  - 数据源: `/api/v1/qe-archive/*` (已存在 backend)
- Playwright tests: `frontend/tests/qe-archive/qe-archive.spec.ts`

### 7.1.b market_regime UI
- 路径: `frontend/src/app/market-regime/`
- 功能:
  - 时间线视图: `market.regime_label` 5 年历史 + 当前 regime
  - 分布图: bull / bear / oscillation / high_vol / low_vol 百分比
  - 切换 source_method: simple_quadrant / hmm_viterbi / bbq / ensemble
  - 数据源: 新加 `/api/v1/market/regime-label/*` endpoints
- Playwright tests: `frontend/tests/market-regime/`

### 7.1.c rl_execution UI tests（page 可能已存在）
- 检查 `frontend/src/app/rl-execution/`，若存在写 Playwright tests
- 若无，新建简单 status 页面

### 7.1.d governance UI
- 路径: `frontend/src/app/strategy-package-governance/`
- 功能:
  - 列表所有 strategy packages + governance_eligibility 状态
  - 5 个 evidence 检查可视化:
    - manifest_identity
    - original_fixed_weight_retest
    - validation_stability
    - protected_asset_ledger
    - runtime_variant paper_candidate
  - paper_ready=true/false + 不通过原因展示
  - enable_paper 按钮（带二次确认 dialog）
- Playwright tests: `frontend/tests/strategy-package-governance/`

**Nox session 注册**:
- noxfile.py 加 `qe_archive_ui` (已存在 skeleton 但 skip), 移除 skip
- 加 `market_regime_ui` / `rl_execution_ui` / `strategy_package_governance_ui` 三个新 session

**完成判据**:
- 4 个页面在 dev port 3012 可正常显示 dev DB 数据
- 各自 Playwright tests 至少 5 个核心 flow（创建 / 查看 / 过滤 / 触发 action / 错误处理）
- 集成到 noxfile.py + module_registry.yaml + test_plans.yaml

## Stage 7.2 — Cross-module E2E flow test

**Worktree**: `claude/dw-foundation-20260510`（dw-foundation team Lead）

**详情**: docs/cross_tool/20260511_strategy_DISPATCH_stage_7_2_cross_module_e2e.md（见独立子 dispatch doc）

**关键 E2E scenario**: paper_v2 full lifecycle

```python
# backend/tests/e2e/test_paper_v2_full_lifecycle.py

def test_paper_v2_simulation_to_archive_full_lifecycle(dev_db):
    """Cross-module E2E:
    paper_v2 simulation -> capture fields write -> daemon emit outbox
    -> handler consume -> qe_archive write -> data consistency assertion
    """
    
    # Step 1: 准备 strategy_package fixture (Batch C 已有 4 packages)
    pkg = setup_synthetic_package(...)
    
    # Step 2: 触发 paper_v2 simulation run（service 层 mock，不启 daemon）
    run_id = run_paper_v2_simulation(pkg, simulation_days=5)
    
    # Step 3: 验证 capture cols 写入 paper_v2.fills
    fills = SELECT * FROM paper_v2.fills WHERE run_id=run_id
    assert all(f['created_at'] is not None for f in fills)
    assert all(f['updated_at'] is not None for f in fills)
    # intended_price 在 MARKET orders 上 NULL (T6.1 §5.7)
    # fill_market_context jsonb 含 13 keys
    assert_jsonb_keys(fills[0]['fill_market_context'], EXPECTED_KEYS)
    
    # Step 4: 模拟 daemon emit outbox events
    emit_paper_v2_outbox_events(run_id, event_types=['paper.portfolio_run.completed'])
    
    # Step 5: 手动跑 PaperV2ArchiveHandler 消费 outbox
    handler = PaperV2ArchiveHandler(...)
    for event in fetch_pending_outbox(event_type='paper.portfolio_run.completed'):
        result = handler.handle(event)
        assert result.status == HandlerStatus.success
    
    # Step 6: 验证 qe_archive.paper_v2_* 22 张表数据完整 + archive_complete=true
    archive_run = SELECT * FROM qe_archive.paper_v2_run WHERE run_id=run_id
    assert archive_run['archive_complete'] is True
    assert archive_run['archive_completed_at'] is not None
    
    # Step 7: 跨表字段级一致性
    src_fill_count = SELECT count(*) FROM paper_v2.fills WHERE run_id=run_id
    archive_fill_count = SELECT count(*) FROM qe_archive.paper_v2_fill WHERE run_id=run_id
    assert src_fill_count == archive_fill_count
    
    # Step 8: governance_eligibility 检查
    eligibility = governance_eligibility(pkg.package_id)
    assert isinstance(eligibility['paper_ready'], bool)
    assert 'manifest_identity' in eligibility
    assert 'protected_asset_ledger' in eligibility
    
    # Step 9: enable_paper 严格 gate（依 paper_ready）
    if eligibility['paper_ready']:
        assert enable_paper(pkg.package_id).status == 200
    else:
        with pytest.raises(StrategyPackageValidationError):
            enable_paper(pkg.package_id)
    
    # Step 10: Idempotency replay
    for event in fetch_completed_outbox(...):
        result = handler.handle(event)
        assert result.rows_inserted == 0  # 完整 archive 已存在, replay skip
```

**Nox session**: `paper_v2_e2e_full_lifecycle`（新加）
- 集成到 paper_v2_l3 + qe_archive_l3 触发链

**完成判据**:
- E2E test 通过, 跨 6+ 模块 (paper_v2 + qe_archive + governance + handler + outbox + idempotency)
- 至少 2 个变体: happy path + governance not-ready path
- 数据一致性 assertion ≥ 10 项

## Stage 7.3 — Deep data quality assertions

**Worktree**: `claude/pipeline-foundation-20260510`

**详情**: docs/cross_tool/20260511_strategy_DISPATCH_stage_7_3_deep_data_quality.md（见独立子 dispatch doc）

**Assertion library**: `backend/tests/data_quality/`

### 7.3.1 字段级一致性
```python
# backend/tests/data_quality/test_field_level_consistency.py
def test_paper_v2_run_archive_mirror(dev_db):
    """每个 paper_v2.run 在 qe_archive.paper_v2_run 中字段对应一致"""
    for run in paper_v2_runs:
        archive = qe_archive.paper_v2_run.where(run_id=run.run_id)
        assert archive.portfolio_id == run.portfolio_id
        assert archive.status.upper() == run.status.upper()
        # ... 30+ 字段对照
```

### 7.3.2 jsonb 结构验证
```python
def test_fill_market_context_jsonb_schema(dev_db):
    """fill_market_context 必含 13 keys + 类型正确"""
    EXPECTED = {
        'stock_id': str, 'trade_date': str, 'data_source': str,
        'prev_close': (int, float), 'limit_up': (int, float),
        'limit_down': (int, float), 'suspend_status': bool,
        'full_day_open': (int, float), 'full_day_close': (int, float),
        'full_day_volume': int, 'full_day_high': (int, float),
        'full_day_low': (int, float), 'generated_at': str,
    }
    for fill in fills_with_market_context:
        for key, expected_type in EXPECTED.items():
            assert key in fill.fill_market_context
            assert isinstance(fill.fill_market_context[key], expected_type)
```

### 7.3.3 派生字段验证
```python
def test_cash_ledger_entry_type_derived_correctly(dev_db):
    """handler derive entry_type 逻辑正确"""
    for archived in qe_archive.paper_v2_cash_ledger:
        source = paper_v2.cash_ledger.where(cash_id=archived.cash_id)
        expected_type = derive_entry_type(source.side, source.notional, source.fee, source.cash_delta)
        assert archived.entry_type == expected_type

def test_simple_quadrant_classification(dev_db):
    """regime_label simple_quadrant 输出与 ret/vol 输入对应"""
    for label in market.regime_label.where(source_method='simple_quadrant'):
        signal = label.source_signal_json
        expected_regime = classify_simple_quadrant(signal['ret_pct_5y'], signal['vol_pct_5y'])
        assert label.regime == expected_regime
```

### 7.3.4 跨表一致性
```python
def test_paper_v2_run_fill_count_matches_archive(dev_db):
    for run in paper_v2.run:
        src_count = paper_v2.fills.count(run_id=run.run_id)
        archive_count = qe_archive.paper_v2_fill.count(run_id=run.run_id)
        assert src_count == archive_count
```

### 7.3.5 时间序列单调性
```python
def test_fills_time_monotonic_per_run(dev_db):
    for run_id in unique_run_ids:
        fills = paper_v2.fills.where(run_id=run_id).order_by('trade_time')
        for i in range(1, len(fills)):
            assert fills[i].trade_time >= fills[i-1].trade_time
```

**Nox session**: `data_quality_deep`（新加）
- 集成到 paper_v2_l3 + qe_archive_l3 触发链

**完成判据**:
- 5 类 assertion × ≥ 3 测试 = ≥ 15 测试覆盖
- 集成到 noxfile + module_registry + test_plans

## Stage 7.4 — DR validation session

**Worktree**: `claude/pipeline-foundation-20260510`（顺接 7.3）

**详情**: docs/cross_tool/20260511_strategy_DISPATCH_stage_7_4_dr_validation.md（见独立子 dispatch doc）

**Tests**: `backend/tests/dr/`

```python
# backend/tests/dr/test_dr_snapshot_restore.py

def test_dr_dump_file_validity(latest_dump):
    """pg_restore --list 验证 dump 完整性"""
    result = subprocess.run(['pg_restore', '--list', latest_dump], capture_output=True)
    assert result.returncode == 0
    # 含 ≥1 TABLE DATA entry
    assert b'TABLE DATA' in result.stdout

def test_dr_dump_schema_matches_dev_db(latest_dump, dev_db):
    """取最新 dump 取头部 schema 部分 → 与当前 dev DB schema diff"""
    schema_from_dump = extract_schema_only(latest_dump)
    schema_from_dev = pg_dump_schema_only(dev_db)
    # 计算差异 (允许 dev DB 有更新但 dump 历史是 frozen)
    diff = schema_diff(schema_from_dump, schema_from_dev)
    # 不应有 missing tables（除非 dev DB 加了新表）
    assert len(diff['removed_in_dev']) == 0
```

**Nox session**: `dr_validate`（新加）
- 集成到 nightly workflow (.github/workflows/nightly.yml)
- 在 dr-snapshot job 完成后跑 dr_validate

**完成判据**:
- ≥ 3 测试覆盖 (dump validity + schema consistency + retention compliance)
- nightly workflow 集成

## 时序

```
Day 0 (Phase 3 全绿 + Stage 6 baseline GREEN 后):
  T-PIPE-7.1 派发到 frontend worktree  ──┐
  T-PIPE-7.2 派发到 dw-foundation      ──┤
  T-PIPE-7.3 派发到 pipeline           ──┤  (3 个并行启动)
  
Day 1-2:
  7.3 完成 → 7.4 派发到 pipeline 同 worktree (顺接)
  
Day 2-3:
  7.1 + 7.2 完成
  
Day 3-4:
  4 个子任务 deliver → Codex review × 4
  
Day 5-7:
  Codex review iterate
  
Day 7-9:
  Stage 7 全绿 → 真正可 merge main
  
Day 9-10:
  R1-R4 production rollout (per playbook)
```

**总: 5-9 天**

## 验收: Stage 7 完成后才可 merge main

| 维度 | 完成判据 |
|---|---|
| **页面验证** | 4 新页面 + Playwright tests 全过 |
| **业务流程** | E2E flow test 跨 6+ 模块通过 |
| **数据准确性** | 5 类 deep assertion 全过 |
| **DR 验证** | dump validity + schema diff session 集成 nightly |
| **现有 sessions** | Stage 6 baseline 仍保持 GREEN |

只有以上全绿，才进入 R1-R4 production rollout phase。

## References

- Stage 6: `docs/cross_tool/20260511_strategy_DISPATCH_pipeline_stage_6_full_validation.md`
- Production rollout: `docs/operations/production_rollout_playbook_20260511.md`
- Protocol v2: `docs/process/cross_tool_communication_protocol_v2_20260511.md`

## 子 Dispatch Docs（待写）

将在本次 commit 中创建：
- `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_1_frontend_pages.md`
- `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_2_cross_module_e2e.md`
- `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_3_deep_data_quality.md`
- `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_4_dr_validation.md`
