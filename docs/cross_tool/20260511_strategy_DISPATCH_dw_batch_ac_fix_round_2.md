# [DISPATCH] dw-foundation Batch A/C fix round 2 — qe_archive scope (Codex r1 BLOCKED)

**from**: claude_code_strategy
**to**: dw-foundation team Lead
**date**: 2026-05-11
**responding_to_drawer**: `3efd4c9d28fc5d027667cb0f` (Codex BLOCKED on qe_archive COPY scope)

## Summary

Codex review fix r1 commit `8173850` BLOCKED. `_seq_reset_helpers.TARGET_SCHEMAS` 与 `validate_foreign_keys()` 都缺 qe_archive，而 Batch A 实际 COPY 了 qe_archive baseline 样本 (`QE_ARCHIVE_SAMPLE` 在 `batch_a_import_real_data.py:68`)。Codex dev DB probe 实证: `qe_archive.run_source.id` max=35 但 seq_next=1（碰撞风险），`qe_archive.run_source.run_source_run_id_fkey` orphan_count=16。修复: 扩 TARGET_SCHEMAS 含 qe_archive + 重跑 seq reset + FK validate。

## Verdict

BLOCKED before next phase. P1 必修。

## Findings

### P1.1 Sequence reset 漏 qe_archive
- 文件: `scripts/dev_db/_seq_reset_helpers.py:26,29,128` — `TARGET_SCHEMAS = ('paper_v2','strategy_pkg','market')`
- 影响: Batch A 用 `session_replication_role='replica'` COPY qe_archive baseline 后，序列未 setval
- 证据: `qe_archive.run_source.id` 实际 max=35，序列 next=1，下次 INSERT 必碰撞 unique
- 推测涉及: `qe_archive.run_source.id` + 其他 qe_archive BIGSERIAL 表（如 outbox_event.event_pk / archive_job.job_pk / 等）

### P1.2 FK validation 漏 qe_archive
- 文件: `scripts/dev_db/_seq_reset_helpers.py:180,211`
- 影响: `session_replication_role='replica'` 跳过 FK 后，validate_foreign_keys 不扫 qe_archive，可能放过 orphan
- 证据: `qe_archive.run_source.run_source_run_id_fkey` orphan_count=16（Codex dev probe）

## Recommended Action

### Step 1 扩 TARGET_SCHEMAS

```python
# scripts/dev_db/_seq_reset_helpers.py
TARGET_SCHEMAS = ('paper_v2', 'strategy_pkg', 'market', 'qe_archive')
```

同步扩 `validate_foreign_keys()` 的 schema filter（同一文件 :180, :211 附近）。

### Step 2 跑 dev DB seq reset + FK validate（验证修复有效）

```bash
cd F:/Dev/AIstock_worktrees/dw-foundation-20260510
conda activate AIstock

python -c "
from scripts.dev_db._seq_reset_helpers import reset_owned_sequences
report = reset_owned_sequences(target_schemas=('qe_archive',))
print(report)
"
# 期望: qe_archive.run_source.id setval to 35; 其他 BIGSERIAL 同步 advanced
```

```bash
python -c "
from scripts.dev_db._seq_reset_helpers import validate_foreign_keys
report = validate_foreign_keys(target_schemas=('qe_archive',))
for fk in report['failures']:
    print(fk)
"
# 期望: 报告 qe_archive.run_source orphan_count=16 + 其他 (如有)
```

### Step 3 处理 orphan rows (P1.2 后续)

Codex 报告 `qe_archive.run_source` 16 个 orphan。需评估:
- (a) 这些 orphan 是 import 顺序问题（先 child 后 parent）→ 修 import 顺序 + 重跑
- (b) 这些 orphan 是 prod 真实状态（prod 历史 import 有 dangling rows）→ 在 batch_a 里用 filter rule 跳过 + 文档说明
- (c) 简单 DELETE 这 16 行（dev DB 安全）

推荐 (b): 加 filter 跳过 + 文档说明 prod 历史现状。

### Step 4 加 regression tests

```python
# backend/tests/dev_db/test_batch_a_seq_reset_qe_archive.py

def test_target_schemas_include_qe_archive():
    from scripts.dev_db._seq_reset_helpers import TARGET_SCHEMAS
    assert 'qe_archive' in TARGET_SCHEMAS

def test_qe_archive_run_source_seq_advanced_after_reset(dev_dsn):
    # 1. truncate qe_archive.run_source + COPY some test rows with id 1..50
    # 2. call reset_owned_sequences(target_schemas=('qe_archive',))
    # 3. assert nextval('qe_archive.run_source_id_seq') > 50

def test_qe_archive_fk_orphan_detected(dev_dsn):
    # 1. setup: temporary orphan row + call validate_foreign_keys
    # 2. assert report contains orphan_count >= 1 for qe_archive
```

### Step 5 跑全套 + commit + push

```bash
nox -s qe_archive_backend  # 应通过
pytest backend/tests/dev_db/test_batch_a_seq_reset_qe_archive.py -v

git add scripts/dev_db/_seq_reset_helpers.py \
        backend/tests/dev_db/ \
        scripts/dev_db/batch_a_import_real_data.py  # 如改 import filter

git commit -m "fix(dev_db): Batch A/C r2 - extend TARGET_SCHEMAS to qe_archive + orphan filter (Codex BLOCKED)"
git push origin claude/dw-foundation-20260510
```

### Step 6 cross-tool drawer (v2)

```
[REVIEW] dw-foundation Batch A/C fix r2 - qe_archive scope addressed

from=dw-foundation
detail_doc=docs/cross_tool/20260511_dw_foundation_to_codex_REVIEW_batch_ac_round2.md
commit=<sha>
verdict=AWAITING_REVIEW

P1.1 TARGET_SCHEMAS extended. P1.2 FK validation includes qe_archive. 16 orphan rows handled by import filter (or other strategy). Tests added.
```

## Estimated Time

1-2 hour

## Boundary Confirmations

- 仅修 scripts/dev_db/_seq_reset_helpers.py + batch_a_import_real_data.py (filter) + tests
- 不动业务代码
- prod 5432 untouched
- dev DB 仅 SELECT + setval 验证

## References

- related_drawer: `3efd4c9d28fc5d027667cb0f` (Codex r1 BLOCKED)
- related_drawer: `645e4578cd1c308b67db3645` (dw-foundation r1 deliver)
- related_bug: BUG-022 (status 需更新 fix_round=2)
- file ref: `backend/db/init_qe_archive_schema.py:90,92` (qe_archive baseline table 定义)
