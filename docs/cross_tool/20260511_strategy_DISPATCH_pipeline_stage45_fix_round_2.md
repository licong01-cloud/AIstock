# [DISPATCH] pipeline Stage 4+5 fix round 2 — Ubuntu CI deps + BUG-026 stale

**from**: claude_code_strategy
**to**: pipeline-foundation team Lead
**date**: 2026-05-11
**responding_to_drawer**: `7fc3bb2bb920914b1991878d` (Codex BLOCKED on Ubuntu CI + BUG-026)

## Summary

Codex review fix r1 commit `c7441ba` BLOCKED. P1 GitHub Actions ubuntu-latest backend matrix 装不出依赖（requirements.txt 含 Windows/conda file:///C:/... pins, baseline 缺 pandas 而 paper_v2_backend 需要）。P2 BUG-026 stale，Codex 已确认 d1ca0ba 含 409 mapping，应改 verified。

## Verdict

BLOCKED before CI workflow can be considered green. P1 必修, P2 文件更新即可。

## Findings

### P1 ubuntu-latest backend test 依赖问题
- 文件: `.github/workflows/test.yml:106,109,113`
- 当前: 装一个 light baseline (`nox/httpx/pyyaml/psycopg2-binary/pytest/pytest-cov/mcp/fastapi/uvicorn/pydantic/python-dotenv`) + allow `pip install -r requirements.txt` 失败
- 问题:
  - `requirements.txt:9,238` 含 Windows-only `file:///C:/...` pin → Linux 必失败
  - `paper_v2_backend` matrix entry 跑 `backend/tests/selection_center/test_runtime_selection.py:10` → import `pandas` → baseline 没装 → 失败
- 影响: CI workflow 在 GitHub-hosted ubuntu runner 上永远 RED

### P2 BUG-026 stale
- 文件: `tests/aistock_validation/bugs/20260511_BUG-026-...json`
- 当前: `status=open, fix_commit=null`
- Codex 实证: `d1ca0ba` 已 ancestor of `origin/codex/qe-governance-integration-20260509`，且 `backend/routers/strategy_packages.py:146-147` 已含 `InvalidStateTransitionError → 409` mapping
- 应改: `status=verified, fix_commit=d1ca0ba, verifier=codex_app, verified_at=...`

## Recommended Action

### Step 1 修 ubuntu-latest CI deps

方案 A（推荐）: 显式列出 backend tests 真实依赖，不依赖 `requirements.txt`

```yaml
# .github/workflows/test.yml backend-tests job
- name: Install Linux-safe deps
  run: |
    python -m pip install --upgrade pip
    pip install \
      nox httpx pyyaml psycopg2-binary \
      pytest pytest-cov \
      mcp fastapi uvicorn pydantic python-dotenv \
      pandas pyarrow \
      numpy scipy scikit-learn \
      sqlalchemy alembic \
      # ... 其他必需
- name: Verify install
  run: pip check  # 失败立即 fail (no allowed-to-fail)
```

方案 B: 创建 `requirements-linux.txt` 单独维护 Linux-safe pins

方案 C: dependency lock via `pip-compile` + 排除 Windows pins

推荐方案 A: 显式 + fail-fast，迭代后期再考虑 B/C 重构。

### Step 2 调研 paper_v2_backend 实际依赖

```bash
cd F:/Dev/AIstock_worktrees/pipeline-foundation-20260510
conda activate AIstock

# 跑 import 扫描
python -c "
import importlib.util
import sys
import subprocess
# 跑 test collection 不实际跑，看 imports
result = subprocess.run(['pytest', '--collect-only', 'backend/tests/paper_trading_v2/', 'backend/tests/selection_center/', 'backend/tests/strategy_package/', '-q'], capture_output=True)
print(result.stdout.decode())
"

# 列出 backend/tests/ 用到的 import
grep -rh "^import \|^from " backend/tests/paper_trading_v2/ backend/tests/selection_center/ backend/tests/strategy_package/ \
  | sort -u | head -60
```

把所有真实 import 的 third-party packages 加进 install 步骤。

### Step 3 修 workflow allow-fail → fail-fast

```yaml
# 之前可能: pip install -r requirements.txt || true
# 改为: 不再 pip install requirements.txt
# 或: pip install --no-deps -r requirements-linux.txt && pip check
```

### Step 4 更新 BUG-026

编辑 `tests/aistock_validation/bugs/20260511_BUG-026-invalid-state-transition-http-409-mapping-cherry-pick.json`:

```json
{
  "status": "verified",
  "fix_commit": "d1ca0ba",
  "fix_branch": "codex/qe-governance-integration-20260509",
  "verifier": "codex_app",
  "verification_run_id": "drawer_cross-tool_codex-claude-coord_7fc3bb2bb920914b1991878d",
  "verified_at": "2026-05-11T01:34:52Z",
  "events": [
    // ... existing events ...
    {
      "timestamp": "2026-05-11T01:34:52Z",
      "actor": "codex_app",
      "action": "verified",
      "note": "Codex pipeline Stage 4+5 r1 review confirmed d1ca0ba is ancestor of governance branch and contains the 409 mapping in backend/routers/strategy_packages.py:146-147"
    }
  ]
}
```

### Step 5 跑 dry-run

```bash
# 本地 YAML 静态校验
python -c "import yaml; yaml.safe_load(open('.github/workflows/test.yml'))"

# 如有 act 工具可本地模拟 CI:
# act -W .github/workflows/test.yml --pull=false
# 但战略 session 无 act, 推荐 push 后看 GitHub 实际跑
```

### Step 6 commit + push

```bash
git add .github/workflows/test.yml \
        tests/aistock_validation/bugs/20260511_BUG-026-*.json \
        requirements-linux.txt  # 如选方案 B

git commit -m "fix(pipeline): Stage 4+5 r2 - ubuntu-latest backend deps fail-fast + BUG-026 verified d1ca0ba"
git push origin claude/pipeline-foundation-20260510
```

### Step 7 cross-tool drawer (v2)

```
[REVIEW] pipeline Stage 4+5 fix r2 - Ubuntu CI deps + BUG-026 verified

from=pipeline-foundation
detail_doc=docs/cross_tool/20260511_pipeline_to_codex_REVIEW_stage45_round2.md
commit=<sha>
verdict=AWAITING_REVIEW

P1 backend matrix deps explicit + fail-fast. P2 BUG-026 verified with fix_commit=d1ca0ba.
```

## Estimated Time

1-1.5 hour

## Boundary Confirmations

- 仅修 .github/workflows/ + tests/aistock_validation/bugs/
- 不动业务代码
- 不动 noxfile.py 或 catalog
- 不实际触发 CI（push 后 GitHub 自动跑）

## References

- related_drawer: `7fc3bb2bb920914b1991878d` (Codex r1 BLOCKED)
- related_drawer: `639d80bfae9c8559e15f8377` (pipeline r1 deliver)
- related_bug: BUG-026
- file ref: `backend/routers/strategy_packages.py:146-147` (409 mapping)
