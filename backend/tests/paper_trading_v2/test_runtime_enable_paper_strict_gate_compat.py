"""
INT-7 placeholder — 待 d1ca0ba (Codex governance hard gate) 合 main 后启用.

Scope: 验证 paper-v2 runtime 调用 enable_paper(package_id) when
governance_eligibility.paper_ready == false → 抛 StrategyPackageValidationError
+ error.detail 含完整 governance_eligibility info.

Activation: d1ca0ba 合 main + paper_v2 dev DB 至少有 1 个 package
(governance_eligibility.paper_ready=false). 当前 dev DB 4 个 prod-like
packages 都满足 paper_ready=false (Batch A 真数据).

Sister test: test_runtime_enable_paper_compat.py (INT-6, pre-d1ca0ba scope).

REV-1 P1.1: Codex requested explicit placeholder for d1ca0ba post-merge coverage.
"""

import pytest

pytest.skip("INT-7: d1ca0ba 未合 main, 待 Phase 3 全绿后启用", allow_module_level=True)
