# BUG-497 analysis: PR Quality P0/P1 gate false positive

## 结论

当前 `scripts/issue_flow.py` 的 PR Quality P0/P1 gate 存在文本引用误判：`_infer_bug_ids_from_text`
在 `scripts/issue_flow.py:959` 使用 `re.findall(r"\bBUG-\d{3,}\b", ...)` 从分支名、commit、
PR title/body 与 changed files 中收集裸 `BUG-NNN` token；`_infer_pr_quality_context` 再把这些 token
放入 `linked_issues`/`bug_id_signals`。在旧逻辑下，`evaluate_pr_quality_gate` 会把
`bug_id_signals` 当作 `explicit_bug_context`，只要同时从文本中看到 `P0`/`P1` severity signal，
就把 PR 归为 high-risk。

这会让 feature/epic PR 仅因正文引用 BUG 编号而进入 `--enforce-p0-p1-evidence` 强制路径：例如 #1523
类 PR 正文写到 "reuses BUG-470 status predicates"，但不修改 `tests/aistock_validation/bugs/*.json`、
不 `Closes` bug issue，也没有 linked bug record severity；这种引用型上下文不代表“正在修 bug”，
应为 `not_applicable`。

## Root Cause

- Linkage inference 是宽松的引用发现：裸 `BUG-NNN` token 可以用于关联背景、术语或历史问题，不能单独证明 PR 是 bug-fix PR。
- P0/P1 gate 旧判定把 `inferred.bug_id_signals` 纳入 high-risk 证据，缺少“真 bug 修复”的结构化判断。
- `linked_issues` 同时承载 bug JSON、GitHub issue refs、PR 文本引用等多种来源；用它直接驱动 enforce 会把 reference-only PR 与 closing/fixing PR 混淆。

## 修复策略

High-risk P0/P1 判定改为必须满足“真 bug 修复证据”之一：

1. changed files 包含新增/修改 `tests/aistock_validation/bugs/*.json`；
2. PR title/body/commit 明确使用 closing verb（`Closes`/`Fixes`/`Resolves` 等）关闭 bug issue 或 `BUG-NNN`；
3. linked bug record 带显式 `severity`/`severity_guess`。

仅 PR body 文本提到 `BUG-NNN`（引用型）不再判 high-risk，即使 PR 文本出现 `P0`/`P1`，也应返回
`not_applicable`；但真实 bug-fix PR 的 enforce 行为保持不变，缺 evidence 时仍 `blocked`。

## Regression Coverage

`backend/tests/scripts/test_issue_flow_pr_quality.py` 覆盖三类场景：

- 真 bug 修复 PR：改动 `tests/aistock_validation/bugs/*.json` 且 severity=P1，缺 validation/production evidence 时仍在 `--enforce-p0-p1-evidence` 下 `blocked`；
- Closes bug issue 的 P1 PR：未修改 bug JSON 但 body 明确 `Closes #...`，缺 evidence 时仍 `blocked`；
- feature/epic PR 仅引用 `BUG-470`：不改 bug JSON、不关闭 bug issue，返回 `not_applicable`，覆盖 #1523 的 runtime/shadow 引用型场景。

## Scope

本次为纯 CI/tooling 改动，不触碰 A epic / paper_v2 产品代码，不启动服务，不写生产 DB；
`production_ddl_gate=noop`。
