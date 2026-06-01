# AIstock CodeGraph Nightly Freshness Artifact Design

版本: v1.0
日期: 2026-06-02
状态: implementation-ready
关联 Issue: https://github.com/licong01-cloud/AIstock/issues/506

## 1. 背景

CodeGraph 已经被 issue workflow 和 PR Quality 用作代码结构与 affected tests 的加速器。为了避免每个 Codex / Claude Code 窗口重复判断图谱是否存在、是否过期，Nightly 需要产出一个轻量 freshness artifact，作为后续 Context Pack、Validation Center 和 Research Assistant 的只读输入。

## 2. 设计目标

- 生成 `codegraph-freshness.json` 和 `codegraph-freshness.md`。
- 检查 CodeGraph CLI、索引是否存在、索引是否 up to date、索引 mtime 是否超过阈值。
- 任何 CodeGraph 缺失或过期都只返回 `workflow_gate=warning`，不阻断 Nightly、issue workflow 或 PR 合入。
- 不 fork CodeGraph，不引入新 DB/DDL/依赖，不触碰生产 8001/3000 或生产数据库。

## 3. Artifact 字段

| 字段 | 含义 |
| --- | --- |
| schema_version | `aistock_codegraph_freshness_v1` |
| workflow_gate | `ready` 或 `warning` |
| freshness | `fresh` / `stale` / `missing_index` / `unavailable` / `unverified` |
| graph_root | 当前或 canonical worktree graph root |
| git_commit | 生成时 commit |
| index_age | index mtime 和 age_seconds |
| index_summary | CodeGraph files/nodes/edges/up_to_date 摘要 |
| blocking_for_issue_workflow | 固定 `false` |
| warnings | 非阻断问题列表 |

## 4. Nightly 接入

现有 `code-intelligence-weekly` job 在生成 Understand Anything summary 前执行：

```bash
python scripts/code_intelligence_adapter.py freshness \
  --output-dir "$OUT_DIR" \
  --output "$OUT_DIR/codegraph-freshness.stdout.json" || true
```

`|| true` 是有意设计：CodeGraph 是上下文加速器，不是验证真源。Nightly 的真实质量门禁仍由 nox、Validation Center、DR、Paper v2/QE L3 负责。

## 5. 验收矩阵

| ID | 验收项 | 验证 |
| --- | --- | --- |
| CGF-001 | fresh index 返回 ready | unit test: up-to-date index |
| CGF-002 | CodeGraph 缺失返回 warning | unit test: missing CLI/index |
| CGF-003 | 过期 index 返回 warning/stale | unit test: stale mtime |
| CGF-004 | Nightly 上传 freshness artifact | workflow YAML parse + local dry-run |
| CGF-005 | 不阻断 issue workflow | payload `blocking_for_issue_workflow=false` |

## 6. 上线策略

- Phase A: 合入 warning-only freshness artifact。
- Phase B: 观察 Nightly artifact，确认 self-hosted runner 是否有 CodeGraph CLI/index。
- Phase C: 后续再把 freshness 展示到 Validation Center；仍不设为合入阻断。
