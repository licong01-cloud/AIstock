# AIstock PR Quality P0/P1 Evidence Gate Design

版本: v1.1
日期: 2026-06-02
状态: implemented
关联 Issue: https://github.com/licong01-cloud/AIstock/issues/504

## 1. 背景

AIstock issue workflow 已经能在 PR Quality 中生成 linkage、scope、required validation、production gates 和 code intelligence 摘要，但此前默认是 report-only。对于 P0/P1 修复，如果 PR 缺少验证证据、scope 或生产 gate 说明，仍可能在合入前由人工发现，造成返工和长耗时 aftercare。

本设计补充一个可配置的 P0/P1 evidence gate。它不是新的 CI 平台，也不替代 GitHub Actions / nox / Validation Center；它只把已有 PR Quality 摘要中的关键证据变成可选阻断项。

## 2. 目标

- 本地 `pr-check` 仍保持默认 warning，避免打断普通离线探索。
- GitHub PR Quality 对 P0/P1 默认启用阻断，尽早拦截缺少证据的修复 PR。
- 对 P0/P1 PR 可 opt-in 阻断缺少证据的情况。
- 输出保持紧凑：PR comment 只显示 gate 状态和缺失项；完整 JSON 留在 artifact。
- 不触碰生产 8001/3000、生产 DB、DDL 或依赖。
- 不新增外部平台，不复制 Validation Center 的验证职责。

## 3. Gate 输入

PR Quality 从以下来源推断：

- PR title/body：BUG id、GitHub issue number、P0/P1 文本、validation evidence、production gate 字段。
- changed BUG JSON：bug_id、github_issue_number、severity、allowed_write_scope、production_*_gate。
- branch / commit message：BUG id、P0/P1 文本。
- changed files：scope check 和 validation plan selection。

如果 `base...head` 没有当前 PR commit 文本，PR Quality 不再回退扫描最近历史 commit，避免把已经合入的旧 BUG/PR 误判为当前 PR linkage。

## 4. Gate 规则

当检测到 severity 为 P0/P1 时，评估以下检查：

| Check | 通过条件 |
| --- | --- |
| linked_issue | PR title/body、branch、commit 或 BUG JSON 能推断 BUG/GitHub Issue |
| scope_passed | changed files 在 issue_record 或 BUG JSON allowed_write_scope 内 |
| validation_evidence | PR body 或 issue record 提供验证通过证据 |
| production_gates | PR body 或 BUG JSON 提供三类 production gate |

Gate 状态：

- `not_applicable`: 未检测到 P0/P1。
- `warning`: 检测到 P0/P1 但未启用阻断；列出缺失项。
- `blocked`: 检测到 P0/P1 且启用阻断，并存在缺失项。
- `passed`: 检测到 P0/P1，关键证据齐全。

## 5. 启用方式

本地或 CI 可显式启用：

```bash
python scripts/issue_flow.py pr-check --enforce-p0-p1-evidence ...
```

GitHub PR Quality 现在默认启用，可通过仓库变量显式关闭：

```text
AISTOCK_PR_QUALITY_ENFORCE_P0P1=false
```

默认值是 `true`。如果仓库变量未设置，GitHub PR Quality 会对 P0/P1 PR 执行 blocking gate；本地命令仍需显式传 `--enforce-p0-p1-evidence` 或设置环境变量。close-sync BUG JSON 已记录 `validation_evidence` 时，也可直接满足 gate，不需要额外在 PR body 复制整段命令输出。

## 6. 验收矩阵

| ID | 验收项 | 验证 |
| --- | --- | --- |
| PQG-001 | 本地默认不阻断 P0/P1 缺证据 PR | unit test: warning by default |
| PQG-002 | opt-in 后缺 evidence/gates 返回非零 | unit test: enforced blocked |
| PQG-003 | evidence 和 production gates 齐全时通过 | unit test: enforced passed |
| PQG-004 | PR Quality workflow 默认启用，可通过变量关闭 | workflow YAML parse + local dry-run |
| PQG-005 | 输出紧凑，不输出 PR body 原文 | summary only stores booleans |
| PQG-006 | 空 diff log 不使用旧历史 BUG/PR 作为当前 linkage | unit test: no stale history fallback |

## 7. 上线策略

1. Phase A: 合入 opt-in gate，仓库变量保持 false。
2. Phase B: 对新 workflow P0/P1 PR 手动 dry-run 或临时启用变量验证。
3. Phase C: 默认启用 GitHub PR Quality blocking gate；如发现特殊兼容性问题，可临时设置 `AISTOCK_PR_QUALITY_ENFORCE_P0P1=false` 回退。

## 8. 影响范围

- `scripts/issue_flow.py`: PR Quality summary 和 opt-in gate。
- `.github/workflows/pr-quality.yml`: 读取仓库变量并传递 CLI flag。
- `backend/tests/scripts/test_issue_flow.py`: gate 行为单元测试。

该变更不会修改运行时服务、数据库、依赖或生产端口。
