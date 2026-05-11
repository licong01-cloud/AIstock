# Codex DISPATCH — R6 Prod Apply Runbook + Rollback Procedure

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~18:05
**Type**: Type B (coordinated) - prod ops doc, NOT prod execution
**Branch**: codex/qe-governance-integration-20260509
**Parallel with**: paper-v2 verify b976c23 (drawer 013ab7f7)

## 上下文

paper-v2 baseline GREEN, hotfix audit PASS, backfill scripts ready (`b976c23`). 实盘目标: **明早 9:30 A股开市**。

今晚需要执行 (用户操作):
1. R5 paper-v2 merge to main
2. DR snapshot of prod DB 5432
3. Codex --apply evidence backfill on prod
4. 6 governance migrations apply on prod  
5. R6 git merge
6. prod backend 8001 启动 + paper-v2 daemon enable
7. cold-start sanity check
8. 9:30 实盘

需要一份精确的 runbook 让用户照着跑，避免遗漏 / 误操作。

## 任务

写 **`docs/operations/r6_prod_apply_runbook_20260511.md`** on codex/qe-governance branch.

### 内容要求

**§1 前置条件 (preflight)**:
- 列出所有必须 GREEN 的项: paper-v2 baseline GREEN ✓, hotfix audit PASS ✓, backfill prep dry-run PASS ✓, paper-v2 verify scripts PASS (pending)
- 用户应在哪个状态点开始执行

**§2 DR snapshot 步骤**:
- pg_dump 命令 (含 schema + data, 排除 partition / parallel options)
- snapshot 文件目标路径 (E:\DEV backup\ 还是其他)
- snapshot 完整性 verify (file size, row count sanity)
- snapshot 时间预估
- 如何 rollback 用此 snapshot

**§3 Evidence backfill --apply 顺序**:
- Step 3.1: `python scripts/strategy_package_evidence_backfill.py --apply --target-db prod --confirm <token>`
  - 含完整命令 + 必需参数
  - 预期 JSON output schema
  - PASS 判定标准
  - 失败 abort 判定 + rollback step
- Step 3.2: `python scripts/protected_asset_ledger_backfill.py --apply --target-db prod --confirm <token>`
  - 同上

**§4 6 governance migrations apply 顺序**:
- 列出 6 个 migration 文件路径
- 每个 migration 的 apply 命令 (per-file txn)
- 失败立停 + 已 apply 的 migration 回退方法
- 各 migration 预期 schema change
- 验证命令 (查表是否生效)

**§5 R6 git merge 命令**:
- 已在 takeover doc §6 草稿, 你可 refine
- 含 verify steps + branch cleanup

**§6 Prod backend 8001 启动 + daemon enable**:
- backend 启动命令 (含 env vars)
- paper-v2 daemon enable 配置 (table or env)
- worker process 启动 + 监控

**§7 Cold-start sanity check**:
- 触发一笔模拟单 (非交易时段)
- 验证: fill 写入 paper_v2.fills + capture 字段 (intended_price, fill_market_context, timestamps) + outbox emit (telemetry vs archive routing)
- 验证 governance enable_paper 强制 gate 生效
- 验证 audit trail 完整

**§8 Rollback procedures** (每步独立):
- evidence backfill rollback: DELETE rows by source_run_id?
- migrations rollback: 逆序 DROP / ALTER?
- R6 merge rollback: `git revert -m 1 <merge_commit>` + push?
- daemon stop + backend restart

**§9 Real trading 9:30 开市 cutover**:
- 实盘账户接入 (broker connection check)
- 限额配置 (起手小额)
- 监控点: fill rate, capture coverage, audit failures
- abort 标准 (单笔异常 → daemon stop)

**§10 Time budget**:
- 各步骤 ETA
- 总耗时估计 (今晚 ~22:00 起始 → 明早 9:00 ready)
- 关键节点 (DR snapshot done, backfill done, migrations done, R6 merged, daemon enabled, cold-start passed)

### 风格

- bash 代码块可直接复制 (无占位符 / Windows shell 转义注意)
- 每步骤含: 命令 / 预期输出 / PASS 判定 / FAIL 判定 / 下一步链接
- abort 路径明确 (失败 → rollback step 哪里)

### Do NOT

- ❌ 不要实际执行任何 prod 操作 (这是 doc, 不是 execute)
- ❌ 不要 commit prod credentials / token (用 placeholder)
- ❌ 不要 merge codex branch to main

## Deliver

- commit doc 到 codex/qe-governance branch
- drawer 通知, 含 doc path + 章节大纲 + 总预估时长
- SLA: ~1.5h (与 paper-v2 verify 并行)

## 参考

- takeover doc §6: R6 merge 命令草稿
- playbook v2 §R6: 同上
- Codex deliver doc: `docs/cross_tool/20260511_codex_to_claude_REVIEW_evidence_backfill_prep.md`
- governance branch tip: b976c23
- BUG-023 atomicity
- 6 migrations 位置: backend/migrations/ 或 scripts/ 下相关 (Codex 查)
