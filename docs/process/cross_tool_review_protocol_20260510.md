# Cross-Tool Review Protocol (Claude Code <-> Codex)

> **作者**: Claude Code 战略 session 2026-05-10
> **状态**: AUTHORITATIVE — 用户 2026-05-10 已确认 4 层扫描 / Phase 3 后启动
> **关联文档**: `docs/process/dev_db_test_data_plan_20260510.md`

## §1 启动条件

**仅在以下条件全部满足后启动**:
- Phase 1A + 1B + Phase 2 (T12 apply) 全部完成
- dev DB 测试数据 Batch A + B + C 全部导入
- Phase 3 三方 live smoke 全绿（Codex governance smoke + Claude archive handler integration + paper-v2 capture fields write tests）

## §2 范围

### Claude Code 检查 → Codex 全量代码

| 工作面 | 路径 / 分支 |
|---|---|
| Codex governance integration | `origin/codex/qe-governance-integration-20260509` HEAD |
| StrategyPackage 服务层 | `backend/services/strategy_package/`（除 `live_inference.py` 由 Claude 拥有） |
| Model Registry | `backend/services/model_registry/` |
| Migrations | `backend/migrations/strategy_pkg_*_20260509.sql` + `model_registry_phase5_20260509.sql` + `qe_phase4_master_seed_contract_20260509.sql` + `trading_core_v2_schema.sql` |
| Governance smoke scripts | `scripts/governance_migration_smoke.py` + `scripts/strategy_package_governance_readonly_smoke.py` |
| Tests | `backend/tests/strategy_package/` + `backend/tests/model_registry/` |

### Codex 检查 → Claude Code 全量代码

| 工作面 | 路径 / 分支 |
|---|---|
| paper_v2 工作面 | `origin/main` paper_v2 capture (T5/T6.1/T6.2) + `origin/claude/paper-v2-vnpy-mvp-20260508` |
| dw-foundation | `origin/claude/dw-foundation-20260510` 全部 (T12 + T14a + T16 + 文档) |
| qe_archive baseline | `backend/db/init_qe_archive_*.sql`（已存在 27 张表的 DDL） |
| Paper v2 runtime | `backend/services/strategy_package/live_inference.py` + Paper v2 / vn.py / trading_core 路径 |
| RL execution | `backend/services/rl_execution/` 全部 |
| Factor pipeline | `backend/services/quantevolver/factor_*.py` |
| Tests | `backend/tests/paper_trading/` + `backend/tests/qe_archive/` + `backend/tests/dw/` 等 |

## §3 4 层自动扫描

每侧执行：

### Layer 1: Static Analysis
```bash
ruff check <target>          # 代码规范 + 常见 bug pattern
mypy <target> --strict       # 类型注解 + 类型一致性
bandit -r <target>           # 安全漏洞扫描
```

### Layer 2: Unit Tests
```bash
pytest <test_dir> -v -p no:cacheprovider --tb=short
```

### Layer 3: Integration Tests against dev DB
- Codex 启动 Claude 写的 PaperV2ArchiveHandler / FactorValueArchiveHandler 集成测试
- Claude 启动 Codex 写的 governance live smoke 全量

### Layer 4: Mutation Testing (抽样)
```bash
mutmut run --paths-to-mutate=<critical_module> --runner='pytest -x -q'
```

抽样关键模块（state machine / fail-fast 路径 / outbox event handler）。

## §4 Bug 报告格式

每侧产出独立报告：
- `docs/cross_review/codex_review_of_claude_code_20260512.md`（Codex 写）
- `docs/cross_review/claude_review_of_codex_20260512.md`（Claude Code 写）

每个 bug 条目结构：

```markdown
### BUG-XXX [BLOCKING|HIGH|MED|LOW] <短标题>

- **File**: `path/to/file.py:line`
- **Branch**: `<branch>@<commit_sha>`
- **Layer**: 1 / 2 / 3 / 4
- **Reproduction**: 最小复现步骤或测试命令
- **Expected**: ...
- **Actual**: ...
- **Recommended fix direction**: 仅文字方向，**不写代码**
- **Suggested owner**: <original author 工作面>
- **Cross-references**: 关联其他 BUG-YYY（如有）
```

报告头部含统计：
```
Total bugs found: N
- BLOCKING: x
- HIGH: y
- MED: z
- LOW: w
Layers contributed: ...
Coverage: <static/unit/integration/mutation 各覆盖率>
```

## §5 修复责任分级（用户默认采纳，Q4 未明确，按战略 session 推荐）

| 类别 | 处理方式 |
|---|---|
| **BLOCKING / 安全 / 数据损坏** | 立即报警 → 用户授权后由原开发方修复 → reviewer 二次验证 |
| **HIGH 语义错误 / 设计偏差** | 仅找 Bug → 写入文档 → 原开发方修复 → reviewer re-verify |
| **MED 性能 / 文档漂移** | 仅找 Bug → 文档 → 原开发方批量修复 |
| **LOW 拼写 / 注释 / unambiguous 一行修复** | reviewer 可发 PR suggestion，原开发方一键 accept |

**默认: Bug-only**。原因：
1. D1 file-level ownership boundary 已锁定
2. 原作者上下文最完整，跨工具修复易"修对症状不修对病因"
3. 两轮 review（reviewer 找 → 作者修 → reviewer re-verify）多抓 bug
4. 审计链清晰，事故复盘容易

## §6 流程时序

```
T0     Phase 3 三方 smoke 全绿（启动条件满足）
T0+1d  双侧 4 层扫描完成
T0+1d  双侧产出 bug report 文档（彼此 review 但不修）
T0+2d  双侧根据收到的 bug report 修复（仅修自己拥有的代码）
T0+3d  双侧 re-verify 修复 → 确认全绿
T0+3d  双侧签字 → 准备生产 rollout 讨论
```

## §7 沟通

每侧完成 4 层扫描后：
1. push bug report 文档到自己的分支
2. 通过 cross-tool mempalace drawer 通知对方文档路径 + commit SHA
3. 对方 review bug report，对每个 bug 答复 accept / reject / discuss

每修复一批 bug 后通过 drawer 通知 reviewer re-verify。

## §8 例外

如发现 cross-tool 不可调和的协议层冲突（如 Phase 1 中 model_registry view DDL 错误），按 BLOCKING 处理：用户实时仲裁。
