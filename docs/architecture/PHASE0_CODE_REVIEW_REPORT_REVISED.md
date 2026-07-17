# Phase 0 代码复审报告（2026-07-17 收敛版）

## 结论

PR #2227 只能证明首版代码进入 main，不能证明 Phase 0 已真实可用。复审发现 5 组 P1
问题并登记 BUG-688～BUG-692。前四组已合入修复；BUG-692 收敛部署 helper 与文档。

当前结论：

- 代码结构、真实 API/DB contract、cache 安全和专用 CI 已达到进入受控 integration 的质量线；
- 未执行当前 QE/DB 外部 smoke，因此不能宣布 Phase 0 全验收或 Phase 1 已解锁；
- production DDL/dependency/runtime 均未触发，状态为 noop/not-run。

不再使用“98%”“1,938/1,940 行”“100% 完整”等无可重放依据的百分比。

## 初始复审问题

| BUG | 初始缺陷 | 修复结果 |
|---|---|---|
| BUG-688 | 不存在的 QE artifact API、固定 endpoint、client 生命周期不明 | #2260：task node + 真实 workspace API + client ownership |
| BUG-689 | 对同步 DB pool 使用 async with、旧表/字段、隐式 latest/不存在预测表 | #2266：同步 repository、canonical market、显式 candidate/provider |
| BUG-690 | 路径逃逸、缺 provenance 仍加载、半写、无跨进程锁/容量/TTL、安全 clear | #2270：remote manifest、原子/锁/TTL/淘汰/reparse-safe |
| BUG-691 | HMM 路径未进入专用 CI；integration 硬编码且包含写/DDL | #2273：专用 HMM nox/CI、coverage/duration receipt、只读 opt-in smoke |
| BUG-692 | 部署器硬编码密码、错误 async DB、旧表、建用户/schema/GRANT、改 gitignore | 退役 DB mutation，仅保留 plan/verify/cache bootstrap |

## 当前代码质量证据

### Unit/contract

- `hmm_data_source_backend`：62 passed、1 个环境受限 reparse skip、4 integration deselected；
- branch-aware coverage：72.26%，门槛 70%；
- 最慢本地用例 0.14s，JUnit/coverage receipt 写入 `tmp/validation/hmm_data_source/`；
- GitHub PR #2273 实际生成并通过 `Backend tests (hmm_data_source_backend)`。

### 安全边界

- QE download 绑定 task node、recorder、remote manifest；
- cache 绑定 SHA/size/row count/schema/quality；
- test provenance 默认拒绝；
- canonical market 只读查询使用 explicit as-of；
- isolation/integration 测试不执行 DML/DDL；
- unsafe deploy helper 不再连接 DB 或修改 tracked files。

### 仍待证据

- 当前 authoritative QE loop 是否发布合规 manifest；
- 配置目标只读 DB 的 trading-calendar/PIT mapping smoke；
- cold/warm cache 在代表性真实行数下的耗时和峰值内存。

这些证据必须通过 `hmm_data_source_readonly_integration` 或后续批准的 benchmark 取得，不能
由 mock coverage 推断。

## 设计符合性

对总体蓝图 v1.1 的 Phase 0 项：

| DAI | 状态 | 证据 |
|---|---|---|
| F-001 QE artifact contract | code/CI complete，live smoke pending | BUG-688、BUG-690 |
| F-002 canonical DB contract | code/contract complete，live smoke pending | BUG-689、BUG-691 |
| F-003 candidate identity | complete | BUG-689 tests |
| F-004 cache safety | complete at code/CI level | BUG-690 tests |
| F-005 test/CI | complete；external receipt pending | BUG-691 / #2273 |
| F-016 isolation guard | complete at static/unit level；production gates noop | BUG-688～692 |

## 最终建议

1. 合入 BUG-692 后，不再维护首版部署/验收叙事。
2. 由战略 session 提供一个具备 trusted manifest 的 QE loop 和明确 as-of，运行只读 integration。
3. integration receipt 通过后再把 Phase 0 标为 accepted 并开始 Phase 1 详细实现。
4. Phase 1 schema 单独走 F2/DDL 审批；不得复活 `deploy_hmm_data_source.py` 的 DB mutation。
