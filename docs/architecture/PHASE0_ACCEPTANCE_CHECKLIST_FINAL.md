# Phase 0 验收清单（2026-07-17 收敛版）

> 状态：代码/CI acceptance complete；controlled external integration pending。<br>
> 权威设计：总体蓝图 v1.1；详细契约见 Phase 0 detailed design v2.0 和模块 README。

## A. 已完成的阻塞项

- [x] QE task node 与真实 workspace API contract（BUG-688 / #2260）
- [x] 同步 DB repository、canonical schema、explicit candidate（BUG-689 / #2266）
- [x] trusted remote manifest、cache path/atomic/process lock/TTL/capacity/safe clear（BUG-690 / #2270）
- [x] 专用 `hmm_data_source_backend` nox/CI route（BUG-691 / #2273）
- [x] unsafe deploy helper 退役；不再硬编码密码、建角色/schema、GRANT 或改 `.gitignore`（BUG-692）
- [x] production DDL/backend dependency/frontend dependency gates 均为 noop

## B. 可重放本地/CI 证据

```powershell
rtk python -m nox -s hmm_data_source_backend
rtk python -m pytest backend/tests/scripts/test_ci_change_classifier.py -q -p no:cacheprovider
rtk python -m nox -s l0
rtk python -m nox -s validation_module_registry_l0
rtk python -m nox -s validation_catalog_integrity
```

当前 receipt：

- HMM：62 passed、1 skipped、4 integration deselected；coverage 72.26%；
- classifier：27 passed；
- catalog integrity：5 passed、0 findings；
- GitHub #2273：`Backend tests (hmm_data_source_backend)` SUCCESS。

覆盖率 70% 是最低回归门，不等于外部路径验收。不得恢复旧文档的“>90% 已完成”结论。

## C. 默认安全行为

- [x] integration 默认跳过；
- [x] `hmm_data_source_readonly_integration` 缺开关/loop/as-of 时直接拒绝；
- [x] integration 代码无 INSERT/UPDATE/DELETE/CREATE/DROP；
- [x] helper 默认 `plan`，不连接 DB；
- [x] cache apply 要求 `--apply --confirm phase0-cache-only`；
- [x] helper 只允许 repo `tmp/` 下目录，拒绝 reparse 和越界路径。

## D. 待战略 session 提供的外部证据（Phase 1 阻塞）

- [ ] 一个当前、可访问且已发布合规 remote manifest 的 `HMM_TEST_QE_LOOP_REF`；
- [ ] 明确 `HMM_TEST_AS_OF_DATE` 和只读目标 DB 配置；
- [ ] 执行：

```powershell
$env:AISTOCK_HMM_READONLY_INTEGRATION = "1"
$env:HMM_TEST_QE_LOOP_REF = "<task>/<LoopN>"
$env:HMM_TEST_AS_OF_DATE = "YYYY-MM-DD"
rtk python -m nox -s hmm_data_source_readonly_integration
```

- [ ] 保存 `tmp/validation/hmm_data_source/readonly-integration.xml` receipt；
- [ ] cold/warm cache 代表性输入的时长、行数和峰值内存证据。

外部 smoke 只读，不启动 8001/3000/19080，不连接未授权生产目标，不执行 DML/DDL。

## E. Phase 0 helper 验收

```powershell
rtk python scripts/deploy_hmm_data_source.py plan --json
rtk python scripts/deploy_hmm_data_source.py verify --json
rtk python scripts/deploy_hmm_data_source.py bootstrap-cache `
  --apply --confirm phase0-cache-only --cache-dir tmp/hmm_evolution_cache --json
```

验收点：

- [x] plan/verify 不修改 tracked files；
- [x] 不导入或调用 `get_conn`；
- [x] 不含角色、GRANT、schema/table DDL；
- [x] cache bootstrap 幂等；
- [x] `production_ddl_gate=noop` 明确输出。

## F. 状态判定

- Source merge：BUG-688～BUG-692 各自独立 PR/aftercare。
- Code/CI readiness：完成。
- Controlled external integration：pending。
- Phase 1 implementation readiness：blocked on section D receipt。
- Production DDL/runtime activation：not requested / noop。

只有 D 段完成并经战略 session 审核后，才能把 Phase 0 从“code/CI accepted”更新为
“externally accepted”，随后进入 Phase 1。
