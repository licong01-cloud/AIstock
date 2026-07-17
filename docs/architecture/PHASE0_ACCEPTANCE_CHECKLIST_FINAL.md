# Phase 0 验收清单（2026-07-17 收敛版）

> 状态：代码/CI 与 controlled external integration 均已验收；Phase 1 implementation unlocked。<br>
> 权威设计：总体蓝图 v1.3；详细契约见 Phase 0 detailed design v2.2 和模块 README。

## A. 已完成的阻塞项

- [x] QE task node 与真实 workspace API contract（BUG-688 / #2260）
- [x] 同步 DB repository、canonical schema、explicit candidate（BUG-689 / #2266）
- [x] trusted remote manifest、cache path/atomic/process lock/TTL/capacity/safe clear（BUG-690 / #2270）
- [x] 专用 `hmm_data_source_backend` nox/CI route（BUG-691 / #2273）
- [x] unsafe deploy helper 退役；不再硬编码密码、建角色/schema、GRANT 或改 `.gitignore`（BUG-692）
- [x] Prediction Store 零副本复用、真实 Qlib MultiIndex 与损坏 fail-loud（#2285）
- [x] integration 使用 canonical Prediction Store，DB connection factory 强制 read-only transaction
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

- HMM：69 passed、1 skipped、5 integration deselected；branch-aware coverage 76.19%；
- classifier：27 passed；
- catalog integrity：5 passed、0 findings；
- GitHub #2273、#2285：`Backend tests (hmm_data_source_backend)` SUCCESS。

覆盖率 70% 是最低回归门，不等于外部路径验收。不得恢复旧文档的“>90% 已完成”结论。

## C. 默认安全行为

- [x] integration 默认跳过；
- [x] `hmm_data_source_readonly_integration` 缺开关/loop/as-of 时直接拒绝；
- [x] integration 代码无 INSERT/UPDATE/DELETE/CREATE/DROP；
- [x] integration DB factory 使用 `REPEATABLE READ` 且 `transaction_read_only=on`；
- [x] integration 绑定 canonical Prediction Store 并使用 `prediction_store_only`，不创建 HMM artifact 副本；
- [x] helper 默认 `plan`，不连接 DB；
- [x] cache apply 要求 `--apply --confirm phase0-cache-only`；
- [x] helper 只允许 repo `tmp/` 下目录，拒绝 reparse 和越界路径。

## D. 已完成的外部证据

- [x] `HMM_TEST_QE_LOOP_REF=qe_20260706_013235_bbd4/Loop8`；任务完成于 2026-07-07，
  CAGR `0.941588`、Sharpe `2.372961`、最大回撤 `-0.132000`；
- [x] prediction/label 均为 `available + parsed`，各 `2,260,161` 行；prediction SHA256
  `bc82351d405b5f370eaef50ce3245d237508f1861806bc31ffdd63b62451cfef`；
- [x] 明确 `HMM_TEST_AS_OF_DATE=2026-07-17`，DB 最新完成交易日为 `2026-07-16`；
- [x] 执行：

```powershell
$env:AISTOCK_HMM_READONLY_INTEGRATION = "1"
$env:HMM_TEST_QE_LOOP_REF = "qe_20260706_013235_bbd4/Loop8"
$env:HMM_TEST_AS_OF_DATE = "2026-07-17"
rtk python -m nox -s hmm_data_source_readonly_integration
```

- [x] `4 passed, 1 deselected`，保存
  `tmp/validation/hmm_data_source/readonly-integration.xml` receipt；
- [x] Prediction Store first deserialize `2.1534s`，进程内 warm filter `0.2702s`；
  RSS baseline `104.83 MiB`、peak `402.01 MiB`、delta `297.18 MiB`；
- [x] prediction 日期 `2024-07-01..2026-04-28`，最近 10 个自然日窗口返回
  `35,840` 行、`5,120` symbols；PIT sector mapping 为 `5,864` symbols / `131` L2 codes；
- [x] `source=prediction_store`、`zero_copy=true`、`hmm_cache_created=false`；
- [x] DB `transaction_read_only=on`，未启动 8001/3000/19080，未执行 DML/DDL。

该 QE task 的 label horizon 为 h20；本 receipt 只将其作为高收益 prediction/data-source
样本，不把 label.pkl 解释为 10 日标签。Phase 1 评估仍必须显式绑定 evaluation horizon。

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
- Prediction Store zero-copy：#2285 已合入。
- Code/CI readiness：完成。
- Controlled external integration：完成，section D receipt 已复核。
- Phase 1 implementation readiness：unlocked；尚未声明 Phase 1 功能已实现。
- Production DDL/runtime activation：not requested / noop。

Phase 0 状态更新为 `externally accepted`。后续从 Phase 1 P1-A schema/repository/job
state machine 开始，仍须逐 PR 执行 F2 设计验收和 production gate。
