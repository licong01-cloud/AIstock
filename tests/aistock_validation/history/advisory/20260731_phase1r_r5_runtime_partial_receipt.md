# Advisory Phase 1R R5 运行时部分验收回执

> 日期：2026-07-31
> 状态：`partial_runtime_evidence`
> 适用设计：`docs/architecture/advisory_phase1r_r5_api_ui_legacy_cutover_f2_design_20260727.md`
> 边界：只记录已发生事实与缺口；不执行 DDL/DML、服务控制、runtime activation 或交易操作

## 1. Source 与时间线

- R5 源码 PR：`#2809`，merge commit `1c3c8acee6d6c9dcf58644c20b6c77724dac5075`，merged at `2026-07-28T02:13:11Z`。
- 权威历史 batch：`ahrb_dccde5770463663ecbde96fbe304cd26`，created at `2026-07-23T05:06:02.128340+08:00`。
- 该 batch 的 CREATE、BUILD_SOURCE_CATALOG 和两次 RESUME 均发生在 2026-07-23，早于 R5 合入。
- 结论：该 batch 可以证明 R3/R4 的单/原生多 Alpha 历史事实，并可用于验证 R5 query、bridge 和 UI readback 兼容性；不能证明 R5 create/resume API E2E。

## 2. 已验证事实

- batch 状态 `COMPLETED`，两个独立 Program、30/30 package-day、`recoverable_program_count=0`，artifact root identity `fb75ee85a2cc431cb5a6b2deee0f74ebd13153540ec0698dd938c3a46ac3d70b`。
- Dataset Bridge parent operation `ahrop_c8a5b7e09cc1edae2ea43f208ab2e26b` 为 `COMPLETED`；result hash `a32317c707c43bf4e7da0ac6b6005e7907efabf69aea6551657098400082b51b`。
- build `advbuild_973b1a4ce8f3466874c05592`、snapshot `advsnap_9faa542fd165be6131715125` 为内容闭合结果。
- 相同幂等键重放保持 operation id、row version、attempt 和 result hash 不变，并返回 `exact_retry=true`、`dispatch_state=NOT_SCHEDULED`。
- 既有页面观察覆盖 1440x900 与 375x812，能够回读同一 batch、两个 Alpha 模式、`SEALED` 和 snapshot id；console error 为 0，document body overflow 为 false。

## 3. 未闭合事实

- 未形成 R5 合入后的 single Alpha 与 native multi Alpha create/resume/query/outcome/summary/bridge 完整业务回执。
- UI 观察未保存可独立复核的 screenshot/trace，未覆盖 768x1024，未持久化 failed-request 分类。
- 生产仍可见 batch `ahrb_babd6ee056575e324d5c7e9186667942`：状态 `PARTIAL`、`recoverable_program_count=1`、artifact root identity `31103a3c0ef28a71c7b539805f8dc4d3f9a84d88e76e1a0e4ad582c43bebbc3d`。
- 当前配置 root identity 为 `fb75ee85a2cc431cb5a6b2deee0f74ebd13153540ec0698dd938c3a46ac3d70b`；执行器会以 `ADVISORY_HR_ARTIFACT_ROOT_MISMATCH` 拒绝加载该旧 PARTIAL batch，故其当前 UI `recoverable` 投影与实际可执行性不一致。

## 4. 验收映射

| item | current status | evidence | remaining work |
|---|---|---|---|
| F-760 | `incomplete_user_acknowledged` | bridge、exact retry、既有双 Program query/UI readback | R5 post-merge 双 Alpha 完整命令链、三 viewport 持久 UI evidence、failed-request 分类、PARTIAL batch 恢复语义 |
| F-763 | `incomplete_dependency_user_acknowledged` | F-740-F-759、F-761-F-762 已有源码与测试证据 | F-760 完成后重新执行 DESIGN-COMPLIANCE-001 和父设计状态同步 |

## 5. 影响边界

- 本回执不撤销 R1-R4 已完成的历史业务、Outcome/Summary 或 retrospective SEALED snapshot 事实。
- Phase 0B 可使用既有 R4 SEALED snapshot 开展详细设计或只读质量审计；这不是对 R5 验收的替代，也不形成新的审批或研究准入门禁。
- 在缺口闭合前，允许的完成表述为：`R5 source verified; bridge/exact retry/existing-batch UI readback verified; full runtime E2E pending`。
