# QE 月度回测数据集发布产品化 F2 详细设计

> Feature tier：`F2`
> Feature ID：`qe_monthly_dataset_release_v2`
> 文档状态：`design_revision_round_13`
> 设计日期：2026-08-11
> 设计范围：一键候选月更、幂等复验、组件复用与增量、资源治理、独立 Worker、后端控制面、Skill/Runbook
> 硬边界：本文及后续实现不授权数据导出、既有候选改写、生产指针切换、DB DDL/DML、服务启停或清理

## 1. Background / 背景

现有 QE 数据更新已经具备日线/分钟 Qlib bin、7 个 H5、static、PIT 股票池、资金流单位、
国内指数上下文、候选目录、checkpoint 和 release receipt 等组件能力；资源有界 H5 strict sample
也已证明旧的全历史 `frames` 与全市场静态矩阵常驻内存问题可以被日期块方案消除。

但当前流程仍不是可长期复用的月度产品：

1. 操作者需要手工执行 `plan -> run -> verify`，代码变化时还要手工处理 sample、full、seed 和绝对路径；
2. release ID 含时间戳，相同输入仍会创建新 release，缺少稳定 intent identity；
3. planner 不会自动发现同 cutoff 的已验证候选，也没有 `NO_OP_VERIFIED`；
4. `verify` 主要复核 receipt 与文件 hash，不能用当前 validator 对旧不可变候选重新签收；
5. 组件复用依赖整个 profile config hash，并且不能复用 factor bundle；资源参数或文档变化会误触发重建；
6. profile 仍以 full rebuild 为主，已计算的 QFQ rebase 股票没有进入选择性执行；
7. release-local `run.lock` 没有 heartbeat、lease expiry、process create time 或跨 release 的全局资源互斥；
8. H5 的内存合同没有完整覆盖 daily、minute、validation、WSL 子进程树和全任务阶段吞吐；
9. Skill 是操作护栏，不是 durable scheduler；FastAPI 进程内 daemon thread/BackgroundTasks 又不适合数小时任务；
10. 最新签收、资源证据和 durable signoff 尚未形成一个可由后端、CLI、Skill 共同读取的权威目录。

因此，本设计不是给现有脚本再包一层命令，而是把月度数据更新重构为一套幂等、可恢复、
资源受控、默认 candidate-only 的发布产品。

## 2. Scope / 范围

### 2.1 Goals / 目标

1. 提供一个普通月份只需一次提交的稳定入口：`monthly --candidate-only`。
2. 自动完成 cutoff、已有候选发现、no-op、re-attest、resume、reuse、incremental 或 selective rebuild 决策。
3. 用组件级 semantic/artifact/validation/resource fingerprint 代替整个配置 hash 的粗粒度失效。
4. 建立 source partition manifest，能够发现历史修订，而不只比较 max date/row count。
5. 普通月更只处理新增或失效分区；QFQ/PIT/历史修订只使受影响股票、日期块和滚动依赖失效。
6. 对同 cutoff 的旧完整候选执行只读 re-attestation；验证通过时不复制、不改写、不重导。
7. 建立跨 release 的 host resource lease、heartbeat、fencing token 和子进程树 telemetry。
8. 将重型执行放在独立 Worker；FastAPI 只作为 durable control plane，不持有导出 DataFrame 或子进程。
9. 让 Skill 保持轻量，只调用单一入口、解释 typed result、展示 receipt 并维护安全边界。
10. 保持现有 PIT、资金流、指数/HMM、TDX->Tushare、X 盘候选和 production candidate-only 合同。

### 2.2 Non-Goals / 非目标与边界

- 本设计不授权重新导出 2026-07-31 数据或修改任何历史 candidate 文件。
- 本设计不在 FastAPI lifespan、BackgroundTasks 或 daemon thread 中运行日线、分钟或 H5 导出。
- 本设计不自动启动独立 Worker；Worker 的启动、停止和服务注册仍是独立运行时授权。
- 本设计不增加普通月更中的 production activation、pointer migration、node1 distribution、DB repair、服务重启或清理。
- 本设计不改变股票 PIT 语义、资金流单位、12 指数清单、HMM `000300.SH` benchmark 或 HMM consumer。
- 本设计不使用“仅追加 max date”掩盖历史行情、PIT、复权或 provider 修订。
- 本设计不通过提高并发占满数据库、WSL、内存或 X 盘来换取速度。
- 本设计不要求把已有候选迁移、复制或重新打包后才允许 re-attest。
- 本设计不把 Skill 文本、Runbook 示例或聊天记录作为执行状态 authority。
- 首版后端控制面不提供 production activation API，也不接受任意 shell、任意路径或任意 profile 文件。

## 3. Design Acceptance Index / 设计验收索引

| ID | 设计验收项 |
|---|---|
| F-001 | 一键入口区分 submission、logical request、resolved intent、run/attempt、release 与 attestation identity；重复提交不得产生重复任务。 |
| F-002 | run outcome 与 component/partition action plan 分层；同一次月更可同时 reuse、incremental 和 selective rebuild，并记录每项原因。 |
| F-003 | 同 cutoff 旧候选可由当前 validator 只读 re-attest；artifact validity 与 current-source equivalence 分开，attestation 写入独立控制目录。 |
| F-004 | 每组件分别保存 semantic、source-input、artifact、validation、resource fingerprint；非数据变化不误使数据失效。 |
| F-005 | source snapshot 使用 table/date/instrument 分区 manifest 或等价内容摘要，能够检测历史修订。 |
| F-006 | daily、minute、index、factor/static 有明确增量与选择性失效规则；不以全量重建冒充增量。 |
| F-007 | QFQ denominator 变化使受影响股票的必要历史范围失效；PIT 与滚动窗口传播边界可重放。 |
| F-008 | 硬链接复用必须 copy-on-write；任何修改不得反向改变 source candidate。 |
| F-009 | SQLite 事务控制库、immutable filesystem CAS、同 release lock 与 host resource lease 分层；lease 具有 heartbeat、expiry、owner identity 和 fencing。 |
| F-010 | Task-owned Windows Job 与 WSL cgroup 强制 private-commit/swap hard limit；Worker 同时监控 host commit/pagefile、RSS、DB、X 盘 I/O 和吞吐。 |
| F-011 | 低资源进入 `WAITING_RESOURCE`/安全 checkpoint；OS hard limit 只能 fail-stop identity-bound task child，绝不控制其他进程、降级子集或伪成功。 |
| F-012 | FastAPI 只提交 durable intent 和读取事件/receipt；重型任务不在请求进程执行。 |
| F-013 | Worker 独立支持 `--once/--drain/--serve`，默认 fail closed，backend restart 不丢任务。 |
| F-014 | 状态转换采用枚举、CAS/fencing 和 append-only event journal；snapshot JSON 不能任意跳状态。 |
| F-015 | cancel 是 durable request，并在 stage/chunk/checkpoint 边界协作式退出；强制终止不属于普通 API。 |
| F-016 | API 使用 allowlisted profile、operator authorization 和 idempotency key；禁止任意命令、路径和 env 注入。 |
| F-017 | 普通月份 sample policy 为 `on_contract_change`；单纯 cutoff 推进不重复 strict sample。 |
| F-018 | 40203、数据冲突、PIT/schema/parity、RSS emergency 等不可恢复错误继续 fail fast，不被通用 retry 掩盖。 |
| F-019 | 可恢复 I/O/DB/网络异常只进行有界 retry，保存 attempt、backoff、last error 和 pending scope。 |
| F-020 | receipt 同时报告 reused/recomputed/reattested 分区、资源 telemetry、数据与生产零写入计数。 |
| F-021 | durable signoff 自动生成，不依赖人工复制路径；旧 candidate receipt 与新 attestation 分开。 |
| F-022 | Skill、CLI、API 和 Runbook 共享状态、错误码和 receipt schema，不复制业务决策实现。 |
| F-023 | `status --latest` 和后端 status/events/log API 使用有界读取，不扫描全部候选或把全日志读入内存。 |
| F-024 | 一天一次轻量 reconcile 可补偿停机窗口；它按 intent key 去重，不依赖“每月 1 日”脆弱 cron。 |
| F-025 | candidate build、distribution、activation、DB repair、restart、cleanup 继续分开授权和回执。 |
| F-026 | 所有实现测试使用 temp/fixture，不访问或改写既有 X/E/WSL candidate，不调用真实导出。 |
| F-027 | 新版 Skill 符合 progressive disclosure，核心文件精简，详细合同进入单层 references。 |
| F-028 | 现有 PIT、资金流、指数/HMM、分钟 provider 和生产不可变性合同全部保持。 |
| F-029 | 性能签收比较有效工作量、DB query、rows/s、I/O 和资源等待；不把等待时间误报为计算退化。 |
| F-030 | 设计、实现和最终审查逐项执行 DESIGN-COMPLIANCE-001，不允许 simplified/partial/silent fallback。 |

## 4. Architecture / 架构

```text
Codex/Claude Skill       CLI operator        AIstock API/UI
        |                    |                    |
        +--------------------+--------------------+
                             v
                  Dataset Release Control Service
                 - preview / submit / status
                 - intent identity / decision
                 - events / receipt / cancel request
                             |
                             v
                  Durable Control Repository
                 - intents / runs / events / leases
                 - bounded log index / signoff index
                             |
                   claim + fencing token
                             v
                    Independent Worker CLI
                 - --once / --drain / --serve
                 - heartbeat / resource monitor
                 - cooperative cancel / resume
                             |
                             v
              backend.services.dataset_release core
              - source manifest / fingerprints
              - no-op / re-attest / reuse / incremental
              - daily/minute/index/factor materializers
              - validators / receipt / signoff
                             |
                             v
                     X: immutable candidates

Production activation / DB repair / node1 / restart / cleanup
remain outside this graph and require separate commands and authorization.
```

### 4.1 Single Authority / 单一权威

- `backend/services/dataset_release/` 是数据发布的唯一确定性业务内核。
- `scripts/update_backtest_dataset_monthly.py` 是本地 operator 与 Worker 共用的薄入口。
- 后端 router 只调用 control service，不复制 exporter、fingerprint 或状态转换逻辑。
- Skill 不计算 cutoff、不选择 reuse source、不拼 exporter 命令；它只调用稳定 CLI/API 并解释结果。
- profile YAML 冻结数据语义与资源策略；密钥仅通过受控 env location 注入，永不进入 plan/receipt。

### 4.2 Deployment Boundary / 运行边界

首版 durable control repository 使用显式 `DATASET_RELEASE_CONTROL_ROOT`，位于 repo 与生产目录之外，
默认建议为 X 盘的专用控制目录。控制目录包含本地 SQLite 事务库和 immutable filesystem CAS：

```text
<control-root>/
├── control.sqlite3             # WAL, synchronous=FULL, foreign_keys=ON
├── cas/sha256/aa/<digest>      # plan, event payload, log segment, receipt, attestation
├── staging/<attempt_id>/<fence>/
├── quarantine/
└── control_store_identity.json
```

SQLite 仅允许本机固定卷上的 NTFS/ReFS 路径；UNC、网络盘、reparse point、symlink、candidate root、
production path 或 volume identity 漂移全部 fail closed。`init-control-store` 是独立离线动作；
backend/Worker 启动时只校验 schema/version，不自动建库或升级。本文实现和验证只在 pytest temp root
初始化，不触碰真实 X 盘。

后端只写小型 submission/command 事务；Worker 才能写新的 candidate。后端不得通过
`BackgroundTasks`、daemon thread 或 lifespan 启动 Worker。SQLite control store 不是 market/production
数据库，不需要 PostgreSQL DDL；未来若切换 repository，必须保持同一 protocol 并另走 DEV/production gate。

## 5. Identity 与自动决策契约

### 5.1 Identity Layers

| Identity | Canonical fields | Purpose |
|---|---|---|
| `submission_key` | principal + route + Idempotency-Key | API 请求重放；不依赖重型 source discovery |
| `request_hash` | canonical request JSON | 同 key 异 payload 冲突检测 |
| `logical_request_key` | profile + resolved cutoff + scope + semantic profile digest | 同 cutoff single-active；auto/explicit 解析到同日必须相同 |
| `resolved_intent_key` | logical request key + source content root + frozen PIT spans digest | Worker 冻结 DB/provider 与 PIT 实际内容后的发布身份；PIT-only revision 必须产生新 intent |
| `source_probe_key` | logical request + candidate identity + artifact root + source content/provenance roots + PIT digest + probe policy version + immutable probe receipt digest | fresh unchanged 证据 identity；是复验证据而非数据 identity |
| `run_generation_digest` | operation kind + decision schema + producer + artifact + exact validation identity + sample policy + operation target/lineage | 相同 source 下区分 no-op、re-attest、重新物化与显式 resume generation |
| `run_id` | resolved intent + run generation digest + monotonic lineage | 一次状态机执行；永久唯一约束下同 intent/generation 恰好一个 run |
| `attempt_id` | run + monotonic attempt number | retry/resume 的 Worker ownership、lease 和 staging |
| `release_digest` | sha256(resolved intent + frozen PIT spans digest + scope + producer fingerprint + artifact fingerprint) | 完整不可截断发布 identity；PIT digest 显式重复绑定用于审计/防实现遗漏 |
| `release_id` | cutoff + profile + scope + release_digest16 + `candidate` | 可读 final 目录；DB/marker保存完整 digest并做碰撞拒绝 |
| `attestation_key` | candidate/release identity + producer provenance state/digest-or-sentinel + artifact root + current source content root + PIT digest + semantic profile + validation fingerprint + equivalence mode | 当前候选/lineage复验，不跨 provenance 复用 |

producer provenance state 固定为 `KNOWN | RECONSTRUCTED_SOURCE_ONLY | UNKNOWN`；原 provenance 缺失时 digest
使用 canonical sentinel `UNKNOWN_PRODUCER_PROVENANCE_V1`，不得生成伪 hash。reconstructed attestation 的 key
因此可确定，但 receipt 必须继续显示原 producer provenance unknown；current-source equivalence 只来自本次全值 parity。

`candidate_identity_v1` 的 canonical fields 为：catalog registration UUID、allowlisted root ID、volume serial、
normalized root-relative path、profile、scope、cutoff、lineage anchor、PIT provenance state/digest-or-sentinel、
完整 artifact root 和 producer provenance state/digest-or-sentinel。新 build 的 tagged lineage anchor 为
`BUILD_RELEASE_DIGEST:<release_digest>`；legacy catalog 为
`LEGACY_RECEIPT:<original release ID>:<original canonical receipt hash-or-sentinel>`，避免 receipt 与 candidate
identity 循环引用。PIT 缺失使用 canonical `UNKNOWN_PIT_SNAPSHOT_V1`，但该候选不得 current-source-equivalent。
字段使用 length-prefixed UTF-8 canonical encoding 后 SHA-256。exact path 第一次只读 catalog 时生成 registration
UUID并永久绑定；候选移动/重注册产生新 identity。即使 artifact bytes 相同，不同物理候选或 lineage 也不能共享
re-attest run/receipt。

`operation_target` 对 build 是 resolved intent/action-plan digest；对 re-attest 必须是 candidate identity +
artifact root + attestation target key；对 no-op 必须是 candidate identity + artifact root + fresh
`source_probe_key` + exact validation identity/attestation key。不同 candidate 或不同 artifact root 绝不能复用
同一 non-terminal re-attest/no-op generation；exact validation identity 变化也必须创建新 generation。

terminal pre-publish `/resume` 的 operation kind 固定为 `RESUME_BUILD`，operation target 额外绑定
`resumes_run_id + original run generation + validated checkpoint root + monotonic resume ordinal`；ordinal 在 SQLite
事务中按原 run lineage 分配。因此新 run 与原 build generation 不冲突，而同一个 resume API idempotency key
仍只产生一个 generation。普通 attempt retry 不分配 resume ordinal、不创建新 run。publish commit point 后禁止
`RESUME_BUILD`，只能同 run `FINALIZER_RECOVERY`。

不同 Idempotency-Key 也不能并发创建多个 resume：`BEGIN IMMEDIATE` 先锁定 lineage row，要求目标是该 lineage
最新 terminal leaf，且不存在 non-terminal run；再递增 ordinal、创建 run、更新 latest pointer。并发第二请求若
checkpoint/target 相同则链接现有 active resume，若不同则返回 `RESUME_LINEAGE_ACTIVE` 409。active resume terminal
失败后，下一次只能 resume 最新 leaf，不能回到旧 ancestor 分叉。

canonical `dataset_release_source_probe_v1` receipt 至少含 probe schema/policy version、candidate identity、artifact
root、logical request、冻结 source content/provenance roots、PIT digest、query/provider snapshot tokens、control-store
monotonic probe ordinal、`observed_at` 与 TTL deadline，并以 canonical receipt bytes 的 SHA-256 形成 receipt digest。
`observed_at`/ordinal 只证明本次观测新鲜度，不进入 resolved intent、release 或 artifact identity。TTL 内相同
probe receipt 可幂等复用；TTL 到期必须重新采集并生成新 receipt、`source_probe_key` 和 no-op generation，
不能用旧 terminal no-op 继续回答“当前源未变”。

`requested_at`、resource policy、文档 hash、日志级别和 UI 参数不得进入数据 identity。

API submission 不计算 source digest，也不创建 run。它在一个 SQLite 事务内按
`(principal, route, idempotency_key)` 插入或重放 `QUEUED_RESOLUTION` submission；submission 只关联
logical request，`intent_id/run_id` 初始为空。resolution Worker 以独立 resolution lease claim submission，
完成 DB/PIT/provider source acquisition 后才冻结 resolved intent。

解析完成事务按 `(logical_request_key, resolved_intent_key)`：

- 已有等价 intent 且同一仍 fresh 的 no-op/run generation、attestation 全兼容：submission 链接该 run，写
  `RESOLVED_TO_EXISTING` event；
- fresh content probe 未变且已有 current-source-equivalent attestation：按 9.2 的 no-op finalize 协议创建
  `operation_kind=NO_OP` 的 terminal run，并将 submission 原子链接到该 run；
- source 相同但 validation strengthening：创建 attestation generation/run，不复用旧 terminal outcome；
- source 相同但 producer/artifact/semantic compatibility 要求重物化：创建新 build generation/run；
- 已有 non-terminal 等价 intent+generation run：链接已有 run，不创建 duplicate；
- source content root 或 frozen PIT spans digest 变化：创建新 intent/run，并用
  `supersedes_intent_id/source_revision_reason` 关联旧 intent；
- provider/source acquisition blocked：submission terminal/block，不创建数据 run/candidate。

同 logical request 只允许一个 resolution claim 和一个 non-terminal build run。daily reconcile 即使已有
validated logical request，也按 profile 的 `source_content_probe_ttl` 创建 `SOURCE_REVISION_PROBE`；
不能只依赖 provenance watermark。每次人工 `monthly` 在返回 current-source-equivalent no-op 前都要求
fresh content probe。probe 重算冻结 cutoff 的 required partition content root，未变即 no-op，变化才形成
superseding intent。源表 cutoff 之后的日常增长不得改变已冻结 partition root。

若 probe 发现 revision 时同 logical request 已有 non-terminal build，submission 进入
`WAITING_ACTIVE_RUN`，不创建第二 heavy run、不终止旧 run。旧 run 结束后重新 probe：旧 run 若因
VerifiedPartitionStream drift 失败则创建新 intent；若它按旧稳定 content root 成功，新 revision intent
排队构建并用 lineage supersede，旧候选仍保留其历史 identity。

preview 返回有过期时间的 `preview_token`，只绑定 request hash、profile/config identity 和轻量 watermarks。
submit/Worker 必须重新解析真实 source；preview 与 resolved intent 不同是可见 `PREVIEW_DRIFT` event，
不是静默沿用 preview，也不是失败本身。

Idempotency-Key 永久保留到显式 control-store maintenance：同 principal+route+key+request hash 返回原响应；
同 key 异 hash 返回 HTTP 409 `DATASET_RELEASE_IDEMPOTENCY_CONFLICT`。principal、route 和 key 共同定域，
不得跨用户或跨 endpoint 重放。

永久 API replay 不承诺重新检查 freshness。CLI 未显式给 key 时为每次新的人工 `monthly` 生成 UUID key，并在
网络 retry/response receipt 中复用；显式复用旧 key 只取回旧 submission。scheduler key 绑定唯一 reconcile-cycle
ID。因而“新的人工 monthly 必须 fresh probe”和“同 key 永久幂等”不冲突。

### 5.2 Run Outcome 与 Component Action Plan

自动决策分两层，不能用单一枚举掩盖混合执行：

1. `run_outcome`：`NO_OP_VERIFIED | REATTESTED | CANDIDATE_VALIDATED | BLOCKED | FAILED | CANCELLED`；
2. `component_action_plan[]`：每个 component/partition 独立为
   `NOOP | REATTEST | RESUME | REUSE | INCREMENTAL | SELECTIVE_REBUILD | FULL_REBUILD`。

planner 先执行以下优先级，但继续为所有 required component 形成完整 action plan：

| Priority | Action | Condition |
|---:|---|---|
| 1 | `NOOP` | 同 component identity、manifest root 和当前 validation/source-equivalence 均有效 |
| 2 | `REATTEST` | artifact/semantic compatible，仅 validator strengthening |
| 3 | `RESUME` | 同 run/attempt lineage 有连续且 fence-valid checkpoint |
| 4 | `REUSE` | component/partition fingerprint 完全相同 |
| 5 | `INCREMENTAL` | 新增 source partitions 且历史 inputs 未变 |
| 6 | `SELECTIVE_REBUILD` | 已定位 instrument/date/chunk 失效集合 |
| 7 | `FULL_REBUILD` | schema/formula/universe/artifact format 不兼容 |

例如普通月更可以同时出现：daily/index=`INCREMENTAL`、大部分 minute=`REUSE`、QFQ 变化股票=
`SELECTIVE_REBUILD`、未变 factor chunks=`REUSE`、新增 factor chunks=`INCREMENTAL`。receipt 必须逐项汇总。

每个 `REUSE/INCREMENTAL/SELECTIVE_REBUILD` action 在 immutable plan 中冻结：source release/attestation、
artifact ID、component/partition key、manifest/Merkle root、file identity、reuse mode、mutation set 和 compatibility
reason。baseline 选择为：current-source-equivalent、semantic/artifact compatible、cutoff 不晚于 target 的最高
cutoff；若多个 candidate 在最高 cutoff 的 artifact root 不同，返回 `REUSE_BASELINE_CONFLICT`，不得任意选 latest。
resume/publish 前重新核对每个 frozen baseline root；漂移即 identity conflict。receipt 只回读 plan，不在事后
重新选择 baseline。

重新构建不得因为“代码或文档有变化”这一粗粒度理由触发；必须列出 changed fingerprint、
invalidation edge、component/partition scope 和预计工作量。

## 6. Fingerprint 与 Source Manifest 契约

### 6.1 Fingerprint Layers

每个组件保存：

```text
semantic_fingerprint   source fields, units, PIT, formulas, calendar, universe
source_input_digest    exact source partitions and overlay request identities
producer_fingerprint   component-owned code/dependency manifest
artifact_fingerprint   file format, serializer, compression, row-group/schema
validation_fingerprint validator and required contract versions
resource_policy_digest batch/chunk/RSS/timeout; does not invalidate data by default
```

producer fingerprint 只包含组件真实依赖文件，不包含整个 dirty tree。若 dirty path 与组件依赖相交，
planner fail closed；不相交的文档或其它模块修改不得使数据失效。

semantic fingerprint 必须显式包含既有强制合同：`tushare_moneyflow_shares_yuan_v1`、股/元转换、
`mf_total_net_*` canonical 来源、121 列 static authority、`l2_code_id int16/-1`、12 指数清单与角色、
HMM `000300.SH` benchmark、分钟 TDX->Tushare/missing-key-only/conflict policy 和 PIT rule/version。
producer dependency manifest 必须指向当前 canonical exporter/contract 符号，禁止选择 legacy 路径。

validation change 分类为：

- `validator_strengthening_compatible`：artifact reader/semantic schema 未变，可 re-attest；
- `reader_or_artifact_incompatible`：必须重新物化受影响 artifact；
- `semantic_contract_changed`：按 dependency graph selective/full rebuild。

分类本身进入版本化 compatibility registry 和测试；不能看到 validation fingerprint 变化就自动 re-attest。

### 6.2 Source Partition Manifest

至少按 `dataset × month` 保存 row count、min/max key、required-null count、duplicate count、schema hash、
content digest 和 ingestion/audit identity。高修订风险或按股票失效的数据集再细分到 instrument。

`canonical_partition_hash_v1` 冻结：query/schema version、主键排序、列顺序、NULL marker、Decimal
规范化、float 非有限值策略、日期/时间与时区编码、UTF-8 length-prefix row encoding 和 SHA-256 Merkle
leaf/root 算法。相同 canonicalizer 同时用于 planner manifest 和 materializer actual-read digest。

planner 在 PostgreSQL `READ ONLY, REPEATABLE READ` 事务中读取 schema、水位、PIT 与分区摘要；不在数小时
任务期间保持长事务。实际构建只能通过 `VerifiedPartitionStream`：同一次有序 query/row stream 同时更新
canonical hash，并把相同行送入 transform 或写成 sealed source CAS partition；exporter 禁止在 digest 后
二次查询 DB。需要被多个组件复用的输入必须先完成 sealed source partition，后续只读 CAS bytes。

每个 partition 完成后 actual digest 必须与 plan leaf 完全一致；不一致时返回
`BLOCKED_SOURCE_SNAPSHOT_DRIFT`，其 staging 永不 publish。中途 source 修订 fault-injection 必须证明 artifact
使用的行流与 actual digest 同源，而不是“先 hash、后重读”。最终 receipt 保存 planned/actual root 与逐分区差异。
manifest 允许 DB 端稳定聚合和流式客户端 hash，但不得把全表加载内存。

source identity 拆分：

```text
source_content_root     canonical DB rows + normalized provider/overlay CAS content
source_provenance_root  ingestion job/audit IDs, provider request/response metadata, fetch times
```

`resolved_intent_key` 只使用 content root；provenance root 绑定 receipt 和审计，但内容未变时不触发重建。
内容变而 provenance 恰好不变必须创建新 intent；只有 provenance 元数据变而 content 不变保持同 intent。

TDX/Tushare 响应先按 request identity 规范化、做 DB overlap parity、写 immutable CAS，再把 content hash
纳入 source content root；materializer 只读 CAS overlay。retry 返回不同内容即形成新的 source content root，
不得在同一 resolved intent 下替换响应。provider acquisition 属于 submission resolution 的可恢复阶段，
只有所有 required provider CAS 完成后才冻结 resolved intent 和创建 build run。

只比较 max date、文件大小、mtime、PIT count 或总行数不足以认定历史未变。

### 6.3 PIT Snapshot Identity

planner 不得调用会 rebuild/bootstrap 的 `ensure_*` 路径。它在同一 source preflight 事务中读取
`market.stock_universe_pit_state/spans`，冻结：

```text
universe_key, rule_version, scope, start, cutoff, state identity,
source_fingerprint_sha256, parameter_hash, ordered canonical spans digest
```

candidate 必须保存这份只读 frozen spans artifact 与 hash。materializer 和 re-attest 只消费该 artifact，
不使用运行时“当前 ST 股票池”替代。若旧 candidate receipt 缺少等价 PIT provenance，最多获得
`ARTIFACT_VALID_ONLY`，不能进入 current-source-equivalent no-op/reuse。

ordered canonical spans digest 是 `resolved_intent_key` 与 `release_digest` 的强制 leaf，不隐含在普通行情
`source_content_root` 中。PIT-only revision 即使行情 root 不变，也必须产生新 intent、release digest/final path
与 invalidation plan；identity/negative test 必须断言旧/new release ID 不同，禁止以 final-path conflict 代替失效。

### 6.4 Historical Revision Propagation

- daily/index：失效精确月份/代码，并重算依赖该输入的下游块；
- minute：新增月份正常追加；每股保存 `basis_start/end + denominator + ordered adj_factor digest`；
  denominator 变化时重建该股 daily/minute 必要全历史，历史 numerator 修订时重建精确日期与下游窗口；
- `stk_limit`、`suspend_d`、daily close/pre_close fallback 也进入 minute dependency graph；
- PIT span：只失效受影响股票及其日期范围；
- moneyflow 5/20 日与 `PriceStrength_10D` 按有效观测数传播；slow-static forward-fill 传播到下一真实
  observation；申万 L2 按 interval、static 左连接 anchor 和 schema 列序传播；
- schema/unit/formula：对应组件 full invalidation；
- static schema authority 变化：factor/static artifact incompatible，不能仅 re-attest。

每类 selective action 必须有 fixture oracle：其结果与相同输入的 clean full rebuild 在 index、dtype、
NaN mask 和数值容差上完全等价。未定义 dependency edge 时 fail closed 为 component full rebuild，
不得猜测较小失效范围。

### 6.5 Domestic Index / HMM Context Contract

`index_universe_version=qe_hmm_domestic_core_v1` 是 `qe_hmm_full_v1` 的 required semantic input，精确冻结如下；
实现 profile、manifest 和测试必须逐字段相等，不得依赖 planned Skill/reference 补全，也不得运行时按“热门”扩张。

| daily_code | semantic_role | required_from | HMM benchmark | weight_api_code |
|---|---|---|---|---|
| `000001.SH` | `shanghai_composite` | 2018-08-01 | no | — |
| `000016.SH` | `super_large_cap` | 2018-08-01 | no | — |
| `000300.SH` | `hmm_benchmark_large_cap` | 2018-08-01 | **yes, unchanged** | `399300.SZ` |
| `000688.SH` | `star_50` | 2020-01-02 | no | — |
| `000852.SH` | `small_cap_1000` | 2018-08-01 | no | — |
| `000905.SH` | `mid_cap_500` | 2018-08-01 | no | — |
| `000985.CSI` | `all_a_proxy` | 2018-08-01 | no | — |
| `932000.CSI` | `micro_cap_2000` | 2018-08-01 | no | — |
| `399001.SZ` | `shenzhen_component` | 2018-08-01 | no | — |
| `399006.SZ` | `chinext_component` | 2018-08-01 | no | — |
| `399102.SZ` | `chinext_composite` | 2018-08-01 | no | — |
| `399107.SZ` | `shenzhen_a_composite` | 2018-08-01 | no | — |

`weight_api_code` 只冻结日线/未来权重 API 的代码映射；v1 不下载或消费 `index_weight`，不得把 effective date
伪称 publication vintage。其余 11 项必须显式序列化 `weight_api_code=null`。`399001/399006` 是成分指数，
`399107/399102` 分别承担深市/创业板综合量价语义，volume/amount 不得跨角色替换。`000688.SH` 的
2019-12-31 仅基点行不进入训练，required coverage 从 2020-01-02 开始。

source authority 为 `market.index_daily` 的冻结 content partitions；缺口优先通过 Tushare `index_daily` 写入
candidate-local immutable provider CAS/overlay，不写 DB。overlay 与 DB 重叠键必须逐字段一致；仍缺失、重复、
required NULL/非有限值或 provider conflict 时完整 profile fail closed，不使用 TDX、邻近指数、补 0 或前填。
每代码按 A 股 trading calendar 从 `required_from..cutoff` 形成 coverage matrix 与 source root。

required outputs 同时包含：

1. daily Qlib bin 的 `instruments/index.txt` 精确 12 项；股票 PIT `instruments/all.txt` 不含任何指数；
2. factor bundle `index_daily.h5`（`key=data`，`MultiIndex[datetime,instrument]`）及
   `metadata/index_context_manifest.json`；
3. H5 固定列：`idx_open_point`、`idx_high_point`、`idx_low_point`、`idx_close_point`、
   `idx_pre_close_point`、`idx_return_1d=pct_chg/100`、
   `idx_volume_hand_source`、`idx_volume_share_equiv=vol*100`、`idx_amount_cny=amount*1000`；
4. manifest 绑定 release/source/PIT、完整代码表/角色/起点/weight mapping、字段/单位、coverage matrix、
   per-code content roots、file/schema hash、producer/validation fingerprints 与 `hmm_consumer_activation=not_activated`。

现有 HMM benchmark 始终是 `000300.SH`；本 feature 只交付统一训练/预测候选数据和显式 market-context metadata，
不切换当前 HMM consumer、不改变状态模型。未来 consumer 必须显式绑定 release、schema/universe version、as-of、
feature builder 与 required roles；跨指数 spread 只在同日两侧均存在时计算。12 项代码/角色/起点/映射任一变化
必须提升 universe/semantic version，使 index bin/context component full invalidation；fixture 必须断言 exact list、
`all.txt/index.txt` 隔离、H5/bin/provider 值与单位 parity，以及 benchmark unchanged。

## 7. Incremental Materialization / 增量物化

### 7.1 Immutable Clone 与 Copy-on-Write

新 cutoff 永远写新 release。hardlink 只允许 sealed、永不交给可写 writer 的 partition/CAS blob。
所有 aggregate H5、Qlib metadata、calendar、instrument 文件和任何外部 writer 目标在启动子进程前
必须完整展开 mutation set，并预先复制到新 inode；禁止在 hardlink 上 `mode=w`、append 或 atomic replace。

writer 只接收 attempt/fence 专属 staging 路径，完成后以 temp + fsync/FlushFileBuffers + atomic replace
生成新文件。receipt 保存 source/target file identity、reuse mode、link count、source Merkle before/after。
能力或 mutation set 不可判定时 fail closed，不允许先写后靠 hash 发现源候选已被污染。

### 7.2 Daily、Index、Minute

- daily/index：分区 authority 可以复用 instrument/month；Qlib 最终文件以 instrument 为写入单元，
  未变股票 sealed feature 文件可链接，受影响股票整文件在新 inode 重建；
- minute：只生成新增/修订月份；对 QFQ denominator、ordered factor series、limit/suspend/reference 变化的
  股票重建 dependency graph 指定范围；
- overlay 预先按 code/date 建索引，禁止每个 batch 扫描完整 overlay；
- daily reference history 使用当前 chunk 的最小 rolling lookback，禁止从任务起点累计重读；
- DB 查询按 batch/server-side cursor 读取并直接写 partition artifact，禁止 batch `frames` 跨块积累。

### 7.3 Factor H5/static

日期分块 Parquet 是可签收的中间 authority。未变 sealed chunk 直接复用；失效 chunk 重新计算。
rolling state 作为小型 checkpoint artifact 保存，resume 不得扫描所有历史 chunk 只为重建尾窗。
aggregate H5/static 永远在新 inode 从 chunk authority 串流物化，不允许 hardlink 后原地追加；
不得重新查询或重算未变 source chunk。

debug shard 在 chunk 生产时同步生成，避免为了少量股票再次扫描 7 个完整 H5/static。

## 8. Re-attestation / 旧候选只读复验

`reattest-existing` 接受 allowlisted candidate receipt/path，执行：

1. 验证 candidate path 位于允许根目录且不是 production target；
2. 重算完整 artifact Merkle root，不只信任旧 receipt；
3. 读取原 plan、receipt、component manifests、PIT/source provenance；缺件不伪造；
4. 运行当前分块、值级 validator：PIT multi-span、moneyflow、index、bin/H5、schema、排序、NaN、
   consumer fixture；不得调用仅检查 shape/末日样本的 legacy validator 作为完整 oracle；
5. 若 provenance 完整，重算当前 source partition root 并与 producer input root 比较；
6. 记录 validation fingerprint、source-equivalence mode、读取规模、耗时、peak RSS 和原 producer identity；
7. 将 `dataset_release_attestation_v1` 写入 control CAS/signoff index；
8. 以只读 handle 打开 candidate，不在其中创建、替换、touch、hardlink 或删除任何文件。

attestation outcome 分层：

| Outcome | Meaning | Eligible for monthly no-op/reuse |
|---|---|---|
| `CURRENT_SOURCE_EQUIVALENT` | artifact valid，完整 producer/source/PIT provenance 与当前 source root 等价 | 是 |
| `CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED` | legacy source root 缺失，但全 profile 值级 parity 重建当前 equivalence；原 producer provenance 仍未知 | 是，绑定本次 current source/PIT/validation identity |
| `ARTIFACT_VALID_SOURCE_CHANGED` | 字节满足 validator，但当前 source 已发生有效修订 | 否；形成 selective rebuild plan |
| `ARTIFACT_VALID_ONLY` | 字节满足 validator，但 legacy provenance 不足以证明 source equivalence | 否；只作历史实验复现 |
| `INVALID` | artifact/contract/hash 失败 | 否 |
| `BLOCKED_LEGACY_PROVENANCE` | 最低 artifact/PIT identity 都不足，无法安全复验 | 否 |

Legacy provenance truth table：

| Artifact root | PIT provenance | Source content root | Full current-source value parity | Outcome |
|---|---|---|---|---|
| invalid/missing | any | any | any | `INVALID` or `BLOCKED_LEGACY_PROVENANCE` |
| valid | missing | any | no | `ARTIFACT_VALID_ONLY`，不可 no-op/reuse |
| valid | valid | missing | no | `ARTIFACT_VALID_ONLY`，不可 no-op/reuse |
| valid | valid | missing | yes，覆盖所有 required daily/minute/index/H5/static source values | `CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED` |
| valid | valid | present and equal | 全部 required、分块但全覆盖 validators PASS | `CURRENT_SOURCE_EQUIVALENT` |
| valid | valid | present and different | current validators pass | `ARTIFACT_VALID_SOURCE_CHANGED` |

“重建 provenance”只生成当前 re-attestation 的 source-equivalence receipt，不补写或伪造原 producer provenance。
完整 parity 必须覆盖 profile 全部 required component、日期、PIT 与 canonical source fields；无法值级读取的 legacy
格式不得用 shape/sample 代替，保持 `ARTIFACT_VALID_ONLY`。

旧候选可通过 exact path 执行只读 `catalog-existing`，只向 control store 写 artifact reference，不修改候选。
最低证据集为完整 artifact root、profile/cutoff/schema、PIT spans digest、moneyflow/index contract 和组件
manifest；缺少 source root 时不能升级到 current-source equivalent，除非当前 re-attest 执行完整值级
source parity 并生成重建 provenance。

`--latest` 只查询 control catalog，不扫描整个 candidate root；排序为 resolved cutoff、validated/attested time、
artifact root hash，完全并列返回 conflict 而非任意选择。re-attestation 只能证明旧字节满足指定合同，
不能伪称它由当前 producer 构建。

Attestation-only finalize 不进入 candidate publish protocol：先把 canonical attestation receipt 写入 control CAS
并 readback hash，再在一个 SQLite 事务中插入 attestation/ref、将 run outcome=`REATTESTED`、写 terminal event。
CAS 后事务前崩溃只留下无引用 content-addressed blob，可按相同 attestation key 幂等复用；整个路径没有
candidate staging、rename、committed marker 或旧候选写句柄。

## 9. Durable Repository、State Machine 与 Recovery

### 9.1 SQLite Schema Authority

`dataset_release_control_v1` 至少包含：

```text
schema_metadata(version, applied_at, code_compat_min, code_compat_max)
idempotency_keys(principal, route, key, request_hash, submission_id, response_ref)
submissions(submission_id, logical_request_key, request_ref, actor, state, row_version,
            intent_id NULL, run_id NULL, resolution_attempt_id NULL)
resolution_attempts(resolution_attempt_id, submission_id, logical_request_key, ordinal, state, owner, fence,
                    source_content_root NULL, source_provenance_root NULL,
                    pit_snapshot_digest NULL, source_probe_ref NULL, error_ref)
intents(intent_id, logical_request_key, resolved_intent_key, source_content_root,
        source_provenance_root, pit_snapshot_digest, supersedes_intent_id)
runs(run_id, intent_id, run_generation_digest, operation_kind, lineage_root_run_id,
     resume_ordinal, state, outcome, plan_ref, terminal_receipt_ref NULL, row_version,
     active_attempt_id, resumes_run_id NULL, publish_nonce NULL)
resume_lineages(lineage_root_run_id PRIMARY KEY, latest_run_id, next_ordinal, row_version)
attempts(attempt_id, run_id, ordinal, attempt_kind, state, owner, attempt_fence,
         host_fence, release_fence NULL, staging_ref, error_ref)
events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, submission_id NULL,
       resolution_attempt_id NULL, run_id NULL, attempt_id NULL, type, payload_ref, created_at)
leases(resource_key PRIMARY KEY, fence_counter, state, attempt_kind, attempt_id,
       owner_identity, heartbeat_at, expires_at)
commands(command_id, target_type, target_id, submission_id NULL, run_id NULL,
         type, request_hash, state, actor, created_at, applied_at)
artifacts(artifact_id, kind, sha256, size, cas_ref, producer_attempt_id, committed)
releases(release_digest PRIMARY KEY, release_id UNIQUE, candidate_identity UNIQUE, run_id UNIQUE,
         profile, scope, cutoff, artifact_root, pit_snapshot_digest, final_path_identity,
         marker_ref, attestation_id, state)
attestations(attestation_id, attestation_key UNIQUE, subject_type, subject_digest,
             candidate_identity NULL, producer_provenance_state,
             producer_provenance_digest_or_sentinel, candidate_artifact_root,
             current_source_content_root, source_probe_key, source_probe_ref,
             pit_snapshot_digest, semantic_profile_digest, validation_fingerprint,
             observed_at, valid_until, equivalence_mode, outcome, receipt_ref)
publish_records(release_id PRIMARY KEY, release_digest, run_id, attempt_id,
                attempt_fence, host_fence, release_fence, publish_nonce,
                published_by_attempt_id, published_by_fence,
                finalized_by_attempt_id NULL, finalized_by_fence NULL,
                state, manifest_root, artifact_root, pit_snapshot_digest,
                build_receipt_ref, attestation_key, attestation_ref,
                source_probe_key, source_probe_ref, final_path_identity, marker_ref)
```

`runs(intent_id,run_generation_digest)` 是永久 UNIQUE，不只约束 non-terminal；no-op concurrent/recovery 命中时
必须逐项核对 operation kind、receipt、candidate/probe/attestation refs 才能链接，否则 `IDENTITY_CONFLICT`。
另建 partial UNIQUE `runs(lineage_root_run_id) WHERE state IN (<all non-terminal states>)`，与
`resume_lineages` row-version CAS 双重保证每个 lineage 只有一个 non-terminal build/resume。
`releases` 是 candidate catalog authority；只有与 committed publish record、完整 marker 和 attestation 同事务一致
的行可被 discovery。上述 identity/digest/ref 列不得以空字符串或 zero hash 代替缺失证据。

resolution claim 还必须在同一事务获取唯一 `leases.resource_key=resolution:<sha256(logical_request_key)>`，并有
partial UNIQUE `resolution_attempts(logical_request_key) WHERE state IN (CLAIMED,RUNNING,ORPHAN_HOLD)`。
不同 Idempotency-Key 的 submission 命中同 logical request 时只有一个 leader 能 claim；其余保持 queued/等待并在
leader 释放后链接已解析 intent/run 或重新做 freshness-required resolution，绝不并发读取/冻结两个 source snapshot。

所有 mutable status/event/lease/idempotency 写入同一 SQLite 事务；payload 大于上限时先写 CAS，readback
hash 后再提交 ref。schema upgrade 只由显式 `init-control-store --migrate --expected-version` 执行，
backend/Worker 对未知或不兼容版本返回 `CONTROL_STORE_SCHEMA_MISMATCH`。

### 9.2 Atomic Commit Protocol

1. CAS：写同卷 temp，flush file，atomic rename/create-if-absent，flush directory/Windows equivalent，
   reopen 并核对 size/hash；相同 digest 异 bytes 为 corruption。
2. API submit：`BEGIN IMMEDIATE`，校验 idempotency，插 submission 与 submission event，commit；
   此时 intent/run 均为空，crash 前事务回滚。
3. No-op finalize：resolution Worker 先将 canonical `dataset_release_noop_receipt_v1` 写入 control CAS；receipt
   绑定 resolved intent、candidate identity/artifact root、fresh source probe receipt/key、exact validation
   identity/attestation key 与 decision schema，明确排除 attempt/lease/resolution fence 等 ownership 字段。随后
   canonical body 固定为 schema version、run generation、resolved intent、candidate/artifact、probe key/ref、
   attestation key/ref、semantic profile、validation fingerprint、decision schema 和
   `outcome=NO_OP_VERIFIED`；不得加入 owner、attempt/fence 或 finalize wall-clock。
   单个 `BEGIN IMMEDIATE` 事务校验当前 resolution attempt/fence、probe
   尚未过 TTL、source content root 与 PIT snapshot digest 均未变、attestation 对
   candidate/artifact/source/PIT/validation current-source-equivalent、submission resolution ownership
   与唯一 generation 约束，创建 `operation_kind=NO_OP,state=SUCCEEDED,outcome=NO_OP_VERIFIED` 的 terminal run，
   将 submission 置 `RESOLVED_NO_OP` 并链接 intent/run/receipt，同时写 submission/run terminal events。
   同一事务还将 resolution attempt=`RELEASED_SUCCEEDED`、清空
   `submission.resolution_attempt_id` 并释放该 attempt 持有的全部 resolution/host lease。该路径不创建 build
   attempt 或 build host/release lease、candidate staging、publish record、final marker，也不写 candidate。
   ownership attempt/fence 只进入 SQLite event，不进入 semantic receipt。CAS 后 DB 前崩溃只留下不可发现的
   orphan CAS；新 fence 的幂等 retry 可复用相同 semantic digest。并发 resolver 若命中已提交的同一 fresh
   generation，只链接 existing run；TTL 到期后 generation 不兼容，必须形成新 probe/no-op run。
4. Worker claim：`BEGIN IMMEDIATE`，验证 expected state/row_version，递增 fence，插 attempt/lease/event，commit。
5. Artifact：大型 candidate staging 位于 `candidate_root/.staging/<attempt>/<fence>`，与 final candidate 同卷；
   control CAS staging 位于 control root。每个 checkpoint 先验证当前 lease/fence，再写 artifact/ref/event。
6. Prepare publish：`VALIDATING` 必须先做 fresh source/PIT readback，证明 partition roots、provider snapshot
   tokens 与 frozen PIT spans digest 仍等于 resolved intent；drift 时在 publish commit point 前 terminal
   `BLOCKED_SOURCE_REVISED` 并由 resolution 形成 superseding intent，不把旧 snapshot 伪称“当前”。相等时用实际 artifact/source/PIT roots 生成
   `dataset_release_attestation_v1(outcome=CURRENT_SOURCE_EQUIVALENT)`，先写 control CAS/readback；build receipt
   不能替代 attestation。事务将 run 置 `PREPARING_PUBLISH`，绑定 attestation hash；检查最后一次 cancel 后，
   再以单一线性化事务创建唯一 publish nonce 与 `publish_records=PREPARED` 并进入 `PUBLISHING`，绑定 manifest
   root/fence/final path identity。该事务提交即 publish commit point；之后 parent-only publisher 才能操作 final path。
7. Filesystem publish：核对 current fence，atomic rename 同卷 candidate staging 到 deterministic final path，
   将 canonical marker bytes（含 payload length/hash）写入 final dir 同卷
   `.committed.<publish_nonce>.<attempt>.tmp`：`CREATE_NEW -> write-all -> FlushFileBuffers -> close`，再以
   create-if-absent atomic rename/`MoveFileEx(...,WRITE_THROUGH)` 到唯一 committed marker，flush parent-directory
   equivalent；随后 reopen 并核对完整 length/hash/fields。marker payload 为
   `{release_id,release_digest,publish_nonce,manifest_root,artifact_root,pit_snapshot_digest,attestation_key,
   attestation_receipt_digest,published_by_attempt/fence}`，readback PASS 后才将 publish record 置
   `FILES_COMMITTED`。candidate discovery 必须同时要求 catalog record 和 marker 完全匹配。
   crash 只能留下不可发现的 partial temp 或完整 committed marker，不能留下 partial committed marker；recovery
   忽略 temp 作为 authority，重新核对 final tree 后可用新 temp 幂等创建同一 marker。matching orphan temp 可保留
   到精确 maintenance，自动流程不删除；已存在 committed marker 仅接受 canonical bytes/hash 完全一致。
8. Finalize：正常 owner 的 SQLite 事务核对 `FILES_COMMITTED`、marker、current fence，将 candidate catalog、
   artifacts、build-produced current-source-equivalent attestation committed，run terminal、publish record=`COMMITTED`，
   attempt=`RELEASED_SUCCEEDED`，清 `run.active_attempt_id`，并 CAS 释放适用 host/release leases 后写 terminal
   event。candidate discovery/no-op 必须要求该 attestation reference。崩溃后 recovery 对
   `PREPARED` 无 final path重做 publish；
   对 `FILES_COMMITTED` 或 valid final marker 已存在的记录只做 readback 后幂等 finalize。
   `PREPARED + final path exists + marker missing` 时，新 recovery attempt 必须先证明整个旧 process tree
   quiescent，再在事务中接管同 publish nonce、更新 publish fences；若 final tree 完整匹配 manifest，可原子
   创建 marker并继续 finalize；任何缺件、partial/invalid marker 或 hash mismatch 返回
   `PUBLISH_FINAL_PATH_CONFLICT`，不删除、覆盖或自动重命名 final。
   marker 永久记录 `published_by_attempt/fence`，不要求它等于后续 recovery fence。marker 已 valid 但 DB 未
   finalize 时，recovery 在确认旧 process tree quiescent 后，以 CAS 接管 `finalized_by_attempt/fence`，保持
   publish nonce、manifest root 与 published-by 不变；terminal transaction 校验 immutable marker/publisher
   identity和当前 finalizer fence。这样新 fence 可完成 DB finalize，而不会改写 marker 或伪造原 publisher。
   从 publish commit point 起，cancel、通用 retry exhaustion、`FAILED_TERMINAL` 和新 run resume 全部禁止；
   transient I/O/owner loss 只能让同一 run/publish nonce 进入 `WAITING_PUBLISH_RECOVERY`，旧 process tree
   quiescent 后由新 attempt/fence 接管并回到 `PUBLISHING`。valid marker 只能幂等 finalize 成功；只有 immutable
   final tree/marker 与 prepared identity 不一致时才 terminal `BLOCKED_PUBLISH_CONFLICT`，并要求独立、精确目标的
   operator repair，不允许新 run 覆盖该 final path。
9. Projection：`run_state.json` 由 event reducer生成，含 last_event_id/root hash；损坏时从 events 重建，
   不能反向更新 control DB。

SQLite 与 filesystem 不宣称单事务原子；上述 prepared/nonce/marker/recovery protocol 明确关闭崩溃窗口。
orphan temp/staging 没有 committed catalog record，不会进入 discovery。corruption 移入 quarantine 需要精确 artifact
target；任务自动流程只标记 quarantine reference，不递归删除。control store 损坏时停止新 claim，保留原文件，
通过 SQLite integrity check、CAS readback 和 event replay 生成独立 recovery receipt。

### 9.3 Entity States 与 Transition Table

decision/action plan 是 immutable artifact，不是 state。submission resolution、run 与 attempt 分开。

Submission：

| Current | Next | Owner/condition |
|---|---|---|
| `QUEUED_RESOLUTION` | `RESOLVING_SOURCE` | resolution Worker claim + resolution fence |
| `RESOLVING_SOURCE` | `WAITING_SOURCE` | retryable source/provider condition，release resolution lease |
| `RESOLVING_SOURCE` | `FAILED_RETRYABLE` | transient DB/provider/I/O；bounded retry |
| `FAILED_RETRYABLE` | `QUEUED_RESOLUTION` | retry budget and next_retry_at |
| `FAILED_RETRYABLE` | `BLOCKED_RETRY_EXHAUSTED` | retry budget exhausted |
| `RESOLVING_SOURCE` | `BLOCKED_PROVIDER_TERMINAL` | 40203、provider conflict/incomplete |
| `RESOLVING_SOURCE` | `BLOCKED_CONTRACT` | schema/PIT/identity/semantic terminal error |
| `WAITING_SOURCE` | `QUEUED_RESOLUTION` | reconcile after `next_retry_at` |
| `WAITING_SOURCE` | `BLOCKED_SOURCE_TIMEOUT` | bounded deadline |
| `RESOLVING_SOURCE` | `WAITING_ACTIVE_RUN` | revision found but same logical request has non-terminal build |
| `WAITING_ACTIVE_RUN` | `QUEUED_RESOLUTION` | active run terminal 后重新 probe |
| `RESOLVING_SOURCE` | `RESOLVED_TO_EXISTING` | equivalent intent/run exists |
| `RESOLVING_SOURCE` | `RESOLVED_NO_OP` | no-op receipt CAS 后，同事务创建 terminal run、链接 submission/run 并写双实体 events |
| `RESOLVING_SOURCE` | `RESOLVED_NEW_RUN` | intent/run created in same transaction |
| any unclaimed queued/waiting | `CANCELLED` | control service directly writes cancellation receipt/event |
| owned resolution | `CANCEL_REQUESTED` -> `CANCELLED` | resolution Worker checkpoint |
| owned resolution lease expired with child alive/unknown | `WAITING_ORPHAN_QUIESCENCE` | no reclaim/no kill |
| `WAITING_ORPHAN_QUIESCENCE` | `QUEUED_RESOLUTION` | full tree verified quiescent |

每个由 `RESOLVING_SOURCE` 离开的 owned transition（resolved existing/no-op/new run、waiting、blocked、cancel 或
retryable）必须在同一 SQLite 事务终结/释放当前 resolution attempt、清空
`submission.resolution_attempt_id`、释放全部 resolution/host lease 并写 attempt+submission event。terminal 或
unowned waiting submission 带 active resolution pointer/lease 是 invariant violation；reconciler 不得把这种脏状态
当正常 expiry 继续处理。

Run：

| Current run state | Next state | Owner/condition | Terminal |
|---|---|---|---|
| `not_exists` | `SUCCEEDED` | resolution Worker；仅 fresh unchanged probe + current-source-equivalent attestation；原子创建 `NO_OP_VERIFIED` run | yes |
| `QUEUED` | `WAITING_RESOURCE` | resource preflight without heavy claim | no |
| `WAITING_RESOURCE` | `QUEUED` | hysteresis satisfied | no |
| `WAITING_RESOURCE` | `BLOCKED_RESOURCE_TIMEOUT` | bounded deadline | yes |
| `QUEUED` | `REATTESTING` or `EXECUTING` | build Worker claim + new attempt/fence | no |
| `REATTESTING` | `FINALIZING_ATTESTATION` | attestation receipt CAS staged；不使用 candidate publish | no |
| `FINALIZING_ATTESTATION` | `SUCCEEDED` | SQLite attestation/ref/outcome/event 原子提交 | yes |
| `EXECUTING` | `WAITING_RESOURCE` | safe checkpoint releases attempt and clears active attempt | no |
| `EXECUTING` | `WAITING_PERFORMANCE_REGRESSION` | normalized compute throughput 持续越过 30%/70% 门限；安全 checkpoint | no |
| `WAITING_PERFORMANCE_REGRESSION` | `QUEUED` | 有下一 pressure-ladder rung 且 DB/provider/resource preflight healthy | no |
| `WAITING_PERFORMANCE_REGRESSION` | `BLOCKED_PERFORMANCE_REGRESSION` | ladder/deadline exhausted；不缩业务范围 | yes |
| `EXECUTING` | `VALIDATING` | all required component manifests committed | no |
| `VALIDATING` | `BLOCKED_SOURCE_REVISED` | pre-publish fresh source/PIT readback 与 resolved intent 不同 | yes |
| `VALIDATING` | `PREPARING_PUBLISH` | candidate receipt/manifest staged | no |
| `PREPARING_PUBLISH` | `PUBLISHING` | final cancel check + publish record PREPARED 原子提交；publish commit point | no |
| `PUBLISHING` | `SUCCEEDED` | marker/catalog two-phase finalize | yes |
| `PUBLISHING` | `WAITING_PUBLISH_RECOVERY` | commit point 后 transient I/O/owner loss；保持同 run/nonce | no |
| `WAITING_PUBLISH_RECOVERY` | `PUBLISHING` | same active owner/attempt/fences healthy，或 old tree quiescent + new finalizer fence CAS adoption | no |
| `PUBLISHING` | `BLOCKED_PUBLISH_CONFLICT` | final tree/marker 与 prepared immutable identity 不一致 | yes |
| owned pre-publish-commit active | `CANCEL_REQUESTED` -> `CANCELLED` | Worker cooperative checkpoint | yes |
| unclaimed `QUEUED/WAITING_RESOURCE/WAITING_PERFORMANCE_REGRESSION` | `CANCELLED` | control service direct terminal cancellation | yes |
| owned pre-publish-commit active | `FAILED_RETRYABLE` | typed error, release attempt/lease | no |
| `FAILED_RETRYABLE` | `QUEUED` | retry budget; new attempt ordinal on next claim | no |
| `FAILED_RETRYABLE` | `BLOCKED_RETRY_EXHAUSTED` | retry budget exhausted | yes |
| any pre-publish-commit active | `BLOCKED_VERSION_MISMATCH` | code/schema capability incompatible | yes |
| any pre-publish-commit active | `FAILED_TERMINAL` | terminal contract/identity/corruption | yes |
| owned pre-publish-commit lease expired with child alive/unknown | `WAITING_ORPHAN_QUIESCENCE` | no reclaim/no kill | no |
| `WAITING_ORPHAN_QUIESCENCE` | `QUEUED` | full tree verified quiescent; reconcile checkpoint | no |

Attempt：`CLAIMED -> RUNNING -> RELEASED_SUCCEEDED | RELEASED_RETRYABLE | RELEASED_WAITING |
RELEASED_CANCELLED | ORPHAN_HOLD | EXPIRED | FAILED_TERMINAL`。claim 创建严格递增 ordinal；release 与 run transition 在
同一事务清除 `active_attempt_id`。expired attempt 不能恢复 ownership，只能由新 fence/new attempt 继续。

resolution attempt 使用同一生命周期并由 submission 的 `resolution_attempt_id` 指向。lease expired 时，
reconciler 先检查完整 process tree/WSL child liveness：全部 quiescent 才能在同一事务把 attempt=`EXPIRED`、
清 active pointer、父 submission/run 重新排队并写 recovery event；任一 child `alive/unknown` 则父实体进入
`WAITING_ORPHAN_QUIESCENCE`。发现 child alive/unknown 时，同一事务将 attempt=`ORPHAN_HOLD`，保留
`active_attempt_id` 与 host/release/resource lease 为 `ORPHAN_HOLD`，禁止新 heavy claim。只有完整 tree 被证明
quiescent，事务才将 attempt=`EXPIRED`、清 active pointer、释放 leases 并重新排队；因此不存在“terminal 但仍占
lease”或“释放 lease 后并发第二个 heavy run”的状态。orphan deadline 只产生 durable
`ORPHAN_TIMEOUT_EXCEEDED` health/event/告警，不把父实体置 terminal。build attempt 采用同样规则，并在新 attempt
只读核对旧 fence checkpoint 后决定 resume scope。

publish commit point 后采用专用 orphan handoff。owner/lease expired 且旧 tree alive/unknown 时，一个事务将旧
attempt 与 host/release leases 全部置 `ORPHAN_HOLD`，run=`WAITING_PUBLISH_RECOVERY`，保留
`active_attempt_id`/publish nonce/final path；不释放任一 lease、不增 fence。orphan deadline 仍只告警。

旧 tree quiescent 后，`BEGIN IMMEDIATE` 同时校验 run/publish identity、旧 attempt/pointer 与两把
`ORPHAN_HOLD` lease，把旧 attempt=`EXPIRED`，递增 host/release fence，创建
`attempt_kind=FINALIZER_RECOVERY` 的新 attempt，将两把 lease 原子转为该 attempt 的 ACTIVE ownership，更新
`active_attempt_id` 并回到 `PUBLISHING`；事务中间从不出现 FREE lease。当前 owner 的 transient I/O 可保持原
attempt/leases 在 `WAITING_PUBLISH_RECOVERY`；只有 expected row/state、active attempt、owner identity、heartbeat、
host/release ACTIVE leases/fences、publish nonce/manifest/marker readback 全部仍匹配且 transient condition cleared，
才能用同 attempt/same fences CAS 回 `PUBLISHING`，不递增 fence。该 run 不得回 `QUEUED`、retry exhausted 或创建新 build；
不启动第二 publisher、不由 reconciler kill，只有 immutable identity conflict 才走 `BLOCKED_PUBLISH_CONFLICT`。

terminal run 不原地 resume。`POST /resume` 校验原 plan/source compatibility后创建 `resumes_run_id` 指向原 run
的新 run；若 source content/PIT root 已变，则先创建 superseding intent。成功旧 run 保留原 outcome，不改写为
SUPERSEDED，intent linkage 记录 revision lineage。publish commit point 之后的 run 不是普通 terminal-resume：
API 只能返回/触发同 run 的 `FINALIZER_RECOVERY`，不得创建新 build run。

每条 owned transition 需要 `(run_id, expected_state, expected_row_version, attempt_id, fence)`；unclaimed control
transition 需要 expected state/row version 且 `active_attempt_id IS NULL`。0 row update 是 `STATE_CONFLICT`。
run outcome 只在 terminal state 写入。`not_exists -> SUCCEEDED` 是唯一允许绕过 active states 的路径，只能用于
上述 no-op 原子建档，且 `attempt_id/lease/publish_record` 必须为空。`worker_unavailable` 是 control-plane health
projection，不是 run state。

除 `ORPHAN_HOLD` 外，每个 owned build transition 到 waiting/retry/terminal（reattest success、resource/performance
waiting、cancel、blocked/failed、publish success/conflict）都必须在同一 SQLite 事务：校验 run/attempt/全部 fences，
写 attempt release state，清 `run.active_attempt_id`，CAS 释放全部适用 host/release leases，并写 run+attempt events。
任何 terminal run 保留 ACTIVE lease/pointer 都是 invariant violation；reconciler 先 fail closed/report，不把它当可用资源。

### 9.4 Lease、Fencing 与 Stale Child

host resource lease 至少包含：

```text
resource_key, run_id, attempt_id, host, owner_pid, owner_create_time,
worker_instance_id, code_sha, capability_digest, attempt_fence, host_fence, release_fence,
acquired_at, heartbeat_at, expires_at, requested_ram, db_connections, io_class
```

heartbeat interval、lease TTL 与 claim recovery 比例由 profile 冻结，要求 `TTL >= 3 × heartbeat interval`。
时钟 authority 为 control host UTC。liveness 覆盖 parent、记录的 Windows child identities 和 WSL child marker，
返回 `alive | dead | unknown`：只有 lease expired 且整个 process tree=`dead/quiescent` 才能自动 reclaim；
任一 child alive/unknown 进入 `WAITING_ORPHAN_QUIESCENCE`，并以 reason code 区分
`ORPHAN_PROCESS_ACTIVE | OWNER_LIVENESS_UNKNOWN`。
reconciler/API 不杀 orphan；在其自然退出并被证实前不启动新的 heavy run。唯一 fail-stop 例外是从创建时即
属于本任务 Job/cgroup 的 child 遇到 OS hard memory limit 或 resource supervisor 非正常丢失 Job handle；该机制
不能附加到、枚举或终止任务外进程，并产生 `RESOURCE_ENFORCEMENT_FAIL_STOP` receipt。

`fence_counter` 永不删除或归零。claim 在 `BEGIN IMMEDIATE` 中对 resource row 执行 compare-and-swap：
仅 `FREE`，或 `ACTIVE + expired + owner dead` 可将 counter 加 1，并绑定新 attempt/owner；heartbeat/release
必须 `WHERE resource_key=? AND attempt_id=? AND owner_identity=? AND fence_counter=? AND state='ACTIVE'`，
row count 不是 1 即 stale-owner conflict。release 清空 owner 但保留 counter/history event。

build claim 在同一 SQLite 事务按固定顺序获取 `host:heavy-dataset` 和 `release:<release_id>` 两把 lease；
任一不可用则整个事务回滚，不保留半把 lease。attempt 分别保存 `host_fence` 与 `release_fence`：资源
admission/heartbeat 校验 host token，candidate checkpoint/publish 校验 attempt+release token，terminal transition
同时校验全部适用 token。resolution/source acquisition 在需要 provider/DB heavy class 时只获取 host lease，
final release identity 尚未解析前不得伪造 release fence。

每个 attempt/fence 使用独立 staging；所有 checkpoint、append、CAS ref 和 publish 都重新验证 fence。
父 Worker 死亡后仍存活的 WSL/子进程最多污染旧 fence staging，不能写 final candidate 或新 attempt staging。
release identity 也使用 `resource_key=release:<release_id>` 的同一持久 lease/fence 协议；lock file 仅为镜像，
不得手删作为恢复方式。publish capability 是 parent 内存中的 nonce/fence，不进入 child env/argv；child 只写
staging 并返回 manifest。parent publisher 再次验证 lease、fence、publish nonce 后才能触及 final path。

### 9.5 Cancellation

API/CLI 只在事务中持久化 `cancel_requested` command。resolution 前 command 以
`target_type=submission,target_id=submission_id` 寻址；run 创建后使用 `target_type=run`，两者不得依赖空 run_id。
Worker 在 resolve、provider request、stage、date chunk、
code batch、materialization 和 validation checkpoint 检查；完成当前原子单元后写 cancellation receipt 并退出。
`PREPARING_PUBLISH -> PUBLISHING` 事务在写 PREPARED record 前执行最后一次 cancel CAS；事务提交后 cancel
command state=`REJECTED_TOO_LATE`，不再改变 run outcome，同时写 `CANCEL_DEFERRED_PUBLISH_COMMIT` event 并等待
同一 run 幂等 finalize/recovery。cancel 先获得 SQLite 写锁并提交时，publish 事务 CAS 失败且不得写 final path；
publish commit point 先提交时，cancel 只能走上述 too-late 分支。
强制 kill、服务停止、删除 lock 或清理 staging 不属于本 API。

## 10. Resource、Performance 与 Logging 契约

### 10.1 Resource Budget

每个 stage 冻结：启动最小 host available/system commit headroom、运行 emergency headroom、Windows Job
private commit、WSL cgroup memory/swap、process-tree RSS telemetry、DB pool/statement timeout、batch/chunk 大小、
X 盘最小空间、等待超时。

`qe_hmm_full_v1` 首版默认值与 hard maximum 固定如下（`GiB=2^30`）。profile 可以降低并发/块大小、提高
available-memory reserve；超过 hard max 或降低 reserve 必须拒绝加载，不允许以 CLI/env 临时绕过。调整这些
边界需要 versioned resource contract、相同 workload benchmark 与 `on_contract_change` sample，但不改变数据 bytes identity。

| Resource | Default | Hard/safety boundary |
|---|---:|---:|
| 同 host heavy full run concurrency | 1 | 1 |
| aggregate owned private commit（Windows Job commit + WSL cgroup `memory.current`） | cap 12 GiB | 12 GiB |
| Windows-only stage Job commit | cap 8 GiB | `JOB_OBJECT_LIMIT_JOB_MEMORY=8 GiB` |
| hybrid/WSL stage Windows-side Job commit | cap 4 GiB | `JOB_OBJECT_LIMIT_JOB_MEMORY=4 GiB` |
| WSL task cgroup `memory.high/max/swap.max` | 6 / 8 / 0 GiB | 6 / 8 / 0 GiB |
| host start `MemAvailable` | >=16 GiB | 不得低于 16 GiB |
| host emergency `MemAvailable` | 8 GiB | 不得低于 8 GiB |
| host start/emergency system commit headroom | 16 / 8 GiB | 不得降低 |
| WSL start `MemAvailable`（WSL stage） | >=12 GiB | 不得低于 12 GiB |
| WSL emergency `MemAvailable`（WSL stage） | 6 GiB | 不得低于 6 GiB |
| DB pool / simultaneous row-producing query | 4 / 1 | 4 / 1 |
| DB statement timeout | 300 s | 300 s；超时 typed retry，不扩大 |
| provider request concurrency | 1 | 1 |
| Qlib dump workers | 8 | 8，仍受 owned-footprint cap |
| minute code batch / date chunk | 20 / 3 months | 20 / 3 months |
| H5 load batch / date chunk | 100 / 3 months | 100 / 3 months |
| Parquet row group / validation read chunk | 100,000 / 100,000 rows | 100,000 / 100,000 rows |
| enforcement sample / receipt rollup / wait deadline | 1 s / 5 s / 3,600 s | sample <=1 s；deadline <=3,600 s |
| candidate free-space start reserve | max(32 GiB, 1.25x predicted new bytes) | 不得降低 |

所有 data-bearing resolution/build/validation helper 必须由 task-owned resource supervisor 以 suspended state
创建，先加入 non-breakaway Windows Job 后才 resume；Windows-only 子 Job commit cap=8 GiB，WSL/hybrid
Windows-side 子 Job cap=4 GiB。supervisor 持有 root/child Job handles，启用 `JOB_OBJECT_LIMIT_JOB_MEMORY`、
`JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION` 与 task-only `KILL_ON_JOB_CLOSE`；它只对 Job 内 Windows child
（包括 launcher `wsl.exe`，但不假设能终止 Linux child）提供 owner-death fail-stop。hard cap/handle loss 绝不枚举、
终止或调整任务外进程。无法 assign/query Job、
发现 breakaway 或 supervisor 不存活时，在任何 source query 前 `BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE`。

WSL helper 必须由 repo-owned `wsl_resource_guardian.py` 作为 transient `systemd --user` service MainPID 启动，
unit 名绑定 attempt/fence；unit 冻结并 readback `MemoryHigh=6G,MemoryMax=8G,MemorySwapMax=0,
KillMode=control-group,SendSIGKILL=yes,CollectMode=inactive-or-failed`。guardian 在该 cgroup 中启动 heavy child，
监控 Windows supervisor 每秒原子更新的 `{attempt_id,fence,counter,host_utc}` heartbeat 与 pipe EOF；TTL（>=3 个
heartbeat interval）过期、identity/fence drift 或 pipe closure 时 guardian nonzero 退出，systemd 按
`KillMode=control-group` fail-stop 该 unit 的所有 task-owned Linux descendants。Windows host suspend/guardian
不确定时宁可停止本 task，不允许 Linux child 无约束继续。

unit cgroup 的 `memory.high/max/swap.max/oom.group=1`、MainPID、ControlGroup、`memory.events/current/peak` 必须
readback 并进入 receipt；没有可用 user systemd、delegated cgroup v2、heartbeat/pipe guardian 或无法证明所有
PID 属于该 unit 时，在 query/export 前 `BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE`。reconciler 仍实际检查
Linux PID/create-time/cgroup quiescence，不能因“应该已被 guardian 停止”而提前释放 lease。
Windows Job commit 与 WSL `memory.current` 相加为 aggregate owned private commit；Windows owned counters 明确
排除 `vmmemWSL`，避免双算。RSS/working set 继续记录但不作为唯一内存安全证据。

system gate 用 `GetPerformanceInfo`/等价 Win32 API 采集 `CommitTotal/CommitLimit`，并读取
`LowMemoryResourceNotification`、Available Bytes、Page Reads/sec 与 pagefile used/limit。start 必须同时满足
available 和 commit headroom >=16 GiB 且无 low-memory signal；运行中任一 available/commit headroom <8 GiB、
low-memory signal，或 `Page Reads/sec>=256` 连续 3 个 1 秒样本且 available/commit headroom 任一 <12 GiB，
都触发 emergency。Job/cgroup 是连续硬限，1 秒采样只做提前 checkpoint 与系统压力判定，不承担防突增的唯一责任。
每个 SQL/provider response 在进入 transform 前即受 batch/chunk 硬上限，禁止先物化超大 frame 再切块。

Worker 记录：

- Windows Job current/peak private commit、parent/child private bytes 与 RSS/working-set peak；
- host available/min available、CommitTotal/Limit/headroom、low-memory signal、Page Reads/sec 与 pagefile used/limit；
- WSL cgroup memory current/peak/high/max/events、swap current/max、WSL `MemAvailable`；
- DB query count、rows returned、statement time；
- X 盘 read/write bytes、artifact rows/bytes；
- compute time、resource wait time、provider wait time；
- 每 stage/chunk rows/s 和 resume/reuse 节省量。

等待时间与计算时间必须分开。资源门禁保护系统可能增加 wall-clock，但不能被误报为算法吞吐退化。

资源算法由 profile 冻结并遵循：

- enforcement sampling interval=1s、receipt rollup=5s；启动 available/commit 低于 stage threshold 时不 claim heavy stage，连续两次满足
  start threshold 才恢复，避免抖动；
- 运行中 host emergency、commit/pagefile gate 触发时在下一个强制 checkpoint 立即停 claim/停下一原子单元；
  Job/cgroup hard limit 由 OS 连续执行，allocation failure/OOM 直接使本 task attempt typed fail，不等待 checkpoint；
- WSL stage 必须同时取得 WSL `MemAvailable` 与 swap telemetry；采集失败为
  `BLOCKED_REQUIRED_TELEMETRY_UNAVAILABLE`，非 WSL stage 显式 `not_applicable`；
- `OMP_NUM_THREADS`、`MKL_NUM_THREADS`、dump workers、DB pool、statement timeout 和 provider 并发
  都来自 effective profile 并写入 plan/receipt；
- DB pool=4 是独立 monthly Worker pool，不复用 backend 主池；row-producing query semaphore=1 是进程内和
  repository lease 双重 hard limit；
- WAIT timeout 到期进入 terminal blocked，不杀进程、不降低数据范围；
- full 并发=1 是 host lease hard limit；sample/audit 是否可并发由 resource class matrix 明确配置，默认 0。

为避免一次压力尖峰让每月任务反复失败，resource contract 内置 deterministic pressure ladder，仅能在安全
checkpoint 后对下一 attempt 依次缩小物理执行单元：H5 batch `100->50->20`、minute batch `20->10->5`、
date chunk `3->1 month`、row/read group `100k->50k`、dump workers `8->4->2`。触发条件为连续两次
aggregate private commit >=85% cap 或 available/commit headroom 接近 emergency reserve 1 GiB；每次变化写 resource fingerprint/event
并通过同值 parity oracle。它不得减少股票、日期、字段、PIT、指数、H5 或验证范围，也不得扩大任一 hard max；
最低档仍 breach 才 checkpoint 后进入 typed `WAITING_RESOURCE/BLOCKED_RESOURCE_TIMEOUT`，不继续挤占内存。

性能 pass/fail 基于相同 semantic workload：source rows、instrument-days、component actions、cache/reuse 命中、
冷/热 cache 标签必须相同。源码合入门禁至少使用 checked-in synthetic/fixture data，对“未改 orchestration 的当前
资源有界 producer baseline”和新路径各运行至少 3 次，比较相同 cache class 的 median：

- `compute_seconds_new <= 1.10 * baseline` 且 `rows_per_second_new >= 0.90 * baseline`；
- row-producing query count 不得增加超过 `max(2, 5%)`，no-op/reuse 的 materialization query count 必须为 0；
- peak aggregate owned private commit 必须 <=12 GiB，且相同 fixture 不得超过
  `max(1.10 * baseline, baseline + 256 MiB)`；RSS/working set 同时报告；
- resource/provider wait 不进入 compute regression，但分别报告；control-plane overhead 单次 run <=5 s；
- 任一阈值不满足即 F-029 FAIL；可通过减小块/并发修复，但仍须重新满足吞吐阈值，不能只牺牲速度换 PASS。

receipt 分开输出 query_count、compute_seconds、resource_wait_seconds、provider_wait_seconds、rows/s、read/write
bytes 和 peak memory。源码阶段若 synthetic workload 不可比，`benchmark_not_comparable` 是合入 blocker；真实 full
因本轮未授权只能保持 `runtime_real_data_evidence=not_run_not_authorized`，不能反向把 fixture PASS 说成 full 性能结论。
历史约 28 GiB private commit/27.15 GiB RSS 失控路径的 full 目标是 aggregate owned private commit <=12 GiB
（至少约 57% commit 降低）；Job/cgroup 是硬执行而非轮询愿望。该绝对 cap 是
运行 hard gate，真实 full 耗时仍须在下一次获授权月更按上述同 workload 口径复核，不用重导旧 cutoff 来造证据。
生产月更因 DB/I/O 抖动不以单窗口 10% 退化自动重配：同 stage/action/profile 按 rows/bytes 归一化，>10% 记录
warning；持续 15 分钟 compute throughput <70% baseline 或退化 >30% 才在安全 checkpoint 暂停为
`WAITING_PERFORMANCE_REGRESSION`。零进展 30 分钟、单 SQL 300 秒超时或资源 hard breach 立即停止当前 attempt。
恢复仍只可使用上述 pressure ladder，禁止用缺字段、缺历史或降低验证换取速度。

### 10.2 Bounded Logs

子进程 stdout/stderr 持续流式写文件并轮转；不得使用 `capture_output=True` 保存数小时完整日志。
API tail 限制 bytes/lines，event payload 不保存大日志或 secrets。

每段日志以 CAS segment + monotonically increasing generation 编号，默认 segment 上限 16 MiB；API 单次
最多返回 1 MiB、1,000 行。control root 达到 profile 容量水位时阻止新 heavy run 并返回
`CONTROL_ROOT_CAPACITY_EXCEEDED`；自动流程不删除候选或历史证据。日志/事件归档与精确 cleanup target
属于独立 maintenance 动作。

## 11. CLI Contract / 一键操作契约

普通操作者只需要：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py status --latest
rtk python scripts/update_backtest_dataset_monthly.py reattest-existing --latest
```

`monthly` 默认：

```text
profile=qe_hmm_full_v1
cutoff=auto-previous-month
reuse=auto
resume=auto
sample_policy=on_contract_change
activation=not_requested
node1=not_requested
db_repair=not_requested
restart=not_requested
cleanup=not_requested
```

高级 `plan/run/reuse/fetch-overlay/verify` 保留用于故障诊断，但不能成为普通月更必需步骤。

## 12. Worker、API 与 Scheduler Contracts / 契约

### 12.1 Worker

```text
scripts/dataset_release_worker.py --once
scripts/dataset_release_worker.py --drain
scripts/dataset_release_worker.py --serve
```

- `dataset_release_worker.py` 入口先作为 resource supervisor 建立 task-owned root Job，再启动轻量 control Worker；
  所有 data-bearing helper 只能经 supervisor suspended-create/Job-or-cgroup admission，control Worker 自身不得持有 panel；
- 默认不自动启动；缺少显式 control root/profile allowlist 时拒绝运行；
- claim 使用 lease/fencing；heartbeat 与 stage events 可跨 backend restart；
- Worker 调用 domain service/CLI，不复制 exporter 逻辑；
- Worker identity 冻结 `instance_id/host/pid/create-time/code SHA/schema capabilities/profile digests`；
- `--once` 最多 claim 一个 attempt；`--drain --max-jobs N` 处理有界数量后退出；`--serve` 持续 poll；
- SIGINT/SIGTERM 只设置 cooperative shutdown；正常 supervisor 不在 child tree quiescent 前关闭 Job handle。
  hard limit、unhandled supervisor loss 的 Job/cgroup fail-stop 只作用于 identity-bound task child，并写恢复证据；
- `--serve` 的 poll interval、resource class 和最大并发来自配置；
- health 以 durable heartbeat/last poll/capability digest 判断，TTL 过期为 stale；
- 退出不删除 candidate、event、lease history 或失败证据。

### 12.2 API

```text
POST /api/v1/dataset-releases/preview
POST /api/v1/dataset-releases/runs
GET  /api/v1/dataset-releases/submissions/{submission_id}
GET  /api/v1/dataset-releases/submissions/{submission_id}/events
POST /api/v1/dataset-releases/submissions/{submission_id}/cancel-request
GET  /api/v1/dataset-releases/runs
GET  /api/v1/dataset-releases/runs/{run_id}
GET  /api/v1/dataset-releases/runs/{run_id}/events
GET  /api/v1/dataset-releases/runs/{run_id}/log
GET  /api/v1/dataset-releases/runs/{run_id}/receipt
POST /api/v1/dataset-releases/runs/{run_id}/resume
POST /api/v1/dataset-releases/runs/{run_id}/cancel-request
```

写接口要求 operator authorization 与 `Idempotency-Key`。请求只接受注册的 profile ID、cutoff policy、
scope 和 candidate-only intent。API 不接受 shell、candidate root、production path、env file 或任意命令。
未运行 Worker 时返回 durable `QUEUED/worker_unavailable`，不得在 API 线程代跑。

`POST /runs` 为操作员友好命名，但返回 `submission_id`、`logical_request_key`、`run_id=null|resolved`；
resolution 前通过 submission endpoints 查询/取消，解析后响应和 event 提供稳定 run link。API 不把尚未存在的
run 伪造成 queued run。

认证使用独立 FastAPI dependency `require_dataset_release_operator`：secret 仅从
`DATASET_RELEASE_OPERATOR_TOKEN_FILE` 指定文件读取，constant-time compare；请求 actor 固定为已认证
principal，不信任客户端传入 actor 或反向代理 header。缺失/错误 token=401，已认证但 profile/scope
不允许=403；所有 GET/POST 端点都受保护并记录 actor。token 文件路径/rotation 属于 runtime config，
值不进入日志/receipt。该认证是技术访问控制，不是新增人工审批。

请求/响应使用版本化 Pydantic schema；scope 为 `sample|full` 枚举，profile 来自 server allowlist。
错误响应固定 `error_code/message/retryable/context_ref`，context 不回显 secrets/path outside allowlist。

分页合同：runs 按 `(created_at, run_id)` 降序；events 按 `event_id` 升序；`limit` 默认 50、最大 200，
返回 opaque `next_cursor/has_more`。log 使用 `(generation, byte_offset)` cursor 并执行 byte+line 双上限；
receipt/evidence 只读取 cataloged CAS ref，先做 path/reparse/symlink guard，不接受用户路径。
cursor 绑定 schema version、endpoint、principal、run/submission、filters 和排序；签名/字段不匹配返回
`DATASET_RELEASE_CURSOR_INVALID`，不得跨 endpoint 或日志 generation 复用。

### 12.3 Reconcile Schedule

可选 scheduler 由独立 Worker 的 `reconcile` mode 或外部调度器调用，不挂 FastAPI lifespan。它每天在
`Asia/Shanghai` 配置时刻执行一次轻量 reconcile：通过交易日历计算 previous-month cutoff，按 logical request 查询
validated/running/blocked 状态。它只创建 candidate intent，不执行 activation。服务停机后恢复时可补提交。
已有 logical request 时：fresh content probe 未过 TTL 且 root 未变才 no-op；probe stale/missing 时创建或复用
唯一 `SOURCE_REVISION_PROBE` submission；已有 non-terminal probe 则链接它，不重复提交。多 instance 通过 SQLite singleton lease 去重。catch-up 最多覆盖配置的
历史月份数，失败写 event 并做有界 retry。scheduler 默认关闭，启用属于独立 runtime 配置与启动授权。

## 13. Skill、Runbook 与 Signoff

Skill 只保留动作选择、安全边界、单一入口和结果解释，控制在 500 行以内。详细内容放到一层 references：

```text
references/monthly-workflow.md
references/fingerprint-and-reuse.md
references/resource-and-worker.md
references/release-receipt.md
references/index-hmm-contract.md
```

Skill forward-test 至少覆盖：普通月更、同 cutoff no-op、旧候选 re-attest、资源不足、40203、
source conflict、生产激活未授权。forward-test 只使用 fixture/mock control root，不运行真实数据任务。

Runbook 面向人类 operator，默认只展示一键命令和状态解释；完整 JSON 只落 artifact，不输出到终端。
signoff 由 CLI 自动生成并校验，不要求人工复制。

## 14. Error Taxonomy / 错误分类

| 类别 | 示例 | 策略 |
|---|---|---|
| retryable | 短暂 DB 断线、临时文件占用、provider 5xx | 有界 retry/backoff |
| waiting | host memory、WSL memory、X 空间暂不足 | `WAITING_RESOURCE` 到超时 |
| resource_hard | Job/cgroup commit/OOM、system commit/pagefile emergency、guardian loss | task-only fail-stop；保留 staging/checkpoint，按 pressure ladder 新 attempt |
| orphan_hold | expired owner 的 Windows/WSL tree仍 alive/unknown | 非终态持有 leases；只在完整 quiescence 后释放 |
| source_blocked | required source watermark/partition 缺失 | `WAITING_SOURCE/BLOCKED` |
| terminal_contract | schema/PIT/moneyflow/index/bin-H5 冲突 | fail fast |
| provider_terminal | 40203、重叠值冲突、240 根不完整 | fail fast，保留 pending scope |
| identity_conflict | intent/fingerprint/manifest/lease fencing 不匹配 | fail fast |
| cancelled | durable cancel request | checkpoint 后退出 |

禁止将 terminal error 转成 retry success，禁止通过换 provider、补零、前填、减组件或减股票掩盖错误。

## 15. Implementation Plan / 实施方案

### Batch A：Deterministic Core

- `backend/services/dataset_release/contracts.py`：版本化 identity/action/outcome/event/error/receipt schema；
- `intent.py`、`catalog.py`、`decision.py`：submission/logical/resolved identities 与 action plan；
- `fingerprints.py`、`source_manifest.py`、`dependency_graph.py`：canonical hash、PIT/QFQ/source invalidation；
- `attestation.py`、`signoff.py`：只读旧候选复验与独立 CAS receipt；
- `scripts/update_backtest_dataset_monthly.py`：`monthly/status --latest/reattest-existing/catalog-existing`；
- `configs/datasets/qe_backtest_monthly_v1.yaml`：semantic/resource/control profile keys。

### Batch B：Reuse、Incremental 与 Performance

- `copy_on_write.py`、`incremental.py`：sealed partition reuse、mutation set 与新 inode materialization；
- `monthly_backtest_dataset.py`：automatic resume/reuse 与 mixed component actions；
- `backend/qlib_exporter/authoritative_bin_exporter.py`：overlay index、chunk-local lookback、batch query；
- `scripts/export_qe_qlib_candidate.py`、`streaming_artifacts.py`：factor chunk/rolling checkpoint/debug shard reuse；
- `candidate_validation.py`、`scripts/validate_qe_qlib_candidate.py`：值级/分块/PIT/source-equivalence validator。

### Batch C：Lease、Worker 与 Resource Telemetry

- `control_store.py`：SQLite schema、事务、idempotency、events、commands、CAS refs；
- `cas_store.py`：create-if-absent、flush、atomic replace、readback/quarantine；
- `lease.py`、`state_machine.py`：host lease、heartbeat、fencing、transition reducer、stale recovery；
- `worker.py`、`resource_supervisor.py`、`windows_job.py`、`wsl_cgroup.py`、`wsl_resource_guardian.py`、`subprocess_runner.py`、
  `resource_budget.py`：claim/cancel、suspended admission、private commit/cgroup hard limit、system commit/pagefile、
  streamed log 与 process-tree telemetry；
- `scripts/dataset_release_worker.py` 与 `scripts/dataset_release_control_store.py`。

### Batch D：Backend Control Plane

- `backend/routers/dataset_releases.py`：preview/submit/status/events/log/receipt/resume/cancel-request；
- `backend/services/dataset_release/control_service.py` 与 `api_models.py`；
- `backend/deps.py`：`require_dataset_release_operator`；`backend/main.py`：router registration only；
- operator auth、profile allowlist、idempotency、cursor/bounded response；
- API contract tests；不启动真实 Worker/导出。

### Batch E：Skill、Runbook 与 Validation

- `.codex/skills/update-backtest-dataset/SKILL.md` 与 `agents/openai.yaml`；
- `.codex/skills/update-backtest-dataset/references/{monthly-workflow,fingerprint-and-reuse,resource-and-worker,release-receipt,index-hmm-contract}.md`；
- `.claude/skills/update-backtest-dataset/SKILL.md` 薄 pointer；
- `docs/operations/qe_backtest_dataset_monthly_update_runbook.md` 与现有 export guide 的稳定引用；
- `backend/tests/dataset_release/`、`backend/tests/routers/test_dataset_releases.py`、
  `backend/tests/scripts/test_update_backtest_dataset_monthly.py`；
- Skill quick validation、forward-tests、F2 matrix 与 PR evidence。

任何 batch 不得通过“后续再补”把设计 required 条目降级为完整交付。若实现必须拆 PR，当前 PR 的
acceptance matrix 必须明确只声明所覆盖批次，不能申报整个 F2 完成。

## 16. Verification Plan / 验证方案

本轮实现验证严格禁止真实数据导出和历史 candidate 修改。数据语义/构建测试使用 pytest temp directory
和合成 fixture；DB、TDX、Tushare、X/E candidate 使用 fake/contract adapter。平台原子性与进程治理不能
只靠 fake，另用本机 temp volume、runner-owned 子进程和只读 WSL telemetry 做隔离 smoke，且不调用 exporter。

### 16.1 Direct Tests

```powershell
rtk python -m pytest `
  backend/tests/dataset_release/test_intent_catalog.py `
  backend/tests/dataset_release/test_fingerprints.py `
  backend/tests/dataset_release/test_index_context.py `
  backend/tests/dataset_release/test_reattest_existing.py `
  backend/tests/dataset_release/test_incremental_planner.py `
  backend/tests/dataset_release/test_copy_on_write.py `
  backend/tests/dataset_release/test_control_store.py `
  backend/tests/dataset_release/test_state_machine.py `
  backend/tests/dataset_release/test_resource_lease.py `
  backend/tests/dataset_release/test_resource_budget.py `
  backend/tests/dataset_release/test_worker.py `
  backend/tests/routers/test_dataset_releases.py `
  backend/tests/scripts/test_update_backtest_dataset_monthly.py -q
```

覆盖重复提交、旧 receipt、validator 变化、历史修订、QFQ/PIT 传播、hardlink 保护、stale PID reuse、
fencing、backend restart、bounded log、typed errors、zero production/DB/process-control counters。

强制可证伪 oracles：

- row count 相同但历史值变化必须改变 source root 并阻断 no-op；
- source 在 plan、stream 中途和 digest 后发生修订时，artifact 与 actual digest 必须来自同一 tee 行流；
- PIT multi-span 内容变化但 count/max-date/source root 相同仍必须改变 resolved intent/release/candidate identity并失效；
- QFQ denominator 变化与 numerator-only 修订分别形成正确 action scope；
- moneyflow 5/20、PriceStrength 10、slow-static forward-fill、sector interval 的窗口边界正确；
- external writer 不能获得 hardlinked aggregate writable path；source Merkle 前后不变；
- legacy candidate 缺 PIT provenance 只能 `ARTIFACT_VALID_ONLY/BLOCKED_LEGACY_PROVENANCE`；PIT provenance
  有效但仅原 source/producer provenance 缺失时，必须 full-profile current-source value parity PASS 才允许
  `CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED`，否则仍为 artifact-only；
- candidate shape 正确但值错误必须 re-attest FAIL；
- SQLite crash points、partial CAS、event projection rebuild、concurrent CAS、idempotency 异 payload冲突；
- build publish 必须在 final fresh source/PIT probe 后生成 attestation；release/catalog/attestation/publish/run
  在同一 terminal transaction 可见，TTL 到期后不得把 build attestation 当 fresh no-op；
- publish 在 PREPARED、rename 后 marker 前、marker 后 DB finalize 前崩溃的同 run/nonce 幂等 recovery；
- marker temp partial/flush/atomic-rename 每个 crash point 都只能得到 absent 或完整 marker；partial temp 不得使
  recovery 进入永久 final-path conflict；
- cancel 与 publish commit point 并发时只能有一个 SQLite 顺序：cancel-first 不写 final path，publish-first
  command=`REJECTED_TOO_LATE`；commit point 后不得 generic retry/terminal/new-run resume；
- submission resolution 无 run、resolved-to-existing、新 intent、WAITING_ACTIVE_RUN 和 source revision probe；
- fresh unchanged probe 必须原子产生可查询的 `SUCCEEDED/NO_OP_VERIFIED` run、submission/run 双事件与
  submission→run 链接；semantic receipt 排除 fence，CAS-before-DB crash 可跨新 fence 幂等恢复，TTL 过期后
  必须形成新 generation；
- host/release 双 lease 原子 claim、独立 token 校验、orphan child 阻断 reclaim；
- every owned resolution/build exit 原子释放 attempt/pointer/全部 leases；orphan deadline 只告警并保持
  `ORPHAN_HOLD`，直到 Windows+WSL tree quiescent 才释放；
- 不同 idempotency keys 并发 resume 只能链接同 active lineage 或 409；不得创建两个 non-terminal resume；
- publish 后 owner loss 必须把 attempt/pointer/host+release leases 原子转入 `ORPHAN_HOLD`，quiescent 后在
  同一事务 fence++ 并交接给唯一 `FINALIZER_RECOVERY`，lease 不出现 FREE 窗口；
- resource profile 对 >12 GiB aggregate private commit、Windows Job 8/4 GiB、WSL high/max/swap 6/8/0、
  低于 host available/commit 16/8 GiB 或 WSL 12/6 GiB reserve、超过 chunk/query/concurrency hard max 全部拒绝；
  small-cap Job/cgroup allocation、system commit/pagefile emergency 在 runner-owned fixture 可证伪；
- 相同 synthetic workload 三次 median 必须满足 compute<=110%、throughput>=90%、query 与 peak RSS 阈值；
  `benchmark_not_comparable` 在源码合入门禁必须 FAIL；
- 每个 non-terminal state 的 cancel/retry/timeout/version mismatch transition；
- expired lease + PID reuse、owner unknown、stale child fence publish rejection；
- runs/events/log cursor、limit、稳定排序、rotation generation 与 path traversal/reparse negative tests；
- scheduler source revision、catch-up、multi-instance singleton 与同 logical request 去重。

### 16.2 Static/Contract Gates

- changed-file Ruff/compile；
- direct pytest；
- `git diff --check`；
- F2 feature validator；
- Skill quick validator 与 metadata 一致性；
- route/profile/command allowlist 与 production path negative tests；
- DESIGN-COMPLIANCE-001 item-by-item review。

建议命令：

```powershell
rtk ruff check <changed-python-files>
rtk python -m compileall <changed-python-packages>
rtk git diff --check
rtk python scripts/aistock_feature_workflow.py validate `
  --design docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md --tier F2
```

### 16.3 Isolated Platform Gates

```powershell
rtk python -m pytest backend/tests/dataset_release/test_windows_control_store_platform.py -q
rtk python tests/aistock_validation/dataset_release_platform_smoke.py `
  --temp-root <runner-owned-local-volume-dir> --read-only-wsl-telemetry
```

验证真实 SQLite WAL/`synchronous=FULL`、same-volume atomic replace、hardlink/COW、reparse/UNC rejection、
CAS crash recovery、parent/child PID+create-time、stale fence publish rejection；以小型测试上限验证 suspended
Job assignment/non-breakaway、Job private-commit allocation failure/PeakJobMemoryUsed、guardian-loss task-only fail-stop、
WSL transient user service/cgroup v2 memory.high/max/swap.max/memory.events；终止 runner-owned heartbeat writer 后
必须观察 guardian exit、unit control-group 全部 descendants quiescent，再验证 mockable
GetPerformanceInfo/low-memory/Page Reads gate。
脚本只能创建/终止自己启动且明确标记 runner-owned 的临时子进程；
不得启动后端、Worker serve、Qlib exporter，或访问 X/E/production candidate。

若目标发布平台是 Windows+WSL，上述 Windows smoke 和 WSL telemetry 必须在 merge 前有 receipt；普通 Linux CI
可标记 platform plan `not_applicable`，但不能替代目标主机证据。环境缺 WSL 时结论为 `BLOCKED_BY_ENV`，
不得静默用 mock PASS。目标平台 receipt 通过后源码状态才可称 `source_ready_platform_verified`。

### 16.4 Deferred Real Data Evidence

真实 full、新 cutoff 增量和月度性能 telemetry 只能在用户未来明确授权数据更新后产生。
本实现 PR 不以缺少真实导出为代码未完成借口，但也不得把 fixture 结果伪称为 full 数据证据。
下一次真实月更优先 re-attest 既有 full 或处理新 cutoff，不为验证代码重导旧 cutoff。

实现 PR 可达到 `source_ready_fixture_verified`；`runtime_real_data_evidence` 保持 `not_run_not_authorized`。
只有未来真实 candidate receipt 才能升级运行时状态，不影响本轮源码是否满足设计，但必须在汇报中分开。

## 17. Rollout / Rollback / 发布与回滚

### 17.1 Source Rollout

1. 设计 PR 独立合入；不改变运行时。
2. 实现 PR 合入只表示 source ready；不安装 client、不启动 Worker、不注册 scheduler。
3. 后续 runtime activation 分别处理 backend reload、Worker 注册、operator secret 和 scheduler enable。
4. 首次 operator 使用仍为 candidate-only；production pointers 全部 `not_requested`。

### 17.2 Source Rollback

实现回滚只回退源码/配置，不删除 control root、candidate、receipt 或 attestation。
未完成 Worker 任务在旧源码不可安全恢复时进入 `BLOCKED_VERSION_MISMATCH`，由兼容版本处理，
不得手改状态或重导旧数据。

数据 activation/rollback 不属于本文实现范围；未来仍只接受明确 receipt SHA、target 和 expected current pointer。

## 18. Risks / Failure Modes / 风险与失败模式

| 风险 | 失败方式 | 控制 |
|---|---|---|
| 一键命令隐藏错误 | 自动流程看似成功但组件缺失 | typed decision、required receipt、无 silent fallback |
| fingerprint 过粗 | 文档变化触发全量重导 | 组件级分层 digest |
| fingerprint 过细 | 真实公式依赖未失效 | owned dependency manifest + negative tests |
| 历史修订未发现 | 只追加新月份产生错误历史 | source partition content manifest |
| hardlink 被原地修改 | 旧 candidate 被污染 | copy-on-write、link count 与前后 hash |
| 多 release 并发 | 内存/DB/X 盘被占满 | global lease + full concurrency=1 |
| PID 复用 | stale lock 误认活跃/死亡 | pid + create time + fencing + expiry |
| backend restart | daemon task丢失 | independent Worker + durable repository |
| Worker 不在线 | API 接口伪装已执行 | durable QUEUED + worker_unavailable |
| 日志撑爆内存 | capture_output 累积数小时 | streamed rotated logs + bounded tail |
| 资源等待混入吞吐 | 错误判断新算法变慢 | wait/compute/provider time 分离 |
| QFQ/PIT 传播不完整 | 局部增量语义错误 | explicit invalidation graph + parity fixture |
| re-attest 改写旧候选 | 破坏不可变证据 | attestation 独立目录 + read-only path guard |
| API 被滥用 | 任意路径/命令或资源 DoS | auth、allowlist、idempotency、candidate-only |
| scheduler 重复触发 | 每次重启产生新 full | daily reconcile + stable intent key |
| 旧 full 被误报新版构建 | producer provenance 失真 | re-attested 与 rebuilt 状态分离 |

## 19. Production Gates / 生产门禁

| Gate | 本设计/实现阶段状态 |
|---|---|
| 数据导出 | `forbidden_this_task` |
| 既有 candidate 写入 | `forbidden_this_task` |
| DEV DB DDL/DML | `noop` |
| Production DB DDL/DML | `noop` |
| Real control-store init/migrate | `not_authorized`; tests only use temp root |
| Backend start/stop/restart | `not_authorized` |
| Worker start/stop/register | `not_authorized` |
| Scheduler enable | `not_authorized` |
| Client install/reload | `not_authorized` |
| node1 distribution | `not_requested` |
| Production activation/pointer migration | `not_requested` |
| Cleanup/deletion | `not_authorized` |

## 20. Design Acceptance Matrix / 设计验收矩阵

本矩阵只验收“详细设计是否把实现位置和可证伪 oracle 冻结完整”。它不表示 planned 文件已存在。
设计 PR 状态为 `design_contract_ready / source_implementation_not_started / runtime_not_authorized`。
实现 PR 必须把每行 planned ref 替换为真实 symbol/line 与测试结果，再申请 code merge。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | planned `contracts.py`, `intent.py`, `control_store.py` identities | test: `backend/tests/dataset_release/test_intent_catalog.py` identity/idempotency matrix | design_contract_ready | 无 |
| F-002 | planned `decision.py` run outcome + component actions | test: `backend/tests/dataset_release/test_incremental_planner.py` mixed-action plan | design_contract_ready | 无 |
| F-003 | planned `attestation.py`, `signoff.py` | test: `backend/tests/dataset_release/test_reattest_existing.py` artifact/source outcomes | design_contract_ready | 无 |
| F-004 | planned `fingerprints.py`, compatibility registry | test: `backend/tests/dataset_release/test_fingerprints.py` layered invalidation | design_contract_ready | 无 |
| F-005 | planned `source_manifest.py` canonical partition hash | test: `backend/tests/dataset_release/test_fingerprints.py` same-count value revision | design_contract_ready | 无 |
| F-006 | planned `incremental.py`, existing bin/H5 materializers | test: `backend/tests/dataset_release/test_incremental_planner.py` partition action oracle | design_contract_ready | 无 |
| F-007 | planned `dependency_graph.py` | test: `backend/tests/dataset_release/test_incremental_planner.py` QFQ/PIT/window propagation | design_contract_ready | 无 |
| F-008 | planned `copy_on_write.py`, `cas_store.py` | test: `backend/tests/dataset_release/test_copy_on_write.py` external-writer/source-Merkle oracle | design_contract_ready | 无 |
| F-009 | planned `control_store.py`, `lease.py`, `cas_store.py` | test: `backend/tests/dataset_release/test_control_store.py`; `test_resource_lease.py` | design_contract_ready | 无 |
| F-010 | planned `resource_supervisor.py`, `windows_job.py`, `wsl_cgroup.py`, `wsl_resource_guardian.py`, `resource_budget.py` | test: `backend/tests/dataset_release/test_resource_budget.py` + `tests/aistock_validation/dataset_release_platform_smoke.py` Job/cgroup/guardian-loss、12 GiB commit、host commit/pagefile | design_contract_ready | 无 |
| F-011 | planned OS hard-limit + resource state transitions/errors | test: `backend/tests/dataset_release/test_state_machine.py` waiting/emergency/timeout；platform small-cap allocation fail-stop | design_contract_ready | 无 |
| F-012 | planned `control_service.py`, `dataset_releases.py` | test: `backend/tests/routers/test_dataset_releases.py` no in-process execution | design_contract_ready | 无 |
| F-013 | planned `worker.py`, `scripts/dataset_release_worker.py` | test: `backend/tests/dataset_release/test_worker.py` once/drain/serve/restart | design_contract_ready | 无 |
| F-014 | planned `state_machine.py`, SQLite events | test: `backend/tests/dataset_release/test_state_machine.py` no-op、concurrent resume unique、publish orphan atomic handoff、lease release | design_contract_ready | 无 |
| F-015 | planned commands table + cooperative cancel | test: `backend/tests/dataset_release/test_worker.py` pre/post publish-commit cancel race and `REJECTED_TOO_LATE` | design_contract_ready | 无 |
| F-016 | planned `require_dataset_release_operator`, profile allowlist | test: `backend/tests/routers/test_dataset_releases.py` 401/403/409/path injection | design_contract_ready | 无 |
| F-017 | planned profile `sample_policy=on_contract_change` | test: `backend/tests/dataset_release/test_incremental_planner.py` cutoff-only no-sample | design_contract_ready | 无 |
| F-018 | existing `minute_overlay.py`, `tushare_sync_engine.py`; planned typed mapping | test: `backend/tests/dataset_release/test_minute_overlay.py`; `backend/tests/test_tushare_sync_engine.py` | design_contract_ready | 无 |
| F-019 | planned retry policy in Worker | test: `backend/tests/dataset_release/test_worker.py` retryable vs terminal table | design_contract_ready | 无 |
| F-020 | planned `contracts.py` receipt/resource/component action summaries | test: `backend/tests/dataset_release/test_reattest_existing.py`; `test_worker.py` receipt oracles | design_contract_ready | 无 |
| F-021 | planned `signoff.py`, CAS artifact index | test: `backend/tests/dataset_release/test_reattest_existing.py` independent signoff path | design_contract_ready | 无 |
| F-022 | planned shared contracts imported by CLI/API/Skill docs | test: `backend/tests/scripts/test_update_backtest_dataset_monthly.py`; router contract test | design_contract_ready | 无 |
| F-023 | planned SQLite indexes + bounded CAS logs | test: `backend/tests/routers/test_dataset_releases.py` cursor/limit/rotation/reparse | design_contract_ready | 无 |
| F-024 | planned Worker reconcile mode + singleton lease | test: `backend/tests/dataset_release/test_worker.py` catch-up/dedup/multi-instance | design_contract_ready | 无 |
| F-025 | existing candidate-only safety plus planned API exclusion | test: `backend/tests/routers/test_dataset_releases.py` activation/DB/restart/cleanup negative routes | design_contract_ready | 无 |
| F-026 | pytest temp-root data adapters + isolated platform smoke | test: `backend/tests/dataset_release/test_reattest_existing.py`; `tests/aistock_validation/dataset_release_platform_smoke.py` | design_contract_ready | 无 |
| F-027 | planned Skill and five one-level references | artifact: `.codex/skills/update-backtest-dataset/SKILL.md`; fixture-only forward-test receipt | design_contract_ready | 无 |
| F-028 | frozen §6.5 12-index roles/starts/benchmark/weight map + existing moneyflow/PIT/minute contracts | test: `backend/tests/dataset_release/test_index_context.py` exact list/units/isolation；`test_fingerprints.py` | design_contract_ready | 无 |
| F-029 | planned comparable-workload benchmark schema | test: `backend/tests/dataset_release/test_worker.py` 3-run fixture median compute<=110%、rows/s>=90%、RSS/query thresholds；non-comparable=FAIL | design_contract_ready | 无 |
| F-030 | F2 matrix + independent design/code reviews | artifact: `docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md#21-design-compliance-001`; review receipt | design_contract_ready | 无 |

## 21. DESIGN-COMPLIANCE-001

1. **禁止简化版/子集/POC/占位/partial**：A-E 五个 batch 的 required acceptance items 全部有实现和测试后，
   才能申报完整 F2 source delivery；后端页面不是必需，但 control API、Worker、Skill 和一键 CLI 不能以 stub 代替。
2. **禁止静默错误或伪成功**：no-op、re-attest、reuse、incremental、waiting、blocked 和 failed 使用不同 typed status；
   provider、PIT、schema、parity、lease、resource 错误均显式保留。
3. **禁止未经确认的业务逻辑迁移**：PIT、资金流、指数清单、HMM benchmark、分钟 provider 和 candidate-only
   语义保持；控制面不重新实现 exporter 或 validator。
4. **禁止未经确认的门禁、审批或人工确认**：只保留技术身份、资源、数据合同与既有生产授权边界；
   普通 candidate 月更不新增人工审批，production activation 仍是独立授权动作。

## 22. Review History / 审核记录

| Round | Reviewer focus | Findings | Resolution | Status |
|---|---|---|---|---|
| 0 | initial draft | 形成 30 项设计索引与 A-E 实施批次 | 进入三方独立审核 | superseded |
| 1A | data semantics | source snapshot、PIT、QFQ、re-attest、hardlink 合同不闭合 | 增加 canonical source readback、frozen PIT、dependency graph、COW 与 attestation 分层 | resolved_in_revision_1 |
| 1B | worker/API | repository atomicity、state、fencing、auth、pagination 不闭合 | 选择 SQLite+CAS；补原子协议、完整 transition、stale child、auth/cursor/resource 算法 | resolved_in_revision_1 |
| 1C | F2 compliance | 单一 decision、身份、状态、matrix 证据自相矛盾 | 拆 run outcome/component actions、六层 identity、精确 artifact inventory 与可证伪 tests | resolved_in_revision_1 |
| 2A | data race/provenance | digest 与实际消费行流、content/provenance root、legacy truth table仍有竞态 | 增加 VerifiedPartitionStream、provider CAS、双 root 与 legacy outcome 真值表 | resolved_in_revision_2 |
| 2B | durable crash recovery | submission/run 链、publish 崩溃窗口、attempt/cancel、fence 单调性不闭合 | 增加 resolution entity、prepared publish recovery、attempt table、direct cancel、persistent fence CAS | resolved_in_revision_2 |
| 2C | adversarial/platform | reconcile 修订、overlay identity、fake-only platform evidence、control init gate缺口 | 增加 revision probe、provider content root、隔离平台 smoke 与 control-store runtime gate | resolved_in_revision_2 |
| 3A | final data identity | release/attestation key、reuse baseline、silent revision probe仍有冲突 | 完整 release digest、frozen reuse source、current source/PIT attestation key、TTL content probe | resolved_in_revision_3 |
| 3B | final durability | submission events、attempt expiry、双 lease、rename-marker crash recovery不闭合 | submission API/event、过期恢复、host/release tokens、publish recovery adoption | resolved_in_revision_3 |
| 3C | final compliance | active revision 与 orphan child 可能击穿 single-active/resource contract | WAITING_ACTIVE_RUN 与全 process-tree quiescence hard block | resolved_in_revision_3 |
| 4A | final data closure | run generation、attestation lineage、re-attest finalize 路径仍有歧义 | run generation digest、producer-bound key、attestation-only CAS transaction | resolved_in_revision_4 |
| 4B | final state closure | provider/orphan submission states 与 marker recovery fence 未闭合 | 补 terminal/waiting transitions、publisher/finalizer fence adoption | resolved_in_revision_4 |
| 4C | final compliance | 一位 reviewer PASS；两位指出相同 re-attest/state 缺口 | 不按多数票通过，完成定点修订后再审 | resolved_in_revision_4 |
| 5A | identity closure | reconstructed attestation 缺 canonical unknown provenance identity | 增加 provenance state 与 `UNKNOWN_PRODUCER_PROVENANCE_V1` sentinel | resolved_in_revision_5 |
| 5B | state/compliance closure | scheduler “existing=no-op” 与 TTL revision probe 冲突 | 仅 fresh unchanged probe no-op；stale probe 幂等创建/复用 | resolved_in_revision_5 |
| 5C | re-attest generation | 不同 candidate 可能共享 source/run generation | operation target 绑定 candidate identity、artifact root 与 attestation target key | resolved_in_revision_5 |
| 5D | submission control | pre-run cancel 无 target，retry budget 耗尽无终态 | command target entity/id + submission/run retry-exhausted transitions | resolved_in_revision_5 |
| 6A | candidate identity closure | candidate identity 缺 canonical fields，artifact 相同的不同候选可能共享 generation | 增加 registration、volume/path、lineage、artifact/provenance 字段与 canonical encoding | resolved_in_revision_6 |
| 6B | no-op state closure | `NO_OP_VERIFIED` 仅有 outcome、没有可达 terminal run 与 submission 链 | 增加 fresh-probe no-op generation、CAS receipt、原子 terminal 建档、双事件与 TTL renewal | resolved_in_revision_6 |
| 7A | data/identity final | PIT-only revision 未进入 release identity；new build 缺 current-source attestation | PIT 进入 intent/release/candidate/marker；fresh probe + attestation 与 publish 原子终结 | resolved_in_revision_7 |
| 7B | state/crash final | no-op fence 破坏幂等；publish commit 后仍可 cancel/retry/new resume | semantic receipt剔除 ownership；冻结 commit point 与同 run finalizer recovery | resolved_in_revision_7 |
| 7C | resource/performance final | 只有 telemetry、没有 hard defaults 与回退阈值 | 冻结 12/8 GiB、host/WSL reserve、chunk/query caps 与 3-run 10% benchmark gate | resolved_in_revision_7 |
| 8A | memory enforcement final | RSS polling 不能阻止 private commit/pagefile 膨胀 | Windows Job commit + WSL cgroup/swap hard limits；system commit/pagefile/low-memory gate | resolved_in_revision_8 |
| 8B | resume identity final | terminal resume 与永久 intent/generation UNIQUE 冲突 | `RESUME_BUILD` operation target 绑定原 run/checkpoint/monotonic ordinal | resolved_in_revision_8 |
| 8C | resolution ownership final | resolved terminal transaction 未释放 resolution attempt/lease | 所有 owned resolution exit 原子终结 attempt、清 pointer、释放 leases | resolved_in_revision_8 |
| 9A | WSL owner-death final | cgroup memory limit 不等于 guardian-loss lifecycle | transient systemd user service + heartbeat/pipe guardian + KillMode control-group | resolved_in_revision_9 |
| 9B | orphan hold final | orphan timeout terminal 与 lease/pointer invariant 冲突 | timeout 只告警；持久 `ORPHAN_HOLD` 到完整 tree quiescent | resolved_in_revision_9 |
| 9C | build lease final | publish success 未明确原子释放 host/release leases | 所有 owned build exits 同事务释放 attempt/pointer/applicable leases | resolved_in_revision_9 |
| 10A | resume concurrency final | 不同 idempotency keys 可分配两个 active resume generations | lineage row CAS + partial UNIQUE + latest-leaf rule | resolved_in_revision_10 |
| 10B | post-publish orphan final | owner loss 后双 lease 的 hold/交接事务不闭合 | host+release `ORPHAN_HOLD` 到 quiescent 后原子 fence/adopt，无 FREE 窗口 | resolved_in_revision_10 |
| 10C | index/HMM implementation contract | 12 指数仅称“既有”，设计分支没有可实施代码/角色/起点/映射 | §6.5 冻结完整 12-code、roles、required_from、benchmark/weight、outputs/units | resolved_in_revision_10 |
| 11A | resolution single-active final | 同 logical request 的不同 submissions 可并发 resolution claim | logical-request resolution lease + partial UNIQUE active attempt | resolved_in_revision_11 |
| 11B | same-owner publish recovery final | 只定义新 fence adoption，当前 owner 无合法 recovery transition | 明确 same attempt/owner/fences/readback CAS 回 `PUBLISHING` | resolved_in_revision_11 |
| 12A | marker atomicity final | committed marker 未定义原子写，crash 可留下 partial marker并误判冲突 | 同卷 temp+flush+create-if-absent atomic rename+readback；partial temp 不可发现 | resolved_in_revision_12 |
| 13A | legacy provenance oracle | 测试把所有 provenance 缺失都判 artifact-only，与 source-only reconstructed truth table 冲突 | 区分 PIT 缺失与原 source/producer 缺失；仅后者 full parity 可 reconstructed | resolved_in_revision_13 |
