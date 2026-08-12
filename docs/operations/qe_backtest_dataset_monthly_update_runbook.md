# QE 回测数据集月度更新操作手册

本手册面向每月执行 candidate 数据集更新的 operator。默认流程只提交和签收候选 release，不覆盖现有候选，不切换 production，不同步 node1，不修 DB，不启停服务。

详细设计：`docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md`。
低层数据公式与历史 exporter 兼容说明：`docs/analysis/qlib_backtest_dataset_export_guide_20260712.md`。

## 1. 最短路径

在当前 AIstock repo root 执行：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py status --latest
```

第一条成功响应会返回 `idempotency_key`。若命令在得到响应后需要重试，显式复用：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py monthly --candidate-only --idempotency-key <原key>
```

不显式给 key 的每次人工调用都是新的 submission，并要求新的 source probe；resolution 层仍会把等价
logical request 链接到既有 run/release，不会因为新 submission 就重复导出。

不要为“保险”重复提交第一条命令。重复 logical request 应链接既有 durable submission/run；不同 payload 使用同一 idempotency identity 时应明确冲突。

RTK 首次 wrapper 失败、不支持或需要精确原始输出时，直接运行等价 `python` 命令并记录回退原因。RTK 不可用不等于数据流程失败。

## 2. 一次性运行时准备与每月动作分开

以下准备不是每月命令的一部分，也不能由月更隐式执行：

- source 已合入并在目标 runtime 可见；
- X 盘 candidate root 与 control root 分离并在 allowlist；
- control store 已由专用管理入口显式初始化/迁移并 readback；
- `qe_hmm_full_v1` profile 已 allowlist；
- 独立 Worker 已由 runtime owner 注册并处于兼容版本；
- 如使用 API，backend route 和 operator token file 已由 runtime owner配置；
- scheduler 是否启用有独立配置和授权，默认关闭。

如果任一项缺失，记录对应状态并交给 runtime owner。不要自行初始化真实 control root、安装依赖、重启 backend、启动 Worker、注册 scheduler 或把任务改成 FastAPI background job。

仅在 runtime owner 已分别授权初始化/迁移和 Worker 运行时，使用以下可复制命令；它们不是每月操作，也不属于本轮源码验证授权：

```powershell
$Profile = (Resolve-Path .\configs\datasets\qe_backtest_monthly_v1.yaml).Path
$ControlRoot = 'X:\AIstock_dataset_release_control'

# 新空 root 只执行一次；不会由 API/Worker 自动调用
rtk python scripts/dataset_release_control_store.py init --profile $Profile --control-root $ControlRoot --expected-version 1

# 只读 readback；每次初始化/迁移后必须 PASS
rtk python scripts/dataset_release_control_store.py status --profile $Profile --control-root $ControlRoot --expected-version 1

# 仅校验既有 store、源码 identity、真实 registry、Windows侧冻结文件SHA与WSL路径配置；不启动WSL
rtk python scripts/dataset_release_worker.py --preflight --profile $Profile --control-root $ControlRoot
```

只有代码明确注册了 schema migration 且迁移另获授权时才执行：

```powershell
rtk python scripts/dataset_release_control_store.py migrate --profile $Profile --control-root $ControlRoot --expected-version <目标版本>
```

当前 v1→v1 是只读/no-op；未知版本 fail closed。不得通过删库、改 `PRAGMA user_version` 或复制 SQLite 文件绕过。

独立 Worker 的启动/注册是单独进程控制授权。获授权后 runtime owner 才可使用：

```powershell
rtk python scripts/dataset_release_worker.py --serve --profile $Profile --control-root $ControlRoot
```

普通 operator 不在月更窗口临时启动 Worker。`--once/--drain/--serve` 都会进入 Worker 生命周期；只有
`--preflight` 是零 claim、零 heartbeat 的准备检查。

API token 文件必须是绝对路径上的 plain local file，内容至少 32 个字符、最多 4096 bytes，并由 runtime owner
限制 ACL；token 值不进入命令、日志、receipt 或 actor ID。轮换同一文件会保持稳定 actor identity、使旧 token 和
旧签名 cursor 失效。不要在文档中粘贴 token。

每月 operator 只做：提交一次、查看 bounded status、处理 typed 状态、读取 terminal receipt、报告独立 gates。

## 3. 提交普通月更

只有在本次真实数据更新已获授权时执行：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py monthly --candidate-only
```

默认由 live profile 决定：

- profile：`qe_hmm_full_v1`；
- cutoff：上一个月最后一个已完成交易日；
- reuse/resume：自动；
- sample：仅 contract 变化时；
- production activation、node1、DB repair、restart、cleanup：`not_requested`。

CLI 只提交 durable intent。返回 `submission_id` 不代表已经有 `run_id`，更不代表开始导出或完成发布。source resolution 可以：

- 链接已有 valid release；
- 产生 fresh `NO_OP_VERIFIED`；
- 创建 mixed-action build run；
- 等待 source/resource/Worker；
- 在 run 创建前以 provider/source/identity 错误终止。

不要把 source resolution 等待误判为 CLI 卡死后再次提交。

## 4. 查看进度

```powershell
rtk python scripts/update_backtest_dataset_monthly.py status --latest
```

交互终端只看 summary 和 bounded event/log tail；完整 JSON、日志和 evidence 保存在 catalog/CAS。不要把数小时日志整体读取进内存。

状态解释：

| 字段=值 | 含义 | 操作 |
|---|---|---|
| `submission_state=QUEUED_RESOLUTION` + `worker_health.state=unavailable|stale|blocked` | durable request 已保存，兼容 Worker 不可用 | 不重提、不代跑；通知 runtime owner |
| `submission_state=RESOLVING_SOURCE` | 冻结 source/PIT/provider content | 等待；不启动 exporter |
| `run_state=QUEUED|EXECUTING|VALIDATING|PREPARING_PUBLISH|PUBLISHING` | run 受 attempt/lease/fence/resource supervisor 管理 | 用 status/events 观察 |
| `WAITING_RESOURCE` | host/WSL commit、memory 或 X 空间不足 | 不提高 cap；等待或由新 attempt 走 pressure ladder |
| `WAITING_PERFORMANCE_REGRESSION` | 可比 workload 持续越过冻结性能门限 | 不缩业务范围；保留 checkpoint，按 pressure ladder/typed deadline 处理 |
| `WAITING_SOURCE` | required source 尚未完成 | 等待 source；禁止补零/减范围 |
| `WAITING_ORPHAN_QUIESCENCE` | 旧 owned process tree 仍 alive/unknown | 只观察；不得 kill/delete lock |
| `BLOCKED_PROVIDER_TERMINAL` | 40203、overlap conflict、无完整 240 bars等 | 报告 code/date/pending scope，不循环重试 |
| `BLOCKED_SOURCE_SNAPSHOT_DRIFT` | plan 与实际消费同行流不一致 | 新 source probe/intent；不得复用旧 staging |
| `SUCCEEDED/NO_OP_VERIFIED` | fresh probe 证明无需重导 | 读取 no-op receipt并完成报告 |
| `SUCCEEDED/REATTESTED` | 旧 candidate 未改，新增 attestation | 读取 attestation outcome |
| `SUCCEEDED/CANDIDATE_VALIDATED` | candidate 已原子发布并签收 | 检查逐组件 action 和完整 receipt |

`attempt.state=RUNNING` 是内部 attempt 状态，不等于公开 `run_state`。`worker_health.state=healthy` 仅表示
profile/config/capability 兼容和 heartbeat fresh，不代表某个数据 run 已完成。

读取 bounded events、terminal receipt 或一页日志：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py events --submission-id <submission_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py events --run-id <run_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py receipt --run-id <run_id>
rtk python scripts/update_backtest_dataset_monthly.py log --run-id <run_id> --stream stdout --max-bytes 262144 --max-lines 1000
```

events 每页最多 200 条；log 每次最多 1 MiB/1000 行。使用返回的 `next_after_event_id`、
`next_log_id/next_generation/next_byte_offset` 继续读取，不把完整历史日志拼进内存。
log cursor 是 catalog `log_id`、CAS segment `generation` 和文件 `byte_offset` 的真实前向位置；API 会同时绑定
principal、endpoint、run、stream、filter 和 order。不要丢弃 cursor 后重复读取“最新 tail”来冒充连续日志。

同一任务的 resource wait、provider wait 和 compute time 分开看。资源保护导致 wall-clock 增长不等于算法吞吐回退。

## 5. 同 cutoff 不要重导

再次运行 `monthly --candidate-only` 让系统发现 cataloged release。只有 fresh source/PIT probe、artifact root 和 exact validator identity 都有效时，才生成 `NO_OP_VERIFIED`。

以下证据不足以 no-op：

- 目录已存在；
- cutoff/max date相同；
- row count、mtime、文件大小或 sampled values相同；
- 旧 receipt 曾经 PASS；
- source probe 已过 freshness TTL。

probe stale 时只创建/复用一个 `SOURCE_REVISION_PROBE`，不要提交 exporter 或新的 full build。

## 6. 复验已有不可变候选

先使用 catalog 功能注册 allowlisted candidate identity；catalog 不等于发布或 source equivalent。然后对最新 eligible candidate：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py reattest-existing --latest
```

复验必须保持 candidate 逐字节不变。attestation 写入独立 control CAS/catalog。结果可能是：

- `CURRENT_SOURCE_EQUIVALENT`；
- `CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED`；
- `ARTIFACT_VALID_SOURCE_CHANGED`；
- `ARTIFACT_VALID_ONLY`；
- `INVALID`。

只有前两项可在其绑定的 current source/PIT/validator identity 下复用。缺 PIT provenance 的 legacy candidate 不得提升为 current-source-equivalent。

## 7. 恢复、中断和重启

正常月更自动识别匹配 lineage/checkpoint 并 resume。不要手工追加 final candidate、移动 checkpoint 或从末日期继续猜测。

如果需要取消，使用当前 submission/run 暴露的 durable `cancel-request`。取消在安全 stage/chunk checkpoint生效；publish commit 后返回 `REJECTED_TOO_LATE` 并由同一 run 完成/恢复 finalization。取消不授权终止进程或删除 staging。

backend 重启不应丢任务；control catalog 和 Worker heartbeat 是 authority。Worker owner 丢失时，只有 lease expiry 且 Windows/WSL 全部 descendants 已证明 quiescent 才能 reclaim。不要删除 lock、改 SQLite、重置 fence 或杀 orphan。

## 8. 资源边界

关键 hard contract：

- 同 host heavy full concurrency=1；
- aggregate owned private commit≤12 GiB；
- Windows-only Job≤8 GiB，hybrid Windows side≤4 GiB；
- WSL memory high/max/swap=6/8/0 GiB；
- host start/emergency available 和 commit headroom=16/8 GiB；
- DB pool=4、row-producing query=1、provider request=1；
- minute batch/date chunk=20 stocks/3 months；factor H5/static 由单日切片与 Parquet row-group 控制；profile 中 `h5_batch=100` 仅为 v1 兼容遥测，不是生效的降压旋钮；
- Parquet row group/validation chunk≤100,000 rows；
- start free space=max(32 GiB, 1.25×predicted new bytes)。

允许的降压只有 profile pressure ladder：batch/chunk/row-group/workers逐级降低。不得通过减少股票、日期、字段、PIT、指数、H5 或验证范围换性能。

性能边界必须如实区分：

- candidate 物化支持 component/partition 级 `REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`。daily/minute
  canonical CSV 按 instrument/date range 保持不可变有序 segment；增量只追加 tail segment，selective repair 必须有
  explicit active-segment override，禁止把全历史拼成一个内存 frame；
- 大文件 writer 使用 single-copy deferred COW：final target 在 writer 完成前不出现在 candidate tree；writer private
  root 最多复制一次 baseline，quiescent/readback 后同卷 atomic rename，`final_recopy_count=0`。外部 writer 永远看不到
  COW hardlink tree，baseline Merkle 必须保持不变；
- minute source query 和 `dump_update` 在行物化前按 pressure rung 分批，hard max=20 codes，降压只能
  `20→10→5`；“先返回无界结果再切批”不合规；
- 在没有可信 DB partition revision ledger 时，初次 source freeze 与 publish 前 DB-only recheck 仍可能分别做一次截止日内的全值扫描；MVCC/provenance watermark 不能替代内容一致性；
- 因此“候选只重写新增/失效部分”不等于“整条月更只读取新增月份”；等待、DB read、provider、compute、validation 时间必须分项报告；
- fixture/synthetic benchmark 只证明算法复杂度和内存上限，不能宣称真实全量耗时；真实 full/new-cutoff telemetry 只能在未来数据更新另获授权后产生；
- 若未来要消除两次全值扫描，需要新的可信 revision-ledger F2、DEV 验证和 production DDL/DML 目标授权，本实现不暗中创建该账本。

外部 Qlib toolchain 也是 hard gate：Ubuntu、conda `rdagent-gpu`、`dump_bin.py` Windows/WSL path、repo guardian/runner
以及冻结 SHA 必须与 profile 完全一致。零执行 `--preflight` 验证 Windows 侧冻结文件内容和 WSL 路径配置，
不会为探测而启动 WSL；首次受监督 WSL dump/consumer 再验证实际 distro/conda/runtime。任一阶段报 `BLOCKED_TOOLCHAIN_*` 时不得 fallback 到另一环境、
旧脚本或临时 pip/conda 安装；依赖安装与 runtime 修改需要独立授权。

完整资源与 orphan 合同见 `.codex/skills/update-backtest-dataset/references/resource-and-worker.md`。

常驻 Worker 的空闲 poll 使用 5→10→15 秒上限退避；health heartbeat 在状态变化或 15 秒间隔写入，不为每次
无变化 idle poll 做 fsync。claimed work 不节流 heartbeat，长 processor 使用独立 health-heartbeat thread。
这只描述源码合同；本轮没有注册或启动真实 Worker。

## 9. Terminal receipt 签收

只有 catalog、committed marker、release、attestation、run terminal transaction一致时才签收。至少确认：

1. profile、requested/effective cutoff、scope、所有 identity；
2. run outcome 与每个 component/partition action/reason；
3. source content/provenance roots、frozen PIT digest、artifact/validation/resource fingerprints；
4. daily/minute value parity、QFQ、limit/suspend/reference；
5. moneyflow share/CNY contract、H5/static raw-field parity、`mf_total_net_amt == mf_net_amt`、
   `mf_total_net_vol == mf_net_vol`、正确 amount/volume denominator，以及跨 chunk 保持相同 value/NaN mask 的
   5/20 valid-observation rolling parity；
6. static 121列、`l2_code_id int16/-1`；
7. exact 12-index list、三列 `code,start,end` 的 `instruments/index.txt`、units、coverage、stock/index隔离；
   `metadata/index_context_manifest.json` 必须绑定 release/source/ArtifactReady/PIT、完整 file/schema hash、per-code
   roots 与 fingerprints 并独立 readback，HMM benchmark仍为 `000300.SH`；
8. QE daily/minute 与 HMM producer smoke，HMM consumer仍未激活；
9. peak resource、query/rows/bytes、wait/compute/provider time；
10. component manifest storage v2 与 canonical lineage v3 的 top/index/shard refs、outer reader/writer capability、
    legacy migration reason；任一单对象超过 32 MiB 或旧 worker 尝试 resume v3 必须 fail closed；
11. required validations全部 PASS；required WARN/SKIP不算完整 full signoff。

签收顺序是 `status --latest` → `events --run-id` → `receipt --run-id`；只有 receipt 已存在且其中
catalog/marker/release/attestation/component action/validator/resource evidence 互相一致，才报告 candidate signoff。
`status` 里的 terminal state 本身不替代 receipt。

详细字段见 `.codex/skills/update-backtest-dataset/references/release-receipt.md`。

## 10. 生产与其他动作另行授权

candidate signoff 后仍分别报告：

```text
production_activation
production pointer/symlink migration
node1 distribution
DB repair / DDL / DML
backend or Worker restart/registration
scheduler enable
dependency/client installation
cleanup/deletion
```

本手册不提供自动 production activation。没有 target-specific 授权时均保持 `not_requested/not_authorized`。不得因为数据候选 PASS 或代码已 merge，就执行上述动作。

## 11. 故障升级模板

汇报时提供：

```text
profile / requested cutoff / effective cutoff
submission_id / run_id / release_id / attestation_id
state / error_code / retryable / bounded context_ref
component + partition + pending code/date scope
last safe checkpoint and applicable host/release fence
resource peak and wait/compute/provider seconds
candidate/catalog/receipt state
production/node1/DB/runtime/cleanup states
```

不要粘贴 token、密码、私钥、完整 DSN、全量日志或未受控绝对路径。

## 12. 低层命令的定位

`plan/run/reuse/fetch-overlay/verify` 和旧 daily/minute/H5 exporter 只用于 typed diagnostics、兼容验证或 domain Worker 内部调用。它们不能替代普通 `monthly`，不能绕过 control catalog、source stream、lease/fence、resource supervisor、validator 和 publisher。

源码/fixture 验证完成但未获真实数据授权时，明确报告：

```text
source_state=source_ready_fixture_verified
mixed_daily_minute_factor_direct_e2e=fixture_verified
selective_override_clean_full_equivalence=fixture_verified
platform_hard_cap_evidence=fixture_platform_verified_real_full_pending
runtime_real_data_evidence=not_run_not_authorized
real_full_scale_performance=pending
production_activation=not_requested
```

当前实现 PR 的合法上限就是上述状态；不得为了获得“真实耗时”重导旧 cutoff，也不得读取或修改上一批候选。
