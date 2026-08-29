# QE 回测数据集月度更新操作手册

本手册面向每月执行 candidate 数据集更新的 operator。默认流程只提交和签收候选 release，不覆盖现有候选，不切换 production，不同步 node1，不修 DB，不启停服务。

详细设计：`docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md`。
低层数据公式与历史 exporter 兼容说明：`docs/analysis/qlib_backtest_dataset_export_guide_20260712.md`。

## 0. 2026-09-01最高优先级执行窗

本执行窗的目标是在2026-09-01凌晨开始，把独立candidate-only数据集更新到2026-08-31并形成terminal receipt。
它不授权production activation、DB DDL/DML、node1、后端重启或cleanup。QE/HMM训练、Selection/Paper/Advisory
消费者迁移不属于本执行窗前置。

### 0.1 8月29日至30日：只闭合首次v2基线

1. 冻结直接参与数据发布的source/profile/toolchain；无交集消费者提交不阻断本任务。
2. 对BUG-1238修复后的最终runtime只提交一次五股sample：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
  --plan pit_v2_initial_20260731_v1 --scope sample --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

历史`BLOCKED_CONTRACT` submission已经终态，不得resume。新sample失败时读取同一submission的bounded events/result，
按typed blocker处理；禁止重复提交第二个sample。只有同合同源码BUG修复合入后，才执行一次final rerun。

3. sample PASS后，复用已经存在的2026-07-31 industry full authority，生成缺失的P3A full：

```powershell
$CandidateRoot = 'X:\AIstock_dataset_candidates\backtest_dataset_candidates'
$IndustryJul = Join-Path $CandidateRoot '.industry_pit_authority\qe_hmm_full_v2\2026-07-31\full'
$SectorJul = Join-Path $CandidateRoot '.sector_data_authority\qe_hmm_full_v2\2026-07-31\full'

rtk python scripts/build_sector_data_candidate.py `
  --industry-candidate-root $IndustryJul `
  --artifact-root $SectorJul `
  --start-date 2018-08-01 `
  --end-date 2026-07-31
```

`$SectorJul`必须是不存在的新目录；命令拒绝覆盖时不得删除或改写旧目录来重跑。

4. P3A full四文件readback和完整分母闭合后，只提交一次2026-07-31 full initial migration：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
  --plan pit_v2_initial_20260731_v1 --scope full --candidate-only
```

planner必须优先复用已验证2026-07-31 v1组件。不能为了流程验收强制全量重导，也不能覆盖任何既有7月数据集。

### 0.2 8月31日收盘后：准备目标cutoff authority

在权威交易日历和当日源数据可用后，先只读检查`market.stock_universe_pit_state`与
`market.stock_universe_pit_spans`中`aistock_equity_pit_canonical_v2`是否ready/clean并覆盖至2026-08-31。
industry/P3A builder会拒绝超出该覆盖的请求，不能用参数、复制7月receipt或缩短窗口绕过。

如果coverage不足，必须先使用`scripts/prepare_canonical_pit_monthly.py`受控operator：默认plan-only，复用
`StockUniversePitService.ensure_canonical_pit_universe()`，固定canonical key/rule/start/cutoff；DEV执行
apply/readback后，可用同cutoff再次apply取得`NO_OP_VERIFIED`证明幂等；该复核是推荐证据而非新增阻断门禁。
再由用户对production目标和2026-08-31 refresh明确授权，最后绑定同cutoff DEV成功receipt执行production apply/readback。DEV是项目规定的验证库，
不再增加强制rollback这一非规范门禁；该调整不放宽production授权、readback或不可变receipt要求。
该CLI由BUG-1243实现；源码合入和用户完成`backend-main`重启前，仍禁止使用临时Python one-liner、直接SQL、
普通ST endpoint或monthly隐式写库替代。所有receipt路径必须是profile control root下`operator_receipts`的
直接子文件，文件名不可复用。

```powershell
$ReceiptRoot = 'X:\AIstock_dataset_release_control\operator_receipts'

# 1. DEV只读计划
rtk python scripts/prepare_canonical_pit_monthly.py --database dev --mode plan `
  --cutoff 2026-08-31 `
  --receipt-path (Join-Path $ReceiptRoot 'canonical-pit-20260831-dev-plan.json')

# 2. DEV apply/readback；不足时只重建canonical rolling state/spans
rtk python scripts/prepare_canonical_pit_monthly.py --database dev --mode apply `
  --cutoff 2026-08-31 `
  --receipt-path (Join-Path $ReceiptRoot 'canonical-pit-20260831-dev-apply.json')

# 3. 可选幂等复核：同cutoff再次apply应得到NO_OP_VERIFIED
rtk python scripts/prepare_canonical_pit_monthly.py --database dev --mode apply `
  --cutoff 2026-08-31 `
  --receipt-path (Join-Path $ReceiptRoot 'canonical-pit-20260831-dev-noop.json')

# 4. 用户对production目标明确授权后执行；禁止从DEV复制state/spans
rtk python scripts/prepare_canonical_pit_monthly.py --database production --mode apply `
  --cutoff 2026-08-31 --authorization-ref '<production-authorization-ref>' `
  --dev-receipt (Join-Path $ReceiptRoot 'canonical-pit-20260831-dev-apply.json') `
  --receipt-path (Join-Path $ReceiptRoot 'canonical-pit-20260831-production-apply.json')
```

任一步出现`schema_contract_missing`、readback仍需重建、target/contract/cutoff不匹配或receipt已存在时均立即停止；
operator不会建表、切换authority pointer、导出数据集、调用provider或控制进程。

canonical PIT coverage PASS后，生成截至2026-08-31的双authority与P3A full：

```powershell
$CandidateRoot = 'X:\AIstock_dataset_candidates\backtest_dataset_candidates'
$IndustryAug = Join-Path $CandidateRoot '.industry_pit_authority\qe_hmm_full_v2\2026-08-31\full'
$SectorAug = Join-Path $CandidateRoot '.sector_data_authority\qe_hmm_full_v2\2026-08-31\full'

rtk python scripts/build_industry_pit_candidates.py `
  --artifact-root $IndustryAug `
  --window-start 2018-08-01 `
  --window-end 2026-08-31

rtk python scripts/build_sector_data_candidate.py `
  --industry-candidate-root $IndustryAug `
  --artifact-root $SectorAug `
  --start-date 2018-08-01 `
  --end-date 2026-08-31
```

两条writer都使用只读数据库事务和repo-external新目录；任何typed unavailable保留在完整分母中，不以内连接、
默认行业或删除股票缩小范围。目标目录已存在时先readback其identity；禁止覆盖或用删除目录制造重跑条件。

### 0.3 9月1日00:05以后：只提交一次August monthly

`monthly`使用`previous_month_last_completed_trading_day`。8月31日当天执行仍会解析到7月31日，因此必须在
2026-09-01 00:05（Asia/Shanghai）以后提交：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

第一条命令只执行一次。后续仅使用同一submission/run的`status/events/receipt/log`有界读取。provider尚未发布或网络
限流属于同一durable intent的retryable waiting；不得新建submission追赶时间。目标是在9月1日开盘前得到
`SUCCEEDED/CANDIDATE_VALIDATED`及完整terminal receipt；时间目标不放宽数据、PIT、行业、指数、资源或验证合同。

### 0.4 P0完成与停止条件

只有以下全部成立才报告8月31日candidate更新完成：

- effective cutoff=`2026-08-31`；
- P1/P2A与P3A full readback、denominator和hash闭合；
- daily/minute/PIT/ST/stk_limit/QFQ/static/H5/12-index/sector及QE/HMM producer smoke全部required PASS；
- terminal receipt、candidate marker、catalog、release和attestation identity一致；
- dataset-release run的production writes、pointer changes、DB writes和service controls均为0；若此前执行了获授权的
  canonical PIT coverage DML，其DEV/production apply/readback receipt必须独立存在，不能记入monthly零写入证明或被其掩盖。

如果出现新的确定性合同错误、authority冲突、内部required gap或源码BUG，保留attempt/checkpoint并停止P0执行；只登记
一个精确owner的P1 BUG，不增加新的设计阶段、不连续提交更多sample/full/monthly。production activation继续等待独立授权。

## 1. 最短路径

在当前 AIstock repo root 执行：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

`qe_hmm_full_v2` 是未来月更的唯一 canonical PIT 候选 profile。`qe_hmm_full_v1` 保持原语义，仅用于已生成
release（包括 2026-07-31 候选）的历史复现、catalog 和只读 re-attestation；在 canonical v2 完成全量验证并获
独立 production activation 授权前，源码合入不会自动切换现有 Selection/Paper/QE runtime。

第一条成功响应会返回 `idempotency_key`。若命令在得到响应后需要重试，显式复用：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only --idempotency-key <原key>
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
- `qe_hmm_full_v1`（历史复现）与 `qe_hmm_full_v2`（canonical candidate）profile 均已 allowlist；
- 独立 Worker 已由 runtime owner 注册并处于兼容版本；
- 如使用 API，backend route 和 operator token file 已由 runtime owner配置；
- scheduler 是否启用有独立配置和授权，默认关闭。

如果任一项缺失，记录对应状态并交给 runtime owner。不要自行初始化真实 control root、安装依赖、重启 backend、启动 Worker、注册 scheduler 或把任务改成 FastAPI background job。

仅在 runtime owner 已分别授权初始化/迁移和 Worker 运行时，使用以下可复制命令；它们不是每月操作，也不属于本轮源码验证授权：

```powershell
$Profile = (Resolve-Path .\configs\datasets\qe_backtest_monthly_v2.yaml).Path
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

## 3. 首次 PIT v2 迁移与普通月更

### 3.1 首次 PIT v2 迁移

首次迁移不是可输入任意 cutoff/路径/证券的通用入口，只接受仓库登记的固定计划：

首次真实 W7 前必须先完成全历史 source-readiness 审计闭环。该动作是独立的数据库 DML，绝不由
`initial-migration`、Worker 或普通月更隐式执行。旧的 `seed_existing_rows` 记录不能替代
dataset-release 已登记的 `physical_audit_seed` 权威。

DEV 不要求复制八年生产数据。先用单行事务验证现有 DEV 表的 upsert/readback 机制；该模式执行后强制
rollback，不留下 audit 行变化，也不调用 provider：

```powershell
$DevDmlReceipt = 'X:\AIstock_dataset_release_control\operator_receipts\pit_v2_audit_seed_dev_dml_validation_20260731.json'
rtk python scripts/seed_dataset_refresh_audit.py --database dev --mode validate-dml `
  --end-date 2026-07-31 --authorization-ref <DEV事务验证授权引用> --receipt-path $DevDmlReceipt
```

receipt 必须为 `mode=validate-dml`、`status=PASS`、`transaction_rolled_back=true`、`rows_changed=1`，并绑定
固定 `dev_dml_contract_digest`。这只证明 DML 机制，不证明 DEV 拥有生产全历史数据。

然后对生产目标执行全范围只读 plan。它只读取生产物理表、PIT、交易日历和现有 audit，不写数据库：

```powershell
$ProdPlanReceipt = 'X:\AIstock_dataset_release_control\operator_receipts\pit_v2_audit_seed_prod_plan_20260731.json'
rtk python scripts/seed_dataset_refresh_audit.py --database production --mode plan `
  --end-date 2026-07-31 --receipt-path $ProdPlanReceipt
```

plan 的 PIT 股票类查询只统计权威 PIT 代码，不能让 ETF、北交所或池外证券污染结果。`bak_basic` 合法空日
记为 `empty_valid`；`index_daily` 精确缺失键和 `stk_limit` 缺失/不完整键记为
`candidate_repairable`，交给候选内 provider/规则层闭环；其他 required dense 内部缺口仍硬阻断。
`stk_limit` 的三列 audit 非空要求与 raw CAS 允许修复列为空是两个层次：raw partial 行必须进入
artifact-ready completion，最终只允许完整的 frozen-PIT 股票日进入 Qlib normalizer；不得在 source
sealer 提前阻断，也不得把非 PIT partial 行泄漏到构建输出。

生产 apply 是独立 target-specific DML 授权，并强制读取成功 DEV transactional receipt：

```powershell
$ProdReceipt = 'X:\AIstock_dataset_release_control\operator_receipts\pit_v2_audit_seed_prod_20260731.json'
rtk python scripts/seed_dataset_refresh_audit.py --database production --mode apply `
  --end-date 2026-07-31 --authorization-ref <生产授权引用> `
  --dev-receipt $DevDmlReceipt --receipt-path $ProdReceipt
```

`apply` 先在生产目标完成全范围计划再开始第一条写入；任一真实硬阻断会整笔回滚。写入固定为
`data_source=physical_audit_seed`，分批 upsert 后在同一事务精确 readback。receipt 只记录 `.env`
凭据位置和数据库 identity digest，不记录密码或 token。DEV rollback 验证和生产 apply/readback 完成后，才可执行
下面的 W7 sample；若 W7 再报 source audit blocker，不得重复 seed 或重复提交 sample，应先按 receipt
中的 dataset/date 范围诊断。

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
  --plan pit_v2_initial_20260731_v1 --scope sample --candidate-only
```

该命令只有在 W7 真实小样本执行另获授权后才能运行；W2 源码/fixture 验证不得提交真实 intent。control service
和 Worker 会共同复验 `plan_id`、canonical `plan_digest`、固定 cutoff `2026-07-31`、5 只样本证券、事件/指数窗口及
零生产动作 safety。即使在其他月份执行，cutoff 也不会随系统日期漂移。sample 的股票代码在数据库返回行之前
完成过滤，并形成 validation-only PIT binding，不能交给 QE/训练。

sample terminal PASS 且绑定最终 source/profile/toolchain/plan digest 后，完整候选仍需独立真实数据授权，并使用
同一计划提交：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
  --plan pit_v2_initial_20260731_v1 --scope full --candidate-only
```

这两条命令都只提交 durable candidate intent，不启动 Worker、不覆盖既有 2026-07-31 v1 candidate、不切 production。

### 3.2 普通月更

只有在本次真实数据更新已获授权时执行：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
```

默认由 live profile 决定：

- profile：显式 `qe_hmm_full_v2`；不带 `--profile` 的 legacy 默认仅为迁移期兼容，不用于未来正式月更；
- PIT authority：`aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2`，IPO 暖机为 252 个交易所交易日并包含历史退市股生命周期；
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
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

交互终端只看 summary 和 bounded event/log tail；完整 JSON、日志和 evidence 保存在 catalog/CAS。不要把数小时日志整体读取进内存。

状态解释：

| 字段=值 | 含义 | 操作 |
|---|---|---|
| `submission_state=QUEUED_RESOLUTION` + `worker_health.state=unavailable|stale|blocked` | durable request 已保存，兼容 Worker 不可用 | 不重提、不代跑；通知 runtime owner |
| `submission_state=RESOLVING_SOURCE` | 冻结 source/PIT/provider content | 等待；不启动 exporter |
| `run_state=QUEUED|EXECUTING|VALIDATING|PREPARING_PUBLISH|PUBLISHING` | run 受 attempt/lease/fence/resource supervisor 管理 | 用 status/events 观察 |
| `WAITING_RESOURCE` | OS 明确 LowMemory 信号、任务自身 hard cap 或按预测所需的 X 空间不足 | 不提高 cap；保留 durable task，条件恢复后继续；等待 deadline 只告警、不终止 |
| 性能 warning | 可比 workload 吞吐退化 | 记录 telemetry；不暂停、不阻断、不改变 pressure rung |
| `WAITING_SOURCE` | required source 尚未完成，或 resolution source freeze 期间检测到 writer/snapshot drift | 保留同一 submission 等待自动重试；禁止重提、补零、减范围或放宽一致性检查 |
| `WAITING_ORPHAN_QUIESCENCE` | 旧 owned process tree 仍 alive/unknown | 只观察；不得 kill/delete lock |
| `BLOCKED_PROVIDER_TERMINAL` | 40203、overlap conflict、无完整 240 bars等 | 报告 code/date/pending scope，不循环重试 |
| `WAITING_SOURCE` + `BLOCKED_SOURCE_SNAPSHOT_DRIFT` | supervised resolution child 已用精确 schema/error/exception/hash/zero-safety 契约证明瞬时 source drift | 同一 durable submission 在 writer 稳定后重试，不计入 retry exhaustion；未知或伪造 child error 仍 terminal fail-closed |
| `SUCCEEDED/NO_OP_VERIFIED` | fresh probe 证明无需重导 | 读取 no-op receipt并完成报告 |
| `SUCCEEDED/REATTESTED` | 旧 candidate 未改，新增 attestation | 读取 attestation outcome |
| `SUCCEEDED/CANDIDATE_VALIDATED` | candidate 已原子发布并签收 | 检查逐组件 action 和完整 receipt |

`attempt.state=RUNNING` 是内部 attempt 状态，不等于公开 `run_state`。`worker_health.state=healthy` 仅表示
profile/config/capability 兼容和 heartbeat fresh，不代表某个数据 run 已完成。

读取 bounded events、terminal receipt 或一页日志：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 events --submission-id <submission_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 events --run-id <run_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 receipt --run-id <run_id>
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 log --run-id <run_id> --stream stdout --max-bytes 262144 --max-lines 1000
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
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v1 reattest-existing --latest
```

历史留存不是只保留 `all.txt`：凡被实验、训练、审计、正式发布或 production 引用的 release，必须完整、不可变地
保留 Qlib daily/minute、H5/static、指数上下文、PIT snapshot、`all.txt`、manifest、source/artifact digest 和
validation/attestation receipt。已有完整历史目录只登记 immutable path，不额外复制一份。仅从未发布且从未引用的
terminal-failed 临时候选可被标记为 cleanup candidate；流程永不自动删除，精确清理仍需独立授权。

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
- host available、system commit headroom、paging/pagefile 与 WSL host available 继续采集并写 receipt，但所有数值阈值仅为 warning telemetry，不参与 admission、checkpoint 或 terminal 判定；OS 明确 `LowMemoryResourceNotification` 仍可安全暂停；
- DB pool=4、row-producing query=1、provider request=1；
- minute batch/date chunk=20 stocks/3 months；factor H5/static 由单日切片与 Parquet row-group 控制；profile 中 `h5_batch=100` 仅为 v1 兼容遥测，不是生效的降压旋钮；
- Parquet row group/validation chunk≤100,000 rows；
- start free space=1.25×predicted remaining new bytes；没有预测值时不应用固定 32 GiB 保留，实际写盘失败仍 typed fail/wait。

允许的降压只有 profile pressure ladder：batch/chunk/row-group/workers逐级降低。不得通过减少股票、日期、字段、PIT、指数、H5 或验证范围换性能。
workload 分级仍由已校验的 durable scope 决定并写入 receipt，但历史 start reserve 字段只作兼容遥测。12 GiB
aggregate、Job/cgroup、WSL zero-swap、DB/provider 并发和 bounded batch 仍是任务自身 hard contract。全局持续换页、
available/commit/WSL host available、固定磁盘 floor 和性能退化不得阻断；receipt 必须披露 warning 与
`system_admission_thresholds_blocking=false`。

性能边界必须如实区分：

- candidate 物化支持 component/partition 级 `REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`。daily/minute
  canonical CSV 按 instrument/date range 保持不可变有序 segment；增量只追加 tail segment，selective repair 必须有
  explicit active-segment override，禁止把全历史拼成一个内存 frame；
- 大文件 writer 使用 single-copy deferred COW：final target 在 writer 完成前不出现在 candidate tree；writer private
  root 最多复制一次 baseline，quiescent/readback 后同卷 atomic rename，`final_recopy_count=0`。外部 writer 永远看不到
  COW hardlink tree，baseline Merkle 必须保持不变；
- minute source query 和 `dump_update` 在行物化前按 pressure rung 分批，hard max=20 codes，降压只能
  `20→10→5`；“先返回无界结果再切批”不合规；
- canonical v2 只为历史 D/P 股票扫描/保留日线子集；缺口按股票以 Tushare `pro_bar` 一次有界窗口请求，
  转换到 DB 的厘/手单位后只写 candidate CAS overlay。数据库重叠键逐字段不一致、overlay 仍缺键或单股票
  返回超过 3,000 行均 fail closed；若 provider 仍无数据，只允许最后一根权威 bar 之后、直到 terminal PIT
  span 结束的严格连续尾段作为 non-trading coverage，不伪造 OHLCV；内部断点、活跃证券尾部或尾段后又有
  权威 bar 仍硬阻断。不会把八年全市场 panel 保留在内存；
- canonical PIT v2 内的 `stk_limit` 缺口不使用 NaN、补零或“不可交易”替代。artifact-ready 阶段按版本化
  沪深主板/创业板/科创板规则，以 raw 前收和 `adj_prev/adj_current` 生成 missing-or-incomplete candidate CAS overlay；
  完整 DB 行不得覆盖；不完整 DB 行只在全部既有非空字段与派生值分币一致时补全。overlay 绑定精确股票代码和月份，首次采用只生成这些代码的 full-history override，禁止
  把稀疏缺口误分类为普通 tail 或全市场重导；未知板块、无涨跌幅日、缺参考输入或 unresolved 键均阻断；
- 在没有可信 DB partition revision ledger 时，初次 source freeze 与 publish 前 DB-only recheck 仍可能分别做一次截止日内的全值扫描；MVCC/provenance watermark 不能替代内容一致性；
- 因此“候选只重写新增/失效部分”不等于“整条月更只读取新增月份”；等待、DB read、provider、compute、validation 时间必须分项报告；
- fixture/synthetic benchmark 只证明算法复杂度和内存上限，不能宣称真实全量耗时；真实 full/new-cutoff telemetry 只能在未来数据更新另获授权后产生；
- 若未来要消除两次全值扫描，需要新的可信 revision-ledger F2、DEV 验证和 production DDL/DML 目标授权，本实现不暗中创建该账本。

### 8.1 阻断治理

- 自动处理：`bak_basic` 合法空日、精确指数缺键、`stk_limit` 缺失/不完整键、合规 terminal daily 尾段。
- 可重试：provider 限流、网络错误、上游数据尚未发布；保留 intent/checkpoint，不改写为永久合同失败。
- 硬阻断：权威值冲突、PIT/日期/identity 损坏、内部日线断点、必要推导输入缺失、越权写入/激活、
  无依据扩大为全量构建或资源安全越界。
- 未来新增或扩大任何门禁（包括等待、重试、阻断和失败条件）前，必须逐项向用户提交触发条件、发生概率、
  误阻成本、准确性风险和替代方案，经用户逐一批准后才可实现；不得用测试或文档先行制造新门禁。

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
