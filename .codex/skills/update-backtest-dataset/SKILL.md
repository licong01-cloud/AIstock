---
name: update-backtest-dataset
description: Operate the durable, candidate-only AIstock monthly QE backtest dataset release workflow. Use for 每月更新/补齐回测数据集, one-click monthly update, Qlib daily/minute and H5/static/index context refresh, same-cutoff NO_OP, existing-candidate re-attestation, catalog/status/resume interpretation, dataset signoff, or investigating resource/provider/source blockers. Keeps PIT, moneyflow, TDX-first minute, 12-index/HMM and immutable release contracts; never activates or overwrites production without separate explicit authorization.
---

# 月度更新 AIstock QE 回测数据集

把 Skill 当作薄编排层。durable authority 是版本化 profile、SQLite control catalog、immutable CAS、release/attestation receipt 和独立 Worker；聊天、Skill 文本、日志摘要都不是运行状态 authority。

## 先选动作

| 用户目标 | 动作 |
|---|---|
| 查看最新状态、判断是否需要更新 | 运行 `status --latest`；不提交任务 |
| 普通月更或推进到指定 cutoff | 只提交一次 `monthly --candidate-only` |
| 同 cutoff 已有 release | 仍走 `monthly`；让 fresh source probe 决定 `NO_OP_VERIFIED`，不要重导 |
| 复核旧的不可变 candidate | 先 catalog，再走 `reattest-existing`；只写独立 attestation |
| 查看复用、增量或失效原因 | 读取 decision/component actions、fingerprints 和 receipt |
| 修代码、做 fixture 验证 | 使用 feature/BUG lane；不得把测试变成真实导出 |
| 首次 PIT v2 迁移 | 只使用仓库白名单 `initial-migration` plan；先 sample、验收后再由独立授权运行 full |
| 激活生产、同步 node1、启动/注册 Worker 或 scheduler | 这是独立授权，不由本 Skill 推导 |

普通 operator 入口：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration --plan pit_v2_initial_20260731_v1 --scope sample --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v1 reattest-existing --latest
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 events --run-id <run_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 receipt --run-id <run_id>
```

`qe_hmm_full_v2` 是 future canonical candidate profile；`qe_hmm_full_v1` 只保留既有 release 的复现与只读重验。
源码交付不等于 runtime activation，未完成 v2 全量验证和独立激活授权前不得把 production 默认值切到 v2。
`initial-migration` 不是普通月更参数化捷径：plan id、canonical plan digest、固定 `2026-07-31` cutoff、
sample instruments/event windows 和 candidate-only safety 必须同时由 control service、Worker resolution reader 与
build receipt 复验。`sample` binding 不得进入 QE/训练；源码或 fixture 验证不得执行该真实提交命令。

`monthly` 成功响应返回随机 `idempotency_key`；只在重试同一次调用时显式复用。未显式给 key 的下一次人工
调用必须形成新 submission/fresh probe，resolution 再按 source identity 复用既有 run/release。

RTK 首次 wrapper 失败、不支持或需要精确原始输出时可直接运行等价 `python` 命令并记录原因。不要把 RTK 可用性当作数据发布门禁。

## 每次执行

1. 新月更读取 `configs/datasets/qe_backtest_monthly_v2.yaml`；历史复现读取 v1。不要复用聊天中的 cutoff、路径、资源值或旧 schema 路径。
2. 确认请求是只读状态、candidate 月更、re-attest，还是独立 production/runtime 动作。授权不得跨类别继承。
3. 对月更只提交一次 durable intent。默认 profile、cutoff policy、reuse/resume 和 sample policy 由 profile/control plane 决定。
4. 用 `status --latest` 或受保护 API 查询 submission/run/events。Worker 未运行时是
   `submission_state=QUEUED_RESOLUTION` 与 `worker_health.state=unavailable|stale|blocked`；执行中公开状态是
   `run_state=EXECUTING|VALIDATING|PREPARING_PUBLISH|PUBLISHING`。不得在 CLI、API 或 Skill 内代跑重任务。
5. 按 typed outcome 处理：
   - `NO_OP_VERIFIED`：fresh probe 证明 source/PIT/validation 未变；没有重导。
   - `REATTESTED`：旧 candidate 未改写；新 attestation 位于 control catalog/CAS。
   - `CANDIDATE_VALIDATED`：读取逐组件 `REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`，不得笼统称全量或增量。
   - `WAITING_*`：保留任务、lease 和 checkpoint，等待条件恢复。
   - `BLOCKED_*`/`FAILED_*`：按 error code 报告；不要换 provider、补零、减范围或调用旧 exporter 绕过。
6. 只有 terminal receipt 与 catalog readback 一致、全部 required validation PASS，才报告 candidate signoff。production/node1/runtime 状态始终另列。

## 不可突破的边界

- 默认始终 `candidate-only`。不得覆盖或原地追加现有 candidate；不得切 production pointer/symlink。
- 不自动初始化/迁移真实 control root，不启动、停止、重启或注册 backend、Worker、scheduler、QE、RDAgent、WSL task 或 node1。
- planner/source acquisition 只读 DB。provider 只进入 candidate-local immutable CAS/overlay，不写 DB；DB repair 是独立动作。
- 历史 D/P 日线缺口使用 candidate-local Tushare `pro_bar` missing-only overlay；价格转厘、成交量转手、amount 千元转厘，DB/provider 重叠必须完全一致。若 Tushare 仍无数据，只允许最后权威 bar 后的严格 terminal PIT 连续尾段作为 non-trading coverage；内部断点或活跃证券尾部仍阻断。分钟缺口固定 TDX 优先、Tushare 次级；只补 missing keys。
- canonical PIT v2 内缺失或不完整 `stk_limit` 不填 NaN、不补零、也不直接标记不可交易：使用唯一版本化 A 股规则计算器，按沪深主板/创业板/科创板、交易日和 PIT ST 状态，以 `previous_raw_close × adj_prev / adj_current` 计算 raw 前收，再按 0.01 元和 `ROUND_HALF_UP` 生成 candidate CAS overlay。完整 DB 行永远优先且不得覆盖；不完整行仅在全部既有非空字段分币一致时补全。未知板块、无涨跌幅日、参考价/复权缺失、非空冲突、重复或 unresolved 键全部 fail closed。首次历史稀疏 overlay 必须携带精确 affected instruments，只允许对应股票的 `SELECTIVE_REBUILD`，不得扩大为全市场导出。
- 股票 universe 使用冻结 PIT spans；candidate 保存 canonical snapshot/hash。不得用当前 ST 列表、实验黑名单或 max-date/count 代替。
- v2 PIT 仅接受 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2`：252 个交易所交易日 IPO 暖机、历史退市股生命周期、公告 as-of 终止风险与 ST snapshot gap 闭环必须同时满足；rolling 与 frozen snapshot 必须同 rule/digest。
- DB 外 moneyflow 固定 `tushare_moneyflow_shares_yuan_v1`：量=股、额=元；`mf_total_net_*` 来自 canonical `net_mf_*`。
- static 固定 121 数据列、`l2_code_id int16`、unknown=`-1`。12 指数和 HMM `000300.SH` benchmark 不得运行时扩张或替换。
- 不降低 resource reserve、不提高 hard cap、不扩大并发。资源压力只能走 profile pressure ladder；不能减少股票、日期、字段、指数或验证。
- mixed/COW 只承诺候选物化按失效范围重写。在没有可信 DB revision ledger 时，初次 source freeze 与
  publish 前 DB-only recheck 仍可能各做一次 cutoff 内全值扫描；不得把它表述为整条链只读新增月。
- MVCC/provenance watermark 不作为内容复用证明。未来 revision ledger 需要独立 F2、DEV 验证和明确的
  production DDL/DML 授权；本 Skill 不创建或推导该授权。
- profile 冻结的 Ubuntu/conda/Qlib dump script/repo guardian-runner 路径和 SHA 是 hard gate；缺失或漂移
  fail closed，不 fallback、不临时安装依赖。
- re-attest、cancel、resume、publish recovery 都必须使用 durable identity/lease/fence；不要删除 lock、staging、receipt、candidate 或失败证据来“恢复”。
- 被 QE、训练、审计、正式发布或 production 引用的历史 release 必须保留完整不可变数据集；`all.txt` 只是索引证据，不能替代 daily/minute、H5/static、指数、PIT 和 receipts。未引用失败候选也只可登记为 cleanup candidate，Skill 不自动删除。
- 真实数据导出、生产激活、DB DDL/DML、进程控制和 cleanup 各自需要明确授权。

## 阻断设计治理

- 自动处理：登记的合法 sparse 空日（含 `bak_basic`）、精确指数候选补齐、`stk_limit` 规则补全、合规 terminal daily 尾段。
- 可重试：provider 限流/网络/尚未发布；保留 durable intent/checkpoint，不把暂态失败升级为永久合同阻断。
- 仅以下类别可硬阻断：权威值冲突；PIT/日期/identity 损坏；内部 required gap；必要推导输入缺失；越权覆盖/激活；无依据扩大重建范围；资源或安全合同违反。
- 不得自行新增或扩大硬阻断。任何新阻断设计必须先向用户报告触发条件、发生概率、误阻代价、准确性风险与替代方案，获得明确批准后才实施。
- DEV 只验证 DML 机制：`validate-dml` 单行 upsert/readback 后强制 rollback；不得要求 DEV 复制八年生产历史。生产仍需全范围只读 plan、目标明确的 DML 授权及 readback。

## 按需读取 references

- 普通月更、catalog、NO_OP、re-attest、status/resume：读 [monthly-workflow.md](references/monthly-workflow.md)。
- fingerprint、source revision、component reuse/invalidation、COW：读 [fingerprint-and-reuse.md](references/fingerprint-and-reuse.md)。
- Worker、lease、hard caps、pressure ladder、WAITING/orphan recovery：读 [resource-and-worker.md](references/resource-and-worker.md)。
- terminal outcome、receipt、attestation、signoff 和 production gates：读 [release-receipt.md](references/release-receipt.md)。
- 12 指数、单位、provider parity 与 HMM consumer 边界：读 [index-hmm-contract.md](references/index-hmm-contract.md)。
- `stk_limit` 规则派生、missing-only 身份和选择性失效：读 [fingerprint-and-reuse.md](references/fingerprint-and-reuse.md) 及 `docs/architecture/qe_stk_limit_rule_derived_overlay_f2_design_20260823.md`。

人类 operator 步骤见 `docs/operations/qe_backtest_dataset_monthly_update_runbook.md`。低层 exporter 与历史 moneyflow/PIT 细节见 `docs/analysis/qlib_backtest_dataset_export_guide_20260712.md`；它不是普通月更入口。

一次性 control-store init/status/migrate、零执行 `--preflight`、独立 Worker 启动授权、events/receipt/log 分页
命令都只以 runbook 为准；不要从聊天或旧日志拼装命令。

## 汇报

至少分别报告：

- profile、requested/effective cutoff、submission/logical request/run/release/attestation identity；
- run outcome 与每个 component/partition action；
- candidate/catalog/receipt 路径或 CAS ref、artifact/source/PIT/validation digest；
- PIT、moneyflow、daily/minute、12-index/HMM、QE/consumer smoke 结果；
- peak resource、wait/compute/provider time、query/read/write规模；
- retryable/waiting/blocked/terminal 状态与下一安全动作；
- `production_activation`、`node1_distribution`、`DB repair`、`runtime restart`、`cleanup` 的独立状态。

没有真实 full receipt 时明确写 `runtime_real_data_evidence=not_run_not_authorized`；fixture 或源码验证不得冒充真实数据完成。
