# 已封存融资及分钟修正版的正式接续

BUG-2079 / #6637。九月 active 仍为 `…4ab0b48642c3`，融资修订 `…-margin1` 的正式操作已 READY 但未激活；分钟修正版 `…-margin1-minute1` 已继承融资修订并封存。原接口仅支持单段融资或 quote-only 证明，不能正式接续这一候选。

本修复仍使用 `POST /api/v1/qlib/monthly-releases/adopt` 和既有独立授权、激活接口，不新增发布脚本或绕过后端写操作。采纳不会重导、训练、写数据库或切换 active。

## 两段身份与严格证据

1. logical predecessor：当前 active profile SHA `169f90b07e484fd4ff53041550d55fc95f830e9b6fcabcadf247ecc8ca2583b7`。
2. physical predecessor：未激活 margin1 profile SHA `9f2f07451b89a596788880adf2c76dffc472bac01fa76cabd1066de03e643966`，操作 `dmr_304c3e2dffdd448c8b4a8927158fc0ed`。复用原融资校验器验证 logical→physical 原始证明，不把新分钟候选伪装为融资回执。
3. target：minute1 profile SHA `dcc4db093f28696ef4f51419292fe9bf607fd4963f1d29fc1093177423997549`；manifest identity `3760d9b257c2870d2b0db4c48576e90f9246add45553d5b3592a0ccea3148e7b`；manifest 文件 SHA `f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`。

接口要求三种不可混合或重复的 typed proof：`aistock_monthly_minute_coverage_amendment_v1`、`aistock_monthly_minute_full_window_readback_v1`、`aistock_monthly_minute_successor_readback_v1`。最后一份明确三个节点、两段 profile hash、待接替 operation，以及各节点实际 manifest、逐证券日/字段、原 QE consumer 代码 hash 与完整覆盖结果。

仅允许修复清单的 12 个分钟字段、对应双布局、明确列出的 instruments/meta/materialization/修复报告发生变化；其他 manifest pins、语义、PIT、日频、因子、日历不得漂移。逐节点零未知缺口，文件 bytes、已固定报告和候选 manifest 必须一致。

## 正式操作及中断恢复

在 product 与 active-profile 两把既有锁内重验当前 active 和精确未激活前驱。先持久化新操作 READY，再创建旧操作 `receipts/superseded.json` 并置 CANCELLED；旧 request、plan、ready、候选与原授权保留。不得先激活 margin1 来绕过同 cutoff 冲突。

同 idempotency key 与同请求可以补完中断的退役步骤，不创建第二份操作。新 request 已持久化而旧退役未完成时，两份候选均不能激活；已 superseded 的旧操作不能 resume。不同 target、已存在 activation intent、active 漂移或证据变化均拒绝。

采纳只产生新的 READY，激活仍要求既有 `dsauth_*` 绑定精确 operation、target profile 与 logical active 前驱。回退亦由原授权/CAS流程执行。此代码不授权任何服务重启或生产激活。

## 验证与部署边界

- 原有采纳与新增流程：17 项测试通过；另有两项注入中断测试验证 marker 写入前后均禁止激活，重放同请求补完而不创建新操作。
- 实际候选只读校验通过：三个节点各 101 个证券日、291792 个字段单元；原 QE consumer 5181 个预期区间、141 个权威停牌解释、0 个未知缺口。实际回执位于 `F:/Dev/AIstock/tmp/handoff/qe-mainline-continuation-20261023/minute-adoption-real-validation.json` 及同目录三节点读回文件。这些是本机运行证据，测试数据不是生产覆盖证明。
- source 持久化后需由既有 `worker-scheduler` 运行目标加载：backend 主进程及其监督的 monthly worker。调用闭包为 `backend/routers/monthly_dataset_releases.py` → `MonthlyReleaseService.adopt_existing_successor/activate` → 新模块。
- runbook：`docs/operations/monthly_unified_dataset_release_v2_runbook.md`。当前仍是源代码验证完成、合并/运行时加载/正式采纳/独立激活未完成；本轮没有重启服务。

DESIGN-COMPLIANCE-001：问题范围为既有统一发布的两段接续；实现覆盖完整证明、同 cutoff 排他、可恢复退役和激活隔离；直接测试含失败与中断分支；业务实际采纳和激活明确待办，不将纯校验称发布完成。
