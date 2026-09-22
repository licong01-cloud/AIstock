# 共享月度数据集发布 v2 运行手册

本文是 `qe_hmm_full_v2` 新月份发布的操作手册。业务状态唯一保存在
`MonthlyReleaseService`；API、CLI、MCP 和 UI 只是同一后端入口。

## 1. 每月标准流程

1. 确认上一自然月已闭合，并由后端解析最后交易日；不手工猜 cutoff。
2. 调用 `plan`，核对 active predecessor、目标 generation/revision、三节点路径和九个 SOURCE gate。
3. 用固定 idempotency key 提交一次 `prepare_only` operation。
4. worker 在单一只读一致性 snapshot 中读取尾部与受管历史修订，完成 SOURCE gate；有缺口则输出 typed repair plan 并停止。
5. 修复由既有 local_data 受控作业执行；DEV、生产授权、生产 apply、readback 分开。月更 worker 不执行隐式 DML。
6. `resume` 后按 action plan 复用、追加、选择性重建或组件重建；只写新的私有 successor。
7. 生成 HMM 派生资产、完整 manifest/profile candidate、release closure，并执行本地与 Windows/WSL/node1 consumer validation。
8. 状态仅在六池、PIT、sector、dataset identity、QE/HMM/因子/荐股/择时/统一回测全部通过后成为 `READY_TO_ACTIVATE`。
9. 获得独立 activation authorization 后执行一次 CAS 激活并读回；失败时保持旧 active。
10. 激活后只验证新任务解析到新 identity，不运行正式训练或实验。

## 2. CLI

token 只从 `DATASET_RELEASE_OPERATOR_TOKEN_FILE` 指向的绝对普通文件读取，
不得出现在命令、日志或 receipt 中。

```powershell
python scripts/monthly_unified_dataset_release.py plan --cutoff 2026-09-30 --idempotency-key monthly-202609-plan
python scripts/monthly_unified_dataset_release.py run --cutoff 2026-09-30 --idempotency-key monthly-202609-run
python scripts/monthly_unified_dataset_release.py status --operation-id dmr_<32hex>
python scripts/monthly_unified_dataset_release.py receipts --operation-id dmr_<32hex>
python scripts/monthly_unified_dataset_release.py resume --operation-id dmr_<32hex>
```

激活与回滚只在用户针对具体 operation 授权后执行：

```powershell
python scripts/monthly_unified_dataset_release.py activate --operation-id dmr_<32hex> --authorization-ref dsauth_<32hex>
python scripts/monthly_unified_dataset_release.py rollback --operation-id dmr_<32hex> --authorization-ref dsauth_<32hex>
```

## 3. MCP

`qlib_monthly_release_*` 工具与 CLI 调用同一 API。`plan/status/receipts`
只读；submit/resume/cancel 使用 `RUN_MONTHLY_DATASET_RELEASE`；activate/rollback
使用 `ACTIVATE_MONTHLY_DATASET_RELEASE` 并仍要求独立 authorization ref。

## 4. Fail-closed 与恢复

- SOURCE 未闭合：查看 gate receipt 和 typed repair plan，修复后 resume；不得缩小人口或放宽字段。
- BUILD 中断：只复用 SHA/size/producer identity 全部有效的 checkpoint；无效输出不得被采用。
- DEPLOY 单节点失败：旧 active 不变；只恢复同一 operation，不创建 QE/HMM 分支候选。
- CONSUMER_VALIDATE 失败：记录具体 consumer、节点、窗口和身份；不得静默退回旧路径。
- 激活并发冲突：CAS 失败且 active 不变；重新读取后决定是否仍需切换。

## 5. 独立状态报告

最终报告必须分别列出：源码/CI、数据库 DDL/DML、source snapshot、candidate 构建、
三节点部署、consumer validation、profile activation、运行态读回、训练/实验、进程控制。
只有 release closure、consumer readback 和激活后 identity 均一致，才能报告本月发布完成。

## 6. Durable worker

API、CLI、MCP 只提交和查询 operation；实际六阶段工作只由代码固定的 worker registry 执行。
worker 运行前需由运行态所有者配置以下绝对路径或节点身份，配置文件只保存位置，不保存凭据：

- `AISTOCK_MONTHLY_RELEASE_STATE_ROOT`
- `AISTOCK_ACTIVE_DATASET_PROFILE_PATH`
- `AISTOCK_MONTHLY_CONTROLLER_RELEASE_ROOT`
- `AISTOCK_MONTHLY_PROFILE_CANDIDATE_ROOT`
- `AISTOCK_MONTHLY_RELEASE_ARTIFACT_ROOT`
- `AISTOCK_MONTHLY_WSL_RELEASE_ROOT`
- `AISTOCK_MONTHLY_NODE1_RELEASE_ROOT`
- `AISTOCK_DATASET_ACTION_AUTHORIZATION_ROOT`
- `AISTOCK_MONTHLY_HMM_AUTHORITY_PATH`
- `AISTOCK_MONTHLY_WSL_DISTRO`
- `AISTOCK_MONTHLY_WSL_PROJECT_ROOT`
- `AISTOCK_MONTHLY_WSL_PYTHON`
- `AISTOCK_MONTHLY_NODE1_SSH_HOST`
- `AISTOCK_MONTHLY_NODE1_PROJECT_ROOT`
- `AISTOCK_MONTHLY_NODE1_PYTHON`

`AISTOCK_MONTHLY_NODE1_SSH_HOST` 必须写直接的 `user@hostname` 或 `user@IP`，不得依赖个人
SSH config 中的 alias。正式 worker 固定使用 `ssh -F NUL`，仅采用系统默认密钥位置和显式参数，避免
`C:\Users\<user>\.ssh\config` 的权限、所有者或个人别名漂移影响月更发布。

RD-Agent Results API 首次部署动态 release reader 时，WSL 与 node1 各自只需一次性配置固定 registry 根：

- WSL：`QE_DATASET_RELEASE_REGISTRY_ROOTS=<AISTOCK_MONTHLY_WSL_RELEASE_ROOT>/.aistock-release-registry`
- node1：`QE_DATASET_RELEASE_REGISTRY_ROOTS=<AISTOCK_MONTHLY_NODE1_RELEASE_ROOT>/.aistock-release-registry`

该环境变量指向稳定的 registry 目录，不指向某个月 candidate。部署阶段会按 dataset manifest identity
create-exclusive 写入登记；运行中的 API 每次请求重新验证登记、candidate 目录名、manifest 字节 SHA 和
canonical identity，因此后续月份不得再修改该环境变量或为数据切换重启 API。首次代码/环境配置生效仍需由运行态所有者执行一次目标服务重启。

部署代码后先做无任务领取、无数据库连接、无候选写入的只读组合预检：

```powershell
python scripts/monthly_unified_dataset_release_worker.py --preflight
```

首次配置或正式 HMM 模型/预设发生变更时，必须先把已经批准且已绑定当前 active manifest 的完整窗口系数产物
封存为 repo-external authority；不得由 worker 查询模型数据库、猜测 preset 或采用旧 release 产物。静态 state-label
模型可由工具确定性生成空 config；动态模型必须显式提供已冻结 config：

```powershell
python scripts/dataset_release_hmm_authority.py `
  --source-coefficients <approved-full-window-coefficients.json> `
  --model-path <approved-models.json> `
  --producer-script <repo>\scripts\precompute_hmm_coefficients.py `
  --output-root <repo-external-create-exclusive-root> `
  --authority-id <approved-authority-id> `
  --asset-id qe-hmm-preset-a-full-window `
  --expected-dataset-manifest-sha256 <active-manifest-identity> `
  --backtest-lag-trade-days 1
```

生成目录同时保存模型、原正式系数产物、config、authority 与 bootstrap receipt 的真实字节和哈希；源 manifest、
模型 SHA、日期网格、state labels 或动态 config 任一不闭合即失败。后续月更只读取
`AISTOCK_MONTHLY_HMM_AUTHORITY_PATH` 指向的 authority；固定模型未变时无需逐月重建 authority。

受控运行方式为：

```powershell
python scripts/monthly_unified_dataset_release_worker.py --once
python scripts/monthly_unified_dataset_release_worker.py --drain --max-operations 10
python scripts/monthly_unified_dataset_release_worker.py --serve --poll-seconds 5
```

`--drain` 必须有上限；`--serve` 仅消费已经持久化的 operation，并响应 SIGINT/SIGTERM
协作退出。worker 不接受调用方指定 producer 命令、组件子集或候选路径。WSL 与 node1
使用固定节点命令进行流式不可变部署和本机 readback；控制器成功不能替代节点成功。

`prepare_only` 到 `READY_TO_ACTIVATE` 后停止。只有 operation 自身从提交时就绑定
`activate_when_ready` 和精确 `dsauth_*`，worker 才能执行中央 profile CAS；不能从普通
prepare 授权推导激活权限。首次部署 worker 代码可能需要运行态所有者安排进程更新，之后每月
release 切换只改变中央 profile，不需要按月修改 QE/HMM/荐股路径或重启节点 API。

## 7. Advisory 与择时离线准备绑定

首次部署本版本并由运行态所有者重启 AIstock 后端后，Advisory 和 position_timing 的新建离线数据准备
通过以下入口冻结共享 active release。两者复用 `AISTOCK_MONTHLY_RELEASE_STATE_ROOT`，不增加逐月路径配置：

- `POST /api/v1/advisory/dataset-preparations`
- `POST /api/v1/position-timing/dataset-preparations`

请求头必须提供稳定的 `Idempotency-Key`，请求体仅包含 `start_date`、`end_date`。首次调用在
`managed-consumer-tasks` 中 create-exclusive 保存 profile、manifest、consumer component binding 和窗口；
同一业务键重试只读取既有回执，不重新解析 active profile。相同键改变窗口返回冲突，窗口超过冻结 cutoff、
binding/hash 漂移或缺少 active profile 均 fail closed。切换 active profile 只影响新的业务键，不改写历史请求、
研究结果或原 Advisory CAS；入口本身不读取收益、不启动训练/实验，也不执行数据库或运行态动作。
