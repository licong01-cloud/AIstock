# LocalSIM 选股前失败的原任务受控恢复（BUG-1952）

关联 Issue #6226，接续既有 `localsim_replay_recovery_contract_fix_20261015.md` 的 R01–R08。范围为 LocalSIM 原生恢复及消费者传输层，不修改共享选股算法、策略包、行情数据、交易政策和账务算法，不增加 DDL。

## 真实触发与修复

原 TCN 任务 `lsreplay_4c2e743bea7789e6` 在完成 2026-06-08 后，于 06-09 的 WSL `cat path-witness.txt` 检查超时。异常发生于选股前，当日尚无 `simulation_daily_run`，旧恢复入口要求存在失败日运行，因而返回 `LOCALSIM_REPLAY_RECOVERY_DAILY_RUN_INVALID`。这是传输失败及恢复覆盖缺口，不是涨跌停或市场成交余量。

1. 仅为能够精确识别的旧 `TimeoutExpired`、WSL `cat`、20 秒路径检查增加 PRESELECTION 恢复分支。路径必须属于 X 盘 LocalSIM 原生请求、独立选股 run 和 node；拒绝其他命令、路径、异常及未知故障。
2. 保留原 source/calendar/day-engine、成功前缀、next day、账户、release/binding/package/manifest 身份检查。查询目标账户当日的所有日运行，任一其他日运行或缺失查询证据均拒绝。候选 run ID 调用原生日运行身份构造，不另造身份算法。
3. 从候选原生日运行 ID 查询原生七类经济事实；必须全部为零，不以“没有日运行”代替无经济事实证明。GET 通过同一个原生传输函数重读 Windows 和 WSL witness 内容，不创建目录、不提交状态、不运行选股或交易。
4. POST 保留原请求的 `expected_version` 和 `snapshot_sha256`，锁定原 job/account，取得原生 2403 日运行经济写锁后重新读回全部事实。新建 LocalSIM 日运行也取得同一写锁，防止缺失行不能加行锁时出现检查与创建竞态。已有日运行的恢复、句柄核销及计划重建合同保持。
5. 恢复只使原任务具备调度重试资格，不制造日运行、选股证据或交易计划。恢复收据随同 CAS 在现有 `paper_v2.config_change_audit` 中原子记录，`object_type=LOCALSIM_REPLAY`、`change_type=RECOVER_PRESELECTION`，并检查唯一回读。原生后续运行清除临时失败字段时，审计收据仍保留；不改变正常状态不得保留失败字段的模型合同。
6. 后续同类路径超时转换为有明确阶段的 `DataUnavailableError / LOCALSIM_WSL_X_PATH_TIMEOUT`，由既有生命周期重试；不记录可能携带敏感命令的原始 TimeoutExpired 对象。

## 验收索引

| 条款 | 实现 | 证据与结论 |
|---|---|---|
| N01 精确旧故障、身份和无日运行证据 | recovery 服务 PRESELECTION 分支、successor 原生快照 | 未知故障、其他命令、身份变化、其他日运行、经济事实非零均拒绝 |
| N02 原子恢复、CAS、互斥和回滚 | successor 恢复事务、runtime 日运行创建 | 现有 DEV 合同覆盖旧摘要、重复请求、外部写锁、注入回读失败和审计回滚 |
| N03 旧日运行恢复和成功前缀保持 | 原恢复分支、原任务 version/cursor 校验 | 原有退役/估值恢复测试保留；真实候选检查前后原任务完全一致 |
| N04 新路径失败原生重试且不泄露命令 | 同一个 WSL witness probe | 传输定向测试及既有生命周期回归 |
| N05 只读候选与运行激活分开 | 独立进程只读数据库事务 | `BUG-1952-source-candidate-native-readonly-r1.json`：v4709 ready、run_id=null、七类经济事实均零，约 1.319 秒；未调用 recover |

定向矩阵 147 项通过。现有 DEV nox 门禁的最终回执另见本 BUG 工作流验证证据；所有夹具回滚。第一次 DEV 全门禁因新增事务锁与旧测试中阻塞取锁的方式冲突而中断，修正为非阻塞互斥验证后重新执行；这不是生产服务异常。

三轮审核分别检查故障范围及冻结身份、无行场景的写入竞态及审计原子性、真实原任务的独立进程只读输入。147 项矩阵通过后不重复扩大本地测试；跨模块 `simulation_core_l2` 交给源码 PR CI，深度场景按既有 nightly 计划去重。

## DESIGN-COMPLIANCE-001

| 条款 | 审核 |
|---|---|
| 禁止简化交付 | 包含精确原生检查、原子恢复、互斥、收据及真实只读证明；生产接续和完整回放另行验收 |
| 禁止静默错误 | 未知故障、副作用、旧摘要、写锁冲突、路径不可用及回读失败全部显式拒绝 |
| 禁止改变业务逻辑 | 不改订单、资金、持仓、账本、市场资格和收益；保留原任务、成功前缀和正常状态合同 |
| 禁止私增门禁审批 | 复用原 recover API 和原版本/摘要合同，无 force、额外请求参数或人工审批 |

源码候选检查不代表生产后端已加载。合入、用户重启、原任务正式 recover、连续多日、全程回放及后续正式 LocalSIM 激活分别报告；技术通过不代表收益证明。
