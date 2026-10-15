# LocalSIM 两账户回放恢复与正式接续长任务计划

关联：BUG-1929 / Issue #6157。本计划由用户“规划完成后直接执行”授权，按阶段连续执行；源码修复、加载验收、生产回放、正式接续分别记账，不把技术验收当收益证明。

## 目标与保留身份

| 账户 | 原任务 | 当前完成前缀 | 当前阻塞日 | 初始资金 |
|---|---|---|---|---|
| TCN | `lsreplay_4c2e743bea7789e6` | 2026-05-08 | 2026-05-11 | 500 万元 |
| LSTM | `lsreplay_81ca085e8ec56e05` | 2026-01-20 | 2026-01-21 | 500 万元 |

保留 job/run、账户、release/binding/manifest、股票池与政策身份；不新建替代回放、不手改终态或断点、不重做成功日。重启由用户执行。数据补录若确有必要，先出具体 DEV、生产目标与回读方案，不能用本计划推导任意生产 SQL 授权。

## Phase 0：文档及接口发现（已完成）

- 2026-10-16 本轮基线：源码和后端 identity 均为 `18817c00d61cb9b503b90f70583f4d54df769178`。
- 权威只读证据：`X:/AIstock_temp/LOCALSIM/workflow/LOCALSIM-longtask-phase0-authoritative-market-20261016-r1.json`。301139.SZ/05-11 与 603616.SH/01-21 都是 `market.suspend_d` 的全天 S 停牌，`stk_limit.pre_close=NaN`。
- 两个原失败日七类经济表均为零。LSTM 仍有 18 条 `backend_id=local_sim` 的内存句柄，失败上下文明示 `economic_commit_staged=false`。这只是核销调查输入，不自动证明可忽略句柄。
- `docs/analysis/localsim_replay_recovery_contract_fix_20261015.md`：恢复、锁、CAS、成功前缀及旧 C 投影禁止复用规则。
- `backend/services/simulation_execution/localsim/runtime.py::LocalSimBackend.submit_order_intent`：历史输入加载先于 OMS 建单；`load_authoritative_position_marks` 的停牌分支目前强制 frozen pre_close。
- `backend/services/strategy_package/reference_prices.py::load_reference_prices(symbols, as_of_date, conn_factory)`：公开只读 PIT 权威收盘参考与完整可用性证据，可复用而不修改其算法。
- `backend/services/simulation_runtime/localsim_replay_recovery.py::{inspect,evaluate,recover}` 与 `successor_repository.py::recover_terminal_replay`：现有受控恢复入口及原子事务范式。
- 已验证 API：GET `/api/v1/runtime-identity`；GET `/api/v1/simulation-runtime/localsim/replays/{id}`、`/{id}/recovery`；POST `/{id}/recover`，请求仅含 `expected_version`、`snapshot_sha256`。
- 正式接续必须使用 `localsim_product_control.py`、`localsim_replay_runtime.py` 的实际原生激活/追赶合同；到该阶段再次核实具体路由和 schema，不自行拼造 API 或修改 end/cursor。

## Phase 1：合同设计（本计划完成后直接实施）

1. 将“禁止交易的完整停牌事实”和“估值所需的权威历史报价”分离；不填补、推算或覆盖交易 pre_close。
2. 估值参考只读取合法截止 C 已知的真实 raw 收盘报价，复用共享参考价可用性合同；必须附报价日、截止日、停牌事实、价格口径和 hash，不能使用上一日 09:33 的账本 mark、成本价、涨跌停反推值或未来 D 收盘价。
3. 单独制定内存句柄核销合同：验证全部句柄与原计划意图一致、纯 LocalSim 来源、经济/执行/outbox/收据事实为零及明确的提交前持久化失败；在经济锁和 CAS 下写不可变核销凭据，保留原始证据。不能仅凭 `broker_called=false` 或七类表为零放行。
4. 合同细节、拒绝矩阵和审核项写入 `localsim_suspension_valuation_and_retirement_contract_20261016.md`。如原生证据无法证明上述前提，继续 BLOCKED，记录精确接口缺口。

验收：合同覆盖两种停牌案例、普通可交易股真缺价、部分时段停牌、未知副作用、冻结身份和旧 mark hash 兼容；遵循 DESIGN-COMPLIANCE-001 四项检查。

## Phase 2：程序修复与受控恢复

1. 在 LocalSIM owner 内完成停牌资格校验顺序与估值薄适配器，复用已有纯共享推理和参考价接口。
2. 复用现有恢复事务，增加带完整身份和摘要的旧内存提交核销收据；scheduler 重建时再次检查实际经济事实及收据谱系。
3. 原生日重建重做 D 日资格投影；合法完整原始评分按既有合同原样复用，缺少冻结原始评分才独立计算。
4. 新文件及修改必须在 BUG 注册范围内；扩展范围先更新 BUG。不修改 Advisory、QE、StrategyPackage、HMM、数据集或 data_service。

验收：停牌没有发出可成交意图；持仓估值可追溯且非 pre_close 补值；核销只接受明确可证明的纯内存失败；未知/已落账/跨后端继续拒绝。

## Phase 3：多轮审核、DEV 与交付

- 第一轮：交易资格、估值时钟及原生共享调用边界。
- 第二轮：锁、CAS、完整经济事实、核销证据和中断恢复。
- 第三轮及后续：按真实原计划/冻结输入复核，失败节点修复后只重跑受影响节点。
- 本地最小 gate：定向不变量/拒绝测试、lint、编译、diff/scope；一次最终相关小矩阵。
- 使用现有 DEV 数据库验证原子提交、回滚、并发、重复请求及失败恢复；所有夹具回滚，不创建新库。
- CI 完成所需 simulation core 计划；宽回归交给既有 CI/nightly。遵循最终 HEAD 绑定的绿灯证据。
- 满足合入条件后按已有授权 PR/合入；用户重启后核对实际加载，生成原生 post-restart receipt。

停止条件：运行仍为旧代码时不调用依赖新合同的生产恢复；不能通过当前进程补丁绕过重启。

## Phase 4：原任务恢复与连续多日验收

- 分别执行只读检查 → 原生恢复命令 → scheduler 接续；每个命令绑定当前版本与摘要，不重发已受理旧请求。
- 核对 TCN 05-11 与 LSTM 01-21 原 run 身份、原始评分、D 资格、目标数量、订单/成交/现金/持仓、经济与投影收据。
- 连续至少三个交易日推进，检查无重单、无成功日前缀改写、无跨日股票池/输入复用、无孤立经济/outbox 状态。
- 停牌、涨跌停或历史 TWAP 残余按原生分类验收，不将合法未完成调仓称为程序失败。
- 实测每天计算与端到端耗时、内存和磁盘；不预设每天 40 秒。

## Phase 5：全程回放与正式 LocalSIM 接续

1. 由原生调度器继续至冻结历史边界，逐次处理真实阻塞；记录失败指纹并区分数据完整性、市场限制和程序故障。
2. 通过原生 catch-up/safe-boundary/activation 合同追赶至合法最新完成日；保留原账户经济连续性。正式接续仅限 LocalSIM，不涉及券商真实资金。
3. 验证激活后的 release/binding、行情来源、调度归属、首个实时日运行与账本一致性；禁止同时运行两套消费者。
4. 收益按真实估值时点单独报告；全程回放或实时激活未完成时，明确保留待验收状态。

## Phase 6：收尾与断点交接

- 通过单 BUG 原生重启验收与 close-sync，同步 GitHub/BUG、main，清理本任务已合入且无活动引用的工作树/分支。
- 成功日志与缓存仅按正式清理 manifest 处理；运行中的 X 盘模型引用、数据和回放产物不在临时清理范围内。
- Windows 临时文件统一在 `X:/AIstock_temp/LOCALSIM`，WSL 路径必须验证可访问；不复制大型模型与数据，不使用 C 盘临时目录。
- 每阶段记录证据路径、状态、下一命令及阻塞。上下文切换后从本计划和 `resume BUG-1929` 继续，不重启任务。

## 当前状态

- Phase 0：完成。
- Phase 1：计划与合同完成。
- Phase 2：源码实现完成；两个原断点候选代码只读检查 ready，原生产状态未变。
- Phase 3：三轮针对性检查及修复，相关回归最终 540 passed/2 skipped，完整 DEV 核心 4 passed；最后反例/工作流/CI/PR 交付继续执行。
- Phase 4—6：等待合入、用户重启和原生恢复；长任务保持 active，未宣称完成生产验收。
- 前向检查发现 603616.SH/01-22 的 S/R 并存及 01-23 C 参考可用性缺口，证据 `X:/AIstock_temp/LOCALSIM/workflow/BUG-1929-forward-input-gap-readonly-r1.json`；保留明确阻塞，不在 LocalSIM 推算价格或修改数据。
