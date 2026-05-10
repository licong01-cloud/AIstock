# Paper v2 阻断点测试矩阵

日期：2026-05-09
归属：Claude Code 工作面（阻断点系列修复验证）
设计依据：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §0 / §7 + `paper_v2_blockers_20260508.md`（worktree 工作面，P0-A ~ P0-H + P1-A ~ P1-G）

## 模块定位

本矩阵覆盖 Paper v2 audit §0 / §7 提炼的 P0 / P1 阻断点的**修复验证**。每个阻断点对应：
- 修复前的失败 case（reproduce）
- 修复后的 pass case
- 不允许回归的反模式

不覆盖：阻断点的根因分析、修复方案设计（已在 audit + paper_v2_blockers_20260508.md 完成）。

| 维度 | 取值 |
| --- | --- |
| 模块 ID | `paper_v2_blockers` |
| 风险等级 | high（阻断点未修 = paper v2 不可投产；任一回归即 P0 退化） |
| 工作面 | Claude Code 维护（阻断点修复在 paper_trading_v2 / selection_center / strategy_package / frontend Claude 工作面） |
| 是否触动 main | 文档 + 修复 PR 走标准流程 |

## L0 静态守卫

L0 trigger：阻断点相关代码变更 / 修复 PR 提交。

- L0-G1：禁止"backtest_contract 锁住 6 项与 QE 一致"在 PR 中扩大锁范围（P0-A 解决方向是缩小，不是扩大）
- L0-G2：禁止 selection_center 路径出现 daily fallback 或 backtest pred.pkl 直接读（P0-G / Paper v2 oracle）
- L0-G3：禁止 live_session.py 引入 silent retry / silent fallback（P0-F 修复禁止退化路径）
- L0-G4：禁止 LEGACY_NON_ST_PIT 包默认获得 minqmt_sim / both 兼容性（与 P0-E 资产准入正向状态机一致）
- L0-G5：UI 不显示原始 Python traceback / 英文枚举值给用户（P1-D / P1-E）

pass criteria：semgrep + lint 通过；P0/P1 反模式 grep 0 命中。

## L1 单能力

L1 trigger：单个阻断点修复 case。

### L1-C1：P0-A backtest_contract 锁松绑
- **修复前 reproduce**：当前 `backend/services/strategy_package/backtest_contract.py` lines 59-93/125/234/406-556 强制锁 6 项（portfolio_policy / minute_execution / risk_policy / HMM / industry_blacklist / tradability / stock_pool）与 QE 一致；UI 形式上"可改"实际改不动
- **修复后 pass**：按 §8.1 用户决策（A 保留 / B 极简 / C 软合约）落地后，UI 修改后端实际生效或显式拒绝（不再"修了等于没改"）
- pass：用户改动 6 项任一 → 后端要么生效（C 选项）要么显式拒（A 选项），不允许"提交成功但实际没改"

### L1-C2：P0-B 三套执行栈一致性（cross-test 落地）
- **修复前 reproduce**：`quantevolver/executors/backtest.py:27` vs `selection_center/service.py:79` vs `paper_trading_v2/day_runner.py:77` 算法实现不同；同 manifest 三处可能产出不同持仓 / NAV
- **修复后 pass**：通过 `qe_paper_consistency.md` 的 Mode G smoke 验证（详见同目录），三 adapter OrderIntent byte-equal
- pass：Mode G smoke 9 case 前 5 case byte-equal；与 §A.3 Strategy Engine 实施联动

### L1-C3：P0-C 一键流程入口
- **修复前 reproduce**：用户从 QE source 到选股结果需 ≥8 次交互跨 2-3 页面
- **修复后 pass**：UI 提供"从 package 到 selection 一键"按钮 / 向导路径
- pass：用户操作步数计数 ≤3；交互跨页 ≤1；不允许"看似一键实际仍需 8 步"

### L1-C4：P0-D ST PIT universe spans 续期
- **修复前 reproduce**：spans 仅到 2026-04-30，落后于交易日；选股配置阶段不暴露，运行后才失败
- **修复后 pass**：spans 续期到当前交易日 + N 天 buffer；缺失时**配置阶段**显示警告（不是运行后才失败）
- pass：spans 数据与最新 trading calendar 差距 ≤ 1 工作日；缺失时 readiness API 立即报告

### L1-C5：P0-E 4 个 LEGACY_NON_ST_PIT 包准入审计
- **修复前 reproduce**：当前 4 个可选策略包全部 LEGACY_NON_ST_PIT；UI `selection/page.tsx:367` 显示警告条但实际无 RUNNABLE 包
- **修复后 pass**：资产准入正向状态机重设计后，至少 1 个包标 RUNNABLE 状态可入选股；LEGACY 包默认 `broker_compatible="LocalSim_only"`（不自动获得 MiniQMTSim）
- pass：selection page 选项 ≥ 1 RUNNABLE 包；准入状态机不允许"自动晋级"

### L1-C6：P0-F live inference 冷启动稳定性
- **修复前 reproduce**：live inference 冷启动失败 30+ 次（per Codex 文档 P0-4）
- **修复后 pass**：100 次连续冷启动失败次数 ≤1；失败时显式 typed error（不静默 retry）
- pass：preflight check 提前暴露 30+ 次中典型失败原因；不允许 live_session.py 静默重试

### L1-C7：P0-G strict feature coverage
- **修复前 reproduce**：strict feature coverage 可能为 0（与 P0-D / P0-E 联动）
- **修复后 pass**：feature coverage > 95% 才允许进 selection；< 95% 时 readiness API 报告
- pass：覆盖率计算可重现；不达标 100% 阻断 selection；不允许"暂时跳过"

### L1-C8：P0-H 两种模拟盘形态二分（task #11 / #20 已设计 + 实施）
- **修复前 reproduce**：现有 paper_trading_v2 = 纯 LocalSim + TDX 行情；miniQMT 通道仅以独立 client 存在未接入
- **修复后 pass**：BrokerBackend 抽象 + LocalSim/MiniQMTSim 二分（Engine §3.6 + task #20）
- pass：`backend/services/paper_trading_v2/broker/{base,localsim}.py` 实施；现有 `test_localsim_backend.py` 20 case 全绿；MiniQMTSim 留 PR-005

### L1-C9：P1-A 日频策略路径
- **修复前 reproduce**：`qe_source_resolver.py:526-540` 仅允许 `1min`/`5min`
- **修复后 pass**：QE 实验合约层放开 freq=`1d`；paper v2 运行 `1d` 路径不报错
- pass：1d 选股 + 1d portfolio 跑通；不允许"配置允许但运行报错"

### L1-C10：P1-B 尾盘处理策略
- **修复前 reproduce**：全库无 `pre_close` / `tail_period` / `close_handling` 字段
- **修复后 pass**：StrategyPackage 加 tail_handling 字段；分钟级执行算法支持尾盘特殊处理
- pass：含尾盘策略的 fixture 可跑通 paper v2

## L2 组件 / API / DB 流

L2 trigger：阻断点修复跨多组件协作。

### L2-F1：P0-A + P0-C 联动（配置 → selection 一致性）
- 用户在 paper v2 UI 改动 P0-A 6 项任一 → 调一键流程 → 验证 selection 实际使用了用户改动后的配置
- pass：跨 UI / API / DB 一致；不允许后端忽略 UI 提交

### L2-F2：P0-D + P0-G 联动（数据基础设施）
- spans 续期 + feature coverage > 95% 后，selection 路径不再因数据问题失败
- pass：100 次 selection 失败率因数据问题部分 ≤ 1%

### L2-F3：P0-F + P0-H 联动（live inference + broker）
- 启动 live_session（LocalSim / TDX_REALTIME）→ live inference 30 次冷启动 → 全部进 broker.submit
- pass：30 次冷启动 → broker 提交 → 失败 ≤ 1；任一失败抛 typed error 持久化

### L2-F4：P1-D / P1-E / P1-F UI 错误显示
- 触发各种典型 backend 错误（broker / selection / strategy_package / data）
- UI 显示中文向用户提示（per `broker_backend_switch_flow_20260509.md` §6.3 风格）
- pass：UI 不显示原始英文枚举 / Python traceback / JSON dump；error.context 字段完整

## L3 模块 UI/API 回归

L3 trigger：阻断点修复后整体 paper v2 UI/API 回归。

### L3-I1：阻断点修复后 paper v2 一日完整流程
- 创建 portfolio → selection 选股 → 一日 paper trading（LocalSim + TDX_REALTIME） → ledger 终态 → UI 持久化
- pass：流程不中断；P0-A ~ P0-H 各阻断点修复后路径不退化

### L3-I2：阻断点修复后 selection center 一日完整流程
- 选 RUNNABLE 包 → 配置（P0-A 6 项可改）→ 一键提交 → selection 结果 → 加 watchlist
- pass：UI 步数 ≤ 3 / 跨页 ≤ 1；selection 结果 traceable（package_id / manifest_sha256 / runtime_config_hash）

### L3-I3：错误路径 UI 显示完整性
- 触发 5 类典型错误（broker / selection / data / risk / execution_algo）
- UI 各自显示中文向用户提示
- pass：5 类全部按 `broker_backend_switch_flow_20260509.md` §6.3 模式渲染；不允许 fallback 到"操作失败"通用 toast

## Pass Criteria 汇总

| 等级 | 必须项 |
| --- | --- |
| L0 | 反模式 grep 0 命中；P0 / P1 各项静态守卫通过 |
| L1 | P0-A ~ P0-H + P1-A / P1-B 共 10 case 单独通过（部分依赖用户决策 §8.1 / §8.2 / §8.3 / §8.4） |
| L2 | 4 类联动场景跑通 |
| L3 | paper v2 + selection center 各自一日完整流程；错误路径 UI 显示完整 |

## 失败处理预期

- L0 失败 → 阻断阻断点修复 PR；先修反模式
- L1 失败 → 该阻断点未修复，**不允许标 P0 已解**
- L2 失败 → 多阻断点联动失败；定位到具体阻断点回归
- L3 失败 → 阻断点回归 = P0 退化；阻断 paper v2 release

## 与 Codex 模块的边界

| 不属于本模块（Codex 范围） | 落地位置 |
| --- | --- |
| QE 实验合约层（P1-A 日频路径需 QE 端放开） | `qe.md`（Codex 维护） |
| StrategyPackage v2 manifest schema 修订（P1-B 尾盘字段需 schema 加） | `strategy_package_v2.md`（Codex 维护） |
| ST PIT universe 数据更新（P0-D） | `qe_data_completeness.md`（已有；Codex 维护） |
| 资产准入正向状态机重设计（P0-E 部分） | `qe_archive.md`（已有；Codex 维护） |

本模块覆盖 paper v2 侧阻断点修复验证；**不**直接修 QE / strategy_package schema / 资产准入流程。

## 取材源

- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §0 / §7 / §8 决策清单
- `paper_v2_blockers_20260508.md`（worktree 工作面阻断点清单 P0-A ~ P0-H + P1-A ~ P1-G）
- `docs/architecture/strategy_engine_design_20260508.md` §3.6（P0-H 二分设计）
- `docs/architecture/broker_backend_switch_flow_20260509.md` §6.3（P1-D/E UI 显示规范）

## Deferred Scope

- §8.1 / §8.2 / §8.3 / §8.4 **用户决策待定** 的阻断点修复细节（P0-A 选项 / P0-B Engine 设计目标 / UI 简化方向 / 日频与尾盘优先级）：本矩阵仅列修复后的验证结构，具体 case 待用户决策后再补
- P1-C / P1-G UI 简化系列：在 `ui_simplification.md` 单独覆盖
- 实盘准入相关阻断点：本矩阵不覆盖（属于实盘准入门槛，主体 §11 流程）
