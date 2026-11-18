# LocalSIM 纠错账本导入一致性 F2 设计

## 1. 背景

LightGBM 的离线纠错账本已覆盖 2026-01-05 至 2026-11-18。前 202 日复用原账本收盘事实，后 9 日来自封存的原生 TWAP 纠错回放。当前生产切换检查仍发现未终结经济运行。本阶段完成原生账本格式的独立导入和 DEV 验证，并出具切换预检依据。

## 2. 范围与写入清单

- `scripts/verify_localsim_corrected_ledger.py`
- `backend/tests/scripts/test_verify_localsim_corrected_ledger.py`
- `docs/architecture/localsim_corrected_ledger_consistency_f2_20261119.md`
- `noxfile.py` 与 `tests/aistock_validation/catalog/file_ownership.yaml`，将验证工具及其定向测试接入现有 LocalSIM CI 计划

现有本地 DEV 的单事务验证，产物存入指定 X 盘任务目录。依赖既有 LocalSIM 账户、账本范围、Paper v2 经济表及工作台口径。本阶段的生产动作限于只读切换预检和原账本事实核对。实际生产切换保留既有安全边界与账户操作授权。

## 3. 非目标

本阶段的交付范围限于 DEV 导入验证及生产只读预检。生产挂接、切换、历史重跑与原账本变更属于独立操作。

## 4. 架构与接口契约

`verify_localsim_corrected_ledger.py` 接受最终核验回执及完整 SHA。读取既有日历、父历史、续跑结果和 exact retry 引用，核对 211 日交易日历、资金原点及前后缀边界。对前缀使用一个生产只读事务中的精确运行 ID 核对封存事实，保留来源字段及原状态。后缀读取原生纠错日文件的订单、成交、费用、持仓和原始收盘价格。建仓日期写入 Paper v2 读取的 `position_trade_date`，快照交易日留在外层日期字段。纠错段成交、现金和快照的投影时间来自封存的事件时间。纠错段执行状态通过原始日文件引用保留，逐订单事件覆盖前 202 日。

将真实事实映射到一个确定的新账本身份，写入暂停账户与独立原生账本范围。无运行绑定。域键按新账户身份重绑，经济量、顺序、来源保留。补充 DEV 中缺失的真实策略包元数据在同一事务回滚。生产无需导出全库或做备份。每批严格按预先固定的列写入并读回，任何同键异值或部分状态均作为冲突失败。

## 5. 实施方案

在 DEV 单个事务内创建暂停账户、账本范围和测试所需的策略包行。按外键顺序写入 211 日事实，并用相同范围重新读回经济行、现金序列及最后一日持仓。使用生产预检接口和只读查询确认正在进行的运行身份。

## 6. 验证方案、回滚与风险

验收覆盖封存文件变化、经济行和现金序列对应、同键异值拒绝、相同请求零新增。调用 Paper v2 验证下一交易日现金和持仓的数量、可用数量、平均成本及建仓日期，调用工作台计算验证净值和盈亏。事务统一回滚，用新连接读回验证经济行、暂停账户、账本范围及临时策略包全部清理。

## 7. 生产门禁

现有 `/simulation-runtime/localsim/cutover-readiness` 返回真实活动运行阻断。本次盘前核对显示 LightGBM 的 11 月 18 日运行仍等待权威估值。后续生产切换沿用既有安全窗口、经济运行和账户授权条件。11 月 19 日自然盘验收取当天运行回执。

## 8. Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-001 | 核对精确 FileRef、完整日历、原生前缀和纠错后缀 |
| F-002 | 核对 211 日现金、数量、费用、估值及源状态，保留后段执行状态引用 |
| F-003 | DEV 暂停账户独立账本、相同重试零新增、同键异值拒绝 |
| F-004 | 下一交易日原生现金和持仓读回、盈亏口径核对及回滚验证 |
| F-005 | 真实生产盘前切换预检，确认未终结运行及当天运行行数 |

## 9. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `prepare`、`collect_original` | `backend/tests/scripts/test_verify_localsim_corrected_ledger.py`、artifact: `dev-full-history-consistency-r3.json` | verified | 无 |
| F-002 | `verify_economics`、`compare_candidate_rows` | artifact: `dev-full-history-consistency-r3.json` | verified | 无 |
| F-003 | `materialize_dev`、`verify_conflict_refused` | artifact: `dev-full-history-consistency-r3.json` | verified | 无 |
| F-004 | `verify_dev` | artifact: `dev-full-history-consistency-r3.json` | verified | 无 |
| F-005 | `GET /simulation-runtime/localsim/cutover-readiness` | artifact: `cutover-readiness-20261119-preopen.json` | verified | 无 |

证据目录为 `X:/AIstock_temp/localsim-lightgbm-repair-20261118`。`dev-full-history-consistency-r3.json` 的 SHA-256 为 `c6b5f2ff81b33e934626d7c6b34de62c6ceaaad6481e158600e65e249fbc37dd`。`cutover-readiness-20261119-preopen.json` 的 SHA-256 为 `f0b59084303db417c4de664622c80dfad9b7a4c7fe0ed7023c200a17aae308c0`。
