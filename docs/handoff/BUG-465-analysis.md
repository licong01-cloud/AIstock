# BUG-465 LocalSim InMemoryLedger 分析（lane L4）

日期：2026-06-22
工作树：F:\Dev\AIstock_worktrees\BUG-465-p1-paper-v2-localsim-inmemoryledger-decimal-mone-20260622
GitHub Issue：#1430

## 结论

Issue #1430 与当前代码一致：backend/services/trading_core/ledger.py 的 LocalSim 权威账本仍以 float 维护现金、成交额和手续费，买入资金校验使用 + 1e-8 epsilon；FeeModel.calculate() 对每个 Fill 独立套用最低 5 元佣金；InMemoryLedger.apply_fill() 本身没有按持仓语义复核 100 股 board-lot 和“仅整仓卖允许零股/奇数股”的约束。

## 根因

1. InMemoryLedger.__init__ 将 initial_cash 转为 float，后续 _apply_buy/_apply_sell/account_snapshot 都在 float 上累加，CashLedgerEntry.notional/fee/cash_delta/cash_after 也记录 float。大额账户、多笔 minute fill 后会把浮点误差写入权威 cash_after。
2. _apply_buy 使用 if total_cost > self.cash + 1e-8，会掩盖真实的微小透支；资金边界应由 Decimal 量化后的精确金额直接比较，不应引入 epsilon。
3. FeeModel.calculate(fill) 用单笔 fill 的 notional 做 max(rate_fee, min_cost)。同一 order 分多根分钟线成交时，每个 fill 都会再次收取最低佣金，导致最多 N x 5 元的过度收费。
4. Fill 模型已有基础 board-lot 校验，但 apply_fill 是账本最终写入点，当前没有结合持仓判断“卖出零股/奇数股必须为整仓卖”。因此通过构造、聚合或未来数据入口绕过模型校验时，账本会接受非整百买入或非整仓奇数卖出。

## 修复方案

- 在 ledger.py 内部引入 Decimal 金额路径：价格、notional、fee、cash_delta、cash_after 均显式量化到分；账本内部现金以 Decimal 保存，保留兼容性 cash 访问器给现有 LocalSim 查询和持久化路径读取。
- 增加订单级手续费累计状态：按 order_id 累计 notional 和已收 fee；每个 fill 的增量手续费为 max(cumulative_notional * rate, min_cost) - already_charged，并量化到分。
- 在 apply_fill 入口先做 board-lot 账本级复核：买入必须满足最小 100 股和 100 股递增；卖出若不是整仓清仓，也必须满足 100 股递增；整仓卖出允许奇数/零股残余一次性清掉。
- 新增 loud fail 上下文：所有新增/触达的账本拒绝路径提供 reason_code、operation、portfolio_id、order_id/fill_id 等上下文，并记录日志；不做 silent fallback。

## 与 issue 的分歧

无业务分歧。实现上为了不触碰 MiniQMT 和 scope 外调用方，FeeModel.calculate() 保持兼容返回 float；LocalSim InMemoryLedger 使用新增 Decimal 计算方法作为权威路径。MiniQMT 专属目录和 day_runner.py / scheduler.py / live_session.py 的 MiniQMT 专属分支不改动。
