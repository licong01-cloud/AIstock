---
name: factor-research
description: 在 AIstock 中开展或恢复因子研究、记录假设和尝试、运行候选评价及诊断存量因子；不接管 QE 实验或正式因子晋升。
---

# 因子研究

先读取当前仓库 `docs/analysis/factor_research_methodology.md`，它是唯一方法正文。通过 `python scripts/factor_research.py --help` 及所需子命令帮助操作，不复制旧 profile 的固定路径、IC 淘汰线或方向黑名单。

先 list/show/context 找到数据库历史与当前输入，再写研究卡，执行明确范围的候选，记录结论与下一步。计算已完成但登记失败时 attach，不重算；候选目录在仓库外，不写正式因子库或覆盖数据。

正式交付时再读取 `docs/operations/factor_research_delivery.md` 并确认具体动作授权。QE/数仓及其他模块缺口交专用窗口，不直接改业务源码或启动实验。研究结束不要求得到有效 alpha。
