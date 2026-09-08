---
name: develop-factor
description: AIstock 因子研发与正式交付的兼容入口，复用现有因子库验证、保存和官方评价能力，采用统一研究方法及数据库历史。
---

# 因子开发与交付

保留原名称，不保留第二套研发政策。先读取当前仓库 `docs/analysis/factor_research_methodology.md`；研究历史与尝试通过 `scripts/factor_research.py` 查询/记录，候选执行不先入正式 catalog。

用户要求正式入库/更新时，读取 `docs/operations/factor_research_delivery.md`，复用现有工具并核对具体因子、目标和授权。保存、官方指标、分类/评级、相关性分别报告，不以保存成功冒充全部完成，不覆盖未批准的同名因子，不自动淘汰。

不照抄旧 profile 中的固定数据路径、过期路由、直接 DB 备用 writer、永久方向黑名单或固定 IC 线。遇到共享业务能力缺口交 owner，本入口不接管 QE/数仓或个人 profile 安装。
