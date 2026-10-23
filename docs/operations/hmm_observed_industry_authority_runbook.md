# BUG-2081：已观测分类 authority 离线构建与交付边界

## 源码运行角色

调用链为 `scripts/hmm_risk/build_observed_industry_authority.py` → `backend/services/hmm_risk/observed_industry_authority.py` → 既有 industry_pit 构建器/adapter。当前只有该 CLI 和直接测试导入新增模块，API、worker、scheduler 不导入它。CLI 的数据库读取使用既有只读入口，生成标准 C-013 bundle、authority 和重新绑定 receipt 的请求；产物是数据，不是驻留后端加载的 Python 模块。

runtime catalog 的 `offline-hmm-observed-industry-authority` 仅覆盖该精确服务文件；不覆盖同目录其他模块、未来 consumer 或路由。脚本沿用既有离线 operator 规则。混入后端文件时仍按 backend-main 检查，显式 `runtime_impact=none` 不能降级推断。

因此本源码包不需要 backend-main 的 HTTP identity、business-smoke 或用户重启收据。不得填写无关 health/HTTP 200 充当业务证明。将来新增 API/worker 消费该 Python 模块时，必须重新审核 catalog 归属并走真实运行态合同。本 runbook 不授权服务控制或产物激活。

## 独立证据

1. 源码交付：干净 commit、直接业务测试、CLI 新进程加载、变更文件检查，以及 canonical finish/pre_pr_gate 和 PR CI。
2. 构建身份：`F:/Dev/AIstock_runtime/hmm_l2_live_authority/BUG-2081/build-02/build_receipt.json` 绑定源码 commit/tree、authority 文件/规范哈希、官方观察时间和标准 C-013 身份；可读取并核对，禁止覆盖既有输出。
3. 目标日来源覆盖：该 receipt 报告 2026-10-22 分母 4,918、解析 4,918。观察时间为 2026-10-12 15:04:50 UTC，因果可用日为 2026-10-13，首次发布时间仍未核验。不得将目标覆盖解释成完整历史输入可用。
4. 完整数据库预检：`F:/Dev/AIstock_runtime/hmm_l2_live_authority/BUG-2081/database-preflight-01/summary.json` 为 BLOCKED，原始原因是 `stock-fact calendar has an internal gap`；配套 `calendar_gap_diagnosis.json` 表明 10 月 8、9、12 日有市场行，但分类早于新观察的因果边界而不可用。不得补默认分类、延长旧 eligibility 或回填已知时间来通过。
5. 激活：现有构建与预检均声明无数据库/数据集写入、fit 或 active-profile/runtime 激活。源码 PR 合入不替代业务预检，也不授予绑定新 authority 或发布推断结果的权限。

## 继续工作

本轮只修源码交付归属。保留 build-02 与预检失败收据的原始内容和身份，不重建 authority、不重跑约 495 秒的数据库预检。后续业务 owner 应定位所需历史上下文的合法分类证据和 typed-NA 处理边界，独立解决数据库预检；不得把本次流水线通过报告成 forward 准备或激活通过。

只读 CLI 加载检查不连接数据库或生成输出：

```powershell
python scripts/hmm_risk/build_observed_industry_authority.py --help
```

恢复本任务时，在任务工作树执行 canonical CLI：

```powershell
python F:/Dev/AIstock/scripts/aistock_issue_workflow.py resume --bug-id BUG-2081 --worktree F:/Dev/AIstock_worktrees/BUG-2081-p1-hmm-forward-classification-authority-expires-20261023
```
