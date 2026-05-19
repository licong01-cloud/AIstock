# AIstock GitHub 私有仓库影响分析

**文档版本**: v1.1
**创建日期**: 2026-05-19
**最近更新**: 2026-05-19
**作者**: Claude (Opus 4.7)，Codex 审核修订
**状态**: 已完成技术影响复核，等待是否执行私有化切换的人工决策

---

## 0. 本次修订说明

本版本在原 v1.0 基础上补充 Codex 对当前仓库、工作流、MCP、Validation Center UI、`gh` 认证和本地 Git remote 的只读核对结论。重点修正如下：

1. 私有化后真正变化的不是代码结构，而是所有 GitHub 远端读取和写入都必须处于已认证状态。
2. `nightly.yml` 使用 self-hosted runner，基本不消耗 GitHub-hosted Actions 分钟；但 `test.yml` 和 Issue/PR 自动化 workflow 仍使用 GitHub-hosted runner，私有仓库下会计入 Actions 分钟。
3. MCP live GitHub 同步除了 token，还必须有 `GITHUB_REPOSITORY=licong01-cloud/AIstock`。
4. Validation Center UI 中的 `No synced GitHub issue link yet` 通常表示本地 BUG JSON 缺少 `github_issue_number` 或 `github_issue_url`，不是仓库公开或私有导致。
5. 私有化验证不建议直接向 `main` 推空提交；推荐用临时验证分支和 PR 跑真实合入门禁。

---

## 1. 执行摘要

将 AIstock GitHub 仓库从公开模式改为私有模式在技术上可行，对本地开发、分支开发、PR、Issue、Validation Center 流水线页面和 MCP 闭环没有架构性阻断。主要影响集中在认证、服务环境变量、外部可见性和 GitHub-hosted Actions 分钟消耗。

### 1.1 核心结论

| 维度 | 结论 | 说明 |
| --- | --- | --- |
| 是否可以改为私有 | 可以 | 当前 Git remote 使用 SSH，`gh` 已登录且 token scope 包含 `repo`。 |
| 本地开发流程 | 低影响 | 本地 worktree、分支、commit、diff、测试均不依赖仓库公开可见性。 |
| GitHub Issue / PR | 低到中影响 | 已认证账号可继续使用；匿名外部访问会不可见。 |
| GitHub Actions | 中等配置影响 | GitHub-hosted workflow 会消耗私有仓库 Actions 分钟；self-hosted nightly 影响较小。 |
| MCP / 后端 GitHub 集成 | 中等配置影响 | 必须保证服务运行环境能读取 `GITHUB_REPOSITORY` 和 token。 |
| Validation Center UI | 低影响 | 本地数据展示正常；远端 GitHub 状态需要认证，失败时必须显示 unavailable。 |
| 生产服务 | 无直接代码影响 | 生产后端如需实时读写 GitHub，必须重启时继承正确 env。 |
| 外部协作 | 高可见性影响 | 外部人员、未登录浏览器、匿名链接无法访问 repo、Issue、PR。 |

### 1.2 当前已核对状态

截至本次修订时，只读核对结果如下：

| 项目 | 当前状态 | 私有化含义 |
| --- | --- | --- |
| GitHub 仓库 | `licong01-cloud/AIstock` 当前为 `PUBLIC` | 需要人工在 GitHub Settings 中切换为 private。 |
| 当前权限 | 当前账号对仓库为 `ADMIN` | 具备修改可见性和配置 Actions/Secrets 的权限。 |
| 本地 Git remote | `git@github.com:licong01-cloud/AIstock.git` | SSH remote，私有化后本机 fetch/push 预计无影响。 |
| `gh auth status` | 已登录 `licong01-cloud`，token scopes 含 `repo` | 满足私有仓库 Issue/PR/API 基础访问。 |
| 当前 shell env | `GH_TOKEN`、`GITHUB_TOKEN`、`GITHUB_REPOSITORY` 未设置 | 交互式 `gh` 可用不等于后台服务 env 已配置。 |
| `main` branch protection | 当前未检测到 branch protection | 私有化不会自动补合入保护，需要单独配置。 |
| GitHub Pages | 当前未检测到 Pages site | 私有化不会影响现有 Pages，因为当前没有启用。 |

---

## 2. GitHub Actions 和流水线影响

### 2.1 当前 workflow 清单

| Workflow | 触发条件 | Runner | 私有化影响 | 建议 |
| --- | --- | --- | --- | --- |
| `test.yml` | PR、push to `main` | `windows-latest`、`ubuntu-latest` | 会消耗私有仓库 GitHub-hosted Actions 分钟 | 保持现状，切换后用临时 PR 验证。 |
| `nightly.yml` | 每日 03:07 CST 左右 | `[self-hosted, windows]` | 不消耗 GitHub-hosted 分钟，但 checkout 仍需 runner 对私有仓库有权限 | 切换后确认 self-hosted runner 在线并能 checkout。 |
| `issue-auto-link.yml` | PR 事件 | `ubuntu-latest` | 会消耗少量 Actions 分钟，需要 Issue/PR 权限 | 保持现有 `permissions`，切换后验证自动评论/链接。 |
| `issue-on-test-fail.yml` | workflow 失败、手动触发 | `ubuntu-latest` | 会消耗少量 Actions 分钟，需要 `issues: write` | 用受控失败或 dry-run 验证写 Issue 能力。 |
| `issue-on-guardrail-fail.yml` | workflow 失败、手动触发 | `ubuntu-latest` | 会消耗少量 Actions 分钟，需要 `issues: write` | 验证 guardrail 结果解析和 Issue 创建链路。 |

### 2.2 Actions 分钟和成本判断

原文档估算当前 GitHub-hosted workflow 月消耗约 80 到 100 分钟量级，低于 GitHub Free 常见私有仓库 Actions 免费额度。该估算可作为当前判断依据，但不应替代 GitHub Billing 页面中的实时用量。

需要注意：

- 公开仓库下 GitHub-hosted runner 的免费策略与私有仓库不同，切换 private 后应观察 Billing 页面。
- Self-hosted runner 自身不消耗 GitHub-hosted 分钟，但仍需要 GitHub Actions 服务调度和仓库访问权限。
- 如果未来 PR 数量、矩阵测试数量或 Playwright/UI 测试大幅增加，Actions 分钟可能从低风险变为需要监控的成本项。

### 2.3 Workflow 权限

当前 workflows 已显式声明必要权限，例如：

- `issue-auto-link.yml`: `issues: write`、`pull-requests: read`。
- `issue-on-test-fail.yml`: `issues: write`。
- `issue-on-guardrail-fail.yml`: `issues: write`。
- `test.yml`: PR 评论和失败提示使用 `pull-requests: write`、`issues: write`。

私有化后应在 GitHub Settings 中确认：

1. Actions 已启用。
2. Workflow permissions 不低于当前 workflow 声明所需权限。
3. Self-hosted runner 仍处于在线状态。
4. 如果组织或仓库启用了更严格的 Actions policy，需要允许当前 workflow 和 actions 使用。

---

## 3. 本地开发、分支和 PR 流程影响

### 3.1 本地开发

本地开发本身不依赖仓库公开可见性，因此以下流程无实质影响：

- `git status`、`git diff`、`git commit`。
- 本地测试、nox、pytest、前端类型检查和 UI smoke。
- 本地 worktree 和 issue 专属分支开发。
- Agent team 并行开发中的文件范围控制和提交隔离。

### 3.2 Git fetch / push

当前本地 remote 使用 SSH：

```text
origin git@github.com:licong01-cloud/AIstock.git
```

因此只要当前 GitHub 账号的 SSH key 可用，私有化后 fetch/push 预计不受影响。需要额外检查的是其他机器或远端节点是否仍使用 HTTPS 匿名访问。

### 3.3 PR 创建和合入

PR 本身支持私有仓库，影响主要是认证：

- `gh pr create`、`gh pr view`、`gh pr merge` 需要已登录 `gh`，且 token 具备 `repo` 权限。
- 浏览器查看 PR 需要登录有仓库权限的 GitHub 账号。
- 外部未授权人员无法查看 PR 链接。

建议继续保持当前人工确认边界：

- Codex 可以创建 PR、跑验证、汇报合入条件。
- 合并 `main`、关闭 P0/P1 Issue、生产同步、重启生产服务仍需人工确认。

---

## 4. GitHub Issue、BUG JSON 和 MCP 同步影响

### 4.1 事实源关系保持不变

AIstock 当前规则是：

- 本地 `tests/aistock_validation/bugs/*.json` 是 BUG 生命周期、写入范围、验证要求和关闭条件的事实源。
- GitHub Issues 是 workflow/UI mirror，用于 PR 链接、移动端通知、协作看板和审计展示。
- Validation Center 是本地索引、证据展示和操作入口。

仓库私有化不会改变这个事实源关系。

### 4.2 MCP live GitHub 调用的真实要求

`F:\Dev\AIstock\scripts\aistock_mcp_server.py` 中的 GitHub Issue client 当前逻辑为：

- `GITHUB_REPOSITORY` 必须存在，格式为 `owner/name`。
- Token 优先读取 `GH_TOKEN`，其次 `GITHUB_TOKEN`，再尝试 `gh auth token`。
- Live GitHub 读取或写入是显式 opt-in，例如 `source=github` / `source=both` 或 `create_github=True`。

因此私有化前后，MCP 要稳定工作，推荐固定配置：

```powershell
GITHUB_REPOSITORY=licong01-cloud/AIstock
GH_TOKEN=<具备 repo 权限的 GitHub token>
```

如果不想在 env 文件保存 token，也可以依赖 `gh auth token`，但这只适合交互式同用户进程；对于后台服务、MCP server、Windows service、计划任务和 runner，不建议只依赖 keyring fallback。

### 4.3 `bug_github_sync.py` 的要求

`F:\Dev\AIstock\scripts\bug_github_sync.py` 支持离线 dry-run。涉及 live GitHub 时要求：

- `--repo owner/name` 或 `GITHUB_REPOSITORY`。
- `--token`、`GITHUB_TOKEN`、`GH_TOKEN` 或可用 `gh auth token`。
- `--apply` 才会真实写入 GitHub 或回填 BUG JSON。

私有化后 dry-run 不受影响；真实同步必须保证 token 具备私有仓库访问权限。

### 4.4 `No synced GitHub issue link yet` 的含义

Validation Center UI 中出现 `No synced GitHub issue link yet`，通常表示本地 BUG JSON 中没有：

- `github_issue_number`
- `github_issue_url`

这不是仓库公开或私有导致的问题。私有化后可能新增的现象是：

- 有链接，但未登录浏览器无法打开，表现为 GitHub 404 或登录页。
- 后端无法读取远端 Issue 状态时，应返回 `data_state=unavailable` 或对应 reason code，而不是显示为绿色成功。

---

## 5. Validation Center UI 和流水线板块影响

| 页面或板块 | 数据来源 | 私有化影响 | 正确降级策略 |
| --- | --- | --- | --- |
| 模块质量 | 本地模块目录、测试覆盖率、BUG JSON | 基本无影响 | GitHub 不可用时仍展示本地模块质量。 |
| 流水线测试情况 | 本地 validation run、CI 结果、测试计划 | 本地数据无影响，远端 Actions 状态需要认证 | 远端不可用显示 unknown/unavailable。 |
| 功能测试验证 | 本地 route/test catalog | 无直接影响 | 不应因为 GitHub 不可用变红，除非当前合入依赖远端证据。 |
| GitHub 议题 | BUG JSON + GitHub Issue mirror | 需要认证读取远端状态 | 区分 `linked`、`missing_link`、`workflow_mismatch`、`unavailable`。 |
| 分支和 PR | git、gh CLI、GitHub API | 远端 PR 读取需要认证 | gh/API 不可用时不返回空成功。 |
| 未合入更改 | 本地 git status/diff | 无影响 | 继续以本地 git 为准。 |
| 历史遗留问题 | 本地扫描基线、BUG JSON | 无影响 | GitHub 链接仅作为附加跳转。 |
| MCP 自动化 | 本地 MCP 能力 + GitHub token/env | GitHub 写操作需要认证 | L3 以上继续 dry-run first，人工确认后执行。 |

---

## 6. 远端节点、自动化任务和生产环境影响

### 6.1 远端节点

如果 `rdagent-node` 或其他远端节点需要拉取 AIstock 仓库：

- 使用 SSH remote：预计无影响。
- 使用 HTTPS 匿名 remote：私有化后会失败。
- 使用 HTTPS + PAT：需要确认 PAT 对私有仓库有权限。

建议在远端节点执行：

```bash
git remote -v
git fetch origin main
```

如发现 HTTPS 匿名访问，应先改为 SSH 或配置凭据管理器/PAT。

### 6.2 生产后端和前端

仓库可见性不会直接改变生产服务代码行为，但会影响服务访问 GitHub API 的能力。生产或准生产环境如需展示 GitHub Issue、PR、分支、同步状态，应配置：

```text
GITHUB_REPOSITORY=licong01-cloud/AIstock
GH_TOKEN=<repo scope token>
NEXT_PUBLIC_GITHUB_REPOSITORY=licong01-cloud/AIstock
```

注意：

- 不建议把 token 写入前端公开变量。
- `NEXT_PUBLIC_GITHUB_REPOSITORY` 只用于生成公开形态的链接，不应包含密钥。
- Token 只应出现在后端、MCP、runner 或受控脚本环境中。

### 6.3 生产安全边界

私有化切换本身不需要：

- 重启生产后端 `8001`。
- 重启生产前端 `3000`。
- 写生产数据库。
- 修改 Paper live、QMT 或 miniQMT 配置。

如果后续要让生产服务读取新的 env，则重启服务属于单独生产操作，需要人工确认。

---

## 7. 外部可见性和协作影响

私有化后，以下内容会保留但不再对外部匿名用户可见：

- 代码仓库。
- Issues。
- Pull Requests。
- Actions run 页面。
- Wiki、Projects、Discussions 等仓库功能。
- 文档中的 GitHub Issue/PR 链接。

对当前“单人主开发、本机为唯一开发环境”的场景，协作风险较低；但如果未来需要外部审计、第三方协作或远端 agent 访问，需要显式邀请协作者或配置机器凭据。

---

## 8. 合入门禁和开发效率设计

私有化后不应把“GitHub 暂时不可用”无差别当成所有合入的 hard block。建议按影响范围区分：

| 场景 | 门禁建议 | 理由 |
| --- | --- | --- |
| 当前 PR 修复 P0/P1，且需要同步 GitHub Issue | `blocked` 或 `need_confirm` | 当前修复闭环依赖 GitHub 状态。 |
| 当前 PR 需要创建/更新 PR、关闭 Issue | `need_confirm` | 涉及远端写操作，必须认证并确认。 |
| 当前分支只改文档，且 GitHub 远端临时不可读 | `warning` | 本地 git 和 diff 证据足够，不应拖慢低风险变更。 |
| 历史 BUG 缺 GitHub 链接，但与当前变更无关 | `warning` | 属于背景债务，不应阻塞无关合入。 |
| gh CLI/API 返回不可用但本地 BUG JSON 完整 | `warning` 或 `unknown` | 不能显示绿色成功，也不应伪造远端状态。 |
| 当前分支新增 P0/P1 finding | `blocked` | 与私有化无关，属于质量硬门禁。 |

---

## 9. Worktree 与分支隔离规范

为避免多个 Codex、Claude Code 或人工窗口同时操作同一物理目录，后续新功能和 bugfix 默认采用“一个任务一个 worktree + 一个分支”的开发方式。

### 9.1 推荐规则

| 任务类型 | 推荐做法 | 原因 |
| --- | --- | --- |
| 新功能开发 | 从最新 `origin/main` 创建新 worktree 和 feature 分支 | 避免切换主目录分支影响其他窗口未提交修改。 |
| P0/P1 bugfix | 从最新 `origin/main` 创建 issue 专属 worktree 和 bug 分支 | 方便按 `allowed_write_scope` 控制修复范围。 |
| 文档-only 小改 | 如主目录干净可直接做；否则也用临时 main worktree | 避免误提交到其他窗口当前分支。 |
| PR 合入验证 | 使用独立 merge/verify worktree | 不污染开发分支，便于复现合入结果。 |
| 生产同步 | 使用明确的生产同步 worktree 或受控流程 | 防止本地开发文件混入生产同步。 |

### 9.2 命名建议

```powershell
git fetch origin
$branch = "feature/<short-task>-YYYYMMDD"
$worktree = "F:\Dev\AIstock_worktrees\<short-task>-YYYYMMDD"
git worktree add -b $branch $worktree origin/main
```

Bugfix 建议：

```powershell
git fetch origin
$branch = "bug/BUG-XXX-<short-slug>"
$worktree = "F:\Dev\AIstock_worktrees\bug-XXX-<short-slug>"
git worktree add -b $branch $worktree origin/main
```

文档直接提交 `main` 时，如果主目录正在被其他窗口使用，建议创建临时 main worktree：

```powershell
git worktree add F:\Dev\AIstock_worktrees\private-repo-doc-main-YYYYMMDD main
```

### 9.3 操作边界

1. 不在别人正在使用的 worktree 中切分支。
2. 不在有未提交修改的目录里执行无关任务提交。
3. 每个任务只提交本任务相关文件。
4. 合并 `main` 前确认分支、HEAD、diff、测试证据和 GitHub Issue/PR 状态。
5. 不用 `git reset --hard`、`git clean -fd` 清理非自己创建的文件，除非明确确认。

---

## 10. 切换前检查清单

执行私有化前建议完成以下检查：

### 10.1 本地和 GitHub 权限

```powershell
gh repo view licong01-cloud/AIstock --json nameWithOwner,visibility,isPrivate,viewerPermission
gh auth status
git remote -v
git fetch origin main
git status --short --branch
```

要求：

- `gh` 登录账号为 `licong01-cloud` 或具备仓库 Admin/Write 权限的账号。
- Token scope 包含 `repo`。
- Remote 使用 SSH，或 HTTPS 已配置 PAT。
- 工作区中无即将被私有化操作误处理的未提交文件。

### 10.2 备份

建议做一次 mirror 备份：

```bash
git clone --mirror git@github.com:licong01-cloud/AIstock.git AIstock.git.mirror
```

### 10.3 服务环境变量

至少确认以下环境在后端/MCP/runner 所属用户下可用：

```text
GITHUB_REPOSITORY=licong01-cloud/AIstock
GH_TOKEN=<repo scope token>
```

前端只需要公开仓库名或 Issues URL：

```text
NEXT_PUBLIC_GITHUB_REPOSITORY=licong01-cloud/AIstock
```

### 10.4 远端节点

在 `rdagent-node` 或其他远端节点检查：

```bash
git remote -v
git fetch origin main
```

如失败，应先修复 SSH/PAT，再切换私有化。

---

## 11. 私有化操作建议

GitHub 页面路径：

```text
Settings -> General -> Danger Zone -> Change repository visibility -> Make private
```

操作原则：

1. 不在脏工作区做与私有化无关的清理。
2. 不通过直接 push 空提交到 `main` 作为首个验证动作。
3. 切换后先做只读访问验证，再做临时 PR 验证。
4. 真实 GitHub Issue 写入继续遵守 dry-run first。
5. 生产服务重启和生产 DB 写入不属于私有化切换步骤。

---

## 12. 切换后验证清单

### 12.1 GitHub 和 Git 基础验证

```powershell
gh repo view licong01-cloud/AIstock --json nameWithOwner,visibility,isPrivate,viewerPermission
gh issue list --limit 5
gh pr list --limit 5
git fetch origin main
git ls-remote --heads origin main
```

### 12.2 临时 PR 验证

推荐创建临时分支和 PR：

```powershell
git switch -c chore/private-repo-access-smoke-YYYYMMDD
git commit --allow-empty -m "test: verify private repo access smoke"
git push -u origin chore/private-repo-access-smoke-YYYYMMDD
gh pr create --fill --base main --head chore/private-repo-access-smoke-YYYYMMDD
```

验证内容：

- `test.yml` 能 checkout 私有仓库并运行。
- PR 页面可读取 checks。
- PR 自动链接或评论 workflow 正常。
- 验证完成后关闭 PR 并删除临时分支，除非用户决定合入该空提交。

### 12.3 MCP 和 Issue 同步验证

先 dry-run：

```powershell
python scripts/bug_github_sync.py --repo licong01-cloud/AIstock --json
```

再选择一个低风险 BUG 做受控同步：

```powershell
python scripts/bug_github_sync.py --repo licong01-cloud/AIstock --bug-id BUG-XXX --json
```

如果要 `--apply`，必须先确认：

- Token 已配置。
- 操作目标和 diff 预览正确。
- 不会误关闭 P0/P1 Issue。

### 12.4 Validation Center 验证

在非生产端口启动后端后验证：

```powershell
curl http://127.0.0.1:<dev-port>/api/v1/validation/github/issues/summary
curl http://127.0.0.1:<dev-port>/api/v1/validation/github/issues
curl http://127.0.0.1:<dev-port>/api/v1/validation/github/prs/summary
curl http://127.0.0.1:<dev-port>/api/v1/validation/git/branches/detail-summary
```

验收要求：

- GitHub 可用时，能正常展示 Issue/PR/分支状态。
- GitHub 不可用时，返回 `data_state=unavailable` 或明确 reason code。
- 不应把 GitHub API 失败显示为绿色成功。
- `No synced GitHub issue link yet` 仍应只表示本地 BUG 未同步链接。

### 12.5 Nightly 验证

等待下一次 nightly，或在 GitHub Actions 手动触发受控 workflow，确认：

- Self-hosted runner 在线。
- 私有仓库 checkout 成功。
- L3/DR/Paper v2 相关任务按原计划运行。
- 失败自动建 Issue 的 workflow 仍具备 `issues: write`。

---

## 13. 风险评估

| 风险 | 严重性 | 可能性 | 缓解措施 |
| --- | --- | --- | --- |
| 后端/MCP 缺 `GITHUB_REPOSITORY` | 中 | 中 | 将 repo env 写入服务启动配置，并增加启动自检。 |
| 后台服务无法读取 `gh` keyring token | 中 | 中 | 不依赖 keyring fallback，给服务显式配置 `GH_TOKEN`。 |
| 远端节点仍使用匿名 HTTPS | 中 | 中 | 切换前检查 remote，改为 SSH 或 PAT。 |
| GitHub-hosted Actions 分钟超额 | 低到中 | 低 | 切换后观察 Billing；必要时减少矩阵或迁移更多任务到 self-hosted。 |
| UI 把 GitHub API 失败显示为成功 | 中 | 低 | 继续遵守 `data_state=unavailable` 规则。 |
| 外部链接不可访问 | 低 | 高 | 这是私有化目标；如需审计则临时邀请协作者。 |
| 未经确认重启生产服务 | 高 | 低 | 私有化切换不包含生产重启，需单独确认。 |

---

## 14. 是否推荐私有化

推荐，但建议按“配置先行、切换后验证”的方式执行。

推荐前提：

1. 本机 `gh` 和 SSH 已确认可用。
2. 后端/MCP/runner 的 `GITHUB_REPOSITORY` 与 token 配置已补齐。
3. 远端节点不再依赖匿名 HTTPS。
4. 接受外部匿名用户无法查看代码、Issue、PR 和 Actions。
5. 切换后执行临时 PR、MCP dry-run、Validation Center 只读接口和 nightly 验证。

不建议的做法：

- 直接切换后立即认为所有自动化都正常。
- 用生产 `8001` 或 `3000` 作为唯一验证路径。
- 把 token 写进前端 `NEXT_PUBLIC_*` 变量、Markdown 文档、Issue 正文或截图。
- 为了验证直接向 `main` 推空提交。

---

## 15. 后续待办

| 优先级 | 待办 | 说明 |
| --- | --- | --- |
| P0 | 为后端/MCP 服务启动环境补齐 `GITHUB_REPOSITORY` | 否则 live GitHub sync 可能依赖交互式 fallback，不够稳定。 |
| P0 | 明确 token 保存位置和轮换方式 | 只在后端/服务/runner 环境保存，不进入前端和文档。 |
| P1 | 私有化后创建临时 PR 验证 CI/PR/Issue 自动化 | 不建议直接 push 空提交到 `main`。 |
| P1 | 验证 Validation Center GitHub 页面 private 模式 | 重点看 linked/missing/unavailable 状态。 |
| P1 | 检查远端节点 remote 和 runner checkout | 避免 rdagent-node 或 self-hosted runner 私有化后无法拉取。 |
| P2 | 根据需要配置 branch protection | 当前未检测到 main branch protection，私有化不会自动启用。 |

---

## 16. 相关文档

- `F:\Dev\AIstock\docs\architecture\github_issues_integration_design_20260512.md`
- `F:\Dev\AIstock\docs\architecture\validation_center_phase1_backend_design_20260515.md`
- `F:\Dev\AIstock\docs\standards\aistock_issue_fix_parallel_workflow_standard_20260514.md`
- `F:\Dev\AIstock\scripts\aistock_mcp_server.py`
- `F:\Dev\AIstock\scripts\bug_github_sync.py`
- `.github/workflows/test.yml`
- `.github/workflows/nightly.yml`
- `.github/workflows/issue-auto-link.yml`
- `.github/workflows/issue-on-test-fail.yml`
- `.github/workflows/issue-on-guardrail-fail.yml`

---

## 17. 最终结论

AIstock 可以切换为 GitHub 私有仓库。对当前单人开发、本机主开发环境和现有 GitHub Issues/MCP/Validation Center 架构而言，风险总体较低。最关键的落地要求是：不要只验证交互式 PowerShell 中的 `gh` 可用，还要验证后端、MCP、runner、远端节点这些非交互式执行环境都具备私有仓库访问能力。

切换后建议以临时 PR + MCP dry-run + Validation Center 只读接口 + nightly 观察作为合格验证组合。通过这些验证后，可以认为私有化对现有开发流程和流水线板块影响可控。
