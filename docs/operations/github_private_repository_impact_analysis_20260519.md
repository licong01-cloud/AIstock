# AIstock GitHub 私有仓库影响分析

**文档版本**: v1.0  
**创建日期**: 2026-05-19  
**作者**: Claude (Opus 4.7)  
**状态**: 待审核

---

## 执行摘要

本文档分析将 AIstock GitHub 仓库从公开模式改为私有模式的影响范围，涵盖 CI/CD 流水线、开发工具链、Codex 集成、以及团队协作流程。

**核心结论**：
- ✅ **技术上可行**：所有自动化流程均可在私有模式下正常运行
- ✅ **成本影响极小**：主要工作流使用 self-hosted runner，不消耗 GitHub Actions 配额
- ⚠️ **需要调整**：部分脚本和文档包含硬编码的公开 URL，需要更新访问权限
- 📋 **建议操作**：提供详细的迁移检查清单

---

## 1. GitHub Actions 自动化流水线分析

### 1.1 当前工作流清单

| 工作流 | 触发条件 | Runner 类型 | 配额消耗 | 私有化影响 |
|--------|---------|------------|---------|-----------|
| **test.yml** (AIstock CI) | PR + push to main | GitHub-hosted (ubuntu-latest, windows-latest) | ✅ 消耗配额 | 需要配额 |
| **nightly.yml** (Nightly L3 + DR) | 每天 03:07 CST | self-hosted (Windows) | ❌ 不消耗 | 无影响 |
| **issue-auto-link.yml** | PR 事件 | GitHub-hosted (ubuntu-latest) | ✅ 消耗配额 | 需要配额 |
| **issue-on-test-fail.yml** | CI/Nightly 失败时 | GitHub-hosted (ubuntu-latest) | ✅ 消耗配额 | 需要配额 |
| **issue-on-guardrail-fail.yml** | 手动触发 | GitHub-hosted (ubuntu-latest) | ✅ 消耗配额 | 需要配额 |

### 1.2 配额消耗估算（月度）

#### **当前公开仓库**（无限免费）
- test.yml: ~73 分钟/月
- issue-auto-link.yml: ~5 分钟/月
- issue-on-test-fail.yml: ~5 分钟/月
- nightly.yml: **0 分钟**（self-hosted）
- **总计**: ~83 分钟/月

#### **私有仓库配额需求**
| GitHub 计划 | 月度配额 | 是否足够 | 成本 |
|------------|---------|---------|------|
| Free | 2000 分钟 | ✅ 是（使用率 4%） | $0 |
| Pro | 3000 分钟 | ✅ 是（使用率 3%） | $4/月 |
| Team | 3000 分钟 | ✅ 是（使用率 3%） | $4/人/月 |

**结论**: 即使是 Free 账户的 2000 分钟配额也完全足够，使用率仅 4%。

---

## 2. 自动化流程功能详解

### 2.1 test.yml (AIstock CI)

**功能**：
- **Static Gate**: L0 守护栏 + 模块注册表验证
- **Backend Tests**: 7 个后端测试矩阵（paper_v2, qe_archive, model_registry, market_regime_label, rl_execution_smoke, validation_center, qe_data_contract）
- **Failure Bug Register**: CI 失败时自动在 PR 上评论

**私有化影响**：
- ✅ 功能完全正常
- ⚠️ 需要 GitHub Actions 配额（~73 分钟/月）
- ⚠️ 使用 `actions/checkout@v4` 需要 `GITHUB_TOKEN` 权限（自动提供）

**依赖的外部资源**：
- Docker Hub: `timescale/timescaledb:latest-pg14`（公开镜像，无影响）
- PyPI: 所有 Python 包（公开，无影响）

---

### 2.2 nightly.yml (Nightly L3 + DR)

**功能**：
1. **DR Snapshot**: 每天备份生产 PostgreSQL 数据库到 `E:/DEV backup/aistock_pg_snapshots/`
2. **DR Validate**: 验证快照有效性、schema diff、保留策略
3. **Nightly L3**: ���归测试（paper_v2_l3 + qe_archive_l3 + qe_read_l3）
4. **Paper v2 Live**: 实时追赶验证（历史回放 → 实时切换）
5. **Full Summary**: 汇总报告 + 失败时自动创建 GitHub Issue

**私有化影响**：
- ✅ **完全无影响**（使用 self-hosted runner，不消耗配额）
- ✅ 所有操作在本地 Windows 机器执行
- ✅ 失败时自动创建 Issue 的功能正常（使用 `GITHUB_TOKEN`）

**时区确认**：
- Cron: `7 19 * * *` = UTC 19:07 = **北京时间 03:07**
- 设计意图：A 股收盘后（15:00）+ 日志轮转后

---

### 2.3 issue-auto-link.yml

**功能**：
- 自动检测 PR 标题/描述中的 `Fixes #N` / `Closes #N` / `Resolves #N` 关键词
- 在 Issue 和 PR 上自动添加关联评论

**私有化影响**：
- ✅ 功能完全正常
- ⚠️ 需要 `issues: write` 和 `pull-requests: read` 权限（已配置）
- ⚠️ 消耗少量配额（~5 分钟/月）

---

### 2.4 issue-on-test-fail.yml

**功能**：
- 监听 `AIstock CI` 和 `AIstock Nightly L3 + DR` 的失败事件
- 自动创建 P0/P1 级别的 GitHub Issue
- 支持手动触发

**私有化影响**：
- ✅ 功能完全正常
- ⚠️ 需要 `issues: write` 权限（已配置）
- ⚠️ 消耗少量配额（~5 分钟/月）

---

### 2.5 issue-on-guardrail-fail.yml

**功能**：
- 手动触发或 CI/Nightly 失败时，从守护栏扫描结果创建 Issue
- 解析 `tmp/validation/guardrails/guardrail_scan.json`
- 自动提取 P0/P1 级别的发现

**私有化影响**：
- ✅ 功能完全正常
- ⚠️ 需要 `issues: write` 权限（已配置）
- ⚠️ 消耗少量配额（~5 分钟/月）

---

## 3. 脚本和工具链影响分析

### 3.1 GitHub API 依赖

**受影响的脚本**：

| 脚本 | 用途 | 私有化影响 | 解决方案 |
|------|------|-----------|---------|
| `scripts/aistock_mcp_server.py` | MCP 服务器（GitHub API 集成） | ⚠️ 需要 PAT | 使用 `GITHUB_TOKEN` 环境变量 |
| `scripts/bug_github_sync.py` | Bug 注册表与 GitHub Issues 同步 | ⚠️ 需要 PAT | 使用 `GITHUB_TOKEN` 环境变量 |

**Personal Access Token (PAT) 需求**：
- 私有仓库需要 PAT 具有 `repo` 权限（公开仓库只需 `public_repo`）
- 当前脚本已支持从环境变量读取 `GITHUB_TOKEN`
- **无需修改代码**，只需确保 PAT 权限足够

---

### 3.2 硬编码的 GitHub URL

**影响范围**：
- `tests/aistock_validation/bugs/*.json`: 253 处引用（Bug 注册表中的 `github_issue_url` 字段）
- `docs/operations/github_issues_live_validation_20260513.md`: 示例 URL
- `frontend/node_modules/`: 第三方库的 README（无影响）

**私有化影响**：
- ✅ **无功能影响**：这些 URL 仅用于文档和元数据，不影响代码执行
- ⚠️ **访问权限**：外部用户无法访问这些 Issue URL（符合私有化预期）

**建议**：
- 保持现状，无需修改
- 如果需要分享 Bug 详情，可导出 JSON 文件或使用截图

---

### 3.3 远端节点 (rdagent-node)

**节点信息**：
- IP: `192.168.50.215`
- 用途: RDAgent 多节点任务分发

**私有化影响**：
- ⚠️ 如果远端节点使用 HTTPS clone，需要更新 PAT
- ✅ 如果使用 SSH clone（`git@github.com:...`），无影响

**检查方法**：
```bash
# 在 rdagent-node 上执行
cd /path/to/AIstock
git remote -v
```

**解决方案**：
- **HTTPS**: 更新 `.git/config` 中的 URL 为 `https://<PAT>@github.com/licong01-cloud/AIstock.git`
- **SSH**: 无需操作（推荐）

---

## 4. Codex 和 Claude Code 集成影响

### 4.1 Codex 工作流

**当前集成点**：
- `.codex/skills/verify-aistock-feature/`: 验证技能（本地执行）
- `noxfile.py`: 调用 Codex 技能进行质量守护

**私有化影响**：
- ✅ **完全无影响**：Codex 技能在本地执行，不依赖 GitHub 仓库可见性

---

### 4.2 Claude Code 开发流程

**当前使用场景**：
1. 本地开发：读取/编辑文件
2. Git 操作：commit, push, PR 创建
3. GitHub CLI (`gh`): 查看 PR, Issue, workflow runs

**私有化影响**：
- ✅ **本地开发无影响**：所有文件操作在本地进行
- ✅ **Git 操作无影响**：使用 SSH 或 HTTPS + PAT
- ⚠️ **GitHub CLI 需要认证**：
  - 首次使用需要 `gh auth login`
  - PAT 需要 `repo` 权限（而非 `public_repo`）

**验证方法**：
```bash
gh auth status
# 如果显示权限不足，重新登录：
gh auth login --scopes repo
```

---

### 4.3 MCP 服务器

**相关服务器**：
- `scripts/aistock_mcp_server.py`: AIstock 验证 MCP 服务器
- `scripts/aistock_validation_mcp_server.py`: 验证中心 MCP 服务器

**私有化影响**：
- ⚠️ 如果 MCP 服务器调用 GitHub API，需要 PAT 具有 `repo` 权限
- ✅ 当前实现已支持从环境变量读取 `GITHUB_TOKEN`

**配置检查**：
```bash
# 确保环境变量已设置
echo $GITHUB_TOKEN
# 或在 .env 文件中：
# GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

---

## 5. 协作和访问控制

### 5.1 团队成员访问

**当前状态**：
- 公开仓库：任何人可查看、fork、clone
- 私有仓库：需要显式邀请

**私有化后**：
1. **Owner**: `licong01-cloud`（完全权限）
2. **Collaborators**: 需要逐一邀请
   - 路径：Settings → Collaborators → Add people
   - 权限级别：Read / Triage / Write / Maintain / Admin

**建议**：
- 列出所有需要访问的团队成员
- 根据职责分配权限（开发者 = Write，CI/CD = Admin）

---

### 5.2 外部贡献

**当前状态**：
- 公开仓库：任何人可 fork 和提交 PR

**私有化后**：
- ��� 外部用户无法 fork
- ❌ 外部用户无法查看 Issues / Discussions
- ✅ 可通过邀请链接临时授权

**影响评估**：
- 如果项目不需要外部贡献，无影响
- 如果需要外部审查（如安全审计），需要临时邀请

---

### 5.3 CI/CD 第三方集成

**当前集成**：
- GitHub Actions: ✅ 原生支持私有仓库
- Dependabot: ✅ 私有仓库可用（需要 GitHub Advanced Security，Free 账户不可用）
- Code scanning: ⚠️ 需要 GitHub Advanced Security（付费功能）

**建议**：
- 保持当前 GitHub Actions 配置
- 如果需要 Dependabot，考虑升级到 Pro/Team 计划

---

## 6. 数据��资产影响

### 6.1 已存在的 Fork

**当前状态**：
- Fork 数量：0（无人 fork）

**私有化后**：
- ✅ 无影响（无现有 fork）

---

### 6.2 Stars 和 Watchers

**当前状态**：
- Stars: 0
- Watchers: 0

**私有化后**：
- ✅ 无影响（无社区关注）

---

### 6.3 Issues 和 PR 历史

**当前状态**：
- Issues: 34 个（部分已关闭）
- PRs: 若干（历史记录）

**私有化后**：
- ✅ **完全保留**：所有 Issues、PRs、评论、标签、里程碑
- ⚠️ **外部不可见**：外部用户无法访问历史 Issue URL

---

## 7. 迁移检查清单

### 7.1 迁移前准备

- [ ] **备份仓库**：
  ```bash
  git clone --mirror git@github.com:licong01-cloud/AIstock.git
  ```

- [ ] **确认 PAT 权限**：
  - 检查当前 PAT 是否具有 `repo` 权限（而非仅 `public_repo`）
  - 路径：GitHub Settings → Developer settings → Personal access tokens

- [ ] **更新远端节点认证**：
  - 在 `rdagent-node (192.168.50.215)` 上检查 `git remote -v`
  - 如果使用 HTTPS，更新为 SSH 或带 PAT 的 HTTPS URL

- [ ] **通知团队成员**：
  - 告知即将私有化
  - 收集需要访���权限的成员列表

---

### 7.2 迁移操作

1. **设置为私有**：
   - 路径：Settings → General → Danger Zone → Change repository visibility → Make private
   - 确认操作

2. **邀请协作者**：
   - Settings → Collaborators → Add people
   - 为每个成员分配适当权限

3. **验证 GitHub Actions**：
   ```bash
   # 触发一次 CI 测试
   git commit --allow-empty -m "test: verify CI after privatization"
   git push origin main
   ```

4. **验证 self-hosted runner**：
   - 检查 runner 状态：Settings → Actions → Runners
   - 确认 runner 在线且可用

---

### 7.3 迁移后验证

- [ ] **CI/CD 流水线**：
  - 触发一次 PR，验证 `test.yml` 正常运行
  - 等待下一次 nightly 运行（03:07 CST），验证 `nightly.yml` 正常

- [ ] **GitHub CLI**：
  ```bash
  gh repo view licong01-cloud/AIstock
  gh pr list
  gh issue list
  ```

- [ ] **远端节点**：
  ```bash
  # 在 rdagent-node 上
  git pull origin main
  ```

- [ ] **MCP 服务器**：
  - 启动 `aistock_mcp_server.py`
  - 测试 GitHub API 调用（如 `report_bug`）

- [ ] **Claude Code**：
  - 执行 `gh auth status`
  - 测试 `gh pr view` 等命令

---

## 8. 风险评估

| 风险 | 严重性 | 可能性 | 缓解措施 |
|------|--------|--------|---------|
| GitHub Actions 配额不足 | 低 | 极低 | 当前使用率仅 4%，Free 账户足够 |
| 远端节点无法 pull | 中 | 低 | 迁移前��新为 SSH 认证 |
| 外部审计无法访问 | 低 | 低 | 临时邀请外部审计员 |
| PAT 权限不足 | 中 | 中 | 迁移前验证 PAT 具有 `repo` 权限 |
| 历史 Issue URL 失效 | 低 | 高 | 预期行为，符合私有化目标 |

---

## 9. 成本分析

### 9.1 GitHub 计划对比

| 功能 | Free | Pro ($4/月) | Team ($4/人/月) |
|------|------|------------|----------------|
| 私有仓库 | ✅ 无限 | ✅ 无限 | ✅ 无限 |
| Actions 配额 | 2000 分钟 | 3000 分钟 | 3000 分钟 |
| 协作者 | 无限 | 无限 | 无限 |
| Protected branches | ❌ 基础 | ✅ 完整 | ✅ 完整 |
| Code owners | ❌ | ✅ | ✅ |
| GitHub Pages | ❌ | ✅ | ✅ |
| Advanced Security | ❌ | ❌ | ✅ |

**推荐**：
- **当前需求**: Free 账户完全足够
- **未来扩展**: 如需 Protected branches 或 Code owners，升级到 Pro

---

### 9.2 总成本估算

| 项目 | 成本 |
|------|------|
| GitHub 账户 | $0（Free）或 $4/月（Pro） |
| GitHub Actions | $0（配额内） |
| Self-hosted runner | $0（本地机器） |
| 开发工具链 | $0（无变化） |
| **总计** | **$0 - $4/月** |

---

## 10. 替代方案

### 10.1 保持公开 + 限制性许可证

**方案**：
- 仓库保持公开
- 修改 LICENSE 为 AGPL-3.0 或自定义限制性许可证
- 添加 "仅供学习研究，禁止商用" 条款

**优点**：
- ✅ 保持开源社区可见性
- ✅ 法律上限制商业使用
- ✅ 无需修改任何配置

**缺点**：
- ❌ 代码仍然可见
- ❌ 依赖用户遵守许可证（执法成本高）

---

### 10.2 部分私有化

**方案**：
- 核心代码仓库私有
- 文档和示例仓库公开

**优点**：
- ✅ 保护核心 IP
- ✅ 保持社区展示

**缺点**：
- ❌ 需要维护两个仓库
- ❌ 同步成本高

---

## 11. 建议和结论

### 11.1 核心建议

1. **技术上完全可行**：
   - 所有自动化流程在私有模式下正常运行
   - 成本影响极小（Free 账户足够）

2. **迁移前必做**：
   - ✅ 验证 PAT 具有 `repo` 权限
   - ✅ 更新远端节点为 SSH 认证
   - ✅ 备份仓库（`git clone --mirror`）

3. **迁移后验证**：
   - ✅ 触发一次 CI 测试
   - ✅ 等待下一次 nightly 运行
   - ✅ 测试 GitHub CLI 和 MCP 服务器

---

### 11.2 最终结论

**推荐操作**：
- ✅ **可以安全地改为私有仓库**
- ✅ 使用 Free 账户（配额足够）
- ✅ 按照迁移检查清单逐步操作

**预期影响**：
- ✅ 技术功能：无影响
- ✅ 开发流程：无影响
- ✅ 成本：$0（Free 账户）
- ⚠️ 外部可见性：完全不可见（符合私有化目标）

---

## 12. 附录

### 12.1 相关文档

- [GitHub Actions 私有仓库配额](https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions)
- [GitHub 私有仓库功能对比](https://docs.github.com/en/get-started/learning-about-github/githubs-products)
- [Self-hosted runners 配置](https://docs.github.com/en/actions/hosting-your-own-runners)

---

### 12.2 联系人

- **技术负责人**: licong01-cloud
- **文档维护**: Claude (Opus 4.7)
- **审核日期**: 待定

---

**文档结束**
