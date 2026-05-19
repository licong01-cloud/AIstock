# AIstock 流水线平台 P0/P1 加固实施执行清单

更新时间：2026-05-19

## 1. 分支与隔离边界

- 实现分支：`codex/validation-platform-p0p1-impl-20260519`
- 实现 worktree：`F:\Dev\AIstock_worktrees\validation-platform-p0p1-impl-20260519`
- 设计基线分支：`codex/validation-platform-p0p1-design-20260519`
- 合入策略：开发完成、验证通过、用户确认后才允许合入 `main`
- 生产边界：不得启动、重启或修改生产 `8001` / `3000` 服务；不得写生产 DB

## 2. 设计输入

本实施以以下设计为准：

- `docs/architecture/validation_platform_p0p1_hardening_design_20260519.md`
- `docs/architecture/validation_test_plan_resource_policy_design_20260519.md`
- `tests/aistock_validation/catalog/resource_policies.yaml`
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`

## 3. 阶段划分

### 阶段 A：P0 平台可信基础

目标：解决流水线读取错误仓库、错误分支或未登记目录的问题。

交付内容：

- 固定 repo root 解析与平台自检
- Validation Center health API 增强
- 测试计划目录一致性自检脚本
- `nox` session 接入
- 结构化报告输出

验证要求：

- 可在独立 worktree 内运行
- 不依赖生产服务
- 不写生产 DB
- 能发现缺失 nox session、未知 command_key、缺失 UI target、未知 resource policy

### 阶段 B：P1 夜间验证与 Runner 可视化

目标：让平台可见 nightly 是否真正可运行、最近运行状态是否健康。

交付内容：

- Nightly / Runner 状态 API
- 最近 workflow / validation run 状态聚合
- UI 卡片入口和状态详情

验证要求：

- GitHub token 缺失时降级为可解释状态，不阻断本地流水线
- runner 不在线时显示明确风险
- 不要求触发真实生产长任务

### 阶段 C：P1 失败自动 BUG 闭环

目标：测试失败、guardrail 失败、workflow 失败可稳定转换为 BUG / GitHub Issue 同步事件。

交付内容：

- Failure Event 模型
- 失败事件到 BUG registry 的转换脚本
- GitHub Issue 同步字段补充
- MCP 查询闭环补充

验证要求：

- dry-run 可验证完整链路
- 不创建重复 BUG
- GitHub 不可用时保留本地 BUG 记录并显示待同步状态

### 阶段 D：UI 卡片化与合入门禁

目标：前端页面按卡片展示平台状态、模块质量、测试计划、GitHub Issue、分支/PR、历史遗留问题。

交付内容：

- 流水线页面顶部导航/卡片化入口
- 概要 + 可展开详情
- 合入门禁只读预览
- 对第二阶段工程健康驾驶舱保留数据接口扩展点

验证要求：

- 本地前端构建通过
- 后端 API 契约测试通过
- UI target catalog 自检通过

## 4. Agent Teams 分工

- 主线程：分支、集成、提交、验证、最终合入条件判断
- Agent A：后端目录自检与资源策略校验
- Agent B：平台健康、repo root 与 nightly/runner API
- Agent C：前端流水线卡片化 UI 与详情页
- Agent D：失败事件、BUG 闭环与 GitHub 同步 dry-run
- Agent E：验证审查、风险扫描与合入门禁检查

所有 agent 必须遵守：

- 不修改生产服务配置
- 不触碰生产 DB
- 不合入 `main`
- 不回滚他人 worktree 中的改动
- 修改文件需要列明路径、验证命令和结果

## 5. 提交与推送策略

- 每个阶段通过验证后单独提交
- 提交信息使用明确 scope，例如：
  - `feat(validation): add catalog integrity check`
  - `feat(validation): expose platform health summary`
  - `feat(validation-ui): add pipeline status cards`
  - `test(validation): cover resource policy validation`
- 开发分支可推送到远端供 PR 使用
- 未经用户确认不得合入 `main`

## 6. 最终合入 main 前门禁

必须全部满足：

- `git status --short --branch` 干净
- 后端单元测试通过
- catalog integrity 通过
- 前端 lint / build 或项目现有等价验证通过
- MCP Validation Center 查询可用，或给出明确的环境原因
- GitHub Issue dry-run / 同步状态验证通过，或给出 token/网络原因
- 未触碰生产 `8001` / `3000`
- 未写生产 DB
- 已推送实现分支
- 用户明确确认合入

## 7. 当前执行记录

| 时间 | 阶段 | 状态 | 提交 | 验证 |
| --- | --- | --- | --- | --- |
| 2026-05-19 | 初始化 | 进行中 | 待提交 | 待执行 |
