"use client";

import Link from "next/link";

const ADMIN_LINKS = [
  {
    href: "/research-assistant/workbench",
    title: "MCP 执行工作台",
    description: "查看工具目录、配置草稿、preflight、dry-run 和深链，允许展示审计详情。",
  },
  {
    href: "/research-assistant/tasks",
    title: "Task Ledger",
    description: "回放任务事件、失败 triage、耗时和状态流转。",
  },
  {
    href: "/research-assistant/memory",
    title: "Memory Ledger",
    description: "管理候选记忆、审批状态、来源证据和长期记忆事实源。",
  },
  {
    href: "/research-assistant/mcp-tools",
    title: "MCP 工具目录",
    description: "查看 MCP server、tool schema、risk level、preflight 和审批要求。",
  },
  {
    href: "/research-assistant/skills",
    title: "本地 Skill Catalog",
    description: "查看本地 skill、权限范围、checksum、启用状态和调用记录。",
  },
  {
    href: "/research-assistant/approvals",
    title: "审批中心",
    description: "处理 L2+ 风险动作审批、确认文本和执行结果回填。",
  },
  {
    href: "/research-assistant/models",
    title: "模型路由",
    description: "查看主模型、低价模型、长上下文模型和路由策略。",
  },
  {
    href: "/research-assistant/trace",
    title: "Trace 与成本",
    description: "审计 LLM、MCP、Skill 的调用耗时、成本和错误信息。",
  },
];

export default function ResearchAssistantAdminPage() {
  return (
    <main className="ra-admin-shell">
      <section className="ra-admin-hero">
        <span className="ra-chat-eyebrow">后台管理 / 审计区</span>
        <h2>旧版表格与 JSON 详情保留在这里</h2>
        <p>
          主对话入口只展示自然语言、计划卡、确认卡和图形化状态。后台管理区面向开发、审计和问题排查，
          可以查看 ID、Trace、schema、payload 和完整事件。
        </p>
      </section>
      <div className="ra-admin-grid">
        {ADMIN_LINKS.map((item) => (
          <Link className="ra-admin-card" href={item.href} key={item.href}>
            <span>{item.title}</span>
            <p>{item.description}</p>
          </Link>
        ))}
      </div>
    </main>
  );
}
