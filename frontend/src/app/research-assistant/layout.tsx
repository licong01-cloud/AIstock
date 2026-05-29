"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import "./research-assistant.css";

const TABS = [
  { href: "/research-assistant", label: "对话", exact: true },
  { href: "/research-assistant/chat", label: "对话窗口" },
  { href: "/research-assistant/admin", label: "后台管理" },
  { href: "/research-assistant/workbench", label: "工作台" },
  { href: "/research-assistant/tasks", label: "任务" },
  { href: "/research-assistant/streams", label: "发现流" },
  { href: "/research-assistant/issue-candidates", label: "候选 Issue" },
  { href: "/research-assistant/memory", label: "记忆" },
  { href: "/research-assistant/graph", label: "图谱" },
  { href: "/research-assistant/mcp-tools", label: "MCP 工具" },
  { href: "/research-assistant/skills", label: "Skills" },
  { href: "/research-assistant/approvals", label: "审批" },
  { href: "/research-assistant/models", label: "模型路由" },
  { href: "/research-assistant/trace", label: "Trace" },
  { href: "/research-assistant/settings", label: "设置" },
];

export default function ResearchAssistantLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  return (
    <div className="ra-shell">
      <header className="ra-hero-shell">
        <div className="ra-hero-top">
          <div>
            <div className="ra-kicker">AIstock Research Assistant / MCP + Skill</div>
            <h1>研究与实验综合助理</h1>
            <p>主入口是自然语言对话；后台页面只用于审计、Trace、MCP 目录和任务账本。</p>
          </div>
          <div className="ra-chip-row">
            <span className="ra-chip">LLM 先理解</span>
            <span className="ra-chip">确认后才执行 MCP</span>
            <span className="ra-chip">主界面不显示 JSON</span>
            <span className="ra-chip">全链路可审计</span>
          </div>
        </div>
        <nav className="ra-tabs" aria-label="研究助理功能导航">
          {TABS.map((tab) => {
            const active = tab.exact ? pathname === tab.href : pathname === tab.href || pathname?.startsWith(`${tab.href}/`);
            return <Link className={`ra-tab ${active ? "ra-tab-active" : ""}`} href={tab.href} key={tab.href}>{tab.label}</Link>;
          })}
        </nav>
      </header>
      {children}
    </div>
  );
}
