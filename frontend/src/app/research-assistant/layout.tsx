"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import "@/app/paper-v2/paper-v2.css";

const TABS = [
  { href: "/research-assistant", label: "总览", exact: true },
  { href: "/research-assistant/chat", label: "对话" },
  { href: "/research-assistant/workbench", label: "工作台" },
  { href: "/research-assistant/tasks", label: "任务" },
  { href: "/research-assistant/streams", label: "发现流" },
  { href: "/research-assistant/issue-candidates", label: "候选 Issue" },
  { href: "/research-assistant/notifications", label: "通知" },
  { href: "/research-assistant/memory", label: "记忆" },
  { href: "/research-assistant/graph", label: "图谱" },
  { href: "/research-assistant/mcp-tools", label: "MCP 工具" },
  { href: "/research-assistant/skills", label: "Skills" },
  { href: "/research-assistant/approvals", label: "审批" },
  { href: "/research-assistant/reports", label: "报告" },
  { href: "/research-assistant/models", label: "模型路由" },
  { href: "/research-assistant/external-agents", label: "外部 Agent" },
  { href: "/research-assistant/trace", label: "Trace" },
  { href: "/research-assistant/settings", label: "设置" },
];

export default function ResearchAssistantLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  return (
    <div className="pv2-shell">
      <header className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">AIstock Research Assistant / MCP + Skill</div>
            <h1>研究与实验综合助理</h1>
            <p>以长期记忆为核心，通过 MCP/API 执行任务、展示实时进展、审批高风险操作，并把候选 Issue 与证据留在可回放队列中。</p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">不控制鼠标键盘</span>
            <span className="pv2-chip">不自动创建正式 Issue</span>
            <span className="pv2-chip">无实盘路径</span>
            <span className="pv2-chip">无静默兜底</span>
          </div>
        </div>
        <nav className="pv2-tabs" aria-label="研究助理顶部导航">
          {TABS.map((tab) => {
            const active = tab.exact ? pathname === tab.href : pathname === tab.href || pathname?.startsWith(`${tab.href}/`);
            return <Link className={`pv2-tab ${active ? "pv2-tab-active" : ""}`} href={tab.href} key={tab.href}>{tab.label}</Link>;
          })}
        </nav>
      </header>
      {children}
    </div>
  );
}
