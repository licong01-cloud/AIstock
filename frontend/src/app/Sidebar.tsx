"use client";

import Link from "next/link";
import { useState } from "react";

const NAV_GROUPS: {
  title: string;
  items: { href: string; label: string }[];
}[] = [
  {
    title: "🔍 功能导航",
    items: [
      { href: "/analysis", label: "🏠 股票分析" },
      { href: "/analysis-trend", label: "📈 趋势分析" },
    ],
  },
  {
    title: "🎯 选股板块",
    items: [
      { href: "/watchlist", label: "⭐ 自选股票池" },
      { href: "/cloud-screening", label: "☁ 云选股" },
      { href: "/market-news", label: "📰 市场资讯 / 市场快讯" },
    ],
  },
  {
    title: "📊 策略分析",
    items: [
      { href: "/sector-strategy", label: "🎯 智策板块" },
    ],
  },
  {
    title: "💼 投资管理",
    items: [
      { href: "/portfolio", label: "📊 持仓分析" },
      { href: "/smart-monitor", label: "🤖 AI盯盘" },
      { href: "/monitor", label: "📡 实时监测" },
    ],
  },
  {
    title: "📈 QMT模拟盘交易",
    items: [
      { href: "/qmt/positions", label: "💼 持仓管理" },
      { href: "/qmt/strategies", label: "📊 策略管理" },
    ],
  },
  {
    title: "系统与数据",
    items: [
      { href: "/config", label: "⚙️ 环境配置" },
      { href: "/local-data", label: "🗄️ 本地数据管理" },
    ],
  },
  {
    title: "RD-Agent管理",
    items: [
      { href: "/qlib", label: "📦 Qlib Snapshot 导出" },
      { href: "/scheduler", label: "🗓️ RD-Agent 调度" },
      { href: "/rdagent/sync", label: "🎯 RD-Agent 同步" },
      { href: "/rdagent/tasks-sync", label: "🧰 Task 资产同步" },
      { href: "/rdagent/tasks", label: "🗂️ Task 列表" },
      { href: "/rdagent/task-selection", label: "🚀 Task 选股" },
      { href: "/rdagent/strategies-catalog", label: "🎯 策略目录" },
      { href: "/rdagent/factors", label: "📊 因子目录" },
      { href: "/rdagent/models", label: "🧠 模型目录" },
      { href: "/rd-agent/prompt-packs", label: "📝 提示词模板管理" },
      { href: "/config/rdagent-llm", label: "🤖 RDagent 模型配置" },
    ],
  },
  {
    title: "QuantEvolver",
    items: [
      { href: "/quantevolver", label: "🧬 QE 总览" },
      { href: "/quantevolver/factors", label: "📊 因子库" },
      { href: "/quantevolver/models", label: "🧠 模型库" },
      { href: "/quantevolver/strategies", label: "🎯 策略库" },
      { href: "/quantevolver/compose", label: "🔧 组合配置" },
      { href: "/quantevolver/experiments", label: "🧪 实验历史" },
      { href: "/quantevolver/evolution", label: "🔁 自动演进" },
      { href: "/quantevolver/evolution/sota", label: "🏆 SOTA 殿堂" },
      { href: "/quantevolver/selection", label: "🚀 实验选股" },
      { href: "/quantevolver/prompts", label: "📝 提示词配置" },
    ],
  },
];

export default function Sidebar() {
  // 管理每个一级目录的展开状态，默认全部折叠
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const toggleGroup = (title: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(title)) {
        next.delete(title);
      } else {
        next.add(title);
      }
      return next;
    });
  };

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h1 className="sidebar-title">📈 多AI智能体股票分析</h1>
        <p className="sidebar-subtitle">基于 DeepSeek 的专业量化投资系统</p>
      </div>

      <nav className="sidebar-nav">
        {NAV_GROUPS.map((group) => {
          const isExpanded = expandedGroups.has(group.title);
          return (
            <div key={group.title} className="sidebar-group">
              <div
                className="sidebar-group-title sidebar-group-toggle"
                onClick={() => toggleGroup(group.title)}
              >
                <span className="toggle-icon">{isExpanded ? "▼" : "▶"}</span>
                {group.title}
              </div>
              {isExpanded && (
                <div className="sidebar-group-items">
                  {group.items.map((item) => (
                    <Link key={item.href} href={item.href} className="sidebar-link">
                      {item.label}
                    </Link>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </nav>
    </aside>
  );
}
