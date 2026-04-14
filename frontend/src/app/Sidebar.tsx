"use client";

import Link from "next/link";
import { useState } from "react";

const NAV_GROUPS: {
  title: string;
  items: { href: string; label: string }[];
}[] = [
  {
    title: "QuantEvolver",
    items: [
      { href: "/quantevolver", label: "🧬 QE 总览" },
      { href: "/quantevolver/factors", label: "📊 因子库" },
      { href: "/quantevolver/factor-correlation", label: "🔗 因子相关性" },
      { href: "/quantevolver/models", label: "🧠 模型库" },
      { href: "/quantevolver/strategies", label: "🎯 策略库" },
      { href: "/quantevolver/execution-algorithms", label: "⚡ 执行算法库" },
      { href: "/quantevolver/compose", label: "🔧 组合配置" },
      { href: "/quantevolver/experiments", label: "🧪 实验历史" },
      { href: "/quantevolver/evolution", label: "🔁 自动演进" },
      { href: "/quantevolver/evolution/sota", label: "🏆 SOTA 殿堂" },
      { href: "/quantevolver/model-training", label: "🧠 模型训练" },
      { href: "/quantevolver/selection", label: "🚀 实验选股" },
      { href: "/quantevolver/prompts", label: "📝 提示词配置" },
    ],
  },
  {
    title: "RD-Agent管理",
    items: [
      { href: "/rdagent/dispatch", label: "🖥️ 多节点调度中心" },
      { href: "/qlib", label: "📦 Qlib Snapshot 导出" },
      { href: "/scheduler", label: "🗓️ RD-Agent 调度" },
      { href: "/rdagent/tasks-sync", label: "🧰 Task 资产同步" },
      { href: "/rdagent/tasks", label: "🗂️ Task 列表" },
      { href: "/rdagent/task-selection", label: "🚀 Task 选股" },
      { href: "/config/rdagent-llm", label: "🤖 RDagent 模型配置" },
    ],
  },
  {
    title: "系统与数据",
    items: [
      { href: "/config", label: "⚙️ 环境配置" },
      { href: "/local-data", label: "🗄️ 本地数据管理" },
      { href: "/rdagent/dispatch/system-monitor", label: "📡 系统监控" },
      { href: "/rdagent/dispatch/db-monitor", label: "🗄️ 数据库监控" },
    ],
  },
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
    title: "📊 实盘演练",
    items: [
      { href: "/paper-trading/selection", label: "🔍 实盘选股" },
      { href: "/paper-trading/training", label: "🔧 模型训练" },
      { href: "/paper-trading/config", label: "⚙️ 模拟盘配置" },
      { href: "/paper-trading/monitor", label: "📈 模拟盘监控" },
      { href: "/paper-trading/reports", label: "📋 报表分析" },
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
