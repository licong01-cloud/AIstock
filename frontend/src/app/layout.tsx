import "./globals.css";
import type { ReactNode } from "react";
import Link from "next/link";
import { redirect } from "next/navigation";

export const metadata = {
  title: "复合多AI智能体股票团队分析系统",
  description: "基于 FastAPI + Next.js 的多智能体股票分析与选股系统",
};

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
      { href: "/hotboard", label: "🔥 热点板块跟踪" },
      { href: "/watchlist", label: "⭐ 自选股票池" },
      { href: "/indicator-screening", label: "📊 指标选股" },
      { href: "/cloud-screening", label: "☁ 云选股" },
      { href: "/main-force", label: "💰 主力选股" },
    ],
  },
  {
    title: "📊 策略分析",
    items: [
      { href: "/sector-strategy", label: "🎯 智策板块" },
      { href: "/longhubang", label: "🐉 智瞰龙虎" },
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
    title: "系统与数据",
    items: [
      { href: "/history", label: "📖 历史记录" },
      { href: "/config", label: "⚙️ 环境配置" },
      { href: "/local-data", label: "🗄️ 本地数据管理" },
      { href: "/quant-models", label: "🧠 模型调度" },
    ],
  },
];

function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h1 className="sidebar-title">📈 多AI智能体股票分析</h1>
        <p className="sidebar-subtitle">基于 DeepSeek 的专业量化投资系统</p>
      </div>

      <nav className="sidebar-nav">
        {NAV_GROUPS.map((group) => (
          <div key={group.title} className="sidebar-group">
            <div className="sidebar-group-title">{group.title}</div>
            <div className="sidebar-group-items">
              {group.items.map((item) => (
                <Link key={item.href} href={item.href} className="sidebar-link">
                  {item.label}
                </Link>
              ))}
            </div>
          </div>
        ))}
      </nav>
    </aside>
  );
}

export default function RootLayout({ children }: { children: ReactNode }) {
  // 默认根路径直接跳转到 /analysis，保持与旧应用首页一致
  if (typeof window === "undefined") {
    // SSR 阶段保持原样，页面文件中会自己处理重定向或内容
  }

  return (
    <html lang="zh-CN">
      <body>
        <div className="app-shell">
          <Sidebar />
          <main className="app-main">{children}</main>
        </div>
      </body>
    </html>
  );
}
