"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ReactNode } from "react";

const TABS = [
  { href: "/paper-trading/selection", label: "🔍 实盘选股" },
  { href: "/paper-trading/training", label: "🔧 模型训练" },
  { href: "/paper-trading/config", label: "⚙️ 模拟盘配置" },
  { href: "/paper-trading/monitor", label: "📈 模拟盘监控" },
  { href: "/paper-trading/reports", label: "📋 报表分析" },
];

export default function PaperTradingLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div style={{ padding: "0 24px 24px 24px" }}>
      {/* QE 风格渐变标题卡片 */}
      <section
        style={{
          background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)",
          borderRadius: 16,
          padding: "24px 24px 0 24px",
          color: "#fff",
          marginBottom: 20,
          marginTop: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 28, fontWeight: 700 }}>📊 实盘演练</h1>
        <p style={{ marginTop: 8, opacity: 0.85, fontSize: 14 }}>
          T-1 信号生成 · T 日模拟成交 · 因子归因 · 实盘 IC 追踪 · 模型重训练
        </p>

        {/* Tab 导航 — 白色字体 */}
        <nav style={{ display: "flex", gap: 4, marginTop: 16 }}>
          {TABS.map((tab) => {
            const active = pathname === tab.href || pathname?.startsWith(tab.href + "/");
            return (
              <Link
                key={tab.href}
                href={tab.href}
                style={{
                  padding: "10px 18px",
                  fontSize: 14,
                  fontWeight: active ? 700 : 500,
                  color: "#fff",
                  opacity: active ? 1 : 0.75,
                  borderBottom: active ? "3px solid #fff" : "3px solid transparent",
                  textDecoration: "none",
                  transition: "all 0.15s",
                }}
              >
                {tab.label}
              </Link>
            );
          })}
        </nav>
      </section>

      {children}
    </div>
  );
}
