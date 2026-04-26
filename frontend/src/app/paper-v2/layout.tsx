"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import "./paper-v2.css";

const TABS = [
  { href: "/paper-v2", label: "总览", exact: true },
  { href: "/paper-v2/packages", label: "策略包", exact: false },
  { href: "/paper-v2/selection", label: "选股中心", exact: false },
  { href: "/paper-v2/portfolios", label: "模拟组合", exact: false },
  { href: "/paper-v2/model-hmm", label: "模型与 HMM", exact: false },
  { href: "/paper-v2/settings", label: "设置", exact: false },
];

export default function PaperV2Layout({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  return (
    <div className="pv2-shell">
      <header className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">StrategyPackage 权威入口</div>
            <h1>模拟盘 v2</h1>
            <p>基于策略包的分钟级模拟盘控制台，覆盖就绪检查、订单、成交、账本、历史回放和模型维护。</p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">禁止静默兜底</span>
            <span className="pv2-chip">仅分钟线交易</span>
            <span className="pv2-chip">仅回测验证策略</span>
          </div>
        </div>
        <nav className="pv2-tabs">
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
