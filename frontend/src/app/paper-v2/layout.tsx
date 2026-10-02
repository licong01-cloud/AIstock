"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import "./paper-v2.css";

const TABS = [
  { href: "/simulation/localsim", label: "LocalSIM", exact: false },
  { href: "/paper-v2/packages", label: "策略包", exact: false },
  { href: "/paper-v2/selection", label: "选股中心", exact: false },
  { href: "/paper-v2/advisory", label: "荐股中心", exact: false },
  { href: "/paper-v2/miniqmt-sim", label: "MiniQMT", exact: false },
  { href: "/paper-v2/model-hmm", label: "模型与 HMM", exact: false },
];

export default function PaperV2Layout({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const advisoryRoute = pathname === "/paper-v2/advisory" || pathname?.startsWith("/paper-v2/advisory/");
  return (
    <div className={`pv2-shell ${advisoryRoute ? "pv2-shell-advisory" : ""}`}>
      <header className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">StrategyPackage 权威入口</div>
            <h1>策略包与交易应用</h1>
            <p>策略包、选股、荐股、MiniQMT 与模型维护入口；LocalSIM 已迁移到独立的统一运行时产品。</p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">禁止静默兜底</span>
            <span className="pv2-chip">LocalSim 独立</span>
            <span className="pv2-chip">MiniQMT 权威交易</span>
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
