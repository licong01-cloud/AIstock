import "./globals.css";
import type { ReactNode } from "react";
import Sidebar from "./Sidebar";
import DataAlertsToast from "./DataAlertsToast";

export const metadata = {
  title: "复合多AI智能体股票团队分析系统",
  description: "基于 FastAPI + Next.js 的多智能体股票分析与选股系统",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>
        <div className="app-shell">
          <Sidebar />
          <main className="app-main">{children}</main>
        </div>
        <DataAlertsToast />
      </body>
    </html>
  );
}
