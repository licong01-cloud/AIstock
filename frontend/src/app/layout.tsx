import "./globals.css";
import type { ReactNode } from "react";
import Sidebar from "./Sidebar";

export const metadata = {
  title: "复合多AI智能体股票团队分析系统",
  description: "基于 FastAPI + Next.js 的多智能体股票分析与选股系统",
};

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
