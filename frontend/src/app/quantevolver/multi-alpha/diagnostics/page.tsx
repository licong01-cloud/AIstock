"use client";

import { useEffect } from "react";
import Link from "next/link";

export default function MultiAlphaDiagnosticsIndexRetired() {
  useEffect(() => {
    window.location.replace("/quantevolver/multi-alpha/combine-backtest");
  }, []);

  return (
    <div style={{ padding: 24, maxWidth: 900 }}>
      <h1 style={{ fontSize: 20, fontWeight: 800, marginBottom: 12, color: "#0f172a" }}>
        多Alpha 诊断入口已迁移
      </h1>
      <p style={{ fontSize: 13, color: "#475569", lineHeight: 1.7 }}>
        旧页面绑定已废弃的 alpha_mode=multi 实验列表，当前诊断从 combine-backtest 任务详情页的“诊断”tab 读取真实
        combine scheme/LOO 数据。
      </p>
      <Link
        href="/quantevolver/multi-alpha/combine-backtest"
        style={{
          display: "inline-block",
          marginTop: 12,
          padding: "8px 14px",
          borderRadius: 8,
          backgroundColor: "#eff6ff",
          border: "1px solid #bfdbfe",
          color: "#1d4ed8",
          fontWeight: 800,
          fontSize: 13,
          textDecoration: "none",
        }}
      >
        打开组合回测列表
      </Link>
    </div>
  );
}
