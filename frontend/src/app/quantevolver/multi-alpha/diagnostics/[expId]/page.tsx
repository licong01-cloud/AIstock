"use client";

import { useEffect } from "react";
import Link from "next/link";

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export default function MultiAlphaDiagnosticsDetailRedirect({ params }: { params: { expId: string } }) {
  const taskKey = safeDecode(params.expId);
  const target = `/quantevolver/multi-alpha/combine-backtest/${encodeURIComponent(taskKey)}?tab=diagnostics`;

  useEffect(() => {
    window.location.replace(target);
  }, [target]);

  return (
    <div style={{ padding: 24, maxWidth: 900 }}>
      <h1 style={{ fontSize: 20, fontWeight: 800, marginBottom: 12, color: "#0f172a" }}>
        旧诊断详情已合并到 combine-backtest
      </h1>
      <p style={{ fontSize: 13, color: "#475569", lineHeight: 1.7 }}>
        旧路由不再读取 /quantevolver/multi-alpha/:id/diagnostics。若该 id 是 combine task_key，将跳转到组合回测详情页诊断
        tab；否则请从组合回测列表选择任务。
      </p>
      <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
        <Link
          href={target}
          style={{
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
          打开 combine 诊断
        </Link>
        <Link
          href="/quantevolver/multi-alpha/combine-backtest"
          style={{
            padding: "8px 14px",
            borderRadius: 8,
            backgroundColor: "#fff",
            border: "1px solid #cbd5e1",
            color: "#475569",
            fontWeight: 800,
            fontSize: 13,
            textDecoration: "none",
          }}
        >
          返回组合回测列表
        </Link>
      </div>
    </div>
  );
}
