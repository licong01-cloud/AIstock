"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Experiment = {
  experiment_id: string;
  experiment_name: string;
  status: string;
  ic: number | null;
  icir: number | null;
  alpha_mode: string;
  created_at: string;
};

export default function MultiAlphaDiagnosticsIndex() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API}/quantevolver/experiments?limit=50&alpha_mode=multi`)
      .then((r) => r.json())
      .then((data) => {
        if (data.ok) setExperiments(data.items || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  return (
    <div style={{ padding: 24, maxWidth: 900 }}>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 16, color: "#7c3aed" }}>
        Multi-Alpha 诊断
      </h1>
      <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 20 }}>
        选择一个多Alpha实验查看诊断报告（组性能、Meta权重、相关性、瓶颈）
      </p>

      {loading ? (
        <div style={{ color: "#9ca3af", fontSize: 13 }}>加载中...</div>
      ) : experiments.length === 0 ? (
        <div style={{ color: "#9ca3af", fontSize: 13, padding: 40, textAlign: "center", background: "#f9fafb", borderRadius: 8 }}>
          暂无多Alpha实验。请先在 Compose 中创建。
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {experiments.map((exp) => (
            <Link
              key={exp.experiment_id}
              href={`/quantevolver/multi-alpha/diagnostics/${exp.experiment_id}`}
              style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                padding: "12px 16px", borderRadius: 8, background: "#fff",
                border: "1px solid #e5e7eb", textDecoration: "none", color: "inherit",
                transition: "border-color 0.15s",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.borderColor = "#8b5cf6")}
              onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#e5e7eb")}
            >
              <div>
                <div style={{ fontWeight: 600, fontSize: 14 }}>{exp.experiment_id}</div>
                <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>
                  {exp.created_at ? new Date(exp.created_at).toLocaleString("zh-CN") : "-"}
                </div>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                {exp.ic != null && (
                  <span style={{ fontSize: 12, fontWeight: 600, color: exp.ic > 0.03 ? "#059669" : "#6b7280" }}>
                    IC: {exp.ic.toFixed(4)}
                  </span>
                )}
                <span style={{
                  fontSize: 11, padding: "2px 8px", borderRadius: 12, fontWeight: 600,
                  background: exp.status === "completed" ? "#10b98120" : exp.status === "running" ? "#f59e0b20" : "#6b728020",
                  color: exp.status === "completed" ? "#10b981" : exp.status === "running" ? "#f59e0b" : "#6b7280",
                }}>
                  {exp.status}
                </span>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
