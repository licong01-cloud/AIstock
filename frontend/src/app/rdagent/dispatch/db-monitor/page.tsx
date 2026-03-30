"use client";

import { useEffect, useState } from "react";

const GRAFANA_BASE = "http://localhost:3001";

const DASHBOARDS = [
  { uid: "pg-advanced", label: "高级监控" },
  { uid: "pg-overview", label: "基础概览" },
] as const;

export default function DbMonitorPage() {
  const [iframeSrc, setIframeSrc] = useState("");
  const [activeUid, setActiveUid] = useState<string>(DASHBOARDS[0].uid);

  useEffect(() => {
    const params = new URLSearchParams();
    params.set("orgId", "1");
    params.set("theme", "dark");
    params.set("kiosk", "");
    if (activeUid === "pg-overview") {
      params.set("var-host", "postgres-exporter:9187");
      params.set("var-db", "aistock");
    }
    setIframeSrc(
      `${GRAFANA_BASE}/d/${activeUid}/?${params.toString()}`
    );
  }, [activeUid]);

  const btnBase = {
    padding: "4px 12px",
    fontSize: 11,
    borderRadius: 6,
    border: "1px solid #475569",
    cursor: "pointer",
    textDecoration: "none",
  } as const;

  return (
    <main style={{ padding: 0, height: "100vh", display: "flex", flexDirection: "column" }}>
      <div
        style={{
          background: "linear-gradient(135deg, #0f172a 0%, #1e293b 100%)",
          padding: "12px 24px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: "1px solid #334155",
          flexShrink: 0,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: "#3b82f6",
              boxShadow: "0 0 8px #3b82f6",
            }}
          />
          <h1 style={{ margin: 0, fontSize: 16, color: "#f1f5f9", fontWeight: 600 }}>
            数据库监控
          </h1>
          <span style={{ fontSize: 12, color: "#64748b" }}>
            TimescaleDB/PG 16 · aistock
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {DASHBOARDS.map((d) => (
            <button
              key={d.uid}
              onClick={() => setActiveUid(d.uid)}
              style={{
                ...btnBase,
                background: activeUid === d.uid ? "#6366f1" : "#334155",
                color: activeUid === d.uid ? "#fff" : "#94a3b8",
                borderColor: activeUid === d.uid ? "#6366f1" : "#475569",
              }}
            >
              {d.label}
            </button>
          ))}
          <a
            href={`${GRAFANA_BASE}/d/${activeUid}`}
            target="_blank"
            rel="noreferrer"
            style={{
              ...btnBase,
              background: "#334155",
              color: "#94a3b8",
            }}
          >
            在 Grafana 中打开
          </a>
        </div>
      </div>

      {iframeSrc && (
        <iframe
          src={iframeSrc}
          style={{
            flex: 1,
            width: "100%",
            border: "none",
            background: "#0f172a",
          }}
          title="Database Monitor - Grafana"
        />
      )}
    </main>
  );
}
