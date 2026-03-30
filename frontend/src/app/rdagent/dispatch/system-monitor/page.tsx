"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const GRAFANA_BASE = "http://localhost:3001";
const DASHBOARD_UID = "node-exporter-full";

interface NodeOption {
  node_id: string;
  display_name: string;
  prometheus_target: string | null;
}

export default function SystemMonitorPage() {
  const [nodes, setNodes] = useState<NodeOption[]>([]);
  const [selectedIdx, setSelectedIdx] = useState<string>("");
  const [iframeSrc, setIframeSrc] = useState("");

  useEffect(() => {
    fetch(`${API_BASE}/dispatch/nodes`)
      .then((r) => r.json())
      .then((data) => {
        const list = (data || []).map((n: any) => ({
          node_id: n.node_id,
          display_name: n.display_name || n.node_id,
          prometheus_target: n.prometheus_target || null,
        }));
        setNodes(list);
      })
      .catch((e) => console.warn("加载节点失败:", e));
  }, []);

  useEffect(() => {
    const params = new URLSearchParams();
    params.set("orgId", "1");
    params.set("theme", "dark");
    params.set("kiosk", "");
    // 仪表盘变量: var-job → var-nodename → var-node (instance addr)
    params.set("var-job", "node_exporter");
    if (selectedIdx && nodes.length > 0) {
      const node = nodes.find((n) => n.node_id === selectedIdx);
      if (node?.prometheus_target) {
        // var-node 接受 Prometheus instance 地址 (如 node-exporter:9100)
        params.set("var-node", node.prometheus_target);
      }
    }
    setIframeSrc(
      `${GRAFANA_BASE}/d/${DASHBOARD_UID}/node-exporter-full?${params.toString()}`
    );
  }, [selectedIdx, nodes]);

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
              background: "#22c55e",
              boxShadow: "0 0 8px #22c55e",
            }}
          />
          <h1 style={{ margin: 0, fontSize: 16, color: "#f1f5f9", fontWeight: 600 }}>
            系统监控
          </h1>
          <span style={{ fontSize: 12, color: "#64748b" }}>
            Grafana + Prometheus + Node Exporter
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <label style={{ fontSize: 12, color: "#94a3b8" }}>
            计算节点:
            <select
              value={selectedIdx}
              onChange={(e) => setSelectedIdx(e.target.value)}
              style={{
                marginLeft: 6,
                padding: "4px 8px",
                fontSize: 12,
                borderRadius: 6,
                border: "1px solid #475569",
                background: "#1e293b",
                color: "#e2e8f0",
              }}
            >
              <option value="">全部节点</option>
              {nodes.map((n) => (
                <option key={n.node_id} value={n.node_id}>
                  {n.display_name}
                </option>
              ))}
            </select>
          </label>
          <a
            href={`${GRAFANA_BASE}/d/${DASHBOARD_UID}`}
            target="_blank"
            rel="noreferrer"
            style={{
              padding: "4px 10px",
              fontSize: 11,
              borderRadius: 6,
              border: "1px solid #475569",
              background: "#334155",
              color: "#94a3b8",
              textDecoration: "none",
              cursor: "pointer",
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
          title="System Monitor - Grafana"
        />
      )}
    </main>
  );
}
