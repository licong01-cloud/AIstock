"use client";

import React, { useState } from "react";
import NodeCard from "./NodeCard";
import NodeRegisterDialog from "./NodeRegisterDialog";

type Node = {
  node_id: string;
  display_name: string;
  api_base_url: string;
  gpu_model: string | null;
  gpu_vram_mb: number | null;
  capabilities: string[];
  status: string;
  current_task_id: string | null;
  last_heartbeat: string | null;
  metrics_snapshot: Record<string, any> | null;
  grafana_dashboard_url: string | null;
  prometheus_target: string | null;
};

export default function NodeStatusPanel({
  nodes,
  onRefresh,
}: {
  nodes: Node[];
  onRefresh: () => void;
}) {
  const [showRegister, setShowRegister] = useState(false);

  return (
    <div>
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        marginBottom: 12,
      }}>
        <h2 style={{ fontSize: 16, fontWeight: 600, margin: 0, color: "#1f2937" }}>
          计算节点
        </h2>
        <button
          onClick={() => setShowRegister(true)}
          style={{
            padding: "4px 12px", fontSize: 13, borderRadius: 6,
            border: "1px solid #d1d5db", background: "#fff", cursor: "pointer",
            color: "#374151",
          }}
        >
          + 注册新节点
        </button>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {nodes.map(node => (
          <NodeCard key={node.node_id} node={node} onRefresh={onRefresh} />
        ))}
        {nodes.length === 0 && (
          <div style={{ padding: 24, textAlign: "center", color: "#9ca3af", fontSize: 14 }}>
            暂无注册节点
          </div>
        )}
      </div>

      {showRegister && (
        <NodeRegisterDialog
          onClose={() => setShowRegister(false)}
          onCreated={() => { setShowRegister(false); onRefresh(); }}
        />
      )}
    </div>
  );
}
