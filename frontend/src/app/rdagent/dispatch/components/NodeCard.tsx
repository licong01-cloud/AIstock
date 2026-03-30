"use client";

import React, { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

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
};

const STATUS_COLORS: Record<string, { bg: string; dot: string; text: string }> = {
  online: { bg: "#dcfce7", dot: "#22c55e", text: "#15803d" },
  busy: { bg: "#fef9c3", dot: "#eab308", text: "#854d0e" },
  offline: { bg: "#fee2e2", dot: "#ef4444", text: "#991b1b" },
  unknown: { bg: "#f3f4f6", dot: "#9ca3af", text: "#6b7280" },
};

export default function NodeCard({ node, onRefresh }: { node: Node; onRefresh: () => void }) {
  const [probing, setProbing] = useState(false);
  const [showMenu, setShowMenu] = useState(false);

  const sc = STATUS_COLORS[node.status] || STATUS_COLORS.unknown;
  const metrics = node.metrics_snapshot || {};

  const [probeError, setProbeError] = useState<string | null>(null);

  const handleProbe = async () => {
    setProbing(true);
    setProbeError(null);
    try {
      const res = await fetch(`${API_BASE}/dispatch/nodes/${node.node_id}/probe`, { method: "POST" });
      if (!res.ok) {
        const d = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
        setProbeError(d.detail || d.error || `探测失败: ${res.status}`);
      } else {
        onRefresh();
      }
    } catch (e: any) {
      setProbeError(`网络错误: ${e.message}`);
    }
    setProbing(false);
  };

  const configLinks = [
    { label: "LLM 模型配置", path: `/config/rdagent-llm?node_id=${node.node_id}` },
    { label: "模板管理", path: `/scheduler?node_id=${node.node_id}` },
    { label: "环境变量", path: `/config/rdagent-env?node_id=${node.node_id}` },
    { label: "数据集管理", path: `/config/rdagent-datasets?node_id=${node.node_id}` },
  ];

  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: 16,
      border: "1px solid #e5e7eb", boxShadow: "0 1px 3px rgba(0,0,0,0.06)",
    }}>
      {/* 标题行 */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        <span style={{
          width: 8, height: 8, borderRadius: "50%", background: sc.dot,
          display: "inline-block", flexShrink: 0,
        }} />
        <span style={{ fontWeight: 600, fontSize: 14, color: "#1f2937", flex: 1 }}>
          {node.display_name}
        </span>
        <span style={{
          fontSize: 11, padding: "2px 8px", borderRadius: 10,
          background: sc.bg, color: sc.text, fontWeight: 500,
        }}>
          {node.status}
        </span>
      </div>

      {/* GPU 信息 */}
      {node.gpu_model && (
        <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 6 }}>
          {node.gpu_model}
          {node.gpu_vram_mb ? ` (${(node.gpu_vram_mb / 1024).toFixed(0)}GB)` : ""}
        </div>
      )}

      {/* 资源指标 */}
      {(metrics.gpu_util_pct !== undefined || metrics.cpu_util_pct !== undefined) && (
        <div style={{ display: "flex", gap: 8, marginBottom: 8, flexWrap: "wrap" }}>
          {metrics.gpu_util_pct !== undefined && (
            <MetricBadge label="GPU" value={`${metrics.gpu_util_pct}%`} />
          )}
          {metrics.cpu_util_pct !== undefined && (
            <MetricBadge label="CPU" value={`${metrics.cpu_util_pct}%`} />
          )}
          {metrics.ram_used_pct !== undefined && (
            <MetricBadge label="RAM" value={`${metrics.ram_used_pct}%`} />
          )}
        </div>
      )}

      {/* 当前任务 */}
      {node.current_task_id && (
        <div style={{
          fontSize: 12, color: "#374151", background: "#f3f4f6",
          borderRadius: 6, padding: "4px 8px", marginBottom: 8,
        }}>
          当前: {node.current_task_id.slice(0, 12)}...
        </div>
      )}

      {!node.current_task_id && node.status === "online" && (
        <div style={{ fontSize: 12, color: "#22c55e", marginBottom: 8 }}>
          空闲
        </div>
      )}

      {/* 心跳时间 */}
      {node.last_heartbeat && (
        <div style={{ fontSize: 11, color: "#9ca3af", marginBottom: 8 }}>
          心跳: {new Date(node.last_heartbeat).toLocaleTimeString()}
        </div>
      )}

      {/* 探测错误提示 */}
      {probeError && (
        <div style={{
          fontSize: 11, color: "#991b1b", background: "#fee2e2",
          borderRadius: 4, padding: "4px 8px", marginBottom: 8,
        }}>
          {probeError}
        </div>
      )}

      {/* 操作按钮 */}
      <div style={{ display: "flex", gap: 8, position: "relative" }}>
        <button
          onClick={handleProbe}
          disabled={probing}
          style={{
            flex: 1, padding: "5px 0", fontSize: 12, borderRadius: 6,
            border: "1px solid #d1d5db", background: "#fff", cursor: "pointer",
            color: "#374151", opacity: probing ? 0.6 : 1,
          }}
        >
          {probing ? "探测中..." : "探测健康"}
        </button>
        <div style={{ position: "relative" }}>
          <button
            onClick={() => setShowMenu(!showMenu)}
            style={{
              padding: "5px 10px", fontSize: 12, borderRadius: 6,
              border: "1px solid #d1d5db", background: "#fff", cursor: "pointer",
              color: "#374151",
            }}
          >
            配置
          </button>
          {showMenu && (
            <div style={{
              position: "absolute", top: "100%", right: 0, marginTop: 4,
              background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8,
              boxShadow: "0 4px 12px rgba(0,0,0,0.1)", zIndex: 100,
              minWidth: 160, padding: 4,
            }}>
              {configLinks.map(l => (
                <a
                  key={l.path}
                  href={l.path}
                  style={{
                    display: "block", padding: "6px 12px", fontSize: 12,
                    color: "#374151", textDecoration: "none", borderRadius: 4,
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
                  onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                >
                  {l.label}
                </a>
              ))}
            </div>
          )}
        </div>
        {node.grafana_dashboard_url && (
          <button
            onClick={() => window.open(`${node.grafana_dashboard_url}?var-node_id=${node.node_id}&kiosk`, "_blank")}
            style={{
              padding: "5px 10px", fontSize: 12, borderRadius: 6,
              border: "1px solid #6366f1", background: "#eef2ff", cursor: "pointer",
              color: "#4338ca",
            }}
          >
            监控
          </button>
        )}
      </div>
    </div>
  );
}

function MetricBadge({ label, value }: { label: string; value: string }) {
  return (
    <span style={{
      fontSize: 11, padding: "2px 6px", borderRadius: 4,
      background: "#f3f4f6", color: "#374151",
    }}>
      {label}: <strong>{value}</strong>
    </span>
  );
}
