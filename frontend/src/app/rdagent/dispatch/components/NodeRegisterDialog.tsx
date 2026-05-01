"use client";

import React, { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export default function NodeRegisterDialog({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: () => void;
}) {
  const [form, setForm] = useState({
    node_id: "",
    display_name: "",
    api_base_url: "http://",
    gpu_model: "",
    gpu_vram_mb: "",
    grafana_dashboard_url: "",
    prometheus_target: "",
    ssh_user: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async () => {
    if (!form.node_id || !form.display_name || !form.api_base_url) {
      setError("node_id, 显示名称, API 地址为必填");
      return;
    }
    setSubmitting(true);
    setError("");
    try {
      const res = await fetch(`${API_BASE}/dispatch/nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          gpu_vram_mb: form.gpu_vram_mb ? parseInt(form.gpu_vram_mb) : null,
          grafana_dashboard_url: form.grafana_dashboard_url || null,
          prometheus_target: form.prometheus_target || null,
          ssh_user: form.ssh_user || null,
          capabilities: ["fin_factor", "fin_model", "fin_quant", "fin_factor_report", "qe_evolution"],
        }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || `HTTP ${res.status}`);
      }
      onCreated();
    } catch (e: any) {
      setError(e.message || "注册失败");
    }
    setSubmitting(false);
  };

  const fieldStyle = {
    width: "100%", padding: "8px 12px", fontSize: 13, borderRadius: 6,
    border: "1px solid #d1d5db", outline: "none", boxSizing: "border-box" as const,
  };

  return (
    <div style={{
      position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
      background: "rgba(0,0,0,0.5)", display: "flex",
      alignItems: "center", justifyContent: "center", zIndex: 1100,
    }}>
      <div style={{
        background: "#fff", borderRadius: 12, padding: 24,
        maxWidth: 480, width: "92%",
        boxShadow: "0 20px 25px -5px rgba(0,0,0,0.15)",
      }}>
        <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 600 }}>注册新计算节点</h3>

        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>Node ID</label>
            <input
              style={fieldStyle}
              placeholder="如: my-gpu-server"
              value={form.node_id}
              onChange={e => setForm(f => ({ ...f, node_id: e.target.value }))}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>显示名称</label>
            <input
              style={fieldStyle}
              placeholder="如: GPU 服务器 (RTX 4090)"
              value={form.display_name}
              onChange={e => setForm(f => ({ ...f, display_name: e.target.value }))}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>API 地址</label>
            <input
              style={fieldStyle}
              placeholder="http://192.168.x.x:9000"
              value={form.api_base_url}
              onChange={e => setForm(f => ({ ...f, api_base_url: e.target.value }))}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>SSH User <span style={{ color: "#9ca3af" }}>(optional)</span></label>
            <input
              style={fieldStyle}
              placeholder="lc999"
              value={form.ssh_user}
              onChange={e => setForm(f => ({ ...f, ssh_user: e.target.value }))}
            />
          </div>
          <div style={{ display: "flex", gap: 12 }}>
            <div style={{ flex: 1 }}>
              <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>GPU 型号</label>
              <input
                style={fieldStyle}
                placeholder="RTX 4090"
                value={form.gpu_model}
                onChange={e => setForm(f => ({ ...f, gpu_model: e.target.value }))}
              />
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>显存 (MB)</label>
              <input
                style={fieldStyle}
                type="number"
                placeholder="24576"
                value={form.gpu_vram_mb}
                onChange={e => setForm(f => ({ ...f, gpu_vram_mb: e.target.value }))}
              />
            </div>
          </div>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>Grafana 仪表盘 URL <span style={{ color: "#9ca3af" }}>(可选)</span></label>
            <input
              style={fieldStyle}
              placeholder="http://localhost:3001/d/node-exporter-full"
              value={form.grafana_dashboard_url}
              onChange={e => setForm(f => ({ ...f, grafana_dashboard_url: e.target.value }))}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, display: "block" }}>Prometheus Target <span style={{ color: "#9ca3af" }}>(可选)</span></label>
            <input
              style={fieldStyle}
              placeholder="192.168.x.x:9100"
              value={form.prometheus_target}
              onChange={e => setForm(f => ({ ...f, prometheus_target: e.target.value }))}
            />
          </div>
        </div>

        {error && (
          <div style={{ marginTop: 12, color: "#ef4444", fontSize: 13 }}>{error}</div>
        )}

        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 20 }}>
          <button
            onClick={onClose}
            style={{
              padding: "8px 16px", fontSize: 13, borderRadius: 6,
              border: "1px solid #d1d5db", background: "#fff", cursor: "pointer", color: "#374151",
            }}
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            style={{
              padding: "8px 16px", fontSize: 13, borderRadius: 6,
              border: "none", background: "#6366f1", cursor: "pointer", color: "#fff",
              opacity: submitting ? 0.6 : 1,
            }}
          >
            {submitting ? "注册中..." : "注册"}
          </button>
        </div>
      </div>
    </div>
  );
}
