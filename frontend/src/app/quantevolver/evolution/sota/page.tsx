"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001/api/v1";

type SotaRow = {
  sota_id: number;
  loop_id: string;
  evaluation_reason: string;
  model_assets_synced: boolean;
  local_asset_path: string | null;
  created_at: string;
  task_name?: string;
  metrics_json?: Record<string, number>;
  config_json?: Record<string, unknown>;
};

export default function EvolutionSotaPage() {
  const [items, setItems] = useState<SotaRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchSota = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/sota`);
      const data = await res.json();
      if (data?.status !== "success" || !Array.isArray(data?.data)) {
        throw new Error(data?.detail || "SOTA接口返回异常");
      }
      setItems(data.data);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSota();
  }, [fetchSota]);

  const summary = useMemo(() => {
    const synced = items.filter((x) => x.model_assets_synced).length;
    return { total: items.length, synced };
  }, [items]);

  return (
    <div style={{ padding: "24px", display: "flex", flexDirection: "column", gap: "16px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1 style={{ margin: 0, fontSize: "22px" }}>SOTA 殿堂</h1>
        <button onClick={fetchSota} style={{ padding: "8px 14px", borderRadius: "8px", border: "1px solid #cbd5e1", background: "#fff", cursor: "pointer" }}>
          刷新
        </button>
      </div>

      <div style={{ color: "#334155", fontSize: "14px" }}>
        总记录: <b>{summary.total}</b>，已同步资产: <b>{summary.synced}</b>
      </div>

      {loading && <div>加载中...</div>}
      {error && <div style={{ color: "#dc2626" }}>加载失败：{error}</div>}

      {!loading && !error && (
        <div style={{ overflowX: "auto", border: "1px solid #e2e8f0", borderRadius: "10px", background: "#fff" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
            <thead>
              <tr style={{ background: "#f8fafc", textAlign: "left" }}>
                <th style={{ padding: "10px" }}>Loop</th>
                <th style={{ padding: "10px" }}>任务</th>
                <th style={{ padding: "10px" }}>IC</th>
                <th style={{ padding: "10px" }}>ICIR</th>
                <th style={{ padding: "10px" }}>同步状态</th>
                <th style={{ padding: "10px" }}>本地路径</th>
                <th style={{ padding: "10px" }}>评估理由</th>
              </tr>
            </thead>
            <tbody>
              {items.map((row) => (
                <tr key={row.sota_id} style={{ borderTop: "1px solid #f1f5f9" }}>
                  <td style={{ padding: "10px", fontFamily: "monospace" }}>{row.loop_id}</td>
                  <td style={{ padding: "10px" }}>{row.task_name || "-"}</td>
                  <td style={{ padding: "10px" }}>{typeof row.metrics_json?.IC === "number" ? row.metrics_json.IC.toFixed(4) : "-"}</td>
                  <td style={{ padding: "10px" }}>{typeof row.metrics_json?.ICIR === "number" ? row.metrics_json.ICIR.toFixed(4) : "-"}</td>
                  <td style={{ padding: "10px", color: row.model_assets_synced ? "#059669" : "#d97706" }}>
                    {row.model_assets_synced ? "已同步" : "未同步"}
                  </td>
                  <td style={{ padding: "10px", maxWidth: "360px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={row.local_asset_path || ""}>
                    {row.local_asset_path || "-"}
                  </td>
                  <td style={{ padding: "10px", color: "#475569" }}>{row.evaluation_reason || "-"}</td>
                </tr>
              ))}
              {items.length === 0 && (
                <tr>
                  <td colSpan={7} style={{ padding: "20px", textAlign: "center", color: "#94a3b8" }}>
                    暂无 SOTA 记录
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
