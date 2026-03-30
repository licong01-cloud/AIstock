"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type SotaRow = {
  // Leaderboard API fields
  loop_id: string;
  task_id?: string;
  task_name?: string;
  loop_index?: number;
  action_type?: string;
  is_sota?: boolean;
  status?: string;
  evaluation_reason?: string;
  created_at?: string;
  // Metrics - either from top-level (leaderboard) or metrics_json (legacy)
  ic?: number | null;
  sharpe?: number | null;
  metrics_json?: Record<string, any>;
  config_json?: Record<string, unknown>;
  // Legacy SOTA API fields
  sota_id?: number;
  model_assets_synced?: boolean;
  local_asset_path?: string | null;
};

type LeaderboardData = {
  summary: { total_tasks: number; total_sota_loops: number; best_ic: number | null; best_sharpe: number | null };
  leaderboard: SotaRow[];
};

/** Helper: extract metric value from row (check top-level then metrics_json) */
function getMetric(row: SotaRow, key: string, fallbackKey?: string): number | null {
  // Top-level (leaderboard API extracts ic/sharpe)
  const topVal = (row as any)[key];
  if (typeof topVal === "number" && isFinite(topVal)) return topVal;
  // From metrics_json
  const m = row.metrics_json;
  if (!m) return null;
  const v = m[key] ?? (fallbackKey ? m[fallbackKey] : undefined);
  return typeof v === "number" && isFinite(v) ? v : null;
}

export default function EvolutionSotaPage() {
  const [items, setItems] = useState<SotaRow[]>([]);
  const [leaderboardSummary, setLeaderboardSummary] = useState<LeaderboardData["summary"] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedRow, setExpandedRow] = useState<string | null>(null); // use loop_id as key

  const fetchSota = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // Try leaderboard API first (enhanced), fallback to legacy
      const lbRes = await fetch(`${API}/quantevolver/evolution/leaderboard`).catch(() => null);
      const lbData = lbRes ? await lbRes.json().catch(() => null) : null;
      if (lbData?.status === "success" && lbData?.data) {
        setLeaderboardSummary(lbData.data.summary || null);
        if (Array.isArray(lbData.data.leaderboard) && lbData.data.leaderboard.length > 0) {
          setItems(lbData.data.leaderboard);
          setLoading(false);
          return;
        }
      }
      // Fallback to legacy SOTA endpoint
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
    const allIc = items.map(r => getMetric(r, "ic", "IC")).filter((v): v is number => v != null);
    const allSharpe = items.map(r => getMetric(r, "sharpe", "Sharpe")).filter((v): v is number => v != null);
    const bestIc = leaderboardSummary?.best_ic ?? (allIc.length > 0 ? Math.max(...allIc) : null);
    const bestSharpe = leaderboardSummary?.best_sharpe ?? (allSharpe.length > 0 ? Math.max(...allSharpe) : null);
    return {
      total: leaderboardSummary?.total_tasks ?? new Set(items.map(r => r.task_id || r.task_name || "").filter(Boolean)).size,
      totalLoops: leaderboardSummary?.total_sota_loops ?? items.length,
      synced,
      bestIc: bestIc != null && isFinite(bestIc) ? bestIc : null,
      bestSharpe: bestSharpe != null && isFinite(bestSharpe) ? bestSharpe : null,
    };
  }, [items, leaderboardSummary]);

  const rowKey = (row: SotaRow) => row.loop_id || String(row.sota_id || Math.random());

  return (
    <div style={{ padding: "24px", display: "flex", flexDirection: "column", gap: "16px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1 style={{ margin: 0, fontSize: "22px" }}>SOTA 殿堂</h1>
        <button onClick={fetchSota} style={{ padding: "8px 14px", borderRadius: "8px", border: "1px solid #cbd5e1", background: "#fff", cursor: "pointer" }}>
          刷新
        </button>
      </div>

      {/* Summary stat cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: "12px" }}>
        {[
          { label: "SOTA 总数", value: items.length, color: "#3b82f6" },
          { label: "已同步", value: summary.synced, color: "#10b981" },
          { label: "涉及任务", value: summary.total, color: "#8b5cf6" },
          { label: "最佳 IC", value: summary.bestIc != null ? summary.bestIc.toFixed(4) : "-", color: "#f59e0b" },
          { label: "最佳 Sharpe", value: summary.bestSharpe != null ? summary.bestSharpe.toFixed(2) : "-", color: "#ec4899" },
        ].map((s) => (
          <div key={s.label} style={{
            padding: "14px 16px", backgroundColor: "#fff", borderRadius: "10px",
            border: "1px solid #e2e8f0", textAlign: "center",
          }}>
            <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>{s.label}</div>
            <div style={{ fontSize: "22px", fontWeight: 700, color: s.color, fontFamily: "monospace", marginTop: "4px" }}>{s.value}</div>
          </div>
        ))}
      </div>

      {loading && <div>加载中...</div>}
      {error && <div style={{ color: "#dc2626" }}>加载失败：{error}</div>}

      {!loading && !error && (
        <div style={{ overflowX: "auto", border: "1px solid #e2e8f0", borderRadius: "10px", background: "#fff" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
            <thead>
              <tr style={{ background: "#f8fafc", textAlign: "left" }}>
                <th style={{ padding: "10px", width: "28px" }}></th>
                <th style={{ padding: "10px" }}>Loop</th>
                <th style={{ padding: "10px" }}>任务</th>
                <th style={{ padding: "10px" }}>IC</th>
                <th style={{ padding: "10px" }}>ICIR</th>
                <th style={{ padding: "10px" }}>Rank IC</th>
                <th style={{ padding: "10px" }}>Sharpe</th>
                <th style={{ padding: "10px" }}>换手率</th>
                <th style={{ padding: "10px" }}>同步</th>
                <th style={{ padding: "10px" }}>评估理由</th>
              </tr>
            </thead>
            <tbody>
              {items.map((row) => {
                const key = rowKey(row);
                const ic = getMetric(row, "ic", "IC");
                const icir = getMetric(row, "icir", "ICIR");
                const rankIc = getMetric(row, "rank_ic", "Rank_IC");
                const sharpe = getMetric(row, "sharpe", "Sharpe");
                const turnover = getMetric(row, "avg_turnover");
                return (
                  <React.Fragment key={key}>
                    <tr style={{ borderTop: "1px solid #f1f5f9", cursor: "pointer" }}
                      onClick={() => setExpandedRow(expandedRow === key ? null : key)}>
                      <td style={{ padding: "10px", fontSize: "10px", color: "#94a3b8" }}>
                        {expandedRow === key ? "▼" : "▶"}
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>
                        {row.loop_index != null ? `Loop ${row.loop_index}` : row.loop_id}
                      </td>
                      <td style={{ padding: "10px" }}>{row.task_name || "-"}</td>
                      <td style={{ padding: "10px", fontFamily: "monospace", fontWeight: 600 }}>
                        {ic != null ? ic.toFixed(4) : "-"}
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>
                        {icir != null ? icir.toFixed(4) : "-"}
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>
                        {rankIc != null ? rankIc.toFixed(4) : "-"}
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>
                        {sharpe != null ? sharpe.toFixed(2) : "-"}
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>
                        {turnover != null ? turnover.toFixed(3) : "-"}
                      </td>
                      <td style={{ padding: "10px", color: row.model_assets_synced ? "#059669" : "#d97706", fontSize: "12px" }}>
                        {row.model_assets_synced ? "✓" : "—"}
                      </td>
                      <td style={{ padding: "10px", color: "#475569", maxWidth: "200px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                        title={row.evaluation_reason || ""}>
                        {row.evaluation_reason || "-"}
                      </td>
                    </tr>
                    {expandedRow === key && (
                      <tr>
                        <td colSpan={10} style={{ padding: "12px 20px", backgroundColor: "#f8fafc", borderTop: "1px solid #e2e8f0" }}>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
                            <div>
                              <div style={{ fontSize: "11px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", marginBottom: "8px" }}>详细指标</div>
                              <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                                {row.metrics_json && Object.entries(row.metrics_json).map(([k, v]) => (
                                  <span key={k} style={{ fontSize: "11px", padding: "3px 8px", backgroundColor: "#fff", borderRadius: "4px", border: "1px solid #e2e8f0", fontFamily: "monospace" }}>
                                    {k}: {typeof v === "number" ? v.toFixed(4) : String(v)}
                                  </span>
                                ))}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: "11px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", marginBottom: "8px" }}>
                                {row.action_type ? `策略类型: ${row.action_type}` : "资产路径"}
                              </div>
                              <div style={{ fontSize: "12px", fontFamily: "monospace", color: "#475569", wordBreak: "break-all" }}>
                                {row.local_asset_path || row.action_type || "未同步"}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
              {items.length === 0 && (
                <tr>
                  <td colSpan={10} style={{ padding: "20px", textAlign: "center", color: "#94a3b8" }}>
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
