"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface SignalRow {
  trade_date: string;
  symbol: string;
  rank: number | null;
  score: number | null;
  target_weight: number | null;
  action: string | null;
}

interface SignalsOverview {
  strategy_id: string;
  strategy_version_id: string;
  date_min: string | null;
  date_max: string | null;
  symbol_count: number;
  row_count: number;
}

export default function RDagentSignalsPage() {
  const params = useParams<{ strategyId: string; versionId: string }>();
  const strategyVersionId = params.versionId;

  const [overview, setOverview] = useState<SignalsOverview | null>(null);
  const [tradeDate, setTradeDate] = useState<string | "">("");
  const [rows, setRows] = useState<SignalRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!strategyVersionId) return;
    loadOverview();
  }, [strategyVersionId]);

  async function loadOverview() {
    setError(null);
    try {
      const res = await fetch(
        `${API_BASE}/rdagent/signals/overview?strategy_version_id=${encodeURIComponent(
          strategyVersionId,
        )}`,
      );
      if (!res.ok) throw new Error(`加载概览失败: ${res.status}`);
      const data = await res.json();
      setOverview(data);
      if (data.date_max) {
        setTradeDate(data.date_max.slice(0, 10));
        await loadSignalsByDate(data.date_max.slice(0, 10));
      }
    } catch (e: any) {
      setError(e?.message || "加载概览失败");
    }
  }

  async function loadSignalsByDate(date: string) {
    if (!strategyVersionId) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(
        `${API_BASE}/rdagent/signals/by_date?strategy_version_id=${encodeURIComponent(
          strategyVersionId,
        )}&trade_date=${encodeURIComponent(date)}&k=200`,
      );
      if (!res.ok) throw new Error(`加载当日 signals 失败: ${res.status}`);
      const data = await res.json();
      setRows(data.rows || []);
    } catch (e: any) {
      setError(e?.message || "加载当日 signals 失败");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #f97316 0%, #ec4899 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent 信号可视化</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          strategy_version_id = {strategyVersionId}
        </p>
      </section>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee2e2",
            border: "1px solid #fecaca",
            borderRadius: 8,
            marginBottom: 16,
            color: "#b91c1c",
          }}
        >
          {error}
        </div>
      )}

      {overview && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 20,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
            marginBottom: 16,
          }}
        >
          <h2 style={{ marginTop: 0, marginBottom: 12 }}>概览</h2>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 16, fontSize: 13 }}>
            <div>
              <div style={{ color: "#6b7280" }}>日期范围</div>
              <div>
                {overview.date_min || "-"} ~ {overview.date_max || "-"}
              </div>
            </div>
            <div>
              <div style={{ color: "#6b7280" }}>标的数</div>
              <div>{overview.symbol_count}</div>
            </div>
            <div>
              <div style={{ color: "#6b7280" }}>总行数</div>
              <div>{overview.row_count}</div>
            </div>
          </div>
        </section>
      )}

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <h2 style={{ margin: 0 }}>按日期查看 TopK / 权重</h2>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input
              type="date"
              value={tradeDate}
              onChange={(e) => setTradeDate(e.target.value)}
              style={{
                padding: "6px 10px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
                fontSize: 13,
              }}
            />
            <button
              type="button"
              onClick={() => tradeDate && loadSignalsByDate(tradeDate)}
              style={{
                padding: "6px 12px",
                borderRadius: 6,
                border: "none",
                background: "#6366f1",
                color: "#fff",
                cursor: "pointer",
                fontSize: 13,
              }}
            >
              加载
            </button>
          </div>
        </div>

        {loading ? (
          <div>加载中...</div>
        ) : rows.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
            当前日期下没有可显示的信号。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>trade_date</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>symbol</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>rank</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>score</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>target_weight</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>action</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, idx) => (
                  <tr key={`${r.trade_date}-${r.symbol}-${idx}`} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={{ padding: 8 }}>{r.trade_date}</td>
                    <td style={{ padding: 8 }}>{r.symbol}</td>
                    <td style={{ padding: 8 }}>{r.rank ?? "-"}</td>
                    <td style={{ padding: 8 }}>{r.score ?? "-"}</td>
                    <td style={{ padding: 8 }}>{r.target_weight ?? "-"}</td>
                    <td style={{ padding: 8 }}>{r.action ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
