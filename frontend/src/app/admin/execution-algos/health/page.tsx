"use client";

import { useEffect, useState } from "react";

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

type AssetHealth = {
  config_key: string;
  configured_path: string;
  status: "ok" | "cached" | "missing";
  resolved_path: string | null;
  source: string | null;
  reason: string | null;
};

type AlgoHealth = {
  algo_code: string;
  algo_name: string;
  status: "ok" | "cached" | "missing";
  required_runtime_asset_keys: string[];
  assets: AssetHealth[];
};

type HealthResponse = {
  overall_status: "ok" | "cached" | "missing";
  status_counts: Record<string, number>;
  cache_root: string;
  generated_at: string;
  algos: AlgoHealth[];
};

const badgeColor: Record<string, string> = {
  ok: "#047857",
  cached: "#b45309",
  missing: "#b91c1c",
};

export default function ExecutionAlgoHealthPage() {
  const [data, setData] = useState<HealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch(`${API_BASE}/execution-algos/health`, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        setData(await response.json());
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  return (
    <main style={{ minHeight: "100vh", background: "#f7f2e8", color: "#17231f", padding: "32px" }}>
      <section style={{ maxWidth: 1180, margin: "0 auto" }}>
        <div style={{ marginBottom: 24 }}>
          <p style={{ margin: 0, letterSpacing: "0.16em", textTransform: "uppercase", color: "#6f5c3e" }}>
            Execution Algorithms
          </p>
          <h1 style={{ margin: "8px 0", fontSize: 44, lineHeight: 1.05 }}>Model Cache Health</h1>
          <p style={{ margin: 0, color: "#5f6b63" }}>
            Read-only observability for enabled execution algorithms and their default_config runtime assets.
          </p>
        </div>

        {loading && <div style={{ padding: 24, background: "#fffaf0", border: "1px solid #e7d7b3" }}>Loading health...</div>}
        {error && <div style={{ padding: 24, background: "#fff1f2", border: "1px solid #fecdd3" }}>Failed: {error}</div>}

        {data && (
          <>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))",
                gap: 14,
                marginBottom: 20,
              }}
            >
              <Metric label="Overall" value={data.overall_status} />
              <Metric label="OK" value={String(data.status_counts.ok || 0)} />
              <Metric label="Cached" value={String(data.status_counts.cached || 0)} />
              <Metric label="Missing" value={String(data.status_counts.missing || 0)} />
            </div>
            <div style={{ fontSize: 13, color: "#66736b", marginBottom: 20 }}>
              Cache root: <code>{data.cache_root}</code> | generated: {data.generated_at}
            </div>
            <div style={{ display: "grid", gap: 14 }}>
              {data.algos.map((algo) => (
                <article key={algo.algo_code} style={{ background: "#fffaf0", border: "1px solid #e7d7b3", borderRadius: 18, padding: 18 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "start" }}>
                    <div>
                      <h2 style={{ margin: 0, fontSize: 22 }}>{algo.algo_code}</h2>
                      <p style={{ margin: "4px 0 0", color: "#66736b" }}>{algo.algo_name}</p>
                    </div>
                    <StatusBadge status={algo.status} />
                  </div>
                  {algo.assets.length === 0 ? (
                    <p style={{ color: "#66736b" }}>No runtime model assets required.</p>
                  ) : (
                    <div style={{ display: "grid", gap: 10, marginTop: 16 }}>
                      {algo.assets.map((asset) => (
                        <div key={asset.config_key} style={{ background: "#fff", borderRadius: 12, padding: 12 }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                            <strong>{asset.config_key}</strong>
                            <StatusBadge status={asset.status} />
                          </div>
                          <PathLine label="configured" value={asset.configured_path} />
                          <PathLine label="resolved" value={asset.resolved_path || "-"} />
                          {asset.reason && <div style={{ color: "#b91c1c", marginTop: 6 }}>{asset.reason}</div>}
                        </div>
                      ))}
                    </div>
                  )}
                </article>
              ))}
            </div>
          </>
        )}
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ background: "#17231f", color: "#fffaf0", borderRadius: 18, padding: 18 }}>
      <div style={{ color: "#e7d7b3", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.12em" }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 800 }}>{value}</div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span style={{ color: "white", background: badgeColor[status] || "#475569", borderRadius: 999, padding: "4px 10px", fontWeight: 700 }}>
      {status}
    </span>
  );
}

function PathLine({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ marginTop: 6, fontSize: 12, color: "#66736b", overflowWrap: "anywhere" }}>
      {label}: <code>{value}</code>
    </div>
  );
}
