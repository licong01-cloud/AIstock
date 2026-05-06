"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type SelectionMode = "intersection" | "union" | "weighted_fusion";

type MetricsSummary = {
  ic?: number | null;
  rank_ic?: number | null;
  icir?: number | null;
  sharpe?: number | null;
  annual_return?: number | null;
  max_drawdown?: number | null;
  final_nav?: number | null;
  turnover?: number | null;
  n_trading_days?: number | null;
  sample_start?: string | null;
  sample_end?: string | null;
  missing_metrics?: string[];
};

type LatestSelectionRun = {
  run_id: string;
  mode: string;
  trade_date: string;
  data_source: string;
  status: string;
  candidate_count: number;
  completed_at?: string | null;
};

type SelectablePackage = {
  package_id: string;
  package_name: string;
  package_version?: string;
  package_status: string;
  source_type?: string;
  source_id?: string;
  manifest_sha256: string;
  alpha_mode?: string;
  alpha_count?: number;
  portfolio_topk?: number;
  metrics_summary?: MetricsSummary;
  model_state?: Record<string, unknown>;
  latest_selection_run?: LatestSelectionRun | null;
};

type SelectionCandidate = {
  symbol: string;
  score: number;
  rank: number;
  target_weight?: number | null;
  reference_price?: number | null;
  component_scores?: Record<string, unknown>;
  reason?: string | null;
};

type SelectionRun = {
  run_id: string;
  mode: string;
  trade_date: string;
  data_source: string;
  package_ids: string[];
  runtime_config: Record<string, unknown>;
  aggregate_results: SelectionCandidate[];
  manifest_sha256_by_package: Record<string, string>;
};

function formatNumber(value: number | null | undefined, digits = 4) {
  return value == null || Number.isNaN(value) ? "-" : value.toFixed(digits);
}

function formatPercent(value: number | null | undefined) {
  return value == null || Number.isNaN(value) ? "-" : `${(value * 100).toFixed(2)}%`;
}

function errorMessage(payload: unknown, fallback: string) {
  if (!payload || typeof payload !== "object") return fallback;
  const obj = payload as Record<string, unknown>;
  const detail = obj.detail;
  if (detail && typeof detail === "object") {
    const d = detail as Record<string, unknown>;
    return [d.error_code, d.message].filter(Boolean).join(": ") || fallback;
  }
  if (typeof detail === "string") return detail;
  if (typeof obj.error === "string") return obj.error;
  return fallback;
}

export default function PackageSelectionPage() {
  const [packages, setPackages] = useState<SelectablePackage[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [mode, setMode] = useState<SelectionMode>("weighted_fusion");
  const [tradeDate, setTradeDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [dataSource, setDataSource] = useState("DB_HISTORICAL");
  const [topK, setTopK] = useState(50);
  const [useExistingRuns, setUseExistingRuns] = useState(false);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [run, setRun] = useState<SelectionRun | null>(null);

  const selectedPackages = useMemo(
    () => packages.filter((item) => selected[item.package_id]),
    [packages, selected],
  );

  const loadPackages = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API}/selection-center/selectable-packages?limit=300`);
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(errorMessage(payload, `load failed: ${response.status}`));
      }
      const items: SelectablePackage[] = payload.packages || [];
      setPackages(items);
      setSelected((prev) => {
        const next: Record<string, boolean> = {};
        for (const item of items) next[item.package_id] = Boolean(prev[item.package_id]);
        return next;
      });
      setWeights((prev) => {
        const next: Record<string, number> = {};
        for (const item of items) next[item.package_id] = prev[item.package_id] ?? 1;
        return next;
      });
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPackages();
  }, [loadPackages]);

  const runSelection = useCallback(async () => {
    if (selectedPackages.length < 2) {
      setError("Select at least two StrategyPackages for aggregation.");
      return;
    }
    setRunning(true);
    setError(null);
    setRun(null);
    try {
      const packageIds = selectedPackages.map((item) => item.package_id);
      const packageWeights = Object.fromEntries(packageIds.map((id) => [id, weights[id] ?? 1]));
      const runtimeConfig: Record<string, unknown> = {
        top_k: topK,
        exclude_suspended: true,
      };
      if (mode === "weighted_fusion") {
        runtimeConfig.package_weights = packageWeights;
      }

      const endpoint = useExistingRuns ? "/selection-center/aggregate-runs" : "/selection-center/runs";
      const body = useExistingRuns
        ? {
            source_run_ids: selectedPackages.map((item) => item.latest_selection_run?.run_id),
            mode,
            runtime_config: runtimeConfig,
          }
        : {
            package_ids: packageIds,
            trade_date: tradeDate,
            data_source: dataSource,
            mode,
            runtime_config: runtimeConfig,
          };
      if (useExistingRuns && (body as { source_run_ids: Array<string | undefined> }).source_run_ids.some((id) => !id)) {
        throw new Error("Every selected package needs a latest successful single-package selection run.");
      }

      const response = await fetch(`${API}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(errorMessage(payload, `selection failed: ${response.status}`));
      }
      setRun(payload.run);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setRunning(false);
    }
  }, [dataSource, mode, selectedPackages, topK, tradeDate, useExistingRuns, weights]);

  const selectedCount = selectedPackages.length;
  const canRun = selectedCount >= 2 && !running;

  return (
    <main style={pageStyle}>
      <section style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>StrategyPackage Selection Center</div>
          <h1 style={titleStyle}>Dynamic multi-package stock selection</h1>
          <p style={subtitleStyle}>
            Pick validated StrategyPackages, inspect QE metrics, and aggregate candidates without freezing a new trading source.
          </p>
        </div>
        <button onClick={loadPackages} disabled={loading} style={refreshButtonStyle}>
          {loading ? "Loading..." : "Refresh packages"}
        </button>
      </section>

      {error && <div style={errorStyle}>{error}</div>}

      <section style={controlGridStyle}>
        <div style={panelStyle}>
          <h2 style={sectionTitleStyle}>Aggregation controls</h2>
          <div style={formGridStyle}>
            <label style={fieldStyle}>
              <span style={labelStyle}>Mode</span>
              <select value={mode} onChange={(event) => setMode(event.target.value as SelectionMode)} style={inputStyle}>
                <option value="weighted_fusion">Weighted rank fusion</option>
                <option value="intersection">Intersection</option>
                <option value="union">Union</option>
              </select>
            </label>
            <label style={fieldStyle}>
              <span style={labelStyle}>Trade date</span>
              <input value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} type="date" style={inputStyle} disabled={useExistingRuns} />
            </label>
            <label style={fieldStyle}>
              <span style={labelStyle}>Data source</span>
              <select value={dataSource} onChange={(event) => setDataSource(event.target.value)} style={inputStyle} disabled={useExistingRuns}>
                <option value="DB_HISTORICAL">DB_HISTORICAL</option>
                <option value="TDX_REALTIME">TDX_REALTIME</option>
              </select>
            </label>
            <label style={fieldStyle}>
              <span style={labelStyle}>Top K per package</span>
              <input value={topK} onChange={(event) => setTopK(Number(event.target.value))} type="number" min={1} max={500} style={inputStyle} />
            </label>
          </div>
          <label style={toggleStyle}>
            <input type="checkbox" checked={useExistingRuns} onChange={(event) => setUseExistingRuns(event.target.checked)} />
            Aggregate latest completed single-package runs instead of running packages now
          </label>
          <div style={runBarStyle}>
            <div>
              <strong>{selectedCount}</strong> packages selected
              {useExistingRuns && <span style={hintStyle}> / requires latest run for each selected package</span>}
            </div>
            <button onClick={runSelection} disabled={!canRun} style={{ ...runButtonStyle, opacity: canRun ? 1 : 0.45 }}>
              {running ? "Running..." : "Run aggregation"}
            </button>
          </div>
        </div>

        <div style={panelStyle}>
          <h2 style={sectionTitleStyle}>Selected weights</h2>
          {selectedPackages.length === 0 ? (
            <p style={emptyStyle}>Select packages to edit weights.</p>
          ) : (
            <div style={weightListStyle}>
              {selectedPackages.map((item) => (
                <label key={item.package_id} style={weightRowStyle}>
                  <span style={weightNameStyle}>{item.package_name}</span>
                  <input
                    type="number"
                    min={0.01}
                    step={0.1}
                    value={weights[item.package_id] ?? 1}
                    onChange={(event) => setWeights((prev) => ({ ...prev, [item.package_id]: Number(event.target.value) }))}
                    style={weightInputStyle}
                    disabled={mode !== "weighted_fusion"}
                  />
                </label>
              ))}
            </div>
          )}
        </div>
      </section>

      <section style={packageGridStyle}>
        {packages.map((item) => {
          const metrics = item.metrics_summary || {};
          const checked = Boolean(selected[item.package_id]);
          return (
            <article key={item.package_id} style={{ ...packageCardStyle, borderColor: checked ? "#0f766e" : "#d8e2dc" }}>
              <div style={packageTopStyle}>
                <label style={packageCheckStyle}>
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(event) => setSelected((prev) => ({ ...prev, [item.package_id]: event.target.checked }))}
                  />
                  <span>{item.package_name}</span>
                </label>
                <span style={statusStyle}>{item.package_status}</span>
              </div>
              <div style={packageMetaStyle}>
                <span>{item.package_id}</span>
                <span>{item.alpha_mode || "-"} / {item.alpha_count || 0} alpha</span>
                <span>TopK {item.portfolio_topk || "-"}</span>
              </div>
              <div style={metricGridStyle}>
                <Metric label="IC" value={formatNumber(metrics.ic)} />
                <Metric label="Rank IC" value={formatNumber(metrics.rank_ic)} />
                <Metric label="ICIR" value={formatNumber(metrics.icir)} />
                <Metric label="Sharpe" value={formatNumber(metrics.sharpe, 3)} missing={metrics.sharpe == null} />
                <Metric label="Annual" value={formatPercent(metrics.annual_return)} />
                <Metric label="Max DD" value={formatPercent(metrics.max_drawdown)} />
              </div>
              <div style={latestRunStyle}>
                Latest run: {item.latest_selection_run ? (
                  <span>{item.latest_selection_run.run_id} / {item.latest_selection_run.candidate_count} candidates</span>
                ) : (
                  <span style={missingStyle}>none</span>
                )}
              </div>
            </article>
          );
        })}
      </section>

      {run && (
        <section style={resultPanelStyle}>
          <div style={resultHeaderStyle}>
            <div>
              <h2 style={sectionTitleStyle}>Aggregate results</h2>
              <div style={packageMetaStyle}>
                <span>{run.run_id}</span>
                <span>{run.mode}</span>
                <span>{run.aggregate_results.length} candidates</span>
              </div>
            </div>
            <div style={paperBlockedStyle}>Multi-package Paper v2 execution is intentionally disabled.</div>
          </div>
          <div style={tableWrapStyle}>
            <table style={tableStyle}>
              <thead>
                <tr>
                  <th style={thStyle}>Rank</th>
                  <th style={thStyle}>Symbol</th>
                  <th style={thStyle}>Score</th>
                  <th style={thStyle}>Weight</th>
                  <th style={thStyle}>Source packages</th>
                  <th style={thStyle}>Reason</th>
                </tr>
              </thead>
              <tbody>
                {run.aggregate_results.map((item) => {
                  const sourcePackages = item.component_scores?.source_package_ids;
                  return (
                    <tr key={`${item.rank}-${item.symbol}`}>
                      <td style={tdStyle}>{item.rank}</td>
                      <td style={tdStrongStyle}>{item.symbol}</td>
                      <td style={tdStyle}>{formatNumber(item.score, 6)}</td>
                      <td style={tdStyle}>{item.target_weight ? formatPercent(item.target_weight) : "-"}</td>
                      <td style={tdStyle}>{Array.isArray(sourcePackages) ? sourcePackages.join(", ") : "-"}</td>
                      <td style={tdStyle}>{item.reason || "-"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </main>
  );
}

function Metric({ label, value, missing = false }: { label: string; value: string; missing?: boolean }) {
  return (
    <div style={{ ...metricStyle, opacity: missing ? 0.55 : 1 }}>
      <span style={metricValueStyle}>{value}</span>
      <span style={metricLabelStyle}>{label}</span>
    </div>
  );
}

const pageStyle: CSSProperties = {
  display: "grid",
  gap: 18,
  fontFamily: "Aptos, Candara, 'Segoe UI', sans-serif",
};

const heroStyle: CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  gap: 16,
  alignItems: "center",
  padding: 24,
  borderRadius: 24,
  color: "#10231f",
  background: "radial-gradient(circle at 20% 20%, #f5d06f 0, transparent 30%), linear-gradient(135deg, #d7f2e3 0%, #9bd8c7 52%, #f4efe2 100%)",
  boxShadow: "0 18px 45px rgba(15, 118, 110, 0.16)",
};

const eyebrowStyle: CSSProperties = { fontSize: 12, fontWeight: 800, letterSpacing: 1.8, textTransform: "uppercase" };
const titleStyle: CSSProperties = { margin: "6px 0", fontSize: 30, lineHeight: 1.1, fontWeight: 900 };
const subtitleStyle: CSSProperties = { margin: 0, maxWidth: 760, color: "#31544d", fontSize: 14 };

const refreshButtonStyle: CSSProperties = {
  border: "1px solid rgba(16,35,31,0.22)",
  background: "rgba(255,255,255,0.72)",
  borderRadius: 999,
  padding: "10px 16px",
  cursor: "pointer",
  fontWeight: 800,
};

const errorStyle: CSSProperties = { padding: 14, borderRadius: 14, background: "#fff1f2", color: "#be123c", border: "1px solid #fecdd3" };
const controlGridStyle: CSSProperties = { display: "grid", gridTemplateColumns: "minmax(0, 1.4fr) minmax(260px, 0.6fr)", gap: 16 };
const panelStyle: CSSProperties = { background: "#fffdfa", border: "1px solid #e6ded0", borderRadius: 20, padding: 18, boxShadow: "0 8px 24px rgba(40, 54, 44, 0.06)" };
const sectionTitleStyle: CSSProperties = { margin: "0 0 12px 0", fontSize: 18, fontWeight: 900, color: "#18332e" };
const formGridStyle: CSSProperties = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12 };
const fieldStyle: CSSProperties = { display: "grid", gap: 5 };
const labelStyle: CSSProperties = { fontSize: 12, fontWeight: 800, color: "#5b6d65" };
const inputStyle: CSSProperties = { border: "1px solid #cbd8d0", borderRadius: 12, padding: "9px 10px", background: "#ffffff", color: "#13211e" };
const toggleStyle: CSSProperties = { display: "flex", alignItems: "center", gap: 8, marginTop: 14, fontSize: 13, color: "#40544e" };
const runBarStyle: CSSProperties = { display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 16, gap: 10 };
const hintStyle: CSSProperties = { color: "#6f817b", fontSize: 12 };
const runButtonStyle: CSSProperties = { border: 0, borderRadius: 14, padding: "11px 18px", background: "#0f766e", color: "#fff", fontWeight: 900, cursor: "pointer" };
const emptyStyle: CSSProperties = { color: "#73827d", margin: 0 };
const weightListStyle: CSSProperties = { display: "grid", gap: 8 };
const weightRowStyle: CSSProperties = { display: "grid", gridTemplateColumns: "1fr 82px", gap: 8, alignItems: "center" };
const weightNameStyle: CSSProperties = { fontSize: 13, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" };
const weightInputStyle: CSSProperties = { ...inputStyle, padding: "7px 8px" };
const packageGridStyle: CSSProperties = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(310px, 1fr))", gap: 14 };
const packageCardStyle: CSSProperties = { background: "#ffffff", border: "2px solid #d8e2dc", borderRadius: 20, padding: 16, boxShadow: "0 8px 22px rgba(15, 34, 31, 0.06)" };
const packageTopStyle: CSSProperties = { display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" };
const packageCheckStyle: CSSProperties = { display: "flex", alignItems: "center", gap: 8, fontWeight: 900, color: "#12231f", minWidth: 0 };
const statusStyle: CSSProperties = { fontSize: 11, fontWeight: 900, color: "#0f766e", background: "#dcfce7", padding: "4px 8px", borderRadius: 999 };
const packageMetaStyle: CSSProperties = { display: "flex", gap: 10, flexWrap: "wrap", color: "#6c7b75", fontSize: 12, marginTop: 8 };
const metricGridStyle: CSSProperties = { display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8, marginTop: 12 };
const metricStyle: CSSProperties = { display: "grid", gap: 2, borderRadius: 12, background: "#f7faf6", border: "1px solid #e1e9df", padding: "8px 9px" };
const metricValueStyle: CSSProperties = { fontWeight: 900, color: "#17332e", fontSize: 14 };
const metricLabelStyle: CSSProperties = { color: "#6b7e77", fontSize: 11 };
const latestRunStyle: CSSProperties = { marginTop: 12, color: "#4d5f59", fontSize: 12 };
const missingStyle: CSSProperties = { color: "#b45309", fontWeight: 800 };
const resultPanelStyle: CSSProperties = { ...panelStyle, overflow: "hidden" };
const resultHeaderStyle: CSSProperties = { display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 12 };
const paperBlockedStyle: CSSProperties = { color: "#92400e", background: "#fffbeb", border: "1px solid #fde68a", borderRadius: 999, padding: "8px 12px", fontSize: 12, fontWeight: 800 };
const tableWrapStyle: CSSProperties = { overflowX: "auto" };
const tableStyle: CSSProperties = { width: "100%", borderCollapse: "collapse", minWidth: 760 };
const thStyle: CSSProperties = { textAlign: "left", padding: "10px 12px", background: "#153d35", color: "#f8fafc", fontSize: 12 };
const tdStyle: CSSProperties = { padding: "10px 12px", borderBottom: "1px solid #e7eee9", fontSize: 13, color: "#283a35" };
const tdStrongStyle: CSSProperties = { ...tdStyle, fontWeight: 900, fontFamily: "Consolas, monospace" };
