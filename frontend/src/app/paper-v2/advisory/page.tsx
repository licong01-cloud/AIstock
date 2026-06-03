"use client";

import { useEffect, useMemo, useState } from "react";
import { advisoryApi, type AdvisoryQualityReport } from "@/lib/api/advisory";
import {
  selectionCenterAdvisoryApi,
  type FusionDiagnostics,
  type FusionDiagnosticRow,
  type SelectionRunSummary,
} from "@/lib/api/selectionCenter";

const DEFAULT_QUALITY_RECORDS = `[
  {
    "code": "000001.SZ",
    "trade_date": "2026-06-03",
    "current_price": 10,
    "entry_band_json": { "max_buy_price": 10.2 },
    "action": "HOLD",
    "reason_code": "HOLD",
    "decision_input_json": { "rank": 1, "score_bucket": "top5" },
    "day_low": 9.9
  }
]`;

function short(value: unknown, maxLength = 12): string {
  const text = String(value ?? "-");
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function packageTrace(row: FusionDiagnosticRow): string {
  const ranks = row.package_ranks || {};
  const presence = row.package_presence || {};
  return Object.keys({ ...ranks, ...presence })
    .sort()
    .map((packageId) => `${short(packageId, 8)}:r${ranks[packageId] ?? "-"}:${presence[packageId] || "unknown"}`)
    .join(" | ");
}

function metricValue(report: AdvisoryQualityReport, key: string): string {
  const value = report.metrics[key];
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(4) : String(value ?? "-");
}

export default function PaperV2AdvisoryPage() {
  const [runs, setRuns] = useState<SelectionRunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [diagnostics, setDiagnostics] = useState<FusionDiagnostics | null>(null);
  const [qualityInput, setQualityInput] = useState(DEFAULT_QUALITY_RECORDS);
  const [qualityReport, setQualityReport] = useState<AdvisoryQualityReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    selectionCenterAdvisoryApi.listRuns(30)
      .then((rows) => {
        if (!alive) return;
        setRuns(rows);
        setSelectedRunId((current) => current || rows.find((row) => row.mode === "weighted_fusion")?.run_id || rows[0]?.run_id || "");
      })
      .catch((exc) => {
        if (alive) setError(exc instanceof Error ? exc.message : String(exc));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    let alive = true;
    setDiagnostics(null);
    if (!selectedRunId) return () => { alive = false; };
    setLoading(true);
    selectionCenterAdvisoryApi.fusionDiagnostics(selectedRunId)
      .then((payload) => {
        if (alive) setDiagnostics(payload);
      })
      .catch((exc) => {
        if (alive) setError(exc instanceof Error ? exc.message : String(exc));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => { alive = false; };
  }, [selectedRunId]);

  const rows = useMemo(() => diagnostics?.diagnostics || [], [diagnostics]);

  async function buildQualityReport() {
    setError(null);
    setQualityReport(null);
    try {
      const parsed = JSON.parse(qualityInput);
      if (!Array.isArray(parsed)) throw new Error("Quality report input must be a JSON array.");
      const report = await advisoryApi.qualityReport(parsed, 1);
      setQualityReport(report);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  return (
    <main className="pv2-main">
      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">Advisory Review</div>
            <h2>Multi-package fusion diagnostics</h2>
            <p className="pv2-muted">
              Shows the canonical fusion rank while preserving each StrategyPackage score, rank, support and missing-evidence state.
            </p>
          </div>
          <button className="pv2-button" onClick={() => window.location.reload()} type="button">Reload</button>
        </div>
        {error ? <div className="pv2-error-panel">{error}</div> : null}
        <div className="pv2-row-actions">
          <label className="pv2-field" style={{ minWidth: 360 }}>
            Selection run
            <select className="pv2-select" value={selectedRunId} onChange={(event) => setSelectedRunId(event.target.value)}>
              {runs.map((run) => (
                <option key={run.run_id} value={run.run_id}>
                  {run.trade_date} / {run.mode} / {short(run.run_id, 10)}
                </option>
              ))}
            </select>
          </label>
          <span className="pv2-chip">{loading ? "Loading" : `${rows.length} rows`}</span>
          <span className="pv2-chip">policy {short(diagnostics?.fusion_policy_sha256, 16)}</span>
        </div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Symbol</th>
                <th>Fusion Score</th>
                <th>Support</th>
                <th>Dispersion</th>
                <th>Package Evidence</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 50).map((row) => (
                <tr key={`${row.symbol}-${row.rank}`}>
                  <td>{row.rank}</td>
                  <td>{row.symbol}</td>
                  <td>{Number(row.fusion_score ?? row.score).toFixed(4)}</td>
                  <td>{row.support_count ?? "-"}</td>
                  <td>{row.rank_dispersion ?? "-"}</td>
                  <td className="pv2-mono">{packageTrace(row)}</td>
                </tr>
              ))}
              {!rows.length ? (
                <tr>
                  <td colSpan={6}>No fusion diagnostics yet. Run weighted_fusion in Selection Center first.</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">Quality Report</div>
            <h2>Post-decision diagnostics API</h2>
            <p className="pv2-muted">
              Build a diagnostics-only quality report from decision-time records. Future outcome fields are rejected as decision inputs.
            </p>
          </div>
          <button className="pv2-button" onClick={buildQualityReport} type="button">Build report</button>
        </div>
        <textarea
          className="pv2-textarea"
          rows={10}
          value={qualityInput}
          onChange={(event) => setQualityInput(event.target.value)}
        />
        {qualityReport ? (
          <div className="pv2-readable-panel" style={{ padding: 12 }}>
            <div className="pv2-grid pv2-grid-3">
              <div className="pv2-metric">
                <div className="pv2-metric-label">Samples</div>
                <div className="pv2-metric-value">{qualityReport.sample_count}</div>
              </div>
              <div className="pv2-metric">
                <div className="pv2-metric-label">Entry Zone Hit</div>
                <div className="pv2-metric-value">{metricValue(qualityReport, "entry_zone_hit_rate")}</div>
              </div>
              <div className="pv2-metric">
                <div className="pv2-metric-label">Fillable</div>
                <div className="pv2-metric-value">{metricValue(qualityReport, "entry_zone_fillable_rate")}</div>
              </div>
            </div>
            <div className="pv2-help" style={{ marginTop: 12 }}>
              {qualityReport.warnings.join(" | ")}
            </div>
          </div>
        ) : null}
      </section>
    </main>
  );
}
