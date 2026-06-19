"use client";

import type { CSSProperties } from "react";
import { useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import SectionCard from "@/components/paper-v2/SectionCard";
import { shortHash } from "@/lib/paper-v2/format";
import {
  type MatrixValue,
  type MultiAlphaOrthogonalityResult,
  multiAlphaApi,
} from "@/lib/multi-alpha/orthogonality";

function parseRunIds(text: string): string[] {
  const seen = new Set<string>();
  return text
    .split(/[\s,]+/)
    .map((item) => item.trim())
    .filter((item) => {
      if (!item || seen.has(item)) return false;
      seen.add(item);
      return true;
    });
}

function formatValue(value: MatrixValue): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return value.toFixed(3);
}

function cellColor(value: MatrixValue, mode: "corr" | "jaccard"): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "#f3f4f6";
  const magnitude = mode === "corr" ? Math.min(1, Math.abs(value)) : Math.min(1, Math.max(0, value));
  const alpha = 0.12 + magnitude * 0.48;
  if (mode === "corr" && value < 0) return `rgba(37, 99, 235, ${alpha})`;
  return `rgba(220, 38, 38, ${alpha})`;
}

function MatrixTable({ title, description, legs, matrix, mode }: {
  title: string;
  description: string;
  legs: string[];
  matrix: MatrixValue[][];
  mode: "corr" | "jaccard";
}) {
  return (
    <SectionCard title={title} eyebrow={description}>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0, minWidth: 640 }}>
          <thead>
            <tr>
              <th style={thStyle}>run</th>
              {legs.map((leg) => <th key={leg} style={thStyle}>{shortHash(leg)}</th>)}
            </tr>
          </thead>
          <tbody>
            {legs.map((leg, rowIndex) => (
              <tr key={leg}>
                <td style={{ ...tdStyle, fontWeight: 700, color: "#111827" }}>{shortHash(leg)}</td>
                {legs.map((col, colIndex) => {
                  const value = matrix[rowIndex]?.[colIndex] ?? null;
                  return (
                    <td
                      key={`${leg}-${col}`}
                      style={{
                        ...tdStyle,
                        background: cellColor(value, mode),
                        color: value === null ? "#9ca3af" : "#111827",
                        fontWeight: rowIndex === colIndex ? 800 : 650,
                      }}
                      title={`${leg} x ${col}`}
                    >
                      {formatValue(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </SectionCard>
  );
}

function ResultPanel({ result }: { result: MultiAlphaOrthogonalityResult }) {
  const offDiag = useMemo(() => {
    const values: number[] = [];
    result.pred_corr_matrix.forEach((row, i) => row.forEach((value, j) => {
      if (i !== j && value !== null && value !== undefined && Number.isFinite(value)) values.push(Math.abs(value));
    }));
    return values.length ? values.reduce((acc, item) => acc + item, 0) / values.length : null;
  }, [result.pred_corr_matrix]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
        <MetricCard label="Alpha 腿数" value={String(result.legs.length)} />
        <MetricCard label="公共交易日" value={String(result.n_common_dates)} hint={`${result.common_date_start || "-"} ~ ${result.common_date_end || "-"}`} />
        <MetricCard label="Top-K" value={String(result.k)} hint="按 score 降序取持仓集合" />
        <MetricCard label="平均 |Corr|" value={offDiag == null ? "-" : offDiag.toFixed(3)} hint="越低越正交" />
      </div>

      <SectionCard title="Alpha 腿元数据" eyebrow="来自 qe_archive 指针与 pred.pkl 解析；缺字段显示为空，不用默认值冒充。">
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={thStyle}>run_id</th>
                <th style={thStyle}>model</th>
                <th style={thStyle}>factor_set</th>
                <th style={thStyle}>dates</th>
                <th style={thStyle}>rows</th>
                <th style={thStyle}>instruments</th>
              </tr>
            </thead>
            <tbody>
              {result.legs.map((runId) => {
                const meta = result.per_leg[runId] || { run_id: runId };
                return (
                  <tr key={runId}>
                    <td style={tdStyle}><code>{runId}</code></td>
                    <td style={tdStyle}>{meta.model || "-"}</td>
                    <td style={tdStyle}>{meta.factor_set || "-"}</td>
                    <td style={tdStyle}>{meta.n_dates ?? "-"}</td>
                    <td style={tdStyle}>{meta.row_count ?? "-"}</td>
                    <td style={tdStyle}>{meta.instrument_count ?? "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </SectionCard>

      <MatrixTable
        title="预测值 Spearman 相关矩阵"
        description="逐公共交易日在公共股票池上算 score Spearman，跨日平均；越低越正交。"
        legs={result.legs}
        matrix={result.pred_corr_matrix}
        mode="corr"
      />
      <MatrixTable
        title="Top-K 持仓 Jaccard 矩阵"
        description="逐日取每腿 score Top-K 集合算交并比，跨日平均；越低表示持仓重叠越少。"
        legs={result.legs}
        matrix={result.jaccard_matrix}
        mode="jaccard"
      />
    </div>
  );
}

const thStyle: CSSProperties = {
  padding: "10px 12px",
  textAlign: "left",
  fontSize: 12,
  color: "#6b7280",
  borderBottom: "1px solid #e5e7eb",
  background: "#f9fafb",
};

const tdStyle: CSSProperties = {
  padding: "10px 12px",
  fontSize: 12,
  borderBottom: "1px solid #eef2f7",
  verticalAlign: "top",
};

export default function MultiAlphaOrthogonalityPage() {
  const [runIdsText, setRunIdsText] = useState("");
  const [k, setK] = useState(25);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<MultiAlphaOrthogonalityResult | null>(null);
  const runIds = useMemo(() => parseRunIds(runIdsText), [runIdsText]);

  async function submit() {
    setError(null);
    setResult(null);
    if (runIds.length < 2) {
      setError("请至少输入 2 个 run_id");
      return;
    }
    setLoading(true);
    try {
      setResult(await multiAlphaApi.orthogonality(runIds, k));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main style={{ padding: 24, display: "grid", gap: 18 }}>
      <div>
        <p style={{ margin: 0, color: "#64748b", fontSize: 13 }}>P3-A / Prediction Store</p>
        <h1 style={{ margin: "4px 0", fontSize: 28, fontWeight: 800, color: "#0f172a" }}>多 Alpha 正交性分析</h1>
        <p style={{ margin: 0, color: "#475569", maxWidth: 920 }}>
          输入已定稿 Alpha 腿的 run_id，从 prediction-store 只读拉取 pred.pkl，计算预测值相关矩阵与 Top-K 持仓 Jaccard 矩阵。真实数据需 prediction-store 已上传落盘。
        </p>
      </div>

      <SectionCard title="输入" eyebrow="支持逗号、空格或换行分隔 run_id；服务端会显式报告缺失 pred.pkl 或无公共日期。">
        <div style={{ display: "grid", gap: 12 }}>
          <textarea
            value={runIdsText}
            onChange={(event) => setRunIdsText(event.target.value)}
            placeholder="run_id_a, run_id_b, run_id_c"
            rows={5}
            style={{ width: "100%", border: "1px solid #cbd5e1", borderRadius: 10, padding: 12, fontFamily: "monospace", fontSize: 13 }}
          />
          <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
            <label style={{ fontSize: 13, color: "#334155", display: "flex", alignItems: "center", gap: 8 }}>
              Top-K
              <input
                type="number"
                min={1}
                max={500}
                value={k}
                onChange={(event) => setK(Number(event.target.value))}
                style={{ width: 88, border: "1px solid #cbd5e1", borderRadius: 8, padding: "8px 10px" }}
              />
            </label>
            <button
              type="button"
              onClick={submit}
              disabled={loading}
              style={{ border: 0, borderRadius: 10, padding: "9px 16px", fontWeight: 800, color: "#fff", background: loading ? "#94a3b8" : "#0f766e", cursor: loading ? "not-allowed" : "pointer" }}
            >
              {loading ? "计算中..." : "计算正交性"}
            </button>
            <span style={{ fontSize: 12, color: "#64748b" }}>已解析 {runIds.length} 条腿</span>
          </div>
        </div>
      </SectionCard>

      {error ? <ErrorPanel title="正交性计算失败" error={new Error(error)} /> : null}
      {loading ? (
        <SectionCard title="加载中" eyebrow="正在通过只读后端拉取 pred.pkl 并计算矩阵。">
          <div style={{ padding: 24, color: "#64748b" }}>loading...</div>
        </SectionCard>
      ) : null}
      {!loading && !error && !result ? (
        <SectionCard title="等待输入" eyebrow="提交 2 条以上 run_id 后显示相关性和持仓重叠矩阵。">
          <div style={{ padding: 24, color: "#64748b" }}>empty</div>
        </SectionCard>
      ) : null}
      {result ? <ResultPanel result={result} /> : null}
    </main>
  );
}


